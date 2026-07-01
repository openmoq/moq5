/*
 * test_media_receiver_subscribe — per-track subscription control on
 * moq_media_receiver_t against a real threaded-picoquic loopback server that
 * publishes a two-track catalog ("v" video + "a" audio) and serves whichever
 * tracks the receiver subscribes.
 *
 * Covers: auto_subscribe=false discovery-only + manual subscribe; subscribe
 * valid after TRACK_ADDED but before CATALOG_READY; cfg validation (bad
 * struct_size / unknown start mode); selective multi-track
 * subscribe/unsubscribe; a deterministic FLOW_CONTROL purge-while-paused proof;
 * peer rejection surfacing TRACK_ENDED without failing the receiver, and that an
 * ended track refuses re-subscribe; a toggle soak past the facade's track table;
 * and ASAN-clean teardown with pending commands + queued objects. Also asserts
 * the event/object struct_size ABI guards and the track_state accessor across
 * DISCOVERED/ACTIVE/PAUSED_APP/PAUSED_FLOW/ENDED.
 */
#include <moq/media_receiver.h>
#include <moq/picoquic_threaded.h>
#include <moq/publisher.h>
#include <moq/msf.h>
#include <moq/loc.h>
#include "test_support.h"

#include <stdatomic.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

static int failures = 0;

static const char k_two_track_catalog[] =
    "{\"version\":1,\"generatedAt\":1,\"tracks\":["
    "{\"name\":\"v\",\"packaging\":\"loc\",\"isLive\":true,"
    "\"role\":\"video\",\"codec\":\"av01\",\"timescale\":1000000},"
    "{\"name\":\"a\",\"packaging\":\"loc\",\"isLive\":true,"
    "\"role\":\"audio\",\"codec\":\"opus\",\"samplerate\":48000,"
    "\"channelConfig\":\"2\"}"
    "]}";

#define OBJS_PER_GROUP 3

/* Per-track object budget. Kept small on purpose: the picoquic adapter cannot
 * apply per-stream transport backpressure, so a publisher that floods objects
 * faster than the receiver's network thread drains the session event ring would
 * force an inbound WOULD_BLOCK the adapter then overruns (a separate transport
 * concern, not the subscription path under test). A modest, bounded count keeps
 * this test focused and deterministic, matching the existing receiver harness. */
#define OBJS_PER_TRACK 6

typedef struct {
    moq_publisher_t *pub;
    moq_pub_track_t *catalog;
    moq_pub_track_t *v;
    moq_pub_track_t *a;
    int              v_pub;
    int              a_pub;
    const char      *reject_name;   /* NULL, or a media track name to reject */
    const char      *record_name;   /* NULL, or a track whose SUBSCRIBE params
                                       to capture (filter/priority) */
    atomic_int       recorded;      /* set once record_name's SUBSCRIBE seen */
    int              rec_filter;    /* moq_subscribe_filter_t as seen on wire */
    int              rec_priority;  /* subscriber_priority as seen on wire */
    const char      *defer_name;    /* NULL, or a track whose SUBSCRIBE to DEFER */
    moq_pub_deferred_t *deferred;   /* captured deferred handle (server thread) */
    uint64_t         deferred_id;
    atomic_int       deferred_seen; /* set when defer_name's SUBSCRIBE arrived */
    atomic_int       do_resolve;    /* test arms: accept the deferred subscribe */
    int              resolved;      /* server-local: resolved once */
    atomic_int       end_v;         /* test arms: reliably end the "v" track */
    int              v_end_done;    /* server-local: end emitted once */
    bool             failed;
} msrv_t;

static msrv_t g_msrv;

/* CALLBACK accept policy: reject one named track, accept everything else. */
static moq_pub_accept_decision_t msrv_on_subscribe(
    void *ctx, const moq_pub_subscribe_info_t *info,
    moq_request_error_t *out_error)
{
    msrv_t *st = (msrv_t *)ctx;
    if (st->reject_name &&
        info->track_name.len == strlen(st->reject_name) &&
        memcmp(info->track_name.data, st->reject_name,
               info->track_name.len) == 0) {
        *out_error = MOQ_REQUEST_ERROR_NOT_SUPPORTED;
        return MOQ_PUB_DECISION_REJECT;
    }
    /* Capture the wire SUBSCRIBE parameters for the named track so the test can
     * assert the cfg (start mode -> filter, priority) reached the publisher. */
    if (st->record_name &&
        info->track_name.len == strlen(st->record_name) &&
        memcmp(info->track_name.data, st->record_name,
               info->track_name.len) == 0) {
        st->rec_filter = (int)info->filter;
        st->rec_priority = (int)info->subscriber_priority;
        atomic_store(&st->recorded, 1);
    }
    /* Defer the named track's decision (so the client observes PENDING) until the
     * test resolves it. Only when a defer slot is free (info->deferred != NULL). */
    if (st->defer_name && info->deferred &&
        info->track_name.len == strlen(st->defer_name) &&
        memcmp(info->track_name.data, st->defer_name,
               info->track_name.len) == 0) {
        st->deferred = info->deferred;
        st->deferred_id = info->deferred_id;
        atomic_store(&st->deferred_seen, 1);
        return MOQ_PUB_DECISION_DEFER;
    }
    return MOQ_PUB_DECISION_ACCEPT;
}

/* Publish a few LOC objects on `track` (keyframe-led groups of three) while it
 * has a subscriber, capped so the queue can fill without unbounded growth. */
static bool publish_track(msrv_t *st, moq_pub_track_t *track, int *counter,
                          uint8_t tag, uint64_t now_us)
{
    if (!track || !moq_pub_has_subscriber(st->pub, track)) return true;
    while (*counter < OBJS_PER_TRACK) {
        int i = *counter;
        moq_loc_headers_t h;
        moq_loc_headers_init(&h);
        h.has_timestamp = true;
        h.timestamp = (uint64_t)i * 1000u;
        h.has_video_frame_marking = true;
        h.video_frame_marking.independent = (i % OBJS_PER_GROUP) == 0;
        moq_rcbuf_t *props = NULL;
        if (moq_loc_encode(moq_alloc_default(), MOQ_LOC_PROFILE_01, &h,
                           &props) != MOQ_OK)
            return false;
        uint8_t bytes[16];
        memset(bytes, tag, sizeof(bytes));
        moq_rcbuf_t *payload = NULL;
        if (moq_rcbuf_create(moq_alloc_default(), bytes, sizeof(bytes),
                             &payload) < 0) {
            if (props) moq_rcbuf_decref(props);
            return false;
        }
        moq_pub_object_cfg_t ocfg;
        moq_pub_object_cfg_init(&ocfg);
        ocfg.group_id = (uint64_t)(i / OBJS_PER_GROUP);
        ocfg.object_id = (uint64_t)(i % OBJS_PER_GROUP);
        ocfg.payload = payload;
        ocfg.properties = props;
        ocfg.end_of_group = true;
        moq_result_t wrc = moq_pub_write_object_ex(st->pub, track, &ocfg,
                                                   now_us);
        moq_rcbuf_decref(payload);
        if (props) moq_rcbuf_decref(props);
        if (wrc == MOQ_ERR_WOULD_BLOCK || wrc == MOQ_ERR_WRONG_STATE)
            return true;
        if (wrc != MOQ_OK) return false;
        (*counter)++;
    }
    return true;
}

static int msrv_pump(moq_pq_threaded_t *t, uint64_t now_us, void *ctx)
{
    msrv_t *st = (msrv_t *)ctx;
    moq_session_t *session = moq_pq_threaded_session(t);
    if (!session) return 0;

    if (!st->pub) {
        moq_pub_cfg_t pcfg;
        moq_pub_cfg_init_sized(&pcfg, sizeof(pcfg));
        pcfg.accept_mode = (st->reject_name || st->record_name || st->defer_name)
                               ? MOQ_PUB_CALLBACK : MOQ_PUB_ACCEPT_ALL;
        if (st->reject_name || st->record_name || st->defer_name) {
            pcfg.on_subscribe = msrv_on_subscribe;
            pcfg.on_subscribe_ctx = st;
        }
        if (moq_pub_create(session, moq_alloc_default(), &pcfg,
                           &st->pub) != MOQ_OK) {
            st->failed = true;
            return -1;
        }
        moq_bytes_t parts[] = {
            MOQ_BYTES_LITERAL("svc"), MOQ_BYTES_LITERAL("demo") };
        moq_namespace_t ns = { parts, 2 };
        struct { moq_pub_track_t **slot; moq_bytes_t name; } adds[] = {
            { &st->catalog, MOQ_BYTES_LITERAL(MOQ_MSF_CATALOG_TRACK_NAME) },
            { &st->v,       MOQ_BYTES_LITERAL("v") },
            { &st->a,       MOQ_BYTES_LITERAL("a") },
        };
        for (size_t i = 0; i < 3; i++) {
            moq_pub_track_cfg_t tcfg;
            moq_pub_track_cfg_init(&tcfg);
            tcfg.track_namespace = ns;
            tcfg.track_name = adds[i].name;
            if (moq_pub_add_track(st->pub, &tcfg, now_us, adds[i].slot)
                != MOQ_OK) {
                st->failed = true;
                return -1;
            }
        }
        moq_rcbuf_t *payload = NULL;
        if (moq_rcbuf_create(moq_alloc_default(),
                             (const uint8_t *)k_two_track_catalog,
                             strlen(k_two_track_catalog), &payload) < 0) {
            st->failed = true;
            return -1;
        }
        moq_pub_retained_object_t robj = { .object_id = 0, .payload = payload };
        moq_pub_retained_group_cfg_t rg;
        moq_pub_retained_group_cfg_init(&rg);
        rg.group_id = 0;
        rg.objects = &robj;
        rg.object_count = 1;
        moq_result_t src = moq_pub_set_retained_group(st->pub, st->catalog,
                                                      &rg);
        moq_rcbuf_decref(payload);
        if (src != MOQ_OK) { st->failed = true; return -1; }
    }

    /* Reliably end "v" when the test arms it (subgroup END_OF_TRACK, not a
     * datagram), once it has a subscriber to deliver the terminal to. */
    if (st->v && !st->v_end_done && atomic_load(&st->end_v) &&
        moq_pub_has_subscriber(st->pub, st->v)) {
        if (moq_pub_end_track(st->pub, st->v, now_us) == MOQ_OK)
            st->v_end_done = 1;   /* else WOULD_BLOCK: retry next pump */
    }

    /* Resolve a previously deferred subscribe when the test arms it (accept). */
    if (st->deferred && !st->resolved && atomic_load(&st->do_resolve)) {
        if (moq_pub_resolve_deferred(st->pub, st->deferred, st->deferred_id,
                                     true, 0, now_us) == MOQ_OK)
            st->resolved = 1;   /* else WOULD_BLOCK: retry next pump */
    }

    (void)moq_pub_tick(st->pub, now_us);
    if (!publish_track(st, st->v, &st->v_pub, 'V', now_us) ||
        !publish_track(st, st->a, &st->a_pub, 'A', now_us)) {
        st->failed = true;
        return -1;
    }

    (void)moq_pub_tick(st->pub, now_us);
    return 0;
}

static moq_pq_threaded_t *start_server(const char *cert, const char *key,
                                       int *out_port)
{
    static int calls = 0;
    int base = 18900 + (int)(getpid() % 997) + (calls++ * 137);
    for (int attempt = 0; attempt < 24; attempt++) {
        int port = base + attempt * 17;
        moq_pq_threaded_cfg_t cfg;
        moq_pq_threaded_cfg_init(&cfg);
        cfg.alloc = moq_alloc_default();
        cfg.perspective = MOQ_PERSPECTIVE_SERVER;
        cfg.cert_path = cert;
        cfg.key_path = key;
        cfg.port = port;
        cfg.send_request_capacity = true;
        cfg.initial_request_capacity = 16;
        cfg.on_pump = msrv_pump;
        cfg.on_pump_ctx = &g_msrv;
        moq_pq_threaded_t *srv = NULL;
        if (moq_pq_threaded_create(&cfg, &srv) == MOQ_OK) {
            *out_port = port;
            return srv;
        }
    }
    return NULL;
}

/* Create a manual (auto_subscribe=false) receiver bound to a private endpoint
 * at the server's port, with the given overflow policy. */
static moq_media_receiver_t *make_receiver(int port,
                                           moq_media_overflow_policy_t policy,
                                           uint32_t max_objects)
{
    static char url[64];
    snprintf(url, sizeof(url), "moqt://127.0.0.1:%d", port);
    moq_endpoint_cfg_t ec;
    moq_endpoint_cfg_init(&ec);
    ec.url = (moq_bytes_t){ (const uint8_t *)url, strlen(url) };
    ec.insecure_skip_verify = true;

    static moq_bytes_t parts[2];
    parts[0] = MOQ_BYTES_LITERAL("svc");
    parts[1] = MOQ_BYTES_LITERAL("demo");
    moq_media_receiver_cfg_t cfg;
    moq_media_receiver_cfg_init_live(&cfg);
    cfg.endpoint = &ec;
    cfg.namespace_ = (moq_namespace_t){ parts, 2 };
    cfg.auto_subscribe = false;          /* manual selection */
    cfg.overflow.policy = policy;
    cfg.overflow.max_objects = max_objects;   /* 0 = library default */

    moq_media_receiver_t *r = NULL;
    if (moq_media_receiver_create(&cfg, &r) != MOQ_OK) return NULL;
    return r;
}

/* Drain discovery; capture the "v" and "a" handles, return true at
 * CATALOG_READY. */
static bool discover(moq_media_receiver_t *r, moq_media_track_t **v,
                     moq_media_track_t **a)
{
    *v = NULL;
    *a = NULL;
    moq_media_track_event_t ev;
    for (int i = 0; i < 300; i++) {
        (void)moq_media_receiver_wait(r, 100000);
        while (moq_media_receiver_poll_track(r, &ev, sizeof(ev)) == MOQ_OK) {
            MOQ_TEST_CHECK(ev.struct_size == sizeof(ev));   /* ABI guard stamped */
            if (ev.kind == MOQ_MEDIA_TRACK_ADDED && ev.desc) {
                if (ev.desc->name.len == 1 && ev.desc->name.data[0] == 'v')
                    *v = ev.track;
                else if (ev.desc->name.len == 1 && ev.desc->name.data[0] == 'a')
                    *a = ev.track;
            } else if (ev.kind == MOQ_MEDIA_CATALOG_READY) {
                return *v && *a;
            }
        }
        if (moq_media_receiver_is_fatal(r)) return false;
    }
    return false;
}

/* Poll up to `budget` objects, counting how many belong to each handle.
 * Returns total polled. */
static int poll_counts(moq_media_receiver_t *r, moq_media_track_t *v,
                       moq_media_track_t *a, int budget, int ticks,
                       int *out_v, int *out_a)
{
    int nv = 0, na = 0, total = 0;
    for (int i = 0; i < ticks && total < budget; i++) {
        moq_media_object_t obj;
        moq_result_t rc;
        while ((rc = moq_media_receiver_poll_object(r, &obj, sizeof(obj))) == MOQ_OK) {
            MOQ_TEST_CHECK(obj.struct_size == sizeof(obj));  /* ABI guard stamped */
            if (obj.track == v) nv++;
            else if (obj.track == a) na++;
            moq_media_object_cleanup(&obj);
            if (++total >= budget) break;
        }
        if (rc == MOQ_ERR_CLOSED) break;
        (void)moq_media_receiver_wait(r, 60000);
    }
    if (out_v) *out_v = nv;
    if (out_a) *out_a = na;
    return total;
}

/* track_state helper: MOQ_OK + state == want. */
static bool ts_is(moq_media_receiver_t *r, moq_media_track_t *t,
                  moq_media_track_state_t want)
{
    moq_media_track_state_t got;
    return moq_media_receiver_track_state(r, t, &got) == MOQ_OK && got == want;
}

int main(int argc, char **argv)
{
    const char *cert = NULL, *key = NULL;
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--cert") && i + 1 < argc) cert = argv[++i];
        else if (!strcmp(argv[i], "--key") && i + 1 < argc) key = argv[++i];
    }
    if (!cert || !key) {
        fprintf(stderr, "usage: --cert <pem> --key <pem>\n");
        return 1;
    }

    /* == auto_subscribe=false: discovery-only until manual subscribe ==== */
    {
        memset(&g_msrv, 0, sizeof(g_msrv));
        int port = 0;
        moq_pq_threaded_t *srv = start_server(cert, key, &port);
        MOQ_TEST_CHECK(srv != NULL);
        moq_media_receiver_t *r =
            make_receiver(port, MOQ_MEDIA_OVERFLOW_DROP_TO_KEYFRAME, 0);
        MOQ_TEST_CHECK(r != NULL);

        moq_media_track_t *v = NULL, *a = NULL;
        MOQ_TEST_CHECK(discover(r, &v, &a));

        /* No subscription yet: poll_object yields DONE, not objects, and the
         * track reports DISCOVERED. */
        moq_media_object_t obj;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_poll_object(r, &obj, sizeof(obj)),
                              (int)MOQ_DONE);
        MOQ_TEST_CHECK(ts_is(r, v, MOQ_MEDIA_TRACK_STATE_DISCOVERED));

        /* cfg validation: a struct_size too small and an unknown start mode are
         * both MOQ_ERR_INVAL (and must not record desired state). */
        moq_media_receiver_track_subscribe_cfg_t bad;
        moq_media_receiver_track_subscribe_cfg_init(&bad);
        bad.struct_size = 1;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_receiver_subscribe_track(r, v, &bad),
            (int)MOQ_ERR_INVAL);
        moq_media_receiver_track_subscribe_cfg_init(&bad);
        bad.start = (moq_media_start_mode_t)99;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_receiver_subscribe_track(r, v, &bad),
            (int)MOQ_ERR_INVAL);
        /* The rejected cfgs left v unsubscribed: still no objects. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_poll_object(r, &obj, sizeof(obj)),
                              (int)MOQ_DONE);

        /* A well-formed cfg (NEXT_GROUP start + explicit priority) is accepted. */
        moq_media_receiver_track_subscribe_cfg_t ok;
        moq_media_receiver_track_subscribe_cfg_init(&ok);
        ok.start = MOQ_MEDIA_START_NEXT_GROUP;
        ok.has_priority = true;
        ok.priority = 100;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_receiver_subscribe_track(r, a, &ok), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_receiver_unsubscribe_track(r, a), (int)MOQ_OK);

        /* Subscribe only "v"; objects for it begin, none for "a". */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_receiver_subscribe_track(r, v, NULL), (int)MOQ_OK);
        int nv = 0, na = 0;
        poll_counts(r, v, a, 6, 120, &nv, &na);
        MOQ_TEST_CHECK(nv > 0);
        MOQ_TEST_CHECK_EQ_INT(na, 0);
        /* Delivering now: ACTIVE; the unsubscribed audio is still DISCOVERED. */
        MOQ_TEST_CHECK(ts_is(r, v, MOQ_MEDIA_TRACK_STATE_ACTIVE));
        MOQ_TEST_CHECK(ts_is(r, a, MOQ_MEDIA_TRACK_STATE_DISCOVERED));

        moq_media_receiver_destroy(r);
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    /* == subscribe is valid after TRACK_ADDED, BEFORE CATALOG_READY ======
     * Poll only until "v"'s TRACK_ADDED (leaving CATALOG_READY -- and possibly
     * other tracks' TRACK_ADDED -- unpolled), subscribe immediately, and prove
     * objects arrive and CATALOG_READY still surfaces afterward in order. == */
    {
        memset(&g_msrv, 0, sizeof(g_msrv));
        int port = 0;
        moq_pq_threaded_t *srv = start_server(cert, key, &port);
        MOQ_TEST_CHECK(srv != NULL);
        moq_media_receiver_t *r =
            make_receiver(port, MOQ_MEDIA_OVERFLOW_DROP_TO_KEYFRAME, 0);
        MOQ_TEST_CHECK(r != NULL);

        /* Stop at v's TRACK_ADDED; CATALOG_READY (pushed after all TRACK_ADDED)
         * is therefore still unpolled when we subscribe. */
        moq_media_track_t *v = NULL;
        bool ready_before = false;
        for (int i = 0; i < 300 && !v; i++) {
            (void)moq_media_receiver_wait(r, 100000);
            moq_media_track_event_t ev;
            while (moq_media_receiver_poll_track(r, &ev, sizeof(ev)) == MOQ_OK) {
                if (ev.kind == MOQ_MEDIA_CATALOG_READY) ready_before = true;
                if (ev.kind == MOQ_MEDIA_TRACK_ADDED && ev.desc &&
                    ev.desc->name.len == 1 && ev.desc->name.data[0] == 'v') {
                    v = ev.track;
                    break;          /* do not drain further (no CATALOG_READY) */
                }
            }
            if (moq_media_receiver_is_fatal(r)) break;
        }
        MOQ_TEST_CHECK(v != NULL);
        MOQ_TEST_CHECK(!ready_before);   /* subscribing strictly pre-CATALOG_READY */

        /* The subscribe is accepted while CATALOG_READY is still unpolled. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_receiver_subscribe_track(r, v, NULL), (int)MOQ_OK);

        /* Drain the rest of discovery: CATALOG_READY (pushed after every
         * TRACK_ADDED) still surfaces, in order, after the subscribe. (Until the
         * event queue is drained, wait() returns immediately on the pending
         * events, so objects must be polled only after this.) */
        bool ready_after = false;
        for (int i = 0; i < 120 && !ready_after; i++) {
            moq_media_track_event_t ev;
            while (moq_media_receiver_poll_track(r, &ev, sizeof(ev)) == MOQ_OK)
                if (ev.kind == MOQ_MEDIA_CATALOG_READY) ready_after = true;
            if (!ready_after) (void)moq_media_receiver_wait(r, 50000);
            if (moq_media_receiver_is_fatal(r)) break;
        }
        MOQ_TEST_CHECK(ready_after);

        /* Objects arrive for the subscription that was issued pre-CATALOG_READY. */
        int nv = 0;
        poll_counts(r, v, NULL, 4, 120, &nv, NULL);
        MOQ_TEST_CHECK(nv > 0);

        moq_media_receiver_destroy(r);
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    /* == cfg fidelity: a NEXT_GROUP + priority subscribe reaches the publisher
     *    with exactly those wire parameters, and a rapid second enable with a
     *    different cfg (before the network hook issues the SUBSCRIBE) does NOT
     *    override the first cfg (lifetime contract). == */
    {
        memset(&g_msrv, 0, sizeof(g_msrv));
        g_msrv.record_name = "v";        /* capture v's SUBSCRIBE params */
        int port = 0;
        moq_pq_threaded_t *srv = start_server(cert, key, &port);
        MOQ_TEST_CHECK(srv != NULL);
        moq_media_receiver_t *r =
            make_receiver(port, MOQ_MEDIA_OVERFLOW_DROP_TO_KEYFRAME, 0);
        MOQ_TEST_CHECK(r != NULL);
        moq_media_track_t *v = NULL, *a = NULL;
        MOQ_TEST_CHECK(discover(r, &v, &a));

        moq_media_receiver_track_subscribe_cfg_t cfg;
        moq_media_receiver_track_subscribe_cfg_init(&cfg);
        cfg.start = MOQ_MEDIA_START_NEXT_GROUP;
        cfg.has_priority = true;
        cfg.priority = 100;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_receiver_subscribe_track(r, v, &cfg), (int)MOQ_OK);
        /* Immediately re-enable with defaults BEFORE waiting: must not overwrite
         * the first cfg, so the publisher still sees NEXT_GROUP + priority 100. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_receiver_subscribe_track(r, v, NULL), (int)MOQ_OK);

        bool got = false;
        for (int i = 0; i < 120 && !got; i++) {
            (void)moq_media_receiver_wait(r, 100000);
            if (atomic_load(&g_msrv.recorded)) got = true;
            if (moq_media_receiver_is_fatal(r)) break;
        }
        MOQ_TEST_CHECK(got);
        MOQ_TEST_CHECK_EQ_INT(g_msrv.rec_filter,
                              (int)MOQ_SUBSCRIBE_FILTER_NEXT_GROUP);
        MOQ_TEST_CHECK_EQ_INT(g_msrv.rec_priority, 100);

        moq_media_receiver_destroy(r);
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    /* == delayed accept: PENDING after the SUBSCRIBE is sent, ACTIVE only after
     *    the peer accepts it. The server DEFERS v's decision; the client must
     *    report PENDING in that window, then ACTIVE once resolved. == */
    {
        memset(&g_msrv, 0, sizeof(g_msrv));
        g_msrv.defer_name = "v";
        int port = 0;
        moq_pq_threaded_t *srv = start_server(cert, key, &port);
        MOQ_TEST_CHECK(srv != NULL);
        moq_media_receiver_t *r =
            make_receiver(port, MOQ_MEDIA_OVERFLOW_DROP_TO_KEYFRAME, 0);
        MOQ_TEST_CHECK(r != NULL);
        moq_media_track_t *v = NULL, *a = NULL;
        MOQ_TEST_CHECK(discover(r, &v, &a));

        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_receiver_subscribe_track(r, v, NULL), (int)MOQ_OK);

        /* Wait until the server has received (and deferred) the SUBSCRIBE. The
         * peer has not accepted, so the track is PENDING and stays PENDING. */
        bool seen = false;
        for (int i = 0; i < 200 && !seen; i++) {
            (void)moq_media_receiver_wait(r, 50000);
            if (atomic_load(&g_msrv.deferred_seen)) seen = true;
            if (moq_media_receiver_is_fatal(r)) break;
        }
        MOQ_TEST_CHECK(seen);
        MOQ_TEST_CHECK(ts_is(r, v, MOQ_MEDIA_TRACK_STATE_PENDING));

        /* Accept the deferred subscribe; the track advances to ACTIVE. */
        atomic_store(&g_msrv.do_resolve, 1);
        bool active = false;
        for (int i = 0; i < 200 && !active; i++) {
            (void)moq_media_receiver_wait(r, 50000);
            if (ts_is(r, v, MOQ_MEDIA_TRACK_STATE_ACTIVE)) active = true;
            if (moq_media_receiver_is_fatal(r)) break;
        }
        MOQ_TEST_CHECK(active);

        moq_media_receiver_destroy(r);
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    /* == multi-track: add audio later; unsubscribe video purges + stops == */
    {
        memset(&g_msrv, 0, sizeof(g_msrv));
        int port = 0;
        moq_pq_threaded_t *srv = start_server(cert, key, &port);
        MOQ_TEST_CHECK(srv != NULL);
        moq_media_receiver_t *r =
            make_receiver(port, MOQ_MEDIA_OVERFLOW_DROP_TO_KEYFRAME, 0);
        MOQ_TEST_CHECK(r != NULL);
        moq_media_track_t *v = NULL, *a = NULL;
        MOQ_TEST_CHECK(discover(r, &v, &a));

        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_receiver_subscribe_track(r, v, NULL), (int)MOQ_OK);
        int nv = 0, na = 0;
        poll_counts(r, v, a, 4, 120, &nv, &na);
        MOQ_TEST_CHECK(nv > 0);
        MOQ_TEST_CHECK_EQ_INT(na, 0);   /* audio not subscribed => not queued */

        /* Now subscribe audio: its objects begin. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_receiver_subscribe_track(r, a, NULL), (int)MOQ_OK);
        nv = 0; na = 0;
        poll_counts(r, v, a, 8, 120, &nv, &na);
        MOQ_TEST_CHECK(na > 0);

        /* Unsubscribe video: future video stops; any queued video is purged
         * (verified below -- no further video surfaces). */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_receiver_unsubscribe_track(r, v), (int)MOQ_OK);
        for (int i = 0; i < 10; i++) (void)moq_media_receiver_wait(r, 60000);
        int after_v = 0, after_a = 0;
        poll_counts(r, v, a, 12, 120, &after_v, &after_a);
        MOQ_TEST_CHECK_EQ_INT(after_v, 0);   /* purged + no further video */
        /* Disabled-by-app video reports PAUSED_APP; audio is still ACTIVE. */
        MOQ_TEST_CHECK(ts_is(r, v, MOQ_MEDIA_TRACK_STATE_PAUSED_APP));
        MOQ_TEST_CHECK(ts_is(r, a, MOQ_MEDIA_TRACK_STATE_ACTIVE));

        /* Stats invariant: every drop is also a receive (objects_dropped is a
         * subset of objects_received), and we did receive media. */
        moq_media_receiver_stats_t st;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_receiver_get_stats(r, &st, sizeof(st)), (int)MOQ_OK);
        MOQ_TEST_CHECK(st.objects_received > 0);
        MOQ_TEST_CHECK(st.objects_dropped <= st.objects_received);

        moq_media_receiver_destroy(r);
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    /* == FLOW_CONTROL: unsubscribe purges queued objects even while the track
     *    is already paused (forward == false). With a tiny object queue the
     *    receiver pauses upstream once it fills; unsubscribing then must still
     *    purge the queued objects (the purge must not depend on forward state).
     *    This is a deterministic purge proof: the queue is provably non-empty
     *    (paused) at unsubscribe time. == */
    {
        memset(&g_msrv, 0, sizeof(g_msrv));
        int port = 0;
        moq_pq_threaded_t *srv = start_server(cert, key, &port);
        MOQ_TEST_CHECK(srv != NULL);
        moq_media_receiver_t *r =
            make_receiver(port, MOQ_MEDIA_OVERFLOW_FLOW_CONTROL, 4);
        MOQ_TEST_CHECK(r != NULL);
        moq_media_track_t *v = NULL, *a = NULL;
        MOQ_TEST_CHECK(discover(r, &v, &a));

        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_receiver_subscribe_track(r, v, NULL), (int)MOQ_OK);

        /* Let objects queue WITHOUT polling until FLOW_CONTROL pauses upstream:
         * paused == true proves the queue is full of video objects. */
        bool paused = false;
        for (int i = 0; i < 200 && !paused; i++) {
            (void)moq_media_receiver_wait(r, 50000);
            moq_media_receiver_stats_t st;
            if (moq_media_receiver_get_stats(r, &st, sizeof(st)) == MOQ_OK && st.paused)
                paused = true;
            if (moq_media_receiver_is_fatal(r)) break;
        }
        MOQ_TEST_CHECK(paused);
        MOQ_TEST_CHECK(!moq_media_receiver_is_fatal(r));
        /* FLOW_CONTROL backpressure reports PAUSED_FLOW (distinct from app pause). */
        MOQ_TEST_CHECK(ts_is(r, v, MOQ_MEDIA_TRACK_STATE_PAUSED_FLOW));

        /* Unsubscribe while paused (forward already false): the queued video
         * must be purged regardless. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_receiver_unsubscribe_track(r, v), (int)MOQ_OK);
        for (int i = 0; i < 10; i++) (void)moq_media_receiver_wait(r, 50000);
        int after_v = 0;
        poll_counts(r, v, a, 12, 60, &after_v, NULL);
        MOQ_TEST_CHECK_EQ_INT(after_v, 0);   /* purged despite prior pause */

        moq_media_receiver_destroy(r);
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    /* == peer rejection of a manual subscribe -> TRACK_ENDED, not fatal == */
    {
        memset(&g_msrv, 0, sizeof(g_msrv));
        g_msrv.reject_name = "a";        /* server rejects audio subscribes */
        int port = 0;
        moq_pq_threaded_t *srv = start_server(cert, key, &port);
        MOQ_TEST_CHECK(srv != NULL);
        moq_media_receiver_t *r =
            make_receiver(port, MOQ_MEDIA_OVERFLOW_DROP_TO_KEYFRAME, 0);
        MOQ_TEST_CHECK(r != NULL);
        moq_media_track_t *v = NULL, *a = NULL;
        MOQ_TEST_CHECK(discover(r, &v, &a));

        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_receiver_subscribe_track(r, a, NULL), (int)MOQ_OK);

        /* Rejection surfaces TRACK_ENDED for "a"; the receiver stays alive. */
        bool a_ended = false;
        for (int i = 0; i < 120 && !a_ended; i++) {
            (void)moq_media_receiver_wait(r, 100000);
            moq_media_track_event_t ev;
            while (moq_media_receiver_poll_track(r, &ev, sizeof(ev)) == MOQ_OK) {
                if (ev.kind == MOQ_MEDIA_TRACK_ENDED && ev.track == a)
                    a_ended = true;
            }
            if (moq_media_receiver_is_fatal(r)) break;
        }
        MOQ_TEST_CHECK(a_ended);
        MOQ_TEST_CHECK(!moq_media_receiver_is_fatal(r));
        MOQ_TEST_CHECK(ts_is(r, a, MOQ_MEDIA_TRACK_STATE_ENDED));

        /* An ended track cannot be re-subscribed: deterministic WRONG_STATE,
         * not a silently-accepted command that can never reconcile. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_receiver_subscribe_track(r, a, NULL),
            (int)MOQ_ERR_WRONG_STATE);

        /* Video still subscribes and delivers despite the audio rejection. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_receiver_subscribe_track(r, v, NULL), (int)MOQ_OK);
        int nv = 0;
        poll_counts(r, v, a, 4, 120, &nv, NULL);
        MOQ_TEST_CHECK(nv > 0);

        moq_media_receiver_destroy(r);
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    /* NOTE: the reliable data-plane END_OF_TRACK -> TRACK_ENDED scenario lives in
     * the "reliable END_OF_TRACK" test above: the server calls moq_pub_end_track,
     * which emits a terminal status object on a subgroup STREAM (not a datagram),
     * so it carries over the picoquic-threaded transport without the QUIC DATAGRAM
     * extension. That scenario also asserts the session stays alive (audio stays
     * ACTIVE) and that re-subscribe on the ended handle == MOQ_ERR_WRONG_STATE. */

    /* == output-struct size contract: too-small -> INVAL (no write); a larger
     *    buffer is filled with only the library's struct and the trailing bytes
     *    are left untouched (struct_size reports the written size). == */
    {
        memset(&g_msrv, 0, sizeof(g_msrv));
        int port = 0;
        moq_pq_threaded_t *srv = start_server(cert, key, &port);
        MOQ_TEST_CHECK(srv != NULL);
        moq_media_receiver_t *r =
            make_receiver(port, MOQ_MEDIA_OVERFLOW_DROP_TO_KEYFRAME, 0);
        MOQ_TEST_CHECK(r != NULL);
        moq_media_track_t *v = NULL, *a = NULL;
        MOQ_TEST_CHECK(discover(r, &v, &a));

        /* A size below the v0 floor is rejected before any write. */
        moq_media_object_t o;
        moq_media_track_event_t te;
        moq_media_receiver_stats_t sst;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_receiver_poll_object(r, &o, 8), (int)MOQ_ERR_INVAL);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_receiver_poll_track(r, &te, 8), (int)MOQ_ERR_INVAL);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_receiver_get_stats(r, &sst, 8), (int)MOQ_ERR_INVAL);

        /* A size at/above the v0 floor but below sizeof(current) -- the stats
         * struct has trailing padding after `paused` -- is SERVED (clamped),
         * proving the floor is the frozen v0 base, not sizeof(current). The old
         * `>= sizeof` validation would have rejected this. */
        memset(&sst, 0, sizeof(sst));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_receiver_get_stats(r, &sst, sizeof(sst) - 1),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)sst.struct_size, (int)(sizeof(sst) - 1));

        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_receiver_subscribe_track(r, v, NULL), (int)MOQ_OK);

        /* Larger buffer: simulate a newer caller. The library clamps to its own
         * struct size; the 16 canary bytes past the struct stay 0xAB. */
        struct { moq_media_object_t obj; unsigned char tail[16]; } big;
        bool got = false;
        for (int i = 0; i < 120 && !got; i++) {
            memset(&big, 0xAB, sizeof(big));
            moq_result_t rc =
                moq_media_receiver_poll_object(r, &big.obj, sizeof(big));
            if (rc == MOQ_OK) {
                got = true;
                MOQ_TEST_CHECK_EQ_INT((int)big.obj.struct_size,
                                      (int)sizeof(moq_media_object_t));
                bool tail_ok = true;
                for (int k = 0; k < 16; k++)
                    if (big.tail[k] != 0xAB) tail_ok = false;
                MOQ_TEST_CHECK(tail_ok);   /* nothing written past the struct */
                moq_media_object_cleanup(&big.obj);
            } else {
                (void)moq_media_receiver_wait(r, 60000);
            }
            if (moq_media_receiver_is_fatal(r)) break;
        }
        MOQ_TEST_CHECK(got);

        moq_media_receiver_destroy(r);
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    /* NOTE: no deterministic clean-close -> ENDED test. Turning the receiver
     * terminal cleanly requires the peer to close gracefully; stopping the test
     * server relies on the connection's idle timeout (tens of seconds), which is
     * neither prompt nor within this test's budget. track_state folds
     * moq_endpoint_is_closed() into ENDED regardless (verified by inspection);
     * the terminal path is otherwise exercised by the endpoint lifecycle tests. */

    /* == reliable END_OF_TRACK: ends one track; session + other tracks survive ==
     * The publisher emits a reliable (subgroup, non-datagram) END_OF_TRACK on v.
     * The receiver surfaces MOQ_MEDIA_TRACK_ENDED, reports ENDED, refuses
     * re-subscribe, and stays alive with audio still active. == */
    {
        memset(&g_msrv, 0, sizeof(g_msrv));
        int port = 0;
        moq_pq_threaded_t *srv = start_server(cert, key, &port);
        MOQ_TEST_CHECK(srv != NULL);
        moq_media_receiver_t *r =
            make_receiver(port, MOQ_MEDIA_OVERFLOW_DROP_TO_KEYFRAME, 0);
        MOQ_TEST_CHECK(r != NULL);
        moq_media_track_t *v = NULL, *a = NULL;
        MOQ_TEST_CHECK(discover(r, &v, &a));

        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_receiver_subscribe_track(r, v, NULL), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_receiver_subscribe_track(r, a, NULL), (int)MOQ_OK);
        int nv = 0, na = 0;
        poll_counts(r, v, a, 12, 200, &nv, &na);
        MOQ_TEST_CHECK(nv > 0);
        MOQ_TEST_CHECK(na > 0);

        atomic_store(&g_msrv.end_v, 1);   /* reliably end v */

        bool v_ended = false;
        for (int i = 0; i < 200 && !v_ended; i++) {
            moq_media_object_t o;
            while (moq_media_receiver_poll_object(r, &o, sizeof(o)) == MOQ_OK)
                moq_media_object_cleanup(&o);
            moq_media_track_event_t ev;
            while (moq_media_receiver_poll_track(r, &ev, sizeof(ev)) == MOQ_OK)
                if (ev.kind == MOQ_MEDIA_TRACK_ENDED && ev.track == v)
                    v_ended = true;
            if (!v_ended) (void)moq_media_receiver_wait(r, 50000);
            if (moq_media_receiver_is_fatal(r)) break;
        }
        MOQ_TEST_CHECK(v_ended);                          /* TRACK_ENDED surfaced */
        MOQ_TEST_CHECK(!moq_media_receiver_is_fatal(r));  /* session not fatal */
        MOQ_TEST_CHECK(!moq_media_receiver_is_closed(r)); /* session still alive */
        MOQ_TEST_CHECK(ts_is(r, v, MOQ_MEDIA_TRACK_STATE_ENDED));
        MOQ_TEST_CHECK(ts_is(r, a, MOQ_MEDIA_TRACK_STATE_ACTIVE)); /* audio lives */
        /* The ended track refuses re-subscribe. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_receiver_subscribe_track(r, v, NULL),
            (int)MOQ_ERR_WRONG_STATE);

        moq_media_receiver_destroy(r);
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    /* == toggle soak: enable/disable far past the facade's track table ===
     * Re-enabling a track must reuse its existing subscription (forward=true),
     * never allocate a fresh handle, so toggling well beyond the facade's 32
     * track slots cannot exhaust them or terminalize the receiver. */
    {
        memset(&g_msrv, 0, sizeof(g_msrv));
        int port = 0;
        moq_pq_threaded_t *srv = start_server(cert, key, &port);
        MOQ_TEST_CHECK(srv != NULL);
        moq_media_receiver_t *r =
            make_receiver(port, MOQ_MEDIA_OVERFLOW_DROP_TO_KEYFRAME, 0);
        MOQ_TEST_CHECK(r != NULL);
        moq_media_track_t *v = NULL, *a = NULL;
        MOQ_TEST_CHECK(discover(r, &v, &a));

        /* Confirm delivery works before the soak. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_receiver_subscribe_track(r, v, NULL), (int)MOQ_OK);
        int nv = 0;
        poll_counts(r, v, a, 2, 120, &nv, NULL);
        MOQ_TEST_CHECK(nv > 0);

        /* Toggle 80 times = 40 re-enables, well past the facade's 32 track
         * slots (one already held by the catalog). A re-subscribe-on-enable
         * implementation would exhaust the table and go FATAL_SETUP_FAILED. */
        for (int i = 0; i < 80; i++) {
            moq_result_t rc = (i & 1)
                ? moq_media_receiver_subscribe_track(r, v, NULL)
                : moq_media_receiver_unsubscribe_track(r, v);
            MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
            (void)moq_media_receiver_wait(r, 15000);   /* let the hook reconcile */
            MOQ_TEST_CHECK(!moq_media_receiver_is_fatal(r));
        }
        /* End enabled and still healthy: not fatal, track not ended. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_receiver_subscribe_track(r, v, NULL), (int)MOQ_OK);
        for (int i = 0; i < 10; i++) (void)moq_media_receiver_wait(r, 20000);
        MOQ_TEST_CHECK(!moq_media_receiver_is_fatal(r));

        moq_media_receiver_destroy(r);
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    /* == ASAN: destroy with pending commands + queued objects =========== */
    {
        memset(&g_msrv, 0, sizeof(g_msrv));
        int port = 0;
        moq_pq_threaded_t *srv = start_server(cert, key, &port);
        MOQ_TEST_CHECK(srv != NULL);
        moq_media_receiver_t *r =
            make_receiver(port, MOQ_MEDIA_OVERFLOW_DROP_TO_KEYFRAME, 0);
        MOQ_TEST_CHECK(r != NULL);
        moq_media_track_t *v = NULL, *a = NULL;
        MOQ_TEST_CHECK(discover(r, &v, &a));

        /* Fire commands and let objects queue, then destroy immediately
         * without draining: exercises teardown with in-flight desired-state
         * commands and queued objects (ASAN guards leaks/UAF). */
        (void)moq_media_receiver_subscribe_track(r, v, NULL);
        (void)moq_media_receiver_subscribe_track(r, a, NULL);
        for (int i = 0; i < 6; i++) (void)moq_media_receiver_wait(r, 50000);
        (void)moq_media_receiver_unsubscribe_track(r, v);
        moq_media_receiver_destroy(r);   /* no drain */

        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    MOQ_TEST_PASS("test_media_receiver_subscribe");
    return failures != 0;
}
