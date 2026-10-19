/* What a sender owes a graceful drain, as the endpoint reads it after each
 * pump (has_pending_output), through the production hook, publisher and a
 * SimPair subscriber. Each owed state is checked before the pump that
 * discharges it and after:
 *   - an object queued for a track nobody subscribes to is retained for a
 *     joiner, not owed: it stays queued and counted;
 *   - an END_OF_TRACK queued behind that retained media is held with it;
 *     when demand later arrives the retained media resumes, followed by
 *     exactly one END;
 *   - with nothing retained ahead of it, an END_OF_TRACK is owed until the
 *     pump hands it over, with or without a subscriber;
 *   - an accepted PUBLISH is an established publication even at Forward 0:
 *     its queued terminal stays owed (no completion is reported while it is
 *     held), and at Forward 1 it is handed over;
 *   - an object of a subscribed track is owed until the pump sends it;
 *   - a reset owed for a group abandoned after its subscriber left is owed
 *     until it is sent;
 *   - a post-ready catalog change is owed until it is republished;
 *   - a removed track is owed until it is torn down, including when its
 *     teardown is refused and retried after the republish committed;
 *   - a live->VOD conversion's PUBLISH_DONE is owed until it is sent. */
#include <moq/media_sender.h>
#include <moq/msf.h>
#include <moq/rcbuf.h>
#include <moq/session.h>
#include <moq/sim.h>
#include "test_support.h"

#include <stdint.h>
#include <string.h>

static int failures;

moq_media_sender_t *moq_media_sender_test_new_cfg(const moq_media_sender_cfg_t *cfg);
void moq_media_sender_test_pump(moq_media_sender_t *s, moq_session_t *session,
                               uint64_t now_us);
void moq_media_sender_test_free(moq_media_sender_t *s);
void moq_media_sender_test_set_clock_ms(moq_media_sender_t *s, bool set, uint64_t ms);
bool moq_media_sender_test_has_pending_output(moq_media_sender_t *s);
void moq_media_sender_test_refuse_teardown_end(moq_media_sender_t *s, uint32_t n);
void moq_media_sender_test_refuse_reset(moq_media_sender_t *s, uint32_t n);

enum { MAX_SUBS = 4 };

typedef struct {
    moq_simpair_t      *pair;
    moq_media_sender_t *sender;
    moq_bytes_t         ns_part;
    moq_namespace_t     ns;
    bool                accept_forward;    /* a PUBLISH is accepted with it */
    moq_subscription_t  sub[MAX_SUBS];
    unsigned            objects[MAX_SUBS], ends[MAX_SUBS], resets[MAX_SUBS];
    unsigned            oks, publishes;
    moq_publication_t   publication;       /* the accepted media PUBLISH */
    uint64_t            now_us;
} fixture_t;

static int sub_index(const fixture_t *f, moq_subscription_t sub)
{
    for (int i = 0; i < MAX_SUBS; i++)
        if (f->sub[i]._opaque && moq_subscription_eq(f->sub[i], sub)) return i;
    return -1;
}

/* The subscriber side: accept the namespace and any PUBLISH, count
 * SUBSCRIBE_OKs and, per subscription, media objects, END_OF_TRACK status
 * objects and subgroup resets. */
static void collect(fixture_t *f)
{
    moq_session_t *peer = moq_simpair_server(f->pair);
    moq_event_t ev;
    while (moq_session_poll_events(peer, &ev, 1) == 1) {
        if (ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED) {
            moq_accept_namespace_cfg_t ac;
            moq_accept_namespace_cfg_init(&ac);
            (void)moq_session_accept_namespace(peer, ev.u.namespace_published.ann,
                                               &ac, f->now_us);
        } else if (ev.kind == MOQ_EVENT_PUBLISH_REQUEST) {
            /* The name is borrowed: read it before the advancing accept. */
            moq_bytes_t n = ev.u.publish_request.track_name;
            bool catalog = n.len == strlen(MOQ_MSF_CATALOG_TRACK_NAME) &&
                memcmp(n.data, MOQ_MSF_CATALOG_TRACK_NAME, n.len) == 0;
            moq_accept_publish_cfg_t ac;
            moq_accept_publish_cfg_init(&ac);
            ac.has_forward = true;
            ac.forward = f->accept_forward;
            MOQ_TEST_CHECK_EQ_INT(moq_session_accept_publish(peer,
                ev.u.publish_request.pub, &ac, f->now_us), MOQ_OK);
            if (!catalog) {
                f->publication = ev.u.publish_request.pub;
                f->publishes++;
            }
        } else if (ev.kind == MOQ_EVENT_SUBSCRIBE_OK) {
            f->oks++;
        } else if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) {
            int i = sub_index(f, ev.u.object_received.sub);
            if (i >= 0) {
                if (ev.u.object_received.status == MOQ_OBJECT_END_OF_TRACK)
                    f->ends[i]++;
                else if (ev.u.object_received.status == MOQ_OBJECT_NORMAL)
                    f->objects[i]++;
            }
        } else if (ev.kind == MOQ_EVENT_SUBGROUP_RESET) {
            int i = sub_index(f, ev.u.subgroup_reset.sub);
            if (i >= 0) f->resets[i]++;
        }
        moq_event_cleanup(&ev);
    }
}

static void drive(fixture_t *f)
{
    moq_media_sender_test_pump(f->sender, moq_simpair_client(f->pair), f->now_us);
    MOQ_TEST_CHECK_EQ_INT(moq_simpair_run_until_quiescent(f->pair, 64, NULL), MOQ_OK);
    collect(f);
}

static void handshake(moq_session_t *session)
{
    moq_event_t ev;
    while (moq_session_poll_events(session, &ev, 1) == 1) moq_event_cleanup(&ev);
}

/* Subscribe `name` into slot `i`; from_start asks for the track from its
 * first location (ABSOLUTE_START 0/0), else from the largest object. */
static bool subscribe(fixture_t *f, int i, const char *name, bool from_start)
{
    unsigned before = f->oks;
    moq_subscribe_cfg_t cfg;
    moq_subscribe_cfg_init(&cfg);
    cfg.track_namespace = f->ns;
    cfg.track_name = (moq_bytes_t){ (const uint8_t *)name, strlen(name) };
    cfg.filter = from_start ? MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START
                            : MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    cfg.has_forward = true;
    cfg.forward = true;
    MOQ_TEST_CHECK_EQ_INT(moq_session_subscribe(moq_simpair_server(f->pair), &cfg,
                                                f->now_us, &f->sub[i]), MOQ_OK);
    for (int k = 0; k < 16 && f->oks == before; k++) drive(f);
    MOQ_TEST_CHECK(f->oks == before + 1);
    return f->oks == before + 1;
}

static void unsubscribe(fixture_t *f, int i)
{
    MOQ_TEST_CHECK_EQ_INT(moq_session_unsubscribe(moq_simpair_server(f->pair),
                                                  f->sub[i], f->now_us), MOQ_OK);
    drive(f);
}

static moq_media_track_t *add_track(fixture_t *f, const char *name)
{
    moq_media_track_cfg_t cfg;
    moq_media_track_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.name = (moq_bytes_t){ (const uint8_t *)name, strlen(name) };
    cfg.media_type = MOQ_MEDIA_TYPE_VIDEO;
    cfg.packaging = MOQ_MEDIA_PACKAGING_RAW;
    cfg.codec = MOQ_BYTES_LITERAL("av01");
    cfg.bitrate = 1500000;
    moq_media_track_t *t = NULL;
    MOQ_TEST_CHECK_EQ_INT(moq_media_sender_add_track(f->sender, &cfg, &t), MOQ_OK);
    return t;
}

static void write_object(fixture_t *f, moq_media_track_t *t, uint64_t pts,
                         bool starts_group)
{
    static const uint8_t payload[] = { 0x31, 0x72, 0xa5 };
    moq_rcbuf_t *buf = NULL;
    MOQ_TEST_CHECK_EQ_INT(moq_rcbuf_create(moq_alloc_default(), payload,
                                          sizeof(payload), &buf), MOQ_OK);
    moq_media_send_object_t obj;
    memset(&obj, 0, sizeof(obj));
    obj.struct_size = sizeof(obj);
    obj.payload = buf;
    obj.is_sync = obj.starts_group = starts_group;
    obj.presentation_time_us = pts;
    moq_result_t rc = moq_media_sender_write(f->sender, t, &obj);
    MOQ_TEST_CHECK_EQ_INT(rc, MOQ_OK);
    if (rc != MOQ_OK) moq_rcbuf_decref(buf);
}

static uint64_t queued(fixture_t *f)
{
    moq_media_sender_stats_t st;
    memset(&st, 0, sizeof(st));
    MOQ_TEST_CHECK_EQ_INT(moq_media_sender_get_stats(f->sender, &st, sizeof(st)), MOQ_OK);
    return st.objects_queued;
}

static bool owed(fixture_t *f) { return moq_media_sender_test_has_pending_output(f->sender); }

typedef struct {
    bool publish_tracks;
    bool accept_forward;
    bool drop_without_demand;
} setup_t;

static bool setup(fixture_t *f, moq_version_t version, setup_t how)
{
    memset(f, 0, sizeof(*f));
    f->ns_part = MOQ_BYTES_LITERAL("owed");
    f->ns = (moq_namespace_t){ &f->ns_part, 1 };
    f->accept_forward = how.accept_forward;
    moq_simpair_cfg_t sc = MOQ_SIMPAIR_CFG_INIT;
    sc.alloc = moq_alloc_default();
    sc.seed = 7;
    sc.initial_now_us = 1000;
    sc.version = version;
    sc.client_send_request_capacity = sc.server_send_request_capacity = true;
    sc.client_initial_request_capacity = sc.server_initial_request_capacity = 32;
    MOQ_TEST_CHECK_EQ_INT(moq_simpair_create(&sc, &f->pair), MOQ_OK);
    if (!f->pair) return false;
    moq_simpair_start(f->pair);
    MOQ_TEST_CHECK_EQ_INT(moq_simpair_run_until_quiescent(f->pair, 64, NULL), MOQ_OK);
    handshake(moq_simpair_client(f->pair));
    handshake(moq_simpair_server(f->pair));
    f->now_us = moq_simpair_now_us(f->pair);
    moq_media_sender_cfg_t cfg;
    moq_media_sender_cfg_init_live_sized(&cfg, sizeof(cfg));
    cfg.namespace_ = f->ns;
    cfg.publish_tracks = how.publish_tracks;
    cfg.drop_without_demand = how.drop_without_demand;
    f->sender = moq_media_sender_test_new_cfg(&cfg);
    MOQ_TEST_CHECK(f->sender != NULL);
    if (!f->sender) return false;
    moq_media_sender_test_set_clock_ms(f->sender, true, 1700000000000ull);
    return true;
}

static bool ready(fixture_t *f)
{
    for (int i = 0; i < 16 && !moq_media_sender_is_ready(f->sender); i++) drive(f);
    MOQ_TEST_CHECK(moq_media_sender_is_ready(f->sender));
    for (int i = 0; i < 4; i++) drive(f);
    return moq_media_sender_is_ready(f->sender);
}

static void teardown(fixture_t *f)
{
    if (f->sender) moq_media_sender_test_free(f->sender);
    if (f->pair) moq_simpair_destroy(f->pair);
}

static void report(int before, const char *row, moq_version_t version)
{
    printf("%s: owed output %s draft=%llu\n", failures == before ? "PASS" : "FAIL",
           row, (unsigned long long)version);
}

/* The obligation sequence on one subscribe-mode sender. */
static void run_sequence(moq_version_t version)
{
    int before = failures;
    fixture_t f;
    if (!setup(&f, version, (setup_t){ 0 })) goto done;
    moq_media_track_t *unsubscribed = add_track(&f, "a");
    moq_media_track_t *empty = add_track(&f, "e");
    moq_media_track_t *subscribed = add_track(&f, "v");
    if (!ready(&f)) goto done;
    MOQ_TEST_CHECK(!owed(&f));

    /* Retained for a joiner: queued and counted, not owed. */
    write_object(&f, unsubscribed, 1000, true);
    MOQ_TEST_CHECK(!owed(&f));
    drive(&f);
    MOQ_TEST_CHECK(!owed(&f));
    MOQ_TEST_CHECK(queued(&f) >= 1);

    /* Its END_OF_TRACK waits behind that retained media, held with it. */
    MOQ_TEST_CHECK_EQ_INT(moq_media_sender_end_track(f.sender, unsubscribed), MOQ_OK);
    MOQ_TEST_CHECK(!owed(&f));
    drive(&f);
    MOQ_TEST_CHECK(!owed(&f));
    MOQ_TEST_CHECK(queued(&f) >= 1);

    /* With nothing retained ahead of it, an END_OF_TRACK is owed although
     * nobody subscribes, until the pump hands it over. */
    MOQ_TEST_CHECK_EQ_INT(moq_media_sender_end_track(f.sender, empty), MOQ_OK);
    MOQ_TEST_CHECK(owed(&f));
    for (int i = 0; i < 4; i++) drive(&f);
    MOQ_TEST_CHECK(!owed(&f));

    /* A subscribed track's object is owed until the pump sends it. */
    if (!subscribe(&f, 0, MOQ_MSF_CATALOG_TRACK_NAME, false) ||
        !subscribe(&f, 1, "v", false)) goto done;
    write_object(&f, subscribed, 2000, true);
    MOQ_TEST_CHECK(owed(&f));
    drive(&f);
    MOQ_TEST_CHECK(!owed(&f));
    MOQ_TEST_CHECK(f.objects[1] >= 1);

    /* A post-ready catalog change is owed until republished. */
    moq_media_track_t *late = add_track(&f, "late");
    MOQ_TEST_CHECK(owed(&f));
    for (int i = 0; i < 4; i++) drive(&f);
    MOQ_TEST_CHECK(!owed(&f));

    /* A removed track is owed until torn down. */
    MOQ_TEST_CHECK_EQ_INT(moq_media_sender_remove_track(f.sender, late), MOQ_OK);
    MOQ_TEST_CHECK(owed(&f));
    for (int i = 0; i < 4; i++) drive(&f);
    MOQ_TEST_CHECK(!owed(&f));

    /* A live->VOD conversion's PUBLISH_DONE is owed until sent. */
    moq_media_vod_track_t vod = { subscribed, 30000 };
    MOQ_TEST_CHECK_EQ_INT(moq_media_sender_convert_to_vod(f.sender, &vod, 1), MOQ_OK);
    MOQ_TEST_CHECK(owed(&f));
    for (int i = 0; i < 6; i++) drive(&f);
    MOQ_TEST_CHECK(!owed(&f));
    MOQ_TEST_CHECK(!moq_media_sender_is_fatal(f.sender));
done:
    teardown(&f);
    report(before, "sequence", version);
}

/* Media and its END held for a never-subscribed track resume when demand
 * arrives: the media first, then exactly one END_OF_TRACK. */
static void run_held_end_resumes(moq_version_t version)
{
    int before = failures;
    fixture_t f;
    if (!setup(&f, version, (setup_t){ 0 })) goto done;
    moq_media_track_t *t = add_track(&f, "r");
    if (!ready(&f)) goto done;
    write_object(&f, t, 1000, true);
    write_object(&f, t, 2000, false);
    MOQ_TEST_CHECK_EQ_INT(moq_media_sender_end_track(f.sender, t), MOQ_OK);
    drive(&f);
    MOQ_TEST_CHECK(!owed(&f));
    MOQ_TEST_CHECK_EQ_U64(queued(&f), 2);

    /* Demand: the held media is owed again and drains, then its END. */
    if (!subscribe(&f, 0, "r", true)) goto done;
    for (int i = 0; i < 6; i++) drive(&f);
    MOQ_TEST_CHECK(!owed(&f));
    MOQ_TEST_CHECK_EQ_U64(queued(&f), 0);
    MOQ_TEST_CHECK_EQ_INT(f.objects[0], 2);
    MOQ_TEST_CHECK_EQ_INT(f.ends[0], 1);
    for (int i = 0; i < 4; i++) drive(&f);
    MOQ_TEST_CHECK_EQ_INT(f.ends[0], 1);          /* never a second END */
done:
    teardown(&f);
    report(before, "held_end_resumes", version);
}

/* An accepted PUBLISH the subscriber then updates to Forward 0 has no
 * demand but is established: an END queued behind its retained media stays
 * owed, pump after pump, so a drain stays pending rather than reporting
 * completion. At Forward 1 the same sequence hands everything over. */
static void run_publication_forward(moq_version_t version, bool forward)
{
    int before = failures;
    fixture_t f;
    if (!setup(&f, version, (setup_t){ .publish_tracks = true,
                                       .accept_forward = forward })) goto done;
    moq_media_track_t *t = add_track(&f, "p");
    if (!ready(&f)) goto done;
    MOQ_TEST_CHECK_EQ_INT(f.publishes, 1);
    if (!forward) {
        moq_publication_update_cfg_t ucfg;
        moq_publication_update_cfg_init(&ucfg);
        ucfg.has_forward = true;
        ucfg.forward = false;
        MOQ_TEST_CHECK_EQ_INT(moq_session_update_publication(
            moq_simpair_server(f.pair), f.publication, &ucfg, f.now_us), MOQ_OK);
        for (int i = 0; i < 4; i++) drive(&f);
    }
    write_object(&f, t, 1000, true);
    drive(&f);
    /* Forward 0 is in effect: the object is retained, not delivered. */
    MOQ_TEST_CHECK_EQ_U64(queued(&f), forward ? 0 : 1);
    MOQ_TEST_CHECK_EQ_INT(moq_media_sender_end_track(f.sender, t), MOQ_OK);
    MOQ_TEST_CHECK(owed(&f));
    for (int i = 0; i < 6; i++) drive(&f);
    if (forward) {
        MOQ_TEST_CHECK(!owed(&f));
        MOQ_TEST_CHECK_EQ_U64(queued(&f), 0);
    } else {
        MOQ_TEST_CHECK(owed(&f));
        MOQ_TEST_CHECK_EQ_U64(queued(&f), 1);
    }
    MOQ_TEST_CHECK(!moq_media_sender_is_fatal(f.sender));
done:
    teardown(&f);
    report(before, forward ? "publication_forward1" : "publication_forward0",
           version);
}

/* A group emitted to a subscriber who then leaves, continued while nobody
 * subscribes under drop_without_demand: the dropped continuation abandons
 * the open subgroup. The pump that drops it arms and normally sends its
 * reset at once; refused, the reset stays owed until a later pump sends it,
 * and the group is counted abandoned exactly once. */
static void run_abandoned_group_reset(moq_version_t version)
{
    int before = failures;
    fixture_t f;
    if (!setup(&f, version, (setup_t){ .drop_without_demand = true })) goto done;
    moq_media_track_t *t = add_track(&f, "g");
    if (!ready(&f)) goto done;
    if (!subscribe(&f, 0, "g", false)) goto done;
    write_object(&f, t, 1000, true);
    drive(&f);
    MOQ_TEST_CHECK_EQ_INT(f.objects[0], 1);
    MOQ_TEST_CHECK(!owed(&f));
    unsubscribe(&f, 0);
    moq_media_sender_test_refuse_reset(f.sender, 1);
    write_object(&f, t, 2000, false);          /* continues the open group */
    drive(&f);
    moq_media_sender_stats_t st; memset(&st, 0, sizeof st);
    MOQ_TEST_CHECK_EQ_INT(moq_media_sender_get_stats(f.sender, &st, sizeof st), MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(st.objects_dropped, 1);
    MOQ_TEST_CHECK_EQ_U64(st.groups_abandoned, 0);   /* refused, still owed */
    MOQ_TEST_CHECK(owed(&f));
    for (int i = 0; i < 4; i++) drive(&f);
    MOQ_TEST_CHECK(!owed(&f));
    MOQ_TEST_CHECK_EQ_INT(moq_media_sender_get_stats(f.sender, &st, sizeof st), MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(st.groups_abandoned, 1);
    MOQ_TEST_CHECK(!moq_media_sender_is_fatal(f.sender));
done:
    teardown(&f);
    report(before, "abandoned_group_reset", version);
}

/* A removed track's teardown refused once: the catalog republish still
 * commits in that pump, and the removal stays owed until the retry tears
 * the track down. */
static void run_removed_retry(moq_version_t version)
{
    int before = failures;
    fixture_t f;
    if (!setup(&f, version, (setup_t){ 0 })) goto done;
    moq_media_track_t *t = add_track(&f, "x");
    if (!ready(&f)) goto done;
    if (!subscribe(&f, 0, MOQ_MSF_CATALOG_TRACK_NAME, false) ||
        !subscribe(&f, 1, "x", false)) goto done;
    write_object(&f, t, 1000, true);
    drive(&f);
    MOQ_TEST_CHECK(!owed(&f));
    moq_media_sender_test_refuse_teardown_end(f.sender, 1);
    MOQ_TEST_CHECK_EQ_INT(moq_media_sender_remove_track(f.sender, t), MOQ_OK);
    unsigned catalog_before = f.objects[0];
    drive(&f);                                  /* teardown refused */
    MOQ_TEST_CHECK(f.objects[0] > catalog_before); /* republish committed */
    MOQ_TEST_CHECK(owed(&f));
    for (int i = 0; i < 4; i++) drive(&f);      /* the retry tears it down */
    MOQ_TEST_CHECK(!owed(&f));
    MOQ_TEST_CHECK(!moq_media_sender_is_fatal(f.sender));
done:
    teardown(&f);
    report(before, "removed_retry", version);
}

int main(void)
{
    const moq_version_t versions[] = { MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18 };
    for (size_t i = 0; i < sizeof(versions) / sizeof(versions[0]); i++) {
        run_sequence(versions[i]);
        run_held_end_resumes(versions[i]);
        run_publication_forward(versions[i], false);
        run_publication_forward(versions[i], true);
        run_abandoned_group_reset(versions[i]);
        run_removed_retry(versions[i]);
    }
    return failures ? 1 : 0;
}
