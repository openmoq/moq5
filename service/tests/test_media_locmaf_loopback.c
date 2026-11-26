/*
 * LOCMAF tracks (draft-einarsson-moq-locmaf-01) through the service tier
 * over a real threaded-picoquic loopback, for draft-16 and draft-18 each
 * pinned exactly. Media endpoints are clients and there is no relay in this
 * tree, so each half meets a raw-session test server:
 *
 *  - Sender: a moq_media_sender writes CMAF chunks on a "locmaf" track; the
 *    server subscribes and checks every Object it receives: dense Locations,
 *    a full header opening each Group and deltas after it, and a library
 *    decoder rebuilding each source chunk byte for byte, then END_OF_TRACK.
 *  - Receiver: the server publishes a "locmaf" catalog entry and Objects a
 *    library encoder made, its first Groups before anyone subscribes; an
 *    auto-subscribing moq_media_receiver joins and must deliver, from the
 *    first Group it joins at its start, every Object as the canonical CMAF
 *    chunk it encodes, with no loss, then TRACK_ENDED.
 */
#include <moq/locmaf.h>
#include <moq/media_receiver.h>
#include <moq/media_sender.h>
#include <moq/msf.h>
#include <moq/picoquic_threaded.h>
#include <moq/publisher.h>
#include <moq/session.h>
#include "media_builders.h"
#include "test_support.h"

#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int failures = 0;

static const char *vname(moq_version_t v)
{
    return v == MOQ_VERSION_DRAFT_18 ? "d18" : "d16";
}

#define GROUPS     4
#define PER_GROUP  4
#define EARLY      2      /* Groups the receiver half publishes before a subscriber */
#define MAX_OBJ    256u
#define MAX_OBJS   64

/* -- The track -------------------------------------------------------------- */

/* track_ID 1, timescale 48000, trex sdi 1 / duration 1024 / size 0 / flags
 * 0x02000000. */
static size_t init_bytes(uint8_t *out)
{
    static const uint32_t trex4[4] = { 1, 1024, 0, 0x02000000u };
    return moq_test_build_cmaf_init_trex(out, 1, 48000, trex4, 1, true);
}

static bool track_facts(moq_locmaf_track_t *facts)
{
    uint8_t init[512];
    moq_cmaf_init_info_t ii;
    moq_cmaf_init_info_init_sized(&ii, sizeof(ii));
    moq_bytes_t b = { init, init_bytes(init) };
    return moq_cmaf_parse_init_sized(b, &ii, sizeof(ii)) == MOQ_OK &&
           moq_locmaf_track_from_cmaf(&ii, facts, sizeof(*facts)) == MOQ_OK;
}

/* The canonical chunk of Object k of the Group with index g: one sample
 * whose size varies, sync at the Group's start. */
static size_t chunk(const moq_locmaf_track_t *facts, uint64_t g, uint64_t k,
                    uint8_t *out, size_t cap)
{
    uint8_t payload[16];
    uint32_t size = 4u + (uint32_t)((g + k) % 5u);
    for (uint32_t i = 0; i < size; i++) payload[i] = (uint8_t)(g * 13u + k * 5u + i);
    moq_locmaf_sample_t s;
    memset(&s, 0, sizeof(s));
    s.duration = 1024;
    s.size = size;
    s.flags = k == 0 ? 0x02000000u : 0x01010000u;
    moq_locmaf_chunk_t c;
    memset(&c, 0, sizeof(c));
    c.struct_size = sizeof(c);
    c.kind = MOQ_LOCMAF_OBJECT_FULL;
    c.base_media_decode_time = (g * 100u + k) * 1024u;
    c.sample_description_index = 1;
    c.sample_count = 1;
    c.samples = &s;
    c.mdat_payload = (moq_bytes_t){ payload, size };
    size_t len = 0;
    if (moq_locmaf_write_cmaf(facts, &c, out, cap, &len) != MOQ_OK) return 0;
    return len;
}

/* -- Shared server plumbing ----------------------------------------------- */

typedef int (*pump_fn_t)(moq_pq_threaded_t *, moq_pq_threaded_lane_t *,
                         uint64_t, void *);

static moq_pq_threaded_t *start_server(const char *cert, const char *key,
                                       pump_fn_t pump, void *ctx,
                                       int *out_port)
{
    /* A port range disjoint from the other network tests. */
    static int calls = 0;
    int base = 23500 + (int)(getpid() % 997) + (calls++ * 131);
    static const char *const alpn[] = { "moqt-18", "moqt-16" };
    for (int attempt = 0; attempt < 8; attempt++) {
        moq_pq_threaded_cfg_t cfg;
        moq_pq_threaded_cfg_init_sized(&cfg, sizeof(cfg));
        cfg.alloc = moq_alloc_default();
        cfg.perspective = MOQ_PERSPECTIVE_SERVER;
        cfg.cert_path = cert;
        cfg.key_path = key;
        cfg.port = base + attempt * 13;
        cfg.send_request_capacity = true;
        cfg.initial_request_capacity = 16;
        cfg.on_lane_pump = pump;
        cfg.on_lane_pump_ctx = ctx;
        /* The server picks the first of these the client offers; each client
         * offers exactly one draft. */
        cfg.alpn_list = alpn;
        cfg.alpn_count = 2;
        moq_pq_threaded_t *srv = NULL;
        if (moq_pq_threaded_create(&cfg, &srv) == MOQ_OK) {
            *out_port = cfg.port;
            return srv;
        }
    }
    return NULL;
}

static void stop_server(moq_pq_threaded_t *srv)
{
    if (!srv) return;
    moq_pq_threaded_stop(srv);
    moq_pq_threaded_destroy(srv);
}

/* An endpoint cfg for the loopback server, offering exactly `ver`. */
static moq_endpoint_cfg_t ep_cfg(char *url, size_t cap, int port,
                                 const moq_version_t *ver)
{
    moq_endpoint_cfg_t c;
    moq_endpoint_cfg_init_sized(&c, sizeof(c));
    snprintf(url, cap, "moqt://127.0.0.1:%d", port);
    c.url = (moq_bytes_t){ (const uint8_t *)url, strlen(url) };
    c.insecure_skip_verify = true;
    c.versions.struct_size = sizeof(moq_version_offer_t);
    c.versions.policy = MOQ_VERSION_POLICY_EXACT;
    c.versions.versions = ver;
    c.versions.version_count = 1;
    return c;
}

static moq_bytes_t g_ns[2] = { { (const uint8_t *)"svc", 3 },
                               { (const uint8_t *)"lm", 2 } };

/* -- Sender half -------------------------------------------------------------- */

typedef struct seen {
    uint64_t group, object;
    uint8_t  bytes[MAX_OBJ];
    size_t   len;
} seen_t;

typedef struct snd_srv {
    pthread_mutex_t    mu;
    bool               ns_ok;
    bool               sub_issued;
    moq_subscription_t sub;
    seen_t             seen[MAX_OBJS];
    int                objects;
    bool               eog_ok;
    atomic_int         eot;
} snd_srv_t;

static snd_srv_t g_snd;

static int snd_pump(moq_pq_threaded_t *t, moq_pq_threaded_lane_t *lane,
                    uint64_t now_us, void *ctx)
{
    (void)lane;
    snd_srv_t *st = (snd_srv_t *)ctx;
    moq_session_t *s = moq_pq_threaded_session(t);
    if (!s || moq_session_state(s) != MOQ_SESS_ESTABLISHED) return 0;
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) == 1) {
        if (ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED) {
            moq_accept_namespace_cfg_t acc;
            moq_accept_namespace_cfg_init(&acc);
            (void)moq_session_accept_namespace(s, ev.u.namespace_published.ann,
                                               &acc, now_us);
            st->ns_ok = true;
        } else if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED && st->sub_issued &&
                   moq_subscription_eq(ev.u.object_received.sub, st->sub)) {
            const moq_object_received_event_t *o = &ev.u.object_received;
            pthread_mutex_lock(&st->mu);
            if (o->status == MOQ_OBJECT_END_OF_TRACK) {
                atomic_fetch_add(&st->eot, 1);
            } else if (o->status == MOQ_OBJECT_NORMAL && o->payload) {
                size_t n = moq_rcbuf_len(o->payload);
                if (!o->end_of_group) st->eog_ok = false;
                if (st->objects < MAX_OBJS && n <= MAX_OBJ) {
                    seen_t *w = &st->seen[st->objects++];
                    w->group = o->group_id;
                    w->object = o->object_id;
                    w->len = n;
                    memcpy(w->bytes, moq_rcbuf_data(o->payload), n);
                } else {
                    st->eog_ok = false;   /* over the capture bound */
                }
            }
            pthread_mutex_unlock(&st->mu);
        }
        moq_event_cleanup(&ev);
    }
    if (st->ns_ok && !st->sub_issued) {
        moq_subscribe_cfg_t sc;
        moq_subscribe_cfg_init(&sc);
        sc.track_namespace = (moq_namespace_t){ g_ns, 2 };
        sc.track_name = (moq_bytes_t){ (const uint8_t *)"v", 1 };
        sc.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        if (moq_session_subscribe(s, &sc, now_us, &st->sub) == MOQ_OK)
            st->sub_issued = true;
    }
    return 0;
}

/* Write one chunk, waiting out backpressure. */
static bool snd_write(moq_media_sender_t *s, moq_media_track_t *t,
                      const uint8_t *b, size_t n, bool starts)
{
    for (int i = 0; i < 1000; i++) {
        moq_rcbuf_t *p = NULL;
        if (moq_rcbuf_create(moq_alloc_default(), b, n, &p) != MOQ_OK) return false;
        moq_media_send_object_t o;
        memset(&o, 0, sizeof(o));
        o.struct_size = sizeof(o);
        o.payload = p;
        o.is_sync = starts;
        o.starts_group = starts;
        moq_result_t rc = moq_media_sender_write(s, t, &o);
        if (rc == MOQ_OK) return true;
        moq_rcbuf_decref(p);
        if (rc != MOQ_ERR_WOULD_BLOCK) {
            fprintf(stderr, "write: %d\n", (int)rc);
            return false;
        }
        (void)moq_media_sender_wait(s, 100000);
    }
    return false;
}

static void test_sender_loopback(const char *cert, const char *key, moq_version_t ver)
{
    int before = failures;
    moq_locmaf_track_t facts;
    moq_locmaf_track_init(&facts);
    MOQ_TEST_CHECK(track_facts(&facts));

    memset(&g_snd, 0, sizeof(g_snd));
    pthread_mutex_init(&g_snd.mu, NULL);
    g_snd.eog_ok = true;
    int port = 0;
    moq_pq_threaded_t *srv = start_server(cert, key, snd_pump, &g_snd, &port);
    if (!srv) { failures++; return; }

    char url[64];
    moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), port, &ver);
    moq_media_sender_cfg_t cfg;
    moq_media_sender_cfg_init_live_sized(&cfg, sizeof(cfg));
    cfg.namespace_ = (moq_namespace_t){ g_ns, 2 };
    cfg.endpoint = &ec;
    cfg.backpressure = MOQ_MEDIA_SEND_BP_BLOCK_TIMEOUT;
    moq_media_sender_t *s = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s), (int)MOQ_OK);
    if (!s) { stop_server(srv); failures++; return; }

    uint8_t init[512];
    moq_media_track_cfg_t tc;
    moq_media_track_cfg_init(&tc);
    tc.name = (moq_bytes_t){ (const uint8_t *)"v", 1 };
    tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
    tc.packaging = MOQ_MEDIA_PACKAGING_LOCMAF;
    tc.codec = (moq_bytes_t){ (const uint8_t *)"avc1.64001f", 11 };
    tc.bitrate = 1000000;
    tc.timescale = 48000;
    tc.init_data = (moq_bytes_t){ init, init_bytes(init) };
    moq_media_track_t *t = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(s, &tc, &t), (int)MOQ_OK);

    for (int i = 0; i < 200 && !(t && moq_media_sender_track_has_subscriber(s, t)); i++)
        (void)moq_media_sender_wait(s, 50000);
    MOQ_TEST_CHECK(t && moq_media_sender_track_has_subscriber(s, t));
    bool wrote = t && moq_media_sender_track_has_subscriber(s, t);
    for (uint64_t g = 0; wrote && g < GROUPS; g++)
        for (uint64_t k = 0; wrote && k < PER_GROUP; k++) {
            uint8_t c[MAX_OBJ];
            size_t n = chunk(&facts, g, k, c, sizeof(c));
            wrote = n > 0 && snd_write(s, t, c, n, k == 0);
        }
    MOQ_TEST_CHECK(wrote);
    for (int i = 0; wrote && i < 1000; i++) {
        moq_result_t rc = moq_media_sender_end_track(s, t);
        if (rc == MOQ_OK) break;
        if (rc != MOQ_ERR_WOULD_BLOCK) { MOQ_TEST_CHECK(rc == MOQ_OK); break; }
        (void)moq_media_sender_wait(s, 100000);
    }
    for (int i = 0; i < 200 && atomic_load(&g_snd.eot) == 0; i++) usleep(20000);

    pthread_mutex_lock(&g_snd.mu);
    MOQ_TEST_CHECK_EQ_INT(atomic_load(&g_snd.eot), 1);
    MOQ_TEST_CHECK(g_snd.eog_ok);
    MOQ_TEST_CHECK_EQ_INT(g_snd.objects, GROUPS * PER_GROUP);
    /* Location order: Groups ride separate streams, so a later Group can
     * arrive before the tail of an earlier one. */
    int idx[MAX_OBJS];
    int n = g_snd.objects;
    for (int i = 0; i < n; i++) idx[i] = i;
    for (int i = 1; i < n; i++)
        for (int j = i; j > 0; j--) {
            const seen_t *a = &g_snd.seen[idx[j - 1]], *b = &g_snd.seen[idx[j]];
            bool gt = a->group > b->group || (a->group == b->group && a->object > b->object);
            if (!gt) break;
            int x = idx[j - 1]; idx[j - 1] = idx[j]; idx[j] = x;
        }
    moq_locmaf_decoder_cfg_t dc;
    moq_locmaf_decoder_cfg_init(&dc);
    dc.track = &facts;
    size_t bytes = 0;
    MOQ_TEST_CHECK(moq_locmaf_decoder_storage_size(&dc, &bytes) == MOQ_OK);
    void *mem = malloc(bytes);
    dc.storage = mem;
    dc.storage_size = bytes;
    moq_locmaf_decoder_t d;
    MOQ_TEST_CHECK(mem && moq_locmaf_decoder_init(&d, sizeof(d), &dc) == MOQ_OK);
    bool dense = true, headers = true, rebuilt = true;
    uint64_t g0 = n > 0 ? g_snd.seen[idx[0]].group : 0;
    for (int i = 0; mem && i < n; i++) {
        const seen_t *w = &g_snd.seen[idx[i]];
        uint64_t gi = (uint64_t)i / PER_GROUP, k = (uint64_t)i % PER_GROUP;
        if (w->group != g0 + gi || w->object != k) dense = false;
        if (w->len == 0 || w->bytes[0] != (k == 0 ? MOQ_LOCMAF_ELEMENT_FULL_HEADER
                                                  : MOQ_LOCMAF_ELEMENT_DELTA_HEADER))
            headers = false;
        moq_locmaf_chunk_t c;
        uint8_t back[MAX_OBJ], want[MAX_OBJ];
        size_t blen = 0, wlen = chunk(&facts, gi, k, want, sizeof(want));
        if (moq_locmaf_decoder_push(&d, w->group, w->object,
                                    (moq_bytes_t){ w->bytes, w->len }, &c, sizeof(c),
                                    NULL) != MOQ_OK ||
            moq_locmaf_write_cmaf(&facts, &c, back, sizeof(back), &blen) != MOQ_OK ||
            blen != wlen || memcmp(back, want, wlen) != 0 || w->len >= wlen)
            rebuilt = false;
    }
    free(mem);
    MOQ_TEST_CHECK(dense && headers && rebuilt);
    if (failures != before) {
        fprintf(stderr, "sender %s: %d Objects:", vname(ver), g_snd.objects);
        for (int i = 0; i < g_snd.objects; i++)
            fprintf(stderr, " (%llu,%llu)/%zu", (unsigned long long)g_snd.seen[i].group,
                    (unsigned long long)g_snd.seen[i].object, g_snd.seen[i].len);
        fprintf(stderr, "\n");
    }
    pthread_mutex_unlock(&g_snd.mu);
    moq_media_sender_track_stats_t st;
    if (t && moq_media_sender_get_track_stats(s, t, &st, sizeof(st)) == MOQ_OK) {
        MOQ_TEST_CHECK(st.objects_written == st.objects_sent);
        MOQ_TEST_CHECK_EQ_U64(st.objects_dropped, 0);
    }
    moq_media_sender_destroy(s);
    stop_server(srv);
    pthread_mutex_destroy(&g_snd.mu);
    if (failures == before) printf("PASS: locmaf_loopback_sender_%s\n", vname(ver));
}

/* -- Receiver half ---------------------------------------------------------- */

typedef struct rcv_srv {
    moq_publisher_t *pub;
    moq_pub_track_t *cat, *trk;
    char             catalog[2048];
    seen_t           objs[GROUPS * PER_GROUP];   /* LOCMAF Objects, in order */
    int              published;
    atomic_bool      ended;
    bool             failed;
} rcv_srv_t;

static rcv_srv_t g_rcv;

static void b64(const uint8_t *in, size_t n, char *out)
{
    static const char tab[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    size_t o = 0;
    for (size_t i = 0; i < n; i += 3) {
        uint32_t v = (uint32_t)in[i] << 16;
        if (i + 1 < n) v |= (uint32_t)in[i + 1] << 8;
        if (i + 2 < n) v |= in[i + 2];
        out[o++] = tab[(v >> 18) & 63];
        out[o++] = tab[(v >> 12) & 63];
        out[o++] = i + 1 < n ? tab[(v >> 6) & 63] : '=';
        out[o++] = i + 2 < n ? tab[v & 63] : '=';
    }
    out[o] = '\0';
}

/* The catalog, and every Object the server will publish, encoded by a
 * library encoder. */
static bool rcv_prepare(rcv_srv_t *st, const moq_locmaf_track_t *facts)
{
    uint8_t init[512];
    char enc[800];
    b64(init, init_bytes(init), enc);
    snprintf(st->catalog, sizeof(st->catalog),
             "{\"version\":\"draft-01\",\"initDataList\":[{\"id\":\"i\","
             "\"type\":\"inline\",\"data\":\"%s\"}],\"tracks\":[{\"name\":\"v\","
             "\"packaging\":\"locmaf\",\"locmafVersion\":\"%s\",\"isLive\":true,"
             "\"role\":\"video\",\"codec\":\"avc1.64001f\",\"bitrate\":1000000,"
             "\"initRef\":\"i\"}]}",
             enc, MOQ_MSF_LOCMAF_VERSION);
    moq_locmaf_encoder_cfg_t ec;
    moq_locmaf_encoder_cfg_init(&ec);
    ec.track = facts;
    size_t bytes = 0;
    if (moq_locmaf_encoder_storage_size(&ec, &bytes) != MOQ_OK) return false;
    void *mem = malloc(bytes);
    if (!mem) return false;
    ec.storage = mem;
    ec.storage_size = bytes;
    moq_locmaf_encoder_t e;
    bool ok = moq_locmaf_encoder_init(&e, sizeof(e), &ec) == MOQ_OK;
    for (uint64_t g = 0; ok && g < GROUPS; g++)
        for (uint64_t k = 0; ok && k < PER_GROUP; k++) {
            uint8_t c[MAX_OBJ];
            size_t n = chunk(facts, g, k, c, sizeof(c));
            seen_t *o = &st->objs[g * PER_GROUP + k];
            o->group = g;
            o->object = k;
            ok = n > 0 && moq_locmaf_encode_chunk(&e, g, k, (moq_bytes_t){ c, n },
                                                  o->bytes, sizeof(o->bytes),
                                                  &o->len, NULL) == MOQ_OK;
            if (ok) moq_locmaf_encoder_commit(&e);
        }
    free(mem);
    return ok;
}

static int rcv_pump(moq_pq_threaded_t *t, moq_pq_threaded_lane_t *lane,
                    uint64_t now_us, void *ctx)
{
    (void)lane;
    rcv_srv_t *st = (rcv_srv_t *)ctx;
    moq_session_t *s = moq_pq_threaded_session(t);
    if (!s || st->failed) return 0;
    if (!st->pub) {
        moq_pub_cfg_t pc;
        moq_pub_cfg_init_sized(&pc, sizeof(pc));
        pc.accept_mode = MOQ_PUB_ACCEPT_ALL;
        if (moq_pub_create(s, moq_alloc_default(), &pc, &st->pub) != MOQ_OK) {
            st->failed = true;
            return -1;
        }
        moq_pub_track_cfg_t tc;
        moq_pub_track_cfg_init_sized(&tc, sizeof(tc));
        tc.track_namespace = (moq_namespace_t){ g_ns, 2 };
        tc.track_name = MOQ_BYTES_LITERAL(MOQ_MSF_CATALOG_TRACK_NAME);
        if (moq_pub_add_track(st->pub, &tc, now_us, &st->cat) != MOQ_OK) {
            st->failed = true;
            return -1;
        }
        tc.track_name = (moq_bytes_t){ (const uint8_t *)"v", 1 };
        if (moq_pub_add_track(st->pub, &tc, now_us, &st->trk) != MOQ_OK) {
            st->failed = true;
            return -1;
        }
        moq_rcbuf_t *json = NULL;
        moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)st->catalog,
                         strlen(st->catalog), &json);
        moq_pub_retained_object_t ro = { .object_id = 0, .payload = json };
        moq_pub_retained_group_cfg_t rg;
        moq_pub_retained_group_cfg_init(&rg);
        rg.group_id = 0;
        rg.objects = &ro;
        rg.object_count = 1;
        moq_result_t rc = moq_pub_set_retained_group(st->pub, st->cat, &rg);
        moq_rcbuf_decref(json);
        if (rc != MOQ_OK) { st->failed = true; return -1; }
    }
    (void)moq_pub_tick(st->pub, now_us);
    bool sub = moq_pub_has_subscriber(st->pub, st->trk);
    /* The early Groups go out with no subscriber, as a live track's would;
     * the rest once the receiver has joined. */
    while (st->published < GROUPS * PER_GROUP) {
        const seen_t *o = &st->objs[st->published];
        if (!sub && o->group >= EARLY) break;
        moq_rcbuf_t *b = NULL;
        moq_rcbuf_create(moq_alloc_default(), o->bytes, o->len, &b);
        moq_pub_object_cfg_t oc;
        moq_pub_object_cfg_init_sized(&oc, sizeof(oc));
        oc.group_id = o->group;
        oc.object_id = o->object;
        oc.payload = b;
        oc.end_of_group = true;
        moq_result_t rc = moq_pub_write_object_ex(st->pub, st->trk, &oc, now_us);
        moq_rcbuf_decref(b);
        if (rc == MOQ_ERR_WOULD_BLOCK || rc == MOQ_ERR_WRONG_STATE) break;
        if (rc != MOQ_OK) { st->failed = true; return -1; }
        st->published++;
    }
    if (st->published == GROUPS * PER_GROUP && sub && !atomic_load(&st->ended)) {
        moq_result_t rc = moq_pub_end_track(st->pub, st->trk, now_us);
        if (rc == MOQ_OK) atomic_store(&st->ended, true);
        else if (rc != MOQ_ERR_WOULD_BLOCK) { st->failed = true; return -1; }
    }
    (void)moq_pub_tick(st->pub, now_us);
    return 0;
}

static void test_receiver_loopback(const char *cert, const char *key, moq_version_t ver)
{
    int before = failures;
    moq_locmaf_track_t facts;
    moq_locmaf_track_init(&facts);
    memset(&g_rcv, 0, sizeof(g_rcv));
    if (!track_facts(&facts) || !rcv_prepare(&g_rcv, &facts)) { failures++; return; }
    int port = 0;
    moq_pq_threaded_t *srv = start_server(cert, key, rcv_pump, &g_rcv, &port);
    if (!srv) { failures++; return; }

    char url[64];
    moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), port, &ver);
    moq_media_receiver_cfg_t rc;
    moq_media_receiver_cfg_init_live_sized(&rc, sizeof(rc));
    rc.namespace_ = (moq_namespace_t){ g_ns, 2 };
    rc.endpoint = &ec;
    rc.auto_subscribe = true;
    rc.overflow.policy = MOQ_MEDIA_OVERFLOW_FLOW_CONTROL;
    rc.overflow.max_objects = 256;
    moq_media_receiver_t *r = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_create(&rc, &r), (int)MOQ_OK);
    if (!r) { stop_server(srv); failures++; return; }

    int objects = 0, ended = 0, bad = 0;
    uint64_t first_group = UINT64_MAX;
    bool first_ok = false, desc_ok = false;
    for (int i = 0; i < 400 && !ended; i++) {
        (void)moq_media_receiver_wait(r, 25000);
        moq_media_track_event_t ev;
        while (moq_media_receiver_poll_track(r, &ev, sizeof(ev)) == MOQ_OK) {
            if (ev.kind == MOQ_MEDIA_TRACK_ADDED && ev.desc)
                desc_ok = ev.desc->info.packaging == MOQ_MEDIA_PACKAGING_LOCMAF &&
                          ev.desc->cmaf != NULL;
            if (ev.kind == MOQ_MEDIA_TRACK_ENDED) ended++;
        }
        moq_media_object_t o;
        while (moq_media_receiver_poll_object(r, &o, sizeof(o)) == MOQ_OK) {
            if (o.status == MOQ_OBJECT_NORMAL) {
                if (objects == 0) {
                    first_group = o.group_id;
                    first_ok = o.object_id == 0;
                }
                uint8_t want[MAX_OBJ];
                size_t wn = chunk(&facts, o.group_id, o.object_id, want, sizeof(want));
                if (o.packaging != MOQ_MEDIA_PACKAGING_CMAF || o.payload.len != 0 ||
                    o.fragment.len != wn || memcmp(o.fragment.data, want, wn) != 0 ||
                    !o.has_loss_report || o.gap ||
                    o.group_id != first_group + (uint64_t)objects / PER_GROUP ||
                    o.object_id != (uint64_t)objects % PER_GROUP)
                    bad++;
                objects++;
            }
            moq_media_object_cleanup(&o);
        }
    }
    MOQ_TEST_CHECK(!g_rcv.failed);
    MOQ_TEST_CHECK(desc_ok);
    MOQ_TEST_CHECK_EQ_INT(ended, 1);
    /* The join lands on a Group start at or after the first live one, and
     * every Object from there on is delivered rebuilt. */
    MOQ_TEST_CHECK(first_ok && first_group >= EARLY && first_group < GROUPS);
    MOQ_TEST_CHECK_EQ_INT(bad, 0);
    if (first_group < GROUPS)
        MOQ_TEST_CHECK_EQ_INT(objects, (int)((GROUPS - first_group) * PER_GROUP));
    moq_media_receiver_stats_t rs;
    if (moq_media_receiver_get_stats(r, &rs, sizeof(rs)) == MOQ_OK) {
        MOQ_TEST_CHECK_EQ_U64(rs.locmaf_objects_rebuilt, (uint64_t)objects);
        MOQ_TEST_CHECK_EQ_U64(rs.locmaf_objects_undecodable, 0);
        MOQ_TEST_CHECK_EQ_U64(rs.locmaf_objects_expired + rs.locmaf_objects_late, 0);
        MOQ_TEST_CHECK_EQ_U64(rs.gaps_reported, 0);
        MOQ_TEST_CHECK_EQ_U64(rs.objects_dropped, 0);
    }
    moq_media_receiver_destroy(r);
    stop_server(srv);
    if (failures == before) printf("PASS: locmaf_loopback_receiver_%s\n", vname(ver));
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
    static const moq_version_t vers[2] = { MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18 };
    for (int v = 0; v < 2; v++) {
        test_sender_loopback(cert, key, vers[v]);
        test_receiver_loopback(cert, key, vers[v]);
    }
    if (failures) {
        printf("FAIL: %d locmaf loopback failures\n", failures);
        return 1;
    }
    printf("PASS: all locmaf loopback tests\n");
    return 0;
}
