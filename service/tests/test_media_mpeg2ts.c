/*
 * MPEG-2 TS through the media receiver, in-process over a SimPair, under
 * draft-16 and draft-18:
 *
 *   - eligibility: one delivery-path rule for auto-subscribe and
 *     subscribe_track, and the TS description (packaging, packet size,
 *     extension record, altGroup, PSI init data);
 *   - the reorder window, driven by a publisher facade that writes exactly
 *     the Objects, Group ends and resets each case needs: Location order,
 *     Group ends from the subgroup FIN, an unconfirmed tail after a reset,
 *     a rejected Object, the window deadline, END_OF_TRACK releasing what is
 *     held, local eviction and an app pause reported as gaps;
 *   - the media sender's byte stream end to end into the receiver: the
 *     delivered payloads are the source packets, every Group opens with its
 *     PSI and a random access point, no gaps, and the 192-to-188 and join-PSI
 *     transforms.
 *
 * In a build without moq::mpeg2ts only the eligibility cases run, with the
 * expectations of such a build.
 */
#include <moq/media_receiver.h>
#include <moq/media_sender.h>
#include <moq/publisher.h>
#include <moq/msf.h>
#include <moq/rcbuf.h>
#include <moq/sim.h>
#include <moq/session.h>
#include "test_support.h"
#include "media_sender_cmaf_init.h"
#include "ts_stream.h"
#include "media_builders.h"
#include "ts_desc_layout_probe.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>


static int failures = 0;

/* -- Test seams (media_receiver.c / media_sender.c, _TESTING) ----------- */
moq_media_receiver_t *moq_media_receiver_test_new(bool auto_subscribe);
moq_media_receiver_t *moq_media_receiver_test_new_cfg(
    const moq_media_receiver_cfg_t *cfg);
void moq_media_receiver_test_set_transport_version(moq_media_receiver_t *r,
                                                   moq_version_t v);
void moq_media_receiver_test_free(moq_media_receiver_t *r);
void moq_media_receiver_test_set_alloc(const moq_alloc_t *alloc);
void moq_media_receiver_test_ingest(moq_media_receiver_t *r, uint64_t group,
                                    uint64_t object, const char *json,
                                    size_t len);
void moq_media_receiver_test_pump(moq_media_receiver_t *r,
                                  moq_session_t *session, uint64_t now_us);
void moq_media_receiver_test_destroy_with_session(moq_media_receiver_t *r,
                                                  moq_session_t *session,
                                                  uint64_t now_us);
uint64_t moq_media_receiver_test_next_deadline_us(moq_media_receiver_t *r);
moq_media_sender_t *moq_media_sender_test_new_cfg(
    const moq_media_sender_cfg_t *cfg);
void moq_media_sender_test_pump(moq_media_sender_t *s, moq_session_t *session,
                                uint64_t now_us);
void moq_media_sender_test_free(moq_media_sender_t *s);
void moq_media_sender_test_set_alloc(const moq_alloc_t *alloc);
void moq_media_sender_test_set_ep(moq_media_sender_t *s, moq_endpoint_t *ep);
void moq_media_sender_test_set_ready(moq_media_sender_t *s);
moq_endpoint_t *moq_endpoint_test_make_bare(void);
void moq_endpoint_test_free_bare(moq_endpoint_t *ep);

static bool span_is(moq_bytes_t b, const char *s)
{
    return b.len == strlen(s) && memcmp(b.data, s, b.len) == 0;
}

/* -- Eligibility and the TS description (no network) -------------------- */

/* A TS track with init data (one PAT and one PMT packet, Base64), a TS
 * track whose record breaks the rules, an es-packets PMT track (a PSI
 * role), an unknown packaging, and a LOC control. */
static void make_catalog(char *out, size_t cap, const char *psi_b64)
{
    snprintf(out, cap,
        "{\"version\":1,"
        "\"initDataList\":[{\"id\":\"psi\",\"type\":\"inline\","
        "\"data\":\"%s\"}],"
        "\"tracks\":["
        "{\"name\":\"ts\",\"packaging\":\"mpeg2ts\",\"isLive\":true,"
        "\"mimeType\":\"video/mp2t\",\"bitrate\":4000000,\"altGroup\":3,"
        "\"mpeg2tsMode\":\"unmodified-program\",\"mpeg2tsPacketSize\":188,"
        "\"mpeg2tsProgramNumber\":1,\"mpeg2tsSiPids\":[17,18],"
        "\"initRef\":\"psi\"},"
        "{\"name\":\"bad\",\"packaging\":\"mpeg2ts\",\"isLive\":true,"
        "\"mpeg2tsMode\":\"unmodified-program\",\"mpeg2tsPacketSize\":204},"
        "{\"name\":\"pmt\",\"packaging\":\"mpeg2ts\",\"isLive\":true,"
        "\"role\":\"pmt\",\"mpeg2tsMode\":\"es-packets\","
        "\"mpeg2tsPacketSize\":188,\"mpeg2tsEsPid\":256},"
        "{\"name\":\"odd\",\"packaging\":\"weird\",\"isLive\":true,"
        "\"role\":\"video\",\"codec\":\"avc1.42e01e\"},"
        "{\"name\":\"loc\",\"packaging\":\"loc\",\"isLive\":true,"
        "\"role\":\"video\",\"codec\":\"avc1.42e01e\"}]}",
        psi_b64);
}

static void b64(const uint8_t *in, size_t n, char *out)
{
    static const char T[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    size_t o = 0;
    for (size_t i = 0; i < n; i += 3) {
        uint32_t v = (uint32_t)in[i] << 16;
        if (i + 1 < n) v |= (uint32_t)in[i + 1] << 8;
        if (i + 2 < n) v |= in[i + 2];
        out[o++] = T[(v >> 18) & 63];
        out[o++] = T[(v >> 12) & 63];
        out[o++] = i + 1 < n ? T[(v >> 6) & 63] : '=';
        out[o++] = i + 2 < n ? T[v & 63] : '=';
    }
    out[o] = 0;
}

static void test_eligibility(void)
{
    int before = failures;
    ts_gen_t g;
    ts_gen_init(&g, 188, 1, false);
    uint8_t psi[2 * 188];
    size_t psi_len = ts_gen_psi(&g, psi);
    static char psi_b64[2 * 188 * 2];
    b64(psi, psi_len, psi_b64);
    static char json[4096];
    make_catalog(json, sizeof(json), psi_b64);

    moq_media_receiver_t *r = moq_media_receiver_test_new(true);
    MOQ_TEST_CHECK(r != NULL);
    if (!r) return;
    moq_media_receiver_test_set_transport_version(r, MOQ_VERSION_DRAFT_16);
    moq_media_receiver_test_ingest(r, 0, 0, json, strlen(json));

    moq_media_track_t *ts = NULL, *bad = NULL, *pmt = NULL, *odd = NULL,
                      *loc = NULL;
    moq_media_track_event_t ev;
    while (moq_media_receiver_poll_track(r, &ev, sizeof(ev)) == MOQ_OK) {
        if (ev.kind != MOQ_MEDIA_TRACK_ADDED) continue;
        if (span_is(ev.desc->name, "ts")) ts = ev.track;
        if (span_is(ev.desc->name, "bad")) bad = ev.track;
        if (span_is(ev.desc->name, "pmt")) pmt = ev.track;
        if (span_is(ev.desc->name, "odd")) odd = ev.track;
        if (span_is(ev.desc->name, "loc")) loc = ev.track;
    }
    MOQ_TEST_CHECK(ts && bad && pmt && odd && loc);
    if (!(ts && bad && pmt && odd && loc)) {
        moq_media_receiver_test_free(r);
        return;
    }

    /* The TS description, whatever the build. */
    const moq_media_track_desc_t *d = moq_media_track_desc_get(ts);
    MOQ_TEST_CHECK_EQ_INT((int)d->info.packaging,
                          (int)MOQ_MEDIA_PACKAGING_MPEG2TS);
    MOQ_TEST_CHECK_EQ_INT((int)d->info.media_type, (int)MOQ_MEDIA_TYPE_VIDEO);
    MOQ_TEST_CHECK(d->has_alt_group && d->alt_group == 3);
    MOQ_TEST_CHECK(d->init_data.len == psi_len &&
                   memcmp(d->init_data.data, psi, psi_len) == 0);
    MOQ_TEST_CHECK_EQ_U64(d->init.codec_config.len, 0);
    const moq_msf_mpeg2ts_t *m =
        moq_msf_ext_mpeg2ts(d->extensions, d->extension_count);
    MOQ_TEST_CHECK(m != NULL);
    if (m) {
        MOQ_TEST_CHECK_EQ_U64(m->packet_size, 188);
        MOQ_TEST_CHECK(m->has_mode &&
                       m->mode == MOQ_MSF_MPEG2TS_MODE_UNMODIFIED_PROGRAM);
        MOQ_TEST_CHECK(span_is(m->mode_name, "unmodified-program"));
        MOQ_TEST_CHECK(m->has_program_number && m->program_number == 1);
        MOQ_TEST_CHECK(m->si_pid_count == 2 && m->si_pids[0] == 17 &&
                       m->si_pids[1] == 18);
    }
    /* A rejected TS track and a PSI-role track derive no info. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_track_desc_get(bad)->info.packaging, 0);
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_track_desc_get(pmt)->info.packaging, 0);
    MOQ_TEST_CHECK(moq_msf_ext_mpeg2ts(
        moq_media_track_desc_get(pmt)->extensions,
        moq_media_track_desc_get(pmt)->extension_count) != NULL);

    /* Auto-subscribe takes the media tracks with a delivery path. */
    moq_media_track_state_t st;
    MOQ_TEST_CHECK(moq_media_receiver_track_state(r, loc, &st) == MOQ_OK &&
                   st == MOQ_MEDIA_TRACK_STATE_PENDING);
    MOQ_TEST_CHECK(moq_media_receiver_track_state(r, odd, &st) == MOQ_OK &&
                   st == MOQ_MEDIA_TRACK_STATE_DISCOVERED);
    MOQ_TEST_CHECK(moq_media_receiver_track_state(r, bad, &st) == MOQ_OK &&
                   st == MOQ_MEDIA_TRACK_STATE_DISCOVERED);
    MOQ_TEST_CHECK(moq_media_receiver_track_state(r, pmt, &st) == MOQ_OK &&
                   st == MOQ_MEDIA_TRACK_STATE_DISCOVERED);
    /* subscribe_track refuses what has no delivery path, recording nothing. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_subscribe_track(r, odd, NULL),
                          (int)MOQ_ERR_UNSUPPORTED);
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_subscribe_track(r, bad, NULL),
                          (int)MOQ_ERR_UNSUPPORTED);
    MOQ_TEST_CHECK(moq_media_receiver_track_state(r, odd, &st) == MOQ_OK &&
                   st == MOQ_MEDIA_TRACK_STATE_DISCOVERED);
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_subscribe_track(r, loc, NULL),
                          (int)MOQ_OK);
#ifdef MOQ_TEST_HAVE_MPEG2TS
    MOQ_TEST_CHECK(moq_media_receiver_track_state(r, ts, &st) == MOQ_OK &&
                   st == MOQ_MEDIA_TRACK_STATE_PENDING);
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_subscribe_track(r, ts, NULL),
                          (int)MOQ_OK);
    /* The reference program is a 16-bit program number. */
    moq_media_receiver_track_subscribe_cfg_t sc;
    moq_media_receiver_track_subscribe_cfg_init_sized(&sc, sizeof(sc));
    sc.mpeg2ts_program_number = 0x10000;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_subscribe_track(r, ts, &sc),
                          (int)MOQ_ERR_INVAL);
#else
    /* This build cannot parse TS: the track is announced, never subscribed. */
    MOQ_TEST_CHECK(moq_media_receiver_track_state(r, ts, &st) == MOQ_OK &&
                   st == MOQ_MEDIA_TRACK_STATE_DISCOVERED);
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_subscribe_track(r, ts, NULL),
                          (int)MOQ_ERR_UNSUPPORTED);
#endif
    /* A v0-sized subscribe cfg is accepted; below it is refused. */
    moq_media_receiver_track_subscribe_cfg_t v0;
    memset(&v0, 0xA5, sizeof(v0));
    moq_media_receiver_track_subscribe_cfg_init(&v0);
    MOQ_TEST_CHECK_EQ_U64(v0.struct_size,
                          sizeof(subscribe_cfg_before_ts_t));
    for (size_t i = sizeof(subscribe_cfg_before_ts_t); i < sizeof(v0); i++)
        MOQ_TEST_CHECK(((const unsigned char *)&v0)[i] == 0xA5);
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_subscribe_track(r, loc, &v0),
                          (int)MOQ_OK);
    v0.struct_size = 4;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_subscribe_track(r, loc, &v0),
                          (int)MOQ_ERR_INVAL);
    moq_media_receiver_test_free(r);
    if (failures == before) printf("PASS: ts_eligibility\n");
}

/* The receiver cfg: the pointer-only init stamps the v0 prefix, a v0-sized
 * cfg is accepted, and an appended field is read only when wholly covered. */
static void test_receiver_cfg_abi(void)
{
    int before = failures;
    union { moq_media_receiver_cfg_t c; uint8_t b[sizeof(moq_media_receiver_cfg_t)]; } u;
    memset(&u, 0xA5, sizeof(u));
    moq_media_receiver_cfg_init_live(&u.c);
    MOQ_TEST_CHECK_EQ_U64(u.c.struct_size, sizeof(receiver_cfg_before_ts_t));
    bool intact = true;
    for (size_t i = sizeof(receiver_cfg_before_ts_t); i < sizeof(u); i++)
        if (u.b[i] != 0xA5) intact = false;
    MOQ_TEST_CHECK(intact);
    moq_bytes_t ns[1] = { { (const uint8_t *)"x", 1 } };
    u.c.namespace_ = (moq_namespace_t){ ns, 1 };
    moq_media_receiver_t *r = moq_media_receiver_test_new_cfg(&u.c);
    MOQ_TEST_CHECK(r != NULL);
    if (r) moq_media_receiver_test_free(r);
    moq_media_receiver_cfg_init_live_sized(&u.c, sizeof(u.c));
    u.c.namespace_ = (moq_namespace_t){ ns, 1 };
    u.c.struct_size = (uint32_t)(MOQ_MEDIA_RECEIVER_CFG_V0_SIZE - 1);
    MOQ_TEST_CHECK(moq_media_receiver_test_new_cfg(&u.c) == NULL);
    u.c.struct_size = (uint32_t)sizeof(u.c);
    r = moq_media_receiver_test_new_cfg(&u.c);
    MOQ_TEST_CHECK(r != NULL);
    if (r) moq_media_receiver_test_free(r);
    if (failures == before) printf("PASS: ts_receiver_cfg_abi\n");
}

#ifdef MOQ_TEST_HAVE_MPEG2TS

static const char *vname(moq_version_t v)
{
    return v == MOQ_VERSION_DRAFT_18 ? "d18" : "d16";
}

/* Drain track events; returns the handle of the track named `name` among
 * the ADDED ones (or NULL), and counts ENDED events for it. */
static moq_media_track_t *drain_events(moq_media_receiver_t *r,
                                       const char *name, int *ended,
                                       int *parse_drops)
{
    moq_media_track_t *found = NULL;
    moq_media_track_event_t ev;
    while (moq_media_receiver_poll_track(r, &ev, sizeof(ev)) == MOQ_OK) {
        if (ev.kind == MOQ_MEDIA_TRACK_ADDED && ev.desc &&
            span_is(ev.desc->name, name))
            found = ev.track;
        if (ev.kind == MOQ_MEDIA_TRACK_ENDED && ended) (*ended)++;
        if (ev.kind == MOQ_MEDIA_TRACK_PARSE_DROP && parse_drops)
            *parse_drops += (int)ev.parse_drops_delta;
    }
    return found;
}

/* -- A receiver fed by a publisher facade -------------------------------- */

typedef struct fx {
    moq_simpair_t        *sp;
    moq_session_t        *cl, *srv;
    moq_publisher_t      *pub;
    moq_pub_track_t      *cat, *ts;
    moq_media_receiver_t *r;
    moq_media_track_t    *track;
    uint64_t              now;
    int                   ended, parse_drops;
} fx_t;

static moq_bytes_t g_ns[2] = { { (const uint8_t *)"svc", 3 },
                               { (const uint8_t *)"ts", 2 } };

static const char FX_CATALOG[] =
    "{\"version\":1,\"tracks\":["
    "{\"name\":\"ts\",\"packaging\":\"mpeg2ts\",\"isLive\":true,"
    "\"bitrate\":4000000,\"mpeg2tsMode\":\"unmodified-program\","
    "\"mpeg2tsPacketSize\":188}]}";

static moq_simpair_t *make_pair(moq_version_t ver)
{
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = moq_alloc_default();
    cfg.seed = 7;
    cfg.initial_now_us = 1000;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 32;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 32;
    cfg.version = ver;
    moq_simpair_t *sp = NULL;
    if (moq_simpair_create(&cfg, &sp) != MOQ_OK) return NULL;
    moq_simpair_start(sp);
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    moq_event_t ev;
    while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
        moq_event_cleanup(&ev);
    while (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1)
        moq_event_cleanup(&ev);
    return sp;
}

static void fx_cycle(fx_t *f)
{
    moq_media_receiver_test_pump(f->r, f->cl, f->now);
    moq_simpair_run_until_quiescent(f->sp, 64, NULL);
    (void)moq_pub_tick(f->pub, f->now);
    moq_simpair_run_until_quiescent(f->sp, 64, NULL);
    moq_media_receiver_test_pump(f->r, f->cl, f->now);
}

/* Reorder slots per TS track for the next fx_setup (0 = the default), and
 * the receiver's allocator (NULL = the default). */
static uint32_t g_fx_slots;
static const moq_alloc_t *g_fx_alloc;

/* An allocator that refuses its next `g_refuse_next` requests. */
static int g_refuse_next;
static void *gate_alloc(size_t n, void *ctx)
{
    (void)ctx;
    if (g_refuse_next > 0) { g_refuse_next--; return NULL; }
    return malloc(n);
}
static void *gate_realloc(void *p, size_t o, size_t n, void *ctx)
{
    (void)o; (void)ctx;
    if (g_refuse_next > 0) { g_refuse_next--; return NULL; }
    return realloc(p, n);
}
static void gate_free(void *p, size_t n, void *ctx)
{
    (void)n; (void)ctx;
    free(p);
}
static const moq_alloc_t g_gate = { NULL, gate_alloc, gate_realloc, gate_free };

static bool fx_setup(fx_t *f, moq_version_t ver, uint32_t max_objects,
                     moq_media_overflow_policy_t policy)
{
    memset(f, 0, sizeof(*f));
    f->now = 1000;
    f->sp = make_pair(ver);
    if (!f->sp) return false;
    f->cl = moq_simpair_client(f->sp);
    f->srv = moq_simpair_server(f->sp);

    moq_pub_cfg_t pc;
    moq_pub_cfg_init_sized(&pc, sizeof(pc));
    pc.accept_mode = MOQ_PUB_ACCEPT_ALL;
    if (moq_pub_create(f->srv, moq_alloc_default(), &pc, &f->pub) != MOQ_OK)
        return false;
    moq_pub_track_cfg_t tc;
    moq_pub_track_cfg_init_sized(&tc, sizeof(tc));
    tc.track_namespace = (moq_namespace_t){ g_ns, 2 };
    tc.track_name = (moq_bytes_t){ (const uint8_t *)"catalog", 7 };
    if (moq_pub_add_track(f->pub, &tc, f->now, &f->cat) != MOQ_OK) return false;
    tc.track_name = (moq_bytes_t){ (const uint8_t *)"ts", 2 };
    if (moq_pub_add_track(f->pub, &tc, f->now, &f->ts) != MOQ_OK) return false;
    moq_rcbuf_t *json = NULL;
    moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)FX_CATALOG,
                     strlen(FX_CATALOG), &json);
    moq_pub_retained_object_t ro = { .object_id = 0, .payload = json };
    moq_pub_retained_group_cfg_t rg;
    moq_pub_retained_group_cfg_init(&rg);
    rg.group_id = 0;
    rg.objects = &ro;
    rg.object_count = 1;
    moq_result_t src = moq_pub_set_retained_group(f->pub, f->cat, &rg);
    moq_rcbuf_decref(json);
    if (src != MOQ_OK) return false;

    moq_media_receiver_cfg_t rc;
    moq_media_receiver_cfg_init_live_sized(&rc, sizeof(rc));
    rc.namespace_ = (moq_namespace_t){ g_ns, 2 };
    rc.auto_subscribe = true;
    rc.overflow.policy = policy;
    rc.overflow.max_objects = max_objects;
    rc.mpeg2ts_reorder_window_us = 300000;
    rc.mpeg2ts_reorder_slots = g_fx_slots;
    moq_media_receiver_test_set_alloc(g_fx_alloc);
    f->r = moq_media_receiver_test_new_cfg(&rc);
    moq_media_receiver_test_set_alloc(NULL);
    if (!f->r) return false;
    for (int i = 0; i < 32 && !(f->track && moq_pub_has_subscriber(f->pub, f->ts));
         i++) {
        fx_cycle(f);
        moq_media_track_t *t = drain_events(f->r, "ts", &f->ended,
                                            &f->parse_drops);
        if (t) f->track = t;
    }
    return f->track && moq_pub_has_subscriber(f->pub, f->ts);
}

static void fx_free(fx_t *f)
{
    if (f->r) moq_media_receiver_test_destroy_with_session(f->r, f->cl, f->now);
    if (f->pub) moq_pub_destroy(f->pub);
    if (f->sp) {
        moq_event_t ev;
        while (moq_session_poll_events(f->cl, &ev, 1) == 1) moq_event_cleanup(&ev);
        while (moq_session_poll_events(f->srv, &ev, 1) == 1) moq_event_cleanup(&ev);
        moq_simpair_destroy(f->sp);
    }
}

/* A small valid TS Object: one GOP-shaped run of packets, tagged in its
 * audio payload so the Object can be recognized after delivery. */
static moq_rcbuf_t *ts_obj(uint8_t tag, size_t *len_out)
{
    static ts_gen_t g;
    static bool init;
    if (!init) { ts_gen_init(&g, 188, 1, false); init = true; }
    uint8_t buf[16 * 188];
    size_t n = ts_gen_gop(&g, buf, 1);
    buf[n - 188 + 30] = tag;           /* inside the audio packet's fill */
    moq_rcbuf_t *b = NULL;
    moq_rcbuf_create(moq_alloc_default(), buf, n, &b);
    if (len_out) *len_out = n;
    return b;
}

static moq_result_t pub_write(fx_t *f, uint64_t g, uint64_t o, uint8_t tag)
{
    moq_rcbuf_t *b = ts_obj(tag, NULL);
    moq_pub_object_cfg_t oc;
    moq_pub_object_cfg_init_sized(&oc, sizeof(oc));
    oc.group_id = g;
    oc.object_id = o;
    oc.payload = b;
    oc.end_of_group = true;
    moq_result_t rc = moq_pub_write_object_ex(f->pub, f->ts, &oc, f->now);
    moq_rcbuf_decref(b);
    return rc;
}

static moq_result_t pub_write_bytes(fx_t *f, uint64_t g, uint64_t o,
                                    const uint8_t *d, size_t n)
{
    moq_rcbuf_t *b = NULL;
    moq_rcbuf_create(moq_alloc_default(), d, n, &b);
    moq_pub_object_cfg_t oc;
    moq_pub_object_cfg_init_sized(&oc, sizeof(oc));
    oc.group_id = g;
    oc.object_id = o;
    oc.payload = b;
    oc.end_of_group = true;
    moq_result_t rc = moq_pub_write_object_ex(f->pub, f->ts, &oc, f->now);
    moq_rcbuf_decref(b);
    return rc;
}

typedef struct got {
    uint64_t group, object;
    bool     has_loss, gap, tail_unknown, discontinuous, keyframe;
    uint64_t gap_group, gap_object;
    uint8_t  tag;
} got_t;

static int poll_all(fx_t *f, got_t *out, int cap)
{
    int n = 0;
    moq_media_object_t o;
    while (moq_media_receiver_poll_object(f->r, &o, sizeof(o)) == MOQ_OK) {
        if (n < cap && o.status == MOQ_OBJECT_NORMAL) {
            got_t *g = &out[n++];
            g->group = o.group_id;
            g->object = o.object_id;
            g->has_loss = o.has_loss_report;
            g->gap = o.gap;
            g->tail_unknown = o.gap_tail_unknown;
            g->discontinuous = o.discontinuous;
            g->keyframe = o.keyframe;
            g->gap_group = o.gap_group;
            g->gap_object = o.gap_object;
            g->tag = o.payload.len >= 188 ? o.payload.data[o.payload.len - 188 + 30]
                                          : 0;
            MOQ_TEST_CHECK_EQ_INT((int)o.packaging,
                                  (int)MOQ_MEDIA_PACKAGING_MPEG2TS);
        }
        moq_media_object_cleanup(&o);
    }
    return n;
}

/* Location order across Groups that arrive out of order; Group ends from the
 * FIN release the next Group at once, with no gap. */
static void test_order_and_fin(moq_version_t ver)
{
    int before = failures;
    fx_t f;
    if (!fx_setup(&f, ver, 64, MOQ_MEDIA_OVERFLOW_DROP_GROUP)) {
        failures++; fx_free(&f); return;
    }
    MOQ_TEST_CHECK(pub_write(&f, 0, 0, 0xA0) == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_end_group(f.pub, f.ts, f.now) == MOQ_OK);
    fx_cycle(&f);
    /* Group 2 before Group 1. */
    MOQ_TEST_CHECK(pub_write(&f, 2, 0, 0xA2) == MOQ_OK);
    MOQ_TEST_CHECK(pub_write(&f, 1, 0, 0xA1) == MOQ_OK);
    MOQ_TEST_CHECK(pub_write(&f, 1, 1, 0xA3) == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_end_group(f.pub, f.ts, f.now) == MOQ_OK);
    fx_cycle(&f);
    fx_cycle(&f);
    got_t got[8];
    int n = poll_all(&f, got, 8);
    MOQ_TEST_CHECK_EQ_INT(n, 4);
    static const uint64_t eg[4] = { 0, 1, 1, 2 }, eo[4] = { 0, 0, 1, 0 };
    static const uint8_t et[4] = { 0xA0, 0xA1, 0xA3, 0xA2 };
    for (int i = 0; i < n && i < 4; i++) {
        MOQ_TEST_CHECK(got[i].group == eg[i] && got[i].object == eo[i] &&
                       got[i].tag == et[i]);
        MOQ_TEST_CHECK(got[i].has_loss && !got[i].gap);
        MOQ_TEST_CHECK(got[i].keyframe);
    }
    moq_media_receiver_stats_t st;
    MOQ_TEST_CHECK(moq_media_receiver_get_stats(f.r, &st, sizeof(st)) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(st.gaps_reported, 0);
    MOQ_TEST_CHECK_EQ_U64(st.gap_tails_unknown, 0);
    fx_free(&f);
    if (failures == before) printf("PASS: ts_order_and_fin_%s\n", vname(ver));
}

/* An Object on a subgroup without the END_OF_GROUP bit, so its FIN says
 * nothing about where the Group ends. */
static moq_result_t pub_write_open(fx_t *f, uint64_t g, uint64_t o,
                                   uint8_t tag)
{
    moq_rcbuf_t *b = ts_obj(tag, NULL);
    moq_pub_object_cfg_t oc;
    moq_pub_object_cfg_init_sized(&oc, sizeof(oc));
    oc.group_id = g;
    oc.object_id = o;
    oc.payload = b;
    moq_result_t rc = moq_pub_write_object_ex(f->pub, f->ts, &oc, f->now);
    moq_rcbuf_decref(b);
    return rc;
}

static moq_result_t pub_status(fx_t *f, uint64_t g, uint64_t o)
{
    moq_pub_object_cfg_t oc;
    moq_pub_object_cfg_init_sized(&oc, sizeof(oc));
    oc.group_id = g;
    oc.object_id = o;
    oc.datagram = true;
    oc.has_status = true;
    oc.status = MOQ_OBJECT_END_OF_GROUP;
    return moq_pub_write_object_ex(f->pub, f->ts, &oc, f->now);
}

/* An END_OF_GROUP status Object names the first Object ID that does not
 * exist: a status at (0, 2) after only (0, 0) arrived leaves (0, 1) a known
 * gap, a status at (1, 1) confirms (1, 0) as the final Object, and a status
 * at (2, 0) marks Group 2 empty so Group 3 follows with no gap. */
static void test_end_of_group_status(moq_version_t ver)
{
    int before = failures;
    fx_t f;
    if (!fx_setup(&f, ver, 64, MOQ_MEDIA_OVERFLOW_DROP_GROUP)) {
        failures++; fx_free(&f); return;
    }
    MOQ_TEST_CHECK(pub_write_open(&f, 0, 0, 0xB0) == MOQ_OK);
    fx_cycle(&f);
    MOQ_TEST_CHECK(pub_status(&f, 0, 2) == MOQ_OK);
    fx_cycle(&f);
    MOQ_TEST_CHECK(pub_write_open(&f, 1, 0, 0xB1) == MOQ_OK);
    fx_cycle(&f);
    MOQ_TEST_CHECK(pub_status(&f, 1, 1) == MOQ_OK);
    fx_cycle(&f);
    MOQ_TEST_CHECK(pub_status(&f, 2, 0) == MOQ_OK);
    fx_cycle(&f);
    MOQ_TEST_CHECK(pub_write_open(&f, 3, 0, 0xB3) == MOQ_OK);
    fx_cycle(&f);
    /* (0, 1) may still arrive until the reorder window passes. */
    f.now += 400000;
    fx_cycle(&f);
    fx_cycle(&f);
    got_t got[8];
    int n = poll_all(&f, got, 8);
    MOQ_TEST_CHECK_EQ_INT(n, 3);
    if (n == 3) {
        MOQ_TEST_CHECK(got[0].group == 0 && got[0].object == 0 && !got[0].gap);
        MOQ_TEST_CHECK(got[1].group == 1 && got[1].object == 0);
        MOQ_TEST_CHECK(got[1].gap && !got[1].tail_unknown);
        MOQ_TEST_CHECK(got[1].gap_group == 0 && got[1].gap_object == 1);
        MOQ_TEST_CHECK(got[2].group == 3 && got[2].object == 0);
        MOQ_TEST_CHECK(!got[2].gap && !got[2].tail_unknown);
    }
    moq_media_receiver_stats_t st;
    MOQ_TEST_CHECK(moq_media_receiver_get_stats(f.r, &st, sizeof(st)) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(st.gaps_reported, 1);
    MOQ_TEST_CHECK_EQ_U64(st.gap_tails_unknown, 0);
    fx_free(&f);
    if (failures == before)
        printf("PASS: ts_end_of_group_status_%s\n", vname(ver));
}

/* A reset subgroup confirms nothing: the next Group waits for the window and
 * then reports the possible tail as unknown. The deadline names that time. */
static void test_reset_tail_unknown(moq_version_t ver)
{
    int before = failures;
    fx_t f;
    if (!fx_setup(&f, ver, 64, MOQ_MEDIA_OVERFLOW_DROP_GROUP)) {
        failures++; fx_free(&f); return;
    }
    MOQ_TEST_CHECK(pub_write(&f, 0, 0, 0xB0) == MOQ_OK);
    MOQ_TEST_CHECK(pub_write(&f, 0, 1, 0xB1) == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_reset_group(f.pub, f.ts, 0x10, f.now) == MOQ_OK);
    MOQ_TEST_CHECK(pub_write(&f, 1, 0, 0xB2) == MOQ_OK);
    fx_cycle(&f);
    got_t got[8];
    int n = poll_all(&f, got, 8);
    MOQ_TEST_CHECK_EQ_INT(n, 2);                 /* Group 1 is held */
    uint64_t dl = moq_media_receiver_test_next_deadline_us(f.r);
    MOQ_TEST_CHECK(dl != UINT64_MAX && dl > f.now);
    f.now = dl;
    fx_cycle(&f);
    n = poll_all(&f, got, 8);
    MOQ_TEST_CHECK_EQ_INT(n, 1);
    if (n == 1) {
        MOQ_TEST_CHECK(got[0].group == 1 && got[0].object == 0);
        MOQ_TEST_CHECK(got[0].gap && got[0].tail_unknown);
        MOQ_TEST_CHECK(got[0].gap_group == 0 && got[0].gap_object == 2);
    }
    MOQ_TEST_CHECK_EQ_U64(moq_media_receiver_test_next_deadline_us(f.r),
                          UINT64_MAX);
    moq_media_receiver_stats_t st;
    MOQ_TEST_CHECK(moq_media_receiver_get_stats(f.r, &st, sizeof(st)) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(st.gaps_reported, 1);
    MOQ_TEST_CHECK_EQ_U64(st.gap_tails_unknown, 1);
    fx_free(&f);
    if (failures == before) printf("PASS: ts_reset_tail_unknown_%s\n", vname(ver));
}

/* An invalid Object is rejected (PARSE_DROP) and is the gap on the next. */
static void test_rejected_object(moq_version_t ver)
{
    int before = failures;
    fx_t f;
    if (!fx_setup(&f, ver, 64, MOQ_MEDIA_OVERFLOW_DROP_GROUP)) {
        failures++; fx_free(&f); return;
    }
    MOQ_TEST_CHECK(pub_write(&f, 0, 0, 0xC0) == MOQ_OK);
    uint8_t junk[200];
    memset(junk, 0x47, sizeof(junk));                  /* not whole packets */
    MOQ_TEST_CHECK(pub_write_bytes(&f, 0, 1, junk, sizeof(junk)) == MOQ_OK);
    MOQ_TEST_CHECK(pub_write(&f, 0, 2, 0xC2) == MOQ_OK);
    fx_cycle(&f);
    (void)drain_events(f.r, "ts", &f.ended, &f.parse_drops);
    got_t got[8];
    int n = poll_all(&f, got, 8);
    MOQ_TEST_CHECK_EQ_INT(n, 2);
    if (n == 2) {
        MOQ_TEST_CHECK(!got[0].gap);
        MOQ_TEST_CHECK(got[1].object == 2 && got[1].gap && !got[1].tail_unknown);
        MOQ_TEST_CHECK(got[1].gap_group == 0 && got[1].gap_object == 1);
    }
    MOQ_TEST_CHECK_EQ_INT(f.parse_drops, 1);
    fx_free(&f);
    if (failures == before) printf("PASS: ts_rejected_object_%s\n", vname(ver));
}

/* END_OF_TRACK releases every held Object before TRACK_ENDED, once the
 * window has given up on the missing ones. */
static void test_end_of_track_releases(moq_version_t ver)
{
    int before = failures;
    fx_t f;
    if (!fx_setup(&f, ver, 64, MOQ_MEDIA_OVERFLOW_DROP_GROUP)) {
        failures++; fx_free(&f); return;
    }
    MOQ_TEST_CHECK(pub_write(&f, 0, 0, 0xD0) == MOQ_OK);
    MOQ_TEST_CHECK(pub_write(&f, 0, 2, 0xD2) == MOQ_OK);    /* (0,1) never */
    fx_cycle(&f);
    got_t got[8];
    MOQ_TEST_CHECK_EQ_INT(poll_all(&f, got, 8), 1);        /* (0,2) held */
    MOQ_TEST_CHECK(moq_pub_end_track(f.pub, f.ts, f.now) == MOQ_OK);
    fx_cycle(&f);
    /* (0,1) could still arrive on another stream: the end waits for the
     * window, then releases what is held, with the gap, before
     * TRACK_ENDED. */
    int n = poll_all(&f, got, 8);
    (void)drain_events(f.r, "ts", &f.ended, &f.parse_drops);
    MOQ_TEST_CHECK_EQ_INT(n, 0);
    MOQ_TEST_CHECK_EQ_INT(f.ended, 0);
    f.now += 400000;
    fx_cycle(&f);
    n = poll_all(&f, got, 8);
    (void)drain_events(f.r, "ts", &f.ended, &f.parse_drops);
    MOQ_TEST_CHECK_EQ_INT(n, 1);
    if (n == 1)
        MOQ_TEST_CHECK(got[0].object == 2 && got[0].gap &&
                       got[0].gap_object == 1);
    MOQ_TEST_CHECK_EQ_INT(f.ended, 1);
    fx_free(&f);
    if (failures == before) printf("PASS: ts_end_of_track_releases_%s\n",
                                   vname(ver));
}

/* A Group end that becomes ready while a catalog Object is polled is
 * applied there, before the next TS Object is offered: TS (0,0), a catalog
 * Object, Group 0's FIN, (1,0), then END_OF_TRACK. (1,0) follows a
 * confirmed end, so it carries no gap. */
static void test_catalog_interleaved_end(moq_version_t ver)
{
    int before = failures;
    fx_t f;
    /* One slot: an Object that cannot be released yet is released at once,
     * so the Group end must already be applied when (1,0) is offered. */
    g_fx_slots = 1;
    bool ok = fx_setup(&f, ver, 64, MOQ_MEDIA_OVERFLOW_DROP_GROUP);
    g_fx_slots = 0;
    if (!ok) { failures++; fx_free(&f); return; }
    MOQ_TEST_CHECK(pub_write(&f, 0, 0, 0xE0) == MOQ_OK);
    moq_rcbuf_t *json = NULL;
    moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)FX_CATALOG,
                     strlen(FX_CATALOG), &json);
    moq_pub_object_cfg_t oc;
    moq_pub_object_cfg_init_sized(&oc, sizeof(oc));
    oc.group_id = 1;
    oc.object_id = 0;
    oc.payload = json;
    oc.end_of_group = true;
    MOQ_TEST_CHECK(moq_pub_write_object_ex(f.pub, f.cat, &oc, f.now) == MOQ_OK);
    moq_rcbuf_decref(json);
    MOQ_TEST_CHECK(moq_pub_end_group(f.pub, f.ts, f.now) == MOQ_OK);
    MOQ_TEST_CHECK(pub_write(&f, 1, 0, 0xE1) == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_end_track(f.pub, f.ts, f.now) == MOQ_OK);
    fx_cycle(&f);
    fx_cycle(&f);
    got_t got[8];
    int n = poll_all(&f, got, 8);
    (void)drain_events(f.r, "ts", &f.ended, &f.parse_drops);
    MOQ_TEST_CHECK_EQ_INT(n, 2);
    if (n == 2) {
        MOQ_TEST_CHECK(got[0].group == 0 && got[0].object == 0 &&
                       got[0].tag == 0xE0 && !got[0].gap);
        MOQ_TEST_CHECK(got[1].group == 1 && got[1].object == 0 &&
                       got[1].tag == 0xE1);
        MOQ_TEST_CHECK(!got[1].gap && !got[1].tail_unknown);
    }
    moq_media_receiver_stats_t st;
    MOQ_TEST_CHECK(moq_media_receiver_get_stats(f.r, &st, sizeof(st)) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(st.gaps_reported, 0);
    MOQ_TEST_CHECK_EQ_U64(st.gap_tails_unknown, 0);
    MOQ_TEST_CHECK_EQ_INT(f.ended, 1);
    fx_free(&f);
    if (failures == before)
        printf("PASS: ts_catalog_interleaved_end_%s\n", vname(ver));
}

/* An arriving Object the receiver cannot hold is a loss at its own
 * Location: (0,0), then (0,2) refused by the allocator, then (0,1). (0,1) is
 * clean; the gap is reported at (0,2) on (0,3), the next Object after it. */
static void test_intake_loss_location(moq_version_t ver)
{
    int before = failures;
    fx_t f;
    g_fx_alloc = &g_gate;
    bool ok = fx_setup(&f, ver, 64, MOQ_MEDIA_OVERFLOW_DROP_GROUP);
    g_fx_alloc = NULL;
    if (!ok) { failures++; fx_free(&f); return; }
    got_t got[8];
    int n = 0;
    MOQ_TEST_CHECK(pub_write_open(&f, 0, 0, 0xF0) == MOQ_OK);
    fx_cycle(&f);
    n += poll_all(&f, got + n, 8 - n);
    /* (0,2) first, as a datagram (a subgroup's Object IDs ascend), its
     * holder refused. */
    {
        moq_rcbuf_t *b = ts_obj(0xF2, NULL);
        moq_pub_object_cfg_t oc;
        moq_pub_object_cfg_init_sized(&oc, sizeof(oc));
        oc.group_id = 0;
        oc.object_id = 2;
        oc.payload = b;
        oc.datagram = true;
        MOQ_TEST_CHECK(moq_pub_write_object_ex(f.pub, f.ts, &oc, f.now) ==
                       MOQ_OK);
        moq_rcbuf_decref(b);
    }
    moq_simpair_run_until_quiescent(f.sp, 64, NULL);
    (void)moq_pub_tick(f.pub, f.now);
    moq_simpair_run_until_quiescent(f.sp, 64, NULL);
    g_refuse_next = 1;
    moq_media_receiver_test_pump(f.r, f.cl, f.now);
    MOQ_TEST_CHECK_EQ_INT(g_refuse_next, 0);
    g_refuse_next = 0;
    MOQ_TEST_CHECK(pub_write_open(&f, 0, 1, 0xF1) == MOQ_OK);
    fx_cycle(&f);
    n += poll_all(&f, got + n, 8 - n);
    MOQ_TEST_CHECK(pub_write_open(&f, 0, 3, 0xF3) == MOQ_OK);
    fx_cycle(&f);
    /* Past the reorder window: nothing may still wait on the hole. */
    f.now += 400000;
    fx_cycle(&f);
    n += poll_all(&f, got + n, 8 - n);
    MOQ_TEST_CHECK_EQ_INT(n, 3);
    if (n == 3) {
        MOQ_TEST_CHECK(got[0].object == 0 && got[0].tag == 0xF0 && !got[0].gap);
        MOQ_TEST_CHECK(got[1].object == 1 && got[1].tag == 0xF1);
        MOQ_TEST_CHECK(!got[1].gap && !got[1].discontinuous);
        MOQ_TEST_CHECK(got[2].object == 3 && got[2].tag == 0xF3);
        MOQ_TEST_CHECK(got[2].gap && !got[2].tail_unknown);
        MOQ_TEST_CHECK(got[2].gap_group == 0 && got[2].gap_object == 2);
    }
    moq_media_receiver_stats_t st;
    MOQ_TEST_CHECK(moq_media_receiver_get_stats(f.r, &st, sizeof(st)) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(st.objects_dropped, 1);
    MOQ_TEST_CHECK_EQ_U64(st.gaps_reported, 1);
    MOQ_TEST_CHECK_EQ_U64(moq_media_receiver_test_next_deadline_us(f.r),
                          UINT64_MAX);
    fx_free(&f);
    if (failures == before)
        printf("PASS: ts_intake_loss_location_%s\n", vname(ver));
}

/* (0,2) as a datagram whose holder the receiver cannot allocate. */
static void write_refused_datagram(fx_t *f, uint64_t g, uint64_t o,
                                   uint8_t tag)
{
    moq_rcbuf_t *b = ts_obj(tag, NULL);
    moq_pub_object_cfg_t oc;
    moq_pub_object_cfg_init_sized(&oc, sizeof(oc));
    oc.group_id = g;
    oc.object_id = o;
    oc.payload = b;
    oc.datagram = true;
    MOQ_TEST_CHECK(moq_pub_write_object_ex(f->pub, f->ts, &oc, f->now) ==
                   MOQ_OK);
    moq_rcbuf_decref(b);
    moq_simpair_run_until_quiescent(f->sp, 64, NULL);
    (void)moq_pub_tick(f->pub, f->now);
    moq_simpair_run_until_quiescent(f->sp, 64, NULL);
    g_refuse_next = 1;
    moq_media_receiver_test_pump(f->r, f->cl, f->now);
    MOQ_TEST_CHECK_EQ_INT(g_refuse_next, 0);
    g_refuse_next = 0;
}

/* A gap that a rejected Object would carry stays with the next Object the
 * app gets: (0,0); (0,1) never sent; (0,2) lost at intake and released as a
 * placeholder once the window expires; (0,3) reports the gap from (0,1).
 * Then again, separated: (0,4) never sent, (0,5) lost, (0,6) reports the
 * gap from (0,4). */
static void test_placeholder_keeps_gap(moq_version_t ver)
{
    int before = failures;
    fx_t f;
    g_fx_alloc = &g_gate;
    bool ok = fx_setup(&f, ver, 64, MOQ_MEDIA_OVERFLOW_DROP_GROUP);
    g_fx_alloc = NULL;
    if (!ok) { failures++; fx_free(&f); return; }
    got_t got[8];
    int n = 0;
    MOQ_TEST_CHECK(pub_write_open(&f, 0, 0, 0xA0) == MOQ_OK);
    fx_cycle(&f);
    n += poll_all(&f, got + n, 8 - n);
    write_refused_datagram(&f, 0, 2, 0xA2);
    f.now += 400000;                          /* the window expires */
    fx_cycle(&f);
    n += poll_all(&f, got + n, 8 - n);
    MOQ_TEST_CHECK_EQ_INT(n, 1);              /* nothing valid released yet */
    MOQ_TEST_CHECK(pub_write_open(&f, 0, 3, 0xA3) == MOQ_OK);
    fx_cycle(&f);
    n += poll_all(&f, got + n, 8 - n);
    write_refused_datagram(&f, 0, 5, 0xA5);
    f.now += 400000;
    fx_cycle(&f);
    MOQ_TEST_CHECK(pub_write_open(&f, 0, 6, 0xA6) == MOQ_OK);
    fx_cycle(&f);
    n += poll_all(&f, got + n, 8 - n);
    MOQ_TEST_CHECK_EQ_INT(n, 3);
    if (n == 3) {
        MOQ_TEST_CHECK(got[0].object == 0 && !got[0].gap);
        MOQ_TEST_CHECK(got[1].object == 3 && got[1].tag == 0xA3 &&
                       got[1].gap && !got[1].tail_unknown &&
                       got[1].gap_group == 0 && got[1].gap_object == 1);
        MOQ_TEST_CHECK(got[2].object == 6 && got[2].tag == 0xA6 &&
                       got[2].gap && !got[2].tail_unknown &&
                       got[2].gap_group == 0 && got[2].gap_object == 4);
    }
    moq_media_receiver_stats_t st;
    MOQ_TEST_CHECK(moq_media_receiver_get_stats(f.r, &st, sizeof(st)) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(st.gaps_reported, 2);
    MOQ_TEST_CHECK_EQ_U64(st.gap_tails_unknown, 0);
    MOQ_TEST_CHECK_EQ_U64(st.objects_dropped, 2);
    MOQ_TEST_CHECK_EQ_U64(st.objects_queued, 0);
    MOQ_TEST_CHECK_EQ_U64(moq_media_receiver_test_next_deadline_us(f.r),
                          UINT64_MAX);
    fx_free(&f);
    if (failures == before)
        printf("PASS: ts_placeholder_keeps_gap_%s\n", vname(ver));
}

/* The same for a payload that is rejected: (0,0); (0,1) never sent; (0,2)
 * not whole packets; (0,3) reports the gap from (0,1). */
static void test_rejected_keeps_gap(moq_version_t ver)
{
    int before = failures;
    fx_t f;
    if (!fx_setup(&f, ver, 64, MOQ_MEDIA_OVERFLOW_DROP_GROUP)) {
        failures++; fx_free(&f); return;
    }
    MOQ_TEST_CHECK(pub_write_open(&f, 0, 0, 0xB0) == MOQ_OK);
    fx_cycle(&f);
    got_t got[8];
    int n = poll_all(&f, got, 8);
    uint8_t junk[200];
    memset(junk, 0x47, sizeof(junk));
    {
        moq_rcbuf_t *b = NULL;
        moq_rcbuf_create(moq_alloc_default(), junk, sizeof(junk), &b);
        moq_pub_object_cfg_t oc;
        moq_pub_object_cfg_init_sized(&oc, sizeof(oc));
        oc.group_id = 0;
        oc.object_id = 2;
        oc.payload = b;
        MOQ_TEST_CHECK(moq_pub_write_object_ex(f.pub, f.ts, &oc, f.now) ==
                       MOQ_OK);
        moq_rcbuf_decref(b);
    }
    fx_cycle(&f);
    f.now += 400000;
    fx_cycle(&f);
    n += poll_all(&f, got + n, 8 - n);
    MOQ_TEST_CHECK(pub_write_open(&f, 0, 3, 0xB3) == MOQ_OK);
    fx_cycle(&f);
    n += poll_all(&f, got + n, 8 - n);
    (void)drain_events(f.r, "ts", &f.ended, &f.parse_drops);
    MOQ_TEST_CHECK_EQ_INT(n, 2);
    if (n == 2)
        MOQ_TEST_CHECK(got[1].object == 3 && got[1].gap &&
                       !got[1].tail_unknown && got[1].gap_group == 0 &&
                       got[1].gap_object == 1);
    MOQ_TEST_CHECK_EQ_INT(f.parse_drops, 1);
    moq_media_receiver_stats_t st;
    MOQ_TEST_CHECK(moq_media_receiver_get_stats(f.r, &st, sizeof(st)) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(st.gaps_reported, 1);
    fx_free(&f);
    if (failures == before)
        printf("PASS: ts_rejected_keeps_gap_%s\n", vname(ver));
}

/* Objects the receiver drops itself -- evicted from a full queue, or
 * released while the app has the track disabled -- are the gap on the next
 * Object the app gets. */
static void test_local_loss(moq_version_t ver)
{
    int before = failures;
    fx_t f;
    if (!fx_setup(&f, ver, 2, MOQ_MEDIA_OVERFLOW_DROP_GROUP)) {
        failures++; fx_free(&f); return;
    }
    /* Five one-Object Groups into a two-Object queue nobody polls. */
    for (uint64_t g = 0; g < 5; g++) {
        MOQ_TEST_CHECK(pub_write(&f, g, 0, (uint8_t)(0xE0 + g)) == MOQ_OK);
        MOQ_TEST_CHECK(moq_pub_end_group(f.pub, f.ts, f.now) == MOQ_OK);
        fx_cycle(&f);
    }
    got_t got[8];
    int n = poll_all(&f, got, 8);
    MOQ_TEST_CHECK(n >= 1 && n <= 2);
    if (n >= 1) {
        MOQ_TEST_CHECK(got[0].gap && !got[0].tail_unknown);
        MOQ_TEST_CHECK(got[0].gap_group == 0 && got[0].gap_object == 0);
    }
    for (int i = 1; i < n; i++) MOQ_TEST_CHECK(!got[i].gap);

    /* Disabled, then enabled again: the Object that arrived meanwhile is
     * dropped, and the paused span is a gap. */
    MOQ_TEST_CHECK(moq_media_receiver_unsubscribe_track(f.r, f.track) == MOQ_OK);
    MOQ_TEST_CHECK(pub_write(&f, 5, 0, 0xE5) == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_end_group(f.pub, f.ts, f.now) == MOQ_OK);
    fx_cycle(&f);
    fx_cycle(&f);
    MOQ_TEST_CHECK_EQ_INT(poll_all(&f, got, 8), 0);
    MOQ_TEST_CHECK(moq_media_receiver_subscribe_track(f.r, f.track, NULL) ==
                   MOQ_OK);
    for (int i = 0; i < 4; i++) fx_cycle(&f);
    MOQ_TEST_CHECK(pub_write(&f, 6, 0, 0xE6) == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_end_group(f.pub, f.ts, f.now) == MOQ_OK);
    fx_cycle(&f);
    n = poll_all(&f, got, 8);
    MOQ_TEST_CHECK_EQ_INT(n, 1);
    if (n == 1) {
        MOQ_TEST_CHECK(got[0].group == 6);
        MOQ_TEST_CHECK(got[0].gap && !got[0].tail_unknown &&
                       got[0].gap_group == 5 && got[0].gap_object == 0);
    }
    fx_free(&f);
    if (failures == before) printf("PASS: ts_local_loss_%s\n", vname(ver));
}

/* -- A receiver fed by a raw session ------------------------------------ */

/* The publisher side driven directly through the session API, for wire
 * shapes the publisher facade does not write: a status Object on a subgroup
 * stream. */
typedef struct raw {
    moq_simpair_t        *sp;
    moq_session_t        *cl, *srv;
    moq_media_receiver_t *r;
    moq_media_track_t    *track;
    moq_subscription_t    ts_sub;
    bool                  have_ts;
    uint64_t              now;
    int                   ended;
} raw_t;

static void raw_serve(raw_t *x)
{
    moq_event_t ev;
    while (moq_session_poll_events(x->srv, &ev, 1) == 1) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
            moq_subscribe_request_event_t *q = &ev.u.subscribe_request;
            moq_accept_subscribe_cfg_t acc;
            moq_accept_subscribe_cfg_init(&acc);
            /* The names are borrowed until the next advancing call. */
            bool cat = span_is(q->track_name, "catalog");
            bool ts = span_is(q->track_name, "ts");
            if (cat) {
                acc.has_largest = true;
                acc.largest_group = 0;
                acc.largest_object = 0;
            }
            moq_subscription_t sub = q->sub;
            MOQ_TEST_CHECK(moq_session_accept_subscribe(x->srv, sub, &acc,
                                                        x->now) == MOQ_OK);
            if (cat) {
                moq_subgroup_cfg_t sc;
                moq_subgroup_cfg_init(&sc);
                sc.end_of_group = true;
                moq_subgroup_handle_t sg;
                moq_rcbuf_t *json = NULL;
                moq_rcbuf_create(moq_alloc_default(),
                                 (const uint8_t *)FX_CATALOG,
                                 strlen(FX_CATALOG), &json);
                MOQ_TEST_CHECK(moq_session_open_subgroup(x->srv, sub, &sc,
                                                         x->now, &sg) == MOQ_OK);
                MOQ_TEST_CHECK(moq_session_write_object(x->srv, sg, 0, json,
                                                        x->now) == MOQ_OK);
                moq_rcbuf_decref(json);
                moq_session_close_subgroup(x->srv, sg, x->now);
            } else if (ts) {
                x->ts_sub = sub;
                x->have_ts = true;
            }
        }
        moq_event_cleanup(&ev);
    }
}

static void raw_cycle(raw_t *x)
{
    moq_media_receiver_test_pump(x->r, x->cl, x->now);
    moq_simpair_run_until_quiescent(x->sp, 64, NULL);
    raw_serve(x);
    moq_simpair_run_until_quiescent(x->sp, 64, NULL);
    moq_media_receiver_test_pump(x->r, x->cl, x->now);
}

static bool raw_setup(raw_t *x, moq_version_t ver)
{
    memset(x, 0, sizeof(*x));
    x->now = 1000;
    x->sp = make_pair(ver);
    if (!x->sp) return false;
    x->cl = moq_simpair_client(x->sp);
    x->srv = moq_simpair_server(x->sp);
    moq_media_receiver_cfg_t rc;
    moq_media_receiver_cfg_init_live_sized(&rc, sizeof(rc));
    rc.namespace_ = (moq_namespace_t){ g_ns, 2 };
    rc.auto_subscribe = true;
    rc.overflow.policy = MOQ_MEDIA_OVERFLOW_DROP_GROUP;
    rc.overflow.max_objects = 64;
    rc.mpeg2ts_reorder_window_us = 300000;
    x->r = moq_media_receiver_test_new_cfg(&rc);
    if (!x->r) return false;
    for (int i = 0; i < 32 && !(x->track && x->have_ts); i++) {
        raw_cycle(x);
        moq_media_track_t *t = drain_events(x->r, "ts", &x->ended, NULL);
        if (t) x->track = t;
    }
    return x->track && x->have_ts;
}

static void raw_free(raw_t *x)
{
    if (x->r) moq_media_receiver_test_destroy_with_session(x->r, x->cl, x->now);
    if (x->sp) {
        moq_event_t ev;
        while (moq_session_poll_events(x->cl, &ev, 1) == 1) moq_event_cleanup(&ev);
        while (moq_session_poll_events(x->srv, &ev, 1) == 1) moq_event_cleanup(&ev);
        moq_simpair_destroy(x->sp);
    }
}

static int poll_all_raw(raw_t *x, got_t *out, int cap)
{
    int n = 0;
    moq_media_object_t o;
    while (moq_media_receiver_poll_object(x->r, &o, sizeof(o)) == MOQ_OK) {
        if (o.status == MOQ_OBJECT_NORMAL && n < cap) {
            got_t *g = &out[n++];
            memset(g, 0, sizeof(*g));
            g->group = o.group_id;
            g->object = o.object_id;
            g->gap = o.gap;
            g->tail_unknown = o.gap_tail_unknown;
        }
        moq_media_object_cleanup(&o);
    }
    (void)drain_events(x->r, "ts", &x->ended, NULL);
    return n;
}

/* An END_OF_GROUP status Object carried on the subgroup stream itself, the
 * reliable form: (0,0), then the status at Object 2 and the subgroup's FIN
 * (END_OF_GROUP bit set), then (1,0), then END_OF_TRACK on a fresh subgroup.
 * The status names Object 1 as the final one and it never arrived, so (1,0)
 * reports a known gap at (0,1) -- the FIN behind the status does not turn it
 * into a clean boundary at (0,0). */
static void test_stream_end_of_group_status(moq_version_t ver)
{
    int before = failures;
    raw_t x;
    if (!raw_setup(&x, ver)) { failures++; raw_free(&x); return; }
    moq_subgroup_cfg_t sc;
    moq_subgroup_cfg_init(&sc);
    sc.end_of_group = true;
    moq_subgroup_handle_t sg;
    sc.group_id = 0;
    MOQ_TEST_CHECK(moq_session_open_subgroup(x.srv, x.ts_sub, &sc, x.now,
                                             &sg) == MOQ_OK);
    moq_rcbuf_t *b = ts_obj(0xC0, NULL);
    MOQ_TEST_CHECK(moq_session_write_object(x.srv, sg, 0, b, x.now) == MOQ_OK);
    moq_rcbuf_decref(b);
    MOQ_TEST_CHECK(moq_session_write_status_object(x.srv, sg, 2,
                                                   MOQ_OBJECT_END_OF_GROUP,
                                                   x.now) == MOQ_OK);
    moq_session_close_subgroup(x.srv, sg, x.now);
    raw_cycle(&x);
    sc.group_id = 1;
    MOQ_TEST_CHECK(moq_session_open_subgroup(x.srv, x.ts_sub, &sc, x.now,
                                             &sg) == MOQ_OK);
    b = ts_obj(0xC1, NULL);
    MOQ_TEST_CHECK(moq_session_write_object(x.srv, sg, 0, b, x.now) == MOQ_OK);
    moq_rcbuf_decref(b);
    moq_session_close_subgroup(x.srv, sg, x.now);
    raw_cycle(&x);
    sc.group_id = 2;
    MOQ_TEST_CHECK(moq_session_open_subgroup(x.srv, x.ts_sub, &sc, x.now,
                                             &sg) == MOQ_OK);
    MOQ_TEST_CHECK(moq_session_write_status_object(x.srv, sg, 0,
                                                   MOQ_OBJECT_END_OF_TRACK,
                                                   x.now) == MOQ_OK);
    moq_session_close_subgroup(x.srv, sg, x.now);
    got_t got[8];
    int n = 0;
    for (int i = 0; i < 8 && x.ended == 0; i++) {
        raw_cycle(&x);
        x.now += 100000;   /* the known gap at (0,1) waits for the window */
        moq_media_object_t o;
        while (moq_media_receiver_poll_object(x.r, &o, sizeof(o)) == MOQ_OK) {
            if (o.status == MOQ_OBJECT_NORMAL && n < 8) {
                got[n].group = o.group_id;
                got[n].object = o.object_id;
                got[n].gap = o.gap;
                got[n].tail_unknown = o.gap_tail_unknown;
                got[n].gap_group = o.gap_group;
                got[n].gap_object = o.gap_object;
                n++;
            }
            moq_media_object_cleanup(&o);
        }
        (void)drain_events(x.r, "ts", &x.ended, NULL);
    }
    MOQ_TEST_CHECK_EQ_INT(x.ended, 1);
    MOQ_TEST_CHECK_EQ_INT(n, 2);
    if (n == 2) {
        MOQ_TEST_CHECK(got[0].group == 0 && got[0].object == 0 && !got[0].gap);
        MOQ_TEST_CHECK(got[1].group == 1 && got[1].object == 0);
        MOQ_TEST_CHECK(got[1].gap && !got[1].tail_unknown &&
                       got[1].gap_group == 0 && got[1].gap_object == 1);
    }
    raw_free(&x);
    if (failures == before)
        printf("PASS: ts_stream_end_of_group_status_%s\n", vname(ver));
}

/* END_OF_TRACK rides its own subgroup stream, so it can arrive before the
 * tail of an earlier Group on another stream. (0,0); (1,0) on its own
 * subgroup (held: Group 0's end is not known yet); END_OF_TRACK at (2,0);
 * then (0,1) and Group 0's FIN. Every Object is still delivered, in order
 * and with no gap, and TRACK_ENDED follows the last of them. */
static void eot_overtake_run(moq_version_t ver, bool repeat)
{
    int before = failures;
    raw_t x;
    if (!raw_setup(&x, ver)) { failures++; raw_free(&x); return; }
    moq_subgroup_cfg_t sc;
    moq_subgroup_cfg_init(&sc);
    sc.end_of_group = true;
    sc.group_id = 0;
    moq_subgroup_handle_t sg0, sg1;
    got_t got[8];
    int n = 0;
    MOQ_TEST_CHECK(moq_session_open_subgroup(x.srv, x.ts_sub, &sc, x.now,
                                             &sg0) == MOQ_OK);
    moq_rcbuf_t *b = ts_obj(0xD0, NULL);
    MOQ_TEST_CHECK(moq_session_write_object(x.srv, sg0, 0, b, x.now) == MOQ_OK);
    moq_rcbuf_decref(b);
    raw_cycle(&x);
    sc.group_id = 1;
    MOQ_TEST_CHECK(moq_session_open_subgroup(x.srv, x.ts_sub, &sc, x.now,
                                             &sg1) == MOQ_OK);
    b = ts_obj(0xD2, NULL);
    MOQ_TEST_CHECK(moq_session_write_object(x.srv, sg1, 0, b, x.now) == MOQ_OK);
    moq_rcbuf_decref(b);
    moq_session_close_subgroup(x.srv, sg1, x.now);
    raw_cycle(&x);
    moq_subgroup_handle_t sg2;
    sc.group_id = 2;
    MOQ_TEST_CHECK(moq_session_open_subgroup(x.srv, x.ts_sub, &sc, x.now,
                                             &sg2) == MOQ_OK);
    MOQ_TEST_CHECK(moq_session_write_status_object(x.srv, sg2, 0,
                                                   MOQ_OBJECT_END_OF_TRACK,
                                                   x.now) == MOQ_OK);
    moq_session_close_subgroup(x.srv, sg2, x.now);
    raw_cycle(&x);
    if (repeat) {
        /* The same END_OF_TRACK again, on another subgroup of Group 2. */
        moq_subgroup_handle_t sg3;
        sc.subgroup_id = 1;
        MOQ_TEST_CHECK(moq_session_open_subgroup(x.srv, x.ts_sub, &sc, x.now,
                                                 &sg3) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_write_status_object(
                           x.srv, sg3, 0, MOQ_OBJECT_END_OF_TRACK, x.now) ==
                       MOQ_OK);
        moq_session_close_subgroup(x.srv, sg3, x.now);
        raw_cycle(&x);
        n += poll_all_raw(&x, got + n, 8 - n);
        MOQ_TEST_CHECK_EQ_INT(x.ended, 0);
    }
    b = ts_obj(0xD1, NULL);
    MOQ_TEST_CHECK(moq_session_write_object(x.srv, sg0, 1, b, x.now) == MOQ_OK);
    moq_rcbuf_decref(b);
    moq_session_close_subgroup(x.srv, sg0, x.now);
    for (int i = 0; i < 4; i++) {
        raw_cycle(&x);
        n += poll_all_raw(&x, got + n, 8 - n);
    }
    /* Everything before the end arrived, so the track ends at once, with
     * no wait for the window. */
    MOQ_TEST_CHECK_EQ_INT(x.ended, 1);
    MOQ_TEST_CHECK_EQ_INT(n, 3);
    if (n == 3) {
        MOQ_TEST_CHECK(got[0].group == 0 && got[0].object == 0 && !got[0].gap);
        MOQ_TEST_CHECK(got[1].group == 0 && got[1].object == 1 && !got[1].gap);
        MOQ_TEST_CHECK(got[2].group == 1 && got[2].object == 0 &&
                       !got[2].gap && !got[2].tail_unknown);
    }
    moq_media_receiver_stats_t st;
    MOQ_TEST_CHECK(moq_media_receiver_get_stats(x.r, &st, sizeof(st)) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(st.reorder_refused, 0);
    MOQ_TEST_CHECK_EQ_U64(st.gaps_reported, 0);
    MOQ_TEST_CHECK_EQ_U64(moq_media_receiver_test_next_deadline_us(x.r),
                          UINT64_MAX);
    raw_free(&x);
    if (failures == before)
        printf("PASS: ts_end_of_track_overtakes_tail%s_%s\n",
               repeat ? "_repeated" : "", vname(ver));
}

/* END_OF_TRACKs the window cannot take still end the track at once: one at
 * a Location where an Object is held, and one at a Location the window has
 * already passed. Either declares Objects that were known to exist
 * non-existent, which drafts 16 and 18 allow (section 2.1). */
static void eot_conflict_run(moq_version_t ver, bool passed)
{
    int before = failures;
    raw_t x;
    if (!raw_setup(&x, ver)) { failures++; raw_free(&x); return; }
    moq_subgroup_cfg_t sc;
    moq_subgroup_cfg_init(&sc);
    sc.end_of_group = true;
    moq_subgroup_handle_t sg0, sg1, sg2;
    got_t got[8];
    int n = 0;
    MOQ_TEST_CHECK(moq_session_open_subgroup(x.srv, x.ts_sub, &sc, x.now,
                                             &sg0) == MOQ_OK);
    moq_rcbuf_t *b = ts_obj(0xE8, NULL);
    MOQ_TEST_CHECK(moq_session_write_object(x.srv, sg0, 0, b, x.now) == MOQ_OK);
    moq_rcbuf_decref(b);
    raw_cycle(&x);
    n += poll_all_raw(&x, got + n, 8 - n);
    uint64_t eg = 0, eo = 0;               /* passed: (0,0) was delivered */
    if (!passed) {
        /* (1,1) held: (0,1) and (1,0) are still missing. */
        sc.group_id = 1;
        MOQ_TEST_CHECK(moq_session_open_subgroup(x.srv, x.ts_sub, &sc, x.now,
                                                 &sg1) == MOQ_OK);
        b = ts_obj(0xE9, NULL);
        MOQ_TEST_CHECK(moq_session_write_object(x.srv, sg1, 1, b, x.now) ==
                       MOQ_OK);
        moq_rcbuf_decref(b);
        raw_cycle(&x);
        n += poll_all_raw(&x, got + n, 8 - n);
        MOQ_TEST_CHECK_EQ_INT(n, 1);
        eg = 1; eo = 1;
    }
    sc.group_id = eg;
    sc.subgroup_id = 7;
    MOQ_TEST_CHECK(moq_session_open_subgroup(x.srv, x.ts_sub, &sc, x.now,
                                             &sg2) == MOQ_OK);
    MOQ_TEST_CHECK(moq_session_write_status_object(x.srv, sg2, eo,
                                                   MOQ_OBJECT_END_OF_TRACK,
                                                   x.now) == MOQ_OK);
    moq_session_close_subgroup(x.srv, sg2, x.now);
    raw_cycle(&x);
    n += poll_all_raw(&x, got + n, 8 - n);
    MOQ_TEST_CHECK_EQ_INT(x.ended, 1);
    /* What was held is released before TRACK_ENDED, with its gap. */
    MOQ_TEST_CHECK_EQ_INT(n, passed ? 1 : 2);
    if (!passed && n == 2)
        MOQ_TEST_CHECK(got[1].group == 1 && got[1].object == 1 && got[1].gap);
    raw_free(&x);
    if (failures == before)
        printf("PASS: ts_end_of_track_%s_%s\n",
               passed ? "passed_location" : "conflicting_location",
               vname(ver));
}

static void test_end_of_track_overtakes_tail(moq_version_t ver)
{
    eot_overtake_run(ver, false);
    /* A repeated, identical END_OF_TRACK changes nothing. */
    eot_overtake_run(ver, true);
    eot_conflict_run(ver, false);
    eot_conflict_run(ver, true);
}

/* -- The sender's byte stream into the receiver --------------------------- */

typedef struct e2e {
    moq_simpair_t        *sp;
    moq_session_t        *cl, *srv;
    moq_media_sender_t   *s;
    moq_media_track_t    *st;
    moq_media_receiver_t *r;
    moq_media_track_t    *rt;
    uint64_t              now;
    int                   ended;
} e2e_t;

static void e2e_cycle(e2e_t *e)
{
    moq_media_sender_test_pump(e->s, e->srv, e->now);
    moq_simpair_run_until_quiescent(e->sp, 64, NULL);
    if (e->r) moq_media_receiver_test_pump(e->r, e->cl, e->now);
    moq_simpair_run_until_quiescent(e->sp, 64, NULL);
    moq_media_sender_test_pump(e->s, e->srv, e->now);
    moq_simpair_run_until_quiescent(e->sp, 64, NULL);
    if (e->r) moq_media_receiver_test_pump(e->r, e->cl, e->now);
}

static bool e2e_setup(e2e_t *e, moq_version_t ver, uint32_t ps,
                      const uint8_t *psi, size_t psi_len, bool strip,
                      bool join)
{
    memset(e, 0, sizeof(*e));
    e->now = 1000;
    e->sp = make_pair(ver);
    if (!e->sp) return false;
    e->cl = moq_simpair_client(e->sp);
    e->srv = moq_simpair_server(e->sp);

    moq_media_sender_cfg_t sc;
    moq_media_sender_cfg_init_lossless_sized(&sc, sizeof(sc));
    sc.namespace_ = (moq_namespace_t){ g_ns, 2 };
    sc.backpressure = MOQ_MEDIA_SEND_BP_RETURN_WOULD_BLOCK;
    sc.catalog_refresh_interval_us = UINT64_MAX;
    moq_media_sender_test_set_alloc(g_fx_alloc);
    e->s = moq_media_sender_test_new_cfg(&sc);
    moq_media_sender_test_set_alloc(NULL);
    if (!e->s) return false;
    moq_msf_mpeg2ts_t rec;
    moq_msf_mpeg2ts_init(&rec);
    rec.has_mode = true;
    rec.mode = MOQ_MSF_MPEG2TS_MODE_UNMODIFIED_PROGRAM;
    rec.has_packet_size = true;
    rec.packet_size = ps;
    rec.has_program_number = true;
    rec.program_number = 1;
    moq_msf_track_ext_t ext = { .kind = MOQ_MSF_EXT_MPEG2TS,
                                .u = { .mpeg2ts = &rec } };
    moq_media_track_cfg_t tc;
    moq_media_track_cfg_init_sized(&tc, sizeof(tc));
    tc.name = (moq_bytes_t){ (const uint8_t *)"prog", 4 };
    tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
    tc.packaging = MOQ_MEDIA_PACKAGING_MPEG2TS;
    tc.bitrate = 4000000;
    tc.init_data = (moq_bytes_t){ psi, psi_len };
    tc.extensions = &ext;
    tc.extension_count = 1;
    tc.mpeg2ts_object_packets = 4;
    if (moq_media_sender_add_track(e->s, &tc, &e->st) != MOQ_OK) return false;

    /* The sender's namespace is accepted before the receiver exists: the
     * receiver's subscriber owns the client session's events from then on. */
    for (int i = 0; i < 16 && !moq_media_sender_is_ready(e->s); i++) {
        e2e_cycle(e);
        moq_event_t ev;
        while (moq_session_poll_events(e->cl, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED) {
                moq_accept_namespace_cfg_t ac;
                moq_accept_namespace_cfg_init(&ac);
                (void)moq_session_accept_namespace(
                    e->cl, ev.u.namespace_published.ann, &ac, e->now);
            }
            moq_event_cleanup(&ev);
        }
    }
    if (!moq_media_sender_is_ready(e->s)) return false;

    moq_media_receiver_cfg_t rc;
    moq_media_receiver_cfg_init_live_sized(&rc, sizeof(rc));
    rc.namespace_ = (moq_namespace_t){ g_ns, 2 };
    rc.auto_subscribe = true;
    rc.overflow.policy = MOQ_MEDIA_OVERFLOW_FLOW_CONTROL;
    rc.overflow.max_objects = 1024;
    rc.mpeg2ts_strip_prefix = strip;
    rc.mpeg2ts_join_psi = join;
    e->r = moq_media_receiver_test_new_cfg(&rc);
    if (!e->r) return false;
    for (int i = 0; i < 32 &&
         !(e->rt && moq_media_sender_track_has_subscriber(e->s, e->st)); i++) {
        e2e_cycle(e);
        moq_media_track_t *t = drain_events(e->r, "prog", &e->ended, NULL);
        if (t) e->rt = t;
    }
    return e->rt && moq_media_sender_track_has_subscriber(e->s, e->st);
}

static void e2e_free(e2e_t *e)
{
    if (e->r) moq_media_receiver_test_destroy_with_session(e->r, e->cl, e->now);
    if (e->s) moq_media_sender_test_free(e->s);
    if (e->sp) {
        moq_event_t ev;
        while (moq_session_poll_events(e->cl, &ev, 1) == 1) moq_event_cleanup(&ev);
        while (moq_session_poll_events(e->srv, &ev, 1) == 1) moq_event_cleanup(&ev);
        moq_simpair_destroy(e->sp);
    }
}

#define E2E_GOPS 4u
#define E2E_MAX  (E2E_GOPS * 9u * 192u + 4u * 192u)

/* Write the stream in uneven chunks, end the track, and collect what the
 * receiver delivers until TRACK_ENDED. */
static size_t e2e_run(e2e_t *e, const uint8_t *src, size_t len, uint8_t *out,
                      size_t cap, int *objects, int *gaps, int *group_starts,
                      bool *starts_ok)
{
    size_t off = 0, out_len = 0;
    static const size_t chunks[] = { 1, 187, 400, 3000, 64, 999 };
    for (int i = 0; off < len && i < 10000; i++) {
        size_t n = chunks[i % 6];
        if (n > len - off) n = len - off;
        moq_media_send_mpeg2ts_t in;
        memset(&in, 0, sizeof(in));
        in.struct_size = sizeof(in);
        in.data = (moq_bytes_t){ src + off, n };
        size_t c = 0;
        moq_result_t rc = moq_media_sender_write_mpeg2ts(e->s, e->st, &in, &c);
        off += c;
        MOQ_TEST_CHECK(rc == MOQ_OK || rc == MOQ_ERR_WOULD_BLOCK);
        if (rc != MOQ_OK) e2e_cycle(e);
        if (i % 7 == 0) e2e_cycle(e);
    }
    for (int i = 0; i < 64; i++) {
        moq_result_t rc = moq_media_sender_end_track(e->s, e->st);
        if (rc == MOQ_OK) break;
        e2e_cycle(e);
    }
    *objects = 0; *gaps = 0; *group_starts = 0; *starts_ok = true;
    for (int i = 0; i < 64 && e->ended == 0; i++) {
        e2e_cycle(e);
        moq_media_object_t o;
        while (moq_media_receiver_poll_object(e->r, &o, sizeof(o)) == MOQ_OK) {
            if (o.status == MOQ_OBJECT_NORMAL) {
                (*objects)++;
                if (!o.has_loss_report || o.gap) (*gaps)++;
                if (o.object_id == 0) {
                    (*group_starts)++;
                    /* A Group opens with its PAT and a random access point. */
                    const uint8_t *p = o.payload.data;
                    if (!(o.keyframe && o.payload.len >= 188 && p[0] == 0x47 &&
                          (p[1] & 0x1F) == 0 && p[2] == 0))
                        *starts_ok = false;
                }
                if (out_len + o.payload.len <= cap) {
                    memcpy(out + out_len, o.payload.data, o.payload.len);
                    out_len += o.payload.len;
                }
            }
            moq_media_object_cleanup(&o);
        }
        (void)drain_events(e->r, "prog", &e->ended, NULL);
    }
    return out_len;
}

static void test_e2e(moq_version_t ver)
{
    int before = failures;
    ts_gen_t g;
    ts_gen_init(&g, 188, 1, false);
    static uint8_t src[E2E_MAX], out[E2E_MAX];
    size_t len = 0;
    for (unsigned i = 0; i < E2E_GOPS; i++) len += ts_gen_gop(&g, src + len, 4);
    e2e_t e;
    if (!e2e_setup(&e, ver, 188, src, 2 * 188, false, false)) {
        failures++; e2e_free(&e); return;
    }
    const moq_media_track_desc_t *d = moq_media_track_desc_get(e.rt);
    MOQ_TEST_CHECK_EQ_INT((int)d->info.packaging,
                          (int)MOQ_MEDIA_PACKAGING_MPEG2TS);
    const moq_msf_mpeg2ts_t *m =
        moq_msf_ext_mpeg2ts(d->extensions, d->extension_count);
    MOQ_TEST_CHECK(m && m->has_random_access && m->random_access);
    MOQ_TEST_CHECK(d->init_data.len == 2 * 188 &&
                   memcmp(d->init_data.data, src, 2 * 188) == 0);
    int objects = 0, gaps = 0, starts = 0;
    bool starts_ok = false;
    size_t out_len = e2e_run(&e, src, len, out, sizeof(out), &objects, &gaps,
                             &starts, &starts_ok);
    MOQ_TEST_CHECK_EQ_INT(e.ended, 1);
    MOQ_TEST_CHECK_EQ_U64(out_len, len);
    MOQ_TEST_CHECK(out_len == len && memcmp(out, src, len) == 0);
    MOQ_TEST_CHECK(objects > (int)E2E_GOPS);
    MOQ_TEST_CHECK_EQ_INT(gaps, 0);
    MOQ_TEST_CHECK_EQ_INT(starts, (int)E2E_GOPS);
    MOQ_TEST_CHECK(starts_ok);
    moq_media_receiver_stats_t st;
    MOQ_TEST_CHECK(moq_media_receiver_get_stats(e.r, &st, sizeof(st)) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(st.gap_tails_unknown, 0);
    MOQ_TEST_CHECK_EQ_U64(st.reorder_refused, 0);
    e2e_free(&e);
    if (failures == before) printf("PASS: ts_e2e_%s\n", vname(ver));
}

/* Receive Objects into `got` until TRACK_ENDED. */
static int e2e_collect(e2e_t *e, got_t *got, int cap)
{
    int n = 0;
    for (int i = 0; i < 64 && e->ended == 0; i++) {
        e2e_cycle(e);
        e->now += 100000;  /* a lost Object waits for the reorder window */
        moq_media_object_t o;
        while (moq_media_receiver_poll_object(e->r, &o, sizeof(o)) == MOQ_OK) {
            if (o.status == MOQ_OBJECT_NORMAL && n < cap) {
                got_t *g = &got[n++];
                memset(g, 0, sizeof(*g));
                g->group = o.group_id;
                g->object = o.object_id;
                g->gap = o.gap;
                g->tail_unknown = o.gap_tail_unknown;
                g->gap_group = o.gap_group;
                g->gap_object = o.gap_object;
            }
            moq_media_object_cleanup(&o);
        }
        (void)drain_events(e->r, "prog", &e->ended, NULL);
    }
    return n;
}

static moq_result_t e2e_write(e2e_t *e, const uint8_t *d, size_t n,
                              size_t *consumed)
{
    moq_media_send_mpeg2ts_t in;
    memset(&in, 0, sizeof(in));
    in.struct_size = sizeof(in);
    in.data = (moq_bytes_t){ d, n };
    return moq_media_sender_write_mpeg2ts(e->s, e->st, &in, consumed);
}

/* A continuation the sender cannot copy, after its Group's first Object is
 * already on the wire: that Group's subgroup is reset, not finished, so the
 * receiver cannot take the truncated Group as complete -- the next Group
 * reports the uncertain tail. The Groups after it are untouched. */
static void test_e2e_copy_loss_resets(moq_version_t ver)
{
    int before = failures;
    ts_gen_t g;
    ts_gen_init(&g, 188, 1, false);
    static uint8_t src[E2E_MAX];
    size_t len = 0;
    for (unsigned i = 0; i < 3; i++) len += ts_gen_gop(&g, src + len, 4);
    const size_t gop = 8 * 188;   /* PAT PMT RAP v v v v A: two Objects */
    e2e_t e;
    g_fx_alloc = &g_gate;
    bool ok = e2e_setup(&e, ver, 188, src, 2 * 188, false, false);
    g_fx_alloc = NULL;
    if (!ok) { failures++; e2e_free(&e); return; }
    size_t c = 0;
    MOQ_TEST_CHECK(e2e_write(&e, src, 4 * 188, &c) == MOQ_OK);
    e2e_cycle(&e);                       /* (0,0) is on the wire */
    g_refuse_next = 1;
    MOQ_TEST_CHECK_EQ_INT((int)e2e_write(&e, src + 4 * 188, 4 * 188, &c),
                          (int)MOQ_ERR_NOMEM);
    MOQ_TEST_CHECK_EQ_INT(g_refuse_next, 0);
    g_refuse_next = 0;
    MOQ_TEST_CHECK(e2e_write(&e, src + gop, len - gop, &c) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(c, len - gop);
    for (int i = 0; i < 64; i++) {
        if (moq_media_sender_end_track(e.s, e.st) == MOQ_OK) break;
        e2e_cycle(&e);
    }
    got_t got[16];
    int n = e2e_collect(&e, got, 16);
    MOQ_TEST_CHECK_EQ_INT(e.ended, 1);
    /* (0,0), then Groups 1 and 2 whole. */
    MOQ_TEST_CHECK_EQ_INT(n, 5);
    if (n == 5) {
        MOQ_TEST_CHECK(got[0].group == 0 && got[0].object == 0 && !got[0].gap);
        MOQ_TEST_CHECK(got[1].group == 1 && got[1].object == 0);
        MOQ_TEST_CHECK(got[1].gap && got[1].tail_unknown &&
                       got[1].gap_group == 0 && got[1].gap_object == 1);
        for (int i = 2; i < 5; i++) MOQ_TEST_CHECK(!got[i].gap);
        MOQ_TEST_CHECK(got[4].group == 2 && got[4].object == 1);
    }
    moq_media_sender_track_stats_t sst;
    MOQ_TEST_CHECK(moq_media_sender_get_track_stats(e.s, e.st, &sst,
                                                    sizeof(sst)) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(sst.groups_abandoned, 1);
    e2e_free(&e);
    if (failures == before)
        printf("PASS: ts_e2e_copy_loss_resets_%s\n", vname(ver));
}

/* A Group start the sender cannot copy loses only its own Group: the Group
 * before it, already complete, still ends with a FIN, and the next Group
 * reports the lost one as a known gap. */
static void test_e2e_lost_start_keeps_previous(moq_version_t ver)
{
    int before = failures;
    ts_gen_t g;
    ts_gen_init(&g, 188, 1, false);
    static uint8_t src[E2E_MAX];
    size_t len = 0;
    for (unsigned i = 0; i < 3; i++) len += ts_gen_gop(&g, src + len, 4);
    const size_t gop = 8 * 188;
    e2e_t e;
    g_fx_alloc = &g_gate;
    bool ok = e2e_setup(&e, ver, 188, src, 2 * 188, false, false);
    g_fx_alloc = NULL;
    if (!ok) { failures++; e2e_free(&e); return; }
    size_t c = 0;
    /* Group 0 whole (its last Object leaves when Group 1 starts). */
    MOQ_TEST_CHECK(e2e_write(&e, src, gop, &c) == MOQ_OK);
    e2e_cycle(&e);
    /* Group 1's first Object, completed by its fourth packet, is lost. */
    g_refuse_next = 1;
    MOQ_TEST_CHECK_EQ_INT((int)e2e_write(&e, src + gop, 4 * 188, &c),
                          (int)MOQ_ERR_NOMEM);
    g_refuse_next = 0;
    MOQ_TEST_CHECK(e2e_write(&e, src + gop + 4 * 188, len - gop - 4 * 188,
                             &c) == MOQ_OK);
    for (int i = 0; i < 64; i++) {
        if (moq_media_sender_end_track(e.s, e.st) == MOQ_OK) break;
        e2e_cycle(&e);
    }
    got_t got[16];
    int n = e2e_collect(&e, got, 16);
    MOQ_TEST_CHECK_EQ_INT(e.ended, 1);
    MOQ_TEST_CHECK_EQ_INT(n, 4);
    if (n == 4) {
        MOQ_TEST_CHECK(got[0].group == 0 && got[1].group == 0 &&
                       !got[0].gap && !got[1].gap);
        MOQ_TEST_CHECK(got[2].object == 0 && got[2].gap &&
                       !got[2].tail_unknown &&
                       got[2].gap_group == 1 && got[2].gap_object == 0);
        MOQ_TEST_CHECK(!got[3].gap);
    }
    moq_media_sender_track_stats_t sst;
    MOQ_TEST_CHECK(moq_media_sender_get_track_stats(e.s, e.st, &sst,
                                                    sizeof(sst)) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(sst.groups_abandoned, 0);
    e2e_free(&e);
    if (failures == before)
        printf("PASS: ts_e2e_lost_start_keeps_previous_%s\n", vname(ver));
}

/* -- A TS continuation across a demand change ------------------------------ */

static moq_result_t loc_write(moq_media_sender_t *s, moq_media_track_t *t,
                              uint8_t tag)
{
    uint8_t buf[16];
    memset(buf, tag, sizeof(buf));
    moq_rcbuf_t *b = NULL;
    moq_rcbuf_create(moq_alloc_default(), buf, sizeof(buf), &b);
    moq_media_send_object_t o;
    memset(&o, 0, sizeof(o));
    o.struct_size = sizeof(o);
    o.payload = b;
    o.is_sync = true;
    o.starts_group = true;
    o.presentation_time_us = 1000u * tag;
    moq_result_t rc = moq_media_sender_write(s, t, &o);
    if (rc != MOQ_OK) moq_rcbuf_decref(b);
    return rc;
}

static moq_result_t ts_write(moq_media_sender_t *s, moq_media_track_t *t,
                             const uint8_t *d, size_t n)
{
    moq_media_send_mpeg2ts_t in;
    memset(&in, 0, sizeof(in));
    in.struct_size = sizeof(in);
    in.data = (moq_bytes_t){ d, n };
    size_t c = 0;
    moq_result_t rc = moq_media_sender_write_mpeg2ts(s, t, &in, &c);
    MOQ_TEST_CHECK_EQ_U64(c, n);
    return rc;
}

/* A TS Group whose start was dropped for want of demand stays abandoned when
 * demand arrives before its queued continuation drains. Queue order: an
 * audio Group, the TS Group's first Object (PAT PMT RAP), six more audio
 * Groups, then its continuation. The first pass sends the audio Group, drops
 * the TS start (no subscriber) and stops on shared capacity before the
 * continuation; the TS track is then subscribed. The continuation is dropped,
 * never sent as Object 0 of Group 0, and the next TS Group arrives as (1,0). */
static void test_ts_no_demand_suffix(moq_version_t ver)
{
    int before = failures;
    ts_gen_t g;
    ts_gen_init(&g, 188, 1, false);
    static uint8_t src[2 * 9 * 188];
    size_t len = ts_gen_gop(&g, src, 5);       /* PAT PMT RAP v v v v v A */
    len += ts_gen_gop(&g, src + len, 5);
    e2e_t e;
    memset(&e, 0, sizeof(e));
    e.now = 1000;
    moq_simpair_cfg_t pc = MOQ_SIMPAIR_CFG_INIT;
    pc.alloc = moq_alloc_default();
    pc.seed = 11;
    pc.initial_now_us = 1000;
    pc.client_send_request_capacity = true;
    pc.client_initial_request_capacity = 32;
    pc.server_send_request_capacity = true;
    pc.server_initial_request_capacity = 32;
    pc.version = ver;
    pc.max_actions = 4;               /* a pass stops on shared capacity */
    if (moq_simpair_create(&pc, &e.sp) != MOQ_OK) { failures++; return; }
    moq_simpair_start(e.sp);
    moq_simpair_run_until_quiescent(e.sp, 64, NULL);
    e.cl = moq_simpair_client(e.sp);
    e.srv = moq_simpair_server(e.sp);

    moq_media_sender_cfg_t sc;
    moq_media_sender_cfg_init_live_sized(&sc, sizeof(sc));
    sc.namespace_ = (moq_namespace_t){ g_ns, 2 };
    sc.drop_without_demand = true;
    sc.catalog_refresh_interval_us = UINT64_MAX;
    e.s = moq_media_sender_test_new_cfg(&sc);
    if (!e.s) { failures++; e2e_free(&e); return; }
    moq_msf_mpeg2ts_t rec;
    moq_msf_mpeg2ts_init(&rec);
    rec.has_mode = true;
    rec.mode = MOQ_MSF_MPEG2TS_MODE_UNMODIFIED_PROGRAM;
    rec.has_packet_size = true;
    rec.packet_size = 188;
    moq_msf_track_ext_t ext = { .kind = MOQ_MSF_EXT_MPEG2TS,
                                .u = { .mpeg2ts = &rec } };
    moq_media_track_cfg_t tc;
    moq_media_track_cfg_init(&tc);
    tc.name = (moq_bytes_t){ (const uint8_t *)"prog", 4 };
    tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
    tc.packaging = MOQ_MEDIA_PACKAGING_MPEG2TS;
    tc.bitrate = 2000000;
    tc.extensions = &ext;
    tc.extension_count = 1;
    tc.mpeg2ts_object_packets = 3;
    MOQ_TEST_CHECK(moq_media_sender_add_track(e.s, &tc, &e.st) == MOQ_OK);
    moq_media_track_cfg_init(&tc);
    tc.name = (moq_bytes_t){ (const uint8_t *)"a", 1 };
    tc.media_type = MOQ_MEDIA_TYPE_AUDIO;
    tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
    tc.codec = (moq_bytes_t){ (const uint8_t *)"opus", 4 };
    tc.bitrate = 64000;
    tc.samplerate = 48000;
    tc.channel_config = (moq_bytes_t){ (const uint8_t *)"2", 1 };
    moq_media_track_t *audio = NULL;
    MOQ_TEST_CHECK(moq_media_sender_add_track(e.s, &tc, &audio) == MOQ_OK);
    if (!e.st || !audio) { failures++; e2e_free(&e); return; }

    /* The peer accepts the namespace and subscribes the audio track only. */
    moq_subscription_t sub_a = { 0 }, sub_ts = { 0 };
    bool have_a = false, have_ts = false;
    int ts_n = 0;
    uint64_t ts_g[16], ts_o[16];
    for (int i = 0; i < 64; i++) {
        e2e_cycle(&e);
        moq_event_t ev;
        while (moq_session_poll_events(e.cl, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED) {
                moq_accept_namespace_cfg_t ac;
                moq_accept_namespace_cfg_init(&ac);
                (void)moq_session_accept_namespace(
                    e.cl, ev.u.namespace_published.ann, &ac, e.now);
            }
            moq_event_cleanup(&ev);
        }
        if (moq_media_sender_is_ready(e.s) && !have_a) {
            moq_subscribe_cfg_t q;
            moq_subscribe_cfg_init(&q);
            q.track_namespace = (moq_namespace_t){ g_ns, 2 };
            q.track_name = (moq_bytes_t){ (const uint8_t *)"a", 1 };
            q.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
            have_a = moq_session_subscribe(e.cl, &q, e.now, &sub_a) == MOQ_OK;
        }
        if (have_a && moq_media_sender_track_has_subscriber(e.s, audio)) break;
    }
    MOQ_TEST_CHECK(moq_media_sender_track_has_subscriber(e.s, audio));

    /* Queue: audio, TS start, six audio Groups, TS continuation. */
    MOQ_TEST_CHECK(loc_write(e.s, audio, 0x10) == MOQ_OK);
    MOQ_TEST_CHECK(ts_write(e.s, e.st, src, 3 * 188) == MOQ_OK);
    for (int i = 0; i < 6; i++)
        MOQ_TEST_CHECK(loc_write(e.s, audio, (uint8_t)(0x11 + i)) == MOQ_OK);
    MOQ_TEST_CHECK(ts_write(e.s, e.st, src + 3 * 188, 3 * 188) == MOQ_OK);

    moq_media_sender_test_pump(e.s, e.srv, e.now);
    moq_simpair_run_until_quiescent(e.sp, 64, NULL);
    moq_media_sender_track_stats_t st;
    MOQ_TEST_CHECK(moq_media_sender_get_track_stats(e.s, e.st, &st,
                                                    sizeof(st)) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(st.objects_dropped, 1);   /* the TS start */
    MOQ_TEST_CHECK_EQ_U64(st.objects_queued, 1);    /* its continuation */

    /* Demand arrives while the continuation is still queued. */
    moq_subscribe_cfg_t q;
    moq_subscribe_cfg_init(&q);
    q.track_namespace = (moq_namespace_t){ g_ns, 2 };
    q.track_name = (moq_bytes_t){ (const uint8_t *)"prog", 4 };
    q.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    have_ts = moq_session_subscribe(e.cl, &q, e.now, &sub_ts) == MOQ_OK;
    MOQ_TEST_CHECK(have_ts);
    bool next_written = false;
    for (int i = 0; i < 64; i++) {
        e2e_cycle(&e);
        moq_event_t ev;
        while (moq_session_poll_events(e.cl, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED && have_ts &&
                moq_subscription_eq(ev.u.object_received.sub, sub_ts) &&
                ev.u.object_received.status == MOQ_OBJECT_NORMAL &&
                ts_n < 16) {
                ts_g[ts_n] = ev.u.object_received.group_id;
                ts_o[ts_n] = ev.u.object_received.object_id;
                ts_n++;
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(moq_media_sender_get_track_stats(e.s, e.st, &st,
                                                        sizeof(st)) == MOQ_OK);
        if (!next_written && st.objects_queued == 0 &&
            moq_media_sender_track_has_subscriber(e.s, e.st)) {
            /* The next TS Group, with its PAT, PMT and random access point. */
            MOQ_TEST_CHECK(ts_write(e.s, e.st, src + 6 * 188, len - 6 * 188) ==
                           MOQ_OK);
            next_written = true;
        }
        if (next_written && ts_n >= 1 && st.objects_queued == 0) break;
    }
    MOQ_TEST_CHECK(next_written);
    MOQ_TEST_CHECK(ts_n >= 1);
    for (int i = 0; i < ts_n; i++)
        MOQ_TEST_CHECK(ts_g[i] == 1);       /* nothing of Group 0 was sent */
    if (ts_n >= 1) MOQ_TEST_CHECK(ts_o[0] == 0);
    MOQ_TEST_CHECK(moq_media_sender_get_track_stats(e.s, e.st, &st,
                                                    sizeof(st)) == MOQ_OK);
    /* The start, the queued continuation, and Group 0's last Object (the
     * rest of that GOP, written with the next one): the Group stays
     * abandoned to its end. */
    MOQ_TEST_CHECK_EQ_U64(st.objects_dropped, 3);
    MOQ_TEST_CHECK_EQ_U64(st.groups_dropped, 1);
    e2e_free(&e);
    if (failures == before)
        printf("PASS: ts_no_demand_suffix_%s\n", vname(ver));
}

/* -- Parity: one app flow for LOC, CMAF and TS ---------------------------- */

#define PAR_UNITS 6

typedef struct par_run {
    int      kinds[16];      /* poll_track kinds, in order */
    int      nkinds;
    uint64_t group[PAR_UNITS], object[PAR_UNITS];
    bool     keyframe[PAR_UNITS], gap[PAR_UNITS];
    int      objects;
    bool     ready;
    uint64_t written, sent, dropped, rx_received, rx_dropped;
} par_run_t;

static void par_events(e2e_t *e, par_run_t *pr)
{
    moq_media_track_event_t ev;
    while (moq_media_receiver_poll_track(e->r, &ev, sizeof(ev)) == MOQ_OK) {
        if (ev.kind == MOQ_MEDIA_TRACK_ADDED && ev.desc &&
            span_is(ev.desc->name, "m"))
            e->rt = ev.track;
        if (ev.track && ev.track != e->rt) continue;   /* the catalog's own */
        if (pr->nkinds < 16) pr->kinds[pr->nkinds++] = (int)ev.kind;
        if (ev.kind == MOQ_MEDIA_TRACK_ENDED) e->ended++;
    }
}

/* One unit of the flow: units 0 and 3 start a Group with a sync point. */
static moq_result_t par_write(e2e_t *e, moq_media_packaging_t pk, int i,
                              const uint8_t *ts, const size_t *ts_off)
{
    if (pk == MOQ_MEDIA_PACKAGING_MPEG2TS) {
        moq_media_send_mpeg2ts_t in;
        memset(&in, 0, sizeof(in));
        in.struct_size = sizeof(in);
        in.data = (moq_bytes_t){ ts + ts_off[i], ts_off[i + 1] - ts_off[i] };
        in.end_object = true;
        size_t c = 0;
        moq_result_t rc = moq_media_sender_write_mpeg2ts(e->s, e->st, &in, &c);
        MOQ_TEST_CHECK_EQ_U64(c, in.data.len);
        return rc;
    }
    bool start = i % 3 == 0;
    uint8_t buf[512];
    size_t n;
    if (pk == MOQ_MEDIA_PACKAGING_CMAF) {
        n = moq_test_build_cmaf_chunk(buf, 1, start ? 0x02000000u : 0x01010000u,
                                      true, false, (uint8_t)(0x30 + i));
    } else {
        n = 32;
        memset(buf, 0x30 + i, n);
    }
    moq_rcbuf_t *b = NULL;
    moq_rcbuf_create(moq_alloc_default(), buf, n, &b);
    moq_media_send_object_t o;
    memset(&o, 0, sizeof(o));
    o.struct_size = sizeof(o);
    o.payload = b;
    o.is_sync = start;
    o.starts_group = start;
    o.presentation_time_us = 1000u + (uint64_t)i * 33333u;
    o.decode_time_us = o.presentation_time_us;
    moq_result_t rc = moq_media_sender_write(e->s, e->st, &o);
    if (rc != MOQ_OK) moq_rcbuf_decref(b);
    return rc;
}

static void par_flow(moq_version_t ver, moq_media_packaging_t pk,
                     par_run_t *pr)
{
    memset(pr, 0, sizeof(*pr));
    /* The TS units: two GOPs (PAT PMT RAP v v A), each split as
     * [PAT PMT RAP] [v v] [A]. */
    ts_gen_t g;
    ts_gen_init(&g, 188, 1, false);
    static uint8_t ts[2 * 6 * 188];
    size_t len = ts_gen_gop(&g, ts, 2);
    len += ts_gen_gop(&g, ts + len, 2);
    static const size_t ts_off[PAR_UNITS + 1] = {
        0, 3 * 188, 5 * 188, 6 * 188, 9 * 188, 11 * 188, 12 * 188 };
    MOQ_TEST_CHECK_EQ_U64(len, ts_off[PAR_UNITS]);

    e2e_t e;
    memset(&e, 0, sizeof(e));
    e.now = 1000;
    e.sp = make_pair(ver);
    if (!e.sp) { failures++; return; }
    e.cl = moq_simpair_client(e.sp);
    e.srv = moq_simpair_server(e.sp);
    moq_media_sender_cfg_t sc;
    moq_media_sender_cfg_init_live_sized(&sc, sizeof(sc));
    sc.namespace_ = (moq_namespace_t){ g_ns, 2 };
    sc.catalog_refresh_interval_us = UINT64_MAX;
    e.s = moq_media_sender_test_new_cfg(&sc);
    if (!e.s) { failures++; e2e_free(&e); return; }

    moq_msf_mpeg2ts_t rec;
    moq_msf_mpeg2ts_init(&rec);
    rec.has_mode = true;
    rec.mode = MOQ_MSF_MPEG2TS_MODE_UNMODIFIED_PROGRAM;
    rec.has_packet_size = true;
    rec.packet_size = 188;
    rec.has_program_number = true;
    rec.program_number = 1;
    moq_msf_track_ext_t ext = { .kind = MOQ_MSF_EXT_MPEG2TS,
                                .u = { .mpeg2ts = &rec } };
    uint8_t init[256];
    moq_media_track_cfg_t tc;
    moq_media_track_cfg_init(&tc);
    tc.name = (moq_bytes_t){ (const uint8_t *)"m", 1 };
    tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
    tc.packaging = pk;
    tc.bitrate = 2000000;
    if (pk == MOQ_MEDIA_PACKAGING_MPEG2TS) {
        tc.extensions = &ext;
        tc.extension_count = 1;
        tc.init_data = (moq_bytes_t){ ts, 2 * 188 };
    } else {
        tc.codec = (moq_bytes_t){ (const uint8_t *)"avc1.64001f", 11 };
        tc.timescale = 90000;
        if (pk == MOQ_MEDIA_PACKAGING_CMAF)
            tc.init_data = (moq_bytes_t){ init, sender_test_cmaf_init(init, 1) };
    }
    MOQ_TEST_CHECK(moq_media_sender_add_track(e.s, &tc, &e.st) == MOQ_OK);
    if (!e.st) { failures++; e2e_free(&e); return; }

    for (int i = 0; i < 16 && !moq_media_sender_is_ready(e.s); i++) {
        e2e_cycle(&e);
        moq_event_t ev;
        while (moq_session_poll_events(e.cl, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED) {
                moq_accept_namespace_cfg_t ac;
                moq_accept_namespace_cfg_init(&ac);
                (void)moq_session_accept_namespace(
                    e.cl, ev.u.namespace_published.ann, &ac, e.now);
            }
            moq_event_cleanup(&ev);
        }
    }
    pr->ready = moq_media_sender_is_ready(e.s);

    moq_media_receiver_cfg_t rc;
    moq_media_receiver_cfg_init_live_sized(&rc, sizeof(rc));
    rc.namespace_ = (moq_namespace_t){ g_ns, 2 };
    rc.auto_subscribe = true;
    rc.overflow.max_objects = 64;
    e.r = moq_media_receiver_test_new_cfg(&rc);
    if (!e.r) { failures++; e2e_free(&e); return; }
    for (int i = 0; i < 32 &&
         !(e.rt && moq_media_sender_track_has_subscriber(e.s, e.st)); i++) {
        e2e_cycle(&e);
        par_events(&e, pr);
    }
    MOQ_TEST_CHECK(e.rt && moq_media_sender_track_has_subscriber(e.s, e.st));

    for (int i = 0; i < PAR_UNITS; i++) {
        MOQ_TEST_CHECK_EQ_INT((int)par_write(&e, pk, i, ts, ts_off),
                              (int)MOQ_OK);
        e2e_cycle(&e);
    }
    for (int i = 0; i < 16; i++) {
        if (moq_media_sender_end_track(e.s, e.st) == MOQ_OK) break;
        e2e_cycle(&e);
    }
    for (int i = 0; i < 64 && e.ended == 0; i++) {
        e2e_cycle(&e);
        moq_media_object_t o;
        while (moq_media_receiver_poll_object(e.r, &o, sizeof(o)) == MOQ_OK) {
            if (o.status == MOQ_OBJECT_NORMAL && pr->objects < PAR_UNITS) {
                int k = pr->objects++;
                pr->group[k] = o.group_id;
                pr->object[k] = o.object_id;
                pr->keyframe[k] = o.keyframe;
                pr->gap[k] = o.gap;
            }
            moq_media_object_cleanup(&o);
        }
        par_events(&e, pr);
    }
    moq_media_sender_track_stats_t st;
    MOQ_TEST_CHECK(moq_media_sender_get_track_stats(e.s, e.st, &st,
                                                    sizeof(st)) == MOQ_OK);
    pr->written = st.objects_written;
    pr->sent = st.objects_sent;
    pr->dropped = st.objects_dropped;
    moq_media_receiver_stats_t rs;
    MOQ_TEST_CHECK(moq_media_receiver_get_stats(e.r, &rs, sizeof(rs)) == MOQ_OK);
    pr->rx_received = rs.objects_received;
    pr->rx_dropped = rs.objects_dropped;
    e2e_free(&e);
}

/* The drop accounting and wait outcomes of one packaging under a small
 * queue that nothing drains: ready (bare endpoint), a three-Object queue,
 * nine units in three Groups. The wait outcomes are the ones that do not
 * block (ready with space, and the interrupt latch); a bare endpoint cannot
 * block, so the full-queue wait is not compared here. */
typedef struct par_drop {
    moq_result_t wait_before, wait_interrupted;
    moq_result_t write_rc[9];
    uint64_t     written, queued, dropped, groups_dropped, keyframes_dropped;
} par_drop_t;

static void par_drops(moq_media_packaging_t pk, par_drop_t *pd)
{
    memset(pd, 0, sizeof(*pd));
    ts_gen_t g;
    ts_gen_init(&g, 188, 1, false);
    static uint8_t ts[3 * 6 * 188];
    size_t len = 0;
    for (int i = 0; i < 3; i++) len += ts_gen_gop(&g, ts + len, 2);
    size_t off[10];
    for (int i = 0; i < 3; i++) {
        off[3 * i] = (size_t)i * 6 * 188;
        off[3 * i + 1] = off[3 * i] + 3 * 188;
        off[3 * i + 2] = off[3 * i] + 5 * 188;
    }
    off[9] = len;

    moq_media_sender_cfg_t sc;
    moq_media_sender_cfg_init_live_sized(&sc, sizeof(sc));
    sc.namespace_ = (moq_namespace_t){ g_ns, 2 };
    sc.queue_max_objects = 3;
    moq_media_sender_t *s = moq_media_sender_test_new_cfg(&sc);
    moq_endpoint_t *ep = moq_endpoint_test_make_bare();
    if (!s || !ep) { failures++; return; }
    moq_media_sender_test_set_ep(s, ep);
    moq_msf_mpeg2ts_t rec;
    moq_msf_mpeg2ts_init(&rec);
    rec.has_mode = true;
    rec.mode = MOQ_MSF_MPEG2TS_MODE_UNMODIFIED_PROGRAM;
    rec.has_packet_size = true;
    rec.packet_size = 188;
    moq_msf_track_ext_t ext = { .kind = MOQ_MSF_EXT_MPEG2TS,
                                .u = { .mpeg2ts = &rec } };
    uint8_t init[256];
    moq_media_track_cfg_t tc;
    moq_media_track_cfg_init(&tc);
    tc.name = (moq_bytes_t){ (const uint8_t *)"m", 1 };
    tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
    tc.packaging = pk;
    tc.bitrate = 2000000;
    if (pk == MOQ_MEDIA_PACKAGING_MPEG2TS) {
        tc.extensions = &ext;
        tc.extension_count = 1;
    } else {
        tc.codec = (moq_bytes_t){ (const uint8_t *)"avc1.64001f", 11 };
        tc.timescale = 90000;
        if (pk == MOQ_MEDIA_PACKAGING_CMAF)
            tc.init_data = (moq_bytes_t){ init, sender_test_cmaf_init(init, 1) };
    }
    e2e_t e;
    memset(&e, 0, sizeof(e));
    e.s = s;
    MOQ_TEST_CHECK(moq_media_sender_add_track(s, &tc, &e.st) == MOQ_OK);
    moq_media_sender_test_set_ready(s);
    pd->wait_before = moq_media_sender_wait(s, 0);
    for (int i = 0; i < 9 && e.st; i++)
        pd->write_rc[i] = par_write(&e, pk, i, ts, off);
    moq_endpoint_set_interrupted(ep, true);
    pd->wait_interrupted = moq_media_sender_wait(s, 0);
    moq_endpoint_set_interrupted(ep, false);
    moq_media_sender_track_stats_t st;
    if (e.st && moq_media_sender_get_track_stats(s, e.st, &st, sizeof(st)) ==
                    MOQ_OK) {
        pd->written = st.objects_written;
        pd->queued = st.objects_queued;
        pd->dropped = st.objects_dropped;
        pd->groups_dropped = st.groups_dropped;
        pd->keyframes_dropped = st.keyframes_dropped;
    }
    moq_media_sender_test_set_ep(s, NULL);
    moq_media_sender_test_free(s);
    moq_endpoint_test_free_bare(ep);
}

/* The same app flow -- live preset, add_track, ready, six units in two
 * Groups, end_track, an auto-subscribing receiver drained to TRACK_ENDED --
 * gives the same readiness, track events, Group and Object numbering,
 * keyframes and counters for LOC, CMAF and TS. */
static void test_parity(moq_version_t ver)
{
    int before = failures;
    static const moq_media_packaging_t pks[3] = {
        MOQ_MEDIA_PACKAGING_RAW, MOQ_MEDIA_PACKAGING_CMAF,
        MOQ_MEDIA_PACKAGING_MPEG2TS };
    static par_run_t runs[3];
    for (int p = 0; p < 3; p++) par_flow(ver, pks[p], &runs[p]);
    const par_run_t *ref = &runs[0];
    MOQ_TEST_CHECK(ref->ready);
    MOQ_TEST_CHECK_EQ_INT(ref->objects, PAR_UNITS);
    for (int i = 0; i < ref->objects; i++) {
        MOQ_TEST_CHECK(ref->group[i] == (uint64_t)(i / 3) &&
                       ref->object[i] == (uint64_t)(i % 3));
        MOQ_TEST_CHECK(ref->keyframe[i] == (i % 3 == 0) && !ref->gap[i]);
    }
    MOQ_TEST_CHECK(ref->nkinds >= 2 &&
                   ref->kinds[0] == (int)MOQ_MEDIA_TRACK_ADDED &&
                   ref->kinds[ref->nkinds - 1] == (int)MOQ_MEDIA_TRACK_ENDED);
    MOQ_TEST_CHECK(ref->written == PAR_UNITS && ref->sent == PAR_UNITS &&
                   ref->dropped == 0);
    MOQ_TEST_CHECK(ref->rx_received == PAR_UNITS && ref->rx_dropped == 0);
    for (int p = 1; p < 3; p++) {
        const par_run_t *r = &runs[p];
        bool same = r->ready == ref->ready && r->objects == ref->objects &&
                    r->nkinds == ref->nkinds &&
                    memcmp(r->kinds, ref->kinds,
                           (size_t)ref->nkinds * sizeof(int)) == 0 &&
                    memcmp(r->group, ref->group, sizeof(ref->group)) == 0 &&
                    memcmp(r->object, ref->object, sizeof(ref->object)) == 0 &&
                    memcmp(r->keyframe, ref->keyframe,
                           sizeof(ref->keyframe)) == 0 &&
                    memcmp(r->gap, ref->gap, sizeof(ref->gap)) == 0 &&
                    r->written == ref->written && r->sent == ref->sent &&
                    r->dropped == ref->dropped &&
                    r->rx_received == ref->rx_received &&
                    r->rx_dropped == ref->rx_dropped;
        if (!same) {
            printf("FAIL: parity %s differs from LOC (objects %d/%d kinds "
                   "%d/%d written %llu sent %llu rx %llu)\n",
                   p == 1 ? "CMAF" : "TS", r->objects, ref->objects,
                   r->nkinds, ref->nkinds, (unsigned long long)r->written,
                   (unsigned long long)r->sent,
                   (unsigned long long)r->rx_received);
            failures++;
        }
    }
    /* Under a three-Object queue nothing drains: the same write results,
     * drop accounting and wait outcomes for every packaging. */
    static par_drop_t drops[3];
    for (int p = 0; p < 3; p++) par_drops(pks[p], &drops[p]);
    const par_drop_t *dr = &drops[0];
    MOQ_TEST_CHECK_EQ_INT((int)dr->wait_before, (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)dr->wait_interrupted, (int)MOQ_ERR_INTERRUPTED);
    MOQ_TEST_CHECK(dr->written == 9 && dr->queued == 3 && dr->dropped == 6 &&
                   dr->groups_dropped == 2 && dr->keyframes_dropped == 2);
    for (int i = 0; i < 9; i++)
        MOQ_TEST_CHECK_EQ_INT((int)dr->write_rc[i], (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(dr->written, dr->queued + dr->dropped);
    MOQ_TEST_CHECK(dr->queued <= 3);
    for (int p = 1; p < 3; p++) {
        const par_drop_t *d = &drops[p];
        if (memcmp(d, dr, sizeof(*d)) != 0) {
            printf("FAIL: parity drops %s differs from LOC (written %llu/%llu "
                   "dropped %llu/%llu groups %llu/%llu keyframes %llu/%llu)\n", p == 1 ? "CMAF" : "TS",
                   (unsigned long long)d->written,
                   (unsigned long long)dr->written,
                   (unsigned long long)d->dropped,
                   (unsigned long long)dr->dropped,
                   (unsigned long long)d->groups_dropped,
                   (unsigned long long)dr->groups_dropped,
                   (unsigned long long)d->keyframes_dropped,
                   (unsigned long long)dr->keyframes_dropped);
            failures++;
        }
    }
    if (failures == before) printf("PASS: ts_parity_%s\n", vname(ver));
}

/* 192-octet packets: the prefixes removed on delivery, and the catalog PSI
 * ahead of the first Object with its continuity counters aligned. */
static void test_e2e_strip_join(moq_version_t ver)
{
    int before = failures;
    ts_gen_t g;
    ts_gen_init(&g, 192, 1, false);
    static uint8_t src[E2E_MAX], out[E2E_MAX + 4 * 192], want[E2E_MAX];
    size_t len = 0;
    for (unsigned i = 0; i < E2E_GOPS; i++) len += ts_gen_gop(&g, src + len, 4);
    /* The catalog PSI: the stream's first PAT and PMT. */
    uint8_t psi[2 * 192];
    memcpy(psi, src, sizeof(psi));
    e2e_t e;
    if (!e2e_setup(&e, ver, 192, psi, sizeof(psi), true, true)) {
        failures++; e2e_free(&e); return;
    }
    int objects = 0, gaps = 0, starts = 0;
    bool starts_ok = false;
    size_t out_len = e2e_run(&e, src, len, out, sizeof(out), &objects, &gaps,
                             &starts, &starts_ok);
    MOQ_TEST_CHECK_EQ_INT(e.ended, 1);
    MOQ_TEST_CHECK_EQ_INT(gaps, 0);
    /* Expected: the PSI packets, then every source packet, as 188 octets. */
    size_t wl = 0;
    for (size_t off = 0; off < sizeof(psi); off += 192) {
        memcpy(want + wl, psi + off + 4, 188);
        wl += 188;
    }
    for (size_t off = 0; off < len; off += 192) {
        memcpy(want + wl, src + off + 4, 188);
        wl += 188;
    }
    MOQ_TEST_CHECK_EQ_U64(out_len, wl);
    if (out_len == wl) {
        /* Everything after the PSI is the source; the PSI copies differ from
         * the source only in their continuity counters, set so the first
         * Object's own PAT and PMT follow them. */
        MOQ_TEST_CHECK(memcmp(out + 2 * 188, want + 2 * 188, wl - 2 * 188) == 0);
        for (int k = 0; k < 2; k++) {
            const uint8_t *a = out + (size_t)k * 188, *b = want + (size_t)k * 188;
            MOQ_TEST_CHECK(a[0] == 0x47 && a[1] == b[1] && a[2] == b[2] &&
                           memcmp(a + 4, b + 4, 184) == 0);
            uint8_t cc_psi = a[3] & 0x0F;
            uint8_t cc_next = out[(size_t)(k + 2) * 188 + 3] & 0x0F;
            MOQ_TEST_CHECK(((cc_psi + 1) & 0x0F) == cc_next);
        }
    }
    e2e_free(&e);
    if (failures == before) printf("PASS: ts_e2e_strip_join_%s\n", vname(ver));
}

#endif /* MOQ_TEST_HAVE_MPEG2TS */

int main(void)
{
    test_eligibility();
    test_receiver_cfg_abi();
#ifdef MOQ_TEST_HAVE_MPEG2TS
    for (int vi = 0; vi < 2; vi++) {
        moq_version_t v = vi ? MOQ_VERSION_DRAFT_18 : MOQ_VERSION_DRAFT_16;
        test_order_and_fin(v);
        test_end_of_group_status(v);
        test_stream_end_of_group_status(v);
        test_end_of_track_overtakes_tail(v);
        test_reset_tail_unknown(v);
        test_rejected_object(v);
        test_end_of_track_releases(v);
        test_catalog_interleaved_end(v);
        test_intake_loss_location(v);
        test_placeholder_keeps_gap(v);
        test_rejected_keeps_gap(v);
        test_local_loss(v);
        test_e2e(v);
        test_parity(v);
        test_ts_no_demand_suffix(v);
        test_e2e_copy_loss_resets(v);
        test_e2e_lost_start_keeps_previous(v);
        test_e2e_strip_join(v);
    }
#endif
    if (failures) {
        printf("FAIL: %d mpeg2ts receiver failures\n", failures);
        return 1;
    }
    printf("PASS: all mpeg2ts receiver tests\n");
    return 0;
}
