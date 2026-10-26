/*
 * SAP and media-timeline record delivery under app under-poll (scripted peer,
 * no network).
 *
 * A REAL moq_media_receiver_t is driven through its real endpoint pump hook
 * with a simpair client session, exactly as test_media_receiver_scripted does:
 * the scripted peer answers the receiver's own SUBSCRIBEs (request ids learned
 * from its outbound control actions) and injects subgroup data bytes. The
 * catalog declares a CMSF SAP event timeline ("sap") and an MSF §7 media
 * timeline ("history"); both are subscribed explicitly and their objects are
 * parsed by the real intake into the bounded typed record queues that
 * moq_media_receiver_poll_sap / poll_media_timeline drain.
 *
 * The contract these rows pin is BOUNDED, NEVER SILENTLY LOSSY delivery:
 * every record a well-formed timeline object carries is observed by the
 * application exactly once and in order. A record once admitted to the
 * bounded queue is retained until polled; when the queue is full, the rest of
 * the independent object is DEFERRED -- not admitted, not marked surfaced,
 * each counted in the receiver's deferral counter -- and the next MSF §7.3 /
 * §8.3 independent object (which carries "all ... records accumulated and
 * accessible up to that point") delivers the deferred tail once the
 * application has drained capacity. With no later independent object, the
 * deferral counter is the public witness of what the application has not
 * seen. A record that vanishes with no trace is what these rows refuse.
 *
 * Each family has: a no-overflow control (delivery and the independent-object
 * dedup work), the exact-capacity boundary (nothing deferred at the cap), the
 * over-capacity rows (cap + 1, and a deep overflow) recovered by the next
 * independent object with the exact deferral counts, and the no-next-object
 * rows where only the counter accounts for the tail. The boundary pair is
 * what proves the oracle observes the queue's capacity rather than an empty
 * queue.
 *
 * Profile boundary: the scripted peer speaks draft-16 (moq_d16_* control
 * frames, the 0x30 subgroup header). The behaviour under test sits AFTER
 * profile parsing: the SAP and media-timeline intakes consume the facade's
 * profile-agnostic moq_sub_object_t and feed the receiver's own bounded
 * record queues and per-track dedup cursors, which draft-18 sessions share
 * unchanged. A draft-18 peer would exercise the same code with different
 * bytes on the wire; it is not scripted here.
 *
 * Every allocation the receiver and the simulated session pair make goes
 * through one counting allocator, and each row ends balanced.
 */
#include <moq/media_receiver.h>
#include <moq/msf.h>
#include <moq/sim.h>
#include <moq/session.h>
#include <moq/control.h>
#include <moq/codec.h>
#include "test_support.h"

#include <inttypes.h>
#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

static int failures = 0;

/* -- Test seams (media_receiver.c, MOQ_MEDIA_RECEIVER_TESTING) ---------- */
moq_media_receiver_t *moq_media_receiver_test_new_cfg_alloc(
    const moq_media_receiver_cfg_t *cfg, const moq_alloc_t *alloc);
void moq_media_receiver_test_destroy_with_session(moq_media_receiver_t *r,
                                                  moq_session_t *session,
                                                  uint64_t now_us);
void moq_media_receiver_test_pump(moq_media_receiver_t *r,
                                  moq_session_t *session, uint64_t now_us);

/* The receiver's bounded record queue depth (media_receiver.c
 * RECEIVER_DEFAULT_SAP_RECORDS / RECEIVER_DEFAULT_MT_RECORDS). The boundary
 * rows below are what pin that this number is the queue's real capacity. */
#define RECORD_QUEUE_CAP 256
#define MAX_RECORDS 1024

static const char k_catalog_json[] =
    "{\"version\":1,\"generatedAt\":1,\"tracks\":["
    "{\"name\":\"v\",\"packaging\":\"loc\",\"isLive\":true,"
    "\"role\":\"video\",\"codec\":\"av01\",\"timescale\":1000000},"
    "{\"name\":\"sap\",\"packaging\":\"eventtimeline\",\"isLive\":true,"
    "\"eventType\":\"org.ietf.moq.cmsf.sap\",\"mimeType\":\"application/json\","
    "\"depends\":[\"v\"]},"
    "{\"name\":\"history\",\"packaging\":\"mediatimeline\",\"isLive\":true,"
    "\"mimeType\":\"application/json\",\"depends\":[\"v\"]}"
    "]}";

/* -- the counting allocator --------------------------------------------- */

typedef struct {
    size_t live_bytes, live_blocks, total_allocs;
} alloc_stats_t;

static void *counting_alloc(size_t size, void *ctx)
{
    alloc_stats_t *st = (alloc_stats_t *)ctx;
    void *p = malloc(size ? size : 1);
    if (p) { st->live_bytes += size; st->live_blocks++; st->total_allocs++; }
    return p;
}

static void *counting_realloc(void *ptr, size_t old_size, size_t new_size, void *ctx)
{
    alloc_stats_t *st = (alloc_stats_t *)ctx;
    void *p = realloc(ptr, new_size ? new_size : 1);
    if (!p) return NULL;
    if (ptr) st->live_bytes -= old_size; else st->live_blocks++;
    st->live_bytes += new_size;
    st->total_allocs++;
    return p;
}

static void counting_free(void *ptr, size_t size, void *ctx)
{
    alloc_stats_t *st = (alloc_stats_t *)ctx;
    if (!ptr) return;
    st->live_bytes -= size;
    st->live_blocks--;
    free(ptr);
}

/* -- scripted-peer byte builders ---------------------------------------- */

static size_t put_varint(uint8_t *buf, uint64_t v)
{
    if (v < 0x40) { buf[0] = (uint8_t)v; return 1; }
    if (v < 0x4000) {
        buf[0] = (uint8_t)(0x40 | (v >> 8));
        buf[1] = (uint8_t)(v & 0xff);
        return 2;
    }
    if (v < 0x40000000ull) {
        buf[0] = (uint8_t)(0x80 | (v >> 24));
        buf[1] = (uint8_t)((v >> 16) & 0xff);
        buf[2] = (uint8_t)((v >> 8) & 0xff);
        buf[3] = (uint8_t)(v & 0xff);
        return 4;
    }
    buf[0] = (uint8_t)(0xc0 | (v >> 56));
    for (int i = 1; i < 8; i++)
        buf[i] = (uint8_t)((v >> (8 * (7 - i))) & 0xff);
    return 8;
}

static size_t build_subscribe_ok(uint8_t *out, size_t out_cap,
                                 uint64_t request_id, uint64_t track_alias)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, out, out_cap);
    if (moq_d16_encode_subscribe_ok(&w, request_id, track_alias,
                                    NULL, 0, NULL, 0) < 0)
        return 0;
    return moq_buf_writer_offset(&w);
}

/* -- the harness -------------------------------------------------------- */

typedef struct {
    alloc_stats_t astats;
    moq_alloc_t alloc;
    moq_simpair_t *sp;
    moq_session_t *client, *server;
    moq_media_receiver_t *r;
    uint64_t now;
    uint64_t next_ref;

    bool have_catalog_rid, have_sap_rid, have_history_rid;
    uint64_t catalog_rid, sap_rid, history_rid;
    moq_media_track_t *sap, *history;
    bool ready;
    int track_events;          /* every event polled after discovery */
    int ended_events, parse_drop_events;
} harness_t;

static bool bytes_is(moq_bytes_t b, const char *s)
{
    size_t n = strlen(s);
    return b.len == n && memcmp(b.data, s, n) == 0;
}

/* Decode the receiver's outbound SUBSCRIBEs: the first is the catalog; later
 * ones are matched by the track name they carry. */
static void drain_and_learn(harness_t *h)
{
    moq_action_t acts[16];
    size_t n;
    while ((n = moq_session_poll_actions(h->client, acts, 16)) > 0) {
        for (size_t i = 0; i < n; i++) {
            if (acts[i].kind != MOQ_ACTION_SEND_CONTROL) continue;
            moq_control_envelope_t env;
            moq_buf_reader_t rd;
            moq_buf_reader_init(&rd, acts[i].u.send_control.data,
                                acts[i].u.send_control.len);
            if (moq_control_decode_envelope(&rd, &env) < 0) continue;
            if (env.msg_type != MOQ_D16_SUBSCRIBE) continue;
            moq_bytes_t ns_parts[8];
            moq_kvp_entry_t params[16];
            moq_d16_subscribe_t s;
            memset(&s, 0, sizeof(s));
            s.params = params;
            s.params_cap = 16;
            if (moq_d16_decode_subscribe(env.payload, env.payload_len,
                                         ns_parts, 8, &s) < 0)
                continue;
            if (!h->have_catalog_rid) {
                h->have_catalog_rid = true;
                h->catalog_rid = s.request_id;
            } else if (bytes_is(s.track_name, "sap")) {
                h->have_sap_rid = true;
                h->sap_rid = s.request_id;
            } else if (bytes_is(s.track_name, "history")) {
                h->have_history_rid = true;
                h->history_rid = s.request_id;
            }
        }
    }
}

static void poll_track_events(harness_t *h)
{
    moq_media_track_event_t te;
    while (moq_media_receiver_poll_track(h->r, &te, sizeof(te)) == MOQ_OK) {
        if (te.kind == MOQ_MEDIA_TRACK_ADDED && te.desc) {
            if (bytes_is(te.desc->name, "sap")) h->sap = te.track;
            else if (bytes_is(te.desc->name, "history")) h->history = te.track;
        } else if (te.kind == MOQ_MEDIA_CATALOG_READY) {
            h->ready = true;
        }
        if (h->ready && te.kind != MOQ_MEDIA_CATALOG_READY) {
            h->track_events++;
            if (te.kind == MOQ_MEDIA_TRACK_ENDED) h->ended_events++;
            if (te.kind == MOQ_MEDIA_TRACK_PARSE_DROP) h->parse_drop_events++;
        }
    }
}

static void pump(harness_t *h, int cycles)
{
    for (int c = 0; c < cycles; c++) {
        h->now += 1000;
        moq_media_receiver_test_pump(h->r, h->client, h->now);
        drain_and_learn(h);
        poll_track_events(h);
        (void)moq_session_process_pending(h->client, h->now);
    }
}

static void send_subscribe_ok(harness_t *h, uint64_t rid, uint64_t alias)
{
    uint8_t ctrl[256];
    size_t n = build_subscribe_ok(ctrl, sizeof(ctrl), rid, alias);
    MOQ_TEST_CHECK(n > 0);
    moq_result_t rc = moq_session_on_control_bytes(h->client, ctrl, n, h->now);
    MOQ_TEST_CHECK(rc >= 0);
}

/* One subgroup stream carrying ONE object (object 0 of `group`) on `alias`. */
static void feed_object(harness_t *h, uint64_t alias, uint64_t group,
                        const char *payload, size_t len)
{
    MOQ_TEST_CHECK(len > 0);                 /* a builder that truncated returns 0 */
    if (len == 0) return;
    size_t cap = len + 64;
    uint8_t *buf = (uint8_t *)malloc(cap);
    MOQ_TEST_CHECK(buf != NULL);
    if (!buf) return;
    size_t o = 0;
    buf[o++] = 0x30;
    o += put_varint(buf + o, alias);
    o += put_varint(buf + o, group);
    o += put_varint(buf + o, 0);
    o += put_varint(buf + o, (uint64_t)len);
    memcpy(buf + o, payload, len);
    o += len;
    moq_stream_ref_t ref = moq_stream_ref_from_u64(h->next_ref++);
    moq_result_t rc = moq_session_on_data_bytes(h->client, ref, buf, o,
                                                /*fin=*/true, h->now);
    if (rc < 0) fprintf(stderr, "data rejected: rc=%d\n", rc);
    MOQ_TEST_CHECK(rc >= 0);
    free(buf);
}

static bool harness_open(harness_t *h)
{
    memset(h, 0, sizeof(*h));
    h->alloc.ctx = &h->astats;
    h->alloc.alloc = counting_alloc;
    h->alloc.realloc = counting_realloc;
    h->alloc.free = counting_free;
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = &h->alloc;
    cfg.seed = 42;
    cfg.initial_now_us = 1000;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 16;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 16;
    MOQ_TEST_CHECK(moq_simpair_create(&cfg, &h->sp) == MOQ_OK);
    if (!h->sp) return false;
    moq_simpair_start(h->sp);
    moq_simpair_run_until_quiescent(h->sp, 16, NULL);
    h->client = moq_simpair_client(h->sp);
    h->server = moq_simpair_server(h->sp);
    MOQ_TEST_CHECK(moq_session_state(h->client) == MOQ_SESS_ESTABLISHED);
    { moq_event_t ev;
      while (moq_session_poll_events(h->client, &ev, 1) == 1) moq_event_cleanup(&ev);
      while (moq_session_poll_events(h->server, &ev, 1) == 1) moq_event_cleanup(&ev); }

    static moq_bytes_t ns_parts[2] = {
        MOQ_BYTES_LITERAL("svc"), MOQ_BYTES_LITERAL("demo") };
    moq_media_receiver_cfg_t rcfg;
    moq_media_receiver_cfg_init_live(&rcfg);
    rcfg.namespace_.parts = ns_parts;
    rcfg.namespace_.count = 2;
    rcfg.auto_subscribe = false;              /* manual: the timelines only */
    rcfg.time_mode = MOQ_MEDIA_TIME_RAW;
    rcfg.overflow.policy = MOQ_MEDIA_OVERFLOW_DROP_GROUP;
    rcfg.overflow.max_objects = 64;
    rcfg.overflow.max_bytes = 1u << 20;
    h->r = moq_media_receiver_test_new_cfg_alloc(&rcfg, &h->alloc);
    MOQ_TEST_CHECK(h->r != NULL);
    if (!h->r) return false;
    h->now = moq_simpair_now_us(h->sp);
    h->next_ref = 200;

    /* Catalog SUBSCRIBE -> OK(alias 0) -> the catalog object -> discovery. */
    for (int i = 0; i < 20 && !h->have_catalog_rid; i++) pump(h, 1);
    MOQ_TEST_CHECK(h->have_catalog_rid);
    if (!h->have_catalog_rid) return false;
    send_subscribe_ok(h, h->catalog_rid, 0);
    pump(h, 2);
    feed_object(h, 0, 0, k_catalog_json, sizeof(k_catalog_json) - 1);
    for (int i = 0; i < 20 && !(h->ready && h->sap && h->history); i++) pump(h, 1);
    MOQ_TEST_CHECK(h->ready && h->sap && h->history);
    if (!(h->ready && h->sap && h->history)) return false;

    /* Explicit subscribes; the hook issues the SUBSCRIBEs on the next pumps. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_subscribe_track(h->r, h->sap, NULL), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_subscribe_track(h->r, h->history, NULL), (int)MOQ_OK);
    for (int i = 0; i < 20 && !(h->have_sap_rid && h->have_history_rid); i++) pump(h, 1);
    MOQ_TEST_CHECK(h->have_sap_rid && h->have_history_rid);
    if (!(h->have_sap_rid && h->have_history_rid)) return false;
    send_subscribe_ok(h, h->sap_rid, 2);
    send_subscribe_ok(h, h->history_rid, 3);
    pump(h, 4);
    MOQ_TEST_CHECK(!moq_media_receiver_is_fatal(h->r));
    return !moq_media_receiver_is_fatal(h->r);
}

static void harness_close(harness_t *h)
{
    if (h->r) {
        moq_simpair_run_until_quiescent(h->sp, 16, NULL);
        h->now += 1000;
        moq_media_receiver_test_destroy_with_session(h->r, h->client, h->now);
        h->r = NULL;
    }
    if (h->sp) {
        moq_simpair_run_until_quiescent(h->sp, 16, NULL);
        moq_event_t ev;
        while (moq_session_poll_events(h->client, &ev, 1) == 1) moq_event_cleanup(&ev);
        while (moq_session_poll_events(h->server, &ev, 1) == 1) moq_event_cleanup(&ev);
        moq_simpair_destroy(h->sp);
        h->sp = NULL;
    }
    /* Balance over the receiver AND the session pair: nothing outlives the row. */
    MOQ_TEST_CHECK(h->astats.total_allocs > 0);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)h->astats.live_bytes, 0);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)h->astats.live_blocks, 0);
}

/* -- payload builders --------------------------------------------------- */

/* SAP records for media Locations (7, first..first+count-1): the JSON array of
 * {"l":[7,i],"data":[2,i*10]}. Returns the length. */
/* Append with a truncation guard: an append that would fill or exceed the
 * buffer fails, so a payload that no longer fits reaches the row as a fixture
 * error, never the parser as a different (shorter) object. Each helper's
 * format matches its arguments exactly. */
static bool append_lit(char *buf, size_t cap, size_t *o, const char *lit)
{
    size_t n = strlen(lit);
    if (*o >= cap || n >= cap - *o) return false;
    memcpy(buf + *o, lit, n + 1);
    *o += n;
    return true;
}

static bool append_sap_record(char *buf, size_t cap, size_t *o, bool first,
                              uint64_t object, uint64_t ept_ms)
{
    if (*o >= cap) return false;
    int n = snprintf(buf + *o, cap - *o, "%s{\"l\":[7,%" PRIu64 "],\"data\":[2,%" PRIu64 "]}",
                     first ? "" : ",", object, ept_ms);
    if (n < 0 || (size_t)n >= cap - *o) return false;
    *o += (size_t)n;
    return true;
}

static bool append_mt_record(char *buf, size_t cap, size_t *o, bool first,
                             uint64_t media_time_ms, uint64_t object, uint64_t wallclock_ms)
{
    if (*o >= cap) return false;
    int n = snprintf(buf + *o, cap - *o, "%s[%" PRIu64 ",[7,%" PRIu64 "],%" PRIu64 "]",
                     first ? "" : ",", media_time_ms, object, wallclock_ms);
    if (n < 0 || (size_t)n >= cap - *o) return false;
    *o += (size_t)n;
    return true;
}

/* SAP records for media Locations (7, first..first+count-1): the JSON array of
 * {"l":[7,i],"data":[2,i*10]}. Returns the length, or 0 when it would not fit. */
static size_t sap_payload(char *buf, size_t cap, uint64_t first, uint64_t count)
{
    size_t o = 0;
    bool ok = append_lit(buf, cap, &o, "[");
    for (uint64_t i = first; ok && i < first + count; i++)
        ok = append_sap_record(buf, cap, &o, i == first, i, i * 10);
    ok = ok && append_lit(buf, cap, &o, "]");
    MOQ_TEST_CHECK(ok && o < cap);          /* the guard: nothing truncated */
    return ok ? o : 0;
}

/* Media-timeline records for media Locations (7, first..): [[i*40,[7,i],1700000000000+i],...]. */
static size_t mt_payload(char *buf, size_t cap, uint64_t first, uint64_t count)
{
    size_t o = 0;
    bool ok = append_lit(buf, cap, &o, "[");
    for (uint64_t i = first; ok && i < first + count; i++)
        ok = append_mt_record(buf, cap, &o, i == first, i * 40, i, 1700000000000ull + i);
    ok = ok && append_lit(buf, cap, &o, "]");
    MOQ_TEST_CHECK(ok && o < cap);
    return ok ? o : 0;
}

/* -- record inventories ------------------------------------------------- */

typedef struct { uint64_t group, object, a, b; } rec_t;   /* a/b: sap_type+ept, or media_time+wallclock */

static size_t drain_sap(harness_t *h, rec_t *out, size_t cap)
{
    size_t n = 0;
    moq_media_sap_record_t rec;
    while (n < cap && moq_media_receiver_poll_sap(h->r, &rec, sizeof(rec)) == MOQ_OK) {
        MOQ_TEST_CHECK(rec.track == h->sap);
        out[n].group = rec.group; out[n].object = rec.object;
        out[n].a = (uint64_t)rec.sap_type; out[n].b = rec.ept_ms;
        n++;
    }
    return n;
}

static size_t drain_mt(harness_t *h, rec_t *out, size_t cap)
{
    size_t n = 0;
    moq_media_timeline_record_t rec;
    while (n < cap && moq_media_receiver_poll_media_timeline(h->r, &rec, sizeof(rec)) == MOQ_OK) {
        MOQ_TEST_CHECK(rec.track == h->history);
        out[n].group = rec.group; out[n].object = rec.object;
        out[n].a = rec.media_time_ms; out[n].b = rec.wallclock_ms;
        n++;
    }
    return n;
}

/* The identity every record i of Location (7, i) must carry. */
static bool sap_identity_ok(const rec_t *r, uint64_t i)
{
    return r->group == 7 && r->object == i && r->a == (uint64_t)MOQ_SAP_TYPE_2 && r->b == i * 10;
}
static bool mt_identity_ok(const rec_t *r, uint64_t i)
{
    return r->group == 7 && r->object == i && r->a == i * 40 && r->b == 1700000000000ull + i;
}

static uint64_t signal_sum(const moq_media_receiver_stats_t *s)
{
    return s->parse_drops + s->objects_dropped + s->groups_dropped +
           s->keyframes_dropped + s->overflow_events + s->catalog_drops;
}

/* -- rows ---------------------------------------------------------------- */

static uint64_t sap_deferrals(const moq_media_receiver_stats_t *s) { return s->sap_record_deferrals; }
static uint64_t mt_deferrals(const moq_media_receiver_stats_t *s) { return s->media_timeline_record_deferrals; }

typedef struct {
    const char *name;
    uint64_t alias;
    size_t (*payload)(char *, size_t, uint64_t, uint64_t);
    size_t (*drain)(harness_t *, rec_t *, size_t);
    bool (*identity)(const rec_t *, uint64_t);
    uint64_t (*deferrals)(const moq_media_receiver_stats_t *);
    uint64_t (*other_deferrals)(const moq_media_receiver_stats_t *);
} family_t;

static const family_t k_sap = { "sap", 2, sap_payload, drain_sap, sap_identity_ok, sap_deferrals, mt_deferrals };
static const family_t k_mt  = { "media-timeline", 3, mt_payload, drain_mt, mt_identity_ok, mt_deferrals, sap_deferrals };

/* No overflow: every record delivers once, in order, with its identity; the
 * next independent object (a new group) re-sending the same records surfaces
 * nothing, and one that appends two surfaces exactly those two. */
static void row_control(const family_t *f)
{
    harness_t h;
    if (!harness_open(&h)) { harness_close(&h); return; }
    static char payload[65536];
    static rec_t got[MAX_RECORDS];
    size_t len = f->payload(payload, sizeof(payload), 0, 100);
    feed_object(&h, f->alias, 0, payload, len);
    pump(&h, 4);
    size_t n = f->drain(&h, got, MAX_RECORDS);
    MOQ_TEST_CHECK_EQ_INT((int)n, 100);
    bool ordered = n == 100;
    for (size_t i = 0; ordered && i < n; i++) ordered = f->identity(&got[i], (uint64_t)i);
    MOQ_TEST_CHECK(ordered);
    /* §7.3 / §8.3: the next group's independent object carries the history. */
    len = f->payload(payload, sizeof(payload), 0, 100);
    feed_object(&h, f->alias, 1, payload, len);
    pump(&h, 4);
    MOQ_TEST_CHECK_EQ_INT((int)f->drain(&h, got, MAX_RECORDS), 0);
    len = f->payload(payload, sizeof(payload), 0, 102);
    feed_object(&h, f->alias, 2, payload, len);
    pump(&h, 4);
    n = f->drain(&h, got, MAX_RECORDS);
    MOQ_TEST_CHECK_EQ_INT((int)n, 2);
    MOQ_TEST_CHECK(n == 2 && f->identity(&got[0], 100) && f->identity(&got[1], 101));
    moq_media_receiver_stats_t st;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_get_stats(h.r, &st, sizeof(st)), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(st.parse_drops, 0);
    MOQ_TEST_CHECK_EQ_U64(f->deferrals(&st), 0);          /* nothing was ever deferred */
    MOQ_TEST_CHECK(!moq_media_receiver_is_fatal(h.r));
    fprintf(stderr, "[%s control] 100 delivered in order, resend surfaced 0, appended resend surfaced 2, deferrals 0\n", f->name);
    harness_close(&h);
}

/* `count` records arrive in one independent object while the application does
 * not poll at all; then the application drains; then the next group's
 * independent object re-sends all `count` (MSF §7.3 / §8.3: "all ... records
 * accumulated and accessible up to that point"); then the application drains
 * again. The contract: the first drain is the admitted prefix from record 0;
 * the re-send delivers exactly the deferred tail; the concatenation is
 * records 0..count-1 in order, each once; the family's deferral counter rose
 * by exactly the deferred count during the overflow and by nothing else; no
 * parse drop, no other signal, no terminal. */
static void row_under_poll(const family_t *f, uint64_t count)
{
    harness_t h;
    if (!harness_open(&h)) { harness_close(&h); return; }
    static char payload[65536];
    static rec_t first[MAX_RECORDS], second[MAX_RECORDS];
    moq_media_receiver_stats_t before, during, after;
    memset(&before, 0, sizeof(before));
    memset(&during, 0, sizeof(during));
    memset(&after, 0, sizeof(after));
    const uint64_t expect_deferred = count > RECORD_QUEUE_CAP ? count - RECORD_QUEUE_CAP : 0;

    /* The baseline is taken BEFORE the overflowing object exists. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_get_stats(h.r, &before, sizeof(before)), (int)MOQ_OK);
    int events_before = h.track_events;

    size_t len = f->payload(payload, sizeof(payload), 0, count);
    feed_object(&h, f->alias, 0, payload, len);
    pump(&h, 4);                                   /* the app is not polling */
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_get_stats(h.r, &during, sizeof(during)), (int)MOQ_OK);
    int events_during = h.track_events - events_before;

    size_t n1 = f->drain(&h, first, MAX_RECORDS);
    len = f->payload(payload, sizeof(payload), 0, count);
    feed_object(&h, f->alias, 1, payload, len);    /* the independent re-send */
    pump(&h, 4);
    size_t n2 = f->drain(&h, second, MAX_RECORDS);
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_get_stats(h.r, &after, sizeof(after)), (int)MOQ_OK);
    bool fatal = moq_media_receiver_is_fatal(h.r);
    int events = h.track_events - events_before;

    /* The concatenated observation: slot j must be record j, exactly. */
    bool exact = (n1 + n2) == count;
    uint64_t first_bad = count;
    for (size_t j = 0; j < n1 + n2 && j < MAX_RECORDS; j++) {
        const rec_t *r = j < n1 ? &first[j] : &second[j - n1];
        if (!f->identity(r, (uint64_t)j)) { exact = false; if (first_bad == count) first_bad = (uint64_t)j; }
    }
    uint64_t deferred_during = f->deferrals(&during) - f->deferrals(&before);
    uint64_t deferred_overall = f->deferrals(&after) - f->deferrals(&before);
    uint64_t other_signals = signal_sum(&after) - signal_sum(&before);

    fprintf(stderr,
            "[%s under-poll %" PRIu64 "] first drain=%zu (first id %" PRIu64 "), resend drain=%zu, "
            "concatenated exact=%d (first wrong slot %" PRIu64 "), deferrals during=%" PRIu64 " overall=%" PRIu64
            " (expected %" PRIu64 "), other family deferrals=%" PRIu64 ", other signals=%" PRIu64 " (parse %" PRIu64 "),"
            " events during/overall=%d/%d, fatal=%d, allocs=%zu\n",
            f->name, count, n1, n1 ? first[0].object : (uint64_t)0, n2, (int)exact, first_bad,
            deferred_during, deferred_overall, expect_deferred,
            f->other_deferrals(&after) - f->other_deferrals(&before), other_signals,
            after.parse_drops - before.parse_drops, events_during, events, (int)fatal, h.astats.total_allocs);

    /* The contract. */
    MOQ_TEST_CHECK(exact);                                              /* slot j == record j over first ++ second */
    MOQ_TEST_CHECK_EQ_U64((uint64_t)n1, count < RECORD_QUEUE_CAP ? count : RECORD_QUEUE_CAP);  /* the admitted prefix */
    MOQ_TEST_CHECK(n1 == 0 || first[0].object == 0);                   /* from record 0: nothing evicted */
    MOQ_TEST_CHECK_EQ_U64((uint64_t)n2, expect_deferred);               /* the re-send delivers exactly the deferred tail */
    MOQ_TEST_CHECK_EQ_U64(deferred_during, expect_deferred);            /* counted at the moment of deferral, per record */
    MOQ_TEST_CHECK_EQ_U64(deferred_overall, expect_deferred);           /* the re-send after draining defers nothing */
    MOQ_TEST_CHECK_EQ_U64(f->other_deferrals(&after) - f->other_deferrals(&before), 0);
    MOQ_TEST_CHECK_EQ_U64(other_signals, 0);                            /* no parse drop, no object drop, no catalog drop */
    MOQ_TEST_CHECK_EQ_INT(events, 0);
    MOQ_TEST_CHECK(!fatal);
    harness_close(&h);
}

/* The window the re-send cannot cover: `count` records overflow the queue,
 * the application drains the admitted prefix, and NO later independent
 * object ever arrives. Nothing more is delivered -- and the family's deferral
 * counter is exactly the deferred tail, the only public witness. */
static void row_no_next_object(const family_t *f, uint64_t count)
{
    harness_t h;
    if (!harness_open(&h)) { harness_close(&h); return; }
    static char payload[65536];
    static rec_t first[MAX_RECORDS], second[MAX_RECORDS];
    moq_media_receiver_stats_t before, after;
    memset(&before, 0, sizeof(before));
    memset(&after, 0, sizeof(after));
    const uint64_t expect_deferred = count - RECORD_QUEUE_CAP;

    MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_get_stats(h.r, &before, sizeof(before)), (int)MOQ_OK);
    int events_before = h.track_events, ended_before = h.ended_events, pdrop_before = h.parse_drop_events;
    size_t len = f->payload(payload, sizeof(payload), 0, count);
    feed_object(&h, f->alias, 0, payload, len);
    pump(&h, 4);
    size_t n1 = f->drain(&h, first, MAX_RECORDS);
    pump(&h, 8);                                   /* time passes; no independent object follows */
    size_t n2 = f->drain(&h, second, MAX_RECORDS);
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_get_stats(h.r, &after, sizeof(after)), (int)MOQ_OK);
    int events = h.track_events - events_before;

    bool prefix_ok = n1 == RECORD_QUEUE_CAP;
    for (size_t j = 0; prefix_ok && j < n1; j++) prefix_ok = f->identity(&first[j], (uint64_t)j);
    uint64_t deferred = f->deferrals(&after) - f->deferrals(&before);
    fprintf(stderr, "[%s no-next-object %" PRIu64 "] first drain=%zu (prefix ok %d), later drain=%zu, deferrals=%" PRIu64
            " (expected %" PRIu64 "), other family deferrals=%" PRIu64 ", other signals=%" PRIu64 ", events=%d, fatal=%d\n",
            f->name, count, n1, (int)prefix_ok, n2, deferred, expect_deferred,
            f->other_deferrals(&after) - f->other_deferrals(&before),
            signal_sum(&after) - signal_sum(&before), events, (int)moq_media_receiver_is_fatal(h.r));
    MOQ_TEST_CHECK(prefix_ok);                                /* the admitted prefix, whole, from record 0 */
    MOQ_TEST_CHECK_EQ_U64((uint64_t)n2, 0);                    /* no tail without a later independent object */
    MOQ_TEST_CHECK_EQ_U64(deferred, expect_deferred);          /* the counter accounts for the tail, exactly */
    MOQ_TEST_CHECK_EQ_U64(f->other_deferrals(&after) - f->other_deferrals(&before), 0);  /* the other family is untouched */
    MOQ_TEST_CHECK_EQ_U64(signal_sum(&after) - signal_sum(&before), 0);  /* no parse / object / catalog signal */
    MOQ_TEST_CHECK_EQ_INT(events, 0);                          /* no track event of any kind */
    MOQ_TEST_CHECK_EQ_INT(h.ended_events - ended_before, 0);
    MOQ_TEST_CHECK_EQ_INT(h.parse_drop_events - pdrop_before, 0);
    MOQ_TEST_CHECK(!moq_media_receiver_is_fatal(h.r));
    harness_close(&h);
}

int main(void)
{
    row_control(&k_sap);
    row_control(&k_mt);
    /* The boundary pair: exactly the queue depth is delivered whole; one more
     * is where the queue must evict -- what the oracle observes is eviction,
     * never an empty queue. */
    row_under_poll(&k_sap, RECORD_QUEUE_CAP);
    row_under_poll(&k_sap, RECORD_QUEUE_CAP + 1);
    row_under_poll(&k_sap, 300);
    row_under_poll(&k_mt, RECORD_QUEUE_CAP);
    row_under_poll(&k_mt, RECORD_QUEUE_CAP + 1);
    row_under_poll(&k_mt, 300);
    row_no_next_object(&k_sap, RECORD_QUEUE_CAP + 1);
    row_no_next_object(&k_sap, 300);
    row_no_next_object(&k_mt, RECORD_QUEUE_CAP + 1);
    row_no_next_object(&k_mt, 300);
    if (failures == 0)
        MOQ_TEST_PASS("media_receiver_timeline_loss");
    return failures ? 1 : 0;
}
