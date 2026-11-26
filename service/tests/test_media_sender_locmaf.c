/*
 * LOCMAF tracks (draft-einarsson-moq-locmaf-01) end to end over a simulated
 * session: a production media sender publishes a "locmaf" track from CMAF
 * chunks, and a production media receiver subscribes, decodes and delivers
 * them, on drafts 16, 18 and 22, under both catalog revisions and each
 * companion. The sender's Objects are observed on their way to the facade
 * (the test-only emit hook).
 *
 * Pinned:
 * - the catalog entry: packaging "locmaf", locmafVersion "0.3", the CMAF
 *   Header inline by initRef, in either revision;
 * - on the wire the first Object of every Group is a full header and the
 *   rest are deltas; the receiver delivers every chunk byte for byte, as
 *   CMAF, with no loss, and then TRACK_ENDED;
 * - write() refuses, taking nothing: more than one chunk and a box outside
 *   the field model (UNSUPPORTED), a malformed chunk (PROTO), and its
 *   buffer reservation failing (NOMEM); add_track refuses an application
 *   LOCMAF record, a header LOCMAF cannot use, and an encoder it cannot
 *   allocate, registering nothing;
 * - a write the facade refuses with WOULD_BLOCK is resent unchanged, and a
 *   wrap that fails at emit is retried with the same bytes, nothing
 *   committed in between;
 * - dropped Groups (eviction) never leave a delta without its reference on
 *   the wire;
 * - the queue charges each Object's wire buffer, and every wire buffer is
 *   released through its own owner, by size, whatever releases it: the
 *   facade after the sender is gone, eviction before or after the wrap,
 *   removal, a failed publish.
 */
#include <moq/media_sender.h>
#include <moq/media_receiver.h>
#include <moq/locmaf.h>
#include <moq/msf.h>
#include <moq/publisher.h>
#include <moq/rcbuf.h>
#include <moq/sim.h>
#include <moq/session.h>
#include "media_builders.h"
#include "test_support.h"

#include <stdalign.h>
#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int failures = 0;

/* Test seams (media_sender.c / media_receiver.c). */
moq_media_sender_t *moq_media_sender_test_new_cfg(const moq_media_sender_cfg_t *cfg);
void moq_media_sender_test_pump(moq_media_sender_t *s, moq_session_t *session,
                                uint64_t now_us);
void moq_media_sender_test_free(moq_media_sender_t *s);
void moq_media_sender_test_set_alloc(const moq_alloc_t *alloc);
moq_result_t moq_media_sender_test_build_catalog(moq_media_sender_t *s,
                                                 moq_rcbuf_t **out);
size_t moq_media_sender_test_track_count(moq_media_sender_t *s);
void moq_media_sender_test_set_emit_hook(moq_media_sender_t *s,
    moq_result_t (*fn)(void *ctx, const moq_pub_object_cfg_t *ocfg), void *ctx);
moq_media_receiver_t *moq_media_receiver_test_new_cfg(const moq_media_receiver_cfg_t *cfg);
void moq_media_receiver_test_destroy_with_session(moq_media_receiver_t *r,
                                                  moq_session_t *session, uint64_t now_us);
void moq_media_receiver_test_pump(moq_media_receiver_t *r, moq_session_t *session,
                                  uint64_t now_us);
void moq_media_receiver_test_set_alloc(const moq_alloc_t *alloc);

/* -- A byte-counting allocator, with one failure on demand --------------- */

typedef struct {
    int64_t  bytes, blocks;
    uint64_t calls, fail_at;
    size_t   fail_size;      /* when nonzero, the next request of this size fails */
    size_t   last_size;
} cnt_t;

static void *cnt_alloc(size_t n, void *ctx)
{
    cnt_t *c = (cnt_t *)ctx;
    c->last_size = n;
    if (++c->calls == c->fail_at) return NULL;
    if (c->fail_size && n == c->fail_size) { c->fail_size = 0; return NULL; }
    void *p = malloc(n ? n : 1);
    if (p) { c->bytes += (int64_t)n; c->blocks++; }
    return p;
}

static void cnt_free(void *p, size_t n, void *ctx)
{
    cnt_t *c = (cnt_t *)ctx;
    if (!p) return;
    c->bytes -= (int64_t)n;
    c->blocks--;
    free(p);
}

static void *cnt_realloc(void *p, size_t o, size_t n, void *ctx)
{
    cnt_t *c = (cnt_t *)ctx;
    void *q = realloc(p, n ? n : 1);
    if (q) {
        c->bytes += (int64_t)n - (int64_t)o;
        if (!p) c->blocks++;
    }
    return q;
}

/* -- Fixture ----------------------------------------------------------------- */

#define MAX_WIRE 64u
#define OBJ_CAP  256u

typedef struct wire_rec {
    uint64_t group, object;
    uint8_t  bytes[OBJ_CAP];
    size_t   len;
} wire_rec_t;

typedef struct fx {
    cnt_t                 cnt;
    moq_alloc_t           alloc;
    moq_simpair_t        *sp;
    uint64_t              now;
    moq_media_sender_t   *s;
    moq_media_receiver_t *r;
    moq_media_track_t    *st;        /* the sender's track */
    moq_media_track_t    *rt;        /* the receiver's */
    moq_locmaf_track_t    facts;
    /* The emit hook: every Object offered to the facade, and the results
     * to stand in for the facade's (MOQ_OK lets the write through). */
    wire_rec_t            wire[MAX_WIRE];
    size_t                wire_count;
    moq_result_t          refuse[8];
    size_t                refuse_count, refuse_next;
    moq_rcbuf_t          *kept;      /* a reference the hook keeps */
    bool                  keep_next;
} fx_t;

static moq_bytes_t g_ns_parts[2];

static moq_result_t emit_hook(void *ctx, const moq_pub_object_cfg_t *ocfg)
{
    fx_t *f = (fx_t *)ctx;
    if (f->wire_count < MAX_WIRE) {
        wire_rec_t *w = &f->wire[f->wire_count++];
        w->group = ocfg->group_id;
        w->object = ocfg->object_id;
        w->len = moq_rcbuf_len(ocfg->payload);
        if (w->len > OBJ_CAP) w->len = OBJ_CAP;
        memcpy(w->bytes, moq_rcbuf_data(ocfg->payload), w->len);
    }
    if (f->keep_next && !f->kept) {
        moq_rcbuf_incref(ocfg->payload);
        f->kept = ocfg->payload;
        f->keep_next = false;
    }
    if (f->refuse_next < f->refuse_count) return f->refuse[f->refuse_next++];
    return MOQ_OK;
}

static size_t init_bytes(uint8_t *out)
{
    static const uint32_t trex4[4] = { 1, 1024, 0, 0x02000000u };
    return moq_test_build_cmaf_init_trex(out, 1, 48000, trex4, 1, true);
}

static bool fx_pair(fx_t *f, moq_version_t v)
{
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = &f->alloc; cfg.seed = 7; cfg.initial_now_us = 1000;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 32;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 32;
    cfg.version = v;
    if (moq_simpair_create(&cfg, &f->sp) != MOQ_OK) return false;
    if (moq_simpair_start(f->sp) != MOQ_OK ||
        moq_simpair_run_until_quiescent(f->sp, 8, NULL) != MOQ_OK)
        return false;
    moq_event_t ev;
    if (moq_session_poll_events(moq_simpair_client(f->sp), &ev, 1) == 1)
        moq_event_cleanup(&ev);
    if (moq_session_poll_events(moq_simpair_server(f->sp), &ev, 1) == 1)
        moq_event_cleanup(&ev);
    f->now = moq_simpair_now_us(f->sp);
    return true;
}

static bool pump_sender(fx_t *f)
{
    moq_media_sender_test_pump(f->s, moq_simpair_client(f->sp), f->now);
    return moq_simpair_run_until_quiescent(f->sp, 8, NULL) == MOQ_OK;
}

static bool pump_both(fx_t *f)
{
    if (!pump_sender(f)) return false;
    if (f->r) {
        moq_media_receiver_test_pump(f->r, moq_simpair_server(f->sp), f->now);
        if (moq_simpair_run_until_quiescent(f->sp, 8, NULL) != MOQ_OK) return false;
    }
    return true;
}

typedef struct opts {
    moq_version_t v;
    moq_media_companion_profile_t companion;
    moq_msf_revision_t revision;
    moq_media_send_backpressure_t bp;
    uint32_t queue_objects;
    bool receiver;
    bool no_validate;     /* validate_cmaf off: the encoder's own checks */
} opts_t;

static moq_result_t add_lm(fx_t *f, moq_media_track_t **out)
{
    uint8_t init[512];
    size_t n = init_bytes(init);
    moq_media_track_cfg_t tc;
    moq_media_track_cfg_init(&tc);
    tc.name = (moq_bytes_t){ (const uint8_t *)"v", 1 };
    tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
    tc.packaging = MOQ_MEDIA_PACKAGING_LOCMAF;
    tc.codec = (moq_bytes_t){ (const uint8_t *)"avc1.64001f", 11 };
    tc.bitrate = 1000000;
    tc.framerate_millis = 30000;
    tc.timescale = 48000;
    tc.init_data = (moq_bytes_t){ init, n };
    tc.is_live = true;
    return moq_media_sender_add_track(f->s, &tc, out);
}

/* A sender (and receiver) on a fresh pair, the sender's track added and,
 * with a receiver, subscribed. */
static bool fx_open(fx_t *f, const opts_t *o)
{
    memset(f, 0, sizeof(*f));
    f->alloc = (moq_alloc_t){ &f->cnt, cnt_alloc, cnt_realloc, cnt_free };
    if (!fx_pair(f, o->v)) return false;
    g_ns_parts[0] = MOQ_BYTES_LITERAL("svc");
    g_ns_parts[1] = MOQ_BYTES_LITERAL("lm");
    moq_media_sender_cfg_t cfg;
    moq_media_sender_cfg_init_live_sized(&cfg, sizeof(cfg));
    cfg.namespace_ = (moq_namespace_t){ g_ns_parts, 2 };
    cfg.companion_profile = o->companion;
    cfg.catalog_revision = o->revision;
    cfg.publish_tracks = false;
    cfg.catalog_refresh_interval_us = UINT64_MAX;
    if (o->bp) cfg.backpressure = o->bp;
    if (o->queue_objects) cfg.queue_max_objects = o->queue_objects;
    if (o->no_validate) cfg.validate_cmaf = false;
    moq_media_sender_test_set_alloc(&f->alloc);
    f->s = moq_media_sender_test_new_cfg(&cfg);
    moq_media_sender_test_set_alloc(NULL);
    MOQ_TEST_CHECK(f->s != NULL);
    if (!f->s) return false;
    moq_media_sender_test_set_emit_hook(f->s, emit_hook, f);
    MOQ_TEST_CHECK(add_lm(f, &f->st) == MOQ_OK);
    if (!f->st) return false;
    uint8_t init[512];
    moq_cmaf_init_info_t ii;
    moq_cmaf_init_info_init_sized(&ii, sizeof(ii));
    MOQ_TEST_CHECK(moq_cmaf_parse_init_sized((moq_bytes_t){ init, init_bytes(init) },
                                             &ii, sizeof(ii)) == MOQ_OK);
    MOQ_TEST_CHECK(moq_locmaf_track_from_cmaf(&ii, &f->facts, sizeof(f->facts)) == MOQ_OK);
    moq_session_t *srv = moq_simpair_server(f->sp);
    moq_event_t ev;
    for (int i = 0; i < 12 && !moq_media_sender_is_ready(f->s); i++) {
        if (!pump_sender(f)) return false;
        while (moq_session_poll_events(srv, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED) {
                moq_accept_namespace_cfg_t ac;
                moq_accept_namespace_cfg_init(&ac);
                MOQ_TEST_CHECK(moq_session_accept_namespace(srv,
                    ev.u.namespace_published.ann, &ac, f->now) == MOQ_OK);
            }
            moq_event_cleanup(&ev);
        }
    }
    MOQ_TEST_CHECK(moq_media_sender_is_ready(f->s));
    if (!o->receiver) return moq_media_sender_is_ready(f->s);
    moq_media_receiver_cfg_t rcfg;
    moq_media_receiver_cfg_init_live_sized(&rcfg, sizeof(rcfg));
    rcfg.namespace_ = (moq_namespace_t){ g_ns_parts, 2 };
    rcfg.auto_subscribe = true;
    rcfg.companion_profile = o->companion;
    moq_media_receiver_test_set_alloc(&f->alloc);
    f->r = moq_media_receiver_test_new_cfg(&rcfg);
    moq_media_receiver_test_set_alloc(NULL);
    MOQ_TEST_CHECK(f->r != NULL);
    if (!f->r) return false;
    for (int i = 0; i < 24 && !(f->rt && moq_media_sender_track_has_subscriber(f->s, f->st)); i++) {
        if (!pump_both(f)) return false;
        moq_media_track_event_t te;
        while (moq_media_receiver_poll_track(f->r, &te, sizeof(te)) == MOQ_OK)
            if (te.kind == MOQ_MEDIA_TRACK_ADDED) f->rt = te.track;
    }
    MOQ_TEST_CHECK(f->rt != NULL);
    MOQ_TEST_CHECK(moq_media_sender_track_has_subscriber(f->s, f->st));
    return f->rt && moq_media_sender_track_has_subscriber(f->s, f->st);
}

static void fx_close(fx_t *f)
{
    if (f->r) {
        moq_media_object_t o;
        while (moq_media_receiver_poll_object(f->r, &o, sizeof(o)) == MOQ_OK) {
            if (o.samples_owned) {
                f->alloc.free(o.samples_owned, o.sample_count * sizeof(moq_cmaf_sample_t),
                              f->alloc.ctx);
                o.samples_owned = NULL;
            }
            moq_media_object_cleanup(&o);
        }
        moq_media_receiver_test_destroy_with_session(f->r, moq_simpair_server(f->sp), f->now);
        f->r = NULL;
    }
    if (f->s) moq_media_sender_test_free(f->s);
    f->s = NULL;
    if (f->kept) moq_rcbuf_decref(f->kept);
    f->kept = NULL;
    if (f->sp) {
        moq_event_t e;
        while (moq_session_poll_events(moq_simpair_client(f->sp), &e, 1) == 1)
            moq_event_cleanup(&e);
        while (moq_session_poll_events(moq_simpair_server(f->sp), &e, 1) == 1)
            moq_event_cleanup(&e);
        moq_simpair_destroy(f->sp);
        f->sp = NULL;
    }
    if (f->cnt.bytes != 0 || f->cnt.blocks != 0)
        fprintf(stderr, "  balance %lld bytes in %lld blocks\n",
                (long long)f->cnt.bytes, (long long)f->cnt.blocks);
    MOQ_TEST_CHECK(f->cnt.bytes == 0 && f->cnt.blocks == 0);
}

/* The canonical chunk of Object k of a Group (index gi in the stream): one
 * sample whose size varies, sync at the Group's start. */
static size_t chunk(const fx_t *f, uint64_t gi, uint64_t k, uint8_t *out, size_t cap)
{
    uint8_t payload[16];
    uint32_t size = 4u + (uint32_t)((gi + k) % 5u);
    for (uint32_t i = 0; i < size; i++) payload[i] = (uint8_t)(gi * 13u + k * 5u + i);
    moq_locmaf_sample_t s;
    memset(&s, 0, sizeof(s));
    s.duration = 1024;
    s.size = size;
    s.flags = k == 0 ? 0x02000000u : 0x01010000u;
    moq_locmaf_chunk_t c;
    memset(&c, 0, sizeof(c));
    c.struct_size = sizeof(c);
    c.kind = MOQ_LOCMAF_OBJECT_FULL;
    c.base_media_decode_time = (gi * 100u + k) * 1024u;
    c.sample_description_index = 1;
    c.sample_count = 1;
    c.samples = &s;
    c.mdat_payload = (moq_bytes_t){ payload, size };
    size_t len = 0;
    MOQ_TEST_CHECK(moq_locmaf_write_cmaf(&f->facts, &c, out, cap, &len) == MOQ_OK);
    return len;
}

/* Write raw bytes as one Object; on a refusal the payload is released here,
 * proving it was not taken. */
static moq_result_t write_bytes(fx_t *f, const uint8_t *b, size_t n, bool starts)
{
    moq_rcbuf_t *p = NULL;
    if (moq_rcbuf_create(&f->alloc, b, n, &p) != MOQ_OK) return MOQ_ERR_NOMEM;
    moq_media_send_object_t o;
    memset(&o, 0, sizeof(o));
    o.struct_size = sizeof(o);
    o.payload = p;
    o.is_sync = starts;
    o.starts_group = starts;
    moq_result_t rc = moq_media_sender_write(f->s, f->st, &o);
    if (rc != MOQ_OK) moq_rcbuf_decref(p);
    return rc;
}

static moq_result_t write_chunk(fx_t *f, uint64_t gi, uint64_t k)
{
    uint8_t c[OBJ_CAP];
    size_t n = chunk(f, gi, k, c, sizeof(c));
    return write_bytes(f, c, n, k == 0);
}

/* The receiver's delivered Objects, compared with the chunks of groups
 * 0..ng-1, n each, in order. */
static int drain_receiver(fx_t *f, size_t ng, size_t n, bool *ended)
{
    int got = 0;
    uint64_t first_group = 0;
    for (int cycle = 0; cycle < 32 && (got < (int)(ng * n) || (ended && !*ended)); cycle++) {
        if (!pump_both(f)) break;
        moq_media_track_event_t te;
        while (moq_media_receiver_poll_track(f->r, &te, sizeof(te)) == MOQ_OK)
            if (ended && te.kind == MOQ_MEDIA_TRACK_ENDED && te.track == f->rt) *ended = true;
        moq_media_object_t o;
        while (moq_media_receiver_poll_object(f->r, &o, sizeof(o)) == MOQ_OK) {
            if (got == 0) first_group = o.group_id;
            uint64_t gi = o.group_id - first_group, k = o.object_id;
            uint8_t want[OBJ_CAP];
            size_t wn = chunk(f, gi, k, want, sizeof(want));
            bool ok = o.packaging == MOQ_MEDIA_PACKAGING_CMAF && o.payload.len == 0 &&
                      o.fragment.len == wn && memcmp(o.fragment.data, want, wn) == 0 &&
                      o.has_loss_report && !o.gap &&
                      (uint64_t)got == gi * n + k;
            if (!ok)
                fprintf(stderr, "  object %d at (%llu, %llu): packaging %u frag %zu/%zu gap %d\n",
                        got, (unsigned long long)o.group_id, (unsigned long long)o.object_id,
                        (unsigned)o.packaging, o.fragment.len, wn, (int)o.gap);
            MOQ_TEST_CHECK(ok);
            got++;
            if (o.samples_owned) {
                f->alloc.free(o.samples_owned, o.sample_count * sizeof(moq_cmaf_sample_t),
                              f->alloc.ctx);
                o.samples_owned = NULL;
            }
            moq_media_object_cleanup(&o);
        }
    }
    return got;
}

/* The wire Objects, decoded in order by a library decoder, rebuild their
 * chunks; deltas only after their Group's full header, never without their
 * reference. */
static void check_wire(fx_t *f, size_t from)
{
    moq_locmaf_decoder_cfg_t cfg;
    moq_locmaf_decoder_cfg_init(&cfg);
    cfg.track = &f->facts;
    size_t bytes = 0;
    MOQ_TEST_CHECK(moq_locmaf_decoder_storage_size(&cfg, &bytes) == MOQ_OK);
    void *mem = malloc(bytes);
    cfg.storage = mem;
    cfg.storage_size = bytes;
    moq_locmaf_decoder_t d;
    MOQ_TEST_CHECK(mem && moq_locmaf_decoder_init(&d, sizeof(d), &cfg) == MOQ_OK);
    for (size_t i = from; mem && i < f->wire_count; i++) {
        const wire_rec_t *w = &f->wire[i];
        if (i > from && w->group == f->wire[i - 1].group && w->object == f->wire[i - 1].object)
            continue;   /* a resend of the same Object */
        moq_locmaf_chunk_t c;
        moq_locmaf_reason_t why = 0;
        moq_result_t rc = moq_locmaf_decoder_push(&d, w->group, w->object,
            (moq_bytes_t){ w->bytes, w->len }, &c, sizeof(c), &why);
        if (rc != MOQ_OK)
            fprintf(stderr, "  wire %zu (%llu, %llu): %d %s\n", i,
                    (unsigned long long)w->group, (unsigned long long)w->object,
                    rc, moq_locmaf_reason_name(why));
        MOQ_TEST_CHECK(rc == MOQ_OK);
        /* A full header (element 2) opens each Group, deltas (3) follow. */
        MOQ_TEST_CHECK(w->len > 0 && w->bytes[0] == (w->object == 0 ? 0x02 : 0x03));
    }
    free(mem);
}

/* -- End to end ------------------------------------------------------------------- */

static void test_e2e(const char *name, moq_version_t v,
                     moq_media_companion_profile_t companion, moq_msf_revision_t rev)
{
    fx_t f;
    opts_t o = { v, companion, rev, 0, 0, true, false };
    if (fx_open(&f, &o)) {
        /* The catalog entry. */
        moq_rcbuf_t *json = NULL;
        MOQ_TEST_CHECK(moq_media_sender_test_build_catalog(f.s, &json) == MOQ_OK);
        if (json) {
            moq_msf_catalog_t cat;
            moq_bytes_t jb = { moq_rcbuf_data(json), moq_rcbuf_len(json) };
            MOQ_TEST_CHECK(moq_msf_catalog_parse(moq_alloc_default(), jb, &cat) == MOQ_OK);
            const moq_msf_track_t *t = NULL;
            for (size_t i = 0; i < cat.track_count; i++)
                if (cat.tracks[i].name.len == 1 && cat.tracks[i].name.data[0] == 'v')
                    t = &cat.tracks[i];
            MOQ_TEST_CHECK(t && t->packaging.len == 6 &&
                           memcmp(t->packaging.data, "locmaf", 6) == 0);
            MOQ_TEST_CHECK(t && t->has_init_ref && cat.init_data_count == 1);
            MOQ_TEST_CHECK(t && moq_msf_track_locmaf_check(t, NULL) == MOQ_OK);
            MOQ_TEST_CHECK(cat.revision == rev);
            moq_msf_catalog_cleanup(moq_alloc_default(), &cat);
            moq_rcbuf_decref(json);
        }
        const moq_media_track_desc_t *d = moq_media_track_desc_get(f.rt);
        MOQ_TEST_CHECK(d && d->info.packaging == MOQ_MEDIA_PACKAGING_LOCMAF && d->cmaf);
        for (uint64_t g = 0; g < 3; g++)
            for (uint64_t k = 0; k < 4; k++) MOQ_TEST_CHECK(write_chunk(&f, g, k) == MOQ_OK);
        MOQ_TEST_CHECK(moq_media_sender_end_track(f.s, f.st) == MOQ_OK);
        bool ended = false;
        int got = drain_receiver(&f, 3, 4, &ended);
        if (got != 12) fprintf(stderr, "  %s: %d of 12 delivered\n", name, got);
        MOQ_TEST_CHECK(got == 12 && ended);
        check_wire(&f, 0);
        MOQ_TEST_CHECK(f.wire_count == 12);
        for (size_t i = 0; i < f.wire_count; i++) {
            uint8_t c[OBJ_CAP];
            size_t n = chunk(&f, i / 4, i % 4, c, sizeof(c));
            MOQ_TEST_CHECK(f.wire[i].len < n);   /* smaller than the chunk */
        }
        moq_media_receiver_stats_t rs;
        moq_media_receiver_get_stats(f.r, &rs, sizeof(rs));
        MOQ_TEST_CHECK(rs.locmaf_objects_rebuilt == 12 && rs.locmaf_objects_undecodable == 0 &&
                       rs.locmaf_objects_expired == 0 && rs.locmaf_objects_late == 0);
        MOQ_TEST_CHECK(!moq_media_receiver_is_fatal(f.r));
    }
    int before = failures;
    fx_close(&f);
    if (failures == before) printf("PASS: e2e %s\n", name);
    else fprintf(stderr, "FAIL: e2e %s\n", name);
}

/* -- Refusals ---------------------------------------------------------------------- */

static void be32_add(uint8_t *p, uint32_t d)
{
    uint32_t v = ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
                 ((uint32_t)p[2] << 8) | p[3];
    v += d;
    p[0] = (uint8_t)(v >> 24); p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >> 8); p[3] = (uint8_t)v;
}

/* The offset of the first box header whose type is `type` (0 if none);
 * a byte scan, enough for the canonical chunks built here. */
static size_t find_box(const uint8_t *b, size_t n, const char type[4])
{
    for (size_t i = 4; i + 4 <= n; i++)
        if (memcmp(b + i, type, 4) == 0) return i - 4;
    return 0;
}

static void test_refusals(void)
{
    fx_t f;
    opts_t o = { MOQ_VERSION_DRAFT_16, MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01,
                 MOQ_MSF_REVISION_01, 0, 0, false, true };
    if (fx_open(&f, &o)) {
        moq_media_sender_stats_t before, after;
        moq_media_sender_get_stats(f.s, &before, sizeof(before));
        uint8_t a[OBJ_CAP], b[OBJ_CAP], two[2 * OBJ_CAP];
        size_t na = chunk(&f, 0, 0, a, sizeof(a)), nb = chunk(&f, 0, 1, b, sizeof(b));
        memcpy(two, a, na);
        memcpy(two + na, b, nb);
        MOQ_TEST_CHECK(write_bytes(&f, two, na + nb, true) == MOQ_ERR_UNSUPPORTED);
        /* A uuid box in the moof, after mfhd: outside the field model. The
         * moof's size and trun's data_offset (relative to the moof) grow by
         * the box. */
        uint8_t u[OBJ_CAP + 32];
        static const uint8_t uuid[24] = { 0, 0, 0, 24, 'u', 'u', 'i', 'd',
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 };
        const size_t at = 8 + 16;            /* moof header, then mfhd */
        MOQ_TEST_CHECK(memcmp(a + 4, "moof", 4) == 0 && memcmp(a + 12, "mfhd", 4) == 0);
        memcpy(u, a, at);
        memcpy(u + at, uuid, sizeof(uuid));
        memcpy(u + at + sizeof(uuid), a + at, na - at);
        be32_add(u, 24);
        size_t trun = find_box(u, na + 24, "trun");
        MOQ_TEST_CHECK(trun != 0 && (u[trun + 11] & 0x01u));  /* data_offset present */
        if (trun) be32_add(u + trun + 16, 24);
        MOQ_TEST_CHECK(write_bytes(&f, u, na + 24, true) == MOQ_ERR_UNSUPPORTED);
        /* Malformed: a truncated moof. */
        MOQ_TEST_CHECK(write_bytes(&f, a, 20, true) == MOQ_ERR_PROTO);
        moq_media_sender_get_stats(f.s, &after, sizeof(after));
        MOQ_TEST_CHECK(after.objects_written == before.objects_written);
        /* An application LOCMAF record. */
        moq_msf_locmaf_t rec;
        moq_msf_locmaf_init(&rec);
        rec.has_version = true;
        rec.version = (moq_bytes_t){ (const uint8_t *)"0.3", 3 };
        moq_msf_track_ext_t ext = { .kind = MOQ_MSF_EXT_LOCMAF, .u = { .locmaf = &rec } };
        uint8_t init[512];
        size_t in = init_bytes(init);
        moq_media_track_cfg_t tc;
        moq_media_track_cfg_init(&tc);
        tc.name = (moq_bytes_t){ (const uint8_t *)"w", 1 };
        tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
        tc.packaging = MOQ_MEDIA_PACKAGING_LOCMAF;
        tc.codec = (moq_bytes_t){ (const uint8_t *)"avc1.64001f", 11 };
        tc.bitrate = 1000000;
        tc.init_data = (moq_bytes_t){ init, in };
        tc.extensions = &ext;
        tc.extension_count = 1;
        moq_media_track_t *t2 = NULL;
        size_t count = moq_media_sender_test_track_count(f.s);
        MOQ_TEST_CHECK(moq_media_sender_add_track(f.s, &tc, &t2) == MOQ_ERR_INVAL && !t2);
        /* A header without a trex, and none at all. */
        tc.extensions = NULL;
        tc.extension_count = 0;
        static const uint32_t trex4[4] = { 1, 1024, 0, 0 };
        tc.init_data = (moq_bytes_t){ init,
            moq_test_build_cmaf_init_trex(init, 1, 48000, trex4, 1, false) };
        MOQ_TEST_CHECK(moq_media_sender_add_track(f.s, &tc, &t2) == MOQ_ERR_INVAL && !t2);
        tc.init_data = (moq_bytes_t){ NULL, 0 };
        MOQ_TEST_CHECK(moq_media_sender_add_track(f.s, &tc, &t2) == MOQ_ERR_INVAL && !t2);
        MOQ_TEST_CHECK(moq_media_sender_test_track_count(f.s) == count);
    }
    int before = failures;
    fx_close(&f);
    if (failures == before) printf("PASS: refusals\n");
}

/* contentProtectionRefIDs follow the CMAF rule: the header must carry the
 * CENC boxes; a protected header gives the encoder its IV size. */
static void test_protected_init(void)
{
    cnt_t c = {0};
    moq_alloc_t alloc = { &c, cnt_alloc, cnt_realloc, cnt_free };
    moq_bytes_t ns_part = { (const uint8_t *)"ns", 2 };
    moq_bytes_t kid_s = { (const uint8_t *)"01234567-89ab-cdef-0123-456789abcdef", 36 };
    moq_cmsf_content_protection_t cp;
    memset(&cp, 0, sizeof(cp));
    cp.ref_id = (moq_bytes_t){ (const uint8_t *)"cp1", 3 };
    cp.scheme = (moq_bytes_t){ (const uint8_t *)"cenc", 4 };
    cp.default_kids = &kid_s;
    cp.default_kid_count = 1;
    cp.drm_system.system_id =
        (moq_bytes_t){ (const uint8_t *)"edef8ba9-79d6-4ace-a3c8-27dcd51d21ed", 36 };
    moq_media_sender_cfg_t cfg;
    moq_media_sender_cfg_init_live_sized(&cfg, sizeof(cfg));
    cfg.namespace_.parts = &ns_part;
    cfg.namespace_.count = 1;
    cfg.content_protections = &cp;
    cfg.content_protection_count = 1;
    moq_media_sender_test_set_alloc(&alloc);
    moq_media_sender_t *s = moq_media_sender_test_new_cfg(&cfg);
    moq_media_sender_test_set_alloc(NULL);
    MOQ_TEST_CHECK(s != NULL);
    if (s) {
        uint8_t clear[512], prot[1024];
        static const uint32_t trex4[4] = { 1, 3000, 0, 0x01010000u };
        size_t nclear = moq_test_build_cmaf_init_trex(clear, 1, 90000, trex4, 1, true);
        static const uint8_t kid[16] = { 1 };
        static const uint8_t avcc[] = { 0x01, 0x64, 0x00, 0x1E, 0xFF };
        moq_test_cenc_init_opts_t o;
        memset(&o, 0, sizeof(o));
        o.with_trex = true;
        memcpy(o.trex, trex4, sizeof(trex4));
        size_t nprot = moq_test_build_cmaf_init_cenc_ex(prot, 1, 90000, 64, 64, avcc,
            sizeof(avcc), "cenc", 1, 8, kid, 0, MOQ_TEST_CENC_OK, &o);
        moq_bytes_t ref = { (const uint8_t *)"cp1", 3 };
        moq_media_track_cfg_t tc;
        moq_media_track_cfg_init(&tc);
        tc.name = (moq_bytes_t){ (const uint8_t *)"v", 1 };
        tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
        tc.packaging = MOQ_MEDIA_PACKAGING_LOCMAF;
        tc.codec = (moq_bytes_t){ (const uint8_t *)"avc1.64001e", 11 };
        tc.bitrate = 1000000;
        tc.content_protection_ref_ids = &ref;
        tc.content_protection_ref_id_count = 1;
        tc.init_data = (moq_bytes_t){ clear, nclear };
        moq_media_track_t *t = NULL;
        MOQ_TEST_CHECK(moq_media_sender_add_track(s, &tc, &t) == MOQ_ERR_INVAL && !t);
        tc.init_data = (moq_bytes_t){ prot, nprot };
        MOQ_TEST_CHECK(moq_media_sender_add_track(s, &tc, &t) == MOQ_OK && t);
        moq_media_sender_test_free(s);
    }
    MOQ_TEST_CHECK(c.bytes == 0 && c.blocks == 0);
    printf("%s: protected_init\n", c.bytes == 0 ? "PASS" : "FAIL");
}

/* -- Allocation failure ---------------------------------------------------------- */

static void test_oom_add_track(void)
{
    fx_t f;
    opts_t o = { MOQ_VERSION_DRAFT_16, MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01,
                 MOQ_MSF_REVISION_01, 0, 0, false, false };
    if (fx_open(&f, &o)) {
        bool added = false;
        for (uint64_t k = 1; k < 64 && !added; k++) {
            size_t count = moq_media_sender_test_track_count(f.s);
            int64_t bytes = f.cnt.bytes;
            f.cnt.calls = 0;
            f.cnt.fail_at = k;
            uint8_t init[512];
            size_t in = init_bytes(init);
            moq_media_track_cfg_t tc;
            moq_media_track_cfg_init(&tc);
            tc.name = (moq_bytes_t){ (const uint8_t *)"w", 1 };
            tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
            tc.packaging = MOQ_MEDIA_PACKAGING_LOCMAF;
            tc.codec = (moq_bytes_t){ (const uint8_t *)"avc1.64001f", 11 };
            tc.bitrate = 1000000;
            tc.init_data = (moq_bytes_t){ init, in };
            moq_media_track_t *t2 = NULL;
            moq_result_t rc = moq_media_sender_add_track(f.s, &tc, &t2);
            f.cnt.fail_at = 0;
            if (rc == MOQ_OK) {
                added = true;
            } else {
                MOQ_TEST_CHECK(rc == MOQ_ERR_NOMEM && !t2);
                MOQ_TEST_CHECK(moq_media_sender_test_track_count(f.s) == count);
                MOQ_TEST_CHECK(f.cnt.bytes == bytes);
            }
        }
        MOQ_TEST_CHECK(added);
    }
    int before = failures;
    fx_close(&f);
    if (failures == before) printf("PASS: oom_add_track\n");
}

static void test_oom_write_and_wrap(void)
{
    fx_t f;
    opts_t o = { MOQ_VERSION_DRAFT_18, MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01,
                 MOQ_MSF_REVISION_01, 0, 0, true, false };
    if (fx_open(&f, &o)) {
        /* The wire buffer at write(): NOMEM, the caller keeps its payload,
         * nothing is queued. */
        uint8_t c[OBJ_CAP];
        size_t n = chunk(&f, 0, 0, c, sizeof(c));
        moq_rcbuf_t *p = NULL;
        MOQ_TEST_CHECK(moq_rcbuf_create(&f.alloc, c, n, &p) == MOQ_OK);
        moq_media_send_object_t so;
        memset(&so, 0, sizeof(so));
        so.struct_size = sizeof(so);
        so.payload = p;
        so.is_sync = so.starts_group = true;
        moq_media_sender_stats_t st0, st1;
        moq_media_sender_get_stats(f.s, &st0, sizeof(st0));
        f.cnt.calls = 0;
        f.cnt.fail_at = 1;
        MOQ_TEST_CHECK(moq_media_sender_write(f.s, f.st, &so) == MOQ_ERR_NOMEM);
        f.cnt.fail_at = 0;
        moq_media_sender_get_stats(f.s, &st1, sizeof(st1));
        MOQ_TEST_CHECK(st1.objects_written == st0.objects_written && st1.objects_queued == 0);
        MOQ_TEST_CHECK(moq_media_sender_write(f.s, f.st, &so) == MOQ_OK);   /* p taken now */
        MOQ_TEST_CHECK(write_chunk(&f, 0, 1) == MOQ_OK);
        /* The rcbuf header at emit: the first pump's wrap fails, nothing is
         * offered to the facade; the next one sends it. The header's size
         * is the one a wrap asks for. */
        static const uint8_t one[1] = { 0 };
        moq_rcbuf_t *probe = NULL;
        MOQ_TEST_CHECK(moq_rcbuf_wrap(&f.alloc, one, 1, NULL, NULL, &probe) == MOQ_OK);
        size_t header = f.cnt.last_size;
        moq_rcbuf_decref(probe);
        f.cnt.fail_size = header;
        moq_media_sender_test_pump(f.s, moq_simpair_client(f.sp), f.now);
        MOQ_TEST_CHECK(f.cnt.fail_size == 0);   /* the refusal was used */
        f.cnt.fail_size = 0;
        MOQ_TEST_CHECK(f.wire_count == 0);
        moq_media_sender_get_stats(f.s, &st1, sizeof(st1));
        MOQ_TEST_CHECK(st1.last_error == MOQ_ERR_NOMEM && st1.objects_sent == st0.objects_sent);
        MOQ_TEST_CHECK(!moq_media_sender_is_fatal(f.s));
        int got = drain_receiver(&f, 1, 2, NULL);
        MOQ_TEST_CHECK(got == 2);
        check_wire(&f, 0);
        MOQ_TEST_CHECK(f.wire_count == 2);
    }
    int before = failures;
    fx_close(&f);
    if (failures == before) printf("PASS: oom_write_and_wrap\n");
}

/* -- Retries ------------------------------------------------------------------------ */

static void test_would_block_retry(void)
{
    fx_t f;
    opts_t o = { MOQ_VERSION_DRAFT_16, MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01,
                 MOQ_MSF_REVISION_01, 0, 0, true, false };
    if (fx_open(&f, &o)) {
        /* The second Object is refused twice, then taken. */
        f.refuse[0] = MOQ_OK;
        f.refuse[1] = MOQ_ERR_WOULD_BLOCK;
        f.refuse[2] = MOQ_ERR_WOULD_BLOCK;
        f.refuse_count = 3;
        for (uint64_t k = 0; k < 4; k++) MOQ_TEST_CHECK(write_chunk(&f, 0, k) == MOQ_OK);
        int got = drain_receiver(&f, 1, 4, NULL);
        MOQ_TEST_CHECK(got == 4);
        MOQ_TEST_CHECK(f.wire_count == 6);
        /* Resent unchanged: the same Location, the same bytes. */
        MOQ_TEST_CHECK(f.wire[1].object == 1 && f.wire[2].object == 1 && f.wire[3].object == 1);
        MOQ_TEST_CHECK(f.wire[1].len == f.wire[2].len && f.wire[2].len == f.wire[3].len &&
                       memcmp(f.wire[1].bytes, f.wire[2].bytes, f.wire[1].len) == 0 &&
                       memcmp(f.wire[2].bytes, f.wire[3].bytes, f.wire[2].len) == 0);
        check_wire(&f, 0);
    }
    int before = failures;
    fx_close(&f);
    if (failures == before) printf("PASS: would_block_retry\n");
}

/* -- Drops ------------------------------------------------------------------------------ */

/* Groups evicted from a small queue: the Objects that go out still decode
 * from their Group's full header. */
static void test_eviction(void)
{
    fx_t f;
    opts_t o = { MOQ_VERSION_DRAFT_16, MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01,
                 MOQ_MSF_REVISION_01, MOQ_MEDIA_SEND_BP_DROP_GROUP, 4, true, false };
    if (fx_open(&f, &o)) {
        for (uint64_t g = 0; g < 3; g++)
            for (uint64_t k = 0; k < 3; k++) MOQ_TEST_CHECK(write_chunk(&f, g, k) == MOQ_OK);
        moq_media_sender_stats_t st;
        moq_media_sender_get_stats(f.s, &st, sizeof(st));
        MOQ_TEST_CHECK(st.groups_dropped >= 1);
        for (int i = 0; i < 16; i++) (void)pump_both(&f);
        MOQ_TEST_CHECK(f.wire_count > 0);
        check_wire(&f, 0);
        moq_media_receiver_stats_t rs;
        moq_media_receiver_get_stats(f.r, &rs, sizeof(rs));
        MOQ_TEST_CHECK(rs.locmaf_objects_undecodable == 0 &&
                       rs.locmaf_objects_rebuilt == f.wire_count);
    }
    int before = failures;
    fx_close(&f);
    if (failures == before) printf("PASS: eviction\n");
}

/* -- The wire buffer's owner ------------------------------------------------------- */

/* Queued Objects whose buffers were reserved and never encoded are released
 * with the sender (no subscriber: pull mode sends nothing). */
static void test_wire_queued_at_destroy(void)
{
    fx_t f;
    opts_t o = { MOQ_VERSION_DRAFT_16, MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01,
                 MOQ_MSF_REVISION_01, 0, 0, false, false };
    if (fx_open(&f, &o)) {
        for (uint64_t k = 0; k < 3; k++) MOQ_TEST_CHECK(write_chunk(&f, 0, k) == MOQ_OK);
        (void)pump_sender(&f);
        MOQ_TEST_CHECK(f.wire_count == 0);
    }
    int before = failures;
    fx_close(&f);
    if (failures == before) printf("PASS: wire_queued_at_destroy\n");
}

/* The facade's last reference goes after the sender is destroyed: the buffer
 * is freed through its own owner, with its exact size. */
static void test_wire_kept_by_facade_after_destroy(void)
{
    fx_t f;
    opts_t o = { MOQ_VERSION_DRAFT_18, MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01,
                 MOQ_MSF_REVISION_01, 0, 0, true, false };
    if (fx_open(&f, &o)) {
        f.keep_next = true;                  /* the hook keeps one reference */
        MOQ_TEST_CHECK(write_chunk(&f, 0, 0) == MOQ_OK);
        for (int i = 0; i < 4 && !f.kept; i++) (void)pump_both(&f);
        MOQ_TEST_CHECK(f.kept != NULL);
        moq_media_receiver_test_destroy_with_session(f.r, moq_simpair_server(f.sp), f.now);
        f.r = NULL;
        moq_media_sender_test_free(f.s);
        f.s = NULL;
        /* The sender is gone; the kept reference still reads the Object. */
        MOQ_TEST_CHECK(f.kept && moq_rcbuf_len(f.kept) > 0 &&
                       ((const uint8_t *)moq_rcbuf_data(f.kept))[0] == 0x02);
    }
    int before = failures;
    fx_close(&f);   /* drops the kept reference last */
    if (failures == before) printf("PASS: wire_kept_by_facade_after_destroy\n");
}

static void test_wire_failed_publish(void)
{
    fx_t f;
    opts_t o = { MOQ_VERSION_DRAFT_16, MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01,
                 MOQ_MSF_REVISION_01, 0, 0, true, false };
    if (fx_open(&f, &o)) {
        f.refuse[0] = MOQ_ERR_INTERNAL;
        f.refuse_count = 1;
        MOQ_TEST_CHECK(write_chunk(&f, 0, 0) == MOQ_OK);
        for (int i = 0; i < 4; i++) (void)pump_both(&f);
        MOQ_TEST_CHECK(f.wire_count == 1);
    }
    int before = failures;
    fx_close(&f);
    if (failures == before) printf("PASS: wire_failed_publish\n");
}

static void test_wire_evicted_after_wrap(void)
{
    fx_t f;
    opts_t o = { MOQ_VERSION_DRAFT_16, MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01,
                 MOQ_MSF_REVISION_01, MOQ_MEDIA_SEND_BP_DROP_GROUP, 3, true, false };
    if (fx_open(&f, &o)) {
        /* Group 0's first Object is wrapped and refused, then its Group is
         * evicted by later ones. */
        f.refuse[0] = MOQ_ERR_WOULD_BLOCK;
        f.refuse_count = 1;
        MOQ_TEST_CHECK(write_chunk(&f, 0, 0) == MOQ_OK);
        MOQ_TEST_CHECK(write_chunk(&f, 0, 1) == MOQ_OK);
        (void)pump_sender(&f);
        MOQ_TEST_CHECK(f.wire_count == 1);
        for (uint64_t g = 1; g < 3; g++)
            for (uint64_t k = 0; k < 2; k++) MOQ_TEST_CHECK(write_chunk(&f, g, k) == MOQ_OK);
        for (int i = 0; i < 16; i++) (void)pump_both(&f);
        check_wire(&f, 1);
    }
    int before = failures;
    fx_close(&f);
    if (failures == before) printf("PASS: wire_evicted_after_wrap\n");
}

static void test_wire_remove_track_queued(void)
{
    fx_t f;
    opts_t o = { MOQ_VERSION_DRAFT_16, MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01,
                 MOQ_MSF_REVISION_01, 0, 0, false, false };
    if (fx_open(&f, &o)) {
        for (uint64_t k = 0; k < 3; k++) MOQ_TEST_CHECK(write_chunk(&f, 0, k) == MOQ_OK);
        MOQ_TEST_CHECK(moq_media_sender_remove_track(f.s, f.st) == MOQ_OK);
        for (int i = 0; i < 4; i++) (void)pump_sender(&f);
    }
    int before = failures;
    fx_close(&f);
    if (failures == before) printf("PASS: wire_remove_track_queued\n");
}

/* The queue charges the chunk and its wire buffer: the owner, rounded to
 * max_align_t, plus the encoder's bound. */
static void test_queue_charge(void)
{
    fx_t f;
    opts_t o = { MOQ_VERSION_DRAFT_16, MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01,
                 MOQ_MSF_REVISION_01, 0, 0, false, false };
    if (fx_open(&f, &o)) {
        uint8_t c[OBJ_CAP];
        size_t n = chunk(&f, 0, 0, c, sizeof(c));
        moq_locmaf_encoder_cfg_t ecfg;
        moq_locmaf_encoder_cfg_init(&ecfg);
        ecfg.track = &f.facts;
        size_t es = 0;
        MOQ_TEST_CHECK(moq_locmaf_encoder_storage_size(&ecfg, &es) == MOQ_OK);
        void *mem = malloc(es);
        ecfg.storage = mem;
        ecfg.storage_size = es;
        moq_locmaf_encoder_t e;
        size_t bound = 0;
        MOQ_TEST_CHECK(mem && moq_locmaf_encoder_init(&e, sizeof(e), &ecfg) == MOQ_OK &&
                       moq_locmaf_encoder_check(&e, (moq_bytes_t){ c, n }, &bound, NULL) == MOQ_OK);
        free(mem);
        size_t owner = sizeof(moq_alloc_t) + sizeof(size_t);
        size_t prefix = (owner + alignof(max_align_t) - 1u) & ~(size_t)(alignof(max_align_t) - 1u);
        MOQ_TEST_CHECK(write_bytes(&f, c, n, true) == MOQ_OK);
        moq_media_sender_stats_t st;
        moq_media_sender_get_stats(f.s, &st, sizeof(st));
        MOQ_TEST_CHECK(st.bytes_queued == n + prefix + bound);
    }
    int before = failures;
    fx_close(&f);
    if (failures == before) printf("PASS: queue_charge\n");
}

int main(void)
{
    static const struct {
        const char *name;
        moq_version_t v;
        moq_media_companion_profile_t c;
    } rows[] = {
        { "legacy-16", MOQ_VERSION_DRAFT_16, MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01 },
        { "legacy-18", MOQ_VERSION_DRAFT_18, MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01 },
        { "loc04-22", MOQ_VERSION_DRAFT_22, MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC },
        { "loc05-22", MOQ_VERSION_DRAFT_22, MOQ_MEDIA_COMPANION_D22_MSF01_LOC05_PUBLIC },
        { "auto-16", MOQ_VERSION_DRAFT_16, MOQ_MEDIA_COMPANION_AUTO },
        { "auto-22", MOQ_VERSION_DRAFT_22, MOQ_MEDIA_COMPANION_AUTO },
    };
    for (int rev = 0; rev < 2; rev++)
        for (size_t i = 0; i < sizeof(rows) / sizeof(rows[0]); i++) {
            char name[64];
            snprintf(name, sizeof(name), "%s r%02d", rows[i].name, rev + 1);
            test_e2e(name, rows[i].v, rows[i].c,
                     rev ? MOQ_MSF_REVISION_02 : MOQ_MSF_REVISION_01);
        }
    test_refusals();
    test_protected_init();
    test_oom_add_track();
    test_oom_write_and_wrap();
    test_would_block_retry();
    test_eviction();
    test_wire_queued_at_destroy();
    test_wire_kept_by_facade_after_destroy();
    test_wire_failed_publish();
    test_wire_evicted_after_wrap();
    test_wire_remove_track_queued();
    test_queue_charge();
    printf("%s: %d failures\n", failures ? "FAIL" : "PASS", failures);
    return failures ? 1 : 0;
}
