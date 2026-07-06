/*
 * Deterministic streaming object delivery scenario runner.
 *
 * Verifies OBJECT_CHUNK event correctness with streaming_objects=true:
 * begin/end pairing, payload reconstruction, no phantom bytes,
 * zero-copy rcbuf lifecycle, and deterministic counters/hash across
 * two runs per seed.
 *
 * Prelude (manual sessions, exact oracle):
 *   1. Small single-chunk object
 *   2. Zero-length object
 *   3. Object fed byte-by-byte
 *   4. Object larger than MOQ_STREAM_CHUNK_MAX
 *   5. Streaming send API (begin_object + write_object_data + end_object)
 *   6. Zero-copy rcbuf continuation (on_data_rcbuf with release callback)
 *   7. Full object in one rcbuf (begin chunk zero-copy emission)
 *   8. WOULD_BLOCK mid-object retry
 *   9. Zero-copy WOULD_BLOCK (rcbuf continuation with pending slice)
 *
 * Random phase (SimPair with client_streaming_objects=true):
 *   Randomized subscribe/accept/open/write/pump/close/reset with
 *   chunk-level oracle. 50/50 rcbuf attempt for eligible tail
 *   chunks via on_data_rcbuf; all other feeds use raw on_data_bytes.
 *   zc_releases == zc_inputs
 *   verified per seed.
 */

#include <moq/sim.h>
#include "../../tests/unit/test_support.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* -- splitmix64 PRNG ------------------------------------------------ */

typedef struct { uint64_t s; } rng_t;

static uint64_t rng_next(rng_t *r) {
    uint64_t z = (r->s += 0x9E3779B97F4A7C15ULL);
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    return z ^ (z >> 31);
}

/* -- FNV-1a trace hash ---------------------------------------------- */

#define FNV_INIT 0xCBF29CE484222325ULL

typedef struct {
    uint64_t hash;
    size_t   count;
    /* Prelude counters. */
    size_t   pre_objects;
    size_t   pre_chunks;
    uint64_t pre_payload_hash;
    /* Random phase counters. */
    size_t   rnd_sent;
    size_t   rnd_chunks;
    size_t   rnd_complete;
    size_t   rnd_wb;
    uint64_t rnd_chunk_hash;
    /* Zero-copy counters.
     * zc_inputs: wrapped rcbufs fed via on_data_rcbuf (prelude + random).
     * zc_chunks: prelude zero-copy continuation chunks only.
     * zc_releases: release callbacks fired (must equal zc_inputs). */
    size_t   zc_inputs;
    size_t   zc_chunks;
    size_t   zc_releases;
    /* Persistent chunk-oracle state (survives across drain calls). */
    bool     rnd_in_object;
    int      rnd_violations;
} trace_summary_t;

/* -- Zero-copy release callbacks ------------------------------------ */

static void zc_release_fn(void *ctx, const uint8_t *data, size_t len)
{
    (void)data; (void)len;
    trace_summary_t *ts = (trace_summary_t *)ctx;
    ts->zc_releases++;
}

typedef struct {
    trace_summary_t *ts;
    moq_alloc_t      alloc;
    size_t           buf_len;
} zc_owned_ctx_t;

static void zc_owned_release_fn(void *ctx, const uint8_t *data, size_t len)
{
    (void)len;
    zc_owned_ctx_t *oc = (zc_owned_ctx_t *)ctx;
    oc->ts->zc_releases++;
    oc->alloc.free((void *)data, oc->buf_len, oc->alloc.ctx);
    oc->alloc.free(oc, sizeof(*oc), oc->alloc.ctx);
}

static void fnv_hash_bytes(uint64_t *h, const uint8_t *d, size_t n) {
    for (size_t i = 0; i < n; i++) {
        *h ^= d[i]; *h *= 0x100000001B3ULL;
    }
}

static void fnv_hash_u64(uint64_t *h, uint64_t v) {
    *h ^= v; *h *= 0x100000001B3ULL;
}

static void trace_hash_fn(void *ctx, const moq_sim_trace_record_t *r) {
    trace_summary_t *s = (trace_summary_t *)ctx;
    uint64_t h = s->hash;
    fnv_hash_u64(&h, r->seed);
    fnv_hash_u64(&h, r->step);
    fnv_hash_u64(&h, r->now_us);
    fnv_hash_u64(&h, (uint64_t)r->kind);
    fnv_hash_u64(&h, (uint64_t)r->from);
    fnv_hash_u64(&h, (uint64_t)r->to);
    fnv_hash_u64(&h, (uint64_t)r->action_kind);
    fnv_hash_u64(&h, (uint64_t)r->input_kind);
    fnv_hash_u64(&h, (uint64_t)r->result);
    fnv_hash_u64(&h, r->code);
    fnv_hash_u64(&h, r->bytes.len);
    if (r->bytes.data && r->bytes.len > 0)
        fnv_hash_bytes(&h, r->bytes.data, r->bytes.len);
    s->hash = h;
    s->count++;
}

/* -- Counting allocator --------------------------------------------- */

typedef struct { int64_t balance; } scen_alloc_t;

static void *sa_alloc(size_t sz, void *ctx) {
    scen_alloc_t *s = (scen_alloc_t *)ctx;
    void *p = malloc(sz); if (p) s->balance++;
    return p;
}
static void *sa_realloc(void *p, size_t o, size_t n, void *ctx) {
    scen_alloc_t *s = (scen_alloc_t *)ctx;
    (void)o;
    if (!p) { void *r = realloc(NULL, n); if (r) s->balance++; return r; }
    return realloc(p, n);
}
static void sa_free(void *p, size_t sz, void *ctx) {
    scen_alloc_t *s = (scen_alloc_t *)ctx;
    (void)sz; if (p) s->balance--;
    free(p);
}

/* -- Prelude helpers ------------------------------------------------ */

static int pump_control(moq_session_t *from, moq_session_t *to, uint64_t now)
{
    moq_action_t acts[16];
    size_t n;
    while ((n = moq_session_poll_actions(from, acts, 16)) > 0)
        for (size_t i = 0; i < n; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL) {
                moq_result_t rc = moq_session_on_control_bytes(
                    to, acts[i].u.send_control.data,
                    acts[i].u.send_control.len, now);
                if (rc < 0) {
                    moq_action_cleanup(&acts[i]);
                    for (size_t j = i + 1; j < n; j++)
                        moq_action_cleanup(&acts[j]);
                    return -1;
                }
            }
            moq_action_cleanup(&acts[i]);
        }
    return 0;
}

static int establish_streaming_pair(const moq_alloc_t *alloc,
    uint32_t max_events, moq_session_t **c_out, moq_session_t **s_out)
{
    moq_session_cfg_t ccfg = MOQ_SESSION_CFG_INIT;
    ccfg.alloc = alloc;
    ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    ccfg.send_request_capacity = true;
    ccfg.initial_request_capacity = 10;
    ccfg.streaming_objects = true;
    if (max_events > 0) ccfg.max_events = max_events;

    moq_session_cfg_t scfg = MOQ_SESSION_CFG_INIT;
    scfg.alloc = alloc;
    scfg.perspective = MOQ_PERSPECTIVE_SERVER;
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 10;

    if (moq_session_create(&ccfg, 0, c_out) < 0) return -1;
    if (moq_session_create(&scfg, 0, s_out) < 0) return -1;
    moq_session_start(*c_out, 0);
    if (pump_control(*c_out, *s_out, 0) < 0) return -1;
    if (pump_control(*s_out, *c_out, 0) < 0) return -1;
    moq_event_t d;
    if (moq_session_poll_events(*c_out, &d, 1) == 1) moq_event_cleanup(&d);
    if (moq_session_poll_events(*s_out, &d, 1) == 1) moq_event_cleanup(&d);
    return 0;
}

static int subscribe_and_accept(moq_session_t *c, moq_session_t *sv,
    moq_subscription_t *ssub_out)
{
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
    moq_namespace_t ns = { ns_parts, 1 };
    moq_subscribe_cfg_t sub_cfg;
    moq_subscribe_cfg_init(&sub_cfg);
    sub_cfg.track_namespace = ns;
    sub_cfg.track_name = MOQ_BYTES_LITERAL("st");

    moq_subscription_t csub;
    if (moq_session_subscribe(c, &sub_cfg, 0, &csub) != MOQ_OK) return -1;
    if (pump_control(c, sv, 0) < 0) return -1;
    moq_event_t ev;
    if (moq_session_poll_events(sv, &ev, 1) != 1) return -1;
    if (ev.kind != MOQ_EVENT_SUBSCRIBE_REQUEST) { moq_event_cleanup(&ev); return -1; }
    *ssub_out = ev.u.subscribe_request.sub;
    moq_event_cleanup(&ev);

    moq_accept_subscribe_cfg_t acc;
    moq_accept_subscribe_cfg_init(&acc);
    if (moq_session_accept_subscribe(sv, *ssub_out, &acc, 0) != MOQ_OK) return -1;
    if (pump_control(sv, c, 0) < 0) return -1;
    if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);
    return 0;
}

/* Collect SEND_DATA actions into a flat buffer. Returns -1 on overflow. */
static int collect_send_data(moq_session_t *sv, uint8_t *buf, size_t cap,
                              size_t *out_len)
{
    size_t total = 0;
    int overflow = 0;
    moq_action_t acts[16]; size_t na;
    while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
        for (size_t i = 0; i < na; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_DATA && !overflow) {
                size_t hl = acts[i].u.send_data.header_len;
                if (hl > 0) {
                    if (total + hl > cap) { overflow = 1; }
                    else { memcpy(buf + total, acts[i].u.send_data.header, hl); total += hl; }
                }
                if (acts[i].u.send_data.payload && !overflow) {
                    size_t pl = moq_rcbuf_len(acts[i].u.send_data.payload);
                    if (total + pl > cap) { overflow = 1; }
                    else { memcpy(buf + total, moq_rcbuf_data(acts[i].u.send_data.payload), pl); total += pl; }
                }
            }
            moq_action_cleanup(&acts[i]);
        }
    *out_len = total;
    return overflow ? -1 : 0;
}

/* Drain and reconstruct OBJECT_CHUNK events into a payload buffer.
 * Returns total reassembled bytes, sets *obj_count and validates
 * begin/end pairing. Returns -1 on oracle violation. */
static int drain_and_reconstruct(moq_session_t *c, uint8_t *out,
    size_t out_cap, size_t *out_len, size_t *chunk_count,
    size_t *obj_count, size_t *wb_count)
{
    bool in_obj = false;
    *out_len = 0;
    *chunk_count = 0;
    *obj_count = 0;

    moq_event_t ev;
    while (moq_session_poll_events(c, &ev, 1) == 1) {
        if (ev.kind != MOQ_EVENT_OBJECT_CHUNK) {
            moq_event_cleanup(&ev);
            continue;
        }
        (*chunk_count)++;

        if (ev.u.object_chunk.begin) {
            if (in_obj) { moq_event_cleanup(&ev); return -1; }
            in_obj = true;
        }
        if (!in_obj) { moq_event_cleanup(&ev); return -1; }

        if (ev.u.object_chunk.chunk) {
            size_t cl = moq_rcbuf_len(ev.u.object_chunk.chunk);
            if (*out_len + cl <= out_cap)
                memcpy(out + *out_len,
                    moq_rcbuf_data(ev.u.object_chunk.chunk), cl);
            *out_len += cl;
        }

        if (ev.u.object_chunk.end) {
            in_obj = false;
            (*obj_count)++;
        }
        moq_event_cleanup(&ev);
    }
    (void)wb_count;
    return in_obj ? 1 : 0;
}

/* -- Prelude -------------------------------------------------------- */

static int run_prelude(uint64_t seed, const moq_alloc_t *alloc,
                        trace_summary_t *ts)
{
    moq_session_t *c = NULL, *sv = NULL;
    int rc = -1;

    /* -- Segment 1-4: normal max_events ------------------------------ */
    if (establish_streaming_pair(alloc, 0, &c, &sv) < 0) goto done;

    moq_subscription_t ssub;
    if (subscribe_and_accept(c, sv, &ssub) < 0) goto done;

    moq_subgroup_cfg_t sg_cfg;
    moq_subgroup_cfg_init(&sg_cfg);
    sg_cfg.group_id = 0;
    sg_cfg.subgroup_id = 0;
    sg_cfg.publisher_priority = 128;
    moq_subgroup_handle_t sg;
    if (moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg) != MOQ_OK) goto done;

    moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(5000);
    uint8_t wire[65536];

    /* Case 1: small object (begin+end in one chunk). */
    {
        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(alloc, (const uint8_t *)"hello", 5, &p);
        moq_session_write_object(sv, sg, 0, p, 0);
        moq_rcbuf_decref(p);

        size_t wlen;
        if (collect_send_data(sv, wire, sizeof(wire), &wlen) < 0) goto done;
        if (moq_session_on_data_bytes(c, rx_ref, wire, wlen, false, 0) != MOQ_OK)
            goto done;

        uint8_t reasm[64]; size_t rlen, cc, oc;
        if (drain_and_reconstruct(c, reasm, sizeof(reasm), &rlen, &cc, &oc, NULL) < 0)
            goto done;
        if (oc != 1 || rlen != 5 || memcmp(reasm, "hello", 5) != 0) goto done;
        ts->pre_objects++;
        ts->pre_chunks += cc;
        fnv_hash_bytes(&ts->pre_payload_hash, reasm, rlen);
    }

    /* Case 2: zero-length object. */
    {
        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(alloc, NULL, 0, &p);
        moq_session_write_object(sv, sg, 1, p, 0);
        moq_rcbuf_decref(p);

        size_t wlen;
        if (collect_send_data(sv, wire, sizeof(wire), &wlen) < 0) goto done;
        if (moq_session_on_data_bytes(c, rx_ref, wire, wlen, false, 0) != MOQ_OK)
            goto done;

        uint8_t reasm[64]; size_t rlen, cc, oc;
        if (drain_and_reconstruct(c, reasm, sizeof(reasm), &rlen, &cc, &oc, NULL) < 0)
            goto done;
        if (oc != 1 || rlen != 0) goto done;
        ts->pre_objects++;
        ts->pre_chunks += cc;
    }

    /* Case 3: byte-by-byte feeding (30-byte payload). */
    {
        uint8_t payload[30];
        for (int i = 0; i < 30; i++) payload[i] = (uint8_t)(i + 0x41);
        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(alloc, payload, sizeof(payload), &p);
        moq_session_write_object(sv, sg, 2, p, 0);
        moq_rcbuf_decref(p);

        size_t wlen;
        if (collect_send_data(sv, wire, sizeof(wire), &wlen) < 0) goto done;
        uint8_t reasm[64]; size_t rlen = 0, cc = 0, oc = 0;
        bool in_obj_bb = false;
        for (size_t off = 0; off < wlen; off++) {
            moq_result_t drc = moq_session_on_data_bytes(c, rx_ref,
                wire + off, 1, false, 0);
            if (drc == MOQ_ERR_WOULD_BLOCK) {
                /* Drain queued chunks and retry. */
                moq_event_t ev;
                while (moq_session_poll_events(c, &ev, 1) == 1) {
                    if (ev.kind == MOQ_EVENT_OBJECT_CHUNK) {
                        cc++;
                        if (ev.u.object_chunk.begin) in_obj_bb = true;
                        if (ev.u.object_chunk.chunk) {
                            size_t cl = moq_rcbuf_len(ev.u.object_chunk.chunk);
                            if (rlen + cl <= sizeof(reasm))
                                memcpy(reasm + rlen,
                                    moq_rcbuf_data(ev.u.object_chunk.chunk), cl);
                            rlen += cl;
                        }
                        if (ev.u.object_chunk.end) { in_obj_bb = false; oc++; }
                    }
                    moq_event_cleanup(&ev);
                }
                drc = moq_session_on_data_bytes(c, rx_ref, NULL, 0, false, 0);
            }
            if (drc < 0) goto done;
        }
        /* Drain any remaining chunks. */
        {
            moq_event_t ev;
            while (moq_session_poll_events(c, &ev, 1) == 1) {
                if (ev.kind == MOQ_EVENT_OBJECT_CHUNK) {
                    cc++;
                    if (ev.u.object_chunk.begin) in_obj_bb = true;
                    if (ev.u.object_chunk.chunk) {
                        size_t cl = moq_rcbuf_len(ev.u.object_chunk.chunk);
                        if (rlen + cl <= sizeof(reasm))
                            memcpy(reasm + rlen,
                                moq_rcbuf_data(ev.u.object_chunk.chunk), cl);
                        rlen += cl;
                    }
                    if (ev.u.object_chunk.end) { in_obj_bb = false; oc++; }
                }
                moq_event_cleanup(&ev);
            }
        }
        (void)in_obj_bb;
        if (oc != 1 || rlen != 30 || memcmp(reasm, payload, 30) != 0) goto done;
        ts->pre_objects++;
        ts->pre_chunks += cc;
        fnv_hash_bytes(&ts->pre_payload_hash, reasm, rlen);
    }

    /* Case 4: large object (>MOQ_STREAM_CHUNK_MAX = 16384). */
    {
        uint8_t payload[20000];
        rng_t prng = { seed ^ 0x4444 };
        for (size_t i = 0; i < sizeof(payload); i++)
            payload[i] = (uint8_t)(rng_next(&prng) & 0xFF);

        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(alloc, payload, sizeof(payload), &p);
        moq_session_write_object(sv, sg, 3, p, 0);
        moq_rcbuf_decref(p);

        size_t wlen;
        if (collect_send_data(sv, wire, sizeof(wire), &wlen) < 0) goto done;
        moq_result_t drc = moq_session_on_data_bytes(c, rx_ref,
            wire, wlen, false, 0);
        if (drc == MOQ_ERR_WOULD_BLOCK) {
            moq_event_t ev;
            while (moq_session_poll_events(c, &ev, 1) == 1)
                moq_event_cleanup(&ev);
            drc = moq_session_on_data_bytes(c, rx_ref, NULL, 0, false, 0);
        }
        if (drc != MOQ_OK) goto done;

        uint8_t reasm[20000]; size_t rlen, cc, oc;
        if (drain_and_reconstruct(c, reasm, sizeof(reasm), &rlen, &cc, &oc, NULL) < 0)
            goto done;
        if (oc != 1 || rlen != sizeof(payload)) goto done;
        if (memcmp(reasm, payload, sizeof(payload)) != 0) goto done;
        if (cc < 2) goto done;
        ts->pre_objects++;
        ts->pre_chunks += cc;
        fnv_hash_bytes(&ts->pre_payload_hash, reasm, rlen);
    }

    /* Case 5: streaming send API (begin + data chunks + end). */
    {
        if (moq_session_begin_object(sv, sg, 4, 12, 0) != MOQ_OK) goto done;
        moq_rcbuf_t *sd1 = NULL, *sd2 = NULL;
        moq_rcbuf_create(alloc, (const uint8_t *)"stream", 6, &sd1);
        moq_rcbuf_create(alloc, (const uint8_t *)"_send!", 6, &sd2);
        if (moq_session_write_object_data(sv, sg, sd1, 0) != MOQ_OK)
            { moq_rcbuf_decref(sd1); moq_rcbuf_decref(sd2); goto done; }
        if (moq_session_write_object_data(sv, sg, sd2, 0) != MOQ_OK)
            { moq_rcbuf_decref(sd1); moq_rcbuf_decref(sd2); goto done; }
        moq_rcbuf_decref(sd1); moq_rcbuf_decref(sd2);
        if (moq_session_end_object(sv, sg, 0) != MOQ_OK) goto done;

        size_t wlen;
        if (collect_send_data(sv, wire, sizeof(wire), &wlen) < 0) goto done;
        if (moq_session_on_data_bytes(c, rx_ref, wire, wlen, false, 0) < 0)
            goto done;

        uint8_t reasm[12]; size_t rlen = 0, cc = 0, oc = 0;
        if (drain_and_reconstruct(c, reasm, sizeof(reasm), &rlen, &cc, &oc, NULL) < 0)
            goto done;
        if (oc != 1 || rlen != 12) goto done;
        if (memcmp(reasm, "stream_send!", 12) != 0) goto done;
        ts->pre_objects++;
        ts->pre_chunks += cc;
        fnv_hash_bytes(&ts->pre_payload_hash, reasm, rlen);
    }

    /* Case 6: zero-copy rcbuf continuation chunk. */
    {
        if (moq_session_begin_object(sv, sg, 5, 15, 0) != MOQ_OK) goto done;
        moq_rcbuf_t *sd = NULL;
        moq_rcbuf_create(alloc, (const uint8_t *)"zerocopy_rcbuf!", 15, &sd);
        if (moq_session_write_object_data(sv, sg, sd, 0) != MOQ_OK)
            { moq_rcbuf_decref(sd); goto done; }
        moq_rcbuf_decref(sd);
        if (moq_session_end_object(sv, sg, 0) != MOQ_OK) goto done;

        /* Collect header-only and payload-only actions separately. */
        uint8_t hdr_wire[64]; size_t hwlen = 0;
        uint8_t pay_wire[64]; size_t pwlen = 0;
        moq_action_t acts[16]; size_t na;
        int ai = 0;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_DATA) {
                    if (ai <= 0 && acts[i].u.send_data.header_len > 0) {
                        memcpy(hdr_wire + hwlen, acts[i].u.send_data.header,
                            acts[i].u.send_data.header_len);
                        hwlen += acts[i].u.send_data.header_len;
                    }
                    if (acts[i].u.send_data.payload) {
                        size_t pl = moq_rcbuf_len(acts[i].u.send_data.payload);
                        memcpy(pay_wire, moq_rcbuf_data(acts[i].u.send_data.payload), pl);
                        pwlen = pl;
                    }
                    ai++;
                }
                moq_action_cleanup(&acts[i]);
            }

        /* Feed header via on_data_bytes (parsed, begin chunk queued). */
        if (moq_session_on_data_bytes(c, rx_ref, hdr_wire, hwlen, false, 0) < 0)
            goto done;

        /* Feed payload via on_data_rcbuf (zero-copy continuation).
         * Don't drain begin chunk yet — drain_and_reconstruct needs it. */
        moq_rcbuf_t *tb = NULL;
        moq_rcbuf_wrap(alloc, pay_wire, pwlen, zc_release_fn, ts, &tb);
        if (!tb) goto done;
        ts->zc_inputs++;
        moq_result_t drc = moq_session_on_data_rcbuf(c, rx_ref, tb, false, 0);
        moq_rcbuf_decref(tb);
        if (drc < 0) goto done;

        uint8_t reasm[15]; size_t rlen = 0, cc = 0, oc = 0;
        if (drain_and_reconstruct(c, reasm, sizeof(reasm), &rlen, &cc, &oc, NULL) < 0)
            goto done;
        if (oc != 1 || rlen != 15) goto done;
        if (memcmp(reasm, "zerocopy_rcbuf!", 15) != 0) goto done;
        ts->pre_objects++;
        ts->pre_chunks += cc;
        ts->zc_chunks++;
        fnv_hash_bytes(&ts->pre_payload_hash, reasm, rlen);
    }

    /* Case 7: full object in one rcbuf (begin chunk zero-copy). */
    {
        moq_rcbuf_t *sd = NULL;
        moq_rcbuf_create(alloc, (const uint8_t *)"begin_zc_pre", 12, &sd);
        moq_session_write_object(sv, sg, 6, sd, 0);
        moq_rcbuf_decref(sd);
        moq_session_close_subgroup(sv, sg, 0);

        /* Collect everything (object header + payload + FIN). */
        uint8_t obj_wire[256]; size_t owl = 0;
        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_DATA) {
                    size_t hl = acts[i].u.send_data.header_len;
                    if (hl > 0) { memcpy(obj_wire + owl, acts[i].u.send_data.header, hl); owl += hl; }
                    if (acts[i].u.send_data.payload) {
                        size_t pl = moq_rcbuf_len(acts[i].u.send_data.payload);
                        memcpy(obj_wire + owl, moq_rcbuf_data(acts[i].u.send_data.payload), pl);
                        owl += pl;
                    }
                }
                moq_action_cleanup(&acts[i]);
            }

        /* Feed as one rcbuf — header+payload parsed, begin+end chunk
         * emitted with zero-copy slice for the payload bytes. */
        size_t pre_rel = ts->zc_releases;
        moq_rcbuf_t *tb = NULL;
        moq_rcbuf_wrap(alloc, obj_wire, owl, zc_release_fn, ts, &tb);
        if (!tb) goto done;
        ts->zc_inputs++;
        if (moq_session_on_data_rcbuf(c, rx_ref, tb, true, 0) < 0)
            { moq_rcbuf_decref(tb); goto done; }
        moq_rcbuf_decref(tb);

        uint8_t reasm[12]; size_t rlen = 0, cc = 0, oc = 0;
        if (drain_and_reconstruct(c, reasm, sizeof(reasm), &rlen, &cc, &oc, NULL) < 0)
            goto done;
        if (oc != 1 || rlen != 12) goto done;
        if (memcmp(reasm, "begin_zc_pre", 12) != 0) goto done;
        if (ts->zc_releases != pre_rel + 1) goto done;
        ts->pre_objects++;
        ts->pre_chunks += cc;
        ts->zc_chunks++;
        fnv_hash_bytes(&ts->pre_payload_hash, reasm, rlen);
    }

    moq_session_destroy(c); c = NULL;
    moq_session_destroy(sv); sv = NULL;

    /* -- Segment 7: WOULD_BLOCK mid-object (max_events=1) ------------ */
    if (establish_streaming_pair(alloc, 1, &c, &sv) < 0) goto done;

    /* Manual subscribe/accept WITHOUT draining SUBSCRIBE_OK from client,
     * so the 1-slot event queue remains full for the data feed. */
    {
        moq_bytes_t ns5[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t nns = { ns5, 1 };
        moq_subscribe_cfg_t scfg5;
        moq_subscribe_cfg_init(&scfg5);
        scfg5.track_namespace = nns;
        scfg5.track_name = MOQ_BYTES_LITERAL("wb");
        moq_subscription_t csub5;
        if (moq_session_subscribe(c, &scfg5, 0, &csub5) != MOQ_OK) goto done;
        if (pump_control(c, sv, 0) < 0) goto done;
        moq_event_t sev;
        if (moq_session_poll_events(sv, &sev, 1) != 1) goto done;
        if (sev.kind != MOQ_EVENT_SUBSCRIBE_REQUEST) { moq_event_cleanup(&sev); goto done; }
        ssub = sev.u.subscribe_request.sub;
        moq_event_cleanup(&sev);
        moq_accept_subscribe_cfg_t acc5;
        moq_accept_subscribe_cfg_init(&acc5);
        if (moq_session_accept_subscribe(sv, ssub, &acc5, 0) != MOQ_OK) goto done;
        if (pump_control(sv, c, 0) < 0) goto done;
        /* SUBSCRIBE_OK now queued on client — DO NOT drain. */
    }

    moq_subgroup_cfg_init(&sg_cfg);
    sg_cfg.group_id = 0; sg_cfg.subgroup_id = 0;
    if (moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg) != MOQ_OK) goto done;

    {
        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(alloc, (const uint8_t *)"retry", 5, &p);
        moq_session_write_object(sv, sg, 0, p, 0);
        moq_rcbuf_decref(p);
        moq_session_close_subgroup(sv, sg, 0);

        rx_ref = moq_stream_ref_from_u64(5001);
        size_t wlen;
        if (collect_send_data(sv, wire, sizeof(wire), &wlen) < 0) goto done;

        /* Event queue full (SUBSCRIBE_OK) → must WOULD_BLOCK. */
        moq_result_t drc = moq_session_on_data_bytes(c, rx_ref,
            wire, wlen, true, 0);
        if (drc != MOQ_ERR_WOULD_BLOCK) goto done;

        /* Drain SUBSCRIBE_OK. */
        moq_event_t ev;
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);

        /* Retry. The OBJECT_CHUNK emits and fills the 1-slot queue, so the
         * trailing SUBGROUP_FINISHED backpressures (WOULD_BLOCK, stream held
         * in PENDING_FINISHED). */
        drc = moq_session_on_data_bytes(c, rx_ref, NULL, 0, false, 0);
        if (drc != MOQ_ERR_WOULD_BLOCK) goto done;

        uint8_t reasm[64]; size_t rlen, cc, oc;
        if (drain_and_reconstruct(c, reasm, sizeof(reasm), &rlen, &cc, &oc, NULL) < 0)
            goto done;
        if (oc != 1 || rlen != 5 || memcmp(reasm, "retry", 5) != 0) goto done;

        /* Queue drained: retry so SUBGROUP_FINISHED can be queued. */
        if (moq_session_on_data_bytes(c, rx_ref, NULL, 0, false, 0) != MOQ_OK)
            goto done;
        { moq_event_t sgf;
          if (moq_session_poll_events(c, &sgf, 1) != 1 ||
              sgf.kind != MOQ_EVENT_SUBGROUP_FINISHED) {
              moq_event_cleanup(&sgf); goto done; }
          moq_event_cleanup(&sgf); }
        ts->pre_objects++;
        ts->pre_chunks += cc;
        ts->rnd_wb++;
        fnv_hash_bytes(&ts->pre_payload_hash, reasm, rlen);
    }

    /* Clean up segment 7 pair before creating segment 8 pair. */
    { moq_event_t d[16]; moq_action_t a[16]; size_t ne, na;
      if (c) { while ((ne = moq_session_poll_events(c, d, 16)) > 0)
                   for (size_t i = 0; i < ne; i++) moq_event_cleanup(&d[i]);
               while ((na = moq_session_poll_actions(c, a, 16)) > 0)
                   for (size_t i = 0; i < na; i++) moq_action_cleanup(&a[i]); }
      if (sv) { while ((ne = moq_session_poll_events(sv, d, 16)) > 0)
                    for (size_t i = 0; i < ne; i++) moq_event_cleanup(&d[i]);
                while ((na = moq_session_poll_actions(sv, a, 16)) > 0)
                    for (size_t i = 0; i < na; i++) moq_action_cleanup(&a[i]); }
    }
    if (c) { moq_session_destroy(c); c = NULL; }
    if (sv) { moq_session_destroy(sv); sv = NULL; }

    /* -- Segment 8: zero-copy WOULD_BLOCK on rcbuf continuation -------- */
    if (establish_streaming_pair(alloc, 1, &c, &sv) < 0) goto done;
    {
        moq_bytes_t ns8[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t nns8 = { ns8, 1 };
        moq_subscribe_cfg_t scfg8;
        moq_subscribe_cfg_init(&scfg8);
        scfg8.track_namespace = nns8;
        scfg8.track_name = MOQ_BYTES_LITERAL("zcwb");
        moq_subscription_t csub8;
        if (moq_session_subscribe(c, &scfg8, 0, &csub8) != MOQ_OK) goto done;
        if (pump_control(c, sv, 0) < 0) goto done;
        moq_event_t sev;
        if (moq_session_poll_events(sv, &sev, 1) != 1) goto done;
        if (sev.kind != MOQ_EVENT_SUBSCRIBE_REQUEST) { moq_event_cleanup(&sev); goto done; }
        ssub = sev.u.subscribe_request.sub;
        moq_event_cleanup(&sev);
        moq_accept_subscribe_cfg_t acc8;
        moq_accept_subscribe_cfg_init(&acc8);
        if (moq_session_accept_subscribe(sv, ssub, &acc8, 0) != MOQ_OK) goto done;
        if (pump_control(sv, c, 0) < 0) goto done;
        /* Drain SUBSCRIBE_OK so we can control timing. */
        moq_event_t sev2;
        if (moq_session_poll_events(c, &sev2, 1) == 1) moq_event_cleanup(&sev2);
    }

    moq_subgroup_cfg_init(&sg_cfg);
    sg_cfg.group_id = 0; sg_cfg.subgroup_id = 0;
    if (moq_session_open_subgroup(sv, ssub, &sg_cfg, 0, &sg) != MOQ_OK) goto done;

    {
        if (moq_session_begin_object(sv, sg, 0, 8, 0) != MOQ_OK) goto done;
        moq_rcbuf_t *sd = NULL;
        moq_rcbuf_create(alloc, (const uint8_t *)"zcwb_pay", 8, &sd);
        if (moq_session_write_object_data(sv, sg, sd, 0) != MOQ_OK)
            { moq_rcbuf_decref(sd); goto done; }
        moq_rcbuf_decref(sd);
        if (moq_session_end_object(sv, sg, 0) != MOQ_OK) goto done;
        moq_session_close_subgroup(sv, sg, 0);

        /* Collect header and payload separately. */
        uint8_t hw[64]; size_t hwl = 0;
        uint8_t pw[64]; size_t pwl = 0;
        moq_action_t acts[16]; size_t na;
        int ai = 0;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_DATA) {
                    size_t hl = acts[i].u.send_data.header_len;
                    if (hl > 0 && ai <= 1) { memcpy(hw + hwl, acts[i].u.send_data.header, hl); hwl += hl; }
                    if (acts[i].u.send_data.payload) {
                        size_t pl = moq_rcbuf_len(acts[i].u.send_data.payload);
                        memcpy(pw, moq_rcbuf_data(acts[i].u.send_data.payload), pl);
                        pwl = pl;
                    }
                    ai++;
                }
                moq_action_cleanup(&acts[i]);
            }

        rx_ref = moq_stream_ref_from_u64(5002);

        /* Feed header — begin chunk fills 1-slot queue. */
        if (moq_session_on_data_bytes(c, rx_ref, hw, hwl, false, 0) != MOQ_OK)
            goto done;
        /* DON'T drain — queue full with begin chunk. */

        /* Feed payload via on_data_rcbuf — must WOULD_BLOCK. */
        size_t pre_releases = ts->zc_releases;
        moq_rcbuf_t *tb = NULL;
        moq_rcbuf_wrap(alloc, pw, pwl, zc_release_fn, ts, &tb);
        if (!tb) goto done;
        ts->zc_inputs++;
        moq_result_t drc = moq_session_on_data_rcbuf(c, rx_ref,
            tb, false, 0);
        moq_rcbuf_decref(tb);
        if (drc != MOQ_ERR_WOULD_BLOCK) goto done;
        /* Release must NOT fire — pending slice holds ref. */
        if (ts->zc_releases != pre_releases) goto done;

        /* Drain begin chunk. */
        moq_event_t ev;
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);

        /* Retry. This segment feeds no FIN (only header + payload), so the
         * continuation chunk completes the object with MOQ_OK and no
         * SUBGROUP_FINISHED. */
        drc = moq_session_on_data_bytes(c, rx_ref, NULL, 0, false, 0);
        if (drc != MOQ_OK) goto done;

        /* The final chunk should be a continuation (begin was drained). */
        moq_event_t cev;
        if (moq_session_poll_events(c, &cev, 1) != 1) goto done;
        if (cev.kind != MOQ_EVENT_OBJECT_CHUNK) { moq_event_cleanup(&cev); goto done; }
        if (cev.u.object_chunk.end != true) { moq_event_cleanup(&cev); goto done; }
        if (!cev.u.object_chunk.chunk) { moq_event_cleanup(&cev); goto done; }
        if (moq_rcbuf_len(cev.u.object_chunk.chunk) != 8) { moq_event_cleanup(&cev); goto done; }
        if (memcmp(moq_rcbuf_data(cev.u.object_chunk.chunk), "zcwb_pay", 8) != 0)
            { moq_event_cleanup(&cev); goto done; }
        moq_event_cleanup(&cev);

        ts->pre_objects++;
        ts->pre_chunks += 2;
        ts->zc_chunks++;
        ts->rnd_wb++;
        fnv_hash_bytes(&ts->pre_payload_hash,
            (const uint8_t *)"zcwb_pay", 8);
        /* Release should have fired after event cleanup. */
        if (ts->zc_releases != pre_releases + 1) goto done;
    }

    rc = 0;
done:
    { /* Drain all owned resources. */
        moq_event_t d[16]; moq_action_t a[16]; size_t ne, na;
        if (c) { while ((ne = moq_session_poll_events(c, d, 16)) > 0)
                     for (size_t i = 0; i < ne; i++) moq_event_cleanup(&d[i]);
                 while ((na = moq_session_poll_actions(c, a, 16)) > 0)
                     for (size_t i = 0; i < na; i++) moq_action_cleanup(&a[i]); }
        if (sv) { while ((ne = moq_session_poll_events(sv, d, 16)) > 0)
                      for (size_t i = 0; i < ne; i++) moq_event_cleanup(&d[i]);
                  while ((na = moq_session_poll_actions(sv, a, 16)) > 0)
                      for (size_t i = 0; i < na; i++) moq_action_cleanup(&a[i]); }
    }
    if (c) moq_session_destroy(c);
    if (sv) moq_session_destroy(sv);
    return rc;
}

static void drain_client_chunks(moq_session_t *c, trace_summary_t *ts);

/* Drive a WOULD_BLOCK on the client receive to completion. A single feed can
 * park more than one delivery in the 1-slot event queue -- e.g. a final object
 * or chunk, followed by the SUBGROUP_FINISHED emitted on graceful FIN -- so one
 * drain+retry is not enough. Drain the queued events (freeing slots) and
 * re-drive the held stream (NULL,0: no new bytes, only the parked delivery is
 * re-attempted) until it makes progress or terminates. Bounded so a genuine
 * stall cannot spin. Mirrors run_prelude's completion discipline. */
static moq_result_t pump_resolve_would_block(moq_simpair_t *sp,
        moq_session_t *client, moq_stream_ref_t ref,
        moq_result_t drc, trace_summary_t *ts) {
    int guard = 0;
    while (drc == MOQ_ERR_WOULD_BLOCK && guard++ < 256) {
        ts->rnd_wb++;
        drain_client_chunks(client, ts);
        drc = moq_session_on_data_bytes(client, ref, NULL, 0, false,
                                        moq_simpair_now_us(sp));
    }
    return drc;
}

/* -- Random phase: chunked data pump -------------------------------- */

static int pump_with_chunking(moq_simpair_t *sp, rng_t *chunk_rng,
                               trace_summary_t *ts,
                               const moq_alloc_t *alloc) {
    moq_session_t *server = moq_simpair_server(sp);
    moq_session_t *client = moq_simpair_client(sp);

    for (int rounds = 0; rounds < 4; rounds++) {
        moq_action_t acts[16];
        size_t na = moq_session_poll_actions(server, acts, 16);
        if (na == 0) {
            moq_action_t cacts[16];
            size_t cn = moq_session_poll_actions(client, cacts, 16);
            for (size_t i = 0; i < cn; i++) {
                if (cacts[i].kind == MOQ_ACTION_SEND_CONTROL) {
                    moq_result_t crc = moq_session_on_control_bytes(
                        server, cacts[i].u.send_control.data,
                        cacts[i].u.send_control.len,
                        moq_simpair_now_us(sp));
                    if (crc < 0) {
                        moq_action_cleanup(&cacts[i]);
                        for (size_t j = i + 1; j < cn; j++)
                            moq_action_cleanup(&cacts[j]);
                        return -1;
                    }
                }
                moq_action_cleanup(&cacts[i]);
            }
            if (cn == 0) break;
            continue;
        }

        for (size_t i = 0; i < na; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL) {
                moq_result_t crc = moq_session_on_control_bytes(
                    client, acts[i].u.send_control.data,
                    acts[i].u.send_control.len,
                    moq_simpair_now_us(sp));
                if (crc < 0) {
                    moq_action_cleanup(&acts[i]);
                    for (size_t j = i + 1; j < na; j++)
                        moq_action_cleanup(&acts[j]);
                    return -1;
                }
            } else if (acts[i].kind == MOQ_ACTION_SEND_DATA) {
                uint8_t combined[4096];
                size_t clen = 0;
                size_t hlen = acts[i].u.send_data.header_len;
                if (hlen > sizeof(combined)) {
                    moq_action_cleanup(&acts[i]);
                    for (size_t j = i + 1; j < na; j++)
                        moq_action_cleanup(&acts[j]);
                    return -1;
                }
                if (hlen > 0) {
                    memcpy(combined, acts[i].u.send_data.header, hlen);
                    clen = hlen;
                }
                if (acts[i].u.send_data.payload) {
                    size_t plen = moq_rcbuf_len(acts[i].u.send_data.payload);
                    if (clen + plen > sizeof(combined)) {
                        moq_action_cleanup(&acts[i]);
                        for (size_t j = i + 1; j < na; j++)
                            moq_action_cleanup(&acts[j]);
                        return -1;
                    }
                    memcpy(combined + clen,
                        moq_rcbuf_data(acts[i].u.send_data.payload), plen);
                    clen += plen;
                }
                bool fin = acts[i].u.send_data.fin;

                moq_stream_ref_t ref = moq_stream_ref_from_u64(
                    acts[i].u.send_data.stream_ref._v + 10000);
                size_t off = 0;
                while (off < clen) {
                    size_t chunk = (rng_next(chunk_rng) % 8) + 1;
                    if (chunk > clen - off) chunk = clen - off;
                    bool chunk_fin = fin && (off + chunk >= clen);
                    moq_result_t drc;

                    /* 50/50 coin flip: use rcbuf path when the chunk
                     * is the entire remaining data (no tail). */
                    bool use_rcbuf = (rng_next(chunk_rng) & 1) &&
                                     (off + chunk >= clen);
                    if (use_rcbuf && alloc) {
                        /* Allocate stable backing + context for the
                         * wrapped rcbuf — combined[] is stack-local. */
                        zc_owned_ctx_t *oc = (zc_owned_ctx_t *)alloc->alloc(
                            sizeof(zc_owned_ctx_t), alloc->ctx);
                        uint8_t *stable = oc ? (uint8_t *)alloc->alloc(
                            chunk, alloc->ctx) : NULL;
                        if (oc && stable) {
                            memcpy(stable, combined + off, chunk);
                            oc->ts = ts;
                            oc->alloc = *alloc;
                            oc->buf_len = chunk;
                            moq_rcbuf_t *rb = NULL;
                            moq_rcbuf_wrap(alloc, stable, chunk,
                                zc_owned_release_fn, oc, &rb);
                            if (rb) {
                                ts->zc_inputs++;
                                drc = moq_session_on_data_rcbuf(
                                    client, ref, rb, chunk_fin,
                                    moq_simpair_now_us(sp));
                                moq_rcbuf_decref(rb);
                            } else {
                                alloc->free(stable, chunk, alloc->ctx);
                                alloc->free(oc, sizeof(*oc), alloc->ctx);
                                drc = moq_session_on_data_bytes(
                                    client, ref, combined + off, chunk,
                                    chunk_fin, moq_simpair_now_us(sp));
                            }
                        } else {
                            if (oc) alloc->free(oc, sizeof(*oc), alloc->ctx);
                            if (stable) alloc->free(stable, chunk, alloc->ctx);
                            drc = moq_session_on_data_bytes(
                                client, ref, combined + off, chunk,
                                chunk_fin, moq_simpair_now_us(sp));
                        }
                    } else {
                        drc = moq_session_on_data_bytes(
                            client, ref, combined + off, chunk,
                            chunk_fin, moq_simpair_now_us(sp));
                    }

                    drc = pump_resolve_would_block(sp, client, ref, drc, ts);
                    if (drc < 0) {
                        moq_action_cleanup(&acts[i]);
                        for (size_t j = i + 1; j < na; j++)
                            moq_action_cleanup(&acts[j]);
                        return -1;
                    }
                    off += chunk;
                }
                if (clen == 0 && fin) {
                    /* Zero-length FIN-only delivery: the graceful FIN can park a
                     * SUBGROUP_FINISHED it cannot queue -- resolve it with the
                     * same discipline as the chunk path, or a co-batched action
                     * (e.g. the next stream's header) is discarded on abort. */
                    moq_result_t frc = moq_session_on_data_bytes(
                        client, ref, NULL, 0, true,
                        moq_simpair_now_us(sp));
                    frc = pump_resolve_would_block(sp, client, ref, frc, ts);
                    if (frc < 0) {
                        moq_action_cleanup(&acts[i]);
                        for (size_t j = i + 1; j < na; j++)
                            moq_action_cleanup(&acts[j]);
                        return -1;
                    }
                }
                if (off == clen && acts[i].u.send_data.payload &&
                    moq_session_state(client) != MOQ_SESS_CLOSED)
                    ts->rnd_sent++;
            } else if (acts[i].kind == MOQ_ACTION_RESET_DATA) {
                moq_stream_ref_t ref = moq_stream_ref_from_u64(
                    acts[i].u.reset_data.stream_ref._v + 10000);
                moq_result_t rrc = moq_session_on_data_reset(client, ref,
                    acts[i].u.reset_data.error_code,
                    moq_simpair_now_us(sp));
                if (rrc < 0) {
                    moq_action_cleanup(&acts[i]);
                    for (size_t j = i + 1; j < na; j++)
                        moq_action_cleanup(&acts[j]);
                    return -1;
                }
            }
            moq_action_cleanup(&acts[i]);
        }
    }
    return 0;
}

/* -- Random phase: chunk oracle ------------------------------------- */

static void oracle_observe_chunk(trace_summary_t *ts,
                                  const moq_object_chunk_event_t *ck)
{
    ts->rnd_chunks++;
    if (ck->begin) {
        if (ts->rnd_in_object) ts->rnd_violations++;
        ts->rnd_in_object = true;
    }
    if (!ts->rnd_in_object)
        ts->rnd_violations++;
    if (ck->chunk) {
        size_t cl = moq_rcbuf_len(ck->chunk);
        fnv_hash_bytes(&ts->rnd_chunk_hash,
            moq_rcbuf_data(ck->chunk), cl);
        fnv_hash_u64(&ts->rnd_chunk_hash, cl);
    }
    if (ck->end) {
        ts->rnd_in_object = false;
        ts->rnd_complete++;
    }
}

static void drain_client_chunks(moq_session_t *c, trace_summary_t *ts)
{
    moq_event_t evts[8];
    size_t ne;
    while ((ne = moq_session_poll_events(c, evts, 8)) > 0)
        for (size_t i = 0; i < ne; i++) {
            if (evts[i].kind == MOQ_EVENT_OBJECT_CHUNK)
                oracle_observe_chunk(ts, &evts[i].u.object_chunk);
            moq_event_cleanup(&evts[i]);
        }
}

/* -- Random phase: shadow model ------------------------------------- */

#define MAX_SUBS 4
#define MAX_SGS  4

typedef struct {
    bool active; bool accepted;
    moq_subscription_t server_handle, client_handle;
} shadow_sub_t;

typedef struct {
    bool active;
    moq_subgroup_handle_t handle;
    uint64_t next_object_id;
} shadow_sg_t;

typedef struct {
    shadow_sub_t subs[MAX_SUBS];
    shadow_sg_t  sgs[MAX_SGS];
    bool         closed;
    uint64_t     next_group;
} shadow_state_t;

typedef enum {
    OP_PUMP, OP_SUBSCRIBE, OP_ACCEPT_SUBSCRIBE,
    OP_OPEN_SUBGROUP, OP_WRITE_OBJECT, OP_CLOSE_SUBGROUP,
    OP_RESET_SUBGROUP, OP_DRAIN_CLIENT, OP_DRAIN_SERVER,
    OP_ADVANCE_TIME, OP_COUNT,
} scenario_op_t;

/* -- Random phase: execute step ------------------------------------- */

static int execute_step(moq_simpair_t *sp, shadow_state_t *st,
                         rng_t *rng, rng_t *chunk_rng,
                         trace_summary_t *ts, const moq_alloc_t *alloc)
{
    if (st->closed) return 0;

    scenario_op_t op = (scenario_op_t)(rng_next(rng) % OP_COUNT);
    moq_session_t *c = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);

    switch (op) {
    case OP_PUMP:
        if (pump_with_chunking(sp, chunk_rng, ts, alloc) < 0) return -1;
        break;

    case OP_SUBSCRIBE: {
        char name[16];
        snprintf(name, sizeof(name), "t%llu",
                 (unsigned long long)(rng_next(rng) % 1000));
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t cfg;
        moq_subscribe_cfg_init(&cfg);
        cfg.track_namespace = ns;
        cfg.track_name = moq_bytes_cstr(name);
        moq_subscription_t h;
        moq_result_t rc = moq_session_subscribe(c, &cfg,
            moq_simpair_now_us(sp), &h);
        if (rc == MOQ_OK) {
            pump_with_chunking(sp, chunk_rng, ts, alloc);
            moq_event_t evts[4];
            size_t ne = moq_session_poll_events(sv, evts, 4);
            for (size_t j = 0; j < ne; j++) {
                if (evts[j].kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                    for (int k = 0; k < MAX_SUBS; k++) {
                        if (!st->subs[k].active) {
                            st->subs[k].active = true;
                            st->subs[k].accepted = false;
                            st->subs[k].server_handle = evts[j].u.subscribe_request.sub;
                            st->subs[k].client_handle = h;
                            break;
                        }
                    }
                }
                moq_event_cleanup(&evts[j]);
            }
        }
        break;
    }

    case OP_ACCEPT_SUBSCRIBE: {
        int idx = -1;
        for (int i = 0; i < MAX_SUBS; i++)
            if (st->subs[i].active && !st->subs[i].accepted) { idx = i; break; }
        if (idx < 0) { pump_with_chunking(sp, chunk_rng, ts, alloc); break; }
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_result_t rc = moq_session_accept_subscribe(sv,
            st->subs[idx].server_handle, &acc, moq_simpair_now_us(sp));
        if (rc == MOQ_OK) {
            st->subs[idx].accepted = true;
            pump_with_chunking(sp, chunk_rng, ts, alloc);
            drain_client_chunks(c, ts);
        }
        break;
    }

    case OP_OPEN_SUBGROUP: {
        int sub_idx = -1;
        for (int i = 0; i < MAX_SUBS; i++)
            if (st->subs[i].active && st->subs[i].accepted) { sub_idx = i; break; }
        if (sub_idx < 0) { pump_with_chunking(sp, chunk_rng, ts, alloc); break; }
        int sg_slot = -1;
        for (int i = 0; i < MAX_SGS; i++)
            if (!st->sgs[i].active) { sg_slot = i; break; }
        if (sg_slot < 0) { pump_with_chunking(sp, chunk_rng, ts, alloc); break; }
        moq_subgroup_cfg_t cfg;
        moq_subgroup_cfg_init(&cfg);
        cfg.group_id = st->next_group;
        cfg.subgroup_id = 0; cfg.publisher_priority = 128;
        moq_subgroup_handle_t h;
        moq_result_t rc = moq_session_open_subgroup(sv,
            st->subs[sub_idx].server_handle, &cfg,
            moq_simpair_now_us(sp), &h);
        if (rc == MOQ_OK) {
            st->sgs[sg_slot].active = true;
            st->sgs[sg_slot].handle = h;
            st->sgs[sg_slot].next_object_id = 0;
            st->next_group++;
        }
        break;
    }

    case OP_WRITE_OBJECT: {
        int sg_idx = -1;
        for (int i = 0; i < MAX_SGS; i++)
            if (st->sgs[i].active) { sg_idx = i; break; }
        if (sg_idx < 0) { pump_with_chunking(sp, chunk_rng, ts, alloc); break; }
        uint64_t obj_id = st->sgs[sg_idx].next_object_id;
        size_t payload_len = rng_next(rng) % 64;
        if (rng_next(rng) % 10 == 0) payload_len = 0;
        uint8_t payload_data[64];
        for (size_t j = 0; j < payload_len; j++)
            payload_data[j] = (uint8_t)(rng_next(rng) & 0xFF);
        bool use_streaming = (rng_next(rng) & 1) != 0;
        if (use_streaming) {
            moq_result_t rc = moq_session_begin_object(sv,
                st->sgs[sg_idx].handle, obj_id, payload_len,
                moq_simpair_now_us(sp));
            if (rc == MOQ_OK && payload_len > 0) {
                moq_rcbuf_t *p = NULL;
                moq_rcbuf_create(alloc, payload_data, payload_len, &p);
                if (p) {
                    rc = moq_session_write_object_data(sv,
                        st->sgs[sg_idx].handle, p, moq_simpair_now_us(sp));
                    moq_rcbuf_decref(p);
                }
            }
            if (rc == MOQ_OK)
                rc = moq_session_end_object(sv, st->sgs[sg_idx].handle,
                    moq_simpair_now_us(sp));
            if (rc == MOQ_OK) {
                st->sgs[sg_idx].next_object_id = obj_id + 1;
                if (payload_len == 0) ts->rnd_sent++;
            }
        } else {
            moq_rcbuf_t *p = NULL;
            moq_rcbuf_create(alloc, payload_data, payload_len, &p);
            if (!p) break;
            moq_result_t rc = moq_session_write_object(sv,
                st->sgs[sg_idx].handle, obj_id, p, moq_simpair_now_us(sp));
            moq_rcbuf_decref(p);
            if (rc == MOQ_OK) st->sgs[sg_idx].next_object_id = obj_id + 1;
        }
        break;
    }

    case OP_CLOSE_SUBGROUP: {
        int sg_idx = -1;
        for (int i = 0; i < MAX_SGS; i++)
            if (st->sgs[i].active) { sg_idx = i; break; }
        if (sg_idx < 0) { pump_with_chunking(sp, chunk_rng, ts, alloc); break; }
        if (moq_session_close_subgroup(sv, st->sgs[sg_idx].handle,
            moq_simpair_now_us(sp)) == MOQ_OK)
            st->sgs[sg_idx].active = false;
        break;
    }

    case OP_RESET_SUBGROUP: {
        int sg_idx = -1;
        for (int i = 0; i < MAX_SGS; i++)
            if (st->sgs[i].active) { sg_idx = i; break; }
        if (sg_idx < 0) { pump_with_chunking(sp, chunk_rng, ts, alloc); break; }
        if (moq_session_reset_subgroup(sv, st->sgs[sg_idx].handle,
            0x1, moq_simpair_now_us(sp)) == MOQ_OK)
            st->sgs[sg_idx].active = false;
        break;
    }

    case OP_DRAIN_CLIENT:
        drain_client_chunks(c, ts);
        break;

    case OP_DRAIN_SERVER: {
        moq_event_t evts[8];
        size_t ne = moq_session_poll_events(sv, evts, 8);
        for (size_t i = 0; i < ne; i++) moq_event_cleanup(&evts[i]);
        break;
    }

    case OP_ADVANCE_TIME: {
        uint64_t delta = (rng_next(rng) % 100) + 1;
        moq_simpair_advance_to(sp, moq_simpair_now_us(sp) + delta);
        break;
    }

    default:
        pump_with_chunking(sp, chunk_rng, ts, alloc);
        break;
    }
    return 0;
}

/* -- Run one scenario ----------------------------------------------- */

static int run_scenario(uint64_t seed, const moq_alloc_t *alloc,
                         trace_summary_t *summary, int run_id,
                         int max_steps)
{
    memset(summary, 0, sizeof(*summary));
    summary->hash = FNV_INIT;
    summary->pre_payload_hash = FNV_INIT;
    summary->rnd_chunk_hash = FNV_INIT;

    if (run_prelude(seed, alloc, summary) < 0) {
        fprintf(stderr, "FAIL seed=0x%llx run=%d: prelude failed\n",
                (unsigned long long)seed, run_id);
        fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx "
                "MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%d "
                "./build/tests/test_scenario_streaming_object\n",
                (unsigned long long)seed, max_steps);
        return 1;
    }

    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = alloc;
    cfg.seed = seed;
    cfg.initial_now_us = 1000;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 32;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 32;
    cfg.trace_fn = trace_hash_fn;
    cfg.trace_ctx = summary;
    cfg.client_streaming_objects = true;

    moq_simpair_t *sp = NULL;
    if (moq_simpair_create(&cfg, &sp) < 0) return -1;
    moq_simpair_start(sp);
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    {
        moq_event_t ev;
        if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);
        if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);
    }

    shadow_state_t st;
    memset(&st, 0, sizeof(st));
    rng_t rng = { seed };
    rng_t chunk_rng = { seed ^ 0xDEADBEEFCAFE1234ULL };
    int failures = 0;

    for (int step = 0; step < max_steps; step++) {
        int src = execute_step(sp, &st, &rng, &chunk_rng, summary,
                               alloc);
        if (src < 0) {
            fprintf(stderr, "FAIL seed=0x%llx run=%d step=%d: "
                    "data input error\n",
                    (unsigned long long)seed, run_id, step);
            fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx "
                    "MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%d "
                    "./build/tests/test_scenario_streaming_object\n",
                    (unsigned long long)seed, max_steps);
            failures++;
            st.closed = true;
            break;
        }

        if (moq_session_state(moq_simpair_client(sp)) == MOQ_SESS_CLOSED ||
            moq_session_state(moq_simpair_server(sp)) == MOQ_SESS_CLOSED) {
            if (!st.closed) {
                fprintf(stderr, "FAIL seed=0x%llx run=%d step=%d: "
                        "unexpected close\n",
                        (unsigned long long)seed, run_id, step);
                fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx "
                        "MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%d "
                        "./build/tests/test_scenario_streaming_object\n",
                        (unsigned long long)seed, max_steps);
                failures++;
            }
            st.closed = true;
            break;
        }
    }

    /* Final pump + drain. */
    if (!st.closed) {
        for (int f = 0; f < 16; f++) {
            if (pump_with_chunking(sp, &chunk_rng, summary, alloc) < 0)
                { failures++; break; }
            drain_client_chunks(moq_simpair_client(sp), summary);
        }
    }

    /* Drain all remaining owned resources through the oracle. */
    drain_client_chunks(moq_simpair_client(sp), summary);
    {
        moq_event_t d[16]; size_t ne;
        while ((ne = moq_session_poll_events(moq_simpair_server(sp), d, 16)) > 0)
            for (size_t i = 0; i < ne; i++) moq_event_cleanup(&d[i]);
        moq_action_t a[16]; size_t na;
        while ((na = moq_session_poll_actions(moq_simpair_server(sp), a, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&a[i]);
        while ((na = moq_session_poll_actions(moq_simpair_client(sp), a, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&a[i]);
    }

    if (summary->rnd_violations > 0) {
        fprintf(stderr, "FAIL seed=0x%llx run=%d: %d chunk oracle "
                "violations (begin/end pairing)\n",
                (unsigned long long)seed, run_id, summary->rnd_violations);
        fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx "
                "MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%d "
                "./build/tests/test_scenario_streaming_object\n",
                (unsigned long long)seed, max_steps);
        failures++;
    }

    moq_simpair_destroy(sp);
    return failures;
}

/* -- Main ----------------------------------------------------------- */

int main(void)
{
    int failures = 0;
    const char *env_seeds = getenv("MOQ_SCENARIO_SEEDS");
    const char *env_start = getenv("MOQ_SCENARIO_SEED_START");
    const char *env_steps = getenv("MOQ_SCENARIO_STEPS");
    uint64_t num_seeds = env_seeds ? (uint64_t)strtoull(env_seeds, NULL, 0) : 100;
    uint64_t seed_start = env_start ? (uint64_t)strtoull(env_start, NULL, 0) : 0;
    int max_steps = env_steps ? atoi(env_steps) : 50;

    size_t total_pre = 0, total_chunks = 0, total_complete = 0, total_wb = 0;
    size_t total_rnd_sent = 0, total_rnd_complete = 0;
    size_t total_zc_in = 0, total_zc_chunks = 0, total_zc_rel = 0;

    for (uint64_t seed = seed_start; seed < seed_start + num_seeds; seed++) {
        trace_summary_t sums[2];

        for (int run = 0; run < 2; run++) {
            scen_alloc_t as = {0};
            moq_alloc_t alloc = { &as, sa_alloc, sa_realloc, sa_free };
            failures += run_scenario(seed, &alloc, &sums[run], run, max_steps);
            if (as.balance != 0) {
                fprintf(stderr, "FAIL seed=0x%llx run=%d: "
                        "alloc balance=%lld\n",
                        (unsigned long long)seed, run,
                        (long long)as.balance);
                fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx "
                        "MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%d "
                        "./build/tests/test_scenario_streaming_object\n",
                        (unsigned long long)seed, max_steps);
                failures++;
            }
        }

        /* Determinism check. */
        if (sums[0].hash != sums[1].hash ||
            sums[0].count != sums[1].count ||
            sums[0].pre_objects != sums[1].pre_objects ||
            sums[0].pre_chunks != sums[1].pre_chunks ||
            sums[0].pre_payload_hash != sums[1].pre_payload_hash ||
            sums[0].rnd_sent != sums[1].rnd_sent ||
            sums[0].rnd_chunks != sums[1].rnd_chunks ||
            sums[0].rnd_complete != sums[1].rnd_complete ||
            sums[0].rnd_wb != sums[1].rnd_wb ||
            sums[0].rnd_chunk_hash != sums[1].rnd_chunk_hash ||
            sums[0].rnd_violations != sums[1].rnd_violations ||
            sums[0].zc_inputs != sums[1].zc_inputs ||
            sums[0].zc_chunks != sums[1].zc_chunks ||
            sums[0].zc_releases != sums[1].zc_releases) {
            fprintf(stderr, "FAIL seed=0x%llx: determinism mismatch\n",
                    (unsigned long long)seed);
            fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx "
                    "MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%d "
                    "./build/tests/test_scenario_streaming_object\n",
                    (unsigned long long)seed, max_steps);
            failures++;
        }

        /* Prelude oracle: must deliver all 9 objects. */
        if (sums[0].pre_objects != 9) {
            fprintf(stderr, "FAIL seed=0x%llx: prelude objects=%zu "
                    "(expected 9)\n",
                    (unsigned long long)seed, sums[0].pre_objects);
            fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx "
                    "MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%d "
                    "./build/tests/test_scenario_streaming_object\n",
                    (unsigned long long)seed, max_steps);
            failures++;
        }

        /* Zero-copy lifecycle: every wrapped input must be released. */
        if (sums[0].zc_releases != sums[0].zc_inputs) {
            fprintf(stderr, "FAIL seed=0x%llx: zc_releases=%zu != "
                    "zc_inputs=%zu\n",
                    (unsigned long long)seed,
                    sums[0].zc_releases, sums[0].zc_inputs);
            fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx "
                    "MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%d "
                    "./build/tests/test_scenario_streaming_object\n",
                    (unsigned long long)seed, max_steps);
            failures++;
        }

        /* No phantom: complete <= sent. */
        if (sums[0].rnd_complete > sums[0].rnd_sent) {
            fprintf(stderr, "FAIL seed=0x%llx: phantom objects "
                    "sent=%zu complete=%zu\n",
                    (unsigned long long)seed,
                    sums[0].rnd_sent, sums[0].rnd_complete);
            fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx "
                    "MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%d "
                    "./build/tests/test_scenario_streaming_object\n",
                    (unsigned long long)seed, max_steps);
            failures++;
        }

        total_pre += sums[0].pre_objects;
        total_chunks += sums[0].pre_chunks + sums[0].rnd_chunks;
        total_complete += sums[0].rnd_complete;
        total_wb += sums[0].rnd_wb;
        total_rnd_sent += sums[0].rnd_sent;
        total_rnd_complete += sums[0].rnd_complete;
        total_zc_in += sums[0].zc_inputs;
        total_zc_chunks += sums[0].zc_chunks;
        total_zc_rel += sums[0].zc_releases;
    }

    if (failures == 0)
        fprintf(stderr, "PASS: test_scenario_streaming_object "
                "(%llu seeds, prelude=%zu chunks=%zu complete=%zu "
                "wb=%zu random=%zu/%zu "
                "zc_in=%zu zc_chunks=%zu zc_rel=%zu)\n",
                (unsigned long long)num_seeds,
                total_pre, total_chunks, total_complete,
                total_wb, total_rnd_sent, total_rnd_complete,
                total_zc_in, total_zc_chunks, total_zc_rel);
    else
        fprintf(stderr, "FAIL: test_scenario_streaming_object "
                "(%d failures in %llu seeds)\n",
                failures, (unsigned long long)num_seeds);

    return failures;
}
