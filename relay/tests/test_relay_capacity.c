/*
 * The relay capacity model against a peak-tracking allocator: the described
 * ceiling is the relay-state allocation-request ceiling, so continuously
 * tracked peak_live must never exceed it — including the rcbuf HEADER bytes
 * behind retained records and coalesced fetch buffers, which logical payload
 * accounting alone misses. Byte budgets are squeezed tiny in these cases so
 * ceiling slack cannot hide an uncounted term.
 */

#include <moq/relay/relay.h>

#include <moq/relay/capacity.h>
#include <moq/relay/log.h>

#include <moq/rcbuf.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../../tests/unit/test_support.h"

/* Counting allocator with continuous peak tracking. */
typedef struct ca {
    moq_alloc_t vt;
    long        allocs, frees;
    long        live, peak;
} ca_t;

typedef struct test_pin_chunk_shape {
    moq_rcbuf_t *buf;
    uint64_t     len;
} test_pin_chunk_shape_t;

#define TEST_LOG_DEF_MAX_CHUNK_NODES 8192u

static moqr_reset_desc_t
rd_wire(uint64_t code)
{
    moqr_reset_desc_t d;

    if (moqr_reset_desc_wire(MOQ_VERSION_DRAFT_16, code, &d) != MOQR_OK) {
        return moqr_reset_desc_none();
    }
    return d;
}

static void *ca_a(size_t n, void *c)
{
    ca_t *a = c;
    void *p = malloc(n);
    if (p != NULL) {
        a->allocs++;
        a->live += (long)n;
        if (a->live > a->peak) {
            a->peak = a->live;
        }
    }
    return p;
}
static void *ca_r(void *p, size_t o, size_t n, void *c)
{
    ca_t *a = c;
    void *q = realloc(p, n);
    if (q != NULL) {
        a->live += (long)n - (long)o;
        if (a->live > a->peak) {
            a->peak = a->live;
        }
    }
    return q;
}
static void ca_f(void *p, size_t n, void *c)
{
    ca_t *a = c;
    if (p != NULL) {
        a->frees++;
        a->live -= (long)n;
    }
    free(p);
}
static void ca_init(ca_t *a)
{
    memset(a, 0, sizeof(*a));
    a->vt.ctx = a;
    a->vt.alloc = ca_a;
    a->vt.realloc = ca_r;
    a->vt.free = ca_f;
}

/* A tiny-budget core config: every byte-cap slack term squeezed so header
 * bytes cannot hide behind a payload/parked/grant/intern ceiling. */
static void
tiny_cfg(moqr_core_relay_cfg_t *cfg, ca_t *a)
{
    moqr_core_relay_cfg_init_sized(cfg, sizeof(*cfg), &a->vt);
    cfg->max_bindings = 2;
    cfg->max_tracks = 1;
    cfg->max_subs = 2;
    cfg->max_ns_nodes = 2;
    cfg->max_ns_subs = 2;
    cfg->max_intents = 4;
    cfg->name_intern_bytes = 64;
    cfg->max_parked = 1;
    cfg->parked_bytes = 16;
    cfg->max_grants = 1;
    cfg->grant_bytes = 16;
    cfg->max_cancels = 1;
    cfg->log_budget.max_groups = 1;
    cfg->log_budget.max_bytes = 64;
    cfg->log_max_subgroups = 1;
    cfg->log_max_cursors = 1;
    cfg->fetch_pin_bytes = 64;   /* floors to the log budget anyway */
    cfg->linger_us = 1000;
}

static moqr_binding_t
open_pub(moqr_core_t *c, uint64_t cookie)
{
    moqr_binding_t b;
    (void)moqr_core_binding_open(c, cookie, &b);
    return b;
}

static moqr_track_t
open_track(moqr_core_t *c, moqr_binding_t b, const char *nsname)
{
    moq_bytes_t part = { (const uint8_t *)nsname,
                         (uint32_t)strlen(nsname) };
    moqr_ns_t ns = { &part, 1 };
    (void)moqr_core_announce(c, b, ns);
    moq_bytes_t name = { (const uint8_t *)"v", 1 };
    moqr_track_t t;
    (void)moqr_core_publish_open(c, b, ns, name, 900, &t);
    return t;
}

/* Retained zero-byte records: max_records empty payload rcbufs — logical
 * retained bytes stay ~0 while one header per record accumulates. The model
 * must carry HDR * (2*max_records + max_chunk_nodes) for the retained log. */
static int
cap_zero_byte_records(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_relay_cfg_t cfg;
    tiny_cfg(&cfg, &a);
    /* 2049 = 2^11 + 1: the record vector's doubling growth lands its actual
     * cap at 4096 ~= the described 2x ceiling, so vector slack cannot absorb
     * the uncounted headers. */
    cfg.log_max_objects_per_group = 2049;

    moqr_core_capacity_t cap;
    moqr_core_capacity_describe(&cfg, &cap);

    moqr_core_t *c = NULL;
    MOQ_TEST_CHECK(moqr_core_create(&cfg, &c) == MOQR_OK);
    moqr_binding_t p = open_pub(c, 1);
    moqr_track_t t = open_track(c, p, "capA");
    for (uint64_t o = 0; o < 2049; o++) {
        moq_rcbuf_t *pl = NULL, *pr = NULL;
        MOQ_TEST_CHECK(moq_rcbuf_create(&a.vt, NULL, 0, &pl) == MOQ_OK);
        MOQ_TEST_CHECK(moq_rcbuf_create(&a.vt, NULL, 0, &pr) == MOQ_OK);
        moqr_log_append_desc_t d;
        moqr_log_append_desc_init(&d);
        d.group_id = 0;
        d.subgroup_id = 0;
        d.object_id = o;
        d.publisher_priority = 100;
        d.payload = pl;         /* zero bytes, one header */
        d.properties = pr;      /* zero bytes, a second header */
        d.now_us = 1;
        moqr_result_t rc = moqr_core_ingest(c, t, &d);
        MOQ_TEST_CHECK(rc == MOQR_OK);
        if (rc != MOQR_OK) {
            moq_rcbuf_decref(pl);
            moq_rcbuf_decref(pr);
        }
    }
    /* The relay-state allocation-request ceiling must hold at the peak:
     * every record's header was requested through the runtime allocator. */
    MOQ_TEST_CHECK((uint64_t)a.peak <= cap.total_bytes);
    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("cap_zero_byte_records");
    return failures;
}

/* Concurrent zero-byte coalesced fetches: max_fetches live peeks each hold
 * an independently allocated coalesced buffer whose PAYLOAD is ~0 bytes —
 * fetch_pin accounting stays flat while one header per fetch accumulates.
 * The model must carry HDR * max_fetches. */
static int
cap_zero_byte_fetches(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_relay_cfg_t cfg;
    tiny_cfg(&cfg, &a);
    cfg.max_fetches = 16384;
    cfg.max_intents = 16;
    cfg.log_max_objects_per_group = 4;
    cfg.fetch_pin_bytes = 32768;   /* 1 byte x 16384 concurrent pins */

    moqr_core_capacity_t cap;
    moqr_core_capacity_describe(&cfg, &cap);

    moqr_core_t *c = NULL;
    MOQ_TEST_CHECK(moqr_core_create(&cfg, &c) == MOQR_OK);
    moqr_binding_t p = open_pub(c, 1);
    moqr_track_t t = open_track(c, p, "capF");
    /* One chunked COMPLETE record with a single 1-byte chunk: a fetch peek
     * coalesces it into a fresh owned buffer (header + 1). */
    {
        moqr_log_append_desc_t d;
        moqr_log_append_desc_init(&d);
        d.group_id = 0;
        d.subgroup_id = 0;
        d.object_id = 0;
        d.publisher_priority = 100;
        d.obj_state = MOQR_OBJ_OPEN;
        d.declared_len = 1;   /* ~zero: each coalesced pin charges 1 byte
                               * while its header goes unaccounted */
        d.now_us = 1;
        MOQ_TEST_CHECK(moqr_core_ingest(c, t, &d) == MOQR_OK);
        uint8_t one = 0x42;
        moq_rcbuf_t *cb = NULL;
        MOQ_TEST_CHECK(moq_rcbuf_create(&a.vt, &one, 1, &cb) == MOQ_OK);
        MOQ_TEST_CHECK(moqr_core_append_chunk(c, t, 0, 0, 0, cb) == MOQR_OK);
        moq_rcbuf_decref(cb);
        MOQ_TEST_CHECK(moqr_core_complete_record(c, t, 0, 0, 0) == MOQR_OK);
    }
    /* max_fetches concurrent fetches on ONE binding, each holding its
     * zero-byte coalesced peek: fetch_pin stays flat, headers accumulate. */
    static moqr_fetch_t fh[16384];
    moqr_binding_t fb = open_pub(c, 100);
    for (int i = 0; i < 16384; i++) {
        moq_bytes_t part = { (const uint8_t *)"capF", 4 };
        moqr_ns_t ns = { &part, 1 };
        moqr_fetch_req_t r;
        moqr_fetch_req_init(&r);
        r.ns = ns;
        r.name = (moq_bytes_t){ (const uint8_t *)"v", 1 };
        r.start_group = 0;
        r.start_object = 0;
        r.end_group = 0;
        r.end_object = 1;
        r.cookie = (uint64_t)i;
        moqr_fetch_plan_t plan;
        moqr_result_t oc = moqr_core_fetch_open(c, fb, &r, 1000, &fh[i],
                                                &plan);
        if (oc != MOQR_OK) {
            fprintf(stderr, "fetch_open %d rc=%d\n", i, (int)oc);
            failures++;
            break;
        }
        moqr_fetch_item_t it;
        moqr_result_t pc = moqr_core_fetch_peek(c, fh[i], 1000, &it);
        if (pc != MOQR_OK || it.kind != MOQR_FETCH_ITEM_OBJECT) {
            fprintf(stderr, "fetch_peek %d rc=%d kind=%d\n", i, (int)pc,
                    (int)it.kind);
            failures++;
            break;
        }
    }
    MOQ_TEST_CHECK((uint64_t)a.peak <= cap.total_bytes);
    for (int i = 0; i < 16384; i++) {
        (void)moqr_core_fetch_close(c, fh[i]);
    }
    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("cap_zero_byte_fetches");
    return failures;
}

/* Overflow refuses INVAL at every layer — never an understated ceiling.
 * The core arm is the reviewer-reproduced schema-valid config: 2^20 tracks
 * with a 2^20-group / 2^20-object / 1 TiB log wraps the per-track product. */
static int
cap_overflow_refused(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    {
        moqr_core_relay_cfg_t cfg;
        moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a.vt);
        cfg.max_tracks = 1048576;
        cfg.log_budget.max_groups = 1048576;
        cfg.log_budget.max_bytes = (uint64_t)1 << 40;
        cfg.log_max_subgroups = 1048576;
        cfg.log_max_objects_per_group = 1048576;
        cfg.log_max_cursors = 1048576;
        moqr_core_capacity_t cap;
        memset(&cap, 0xEE, sizeof(cap));
        MOQ_TEST_CHECK(moqr_core_capacity_describe(&cfg, &cap) ==
                       MOQR_ERR_INVAL);
        MOQ_TEST_CHECK_EQ_U64(cap.total_bytes, 0);   /* zeroed, not stale */
    }
    {
        moqr_log_cfg_t lc;
        moqr_log_cfg_init_sized(&lc, sizeof(lc), &a.vt);
        lc.budget.max_groups = UINT32_MAX;
        lc.max_subgroups_per_group = UINT32_MAX;
        lc.budget.max_bytes = UINT64_MAX;
        moqr_log_capacity_t cap;
        MOQ_TEST_CHECK(moqr_log_capacity_describe(&lc, &cap) ==
                       MOQR_ERR_INVAL);
        MOQ_TEST_CHECK_EQ_U64(cap.total_bytes, 0);
    }
    /* Isolated derived-count rejections — no byte term wraps first. */
    {
        moqr_log_cfg_t lc;
        moqr_log_cfg_init_sized(&lc, sizeof(lc), &a.vt);
        lc.max_objects_per_group = UINT32_MAX;   /* id_set 2x wraps u32 */
        moqr_log_capacity_t cap;
        MOQ_TEST_CHECK(moqr_log_capacity_describe(&lc, &cap) ==
                       MOQR_ERR_INVAL);
    }
    {
        moqr_core_relay_cfg_t cfg;
        moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a.vt);
        cfg.log_max_subgroups = 1u << 29;   /* gpos stride exceeds u32 */
        moqr_core_capacity_t cap;
        MOQ_TEST_CHECK(moqr_core_capacity_describe(&cfg, &cap) ==
                       MOQR_ERR_INVAL);
        moqr_core_t *c = NULL;
        MOQ_TEST_CHECK(moqr_core_create(&cfg, &c) == MOQR_ERR_INVAL);
        MOQ_TEST_CHECK(c == NULL);
        MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    }
    {
        /* gpos hash cap: (1<<30)+1 groups needs a 2^32 index — refused at
         * BOTH create and describe, before any subscription could enter the
         * runtime's sizing loop. */
        moqr_core_relay_cfg_t cfg;
        moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a.vt);
        cfg.log_budget.max_groups = (1u << 30) + 1;
        moqr_core_capacity_t cap;
        MOQ_TEST_CHECK(moqr_core_capacity_describe(&cfg, &cap) ==
                       MOQR_ERR_INVAL);
        moqr_core_t *c = NULL;
        MOQ_TEST_CHECK(moqr_core_create(&cfg, &c) == MOQR_ERR_INVAL);
        MOQ_TEST_CHECK(c == NULL);
        MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    }
    MOQ_TEST_PASS("cap_overflow_refused");
    return failures;
}

/* Poisoned-prefix parity for the CORE resolver: a prefix-sized cfg with a
 * poisoned tail resolves exactly like the clean prefix, and a live core
 * created from it reports the same limits (create/describe parity). */
static int
cap_core_poisoned_prefix(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_relay_cfg_t poisoned, clean;
    memset(&poisoned, 0xA5, sizeof(poisoned));
    memset(&clean, 0, sizeof(clean));
    uint32_t prefix = (uint32_t)(offsetof(moqr_core_relay_cfg_t,
                                          max_bindings) +
                                 sizeof(poisoned.max_bindings));
    poisoned.struct_size = prefix;
    poisoned.alloc = &a.vt;
    poisoned.trace = NULL;
    poisoned.max_bindings = 3;
    clean.struct_size = prefix;
    clean.alloc = &a.vt;
    clean.max_bindings = 3;
    moqr_core_limits_t lp, lc;
    MOQ_TEST_CHECK(moqr_core_limits_resolve(&poisoned, &lp) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_limits_resolve(&clean, &lc) == MOQR_OK);
    MOQ_TEST_CHECK(memcmp(&lp, &lc, sizeof(lp)) == 0);
    MOQ_TEST_CHECK_EQ_U64(lp.max_bindings, 3);
    moqr_core_t *c = NULL;
    MOQ_TEST_CHECK(moqr_core_create(&poisoned, &c) == MOQR_OK);
    moqr_core_limits_t live;
    moqr_core_get_limits(c, &live);
    /* fetch_pin is floored to the log budget at create; the pure resolver
     * reports the pre-floor value by contract. Compare the rest exactly. */
    live.fetch_pin_bytes = lp.fetch_pin_bytes;
    MOQ_TEST_CHECK(memcmp(&live, &lp, sizeof(live)) == 0);
    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("cap_core_poisoned_prefix");
    return failures;
}

static int
cap_core_log_max_chunk_nodes_knob(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);

    moqr_core_relay_cfg_t def_cfg, small_cfg, zero_cfg;
    tiny_cfg(&def_cfg, &a);
    tiny_cfg(&small_cfg, &a);
    tiny_cfg(&zero_cfg, &a);
    small_cfg.log_max_chunk_nodes = 7;
    zero_cfg.log_max_chunk_nodes = 0;

    moqr_core_capacity_t def_cap, small_cap, zero_cap;
    MOQ_TEST_CHECK(moqr_core_capacity_describe(&def_cfg, &def_cap) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_capacity_describe(&small_cfg, &small_cap) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_capacity_describe(&zero_cfg, &zero_cap) ==
                   MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(zero_cap.total_bytes, def_cap.total_bytes);

    moqr_log_cfg_t def_lc, small_lc;
    moqr_log_cfg_init_sized(&def_lc, sizeof(def_lc), &a.vt);
    moqr_log_cfg_init_sized(&small_lc, sizeof(small_lc), &a.vt);
    def_lc.budget = def_cfg.log_budget;
    def_lc.max_subgroups_per_group = def_cfg.log_max_subgroups;
    def_lc.max_objects_per_group = def_cfg.log_max_objects_per_group;
    def_lc.max_cursors = def_cfg.log_max_cursors;
    small_lc = def_lc;
    small_lc.max_chunk_nodes = small_cfg.log_max_chunk_nodes;

    moqr_log_capacity_t def_logc, small_logc;
    MOQ_TEST_CHECK(moqr_log_capacity_describe(&def_lc, &def_logc) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_log_capacity_describe(&small_lc, &small_logc) ==
                   MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(def_logc.max_chunk_nodes,
                          TEST_LOG_DEF_MAX_CHUNK_NODES);
    MOQ_TEST_CHECK_EQ_U64(small_logc.max_chunk_nodes, 7);

    size_t rc_hdr = 0;
    MOQ_TEST_CHECK(moq_rcbuf_allocation_size(0, &rc_hdr) == MOQ_OK);
    const uint64_t delta_nodes =
        (uint64_t)def_logc.max_chunk_nodes - small_logc.max_chunk_nodes;
    MOQ_TEST_CHECK_EQ_U64(def_logc.header_bytes - small_logc.header_bytes,
                          delta_nodes * rc_hdr);
    const uint64_t expected_delta =
        (uint64_t)def_cfg.max_tracks *
            ((def_logc.structure_bytes - small_logc.structure_bytes) +
             (def_logc.header_bytes - small_logc.header_bytes)) +
        delta_nodes * (uint64_t)def_cfg.max_bindings *
            sizeof(test_pin_chunk_shape_t);
    MOQ_TEST_CHECK_EQ_U64(def_cap.structure_bytes - small_cap.structure_bytes,
                          expected_delta);
    MOQ_TEST_CHECK_EQ_U64(def_cap.payload_bytes, small_cap.payload_bytes);
    MOQ_TEST_CHECK_EQ_U64(def_cap.total_bytes - small_cap.total_bytes,
                          expected_delta);

    ca_t ca_small;
    ca_init(&ca_small);
    moqr_core_relay_cfg_t live_cfg = small_cfg;
    live_cfg.alloc = &ca_small.vt;
    moqr_core_t *core = NULL;
    MOQ_TEST_CHECK(moqr_core_create(&live_cfg, &core) == MOQR_OK);
    MOQ_TEST_CHECK(core != NULL);
    MOQ_TEST_CHECK((uint64_t)ca_small.peak <= small_cap.total_bytes);
    moqr_core_destroy(core);
    MOQ_TEST_CHECK_EQ_INT((int)ca_small.live, 0);

    union {
        moqr_core_relay_cfg_t c;
        unsigned char raw[sizeof(moqr_core_relay_cfg_t)];
    } poisoned, clean_prefix;
    memset(&poisoned, 0xA5, sizeof(poisoned));
    memset(&clean_prefix, 0, sizeof(clean_prefix));
    moqr_core_relay_cfg_init_sized(&poisoned.c,
                                   offsetof(moqr_core_relay_cfg_t,
                                            log_max_chunk_nodes),
                                   &a.vt);
    moqr_core_relay_cfg_init_sized(&clean_prefix.c,
                                   offsetof(moqr_core_relay_cfg_t,
                                            log_max_chunk_nodes),
                                   &a.vt);
    poisoned.c.max_tracks = def_cfg.max_tracks;
    poisoned.c.max_bindings = def_cfg.max_bindings;
    poisoned.c.log_budget = def_cfg.log_budget;
    poisoned.c.log_max_subgroups = def_cfg.log_max_subgroups;
    poisoned.c.log_max_objects_per_group = def_cfg.log_max_objects_per_group;
    poisoned.c.log_max_cursors = def_cfg.log_max_cursors;
    clean_prefix.c.max_tracks = poisoned.c.max_tracks;
    clean_prefix.c.max_bindings = poisoned.c.max_bindings;
    clean_prefix.c.log_budget = poisoned.c.log_budget;
    clean_prefix.c.log_max_subgroups = poisoned.c.log_max_subgroups;
    clean_prefix.c.log_max_objects_per_group =
        poisoned.c.log_max_objects_per_group;
    clean_prefix.c.log_max_cursors = poisoned.c.log_max_cursors;
    moqr_core_capacity_t prefix_cap, clean_prefix_cap;
    MOQ_TEST_CHECK(moqr_core_capacity_describe(&poisoned.c, &prefix_cap) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_capacity_describe(&clean_prefix.c,
                                               &clean_prefix_cap) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(prefix_cap.total_bytes, clean_prefix_cap.total_bytes);

    ca_t op_a;
    ca_init(&op_a);
    moqr_core_relay_cfg_t one_cfg;
    tiny_cfg(&one_cfg, &op_a);
    one_cfg.log_max_chunk_nodes = 1;
    one_cfg.log_budget.max_bytes = 64;
    moqr_core_t *op_core = NULL;
    MOQ_TEST_CHECK(moqr_core_create(&one_cfg, &op_core) == MOQR_OK);
    moqr_binding_t pub = open_pub(op_core, 77);
    moqr_track_t tr = open_track(op_core, pub, "chnk");
    moqr_log_append_desc_t d;
    moqr_log_append_desc_init(&d);
    d.group_id = 0;
    d.subgroup_id = 0;
    d.object_id = 0;
    d.publisher_priority = 100;
    d.obj_state = MOQR_OBJ_OPEN;
    d.declared_len = 20;
    d.now_us = 1;
    MOQ_TEST_CHECK(moqr_core_ingest(op_core, tr, &d) == MOQR_OK);
    uint8_t bytes[10] = { 0 };
    moq_rcbuf_t *chunk = NULL;
    MOQ_TEST_CHECK(moq_rcbuf_create(&op_a.vt, bytes, sizeof(bytes),
                                    &chunk) == MOQ_OK);
    MOQ_TEST_CHECK(moqr_core_append_chunk(op_core, tr, 0, 0, 0, chunk) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_append_chunk(op_core, tr, 0, 0, 0, chunk) ==
                   MOQR_ERR_CAPACITY);
    MOQ_TEST_CHECK(moqr_core_abandon_record(op_core, tr, 0, 0, 0,
                                            rd_wire(0)) == MOQR_OK);
    d.object_id = 1;
    MOQ_TEST_CHECK(moqr_core_ingest(op_core, tr, &d) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_append_chunk(op_core, tr, 0, 0, 1, chunk) ==
                   MOQR_OK);
    moq_rcbuf_decref(chunk);
    moqr_core_destroy(op_core);
    MOQ_TEST_CHECK_EQ_INT((int)op_a.live, 0);

    MOQ_TEST_PASS("cap_core_log_max_chunk_nodes_knob");
    return failures;
}

int
main(void)
{
    int failures = 0;
    failures += cap_zero_byte_records();
    failures += cap_zero_byte_fetches();
    failures += cap_overflow_refused();
    failures += cap_core_poisoned_prefix();
    failures += cap_core_log_max_chunk_nodes_knob();
    if (failures == 0) {
        printf("ALL PASS\n");
    }
    return failures == 0 ? 0 : 1;
}
