/*
 * Deterministic streaming fault injection scenario runner.
 *
 * Exercises OBJECT_CHUNK delivery under transport-level data faults:
 * SPLIT_DATA, DROP_DATA, MUTATE_DATA, REORDER_ACTION, INJECT_RESET,
 * INJECT_STOP, INJECT_CLOSE.
 *
 * Uses SimPair with client_streaming_objects=true. Pumps through
 * run_until_quiescent so faults are actually applied to data streams.
 * Persistent chunk oracle validates begin/end pairing across all drain
 * paths. Each seed runs twice; all counters and hashes must match.
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

/* -- FNV-1a helpers ------------------------------------------------- */

#define FNV_INIT 0xCBF29CE484222325ULL

static void fnv_u64(uint64_t *h, uint64_t v) {
    *h ^= v; *h *= 0x100000001B3ULL;
}

static void fnv_bytes(uint64_t *h, const uint8_t *d, size_t n) {
    for (size_t i = 0; i < n; i++) {
        *h ^= d[i]; *h *= 0x100000001B3ULL;
    }
}

/* -- Run summary ---------------------------------------------------- */

typedef struct {
    /* Trace determinism. */
    uint64_t trace_hash;
    size_t   trace_count;
    /* Chunk oracle. */
    size_t   chunks;
    size_t   complete;
    size_t   violations;
    uint64_t chunk_hash;
    bool     in_object;
    /* Fault counters. */
    size_t   data_inputs;
    size_t   f_drop;
    size_t   f_mutate;
    size_t   f_reorder;
    size_t   f_inject_reset;
    size_t   f_inject_stop;
    size_t   f_inject_close;
    size_t   f_truncate_control;
    size_t   f_truncate_data;
    size_t   reset_terminals;
    /* Session state. */
    size_t   closed;
    /* Prelude. */
    size_t   pre_objects;
    size_t   pre_chunks;
    uint64_t pre_payload_hash;
} run_summary_t;

static void trace_fn(void *ctx, const moq_sim_trace_record_t *r) {
    run_summary_t *s = (run_summary_t *)ctx;
    uint64_t h = s->trace_hash;
    fnv_u64(&h, r->seed);
    fnv_u64(&h, r->step);
    fnv_u64(&h, r->now_us);
    fnv_u64(&h, (uint64_t)r->kind);
    fnv_u64(&h, (uint64_t)r->from);
    fnv_u64(&h, (uint64_t)r->to);
    fnv_u64(&h, (uint64_t)r->action_kind);
    fnv_u64(&h, (uint64_t)r->input_kind);
    fnv_u64(&h, (uint64_t)r->result);
    fnv_u64(&h, r->code);
    fnv_u64(&h, r->bytes.len);
    if (r->bytes.data && r->bytes.len > 0)
        fnv_bytes(&h, r->bytes.data, r->bytes.len);
    s->trace_hash = h;
    s->trace_count++;

    if (r->kind == MOQ_SIM_TRACE_FAULT_DROP) {
        if (r->action_kind == MOQ_ACTION_SEND_DATA) s->f_drop++;
    }
    if (r->kind == MOQ_SIM_TRACE_FAULT_MUTATE) {
        if (r->action_kind == MOQ_ACTION_SEND_DATA) s->f_mutate++;
    }
    if (r->kind == MOQ_SIM_TRACE_FAULT_REORDER)
        s->f_reorder++;
    if (r->kind == MOQ_SIM_TRACE_FAULT_INJECT) {
        if (r->action_kind == MOQ_ACTION_RESET_DATA)
            s->f_inject_reset++;
        else if (r->action_kind == MOQ_ACTION_STOP_DATA)
            s->f_inject_stop++;
        else if (r->action_kind == MOQ_ACTION_CLOSE_SESSION)
            s->f_inject_close++;
    }
    if (r->kind == MOQ_SIM_TRACE_FAULT_TRUNCATE) {
        if (r->action_kind == MOQ_ACTION_SEND_CONTROL)
            s->f_truncate_control++;
        else if (r->action_kind == MOQ_ACTION_SEND_DATA)
            s->f_truncate_data++;
    }
    if (r->kind == MOQ_SIM_TRACE_INPUT &&
        r->input_kind == MOQ_SIM_INPUT_DATA_BYTES)
        s->data_inputs++;
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

/* -- Chunk oracle --------------------------------------------------- */

static void oracle_observe(run_summary_t *s, const moq_object_chunk_event_t *ck)
{
    s->chunks++;
    if (ck->begin) {
        if (s->in_object) s->violations++;
        s->in_object = true;
    }
    if (!s->in_object) s->violations++;
    if (ck->chunk) {
        size_t cl = moq_rcbuf_len(ck->chunk);
        fnv_bytes(&s->chunk_hash, moq_rcbuf_data(ck->chunk), cl);
        fnv_u64(&s->chunk_hash, cl);
    }
    if (ck->end) {
        s->in_object = false;
        s->complete++;
        if (ck->terminal == MOQ_OBJECT_TERMINAL_RESET)
            s->reset_terminals++;
    }
}

static void drain_client(moq_session_t *c, run_summary_t *s)
{
    moq_event_t evts[8];
    size_t ne;
    while ((ne = moq_session_poll_events(c, evts, 8)) > 0)
        for (size_t i = 0; i < ne; i++) {
            if (evts[i].kind == MOQ_EVENT_OBJECT_CHUNK)
                oracle_observe(s, &evts[i].u.object_chunk);
            moq_event_cleanup(&evts[i]);
        }
}

/* -- Shadow model --------------------------------------------------- */

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

/* -- Return-code policy --------------------------------------------- */

static bool rc_expected(moq_result_t rc) {
    return rc == MOQ_OK || rc == MOQ_ERR_WOULD_BLOCK ||
           rc == MOQ_ERR_CLOSED || rc == MOQ_ERR_WRONG_STATE ||
           rc == MOQ_ERR_STALE_HANDLE || rc == MOQ_ERR_REQUEST_BLOCKED ||
           rc == MOQ_ERR_PROTO || rc == MOQ_ERR_INVAL ||
           rc == MOQ_ERR_NOMEM || rc == MOQ_ERR_BUFFER;
}

static int pump_checked(moq_simpair_t *sp, size_t max, run_summary_t *s)
{
    moq_result_t rc = moq_simpair_run_until_quiescent(sp, max, NULL);
    if (rc != MOQ_OK && rc != MOQ_ERR_WOULD_BLOCK &&
        rc != MOQ_ERR_CLOSED && rc != MOQ_ERR_PROTO)
        return -1;
    drain_client(moq_simpair_client(sp), s);
    return 0;
}

/* -- Prelude (no faults) -------------------------------------------- */

static int run_prelude(moq_simpair_t *sp, run_summary_t *s,
                        const moq_alloc_t *alloc)
{
    moq_session_t *c = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);
    uint64_t now = moq_simpair_now_us(sp);

    /* Subscribe. */
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
    moq_namespace_t ns = { ns_parts, 1 };
    moq_subscribe_cfg_t sub_cfg;
    moq_subscribe_cfg_init(&sub_cfg);
    sub_cfg.track_namespace = ns;
    sub_cfg.track_name = MOQ_BYTES_LITERAL("pre");
    moq_subscription_t csub;
    if (moq_session_subscribe(c, &sub_cfg, now, &csub) != MOQ_OK) return -1;
    if (pump_checked(sp, 8, s) < 0) return -1;

    /* Accept. */
    moq_event_t ev;
    if (moq_session_poll_events(sv, &ev, 1) != 1) return -1;
    if (ev.kind != MOQ_EVENT_SUBSCRIBE_REQUEST) { moq_event_cleanup(&ev); return -1; }
    moq_subscription_t ssub = ev.u.subscribe_request.sub;
    moq_event_cleanup(&ev);
    moq_accept_subscribe_cfg_t acc;
    moq_accept_subscribe_cfg_init(&acc);
    if (moq_session_accept_subscribe(sv, ssub, &acc, now) != MOQ_OK) return -1;
    if (pump_checked(sp, 8, s) < 0) return -1;

    /* Open subgroup + write multi-chunk object (100 bytes). */
    moq_subgroup_cfg_t sg_cfg;
    moq_subgroup_cfg_init(&sg_cfg);
    sg_cfg.group_id = 0; sg_cfg.subgroup_id = 0;
    moq_subgroup_handle_t sg;
    if (moq_session_open_subgroup(sv, ssub, &sg_cfg, now, &sg) != MOQ_OK) return -1;

    uint8_t payload[100];
    for (int i = 0; i < 100; i++) payload[i] = (uint8_t)(i + 1);
    moq_rcbuf_t *p = NULL;
    if (moq_rcbuf_create(alloc, payload, sizeof(payload), &p) != MOQ_OK) return -1;
    if (moq_session_write_object(sv, sg, 0, p, now) != MOQ_OK) {
        moq_rcbuf_decref(p); return -1;
    }
    moq_rcbuf_decref(p);
    if (moq_session_close_subgroup(sv, sg, now) != MOQ_OK) return -1;

    /* Pump and reconstruct payload from OBJECT_CHUNK events. */
    {
        moq_result_t prc = moq_simpair_run_until_quiescent(sp, 16, NULL);
        if (prc != MOQ_OK && prc != MOQ_ERR_WOULD_BLOCK) return -1;

        uint8_t reasm[100];
        size_t rlen = 0;
        bool got_begin = false, got_end = false;
        moq_event_t ev;
        while (moq_session_poll_events(c, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_OBJECT_CHUNK) {
                oracle_observe(s, &ev.u.object_chunk);
                if (ev.u.object_chunk.begin) got_begin = true;
                if (ev.u.object_chunk.chunk) {
                    size_t cl = moq_rcbuf_len(ev.u.object_chunk.chunk);
                    if (rlen + cl <= sizeof(reasm))
                        memcpy(reasm + rlen,
                            moq_rcbuf_data(ev.u.object_chunk.chunk), cl);
                    rlen += cl;
                }
                if (ev.u.object_chunk.end) got_end = true;
            }
            moq_event_cleanup(&ev);
        }
        if (!got_begin || !got_end) return -1;
        if (rlen != sizeof(payload)) return -1;
        if (memcmp(reasm, payload, sizeof(payload)) != 0) return -1;

        /* Hash the expected payload for determinism comparison. */
        fnv_bytes(&s->pre_payload_hash, payload, sizeof(payload));
        s->pre_chunks = s->chunks;
        /* Exactly one prelude object must have completed. */
        if (s->complete != 1) return -1;
        s->pre_objects = 1;
    }

    return 0;
}

/* -- Operations ----------------------------------------------------- */

typedef enum {
    OP_PUMP, OP_SUBSCRIBE, OP_ACCEPT,
    OP_OPEN_SG, OP_WRITE, OP_CLOSE_SG, OP_RESET_SG,
    OP_DRAIN, OP_DRAIN_SERVER, OP_ADVANCE_TIME,
    OP_COUNT,
} scenario_op_t;

static int execute_step(moq_simpair_t *sp, shadow_state_t *st,
    rng_t *rng, run_summary_t *s, const moq_alloc_t *alloc)
{
    if (st->closed) return 0;
    moq_session_t *c = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);
    uint64_t now = moq_simpair_now_us(sp);

    scenario_op_t op = (scenario_op_t)(rng_next(rng) % OP_COUNT);

    switch (op) {
    case OP_PUMP:
        if (pump_checked(sp, 4, s) < 0) return -1;
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
        moq_result_t rc = moq_session_subscribe(c, &cfg, now, &h);
        if (rc == MOQ_OK) {
            if (pump_checked(sp, 4, s) < 0) return -1;
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
        } else if (!rc_expected(rc)) return -1;
        break;
    }

    case OP_ACCEPT: {
        int idx = -1;
        for (int i = 0; i < MAX_SUBS; i++)
            if (st->subs[i].active && !st->subs[i].accepted) { idx = i; break; }
        if (idx < 0) { pump_checked(sp, 4, s); break; }
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_result_t rc = moq_session_accept_subscribe(sv,
            st->subs[idx].server_handle, &acc, now);
        if (rc == MOQ_OK) {
            st->subs[idx].accepted = true;
            if (pump_checked(sp, 4, s) < 0) return -1;
        } else if (!rc_expected(rc)) return -1;
        break;
    }

    case OP_OPEN_SG: {
        int sub_idx = -1;
        for (int i = 0; i < MAX_SUBS; i++)
            if (st->subs[i].active && st->subs[i].accepted) { sub_idx = i; break; }
        if (sub_idx < 0) { pump_checked(sp, 4, s); break; }
        int sg_slot = -1;
        for (int i = 0; i < MAX_SGS; i++)
            if (!st->sgs[i].active) { sg_slot = i; break; }
        if (sg_slot < 0) { pump_checked(sp, 4, s); break; }
        moq_subgroup_cfg_t cfg;
        moq_subgroup_cfg_init(&cfg);
        cfg.group_id = st->next_group; cfg.subgroup_id = 0;
        moq_subgroup_handle_t h;
        moq_result_t rc = moq_session_open_subgroup(sv,
            st->subs[sub_idx].server_handle, &cfg, now, &h);
        if (rc == MOQ_OK) {
            st->sgs[sg_slot].active = true;
            st->sgs[sg_slot].handle = h;
            st->sgs[sg_slot].next_object_id = 0;
            st->next_group++;
        } else if (!rc_expected(rc)) return -1;
        break;
    }

    case OP_WRITE: {
        int sg_idx = -1;
        for (int i = 0; i < MAX_SGS; i++)
            if (st->sgs[i].active) { sg_idx = i; break; }
        if (sg_idx < 0) { pump_checked(sp, 4, s); break; }
        size_t plen = rng_next(rng) % 64;
        if (rng_next(rng) % 10 == 0) plen = 0;
        uint8_t pdata[64];
        for (size_t j = 0; j < plen; j++) pdata[j] = (uint8_t)(rng_next(rng) & 0xFF);
        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(alloc, pdata, plen, &p);
        if (!p) break;
        moq_result_t rc = moq_session_write_object(sv,
            st->sgs[sg_idx].handle, st->sgs[sg_idx].next_object_id,
            p, now);
        moq_rcbuf_decref(p);
        if (rc == MOQ_OK) st->sgs[sg_idx].next_object_id++;
        else if (!rc_expected(rc)) return -1;
        break;
    }

    case OP_CLOSE_SG: {
        int sg_idx = -1;
        for (int i = 0; i < MAX_SGS; i++)
            if (st->sgs[i].active) { sg_idx = i; break; }
        if (sg_idx < 0) { pump_checked(sp, 4, s); break; }
        moq_result_t rc = moq_session_close_subgroup(sv,
            st->sgs[sg_idx].handle, now);
        if (rc == MOQ_OK) st->sgs[sg_idx].active = false;
        else if (!rc_expected(rc)) return -1;
        break;
    }

    case OP_RESET_SG: {
        int sg_idx = -1;
        for (int i = 0; i < MAX_SGS; i++)
            if (st->sgs[i].active) { sg_idx = i; break; }
        if (sg_idx < 0) { pump_checked(sp, 4, s); break; }
        moq_result_t rc = moq_session_reset_subgroup(sv,
            st->sgs[sg_idx].handle, 0x1, now);
        if (rc == MOQ_OK) st->sgs[sg_idx].active = false;
        else if (!rc_expected(rc)) return -1;
        break;
    }

    case OP_DRAIN:
        drain_client(c, s);
        break;

    case OP_DRAIN_SERVER: {
        moq_event_t evts[8];
        size_t ne = moq_session_poll_events(sv, evts, 8);
        for (size_t i = 0; i < ne; i++) moq_event_cleanup(&evts[i]);
        break;
    }

    case OP_ADVANCE_TIME: {
        uint64_t delta = (rng_next(rng) % 100) + 1;
        moq_result_t rc = moq_simpair_advance_to(sp, now + delta);
        if (rc != MOQ_OK && rc != MOQ_ERR_CLOSED) return -1;
        break;
    }

    default:
        pump_checked(sp, 4, s);
        break;
    }

    if (moq_session_state(c) == MOQ_SESS_CLOSED ||
        moq_session_state(sv) == MOQ_SESS_CLOSED) {
        st->closed = true;
        s->closed++;
        s->in_object = false;
    }

    return 0;
}

/* -- Run one scenario ----------------------------------------------- */

static int run_scenario(uint64_t seed, uint64_t per_mille,
                         const moq_alloc_t *alloc,
                         run_summary_t *summary, int run_id,
                         int max_steps)
{
    memset(summary, 0, sizeof(*summary));
    summary->trace_hash = FNV_INIT;
    summary->chunk_hash = FNV_INIT;
    summary->pre_payload_hash = FNV_INIT;

    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = alloc;
    cfg.seed = seed;
    cfg.initial_now_us = 1000;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 32;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 32;
    cfg.trace_fn = trace_fn;
    cfg.trace_ctx = summary;
    cfg.client_streaming_objects = true;
    cfg.fault_per_mille = (uint32_t)per_mille;
    cfg.fault_flags = MOQ_SIM_FAULT_SPLIT_DATA |
                      MOQ_SIM_FAULT_DROP_DATA |
                      MOQ_SIM_FAULT_MUTATE_DATA |
                      MOQ_SIM_FAULT_REORDER_ACTION |
                      MOQ_SIM_FAULT_INJECT_RESET |
                      MOQ_SIM_FAULT_INJECT_STOP |
                      MOQ_SIM_FAULT_INJECT_CLOSE |
                      MOQ_SIM_FAULT_TRUNCATE_CONTROL |
                      MOQ_SIM_FAULT_TRUNCATE_DATA;

    moq_simpair_t *sp = NULL;
    if (moq_simpair_create(&cfg, &sp) < 0) return -1;
    moq_simpair_start(sp);
    {
        moq_result_t rc = moq_simpair_run_until_quiescent(sp, 8, NULL);
        if (rc != MOQ_OK && rc != MOQ_ERR_WOULD_BLOCK) {
            moq_simpair_destroy(sp); return -1;
        }
    }
    { moq_event_t ev;
      if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev);
      if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev);
    }

    /* Prelude without faults. */
    if (run_prelude(sp, summary, alloc) < 0) {
        fprintf(stderr, "FAIL seed=0x%llx run=%d: prelude failed\n",
                (unsigned long long)seed, run_id);
        fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx "
                "MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%d "
                "MOQ_SCENARIO_FAULT_PERMILLE=%llu "
                "./build/tests/test_scenario_streaming_faults\n",
                (unsigned long long)seed, max_steps,
                (unsigned long long)per_mille);
        /* Drain owned resources. */
        drain_client(moq_simpair_client(sp), summary);
        { moq_event_t d[16]; size_t ne;
          while ((ne = moq_session_poll_events(moq_simpair_server(sp), d, 16)) > 0)
              for (size_t i = 0; i < ne; i++) moq_event_cleanup(&d[i]);
          moq_action_t a[16]; size_t na;
          while ((na = moq_session_poll_actions(moq_simpair_server(sp), a, 16)) > 0)
              for (size_t i = 0; i < na; i++) moq_action_cleanup(&a[i]);
          while ((na = moq_session_poll_actions(moq_simpair_client(sp), a, 16)) > 0)
              for (size_t i = 0; i < na; i++) moq_action_cleanup(&a[i]);
        }
        moq_simpair_destroy(sp);
        return 1;
    }

    /* Enable faults for random phase. */
    moq_simpair_enable_faults(sp);

    shadow_state_t st;
    memset(&st, 0, sizeof(st));
    rng_t rng = { seed };
    int failures = 0;

    for (int step = 0; step < max_steps; step++) {
        int src = execute_step(sp, &st, &rng, summary, alloc);
        if (src < 0) {
            fprintf(stderr, "FAIL seed=0x%llx run=%d step=%d: "
                    "unexpected error\n",
                    (unsigned long long)seed, run_id, step);
            fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx "
                    "MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%d "
                    "MOQ_SCENARIO_FAULT_PERMILLE=%llu "
                    "./build/tests/test_scenario_streaming_faults\n",
                    (unsigned long long)seed, max_steps,
                    (unsigned long long)per_mille);
            failures++;
            st.closed = true;
            break;
        }
        if (st.closed) break;
    }

    /* Final pump + drain. */
    if (!st.closed) {
        for (int f = 0; f < 8; f++) {
            moq_result_t prc = moq_simpair_run_until_quiescent(sp, 8, NULL);
            if (prc != MOQ_OK && prc != MOQ_ERR_WOULD_BLOCK) break;
            drain_client(moq_simpair_client(sp), summary);
        }
    }

    /* Drain all remaining owned resources through the oracle. */
    drain_client(moq_simpair_client(sp), summary);
    { moq_event_t d[16]; size_t ne;
      while ((ne = moq_session_poll_events(moq_simpair_server(sp), d, 16)) > 0)
          for (size_t i = 0; i < ne; i++) moq_event_cleanup(&d[i]);
      moq_action_t a[16]; size_t na;
      while ((na = moq_session_poll_actions(moq_simpair_server(sp), a, 16)) > 0)
          for (size_t i = 0; i < na; i++) moq_action_cleanup(&a[i]);
      while ((na = moq_session_poll_actions(moq_simpair_client(sp), a, 16)) > 0)
          for (size_t i = 0; i < na; i++) moq_action_cleanup(&a[i]);
    }

    /* Dangling in_object at clean shutdown is a violation —
     * unless session closed from injected fault. */
    if (summary->in_object && !st.closed)
        summary->violations++;
    summary->in_object = false;

    if (summary->violations > 0) {
        fprintf(stderr, "FAIL seed=0x%llx run=%d: %zu chunk oracle "
                "violations\n",
                (unsigned long long)seed, run_id, summary->violations);
        fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx "
                "MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%d "
                "MOQ_SCENARIO_FAULT_PERMILLE=%llu "
                "./build/tests/test_scenario_streaming_faults\n",
                (unsigned long long)seed, max_steps,
                (unsigned long long)per_mille);
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
    const char *env_fault = getenv("MOQ_SCENARIO_FAULT_PERMILLE");

    uint64_t num_seeds = env_seeds ? (uint64_t)strtoull(env_seeds, NULL, 0) : 100;
    uint64_t seed_start = env_start ? (uint64_t)strtoull(env_start, NULL, 0) : 0;
    int max_steps = env_steps ? atoi(env_steps) : 50;
    uint64_t per_mille = env_fault ? (uint64_t)strtoull(env_fault, NULL, 0) : 50;
    if (per_mille > 1000) per_mille = 1000;

    size_t t_chunks = 0, t_complete = 0, t_violations = 0, t_closed = 0, t_rst_term = 0;
    size_t t_data_inputs = 0, t_drop = 0, t_mutate = 0, t_reorder = 0;
    size_t t_inj_rst = 0, t_inj_stp = 0, t_inj_cls = 0;
    size_t t_trunc_ctl = 0, t_trunc_data = 0;
    size_t t_pre = 0;

    for (uint64_t seed = seed_start; seed < seed_start + num_seeds; seed++) {
        run_summary_t sums[2];

        for (int run = 0; run < 2; run++) {
            scen_alloc_t as = {0};
            moq_alloc_t alloc = { &as, sa_alloc, sa_realloc, sa_free };
            failures += run_scenario(seed, per_mille, &alloc,
                                      &sums[run], run, max_steps);
            if (as.balance != 0) {
                fprintf(stderr, "FAIL seed=0x%llx run=%d: "
                        "alloc balance=%lld\n",
                        (unsigned long long)seed, run,
                        (long long)as.balance);
                fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx "
                        "MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%d "
                        "MOQ_SCENARIO_FAULT_PERMILLE=%llu "
                        "./build/tests/test_scenario_streaming_faults\n",
                        (unsigned long long)seed, max_steps,
                        (unsigned long long)per_mille);
                failures++;
            }
        }

        /* Determinism. */
        if (sums[0].trace_hash != sums[1].trace_hash ||
            sums[0].trace_count != sums[1].trace_count ||
            sums[0].chunks != sums[1].chunks ||
            sums[0].complete != sums[1].complete ||
            sums[0].violations != sums[1].violations ||
            sums[0].chunk_hash != sums[1].chunk_hash ||
            sums[0].reset_terminals != sums[1].reset_terminals ||
            sums[0].closed != sums[1].closed ||
            sums[0].data_inputs != sums[1].data_inputs ||
            sums[0].f_drop != sums[1].f_drop ||
            sums[0].f_mutate != sums[1].f_mutate ||
            sums[0].f_reorder != sums[1].f_reorder ||
            sums[0].f_inject_reset != sums[1].f_inject_reset ||
            sums[0].f_inject_stop != sums[1].f_inject_stop ||
            sums[0].f_inject_close != sums[1].f_inject_close ||
            sums[0].f_truncate_control != sums[1].f_truncate_control ||
            sums[0].f_truncate_data != sums[1].f_truncate_data ||
            sums[0].pre_objects != sums[1].pre_objects ||
            sums[0].pre_chunks != sums[1].pre_chunks ||
            sums[0].pre_payload_hash != sums[1].pre_payload_hash) {
            fprintf(stderr, "FAIL seed=0x%llx: determinism mismatch\n",
                    (unsigned long long)seed);
            fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx "
                    "MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%d "
                    "MOQ_SCENARIO_FAULT_PERMILLE=%llu "
                    "./build/tests/test_scenario_streaming_faults\n",
                    (unsigned long long)seed, max_steps,
                    (unsigned long long)per_mille);
            failures++;
        }

        t_chunks += sums[0].chunks;
        t_complete += sums[0].complete;
        t_violations += sums[0].violations;
        t_closed += sums[0].closed;
        t_rst_term += sums[0].reset_terminals;
        t_data_inputs += sums[0].data_inputs;
        t_drop += sums[0].f_drop;
        t_mutate += sums[0].f_mutate;
        t_reorder += sums[0].f_reorder;
        t_inj_rst += sums[0].f_inject_reset;
        t_inj_stp += sums[0].f_inject_stop;
        t_inj_cls += sums[0].f_inject_close;
        t_trunc_ctl += sums[0].f_truncate_control;
        t_trunc_data += sums[0].f_truncate_data;
        t_pre += sums[0].pre_objects;
    }

    if (t_violations > 0) {
        fprintf(stderr, "FAIL: %zu total chunk oracle violations across "
                "%llu seeds\n", t_violations, (unsigned long long)num_seeds);
        failures++;
    }

    if (t_pre != num_seeds) {
        fprintf(stderr, "FAIL: prelude=%zu expected=%llu\n",
                t_pre, (unsigned long long)num_seeds);
        failures++;
    }

    if (per_mille > 0 && num_seeds >= 10 &&
        t_drop + t_mutate + t_reorder +
        t_inj_rst + t_inj_stp + t_inj_cls +
        t_trunc_ctl + t_trunc_data == 0) {
        fprintf(stderr, "FAIL: no data faults across %llu seeds\n",
                (unsigned long long)num_seeds);
        failures++;
    }
    if (per_mille > 0 && num_seeds >= 10 &&
        t_trunc_ctl + t_trunc_data == 0) {
        fprintf(stderr, "FAIL: no truncation faults across %llu seeds\n",
                (unsigned long long)num_seeds);
        failures++;
    }

    /* rst_term depends on inject timing vs. parser state. SimPair's
     * inject phase fires between pump rounds, so small objects usually
     * complete before inject lands. The unit tests verify the terminal
     * path; the scenario validates no oracle corruption either way. */

    if (failures == 0)
        fprintf(stderr, "PASS: test_scenario_streaming_faults "
                "(%llu seeds, prelude=%zu chunks=%zu complete=%zu "
                "closed=%zu violations=%zu rst_term=%zu "
                "data_in=%zu drop=%zu mutate=%zu reord=%zu "
                "inj_rst=%zu inj_stp=%zu inj_cls=%zu "
                "trunc_ctl=%zu trunc_data=%zu)\n",
                (unsigned long long)num_seeds,
                t_pre, t_chunks, t_complete, t_closed, t_violations,
                t_rst_term,
                t_data_inputs, t_drop, t_mutate, t_reorder,
                t_inj_rst, t_inj_stp, t_inj_cls,
                t_trunc_ctl, t_trunc_data);
    else
        fprintf(stderr, "FAIL: test_scenario_streaming_faults "
                "(%d failures in %llu seeds)\n",
                failures, (unsigned long long)num_seeds);

    return failures;
}
