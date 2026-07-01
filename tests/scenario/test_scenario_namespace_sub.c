/*
 * Deterministic SUBSCRIBE_NAMESPACE lifecycle scenario runner.
 *
 * Exercises the full bidi-stream namespace subscription flow
 * through SimPair using public session APIs:
 *   subscribe_namespace → accept/reject → send_namespace /
 *   send_namespace_done → cancel → stream close/reset.
 *
 * Deterministic prelude guarantees coverage of:
 *   - subscribe_namespace → NS_SUB_REQUEST on publisher
 *   - accept → NS_SUB_OK on subscriber
 *   - send_namespace → NAMESPACE_FOUND on subscriber (routed via SimPair)
 *   - send_namespace_done → NAMESPACE_GONE on subscriber
 *   - cancel → CLOSE_BIDI_STREAM → publisher sees reset
 *
 * Random phase interleaves: subscribe, accept/reject,
 * send_namespace/send_namespace_done, cancel, drain, pump.
 *
 * Each seed runs twice; trace hash + all counters must match.
 */

#include <moq/sim.h>
#include <moq/codec.h>
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

/* -- Trace hash ----------------------------------------------------- */

typedef struct {
    uint64_t hash;
    size_t   count;
    size_t   ns_sub_sent;
    size_t   ns_sub_request;
    size_t   ns_sub_ok;
    size_t   ns_sub_error;
    size_t   ns_found;
    size_t   ns_gone;
    size_t   cancel_sent;
    size_t   ns_injected;
    size_t   closed;
} trace_summary_t;

static void trace_hash_fn(void *ctx, const moq_sim_trace_record_t *r) {
    trace_summary_t *s = (trace_summary_t *)ctx;
    uint64_t h = s->hash;
    h ^= r->seed;   h *= 0x100000001B3ULL;
    h ^= r->step;   h *= 0x100000001B3ULL;
    h ^= r->now_us; h *= 0x100000001B3ULL;
    h ^= (uint64_t)r->kind;        h *= 0x100000001B3ULL;
    h ^= (uint64_t)r->from;        h *= 0x100000001B3ULL;
    h ^= (uint64_t)r->to;          h *= 0x100000001B3ULL;
    h ^= (uint64_t)r->action_kind; h *= 0x100000001B3ULL;
    h ^= (uint64_t)r->input_kind;  h *= 0x100000001B3ULL;
    h ^= (uint64_t)r->result;      h *= 0x100000001B3ULL;
    h ^= r->code;                  h *= 0x100000001B3ULL;
    h ^= r->bytes.len;             h *= 0x100000001B3ULL;
    if (r->bytes.data && r->bytes.len > 0)
        for (size_t i = 0; i < r->bytes.len; i++) {
            h ^= r->bytes.data[i]; h *= 0x100000001B3ULL;
        }
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

/* -- Shadow model --------------------------------------------------- */

#define MAX_NS_SUBS 4

typedef struct {
    bool active;
    bool pending;
    bool accepted;
    bool has_publisher_handle;
    moq_ns_sub_handle_t client_handle;
    moq_ns_sub_handle_t publisher_handle;
    moq_stream_ref_t    client_ref;
} shadow_ns_sub_t;

typedef struct {
    shadow_ns_sub_t subs[MAX_NS_SUBS];
    bool closed;
    uint64_t next_client_ref;
} shadow_state_t;

/* -- Operations ----------------------------------------------------- */

typedef enum {
    OP_PUMP,
    OP_SUBSCRIBE,
    OP_ACCEPT,
    OP_REJECT,
    OP_CANCEL,
    OP_INJECT_NS,
    OP_DRAIN_CLIENT,
    OP_DRAIN_SERVER,
    OP_COUNT,
} scenario_op_t;

static const char *op_name(scenario_op_t op) {
    static const char *n[] = {
        "PUMP","SUB","ACC","REJ","CANCEL","INJ_NS",
        "DRAIN_C","DRAIN_S",
    };
    return op < OP_COUNT ? n[op] : "?";
}

#define MAX_LOG 64
typedef struct { scenario_op_t op; uint64_t p; } log_entry_t;
typedef struct { log_entry_t e[MAX_LOG]; size_t n; } op_log_t;

static void log_op(op_log_t *l, scenario_op_t op, uint64_t p) {
    if (l->n < MAX_LOG) { l->e[l->n].op = op; l->e[l->n].p = p; l->n++; }
}

static void dump_log(uint64_t seed, int run, const op_log_t *l) __attribute__((unused));
static void dump_log(uint64_t seed, int run, const op_log_t *l) {
    fprintf(stderr, "  seed=0x%llx run=%d ops:\n",
            (unsigned long long)seed, run);
    for (size_t i = 0; i < l->n; i++)
        fprintf(stderr, "    %zu: %s p=%llu\n", i,
                op_name(l->e[i].op), (unsigned long long)l->e[i].p);
}

/* -- Drain helpers -------------------------------------------------- */

static void drain_client(moq_session_t *c, trace_summary_t *ts) {
    moq_event_t evts[8]; size_t ne;
    while ((ne = moq_session_poll_events(c, evts, 8)) > 0)
        for (size_t i = 0; i < ne; i++) {
            if (evts[i].kind == MOQ_EVENT_NS_SUB_OK)
                ts->ns_sub_ok++;
            else if (evts[i].kind == MOQ_EVENT_NS_SUB_ERROR)
                ts->ns_sub_error++;
            else if (evts[i].kind == MOQ_EVENT_NAMESPACE_FOUND)
                ts->ns_found++;
            else if (evts[i].kind == MOQ_EVENT_NAMESPACE_GONE)
                ts->ns_gone++;
            moq_event_cleanup(&evts[i]);
        }
}

static void drain_server(moq_session_t *sv, trace_summary_t *ts,
                           shadow_state_t *st) {
    moq_event_t evts[8]; size_t ne;
    while ((ne = moq_session_poll_events(sv, evts, 8)) > 0)
        for (size_t i = 0; i < ne; i++) {
            if (evts[i].kind == MOQ_EVENT_NS_SUB_REQUEST) {
                ts->ns_sub_request++;
                for (int j = 0; j < MAX_NS_SUBS; j++) {
                    if (st->subs[j].active && st->subs[j].pending &&
                        !st->subs[j].has_publisher_handle) {
                        st->subs[j].publisher_handle =
                            evts[i].u.ns_sub_request.handle;
                        st->subs[j].has_publisher_handle = true;
                        break;
                    }
                }
            }
            moq_event_cleanup(&evts[i]);
        }
}

/* -- Execute step --------------------------------------------------- */

static int execute_step(moq_simpair_t *sp, shadow_state_t *st,
                         rng_t *rng, op_log_t *log,
                         trace_summary_t *ts)
{
    if (st->closed) return 0;

    scenario_op_t op = (scenario_op_t)(rng_next(rng) % OP_COUNT);
    moq_session_t *c = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);

    switch (op) {
    case OP_PUMP: {
        log_op(log, op, 0);
        moq_result_t prc = moq_simpair_run_until_quiescent(sp, 4, NULL);
        if (prc != MOQ_OK && prc != MOQ_ERR_WOULD_BLOCK) return -1;
        drain_server(sv, ts, st);
        break;
    }

    case OP_SUBSCRIBE: {
        int slot = -1;
        for (int i = 0; i < MAX_NS_SUBS; i++)
            if (!st->subs[i].active) { slot = i; break; }
        if (slot < 0) { log_op(log, OP_PUMP, 0); break; }

        moq_subscribe_namespace_cfg_t cfg;
        moq_subscribe_namespace_cfg_init(&cfg);
        char name[16];
        snprintf(name, sizeof(name), "ns%llu",
                 (unsigned long long)(rng_next(rng) % 100));
        moq_bytes_t parts[] = { moq_bytes_cstr(name) };
        cfg.track_namespace_prefix.parts = parts;
        cfg.track_namespace_prefix.count = 1;

        moq_ns_sub_handle_t h;
        moq_result_t rc = moq_session_subscribe_namespace(c, &cfg,
            moq_simpair_now_us(sp), &h);
        log_op(log, op, rc >= 0 ? 1 : 0);

        if (rc == MOQ_OK) {
            st->subs[slot].active = true;
            st->subs[slot].pending = true;
            st->subs[slot].accepted = false;
            st->subs[slot].has_publisher_handle = false;
            st->subs[slot].client_handle = h;
            st->subs[slot].client_ref =
                moq_stream_ref_from_u64(st->next_client_ref++);
            ts->ns_sub_sent++;

            moq_result_t prc = moq_simpair_run_until_quiescent(sp, 4, NULL);
            if (prc != MOQ_OK && prc != MOQ_ERR_WOULD_BLOCK) return -1;
            drain_server(sv, ts, st);
        }
        break;
    }

    case OP_ACCEPT: {
        int idx = -1;
        for (int i = 0; i < MAX_NS_SUBS; i++)
            if (st->subs[i].active && st->subs[i].pending &&
                st->subs[i].has_publisher_handle) { idx = i; break; }
        if (idx < 0) { log_op(log, OP_PUMP, 0); break; }

        moq_accept_ns_sub_cfg_t acc;
        moq_accept_ns_sub_cfg_init(&acc);
        moq_result_t rc = moq_session_accept_ns_sub(sv,
            st->subs[idx].publisher_handle, &acc,
            moq_simpair_now_us(sp));
        log_op(log, op, (uint64_t)idx);

        if (rc == MOQ_OK) {
            st->subs[idx].pending = false;
            st->subs[idx].accepted = true;
            moq_result_t prc = moq_simpair_run_until_quiescent(sp, 4, NULL);
            if (prc != MOQ_OK && prc != MOQ_ERR_WOULD_BLOCK) return -1;
            drain_client(c, ts);
        }
        break;
    }

    case OP_REJECT: {
        int idx = -1;
        for (int i = 0; i < MAX_NS_SUBS; i++)
            if (st->subs[i].active && st->subs[i].pending &&
                st->subs[i].has_publisher_handle) { idx = i; break; }
        if (idx < 0) { log_op(log, OP_PUMP, 0); break; }

        moq_reject_ns_sub_cfg_t rej;
        moq_reject_ns_sub_cfg_init(&rej);
        rej.error_code = 0x10;
        rej.reason = MOQ_BYTES_LITERAL("no");
        moq_result_t rc = moq_session_reject_ns_sub(sv,
            st->subs[idx].publisher_handle, &rej,
            moq_simpair_now_us(sp));
        log_op(log, op, (uint64_t)idx);

        if (rc == MOQ_OK) {
            st->subs[idx].active = false;
            moq_result_t prc = moq_simpair_run_until_quiescent(sp, 4, NULL);
            if (prc != MOQ_OK && prc != MOQ_ERR_WOULD_BLOCK) return -1;
            drain_client(c, ts);
        }
        break;
    }

    case OP_CANCEL: {
        int idx = -1;
        for (int i = 0; i < MAX_NS_SUBS; i++)
            if (st->subs[i].active &&
                (st->subs[i].accepted || st->subs[i].pending)) {
                idx = i; break;
            }
        if (idx < 0) { log_op(log, OP_PUMP, 0); break; }

        moq_result_t rc = moq_session_cancel_namespace_sub(c,
            st->subs[idx].client_handle, moq_simpair_now_us(sp));
        log_op(log, op, (uint64_t)idx);

        if (rc == MOQ_OK) {
            st->subs[idx].active = false;
            ts->cancel_sent++;
            moq_result_t prc = moq_simpair_run_until_quiescent(sp, 4, NULL);
            if (prc != MOQ_OK && prc != MOQ_ERR_WOULD_BLOCK &&
                prc != MOQ_ERR_CLOSED) return -1;
        }
        break;
    }

    case OP_INJECT_NS: {
        int idx = -1;
        for (int i = 0; i < MAX_NS_SUBS; i++)
            if (st->subs[i].active && st->subs[i].accepted &&
                st->subs[i].has_publisher_handle) {
                idx = i; break;
            }
        if (idx < 0) { log_op(log, OP_PUMP, 0); break; }

        char suffix_name[16];
        snprintf(suffix_name, sizeof(suffix_name), "s%llu",
                 (unsigned long long)(rng_next(rng) % 100));
        moq_bytes_t suffix_parts[] = { moq_bytes_cstr(suffix_name) };
        moq_namespace_t suffix = { suffix_parts, 1 };
        bool do_done = (rng_next(rng) & 1) != 0;

        moq_result_t ns_rc;
        if (do_done) {
            ns_rc = moq_session_send_namespace(sv,
                st->subs[idx].publisher_handle, &suffix,
                moq_simpair_now_us(sp));
            if (ns_rc == MOQ_OK) {
                moq_simpair_run_until_quiescent(sp, 4, NULL);
                drain_client(c, ts);
                ns_rc = moq_session_send_namespace_done(sv,
                    st->subs[idx].publisher_handle, &suffix,
                    moq_simpair_now_us(sp));
            }
        } else {
            ns_rc = moq_session_send_namespace(sv,
                st->subs[idx].publisher_handle, &suffix,
                moq_simpair_now_us(sp));
        }
        log_op(log, op, do_done ? MOQ_D16_NAMESPACE_DONE : MOQ_D16_NAMESPACE);

        if (ns_rc == MOQ_ERR_WRONG_STATE) {
            /* e.g. entry already closed — skip. */
        } else if (ns_rc == MOQ_OK) {
            ts->ns_injected++;
            moq_result_t prc = moq_simpair_run_until_quiescent(sp, 4, NULL);
            if (prc != MOQ_OK && prc != MOQ_ERR_WOULD_BLOCK) return -1;
            drain_client(c, ts);
        } else if (ns_rc != MOQ_ERR_WOULD_BLOCK &&
                   ns_rc != MOQ_ERR_CLOSED) {
            return -1;
        }
        break;
    }

    case OP_DRAIN_CLIENT:
        log_op(log, op, 0);
        drain_client(c, ts);
        break;

    case OP_DRAIN_SERVER:
        log_op(log, op, 0);
        drain_server(sv, ts, st);
        break;

    default:
        log_op(log, OP_PUMP, 0);
        break;
    }

    if (moq_session_state(c) == MOQ_SESS_CLOSED ||
        moq_session_state(sv) == MOQ_SESS_CLOSED) {
        st->closed = true;
        ts->closed++;
    }
    return 0;
}

/* -- Deterministic prelude ------------------------------------------ */

static int run_prelude(moq_simpair_t *sp, trace_summary_t *ts)
{
    moq_session_t *c = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);

    /* 1. Subscribe namespace. */
    moq_subscribe_namespace_cfg_t cfg;
    moq_subscribe_namespace_cfg_init(&cfg);
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("pre") };
    cfg.track_namespace_prefix.parts = parts;
    cfg.track_namespace_prefix.count = 1;

    moq_ns_sub_handle_t csub;
    if (moq_session_subscribe_namespace(c, &cfg, moq_simpair_now_us(sp),
        &csub) != MOQ_OK) return -1;
    ts->ns_sub_sent++;
    if (moq_simpair_run_until_quiescent(sp, 8, NULL) < 0) return -1;

    /* 2. Publisher receives NS_SUB_REQUEST. */
    moq_event_t ev;
    if (moq_session_poll_events(sv, &ev, 1) != 1) return -1;
    if (ev.kind != MOQ_EVENT_NS_SUB_REQUEST) {
        moq_event_cleanup(&ev); return -1;
    }
    if (ev.u.ns_sub_request.track_namespace_prefix.count != 1)
        { moq_event_cleanup(&ev); return -1; }
    moq_ns_sub_handle_t psub = ev.u.ns_sub_request.handle;
    ts->ns_sub_request++;
    moq_event_cleanup(&ev);

    /* 3. Publisher accepts. */
    moq_accept_ns_sub_cfg_t acc;
    moq_accept_ns_sub_cfg_init(&acc);
    if (moq_session_accept_ns_sub(sv, psub, &acc,
        moq_simpair_now_us(sp)) != MOQ_OK) return -1;
    if (moq_simpair_run_until_quiescent(sp, 8, NULL) < 0) return -1;

    /* 4. Subscriber receives NS_SUB_OK. */
    if (moq_session_poll_events(c, &ev, 1) != 1) return -1;
    if (ev.kind != MOQ_EVENT_NS_SUB_OK) {
        moq_event_cleanup(&ev); return -1;
    }
    ts->ns_sub_ok++;
    moq_event_cleanup(&ev);

    /* 5. Publisher sends NAMESPACE → subscriber sees NAMESPACE_FOUND. */
    {
        moq_bytes_t suffix_parts[] = { MOQ_BYTES_LITERAL("found") };
        moq_namespace_t suffix = { suffix_parts, 1 };
        if (moq_session_send_namespace(sv, psub, &suffix,
            moq_simpair_now_us(sp)) != MOQ_OK) return -1;
        ts->ns_injected++;
        if (moq_simpair_run_until_quiescent(sp, 8, NULL) < 0) return -1;
    }

    if (moq_session_poll_events(c, &ev, 1) != 1) return -1;
    if (ev.kind != MOQ_EVENT_NAMESPACE_FOUND) {
        moq_event_cleanup(&ev); return -1;
    }
    if (ev.u.namespace_found.track_namespace_suffix.count != 1)
        { moq_event_cleanup(&ev); return -1; }
    ts->ns_found++;
    moq_event_cleanup(&ev);

    /* 6. Publisher sends NAMESPACE_DONE → NAMESPACE_GONE. */
    {
        moq_bytes_t suffix_parts[] = { MOQ_BYTES_LITERAL("found") };
        moq_namespace_t suffix = { suffix_parts, 1 };
        if (moq_session_send_namespace_done(sv, psub, &suffix,
            moq_simpair_now_us(sp)) != MOQ_OK) return -1;
        ts->ns_injected++;
        if (moq_simpair_run_until_quiescent(sp, 8, NULL) < 0) return -1;
    }
    if (moq_session_poll_events(c, &ev, 1) != 1) return -1;
    if (ev.kind != MOQ_EVENT_NAMESPACE_GONE) {
        moq_event_cleanup(&ev); return -1;
    }
    ts->ns_gone++;
    moq_event_cleanup(&ev);

    /* 7. Cancel. */
    if (moq_session_cancel_namespace_sub(c, csub,
        moq_simpair_now_us(sp)) != MOQ_OK) return -1;
    ts->cancel_sent++;
    if (moq_simpair_run_until_quiescent(sp, 8, NULL) < 0) return -1;

    /* Drain remaining. */
    {
        moq_event_t d[8]; size_t ne;
        while ((ne = moq_session_poll_events(c, d, 8)) > 0)
            for (size_t i = 0; i < ne; i++) moq_event_cleanup(&d[i]);
        while ((ne = moq_session_poll_events(sv, d, 8)) > 0)
            for (size_t i = 0; i < ne; i++) moq_event_cleanup(&d[i]);
    }

    return 0;
}

/* -- Run one scenario ----------------------------------------------- */

static int run_scenario(uint64_t seed, const moq_alloc_t *alloc,
                         trace_summary_t *summary, int run_id,
                         int max_steps)
{
    memset(summary, 0, sizeof(*summary));
    summary->hash = 0xCBF29CE484222325ULL;

    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = alloc;
    cfg.seed = seed;
    cfg.initial_now_us = 1000;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 64;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 64;
    cfg.trace_fn = trace_hash_fn;
    cfg.trace_ctx = summary;

    moq_simpair_t *sp = NULL;
    if (moq_simpair_create(&cfg, &sp) < 0) return -1;
    moq_simpair_start(sp);
    if (moq_simpair_run_until_quiescent(sp, 8, NULL) < 0) {
        moq_simpair_destroy(sp);
        return -1;
    }

    {
        moq_event_t ev;
        if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);
        if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);
    }

    int failures = 0;
    int prelude_rc = run_prelude(sp, summary);
    if (prelude_rc < 0) {
        fprintf(stderr, "FAIL seed=0x%llx run=%d: prelude failed\n",
                (unsigned long long)seed, run_id);
        fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx "
                "MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%d "
                "./build/tests/test_scenario_namespace_sub\n",
                (unsigned long long)seed, max_steps);
        failures++;
    }

    if (failures == 0) {
        shadow_state_t st;
        memset(&st, 0, sizeof(st));
        st.next_client_ref = 2;
        rng_t rng = { seed };
        op_log_t log = {0};

        for (int step = 0; step < max_steps; step++) {
            int src = execute_step(sp, &st, &rng, &log, summary);
            if (src < 0) {
                fprintf(stderr, "FAIL seed=0x%llx run=%d step=%d: "
                        "unexpected error\n",
                        (unsigned long long)seed, run_id, step);
                dump_log(seed, run_id, &log);
                fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx "
                        "MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%d "
                        "./build/tests/test_scenario_namespace_sub\n",
                        (unsigned long long)seed, max_steps);
                failures++;
                break;
            }
            if (st.closed) break;
        }
    }

    {
        moq_event_t drain[16]; size_t ne;
        while ((ne = moq_session_poll_events(moq_simpair_client(sp), drain, 16)) > 0)
            for (size_t i = 0; i < ne; i++) moq_event_cleanup(&drain[i]);
        while ((ne = moq_session_poll_events(moq_simpair_server(sp), drain, 16)) > 0)
            for (size_t i = 0; i < ne; i++) moq_event_cleanup(&drain[i]);
        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(moq_simpair_client(sp), acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        while ((na = moq_session_poll_actions(moq_simpair_server(sp), acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
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

    size_t total_sent = 0, total_request = 0, total_ok = 0, total_error = 0;
    size_t total_found = 0, total_gone = 0, total_cancel = 0;
    size_t total_injected = 0;

    for (uint64_t seed = seed_start; seed < seed_start + num_seeds; seed++) {
        trace_summary_t sums[2];

        for (int run = 0; run < 2; run++) {
            scen_alloc_t as = {0};
            moq_alloc_t alloc = { &as, sa_alloc, sa_realloc, sa_free };

            int rf = run_scenario(seed, &alloc, &sums[run], run, max_steps);
            failures += rf;

            if (as.balance != 0) {
                fprintf(stderr, "FAIL seed=0x%llx run=%d: "
                        "alloc balance=%lld\n",
                        (unsigned long long)seed, run,
                        (long long)as.balance);
                fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx "
                        "MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%d "
                        "./build/tests/test_scenario_namespace_sub\n",
                        (unsigned long long)seed, max_steps);
                failures++;
            }
        }

        total_sent += sums[0].ns_sub_sent;
        total_request += sums[0].ns_sub_request;
        total_ok += sums[0].ns_sub_ok;
        total_error += sums[0].ns_sub_error;
        total_found += sums[0].ns_found;
        total_gone += sums[0].ns_gone;
        total_cancel += sums[0].cancel_sent;
        total_injected += sums[0].ns_injected;

        if (sums[0].hash != sums[1].hash ||
            sums[0].count != sums[1].count ||
            sums[0].ns_sub_sent != sums[1].ns_sub_sent ||
            sums[0].ns_sub_request != sums[1].ns_sub_request ||
            sums[0].ns_sub_ok != sums[1].ns_sub_ok ||
            sums[0].ns_sub_error != sums[1].ns_sub_error ||
            sums[0].ns_found != sums[1].ns_found ||
            sums[0].ns_gone != sums[1].ns_gone ||
            sums[0].cancel_sent != sums[1].cancel_sent ||
            sums[0].ns_injected != sums[1].ns_injected ||
            sums[0].closed != sums[1].closed) {
            fprintf(stderr, "FAIL seed=0x%llx: trace/counter mismatch\n",
                    (unsigned long long)seed);
            fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx "
                    "MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%d "
                    "./build/tests/test_scenario_namespace_sub\n",
                    (unsigned long long)seed, max_steps);
            failures++;
        }

        if (sums[0].ns_sub_ok + sums[0].ns_sub_error > sums[0].ns_sub_sent) {
            fprintf(stderr, "FAIL seed=0x%llx: phantom response "
                    "ok=%zu err=%zu sent=%zu\n",
                    (unsigned long long)seed,
                    sums[0].ns_sub_ok, sums[0].ns_sub_error,
                    sums[0].ns_sub_sent);
            failures++;
        }
    }

    if (num_seeds >= 10) {
        if (total_sent == 0) {
            fprintf(stderr, "FAIL: no ns_sub_sent\n"); failures++; }
        if (total_request == 0) {
            fprintf(stderr, "FAIL: no ns_sub_request\n"); failures++; }
        if (total_ok == 0) {
            fprintf(stderr, "FAIL: no ns_sub_ok\n"); failures++; }
        if (total_found == 0) {
            fprintf(stderr, "FAIL: no ns_found\n"); failures++; }
        if (total_gone == 0) {
            fprintf(stderr, "FAIL: no ns_gone\n"); failures++; }
        if (total_cancel == 0) {
            fprintf(stderr, "FAIL: no cancel_sent\n"); failures++; }
        if (total_found <= num_seeds || total_gone <= num_seeds) {
            fprintf(stderr, "FAIL: ns_found=%zu ns_gone=%zu should exceed "
                    "prelude baseline (%llu)\n",
                    total_found, total_gone,
                    (unsigned long long)num_seeds);
            failures++;
        }
    }

    if (failures == 0)
        fprintf(stderr, "PASS: test_scenario_namespace_sub "
                "(%llu seeds, sent=%zu req=%zu ok=%zu err=%zu "
                "found=%zu gone=%zu cancel=%zu inj=%zu)\n",
                (unsigned long long)num_seeds,
                total_sent, total_request, total_ok, total_error,
                total_found, total_gone, total_cancel, total_injected);
    else
        fprintf(stderr, "FAIL: test_scenario_namespace_sub "
                "(%d failures in %llu seeds)\n",
                failures, (unsigned long long)num_seeds);

    return failures;
}
