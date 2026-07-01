/*
 * Deterministic transport fault injection scenario runner.
 *
 * Uses SimPair transport-level fault injection (DROP_CONTROL,
 * DROP_DATA, DROP_RESET, DROP_STOP) with the publisher facade.
 * Every seed runs twice; trace hash and per-kind fault counts
 * must match. Allocator balance must return to zero.
 *
 * All session/facade return codes are checked against an allowed
 * set. Unexpected errors are flagged with seed, step, and replay
 * command.
 */

#include <moq/publisher.h>
#include <moq/sim.h>
#include "../../tests/unit/test_support.h"
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* -- splitmix64 PRNG ------------------------------------------------ */

typedef struct { uint64_t s; } rng_t;

static uint64_t rng_next(rng_t *r) {
    uint64_t z = (r->s += 0x9E3779B97F4A7C15ULL);
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    return z ^ (z >> 31);
}

/* -- Trace/fault summary -------------------------------------------- */

typedef struct {
    uint64_t trace_hash;
    size_t   trace_count;
    size_t   drop_control;
    size_t   drop_data;
    size_t   drop_reset;
    size_t   drop_stop;
    size_t   mutate_control;
    size_t   mutate_data;
    size_t   reorder_action;
    size_t   inject_reset;
    size_t   inject_stop;
    size_t   inject_close;
    size_t   truncate_control;
    size_t   truncate_data;
} run_summary_t;

static size_t total_faults(const run_summary_t *s) {
    return s->drop_control + s->drop_data + s->drop_reset + s->drop_stop +
           s->mutate_control + s->mutate_data + s->reorder_action +
           s->inject_reset + s->inject_stop + s->inject_close +
           s->truncate_control + s->truncate_data;
}

static void trace_hash_fn(void *ctx, const moq_sim_trace_record_t *r) {
    run_summary_t *s = (run_summary_t *)ctx;
    uint64_t h = s->trace_hash;
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
    s->trace_hash = h;
    s->trace_count++;

    if (r->kind == MOQ_SIM_TRACE_FAULT_DROP) {
        switch (r->action_kind) {
        case MOQ_ACTION_SEND_CONTROL: s->drop_control++; break;
        case MOQ_ACTION_SEND_DATA:    s->drop_data++;    break;
        case MOQ_ACTION_RESET_DATA:   s->drop_reset++;   break;
        case MOQ_ACTION_STOP_DATA:    s->drop_stop++;    break;
        default: break;
        }
    }
    if (r->kind == MOQ_SIM_TRACE_FAULT_MUTATE) {
        if (r->action_kind == MOQ_ACTION_SEND_CONTROL)
            s->mutate_control++;
        else if (r->action_kind == MOQ_ACTION_SEND_DATA)
            s->mutate_data++;
    }
    if (r->kind == MOQ_SIM_TRACE_FAULT_REORDER)
        s->reorder_action++;
    if (r->kind == MOQ_SIM_TRACE_FAULT_INJECT) {
        if (r->action_kind == MOQ_ACTION_RESET_DATA)
            s->inject_reset++;
        else if (r->action_kind == MOQ_ACTION_STOP_DATA)
            s->inject_stop++;
        else if (r->action_kind == MOQ_ACTION_CLOSE_SESSION)
            s->inject_close++;
    }
    if (r->kind == MOQ_SIM_TRACE_FAULT_TRUNCATE) {
        if (r->action_kind == MOQ_ACTION_SEND_CONTROL)
            s->truncate_control++;
        else if (r->action_kind == MOQ_ACTION_SEND_DATA)
            s->truncate_data++;
    }
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

#define MAX_TRACKS 4

typedef struct {
    bool             active;
    bool             has_sub;
    bool             group_open;
    moq_pub_track_t *track;
    char             name[16];
    uint64_t         group_id;
    uint64_t         object_id;
} shadow_track_t;

typedef struct {
    shadow_track_t tracks[MAX_TRACKS];
    uint64_t       next_track_id;
    bool           closed;
} shadow_state_t;

static int pick_track(const shadow_state_t *st, rng_t *rng,
                      bool require_sub, bool require_group)
{
    int candidates[MAX_TRACKS];
    int n = 0;
    for (int i = 0; i < MAX_TRACKS; i++) {
        if (!st->tracks[i].active) continue;
        if (require_sub && !st->tracks[i].has_sub) continue;
        if (require_group && !st->tracks[i].group_open) continue;
        candidates[n++] = i;
    }
    if (n == 0) return -1;
    return candidates[rng_next(rng) % (uint64_t)n];
}

static void refresh_subscriptions(moq_publisher_t *pub, shadow_state_t *st)
{
    for (int i = 0; i < MAX_TRACKS; i++) {
        if (!st->tracks[i].active) continue;
        st->tracks[i].has_sub =
            moq_pub_active_subscriptions(pub, st->tracks[i].track) > 0;
        if (!st->tracks[i].has_sub)
            st->tracks[i].group_open = false;
    }
}

/* -- Operations ----------------------------------------------------- */

typedef enum {
    OP_PUMP,
    OP_ADD_TRACK,
    OP_REMOVE_TRACK,
    OP_SUBSCRIBE,
    OP_WRITE_OBJECT,
    OP_END_GROUP,
    OP_DRAIN_EVENTS,
    OP_FLUSH,
    OP_ADVANCE_TIME,
    OP_COUNT,
} scenario_op_t;

static const char *op_name(scenario_op_t op) {
    static const char *names[] = {
        "PUMP","ADD_TRACK","RM_TRACK","SUBSCRIBE","WRITE_OBJ",
        "END_GROUP","DRAIN_EV","FLUSH","ADV_TIME",
    };
    return op < OP_COUNT ? names[op] : "?";
}

#define MAX_LOG 80
typedef struct { scenario_op_t op; uint64_t p; } log_entry_t;
typedef struct { log_entry_t e[MAX_LOG]; size_t n; } op_log_t;

static void log_op(op_log_t *log, scenario_op_t op, uint64_t p) {
    if (log->n < MAX_LOG) { log->e[log->n].op = op; log->e[log->n].p = p; log->n++; }
}

static void dump_log(uint64_t seed, int run, const op_log_t *log,
                     int max_steps, uint64_t per_mille) {
    fprintf(stderr, "  seed=0x%llx run=%d ops:\n",
            (unsigned long long)seed, run);
    for (size_t i = 0; i < log->n; i++)
        fprintf(stderr, "    %zu: %s p=%llu\n", i,
                op_name(log->e[i].op), (unsigned long long)log->e[i].p);
    fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx "
            "MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%d "
            "MOQ_SCENARIO_FAULT_PERMILLE=%llu "
            "./build/tests/test_scenario_transport_faults\n",
            (unsigned long long)seed, max_steps,
            (unsigned long long)per_mille);
}

/* -- Return-code checking ------------------------------------------- */

typedef struct {
    uint64_t seed; int run_id; const op_log_t *log;
    int max_steps; uint64_t per_mille;
} ctx_t;

static int note_bad_rc(const char *where, moq_result_t rc,
                        const ctx_t *ctx)
{
    fprintf(stderr, "FAIL seed=0x%llx run=%d: unexpected rc=%d at %s\n",
            (unsigned long long)ctx->seed, ctx->run_id, (int)rc, where);
    dump_log(ctx->seed, ctx->run_id, ctx->log,
             ctx->max_steps, ctx->per_mille);
    return 1;
}

static bool ok_or_closed(moq_result_t rc) {
    return rc == MOQ_OK || rc == MOQ_ERR_CLOSED;
}

static bool ok_or_backpressure(moq_result_t rc) {
    return rc == MOQ_OK || rc == MOQ_ERR_WOULD_BLOCK ||
           rc == MOQ_ERR_CLOSED || rc == MOQ_ERR_WRONG_STATE ||
           rc == MOQ_ERR_REQUEST_BLOCKED;
}

static bool ok_or_stale(moq_result_t rc) {
    return rc == MOQ_OK || rc == MOQ_ERR_WOULD_BLOCK ||
           rc == MOQ_ERR_CLOSED || rc == MOQ_ERR_WRONG_STATE ||
           rc == MOQ_ERR_STALE_HANDLE;
}

static int pump_checked(moq_simpair_t *sp, size_t max,
                         const ctx_t *ctx, const char *where)
{
    moq_result_t rc = moq_simpair_run_until_quiescent(sp, max, NULL);
    /* OK, WOULD_BLOCK (step limit), and CLOSED (session closed from
     * dropped control message causing protocol violation) are all
     * expected under transport faults. */
    if (rc != MOQ_OK && rc != MOQ_ERR_WOULD_BLOCK &&
        rc != MOQ_ERR_CLOSED && rc != MOQ_ERR_PROTO)
        return note_bad_rc(where, rc, ctx);
    return 0;
}

/* -- Event draining with rc checks ---------------------------------- */

static int drain_events(moq_simpair_t *sp, moq_publisher_t *pub,
                         shadow_state_t *st, rng_t *rng, const ctx_t *ctx)
{
    int failures = 0;
    moq_session_t *client = moq_simpair_client(sp);
    moq_session_t *server = moq_simpair_server(sp);
    uint64_t now = moq_simpair_now_us(sp);

    moq_event_t evs[8]; size_t n;
    while ((n = moq_session_poll_events(client, evs, 8)) > 0) {
        bool responded = false;
        for (size_t i = 0; i < n; i++) {
            if (evs[i].kind == MOQ_EVENT_NAMESPACE_PUBLISHED) {
                moq_result_t rc;
                if ((rng_next(rng) & 1) == 0) {
                    moq_accept_namespace_cfg_t cfg;
                    moq_accept_namespace_cfg_init(&cfg);
                    rc = moq_session_accept_namespace(client,
                        evs[i].u.namespace_published.ann, &cfg, now);
                } else {
                    moq_reject_namespace_cfg_t cfg;
                    moq_reject_namespace_cfg_init(&cfg);
                    cfg.error_code = MOQ_REQUEST_ERROR_UNAUTHORIZED;
                    rc = moq_session_reject_namespace(client,
                        evs[i].u.namespace_published.ann, &cfg, now);
                }
                if (!ok_or_backpressure(rc))
                    failures += note_bad_rc("ns_accept_reject", rc, ctx);
                responded = true;
            }
            moq_event_cleanup(&evs[i]);
        }
        if (responded)
            failures += pump_checked(sp, 4, ctx, "drain_ns_pump");
    }

    while ((n = moq_session_poll_events(server, evs, 8)) > 0) {
        for (size_t i = 0; i < n; i++) {
            moq_pub_event_result_t res;
            moq_result_t rc = moq_pub_handle_event(pub, &evs[i], now, &res);
            if (!ok_or_backpressure(rc))
                failures += note_bad_rc("pub_handle_event", rc, ctx);
            moq_event_cleanup(&evs[i]);
        }
        refresh_subscriptions(pub, st);
    }
    return failures;
}

/* -- Execute one step ----------------------------------------------- */

static int execute_step(moq_simpair_t *sp, moq_publisher_t *pub,
                         shadow_state_t *st, rng_t *rng, op_log_t *log,
                         const moq_alloc_t *alloc, const ctx_t *ctx)
{
    if (st->closed) return 0;
    int failures = 0;

    scenario_op_t op = (scenario_op_t)(rng_next(rng) % OP_COUNT);
    moq_session_t *client = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);

    switch (op) {
    case OP_PUMP:
        log_op(log, op, 0);
        failures += pump_checked(sp, 4, ctx, "op_pump");
        break;

    case OP_ADD_TRACK: {
        int slot = -1;
        for (int i = 0; i < MAX_TRACKS; i++)
            if (!st->tracks[i].active) { slot = i; break; }
        if (slot < 0) { log_op(log, OP_PUMP, 0); break; }

        char name[16];
        snprintf(name, sizeof(name), "tf%llu",
                 (unsigned long long)st->next_track_id++);

        moq_pub_track_cfg_t cfg;
        moq_pub_track_cfg_init(&cfg);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("tns") };
        cfg.track_namespace.parts = ns_parts;
        cfg.track_namespace.count = 1;
        cfg.track_name = moq_bytes_cstr(name);

        moq_pub_track_t *track = NULL;
        moq_result_t rc = moq_pub_add_track(pub, &cfg, now, &track);
        log_op(log, op, rc == MOQ_OK ? 1 : 0);
        if (rc == MOQ_OK) {
            st->tracks[slot].active = true;
            st->tracks[slot].track = track;
            st->tracks[slot].has_sub = false;
            st->tracks[slot].group_open = false;
            st->tracks[slot].group_id = 0;
            st->tracks[slot].object_id = 0;
            memcpy(st->tracks[slot].name, name, sizeof(name));
        } else if (!ok_or_backpressure(rc)) {
            failures += note_bad_rc("pub_add_track", rc, ctx);
        }
        break;
    }

    case OP_REMOVE_TRACK: {
        int idx = pick_track(st, rng, false, false);
        if (idx < 0) { log_op(log, OP_PUMP, 0); break; }
        log_op(log, op, (uint64_t)idx);
        moq_result_t rc = moq_pub_remove_track(pub, st->tracks[idx].track, now);
        if (rc == MOQ_OK) {
            memset(&st->tracks[idx], 0, sizeof(st->tracks[idx]));
        } else if (!ok_or_stale(rc)) {
            failures += note_bad_rc("pub_remove_track", rc, ctx);
        }
        break;
    }

    case OP_SUBSCRIBE: {
        int idx = pick_track(st, rng, false, false);
        if (idx < 0 || st->tracks[idx].has_sub) {
            log_op(log, OP_PUMP, 0); break;
        }

        moq_subscribe_cfg_t cfg;
        moq_subscribe_cfg_init(&cfg);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("tns") };
        cfg.track_namespace.parts = ns_parts;
        cfg.track_namespace.count = 1;
        cfg.track_name = moq_bytes_cstr(st->tracks[idx].name);

        moq_subscription_t sub;
        moq_result_t rc = moq_session_subscribe(client, &cfg, now, &sub);
        log_op(log, op, rc == MOQ_OK ? (uint64_t)idx : UINT64_MAX);
        if (rc == MOQ_OK) {
            failures += pump_checked(sp, 4, ctx, "subscribe_pump1");
            failures += drain_events(sp, pub, st, rng, ctx);
            failures += pump_checked(sp, 4, ctx, "subscribe_pump2");
            failures += drain_events(sp, pub, st, rng, ctx);
        } else if (!ok_or_backpressure(rc) && rc != MOQ_ERR_INVAL) {
            /* INVAL can occur if a prior dropped control message left
             * the session in a state where subscribe validation fails
             * (e.g., request ID sequencing broken by dropped messages). */
            failures += note_bad_rc("session_subscribe", rc, ctx);
        }
        break;
    }

    case OP_WRITE_OBJECT: {
        int idx = pick_track(st, rng, true, false);
        if (idx < 0) { log_op(log, OP_PUMP, 0); break; }

        uint8_t pdata[16];
        size_t plen = (rng_next(rng) % sizeof(pdata)) + 1;
        for (size_t j = 0; j < plen; j++)
            pdata[j] = (uint8_t)(rng_next(rng) & 0xFF);

        moq_rcbuf_t *payload = NULL;
        moq_result_t rc = moq_rcbuf_create(alloc, pdata, plen, &payload);
        if (rc != MOQ_OK || !payload) {
            log_op(log, op, UINT64_MAX);
            if (rc != MOQ_OK && rc != MOQ_ERR_NOMEM)
                failures += note_bad_rc("rcbuf_create", rc, ctx);
            break;
        }

        shadow_track_t *t = &st->tracks[idx];
        rc = moq_pub_write_object(pub, t->track,
            t->group_id, t->object_id, payload, now);
        log_op(log, op, rc == MOQ_OK ? t->object_id : UINT64_MAX);
        if (rc == MOQ_OK) {
            t->object_id++;
            t->group_open = true;
        } else if (!ok_or_stale(rc)) {
            failures += note_bad_rc("pub_write_object", rc, ctx);
        }
        moq_rcbuf_decref(payload);
        break;
    }

    case OP_END_GROUP: {
        int idx = pick_track(st, rng, true, true);
        if (idx < 0) { log_op(log, OP_PUMP, 0); break; }
        shadow_track_t *t = &st->tracks[idx];
        moq_result_t rc = moq_pub_end_group(pub, t->track, now);
        log_op(log, op, rc == MOQ_OK ? t->group_id : UINT64_MAX);
        if (rc == MOQ_OK) {
            t->group_id++;
            t->object_id = 0;
            t->group_open = false;
        } else if (!ok_or_stale(rc)) {
            failures += note_bad_rc("pub_end_group", rc, ctx);
        }
        break;
    }

    case OP_DRAIN_EVENTS:
        log_op(log, op, 0);
        failures += drain_events(sp, pub, st, rng, ctx);
        break;

    case OP_FLUSH: {
        log_op(log, op, 0);
        moq_result_t rc = moq_pub_flush(pub, now);
        if (!ok_or_backpressure(rc))
            failures += note_bad_rc("pub_flush", rc, ctx);
        refresh_subscriptions(pub, st);
        break;
    }

    case OP_ADVANCE_TIME: {
        uint64_t delta = (rng_next(rng) % 100) + 1;
        log_op(log, op, delta);
        moq_result_t rc = moq_simpair_advance_to(sp, now + delta);
        if (!ok_or_closed(rc))
            failures += note_bad_rc("simpair_advance_to", rc, ctx);
        break;
    }

    default: break;
    }

    if (moq_session_state(moq_simpair_client(sp)) == MOQ_SESS_CLOSED ||
        moq_session_state(moq_simpair_server(sp)) == MOQ_SESS_CLOSED)
        st->closed = true;

    return failures;
}

/* -- Run one scenario ----------------------------------------------- */

static int run_scenario(uint64_t seed, uint64_t per_mille,
                         const moq_alloc_t *alloc,
                         run_summary_t *summary, int run_id,
                         int max_steps)
{
    memset(summary, 0, sizeof(*summary));
    summary->trace_hash = 0xCBF29CE484222325ULL;

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
    cfg.fault_per_mille = (uint32_t)per_mille;
    cfg.fault_flags = MOQ_SIM_FAULT_ALL | MOQ_SIM_FAULT_ALL_INJECT;

    moq_simpair_t *sp = NULL;
    if (moq_simpair_create(&cfg, &sp) < 0) return -1;
    moq_simpair_start(sp);
    {
        moq_result_t rc = moq_simpair_run_until_quiescent(sp, 8, NULL);
        if (rc != MOQ_OK && rc != MOQ_ERR_WOULD_BLOCK) {
            moq_simpair_destroy(sp);
            return -1;
        }
    }

    { moq_event_t ev;
      if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev);
      if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev);
    }

    moq_pub_cfg_t pcfg;
    moq_pub_cfg_init_sized(&pcfg, sizeof(pcfg));
    pcfg.accept_mode = MOQ_PUB_ACCEPT_ALL;

    moq_publisher_t *pub = NULL;
    if (moq_pub_create(moq_simpair_server(sp), alloc, &pcfg, &pub) < 0) {
        moq_simpair_destroy(sp);
        return -1;
    }

    moq_simpair_enable_faults(sp);

    shadow_state_t st;
    memset(&st, 0, sizeof(st));
    rng_t rng = { seed };
    op_log_t log = {0};
    int failures = 0;

    ctx_t ctx = { seed, run_id, &log, max_steps, per_mille };

    for (int step = 0; step < max_steps; step++) {
        int sf = execute_step(sp, pub, &st, &rng, &log, alloc, &ctx);
        if (sf > 0) { failures += sf; break; }

        sf = pump_checked(sp, 4, &ctx, "step_pump");
        if (sf > 0) { failures += sf; break; }
        sf = drain_events(sp, pub, &st, &rng, &ctx);
        if (sf > 0) { failures += sf; break; }

        if (st.closed) {
            if (total_faults(summary) == 0) {
                fprintf(stderr, "FAIL seed=0x%llx run=%d step=%d: "
                        "closed without transport fault\n",
                        (unsigned long long)seed, run_id, step);
                dump_log(seed, run_id, &log, max_steps, per_mille);
                failures++;
            }
            break;
        }
    }

    for (int i = 0; i < MAX_TRACKS; i++) {
        if (st.tracks[i].active && st.tracks[i].group_open)
            moq_pub_end_group(pub, st.tracks[i].track,
                moq_simpair_now_us(sp));
    }
    for (int p = 0; p < 8; p++) {
        moq_result_t prc = moq_simpair_run_until_quiescent(sp, 8, NULL);
        if (prc != MOQ_OK && prc != MOQ_ERR_WOULD_BLOCK) break;
    }

    { moq_event_t evs[16]; size_t ne;
      while ((ne = moq_session_poll_events(moq_simpair_client(sp), evs, 16)) > 0)
          for (size_t i = 0; i < ne; i++) moq_event_cleanup(&evs[i]);
      while ((ne = moq_session_poll_events(moq_simpair_server(sp), evs, 16)) > 0)
          for (size_t i = 0; i < ne; i++) moq_event_cleanup(&evs[i]);
      moq_action_t acts[16]; size_t na;
      while ((na = moq_session_poll_actions(moq_simpair_server(sp), acts, 16)) > 0)
          for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
      while ((na = moq_session_poll_actions(moq_simpair_client(sp), acts, 16)) > 0)
          for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
    }

    moq_pub_destroy(pub);
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

    size_t total_control = 0, total_data = 0;
    size_t total_reset = 0, total_stop = 0;
    size_t total_mut_c = 0, total_mut_d = 0, total_reorder = 0;
    size_t total_inj_rst = 0, total_inj_stp = 0, total_inj_cls = 0;
    size_t total_trunc_ctl = 0, total_trunc_data = 0;

    for (uint64_t seed = seed_start; seed < seed_start + num_seeds; seed++) {
        run_summary_t sums[2];

        for (int run = 0; run < 2; run++) {
            scen_alloc_t as = {0};
            moq_alloc_t alloc = { &as, sa_alloc, sa_realloc, sa_free };

            int rf = run_scenario(seed, per_mille, &alloc,
                                   &sums[run], run, max_steps);
            failures += rf;

            if (as.balance != 0) {
                fprintf(stderr, "FAIL seed=0x%llx run=%d: "
                        "alloc balance=%lld\n",
                        (unsigned long long)seed, run,
                        (long long)as.balance);
                fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx "
                        "MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%d "
                        "MOQ_SCENARIO_FAULT_PERMILLE=%llu "
                        "./build/tests/test_scenario_transport_faults\n",
                        (unsigned long long)seed, max_steps,
                        (unsigned long long)per_mille);
                failures++;
            }
        }

        total_control += sums[0].drop_control;
        total_data    += sums[0].drop_data;
        total_reset   += sums[0].drop_reset;
        total_stop    += sums[0].drop_stop;
        total_mut_c   += sums[0].mutate_control;
        total_mut_d   += sums[0].mutate_data;
        total_reorder += sums[0].reorder_action;
        total_inj_rst += sums[0].inject_reset;
        total_inj_stp += sums[0].inject_stop;
        total_inj_cls += sums[0].inject_close;
        total_trunc_ctl += sums[0].truncate_control;
        total_trunc_data += sums[0].truncate_data;

        if (sums[0].trace_hash != sums[1].trace_hash ||
            sums[0].trace_count != sums[1].trace_count ||
            sums[0].drop_control != sums[1].drop_control ||
            sums[0].drop_data != sums[1].drop_data ||
            sums[0].drop_reset != sums[1].drop_reset ||
            sums[0].drop_stop != sums[1].drop_stop ||
            sums[0].mutate_control != sums[1].mutate_control ||
            sums[0].mutate_data != sums[1].mutate_data ||
            sums[0].reorder_action != sums[1].reorder_action ||
            sums[0].inject_reset != sums[1].inject_reset ||
            sums[0].inject_stop != sums[1].inject_stop ||
            sums[0].inject_close != sums[1].inject_close ||
            sums[0].truncate_control != sums[1].truncate_control ||
            sums[0].truncate_data != sums[1].truncate_data) {
            fprintf(stderr, "FAIL seed=0x%llx: nondeterministic transport fault\n",
                    (unsigned long long)seed);
            fprintf(stderr, "  run0 trace=0x%llx/%zu "
                    "ctrl=%zu data=%zu rst=%zu stop=%zu\n",
                    (unsigned long long)sums[0].trace_hash,
                    sums[0].trace_count,
                    sums[0].drop_control, sums[0].drop_data,
                    sums[0].drop_reset, sums[0].drop_stop);
            fprintf(stderr, "  run1 trace=0x%llx/%zu "
                    "ctrl=%zu data=%zu rst=%zu stop=%zu\n",
                    (unsigned long long)sums[1].trace_hash,
                    sums[1].trace_count,
                    sums[1].drop_control, sums[1].drop_data,
                    sums[1].drop_reset, sums[1].drop_stop);
            fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx "
                    "MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%d "
                    "MOQ_SCENARIO_FAULT_PERMILLE=%llu "
                    "./build/tests/test_scenario_transport_faults\n",
                    (unsigned long long)seed, max_steps,
                    (unsigned long long)per_mille);
            failures++;
        }
    }

    /* CONTROL and DATA drops should fire over any reasonable sweep.
     * RESET and STOP are opportunistic — they require subgroup
     * close/reset actions which depend on operation sequencing. */
    if (per_mille > 0 && num_seeds >= 10) {
        if (total_control + total_data + total_mut_c + total_mut_d == 0) {
            fprintf(stderr, "FAIL: no transport faults across %llu seeds\n",
                    (unsigned long long)num_seeds);
            failures++;
        }
    }

    if (failures == 0)
        fprintf(stderr, "PASS: test_scenario_transport_faults "
                "(%llu seeds, ctrl=%zu data=%zu rst=%zu stop=%zu "
                "mut_c=%zu mut_d=%zu reord=%zu "
                "inj_rst=%zu inj_stp=%zu inj_cls=%zu "
                "trunc_ctl=%zu trunc_data=%zu)\n",
                (unsigned long long)num_seeds,
                total_control, total_data, total_reset, total_stop,
                total_mut_c, total_mut_d, total_reorder,
                total_inj_rst, total_inj_stp, total_inj_cls,
                total_trunc_ctl, total_trunc_data);
    else
        fprintf(stderr, "FAIL: test_scenario_transport_faults "
                "(%d failures in %llu seeds)\n",
                failures, (unsigned long long)num_seeds);

    return failures;
}
