/*
 * Combined allocation + transport fault injection scenario runner.
 *
 * Exercises publisher facade + SimPair under both seeded allocator
 * failures and seeded transport drops in the same run. Catches
 * interactions the single-fault runners won't see: allocation failure
 * while recovering from dropped control/data, cleanup paths after
 * partially completed faulted operations.
 *
 * Each seed runs twice; trace hash, allocation counts, and per-kind
 * transport drop counts must match. Allocator balance must return
 * to zero.
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

static uint64_t mix64(uint64_t z) {
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    return z ^ (z >> 31);
}

/* -- Summary -------------------------------------------------------- */

typedef struct {
    uint64_t trace_hash;
    size_t   trace_count;
    uint64_t alloc_calls;
    uint64_t alloc_failures;
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

static size_t total_transport(const run_summary_t *s) {
    return s->drop_control + s->drop_data + s->drop_reset + s->drop_stop +
           s->mutate_control + s->mutate_data + s->reorder_action +
           s->inject_reset + s->inject_stop + s->inject_close +
           s->truncate_control + s->truncate_data;
}

static size_t total_faults(const run_summary_t *s) {
    return (size_t)s->alloc_failures + total_transport(s);
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

/* -- Fault allocator ------------------------------------------------ */

typedef struct {
    uint64_t seed;
    uint64_t calls;
    uint64_t failures;
    uint64_t per_mille;
    int64_t  balance;
    bool     enabled;
} fault_alloc_state_t;

static bool fault_should_fail(fault_alloc_state_t *s, size_t size,
                              uint64_t kind)
{
    s->calls++;
    if (!s->enabled || s->per_mille == 0)
        return false;
    uint64_t x = s->seed;
    x ^= s->calls * 0xD6E8FEB86659FD93ULL;
    x ^= kind * 0xA0761D6478BD642FULL;
    x ^= (uint64_t)size * 0xE7037ED1A0B428DBULL;
    if ((mix64(x) % 1000) < s->per_mille) {
        s->failures++;
        return true;
    }
    return false;
}

static void *fa_alloc(size_t sz, void *ctx) {
    fault_alloc_state_t *s = (fault_alloc_state_t *)ctx;
    if (fault_should_fail(s, sz, 1)) return NULL;
    void *p = malloc(sz);
    if (p) s->balance++;
    return p;
}

static void *fa_realloc(void *ptr, size_t old_sz, size_t new_sz, void *ctx) {
    fault_alloc_state_t *s = (fault_alloc_state_t *)ctx;
    (void)old_sz;
    if (new_sz == 0) {
        if (ptr) { s->balance--; free(ptr); }
        return NULL;
    }
    if (fault_should_fail(s, new_sz, 2)) return NULL;
    void *p = realloc(ptr, new_sz);
    if (p && !ptr) s->balance++;
    return p;
}

static void fa_free(void *ptr, size_t sz, void *ctx) {
    fault_alloc_state_t *s = (fault_alloc_state_t *)ctx;
    (void)sz;
    if (ptr) { s->balance--; free(ptr); }
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

static void refresh_subs(moq_publisher_t *pub, shadow_state_t *st) {
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
    OP_PUMP, OP_ADD_TRACK, OP_REMOVE_TRACK, OP_SUBSCRIBE,
    OP_WRITE_OBJECT, OP_END_GROUP, OP_DRAIN_EVENTS, OP_FLUSH,
    OP_ADVANCE_TIME, OP_COUNT,
} scenario_op_t;

static const char *op_name(scenario_op_t op) {
    static const char *n[] = {
        "PUMP","ADD_TRACK","RM_TRACK","SUBSCRIBE","WRITE_OBJ",
        "END_GROUP","DRAIN_EV","FLUSH","ADV_TIME",
    };
    return op < OP_COUNT ? n[op] : "?";
}

#define MAX_LOG 80
typedef struct { scenario_op_t op; uint64_t p; } log_entry_t;
typedef struct { log_entry_t e[MAX_LOG]; size_t n; } op_log_t;

static void log_op(op_log_t *l, scenario_op_t op, uint64_t p) {
    if (l->n < MAX_LOG) { l->e[l->n].op = op; l->e[l->n].p = p; l->n++; }
}

typedef struct {
    uint64_t seed; int run_id; const op_log_t *log;
    int max_steps; uint64_t alloc_pm; uint64_t transport_pm;
} ctx_t;

static void dump_log(const ctx_t *ctx) {
    fprintf(stderr, "  seed=0x%llx run=%d ops:\n",
            (unsigned long long)ctx->seed, ctx->run_id);
    for (size_t i = 0; i < ctx->log->n; i++)
        fprintf(stderr, "    %zu: %s p=%llu\n", i,
                op_name(ctx->log->e[i].op),
                (unsigned long long)ctx->log->e[i].p);
    fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx "
            "MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%d "
            "MOQ_SCENARIO_ALLOC_FAULT_PERMILLE=%llu "
            "MOQ_SCENARIO_TRANSPORT_FAULT_PERMILLE=%llu "
            "./build/tests/test_scenario_combined_faults\n",
            (unsigned long long)ctx->seed, ctx->max_steps,
            (unsigned long long)ctx->alloc_pm,
            (unsigned long long)ctx->transport_pm);
}

static int note_bad_rc(const char *where, moq_result_t rc,
                        const ctx_t *ctx) {
    fprintf(stderr, "FAIL seed=0x%llx run=%d: unexpected rc=%d at %s\n",
            (unsigned long long)ctx->seed, ctx->run_id, (int)rc, where);
    dump_log(ctx);
    return 1;
}

/* -- Per-call rc whitelists ----------------------------------------- */

static bool rc_ok_pump(moq_result_t rc) {
    return rc == MOQ_OK || rc == MOQ_ERR_WOULD_BLOCK ||
           rc == MOQ_ERR_CLOSED || rc == MOQ_ERR_PROTO ||
           rc == MOQ_ERR_NOMEM;
}

static bool rc_ok_facade(moq_result_t rc) {
    return rc == MOQ_OK || rc == MOQ_ERR_WOULD_BLOCK ||
           rc == MOQ_ERR_CLOSED || rc == MOQ_ERR_WRONG_STATE ||
           rc == MOQ_ERR_NOMEM || rc == MOQ_ERR_REQUEST_BLOCKED;
}

static bool rc_ok_write(moq_result_t rc) {
    return rc_ok_facade(rc) || rc == MOQ_ERR_STALE_HANDLE;
}

static int pump_checked(moq_simpair_t *sp, size_t max,
                         const ctx_t *ctx, const char *where) {
    moq_result_t rc = moq_simpair_run_until_quiescent(sp, max, NULL);
    if (!rc_ok_pump(rc)) return note_bad_rc(where, rc, ctx);
    return 0;
}

/* -- Event draining ------------------------------------------------- */

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
                if (!rc_ok_facade(rc))
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
            if (!rc_ok_facade(rc))
                failures += note_bad_rc("pub_handle_event", rc, ctx);
            moq_event_cleanup(&evs[i]);
        }
        refresh_subs(pub, st);
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
        snprintf(name, sizeof(name), "cf%llu",
                 (unsigned long long)st->next_track_id++);

        moq_pub_track_cfg_t cfg;
        moq_pub_track_cfg_init(&cfg);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("cns") };
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
        } else if (!rc_ok_facade(rc)) {
            failures += note_bad_rc("pub_add_track", rc, ctx);
        }
        break;
    }

    case OP_REMOVE_TRACK: {
        int idx = pick_track(st, rng, false, false);
        if (idx < 0) { log_op(log, OP_PUMP, 0); break; }
        log_op(log, op, (uint64_t)idx);
        moq_result_t rc = moq_pub_remove_track(pub, st->tracks[idx].track, now);
        if (rc == MOQ_OK)
            memset(&st->tracks[idx], 0, sizeof(st->tracks[idx]));
        else if (!rc_ok_write(rc))
            failures += note_bad_rc("pub_remove_track", rc, ctx);
        break;
    }

    case OP_SUBSCRIBE: {
        int idx = pick_track(st, rng, false, false);
        if (idx < 0 || st->tracks[idx].has_sub) {
            log_op(log, OP_PUMP, 0); break;
        }

        moq_subscribe_cfg_t cfg;
        moq_subscribe_cfg_init(&cfg);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("cns") };
        cfg.track_namespace.parts = ns_parts;
        cfg.track_namespace.count = 1;
        cfg.track_name = moq_bytes_cstr(st->tracks[idx].name);

        moq_subscription_t sub;
        moq_result_t rc = moq_session_subscribe(client, &cfg, now, &sub);
        log_op(log, op, rc == MOQ_OK ? (uint64_t)idx : UINT64_MAX);
        if (rc == MOQ_OK) {
            failures += pump_checked(sp, 4, ctx, "sub_pump1");
            failures += drain_events(sp, pub, st, rng, ctx);
            failures += pump_checked(sp, 4, ctx, "sub_pump2");
            failures += drain_events(sp, pub, st, rng, ctx);
        } else if (!rc_ok_facade(rc) && rc != MOQ_ERR_INVAL) {
            /* INVAL possible when dropped control messages break
             * request ID sequencing on the client session. */
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
        } else if (!rc_ok_write(rc)) {
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
        } else if (!rc_ok_write(rc)) {
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
        if (!rc_ok_facade(rc))
            failures += note_bad_rc("pub_flush", rc, ctx);
        refresh_subs(pub, st);
        break;
    }

    case OP_ADVANCE_TIME: {
        uint64_t delta = (rng_next(rng) % 100) + 1;
        log_op(log, op, delta);
        moq_result_t rc = moq_simpair_advance_to(sp, now + delta);
        if (rc != MOQ_OK && rc != MOQ_ERR_CLOSED)
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

static int run_scenario(uint64_t seed, uint64_t alloc_pm,
                         uint64_t transport_pm,
                         run_summary_t *summary, int run_id,
                         int max_steps)
{
    memset(summary, 0, sizeof(*summary));
    summary->trace_hash = 0xCBF29CE484222325ULL;

    fault_alloc_state_t fas = {
        .seed = seed ^ 0xC0AB1DFA017ULL,
        .per_mille = alloc_pm,
    };
    moq_alloc_t alloc = { &fas, fa_alloc, fa_realloc, fa_free };

    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = &alloc;
    cfg.seed = seed;
    cfg.initial_now_us = 1000;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 64;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 64;
    cfg.trace_fn = trace_hash_fn;
    cfg.trace_ctx = summary;
    cfg.fault_per_mille = (uint32_t)transport_pm;
    cfg.fault_flags = MOQ_SIM_FAULT_ALL | MOQ_SIM_FAULT_ALL_INJECT;

    /* Faults are disabled during setup — these must succeed. */
    moq_simpair_t *sp = NULL;
    int failures = 0;
    op_log_t log = {0};
    ctx_t ctx = { seed, run_id, &log, max_steps, alloc_pm, transport_pm };

    moq_result_t rc = moq_simpair_create(&cfg, &sp);
    if (rc != MOQ_OK || !sp) {
        fprintf(stderr, "FAIL seed=0x%llx run=%d: simpair_create rc=%d\n",
                (unsigned long long)seed, run_id, (int)rc);
        dump_log(&ctx);
        if (fas.balance != 0) failures++;
        summary->alloc_calls = fas.calls;
        summary->alloc_failures = fas.failures;
        return failures + 1;
    }

    rc = moq_simpair_start(sp);
    if (rc != MOQ_OK) {
        fprintf(stderr, "FAIL seed=0x%llx run=%d: simpair_start rc=%d\n",
                (unsigned long long)seed, run_id, (int)rc);
        dump_log(&ctx);
        moq_simpair_destroy(sp);
        if (fas.balance != 0) failures++;
        summary->alloc_calls = fas.calls;
        summary->alloc_failures = fas.failures;
        return failures + 1;
    }

    rc = moq_simpair_run_until_quiescent(sp, 8, NULL);
    if (rc != MOQ_OK && rc != MOQ_ERR_WOULD_BLOCK) {
        fprintf(stderr, "FAIL seed=0x%llx run=%d: setup pump rc=%d\n",
                (unsigned long long)seed, run_id, (int)rc);
        dump_log(&ctx);
        moq_simpair_destroy(sp);
        if (fas.balance != 0) failures++;
        summary->alloc_calls = fas.calls;
        summary->alloc_failures = fas.failures;
        return failures + 1;
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
    rc = moq_pub_create(moq_simpair_server(sp), &alloc, &pcfg, &pub);
    if (rc != MOQ_OK || !pub) {
        fprintf(stderr, "FAIL seed=0x%llx run=%d: pub_create rc=%d\n",
                (unsigned long long)seed, run_id, (int)rc);
        dump_log(&ctx);
        moq_simpair_destroy(sp);
        if (fas.balance != 0) failures++;
        summary->alloc_calls = fas.calls;
        summary->alloc_failures = fas.failures;
        return failures + 1;
    }

    fas.enabled = true;
    moq_simpair_enable_faults(sp);

    shadow_state_t st;
    memset(&st, 0, sizeof(st));
    rng_t rng = { seed };

    for (int step = 0; step < max_steps; step++) {
        int sf = execute_step(sp, pub, &st, &rng, &log, &alloc, &ctx);
        if (sf > 0) { failures += sf; break; }

        sf = pump_checked(sp, 4, &ctx, "step_pump");
        if (sf > 0) { failures += sf; break; }
        sf = drain_events(sp, pub, &st, &rng, &ctx);
        if (sf > 0) { failures += sf; break; }

        if (st.closed) {
            if (total_faults(summary) == 0 && fas.failures == 0) {
                fprintf(stderr, "FAIL seed=0x%llx run=%d step=%d: "
                        "closed without any fault\n",
                        (unsigned long long)seed, run_id, step);
                dump_log(&ctx);
                failures++;
            }
            break;
        }
    }

    for (int i = 0; i < MAX_TRACKS; i++) {
        if (st.tracks[i].active && st.tracks[i].group_open) {
            moq_result_t egrc = moq_pub_end_group(pub, st.tracks[i].track,
                moq_simpair_now_us(sp));
            if (!rc_ok_write(egrc))
                failures += note_bad_rc("cleanup_end_group", egrc, &ctx);
        }
    }
    for (int p = 0; p < 8; p++) {
        moq_result_t prc = moq_simpair_run_until_quiescent(sp, 8, NULL);
        if (!rc_ok_pump(prc)) {
            failures += note_bad_rc("cleanup_pump", prc, &ctx);
            break;
        }
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

    if (fas.balance != 0) {
        fprintf(stderr, "FAIL seed=0x%llx run=%d: alloc balance=%lld\n",
                (unsigned long long)seed, run_id, (long long)fas.balance);
        dump_log(&ctx);
        failures++;
    }

    summary->alloc_calls = fas.calls;
    summary->alloc_failures = fas.failures;
    return failures;
}

/* -- Main ----------------------------------------------------------- */

int main(void)
{
    int failures = 0;
    const char *env_seeds = getenv("MOQ_SCENARIO_SEEDS");
    const char *env_start = getenv("MOQ_SCENARIO_SEED_START");
    const char *env_steps = getenv("MOQ_SCENARIO_STEPS");
    const char *env_alloc = getenv("MOQ_SCENARIO_ALLOC_FAULT_PERMILLE");
    const char *env_xport = getenv("MOQ_SCENARIO_TRANSPORT_FAULT_PERMILLE");

    uint64_t num_seeds = env_seeds ? strtoull(env_seeds, NULL, 0) : 100;
    uint64_t seed_start = env_start ? strtoull(env_start, NULL, 0) : 0;
    int max_steps = env_steps ? atoi(env_steps) : 50;
    uint64_t alloc_pm = env_alloc ? strtoull(env_alloc, NULL, 0) : 25;
    uint64_t transport_pm = env_xport ? strtoull(env_xport, NULL, 0) : 40;
    if (alloc_pm > 1000) alloc_pm = 1000;
    if (transport_pm > 1000) transport_pm = 1000;

    size_t total_ctrl = 0, total_data = 0, total_rst = 0, total_stp = 0;
    size_t total_mut_c = 0, total_mut_d = 0, total_reord = 0;
    size_t total_inj_rst = 0, total_inj_stp = 0, total_inj_cls = 0;
    size_t total_trunc_ctl = 0, total_trunc_data = 0;
    uint64_t total_alloc_fail = 0;

    for (uint64_t seed = seed_start; seed < seed_start + num_seeds; seed++) {
        run_summary_t sums[2];

        for (int run = 0; run < 2; run++)
            failures += run_scenario(seed, alloc_pm, transport_pm,
                                      &sums[run], run, max_steps);

        total_ctrl += sums[0].drop_control;
        total_data += sums[0].drop_data;
        total_rst  += sums[0].drop_reset;
        total_stp  += sums[0].drop_stop;
        total_mut_c += sums[0].mutate_control;
        total_mut_d += sums[0].mutate_data;
        total_reord += sums[0].reorder_action;
        total_inj_rst += sums[0].inject_reset;
        total_inj_stp += sums[0].inject_stop;
        total_inj_cls += sums[0].inject_close;
        total_trunc_ctl += sums[0].truncate_control;
        total_trunc_data += sums[0].truncate_data;
        total_alloc_fail += sums[0].alloc_failures;

        if (sums[0].trace_hash != sums[1].trace_hash ||
            sums[0].trace_count != sums[1].trace_count ||
            sums[0].alloc_calls != sums[1].alloc_calls ||
            sums[0].alloc_failures != sums[1].alloc_failures ||
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
            fprintf(stderr, "FAIL seed=0x%llx: nondeterministic\n",
                    (unsigned long long)seed);
            fprintf(stderr, "  run0 trace=0x%llx/%zu alloc=%llu/%llu "
                    "ctrl=%zu data=%zu rst=%zu stp=%zu\n",
                    (unsigned long long)sums[0].trace_hash,
                    sums[0].trace_count,
                    (unsigned long long)sums[0].alloc_calls,
                    (unsigned long long)sums[0].alloc_failures,
                    sums[0].drop_control, sums[0].drop_data,
                    sums[0].drop_reset, sums[0].drop_stop);
            fprintf(stderr, "  run1 trace=0x%llx/%zu alloc=%llu/%llu "
                    "ctrl=%zu data=%zu rst=%zu stp=%zu\n",
                    (unsigned long long)sums[1].trace_hash,
                    sums[1].trace_count,
                    (unsigned long long)sums[1].alloc_calls,
                    (unsigned long long)sums[1].alloc_failures,
                    sums[1].drop_control, sums[1].drop_data,
                    sums[1].drop_reset, sums[1].drop_stop);
            fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx "
                    "MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%d "
                    "MOQ_SCENARIO_ALLOC_FAULT_PERMILLE=%llu "
                    "MOQ_SCENARIO_TRANSPORT_FAULT_PERMILLE=%llu "
                    "./build/tests/test_scenario_combined_faults\n",
                    (unsigned long long)seed, max_steps,
                    (unsigned long long)alloc_pm,
                    (unsigned long long)transport_pm);
            failures++;
        }
    }

    if (num_seeds >= 10) {
        if (alloc_pm > 0 && total_alloc_fail == 0) {
            fprintf(stderr, "FAIL: no allocation faults injected\n");
            failures++;
        }
        /* Skip transport-drop coverage check when allocation fault rate
         * is high enough to starve action generation (100% alloc failure
         * prevents most session operations from producing actions). */
        if (transport_pm > 0 && alloc_pm < 500 &&
            total_ctrl + total_data + total_mut_c + total_mut_d == 0) {
            fprintf(stderr, "FAIL: no transport faults injected\n");
            failures++;
        }
    }

    if (failures == 0)
        fprintf(stderr, "PASS: test_scenario_combined_faults "
                "(%llu seeds, alloc_fail=%llu, "
                "ctrl=%zu data=%zu rst=%zu stp=%zu "
                "mut_c=%zu mut_d=%zu reord=%zu "
                "inj_rst=%zu inj_stp=%zu inj_cls=%zu "
                "trunc_ctl=%zu trunc_data=%zu)\n",
                (unsigned long long)num_seeds,
                (unsigned long long)total_alloc_fail,
                total_ctrl, total_data, total_rst, total_stp,
                total_mut_c, total_mut_d, total_reord,
                total_inj_rst, total_inj_stp, total_inj_cls,
                total_trunc_ctl, total_trunc_data);
    else
        fprintf(stderr, "FAIL: test_scenario_combined_faults "
                "(%d failures in %llu seeds)\n",
                failures, (unsigned long long)num_seeds);

    return failures;
}
