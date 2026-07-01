/*
 * Seeded deterministic fault-injection scenario runner.
 *
 * This is the first BUGGIFY slice: runtime allocation failures under
 * SimPair + publisher facade operations. The allocator fails alloc/realloc
 * calls from a seed-derived predicate; every seed runs twice and must produce
 * the same trace/fault summary with zero leaked allocations.
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

/* -- Trace hash ----------------------------------------------------- */

typedef struct {
    uint64_t trace_hash;
    size_t   trace_count;
    uint64_t alloc_calls;
    uint64_t injected_failures;
} run_summary_t;

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
    if (r->bytes.data && r->bytes.len > 0) {
        for (size_t i = 0; i < r->bytes.len; i++) {
            h ^= r->bytes.data[i];
            h *= 0x100000001B3ULL;
        }
    }
    s->trace_hash = h;
    s->trace_count++;
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

static void *fault_alloc(size_t size, void *ctx)
{
    fault_alloc_state_t *s = (fault_alloc_state_t *)ctx;
    if (fault_should_fail(s, size, 1))
        return NULL;
    void *p = malloc(size);
    if (p) s->balance++;
    return p;
}

static void *fault_realloc(void *ptr, size_t old_size, size_t new_size,
                           void *ctx)
{
    fault_alloc_state_t *s = (fault_alloc_state_t *)ctx;
    (void)old_size;

    if (new_size == 0) {
        if (ptr) {
            s->balance--;
            free(ptr);
        }
        return NULL;
    }
    if (fault_should_fail(s, new_size, 2))
        return NULL;

    void *p = realloc(ptr, new_size);
    if (p && !ptr)
        s->balance++;
    return p;
}

static void fault_free(void *ptr, size_t size, void *ctx)
{
    fault_alloc_state_t *s = (fault_alloc_state_t *)ctx;
    (void)size;
    if (ptr) {
        s->balance--;
        free(ptr);
    }
}

/* -- Shadow model --------------------------------------------------- */

#define MAX_TRACKS 6

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

static const char *op_name(scenario_op_t op)
{
    static const char *names[] = {
        "PUMP", "ADD_TRACK", "REMOVE_TRACK", "SUBSCRIBE",
        "WRITE_OBJECT", "END_GROUP", "DRAIN_EVENTS", "FLUSH",
        "ADVANCE_TIME",
    };
    return op < OP_COUNT ? names[op] : "?";
}

#define MAX_LOG 80
typedef struct { scenario_op_t op; uint64_t p; } log_entry_t;
typedef struct { log_entry_t e[MAX_LOG]; size_t n; } op_log_t;

static void log_op(op_log_t *log, scenario_op_t op, uint64_t p)
{
    if (log->n < MAX_LOG) {
        log->e[log->n].op = op;
        log->e[log->n].p = p;
        log->n++;
    }
}

static void dump_log(uint64_t seed, int run, const op_log_t *log,
                     int max_steps, uint64_t per_mille)
{
    fprintf(stderr, "  seed=0x%llx run=%d ops:\n",
            (unsigned long long)seed, run);
    for (size_t i = 0; i < log->n; i++) {
        fprintf(stderr, "    %zu: %s p=%llu\n", i,
                op_name(log->e[i].op), (unsigned long long)log->e[i].p);
    }
    fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx "
            "MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%d "
            "MOQ_SCENARIO_FAULT_PERMILLE=%llu "
            "./build/tests/test_scenario_faults\n",
            (unsigned long long)seed, max_steps,
            (unsigned long long)per_mille);
}

static bool allowed_fault_rc(moq_result_t rc, bool allow_inval)
{
    return rc == MOQ_OK || rc == MOQ_ERR_NOMEM ||
           rc == MOQ_ERR_WOULD_BLOCK || rc == MOQ_ERR_WRONG_STATE ||
           rc == MOQ_ERR_STALE_HANDLE ||
           (allow_inval && rc == MOQ_ERR_INVAL);
}

static void note_unexpected_rc(const char *where, moq_result_t rc,
                               int *failures)
{
    fprintf(stderr, "  unexpected rc at %s: %d\n", where, (int)rc);
    (*failures)++;
}

static void drain_events(moq_simpair_t *sp, moq_publisher_t *pub,
                         shadow_state_t *st, rng_t *rng, int *failures)
{
    moq_session_t *client = moq_simpair_client(sp);
    moq_session_t *server = moq_simpair_server(sp);
    uint64_t now = moq_simpair_now_us(sp);

    moq_event_t evs[8];
    size_t n;

    while ((n = moq_session_poll_events(client, evs, 8)) > 0) {
        bool responded = false;
        for (size_t i = 0; i < n; i++) {
            if (evs[i].kind == MOQ_EVENT_NAMESPACE_PUBLISHED) {
                if ((rng_next(rng) & 1) == 0) {
                    moq_accept_namespace_cfg_t cfg;
                    moq_accept_namespace_cfg_init(&cfg);
                    moq_result_t rc = moq_session_accept_namespace(client,
                        evs[i].u.namespace_published.ann, &cfg, now);
                    if (!allowed_fault_rc(rc, false))
                        note_unexpected_rc("accept_namespace", rc, failures);
                } else {
                    moq_reject_namespace_cfg_t cfg;
                    moq_reject_namespace_cfg_init(&cfg);
                    cfg.error_code = MOQ_REQUEST_ERROR_UNAUTHORIZED;
                    moq_result_t rc = moq_session_reject_namespace(client,
                        evs[i].u.namespace_published.ann, &cfg, now);
                    if (!allowed_fault_rc(rc, false))
                        note_unexpected_rc("reject_namespace", rc, failures);
                }
                responded = true;
            }
            moq_event_cleanup(&evs[i]);
        }
        if (responded)
            (void)moq_simpair_run_until_quiescent(sp, 4, NULL);
    }

    while ((n = moq_session_poll_events(server, evs, 8)) > 0) {
        for (size_t i = 0; i < n; i++) {
            moq_pub_event_result_t res;
            moq_result_t rc = moq_pub_handle_event(pub, &evs[i], now, &res);
            if (!allowed_fault_rc(rc, false))
                note_unexpected_rc("pub_handle_event", rc, failures);
            moq_event_cleanup(&evs[i]);
        }
        refresh_subscriptions(pub, st);
    }
}

static void drain_actions(moq_simpair_t *sp)
{
    moq_action_t acts[16];
    size_t n;
    while ((n = moq_session_poll_actions(moq_simpair_client(sp), acts, 16)) > 0)
        for (size_t i = 0; i < n; i++) moq_action_cleanup(&acts[i]);
    while ((n = moq_session_poll_actions(moq_simpair_server(sp), acts, 16)) > 0)
        for (size_t i = 0; i < n; i++) moq_action_cleanup(&acts[i]);
}

static int execute_step(moq_simpair_t *sp, moq_publisher_t *pub,
                        shadow_state_t *st, rng_t *rng, op_log_t *log,
                        const moq_alloc_t *alloc)
{
    if (st->closed) return 0;

    int failures = 0;
    scenario_op_t op = (scenario_op_t)(rng_next(rng) % OP_COUNT);
    moq_session_t *client = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);

    switch (op) {
    case OP_PUMP:
        log_op(log, op, 0);
        (void)moq_simpair_run_until_quiescent(sp, 4, NULL);
        break;

    case OP_ADD_TRACK: {
        int slot = -1;
        for (int i = 0; i < MAX_TRACKS; i++) {
            if (!st->tracks[i].active) { slot = i; break; }
        }
        if (slot < 0) {
            log_op(log, OP_PUMP, 0);
            (void)moq_simpair_run_until_quiescent(sp, 4, NULL);
            break;
        }

        char name[16];
        snprintf(name, sizeof(name), "ft%llu",
                 (unsigned long long)st->next_track_id++);

        moq_pub_track_cfg_t cfg;
        moq_pub_track_cfg_init(&cfg);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("fault") };
        cfg.track_namespace.parts = ns_parts;
        cfg.track_namespace.count = 1;
        cfg.track_name = moq_bytes_cstr(name);
        cfg.advertise_namespace = (rng_next(rng) % 4) == 0;

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
        } else if (!allowed_fault_rc(rc, true)) {
            note_unexpected_rc("pub_add_track", rc, &failures);
        }
        break;
    }

    case OP_REMOVE_TRACK: {
        int idx = pick_track(st, rng, false, false);
        if (idx < 0) {
            log_op(log, OP_PUMP, 0);
            break;
        }
        log_op(log, op, (uint64_t)idx);
        moq_result_t rc = moq_pub_remove_track(pub, st->tracks[idx].track, now);
        if (rc == MOQ_OK) {
            memset(&st->tracks[idx], 0, sizeof(st->tracks[idx]));
        } else if (!allowed_fault_rc(rc, false)) {
            note_unexpected_rc("pub_remove_track", rc, &failures);
        }
        break;
    }

    case OP_SUBSCRIBE: {
        int idx = pick_track(st, rng, false, false);
        if (idx < 0 || st->tracks[idx].has_sub) {
            log_op(log, OP_PUMP, 0);
            break;
        }

        moq_subscribe_cfg_t cfg;
        moq_subscribe_cfg_init(&cfg);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("fault") };
        cfg.track_namespace.parts = ns_parts;
        cfg.track_namespace.count = 1;
        cfg.track_name = moq_bytes_cstr(st->tracks[idx].name);

        moq_subscription_t sub;
        moq_result_t rc = moq_session_subscribe(client, &cfg, now, &sub);
        log_op(log, op, rc == MOQ_OK ? (uint64_t)idx : UINT64_MAX);
        if (rc == MOQ_OK) {
            (void)moq_simpair_run_until_quiescent(sp, 4, NULL);
            drain_events(sp, pub, st, rng, &failures);
            (void)moq_simpair_run_until_quiescent(sp, 4, NULL);
            drain_events(sp, pub, st, rng, &failures);
        } else if (!allowed_fault_rc(rc, true)) {
            note_unexpected_rc("session_subscribe", rc, &failures);
        }
        break;
    }

    case OP_WRITE_OBJECT: {
        int idx = pick_track(st, rng, true, false);
        if (idx < 0) {
            log_op(log, OP_PUMP, 0);
            break;
        }

        uint8_t payload_data[16];
        size_t payload_len = (rng_next(rng) % sizeof(payload_data)) + 1;
        for (size_t i = 0; i < payload_len; i++)
            payload_data[i] = (uint8_t)(rng_next(rng) & 0xFF);

        moq_rcbuf_t *payload = NULL;
        moq_result_t rc = moq_rcbuf_create(alloc, payload_data, payload_len,
                                           &payload);
        if (rc != MOQ_OK) {
            log_op(log, op, UINT64_MAX);
            break;
        }

        shadow_track_t *t = &st->tracks[idx];
        uint64_t obj = t->object_id;
        rc = moq_pub_write_object(pub, t->track, t->group_id, obj,
                                  payload, now);
        log_op(log, op, rc == MOQ_OK ? obj : UINT64_MAX);
        if (rc == MOQ_OK) {
            t->object_id++;
            t->group_open = true;
        } else if (!allowed_fault_rc(rc, false)) {
            note_unexpected_rc("pub_write_object", rc, &failures);
        }
        moq_rcbuf_decref(payload);
        break;
    }

    case OP_END_GROUP: {
        int idx = pick_track(st, rng, true, true);
        if (idx < 0) {
            log_op(log, OP_PUMP, 0);
            break;
        }
        shadow_track_t *t = &st->tracks[idx];
        moq_result_t rc = moq_pub_end_group(pub, t->track, now);
        log_op(log, op, rc == MOQ_OK ? t->group_id : UINT64_MAX);
        if (rc == MOQ_OK) {
            t->group_id++;
            t->object_id = 0;
            t->group_open = false;
        } else if (!allowed_fault_rc(rc, false)) {
            note_unexpected_rc("pub_end_group", rc, &failures);
        }
        break;
    }

    case OP_DRAIN_EVENTS:
        log_op(log, op, 0);
        drain_events(sp, pub, st, rng, &failures);
        break;

    case OP_FLUSH: {
        log_op(log, op, 0);
        moq_result_t rc = moq_pub_flush(pub, now);
        if (!allowed_fault_rc(rc, false))
            note_unexpected_rc("pub_flush", rc, &failures);
        refresh_subscriptions(pub, st);
        break;
    }

    case OP_ADVANCE_TIME: {
        uint64_t delta = (rng_next(rng) % 100) + 1;
        log_op(log, op, delta);
        (void)moq_simpair_advance_to(sp, now + delta);
        break;
    }

    default:
        break;
    }

    if (moq_session_state(moq_simpair_client(sp)) == MOQ_SESS_CLOSED ||
        moq_session_state(moq_simpair_server(sp)) == MOQ_SESS_CLOSED) {
        st->closed = true;
    }
    return failures;
}

static int run_scenario(uint64_t seed, uint64_t per_mille,
                        run_summary_t *summary, int run_id,
                        int max_steps)
{
    summary->trace_hash = 0xCBF29CE484222325ULL;
    summary->trace_count = 0;
    summary->alloc_calls = 0;
    summary->injected_failures = 0;

    fault_alloc_state_t fas = {
        .seed = seed ^ 0x51A7E5D00DULL,
        .per_mille = per_mille,
    };
    moq_alloc_t alloc = { &fas, fault_alloc, fault_realloc, fault_free };

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

    moq_simpair_t *sp = NULL;
    int failures = 0;
    moq_result_t rc = moq_simpair_create(&cfg, &sp);
    if (rc != MOQ_OK || !sp) {
        if (fas.balance != 0)
            failures++;
        summary->alloc_calls = fas.calls;
        summary->injected_failures = fas.failures;
        return failures;
    }

    rc = moq_simpair_start(sp);
    if (rc == MOQ_OK)
        (void)moq_simpair_run_until_quiescent(sp, 8, NULL);

    moq_event_t ev;
    if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
        moq_event_cleanup(&ev);
    if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1)
        moq_event_cleanup(&ev);

    moq_pub_cfg_t pcfg;
    moq_pub_cfg_init_sized(&pcfg, sizeof(pcfg));
    pcfg.accept_mode = MOQ_PUB_ACCEPT_ALL;

    moq_publisher_t *pub = NULL;
    rc = moq_pub_create(moq_simpair_server(sp), &alloc, &pcfg, &pub);
    if (rc != MOQ_OK || !pub) {
        moq_simpair_destroy(sp);
        if (fas.balance != 0)
            failures++;
        summary->alloc_calls = fas.calls;
        summary->injected_failures = fas.failures;
        return failures;
    }

    fas.enabled = true;

    shadow_state_t st;
    memset(&st, 0, sizeof(st));
    rng_t rng = { seed };
    op_log_t log = {0};

    for (int step = 0; step < max_steps; step++) {
        int step_failures = execute_step(sp, pub, &st, &rng, &log, &alloc);
        if (step_failures > 0) {
            failures += step_failures;
            dump_log(seed, run_id, &log, max_steps, per_mille);
            break;
        }
        (void)moq_simpair_run_until_quiescent(sp, 4, NULL);
        int before_drain = failures;
        drain_events(sp, pub, &st, &rng, &failures);
        if (failures > before_drain) {
            dump_log(seed, run_id, &log, max_steps, per_mille);
            break;
        }
        if (st.closed) {
            if (fas.failures == 0) {
                fprintf(stderr, "FAIL seed=0x%llx run=%d step=%d: "
                        "closed without injected fault\n",
                        (unsigned long long)seed, run_id, step);
                dump_log(seed, run_id, &log, max_steps, per_mille);
                failures++;
            }
            break;
        }
    }

    for (int i = 0; i < MAX_TRACKS; i++) {
        if (st.tracks[i].active && st.tracks[i].group_open)
            (void)moq_pub_end_group(pub, st.tracks[i].track,
                                    moq_simpair_now_us(sp));
    }

    for (int p = 0; p < 8; p++)
        (void)moq_simpair_run_until_quiescent(sp, 8, NULL);
    drain_events(sp, pub, &st, &rng, &failures);
    drain_actions(sp);

    moq_pub_destroy(pub);
    moq_simpair_destroy(sp);

    if (fas.balance != 0) {
        fprintf(stderr, "FAIL seed=0x%llx run=%d: alloc balance=%lld\n",
                (unsigned long long)seed, run_id, (long long)fas.balance);
        dump_log(seed, run_id, &log, max_steps, per_mille);
        failures++;
    }

    summary->alloc_calls = fas.calls;
    summary->injected_failures = fas.failures;
    return failures;
}

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
    uint64_t per_mille = env_fault ? (uint64_t)strtoull(env_fault, NULL, 0) : 30;
    if (per_mille > 1000)
        per_mille = 1000;

    uint64_t total_injected = 0;

    for (uint64_t seed = seed_start; seed < seed_start + num_seeds; seed++) {
        run_summary_t sums[2];
        for (int run = 0; run < 2; run++)
            failures += run_scenario(seed, per_mille, &sums[run], run,
                                     max_steps);

        total_injected += sums[0].injected_failures;

        if (sums[0].trace_hash != sums[1].trace_hash ||
            sums[0].trace_count != sums[1].trace_count ||
            sums[0].alloc_calls != sums[1].alloc_calls ||
            sums[0].injected_failures != sums[1].injected_failures) {
            fprintf(stderr, "FAIL seed=0x%llx: nondeterministic fault run\n",
                    (unsigned long long)seed);
            fprintf(stderr, "  run0 trace=0x%llx/%zu alloc=%llu fail=%llu\n",
                    (unsigned long long)sums[0].trace_hash,
                    sums[0].trace_count,
                    (unsigned long long)sums[0].alloc_calls,
                    (unsigned long long)sums[0].injected_failures);
            fprintf(stderr, "  run1 trace=0x%llx/%zu alloc=%llu fail=%llu\n",
                    (unsigned long long)sums[1].trace_hash,
                    sums[1].trace_count,
                    (unsigned long long)sums[1].alloc_calls,
                    (unsigned long long)sums[1].injected_failures);
            fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx "
                    "MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%d "
                    "MOQ_SCENARIO_FAULT_PERMILLE=%llu "
                    "./build/tests/test_scenario_faults\n",
                    (unsigned long long)seed, max_steps,
                    (unsigned long long)per_mille);
            failures++;
        }
    }

    if (per_mille > 0 && num_seeds >= 10 && total_injected == 0) {
        fprintf(stderr, "FAIL: no allocation faults injected across %llu seeds\n",
                (unsigned long long)num_seeds);
        failures++;
    }

    if (failures == 0) {
        fprintf(stderr, "PASS: test_scenario_faults (%llu seeds, %llu faults)\n",
                (unsigned long long)num_seeds,
                (unsigned long long)total_injected);
    } else {
        fprintf(stderr, "FAIL: test_scenario_faults (%d failures in %llu seeds)\n",
                failures, (unsigned long long)num_seeds);
    }
    return failures;
}
