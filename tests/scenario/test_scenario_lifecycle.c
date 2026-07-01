/*
 * Deterministic subscribe + namespace lifecycle scenario runner.
 *
 * Generates seeded sequences of subscribe/accept/reject and
 * namespace publish/accept/reject/cancel/done operations through
 * SimPair. Each seed runs twice; trace hash must match.
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

/* -- Trace hash ----------------------------------------------------- */

typedef struct {
    uint64_t hash;
    size_t   count;
    size_t   unsub_sent;
    size_t   unsub_events;
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

#define MAX_SUBS 8
#define MAX_ANNS 8

typedef struct {
    bool active;
    bool pending;
    bool accepted;
    moq_subscription_t server_handle;
    moq_subscription_t client_handle;
} shadow_sub_t;

typedef struct {
    bool active;
    bool pending;
    bool accepted;
    moq_announcement_t client_handle;
    moq_announcement_t server_handle;
} shadow_ann_t;

typedef struct {
    shadow_sub_t subs[MAX_SUBS];
    shadow_ann_t anns[MAX_ANNS];
    bool closed;
} shadow_state_t;

static int shadow_find_pending_sub(const shadow_state_t *st) {
    for (int i = 0; i < MAX_SUBS; i++)
        if (st->subs[i].active && st->subs[i].pending) return i;
    return -1;
}

static int shadow_find_pending_ann(const shadow_state_t *st) {
    for (int i = 0; i < MAX_ANNS; i++)
        if (st->anns[i].active && st->anns[i].pending) return i;
    return -1;
}

static int shadow_find_accepted_sub(const shadow_state_t *st) {
    for (int i = 0; i < MAX_SUBS; i++)
        if (st->subs[i].active && st->subs[i].accepted) return i;
    return -1;
}

static int shadow_find_accepted_ann(const shadow_state_t *st) {
    for (int i = 0; i < MAX_ANNS; i++)
        if (st->anns[i].active && st->anns[i].accepted) return i;
    return -1;
}

static int shadow_find_accepted_ann_server(const shadow_state_t *st) {
    for (int i = 0; i < MAX_ANNS; i++)
        if (st->anns[i].active && st->anns[i].accepted) return i;
    return -1;
}

/* -- Operations ----------------------------------------------------- */

typedef enum {
    OP_PUMP,
    OP_SUBSCRIBE,
    OP_ACCEPT_SUBSCRIBE,
    OP_REJECT_SUBSCRIBE,
    OP_PUBLISH_NAMESPACE,
    OP_ACCEPT_NAMESPACE,
    OP_REJECT_NAMESPACE,
    OP_CANCEL_NAMESPACE,
    OP_DONE_NAMESPACE,
    OP_UNSUBSCRIBE,
    OP_DRAIN_CLIENT_EVENTS,
    OP_DRAIN_SERVER_EVENTS,
    OP_ADVANCE_TIME,
    OP_COUNT,
} scenario_op_t;

static const char *op_name(scenario_op_t op) {
    static const char *names[] = {
        "PUMP","SUBSCRIBE","ACCEPT_SUB","REJECT_SUB",
        "PUB_NS","ACCEPT_NS","REJECT_NS","CANCEL_NS",
        "DONE_NS","UNSUB","DRAIN_C_EV","DRAIN_S_EV","ADV_TIME",
    };
    return op < OP_COUNT ? names[op] : "?";
}

/* -- Operation log -------------------------------------------------- */

#define MAX_LOG 64
typedef struct { scenario_op_t op; uint64_t p; } log_entry_t;
typedef struct { log_entry_t e[MAX_LOG]; size_t n; } op_log_t;

static void log_op(op_log_t *l, scenario_op_t op, uint64_t p) {
    if (l->n < MAX_LOG) { l->e[l->n].op = op; l->e[l->n].p = p; l->n++; }
}

static void dump_log(uint64_t seed, int run, const op_log_t *l) {
    fprintf(stderr, "  seed=0x%llx run=%d ops:\n",
            (unsigned long long)seed, run);
    for (size_t i = 0; i < l->n; i++)
        fprintf(stderr, "    %zu: %s p=%llu\n", i,
                op_name(l->e[i].op), (unsigned long long)l->e[i].p);
}

/* -- Execute one step ----------------------------------------------- */

static void execute_step(moq_simpair_t *sp, shadow_state_t *st,
                          rng_t *rng, op_log_t *log,
                          trace_summary_t *ts)
{
    if (st->closed) return;

    scenario_op_t op = (scenario_op_t)(rng_next(rng) % OP_COUNT);
    moq_session_t *client = moq_simpair_client(sp);
    moq_session_t *server = moq_simpair_server(sp);

    switch (op) {
    case OP_PUMP:
        log_op(log, op, 0);
        moq_simpair_run_until_quiescent(sp, 4, NULL);
        break;

    case OP_SUBSCRIBE: {
        char name[16];
        uint64_t r = rng_next(rng);
        snprintf(name, sizeof(name), "t%llu", (unsigned long long)(r % 1000));

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t cfg;
        moq_subscribe_cfg_init(&cfg);
        cfg.track_namespace = ns;
        cfg.track_name = moq_bytes_cstr(name);
        cfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;

        moq_subscription_t h;
        moq_result_t rc = moq_session_subscribe(client, &cfg,
            moq_simpair_now_us(sp), &h);
        log_op(log, op, rc >= 0 ? 1 : 0);

        if (rc == MOQ_OK) {
            moq_simpair_run_until_quiescent(sp, 4, NULL);

            moq_event_t ev;
            if (moq_session_poll_events(server, &ev, 1) == 1) {
                if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                    for (int i = 0; i < MAX_SUBS; i++) {
                        if (!st->subs[i].active) {
                            st->subs[i].active = true;
                            st->subs[i].pending = true;
                            st->subs[i].accepted = false;
                            st->subs[i].server_handle = ev.u.subscribe_request.sub;
                            st->subs[i].client_handle = h;
                            break;
                        }
                    }
                }
                moq_event_cleanup(&ev);
            }
        }
        break;
    }

    case OP_ACCEPT_SUBSCRIBE: {
        int idx = shadow_find_pending_sub(st);
        if (idx < 0) { log_op(log, OP_PUMP, 0); break; }

        log_op(log, op, (uint64_t)idx);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_result_t rc = moq_session_accept_subscribe(server,
            st->subs[idx].server_handle, &acc, moq_simpair_now_us(sp));

        if (rc == MOQ_OK) {
            st->subs[idx].pending = false;
            st->subs[idx].accepted = true;
            moq_simpair_run_until_quiescent(sp, 4, NULL);
            moq_event_t ev;
            if (moq_session_poll_events(client, &ev, 1) == 1)
                moq_event_cleanup(&ev);
        }
        break;
    }

    case OP_REJECT_SUBSCRIBE: {
        int idx = shadow_find_pending_sub(st);
        if (idx < 0) { log_op(log, OP_PUMP, 0); break; }

        log_op(log, op, (uint64_t)idx);
        moq_reject_subscribe_cfg_t rej;
        moq_reject_subscribe_cfg_init(&rej);
        rej.error_code = MOQ_REQUEST_ERROR_DOES_NOT_EXIST;
        rej.reason = MOQ_BYTES_LITERAL("no");
        moq_result_t rc = moq_session_reject_subscribe(server,
            st->subs[idx].server_handle, &rej, moq_simpair_now_us(sp));

        if (rc == MOQ_OK) {
            st->subs[idx].active = false;
            st->subs[idx].pending = false;
            moq_simpair_run_until_quiescent(sp, 4, NULL);
            moq_event_t ev;
            if (moq_session_poll_events(client, &ev, 1) == 1)
                moq_event_cleanup(&ev);
        }
        break;
    }

    case OP_PUBLISH_NAMESPACE: {
        char ns_name[16];
        uint64_t r = rng_next(rng);
        snprintf(ns_name, sizeof(ns_name), "ns%llu", (unsigned long long)(r % 100));

        moq_publish_namespace_cfg_t cfg;
        moq_publish_namespace_cfg_init(&cfg);
        moq_bytes_t ns_parts[] = { moq_bytes_cstr(ns_name) };
        cfg.track_namespace.parts = ns_parts;
        cfg.track_namespace.count = 1;

        moq_announcement_t h;
        moq_result_t rc = moq_session_publish_namespace(server, &cfg,
            moq_simpair_now_us(sp), &h);
        log_op(log, op, rc >= 0 ? 1 : 0);

        if (rc == MOQ_OK) {
            moq_simpair_run_until_quiescent(sp, 4, NULL);

            moq_event_t ev;
            if (moq_session_poll_events(client, &ev, 1) == 1) {
                if (ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED) {
                    for (int i = 0; i < MAX_ANNS; i++) {
                        if (!st->anns[i].active) {
                            st->anns[i].active = true;
                            st->anns[i].pending = true;
                            st->anns[i].accepted = false;
                            st->anns[i].client_handle = ev.u.namespace_published.ann;
                            st->anns[i].server_handle = h;
                            break;
                        }
                    }
                }
                moq_event_cleanup(&ev);
            }
        }
        break;
    }

    case OP_ACCEPT_NAMESPACE: {
        int idx = shadow_find_pending_ann(st);
        if (idx < 0) { log_op(log, OP_PUMP, 0); break; }

        log_op(log, op, (uint64_t)idx);
        moq_accept_namespace_cfg_t acc;
        moq_accept_namespace_cfg_init(&acc);
        moq_result_t rc = moq_session_accept_namespace(client,
            st->anns[idx].client_handle, &acc, moq_simpair_now_us(sp));

        if (rc == MOQ_OK) {
            st->anns[idx].pending = false;
            st->anns[idx].accepted = true;
            moq_simpair_run_until_quiescent(sp, 4, NULL);
            moq_event_t ev;
            if (moq_session_poll_events(server, &ev, 1) == 1)
                moq_event_cleanup(&ev);
        }
        break;
    }

    case OP_REJECT_NAMESPACE: {
        int idx = shadow_find_pending_ann(st);
        if (idx < 0) { log_op(log, OP_PUMP, 0); break; }

        log_op(log, op, (uint64_t)idx);
        moq_reject_namespace_cfg_t rej;
        moq_reject_namespace_cfg_init(&rej);
        rej.error_code = MOQ_REQUEST_ERROR_DOES_NOT_EXIST;
        moq_result_t rc = moq_session_reject_namespace(client,
            st->anns[idx].client_handle, &rej, moq_simpair_now_us(sp));

        if (rc == MOQ_OK) {
            st->anns[idx].active = false;
            moq_simpair_run_until_quiescent(sp, 4, NULL);
            moq_event_t ev;
            if (moq_session_poll_events(server, &ev, 1) == 1)
                moq_event_cleanup(&ev);
        }
        break;
    }

    case OP_CANCEL_NAMESPACE: {
        int idx = shadow_find_accepted_ann(st);
        if (idx < 0) { log_op(log, OP_PUMP, 0); break; }

        log_op(log, op, (uint64_t)idx);
        moq_cancel_namespace_cfg_t cfg;
        moq_cancel_namespace_cfg_init(&cfg);
        cfg.error_code = MOQ_REQUEST_ERROR_INTERNAL_ERROR;
        moq_result_t rc = moq_session_cancel_namespace(client,
            st->anns[idx].client_handle, &cfg, moq_simpair_now_us(sp));

        if (rc == MOQ_OK) {
            st->anns[idx].active = false;
            moq_simpair_run_until_quiescent(sp, 4, NULL);
            moq_event_t ev;
            if (moq_session_poll_events(server, &ev, 1) == 1)
                moq_event_cleanup(&ev);
        }
        break;
    }

    case OP_DONE_NAMESPACE: {
        int idx = shadow_find_accepted_ann_server(st);
        if (idx < 0) { log_op(log, OP_PUMP, 0); break; }

        log_op(log, op, (uint64_t)idx);
        moq_result_t rc = moq_session_publish_namespace_done(server,
            st->anns[idx].server_handle, moq_simpair_now_us(sp));

        if (rc == MOQ_OK) {
            st->anns[idx].active = false;
            moq_simpair_run_until_quiescent(sp, 4, NULL);
            moq_event_t ev;
            if (moq_session_poll_events(client, &ev, 1) == 1)
                moq_event_cleanup(&ev);
        }
        break;
    }

    case OP_UNSUBSCRIBE: {
        int idx = shadow_find_accepted_sub(st);
        if (idx < 0) { log_op(log, OP_PUMP, 0); break; }

        log_op(log, op, (uint64_t)idx);
        moq_result_t rc = moq_session_unsubscribe(client,
            st->subs[idx].client_handle, moq_simpair_now_us(sp));
        if (rc == MOQ_OK) {
            ts->unsub_sent++;
            st->subs[idx].active = false;
            moq_simpair_run_until_quiescent(sp, 4, NULL);
            moq_event_t ev;
            if (moq_session_poll_events(server, &ev, 1) == 1) {
                if (ev.kind == MOQ_EVENT_UNSUBSCRIBED)
                    ts->unsub_events++;
                moq_event_cleanup(&ev);
            }
        }
        break;
    }

    case OP_DRAIN_CLIENT_EVENTS: {
        log_op(log, op, 0);
        moq_event_t evts[8];
        size_t ne = moq_session_poll_events(client, evts, 8);
        for (size_t i = 0; i < ne; i++) moq_event_cleanup(&evts[i]);
        break;
    }

    case OP_DRAIN_SERVER_EVENTS: {
        log_op(log, op, 0);
        moq_event_t evts[8];
        size_t ne = moq_session_poll_events(server, evts, 8);
        for (size_t i = 0; i < ne; i++) moq_event_cleanup(&evts[i]);
        break;
    }

    case OP_ADVANCE_TIME: {
        uint64_t delta = (rng_next(rng) % 100) + 1;
        log_op(log, op, delta);
        moq_simpair_advance_to(sp, moq_simpair_now_us(sp) + delta);
        break;
    }

    default:
        log_op(log, OP_PUMP, 0);
        break;
    }
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
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    {
        moq_event_t ev;
        if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);
        if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);
    }

    /* Deterministic prelude: subscribe → accept → unsubscribe. */
    {
        moq_session_t *pc = moq_simpair_client(sp);
        moq_session_t *ps = moq_simpair_server(sp);
        moq_event_t pev;

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("pre");
        sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t pre_csub;
        if (moq_session_subscribe(pc, &sub_cfg, moq_simpair_now_us(sp),
                &pre_csub) == MOQ_OK) {
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            if (moq_session_poll_events(ps, &pev, 1) == 1) {
                if (pev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                    moq_accept_subscribe_cfg_t acc;
                    moq_accept_subscribe_cfg_init(&acc);
                    moq_session_accept_subscribe(ps,
                        pev.u.subscribe_request.sub, &acc,
                        moq_simpair_now_us(sp));
                    moq_simpair_run_until_quiescent(sp, 8, NULL);
                    if (moq_session_poll_events(pc, &pev, 1) == 1)
                        moq_event_cleanup(&pev);

                    if (moq_session_unsubscribe(pc, pre_csub,
                            moq_simpair_now_us(sp)) == MOQ_OK) {
                        summary->unsub_sent++;
                        moq_simpair_run_until_quiescent(sp, 8, NULL);
                        if (moq_session_poll_events(ps, &pev, 1) == 1) {
                            if (pev.kind == MOQ_EVENT_UNSUBSCRIBED)
                                summary->unsub_events++;
                            moq_event_cleanup(&pev);
                        }
                    }
                } else {
                    moq_event_cleanup(&pev);
                }
            }
        }
        /* Drain any remaining prelude state. */
        { moq_event_t d[8]; size_t ne;
          while ((ne = moq_session_poll_events(pc, d, 8)) > 0)
              for (size_t i = 0; i < ne; i++) moq_event_cleanup(&d[i]);
          while ((ne = moq_session_poll_events(ps, d, 8)) > 0)
              for (size_t i = 0; i < ne; i++) moq_event_cleanup(&d[i]);
          moq_action_t a[8]; size_t na;
          while ((na = moq_session_poll_actions(ps, a, 8)) > 0)
              for (size_t i = 0; i < na; i++) moq_action_cleanup(&a[i]);
          while ((na = moq_session_poll_actions(pc, a, 8)) > 0)
              for (size_t i = 0; i < na; i++) moq_action_cleanup(&a[i]);
        }
    }

    shadow_state_t st;
    memset(&st, 0, sizeof(st));
    rng_t rng = { seed };
    op_log_t log = {0};
    int failures = 0;

    for (int step = 0; step < max_steps; step++) {
        execute_step(sp, &st, &rng, &log, summary);

        if (moq_session_state(moq_simpair_client(sp)) == MOQ_SESS_CLOSED ||
            moq_session_state(moq_simpair_server(sp)) == MOQ_SESS_CLOSED) {
            if (!st.closed) {
                fprintf(stderr, "FAIL seed=0x%llx run=%d step=%d: "
                        "unexpected session close\n",
                        (unsigned long long)seed, run_id, step);
                dump_log(seed, run_id, &log);
                fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx "
                        "MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%d "
                        "./build/tests/test_scenario_lifecycle\n",
                        (unsigned long long)seed, max_steps);
                failures++;
            }
            st.closed = true;
            break;
        }
    }

    {
        moq_event_t drain[16];
        size_t ne;
        while ((ne = moq_session_poll_events(moq_simpair_client(sp), drain, 16)) > 0)
            for (size_t i = 0; i < ne; i++) moq_event_cleanup(&drain[i]);
        while ((ne = moq_session_poll_events(moq_simpair_server(sp), drain, 16)) > 0)
            for (size_t i = 0; i < ne; i++) moq_event_cleanup(&drain[i]);
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

    size_t total_unsub_sent = 0, total_unsub_events = 0;

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
                        "./build/tests/test_scenario_lifecycle\n",
                        (unsigned long long)seed, max_steps);
                failures++;
            }
        }

        if (sums[0].hash != sums[1].hash ||
            sums[0].count != sums[1].count ||
            sums[0].unsub_sent != sums[1].unsub_sent ||
            sums[0].unsub_events != sums[1].unsub_events) {
            fprintf(stderr, "FAIL seed=0x%llx: trace/counter mismatch\n",
                    (unsigned long long)seed);
            fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx "
                    "MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%d "
                    "./build/tests/test_scenario_lifecycle\n",
                    (unsigned long long)seed, max_steps);
            failures++;
        }

        total_unsub_sent += sums[0].unsub_sent;
        total_unsub_events += sums[0].unsub_events;
    }

    if (num_seeds >= 10) {
        if (total_unsub_sent == 0) {
            fprintf(stderr, "FAIL: no unsubscribe_sent across %llu seeds\n",
                    (unsigned long long)num_seeds);
            failures++;
        }
        if (total_unsub_events == 0) {
            fprintf(stderr, "FAIL: no unsubscribed_events across %llu seeds\n",
                    (unsigned long long)num_seeds);
            failures++;
        }
    }

    if (failures == 0)
        fprintf(stderr, "PASS: test_scenario_lifecycle "
                "(%llu seeds, unsub_sent=%zu unsub_events=%zu)\n",
                (unsigned long long)num_seeds,
                total_unsub_sent, total_unsub_events);
    else
        fprintf(stderr, "FAIL: test_scenario_lifecycle (%d failures in %llu seeds)\n",
                failures, (unsigned long long)num_seeds);

    return failures;
}
