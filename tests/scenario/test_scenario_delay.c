/*
 * Deterministic delay fault scenario runner.
 *
 * Opts into MOQ_SIM_FAULT_DELAY explicitly (not via FAULT_ALL) because
 * delay is time-domain: the runner must advance virtual time past
 * deadlines to drain delayed inputs. Combined with SPLIT_CONTROL,
 * SPLIT_DATA, and SPLIT_BIDI for maximal parser + timing coverage.
 *
 * Deterministic prelude: subscribe, accept, write object, close
 * subgroup, namespace-sub roundtrip — all through delayed delivery
 * with deadline advancement. Exact payload oracle.
 *
 * Random phase: seeded operations with deadline-aware pumping.
 * Each seed runs twice; trace hash must match.
 */
#include <moq/sim.h>
#include "../../tests/unit/test_support.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#define REPLAY(seed, ms) fprintf(stderr, \
    "  Replay: MOQ_SCENARIO_SEED_START=0x%llx " \
    "MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%d " \
    "./build/tests/test_scenario_delay\n", \
    (unsigned long long)(seed), (ms))

typedef struct { uint64_t s; } rng_t;
static uint64_t rng_next(rng_t *r) {
    uint64_t z = (r->s += 0x9E3779B97F4A7C15ULL);
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    return z ^ (z >> 31);
}

typedef struct {
    uint64_t hash;
    size_t count;
    size_t delay_enqueue;
    size_t delay_stale;
    size_t objects;
} trace_t;

#define MIX(h,v) do { (h) ^= (v); (h) *= 0x100000001B3ULL; } while(0)
#define INIT_HASH 0xCBF29CE484222325ULL

static void trace_fn(void *ctx, const moq_sim_trace_record_t *r) {
    trace_t *s = (trace_t *)ctx;
    uint64_t h = s->hash;
    MIX(h,r->seed); MIX(h,r->step); MIX(h,r->now_us);
    MIX(h,(uint64_t)r->kind); MIX(h,(uint64_t)r->from);
    MIX(h,(uint64_t)r->to); MIX(h,(uint64_t)r->action_kind);
    MIX(h,(uint64_t)r->input_kind); MIX(h,(uint64_t)r->result);
    MIX(h,r->code); MIX(h,r->bytes.len);
    if (r->bytes.data && r->bytes.len > 0)
        for (size_t i = 0; i < r->bytes.len; i++) MIX(h,r->bytes.data[i]);
    s->hash = h; s->count++;
    if (r->kind == MOQ_SIM_TRACE_DELAY_ENQUEUE) s->delay_enqueue++;
    if (r->kind == MOQ_SIM_TRACE_DELAY_STALE)   s->delay_stale++;
}

typedef struct { int64_t balance; } scen_alloc_t;
static void *sa_alloc(size_t sz, void *ctx) {
    void *p = malloc(sz); if (p) ((scen_alloc_t*)ctx)->balance++; return p;
}
static void *sa_realloc(void *p, size_t o, size_t n, void *ctx) {
    (void)o;
    if (!p) { void *r = realloc(NULL, n); if (r) ((scen_alloc_t*)ctx)->balance++; return r; }
    return realloc(p, n);
}
static void sa_free(void *p, size_t sz, void *ctx) {
    (void)sz; if (p) ((scen_alloc_t*)ctx)->balance--; free(p);
}

static void drain_deadlines(moq_simpair_t *sp, int max_advances) {
    for (int i = 0; i < max_advances; i++) {
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        uint64_t dl = moq_simpair_next_deadline_us(sp);
        if (dl == UINT64_MAX || dl <= moq_simpair_now_us(sp)) break;
        moq_simpair_advance_to(sp, dl);
    }
    moq_simpair_run_until_quiescent(sp, 16, NULL);
}

static void drain_events(moq_session_t *s) {
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) == 1)
        moq_event_cleanup(&ev);
}

static void drain_actions(moq_session_t *s) {
    moq_action_t acts[16]; size_t na;
    while ((na = moq_session_poll_actions(s, acts, 16)) > 0)
        for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
}

typedef enum {
    PHASE_SETUP, PHASE_SUBSCRIBE, PHASE_OBJECT, PHASE_NS_PUB,
    PHASE_NS_SUB, PHASE_RANDOM, PHASE_DRAIN, PHASE_END
} phase_t;

static const char *phase_name(phase_t p) {
    static const char *n[] = {
        "setup","subscribe","object","ns_pub",
        "ns_sub","random","drain","end"
    };
    return p < PHASE_END + 1 ? n[p] : "?";
}

typedef struct {
    uint64_t now_us;
    uint64_t next_deadline_us;
    size_t   delayed_count;
    phase_t  phase;
    int      step;
    int      op;
} fail_diag_t;

static int run_seed(uint64_t seed, int max_steps, trace_t *ts,
                    size_t *out_pre_objects, fail_diag_t *diag)
{
    scen_alloc_t sa = {0};
    moq_alloc_t alloc = { &sa, sa_alloc, sa_realloc, sa_free };

    memset(ts, 0, sizeof(*ts));
    ts->hash = INIT_HASH;
    memset(diag, 0, sizeof(*diag));
    diag->step = -1;
    diag->op = -1;

#define FAIL_WITH(sp, code) do { \
    diag->now_us = moq_simpair_now_us(sp); \
    diag->next_deadline_us = moq_simpair_next_deadline_us(sp); \
    diag->delayed_count = moq_simpair_delayed_count(sp); \
    drain_actions(moq_simpair_client(sp)); \
    drain_actions(moq_simpair_server(sp)); \
    moq_simpair_destroy(sp); \
    return (sa.balance == 0) ? (code) : (code) - 1; \
} while(0)

#define SET_PHASE(p) diag->phase = (p)

    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = &alloc;
    cfg.seed = seed;
    cfg.initial_now_us = 1000;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 16;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 16;
    cfg.fault_per_mille = 500;
    cfg.fault_flags = MOQ_SIM_FAULT_DELAY |
                      MOQ_SIM_FAULT_SPLIT_CONTROL |
                      MOQ_SIM_FAULT_SPLIT_DATA |
                      MOQ_SIM_FAULT_SPLIT_BIDI;
    cfg.trace_fn = trace_fn;
    cfg.trace_ctx = ts;

    moq_simpair_t *sp = NULL;
    if (moq_simpair_create(&cfg, &sp) < 0) return -1;
    moq_simpair_start(sp);
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);

    drain_events(cl);
    drain_events(sv);
    moq_simpair_enable_faults(sp);

    /* ---- Prelude: subscribe + object delivery through delay -------- */
    SET_PHASE(PHASE_SUBSCRIBE);

    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("delay") };
    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
    scfg.track_name = MOQ_BYTES_LITERAL("track");
    moq_subscription_t sub_h;
    moq_session_subscribe(cl, &scfg, moq_simpair_now_us(sp), &sub_h);

    drain_deadlines(sp, 30);

    moq_subscription_t srv_sub = {0};
    {
        moq_event_t ev;
        while (moq_session_poll_events(sv, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                srv_sub = ev.u.subscribe_request.sub;
                moq_accept_subscribe_cfg_t acc;
                moq_accept_subscribe_cfg_init(&acc);
                moq_session_accept_subscribe(sv, srv_sub, &acc,
                    moq_simpair_now_us(sp));
            }
            moq_event_cleanup(&ev);
        }
    }

    if (!moq_subscription_is_valid(srv_sub))
        FAIL_WITH(sp, -2);

    drain_deadlines(sp, 30);
    drain_events(cl);

    uint8_t prelude_payload[16] = {
        0xDE,0xAD,0xBE,0xEF, 1,2,3,4, 5,6,7,8, 9,10,11,12
    };
    moq_rcbuf_t *payload = NULL;
    moq_rcbuf_create(&alloc, prelude_payload, 16, &payload);
    moq_subgroup_cfg_t sgcfg;
    moq_subgroup_cfg_init(&sgcfg);
    moq_subgroup_handle_t sg;
    moq_session_open_subgroup(sv, srv_sub, &sgcfg,
        moq_simpair_now_us(sp), &sg);
    moq_session_write_object(sv, sg, 0, payload, moq_simpair_now_us(sp));
    moq_rcbuf_decref(payload);
    moq_session_close_subgroup(sv, sg, moq_simpair_now_us(sp));

    SET_PHASE(PHASE_OBJECT);
    drain_deadlines(sp, 40);

    size_t pre_objects = 0;
    {
        moq_event_t ev;
        while (moq_session_poll_events(cl, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED &&
                ev.u.object_received.payload) {
                size_t plen = moq_rcbuf_len(ev.u.object_received.payload);
                const uint8_t *pd = moq_rcbuf_data(ev.u.object_received.payload);
                if (plen == 16 && memcmp(pd, prelude_payload, 16) == 0)
                    pre_objects++;
            }
            moq_event_cleanup(&ev);
        }
    }
    *out_pre_objects = pre_objects;

    /* ---- Prelude: namespace-sub roundtrip through delay ------------- */
    SET_PHASE(PHASE_NS_PUB);

    moq_publish_namespace_cfg_t ncfg;
    moq_publish_namespace_cfg_init(&ncfg);
    ncfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
    moq_announcement_t ann;
    moq_session_publish_namespace(sv, &ncfg, moq_simpair_now_us(sp), &ann);

    drain_deadlines(sp, 30);
    drain_events(cl);

    SET_PHASE(PHASE_NS_SUB);
    moq_subscribe_namespace_cfg_t sncfg;
    moq_subscribe_namespace_cfg_init(&sncfg);
    sncfg.track_namespace_prefix = (moq_namespace_t){ ns_parts, 1 };
    moq_ns_sub_handle_t nsh;
    moq_session_subscribe_namespace(cl, &sncfg, moq_simpair_now_us(sp), &nsh);

    drain_deadlines(sp, 30);

    moq_ns_sub_handle_t srv_nsh = MOQ_NS_SUB_HANDLE_INVALID;
    {
        moq_event_t ev;
        while (moq_session_poll_events(sv, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_NS_SUB_REQUEST)
                srv_nsh = ev.u.ns_sub_request.handle;
            moq_event_cleanup(&ev);
        }
    }
    if (!moq_ns_sub_handle_is_valid(srv_nsh))
        FAIL_WITH(sp, -5);

    moq_accept_ns_sub_cfg_t ans;
    moq_accept_ns_sub_cfg_init(&ans);
    moq_session_accept_ns_sub(sv, srv_nsh, &ans,
        moq_simpair_now_us(sp));
    drain_deadlines(sp, 30);

    {
        bool found_ns_ok = false;
        moq_event_t ev;
        while (moq_session_poll_events(cl, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_NS_SUB_OK)
                found_ns_ok = true;
            moq_event_cleanup(&ev);
        }
        if (!found_ns_ok)
            FAIL_WITH(sp, -7);
    }

    /* ---- Random phase ---------------------------------------------- */
    SET_PHASE(PHASE_RANDOM);

    rng_t rng = { seed };
    for (int step = 0; step < max_steps; step++) {
        uint64_t op = rng_next(&rng) % 6;
        diag->step = step;
        diag->op = (int)op;
        MIX(ts->hash, op);

        switch (op) {
        case 0: /* pump + drain deadlines */
            drain_deadlines(sp, 10);
            break;
        case 1: { /* write object */
            uint8_t d[8];
            for (int k = 0; k < 8; k++) d[k] = (uint8_t)rng_next(&rng);
            moq_rcbuf_t *pl = NULL;
            if (moq_rcbuf_create(&alloc, d, 8, &pl) == MOQ_OK) {
                moq_subgroup_cfg_t sc;
                moq_subgroup_cfg_init(&sc);
                sc.group_id = (uint64_t)step;
                moq_subgroup_handle_t sh;
                if (moq_session_open_subgroup(sv, srv_sub, &sc,
                        moq_simpair_now_us(sp), &sh) == MOQ_OK) {
                    moq_session_write_object(sv, sh, 0, pl,
                        moq_simpair_now_us(sp));
                    moq_session_close_subgroup(sv, sh,
                        moq_simpair_now_us(sp));
                    MIX(ts->hash, 0xF00D);
                }
                moq_rcbuf_decref(pl);
            }
            break;
        }
        case 2: /* drain client events */
        {
            moq_event_t ev;
            while (moq_session_poll_events(cl, &ev, 1) == 1) {
                if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED)
                    ts->objects++;
                moq_event_cleanup(&ev);
            }
            break;
        }
        case 3: /* drain server events */
            drain_events(sv);
            break;
        case 4: /* advance time by 1-500us */
        {
            uint64_t delta = (rng_next(&rng) % 500) + 1;
            moq_simpair_advance_to(sp,
                moq_simpair_now_us(sp) + delta);
            break;
        }
        case 5: /* pump only (no deadline drain) */
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            break;
        }

        if (moq_session_state(cl) == MOQ_SESS_CLOSED ||
            moq_session_state(sv) == MOQ_SESS_CLOSED)
            break;
    }

    SET_PHASE(PHASE_DRAIN);
    drain_deadlines(sp, 40);
    drain_events(cl);
    drain_events(sv);
    drain_actions(cl);
    drain_actions(sv);

    SET_PHASE(PHASE_END);
    diag->now_us = moq_simpair_now_us(sp);
    diag->next_deadline_us = moq_simpair_next_deadline_us(sp);
    diag->delayed_count = moq_simpair_delayed_count(sp);

    moq_simpair_destroy(sp);
#undef FAIL_WITH
#undef SET_PHASE
    return (sa.balance == 0) ? 0 : -4;
}

static void print_diag(uint64_t seed, int run, const char *reason,
                       int rc, int max_steps, const fail_diag_t *d)
{
    fprintf(stderr,
        "FAIL seed=0x%llx run=%d: %s (rc=%d)\n"
        "  phase=%s step=%d op=%d\n"
        "  now_us=%llu next_deadline=%llu delayed=%zu\n"
        "  fault_flags=DELAY|SPLIT_CONTROL|SPLIT_DATA|SPLIT_BIDI"
        " fault_per_mille=500\n",
        (unsigned long long)seed, run, reason, rc,
        phase_name(d->phase), d->step, d->op,
        (unsigned long long)d->now_us,
        (unsigned long long)d->next_deadline_us,
        d->delayed_count);
    REPLAY(seed, max_steps);
}

int main(void)
{
    uint64_t seed_start = 0;
    uint64_t num_seeds = 100;
    int max_steps = 50;

    const char *e;
    if ((e = getenv("MOQ_SCENARIO_SEED_START")) != NULL)
        seed_start = strtoull(e, NULL, 0);
    if ((e = getenv("MOQ_SCENARIO_SEEDS")) != NULL)
        num_seeds = strtoull(e, NULL, 0);
    if ((e = getenv("MOQ_SCENARIO_STEPS")) != NULL)
        max_steps = atoi(e);

    int failures = 0;
    for (uint64_t s = seed_start; s < seed_start + num_seeds; s++) {
        trace_t runs[2];
        size_t pre_objects[2];
        bool ok = true;

        fail_diag_t diags[2];
        for (int r = 0; r < 2; r++) {
            int rc = run_seed(s, max_steps, &runs[r], &pre_objects[r],
                              &diags[r]);
            if (rc < 0) {
                const char *reason = "unknown";
                switch (rc) {
                case -1: reason = "create failed"; break;
                case -2: reason = "subscribe not delivered"; break;
                case -3: reason = "subscribe not delivered + alloc leak"; break;
                case -4: reason = "alloc leak at end"; break;
                case -5: reason = "ns_sub_request not delivered"; break;
                case -6: reason = "ns_sub_request not delivered + alloc leak"; break;
                case -7: reason = "ns_sub_ok not delivered"; break;
                case -8: reason = "ns_sub_ok not delivered + alloc leak"; break;
                default: break;
                }
                print_diag(s, r, reason, rc, max_steps, &diags[r]);
                ok = false;
                break;
            }
        }
        if (!ok) { failures++; continue; }

        if (runs[0].hash != runs[1].hash ||
            runs[0].count != runs[1].count) {
            print_diag(s, 0, "trace hash mismatch", -10,
                       max_steps, &diags[0]);
            fprintf(stderr, "  run0: hash=0x%llx count=%zu\n"
                            "  run1: hash=0x%llx count=%zu\n",
                (unsigned long long)runs[0].hash, runs[0].count,
                (unsigned long long)runs[1].hash, runs[1].count);
            failures++;
            continue;
        }

        if (pre_objects[0] != 1) {
            print_diag(s, 0, "prelude object not delivered", -11,
                       max_steps, &diags[0]);
            fprintf(stderr, "  pre_objects=%zu\n", pre_objects[0]);
            failures++;
            continue;
        }

        if (runs[0].delay_enqueue == 0) {
            print_diag(s, 0, "no delay enqueue records", -12,
                       max_steps, &diags[0]);
            failures++;
            continue;
        }
    }

    if (failures > 0) {
        fprintf(stderr, "FAIL: test_scenario_delay (%d failures in "
                "%llu seeds)\n", failures,
                (unsigned long long)num_seeds);
        return 1;
    }
    fprintf(stderr, "PASS: test_scenario_delay (%llu seeds)\n",
            (unsigned long long)num_seeds);
    return 0;
}
