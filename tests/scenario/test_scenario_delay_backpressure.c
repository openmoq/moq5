/*
 * Deterministic delay + small-queue stability scenario runner.
 *
 * Uses MOQ_SIM_FAULT_DELAY | MOQ_SIM_FAULT_SPLIT_CONTROL |
 * MOQ_SIM_FAULT_SPLIT_DATA with tiny max_actions/max_events to
 * verify no crash, leak, or unexpected close under delayed delivery
 * with constrained queue capacities.
 *
 * This runner does NOT assert explicit WOULD_BLOCK. A local drain
 * helper catches propagated WOULD_BLOCK from the step loop, drains
 * queues, calls process_pending, and retries. For explicit
 * WOULD_BLOCK proof, see test_scenario_backpressure which uses raw
 * sessions with manual byte injection.
 *
 * Four independent sub-scenarios per seed:
 *   1. Subscribe + accept with max_actions=2
 *   2. Object delivery with max_events=2
 *   3. Write + close subgroup with max_actions=2, max_events=2
 *   4. Namespace-sub bidi with max_actions=2, max_events=2
 *
 * Each seed runs twice; combined trace hash must match.
 */
#include <moq/sim.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#define REPLAY(seed) fprintf(stderr, \
    "  Replay: MOQ_SCENARIO_SEED_START=0x%llx " \
    "MOQ_SCENARIO_SEEDS=1 " \
    "./build/tests/test_scenario_delay_backpressure\n", \
    (unsigned long long)(seed))

typedef struct {
    uint64_t hash; size_t count;
} trace_t;
#define MIX(h,v) do { (h) ^= (v); (h) *= 0x100000001B3ULL; } while(0)
#define INIT_HASH 0xCBF29CE484222325ULL

static void trace_fn(void *ctx, const moq_sim_trace_record_t *r) {
    trace_t *s = (trace_t *)ctx; uint64_t h = s->hash;
    MIX(h,r->seed); MIX(h,r->step); MIX(h,r->now_us);
    MIX(h,(uint64_t)r->kind); MIX(h,(uint64_t)r->from);
    MIX(h,(uint64_t)r->to); MIX(h,(uint64_t)r->action_kind);
    MIX(h,(uint64_t)r->input_kind); MIX(h,(uint64_t)r->result);
    MIX(h,r->code); MIX(h,r->bytes.len);
    if (r->bytes.data && r->bytes.len > 0)
        for (size_t i = 0; i < r->bytes.len; i++) MIX(h,r->bytes.data[i]);
    s->hash = h; s->count++;
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

typedef struct {
    uint64_t now_us;
    uint64_t next_deadline;
    size_t   delayed;
} stab_diag_t;

static stab_diag_t g_diag;

static void capture_diag(moq_simpair_t *sp) {
    g_diag.now_us = moq_simpair_now_us(sp);
    g_diag.next_deadline = moq_simpair_next_deadline_us(sp);
    g_diag.delayed = moq_simpair_delayed_count(sp);
}

static moq_result_t drain_with_retry(moq_simpair_t *sp, int max_iter) {
    for (int i = 0; i < max_iter; i++) {
        moq_result_t rc = moq_simpair_run_until_quiescent(sp, 16, NULL);
        if (rc == MOQ_ERR_WOULD_BLOCK) {
            { moq_event_t ev;
              while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
                  moq_event_cleanup(&ev);
              while (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1)
                  moq_event_cleanup(&ev);
            }
            { moq_action_t acts[16]; size_t na;
              while ((na = moq_session_poll_actions(moq_simpair_client(sp), acts, 16)) > 0)
                  for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
              while ((na = moq_session_poll_actions(moq_simpair_server(sp), acts, 16)) > 0)
                  for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
            }
            moq_result_t prc;
            prc = moq_session_process_pending(moq_simpair_client(sp),
                moq_simpair_now_us(sp));
            if (prc < 0 && prc != MOQ_ERR_WOULD_BLOCK) return prc;
            prc = moq_session_process_pending(moq_simpair_server(sp),
                moq_simpair_now_us(sp));
            if (prc < 0 && prc != MOQ_ERR_WOULD_BLOCK) return prc;
            continue;
        }
        if (rc < 0) return rc;
        uint64_t dl = moq_simpair_next_deadline_us(sp);
        if (dl == UINT64_MAX || dl <= moq_simpair_now_us(sp)) break;
        moq_simpair_advance_to(sp, dl);
    }
    return moq_simpair_run_until_quiescent(sp, 16, NULL);
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

static void destroy_pair(moq_simpair_t *sp) {
    drain_events(moq_simpair_client(sp));
    drain_events(moq_simpair_server(sp));
    drain_actions(moq_simpair_client(sp));
    drain_actions(moq_simpair_server(sp));
    moq_simpair_destroy(sp);
}

#define DRAIN(sp, n) do { \
    moq_result_t _drc = drain_with_retry(sp, n); \
    if (_drc < 0) { \
        capture_diag(sp); destroy_pair(sp); return _drc; \
    } \
} while(0)

#define STAB_FAIL(sp, code) do { \
    capture_diag(sp); destroy_pair(sp); return (code); \
} while(0)

static moq_simpair_t *make_pair(uint64_t seed, moq_alloc_t *alloc,
                                 trace_t *ts, uint32_t max_act,
                                 uint32_t max_evt, uint32_t fault_flags)
{
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = alloc;
    cfg.seed = seed;
    cfg.initial_now_us = 1000;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 16;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 16;
    cfg.fault_per_mille = 500;
    cfg.fault_flags = fault_flags;
    cfg.trace_fn = trace_fn;
    cfg.trace_ctx = ts;
    cfg.max_actions = max_act;
    cfg.max_events = max_evt;

    moq_simpair_t *sp = NULL;
    if (moq_simpair_create(&cfg, &sp) < 0) return NULL;
    moq_simpair_start(sp);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    drain_events(moq_simpair_client(sp));
    drain_events(moq_simpair_server(sp));
    moq_simpair_enable_faults(sp);
    return sp;
}

#define CTL_FLAGS (MOQ_SIM_FAULT_DELAY | MOQ_SIM_FAULT_SPLIT_CONTROL | \
                   MOQ_SIM_FAULT_SPLIT_DATA)
#define BIDI_FLAGS (MOQ_SIM_FAULT_DELAY | MOQ_SIM_FAULT_SPLIT_BIDI)

static int stab_control(uint64_t seed, moq_alloc_t *alloc, trace_t *ts)
{
    moq_simpair_t *sp = make_pair(seed, alloc, ts, 2, 0, CTL_FLAGS);
    if (!sp) return -1;
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("bp") };
    moq_subscribe_cfg_t sc; moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ ns, 1 };
    sc.track_name = MOQ_BYTES_LITERAL("t1");
    moq_subscription_t sh;
    moq_session_subscribe(moq_simpair_client(sp), &sc,
        moq_simpair_now_us(sp), &sh);
    DRAIN(sp, 40);
    moq_subscription_t srv = {0};
    { moq_event_t ev;
      while (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
          if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
              srv = ev.u.subscribe_request.sub;
              moq_accept_subscribe_cfg_t acc;
              moq_accept_subscribe_cfg_init(&acc);
              moq_session_accept_subscribe(moq_simpair_server(sp),
                  srv, &acc, moq_simpair_now_us(sp));
          }
          moq_event_cleanup(&ev);
      }
    }
    if (!moq_subscription_is_valid(srv)) STAB_FAIL(sp, -2);
    DRAIN(sp, 40);
    drain_events(moq_simpair_client(sp));
    if (moq_session_state(moq_simpair_client(sp)) == MOQ_SESS_CLOSED ||
        moq_session_state(moq_simpair_server(sp)) == MOQ_SESS_CLOSED)
        STAB_FAIL(sp, -3);
    destroy_pair(sp);
    return 0;
}

static int stab_events(uint64_t seed, moq_alloc_t *alloc, trace_t *ts)
{
    moq_simpair_t *sp = make_pair(seed, alloc, ts, 0, 2, CTL_FLAGS);
    if (!sp) return -1;
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("bp") };
    moq_subscribe_cfg_t sc; moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ ns, 1 };
    sc.track_name = MOQ_BYTES_LITERAL("t2");
    moq_subscription_t sh;
    moq_session_subscribe(moq_simpair_client(sp), &sc,
        moq_simpair_now_us(sp), &sh);
    DRAIN(sp, 40);
    moq_subscription_t srv = {0};
    { moq_event_t ev;
      while (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
          if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
              srv = ev.u.subscribe_request.sub;
              moq_accept_subscribe_cfg_t acc;
              moq_accept_subscribe_cfg_init(&acc);
              moq_session_accept_subscribe(moq_simpair_server(sp),
                  srv, &acc, moq_simpair_now_us(sp));
          }
          moq_event_cleanup(&ev);
      }
    }
    if (!moq_subscription_is_valid(srv)) STAB_FAIL(sp, -4);
    DRAIN(sp, 40);
    drain_events(moq_simpair_client(sp));
    uint8_t pd[8] = {1,2,3,4,5,6,7,8};
    moq_rcbuf_t *pl = NULL;
    moq_rcbuf_create(alloc, pd, 8, &pl);
    moq_subgroup_cfg_t sgc; moq_subgroup_cfg_init(&sgc);
    moq_subgroup_handle_t sg;
    moq_session_open_subgroup(moq_simpair_server(sp), srv, &sgc,
        moq_simpair_now_us(sp), &sg);
    moq_session_write_object(moq_simpair_server(sp), sg, 0, pl,
        moq_simpair_now_us(sp));
    moq_rcbuf_decref(pl);
    moq_session_close_subgroup(moq_simpair_server(sp), sg,
        moq_simpair_now_us(sp));
    DRAIN(sp, 40);
    bool found = false;
    { moq_event_t ev;
      while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1) {
          if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) found = true;
          moq_event_cleanup(&ev);
      }
    }
    if (!found) STAB_FAIL(sp, -5);
    if (moq_session_state(moq_simpair_client(sp)) == MOQ_SESS_CLOSED ||
        moq_session_state(moq_simpair_server(sp)) == MOQ_SESS_CLOSED)
        STAB_FAIL(sp, -6);
    destroy_pair(sp);
    return 0;
}

static int stab_data(uint64_t seed, moq_alloc_t *alloc, trace_t *ts)
{
    moq_simpair_t *sp = make_pair(seed, alloc, ts, 2, 2, CTL_FLAGS);
    if (!sp) return -1;
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("bp") };
    moq_subscribe_cfg_t sc; moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ ns, 1 };
    sc.track_name = MOQ_BYTES_LITERAL("t3");
    moq_subscription_t sh;
    moq_session_subscribe(moq_simpair_client(sp), &sc,
        moq_simpair_now_us(sp), &sh);
    DRAIN(sp, 40);
    moq_subscription_t srv = {0};
    { moq_event_t ev;
      while (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
          if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
              srv = ev.u.subscribe_request.sub;
              moq_accept_subscribe_cfg_t acc;
              moq_accept_subscribe_cfg_init(&acc);
              moq_session_accept_subscribe(moq_simpair_server(sp),
                  srv, &acc, moq_simpair_now_us(sp));
          }
          moq_event_cleanup(&ev);
      }
    }
    if (!moq_subscription_is_valid(srv)) STAB_FAIL(sp, -7);
    DRAIN(sp, 40);
    drain_events(moq_simpair_client(sp));
    uint8_t pd[16] = {0xDE,0xAD,0xBE,0xEF,1,2,3,4,5,6,7,8,9,10,11,12};
    moq_rcbuf_t *pl = NULL;
    moq_rcbuf_create(alloc, pd, 16, &pl);
    moq_subgroup_cfg_t sgc; moq_subgroup_cfg_init(&sgc);
    moq_subgroup_handle_t sg;
    moq_session_open_subgroup(moq_simpair_server(sp), srv, &sgc,
        moq_simpair_now_us(sp), &sg);
    moq_session_write_object(moq_simpair_server(sp), sg, 0, pl,
        moq_simpair_now_us(sp));
    moq_rcbuf_decref(pl);
    moq_session_close_subgroup(moq_simpair_server(sp), sg,
        moq_simpair_now_us(sp));
    DRAIN(sp, 60);
    drain_events(moq_simpair_client(sp));
    drain_events(moq_simpair_server(sp));
    if (moq_session_state(moq_simpair_client(sp)) == MOQ_SESS_CLOSED ||
        moq_session_state(moq_simpair_server(sp)) == MOQ_SESS_CLOSED)
        STAB_FAIL(sp, -8);
    destroy_pair(sp);
    return 0;
}

static int stab_ns_sub(uint64_t seed, moq_alloc_t *alloc, trace_t *ts)
{
    moq_simpair_t *sp = make_pair(seed, alloc, ts, 2, 2, BIDI_FLAGS);
    if (!sp) return -1;
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("bp") };
    moq_publish_namespace_cfg_t nc;
    moq_publish_namespace_cfg_init(&nc);
    nc.track_namespace = (moq_namespace_t){ ns, 1 };
    moq_announcement_t ann;
    moq_session_publish_namespace(moq_simpair_server(sp), &nc,
        moq_simpair_now_us(sp), &ann);
    DRAIN(sp, 16);
    drain_events(moq_simpair_client(sp));
    moq_subscribe_namespace_cfg_t snc;
    moq_subscribe_namespace_cfg_init(&snc);
    snc.track_namespace_prefix = (moq_namespace_t){ ns, 1 };
    moq_ns_sub_handle_t nsh;
    moq_session_subscribe_namespace(moq_simpair_client(sp), &snc,
        moq_simpair_now_us(sp), &nsh);
    DRAIN(sp, 40);
    moq_ns_sub_handle_t srv_nsh = MOQ_NS_SUB_HANDLE_INVALID;
    { moq_event_t ev;
      while (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
          if (ev.kind == MOQ_EVENT_NS_SUB_REQUEST)
              srv_nsh = ev.u.ns_sub_request.handle;
          moq_event_cleanup(&ev);
      }
    }
    if (!moq_ns_sub_handle_is_valid(srv_nsh)) STAB_FAIL(sp, -9);
    moq_accept_ns_sub_cfg_t ans;
    moq_accept_ns_sub_cfg_init(&ans);
    moq_session_accept_ns_sub(moq_simpair_server(sp), srv_nsh, &ans,
        moq_simpair_now_us(sp));
    DRAIN(sp, 40);
    drain_events(moq_simpair_client(sp));
    drain_events(moq_simpair_server(sp));
    if (moq_session_state(moq_simpair_client(sp)) == MOQ_SESS_CLOSED ||
        moq_session_state(moq_simpair_server(sp)) == MOQ_SESS_CLOSED)
        STAB_FAIL(sp, -10);
    destroy_pair(sp);
    return 0;
}

static const char *sub_name(int idx) {
    static const char *n[] = { "control", "events", "data", "ns_sub" };
    return idx < 4 ? n[idx] : "?";
}

typedef int (*sub_fn)(uint64_t, moq_alloc_t*, trace_t*);

int main(void)
{
    uint64_t seed_start = 0;
    uint64_t num_seeds = 100;
    const char *e;
    if ((e = getenv("MOQ_SCENARIO_SEED_START")) != NULL)
        seed_start = strtoull(e, NULL, 0);
    if ((e = getenv("MOQ_SCENARIO_SEEDS")) != NULL)
        num_seeds = strtoull(e, NULL, 0);

    static const sub_fn subs[] = {
        stab_control, stab_events, stab_data, stab_ns_sub
    };

    int failures = 0;
    for (uint64_t s = seed_start; s < seed_start + num_seeds; s++) {
        trace_t runs[2];
        bool ok = true;
        for (int r = 0; r < 2; r++) {
            memset(&runs[r], 0, sizeof(runs[r]));
            runs[r].hash = INIT_HASH;
            for (int sub = 0; sub < 4; sub++) {
                scen_alloc_t sa = {0};
                moq_alloc_t alloc = { &sa, sa_alloc, sa_realloc, sa_free };
                memset(&g_diag, 0, sizeof(g_diag));
                int rc = subs[sub](s ^ ((uint64_t)sub << 48),
                                    &alloc, &runs[r]);
                if (sa.balance != 0) {
                    fprintf(stderr,
                        "FAIL seed=0x%llx run=%d sub=%s: alloc leak "
                        "(balance=%lld)\n"
                        "  now=%llu deadline=%llu delayed=%zu\n",
                        (unsigned long long)s, r, sub_name(sub),
                        (long long)sa.balance,
                        (unsigned long long)g_diag.now_us,
                        (unsigned long long)g_diag.next_deadline,
                        g_diag.delayed);
                    REPLAY(s);
                    ok = false; break;
                }
                if (rc < 0) {
                    fprintf(stderr,
                        "FAIL seed=0x%llx run=%d sub=%s: rc=%d\n"
                        "  now=%llu deadline=%llu delayed=%zu\n",
                        (unsigned long long)s, r, sub_name(sub), rc,
                        (unsigned long long)g_diag.now_us,
                        (unsigned long long)g_diag.next_deadline,
                        g_diag.delayed);
                    REPLAY(s);
                    ok = false; break;
                }
            }
            if (!ok) break;
        }
        if (!ok) { failures++; continue; }
        if (runs[0].hash != runs[1].hash ||
            runs[0].count != runs[1].count) {
            fprintf(stderr,
                "FAIL seed=0x%llx: trace hash mismatch "
                "(0x%llx/%zu vs 0x%llx/%zu)\n"
                "  last diag: now=%llu deadline=%llu delayed=%zu\n",
                (unsigned long long)s,
                (unsigned long long)runs[0].hash, runs[0].count,
                (unsigned long long)runs[1].hash, runs[1].count,
                (unsigned long long)g_diag.now_us,
                (unsigned long long)g_diag.next_deadline,
                g_diag.delayed);
            REPLAY(s);
            failures++;
            continue;
        }
    }

    if (failures > 0) {
        fprintf(stderr,
            "FAIL: test_scenario_delay_backpressure (%d failures in "
            "%llu seeds)\n", failures, (unsigned long long)num_seeds);
        return 1;
    }
    fprintf(stderr,
        "PASS: test_scenario_delay_backpressure (%llu seeds)\n",
        (unsigned long long)num_seeds);
    return 0;
}
