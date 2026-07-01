/*
 * Deterministic crossed-message scenario runner.
 *
 * Uses MOQ_SIM_FAULT_DELAY | MOQ_SIM_FAULT_SPLIT_CONTROL |
 * MOQ_SIM_FAULT_SPLIT_DATA | MOQ_SIM_FAULT_SPLIT_BIDI to stress
 * protocol races where terminal and update messages cross in flight.
 *
 * Four independent sub-scenarios per seed, each in a fresh simpair:
 *   1. Subscription update × unsubscribe
 *   2. PUBLISH update × PUBLISH_DONE
 *   3. Namespace-sub terminal crossing
 *   4. GOAWAY under delayed traffic
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
    "./build/tests/test_scenario_crossed\n", \
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

static void drain_deadlines(moq_simpair_t *sp, int max_adv) {
    for (int i = 0; i < max_adv; i++) {
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

static moq_simpair_t *make_pair(uint64_t seed,
                                 moq_alloc_t *alloc, trace_t *ts)
{
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = alloc;
    cfg.seed = seed;
    cfg.initial_now_us = 1000;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 32;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 32;
    cfg.fault_per_mille = 500;
    cfg.fault_flags = MOQ_SIM_FAULT_DELAY |
                      MOQ_SIM_FAULT_SPLIT_CONTROL |
                      MOQ_SIM_FAULT_SPLIT_DATA |
                      MOQ_SIM_FAULT_SPLIT_BIDI;
    cfg.trace_fn = trace_fn;
    cfg.trace_ctx = ts;

    moq_simpair_t *sp = NULL;
    if (moq_simpair_create(&cfg, &sp) < 0) return NULL;
    moq_simpair_start(sp);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    drain_events(moq_simpair_client(sp));
    drain_events(moq_simpair_server(sp));
    moq_simpair_enable_faults(sp);
    return sp;
}

static void destroy_pair(moq_simpair_t *sp) {
    drain_events(moq_simpair_client(sp));
    drain_events(moq_simpair_server(sp));
    drain_actions(moq_simpair_client(sp));
    drain_actions(moq_simpair_server(sp));
    moq_simpair_destroy(sp);
}

/* ---- Sub-scenario 1: subscription update × unsubscribe ----------- */

static int cross_sub_update(uint64_t seed,
                             moq_alloc_t *alloc, trace_t *ts)
{
    moq_simpair_t *sp = make_pair(seed, alloc, ts);
    if (!sp) return -1;
    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);

    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("crossed") };
    moq_namespace_t ns = { ns_parts, 1 };
    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = ns;
    scfg.track_name = MOQ_BYTES_LITERAL("t1");
    moq_subscription_t sub_h;
    moq_session_subscribe(cl, &scfg, moq_simpair_now_us(sp), &sub_h);
    drain_deadlines(sp, 30);

    moq_subscription_t srv_sub = {0};
    { moq_event_t ev;
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
    if (!moq_subscription_is_valid(srv_sub)) {
        destroy_pair(sp); return -2;
    }

    drain_deadlines(sp, 30);
    drain_events(cl);

    moq_subscription_update_cfg_t ucfg;
    moq_subscription_update_cfg_init(&ucfg);
    ucfg.has_subscriber_priority = true;
    ucfg.subscriber_priority = 42;
    moq_result_t urc = moq_session_update_subscription(cl, sub_h,
        &ucfg, moq_simpair_now_us(sp));

    if (urc == MOQ_OK) {
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        moq_session_unsubscribe(cl, sub_h, moq_simpair_now_us(sp));
        drain_deadlines(sp, 40);
        drain_events(cl);
        drain_events(sv);
    }

    if (moq_session_state(cl) == MOQ_SESS_CLOSED ||
        moq_session_state(sv) == MOQ_SESS_CLOSED) {
        destroy_pair(sp); return -3;
    }

    destroy_pair(sp);
    return 0;
}

/* ---- Sub-scenario 2: PUBLISH update × PUBLISH_DONE --------------- */

static int cross_pub_update(uint64_t seed,
                             moq_alloc_t *alloc, trace_t *ts)
{
    moq_simpair_t *sp = make_pair(seed, alloc, ts);
    if (!sp) return -1;
    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);

    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("crossed") };
    moq_namespace_t ns = { ns_parts, 1 };
    moq_publish_cfg_t pcfg;
    moq_publish_cfg_init(&pcfg);
    pcfg.track_namespace = ns;
    pcfg.track_name = MOQ_BYTES_LITERAL("t2");
    moq_publication_t pub_h;
    moq_session_publish(cl, &pcfg, moq_simpair_now_us(sp), &pub_h);
    drain_deadlines(sp, 30);

    moq_publication_t sv_pub = MOQ_PUBLICATION_INVALID;
    { moq_event_t ev;
      while (moq_session_poll_events(sv, &ev, 1) == 1) {
          if (ev.kind == MOQ_EVENT_PUBLISH_REQUEST) {
              sv_pub = ev.u.publish_request.pub;
              moq_accept_publish_cfg_t acfg;
              moq_accept_publish_cfg_init(&acfg);
              moq_session_accept_publish(sv, sv_pub, &acfg,
                  moq_simpair_now_us(sp));
          }
          moq_event_cleanup(&ev);
      }
    }
    if (!moq_publication_is_valid(sv_pub)) {
        destroy_pair(sp); return -4;
    }

    drain_deadlines(sp, 30);
    drain_events(cl);

    moq_publication_update_cfg_t pucfg;
    moq_publication_update_cfg_init(&pucfg);
    pucfg.has_subscriber_priority = true;
    pucfg.subscriber_priority = 77;
    moq_result_t purc = moq_session_update_publication(sv, sv_pub,
        &pucfg, moq_simpair_now_us(sp));

    if (purc == MOQ_OK) {
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        moq_finish_publish_cfg_t fcfg;
        moq_finish_publish_cfg_init(&fcfg);
        moq_session_finish_publish(cl, pub_h, &fcfg,
            moq_simpair_now_us(sp));
        drain_deadlines(sp, 40);
    }

    /* Per D16 Section 9.11: late REQUEST_UPDATE with unknown
     * existing_request_id causes PROTOCOL_VIOLATION (0x3) with
     * reason "REQUEST_UPDATE unknown existing ID". Verify the
     * exact close code and reason to avoid masking unrelated
     * protocol violations. */
    if (moq_session_state(cl) == MOQ_SESS_CLOSED ||
        moq_session_state(sv) == MOQ_SESS_CLOSED) {
        static const char expected_reason[] =
            "REQUEST_UPDATE unknown existing ID";
        bool matched = false;
        bool unexpected_close = false;
        moq_event_t ev;
        while (moq_session_poll_events(cl, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_SESSION_CLOSED) {
                if (ev.u.closed.code == 0x3 &&
                    ev.u.closed.reason.len == sizeof(expected_reason) - 1 &&
                    ev.u.closed.reason.data &&
                    memcmp(ev.u.closed.reason.data, expected_reason,
                           sizeof(expected_reason) - 1) == 0)
                    matched = true;
                else
                    unexpected_close = true;
            }
            moq_event_cleanup(&ev);
        }
        while (moq_session_poll_events(sv, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_SESSION_CLOSED) {
                if (ev.u.closed.code == 0x3 &&
                    ev.u.closed.reason.len == sizeof(expected_reason) - 1 &&
                    ev.u.closed.reason.data &&
                    memcmp(ev.u.closed.reason.data, expected_reason,
                           sizeof(expected_reason) - 1) == 0)
                    matched = true;
                else
                    unexpected_close = true;
            }
            moq_event_cleanup(&ev);
        }
        destroy_pair(sp);
        if (unexpected_close) return -6;
        return matched ? 0 : -5;
    }

    drain_events(cl);
    drain_events(sv);
    destroy_pair(sp);
    return 0;
}

/* ---- Sub-scenario 3: namespace-sub terminal crossing -------------- */

static int cross_ns_sub(uint64_t seed,
                         moq_alloc_t *alloc, trace_t *ts)
{
    moq_simpair_t *sp = make_pair(seed, alloc, ts);
    if (!sp) return -1;
    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);

    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("crossed") };
    moq_namespace_t ns = { ns_parts, 1 };
    moq_publish_namespace_cfg_t ncfg;
    moq_publish_namespace_cfg_init(&ncfg);
    ncfg.track_namespace = ns;
    moq_announcement_t ann;
    moq_session_publish_namespace(sv, &ncfg, moq_simpair_now_us(sp), &ann);
    drain_deadlines(sp, 30);
    drain_events(cl);

    moq_subscribe_namespace_cfg_t sncfg;
    moq_subscribe_namespace_cfg_init(&sncfg);
    sncfg.track_namespace_prefix = ns;
    moq_ns_sub_handle_t nsh;
    moq_session_subscribe_namespace(cl, &sncfg, moq_simpair_now_us(sp), &nsh);
    drain_deadlines(sp, 30);

    moq_ns_sub_handle_t srv_nsh = MOQ_NS_SUB_HANDLE_INVALID;
    { moq_event_t ev;
      while (moq_session_poll_events(sv, &ev, 1) == 1) {
          if (ev.kind == MOQ_EVENT_NS_SUB_REQUEST)
              srv_nsh = ev.u.ns_sub_request.handle;
          moq_event_cleanup(&ev);
      }
    }
    if (!moq_ns_sub_handle_is_valid(srv_nsh)) {
        destroy_pair(sp); return -6;
    }

    moq_accept_ns_sub_cfg_t ans;
    moq_accept_ns_sub_cfg_init(&ans);
    moq_session_accept_ns_sub(sv, srv_nsh, &ans, moq_simpair_now_us(sp));

    moq_bytes_t suffix_parts[] = { MOQ_BYTES_LITERAL("track") };
    moq_namespace_t suffix = { suffix_parts, 1 };
    moq_session_send_namespace(sv, srv_nsh, &suffix,
        moq_simpair_now_us(sp));
    moq_session_send_namespace_done(sv, srv_nsh, &suffix,
        moq_simpair_now_us(sp));

    drain_deadlines(sp, 40);
    drain_events(cl);
    drain_events(sv);

    if (moq_session_state(cl) == MOQ_SESS_CLOSED ||
        moq_session_state(sv) == MOQ_SESS_CLOSED) {
        destroy_pair(sp); return -7;
    }

    destroy_pair(sp);
    return 0;
}

/* ---- Sub-scenario 4: GOAWAY under delayed traffic ---------------- */

static int cross_goaway(uint64_t seed,
                         moq_alloc_t *alloc, trace_t *ts)
{
    moq_simpair_t *sp = make_pair(seed, alloc, ts);
    if (!sp) return -1;
    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);

    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("crossed") };
    moq_namespace_t ns = { ns_parts, 1 };
    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = ns;
    scfg.track_name = MOQ_BYTES_LITERAL("t3");
    moq_subscription_t sub;
    moq_session_subscribe(cl, &scfg, moq_simpair_now_us(sp), &sub);

    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_session_goaway(sv, NULL, 0, moq_simpair_now_us(sp));

    drain_deadlines(sp, 40);
    drain_events(cl);
    drain_events(sv);

    destroy_pair(sp);
    return 0;
}

/* ---- Main -------------------------------------------------------- */

static const char *sub_name(int idx) {
    static const char *n[] = {
        "sub_update", "pub_update", "ns_sub", "goaway"
    };
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
        cross_sub_update, cross_pub_update, cross_ns_sub, cross_goaway
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
                int rc = subs[sub](s ^ ((uint64_t)sub << 48),
                                    &alloc, &runs[r]);
                if (sa.balance != 0) {
                    fprintf(stderr,
                        "FAIL seed=0x%llx run=%d sub=%s: alloc leak "
                        "(balance=%lld)\n",
                        (unsigned long long)s, r, sub_name(sub),
                        (long long)sa.balance);
                    REPLAY(s);
                    ok = false; break;
                }
                if (rc < 0) {
                    fprintf(stderr,
                        "FAIL seed=0x%llx run=%d sub=%s: rc=%d\n",
                        (unsigned long long)s, r, sub_name(sub), rc);
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
                "(0x%llx/%zu vs 0x%llx/%zu)\n",
                (unsigned long long)s,
                (unsigned long long)runs[0].hash, runs[0].count,
                (unsigned long long)runs[1].hash, runs[1].count);
            REPLAY(s);
            failures++;
            continue;
        }
    }

    if (failures > 0) {
        fprintf(stderr, "FAIL: test_scenario_crossed (%d failures in "
                "%llu seeds)\n", failures,
                (unsigned long long)num_seeds);
        return 1;
    }
    fprintf(stderr, "PASS: test_scenario_crossed (%llu seeds)\n",
            (unsigned long long)num_seeds);
    return 0;
}
