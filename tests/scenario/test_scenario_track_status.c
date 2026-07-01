/* Deterministic TRACK_STATUS scenario runner.
 * Prelude: exact accept + reject flow with stale-handle verification.
 * Random: request/accept/reject with slot reuse after terminal events.
 * Each seed runs twice; trace hash + all counters must match. */
#include <moq/sim.h>
#include "../../tests/unit/test_support.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#define REPLAY(seed, ms) fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx " \
    "MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%d ./build/tests/test_scenario_track_status\n", \
    (unsigned long long)(seed), (ms))

typedef struct { uint64_t s; } rng_t;
static uint64_t rng_next(rng_t *r) {
    uint64_t z = (r->s += 0x9E3779B97F4A7C15ULL);
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    return z ^ (z >> 31);
}

typedef struct {
    uint64_t hash; size_t count;
    size_t ok_count;
    size_t error_count;
    size_t sent_count;
    size_t upd_not_supported;
} trace_t;

#define MIX(h,v) do { (h) ^= (v); (h) *= 0x100000001B3ULL; } while(0)
#define HMIX(ts, v) MIX((ts)->hash, (uint64_t)(v))
#define INIT_HASH 0xCBF29CE484222325ULL

static void trace_hash_fn(void *ctx, const moq_sim_trace_record_t *r) {
    trace_t *s = (trace_t *)ctx; uint64_t h = s->hash;
    MIX(h,r->seed); MIX(h,r->step); MIX(h,r->now_us); MIX(h,(uint64_t)r->kind);
    MIX(h,(uint64_t)r->from); MIX(h,(uint64_t)r->to); MIX(h,(uint64_t)r->action_kind);
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

#define MAX_TS 4
typedef enum { TS_NONE=0, TS_PENDING } ts_state_t;
typedef struct {
    ts_state_t st;
    moq_track_status_handle_t cli;
    moq_track_status_handle_t srv;
    uint64_t request_id;
} sts_t;
typedef struct { sts_t s[MAX_TS]; bool closed; } shadow_t;

typedef enum {
    OP_PUMP, OP_REQUEST, OP_ACCEPT, OP_REJECT,
    OP_DRAIN_C, OP_DRAIN_S, OP_ADV_T,
    OP_COUNT,
} op_t;
static const char *op_name(op_t o) {
    static const char *n[] = {"PUMP","REQUEST","ACCEPT","REJECT",
        "DRAIN_C","DRAIN_S","ADV_T"};
    return o < OP_COUNT ? n[o] : "?";
}

#define MAX_LOG 64
typedef struct { op_t op; uint64_t p; } loge_t;
typedef struct { loge_t e[MAX_LOG]; size_t n; } oplog_t;
static void log_op(oplog_t *l, op_t o, uint64_t p) {
    if (l->n < MAX_LOG) { l->e[l->n].op = o; l->e[l->n].p = p; l->n++; }
}
static void dump_log(uint64_t s, int r, const oplog_t *l) {
    fprintf(stderr, "  seed=0x%llx run=%d ops:\n", (unsigned long long)s, r);
    for (size_t i = 0; i < l->n; i++)
        fprintf(stderr, "    %zu: %s p=%llu\n", i, op_name(l->e[i].op),
                (unsigned long long)l->e[i].p);
}

#define FIND_SLOT(want_st, out) int out = -1; \
    for (int _i=0; _i<MAX_TS; _i++) if (st->s[_i].st==(want_st)){out=_i;break;}

static void drain_all(moq_session_t *c, moq_session_t *sv) {
    moq_event_t d[16]; size_t ne;
    while ((ne = moq_session_poll_events(c, d, 16)) > 0)
        for (size_t i = 0; i < ne; i++) moq_event_cleanup(&d[i]);
    while ((ne = moq_session_poll_events(sv, d, 16)) > 0)
        for (size_t i = 0; i < ne; i++) moq_event_cleanup(&d[i]);
    moq_action_t a[16]; size_t na;
    while ((na = moq_session_poll_actions(sv, a, 16)) > 0)
        for (size_t i = 0; i < na; i++) moq_action_cleanup(&a[i]);
    while ((na = moq_session_poll_actions(c, a, 16)) > 0)
        for (size_t i = 0; i < na; i++) moq_action_cleanup(&a[i]);
}

#define DO_PUMP do { log_op(log,OP_PUMP,0); HMIX(ts,OP_PUMP); \
    if (moq_simpair_run_until_quiescent(sp,8,NULL) < 0) return -1; } while(0)

static int execute_step(moq_simpair_t *sp, shadow_t *st, rng_t *rng,
                         oplog_t *log, trace_t *ts)
{
    if (st->closed) return 0;
    op_t op = (op_t)(rng_next(rng) % OP_COUNT);
    moq_session_t *cl = moq_simpair_client(sp), *sv = moq_simpair_server(sp);

    switch (op) {
    case OP_PUMP:
        log_op(log,op,0); HMIX(ts,op);
        if (moq_simpair_run_until_quiescent(sp,8,NULL) < 0) return -1;
        break;

    case OP_REQUEST: {
        FIND_SLOT(TS_NONE, slot); if (slot < 0) { DO_PUMP; break; }
        moq_bytes_t nsp[] = { MOQ_BYTES_LITERAL("test") };
        moq_namespace_t ns = {nsp,1};
        char nm[16]; snprintf(nm,sizeof(nm),"ts%llu",(unsigned long long)(rng_next(rng)%1000));
        moq_track_status_cfg_t cfg; moq_track_status_cfg_init(&cfg);
        cfg.track_namespace = ns; cfg.track_name = moq_bytes_cstr(nm);
        moq_track_status_handle_t h;
        moq_result_t rc = moq_session_track_status(cl, &cfg,
            moq_simpair_now_us(sp), &h);
        log_op(log,op,rc>=0?1:0); HMIX(ts,op); HMIX(ts,rc);
        if (rc == MOQ_OK) {
            if (moq_simpair_run_until_quiescent(sp,8,NULL) < 0) return -1;
            moq_event_t ev[4]; size_t ne = moq_session_poll_events(sv,ev,4);
            HMIX(ts,ne);
            bool found = false;
            for (size_t j=0; j<ne; j++) {
                HMIX(ts,ev[j].kind);
                if (ev[j].kind == MOQ_EVENT_TRACK_STATUS_REQUEST) {
                    st->s[slot] = (sts_t){TS_PENDING, h,
                        ev[j].u.track_status_request.handle, 0};
                    found = true;
                }
                moq_event_cleanup(&ev[j]);
            }
            if (!found) return -1;
            ts->sent_count++;
        }
        break; }

    case OP_ACCEPT: {
        FIND_SLOT(TS_PENDING, idx); if (idx < 0) { DO_PUMP; break; }
        log_op(log,op,(uint64_t)idx); HMIX(ts,op);
        moq_accept_track_status_cfg_t ac;
        moq_accept_track_status_cfg_init(&ac);
        moq_result_t rc = moq_session_accept_track_status(sv,
            st->s[idx].srv, &ac, moq_simpair_now_us(sp));
        HMIX(ts,rc);
        if (rc == MOQ_OK) {
            if (moq_simpair_run_until_quiescent(sp,8,NULL) < 0) return -1;
            moq_event_t ev[4]; size_t ne = moq_session_poll_events(cl,ev,4);
            HMIX(ts,ne);
            bool found_terminal = false;
            for (size_t j=0; j<ne; j++) {
                HMIX(ts,ev[j].kind);
                if (ev[j].kind == MOQ_EVENT_TRACK_STATUS_OK) {
                    ts->ok_count++; found_terminal = true;
                }
                moq_event_cleanup(&ev[j]);
            }
            if (!found_terminal) return -1;
            st->s[idx].st = TS_NONE;
        }
        break; }

    case OP_REJECT: {
        FIND_SLOT(TS_PENDING, idx); if (idx < 0) { DO_PUMP; break; }
        log_op(log,op,(uint64_t)idx); HMIX(ts,op);
        moq_reject_track_status_cfg_t rj;
        moq_reject_track_status_cfg_init(&rj);
        rj.error_code = 0x10;
        moq_result_t rc = moq_session_reject_track_status(sv,
            st->s[idx].srv, &rj, moq_simpair_now_us(sp));
        HMIX(ts,rc);
        if (rc == MOQ_OK) {
            if (moq_simpair_run_until_quiescent(sp,8,NULL) < 0) return -1;
            moq_event_t ev[4]; size_t ne = moq_session_poll_events(cl,ev,4);
            HMIX(ts,ne);
            bool found_terminal = false;
            for (size_t j=0; j<ne; j++) {
                HMIX(ts,ev[j].kind);
                if (ev[j].kind == MOQ_EVENT_TRACK_STATUS_ERROR) {
                    ts->error_count++; found_terminal = true;
                }
                moq_event_cleanup(&ev[j]);
            }
            if (!found_terminal) return -1;
            st->s[idx].st = TS_NONE;
        }
        break; }

    case OP_DRAIN_C: {
        log_op(log,op,0); HMIX(ts,op);
        moq_event_t ev[16]; size_t ne;
        while ((ne = moq_session_poll_events(cl, ev, 16)) > 0)
            for (size_t i = 0; i < ne; i++) {
                HMIX(ts,ev[i].kind);
                if (ev[i].kind == MOQ_EVENT_TRACK_STATUS_OK) ts->ok_count++;
                if (ev[i].kind == MOQ_EVENT_TRACK_STATUS_ERROR) ts->error_count++;
                moq_event_cleanup(&ev[i]);
            }
        moq_action_t a[16]; size_t na;
        while ((na = moq_session_poll_actions(cl, a, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&a[i]);
        break; }

    case OP_DRAIN_S: {
        log_op(log,op,0); HMIX(ts,op);
        moq_event_t ev[16]; size_t ne;
        while ((ne = moq_session_poll_events(sv, ev, 16)) > 0)
            for (size_t i = 0; i < ne; i++) {
                HMIX(ts,ev[i].kind);
                moq_event_cleanup(&ev[i]);
            }
        moq_action_t a[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, a, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&a[i]);
        break; }

    case OP_ADV_T: {
        uint64_t delta = rng_next(rng) % 10000;
        log_op(log,op,delta); HMIX(ts,op); HMIX(ts,delta);
        moq_simpair_advance_to(sp, moq_simpair_now_us(sp) + delta);
        break; }

    default: break;
    }
    return 0;
}

static int run_exact_prelude(uint64_t seed, const moq_alloc_t *alloc,
                              trace_t *ts)
{
    int rc_out = -1;
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = alloc; cfg.seed = seed; cfg.initial_now_us = 1000;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 10;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 10;
    cfg.trace_fn = trace_hash_fn; cfg.trace_ctx = ts;

    moq_simpair_t *sp = NULL;
    if (moq_simpair_create(&cfg, &sp) < 0) return -1;
    moq_simpair_start(sp);
    moq_session_t *c = moq_simpair_client(sp), *sv = moq_simpair_server(sp);
    if (moq_simpair_run_until_quiescent(sp, 8, NULL) < 0) goto cleanup;
    { moq_event_t ev;
      if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);
      if (moq_session_poll_events(sv, &ev, 1) == 1) moq_event_cleanup(&ev); }

    /* 1. Client sends track_status. */
    moq_track_status_cfg_t tscfg; moq_track_status_cfg_init(&tscfg);
    moq_bytes_t nsp[] = { MOQ_BYTES_LITERAL("test") };
    tscfg.track_namespace = (moq_namespace_t){nsp, 1};
    tscfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_track_status_handle_t ch1;
    if (moq_session_track_status(c, &tscfg, 1000, &ch1) != MOQ_OK) goto cleanup;
    if (moq_simpair_run_until_quiescent(sp, 8, NULL) < 0) goto cleanup;

    /* 2. Server receives TRACK_STATUS_REQUEST. */
    moq_event_t ev;
    if (moq_session_poll_events(sv, &ev, 1) != 1) goto cleanup;
    if (ev.kind != MOQ_EVENT_TRACK_STATUS_REQUEST) { moq_event_cleanup(&ev); goto cleanup; }
    moq_track_status_handle_t sh1 = ev.u.track_status_request.handle;
    HMIX(ts, ev.u.track_status_request.track_namespace.count);
    HMIX(ts, ev.u.track_status_request.track_name.len);
    moq_event_cleanup(&ev);

    /* 3. Server accepts. */
    moq_accept_track_status_cfg_t acc; moq_accept_track_status_cfg_init(&acc);
    if (moq_session_accept_track_status(sv, sh1, &acc, 1000) != MOQ_OK) goto cleanup;
    if (moq_simpair_run_until_quiescent(sp, 8, NULL) < 0) goto cleanup;

    /* 4. Client receives TRACK_STATUS_OK. */
    if (moq_session_poll_events(c, &ev, 1) != 1) goto cleanup;
    if (ev.kind != MOQ_EVENT_TRACK_STATUS_OK) { moq_event_cleanup(&ev); goto cleanup; }
    HMIX(ts, ev.kind);
    ts->ok_count++;
    ts->sent_count++;
    moq_event_cleanup(&ev);

    /* 5. Client sends another track_status. */
    tscfg.track_name = MOQ_BYTES_LITERAL("audio");
    moq_track_status_handle_t ch2;
    if (moq_session_track_status(c, &tscfg, 1000, &ch2) != MOQ_OK) goto cleanup;
    if (moq_simpair_run_until_quiescent(sp, 8, NULL) < 0) goto cleanup;

    /* 6. Server receives second request and rejects. */
    if (moq_session_poll_events(sv, &ev, 1) != 1) goto cleanup;
    if (ev.kind != MOQ_EVENT_TRACK_STATUS_REQUEST) { moq_event_cleanup(&ev); goto cleanup; }
    moq_track_status_handle_t sh2 = ev.u.track_status_request.handle;
    moq_event_cleanup(&ev);
    moq_reject_track_status_cfg_t rej; moq_reject_track_status_cfg_init(&rej);
    rej.error_code = 0x10;
    if (moq_session_reject_track_status(sv, sh2, &rej, 1000) != MOQ_OK) goto cleanup;
    if (moq_simpair_run_until_quiescent(sp, 8, NULL) < 0) goto cleanup;

    /* 7. Client receives TRACK_STATUS_ERROR. */
    if (moq_session_poll_events(c, &ev, 1) != 1) goto cleanup;
    if (ev.kind != MOQ_EVENT_TRACK_STATUS_ERROR) { moq_event_cleanup(&ev); goto cleanup; }
    if (ev.u.track_status_error.error_code != 0x10) { moq_event_cleanup(&ev); goto cleanup; }
    HMIX(ts, ev.kind);
    HMIX(ts, ev.u.track_status_error.error_code);
    ts->error_count++;
    ts->sent_count++;
    moq_event_cleanup(&ev);

    /* 8. Server handles should be stale after accept/reject. */
    {
        moq_accept_track_status_cfg_t stale_acc;
        moq_accept_track_status_cfg_init(&stale_acc);
        if (moq_session_accept_track_status(sv, sh1, &stale_acc, 1000) !=
            MOQ_ERR_STALE_HANDLE) goto cleanup;
        if (moq_session_accept_track_status(sv, sh2, &stale_acc, 1000) !=
            MOQ_ERR_STALE_HANDLE) goto cleanup;
    }
    if (moq_session_state(c) != MOQ_SESS_ESTABLISHED) goto cleanup;
    if (moq_session_state(sv) != MOQ_SESS_ESTABLISHED) goto cleanup;

    rc_out = 0;
cleanup:
    drain_all(c, sv); moq_simpair_destroy(sp); return rc_out;
}

static int run_scenario(uint64_t seed, const moq_alloc_t *alloc,
                         trace_t *sum, int run_id, int max_steps)
{
    memset(sum, 0, sizeof(*sum));
    sum->hash = INIT_HASH;

    if (run_exact_prelude(seed, alloc, sum) < 0) {
        fprintf(stderr, "FAIL seed=0x%llx run=%d: prelude failed\n",
            (unsigned long long)seed, run_id);
        REPLAY(seed, max_steps); return 1;
    }

    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = alloc; cfg.seed = seed; cfg.initial_now_us = 1000;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 8;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 8;
    cfg.trace_fn = trace_hash_fn; cfg.trace_ctx = sum;

    moq_simpair_t *sp = NULL;
    if (moq_simpair_create(&cfg, &sp) < 0) return -1;
    moq_simpair_start(sp);
    if (moq_simpair_run_until_quiescent(sp, 8, NULL) < 0) {
        moq_simpair_destroy(sp); return 1;
    }
    { moq_event_t ev;
      if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1) moq_event_cleanup(&ev);
      if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) moq_event_cleanup(&ev); }

    shadow_t st; memset(&st, 0, sizeof(st));
    rng_t rng = { seed };
    oplog_t log = {0}; int failures = 0;

    for (int step = 0; step < max_steps; step++) {
        if (execute_step(sp, &st, &rng, &log, sum) < 0) {
            fprintf(stderr, "FAIL seed=0x%llx run=%d step=%d: operation error\n",
                (unsigned long long)seed, run_id, step);
            dump_log(seed, run_id, &log); REPLAY(seed, max_steps);
            failures++; st.closed = true; break;
        }
        if (moq_session_state(moq_simpair_client(sp)) == MOQ_SESS_CLOSED ||
            moq_session_state(moq_simpair_server(sp)) == MOQ_SESS_CLOSED) {
            if (!st.closed) {
                fprintf(stderr, "FAIL seed=0x%llx run=%d step=%d: unexpected close\n",
                    (unsigned long long)seed, run_id, step);
                dump_log(seed, run_id, &log); REPLAY(seed, max_steps); failures++;
            }
            st.closed = true; break;
        }
    }
    /* Final drain. */
    if (!st.closed) {
        for (int f = 0; f < 16; f++) {
            if (moq_simpair_run_until_quiescent(sp, 8, NULL) < 0)
                { failures++; break; }
            moq_event_t ev[16]; size_t ne;
            while ((ne = moq_session_poll_events(moq_simpair_client(sp), ev, 16)) > 0)
                for (size_t i = 0; i < ne; i++) {
                    if (ev[i].kind == MOQ_EVENT_TRACK_STATUS_OK) sum->ok_count++;
                    if (ev[i].kind == MOQ_EVENT_TRACK_STATUS_ERROR) sum->error_count++;
                    moq_event_cleanup(&ev[i]);
                }
        }
    }
    drain_all(moq_simpair_client(sp), moq_simpair_server(sp));
    moq_simpair_destroy(sp);
    return failures;
}

int main(void)
{
    int failures = 0;
    const char *es = getenv("MOQ_SCENARIO_SEEDS"), *eb = getenv("MOQ_SCENARIO_SEED_START"),
               *et = getenv("MOQ_SCENARIO_STEPS");
    uint64_t num_seeds = es ? (uint64_t)strtoull(es, NULL, 0) : 100;
    uint64_t seed_start = eb ? (uint64_t)strtoull(eb, NULL, 0) : 0;
    int max_steps = et ? atoi(et) : 50;
    size_t total_sent = 0, total_ok = 0, total_err = 0;

    for (uint64_t seed = seed_start; seed < seed_start + num_seeds; seed++) {
        trace_t sums[2];
        for (int run = 0; run < 2; run++) {
            scen_alloc_t as = {0};
            moq_alloc_t al = { &as, sa_alloc, sa_realloc, sa_free };
            failures += run_scenario(seed, &al, &sums[run], run, max_steps);
            if (as.balance != 0) {
                fprintf(stderr, "FAIL seed=0x%llx run=%d: alloc balance=%lld\n",
                    (unsigned long long)seed, run, (long long)as.balance);
                REPLAY(seed, max_steps); failures++;
            }
        }
#define CMP(f) (sums[0].f != sums[1].f)
        if (CMP(hash) || CMP(count) || CMP(ok_count) || CMP(error_count) ||
            CMP(sent_count)) {
            fprintf(stderr, "FAIL seed=0x%llx: determinism mismatch\n",
                (unsigned long long)seed);
            REPLAY(seed, max_steps); failures++;
        }
#undef CMP
        if (sums[0].ok_count + sums[0].error_count > sums[0].sent_count) {
            fprintf(stderr, "FAIL seed=0x%llx: phantom responses ok=%zu err=%zu sent=%zu\n",
                (unsigned long long)seed, sums[0].ok_count, sums[0].error_count,
                sums[0].sent_count);
            REPLAY(seed, max_steps); failures++;
        }
        total_sent += sums[0].sent_count;
        total_ok += sums[0].ok_count;
        total_err += sums[0].error_count;
    }

    if (failures == 0 && num_seeds >= 100) {
        if (total_ok <= num_seeds || total_err <= num_seeds) {
            fprintf(stderr, "FAIL: random phase underexercised "
                "ok=%zu err=%zu (need >%llu each)\n",
                total_ok, total_err, (unsigned long long)num_seeds);
            failures++;
        }
    }

    if (failures == 0)
        fprintf(stderr, "PASS: test_scenario_track_status (%llu seeds, "
            "sent=%zu ok=%zu error=%zu)\n",
            (unsigned long long)num_seeds, total_sent, total_ok, total_err);
    else
        fprintf(stderr, "FAIL: test_scenario_track_status (%d failures in %llu seeds)\n",
            failures, (unsigned long long)num_seeds);
    return failures;
}
