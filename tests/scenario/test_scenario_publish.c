/* Deterministic PUBLISH (publisher-initiated subscription) scenario runner.
 * Two-tier oracle: prelude (exact delivery) + random (received <= fed).
 * Each seed runs twice; trace hash + all oracle counters must match. */
#include <moq/sim.h>
#include "../../tests/unit/test_support.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#define REPLAY(seed, ms) fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx " \
    "MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%d ./build/tests/test_scenario_publish\n", \
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
    uint64_t pre_exp_hash; size_t pre_exp_count;
    uint64_t pre_rcv_hash; size_t pre_rcv_count;
    uint64_t rnd_fed_hash; size_t rnd_fed_count;
    uint64_t rnd_rcv_hash; size_t rnd_rcv_count;
} trace_t;
#define MIX(h,v) do { (h) ^= (v); (h) *= 0x100000001B3ULL; } while(0)
#define HMIX(ts, v) MIX((ts)->hash, (uint64_t)(v))
static void hash_pl(uint64_t *h, size_t len, const uint8_t *d) {
    MIX(*h, len); for (size_t i = 0; i < len; i++) MIX(*h, d[i]);
}
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
#define INIT_HASH 0xCBF29CE484222325ULL
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

#define MAX_PUBS 4
typedef enum { PS_NONE=0, PS_PENDING, PS_ACCEPTED, PS_DONE } pstate_t;
typedef struct {
    pstate_t st; moq_publication_t cli; moq_publication_t srv;
    moq_subgroup_handle_t sg; bool has_sg;
} spub_t;
typedef struct { spub_t p[MAX_PUBS]; bool closed; } shadow_t;
typedef enum {
    OP_PUMP, OP_PUBLISH, OP_ACCEPT, OP_REJECT,
    OP_OPEN_SG, OP_WRITE_OBJ, OP_CLOSE_SG, OP_FINISH,
    OP_DRAIN_C, OP_DRAIN_S, OP_ADV_T, OP_COUNT,
} op_t;
static const char *op_name(op_t o) {
    static const char *n[] = {"PUMP","PUBLISH","ACCEPT","REJECT",
        "OPEN_SG","WRITE_OBJ","CLOSE_SG","FINISH","DRAIN_C","DRAIN_S","ADV_T"};
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

#define FIND_PUB(sv, out) int out = -1; \
    for (int _i=0; _i<MAX_PUBS; _i++) if (st->p[_i].st==(sv)){out=_i;break;}
#define FIND_ACCEPTED_SG(want_sg, out) int out = -1; \
    for (int _i=0; _i<MAX_PUBS; _i++) \
        if (st->p[_i].st==PS_ACCEPTED && st->p[_i].has_sg==(want_sg)){out=_i;break;}

static void drain_srv_evts(moq_session_t *sv, trace_t *ts) {
    moq_event_t ev[16]; size_t ne;
    while ((ne = moq_session_poll_events(sv, ev, 16)) > 0)
        for (size_t i = 0; i < ne; i++) {
            HMIX(ts, ev[i].kind);
            if (ev[i].kind == MOQ_EVENT_OBJECT_RECEIVED && ev[i].u.object_received.payload) {
                hash_pl(&ts->rnd_rcv_hash, moq_rcbuf_len(ev[i].u.object_received.payload),
                         moq_rcbuf_data(ev[i].u.object_received.payload));
                ts->rnd_rcv_count++;
            }
            moq_event_cleanup(&ev[i]);
        }
}
static void drain_all(moq_session_t *c, moq_session_t *sv, trace_t *ts) {
    { moq_event_t d[16]; size_t ne;
      while ((ne = moq_session_poll_events(c, d, 16)) > 0)
          for (size_t i = 0; i < ne; i++) moq_event_cleanup(&d[i]); }
    if (ts) drain_srv_evts(sv, ts);
    else { moq_event_t d[16]; size_t ne;
           while ((ne = moq_session_poll_events(sv, d, 16)) > 0)
               for (size_t i = 0; i < ne; i++) moq_event_cleanup(&d[i]); }
    { moq_action_t a[16]; size_t na;
      while ((na = moq_session_poll_actions(sv, a, 16)) > 0)
          for (size_t i = 0; i < na; i++) moq_action_cleanup(&a[i]);
      while ((na = moq_session_poll_actions(c, a, 16)) > 0)
          for (size_t i = 0; i < na; i++) moq_action_cleanup(&a[i]); }
}
#define DO_PUMP do { log_op(log,OP_PUMP,0); HMIX(ts,OP_PUMP); \
    if (moq_simpair_run_until_quiescent(sp,8,NULL) < 0) return -1; } while(0)

static int execute_step(moq_simpair_t *sp, shadow_t *st, rng_t *rng,
                         oplog_t *log, trace_t *ts, const moq_alloc_t *alloc)
{
    if (st->closed) return 0;
    op_t op = (op_t)(rng_next(rng) % OP_COUNT);
    moq_session_t *cl = moq_simpair_client(sp), *sv = moq_simpair_server(sp);

    switch (op) {
    case OP_PUMP:
        log_op(log,op,0); HMIX(ts,op);
        if (moq_simpair_run_until_quiescent(sp,8,NULL) < 0) return -1;
        break;
    case OP_PUBLISH: {
        FIND_PUB(PS_NONE, slot); if (slot < 0) { DO_PUMP; break; }
        moq_bytes_t nsp[] = { MOQ_BYTES_LITERAL("test") }; moq_namespace_t ns = {nsp,1};
        char nm[16]; snprintf(nm,sizeof(nm),"p%llu",(unsigned long long)(rng_next(rng)%1000));
        moq_publish_cfg_t pc; moq_publish_cfg_init(&pc);
        pc.track_namespace = ns; pc.track_name = moq_bytes_cstr(nm);
        moq_publication_t h;
        moq_result_t rc = moq_session_publish(cl, &pc, moq_simpair_now_us(sp), &h);
        log_op(log,op,rc>=0?1:0); HMIX(ts,op); HMIX(ts,rc);
        if (rc == MOQ_OK) {
            if (moq_simpair_run_until_quiescent(sp,8,NULL) < 0) return -1;
            moq_event_t ev[4]; size_t ne = moq_session_poll_events(sv,ev,4); HMIX(ts,ne);
            for (size_t j=0; j<ne; j++) {
                HMIX(ts,ev[j].kind);
                if (ev[j].kind == MOQ_EVENT_PUBLISH_REQUEST)
                    st->p[slot] = (spub_t){PS_PENDING, h, ev[j].u.publish_request.pub, {0}, false};
                moq_event_cleanup(&ev[j]);
            }
        }
        break; }
    case OP_ACCEPT: {
        FIND_PUB(PS_PENDING, idx); if (idx < 0) { DO_PUMP; break; }
        log_op(log,op,(uint64_t)idx); HMIX(ts,op);
        moq_accept_publish_cfg_t ac; moq_accept_publish_cfg_init(&ac);
        moq_result_t rc = moq_session_accept_publish(sv, st->p[idx].srv, &ac, moq_simpair_now_us(sp));
        HMIX(ts,rc);
        if (rc == MOQ_OK) {
            st->p[idx].st = PS_ACCEPTED;
            if (moq_simpair_run_until_quiescent(sp,8,NULL) < 0) return -1;
            moq_event_t ev[4]; size_t ne = moq_session_poll_events(cl,ev,4); HMIX(ts,ne);
            for (size_t j=0; j<ne; j++) { HMIX(ts,ev[j].kind); moq_event_cleanup(&ev[j]); }
        }
        break; }
    case OP_REJECT: {
        FIND_PUB(PS_PENDING, idx); if (idx < 0) { DO_PUMP; break; }
        log_op(log,op,(uint64_t)idx); HMIX(ts,op);
        moq_reject_publish_cfg_t rj; moq_reject_publish_cfg_init(&rj);
        rj.error_code = MOQ_REQUEST_ERROR_DOES_NOT_EXIST; rj.reason = MOQ_BYTES_LITERAL("no");
        moq_result_t rc = moq_session_reject_publish(sv, st->p[idx].srv, &rj, moq_simpair_now_us(sp));
        HMIX(ts,rc);
        if (rc == MOQ_OK) {
            st->p[idx].st = PS_DONE;
            if (moq_simpair_run_until_quiescent(sp,8,NULL) < 0) return -1;
            moq_event_t ev[4]; size_t ne = moq_session_poll_events(cl,ev,4); HMIX(ts,ne);
            for (size_t j=0; j<ne; j++) { HMIX(ts,ev[j].kind); moq_event_cleanup(&ev[j]); }
        }
        break; }
    case OP_OPEN_SG: {
        FIND_ACCEPTED_SG(false, idx); if (idx < 0) { DO_PUMP; break; }
        log_op(log,op,(uint64_t)idx); HMIX(ts,op);
        moq_subgroup_cfg_t sg_cfg; moq_subgroup_cfg_init(&sg_cfg);
        sg_cfg.group_id = rng_next(rng)%4; sg_cfg.subgroup_id = 0; sg_cfg.publisher_priority = 128;
        moq_subgroup_handle_t sg;
        moq_result_t rc = moq_session_open_pub_subgroup(cl, st->p[idx].cli, &sg_cfg, moq_simpair_now_us(sp), &sg);
        HMIX(ts,rc);
        if (rc == MOQ_OK) { st->p[idx].sg = sg; st->p[idx].has_sg = true; }
        break; }
    case OP_WRITE_OBJ: {
        FIND_ACCEPTED_SG(true, idx); if (idx < 0) { DO_PUMP; break; }
        size_t pl = rng_next(rng)%64; uint8_t pd[64];
        for (size_t j=0; j<pl; j++) pd[j] = (uint8_t)(rng_next(rng)&0xFF);
        moq_rcbuf_t *pay = NULL; moq_rcbuf_create(alloc, pd, pl, &pay);
        if (!pay) { log_op(log,OP_PUMP,0); break; }
        uint64_t obj_id = rng_next(rng)%8;
        moq_result_t rc = moq_session_write_object(cl, st->p[idx].sg, obj_id, pay, moq_simpair_now_us(sp));
        moq_rcbuf_decref(pay);
        log_op(log,op,rc>=0?obj_id:UINT64_MAX); HMIX(ts,op); HMIX(ts,rc);
        if (rc == MOQ_OK) { hash_pl(&ts->rnd_fed_hash, pl, pd); ts->rnd_fed_count++; }
        if (rc < 0 && rc != MOQ_ERR_WOULD_BLOCK && rc != MOQ_ERR_STALE_HANDLE && rc != MOQ_ERR_INVAL)
            return -1;
        break; }
    case OP_CLOSE_SG: {
        FIND_ACCEPTED_SG(true, idx); if (idx < 0) { DO_PUMP; break; }
        log_op(log,op,(uint64_t)idx); HMIX(ts,op);
        moq_result_t rc = moq_session_close_subgroup(cl, st->p[idx].sg, moq_simpair_now_us(sp));
        HMIX(ts,rc);
        if (rc == MOQ_OK) st->p[idx].has_sg = false;
        break; }
    case OP_FINISH: {
        FIND_PUB(PS_ACCEPTED, idx); if (idx < 0) { DO_PUMP; break; }
        if (st->p[idx].has_sg) { DO_PUMP; break; }
        log_op(log,op,(uint64_t)idx); HMIX(ts,op);
        moq_finish_publish_cfg_t fc; moq_finish_publish_cfg_init(&fc);
        moq_result_t rc = moq_session_finish_publish(cl, st->p[idx].cli, &fc, moq_simpair_now_us(sp));
        HMIX(ts,rc);
        if (rc == MOQ_OK) st->p[idx].st = PS_DONE;
        break; }
    case OP_DRAIN_C:
        log_op(log,op,0); HMIX(ts,op);
        { moq_event_t ev[8]; size_t ne = moq_session_poll_events(cl,ev,8); HMIX(ts,ne);
          for (size_t i=0; i<ne; i++) { HMIX(ts,ev[i].kind); moq_event_cleanup(&ev[i]); } }
        break;
    case OP_DRAIN_S:
        log_op(log,op,0); HMIX(ts,op); drain_srv_evts(sv,ts); break;
    case OP_ADV_T: {
        uint64_t dt = (rng_next(rng)%100)+1;
        log_op(log,op,dt); HMIX(ts,op); HMIX(ts,dt);
        moq_simpair_advance_to(sp, moq_simpair_now_us(sp)+dt);
        break; }
    default:
        log_op(log,OP_PUMP,0); HMIX(ts,OP_PUMP);
        if (moq_simpair_run_until_quiescent(sp,8,NULL) < 0) return -1;
        break;
    }
    for (int i=0; i<MAX_PUBS; i++)
        if (st->p[i].st == PS_DONE) { st->p[i].st = PS_NONE; st->p[i].has_sg = false; }
    return 0;
}

static int run_exact_prelude(uint64_t seed, const moq_alloc_t *alloc, trace_t *ts)
{
    int rc_out = -1;
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = alloc; cfg.seed = seed ^ 0x5055424C53484544ULL; cfg.initial_now_us = 1000;
    cfg.client_send_request_capacity = true; cfg.client_initial_request_capacity = 10;
    cfg.server_send_request_capacity = true; cfg.server_initial_request_capacity = 10;

    moq_simpair_t *sp = NULL;
    if (moq_simpair_create(&cfg, &sp) < 0) return -1;
    moq_simpair_start(sp);
    moq_session_t *c = moq_simpair_client(sp), *sv = moq_simpair_server(sp);
    if (moq_simpair_run_until_quiescent(sp, 8, NULL) < 0) goto cleanup;
    moq_event_t ev;
    if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);
    if (moq_session_poll_events(sv, &ev, 1) == 1) moq_event_cleanup(&ev);

    /* 1. Client publishes. */
    moq_bytes_t nsp[] = { MOQ_BYTES_LITERAL("test") }; moq_namespace_t ns = {nsp, 1};
    moq_publish_cfg_t pc; moq_publish_cfg_init(&pc);
    pc.track_namespace = ns; pc.track_name = MOQ_BYTES_LITERAL("pub");
    moq_publication_t cpub;
    if (moq_session_publish(c, &pc, 1000, &cpub) != MOQ_OK) goto cleanup;

    /* 2. Pump. 3. Server drains PUBLISH_REQUEST, accepts. */
    if (moq_simpair_run_until_quiescent(sp, 8, NULL) < 0) goto cleanup;
    if (moq_session_poll_events(sv, &ev, 1) != 1) goto cleanup;
    if (ev.kind != MOQ_EVENT_PUBLISH_REQUEST) { moq_event_cleanup(&ev); goto cleanup; }
    moq_publication_t spub = ev.u.publish_request.pub; moq_event_cleanup(&ev);
    { moq_accept_publish_cfg_t ac; moq_accept_publish_cfg_init(&ac);
      if (moq_session_accept_publish(sv, spub, &ac, 1000) != MOQ_OK) goto cleanup; }

    /* 4. Pump. 5. Client drains PUBLISH_OK. */
    if (moq_simpair_run_until_quiescent(sp, 8, NULL) < 0) goto cleanup;
    if (moq_session_poll_events(c, &ev, 1) != 1) goto cleanup;
    if (ev.kind != MOQ_EVENT_PUBLISH_OK) { moq_event_cleanup(&ev); goto cleanup; }
    moq_event_cleanup(&ev);

    /* 6. Client opens subgroup. */
    moq_subgroup_cfg_t sg_cfg; moq_subgroup_cfg_init(&sg_cfg);
    sg_cfg.group_id = 0; sg_cfg.subgroup_id = 0; sg_cfg.publisher_priority = 128;
    moq_subgroup_handle_t sg;
    if (moq_session_open_pub_subgroup(c, cpub, &sg_cfg, 1000, &sg) != MOQ_OK) goto cleanup;

    /* 7. Write object with payload "prelude". */
    { moq_rcbuf_t *p = NULL;
      if (moq_rcbuf_create(alloc, (const uint8_t*)"prelude", 7, &p) != MOQ_OK) goto cleanup;
      moq_result_t wr = moq_session_write_object(c, sg, 0, p, 1000);
      moq_rcbuf_decref(p);
      if (wr != MOQ_OK) goto cleanup;
      hash_pl(&ts->pre_exp_hash, 7, (const uint8_t*)"prelude"); ts->pre_exp_count++; }

    /* 8. Close subgroup. 9. Pump. */
    if (moq_session_close_subgroup(c, sg, 1000) != MOQ_OK) goto cleanup;
    if (moq_simpair_run_until_quiescent(sp, 16, NULL) < 0) goto cleanup;

    /* 10. Server drains → expect OBJECT_RECEIVED with payload "prelude". */
    { bool got_obj = false;
      moq_event_t ev2[8]; size_t ne;
      while ((ne = moq_session_poll_events(sv, ev2, 8)) > 0)
          for (size_t i = 0; i < ne; i++) {
              if (ev2[i].kind == MOQ_EVENT_OBJECT_RECEIVED && ev2[i].u.object_received.payload) {
                  hash_pl(&ts->pre_rcv_hash, moq_rcbuf_len(ev2[i].u.object_received.payload),
                           moq_rcbuf_data(ev2[i].u.object_received.payload));
                  ts->pre_rcv_count++; got_obj = true;
              }
              moq_event_cleanup(&ev2[i]);
          }
      if (!got_obj) goto cleanup; }

    /* 11. Publisher finishes. */
    { moq_finish_publish_cfg_t fc; moq_finish_publish_cfg_init(&fc);
      if (moq_session_finish_publish(c, cpub, &fc, 1000) != MOQ_OK) goto cleanup; }
    if (moq_simpair_run_until_quiescent(sp, 8, NULL) < 0) goto cleanup;

    /* 12. Server receives PUBLISH_FINISHED. */
    { moq_event_t ev2;
      if (moq_session_poll_events(sv, &ev2, 1) != 1) goto cleanup;
      if (ev2.kind != MOQ_EVENT_PUBLISH_FINISHED) { moq_event_cleanup(&ev2); goto cleanup; }
      moq_event_cleanup(&ev2); }

    rc_out = 0;
cleanup:
    drain_all(c, sv, NULL); moq_simpair_destroy(sp); return rc_out;
}

static int run_scenario(uint64_t seed, const moq_alloc_t *alloc,
                         trace_t *sum, int run_id, int max_steps)
{
    memset(sum, 0, sizeof(*sum));
    sum->hash = sum->pre_exp_hash = sum->pre_rcv_hash = INIT_HASH;
    sum->rnd_fed_hash = sum->rnd_rcv_hash = INIT_HASH;

    if (run_exact_prelude(seed, alloc, sum) < 0) {
        fprintf(stderr, "FAIL seed=0x%llx run=%d: prelude failed\n",
            (unsigned long long)seed, run_id);
        REPLAY(seed, max_steps); return 1;
    }

    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = alloc; cfg.seed = seed; cfg.initial_now_us = 1000;
    cfg.client_send_request_capacity = true; cfg.client_initial_request_capacity = 32;
    cfg.server_send_request_capacity = true; cfg.server_initial_request_capacity = 32;
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
        if (execute_step(sp, &st, &rng, &log, sum, alloc) < 0) {
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
    if (!st.closed) {
        for (int f = 0; f < 16; f++) {
            if (moq_simpair_run_until_quiescent(sp, 8, NULL) < 0)
                { failures++; break; }
            drain_srv_evts(moq_simpair_server(sp), sum);
        }
    }
    drain_all(moq_simpair_client(sp), moq_simpair_server(sp), sum);
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
    size_t tp = 0, tf = 0, tr = 0;

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
        if (CMP(hash) || CMP(count) || CMP(pre_exp_count) || CMP(pre_rcv_count) ||
            CMP(pre_exp_hash) || CMP(pre_rcv_hash) || CMP(rnd_fed_count) ||
            CMP(rnd_rcv_count) || CMP(rnd_fed_hash) || CMP(rnd_rcv_hash)) {
            fprintf(stderr, "FAIL seed=0x%llx: determinism mismatch\n", (unsigned long long)seed);
            REPLAY(seed, max_steps); failures++;
        }
#undef CMP
        if (sums[0].pre_exp_count != sums[0].pre_rcv_count ||
            sums[0].pre_exp_hash != sums[0].pre_rcv_hash) {
            fprintf(stderr, "FAIL seed=0x%llx: prelude oracle exp=%zu/%llx rcv=%zu/%llx\n",
                (unsigned long long)seed, sums[0].pre_exp_count,
                (unsigned long long)sums[0].pre_exp_hash,
                sums[0].pre_rcv_count, (unsigned long long)sums[0].pre_rcv_hash);
            REPLAY(seed, max_steps); failures++;
        }
        if (sums[0].rnd_rcv_count > sums[0].rnd_fed_count) {
            fprintf(stderr, "FAIL seed=0x%llx: phantom objects fed=%zu received=%zu\n",
                (unsigned long long)seed, sums[0].rnd_fed_count, sums[0].rnd_rcv_count);
            REPLAY(seed, max_steps); failures++;
        }
        tp += sums[0].pre_rcv_count; tf += sums[0].rnd_fed_count; tr += sums[0].rnd_rcv_count;
    }

    if (failures == 0)
        fprintf(stderr, "PASS: test_scenario_publish (%llu seeds, prelude=%zu random=%zu/%zu)\n",
            (unsigned long long)num_seeds, tp, tf, tr);
    else
        fprintf(stderr, "FAIL: test_scenario_publish (%d failures in %llu seeds)\n",
            failures, (unsigned long long)num_seeds);
    return failures;
}
