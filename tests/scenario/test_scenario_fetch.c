/* Deterministic FETCH scenario runner.
 * Two-tier oracle: prelude (exact delivery) + random (received <= fed).
 * Each seed runs twice; trace hash + all oracle counters must match. */
#include <moq/sim.h>
#include "../../tests/unit/test_support.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#define REPLAY(seed, ms) fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx " \
    "MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%d ./build/tests/test_scenario_fetch\n", \
    (unsigned long long)(seed), (ms))

typedef struct { uint64_t s; } rng_t;
static uint64_t rng_next(rng_t *r) {
    uint64_t z = (r->s += 0x9E3779B97F4A7C15ULL);
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    return z ^ (z >> 31);
}
typedef struct {
    uint64_t hash;   size_t count;
    uint64_t pre_exp_hash;  size_t pre_exp_count;
    uint64_t pre_rcv_hash;  size_t pre_rcv_count;
    uint64_t rnd_fed_hash;  size_t rnd_fed_count;
    uint64_t rnd_rcv_hash;  size_t rnd_rcv_count;
} trace_t;
#define MIX(h,v) do { (h) ^= (v); (h) *= 0x100000001B3ULL; } while(0)
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
#define MAX_FETCHES 4
typedef enum { FS_NONE=0, FS_PENDING, FS_ACCEPTED, FS_DONE } fstate_t;
typedef struct { fstate_t st; moq_fetch_t srv; moq_fetch_t cli; } sfetch_t;
typedef struct { sfetch_t f[MAX_FETCHES]; bool closed; } shadow_t;
typedef enum {
    OP_PUMP, OP_FETCH, OP_ACCEPT, OP_REJECT, OP_WRITE_OBJ,
    OP_WRITE_RNG, OP_END, OP_CANCEL, OP_DRAIN_C, OP_DRAIN_S, OP_ADV_T, OP_COUNT,
} op_t;
static const char *op_name(op_t o) {
    static const char *n[] = {"PUMP","FETCH","ACCEPT","REJECT","WRITE_OBJ",
        "WRITE_RNG","END","CANCEL","DRAIN_C","DRAIN_S","ADV_T"};
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
        fprintf(stderr, "    %zu: %s p=%llu\n", i, op_name(l->e[i].op), (unsigned long long)l->e[i].p);
}

#define PUMP_FAIL(acts,i,na) do { moq_action_cleanup(&(acts)[i]); \
    for (size_t _j=i+1;_j<(na);_j++) { moq_action_cleanup(&(acts)[_j]); } \
    return -1; } while(0)

static int pump_with_chunking(moq_simpair_t *sp, rng_t *cr,
                               uint64_t *fh, size_t *fc) {
    moq_session_t *sv = moq_simpair_server(sp), *cl = moq_simpair_client(sp);
    for (int rd = 0; rd < 4; rd++) {
        moq_action_t a[16]; size_t na = moq_session_poll_actions(sv, a, 16);
        if (na == 0) {
            moq_action_t ca[16]; size_t cn = moq_session_poll_actions(cl, ca, 16);
            for (size_t i = 0; i < cn; i++) {
                if (ca[i].kind == MOQ_ACTION_SEND_CONTROL)
                    if (moq_session_on_control_bytes(sv, ca[i].u.send_control.data,
                            ca[i].u.send_control.len, moq_simpair_now_us(sp)) < 0)
                        PUMP_FAIL(ca,i,cn);
                moq_action_cleanup(&ca[i]);
            }
            if (cn == 0) break;
            continue;
        }
        for (size_t i = 0; i < na; i++) {
            if (a[i].kind == MOQ_ACTION_SEND_CONTROL) {
                if (moq_session_on_control_bytes(cl, a[i].u.send_control.data,
                        a[i].u.send_control.len, moq_simpair_now_us(sp)) < 0)
                    PUMP_FAIL(a,i,na);
            } else if (a[i].kind == MOQ_ACTION_SEND_DATA) {
                uint8_t buf[4096]; size_t bl = 0;
                uint8_t hl = a[i].u.send_data.header_len;
                if (hl) { memcpy(buf, a[i].u.send_data.header, hl); bl = hl; }
                if (a[i].u.send_data.payload) {
                    size_t pl = moq_rcbuf_len(a[i].u.send_data.payload);
                    if (bl + pl <= sizeof(buf)) {
                        memcpy(buf+bl, moq_rcbuf_data(a[i].u.send_data.payload), pl); bl += pl;
                    } else { PUMP_FAIL(a,i,na); }
                }
                bool fin = a[i].u.send_data.fin;
                moq_stream_ref_t ref = moq_stream_ref_from_u64(a[i].u.send_data.stream_ref._v + 10000);
                size_t off = 0;
                while (off < bl) {
                    size_t ch = (rng_next(cr) % 8) + 1;
                    if (ch > bl - off) ch = bl - off;
                    if (moq_session_on_data_bytes(cl, ref, buf+off, ch,
                            fin && (off+ch >= bl), moq_simpair_now_us(sp)) < 0)
                        PUMP_FAIL(a,i,na);
                    off += ch;
                }
                if (bl == 0 && fin)
                    if (moq_session_on_data_bytes(cl, ref, NULL, 0, true, moq_simpair_now_us(sp)) < 0)
                        PUMP_FAIL(a,i,na);
                if (off == bl && a[i].u.send_data.payload &&
                    moq_session_state(cl) != MOQ_SESS_CLOSED && fh && fc) {
                    hash_pl(fh, moq_rcbuf_len(a[i].u.send_data.payload),
                            moq_rcbuf_data(a[i].u.send_data.payload));
                    (*fc)++;
                }
            } else if (a[i].kind == MOQ_ACTION_RESET_DATA) {
                moq_stream_ref_t ref = moq_stream_ref_from_u64(a[i].u.reset_data.stream_ref._v + 10000);
                if (moq_session_on_data_reset(cl, ref, a[i].u.reset_data.error_code,
                        moq_simpair_now_us(sp)) < 0)
                    PUMP_FAIL(a,i,na);
            }
            moq_action_cleanup(&a[i]);
        }
    }
    return 0;
}

#define HMIX(ts, v) MIX((ts)->hash, (uint64_t)(v))

static void drain_cli_evts(moq_session_t *c, trace_t *ts) {
    moq_event_t ev[16]; size_t ne;
    while ((ne = moq_session_poll_events(c, ev, 16)) > 0)
        for (size_t i = 0; i < ne; i++) {
            HMIX(ts, ev[i].kind);
            if (ev[i].kind == MOQ_EVENT_FETCH_OBJECT && ev[i].u.fetch_object.payload) {
                hash_pl(&ts->rnd_rcv_hash, moq_rcbuf_len(ev[i].u.fetch_object.payload),
                         moq_rcbuf_data(ev[i].u.fetch_object.payload));
                ts->rnd_rcv_count++;
            }
            moq_event_cleanup(&ev[i]);
        }
}

static void drain_all(moq_session_t *c, moq_session_t *sv, trace_t *ts) {
    if (ts) drain_cli_evts(c, ts);
    else { moq_event_t d[16]; size_t ne;
        while ((ne = moq_session_poll_events(c, d, 16)) > 0)
            for (size_t i = 0; i < ne; i++) moq_event_cleanup(&d[i]); }
    { moq_event_t d[16]; size_t ne;
      while ((ne = moq_session_poll_events(sv, d, 16)) > 0)
          for (size_t i = 0; i < ne; i++) moq_event_cleanup(&d[i]); }
    { moq_action_t a[16]; size_t na;
      while ((na = moq_session_poll_actions(sv, a, 16)) > 0)
          for (size_t i = 0; i < na; i++) moq_action_cleanup(&a[i]);
      while ((na = moq_session_poll_actions(c, a, 16)) > 0)
          for (size_t i = 0; i < na; i++) moq_action_cleanup(&a[i]); }
}

#define DO_PUMP do { log_op(log,OP_PUMP,0); HMIX(ts,OP_PUMP); \
    if (pump_with_chunking(sp,crng,&ts->rnd_fed_hash,&ts->rnd_fed_count) < 0) return -1; } while(0)
#define FIND(st_val, out) int out = -1; \
    for (int _i = 0; _i < MAX_FETCHES; _i++) if (st->f[_i].st == (st_val)) { out = _i; break; }
static int execute_step(moq_simpair_t *sp, shadow_t *st,
                         rng_t *rng, rng_t *crng, oplog_t *log,
                         trace_t *ts, const moq_alloc_t *alloc)
{
    if (st->closed) return 0;
    op_t op = (op_t)(rng_next(rng) % OP_COUNT);
    moq_session_t *cl = moq_simpair_client(sp), *sv = moq_simpair_server(sp);

    switch (op) {
    case OP_PUMP:
        log_op(log, op, 0); HMIX(ts, op);
        if (pump_with_chunking(sp, crng, &ts->rnd_fed_hash, &ts->rnd_fed_count) < 0) return -1;
        break;
    case OP_FETCH: {
        FIND(FS_NONE, slot); if (slot < 0) { DO_PUMP; break; }
        moq_bytes_t nsp[] = { MOQ_BYTES_LITERAL("test") };
        moq_namespace_t ns = { nsp, 1 };
        char nm[16]; snprintf(nm, sizeof(nm), "f%llu", (unsigned long long)(rng_next(rng)%1000));
        moq_fetch_cfg_t fc; moq_fetch_cfg_init(&fc);
        fc.track_namespace = ns; fc.track_name = moq_bytes_cstr(nm);
        fc.end_group = (rng_next(rng) % 5) + 1;
        moq_fetch_t h; moq_result_t rc = moq_session_fetch(cl, &fc, moq_simpair_now_us(sp), &h);
        log_op(log, op, rc >= 0 ? 1 : 0); HMIX(ts, op); HMIX(ts, rc);
        if (rc == MOQ_OK) {
            if (pump_with_chunking(sp, crng, &ts->rnd_fed_hash, &ts->rnd_fed_count) < 0) return -1;
            moq_event_t ev[4]; size_t ne = moq_session_poll_events(sv, ev, 4);
            HMIX(ts, ne);
            for (size_t j = 0; j < ne; j++) {
                HMIX(ts, ev[j].kind);
                if (ev[j].kind == MOQ_EVENT_FETCH_REQUEST)
                    { st->f[slot] = (sfetch_t){FS_PENDING, ev[j].u.fetch_request.fetch, h}; }
                moq_event_cleanup(&ev[j]);
            }
        }
        break; }
    case OP_ACCEPT: {
        FIND(FS_PENDING, idx); if (idx < 0) { DO_PUMP; break; }
        log_op(log, op, (uint64_t)idx); HMIX(ts, op);
        moq_accept_fetch_cfg_t ac; moq_accept_fetch_cfg_init(&ac); ac.end_group = 5;
        moq_result_t rc = moq_session_accept_fetch(sv, st->f[idx].srv, &ac, moq_simpair_now_us(sp));
        HMIX(ts, rc);
        if (rc == MOQ_OK) {
            st->f[idx].st = FS_ACCEPTED;
            if (pump_with_chunking(sp, crng, &ts->rnd_fed_hash, &ts->rnd_fed_count) < 0) return -1;
            moq_event_t ev[4]; size_t ne = moq_session_poll_events(cl, ev, 4);
            HMIX(ts, ne);
            for (size_t j = 0; j < ne; j++) { HMIX(ts, ev[j].kind); moq_event_cleanup(&ev[j]); }
        }
        break; }
    case OP_REJECT: {
        FIND(FS_PENDING, idx); if (idx < 0) { DO_PUMP; break; }
        log_op(log, op, (uint64_t)idx); HMIX(ts, op);
        moq_reject_fetch_cfg_t rj; moq_reject_fetch_cfg_init(&rj);
        rj.error_code = MOQ_REQUEST_ERROR_DOES_NOT_EXIST; rj.reason = MOQ_BYTES_LITERAL("no");
        moq_result_t rc = moq_session_reject_fetch(sv, st->f[idx].srv, &rj, moq_simpair_now_us(sp));
        HMIX(ts, rc);
        if (rc == MOQ_OK) {
            st->f[idx].st = FS_DONE;
            if (pump_with_chunking(sp, crng, &ts->rnd_fed_hash, &ts->rnd_fed_count) < 0) return -1;
            moq_event_t ev[4]; size_t ne = moq_session_poll_events(cl, ev, 4);
            HMIX(ts, ne);
            for (size_t j = 0; j < ne; j++) { HMIX(ts, ev[j].kind); moq_event_cleanup(&ev[j]); }
        }
        break; }
    case OP_WRITE_OBJ: {
        FIND(FS_ACCEPTED, idx); if (idx < 0) { DO_PUMP; break; }
        size_t pl = rng_next(rng) % 64; uint8_t pd[64];
        for (size_t j = 0; j < pl; j++) pd[j] = (uint8_t)(rng_next(rng) & 0xFF);
        moq_rcbuf_t *pay = NULL;
        moq_rcbuf_create(alloc, pd, pl, &pay);
        if (!pay) { log_op(log, OP_PUMP, 0); break; }
        moq_fetch_object_cfg_t oc; moq_fetch_object_cfg_init(&oc);
        oc.group_id = rng_next(rng) % 5; oc.object_id = rng_next(rng) % 3;
        oc.publisher_priority = 128; oc.payload = pay;
        moq_result_t rc = moq_session_write_fetch_object(sv, st->f[idx].srv, &oc, moq_simpair_now_us(sp));
        moq_rcbuf_decref(pay);
        log_op(log, op, rc >= 0 ? oc.group_id : UINT64_MAX);
        HMIX(ts, op); HMIX(ts, rc);
        if (rc < 0 && rc != MOQ_ERR_WOULD_BLOCK && rc != MOQ_ERR_STALE_HANDLE) return -1;
        break; }
    case OP_WRITE_RNG: {
        FIND(FS_ACCEPTED, idx); if (idx < 0) { DO_PUMP; break; }
        log_op(log, op, (uint64_t)idx); HMIX(ts, op);
        moq_result_t rc = moq_session_write_fetch_range(sv, st->f[idx].srv,
            MOQ_FETCH_RANGE_NON_EXISTENT, rng_next(rng) % 5, 0, moq_simpair_now_us(sp));
        HMIX(ts, rc);
        if (rc < 0 && rc != MOQ_ERR_WOULD_BLOCK && rc != MOQ_ERR_STALE_HANDLE) return -1;
        break; }
    case OP_END: {
        FIND(FS_ACCEPTED, idx); if (idx < 0) { DO_PUMP; break; }
        log_op(log, op, (uint64_t)idx); HMIX(ts, op);
        moq_result_t rc = moq_session_end_fetch(sv, st->f[idx].srv, moq_simpair_now_us(sp));
        HMIX(ts, rc);
        if (rc == MOQ_OK) st->f[idx].st = FS_DONE;
        break; }
    case OP_CANCEL: {
        FIND(FS_PENDING, idx); if (idx < 0) { DO_PUMP; break; }
        log_op(log, op, (uint64_t)idx); HMIX(ts, op);
        moq_result_t rc = moq_session_fetch_cancel(cl, st->f[idx].cli, moq_simpair_now_us(sp));
        HMIX(ts, rc);
        if (rc == MOQ_OK) st->f[idx].st = FS_DONE;
        break; }
    case OP_DRAIN_C:
        log_op(log, op, 0); HMIX(ts, op); drain_cli_evts(cl, ts); break;
    case OP_DRAIN_S: {
        log_op(log, op, 0); HMIX(ts, op);
        moq_event_t ev[8]; size_t ne = moq_session_poll_events(sv, ev, 8);
        HMIX(ts, ne);
        for (size_t i = 0; i < ne; i++) {
            HMIX(ts, ev[i].kind);
            if (ev[i].kind == MOQ_EVENT_FETCH_CANCELLED)
                for (int k = 0; k < MAX_FETCHES; k++)
                    if (st->f[k].st != FS_NONE &&
                        moq_fetch_eq(st->f[k].srv, ev[i].u.fetch_cancelled.fetch))
                        st->f[k].st = FS_DONE;
            moq_event_cleanup(&ev[i]);
        }
        break; }
    case OP_ADV_T: {
        uint64_t dt = (rng_next(rng) % 100) + 1;
        log_op(log, op, dt); HMIX(ts, op); HMIX(ts, dt);
        moq_simpair_advance_to(sp, moq_simpair_now_us(sp) + dt);
        break; }
    default:
        log_op(log, OP_PUMP, 0); HMIX(ts, OP_PUMP);
        if (pump_with_chunking(sp, crng, &ts->rnd_fed_hash, &ts->rnd_fed_count) < 0) return -1;
        break;
    }
    for (int i = 0; i < MAX_FETCHES; i++)
        if (st->f[i].st == FS_DONE) st->f[i].st = FS_NONE;
    return 0;
}

static int run_exact_prelude(uint64_t seed, const moq_alloc_t *alloc, trace_t *ts)
{
    int rc_out = -1;
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = alloc; cfg.seed = seed ^ 0x46455443ULL;
    cfg.initial_now_us = 1000;
    cfg.client_send_request_capacity = true; cfg.client_initial_request_capacity = 10;
    cfg.server_send_request_capacity = true; cfg.server_initial_request_capacity = 10;

    moq_simpair_t *sp = NULL;
    if (moq_simpair_create(&cfg, &sp) < 0) return -1;
    moq_simpair_start(sp);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_session_t *c = moq_simpair_client(sp), *sv = moq_simpair_server(sp);
    moq_event_t ev;
    if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);
    if (moq_session_poll_events(sv, &ev, 1) == 1) moq_event_cleanup(&ev);

    /* 1. Client sends FETCH. */
    moq_bytes_t nsp[] = { MOQ_BYTES_LITERAL("test") };
    moq_namespace_t ns = { nsp, 1 };
    moq_fetch_cfg_t fc; moq_fetch_cfg_init(&fc);
    fc.track_namespace = ns; fc.track_name = MOQ_BYTES_LITERAL("fetch"); fc.end_group = 5;
    moq_fetch_t cfh;
    if (moq_session_fetch(c, &fc, 1000, &cfh) != MOQ_OK) goto cleanup;

    /* 2-3. Pump, server drains FETCH_REQUEST. */
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    if (moq_session_poll_events(sv, &ev, 1) != 1) goto cleanup;
    if (ev.kind != MOQ_EVENT_FETCH_REQUEST) { moq_event_cleanup(&ev); goto cleanup; }
    moq_fetch_t sfh = ev.u.fetch_request.fetch;
    moq_event_cleanup(&ev);

    /* 4-5. Server accepts, pump. */
    { moq_accept_fetch_cfg_t ac; moq_accept_fetch_cfg_init(&ac);
      ac.end_group = 5; ac.empty = false;
      if (moq_session_accept_fetch(sv, sfh, &ac, 1000) != MOQ_OK) goto cleanup; }
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    /* 6. Server writes two fetch objects. */
    { static const struct { uint64_t g,o; const char *d; size_t l; } objs[] =
          { {0,0,"obj0",4}, {1,0,"obj1",4} };
      for (size_t oi = 0; oi < 2; oi++) {
          moq_rcbuf_t *p = NULL;
          if (moq_rcbuf_create(alloc, (const uint8_t*)objs[oi].d, objs[oi].l, &p) != MOQ_OK) goto cleanup;
          moq_fetch_object_cfg_t oc; moq_fetch_object_cfg_init(&oc);
          oc.group_id = objs[oi].g; oc.object_id = objs[oi].o;
          oc.publisher_priority = 128; oc.payload = p;
          moq_result_t wr = moq_session_write_fetch_object(sv, sfh, &oc, 1000);
          moq_rcbuf_decref(p);
          if (wr != MOQ_OK) goto cleanup;
          hash_pl(&ts->pre_exp_hash, objs[oi].l, (const uint8_t*)objs[oi].d);
          ts->pre_exp_count++;
      } }

    /* 7. Gap. 8. End fetch. */
    if (moq_session_write_fetch_range(sv, sfh, MOQ_FETCH_RANGE_NON_EXISTENT, 2, 0, 1000) != MOQ_OK)
        goto cleanup;
    if (moq_session_end_fetch(sv, sfh, 1000) != MOQ_OK) goto cleanup;

    /* 9. Pump until done. */
    moq_simpair_run_until_quiescent(sp, 16, NULL);

    /* 10. Client drains: FETCH_OK, FETCH_OBJECT x2, FETCH_GAP, FETCH_COMPLETE. */
    { bool got_ok = false, got_done = false, gap_ok = false;
      size_t got_obj = 0;
      moq_event_t ev2[8]; size_t ne;
      while ((ne = moq_session_poll_events(c, ev2, 8)) > 0)
          for (size_t i = 0; i < ne; i++) {
              if (ev2[i].kind == MOQ_EVENT_FETCH_OK) got_ok = true;
              else if (ev2[i].kind == MOQ_EVENT_FETCH_OBJECT) {
                  if (ev2[i].u.fetch_object.payload) {
                      hash_pl(&ts->pre_rcv_hash, moq_rcbuf_len(ev2[i].u.fetch_object.payload),
                              moq_rcbuf_data(ev2[i].u.fetch_object.payload));
                      ts->pre_rcv_count++;
                  }
                  got_obj++;
              } else if (ev2[i].kind == MOQ_EVENT_FETCH_GAP) {
                  if (ev2[i].u.fetch_gap.range_kind == MOQ_FETCH_RANGE_NON_EXISTENT &&
                      ev2[i].u.fetch_gap.group_id == 2 &&
                      ev2[i].u.fetch_gap.object_id == 0)
                      gap_ok = true;
              } else if (ev2[i].kind == MOQ_EVENT_FETCH_COMPLETE) got_done = true;
              moq_event_cleanup(&ev2[i]);
          }
      if (!got_ok || got_obj != 2 || !gap_ok || !got_done) goto cleanup; }

    /* -- Joining fetch prelude ----------------------------------------- */
    /* Subscribe with LARGEST_OBJECT, accept with largest=(5,2), then
     * send a relative joining fetch 2 groups back. */
    {
        moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
        moq_bytes_t sub_ns[] = { MOQ_BYTES_LITERAL("test") };
        scfg.track_namespace = (moq_namespace_t){ sub_ns, 1 };
        scfg.track_name = MOQ_BYTES_LITERAL("jf");
        scfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        moq_subscription_t csub;
        if (moq_session_subscribe(c, &scfg, 2000, &csub) != MOQ_OK) goto cleanup;
        if (moq_simpair_run_until_quiescent(sp, 8, NULL) < 0) goto cleanup;
        if (moq_session_poll_events(sv, &ev, 1) != 1) goto cleanup;
        if (ev.kind != MOQ_EVENT_SUBSCRIBE_REQUEST) { moq_event_cleanup(&ev); goto cleanup; }
        moq_subscription_t ssub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
        moq_accept_subscribe_cfg_t as; moq_accept_subscribe_cfg_init(&as);
        as.has_largest = true; as.largest_group = 5; as.largest_object = 2;
        if (moq_session_accept_subscribe(sv, ssub, &as, 2000) != MOQ_OK) goto cleanup;
        if (moq_simpair_run_until_quiescent(sp, 8, NULL) < 0) goto cleanup;
        if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);

        moq_fetch_cfg_t jfc; moq_fetch_cfg_init(&jfc);
        jfc.is_joining = true;
        jfc.joining_relative = true;
        jfc.joining_sub = csub;
        jfc.joining_start = 2;
        moq_fetch_t jfh;
        if (moq_session_fetch(c, &jfc, 3000, &jfh) != MOQ_OK) goto cleanup;
        if (moq_simpair_run_until_quiescent(sp, 8, NULL) < 0) goto cleanup;

        if (moq_session_poll_events(sv, &ev, 1) != 1) goto cleanup;
        if (ev.kind != MOQ_EVENT_FETCH_REQUEST) { moq_event_cleanup(&ev); goto cleanup; }
        if (ev.u.fetch_request.start_group != 3) { moq_event_cleanup(&ev); goto cleanup; }
        if (ev.u.fetch_request.end_group != 5) { moq_event_cleanup(&ev); goto cleanup; }
        if (ev.u.fetch_request.end_object != 3) { moq_event_cleanup(&ev); goto cleanup; }
        moq_fetch_t sjfh = ev.u.fetch_request.fetch;
        moq_event_cleanup(&ev);

        moq_accept_fetch_cfg_t jac; moq_accept_fetch_cfg_init(&jac);
        jac.end_group = 5; jac.end_object = 3; jac.empty = false;
        if (moq_session_accept_fetch(sv, sjfh, &jac, 3000) != MOQ_OK) goto cleanup;

        moq_rcbuf_t *jp = NULL;
        if (moq_rcbuf_create(alloc, (const uint8_t*)"join", 4, &jp) != MOQ_OK) goto cleanup;
        moq_fetch_object_cfg_t joc; moq_fetch_object_cfg_init(&joc);
        joc.group_id = 3; joc.object_id = 0; joc.publisher_priority = 128;
        joc.payload = jp;
        moq_result_t jwr = moq_session_write_fetch_object(sv, sjfh, &joc, 3000);
        moq_rcbuf_decref(jp);
        if (jwr != MOQ_OK) goto cleanup;
        if (moq_session_end_fetch(sv, sjfh, 3000) != MOQ_OK) goto cleanup;
        if (moq_simpair_run_until_quiescent(sp, 16, NULL) < 0) goto cleanup;

        bool got_jobj = false, got_jok = false, got_jdone = false;
        moq_event_t jev[16]; size_t jne;
        while ((jne = moq_session_poll_events(c, jev, 16)) > 0)
            for (size_t ji = 0; ji < jne; ji++) {
                if (jev[ji].kind == MOQ_EVENT_FETCH_OK) got_jok = true;
                if (jev[ji].kind == MOQ_EVENT_FETCH_OBJECT &&
                    jev[ji].u.fetch_object.payload) {
                    hash_pl(&ts->pre_exp_hash,
                             moq_rcbuf_len(jev[ji].u.fetch_object.payload),
                             moq_rcbuf_data(jev[ji].u.fetch_object.payload));
                    ts->pre_exp_count++;
                    got_jobj = true;
                }
                if (jev[ji].kind == MOQ_EVENT_FETCH_COMPLETE) got_jdone = true;
                moq_event_cleanup(&jev[ji]);
            }
        if (!got_jok || !got_jobj || !got_jdone) goto cleanup;
        hash_pl(&ts->pre_rcv_hash, 4, (const uint8_t*)"join");
        ts->pre_rcv_count++;
    }

    rc_out = 0;
cleanup:
    drain_all(c, sv, NULL);
    moq_simpair_destroy(sp);
    return rc_out;
}

static int run_scenario(uint64_t seed, const moq_alloc_t *alloc,
                         trace_t *sum, int run_id, int max_steps)
{
    memset(sum, 0, sizeof(*sum));
    sum->hash = sum->pre_exp_hash = sum->pre_rcv_hash = INIT_HASH;
    sum->rnd_fed_hash = sum->rnd_rcv_hash = INIT_HASH;

    if (run_exact_prelude(seed, alloc, sum) < 0) {
        fprintf(stderr, "FAIL seed=0x%llx run=%d: prelude failed\n", (unsigned long long)seed, run_id);
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
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1) moq_event_cleanup(&ev);
      if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) moq_event_cleanup(&ev); }

    shadow_t st; memset(&st, 0, sizeof(st));
    rng_t rng = { seed }, crng = { seed ^ 0xDEADBEEFCAFE1234ULL };
    oplog_t log = {0}; int failures = 0;

    for (int step = 0; step < max_steps; step++) {
        if (execute_step(sp, &st, &rng, &crng, &log, sum, alloc) < 0) {
            fprintf(stderr, "FAIL seed=0x%llx run=%d step=%d: data input error\n",
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

    /* Final pump to flush remaining data. */
    if (!st.closed) {
        for (int f = 0; f < 16; f++) {
            if (pump_with_chunking(sp, &crng, &sum->rnd_fed_hash, &sum->rnd_fed_count) < 0)
                { failures++; break; }
            drain_cli_evts(moq_simpair_client(sp), sum);
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
        fprintf(stderr, "PASS: test_scenario_fetch (%llu seeds, prelude=%zu random=%zu/%zu)\n",
                (unsigned long long)num_seeds, tp, tf, tr);
    else
        fprintf(stderr, "FAIL: test_scenario_fetch (%d failures in %llu seeds)\n",
                failures, (unsigned long long)num_seeds);
    return failures;
}
