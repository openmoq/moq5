/* Deterministic OBJECT_DATAGRAM scenario runner.
 * Two-tier oracle: prelude (exact delivery) + random (received <= fed).
 * Each seed runs twice; trace hash + all oracle counters must match. */
#include <moq/sim.h>
#include "../../tests/unit/test_support.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#define REPLAY(seed, ms) fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx " \
    "MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%d ./build/tests/test_scenario_object_datagram\n", \
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
    uint64_t rnd_prop_fed_hash; size_t rnd_prop_fed_count;
    uint64_t rnd_prop_rcv_hash; size_t rnd_prop_rcv_count;
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

typedef enum {
    OP_PUMP, OP_SEND_DG, OP_SEND_STATUS_DG, OP_DRAIN_C, OP_DRAIN_S, OP_ADV_T,
    OP_COUNT,
} op_t;

typedef struct {
    size_t fed; size_t rcv; size_t prop_fed; size_t prop_rcv;
} run_counters_t;

static int run_one(uint64_t seed, int max_steps, run_counters_t *out)
{
    memset(out, 0, sizeof(*out));
    scen_alloc_t sa = {0};
    moq_alloc_t alloc = { &sa, sa_alloc, sa_realloc, sa_free };

    trace_t traces[2];
    for (int run = 0; run < 2; run++) {
        memset(&traces[run], 0, sizeof(traces[run]));
        traces[run].hash = INIT_HASH;
        traces[run].pre_exp_hash = INIT_HASH;
        traces[run].pre_rcv_hash = INIT_HASH;
        traces[run].rnd_fed_hash = INIT_HASH;
        traces[run].rnd_rcv_hash = INIT_HASH;
        traces[run].rnd_prop_fed_hash = INIT_HASH;
        traces[run].rnd_prop_rcv_hash = INIT_HASH;

        moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.seed = seed;
        cfg.client_send_request_capacity = true;
        cfg.client_initial_request_capacity = 16;
        cfg.server_send_request_capacity = true;
        cfg.server_initial_request_capacity = 16;
        cfg.trace_fn = trace_hash_fn;
        cfg.trace_ctx = &traces[run];

        moq_simpair_t *sp = NULL;
        if (moq_simpair_create(&cfg, &sp) < 0) return -1;
        if (moq_simpair_start(sp) < 0) { moq_simpair_destroy(sp); return -1; }
        if (moq_simpair_run_until_quiescent(sp, 8, NULL) < 0) {
            moq_simpair_destroy(sp); return -1;
        }

        moq_session_t *cl = moq_simpair_client(sp);
        moq_session_t *sv = moq_simpair_server(sp);
        trace_t *ts = &traces[run];

        /* Drain setup events. */
        drain_all(cl, sv);

        /* -- Prelude: subscribe -> accept -> send datagram -> receive -- */

        /* Client subscribes. */
        moq_subscribe_cfg_t scfg;
        moq_subscribe_cfg_init(&scfg);
        moq_bytes_t nsp[] = { MOQ_BYTES_LITERAL("test") };
        moq_namespace_t ns = { nsp, 1 };
        scfg.track_namespace = ns;
        scfg.track_name = MOQ_BYTES_LITERAL("dgram");
        scfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t cli_sub;
        if (moq_session_subscribe(cl, &scfg, moq_simpair_now_us(sp), &cli_sub) < 0) {
            moq_simpair_destroy(sp); return -1;
        }
        if (moq_simpair_run_until_quiescent(sp, 8, NULL) < 0) {
            moq_simpair_destroy(sp); return -1;
        }

        /* Server receives subscribe request + accepts. */
        moq_event_t ev[4];
        size_t ne = moq_session_poll_events(sv, ev, 4);
        moq_subscription_t srv_sub = {0};
        for (size_t j = 0; j < ne; j++) {
            if (ev[j].kind == MOQ_EVENT_SUBSCRIBE_REQUEST)
                srv_sub = ev[j].u.subscribe_request.sub;
            moq_event_cleanup(&ev[j]);
        }
        if (!moq_subscription_is_valid(srv_sub)) {
            moq_simpair_destroy(sp); return -1;
        }

        moq_accept_subscribe_cfg_t acfg;
        moq_accept_subscribe_cfg_init(&acfg);
        if (moq_session_accept_subscribe(sv, srv_sub, &acfg,
            moq_simpair_now_us(sp)) < 0) {
            moq_simpair_destroy(sp); return -1;
        }
        if (moq_simpair_run_until_quiescent(sp, 8, NULL) < 0) {
            moq_simpair_destroy(sp); return -1;
        }
        drain_all(cl, sv);

        /* Server sends a payload datagram. */
        uint8_t pay_data[] = "hello_datagram";
        moq_rcbuf_t *pay = NULL;
        if (moq_rcbuf_create(&alloc, pay_data, sizeof(pay_data) - 1, &pay) < 0) {
            moq_simpair_destroy(sp); return -1;
        }
        hash_pl(&ts->pre_exp_hash, sizeof(pay_data) - 1, pay_data);
        ts->pre_exp_count++;

        if (moq_session_send_object_datagram(sv, srv_sub,
            0, 1, 128, false, pay, NULL, 0,
            moq_simpair_now_us(sp)) < 0) {
            moq_rcbuf_decref(pay);
            moq_simpair_destroy(sp); return -1;
        }
        moq_rcbuf_decref(pay);

        if (moq_simpair_run_until_quiescent(sp, 8, NULL) < 0) {
            moq_simpair_destroy(sp); return -1;
        }

        /* Client receives datagram event. */
        ne = moq_session_poll_events(cl, ev, 4);
        for (size_t j = 0; j < ne; j++) {
            if (ev[j].kind == MOQ_EVENT_OBJECT_RECEIVED) {
                moq_object_received_event_t *obj = &ev[j].u.object_received;
                if (obj->datagram && obj->payload) {
                    hash_pl(&ts->pre_rcv_hash,
                             moq_rcbuf_len(obj->payload),
                             moq_rcbuf_data(obj->payload));
                    ts->pre_rcv_count++;
                }
            }
            moq_event_cleanup(&ev[j]);
        }

        /* Server sends status datagram. */
        if (moq_session_send_status_datagram(sv, srv_sub,
            0, 2, 128, MOQ_OBJECT_END_OF_GROUP,
            moq_simpair_now_us(sp)) < 0) {
            moq_simpair_destroy(sp); return -1;
        }
        if (moq_simpair_run_until_quiescent(sp, 8, NULL) < 0) {
            moq_simpair_destroy(sp); return -1;
        }

        /* Client must receive a matching status datagram event. */
        ne = moq_session_poll_events(cl, ev, 4);
        {
            bool got_status = false;
            for (size_t j = 0; j < ne; j++) {
                if (ev[j].kind == MOQ_EVENT_OBJECT_RECEIVED) {
                    moq_object_received_event_t *obj = &ev[j].u.object_received;
                    if (obj->datagram &&
                        obj->status == MOQ_OBJECT_END_OF_GROUP &&
                        obj->group_id == 0 &&
                        obj->object_id == 2 &&
                        obj->payload == NULL)
                        got_status = true;
                    HMIX(ts, obj->datagram ? 1 : 0);
                    HMIX(ts, obj->status);
                    HMIX(ts, obj->group_id);
                    HMIX(ts, obj->object_id);
                }
                moq_event_cleanup(&ev[j]);
            }
            if (!got_status) { moq_simpair_destroy(sp); return -1; }
        }

        /* -- Random phase ---------------------------------------------- */
        rng_t rng = { seed ^ 0xDA7A60A3ULL };
        bool closed = false;

        for (int step = 0; step < max_steps && !closed; step++) {
            op_t op = (op_t)(rng_next(&rng) % OP_COUNT);
            HMIX(ts, op);

            switch (op) {
            case OP_PUMP:
                if (moq_simpair_run_until_quiescent(sp, 8, NULL) < 0)
                    closed = true;
                break;
            case OP_SEND_DG: {
                size_t pl = 1 + rng_next(&rng) % 63;
                uint8_t pd[64];
                for (size_t j = 0; j < pl; j++)
                    pd[j] = (uint8_t)(rng_next(&rng) & 0xFF);
                moq_rcbuf_t *p = NULL;
                if (moq_rcbuf_create(&alloc, pd, pl, &p) < 0) break;
                uint64_t gid = rng_next(&rng) % 4;
                uint64_t oid = 1 + rng_next(&rng) % 15;
                bool eog = (rng_next(&rng) % 8) == 0;
                bool use_props = (rng_next(&rng) % 4) == 0;
                uint8_t prop_buf[8];
                size_t prop_len = 0;
                if (use_props) {
                    prop_len = 1 + rng_next(&rng) % 7;
                    for (size_t j = 0; j < prop_len; j++)
                        prop_buf[j] = (uint8_t)(rng_next(&rng) & 0xFF);
                }
                moq_result_t rc = moq_session_send_object_datagram(
                    sv, srv_sub, gid, oid, 128, eog, p,
                    use_props ? prop_buf : NULL, prop_len,
                    moq_simpair_now_us(sp));
                moq_rcbuf_decref(p);
                if (rc == MOQ_OK) {
                    hash_pl(&ts->rnd_fed_hash, pl, pd);
                    ts->rnd_fed_count++;
                    if (use_props) {
                        hash_pl(&ts->rnd_prop_fed_hash, prop_len, prop_buf);
                        ts->rnd_prop_fed_count++;
                    }
                }
                HMIX(ts, rc);
                break;
            }
            case OP_SEND_STATUS_DG: {
                uint64_t gid = rng_next(&rng) % 4;
                uint64_t oid = rng_next(&rng) % 16;
                moq_result_t rc = moq_session_send_status_datagram(
                    sv, srv_sub, gid, oid, 128, MOQ_OBJECT_END_OF_GROUP,
                    moq_simpair_now_us(sp));
                HMIX(ts, rc);
                break;
            }
            case OP_DRAIN_C: {
                moq_event_t de[16];
                size_t dne = moq_session_poll_events(cl, de, 16);
                for (size_t j = 0; j < dne; j++) {
                    HMIX(ts, de[j].kind);
                    if (de[j].kind == MOQ_EVENT_OBJECT_RECEIVED) {
                        moq_object_received_event_t *obj = &de[j].u.object_received;
                        if (obj->payload) {
                            hash_pl(&ts->rnd_rcv_hash,
                                     moq_rcbuf_len(obj->payload),
                                     moq_rcbuf_data(obj->payload));
                            ts->rnd_rcv_count++;
                        }
                        if (obj->properties) {
                            hash_pl(&ts->rnd_prop_rcv_hash,
                                     moq_rcbuf_len(obj->properties),
                                     moq_rcbuf_data(obj->properties));
                            ts->rnd_prop_rcv_count++;
                        }
                    }
                    if (de[j].kind == MOQ_EVENT_SESSION_CLOSED)
                        closed = true;
                    moq_event_cleanup(&de[j]);
                }
                break;
            }
            case OP_DRAIN_S: {
                moq_event_t de[16];
                size_t dne = moq_session_poll_events(sv, de, 16);
                for (size_t j = 0; j < dne; j++) {
                    HMIX(ts, de[j].kind);
                    if (de[j].kind == MOQ_EVENT_SESSION_CLOSED)
                        closed = true;
                    moq_event_cleanup(&de[j]);
                }
                break;
            }
            case OP_ADV_T: {
                uint64_t dt = rng_next(&rng) % 100000;
                moq_simpair_advance_to(sp, moq_simpair_now_us(sp) + dt);
                break;
            }
            default: break;
            }
        }

        drain_all(cl, sv);
        moq_simpair_destroy(sp);
    }

    /* Compare two runs. */
    if (traces[0].hash != traces[1].hash ||
        traces[0].count != traces[1].count ||
        traces[0].pre_exp_hash != traces[1].pre_exp_hash ||
        traces[0].pre_exp_count != traces[1].pre_exp_count ||
        traces[0].pre_rcv_hash != traces[1].pre_rcv_hash ||
        traces[0].pre_rcv_count != traces[1].pre_rcv_count ||
        traces[0].rnd_fed_hash != traces[1].rnd_fed_hash ||
        traces[0].rnd_fed_count != traces[1].rnd_fed_count ||
        traces[0].rnd_rcv_hash != traces[1].rnd_rcv_hash ||
        traces[0].rnd_rcv_count != traces[1].rnd_rcv_count ||
        traces[0].rnd_prop_fed_hash != traces[1].rnd_prop_fed_hash ||
        traces[0].rnd_prop_fed_count != traces[1].rnd_prop_fed_count ||
        traces[0].rnd_prop_rcv_hash != traces[1].rnd_prop_rcv_hash ||
        traces[0].rnd_prop_rcv_count != traces[1].rnd_prop_rcv_count) {
        fprintf(stderr, "FAIL: determinism mismatch seed=0x%llx\n",
                (unsigned long long)seed);
        REPLAY(seed, max_steps);
        return -1;
    }

    /* Prelude oracle: payload datagram delivered exactly. */
    if (traces[0].pre_exp_count != traces[0].pre_rcv_count ||
        traces[0].pre_exp_hash != traces[0].pre_rcv_hash) {
        fprintf(stderr, "FAIL: prelude mismatch seed=0x%llx "
                "exp=%zu rcv=%zu\n", (unsigned long long)seed,
                traces[0].pre_exp_count, traces[0].pre_rcv_count);
        REPLAY(seed, max_steps);
        return -1;
    }

    /* Random oracle: received <= fed (payload and properties). */
    if (traces[0].rnd_rcv_count > traces[0].rnd_fed_count) {
        fprintf(stderr, "FAIL: rcv > fed seed=0x%llx "
                "fed=%zu rcv=%zu\n", (unsigned long long)seed,
                traces[0].rnd_fed_count, traces[0].rnd_rcv_count);
        REPLAY(seed, max_steps);
        return -1;
    }
    if (traces[0].rnd_prop_rcv_count > traces[0].rnd_prop_fed_count) {
        fprintf(stderr, "FAIL: prop rcv > fed seed=0x%llx "
                "fed=%zu rcv=%zu\n", (unsigned long long)seed,
                traces[0].rnd_prop_fed_count, traces[0].rnd_prop_rcv_count);
        REPLAY(seed, max_steps);
        return -1;
    }

    if (sa.balance != 0) {
        fprintf(stderr, "FAIL: alloc leak seed=0x%llx balance=%lld\n",
                (unsigned long long)seed, (long long)sa.balance);
        REPLAY(seed, max_steps);
        return -1;
    }

    out->fed = traces[0].rnd_fed_count;
    out->rcv = traces[0].rnd_rcv_count;
    out->prop_fed = traces[0].rnd_prop_fed_count;
    out->prop_rcv = traces[0].rnd_prop_rcv_count;
    return 0;
}

int main(void)
{
    int failures = 0;

    uint64_t seed_start = 0;
    uint64_t num_seeds = 100;
    int max_steps = 50;

    const char *env_start = getenv("MOQ_SCENARIO_SEED_START");
    const char *env_seeds = getenv("MOQ_SCENARIO_SEEDS");
    const char *env_steps = getenv("MOQ_SCENARIO_STEPS");
    if (env_start) seed_start = strtoull(env_start, NULL, 0);
    if (env_seeds) num_seeds = strtoull(env_seeds, NULL, 0);
    if (env_steps) max_steps = atoi(env_steps);

    size_t total_fed = 0, total_rcv = 0;
    size_t total_prop_fed = 0, total_prop_rcv = 0;

    for (uint64_t i = 0; i < num_seeds; i++) {
        uint64_t seed = seed_start + i;
        run_counters_t ctr;
        if (run_one(seed, max_steps, &ctr) < 0) {
            failures++;
        }
        total_fed += ctr.fed; total_rcv += ctr.rcv;
        total_prop_fed += ctr.prop_fed; total_prop_rcv += ctr.prop_rcv;
    }

    if (failures == 0 && num_seeds >= 100) {
        if (total_fed <= num_seeds) {
            fprintf(stderr, "FAIL: random phase underexercised fed=%zu\n",
                    total_fed);
            failures++;
        }
        if (total_prop_fed == 0) {
            fprintf(stderr, "FAIL: no properties sent in random phase\n");
            failures++;
        }
        if (total_prop_rcv == 0) {
            fprintf(stderr, "FAIL: no properties received in random phase\n");
            failures++;
        }
    }

    if (failures == 0) {
        fprintf(stderr, "PASS: %llu seeds, %d steps/seed "
            "(fed=%zu rcv=%zu prop_fed=%zu prop_rcv=%zu)\n",
            (unsigned long long)num_seeds, max_steps,
            total_fed, total_rcv, total_prop_fed, total_prop_rcv);
    }

    return failures;
}
