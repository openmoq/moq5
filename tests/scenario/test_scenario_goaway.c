/*
 * Deterministic GOAWAY lifecycle scenario runner.
 *
 * Exercises GOAWAY send/receive, DRAINING state, request blocking,
 * object delivery after GOAWAY, duplicate GOAWAY close, and GOAWAY
 * drain timeout with active data flow.
 * Each seed runs twice; trace hash must match.
 *
 * Deterministic prelude guarantees coverage of:
 *   - subscribe + accept before GOAWAY
 *   - server sends GOAWAY
 *   - subscribe after GOAWAY → MOQ_ERR_GOAWAY
 *   - object write/delivery after GOAWAY
 *
 * Deterministic timeout prelude (separate SimPair) guarantees:
 *   - subscription accepted, subgroup opened, object delivered
 *   - server sends GOAWAY with drain timeout armed
 *   - second object written AFTER GOAWAY without pumping
 *     (SEND_DATA action queued when deadline fires)
 *   - advance just before deadline does NOT close
 *   - advance at deadline closes with MOQ_CLOSE_GOAWAY_TIMEOUT
 *   - cleanup of active data state is leak-free
 *
 * Random phase interleaves: pump, write, drain, advance time,
 * client GOAWAY, subscribe-blocked, duplicate GOAWAY. The
 * OP_ADVANCE_TIME op (1-2000us delta) can cross GOAWAY deadlines.
 */

#include <moq/sim.h>
#include <moq/codec.h>
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
    size_t   goaway_events;
    size_t   goaway_blocked;
    size_t   objects_after_goaway;
    size_t   duplicate_goaway_close;
    size_t   goaway_timeout_closes;
    size_t   ticks_before_deadline;
    size_t   rnd_time_advances;
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

typedef struct {
    bool server_goaway_sent;
    bool client_goaway_received;
    bool client_goaway_sent;
    bool server_goaway_received;
    bool sub_accepted;
    moq_subscription_t server_sub;
    moq_subgroup_handle_t server_sg;
    bool sg_open;
    size_t objects_written;
    bool closed;
    bool goaway_deadline_armed;
    uint64_t goaway_deadline_us;
} shadow_t;

/* -- Deterministic prelude ------------------------------------------ */

static int run_prelude(moq_simpair_t *sp, shadow_t *sh,
                       trace_summary_t *ts, const moq_alloc_t *alloc)
{
    moq_session_t *c = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);
    moq_event_t ev;

    /* 1. Client subscribes. */
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ga") };
    moq_namespace_t ns = { ns_parts, 1 };
    moq_subscribe_cfg_t sub_cfg;
    moq_subscribe_cfg_init(&sub_cfg);
    sub_cfg.track_namespace = ns;
    sub_cfg.track_name = MOQ_BYTES_LITERAL("trk");
    sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
    moq_subscription_t sub;
    if (moq_session_subscribe(c, &sub_cfg, moq_simpair_now_us(sp), &sub)
        != MOQ_OK) return -1;
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    if (moq_session_poll_events(sv, &ev, 1) != 1) return -1;
    if (ev.kind != MOQ_EVENT_SUBSCRIBE_REQUEST) {
        moq_event_cleanup(&ev); return -1;
    }
    sh->server_sub = ev.u.subscribe_request.sub;
    moq_event_cleanup(&ev);

    /* 2. Server accepts. */
    moq_accept_subscribe_cfg_t acc;
    moq_accept_subscribe_cfg_init(&acc);
    if (moq_session_accept_subscribe(sv, sh->server_sub, &acc,
        moq_simpair_now_us(sp)) != MOQ_OK) return -1;
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    if (moq_session_poll_events(c, &ev, 1) != 1) return -1;
    if (ev.kind != MOQ_EVENT_SUBSCRIBE_OK) {
        moq_event_cleanup(&ev); return -1;
    }
    moq_event_cleanup(&ev);
    sh->sub_accepted = true;

    /* 3. Server sends GOAWAY. */
    if (moq_session_goaway(sv, NULL, 0, moq_simpair_now_us(sp))
        != MOQ_OK) return -1;
    sh->server_goaway_sent = true;
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    if (moq_session_poll_events(c, &ev, 1) != 1) return -1;
    if (ev.kind != MOQ_EVENT_GOAWAY) {
        moq_event_cleanup(&ev); return -1;
    }
    moq_event_cleanup(&ev);
    sh->client_goaway_received = true;
    ts->goaway_events++;

    /* 4. Client subscribe after GOAWAY → blocked. */
    moq_subscribe_cfg_t sub2_cfg;
    moq_subscribe_cfg_init(&sub2_cfg);
    sub2_cfg.track_namespace = ns;
    sub2_cfg.track_name = MOQ_BYTES_LITERAL("blocked");
    sub2_cfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
    moq_subscription_t sub2;
    moq_result_t rc = moq_session_subscribe(c, &sub2_cfg,
        moq_simpair_now_us(sp), &sub2);
    if (rc != MOQ_ERR_GOAWAY) return -1;
    ts->goaway_blocked++;

    /* 5. Server writes object on existing subscription after GOAWAY. */
    moq_subgroup_cfg_t sg_cfg;
    moq_subgroup_cfg_init(&sg_cfg);
    sg_cfg.group_id = 0;
    sg_cfg.subgroup_id = 0;
    sg_cfg.publisher_priority = 128;
    if (moq_session_open_subgroup(sv, sh->server_sub, &sg_cfg,
        moq_simpair_now_us(sp), &sh->server_sg) != MOQ_OK) return -1;
    sh->sg_open = true;

    moq_rcbuf_t *payload = NULL;
    if (moq_rcbuf_create(alloc, (const uint8_t *)"pre", 3, &payload)
        != MOQ_OK) return -1;
    if (moq_session_write_object(sv, sh->server_sg, 0, payload,
        moq_simpair_now_us(sp)) != MOQ_OK) {
        moq_rcbuf_decref(payload); return -1;
    }
    moq_rcbuf_decref(payload);
    sh->objects_written++;

    moq_simpair_run_until_quiescent(sp, 8, NULL);

    if (moq_session_poll_events(c, &ev, 1) == 1) {
        if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED)
            ts->objects_after_goaway++;
        moq_event_cleanup(&ev);
    }

    return 0;
}

/* -- Deterministic timeout prelude ---------------------------------- */

static int run_timeout_prelude(moq_simpair_t *sp, trace_summary_t *ts,
                               const moq_alloc_t *alloc, uint64_t timeout_us)
{
    moq_session_t *c = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);
    moq_event_t ev;

    /* 1. Client subscribes. */
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("to") };
    moq_namespace_t ns = { ns_parts, 1 };
    moq_subscribe_cfg_t sub_cfg;
    moq_subscribe_cfg_init(&sub_cfg);
    sub_cfg.track_namespace = ns;
    sub_cfg.track_name = MOQ_BYTES_LITERAL("tto");
    sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
    moq_subscription_t sub;
    if (moq_session_subscribe(c, &sub_cfg, moq_simpair_now_us(sp), &sub)
        != MOQ_OK) return -1;
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    if (moq_session_poll_events(sv, &ev, 1) != 1) return -1;
    if (ev.kind != MOQ_EVENT_SUBSCRIBE_REQUEST) {
        moq_event_cleanup(&ev); return -1;
    }
    moq_subscription_t sv_sub = ev.u.subscribe_request.sub;
    moq_event_cleanup(&ev);

    /* 2. Server accepts. */
    moq_accept_subscribe_cfg_t acc;
    moq_accept_subscribe_cfg_init(&acc);
    if (moq_session_accept_subscribe(sv, sv_sub, &acc,
        moq_simpair_now_us(sp)) != MOQ_OK) return -1;
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    if (moq_session_poll_events(c, &ev, 1) != 1) return -1;
    if (ev.kind != MOQ_EVENT_SUBSCRIBE_OK) {
        moq_event_cleanup(&ev); return -1;
    }
    moq_event_cleanup(&ev);

    /* 3. Open subgroup, write one object, pump so stream is established. */
    moq_subgroup_cfg_t sg_cfg;
    moq_subgroup_cfg_init(&sg_cfg);
    sg_cfg.group_id = 0;
    sg_cfg.subgroup_id = 0;
    sg_cfg.publisher_priority = 128;
    moq_subgroup_handle_t sg;
    if (moq_session_open_subgroup(sv, sv_sub, &sg_cfg,
        moq_simpair_now_us(sp), &sg) != MOQ_OK) return -1;

    moq_rcbuf_t *payload = NULL;
    if (moq_rcbuf_create(alloc, (const uint8_t *)"tpre", 4, &payload)
        != MOQ_OK) return -1;
    if (moq_session_write_object(sv, sg, 0, payload,
        moq_simpair_now_us(sp)) != MOQ_OK) {
        moq_rcbuf_decref(payload); return -1;
    }
    moq_rcbuf_decref(payload);

    moq_simpair_run_until_quiescent(sp, 8, NULL);

    /* Drain client events (object received). */
    {
        moq_event_t devts[8]; size_t dne;
        while ((dne = moq_session_poll_events(c, devts, 8)) > 0)
            for (size_t di = 0; di < dne; di++)
                moq_event_cleanup(&devts[di]);
    }

    /* 4. Server sends GOAWAY → deadline arms. */
    uint64_t goaway_time = moq_simpair_now_us(sp);
    if (moq_session_goaway(sv, NULL, 0, goaway_time) != MOQ_OK)
        return -1;
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    /* Drain GOAWAY event on client. */
    if (moq_session_poll_events(c, &ev, 1) == 1)
        moq_event_cleanup(&ev);

    /* Verify deadline is armed on server. */
    uint64_t deadline = moq_session_next_deadline_us(sv);
    if (deadline == UINT64_MAX) return -1;
    uint64_t expected_deadline = goaway_time + timeout_us;

    /* 5. Advance to just before deadline → should NOT close. */
    moq_simpair_advance_to(sp, expected_deadline - 1);
    if (moq_session_state(sv) == MOQ_SESS_CLOSED)
        return -1; /* should not have closed yet */
    ts->ticks_before_deadline++;

    /* 6. Write a second object AFTER GOAWAY but do NOT pump.
     * This leaves a SEND_DATA action queued on the server when the
     * deadline fires — exercises cleanup of active data state. */
    payload = NULL;
    if (moq_rcbuf_create(alloc, (const uint8_t *)"active", 6, &payload)
        != MOQ_OK) return -1;
    if (moq_session_write_object(sv, sg, 1, payload,
        moq_simpair_now_us(sp)) != MOQ_OK) {
        moq_rcbuf_decref(payload); return -1;
    }
    moq_rcbuf_decref(payload);

    /* write_object == MOQ_OK above guarantees a SEND_DATA action is
     * queued; no pump/poll between here and the deadline tick, so the
     * action is still present when the timeout fires. */

    /* 7. Advance to deadline → should close with MOQ_CLOSE_GOAWAY_TIMEOUT
     * while the queued SEND_DATA is still pending. */
    moq_simpair_advance_to(sp, expected_deadline);
    if (moq_session_state(sv) != MOQ_SESS_CLOSED)
        return -1;

    /* Drain events/actions; verify close code. */
    bool found_timeout_close = false;
    {
        moq_event_t devts[8]; size_t dne;
        while ((dne = moq_session_poll_events(sv, devts, 8)) > 0)
            for (size_t di = 0; di < dne; di++) {
                if (devts[di].kind == MOQ_EVENT_SESSION_CLOSED &&
                    devts[di].u.closed.code == MOQ_CLOSE_GOAWAY_TIMEOUT)
                    found_timeout_close = true;
                moq_event_cleanup(&devts[di]);
            }
        while ((dne = moq_session_poll_events(c, devts, 8)) > 0)
            for (size_t di = 0; di < dne; di++)
                moq_event_cleanup(&devts[di]);
        moq_action_t dacts[8]; size_t dna;
        while ((dna = moq_session_poll_actions(sv, dacts, 8)) > 0)
            for (size_t di = 0; di < dna; di++)
                moq_action_cleanup(&dacts[di]);
        while ((dna = moq_session_poll_actions(c, dacts, 8)) > 0)
            for (size_t di = 0; di < dna; di++)
                moq_action_cleanup(&dacts[di]);
    }

    if (!found_timeout_close) return -1;
    ts->goaway_timeout_closes++;

    return 0;
}

/* -- Random operations ---------------------------------------------- */

typedef enum {
    OP_PUMP,
    OP_WRITE_OBJECT,
    OP_DRAIN_CLIENT,
    OP_DRAIN_SERVER,
    OP_ADVANCE_TIME,
    OP_CLIENT_GOAWAY,
    OP_SUBSCRIBE_BLOCKED,
    OP_DUPLICATE_GOAWAY,
    OP_COUNT,
} scenario_op_t;

static void execute_step(moq_simpair_t *sp, shadow_t *sh,
                          rng_t *rng, trace_summary_t *ts,
                          const moq_alloc_t *alloc)
{
    if (sh->closed) return;

    scenario_op_t op = (scenario_op_t)(rng_next(rng) % OP_COUNT);
    moq_session_t *c = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);

    switch (op) {
    case OP_PUMP:
        moq_simpair_run_until_quiescent(sp, 4, NULL);
        break;

    case OP_WRITE_OBJECT:
        if (sh->sg_open && !sh->closed) {
            moq_rcbuf_t *p = NULL;
            if (moq_rcbuf_create(alloc, (const uint8_t *)"d", 1, &p)
                == MOQ_OK) {
                moq_result_t rc = moq_session_write_object(sv,
                    sh->server_sg, sh->objects_written, p,
                    moq_simpair_now_us(sp));
                moq_rcbuf_decref(p);
                if (rc == MOQ_OK)
                    sh->objects_written++;
            }
        }
        break;

    case OP_DRAIN_CLIENT: {
        moq_event_t evts[8]; size_t ne;
        while ((ne = moq_session_poll_events(c, evts, 8)) > 0)
            for (size_t i = 0; i < ne; i++) {
                if (evts[i].kind == MOQ_EVENT_OBJECT_RECEIVED &&
                    sh->client_goaway_received)
                    ts->objects_after_goaway++;
                if (evts[i].kind == MOQ_EVENT_SESSION_CLOSED) {
                    if (evts[i].u.closed.code == MOQ_CLOSE_GOAWAY_TIMEOUT)
                        ts->goaway_timeout_closes++;
                    sh->closed = true;
                }
                moq_event_cleanup(&evts[i]);
            }
        break;
    }

    case OP_DRAIN_SERVER: {
        moq_event_t evts[8]; size_t ne;
        while ((ne = moq_session_poll_events(sv, evts, 8)) > 0)
            for (size_t i = 0; i < ne; i++) {
                if (evts[i].kind == MOQ_EVENT_SESSION_CLOSED) {
                    if (evts[i].u.closed.code == MOQ_CLOSE_GOAWAY_TIMEOUT)
                        ts->goaway_timeout_closes++;
                    sh->closed = true;
                }
                moq_event_cleanup(&evts[i]);
            }
        break;
    }

    case OP_ADVANCE_TIME: {
        uint64_t delta = 1 + (rng_next(rng) % 2000);
        uint64_t before = moq_simpair_now_us(sp);
        bool had_deadline =
            (moq_session_next_deadline_us(sv) != UINT64_MAX ||
             moq_session_next_deadline_us(c) != UINT64_MAX);
        moq_simpair_advance_to(sp, before + delta);
        ts->rnd_time_advances++;

        /* Check if the advance crossed a GOAWAY deadline. */
        bool sv_closed = (moq_session_state(sv) == MOQ_SESS_CLOSED);
        bool c_closed = (moq_session_state(c) == MOQ_SESS_CLOSED);

        if (sv_closed || c_closed) {
            /* Drain to find timeout closes. */
            moq_event_t devts[8]; size_t dne;
            while ((dne = moq_session_poll_events(sv, devts, 8)) > 0)
                for (size_t di = 0; di < dne; di++) {
                    if (devts[di].kind == MOQ_EVENT_SESSION_CLOSED &&
                        devts[di].u.closed.code == MOQ_CLOSE_GOAWAY_TIMEOUT)
                        ts->goaway_timeout_closes++;
                    moq_event_cleanup(&devts[di]);
                }
            while ((dne = moq_session_poll_events(c, devts, 8)) > 0)
                for (size_t di = 0; di < dne; di++) {
                    if (devts[di].kind == MOQ_EVENT_SESSION_CLOSED &&
                        devts[di].u.closed.code == MOQ_CLOSE_GOAWAY_TIMEOUT)
                        ts->goaway_timeout_closes++;
                    moq_event_cleanup(&devts[di]);
                }
            moq_action_t dacts[8]; size_t dna;
            while ((dna = moq_session_poll_actions(sv, dacts, 8)) > 0)
                for (size_t di = 0; di < dna; di++)
                    moq_action_cleanup(&dacts[di]);
            while ((dna = moq_session_poll_actions(c, dacts, 8)) > 0)
                for (size_t di = 0; di < dna; di++)
                    moq_action_cleanup(&dacts[di]);
            sh->closed = true;
        } else if (had_deadline) {
            ts->ticks_before_deadline++;
        }
        break;
    }

    case OP_CLIENT_GOAWAY:
        if (!sh->client_goaway_sent && !sh->closed) {
            moq_result_t rc = moq_session_goaway(c, NULL, 0,
                moq_simpair_now_us(sp));
            if (rc == MOQ_OK) {
                sh->client_goaway_sent = true;
                sh->goaway_deadline_armed = true;
                sh->goaway_deadline_us = moq_simpair_now_us(sp) + 5000;
                moq_simpair_run_until_quiescent(sp, 4, NULL);
                moq_event_t ev;
                if (moq_session_poll_events(sv, &ev, 1) == 1) {
                    if (ev.kind == MOQ_EVENT_GOAWAY) {
                        sh->server_goaway_received = true;
                        ts->goaway_events++;
                    }
                    moq_event_cleanup(&ev);
                }
            }
        }
        break;

    case OP_SUBSCRIBE_BLOCKED:
        if (sh->client_goaway_received && !sh->closed) {
            moq_bytes_t ns_p[] = { MOQ_BYTES_LITERAL("ga") };
            moq_namespace_t ns = { ns_p, 1 };
            moq_subscribe_cfg_t cfg;
            moq_subscribe_cfg_init(&cfg);
            cfg.track_namespace = ns;
            cfg.track_name = MOQ_BYTES_LITERAL("x");
            cfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
            moq_subscription_t h;
            moq_result_t rc = moq_session_subscribe(c, &cfg,
                moq_simpair_now_us(sp), &h);
            if (rc == MOQ_ERR_GOAWAY)
                ts->goaway_blocked++;
        }
        break;

    case OP_DUPLICATE_GOAWAY:
        if (sh->server_goaway_sent && !sh->closed) {
            uint8_t buf[16];
            moq_buf_writer_t w;
            moq_buf_writer_init(&w, buf, sizeof(buf));
            moq_d16_encode_goaway(&w, NULL, 0);
            moq_session_on_control_bytes(c, buf,
                moq_buf_writer_offset(&w), moq_simpair_now_us(sp));
            if (moq_session_state(c) == MOQ_SESS_CLOSED) {
                ts->duplicate_goaway_close++;
                sh->closed = true;
            }
        }
        break;

    default:
        break;
    }

    if (moq_session_state(c) == MOQ_SESS_CLOSED ||
        moq_session_state(sv) == MOQ_SESS_CLOSED)
        sh->closed = true;
}

/* -- Run one scenario ----------------------------------------------- */

#define GOAWAY_TIMEOUT_US 5000

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
    cfg.server_goaway_timeout_us = GOAWAY_TIMEOUT_US;

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

    shadow_t sh;
    memset(&sh, 0, sizeof(sh));
    int failures = 0;

    int prelude_rc = run_prelude(sp, &sh, summary, alloc);
    if (prelude_rc < 0) {
        fprintf(stderr, "FAIL seed=0x%llx run=%d: prelude failed\n",
                (unsigned long long)seed, run_id);
        fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx "
                "MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%d "
                "./build/tests/test_scenario_goaway\n",
                (unsigned long long)seed, max_steps);
        failures++;
    }

    if (failures == 0) {
        rng_t rng = { seed };
        for (int step = 0; step < max_steps; step++)
            execute_step(sp, &sh, &rng, summary, alloc);
    }

    {
        moq_event_t drain[16]; size_t ne;
        while ((ne = moq_session_poll_events(moq_simpair_client(sp), drain, 16)) > 0)
            for (size_t i = 0; i < ne; i++) moq_event_cleanup(&drain[i]);
        while ((ne = moq_session_poll_events(moq_simpair_server(sp), drain, 16)) > 0)
            for (size_t i = 0; i < ne; i++) moq_event_cleanup(&drain[i]);
        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(moq_simpair_client(sp), acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        while ((na = moq_session_poll_actions(moq_simpair_server(sp), acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
    }

    moq_simpair_destroy(sp);
    return failures;
}

/* -- Run one timeout scenario --------------------------------------- */

static int run_timeout_scenario(uint64_t seed, const moq_alloc_t *alloc,
                                 trace_summary_t *summary, int run_id)
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
    cfg.server_goaway_timeout_us = GOAWAY_TIMEOUT_US;

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

    int failures = 0;
    int tp_rc = run_timeout_prelude(sp, summary, alloc, GOAWAY_TIMEOUT_US);
    if (tp_rc < 0) {
        fprintf(stderr, "FAIL seed=0x%llx run=%d: timeout prelude failed\n",
                (unsigned long long)seed, run_id);
        fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx "
                "MOQ_SCENARIO_SEEDS=1 "
                "./build/tests/test_scenario_goaway\n",
                (unsigned long long)seed);
        failures++;
    }

    {
        moq_event_t drain[16]; size_t ne;
        while ((ne = moq_session_poll_events(moq_simpair_client(sp), drain, 16)) > 0)
            for (size_t i = 0; i < ne; i++) moq_event_cleanup(&drain[i]);
        while ((ne = moq_session_poll_events(moq_simpair_server(sp), drain, 16)) > 0)
            for (size_t i = 0; i < ne; i++) moq_event_cleanup(&drain[i]);
        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(moq_simpair_client(sp), acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        while ((na = moq_session_poll_actions(moq_simpair_server(sp), acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
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

    size_t total_goaway_events = 0;
    size_t total_goaway_blocked = 0;
    size_t total_objects_after = 0;
    size_t total_dup_close = 0;
    size_t total_timeout_closes = 0;
    size_t total_rnd_timeout_closes = 0;
    size_t total_ticks_before = 0;

    for (uint64_t seed = seed_start; seed < seed_start + num_seeds; seed++) {
        /* -- Original scenario (with random phase) -- */
        {
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
                            "./build/tests/test_scenario_goaway\n",
                            (unsigned long long)seed, max_steps);
                    failures++;
                }
            }

            total_goaway_events += sums[0].goaway_events;
            total_goaway_blocked += sums[0].goaway_blocked;
            total_objects_after += sums[0].objects_after_goaway;
            total_dup_close += sums[0].duplicate_goaway_close;
            total_timeout_closes += sums[0].goaway_timeout_closes;
            total_rnd_timeout_closes += sums[0].goaway_timeout_closes;
            total_ticks_before += sums[0].ticks_before_deadline;

            if (sums[0].hash != sums[1].hash ||
                sums[0].count != sums[1].count ||
                sums[0].goaway_events != sums[1].goaway_events ||
                sums[0].goaway_blocked != sums[1].goaway_blocked ||
                sums[0].objects_after_goaway != sums[1].objects_after_goaway ||
                sums[0].duplicate_goaway_close != sums[1].duplicate_goaway_close ||
                sums[0].goaway_timeout_closes != sums[1].goaway_timeout_closes ||
                sums[0].ticks_before_deadline != sums[1].ticks_before_deadline ||
                sums[0].rnd_time_advances != sums[1].rnd_time_advances) {
                fprintf(stderr, "FAIL seed=0x%llx: trace/counter mismatch\n",
                        (unsigned long long)seed);
                fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx "
                        "MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%d "
                        "./build/tests/test_scenario_goaway\n",
                        (unsigned long long)seed, max_steps);
                failures++;
            }
        }

        /* -- Timeout prelude scenario -- */
        {
            trace_summary_t tsums[2];

            for (int run = 0; run < 2; run++) {
                scen_alloc_t as = {0};
                moq_alloc_t alloc = { &as, sa_alloc, sa_realloc, sa_free };

                int rf = run_timeout_scenario(seed, &alloc, &tsums[run], run);
                failures += rf;

                if (as.balance != 0) {
                    fprintf(stderr, "FAIL seed=0x%llx run=%d timeout: "
                            "alloc balance=%lld\n",
                            (unsigned long long)seed, run,
                            (long long)as.balance);
                    fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx "
                            "MOQ_SCENARIO_SEEDS=1 "
                            "./build/tests/test_scenario_goaway\n",
                            (unsigned long long)seed);
                    failures++;
                }
            }

            total_timeout_closes += tsums[0].goaway_timeout_closes;
            total_ticks_before += tsums[0].ticks_before_deadline;

            if (tsums[0].hash != tsums[1].hash ||
                tsums[0].count != tsums[1].count ||
                tsums[0].goaway_timeout_closes != tsums[1].goaway_timeout_closes ||
                tsums[0].ticks_before_deadline != tsums[1].ticks_before_deadline) {
                fprintf(stderr, "FAIL seed=0x%llx: timeout trace/counter mismatch\n",
                        (unsigned long long)seed);
                fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx "
                        "MOQ_SCENARIO_SEEDS=1 "
                        "./build/tests/test_scenario_goaway\n",
                        (unsigned long long)seed);
                failures++;
            }
        }
    }

    if (num_seeds >= 10) {
        if (total_goaway_events == 0) {
            fprintf(stderr, "FAIL: no GOAWAY events across %llu seeds\n",
                    (unsigned long long)num_seeds);
            failures++;
        }
        if (total_goaway_blocked == 0) {
            fprintf(stderr, "FAIL: no GOAWAY-blocked subscribes\n");
            failures++;
        }
        if (total_objects_after == 0) {
            fprintf(stderr, "FAIL: no objects delivered after GOAWAY\n");
            failures++;
        }
        if (total_dup_close == 0) {
            fprintf(stderr, "FAIL: no duplicate GOAWAY closes\n");
            failures++;
        }
        if (total_timeout_closes == 0) {
            fprintf(stderr, "FAIL: no GOAWAY timeout closes\n");
            failures++;
        }
        if (total_rnd_timeout_closes == 0) {
            fprintf(stderr, "FAIL: no random GOAWAY timeout closes\n");
            failures++;
        }
    }

    if (failures == 0)
        fprintf(stderr, "PASS: test_scenario_goaway "
                "(%llu seeds, goaway=%zu blocked=%zu "
                "obj_after=%zu dup_close=%zu "
                "timeout_close=%zu rnd_timeout=%zu "
                "ticks_before=%zu)\n",
                (unsigned long long)num_seeds,
                total_goaway_events, total_goaway_blocked,
                total_objects_after, total_dup_close,
                total_timeout_closes, total_rnd_timeout_closes,
                total_ticks_before);
    else
        fprintf(stderr, "FAIL: test_scenario_goaway "
                "(%d failures in %llu seeds)\n",
                failures, (unsigned long long)num_seeds);

    return failures;
}
