/*
 * Picoquic WebTransport conformance pair.
 *
 * Wraps the shared pico WT loopback harness (see pico_wt_harness.{c,h})
 * with the moq_adapter_pair_ops_t vtable so the shared conformance
 * scenarios run through the actual picoquic WebTransport stack.
 */

#include "pico_wt_conformance_pair.h"
#include "pico_wt_harness.h"
#include <moq/moq.h>

#include <stdlib.h>
#include <string.h>

/* -- Context -------------------------------------------------------- */

typedef struct {
    pico_wt_harness_t harness;
    char last_error[256];
} pico_wt_conf_ctx_t;

static pico_wt_conf_ctx_t *C(void *ctx) {
    return (pico_wt_conf_ctx_t *)ctx;
}

/* -- Bridge helpers ------------------------------------------------- */

static moq_transport_bridge_t *side_bridge(pico_wt_conf_ctx_t *c,
                                            moq_adapter_pair_side_t s)
{
    return s == MOQ_ADAPTER_PAIR_CLIENT
        ? c->harness.client_conn->bridge : c->harness.server_conn->bridge;
}

static moq_pico_wt_conn_t *side_conn(pico_wt_conf_ctx_t *c,
                                      moq_adapter_pair_side_t s)
{
    return s == MOQ_ADAPTER_PAIR_CLIENT
        ? c->harness.client_conn : c->harness.server_conn;
}

/* -- Vtable ops ----------------------------------------------------- */

static moq_session_t *op_client(void *ctx) {
    return C(ctx)->harness.client_session;
}
static moq_session_t *op_server(void *ctx) {
    return C(ctx)->harness.server_session;
}

static uint64_t op_now(void *ctx) { return C(ctx)->harness.now; }
static void op_advance(void *ctx, uint64_t t) { C(ctx)->harness.now = t; }

static uint64_t op_deadline(void *ctx) {
    pico_wt_harness_t *h = &C(ctx)->harness;
    uint64_t cd = moq_session_next_deadline_us(h->client_session);
    uint64_t sd = moq_session_next_deadline_us(h->server_session);
    return cd < sd ? cd : sd;
}

/*
 * Pump: one sim round per call, bounded time advance (100ms per call,
 * not seconds), mapping the harness round result onto the pair enum.
 */
static moq_adapter_pair_pump_result_t op_pump_once(void *ctx,
                                                    uint64_t now_us)
{
    pico_wt_harness_t *h = &C(ctx)->harness;
    h->now = now_us;

    int was_active = 0;
    if (pico_wt_harness_sim_round(h, h->now + 100000, &was_active) != 0)
        return MOQ_ADAPTER_PAIR_ERROR;

    if (moq_pico_wt_conn_is_fatal(h->client_conn) ||
        moq_pico_wt_conn_is_fatal(h->server_conn))
        return MOQ_ADAPTER_PAIR_ERROR;

    return was_active ? MOQ_ADAPTER_PAIR_PROGRESS
                      : MOQ_ADAPTER_PAIR_QUIESCENT;
}

static moq_adapter_pair_pump_result_t op_pump_until(void *ctx,
                                                     int max_steps)
{
    pico_wt_harness_t *h = &C(ctx)->harness;
    /* Scale for real QUIC: each MoQ round-trip needs ~10-20 sim
     * steps (packet send → link → receive → process → response). */
    int limit = max_steps * 50;
    int inactive = 0;
    for (int i = 0; i < limit; i++) {
        moq_adapter_pair_pump_result_t r = op_pump_once(ctx, h->now);
        if (r == MOQ_ADAPTER_PAIR_ERROR) return r;
        if (r == MOQ_ADAPTER_PAIR_QUIESCENT) {
            if (++inactive > 10) return MOQ_ADAPTER_PAIR_QUIESCENT;
        } else {
            inactive = 0;
        }
    }
    return MOQ_ADAPTER_PAIR_MAX_STEPS;
}

static bool op_has_error(void *ctx) {
    pico_wt_harness_t *h = &C(ctx)->harness;
    return moq_pico_wt_conn_is_fatal(h->client_conn) ||
           moq_pico_wt_conn_is_fatal(h->server_conn);
}

static bool op_is_closed(void *ctx, moq_adapter_pair_side_t s) {
    return moq_transport_bridge_is_closed(side_bridge(C(ctx), s));
}
static uint64_t op_close_code(void *ctx, moq_adapter_pair_side_t s) {
    return moq_transport_bridge_close_code(side_bridge(C(ctx), s));
}
static bool op_has_fatal(void *ctx, moq_adapter_pair_side_t s) {
    return moq_transport_bridge_is_fatal(side_bridge(C(ctx), s));
}
static uint64_t op_fatal_code(void *ctx, moq_adapter_pair_side_t s) {
    return moq_transport_bridge_fatal_code(side_bridge(C(ctx), s));
}
static const char *op_last_error(void *ctx) {
    return C(ctx)->last_error;
}

/* Fault injection: not supported for real picoquic transport. */
static void op_block_writes(void *ctx, moq_adapter_pair_side_t s,
                             bool block) {
    (void)ctx; (void)s; (void)block;
}
static void op_drop_datagrams(void *ctx, moq_adapter_pair_side_t s,
                               bool drop) {
    (void)ctx; (void)s; (void)drop;
}

static size_t op_stream_count(void *ctx, moq_adapter_pair_side_t s) {
    return moq_transport_bridge_stream_count(side_bridge(C(ctx), s));
}
static size_t op_tombstone_count(void *ctx,
                                  moq_adapter_pair_side_t s) {
    return moq_transport_bridge_tombstone_count(side_bridge(C(ctx), s));
}
static size_t op_opened_bidi(void *ctx, moq_adapter_pair_side_t s) {
    return side_conn(C(ctx), s)->opened_bidi_count;
}

static int op_inject_fin(void *ctx, moq_adapter_pair_side_t from) {
    pico_wt_conf_ctx_t *c = C(ctx);
    moq_pico_wt_conn_t *src = side_conn(c, from);

    /* Target the last non-control bidi opened by this side. */
    if (src->last_opened_bidi_id == UINT64_MAX ||
        src->last_opened_bidi_id == src->moq_control_stream_id)
        return -1;

    /* Send FIN on that stream via the source's QUIC connection. */
    if (picoquic_add_to_stream(src->cnx,
            src->last_opened_bidi_id, NULL, 0, 1) != 0)
        return -1;

    /* Pump sim rounds to deliver the FIN to the peer and let the
     * bridge absorb it (tombstone cleanup). */
    int idle = 0;
    for (int i = 0; i < 500 && idle < 10; i++) {
        int wa = 0;
        if (pico_wt_harness_sim_round(&c->harness,
                                       c->harness.now + 100000, &wa) != 0)
            break;
        if (!wa) idle++;
        else idle = 0;
    }
    return 0;
}

static void op_destroy(void *ctx) {
    pico_wt_conf_ctx_t *c = C(ctx);
    pico_wt_harness_cleanup(&c->harness);
    free(c);
}

static const moq_adapter_pair_ops_t pico_wt_conf_ops = {
    op_client, op_server,
    op_now, op_advance, op_deadline,
    op_pump_once, op_pump_until,
    op_has_error,
    op_is_closed, op_close_code,
    op_has_fatal, op_fatal_code,
    op_last_error,
    op_block_writes, op_drop_datagrams,
    op_stream_count, op_tombstone_count,
    op_opened_bidi,
    op_inject_fin,
    op_destroy,
};

/* -- Factory -------------------------------------------------------- */

moq_adapter_pair_t pico_wt_conformance_create(void)
{
    moq_adapter_pair_t pair = {0};

    pico_wt_conf_ctx_t *c = (pico_wt_conf_ctx_t *)calloc(1, sizeof(*c));
    if (!c) return pair;

    pico_wt_harness_cfg_t cfg = {
        .cid_byte = 0xc0,
        .server_goaway_timeout_us = 1000,
        .request_capacity = 64,
    };
    if (pico_wt_harness_setup(&c->harness, &cfg) != 0) {
        pico_wt_harness_cleanup(&c->harness);
        free(c);
        return pair;
    }

    pair.ops = &pico_wt_conf_ops;
    pair.capabilities =
        MOQ_ADAPTER_PAIR_CAP_STREAM_COUNTS |
        MOQ_ADAPTER_PAIR_CAP_TOMBSTONES |
        MOQ_ADAPTER_PAIR_CAP_DATAGRAMS |
        MOQ_ADAPTER_PAIR_CAP_BIDI_STREAMS |
        MOQ_ADAPTER_PAIR_CAP_REAL_QUIC_IDS |
        MOQ_ADAPTER_PAIR_CAP_OPENED_BIDI_COUNT |
        MOQ_ADAPTER_PAIR_CAP_INJECT_BIDI_FIN;
    /* NOT: VIRTUAL_TIME — picoquic sim advances time internally.
     * NOT: BLOCK_WRITES — picoquic queues internally.
     * NOT: DROP_DATAGRAMS — no sticky drop in real transport. */
    pair.ctx = c;
    return pair;
}
