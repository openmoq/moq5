#include "fake_pq_pair.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* ================================================================== */
/* Per-connection dispatch                                             */
/* ================================================================== */

#define FAKE_PQ_MAX_PAIRS 4

typedef struct {
    picoquic_cnx_t   *cnx;
    fake_pq_pair_t   *pair;
    fake_pq_side_t   *side;
} fake_pq_cnx_entry_t;

static fake_pq_cnx_entry_t g_cnx_table[FAKE_PQ_MAX_PAIRS * 2];
static size_t g_cnx_count = 0;

static void cnx_table_add(picoquic_cnx_t *cnx, fake_pq_pair_t *pair,
                            fake_pq_side_t *side)
{
    if (g_cnx_count < FAKE_PQ_MAX_PAIRS * 2) {
        g_cnx_table[g_cnx_count].cnx = cnx;
        g_cnx_table[g_cnx_count].pair = pair;
        g_cnx_table[g_cnx_count].side = side;
        g_cnx_count++;
    }
}

static void cnx_table_remove(fake_pq_pair_t *pair)
{
    size_t w = 0;
    for (size_t r = 0; r < g_cnx_count; r++) {
        if (g_cnx_table[r].pair != pair)
            g_cnx_table[w++] = g_cnx_table[r];
    }
    g_cnx_count = w;
}

static fake_pq_side_t *side_for_cnx(picoquic_cnx_t *cnx)
{
    for (size_t i = 0; i < g_cnx_count; i++)
        if (g_cnx_table[i].cnx == cnx) return g_cnx_table[i].side;
    return NULL;
}

static fake_pq_pair_t *pair_for_cnx(picoquic_cnx_t *cnx)
{
    for (size_t i = 0; i < g_cnx_count; i++)
        if (g_cnx_table[i].cnx == cnx) return g_cnx_table[i].pair;
    return NULL;
}

/* ================================================================== */
/* picoquic stubs                                                      */
/* ================================================================== */

picoquic_quic_t *picoquic_get_quic_ctx(picoquic_cnx_t *cnx)
{
    return (picoquic_quic_t *)pair_for_cnx(cnx);
}

uint64_t picoquic_get_quic_time(picoquic_quic_t *q)
{
    fake_pq_pair_t *p = (fake_pq_pair_t *)q;
    return p ? p->now : 0;
}

picoquic_stream_data_cb_fn g_last_set_callback_fn = NULL;
void *g_last_set_callback_ctx = (void *)(uintptr_t)1;

void picoquic_set_callback(picoquic_cnx_t *cnx,
    picoquic_stream_data_cb_fn fn, void *ctx)
{
    (void)cnx;
    g_last_set_callback_fn = fn;
    g_last_set_callback_ctx = ctx;
}

int picoquic_add_to_stream(picoquic_cnx_t *cnx, uint64_t stream_id,
    const uint8_t *data, size_t length, int set_fin)
{
    fake_pq_side_t *s = side_for_cnx(cnx);
    if (!s) return -1;
    if (s->block_write) return -1;
    if (s->count >= FAKE_PQ_MAX_OPS) return -1;
    fake_pq_op_t *op = &s->ops[s->count++];
    op->kind = FAKE_PQ_OP_STREAM_WRITE;
    op->stream_id = stream_id;
    op->len = 0;
    if (data && length > 0) {
        size_t copy = length < sizeof(op->data) ? length : sizeof(op->data);
        memcpy(op->data, data, copy);
        op->len = copy;
    }
    op->fin = set_fin != 0;
    return 0;
}

uint64_t picoquic_get_next_local_stream_id(picoquic_cnx_t *cnx, int is_unidir)
{
    fake_pq_side_t *s = side_for_cnx(cnx);
    if (!s) return 0;
    if (is_unidir) {
        uint64_t id = s->next_uni_id;
        s->next_uni_id += 4;
        return id;
    }
    uint64_t id = s->next_bidi_id;
    s->next_bidi_id += 4;
    s->opened_bidi++;
    return id;
}

uint64_t picoquic_get_remote_stream_error(picoquic_cnx_t *cnx,
    uint64_t stream_id)
{
    (void)cnx; (void)stream_id;
    return 0;
}

uint64_t g_pending_app_error = 0;

uint64_t picoquic_get_application_error(picoquic_cnx_t *cnx)
{
    (void)cnx;
    return g_pending_app_error;
}

int picoquic_reset_stream(picoquic_cnx_t *cnx, uint64_t stream_id,
    uint64_t error_code)
{
    fake_pq_side_t *s = side_for_cnx(cnx);
    if (!s || s->count >= FAKE_PQ_MAX_OPS) return -1;
    fake_pq_op_t *op = &s->ops[s->count++];
    op->kind = FAKE_PQ_OP_RESET;
    op->stream_id = stream_id;
    op->error_code = error_code;
    op->len = 0;
    op->fin = false;
    return 0;
}

int picoquic_stop_sending(picoquic_cnx_t *cnx, uint64_t stream_id,
    uint64_t error_code)
{
    fake_pq_side_t *s = side_for_cnx(cnx);
    if (!s || s->count >= FAKE_PQ_MAX_OPS) return -1;
    fake_pq_op_t *op = &s->ops[s->count++];
    op->kind = FAKE_PQ_OP_STOP_SENDING;
    op->stream_id = stream_id;
    op->error_code = error_code;
    op->len = 0;
    op->fin = false;
    return 0;
}

int picoquic_close(picoquic_cnx_t *cnx, uint64_t error_code)
{
    fake_pq_side_t *s = side_for_cnx(cnx);
    if (!s || s->count >= FAKE_PQ_MAX_OPS) return -1;
    fake_pq_op_t *op = &s->ops[s->count++];
    op->kind = FAKE_PQ_OP_CLOSE;
    op->error_code = error_code;
    op->len = 0;
    op->fin = false;
    op->stream_id = 0;
    return 0;
}

int picoquic_close_ex(picoquic_cnx_t *cnx, uint64_t error_code,
    const char *reason)
{
    (void)reason;
    return picoquic_close(cnx, error_code);
}

int picoquic_queue_datagram_frame(picoquic_cnx_t *cnx, size_t length,
    const uint8_t *data)
{
    fake_pq_side_t *s = side_for_cnx(cnx);
    if (!s) return -1;
    if (s->drop_datagram) return 0;
    if (s->count >= FAKE_PQ_MAX_OPS) return -1;
    fake_pq_op_t *op = &s->ops[s->count++];
    op->kind = FAKE_PQ_OP_DATAGRAM;
    op->len = 0;
    if (data && length > 0) {
        size_t copy = length < sizeof(op->data) ? length : sizeof(op->data);
        memcpy(op->data, data, copy);
        op->len = copy;
    }
    op->fin = false;
    op->stream_id = 0;
    return 0;
}

/* The endpoint's DATAGRAM honesty gate reads the peer's negotiated
 * max_datagram_frame_size through this. Return the fake side's configured
 * datagram_max (0 = not negotiated -> sends are dropped non-fatally). Pointer
 * is to a per-call static; the caller reads the field immediately. */
picoquic_tp_t const *picoquic_get_transport_parameters(picoquic_cnx_t *cnx,
                                                       int get_local)
{
    (void)get_local;
    static picoquic_tp_t tp;
    memset(&tp, 0, sizeof(tp));
    fake_pq_side_t *s = side_for_cnx(cnx);
    tp.max_datagram_frame_size = s ? s->datagram_max : 0;
    return &tp;
}

/* ================================================================== */
/* Pair lifecycle                                                      */
/* ================================================================== */

int fake_pq_pair_create(fake_pq_pair_t *pair)
{
    memset(pair, 0, sizeof(*pair));
    pair->now = 1000;

    /* Client: uni=2,6,10..., bidi=0,4,8... */
    pair->client_side.next_uni_id = 2;
    pair->client_side.next_bidi_id = 0;

    /* Server: uni=3,7,11..., bidi=1,5,9... */
    pair->server_side.next_uni_id = 3;
    pair->server_side.next_bidi_id = 1;

    /* DATAGRAM negotiated by default so the datagram conformance scenarios
     * exercise delivery; the endpoint's honesty gate reads this via the
     * picoquic_get_transport_parameters stub. (drop_datagram independently
     * simulates an in-flight drop on an otherwise-negotiated connection.) */
    pair->client_side.datagram_max = 1252;
    pair->server_side.datagram_max = 1252;

    const moq_alloc_t *alloc = moq_alloc_default();

    moq_session_cfg_t ccfg;
    moq_session_cfg_init_sized(&ccfg, sizeof(ccfg), alloc, MOQ_PERSPECTIVE_CLIENT);
    ccfg.send_request_capacity = true;
    ccfg.initial_request_capacity = 16;
    if (moq_session_create(&ccfg, 0, &pair->client_session) < 0)
        return -1;

    moq_session_cfg_t scfg;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), alloc, MOQ_PERSPECTIVE_SERVER);
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 16;
    scfg.goaway_timeout_us = 1000;
    if (moq_session_create(&scfg, 0, &pair->server_session) < 0)
        return -1;

    cnx_table_add((picoquic_cnx_t *)&pair->client_side, pair,
                   &pair->client_side);
    cnx_table_add((picoquic_cnx_t *)&pair->server_side, pair,
                   &pair->server_side);

    moq_pq_conn_cfg_t cc;
    moq_pq_conn_cfg_init_sized(&cc, sizeof(cc));
    cc.session = pair->client_session;
    cc.cnx = (picoquic_cnx_t *)&pair->client_side;
    cc.alloc = alloc;
    if (moq_pq_conn_create(&cc, &pair->client_conn) != 0)
        return -1;

    moq_pq_conn_cfg_t sc;
    moq_pq_conn_cfg_init_sized(&sc, sizeof(sc));
    sc.session = pair->server_session;
    sc.cnx = (picoquic_cnx_t *)&pair->server_side;
    sc.alloc = alloc;
    if (moq_pq_conn_create(&sc, &pair->server_conn) != 0)
        return -1;

    return 0;
}

void fake_pq_pair_destroy(fake_pq_pair_t *pair)
{
    if (pair->client_conn) moq_pq_conn_destroy(pair->client_conn);
    if (pair->server_conn) moq_pq_conn_destroy(pair->server_conn);
    if (pair->client_session) moq_session_destroy(pair->client_session);
    if (pair->server_session) moq_session_destroy(pair->server_session);
    cnx_table_remove(pair);
    memset(pair, 0, sizeof(*pair));
}

/* ================================================================== */
/* Pump: deliver outbox ops to peer callback                           */
/* ================================================================== */

static bool deliver_ops(fake_pq_side_t *from, moq_pq_conn_t *to_conn,
                         picoquic_cnx_t *to_cnx)
{
    if (from->count == 0) return false;
    size_t n = from->count;
    fake_pq_op_t ops[FAKE_PQ_MAX_OPS];
    memcpy(ops, from->ops, n * sizeof(fake_pq_op_t));
    from->count = 0;

    for (size_t i = 0; i < n; i++) {
        fake_pq_op_t *op = &ops[i];
        switch (op->kind) {
        case FAKE_PQ_OP_STREAM_WRITE:
            moq_pq_callback(to_cnx, op->stream_id,
                op->len > 0 ? op->data : NULL, op->len,
                op->fin ? picoquic_callback_stream_fin
                        : picoquic_callback_stream_data,
                to_conn, NULL);
            break;
        case FAKE_PQ_OP_DATAGRAM:
            moq_pq_callback(to_cnx, 0,
                op->data, op->len,
                picoquic_callback_datagram,
                to_conn, NULL);
            break;
        case FAKE_PQ_OP_RESET:
            moq_pq_callback(to_cnx, op->stream_id,
                NULL, 0,
                picoquic_callback_stream_reset,
                to_conn, NULL);
            break;
        case FAKE_PQ_OP_STOP_SENDING:
            moq_pq_callback(to_cnx, op->stream_id,
                NULL, 0,
                picoquic_callback_stop_sending,
                to_conn, NULL);
            break;
        case FAKE_PQ_OP_CLOSE:
            g_pending_app_error = op->error_code;
            moq_pq_callback(to_cnx, 0,
                NULL, 0,
                picoquic_callback_application_close,
                to_conn, NULL);
            g_pending_app_error = 0;
            break;
        }
    }
    return true;
}

bool fake_pq_pair_pump_once(fake_pq_pair_t *pair)
{
    bool progress = false;

    moq_pq_service(pair->client_conn, pair->now);
    progress |= deliver_ops(&pair->client_side, pair->server_conn,
                              (picoquic_cnx_t *)&pair->server_side);

    moq_pq_service(pair->server_conn, pair->now);
    progress |= deliver_ops(&pair->server_side, pair->client_conn,
                              (picoquic_cnx_t *)&pair->client_side);

    return progress;
}

/* ================================================================== */
/* Conformance adapter vtable                                          */
/* ================================================================== */

static moq_session_t *vt_client(void *ctx) {
    return ((fake_pq_pair_t *)ctx)->client_session;
}
static moq_session_t *vt_server(void *ctx) {
    return ((fake_pq_pair_t *)ctx)->server_session;
}
static uint64_t vt_now(void *ctx) {
    return ((fake_pq_pair_t *)ctx)->now;
}
static void vt_advance(void *ctx, uint64_t t) {
    ((fake_pq_pair_t *)ctx)->now = t;
}
static uint64_t vt_deadline(void *ctx) {
    fake_pq_pair_t *p = (fake_pq_pair_t *)ctx;
    uint64_t d1 = moq_session_next_deadline_us(p->client_session);
    uint64_t d2 = moq_session_next_deadline_us(p->server_session);
    return d1 < d2 ? d1 : d2;
}
static moq_adapter_pair_pump_result_t vt_pump_once(void *ctx, uint64_t now) {
    fake_pq_pair_t *p = (fake_pq_pair_t *)ctx;
    p->now = now;
    bool progress = fake_pq_pair_pump_once(p);
    if (moq_pq_conn_is_fatal(p->client_conn) ||
        moq_pq_conn_is_fatal(p->server_conn))
        return MOQ_ADAPTER_PAIR_ERROR;
    return progress ? MOQ_ADAPTER_PAIR_PROGRESS
                    : MOQ_ADAPTER_PAIR_QUIESCENT;
}
static moq_adapter_pair_pump_result_t vt_pump_quiescent(void *ctx, int max) {
    fake_pq_pair_t *p = (fake_pq_pair_t *)ctx;
    for (int i = 0; i < max; i++) {
        if (moq_pq_conn_is_fatal(p->client_conn) ||
            moq_pq_conn_is_fatal(p->server_conn))
            return MOQ_ADAPTER_PAIR_ERROR;
        if (!fake_pq_pair_pump_once(p)) {
            if (moq_pq_conn_is_fatal(p->client_conn) ||
                moq_pq_conn_is_fatal(p->server_conn))
                return MOQ_ADAPTER_PAIR_ERROR;
            return MOQ_ADAPTER_PAIR_QUIESCENT;
        }
    }
    return MOQ_ADAPTER_PAIR_MAX_STEPS;
}
static bool vt_has_error(void *ctx) {
    fake_pq_pair_t *p = (fake_pq_pair_t *)ctx;
    return moq_pq_conn_is_fatal(p->client_conn) ||
           moq_pq_conn_is_fatal(p->server_conn);
}
static bool vt_is_closed(void *ctx, moq_adapter_pair_side_t side) {
    fake_pq_pair_t *p = (fake_pq_pair_t *)ctx;
    return side == MOQ_ADAPTER_PAIR_CLIENT
        ? moq_pq_conn_is_closed(p->client_conn)
        : moq_pq_conn_is_closed(p->server_conn);
}
static uint64_t vt_close_code(void *ctx, moq_adapter_pair_side_t side) {
    fake_pq_pair_t *p = (fake_pq_pair_t *)ctx;
    return side == MOQ_ADAPTER_PAIR_CLIENT
        ? moq_pq_conn_close_code(p->client_conn)
        : moq_pq_conn_close_code(p->server_conn);
}
static bool vt_has_fatal(void *ctx, moq_adapter_pair_side_t side) {
    fake_pq_pair_t *p = (fake_pq_pair_t *)ctx;
    return side == MOQ_ADAPTER_PAIR_CLIENT
        ? moq_pq_conn_is_fatal(p->client_conn)
        : moq_pq_conn_is_fatal(p->server_conn);
}
static uint64_t vt_fatal_code(void *ctx, moq_adapter_pair_side_t side) {
    fake_pq_pair_t *p = (fake_pq_pair_t *)ctx;
    return side == MOQ_ADAPTER_PAIR_CLIENT
        ? moq_pq_conn_fatal_code(p->client_conn)
        : moq_pq_conn_fatal_code(p->server_conn);
}
static const char *vt_last_error(void *ctx) {
    (void)ctx;
    return "";
}
static void vt_block_writes(void *ctx, moq_adapter_pair_side_t side,
                             bool block) {
    fake_pq_pair_t *p = (fake_pq_pair_t *)ctx;
    fake_pq_side_t *s = side == MOQ_ADAPTER_PAIR_CLIENT
        ? &p->client_side : &p->server_side;
    s->block_write = block;
}
static void vt_drop_dg(void *ctx, moq_adapter_pair_side_t side,
                         bool drop) {
    fake_pq_pair_t *p = (fake_pq_pair_t *)ctx;
    fake_pq_side_t *s = side == MOQ_ADAPTER_PAIR_CLIENT
        ? &p->client_side : &p->server_side;
    s->drop_datagram = drop;
}
static size_t vt_stream_count(void *ctx, moq_adapter_pair_side_t side) {
    fake_pq_pair_t *p = (fake_pq_pair_t *)ctx;
    return side == MOQ_ADAPTER_PAIR_CLIENT
        ? moq_pq_conn_active_stream_count(p->client_conn)
        : moq_pq_conn_active_stream_count(p->server_conn);
}
static size_t vt_tombstone_count(void *ctx, moq_adapter_pair_side_t side) {
    fake_pq_pair_t *p = (fake_pq_pair_t *)ctx;
    return side == MOQ_ADAPTER_PAIR_CLIENT
        ? moq_pq_conn_tombstone_count(p->client_conn)
        : moq_pq_conn_tombstone_count(p->server_conn);
}
static size_t vt_opened_bidi(void *ctx, moq_adapter_pair_side_t side) {
    fake_pq_pair_t *p = (fake_pq_pair_t *)ctx;
    return side == MOQ_ADAPTER_PAIR_CLIENT
        ? p->client_side.opened_bidi
        : p->server_side.opened_bidi;
}
static int vt_inject_bidi_fin(void *ctx, moq_adapter_pair_side_t from) {
    fake_pq_pair_t *p = (fake_pq_pair_t *)ctx;
    /* Find the last bidi stream opened by from_side (excluding
     * control which is stream 0 for client). */
    fake_pq_side_t *src = from == MOQ_ADAPTER_PAIR_CLIENT
        ? &p->client_side : &p->server_side;
    uint64_t last_bidi = UINT64_MAX;
    /* The from-side opened bidis sequentially. The last one
     * before the current next_bidi_id is the target. Skip the
     * first (control stream). */
    if (src->opened_bidi < 2) return -1;
    last_bidi = src->next_bidi_id - 4;

    moq_pq_conn_t *target = from == MOQ_ADAPTER_PAIR_CLIENT
        ? p->server_conn : p->client_conn;
    picoquic_cnx_t *target_cnx = from == MOQ_ADAPTER_PAIR_CLIENT
        ? (picoquic_cnx_t *)&p->server_side
        : (picoquic_cnx_t *)&p->client_side;

    return moq_pq_callback(target_cnx, last_bidi, NULL, 0,
        picoquic_callback_stream_fin, target, NULL);
}

static void vt_destroy(void *ctx) {
    fake_pq_pair_t *p = (fake_pq_pair_t *)ctx;
    fake_pq_pair_destroy(p);
    free(p);
}

static const moq_adapter_pair_ops_t fake_pq_ops = {
    vt_client, vt_server,
    vt_now, vt_advance, vt_deadline,
    vt_pump_once, vt_pump_quiescent,
    vt_has_error,
    vt_is_closed, vt_close_code,
    vt_has_fatal, vt_fatal_code,
    vt_last_error,
    vt_block_writes, vt_drop_dg,
    vt_stream_count, vt_tombstone_count, vt_opened_bidi,
    vt_inject_bidi_fin,
    vt_destroy,
};

moq_adapter_pair_t fake_pq_conformance_create(void)
{
    fake_pq_pair_t *p = (fake_pq_pair_t *)malloc(sizeof(*p));
    moq_adapter_pair_t pair = {NULL, 0, NULL};
    if (!p) return pair;
    if (fake_pq_pair_create(p) < 0) {
        fake_pq_pair_destroy(p);
        free(p);
        return pair;
    }
    pair.ops = &fake_pq_ops;
    pair.capabilities =
        MOQ_ADAPTER_PAIR_CAP_DROP_DATAGRAMS |
        MOQ_ADAPTER_PAIR_CAP_DATAGRAMS |
        MOQ_ADAPTER_PAIR_CAP_BIDI_STREAMS |
        MOQ_ADAPTER_PAIR_CAP_VIRTUAL_TIME |
        MOQ_ADAPTER_PAIR_CAP_REAL_QUIC_IDS |
        MOQ_ADAPTER_PAIR_CAP_OPENED_BIDI_COUNT |
        MOQ_ADAPTER_PAIR_CAP_INJECT_BIDI_FIN |
        MOQ_ADAPTER_PAIR_CAP_STREAM_COUNTS |
        MOQ_ADAPTER_PAIR_CAP_TOMBSTONES;
    pair.ctx = p;
    return pair;
}
