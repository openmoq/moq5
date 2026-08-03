/*
 * pico_wt_adapter.c — Picoquic WebTransport adapter.
 *
 * Routes picohttp/h3zero WebTransport events to
 * moq_transport_bridge_t inbound handlers. Attach mode:
 * caller owns the picoquic connection and h3zero context.
 * The adapter must be created AFTER the WT session is
 * established (CONNECT accepted on both sides). On creation
 * it rebinds the h3zero stream prefix so all WT data streams
 * and datagrams route through this adapter's callback.
 *
 * MoQ control stream ≠ WT control stream. The WT control stream
 * carries HTTP/3 capsules (CONNECT, CLOSE, DRAIN) and routes to the
 * adapter lifecycle, never to MoQ data. Where MoQ control lives is
 * profile-dependent (the bridge's control mode):
 *
 * Bidi-control profiles (draft-16):
 * - Server: first peer WT bidi → MoQ control → on_peer_control_bytes
 * - Client: first local WT bidi → MoQ control (via on_bidi_opened)
 * - Other WT bidi → on_peer_bidi_bytes
 * - WT uni → on_peer_uni_bytes
 *
 * Uni-control-pair profiles (draft-18):
 * - No WT bidi is ever MoQ control; every bidi (including the first,
 *   each way) is a request stream → on_peer_bidi_bytes
 * - WT uni → on_peer_uni_bytes; the bridge classifies each peer uni
 *   itself (control vs data vs padding) by its leading stream type
 */

#include "pico_wt_adapter.h"
#include <picoquic.h>
#include <string.h>
#include <stddef.h>

/* Inbound receive flow control (defined below). */
static void pw_rx_after_service(moq_pico_wt_conn_t *c, uint64_t now);

/* Dispatch a FIN-initiated deferred bridge close from the post-service sweep.
 * Thin wrapper over moq_transport_bridge_service so a test can force the
 * "returned negative WITHOUT going terminal" branch, which cannot be provoked
 * through the real bridge. Present only in the test build; no symbol is
 * exported from the shipped library. */
#ifdef MOQ_PICO_WT_TESTING
moq_result_t (*pw_test_after_fin_service_hook)(moq_transport_bridge_t *,
                                               uint64_t) = NULL;
#endif
static moq_result_t pw_after_fin_service(moq_pico_wt_conn_t *c, uint64_t now)
{
#ifdef MOQ_PICO_WT_TESTING
    if (pw_test_after_fin_service_hook)
        return pw_test_after_fin_service_hook(c->bridge, now);
#endif
    return moq_transport_bridge_service(c->bridge, now);
}
static void pw_rx_free_all(moq_pico_wt_conn_t *c);
static void pw_rx_drop(moq_pico_wt_conn_t *c, uint64_t sid);


/* QUIC stream ID classification */
#ifndef PICOQUIC_IS_BIDIR_STREAM_ID
#define PICOQUIC_IS_BIDIR_STREAM_ID(id) (((id) & 2) == 0)
#endif
/* Matches picoquic's IS_LOCAL_STREAM_ID: nonzero when stream was
 * initiated by the local side. is_client: 1 for client, 0 for server. */
#define PICO_WT_IS_LOCAL_STREAM_ID(id, is_client) \
    (((id) ^ (is_client)) & 1)

/* -- Local stream opened callbacks (from endpoint ops) -------------- */

/* A local STOP_SENDING succeeded: cancel this stream's receive tracking so no
 * retained bytes are replayed and no further credit is granted for it. */
static void on_local_stop_sending(void *ctx, uint64_t stream_id)
{
    moq_pico_wt_conn_t *c = (moq_pico_wt_conn_t *)ctx;
    if (!c) return;
    pw_rx_drop(c, stream_id);
}

static void on_local_bidi_opened(void *ctx, uint64_t stream_id)
{
    moq_pico_wt_conn_t *c = (moq_pico_wt_conn_t *)ctx;
    if (!c) return;

    c->opened_bidi_count++;
    c->last_opened_bidi_id = stream_id;

    /* Bidi-control profiles (draft-16): the client's first locally opened
     * bidi is the MoQ control stream. Uni-control-pair profiles
     * (draft-18) carry control on unidirectional streams; every locally
     * opened bidi is a request stream and must NOT be latched as
     * control. */
    if (c->perspective == MOQ_PERSPECTIVE_CLIENT &&
        c->moq_control_stream_id == UINT64_MAX &&
        !moq_transport_bridge_uses_uni_control(c->bridge)) {
        c->moq_control_stream_id = stream_id;
    }
}

static void on_local_uni_opened(void *ctx, uint64_t stream_id)
{
    moq_pico_wt_conn_t *c = (moq_pico_wt_conn_t *)ctx;
    if (!c) return;
    c->opened_uni_count++;
    c->last_opened_uni_id = stream_id;
}

/* -- Lifecycle ------------------------------------------------------ */

/* Frozen v0 layout size: the prefix present in every released version of this
 * struct, ending just before the first appended field (user_ctx). The pointer-
 * only initializer cannot know the caller's storage size, so it touches only
 * this prefix -- safe for an old binary whose moq_pico_wt_conn_cfg_t was
 * exactly this size. */
#define MOQ_PICO_WT_CONN_CFG_V0_SIZE \
    (offsetof(moq_pico_wt_conn_cfg_t, user_ctx))

void moq_pico_wt_conn_cfg_init(moq_pico_wt_conn_cfg_t *cfg)
{
    if (!cfg) return;
    /* Clear and stamp ONLY the v0 prefix: writing sizeof(*cfg) here would
     * overflow an old caller that allocated the smaller v0 struct. The
     * appended user_ctx stays disabled (struct_size == v0 size); callers that
     * want it use moq_pico_wt_conn_cfg_init_sized(). */
    memset(cfg, 0, MOQ_PICO_WT_CONN_CFG_V0_SIZE);
    cfg->struct_size = (uint32_t)MOQ_PICO_WT_CONN_CFG_V0_SIZE;
}

void moq_pico_wt_conn_cfg_init_sized(moq_pico_wt_conn_cfg_t *cfg,
                                     size_t cfg_size)
{
    if (!cfg) return;
    /* Clear exactly what the caller allocated, never more than this library's
     * struct knows about (clamp down for a caller newer than the library). */
    size_t n = cfg_size < sizeof(*cfg) ? cfg_size : sizeof(*cfg);
    if (n < sizeof(cfg->struct_size)) return;  /* too small to even stamp */
    memset(cfg, 0, n);
    cfg->struct_size = (uint32_t)n;
}

int moq_pico_wt_conn_create(const moq_pico_wt_conn_cfg_t *cfg,
                              moq_pico_wt_conn_t **out)
{
    if (!cfg || !out) return -1;
    *out = NULL;

    /* ABI-safe config: require all fields through `alloc`. */
    size_t min_size = offsetof(moq_pico_wt_conn_cfg_t, alloc)
                    + sizeof(cfg->alloc);
    if (cfg->struct_size < min_size) return -1;
    if (!cfg->session || !cfg->cnx || !cfg->alloc ||
        !cfg->h3_ctx || !cfg->ctrl_ctx)
        return -1;
    if (!cfg->alloc->alloc || !cfg->alloc->realloc || !cfg->alloc->free)
        return -1;

    moq_pico_wt_conn_t *c = (moq_pico_wt_conn_t *)cfg->alloc->alloc(
        sizeof(moq_pico_wt_conn_t), cfg->alloc->ctx);
    if (!c) return -1;
    memset(c, 0, sizeof(*c));

    c->session = cfg->session;
    c->cnx = cfg->cnx;
    c->alloc = *cfg->alloc;
    c->h3_ctx = cfg->h3_ctx;
    c->control_stream_ctx = cfg->ctrl_ctx;
    c->control_stream_id = cfg->ctrl_ctx->stream_id;
    /* user_ctx is optional — only read if the caller's struct
     * extends that far. */
    if (cfg->struct_size >= offsetof(moq_pico_wt_conn_cfg_t, user_ctx)
        + sizeof(cfg->user_ctx))
        c->user_ctx = cfg->user_ctx;
    c->perspective = moq_session_perspective(cfg->session);
    c->first_peer_wt_bidi_seen = false;
    c->moq_control_stream_id = UINT64_MAX;
    c->prefix_registered = false;
    c->opened_bidi_count = 0;
    c->last_opened_bidi_id = UINT64_MAX;
    c->opened_uni_count = 0;
    c->last_opened_uni_id = UINT64_MAX;
    c->stop_sending_count = 0;
    c->last_stop_sending_stream_id = UINT64_MAX;

    /* Initialize endpoint ops. */
    if (pico_wt_endpoint_init(&c->endpoint_ops, &c->endpoint_ctx,
                              cfg->cnx, cfg->h3_ctx, cfg->ctrl_ctx,
                              &c->alloc) != 0) {
        c->alloc.free(c, sizeof(*c), c->alloc.ctx);
        return -1;
    }
    c->endpoint_ctx.on_bidi_opened = on_local_bidi_opened;
    c->endpoint_ctx.on_uni_opened = on_local_uni_opened;
    c->endpoint_ctx.on_local_stop_sending = on_local_stop_sending;
    c->endpoint_ctx.cb_ctx = c;
    c->endpoint_ctx.app_callback = moq_pico_wt_callback;
    c->endpoint_ctx.app_callback_ctx = c;

    /* Create bridge. */
    moq_transport_bridge_cfg_t bcfg;
    moq_transport_bridge_cfg_init(&bcfg, cfg->alloc);
    moq_result_t brc = moq_transport_bridge_create(
        &bcfg, cfg->session, &c->endpoint_ops, &c->endpoint_ctx,
        &c->bridge);
    if (brc < 0) {
        pico_wt_endpoint_cleanup(&c->endpoint_ctx);
        c->alloc.free(c, sizeof(*c), c->alloc.ctx);
        return -1;
    }

    /* Rebind the h3zero stream prefix so WT data streams and
     * datagrams route through this adapter's callback. The prefix
     * was registered by picowt_connect (client) or by the server
     * accept handler before this adapter was created. */
    h3zero_stream_prefix_t *pfx = h3zero_find_stream_prefix(
        cfg->h3_ctx, cfg->ctrl_ctx->stream_id);
    if (!pfx) {
        moq_transport_bridge_destroy(c->bridge);
        pico_wt_endpoint_cleanup(&c->endpoint_ctx);
        c->alloc.free(c, sizeof(*c), c->alloc.ctx);
        return -1;
    }
    pfx->function_call = moq_pico_wt_callback;
    pfx->function_ctx = c;
    c->prefix_registered = true;

    /* Also rebind the WT control stream's path callback. */
    cfg->ctrl_ctx->path_callback = moq_pico_wt_callback;
    cfg->ctrl_ctx->path_callback_ctx = c;

    *out = c;
    return 0;
}

/*
 * Clear path_callback on every h3zero stream that references this
 * adapter. Called from both destroy and deregister so no stale
 * callback can reach freed or detached adapter memory.
 */
static void clear_all_stream_callbacks(moq_pico_wt_conn_t *conn)
{
    if (!conn->h3_ctx) return;
    picosplay_node_t *node = picosplay_first(
        &conn->h3_ctx->h3_stream_tree);
    while (node) {
        h3zero_stream_ctx_t *sc = (h3zero_stream_ctx_t *)
            ((char *)node - offsetof(h3zero_stream_ctx_t,
                                      http_stream_node));
        if (sc->path_callback_ctx == conn) {
            sc->path_callback = NULL;
            sc->path_callback_ctx = NULL;
        }
        node = picosplay_next(node);
    }
}

/*
 * Detach this adapter from all picoquic/h3zero state. After this,
 * no h3zero callback can reach this adapter. Safe to call multiple
 * times (idempotent). Does NOT free the conn itself.
 */
static void detach_from_picoquic(moq_pico_wt_conn_t *conn)
{
    picoquic_cnx_t *cnx = conn->cnx;
    h3zero_callback_ctx_t *h3 = conn->h3_ctx;
    h3zero_stream_ctx_t *ctrl = conn->control_stream_ctx;
    uint64_t ctrl_id = conn->control_stream_id;

    clear_all_stream_callbacks(conn);

    conn->cnx = NULL;
    conn->h3_ctx = NULL;
    conn->control_stream_ctx = NULL;
    conn->prefix_registered = false;
    conn->endpoint_ctx.cnx = NULL;
    conn->endpoint_ctx.h3_ctx = NULL;
    conn->endpoint_ctx.control_stream_ctx = NULL;

    if (cnx && h3 && ctrl) {
        picowt_deregister(cnx, h3, ctrl);
        h3zero_delete_stream_prefix(cnx, h3, ctrl_id);
    }
}

void moq_pico_wt_conn_destroy(moq_pico_wt_conn_t *conn)
{
    if (!conn) return;
    detach_from_picoquic(conn);
    pw_rx_free_all(conn);   /* idempotent if deregister already ran */
    picowt_release_capsule(&conn->inbound_capsule);
    moq_transport_bridge_destroy(conn->bridge);
    pico_wt_endpoint_cleanup(&conn->endpoint_ctx);
    moq_alloc_t alloc = conn->alloc;
    alloc.free(conn, sizeof(moq_pico_wt_conn_t), alloc.ctx);
}

moq_session_t *moq_pico_wt_conn_session(moq_pico_wt_conn_t *conn)
{
    return conn ? conn->session : NULL;
}

bool moq_pico_wt_conn_is_fatal(const moq_pico_wt_conn_t *conn)
{
    return conn ? moq_transport_bridge_is_fatal(conn->bridge) : false;
}

bool moq_pico_wt_conn_is_closed(const moq_pico_wt_conn_t *conn)
{
    return conn ? moq_transport_bridge_is_closed(conn->bridge) : false;
}

uint64_t moq_pico_wt_conn_close_code(const moq_pico_wt_conn_t *conn)
{
    return conn ? moq_transport_bridge_close_code(conn->bridge) : 0;
}

void moq_pico_wt_conn_notify_transport_closed(moq_pico_wt_conn_t *conn,
                                              uint64_t code, uint64_t now_us)
{
    if (!conn) return;
    moq_transport_bridge_on_transport_close(conn->bridge, code, now_us);
}

/* -- Service -------------------------------------------------------- */

int moq_pico_wt_service(moq_pico_wt_conn_t *conn, uint64_t now_us)
{
    if (!conn) return -1;
    if (moq_transport_bridge_is_fatal(conn->bridge)) return -1;
    if (moq_transport_bridge_is_closed(conn->bridge)) return 0;

    moq_result_t rc = moq_transport_bridge_service(
        conn->bridge, now_us);
    if (rc >= 0 && !moq_transport_bridge_is_terminal(conn->bridge)) {
        /* replay anything the drain unblocked and issue owed grants */
        pw_rx_after_service(conn, now_us);
        if (moq_transport_bridge_is_fatal(conn->bridge)) return -1;
    }

    return rc < 0 ? -1 : 0;
}

/* -- Inbound helpers ------------------------------------------------ */

/*
 * Adapter-private fatal code recorded on the bridge when the adapter cannot
 * honour an inbound condition at all: an allocation failure while retaining
 * bytes, or an inability to establish or advance the owned receive window.
 * Session backpressure is NOT such a condition -- a stream that refuses bytes
 * is paused and its credit withheld, never torn down.
 *
 * Deliberately distinct from the shared bridge's codes so diagnostics and
 * tests can identify this exact adapter-initiated teardown: the bridge uses
 * 0x1 for generic internal fatals (NOMEM, session errors, ...) and 0x3 for the
 * "bytes arrived on a stream with pending inbound" contract violation, so we
 * use 0x10. The peer still sees picoquic's PICOQUIC_TRANSPORT_INTERNAL_ERROR,
 * raised when the callback returns -1.
 */
#define PICO_WT_INBOUND_FATAL_CODE 0x10


/* -- Inbound receive flow control ------------------------------------ *
 *
 * h3zero cannot decline a WT data callback and picoquic does not redeliver it,
 * so an adapter that cannot pause has only two options when the session refuses
 * bytes: drop them, or kill the connection. This adapter does neither. It takes
 * ownership of each WT data stream's QUIC receive window at first sight and
 * keeps it at most `budget` bytes ahead of what the session has taken. While a
 * stream has retained work the window is NOT advanced, so the peer stalls once
 * it exhausts the frozen window -- real, peer-visible backpressure rather than
 * unbounded local buffering. Bytes already inside the advertised window are
 * retained here (bounded BY that window) and replayed in order.
 *
 * Per-stream bounds are direct. Connection-level arrest is DERIVATIVE: picoquic
 * grows connection MAX_DATA from delivery, so once every stream's window is
 * frozen, delivery stops and the connection stops advancing too. There is no
 * per-connection control here (picoquic_set_max_data_control is quic-wide and
 * mutually exclusive with picoquic_open_flow_control).
 */

static pw_rx_stream_t *pw_rx_find(moq_pico_wt_conn_t *c, uint64_t sid)
{
    for (size_t i = 0; i < c->rx_count; i++)
        if (c->rx[i].active && c->rx[i].stream_id == sid)
            return &c->rx[i];
    return NULL;
}

/* The local receive window this endpoint advertised for `sid`'s category and
 * initiator. Never grant more than the operator configured. */
static bool pw_rx_budget_for(moq_pico_wt_conn_t *c, uint64_t sid, uint64_t *out)
{
    const picoquic_tp_t *tp;
    if (c->cnx == NULL) return false;
    tp = picoquic_get_transport_parameters(c->cnx, 1);
    if (tp == NULL) return false;
    if (!PICOQUIC_IS_BIDIR_STREAM_ID(sid)) {
        *out = tp->initial_max_stream_data_uni;
    } else {
        bool local_initiated =
            (PICOQUIC_IS_CLIENT_STREAM_ID(sid) != 0) ==
            (picoquic_is_client(c->cnx) != 0);
        *out = local_initiated ? tp->initial_max_stream_data_bidi_local
                               : tp->initial_max_stream_data_bidi_remote;
    }
    return true;
}

/* Find-or-create tracked state. NULL on allocation failure or when the budget
 * basis is unreadable (both fail closed). */
static pw_rx_stream_t *pw_rx_get(moq_pico_wt_conn_t *c, uint64_t sid,
                                 pw_rx_kind_t kind)
{
    pw_rx_stream_t *st = pw_rx_find(c, sid);
    if (st != NULL) return st;

    uint64_t budget = 0;
    if (!pw_rx_budget_for(c, sid, &budget))
        return NULL;              /* fail closed before taking a slot */

    for (size_t i = 0; i < c->rx_count; i++)
        if (!c->rx[i].active) { st = &c->rx[i]; break; }

    if (st == NULL) {
        if (c->rx_count == c->rx_cap) {
            size_t ncap = c->rx_cap ? c->rx_cap * 2 : 8;
            if (ncap <= c->rx_cap || ncap > SIZE_MAX / sizeof(*c->rx))
                return NULL;
            size_t oldb = c->rx_cap * sizeof(*c->rx);
            pw_rx_stream_t *n = (pw_rx_stream_t *)c->alloc.realloc(
                c->rx, oldb, ncap * sizeof(*c->rx), c->alloc.ctx);
            if (n == NULL) return NULL;
            c->rx = n;
            c->rx_cap = ncap;
        }
        st = &c->rx[c->rx_count++];
    }

    memset(st, 0, sizeof(*st));
    st->stream_id = sid;
    st->kind = (uint8_t)kind;
    st->budget = budget;
    st->granted = budget;         /* the frozen initial window is `budget` */
    st->active = true;
    return st;
}

/* Retire a stream's receive state. Idempotent. */
static void pw_rx_drop(moq_pico_wt_conn_t *c, uint64_t sid)
{
    pw_rx_stream_t *st = pw_rx_find(c, sid);
    if (st == NULL) return;
    if (st->buf != NULL)
        c->alloc.free(st->buf, st->buf_cap, c->alloc.ctx);
    memset(st, 0, sizeof(*st));   /* active = false */
}

static void pw_rx_free_all(moq_pico_wt_conn_t *c)
{
    for (size_t i = 0; i < c->rx_count; i++)
        if (c->rx[i].buf != NULL)
            c->alloc.free(c->rx[i].buf, c->rx[i].buf_cap, c->alloc.ctx);
    if (c->rx != NULL)
        c->alloc.free(c->rx, c->rx_cap * sizeof(*c->rx), c->alloc.ctx);
    c->rx = NULL;
    c->rx_count = 0;
    c->rx_cap = 0;
}

/* Append callback-owned bytes (and an optional trailing FIN) to the retention
 * buffer. Bounded by the advertised window: the peer cannot send past it
 * without a flow-control violation picoquic rejects, so exceeding it is an
 * impossible transport state. All arithmetic is overflow-checked. */
static bool pw_rx_retain(moq_pico_wt_conn_t *c, pw_rx_stream_t *st,
                         const uint8_t *bytes, size_t len, bool fin)
{
    if (len > 0) {
        uint64_t held = st->blocked + (uint64_t)st->buf_len;
        if (held < st->blocked)
            return false;
        if ((uint64_t)len > st->budget || held > st->budget - (uint64_t)len)
            return false;
        size_t need = st->buf_len + len;
        if (need < st->buf_len)
            return false;
        if (need > st->buf_cap) {
            size_t ncap = st->buf_cap ? st->buf_cap : 512;
            while (ncap < need) {
                if (ncap > SIZE_MAX / 2) { ncap = need; break; }
                ncap *= 2;
            }
            if ((uint64_t)ncap > st->budget) ncap = need;
            uint8_t *n = (uint8_t *)c->alloc.realloc(
                st->buf, st->buf_cap, ncap, c->alloc.ctx);
            if (n == NULL) return false;
            st->buf = n;
            st->buf_cap = ncap;
        }
        memcpy(st->buf + st->buf_len, bytes, len);
        st->buf_len = need;
    }
    if (fin) st->buf_fin = true;
    return true;
}

/* Feed the bridge through the stream's original routing kind. */
static moq_result_t pw_rx_feed(moq_pico_wt_conn_t *c, pw_rx_stream_t *st,
                               const uint8_t *bytes, size_t len, bool fin,
                               uint64_t now)
{
    switch ((pw_rx_kind_t)st->kind) {
    case PW_RX_CONTROL:
        return moq_transport_bridge_on_peer_control_bytes(
            c->bridge, st->stream_id, bytes, len, fin, now);
    case PW_RX_BIDI:
        return moq_transport_bridge_on_peer_bidi_bytes(
            c->bridge, st->stream_id, bytes, len, fin, now);
    case PW_RX_UNI:
    default:
        return moq_transport_bridge_on_peer_uni_bytes(
            c->bridge, st->stream_id, bytes, len, fin, now);
    }
}

/*
 * Advance the peer's receive window to keep at most `budget` bytes ahead of what
 * the session has taken. While held >= budget nothing is granted, so the window
 * stays frozen and the peer stalls.
 *
 * The caller gates this on the stream having NO retained bridge work. That gate
 * is load-bearing, not incidental: `delivered` mirrors picoquic's
 * consumed_offset, which counts transport-to-callback delivery rather than
 * application progress, so granting while work is still retained would
 * replenish credit from delivery alone and let blocked session input grow. With
 * the gate, an advance can only follow the session draining that work.
 *
 * Grants are BATCHED at budget/2: picoquic_open_flow_control also increments
 * connection-wide MAX_DATA by its argument, so per-callback grants would inflate
 * connection credit without bound. Batching bounds this adapter's contribution
 * to <= 2x the bytes the session has taken.
 *
 * picoquic_open_flow_control SILENTLY no-ops unless the connection is exactly
 * READY, so a pre-ready grant is left OWED (granted is not advanced) and issued
 * by the post-service sweep once ready. Returns 0, or -1 if picoquic rejected
 * the grant (the claimed window ownership no longer holds: fatal).
 */
static int pw_rx_refresh_credit(moq_pico_wt_conn_t *c, pw_rx_stream_t *st)
{
    if (!st->app_fc || c->cnx == NULL) return 0;
    uint64_t held = (uint64_t)st->buf_len + st->blocked;
    if (held < st->blocked || held >= st->budget)
        return 0;                                   /* no headroom: frozen */
    uint64_t headroom = st->budget - held;
    uint64_t target = st->delivered + headroom;
    if (target < st->delivered) return 0;
    uint64_t quantum = st->budget / 2;
    if (quantum == 0) quantum = 1;
    if (st->granted + quantum < st->granted ||
        target < st->granted + quantum)
        return 0;                                   /* batch: not enough owed */
    if (picoquic_get_cnx_state(c->cnx) != picoquic_state_ready)
        return 0;                                   /* stays OWED */
    if (picoquic_open_flow_control(c->cnx, st->stream_id, headroom) != 0)
        return -1;
    st->granted = target;
    return 0;
}

/*
 * One inbound WT data callback. Returns 0, or -1 after signalling a transport
 * error (the caller returns -1 to h3zero, which closes the connection).
 *
 * A persisting WOULD_BLOCK is NOT fatal here: the stream is paused, the bytes
 * are retained, and the frozen window makes the peer wait.
 */
static int pw_rx_on_data(moq_pico_wt_conn_t *c, uint64_t sid, pw_rx_kind_t kind,
                         const uint8_t *bytes, size_t len, bool fin,
                         uint64_t now)
{
    pw_rx_stream_t *st = pw_rx_get(c, sid, kind);
    if (st == NULL) {
        moq_transport_bridge_on_transport_error(
            c->bridge, PICO_WT_INBOUND_FATAL_CODE, now);
        return -1;
    }
    if (len > 0 && st->budget == 0) {
        /* a real zero budget admits a bare FIN but no payload */
        moq_transport_bridge_on_transport_error(
            c->bridge, PICO_WT_INBOUND_FATAL_CODE, now);
        return -1;
    }
    if (!st->app_fc) {                       /* first sight: take the window */
        if (c->cnx == NULL ||
            picoquic_set_app_flow_control(c->cnx, sid, 1) != 0) {
            moq_transport_bridge_on_transport_error(
                c->bridge, PICO_WT_INBOUND_FATAL_CODE, now);
            return -1;
        }
        st->app_fc = true;
    }
    st->delivered += len;

    if (st->paused) {
        /* the bridge still holds prior input: retain in order, do not feed */
        if (!pw_rx_retain(c, st, bytes, len, fin)) {
            moq_transport_bridge_on_transport_error(
                c->bridge, PICO_WT_INBOUND_FATAL_CODE, now);
            return -1;
        }
        return 0;
    }

    moq_result_t rc = pw_rx_feed(c, st, bytes, len, fin, now);
    if (moq_transport_bridge_is_terminal(c->bridge))
        return -1;                           /* handler drove terminal */
    if (rc == MOQ_ERR_WOULD_BLOCK) {
        st->paused = true;
        st->blocked = len;
        if (fin) st->fin_blocked = true;
        /* service may clear it immediately; the sweep below resumes if so */
        moq_transport_bridge_service(c->bridge, now);
        pw_rx_after_service(c, now);
        return moq_transport_bridge_is_fatal(c->bridge) ? -1 : 0;
    }
    if (rc < 0) {
        if (!moq_transport_bridge_is_terminal(c->bridge))
            moq_transport_bridge_on_transport_error(
                c->bridge, PICO_WT_INBOUND_FATAL_CODE, now);
        return -1;
    }
    if (fin) {                               /* fully consumed: retire */
        pw_rx_drop(c, sid);
    } else if (pw_rx_refresh_credit(c, st) != 0) {
        moq_transport_bridge_on_transport_error(
            c->bridge, PICO_WT_INBOUND_FATAL_CODE, now);
        return -1;
    }
    if (!moq_transport_bridge_is_terminal(c->bridge)) {
        moq_transport_bridge_service(c->bridge, now);
        pw_rx_after_service(c, now);
    }
    return moq_transport_bridge_is_fatal(c->bridge) ? -1 : 0;
}

/*
 * After the bridge drained: replay each paused stream whose pending input has
 * cleared, and issue any owed grant. Also runs the owed-grant sweep for streams
 * that were never paused, so a grant deferred while the connection was not yet
 * READY is not stranded waiting for further stream activity.
 */
static void pw_rx_after_service(moq_pico_wt_conn_t *c, uint64_t now)
{
    /* Never sweep a terminal connection. Callers check the bridge BEFORE
     * servicing it, but the service itself can go terminal -- a directly
     * delivered control FIN records a deferred close, and the service that
     * follows dispatches it. Without this guard the sweep would still run and
     * could replay retained bytes, or grant credit, on other streams after the
     * connection was already closing. */
    if (moq_transport_bridge_is_terminal(c->bridge))
        return;
    for (size_t i = 0; i < c->rx_count; i++) {
        pw_rx_stream_t *st = &c->rx[i];
        if (!st->active) continue;

        if (!st->paused) {
            if (pw_rx_refresh_credit(c, st) != 0) {
                moq_transport_bridge_on_transport_error(
                    c->bridge, PICO_WT_INBOUND_FATAL_CODE, now);
                return;
            }
            continue;
        }
        if (moq_transport_bridge_stream_has_pending(c->bridge, st->stream_id))
            continue;                        /* still blocked: stay frozen */

        st->blocked = 0;

        if (st->fin_blocked) {
            /* The FIN rode the blocked chunk and the bridge has now delivered
             * it. No close dispatch is needed here, unlike the buf_fin branch
             * below: this FIN was delivered by the bridge service that runs
             * BEFORE this sweep, so a close it initiated is already dispatched
             * (and a terminal bridge skips the sweep entirely). */
            pw_rx_drop(c, st->stream_id);
            continue;
        }

        if (st->buf_len > 0 || st->buf_fin) {
            uint8_t *buf = st->buf; size_t blen = st->buf_len;
            bool bfin = st->buf_fin; size_t bcap = st->buf_cap;
            /* detach first: the bridge does not hold it past the call, and a
             * re-block must re-track by byte count, not re-own this buffer */
            st->buf = NULL; st->buf_len = 0; st->buf_cap = 0; st->buf_fin = false;
            moq_result_t rc = pw_rx_feed(c, st, buf, blen, bfin, now);
            if (buf != NULL) c->alloc.free(buf, bcap, c->alloc.ctx);
            if (rc == MOQ_ERR_WOULD_BLOCK) {
                st->blocked = blen;
                if (bfin) st->fin_blocked = true;
                continue;                    /* remain paused */
            }
            if (rc < 0) {
                if (!moq_transport_bridge_is_terminal(c->bridge))
                    moq_transport_bridge_on_transport_error(
                        c->bridge, PICO_WT_INBOUND_FATAL_CODE, now);
                return;
            }
            if (bfin) {
                pw_rx_drop(c, st->stream_id);          /* stream done */
                /* A control FIN retained while paused is replayed HERE, inside
                 * the sweep. On the control stream that only sets the bridge's
                 * deferred close; without dispatching it now, a later stream in
                 * this same sweep could be replayed or granted credit after the
                 * connection was already meant to be closing. Dispatch it and
                 * stop the sweep once terminal; an unexpected negative dispatch
                 * that did not itself go terminal is a transport error. */
                if (!moq_transport_bridge_is_terminal(c->bridge) &&
                    pw_after_fin_service(c, now) < 0 &&
                    !moq_transport_bridge_is_terminal(c->bridge)) {
                    moq_transport_bridge_on_transport_error(
                        c->bridge, PICO_WT_INBOUND_FATAL_CODE, now);
                    return;
                }
                if (moq_transport_bridge_is_terminal(c->bridge))
                    return;
                continue;
            }
        }

        st->paused = false;
        if (pw_rx_refresh_credit(c, st) != 0) {
            moq_transport_bridge_on_transport_error(
                c->bridge, PICO_WT_INBOUND_FATAL_CODE, now);
            return;
        }
    }
}


/*
 * Classify a WT data stream for MoQ routing.
 * Returns true if the stream is the MoQ control bidi.
 */
static bool is_moq_control(moq_pico_wt_conn_t *c,
                            h3zero_stream_ctx_t *stream_ctx)
{
    uint64_t sid = stream_ctx->stream_id;

    /* WT control stream is NOT MoQ control. */
    if (sid == c->control_stream_id)
        return false;

    /* Uni-control-pair profiles (draft-18): control rides unidirectional
     * streams the bridge classifies itself; no bidi is ever MoQ control
     * (the first peer WT bidi is a request stream). */
    if (moq_transport_bridge_uses_uni_control(c->bridge))
        return false;

    /* Already classified? */
    if (c->moq_control_stream_id != UINT64_MAX)
        return sid == c->moq_control_stream_id;

    /* Server: first peer WT bidi is MoQ control. */
    if (c->perspective == MOQ_PERSPECTIVE_SERVER &&
        !c->first_peer_wt_bidi_seen &&
        PICOQUIC_IS_BIDIR_STREAM_ID(sid) &&
        !PICO_WT_IS_LOCAL_STREAM_ID(sid, 0)) {
        c->first_peer_wt_bidi_seen = true;
        c->moq_control_stream_id = sid;
        return true;
    }

    return false;
}

/*
 * Handle inbound bytes on the WT control stream. These are WebTransport
 * capsules (NOT MoQ data), so they are parsed here rather than delivered
 * to the bridge's stream handlers.
 *
 * v1 acts only on a well-formed CLOSE_WEBTRANSPORT_SESSION: a clean peer
 * close → the bridge goes closed (not fatal) with the capsule's session
 * error code, so close_code propagates end to end. A control-stream FIN
 * with no close capsule is also a clean close.
 *
 * Everything else is deliberately IGNORED (no fatal, no close):
 *  - DRAIN_WEBTRANSPORT_SESSION: this picoquic build encodes DRAIN as a
 *    zero-length close-type capsule, which picowt_receive_capsule itself
 *    reports as malformed (length < 4). A draining session must not be
 *    turned fatal, and DRAIN/short session capsules are byte-identical
 *    here, so we cannot fault one without faulting the other.
 *  - unknown capsule types (matches picowt, which logs and continues).
 *  - parse errors (rc < 0): ignored at this layer (the accumulator is
 *    reset so a later valid capsule still parses); a genuinely broken
 *    control stream still surfaces via reset/deregister, and a real
 *    CLOSE_SESSION arrives with the control-stream FIN handled below.
 * True DRAIN handling and stricter malformed rejection are deferred
 * until the two can be distinguished (see plan 14c/14d).
 *
 * The accumulator auto-resets on the next picowt_receive_capsule call
 * once a capsule is stored, so later capsules still parse.
 */
static void handle_control_capsule(moq_pico_wt_conn_t *c,
                                    uint8_t *bytes, size_t length,
                                    bool fin, uint64_t now)
{
    /* Parse only when there is an actual payload. h3zero can deliver a
     * zero-length post_fin (bytes == NULL, length == 0), and
     * h3zero_accumulate_capsule dereferences *bytes before guarding the
     * empty range, so calling picowt_receive_capsule with no bytes is
     * unsafe (OOB/NULL read). The FIN itself is handled below. */
    if (bytes != NULL && length > 0) {
        int rc = picowt_receive_capsule(c->cnx, bytes, bytes + length,
                                        &c->inbound_capsule);
        if (rc == 0 && c->inbound_capsule.h3_capsule.is_stored &&
            c->inbound_capsule.h3_capsule.capsule_type ==
                picowt_capsule_close_webtransport_session) {
            /* Clean peer close; propagate the WT session error code. */
            moq_transport_bridge_on_transport_close(
                c->bridge, c->inbound_capsule.error_code, now);
        } else if (rc < 0) {
            /* Malformed/short/DRAIN (see note above): ignore — but reset
             * the accumulator so a subsequent valid capsule still parses.
             * picowt_receive_capsule can leave partial, not-yet-stored
             * state behind, which would corrupt the next capsule. */
            picowt_release_capsule(&c->inbound_capsule);
        }
        /* Well-formed non-close capsules (a proper DRAIN, unknown types)
         * are ignored; the accumulator auto-resets on the next call once
         * a capsule has been stored. */
    }

    /* A control-stream FIN is a clean session close if nothing else
     * already terminated the bridge (idempotent: a CLOSE capsule's code
     * latched above wins). Covers the zero-length-FIN case too. */
    if (fin && !moq_transport_bridge_is_terminal(c->bridge))
        moq_transport_bridge_on_transport_close(c->bridge, 0, now);

    if (!moq_transport_bridge_is_terminal(c->bridge))
        moq_transport_bridge_service(c->bridge, now);
}

/* -- Inbound: picohttp callback → bridge --------------------------- */

int moq_pico_wt_callback(picoquic_cnx_t *cnx,
                           uint8_t *bytes, size_t length,
                           picohttp_call_back_event_t event,
                           h3zero_stream_ctx_t *stream_ctx,
                           void *path_app_ctx)
{
    moq_pico_wt_conn_t *c = (moq_pico_wt_conn_t *)path_app_ctx;
    if (!c || !c->bridge) return 0;

    uint64_t sid = stream_ctx ? stream_ctx->stream_id : 0;

    /* Let cleanup events run even when bridge is terminal.
     * Deregister means picoquic/h3zero is tearing down this session.
     * Compute now before clearing pointers, then detach from all
     * picoquic-owned state so destroy is safe after this. */
    if (event == picohttp_callback_deregister) {
        uint64_t now = picoquic_get_quic_time(picoquic_get_quic_ctx(cnx));
        clear_all_stream_callbacks(c);
        /* the transport is gone: drop every retained buffer now, and make sure
         * nothing can attempt a grant through the detached cnx afterwards */
        pw_rx_free_all(c);
        c->prefix_registered = false;
        c->cnx = NULL;
        c->h3_ctx = NULL;
        c->control_stream_ctx = NULL;
        c->endpoint_ctx.cnx = NULL;
        c->endpoint_ctx.h3_ctx = NULL;
        c->endpoint_ctx.control_stream_ctx = NULL;
        if (!moq_transport_bridge_is_terminal(c->bridge))
            moq_transport_bridge_on_transport_close(c->bridge, 0, now);
        return 0;
    }
    if (event == picohttp_callback_free)
        return 0;

    /* Pull send: h3zero is ready to put bytes on the wire for this WT data
     * stream. Serviced even while terminal so picoquic always gets a buffer
     * response (the queue is drained/empty by then, so it reneges). The WT
     * control stream is driven by h3zero itself, never routed here. */
    if (event == picohttp_callback_provide_data)
        return pico_wt_endpoint_on_provide_data(&c->endpoint_ctx, sid,
                                                bytes, length);

    if (moq_transport_bridge_is_terminal(c->bridge)) return 0;

    uint64_t now = picoquic_get_quic_time(picoquic_get_quic_ctx(cnx));

    switch (event) {
    case picohttp_callback_connecting:
        break;

    case picohttp_callback_connect:
        /* Server: picohttp_callback_connect fires on the path_callback
         * of the WT control stream. For attach-after-established, the
         * loopback harness handles CONNECT acceptance and prefix
         * registration before creating the adapter. */
        break;

    case picohttp_callback_connect_accepted:
        break;

    case picohttp_callback_connect_refused:
        moq_transport_bridge_on_transport_error(c->bridge, 0x1, now);
        break;

    case picohttp_callback_post_data:
    case picohttp_callback_post_fin: {
        bool fin = (event == picohttp_callback_post_fin);

        /* WT control stream carries WebTransport capsules, not MoQ
         * bytes — parse them here, never route to MoQ handlers. */
        if (sid == c->control_stream_id) {
            handle_control_capsule(c, bytes, length, fin, now);
            break;
        }

        bool is_bidi = PICOQUIC_IS_BIDIR_STREAM_ID(sid);
        bool ctrl = is_moq_control(c, stream_ctx);

        /* Inbound receive flow control owns this path: a session that cannot
         * accept the bytes pauses the stream and freezes its window instead of
         * killing the connection. -1 here is a genuine transport failure. */
        pw_rx_kind_t kind = ctrl ? PW_RX_CONTROL
                                 : (is_bidi ? PW_RX_BIDI : PW_RX_UNI);
        if (pw_rx_on_data(c, sid, kind, bytes, length, fin, now) != 0)
            return -1;
        break;
    }

    case picohttp_callback_post_datagram: {
        moq_result_t rc = moq_transport_bridge_on_peer_datagram(
            c->bridge, bytes, length, now);
        (void)rc;
        if (!moq_transport_bridge_is_terminal(c->bridge))
            moq_transport_bridge_service(c->bridge, now);
        break;
    }

    case picohttp_callback_provide_datagram:
        pico_wt_endpoint_provide_datagram(
            &c->endpoint_ctx, bytes, length);
        break;

    /* picohttp_callback_provide_data handled above (before the terminal gate). */

    case picohttp_callback_reset: {
        if (!stream_ctx) break;
        /* Inbound RESET_STREAM code is in the WebTransport application-error
         * space; map it back to the MoQ code before the bridge/session see
         * it (symmetric with ep_reset's outbound mapping). A legacy peer's
         * raw code passes through unchanged. */
        uint64_t error_code = pico_wt_wt_err_to_moq(
            picoquic_get_remote_stream_error(cnx, sid));
        if (sid == c->moq_control_stream_id) {
            moq_transport_bridge_on_transport_error(
                c->bridge, error_code ? error_code : 0x1, now);
        } else if (sid != c->control_stream_id) {
            /* RESET_STREAM aborts the PEER's sending direction: nothing more
             * will arrive, so abandon retained inbound bytes and stop granting
             * credit for this stream. */
            pw_rx_drop(c, sid);
            moq_transport_bridge_on_peer_stream_reset(
                c->bridge, sid, error_code, now);
            if (!moq_transport_bridge_is_terminal(c->bridge)) {
                moq_transport_bridge_service(c->bridge, now);
                pw_rx_after_service(c, now);
            }
        }
        break;
    }

    case picohttp_callback_stop_sending: {
        if (!stream_ctx) break;
        /* picoquic has no public stop-sending error code getter. */
        if (sid != c->control_stream_id) {
            /* STOP_SENDING aborts OUR sending direction only. On a bidi stream
             * the peer may keep sending, so inbound receive state is KEPT:
             * retained bytes still replay and credit keeps flowing. */
            moq_result_t rc = moq_transport_bridge_on_peer_stop_sending(
                c->bridge, sid, 0, now);
            c->stop_sending_count++;
            c->last_stop_sending_stream_id = sid;
            if (rc < 0 && rc != MOQ_ERR_WOULD_BLOCK)
                return -1;
            if (!moq_transport_bridge_is_terminal(c->bridge)) {
                moq_transport_bridge_service(c->bridge, now);
                pw_rx_after_service(c, now);
            }
            if (moq_transport_bridge_is_terminal(c->bridge))
                return -1;
        }
        break;
    }

    default:
        break;
    }

    return 0;
}
