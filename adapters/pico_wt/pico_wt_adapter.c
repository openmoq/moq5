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

/* QUIC stream ID classification */
#ifndef PICOQUIC_IS_BIDIR_STREAM_ID
#define PICOQUIC_IS_BIDIR_STREAM_ID(id) (((id) & 2) == 0)
#endif
/* Matches picoquic's IS_LOCAL_STREAM_ID: nonzero when stream was
 * initiated by the local side. is_client: 1 for client, 0 for server. */
#define PICO_WT_IS_LOCAL_STREAM_ID(id, is_client) \
    (((id) ^ (is_client)) & 1)

/* -- Local stream opened callbacks (from endpoint ops) -------------- */

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
    if (!cfg->alloc->alloc || !cfg->alloc->free)
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
    pico_wt_endpoint_init(&c->endpoint_ops, &c->endpoint_ctx,
                           cfg->cnx, cfg->h3_ctx, cfg->ctrl_ctx);
    c->endpoint_ctx.on_bidi_opened = on_local_bidi_opened;
    c->endpoint_ctx.on_uni_opened = on_local_uni_opened;
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

/* -- Service -------------------------------------------------------- */

int moq_pico_wt_service(moq_pico_wt_conn_t *conn, uint64_t now_us)
{
    if (!conn) return -1;
    if (moq_transport_bridge_is_fatal(conn->bridge)) return -1;
    if (moq_transport_bridge_is_closed(conn->bridge)) return 0;

    moq_result_t rc = moq_transport_bridge_service(
        conn->bridge, now_us);

    return rc < 0 ? -1 : 0;
}

/* -- Inbound helpers ------------------------------------------------ */

/*
 * Adapter-private fatal code recorded on the bridge when the adapter is
 * forced to tear down the connection on an unresolvable inbound
 * condition (h3zero cannot pause reads). Deliberately distinct from the
 * shared bridge's codes so diagnostics/tests can identify this exact
 * adapter-initiated teardown: the bridge uses 0x1 for generic internal
 * fatals (NOMEM, session errors, ...) and 0x3 for the "bytes arrived on
 * a stream with pending inbound" contract violation, so we use 0x10.
 * The peer still sees picoquic's PICOQUIC_TRANSPORT_INTERNAL_ERROR,
 * raised when the callback below returns -1.
 */
#define PICO_WT_INBOUND_FATAL_CODE 0x10

/*
 * Deliver inbound stream bytes to the bridge and handle the result.
 *
 * h3zero cannot pause reads. The bridge's inbound WOULD_BLOCK contract
 * forbids delivering more bytes on a stream that still has pending
 * state. Since we cannot pause, the only honest response when pending
 * cannot be cleared is to tear the connection down: we mark the bridge
 * fatal (so the adapter's own state is consistent) and return -1.
 *
 * Returning -1 from the picohttp callback makes picoquic raise
 * PICOQUIC_TRANSPORT_INTERNAL_ERROR and close the connection. Because
 * that close is locally initiated, picoquic delivers no further
 * close/deregister callback to confirm it - so the bridge MUST be
 * marked terminal here, not left waiting for a callback that will
 * never arrive.
 *
 * This is a hard failure, not a soft pause and not a clean close.
 *
 * Returns 0 on success, -1 on fatal or unresolvable WOULD_BLOCK.
 */
static int deliver_stream_bytes(moq_pico_wt_conn_t *c,
                                 uint64_t sid, uint8_t *bytes,
                                 size_t length, bool fin,
                                 bool is_control, bool is_bidi,
                                 uint64_t now)
{
    moq_result_t rc;

    if (is_control) {
        rc = moq_transport_bridge_on_peer_control_bytes(
            c->bridge, sid, bytes, length, fin, now);
    } else if (is_bidi) {
        rc = moq_transport_bridge_on_peer_bidi_bytes(
            c->bridge, sid, bytes, length, fin, now);
    } else {
        rc = moq_transport_bridge_on_peer_uni_bytes(
            c->bridge, sid, bytes, length, fin, now);
    }

    /* Handler drove the bridge terminal (clean close or fatal). */
    if (moq_transport_bridge_is_terminal(c->bridge))
        return -1;

    if (rc == MOQ_ERR_WOULD_BLOCK) {
        moq_transport_bridge_service(c->bridge, now);
        if (moq_transport_bridge_stream_has_pending(c->bridge, sid)) {
            /* Cannot pause, cannot accept: tear down. Mark the bridge
             * fatal so adapter state matches the connection close that
             * the -1 return triggers. */
            moq_transport_bridge_on_transport_error(
                c->bridge, PICO_WT_INBOUND_FATAL_CODE, now);
            return -1;
        }
    } else if (rc < 0) {
        /* Non-WOULD_BLOCK negative is a fatal inbound error. Ensure the
         * bridge reflects the teardown the -1 return will cause. */
        if (!moq_transport_bridge_is_terminal(c->bridge))
            moq_transport_bridge_on_transport_error(
                c->bridge, PICO_WT_INBOUND_FATAL_CODE, now);
        return -1;
    }

    if (!moq_transport_bridge_is_terminal(c->bridge))
        moq_transport_bridge_service(c->bridge, now);

    return 0;
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

        /* Propagate deliver_stream_bytes failure to h3zero. Returning
         * -1 causes h3zero to close the connection. This is the
         * correct hard-failure path when the bridge has unresolvable
         * inbound pending state - h3zero cannot pause reads, so the
         * only honest response is connection teardown. */
        if (deliver_stream_bytes(c, sid, bytes, length, fin,
                                  ctrl, is_bidi, now) != 0)
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

    case picohttp_callback_provide_data:
        break;

    case picohttp_callback_reset: {
        if (!stream_ctx) break;
        uint64_t error_code = picoquic_get_remote_stream_error(
            cnx, sid);
        if (sid == c->moq_control_stream_id) {
            moq_transport_bridge_on_transport_error(
                c->bridge, error_code ? error_code : 0x1, now);
        } else if (sid != c->control_stream_id) {
            moq_transport_bridge_on_peer_stream_reset(
                c->bridge, sid, error_code, now);
            if (!moq_transport_bridge_is_terminal(c->bridge))
                moq_transport_bridge_service(c->bridge, now);
        }
        break;
    }

    case picohttp_callback_stop_sending: {
        if (!stream_ctx) break;
        /* picoquic has no public stop-sending error code getter. */
        if (sid != c->control_stream_id) {
            moq_result_t rc = moq_transport_bridge_on_peer_stop_sending(
                c->bridge, sid, 0, now);
            c->stop_sending_count++;
            c->last_stop_sending_stream_id = sid;
            if (rc < 0 && rc != MOQ_ERR_WOULD_BLOCK)
                return -1;
            if (!moq_transport_bridge_is_terminal(c->bridge))
                moq_transport_bridge_service(c->bridge, now);
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
