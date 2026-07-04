/*
 * moq_picoquic.c — Picoquic adapter using the shared transport bridge.
 *
 * Routes picoquic callback events to moq_transport_bridge_t inbound
 * handlers. Outbound actions are dispatched by the bridge through the
 * picoquic endpoint ops (picoquic_endpoint.c).
 *
 * The bridge owns: stream mapping, pending state (outbound + inbound),
 * tombstones, close/drain semantics, and service ordering.
 * This file owns: picoquic callback routing, public API surface.
 */

#include <moq/picoquic.h>
#include <moq/transport_bridge.h>
#include "picoquic_endpoint.h"
#include <stdlib.h>
#include <string.h>

struct moq_pq_conn {
    moq_session_t              *session;
    picoquic_cnx_t             *cnx;
    moq_alloc_t                 alloc;
    void                       *user_ctx;
    void                      (*after_callback)(moq_pq_conn_t *conn,
                                                 void *user_ctx);

    moq_transport_bridge_t     *bridge;
    moq_transport_endpoint_ops_t endpoint_ops;
    pq_endpoint_ctx_t           endpoint_ctx;
};

/* -- Helpers -------------------------------------------------------- */

static uint64_t pq_now_us(picoquic_cnx_t *cnx)
{
    return picoquic_get_quic_time(picoquic_get_quic_ctx(cnx));
}

/* -- Public API ----------------------------------------------------- */

/* Frozen v0 layout size: the prefix present in every released version of this
 * struct, ending just before the first appended field (user_ctx). The pointer-
 * only initializer cannot know the caller's storage size, so it touches only
 * this prefix -- safe for an old binary whose moq_pq_conn_cfg_t was exactly
 * this size. */
#define MOQ_PQ_CONN_CFG_V0_SIZE (offsetof(moq_pq_conn_cfg_t, user_ctx))

void moq_pq_conn_cfg_init(moq_pq_conn_cfg_t *cfg)
{
    if (!cfg) return;
    /* Clear and stamp ONLY the v0 prefix: writing sizeof(*cfg) here would
     * overflow an old caller that allocated the smaller v0 struct. Appended
     * fields stay disabled (struct_size == v0 size); callers that want them
     * use moq_pq_conn_cfg_init_sized(). */
    memset(cfg, 0, MOQ_PQ_CONN_CFG_V0_SIZE);
    cfg->struct_size = (uint32_t)MOQ_PQ_CONN_CFG_V0_SIZE;
}

void moq_pq_conn_cfg_init_sized(moq_pq_conn_cfg_t *cfg, size_t cfg_size)
{
    if (!cfg) return;
    /* Clear exactly what the caller allocated, never more than this library's
     * struct knows about. A caller older than the library passes a smaller
     * cfg_size (we clear/stamp that prefix); a caller newer than the library
     * passes a larger one (we clamp to our sizeof and leave its extra fields
     * to its own initializer). */
    size_t n = cfg_size < sizeof(*cfg) ? cfg_size : sizeof(*cfg);
    if (n < sizeof(cfg->struct_size)) return;  /* too small to even stamp */
    memset(cfg, 0, n);
    cfg->struct_size = (uint32_t)n;
}

int moq_pq_conn_create(const moq_pq_conn_cfg_t *cfg,
                         moq_pq_conn_t **out)
{
    if (!cfg || !out) return -1;
    *out = NULL;

    size_t min_size = offsetof(moq_pq_conn_cfg_t, alloc)
                    + sizeof(cfg->alloc);
    if (cfg->struct_size < min_size) return -1;
    if (!cfg->session || !cfg->cnx || !cfg->alloc) return -1;
    if (!cfg->alloc->alloc || !cfg->alloc->free || !cfg->alloc->realloc)
        return -1;

    moq_pq_conn_t *c = (moq_pq_conn_t *)cfg->alloc->alloc(
        sizeof(moq_pq_conn_t), cfg->alloc->ctx);
    if (!c) return -1;
    memset(c, 0, sizeof(*c));

    c->session = cfg->session;
    c->cnx = cfg->cnx;
    c->alloc = *cfg->alloc;
    if (cfg->struct_size >= offsetof(moq_pq_conn_cfg_t, user_ctx) +
        sizeof(cfg->user_ctx))
        c->user_ctx = cfg->user_ctx;
    if (cfg->struct_size >= offsetof(moq_pq_conn_cfg_t, after_callback) +
        sizeof(cfg->after_callback))
        c->after_callback = cfg->after_callback;

    /* Initialize endpoint ops and bridge. */
    if (pq_endpoint_init(&c->endpoint_ops, &c->endpoint_ctx, cfg->cnx,
                         &c->alloc) != 0) {
        c->alloc.free(c, sizeof(*c), c->alloc.ctx);
        return -1;
    }

    moq_transport_bridge_cfg_t bcfg;
    moq_transport_bridge_cfg_init(&bcfg, cfg->alloc);
    moq_result_t brc = moq_transport_bridge_create(
        &bcfg, cfg->session, &c->endpoint_ops, &c->endpoint_ctx,
        &c->bridge);
    if (brc < 0) {
        pq_endpoint_cleanup(&c->endpoint_ctx);
        c->alloc.free(c, sizeof(*c), c->alloc.ctx);
        return -1;
    }

    picoquic_set_callback(cfg->cnx, moq_pq_callback, c);

    *out = c;
    return 0;
}

void moq_pq_conn_destroy(moq_pq_conn_t *conn)
{
    if (!conn) return;
    picoquic_set_callback(conn->cnx, NULL, NULL);
    moq_transport_bridge_destroy(conn->bridge);
    pq_endpoint_cleanup(&conn->endpoint_ctx);
    moq_alloc_t alloc = conn->alloc;
    alloc.free(conn, sizeof(moq_pq_conn_t), alloc.ctx);
}

moq_session_t *moq_pq_conn_session(moq_pq_conn_t *conn)
{
    return conn ? conn->session : NULL;
}

void *moq_pq_conn_user_ctx(const moq_pq_conn_t *conn)
{
    return conn ? conn->user_ctx : NULL;
}

void moq_pq_conn_set_user_ctx(moq_pq_conn_t *conn, void *user_ctx)
{
    if (conn) conn->user_ctx = user_ctx;
}

bool moq_pq_conn_is_fatal(const moq_pq_conn_t *conn)
{
    return conn ? moq_transport_bridge_is_fatal(conn->bridge) : false;
}

uint64_t moq_pq_conn_fatal_code(const moq_pq_conn_t *conn)
{
    return conn ? moq_transport_bridge_fatal_code(conn->bridge) : 0;
}

bool moq_pq_conn_is_closed(const moq_pq_conn_t *conn)
{
    return conn ? moq_transport_bridge_is_closed(conn->bridge) : false;
}

uint64_t moq_pq_conn_close_code(const moq_pq_conn_t *conn)
{
    return conn ? moq_transport_bridge_close_code(conn->bridge) : 0;
}

/* -- Test accessors ------------------------------------------------- */

size_t moq_pq_conn_active_stream_count(const moq_pq_conn_t *c)
{
    return c ? moq_transport_bridge_stream_count(c->bridge) : 0;
}

size_t moq_pq_conn_tombstone_count(const moq_pq_conn_t *c)
{
    return c ? moq_transport_bridge_tombstone_count(c->bridge) : 0;
}

/* -- Inbound: picoquic callback → bridge ---------------------------- */

int moq_pq_callback(picoquic_cnx_t *cnx,
                      uint64_t stream_id,
                      uint8_t *bytes, size_t length,
                      picoquic_call_back_event_t event,
                      void *callback_ctx,
                      void *stream_ctx)
{
    moq_pq_conn_t *c = (moq_pq_conn_t *)callback_ctx;
    if (!c) return 0;

    /* Pull send: picoquic is ready to put bytes on the wire for `stream_id`.
     * `bytes` is the provide context, `length` the largest buffer available.
     * Serviced even while terminal so picoquic always gets a buffer response
     * (the queue is drained/empty by then, so it reneges). */
    if (event == picoquic_callback_prepare_to_send) {
        pq_endpoint_on_prepare_to_send(&c->endpoint_ctx, stream_id,
                                       bytes, length);
        if (c->after_callback)
            c->after_callback(c, c->user_ctx);
        return 0;
    }

    if (moq_transport_bridge_is_terminal(c->bridge)) return 0;
    uint64_t now = pq_now_us(cnx);
    (void)stream_ctx;

    switch (event) {
    case picoquic_callback_stream_data:
    case picoquic_callback_stream_fin: {
        bool fin = (event == picoquic_callback_stream_fin);
        bool is_bidi = PICOQUIC_IS_BIDIR_STREAM_ID(stream_id);

        if (is_bidi) {
            /* Control routing is profile-dependent. Bidi-control profiles
             * (draft-16) carry control on client-initiated bidi stream 0
             * (matched in both directions: the peer's stream 0 and the
             * read half of our own). Uni-control-pair profiles (draft-18)
             * carry control on unidirectional streams the bridge
             * classifies itself -- every bidi stream is a request stream
             * there, including stream 0. */
            bool is_control = (stream_id == 0) &&
                !moq_transport_bridge_uses_uni_control(c->bridge);

            if (is_control) {
                moq_transport_bridge_on_peer_control_bytes(
                    c->bridge, stream_id, bytes, length, fin, now);
            } else {
                moq_transport_bridge_on_peer_bidi_bytes(
                    c->bridge, stream_id, bytes, length, fin, now);
            }
        } else {
            moq_transport_bridge_on_peer_uni_bytes(
                c->bridge, stream_id, bytes, length, fin, now);
        }
        break;
    }

    case picoquic_callback_stream_reset: {
        uint64_t error_code = picoquic_get_remote_stream_error(
            cnx, stream_id);
        moq_transport_bridge_on_peer_stream_reset(
            c->bridge, stream_id, error_code, now);
        break;
    }

    case picoquic_callback_stop_sending: {
        uint64_t error_code = picoquic_get_remote_stream_error(
            cnx, stream_id);
        moq_transport_bridge_on_peer_stop_sending(
            c->bridge, stream_id, error_code, now);
        break;
    }

    case picoquic_callback_datagram:
        moq_transport_bridge_on_peer_datagram(
            c->bridge, bytes, length, now);
        break;

    case picoquic_callback_close:
        moq_transport_bridge_on_transport_close(c->bridge, 0, now);
        break;

    case picoquic_callback_application_close: {
        uint64_t app_err = picoquic_get_application_error(cnx);
        moq_transport_bridge_on_transport_close(c->bridge, app_err, now);
        break;
    }

    default:
        break;
    }

    if (c->after_callback)
        c->after_callback(c, c->user_ctx);
    return 0;
}

/* -- Service -------------------------------------------------------- */

int moq_pq_service(moq_pq_conn_t *conn, uint64_t now_us)
{
    if (!conn) return -1;
    if (moq_transport_bridge_is_fatal(conn->bridge)) return -1;
    if (moq_transport_bridge_is_closed(conn->bridge)) return 0;

    moq_result_t rc = moq_transport_bridge_service(conn->bridge, now_us);

    if (conn->after_callback)
        conn->after_callback(conn, conn->user_ctx);

    return rc < 0 ? -1 : 0;
}
