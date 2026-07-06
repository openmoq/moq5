/*
 * moq_pico_wt_managed — managed picoquic WebTransport facade.
 *
 * Owns the QUIC context, one picoquic network thread, the WT CONNECT
 * lifecycle, and the MoQ session + adapter. Threading and wake/wait
 * machinery mirror moq_pq_threaded; the transport setup and servicing
 * use picoquic h3zero/picowt + moq_pico_wt_service. Supports CLIENT and
 * SERVER perspectives.
 */

#include <moq/pico_wt_managed.h>
#include <moq/picoquic_verify.h>

#include "../common/moq_alpn.h"
#include "../common/moq_pq_stream_backlog.h"
#include "pico_wt_adapter.h"

#include <picoquic.h>
#include <picoquic_packet_loop.h>
#include <picoquic_utils.h>
#include <pico_webtransport.h>
#include <h3zero_common.h>

#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <time.h>

#define PICO_WT_MANAGED_ALPN "h3"

/* Advertised inbound flow-control credit, per unidirectional stream and
 * connection-wide. picoquic's defaults (initial_max_stream_data_uni 64 KiB,
 * initial_max_data 1 MiB) cap how much of an inbound MoQ object is admitted
 * before the stream blocks on flow control. Sized to the session's receive
 * budget so a large object is admitted in full. */
#define PICO_WT_MANAGED_RECV_FLOW_CONTROL (16u * 1024u * 1024u)

/* Advertise the inbound uni-stream + connection receive credit on the default
 * transport parameters, so every cnx created from this quic context (client
 * cnx and inbound server cnxs) inherits it. picowt's per-cnx parameter setup
 * only min-raises (>= 16 KiB), so it preserves these. A configure_quic hook
 * still runs after and may override. */
static void managed_raise_recv_windows(picoquic_quic_t *quic)
{
    picoquic_set_default_tp_value(quic,
        picoquic_tp_initial_max_stream_data_uni,
        PICO_WT_MANAGED_RECV_FLOW_CONTROL);
    picoquic_set_default_tp_value(quic,
        picoquic_tp_initial_max_data, PICO_WT_MANAGED_RECV_FLOW_CONTROL);
}

struct moq_pico_wt_managed {
    moq_alloc_t        alloc;

    /* Copied config. */
    moq_perspective_t  perspective;
    char              *host;
    char              *sni;
    char              *path;
    char              *wt_protocols;   /* offer (client) / supported (server);
                                        * NULL = legacy no-negotiation */
    moq_version_t      negotiated;     /* 0 until known (under mutex) */
    int                port;
    bool               send_request_capacity;
    uint64_t           initial_request_capacity;
    uint64_t           goaway_timeout_us;
    bool               insecure_skip_verify;
    int              (*configure_quic)(picoquic_quic_t *, void *);
    void              *configure_quic_ctx;
    int              (*on_pump)(moq_pico_wt_managed_t *, uint64_t, void *);
    void              *on_pump_ctx;
    void             (*on_activity)(moq_pico_wt_managed_t *, void *);
    void              *on_activity_ctx;

    /* Owned transport. */
    picoquic_quic_t   *quic;
    picoquic_cnx_t    *cnx;             /* client connection */
    h3zero_callback_ctx_t *h3_ctx;      /* client */
    h3zero_stream_ctx_t   *ctrl_ctx;    /* client */
    picoquic_packet_loop_param_t loop_param;

    /* Server WT path handler (create-time, persistent for the QUIC
     * context's lifetime). */
    picohttp_server_path_item_t  path_table[1];
    picohttp_server_parameters_t server_param;

    /* Mutex-protected state. */
    pthread_mutex_t    mutex;
    pthread_cond_t     cond;
    picoquic_network_thread_ctx_t *thread_ctx;
    moq_session_t     *session;
    moq_pico_wt_conn_t *conn;
    picoquic_cnx_t    *active_cnx;
    bool               fatal;
    uint64_t           fatal_code;
    bool               closed;
    uint64_t           close_code;
    bool               close_flush_started;       /* network thread only */
    bool               close_flush_saw_inflight;   /* network thread only */
    uint64_t           close_flush_deadline_us;    /* network thread only */
    bool               stopped;
    bool               loop_came_up;   /* loop reached ready (latched) */
    bool               pump_exit;
    bool               tx_drained;      /* network thread: local stream flush
                                         * done (no queued/ready stream data or
                                         * unsent FIN). Read by drain(). */
    bool               activity_pending;
    bool               wake_pending;
    int                wake_in_flight;
    pthread_t          network_thread_id;
    bool               network_thread_id_set;
};

/* -- cfg helpers ---------------------------------------------------- */

static bool cfg_has_field(const moq_pico_wt_managed_cfg_t *cfg,
                           size_t offset, size_t size)
{
    return cfg->struct_size >= offset && size <= cfg->struct_size - offset;
}
#define CFG_HAS(cfg, field) \
    cfg_has_field(cfg, offsetof(moq_pico_wt_managed_cfg_t, field), \
                  sizeof((cfg)->field))

/* Exact membership of `tok` in a comma-separated protocol list, with
 * optional whitespace around entries -- a substring search (strstr) would
 * admit a selection that merely appears INSIDE an offered token (e.g.
 * "moqt-16" against an offer of "xmoqt-16"), violating the contract that
 * an un-offered selection is a terminal fatal. */
static bool wt_offer_contains(const char *list, const char *tok)
{
    size_t tok_len = strlen(tok);
    const char *p = list;
    while (*p) {
        while (*p == ' ' || *p == '\t' || *p == ',') p++;
        const char *start = p;
        while (*p && *p != ',') p++;
        const char *end = p;
        while (end > start && (end[-1] == ' ' || end[-1] == '\t')) end--;
        /* Tolerate RFC 8941 quoted-string members: strip one surrounding
         * pair of double-quotes so a quoted offer ("moqt-16") matches a
         * de-quoted token (moqt-16). The endpoint emits the offer quoted;
         * tests and legacy callers may pass bare tokens -- both work. */
        if (end - start >= 2 && start[0] == '"' && end[-1] == '"') {
            start++;
            end--;
        }
        if (tok_len > 0 && (size_t)(end - start) == tok_len &&
            memcmp(start, tok, tok_len) == 0)
            return true;
    }
    return false;
}

static char *dup_str(const moq_alloc_t *a, const char *s)
{
    if (!s) return NULL;
    size_t n = strlen(s) + 1;
    char *p = (char *)a->alloc(n, a->ctx);
    if (p) memcpy(p, s, n);
    return p;
}

static void allow_peer_missing_reset_stream_at(moq_pico_wt_managed_t *m,
                                               picoquic_cnx_t *cnx)
{
    if (!m || !cnx)
        return;

    picoquic_tp_t const *remote_const =
        picoquic_get_transport_parameters(cnx, 0);
    if (!remote_const || remote_const->is_reset_stream_at_enabled ||
        remote_const->max_datagram_frame_size == 0)
        return;

    /* Default WT interop compatibility: some deployed WT relays (e.g. moqx)
     * advertise every required H3/WT setting EXCEPT reset_stream_at. picowt
     * gates sending the WebTransport CONNECT on the parsed peer transport
     * parameters, so without this the CONNECT is never sent, the WT session
     * never establishes, and the relay idle-closes the connection (NO_ERROR)
     * before any MoQ control flows. We mark the peer as accepted here, before
     * h3zero processes SETTINGS and drains the pending CONNECT.
     *
     * Self-gating + safe: it is a no-op for a peer that already advertises
     * reset_stream_at, and applies only to the CLIENT path (the server path
     * does not install this callback). libmoq's WT data path uses RESET_STREAM,
     * never RESET_STREAM_AT, so faking this peer TP only unblocks CONNECT and
     * never causes libmoq to emit a frame the peer cannot accept. Kept in
     * libmoq's adapter rather than patching picoquic. */
    ((picoquic_tp_t *)remote_const)->is_reset_stream_at_enabled = 1;
}

static int managed_client_h3_callback(picoquic_cnx_t *cnx,
    uint64_t stream_id, uint8_t *bytes, size_t length,
    picoquic_call_back_event_t fin_or_event, void *callback_ctx,
    void *v_stream_ctx)
{
    moq_pico_wt_managed_t *m = (moq_pico_wt_managed_t *)callback_ctx;
    if (!m || !m->h3_ctx)
        return -1;
    allow_peer_missing_reset_stream_at(m, cnx);
    return h3zero_callback(cnx, stream_id, bytes, length, fin_or_event,
                           m->h3_ctx, v_stream_ctx);
}

static void mark_activity(moq_pico_wt_managed_t *m)
{
    pthread_mutex_lock(&m->mutex);
    m->activity_pending = true;
    pthread_cond_broadcast(&m->cond);
    pthread_mutex_unlock(&m->mutex);
    if (m->on_activity)
        m->on_activity(m, m->on_activity_ctx);
}

static void build_session_cfg(moq_pico_wt_managed_t *m,
                               moq_session_cfg_t *scfg,
                               moq_perspective_t persp)
{
    moq_session_cfg_init_sized(scfg, sizeof(*scfg), &m->alloc, persp);
    scfg->send_request_capacity = m->send_request_capacity;
    scfg->initial_request_capacity =
        m->initial_request_capacity ? m->initial_request_capacity : 64;
    scfg->goaway_timeout_us = m->goaway_timeout_us;
}

/* Server ALPN selector: pick "h3" (returns its index, or count if the
 * peer did not offer it). Local — avoids depending on picoquic's demo
 * server helper. */
static size_t managed_alpn_select(picoquic_quic_t *quic,
    picoquic_iovec_t *list, size_t count)
{
    (void)quic;
    for (size_t i = 0; i < count; i++)
        if (list[i].len == 2 && memcmp(list[i].base, "h3", 2) == 0)
            return i;
    return count;
}

/* -- WT client control-stream callback (network thread) ------------- */

static int managed_client_wt_cb(picoquic_cnx_t *cnx, uint8_t *bytes,
    size_t length, picohttp_call_back_event_t event,
    h3zero_stream_ctx_t *stream_ctx, void *app_ctx)
{
    moq_pico_wt_managed_t *m = (moq_pico_wt_managed_t *)app_ctx;
    (void)bytes; (void)length;

    if (event == picohttp_callback_connecting)
        return 0;

    if (event == picohttp_callback_connect_refused) {
        /* Server refused the WT session (non-2xx CONNECT response, e.g.
         * HTTP 501 for a one-connection server's second CONNECT). Latch a
         * terminal fatal so wait() returns MOQ_ERR_CLOSED promptly instead
         * of an indefinite no-setup wait. fatal_code stays 0: the refusal
         * callback carries no HTTP status, and 0 is the consistent
         * transport/handshake-level code (same as cert reject / refused). */
        pthread_mutex_lock(&m->mutex);
        m->fatal = true;
        m->fatal_code = 0;
        pthread_mutex_unlock(&m->mutex);
        mark_activity(m);
        return 0;
    }
    if (event != picohttp_callback_connect_accepted)
        return 0;

    uint64_t now = picoquic_get_quic_time(picoquic_get_quic_ctx(cnx));

    moq_session_cfg_t scfg;
    build_session_cfg(m, &scfg, MOQ_PERSPECTIVE_CLIENT);
    /* When an offer was sent, the selected WT-Protocol token decides the
     * session version (the token is parsed, de-quoted, and readable here,
     * BEFORE the session exists). A server-echoed token that is unknown or
     * one we did not offer is a terminal fatal -- never a silent default.
     *
     * A MISSING token is the legacy no-subprotocol case: moxygen-based
     * relays (e.g. moqx) accept the WebTransport CONNECT without echoing
     * WT-Protocol and negotiate the MoQ version in CLIENT/SERVER_SETUP
     * instead. LibMoQ binds ONE concrete profile at session creation and
     * cannot infer draft-18 without a transport-selected token, so it falls
     * back to draft-16 ONLY when draft-16 was offered; an offer that omits
     * draft-16 (e.g. EXACT draft-18) stays terminal fatal -- never a silent
     * draft-18 -> draft-16 downgrade. This is a draft-16 legacy fallback for
     * WT peers that omit WT-Protocol, not in-band multi-version negotiation. */
    if (m->wt_protocols) {
        const char *tok =
            (const char *)stream_ctx->ps.stream_state.header.wt_protocol;
        moq_version_t v = (moq_version_t)0;
        if (tok && tok[0] != '\0') {
            if (!moq_alpn_to_version(tok, strlen(tok), &v) ||
                !wt_offer_contains(m->wt_protocols, tok))
                goto fail;
            scfg.version = v;
        } else {
            const char *d16 = moq_alpn_for_version(MOQ_VERSION_DRAFT_16);
            if (!d16 || !wt_offer_contains(m->wt_protocols, d16))
                goto fail;
            scfg.version = MOQ_VERSION_DRAFT_16;
        }
    }
    moq_session_t *session = NULL;
    if (moq_session_create(&scfg, now, &session) != MOQ_OK)
        goto fail;

    moq_pico_wt_conn_cfg_t wc;
    moq_pico_wt_conn_cfg_init_sized(&wc, sizeof(wc));
    wc.session = session;
    wc.cnx = cnx;
    wc.h3_ctx = m->h3_ctx;
    wc.ctrl_ctx = m->ctrl_ctx;
    wc.alloc = &m->alloc;
    moq_pico_wt_conn_t *conn = NULL;
    if (moq_pico_wt_conn_create(&wc, &conn) != 0) {
        moq_session_destroy(session);
        goto fail;
    }

    moq_session_start(session, now);

    pthread_mutex_lock(&m->mutex);
    m->session = session;
    m->conn = conn;
    m->active_cnx = cnx;
    m->negotiated = scfg.version ? scfg.version : MOQ_VERSION_DRAFT_16;
    pthread_mutex_unlock(&m->mutex);
    mark_activity(m);
    return 0;

fail:
    pthread_mutex_lock(&m->mutex);
    m->fatal = true;
    m->fatal_code = 0;
    pthread_mutex_unlock(&m->mutex);
    mark_activity(m);
    return -1;
}

/* -- WT server control-stream callback (network thread) ------------- */

static int managed_server_wt_cb(picoquic_cnx_t *cnx, uint8_t *bytes,
    size_t length, picohttp_call_back_event_t event,
    h3zero_stream_ctx_t *stream_ctx, void *app_ctx)
{
    moq_pico_wt_managed_t *m = (moq_pico_wt_managed_t *)app_ctx;
    (void)bytes; (void)length;

    if (event != picohttp_callback_connect)
        return 0;  /* data routes to the adapter after attach */

    /* One active connection: refuse a second CONNECT. (Callbacks are
     * serialized on the single network thread, so this is race-free.)
     * Returning -1 on picohttp_callback_connect makes h3zero answer with
     * an explicit HTTP 501 (FIN'd) on the CONNECT stream; the client maps
     * the non-2xx status to picohttp_callback_connect_refused, which
     * managed_client_wt_cb latches as fatal. So refusal is a deterministic
     * client-terminal fatal, not a silent timeout. */
    if (m->conn)
        return -1;

    h3zero_callback_ctx_t *h3 =
        (h3zero_callback_ctx_t *)picoquic_get_callback_context(cnx);
    if (h3zero_declare_stream_prefix(h3, stream_ctx->stream_id,
                                     managed_server_wt_cb, m) != 0)
        return -1;

    uint64_t now = picoquic_get_quic_time(picoquic_get_quic_ctx(cnx));
    moq_session_cfg_t scfg;
    build_session_cfg(m, &scfg, MOQ_PERSPECTIVE_SERVER);
    /* Server-side negotiation: with a supported list configured, a client
     * offer is matched in THE CLIENT'S order (picowt_select_wt_protocol
     * writes the pick into the response's WT-Protocol header) and the
     * session version follows the pick; no overlap refuses the CONNECT
     * (explicit non-2xx). A client that sent no offer gets the legacy
     * draft-16 behavior whether or not a list is configured. */
    if (m->wt_protocols &&
        stream_ctx->ps.stream_state.header.wt_available_protocols != NULL) {
        moq_version_t v = (moq_version_t)0;
        if (picowt_select_wt_protocol(stream_ctx, m->wt_protocols) != 0)
            return -1;
        const char *sel = stream_ctx->ps.stream_state.wt_protocol;
        if (!sel || !moq_alpn_to_version(sel, strlen(sel), &v))
            return -1;
        scfg.version = v;
    }
    moq_session_t *session = NULL;
    if (moq_session_create(&scfg, now, &session) != MOQ_OK)
        goto fail;

    moq_pico_wt_conn_cfg_t wc;
    moq_pico_wt_conn_cfg_init_sized(&wc, sizeof(wc));
    wc.session = session;
    wc.cnx = cnx;
    wc.h3_ctx = h3;
    wc.ctrl_ctx = stream_ctx;
    wc.alloc = &m->alloc;
    moq_pico_wt_conn_t *conn = NULL;
    if (moq_pico_wt_conn_create(&wc, &conn) != 0) {
        moq_session_destroy(session);
        goto fail;
    }

    /* Start the server session: uni-control-pair profiles (draft-18) send
     * their own SETUP from start; a draft-16 server defines start as
     * MOQ_ERR_WRONG_STATE (the client initiates), the draft-neutral no-op. */
    {
        moq_result_t src = moq_session_start(session, now);
        if (src != MOQ_OK && src != MOQ_ERR_WRONG_STATE) {
            moq_pico_wt_conn_destroy(conn);
            moq_session_destroy(session);
            goto fail;
        }
    }

    pthread_mutex_lock(&m->mutex);
    m->session = session;
    m->conn = conn;
    m->active_cnx = cnx;
    m->negotiated = scfg.version ? scfg.version : MOQ_VERSION_DRAFT_16;
    pthread_mutex_unlock(&m->mutex);
    mark_activity(m);
    return 0;  /* accept the WT session */

fail:
    pthread_mutex_lock(&m->mutex);
    m->fatal = true;
    m->fatal_code = 0;
    pthread_mutex_unlock(&m->mutex);
    mark_activity(m);
    return -1;
}

/* -- Packet-loop callback (network thread) -------------------------- */

static int loop_callback(picoquic_quic_t *quic,
    picoquic_packet_loop_cb_enum cb_mode,
    void *callback_ctx, void *callback_arg)
{
    moq_pico_wt_managed_t *m = (moq_pico_wt_managed_t *)callback_ctx;

    if (cb_mode == picoquic_packet_loop_ready) {
        pthread_mutex_lock(&m->mutex);
        m->network_thread_id = pthread_self();
        m->network_thread_id_set = true;
        m->loop_came_up = true;
        pthread_cond_broadcast(&m->cond);
        pthread_mutex_unlock(&m->mutex);
        if (callback_arg)
            ((picoquic_packet_loop_options_t *)callback_arg)
                ->do_time_check = 1;
        return 0;
    }

    if (cb_mode == picoquic_packet_loop_wake_up) {
        pthread_mutex_lock(&m->mutex);
        m->wake_pending = false;
        pthread_mutex_unlock(&m->mutex);
    } else if (cb_mode == picoquic_packet_loop_time_check) {
        packet_loop_time_check_arg_t *tc =
            (packet_loop_time_check_arg_t *)callback_arg;
        if (tc && tc->delta_t > 20000) tc->delta_t = 20000;
    } else if (cb_mode != picoquic_packet_loop_after_receive &&
               cb_mode != picoquic_packet_loop_after_send) {
        return 0;
    }

    /* Client connect failure: if the QUIC connection reaches the
     * disconnected state before the WT session is up (handshake
     * timeout, connection refused, cert rejected), surface it as fatal
     * so the app does not wait forever. */
    if (m->perspective == MOQ_PERSPECTIVE_CLIENT && m->cnx && !m->conn &&
        picoquic_get_cnx_state(m->cnx) == picoquic_state_disconnected) {
        pthread_mutex_lock(&m->mutex);
        if (!m->fatal && !m->closed) { m->fatal = true; m->fatal_code = 0; }
        pthread_mutex_unlock(&m->mutex);
        mark_activity(m);
        return PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP;
    }

    /* Snapshot conn (set by the WT callback on this same thread). */
    moq_pico_wt_conn_t *conn = m->conn;
    if (!conn)
        return 0;

    if (m->cnx &&
        picoquic_get_cnx_state(m->cnx) == picoquic_state_disconnected &&
        !moq_pico_wt_conn_is_closed(conn) && !moq_pico_wt_conn_is_fatal(conn)) {
        moq_pico_wt_conn_notify_transport_closed(
            conn, 0, picoquic_get_quic_time(quic));
    }

    uint64_t now = picoquic_get_quic_time(quic);

    /* service → on_pump → service (all on the network thread). */
    if (moq_pico_wt_service(conn, now) < 0) {
        pthread_mutex_lock(&m->mutex);
        m->fatal = true;
        m->fatal_code = moq_transport_bridge_fatal_code(conn->bridge);
        pthread_mutex_unlock(&m->mutex);
        mark_activity(m);
        return PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP;
    }

    if (m->on_pump) {
        int prc = m->on_pump(m, now, m->on_pump_ctx);
        if (prc != 0) {
            pthread_mutex_lock(&m->mutex);
            m->pump_exit = true;
            pthread_mutex_unlock(&m->mutex);
            mark_activity(m);
            return PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP;
        }
    }

    if (moq_pico_wt_service(conn, now) < 0) {
        pthread_mutex_lock(&m->mutex);
        m->fatal = true;
        m->fatal_code = moq_transport_bridge_fatal_code(conn->bridge);
        pthread_mutex_unlock(&m->mutex);
        mark_activity(m);
        return PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP;
    }

    /* Cache terminal state for thread-safe accessors. fatal wins over
     * closed if the bridge reports both. */
    bool fatal_now, closed_now;
    pthread_mutex_lock(&m->mutex);
    if (moq_pico_wt_conn_is_fatal(conn)) {
        m->fatal = true;
        m->fatal_code = moq_transport_bridge_fatal_code(conn->bridge);
    }
    if (moq_pico_wt_conn_is_closed(conn)) {
        m->closed = true;
        m->close_code = moq_pico_wt_conn_close_code(conn);
    }
    fatal_now = m->fatal;
    closed_now = m->closed;
    pthread_mutex_unlock(&m->mutex);

    /* Fatal is terminal immediately — there is no clean frame worth
     * preserving. wait()/wake() are already terminal (m->fatal). */
    if (fatal_now) {
        mark_activity(m);
        return PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP;
    }

    /* Clean close: wait()/wake() are already terminal (m->closed), so a
     * consumer that only loops on wait() observes the close immediately
     * without polling. Keep the loop running briefly so picoquic can push
     * the queued CLOSE_SESSION/FIN out (a WT session close is a
     * control-stream capsule, not a QUIC close, so we must drive the
     * send). We let the loop's normal send pass run — forcing an
     * immediate wake here would re-enter before the send and skip it.
     * picoquic_is_cnx_backlog_empty tracks the retransmission backlog
     * (sent-unacked packets), not queued-but-unsent stream data, so it
     * reads "empty" before the queued close has been sent. Only treat
     * empty as flushed once we have observed non-ACK backlog after the
     * close (best available signal, not capsule-specific proof); a short
     * cap still bounds the wait so a vanished peer cannot pin the loop
     * open. */
    if (closed_now) {
        mark_activity(m);
        if (!m->close_flush_started) {
            m->close_flush_started = true;
            m->close_flush_deadline_us = now + 200000;  /* 200ms cap */
        }
        bool backlog_empty = !m->active_cnx ||
                             picoquic_is_cnx_backlog_empty(m->active_cnx);
        if (!backlog_empty)
            m->close_flush_saw_inflight = true;  /* non-ACK backlog seen */
        if ((m->close_flush_saw_inflight && backlog_empty) ||
            now >= m->close_flush_deadline_us)
            return PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP;
        return 0;
    }

    /* Wake at the next session deadline. */
    if (m->active_cnx) {
        uint64_t dl = moq_session_next_deadline_us(
            moq_pico_wt_conn_session(conn));
        picoquic_set_app_wake_time(m->active_cnx,
                                   dl == UINT64_MAX ? 0 : dl);
    }

    /* Publish the graceful-drain state (network thread): local stream flush is
     * done once no stream has queued/ready reliable data or an unsent FIN -- i.e.
     * every reliable stream byte + FIN has been handed to the transport. This is
     * deliberately NOT the packet-ACK backlog (picoquic_is_cnx_backlog_empty),
     * which can stay non-empty forever on an idle connection after the bytes are
     * sent and the peer has surfaced the object. The probe is network-thread-only
     * and side-effect free. */
    {
        bool drained =
            m->active_cnx &&
            picoquic_get_cnx_state(m->active_cnx) == picoquic_state_ready &&
            moq_pq_cnx_stream_backlog_empty(m->active_cnx);
        pthread_mutex_lock(&m->mutex);
        m->tx_drained = drained;
        pthread_mutex_unlock(&m->mutex);
    }

    mark_activity(m);
    return 0;
}

/* -- cfg_init ------------------------------------------------------- */

void moq_pico_wt_managed_cfg_init(moq_pico_wt_managed_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = sizeof(*cfg);
}

/* -- create --------------------------------------------------------- */

/* True if `host` is a numeric IP literal (v4 or v6) rather than a DNS name.
 * SNI (RFC 6066) is a DNS name and must not carry an IP literal, so the SNI /
 * verified-name default differs for IP hosts (see the create path). */
static bool host_is_ip_literal(const char *host)
{
    unsigned char buf[16];
    return inet_pton(AF_INET, host, buf) == 1 ||
           inet_pton(AF_INET6, host, buf) == 1;
}

static void free_strings(moq_pico_wt_managed_t *m)
{
    if (m->host) m->alloc.free(m->host, strlen(m->host) + 1, m->alloc.ctx);
    if (m->sni)  m->alloc.free(m->sni, strlen(m->sni) + 1, m->alloc.ctx);
    if (m->path) m->alloc.free(m->path, strlen(m->path) + 1, m->alloc.ctx);
    if (m->wt_protocols)
        m->alloc.free(m->wt_protocols, strlen(m->wt_protocols) + 1,
                      m->alloc.ctx);
    m->host = m->sni = m->path = NULL;
    m->wt_protocols = NULL;
}

moq_result_t moq_pico_wt_managed_create(
    const moq_pico_wt_managed_cfg_t *cfg, moq_pico_wt_managed_t **out)
{
    if (out) *out = NULL;
    if (!cfg || !out) return MOQ_ERR_INVAL;
    if (cfg->struct_size < offsetof(moq_pico_wt_managed_cfg_t, alloc) +
        sizeof(cfg->alloc))
        return MOQ_ERR_INVAL;
    if (!cfg->alloc || !cfg->alloc->alloc || !cfg->alloc->free ||
        !cfg->alloc->realloc)
        return MOQ_ERR_INVAL;

    moq_perspective_t persp = CFG_HAS(cfg, perspective)
        ? cfg->perspective : 0;
    if (persp != MOQ_PERSPECTIVE_CLIENT && persp != MOQ_PERSPECTIVE_SERVER)
        return MOQ_ERR_INVAL;
    if (!CFG_HAS(cfg, on_pump) || !cfg->on_pump)
        return MOQ_ERR_INVAL;

    int port = CFG_HAS(cfg, port) ? cfg->port : 0;
    if (port <= 0 || port > 65535) return MOQ_ERR_INVAL;
    const char *host = CFG_HAS(cfg, host) ? cfg->host : NULL;
    const char *cert = CFG_HAS(cfg, cert_path) ? cfg->cert_path : NULL;
    const char *key  = CFG_HAS(cfg, key_path)  ? cfg->key_path  : NULL;
    if (persp == MOQ_PERSPECTIVE_CLIENT) {
        if (!host || host[0] == '\0') return MOQ_ERR_INVAL;
    } else {                                        /* SERVER */
        if (!cert || cert[0] == '\0' || !key || key[0] == '\0')
            return MOQ_ERR_INVAL;
    }

    moq_pico_wt_managed_t *m = (moq_pico_wt_managed_t *)cfg->alloc->alloc(
        sizeof(*m), cfg->alloc->ctx);
    if (!m) return MOQ_ERR_NOMEM;
    memset(m, 0, sizeof(*m));
    m->alloc = *cfg->alloc;
    m->perspective = persp;
    m->port = port;

    /* path for both; host + sni for the client. */
    m->path = dup_str(&m->alloc,
        (CFG_HAS(cfg, path) && cfg->path) ? cfg->path : "/moq");
    bool dup_ok = (m->path != NULL);
    if (persp == MOQ_PERSPECTIVE_CLIENT) {
        m->host = dup_str(&m->alloc, host);
        /* Choose the SNI / verified server name. An explicit cfg->sni always
         * wins. Otherwise: for a DNS host, default to the host (the default
         * verifier then checks the cert against the real name we connected to).
         * For an IP-literal host there is no DNS name to send as SNI (RFC 6066),
         * so never feed the IP to picoquic -- use a benign valid SNI
         * ("localhost", the historical default). The exception is the one case
         * that would silently verify against that placeholder: relying on the
         * BUILT-IN default verifier (verified mode AND no configure_quic hook to
         * override it) to authenticate a bare IP with no expected name -> fail
         * closed (MOQ_ERR_INVAL) so the caller supplies cfg->sni. insecure mode
         * and caller-supplied configure_quic verifiers are the caller's call. */
        const char *sni_src = NULL;
        bool insecure = CFG_HAS(cfg, insecure_skip_verify) &&
                        cfg->insecure_skip_verify;
        bool has_configure = CFG_HAS(cfg, configure_quic) && cfg->configure_quic;
        if (CFG_HAS(cfg, sni) && cfg->sni) {
            sni_src = cfg->sni;
        } else if (!host_is_ip_literal(host)) {
            sni_src = host;
        } else if (insecure || has_configure) {
            sni_src = "localhost";
        } else {
            /* IP host authenticated by the default verifier, no expected
             * name -> cannot verify. */
            free_strings(m);
            m->alloc.free(m, sizeof(*m), m->alloc.ctx);
            return MOQ_ERR_INVAL;
        }
        m->sni = dup_str(&m->alloc, sni_src);
        dup_ok = dup_ok && m->host && m->sni;
    }
    if (CFG_HAS(cfg, wt_protocols) && cfg->wt_protocols &&
        cfg->wt_protocols[0] != '\0') {
        m->wt_protocols = dup_str(&m->alloc, cfg->wt_protocols);
        dup_ok = dup_ok && m->wt_protocols;
    }
    if (!dup_ok) {
        free_strings(m);
        m->alloc.free(m, sizeof(*m), m->alloc.ctx);
        return MOQ_ERR_NOMEM;
    }

    if (CFG_HAS(cfg, send_request_capacity))
        m->send_request_capacity = cfg->send_request_capacity;
    if (CFG_HAS(cfg, initial_request_capacity))
        m->initial_request_capacity = cfg->initial_request_capacity;
    if (CFG_HAS(cfg, goaway_timeout_us))
        m->goaway_timeout_us = cfg->goaway_timeout_us;
    if (CFG_HAS(cfg, insecure_skip_verify))
        m->insecure_skip_verify = cfg->insecure_skip_verify;
    if (CFG_HAS(cfg, configure_quic)) {
        m->configure_quic = cfg->configure_quic;
        m->configure_quic_ctx = cfg->configure_quic_ctx;
    }
    m->on_pump = cfg->on_pump;
    m->on_pump_ctx = CFG_HAS(cfg, on_pump_ctx) ? cfg->on_pump_ctx : NULL;
    if (CFG_HAS(cfg, on_activity)) {
        m->on_activity = cfg->on_activity;
        m->on_activity_ctx = cfg->on_activity_ctx;
    }

    if (pthread_mutex_init(&m->mutex, NULL) != 0) {
        free_strings(m);
        m->alloc.free(m, sizeof(*m), m->alloc.ctx);
        return MOQ_ERR_INTERNAL;
    }
    if (pthread_cond_init(&m->cond, NULL) != 0) {
        pthread_mutex_destroy(&m->mutex);
        free_strings(m);
        m->alloc.free(m, sizeof(*m), m->alloc.ctx);
        return MOQ_ERR_INTERNAL;
    }

    uint64_t now = picoquic_current_time();

    if (persp == MOQ_PERSPECTIVE_SERVER) {
        /* Designated initializers: picohttp_server_path_item_t grows new
         * trailing fields upstream (connect_protocol, origin_validator,
         * connect_error_status, ...); naming the fields we set zero-fills the
         * rest to their defaults and stays warning-clean under
         * -Werror=missing-field-initializers as the struct evolves. */
        m->path_table[0] = (picohttp_server_path_item_t){
            .path = m->path,
            .path_length = strlen(m->path),
            .path_callback = managed_server_wt_cb,
            .path_app_ctx = m,
        };
        m->server_param.path_table = m->path_table;
        m->server_param.path_table_nb = 1;

        m->quic = picoquic_create(8, cert, key, NULL,
            PICO_WT_MANAGED_ALPN, h3zero_callback, &m->server_param,
            NULL, NULL, NULL, now, NULL, NULL, NULL, 0);
        if (!m->quic) goto fail_quic;
        picoquic_set_alpn_select_fn_v2(m->quic, managed_alpn_select);
        picowt_set_default_transport_parameters(m->quic);
        managed_raise_recv_windows(m->quic);
        if (m->configure_quic &&
            m->configure_quic(m->quic, m->configure_quic_ctx) != 0)
            goto fail_configure;
        /* Session + adapter created lazily in managed_server_wt_cb. */
    } else {
        m->quic = picoquic_create(1, NULL, NULL, NULL,
            PICO_WT_MANAGED_ALPN, NULL, NULL, NULL, NULL, NULL, now,
            NULL, NULL, NULL, 0);
        if (!m->quic) goto fail_quic;
        managed_raise_recv_windows(m->quic);
        /* Fail closed by default. picoquic's built-in default has no CA store
         * and ACCEPTS any peer cert, so the safe-looking default config must
         * install a real verifier itself -- otherwise insecure_skip_verify=false
         * silently accepts a MITM cert. insecure_skip_verify=true is the only
         * way to opt out (explicit null verifier, tests/demos). The default
         * verifier uses the system trust store and checks the server name
         * (m->sni, defaulted to the connect host); a configure_quic hook still
         * runs AFTER it, so callers can swap in a private-CA verifier or other
         * TLS policy. A verifier that cannot be installed (e.g. no system trust
         * store) fails create rather than connecting unauthenticated. */
        if (m->insecure_skip_verify) {
            picoquic_set_null_verifier(m->quic);
        } else if (moq_picoquic_set_cert_verifier(m->quic, NULL) != 0) {
            goto fail_configure;
        }
        if (m->configure_quic &&
            m->configure_quic(m->quic, m->configure_quic_ctx) != 0)
            goto fail_configure;

        struct addrinfo hints, *res = NULL;
        memset(&hints, 0, sizeof(hints));
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_DGRAM;
        char port_str[8];
        snprintf(port_str, sizeof(port_str), "%d", port);
        if (getaddrinfo(m->host, port_str, &hints, &res) != 0 || !res)
            goto fail_configure;
        int prc = picowt_prepare_client_cnx(m->quic, res->ai_addr,
            &m->cnx, &m->h3_ctx, &m->ctrl_ctx, now, m->sni);
        freeaddrinfo(res);
        if (prc != 0) goto fail_configure;

        picowt_set_transport_parameters(m->cnx);
        if (picowt_connect(m->cnx, m->h3_ctx, m->ctrl_ctx, m->sni,
                           m->path, managed_client_wt_cb, m,
                           m->wt_protocols) != 0)
            goto fail_configure;
        /* Always install the client H3 callback: it tolerates a peer that omits
         * the reset_stream_at transport parameter (default WT interop; a no-op
         * for compliant peers) -- see allow_peer_missing_reset_stream_at. */
        picoquic_set_callback(m->cnx, managed_client_h3_callback, m);
        if (picoquic_start_client_cnx(m->cnx) != 0)
            goto fail_configure;
    }

    memset(&m->loop_param, 0, sizeof(m->loop_param));
    if (persp == MOQ_PERSPECTIVE_SERVER)
        m->loop_param.local_port = (uint16_t)port;
    /* The packet loop opens its sockets ON the network thread; a bind
     * failure there (e.g. the dual-stack ephemeral pairing losing its
     * v6 port to another process) kills the loop BEFORE the ready
     * callback with no error surfaced -- a zombie facade. Wait
     * (bounded) for the ready flag and retry on a startup death. */
    picoquic_network_thread_ctx_t *tctx = NULL;
    for (int attempt = 0; attempt < 3 && !tctx; attempt++) {
        pthread_mutex_lock(&m->mutex);
        m->loop_came_up = false;
        pthread_mutex_unlock(&m->mutex);
        int thread_ret = 0;
        picoquic_network_thread_ctx_t *cand =
            picoquic_start_network_thread(m->quic, &m->loop_param,
                loop_callback, m, &thread_ret);
        if (!cand) continue;
        /* Wait on OUR ready latch (a loop that came up and exited again
         * clears thread_is_ready -- see moq_picoquic_threaded.c). */
        bool came_up = false;
        for (int waited_ms = 0; waited_ms < 2000; waited_ms++) {
            pthread_mutex_lock(&m->mutex);
            came_up = m->loop_came_up;
            pthread_mutex_unlock(&m->mutex);
            if (came_up || cand->return_code != 0) break;
            usleep(1000);
        }
        if (came_up)
            tctx = cand;
        else
            picoquic_delete_network_thread(cand);
    }
    if (!tctx) goto fail_configure;

    pthread_mutex_lock(&m->mutex);
    m->thread_ctx = tctx;
    pthread_mutex_unlock(&m->mutex);

    *out = m;
    return MOQ_OK;

fail_configure:
    /* A client cnx whose setup got past picowt_prepare_client_cnx owns an
     * h3zero context (and its per-stream contexts); delete it before the cnx is
     * freed, so a failed connect does not leak it. NULL for the server path. */
    if (m->h3_ctx) {
        h3zero_callback_delete_context(m->cnx, m->h3_ctx);
        m->h3_ctx = NULL;
        m->ctrl_ctx = NULL;
    }
    picoquic_free(m->quic);
    m->quic = NULL;
fail_quic:
    pthread_cond_destroy(&m->cond);
    pthread_mutex_destroy(&m->mutex);
    free_strings(m);
    m->alloc.free(m, sizeof(*m), m->alloc.ctx);
    return MOQ_ERR_INTERNAL;
}

/* -- stop ----------------------------------------------------------- */

moq_result_t moq_pico_wt_managed_stop(moq_pico_wt_managed_t *m)
{
    if (!m) return MOQ_ERR_INVAL;
    pthread_mutex_lock(&m->mutex);
    if (m->network_thread_id_set &&
        pthread_equal(pthread_self(), m->network_thread_id)) {
        pthread_mutex_unlock(&m->mutex);
        return MOQ_ERR_WRONG_STATE;
    }
    if (m->stopped) {
        bool was_fatal = m->fatal;
        pthread_mutex_unlock(&m->mutex);
        return was_fatal ? MOQ_ERR_CLOSED : MOQ_OK;
    }
    picoquic_network_thread_ctx_t *ctx = m->thread_ctx;
    m->thread_ctx = NULL;
    m->stopped = true;
    m->activity_pending = true;
    pthread_cond_broadcast(&m->cond);
    while (m->wake_in_flight > 0)
        pthread_cond_wait(&m->cond, &m->mutex);
    pthread_mutex_unlock(&m->mutex);

    if (ctx)
        picoquic_delete_network_thread(ctx);

    pthread_mutex_lock(&m->mutex);
    bool was_fatal = m->fatal;
    pthread_mutex_unlock(&m->mutex);
    return was_fatal ? MOQ_ERR_CLOSED : MOQ_OK;
}

/* -- destroy -------------------------------------------------------- */

void moq_pico_wt_managed_destroy(moq_pico_wt_managed_t *m)
{
    if (!m) return;
    if (m->conn) moq_pico_wt_conn_destroy(m->conn);
    if (m->session) moq_session_destroy(m->session);
    /* The managed client owns the h3zero context returned by
     * picowt_prepare_client_cnx. conn_destroy only detaches/deregisters it
     * (attach mode does not own it), so the managed path deletes it here --
     * after the conn is torn down and before picoquic_free() frees the cnx it
     * references. This empties the per-stream h3zero context tree that ASan
     * flagged as leaked. The server path leaves m->h3_ctx NULL (picoquic owns
     * the server-side h3 contexts), so this is client-only. */
    if (m->h3_ctx) {
        h3zero_callback_delete_context(m->cnx, m->h3_ctx);
        m->h3_ctx = NULL;
        m->ctrl_ctx = NULL;
    }
    if (m->quic) picoquic_free(m->quic);
    pthread_cond_destroy(&m->cond);
    pthread_mutex_destroy(&m->mutex);
    free_strings(m);
    moq_alloc_t alloc = m->alloc;
    alloc.free(m, sizeof(*m), alloc.ctx);
}

/* -- accessors ------------------------------------------------------ */

moq_session_t *moq_pico_wt_managed_session(moq_pico_wt_managed_t *m)
{
    if (!m) return NULL;
    pthread_mutex_lock(&m->mutex);
    moq_session_t *s = m->session;
    pthread_mutex_unlock(&m->mutex);
    return s;
}

moq_version_t moq_pico_wt_managed_negotiated_version(
    const moq_pico_wt_managed_t *m)
{
    if (!m) return (moq_version_t)0;
    moq_pico_wt_managed_t *mm = (moq_pico_wt_managed_t *)m;
    pthread_mutex_lock(&mm->mutex);
    moq_version_t v = m->negotiated;
    pthread_mutex_unlock(&mm->mutex);
    return v;
}

static bool locked_bool(const moq_pico_wt_managed_t *m, size_t which)
{
    moq_pico_wt_managed_t *mm = (moq_pico_wt_managed_t *)m;
    pthread_mutex_lock(&mm->mutex);
    bool v = which == 0 ? mm->fatal : mm->closed;
    pthread_mutex_unlock(&mm->mutex);
    return v;
}

bool moq_pico_wt_managed_is_fatal(const moq_pico_wt_managed_t *m)
{
    return m ? locked_bool(m, 0) : false;
}
bool moq_pico_wt_managed_is_closed(const moq_pico_wt_managed_t *m)
{
    return m ? locked_bool(m, 1) : false;
}
/* Graceful-drain probe for moq_endpoint_drain()'s backend vtable. Returns
 * 1 = local stream flush done (no queued/ready reliable stream data or unsent
 * FIN; NOT a packet-ACK check), 0 = still draining, -2 = called on the network
 * thread (caller -> WRONG_STATE). The flushed state is computed on the network
 * thread (loop_callback) and read here under the mutex; this never touches
 * picoquic from the app thread. */
int moq_pico_wt_managed_drain_state(moq_pico_wt_managed_t *m)
{
    if (!m) return 0;
    pthread_mutex_lock(&m->mutex);
    if (m->network_thread_id_set &&
        pthread_equal(pthread_self(), m->network_thread_id)) {
        pthread_mutex_unlock(&m->mutex);
        return -2;
    }
    int drained = m->tx_drained ? 1 : 0;
    pthread_mutex_unlock(&m->mutex);
    return drained;
}
uint64_t moq_pico_wt_managed_fatal_code(const moq_pico_wt_managed_t *m)
{
    if (!m) return 0;
    moq_pico_wt_managed_t *mm = (moq_pico_wt_managed_t *)m;
    pthread_mutex_lock(&mm->mutex);
    uint64_t c = mm->fatal_code;
    pthread_mutex_unlock(&mm->mutex);
    return c;
}
uint64_t moq_pico_wt_managed_close_code(const moq_pico_wt_managed_t *m)
{
    if (!m) return 0;
    moq_pico_wt_managed_t *mm = (moq_pico_wt_managed_t *)m;
    pthread_mutex_lock(&mm->mutex);
    uint64_t c = mm->close_code;
    pthread_mutex_unlock(&mm->mutex);
    return c;
}

int moq_pico_wt_managed_local_port(const moq_pico_wt_managed_t *m)
{
    return m ? m->port : 0;  /* configured listen/target port */
}

/* -- wake ----------------------------------------------------------- */

moq_result_t moq_pico_wt_managed_wake(moq_pico_wt_managed_t *m)
{
    if (!m) return MOQ_ERR_INVAL;
    pthread_mutex_lock(&m->mutex);
    if (m->stopped || m->fatal || m->closed || m->pump_exit ||
        !m->thread_ctx) {
        pthread_mutex_unlock(&m->mutex);
        return MOQ_ERR_CLOSED;
    }
    if (m->wake_pending) {
        pthread_mutex_unlock(&m->mutex);
        return MOQ_OK;
    }
    m->wake_pending = true;
    m->wake_in_flight++;
    picoquic_network_thread_ctx_t *ctx = m->thread_ctx;
    pthread_mutex_unlock(&m->mutex);

    int rc = picoquic_wake_up_network_thread(ctx);

    pthread_mutex_lock(&m->mutex);
    m->wake_in_flight--;
    if (m->wake_in_flight == 0)
        pthread_cond_broadcast(&m->cond);
    if (rc != 0) {
        m->wake_pending = false;
        pthread_mutex_unlock(&m->mutex);
        return MOQ_ERR_INTERNAL;
    }
    pthread_mutex_unlock(&m->mutex);
    return MOQ_OK;
}

/* -- wait ----------------------------------------------------------- */

moq_result_t moq_pico_wt_managed_wait(moq_pico_wt_managed_t *m,
                                       uint64_t timeout_us)
{
    if (!m) return MOQ_ERR_INVAL;
    pthread_mutex_lock(&m->mutex);

    if (m->stopped || m->fatal || m->closed || m->pump_exit) {
        pthread_mutex_unlock(&m->mutex);
        return MOQ_ERR_CLOSED;
    }
    if (m->activity_pending) {
        m->activity_pending = false;
        pthread_mutex_unlock(&m->mutex);
        return MOQ_OK;
    }
    if (timeout_us == 0) {
        pthread_mutex_unlock(&m->mutex);
        return MOQ_DONE;
    }

    struct timespec deadline;
    bool has_deadline = (timeout_us != UINT64_MAX);
    if (has_deadline) {
        clock_gettime(CLOCK_REALTIME, &deadline);
        deadline.tv_sec  += (time_t)(timeout_us / 1000000ULL);
        deadline.tv_nsec += (long)((timeout_us % 1000000ULL) * 1000ULL);
        if (deadline.tv_nsec >= 1000000000L) {
            deadline.tv_sec++;
            deadline.tv_nsec -= 1000000000L;
        }
    }

    while (!m->activity_pending && !m->stopped &&
           !m->fatal && !m->closed && !m->pump_exit) {
        if (has_deadline) {
            if (pthread_cond_timedwait(&m->cond, &m->mutex,
                                       &deadline) == ETIMEDOUT)
                break;
        } else {
            pthread_cond_wait(&m->cond, &m->mutex);
        }
    }

    moq_result_t result;
    if (m->stopped || m->fatal || m->closed || m->pump_exit) {
        result = MOQ_ERR_CLOSED;
    } else if (m->activity_pending) {
        m->activity_pending = false;
        result = MOQ_OK;
    } else {
        result = MOQ_DONE;
    }
    pthread_mutex_unlock(&m->mutex);
    return result;
}
