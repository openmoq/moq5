/*
 * MoQ over wtquic: transport-bridge endpoint ops on wtquic's public
 * API, and wtquic session events fed into the bridge.
 *
 * Confinement: wtquic delivers a session's events serialized on its
 * transport worker; every bridge feed, service() and hook invocation
 * happens inside those callbacks, so the moq session and bridge stay
 * single-threaded without locks.
 *
 * Send ownership: the bridge's plain write is borrow-during-call, but
 * wtquic borrows span data until the send's completion — so cold
 * writes copy into an adapter record released at on_send_complete.
 * write_payload passes the rcbuf's bytes through zero-copy, holding an
 * incref until the completion (single-shard refcounts: the incref and
 * decref both happen on the transport worker).
 *
 * Inbound backpressure: a bridge feed returning WOULD_BLOCK pauses the
 * stream's receive (wtq_stream_pause_receive); once service() clears
 * the stream's pending state the receive is resumed. Bytes the bridge
 * holds pending are never re-delivered by us.
 */

#if defined(__linux__) && !defined(_POSIX_C_SOURCE)
#define _POSIX_C_SOURCE 200809L
#endif

#include <string.h>
#include <time.h>

#include <moq/rcbuf.h>
#include <moq/transport_bridge.h>
#include <moq/wtquic.h>

#define MOQ_WTQ_MAX_STREAMS 16 /* mirrors wtquic's session handle pool */

struct moq_wtq_stream {
    uint64_t id;
    wtq_stream_t *st;   /* valid until its terminal event */
    bool in_use;
    bool paused;        /* receive paused pending bridge retry */
};

/* One in-flight send: either a copied cold write or a retained rcbuf
 * whose bytes wtquic borrows until the completion. */
struct moq_wtq_send_rec {
    bool is_rcbuf;
    size_t alloc_size;
    moq_rcbuf_t *buf;   /* retained (rcbuf sends only) */
    /* copied cold bytes follow */
};

struct moq_wtquic_conn {
    moq_alloc_t alloc;
    moq_session_t *session;       /* not owned */
    moq_transport_bridge_t *bridge;
    wtq_session_t *ws;            /* bound at the first event */
    moq_wtquic_hook_fn hook;
    void *hook_user;
    struct moq_wtq_stream streams[MOQ_WTQ_MAX_STREAMS];
    /* Opaque transport-key allocator: every local AND peer stream key
     * comes from this one monotonic counter (bridge contract:
     * connection-lifetime unique, never UINT64_MAX). Native QUIC ids
     * are NEVER identity — wtq_stream_id() is diagnostics only, and may
     * legitimately stay WTQ_STREAM_ID_UNKNOWN on async-id transports. */
    uint64_t next_stream_key;
    uint64_t ctrl_id;             /* MoQ control stream (bidi profiles) */
    bool ctrl_latched;
    bool started;
    bool teardown_sent;           /* fatal-bridge teardown issued once */
    bool in_service;              /* conn_service is on the stack */
    bool service_pending;         /* re-run requested while in service */
    bool close_pending;           /* deferred transport-close feed */
    bool close_fed;
    bool close_clean;
    uint64_t close_code;
    /*
     * Deferred stream-terminal feed. A synchronous on_stream_closed can
     * fire from INSIDE an endpoint op (e.g. wtq_stream_abort -> immediate
     * terminal), and the bridge forbids inbound handlers being called
     * from an endpoint op. So ev_stream_closed only ENQUEUES the opaque
     * key here; conn_service drains it outside the endpoint-op/bridge-
     * service frame. Sized one per adapter stream slot -- the maximum
     * that can terminate before a drain -- plus headroom; overflow is
     * fatal, never a silent drop. */
    uint64_t term_keys[MOQ_WTQ_MAX_STREAMS];
    size_t   term_count;
    bool     term_overflow;
    moq_transport_endpoint_ops_t ops;
};

static uint64_t now_us(void)
{
    struct timespec ts;

    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000u + (uint64_t)ts.tv_nsec / 1000u;
}

/* --- stream table --------------------------------------------------------- */

/* stream_add outcome: distinguishes temporary table pressure (retryable)
 * from permanent key exhaustion (fatal). */
typedef enum {
    STREAM_ADD_OK = 0,
    STREAM_ADD_FULL,       /* table full now — retry after a slot frees */
    STREAM_ADD_EXHAUSTED,  /* key space exhausted — connection is over */
} stream_add_rc_t;

static stream_add_rc_t stream_add(moq_wtquic_conn_t *c, wtq_stream_t *st,
                                  struct moq_wtq_stream **out)
{
    *out = NULL;
    /* Key exhaustion fails CLOSED and is PERMANENT: the reserved
     * UINT64_MAX sentinel is never issued and a key is never reused. */
    if (c->next_stream_key == UINT64_MAX)
        return STREAM_ADD_EXHAUSTED;
    for (size_t i = 0; i < MOQ_WTQ_MAX_STREAMS; i++)
        if (!c->streams[i].in_use) {
            c->streams[i].in_use = true;
            c->streams[i].paused = false;
            c->streams[i].st = st;
            c->streams[i].id = c->next_stream_key++;
            *out = &c->streams[i];
            return STREAM_ADD_OK;
        }
    return STREAM_ADD_FULL; /* temporary: a terminal event frees a slot */
}

static struct moq_wtq_stream *stream_find(moq_wtquic_conn_t *c,
                                          uint64_t id)
{
    for (size_t i = 0; i < MOQ_WTQ_MAX_STREAMS; i++)
        if (c->streams[i].in_use && c->streams[i].id == id)
            return &c->streams[i];
    return NULL;
}

static struct moq_wtq_stream *stream_find_by_handle(moq_wtquic_conn_t *c,
                                                    wtq_stream_t *st)
{
    for (size_t i = 0; i < MOQ_WTQ_MAX_STREAMS; i++)
        if (c->streams[i].in_use && c->streams[i].st == st)
            return &c->streams[i];
    return NULL;
}

/* --- endpoint ops (bridge -> wtquic) --------------------------------------- */

static moq_transport_result_t map_open_rc(wtq_result_t rc)
{
    if (rc == WTQ_OK)
        return MOQ_TRANSPORT_OK;
    if (rc == WTQ_ERR_STREAM_LIMIT || rc == WTQ_ERR_WOULD_BLOCK)
        return MOQ_TRANSPORT_WOULD_BLOCK;
    return MOQ_TRANSPORT_ERROR;
}

static moq_transport_result_t ep_open(void *ctx, bool bidi,
                                      uint64_t *out_id)
{
    moq_wtquic_conn_t *c = ctx;
    wtq_stream_t *st = NULL;

    if (c->ws == NULL)
        return MOQ_TRANSPORT_ERROR;
    wtq_result_t rc = bidi ? wtq_session_open_bidi(c->ws, &st)
                           : wtq_session_open_uni(c->ws, &st);
    if (rc != WTQ_OK)
        return map_open_rc(rc);
    struct moq_wtq_stream *ms = NULL;
    switch (stream_add(c, st, &ms)) {
    case STREAM_ADD_OK:
        break;
    case STREAM_ADD_FULL:
        /* temporary table pressure: retry when a slot frees */
        (void)wtq_stream_abort(st, 0);
        return MOQ_TRANSPORT_WOULD_BLOCK;
    case STREAM_ADD_EXHAUSTED:
    default:
        /* permanent: fail the connection ONCE, never issue UINT64_MAX */
        (void)wtq_stream_abort(st, 0);
        return MOQ_TRANSPORT_ERROR;
    }
    *out_id = ms->id;
    /* bidi-control profiles: the first locally opened bidi carries the
     * MoQ control channel */
    if (bidi && !c->ctrl_latched &&
        !moq_transport_bridge_uses_uni_control(c->bridge) &&
        moq_session_perspective(c->session) == MOQ_PERSPECTIVE_CLIENT) {
        c->ctrl_id = *out_id;
        c->ctrl_latched = true;
    }
    return MOQ_TRANSPORT_OK;
}

static moq_transport_result_t ep_open_uni(void *ctx, uint64_t *out_id)
{
    return ep_open(ctx, false, out_id);
}

static moq_transport_result_t ep_open_bidi(void *ctx, uint64_t *out_id)
{
    return ep_open(ctx, true, out_id);
}

static moq_transport_result_t map_send_rc(wtq_result_t rc)
{
    if (rc == WTQ_OK)
        return MOQ_TRANSPORT_OK;
    if (rc == WTQ_ERR_WOULD_BLOCK)
        return MOQ_TRANSPORT_WOULD_BLOCK;
    return MOQ_TRANSPORT_ERROR;
}

static moq_transport_result_t ep_write(void *ctx, uint64_t stream_id,
                                       const uint8_t *data, size_t len,
                                       bool fin)
{
    moq_wtquic_conn_t *c = ctx;
    struct moq_wtq_stream *ms = stream_find(c, stream_id);

    if (ms == NULL)
        return MOQ_TRANSPORT_ERROR;
    if (len == 0)
        return map_send_rc(
            wtq_stream_send(ms->st, NULL, 0, fin ? WTQ_SEND_FIN : 0,
                            NULL));

    /* wtquic borrows the bytes until the completion; the bridge's
     * write contract releases them at return — copy into a record */
    size_t rec_size = sizeof(struct moq_wtq_send_rec) + len;
    struct moq_wtq_send_rec *rec =
        c->alloc.alloc(rec_size, c->alloc.ctx);
    if (rec == NULL)
        return MOQ_TRANSPORT_ERROR;
    rec->is_rcbuf = false;
    rec->alloc_size = rec_size;
    rec->buf = NULL;
    memcpy(rec + 1, data, len);

    wtq_span_t span = { (const uint8_t *)(rec + 1), len };
    wtq_result_t rc =
        wtq_stream_send(ms->st, &span, 1, fin ? WTQ_SEND_FIN : 0, rec);
    if (rc != WTQ_OK)
        c->alloc.free(rec, rec_size, c->alloc.ctx);
    return map_send_rc(rc);
}

static moq_transport_result_t ep_write_payload(void *ctx,
                                               uint64_t stream_id,
                                               moq_rcbuf_t *buf, bool fin)
{
    moq_wtquic_conn_t *c = ctx;
    struct moq_wtq_stream *ms = stream_find(c, stream_id);

    if (ms == NULL)
        return MOQ_TRANSPORT_ERROR;

    struct moq_wtq_send_rec *rec =
        c->alloc.alloc(sizeof(*rec), c->alloc.ctx);
    if (rec == NULL)
        return MOQ_TRANSPORT_ERROR;
    rec->is_rcbuf = true;
    rec->alloc_size = sizeof(*rec);
    rec->buf = buf;
    moq_rcbuf_incref(buf); /* held until the send completion */

    wtq_span_t span = { moq_rcbuf_data(buf), moq_rcbuf_len(buf) };
    wtq_result_t rc =
        wtq_stream_send(ms->st, &span, 1, fin ? WTQ_SEND_FIN : 0, rec);
    if (rc != WTQ_OK) {
        moq_rcbuf_decref(buf);
        c->alloc.free(rec, sizeof(*rec), c->alloc.ctx);
    }
    return map_send_rc(rc);
}

static moq_transport_result_t ep_reset_stream(void *ctx,
                                              uint64_t stream_id,
                                              uint64_t error_code)
{
    moq_wtquic_conn_t *c = ctx;
    struct moq_wtq_stream *ms = stream_find(c, stream_id);

    if (ms == NULL)
        return MOQ_TRANSPORT_OK; /* already gone: idempotent */
    wtq_result_t rc = wtq_stream_reset(ms->st, (uint32_t)error_code);
    if (rc == WTQ_OK || rc == WTQ_ERR_STATE || rc == WTQ_ERR_CLOSED)
        return MOQ_TRANSPORT_OK;
    return MOQ_TRANSPORT_ERROR;
}

static moq_transport_result_t ep_abort_stream(void *ctx,
                                              uint64_t stream_id,
                                              uint64_t error_code)
{
    moq_wtquic_conn_t *c = ctx;
    struct moq_wtq_stream *ms = stream_find(c, stream_id);

    if (ms == NULL)
        return MOQ_TRANSPORT_OK; /* already gone: idempotent */
    wtq_result_t rc = wtq_stream_abort(ms->st, (uint32_t)error_code);
    if (rc == WTQ_OK || rc == WTQ_ERR_STATE || rc == WTQ_ERR_CLOSED)
        return MOQ_TRANSPORT_OK;
    return MOQ_TRANSPORT_ERROR;
}

static moq_transport_result_t ep_stop_sending(void *ctx,
                                              uint64_t stream_id,
                                              uint64_t error_code)
{
    moq_wtquic_conn_t *c = ctx;
    struct moq_wtq_stream *ms = stream_find(c, stream_id);

    if (ms == NULL)
        return MOQ_TRANSPORT_OK;
    wtq_result_t rc =
        wtq_stream_stop_sending(ms->st, (uint32_t)error_code);
    if (rc == WTQ_OK || rc == WTQ_ERR_STATE || rc == WTQ_ERR_CLOSED)
        return MOQ_TRANSPORT_OK;
    return MOQ_TRANSPORT_ERROR;
}

static moq_transport_result_t ep_close_transport(void *ctx, uint64_t code,
                                                 const uint8_t *reason,
                                                 size_t reason_len)
{
    moq_wtquic_conn_t *c = ctx;

    if (c->ws == NULL)
        return MOQ_TRANSPORT_ERROR;
    wtq_result_t rc =
        wtq_session_close(c->ws, (uint32_t)code, reason, reason_len);
    if (rc == WTQ_OK || rc == WTQ_ERR_CLOSED || rc == WTQ_ERR_STATE)
        return MOQ_TRANSPORT_OK;
    return MOQ_TRANSPORT_ERROR;
}

/* --- servicing ------------------------------------------------------------- */

/*
 * Feed happened: drive the bridge, give the app its slot, drive again
 * for whatever the app queued. A fatal bridge must not leave the
 * transport running: tear the WebTransport session down once, with the
 * fatal code (harmless when the session is already terminal).
 *
 * Re-entrancy guard: wtquic delivers some events SYNCHRONOUSLY from
 * inside its own entry points (a wtq_session_close made by the
 * bridge's close_transport op — i.e. from inside service() — raises
 * on_closed before returning), and the bridge is non-reentrant. Any
 * conn_service reached while one is already on the stack only flags a
 * re-run; the transport-close feed itself is deferred the same way and
 * fed exactly once, outside any endpoint op.
 */
/* Drain queued stream terminals into the bridge. Called ONLY from
 * conn_service, outside any endpoint-op/bridge-service frame. */
static void conn_feed_terminals(moq_wtquic_conn_t *c)
{
    if (c->term_overflow) {
        /* a terminal was lost: fail the connection rather than proceed
         * with a stale aborting mapping the bridge can never retire */
        c->term_overflow = false;
        (void)moq_transport_bridge_on_transport_error(c->bridge, 0x1,
                                                      now_us());
        return;
    }
    while (c->term_count > 0) {
        /* FIFO; take a snapshot count so terminals enqueued by a
         * reentrant callback during the bridge call are handled on the
         * next pass, never lost */
        size_t n = c->term_count;
        uint64_t key = c->term_keys[0];
        for (size_t i = 1; i < n; i++)
            c->term_keys[i - 1] = c->term_keys[i];
        c->term_count = n - 1;
        (void)moq_transport_bridge_on_peer_stream_terminal(c->bridge, key,
                                                           now_us());
    }
}

static void conn_feed_close(moq_wtquic_conn_t *c)
{
    if (!c->close_pending || c->close_fed)
        return;
    c->close_fed = true;
    if (c->close_clean)
        (void)moq_transport_bridge_on_transport_close(
            c->bridge, c->close_code, now_us());
    else
        (void)moq_transport_bridge_on_transport_error(
            c->bridge, c->close_code, now_us());
}

/* Suppress delivery on a stream whose bridge input has gone pending.
 *
 * `paused` is published ONLY on a CONFIRMED suppression: marking it before the
 * call would leave the adapter believing delivery is suppressed while data still
 * flows into an already-pending bridge. WTQ_ERR_STATE / WTQ_ERR_CLOSED mean the
 * receive direction is already finished — nothing to suppress, and nothing for
 * the resume scan to reconcile later, so the stream stays unpaused without an
 * error. Any other failure means the adapter cannot honor its inbound-
 * backpressure contract on this stream: surface a transport error rather than
 * silently keep feeding a pending bridge. (WTQ_ERR_UNSUPPORTED lands here: a
 * backend that cannot pause at all cannot support this contract, and failing
 * loudly beats unbounded buffering below the bridge.) */
static void pause_stream_for_backpressure(moq_wtquic_conn_t *c,
                                          struct moq_wtq_stream *ms,
                                          wtq_stream_t *st)
{
    if (ms->paused)
        return;
    wtq_result_t rc = wtq_stream_pause_receive(st);
    if (rc == WTQ_OK)
        ms->paused = true;
    else if (rc != WTQ_ERR_STATE && rc != WTQ_ERR_CLOSED)
        (void)moq_transport_bridge_on_transport_error(c->bridge, 0x2, now_us());
}

/* Paused-stream reconciliation. A stream paused in ev_stream_data because its
 * bridge input went pending stays paused until the bridge input clears — which
 * happens when the app later drains the session's event queue and a subsequent
 * service() re-feeds and retires the pending. That later service does NOT arrive
 * on the stream's own receive path (it is paused, so no on_stream_data fires),
 * so the resume MUST happen here, on the service path, or the stream is stranded
 * paused forever. Scan active paused streams; resume any whose
 * bridge input has cleared. Clear paused strictly per the resume result — never
 * silently strand a failed resume. */
static void resume_paused_streams(moq_wtquic_conn_t *c)
{
    for (uint32_t i = 0; i < MOQ_WTQ_MAX_STREAMS; i++) {
        struct moq_wtq_stream *ms = &c->streams[i];
        if (!ms->in_use || !ms->paused || ms->st == NULL)
            continue;
        if (moq_transport_bridge_stream_has_pending(c->bridge, ms->id))
            continue; /* still backed up on this stream — keep it paused */
        wtq_result_t rc = wtq_stream_resume_receive(ms->st);
        if (rc == WTQ_OK || rc == WTQ_ERR_STATE || rc == WTQ_ERR_CLOSED) {
            /* resumed, or its incoming direction is already finished/closed
             * (nothing left to strand) — the pause is genuinely cleared */
            ms->paused = false;
        } else {
            /* an UNEXPECTED resume failure: do not silently strand the stream.
             * Surface a transport error so conn_service tears the conn down
             * this same pass (paused stays true so no phantom re-attempt), and
             * STOP the scan — the bridge is fatal now, so no further transport
             * operation may be issued from this pass. */
            (void)moq_transport_bridge_on_transport_error(c->bridge, 0x2,
                                                          now_us());
            return;
        }
    }
}

static void conn_service(moq_wtquic_conn_t *c)
{
    if (c->in_service) {
        c->service_pending = true;
        return;
    }
    c->in_service = true;
    do {
        c->service_pending = false;
        conn_feed_close(c);
        conn_feed_terminals(c);
        (void)moq_transport_bridge_service(c->bridge, now_us());
        if (c->hook != NULL) {
            c->hook(c, c->hook_user);
            /* the hook may have closed the WebTransport session (a
             * synchronous on_closed lands as close_pending); the
             * bridge must learn of the terminal before it services
             * anything the hook queued, or those sends would hit the
             * closed session and read as fatal */
            conn_feed_close(c);
            conn_feed_terminals(c);
            (void)moq_transport_bridge_service(c->bridge, now_us());
        }
        /* After servicing (which retires cleared bridge input), resume any
         * stream whose pending has drained — otherwise a pause that clears on
         * THIS service (not on the stream's own receive callback) strands it. */
        resume_paused_streams(c);
        if (!c->teardown_sent && c->ws != NULL &&
            moq_transport_bridge_is_fatal(c->bridge)) {
            c->teardown_sent = true;
            (void)wtq_session_close(
                c->ws,
                (uint32_t)moq_transport_bridge_fatal_code(c->bridge),
                NULL, 0);
        }
    } while (c->service_pending || (c->close_pending && !c->close_fed) ||
             c->term_count > 0 || c->term_overflow);
    c->in_service = false;
}

void moq_wtquic_conn_service(moq_wtquic_conn_t *conn)
{
    if (conn != NULL)
        conn_service(conn);
}

/* --- wtquic events (wtquic -> bridge) --------------------------------------- */

static void ev_established(wtq_session_t *s, wtq_str_t sub, void *user)
{
    moq_wtquic_conn_t *c = user;

    (void)sub;
    c->ws = s;
    if (!c->started) {
        c->started = true;
        /* symmetric-start profiles start both sides; bidi-control
         * starts the client only (the server is reactive) */
        if (moq_session_perspective(c->session) ==
                MOQ_PERSPECTIVE_CLIENT ||
            moq_transport_bridge_uses_uni_control(c->bridge))
            (void)moq_session_start(c->session, now_us());
    }
    conn_service(c);
}

static void ev_refused(wtq_session_t *s, uint16_t status, void *user)
{
    moq_wtquic_conn_t *c = user;

    c->ws = s;
    (void)moq_transport_bridge_on_transport_error(c->bridge, status,
                                                  now_us());
    conn_service(c);
}

static void ev_failed(wtq_session_t *s, wtq_connect_failure_t why,
                      void *user)
{
    moq_wtquic_conn_t *c = user;

    c->ws = s;
    (void)moq_transport_bridge_on_transport_error(c->bridge,
                                                  (uint64_t)why,
                                                  now_us());
    conn_service(c);
}

static void ev_closed(wtq_session_t *s, uint32_t code,
                      const uint8_t *reason, size_t rlen, bool clean,
                      void *user)
{
    moq_wtquic_conn_t *c = user;

    (void)reason;
    (void)rlen;
    c->ws = s;
    /* may fire synchronously from inside a wtq call the bridge made:
     * record the terminal and let conn_service feed it exactly once */
    if (!c->close_pending) {
        c->close_pending = true;
        c->close_clean = clean;
        c->close_code = code;
    }
    conn_service(c);
}

static void ev_stream_opened(wtq_session_t *s, wtq_stream_t *st,
                             bool bidi, void *user)
{
    moq_wtquic_conn_t *c = user;

    c->ws = s;
    struct moq_wtq_stream *ms = NULL;
    switch (stream_add(c, st, &ms)) {
    case STREAM_ADD_OK:
        break;
    case STREAM_ADD_FULL:
        (void)wtq_stream_abort(st, 0);
        return;
    case STREAM_ADD_EXHAUSTED:
    default:
        /* Key exhaustion is a local INVARIANT failure: on_transport_error
         * marks the bridge FATAL (not cleanly closed), so conn_service()
         * takes its fatal-teardown branch and issues exactly one
         * wtq_session_close(). Repeated callbacks find is_fatal already
         * set (teardown_sent latched) and cannot initiate another close. */
        (void)wtq_stream_abort(st, 0);
        (void)moq_transport_bridge_on_transport_error(c->bridge, 0x1, now_us());
        conn_service(c);
        return;
    }
    /* bidi-control profiles: the server's control channel is the first
     * peer-opened bidi */
    if (bidi && !c->ctrl_latched &&
        !moq_transport_bridge_uses_uni_control(c->bridge) &&
        moq_session_perspective(c->session) == MOQ_PERSPECTIVE_SERVER) {
        c->ctrl_id = ms->id;
        c->ctrl_latched = true;
    }
}

static void ev_stream_data(wtq_session_t *s, wtq_stream_t *st,
                           const uint8_t *data, size_t len, bool fin,
                           void *user)
{
    moq_wtquic_conn_t *c = user;
    struct moq_wtq_stream *ms = stream_find_by_handle(c, st);

    c->ws = s;
    if (ms == NULL)
        return;

    moq_result_t rc;
    if (c->ctrl_latched && ms->id == c->ctrl_id)
        rc = moq_transport_bridge_on_peer_control_bytes(
            c->bridge, ms->id, data, len, fin, now_us());
    else if (wtq_stream_is_bidi(st))
        rc = moq_transport_bridge_on_peer_bidi_bytes(
            c->bridge, ms->id, data, len, fin, now_us());
    else
        rc = moq_transport_bridge_on_peer_uni_bytes(
            c->bridge, ms->id, data, len, fin, now_us());
    (void)rc;

    /* inbound backpressure: while the bridge holds this stream's bytes
     * pending, no more may be delivered — pause and let service() clear.
     * conn_service() below reconciles the resume (resume_paused_streams), so
     * a pause that clears on a LATER service call — not this receive — is not
     * stranded. */
    if (moq_transport_bridge_stream_has_pending(c->bridge, ms->id))
        pause_stream_for_backpressure(c, ms, st);
    conn_service(c);
}

static void ev_stream_reset(wtq_session_t *s, wtq_stream_t *st,
                            uint32_t code, void *user)
{
    moq_wtquic_conn_t *c = user;
    struct moq_wtq_stream *ms = stream_find_by_handle(c, st);

    c->ws = s;
    if (ms == NULL)
        return;
    if (c->ctrl_latched && ms->id == c->ctrl_id) {
        /* A reset of the MoQ control stream is part of normal session
         * teardown when the peer is closing (its close message rides a
         * different stream and reset frames overtake stream data), so
         * the verdict is deferred to the WebTransport session terminal:
         * a clean close stays clean, anything else reports the error.
         * A peer that resets the control stream and then keeps the
         * session open resolves through that terminal too, via its
         * eventual (unclean) transport close. */
        return;
    }
    (void)moq_transport_bridge_on_peer_stream_reset(c->bridge, ms->id,
                                                    code, now_us());
    conn_service(c);
}

static void ev_stream_stop(wtq_session_t *s, wtq_stream_t *st,
                           uint32_t code, void *user)
{
    moq_wtquic_conn_t *c = user;
    struct moq_wtq_stream *ms = stream_find_by_handle(c, st);

    c->ws = s;
    if (ms == NULL)
        return;
    (void)moq_transport_bridge_on_peer_stop_sending(c->bridge, ms->id,
                                                    code, now_us());
    conn_service(c);
}

static void ev_stream_closed(wtq_session_t *s, wtq_stream_t *st,
                             void *user)
{
    moq_wtquic_conn_t *c = user;
    struct moq_wtq_stream *ms = stream_find_by_handle(c, st);

    (void)s;
    if (ms != NULL) {
        /* ENQUEUE the backend-neutral terminal; NEVER feed the bridge
         * here -- this may run synchronously from inside an endpoint op
         * (wtq_stream_abort -> immediate on_stream_closed), and inbound
         * handlers must not be called from an endpoint op. conn_service
         * drains it later, outside that frame. */
        if (c->term_count < MOQ_WTQ_MAX_STREAMS)
            c->term_keys[c->term_count++] = ms->id;
        else
            c->term_overflow = true; /* fatal on drain; never dropped */
        memset(ms, 0, sizeof(*ms)); /* free the adapter slot */
    }
    /* a freed stream slot may unblock a pending open (re-entrant-safe:
     * conn_service latches in_service and defers) */
    conn_service(c);
}

static void ev_send_complete(wtq_session_t *s, void *send_ctx,
                             bool canceled, void *user)
{
    moq_wtquic_conn_t *c = user;
    struct moq_wtq_send_rec *rec = send_ctx;

    (void)s;
    (void)canceled;
    if (rec != NULL) {
        if (rec->is_rcbuf)
            moq_rcbuf_decref(rec->buf);
        c->alloc.free(rec, rec->alloc_size, c->alloc.ctx);
    }
    /* a completion frees send budget: retry anything the bridge held
     * and give the application its pacing slot */
    conn_service(c);
}

static void ev_stream_writable(wtq_session_t *s, wtq_stream_t *st,
                               void *user)
{
    moq_wtquic_conn_t *c = user;

    (void)s;
    (void)st;
    /* the transport says a refused send would fit now — budget
     * released or its buffering advice grew. The completion path
     * above retries too, but this edge is the only prompt signal when
     * the peer is fully caught up and the pending completion is
     * parked behind its delayed-ACK timer. */
    conn_service(c);
}

static void ev_datagram(wtq_session_t *s, const uint8_t *data, size_t len,
                        void *user)
{
    moq_wtquic_conn_t *c = user;

    c->ws = s;
    (void)moq_transport_bridge_on_peer_datagram(c->bridge, data, len,
                                                now_us());
    conn_service(c);
}

/* --- public surface --------------------------------------------------------- */

/* Immutable: concurrent first calls from different transports (e.g. a
 * listener thread and a Network.framework domain) must never observe a
 * half-written table, so there is no lazy initialization here. */
static const wtq_session_events_t adapter_event_table = {
    .struct_size = (uint32_t)sizeof(wtq_session_events_t),
    .on_established = ev_established,
    .on_refused = ev_refused,
    .on_failed = ev_failed,
    .on_closed = ev_closed,
    .on_stream_opened = ev_stream_opened,
    .on_stream_data = ev_stream_data,
    .on_stream_reset = ev_stream_reset,
    .on_stream_stop = ev_stream_stop,
    .on_stream_closed = ev_stream_closed,
    .on_send_complete = ev_send_complete,
    .on_datagram = ev_datagram,
    .on_stream_writable = ev_stream_writable,
};

const wtq_session_events_t *moq_wtquic_conn_events(void)
{
    return &adapter_event_table;
}

void moq_wtquic_conn_cfg_init_sized(moq_wtquic_conn_cfg_t *cfg,
                                    size_t size)
{
    if (cfg == NULL || size < sizeof(*cfg))
        return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = (uint32_t)sizeof(*cfg);
}

moq_result_t moq_wtquic_conn_create(const moq_wtquic_conn_cfg_t *cfg,
                                    moq_wtquic_conn_t **out)
{
    if (out == NULL)
        return MOQ_ERR_INVAL;
    *out = NULL;
    /* the size guard comes first: a too-small struct_size means the
     * caller's struct may not even contain the fields read below */
    if (cfg == NULL || cfg->struct_size < sizeof(*cfg))
        return MOQ_ERR_INVAL;
    if (cfg->alloc == NULL || cfg->session == NULL)
        return MOQ_ERR_INVAL;
    if (cfg->alloc->alloc == NULL || cfg->alloc->realloc == NULL ||
        cfg->alloc->free == NULL)
        return MOQ_ERR_INVAL;

    moq_wtquic_conn_t *c =
        cfg->alloc->alloc(sizeof(*c), cfg->alloc->ctx);
    if (c == NULL)
        return MOQ_ERR_NOMEM;
    memset(c, 0, sizeof(*c));
    c->next_stream_key = 1; /* 0 unused; UINT64_MAX reserved */
    c->alloc = *cfg->alloc;
    c->session = cfg->session;
    c->hook = cfg->hook;
    c->hook_user = cfg->hook_user;
    c->ctrl_id = UINT64_MAX;

    c->ops = (moq_transport_endpoint_ops_t){
        .struct_size = sizeof(moq_transport_endpoint_ops_t),
        .capabilities = MOQ_TRANSPORT_CAP_WRITE_PAYLOAD,
        .open_uni = ep_open_uni,
        .open_bidi = ep_open_bidi,
        .write = ep_write,
        .write_payload = ep_write_payload,
        .reset_stream = ep_reset_stream,
        .stop_sending = ep_stop_sending,
        .abort_stream = ep_abort_stream,
        .close_transport = ep_close_transport,
    };

    moq_transport_bridge_cfg_t bcfg;
    moq_transport_bridge_cfg_init(&bcfg, &c->alloc);
    moq_result_t rc = moq_transport_bridge_create(&bcfg, cfg->session,
                                                  &c->ops, c, &c->bridge);
    if (rc != MOQ_OK) {
        c->alloc.free(c, sizeof(*c), c->alloc.ctx);
        return rc;
    }
    *out = c;
    return MOQ_OK;
}

void moq_wtquic_conn_destroy(moq_wtquic_conn_t *c)
{
    if (c == NULL)
        return;
    moq_alloc_t alloc = c->alloc;

    moq_transport_bridge_destroy(c->bridge);
    alloc.free(c, sizeof(*c), alloc.ctx);
}

moq_session_t *moq_wtquic_conn_session(moq_wtquic_conn_t *c)
{
    return c != NULL ? c->session : NULL;
}

wtq_session_t *moq_wtquic_conn_wtq_session(moq_wtquic_conn_t *c)
{
    return c != NULL ? c->ws : NULL;
}

bool moq_wtquic_conn_is_fatal(const moq_wtquic_conn_t *c)
{
    return c != NULL && moq_transport_bridge_is_fatal(c->bridge);
}

bool moq_wtquic_conn_is_closed(const moq_wtquic_conn_t *c)
{
    return c != NULL && moq_transport_bridge_is_closed(c->bridge);
}

uint64_t moq_wtquic_conn_event_progress(const moq_wtquic_conn_t *c,
                                        bool *out_has_events)
{
    if (out_has_events)
        *out_has_events = (c != NULL) &&
                          moq_transport_bridge_has_events(c->bridge);
    return c != NULL ? moq_transport_bridge_event_progress_token(c->bridge) : 0;
}

bool moq_wtquic_conn_terminal_facts(const moq_wtquic_conn_t *c,
                                     bool *out_observed)
{
    /* Straight pass-through of the session's two facts, read together by the
     * bridge query so the facade never sees two instants -- and never the
     * bridge pointer. */
    if (c == NULL) {
        if (out_observed) *out_observed = false;
        return false;
    }
    return moq_transport_bridge_terminal_facts(c->bridge, out_observed);
}
