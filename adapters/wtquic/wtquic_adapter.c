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

#include <stdint.h>
#include <string.h>
#include <time.h>

#include <moq/rcbuf.h>
#include <moq/transport_bridge.h>
#include <moq/wtquic.h>

#include "wtquic_adapter_internal.h"
#include "../../core/src/session/session_outbound_internal.h"

#define MOQ_WTQ_RECEIVE_QUANTUM 65535u
/* The provider's accepted stream-plan bounds (wtquic_msquic.h): native peer
 * grants U+B <= 1024 and local reservations L <= 1024, so api_capacity =
 * U+B+L <= 2048. A snapshot outside them is refused, never truncated. */
#define MOQ_WTQ_MAX_PEER_STREAMS 1024u
#define MOQ_WTQ_MAX_LOCAL_STREAMS 1024u

typedef struct {
    uint32_t struct_size;
    const moq_alloc_t *alloc;
    moq_session_t *session;
    moq_wtquic_hook_fn hook;
    void *hook_user;
} moq_wtquic_conn_cfg_v0_t;
_Static_assert(offsetof(moq_wtquic_conn_cfg_t, wt_session) ==
               sizeof(moq_wtquic_conn_cfg_v0_t), "binding follows frozen prefix");
_Static_assert(sizeof(moq_wtquic_conn_cfg_v0_t) ==
               (sizeof(void *) == 8 ? 40 : 20), "LP64/ILP32 config floor");

struct moq_wtq_stream {
    uint64_t id;
    wtq_stream_t *st;   /* valid until its terminal event */
    wtq_stream_t *lease; /* retained through terminal held-input debt */
    bool in_use;
    bool paused;        /* receive paused pending bridge retry */
    bool bidi;
    bool held;
    bool held_fin;
    bool receive_reset;
    bool commit_enabled;
    bool fin_submitting;
    bool fin_sent;
    bool committed;
    bool send_cancelled;
    bool stop_seen;
    unsigned pending_events;
    unsigned borrowed;
    uint64_t replay_epoch;
    size_t held_len;
    uint8_t held_bytes[MOQ_WTQ_RECEIVE_QUANTUM];
};

typedef enum {
    STREAM_EVENT_CLOSED,
    STREAM_EVENT_COMMITTED,
    STREAM_EVENT_STOP,
} stream_event_kind_t;

struct moq_wtq_stream_event {
    uint64_t key;
    uint32_t code;
    stream_event_kind_t kind;
};

/* One in-flight send: either a copied cold write or a retained rcbuf
 * whose bytes wtquic borrows until the completion. */
struct moq_wtq_send_rec {
    struct moq_wtq_send_rec *next;
    struct moq_wtq_send_rec **prev_next;
    bool is_rcbuf;
    size_t alloc_size;
    moq_rcbuf_t *buf;   /* retained (rcbuf sends only) */
    /* copied cold bytes follow */
};

struct moq_wtquic_conn {
    moq_alloc_t alloc;
    moq_session_t *session;       /* not owned */
    moq_transport_bridge_t *bridge;
    wtq_session_t *ws;            /* immutable qualified creation binding */
    struct moq_wtq_send_rec *sends; /* actual retained adapter send ownership */
    moq_wtquic_hook_fn hook;
    void *hook_user;
    /* api_capacity entries, allocated with the conn at create and never
     * resized: no stream callback allocates. */
    struct moq_wtq_stream *streams;
    size_t stream_capacity;
    size_t alloc_size;            /* the conn's single allocation */
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
    bool stream_close_context;    /* observed close in this/deferred service */
    bool close_pending;           /* deferred transport-close feed */
    bool close_fed;
    bool close_clean;
    uint64_t close_code;
    /* A slot remains retained until its queued callbacks drain. Each slot
     * contributes at most one send outcome (commit or STOP) and one close;
     * duplicate outcomes are latched before enqueue. Endpoint operations may
     * produce these callbacks synchronously, so bridge feeds are deferred. */
    struct moq_wtq_stream_event *term_events; /* 2 * stream_capacity */
    size_t   term_capacity;
    size_t   term_count;
    bool     term_overflow;
    moq_transport_endpoint_ops_t ops;
    size_t receive_quantum;
    unsigned feed_depth;
    uint64_t service_epoch;
    bool misrouted;
};

/* The stream table and the terminal queue trail the conn in one allocation. */
_Static_assert(sizeof(moq_wtquic_conn_t) % _Alignof(struct moq_wtq_stream) == 0,
               "stream table follows the conn aligned");
_Static_assert(sizeof(struct moq_wtq_stream) %
                   _Alignof(struct moq_wtq_stream_event) == 0,
               "terminal queue follows the stream table aligned");

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
    for (size_t i = 0; i < c->stream_capacity; i++)
        if (!c->streams[i].in_use) {
            c->streams[i].in_use = true;
            c->streams[i].paused = false;
            c->streams[i].st = st;
            c->streams[i].lease = st;
            if (st) wtq_stream_add_ref(st);
            c->streams[i].bidi = st ? wtq_stream_is_bidi(st) : false;
            c->streams[i].id = c->next_stream_key++;
            *out = &c->streams[i];
            return STREAM_ADD_OK;
        }
    return STREAM_ADD_FULL; /* temporary: a terminal event frees a slot */
}

static struct moq_wtq_stream *stream_find(moq_wtquic_conn_t *c,
                                          uint64_t id)
{
    for (size_t i = 0; i < c->stream_capacity; i++)
        if (c->streams[i].in_use && c->streams[i].id == id)
            return &c->streams[i];
    return NULL;
}

static struct moq_wtq_stream *stream_find_by_handle(moq_wtquic_conn_t *c,
                                                    wtq_stream_t *st)
{
    for (size_t i = 0; i < c->stream_capacity; i++)
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
    struct moq_wtq_stream *ms = NULL;
    stream_add_rc_t reserved = stream_add(c, NULL, &ms);
    if (reserved != STREAM_ADD_OK)
        return reserved == STREAM_ADD_FULL ? MOQ_TRANSPORT_WOULD_BLOCK : MOQ_TRANSPORT_ERROR;
    wtq_result_t rc = bidi ? wtq_session_open_bidi(c->ws, &st)
                           : wtq_session_open_uni(c->ws, &st);
    if (rc != WTQ_OK) {
        memset(ms, 0, sizeof(*ms));
        /* WT closes streams before reporting the session outcome. A retry
         * caused by that close must leave the verdict to the session callback.
         * CLOSED outside observed terminal work is still an error. */
        if (rc == WTQ_ERR_CLOSED && !c->close_fed &&
            (c->close_pending || c->stream_close_context))
            return MOQ_TRANSPORT_WOULD_BLOCK;
        return map_open_rc(rc);
    }
    ms->st = ms->lease = st;
    ms->bidi = bidi;
    wtq_stream_add_ref(st);
    if (!bidi && (c->ops.capabilities & MOQ_TRANSPORT_CAP_COMMIT_REPORT)) {
        c->feed_depth++;
        rc = wtq_stream_enable_send_commit(st);
        if (rc == WTQ_OK)
            ms->commit_enabled = true;
        else
            (void)wtq_stream_abort(st, 0);
        c->feed_depth--;
        if (rc != WTQ_OK)
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

/* Publish before the provider call: completion may return the borrow
 * synchronously. On refusal the caller withdraws the same record. */
static void send_rec_link(moq_wtquic_conn_t *c, struct moq_wtq_send_rec *rec)
{
    rec->next = c->sends;
    rec->prev_next = &c->sends;
    if (rec->next) rec->next->prev_next = &rec->next;
    c->sends = rec;
}

static void send_rec_unlink(struct moq_wtq_send_rec *rec)
{
    *rec->prev_next = rec->next;
    if (rec->next) rec->next->prev_next = rec->prev_next;
}

static wtq_result_t stream_send(moq_wtquic_conn_t *c,
                                struct moq_wtq_stream *ms,
                                const wtq_span_t *spans, size_t count,
                                bool fin, void *send_ctx)
{
    if (!ms->st) return WTQ_ERR_CLOSED;
    c->feed_depth++;
    ms->borrowed++;
    ms->fin_submitting = fin;
    wtq_result_t rc = wtq_stream_send(ms->st, spans, count,
                                     fin ? WTQ_SEND_FIN : 0, send_ctx);
    if (rc == WTQ_OK && fin) ms->fin_sent = true;
    ms->fin_submitting = false;
    ms->borrowed--;
    c->feed_depth--;
    return rc;
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
        return map_send_rc(stream_send(c, ms, NULL, 0, fin, NULL));

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

    wtq_span_t span = {.data = (const uint8_t *)(rec + 1), .len = len};
    send_rec_link(c, rec);
    wtq_result_t rc = stream_send(c, ms, &span, 1, fin, rec);
    if (rc != WTQ_OK) {
        send_rec_unlink(rec);
        c->alloc.free(rec, rec_size, c->alloc.ctx);
    }
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

    wtq_span_t span = {.data = moq_rcbuf_data(buf), .len = moq_rcbuf_len(buf)};
    send_rec_link(c, rec);
    wtq_result_t rc = stream_send(c, ms, &span, 1, fin, rec);
    if (rc != WTQ_OK) {
        send_rec_unlink(rec);
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

    if (ms == NULL || ms->st == NULL || ms->committed)
        return MOQ_TRANSPORT_OK; /* observed terminal: idempotent, not a reset */
    if (ms->stop_seen && ms->commit_enabled)
        return MOQ_TRANSPORT_WOULD_BLOCK; /* await the causal closed callback */
    c->feed_depth++;
    ms->borrowed++;
    wtq_result_t rc = wtq_stream_reset(ms->st, (uint32_t)error_code);
    if (rc == WTQ_OK) ms->send_cancelled = true;
    ms->borrowed--;
    c->feed_depth--;
    return map_send_rc(rc);
}

static moq_transport_result_t ep_abort_stream(void *ctx,
                                              uint64_t stream_id,
                                              uint64_t error_code)
{
    moq_wtquic_conn_t *c = ctx;
    struct moq_wtq_stream *ms = stream_find(c, stream_id);

    if (ms == NULL || ms->st == NULL)
        return MOQ_TRANSPORT_OK; /* an actual closed callback retired it */
    if (ms->stop_seen && ms->commit_enabled)
        return MOQ_TRANSPORT_WOULD_BLOCK;
    c->feed_depth++;
    ms->borrowed++;
    wtq_result_t rc = wtq_stream_abort(ms->st, (uint32_t)error_code);
    if (rc == WTQ_OK) ms->send_cancelled = true;
    ms->borrowed--;
    c->feed_depth--;
    return map_send_rc(rc);
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

/* Outbound datagram. wtquic copies the payload during the call, so nothing
 * outlives it. Results follow the bridge's lossy datagram policy: a datagram
 * over wtq_session_datagram_max_size() is TOO_LARGE, a momentarily full
 * transport queue is WOULD_BLOCK, and a session that cannot carry datagrams
 * -- none negotiated (WTQ_ERR_DGRAM_DISABLED), not yet active, or already
 * ended -- drops it, as does a send the transport cannot complete
 * (WTQ_ERR_NOMEM, WTQ_ERR_BACKEND). None of these is fatal: the session's own
 * terminal, if any, still arrives through the session callbacks. */
static moq_transport_result_t ep_send_datagram(void *ctx,
                                               const uint8_t *data,
                                               size_t len)
{
    moq_wtquic_conn_t *c = ctx;

    if (c->ws == NULL || (len > 0 && data == NULL))
        return MOQ_TRANSPORT_DROPPED;
    wtq_span_t span = { .data = data, .len = len };
    switch (wtq_session_send_datagram(c->ws, &span, 1)) {
    case WTQ_OK:
        return MOQ_TRANSPORT_OK;
    case WTQ_ERR_WOULD_BLOCK:
        return MOQ_TRANSPORT_WOULD_BLOCK;
    case WTQ_ERR_TOO_LARGE:
        return MOQ_TRANSPORT_TOO_LARGE;
    default:
        return MOQ_TRANSPORT_DROPPED;
    }
}

static size_t ep_max_datagram_size(void *ctx)
{
    moq_wtquic_conn_t *c = ctx;

    return c->ws != NULL ? wtq_session_datagram_max_size(c->ws) : 0;
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
static void stream_retire_if_idle(struct moq_wtq_stream *ms);

static void stream_enqueue(moq_wtquic_conn_t *c, struct moq_wtq_stream *ms,
                            stream_event_kind_t kind, uint32_t code)
{
    if (c->term_count == c->term_capacity) {
        c->term_overflow = true;
        return;
    }
    c->term_events[c->term_count++] = (struct moq_wtq_stream_event){
        .key = ms->id, .code = code, .kind = kind,
    };
    ms->pending_events++;
}

/* Drain queued stream outcomes into the bridge. Called ONLY from
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
        /* Remove before feeding: a callback caused by the feed can append
         * another event without overwriting this one. */
        size_t n = c->term_count;
        struct moq_wtq_stream_event event = c->term_events[0];
        for (size_t i = 1; i < n; i++)
            c->term_events[i - 1] = c->term_events[i];
        c->term_count = n - 1;
        struct moq_wtq_stream *ms = stream_find(c, event.key);
        if (ms == NULL || ms->pending_events == 0) {
            (void)moq_transport_bridge_on_transport_error(c->bridge, 0x1,
                                                          now_us());
            continue;
        }
        if (event.kind == STREAM_EVENT_COMMITTED) {
            /* A synchronous callback is retained until send has returned;
             * rejected FIN cannot become a successful commitment. */
            if (!ms->fin_sent)
                (void)moq_transport_bridge_on_transport_error(c->bridge, 0x1,
                                                              now_us());
            else
                (void)moq_transport_bridge_on_local_stream_committed(
                    c->bridge, event.key, now_us());
        } else if (event.kind == STREAM_EVENT_STOP) {
            (void)moq_transport_bridge_on_peer_stop_sending(
                c->bridge, event.key, event.code, now_us());
        } else {
            (void)moq_transport_bridge_on_peer_stream_terminal(
                c->bridge, event.key, now_us());
        }
        ms->pending_events--;
        stream_retire_if_idle(ms);
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
    uint64_t key = ms->id;
    wtq_result_t rc = wtq_stream_pause_receive(st);
    if (rc == WTQ_OK) {
        /* A synchronous terminal may have freed or reused this slot. */
        if (ms->in_use && ms->id == key)
            ms->paused = true;
    } else if (rc != WTQ_ERR_STATE && rc != WTQ_ERR_CLOSED)
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
    for (size_t i = 0; i < c->stream_capacity; i++) {
        struct moq_wtq_stream *ms = &c->streams[i];
        if (!ms->in_use || ms->held || !ms->paused || ms->st == NULL)
            continue;
        if (moq_transport_bridge_stream_has_pending(c->bridge, ms->id))
            continue; /* still backed up on this stream — keep it paused */
        uint64_t key = ms->id;
        wtq_result_t rc = wtq_stream_resume_receive(ms->st);
        if (rc == WTQ_OK || rc == WTQ_ERR_STATE || rc == WTQ_ERR_CLOSED) {
            /* resumed, or its incoming direction is already finished/closed
             * (nothing left to strand) — the pause is genuinely cleared */
            /* The provider can close and reuse the slot before returning. */
            if (ms->in_use && ms->id == key)
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

static void stream_retire_if_idle(struct moq_wtq_stream *ms)
{
    if (ms->st == NULL && !ms->held && ms->borrowed == 0 &&
        ms->pending_events == 0) {
        wtq_stream_t *lease = ms->lease;
        memset(ms, 0, sizeof(*ms));
        if (lease) wtq_stream_release(lease);
    }
}

static void cancel_held_input(moq_wtquic_conn_t *c)
{
    for (size_t i = 0; i < c->stream_capacity; i++) {
        struct moq_wtq_stream *ms = &c->streams[i];
        ms->held = false;
        ms->held_len = 0;
        stream_retire_if_idle(ms);
    }
}

static void replay_held_input(moq_wtquic_conn_t *c)
{
    for (size_t i = 0; i < c->stream_capacity; i++) {
        struct moq_wtq_stream *ms = &c->streams[i];
        if (!ms->in_use || !ms->held || ms->borrowed ||
            ms->replay_epoch == c->service_epoch ||
            moq_transport_bridge_stream_has_pending(c->bridge, ms->id))
            continue;
        ms->replay_epoch = c->service_epoch;
        ms->borrowed++;
        c->feed_depth++;
        moq_result_t rc = moq_transport_bridge_on_peer_uni_bytes(
            c->bridge, ms->id, ms->held_bytes, ms->held_len, ms->held_fin, now_us());
        c->feed_depth--;
        if (rc != MOQ_ERR_INPUT_NOT_CONSUMED) {
            ms->held = false;
            ms->held_len = 0;
        }
        ms->borrowed--;
        stream_retire_if_idle(ms);
        if (moq_transport_bridge_is_terminal(c->bridge)) break;
    }
}

static void conn_service(moq_wtquic_conn_t *c)
{
    if (c->in_service || c->feed_depth) {
        c->service_pending = true;
        return;
    }
    c->in_service = true;
    if (++c->service_epoch == 0) {
        c->service_epoch = 1;
        for (size_t i = 0; i < c->stream_capacity; i++)
            c->streams[i].replay_epoch = 0;
    }
    do {
        c->service_pending = false;
        if (c->misrouted) {
            c->misrouted = false;
            (void)moq_transport_bridge_on_transport_error(c->bridge, 0x2, now_us());
        }
        conn_feed_terminals(c);
        conn_feed_close(c);
        (void)moq_transport_bridge_service(c->bridge, now_us());
        /* Endpoint operations may synchronously retain terminal callbacks.
         * Feed them before invoking hooks or issuing further transport work. */
        conn_feed_terminals(c);
        conn_feed_close(c);
        if (c->hook != NULL) {
            c->hook(c, c->hook_user);
            /* the hook may have closed the WebTransport session (a
             * synchronous on_closed lands as close_pending); the
             * bridge must learn of the terminal before it services
             * anything the hook queued, or those sends would hit the
             * closed session and read as fatal */
            conn_feed_terminals(c);
            conn_feed_close(c);
            (void)moq_transport_bridge_service(c->bridge, now_us());
            conn_feed_terminals(c);
            conn_feed_close(c);
        }
        /* After servicing (which retires cleared bridge input), resume any
         * stream whose pending has drained — otherwise a pause that clears on
         * THIS service (not on the stream's own receive callback) strands it. */
        if (!moq_transport_bridge_is_terminal(c->bridge))
            replay_held_input(c);
        if (moq_transport_bridge_is_terminal(c->bridge))
            cancel_held_input(c);
        else
            resume_paused_streams(c);
        if (!c->teardown_sent && c->ws != NULL &&
            moq_transport_bridge_is_fatal(c->bridge)) {
            c->teardown_sent = true;
            (void)wtq_session_close(
                c->ws,
                (uint32_t)moq_transport_bridge_fatal_code(c->bridge),
                NULL, 0);
        }
        if (!moq_transport_bridge_is_terminal(c->bridge) && c->ws) {
            wtq_session_add_ref(c->ws);
            wtq_result_t admission_rc = wtq_session_service_stream_admission(c->ws);
            wtq_session_release(c->ws);
            if (admission_rc != WTQ_OK && admission_rc != WTQ_ERR_CLOSED) {
                (void)moq_transport_bridge_on_transport_error(c->bridge, 0x2, now_us());
                c->service_pending = true;
            }
        }
    } while (c->service_pending || (c->close_pending && !c->close_fed) ||
             c->term_count > 0 || c->term_overflow);
    c->stream_close_context = false;
    c->in_service = false;
}

void moq_wtquic_conn_service(moq_wtquic_conn_t *conn)
{
    if (conn != NULL)
        conn_service(conn);
}

/* --- wtquic events (wtquic -> bridge) --------------------------------------- */

static bool callback_matches(moq_wtquic_conn_t *c, wtq_session_t *s)
{
    if (s == c->ws) return true;
    c->misrouted = true;
    conn_service(c);
    return false;
}

static void ev_established(wtq_session_t *s, wtq_str_t sub, void *user)
{
    moq_wtquic_conn_t *c = user;

    (void)sub;
    if (!callback_matches(c, s)) return;
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

    if (!callback_matches(c, s)) return;
    (void)moq_transport_bridge_on_transport_error(c->bridge, status,
                                                  now_us());
    conn_service(c);
}

static void ev_failed(wtq_session_t *s, wtq_connect_failure_t why,
                      void *user)
{
    moq_wtquic_conn_t *c = user;

    if (!callback_matches(c, s)) return;
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
    if (!callback_matches(c, s)) return;
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

    if (!callback_matches(c, s)) return;
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
    /* Actual publication owes one later safe pass, including prefix-only
     * streams. Permanently blocked admission produces no callback/re-arm. */
    if (c->in_service) c->service_pending = true;
}

static void ev_stream_data(wtq_session_t *s, wtq_stream_t *st,
                           const uint8_t *data, size_t len, bool fin,
                           void *user)
{
    moq_wtquic_conn_t *c = user;
    struct moq_wtq_stream *ms = stream_find_by_handle(c, st);

    if (!callback_matches(c, s)) return;
    if (ms == NULL)
        return;

    if (len > c->receive_quantum || ms->held || ms->receive_reset) {
        (void)moq_transport_bridge_on_transport_error(c->bridge, 0x2, now_us());
        conn_service(c);
        return;
    }
    ms->borrowed++;
    c->feed_depth++;

    moq_result_t rc;
    if (c->ctrl_latched && ms->id == c->ctrl_id)
        rc = moq_transport_bridge_on_peer_control_bytes(
            c->bridge, ms->id, data, len, fin, now_us());
    else if (ms->bidi)
        rc = moq_transport_bridge_on_peer_bidi_bytes(
            c->bridge, ms->id, data, len, fin, now_us());
    else
        rc = moq_transport_bridge_on_peer_uni_bytes(
            c->bridge, ms->id, data, len, fin, now_us());
    c->feed_depth--;
    if (rc == MOQ_ERR_INPUT_NOT_CONSUMED && !ms->receive_reset &&
        !moq_transport_bridge_is_terminal(c->bridge)) {
        if (len) memcpy(ms->held_bytes, data, len);
        ms->held_len = len;
        ms->held_fin = fin;
        ms->held = true;
    }

    /* inbound backpressure: while the bridge holds this stream's bytes
     * pending, no more may be delivered — pause and let service() clear.
     * conn_service() below reconciles the resume (resume_paused_streams), so
     * a pause that clears on a LATER service call — not this receive — is not
     * stranded. */
    if (ms->st && moq_transport_bridge_stream_has_pending(c->bridge, ms->id))
        pause_stream_for_backpressure(c, ms, ms->st);
    ms->borrowed--;
    stream_retire_if_idle(ms);
    conn_service(c);
}

static void ev_stream_reset(wtq_session_t *s, wtq_stream_t *st,
                            uint32_t code, void *user)
{
    moq_wtquic_conn_t *c = user;
    struct moq_wtq_stream *ms = stream_find_by_handle(c, st);

    if (!callback_matches(c, s)) return;
    if (ms == NULL)
        return;
    ms->receive_reset = true;
    ms->held = false;
    ms->held_len = 0;
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

    if (!callback_matches(c, s)) return;
    if (ms == NULL)
        return;
    if (!ms->stop_seen && !ms->committed && !ms->send_cancelled) {
        ms->stop_seen = true;
        ms->send_cancelled = true;
        stream_enqueue(c, ms, STREAM_EVENT_STOP, code);
    }
    conn_service(c);
}

static void ev_stream_closed(wtq_session_t *s, wtq_stream_t *st,
                             void *user)
{
    moq_wtquic_conn_t *c = user;
    struct moq_wtq_stream *ms = stream_find_by_handle(c, st);

    if (!callback_matches(c, s)) return;
    if (ms != NULL) {
        /* Retain through a deferred/reentrant service pass, not merely the
         * callback frame: WT may defer session close until its caller unwinds. */
        c->stream_close_context = true;
        /* ENQUEUE the backend-neutral terminal; NEVER feed the bridge
         * here -- this may run synchronously from inside an endpoint op
         * (wtq_stream_abort -> immediate on_stream_closed), and inbound
         * handlers must not be called from an endpoint op. conn_service
         * drains it later, outside that frame. */
        stream_enqueue(c, ms, STREAM_EVENT_CLOSED, 0);
        ms->st = NULL;
        ms->paused = false;
        stream_retire_if_idle(ms);
    }
    /* a freed stream slot may unblock a pending open (re-entrant-safe:
     * conn_service latches in_service and defers) */
    conn_service(c);
}

static void ev_stream_committed(wtq_session_t *s, wtq_stream_t *st,
                                void *user)
{
    moq_wtquic_conn_t *c = user;
    if (!callback_matches(c, s)) return;
    struct moq_wtq_stream *ms = stream_find_by_handle(c, st);
    if (!ms || !ms->commit_enabled || ms->bidi || ms->committed ||
        ms->send_cancelled || c->close_pending ||
        (!ms->fin_sent && !ms->fin_submitting)) return;
    ms->committed = true;
    stream_enqueue(c, ms, STREAM_EVENT_COMMITTED, 0);
    conn_service(c);
}

static void ev_send_complete(wtq_session_t *s, void *send_ctx,
                             bool canceled, void *user)
{
    moq_wtquic_conn_t *c = user;
    struct moq_wtq_send_rec *rec = send_ctx;

    if (!callback_matches(c, s)) return;
    (void)canceled;
    if (rec != NULL) {
        send_rec_unlink(rec);
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

    if (!callback_matches(c, s)) return;
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

    if (!callback_matches(c, s)) return;
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
    .on_stream_send_committed = ev_stream_committed,
};

const wtq_session_events_t *moq_wtquic_conn_events(void)
{
    return &adapter_event_table;
}

void moq_wtquic_conn_cfg_init_sized(moq_wtquic_conn_cfg_t *cfg,
                                    size_t size)
{
    if (cfg == NULL || size < sizeof(moq_wtquic_conn_cfg_v0_t))
        return;
    size_t n = size < sizeof(*cfg) ? size : sizeof(*cfg);
    memset(cfg, 0, n);
    cfg->struct_size = (uint32_t)n;
}

/* Layout of one conn allocation holding `cap` stream entries: the conn,
 * then the stream table at *streams_off, then the terminal queue (each entry
 * contributes at most two queued outcomes) at *events_off. Every
 * multiplication and addition is checked against `limit` (SIZE_MAX for a real
 * allocation), so the arithmetic is the same for any size_t width. Returns
 * false when the total exceeds `limit`, leaving the outputs untouched. */
static bool conn_layout(size_t cap, size_t limit, size_t *streams_off,
                        size_t *events_off, size_t *total)
{
    const size_t base = sizeof(moq_wtquic_conn_t);
    const size_t entry = sizeof(struct moq_wtq_stream);
    const size_t event = sizeof(struct moq_wtq_stream_event);

    if (base > limit)
        return false;
    size_t room = limit - base;
    if (cap > room / entry)
        return false;
    size_t table = cap * entry;
    room -= table;
    if (cap > room / 2u / event)
        return false;
    *streams_off = base;
    *events_off = base + table;
    *total = base + table + 2u * cap * event;
    return true;
}

moq_result_t moq_wtquic_conn_create(const moq_wtquic_conn_cfg_t *cfg,
                                    moq_wtquic_conn_t **out)
{
    if (out == NULL)
        return MOQ_ERR_INVAL;
    *out = NULL;
    /* the size guard comes first: a too-small struct_size means the
     * caller's struct may not even contain the fields read below */
    if (cfg == NULL || cfg->struct_size < sizeof(moq_wtquic_conn_cfg_v0_t))
        return MOQ_ERR_INVAL;
    if (cfg->alloc == NULL || cfg->session == NULL)
        return MOQ_ERR_INVAL;
    if (cfg->alloc->alloc == NULL || cfg->alloc->realloc == NULL ||
        cfg->alloc->free == NULL)
        return MOQ_ERR_INVAL;

    if (cfg->struct_size < offsetof(moq_wtquic_conn_cfg_t, wt_session) +
                           sizeof(cfg->wt_session) || !cfg->wt_session)
        return MOQ_ERR_UNSUPPORTED;
    size_t quantum = 0;
    wtq_receive_pause_mode_t mode = WTQ_RECEIVE_PAUSE_UNSUPPORTED;
    if (wtq_session_receive_contract(cfg->wt_session, &quantum, &mode) != WTQ_OK ||
        mode != WTQ_RECEIVE_PAUSE_FLOW_CONTROLLED ||
        quantum == 0 || quantum > MOQ_WTQ_RECEIVE_QUANTUM)
        return MOQ_ERR_UNSUPPORTED;

    wtq_stream_budget_t budget = WTQ_STREAM_BUDGET_INIT;
    if (wtq_session_stream_budget(cfg->wt_session, &budget) != WTQ_OK ||
        budget.struct_size != sizeof(budget))
        return MOQ_ERR_UNSUPPORTED;
    /* These are the public provider's accepted count bounds. Refuse an
     * incoherent snapshot before allocating; never truncate its API capacity. */
    uint64_t peers = (uint64_t)budget.peer_unidi_stream_count +
                     budget.peer_bidi_stream_count;
    if (budget.peer_unidi_stream_count < 3 ||
        budget.peer_unidi_stream_count > MOQ_WTQ_MAX_PEER_STREAMS ||
        budget.peer_bidi_stream_count < 1 ||
        budget.peer_bidi_stream_count > MOQ_WTQ_MAX_PEER_STREAMS ||
        peers > MOQ_WTQ_MAX_PEER_STREAMS || budget.max_local_streams < 1 ||
        budget.max_local_streams > MOQ_WTQ_MAX_LOCAL_STREAMS ||
        budget.api_capacity != peers + budget.max_local_streams)
        return MOQ_ERR_UNSUPPORTED;
    /* One allocation: the conn, then its stream table, then the terminal
     * queue. A total this platform cannot represent is refused here, before
     * allocation, bridge creation or any transport effect. */
    size_t capacity = budget.api_capacity;
    size_t streams_off, events_off, total;
    if (!conn_layout(capacity, SIZE_MAX, &streams_off, &events_off, &total))
        return MOQ_ERR_UNSUPPORTED;

    moq_wtquic_conn_t *c = cfg->alloc->alloc(total, cfg->alloc->ctx);
    if (c == NULL)
        return MOQ_ERR_NOMEM;
    memset(c, 0, total);
    c->alloc_size = total;
    c->streams = (struct moq_wtq_stream *)(void *)((uint8_t *)c + streams_off);
    c->stream_capacity = capacity;
    c->term_events =
        (struct moq_wtq_stream_event *)(void *)((uint8_t *)c + events_off);
    c->term_capacity = 2u * capacity;
    c->next_stream_key = 1; /* 0 unused; UINT64_MAX reserved */
    c->alloc = *cfg->alloc;
    c->session = cfg->session;
    c->hook = cfg->hook;
    c->hook_user = cfg->hook_user;
    c->ctrl_id = UINT64_MAX;
    c->ws = cfg->wt_session;
    c->receive_quantum = quantum;

    c->ops = (moq_transport_endpoint_ops_t){
        .struct_size = sizeof(moq_transport_endpoint_ops_t),
        .capabilities = MOQ_TRANSPORT_CAP_DATAGRAM |
                        MOQ_TRANSPORT_CAP_WRITE_PAYLOAD |
                        MOQ_TRANSPORT_CAP_HOLD_INPUT,
        .open_uni = ep_open_uni,
        .open_bidi = ep_open_bidi,
        .write = ep_write,
        .write_payload = ep_write_payload,
        .reset_stream = ep_reset_stream,
        .stop_sending = ep_stop_sending,
        .abort_stream = ep_abort_stream,
        .send_datagram = ep_send_datagram,
        .max_datagram_size = ep_max_datagram_size,
        .close_transport = ep_close_transport,
    };
    if (wtq_session_send_commit_mode(c->ws) ==
        WTQ_SEND_COMMIT_LOCAL_UNI_ACK_FIN_RESETTABLE)
        c->ops.capabilities |= MOQ_TRANSPORT_CAP_COMMIT_REPORT;

    moq_transport_bridge_cfg_t bcfg;
    moq_transport_bridge_cfg_init(&bcfg, &c->alloc);
    moq_result_t rc = moq_transport_bridge_create(&bcfg, cfg->session,
                                                  &c->ops, c, &c->bridge);
    if (rc != MOQ_OK) {
        c->alloc.free(c, c->alloc_size, c->alloc.ctx);
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
    size_t alloc_size = c->alloc_size;

    moq_transport_bridge_destroy(c->bridge);
    for (size_t i = 0; i < c->stream_capacity; ++i) {
        wtq_stream_t *lease = c->streams[i].lease;
        c->streams[i].lease = NULL;
        if (lease) wtq_stream_release(lease);
    }
    alloc.free(c, alloc_size, alloc.ctx);
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

moq_result_t moq_wtquic_conn_poll_flush(const moq_wtquic_conn_t *c)
{
    if (c == NULL) return MOQ_ERR_INVAL;
    if (c->close_pending || c->teardown_sent ||
        moq_transport_bridge_is_terminal(c->bridge))
        return MOQ_ERR_CLOSED;
    if (c->misrouted || c->term_overflow) return MOQ_ERR_INTERNAL;
    if (c->ws == NULL || c->session == NULL) return MOQ_DONE;

    /* Observe errors even when an upper layer retains output. Otherwise sticky
     * loss could be hidden forever behind a pending action or send borrow. */
    wtq_result_t rc = wtq_session_local_flush(c->ws);
    switch (rc) {
    case WTQ_OK:
    case WTQ_ERR_WOULD_BLOCK: break;
    case WTQ_ERR_STATE:       return MOQ_DONE;
    case WTQ_ERR_CLOSED:      return MOQ_ERR_CLOSED;
    case WTQ_ERR_UNSUPPORTED: return MOQ_ERR_UNSUPPORTED;
    default:                 return MOQ_ERR_INTERNAL;
    }
    wtq_local_flush_mode_t mode = wtq_session_local_flush_mode(c->ws);
    if (mode != WTQ_LOCAL_FLUSH_TRANSPORT && mode != WTQ_LOCAL_FLUSH_ACKNOWLEDGED)
        return MOQ_ERR_UNSUPPORTED;
    if (rc == WTQ_ERR_WOULD_BLOCK || c->sends != NULL ||
        session_has_reliable_outbound_obligation(c->session) ||
        moq_transport_bridge_has_outbound_pending(c->bridge))
        return MOQ_DONE;
    for (size_t i = 0; i < c->stream_capacity; ++i)
        if (c->streams[i].fin_submitting) return MOQ_DONE;
    return MOQ_OK;
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
