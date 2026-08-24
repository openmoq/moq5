#ifndef MOQ_MSQUIC_INTERNAL_H
#define MOQ_MSQUIC_INTERNAL_H

/*
 * Internal state shared between the attach adapter, the managed
 * facade, and the white-box tests (which compile the adapter sources
 * with MOQ_MSQUIC_TESTING). Not installed.
 */

#include <moq/msquic.h>
#include <moq/rcbuf.h>
#include <moq/transport_bridge.h>

/* MsQuic's generic SetCallbackHandler() takes the callback handler as a
 * void*, but ISO C forbids a direct function<->object pointer conversion
 * (a POSIX extension the -Wpedantic CI build rejects). Launder the handler
 * through a union at the few sites that register one. */
static inline void *moq_msq_conn_cb_ptr(QUIC_CONNECTION_CALLBACK_HANDLER fn)
{
    union { QUIC_CONNECTION_CALLBACK_HANDLER fn; void *obj; } u = { .fn = fn };
    return u.obj;
}
static inline void *moq_msq_stream_cb_ptr(QUIC_STREAM_CALLBACK_HANDLER fn)
{
    union { QUIC_STREAM_CALLBACK_HANDLER fn; void *obj; } u = { .fn = fn };
    return u.obj;
}

/* Per-stream in-flight send budget floor. The budget throttles queued
 * DEPTH: an idle stream admits one legal send of any size (a refused
 * send has no completion to wake a retry — a size cap would deadlock,
 * not backpressure). The floor must cover bandwidth x delayed-ACK:
 * SEND_COMPLETE means FULL ACKNOWLEDGMENT here (send buffering off),
 * so a floor near the object size serializes one object per ACK
 * round-trip (~25 ms against a quiet peer). 1 MiB matches the
 * picoquic adapter's default queue capacity. */
#define MOQ_MSQ_SEND_BUDGET_MIN (1024u * 1024u)

/* Writes staged within one service pass flush as ONE multi-buffer
 * StreamSend (one completion, one accounting update): the bridge
 * emits a header write plus a payload write per object, and a burst
 * of objects per pass — per-send costs dominate small objects
 * otherwise. */
#define MOQ_MSQ_BATCH_MAX 64

/* MsQuic's own datagram queue is unbounded; the adapter caps accepted
 * in-flight datagram sends and reports WOULD_BLOCK past the cap. */
#define MOQ_MSQ_DGRAM_INFLIGHT_MAX 64

struct moq_msq_stream {
    struct moq_msq_stream *next;
    moq_msquic_conn_t *conn;   /* stream-callback context back-pointer */
    HQUIC stream;              /* NULL once the transport handle died */
    uint64_t id;
    uint64_t inflight_bytes;   /* accepted send bytes awaiting completion */
    uint64_t ideal_send;       /* last IDEAL_SEND_BUFFER_SIZE (0 = none) */
    bool is_bidi;
    bool is_local;
    bool fin_fed;              /* bridge already got this stream's FIN */
    bool paused;               /* bridge is backed up on this stream:
                                  arriving receives are rejected (0 bytes
                                  accepted) instead of fed, until the
                                  bridge drains and delivery is resumed */
    bool recv_disabled;        /* a receive was rejected (TotalBufferLength
                                  set to 0), so MsQuic has stopped
                                  indicating RECEIVE and holds the bytes;
                                  a StreamReceiveSetEnabled(TRUE) is owed
                                  to redeliver them. Distinct from paused:
                                  the pending-causing receive is accepted
                                  in full, so paused can be set without
                                  MsQuic being disabled yet. */
};

/* One accepted datagram send: the bridge's datagram contract is
 * borrow-during-call, but MsQuic borrows the QUIC_BUFFER and bytes
 * until a FINAL send state — so the bytes are copied into the tail and
 * the record freed at IS_FINAL. */
struct moq_msq_dgram_rec {
    QUIC_BUFFER buf;
    size_t alloc_size;
    /* copied bytes follow */
};

/* One staged write inside a batch: either a retained rcbuf (payload,
 * zero-copy) or an owned copy of cold bytes — released exactly once
 * when the batch's single SEND_COMPLETE fires. */
struct moq_msq_batch_entry {
    moq_rcbuf_t *rcbuf;   /* retained; NULL = copied cold bytes */
    void *copy;           /* owned; NULL for rcbuf entries */
    size_t copy_size;
};

/* One accepted (flushed) batch send: MsQuic borrows the QUIC_BUFFER
 * array and every buffer's bytes until SEND_COMPLETE. Recycled through
 * a per-conn freelist. */
struct moq_msq_batch_rec {
    struct moq_msq_batch_rec *next; /* pool freelist */
    struct moq_msq_stream *ms;
    uint64_t bytes;
    size_t count;
    QUIC_BUFFER bufs[MOQ_MSQ_BATCH_MAX];
    struct moq_msq_batch_entry ents[MOQ_MSQ_BATCH_MAX];
};

struct moq_msquic_conn {
    moq_alloc_t alloc;
    moq_session_t *session;        /* not owned */
    const QUIC_API_TABLE *api;     /* not owned */
    moq_transport_bridge_t *bridge;
    HQUIC conn;                    /* bound by moq_msquic_conn_bind */
    moq_msquic_hook_fn hook;
    void *hook_user;
    moq_msquic_guard_fn guard_enter; /* both set or both NULL */
    moq_msquic_guard_fn guard_leave;
    void *guard_user;

    struct moq_msq_stream *streams;
    struct moq_msq_batch_rec *rec_free; /* pooled batch records */
    int pending_sends;                  /* batches awaiting SEND_COMPLETE */

    /* Accepted StreamSend accounting: batches the transport actually took
     * and their byte totals (synchronous send failures are never counted).
     * Written only inside batch_flush, which always runs under the
     * connection's serialization (the managed facade's lane lock, or the
     * attach caller's single-threaded discipline). */
    uint64_t flush_sends;
    uint64_t flush_bytes;

    /* the batch being staged in the CURRENT service pass; always
     * flushed (or empty) by the time conn_service returns */
    struct moq_msq_batch_rec *building;
    struct moq_msq_stream *building_ms;
    bool building_fin;
    bool flush_failed;                  /* deferred fatal (fed by service) */

    /* local stream-id allocation (QUIC numbering: type + (count<<2));
     * MsQuic assigns ids asynchronously, so the adapter computes them
     * and verifies against START_COMPLETE */
    uint64_t local_bidi_count;
    uint64_t local_uni_count;
    bool is_client;

    /* peer stream credit, tracked from STREAMS_AVAILABLE; open_* gates
     * on it once known (credit_known) and returns WOULD_BLOCK at zero */
    uint16_t bidi_credit;
    uint16_t uni_credit;
    bool credit_known;

    /* datagram availability (DATAGRAM_STATE_CHANGED) + in-flight cap */
    bool dgram_send_enabled;
    uint16_t dgram_max;
    int dgram_inflight;

    uint64_t ctrl_id;              /* MoQ control stream (bidi profiles) */
    bool ctrl_latched;
    bool started;
    bool teardown_sent;            /* fatal-bridge teardown issued once */
    bool in_service;               /* conn_service is on the stack */
    bool service_pending;          /* re-run requested while in service */
    bool close_pending;            /* deferred transport-close feed */
    bool close_fed;
    bool close_clean;
    uint64_t close_code;
    bool shutdown_complete;        /* conn SHUTDOWN_COMPLETE observed */
#ifdef MOQ_MSQUIC_TESTING
    /* Number of close feeds that crossed conn_feed_close's pending/fed guard.
     * Tests use this to distinguish one committed bridge call from a boolean
     * that merely says at least one feed was attempted. */
    uint32_t test_close_feed_commits;
#endif

    moq_transport_endpoint_ops_t ops;
};

/* Adapter-internal (shared with the managed facade, not public): the
 * wake/timer-driven service cycle. Honors the wake => on_lane_pump
 * contract with exactly one post-hook bridge pass to drain whatever the pump
 * queued, and skips the pre-hook retry pass — every budget, credit or
 * receive change runs the full service on its own transport event
 * before any wake can observe it, so a wake-time retry scan can never
 * make progress those events did not already attempt. Terminal feeds
 * (deferred close, failed flush, fatal backstop) still take the full
 * path. */
void moq_msq_conn_service_wake(moq_msquic_conn_t *conn);

/* Managed re-drive predicate inputs (private-SPI pass-throughs to the bridge).
 * The coalesced doorbell snapshots moq_msq_conn_event_token() before the app
 * pump and, after the post-pump service pass, re-arms one pump when the token
 * moved across the cycle (events enqueued by service OR dequeued by the pump)
 * AND moq_msq_conn_has_events() still reports queued events. A non-draining app
 * moves no events once its queue is full, so the token stays put and no re-arm
 * fires (no spin). */
uint64_t moq_msq_conn_event_token(const moq_msquic_conn_t *conn);
bool moq_msq_conn_has_events(const moq_msquic_conn_t *conn);

#ifdef MOQ_MSQUIC_TESTING
/* White-box seams for the unit tests (never shipped). */
/* One clock source shared by the attach adapter and managed facade. Tests
 * install it before constructing either object and clear it only after every
 * worker has stopped, so session deadlines and doorbell attribution advance
 * from the same deterministic time. NULL preserves the production clock. */
extern uint64_t (*moq_msq_test_now_us)(void);

const moq_transport_endpoint_ops_t *moq_msquic_test_ops(
    moq_msquic_conn_t *conn);
void moq_msquic_test_flush(moq_msquic_conn_t *conn);
QUIC_STREAM_CALLBACK_HANDLER moq_msquic_test_stream_callback(void);
struct moq_msq_stream *moq_msquic_test_stream_find(moq_msquic_conn_t *conn,
                                                   uint64_t id);
#endif

#endif /* MOQ_MSQUIC_INTERNAL_H */
