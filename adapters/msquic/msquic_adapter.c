/*
 * MoQ over raw QUIC via MsQuic: transport-bridge endpoint ops on the
 * MsQuic API table, and MsQuic connection/stream events fed into the
 * bridge.
 *
 * Confinement: MsQuic delivers a connection's events serialized on its
 * worker (never recursively); every bridge feed, service() and hook
 * invocation happens inside those callbacks, so the moq session and
 * bridge stay single-threaded without locks.
 *
 * Send ownership: with SendBufferingEnabled=FALSE MsQuic borrows the
 * QUIC_BUFFER array and every buffer's bytes until SEND_COMPLETE
 * (which then means full acknowledgment). Writes staged within one
 * service pass batch into a single multi-buffer StreamSend per stream:
 * the bridge emits a header write plus a payload write per object and
 * a burst of objects per pass, so per-send transport costs would
 * otherwise dominate small objects. Each staged entry owns its bytes —
 * a retained rcbuf (zero-copy) or a copy of the bridge's
 * borrow-during-call cold bytes — released exactly once when the
 * batch's single completion fires (single-shard refcounts: the incref
 * and decref both happen on the connection's serialized context). The
 * staged batch never outlives the service pass that built it: every
 * pass flushes before returning, and reset/stop/close flush first so
 * ordering is preserved.
 *
 * Send budget: MsQuic queues sends without bound, so the adapter
 * enforces a per-stream in-flight budget BEFORE StreamSend. The budget
 * throttles queued depth, not write size: an idle stream admits one
 * legal send of any size, because a refused send has no completion to
 * wake a retry.
 *
 * Stream ids: StreamStart is always queued — MsQuic assigns the id
 * asynchronously — but the bridge needs an id synchronously from
 * open_uni/open_bidi. The adapter computes QUIC-numbered ids itself
 * (type + (count << 2)) and verifies them against START_COMPLETE.ID.
 *
 * Inbound backpressure: once a bridge feed leaves the stream pending, an
 * arriving RECEIVE is arrested SYNCHRONOUSLY by accepting zero bytes
 * (RECEIVE.TotalBufferLength -> 0) — MsQuic then holds the unconsumed
 * bytes and stops indicating RECEIVE. StreamReceiveSetEnabled(FALSE) is
 * async (a queued RECEIVE still fires) and is not used. Once service()
 * clears the stream's pending state, StreamReceiveSetEnabled(TRUE) is
 * submitted to redeliver the held bytes; a failed resume is unrecoverable
 * and surfaced as a transport error (fatal), and the paused/disabled
 * state is published only after the resume submission succeeds.
 */

#if defined(__linux__) && !defined(_POSIX_C_SOURCE)
#define _POSIX_C_SOURCE 200809L
#endif

#include <string.h>
#include <time.h>

#include "msquic_internal.h"

#ifdef MOQ_MSQUIC_TESTING
uint64_t (*moq_msq_test_now_us)(void);
#endif

static uint64_t now_us(void)
{
#ifdef MOQ_MSQUIC_TESTING
    if (moq_msq_test_now_us != NULL)
        return moq_msq_test_now_us();
#endif
    struct timespec ts;

    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000u + (uint64_t)ts.tv_nsec / 1000u;
}

/* --- stream table ----------------------------------------------------------- */

static struct moq_msq_stream *stream_find(moq_msquic_conn_t *c,
                                          uint64_t id)
{
    for (struct moq_msq_stream *ms = c->streams; ms != NULL;
         ms = ms->next)
        if (ms->id == id)
            return ms;
    return NULL;
}

static struct moq_msq_stream *stream_new(moq_msquic_conn_t *c,
                                         uint64_t id, bool is_bidi,
                                         bool is_local)
{
    struct moq_msq_stream *ms =
        c->alloc.alloc(sizeof(*ms), c->alloc.ctx);

    if (ms == NULL)
        return NULL;
    memset(ms, 0, sizeof(*ms));
    ms->conn = c;
    ms->id = id;
    ms->is_bidi = is_bidi;
    ms->is_local = is_local;
    ms->next = c->streams;
    c->streams = ms;
    return ms;
}

static void stream_free(moq_msquic_conn_t *c, struct moq_msq_stream *ms)
{
    for (struct moq_msq_stream **p = &c->streams; *p != NULL;
         p = &(*p)->next)
        if (*p == ms) {
            *p = ms->next;
            break;
        }
    c->alloc.free(ms, sizeof(*ms), c->alloc.ctx);
}

/* --- batched send records ----------------------------------------------------- */

static struct moq_msq_batch_rec *batch_get(moq_msquic_conn_t *c)
{
    struct moq_msq_batch_rec *rec = c->rec_free;

    if (rec != NULL) {
        c->rec_free = rec->next;
        return rec;
    }
    return c->alloc.alloc(sizeof(*rec), c->alloc.ctx);
}

static void batch_put(moq_msquic_conn_t *c, struct moq_msq_batch_rec *rec)
{
    rec->next = c->rec_free;
    c->rec_free = rec;
}

/* Release every staged entry's bytes — exactly once per entry. */
static void batch_release_entries(moq_msquic_conn_t *c,
                                  struct moq_msq_batch_rec *rec)
{
    for (size_t i = 0; i < rec->count; i++) {
        struct moq_msq_batch_entry *e = &rec->ents[i];

        if (e->rcbuf != NULL)
            moq_rcbuf_decref(e->rcbuf);
        else if (e->copy != NULL)
            c->alloc.free(e->copy, e->copy_size, c->alloc.ctx);
        e->rcbuf = NULL;
        e->copy = NULL;
    }
}

/*
 * Flush the staged batch as one StreamSend. Runs both from endpoint
 * ops (stream switch, batch full, FIN) and from conn_service after
 * each bridge pass; when it runs inside an op it must not call into
 * the bridge, so a synchronous StreamSend failure only latches
 * flush_failed — conn_service feeds the transport error exactly once.
 */
static void batch_flush(moq_msquic_conn_t *c)
{
    struct moq_msq_batch_rec *rec = c->building;
    struct moq_msq_stream *ms = c->building_ms;
    bool fin = c->building_fin;

    if (rec == NULL)
        return;
    c->building = NULL;
    c->building_ms = NULL;
    c->building_fin = false;

    if (ms->stream == NULL ||
        QUIC_FAILED(c->api->StreamSend(
            ms->stream, rec->bufs, (uint32_t)rec->count,
            fin ? QUIC_SEND_FLAG_FIN : QUIC_SEND_FLAG_NONE, rec))) {
        /* not accepted: no completion will fire — release ownership
         * here, exactly once, and let the next service pass report
         * the dead transport */
        batch_release_entries(c, rec);
        batch_put(c, rec);
        c->flush_failed = true;
        return;
    }
    rec->ms = ms;
    ms->inflight_bytes += rec->bytes;
    c->pending_sends++;
    c->flush_sends++;            /* ACCEPTED batches only (failure returned) */
    c->flush_bytes += rec->bytes;
}

/* Stage one write into the current batch (starting or flushing as
 * needed). ALWAYS takes ownership of `copy` (freed here on every
 * failure path); rcbuf ownership is taken only when the entry is
 * staged (the incref) and then released by whichever flush disposes
 * of the batch. A flush that fails — the stream-switch flush before
 * staging, or the FIN/full flush after — stops the op immediately
 * with an error, so the bridge halts instead of piling actions onto
 * a transport that already discarded accepted bytes; flush_failed
 * stays latched for conn_service's exactly-once fatal feed. */
static moq_transport_result_t batch_append(moq_msquic_conn_t *c,
                                           struct moq_msq_stream *ms,
                                           moq_rcbuf_t *rcbuf,
                                           void *copy, size_t copy_size,
                                           const uint8_t *data,
                                           size_t len, bool fin)
{
    if (c->building != NULL && c->building_ms != ms) {
        batch_flush(c);
        if (c->flush_failed)
            goto refuse; /* transport dead; this write never staged */
    }
    if (c->building == NULL) {
        c->building = batch_get(c);
        if (c->building == NULL)
            goto refuse;
        c->building->count = 0;
        c->building->bytes = 0;
        c->building_ms = ms;
        c->building_fin = false;
    }
    struct moq_msq_batch_rec *rec = c->building;
    struct moq_msq_batch_entry *e = &rec->ents[rec->count];

    e->rcbuf = rcbuf;
    e->copy = copy;
    e->copy_size = copy_size;
    if (rcbuf != NULL)
        moq_rcbuf_incref(rcbuf); /* held until the batch completion */
    /* QUIC_BUFFER is not const-qualified, but MsQuic only reads send
     * buffers */
    rec->bufs[rec->count].Buffer = (uint8_t *)(uintptr_t)data;
    rec->bufs[rec->count].Length = (uint32_t)len;
    rec->count++;
    rec->bytes += len;
    if (fin)
        c->building_fin = true;
    if (fin || rec->count == MOQ_MSQ_BATCH_MAX) {
        batch_flush(c);
        if (c->flush_failed)
            /* the failed flush already released every staged entry —
             * this one included (its copy/rcbuf ownership is gone,
             * exactly once) */
            return MOQ_TRANSPORT_ERROR;
    }
    return MOQ_TRANSPORT_OK;

refuse:
    if (copy != NULL)
        c->alloc.free(copy, copy_size, c->alloc.ctx);
    return MOQ_TRANSPORT_ERROR;
}

/* Depth throttle, subtraction form: an accepted oversized send can
 * leave inflight above the ceiling, so the naive addition could wrap;
 * zero-byte sends (a bare FIN) always pass. Bytes staged in the
 * current batch count as already in flight. */
static bool budget_blocks(const moq_msquic_conn_t *c,
                          const struct moq_msq_stream *ms, uint64_t total)
{
    uint64_t ceiling = ms->ideal_send > MOQ_MSQ_SEND_BUDGET_MIN
                           ? ms->ideal_send
                           : MOQ_MSQ_SEND_BUDGET_MIN;
    uint64_t staged = (c->building != NULL && c->building_ms == ms)
                          ? c->building->bytes
                          : 0;
    uint64_t inflight = ms->inflight_bytes + staged; /* staged bounded:
                          <= MOQ_MSQ_BATCH_MAX * UINT32_MAX, no wrap */

    return total != 0 && inflight != 0 &&
           (inflight >= ceiling || total > ceiling - inflight);
}

/* --- servicing ---------------------------------------------------------------- */

static void conn_service(moq_msquic_conn_t *c);

static void conn_feed_close(moq_msquic_conn_t *c)
{
    if (!c->close_pending || c->close_fed)
        return;
#ifdef MOQ_MSQUIC_TESTING
    c->test_close_feed_commits++;
#endif
    c->close_fed = true;
    if (c->close_clean)
        (void)moq_transport_bridge_on_transport_close(
            c->bridge, c->close_code, now_us());
    else
        (void)moq_transport_bridge_on_transport_error(
            c->bridge, c->close_code, now_us());
}

/*
 * A stream whose bridge state has drained may take bytes again. Re-enable
 * MsQuic delivery for any receive we rejected while paused, then clear the
 * paused mark. Ordering matters: publish the cleared state ONLY after the
 * StreamReceiveSetEnabled(TRUE) submission succeeds. A failed resume can
 * never be retried (the mark would already be cleared) and leaves the
 * stream unable to ever deliver again — an unrecoverable break of the
 * inbound invariant — so surface it as a transport error (fatal) rather
 * than silently stalling. Returns false and stops on the first such
 * failure; conn_service's fatal check then tears the connection down.
 */
static bool resume_paused(moq_msquic_conn_t *c)
{
    for (struct moq_msq_stream *ms = c->streams; ms != NULL;
         ms = ms->next) {
        if (!ms->paused || ms->stream == NULL ||
            moq_transport_bridge_stream_has_pending(c->bridge, ms->id))
            continue;
        if (ms->recv_disabled) {
            if (QUIC_FAILED(c->api->StreamReceiveSetEnabled(ms->stream,
                                                            TRUE))) {
                (void)moq_transport_bridge_on_transport_error(
                    c->bridge, 0x2, now_us());
                return false;
            }
            ms->recv_disabled = false;
        }
        ms->paused = false;
    }
    return true;
}

/*
 * Feed happened: drive the bridge, give the app its slot, drive again
 * for whatever the app queued. A fatal bridge must not leave the
 * transport running: shut the connection down once with the fatal
 * code. Re-entrancy guard: any conn_service reached while one is on
 * the stack only flags a re-run; a transport close recorded during the
 * hook is fed before the bridge services anything the hook queued.
 */
static void conn_service(moq_msquic_conn_t *c)
{
    if (c->in_service) {
        c->service_pending = true;
        return;
    }
    c->in_service = true;
    do {
        c->service_pending = false;
        conn_feed_close(c);
        (void)moq_transport_bridge_service(c->bridge, now_us());
        batch_flush(c); /* everything this pass staged goes out */
        /* a batch the bridge already counted as written could not be
         * handed to the transport: the connection is dead — report it
         * IMMEDIATELY (outside any endpoint op), before any app code
         * could run against a transport whose accepted bytes are
         * gone, and skip the hook for this pass (later passes still
         * run it, so the app observes the fatal there) */
        bool flush_fatal = false;
        if (c->flush_failed) {
            c->flush_failed = false;
            flush_fatal = true;
            (void)moq_transport_bridge_on_transport_error(c->bridge, 0x1,
                                                          now_us());
        }
        if (c->hook != NULL && !flush_fatal) {
            c->hook(c, c->hook_user);
            conn_feed_close(c);
            (void)moq_transport_bridge_service(c->bridge, now_us());
            batch_flush(c);
            if (c->flush_failed) {
                c->flush_failed = false;
                (void)moq_transport_bridge_on_transport_error(
                    c->bridge, 0x1, now_us());
            }
        }
        (void)resume_paused(c);
        if (!c->teardown_sent && c->conn != NULL &&
            moq_transport_bridge_is_fatal(c->bridge)) {
            c->teardown_sent = true;
            c->api->ConnectionShutdown(
                c->conn, QUIC_CONNECTION_SHUTDOWN_FLAG_NONE,
                moq_transport_bridge_fatal_code(c->bridge));
        }
    } while (c->service_pending || (c->close_pending && !c->close_fed));
    c->in_service = false;
}

static void guard_enter(moq_msquic_conn_t *c)
{
    if (c->guard_enter != NULL)
        c->guard_enter(c->guard_user);
}

static void guard_leave(moq_msquic_conn_t *c)
{
    if (c->guard_leave != NULL)
        c->guard_leave(c->guard_user);
}

/* --- endpoint ops (bridge -> MsQuic) ------------------------------------------ */

static QUIC_STATUS stream_event_cb(HQUIC stream, void *ctx,
                                   QUIC_STREAM_EVENT *ev);

static moq_transport_result_t ep_open(void *ctx, bool bidi,
                                      uint64_t *out_id)
{
    moq_msquic_conn_t *c = ctx;
    HQUIC h = NULL;

    if (c->conn == NULL)
        return MOQ_TRANSPORT_ERROR;
    if (c->credit_known) {
        uint16_t credit = bidi ? c->bidi_credit : c->uni_credit;

        if (credit == 0)
            return MOQ_TRANSPORT_WOULD_BLOCK;
    }

    /* QUIC numbering: type bits from perspective+direction, count in
     * the upper bits */
    uint64_t type = bidi ? (c->is_client ? 0u : 1u)
                         : (c->is_client ? 2u : 3u);
    uint64_t count = bidi ? c->local_bidi_count : c->local_uni_count;
    uint64_t id = type + (count << 2);

    struct moq_msq_stream *ms = stream_new(c, id, bidi, true);
    if (ms == NULL)
        return MOQ_TRANSPORT_ERROR;

    if (QUIC_FAILED(c->api->StreamOpen(
            c->conn,
            bidi ? QUIC_STREAM_OPEN_FLAG_NONE
                 : QUIC_STREAM_OPEN_FLAG_UNIDIRECTIONAL,
            stream_event_cb, ms, &h))) {
        stream_free(c, ms);
        return MOQ_TRANSPORT_ERROR;
    }
    ms->stream = h;
    /* FAIL_BLOCKED backs the credit gate: with the gate working the
     * start never exceeds peer limits; if it somehow does, the failure
     * surfaces in START_COMPLETE instead of queueing invisibly */
    if (QUIC_FAILED(c->api->StreamStart(
            h, QUIC_STREAM_START_FLAG_FAIL_BLOCKED |
                   QUIC_STREAM_START_FLAG_SHUTDOWN_ON_FAIL))) {
        c->api->StreamClose(h);
        stream_free(c, ms);
        return MOQ_TRANSPORT_ERROR;
    }
    if (bidi) {
        c->local_bidi_count++;
        if (c->credit_known)
            c->bidi_credit--;
    } else {
        c->local_uni_count++;
        if (c->credit_known)
            c->uni_credit--;
    }
    *out_id = id;
    /* bidi-control profiles: the first client-initiated bidi carries
     * the MoQ control channel */
    if (bidi && !c->ctrl_latched && c->is_client &&
        !moq_transport_bridge_uses_uni_control(c->bridge)) {
        c->ctrl_id = id;
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

static moq_transport_result_t ep_write(void *ctx, uint64_t stream_id,
                                       const uint8_t *data, size_t len,
                                       bool fin)
{
    moq_msquic_conn_t *c = ctx;
    struct moq_msq_stream *ms = stream_find(c, stream_id);

    if (ms == NULL || ms->stream == NULL)
        return MOQ_TRANSPORT_ERROR;
    if (len == 0) {
        if (!fin)
            return MOQ_TRANSPORT_OK;
        /* pure FIN: ride the staged batch when one is open on this
         * stream (ordering), else a graceful send shutdown — queued
         * ahead of nothing, after everything already flushed */
        if (c->building != NULL && c->building_ms == ms) {
            c->building_fin = true;
            batch_flush(c);
            return c->flush_failed ? MOQ_TRANSPORT_ERROR
                                   : MOQ_TRANSPORT_OK;
        }
        if (QUIC_FAILED(c->api->StreamShutdown(
                ms->stream, QUIC_STREAM_SHUTDOWN_FLAG_GRACEFUL, 0)))
            return MOQ_TRANSPORT_ERROR;
        return MOQ_TRANSPORT_OK;
    }
    /* one QUIC_BUFFER per entry, and the copy sizing must not wrap:
     * reject on length alone, before the budget, the allocator, or
     * the bytes are touched */
    if (len > UINT32_MAX)
        return MOQ_TRANSPORT_ERROR;
    if (budget_blocks(c, ms, len))
        return MOQ_TRANSPORT_WOULD_BLOCK;

    /* MsQuic borrows the bytes until the completion; the bridge's
     * write contract releases them at return — copy them */
    void *copy = c->alloc.alloc(len, c->alloc.ctx);
    if (copy == NULL)
        return MOQ_TRANSPORT_ERROR;
    memcpy(copy, data, len);
    /* batch_append owns `copy` from here on every path */
    return batch_append(c, ms, NULL, copy, len, copy, len, fin);
}

static moq_transport_result_t ep_write_payload(void *ctx,
                                               uint64_t stream_id,
                                               moq_rcbuf_t *buf, bool fin)
{
    moq_msquic_conn_t *c = ctx;
    struct moq_msq_stream *ms = stream_find(c, stream_id);
    size_t len = moq_rcbuf_len(buf);

    if (ms == NULL || ms->stream == NULL)
        return MOQ_TRANSPORT_ERROR;
    if (len > UINT32_MAX)
        return MOQ_TRANSPORT_ERROR; /* one QUIC_BUFFER per entry */
    if (budget_blocks(c, ms, len))
        return MOQ_TRANSPORT_WOULD_BLOCK;

    return batch_append(c, ms, buf, NULL, 0, moq_rcbuf_data(buf), len,
                        fin);
}

static moq_transport_result_t ep_reset_stream(void *ctx,
                                              uint64_t stream_id,
                                              uint64_t error_code)
{
    moq_msquic_conn_t *c = ctx;
    struct moq_msq_stream *ms = stream_find(c, stream_id);

    batch_flush(c); /* staged data must not reorder past the reset */
    if (c->flush_failed)
        return MOQ_TRANSPORT_ERROR; /* dead transport; stop the pass */
    if (ms == NULL || ms->stream == NULL)
        return MOQ_TRANSPORT_OK; /* already gone: idempotent */
    if (QUIC_FAILED(c->api->StreamShutdown(
            ms->stream, QUIC_STREAM_SHUTDOWN_FLAG_ABORT_SEND,
            error_code)))
        return MOQ_TRANSPORT_ERROR;
    return MOQ_TRANSPORT_OK;
}

static moq_transport_result_t ep_stop_sending(void *ctx,
                                              uint64_t stream_id,
                                              uint64_t error_code)
{
    moq_msquic_conn_t *c = ctx;
    struct moq_msq_stream *ms = stream_find(c, stream_id);

    batch_flush(c);
    if (c->flush_failed)
        return MOQ_TRANSPORT_ERROR;
    if (ms == NULL || ms->stream == NULL)
        return MOQ_TRANSPORT_OK;
    if (QUIC_FAILED(c->api->StreamShutdown(
            ms->stream, QUIC_STREAM_SHUTDOWN_FLAG_ABORT_RECEIVE,
            error_code)))
        return MOQ_TRANSPORT_ERROR;
    return MOQ_TRANSPORT_OK;
}

/*
 * Datagram send. Borrow-during-call at the bridge, but MsQuic borrows
 * the bytes until a FINAL send state — copy into a record freed at
 * IS_FINAL. All refusals are the bridge's non-fatal datagram shapes
 * (it never retries datagrams; WOULD_BLOCK/DROPPED/TOO_LARGE all just
 * drop the object — lossy by contract).
 */
static moq_transport_result_t ep_send_datagram(void *ctx,
                                               const uint8_t *data,
                                               size_t len)
{
    moq_msquic_conn_t *c = ctx;

    if (c->conn == NULL)
        return MOQ_TRANSPORT_ERROR;
    /* one QUIC_BUFFER per record, and the record-plus-copy sizing must
     * not wrap: reject on length alone, before the transport, the
     * allocator, or the bytes are touched */
    if (len > UINT32_MAX ||
        len > SIZE_MAX - sizeof(struct moq_msq_dgram_rec))
        return MOQ_TRANSPORT_TOO_LARGE;
    if (!c->dgram_send_enabled || c->dgram_max == 0)
        return MOQ_TRANSPORT_DROPPED;
    if (len > c->dgram_max)
        return MOQ_TRANSPORT_TOO_LARGE;
    if (c->dgram_inflight >= MOQ_MSQ_DGRAM_INFLIGHT_MAX)
        return MOQ_TRANSPORT_WOULD_BLOCK;

    size_t rec_size = sizeof(struct moq_msq_dgram_rec) + len;
    struct moq_msq_dgram_rec *rec =
        c->alloc.alloc(rec_size, c->alloc.ctx);
    if (rec == NULL)
        return MOQ_TRANSPORT_DROPPED;
    rec->alloc_size = rec_size;
    memcpy(rec + 1, data, len);
    rec->buf.Buffer = (uint8_t *)(rec + 1);
    rec->buf.Length = (uint32_t)len;
    if (QUIC_FAILED(c->api->DatagramSend(c->conn, &rec->buf, 1,
                                         QUIC_SEND_FLAG_NONE, rec))) {
        /* not accepted: no final will fire — the record dies here, and
         * the refusal stays non-fatal (conn-level failures surface
         * through connection events) */
        c->alloc.free(rec, rec_size, c->alloc.ctx);
        return MOQ_TRANSPORT_DROPPED;
    }
    c->dgram_inflight++;
    return MOQ_TRANSPORT_OK;
}

static size_t ep_max_datagram_size(void *ctx)
{
    moq_msquic_conn_t *c = ctx;

    return c->dgram_send_enabled ? c->dgram_max : 0;
}

static moq_transport_result_t ep_close_transport(void *ctx, uint64_t code,
                                                 const uint8_t *reason,
                                                 size_t reason_len)
{
    moq_msquic_conn_t *c = ctx;

    (void)reason;
    (void)reason_len;
    batch_flush(c); /* staged data goes out ahead of the close */
    if (c->flush_failed)
        return MOQ_TRANSPORT_ERROR;
    if (c->conn == NULL)
        return MOQ_TRANSPORT_ERROR;
    /* A local close is a terminal cause in its own right: orderly, carrying
     * the application's code. Record it BEFORE telling the transport --
     * afterwards the connection delivers only SHUTDOWN_COMPLETE, whose
     * fallback would read this as an unclean death with code 0, which is
     * indistinguishable from a transport failure reporting 0. First-wins, as
     * at every other initiating site: a cause already recorded stands. */
    if (!c->close_pending) {
        c->close_pending = true;
        c->close_clean = true;
        c->close_code = code;
    }
    /* async, never blocks; SHUTDOWN_COMPLETE follows on the worker */
    c->api->ConnectionShutdown(c->conn,
                               QUIC_CONNECTION_SHUTDOWN_FLAG_NONE, code);
    return MOQ_TRANSPORT_OK;
}

/* --- stream events (MsQuic -> bridge) ----------------------------------------- */

static void feed_stream_bytes(moq_msquic_conn_t *c,
                              struct moq_msq_stream *ms,
                              const uint8_t *data, size_t len, bool fin)
{
    if (ms->fin_fed)
        return;
    if (fin)
        ms->fin_fed = true;

    /* control routing is profile-dependent: bidi-control profiles
     * (draft-16) carry control on the first client-initiated bidi
     * stream; uni-control-pair profiles classify control among the
     * unidirectional streams inside the bridge */
    if (ms->is_bidi &&
        !moq_transport_bridge_uses_uni_control(c->bridge)) {
        if (!c->ctrl_latched && (ms->id & 0x1u) == 0u) {
            c->ctrl_id = ms->id;
            c->ctrl_latched = true;
        }
        if (c->ctrl_latched && ms->id == c->ctrl_id) {
            (void)moq_transport_bridge_on_peer_control_bytes(
                c->bridge, ms->id, data, len, fin, now_us());
            return;
        }
    }
    if (ms->is_bidi)
        (void)moq_transport_bridge_on_peer_bidi_bytes(
            c->bridge, ms->id, data, len, fin, now_us());
    else
        (void)moq_transport_bridge_on_peer_uni_bytes(
            c->bridge, ms->id, data, len, fin, now_us());
}

static QUIC_STATUS stream_event_locked(HQUIC stream, void *ctx,
                                       QUIC_STREAM_EVENT *ev)
{
    struct moq_msq_stream *ms = ctx;
    moq_msquic_conn_t *c = ms->conn;

    switch (ev->Type) {
    case QUIC_STREAM_EVENT_START_COMPLETE:
        if (QUIC_FAILED(ev->START_COMPLETE.Status)) {
            /* the credit gate should make this unreachable; if the
             * transport refuses a start anyway, the bridge already
             * owns the id — nothing recoverable remains */
            (void)moq_transport_bridge_on_transport_error(
                c->bridge, (uint64_t)ev->START_COMPLETE.Status,
                now_us());
        } else if (ev->START_COMPLETE.ID != ms->id) {
            /* self-computed id diverged from the transport's — every
             * byte routed so far went to the wrong stream */
            (void)moq_transport_bridge_on_transport_error(
                c->bridge, ev->START_COMPLETE.ID, now_us());
        }
        conn_service(c);
        break;

    case QUIC_STREAM_EVENT_RECEIVE: {
        bool fin = (ev->RECEIVE.Flags & QUIC_RECEIVE_FLAG_FIN) != 0;

        /*
         * Inbound backpressure, done the documented MsQuic way.
         * StreamReceiveSetEnabled(FALSE) is asynchronous — it delegates
         * to the worker, and any RECEIVE already queued still fires — so
         * it cannot arrest delivery on its own. Instead, while the bridge
         * is backed up on this stream we REJECT the receive synchronously
         * by accepting zero bytes (RECEIVE.TotalBufferLength is inout;
         * setting it to 0 tells MsQuic the app consumed nothing). MsQuic
         * then holds the unconsumed bytes in its own buffer (flow-control
         * backpressure to the peer) and stops indicating RECEIVE until a
         * StreamReceiveSetEnabled(TRUE). The rejected bytes never enter
         * the bridge, so nothing is duplicated, and the redelivery after
         * resume carries them exactly once. Default (non-multi-receive)
         * mode: at most one RECEIVE is outstanding at a time, so a single
         * paused-stream reject reliably arrests the stream.
         */
        if (ms->paused) {
            if (fin && ev->RECEIVE.TotalBufferLength == 0) {
                /* a pure zero-byte FIN carries no data to hold and is the
                 * stream's terminal marker — deliver it even while paused
                 * (0 bytes indicated: accepting 0 accepts it in full) */
                feed_stream_bytes(c, ms, NULL, 0, true);
            } else {
                ev->RECEIVE.TotalBufferLength = 0; /* hold + disable */
                ms->recv_disabled = true;
            }
            conn_service(c);
            break;
        }

        /*
         * Feed the buffers in order, but STOP at the first one that
         * leaves the bridge pending: the remaining buffers must NOT be
         * fed onward (on a control/bidi stream appending past a pending
         * frame can break framing — protocol-fatal). Instead they are
         * held by MsQuic (TotalBufferLength lowered to the accepted
         * prefix) for exactly-once redelivery after resume. The per-
         * buffer feed is all-or-nothing, so the buffer that caused the
         * pending state was itself retained by the bridge and counts as
         * accepted; only the buffers after it are held.
         */
        uint64_t accepted = 0;
        bool now_pending = false;
        for (uint32_t i = 0; i < ev->RECEIVE.BufferCount; i++) {
            const QUIC_BUFFER *b = &ev->RECEIVE.Buffers[i];
            bool last = (i + 1 == ev->RECEIVE.BufferCount);

            feed_stream_bytes(c, ms, b->Buffer, b->Length, fin && last);
            accepted += b->Length;
            if (moq_transport_bridge_stream_has_pending(c->bridge,
                                                        ms->id)) {
                now_pending = true;
                break; /* leave buffers i+1.. with MsQuic */
            }
        }
        if (ev->RECEIVE.BufferCount == 0 && fin)
            feed_stream_bytes(c, ms, NULL, 0, true);
        /* If a buffer left the bridge pending, mark the stream paused (the
         * NEXT RECEIVE is rejected wholesale above) and hold any buffers
         * we stopped short of. conn_service() below may drain the bridge
         * and clear paused again in the same pass, so a stream is only
         * left paused while the bridge genuinely cannot take more. */
        if (now_pending && ms->stream != NULL) {
            ms->paused = true;
            if (accepted < ev->RECEIVE.TotalBufferLength) {
                ev->RECEIVE.TotalBufferLength = accepted; /* hold the rest */
                ms->recv_disabled = true;
            }
        }
        conn_service(c);
        break;
    }

    case QUIC_STREAM_EVENT_PEER_SEND_SHUTDOWN:
        /* the receive direction is ending (FIN): no further RECEIVE will
         * come, so there is nothing to resume — clear any pause/hold
         * state before servicing so the pass does not re-enable a
         * finished stream */
        ms->paused = false;
        ms->recv_disabled = false;
        feed_stream_bytes(c, ms, NULL, 0, true);
        conn_service(c);
        break;

    case QUIC_STREAM_EVENT_PEER_SEND_ABORTED:
        /* the peer aborted its send: the receive direction is terminal.
         * Clear pause/hold state FIRST so the service pass does not try to
         * resume (StreamReceiveSetEnabled(TRUE)) a torn-down receive — a
         * resume failure must never turn a valid peer reset into a
         * transport fatal. */
        ms->paused = false;
        ms->recv_disabled = false;
        (void)moq_transport_bridge_on_peer_stream_reset(
            c->bridge, ms->id, ev->PEER_SEND_ABORTED.ErrorCode,
            now_us());
        conn_service(c);
        break;

    case QUIC_STREAM_EVENT_PEER_RECEIVE_ABORTED:
        (void)moq_transport_bridge_on_peer_stop_sending(
            c->bridge, ms->id, ev->PEER_RECEIVE_ABORTED.ErrorCode,
            now_us());
        conn_service(c);
        break;

    case QUIC_STREAM_EVENT_SEND_COMPLETE: {
        struct moq_msq_batch_rec *rec =
            ev->SEND_COMPLETE.ClientContext;

        if (rec != NULL) {
            rec->ms->inflight_bytes -= rec->bytes;
            c->pending_sends--;
            batch_release_entries(c, rec);
            batch_put(c, rec);
        }
        /* a completion frees send budget: retry anything the bridge
         * held and give the application its pacing slot */
        conn_service(c);
        break;
    }

    case QUIC_STREAM_EVENT_IDEAL_SEND_BUFFER_SIZE: {
        uint64_t ideal = ev->IDEAL_SEND_BUFFER_SIZE.ByteCount;
        bool grew = ideal > ms->ideal_send;

        ms->ideal_send = ideal;
        /* a raised ceiling can admit sends the budget parked, and
         * nothing else will retry them promptly: with the peer fully
         * caught up there is no reverse traffic, so the batch's
         * SEND_COMPLETE (== full ACK) sits behind the peer's delayed-
         * ACK timer. A shrunk ceiling unblocks nothing — skip it. */
        if (grew)
            conn_service(c);
        break;
    }

    case QUIC_STREAM_EVENT_SHUTDOWN_COMPLETE:
        /* all this stream's events (completions included) precede
         * this one; the handle dies here, the bookkeeping with it */
        if (ms->stream != NULL &&
            !ev->SHUTDOWN_COMPLETE.AppCloseInProgress)
            c->api->StreamClose(ms->stream);
        stream_free(c, ms);
        /* a freed stream may unblock a pending open via credit */
        conn_service(c);
        break;

    default:
        break;
    }
    (void)stream;
    return QUIC_STATUS_SUCCESS;
}

static QUIC_STATUS stream_event_cb(HQUIC stream, void *ctx,
                                   QUIC_STREAM_EVENT *ev)
{
    moq_msquic_conn_t *c = ((struct moq_msq_stream *)ctx)->conn;
    QUIC_STATUS rc;

    guard_enter(c);
    rc = stream_event_locked(stream, ctx, ev);
    guard_leave(c);
    return rc;
}

/* --- connection events (MsQuic -> bridge) ------------------------------------- */

static QUIC_STATUS conn_event_locked(HQUIC conn, void *ctx,
                                     QUIC_CONNECTION_EVENT *ev)
{
    moq_msquic_conn_t *c = ctx;

    switch (ev->Type) {
    case QUIC_CONNECTION_EVENT_CONNECTED:
        if (!c->started) {
            c->started = true;
            /* symmetric-start profiles start both sides; bidi-control
             * starts the client only (the server is reactive) */
            if (c->is_client ||
                moq_transport_bridge_uses_uni_control(c->bridge))
                (void)moq_session_start(c->session, now_us());
        }
        conn_service(c);
        break;

    case QUIC_CONNECTION_EVENT_STREAMS_AVAILABLE:
        c->bidi_credit = ev->STREAMS_AVAILABLE.BidirectionalCount;
        c->uni_credit = ev->STREAMS_AVAILABLE.UnidirectionalCount;
        c->credit_known = true;
        /* fresh credit may unblock a parked open */
        conn_service(c);
        break;

    case QUIC_CONNECTION_EVENT_PEER_STREAM_STARTED: {
        HQUIC h = ev->PEER_STREAM_STARTED.Stream;
        bool bidi = (ev->PEER_STREAM_STARTED.Flags &
                     QUIC_STREAM_OPEN_FLAG_UNIDIRECTIONAL) == 0;
        uint64_t id = 0;
        uint32_t id_len = sizeof(id);

        if (QUIC_FAILED(c->api->GetParam(h, QUIC_PARAM_STREAM_ID,
                                         &id_len, &id)))
            return QUIC_STATUS_INTERNAL_ERROR;

        struct moq_msq_stream *ms = stream_new(c, id, bidi, false);
        if (ms == NULL)
            return QUIC_STATUS_OUT_OF_MEMORY;
        ms->stream = h;
        c->api->SetCallbackHandler(h, moq_msq_stream_cb_ptr(stream_event_cb), ms);
        break;
    }

    case QUIC_CONNECTION_EVENT_SHUTDOWN_INITIATED_BY_TRANSPORT:
        if (!c->close_pending) {
            c->close_pending = true;
            c->close_clean = false;
            c->close_code =
                ev->SHUTDOWN_INITIATED_BY_TRANSPORT.ErrorCode;
        }
        conn_service(c);
        break;

    case QUIC_CONNECTION_EVENT_SHUTDOWN_INITIATED_BY_PEER:
        /* the peer's application close: the session layer judges the
         * code; the transport terminal itself is orderly */
        if (!c->close_pending) {
            c->close_pending = true;
            c->close_clean = true;
            c->close_code = ev->SHUTDOWN_INITIATED_BY_PEER.ErrorCode;
        }
        conn_service(c);
        break;

    case QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE:
        /* the last event this connection will ever deliver */
        if (!c->close_pending) {
            c->close_pending = true;
            c->close_clean = false;
            c->close_code = 0;
        }
        c->shutdown_complete = true;
        conn_service(c);
        break;

    case QUIC_CONNECTION_EVENT_DATAGRAM_STATE_CHANGED:
        c->dgram_send_enabled =
            ev->DATAGRAM_STATE_CHANGED.SendEnabled;
        c->dgram_max = ev->DATAGRAM_STATE_CHANGED.MaxSendLength;
        conn_service(c);
        break;

    case QUIC_CONNECTION_EVENT_DATAGRAM_SEND_STATE_CHANGED:
        /* every accepted datagram gets exactly one FINAL state (ACKed,
         * lost-discarded, or canceled at shutdown), and MsQuic
         * delivers all of them before the connection's
         * SHUTDOWN_COMPLETE — so freeing here is complete teardown; no
         * sweep is needed at destroy */
        if (QUIC_DATAGRAM_SEND_STATE_IS_FINAL(
                ev->DATAGRAM_SEND_STATE_CHANGED.State)) {
            struct moq_msq_dgram_rec *rec =
                ev->DATAGRAM_SEND_STATE_CHANGED.ClientContext;

            if (rec != NULL) {
                c->alloc.free(rec, rec->alloc_size, c->alloc.ctx);
                c->dgram_inflight--;
            }
            /* a freed slot may unblock the application's pacing */
            conn_service(c);
        }
        break;

    case QUIC_CONNECTION_EVENT_DATAGRAM_RECEIVED:
        (void)moq_transport_bridge_on_peer_datagram(
            c->bridge, ev->DATAGRAM_RECEIVED.Buffer->Buffer,
            ev->DATAGRAM_RECEIVED.Buffer->Length, now_us());
        conn_service(c);
        break;

    default:
        break;
    }
    (void)conn;
    return QUIC_STATUS_SUCCESS;
}

static QUIC_STATUS conn_event_cb(HQUIC conn, void *ctx,
                                 QUIC_CONNECTION_EVENT *ev)
{
    moq_msquic_conn_t *c = ctx;
    QUIC_STATUS rc;

    guard_enter(c);
    rc = conn_event_locked(conn, ctx, ev);
    guard_leave(c);
    return rc;
}

/* --- public surface ------------------------------------------------------------ */

QUIC_CONNECTION_CALLBACK_HANDLER moq_msquic_conn_callback(void)
{
    return conn_event_cb;
}

void moq_msquic_conn_cfg_init_sized(moq_msquic_conn_cfg_t *cfg,
                                    size_t size)
{
    if (cfg == NULL || size < sizeof(*cfg))
        return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = (uint32_t)sizeof(*cfg);
}

moq_result_t moq_msquic_conn_create(const moq_msquic_conn_cfg_t *cfg,
                                    moq_msquic_conn_t **out)
{
    if (out == NULL)
        return MOQ_ERR_INVAL;
    *out = NULL;
    /* the size guard comes first: a too-small struct_size means the
     * caller's struct may not even contain the fields read below */
    if (cfg == NULL || cfg->struct_size < sizeof(*cfg))
        return MOQ_ERR_INVAL;
    if (cfg->alloc == NULL || cfg->session == NULL || cfg->api == NULL)
        return MOQ_ERR_INVAL;
    if (cfg->alloc->alloc == NULL || cfg->alloc->realloc == NULL ||
        cfg->alloc->free == NULL)
        return MOQ_ERR_INVAL;
    if ((cfg->guard_enter == NULL) != (cfg->guard_leave == NULL))
        return MOQ_ERR_INVAL; /* both or neither */

    moq_msquic_conn_t *c =
        cfg->alloc->alloc(sizeof(*c), cfg->alloc->ctx);
    if (c == NULL)
        return MOQ_ERR_NOMEM;
    memset(c, 0, sizeof(*c));
    c->alloc = *cfg->alloc;
    c->session = cfg->session;
    c->api = cfg->api;
    c->hook = cfg->hook;
    c->hook_user = cfg->hook_user;
    c->guard_enter = cfg->guard_enter;
    c->guard_leave = cfg->guard_leave;
    c->guard_user = cfg->guard_user;
    c->is_client = moq_session_perspective(cfg->session) ==
                   MOQ_PERSPECTIVE_CLIENT;
    c->ctrl_id = UINT64_MAX;

    c->ops = (moq_transport_endpoint_ops_t){
        .struct_size = sizeof(moq_transport_endpoint_ops_t),
        .capabilities = MOQ_TRANSPORT_CAP_WRITE_PAYLOAD |
                        MOQ_TRANSPORT_CAP_DATAGRAM,
        .open_uni = ep_open_uni,
        .open_bidi = ep_open_bidi,
        .write = ep_write,
        .write_payload = ep_write_payload,
        .reset_stream = ep_reset_stream,
        .stop_sending = ep_stop_sending,
        .send_datagram = ep_send_datagram,
        .max_datagram_size = ep_max_datagram_size,
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

void moq_msquic_conn_destroy(moq_msquic_conn_t *c)
{
    if (c == NULL)
        return;
    moq_alloc_t alloc = c->alloc;

    while (c->streams != NULL)
        stream_free(c, c->streams);
    if (c->building != NULL) {
        /* defensive: every service pass flushes before returning, so
         * a staged batch here means the invariant broke — release the
         * ownership rather than leak it */
        batch_release_entries(c, c->building);
        batch_put(c, c->building);
        c->building = NULL;
    }
    while (c->rec_free != NULL) {
        struct moq_msq_batch_rec *rec = c->rec_free;

        c->rec_free = rec->next;
        alloc.free(rec, sizeof(*rec), alloc.ctx);
    }
    moq_transport_bridge_destroy(c->bridge);
    alloc.free(c, sizeof(*c), alloc.ctx);
}

moq_result_t moq_msquic_conn_bind(moq_msquic_conn_t *c, HQUIC connection)
{
    if (c == NULL || connection == NULL)
        return MOQ_ERR_INVAL;
    if (c->conn != NULL)
        return MOQ_ERR_WRONG_STATE;
    c->conn = connection;
    return MOQ_OK;
}

void moq_msquic_conn_service(moq_msquic_conn_t *c)
{
    if (c != NULL)
        conn_service(c);
}

void moq_msq_conn_service_wake(moq_msquic_conn_t *c)
{
    if (c->in_service) {
        c->service_pending = true;
        return;
    }
    /* anything terminal-ish pending takes the full, ordered path */
    if ((c->close_pending && !c->close_fed) || c->flush_failed ||
        (!c->teardown_sent && c->conn != NULL &&
         moq_transport_bridge_is_fatal(c->bridge))) {
        conn_service(c);
        return;
    }
    if (c->hook == NULL)
        return;
    c->in_service = true;
    c->hook(c, c->hook_user);
    /* a close recorded during the pump is fed BEFORE the pass that
     * would service anything the pump queued — same ordering as the
     * full path's hook section */
    conn_feed_close(c);
    (void)moq_transport_bridge_service(c->bridge, now_us());
    batch_flush(c);
    if (c->flush_failed) {
        c->flush_failed = false;
        (void)moq_transport_bridge_on_transport_error(c->bridge, 0x1,
                                                      now_us());
    }
    /* resume before the teardown check so a failed resume (which feeds a
     * transport error) is torn down in this same pass */
    (void)resume_paused(c);
    if (!c->teardown_sent && c->conn != NULL &&
        moq_transport_bridge_is_fatal(c->bridge)) {
        c->teardown_sent = true;
        c->api->ConnectionShutdown(
            c->conn, QUIC_CONNECTION_SHUTDOWN_FLAG_NONE,
            moq_transport_bridge_fatal_code(c->bridge));
    }
    bool rerun = c->service_pending || (c->close_pending && !c->close_fed);
    c->in_service = false;
    if (rerun)
        conn_service(c);
}

/* The managed doorbell's re-drive predicate reads these across a whole
 * pump+service cycle: it snapshots the token before the app pump and, after
 * servicing, re-arms one pump when the token moved (events enqueued or dequeued)
 * AND events still remain. Thin private-SPI pass-throughs to the bridge. */
uint64_t moq_msq_conn_event_token(const moq_msquic_conn_t *c)
{
    return moq_transport_bridge_event_progress_token(c->bridge);
}

bool moq_msq_conn_has_events(const moq_msquic_conn_t *c)
{
    return moq_transport_bridge_has_events(c->bridge);
}

moq_session_t *moq_msquic_conn_session(moq_msquic_conn_t *c)
{
    return c != NULL ? c->session : NULL;
}

HQUIC moq_msquic_conn_connection(moq_msquic_conn_t *c)
{
    return c != NULL ? c->conn : NULL;
}

int moq_msquic_conn_pending_sends(const moq_msquic_conn_t *c)
{
    return c != NULL ? c->pending_sends : 0;
}

int moq_msquic_conn_pending_datagrams(const moq_msquic_conn_t *c)
{
    return c != NULL ? c->dgram_inflight : 0;
}

bool moq_msquic_conn_is_fatal(const moq_msquic_conn_t *c)
{
    return c != NULL && moq_transport_bridge_is_fatal(c->bridge);
}

uint64_t moq_msquic_conn_fatal_code(const moq_msquic_conn_t *c)
{
    return c != NULL ? moq_transport_bridge_fatal_code(c->bridge) : 0;
}

bool moq_msquic_conn_is_closed(const moq_msquic_conn_t *c)
{
    return c != NULL && moq_transport_bridge_is_closed(c->bridge);
}

uint64_t moq_msquic_conn_close_code(const moq_msquic_conn_t *c)
{
    return c != NULL ? moq_transport_bridge_close_code(c->bridge) : 0;
}

void moq_msquic_settings_init(QUIC_SETTINGS *settings)
{
    if (settings == NULL)
        return;
    memset(settings, 0, sizeof(*settings));
    /* mandatory: send ownership + budget + drain all assume
     * SEND_COMPLETE == full acknowledgment */
    settings->SendBufferingEnabled = FALSE;
    settings->IsSet.SendBufferingEnabled = TRUE;
    /*
     * mandatory: default (non-multi) receive mode. The inbound-backpressure
     * design depends on it — default mode indicates at most one RECEIVE at
     * a time and disables further RECEIVE as soon as the app accepts a
     * receive only partially, which is exactly how the adapter arrests a
     * backed-up stream (accept zero bytes in the RECEIVE callback, see
     * stream_event_locked). Multi-receive would keep indicating RECEIVE
     * after a partial accept and require cross-event TotalBufferLength
     * bookkeeping. It is a preview-gated MsQuic feature, so the QUIC_SETTINGS
     * field only exists (and can only be enabled) under
     * QUIC_API_ENABLE_PREVIEW_FEATURES — stamp it OFF explicitly (IsSet)
     * there so a preview-build attach caller that merges these settings
     * cannot silently leave it enabled. Stable-ABI callers cannot set it at
     * all (the field is absent), so it is already forced off for them.
     */
#ifdef QUIC_API_ENABLE_PREVIEW_FEATURES
    settings->StreamMultiReceiveEnabled = FALSE;
    settings->IsSet.StreamMultiReceiveEnabled = TRUE;
#endif
    settings->DatagramReceiveEnabled = TRUE;
    settings->IsSet.DatagramReceiveEnabled = TRUE;
    /* MoQ-scale stream counts and media-scale flow control windows */
    settings->PeerBidiStreamCount = 129;
    settings->IsSet.PeerBidiStreamCount = TRUE;
    settings->PeerUnidiStreamCount = 131;
    settings->IsSet.PeerUnidiStreamCount = TRUE;
    settings->StreamRecvWindowDefault = 16u * 1024u * 1024u;
    settings->IsSet.StreamRecvWindowDefault = TRUE;
    settings->ConnFlowControlWindow = 32u * 1024u * 1024u;
    settings->IsSet.ConnFlowControlWindow = TRUE;
}

/* --- white-box seams (tests only, never shipped) ------------------------------- */

#ifdef MOQ_MSQUIC_TESTING
const moq_transport_endpoint_ops_t *moq_msquic_test_ops(
    moq_msquic_conn_t *conn)
{
    return &conn->ops;
}

void moq_msquic_test_flush(moq_msquic_conn_t *conn)
{
    batch_flush(conn);
}

QUIC_STREAM_CALLBACK_HANDLER moq_msquic_test_stream_callback(void)
{
    return stream_event_cb;
}

struct moq_msq_stream *moq_msquic_test_stream_find(moq_msquic_conn_t *conn,
                                                   uint64_t id)
{
    return stream_find(conn, id);
}
#endif
