/*
 * moq_pq_send_gate.h -- bounded queued-send gate for the picoquic endpoints.
 *
 * picoquic_add_to_stream() appends to a per-stream send_queue by tail-walking
 * the singly-linked list, so an unbounded backlog turns a deep single stream
 * into accidental O(n^2) and starves the packet loop that would drain it. This
 * gate bounds the bytes we keep buffered in picoquic: once the estimated
 * backlog would exceed a cap, the endpoint returns MOQ_TRANSPORT_WOULD_BLOCK and
 * the transport bridge retains the action and retries as the queue drains.
 *
 * Accounting is endpoint-local and saturating (no picoquic private state, so it
 * is safe in installed-picoquic builds too):
 *   - queued  : bytes we have handed picoquic that it has not yet formatted.
 *   - last_sent: picoquic_get_data_sent() at the previous observation. That
 *     counter advances as picoquic formats queued stream bytes into packets and
 *     frees send_queue nodes -- the exact signal we need to bound the queue.
 * Each write folds the monotonic delta back into queued, clamped by min() so
 * unrelated stream activity on the same connection (e.g. WebTransport/H3 bytes)
 * can only under-estimate the backlog, never underflow it.
 *
 * The cap defaults to 1 MiB, overridable per endpoint by the internal env var
 * MOQ_PQ_STREAM_QUEUE_BYTES -- a test/ops escape hatch, NOT public API and not
 * documented in installed headers.
 */
#ifndef MOQ_PQ_SEND_GATE_H
#define MOQ_PQ_SEND_GATE_H

#include <picoquic.h>
#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>

#define MOQ_PQ_STREAM_QUEUE_CAP_DEFAULT ((uint64_t)(1u << 20))  /* 1 MiB */

typedef struct {
    uint64_t cap;        /* max queued backlog before WOULD_BLOCK */
    uint64_t queued;     /* estimate still buffered in picoquic */
    uint64_t last_sent;  /* picoquic_get_data_sent() last observed */
} moq_pq_send_gate_t;

static inline uint64_t moq_pq_send_gate_read_cap(void)
{
    const char *e = getenv("MOQ_PQ_STREAM_QUEUE_BYTES");
    if (e && *e) {
        char *end = NULL;
        unsigned long long v = strtoull(e, &end, 10);
        if (end && *end == '\0' && v > 0)
            return (uint64_t)v;
    }
    return MOQ_PQ_STREAM_QUEUE_CAP_DEFAULT;
}

static inline void moq_pq_send_gate_init(moq_pq_send_gate_t *g,
                                         picoquic_cnx_t *cnx)
{
    g->cap = moq_pq_send_gate_read_cap();
    g->queued = 0;
    /* Baseline against the connection's current sent count so pre-existing
     * stream traffic (handshake/control) is never counted as our backlog. */
    g->last_sent = picoquic_get_data_sent(cnx);
}

/*
 * Fold picoquic's drain progress into the estimate, then decide this write.
 * Returns non-zero when the write must be refused (WOULD_BLOCK) WITHOUT calling
 * picoquic_add_to_stream. The cap bounds the BACKLOG, not a single write: a
 * write onto an empty backlog always proceeds (so an object larger than the cap
 * is never permanently blocked), and a zero-length FIN-only write always
 * proceeds (it adds no send_queue node).
 */
static inline int moq_pq_send_gate_would_block(moq_pq_send_gate_t *g,
                                               picoquic_cnx_t *cnx, size_t len)
{
    uint64_t now = picoquic_get_data_sent(cnx);
    uint64_t delta = now - g->last_sent;   /* monotonic counter: delta >= 0 */
    g->last_sent = now;
    uint64_t drained = delta < g->queued ? delta : g->queued;
    g->queued -= drained;

    /* Empty backlog or FIN-only always proceeds. Otherwise refuse once the
     * backlog would exceed the cap, written to avoid queued + len overflowing
     * (queued can exceed the cap after an oversized write). */
    if (len == 0 || g->queued == 0)
        return 0;
    if (g->queued >= g->cap)
        return 1;
    return (uint64_t)len > g->cap - g->queued;
}

/* Record a successful picoquic_add_to_stream of `len` bytes. */
static inline void moq_pq_send_gate_on_added(moq_pq_send_gate_t *g, size_t len)
{
    g->queued += (uint64_t)len;
}

#endif /* MOQ_PQ_SEND_GATE_H */
