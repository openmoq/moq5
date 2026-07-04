/*
 * moq_pq_send_queue.h -- adapter-owned outbound queue for picoquic pull sending.
 *
 * The picoquic endpoints send MoQ stream data with picoquic's "just in time"
 * (pull) API: the endpoint keeps the bytes in this queue, marks the stream
 * active, and copies them into picoquic's packet buffer from the
 * picoquic_callback_prepare_to_send callback, only when the transport is ready
 * to put them on the wire.
 *
 * The queue owns what it holds:
 *   - copied chunks   (borrowed bridge bytes: control/header/small literals),
 *   - rcbuf chunks    (payload writes; the rcbuf is retained until drained),
 *   - a FIN flag on the final chunk (a zero-length chunk carries a bare FIN).
 * All are released exactly once on drain, drop, or destroy.
 *
 * An aggregate byte cap bounds the buffered backlog: a push over the cap is
 * refused so the bridge retains and retries, except a push onto an empty
 * aggregate always succeeds so a single object larger than the cap is never
 * permanently blocked.
 *
 * Transport-agnostic (no picoquic dependency): the endpoint owns the picoquic
 * glue (mark_active_stream / provide_stream_data_buffer). Single-threaded:
 * confined to the network thread like the rest of the adapter.
 */
#ifndef MOQ_PQ_SEND_QUEUE_H
#define MOQ_PQ_SEND_QUEUE_H

#include <moq/rcbuf.h>
#include <moq/types.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define MOQ_PQ_SEND_QUEUE_CAP_DEFAULT ((uint64_t)(1u << 20))  /* 1 MiB */

typedef struct moq_pq_send_queue moq_pq_send_queue_t;

/* Create with an aggregate byte cap (0 -> MOQ_PQ_SEND_QUEUE_CAP_DEFAULT, also
 * overridable by the internal env var MOQ_PQ_STREAM_QUEUE_BYTES). Returns NULL
 * on allocation failure. */
moq_pq_send_queue_t *moq_pq_send_queue_create(const moq_alloc_t *alloc,
                                              uint64_t cap);

/* Release every queued chunk (decref rcbufs, free copied buffers) and the
 * queue itself. */
void moq_pq_send_queue_destroy(moq_pq_send_queue_t *q);

/* Append a copy of `data[0..len)` to stream `sid`. `fin` marks it as the
 * stream's final chunk. Returns 1 if accepted, 0 if the aggregate cap would be
 * exceeded (the caller returns WOULD_BLOCK), -1 on allocation failure. */
int moq_pq_send_queue_push_copy(moq_pq_send_queue_t *q, uint64_t sid,
                                const uint8_t *data, size_t len, bool fin);

/* Append a retained reference to `buf` (increfs on accept) to stream `sid`.
 * Returns 1 accepted, 0 cap-full, -1 on allocation failure. On non-accept the
 * caller keeps ownership of its own reference (this did not incref). */
int moq_pq_send_queue_push_rcbuf(moq_pq_send_queue_t *q, uint64_t sid,
                                 moq_rcbuf_t *buf, bool fin);

/* True if `sid` has undrained bytes or a pending FIN. */
bool moq_pq_send_queue_has_data(moq_pq_send_queue_t *q, uint64_t sid);

/* Plan the next provide for `sid` given picoquic's max buffer `max`. Sets
 * *nbytes (<= max) to copy now, *is_fin true iff this send delivers the stream
 * FIN, *still_active true iff bytes or a FIN remain after this send. Returns
 * true if the stream has anything to provide, false if nothing is queued (the
 * caller should renege with provide(0,0,0)). Does not mutate. */
bool moq_pq_send_queue_plan(moq_pq_send_queue_t *q, uint64_t sid, size_t max,
                            size_t *nbytes, bool *is_fin, bool *still_active);

/* Copy exactly `nbytes` (as returned by the matching plan) into `dst`,
 * advancing and popping chunks and releasing each as it is fully consumed. */
void moq_pq_send_queue_commit(moq_pq_send_queue_t *q, uint64_t sid,
                              uint8_t *dst, size_t nbytes);

/* Drop all queued bytes/FIN for `sid` (reset/stop/close), releasing chunks. */
void moq_pq_send_queue_drop(moq_pq_send_queue_t *q, uint64_t sid);

/* Aggregate bytes currently buffered across all streams (test/telemetry). */
uint64_t moq_pq_send_queue_queued_bytes(const moq_pq_send_queue_t *q);

/* Telemetry: peak aggregate backlog, and pushes refused for hitting the cap. */
uint64_t moq_pq_send_queue_high_water(const moq_pq_send_queue_t *q);
uint64_t moq_pq_send_queue_would_block_count(const moq_pq_send_queue_t *q);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_PQ_SEND_QUEUE_H */
