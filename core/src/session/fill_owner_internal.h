#ifndef MOQ_FILL_OWNER_INTERNAL_H
#define MOQ_FILL_OWNER_INTERNAL_H

#include "moq/types.h"

/* The fill child's ownership table: allocation-free state for every fill a
 * subscription owns, on either side. No request dispatch, buffers, queue
 * admission, clock, wire decoding or public fill API live here. The
 * initiating ID has already passed the session's non-recycling request-ID
 * ledger. */
#define MOQ_FILL_PARENT_SUBSCRIBE 1u
#define MOQ_FILL_PARENT_PUBLISH 2u
#define MOQ_FILL_SUBSCRIBER 1u
#define MOQ_FILL_PUBLISHER 2u
#define MOQ_FILL_OUTCOME_STREAM 1u
#define MOQ_FILL_OUTCOME_NO_STREAM 2u

typedef struct moq_fill_parent {
    uint32_t family;
    uint64_t slot;
    uint64_t generation;
} moq_fill_parent_t;

typedef struct moq_fill_ref {
    size_t slot;
    uint64_t generation;
    moq_fill_parent_t parent;
} moq_fill_ref_t;

/* Immutable resolved delivery snapshot. NO_STREAM is a positive conclusion
 * from the later snapshot resolver (paused/empty/no content), never inferred
 * here from coordinates, silence, an ACK, or fill_timeout_ms. Coordinates are
 * absolute/inclusive for STREAM; all discriminator storage is fixed-width.
 * Positive range filters are outside this primitive and advertised capacity. */
typedef struct moq_fill_delivery {
    uint32_t outcome;
    uint32_t group_order; /* STREAM: 1 ascending, 2 descending */
    uint64_t start_group;
    uint64_t start_object;
    uint64_t end_group;
    uint64_t end_object;
    uint64_t processed_at_us;
    uint64_t fill_timeout_ms;
    uint8_t subscriber_priority;
    bool has_fill_timeout;
} moq_fill_delivery_t;

#define MOQ_FILL_CONTROL_PENDING 0u
#define MOQ_FILL_CONTROL_ACCEPTED 1u
#define MOQ_FILL_CONTROL_REJECTED 2u
#define MOQ_FILL_CONTROL_RESET 3u

#define MOQ_FILL_TERM_NONE 0u
#define MOQ_FILL_TERM_COMPLETE 1u
#define MOQ_FILL_TERM_NO_STREAM 2u
#define MOQ_FILL_TERM_CANCELLED 3u
#define MOQ_FILL_TERM_ABORTED 4u
#define MOQ_FILL_TERM_RESET 5u
#define MOQ_FILL_TERM_PARENT_CANCELLED 6u
#define MOQ_FILL_TERM_CONTROL_REJECTED 7u

#define MOQ_FILL_ACTION_NONE 0u
#define MOQ_FILL_ACTION_HEADER 1u
#define MOQ_FILL_ACTION_FIN 2u
#define MOQ_FILL_ACTION_RESET 3u
#define MOQ_FILL_ACTION_STOP 4u

typedef struct moq_fill_entry {
    uint64_t generation; /* retained on reuse; exhausted UINT64_MAX never wraps */
    bool active;
    moq_fill_parent_t parent;
    uint64_t initiating_id;
    uint32_t role;
    uint32_t control;
    bool delivery_known;
    moq_fill_delivery_t delivery;
    bool stream_bound;
    moq_stream_ref_t stream;
    bool data_fin;
    bool data_reset;
    /* A receiving child whose stream we stopped, retired by the transport
     * with no peer FIN or RESET ever reaching us: its stream ended, but this
     * says nothing of how (no COMPLETE, no stream error). */
    bool data_local_retired;
    bool stop_committed;
    bool peer_stop;
    bool cancel_requested;
    bool abort_requested;
    bool finish_requested;
    bool parent_cancelled;
    bool stream_accounted;
    uint32_t terminal_reason;
    bool terminal_published;
    bool terminal_retired;
} moq_fill_entry_t;

typedef struct moq_fill_table {
    moq_fill_entry_t *entries;
    size_t capacity;
    size_t live;
} moq_fill_table_t;

/* Pure reservation ticket; preparing neither reserves a live slot nor advances
 * generation. The enclosing transaction reserves all other effects, rechecks
 * this ticket, then commits without intervening session-owner work. */
typedef struct moq_fill_reservation {
    moq_fill_ref_t ref;
    uint64_t initiating_id;
    uint32_t role;
    bool delivery_known;
    moq_fill_delivery_t delivery;
} moq_fill_reservation_t;

typedef struct moq_fill_work {
    uint32_t action;
    bool account_stream;
    bool publish_terminal;
    uint32_t terminal_reason;
    bool reclaimable;
} moq_fill_work_t;

/* Zero capacity is valid but cannot admit. Overflow/invalid storage changes
 * neither table nor storage. Successful init clears caller-owned entries. */
moq_result_t moq_fill_table_init(moq_fill_table_t *t,
    moq_fill_entry_t *storage, size_t capacity);
moq_result_t moq_fill_prepare(const moq_fill_table_t *t,
    moq_fill_parent_t parent, uint64_t initiating_id, uint32_t role,
    const moq_fill_delivery_t *delivery, moq_fill_reservation_t *out);
moq_result_t moq_fill_check_commit(const moq_fill_table_t *t,
    const moq_fill_reservation_t *reservation);
/* Locally sent subscriber child / accepted inbound publisher child. No
 * response is inferred; matching control resolution is a separate fact. */
moq_result_t moq_fill_commit(moq_fill_table_t *t,
    const moq_fill_reservation_t *reservation);
moq_result_t moq_fill_find(const moq_fill_table_t *t,
    uint64_t initiating_id, moq_fill_ref_t *out);
moq_result_t moq_fill_read(const moq_fill_table_t *t,
    moq_fill_ref_t ref, moq_fill_entry_t *out);

/* Match both child/parent generations AND initiating ID. Response is received
 * for subscriber or successfully queued for publisher. Duplicate response is
 * refused. Control reset means actual parent request cancellation settlement,
 * never a half-FIN. Neither operation permits late-header identity eviction. */
moq_result_t moq_fill_control_response(moq_fill_table_t *t,
    moq_fill_ref_t ref, uint64_t initiating_id, bool accepted);
moq_result_t moq_fill_control_reset(moq_fill_table_t *t,
    moq_fill_ref_t ref, uint64_t initiating_id);
/* Freeze previously unknown delivery after the snapshot resolver can prove it.
 * Idempotent identical resolution; changed snapshots or NO_STREAM after a
 * header are refused. NO_STREAM becomes a terminal cause only with accepted
 * control, never while pending or instead of an earlier cancellation/rejection.
 * No unresolved metadata is guessed from the parent. */
moq_result_t moq_fill_resolve_delivery(moq_fill_table_t *t,
    moq_fill_ref_t ref, uint64_t initiating_id,
    const moq_fill_delivery_t *delivery);

/* Local intent, distinct from successfully queued actions. Caller must reserve
 * a complete explicit API output transaction before committing its intent;
 * teardown sweeps may retain intent across refused queues. Parent cancellation
 * is per child and never dereferences or mutates a parent/sibling. */
moq_result_t moq_fill_cancel(moq_fill_table_t *t, moq_fill_ref_t ref);
moq_result_t moq_fill_abort(moq_fill_table_t *t, moq_fill_ref_t ref);
moq_result_t moq_fill_finish(moq_fill_table_t *t, moq_fill_ref_t ref);
moq_result_t moq_fill_parent_cancel(moq_fill_table_t *t, moq_fill_ref_t ref);

/* Bind a received header (subscriber) or commit a queued HEADER (publisher).
 * Same-reference retry is idempotent, a different stream is refused. References
 * must be nonzero and cannot be bound to another live child in this table. */
moq_result_t moq_fill_bind_received(moq_fill_table_t *t,
    moq_fill_ref_t ref, moq_stream_ref_t stream);
moq_result_t moq_fill_header_committed(moq_fill_table_t *t,
    moq_fill_ref_t ref, moq_stream_ref_t stream);
moq_result_t moq_fill_peer_fin(moq_fill_table_t *t,
    moq_fill_ref_t ref, moq_stream_ref_t stream);
moq_result_t moq_fill_peer_reset(moq_fill_table_t *t,
    moq_fill_ref_t ref, moq_stream_ref_t stream);
/* The bound stream of a receiving child we stopped ended without a peer
 * FIN or RESET (local receive retirement). Requires the matching child
 * (generation, parent), the bound stream ref and the committed local STOP;
 * keeps the terminal reason the cancellation recorded. A child whose peer
 * terminal is already in is unchanged. */
moq_result_t moq_fill_local_retired(moq_fill_table_t *t,
    moq_fill_ref_t ref, moq_stream_ref_t stream);
moq_result_t moq_fill_peer_stop(moq_fill_table_t *t,
    moq_fill_ref_t ref, moq_stream_ref_t stream);
moq_result_t moq_fill_action_committed(moq_fill_table_t *t,
    moq_fill_ref_t ref, uint32_t action);

/* Queries never advance progress. STOP alone cannot account a stream. The
 * integration increments the generation-pinned parent's counter only after
 * reserving that effect, then commits account_stream once. Counter overflow
 * and queued-buffer ownership remain the integration's responsibilities. */
moq_result_t moq_fill_next(const moq_fill_table_t *t,
    moq_fill_ref_t ref, moq_fill_work_t *out);
moq_result_t moq_fill_stream_accounted(moq_fill_table_t *t,
    moq_fill_ref_t ref);
moq_result_t moq_fill_terminal_committed(moq_fill_table_t *t,
    moq_fill_ref_t ref);
moq_result_t moq_fill_terminal_retired(moq_fill_table_t *t,
    moq_fill_ref_t ref);
/* Reclamation proves ONLY table-represented obligations. Integration must
 * detach/retain queued buffers and generation-pinned events separately first.
 * Unbound cancelled/rejected subscribers remain retained unless an explicit
 * no-stream snapshot plus accepted control proves no stream exists. */
moq_result_t moq_fill_reclaim(moq_fill_table_t *t, moq_fill_ref_t ref);
/* Release a child that never bound a stream once its terminal is published
 * and the integration has taken over its late-stream identity (a bounded
 * tombstone that stops a late header) or proven no stream can arrive (the
 * parent's response was an error). A bound child, or one whose terminal is
 * unpublished, is refused; the slot's generation is retained as for any
 * reclaim. */
moq_result_t moq_fill_release_unbound(moq_fill_table_t *t, moq_fill_ref_t ref);
bool moq_fill_publisher_clear(const moq_fill_table_t *t,
    moq_fill_parent_t parent);

#endif
