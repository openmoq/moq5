#ifndef MOQR_BIND_RETRIEVAL_H
#define MOQR_BIND_RETRIEVAL_H

#include <moq/relay/moqr_bind.h>
#include "moqr_bind_metadata.h"

/* Owner-thread-only, non-installed interface shared by the local fill writer
 * and the shard owner. No authorization entry point: parent must be the live,
 * already-accepted subscription (the owner's pseudo subscription for a shard
 * demand), or, for a standalone FETCH, the request's own admission: a core
 * retrieval owner (core/retrieve_internal.h) that the authorized FETCH
 * opened, whose view is then the cursor's only cache reader. Range and
 * priority are the resolved child values. Range Filters are not negotiated
 * by this implementation. A fill is served in ascending order; a standalone
 * FETCH may ask for descending group order, which orders groups, not the
 * Objects within a group. A cursor must not move or be copied while live. No
 * callback may reenter bind.
 */
typedef struct moqr_retrieve_location {
    uint64_t group, object;
} moqr_retrieve_location_t;

typedef struct moqr_retrieve_request {
    moqr_binding_t binding;
    moqr_sub_t parent;      /* fills; unused for a standalone FETCH        */
    /* A standalone FETCH: owner is its admitted core retrieval owner, which
     * the cursor never closes. The track generation it was admitted against
     * is the one the cursor serves and retrieves for. */
    bool standalone;
    moqr_fetch_t owner;
    moqr_track_t track;
    uint64_t track_gen;
    /* Full consumer identity, never a pointer to a reusable owner table row.
 * Local: source = connection cookie, demand = core subscription handle.
     * Shard: source = source shard, demand = generation-qualified demand id.
     * child is the local fill handle / remote fill id respectively. */
    uint64_t source, demand, child;
    moqr_retrieve_location_t start, end; /* both inclusive, full uint64_t */
    moqr_group_order_t group_order;
    uint8_t subscriber_priority;
    uint64_t deadline_us; /* original absolute deadline; never renewed */
} moqr_retrieve_request_t;

typedef enum moqr_retrieve_kind {
    MOQR_RETRIEVE_OBJECT = 1,
    MOQR_RETRIEVE_GAP,
    MOQR_RETRIEVE_END,
    MOQR_RETRIEVE_ERROR,
} moqr_retrieve_kind_t;

typedef enum moqr_retrieve_error_kind {
    MOQR_RETRIEVE_LOCAL = 1, /* local_result, including capacity/allocation */
    MOQR_RETRIEVE_SOURCE_LOST,
    MOQR_RETRIEVE_RESPONSE,  /* upstream REQUEST_ERROR, not a stream reset */
    MOQR_RETRIEVE_RESET,
    MOQR_RETRIEVE_MALFORMED,
    MOQR_RETRIEVE_SESSION,  /* session_result: local send/cancel failure */
} moqr_retrieve_error_kind_t;

typedef struct moqr_retrieve_item {
    moqr_retrieve_kind_t kind;
    moqr_retrieve_location_t start, end;
    moqr_record_view_t object; /* borrowed buffers, complete object only */
    moq_fetch_range_kind_t gap_kind;
    moqr_retrieve_error_kind_t error_kind;
    moqr_result_t local_result;
    moq_result_t session_result;
    uint64_t error_code, retry_after_ms;
    moq_request_terminal_cause_t cause;
    bool can_retry, data_stream, stop_sending;
    moq_bytes_t reason; /* borrowed until commit/close, like object buffers */
} moqr_retrieve_item_t;

typedef struct moqr_retrieve_parent {
    bool publish;
    union {
        moq_subscription_t sub;
        moq_publication_t pub;
    } handle;
} moqr_retrieve_parent_t;

/* Inline owner storage, charged by the owning table's sizeof, not against the
 * transient byte pool. Fields below request are private to the implementation.
 * One core cursor, at most one collector token and one cached-borrow token;
 * the latter names bind-owned copies, not another upstream collector. */
typedef struct moqr_retrieve_cursor {
    moqr_retrieve_request_t request;
    bool live, done, pending, cache_peeked, cache_absent;
    bool span_active, span_refreshed, source_bound, waiting_origin;
    moqr_retrieve_location_t next, cache_end, span_end;
    moqr_fetch_t cache;
    moqr_fetch_item_t cache_item;
    uint32_t collector_slot; /* one-based; zero means none */
    uint64_t collector_generation;
    uint64_t source_cookie;
    moqr_binding_t source_binding;
    moqr_retrieve_parent_t source_parent;
    uint32_t cached_slot; /* one-based bind-owned pending-byte slot */
    uint64_t cached_generation;
    moqr_retrieve_item_t item;
} moqr_retrieve_cursor_t;

/* init requires zeroed/closed storage, validates identities/order and sends
 * nothing; reinitializing a live cursor is refused. peek returns OK with a
 * typed item, WOULD_BLOCK without consuming coverage, or a local usage error.
 * A successful peek is stable across credit waits until commit/close, except
 * terminal parent/source loss or a hard collector failure supersedes it with
 * ERROR. Previously borrowed buffers remain alive until close in that case.
 * Commit consumes exactly that item, never a freshly recomputed cache gap. END means
 * every Location was delivered or explicitly classified; ERROR never FINs.
 * Cancellation is detached into the bounded collector, which cannot be reused
 * until its actual upstream cancel/terminal settles, even after cursor reuse.
 */
moqr_result_t moqr_bind_retrieve_init(moqr_bind_t *b,
                                     moqr_retrieve_cursor_t *cursor,
                                     const moqr_retrieve_request_t *request);
moqr_result_t moqr_bind_retrieve_peek(moqr_bind_t *b,
                                     moqr_retrieve_cursor_t *cursor,
                                     uint64_t now_us,
                                     moqr_retrieve_item_t *out);
moqr_result_t moqr_bind_retrieve_commit(moqr_bind_t *b,
                                       moqr_retrieve_cursor_t *cursor);
void moqr_bind_retrieve_close(moqr_bind_t *b, moqr_retrieve_cursor_t *cursor);
uint64_t moqr_bind_retrieve_next_deadline(const moqr_retrieve_cursor_t *cursor);

/* Delivery order over an inclusive range [start .. last]: the first Location,
 * the last Location of `group`'s part of the range (window_end; the whole
 * range in ascending order), and the Location after `end` (false once `end`
 * reaches the last Location the order delivers). Ascending runs the range in
 * Location order. Descending runs groups from last.group down to
 * start.group, each group in Object order: from {last.group, 0} (or the start
 * when both ends share a group) to {start.group, UINT64_MAX} (or the end,
 * likewise). */
static inline moqr_retrieve_location_t
moqr_retrieve_window_end(moqr_retrieve_location_t last, moqr_group_order_t order,
                         uint64_t group)
{
    if (order != MOQR_GROUP_ORDER_DESCENDING || group == last.group) return last;
    return (moqr_retrieve_location_t){ group, UINT64_MAX };
}

static inline moqr_retrieve_location_t
moqr_retrieve_first(moqr_retrieve_location_t start, moqr_retrieve_location_t last,
                    moqr_group_order_t order)
{
    if (order != MOQR_GROUP_ORDER_DESCENDING || start.group == last.group) {
        return start;
    }
    return (moqr_retrieve_location_t){ last.group, 0 };
}

static inline bool
moqr_retrieve_after(moqr_retrieve_location_t start, moqr_retrieve_location_t last,
                    moqr_group_order_t order, moqr_retrieve_location_t end,
                    moqr_retrieve_location_t *next)
{
    if (order != MOQR_GROUP_ORDER_DESCENDING) {
        if (end.group > last.group ||
            (end.group == last.group && end.object >= last.object)) return false;
        /* end < last, so a group's last id is followed by the next group. */
        *next = end.object == UINT64_MAX
            ? (moqr_retrieve_location_t){ end.group + 1u, 0 }
            : (moqr_retrieve_location_t){ end.group, end.object + 1u };
        return true;
    }
    moqr_retrieve_location_t w = moqr_retrieve_window_end(last, order, end.group);
    if (end.object < w.object) {
        *next = (moqr_retrieve_location_t){ end.group, end.object + 1u };
        return true;
    }
    if (end.group <= start.group) return false;
    uint64_t g = end.group - 1u;
    *next = (moqr_retrieve_location_t){ g, g == start.group ? start.object : 0 };
    return true;
}

/* Quiesced teardown only: forget makes NO bind/core/session call. The shard
 * destroy order is release metadata pins, bind_destroy, mgr_free, core_destroy.
 * mgr_free must forget these cursors, NOT close them through the freed bind.
 * bind_destroy frees collector and cached-borrow copies; core_destroy frees
 * core cursor pins. No owned allocation lives only in this cursor.
 * Normal live demand/fill retirement instead calls retrieve_close first. */
void moqr_bind_retrieve_forget(moqr_retrieve_cursor_t *cursor);

/* Optional hidden typed adjunct to the existing public fill-source lifecycle.
 * open/commit/close/deadline remain the existing callbacks with the same ctx
 * and claim handle. The built-in shard path can carry real GAP/ERROR without
 * recoding them as a cache miss or adding a second retrieval policy. Install
 * only while there are no live fills; NULL restores the public-only reader. */
typedef moqr_result_t (*moqr_retrieve_source_peek_fn)(
    void *ctx, uint64_t handle, uint64_t now_us, moqr_retrieve_item_t *out);
moqr_result_t moqr_bind_retrieve_source_set(moqr_bind_t *b,
                                           moqr_retrieve_source_peek_fn peek);

/* A standalone FETCH whose track another shard of this runtime owns is
 * answered by that shard. Same-runtime internal source, installed by the
 * shard runtime only while no FETCH is open; handles are the source's own.
 * open never sends: the source asks the owner, whose admission arrives as
 * plan (WOULD_BLOCK until then; an ERROR terminal before it is an error
 * result). After an ACCEPT plan, peek/commit follow the retrieval item
 * contract above over the planned range in the request's order; close is
 * final and sends whatever cancellation is owed. */
typedef struct moqr_retrieve_standalone_query {
    uint64_t remote_cookie;   /* the owner shard's binding cookie here     */
    moqr_ns_t ns;             /* borrowed for the open call                */
    moq_bytes_t name;
    uint64_t start_group, start_object;  /* the request as received:    */
    uint64_t end_group, end_object;      /* raw wire end (last + 1)     */
    uint32_t range_form;
    uint64_t relative_start;
    moqr_group_order_t group_order;
    uint8_t subscriber_priority;
    uint64_t deadline_us;     /* the request's absolute wait deadline      */
    /* A Joining FETCH: the track, in this shard's core, that the joined
     * subscription was answered for. The source names the owner's identity
     * that feeds it, and the owner refuses the request (STALE_HANDLE at open
     * when nothing here feeds it any more) unless that identity still
     * answers the name. */
    bool join;
    moqr_track_t join_track;
} moqr_retrieve_standalone_query_t;

typedef struct moqr_retrieve_standalone_plan {
    moqr_fetch_plan_t wire;   /* as moqr_core_fetch_open reports it        */
    moqr_retrieve_location_t start, end; /* ACCEPT: inclusive range        */
    /* ACCEPT: the Track Properties the answer states, as the owner's reply
     * pin held them when it admitted the request (moqr_bind_meta_reply): the
     * set and the draft that stated it, or UNAVAILABLE when they cannot be
     * stated. meta_bytes stays valid while the handle is open. */
    moqr_bind_meta_state_t meta_state;
    moq_version_t meta_wire_version;
    moq_bytes_t meta_bytes;
    /* REJECT: the owner had no room for the request (its FETCH pool full),
     * answered as capacity rather than by error_code. */
    bool capacity;
} moqr_retrieve_standalone_plan_t;

typedef struct moqr_retrieve_standalone_ops {
    void *ctx;
    moqr_result_t (*open)(void *ctx, const moqr_retrieve_standalone_query_t *q,
                          uint64_t *handle);
    moqr_result_t (*plan)(void *ctx, uint64_t handle,
                          moqr_retrieve_standalone_plan_t *out);
    moqr_retrieve_source_peek_fn peek;
    moqr_result_t (*commit)(void *ctx, uint64_t handle);
    void (*close)(void *ctx, uint64_t handle);
} moqr_retrieve_standalone_ops_t;

/* NULL removes it. MOQR_ERR_INVAL for a table missing an entry,
 * MOQR_ERR_WRONG_STATE during a pump or while a standalone FETCH is open. */
moqr_result_t moqr_bind_retrieve_standalone_set(
    moqr_bind_t *b, const moqr_retrieve_standalone_ops_t *ops);

#endif
