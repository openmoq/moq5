#include <moq/subscriber.h>
#include <moq/rcbuf.h>
#include "fill_identity_internal.h"
#include "migration_internal.h"
#include "../session/session_event_admission.h"
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#define SUB_DEFAULT_MAX_TRACKS  16
#define SUB_DEFAULT_MAX_OBJECTS 256
/* Default facade byte budget; mirrors the session's MOQ_DEFAULT_MAX_RECV_BUF. */
#define SUB_DEFAULT_MAX_QUEUED_BYTES (16u * 1024 * 1024)

typedef enum {
    SUB_TRACK_FREE = 0,
    SUB_TRACK_PENDING,
    SUB_TRACK_ACTIVE,
    SUB_TRACK_ERROR,
    SUB_TRACK_DONE,
} sub_track_state_t;

typedef struct {
    bool failed;
    bool pending;
    bool cancel_pending;
    bool terminal_polled;
} sub_delivery_failure_t;

/* PUBLISH_DONE status GOING_AWAY (draft-22 §12.4, draft-18 and draft-16
 * alike): the status a subscription migrated by a per-request GOAWAY ends
 * with. */
#define SUB_DONE_GOING_AWAY UINT64_C(0x4)

struct moq_sub_track {
    sub_track_state_t     state;
    moq_subscription_t    handle;
    moq_subscriber_t     *sub;
    moq_request_error_t   error_code;
    moq_sub_track_result_t initial_result;
    moq_rcbuf_t          *initial_properties;
    moq_result_t          initial_result_error;
    sub_delivery_failure_t delivery_failure;
    /* Where the subscription went when a REDIRECT or a per-request GOAWAY
     * ended it. */
    facade_migration_t    migration;
};

#define SUB_DEFAULT_MAX_STATUS 16

typedef enum {
    SUB_STATUS_FREE = 0,
    SUB_STATUS_PENDING,
    SUB_STATUS_DONE,
} sub_status_state_t;

struct moq_sub_status_req {
    sub_status_state_t          state;
    moq_track_status_handle_t   handle;
    moq_subscriber_t           *sub;
};

#define SUB_DEFAULT_MAX_FETCHES    16
#define SUB_DEFAULT_MAX_FETCH_ITEMS 256
#define SUB_DEFAULT_MAX_CHUNKS     256

typedef enum {
    SUB_FETCH_FREE = 0,
    SUB_FETCH_PENDING,
    SUB_FETCH_ACTIVE,
    SUB_FETCH_DONE,
} sub_fetch_state_t;

struct moq_sub_fetch_req {
    sub_fetch_state_t   state;
    moq_fetch_t         handle;
    moq_subscriber_t   *sub;
    sub_delivery_failure_t delivery_failure;
};

#define SUB_DEFAULT_MAX_FILLS      16

static sub_fill_identity_allocator_t fill_identities = SUB_FILL_IDENTITY_ALLOCATOR_INIT;

typedef enum {
    SUB_HISTORY_FREE = 0,
    SUB_HISTORY_FETCH,
    SUB_HISTORY_FILL,
} sub_history_kind_t;

typedef struct {
    sub_history_kind_t kind;
    size_t next;
    union {
        moq_sub_fetch_item_t fetch;
        moq_sub_fill_item_t fill;
    } u;
} sub_history_slot_t;

/* Only FIFO indices and logical credit; both families store items in the
 * same tagged arena and return slots to its free list. */
typedef struct {
    size_t head, tail, count, cap;
} sub_history_queue_t;

/* A fill request slot. REQUESTED: the request was carried on a SUBSCRIBE or
 * update and the session's FILL_CREATED (queued synchronously by that call, in
 * request order) has not been consumed yet, so the slot has no session handle.
 * ACTIVE: the handle is known. DONE: the terminal is queued; the slot frees
 * when it is polled. `seq` orders slots so the oldest handle-less request on a
 * track takes the next FILL_CREATED and the newest live one answers
 * moq_sub_track_fill. */
typedef enum {
    SUB_FILL_FREE = 0,
    SUB_FILL_REQUESTED,
    SUB_FILL_ACTIVE,
    SUB_FILL_DONE,
} sub_fill_state_t;

typedef struct moq_sub_fill_req moq_sub_fill_req_t;

/* `seq` is also the request's public handle (moq_sub_fill_t): never zero or
 * reused within this linked core instance, even after owner destruction.
 * Ordering uses only comparisons, so gaps from other owners or refusals are OK. */
struct moq_sub_fill_req {
    sub_fill_state_t    state;
    moq_fill_t          handle;
    moq_sub_track_t    *track;
    moq_subscriber_t   *sub;
    uint64_t            seq;
    bool                cancelled;   /* cancel accepted; terminal pending */
    uint64_t            initiating_request_id;
    sub_delivery_failure_t delivery_failure;
};

/* A queued stream terminal, with the object and chunk queue positions it
 * waits behind. The facade's queues are strictly FIFO, so a position is the
 * ingress sequence of what was queued there: the terminal is pollable once
 * both heads have reached them. */
typedef struct sub_stream_terminal_entry {
    moq_sub_stream_terminal_t rec;
    size_t                    obj_seq;
    size_t                    chunk_seq;
} sub_stream_terminal_entry_t;

/* A queued Group end, with the object and chunk queue positions it waits
 * behind: it is pollable once both heads have reached them. */
typedef struct sub_group_end_entry {
    moq_sub_group_end_t end;
    size_t              obj_seq;
    size_t              chunk_seq;
} sub_group_end_entry_t;

struct moq_subscriber {
    moq_session_t      *session;
    moq_alloc_t         alloc;
    moq_sub_callbacks_t callbacks;
    void              (*on_goaway)(void *ctx, moq_bytes_t new_session_uri);
    void              (*on_subscribe_done)(void *ctx, moq_sub_track_t *track,
                                           uint64_t status_code);
    void              (*on_update_ok)(void *ctx, moq_sub_track_t *track,
                                      const moq_sub_update_result_t *result);
    bool                closed;
    bool                draining;

    /* Byte budget for payload+properties rcbufs retained in the object and
     * fetch-object ring queues. Counts bytes currently in the rings only; the
     * one-deep pending slots are outside this gauge. Legacy profiles admit at
     * the high-water mark; D22's pre-dequeue gate includes all pending slots,
     * chunks and retained ACK metadata in the exact configured byte bound. */
    size_t              max_queued_bytes;
    size_t              queued_bytes;
    size_t              retained_ack_bytes; /* Not part of the queue gauge. */

    moq_sub_track_t    *tracks;
    size_t              track_cap;

    moq_sub_object_t   *objects;
    size_t              obj_cap;
    size_t              obj_head;
    size_t              obj_tail;

    bool                has_pending;
    moq_sub_object_t    pending;

    moq_sub_status_req_t *status_reqs;
    size_t                status_cap;

    moq_sub_status_outcome_t *status_results;
    size_t                   sr_cap;
    size_t                   sr_head;
    size_t                   sr_tail;

    moq_sub_fetch_req_t  *fetch_reqs;
    size_t                fetch_cap;

    sub_history_slot_t  *history;
    size_t                history_cap;
    size_t                history_free;
    sub_history_queue_t   fetch_queue;
    sub_history_queue_t   fill_queue;

    bool                  has_pending_fi;
    moq_sub_fetch_item_t  pending_fi;

    moq_sub_fill_req_t   *fill_reqs;
    size_t                fill_cap;

    bool                  has_pending_fli;
    moq_sub_fill_item_t   pending_fli;

    bool                streaming;
    moq_sub_chunk_t    *chunks;
    size_t              chunk_cap;
    size_t              chunk_head;
    size_t              chunk_tail;

    bool                has_pending_chunk;
    moq_sub_chunk_t     pending_chunk;

    /* Group ends (surface_group_ends): a ring of ge_cap entries, allocated
     * only when the option is set, plus the same one-deep pending slot the
     * other queues use under backpressure. */
    bool                   surface_group_ends;
    sub_group_end_entry_t *group_ends;
    size_t                 ge_cap;
    size_t                 ge_head;
    size_t                 ge_tail;
    bool                   has_pending_ge;
    sub_group_end_entry_t  pending_ge;

    /* Stream terminals (surface_stream_terminals): a ring of st_cap
     * entries plus the one-deep pending slot, whose positions are taken
     * when it enters the ring. */
    bool                         surface_stream_terminals;
    sub_stream_terminal_entry_t *stream_terminals;
    size_t                       st_cap;
    size_t                       st_head;
    size_t                       st_tail;
    bool                         has_pending_st;
    moq_sub_stream_terminal_t    pending_st;

    moq_sub_stats_t     stats;
};

/* -- Helpers -------------------------------------------------------- */

static moq_sub_track_t *find_track_by_handle(moq_subscriber_t *s,
                                               moq_subscription_t h)
{
    for (size_t i = 0; i < s->track_cap; i++)
        if (s->tracks[i].state != SUB_TRACK_FREE &&
            moq_subscription_eq(s->tracks[i].handle, h))
            return &s->tracks[i];
    return NULL;
}

static moq_sub_track_t *alloc_track(moq_subscriber_t *s)
{
    for (size_t i = 0; i < s->track_cap; i++)
        if (s->tracks[i].state == SUB_TRACK_FREE)
            return &s->tracks[i];
    return NULL;
}

static void track_result_release(moq_subscriber_t *s, moq_sub_track_t *t)
{
    s->retained_ack_bytes -= moq_rcbuf_len(t->initial_properties);
    moq_rcbuf_decref(t->initial_properties);
    t->initial_properties = NULL;
    memset(&t->initial_result, 0, sizeof(t->initial_result));
    t->initial_result_error = MOQ_OK;
    facade_migration_clear(&t->migration, &s->alloc);
}

static moq_sub_status_req_t *find_status_by_handle(moq_subscriber_t *s,
    moq_track_status_handle_t h)
{
    for (size_t i = 0; i < s->status_cap; i++)
        if (s->status_reqs[i].state != SUB_STATUS_FREE &&
            moq_track_status_handle_eq(s->status_reqs[i].handle, h))
            return &s->status_reqs[i];
    return NULL;
}

static moq_sub_status_req_t *alloc_status_req(moq_subscriber_t *s)
{
    for (size_t i = 0; i < s->status_cap; i++)
        if (s->status_reqs[i].state == SUB_STATUS_FREE)
            return &s->status_reqs[i];
    return NULL;
}

static bool sr_queue_full(const moq_subscriber_t *s)
{
    return (s->sr_tail - s->sr_head) >= s->sr_cap;
}

static moq_sub_status_outcome_t *sr_push(moq_subscriber_t *s)
{
    if (sr_queue_full(s)) return NULL;
    moq_sub_status_outcome_t *r = &s->status_results[s->sr_tail % s->sr_cap];
    memset(r, 0, sizeof(*r));
    r->struct_size = sizeof(*r);
    s->sr_tail++;
    return r;
}

/* -- Retained-byte accounting --------------------------------------- */

static size_t sub_sat_add(size_t a, size_t b)
{
    return b > SIZE_MAX - a ? SIZE_MAX : a + b;
}

/* Bytes a queued object/fetch-object retains: payload + properties. */
static size_t rcbuf_pair_bytes(const moq_rcbuf_t *payload,
                               const moq_rcbuf_t *properties)
{
    size_t n = payload ? moq_rcbuf_len(payload) : 0;
    if (properties)
        n = sub_sat_add(n, moq_rcbuf_len(properties));
    return n;
}

static uint64_t u64_sat_add_size(uint64_t a, size_t b)
{
    uint64_t ub = (uint64_t)b;
    if (ub < b || UINT64_MAX - a < ub) return UINT64_MAX;
    return a + ub;
}

static void sub_bytes_add(moq_subscriber_t *s, size_t n)
{
    s->queued_bytes = sub_sat_add(s->queued_bytes, n);
}

static void sub_bytes_sub(moq_subscriber_t *s, size_t n)
{
    s->queued_bytes = n <= s->queued_bytes ? s->queued_bytes - n : 0;
}

/* High-water gate: true once retained bytes have reached the cap. New objects
 * are admitted while strictly under the cap (so the ring may overshoot by one
 * object); once at/over the cap they divert to the one-deep pending slot. The
 * pending slot's bytes are tracked off-budget, which keeps the pending-flush
 * gate from deadlocking on an object larger than the cap. */
static bool obj_bytes_over_budget(const moq_subscriber_t *s)
{
    return s->retained_ack_bytes >= s->max_queued_bytes ||
        s->queued_bytes >= s->max_queued_bytes - s->retained_ack_bytes;
}

static bool obj_queue_full(const moq_subscriber_t *s)
{
    return (s->obj_tail - s->obj_head) >= s->obj_cap;
}

static moq_sub_object_t *obj_push(moq_subscriber_t *s)
{
    if (obj_queue_full(s)) return NULL;
    moq_sub_object_t *o = &s->objects[s->obj_tail % s->obj_cap];
    s->obj_tail++;
    return o;
}

static moq_sub_fetch_req_t *find_fetch_by_handle(moq_subscriber_t *s,
    moq_fetch_t h)
{
    for (size_t i = 0; i < s->fetch_cap; i++)
        if (s->fetch_reqs[i].state != SUB_FETCH_FREE &&
            moq_fetch_eq(s->fetch_reqs[i].handle, h))
            return &s->fetch_reqs[i];
    return NULL;
}

static moq_sub_fetch_req_t *alloc_fetch_req(moq_subscriber_t *s)
{
    for (size_t i = 0; i < s->fetch_cap; i++)
        if (s->fetch_reqs[i].state == SUB_FETCH_FREE)
            return &s->fetch_reqs[i];
    return NULL;
}

static bool fi_queue_full(const moq_subscriber_t *s)
{
    return s->fetch_queue.count >= s->fetch_queue.cap;
}

static sub_history_slot_t *history_push(moq_subscriber_t *s,
    sub_history_queue_t *q, sub_history_kind_t kind)
{
    if (q->count >= q->cap) return NULL;
    /* Summed logical quotas guarantee a free slot whenever this family has
     * credit. Pending items retain their existing, separate one-deep slot. */
    size_t index = s->history_free;
    sub_history_slot_t *slot = &s->history[index];
    s->history_free = slot->next;
    slot->kind = kind;
    slot->next = SIZE_MAX;
    if (q->count) s->history[q->tail].next = index;
    else q->head = index;
    q->tail = index;
    q->count++;
    return slot;
}

static void history_remove(moq_subscriber_t *s, sub_history_queue_t *q,
    size_t index, size_t previous)
{
    sub_history_slot_t *slot = &s->history[index];
    if (previous == SIZE_MAX) q->head = slot->next;
    else s->history[previous].next = slot->next;
    if (q->tail == index) q->tail = previous;
    q->count--;
    memset(slot, 0, sizeof(*slot));
    slot->next = s->history_free;
    s->history_free = index;
}

static moq_sub_fetch_item_t *fi_push(moq_subscriber_t *s)
{
    sub_history_slot_t *slot = history_push(s, &s->fetch_queue, SUB_HISTORY_FETCH);
    return slot ? &slot->u.fetch : NULL;
}

static void fetch_item_release_refs(moq_sub_fetch_item_t *item)
{
    if (item->kind == MOQ_SUB_FETCH_OBJECT) {
        if (item->u.object.payload) {
            moq_rcbuf_decref(item->u.object.payload);
            item->u.object.payload = NULL;
        }
        if (item->u.object.properties) {
            moq_rcbuf_decref(item->u.object.properties);
            item->u.object.properties = NULL;
        }
    }
}

/* Discard every queued/pending fetch item that belongs to `req`. Called on
 * cancel before the request slot is freed and reused: the request pointer is
 * the application-visible correlation token, so stale items left behind would
 * be re-attributed to a later fetch that reuses the slot. Releases rcbuf refs
 * and returns retained bytes to the budget for removed queued objects; the
 * one-deep pending slot is tracked off-budget, so it only releases refs. FIFO
 * order of the surviving items is preserved. */
static void purge_fetch_items_for(moq_subscriber_t *sub,
                                  const moq_sub_fetch_req_t *req)
{
    if (sub->has_pending_fi && sub->pending_fi.request == req) {
        fetch_item_release_refs(&sub->pending_fi);
        memset(&sub->pending_fi, 0, sizeof(sub->pending_fi));
        sub->has_pending_fi = false;
    }

    size_t previous = SIZE_MAX;
    size_t index = sub->fetch_queue.head;
    while (index != SIZE_MAX) {
        sub_history_slot_t *slot = &sub->history[index];
        size_t next = slot->next;
        moq_sub_fetch_item_t *src = &slot->u.fetch;
        if (src->request == req) {
            if (src->kind == MOQ_SUB_FETCH_OBJECT)
                sub_bytes_sub(sub, rcbuf_pair_bytes(src->u.object.payload,
                                                    src->u.object.properties));
            fetch_item_release_refs(src);
            history_remove(sub, &sub->fetch_queue, index, previous);
        } else previous = index;
        index = next;
    }
}

static moq_sub_fill_req_t *find_fill_by_handle(moq_subscriber_t *s, moq_fill_t h)
{
    for (size_t i = 0; i < s->fill_cap; i++)
        if (s->fill_reqs[i].state == SUB_FILL_ACTIVE &&
            moq_fill_eq(s->fill_reqs[i].handle, h))
            return &s->fill_reqs[i];
    return NULL;
}

/* The oldest handle-less request on `t`: the one the next FILL_CREATED for
 * that subscription belongs to (the session creates children in request
 * order, within the requesting call). */
static moq_sub_fill_req_t *find_fill_awaiting_handle(moq_subscriber_t *s,
                                                     const moq_sub_track_t *t)
{
    moq_sub_fill_req_t *best = NULL;
    for (size_t i = 0; i < s->fill_cap; i++) {
        moq_sub_fill_req_t *r = &s->fill_reqs[i];
        if (r->state == SUB_FILL_REQUESTED && r->track == t &&
            (!best || r->seq < best->seq))
            best = r;
    }
    return best;
}

static moq_result_t alloc_fill_req(moq_subscriber_t *s, moq_sub_track_t *t,
                                   sub_fill_identity_allocator_t *ids,
                                   moq_sub_fill_req_t **out)
{
    for (size_t i = 0; i < s->fill_cap; i++) {
        moq_sub_fill_req_t *r = &s->fill_reqs[i];
        if (r->state == SUB_FILL_FREE) {
            uint64_t seq;
            if (!sub_fill_identity_next(ids, &seq)) return MOQ_ERR_LIMIT;
            r->state = SUB_FILL_REQUESTED;
            r->track = t;
            r->seq = seq;
            r->cancelled = false;
            r->initiating_request_id = 0;
            memset(&r->delivery_failure, 0, sizeof(r->delivery_failure));
            memset(&r->handle, 0, sizeof(r->handle));
            *out = r;
            return MOQ_OK;
        }
    }
    return MOQ_ERR_WOULD_BLOCK;
}

static moq_sub_fill_t fill_handle_of(const moq_sub_fill_req_t *r)
{
    moq_sub_fill_t h = { r->seq };
    return h;
}

/* The live request a handle names, or NULL once its terminal was polled. */
static moq_sub_fill_req_t *fill_resolve(moq_subscriber_t *s, moq_sub_fill_t h)
{
    if (h._opaque == 0) return NULL;
    for (size_t i = 0; i < s->fill_cap; i++)
        if (s->fill_reqs[i].state != SUB_FILL_FREE && s->fill_reqs[i].seq == h._opaque)
            return &s->fill_reqs[i];
    return NULL;
}

static void free_fill_req(moq_sub_fill_req_t *r)
{
    r->state = SUB_FILL_FREE;
    r->track = NULL;
    memset(&r->handle, 0, sizeof(r->handle));
}

static bool fli_queue_full(const moq_subscriber_t *s)
{
    return s->fill_queue.count >= s->fill_queue.cap;
}

static moq_sub_fill_item_t *fli_push(moq_subscriber_t *s)
{
    sub_history_slot_t *slot = history_push(s, &s->fill_queue, SUB_HISTORY_FILL);
    return slot ? &slot->u.fill : NULL;
}

static void fill_item_release_refs(moq_sub_fill_item_t *item)
{
    if (item->kind == MOQ_SUB_FILL_OBJECT) {
        if (item->u.object.payload) {
            moq_rcbuf_decref(item->u.object.payload);
            item->u.object.payload = NULL;
        }
        if (item->u.object.properties) {
            moq_rcbuf_decref(item->u.object.properties);
            item->u.object.properties = NULL;
        }
    }
}

static bool chunk_queue_full(const moq_subscriber_t *s)
{
    return (s->chunk_tail - s->chunk_head) >= s->chunk_cap;
}

static moq_sub_chunk_t *chunk_push(moq_subscriber_t *s)
{
    if (chunk_queue_full(s)) return NULL;
    moq_sub_chunk_t *c = &s->chunks[s->chunk_tail % s->chunk_cap];
    s->chunk_tail++;
    return c;
}

static bool ge_queue_full(const moq_subscriber_t *s)
{
    return (s->ge_tail - s->ge_head) >= s->ge_cap;
}

static void ge_push(moq_subscriber_t *s, const sub_group_end_entry_t *e)
{
    s->group_ends[s->ge_tail % s->ge_cap] = *e;
    s->ge_tail++;
}

static bool st_queue_full(const moq_subscriber_t *s)
{
    return (s->st_tail - s->st_head) >= s->st_cap;
}

/* Queue a terminal behind everything queued so far. */
static void st_push(moq_subscriber_t *s, const moq_sub_stream_terminal_t *rec)
{
    sub_stream_terminal_entry_t *e = &s->stream_terminals[s->st_tail % s->st_cap];
    e->rec = *rec;
    e->obj_seq = s->obj_tail;
    e->chunk_seq = s->chunk_tail;
    s->st_tail++;
}

/* Queue `rec`, or hold it in the pending slot when the lane is full; true
 * when it was held, so the caller returns WOULD_BLOCK. */
static bool st_offer(moq_subscriber_t *s, const moq_sub_stream_terminal_t *rec)
{
    if (st_queue_full(s)) {
        s->pending_st = *rec;
        s->has_pending_st = true;
        return true;
    }
    st_push(s, rec);
    return false;
}

static void st_record(moq_sub_stream_terminal_t *rec, moq_sub_track_t *t,
                      moq_sub_stream_terminal_kind_t kind,
                      uint64_t group_id, uint64_t subgroup_id)
{
    memset(rec, 0, sizeof(*rec));
    rec->struct_size = (uint32_t)sizeof(*rec);
    rec->kind = kind;
    rec->track = t;
    rec->group_id = group_id;
    rec->subgroup_id = subgroup_id;
}

static void chunk_release_refs(moq_sub_chunk_t *c)
{
    if (c->chunk) { moq_rcbuf_decref(c->chunk); c->chunk = NULL; }
    if (c->properties) { moq_rcbuf_decref(c->properties); c->properties = NULL; }
}

static bool ack_charge(size_t *remaining, size_t bytes)
{
    if (bytes > *remaining) return false;
    *remaining -= bytes;
    return true;
}

static bool ack_charge_pair(size_t *remaining, const moq_rcbuf_t *a,
                             const moq_rcbuf_t *b)
{
    return ack_charge(remaining, moq_rcbuf_len(a)) &&
        ack_charge(remaining, moq_rcbuf_len(b));
}

/* Measure every owned reference by subtraction, including pending items and
 * chunks. The legacy ring gauge alone cannot establish the D22 byte bound. */
static bool retained_refs_fit(const moq_subscriber_t *s, size_t remaining)
{
    if (!ack_charge(&remaining, s->retained_ack_bytes)) return false;
    for (size_t i = s->obj_head; i < s->obj_tail; i++) {
        const moq_sub_object_t *o = &s->objects[i % s->obj_cap];
        if (!ack_charge_pair(&remaining, o->payload, o->properties)) return false;
    }
    if (s->has_pending && !ack_charge_pair(&remaining,
        s->pending.payload, s->pending.properties)) return false;
    for (size_t i = 0; i < s->history_cap; i++) {
        const sub_history_slot_t *slot = &s->history[i];
        if (slot->kind == SUB_HISTORY_FETCH) {
            const moq_sub_fetch_item_t *f = &slot->u.fetch;
            if (f->kind == MOQ_SUB_FETCH_OBJECT && !ack_charge_pair(&remaining,
                f->u.object.payload, f->u.object.properties)) return false;
        } else if (slot->kind == SUB_HISTORY_FILL) {
            const moq_sub_fill_item_t *f = &slot->u.fill;
            if (f->kind == MOQ_SUB_FILL_OBJECT && !ack_charge_pair(&remaining,
                f->u.object.payload, f->u.object.properties)) return false;
        }
    }
    if (s->has_pending_fi && s->pending_fi.kind == MOQ_SUB_FETCH_OBJECT &&
        !ack_charge_pair(&remaining, s->pending_fi.u.object.payload,
                         s->pending_fi.u.object.properties)) return false;
    if (s->has_pending_fli && s->pending_fli.kind == MOQ_SUB_FILL_OBJECT &&
        !ack_charge_pair(&remaining, s->pending_fli.u.object.payload,
                         s->pending_fli.u.object.properties)) return false;
    for (size_t i = s->chunk_head; i < s->chunk_tail; i++) {
        const moq_sub_chunk_t *c = &s->chunks[i % s->chunk_cap];
        if (!ack_charge_pair(&remaining, c->chunk, c->properties)) return false;
    }
    return !s->has_pending_chunk || ack_charge_pair(&remaining,
        s->pending_chunk.chunk, s->pending_chunk.properties);
}

static bool ack_bytes_fit(const moq_subscriber_t *s, size_t bytes)
{
    size_t remaining = s->max_queued_bytes;
    return ack_charge(&remaining, bytes) && retained_refs_fit(s, remaining);
}

static moq_result_t track_result_capture(moq_subscriber_t *s,
    moq_sub_track_t *t, const moq_subscribe_ok_event_t *ack)
{
    moq_sub_track_result_t *r = &t->initial_result;
    r->flags = MOQ_SUB_TRACK_RESULT_RESPONSE_SEEN;
    if (ack->has_largest) {
        r->flags |= MOQ_SUB_TRACK_RESULT_HAS_LARGEST;
        r->largest_group = ack->largest_group;
        r->largest_object = ack->largest_object;
    }
    if (ack->has_expires) {
        r->flags |= MOQ_SUB_TRACK_RESULT_HAS_EXPIRES;
        r->expires_ms = ack->expires_ms;
    }
    size_t len = ack->track_properties.len;
    if (len) {
        size_t allocation_size;
        if (!ack_bytes_fit(s, len) ||
            moq_rcbuf_allocation_size(len, &allocation_size) != MOQ_OK)
            return MOQ_ERR_BUFFER;
        moq_result_t rc = moq_rcbuf_create(&s->alloc, ack->track_properties.data,
                                          len, &t->initial_properties);
        if (rc != MOQ_OK) return rc;
        s->retained_ack_bytes += len; /* Checked by ack_bytes_fit. */
        r->track_properties.data = moq_rcbuf_data(t->initial_properties);
        r->track_properties.len = len;
    }
    return MOQ_OK;
}

static uint64_t sub_retained_bytes(const moq_subscriber_t *s)
{
    uint64_t n = (uint64_t)s->queued_bytes;
    if (n < s->queued_bytes) n = UINT64_MAX;
    if (s->has_pending)
        n = u64_sat_add_size(n, rcbuf_pair_bytes(s->pending.payload,
                                                 s->pending.properties));
    if (s->has_pending_fi &&
        s->pending_fi.kind == MOQ_SUB_FETCH_OBJECT)
        n = u64_sat_add_size(n, rcbuf_pair_bytes(
            s->pending_fi.u.object.payload, s->pending_fi.u.object.properties));
    if (s->has_pending_fli &&
        s->pending_fli.kind == MOQ_SUB_FILL_OBJECT)
        n = u64_sat_add_size(n, rcbuf_pair_bytes(
            s->pending_fli.u.object.payload, s->pending_fli.u.object.properties));
    for (size_t i = s->chunk_head; i < s->chunk_tail; i++) {
        const moq_sub_chunk_t *c = &s->chunks[i % s->chunk_cap];
        n = u64_sat_add_size(n, rcbuf_pair_bytes(c->chunk, c->properties));
    }
    if (s->has_pending_chunk)
        n = u64_sat_add_size(n, rcbuf_pair_bytes(s->pending_chunk.chunk,
                                                 s->pending_chunk.properties));
    return n;
}

static moq_result_t sub_would_block(moq_subscriber_t *sub)
{
    sub->stats.tick_would_blocks++;
    return MOQ_ERR_WOULD_BLOCK;
}

static bool history_has_fetch(const moq_subscriber_t *s,
                               const moq_sub_fetch_req_t *req)
{
    if (s->has_pending_fi && s->pending_fi.request == req) return true;
    for (size_t i = 0; i < s->history_cap; i++)
        if (s->history[i].kind == SUB_HISTORY_FETCH &&
            s->history[i].u.fetch.request == req) return true;
    return false;
}

static bool history_has_fill(const moq_subscriber_t *s,
                              const moq_sub_fill_req_t *req)
{
    moq_sub_fill_t token = fill_handle_of(req);
    if (s->has_pending_fli &&
        moq_sub_fill_eq(s->pending_fli.request, token)) return true;
    for (size_t i = 0; i < s->history_cap; i++)
        if (s->history[i].kind == SUB_HISTORY_FILL &&
            moq_sub_fill_eq(s->history[i].u.fill.request, token)) return true;
    return false;
}

static bool delivery_failure_settled(const sub_delivery_failure_t *failure)
{
    return failure->failed && !failure->pending &&
        !failure->cancel_pending && failure->terminal_polled;
}

static void reap_failed_history(moq_subscriber_t *s)
{
    for (size_t i = 0; i < s->fetch_cap; i++) {
        moq_sub_fetch_req_t *r = &s->fetch_reqs[i];
        if (r->state != SUB_FETCH_FREE &&
            delivery_failure_settled(&r->delivery_failure) &&
            !history_has_fetch(s, r)) r->state = SUB_FETCH_FREE;
    }
    for (size_t i = 0; i < s->fill_cap; i++) {
        moq_sub_fill_req_t *r = &s->fill_reqs[i];
        if (r->state != SUB_FILL_FREE &&
            delivery_failure_settled(&r->delivery_failure) &&
            !history_has_fill(s, r)) free_fill_req(r);
    }
}

static void retry_failed_deliveries(moq_subscriber_t *s, uint64_t now_us)
{
    bool closed = moq_session_state(s->session) == MOQ_SESS_CLOSED;
    for (size_t i = 0; i < s->track_cap; i++) {
        moq_sub_track_t *t = &s->tracks[i];
        sub_delivery_failure_t *f = &t->delivery_failure;
        if (t->state == SUB_TRACK_FREE || !f->cancel_pending) continue;
        moq_result_t rc = closed ? MOQ_ERR_CLOSED :
            moq_session_unsubscribe(s->session, t->handle, now_us);
        if (rc == MOQ_OK || rc == MOQ_ERR_CLOSED || rc == MOQ_ERR_STALE_HANDLE)
            f->cancel_pending = false;
        /* STALE can mean the real terminal is still queued. Do not retire its
         * facade identity until that event has also been observed. */
        if (rc == MOQ_OK || rc == MOQ_ERR_CLOSED) f->terminal_polled = true;
    }
    for (size_t i = 0; i < s->fetch_cap; i++) {
        moq_sub_fetch_req_t *r = &s->fetch_reqs[i];
        sub_delivery_failure_t *f = &r->delivery_failure;
        if (r->state == SUB_FETCH_FREE || !f->cancel_pending) continue;
        moq_result_t rc = closed ? MOQ_ERR_CLOSED :
            moq_session_fetch_cancel(s->session, r->handle, now_us);
        if (rc == MOQ_OK || rc == MOQ_ERR_CLOSED || rc == MOQ_ERR_STALE_HANDLE)
            f->cancel_pending = false;
        if (rc == MOQ_OK || rc == MOQ_ERR_CLOSED) f->terminal_polled = true;
    }
    for (size_t i = 0; i < s->fill_cap; i++) {
        moq_sub_fill_req_t *r = &s->fill_reqs[i];
        sub_delivery_failure_t *f = &r->delivery_failure;
        if (r->state == SUB_FILL_FREE || !f->cancel_pending) continue;
        moq_result_t rc = closed ? MOQ_ERR_CLOSED :
            moq_session_cancel_fill(s->session, r->handle, now_us);
        if (rc == MOQ_OK || rc == MOQ_ERR_CLOSED || rc == MOQ_ERR_STALE_HANDLE)
            f->cancel_pending = false;
        if (rc == MOQ_OK) r->cancelled = true;
        /* Fill cancellation has its own real terminal; success is not an
         * invented terminal observation. A closed session purges that debt. */
        if (rc == MOQ_ERR_CLOSED) f->terminal_polled = true;
    }
    reap_failed_history(s);
}

static session_event_admission_t admit_delivery(void *ctx, const moq_event_t *ev)
{
    moq_subscriber_t *s = ctx;
    const moq_rcbuf_t *payload = NULL, *properties = NULL;
    sub_delivery_failure_t *failure = NULL;
    moq_sub_track_t *track = NULL;
    moq_sub_fetch_req_t *fetch = NULL;
    switch (ev->kind) {
    case MOQ_EVENT_OBJECT_RECEIVED:
        track = find_track_by_handle(s, ev->u.object_received.sub);
        if (track && track->state == SUB_TRACK_ACTIVE) {
            failure = &track->delivery_failure;
            payload = ev->u.object_received.payload;
            properties = ev->u.object_received.properties;
        }
        break;
    case MOQ_EVENT_OBJECT_CHUNK:
        track = find_track_by_handle(s, ev->u.object_chunk.sub);
        if (s->streaming && track && track->state == SUB_TRACK_ACTIVE) {
            failure = &track->delivery_failure;
            payload = ev->u.object_chunk.chunk;
            properties = ev->u.object_chunk.properties;
        }
        break;
    case MOQ_EVENT_FETCH_OBJECT:
        fetch = find_fetch_by_handle(s, ev->u.fetch_object.fetch);
        if (fetch && (fetch->state == SUB_FETCH_PENDING ||
                      fetch->state == SUB_FETCH_ACTIVE)) {
            failure = &fetch->delivery_failure;
            payload = ev->u.fetch_object.payload;
            properties = ev->u.fetch_object.properties;
        }
        break;
    case MOQ_EVENT_FILL_OBJECT: {
        moq_sub_fill_req_t *fill = find_fill_by_handle(s, ev->u.fill_object.fill);
        if (fill && !fill->delivery_failure.failed) {
            failure = &fill->delivery_failure;
            payload = ev->u.fill_object.payload;
            properties = ev->u.fill_object.properties;
        }
        break;
    }
    default:
        break;
    }
    if (!failure) return SESSION_EVENT_TAKE;
    size_t remaining = s->max_queued_bytes;
    if (!ack_charge(&remaining, s->retained_ack_bytes) ||
        !ack_charge_pair(&remaining, payload, properties)) {
        *failure = (sub_delivery_failure_t){
            .failed = true, .pending = true, .cancel_pending = true };
        if (track) track->state = SUB_TRACK_DONE;
        if (fetch) fetch->state = SUB_FETCH_DONE;
        return SESSION_EVENT_DISCARD;
    }
    remaining = s->max_queued_bytes;
    if (!ack_charge_pair(&remaining, payload, properties) ||
        !retained_refs_fit(s, remaining)) return SESSION_EVENT_LEAVE;
    return SESSION_EVENT_TAKE;
}

moq_result_t moq_sub_poll_local_failure(moq_subscriber_t *sub,
    moq_sub_local_failure_t *out, size_t out_size)
{
    if (!sub || !out || out_size < MOQ_SUB_LOCAL_FAILURE_V0_SIZE)
        return MOQ_ERR_INVAL;
    moq_sub_local_failure_t result = {0};
    sub_delivery_failure_t *found = NULL;
    for (size_t i = 0; i < sub->track_cap && !found; i++) {
        moq_sub_track_t *t = &sub->tracks[i];
        if (t->state != SUB_TRACK_FREE && t->delivery_failure.pending) {
            found = &t->delivery_failure;
            result.owner_kind = MOQ_SUB_LOCAL_FAILURE_TRACK;
            result.track = t;
        }
    }
    for (size_t i = 0; i < sub->fetch_cap && !found; i++) {
        moq_sub_fetch_req_t *r = &sub->fetch_reqs[i];
        if (r->state != SUB_FETCH_FREE && r->delivery_failure.pending) {
            found = &r->delivery_failure;
            result.owner_kind = MOQ_SUB_LOCAL_FAILURE_FETCH;
            result.fetch = r;
        }
    }
    for (size_t i = 0; i < sub->fill_cap && !found; i++) {
        moq_sub_fill_req_t *r = &sub->fill_reqs[i];
        if (r->state != SUB_FILL_FREE && r->delivery_failure.pending) {
            found = &r->delivery_failure;
            result.owner_kind = MOQ_SUB_LOCAL_FAILURE_FILL;
            result.track = r->track;
            result.fill = fill_handle_of(r);
            result.initiating_request_id = r->initiating_request_id;
        }
    }
    if (!found) return MOQ_DONE;
    size_t written = out_size < sizeof(result) ? out_size : sizeof(result);
    result.struct_size = (uint32_t)written;
    result.local_result = MOQ_ERR_BUFFER;
    memcpy(out, &result, written);
    found->pending = false;
    reap_failed_history(sub);
    return MOQ_OK;
}

/* -- Public API ----------------------------------------------------- */

void moq_sub_callbacks_init(moq_sub_callbacks_t *cb)
{
    if (!cb) return;
    memset(cb, 0, sizeof(*cb));
    cb->struct_size = sizeof(*cb);
}

/* Frozen v0 prefix size: the layout before the first appended field
 * (on_goaway). The pointer-only initializer cannot know the caller's storage
 * size, so it touches only this prefix -- safe for a caller compiled against the
 * original (smaller) struct. */
#define MOQ_SUB_CFG_V0_SIZE (offsetof(moq_sub_cfg_t, on_goaway))

void moq_sub_cfg_init(moq_sub_cfg_t *cfg)
{
    if (!cfg) return;
    /* Clear and stamp ONLY the frozen v0 prefix: writing sizeof(*cfg) here would
     * overflow a caller that allocated the smaller original struct. Appended
     * fields (on_goaway, on_subscribe_done, max_queued_object_bytes) stay
     * disabled (struct_size == prefix); callers that want them use
     * moq_sub_cfg_init_sized(). callbacks is inside the prefix. */
    memset(cfg, 0, MOQ_SUB_CFG_V0_SIZE);
    cfg->struct_size = (uint32_t)MOQ_SUB_CFG_V0_SIZE;
    moq_sub_callbacks_init(&cfg->callbacks);
}

void moq_sub_cfg_init_sized(moq_sub_cfg_t *cfg, size_t cfg_size)
{
    if (!cfg) return;
    /* Clear exactly what the caller allocated, never more than this library's
     * struct knows about. An older caller passes a smaller cfg_size (we clear/
     * stamp that prefix); a newer caller's extra trailing fields are left to its
     * own initializer. */
    size_t n = cfg_size < sizeof(*cfg) ? cfg_size : sizeof(*cfg);
    if (n < sizeof(cfg->struct_size)) return;  /* too small to even stamp */
    memset(cfg, 0, n);
    cfg->struct_size = (uint32_t)n;
    if (n >= offsetof(moq_sub_cfg_t, callbacks) + sizeof(cfg->callbacks))
        moq_sub_callbacks_init(&cfg->callbacks);
}

moq_result_t moq_sub_create(moq_session_t *session,
                              const moq_alloc_t *alloc,
                              const moq_sub_cfg_t *cfg,
                              moq_subscriber_t **out)
{
    if (!session || !alloc || !cfg || !out) return MOQ_ERR_INVAL;
    *out = NULL;
    if (cfg->struct_size < offsetof(moq_sub_cfg_t, callbacks) +
        sizeof(cfg->callbacks))
        return MOQ_ERR_INVAL;

/* ABI boundary: the appended callback must start exactly at the
 * v0 sizeof -- the align-rounded end of max_queued_object_bytes, the
 * old struct's last field (probe-verified zero trailing padding, so no
 * reserved spacer is needed). */
_Static_assert(offsetof(moq_sub_cfg_t, on_update_ok) ==
               ((offsetof(moq_sub_cfg_t, max_queued_object_bytes) +
                 sizeof(((moq_sub_cfg_t *)0)->max_queued_object_bytes) +
                 (_Alignof(moq_sub_cfg_t) - 1)) &
                ~(size_t)(_Alignof(moq_sub_cfg_t) - 1)),
               "on_update_ok must sit exactly at the v0 sizeof");
/* ABI boundary: surface_group_ends starts outside the sizeof of the struct that
 * ended with on_update_ok, so an older caller's storage never reaches it. */
_Static_assert(offsetof(moq_sub_cfg_t, surface_group_ends) >=
               ((offsetof(moq_sub_cfg_t, on_update_ok) +
                 sizeof(((moq_sub_cfg_t *)0)->on_update_ok) +
                 (_Alignof(moq_sub_cfg_t) - 1)) &
                ~(size_t)(_Alignof(moq_sub_cfg_t) - 1)),
               "surface_group_ends must not consume old tail padding");
_Static_assert(offsetof(moq_sub_cfg_t, surface_group_ends) ==
               offsetof(moq_sub_cfg_t, _reserved_group_ends) + sizeof(uint32_t),
               "surface_group_ends must follow its reserved slot");
/* The stream-terminal block starts at the complete sizeof of the layout
 * that ends with max_fill_items, which has no tail padding. */
_Static_assert(offsetof(moq_sub_cfg_t, max_stream_terminals) ==
               offsetof(moq_sub_cfg_t, max_fill_items) + sizeof(uint32_t),
               "max_stream_terminals follows max_fill_items");
_Static_assert((offsetof(moq_sub_cfg_t, max_fill_items) + sizeof(uint32_t)) %
               _Alignof(moq_sub_cfg_t) == 0,
               "max_fill_items ends the old layout without tail padding");
_Static_assert(sizeof(moq_sub_stream_terminal_t) ==
               MOQ_SUB_STREAM_TERMINAL_V0_SIZE,
               "stream terminal record v0 size");
/* The fill limits start past the complete sizeof of the layout that ends
 * with surface_group_ends (its tail padding included), on every target. */
_Static_assert(offsetof(moq_sub_cfg_t, max_fills) ==
               ((offsetof(moq_sub_cfg_t, surface_group_ends) + sizeof(bool) +
                 (_Alignof(moq_sub_cfg_t) - 1)) &
                ~(size_t)(_Alignof(moq_sub_cfg_t) - 1)),
               "max_fills must not reuse the group-end tail padding");

#define SUB_CFG_HAS(field) \
    (cfg->struct_size >= offsetof(moq_sub_cfg_t, field) + sizeof(cfg->field))

    size_t tc = cfg->max_tracks ? cfg->max_tracks : SUB_DEFAULT_MAX_TRACKS;
    size_t oc = cfg->max_objects ? cfg->max_objects : SUB_DEFAULT_MAX_OBJECTS;
    size_t fc = SUB_CFG_HAS(max_fetches) && cfg->max_fetches
        ? cfg->max_fetches : SUB_DEFAULT_MAX_FETCHES;
    size_t fic = SUB_CFG_HAS(max_fetch_items) && cfg->max_fetch_items
        ? cfg->max_fetch_items : SUB_DEFAULT_MAX_FETCH_ITEMS;
    size_t cc = SUB_CFG_HAS(max_chunks) && cfg->max_chunks
        ? cfg->max_chunks : SUB_DEFAULT_MAX_CHUNKS;
    bool streaming = SUB_CFG_HAS(streaming_objects) && cfg->streaming_objects;
    bool group_ends = SUB_CFG_HAS(surface_group_ends) &&
                      cfg->surface_group_ends;
    bool stream_terminals = SUB_CFG_HAS(surface_stream_terminals) &&
                            cfg->surface_stream_terminals;
    size_t stc = stream_terminals && cfg->max_stream_terminals
        ? cfg->max_stream_terminals : oc;
    size_t flc = SUB_CFG_HAS(max_fills) && cfg->max_fills
        ? cfg->max_fills : SUB_DEFAULT_MAX_FILLS;
    size_t flic = SUB_CFG_HAS(max_fill_items) && cfg->max_fill_items
        ? cfg->max_fill_items : fic;
    /* Legacy drafts reserve no fill slots or fill item credit. */
    if (!moq_session_supports_fill(session)) {
        flc = 0;
        flic = 0;
    }
    if (fic > SIZE_MAX - flic) return MOQ_ERR_NOMEM;
    size_t hc = fic + flic;
    if (tc > SIZE_MAX / sizeof(moq_sub_track_t) ||
        oc > SIZE_MAX / sizeof(moq_sub_object_t) ||
        fc > SIZE_MAX / sizeof(moq_sub_fetch_req_t) ||
        cc > SIZE_MAX / sizeof(moq_sub_chunk_t) ||
        flc > SIZE_MAX / sizeof(moq_sub_fill_req_t) ||
        hc > SIZE_MAX / sizeof(sub_history_slot_t) ||
        stc > SIZE_MAX / sizeof(sub_stream_terminal_entry_t))
        return MOQ_ERR_NOMEM;
    size_t qbytes = SUB_DEFAULT_MAX_QUEUED_BYTES;
    if (SUB_CFG_HAS(max_queued_object_bytes) && cfg->max_queued_object_bytes) {
        /* Clamp (not truncate) a configured value that cannot fit in size_t. */
        qbytes = cfg->max_queued_object_bytes > (uint64_t)SIZE_MAX
                     ? SIZE_MAX
                     : (size_t)cfg->max_queued_object_bytes;
    }

    moq_subscriber_t *s = (moq_subscriber_t *)alloc->alloc(
        sizeof(moq_subscriber_t), alloc->ctx);
    if (!s) return MOQ_ERR_NOMEM;
    memset(s, 0, sizeof(*s));

    s->tracks = (moq_sub_track_t *)alloc->alloc(
        tc * sizeof(moq_sub_track_t), alloc->ctx);
    if (!s->tracks) {
        alloc->free(s, sizeof(moq_subscriber_t), alloc->ctx);
        return MOQ_ERR_NOMEM;
    }
    memset(s->tracks, 0, tc * sizeof(moq_sub_track_t));

    s->objects = (moq_sub_object_t *)alloc->alloc(
        oc * sizeof(moq_sub_object_t), alloc->ctx);
    if (!s->objects) {
        alloc->free(s->tracks, tc * sizeof(moq_sub_track_t), alloc->ctx);
        alloc->free(s, sizeof(moq_subscriber_t), alloc->ctx);
        return MOQ_ERR_NOMEM;
    }
    memset(s->objects, 0, oc * sizeof(moq_sub_object_t));

    s->session = session;
    s->alloc = *alloc;
    size_t sc = SUB_DEFAULT_MAX_STATUS;
    s->status_reqs = (moq_sub_status_req_t *)alloc->alloc(
        sc * sizeof(moq_sub_status_req_t), alloc->ctx);
    if (!s->status_reqs) {
        alloc->free(s->objects, oc * sizeof(moq_sub_object_t), alloc->ctx);
        alloc->free(s->tracks, tc * sizeof(moq_sub_track_t), alloc->ctx);
        alloc->free(s, sizeof(moq_subscriber_t), alloc->ctx);
        return MOQ_ERR_NOMEM;
    }
    memset(s->status_reqs, 0, sc * sizeof(moq_sub_status_req_t));

    s->status_results = (moq_sub_status_outcome_t *)alloc->alloc(
        sc * sizeof(moq_sub_status_outcome_t), alloc->ctx);
    if (!s->status_results) {
        alloc->free(s->status_reqs, sc * sizeof(moq_sub_status_req_t), alloc->ctx);
        alloc->free(s->objects, oc * sizeof(moq_sub_object_t), alloc->ctx);
        alloc->free(s->tracks, tc * sizeof(moq_sub_track_t), alloc->ctx);
        alloc->free(s, sizeof(moq_subscriber_t), alloc->ctx);
        return MOQ_ERR_NOMEM;
    }
    memset(s->status_results, 0, sc * sizeof(moq_sub_status_outcome_t));

    s->fetch_reqs = (moq_sub_fetch_req_t *)alloc->alloc(
        fc * sizeof(moq_sub_fetch_req_t), alloc->ctx);
    if (!s->fetch_reqs) {
        alloc->free(s->status_results,
            sc * sizeof(moq_sub_status_outcome_t), alloc->ctx);
        alloc->free(s->status_reqs,
            sc * sizeof(moq_sub_status_req_t), alloc->ctx);
        alloc->free(s->objects, oc * sizeof(moq_sub_object_t), alloc->ctx);
        alloc->free(s->tracks, tc * sizeof(moq_sub_track_t), alloc->ctx);
        alloc->free(s, sizeof(moq_subscriber_t), alloc->ctx);
        return MOQ_ERR_NOMEM;
    }
    memset(s->fetch_reqs, 0, fc * sizeof(moq_sub_fetch_req_t));

    s->history = (sub_history_slot_t *)alloc->alloc(
        hc * sizeof(sub_history_slot_t), alloc->ctx);
    if (!s->history) {
        alloc->free(s->fetch_reqs,
            fc * sizeof(moq_sub_fetch_req_t), alloc->ctx);
        alloc->free(s->status_results,
            sc * sizeof(moq_sub_status_outcome_t), alloc->ctx);
        alloc->free(s->status_reqs,
            sc * sizeof(moq_sub_status_req_t), alloc->ctx);
        alloc->free(s->objects, oc * sizeof(moq_sub_object_t), alloc->ctx);
        alloc->free(s->tracks, tc * sizeof(moq_sub_track_t), alloc->ctx);
        alloc->free(s, sizeof(moq_subscriber_t), alloc->ctx);
        return MOQ_ERR_NOMEM;
    }
    memset(s->history, 0, hc * sizeof(sub_history_slot_t));
    for (size_t i = 0; i < hc; i++)
        s->history[i].next = i + 1 < hc ? i + 1 : SIZE_MAX;
    s->history_cap = hc;
    s->history_free = 0;
    s->fetch_queue.head = s->fetch_queue.tail = SIZE_MAX;
    s->fill_queue.head = s->fill_queue.tail = SIZE_MAX;

    s->chunks = (moq_sub_chunk_t *)alloc->alloc(
        cc * sizeof(moq_sub_chunk_t), alloc->ctx);
    if (!s->chunks) {
        alloc->free(s->history,
            hc * sizeof(sub_history_slot_t), alloc->ctx);
        alloc->free(s->fetch_reqs,
            fc * sizeof(moq_sub_fetch_req_t), alloc->ctx);
        alloc->free(s->status_results,
            sc * sizeof(moq_sub_status_outcome_t), alloc->ctx);
        alloc->free(s->status_reqs,
            sc * sizeof(moq_sub_status_req_t), alloc->ctx);
        alloc->free(s->objects, oc * sizeof(moq_sub_object_t), alloc->ctx);
        alloc->free(s->tracks, tc * sizeof(moq_sub_track_t), alloc->ctx);
        alloc->free(s, sizeof(moq_subscriber_t), alloc->ctx);
        return MOQ_ERR_NOMEM;
    }
    memset(s->chunks, 0, cc * sizeof(moq_sub_chunk_t));

    /* One Group end per finished subgroup: the object bound is the natural
     * bound for them too. */
    if (group_ends) {
        s->group_ends = (sub_group_end_entry_t *)alloc->alloc(
            oc * sizeof(sub_group_end_entry_t), alloc->ctx);
        if (!s->group_ends) {
            alloc->free(s->chunks, cc * sizeof(moq_sub_chunk_t), alloc->ctx);
            alloc->free(s->history,
                hc * sizeof(sub_history_slot_t), alloc->ctx);
            alloc->free(s->fetch_reqs,
                fc * sizeof(moq_sub_fetch_req_t), alloc->ctx);
            alloc->free(s->status_results,
                sc * sizeof(moq_sub_status_outcome_t), alloc->ctx);
            alloc->free(s->status_reqs,
                sc * sizeof(moq_sub_status_req_t), alloc->ctx);
            alloc->free(s->objects, oc * sizeof(moq_sub_object_t), alloc->ctx);
            alloc->free(s->tracks, tc * sizeof(moq_sub_track_t), alloc->ctx);
            alloc->free(s, sizeof(moq_subscriber_t), alloc->ctx);
            return MOQ_ERR_NOMEM;
        }
        memset(s->group_ends, 0, oc * sizeof(sub_group_end_entry_t));
        s->ge_cap = oc;
        s->surface_group_ends = true;
    }

    if (flc > 0) {
        s->fill_reqs = (moq_sub_fill_req_t *)alloc->alloc(
            flc * sizeof(moq_sub_fill_req_t), alloc->ctx);
        if (!s->fill_reqs) {
            if (s->group_ends)
                alloc->free(s->group_ends,
                    oc * sizeof(sub_group_end_entry_t), alloc->ctx);
            alloc->free(s->chunks, cc * sizeof(moq_sub_chunk_t), alloc->ctx);
            alloc->free(s->history,
                hc * sizeof(sub_history_slot_t), alloc->ctx);
            alloc->free(s->fetch_reqs,
                fc * sizeof(moq_sub_fetch_req_t), alloc->ctx);
            alloc->free(s->status_results,
                sc * sizeof(moq_sub_status_outcome_t), alloc->ctx);
            alloc->free(s->status_reqs,
                sc * sizeof(moq_sub_status_req_t), alloc->ctx);
            alloc->free(s->objects, oc * sizeof(moq_sub_object_t), alloc->ctx);
            alloc->free(s->tracks, tc * sizeof(moq_sub_track_t), alloc->ctx);
            alloc->free(s, sizeof(moq_subscriber_t), alloc->ctx);
            return MOQ_ERR_NOMEM;
        }
        memset(s->fill_reqs, 0, flc * sizeof(moq_sub_fill_req_t));
    }

    s->callbacks = cfg->callbacks;
    s->fill_cap = flc;
    s->fill_queue.cap = flic;
    s->track_cap = tc;
    s->obj_cap = oc;
    s->status_cap = sc;
    s->sr_cap = sc;
    s->fetch_cap = fc;
    s->fetch_queue.cap = fic;
    s->chunk_cap = cc;
    s->streaming = streaming;
    s->max_queued_bytes = qbytes;
    if (SUB_CFG_HAS(on_goaway))
        s->on_goaway = cfg->on_goaway;
    if (SUB_CFG_HAS(on_subscribe_done))
        s->on_subscribe_done = cfg->on_subscribe_done;
    if (SUB_CFG_HAS(on_update_ok))
        s->on_update_ok = cfg->on_update_ok;

#undef SUB_CFG_HAS

    for (size_t i = 0; i < tc; i++)
        s->tracks[i].sub = s;
    for (size_t i = 0; i < sc; i++)
        s->status_reqs[i].sub = s;
    for (size_t i = 0; i < fc; i++)
        s->fetch_reqs[i].sub = s;
    for (size_t i = 0; i < flc; i++)
        s->fill_reqs[i].sub = s;

    /* Last: everything above is in place, so destroy unwinds a failure. */
    if (stream_terminals) {
        s->stream_terminals = (sub_stream_terminal_entry_t *)alloc->alloc(
            stc * sizeof(sub_stream_terminal_entry_t), alloc->ctx);
        if (!s->stream_terminals) {
            moq_sub_destroy(s);
            return MOQ_ERR_NOMEM;
        }
        memset(s->stream_terminals, 0, stc * sizeof(sub_stream_terminal_entry_t));
        s->st_cap = stc;
        s->surface_stream_terminals = true;
    }

    *out = s;
    return MOQ_OK;
}

void moq_sub_destroy(moq_subscriber_t *sub)
{
    if (!sub) return;

    for (size_t i = 0; i < sub->track_cap; i++)
        track_result_release(sub, &sub->tracks[i]);

    /* Release pending and queued object refs. */
    if (sub->has_pending) {
        if (sub->pending.payload) moq_rcbuf_decref(sub->pending.payload);
        if (sub->pending.properties) moq_rcbuf_decref(sub->pending.properties);
    }
    while (sub->obj_head < sub->obj_tail) {
        moq_sub_object_t *o = &sub->objects[sub->obj_head % sub->obj_cap];
        if (o->payload) moq_rcbuf_decref(o->payload);
        if (o->properties) moq_rcbuf_decref(o->properties);
        sub->obj_head++;
    }

    if (sub->has_pending_fi)
        fetch_item_release_refs(&sub->pending_fi);
    if (sub->has_pending_fli)
        fill_item_release_refs(&sub->pending_fli);
    for (size_t i = 0; i < sub->history_cap; i++) {
        sub_history_slot_t *slot = &sub->history[i];
        if (slot->kind == SUB_HISTORY_FETCH)
            fetch_item_release_refs(&slot->u.fetch);
        else if (slot->kind == SUB_HISTORY_FILL)
            fill_item_release_refs(&slot->u.fill);
    }

    if (sub->has_pending_chunk)
        chunk_release_refs(&sub->pending_chunk);
    while (sub->chunk_head < sub->chunk_tail) {
        moq_sub_chunk_t *c = &sub->chunks[sub->chunk_head % sub->chunk_cap];
        chunk_release_refs(c);
        sub->chunk_head++;
    }

    moq_alloc_t alloc = sub->alloc;
    if (sub->stream_terminals)
        alloc.free(sub->stream_terminals,
            sub->st_cap * sizeof(sub_stream_terminal_entry_t), alloc.ctx);
    if (sub->group_ends)
        alloc.free(sub->group_ends,
            sub->ge_cap * sizeof(sub_group_end_entry_t), alloc.ctx);
    if (sub->fill_reqs)
        alloc.free(sub->fill_reqs,
            sub->fill_cap * sizeof(moq_sub_fill_req_t), alloc.ctx);
    alloc.free(sub->chunks,
        sub->chunk_cap * sizeof(moq_sub_chunk_t), alloc.ctx);
    alloc.free(sub->history,
        sub->history_cap * sizeof(sub_history_slot_t), alloc.ctx);
    alloc.free(sub->fetch_reqs,
        sub->fetch_cap * sizeof(moq_sub_fetch_req_t), alloc.ctx);
    alloc.free(sub->status_results,
        sub->sr_cap * sizeof(moq_sub_status_outcome_t), alloc.ctx);
    alloc.free(sub->status_reqs,
        sub->status_cap * sizeof(moq_sub_status_req_t), alloc.ctx);
    alloc.free(sub->objects,
        sub->obj_cap * sizeof(moq_sub_object_t), alloc.ctx);
    alloc.free(sub->tracks,
        sub->track_cap * sizeof(moq_sub_track_t), alloc.ctx);
    alloc.free(sub, sizeof(moq_subscriber_t), alloc.ctx);
}

/* The sizeof of the layout through auth_token_count: what a caller built
 * before `location` was appended allocates and passes. */
#define MOQ_SUB_TRACK_CFG_SIZE_THROUGH_AUTH_TOKEN_COUNT \
    ((offsetof(moq_sub_track_cfg_t, auth_token_count) + \
      sizeof(((moq_sub_track_cfg_t *)0)->auth_token_count) + \
      (_Alignof(moq_sub_track_cfg_t) - 1)) & \
     ~(_Alignof(moq_sub_track_cfg_t) - 1))

/* Parenthesized: defines the symbol, not the header's sized macro. */
void (moq_sub_track_cfg_init)(moq_sub_track_cfg_t *cfg)
{
    moq_sub_track_cfg_init_sized(cfg, MOQ_SUB_TRACK_CFG_SIZE_THROUGH_AUTH_TOKEN_COUNT);
}

void moq_sub_track_cfg_init_sized(moq_sub_track_cfg_t *cfg, size_t cfg_size)
{
    if (!cfg) return;
    size_t n = cfg_size < sizeof(*cfg) ? cfg_size : sizeof(*cfg);
    if (n < sizeof(cfg->struct_size)) return;   /* too small to even stamp */
    memset(cfg, 0, n);
    cfg->struct_size = (uint32_t)n;
    if (n >= offsetof(moq_sub_track_cfg_t, filter) + sizeof(cfg->filter))
        cfg->filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
}

moq_result_t moq_sub_subscribe(moq_subscriber_t *sub,
                                 const moq_sub_track_cfg_t *cfg,
                                 uint64_t now_us,
                                 moq_sub_track_t **out)
{
    if (!sub || !cfg || !out) return MOQ_ERR_INVAL;
    *out = NULL;
#define STCFG_MIN offsetof(moq_sub_track_cfg_t, auth_tokens)
    if (cfg->struct_size < STCFG_MIN) return MOQ_ERR_INVAL;
#define STCFG_HAS(f) \
    (cfg->struct_size >= offsetof(moq_sub_track_cfg_t, f) + sizeof(cfg->f))
    if (sub->closed) return MOQ_ERR_CLOSED;

    const moq_fill_cfg_t *fill = STCFG_HAS(fill) ? cfg->fill : NULL;
    /* Refused before anything is allocated or sent, as the session does. */
    if (fill && !moq_session_supports_fill(sub->session))
        return MOQ_ERR_UNSUPPORTED;
    /* The session refuses a selection its draft cannot carry
     * (MOQ_ERR_UNSUPPORTED) before anything is sent or kept here. */
    const uint64_t *rendezvous = STCFG_HAS(rendezvous_timeout_ms)
        ? cfg->rendezvous_timeout_ms : NULL;

    moq_sub_track_t *t = alloc_track(sub);
    if (!t) return MOQ_ERR_WOULD_BLOCK;

    moq_sub_fill_req_t *fr = NULL;
    if (fill) {
        moq_result_t rc = alloc_fill_req(sub, t, &fill_identities, &fr);
        if (rc != MOQ_OK) return rc;
    }

    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = cfg->track_namespace;
    scfg.track_name = cfg->track_name;
    scfg.filter = cfg->filter;
    if (cfg->has_subscriber_priority) {
        scfg.has_subscriber_priority = true;
        scfg.subscriber_priority = cfg->subscriber_priority;
    }
    if (STCFG_HAS(auth_token_count) && cfg->auth_token_count > 0) {
        scfg.auth_tokens = cfg->auth_tokens;
        scfg.auth_token_count = cfg->auth_token_count;
    }
    if (STCFG_HAS(location) && cfg->location)
        scfg.location = cfg->location;
    scfg.fill = fill;
    scfg.has_rendezvous_timeout = rendezvous != NULL;
    scfg.rendezvous_timeout_ms = rendezvous ? *rendezvous : 0;
    if (STCFG_HAS(object_delivery_timeout_ms) && cfg->object_delivery_timeout_ms) {
        scfg.delivery_timeout_flags |= MOQ_SUBSCRIBE_OBJECT_DELIVERY_TIMEOUT;
        scfg.object_delivery_timeout_ms = *cfg->object_delivery_timeout_ms;
    }
    if (STCFG_HAS(subgroup_delivery_timeout_ms) && cfg->subgroup_delivery_timeout_ms) {
        scfg.delivery_timeout_flags |= MOQ_SUBSCRIBE_SUBGROUP_DELIVERY_TIMEOUT;
        scfg.subgroup_delivery_timeout_ms = *cfg->subgroup_delivery_timeout_ms;
    }
#undef STCFG_HAS
#undef STCFG_MIN

    moq_subscription_t h;
    moq_result_t rc = moq_session_subscribe(sub->session, &scfg, now_us, &h);
    if (rc < 0) {
        if (fr) free_fill_req(fr);
        return rc;
    }

    t->state = SUB_TRACK_PENDING;
    t->handle = h;
    memset(&t->delivery_failure, 0, sizeof(t->delivery_failure));
    *out = t;
    return MOQ_OK;
}

moq_result_t moq_sub_unsubscribe(moq_subscriber_t *sub,
                                   moq_sub_track_t *track,
                                   uint64_t now_us)
{
    if (!sub || !track) return MOQ_ERR_INVAL;
    if (track->sub != sub) return MOQ_ERR_INVAL;
    if (track->state != SUB_TRACK_ACTIVE &&
        track->state != SUB_TRACK_PENDING)
        return MOQ_ERR_WRONG_STATE;

    moq_result_t rc = moq_session_unsubscribe(sub->session,
        track->handle, now_us);
    if (rc < 0) return rc;

    track->state = SUB_TRACK_DONE;
    return MOQ_OK;
}

/* A queued or held stream terminal names `track`. */
static bool track_has_stream_terminals(const moq_subscriber_t *s,
                                       const moq_sub_track_t *track)
{
    if (s->has_pending_st && s->pending_st.track == track) return true;
    for (size_t n = 0; n < s->st_tail - s->st_head; n++)
        if (s->stream_terminals[(s->st_head + n) % s->st_cap].rec.track == track)
            return true;
    return false;
}

bool moq_sub_track_has_queued_input(const moq_subscriber_t *s,
                                    const moq_sub_track_t *track)
{
    if (!s || !track) return false;
    /* Only an owned slot is read: the pointer is compared first. */
    bool owned = false;
    for (size_t i = 0; i < s->track_cap && !owned; i++)
        owned = &s->tracks[i] == track;
    if (!owned || track->state == SUB_TRACK_FREE) return false;
    if ((s->has_pending && s->pending.track == track) ||
        (s->has_pending_chunk && s->pending_chunk.track == track) ||
        (s->has_pending_fli && s->pending_fli.track == track &&
         s->pending_fli.kind == MOQ_SUB_FILL_OBJECT))
        return true;
    for (size_t n = 0; n < s->obj_tail - s->obj_head; n++)
        if (s->objects[(s->obj_head + n) % s->obj_cap].track == track) return true;
    for (size_t n = 0; n < s->chunk_tail - s->chunk_head; n++)
        if (s->chunks[(s->chunk_head + n) % s->chunk_cap].track == track) return true;
    size_t idx = s->fill_queue.head;
    for (size_t n = 0; n < s->fill_queue.count; n++) {
        const sub_history_slot_t *slot = &s->history[idx];
        if (slot->u.fill.track == track && slot->u.fill.kind == MOQ_SUB_FILL_OBJECT)
            return true;
        idx = slot->next;
    }
    return false;
}

static bool track_has_facade_refs(const moq_subscriber_t *s,
    const moq_sub_track_t *track)
{
    for (size_t i = 0; i < s->fill_cap; i++)
        if (s->fill_reqs[i].state != SUB_FILL_FREE &&
            s->fill_reqs[i].track == track)
            return true;
    if ((s->has_pending && s->pending.track == track) ||
        (s->has_pending_fli && s->pending_fli.track == track) ||
        (s->has_pending_chunk && s->pending_chunk.track == track))
        return true;
    for (size_t n = 0; n < s->obj_tail - s->obj_head; n++)
        if (s->objects[(s->obj_head + n) % s->obj_cap].track == track)
            return true;
    for (size_t n = 0; n < s->chunk_tail - s->chunk_head; n++)
        if (s->chunks[(s->chunk_head + n) % s->chunk_cap].track == track)
            return true;
    /* Queued Group ends name the track too. Holding the slot for them is a
     * D22 strengthening of the documented "poll them before releasing" rule:
     * a released slot can be reused, and a record naming it would then name
     * the new subscription. */
    if (s->has_pending_ge && s->pending_ge.end.track == track) return true;
    for (size_t n = 0; n < s->ge_tail - s->ge_head; n++)
        if (s->group_ends[(s->ge_head + n) % s->ge_cap].end.track == track)
            return true;
    if (track_has_stream_terminals(s, track)) return true;
    for (size_t i = 0; i < s->history_cap; i++)
        if (s->history[i].kind == SUB_HISTORY_FILL &&
            s->history[i].u.fill.track == track)
            return true;
    return false;
}

moq_result_t moq_sub_release_track(moq_subscriber_t *sub,
                                   moq_sub_track_t *track)
{
    if (!sub || !track) return MOQ_ERR_INVAL;
    if (track->sub != sub) return MOQ_ERR_INVAL;
    /* Only a terminal track may be released; a live one must be unsubscribed
     * first so the session sends UNSUBSCRIBE. The subscription is already
     * complete here, so no session message is needed -- just free the slot. */
    if (track->state != SUB_TRACK_DONE && track->state != SUB_TRACK_ERROR)
        return MOQ_ERR_WRONG_STATE;
    if (track->delivery_failure.failed &&
        !delivery_failure_settled(&track->delivery_failure))
        return MOQ_ERR_WRONG_STATE;
    if (moq_session_supports_fill(sub->session) &&
        track_has_facade_refs(sub, track))
        return MOQ_ERR_WRONG_STATE;
    /* On every profile: a stream terminal names the slot it was queued for. */
    if (track_has_stream_terminals(sub, track))
        return MOQ_ERR_WRONG_STATE;
    track_result_release(sub, track);
    track->state = SUB_TRACK_FREE;
    track->error_code = 0;
    return MOQ_OK;
}

moq_result_t moq_sub_get_track_migration(const moq_subscriber_t *sub,
    const moq_sub_track_t *track, moq_request_migration_t *out, size_t out_size)
{
    if (!sub || !track) return MOQ_ERR_INVAL;
    /* Compare identities before dereferencing a possibly foreign pointer. */
    for (size_t i = 0; i < sub->track_cap; i++) {
        const moq_sub_track_t *t = &sub->tracks[i];
        if (t != track) continue;
        if (t->state == SUB_TRACK_FREE) return MOQ_ERR_INVAL;
        return facade_migration_get(&t->migration, out, out_size);
    }
    return MOQ_ERR_INVAL;
}

moq_result_t moq_sub_get_track_result(const moq_subscriber_t *sub,
    const moq_sub_track_t *track, moq_sub_track_result_t *out, size_t out_size)
{
    if (!sub || !track || !out || out_size < MOQ_SUB_TRACK_RESULT_V0_SIZE)
        return MOQ_ERR_INVAL;
    /* Compare identities before dereferencing a possibly foreign pointer. */
    for (size_t i = 0; i < sub->track_cap; i++) {
        const moq_sub_track_t *t = &sub->tracks[i];
        if (t != track) continue;
        if (t->state == SUB_TRACK_FREE) return MOQ_ERR_INVAL;
        if (t->initial_result_error != MOQ_OK) return t->initial_result_error;
        moq_sub_track_result_t result = t->initial_result;
        size_t n = out_size < sizeof(result) ? out_size : sizeof(result);
        result.struct_size = (uint32_t)n;
        result.state = (uint32_t)moq_sub_track_get_state(t);
        memcpy(out, &result, n);
        return MOQ_OK;
    }
    return MOQ_ERR_INVAL;
}

/* Frozen v0 prefix: struct_size .. delivery_timeout_us -- the layout before
 * the filter append. The pointer-only initializer touches only this
 * prefix (writing sizeof(current) would overflow an old caller's storage);
 * the appended filter block stays disabled unless _init_sized opts in. */
#define MOQ_SUB_UPDATE_CFG_V0_SIZE \
    (offsetof(moq_sub_update_cfg_t, delivery_timeout_us) + \
     sizeof(((moq_sub_update_cfg_t *)0)->delivery_timeout_us))

/* ABI boundary: the appended filter block must start exactly at the
 * independently derived v0 floor (the align-rounded end of
 * delivery_timeout_us) -- never inside the old struct's storage. */
_Static_assert(offsetof(moq_sub_update_cfg_t, has_filter) ==
               ((offsetof(moq_sub_update_cfg_t, delivery_timeout_us) +
                 sizeof(((moq_sub_update_cfg_t *)0)->delivery_timeout_us) +
                 (_Alignof(moq_sub_update_cfg_t) - 1)) &
                ~(size_t)(_Alignof(moq_sub_update_cfg_t) - 1)),
               "filter block must sit exactly at the v0 sizeof");

void moq_sub_update_cfg_init(moq_sub_update_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, MOQ_SUB_UPDATE_CFG_V0_SIZE);
    cfg->struct_size = (uint32_t)MOQ_SUB_UPDATE_CFG_V0_SIZE;
}

void moq_sub_update_cfg_init_sized(moq_sub_update_cfg_t *cfg,
                                   size_t cfg_size)
{
    if (!cfg) return;
    size_t n = cfg_size < sizeof(*cfg) ? cfg_size : sizeof(*cfg);
    if (n < sizeof(cfg->struct_size)) return;
    memset(cfg, 0, n);
    cfg->struct_size = (uint32_t)n;
}

moq_result_t moq_sub_update_subscription(moq_subscriber_t *sub,
                                           moq_sub_track_t *track,
                                           const moq_sub_update_cfg_t *cfg,
                                           uint64_t now_us)
{
    if (!sub || !track || !cfg) return MOQ_ERR_INVAL;
    if (track->sub != sub) return MOQ_ERR_INVAL;
    /* Frozen v0 floor: an old caller's 24-byte struct stays valid; the
     * appended filter block is whole-block-gated below. */
    if (cfg->struct_size < MOQ_SUB_UPDATE_CFG_V0_SIZE) return MOQ_ERR_INVAL;
    if (sub->closed) return MOQ_ERR_CLOSED;
    if (track->state != SUB_TRACK_ACTIVE) return MOQ_ERR_WRONG_STATE;

    moq_subscription_update_cfg_t ucfg;
    moq_subscription_update_cfg_init_sized(&ucfg, sizeof(ucfg));
    ucfg.has_subscriber_priority = cfg->has_subscriber_priority;
    ucfg.subscriber_priority = cfg->subscriber_priority;
    ucfg.has_forward = cfg->has_forward;
    ucfg.forward = cfg->forward;
    ucfg.has_delivery_timeout = cfg->has_delivery_timeout;
    ucfg.delivery_timeout_us = cfg->delivery_timeout_us;
    if (cfg->struct_size >= offsetof(moq_sub_update_cfg_t, end_group) +
                                sizeof(cfg->end_group) &&
        cfg->has_filter) {
        ucfg.has_filter = true;
        ucfg.filter = cfg->filter;
        ucfg.start_group = cfg->start_group;
        ucfg.start_object = cfg->start_object;
        ucfg.end_group = cfg->end_group;
    }

    if (cfg->struct_size >= offsetof(moq_sub_update_cfg_t, location) +
                                sizeof(cfg->location))
        ucfg.location = cfg->location;
    if (cfg->struct_size >= offsetof(moq_sub_update_cfg_t,
                                     subgroup_delivery_timeout_ms) +
                                sizeof(cfg->subgroup_delivery_timeout_ms)) {
        ucfg.delivery_timeout_flags = cfg->delivery_timeout_flags;
        ucfg.reserved = cfg->reserved;
        ucfg.object_delivery_timeout_ms = cfg->object_delivery_timeout_ms;
        ucfg.subgroup_delivery_timeout_ms = cfg->subgroup_delivery_timeout_ms;
    }

    const moq_fill_cfg_t *fill = NULL;
    if (cfg->struct_size >= offsetof(moq_sub_update_cfg_t, fill) +
                                sizeof(cfg->fill))
        fill = cfg->fill;
    if (fill && !moq_session_supports_fill(sub->session))
        return MOQ_ERR_UNSUPPORTED;
    moq_sub_fill_req_t *fr = NULL;
    if (fill) {
        moq_result_t reserve_rc = alloc_fill_req(sub, track, &fill_identities, &fr);
        if (reserve_rc != MOQ_OK) return reserve_rc;
        ucfg.fill = fill;
    }

    moq_result_t rc = moq_session_update_subscription(sub->session,
        track->handle, &ucfg, now_us);
    if (rc < 0 && fr) free_fill_req(fr);
    return rc;
}

static void fill_sub_object(moq_sub_object_t *o, moq_sub_track_t *t,
                             const moq_object_received_event_t *obj)
{
    o->track = t;
    o->group_id = obj->group_id;
    o->subgroup_id = obj->subgroup_id;
    o->object_id = obj->object_id;
    o->publisher_priority = obj->publisher_priority;
    o->status = obj->status;
    o->end_of_group = obj->end_of_group;
    o->datagram = obj->datagram;
    o->payload = obj->payload;
    o->properties = obj->properties;
}

/* The lane record of a SUBGROUP_FINISHED; true when it was held for a full
 * lane (or must wait behind another held record), so the caller returns
 * WOULD_BLOCK. */
static bool stream_finished_terminal(moq_subscriber_t *sub, moq_sub_track_t *t,
                                     const moq_event_t *ev)
{
    if (!sub->surface_stream_terminals) return false;
    const moq_subgroup_finished_event_t *f = &ev->u.subgroup_finished;
    moq_sub_stream_terminal_t st;
    st_record(&st, t, f->end_of_group ? MOQ_SUB_STREAM_FIN_END_OF_GROUP
                                      : MOQ_SUB_STREAM_FIN,
              f->group_id, f->subgroup_id);
    if (ev->detail_size >= offsetof(moq_subgroup_finished_event_t,
                                    last_object_id) + sizeof(uint64_t) &&
        f->has_last_object) {
        st.has_last_object = true;
        st.last_object_id = f->last_object_id;
    }
    return st_offer(sub, &st);
}

moq_result_t moq_sub_tick(moq_subscriber_t *sub, uint64_t now_us)
{
    if (!sub) return MOQ_ERR_INVAL;
    bool exact_bytes = moq_session_supports_fill(sub->session);
    if (exact_bytes) retry_failed_deliveries(sub, now_us);

    if (sub->has_pending) {
        if (obj_queue_full(sub) || (!exact_bytes && obj_bytes_over_budget(sub)))
            return sub_would_block(sub);
        moq_sub_object_t *o = obj_push(sub);
        *o = sub->pending;
        sub_bytes_add(sub, rcbuf_pair_bytes(o->payload, o->properties));
        memset(&sub->pending, 0, sizeof(sub->pending));
        sub->has_pending = false;
    }

    if (sub->has_pending_fi) {
        bool fi_is_object = sub->pending_fi.kind == MOQ_SUB_FETCH_OBJECT;
        if (fi_queue_full(sub) ||
            (fi_is_object && !exact_bytes && obj_bytes_over_budget(sub)))
            return sub_would_block(sub);
        moq_sub_fetch_item_t *f = fi_push(sub);
        *f = sub->pending_fi;
        if (fi_is_object)
            sub_bytes_add(sub, rcbuf_pair_bytes(f->u.object.payload,
                                                f->u.object.properties));
        memset(&sub->pending_fi, 0, sizeof(sub->pending_fi));
        sub->has_pending_fi = false;
    }

    if (sub->has_pending_chunk) {
        if (chunk_queue_full(sub)) return sub_would_block(sub);
        moq_sub_chunk_t *c = chunk_push(sub);
        *c = sub->pending_chunk;
        memset(&sub->pending_chunk, 0, sizeof(sub->pending_chunk));
        sub->has_pending_chunk = false;
    }

    if (sub->has_pending_ge) {
        if (ge_queue_full(sub)) return sub_would_block(sub);
        ge_push(sub, &sub->pending_ge);
        memset(&sub->pending_ge, 0, sizeof(sub->pending_ge));
        sub->has_pending_ge = false;
    }

    /* After the pending chunk: a reset that cut an object short follows
     * that object's terminal chunk. */
    if (sub->has_pending_st) {
        if (st_queue_full(sub)) return sub_would_block(sub);
        st_push(sub, &sub->pending_st);
        memset(&sub->pending_st, 0, sizeof(sub->pending_st));
        sub->has_pending_st = false;
    }

    if (sub->has_pending_fli) {
        bool is_object = sub->pending_fli.kind == MOQ_SUB_FILL_OBJECT;
        if (fli_queue_full(sub) ||
            (is_object && !exact_bytes && obj_bytes_over_budget(sub)))
            return sub_would_block(sub);
        moq_sub_fill_item_t *f = fli_push(sub);
        *f = sub->pending_fli;
        if (is_object)
            sub_bytes_add(sub, rcbuf_pair_bytes(f->u.object.payload,
                                                f->u.object.properties));
        memset(&sub->pending_fli, 0, sizeof(sub->pending_fli));
        sub->has_pending_fli = false;
    }

    moq_event_t ev;
    for (;;) {
        moq_result_t polled = exact_bytes
            ? session_poll_event_admitted(sub->session, &ev, admit_delivery, sub)
            : (moq_session_poll_events(sub->session, &ev, 1) == 1 ? MOQ_OK : MOQ_DONE);
        if (polled == MOQ_ERR_WOULD_BLOCK) return sub_would_block(sub);
        if (polled == MOQ_DONE) break;
        if (polled != MOQ_OK) return polled;
        switch (ev.kind) {
        case MOQ_EVENT_SUBSCRIBE_OK: {
            moq_sub_track_t *t = find_track_by_handle(sub,
                ev.u.subscribe_ok.sub);
            if (t && !(t->initial_result.flags &
                       MOQ_SUB_TRACK_RESULT_RESPONSE_SEEN)) {
                bool pending = t->state == SUB_TRACK_PENDING;
                t->initial_result_error = track_result_capture(sub, t,
                    &ev.u.subscribe_ok);
                /* The core has already accepted this subscription. Preserve
                 * that lifetime even when the local snapshot cannot be kept. */
                if (pending) {
                    t->state = SUB_TRACK_ACTIVE;
                    sub->stats.subscribe_ok++;
                }
                if (t->initial_result_error != MOQ_OK) {
                    moq_event_cleanup(&ev);
                    return t->initial_result_error;
                }
                if (pending && sub->callbacks.on_subscribed)
                    sub->callbacks.on_subscribed(sub->callbacks.ctx, t);
            }
            break;
        }

        case MOQ_EVENT_SUBSCRIBE_ERROR: {
            moq_sub_track_t *t = find_track_by_handle(sub,
                ev.u.subscribe_error.sub);
            if (t && t->state == SUB_TRACK_PENDING) {
                t->state = SUB_TRACK_ERROR;
                t->error_code = ev.u.subscribe_error.error_code;
                sub->stats.subscribe_errors++;
                if (sub->callbacks.on_subscribe_error)
                    sub->callbacks.on_subscribe_error(
                        sub->callbacks.ctx, t,
                        ev.u.subscribe_error.error_code,
                        ev.u.subscribe_error.reason);
            }
            break;
        }

        case MOQ_EVENT_OBJECT_RECEIVED: {
            moq_object_received_event_t *obj = &ev.u.object_received;
            moq_sub_track_t *t = find_track_by_handle(sub, obj->sub);
            if (t && t->state == SUB_TRACK_ACTIVE) {
                sub->stats.objects_received++;
                if (obj_queue_full(sub) || (!exact_bytes && obj_bytes_over_budget(sub))) {
                    fill_sub_object(&sub->pending, t, obj);
                    sub->has_pending = true;
                    ev.u.object_received.payload = NULL;
                    ev.u.object_received.properties = NULL;
                    moq_event_cleanup(&ev);
                    return sub_would_block(sub);
                }
                moq_sub_object_t *o = obj_push(sub);
                fill_sub_object(o, t, obj);
                sub_bytes_add(sub,
                    rcbuf_pair_bytes(o->payload, o->properties));
                ev.u.object_received.payload = NULL;
                ev.u.object_received.properties = NULL;
            }
            break;
        }

        case MOQ_EVENT_OBJECT_CHUNK: {
            if (!sub->streaming) {
                moq_event_cleanup(&ev);
                return MOQ_ERR_WRONG_STATE;
            }
            moq_object_chunk_event_t *ck = &ev.u.object_chunk;
            moq_sub_track_t *t = find_track_by_handle(sub, ck->sub);
            if (t && t->state == SUB_TRACK_ACTIVE) {
                sub->stats.chunks_received++;
                if (ck->end && ck->terminal == MOQ_OBJECT_TERMINAL_RESET)
                    sub->stats.subgroup_resets++;
                moq_sub_chunk_t sc;
                memset(&sc, 0, sizeof(sc));
                sc.track = t;
                sc.group_id = ck->group_id;
                sc.subgroup_id = ck->subgroup_id;
                sc.object_id = ck->object_id;
                sc.payload_length = ck->payload_length;
                sc.publisher_priority = ck->publisher_priority;
                sc.status = ck->status;
                sc.terminal = ck->terminal;
                sc.begin = ck->begin;
                sc.end = ck->end;
                sc.end_of_group = ck->end_of_group;
                sc.chunk = ck->chunk;
                sc.properties = ck->properties;
                /* A reset that cut this object short is the stream's
                 * terminal, queued behind the chunk. */
                bool cut = sub->surface_stream_terminals && ck->end &&
                           ck->terminal == MOQ_OBJECT_TERMINAL_RESET;
                moq_sub_stream_terminal_t st;
                if (cut) {
                    st_record(&st, t, MOQ_SUB_STREAM_RESET,
                              ck->group_id, ck->subgroup_id);
                    st.error_code = ck->error_code;
                    st.has_cut_object = true;
                    st.cut_object_id = ck->object_id;
                }
                if (chunk_queue_full(sub)) {
                    sub->pending_chunk = sc;
                    sub->has_pending_chunk = true;
                    if (cut) {
                        sub->pending_st = st;
                        sub->has_pending_st = true;
                    }
                    ck->chunk = NULL;
                    ck->properties = NULL;
                    moq_event_cleanup(&ev);
                    return sub_would_block(sub);
                }
                moq_sub_chunk_t *dst = chunk_push(sub);
                *dst = sc;
                ck->chunk = NULL;
                ck->properties = NULL;
                if (cut && st_offer(sub, &st)) {
                    moq_event_cleanup(&ev);
                    return sub_would_block(sub);
                }
            }
            break;
        }

        /* Result ring capacity == status request capacity, and a
         * request slot stays occupied until its result is polled,
         * so the ring can never be full when a request exists. */
        case MOQ_EVENT_TRACK_STATUS_OK: {
            moq_sub_status_req_t *r = find_status_by_handle(sub,
                ev.u.track_status_ok.handle);
            if (r && r->state == SUB_STATUS_PENDING) {
                if (sr_queue_full(sub)) {
                    moq_event_cleanup(&ev);
                    return MOQ_ERR_INTERNAL;
                }
                moq_sub_status_outcome_t *sr = sr_push(sub);
                sr->kind = MOQ_SUB_STATUS_OUTCOME_OK;
                sr->request = r;
                if (ev.u.track_status_ok.has_largest) {
                    sr->flags |= MOQ_SUB_STATUS_OUTCOME_HAS_LARGEST;
                    sr->largest_group = ev.u.track_status_ok.largest_group;
                    sr->largest_object = ev.u.track_status_ok.largest_object;
                }
                if (ev.u.track_status_ok.has_expires) {
                    sr->flags |= MOQ_SUB_STATUS_OUTCOME_HAS_EXPIRES;
                    sr->expires_ms = ev.u.track_status_ok.expires_ms;
                }
                r->state = SUB_STATUS_DONE;
            }
            break;
        }

        case MOQ_EVENT_TRACK_STATUS_ERROR: {
            moq_sub_status_req_t *r = find_status_by_handle(sub,
                ev.u.track_status_error.handle);
            if (r && r->state == SUB_STATUS_PENDING) {
                if (sr_queue_full(sub)) {
                    moq_event_cleanup(&ev);
                    return MOQ_ERR_INTERNAL;
                }
                moq_sub_status_outcome_t *sr = sr_push(sub);
                sr->kind = MOQ_SUB_STATUS_OUTCOME_ERROR;
                sr->request = r;
                sr->error_code = ev.u.track_status_error.error_code;
                if (ev.u.track_status_error.can_retry) {
                    sr->flags |= MOQ_SUB_STATUS_OUTCOME_CAN_RETRY;
                    sr->retry_after_ms = ev.u.track_status_error.retry_after_ms;
                }
                r->state = SUB_STATUS_DONE;
            }
            break;
        }

        case MOQ_EVENT_TRACK_STATUS_TERMINATED: {
            moq_sub_status_req_t *r = find_status_by_handle(sub,
                ev.u.track_status_terminated.handle);
            if (r && r->state == SUB_STATUS_PENDING) {
                if (sr_queue_full(sub)) {
                    moq_event_cleanup(&ev);
                    return MOQ_ERR_INTERNAL;
                }
                moq_sub_status_outcome_t *sr = sr_push(sub);
                sr->kind = MOQ_SUB_STATUS_OUTCOME_TERMINATED;
                sr->request = r;
                sr->cause = ev.u.track_status_terminated.cause;
                sr->local_role = ev.u.track_status_terminated.local_role;
                sr->stream_error_code = ev.u.track_status_terminated.stream_error_code;
                r->state = SUB_STATUS_DONE;
            }
            break;
        }

        case MOQ_EVENT_FETCH_OK: {
            moq_sub_fetch_req_t *r = find_fetch_by_handle(sub,
                ev.u.fetch_ok.fetch);
            if (r && r->state == SUB_FETCH_PENDING) {
                sub->stats.fetch_ok++;
                if (fi_queue_full(sub)) {
                    sub->pending_fi.kind = MOQ_SUB_FETCH_OK;
                    sub->pending_fi.request = r;
                    sub->pending_fi.u.ok.end_of_track = ev.u.fetch_ok.end_of_track;
                    sub->pending_fi.u.ok.end_group = ev.u.fetch_ok.end_group;
                    sub->pending_fi.u.ok.end_object = ev.u.fetch_ok.end_object;
                    sub->has_pending_fi = true;
                    r->state = SUB_FETCH_ACTIVE;
                    moq_event_cleanup(&ev);
                    return sub_would_block(sub);
                }
                moq_sub_fetch_item_t *f = fi_push(sub);
                f->kind = MOQ_SUB_FETCH_OK;
                f->request = r;
                f->u.ok.end_of_track = ev.u.fetch_ok.end_of_track;
                f->u.ok.end_group = ev.u.fetch_ok.end_group;
                f->u.ok.end_object = ev.u.fetch_ok.end_object;
                r->state = SUB_FETCH_ACTIVE;
            }
            break;
        }

        case MOQ_EVENT_FETCH_ERROR: {
            moq_sub_fetch_req_t *r = find_fetch_by_handle(sub,
                ev.u.fetch_error.fetch);
            if (r) {
                r->delivery_failure.cancel_pending = false;
                sub->stats.fetch_errors++;
                if (fi_queue_full(sub)) {
                    sub->pending_fi.kind = MOQ_SUB_FETCH_ERROR;
                    sub->pending_fi.request = r;
                    sub->pending_fi.u.error.error_code = ev.u.fetch_error.error_code;
                    sub->pending_fi.u.error.can_retry = ev.u.fetch_error.can_retry;
                    sub->pending_fi.u.error.retry_after_ms = ev.u.fetch_error.retry_after_ms;
                    sub->has_pending_fi = true;
                    r->state = SUB_FETCH_DONE;
                    moq_event_cleanup(&ev);
                    return sub_would_block(sub);
                }
                moq_sub_fetch_item_t *f = fi_push(sub);
                f->kind = MOQ_SUB_FETCH_ERROR;
                f->request = r;
                f->u.error.error_code = ev.u.fetch_error.error_code;
                f->u.error.can_retry = ev.u.fetch_error.can_retry;
                f->u.error.retry_after_ms = ev.u.fetch_error.retry_after_ms;
                r->state = SUB_FETCH_DONE;
            }
            break;
        }

        case MOQ_EVENT_FETCH_OBJECT: {
            moq_sub_fetch_req_t *r = find_fetch_by_handle(sub,
                ev.u.fetch_object.fetch);
            /* FETCH_OK (draft-16 §9.17) may arrive at any time relative to the
             * FETCH (§9.16) object delivery -- a relay can FIN the FETCH object
             * stream before its FETCH_OK reaches us. Queue objects that arrive
             * while still PENDING; FETCH_OK still owns the PENDING->ACTIVE
             * transition (we do NOT mark ACTIVE here). */
            if (r && (r->state == SUB_FETCH_PENDING ||
                      r->state == SUB_FETCH_ACTIVE)) {
                sub->stats.fetch_objects++;
                if (fi_queue_full(sub) || (!exact_bytes && obj_bytes_over_budget(sub))) {
                    sub->pending_fi.kind = MOQ_SUB_FETCH_OBJECT;
                    sub->pending_fi.request = r;
                    sub->pending_fi.u.object.group_id = ev.u.fetch_object.group_id;
                    sub->pending_fi.u.object.object_id = ev.u.fetch_object.object_id;
                    sub->pending_fi.u.object.publisher_priority = ev.u.fetch_object.publisher_priority;
                    sub->pending_fi.u.object.payload = ev.u.fetch_object.payload;
                    sub->pending_fi.u.object.properties = ev.u.fetch_object.properties;
                    sub->has_pending_fi = true;
                    ev.u.fetch_object.payload = NULL;
                    ev.u.fetch_object.properties = NULL;
                    moq_event_cleanup(&ev);
                    return sub_would_block(sub);
                }
                moq_sub_fetch_item_t *f = fi_push(sub);
                f->kind = MOQ_SUB_FETCH_OBJECT;
                f->request = r;
                f->u.object.group_id = ev.u.fetch_object.group_id;
                f->u.object.object_id = ev.u.fetch_object.object_id;
                f->u.object.publisher_priority = ev.u.fetch_object.publisher_priority;
                f->u.object.payload = ev.u.fetch_object.payload;
                f->u.object.properties = ev.u.fetch_object.properties;
                sub_bytes_add(sub,
                    rcbuf_pair_bytes(f->u.object.payload,
                                     f->u.object.properties));
                ev.u.fetch_object.payload = NULL;
                ev.u.fetch_object.properties = NULL;
            }
            break;
        }

        case MOQ_EVENT_FETCH_GAP: {
            moq_sub_fetch_req_t *r = find_fetch_by_handle(sub,
                ev.u.fetch_gap.fetch);
            /* Same as FETCH_OBJECT: a gap can arrive on the object stream before
             * FETCH_OK. Queue it while PENDING; FETCH_OK owns the transition. */
            if (r && (r->state == SUB_FETCH_PENDING ||
                      r->state == SUB_FETCH_ACTIVE)) {
                sub->stats.fetch_gaps++;
                if (fi_queue_full(sub)) {
                    sub->pending_fi.kind = MOQ_SUB_FETCH_GAP;
                    sub->pending_fi.request = r;
                    sub->pending_fi.u.gap.range_kind = ev.u.fetch_gap.range_kind;
                    sub->pending_fi.u.gap.group_id = ev.u.fetch_gap.group_id;
                    sub->pending_fi.u.gap.object_id = ev.u.fetch_gap.object_id;
                    sub->has_pending_fi = true;
                    moq_event_cleanup(&ev);
                    return sub_would_block(sub);
                }
                moq_sub_fetch_item_t *f = fi_push(sub);
                f->kind = MOQ_SUB_FETCH_GAP;
                f->request = r;
                f->u.gap.range_kind = ev.u.fetch_gap.range_kind;
                f->u.gap.group_id = ev.u.fetch_gap.group_id;
                f->u.gap.object_id = ev.u.fetch_gap.object_id;
            }
            break;
        }

        case MOQ_EVENT_FETCH_RESET: {
            /* Same shape as FETCH_ERROR: terminal for the request, retained
             * as the pending item under queue pressure (the code and its
             * source flags travel with it), slot released when polled. */
            moq_sub_fetch_req_t *r = find_fetch_by_handle(sub,
                ev.u.fetch_reset.fetch);
            if (r) {
                r->delivery_failure.cancel_pending = false;
                if (fi_queue_full(sub)) {
                    sub->pending_fi.kind = MOQ_SUB_FETCH_RESET;
                    sub->pending_fi.request = r;
                    sub->pending_fi.u.reset.error_code = ev.u.fetch_reset.error_code;
                    sub->pending_fi.u.reset.data_stream = ev.u.fetch_reset.data_stream;
                    sub->pending_fi.u.reset.stop_sending = ev.u.fetch_reset.stop_sending;
                    sub->has_pending_fi = true;
                    r->state = SUB_FETCH_DONE;
                    moq_event_cleanup(&ev);
                    return sub_would_block(sub);
                }
                moq_sub_fetch_item_t *f = fi_push(sub);
                f->kind = MOQ_SUB_FETCH_RESET;
                f->request = r;
                f->u.reset.error_code = ev.u.fetch_reset.error_code;
                f->u.reset.data_stream = ev.u.fetch_reset.data_stream;
                f->u.reset.stop_sending = ev.u.fetch_reset.stop_sending;
                r->state = SUB_FETCH_DONE;
            }
            break;
        }

        case MOQ_EVENT_FETCH_COMPLETE: {
            moq_sub_fetch_req_t *r = find_fetch_by_handle(sub,
                ev.u.fetch_complete.fetch);
            if (r && (r->state == SUB_FETCH_ACTIVE || r->delivery_failure.failed)) {
                r->delivery_failure.cancel_pending = false;
                sub->stats.fetch_complete++;
                if (fi_queue_full(sub)) {
                    sub->pending_fi.kind = MOQ_SUB_FETCH_COMPLETE;
                    sub->pending_fi.request = r;
                    sub->has_pending_fi = true;
                    r->state = SUB_FETCH_DONE;
                    moq_event_cleanup(&ev);
                    return sub_would_block(sub);
                }
                moq_sub_fetch_item_t *f = fi_push(sub);
                f->kind = MOQ_SUB_FETCH_COMPLETE;
                f->request = r;
                r->state = SUB_FETCH_DONE;
            }
            break;
        }

        case MOQ_EVENT_FETCH_CANCELLED:
            break;

        case MOQ_EVENT_FILL_CREATED: {
            /* The child this side requested now has its session handle;
             * bind it to the oldest request on that track still awaiting
             * one (children are created in request order). */
            moq_sub_track_t *t = find_track_by_handle(sub, ev.u.fill_created.sub);
            moq_sub_fill_req_t *r = t ? find_fill_awaiting_handle(sub, t) : NULL;
            if (r) {
                r->handle = ev.u.fill_created.fill;
                r->state = SUB_FILL_ACTIVE;
                r->initiating_request_id = ev.u.fill_created.initiating_request_id;
                moq_sub_fill_item_t it;
                memset(&it, 0, sizeof(it));
                it.struct_size = (uint32_t)sizeof(it);
                it.kind = MOQ_SUB_FILL_CREATED;
                it.request = fill_handle_of(r);
                it.track = r->track;
                it.initiating_request_id = ev.u.fill_created.initiating_request_id;
                if (fli_queue_full(sub)) {
                    sub->pending_fli = it;
                    sub->has_pending_fli = true;
                    moq_event_cleanup(&ev);
                    return sub_would_block(sub);
                }
                *fli_push(sub) = it;
            }
            break;
        }

        case MOQ_EVENT_FILL_OBJECT: {
            moq_sub_fill_req_t *r = find_fill_by_handle(sub, ev.u.fill_object.fill);
            if (r && !r->delivery_failure.failed) {
                sub->stats.fill_objects++;
                moq_sub_fill_item_t it;
                memset(&it, 0, sizeof(it));
                it.struct_size = (uint32_t)sizeof(it);
                it.kind = MOQ_SUB_FILL_OBJECT;
                it.request = fill_handle_of(r);
                it.track = r->track;
                it.initiating_request_id = ev.u.fill_object.initiating_request_id;
                it.u.object.group_id = ev.u.fill_object.group_id;
                it.u.object.subgroup_id = ev.u.fill_object.subgroup_id;
                it.u.object.object_id = ev.u.fill_object.object_id;
                it.u.object.publisher_priority = ev.u.fill_object.publisher_priority;
                it.u.object.datagram = ev.u.fill_object.datagram;
                it.u.object.payload = ev.u.fill_object.payload;
                it.u.object.properties = ev.u.fill_object.properties;
                ev.u.fill_object.payload = NULL;
                ev.u.fill_object.properties = NULL;
                if (fli_queue_full(sub) || (!exact_bytes && obj_bytes_over_budget(sub))) {
                    sub->pending_fli = it;
                    sub->has_pending_fli = true;
                    moq_event_cleanup(&ev);
                    return sub_would_block(sub);
                }
                moq_sub_fill_item_t *f = fli_push(sub);
                *f = it;
                sub_bytes_add(sub, rcbuf_pair_bytes(f->u.object.payload,
                                                    f->u.object.properties));
            }
            break;
        }

        case MOQ_EVENT_FILL_GAP: {
            moq_sub_fill_req_t *r = find_fill_by_handle(sub, ev.u.fill_gap.fill);
            if (r && !r->delivery_failure.failed) {
                sub->stats.fill_gaps++;
                moq_sub_fill_item_t it;
                memset(&it, 0, sizeof(it));
                it.struct_size = (uint32_t)sizeof(it);
                it.kind = MOQ_SUB_FILL_GAP;
                it.request = fill_handle_of(r);
                it.track = r->track;
                it.initiating_request_id = ev.u.fill_gap.initiating_request_id;
                it.u.gap.range_kind = ev.u.fill_gap.kind;
                it.u.gap.group_id = ev.u.fill_gap.group_id;
                it.u.gap.object_id = ev.u.fill_gap.object_id;
                if (fli_queue_full(sub)) {
                    sub->pending_fli = it;
                    sub->has_pending_fli = true;
                    moq_event_cleanup(&ev);
                    return sub_would_block(sub);
                }
                *fli_push(sub) = it;
            }
            break;
        }

        case MOQ_EVENT_FILL_TERMINAL: {
            /* Exactly once per fill; the slot is held until the item is
             * polled so queued objects keep their correlation token. A
             * terminal for a child that never got its handle (the session
             * ended it inside the requesting call's own event batch) still
             * resolves through the awaiting-handle lookup. */
            moq_sub_fill_req_t *r = find_fill_by_handle(sub, ev.u.fill_terminal.fill);
            if (!r) {
                moq_sub_track_t *t = find_track_by_handle(sub, ev.u.fill_terminal.sub);
                r = t ? find_fill_awaiting_handle(sub, t) : NULL;
                if (r) r->handle = ev.u.fill_terminal.fill;
            }
            if (r) {
                sub->stats.fill_terminals++;
                moq_sub_fill_item_t it;
                memset(&it, 0, sizeof(it));
                it.struct_size = (uint32_t)sizeof(it);
                it.kind = MOQ_SUB_FILL_TERMINAL;
                it.request = fill_handle_of(r);
                it.track = r->track;
                it.initiating_request_id = ev.u.fill_terminal.initiating_request_id;
                it.u.terminal.reason = ev.u.fill_terminal.reason;
                it.u.terminal.error_domain = ev.u.fill_terminal.error_domain;
                it.u.terminal.error_code = ev.u.fill_terminal.error_code;
                r->state = SUB_FILL_DONE;
                r->delivery_failure.cancel_pending = false;
                if (fli_queue_full(sub)) {
                    sub->pending_fli = it;
                    sub->has_pending_fli = true;
                    moq_event_cleanup(&ev);
                    return sub_would_block(sub);
                }
                *fli_push(sub) = it;
            }
            break;
        }

        case MOQ_EVENT_SUBSCRIPTION_UPDATE_OK: {
            /* success-only acknowledgment of our outbound update. The
             * session already applied the latches; the callback observes
             * post-acknowledgment state. Consumed (queue drains) even when
             * no callback is installed. */
            moq_sub_track_t *t = find_track_by_handle(sub,
                ev.u.subscription_update_ok.sub);
            if (t) {
                sub->stats.subscription_update_ok++;
                if (sub->on_update_ok) {
                    moq_sub_update_result_t res;
                    memset(&res, 0, sizeof(res));
                    res.struct_size = (uint32_t)sizeof(res);
                    res.has_largest = ev.u.subscription_update_ok.has_largest;
                    res.largest_group =
                        ev.u.subscription_update_ok.largest_group;
                    res.largest_object =
                        ev.u.subscription_update_ok.largest_object;
                    res.has_expires = ev.u.subscription_update_ok.has_expires;
                    res.expires_ms = ev.u.subscription_update_ok.expires_ms;
                    sub->on_update_ok(sub->callbacks.ctx, t, &res);
                }
            }
            break;
        }
        case MOQ_EVENT_UNSUBSCRIBED: {
            moq_sub_track_t *t = find_track_by_handle(sub,
                ev.u.unsubscribed.sub);
            if (t) {
                t->state = SUB_TRACK_DONE;
                t->delivery_failure.cancel_pending = false;
                t->delivery_failure.terminal_polled = true;
                sub->stats.unsubscribed++;
                /* The session cancelled it because its Track is malformed:
                 * the application learns through the same terminal callback
                 * a publisher's done uses, with MALFORMED_TRACK as status. */
                bool malformed = ev.detail_size >=
                        offsetof(moq_unsubscribed_event_t, cause) +
                        sizeof(ev.u.unsubscribed.cause) &&
                    ev.u.unsubscribed.cause == MOQ_REQUEST_TERMINAL_MALFORMED_TRACK;
                if (malformed) {
                    sub->stats.malformed_tracks++;
                    if (sub->on_subscribe_done)
                        sub->on_subscribe_done(sub->callbacks.ctx, t,
                                               MOQ_STREAM_ERROR_MALFORMED_TRACK);
                }
            }
            break;
        }

        case MOQ_EVENT_SUBSCRIBE_DONE: {
            moq_sub_track_t *t = find_track_by_handle(sub,
                ev.u.subscribe_done.sub);
            if (t) {
                t->state = SUB_TRACK_DONE;
                t->delivery_failure.cancel_pending = false;
                t->delivery_failure.terminal_polled = true;
                sub->stats.subscribe_done++;
                if (sub->on_subscribe_done)
                    sub->on_subscribe_done(sub->callbacks.ctx, t,
                                           ev.u.subscribe_done.status_code);
            }
            break;
        }

        case MOQ_EVENT_SUBGROUP_FINISHED: {
            moq_sub_track_t *t = find_track_by_handle(sub,
                ev.u.subgroup_finished.sub);
            if (t && t->state == SUB_TRACK_ACTIVE) {
                sub->stats.subgroup_finished++;
                /* The session emits this strictly after the subgroup's last
                 * object, so recording the queue tails here puts the Group
                 * end behind every object of the subgroup. No object is
                 * pending at this point: a pending one returns early. */
                if (sub->surface_group_ends &&
                    ev.u.subgroup_finished.end_of_group) {
                    sub_group_end_entry_t ge;
                    memset(&ge, 0, sizeof(ge));
                    ge.end.track = t;
                    ge.end.group_id = ev.u.subgroup_finished.group_id;
                    ge.end.subgroup_id = ev.u.subgroup_finished.subgroup_id;
                    ge.obj_seq = sub->obj_tail;
                    ge.chunk_seq = sub->chunk_tail;
                    if (ge_queue_full(sub)) {
                        sub->pending_ge = ge;
                        sub->has_pending_ge = true;
                        stream_finished_terminal(sub, t, &ev);
                        moq_event_cleanup(&ev);
                        return sub_would_block(sub);
                    }
                    ge_push(sub, &ge);
                }
                if (stream_finished_terminal(sub, t, &ev)) {
                    moq_event_cleanup(&ev);
                    return sub_would_block(sub);
                }
            }
            break;
        }

        case MOQ_EVENT_SUBGROUP_RESET: {
            moq_sub_track_t *t = find_track_by_handle(sub,
                ev.u.subgroup_reset.sub);
            if (t && t->state == SUB_TRACK_ACTIVE) {
                sub->stats.subgroup_resets++;
                if (sub->surface_stream_terminals) {
                    const moq_subgroup_reset_event_t *r = &ev.u.subgroup_reset;
                    moq_sub_stream_terminal_t st;
                    st_record(&st, t, MOQ_SUB_STREAM_RESET,
                              r->group_id, r->subgroup_id);
                    st.error_code = r->error_code;
                    if (ev.detail_size >= offsetof(moq_subgroup_reset_event_t,
                                                   last_object_id) +
                                          sizeof(uint64_t) &&
                        r->has_last_object) {
                        st.has_last_object = true;
                        st.last_object_id = r->last_object_id;
                    }
                    if (st_offer(sub, &st)) {
                        moq_event_cleanup(&ev);
                        return sub_would_block(sub);
                    }
                }
            }
            break;
        }

        case MOQ_EVENT_REQUEST_REDIRECT: {
            /* The peer refused the request with REDIRECT: the request ended
             * as its error would, and the facade keeps where it went. */
            const moq_request_redirect_event_t *rd = &ev.u.request_redirect;
            if (rd->family == MOQ_REQUEST_FAMILY_SUBSCRIBE) {
                moq_sub_track_t *t = find_track_by_handle(sub, rd->handle.subscription);
                if (t && t->state == SUB_TRACK_PENDING) {
                    facade_migration_set(&t->migration, &sub->alloc, true,
                        rd->can_retry, rd->retry_after_ms, 0, rd->connect_uri,
                        &rd->track_namespace, rd->track_name);
                    t->state = SUB_TRACK_ERROR;
                    t->error_code = rd->error_code;
                    sub->stats.subscribe_errors++;
                    if (sub->callbacks.on_subscribe_error)
                        sub->callbacks.on_subscribe_error(sub->callbacks.ctx, t,
                            rd->error_code, rd->reason);
                }
            } else if (rd->family == MOQ_REQUEST_FAMILY_FETCH) {
                moq_sub_fetch_req_t *r = find_fetch_by_handle(sub, rd->handle.fetch);
                if (r && r->state == SUB_FETCH_PENDING) {
                    r->delivery_failure.cancel_pending = false;
                    sub->stats.fetch_errors++;
                    moq_sub_fetch_item_t it;
                    memset(&it, 0, sizeof(it));
                    it.kind = MOQ_SUB_FETCH_ERROR;
                    it.request = r;
                    it.u.error.error_code = rd->error_code;
                    it.u.error.can_retry = rd->can_retry;
                    it.u.error.retry_after_ms = rd->retry_after_ms;
                    r->state = SUB_FETCH_DONE;
                    if (fi_queue_full(sub)) {
                        sub->pending_fi = it;
                        sub->has_pending_fi = true;
                        moq_event_cleanup(&ev);
                        return sub_would_block(sub);
                    }
                    *fi_push(sub) = it;
                }
            } else if (rd->family == MOQ_REQUEST_FAMILY_TRACK_STATUS) {
                moq_sub_status_req_t *r = find_status_by_handle(sub,
                    rd->handle.track_status);
                if (r && r->state == SUB_STATUS_PENDING) {
                    if (sr_queue_full(sub)) {
                        moq_event_cleanup(&ev);
                        return MOQ_ERR_INTERNAL;
                    }
                    moq_sub_status_outcome_t *sr = sr_push(sub);
                    sr->kind = MOQ_SUB_STATUS_OUTCOME_ERROR;
                    sr->request = r;
                    sr->error_code = rd->error_code;
                    if (rd->can_retry) {
                        sr->flags |= MOQ_SUB_STATUS_OUTCOME_CAN_RETRY;
                        sr->retry_after_ms = rd->retry_after_ms;
                    }
                    r->state = SUB_STATUS_DONE;
                }
            }
            break;
        }

        case MOQ_EVENT_REQUEST_GOAWAY: {
            /* The peer migrated one request (draft-22 §9.2): it ends here, to
             * be re-issued at the new session, and the facade keeps where. A
             * subscription ends through on_subscribe_done with GOING_AWAY
             * ("The subscriber or publisher issued a GOAWAY", §12.4); a fetch
             * through its error item with the GOING_AWAY request error. */
            const moq_request_goaway_event_t *ga = &ev.u.request_goaway;
            if (ga->family == MOQ_REQUEST_FAMILY_TRACK_STATUS) {
                /* A status request ends with its GOING_AWAY error outcome;
                 * polling it frees the slot. The ring holds one outcome per
                 * slot, so a pending request always finds room. */
                moq_sub_status_req_t *r = find_status_by_handle(sub,
                    ga->handle.track_status);
                if (r && r->state == SUB_STATUS_PENDING) {
                    if (sr_queue_full(sub)) {
                        moq_event_cleanup(&ev);
                        return MOQ_ERR_INTERNAL;
                    }
                    moq_sub_status_outcome_t *sr = sr_push(sub);
                    sr->kind = MOQ_SUB_STATUS_OUTCOME_ERROR;
                    sr->request = r;
                    sr->error_code = MOQ_REQUEST_ERROR_GOING_AWAY;
                    r->state = SUB_STATUS_DONE;
                }
            } else if (ga->family == MOQ_REQUEST_FAMILY_SUBSCRIBE) {
                moq_sub_track_t *t = find_track_by_handle(sub, ga->handle.subscription);
                if (t && (t->state == SUB_TRACK_ACTIVE || t->state == SUB_TRACK_PENDING)) {
                    facade_migration_set(&t->migration, &sub->alloc, false,
                        false, 0, ga->timeout_ms, ga->new_session_uri, NULL,
                        (moq_bytes_t){ NULL, 0 });
                    t->state = SUB_TRACK_DONE;
                    t->delivery_failure.cancel_pending = false;
                    t->delivery_failure.terminal_polled = true;
                    sub->stats.subscribe_done++;
                    if (sub->on_subscribe_done)
                        sub->on_subscribe_done(sub->callbacks.ctx, t,
                                               SUB_DONE_GOING_AWAY);
                }
            } else if (ga->family == MOQ_REQUEST_FAMILY_FETCH) {
                moq_sub_fetch_req_t *r = find_fetch_by_handle(sub, ga->handle.fetch);
                if (r && (r->state == SUB_FETCH_PENDING || r->state == SUB_FETCH_ACTIVE)) {
                    r->delivery_failure.cancel_pending = false;
                    sub->stats.fetch_errors++;
                    moq_sub_fetch_item_t it;
                    memset(&it, 0, sizeof(it));
                    it.kind = MOQ_SUB_FETCH_ERROR;
                    it.request = r;
                    it.u.error.error_code = MOQ_REQUEST_ERROR_GOING_AWAY;
                    r->state = SUB_FETCH_DONE;
                    if (fi_queue_full(sub)) {
                        sub->pending_fi = it;
                        sub->has_pending_fi = true;
                        moq_event_cleanup(&ev);
                        return sub_would_block(sub);
                    }
                    *fi_push(sub) = it;
                }
            }
            break;
        }

        case MOQ_EVENT_GOAWAY:
            sub->draining = true;
            if (sub->on_goaway) {
                sub->on_goaway(sub->callbacks.ctx,
                    ev.u.goaway.new_session_uri);
            } else if (sub->callbacks.on_draining) {
                sub->callbacks.on_draining(sub->callbacks.ctx);
            }
            break;

        case MOQ_EVENT_SESSION_CLOSED:
            sub->closed = true;
            for (size_t i = 0; i < sub->track_cap; i++) {
                sub->tracks[i].delivery_failure.cancel_pending = false;
                sub->tracks[i].delivery_failure.terminal_polled = true;
                if (sub->tracks[i].state == SUB_TRACK_ACTIVE ||
                    sub->tracks[i].state == SUB_TRACK_PENDING)
                    sub->tracks[i].state = SUB_TRACK_DONE;
            }
            for (size_t i = 0; i < sub->status_cap; i++)
                if (sub->status_reqs[i].state == SUB_STATUS_PENDING)
                    sub->status_reqs[i].state = SUB_STATUS_DONE;
            for (size_t i = 0; i < sub->fetch_cap; i++) {
                sub->fetch_reqs[i].delivery_failure.cancel_pending = false;
                sub->fetch_reqs[i].delivery_failure.terminal_polled = true;
                if (sub->fetch_reqs[i].state == SUB_FETCH_PENDING ||
                    sub->fetch_reqs[i].state == SUB_FETCH_ACTIVE)
                    sub->fetch_reqs[i].state = SUB_FETCH_DONE;
            }
            for (size_t i = 0; i < sub->fill_cap; i++) {
                if (sub->fill_reqs[i].delivery_failure.failed) {
                    sub->fill_reqs[i].delivery_failure.cancel_pending = false;
                    sub->fill_reqs[i].delivery_failure.terminal_polled = true;
                }
                if (sub->fill_reqs[i].state == SUB_FILL_REQUESTED ||
                    sub->fill_reqs[i].state == SUB_FILL_ACTIVE) {
                    /* Close has no per-child terminal fanout. Retire only
                     * children without a queued terminal; owned items still
                     * pin their track independently until polled. */
                    if (sub->fill_reqs[i].delivery_failure.failed) {
                        sub->fill_reqs[i].state = SUB_FILL_DONE;
                        sub->fill_reqs[i].delivery_failure.cancel_pending = false;
                        sub->fill_reqs[i].delivery_failure.terminal_polled = true;
                    } else {
                        free_fill_req(&sub->fill_reqs[i]);
                    }
                }
            }
            if (sub->callbacks.on_closed)
                sub->callbacks.on_closed(sub->callbacks.ctx,
                    ev.u.closed.code);
            break;

        default:
            break;
        }

        moq_event_cleanup(&ev);
    }

    if (exact_bytes) retry_failed_deliveries(sub, now_us);

    /* Retention failure stays observable without preventing the terminal
     * events that make the failed track releasable, or other track progress. */
    for (size_t i = 0; i < sub->track_cap; i++)
        if (sub->tracks[i].state == SUB_TRACK_ACTIVE &&
            sub->tracks[i].initial_result_error != MOQ_OK)
            return sub->tracks[i].initial_result_error;

    return MOQ_OK;
}

moq_result_t moq_sub_poll_object(moq_subscriber_t *sub,
                                   moq_sub_object_t *out)
{
    if (!sub || !out) return MOQ_ERR_INVAL;
    if (sub->obj_head >= sub->obj_tail) return MOQ_DONE;

    *out = sub->objects[sub->obj_head % sub->obj_cap];
    sub->obj_head++;
    sub_bytes_sub(sub, rcbuf_pair_bytes(out->payload, out->properties));
    sub->stats.objects_polled++;
    return MOQ_OK;
}

moq_result_t moq_sub_poll_group_end(moq_subscriber_t *sub,
                                    moq_sub_group_end_t *out)
{
    if (!sub || !out) return MOQ_ERR_INVAL;
    if (sub->ge_head >= sub->ge_tail) return MOQ_DONE;
    const sub_group_end_entry_t *e =
        &sub->group_ends[sub->ge_head % sub->ge_cap];
    /* Held until the objects and chunks queued before it are polled. */
    if (sub->obj_head < e->obj_seq || sub->chunk_head < e->chunk_seq)
        return MOQ_DONE;
    *out = e->end;
    sub->ge_head++;
    return MOQ_OK;
}

moq_result_t moq_sub_poll_stream_terminal(moq_subscriber_t *sub,
                                          moq_sub_stream_terminal_t *out,
                                          size_t out_size)
{
    if (!sub || !out) return MOQ_ERR_INVAL;
    if (out_size < MOQ_SUB_STREAM_TERMINAL_V0_SIZE) return MOQ_ERR_ABI_MISMATCH;
    if (sub->st_head >= sub->st_tail) return MOQ_DONE;
    const sub_stream_terminal_entry_t *e =
        &sub->stream_terminals[sub->st_head % sub->st_cap];
    /* Held until the objects and chunks queued before it are polled. */
    if (sub->obj_head < e->obj_seq || sub->chunk_head < e->chunk_seq)
        return MOQ_DONE;
    size_t n = out_size < sizeof(e->rec) ? out_size : sizeof(e->rec);
    memcpy(out, &e->rec, n);
    out->struct_size = (uint32_t)n;
    sub->st_head++;
    return MOQ_OK;
}

void moq_sub_object_cleanup(moq_sub_object_t *obj)
{
    if (!obj) return;
    if (obj->payload) { moq_rcbuf_decref(obj->payload); obj->payload = NULL; }
    if (obj->properties) { moq_rcbuf_decref(obj->properties); obj->properties = NULL; }
}

void moq_sub_status_cfg_init(moq_sub_status_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = sizeof(*cfg);
}

moq_result_t moq_sub_track_status(moq_subscriber_t *sub,
                                    const moq_sub_status_cfg_t *cfg,
                                    uint64_t now_us,
                                    moq_sub_status_req_t **out)
{
    if (!sub || !cfg || !out) return MOQ_ERR_INVAL;
    *out = NULL;
#define SSCFG_MIN offsetof(moq_sub_status_cfg_t, auth_tokens)
    if (cfg->struct_size < SSCFG_MIN) return MOQ_ERR_INVAL;
#define SSCFG_HAS(f) \
    (cfg->struct_size >= offsetof(moq_sub_status_cfg_t, f) + sizeof(cfg->f))
    if (sub->closed) return MOQ_ERR_CLOSED;

    moq_sub_status_req_t *r = alloc_status_req(sub);
    if (!r) return MOQ_ERR_WOULD_BLOCK;

    moq_track_status_cfg_t scfg;
    moq_track_status_cfg_init(&scfg);
    scfg.track_namespace = cfg->track_namespace;
    scfg.track_name = cfg->track_name;
    if (SSCFG_HAS(auth_token_count) && cfg->auth_token_count > 0) {
        scfg.auth_tokens = cfg->auth_tokens;
        scfg.auth_token_count = cfg->auth_token_count;
    }
#undef SSCFG_HAS
#undef SSCFG_MIN

    moq_track_status_handle_t h;
    moq_result_t rc = moq_session_track_status(sub->session, &scfg, now_us, &h);
    if (rc < 0) return rc;

    r->state = SUB_STATUS_PENDING;
    r->handle = h;
    *out = r;
    return MOQ_OK;
}

moq_result_t moq_sub_poll_status(moq_subscriber_t *sub,
                                   moq_sub_status_result_t *out)
{
    if (!sub || !out) return MOQ_ERR_INVAL;
    if (sub->sr_head >= sub->sr_tail) return MOQ_DONE;
    const moq_sub_status_outcome_t *sr = &sub->status_results[sub->sr_head % sub->sr_cap];
    if (sr->kind != MOQ_SUB_STATUS_OUTCOME_OK &&
        sr->kind != MOQ_SUB_STATUS_OUTCOME_ERROR)
        return MOQ_ERR_UNSUPPORTED;
    moq_sub_status_result_t result;
    memset(&result, 0, sizeof(result));
    result.kind = sr->kind == MOQ_SUB_STATUS_OUTCOME_OK ? MOQ_SUB_STATUS_OK : MOQ_SUB_STATUS_ERROR;
    result.request = sr->request;
    result.has_largest = (sr->flags & MOQ_SUB_STATUS_OUTCOME_HAS_LARGEST) != 0;
    result.largest_group = sr->largest_group;
    result.largest_object = sr->largest_object;
    result.has_expires = (sr->flags & MOQ_SUB_STATUS_OUTCOME_HAS_EXPIRES) != 0;
    result.expires_ms = sr->expires_ms;
    result.error_code = sr->error_code;
    result.can_retry = (sr->flags & MOQ_SUB_STATUS_OUTCOME_CAN_RETRY) != 0;
    result.retry_after_ms = sr->retry_after_ms;
    *out = result;
    if (sr->request) sr->request->state = SUB_STATUS_FREE;
    sub->sr_head++;
    return MOQ_OK;
}

moq_result_t moq_sub_poll_status_outcome(moq_subscriber_t *sub,
                                        moq_sub_status_outcome_t *out,
                                        size_t out_size)
{
    if (!sub || !out || out_size < MOQ_SUB_STATUS_OUTCOME_V0_SIZE)
        return MOQ_ERR_INVAL;
    if (sub->sr_head >= sub->sr_tail) return MOQ_DONE;
    moq_sub_status_outcome_t result = sub->status_results[sub->sr_head % sub->sr_cap];
    size_t n = out_size < sizeof(result) ? out_size : sizeof(result);
    result.struct_size = (uint32_t)n;
    memcpy(out, &result, n);
    if (result.request) result.request->state = SUB_STATUS_FREE;
    sub->sr_head++;
    return MOQ_OK;
}

#define MOQ_SUB_STATS_V0_SIZE \
    (offsetof(moq_sub_stats_t, closed) + sizeof(bool))

moq_result_t moq_sub_get_stats(const moq_subscriber_t *sub,
                               moq_sub_stats_t *out,
                               size_t out_size)
{
    if (!sub || !out || out_size < MOQ_SUB_STATS_V0_SIZE)
        return MOQ_ERR_INVAL;

    moq_sub_stats_t snap = sub->stats;
    snap.objects_queued =
        (uint64_t)(sub->obj_tail - sub->obj_head + (sub->has_pending ? 1u : 0u));
    snap.chunks_queued =
        (uint64_t)(sub->chunk_tail - sub->chunk_head +
                   (sub->has_pending_chunk ? 1u : 0u));
    snap.fetch_items_queued =
        (uint64_t)(sub->fetch_queue.count +
                   (sub->has_pending_fi ? 1u : 0u));
    snap.fill_items_queued =
        (uint64_t)(sub->fill_queue.count +
                   (sub->has_pending_fli ? 1u : 0u));
    snap.bytes_queued = sub_retained_bytes(sub);
    snap.draining = sub->draining;
    snap.closed = sub->closed;

    size_t n = out_size < sizeof(*out) ? out_size : sizeof(*out);
    snap.struct_size = (uint32_t)n;
    memcpy(out, &snap, n);
    return MOQ_OK;
}

bool moq_sub_is_draining(const moq_subscriber_t *sub)
{
    return sub ? sub->draining : false;
}

moq_sub_track_state_t moq_sub_track_get_state(const moq_sub_track_t *track)
{
    if (!track) return MOQ_SUB_TRACK_DONE;
    switch (track->state) {
    case SUB_TRACK_PENDING: return MOQ_SUB_TRACK_PENDING;
    case SUB_TRACK_ACTIVE:  return MOQ_SUB_TRACK_ACTIVE;
    case SUB_TRACK_ERROR:   return MOQ_SUB_TRACK_ERROR;
    default:                return MOQ_SUB_TRACK_DONE;
    }
}

bool moq_sub_track_is_active(const moq_sub_track_t *track)
{
    return track && track->state == SUB_TRACK_ACTIVE;
}

bool moq_sub_track_get_error(const moq_sub_track_t *track,
                              moq_request_error_t *out_code)
{
    if (!track || track->state != SUB_TRACK_ERROR) return false;
    if (out_code) *out_code = track->error_code;
    return true;
}

/* -- Fetch ---------------------------------------------------------- */

void moq_sub_fetch_cfg_init(moq_sub_fetch_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = sizeof(*cfg);
}

moq_result_t moq_sub_fetch(moq_subscriber_t *sub,
                             const moq_sub_fetch_cfg_t *cfg,
                             uint64_t now_us,
                             moq_sub_fetch_req_t **out)
{
    if (!sub || !cfg || !out) return MOQ_ERR_INVAL;
    *out = NULL;
#define SFCFG_MIN offsetof(moq_sub_fetch_cfg_t, auth_tokens)
    if (cfg->struct_size < SFCFG_MIN) return MOQ_ERR_INVAL;
#define SFCFG_HAS(f) \
    (cfg->struct_size >= offsetof(moq_sub_fetch_cfg_t, f) + sizeof(cfg->f))
    if (sub->closed) return MOQ_ERR_CLOSED;

    moq_sub_fetch_req_t *r = alloc_fetch_req(sub);
    if (!r) return MOQ_ERR_WOULD_BLOCK;

    moq_fetch_cfg_t fcfg;
    /* SIZED: this forwards the caller's auth tokens, which are an APPENDED
     * field past the frozen v0 floor. The pointer-only init stamps only that
     * floor, so the session would ignore them. */
    moq_fetch_cfg_init_sized(&fcfg, sizeof(fcfg));
    fcfg.track_namespace = cfg->track_namespace;
    fcfg.track_name = cfg->track_name;
    fcfg.start_group = cfg->start_group;
    fcfg.start_object = cfg->start_object;
    fcfg.end_group = cfg->end_group;
    fcfg.end_object = cfg->end_object;
    fcfg.group_order = cfg->group_order;
    fcfg.has_subscriber_priority = cfg->has_subscriber_priority;
    fcfg.subscriber_priority = cfg->subscriber_priority;
    if (SFCFG_HAS(auth_token_count) && cfg->auth_token_count > 0) {
        fcfg.auth_tokens = cfg->auth_tokens;
        fcfg.auth_token_count = cfg->auth_token_count;
    }
#undef SFCFG_HAS
#undef SFCFG_MIN

    moq_fetch_t h;
    moq_result_t rc = moq_session_fetch(sub->session, &fcfg, now_us, &h);
    if (rc < 0) return rc;

    r->state = SUB_FETCH_PENDING;
    r->handle = h;
    memset(&r->delivery_failure, 0, sizeof(r->delivery_failure));
    *out = r;
    return MOQ_OK;
}

/* -- Joining fetch -------------------------------------------------- */

void moq_sub_joining_fetch_cfg_init(moq_sub_joining_fetch_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = sizeof(*cfg);
}

moq_result_t moq_sub_joining_fetch(moq_subscriber_t *sub,
                                     const moq_sub_joining_fetch_cfg_t *cfg,
                                     uint64_t now_us,
                                     moq_sub_fetch_req_t **out)
{
    if (!sub || !cfg || !out) return MOQ_ERR_INVAL;
    *out = NULL;
    if (cfg->struct_size < sizeof(moq_sub_joining_fetch_cfg_t))
        return MOQ_ERR_INVAL;
    if (!cfg->track) return MOQ_ERR_INVAL;
    if (cfg->track->sub != sub) return MOQ_ERR_INVAL;
    if (cfg->track->state != SUB_TRACK_ACTIVE) return MOQ_ERR_WRONG_STATE;
    if (sub->closed) return MOQ_ERR_CLOSED;

    moq_sub_fetch_req_t *r = alloc_fetch_req(sub);
    if (!r) return MOQ_ERR_WOULD_BLOCK;

    moq_fetch_cfg_t fcfg;
    moq_fetch_cfg_init(&fcfg);
    fcfg.is_joining = true;
    fcfg.joining_sub = cfg->track->handle;
    fcfg.joining_relative = cfg->relative;
    fcfg.joining_start = cfg->joining_start;
    fcfg.group_order = cfg->group_order;
    fcfg.has_subscriber_priority = cfg->has_subscriber_priority;
    fcfg.subscriber_priority = cfg->subscriber_priority;

    moq_fetch_t h;
    moq_result_t rc = moq_session_fetch(sub->session, &fcfg, now_us, &h);
    if (rc < 0) return rc;

    r->state = SUB_FETCH_PENDING;
    r->handle = h;
    memset(&r->delivery_failure, 0, sizeof(r->delivery_failure));
    *out = r;
    return MOQ_OK;
}

moq_result_t moq_sub_cancel_fetch(moq_subscriber_t *sub,
                                    moq_sub_fetch_req_t *req,
                                    uint64_t now_us)
{
    if (!sub || !req) return MOQ_ERR_INVAL;
    if (req->sub != sub) return MOQ_ERR_INVAL;
    if (req->state != SUB_FETCH_PENDING &&
        req->state != SUB_FETCH_ACTIVE)
        return MOQ_ERR_WRONG_STATE;

    moq_result_t rc = moq_session_fetch_cancel(sub->session,
        req->handle, now_us);
    if (rc < 0) return rc;

    /* Drop any queued/pending items for this request before the slot becomes
     * reusable, so a later fetch reusing the slot pointer cannot inherit them. */
    purge_fetch_items_for(sub, req);

    req->state = SUB_FETCH_FREE;
    return MOQ_OK;
}

moq_result_t moq_sub_poll_fetch(moq_subscriber_t *sub,
                                  moq_sub_fetch_item_t *out)
{
    if (!sub || !out) return MOQ_ERR_INVAL;
    if (!sub->fetch_queue.count) return MOQ_DONE;

    size_t index = sub->fetch_queue.head;
    *out = sub->history[index].u.fetch;
    history_remove(sub, &sub->fetch_queue, index, SIZE_MAX);
    if (out->kind == MOQ_SUB_FETCH_OBJECT)
        sub_bytes_sub(sub, rcbuf_pair_bytes(out->u.object.payload,
                                            out->u.object.properties));
    sub->stats.fetch_items_polled++;

    if (out->request &&
        (out->kind == MOQ_SUB_FETCH_ERROR ||
         out->kind == MOQ_SUB_FETCH_COMPLETE ||
         out->kind == MOQ_SUB_FETCH_RESET)) {
        if (out->request->delivery_failure.failed) {
            out->request->delivery_failure.terminal_polled = true;
            out->request->delivery_failure.cancel_pending = false;
        } else {
            out->request->state = SUB_FETCH_FREE;
        }
    }
    reap_failed_history(sub);

    return MOQ_OK;
}

void moq_sub_fetch_item_cleanup(moq_sub_fetch_item_t *item)
{
    if (!item) return;
    fetch_item_release_refs(item);
}

/* -- Fills ---------------------------------------------------------- */

moq_sub_fill_t moq_sub_track_fill(const moq_sub_track_t *track)
{
    moq_sub_fill_t none = { 0 };
    if (!track || !track->sub) return none;
    const moq_subscriber_t *s = track->sub;
    const moq_sub_fill_req_t *best = NULL;
    for (size_t i = 0; i < s->fill_cap; i++) {
        const moq_sub_fill_req_t *r = &s->fill_reqs[i];
        if (r->state != SUB_FILL_FREE && r->track == track &&
            (!best || r->seq > best->seq))
            best = r;
    }
    return best ? fill_handle_of(best) : none;
}

moq_result_t moq_sub_cancel_fill(moq_subscriber_t *sub,
                                 moq_sub_fill_t fill,
                                 uint64_t now_us)
{
    if (!sub) return MOQ_ERR_INVAL;
    moq_sub_fill_req_t *req = fill_resolve(sub, fill);
    if (!req) return MOQ_ERR_STALE_HANDLE;
    if (req->state != SUB_FILL_ACTIVE || req->cancelled)
        return MOQ_ERR_WRONG_STATE;
    /* The slot stays until the LOCAL_CANCELLED terminal is polled. */
    moq_result_t rc = moq_session_cancel_fill(sub->session, req->handle, now_us);
    if (rc == MOQ_OK) req->cancelled = true;
    return rc;
}

moq_result_t moq_sub_poll_fill(moq_subscriber_t *sub,
                               moq_sub_fill_item_t *out,
                               size_t out_size)
{
    if (!sub || !out || out_size < MOQ_SUB_FILL_ITEM_V0_SIZE)
        return MOQ_ERR_INVAL;
    if (!sub->fill_queue.count) return MOQ_DONE;

    size_t index = sub->fill_queue.head;
    moq_sub_fill_item_t item = sub->history[index].u.fill;
    size_t written = out_size < sizeof(item) ? out_size : sizeof(item);
    item.struct_size = (uint32_t)written;
    memcpy(out, &item, written);
    history_remove(sub, &sub->fill_queue, index, SIZE_MAX);
    if (item.kind == MOQ_SUB_FILL_OBJECT)
        sub_bytes_sub(sub, rcbuf_pair_bytes(item.u.object.payload,
                                            item.u.object.properties));
    sub->stats.fill_items_polled++;

    if (item.kind == MOQ_SUB_FILL_TERMINAL) {
        moq_sub_fill_req_t *r = fill_resolve(sub, item.request);
        if (r && r->delivery_failure.failed) {
            r->delivery_failure.terminal_polled = true;
            r->delivery_failure.cancel_pending = false;
        } else if (r) {
            free_fill_req(r);
        }
    }
    reap_failed_history(sub);

    return MOQ_OK;
}

void moq_sub_fill_item_cleanup(moq_sub_fill_item_t *item)
{
    if (!item) return;
    size_t size = item->struct_size;
    if (size < offsetof(moq_sub_fill_item_t, kind) + sizeof(item->kind) ||
        item->kind != MOQ_SUB_FILL_OBJECT) return;
    if (size >= offsetof(moq_sub_fill_item_t, u.object.payload) +
                sizeof(item->u.object.payload) && item->u.object.payload) {
        moq_rcbuf_decref(item->u.object.payload);
        item->u.object.payload = NULL;
    }
    if (size >= offsetof(moq_sub_fill_item_t, u.object.properties) +
                sizeof(item->u.object.properties) && item->u.object.properties) {
        moq_rcbuf_decref(item->u.object.properties);
        item->u.object.properties = NULL;
    }
}

/* -- Chunk poll ----------------------------------------------------- */

moq_result_t moq_sub_poll_chunk(moq_subscriber_t *sub,
                                  moq_sub_chunk_t *out)
{
    if (!sub || !out) return MOQ_ERR_INVAL;
    if (sub->chunk_head >= sub->chunk_tail) return MOQ_DONE;

    *out = sub->chunks[sub->chunk_head % sub->chunk_cap];
    memset(&sub->chunks[sub->chunk_head % sub->chunk_cap], 0,
        sizeof(moq_sub_chunk_t));
    sub->chunk_head++;
    sub->stats.chunks_polled++;
    return MOQ_OK;
}

void moq_sub_chunk_cleanup(moq_sub_chunk_t *chunk)
{
    if (!chunk) return;
    chunk_release_refs(chunk);
}
