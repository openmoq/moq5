#include "fill_owner_internal.h"
#include <string.h>

static bool parent_valid(moq_fill_parent_t p)
{
    return p.generation != 0 && (p.family == MOQ_FILL_PARENT_SUBSCRIBE ||
        p.family == MOQ_FILL_PARENT_PUBLISH);
}

static bool parent_equal(moq_fill_parent_t a, moq_fill_parent_t b)
{
    return a.family == b.family && a.slot == b.slot && a.generation == b.generation;
}

static bool table_valid(const moq_fill_table_t *t)
{
    return t && t->capacity <= SIZE_MAX / sizeof(*t->entries) &&
        (!t->capacity || t->entries) && t->live <= t->capacity;
}

static bool delivery_valid(const moq_fill_delivery_t *d)
{
    if (d->outcome == MOQ_FILL_OUTCOME_NO_STREAM) return true;
    return d->outcome == MOQ_FILL_OUTCOME_STREAM &&
        (d->group_order == 1 || d->group_order == 2) &&
        (d->end_group > d->start_group || (d->end_group == d->start_group &&
         d->end_object >= d->start_object));
}

static bool delivery_equal(const moq_fill_delivery_t *a,
    const moq_fill_delivery_t *b)
{
    return a->outcome == b->outcome && a->group_order == b->group_order &&
        a->start_group == b->start_group && a->start_object == b->start_object &&
        a->end_group == b->end_group && a->end_object == b->end_object &&
        a->processed_at_us == b->processed_at_us &&
        a->subscriber_priority == b->subscriber_priority &&
        a->has_fill_timeout == b->has_fill_timeout &&
        a->fill_timeout_ms == b->fill_timeout_ms;
}

static const moq_fill_entry_t *lookup(const moq_fill_table_t *t,
    moq_fill_ref_t ref)
{
    if (!table_valid(t) || ref.slot >= t->capacity || !ref.generation) return NULL;
    const moq_fill_entry_t *e = &t->entries[ref.slot];
    return e->active && e->generation == ref.generation &&
        parent_equal(e->parent, ref.parent) ? e : NULL;
}

static moq_fill_entry_t *lookup_mut(moq_fill_table_t *t,
    moq_fill_ref_t ref)
{
    return lookup(t, ref) ? &t->entries[ref.slot] : NULL;
}

static void reason_first(moq_fill_entry_t *e, uint32_t reason)
{
    if (!e->terminal_reason) e->terminal_reason = reason;
}

static bool data_terminal(const moq_fill_entry_t *e)
{
    return e->stream_bound && (e->data_fin || e->data_reset);
}

/* The bound stream is over: its FIN or RESET, or a local receive retirement
 * of a stream we stopped. Only the first two are a peer outcome. */
static bool stream_ended(const moq_fill_entry_t *e)
{
    return data_terminal(e) || (e->stream_bound && e->data_local_retired);
}

static bool no_stream_proven(const moq_fill_entry_t *e)
{
    return e->control == MOQ_FILL_CONTROL_ACCEPTED && e->delivery_known &&
        e->delivery.outcome == MOQ_FILL_OUTCOME_NO_STREAM;
}

static bool stopping(const moq_fill_entry_t *e)
{
    return e->cancel_requested || e->abort_requested || e->parent_cancelled ||
        e->peer_stop || e->control == MOQ_FILL_CONTROL_REJECTED ||
        e->control == MOQ_FILL_CONTROL_RESET;
}

static bool data_settled(const moq_fill_entry_t *e)
{
    if (e->stream_bound) return stream_ended(e) && e->stream_accounted;
    if (no_stream_proven(e)) return true;
    /* Only an outgoing unopened obligation can be removed by parent/request
     * cancellation. An unbound receiver still owns a possible late header. */
    return e->role == MOQ_FILL_PUBLISHER && (e->parent_cancelled ||
        e->control == MOQ_FILL_CONTROL_REJECTED ||
        e->control == MOQ_FILL_CONTROL_RESET);
}

static uint32_t next_action(const moq_fill_entry_t *e)
{
    if (stream_ended(e)) return MOQ_FILL_ACTION_NONE;
    if (e->role == MOQ_FILL_SUBSCRIBER)
        return e->stream_bound && stopping(e) && !e->stop_committed ?
            MOQ_FILL_ACTION_STOP : MOQ_FILL_ACTION_NONE;
    if (e->stream_bound) {
        if (stopping(e)) return MOQ_FILL_ACTION_RESET;
        return e->finish_requested ? MOQ_FILL_ACTION_FIN : MOQ_FILL_ACTION_NONE;
    }
    if (e->parent_cancelled || e->control != MOQ_FILL_CONTROL_ACCEPTED ||
        !e->delivery_known || e->delivery.outcome != MOQ_FILL_OUTCOME_STREAM)
        return MOQ_FILL_ACTION_NONE;
    return MOQ_FILL_ACTION_HEADER;
}

static bool terminal_ready(const moq_fill_entry_t *e)
{
    if (!e->terminal_reason || e->terminal_published) return false;
    if (e->role == MOQ_FILL_SUBSCRIBER)
        return data_terminal(e) || stopping(e) || no_stream_proven(e);
    return data_terminal(e) || (!e->stream_bound && data_settled(e));
}

static bool reclaimable(const moq_fill_entry_t *e)
{
    return e->control != MOQ_FILL_CONTROL_PENDING &&
        (e->control != MOQ_FILL_CONTROL_ACCEPTED || e->delivery_known) &&
        e->terminal_published && e->terminal_retired && data_settled(e);
}

moq_result_t moq_fill_table_init(moq_fill_table_t *t,
    moq_fill_entry_t *storage, size_t capacity)
{
    if (!t || (capacity && !storage) || capacity > SIZE_MAX / sizeof(*storage))
        return MOQ_ERR_INVAL;
    if (capacity) memset(storage, 0, capacity * sizeof(*storage));
    *t = (moq_fill_table_t){.entries = storage, .capacity = capacity};
    return MOQ_OK;
}

moq_result_t moq_fill_prepare(const moq_fill_table_t *t,
    moq_fill_parent_t parent, uint64_t id, uint32_t role,
    const moq_fill_delivery_t *delivery, moq_fill_reservation_t *out)
{
    if (!table_valid(t) || !out || !parent_valid(parent) ||
        (role != MOQ_FILL_SUBSCRIBER && role != MOQ_FILL_PUBLISHER) ||
        (delivery && !delivery_valid(delivery))) return MOQ_ERR_INVAL;
    size_t slot = t->capacity;
    for (size_t i = 0; i < t->capacity; ++i) {
        const moq_fill_entry_t *e = &t->entries[i];
        if (e->active && e->initiating_id == id) return MOQ_ERR_WRONG_STATE;
        if (!e->active && e->generation != UINT64_MAX && slot == t->capacity)
            slot = i;
    }
    if (slot == t->capacity) return MOQ_ERR_REQUEST_BLOCKED;
    moq_fill_reservation_t r = {0};
    r.ref.slot = slot;
    r.ref.generation = t->entries[slot].generation + 1;
    r.ref.parent = parent;
    r.initiating_id = id;
    r.role = role;
    r.delivery_known = delivery != NULL;
    if (delivery) r.delivery = *delivery;
    *out = r;
    return MOQ_OK;
}

moq_result_t moq_fill_check_commit(const moq_fill_table_t *t,
    const moq_fill_reservation_t *r)
{
    if (!table_valid(t) || !r || !parent_valid(r->ref.parent) ||
        (r->role != MOQ_FILL_SUBSCRIBER && r->role != MOQ_FILL_PUBLISHER) ||
        (r->delivery_known && !delivery_valid(&r->delivery))) return MOQ_ERR_INVAL;
    if (r->ref.slot >= t->capacity) return MOQ_ERR_STALE_HANDLE;
    const moq_fill_entry_t *e = &t->entries[r->ref.slot];
    if (e->active || e->generation == UINT64_MAX ||
        r->ref.generation != e->generation + 1) return MOQ_ERR_STALE_HANDLE;
    if (t->live >= t->capacity) return MOQ_ERR_REQUEST_BLOCKED;
    for (size_t i = 0; i < t->capacity; ++i)
        if (t->entries[i].active && t->entries[i].initiating_id == r->initiating_id)
            return MOQ_ERR_WRONG_STATE;
    return MOQ_OK;
}

moq_result_t moq_fill_commit(moq_fill_table_t *t,
    const moq_fill_reservation_t *r)
{
    moq_result_t rc = moq_fill_check_commit(t, r);
    if (rc != MOQ_OK) return rc;
    moq_fill_entry_t e = {0};
    e.active = true;
    e.generation = r->ref.generation;
    e.parent = r->ref.parent;
    e.initiating_id = r->initiating_id;
    e.role = r->role;
    e.delivery_known = r->delivery_known;
    if (r->delivery_known) {
        e.delivery = r->delivery;
    }
    t->entries[r->ref.slot] = e;
    ++t->live;
    return MOQ_OK;
}

moq_result_t moq_fill_find(const moq_fill_table_t *t,
    uint64_t id, moq_fill_ref_t *out)
{
    if (!table_valid(t) || !out) return MOQ_ERR_INVAL;
    for (size_t i = 0; i < t->capacity; ++i) {
        const moq_fill_entry_t *e = &t->entries[i];
        if (e->active && e->initiating_id == id) {
            *out = (moq_fill_ref_t){.slot = i, .generation = e->generation,
                .parent = e->parent};
            return MOQ_OK;
        }
    }
    return MOQ_ERR_STALE_HANDLE;
}

moq_result_t moq_fill_read(const moq_fill_table_t *t,
    moq_fill_ref_t ref, moq_fill_entry_t *out)
{
    if (!out) return MOQ_ERR_INVAL;
    const moq_fill_entry_t *e = lookup(t, ref);
    if (!e) return MOQ_ERR_STALE_HANDLE;
    *out = *e;
    return MOQ_OK;
}

moq_result_t moq_fill_control_response(moq_fill_table_t *t,
    moq_fill_ref_t ref, uint64_t id, bool accepted)
{
    moq_fill_entry_t *e = lookup_mut(t, ref);
    if (!e) return MOQ_ERR_STALE_HANDLE;
    if (e->initiating_id != id) return MOQ_ERR_INVAL;
    if (e->control != MOQ_FILL_CONTROL_PENDING) return MOQ_ERR_WRONG_STATE;
    e->control = accepted ? MOQ_FILL_CONTROL_ACCEPTED : MOQ_FILL_CONTROL_REJECTED;
    if (!accepted) reason_first(e, MOQ_FILL_TERM_CONTROL_REJECTED);
    else if (e->delivery_known && e->delivery.outcome == MOQ_FILL_OUTCOME_NO_STREAM)
        reason_first(e, MOQ_FILL_TERM_NO_STREAM);
    return MOQ_OK;
}

moq_result_t moq_fill_control_reset(moq_fill_table_t *t,
    moq_fill_ref_t ref, uint64_t id)
{
    moq_fill_entry_t *e = lookup_mut(t, ref);
    if (!e) return MOQ_ERR_STALE_HANDLE;
    if (e->initiating_id != id) return MOQ_ERR_INVAL;
    if (e->control == MOQ_FILL_CONTROL_RESET) return MOQ_OK;
    if (e->control != MOQ_FILL_CONTROL_PENDING) return MOQ_ERR_WRONG_STATE;
    e->control = MOQ_FILL_CONTROL_RESET;
    e->parent_cancelled = true;
    reason_first(e, MOQ_FILL_TERM_PARENT_CANCELLED);
    return MOQ_OK;
}

moq_result_t moq_fill_resolve_delivery(moq_fill_table_t *t,
    moq_fill_ref_t ref, uint64_t id, const moq_fill_delivery_t *d)
{
    if (!d || !delivery_valid(d)) return MOQ_ERR_INVAL;
    moq_fill_entry_t *e = lookup_mut(t, ref);
    if (!e) return MOQ_ERR_STALE_HANDLE;
    if (e->initiating_id != id) return MOQ_ERR_INVAL;
    if (e->delivery_known)
        return delivery_equal(&e->delivery, d) ? MOQ_OK : MOQ_ERR_WRONG_STATE;
    if (e->stream_bound && d->outcome == MOQ_FILL_OUTCOME_NO_STREAM) return MOQ_ERR_PROTO;
    e->delivery = *d;
    e->delivery_known = true;
    if (no_stream_proven(e)) reason_first(e, MOQ_FILL_TERM_NO_STREAM);
    return MOQ_OK;
}

moq_result_t moq_fill_cancel(moq_fill_table_t *t, moq_fill_ref_t ref)
{
    moq_fill_entry_t *e = lookup_mut(t, ref);
    if (!e) return MOQ_ERR_STALE_HANDLE;
    if (e->role != MOQ_FILL_SUBSCRIBER) return MOQ_ERR_WRONG_STATE;
    if (e->cancel_requested) return MOQ_OK;
    if (data_terminal(e) || no_stream_proven(e)) return MOQ_ERR_WRONG_STATE;
    e->cancel_requested = true;
    reason_first(e, MOQ_FILL_TERM_CANCELLED);
    return MOQ_OK;
}

moq_result_t moq_fill_abort(moq_fill_table_t *t, moq_fill_ref_t ref)
{
    moq_fill_entry_t *e = lookup_mut(t, ref);
    if (!e) return MOQ_ERR_STALE_HANDLE;
    if (e->role != MOQ_FILL_PUBLISHER) return MOQ_ERR_WRONG_STATE;
    if (e->abort_requested) return MOQ_OK;
    if (data_terminal(e) || (e->delivery_known &&
        e->delivery.outcome == MOQ_FILL_OUTCOME_NO_STREAM) || e->parent_cancelled ||
        e->control == MOQ_FILL_CONTROL_REJECTED ||
        e->control == MOQ_FILL_CONTROL_RESET) return MOQ_ERR_WRONG_STATE;
    e->abort_requested = true;
    reason_first(e, MOQ_FILL_TERM_ABORTED);
    return MOQ_OK;
}

moq_result_t moq_fill_finish(moq_fill_table_t *t, moq_fill_ref_t ref)
{
    moq_fill_entry_t *e = lookup_mut(t, ref);
    if (!e) return MOQ_ERR_STALE_HANDLE;
    if (e->role != MOQ_FILL_PUBLISHER) return MOQ_ERR_WRONG_STATE;
    if (e->finish_requested) return MOQ_OK;
    if (!e->stream_bound || data_terminal(e) || stopping(e)) return MOQ_ERR_WRONG_STATE;
    e->finish_requested = true;
    return MOQ_OK;
}

moq_result_t moq_fill_parent_cancel(moq_fill_table_t *t, moq_fill_ref_t ref)
{
    moq_fill_entry_t *e = lookup_mut(t, ref);
    if (!e) return MOQ_ERR_STALE_HANDLE;
    e->parent_cancelled = true;
    reason_first(e, MOQ_FILL_TERM_PARENT_CANCELLED);
    return MOQ_OK;
}

static moq_result_t bind_stream(moq_fill_table_t *t, moq_fill_ref_t ref,
    moq_stream_ref_t stream, uint32_t role)
{
    if (!stream._v) return MOQ_ERR_INVAL;
    moq_fill_entry_t *e = lookup_mut(t, ref);
    if (!e) return MOQ_ERR_STALE_HANDLE;
    if (e->role != role) return MOQ_ERR_WRONG_STATE;
    if (e->stream_bound) return e->stream._v == stream._v ? MOQ_OK : MOQ_ERR_PROTO;
    if (e->delivery_known && e->delivery.outcome == MOQ_FILL_OUTCOME_NO_STREAM)
        return MOQ_ERR_PROTO;
    if (role == MOQ_FILL_PUBLISHER && next_action(e) != MOQ_FILL_ACTION_HEADER)
        return MOQ_ERR_WRONG_STATE;
    for (size_t i = 0; i < t->capacity; ++i)
        if (t->entries[i].active && t->entries[i].stream_bound &&
            t->entries[i].stream._v == stream._v) return MOQ_ERR_PROTO;
    e->stream_bound = true;
    e->stream = stream;
    return MOQ_OK;
}

moq_result_t moq_fill_bind_received(moq_fill_table_t *t,
    moq_fill_ref_t ref, moq_stream_ref_t stream)
{ return bind_stream(t, ref, stream, MOQ_FILL_SUBSCRIBER); }

moq_result_t moq_fill_header_committed(moq_fill_table_t *t,
    moq_fill_ref_t ref, moq_stream_ref_t stream)
{ return bind_stream(t, ref, stream, MOQ_FILL_PUBLISHER); }

static moq_result_t receive_terminal(moq_fill_table_t *t,
    moq_fill_ref_t ref, moq_stream_ref_t stream, bool reset)
{
    moq_fill_entry_t *e = lookup_mut(t, ref);
    if (!e) return MOQ_ERR_STALE_HANDLE;
    if (e->role != MOQ_FILL_SUBSCRIBER || !e->stream_bound ||
        e->stream._v != stream._v) return MOQ_ERR_WRONG_STATE;
    /* Repeated terminal callbacks never recount; retain the first outcome. */
    if (stream_ended(e)) return MOQ_OK;
    if (reset) e->data_reset = true;
    else e->data_fin = true;
    reason_first(e, reset ? MOQ_FILL_TERM_RESET : MOQ_FILL_TERM_COMPLETE);
    return MOQ_OK;
}

moq_result_t moq_fill_peer_fin(moq_fill_table_t *t,
    moq_fill_ref_t ref, moq_stream_ref_t stream)
{ return receive_terminal(t, ref, stream, false); }

moq_result_t moq_fill_peer_reset(moq_fill_table_t *t,
    moq_fill_ref_t ref, moq_stream_ref_t stream)
{ return receive_terminal(t, ref, stream, true); }

moq_result_t moq_fill_local_retired(moq_fill_table_t *t,
    moq_fill_ref_t ref, moq_stream_ref_t stream)
{
    moq_fill_entry_t *e = lookup_mut(t, ref);
    if (!e) return MOQ_ERR_STALE_HANDLE;
    if (e->role != MOQ_FILL_SUBSCRIBER || !e->stream_bound ||
        e->stream._v != stream._v || !e->stop_committed)
        return MOQ_ERR_WRONG_STATE;
    if (stream_ended(e)) return MOQ_OK;
    e->data_local_retired = true;
    return MOQ_OK;
}

moq_result_t moq_fill_peer_stop(moq_fill_table_t *t,
    moq_fill_ref_t ref, moq_stream_ref_t stream)
{
    moq_fill_entry_t *e = lookup_mut(t, ref);
    if (!e) return MOQ_ERR_STALE_HANDLE;
    if (e->role != MOQ_FILL_PUBLISHER || !e->stream_bound ||
        e->stream._v != stream._v) return MOQ_ERR_WRONG_STATE;
    e->peer_stop = true;
    if (!data_terminal(e)) reason_first(e, MOQ_FILL_TERM_RESET);
    return MOQ_OK;
}

moq_result_t moq_fill_action_committed(moq_fill_table_t *t,
    moq_fill_ref_t ref, uint32_t action)
{
    moq_fill_entry_t *e = lookup_mut(t, ref);
    if (!e) return MOQ_ERR_STALE_HANDLE;
    if (action < MOQ_FILL_ACTION_FIN || action > MOQ_FILL_ACTION_STOP)
        return MOQ_ERR_INVAL;
    if ((action == MOQ_FILL_ACTION_FIN && e->data_fin && e->role == MOQ_FILL_PUBLISHER) ||
        (action == MOQ_FILL_ACTION_RESET && e->data_reset && e->role == MOQ_FILL_PUBLISHER) ||
        (action == MOQ_FILL_ACTION_STOP && e->stop_committed && e->role == MOQ_FILL_SUBSCRIBER))
        return MOQ_OK;
    if (next_action(e) != action) return MOQ_ERR_WRONG_STATE;
    if (action == MOQ_FILL_ACTION_STOP) e->stop_committed = true;
    else if (action == MOQ_FILL_ACTION_FIN) {
        e->data_fin = true;
        reason_first(e, MOQ_FILL_TERM_COMPLETE);
    } else {
        e->data_reset = true;
        reason_first(e, MOQ_FILL_TERM_RESET);
    }
    return MOQ_OK;
}

moq_result_t moq_fill_next(const moq_fill_table_t *t,
    moq_fill_ref_t ref, moq_fill_work_t *out)
{
    if (!out) return MOQ_ERR_INVAL;
    const moq_fill_entry_t *e = lookup(t, ref);
    if (!e) return MOQ_ERR_STALE_HANDLE;
    *out = (moq_fill_work_t){.action = next_action(e),
        .account_stream = stream_ended(e) && !e->stream_accounted,
        .publish_terminal = terminal_ready(e), .terminal_reason = e->terminal_reason,
        .reclaimable = reclaimable(e)};
    return MOQ_OK;
}

moq_result_t moq_fill_stream_accounted(moq_fill_table_t *t,
    moq_fill_ref_t ref)
{
    moq_fill_entry_t *e = lookup_mut(t, ref);
    if (!e) return MOQ_ERR_STALE_HANDLE;
    if (!stream_ended(e) || e->stream_accounted) return MOQ_ERR_WRONG_STATE;
    e->stream_accounted = true;
    return MOQ_OK;
}

moq_result_t moq_fill_terminal_committed(moq_fill_table_t *t,
    moq_fill_ref_t ref)
{
    moq_fill_entry_t *e = lookup_mut(t, ref);
    if (!e) return MOQ_ERR_STALE_HANDLE;
    if (!terminal_ready(e)) return MOQ_ERR_WRONG_STATE;
    e->terminal_published = true;
    return MOQ_OK;
}

moq_result_t moq_fill_terminal_retired(moq_fill_table_t *t,
    moq_fill_ref_t ref)
{
    moq_fill_entry_t *e = lookup_mut(t, ref);
    if (!e) return MOQ_ERR_STALE_HANDLE;
    if (!e->terminal_published) return MOQ_ERR_WRONG_STATE;
    e->terminal_retired = true;
    return MOQ_OK;
}

moq_result_t moq_fill_reclaim(moq_fill_table_t *t, moq_fill_ref_t ref)
{
    moq_fill_entry_t *e = lookup_mut(t, ref);
    if (!e) return MOQ_ERR_STALE_HANDLE;
    if (!reclaimable(e)) return MOQ_ERR_WOULD_BLOCK;
    const uint64_t generation = e->generation;
    memset(e, 0, sizeof(*e));
    e->generation = generation;
    --t->live;
    return MOQ_OK;
}

moq_result_t moq_fill_release_unbound(moq_fill_table_t *t, moq_fill_ref_t ref)
{
    moq_fill_entry_t *e = lookup_mut(t, ref);
    if (!e) return MOQ_ERR_STALE_HANDLE;
    if (e->stream_bound || !e->terminal_published) return MOQ_ERR_WRONG_STATE;
    const uint64_t generation = e->generation;
    memset(e, 0, sizeof(*e));
    e->generation = generation;
    --t->live;
    return MOQ_OK;
}

bool moq_fill_publisher_clear(const moq_fill_table_t *t,
    moq_fill_parent_t parent)
{
    if (!table_valid(t) || !parent_valid(parent)) return false;
    for (size_t i = 0; i < t->capacity; ++i) {
        const moq_fill_entry_t *e = &t->entries[i];
        if (e->active && e->role == MOQ_FILL_PUBLISHER &&
            parent_equal(e->parent, parent) &&
            (e->control == MOQ_FILL_CONTROL_PENDING || !data_settled(e))) return false;
    }
    return true;
}
