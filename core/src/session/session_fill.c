/*
 * Fill children of a subscription.
 *
 * A fill is a fetch response stream a publisher owes a subscription because
 * the SUBSCRIBE or a REQUEST_UPDATE asked for one. The child's ownership
 * state lives in the pure table (fill_owner.c); this file binds that table
 * to the session: the pool, public handles, the effective request each child
 * carries, the Largest-snapshot resolution of its range (fill_range.c), the
 * actions and events it owes, and the publisher's write path on its stream.
 *
 * Capacity rules. Admission reserves the pool slot and, for the event the
 * admission itself surfaces, one event slot; nothing else is reserved at
 * admission. Every later effect -- a RESET or STOP the stream is owed, the
 * FILL_REQUEST, the FILL_TERMINAL, reclamation -- is an obligation the table
 * retains until fill_service finds the capacity for it, so teardown is never
 * dropped. Explicit operations reserve their complete output before
 * committing intent and refuse with MOQ_ERR_WOULD_BLOCK otherwise.
 *
 * A peer's fill request the pool cannot admit is not refused: the request
 * is processed normally and the fill is answered on the wire as a fill
 * fetch stream reset right after its FETCH_HEADER (fill_open_failed), so
 * the subscriber learns that this fill failed. Every fill stream the session
 * opens, served or failed, is counted on the parent for PUBLISH_DONE.
 */
#include "session_internal.h"
#include "fill_range_internal.h"

/* -- Pool ---------------------------------------------------------------- *
 * The configured capacity is recorded at create; the storage is allocated
 * by the first admission, so a session that never requests or receives a
 * fill performs no allocation for them. */

moq_result_t fill_pool_init(moq_session_t *s, size_t cap)
{
    s->fill_cap = cap;
    s->fill_storage = NULL;
    s->fill_side = NULL;
    if (cap > SIZE_MAX / sizeof(moq_fill_entry_t) ||
        cap > SIZE_MAX / sizeof(moq_fill_side_t))
        return MOQ_ERR_INVAL;
    return moq_fill_table_init(&s->fills, NULL, 0) == MOQ_OK ? MOQ_OK : MOQ_ERR_INTERNAL;
}

/* Allocate the pool on first use. A refusal leaves nothing allocated. */
static moq_result_t fill_pool_ensure(moq_session_t *s)
{
    if (s->fill_storage || s->fill_cap == 0) return MOQ_OK;
    size_t cap = s->fill_cap;
    size_t ebytes = cap * sizeof(moq_fill_entry_t);
    size_t sbytes = cap * sizeof(moq_fill_side_t);
    moq_fill_entry_t *entries = (moq_fill_entry_t *)s->alloc.alloc(ebytes, s->alloc.ctx);
    if (!entries) return MOQ_ERR_NOMEM;
    moq_fill_side_t *side = (moq_fill_side_t *)s->alloc.alloc(sbytes, s->alloc.ctx);
    if (!side) {
        s->alloc.free(entries, ebytes, s->alloc.ctx);
        return MOQ_ERR_NOMEM;
    }
    memset(side, 0, sbytes);
    if (moq_fill_table_init(&s->fills, entries, cap) != MOQ_OK) {
        s->alloc.free(side, sbytes, s->alloc.ctx);
        s->alloc.free(entries, ebytes, s->alloc.ctx);
        return MOQ_ERR_INTERNAL;
    }
    s->fill_storage = entries;
    s->fill_side = side;
    return MOQ_OK;
}

void fill_pool_destroy(moq_session_t *s)
{
    if (s->fill_storage)
        s->alloc.free(s->fill_storage, s->fill_cap * sizeof(moq_fill_entry_t),
                      s->alloc.ctx);
    if (s->fill_side)
        s->alloc.free(s->fill_side, s->fill_cap * sizeof(moq_fill_side_t),
                      s->alloc.ctx);
    s->fill_storage = NULL;
    s->fill_side = NULL;
    s->fill_cap = 0;
    (void)moq_fill_table_init(&s->fills, NULL, 0);
}

/* -- Handles and identities ---------------------------------------------- */

static moq_fill_ref_t ref_of(const moq_session_t *s, size_t slot)
{
    const moq_fill_entry_t *e = &s->fill_storage[slot];
    moq_fill_ref_t ref = { slot, e->generation, e->parent };
    return ref;
}

static bool parent_is(const moq_fill_entry_t *e, moq_fill_parent_t p)
{
    return e->parent.family == p.family && e->parent.slot == p.slot &&
           e->parent.generation == p.generation;
}

static moq_subscription_t parent_sub(const moq_session_t *s, moq_fill_parent_t p)
{
    moq_subscription_t h = { 0 };
    if (p.family == MOQ_FILL_PARENT_SUBSCRIBE)
        h._opaque = moq_handle_pack(MOQ_HANDLE_POOL_SUBSCRIPTION, s->session_tag,
                                    (uint32_t)p.generation, (uint32_t)p.slot);
    return h;
}

static moq_publication_t parent_pub(const moq_session_t *s, moq_fill_parent_t p)
{
    moq_publication_t h = { 0 };
    if (p.family == MOQ_FILL_PARENT_PUBLISH)
        h._opaque = moq_handle_pack(MOQ_HANDLE_POOL_PUBLICATION, s->session_tag,
                                    (uint32_t)p.generation, (uint32_t)p.slot);
    return h;
}

/* A public handle names a live child whose terminal has not been committed. */
static moq_result_t fill_resolve(moq_session_t *s, moq_fill_t h, size_t *out_slot)
{
    moq_fill_identity_t id;
    if (moq_fill_handle_decode(s->session_tag, h, &id) != MOQ_OK)
        return MOQ_ERR_STALE_HANDLE;
    if (id.slot >= s->fills.capacity) return MOQ_ERR_STALE_HANDLE;
    const moq_fill_entry_t *e = &s->fill_storage[id.slot];
    if (!e->active || e->generation != id.generation || e->terminal_published)
        return MOQ_ERR_STALE_HANDLE;
    *out_slot = id.slot;
    return MOQ_OK;
}

/* -- Effective request ------------------------------------------------- */

/* A value the request left out takes the parent's; DEFAULT order remains
 * unresolved until the authoritative control snapshot supplies track order. */
static void effective_input(const moq_fill_input_t *req, uint8_t priority,
                            uint8_t group_order, uint32_t filter, uint64_t sg,
                            uint64_t so, uint64_t eg, uint64_t eo,
                            moq_fill_input_t *out)
{
    *out = *req;
    out->requested = true;
    if (!req->has_subscriber_priority) {
        out->has_subscriber_priority = true;
        out->subscriber_priority = priority;
    }
    /* An order left out takes the subscription's: the one requested, kept
     * DEFAULT when that was left out too until the delivery resolves it
     * against the publisher's preference from the track. */
    if (!req->has_group_order) {
        out->has_group_order = true;
        out->group_order = group_order;
    }
    if (!req->has_location) {
        out->has_location = true;
        out->filter_type = filter;
        out->start_group = sg;
        out->start_object = so;
        out->end_group = eg;
        out->end_object = eo;
    }
}

static moq_result_t fill_prepare(moq_session_t *s, moq_fill_parent_t parent,
                                 uint64_t initiating_id, uint32_t role,
                                 const moq_fill_input_t *req, uint8_t priority,
                                 uint8_t group_order, uint32_t filter, uint64_t sg,
                                 uint64_t so, uint64_t eg, uint64_t eo,
                                 moq_fill_reservation_t *out, moq_fill_input_t *eff)
{
    if (!req || !req->requested) return MOQ_ERR_INVAL;
    effective_input(req, priority, group_order, filter, sg, so, eg, eo, eff);
    moq_result_t rc = fill_pool_ensure(s);
    if (rc != MOQ_OK) return rc;
    rc = moq_fill_prepare(&s->fills, parent, initiating_id, role, NULL, out);
    if (rc != MOQ_OK) return rc;
    /* The slot's public generation space is finite; a slot past it can take
     * no new child even though the table would admit one. */
    moq_fill_t probe;
    if (moq_fill_handle_encode(s->session_tag, out->ref.slot, out->ref.generation,
                               &probe) != MOQ_OK)
        return MOQ_ERR_REQUEST_BLOCKED;
    return MOQ_OK;
}

static void fill_commit(moq_session_t *s, const moq_fill_reservation_t *r,
                        const moq_fill_input_t *eff)
{
    moq_result_t rc = moq_fill_commit(&s->fills, r);
    (void)rc;   /* prepared against this table in the same transaction */
    moq_fill_side_t *side = &s->fill_side[r->ref.slot];
    memset(side, 0, sizeof(*side));
    side->input = *eff;
    (void)moq_fill_handle_encode(s->session_tag, r->ref.slot, r->ref.generation,
                                 &side->handle);
}

moq_result_t fill_local_prepare(moq_session_t *s, moq_fill_parent_t parent,
                                uint64_t initiating_id, const moq_fill_input_t *req,
                                uint8_t priority, uint8_t group_order,
                                uint32_t filter, uint64_t sg, uint64_t so,
                                uint64_t eg, uint64_t eo,
                                moq_fill_reservation_t *out, moq_fill_input_t *eff)
{
    /* A local request the pool cannot take is a retryable refusal: nothing
     * was sent, and a slot frees when a live child ends. */
    moq_result_t rc = fill_prepare(s, parent, initiating_id, MOQ_FILL_SUBSCRIBER, req,
                                   priority, group_order, filter, sg, so, eg, eo, out, eff);
    return rc == MOQ_ERR_REQUEST_BLOCKED ? MOQ_ERR_WOULD_BLOCK : rc;
}

moq_result_t fill_inbound_prepare(moq_session_t *s, moq_fill_parent_t parent,
                                  uint64_t initiating_id, const moq_fill_input_t *req,
                                  uint8_t priority, uint8_t group_order,
                                  uint32_t filter, uint64_t sg, uint64_t so,
                                  uint64_t eg, uint64_t eo,
                                  moq_fill_reservation_t *out, moq_fill_input_t *eff)
{
    return fill_prepare(s, parent, initiating_id, MOQ_FILL_PUBLISHER, req, priority,
                        group_order, filter, sg, so, eg, eo, out, eff);
}

static void event_identity(const moq_session_t *s, size_t slot, moq_fill_t *fill,
                           moq_subscription_t *sub, moq_publication_t *pub,
                           uint64_t *initiating_id)
{
    const moq_fill_entry_t *e = &s->fill_storage[slot];
    *fill = s->fill_side[slot].handle;
    *sub = parent_sub(s, e->parent);
    *pub = parent_pub(s, e->parent);
    *initiating_id = e->initiating_id;
}

void fill_local_commit(moq_session_t *s, const moq_fill_reservation_t *r,
                       const moq_fill_input_t *eff)
{
    fill_commit(s, r, eff);
    moq_event_t ev;
    memset(&ev, 0, sizeof(ev));
    ev.kind = MOQ_EVENT_FILL_CREATED;
    ev.detail_size = (uint32_t)sizeof(moq_fill_created_event_t);
    ev.borrow_epoch = s->borrow_epoch;
    event_identity(s, r->ref.slot, &ev.u.fill_created.fill, &ev.u.fill_created.sub,
                   &ev.u.fill_created.pub, &ev.u.fill_created.initiating_request_id);
    (void)push_event(s, &ev);   /* the caller checked event capacity */
}

void fill_inbound_commit(moq_session_t *s, const moq_fill_reservation_t *r,
                         const moq_fill_input_t *eff)
{
    fill_commit(s, r, eff);
}

/* -- Control resolution -------------------------------------------------- */

/* A fill's group order: its own, else its parent subscription's effective
 * order (session_group_order). */
static uint8_t parent_group_order(const moq_session_t *s, moq_fill_parent_t p,
                                  uint8_t order)
{
    if (order == MOQ_GROUP_ORDER_ASCENDING || order == MOQ_GROUP_ORDER_DESCENDING)
        return order;
    if (p.family == MOQ_FILL_PARENT_SUBSCRIBE && p.slot < s->sub_cap &&
        s->subs[p.slot].generation == p.generation)
        return session_group_order(s->subs[p.slot].group_order,
                                   s->subs[p.slot].track_group_order);
    if (p.family == MOQ_FILL_PARENT_PUBLISH && p.slot < s->pub_cap &&
        s->publishes[p.slot].generation == p.generation)
        return session_group_order(s->publishes[p.slot].group_order,
                                   s->publishes[p.slot].track_group_order);
    return MOQ_GROUP_ORDER_DEFAULT;
}

static void resolve_delivery(moq_session_t *s, size_t slot, bool has_largest,
                             uint64_t largest_group, uint64_t largest_object,
                             bool paused)
{
    moq_fill_entry_t *e = &s->fill_storage[slot];
    const moq_fill_input_t *in = &s->fill_side[slot].input;
    moq_fill_range_t r;
    moq_result_t rc = moq_fill_range_resolve(in->filter_type, in->start_group,
        in->start_object, in->end_group, in->end_object, has_largest,
        largest_group, largest_object, paused, &r);
    moq_fill_delivery_t d;
    memset(&d, 0, sizeof(d));
    d.outcome = (rc == MOQ_OK && r.outcome == MOQ_FILL_RANGE_STREAM)
        ? MOQ_FILL_OUTCOME_STREAM : MOQ_FILL_OUTCOME_NO_STREAM;
    d.group_order = parent_group_order(s, e->parent, (uint8_t)in->group_order);
    d.start_group = r.start_group;
    d.start_object = r.start_object;
    d.end_group = r.end_group;
    d.end_object = r.end_object;
    d.processed_at_us = s->last_now_us;
    d.subscriber_priority = in->subscriber_priority;
    d.has_fill_timeout = in->has_fill_timeout;
    d.fill_timeout_ms = in->fill_timeout_ms;
    /* A stream that already arrived outranks a snapshot that predicts none;
     * a delivery resolved earlier is kept. Both refusals are deliberate. */
    (void)moq_fill_resolve_delivery(&s->fills, ref_of(s, slot), e->initiating_id, &d);
}

void fill_control_resolved(moq_session_t *s, moq_fill_parent_t parent,
                           uint64_t initiating_id, bool accepted, uint64_t error_code,
                           bool has_largest, uint64_t largest_group,
                           uint64_t largest_object, bool paused)
{
    fill_control_resolved_receipt(s, parent, initiating_id, accepted, error_code,
        has_largest, largest_group, largest_object, paused, NULL);
}

void fill_control_resolved_receipt(moq_session_t *s, moq_fill_parent_t parent,
    uint64_t initiating_id, bool accepted, uint64_t error_code,
    bool has_largest, uint64_t largest_group, uint64_t largest_object, bool paused,
    moq_pub_fill_receipt_t *receipt)
{
    if (s->fills.live == 0) return;
    for (size_t i = 0; i < s->fills.capacity; i++) {
        moq_fill_entry_t *e = &s->fill_storage[i];
        if (!e->active || !parent_is(e, parent) || e->initiating_id != initiating_id)
            continue;
        if (e->control == MOQ_FILL_CONTROL_PENDING) {
            if (!accepted && s->fill_side[i].error_domain == MOQ_FILL_ERROR_NONE) {
                s->fill_side[i].error_domain = MOQ_FILL_ERROR_PARENT_REQUEST;
                s->fill_side[i].error_code = error_code;
            }
            (void)moq_fill_control_response(&s->fills, ref_of(s, i), initiating_id,
                                            accepted);
        }
        if (accepted) {
            resolve_delivery(s, i, has_largest, largest_group, largest_object, paused);
            /* Copy the committed delivery before fill_service can reclaim a
             * NO_STREAM child. No second range resolution or event polling. */
            if (receipt && e->role == MOQ_FILL_PUBLISHER && e->delivery_known) {
                receipt->requested = true;
                receipt->outcome = e->delivery.outcome == MOQ_FILL_OUTCOME_STREAM ? 2 : 1;
                moq_fill_request_event_t *r = &receipt->request;
                event_identity(s, i, &r->fill, &r->sub, &r->pub, &r->initiating_request_id);
                r->start_group = e->delivery.start_group;
                r->start_object = e->delivery.start_object;
                r->end_group = e->delivery.end_group;
                r->end_object = e->delivery.end_object;
                r->processed_at_us = e->delivery.processed_at_us;
                r->fill_timeout_ms = e->delivery.fill_timeout_ms;
                r->group_order = e->delivery.group_order;
                r->subscriber_priority = e->delivery.subscriber_priority;
                r->has_fill_timeout = e->delivery.has_fill_timeout;
            }
        }
    }
    /* Copy order once, while the authoritative control's parent and child
     * generations still match. Object decoding never consults these slots. */
    if (accepted) {
        for (size_t i = 0; i < s->rx_cap; i++) {
            moq_rx_stream_t *rx = &s->rx_streams[i];
            if (!rx->active || !rx->fill_bound || rx->fetch_order_known ||
                rx->fill_ref.slot >= s->fills.capacity)
                continue;
            const moq_fill_entry_t *e = &s->fill_storage[rx->fill_ref.slot];
            if (!e->active || e->generation != rx->fill_ref.generation ||
                !parent_is(e, parent) || e->initiating_id != initiating_id)
                continue;
            const moq_fill_input_t *in = &s->fill_side[rx->fill_ref.slot].input;
            uint8_t order = e->delivery_known ? e->delivery.group_order
                : parent_group_order(s, parent, (uint8_t)in->group_order);
            if (order != MOQ_GROUP_ORDER_ASCENDING &&
                order != MOQ_GROUP_ORDER_DESCENDING)
                continue;
            rx->effective_fetch_group_order = (moq_group_order_t)order;
            rx->fetch_order_known = true;
            rx->resumed_deferred = true;
        }
        session_retry_resumed_deferred(s);
    }
    fill_service(s);
}

/* -- Parent teardown ----------------------------------------------------- */

static bool stream_open(const moq_fill_entry_t *e)
{
    return e->stream_bound && !e->data_fin && !e->data_reset;
}

size_t fill_parent_actions_needed(const moq_session_t *s, moq_fill_parent_t parent)
{
    size_t n = 0;
    if (s->fills.live == 0) return 0;
    for (size_t i = 0; i < s->fills.capacity; i++) {
        const moq_fill_entry_t *e = &s->fill_storage[i];
        if (!e->active || !parent_is(e, parent) || !stream_open(e)) continue;
        if (e->role == MOQ_FILL_PUBLISHER) n++;
        else if (!e->stop_committed) n++;
    }
    return n;
}

static void push_stream_action(moq_session_t *s, size_t slot, uint32_t action)
{
    moq_fill_entry_t *e = &s->fill_storage[slot];
    moq_fill_side_t *side = &s->fill_side[slot];
    moq_action_t a;
    memset(&a, 0, sizeof(a));
    a.borrow_epoch = s->borrow_epoch;
    uint64_t code = side->error_domain == MOQ_FILL_ERROR_STREAM ? side->error_code : 0x1;
    if (action == MOQ_FILL_ACTION_RESET) {
        a.kind = MOQ_ACTION_RESET_DATA;
        a.detail_size = (uint32_t)sizeof(moq_reset_data_action_t);
        a.u.reset_data.stream_ref = e->stream;
        a.u.reset_data.error_code = code;
        a.u.reset_data.reliable_size = side->header_len;
    } else {
        a.kind = MOQ_ACTION_STOP_DATA;
        a.detail_size = (uint32_t)sizeof(moq_stop_data_action_t);
        a.u.stop_data.stream_ref = e->stream;
        a.u.stop_data.error_code = code;
    }
    (void)push_action(s, &a);   /* the caller checked action capacity */
    (void)moq_fill_action_committed(&s->fills, ref_of(s, slot), action);
    if (action == MOQ_FILL_ACTION_STOP) rx_fill_stopped(s, e->stream);
}

void fill_parent_cancel(moq_session_t *s, moq_fill_parent_t parent,
                        uint32_t error_domain, uint64_t error_code)
{
    if (s->fills.live == 0) return;
    fill_parent_cancel_keep(s, parent, error_domain, error_code, 0);
    fill_service(s);
}

void fill_parent_cancel_keep(moq_session_t *s, moq_fill_parent_t parent,
                             uint32_t error_domain, uint64_t error_code,
                             size_t keep)
{
    if (s->fills.live == 0) return;
    for (size_t i = 0; i < s->fills.capacity; i++) {
        moq_fill_entry_t *e = &s->fill_storage[i];
        if (!e->active || !parent_is(e, parent)) continue;
        /* The first cause wins, domain included: a child whose terminal
         * reason is already decided (its stream ended, it was cancelled or
         * aborted) keeps that reason and its own error cause. */
        if (e->terminal_reason == MOQ_FILL_TERM_NONE &&
            s->fill_side[i].error_domain == MOQ_FILL_ERROR_NONE &&
            error_domain != MOQ_FILL_ERROR_NONE) {
            s->fill_side[i].error_domain = error_domain;
            s->fill_side[i].error_code = error_code;
        }
        (void)moq_fill_parent_cancel(&s->fills, ref_of(s, i));
    }
    /* This parent's streams first, with the capacity its caller reserved;
     * the general service then handles terminals and anything else owed. */
    for (size_t i = 0; i < s->fills.capacity; i++) {
        moq_fill_entry_t *e = &s->fill_storage[i];
        if (!e->active || !parent_is(e, parent)) continue;
        moq_fill_work_t w;
        if (moq_fill_next(&s->fills, ref_of(s, i), &w) != MOQ_OK) continue;
        if ((w.action == MOQ_FILL_ACTION_RESET || w.action == MOQ_FILL_ACTION_STOP) &&
            action_queue_avail(s) > keep)
            push_stream_action(s, i, w.action);
    }
}

/* -- The service: every retained obligation whose capacity is now there -- */

static uint32_t public_reason(uint32_t term)
{
    switch (term) {
    case MOQ_FILL_TERM_COMPLETE:         return MOQ_FILL_COMPLETE;
    case MOQ_FILL_TERM_NO_STREAM:        return MOQ_FILL_NO_STREAM;
    case MOQ_FILL_TERM_CANCELLED:        return MOQ_FILL_LOCAL_CANCELLED;
    case MOQ_FILL_TERM_ABORTED:          return MOQ_FILL_LOCAL_ABORTED;
    case MOQ_FILL_TERM_RESET:            return MOQ_FILL_PEER_CANCELLED_OR_RESET;
    case MOQ_FILL_TERM_CONTROL_REJECTED: return MOQ_FILL_PARENT_REJECTED;
    default:                             return MOQ_FILL_PARENT_TERMINATED;
    }
}

static bool request_owed(const moq_session_t *s, size_t slot)
{
    const moq_fill_entry_t *e = &s->fill_storage[slot];
    return e->role == MOQ_FILL_PUBLISHER && !s->fill_side[slot].request_published &&
           e->control == MOQ_FILL_CONTROL_ACCEPTED && e->delivery_known &&
           e->delivery.outcome == MOQ_FILL_OUTCOME_STREAM && !e->parent_cancelled &&
           e->terminal_reason == MOQ_FILL_TERM_NONE;
}

static void publish_request(moq_session_t *s, size_t slot)
{
    const moq_fill_entry_t *e = &s->fill_storage[slot];
    moq_event_t ev;
    memset(&ev, 0, sizeof(ev));
    ev.kind = MOQ_EVENT_FILL_REQUEST;
    ev.detail_size = (uint32_t)sizeof(moq_fill_request_event_t);
    ev.borrow_epoch = s->borrow_epoch;
    moq_fill_request_event_t *r = &ev.u.fill_request;
    event_identity(s, slot, &r->fill, &r->sub, &r->pub, &r->initiating_request_id);
    r->start_group = e->delivery.start_group;
    r->start_object = e->delivery.start_object;
    r->end_group = e->delivery.end_group;
    r->end_object = e->delivery.end_object;
    r->processed_at_us = e->delivery.processed_at_us;
    r->fill_timeout_ms = e->delivery.fill_timeout_ms;
    r->group_order = e->delivery.group_order;
    r->subscriber_priority = e->delivery.subscriber_priority;
    r->has_fill_timeout = e->delivery.has_fill_timeout;
    if (push_event(s, &ev) == MOQ_OK) s->fill_side[slot].request_published = true;
}

static void publish_terminal(moq_session_t *s, size_t slot, uint32_t term)
{
    const moq_fill_side_t *side = &s->fill_side[slot];
    moq_event_t ev;
    memset(&ev, 0, sizeof(ev));
    ev.kind = MOQ_EVENT_FILL_TERMINAL;
    ev.detail_size = (uint32_t)sizeof(moq_fill_terminal_event_t);
    ev.borrow_epoch = s->borrow_epoch;
    moq_fill_terminal_event_t *t = &ev.u.fill_terminal;
    event_identity(s, slot, &t->fill, &t->sub, &t->pub, &t->initiating_request_id);
    t->reason = public_reason(term);
    t->error_domain = side->error_domain;
    t->error_code = side->error_domain == MOQ_FILL_ERROR_NONE ? 0 : side->error_code;
    if (push_event(s, &ev) != MOQ_OK) return;
    moq_fill_ref_t ref = ref_of(s, slot);
    (void)moq_fill_terminal_committed(&s->fills, ref);
    (void)moq_fill_terminal_retired(&s->fills, ref);
}

void fill_service(moq_session_t *s)
{
    if (s->fills.live == 0) return;
    for (size_t i = 0; i < s->fills.capacity; i++) {
        moq_fill_entry_t *e = &s->fill_storage[i];
        if (!e->active) continue;
        if (request_owed(s, i)) {
            if (event_queue_full(s)) continue;
            publish_request(s, i);
        }
        moq_fill_ref_t ref = ref_of(s, i);
        moq_fill_work_t w;
        if (moq_fill_next(&s->fills, ref, &w) != MOQ_OK) continue;
        if ((w.action == MOQ_FILL_ACTION_RESET || w.action == MOQ_FILL_ACTION_STOP) &&
            !action_queue_full(s)) {
            push_stream_action(s, i, w.action);
            if (moq_fill_next(&s->fills, ref, &w) != MOQ_OK) continue;
        }
        /* The subscriber's stream counter advanced at the stream's own
         * terminal through its parent binding; here the table just learns the
         * effect is settled. */
        if (w.account_stream) {
            (void)moq_fill_stream_accounted(&s->fills, ref);
            if (moq_fill_next(&s->fills, ref, &w) != MOQ_OK) continue;
        }
        if (w.publish_terminal) {
            if (event_queue_full(s)) continue;
            publish_terminal(s, i, w.terminal_reason);
            if (moq_fill_next(&s->fills, ref, &w) != MOQ_OK) continue;
        }
        bool receiver_unbound = e->role == MOQ_FILL_SUBSCRIBER && !e->stream_bound;
        uint64_t initiating_id = e->initiating_id;
        if (w.reclaimable) {
            /* A receiving child reclaimed without a stream concluded that
             * none would open. A peer that opens one anyway must find a
             * tombstone, so the header is stopped instead of closing the
             * session as an unknown request. */
            if (receiver_unbound) fetch_cancel_tomb_add(s, initiating_id);
            if (moq_fill_reclaim(&s->fills, ref) == MOQ_OK)
                memset(&s->fill_side[i], 0, sizeof(s->fill_side[i]));
            continue;
        }
        /* A receiving child that ended before any stream bound keeps its
         * slot while its parent lives: a late header must bind here so the
         * stream is stopped and settles the child. The slot is released once
         * no stream can be bound any more -- the request was refused, so no
         * stream will ever open, or the parent is gone -- with a tombstone
         * standing in for a header the peer may still send. */
        if (receiver_unbound && e->terminal_published && e->terminal_retired) {
            if (e->control == MOQ_FILL_CONTROL_REJECTED) {
                if (moq_fill_release_unbound(&s->fills, ref) == MOQ_OK)
                    memset(&s->fill_side[i], 0, sizeof(s->fill_side[i]));
            } else if (e->parent_cancelled) {
                fetch_cancel_tomb_add(s, initiating_id);
                if (moq_fill_release_unbound(&s->fills, ref) == MOQ_OK)
                    memset(&s->fill_side[i], 0, sizeof(s->fill_side[i]));
            }
        }
    }
}

/* -- Receive-side bindings (subscriber role) ------------------------------ */

moq_result_t fill_rx_bind(moq_session_t *s, moq_rx_stream_t *rx, uint64_t initiating_id)
{
    moq_fill_ref_t ref;
    if (s->fills.live == 0 || moq_fill_find(&s->fills, initiating_id, &ref) != MOQ_OK)
        return MOQ_ERR_STALE_HANDLE;
    const moq_fill_entry_t *e = &s->fill_storage[ref.slot];
    if (e->role != MOQ_FILL_SUBSCRIBER) return MOQ_ERR_STALE_HANDLE;
    moq_result_t rc = moq_fill_bind_received(&s->fills, ref, rx->stream_ref);
    if (rc != MOQ_OK) return MOQ_ERR_PROTO;
    rx->fill_bound = true;
    rx->fill_ref = ref;
    rx->sub = parent_sub(s, e->parent);
    rx->pub_handle = parent_pub(s, e->parent);
    uint32_t order = e->delivery_known ? e->delivery.group_order
        : s->fill_side[ref.slot].input.group_order;
    if (order == MOQ_GROUP_ORDER_ASCENDING || order == MOQ_GROUP_ORDER_DESCENDING) {
        rx->effective_fetch_group_order = (moq_group_order_t)order;
        rx->fetch_order_known = true;
    }
    return MOQ_OK;
}

static const moq_fill_entry_t *rx_entry(const moq_session_t *s, const moq_rx_stream_t *rx)
{
    if (!rx->fill_bound || rx->fill_ref.slot >= s->fills.capacity) return NULL;
    const moq_fill_entry_t *e = &s->fill_storage[rx->fill_ref.slot];
    if (!e->active || e->generation != rx->fill_ref.generation) return NULL;
    return e;
}

void fill_rx_peer_fin(moq_session_t *s, const moq_rx_stream_t *rx)
{
    if (!rx_entry(s, rx)) return;
    (void)moq_fill_peer_fin(&s->fills, rx->fill_ref, rx->stream_ref);
}

void fill_rx_peer_reset(moq_session_t *s, const moq_rx_stream_t *rx, uint64_t error_code)
{
    const moq_fill_entry_t *e = rx_entry(s, rx);
    if (!e) return;
    moq_fill_side_t *side = &s->fill_side[rx->fill_ref.slot];
    if (moq_fill_peer_reset(&s->fills, rx->fill_ref, rx->stream_ref) == MOQ_OK &&
        side->error_domain == MOQ_FILL_ERROR_NONE &&
        e->terminal_reason == MOQ_FILL_TERM_RESET) {
        side->error_domain = MOQ_FILL_ERROR_STREAM;
        side->error_code = error_code;
    }
}

void fill_rx_local_retired(moq_session_t *s, const moq_rx_stream_t *rx)
{
    if (!rx_entry(s, rx)) return;
    (void)moq_fill_local_retired(&s->fills, rx->fill_ref, rx->stream_ref);
}

void fill_rx_local_stop(moq_session_t *s, const moq_rx_stream_t *rx)
{
    if (!rx_entry(s, rx)) return;
    (void)moq_fill_cancel(&s->fills, rx->fill_ref);
    (void)moq_fill_action_committed(&s->fills, rx->fill_ref, MOQ_FILL_ACTION_STOP);
}

void fill_rx_identity(const moq_session_t *s, const moq_rx_stream_t *rx,
                      moq_fill_t *fill, moq_subscription_t *sub,
                      moq_publication_t *pub, uint64_t *initiating_id)
{
    const moq_fill_entry_t *e = rx_entry(s, rx);
    if (!e) {
        memset(fill, 0, sizeof(*fill));
        memset(sub, 0, sizeof(*sub));
        memset(pub, 0, sizeof(*pub));
        *initiating_id = 0;
        return;
    }
    event_identity(s, rx->fill_ref.slot, fill, sub, pub, initiating_id);
}

bool fill_rx_delivering(const moq_session_t *s, const moq_rx_stream_t *rx)
{
    const moq_fill_entry_t *e = rx_entry(s, rx);
    return e && !e->terminal_published && !e->cancel_requested && !e->parent_cancelled;
}

/* -- Publisher side: the peer stops a fill stream ------------------------- */

moq_result_t fill_on_data_stop(moq_session_t *s, moq_stream_ref_t ref, uint64_t error_code)
{
    if (s->fills.live == 0) return MOQ_ERR_STALE_HANDLE;
    for (size_t i = 0; i < s->fills.capacity; i++) {
        moq_fill_entry_t *e = &s->fill_storage[i];
        if (!e->active || e->role != MOQ_FILL_PUBLISHER || !e->stream_bound ||
            e->stream._v != ref._v)
            continue;
        if (!stream_open(e)) return MOQ_OK;
        if (action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
        moq_fill_side_t *side = &s->fill_side[i];
        if (side->error_domain == MOQ_FILL_ERROR_NONE) {
            side->error_domain = MOQ_FILL_ERROR_STREAM;
            side->error_code = error_code;
        }
        (void)moq_fill_peer_stop(&s->fills, ref_of(s, i), ref);
        moq_fill_work_t w;
        if (moq_fill_next(&s->fills, ref_of(s, i), &w) == MOQ_OK &&
            w.action == MOQ_FILL_ACTION_RESET)
            push_stream_action(s, i, MOQ_FILL_ACTION_RESET);
        fill_service(s);
        return MOQ_OK;
    }
    return MOQ_ERR_STALE_HANDLE;
}

/* -- Public operations ---------------------------------------------------- */

static moq_result_t publisher_entry(moq_session_t *s, moq_fill_t fill, size_t *slot)
{
    moq_result_t rc = fill_resolve(s, fill, slot);
    if (rc != MOQ_OK) return rc;
    if (s->fill_storage[*slot].role != MOQ_FILL_PUBLISHER) return MOQ_ERR_WRONG_STATE;
    return MOQ_OK;
}

/* The parent's count of fill streams the session opened, or NULL when the
 * parent entry named by the child is no longer that generation. */
static uint64_t *parent_streams_opened(moq_session_t *s, moq_fill_parent_t p)
{
    if (p.family == MOQ_FILL_PARENT_SUBSCRIBE) {
        if (p.slot >= s->sub_cap || s->subs[p.slot].generation != p.generation) return NULL;
        return &s->subs[p.slot].fill_streams_opened;
    }
    if (p.family == MOQ_FILL_PARENT_PUBLISH) {
        if (p.slot >= s->pub_cap || s->publishes[p.slot].generation != p.generation)
            return NULL;
        return &s->publishes[p.slot].fill_streams_opened;
    }
    return NULL;
}

static void count_stream_opened(uint64_t *streams_opened)
{
    if (streams_opened && *streams_opened != UINT64_MAX) (*streams_opened)++;
}

void fill_stream_meta(const moq_session_t *s, const moq_fill_entry_t *f,
                      moq_action_meta_t *out)
{
    memset(out, 0, sizeof(*out));
    out->deadline_us = UINT64_MAX;
    out->mode = MOQ_SCHED_MODE_FILL;
    out->subscriber_priority = f->delivery.subscriber_priority;
    out->publisher_priority = 128;
    out->fill_group_order = (uint8_t)f->delivery.group_order;
    out->group_order = MOQ_GROUP_ORDER_ASCENDING;
    if (f->parent.family == MOQ_FILL_PARENT_SUBSCRIBE && f->parent.slot < s->sub_cap) {
        const moq_sub_entry_t *se = &s->subs[f->parent.slot];
        out->owner = se->handle._opaque;
        out->group_order = session_group_order(se->group_order, se->track_group_order);
    } else if (f->parent.family == MOQ_FILL_PARENT_PUBLISH && f->parent.slot < s->pub_cap) {
        const moq_pub_entry_t *pe = &s->publishes[f->parent.slot];
        out->owner = pe->handle._opaque;
        out->group_order = session_group_order(pe->group_order, pe->track_group_order);
    }
}

/* Queue the FETCH_HEADER that opens a fill fetch stream for `initiating_id`
 * on a fresh unidirectional stream; the action is reserved by the caller.
 * Returns the new stream's reference through *out_ref and the header's
 * encoded length through *out_len. */
static moq_result_t push_fill_header(moq_session_t *s, uint64_t initiating_id,
                                     uint8_t subscriber_priority,
                                     const moq_action_meta_t *meta,
                                     moq_stream_ref_t *out_ref, uint8_t *out_len)
{
    uint8_t hdr[32];
    moq_buf_writer_t hw;
    moq_buf_writer_init(&hw, hdr, sizeof(hdr));
    moq_result_t rc = s->profile->encode_fetch_header(s, &hw, initiating_id);
    if (rc < 0) return rc;
    moq_action_t a;
    memset(&a, 0, sizeof(a));
    a.kind = MOQ_ACTION_SEND_DATA;
    a.detail_size = (uint32_t)sizeof(moq_send_data_action_t);
    a.borrow_epoch = s->borrow_epoch;
    memcpy(a.u.send_data.header, hdr, moq_buf_writer_offset(&hw));
    a.u.send_data.header_len = (uint8_t)moq_buf_writer_offset(&hw);
    a.u.send_data.stream_ref = moq_stream_ref_from_u64(s->next_stream_ref);
    a.u.send_data.payload = NULL;
    a.u.send_data.fin = false;
    a.u.send_data.scheduling_priority = UINT64_C(0x10000) |
        ((uint64_t)subscriber_priority << 8) | 128;
    rc = push_action_ex(s, &a, meta, UINT64_MAX);
    if (rc < 0) return rc;
    s->next_stream_ref++;
    *out_ref = a.u.send_data.stream_ref;
    *out_len = a.u.send_data.header_len;
    return MOQ_OK;
}

/* The FETCH_HEADER on a fresh unidirectional stream; the action is reserved.
 * The stream joins the parent's count of fill streams opened. */
static moq_result_t open_stream(moq_session_t *s, size_t slot)
{
    moq_fill_entry_t *e = &s->fill_storage[slot];
    moq_stream_ref_t ref;
    /* The header is queued before the stream is bound to the child, so it
     * carries its record explicitly, scheduled where the fill's first object
     * (the start of its range) would be. */
    moq_action_meta_t meta;
    fill_stream_meta(s, e, &meta);
    meta.group_id = e->delivery.start_group;
    meta.object_id = e->delivery.start_object;
    meta.has_location = true;
    uint8_t hdr_len = 0;
    moq_result_t rc = push_fill_header(s, e->initiating_id,
                                       e->delivery.subscriber_priority, &meta, &ref,
                                       &hdr_len);
    if (rc < 0) return rc;
    (void)moq_fill_header_committed(&s->fills, ref_of(s, slot), ref);
    count_stream_opened(parent_streams_opened(s, e->parent));
    memset(&s->fill_side[slot].prior, 0, sizeof(s->fill_side[slot].prior));
    s->fill_side[slot].header_len = hdr_len;
    return MOQ_OK;
}

bool fill_unserved_opens_stream(const moq_fill_input_t *effective, bool has_largest,
                                uint64_t largest_group, uint64_t largest_object,
                                bool paused)
{
    moq_fill_range_t r;
    moq_result_t rc = moq_fill_range_resolve(effective->filter_type, effective->start_group,
        effective->start_object, effective->end_group, effective->end_object,
        has_largest, largest_group, largest_object, paused, &r);
    return rc == MOQ_OK && r.outcome == MOQ_FILL_RANGE_STREAM;
}

void fill_open_failed(moq_session_t *s, uint64_t initiating_id,
                      uint8_t subscriber_priority, uint64_t *streams_opened)
{
    moq_stream_ref_t ref;
    uint8_t hdr_len = 0;
    if (push_fill_header(s, initiating_id, subscriber_priority, NULL, &ref, &hdr_len) < 0)
        return;
    moq_action_t a;
    memset(&a, 0, sizeof(a));
    a.kind = MOQ_ACTION_RESET_DATA;
    a.detail_size = (uint32_t)sizeof(moq_reset_data_action_t);
    a.borrow_epoch = s->borrow_epoch;
    a.u.reset_data.stream_ref = ref;
    a.u.reset_data.error_code = 0;
    a.u.reset_data.reliable_size = hdr_len;
    (void)push_action(s, &a);   /* both actions reserved by the caller */
    count_stream_opened(streams_opened);
}

uint64_t fill_stream_count(const moq_session_t *s, uint64_t app_count,
                           uint64_t fill_streams_opened)
{
    uint64_t unknown = s->profile->stream_count_unknown;
    if (app_count >= unknown || fill_streams_opened == 0) return app_count;
    if (fill_streams_opened >= unknown - app_count) return unknown;
    return app_count + fill_streams_opened;
}

uint64_t session_done_stream_count(const moq_session_t *s,
                                   uint64_t subgroup_streams_opened,
                                   uint64_t fill_streams_opened)
{
    uint64_t unknown = s->profile->stream_count_unknown;
    if (subgroup_streams_opened > unknown) subgroup_streams_opened = unknown;
    return fill_stream_count(s, subgroup_streams_opened, fill_streams_opened);
}

moq_result_t moq_session_open_fill(moq_session_t *s, moq_fill_t fill, uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;
    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;
    size_t slot;
    moq_result_t rc = publisher_entry(s, fill, &slot);
    if (rc != MOQ_OK) return rc;
    moq_fill_work_t w;
    if (moq_fill_next(&s->fills, ref_of(s, slot), &w) != MOQ_OK) return MOQ_ERR_STALE_HANDLE;
    if (w.action != MOQ_FILL_ACTION_HEADER) return MOQ_ERR_WRONG_STATE;
    if (action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    rc = open_stream(s, slot);
    if (rc < 0) return rc;
    fill_service(s);
    return MOQ_OK;
}

/* A bound stream that still accepts the publisher's writes. */
static moq_result_t writable_entry(moq_session_t *s, moq_fill_t fill, size_t *slot)
{
    moq_result_t rc = publisher_entry(s, fill, slot);
    if (rc != MOQ_OK) return rc;
    const moq_fill_entry_t *e = &s->fill_storage[*slot];
    if (!stream_open(e) || e->finish_requested || e->abort_requested ||
        e->parent_cancelled || e->peer_stop)
        return MOQ_ERR_WRONG_STATE;
    return MOQ_OK;
}

moq_result_t moq_session_write_fill_object(moq_session_t *s, moq_fill_t fill,
                                           const moq_fetch_object_cfg_t *cfg,
                                           uint64_t now_us)
{
    if (!s || !cfg) return MOQ_ERR_INVAL;
    if (cfg->struct_size < sizeof(moq_fetch_object_cfg_t)) return MOQ_ERR_INVAL;
    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;
    size_t slot;
    moq_result_t rc = writable_entry(s, fill, &slot);
    if (rc != MOQ_OK) return rc;
    moq_fill_entry_t *e = &s->fill_storage[slot];
    moq_fill_side_t *side = &s->fill_side[slot];

    size_t payload_len = cfg->payload ? moq_rcbuf_len(cfg->payload) : 0;
    if (!session_payload_size_fits_profile(s, payload_len)) return MOQ_ERR_INVAL;
    size_t props_len = cfg->properties ? moq_rcbuf_len(cfg->properties) : 0;
    if (props_len > 0 && s->profile->validate_object_properties &&
        s->profile->validate_object_properties(
            s, moq_rcbuf_data(cfg->properties), props_len) < 0)
        return MOQ_ERR_INVAL;
    bool has_props = props_len > 0;
    if (action_queue_avail(s) < (has_props ? 2u : 1u)) return MOQ_ERR_WOULD_BLOCK;

    uint64_t sched = UINT64_C(0x10000) |
        ((uint64_t)e->delivery.subscriber_priority << 8) | cfg->publisher_priority;
    moq_action_meta_t meta;
    fill_stream_meta(s, e, &meta);
    meta.publisher_priority = cfg->publisher_priority;
    meta.group_id = cfg->group_id;
    meta.object_id = cfg->object_id;
    meta.has_location = true;
    moq_action_t a1, a2;
    memset(&a1, 0, sizeof(a1));
    a1.kind = MOQ_ACTION_SEND_DATA;
    a1.detail_size = (uint32_t)sizeof(moq_send_data_action_t);
    a1.borrow_epoch = s->borrow_epoch;
    a1.u.send_data.stream_ref = e->stream;
    a1.u.send_data.scheduling_priority = sched;
    moq_fetch_object_encode_args_t args = {
        .group_id = cfg->group_id,
        .subgroup_id = cfg->subgroup_id,
        .object_id = cfg->object_id,
        .publisher_priority = cfg->publisher_priority,
        .datagram = cfg->datagram,
        .properties_len = props_len,
        .payload_len = payload_len,
        .header_only = has_props,
    };
    moq_buf_writer_t hw;
    moq_buf_writer_init(&hw, a1.u.send_data.header, sizeof(a1.u.send_data.header));
    rc = s->profile->encode_fetch_object(s, &hw,
        (moq_group_order_t)e->delivery.group_order, &args, &side->prior);
    if (rc < 0) return rc;
    a1.u.send_data.header_len = (uint8_t)moq_buf_writer_offset(&hw);
    if (has_props) {
        /* Two actions: the header with the properties block as its payload,
         * then the payload-length prefix with the object payload. */
        a1.u.send_data.payload = cfg->properties;
        memset(&a2, 0, sizeof(a2));
        a2.kind = MOQ_ACTION_SEND_DATA;
        a2.detail_size = (uint32_t)sizeof(moq_send_data_action_t);
        a2.borrow_epoch = s->borrow_epoch;
        a2.u.send_data.stream_ref = e->stream;
        a2.u.send_data.scheduling_priority = sched;
        moq_buf_writer_t hw2;
        moq_buf_writer_init(&hw2, a2.u.send_data.header, sizeof(a2.u.send_data.header));
        rc = s->profile->encode_object_payload_prefix(s, &hw2, payload_len, false);
        if (rc < 0) return rc;
        a2.u.send_data.header_len = (uint8_t)moq_buf_writer_offset(&hw2);
        a2.u.send_data.payload = cfg->payload;
        moq_rcbuf_incref(cfg->properties);
        if (cfg->payload) moq_rcbuf_incref(cfg->payload);
        (void)push_action_ex(s, &a1, &meta, UINT64_MAX);   /* both slots reserved above */
        (void)push_action_ex(s, &a2, &meta, UINT64_MAX);
    } else {
        a1.u.send_data.payload = cfg->payload;
        if (cfg->payload) moq_rcbuf_incref(cfg->payload);
        (void)push_action_ex(s, &a1, &meta, UINT64_MAX);
    }
    side->prior.has_prev = true;
    side->prior.has_actual = true;
    side->prior.group_id = cfg->group_id;
    side->prior.subgroup_id = cfg->datagram ? 0 : cfg->subgroup_id;
    side->prior.object_id = cfg->object_id;
    side->prior.publisher_priority = cfg->publisher_priority;
    return MOQ_OK;
}

moq_result_t moq_session_write_fill_range(moq_session_t *s, moq_fill_t fill,
                                          moq_fetch_range_kind_t kind,
                                          uint64_t group_id, uint64_t object_id,
                                          uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;
    if (kind != MOQ_FETCH_RANGE_NON_EXISTENT && kind != MOQ_FETCH_RANGE_UNKNOWN &&
        kind != MOQ_FETCH_RANGE_TIMED_OUT)
        return MOQ_ERR_INVAL;
    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;
    size_t slot;
    moq_result_t rc = writable_entry(s, fill, &slot);
    if (rc != MOQ_OK) return rc;
    moq_fill_entry_t *e = &s->fill_storage[slot];
    if (action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    moq_action_t a;
    memset(&a, 0, sizeof(a));
    a.kind = MOQ_ACTION_SEND_DATA;
    a.detail_size = (uint32_t)sizeof(moq_send_data_action_t);
    a.borrow_epoch = s->borrow_epoch;
    moq_buf_writer_t hw;
    moq_buf_writer_init(&hw, a.u.send_data.header, sizeof(a.u.send_data.header));
    /* The profile owns which range kinds its wire can express. */
    rc = s->profile->encode_fetch_range(s, &hw, (uint32_t)kind, group_id, object_id);
    if (rc < 0) return rc;
    a.u.send_data.header_len = (uint8_t)moq_buf_writer_offset(&hw);
    a.u.send_data.stream_ref = e->stream;
    a.u.send_data.scheduling_priority = UINT64_C(0x10000) |
        ((uint64_t)e->delivery.subscriber_priority << 8) | 128;
    (void)push_action(s, &a);
    moq_fill_side_t *side = &s->fill_side[slot];
    side->prior.has_prev = true;
    side->prior.group_id = group_id;
    side->prior.object_id = object_id;
    return MOQ_OK;
}

moq_result_t moq_session_end_fill(moq_session_t *s, moq_fill_t fill, uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;
    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;
    size_t slot;
    moq_result_t rc = writable_entry(s, fill, &slot);
    if (rc != MOQ_OK) return rc;
    if (action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    moq_fill_ref_t ref = ref_of(s, slot);
    rc = moq_fill_finish(&s->fills, ref);
    if (rc != MOQ_OK) return rc;
    moq_fill_entry_t *e = &s->fill_storage[slot];
    moq_action_t a;
    memset(&a, 0, sizeof(a));
    a.kind = MOQ_ACTION_SEND_DATA;
    a.detail_size = (uint32_t)sizeof(moq_send_data_action_t);
    a.borrow_epoch = s->borrow_epoch;
    a.u.send_data.stream_ref = e->stream;
    a.u.send_data.fin = true;
    a.u.send_data.scheduling_priority = UINT64_C(0x10000) |
        ((uint64_t)e->delivery.subscriber_priority << 8) | 128;
    (void)push_action(s, &a);   /* reserved above */
    (void)moq_fill_action_committed(&s->fills, ref, MOQ_FILL_ACTION_FIN);
    fill_service(s);
    return MOQ_OK;
}

moq_result_t moq_session_abort_fill(moq_session_t *s, moq_fill_t fill,
                                    uint64_t error_code, uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;
    if (error_code > MOQ_QUIC_VARINT_MAX) return MOQ_ERR_INVAL;
    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;
    size_t slot;
    moq_result_t rc = publisher_entry(s, fill, &slot);
    if (rc != MOQ_OK) return rc;
    moq_fill_entry_t *e = &s->fill_storage[slot];
    moq_fill_ref_t ref = ref_of(s, slot);
    moq_fill_work_t w;
    if (moq_fill_next(&s->fills, ref, &w) != MOQ_OK) return MOQ_ERR_STALE_HANDLE;
    /* An unopened child is opened with its header first so the peer can tell
     * which fill failed, then reset; both actions are reserved up front. */
    bool need_header = !e->stream_bound;
    if (need_header && w.action != MOQ_FILL_ACTION_HEADER) return MOQ_ERR_WRONG_STATE;
    if (!need_header && !stream_open(e)) return MOQ_ERR_WRONG_STATE;
    if (e->abort_requested || e->parent_cancelled || e->peer_stop) return MOQ_ERR_WRONG_STATE;
    if (action_queue_avail(s) < (need_header ? 2u : 1u)) return MOQ_ERR_WOULD_BLOCK;
    if (need_header) {
        rc = open_stream(s, slot);
        if (rc < 0) return rc;
    }
    rc = moq_fill_abort(&s->fills, ref);
    if (rc != MOQ_OK) return rc;
    moq_fill_side_t *side = &s->fill_side[slot];
    side->error_domain = MOQ_FILL_ERROR_STREAM;
    side->error_code = error_code;
    push_stream_action(s, slot, MOQ_FILL_ACTION_RESET);
    fill_service(s);
    return MOQ_OK;
}

moq_result_t moq_session_cancel_fill(moq_session_t *s, moq_fill_t fill, uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;
    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;
    size_t slot;
    moq_result_t rc = fill_resolve(s, fill, &slot);
    if (rc != MOQ_OK) return rc;
    moq_fill_entry_t *e = &s->fill_storage[slot];
    if (e->role != MOQ_FILL_SUBSCRIBER) return MOQ_ERR_WRONG_STATE;
    if (e->cancel_requested) return MOQ_OK;
    bool need_stop = stream_open(e) && !e->stop_committed;
    if (need_stop && action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    moq_fill_ref_t ref = ref_of(s, slot);
    rc = moq_fill_cancel(&s->fills, ref);
    if (rc != MOQ_OK) return rc;
    if (need_stop) push_stream_action(s, slot, MOQ_FILL_ACTION_STOP);
    fill_service(s);
    return MOQ_OK;
}
