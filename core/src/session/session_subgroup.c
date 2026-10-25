#include "session_internal.h"
#include "../bridge/transport_bridge_internal.h"

_Static_assert(sizeof(moq_outgoing_subgroup_terminated_event_t) ==
               MOQ_OUTGOING_SUBGROUP_TERMINATED_EVENT_V0_SIZE,
               "outgoing subgroup terminal detail layout");
_Static_assert(sizeof(moq_outgoing_subgroup_terminated_event_t) <= MOQ_EVENT_DETAIL_MAX,
               "outgoing subgroup terminal exceeds event reserve");
_Static_assert(offsetof(moq_outgoing_subgroup_terminated_event_t, cause) == 56 &&
               offsetof(moq_outgoing_subgroup_terminated_event_t, _reserved) == 60,
               "outgoing subgroup terminal frozen prefix");

#define MOQ_SG_TERMINAL_RETRY_US 5000

/* Backstop registry feed: max-merge an outbound object's Location
 * into the owning subscription's or publication's reserved record. The facade
 * additionally merges via its own record reference (the authoritative feed);
 * this covers raw-session writers and is idempotent (monotonic max). */
static void sg_note_object_published(moq_session_t *s, const moq_sg_entry_t *e,
                                     uint64_t object_id)
{
    int slot = sub_resolve_handle(s, e->sub);
    if (slot >= 0) {
        track_hist_merge(s->subs[slot].hist, e->group_id, object_id);
        return;
    }
    slot = pub_resolve_handle(s, e->pub);
    if (slot >= 0)
        track_hist_merge(s->publishes[slot].hist, e->group_id, object_id);
}

/* Whether a subscription-owned subgroup belongs to a subscription the peer has
 * cancelled. Such a subgroup is owed the cancellation's reset: nothing else may
 * be written to it, a FIN included. A publication's failed-update teardown
 * likewise owes resets and must not be overtaken by producer output. */
static bool sg_owner_cancelled(moq_session_t *s, const moq_sg_entry_t *sg)
{
    if (moq_subscription_is_valid(sg->sub))
        return sub_cancel_pending(s, sg->sub);
    if (moq_publication_is_valid(sg->pub)) {
        int slot = pub_resolve_handle(s, sg->pub);
        return slot >= 0 && pub_control_reject_teardown(s, &s->publishes[slot]);
    }
    return false;
}

static bool sg_send_allowed(moq_session_t *s, const moq_sg_entry_t *sg)
{
    if (moq_subscription_is_valid(sg->sub)) {
        int slot = sub_resolve_handle(s, sg->sub);
        if (slot < 0) return false;
        if (s->subs[slot].cancel_pending) return false;
        return s->subs[slot].forward;
    }
    if (moq_publication_is_valid(sg->pub)) {
        int slot = pub_resolve_handle(s, sg->pub);
        if (slot < 0) return false;
        if (pub_control_reject_teardown(s, &s->publishes[slot])) return false;
        return s->publishes[slot].send_allowed;
    }
    return false;
}

/* -- Subgroup pool ------------------------------------------------- */

int sg_find_free(moq_session_t *s)
{
    for (size_t i = 0; i < s->sg_cap; i++)
        if (s->subgroups[i].state == MOQ_SG_FREE) return (int)i;
    return -1;
}

static bool sg_is_duplicate(moq_session_t *s, moq_subscription_t sub,
                             uint64_t group_id, uint64_t subgroup_id)
{
    for (size_t i = 0; i < s->sg_cap; i++) {
        moq_sg_entry_t *e = &s->subgroups[i];
        /* A stream already reset carries nothing more: opening the subgroup
         * on a new stream is how a sender continues it (§11.3.2: a relay
         * that does not know an Object is the next one "MUST reset the
         * Subgroup stream and open a new one to forward it"). */
        if (e->state == MOQ_SG_FREE || e->state == MOQ_SG_RESETTING ||
            e->state == MOQ_SG_RESET_OWED)
            continue;
        if (moq_subscription_eq(e->sub, sub) &&
            e->group_id == group_id && e->subgroup_id == subgroup_id)
            return true;
    }
    return false;
}

moq_subgroup_handle_t sg_make_handle(moq_session_t *s, size_t slot)
{
    moq_sg_entry_t *e = &s->subgroups[slot];
    uint64_t packed = moq_handle_pack(MOQ_HANDLE_POOL_SUBGROUP,
                                       s->session_tag,
                                       e->generation, (uint32_t)slot);
    moq_subgroup_handle_t h = { packed };
    return h;
}

/* Returns slot on a match, -1 if stale, -2 for another state, and -3 while
 * the exact handle is frozen behind an unpublished terminal obligation. */
static int sg_resolve_handle_ex(moq_session_t *s, moq_subgroup_handle_t h,
                                 moq_sg_state_t required)
{
    uint32_t pool = moq_handle_pool_tag(h._opaque);
    uint16_t tag  = moq_handle_session_tag(h._opaque);
    uint32_t slot = moq_handle_slot(h._opaque);
    uint32_t gen  = moq_handle_generation(h._opaque);
    if (pool != MOQ_HANDLE_POOL_SUBGROUP) return -1;
    if (tag != s->session_tag) return -1;
    if (slot >= s->sg_cap) return -1;
    if (s->subgroups[slot].generation != gen) return -1;
    if (s->subgroups[slot].state == MOQ_SG_TERMINAL_OWED &&
        required != MOQ_SG_TERMINAL_OWED) return -3;
    if (s->subgroups[slot].state != required) return -2;
    return (int)slot;
}

static int sg_resolve_handle(moq_session_t *s, moq_subgroup_handle_t h)
{
    int r = sg_resolve_handle_ex(s, h, MOQ_SG_OPEN);
    return r == -2 ? -1 : r;
}

int sg_find_by_stream_ref(moq_session_t *s, moq_stream_ref_t ref)
{
    /* The occupancy list holds exactly the live subgroups, in ascending slot
     * order, so its first match is the lowest-slot match. */
    for (int32_t i = s->sg_occ_head; i >= 0; i = s->subgroups[i].occ_next) {
#ifdef MOQ_SESSION_SWEEP_TESTING
        session_work_sg_stream_find++;
#endif
        moq_sg_entry_t *e = &s->subgroups[i];
        if (e->state != MOQ_SG_FREE && e->stream_ref._v == ref._v)
            return (int)i;
    }
    return -1;
}

void sg_free_entry(moq_session_t *s, size_t slot)
{
    moq_sg_entry_t *entries = s->subgroups;
    sg_occ_unlink(s, slot);
    entries[slot].delivery_deadline_us = UINT64_MAX;
    entries[slot].streaming_deadline_us = UINT64_MAX;
    entries[slot].ovr_has_object = entries[slot].ovr_has_subgroup = false;
    entries[slot].terminal_cause = MOQ_OUTGOING_SUBGROUP_TERMINAL_NONE;
    entries[slot].terminal_code = 0;
    entries[slot].state = MOQ_SG_FREE;
    entries[slot].generation++;
    /* Clear ownership handles so a reused slot can never be matched by an
     * owner scan (e.g. session_core_on_publish_unsubscribed) via a stale
     * pub/sub left over from the slot's previous occupant. */
    entries[slot].sub = MOQ_SUBSCRIPTION_INVALID;
    entries[slot].pub = MOQ_PUBLICATION_INVALID;
}

void sg_recompute_deadline(moq_session_t *s)
{
    uint64_t d = UINT64_MAX;
    /* Only live subgroups can carry a deadline, and only those are linked. */
    for (int32_t i = s->sg_occ_head; i >= 0; i = s->subgroups[i].occ_next) {
#ifdef MOQ_SESSION_SWEEP_TESTING
        session_work_sg_deadline_probes++;
#endif
        if (s->subgroups[i].state != MOQ_SG_FREE &&
            s->subgroups[i].delivery_deadline_us < d)
            d = s->subgroups[i].delivery_deadline_us;
    }
    s->subgroup_deadline_us = d;
}

/* -- Delivery timeouts (design: Delivery Timeouts and Data Reliability) --- */

/* The delivery timeouts the subgroup's owner negotiated: the publisher-side
 * values (this side's Track Properties) and the subscriber-side values (the
 * peer's parameters), with the owner's scheduling identity. */
static void sg_owner_terms(moq_session_t *s, const moq_sg_entry_t *e,
                           moq_dt_scan_t *pub, moq_dt_scan_t *sub,
                           uint64_t *owner, uint8_t *subscriber_priority,
                           uint8_t *group_order)
{
    memset(pub, 0, sizeof(*pub));
    memset(sub, 0, sizeof(*sub));
    *owner = 0;
    *subscriber_priority = 128;
    *group_order = MOQ_GROUP_ORDER_ASCENDING;
    int slot = sub_resolve_handle(s, e->sub);
    if (slot >= 0) {
        const moq_sub_entry_t *se = &s->subs[slot];
        pub->has_object = se->dt_pub_has_object;     pub->object_ms = se->dt_pub_object_ms;
        pub->has_subgroup = se->dt_pub_has_subgroup; pub->subgroup_ms = se->dt_pub_subgroup_ms;
        sub->has_object = se->dt_sub_has_object;     sub->object_ms = se->dt_sub_object_ms;
        sub->has_subgroup = se->dt_sub_has_subgroup; sub->subgroup_ms = se->dt_sub_subgroup_ms;
        *owner = se->handle._opaque;
        *subscriber_priority = se->subscriber_priority;
        *group_order = session_group_order(se->group_order, se->track_group_order);
        return;
    }
    slot = pub_resolve_handle(s, e->pub);
    if (slot >= 0) {
        const moq_pub_entry_t *pe = &s->publishes[slot];
        pub->has_object = pe->dt_pub_has_object;     pub->object_ms = pe->dt_pub_object_ms;
        pub->has_subgroup = pe->dt_pub_has_subgroup; pub->subgroup_ms = pe->dt_pub_subgroup_ms;
        sub->has_object = pe->dt_sub_has_object;     sub->object_ms = pe->dt_sub_object_ms;
        sub->has_subgroup = pe->dt_sub_has_subgroup; sub->subgroup_ms = pe->dt_sub_subgroup_ms;
        *owner = pe->handle._opaque;
        *subscriber_priority = pe->subscriber_priority;
        *group_order = session_group_order(pe->group_order, pe->track_group_order);
    }
}

/* The effective timeout of one kind for this subgroup: the publisher's value
 * is the first object's property when the profile honours it and it was
 * present, else the Track Property; the smaller of that and the subscriber's
 * non-zero value applies. */
static uint64_t sg_timeout_us(moq_session_t *s, const moq_sg_entry_t *e, bool subgroup_kind)
{
    moq_dt_scan_t pub, sub; uint64_t owner; uint8_t sp, go;
    sg_owner_terms(s, e, &pub, &sub, &owner, &sp, &go);
    bool ph, sh; uint64_t pm, sm;
    if (subgroup_kind) {
        ph = e->ovr_has_subgroup || pub.has_subgroup;
        pm = e->ovr_has_subgroup ? e->ovr_subgroup_ms : pub.subgroup_ms;
        sh = sub.has_subgroup; sm = sub.subgroup_ms;
    } else {
        ph = e->ovr_has_object || pub.has_object;
        pm = e->ovr_has_object ? e->ovr_object_ms : pub.object_ms;
        sh = sub.has_object; sm = sub.object_ms;
    }
    return ms_to_us_sat(dt_negotiate_ms(ph, pm, sh, sm));
}

uint64_t sg_object_deadline_us(moq_session_t *s, const moq_sg_entry_t *e, uint64_t now_us)
{
    uint64_t to = sg_timeout_us(s, e, false);
    return to ? deadline_add(now_us, to) : UINT64_MAX;
}

uint64_t sg_subgroup_timeout_us(moq_session_t *s, const moq_sg_entry_t *e)
{
    return sg_timeout_us(s, e, true);
}

void sg_stream_meta(moq_session_t *s, const moq_sg_entry_t *e, moq_action_meta_t *out)
{
    moq_dt_scan_t pub, sub;
    memset(out, 0, sizeof(*out));
    out->deadline_us = UINT64_MAX;
    sg_owner_terms(s, e, &pub, &sub, &out->owner, &out->subscriber_priority, &out->group_order);
    out->publisher_priority = e->publisher_priority;
    out->mode = MOQ_SCHED_MODE_SUBGROUP;
    out->group_id = e->group_id;
    out->subgroup_id = e->subgroup_id;
    out->has_location = true;
}

/* The first object's delivery-timeout properties, read into a copy of the
 * entry so the caller can price the object before committing anything. */
static void sg_read_first_object_override(moq_session_t *s, moq_sg_entry_t *probe,
                                          const uint8_t *props, size_t props_len)
{
    if (probe->has_prev_object || props_len == 0 ||
        !s->profile->first_object_timeout_override || !s->profile->scan_delivery_timeouts)
        return;
    moq_dt_scan_t sc;
    if (s->profile->scan_delivery_timeouts(props, props_len, true, &sc) != MOQ_OK) return;
    probe->ovr_has_object = sc.has_object;     probe->ovr_object_ms = sc.object_ms;
    probe->ovr_has_subgroup = sc.has_subgroup; probe->ovr_subgroup_ms = sc.subgroup_ms;
}

static void sg_commit_override(moq_sg_entry_t *entry, const moq_sg_entry_t *probe)
{
    entry->ovr_has_object = probe->ovr_has_object;
    entry->ovr_object_ms = probe->ovr_object_ms;
    entry->ovr_has_subgroup = probe->ovr_has_subgroup;
    entry->ovr_subgroup_ms = probe->ovr_subgroup_ms;
}

/* Where a streamed object's clock starts: at its header on profiles that say
 * so, otherwise at its first payload byte -- an object with no payload has
 * no such byte, so its header is its first byte. */
static uint64_t sg_streaming_header_deadline(moq_session_t *s, const moq_sg_entry_t *e,
                                             uint64_t payload_length, uint64_t now_us)
{
    if (s->profile->object_timer_at_header || payload_length == 0)
        return sg_object_deadline_us(s, e, now_us);
    return UINT64_MAX;
}

static void sg_terminal_action(moq_session_t *s, const moq_sg_entry_t *sg,
                                moq_action_t *a)
{
    memset(a, 0, sizeof(*a));
    a->kind = MOQ_ACTION_RESET_DATA;
    a->detail_size = (uint32_t)sizeof(moq_reset_data_action_t);
    a->borrow_epoch = s->borrow_epoch;
    a->u.reset_data.stream_ref = sg->stream_ref;
    a->u.reset_data.error_code = sg->terminal_code;
    a->u.reset_data.reliable_size = sg->header_len;
}

/* Keep the reset where the obsolete output began. The barrier is private:
 * poll never transfers it until its event can commit as well. */
static void sg_latch_terminal(moq_session_t *s, size_t slot,
                               moq_outgoing_subgroup_terminal_cause_t cause,
                               uint64_t code, size_t first)
{
    moq_sg_entry_t *sg = &s->subgroups[slot];
    if (sg->state != MOQ_SG_OPEN && sg->state != MOQ_SG_STREAMING &&
        sg->state != MOQ_SG_CLOSING) return;
    sg->terminal_cause = cause;
    sg->terminal_code = code;
    sg->state = MOQ_SG_TERMINAL_OWED;
    s->outgoing_terminal_dirty = true;
    sg->streaming_payload_len = sg->streaming_bytes_written = 0;
    sg->streaming_deadline_us = sg->delivery_deadline_us = UINT64_MAX;
    bool barrier = false;
    size_t w = s->action_head, cap = s->action_cap;
    for (size_t r = s->action_head; r < s->action_tail; r++) {
        moq_action_t *a = &s->actions[r % cap];
        moq_action_meta_t *m = &s->action_meta[r % cap];
        if (r >= first && a->kind == MOQ_ACTION_SEND_DATA &&
            a->u.send_data.stream_ref._v == sg->stream_ref._v) {
            session_action_dequeued(s, a, m);
            moq_action_cleanup(a);
            if (barrier) continue;
            /* Keep its data scheduling identity while unpublished: unrelated
             * higher-priority data can progress, but this stream's expired
             * bytes and its later owner terminal cannot cross the barrier. */
            memset(a, 0, sizeof(*a));
            a->kind = MOQ_ACTION_SEND_DATA;
            a->detail_size = (uint32_t)sizeof(moq_send_data_action_t);
            a->borrow_epoch = s->borrow_epoch;
            a->u.send_data.stream_ref = sg->stream_ref;
            m->deadline_us = UINT64_MAX;
            m->terminal_barrier = true;
            m->outgoing_terminal = true;
            m->subgroup_header = false;
            session_action_enqueued(s, a, m);
            barrier = true;
        }
        if (w != r) {
            s->actions[w % cap] = *a;
            s->action_meta[w % cap] = *m;
        }
        w++;
    }
    s->action_tail = w;
}

static size_t sg_first_output(const moq_session_t *s, moq_stream_ref_t ref,
                               bool keep_header)
{
    for (size_t i = s->action_head; i < s->action_tail; i++) {
        const moq_action_t *a = &s->actions[i % s->action_cap];
        if (a->kind == MOQ_ACTION_SEND_DATA &&
            a->u.send_data.stream_ref._v == ref._v &&
            !(keep_header && s->action_meta[i % s->action_cap].subgroup_header))
            return i;
    }
    return s->action_tail;
}

void session_expire_queued_data(moq_session_t *s)
{
    /* Preserve the untimed-ring fast path, including saturated-clock
     * expiry, while still retrying terminal debt after event admission. */
    bool due = false;
    for (size_t i = s->action_head;
         (s->action_timed != 0 || s->last_now_us == UINT64_MAX) &&
             i < s->action_tail && !due; i++) {
#ifdef MOQ_SESSION_SWEEP_TESTING
        session_work_expiry_search++;
#endif
        uint64_t deadline = s->action_meta[i % s->action_cap].deadline_us;
        due = deadline <= s->last_now_us;
    }
    if (!due) {
        sg_retry_owed_resets(s);
        return;
    }
    for (size_t i = s->action_head; i < s->action_tail; i++) {
#ifdef MOQ_SESSION_SWEEP_TESTING
        session_work_expiry_compact++;
#endif
        moq_action_t *a = &s->actions[i % s->action_cap];
        if (a->kind != MOQ_ACTION_SEND_DATA ||
            s->action_meta[i % s->action_cap].terminal_barrier ||
            s->action_meta[i % s->action_cap].subgroup_header ||
            s->action_meta[i % s->action_cap].deadline_us > s->last_now_us)
            continue;
        int slot = sg_find_by_stream_ref(s, a->u.send_data.stream_ref);
        if (slot >= 0 && (s->subgroups[slot].state == MOQ_SG_OPEN ||
                         s->subgroups[slot].state == MOQ_SG_STREAMING ||
                         s->subgroups[slot].state == MOQ_SG_CLOSING)) {
            sg_latch_terminal(s, (size_t)slot,
                MOQ_OUTGOING_SUBGROUP_TERMINAL_DELIVERY_TIMEOUT,
                MOQ_RESET_DELIVERY_TIMEOUT, i);
        } else if ((slot >= 0 && (s->subgroups[slot].state == MOQ_SG_RESETTING ||
                                 s->subgroups[slot].state == MOQ_SG_RESET_OWED)) ||
                   bridge_session_has_queued_data_reset(s, a->u.send_data.stream_ref)) {
            /* An earlier application/owner reset already won. Drop expired
             * output behind it without manufacturing a timeout event. */
            moq_stream_ref_t ref = a->u.send_data.stream_ref;
            for (size_t j = i; j < s->action_tail; j++) {
                moq_action_t *later = &s->actions[j % s->action_cap];
                moq_action_meta_t *m = &s->action_meta[j % s->action_cap];
                if (later->kind == MOQ_ACTION_SEND_DATA &&
                    later->u.send_data.stream_ref._v == ref._v) {
                    session_action_dequeued(s, later, m);
                    m->deadline_us = 0;
                    session_action_enqueued(s, later, m);
                }
            }
        }
    }
    size_t w = s->action_head;
    for (size_t r = s->action_head; r < s->action_tail; r++) {
#ifdef MOQ_SESSION_SWEEP_TESTING
        session_work_expiry_compact++;
#endif
        moq_action_t *a = &s->actions[r % s->action_cap];
        if ((a->kind == MOQ_ACTION_SEND_DATAGRAM ||
             (a->kind == MOQ_ACTION_SEND_DATA &&
              !s->action_meta[r % s->action_cap].terminal_barrier &&
              !s->action_meta[r % s->action_cap].subgroup_header)) &&
            s->action_meta[r % s->action_cap].deadline_us <= s->last_now_us) {
            session_action_dequeued(s, a, &s->action_meta[r % s->action_cap]);
            moq_action_cleanup(a);
            continue;
        }
        if (w != r) {
            s->actions[w % s->action_cap] = *a;
            s->action_meta[w % s->action_cap] = s->action_meta[r % s->action_cap];
        }
        w++;
    }
    s->action_tail = w;
    sg_retry_owed_resets(s);
    sg_recompute_deadline(s);
}

bool sg_has_terminal_debt(const moq_session_t *s)
{
    for (int32_t i = s->sg_occ_head; i >= 0; i = s->subgroups[i].occ_next)
        if (s->subgroups[i].state == MOQ_SG_TERMINAL_OWED) return true;
    return false;
}

bool bridge_session_take_terminal_changes(moq_session_t *s)
{
    bool dirty = s->outgoing_terminal_dirty;
    s->outgoing_terminal_dirty = false;
    return dirty;
}

void session_service_outgoing_terminals(moq_session_t *s)
{
    session_expire_queued_data(s);
    for (int32_t i = s->sg_occ_head; i >= 0; i = s->subgroups[i].occ_next) {
        moq_sg_entry_t *sg = &s->subgroups[i];
        if (sg->delivery_deadline_us != UINT64_MAX &&
            sg->delivery_deadline_us <= s->last_now_us)
            sg_latch_terminal(s, (size_t)i,
                MOQ_OUTGOING_SUBGROUP_TERMINAL_DELIVERY_TIMEOUT,
                MOQ_RESET_DELIVERY_TIMEOUT, sg_first_output(s, sg->stream_ref, true));
    }
    sg_retry_owed_resets(s);
    sg_recompute_deadline(s);
}

static void sg_queue_reset(moq_session_t *s, moq_sg_entry_t *sg, uint64_t code)
{
    moq_action_t a;
    memset(&a, 0, sizeof(a));
    a.kind = MOQ_ACTION_RESET_DATA;
    a.detail_size = (uint32_t)sizeof(moq_reset_data_action_t);
    a.borrow_epoch = s->borrow_epoch;
    a.u.reset_data.stream_ref = sg->stream_ref;
    a.u.reset_data.error_code = code;
    a.u.reset_data.reliable_size = sg->header_len;
    (void)push_action(s, &a);   /* the caller checked capacity */
    if (sg->terminal_cause != MOQ_OUTGOING_SUBGROUP_TERMINAL_NONE) {
        /* A pressure claim already published the event. Its compact reset
         * still precedes the owner's DONE when action capacity returns. */
        size_t at = s->action_head, last = s->action_tail - 1;
        for (size_t j = s->action_head; j < last; j++)
            if (s->action_meta[j % s->action_cap].subgroup_header &&
                s->actions[j % s->action_cap].u.send_data.stream_ref._v ==
                    sg->stream_ref._v) at = j + 1;
        moq_action_meta_t meta = s->action_meta[last % s->action_cap];
        meta.outgoing_terminal = true;
        for (size_t j = last; j > at; j--) {
            s->actions[j % s->action_cap] = s->actions[(j - 1) % s->action_cap];
            s->action_meta[j % s->action_cap] = s->action_meta[(j - 1) % s->action_cap];
        }
        s->actions[at % s->action_cap] = a;
        s->action_meta[at % s->action_cap] = meta;
    }
    sg->state = MOQ_SG_RESETTING;
}

void sg_reset_or_owe(moq_session_t *s, size_t slot, uint64_t code, size_t keep)
{
    moq_sg_entry_t *sg = &s->subgroups[slot];
    if (sg->terminal_cause != MOQ_OUTGOING_SUBGROUP_TERMINAL_NONE) return;
    sg->streaming_payload_len = 0;
    sg->streaming_bytes_written = 0;
    sg->streaming_deadline_us = UINT64_MAX;
    bool had_deadline = sg->delivery_deadline_us != UINT64_MAX;
    sg->delivery_deadline_us = UINT64_MAX;
    if (action_queue_avail(s) > keep) {
        sg_queue_reset(s, sg, code);
    } else {
        sg->state = MOQ_SG_RESET_OWED;
        sg->owed_reset_code = code;
    }
    if (had_deadline) sg_recompute_deadline(s);
}

static void sg_commit_terminal(moq_session_t *s, size_t slot)
{
    moq_sg_entry_t *sg = &s->subgroups[slot];
    moq_event_t ev;
    memset(&ev, 0, sizeof(ev));
    ev.kind = MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED;
    ev.detail_size = MOQ_OUTGOING_SUBGROUP_TERMINATED_EVENT_V0_SIZE;
    ev.borrow_epoch = s->borrow_epoch;
    ev.u.outgoing_subgroup_terminated =
        (moq_outgoing_subgroup_terminated_event_t){
            .subgroup = sg_make_handle(s, slot),
            .sub = sg->sub, .pub = sg->pub,
            .group_id = sg->group_id, .subgroup_id = sg->subgroup_id,
            .stream_ref = sg->stream_ref, .error_code = sg->terminal_code,
            .cause = sg->terminal_cause,
        };
    (void)push_event(s, &ev);  /* caller reserved capacity; no intervening producer */
    sg->state = MOQ_SG_RESETTING;
}

bool bridge_session_take_data_timeout(moq_session_t *s, moq_stream_ref_t ref,
                                      uint64_t now_us,
                                      moq_reset_data_action_t *reset,
                                      bool *outgoing_terminal)
{
    /* Bridge-retained expiry can be the first due work since the core's
     * last advance. Stamp retries at recognition without advancing here. */
    const uint64_t retry_now = now_us > s->last_now_us ? now_us : s->last_now_us;
    moq_reset_data_action_t value = *reset;
    value.stream_ref = ref;
    value.error_code = MOQ_RESET_DELIVERY_TIMEOUT;
    *outgoing_terminal = false;
    bool queued = false;
    for (size_t i = s->action_head;
         s->action_nondata != 0 && i < s->action_tail; ++i) {
        const moq_action_t *a = &s->actions[i % s->action_cap];
        if (a->kind == MOQ_ACTION_RESET_DATA && a->u.reset_data.stream_ref._v == ref._v) {
            value = a->u.reset_data;
            *outgoing_terminal = s->action_meta[i % s->action_cap].outgoing_terminal;
            queued = true;
            break;
        }
    }
    int slot = sg_find_by_stream_ref(s, ref);
    if (slot >= 0) {
        moq_sg_entry_t *sg = &s->subgroups[slot];
        if (!queued) {
            /* Freeze production before the entry can reap. An earlier
             * STOP/application reset remains the first cause. */
            sg_latch_terminal(s, (size_t)slot,
                MOQ_OUTGOING_SUBGROUP_TERMINAL_DELIVERY_TIMEOUT,
                MOQ_RESET_DELIVERY_TIMEOUT, sg_first_output(s, ref, true));
            value.reliable_size = sg->header_len;
            if (sg->terminal_cause != MOQ_OUTGOING_SUBGROUP_TERMINAL_NONE)
                value.error_code = sg->terminal_code;
            else if (sg->state == MOQ_SG_RESET_OWED)
                value.error_code = sg->owed_reset_code;
        }
        *outgoing_terminal |= sg->terminal_cause != MOQ_OUTGOING_SUBGROUP_TERMINAL_NONE;
        if (sg->state == MOQ_SG_TERMINAL_OWED) {
            if (event_queue_full(s)) {
                s->subgroup_retry_deadline_us =
                    deadline_add(retry_now, MOQ_SG_TERMINAL_RETRY_US);
                sg_recompute_deadline(s);
                return false;
            }
            /* The bridge already owns the reliable prefix. Commit only the
             * event here; its reset needs no extra action-ring slot. */
            sg_commit_terminal(s, (size_t)slot);
        }
        sg->state = MOQ_SG_RESETTING;
        sg->streaming_payload_len = sg->streaming_bytes_written = 0;
        sg->streaming_deadline_us = sg->delivery_deadline_us = UINT64_MAX;
        s->subgroup_retry_deadline_us = sg_has_terminal_debt(s)
            ? deadline_add(retry_now, MOQ_SG_TERMINAL_RETRY_US) : UINT64_MAX;
        sg_recompute_deadline(s);
    }
    /* Transfer an existing reset as well as its obsolete core data. If the
     * subgroup already reaped, preserve the queued cause and provenance. */
    bridge_session_discard_reset_data(s, ref);
    if (queued) {
        size_t dst = s->action_head, tail = s->action_tail;
        for (size_t src = s->action_head; src < tail; ++src) {
            moq_action_t *a = &s->actions[src % s->action_cap];
            moq_action_meta_t *m = &s->action_meta[src % s->action_cap];
            if (a->kind == MOQ_ACTION_RESET_DATA && a->u.reset_data.stream_ref._v == ref._v) {
                session_action_dequeued(s, a, m);
                moq_action_cleanup(a);
                continue;
            }
            size_t to = dst++ % s->action_cap;
            if (to != src % s->action_cap) {
                s->actions[to] = *a;
                s->action_meta[to] = *m;
            }
        }
        s->action_tail = dst;
        for (; dst < tail; ++dst) {
            memset(&s->actions[dst % s->action_cap], 0, sizeof(*s->actions));
            memset(&s->action_meta[dst % s->action_cap], 0, sizeof(*s->action_meta));
        }
    }
    *reset = value;
    return true;
}

bool sg_terminal_reset_ready(const moq_session_t *s, const moq_sg_entry_t *sg)
{
    if (sg->state == MOQ_SG_TERMINAL_OWED) {
        if (event_queue_full(s)) return false;
    } else if (sg->state != MOQ_SG_RESET_OWED ||
               sg->terminal_cause == MOQ_OUTGOING_SUBGROUP_TERMINAL_NONE) {
        return false;
    }
    for (size_t j = s->action_head; j < s->action_tail; j++) {
        const moq_action_t *a = &s->actions[j % s->action_cap];
        const moq_action_meta_t *m = &s->action_meta[j % s->action_cap];
        if (a->kind == MOQ_ACTION_SEND_DATA &&
            (m->terminal_barrier || m->subgroup_header) &&
            a->u.send_data.stream_ref._v == sg->stream_ref._v)
            return false;
    }
    return true;
}

bool sg_claim_terminal_reset(moq_session_t *s, size_t slot)
{
    moq_sg_entry_t *sg = &s->subgroups[slot];
    if (sg->state != MOQ_SG_TERMINAL_OWED || !sg_terminal_reset_ready(s, sg))
        return false;
    sg_commit_terminal(s, slot);
    sg->owed_reset_code = sg->terminal_code;
    sg->state = MOQ_SG_RESET_OWED;
    s->subgroup_retry_deadline_us = sg_has_terminal_debt(s)
        ? deadline_add(s->last_now_us, MOQ_SG_TERMINAL_RETRY_US) : UINT64_MAX;
    return true;
}

bool sg_take_terminal_action(moq_session_t *s, moq_action_t *out)
{
    for (int32_t i = s->sg_occ_head; i >= 0; i = s->subgroups[i].occ_next) {
        moq_sg_entry_t *sg = &s->subgroups[i];
        if (!sg_terminal_reset_ready(s, sg)) continue;
        sg_terminal_action(s, sg, out);
        if (sg->state == MOQ_SG_TERMINAL_OWED)
            sg_commit_terminal(s, (size_t)i);
        else
            sg->state = MOQ_SG_RESETTING; /* event committed by pressure claim */
        s->subgroup_retry_deadline_us = sg_has_terminal_debt(s)
            ? deadline_add(s->last_now_us, MOQ_SG_TERMINAL_RETRY_US) : UINT64_MAX;
        return true;
    }
    return false;
}

void sg_retry_owed_resets(moq_session_t *s)
{
    for (int32_t i = s->sg_occ_head; i >= 0; i = s->subgroups[i].occ_next) {
        moq_sg_entry_t *sg = &s->subgroups[i];
        if (sg->state == MOQ_SG_TERMINAL_OWED) {
            if (event_queue_full(s)) continue;
            size_t barrier = s->action_tail;
            for (size_t j = s->action_head; j < s->action_tail; j++) {
                if (s->action_meta[j % s->action_cap].terminal_barrier &&
                    s->actions[j % s->action_cap].u.send_data.stream_ref._v ==
                        sg->stream_ref._v) {
                    barrier = j;
                    break;
                }
            }
            if (barrier == s->action_tail) {
                if (action_queue_full(s)) continue;
                moq_action_t a;
                sg_terminal_action(s, sg, &a);
                (void)push_action(s, &a);
                /* The reset precedes any later owner terminal, but a queued
                 * subgroup header still has to reach the transport first. */
                size_t at = s->action_head;
                for (size_t j = s->action_head; j + 1 < s->action_tail; j++)
                    if (s->action_meta[j % s->action_cap].subgroup_header &&
                        s->actions[j % s->action_cap].u.send_data.stream_ref._v ==
                            sg->stream_ref._v) at = j + 1;
                size_t last = s->action_tail - 1;
                moq_action_meta_t meta = s->action_meta[last % s->action_cap];
                meta.outgoing_terminal = true;
                for (size_t j = last; j > at; j--) {
                    s->actions[j % s->action_cap] = s->actions[(j - 1) % s->action_cap];
                    s->action_meta[j % s->action_cap] = s->action_meta[(j - 1) % s->action_cap];
                }
                s->actions[at % s->action_cap] = a;
                s->action_meta[at % s->action_cap] = meta;
                barrier = at;
            }
            moq_action_t *a = &s->actions[barrier % s->action_cap];
            moq_action_meta_t *m = &s->action_meta[barrier % s->action_cap];
            session_action_dequeued(s, a, m);
            sg_terminal_action(s, sg, a);
            sg_commit_terminal(s, (size_t)i);
            m->terminal_barrier = false;
            session_action_enqueued(s, a, m);
        } else if (sg->state == MOQ_SG_RESET_OWED && !action_queue_full(s)) {
            sg_queue_reset(s, sg, sg->owed_reset_code);
        }
    }
    s->subgroup_retry_deadline_us = sg_has_terminal_debt(s)
        ? deadline_add(s->last_now_us, MOQ_SG_TERMINAL_RETRY_US) : UINT64_MAX;
}

/*
 * Reap CLOSING/RESETTING subgroups, resumably.
 *
 * Resumes at s->sweep_slot and charges one unit per ELIGIBLE reap; slots in
 * any other state cost nothing and never suspend the scan. Returns true when
 * the pool retired. The subgroup deadline is recomputed only after the
 * COMPLETE scan -- recomputing from a partial scan could drop a wake that a
 * not-yet-visited entry still needs.
 */
bool sg_reap_terminal_resumable(moq_session_t *s, uint32_t *budget)
{
    /*
     * Walks the subgroup OCCUPANCY list, not the pool: the reap's cost follows
     * the live subgroups, not `sg_cap`. The list is slot-ordered, so entries
     * are reaped in exactly the order the old full scan reaped them.
     */
    while (s->sweep_slot < s->sg_cap) {
        size_t i = s->sweep_slot;
        int32_t nxt = s->subgroups[i].occ_next;
#ifdef MOQ_SESSION_SWEEP_TESTING
        session_work_sg_reap_probes++;
#endif
        /* A CLOSING entry with an armed deadline is awaiting the transport's
         * commit report for its FIN; it stays until that report or its timer. */
        bool queued_expiry = false;
        if (s->action_timed != 0 && s->subgroups[i].state == MOQ_SG_CLOSING)
            for (size_t j = s->action_head; j < s->action_tail; j++)
                if (s->actions[j % s->action_cap].kind == MOQ_ACTION_SEND_DATA &&
                    s->actions[j % s->action_cap].u.send_data.stream_ref._v ==
                        s->subgroups[i].stream_ref._v &&
                    s->action_meta[j % s->action_cap].deadline_us != UINT64_MAX) {
                    queued_expiry = true;
                    break;
                }
        if (queued_expiry || (s->subgroups[i].state != MOQ_SG_CLOSING &&
             s->subgroups[i].state != MOQ_SG_RESETTING) ||
            (s->subgroups[i].state == MOQ_SG_CLOSING &&
             s->subgroups[i].delivery_deadline_us != UINT64_MAX)) {
            s->sweep_slot = (nxt >= 0) ? (size_t)nxt : s->sg_cap;
            continue;                      /* costs nothing; never suspends */
        }
        if (budget) {
            if (*budget == 0) return false;
            (*budget)--;
        }
#ifdef MOQ_SESSION_SWEEP_TESTING
        session_work_sg_reap_charged++;
#endif
        sg_free_entry(s, i);               /* unlinks, and advances the cursor */
        s->sweep_reaped_subgroup = true;
        /* The unlink fixup already moved the cursor off the freed slot; only a
         * path that did NOT move it needs the explicit advance. */
        if (s->sweep_slot == i)
            s->sweep_slot = (nxt >= 0) ? (size_t)nxt : s->sg_cap;
    }
    if (s->sweep_reaped_subgroup) {
        sg_recompute_deadline(s);
        s->sweep_reaped_subgroup = false;
    }
    return true;
}

/* -- Subgroup handle helpers --------------------------------------- */

static bool sg_handle_valid(uint64_t h)
{
    if (h == 0) return false;
    uint32_t pool = moq_handle_pool_tag(h);
    if (pool != MOQ_HANDLE_POOL_SUBGROUP) return false;
    uint16_t tag = moq_handle_session_tag(h);
    if (tag == 0) return false;
    uint32_t gen = moq_handle_generation(h);
    return (gen & 1u) != 0;
}

bool moq_subgroup_is_valid(moq_subgroup_handle_t h)
{
    return sg_handle_valid(h._opaque);
}

bool moq_subgroup_eq(moq_subgroup_handle_t a, moq_subgroup_handle_t b)
{
    return a._opaque == b._opaque;
}

uint64_t moq_subgroup_hash(moq_subgroup_handle_t h)
{
    uint64_t x = h._opaque;
    x ^= x >> 33;
    x *= 0xFF51AFD7ED558CCDULL;
    x ^= x >> 33;
    return x;
}

uint64_t moq_subgroup_id_for_trace(moq_subgroup_handle_t h)
{
    return h._opaque;
}

/* The sizeof of the layout that ends at end_of_group: the storage a caller
 * built against it allocates. */
#define SUBGROUP_CFG_SIZE_THROUGH_END_OF_GROUP                                 \
    ((offsetof(moq_subgroup_cfg_t, end_of_group) + sizeof(bool) +              \
      _Alignof(moq_subgroup_cfg_t) - 1u) &                                     \
     ~(size_t)(_Alignof(moq_subgroup_cfg_t) - 1u))

/* Parenthesized: defines the symbol, not the header's sized macro. A binary
 * that calls the symbol may hold the layout that ends at end_of_group, so it
 * clears and stamps only that extent; first_object stays unread. */
void (moq_subgroup_cfg_init)(moq_subgroup_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, SUBGROUP_CFG_SIZE_THROUGH_END_OF_GROUP);
    cfg->struct_size = (uint32_t)SUBGROUP_CFG_SIZE_THROUGH_END_OF_GROUP;
}

void moq_subgroup_cfg_init_sized(moq_subgroup_cfg_t *cfg, size_t cfg_size)
{
    if (!cfg || cfg_size < sizeof(cfg->struct_size)) return;
    size_t n = cfg_size < sizeof(*cfg) ? cfg_size : sizeof(*cfg);
    memset(cfg, 0, n);
    cfg->struct_size = (uint32_t)n;
}

/* -- Subgroup data path -------------------------------------------- */

void session_subgroup_data_key(const moq_action_meta_t *m,
                               moq_data_priority_t *out)
{
    memset(out, 0, sizeof(*out));
    out->subscriber_priority = m->subscriber_priority;
    out->publisher_priority = m->publisher_priority;
    out->descending = m->group_order == MOQ_GROUP_ORDER_DESCENDING;
    out->owner = m->owner;
    out->group_id = m->group_id;
    out->subgroup_id = m->subgroup_id;
}

moq_result_t session_push_data_key(moq_session_t *s, moq_stream_ref_t ref,
                                   const moq_data_priority_t *key,
                                   const moq_action_meta_t *meta)
{
    if (!s->data_priority_updates) return MOQ_OK;
    moq_action_t a;
    memset(&a, 0, sizeof(a));
    a.kind = MOQ_ACTION_SET_DATA_PRIORITY;
    a.detail_size = (uint32_t)sizeof(moq_set_data_priority_action_t);
    a.borrow_epoch = s->borrow_epoch;
    a.u.set_data_priority.stream_ref = ref;
    a.u.set_data_priority.priority = *key;
    return push_action_ex(s, &a, meta, UINT64_MAX);
}

moq_result_t session_update_owner_priority(moq_session_t *s, uint64_t owner,
                                           uint8_t subscriber_priority)
{
    moq_data_priority_t key;
    memset(&key, 0, sizeof(key));
    key.owner = owner;
    key.subscriber_priority = subscriber_priority;
    return session_push_data_key(s, moq_stream_ref_from_u64(0), &key, NULL);
}

moq_result_t moq_session_open_subgroup(
    moq_session_t *s, moq_subscription_t sub,
    const moq_subgroup_cfg_t *cfg, uint64_t now_us,
    moq_subgroup_handle_t *out_handle)
{
    if (!s || !cfg || !out_handle) return MOQ_ERR_INVAL;
    if (cfg->struct_size < offsetof(moq_subgroup_cfg_t, publisher_priority) +
        sizeof(cfg->publisher_priority)) return MOQ_ERR_INVAL;
    if (cfg->group_id > s->profile->location_varint_max) return MOQ_ERR_INVAL;
    if (cfg->subgroup_id > s->profile->location_varint_max) return MOQ_ERR_INVAL;
    *out_handle = MOQ_SUBGROUP_INVALID;

    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_WRONG_STATE;

    int sub_slot = sub_resolve_handle(s, sub);
    if (sub_slot < 0) return MOQ_ERR_STALE_HANDLE;
    if (s->subs[sub_slot].state != MOQ_SUB_ESTABLISHED) return MOQ_ERR_WRONG_STATE;
    if (s->subs[sub_slot].role != MOQ_SUB_ROLE_PUBLISHER) return MOQ_ERR_WRONG_STATE;
    if (s->subs[sub_slot].cancel_pending) return MOQ_ERR_WRONG_STATE;
    if (!s->subs[sub_slot].forward) return MOQ_ERR_WRONG_STATE;

    if (sg_is_duplicate(s, sub, cfg->group_id, cfg->subgroup_id))
        return MOQ_ERR_INVAL;

    int slot = sg_find_free(s);
    if (slot < 0) return MOQ_ERR_WOULD_BLOCK;

    /* The stream's key goes out ahead of its first SEND_DATA. */
    if (action_queue_avail(s) < (s->data_priority_updates ? 2u : 1u))
        return MOQ_ERR_WOULD_BLOCK;

    bool has_ext = false;
    if (cfg->struct_size >= offsetof(moq_subgroup_cfg_t, object_properties) +
        sizeof(cfg->object_properties))
        has_ext = cfg->object_properties;

    bool eog = false;
    if (cfg->struct_size >= offsetof(moq_subgroup_cfg_t, end_of_group) +
        sizeof(cfg->end_of_group))
        eog = cfg->end_of_group;
    bool first = false;
    if (cfg->struct_size >= offsetof(moq_subgroup_cfg_t, first_object) +
        sizeof(cfg->first_object))
        first = cfg->first_object;

    moq_subgroup_header_encode_args_t hdr_args = {
        .track_alias = s->subs[sub_slot].track_alias,
        .group_id = cfg->group_id,
        .subgroup_id = cfg->subgroup_id,
        .publisher_priority = cfg->publisher_priority,
        .has_extensions = has_ext,
        .end_of_group = eog,
        .first_object = first,
    };

    moq_action_t a;
    memset(&a, 0, sizeof(a));
    a.kind = MOQ_ACTION_SEND_DATA;
    a.detail_size = (uint32_t)sizeof(moq_send_data_action_t);
    a.borrow_epoch = s->borrow_epoch;

    moq_buf_writer_t hw;
    moq_buf_writer_init(&hw, a.u.send_data.header, 32);
    moq_result_t rc = s->profile->encode_subgroup_header(s, &hw, &hdr_args);
    if (rc < 0) return rc;
    a.u.send_data.header_len = (uint8_t)moq_buf_writer_offset(&hw);

    a.u.send_data.stream_ref = moq_stream_ref_from_u64(s->next_stream_ref);
    a.u.send_data.payload = NULL;
    a.u.send_data.fin = false;

    a.u.send_data.scheduling_priority = UINT64_C(0x10000) |
        ((uint64_t)s->subs[sub_slot].subscriber_priority << 8) |
        cfg->publisher_priority;

    /* The header is queued before the entry exists; its scheduling record
     * is the record every later action on this stream will carry. */
    moq_sg_entry_t probe;
    memset(&probe, 0, sizeof(probe));
    probe.sub = sub;
    probe.pub = MOQ_PUBLICATION_INVALID;
    probe.group_id = cfg->group_id;
    probe.subgroup_id = cfg->subgroup_id;
    probe.publisher_priority = cfg->publisher_priority;
    moq_action_meta_t meta;
    sg_stream_meta(s, &probe, &meta);
    meta.subgroup_header = true;

    moq_data_priority_t key;
    session_subgroup_data_key(&meta, &key);
    rc = session_push_data_key(s, a.u.send_data.stream_ref, &key, &meta);
    if (rc < 0) return rc;
    rc = push_action_ex(s, &a, &meta, UINT64_MAX);
    if (rc < 0) return rc;

    /* Commit. */
    moq_sg_entry_t *entry = &s->subgroups[slot];
    entry->generation |= 1;
    /* Allocated: join the occupancy list the preamble scans walk. */
    sg_occ_link(s, (size_t)slot);
    entry->state = MOQ_SG_OPEN;
    entry->publisher_priority = cfg->publisher_priority;
    entry->header_len = a.u.send_data.header_len;
    entry->sub = sub;
    entry->pub = MOQ_PUBLICATION_INVALID;   /* subscription-backed: no owning
                                             * publication (mirror of the
                                             * publication path setting sub
                                             * INVALID) so an owner scan never
                                             * matches a stale reused handle. */
    entry->stream_ref = a.u.send_data.stream_ref;
    entry->group_id = cfg->group_id;
    entry->subgroup_id = cfg->subgroup_id;
    entry->has_prev_object = false;
    entry->has_extensions = has_ext;
    entry->streaming_deadline_us = UINT64_MAX;
    entry->ovr_has_object = entry->ovr_has_subgroup = false;
    entry->terminal_cause = MOQ_OUTGOING_SUBGROUP_TERMINAL_NONE;
    entry->terminal_code = 0;
    /* The subgroup timer uses the negotiated SUBGROUP timeout: the per-type
     * smaller non-zero of the publisher-side (our Track Properties) and
     * subscriber-side (peer parameters / acknowledged updates) values. On
     * profiles whose timer runs from the FIN it is armed by close_subgroup
     * instead, where the first object's override is also known. */
    entry->delivery_deadline_us = UINT64_MAX;
    if (!s->profile->subgroup_timer_at_fin) {
        const moq_sub_entry_t *dte = &s->subs[sub_slot];
        uint64_t sg_ms = dt_negotiate_ms(
            dte->dt_pub_has_subgroup, dte->dt_pub_subgroup_ms,
            dte->dt_sub_has_subgroup, dte->dt_sub_subgroup_ms);
        uint64_t sg_us = ms_to_us_sat(sg_ms);
        if (sg_us > 0) {
            entry->delivery_deadline_us = deadline_add(now_us, sg_us);
            if (entry->delivery_deadline_us < s->subgroup_deadline_us)
                s->subgroup_deadline_us = entry->delivery_deadline_us;
        }
    }
    if (s->subs[sub_slot].subgroup_streams_opened != UINT64_MAX)
        s->subs[sub_slot].subgroup_streams_opened++;
    s->next_stream_ref++;

    *out_handle = sg_make_handle(s, (size_t)slot);
    return MOQ_OK;
}

moq_result_t moq_session_write_object(
    moq_session_t *s, moq_subgroup_handle_t subgroup,
    uint64_t object_id, moq_rcbuf_t *payload, uint64_t now_us)
{
    if (!s || !payload) return MOQ_ERR_INVAL;

    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;

    int slot = sg_resolve_handle(s, subgroup);
    if (slot == -3) return MOQ_ERR_WOULD_BLOCK;
    if (slot < 0) {
        if (sg_resolve_handle_ex(s, subgroup, MOQ_SG_STREAMING) >= 0)
            return MOQ_ERR_WRONG_STATE;
        return MOQ_ERR_STALE_HANDLE;
    }

    moq_sg_entry_t *entry = &s->subgroups[slot];
    if (!sg_send_allowed(s, entry)) return MOQ_ERR_WRONG_STATE;

    /* Monotonically increasing object IDs; gaps legal. */
    if (entry->has_prev_object && object_id <= entry->prev_object_id)
        return MOQ_ERR_INVAL;

    size_t payload_len = moq_rcbuf_len(payload);
    if (!session_payload_size_fits_profile(s, payload_len))
        return MOQ_ERR_INVAL;

    if (action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;

    moq_object_header_encode_args_t obj_args = {
        .object_id = object_id,
        .prev_object_id = entry->prev_object_id,
        .has_prev_object = entry->has_prev_object,
        .payload_len = payload_len,
        /* On an extension-enabled subgroup, an object with no properties still
         * encodes a zero-length extensions field (matches *_ex with no
         * properties); omitting it would emit the non-extension header on a
         * stream whose subgroup header promised extensions. */
        .has_extensions = entry->has_extensions,
    };

    moq_action_t a;
    memset(&a, 0, sizeof(a));
    a.kind = MOQ_ACTION_SEND_DATA;
    a.detail_size = (uint32_t)sizeof(moq_send_data_action_t);
    a.borrow_epoch = s->borrow_epoch;

    moq_buf_writer_t hw;
    moq_buf_writer_init(&hw, a.u.send_data.header, 32);
    moq_result_t rc = s->profile->encode_object_header(s, &hw, &obj_args);
    if (rc < 0) return rc;
    a.u.send_data.header_len = (uint8_t)moq_buf_writer_offset(&hw);

    a.u.send_data.stream_ref = entry->stream_ref;
    a.u.send_data.payload = payload;
    a.u.send_data.fin = false;

    /* Incref AFTER all validation succeeds. */
    moq_rcbuf_incref(payload);

    rc = push_action_ex(s, &a, NULL, sg_object_deadline_us(s, entry, now_us));
    if (rc < 0) {
        moq_rcbuf_decref(payload);
        return rc;
    }

    /* Commit. */
    entry->prev_object_id = object_id;
    entry->has_prev_object = true;
    sg_note_object_published(s, entry, object_id);
    return MOQ_OK;
}

/* Write a zero-length terminal status object on a subgroup stream (the reliable,
 * non-datagram counterpart to send_status_datagram). END_OF_GROUP / END_OF_TRACK
 * only -- NORMAL has no zero-length stream meaning. The terminal object carries
 * no properties, but on an extension-enabled subgroup it still encodes the
 * zero-length extensions field (a zero-length extension block on a non-Normal
 * stream object is legal). The caller closes the subgroup afterwards
 * (END_OF_TRACK ends the track; the stream FIN is the close). */
moq_result_t moq_session_write_status_object(
    moq_session_t *s, moq_subgroup_handle_t subgroup,
    uint64_t object_id, moq_object_status_t status, uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;
    uint64_t wire;
    if (status == MOQ_OBJECT_END_OF_GROUP) wire = 0x3;
    else if (status == MOQ_OBJECT_END_OF_TRACK) wire = 0x4;
    else return MOQ_ERR_INVAL;

    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;

    int slot = sg_resolve_handle(s, subgroup);
    if (slot == -3) return MOQ_ERR_WOULD_BLOCK;
    if (slot < 0) {
        if (sg_resolve_handle_ex(s, subgroup, MOQ_SG_STREAMING) >= 0)
            return MOQ_ERR_WRONG_STATE;
        return MOQ_ERR_STALE_HANDLE;
    }

    moq_sg_entry_t *entry = &s->subgroups[slot];
    if (!sg_send_allowed(s, entry)) return MOQ_ERR_WRONG_STATE;
    if (entry->has_prev_object && object_id <= entry->prev_object_id)
        return MOQ_ERR_INVAL;
    if (action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;

    moq_object_header_encode_args_t obj_args = {
        .object_id = object_id,
        .prev_object_id = entry->prev_object_id,
        .has_prev_object = entry->has_prev_object,
        .payload_len = 0,
        .object_status = wire,
        /* Encode the zero-length extensions field on an extension-enabled
         * subgroup so the terminal status object matches the stream's promised
         * header shape. */
        .has_extensions = entry->has_extensions,
    };

    moq_action_t a;
    memset(&a, 0, sizeof(a));
    a.kind = MOQ_ACTION_SEND_DATA;
    a.detail_size = (uint32_t)sizeof(moq_send_data_action_t);
    a.borrow_epoch = s->borrow_epoch;

    moq_buf_writer_t hw;
    moq_buf_writer_init(&hw, a.u.send_data.header, 32);
    moq_result_t rc = s->profile->encode_object_header(s, &hw, &obj_args);
    if (rc < 0) return rc;
    a.u.send_data.header_len = (uint8_t)moq_buf_writer_offset(&hw);
    a.u.send_data.stream_ref = entry->stream_ref;
    a.u.send_data.payload = NULL;   /* zero-length status object */
    a.u.send_data.fin = false;

    rc = push_action_ex(s, &a, NULL, sg_object_deadline_us(s, entry, now_us));
    if (rc < 0) return rc;

    entry->prev_object_id = object_id;
    entry->has_prev_object = true;
    sg_note_object_published(s, entry, object_id);
    return MOQ_OK;
}

/* The extent a caller built before the gap fields were appended holds. */
#define MOQ_OBJECT_CFG_SIZE_THROUGH_PROPERTIES \
    (offsetof(moq_object_cfg_t, properties) + sizeof(moq_rcbuf_t *))
#define OBJECT_CFG_HAS_GAPS(cfg) \
    ((cfg)->struct_size >= offsetof(moq_object_cfg_t, prior_object_gap) + \
                           sizeof(uint64_t))

/* Parenthesized: defines the symbol, not the header's sized macro. */
void (moq_object_cfg_init)(moq_object_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, MOQ_OBJECT_CFG_SIZE_THROUGH_PROPERTIES);
    cfg->struct_size = (uint32_t)MOQ_OBJECT_CFG_SIZE_THROUGH_PROPERTIES;
}

void moq_object_cfg_init_sized(moq_object_cfg_t *cfg, size_t cfg_size)
{
    if (!cfg) return;
    size_t n = cfg_size < sizeof(*cfg) ? cfg_size : sizeof(*cfg);
    if (n < sizeof(cfg->struct_size)) return;   /* too small to even stamp */
    memset(cfg, 0, n);
    cfg->struct_size = (uint32_t)n;
}

static moq_result_t write_object_ex_impl(moq_session_t *s,
                                         moq_subgroup_handle_t subgroup,
                                         const moq_object_cfg_t *cfg,
                                         uint64_t now_us);

/* The object's properties with its Prior Group/Object ID Gaps added in type
 * order (§10.8, §10.9), as a new buffer the caller releases. A gap the
 * properties already state with another value is MOQ_ERR_INVAL. */
static moq_result_t object_gap_properties(moq_session_t *s,
                                          moq_subgroup_handle_t subgroup,
                                          const moq_object_cfg_t *cfg,
                                          moq_rcbuf_t **out)
{
    *out = NULL;
    if (!s->profile->add_object_gap) return MOQ_ERR_UNSUPPORTED;
    int slot = sg_resolve_handle(s, subgroup);
    if (slot == -3) return MOQ_ERR_WOULD_BLOCK;
    if (slot >= 0) {
        if (cfg->has_prior_group_gap &&
            cfg->prior_group_gap > s->subgroups[slot].group_id)
            return MOQ_ERR_INVAL;
    }
    if (cfg->has_prior_object_gap && cfg->prior_object_gap > cfg->object_id)
        return MOQ_ERR_INVAL;
    size_t in_len = cfg->properties ? moq_rcbuf_len(cfg->properties) : 0;
    const uint8_t *in = cfg->properties ? moq_rcbuf_data(cfg->properties) : NULL;
    /* Each added pair is a type delta and a value (at most 9 bytes each), and
     * re-deltas the pair after it by at most another 9. */
    size_t cap = in_len + 2u * 27u;
    uint8_t *a = (uint8_t *)s->alloc.alloc(cap, s->alloc.ctx);
    uint8_t *b = (uint8_t *)s->alloc.alloc(cap, s->alloc.ctx);
    moq_result_t rc = (a && b) ? MOQ_OK : MOQ_ERR_NOMEM;
    size_t len = in_len;
    if (rc == MOQ_OK && in_len > 0) memcpy(a, in, in_len);
    if (rc == MOQ_OK && cfg->has_prior_group_gap) {
        rc = s->profile->add_object_gap(a, len, false, cfg->prior_group_gap,
                                        b, cap, &len);
        if (rc == MOQ_OK) memcpy(a, b, len);
    }
    if (rc == MOQ_OK && cfg->has_prior_object_gap) {
        rc = s->profile->add_object_gap(a, len, true, cfg->prior_object_gap,
                                        b, cap, &len);
        if (rc == MOQ_OK) memcpy(a, b, len);
    }
    if (rc == MOQ_ERR_BUFFER) rc = MOQ_ERR_INVAL;
    if (rc == MOQ_OK) rc = moq_rcbuf_create(&s->alloc, a, len, out);
    if (a) s->alloc.free(a, cap, s->alloc.ctx);
    if (b) s->alloc.free(b, cap, s->alloc.ctx);
    return rc;
}

moq_result_t moq_session_write_object_ex(
    moq_session_t *s, moq_subgroup_handle_t subgroup,
    const moq_object_cfg_t *cfg, uint64_t now_us)
{
    if (!s || !cfg) return MOQ_ERR_INVAL;
    if (cfg->struct_size < MOQ_OBJECT_CFG_SIZE_THROUGH_PROPERTIES)
        return MOQ_ERR_INVAL;
    if (!OBJECT_CFG_HAS_GAPS(cfg) ||
        (!cfg->has_prior_group_gap && !cfg->has_prior_object_gap))
        return write_object_ex_impl(s, subgroup, cfg, now_us);
    moq_rcbuf_t *props = NULL;
    moq_result_t rc = object_gap_properties(s, subgroup, cfg, &props);
    if (rc < 0) return rc;
    moq_object_cfg_t merged;
    moq_object_cfg_init_sized(&merged, MOQ_OBJECT_CFG_SIZE_THROUGH_PROPERTIES);
    merged.object_id = cfg->object_id;
    merged.payload = cfg->payload;
    merged.properties = props;
    rc = write_object_ex_impl(s, subgroup, &merged, now_us);
    moq_rcbuf_decref(props);
    return rc;
}

static moq_result_t write_object_ex_impl(moq_session_t *s,
                                         moq_subgroup_handle_t subgroup,
                                         const moq_object_cfg_t *cfg,
                                         uint64_t now_us)
{

    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;

    int slot = sg_resolve_handle(s, subgroup);
    if (slot == -3) return MOQ_ERR_WOULD_BLOCK;
    if (slot < 0) {
        if (sg_resolve_handle_ex(s, subgroup, MOQ_SG_STREAMING) >= 0)
            return MOQ_ERR_WRONG_STATE;
        return MOQ_ERR_STALE_HANDLE;
    }

    moq_sg_entry_t *entry = &s->subgroups[slot];
    if (!sg_send_allowed(s, entry)) return MOQ_ERR_WRONG_STATE;

    if (entry->has_prev_object && cfg->object_id <= entry->prev_object_id)
        return MOQ_ERR_INVAL;

    size_t payload_len = cfg->payload ? moq_rcbuf_len(cfg->payload) : 0;
    if (!session_payload_size_fits_profile(s, payload_len))
        return MOQ_ERR_INVAL;
    size_t props_len = cfg->properties ? moq_rcbuf_len(cfg->properties) : 0;

    /* Properties require extensions-enabled subgroup. */
    if (props_len > 0 && !entry->has_extensions) return MOQ_ERR_INVAL;

    /* Refuse to emit object properties the draft forbids (draft-18: a Mandatory
     * Track Property carried as an object property is malformed) -- symmetric
     * with the inbound check, so we never put unreadable wire on the network. */
    if (props_len > 0 && s->profile->validate_object_properties &&
        s->profile->validate_object_properties(
            s, moq_rcbuf_data(cfg->properties), props_len) < 0)
        return MOQ_ERR_INVAL;

    bool has_props = (props_len > 0);
    size_t slots_needed = has_props ? 2 : 1;
    if (action_queue_avail(s) < slots_needed) return MOQ_ERR_WOULD_BLOCK;

    moq_sg_entry_t probe = *entry;
    if (has_props)
        sg_read_first_object_override(s, &probe, moq_rcbuf_data(cfg->properties), props_len);
    uint64_t deadline = sg_object_deadline_us(s, &probe, now_us);

    if (has_props) {
        /* Two-action pattern: header+properties, then payload_len+payload. */
        moq_object_header_encode_args_t hdr_args = {
            .object_id = cfg->object_id,
            .prev_object_id = entry->prev_object_id,
            .has_prev_object = entry->has_prev_object,
            .has_extensions = true,
            .properties_len = props_len,
            .header_only = true,
        };

        moq_action_t a1;
        memset(&a1, 0, sizeof(a1));
        a1.kind = MOQ_ACTION_SEND_DATA;
        a1.detail_size = (uint32_t)sizeof(moq_send_data_action_t);
        a1.borrow_epoch = s->borrow_epoch;

        moq_buf_writer_t hw1;
        moq_buf_writer_init(&hw1, a1.u.send_data.header, 32);
        moq_result_t rc = s->profile->encode_object_header(s, &hw1, &hdr_args);
        if (rc < 0) return rc;
        a1.u.send_data.header_len = (uint8_t)moq_buf_writer_offset(&hw1);
        a1.u.send_data.stream_ref = entry->stream_ref;
        a1.u.send_data.payload = cfg->properties;
        a1.u.send_data.fin = false;

        moq_action_t a2;
        memset(&a2, 0, sizeof(a2));
        a2.kind = MOQ_ACTION_SEND_DATA;
        a2.detail_size = (uint32_t)sizeof(moq_send_data_action_t);
        a2.borrow_epoch = s->borrow_epoch;

        moq_buf_writer_t hw2;
        moq_buf_writer_init(&hw2, a2.u.send_data.header, 32);
        rc = s->profile->encode_object_payload_prefix(s, &hw2, payload_len, true);
        if (rc < 0) return rc;
        a2.u.send_data.header_len = (uint8_t)moq_buf_writer_offset(&hw2);
        a2.u.send_data.stream_ref = entry->stream_ref;
        a2.u.send_data.payload = cfg->payload;
        a2.u.send_data.fin = false;

        moq_rcbuf_incref(cfg->properties);
        if (cfg->payload) moq_rcbuf_incref(cfg->payload);

        rc = push_action_ex(s, &a1, NULL, deadline);
        if (rc < 0) {
            moq_rcbuf_decref(cfg->properties);
            if (cfg->payload) moq_rcbuf_decref(cfg->payload);
            return rc;
        }
        rc = push_action_ex(s, &a2, NULL, deadline);
        if (rc < 0) {
            if (cfg->payload) moq_rcbuf_decref(cfg->payload);
            return rc;
        }
    } else {
        /* Single-action: encode full object header + payload. */
        moq_object_header_encode_args_t obj_args = {
            .object_id = cfg->object_id,
            .prev_object_id = entry->prev_object_id,
            .has_prev_object = entry->has_prev_object,
            .payload_len = payload_len,
            .has_extensions = entry->has_extensions,
        };

        moq_action_t a;
        memset(&a, 0, sizeof(a));
        a.kind = MOQ_ACTION_SEND_DATA;
        a.detail_size = (uint32_t)sizeof(moq_send_data_action_t);
        a.borrow_epoch = s->borrow_epoch;

        moq_buf_writer_t hw;
        moq_buf_writer_init(&hw, a.u.send_data.header, 32);
        moq_result_t rc = s->profile->encode_object_header(s, &hw, &obj_args);
        if (rc < 0) return rc;
        a.u.send_data.header_len = (uint8_t)moq_buf_writer_offset(&hw);
        a.u.send_data.stream_ref = entry->stream_ref;
        a.u.send_data.payload = cfg->payload;
        a.u.send_data.fin = false;

        if (cfg->payload) moq_rcbuf_incref(cfg->payload);

        rc = push_action_ex(s, &a, NULL, deadline);
        if (rc < 0) {
            if (cfg->payload) moq_rcbuf_decref(cfg->payload);
            return rc;
        }
    }

    sg_commit_override(entry, &probe);
    entry->prev_object_id = cfg->object_id;
    entry->has_prev_object = true;
    sg_note_object_published(s, entry, cfg->object_id);
    return MOQ_OK;
}

moq_result_t moq_session_close_subgroup(
    moq_session_t *s, moq_subgroup_handle_t subgroup, uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;

    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;

    int slot = sg_resolve_handle(s, subgroup);
    if (slot == -3) return MOQ_ERR_WOULD_BLOCK;
    if (slot < 0) {
        if (sg_resolve_handle_ex(s, subgroup, MOQ_SG_STREAMING) >= 0)
            return MOQ_ERR_WRONG_STATE;
        return MOQ_ERR_STALE_HANDLE;
    }
    /* A FIN cannot stand in for the reset a cancelled subscription owes this
     * stream. Forward=0 alone does not forbid closing. */
    if (sg_owner_cancelled(s, &s->subgroups[slot])) return MOQ_ERR_WRONG_STATE;

    if (action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;

    moq_action_t a;
    memset(&a, 0, sizeof(a));
    a.kind = MOQ_ACTION_SEND_DATA;
    a.detail_size = (uint32_t)sizeof(moq_send_data_action_t);
    a.borrow_epoch = s->borrow_epoch;
    a.u.send_data.stream_ref = s->subgroups[slot].stream_ref;
    a.u.send_data.header_len = 0;
    a.u.send_data.payload = NULL;
    a.u.send_data.fin = true;

    moq_result_t rc = push_action(s, &a);
    if (rc < 0) return rc;

    s->subgroups[slot].state = MOQ_SG_CLOSING;
    s->subgroups[slot].streaming_deadline_us = UINT64_MAX;
    if (s->profile->subgroup_timer_at_fin) {
        /* All objects of the subgroup are published: the SUBGROUP timer
         * runs from here until the transport reports the stream committed. */
        uint64_t to = sg_subgroup_timeout_us(s, &s->subgroups[slot]);
        s->subgroups[slot].delivery_deadline_us = to ? deadline_add(now_us, to) : UINT64_MAX;
        sg_recompute_deadline(s);
    } else if (s->subgroups[slot].delivery_deadline_us != UINT64_MAX) {
        s->subgroups[slot].delivery_deadline_us = UINT64_MAX;
        sg_recompute_deadline(s);
    }
    return MOQ_OK;
}

moq_result_t moq_session_reset_subgroup(
    moq_session_t *s, moq_subgroup_handle_t subgroup,
    uint64_t error_code, uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;
    if (error_code > MOQ_QUIC_VARINT_MAX) return MOQ_ERR_INVAL;

    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;

    int slot = sg_resolve_handle(s, subgroup);
    if (slot == -3) return MOQ_ERR_WOULD_BLOCK;
    if (slot < 0) {
        slot = sg_resolve_handle_ex(s, subgroup, MOQ_SG_STREAMING);
        if (slot == -2) return MOQ_ERR_STALE_HANDLE;
        if (slot < 0) return MOQ_ERR_STALE_HANDLE;
    }

    if (action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;

    moq_action_t a;
    memset(&a, 0, sizeof(a));
    a.kind = MOQ_ACTION_RESET_DATA;
    a.detail_size = (uint32_t)sizeof(moq_reset_data_action_t);
    a.borrow_epoch = s->borrow_epoch;
    a.u.reset_data.stream_ref = s->subgroups[slot].stream_ref;
    a.u.reset_data.error_code = error_code;
    a.u.reset_data.reliable_size = s->subgroups[slot].header_len;

    moq_result_t rc = push_action(s, &a);
    if (rc < 0) return rc;

    s->subgroups[slot].state = MOQ_SG_RESETTING;
    s->subgroups[slot].streaming_payload_len = 0;
    s->subgroups[slot].streaming_bytes_written = 0;
    if (s->subgroups[slot].delivery_deadline_us != UINT64_MAX) {
        s->subgroups[slot].delivery_deadline_us = UINT64_MAX;
        sg_recompute_deadline(s);
    }
    return MOQ_OK;
}

/* -- Streaming object send ----------------------------------------- */

moq_result_t moq_session_begin_object(
    moq_session_t *s, moq_subgroup_handle_t subgroup,
    uint64_t object_id, uint64_t payload_length, uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;

    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;

    int slot = sg_resolve_handle(s, subgroup);
    if (slot == -3) return MOQ_ERR_WOULD_BLOCK;
    if (slot < 0) {
        if (sg_resolve_handle_ex(s, subgroup, MOQ_SG_STREAMING) >= 0)
            return MOQ_ERR_WRONG_STATE;
        return MOQ_ERR_STALE_HANDLE;
    }

    moq_sg_entry_t *entry = &s->subgroups[slot];
    if (!sg_send_allowed(s, entry)) return MOQ_ERR_WRONG_STATE;

    if (entry->has_prev_object && object_id <= entry->prev_object_id)
        return MOQ_ERR_INVAL;

    if (payload_length > s->profile->object_payload_len_max)
        return MOQ_ERR_INVAL;

    if (action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;

    moq_object_header_encode_args_t obj_args = {
        .object_id = object_id,
        .prev_object_id = entry->prev_object_id,
        .has_prev_object = entry->has_prev_object,
        .payload_len = payload_length,
        /* Match *_ex: an extension-enabled subgroup encodes a zero-length
         * extensions field even when the streamed object carries no properties. */
        .has_extensions = entry->has_extensions,
    };

    moq_action_t a;
    memset(&a, 0, sizeof(a));
    a.kind = MOQ_ACTION_SEND_DATA;
    a.detail_size = (uint32_t)sizeof(moq_send_data_action_t);
    a.borrow_epoch = s->borrow_epoch;

    moq_buf_writer_t hw;
    moq_buf_writer_init(&hw, a.u.send_data.header, 32);
    moq_result_t rc = s->profile->encode_object_header(s, &hw, &obj_args);
    if (rc < 0) return rc;
    a.u.send_data.header_len = (uint8_t)moq_buf_writer_offset(&hw);

    a.u.send_data.stream_ref = entry->stream_ref;
    a.u.send_data.payload = NULL;
    a.u.send_data.fin = false;

    uint64_t deadline = sg_streaming_header_deadline(s, entry, payload_length, now_us);
    rc = push_action_ex(s, &a, NULL, deadline);
    if (rc < 0) return rc;

    entry->prev_object_id = object_id;
    entry->has_prev_object = true;
    sg_note_object_published(s, entry, object_id);
    entry->state = MOQ_SG_STREAMING;
    entry->streaming_payload_len = payload_length;
    entry->streaming_bytes_written = 0;
    entry->streaming_deadline_us = deadline;
    return MOQ_OK;
}

void moq_begin_object_cfg_init(moq_begin_object_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = sizeof(*cfg);
}

moq_result_t moq_session_begin_object_ex(
    moq_session_t *s, moq_subgroup_handle_t subgroup,
    const moq_begin_object_cfg_t *cfg, uint64_t now_us)
{
    if (!s || !cfg) return MOQ_ERR_INVAL;
    if (cfg->struct_size < sizeof(moq_begin_object_cfg_t)) return MOQ_ERR_INVAL;

    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;

    int slot = sg_resolve_handle(s, subgroup);
    if (slot == -3) return MOQ_ERR_WOULD_BLOCK;
    if (slot < 0) {
        if (sg_resolve_handle_ex(s, subgroup, MOQ_SG_STREAMING) >= 0)
            return MOQ_ERR_WRONG_STATE;
        return MOQ_ERR_STALE_HANDLE;
    }

    moq_sg_entry_t *entry = &s->subgroups[slot];
    if (!sg_send_allowed(s, entry)) return MOQ_ERR_WRONG_STATE;

    if (entry->has_prev_object && cfg->object_id <= entry->prev_object_id)
        return MOQ_ERR_INVAL;

    if (cfg->payload_length > s->profile->object_payload_len_max)
        return MOQ_ERR_INVAL;

    size_t props_len = cfg->properties ? moq_rcbuf_len(cfg->properties) : 0;
    if (props_len > 0 && !entry->has_extensions) return MOQ_ERR_INVAL;
    if (props_len > 0 && s->profile->validate_object_properties &&
        s->profile->validate_object_properties(
            s, moq_rcbuf_data(cfg->properties), props_len) < 0)
        return MOQ_ERR_INVAL;

    bool has_props = (props_len > 0);
    size_t slots_needed = has_props ? 2 : 1;
    if (action_queue_avail(s) < slots_needed) return MOQ_ERR_WOULD_BLOCK;

    moq_sg_entry_t probe = *entry;
    if (has_props)
        sg_read_first_object_override(s, &probe, moq_rcbuf_data(cfg->properties), props_len);
    uint64_t deadline = sg_streaming_header_deadline(s, &probe, cfg->payload_length, now_us);

    if (has_props) {
        /* Two-action: header(delta+extensions_len)+properties, then payload_len. */
        moq_object_header_encode_args_t hdr_args = {
            .object_id = cfg->object_id,
            .prev_object_id = entry->prev_object_id,
            .has_prev_object = entry->has_prev_object,
            .has_extensions = true,
            .properties_len = props_len,
            .header_only = true,
        };

        moq_action_t a1;
        memset(&a1, 0, sizeof(a1));
        a1.kind = MOQ_ACTION_SEND_DATA;
        a1.detail_size = (uint32_t)sizeof(moq_send_data_action_t);
        a1.borrow_epoch = s->borrow_epoch;

        moq_buf_writer_t hw1;
        moq_buf_writer_init(&hw1, a1.u.send_data.header, 32);
        moq_result_t rc = s->profile->encode_object_header(s, &hw1, &hdr_args);
        if (rc < 0) return rc;
        a1.u.send_data.header_len = (uint8_t)moq_buf_writer_offset(&hw1);
        a1.u.send_data.stream_ref = entry->stream_ref;
        a1.u.send_data.payload = cfg->properties;
        a1.u.send_data.fin = false;

        moq_action_t a2;
        memset(&a2, 0, sizeof(a2));
        a2.kind = MOQ_ACTION_SEND_DATA;
        a2.detail_size = (uint32_t)sizeof(moq_send_data_action_t);
        a2.borrow_epoch = s->borrow_epoch;

        moq_buf_writer_t hw2;
        moq_buf_writer_init(&hw2, a2.u.send_data.header, 32);
        rc = s->profile->encode_object_payload_prefix(s, &hw2,
                                                      cfg->payload_length, true);
        if (rc < 0) return rc;
        a2.u.send_data.header_len = (uint8_t)moq_buf_writer_offset(&hw2);
        a2.u.send_data.stream_ref = entry->stream_ref;
        a2.u.send_data.payload = NULL;
        a2.u.send_data.fin = false;

        moq_rcbuf_incref(cfg->properties);

        rc = push_action_ex(s, &a1, NULL, deadline);
        if (rc < 0) {
            moq_rcbuf_decref(cfg->properties);
            return rc;
        }
        rc = push_action_ex(s, &a2, NULL, deadline);
        if (rc < 0) return rc;
    } else {
        moq_object_header_encode_args_t obj_args = {
            .object_id = cfg->object_id,
            .prev_object_id = entry->prev_object_id,
            .has_prev_object = entry->has_prev_object,
            .payload_len = cfg->payload_length,
            .has_extensions = entry->has_extensions,
        };

        moq_action_t a;
        memset(&a, 0, sizeof(a));
        a.kind = MOQ_ACTION_SEND_DATA;
        a.detail_size = (uint32_t)sizeof(moq_send_data_action_t);
        a.borrow_epoch = s->borrow_epoch;

        moq_buf_writer_t hw;
        moq_buf_writer_init(&hw, a.u.send_data.header, 32);
        moq_result_t rc = s->profile->encode_object_header(s, &hw, &obj_args);
        if (rc < 0) return rc;
        a.u.send_data.header_len = (uint8_t)moq_buf_writer_offset(&hw);
        a.u.send_data.stream_ref = entry->stream_ref;
        a.u.send_data.payload = NULL;
        a.u.send_data.fin = false;

        rc = push_action_ex(s, &a, NULL, deadline);
        if (rc < 0) return rc;
    }

    sg_commit_override(entry, &probe);
    entry->prev_object_id = cfg->object_id;
    entry->has_prev_object = true;
    sg_note_object_published(s, entry, cfg->object_id);
    entry->state = MOQ_SG_STREAMING;
    entry->streaming_payload_len = cfg->payload_length;
    entry->streaming_bytes_written = 0;
    entry->streaming_deadline_us = deadline;
    return MOQ_OK;
}

moq_result_t moq_session_write_object_data(
    moq_session_t *s, moq_subgroup_handle_t subgroup,
    moq_rcbuf_t *data, uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;

    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;

    int slot = sg_resolve_handle_ex(s, subgroup, MOQ_SG_STREAMING);
    if (slot == -3) return MOQ_ERR_WOULD_BLOCK;
    if (slot == -2) return MOQ_ERR_WRONG_STATE;
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;

    if (!data) return MOQ_ERR_INVAL;

    size_t data_len = moq_rcbuf_len(data);
    if (data_len == 0) return MOQ_OK;

    moq_sg_entry_t *entry = &s->subgroups[slot];
    if (!sg_send_allowed(s, entry)) return MOQ_ERR_WRONG_STATE;

    if (data_len > entry->streaming_payload_len -
        entry->streaming_bytes_written)
        return MOQ_ERR_INVAL;

    if (action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;

    moq_action_t a;
    memset(&a, 0, sizeof(a));
    a.kind = MOQ_ACTION_SEND_DATA;
    a.detail_size = (uint32_t)sizeof(moq_send_data_action_t);
    a.borrow_epoch = s->borrow_epoch;
    a.u.send_data.stream_ref = entry->stream_ref;
    a.u.send_data.header_len = 0;
    a.u.send_data.payload = data;
    a.u.send_data.fin = false;

    moq_rcbuf_incref(data);

    uint64_t deadline = entry->streaming_deadline_us;
    if (deadline == UINT64_MAX && entry->streaming_bytes_written == 0)
        deadline = sg_object_deadline_us(s, entry, now_us);

    moq_result_t rc = push_action_ex(s, &a, NULL, deadline);
    if (rc < 0) {
        moq_rcbuf_decref(data);
        return rc;
    }

    entry->streaming_deadline_us = deadline;
    entry->streaming_bytes_written += data_len;
    return MOQ_OK;
}

moq_result_t moq_session_end_object(
    moq_session_t *s, moq_subgroup_handle_t subgroup,
    uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;

    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;

    int slot = sg_resolve_handle_ex(s, subgroup, MOQ_SG_STREAMING);
    if (slot == -3) return MOQ_ERR_WOULD_BLOCK;
    if (slot == -2) return MOQ_ERR_WRONG_STATE;
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;

    moq_sg_entry_t *entry = &s->subgroups[slot];

    if (entry->streaming_bytes_written != entry->streaming_payload_len)
        return MOQ_ERR_INVAL;
    /* The stream is owed the cancellation's reset; it is not returned to
     * service. */
    if (sg_owner_cancelled(s, entry)) return MOQ_ERR_WRONG_STATE;

    entry->state = MOQ_SG_OPEN;
    entry->streaming_payload_len = 0;
    entry->streaming_bytes_written = 0;
    entry->streaming_deadline_us = UINT64_MAX;
    return MOQ_OK;
}

/* -- Datagram inbound handler -------------------------------------- */

static moq_result_t datagram_fan_out(moq_session_t *s, uint64_t alias,
    const moq_decoded_object_datagram_t *d,
    const uint8_t *payload_data, size_t payload_len,
    const uint8_t *props_data, size_t props_len)
{
    moq_alias_iter_t it;
    moq_subscription_t sub;
    moq_publication_t pub;
    size_t n = 0;
    moq_track_hist_t *h = NULL;
    alias_iter_begin(s, &it, alias);
    while (alias_iter_next(s, &it, &sub, &pub)) {
        if (!h) h = alias_owner_hist(s, sub, pub);
        if (alias_owner_accepts_object(s, sub, pub, d->group_id, d->object_id)) n++;
    }
    bool pend = h && h->pending_subs > 0;
    size_t held = 0;
    if (pend)
        for (int32_t i = s->sub_occ_head; i >= 0; i = s->subs[i].occ_next)
            if (sub_pending_for_track(&s->subs[i], h) &&
                sub_pending_accepts(s, &s->subs[i], d->group_id, d->object_id))
                held++;
    if (n == 0 && held == 0) return MOQ_OK;
    /* Every copy holds its own budget share, as the poll releases one per
     * event. A datagram has no stream to park on: it is queued to all
     * matching subscriptions at once, or refused like any datagram that
     * finds no room. */
    size_t obj_budget = payload_len + props_len;
    if (event_queue_avail(s) < n) return MOQ_ERR_WOULD_BLOCK;
    if (obj_budget != 0 &&
        (n > SIZE_MAX / obj_budget ||
         s->recv_payload_bytes > s->max_recv_buf ||
         n * obj_budget > s->max_recv_buf - s->recv_payload_bytes))
        return MOQ_ERR_WOULD_BLOCK;

    moq_rcbuf_t *pay_rc = NULL;
    moq_rcbuf_t *prop_rc = NULL;
    if (payload_len > 0) {
        moq_result_t rc = moq_rcbuf_create(&s->alloc, payload_data, payload_len,
                                           &pay_rc);
        if (rc < 0) return rc;
    }
    if (props_len > 0) {
        moq_result_t rc = moq_rcbuf_create(&s->alloc, props_data, props_len,
                                           &prop_rc);
        if (rc < 0) {
            if (pay_rc) moq_rcbuf_decref(pay_rc);
            return rc;
        }
    }
    /* Held copies for subscriptions to the track still awaiting their
     * answer (draft-22 §3.1.3); the ring takes its own references. */
    if (held) {
        moq_held_object_t ho = {
            .group_id = d->group_id, .subgroup_id = 0, .object_id = d->object_id,
            .publisher_priority = d->publisher_priority, .status = (uint8_t)d->status,
            .end_of_group = d->end_of_group, .datagram = true,
            .default_priority = d->default_priority,
            .payload = pay_rc, .properties = prop_rc,
        };
        for (int32_t i = s->sub_occ_head; i >= 0; i = s->subs[i].occ_next)
            if (sub_pending_for_track(&s->subs[i], h) &&
                sub_pending_accepts(s, &s->subs[i], d->group_id, d->object_id))
                session_stage_held_object(s, s->subs[i].handle, alias, &ho);
    }
    alias_iter_begin(s, &it, alias);
    while (alias_iter_next(s, &it, &sub, &pub)) {
        if (!alias_owner_accepts_object(s, sub, pub, d->group_id, d->object_id)) continue;
        moq_event_t e;
        memset(&e, 0, sizeof(e));
        e.kind = MOQ_EVENT_OBJECT_RECEIVED;
        e.detail_size = (uint32_t)sizeof(moq_object_received_event_t);
        e.borrow_epoch = s->borrow_epoch;
        moq_object_received_event_t *obj = &e.u.object_received;
        obj->sub                = sub;
        obj->pub                = pub;
        obj->group_id           = d->group_id;
        obj->object_id          = d->object_id;
        obj->publisher_priority = session_resolve_publisher_priority(s, sub, pub,
            d->default_priority, d->publisher_priority);
        obj->status             = d->status;
        obj->end_of_group       = d->end_of_group;
        obj->datagram           = true;
        obj->payload    = pay_rc ? moq_rcbuf_incref(pay_rc) : NULL;
        obj->properties = prop_rc ? moq_rcbuf_incref(prop_rc) : NULL;
        (void)push_event(s, &e);   /* room reserved above */
        s->recv_payload_bytes += obj_budget;
    }
    if (pay_rc) moq_rcbuf_decref(pay_rc);
    if (prop_rc) moq_rcbuf_decref(prop_rc);
    return MOQ_OK;
}

moq_result_t session_core_on_object_datagram(moq_session_t *s,
    const moq_decoded_object_datagram_t *d,
    const uint8_t *payload_data, size_t payload_len,
    const uint8_t *props_data, size_t props_len)
{
    /* A verdict the decoder already reached (a property block that cannot be
     * parsed, a repeated non-repeatable property) is the Track's, whatever
     * becomes of this datagram: it is latched before the unreliable
     * admission below may drop the datagram, so a datagram dropped for
     * receive limits still fails its Track. */
    if (s->profile->malformed_track_local && d->malformed != MOQ_MALFORMED_NONE &&
        (d->sub_slot >= 0 || d->pub_slot >= 0)) {
        session_fail_malformed_alias(s, d->sub_slot >= 0
                                            ? s->subs[d->sub_slot].track_alias
                                            : s->publishes[d->pub_slot].track_alias);
        return s->state == MOQ_SESS_CLOSED ? MOQ_ERR_CLOSED : MOQ_OK;
    }

    /* Apply the same receive limits as the stream/fetch object paths. A datagram
     * is a self-contained, unreliable unit, so an over-limit one is dropped
     * (no event, session stays established) rather than closing the session.
     * All arithmetic is overflow-safe against attacker-controlled lengths. */
    if (payload_len > s->max_obj_payload)
        return MOQ_ERR_WOULD_BLOCK;
    if (props_len > SIZE_MAX - payload_len)
        return MOQ_ERR_WOULD_BLOCK;
    size_t obj_budget = payload_len + props_len;
    if (s->recv_payload_bytes > s->max_recv_buf ||
        obj_budget > s->max_recv_buf - s->recv_payload_bytes)
        return MOQ_ERR_WOULD_BLOCK;

    /* §12.1: a datagram that makes its Track malformed fails the requests
     * for that Track, not the session, and is not delivered. The verdict is
     * latched on those requests before anything is retired, so a
     * cancellation that cannot be queued now completes on a later call even
     * if the peer sends nothing more; the datagram itself is dropped. A
     * datagram for a Track already failed is dropped as well. */
    if (s->profile->malformed_track_local && (d->sub_slot >= 0 || d->pub_slot >= 0)) {
        uint64_t alias = d->sub_slot >= 0 ? s->subs[d->sub_slot].track_alias
                                          : s->publishes[d->pub_slot].track_alias;
        uint8_t why = d->malformed;
        if (why == MOQ_MALFORMED_NONE && session_alias_failing(s, alias))
            why = MOQ_MALFORMED_TRACK_FAILED;
        if (why == MOQ_MALFORMED_NONE) {
            moq_result_t trc;
            moq_mt_track_t *t = mt_track_for_alias(s, alias, &trc);
            if (trc < 0) return trc;
            moq_mt_object_t mo = {
                .group_id = d->group_id,
                .object_id = d->object_id,
                .end_of_group = d->end_of_group,
                .status = d->status,
                .gaps = &d->gaps,
            };
            why = mt_object(t, &mo);
        }
        if (why != MOQ_MALFORMED_NONE) {
            session_fail_malformed_alias(s, alias);
            return s->state == MOQ_SESS_CLOSED ? MOQ_ERR_CLOSED : MOQ_OK;
        }
    }

    /* An alias several subscriptions share, or that a pending subscription
     * to its track may share (draft-22 §3.1, §3.1.3): the datagram is
     * surfaced once to every subscription whose filter -- Forward included --
     * accepts it, all or none, held for a pending one, and discarded when
     * none does. */
    if (d->sub_slot >= 0 || d->pub_slot >= 0) {
        uint64_t alias = d->sub_slot >= 0 ? s->subs[d->sub_slot].track_alias
                                          : s->publishes[d->pub_slot].track_alias;
        if (alias_attribution_shared(s, alias)) {
            moq_track_hist_t *th = d->sub_slot >= 0 ? s->subs[d->sub_slot].hist
                                                    : s->publishes[d->pub_slot].hist;
            track_hist_merge(th, d->group_id, d->object_id);
            return datagram_fan_out(s, alias, d, payload_data, payload_len,
                                    props_data, props_len);
        }
    }

    /* Feed received-object history at ADMISSION -- once the object passed the
     * receive-limit checks above, before delivery capacity.
     * Idempotent (monotonic max), so a WOULD_BLOCK retry re-merges harmlessly. */
    if (d->sub_slot >= 0)
        track_hist_merge(s->subs[d->sub_slot].hist, d->group_id, d->object_id);
    else if (d->pub_slot >= 0)
        track_hist_merge(s->publishes[d->pub_slot].hist,
                         d->group_id, d->object_id);

    /* Forward State 0 prohibits Objects: a paused binding -- subscriber-role
     * subscription OR publication -- drops prohibited datagram objects HERE,
     * after the history merge (Largest advances on receipt) and after alias
     * resolution (so a paused publication's datagram is never mistaken for
     * unknown-alias data and staged). A datagram is single-shot, so the
     * decision is made once at admission -- no chunk sequence to freeze. */
    if (d->sub_slot >= 0 &&
        s->subs[d->sub_slot].role == MOQ_SUB_ROLE_SUBSCRIBER &&
        !s->subs[d->sub_slot].forward)
        return MOQ_OK;
    if (d->pub_slot >= 0 && !s->publishes[d->pub_slot].send_allowed)
        return MOQ_OK;


    if (event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;

    moq_rcbuf_t *pay_rc = NULL;
    moq_rcbuf_t *prop_rc = NULL;

    if (payload_len > 0) {
        moq_result_t rc = moq_rcbuf_create(&s->alloc,
            payload_data, payload_len, &pay_rc);
        if (rc < 0) return rc;
    }
    if (props_len > 0) {
        moq_result_t rc = moq_rcbuf_create(&s->alloc,
            props_data, props_len, &prop_rc);
        if (rc < 0) {
            if (pay_rc) moq_rcbuf_decref(pay_rc);
            return rc;
        }
    }

    moq_event_t e;
    memset(&e, 0, sizeof(e));
    e.kind = MOQ_EVENT_OBJECT_RECEIVED;
    e.detail_size = (uint32_t)sizeof(moq_object_received_event_t);
    e.borrow_epoch = s->borrow_epoch;

    moq_object_received_event_t *obj = &e.u.object_received;

    if (d->sub_slot >= 0)
        obj->sub = s->subs[d->sub_slot].handle;
    if (d->pub_slot >= 0)
        obj->pub = s->publishes[d->pub_slot].handle;

    obj->group_id           = d->group_id;
    obj->subgroup_id        = 0;
    obj->object_id          = d->object_id;
    obj->publisher_priority = session_resolve_publisher_priority(s, obj->sub, obj->pub,
        d->default_priority, d->publisher_priority);
    obj->status             = d->status;
    obj->end_of_group       = d->end_of_group;
    obj->datagram           = true;
    obj->payload            = pay_rc;
    obj->properties         = prop_rc;

    moq_result_t rc = push_event(s, &e);
    if (rc < 0) {
        if (pay_rc) moq_rcbuf_decref(pay_rc);
        if (prop_rc) moq_rcbuf_decref(prop_rc);
        return rc;
    }

    s->recv_payload_bytes += obj_budget;
    return MOQ_OK;
}

/* -- Datagram outbound send ---------------------------------------- */

/* The smaller non-zero of two timeouts (0 = none). */
static uint64_t dt_min_nonzero_us(uint64_t a, uint64_t b)
{
    if (a == 0) return b;
    if (b == 0) return a;
    return a < b ? a : b;
}

/* A datagram's scheduling record and OBJECT_DELIVERY_TIMEOUT expiry: the
 * owner's subscriber priority and group order, and the smaller of the owner's
 * two negotiated timeouts (SUBGROUP_DELIVERY_TIMEOUT acts as
 * OBJECT_DELIVERY_TIMEOUT for datagrams). */
static void datagram_meta(moq_session_t *s, int sub_slot, int pub_slot,
                          const moq_datagram_encode_args_t *args,
                          moq_action_meta_t *m, uint64_t *deadline)
{
    moq_dt_scan_t pub, sub;
    memset(&pub, 0, sizeof(pub)); memset(&sub, 0, sizeof(sub));
    memset(m, 0, sizeof(*m));
    m->deadline_us = UINT64_MAX;
    m->mode = MOQ_SCHED_MODE_DATAGRAM;
    m->group_order = MOQ_GROUP_ORDER_ASCENDING;
    m->subscriber_priority = 128;
    if (sub_slot >= 0) {
        const moq_sub_entry_t *se = &s->subs[sub_slot];
        m->owner = se->handle._opaque;
        m->subscriber_priority = se->subscriber_priority;
        m->group_order = session_group_order(se->group_order, se->track_group_order);
        pub.has_object = se->dt_pub_has_object;     pub.object_ms = se->dt_pub_object_ms;
        pub.has_subgroup = se->dt_pub_has_subgroup; pub.subgroup_ms = se->dt_pub_subgroup_ms;
        sub.has_object = se->dt_sub_has_object;     sub.object_ms = se->dt_sub_object_ms;
        sub.has_subgroup = se->dt_sub_has_subgroup; sub.subgroup_ms = se->dt_sub_subgroup_ms;
    } else if (pub_slot >= 0) {
        const moq_pub_entry_t *pe = &s->publishes[pub_slot];
        m->owner = pe->handle._opaque;
        m->subscriber_priority = pe->subscriber_priority;
        m->group_order = session_group_order(pe->group_order, pe->track_group_order);
        pub.has_object = pe->dt_pub_has_object;     pub.object_ms = pe->dt_pub_object_ms;
        pub.has_subgroup = pe->dt_pub_has_subgroup; pub.subgroup_ms = pe->dt_pub_subgroup_ms;
        sub.has_object = pe->dt_sub_has_object;     sub.object_ms = pe->dt_sub_object_ms;
        sub.has_subgroup = pe->dt_sub_has_subgroup; sub.subgroup_ms = pe->dt_sub_subgroup_ms;
    }
    m->publisher_priority = session_resolve_publisher_priority(s,
        sub_slot >= 0 ? s->subs[sub_slot].handle : MOQ_SUBSCRIPTION_INVALID,
        pub_slot >= 0 ? s->publishes[pub_slot].handle : MOQ_PUBLICATION_INVALID,
        args->default_priority, args->publisher_priority);
    m->group_id = args->group_id;
    m->object_id = args->object_id;
    m->has_location = true;
    uint64_t o = ms_to_us_sat(dt_negotiate_ms(pub.has_object, pub.object_ms,
                                              sub.has_object, sub.object_ms));
    uint64_t g = ms_to_us_sat(dt_negotiate_ms(pub.has_subgroup, pub.subgroup_ms,
                                              sub.has_subgroup, sub.subgroup_ms));
    uint64_t to = dt_min_nonzero_us(o, g);
    *deadline = to ? deadline_add(s->last_now_us, to) : UINT64_MAX;
}

static moq_result_t send_datagram_encoded(moq_session_t *s,
    const moq_datagram_encode_args_t *args, int sub_slot, int pub_slot)
{
    if (action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;

    moq_buf_writer_t w;
    moq_buf_writer_init(&w, s->send_buf + s->send_len,
                         s->send_cap - s->send_len);

    moq_result_t rc = s->profile->encode_object_datagram(s, &w, args);
    if (rc < 0) return rc;

    size_t elen = moq_buf_writer_offset(&w);
    moq_action_t a;
    memset(&a, 0, sizeof(a));
    a.kind = MOQ_ACTION_SEND_DATAGRAM;
    a.detail_size = (uint32_t)sizeof(moq_send_datagram_action_t);
    a.borrow_epoch = s->borrow_epoch;
    a.u.send_datagram.data = s->send_buf + s->send_len;
    a.u.send_datagram.len  = elen;

    moq_action_meta_t meta; uint64_t deadline;
    datagram_meta(s, sub_slot, pub_slot, args, &meta, &deadline);
    rc = push_action_ex(s, &a, &meta, deadline);
    if (rc < 0) return rc;

    s->send_len += elen;
    return MOQ_OK;
}

moq_result_t moq_session_send_object_datagram(
    moq_session_t *s,
    moq_subscription_t sub,
    uint64_t group_id, uint64_t object_id,
    uint8_t publisher_priority,
    bool end_of_group,
    moq_rcbuf_t *payload,
    const uint8_t *properties, size_t properties_len,
    uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;

    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_WRONG_STATE;

    int sub_slot = sub_resolve_handle(s, sub);
    if (sub_slot < 0) return MOQ_ERR_STALE_HANDLE;
    if (s->subs[sub_slot].state != MOQ_SUB_ESTABLISHED)
        return MOQ_ERR_WRONG_STATE;
    if (s->subs[sub_slot].role != MOQ_SUB_ROLE_PUBLISHER)
        return MOQ_ERR_WRONG_STATE;
    if (s->subs[sub_slot].cancel_pending) return MOQ_ERR_WRONG_STATE;
    if (!s->subs[sub_slot].forward) return MOQ_ERR_WRONG_STATE;

    size_t plen = payload ? moq_rcbuf_len(payload) : 0;
    const uint8_t *pdata = payload ? moq_rcbuf_data(payload) : NULL;

    moq_datagram_encode_args_t args;
    memset(&args, 0, sizeof(args));
    args.track_alias        = s->subs[sub_slot].track_alias;
    args.group_id           = group_id;
    args.object_id          = object_id;
    args.publisher_priority = publisher_priority;
    args.end_of_group       = end_of_group;
    args.properties         = properties;
    args.properties_len     = properties_len;
    args.payload            = pdata;
    args.payload_len        = plen;

    moq_result_t drc = send_datagram_encoded(s, &args, sub_slot, -1);
    if (drc >= 0)
        track_hist_merge(s->subs[sub_slot].hist, group_id, object_id);
    return drc;
}

moq_result_t moq_session_send_status_datagram(
    moq_session_t *s,
    moq_subscription_t sub,
    uint64_t group_id, uint64_t object_id,
    uint8_t publisher_priority,
    moq_object_status_t status,
    uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;

    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_WRONG_STATE;

    int sub_slot = sub_resolve_handle(s, sub);
    if (sub_slot < 0) return MOQ_ERR_STALE_HANDLE;
    if (s->subs[sub_slot].state != MOQ_SUB_ESTABLISHED)
        return MOQ_ERR_WRONG_STATE;
    if (s->subs[sub_slot].role != MOQ_SUB_ROLE_PUBLISHER)
        return MOQ_ERR_WRONG_STATE;
    if (s->subs[sub_slot].cancel_pending) return MOQ_ERR_WRONG_STATE;
    if (!s->subs[sub_slot].forward) return MOQ_ERR_WRONG_STATE;

    uint64_t wire_status;
    if (status == MOQ_OBJECT_NORMAL) wire_status = 0x0;
    else if (status == MOQ_OBJECT_END_OF_GROUP) wire_status = 0x3;
    else if (status == MOQ_OBJECT_END_OF_TRACK) wire_status = 0x4;
    else return MOQ_ERR_INVAL;

    moq_datagram_encode_args_t args;
    memset(&args, 0, sizeof(args));
    args.track_alias        = s->subs[sub_slot].track_alias;
    args.group_id           = group_id;
    args.object_id          = object_id;
    args.publisher_priority = publisher_priority;
    args.is_status          = true;
    args.object_status      = wire_status;

    moq_result_t drc = send_datagram_encoded(s, &args, sub_slot, -1);
    if (drc >= 0)
        track_hist_merge(s->subs[sub_slot].hist, group_id, object_id);
    return drc;
}

moq_result_t moq_session_send_pub_object_datagram(
    moq_session_t *s,
    moq_publication_t pub,
    uint64_t group_id, uint64_t object_id,
    uint8_t publisher_priority,
    bool end_of_group,
    moq_rcbuf_t *payload,
    const uint8_t *properties, size_t properties_len,
    uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;

    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_WRONG_STATE;

    int slot = pub_resolve_handle(s, pub);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;
    if (s->publishes[slot].state != MOQ_PUB_ESTABLISHED)
        return MOQ_ERR_WRONG_STATE;
    if (s->publishes[slot].role != MOQ_PUB_ROLE_PUBLISHER)
        return MOQ_ERR_WRONG_STATE;
    if (pub_control_reject_teardown(s, &s->publishes[slot])) return MOQ_ERR_WRONG_STATE;
    if (!s->publishes[slot].send_allowed)
        return MOQ_ERR_WRONG_STATE;

    size_t plen = payload ? moq_rcbuf_len(payload) : 0;
    const uint8_t *pdata = payload ? moq_rcbuf_data(payload) : NULL;

    moq_datagram_encode_args_t args;
    memset(&args, 0, sizeof(args));
    args.track_alias        = s->publishes[slot].track_alias;
    args.group_id           = group_id;
    args.object_id          = object_id;
    args.publisher_priority = publisher_priority;
    args.end_of_group       = end_of_group;
    args.properties         = properties;
    args.properties_len     = properties_len;
    args.payload            = pdata;
    args.payload_len        = plen;

    moq_result_t drc = send_datagram_encoded(s, &args, -1, slot);
    if (drc >= 0)
        track_hist_merge(s->publishes[slot].hist, group_id, object_id);
    return drc;
}

moq_result_t moq_session_send_pub_status_datagram(
    moq_session_t *s,
    moq_publication_t pub,
    uint64_t group_id, uint64_t object_id,
    uint8_t publisher_priority,
    moq_object_status_t status,
    uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;

    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_WRONG_STATE;

    int slot = pub_resolve_handle(s, pub);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;
    if (s->publishes[slot].state != MOQ_PUB_ESTABLISHED)
        return MOQ_ERR_WRONG_STATE;
    if (s->publishes[slot].role != MOQ_PUB_ROLE_PUBLISHER)
        return MOQ_ERR_WRONG_STATE;
    if (pub_control_reject_teardown(s, &s->publishes[slot])) return MOQ_ERR_WRONG_STATE;
    if (!s->publishes[slot].send_allowed)
        return MOQ_ERR_WRONG_STATE;

    uint64_t wire_status;
    if (status == MOQ_OBJECT_NORMAL) wire_status = 0x0;
    else if (status == MOQ_OBJECT_END_OF_GROUP) wire_status = 0x3;
    else if (status == MOQ_OBJECT_END_OF_TRACK) wire_status = 0x4;
    else return MOQ_ERR_INVAL;

    moq_datagram_encode_args_t args;
    memset(&args, 0, sizeof(args));
    args.track_alias        = s->publishes[slot].track_alias;
    args.group_id           = group_id;
    args.object_id          = object_id;
    args.publisher_priority = publisher_priority;
    args.is_status          = true;
    args.object_status      = wire_status;

    moq_result_t drc = send_datagram_encoded(s, &args, -1, slot);
    if (drc >= 0)
        track_hist_merge(s->publishes[slot].hist, group_id, object_id);
    return drc;
}

uint8_t bridge_session_subgroup_header_size(
    const moq_session_t *s, const moq_send_data_action_t *data)
{
    return s->profile->classify_data_stream(data->header, data->header_len) ==
        MOQ_STREAM_KIND_SUBGROUP ? data->header_len : 0;
}

bool bridge_session_has_queued_data_reset(
    const moq_session_t *s, moq_stream_ref_t ref)
{
    for (size_t i = s->action_head; i < s->action_tail; ++i) {
        const moq_action_t *a = &s->actions[i % s->action_cap];
        if (a->kind == MOQ_ACTION_RESET_DATA &&
            a->u.reset_data.stream_ref._v == ref._v)
            return true;
    }
    return false;
}

bool bridge_session_has_data_terminal(const moq_session_t *s, moq_stream_ref_t ref)
{
    for (int32_t i = s->sg_occ_head; i >= 0; i = s->subgroups[i].occ_next)
        if (s->subgroups[i].stream_ref._v == ref._v &&
            s->subgroups[i].terminal_cause != MOQ_OUTGOING_SUBGROUP_TERMINAL_NONE)
            return true;
    for (size_t i = s->action_head; i < s->action_tail; i++)
        if (s->action_meta[i % s->action_cap].outgoing_terminal &&
            s->actions[i % s->action_cap].kind == MOQ_ACTION_RESET_DATA &&
            s->actions[i % s->action_cap].u.reset_data.stream_ref._v == ref._v)
            return true;
    return false;
}

bool bridge_session_is_outgoing_subgroup(const moq_session_t *s, moq_stream_ref_t ref)
{
    for (int32_t i = s->sg_occ_head; i >= 0; i = s->subgroups[i].occ_next)
        if (s->subgroups[i].stream_ref._v == ref._v) return true;
    return false;
}

static moq_result_t session_on_data_stop(moq_session_t *s,
                                         moq_stream_ref_t stream_ref,
                                         uint64_t error_code,
                                         uint64_t now_us,
                                         uint8_t retained_header_len)
{
    if (!s) return MOQ_ERR_INVAL;
    if (error_code > MOQ_QUIC_VARINT_MAX) return MOQ_ERR_INVAL;
    if (moq_binding_required_unbound(s)) return MOQ_ERR_WRONG_STATE;
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;
    int stopped_slot = sg_find_by_stream_ref(s, stream_ref);
    if (stopped_slot >= 0 &&
        (s->subgroups[stopped_slot].state != MOQ_SG_CLOSING ||
         s->subgroups[stopped_slot].delivery_deadline_us != UINT64_MAX))
        sg_latch_terminal(s, (size_t)stopped_slot,
            MOQ_OUTGOING_SUBGROUP_TERMINAL_PEER_STOP_SENDING, error_code,
            sg_first_output(s, stream_ref, false));
    moq_result_t arc = session_advance_entry(s, now_us);
    if (arc != MOQ_OK) return arc;
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;

    int slot = sg_find_by_stream_ref(s, stream_ref);
    if (slot < 0 && retained_header_len == 0) {
        /* A fill stream answers the peer's STOP with its RESET and ends the
         * child; a stream nobody owns is nothing to act on. */
        moq_result_t frc = fill_on_data_stop(s, stream_ref, error_code);
        return frc == MOQ_ERR_STALE_HANDLE ? MOQ_OK : frc;
    }
    if (slot >= 0) {
        sg_latch_terminal(s, (size_t)slot,
            MOQ_OUTGOING_SUBGROUP_TERMINAL_PEER_STOP_SENDING, error_code,
            sg_first_output(s, stream_ref, false));
        sg_retry_owed_resets(s);
        sg_recompute_deadline(s);
        return s->subgroups[slot].state == MOQ_SG_TERMINAL_OWED
            ? MOQ_ERR_WOULD_BLOCK : MOQ_OK;
    } else {
        /* Only a bridge-retained subgroup FIN can reach this fallback. A
         * timeout/local reset may already own an action even though the slot
         * was reaped: preserve that first reset and its exact code. */
        if (bridge_session_has_queued_data_reset(s, stream_ref)) return MOQ_OK;
    }

    if (action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;

    moq_action_t a;
    memset(&a, 0, sizeof(a));
    a.kind = MOQ_ACTION_RESET_DATA;
    a.detail_size = (uint32_t)sizeof(moq_reset_data_action_t);
    a.borrow_epoch = s->borrow_epoch;
    a.u.reset_data.stream_ref = stream_ref;
    a.u.reset_data.error_code = error_code;
    a.u.reset_data.reliable_size = retained_header_len;

    moq_result_t rc = push_action(s, &a);
    if (rc < 0) return rc;
    return MOQ_OK;
}

moq_result_t moq_session_on_data_stop(moq_session_t *s,
                                       moq_stream_ref_t stream_ref,
                                       uint64_t error_code,
                                       uint64_t now_us)
{
    return session_on_data_stop(s, stream_ref, error_code, now_us, 0);
}

moq_result_t bridge_session_on_subgroup_stop(
    moq_session_t *s, moq_stream_ref_t ref, uint64_t error_code,
    uint64_t now_us, uint8_t retained_header_len)
{
    return session_on_data_stop(s, ref, error_code, now_us, retained_header_len);
}

moq_result_t moq_session_on_data_committed(moq_session_t *s,
                                            moq_stream_ref_t stream_ref,
                                            uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;
    if (moq_binding_required_unbound(s)) return MOQ_ERR_WRONG_STATE;
    if (s->state == MOQ_SESS_CLOSED) return MOQ_ERR_CLOSED;
    int slot = sg_find_by_stream_ref(s, stream_ref);
    if (slot < 0 || s->subgroups[slot].state != MOQ_SG_CLOSING ||
        s->subgroups[slot].delivery_deadline_us == UINT64_MAX)
        return MOQ_OK;
    moq_result_t arc = session_advance_entry(s, now_us);
    if (arc != MOQ_OK) return arc;
    slot = sg_find_by_stream_ref(s, stream_ref);
    if (slot < 0 || s->subgroups[slot].state != MOQ_SG_CLOSING)
        return MOQ_OK;
    s->subgroups[slot].delivery_deadline_us = UINT64_MAX;
    sg_recompute_deadline(s);
    return MOQ_OK;
}
