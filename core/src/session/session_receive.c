#include "session_internal.h"
#include "../bridge/transport_bridge_internal.h"
#include "../base/rcbuf_internal.h"

/* -- Receive data stream pool -------------------------------------- */

static int rx_find_by_ref(moq_session_t *s, moq_stream_ref_t ref)
{
    return moq_index_find(s->idx_rx_by_ref, s->idx_rx_mask, ref._v);
}

static int rx_find_free(moq_session_t *s)
{
    for (size_t i = 0; i < s->rx_cap; i++)
        if (!s->rx_streams[i].active) return (int)i;
    return -1;
}

/* Release everything a receive entry owns besides its identity: the object
 * being assembled, its extensions, the staged input and any parked chunk,
 * with their receive-budget charges. The entry stays allocated and indexed. */
static void rx_release_resources(moq_session_t *s, size_t slot)
{
    moq_rx_stream_t *rx = &s->rx_streams[slot];
    if (rx->payload_rcbuf) {
        /* An object assembled but not yet emitted: the payload lives inside
         * this rcbuf (payload_buf points into it), so decref rather than free
         * the cursor. */
        moq_rcbuf_decref(rx->payload_rcbuf);
        if ((size_t)rx->payload_expected <= s->recv_payload_bytes)
            s->recv_payload_bytes -= (size_t)rx->payload_expected;
        else
            s->recv_payload_bytes = 0;
        rx->payload_rcbuf = NULL;
        rx->payload_buf = NULL;
    }
    if (rx->cur_extensions) {
        size_t ext_len = moq_rcbuf_len(rx->cur_extensions);
        if (ext_len <= s->recv_payload_bytes)
            s->recv_payload_bytes -= ext_len;
        else
            s->recv_payload_bytes = 0;
        moq_rcbuf_decref(rx->cur_extensions);
        rx->cur_extensions = NULL;
    }
    if (rx->input_buf) {
        if (rx->input_cap <= s->recv_input_bytes)
            s->recv_input_bytes -= rx->input_cap;
        else
            s->recv_input_bytes = 0;
        s->alloc.free(rx->input_buf, rx->input_cap, s->alloc.ctx);
        rx->input_buf = NULL;
    }
    /* The entry may stay allocated (a STOPped stream): it holds no input. */
    rx->input_len = 0;
    rx->input_cap = 0;
    if (rx->pending_chunk) {
        moq_rcbuf_decref(rx->pending_chunk);
        rx->pending_chunk = NULL;
    }
}

/* -- Subgroup streams on a shared alias ---------------------------- *
 * Draft-22 §3.1: "Because subscriptions can share a Track Alias, the
 * subscriber re-applies each subscription's filter to determine which
 * subscription a received Object belongs to." A subgroup stream whose alias
 * several established subscriptions share -- SUBSCRIBE- or PUBLISH-
 * initiated, §3.1.3 forbids only "a different Track" -- is carried in shared
 * mode (see moq_rx_stream_t.fan): every object is matched at its admission
 * against each sharing subscription's Forward state and filter, frozen for
 * the object like the Forward decision, and surfaced once to every
 * subscription that accepts it. An object no subscription accepts is
 * discarded.
 *
 * §3.1.3 also: "The Subscriber can assume the Track Alias is reused until
 * told otherwise, in order to avoid missing Objects." While a subscription
 * to the same track awaits its response, the alias is treated as shared
 * with it too: an object its filter would accept is held for it in the
 * session's bounded held-object ring (session_stage_held_object) and
 * surfaced if its SUBSCRIBE_OK names this alias, discarded otherwise.
 *
 * While one subscription holds the alias and none to its track is pending,
 * the stream is bound to it as on every other profile. */

#define MOQ_RX_FAN_MAX (UINT32_C(1) << 20)

/* The track of the subscriptions holding `alias` has a subscription
 * awaiting its response. */
static moq_track_hist_t *rx_alias_track(moq_session_t *s, uint64_t alias)
{
    moq_alias_iter_t it;
    moq_subscription_t sub;
    moq_publication_t pub;
    alias_iter_begin(s, &it, alias);
    if (!alias_iter_next(s, &it, &sub, &pub)) return NULL;
    return alias_owner_hist(s, sub, pub);
}

bool alias_attribution_shared(moq_session_t *s, uint64_t alias)
{
    if (!s->profile->allows_concurrent_subscriptions) return false;
    moq_alias_iter_t it;
    moq_subscription_t sub;
    moq_publication_t pub;
    alias_iter_begin(s, &it, alias);
    if (!alias_iter_next(s, &it, &sub, &pub)) return false;
    moq_track_hist_t *h = alias_owner_hist(s, sub, pub);
    if (h && h->pending_subs > 0) return true;
    return alias_iter_next(s, &it, &sub, &pub);
}

static bool rx_fan_owner_eq(const moq_rx_fan_t *f, moq_subscription_t sub,
                            moq_publication_t pub)
{
    if (moq_subscription_is_valid(sub))
        return moq_subscription_is_valid(f->sub) && moq_subscription_eq(f->sub, sub);
    return moq_publication_is_valid(pub) && moq_publication_is_valid(f->pub) &&
           moq_publication_eq(f->pub, pub);
}

static int rx_fan_find(const moq_rx_stream_t *rx, moq_subscription_t sub,
                       moq_publication_t pub)
{
    for (uint32_t i = 0; i < rx->fan_count; i++)
        if (rx_fan_owner_eq(&rx->fan[i], sub, pub)) return (int)i;
    return -1;
}

static bool rx_fan_alive(moq_session_t *s, const moq_rx_fan_t *f)
{
    if (moq_subscription_is_valid(f->sub)) return sub_resolve_handle(s, f->sub) >= 0;
    return pub_resolve_handle(s, f->pub) >= 0;
}

/* Capacity for `need` entries; nothing changes on failure. Shared mode
 * starts with room for two, so attributing an empty stream to its one
 * candidate never allocates. */
static moq_result_t rx_fan_reserve(moq_session_t *s, moq_rx_stream_t *rx,
                                   uint32_t need)
{
    if (need <= rx->fan_cap) return MOQ_OK;
    /* Bounded far below any size_t overflow on every data model. */
    if (need > MOQ_RX_FAN_MAX) return MOQ_ERR_NOMEM;
    uint32_t cap = rx->fan_cap ? rx->fan_cap : 2;
    while (cap < need) cap *= 2;
    moq_rx_fan_t *nf = rx->fan
        ? (moq_rx_fan_t *)s->alloc.realloc(rx->fan,
              (size_t)rx->fan_cap * sizeof(moq_rx_fan_t),
              (size_t)cap * sizeof(moq_rx_fan_t), s->alloc.ctx)
        : (moq_rx_fan_t *)s->alloc.alloc((size_t)cap * sizeof(moq_rx_fan_t),
                                         s->alloc.ctx);
    if (!nf) return MOQ_ERR_NOMEM;
    rx->fan = nf;
    rx->fan_cap = cap;
    return MOQ_OK;
}

static void rx_fan_free(moq_session_t *s, moq_rx_stream_t *rx)
{
    if (rx->fan)
        s->alloc.free(rx->fan, (size_t)rx->fan_cap * sizeof(moq_rx_fan_t),
                      s->alloc.ctx);
    rx->fan = NULL;
    rx->fan_cap = rx->fan_count = 0;
}

static void rx_fan_append(moq_rx_stream_t *rx, moq_subscription_t sub,
                          moq_publication_t pub, bool pending)
{
    moq_rx_fan_t *f = &rx->fan[rx->fan_count++];
    memset(f, 0, sizeof(*f));
    f->sub = sub;
    f->pub = pub;
    f->pending = pending;
}

static void rx_fan_remove(moq_rx_stream_t *rx, uint32_t i)
{
    for (; i + 1 < rx->fan_count; i++) rx->fan[i] = rx->fan[i + 1];
    rx->fan_count--;
}

/* More than one established subscription has been reached: the stream can
 * no longer be counted toward any one of them (rx_fan_owner). */
static void rx_fan_note_ambiguity(moq_rx_stream_t *rx)
{
    uint32_t n = 0;
    for (uint32_t k = 0; k < rx->fan_count; k++)
        if (!rx->fan[k].pending) n++;
    if (n > 1) rx->fan_ambiguous = true;
}

/* Enter shared mode: the stream is attributed per object from now on. A
 * stream already bound to one subscription keeps it among the ones it
 * delivered to when an object reached it. */
static moq_result_t rx_fan_enter(moq_session_t *s, moq_rx_stream_t *rx)
{
    moq_result_t rc = rx_fan_reserve(s, rx, 2);
    if (rc < 0) return rc;
    if (rx->has_prev_object) {
        if (moq_subscription_is_valid(rx->sub) && sub_resolve_handle(s, rx->sub) >= 0)
            rx_fan_append(rx, rx->sub, MOQ_PUBLICATION_INVALID, false);
        else if (moq_publication_is_valid(rx->pub_handle) &&
                 pub_resolve_handle(s, rx->pub_handle) >= 0)
            rx_fan_append(rx, MOQ_SUBSCRIPTION_INVALID, rx->pub_handle, false);
    }
    rx->sub = MOQ_SUBSCRIPTION_INVALID;
    rx->pub_handle = MOQ_PUBLICATION_INVALID;
    rx->fan_shared = true;
    return MOQ_OK;
}

/* Bind a subgroup stream to the subscription(s) holding its alias: shared
 * mode when the alias is (or may be) shared, the single holder otherwise. */
static moq_result_t rx_bind_alias(moq_session_t *s, moq_rx_stream_t *rx,
                                  int sub_slot, int pub_slot)
{
    if (alias_attribution_shared(s, rx->track_alias)) {
        rx->sub = MOQ_SUBSCRIPTION_INVALID;
        rx->pub_handle = MOQ_PUBLICATION_INVALID;
        return rx_fan_enter(s, rx);
    }
    if (sub_slot >= 0) {
        rx->sub = s->subs[sub_slot].handle;
        rx->pub_handle = MOQ_PUBLICATION_INVALID;
    } else {
        rx->sub = MOQ_SUBSCRIPTION_INVALID;
        rx->pub_handle = s->publishes[pub_slot].handle;
    }
    return MOQ_OK;
}

/* A subscription to `hist`'s track awaiting its response would accept the
 * object at {group, object}: forwarding, and its filter -- resolved against
 * the Largest Object known here when it was sent, no later than the one its
 * publisher resolves it against -- admits the Location. Its actual window
 * is applied again when a held object is surfaced. */
bool sub_pending_accepts(moq_session_t *s, const moq_sub_entry_t *e,
                            uint64_t group, uint64_t object)
{
    if (!e->forward) return false;
    moq_resolved_window_t w;
    moq_resolve_filter_window(e->filter_type, e->req_start_group,
                              e->req_start_object, e->req_end_group,
                              e->req_end_object, e->sent_has_largest,
                              e->sent_largest_group, e->sent_largest_object,
                              s->profile->location_varint_max, &w);
    if (w.unsatisfiable) return false;
    if (group < w.start_group || (group == w.start_group && object < w.start_object))
        return false;
    if (w.has_end && group > w.end_group) return false;
    if (w.has_end_object && group == w.end_group && object > w.end_object)
        return false;
    return true;
}

bool sub_pending_for_track(const moq_sub_entry_t *e, const moq_track_hist_t *h)
{
    return e->state == MOQ_SUB_PENDING_SUBSCRIBER &&
           e->role == MOQ_SUB_ROLE_SUBSCRIBER && e->hist == h;
}

/* Match the object at {group, object} against every subscription sharing
 * the alias and every subscription to its track still pending, freezing the
 * result in each entry's `cur` and arming its event. NOMEM leaves the stream
 * unchanged. */
static moq_result_t rx_fan_match(moq_session_t *s, moq_rx_stream_t *rx,
                                 uint64_t group, uint64_t object)
{
    moq_alias_iter_t it;
    moq_subscription_t sub;
    moq_publication_t pub;
    moq_track_hist_t *h = rx_alias_track(s, rx->track_alias);
    bool pend = h && h->pending_subs > 0;
    uint32_t add = 0;
    alias_iter_begin(s, &it, rx->track_alias);
    while (alias_iter_next(s, &it, &sub, &pub))
        if (alias_owner_accepts_object(s, sub, pub, group, object) &&
            rx_fan_find(rx, sub, pub) < 0)
            add++;
    if (pend) {
        for (int32_t i = s->sub_occ_head; i >= 0; i = s->subs[i].occ_next) {
            const moq_sub_entry_t *e = &s->subs[i];
            if (sub_pending_for_track(e, h) && sub_pending_accepts(s, e, group, object) &&
                rx_fan_find(rx, e->handle, MOQ_PUBLICATION_INVALID) < 0)
                add++;
        }
    }
    if (add > UINT32_MAX - rx->fan_count) return MOQ_ERR_NOMEM;
    moq_result_t rc = rx_fan_reserve(s, rx, rx->fan_count + add);
    if (rc < 0) return rc;
    for (uint32_t k = 0; k < rx->fan_count; k++) rx->fan[k].cur = false;
    alias_iter_begin(s, &it, rx->track_alias);
    while (alias_iter_next(s, &it, &sub, &pub)) {
        if (!alias_owner_accepts_object(s, sub, pub, group, object)) continue;
        int k = rx_fan_find(rx, sub, pub);
        if (k < 0) {
            rx_fan_append(rx, sub, pub, false);
            k = (int)rx->fan_count - 1;
        }
        rx->fan[k].pending = false;
        rx->fan[k].cur = true;
    }
    if (pend) {
        for (int32_t i = s->sub_occ_head; i >= 0; i = s->subs[i].occ_next) {
            const moq_sub_entry_t *e = &s->subs[i];
            if (!sub_pending_for_track(e, h) || !sub_pending_accepts(s, e, group, object))
                continue;
            int k = rx_fan_find(rx, e->handle, MOQ_PUBLICATION_INVALID);
            if (k < 0) {
                rx_fan_append(rx, e->handle, MOQ_PUBLICATION_INVALID, true);
                k = (int)rx->fan_count - 1;
            }
            rx->fan[k].cur = true;
        }
    }
    rx_fan_note_ambiguity(rx);
    for (uint32_t k = 0; k < rx->fan_count; k++) rx->fan[k].owed = rx->fan[k].cur;
    return MOQ_OK;
}

/* Some established subscription takes the current object as an event
 * (a pending one only has it held). */
static bool rx_fan_any_cur(const moq_rx_stream_t *rx, bool streaming)
{
    for (uint32_t k = 0; k < rx->fan_count; k++)
        if (rx->fan[k].cur && (!rx->fan[k].pending || !streaming)) return true;
    return false;
}

/* A new event unit (chunk) of the current object is owed to each matching
 * subscription. */
static void rx_fan_arm(moq_rx_stream_t *rx)
{
    for (uint32_t k = 0; k < rx->fan_count; k++) rx->fan[k].owed = rx->fan[k].cur;
}

/* The one subscription a shared stream belongs to, if any: the only one it
 * ever delivered to; or, when it delivered nothing (an empty subgroup), the
 * only sharing subscription whose filter admits its group. Several, none,
 * or a subscription still pending whose answer could claim the alias, leave
 * it unattributed: its closure goes to each subscription it reached and its
 * Stream Count to none, so those subscriptions' PUBLISH_DONE waits complete
 * on their timer (draft-22 §9.9: "Subscribers SHOULD use a timeout or other
 * mechanism to remove subscription state"). Counting it toward a guess
 * could complete a subscription before its own last stream arrived. */
static bool rx_fan_owner(moq_session_t *s, const moq_rx_stream_t *rx,
                         moq_subscription_t *osub, moq_publication_t *opub)
{
    *osub = MOQ_SUBSCRIPTION_INVALID;
    *opub = MOQ_PUBLICATION_INVALID;
    if (rx->fan_ambiguous) return false;
    for (uint32_t k = 0; k < rx->fan_count; k++)
        if (rx->fan[k].pending) return false;
    if (rx->fan_count == 1) {
        *osub = rx->fan[0].sub;
        *opub = rx->fan[0].pub;
        return true;
    }
    if (rx->fan_count > 1) return false;
    moq_track_hist_t *h = rx_alias_track(s, rx->track_alias);
    if (h && h->pending_subs > 0) return false;
    size_t n = 0;
    moq_alias_iter_t it;
    moq_subscription_t sub;
    moq_publication_t pub;
    alias_iter_begin(s, &it, rx->track_alias);
    while (alias_iter_next(s, &it, &sub, &pub)) {
        if (!alias_owner_accepts_group(s, sub, pub, rx->group_id)) continue;
        *osub = sub;
        *opub = pub;
        n++;
    }
    return n == 1;
}

/* The subgroup closure (SUBGROUP_FINISHED or SUBGROUP_RESET) of a shared
 * stream, once to each established subscription it delivered to that an
 * object terminal did not already close. Resumable: an entry is marked as
 * its event is queued, so WOULD_BLOCK retries only the rest. */
static moq_result_t rx_fan_closure(moq_session_t *s, moq_rx_stream_t *rx,
                                   bool reset, uint64_t error_code)
{
    if (rx->fan_count == 0) {
        moq_subscription_t osub;
        moq_publication_t opub;
        if (rx_fan_owner(s, rx, &osub, &opub) && rx->fan_cap > 0)
            rx_fan_append(rx, osub, opub, false);
    }
    for (uint32_t k = 0; k < rx->fan_count; k++) {
        moq_rx_fan_t *f = &rx->fan[k];
        if (f->pending || f->closure || (reset && f->closed)) continue;
        if (!rx_fan_alive(s, f)) { f->closure = true; continue; }
        if (event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
        moq_event_t e;
        memset(&e, 0, sizeof(e));
        e.borrow_epoch = s->borrow_epoch;
        if (reset) {
            e.kind = MOQ_EVENT_SUBGROUP_RESET;
            e.detail_size = (uint32_t)sizeof(moq_subgroup_reset_event_t);
            e.u.subgroup_reset.sub = f->sub;
            e.u.subgroup_reset.pub = f->pub;
            e.u.subgroup_reset.group_id = rx->group_id;
            e.u.subgroup_reset.subgroup_id = rx->subgroup_id;
            e.u.subgroup_reset.error_code = error_code;
            e.u.subgroup_reset.end_of_group = rx->end_of_group;
        } else {
            e.kind = MOQ_EVENT_SUBGROUP_FINISHED;
            e.detail_size = (uint32_t)sizeof(moq_subgroup_finished_event_t);
            e.u.subgroup_finished.sub = f->sub;
            e.u.subgroup_finished.pub = f->pub;
            e.u.subgroup_finished.group_id = rx->group_id;
            e.u.subgroup_finished.subgroup_id = rx->subgroup_id;
            e.u.subgroup_finished.end_of_group = rx->end_of_group;
        }
        moq_result_t rc = push_event(s, &e);
        if (rc < 0) return rc;
        f->closure = true;
    }
    return MOQ_OK;
}

/* A pending subscription got its answer (`established`, under `alias`) or
 * ended: on every shared stream of that alias its held-for entry becomes an
 * ordinary one; elsewhere it is dropped. */
void rx_settle_pending(moq_session_t *s, moq_subscription_t sub,
                       bool established, uint64_t alias)
{
    for (int32_t i = s->rx_occ_head; i >= 0; i = s->rx_streams[i].occ_next) {
        moq_rx_stream_t *rx = &s->rx_streams[i];
        if (!rx->active || !rx->fan_shared) continue;
        int k = rx_fan_find(rx, sub, MOQ_PUBLICATION_INVALID);
        if (k < 0 || !rx->fan[k].pending) continue;
        if (established && rx->track_alias == alias) {
            rx->fan[k].pending = false;
            rx_fan_note_ambiguity(rx);
        } else {
            rx_fan_remove(rx, (uint32_t)k);
        }
    }
}

static void rx_free_entry(moq_session_t *s, size_t slot)
{
    moq_rx_stream_t *rx = &s->rx_streams[slot];
    rx_occ_unlink(s, slot);
    moq_index_remove(s->idx_rx_by_ref, s->idx_rx_mask, rx->stream_ref._v);
    rx_release_resources(s, slot);
    rx_fan_free(s, rx);
    memset(rx, 0, sizeof(*rx));
    rx->occ_next = -1;     /* the memset zeroed these; -1 is "unlinked" */
    rx->occ_prev = -1;
}

/* An IDENTIFIABLE (post-header, publication- or subscription-bound) data
 * stream counts toward the terminal done's Stream Count once the peer's
 * terminal for it has arrived -- a FIN, or a RESET -- whether or not this
 * side had STOPped the stream first: the publisher counted every stream it
 * opened, and a stream whose peer terminal is in is processed, nothing more
 * arrives on it. Called only at the terminal teardown sites in handle_data_*
 * (after any streaming terminal-RESET event queued), NEVER from generic
 * rx_free_entry -- session teardown and malformed-stream cleanup must not
 * count, and a pre-header RESET is un-attributable (the binding never
 * happened). A locally stopped entry keeps its binding for exactly this. */
static void rx_record_terminal_processed(moq_session_t *s, int slot)
{
    if (moq_publication_is_valid(s->rx_streams[slot].pub_handle))
        pub_note_stream_processed(s, s->rx_streams[slot].pub_handle);
    else if (moq_subscription_is_valid(s->rx_streams[slot].sub))
        sub_note_stream_processed(s, s->rx_streams[slot].sub);
    else if (s->rx_streams[slot].fan_shared) {
        moq_subscription_t osub;
        moq_publication_t opub;
        if (rx_fan_owner(s, &s->rx_streams[slot], &osub, &opub)) {
            if (moq_subscription_is_valid(osub)) sub_note_stream_processed(s, osub);
            else if (moq_publication_is_valid(opub)) pub_note_stream_processed(s, opub);
        }
    }
}

/* Remember that the peer FIN'd this transport stream, so later bytes on the
 * same ref are rejected as data after FIN instead of parsed afresh. This is
 * the transport fact only: it carries no completion side effect (no
 * SUBGROUP_FINISHED), so it is what a locally STOPped stream records when
 * its FIN finally arrives -- the stream count is recorded separately. */
static void rx_note_finished_ref(moq_session_t *s, uint64_t ref_v)
{
    size_t idx = (s->rx_fin_head + s->rx_fin_count) % s->rx_fin_cap;
    if (s->rx_fin_count >= s->rx_fin_cap)
        s->rx_fin_head = (s->rx_fin_head + 1) % s->rx_fin_cap;
    else
        s->rx_fin_count++;
    s->rx_finished[idx] = ref_v;
}

static void rx_record_finished(moq_session_t *s, uint64_t ref_v)
{
    /* A COMPLETED data stream bound to a subscriber-role publication or
     * subscription counts toward that binding's terminal-done Stream Count.
     * This is the successful-completion signal; a stream the session
     * STOPped and identifiable RESET terminations count via
     * rx_record_terminal_processed at their teardown sites. The rx is still
     * live here (freed by the caller right after). Fetch streams carry an
     * invalid pub_handle AND an invalid sub, so the count is a no-op for
     * them; a fill stream carries its parent and counts. */
    int rxslot = rx_find_by_ref(s, moq_stream_ref_from_u64(ref_v));
    if (rxslot >= 0) rx_record_terminal_processed(s, rxslot);
    rx_note_finished_ref(s, ref_v);
}

/* Forward State 0 prohibits Objects, not streams: a bound stream stays
 * bound (and its FIN / identifiable RESET counts exactly once) while the
 * binding's CURRENT acknowledged Forward state is 0, but its objects are
 * consumed without surfacing object/chunk events. Applies to BOTH
 * subscriber-role subscriptions (we asked the publisher to pause) and
 * subscriber-role publications. Evaluated ONCE per object at header
 * admission and frozen into rx->suppress_cur_object -- never re-read at
 * emission time, so a mid-object Forward flip cannot truncate or expose a
 * partial chunk sequence. */
/* Streaming receive surfaces an object as a run of chunks a consumer routes
 * by (group, subgroup, object). A publisher that resets a subgroup stream
 * mid-object and reopens the subgroup on a new stream may resend that object
 * while the old stream's bytes are still arriving (draft-22 §11.3.2 has a
 * relay that does not know continuity reset and reopen). Two runs for one
 * identity must not interleave for any one recipient: an object that begins
 * while another stream of the same Track alias is mid-way through surfacing
 * that same object to a recipient is consumed without events for that
 * recipient. A recipient the other stream does not deliver the object to --
 * one that subscribed after that stream's object began -- has nothing in
 * flight and receives this copy. */
static bool rx_delivers_cur_to(const moq_rx_stream_t *o, moq_subscription_t sub,
                               moq_publication_t pub)
{
    if (o->fan_shared) {
        int k = rx_fan_find(o, sub, pub);
        return k >= 0 && o->fan[k].cur && !o->fan[k].pending;
    }
    moq_rx_fan_t single = { .sub = o->sub, .pub = o->pub_handle };
    return rx_fan_owner_eq(&single, sub, pub);
}

/* Remove from the object just admitted on `slot` every recipient that has
 * the same object in flight on another stream; true when no recipient is
 * left, so the copy is consumed without events. */
static bool rx_copy_in_flight_elsewhere(moq_session_t *s, int slot, uint64_t object_id)
{
    if (!s->streaming_objects) return false;
    moq_rx_stream_t *rx = &s->rx_streams[slot];
    for (int32_t i = s->rx_occ_head; i >= 0; i = s->rx_streams[i].occ_next) {
        if (i == slot) continue;
        const moq_rx_stream_t *o = &s->rx_streams[i];
        if (!o->active || o->stream_kind != MOQ_STREAM_KIND_SUBGROUP ||
            o->track_alias != rx->track_alias || o->group_id != rx->group_id ||
            !o->subgroup_id_resolved || o->subgroup_id != rx->subgroup_id ||
            o->cur_object_id != object_id || o->suppress_cur_object)
            continue;
        if (o->parse_state != MOQ_RX_STREAMING_PAYLOAD &&
            o->parse_state != MOQ_RX_PENDING_CHUNK)
            continue;
        if (!rx->fan_shared) {
            if (rx_delivers_cur_to(o, rx->sub, rx->pub_handle)) return true;
            continue;
        }
        for (uint32_t k = 0; k < rx->fan_count; k++) {
            moq_rx_fan_t *f = &rx->fan[k];
            if (f->cur && !f->pending && rx_delivers_cur_to(o, f->sub, f->pub)) {
                f->cur = false;
                f->owed = false;
            }
        }
    }
    return rx->fan_shared && !rx_fan_any_cur(rx, true);
}

/* A Track with a malformed verdict delivers nothing more: not the rest of an
 * object admitted before the verdict, nor an object already complete and
 * waiting for event capacity. From the verdict on, the stream's current
 * object is consumed as a suppressed one; the request's malformed terminal
 * ends whatever of it was surfaced. */
static void rx_gate_on_verdict(moq_session_t *s, moq_rx_stream_t *rx)
{
    if (!rx->suppress_cur_object && s->mt_failing_count > 0 &&
        rx->stream_kind == MOQ_STREAM_KIND_SUBGROUP &&
        session_alias_failing(s, rx->track_alias))
        rx->suppress_cur_object = true;
}

static bool rx_objects_suppressed(moq_session_t *s,
                                  const moq_rx_stream_t *rx)
{
    /* Shared mode: Forward is part of each subscription's filter (draft-22
     * §3.3.3), already applied by rx_fan_match. */
    if (rx->fan_shared) return !rx_fan_any_cur(rx, s->streaming_objects);
    if (moq_subscription_is_valid(rx->sub)) {
        int sslot = sub_resolve_handle(s, rx->sub);
        return sslot >= 0 &&
               s->subs[sslot].role == MOQ_SUB_ROLE_SUBSCRIBER &&
               !s->subs[sslot].forward;
    }
    if (moq_publication_is_valid(rx->pub_handle)) {
        int pslot = pub_resolve_handle(s, rx->pub_handle);
        return pslot >= 0 && !s->publishes[pslot].send_allowed;
    }
    return false;
}

static moq_result_t rx_try_stop(moq_session_t *s, int slot);

/* Stop a stream whose subscription or publication is gone: its binding has
 * ended on this side, so the STOP carries CANCELLED and, once queued, only
 * the stream's identity is kept (see rx_end_bound_streams). */
static moq_result_t rx_stop_gone(moq_session_t *s, int slot)
{
    s->rx_streams[slot].binding_ended = true;
    return rx_try_stop(s, slot);
}

/* §9.8 EXPIRED sweep: STOP-and-discard every live rx stream bound to the
 * given subscription/publication (the drafts' permitted early-discard),
 * rescanning to zero -- stopping frees the rx entry, so a fully successful
 * sweep terminates with a clean scan. Returns MOQ_OK when no live bound
 * stream remains; MOQ_ERR_WOULD_BLOCK when action capacity ran out (the
 * remaining streams are taken up by the next reap pass). */

/*
 * Resumable form: each STOP attempt costs one budget unit and the scan position
 * persists in s->sweep_rx_pos, so a suspended sweep resumes mid-pool instead of
 * re-attempting streams it already stopped. Non-matching and inactive slots are
 * free and never suspend.
 *
 * Returns MOQ_SESSION_SUSPENDED when the budget ran out with matching streams
 * still live. That is distinct from MOQ_ERR_WOULD_BLOCK, which continues to
 * mean action capacity was exhausted and the remaining streams pass to the next
 * reap.
 */
#ifdef MOQ_SESSION_SWEEP_TESTING
/* Counts entries into the bound-stream scan. Present only in the test-internals
 * archive; the shipped library has no such symbol. */
uint64_t session_stop_scan_entries;
#endif

moq_result_t session_stop_bound_streams_resumable(moq_session_t *s,
                                                  moq_subscription_t sub,
                                                  moq_publication_t pub,
                                                  uint32_t *budget)
{
#ifdef MOQ_SESSION_SWEEP_TESTING
    session_stop_scan_entries++;
#endif
    for (;;) {
#ifdef MOQ_SESSION_SWEEP_TESTING
        session_work_rx_rescan_passes++;
#endif
        /* Walks the rx OCCUPANCY list: the scan's cost follows the live
         * streams, not `rx_cap`. `nxt` is captured before rx_try_stop(), which
         * frees the entry and unlinks it (that unlink also advances this
         * cursor, hence the conditional advance). */
        while (s->sweep_rx_pos < s->rx_cap) {
            size_t i = s->sweep_rx_pos;
            int32_t nxt = s->rx_streams[i].occ_next;
#define RX_SCAN_ADVANCE()                                                     \
    do { if (s->sweep_rx_pos == i)                                            \
             s->sweep_rx_pos = (nxt >= 0) ? (size_t)nxt : s->rx_cap; } while (0)
            moq_rx_stream_t *rx = &s->rx_streams[i];
#ifdef MOQ_SESSION_SWEEP_TESTING
            session_work_rx_probes++;
#endif
            if (!rx->active || rx->parse_state == MOQ_RX_STOPPED) {
                RX_SCAN_ADVANCE();   /* a stopped entry is already discarded */
                continue;
            }
            bool match = false;
            if (moq_subscription_is_valid(sub) &&
                moq_subscription_is_valid(rx->sub) &&
                moq_subscription_eq(rx->sub, sub))
                match = true;
            if (moq_publication_is_valid(pub) &&
                moq_publication_is_valid(rx->pub_handle) &&
                moq_publication_eq(rx->pub_handle, pub))
                match = true;
            /* A shared stream is this subscription's alone only when it
             * delivered to no other; otherwise it stays for the others. */
            if (rx->fan_shared && rx->fan_count == 1 && !rx->fan[0].pending &&
                rx_fan_owner_eq(&rx->fan[0], sub, pub))
                match = true;
            if (!match) { RX_SCAN_ADVANCE(); continue; }

            if (budget) {
                if (*budget == 0) return MOQ_SESSION_SUSPENDED;
                (*budget)--;
            }
            s->sweep_rx_found = true;
#ifdef MOQ_SESSION_SWEEP_TESTING
            session_work_rx_charged++;
#endif
            /* The subscriber is deleting the subscription's state: the
             * stream is cancelled (CANCELLED), and held stopped until the
             * peer's FIN or RESET like any stopped stream. */
            s->rx_streams[i].stop_cancels = true;
            moq_result_t rc = rx_try_stop(s, (int)i);
            if (rc < 0) return rc;
            RX_SCAN_ADVANCE();
        }
#undef RX_SCAN_ADVANCE
        /* A completed scan that stopped nothing means no bound stream remains.
         * Otherwise rescan from zero: a stopped entry is skipped by its state
         * (its binding is kept for the stream count), so the pool settles to
         * a clean pass before the owner may finalize. */
        bool found = s->sweep_rx_found;
        s->sweep_rx_pos = moq_occ_first(s->rx_occ_head, s->rx_cap);
        s->sweep_rx_found = false;
        if (!found) return MOQ_OK;
    }
}

/* -- Streams stopped without a receive entry ----------------------- *
 * A compact, unordered set of stream refs (see rx_stopped_refs). */
int rx_stopped_ref_find(const moq_session_t *s, uint64_t ref_v)
{
    for (size_t i = 0; i < s->rx_stopped_count; i++)
        if (s->rx_stopped_refs[i] == ref_v) return (int)i;
    return -1;
}

static void rx_stopped_ref_remove(moq_session_t *s, int idx)
{
    s->rx_stopped_count--;
    s->rx_stopped_refs[idx] = s->rx_stopped_refs[s->rx_stopped_count];
}

/* -- Inbound streams of a subscription that ends on this side --------- *
 * When the subscriber deletes subscription state it "SHOULD send STOP_SENDING
 * on all streams related to the subscription" (draft-16 §9.15, draft-18
 * §10.11, draft-22 §9.9), and objects that still arrive afterwards are to be
 * discarded quickly (draft-16 §10.1, draft-18 §11.1, draft-22 §3.1.3). Fill
 * streams are not covered here: the fill owner stops them when the parent
 * ends. */

static bool rx_bound_to(const moq_rx_stream_t *rx, moq_subscription_t sub,
                        moq_publication_t pub)
{
    if (!rx->active || rx->fill_bound) return false;
    if (moq_subscription_is_valid(sub) && moq_subscription_is_valid(rx->sub) &&
        moq_subscription_eq(rx->sub, sub))
        return true;
    return moq_publication_is_valid(pub) &&
           moq_publication_is_valid(rx->pub_handle) &&
           moq_publication_eq(rx->pub_handle, pub);
}

/* The peer's FIN or RESET for the stream is already in: nothing to stop. */
static bool rx_peer_terminal_in(const moq_rx_stream_t *rx)
{
    return rx->pending_fin || rx->reset_owed;
}

/* A stream parked on its SUBGROUP_FINISHED or SUBGROUP_RESET owes that one
 * closure for objects already delivered, even once the subscription is freed
 * (see handle_data_bytes_impl), and completes on its own. */
static bool rx_closure_parked(const moq_rx_stream_t *rx)
{
    return rx->parse_state == MOQ_RX_PENDING_FINISHED ||
           rx->parse_state == MOQ_RX_PENDING_RESET;
}

/* Give a stopped stream's receive entry up for an identity record (see
 * rx_stopped_refs); with no record left the entry keeps the identity. */
static void rx_stopped_to_record(moq_session_t *s, int slot)
{
    if (s->rx_stopped_count >= s->rx_stopped_cap) return;
    uint64_t ref_v = s->rx_streams[slot].stream_ref._v;
    rx_free_entry(s, (size_t)slot);
    s->rx_stopped_refs[s->rx_stopped_count++] = ref_v;
}

void rx_end_bound_streams(moq_session_t *s, moq_subscription_t sub,
                          moq_publication_t pub, size_t keep_free)
{
    /* Walks the rx OCCUPANCY list; the successor is captured before a free
     * unlinks the entry. */
    for (int32_t i = s->rx_occ_head, nxt; i >= 0; i = nxt) {
        moq_rx_stream_t *rx = &s->rx_streams[i];
        nxt = rx->occ_next;
        if (rx->active && rx->fan_shared && !rx->fill_bound) {
            /* A shared stream belongs to the ending subscription only when
             * that subscription is the only one it delivered to, or when it
             * delivered to none and no other subscription shares the alias.
             * Otherwise the subscription just leaves its list. */
            int k = rx_fan_find(rx, sub, pub);
            if (k >= 0 && rx->fan_count > 1) {
                rx_fan_remove(rx, (uint32_t)k);
                continue;
            }
            if (k < 0) {
                bool others = rx->fan_count > 0;
                moq_alias_iter_t it;
                moq_subscription_t osub;
                moq_publication_t opub;
                alias_iter_begin(s, &it, rx->track_alias);
                while (!others && alias_iter_next(s, &it, &osub, &opub)) {
                    moq_rx_fan_t probe = { .sub = osub, .pub = opub };
                    if (!rx_fan_owner_eq(&probe, sub, pub)) others = true;
                }
                if (others) continue;
            }
            if (rx_closure_parked(rx)) continue;
        } else if (!rx_bound_to(rx, sub, pub) || rx_closure_parked(rx))
            continue;
        if (rx->parse_state != MOQ_RX_STOPPED && rx_peer_terminal_in(rx)) {
            /* Nothing more arrives: release it without its parked object or
             * terminal, which belong to the ended subscription. */
            if (rx->pending_fin) rx_note_finished_ref(s, rx->stream_ref._v);
            rx_free_entry(s, (size_t)i);
            continue;
        }
        /* The ended subscription counts no more streams, so only the
         * stream's identity is kept, to consume whatever the peer had already
         * sent until its FIN or RESET; the receive entry is free for new
         * streams. */
        rx->binding_ended = true;
        if (rx->parse_state == MOQ_RX_STOPPED) {
            rx_stopped_to_record(s, (int)i);
            continue;
        }
        /* Nothing parsed from here on: the buffers go now, whether the STOP
         * is queued at once or stays owed for want of action capacity. */
        rx_release_resources(s, (size_t)i);
        rx->reset_owed = false;
        rx->resumed_deferred = false;
        if (action_queue_avail(s) > keep_free)
            (void)rx_try_stop(s, (int)i);
        else
            rx->parse_state = MOQ_RX_NEED_STOP;   /* owed; see above */
    }
}

static bool rx_is_finished(moq_session_t *s, uint64_t ref_v)
{
    for (size_t i = 0; i < s->rx_fin_count; i++) {
        size_t idx = (s->rx_fin_head + i) % s->rx_fin_cap;
        if (s->rx_finished[idx] == ref_v) return true;
    }
    return false;
}

/* The single graceful-FIN completion path for a receive data stream. For a
 * subgroup stream whose header was parsed and bound to exactly one of a
 * subscription / publication, emit SUBGROUP_FINISHED (after the final
 * object/chunk of the stream, which the caller has already delivered); then
 * record the FIN and free the entry. Fetch streams, headerless streams, and
 * unbound (deferred-alias) streams carry no such binding and skip the event --
 * they just record + free, matching the prior behavior exactly.
 *
 * Backpressure: if the event queue is full the entry is left ACTIVE in
 * MOQ_RX_PENDING_FINISHED with pending_fin set and MOQ_ERR_WOULD_BLOCK is
 * returned WITHOUT recording/freeing. The retry (a re-driven FIN delivery)
 * re-enters here and re-attempts only the SUBGROUP_FINISHED push -- never any
 * prior object/chunk, whose ownership has already transferred. */
/*
 * Emit SUBGROUP_RESET for an abnormally terminated subgroup stream. Mirrors
 * rx_finish_stream(): same binding/resolution guard, same parking discipline
 * when the event queue is full. The caller frees the entry on MOQ_OK.
 */
static moq_result_t rx_drive_reset(moq_session_t *s, int slot);

static moq_result_t rx_emit_subgroup_reset(moq_session_t *s, int slot,
                                           uint64_t error_code)
{
    moq_rx_stream_t *rx = &s->rx_streams[slot];

    /* A FETCH response stream: the closure is the request-local FETCH_RESET,
     * owed only when the stream's FETCH_HEADER bound it to a live fetcher-role
     * request. An unbound stream (header never arrived, or stale/retired
     * handle) has no request identity to report and is released silently --
     * a request-side terminal, not this stream, closes that request. A refusal
     * parks the obligation exactly like the subgroup path, so the documented
     * retry (the same reset input, or any later drive) completes it. */
    if (rx->stream_kind == MOQ_STREAM_KIND_FETCH) {
        /* A fill stream: the child records the peer's RESET (nothing to
         * reserve) and surfaces its terminal through the fill service. */
        if (rx->fill_bound) {
            fill_rx_peer_reset(s, rx, error_code);
            return MOQ_OK;
        }
        int fslot = fetch_resolve_handle(s, rx->fetch);
        if (fslot < 0) return MOQ_OK;
        moq_result_t frc = fetch_on_data_stream_reset(s, fslot, error_code);
        if (frc == MOQ_ERR_WOULD_BLOCK) {
            rx->reset_error_code = error_code;
            rx->parse_state = MOQ_RX_PENDING_RESET;
        }
        return frc;
    }

    if (rx->fan_shared) {
        if (rx->stream_kind != MOQ_STREAM_KIND_SUBGROUP ||
            !rx->subgroup_id_resolved)
            return MOQ_OK;
        moq_result_t frc = rx_fan_closure(s, rx, true, error_code);
        if (frc < 0) {
            rx->reset_error_code = error_code;
            rx->parse_state = MOQ_RX_PENDING_RESET;
        }
        return frc;
    }

    bool bound = moq_subscription_is_valid(rx->sub) !=
                 moq_publication_is_valid(rx->pub_handle);
    /* Same guard as the clean-FIN path: a FIRST_OBJECT-mode header whose
     * stream never carried an object holds only the decoder's default (0),
     * not a real subgroup ID -- reporting it would let a relay seal subgroup
     * 0 by mistake. */
    if (rx->stream_kind != MOQ_STREAM_KIND_SUBGROUP || !bound ||
        !rx->subgroup_id_resolved)
        return MOQ_OK;

    if (event_queue_full(s)) {
        rx->reset_error_code = error_code;
        rx->parse_state = MOQ_RX_PENDING_RESET;
        return MOQ_ERR_WOULD_BLOCK;
    }

    moq_event_t e;
    memset(&e, 0, sizeof(e));
    e.kind = MOQ_EVENT_SUBGROUP_RESET;
    e.detail_size = (uint32_t)sizeof(moq_subgroup_reset_event_t);
    e.borrow_epoch = s->borrow_epoch;
    e.u.subgroup_reset.sub = rx->sub;
    e.u.subgroup_reset.pub = rx->pub_handle;
    e.u.subgroup_reset.group_id = rx->group_id;
    e.u.subgroup_reset.subgroup_id = rx->subgroup_id;
    e.u.subgroup_reset.error_code = error_code;
    e.u.subgroup_reset.end_of_group = rx->end_of_group;
    moq_result_t rc = push_event(s, &e);
    if (rc < 0) {
        rx->reset_error_code = error_code;
        rx->parse_state = MOQ_RX_PENDING_RESET;
        return rc;
    }
    return MOQ_OK;
}

static moq_result_t rx_stop_gone(moq_session_t *s, int slot);

/* The publisher priority a subgroup stream's objects carry, with an omitted
 * one resolved against the Track the alias names. */
static uint8_t rx_object_priority(moq_session_t *s, const moq_rx_stream_t *rx)
{
    moq_subscription_t sub = rx->sub;
    moq_publication_t pub = rx->pub_handle;
    if (rx->fan_shared) {
        int ss = sub_find_by_alias_subscriber(s, rx->track_alias);
        if (ss >= 0) sub = s->subs[ss].handle;
    }
    return session_resolve_publisher_priority(s, sub, pub, rx->default_priority,
                                              rx->publisher_priority);
}

/* Stop a stream whose Track was found malformed, unless cancelling its
 * requests already ended, stopped or released it. Nothing more of it is
 * parsed: a STOP the queue cannot take now stays owed (NEED_STOP) and is
 * issued once actions drain. The caller returns at once and never compacts
 * the input, which the stream may no longer own. */
static moq_result_t rx_stop_malformed(moq_session_t *s, int slot, uint64_t ref_v)
{
    moq_rx_stream_t *rx = &s->rx_streams[slot];
    if (!rx->active || rx->stream_ref._v != ref_v ||
        rx->parse_state == MOQ_RX_STOPPED || rx->parse_state == MOQ_RX_NEED_STOP)
        return MOQ_OK;
    rx->stop_cancels = true;
    moq_result_t rc = rx_try_stop(s, slot);
    return rc == MOQ_ERR_WOULD_BLOCK ? MOQ_OK : rc;
}

/* The Track a subgroup stream carries is malformed (draft-22 §12.1): the
 * verdict is latched on every request for that Track from the peer and their
 * cancellations driven (see session_fail_malformed_track), and this stream is
 * stopped. Nothing more is delivered from it, and the verdict does not depend
 * on the stream being driven again. */
static moq_result_t rx_fail_malformed(moq_session_t *s, int slot)
{
    uint64_t ref_v = s->rx_streams[slot].stream_ref._v;
    session_fail_malformed_alias(s, s->rx_streams[slot].track_alias);
    if (s->state == MOQ_SESS_CLOSED) return MOQ_ERR_CLOSED;
    return rx_stop_malformed(s, slot, ref_v);
}

/* 1 when a FETCH or fill response object makes its Track malformed (-1 when
 * its detection record could not be created, 0 otherwise): an
 * Object Property violation, a Location that does not follow the prior one in
 * the same Group (§11.4: Objects in ascending Object ID within a Group, after
 * any End of Range), or, for a FETCH, a condition its Track record catches. */
static int rx_fetch_object_malformed(moq_session_t *s, const moq_rx_stream_t *rx,
                                     const moq_decoded_fetch_object_t *fobj,
                                     const moq_fetch_t *fetch)
{
    if (!s->profile->malformed_track_local || fobj->is_range_marker) return 0;
    if (fobj->malformed != MOQ_MALFORMED_NONE) return 1;
    if (rx->fetch_prior.has_prev && fobj->group_id == rx->fetch_prior.group_id &&
        fobj->object_id <= rx->fetch_prior.object_id)
        return 1;
    if (!fetch) return 0;
    moq_result_t trc;
    moq_mt_track_t *t = mt_track_for_fetch(s, *fetch, &trc);
    if (trc < 0) return -1;
    moq_mt_object_t mo = {
        .group_id = fobj->group_id,
        .object_id = fobj->object_id,
        .in_subgroup = !fobj->datagram,
        .priority = fobj->publisher_priority,
        .subgroup_id = fobj->subgroup_id,
        .status = MOQ_OBJECT_NORMAL,
        .gaps = &fobj->gaps,
    };
    return mt_object(t, &mo) != MOQ_MALFORMED_NONE ? 1 : 0;
}

/* A fill response for a subscription or publication carries a malformed
 * Track: the parent's Track fails (which ends the fill with its parent) and
 * this stream is stopped. */
static moq_result_t rx_fail_fill_malformed(moq_session_t *s, int slot)
{
    moq_fill_t fill;
    moq_subscription_t sub;
    moq_publication_t pub;
    uint64_t initiating;
    uint64_t ref_v = s->rx_streams[slot].stream_ref._v;
    fill_rx_identity(s, &s->rx_streams[slot], &fill, &sub, &pub, &initiating);
    session_fail_malformed_owner(s, sub, pub);
    if (s->state == MOQ_SESS_CLOSED) return MOQ_ERR_CLOSED;
    return rx_stop_malformed(s, slot, ref_v);
}

/* True when the fill on this stream belongs to a parent whose Track already
 * has a malformed verdict: nothing more is delivered to it. */
static bool rx_fill_parent_failing(moq_session_t *s, int slot)
{
    moq_fill_t fill;
    moq_subscription_t sub;
    moq_publication_t pub;
    uint64_t initiating;
    if (s->mt_failing_count == 0) return false;
    fill_rx_identity(s, &s->rx_streams[slot], &fill, &sub, &pub, &initiating);
    int ss = sub_resolve_handle(s, sub);
    if (ss >= 0) return s->subs[ss].mt_failing;
    int ps = pub_resolve_handle(s, pub);
    return ps >= 0 && s->publishes[ps].mt_failing;
}

static moq_result_t rx_finish_stream(moq_session_t *s, int slot)
{
    moq_rx_stream_t *rx = &s->rx_streams[slot];
    uint64_t ref_v = rx->stream_ref._v;

    /* §12.1 conditions 2-4: the FIN fixes the subgroup's final Object (and
     * the group's, under END_OF_GROUP); a final that contradicts what the
     * Track already said fails it. The stream ended either way. */
    if (rx->stream_kind == MOQ_STREAM_KIND_SUBGROUP && rx->subgroup_id_resolved &&
        s->profile->malformed_track_local) {
        moq_result_t trc;
        moq_mt_track_t *t = mt_track_for_alias(s, rx->track_alias, &trc);
        if (trc < 0) {
            rx->parse_state = MOQ_RX_PENDING_FINISHED;
            return trc;
        }
        uint8_t why = mt_subgroup_fin(t, rx->group_id, rx->subgroup_id,
                                      rx->has_prev_object, rx->prev_object_id,
                                      rx->end_of_group);
        if (why != MOQ_MALFORMED_NONE) {
            session_fail_malformed_alias(s, rx->track_alias);
            if (s->state == MOQ_SESS_CLOSED) return MOQ_ERR_CLOSED;
            rx = &s->rx_streams[slot];
            if (rx->active && rx->stream_ref._v == ref_v) {
                rx_record_finished(s, ref_v);
                rx_free_entry(s, (size_t)slot);
            }
            return MOQ_OK;
        }
    }

    if (rx->fan_shared) {
        if (rx->stream_kind == MOQ_STREAM_KIND_SUBGROUP &&
            rx->subgroup_id_resolved) {
            moq_result_t frc = rx_fan_closure(s, rx, false, 0);
            if (frc < 0) {
                rx->parse_state = MOQ_RX_PENDING_FINISHED;
                return frc;
            }
        }
        rx_record_finished(s, ref_v);
        rx_free_entry(s, (size_t)slot);
        return MOQ_OK;
    }

    bool bound = moq_subscription_is_valid(rx->sub) !=
                 moq_publication_is_valid(rx->pub_handle);
    /* Skip the event when the subgroup ID was never resolved: a
     * FIRST_OBJECT-mode header whose stream FINs with no object carries only
     * the decoder's default (0), not a real subgroup ID -- emitting it would
     * let a relay seal/free subgroup 0 by mistake. Any object on the stream
     * resolves the ID, and ZERO/PRESENT modes are resolved at the header. */
    if (rx->stream_kind == MOQ_STREAM_KIND_SUBGROUP && bound &&
        rx->subgroup_id_resolved) {
        if (event_queue_full(s)) {
            rx->parse_state = MOQ_RX_PENDING_FINISHED;
            return MOQ_ERR_WOULD_BLOCK;
        }
        moq_event_t e;
        memset(&e, 0, sizeof(e));
        e.kind = MOQ_EVENT_SUBGROUP_FINISHED;
        e.detail_size = (uint32_t)sizeof(moq_subgroup_finished_event_t);
        e.borrow_epoch = s->borrow_epoch;
        e.u.subgroup_finished.sub = rx->sub;
        e.u.subgroup_finished.pub = rx->pub_handle;
        e.u.subgroup_finished.group_id = rx->group_id;
        e.u.subgroup_finished.subgroup_id = rx->subgroup_id;
        e.u.subgroup_finished.end_of_group = rx->end_of_group;
        moq_result_t rc = push_event(s, &e);
        if (rc < 0) {
            rx->parse_state = MOQ_RX_PENDING_FINISHED;
            return rc;
        }
    }

    rx_record_finished(s, ref_v);
    rx_free_entry(s, (size_t)slot);
    return MOQ_OK;
}

/* -- Receive helpers ----------------------------------------------- */

static moq_result_t rx_try_stop(moq_session_t *s, int slot)
{
    moq_rx_stream_t *rx = &s->rx_streams[slot];
    if (action_queue_full(s)) {
        rx->parse_state = MOQ_RX_NEED_STOP;
        return MOQ_ERR_WOULD_BLOCK;
    }
    moq_action_t a;
    memset(&a, 0, sizeof(a));
    a.kind = MOQ_ACTION_STOP_DATA;
    a.detail_size = (uint32_t)sizeof(moq_stop_data_action_t);
    a.borrow_epoch = s->borrow_epoch;
    a.u.stop_data.stream_ref = rx->stream_ref;
    a.u.stop_data.error_code =
        (rx->binding_ended || rx->stop_cancels) ? 0x1 /* CANCELLED */ : 0;
    moq_result_t rc = push_action(s, &a);
    if (rc < 0) {
        rx->parse_state = MOQ_RX_NEED_STOP;
        return rc;
    }
    /* The STOP is now queued: if this stream was the late response to a
     * locally-cancelled fetch, release its cancel tombstone. Doing it here (not
     * at FETCH_HEADER decode) means a deferred/retried stop still consumes it. */
    if (rx->stop_consumes_cancel_tomb)
        fetch_cancel_tomb_consume(s, rx->cancel_tomb_request_id);
    /* A bound fill stream the session stops is a locally cancelled child;
     * the binding stays so the peer's FIN or RESET still settles it. */
    if (rx->fill_bound) fill_rx_local_stop(s, rx);
    /* The peer may still deliver what it had already sent: keep the stream's
     * identity in the stopped/discard state until its FIN or RESET, holding
     * nothing but the entry itself. The subscription / publication binding
     * stays too: the peer's terminal on a stopped stream counts it toward
     * the Stream Count gate like any other identifiable stream's. A FIN that
     * arrived with these very bytes ends it right here. */
    rx_release_resources(s, (size_t)slot);
    if (rx->pending_fin) {
        if (rx->fill_bound) fill_rx_peer_fin(s, rx);
        rx_record_terminal_processed(s, slot);
        rx_note_finished_ref(s, rx->stream_ref._v);
        rx_free_entry(s, (size_t)slot);
        return MOQ_OK;
    }
    rx->fetch = (moq_fetch_t){ 0 };
    rx->stop_consumes_cancel_tomb = false;
    rx->reset_owed = false;
    rx->parse_state = MOQ_RX_STOPPED;
    if (rx->binding_ended) rx_stopped_to_record(s, slot);
    return MOQ_OK;
}

/* A data stream is bound (at its header) to a subscription or publication. That
 * binding can be freed mid-stream -- e.g. the app rejects a still-pending PUBLISH
 * after early objects (§9.4) already bound this stream. Re-resolve the bound handle
 * so we never emit an object/chunk against a stale handle; an unbound stream (no
 * handle yet) and a FETCH stream (resolved separately) are always "alive" here. */
static bool rx_binding_alive(moq_session_t *s, const moq_rx_stream_t *rx)
{
    if (moq_subscription_is_valid(rx->sub))
        return sub_resolve_handle(s, rx->sub) >= 0;
    if (moq_publication_is_valid(rx->pub_handle))
        return pub_resolve_handle(s, rx->pub_handle) >= 0;
    return true;
}

/* -- Streaming chunk emit ------------------------------------------
 *
 * Ownership during PENDING_CHUNK retry:
 *   - Chunk payload bytes are owned by rx->pending_chunk (rcbuf) while
 *     retrying. Freed by rx_free_entry if the stream is torn down.
 *   - Begin-object extensions remain in rx->cur_extensions until
 *     push_event succeeds. On successful begin-event push, ownership
 *     transfers to the queued event and cur_extensions is set NULL.
 */

/* Shared mode: the pending chunk once to each matching subscription still
 * owed it. Each event holds its own reference to the chunk (and, on a begin
 * chunk, to the object's properties, charged to the receive budget per
 * event as the poll releases it per event); the stream's own references are
 * released once every subscription has its event. Resumable under
 * WOULD_BLOCK: a served subscription is never served twice. */
static moq_result_t rx_fan_push_chunk(moq_session_t *s, int slot)
{
    moq_rx_stream_t *rx = &s->rx_streams[slot];
    size_t ext_len = (rx->pending_begin && rx->cur_extensions)
                         ? moq_rcbuf_len(rx->cur_extensions) : 0;
    for (uint32_t k = 0; k < rx->fan_count; k++) {
        moq_rx_fan_t *f = &rx->fan[k];
        if (!f->owed) continue;
        /* A pending subscription is not held a chunked object: holding it
         * would mean assembling the payload streaming delivery exists to
         * avoid. */
        if (f->pending || !rx_fan_alive(s, f)) { f->owed = false; continue; }
        if (event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
        moq_event_t e;
        memset(&e, 0, sizeof(e));
        e.kind = MOQ_EVENT_OBJECT_CHUNK;
        e.detail_size = (uint32_t)sizeof(moq_object_chunk_event_t);
        e.borrow_epoch = s->borrow_epoch;
        e.u.object_chunk.sub = f->sub;
        e.u.object_chunk.pub = f->pub;
        e.u.object_chunk.chunk = rx->pending_chunk;
        e.u.object_chunk.begin = rx->pending_begin;
        e.u.object_chunk.end = rx->pending_end;
        e.u.object_chunk.terminal = rx->pending_terminal;
        e.u.object_chunk.group_id = rx->group_id;
        e.u.object_chunk.subgroup_id = rx->subgroup_id;
        e.u.object_chunk.object_id = rx->cur_object_id;
        e.u.object_chunk.error_code =
            (rx->pending_terminal == MOQ_OBJECT_TERMINAL_RESET)
                ? rx->pending_reset_code
                : 0;
        if (rx->pending_begin) {
            e.u.object_chunk.publisher_priority = session_resolve_publisher_priority(
                s, f->sub, f->pub, rx->default_priority, rx->publisher_priority);
            e.u.object_chunk.status = rx->cur_status;
            e.u.object_chunk.end_of_group = rx->end_of_group;
            e.u.object_chunk.first_object = rx->cur_first_object;
            e.u.object_chunk.subgroup_properties = rx->has_extensions;
            e.u.object_chunk.payload_length = rx->payload_expected;
            e.u.object_chunk.properties = rx->cur_extensions;
        }
        moq_result_t rc = push_event(s, &e);
        if (rc < 0) return rc;
        if (rx->pending_chunk) moq_rcbuf_incref(rx->pending_chunk);
        if (e.u.object_chunk.properties) {
            moq_rcbuf_incref(rx->cur_extensions);
            s->recv_payload_bytes += ext_len;
        }
        f->owed = false;
        if (rx->pending_end && rx->pending_terminal == MOQ_OBJECT_TERMINAL_RESET)
            f->closed = true;
    }
    if (rx->pending_chunk) {
        moq_rcbuf_decref(rx->pending_chunk);
        rx->pending_chunk = NULL;
    }
    if (rx->pending_begin && rx->cur_extensions) {
        if (ext_len <= s->recv_payload_bytes) s->recv_payload_bytes -= ext_len;
        else s->recv_payload_bytes = 0;
        moq_rcbuf_decref(rx->cur_extensions);
        rx->cur_extensions = NULL;
    }
    return MOQ_OK;
}

static moq_result_t rx_finish_stream(moq_session_t *s, int slot);

/* Shared mode: the assembled object once to each matching subscription
 * still owed it, with the same per-event references and budget charges as
 * rx_fan_push_chunk; then the stream's own references are released and the
 * stream moves on exactly as after a single emit. */
static moq_result_t rx_fan_emit_object(moq_session_t *s, int slot)
{
    moq_rx_stream_t *rx = &s->rx_streams[slot];
    if (!rx->payload_rcbuf && rx->cur_status == MOQ_OBJECT_NORMAL) {
        /* A zero-length normal object: one empty buffer, shared by the
         * events and held by the stream until they are all queued. */
        moq_result_t rc = moq_rcbuf_create(&s->alloc, NULL, 0, &rx->payload_rcbuf);
        if (rc < 0) { rx->parse_state = MOQ_RX_PENDING_EMIT; return rc; }
        rx->payload_buf = NULL;
        rx->payload_expected = 0;
    }
    size_t plen = rx->payload_rcbuf ? moq_rcbuf_len(rx->payload_rcbuf) : 0;
    size_t elen = rx->cur_extensions ? moq_rcbuf_len(rx->cur_extensions) : 0;
    for (uint32_t k = 0; k < rx->fan_count; k++) {
        moq_rx_fan_t *f = &rx->fan[k];
        if (!f->owed) continue;
        if (!rx_fan_alive(s, f)) { f->owed = false; continue; }
        if (f->pending) {
            /* Held, not surfaced: the subscription's answer decides. */
            moq_held_object_t ho = {
                .group_id = rx->group_id, .subgroup_id = rx->subgroup_id,
                .object_id = rx->cur_object_id,
                .publisher_priority = rx->publisher_priority,
                .status = rx->cur_status, .end_of_group = rx->end_of_group,
                .datagram = false, .first_object = rx->cur_first_object,
                .subgroup_properties = rx->has_extensions,
                .default_priority = rx->default_priority,
                .payload = rx->payload_rcbuf, .properties = rx->cur_extensions,
            };
            session_stage_held_object(s, f->sub, rx->track_alias, &ho);
            f->owed = false;
            continue;
        }
        if (event_queue_full(s)) {
            rx->parse_state = MOQ_RX_PENDING_EMIT;
            return MOQ_ERR_WOULD_BLOCK;
        }
        moq_event_t e;
        memset(&e, 0, sizeof(e));
        e.kind = MOQ_EVENT_OBJECT_RECEIVED;
        e.detail_size = (uint32_t)sizeof(moq_object_received_event_t);
        e.borrow_epoch = s->borrow_epoch;
        e.u.object_received.sub = f->sub;
        e.u.object_received.pub = f->pub;
        e.u.object_received.group_id = rx->group_id;
        e.u.object_received.subgroup_id = rx->subgroup_id;
        e.u.object_received.object_id = rx->cur_object_id;
        e.u.object_received.publisher_priority = session_resolve_publisher_priority(
            s, f->sub, f->pub, rx->default_priority, rx->publisher_priority);
        e.u.object_received.status = rx->cur_status;
        e.u.object_received.end_of_group = rx->end_of_group;
        e.u.object_received.first_object = rx->cur_first_object;
        e.u.object_received.subgroup_properties = rx->has_extensions;
        e.u.object_received.payload = rx->payload_rcbuf;
        e.u.object_received.properties = rx->cur_extensions;
        moq_result_t rc = push_event(s, &e);
        if (rc < 0) { rx->parse_state = MOQ_RX_PENDING_EMIT; return rc; }
        if (rx->payload_rcbuf) moq_rcbuf_incref(rx->payload_rcbuf);
        if (rx->cur_extensions) moq_rcbuf_incref(rx->cur_extensions);
        s->recv_payload_bytes += plen + elen;
        f->owed = false;
    }
    if (rx->payload_rcbuf) {
        moq_rcbuf_decref(rx->payload_rcbuf);
        if ((size_t)rx->payload_expected <= s->recv_payload_bytes)
            s->recv_payload_bytes -= (size_t)rx->payload_expected;
        else
            s->recv_payload_bytes = 0;
        rx->payload_rcbuf = NULL;
        rx->payload_buf = NULL;
    }
    if (rx->cur_extensions) {
        if (elen <= s->recv_payload_bytes) s->recv_payload_bytes -= elen;
        else s->recv_payload_bytes = 0;
        moq_rcbuf_decref(rx->cur_extensions);
        rx->cur_extensions = NULL;
    }
    rx->payload_written = 0;
    if (rx->pending_fin && rx->hdr_len == 0 && rx->input_len == 0)
        return rx_finish_stream(s, slot);
    rx->parse_state = MOQ_RX_AWAITING_OBJECT;
    return MOQ_OK;
}

static moq_result_t rx_push_pending_chunk(moq_session_t *s, int slot)
{
    moq_rx_stream_t *rx = &s->rx_streams[slot];
    rx_gate_on_verdict(s, rx);

    /* Prohibited object (frozen at header admission): consume the pending
     * chunk without surfacing OBJECT_CHUNK. Reporting success lets every
     * caller -- begin/data/end emission, PENDING_CHUNK re-drives, and the
     * terminal-RESET teardown -- run its normal state transitions and
     * exactly-once FIN/RESET accounting. */
    if (rx->suppress_cur_object) {
        if (rx->pending_chunk) {
            moq_rcbuf_decref(rx->pending_chunk);
            rx->pending_chunk = NULL;
        }
        if (rx->pending_begin && rx->cur_extensions) {
            size_t ext_len = moq_rcbuf_len(rx->cur_extensions);
            if (ext_len <= s->recv_payload_bytes)
                s->recv_payload_bytes -= ext_len;
            else
                s->recv_payload_bytes = 0;
            moq_rcbuf_decref(rx->cur_extensions);
            rx->cur_extensions = NULL;
        }
        return MOQ_OK;
    }

    /* Re-allocate chunk if a prior attempt got NOMEM. */
    if (!rx->pending_chunk && rx->pending_data_len > 0) {
        moq_result_t rc = moq_rcbuf_create(&s->alloc,
            rx->input_buf, rx->pending_data_len, &rx->pending_chunk);
        if (rc < 0) return rc;
    }

    if (rx->fan_shared) return rx_fan_push_chunk(s, slot);

    if (event_queue_full(s))
        return MOQ_ERR_WOULD_BLOCK;

    moq_event_t e;
    memset(&e, 0, sizeof(e));
    e.kind = MOQ_EVENT_OBJECT_CHUNK;
    e.detail_size = (uint32_t)sizeof(moq_object_chunk_event_t);
    e.borrow_epoch = s->borrow_epoch;
    e.u.object_chunk.sub = rx->sub;
    e.u.object_chunk.pub = rx->pub_handle;
    e.u.object_chunk.chunk = rx->pending_chunk;
    e.u.object_chunk.begin = rx->pending_begin;
    e.u.object_chunk.end = rx->pending_end;
    e.u.object_chunk.terminal = rx->pending_terminal;
    /* Routing identity rides EVERY chunk: a consumer multiplexing concurrent
     * subgroups needs it on continuations and terminals, not only on begin. */
    e.u.object_chunk.group_id = rx->group_id;
    e.u.object_chunk.subgroup_id = rx->subgroup_id;
    e.u.object_chunk.object_id = rx->cur_object_id;
    /* The cause is meaningful only for an abnormal terminal; zero otherwise. */
    e.u.object_chunk.error_code =
        (rx->pending_terminal == MOQ_OBJECT_TERMINAL_RESET)
            ? rx->pending_reset_code
            : 0;

    if (rx->pending_begin) {
        e.u.object_chunk.publisher_priority = session_resolve_publisher_priority(
            s, rx->sub, rx->pub_handle, rx->default_priority, rx->publisher_priority);
        e.u.object_chunk.status = rx->cur_status;
        e.u.object_chunk.end_of_group = rx->end_of_group;
        e.u.object_chunk.first_object = rx->cur_first_object;
        e.u.object_chunk.subgroup_properties = rx->has_extensions;
        e.u.object_chunk.payload_length = rx->payload_expected;
        e.u.object_chunk.properties = rx->cur_extensions;
    }

    moq_result_t rc = push_event(s, &e);
    if (rc < 0) return rc;

    /* Ownership transferred to event queue. */
    rx->pending_chunk = NULL;
    if (rx->pending_begin)
        rx->cur_extensions = NULL;

    return MOQ_OK;
}

static moq_result_t rx_emit_chunk(moq_session_t *s, int slot,
                                   const uint8_t *data, size_t len,
                                   bool begin, bool end,
                                   moq_rcbuf_t *input_rcbuf,
                                   bool data_in_input_buf)
{
    moq_rx_stream_t *rx = &s->rx_streams[slot];

    /* A new chunk: owed again to every subscription the object matched. */
    if (rx->fan_shared) rx_fan_arm(rx);

    /* Allocate chunk rcbuf first, before mutating state. */
    moq_rcbuf_t *chunk = NULL;
    bool zero_copy = false;
    if (len > 0) {
        if (input_rcbuf) {
            const uint8_t *rcbuf_start = moq_rcbuf_data(input_rcbuf);
            size_t rcbuf_len = moq_rcbuf_len(input_rcbuf);
            size_t offset = SIZE_MAX;
            /* Continuation chunks: data points directly into rcbuf. */
            uintptr_t dp = (uintptr_t)data;
            uintptr_t rs = (uintptr_t)rcbuf_start;
            if (rcbuf_start && dp >= rs &&
                len <= rcbuf_len && dp - rs <= rcbuf_len - len)
                offset = (size_t)(dp - rs);
            /* Begin chunks via input_src_rcbuf: data is in input_buf,
             * which is a copy of the rcbuf. Cursor offset maps
             * directly because hdr_len was 0 at entry. */
            else if (data_in_input_buf && rx->input_buf) {
                uintptr_t ib = (uintptr_t)rx->input_buf;
                if (dp >= ib && dp - ib < rx->input_len) {
                    size_t ib_off = (size_t)(dp - ib);
                    if (len <= rcbuf_len && ib_off <= rcbuf_len - len)
                        offset = ib_off;
                }
            }
            if (offset != SIZE_MAX) {
                moq_result_t rc = moq_rcbuf_slice(&s->alloc, input_rcbuf,
                    offset, len, &chunk);
                if (rc < 0) {
                    if (data_in_input_buf) {
                        rx->pending_chunk = NULL;
                        rx->pending_begin = begin;
                        rx->pending_end = end;
                        rx->pending_terminal = MOQ_OBJECT_TERMINAL_NORMAL;
                        rx->pending_data_len = len;
                        rx->pending_from_input = true;
                        rx->parse_state = MOQ_RX_PENDING_CHUNK;
                    }
                    return rc;
                }
                zero_copy = true;
            }
        }
        if (!zero_copy) {
            moq_result_t rc = moq_rcbuf_create(&s->alloc, data, len, &chunk);
            if (rc < 0) {
                rx->pending_chunk = NULL;
                rx->pending_begin = begin;
                rx->pending_end = end;
                rx->pending_terminal = MOQ_OBJECT_TERMINAL_NORMAL;
                rx->pending_data_len = len;
                rx->pending_from_input = true;
                rx->parse_state = MOQ_RX_PENDING_CHUNK;
                return rc;
            }
        }
    }

    /* Store complete pending state before attempting push. */
    rx->pending_chunk = chunk;
    rx->pending_begin = begin;
    rx->pending_end = end;
    rx->pending_terminal = MOQ_OBJECT_TERMINAL_NORMAL;
    rx->pending_data_len = len;
    rx->pending_from_input = data_in_input_buf;
    rx->parse_state = MOQ_RX_PENDING_CHUNK;

    moq_result_t rc = rx_push_pending_chunk(s, slot);
    if (rc < 0) return rc;

    /* Push succeeded. Finalize state transition. */
    if (end) {
        if (rx->pending_fin && rx->input_len == 0) {
            return rx_finish_stream(s, slot);
        } else {
            rx->parse_state = MOQ_RX_AWAITING_OBJECT;
            rx->payload_written = 0;
            rx->payload_expected = 0;
        }
    } else {
        rx->parse_state = MOQ_RX_STREAMING_PAYLOAD;
    }

    return MOQ_OK;
}

static moq_result_t rx_emit_object(moq_session_t *s, int slot)
{
    moq_rx_stream_t *rx = &s->rx_streams[slot];
    rx_gate_on_verdict(s, rx);

    /* Prohibited object (frozen at header admission): consume without
     * surfacing OBJECT_RECEIVED -- release the assembled payload/extensions
     * (with the same budget bookkeeping as rx_free_entry) and take the same
     * state transition as a successful emit. FIN teardown is unaffected. */
    if (rx->suppress_cur_object) {
        if (rx->payload_rcbuf) {
            moq_rcbuf_decref(rx->payload_rcbuf);
            if ((size_t)rx->payload_expected <= s->recv_payload_bytes)
                s->recv_payload_bytes -= (size_t)rx->payload_expected;
            else
                s->recv_payload_bytes = 0;
            rx->payload_rcbuf = NULL;
            rx->payload_buf = NULL;
        }
        if (rx->cur_extensions) {
            size_t ext_len = moq_rcbuf_len(rx->cur_extensions);
            if (ext_len <= s->recv_payload_bytes)
                s->recv_payload_bytes -= ext_len;
            else
                s->recv_payload_bytes = 0;
            moq_rcbuf_decref(rx->cur_extensions);
            rx->cur_extensions = NULL;
        }
        rx->payload_written = 0;
        if (rx->pending_fin && rx->hdr_len == 0 && rx->input_len == 0)
            return rx_finish_stream(s, slot);
        rx->parse_state = MOQ_RX_AWAITING_OBJECT;
        return MOQ_OK;
    }

    if (rx->fan_shared) return rx_fan_emit_object(s, slot);

    if (event_queue_full(s)) {
        rx->parse_state = MOQ_RX_PENDING_EMIT;
        return MOQ_ERR_WOULD_BLOCK;
    }

    moq_rcbuf_t *payload = NULL;
    if (rx->payload_rcbuf) {
        /* Payload was assembled directly into this rcbuf. It stays owned by rx
         * until push_event succeeds, so a WOULD_BLOCK retry re-emits the same
         * buffer without rebuilding it. */
        payload = rx->payload_rcbuf;
    } else if (rx->cur_status == MOQ_OBJECT_NORMAL) {
        /* Zero-length normal object: a distinct empty rcbuf, created here and
         * owned locally (not by rx) until the push succeeds. */
        moq_result_t rc = moq_rcbuf_create(&s->alloc, NULL, 0, &payload);
        if (rc < 0) {
            rx->parse_state = MOQ_RX_PENDING_EMIT;
            return rc;
        }
    }

    moq_event_t e;
    memset(&e, 0, sizeof(e));
    e.kind = MOQ_EVENT_OBJECT_RECEIVED;
    e.detail_size = (uint32_t)sizeof(moq_object_received_event_t);
    e.borrow_epoch = s->borrow_epoch;
    e.u.object_received.sub = rx->sub;
    e.u.object_received.pub = rx->pub_handle;
    e.u.object_received.group_id = rx->group_id;
    e.u.object_received.subgroup_id = rx->subgroup_id;
    e.u.object_received.object_id = rx->cur_object_id;
    e.u.object_received.publisher_priority = session_resolve_publisher_priority(
        s, rx->sub, rx->pub_handle, rx->default_priority, rx->publisher_priority);
    e.u.object_received.status = rx->cur_status;
    e.u.object_received.end_of_group = rx->end_of_group;
    e.u.object_received.first_object = rx->cur_first_object;
    e.u.object_received.subgroup_properties = rx->has_extensions;
    e.u.object_received.payload = payload;
    e.u.object_received.properties = rx->cur_extensions;

    moq_result_t rc = push_event(s, &e);
    if (rc < 0) {
        /* Keep a pre-assembled payload owned by rx for the PENDING_EMIT retry;
         * only free a payload created locally in this call (the empty-object
         * rcbuf). Never decref rx->payload_rcbuf here -- teardown owns it. */
        if (payload && payload != rx->payload_rcbuf)
            moq_rcbuf_decref(payload);
        rx->parse_state = MOQ_RX_PENDING_EMIT;
        return rc;
    }

    rx->cur_extensions = NULL;

    /* Payload ownership transferred to the queued event; payload_buf pointed
     * into that rcbuf, so drop the references without freeing. */
    rx->payload_rcbuf = NULL;
    rx->payload_buf = NULL;
    rx->payload_written = 0;

    /* (Received-object history is fed at object ADMISSION in the header-parse
     * loop, before delivery -- see the track_hist_merge there -- so both the
     * streaming and non-streaming emit paths are covered.) */

    /* Handle deferred FIN: if no more buffered data, finish now (emitting
     * SUBGROUP_FINISHED after this final object). */
    if (rx->pending_fin && rx->hdr_len == 0 && rx->input_len == 0) {
        return rx_finish_stream(s, slot);
    }

    rx->parse_state = MOQ_RX_AWAITING_OBJECT;
    return MOQ_OK;
}

/* Append caller bytes to the rx-owned input_buf for `slot`, enforcing the shared
 * receive budget (max_recv_buf covers retained payload + buffered input). On
 * success sets *out_appended = true and returns MOQ_OK. If the budget is
 * exceeded it issues a STOP (rx_try_stop, which may free the entry) and returns
 * that result with *out_appended = false; on allocation failure returns NOMEM
 * with *out_appended = false. In either failure case the caller must return the
 * value immediately -- the entry may already be gone. */
static moq_result_t rx_append_input(moq_session_t *s, int slot,
                                    const uint8_t *data, size_t len,
                                    bool *out_appended)
{
    moq_rx_stream_t *rx = &s->rx_streams[slot];
    *out_appended = false;

    size_t budget = s->max_recv_buf;
    if (len > SIZE_MAX - rx->input_len)
        return rx_try_stop(s, slot);
    size_t needed = rx->input_len + len;
    /* Only capacity growth beyond what this stream already holds consumes new
     * receive budget. The current input_cap is already charged to
     * recv_input_bytes (and stays charged across drains now that input_buf is
     * retained for reuse), so bytes that fit within it add no memory and must
     * not be re-charged -- otherwise a retained buffer would shrink the
     * effective budget for the next object on the same stream. When input_cap
     * is 0 (no retained buffer) addl == len, matching the prior check. */
    size_t addl = needed > rx->input_cap ? needed - rx->input_cap : 0;
    if (s->recv_payload_bytes >= budget ||
        s->recv_input_bytes > budget - s->recv_payload_bytes ||
        addl > budget - s->recv_payload_bytes - s->recv_input_bytes) {
        return rx_try_stop(s, slot);
    }
    if (needed > rx->input_cap) {
        size_t old_cap = rx->input_cap;
        /* Grow geometrically (double) to amortize repeated small appends into
         * O(log n) reallocs instead of one exact-fit realloc per chunk. Bound
         * the new capacity by what the receive budget still allows for THIS
         * buffer: cap_ceiling excludes this stream's own old_cap (already part
         * of recv_input_bytes) so it measures headroom against other consumers.
         * The budget check above proved `len` fits, which makes cap_ceiling
         * >= needed, so the clamp never drops below what this append requires. */
        size_t cap_ceiling = budget - s->recv_payload_bytes
                             - (s->recv_input_bytes - old_cap);
        size_t new_cap = old_cap ? old_cap : needed;
        while (new_cap < needed) {
            if (new_cap > SIZE_MAX / 2) { new_cap = needed; break; }
            new_cap *= 2;
        }
        if (new_cap > cap_ceiling)
            new_cap = cap_ceiling;
        uint8_t *nb;
        if (rx->input_buf) {
            nb = (uint8_t *)s->alloc.realloc(
                rx->input_buf, old_cap, new_cap, s->alloc.ctx);
        } else {
            nb = (uint8_t *)s->alloc.alloc(new_cap, s->alloc.ctx);
        }
        if (!nb) return MOQ_ERR_NOMEM;
        rx->input_buf = nb;
        rx->input_cap = new_cap;
        s->recv_input_bytes += new_cap - old_cap;
    }
    memcpy(rx->input_buf + rx->input_len, data, len);
    rx->input_len += len;
    *out_appended = true;
    return MOQ_OK;
}

/* Resolve the rx stream for an inbound data delivery: return an existing entry,
 * or create one for a fresh stream. Returns the slot (>= 0) when the caller
 * should proceed with parsing, or -1 when the delivery is already fully handled
 * -- in which case *out_rc holds the result to return (session closed, an
 * ignored empty non-FIN probe, data-after-FIN close, stopped-stream discard,
 * or an unowned admission WOULD_BLOCK). */
static int rx_get_or_create_stream(moq_session_t *s, moq_stream_ref_t stream_ref,
                                   size_t len, bool fin, moq_result_t *out_rc)
{
    if (!session_is_active(s)) { *out_rc = MOQ_ERR_CLOSED; return -1; }

    int slot = rx_find_by_ref(s, stream_ref);
    if (slot >= 0) return slot;          /* existing stream: proceed */

    /* Stopped without an entry: consumed unparsed; the FIN releases the
     * retained identity (not a completion, nothing recorded). */
    int stopped = rx_stopped_ref_find(s, stream_ref._v);
    if (stopped >= 0) {
        if (fin) {
            rx_stopped_ref_remove(s, stopped);
            rx_note_finished_ref(s, stream_ref._v);
        }
        *out_rc = MOQ_OK;
        return -1;
    }

    /* Unknown stream. */
    if (len == 0 && !fin) { *out_rc = MOQ_OK; return -1; }
    if (rx_is_finished(s, stream_ref._v)) {
        *out_rc = close_with_error(s, 0x3, "data after FIN");
        return -1;
    }
    slot = rx_find_free(s);
    if (slot < 0) {
        /* No parser owns this delivery, so its request is not yet known.
         * Consume neither bytes nor FIN: the caller must retain and replay
         * the exact delivery once an entry is available. Blindly stopping it
         * here would discard a FETCH response without settling its request. */
        *out_rc = MOQ_ERR_WOULD_BLOCK;
        return -1;
    }
    moq_rx_stream_t *rx = &s->rx_streams[slot];
    memset(rx, 0, sizeof(*rx));
    rx->occ_next = -1;
    rx->occ_prev = -1;
    /* Allocated: join the occupancy list the preamble scans walk. */
    rx_occ_link(s, (size_t)slot);
    rx->active = true;
    rx->stream_kind = MOQ_STREAM_KIND_UNKNOWN;
    rx->stream_ref = stream_ref;
    rx->parse_state = MOQ_RX_AWAITING_HEADER;
    moq_index_insert(s->idx_rx_by_ref, s->idx_rx_mask,
                      stream_ref._v, slot);
    return slot;                          /* new stream: proceed */
}

/* -- Data bytes handler -------------------------------------------- */

static moq_result_t handle_data_bytes_impl(moq_session_t *s,
                                            moq_stream_ref_t stream_ref,
                                            const uint8_t *data, size_t len,
                                            bool fin,
                                            moq_rcbuf_t *input_rcbuf)
{
    moq_result_t acquire_rc;
    int slot = rx_get_or_create_stream(s, stream_ref, len, fin, &acquire_rc);
    if (slot < 0) return acquire_rc;

    moq_rx_stream_t *rx = &s->rx_streams[slot];

    /* Stopped locally: whatever the peer still delivers is consumed unparsed;
     * its FIN releases the entry -- no completion event, but the stream is
     * processed and counts toward its binding's Stream Count gate. A stopped
     * fill stream's FIN still settles its child. */
    if (rx->parse_state == MOQ_RX_STOPPED) {
        if (fin) {
            bool fill = rx->fill_bound;
            if (fill) fill_rx_peer_fin(s, rx);
            rx_record_terminal_processed(s, slot);
            rx_note_finished_ref(s, rx->stream_ref._v);
            rx_free_entry(s, (size_t)slot);
            if (fill) fill_service(s);
        }
        return MOQ_OK;
    }

    /* The STOP is still owed from an earlier refusal. A FIN arriving now is
     * latched first: the entry is released at the STOP (or at this call's
     * retry) and never left waiting for a terminal already delivered. */
    if (rx->parse_state == MOQ_RX_NEED_STOP) {
        if (fin) rx->pending_fin = true;
        return rx_try_stop(s, slot);
    }

    /* All objects were emitted and the FIN seen, but SUBGROUP_FINISHED could not
     * be queued last time (event queue was full). No object bytes remain to
     * parse -- just re-attempt the finish. On success the entry is freed; a
     * still-full queue WOULD_BLOCKs again and the bridge re-drives the FIN. This
     * MUST run before the stale-binding check below: the object/chunk events
     * already transferred with the stored handles, so once we are pending-finish
     * the stream owes exactly one SUBGROUP_FINISHED even if the app frees the
     * subscription/publication in the meantime -- it must not degrade to a
     * STOP_DATA. */
    if (rx->parse_state == MOQ_RX_PENDING_FINISHED) {
        rx->pending_fin = true;
        return rx_finish_stream(s, slot);
    }

    /* If the bound subscription/publication was freed since this stream's header
     * bound it (e.g. a rejected pending PUBLISH), drop the stream instead of
     * emitting against a stale handle. The binding cannot change mid-call (it is
     * only freed by separate app calls), so one check per delivery suffices. */
    if (!rx_binding_alive(s, rx)) {
        if (fin) rx->pending_fin = true;
        return rx_stop_gone(s, slot);
    }

    /* If the stream is already awaiting a pending emit/chunk retry, retain any
     * newly delivered caller bytes (and a FIN) BEFORE retrying. The pending
     * retry can itself WOULD_BLOCK (the event queue is still full), so appending
     * first keeps the new bytes in rx->input_buf under the receive budget -- to
     * be parsed once the pending object/chunk drains -- instead of being silently
     * dropped (the transport does not re-deliver, and the bridge treats
     * WOULD_BLOCK here as "session retained these bytes"). Ordering holds: the
     * pending emit/chunk completes before the parse loop below consumes these
     * appended bytes, and a retained FIN finishes only once input_buf drains
     * (rx_emit_object / the loop check input_len == 0). An rcbuf input is copied
     * -- a backpressure path where simple lifetime beats zero-copy. */
    if ((len > 0 || fin) &&
        (rx->parse_state == MOQ_RX_PENDING_EMIT ||
         rx->parse_state == MOQ_RX_PENDING_CHUNK)) {
        /* Retain the FIN BEFORE the pending retry too: a FIN-only delivery
         * (len == 0) must set pending_fin even when the retry below WOULD_BLOCKs,
         * otherwise the bridge -- which marks fin_retained on that WOULD_BLOCK --
         * believes the FIN was retained while the session leaves the rx entry
         * active awaiting more data. */
        if (fin) rx->pending_fin = true;
        if (len > 0) {
            bool appended;
            moq_result_t arc = rx_append_input(s, slot, data, len, &appended);
            if (!appended) return arc;
            /* Consumed into input_buf; append/zero-copy paths below must skip. */
            data = NULL;
            len = 0;
            input_rcbuf = NULL;
        }
    }

    /* A parked subgroup terminal is owed: every drive route retries it,
     * including a generic delivery that carries no new bytes. */
    if (rx->parse_state == MOQ_RX_PENDING_RESET)
        return rx_drive_reset(s, slot);

    if (rx->parse_state == MOQ_RX_PENDING_EMIT) {
        moq_result_t erc = rx_emit_object(s, slot);
        if (erc < 0) return erc;
        if (!s->rx_streams[slot].active) return MOQ_OK;
        rx = &s->rx_streams[slot];
        /* The parked object is delivered; if a RESET was observed while it
         * waited, that obligation is still owed and must be completed here
         * rather than resuming ordinary parsing. */
        if (rx->reset_owed)
            return rx_drive_reset(s, slot);
    }

    if (rx->parse_state == MOQ_RX_PENDING_CHUNK) {
        bool was_end = rx->pending_end;
        size_t data_len = rx->pending_data_len;
        /* A non-normal terminal (e.g. a RESET deferred by handle_data_reset
         * when the event queue was full) ends the stream: once emitted, the
         * rx slot must be freed, not recycled to AWAITING_OBJECT as a normal
         * object completion would be. */
        bool was_terminal = rx->pending_terminal != MOQ_OBJECT_TERMINAL_NORMAL;

        moq_result_t erc = rx_push_pending_chunk(s, slot);
        if (erc < 0) return erc;
        if (!s->rx_streams[slot].active) return MOQ_OK;
        rx = &s->rx_streams[slot];

        /* Advance payload tracking for the bytes in this chunk. */
        rx->payload_written += data_len;
        rx->pending_begin = false;
        rx->pending_end = false;
        rx->pending_data_len = 0;

        /* Consume the chunk data bytes from input_buf (only for
         * input_buf-backed pending chunks, not slices). */
        bool from_input = rx->pending_from_input;
        rx->pending_from_input = false;
        if (from_input && data_len > 0 && rx->input_buf &&
            data_len <= rx->input_len) {
            size_t remaining = rx->input_len - data_len;
            if (remaining > 0)
                memmove(rx->input_buf, rx->input_buf + data_len, remaining);
            /* Retain drained capacity for reuse (see the compact: label).
             * input_cap stays charged to recv_input_bytes until the stream
             * is freed. */
            rx->input_len = remaining;
        }

        /* A RESET observed earlier is still owed: resuming ordinary parsing
         * here would strand the obligation. Normalize the just-drained chunk's
         * state FIRST -- it has been emitted, so the stream is either between
         * objects (its end shipped) or mid-object -- and only then hand to the
         * single driver. Handing over while the state still reads
         * PENDING_CHUNK would make the driver push the same chunk again and
         * enqueue a spurious empty continuation. */
        if (rx->reset_owed && !was_terminal) {
            rx->parse_state = was_end ? MOQ_RX_AWAITING_OBJECT
                                      : MOQ_RX_STREAMING_PAYLOAD;
            return rx_drive_reset(s, slot);
        }

        if (was_end) {
            if (was_terminal) {
                /* Terminal reset: the stream is gone (mirrors the direct
                 * handle_data_reset path). An identifiable publication-bound
                 * stream counts as processed, exactly like the direct path.
                 * A shared stream first closes the subscriptions this object
                 * did not reach (rx_drive_reset's rule). */
                if (rx->fan_shared) {
                    moq_result_t frc = rx_emit_subgroup_reset(s, slot,
                                                              rx->pending_reset_code);
                    if (frc < 0) return frc;
                }
                rx_record_terminal_processed(s, slot);
                rx_free_entry(s, (size_t)slot);
                return MOQ_OK;
            }
            if (rx->pending_fin && rx->input_len == 0) {
                return rx_finish_stream(s, slot);
            }
            rx->parse_state = MOQ_RX_AWAITING_OBJECT;
            rx->payload_written = 0;
            rx->payload_expected = 0;
        } else {
            rx->parse_state = MOQ_RX_STREAMING_PAYLOAD;
        }
    }

    if (fin) rx->pending_fin = true;

    /* Zero-copy fast path: emit a single continuation chunk directly
     * from the caller's rcbuf when the entire buffer fits in one chunk
     * and no tail needs preserving. */
    if (input_rcbuf && len > 0 && rx->input_len == 0 &&
        rx->parse_state == MOQ_RX_STREAMING_PAYLOAD) {
        size_t need = (size_t)rx->payload_expected - rx->payload_written;
        size_t chunk_len = len < need ? len : need;
        if (chunk_len > MOQ_STREAM_CHUNK_MAX)
            chunk_len = MOQ_STREAM_CHUNK_MAX;

        /* Only zero-copy if the entire rcbuf is consumed by this chunk
         * (no tail to preserve on WOULD_BLOCK). */
        if (chunk_len > 0 && chunk_len >= len) {
            bool final = (rx->payload_written + chunk_len >=
                (size_t)rx->payload_expected);

            moq_result_t erc = rx_emit_chunk(s, slot,
                data, chunk_len, false, final, input_rcbuf, false);
            if (erc < 0) return erc;
            if (!s->rx_streams[slot].active) return MOQ_OK;
            rx = &s->rx_streams[slot];
            rx->payload_written += chunk_len;

            /* A FIN that arrives before the declared payload is complete is a
             * truncated object. Mirror the slow path (the STREAMING_PAYLOAD
             * loop case): emit the partial chunk, then close. Without this the
             * rx slot stays active (STREAMING_PAYLOAD, pending_fin) and is
             * pinned -- MOQ_OK is returned but the stream never finishes. */
            if (rx->pending_fin && !final)
                return close_with_error(s, 0x3, "truncated payload at FIN");

            if (rx->pending_fin && rx->active &&
                rx->parse_state != MOQ_RX_STREAMING_PAYLOAD &&
                rx->input_len == 0) {
                return rx_finish_stream(s, slot);
            }
            return MOQ_OK;
        }
    }

    /* Track whether the input_buf contents came entirely from this
     * rcbuf (enables zero-copy begin chunk emission via offset mapping).
     * Only valid if no partial subgroup header was buffered from a prior
     * call — hdr_len > 0 breaks the cursor-to-rcbuf-offset mapping. */
    moq_rcbuf_t *input_src_rcbuf = NULL;
    if (input_rcbuf && rx->input_len == 0 && rx->hdr_len == 0 && len > 0)
        input_src_rcbuf = input_rcbuf;
    input_rcbuf = NULL;

    /* Append caller bytes to rx-owned input_buf before parsing. */
    if (len > 0) {
        bool appended;
        moq_result_t arc = rx_append_input(s, slot, data, len, &appended);
        if (!appended) return arc;
    }

    size_t cursor = 0;
    moq_result_t loop_rc = MOQ_OK;

    while (cursor < rx->input_len || (rx->pending_fin && rx->active)) {
        if (!rx->active) break;

        switch (rx->parse_state) {
        case MOQ_RX_DEFERRED_ALIAS:
            /* Alias not yet established by a SUBSCRIBE_OK: hold all buffered
             * bytes unparsed (compact + return). When the OK binds the alias,
             * this entry is flipped to AWAITING_OBJECT and re-driven. */
            goto compact;
        case MOQ_RX_AWAITING_HEADER: {
            size_t avail = rx->input_len - cursor;
            size_t space = MOQ_RX_HDR_BUF - rx->hdr_len;
            size_t copy = avail < space ? avail : space;
            if (copy > 0) {
                memcpy(rx->hdr_buf + rx->hdr_len, rx->input_buf + cursor, copy);
                rx->hdr_len += (uint8_t)copy;
                cursor += copy;
            }

            /* Classify stream kind from first byte(s) if not yet known. */
            if (rx->stream_kind == MOQ_STREAM_KIND_UNKNOWN && rx->hdr_len > 0) {
                moq_stream_kind_t k = (moq_stream_kind_t)
                    s->profile->classify_data_stream(rx->hdr_buf, rx->hdr_len);
                if (k == MOQ_STREAM_KIND_NEED_MORE) {
                    /* Leading stream type incomplete: buffer more and retry. */
                    if (rx->hdr_len >= MOQ_RX_HDR_BUF)
                        return close_with_error(s, 0x3, "unclassifiable data stream");
                    if (rx->pending_fin)
                        return close_with_error(s, 0x3,
                            "truncated data stream type at FIN");
                    goto compact;
                }
                rx->stream_kind = k;
                if (rx->stream_kind == MOQ_STREAM_KIND_UNKNOWN)
                    return close_with_error(s, 0x3, "unknown data stream type");
            }

            if (rx->stream_kind == MOQ_STREAM_KIND_FETCH) {
                moq_buf_reader_t r;
                moq_buf_reader_init(&r, rx->hdr_buf, rx->hdr_len);
                moq_decoded_fetch_stream_header_t fhdr;
                moq_result_t rc = s->profile->decode_fetch_header(s, &r, &fhdr);
                if (rc == MOQ_ERR_BUFFER) {
                    if (rx->hdr_len >= MOQ_RX_HDR_BUF)
                        return close_with_error(s, 0x3, "fetch header too large");
                    if (rx->pending_fin)
                        return close_with_error(s, 0x3, "truncated fetch header at FIN");
                    goto compact;
                }
                if (rc < 0)
                    return close_with_error(s, 0x3, "malformed fetch header");

                int fslot = fetch_find_by_request_id(s, fhdr.request_id);
                if (fslot < 0) {
                    /* A fill stream: the header names the SUBSCRIBE or
                     * REQUEST_UPDATE that asked for it. Bind the stream to that
                     * child; a child that can take no stream (its range
                     * resolved to none, or it already has one) stops this one. */
                    moq_result_t brc = fill_rx_bind(s, rx, fhdr.request_id);
                    if (brc == MOQ_OK) {
                        size_t fconsumed = moq_buf_reader_offset(&r);
                        uint8_t fleftover = rx->hdr_len - (uint8_t)fconsumed;
                        cursor -= fleftover;
                        rx->hdr_len = 0;
                        if (rx->pending_fin && cursor >= rx->input_len) {
                            fill_rx_peer_fin(s, rx);
                            moq_result_t frc = rx_finish_stream(s, slot);
                            fill_service(s);
                            return frc;
                        }
                        rx->parse_state = MOQ_RX_AWAITING_OBJECT;
                        continue;
                    }
                    if (brc == MOQ_ERR_PROTO) {
                        moq_result_t src = rx_try_stop(s, slot);
                        if (src < 0) loop_rc = src;
                        goto compact;
                    }
                    /* Late data uni for a locally-cancelled fetch (its slot was
                     * freed at cancel): stop the stream. Mark the rx so the
                     * cancel tombstone is consumed when the STOP is actually
                     * queued -- including a stop deferred via MOQ_RX_NEED_STOP
                     * and completed on a later retry -- never closing. */
                    if (fetch_cancel_tomb_contains(s, fhdr.request_id)) {
                        rx->stop_consumes_cancel_tomb = true;
                        rx->cancel_tomb_request_id = fhdr.request_id;
                        moq_result_t src = rx_try_stop(s, slot);
                        if (src < 0) { loop_rc = src; goto compact; }
                        goto compact;
                    }
                    return close_with_error(s, 0x3, "FETCH_HEADER unknown request ID");
                }

                moq_fetch_entry_t *fe = &s->fetches[fslot];
                if (fe->role != MOQ_FETCH_ROLE_FETCHER)
                    return close_with_error(s, 0x3, "FETCH_HEADER for non-fetcher");
                rx->effective_fetch_group_order = fe->effective_group_order;
                rx->fetch_order_known = true;

                /* Late data after a request-stream GOAWAY migration (§10.4):
                 * absorb the response WITHOUT stopping (graceful migration leaves
                 * the data uni alone). Release the migration tombstone and bind the
                 * already-invalidated handle so the object loop drops everything via
                 * the stale-handle path; the FIN then frees the rx context. */
                if (fe->state == MOQ_FETCH_GOAWAY_LOCAL) {
                    size_t gconsumed = moq_buf_reader_offset(&r);
                    uint8_t gleftover = rx->hdr_len - (uint8_t)gconsumed;
                    rx->fetch = fe->handle;        /* stale generation */
                    fetch_free_entry(s, fslot);    /* tombstone served its purpose */
                    cursor -= gleftover;           /* re-read object bytes */
                    rx->hdr_len = 0;
                    rx->parse_state = MOQ_RX_AWAITING_OBJECT;
                    continue;
                }

                /* A fetch binds exactly one response data stream. A FETCH_HEADER
                 * that arrives once a stream is already bound, on a *different*
                 * stream, is a protocol violation -- not a second response.
                 * Accepting it would overwrite data_stream_ref and could emit
                 * FETCH_COMPLETE / free the fetch while the original stream is
                 * still open. A re-feed of the same stream (e.g. a WOULD_BLOCK
                 * re-drive) keeps the same stream_ref and is fine; such retries
                 * also occur before data_stream_started is set, so this guard
                 * never rejects legitimate re-drive. (GOAWAY migration, handled
                 * above, frees the entry first, so it never reaches here.) */
                if (fe->data_stream_started &&
                    fe->data_stream_ref._v != stream_ref._v)
                    return close_with_error(s, 0x3,
                        "duplicate FETCH_HEADER on a second data stream");

                size_t consumed = moq_buf_reader_offset(&r);
                uint8_t leftover = rx->hdr_len - (uint8_t)consumed;
                size_t post_cursor = cursor - leftover;

                bool is_fin_complete = rx->pending_fin &&
                                       post_cursor >= rx->input_len;

                bool want_complete = is_fin_complete && fe->control_ok;
                if (want_complete && !s->profile->request_fin_is_not_cancel &&
                    !fetch_fetcher_completion_fits(s, (int)fslot, 0)) {
                    loop_rc = MOQ_ERR_WOULD_BLOCK;
                    goto compact;
                }

                /* Commit: safe to mutate — completion reserved if needed. Bound
                 * once; a draft-22 completion can retain this rx for FIN retry. */
                fe->data_stream_ref = stream_ref;
                fe->data_stream_started = true;
                rx->fetch = fe->handle;
                cursor = post_cursor;
                rx->hdr_len = 0;
                rx->parse_state = MOQ_RX_AWAITING_OBJECT;

                if (is_fin_complete) {
                    fe->data_stream_fin = true;
                    if (s->profile->request_fin_is_not_cancel) {
                        moq_result_t frc = fetch_finish_response(s, fslot);
                        if (frc < 0) { loop_rc = frc; goto compact; }
                    } else if (want_complete) {
                        moq_result_t erc2 =
                            fetch_fetcher_completion_commit(s, (int)fslot);
                        if (erc2 < 0) { loop_rc = erc2; goto compact; }
                    }
                    /* Fetch stream: rx_finish_stream skips the subgroup event. */
                    return rx_finish_stream(s, slot);
                }

                rx->parse_state = MOQ_RX_AWAITING_OBJECT;
                continue;
            }

            /* Subgroup header parse. */
            moq_buf_reader_t r;
            moq_buf_reader_init(&r, rx->hdr_buf, rx->hdr_len);
            moq_decoded_subgroup_header_t hdr;
            moq_result_t rc = s->profile->decode_subgroup_header(s, &r, &hdr);
            if (rc == MOQ_ERR_BUFFER) {
                if (rx->hdr_len >= MOQ_RX_HDR_BUF)
                    return close_with_error(s, 0x3, "subgroup header too large");
                if (rx->pending_fin) {
                    if (rx->hdr_len > 0)
                        return close_with_error(s, 0x3, "truncated subgroup header at FIN");
                    /* No subgroup header was parsed: headerless graceful FIN,
                     * no SUBGROUP_FINISHED (rx_finish_stream skips: unbound). */
                    return rx_finish_stream(s, slot);
                }
                goto compact;
            }
            if (rc < 0)
                return close_with_error(s, 0x3, "malformed subgroup header");

            int sub_slot = sub_find_by_alias_subscriber(s, hdr.track_alias);
            int pub_slot_rx = -1;
            if (sub_slot < 0)
                pub_slot_rx = pub_find_by_alias_subscriber(s,
                    hdr.track_alias);
            if (sub_slot < 0 && pub_slot_rx < 0) {
                /* Alias not yet established. If a forwarding subscription is
                 * pending, a SUBSCRIBE_OK may establish this alias imminently:
                 * defer the stream (retain its header, hold object bytes for
                 * control/data reordering) rather than stopping it. */
                if (session_has_forwarding_pending_subscriber(s)) {
                    rx->sub = MOQ_SUBSCRIPTION_INVALID;
                    rx->pub_handle = MOQ_PUBLICATION_INVALID;
                    rx->track_alias = hdr.track_alias;
                    rx->group_id = hdr.group_id;
                    rx->publisher_priority = hdr.publisher_priority;
                    rx->default_priority = hdr.default_priority;
                    rx->has_extensions = hdr.has_extensions;
                    rx->end_of_group = hdr.end_of_group;
                    rx->first_object = hdr.first_object;
                    rx->subgroup_id_from_first_object =
                        hdr.subgroup_id_from_first_object;
                    rx->subgroup_id = hdr.subgroup_id;
                    rx->subgroup_id_resolved = hdr.subgroup_id_resolved;
                    rx->parse_state = MOQ_RX_DEFERRED_ALIAS;
                    size_t consumed = moq_buf_reader_offset(&r);
                    uint8_t leftover = rx->hdr_len - (uint8_t)consumed;
                    cursor -= leftover;
                    rx->hdr_len = 0;
                    continue;
                }
                moq_result_t src = rx_try_stop(s, slot);
                if (src < 0) { loop_rc = src; goto compact; }
                goto compact;
            }

            rx->track_alias = hdr.track_alias;
            {
                moq_result_t brc = rx_bind_alias(s, rx, sub_slot, pub_slot_rx);
                if (brc < 0) { loop_rc = brc; goto compact; }
            }
            rx->group_id = hdr.group_id;
            rx->publisher_priority = hdr.publisher_priority;
            rx->default_priority = hdr.default_priority;
            rx->has_extensions = hdr.has_extensions;
            rx->end_of_group = hdr.end_of_group;
            rx->first_object = hdr.first_object;
            rx->subgroup_id_from_first_object = hdr.subgroup_id_from_first_object;
            rx->subgroup_id = hdr.subgroup_id;
            rx->subgroup_id_resolved = hdr.subgroup_id_resolved;

            rx->parse_state = MOQ_RX_AWAITING_OBJECT;

            size_t consumed = moq_buf_reader_offset(&r);
            uint8_t leftover = rx->hdr_len - (uint8_t)consumed;
            cursor -= leftover;
            rx->hdr_len = 0;
            continue;
        }

        case MOQ_RX_AWAITING_OBJECT: {
            if (rx->stream_kind == MOQ_STREAM_KIND_FETCH && rx->fill_bound) {
                size_t avail = rx->input_len - cursor;
                if (avail == 0 && rx->pending_fin) {
                    fill_rx_peer_fin(s, rx);
                    moq_result_t frc = rx_finish_stream(s, slot);
                    fill_service(s);
                    return frc;
                }
                if (avail == 0) goto compact;

                /* An inherited DEFAULT may need SUBSCRIBE_OK on a distinct
                 * request bidi. Retain bounded input, including FIN, without
                 * decoding until that control freezes the effective order. */
                if (!rx->fetch_order_known) goto compact;
                moq_buf_reader_t fr;
                moq_buf_reader_init(&fr, rx->input_buf + cursor, avail);
                moq_decoded_fetch_object_t fobj;
                moq_result_t forc = s->profile->decode_fetch_object(
                    s, &fr, rx->effective_fetch_group_order, &rx->fetch_prior, &fobj);
                if (forc == MOQ_ERR_BUFFER) {
                    if (rx->pending_fin)
                        return close_with_error(s, 0x3, "truncated fetch object at FIN");
                    goto compact;
                }
                if (forc < 0)
                    return close_with_error(s, 0x3, "malformed fetch object");

                /* Objects for a child that ended or was cancelled are consumed
                 * without events; the prior Location still advances so later
                 * deltas decode. */
                bool deliver = fill_rx_delivering(s, rx);
                /* §12.1: a fill carries its parent's Track, so a malformed
                 * object fails the parent subscription. */
                if (deliver && !fobj.is_range_marker &&
                    (rx_fill_parent_failing(s, slot) ||
                     rx_fetch_object_malformed(s, rx, &fobj, NULL) > 0))
                    return rx_fail_fill_malformed(s, slot);
                if (fobj.is_range_marker) {
                    if (deliver) {
                        if (event_queue_full(s)) { loop_rc = MOQ_ERR_WOULD_BLOCK; goto compact; }
                        moq_event_t gev;
                        memset(&gev, 0, sizeof(gev));
                        gev.kind = MOQ_EVENT_FILL_GAP;
                        gev.detail_size = (uint32_t)sizeof(moq_fill_gap_event_t);
                        gev.borrow_epoch = s->borrow_epoch;
                        fill_rx_identity(s, rx, &gev.u.fill_gap.fill, &gev.u.fill_gap.sub,
                                         &gev.u.fill_gap.pub,
                                         &gev.u.fill_gap.initiating_request_id);
                        gev.u.fill_gap.group_id = fobj.group_id;
                        gev.u.fill_gap.object_id = fobj.object_id;
                        gev.u.fill_gap.kind = fobj.range_kind;
                        moq_result_t gerc = push_event(s, &gev);
                        if (gerc < 0) { loop_rc = gerc; goto compact; }
                    }
                    rx->fetch_prior.has_prev = true;
                    rx->fetch_prior.group_id = fobj.group_id;
                    rx->fetch_prior.object_id = fobj.object_id;
                    cursor += moq_buf_reader_offset(&fr);
                    continue;
                }
                /* An object over this side's payload limit is a local
                 * resource refusal, not a peer error. §3.4.1: "A subscriber
                 * can cancel a fill fetch stream independently using
                 * STOP_SENDING" -- the child ends, the subscription goes on.
                 * CANCELLED (§12.5). */
                if (fobj.payload_len > s->max_obj_payload) {
                    rx->stop_cancels = true;
                    return rx_try_stop(s, slot);
                }
                if (deliver) {
                    size_t obj_budget = (size_t)fobj.payload_len + fobj.properties_len;
                    if (s->recv_payload_bytes + obj_budget > s->max_recv_buf)
                        return rx_try_stop(s, slot);
                    if (event_queue_full(s)) { loop_rc = MOQ_ERR_WOULD_BLOCK; goto compact; }
                    moq_rcbuf_t *fpayload = NULL;
                    if (fobj.payload_len > 0) {
                        const uint8_t *pdata = fr.data + fr.pos - (size_t)fobj.payload_len;
                        if (moq_rcbuf_create(&s->alloc, pdata, (size_t)fobj.payload_len,
                                             &fpayload) < 0) {
                            loop_rc = MOQ_ERR_NOMEM;
                            goto compact;
                        }
                    }
                    moq_rcbuf_t *fprops = NULL;
                    if (fobj.properties_len > 0 &&
                        moq_rcbuf_create(&s->alloc, fobj.properties, fobj.properties_len,
                                         &fprops) < 0) {
                        if (fpayload) moq_rcbuf_decref(fpayload);
                        loop_rc = MOQ_ERR_NOMEM;
                        goto compact;
                    }
                    moq_event_t oev;
                    memset(&oev, 0, sizeof(oev));
                    oev.kind = MOQ_EVENT_FILL_OBJECT;
                    oev.detail_size = (uint32_t)sizeof(moq_fill_object_event_t);
                    oev.borrow_epoch = s->borrow_epoch;
                    fill_rx_identity(s, rx, &oev.u.fill_object.fill, &oev.u.fill_object.sub,
                                     &oev.u.fill_object.pub,
                                     &oev.u.fill_object.initiating_request_id);
                    oev.u.fill_object.group_id = fobj.group_id;
                    oev.u.fill_object.subgroup_id = fobj.subgroup_id;
                    oev.u.fill_object.object_id = fobj.object_id;
                    oev.u.fill_object.publisher_priority = fobj.publisher_priority;
                    oev.u.fill_object.datagram = fobj.datagram;
                    oev.u.fill_object.payload = fpayload;
                    oev.u.fill_object.properties = fprops;
                    moq_result_t oerc = push_event(s, &oev);
                    if (oerc < 0) {
                        if (fpayload) moq_rcbuf_decref(fpayload);
                        if (fprops) moq_rcbuf_decref(fprops);
                        loop_rc = oerc;
                        goto compact;
                    }
                    s->recv_payload_bytes += obj_budget;
                }
                rx->fetch_prior.has_prev = true;
                rx->fetch_prior.has_actual = true;
                rx->fetch_prior.group_id = fobj.group_id;
                rx->fetch_prior.subgroup_id = fobj.subgroup_id;
                rx->fetch_prior.object_id = fobj.object_id;
                rx->fetch_prior.publisher_priority = fobj.publisher_priority;
                cursor += moq_buf_reader_offset(&fr);
                continue;
            }
            if (rx->stream_kind == MOQ_STREAM_KIND_FETCH) {
                size_t avail = rx->input_len - cursor;

                if (avail == 0 && rx->pending_fin) {
                    int fslot2 = fetch_resolve_handle(s, rx->fetch);
                    bool emit = false;
                    if (fslot2 >= 0) {
                        moq_fetch_entry_t *fe2 = &s->fetches[fslot2];
                        fe2->data_stream_fin = true;
                        emit = fe2->control_ok;
                    }
                    if (fslot2 >= 0 && s->profile->request_fin_is_not_cancel) {
                        moq_result_t frc = fetch_finish_response(s, fslot2);
                        if (frc < 0) { loop_rc = frc; goto compact; }
                    } else if (emit) {
                        if (!fetch_fetcher_completion_fits(s, fslot2, 0)) {
                            loop_rc = MOQ_ERR_WOULD_BLOCK;
                            goto compact;
                        }
                        moq_result_t ferc =
                            fetch_fetcher_completion_commit(s, fslot2);
                        if (ferc < 0) { loop_rc = ferc; goto compact; }
                    }
                    /* Fetch stream: rx_finish_stream skips the subgroup event. */
                    return rx_finish_stream(s, slot);
                }

                if (avail == 0) goto compact;

                moq_buf_reader_t fr;
                moq_buf_reader_init(&fr, rx->input_buf + cursor, avail);

                moq_decoded_fetch_object_t fobj;
                moq_result_t forc = s->profile->decode_fetch_object(
                    s, &fr, rx->effective_fetch_group_order, &rx->fetch_prior, &fobj);
                if (forc == MOQ_ERR_BUFFER) {
                    if (rx->pending_fin)
                        return close_with_error(s, 0x3,
                            "truncated fetch object at FIN");
                    goto compact;
                }
                if (forc < 0)
                    return close_with_error(s, 0x3,
                        "malformed fetch object");

                /* Stale handle check first — absorb late data after
                 * REQUEST_ERROR without needing an event slot. */
                if (fetch_resolve_handle(s, rx->fetch) < 0) {
                    rx->fetch_prior.has_prev = true;
                    rx->fetch_prior.group_id = fobj.group_id;
                    rx->fetch_prior.object_id = fobj.object_id;
                    if (!fobj.is_range_marker) {
                        rx->fetch_prior.has_actual = true;
                        rx->fetch_prior.subgroup_id = fobj.subgroup_id;
                        rx->fetch_prior.publisher_priority = fobj.publisher_priority;
                    }
                    cursor += moq_buf_reader_offset(&fr);
                    continue;
                }

                if (fobj.is_range_marker) {
                    if (event_queue_full(s)) {
                        loop_rc = MOQ_ERR_WOULD_BLOCK;
                        goto compact;
                    }
                    moq_event_t gev;
                    memset(&gev, 0, sizeof(gev));
                    gev.kind = MOQ_EVENT_FETCH_GAP;
                    gev.detail_size = (uint32_t)sizeof(moq_fetch_gap_event_t);
                    gev.borrow_epoch = s->borrow_epoch;
                    gev.u.fetch_gap.fetch = rx->fetch;
                    gev.u.fetch_gap.range_kind =
                        (moq_fetch_range_kind_t)fobj.range_kind;
                    gev.u.fetch_gap.group_id = fobj.group_id;
                    gev.u.fetch_gap.object_id = fobj.object_id;
                    moq_result_t gerc = push_event(s, &gev);
                    if (gerc < 0) { loop_rc = gerc; goto compact; }

                    /* An End-of-Range marker sets the prior Location only; the
                     * prior Subgroup/Priority (has_actual) come from the last
                     * actual object and are left untouched. */
                    rx->fetch_prior.has_prev = true;
                    rx->fetch_prior.group_id = fobj.group_id;
                    rx->fetch_prior.object_id = fobj.object_id;
                    cursor += moq_buf_reader_offset(&fr);
                    continue;
                }

                /* §12.1: a response that makes its Track malformed fails the
                 * fetch, not the session (§3.2.4 cancel: the request stream
                 * reset and stopped, this data stream stopped), and the object
                 * is not delivered. The cancel needs one event and up to
                 * three actions. */
                int fslot_mt = fetch_resolve_handle(s, rx->fetch);
                int fmal = (fslot_mt >= 0 && s->fetches[fslot_mt].mt_failing)
                    ? 1 : rx_fetch_object_malformed(s, rx, &fobj, &rx->fetch);
                if (fmal < 0) { loop_rc = MOQ_ERR_NOMEM; goto compact; }
                if (fmal > 0) {
                    uint64_t ref_v = rx->stream_ref._v;
                    if (fslot_mt >= 0) session_fail_malformed_fetch(s, fslot_mt);
                    if (s->state == MOQ_SESS_CLOSED) return MOQ_ERR_CLOSED;
                    return rx_stop_malformed(s, slot, ref_v);
                }

                /* Normal fetch object: payload was consumed by the decoder.
                 * An object over this side's payload limit is a local
                 * resource refusal, not a peer error: the fetch is cancelled
                 * (§3.2.4) -- its request is cancelled and this data stream
                 * stopped, both CANCELLED -- nothing is delivered for the
                 * object, and the application sees the fetch's terminal.
                 * The cancel needs one event and two actions: the request
                 * stream's abort (draft-16: FETCH_CANCEL) and this stream's
                 * STOP, reserved together so neither goes out alone. */
                if (fobj.payload_len > s->max_obj_payload) {
                    int fslot = fetch_resolve_handle(s, rx->fetch);
                    if (fslot >= 0) {
                        if (event_queue_full(s) || action_queue_avail(s) < 2) {
                            loop_rc = MOQ_ERR_WOULD_BLOCK;
                            goto compact;
                        }
                        moq_result_t crc = fetch_cancel_on_receive(s, fslot);
                        if (crc == MOQ_ERR_WOULD_BLOCK) {
                            loop_rc = crc;
                            goto compact;
                        }
                        if (crc < 0) return crc;
                    }
                    rx->stop_cancels = true;
                    return rx_try_stop(s, slot);
                }

                size_t obj_budget = (size_t)fobj.payload_len + fobj.properties_len;
                if (s->recv_payload_bytes + obj_budget > s->max_recv_buf)
                    return rx_try_stop(s, slot);

                if (event_queue_full(s)) {
                    loop_rc = MOQ_ERR_WOULD_BLOCK;
                    goto compact;
                }

                moq_rcbuf_t *fpayload = NULL;
                if (fobj.payload_len > 0) {
                    const uint8_t *pdata = fr.data + fr.pos -
                                            (size_t)fobj.payload_len;
                    moq_result_t crc2 = moq_rcbuf_create(&s->alloc,
                        pdata, (size_t)fobj.payload_len, &fpayload);
                    if (crc2 < 0) { loop_rc = MOQ_ERR_NOMEM; goto compact; }
                }

                moq_rcbuf_t *fprops = NULL;
                if (fobj.properties_len > 0) {
                    moq_result_t crc3 = moq_rcbuf_create(&s->alloc,
                        fobj.properties, fobj.properties_len, &fprops);
                    if (crc3 < 0) {
                        if (fpayload) moq_rcbuf_decref(fpayload);
                        loop_rc = MOQ_ERR_NOMEM;
                        goto compact;
                    }
                }

                moq_event_t oev;
                memset(&oev, 0, sizeof(oev));
                oev.kind = MOQ_EVENT_FETCH_OBJECT;
                oev.detail_size = (uint32_t)sizeof(moq_fetch_object_event_t);
                oev.borrow_epoch = s->borrow_epoch;
                oev.u.fetch_object.fetch = rx->fetch;
                oev.u.fetch_object.group_id = fobj.group_id;
                oev.u.fetch_object.subgroup_id = fobj.subgroup_id;
                oev.u.fetch_object.object_id = fobj.object_id;
                oev.u.fetch_object.publisher_priority = fobj.publisher_priority;
                oev.u.fetch_object.datagram = fobj.datagram;
                oev.u.fetch_object.payload = fpayload;
                oev.u.fetch_object.properties = fprops;

                moq_result_t oerc = push_event(s, &oev);
                if (oerc < 0) {
                    if (fpayload) moq_rcbuf_decref(fpayload);
                    if (fprops) moq_rcbuf_decref(fprops);
                    loop_rc = oerc;
                    goto compact;
                }

                s->recv_payload_bytes += obj_budget;

                rx->fetch_prior.has_prev = true;
                rx->fetch_prior.has_actual = true;   /* actual object metadata */
                rx->fetch_prior.group_id = fobj.group_id;
                rx->fetch_prior.subgroup_id = fobj.subgroup_id;
                rx->fetch_prior.object_id = fobj.object_id;
                rx->fetch_prior.publisher_priority = fobj.publisher_priority;
                cursor += moq_buf_reader_offset(&fr);
                continue;
            }

            size_t avail = rx->input_len - cursor;

            moq_buf_reader_t r;
            moq_buf_reader_init(&r,
                avail > 0 ? rx->input_buf + cursor : NULL, avail);

            moq_decoded_object_header_t obj;
            moq_result_t orc = s->profile->decode_object_header(s, &r,
                rx->has_extensions, rx->prev_object_id, rx->has_prev_object,
                &obj);
            if (orc == MOQ_ERR_BUFFER) {
                if (rx->pending_fin && avail == 0) {
                    /* Subgroup header parsed, no (more) objects: graceful FIN.
                     * Emits SUBGROUP_FINISHED (empty or post-final object). */
                    return rx_finish_stream(s, slot);
                }
                if (rx->pending_fin)
                    return close_with_error(s, 0x3, "truncated object header at FIN");
                goto compact;
            }
            if (orc == MOQ_ERR_PROTO)
                return close_with_error(s, 0x3, "malformed object header");
            if (orc < 0)
                return close_with_error(s, 0x3, "object header decode error");

            /* §12.1: an object that makes its Track malformed fails the
             * subscriptions carrying it, not the session, and is not
             * delivered. */
            if (s->profile->malformed_track_local) {
                uint8_t why = obj.malformed;
                /* A Track already failed delivers nothing more. */
                if (why == MOQ_MALFORMED_NONE &&
                    session_alias_failing(s, rx->track_alias))
                    why = MOQ_MALFORMED_TRACK_FAILED;
                if (why == MOQ_MALFORMED_NONE) {
                    moq_result_t trc;
                    moq_mt_track_t *t = mt_track_for_alias(s, rx->track_alias, &trc);
                    if (trc < 0) { loop_rc = trc; goto compact; }
                    moq_mt_object_t mo = {
                        .group_id = rx->group_id,
                        .object_id = obj.object_id,
                        .in_subgroup = true,
                        .priority = rx_object_priority(s, rx),
                        .priority_inherited = rx->default_priority,
                        .subgroup_id = (rx->subgroup_id_resolved ||
                                        !rx->subgroup_id_from_first_object)
                                           ? rx->subgroup_id : obj.object_id,
                        .status = obj.status,
                        .gaps = &obj.gaps,
                    };
                    why = mt_object(t, &mo);
                }
                if (why != MOQ_MALFORMED_NONE)
                    return rx_fail_malformed(s, slot);
            }

            /* A second subscription now shares this stream's alias: the
             * stream is attributed per object from here on. Then match this
             * object against every sharing subscription's filter. */
            if (!rx->fan_shared && rx->stream_kind == MOQ_STREAM_KIND_SUBGROUP &&
                (moq_subscription_is_valid(rx->sub) ||
                 moq_publication_is_valid(rx->pub_handle)) &&
                alias_attribution_shared(s, rx->track_alias)) {
                moq_result_t frc = rx_fan_enter(s, rx);
                if (frc < 0) { loop_rc = frc; goto compact; }
            }
            if (rx->fan_shared) {
                moq_result_t frc = rx_fan_match(s, rx, rx->group_id, obj.object_id);
                if (frc < 0) { loop_rc = frc; goto compact; }
            }

            moq_rcbuf_t *ext_rcbuf = NULL;
            if (obj.has_properties && obj.properties_len > 0) {
                moq_result_t erc = moq_rcbuf_create(&s->alloc,
                    obj.properties, obj.properties_len, &ext_rcbuf);
                if (erc < 0) { loop_rc = erc; goto compact; }
                s->recv_payload_bytes += obj.properties_len;
            }

            uint64_t object_id = obj.object_id;
            moq_object_status_t semantic = obj.status;
            uint64_t payload_len = obj.payload_len;

            if (!rx->subgroup_id_resolved &&
                rx->subgroup_id_from_first_object) {
                rx->subgroup_id = object_id;
                rx->subgroup_id_resolved = true;
            }

            if (payload_len > s->max_obj_payload) {
                if (ext_rcbuf) {
                    s->recv_payload_bytes -= moq_rcbuf_len(ext_rcbuf);
                    moq_rcbuf_decref(ext_rcbuf);
                }
                cursor += r.pos;
                loop_rc = rx_try_stop(s, slot);
                goto compact;
            }

            if (payload_len > 0) {
                size_t budget = s->max_recv_buf;
                if (s->recv_payload_bytes > budget ||
                    s->recv_input_bytes > budget - s->recv_payload_bytes ||
                    payload_len > budget - s->recv_payload_bytes - s->recv_input_bytes) {
                    if (ext_rcbuf) {
                        s->recv_payload_bytes -= moq_rcbuf_len(ext_rcbuf);
                        moq_rcbuf_decref(ext_rcbuf);
                    }
                    cursor += r.pos;
                    loop_rc = rx_try_stop(s, slot);
                    goto compact;
                }
            }

            rx->cur_object_id = object_id;
            rx->cur_status = semantic;
            /* FIRST_OBJECT speaks for the stream's first object only. */
            rx->cur_first_object = rx->first_object && !rx->has_prev_object;
            rx->prev_object_id = object_id;
            rx->has_prev_object = true;
            rx->payload_expected = payload_len;
            rx->payload_written = 0;
            rx->cur_extensions = ext_rcbuf;
            /* Freeze this object's suppression decision (Forward State at
             * ADMISSION): retained through chunks/retries/FIN/RESET; a
             * Forward flip affects the next object only. */
            rx->suppress_cur_object = rx_objects_suppressed(s, rx) ||
                rx_copy_in_flight_elsewhere(s, slot, object_id);

            /* Feed received-object history at ADMISSION --
             * covers both the streaming and non-streaming emit paths below.
             * Idempotent (monotonic max) so a re-driven parse is safe. */
            {
                int hslot = rx->fan_shared ? -1 : sub_resolve_handle(s, rx->sub);
                if (rx->fan_shared) {
                    moq_track_hist_t *th = rx_alias_track(s, rx->track_alias);
                    if (th) track_hist_merge(th, rx->group_id, object_id);
                } else if (hslot >= 0)
                    track_hist_merge(s->subs[hslot].hist,
                                     rx->group_id, object_id);
                else {
                    int pslot = pub_resolve_handle(s, rx->pub_handle);
                    if (pslot >= 0)
                        track_hist_merge(s->publishes[pslot].hist,
                                         rx->group_id, object_id);
                }
            }

            cursor += r.pos;

            if (s->streaming_objects) {
                /* Streaming mode: emit begin chunk, possibly with
                 * initial payload data from the same input call. */
                size_t avail = rx->input_len - cursor;
                size_t need = (size_t)payload_len - rx->payload_written;
                size_t chunk_len = avail < need ? avail : need;
                if (chunk_len > MOQ_STREAM_CHUNK_MAX)
                    chunk_len = MOQ_STREAM_CHUNK_MAX;
                bool final = (rx->payload_written + chunk_len >=
                    (size_t)payload_len);
                bool is_begin_end = (payload_len == 0) || final;

                /* Zero-copy begin chunk emission: if input_buf came
                 * from a single rcbuf and this chunk consumes all
                 * remaining data (cursor + chunk_len == original len,
                 * no tail to preserve). */
                moq_rcbuf_t *begin_rcbuf = NULL;
                if (input_src_rcbuf && chunk_len > 0 &&
                    cursor + chunk_len == rx->input_len)
                    begin_rcbuf = input_src_rcbuf;

                moq_result_t erc = rx_emit_chunk(s, slot,
                    chunk_len > 0 ? rx->input_buf + cursor : NULL,
                    chunk_len, true, is_begin_end, begin_rcbuf, true);
                if (erc < 0) { loop_rc = erc; goto compact; }

                rx->payload_written += chunk_len;
                cursor += chunk_len;

                if (!s->rx_streams[slot].active) { loop_rc = MOQ_OK; goto compact; }
                rx = &s->rx_streams[slot];
                continue;
            }

            if (payload_len == 0) {
                moq_result_t erc = rx_emit_object(s, slot);
                if (erc < 0) { loop_rc = erc; goto compact; }
                continue;
            }

            /* Allocate the delivered payload rcbuf up front and stream the
             * object's bytes directly into its inline storage, so rx_emit_object
             * can hand it to the event without a copy. payload_buf is the
             * writable cursor into that rcbuf. */
            moq_result_t arc = moq_rcbuf_alloc_uninit(
                &s->alloc, (size_t)payload_len,
                &rx->payload_rcbuf, &rx->payload_buf);
            if (arc < 0) { loop_rc = arc; goto compact; }
            s->recv_payload_bytes += (size_t)payload_len;

            if (rx->payload_written >= (size_t)payload_len) {
                moq_result_t erc = rx_emit_object(s, slot);
                if (erc < 0) { loop_rc = erc; goto compact; }
                continue;
            }

            rx->parse_state = MOQ_RX_READING_PAYLOAD;
            continue;
        }

        case MOQ_RX_READING_PAYLOAD: {
            size_t avail = rx->input_len - cursor;
            size_t need = (size_t)rx->payload_expected - rx->payload_written;
            size_t copy = avail < need ? avail : need;
            if (copy > 0) {
                memcpy(rx->payload_buf + rx->payload_written, rx->input_buf + cursor, copy);
                rx->payload_written += copy;
                cursor += copy;
            }

            if (rx->payload_written >= (size_t)rx->payload_expected) {
                moq_result_t erc = rx_emit_object(s, slot);
                if (erc < 0) { loop_rc = erc; goto compact; }
                continue;
            }

            if (rx->pending_fin)
                return close_with_error(s, 0x3, "truncated payload at FIN");
            goto compact;
        }

        case MOQ_RX_STREAMING_PAYLOAD: {
            size_t avail = rx->input_len - cursor;
            size_t need = (size_t)rx->payload_expected - rx->payload_written;
            size_t chunk_len = avail < need ? avail : need;
            if (chunk_len > MOQ_STREAM_CHUNK_MAX)
                chunk_len = MOQ_STREAM_CHUNK_MAX;
            if (chunk_len == 0 && !rx->pending_fin) goto compact;

            bool final = (rx->payload_written + chunk_len >=
                (size_t)rx->payload_expected);

            if (chunk_len == 0 && rx->pending_fin && !final)
                return close_with_error(s, 0x3, "truncated payload at FIN");

            moq_result_t erc = rx_emit_chunk(s, slot,
                chunk_len > 0 ? rx->input_buf + cursor : NULL,
                chunk_len, false, final, input_rcbuf, true);
            if (erc < 0) { loop_rc = erc; goto compact; }

            rx = &s->rx_streams[slot];
            rx->payload_written += chunk_len;
            cursor += chunk_len;

            if (!s->rx_streams[slot].active) { loop_rc = MOQ_OK; goto compact; }
            continue;
        }

        default:
            goto compact;
        } /* switch */
    } /* while */

    if (rx->pending_fin && rx->active && cursor >= rx->input_len) {
        return rx_finish_stream(s, slot);
    }

compact:
    /* A stream STOPped inside the loop keeps its entry but no input; one
     * whose input was released (an owed STOP) has none to compact either. */
    if (rx->active && rx->input_buf && rx->parse_state != MOQ_RX_STOPPED &&
        cursor > 0) {
        size_t remaining = rx->input_len - cursor;
        if (remaining > 0)
            memmove(rx->input_buf, rx->input_buf + cursor, remaining);
        /* When the buffer fully drains, retain its capacity for the next
         * object on this stream instead of freeing it. A transport that
         * delivers each object as its own read (or splits header from
         * payload, as SimPair does) would otherwise re-allocate input_buf
         * per delivery; reuse removes that per-object staging alloc in the
         * steady-state receive path. The retained input_cap stays charged to
         * recv_input_bytes until rx_free_entry / session destroy, so the
         * receive budget still accounts for it exactly once -- which means a
         * retained buffer may reduce tight-buffer headroom for other consumers
         * (honest accounting: it is real allocated memory until the stream is
         * freed). rx_append_input charges only growth beyond input_cap, so
         * reusing this capacity for the next object adds no new budget. */
        rx->input_len = remaining;
    }

    return loop_rc;
}

moq_result_t handle_data_bytes(moq_session_t *s,
                                moq_stream_ref_t stream_ref,
                                const uint8_t *data, size_t len,
                                bool fin)
{
    return handle_data_bytes_impl(s, stream_ref, data, len, fin, NULL);
}

/* Classify a deferred-replay result and decide whether to stop the replay loop.
 * WOULD_BLOCK is the only retryable outcome here (the entry keeps resumed_deferred
 * set and a later advance re-drives it). Because replay has no external caller
 * holding the bytes and no return channel, a hard failure must be latched rather
 * than silently retried forever: MOQ_ERR_CLOSED means handle_data_bytes_impl
 * already latched a protocol close, and any other negative result (e.g.
 * MOQ_ERR_NOMEM) is latched here as an internal error. Returns true to stop. */
static bool replay_result_should_stop(moq_session_t *s, moq_result_t rc)
{
    if (rc >= 0 || rc == MOQ_ERR_WOULD_BLOCK)
        return false;
    if (rc != MOQ_ERR_CLOSED && session_is_active(s))
        (void)close_with_error(s, 0x1, "internal error replaying deferred objects");
    return true;
}

void session_resume_deferred_for_alias(moq_session_t *s, uint64_t alias)
{
    int sub_slot = sub_find_by_alias_subscriber(s, alias);
    if (sub_slot < 0) return;
    for (size_t i = 0; i < s->rx_cap; i++) {
        moq_rx_stream_t *rx = &s->rx_streams[i];
        if (!rx->active || rx->parse_state != MOQ_RX_DEFERRED_ALIAS ||
            rx->track_alias != alias)
            continue;
        /* Bind to the now-established subscription and drive the buffered
         * objects. Backpressure leaves the entry partially drained with
         * resumed_deferred set; session_retry_resumed_deferred finishes it. */
        /* With no memory for shared attribution the stream binds to the
         * first subscription; its first object's admission retries shared
         * mode (and reports NOMEM there, retryably). */
        if (rx_bind_alias(s, rx, sub_slot, -1) < 0)
            rx->sub = s->subs[sub_slot].handle;
        rx->parse_state = MOQ_RX_AWAITING_OBJECT;
        rx->resumed_deferred = true;
        moq_result_t rc =
            handle_data_bytes_impl(s, rx->stream_ref, NULL, 0, false, NULL);
        if (replay_result_should_stop(s, rc))
            return;
    }
}

void session_retry_resumed_deferred(moq_session_t *s)
{
    for (size_t i = 0; i < s->rx_cap; i++) {
        moq_rx_stream_t *rx = &s->rx_streams[i];
        if (!rx->active || !rx->resumed_deferred) continue;
        /* Fully drained and idle: clear the marker. */
        if (rx->input_len == 0 && !rx->pending_fin &&
            rx->parse_state != MOQ_RX_PENDING_EMIT &&
            rx->parse_state != MOQ_RX_PENDING_CHUNK) {
            rx->resumed_deferred = false;
            continue;
        }
        moq_result_t rc =
            handle_data_bytes_impl(s, rx->stream_ref, NULL, 0, false, NULL);
        if (replay_result_should_stop(s, rc))
            return;
    }
}

void session_discard_deferred_streams(moq_session_t *s)
{
    /* Walks the rx OCCUPANCY list: only active streams can be deferred, and the
     * successor is captured before the free that unlinks the entry. */
    for (int32_t i = s->rx_occ_head, nxt; i >= 0; i = nxt) {
        moq_rx_stream_t *rx = &s->rx_streams[i];
        nxt = rx->occ_next;
#ifdef MOQ_SESSION_SWEEP_TESTING
        session_work_deferred_rx_probes++;
#endif
        if (!rx->active || rx->parse_state != MOQ_RX_DEFERRED_ALIAS)
            continue;
        /* A STOP the action queue refuses stays owed on the entry
         * (NEED_STOP): it keeps the stream's identity so later bytes are
         * consumed, not parsed as a fresh stream, and session_retry_owed_stops
         * issues it once a poll returns action capacity. */
        (void)rx_try_stop(s, (int)i);
    }
}

void session_retry_owed_stops(moq_session_t *s)
{
    /* Walks the rx OCCUPANCY list; the successor is captured before a stop
     * that may free (unlink) the entry when its FIN was already latched. */
    for (int32_t i = s->rx_occ_head, nxt; i >= 0; i = nxt) {
        moq_rx_stream_t *rx = &s->rx_streams[i];
        nxt = rx->occ_next;
        if (!rx->active || rx->parse_state != MOQ_RX_NEED_STOP)
            continue;
        if (rx_try_stop(s, (int)i) == MOQ_ERR_WOULD_BLOCK)
            return;                      /* capacity exhausted again */
    }
}

moq_result_t handle_data_bytes_rcbuf(moq_session_t *s,
                                      moq_stream_ref_t stream_ref,
                                      moq_rcbuf_t *input_rcbuf,
                                      bool fin)
{
    const uint8_t *data = input_rcbuf ? moq_rcbuf_data(input_rcbuf) : NULL;
    size_t len = input_rcbuf ? moq_rcbuf_len(input_rcbuf) : 0;
    return handle_data_bytes_impl(s, stream_ref, data, len, fin, input_rcbuf);
}

/*
 * Complete the RESET obligation recorded on this stream. Every route that can
 * observe the reset -- a repeated on_data_reset, or the generic pending-chunk
 * path of on_data_bytes -- funnels here, so the terminal is emitted exactly
 * once with the ORIGINAL cause however the drive arrives.
 *
 * The two surfaces do not overlap, and the split is by VISIBILITY, not by
 * whether the parser has an object open: when a streaming object is in flight
 * AND its object events are surfaced, the closure is that object's
 * OBJECT_CHUNK(RESET). Otherwise no visible object terminal can carry it --
 * whole-object mode, a stream between objects or past its last one, or an
 * object suppressed by Forward State 0 -- so a bound subgroup with a resolved
 * ID reports SUBGROUP_RESET instead. A subgroup whose identity was never
 * resolved (FIRST_OBJECT header, no object) reports neither, rather than
 * fabricating subgroup 0.
 */
static moq_result_t rx_drive_reset(moq_session_t *s, int slot)
{
    moq_rx_stream_t *rx = &s->rx_streams[slot];
    const uint64_t code = rx->reset_error_code;

    /* A parked subgroup-level terminal: retry that emit, nothing else. */
    if (rx->parse_state == MOQ_RX_PENDING_RESET) {
        moq_result_t prc = rx_emit_subgroup_reset(s, slot, code);
        if (prc < 0) return prc;
        rx_record_terminal_processed(s, slot);
        rx_free_entry(s, (size_t)slot);
        return MOQ_OK;
    }

    /* A COMPLETE whole-object payload already parsed off the wire, whose
     * OBJECT_RECEIVED was refused, is not a partial the reset may discard: it
     * predates the reset and keeps its place in the event order. Emit it
     * first, then the subgroup terminal. A refusal here retains the
     * obligation, so the next drive resumes at this same point. */
    if (!s->streaming_objects && rx->parse_state == MOQ_RX_PENDING_EMIT) {
        moq_result_t oc = rx_emit_object(s, slot);
        if (oc < 0) return oc;
        if (!s->rx_streams[slot].active) return MOQ_OK;
        rx = &s->rx_streams[slot];
    }

    bool mid_object = s->streaming_objects &&
                      (rx->parse_state == MOQ_RX_STREAMING_PAYLOAD ||
                       rx->parse_state == MOQ_RX_PENDING_CHUNK);

    if (mid_object && rx->parse_state == MOQ_RX_PENDING_CHUNK &&
        rx->pending_terminal == MOQ_OBJECT_TERMINAL_NORMAL) {
        /* An already-pending NORMAL chunk is owed to the receiver first. A
         * refusal here keeps the obligation (captured by the caller) so the
         * next drive resumes at this same point. */
        bool was_final = rx->pending_end;
        moq_result_t rc = rx_push_pending_chunk(s, slot);
        if (rc < 0) return rc;
        rx = &s->rx_streams[slot];
        rx->pending_begin = false;
        rx->pending_end = false;
        rx->pending_data_len = 0;
        /* That chunk completed the object: no object remains in flight, so the
         * terminal below is the subgroup-level one, not a second chunk. */
        if (was_final) mid_object = false;
    }

    if (mid_object) {
        /* Arm the object's terminal chunk with the retained cause. A
         * suppressed object (Forward State 0 at its admission) consumes that
         * terminal WITHOUT surfacing it, so it cannot be the closure: the
         * subgroup -- already visible to the application from an earlier
         * object -- still needs exactly one signal, and falls through to the
         * subgroup terminal below. The resolved/bound guard there decides,
         * not whether an object event happened to be visible. */
        bool suppressed = rx->suppress_cur_object;
        /* The terminal is owed once to every subscription the object
         * matched; a re-drive of an already armed terminal keeps what was
         * served. */
        if (rx->fan_shared &&
            !(rx->parse_state == MOQ_RX_PENDING_CHUNK &&
              rx->pending_terminal == MOQ_OBJECT_TERMINAL_RESET))
            rx_fan_arm(rx);
        rx->pending_chunk = NULL;
        rx->pending_begin = false;
        rx->pending_end = true;
        rx->pending_terminal = MOQ_OBJECT_TERMINAL_RESET;
        rx->pending_reset_code = code;
        rx->pending_data_len = 0;
        rx->parse_state = MOQ_RX_PENDING_CHUNK;

        moq_result_t rc = rx_push_pending_chunk(s, slot);
        if (rc < 0) return rc;
        /* A shared stream also closes the subscriptions it reached earlier
         * that this object did not match. */
        if (suppressed || rx->fan_shared) {
            rx = &s->rx_streams[slot];
            moq_result_t erc = rx_emit_subgroup_reset(s, slot, code);
            if (erc < 0) return erc;
        }
    } else {
        /* No object in flight (whole-object mode, an idle streaming subgroup,
         * or a streaming object that just completed): the closure is the
         * subgroup terminal, emitted only for a resolved, bound subgroup. */
        moq_result_t erc = rx_emit_subgroup_reset(s, slot, code);
        if (erc < 0) return erc;
    }

    /* Counted only now: a refused terminal returned above and the retry
     * re-enters, so the count lands exactly once at the successful teardown. */
    rx_record_terminal_processed(s, slot);
    bool fill = rx->fill_bound;
    rx_free_entry(s, (size_t)slot);
    if (fill) fill_service(s);
    return MOQ_OK;
}

moq_result_t handle_data_reset(moq_session_t *s,
                                uint64_t error_code,
                                moq_stream_ref_t stream_ref)
{
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;

    int slot = rx_find_by_ref(s, stream_ref);
    if (slot < 0) {
        /* A stream stopped without an entry ends with its RESET. */
        int stopped = rx_stopped_ref_find(s, stream_ref._v);
        if (stopped >= 0) rx_stopped_ref_remove(s, stopped);
        return MOQ_OK;
    }

    moq_rx_stream_t *rx = &s->rx_streams[slot];

    /* The peer's RESET answers our STOP: the stopped entry ends here, with no
     * terminal to surface, and the stream counts toward its binding's Stream
     * Count gate -- the publisher counted it, and nothing more arrives. A
     * stopped fill stream's RESET still settles its child. */
    if (rx->parse_state == MOQ_RX_STOPPED) {
        bool fill = rx->fill_bound;
        if (fill) fill_rx_peer_reset(s, rx, error_code);
        rx_record_terminal_processed(s, slot);
        rx_free_entry(s, (size_t)slot);
        if (fill) fill_service(s);
        return MOQ_OK;
    }

    /* Capture the obligation BEFORE anything that can block: a refused flush
     * below must not lose the fact that a RESET is owed, nor its cause. The
     * FIRST peer code wins -- a later re-drive retries the stored obligation
     * and never overwrites it. */
    if (!rx->reset_owed) {
        rx->reset_owed = true;
        rx->reset_error_code = error_code;
    }
    return rx_drive_reset(s, slot);
}

/* A fill child's STOP_DATA was queued (session_fill.c): discard whatever the
 * peer still sends on the stream until its FIN or RESET, which settle the
 * child through the retained binding and count the stream on its parent. A
 * FIN that already arrived ends it now. */
void rx_fill_stopped(moq_session_t *s, moq_stream_ref_t ref)
{
    int slot = rx_find_by_ref(s, ref);
    if (slot < 0) return;
    moq_rx_stream_t *rx = &s->rx_streams[slot];
    if (rx->parse_state == MOQ_RX_STOPPED) return;
    rx_release_resources(s, (size_t)slot);
    if (rx->pending_fin) {
        fill_rx_peer_fin(s, rx);
        rx_record_terminal_processed(s, slot);
        rx_note_finished_ref(s, ref._v);
        rx_free_entry(s, (size_t)slot);
        return;
    }
    rx->fetch = (moq_fetch_t){ 0 };
    rx->stop_consumes_cancel_tomb = false;
    rx->reset_owed = false;
    rx->parse_state = MOQ_RX_STOPPED;
}

/* -- Local receive retirement --------------------------------------- *
 * The bridge's half is in transport_bridge.c. A stream this side stopped is
 * kept, as a STOPPED entry or an identity-only record, until the peer's FIN
 * or RESET; once the transport reports it finished with the stream after our
 * accepted STOP, neither will come. Release the owner then -- no event, no
 * FIN fact (rx_note_finished_ref is the peer's FIN only), no reset cause. A
 * still-bound entry is a processed stream for its binding's Stream Count,
 * counted once here exactly as the peer's terminal would have counted it; an
 * identity-only record lost its binding and counts nothing. A bound fill
 * child's stream is settled locally, keeping its cancellation's reason. */
moq_result_t bridge_session_on_data_retired(moq_session_t *s,
                                            moq_stream_ref_t ref,
                                            uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;
    moq_result_t arc = session_advance_entry(s, now_us);
    if (arc != MOQ_OK) return arc;
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;

    int slot = rx_find_by_ref(s, ref);
    if (slot >= 0) {
        moq_rx_stream_t *rx = &s->rx_streams[slot];
        if (rx->parse_state != MOQ_RX_STOPPED) return MOQ_ERR_WRONG_STATE;
        bool fill = rx->fill_bound;
        if (fill) fill_rx_local_retired(s, rx);
        rx_record_terminal_processed(s, slot);
        rx_free_entry(s, (size_t)slot);
        if (fill) fill_service(s);
        return MOQ_OK;
    }
    int stopped = rx_stopped_ref_find(s, ref._v);
    if (stopped >= 0) {
        rx_stopped_ref_remove(s, stopped);
        return MOQ_OK;
    }
    return MOQ_ERR_STALE_HANDLE;
}
