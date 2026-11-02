#include "bind_internal.h"
#include "subscription_update_internal.h"
#include "authorization_internal.h"
#include "delivery_internal.h"
#include "discovery_internal.h"
#include "fetch_joining_internal.h"
#include "testing_internal.h"

/* Legacy profiles use the relay's monotonic filter model. Omission means
 * unfiltered, not the relay request initializer's Largest default.
 * D22 passes the semantic value directly to relay-core. */
static moqr_result_t
bind_map_location_filter(const moq_location_filter_cfg_t *loc,
                         moqr_filter_t *out)
{
    moqr_subscribe_req_t defaults;
    moqr_subscribe_req_init(&defaults);
    *out = defaults.filter;
    switch (loc->kind) {
    case MOQ_LOCATION_FILTER_NONE:
        out->type = MOQR_FILTER_ABSOLUTE_START;
        out->start_group = 0;
        out->start_object = 0;
        return MOQR_OK;
    case MOQ_LOCATION_FILTER_NEXT_OBJECT:
        out->type = MOQR_FILTER_LARGEST_OBJECT;
        return MOQR_OK;
    case MOQ_LOCATION_FILTER_RELATIVE_GROUP:
        if (loc->relative_group_offset == 0) {
            out->type = MOQR_FILTER_NEXT_GROUP_START;
            return MOQR_OK;
        }
        if (loc->relative_group_offset == 1) {
            out->type = MOQR_FILTER_CURRENT_GROUP_START;
            return MOQR_OK;
        }
        out->type = MOQR_FILTER_RELATIVE_START;
        out->start_group = loc->relative_group_offset;
        return MOQR_OK;
    case MOQ_LOCATION_FILTER_ABSOLUTE_START:
        out->type = MOQR_FILTER_ABSOLUTE_START;
        out->start_group = loc->start_group;
        out->start_object = loc->start_object;
        return MOQR_OK;
    case MOQ_LOCATION_FILTER_GROUP_END:
        if (loc->end_group < loc->start_group) {
            return MOQR_ERR_INVAL;
        }
        out->type = MOQR_FILTER_ABSOLUTE_RANGE;
        out->start_group = loc->start_group;
        out->start_object = loc->start_object;
        out->end_group_delta = loc->end_group - loc->start_group;
        return MOQR_OK;
    case MOQ_LOCATION_FILTER_OBJECT_END:
    default:
        return MOQR_ERR_UNSUPPORTED;
    }
}


static void
bind_location_for_test(moq_location_filter_cfg_t *loc)
{
#ifdef MOQR_BIND_TESTING
    uint32_t forced = atomic_exchange(&bind_dbg_update_filter_kind, 0);
    if (forced != 0) {
        loc->kind = forced;
        loc->relative_group_offset = 2;
        loc->end_group = loc->start_group;
        loc->end_object = 0;
    }
#else
    (void)loc;
#endif
}


/* Legacy profiles retain the old relay filter contract. */
static moqr_result_t
bind_map_update_filter(moq_location_filter_cfg_t *loc, moqr_filter_t *out)
{
    bind_location_for_test(loc);
    return bind_map_location_filter(loc, out);
}


/* The filter in effect on a downstream subscription as the session knows it
 * (the request's, or the replacement an accepted update carried), onto the
 * relay filter. */
moqr_result_t
bind_read_sub_filter(b_conn_t *cn, moq_subscription_t sub, moqr_filter_t *out)
{
    moq_location_filter_cfg_t loc;
    moq_location_filter_cfg_init(&loc);
    if (moq_session_subscription_filter(cn->session, sub, &loc) != MOQ_OK) {
        moqr_subscribe_req_t defaults;
        moqr_subscribe_req_init(&defaults);
        *out = defaults.filter;
        return MOQR_ERR_INVAL;
    }
    return bind_map_update_filter(&loc, out);
}


moqr_result_t
bind_read_sub_location(b_conn_t *cn, moq_subscription_t sub,
                       moq_location_filter_cfg_t *out)
{
    moq_location_filter_cfg_init(out);
    if (moq_session_subscription_filter(cn->session, sub, out) != MOQ_OK) {
        return MOQR_ERR_INVAL;
    }
    bind_location_for_test(out);
    return MOQR_OK;
}


/* Whether this connection's draft keeps a subscription established once
 * Largest Object passes the end of its Location Filter. Draft-16/18 end it
 * with PUBLISH_DONE SUBSCRIPTION_ENDED; draft-22 removed that status and its
 * publisher does not end the subscription for that reason. */
bool
bind_range_end_holds(const b_conn_t *cn)
{
    return cn->wire_draft == B_DRAFT_22;
}


/* Whether a subscription update on this connection's draft carries a
 * replacement Location Filter the relay applies. Draft-22's REQUEST_UPDATE
 * carries LOCATION_FILTER as a parameter; the draft-16/18 SUBSCRIBE_UPDATE
 * window is left to the session, as before. */
bool
bind_update_filter_applies(const b_conn_t *cn)
{
    return cn->wire_draft == B_DRAFT_22;
}


/* Typed UPDATE selections, not the inactive legacy projections. An empty or
 * uncovered record selects no replacement; it never restores a legacy filter. */
const moq_subscription_update_options_t *
bind_update_options(const moq_event_t *ev)
{
    const moq_subscribe_updated_event_t *up = &ev->u.subscribe_updated;
    if (ev->detail_size < offsetof(moq_subscribe_updated_event_t, options) +
                              sizeof(up->options) ||
        up->options == NULL ||
        up->options->struct_size <
            offsetof(moq_subscription_update_options_t, location) +
                sizeof(up->options->location)) {
        return NULL;
    }
    return up->options;
}


/* An automatic ACK no longer has an owed query. Its event must retain the
 * exact processing-time replacement, never re-resolve raw relative intent. */
moqr_result_t
bind_update_resolved_location(const moq_subscription_update_options_t *options,
                              uint64_t id, moq_location_filter_cfg_t *out)
{
    if (options == NULL || options->struct_size <
            offsetof(moq_subscription_update_options_t, resolution) +
                sizeof(options->resolution)) {
        return MOQR_ERR_INVAL;
    }
    const moq_update_resolution_t *r = &options->resolution;
    if (r->struct_size < MOQ_UPDATE_RESOLUTION_V0_SIZE ||
        r->update_request_id != id ||
        !(r->flags & MOQ_UPDATE_RESOLUTION_LOCATION_REPLACEMENT) ||
        r->location.struct_size < MOQ_LOCATION_FILTER_CFG_V0_SIZE) {
        return MOQR_ERR_INVAL;
    }
    *out = r->location;
    bind_location_for_test(out);
    return MOQR_OK;
}


/* The End of Range kind for Locations a fill will not serve and does not
 * wait for. An unmarked gap in a fill fetch stream says the skipped Objects
 * do not exist, so a Location the relay cannot vouch for is always marked:
 * Timed-Out once the fill's wait budget is spent (9.20.5: with a budget of 0
 * "any unavailable Objects" are), Unknown before that, for Objects whose
 * state the relay cannot determine and has no source to wait on (3.2.2). */
moq_fetch_range_kind_t
bind_fill_unserved_kind(bool budget_spent)
{
    return budget_spent ? MOQ_FETCH_RANGE_TIMED_OUT : MOQ_FETCH_RANGE_UNKNOWN;
}


static bool
bind_update_answer_blocked_for_test(void)
{
#ifdef MOQR_BIND_TESTING
    if (atomic_load(&bind_dbg_block_update_answers) > 0) {
        (void)atomic_fetch_sub(&bind_dbg_block_update_answers, 1);
        return true;
    }
#endif
    return false;
}


void
bind_dsub_release(moqr_bind_t *b, b_conn_t *cn, b_dsub_t *ds)
{
    (void)b;
    (void)cn;
    memset(ds, 0, sizeof(*ds));
}


/* The downstream request is over on the session's side: the session ended
 * it itself (a refusal's UPDATE_FAILED terminal, or a terminal it holds
 * behind owed resets), or the peer cancelled or refused it. Whatever its open
 * data streams are still owed -- their resets, a terminal -- is the session's
 * to send. The binding releases its subgroup slots for exactly this request
 * (its own handle; a later request in the same session slot is another
 * value), each with its write progress and any owed reset cleared and
 * capacity rearmed once, and retires the relay subscription. Nothing is
 * written. */
void
bind_dsub_retire(moqr_bind_t *b, b_conn_t *cn, b_dsub_t *ds, uint64_t now_us)
{
    for (uint32_t i = 0; i < b->n_sgs; i++) {
        if (cn->sgs[i].used && cn->sgs[i].sub_raw == ds->sub_raw) {
            cn->sgs[i].used = false;
            cn->sgs[i].obj_begun = false;
            cn->sgs[i].obj_id = 0;
            cn->sgs[i].next_chunk = 0;
            cn->sgs[i].reset_owed = false;
            cn->sgs[i].reset_wire = 0;
            bind_dl_rearm_on_slot_release(b, cn);
        }
    }
    (void)moqr_core_unsubscribe(b->core, ds->rsub, now_us);
    bind_dsub_release(b, cn, ds);
}


/* Keep D22 downstream histories current even when Forward or a filter prevents
 * delivery. UPDATE snapshots Largest at message processing, so noting it only
 * when answering is too late. The note is non-advancing and monotonic; later
 * ingest cannot replace a snapshot already held for an owed answer. */
void
bind_note_relay_largest(moqr_bind_t *b, moqr_track_t track)
{
    for (uint32_t ci = 0; ci < b->max_conns; ci++) {
        b_conn_t *cn = &b->conns[ci];
        if (!cn->used || cn->closed || cn->session == NULL ||
            cn->wire_draft != B_DRAFT_22) {
            continue;
        }
        for (uint32_t i = 0; i < b->n_dsubs; i++) {
            b_dsub_t *ds = &cn->dsubs[i];
            moqr_track_t sub_track = MOQR_TRACK_INVALID;
            uint64_t track_gen = 0;
            if (!ds->used ||
                moqr_core_sub_track(b->core, ds->rsub, &sub_track, &track_gen) !=
                    MOQR_OK ||
                !moqr_track_eq(track, sub_track)) {
                continue;
            }
            bool has = false;
            uint64_t lg = 0, lo = 0;
            moq_bytes_t parts[32];
            moqr_ns_t ns;
            moq_bytes_t name;
            if (moqr_core_track_largest(b->core, track, track_gen, &has, &lg,
                                        &lo) == MOQR_OK && has &&
                moqr_core_track_key(b->core, track, track_gen, parts, 32, &ns,
                                    &name) == MOQR_OK) {
                const moq_namespace_t sns = { .parts = ns.parts, .count = ns.count };
                (void)moq_session_note_object_published(cn->session, &sns, name,
                                                        lg, lo);
            }
            break;   /* one history per track per session */
        }
    }
}


/* Give the held answer on `ds`. An acceptance applies the held Forward
 * change to the relay subscription once the session has taken it; a
 * refusal releases the subscription's tracking. A subscription the session
 * no longer has, or that no longer owes this answer, owes nothing. */
static b_upd_give_t
bind_update_answer_give(moqr_bind_t *b, b_conn_t *cn, b_dsub_t *ds,
                        uint64_t now_us)
{
    if (ds->upd_answer == B_UPD_NONE) {
        return B_UPD_GIVEN;
    }
    moq_subscription_t sub = { ds->sub_raw };
    moq_result_t rc;
    bool accept = ds->upd_answer == B_UPD_ACCEPT;
    if (bind_update_answer_blocked_for_test()) {
        rc = MOQ_ERR_WOULD_BLOCK;
    } else if (accept) {
        moq_accept_subscription_update_cfg_t ac;
        moq_accept_subscription_update_cfg_init(&ac);
        ac.update_request_id = ds->upd_id;
        rc = ds->publication
                 ? moq_session_accept_publication_update(
                       cn->session,
                       (moq_publication_t){ ._opaque = ds->sub_raw }, &ac,
                       now_us)
                 : moq_session_accept_subscription_update(cn->session, sub,
                                                          &ac, now_us);
    } else {
        bool filter = ds->upd_answer == B_UPD_REJECT_FILTER;
        moq_request_error_t code = filter ? BIND_FILTER_REFUSAL_CODE
                                          : MOQ_REQUEST_ERROR_INTERNAL_ERROR;
        moq_reject_subscription_update_cfg_t rj;
        moq_reject_subscription_update_cfg_init(&rj);
        rj.update_request_id = ds->upd_id;
        rj.error_code = code;
        rj.reason = filter ? MOQ_BYTES_LITERAL(BIND_FILTER_REFUSAL_REASON)
                           : bind_subscribe_error_reason(code);
        rc = ds->publication
                 ? moq_session_reject_publication_update(
                       cn->session,
                       (moq_publication_t){ ._opaque = ds->sub_raw }, &rj,
                       now_us)
                 : moq_session_reject_subscription_update(cn->session, sub,
                                                          &rj, now_us);
    }
    if (rc == MOQ_ERR_WOULD_BLOCK) {
        return B_UPD_HELD;
    }
    bool has_forward = ds->upd_has_forward, forward = ds->upd_forward;
    ds->upd_answer = B_UPD_NONE;
    ds->upd_has_forward = false;
    ds->upd_forward = false;
    ds->upd_id = 0;
    if (rc != MOQ_OK) {
        /* STALE / WRONG_STATE / CLOSED: the subscription ended, or the
         * answer was already given, meanwhile. Anything else is a broken
         * invariant: count it. */
        if (!fwd_result_is_gone(rc)) {
            b->session_errors++;
        }
        return B_UPD_GIVEN;
    }
    if (!accept) {
        bind_dsub_retire(b, cn, ds, now_us);
        return B_UPD_ENDED;
    }
    if (has_forward) {
        moqr_result_t frc =
            moqr_core_sub_set_forward(b->core, ds->rsub, forward, now_us);
        if (frc != MOQR_OK && frc != MOQR_ERR_STALE_HANDLE) {
            b->session_errors++;
        }
    }
    return B_UPD_GIVEN;
}


/* Decide the answer to a downstream update the session left to the relay,
 * then give it (or hold it). The event carries the cumulative values of
 * every update the answer covers, so a later event for the same
 * subscription replaces a held decision.
 *
 * A selected semantic Location Filter is applied once to the relay
 * subscription before the acceptance is given, so the
 * acknowledgement follows the change. A Forward change is applied once the
 * acceptance is given -- the session's own forwarding state changes then.
 * Priority / delivery-timeout / new-group changes are not applied here. */
void
bind_update_answer_for(moqr_bind_t *b, b_conn_t *cn, uint64_t raw, uint64_t id,
                       bool has_forward, bool forward, bool resolve_filter,
                       uint64_t now_us)
{
    /* A subscription the relay began with PUBLISH (track discovery) is
     * updated through the same answer as one a SUBSCRIBE began. */
    const bool pub = bind_cookie_is_publication(raw);
    b_dsub_t *ds = NULL;
    for (uint32_t i = 0; i < b->n_dsubs; i++) {
        if (cn->dsubs[i].used && cn->dsubs[i].sub_raw == raw) {
            ds = &cn->dsubs[i];
            break;
        }
    }
    if (ds == NULL) {
        /* No tracked subscription: the relay already released it (its
         * terminal crossed this update) or never tracked it. The session
         * may still owe the answer; refuse so the subscription ends rather
         * than wait forever. Gone means nothing is owed; a refusal that
         * lands is a broken invariant and is counted. */
        moq_reject_subscription_update_cfg_t rj;
        moq_reject_subscription_update_cfg_init(&rj);
        rj.update_request_id = id;
        rj.error_code = MOQ_REQUEST_ERROR_INTERNAL_ERROR;
        rj.reason = bind_subscribe_error_reason(rj.error_code);
        moq_result_t rc = pub
            ? moq_session_reject_publication_update(
                  cn->session, (moq_publication_t){ ._opaque = raw }, &rj, now_us)
            : moq_session_reject_subscription_update(
                  cn->session, (moq_subscription_t){ ._opaque = raw }, &rj, now_us);
        if (!fwd_result_is_gone(rc)) {
            b->session_errors++;
        }
        return;
    }
    bool was_held = ds->upd_answer != B_UPD_NONE;
    ds->upd_id = id;
    ds->upd_has_forward = has_forward;
    ds->upd_forward = forward;
    ds->upd_answer = B_UPD_ACCEPT;
    if (resolve_filter) {
        moq_update_resolution_t resolution;
        moqr_result_t mrc = MOQR_ERR_INVAL;
        moq_result_t qrc = pub
            ? moq_session_get_publication_update_resolution(cn->session,
                  (moq_publication_t){ ._opaque = raw }, id, &resolution,
                  sizeof(resolution))
            : moq_session_get_subscription_update_resolution(cn->session,
                  (moq_subscription_t){ ._opaque = raw }, id, &resolution,
                  sizeof(resolution));
        if (qrc == MOQ_OK) {
            mrc = MOQR_OK;
            if (resolution.flags & MOQ_UPDATE_RESOLUTION_LOCATION_REPLACEMENT) {
                bind_location_for_test(&resolution.location);
                mrc = moqr_core_sub_set_location_filter(b->core, ds->rsub,
                                                       &resolution.location, now_us);
            }
        }
        if (mrc != MOQR_OK && mrc != MOQR_ERR_STALE_HANDLE) {
            ds->upd_answer = mrc == MOQR_ERR_UNSUPPORTED || mrc == MOQR_ERR_INVAL
                                 ? B_UPD_REJECT_FILTER
                                 : B_UPD_REJECT_INTERNAL;
            if (mrc != MOQR_ERR_INVAL && mrc != MOQR_ERR_UNSUPPORTED &&
                mrc != MOQR_ERR_NOMEM) {
                b->session_errors++;
            }
        }
    }
    if (bind_update_answer_give(b, cn, ds, now_us) == B_UPD_HELD) {
        if (!was_held) {
            b->upd_answers_held++;
        }
    }
}


void
bind_update_answer(moqr_bind_t *b, b_conn_t *cn,
                   const moq_subscribe_updated_event_t *up, bool resolve_filter,
                   uint64_t now_us)
{
    bind_update_answer_for(b, cn, up->sub._opaque, up->update_request_id,
                           up->has_forward, up->forward, resolve_filter,
                           now_us);
}


/* Retry every held update answer, once per pump. Recounts the held set, so
 * answers left on a connection that has since gone stop being counted. */
void
bind_update_answer_pump(moqr_bind_t *b, uint64_t now_us)
{
    if (b->upd_answers_held == 0) {
        return;
    }
    uint32_t held = 0;
    for (uint32_t ci = 0; ci < b->max_conns; ci++) {
        b_conn_t *cn = &b->conns[ci];
        if (!cn->used || cn->closed || cn->session == NULL) {
            continue;
        }
        for (uint32_t i = 0; i < b->n_dsubs; i++) {
            b_dsub_t *ds = &cn->dsubs[i];
            if (!ds->used || ds->upd_answer == B_UPD_NONE) {
                continue;
            }
            if (cn->events_pending) {
                held++;
                continue;
            }
            if (bind_update_answer_give(b, cn, ds, now_us) == B_UPD_HELD) {
                held++;
            }
        }
    }
    b->upd_answers_held = held;
}


/* Before the relay ends a downstream subscription itself: the session
 * refuses a terminal while an update answer is owed (§9.5 -- exactly one
 * answer per update), so that answer goes first. A held decision is given;
 * an owed answer whose event the binding has not polled yet is accepted, as
 * nothing it asks for will be served once the subscription ends. */
b_upd_give_t
bind_update_answer_settle(moqr_bind_t *b, b_conn_t *cn, uint64_t sub_raw,
                          uint64_t now_us)
{
    for (uint32_t i = 0; i < b->n_dsubs; i++) {
        b_dsub_t *ds = &cn->dsubs[i];
        if (!ds->used || ds->sub_raw != sub_raw) {
            continue;
        }
        if (ds->upd_answer == B_UPD_NONE) {
            uint64_t id = 0;
            moq_result_t orc = ds->publication
                ? moq_session_publication_owed_update(
                      cn->session, (moq_publication_t){ ._opaque = sub_raw },
                      &id, NULL, NULL)
                : moq_session_subscription_owed_update(
                      cn->session, (moq_subscription_t){ sub_raw }, &id, NULL,
                      NULL);
            if (orc != MOQ_OK) {
                return B_UPD_GIVEN;
            }
            ds->upd_answer = B_UPD_ACCEPT;
            ds->upd_id = id;
            ds->upd_has_forward = false;
            b->upd_answers_held++;
        }
        return bind_update_answer_give(b, cn, ds, now_us);
    }
    return B_UPD_GIVEN;
}
