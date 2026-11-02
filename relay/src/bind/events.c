#include "bind_internal.h"
#include "events_internal.h"
#include "authorization_internal.h"
#include "delivery_internal.h"
#include "discovery_internal.h"
#include "fetch_internal.h"
#include "fetch_fill_internal.h"
#include "fetch_joining_internal.h"
#include "intents_internal.h"
#include "metadata_internal.h"
#include "request_answers_internal.h"
#include "subscription_update_internal.h"
#include "upstream_internal.h"

/* -- event translation (the parity-proven table) --------------------------- */

static moqr_ns_t
ns_from_event(const moq_namespace_t *ns, moq_bytes_t *storage)
{
    size_t count = ns->count < 32 ? ns->count : 32;
    for (size_t i = 0; i < count; i++) {
        storage[i] = ns->parts[i];
    }
    return (moqr_ns_t){ .parts = storage, .count = count };
}


/* Copy a request's namespace (and optional name) out of session event memory
 * into the binding's own stash, repointing the views. A core call retried with
 * BIND_CALL_RETRY drains intents between attempts, and executing an intent on
 * the same session can reset that session's event scratch; a retry must still
 * read the request's own bytes. Views too large to stash are left as they are:
 * the core refuses them as INVAL on the first attempt, before any retry. One
 * stash serves the one event being handled. */
static void
bind_stash_ftn(moqr_bind_t *b, moqr_ns_t *ns, moq_bytes_t *name)
{
    size_t total = name != NULL ? name->len : 0;
    if (ns->count > 32) {
        return;
    }
    for (size_t i = 0; i < ns->count; i++) {
        total += ns->parts[i].len;
    }
    if (total > sizeof(b->stash_bytes)) {
        return;
    }
    size_t off = 0;
    for (size_t i = 0; i < ns->count; i++) {
        size_t len = ns->parts[i].len;
        if (len != 0) {
            memcpy(b->stash_bytes + off, ns->parts[i].data, len);
        }
        b->stash_parts[i].data = len != 0 ? b->stash_bytes + off : NULL;
        b->stash_parts[i].len = len;
        off += len;
    }
    ns->parts = b->stash_parts;
    if (name != NULL) {
        if (name->len != 0) {
            memcpy(b->stash_bytes + off, name->data, name->len);
            name->data = b->stash_bytes + off;
        }
    }
}


/* Session-level error code for an unauthorized connection (setup DENY). This
 * is the SESSION close code (0x2), distinct from the request-level
 * MOQ_REQUEST_ERROR_UNAUTHORIZED (0x1) used to reject individual requests. */
#define BIND_SESSION_UNAUTHORIZED 0x2u



void
bind_on_event(moqr_bind_t *b, b_conn_t *cn, moq_event_t *ev,
              uint64_t now_us)
{
    moq_bytes_t nsbuf[32];
    b->events_translated++;
    bind_cluster_sync(b, cn, now_us, false);
    if (cn->closed || !cn->used) return;
    switch (ev->kind) {
    case MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED: {
        if (ev->detail_size < MOQ_OUTGOING_SUBGROUP_TERMINATED_EVENT_V0_SIZE) {
            break;
        }
        const moq_outgoing_subgroup_terminated_event_t *te =
            &ev->u.outgoing_subgroup_terminated;
        if (te->cause != MOQ_OUTGOING_SUBGROUP_TERMINAL_PEER_STOP_SENDING &&
            te->cause != MOQ_OUTGOING_SUBGROUP_TERMINAL_DELIVERY_TIMEOUT) break;
        bool pub = moq_publication_is_valid(te->pub);
        bool sub = moq_subscription_is_valid(te->sub);
        if (pub == sub) break;
        uint64_t owner = pub ? te->pub._opaque : te->sub._opaque;
        if (bind_cookie_is_publication(owner) != pub) break;
        for (uint32_t i = 0; i < b->n_sgs; i++) {
            b_sg_t *sg = &cn->sgs[i];
            if (!sg->used || sg->sub_raw != owner ||
                !moq_subgroup_eq(sg->h, te->subgroup) ||
                sg->group != te->group_id || sg->subgroup != te->subgroup_id) {
                continue;
            }
            if (sg->stopped) break;   /* keep the first exact terminal identity */
            sg->stopped = true;
            sg->stopped_ref = te->stream_ref;
            sg->obj_begun = false;
            sg->obj_id = 0;
            sg->next_chunk = 0;
            sg->reset_owed = false;
            sg->reset_wire = 0;
            /* The failed write's capacity floor is obsolete: reconciliation
             * needs no action space, and may preserve a sibling's hold. */
            uint32_t ci = (uint32_t)(cn - b->conns);
            bind_dl_clear(b->dl_parked, ci);
            cn->park_reason = B_PARK_NONE;
            cn->park_cap = 0;
            bind_dl_set(b->dl_ready, ci);
            break;
        }
        break;
    }
    case MOQ_EVENT_NAMESPACE_FOUND:
        bind_cluster_found(b, cn, &ev->u.namespace_found, now_us);
        break;
    case MOQ_EVENT_NAMESPACE_GONE: {
        const moq_namespace_gone_event_t *gone = &ev->u.namespace_gone;
        moqr_ns_t ns = {gone->track_namespace_suffix.parts, gone->track_namespace_suffix.count};
        for (uint32_t i = 0; i < b->n_anns; i++) {
            b_ann_t *a = &cn->anns[i];
            if (a->used && a->discovered && a->discovery._opaque == gone->handle._opaque && ns_equal(a->ns, ns)) {
                (void)bind_ann_withdraw(b, cn, a->ann_raw, now_us); break;
            }
        }
        break;
    }
    case MOQ_EVENT_NS_SUB_ERROR:
        if (ev->u.ns_sub_error.handle._opaque == cn->cluster_discovery._opaque)
            bind_cluster_discovery_end(b, cn, cn->cluster_discovery._opaque, now_us);
        break;
    case MOQ_EVENT_NAMESPACE_PUBLISHED: {
        const moq_namespace_published_event_t *np =
            &ev->u.namespace_published;
        moqr_ns_t ns = ns_from_event(&np->track_namespace, nsbuf);
        uint64_t aerr = 0;
        uint64_t atkt = 0;
        uint64_t alease = 0;
        bind_auth_result_t aar =
            bind_authorize(b, cn, MOQR_AUTH_PUBLISH_NAMESPACE, ns,
                           (moq_bytes_t){ NULL, 0 }, np->tokens,
                           np->token_count, now_us, &aerr, &atkt, &alease);
        if (aar != BIND_AUTH_ALLOW) {
            moqr_park_req_t pr;
            bind_park_init(&pr, cn, b, MOQR_AUTH_PUBLISH_NAMESPACE, ns,
                           (moq_bytes_t){ NULL, 0 }, np->ann._opaque);
            bind_refuse_or_park(b, cn, aar, &pr, np->tokens, np->token_count,
                                atkt, aerr, now_us);
            break;
        }
        /* ALLOW: if it carries a lease, reserve a revalidation grant BEFORE the
         * announce mutates routing state. A reserve failure fails the request
         * closed with no wire accept (CAT: a recipient that cannot track a
         * revalidatable grant rejects the request). */
        moqr_park_req_t gpr;
        bind_park_init(&gpr, cn, b, MOQR_AUTH_PUBLISH_NAMESPACE, ns,
                       (moq_bytes_t){ NULL, 0 }, np->ann._opaque);
        moqr_grant_res_t gres;
        if (bind_grant_reserve(b, &gpr, np->tokens, np->token_count, alease,
                               now_us, &gres) != MOQR_OK) {
            bind_refuse(b, cn, MOQR_AUTH_PUBLISH_NAMESPACE, np->ann._opaque,
                        MOQ_REQUEST_ERROR_UNAUTHORIZED, false, 0, now_us);
            break;
        }
        int ann_slot = ann_store(b, cn, np->ann._opaque, ns);
        if (ann_slot < 0) {
            if (gres.gen != 0) {
                moqr_core_grant_abort(b->core, gres);
            }
            /* The announce table is a bounded pool: a capacity refusal. */
            bind_reject_admission(b, cn, MOQR_AUTH_PUBLISH_NAMESPACE,
                                  np->ann._opaque, MOQR_ERR_CAPACITY, now_us);
            break;
        }
        /* From here on the stored copy, never the event's views: the
         * announce may be retried after an intent drain, and executing an
         * intent on this session can reset its event memory. */
        moqr_ns_t ans = cn->anns[ann_slot].ns;
        /* The event's tokens, retained before the announce can drain
         * intents and reset the event memory they live in. */
        bind_retain_session_tokens(b, cn, np->ann._opaque, np->tokens,
                                   np->token_count);
        moqr_result_t arc;
        arc = bind_announce(b, cn, ans, np->ann._opaque, now_us);
        if (arc != MOQR_OK) {
            moqr_core_request_tokens_clear(b->core, cn->binding,
                                           np->ann._opaque);
            ann_clear_slot(b, cn, (uint32_t)ann_slot);
            if (gres.gen != 0) {
                moqr_core_grant_abort(b->core, gres);
            }
            bind_reject_admission(b, cn, MOQR_AUTH_PUBLISH_NAMESPACE,
                                  np->ann._opaque, arc, now_us);
            break;
        }
        /* Announce succeeded. Commit the reservation (if any), undoing the
         * announce on the invariant commit failure. */
        if (bind_grant_commit_ann(b, cn, gres, ans, np->ann._opaque, now_us)) {
            bind_accept(b, cn, B_ACCEPT_NAMESPACE, np->ann._opaque, now_us);
        } else {
            moqr_core_request_tokens_clear(b->core, cn->binding,
                                           np->ann._opaque);
            ann_clear_slot(b, cn, (uint32_t)ann_slot);
        }
        break;
    }
    case MOQ_EVENT_NAMESPACE_DONE:
    case MOQ_EVENT_NAMESPACE_TERMINATED: {
        /* D22 retires the advertisement through a request-local outcome,
         * not the legacy DONE event. Only incoming advertisements own routes. */
        if (ev->kind == MOQ_EVENT_NAMESPACE_TERMINATED &&
            ev->u.namespace_terminated.local_role != MOQ_NAMESPACE_ROLE_RESPONDER) {
            break;
        }
        uint64_t raw = ev->kind == MOQ_EVENT_NAMESPACE_DONE
            ? ev->u.namespace_done.ann._opaque
            : ev->u.namespace_terminated.ann._opaque;
        moqr_core_request_tokens_clear(b->core, cn->binding, raw);
        if (bind_ann_withdraw(b, cn, raw, now_us) == MOQR_ERR_WOULD_BLOCK) {
            b->session_errors++;
        }
        break;
    }
    case MOQ_EVENT_SUBSCRIBE_REQUEST: {
        const moq_subscribe_request_event_t *sq = &ev->u.subscribe_request;
        moqr_ns_t sns = ns_from_event(&sq->track_namespace, nsbuf);
        uint64_t rendezvous_deadline = 0;
        bool rendezvous = bind_sub_rendezvous(cn, ev, now_us,
                                              &rendezvous_deadline);
        uint64_t serr = 0;
        uint64_t stkt = 0;
        uint64_t slease = 0;
        bind_auth_result_t sar =
            bind_authorize(b, cn, MOQR_AUTH_SUBSCRIBE, sns, sq->track_name,
                           sq->tokens, sq->token_count, now_us, &serr, &stkt,
                           &slease);
        /* A failed filter observation is answered only after authorization;
         * a deferred request waits for its decision in bind_resume. */
        moqr_subscribe_req_t defaults;
        moqr_subscribe_req_init(&defaults);
        moqr_filter_t sfilter = defaults.filter;
        moq_location_filter_cfg_t location;
        bool semantic = cn->wire_draft == B_DRAFT_22;
        moqr_result_t frc = semantic
            ? bind_read_sub_location(cn, sq->sub, &location)
            : bind_read_sub_filter(cn, sq->sub, &sfilter);
        if (frc != MOQR_OK && sar == BIND_AUTH_ALLOW) {
            bind_reject_filter(b, cn, sq->sub, now_us);
            break;
        }
        if (sar != BIND_AUTH_ALLOW) {
            moqr_park_req_t pr;
            bind_park_init(&pr, cn, b, MOQR_AUTH_SUBSCRIBE, sns,
                           sq->track_name, sq->sub._opaque);
            pr.sub_filter_type = frc == MOQR_OK ? sfilter.type
                                                : BIND_PARKED_FILTER_REFUSED;
            pr.sub_start_group = sfilter.start_group;
            pr.sub_start_object = sfilter.start_object;
            pr.sub_end_group_delta = sfilter.end_group_delta;
            pr.sub_priority = sq->subscriber_priority;
            pr.sub_group_order = sq->group_order;
            pr.sub_forward_off = !sq->forward;   /* resumed paused, as asked */
            pr.sub_rendezvous = rendezvous;
            pr.sub_rendezvous_deadline_us = rendezvous_deadline;
            bind_refuse_or_park(b, cn, sar, &pr, sq->tokens, sq->token_count,
                                stkt, serr, now_us);
            break;
        }
        /* ALLOW: reserve a revalidation grant BEFORE subscribing when the
         * verdict carries a lease. A reserve failure fails the request closed
         * with no wire OK (CAT: cannot track a revalidatable grant -> reject). */
        moqr_park_req_t gpr;
        bind_park_init(&gpr, cn, b, MOQR_AUTH_SUBSCRIBE, sns, sq->track_name,
                       sq->sub._opaque);
        moqr_grant_res_t gres;
        if (bind_grant_reserve(b, &gpr, sq->tokens, sq->token_count, slease,
                               now_us, &gres) != MOQR_OK) {
            bind_refuse(b, cn, MOQR_AUTH_SUBSCRIBE, sq->sub._opaque,
                        MOQ_REQUEST_ERROR_UNAUTHORIZED, false, 0, now_us);
            break;
        }
        moqr_subscribe_req_t req;
        moqr_subscribe_req_init(&req);
        moq_bytes_t sname = sq->track_name;
        bind_stash_ftn(b, &sns, &sname);
        req.ns = sns;
        req.name = sname;
        req.filter = sfilter;
        req.subscriber_priority = sq->subscriber_priority;
        req.group_order = (moqr_group_order_t)sq->group_order;
        req.cookie = sq->sub._opaque;
        req.forward = sq->forward;   /* carry initial Forward (a paused
                                      * subscription must not be scheduled) */
        req.range_end_holds = bind_range_end_holds(cn);
        req.rendezvous = rendezvous;
        req.rendezvous_deadline_us = rendezvous_deadline;
        if (semantic) req.location = &location;
        moqr_sub_t rsub;
        moqr_result_t src;
        BIND_CALL_RETRY(src, moqr_core_subscribe(b->core, cn->binding,
                                                 &req, &rsub));
        if (src == MOQR_ERR_CAPACITY && rendezvous) {
            if (gres.gen != 0) {
                moqr_core_grant_abort(b->core, gres);
            }
            bind_reject_load(b, cn, sq->sub, now_us);
            break;
        }
        if (src != MOQR_OK) {
            if (gres.gen != 0) {
                moqr_core_grant_abort(b->core, gres);
            }
            bind_reject_admission(b, cn, MOQR_AUTH_SUBSCRIBE, sq->sub._opaque,
                                  src, now_us);
            break;
        }
        /* Subscribe succeeded; the ACCEPT_SUB flows through intents. Commit the
         * reservation with the new sub handle (revoking it on the invariant
         * commit failure). */
        bind_grant_commit_sub(b, gres, rsub, now_us);
        break;
    }
    case MOQ_EVENT_UNSUBSCRIBED: {
        uint64_t raw = ev->u.unsubscribed.sub._opaque;
        bool tracked = false;
        /* The relay's own upstream subscription, cancelled by its session
         * because the Track is malformed (§12.1): the downstream side ends
         * with MALFORMED_TRACK. */
        if (ev->detail_size >= offsetof(moq_unsubscribed_event_t, cause) +
                                   sizeof(ev->u.unsubscribed.cause) &&
            ev->u.unsubscribed.cause == MOQ_REQUEST_TERMINAL_MALFORMED_TRACK) {
            for (uint32_t i = 0; i < b->n_usubs; i++) {
                if (!cn->usubs[i].used || cn->usubs[i].ssub._opaque != raw) {
                    continue;
                }
                moqr_track_t track = cn->usubs[i].track;
                if (!bind_terminal_owns_track(b, track, cn->usubs[i].track_gen)) {
                    usub_release(b, &cn->usubs[i]);
                    tracked = true;
                    continue;
                }
                moqr_result_t mrc;
                b->malformed_tracks++;
                bind_reset_track_streams(b, track, now_us);
                BIND_CALL_RETRY(mrc, moqr_core_source_done(
                    b->core, track, cn->usubs[i].track_gen,
                    bind_local_pd(MOQR_PD_MALFORMED_TRACK), now_us));
                if (mrc != MOQR_OK && mrc != MOQR_ERR_STALE_HANDLE &&
                    mrc != MOQR_ERR_WRONG_STATE) {
                    b->session_errors++;
                }
                bind_fetches_malformed(b, track, now_us);
                usub_release(b, &cn->usubs[i]);
                tracked = true;
            }
        }
        /* The peer cancelled a request the relay serves: the session stops
         * its open streams itself, so the binding only retires its side. */
        for (uint32_t i = 0; i < b->n_dsubs; i++) {
            if (cn->dsubs[i].used && cn->dsubs[i].sub_raw == raw) {
                bind_dsub_retire(b, cn, &cn->dsubs[i], now_us);
                tracked = true;
            }
        }
        /* A draft-22 SUBSCRIBE withdrawn before its answer -- one held for
         * a publisher -- ends its wait too. */
        moqr_sub_t pending;
        if (!tracked && cn->wire_draft == B_DRAFT_22 &&
            moqr_core_sub_find(b->core, cn->binding, raw, &pending) ==
                MOQR_OK) {
            (void)moqr_core_unsubscribe(b->core, pending, now_us);
        }
        break;
    }
    case MOQ_EVENT_SUBSCRIBE_OK: {
        uint64_t raw = ev->u.subscribe_ok.sub._opaque;
        for (uint32_t i = 0; i < b->n_usubs; i++) {
            if (cn->usubs[i].used && cn->usubs[i].ssub._opaque == raw) {
                moqr_binding_t binding = cn->binding;
                moqr_track_t track = cn->usubs[i].track;
                uint64_t gen = cn->usubs[i].track_gen;
                moqr_bind_meta_ref_t input = bind_meta_input(
                    cn, raw, gen, ev->u.subscribe_ok.track_properties);
                moqr_result_t mrc = moqr_bind_meta_replace(
                    b, track, gen, &input, now_us, NULL);
                /* replace seals older intents and can detach a source. Byte
                 * exhaustion published UNAVAILABLE; this ACK may still make
                 * the core active, but each child will be refused locally. */
                if ((mrc != MOQR_OK && mrc != MOQR_ERR_CAPACITY &&
                     mrc != MOQR_ERR_NOMEM) || !cn->used || cn->closed ||
                    !moqr_binding_eq(cn->binding, binding) ||
                    !cn->usubs[i].used || cn->usubs[i].ssub._opaque != raw ||
                    !moqr_track_eq(cn->usubs[i].track, track) ||
                    cn->usubs[i].track_gen != gen) {
                    break;
                }
                moqr_result_t urc;
                BIND_CALL_RETRY(urc, moqr_core_upstream_ok(
                    b->core, track, gen,
                    raw, ev->u.subscribe_ok.has_largest,
                    ev->u.subscribe_ok.largest_group,
                    ev->u.subscribe_ok.largest_object));
                if (urc != MOQR_OK) {
                    b->session_errors++;   /* invariant makes this unreachable */
                }
            }
        }
        break;
    }
    case MOQ_EVENT_SUBSCRIBE_ERROR: {
        uint64_t raw = ev->u.subscribe_error.sub._opaque;
        for (uint32_t i = 0; i < b->n_usubs; i++) {
            if (cn->usubs[i].used && cn->usubs[i].ssub._opaque == raw) {
                moqr_result_t urc;
                BIND_CALL_RETRY(urc, moqr_core_upstream_error(
                    b->core, cn->usubs[i].track, cn->usubs[i].track_gen,
                    ev->u.subscribe_error.error_code, now_us));
                /* Only release the tracking slot once the core actually
                 * consumed the resolution (it retires the parked subs). */
                if (urc == MOQR_OK) {
                    usub_release(b, &cn->usubs[i]);
                } else {
                    b->session_errors++;
                }
            }
        }
        break;
    }
    case MOQ_EVENT_SUBSCRIBE_UPDATED: {
        /* A downstream subscription update. Request-credit accounting already
         * counted it (bind_event_is_request). The relay answers it
         * (bind_update_answer) unless the session already did -- an update
         * it took before this binding switched it to deferral. Then what the
         * relay schedules on is applied after the fact: a replacement
         * Location Filter (copied from this event's resolved snapshot), then a
         * Forward change -- Forward=0 pauses object scheduling only (control
         * stays live), Forward=1 resumes. An update omitting a field leaves
         * that state unchanged. Priority / delivery-timeout / new-group
         * changes on this event are not applied here. */
        const moq_subscribe_updated_event_t *up = &ev->u.subscribe_updated;
        const moq_subscription_update_options_t *options =
            bind_update_options(ev);
        bool want_filter = bind_update_filter_applies(cn) && options != NULL &&
            (options->flags & MOQ_SUBSCRIPTION_UPDATE_OPTIONS_LOCATION) != 0;
        if (up->answer_owed) {
            bind_update_answer(b, cn, up, bind_update_filter_applies(cn), now_us);
            break;
        }
        if (!want_filter && !up->has_forward) {
            break;
        }
        uint64_t raw = up->sub._opaque;
        b_dsub_t *ds = NULL;
        for (uint32_t i = 0; i < b->n_dsubs; i++) {
            if (cn->dsubs[i].used && cn->dsubs[i].sub_raw == raw) {
                ds = &cn->dsubs[i];
                break;
            }
        }
        /* An update for a subscription the binding does not track is an
         * invariant break: fail closed (count it), mutate nothing. */
        if (ds == NULL) {
            b->session_errors++;
            break;
        }
        if (want_filter) {
            /* The session already acknowledged this selection. Failure to
             * apply it now requires PUBLISH_DONE UPDATE_FAILED; the SUB_DONE
             * intent releases its tracking slot. */
            moq_location_filter_cfg_t loc;
            moqr_result_t mrc = bind_update_resolved_location(options,
                up->update_request_id, &loc);
            if (mrc == MOQR_OK) {
                mrc = moqr_core_sub_set_location_filter(
                    b->core, ds->rsub, &loc, now_us);
            }
            moqr_result_t frc = mrc;
            if (mrc != MOQR_OK && mrc != MOQR_ERR_STALE_HANDLE) {
                moqr_pd_desc_t pd;
                if (moqr_pd_desc_local(MOQR_PD_UPDATE_FAILED, &pd) != MOQR_OK) {
                    b->session_errors++;
                    break;
                }
                BIND_CALL_RETRY(frc, moqr_core_revoke_sub(b->core, ds->rsub,
                                                          pd, now_us));
            }
            /* STALE = core already retired it (a terminal crossed): moot. Any
             * other non-OK is a broken invariant — fail closed via the counter,
             * never touch another slot. */
            if (frc != MOQR_OK && frc != MOQR_ERR_STALE_HANDLE) {
                b->session_errors++;
            }
            if (mrc != MOQR_OK) {
                break;   /* the subscription is ending: no Forward to apply */
            }
        }
        if (up->has_forward) {
            moqr_result_t frc = moqr_core_sub_set_forward(
                b->core, ds->rsub, up->forward, now_us);
            if (frc != MOQR_OK && frc != MOQR_ERR_STALE_HANDLE) {
                b->session_errors++;
            }
        }
        break;
    }
    case MOQ_EVENT_SUBSCRIBE_DONE: {
        /* The origin finished the relay's upstream subscription. The session
         * surfaces this only once the done's Stream Count of data streams has
         * been processed (or its wait expired), so everything the origin sent
         * is ingested: each downstream subscriber receives what is still
         * owed to it and then its wire SUBSCRIBE_DONE, and the upstream is
         * released, so nothing hangs behind a sourceless ACTIVE track. The
         * finish queues no intent and cannot be refused for room, so the
         * tracking slot is always released (STALE/WRONG_STATE mean the core
         * already moved on). */
        uint64_t raw = ev->u.subscribe_done.sub._opaque;
        for (uint32_t i = 0; i < b->n_usubs; i++) {
            if (!cn->usubs[i].used || cn->usubs[i].ssub._opaque != raw) {
                continue;
            }
            moqr_result_t drc = moqr_core_source_finish(
                b->core, cn->usubs[i].track, cn->usubs[i].track_gen,
                bind_upstream_pd(b, cn, ev->u.subscribe_done.status_code),
                now_us);
            if (drc != MOQR_OK && drc != MOQR_ERR_STALE_HANDLE &&
                drc != MOQR_ERR_WRONG_STATE) {
                b->session_errors++;
            }
            usub_release(b, &cn->usubs[i]);
        }
        break;
    }
    case MOQ_EVENT_REQUEST_REDIRECT: {
        /* The relay does not follow redirect URIs (a later routing feature);
         * the redirected upstream request is terminal. The Redirect's retry
         * interval is deliberately not read: there is no reissue schedule to
         * feed it to. */
        bind_upstream_terminated(b, cn, ev->u.request_redirect.family,
                                 ev->u.request_redirect.handle.raw,
                                 BIND_UP_END_REDIRECT, now_us);
        break;
    }
    case MOQ_EVENT_REQUEST_GOAWAY: {
        /* The relay does not migrate a request to a new session; treat the
         * per-request GOAWAY as terminal for the upstream subscription. */
        bind_upstream_terminated(b, cn, ev->u.request_goaway.family,
                                 ev->u.request_goaway.handle.raw,
                                 BIND_UP_END_GOAWAY, now_us);
        break;
    }
    case MOQ_EVENT_OBJECT_RECEIVED: {
        moq_object_received_event_t *ob = &ev->u.object_received;
        moqr_track_t track = bind_ingest_track(b, cn, ob->sub, ob->pub);
        if (!moqr_track_is_valid(track)) {
            break;   /* not relay ingest */
        }
        moqr_log_append_desc_t d;
        moqr_log_append_desc_init(&d);
        d.group_id = ob->group_id;
        d.subgroup_id = ob->subgroup_id;
        d.object_id = ob->object_id;
        d.publisher_priority = ob->publisher_priority;
        d.status = (moqr_obj_status_t)ob->status;
        d.datagram_pref = ob->datagram;
        d.end_of_group = ob->end_of_group;
        d.first_object = ob->first_object;
        d.subgroup_properties = ob->subgroup_properties;
        d.properties_codec = bind_props_codec(cn);
        d.payload = ob->payload;        /* ownership transfers on OK */
        d.properties = ob->properties;
        d.now_us = now_us;
        moqr_result_t irc = moqr_core_ingest(b->core, track, &d);
        if (irc == MOQR_OK) {
            ob->payload = NULL;
            ob->properties = NULL;
            bind_note_relay_largest(b, track);
        } else {
            /* event cleanup releases the refs */
            bind_ingest_refused(b, cn, track, &d, irc, ob->sub, ob->pub, now_us);
        }
        break;
    }
    case MOQ_EVENT_OBJECT_CHUNK: {
        /* Streaming ingest: map a streamed NORMAL object onto the
         * OPEN-record machinery -- begin opens, payload slices append, end
         * completes (NORMAL) or abandons (RESET/STOP). Every chunk carries
         * (group, subgroup, object), so continuation chunks route correctly
         * even when concurrent subgroups interleave. Chunked COMPLETE records
         * deliver on the subscribe path, OPEN records at the live edge;
         * an abandon stores the terminal reset code (upstream RESET →
         * the publisher's code, else a relay-defined code) which the subscribe
         * path forwards downstream via reset_subgroup. Retained FETCH
         * still skips chunked records. */
        moq_object_chunk_event_t *oc = &ev->u.object_chunk;
        moqr_track_t track = bind_ingest_track(b, cn, oc->sub, oc->pub);
        if (!moqr_track_is_valid(track)) {
            break;   /* not relay ingest */
        }
        uint64_t src = moq_subscription_is_valid(oc->sub) ? oc->sub._opaque
                                                          : oc->pub._opaque;
        /* A chunk of a copy being compared: compared, never ingested. */
        b_dup_owner_t *dup_owner = bind_dup_owner_find(b, cn, src, oc);
        if (dup_owner != NULL) {
            bind_dup_owner_chunk(b, cn, dup_owner, oc, now_us);
            break;
        }
        /* Status objects (no payload) arrive as one begin+end chunk and are
         * whole-object records -- ingest via the COMPLETE path, which handles
         * EOG/EOT. Only NORMAL objects use the OPEN chunk machinery. */
        if (oc->begin && oc->status != MOQ_OBJECT_NORMAL) {
            moqr_log_append_desc_t d;
            moqr_log_append_desc_init(&d);
            d.group_id = oc->group_id;
            d.subgroup_id = oc->subgroup_id;
            d.object_id = oc->object_id;
            d.publisher_priority = oc->publisher_priority;
            d.status = (moqr_obj_status_t)oc->status;
            d.end_of_group = oc->end_of_group;
            d.first_object = oc->first_object;
            d.subgroup_properties = oc->subgroup_properties;
            d.properties_codec = bind_props_codec(cn);
            d.payload = NULL;
            d.properties = oc->properties;   /* ownership transfers on OK */
            d.now_us = now_us;
            moqr_result_t irc = moqr_core_ingest(b->core, track, &d);
            if (irc == MOQR_OK) {
                oc->properties = NULL;
                bind_note_relay_largest(b, track);
            } else {
                bind_ingest_refused(b, cn, track, &d, irc, oc->sub, oc->pub,
                                    now_us);
            }
            break;
        }
        /* NORMAL object: begin opens the OPEN record, carrying the subgroup
         * header's EOG bit — subgroup metadata the log latches for the
         * deferred group-final at seal, and the record retains for the
         * downstream subgroup open. The subgroup itself still seals only via
         * SUBGROUP_FINISHED. */
        if (oc->begin) {
            moqr_log_append_desc_t d;
            moqr_log_append_desc_init(&d);
            d.group_id = oc->group_id;
            d.subgroup_id = oc->subgroup_id;
            d.object_id = oc->object_id;
            d.publisher_priority = oc->publisher_priority;
            d.status = MOQR_OBJ_NORMAL;
            d.end_of_group = oc->end_of_group;
            d.first_object = oc->first_object;
            d.subgroup_properties = oc->subgroup_properties;
            d.properties_codec = bind_props_codec(cn);
            d.obj_state = MOQR_OBJ_OPEN;
            d.declared_len = oc->payload_length;
            d.properties = oc->properties;   /* ownership transfers on OK */
            d.now_us = now_us;
            moqr_result_t irc = moqr_core_ingest(b->core, track, &d);
            if (irc == MOQR_OK) {
                oc->properties = NULL;
                bind_note_relay_largest(b, track);
                /* The record's chunks are this source's only. */
                (void)moqr_core_internal_set_opener(b->core, track, oc->group_id,
                                                    oc->subgroup_id, oc->object_id,
                                                    bind_conn_slot(b, cn), src);
            } else {
                uint32_t dup = bind_ingest_refused(b, cn, track, &d, irc, oc->sub,
                                                   oc->pub, now_us);
                if (dup == MOQR_LOG_DUP_PENDING) {
                    /* The copy is compared chunk by chunk from here on. */
                    for (uint32_t i = 0; i < BIND_DUP_OWNERS; i++) {
                        b_dup_owner_t *o = &b->dup_owners[i];
                        if (o->used) continue;
                        *o = (b_dup_owner_t){ .used = true,
                                              .conn = bind_conn_slot(b, cn),
                                              .src = src, .track = track,
                                              .group = oc->group_id,
                                              .subgroup = oc->subgroup_id,
                                              .object = oc->object_id };
                        bind_dup_owner_chunk(b, cn, o, oc, now_us);
                        break;
                    }
                }
                break;   /* open failed: no record to abandon */
            }
        }
        /* A copy whose begin found no free comparison owner is ignored
         * uncompared (§7.1 lets a relay ignore a later copy); its chunks are
         * never appended to, nor complete or abandon, a record still arriving
         * from another source. */
        if (!oc->begin &&
            moqr_core_internal_open_by_other(b->core, track, oc->group_id,
                                             oc->subgroup_id, oc->object_id,
                                             bind_conn_slot(b, cn), src)) {
            if (oc->end) b->ingest_duplicates++;
            break;
        }
        /* Payload slice (begin/continuation/end may each carry one). The log
         * increfs the slice; the event keeps its ref (freed by event cleanup).
         * Fail-closed: an append failure abandons the record, no half-state. */
        if (oc->chunk != NULL) {
            moqr_result_t arc =
                moqr_core_append_chunk(b->core, track, oc->group_id,
                                       oc->subgroup_id, oc->object_id,
                                       oc->chunk);
            if (arc != MOQR_OK) {
                /* The chunk is not held, so the object is lost here. Refused
                 * for capacity, with nothing older left to reclaim, it is
                 * lost to the relay's bounded cache: the record and its
                 * subgroup's stream end with the eviction cause, as a refused
                 * ingest does, so no subscriber's stream goes on past the
                 * lost object or ends with a FIN, and a later object of the
                 * subgroup starts a new stream. Any other refusal (a chunk
                 * past the declared length, a record no longer OPEN) ends the
                 * record alone, as a local STOP does. */
                bool lost = arc == MOQR_ERR_CAPACITY;
                moqr_reset_desc_t cause;
                if (moqr_reset_desc_internal(lost ? MOQR_RESET_CODE_EVICTED
                                                  : MOQR_RESET_CODE_STOPPED,
                                             &cause) == MOQR_OK &&
                    moqr_core_abandon_record(b->core, track, oc->group_id,
                                             oc->subgroup_id, oc->object_id,
                                             cause) == MOQR_OK &&
                    lost) {
                    (void)moqr_core_reset_subgroup(b->core, track,
                                                   oc->group_id,
                                                   oc->subgroup_id, cause);
                }
                b->ingest_refusals++;
                break;
            }
        }
        /* End: complete (NORMAL) or abandon (RESET/STOP). A failed complete
         * (e.g. bytes != declared) also abandons rather than leave OPEN. The
         * abandon stores the terminal cause code: a genuine upstream RESET
         * forwards the publisher's oc->error_code; STOP / a relay-initiated
         * abandon uses a relay-defined code. Downstream delivery
         * propagates it.
         *
         * An upstream RESET also ends the subgroup's stream, and this chunk
         * is the session's only report of it (a reset is reported once, on
         * one surface): the subgroup's list ends with the same reset, so a
         * subscription that never began this object is still closed with a
         * reset after the retained objects. Only the source whose OPEN record
         * this abandoned ends the list; a copy's terminal from another source
         * finds no OPEN record of its own. STOP and a failed complete end the
         * record alone. */
        if (oc->end) {
            moqr_result_t erc = MOQR_OK;
            if (oc->terminal == MOQ_OBJECT_TERMINAL_NORMAL) {
                erc = moqr_core_complete_record(b->core, track, oc->group_id,
                                                oc->subgroup_id, oc->object_id);
            }
            if (oc->terminal != MOQ_OBJECT_TERMINAL_NORMAL || erc != MOQR_OK) {
                moqr_reset_desc_t rdesc;
                bool have_rdesc;
                if (oc->terminal == MOQ_OBJECT_TERMINAL_RESET) {
                    rdesc = bind_upstream_reset(b, cn, oc->error_code);
                    have_rdesc = rdesc.tag != MOQR_CODE_NONE;
                } else {
                    have_rdesc = moqr_reset_desc_internal(
                                     MOQR_RESET_CODE_STOPPED, &rdesc) ==
                                 MOQR_OK;
                }
                if (have_rdesc &&
                    moqr_core_abandon_record(b->core, track, oc->group_id,
                                             oc->subgroup_id, oc->object_id,
                                             rdesc) == MOQR_OK &&
                    oc->terminal == MOQ_OBJECT_TERMINAL_RESET) {
                    (void)moqr_core_reset_subgroup(b->core, track,
                                                   oc->group_id,
                                                   oc->subgroup_id, rdesc);
                }
                if (erc != MOQR_OK) {
                    b->ingest_refusals++;
                }
            }
        }
        break;
    }
#ifdef MOQ_EVENT_SUBGROUP_RESET
    case MOQ_EVENT_SUBGROUP_RESET: {
        /* An upstream subgroup stream RESET that no surfaced object terminal
         * carries: in whole-object receive mode (the session dropped the
         * partial object, no OBJECT_RECEIVED for it), and in streaming mode
         * a stream reset between objects or after its last object, an object
         * suppressed by Forward State 0, or a shared stream's subscriptions
         * the object in flight did not match. This event is then the only
         * ingest-side evidence the stream ended. Terminate the (group,
         * subgroup) ledger abnormally with the publisher's code — retained
         * COMPLETE records stay servable and each subscription's downstream
         * subgroup is reset (never FIN'd) once it has consumed them. A reset
         * that cuts a surfaced streaming object off arrives as that object's
         * terminal OBJECT_CHUNK instead; where both arrive (a shared stream)
         * the list keeps the first terminal recorded. */
        moq_subgroup_reset_event_t *sr = &ev->u.subgroup_reset;
        moqr_track_t track = bind_ingest_track(b, cn, sr->sub, sr->pub);
        if (moqr_track_is_valid(track)) {
            moqr_reset_desc_t srd = bind_upstream_reset(b, cn,
                                                        sr->error_code);
            if (srd.tag != MOQR_CODE_NONE) {
                (void)moqr_core_reset_subgroup(b->core, track, sr->group_id,
                                               sr->subgroup_id, srd);
            }
        }
        break;
    }
#endif
    case MOQ_EVENT_SUBGROUP_FINISHED: {
        /* The upstream subgroup stream FIN'd (plain FIN or EOG). Seal the log's
         * (group, subgroup): each subscription is then owed one acknowledged
         * SEAL notice (records only carry subgroup_end as advisory metadata),
         * and the notice's close — the ONLY durable FIN path — retires the
         * downstream stream and reclaims the slot. Not emitted for RESET/STOP,
         * so a reset is never mistaken for a graceful finish. Ordering is safe:
         * every OBJECT_RECEIVED for this subgroup was polled and ingested before
         * this event, so the sealed list will not grow. */
        moq_subgroup_finished_event_t *sf = &ev->u.subgroup_finished;
        moqr_track_t track = bind_ingest_track(b, cn, sf->sub, sf->pub);
        if (moqr_track_is_valid(track)) {
            moqr_result_t src = moqr_core_seal_subgroup(
                b->core, track, sf->group_id, sf->subgroup_id);
            if (src == MOQR_ERR_INVAL) {
                /* Malformed final evidence: this header-EOG subgroup's last
                 * object contradicts an already-retained higher object in
                 * the group. The cache must never declare a final below
                 * retained content — fail the publisher closed rather than
                 * discard the contradiction. */
                b->session_errors++;
                (void)moq_session_close(cn->session,
                                        BIND_SESSION_INTERNAL_ERROR,
                                        "eog final conflict", now_us);
                conn_detach(b, cn, now_us);
                break;
            }
        }
        break;
    }
    case MOQ_EVENT_PUBLISH_FINISHED: {
        /* A push publisher finished: same "source ended" contract as an upstream
         * SUBSCRIBE_DONE — terminate the track's downstream subscribers with a
         * wire SUBSCRIBE_DONE (forwarding the finish status) and release the
         * source, so no open-ended subscriber hangs behind a WARM track. */
        uint64_t raw = ev->u.publish_finished.pub._opaque;
        b_meta_publish_t *p = bind_meta_pub_find(b, cn, raw);
        if (p != NULL) bind_meta_pub_release(b, p);
        for (uint32_t i = 0; i < b->n_pubs; i++) {
            if (!cn->pubs[i].used || cn->pubs[i].pub_raw != raw) {
                continue;
            }
            /* Cancelled by the relay's session because the Track is
             * malformed (§12.1): downstream ends with MALFORMED_TRACK. */
            bool malformed =
                ev->detail_size >= offsetof(moq_publish_finished_event_t, cause) +
                                       sizeof(ev->u.publish_finished.cause) &&
                ev->u.publish_finished.cause == MOQ_REQUEST_TERMINAL_MALFORMED_TRACK;
            if (malformed && !bind_terminal_owns_track(
                    b, cn->pubs[i].track, cn->pubs[i].track_gen)) {
                cn->pubs[i].used = false;
                continue;
            }
            if (malformed) {
                b->malformed_tracks++;
                bind_reset_track_streams(b, cn->pubs[i].track, now_us);
                bind_fetches_malformed(b, cn->pubs[i].track, now_us);
            }
            /* A malformed track ends every subscriber at once; a publisher's
             * ordinary finish lets each one receive what is owed first (it
             * queues no intent and cannot be refused for room). */
            moqr_result_t lrc;
            if (malformed) {
                BIND_CALL_RETRY(lrc, moqr_core_source_done(
                    b->core, cn->pubs[i].track, cn->pubs[i].track_gen,
                    bind_local_pd(MOQR_PD_MALFORMED_TRACK), now_us));
                if (lrc == MOQR_ERR_WOULD_BLOCK) {
                    /* The ring could not take the terminals even after a
                     * drain: end the source now and let each subscriber's
                     * next delivery scan send its terminal, so the source
                     * this slot owns is never left ACTIVE without it. */
                    lrc = moqr_core_internal_source_abort(
                        b->core, cn->pubs[i].track, cn->pubs[i].track_gen,
                        bind_local_pd(MOQR_PD_MALFORMED_TRACK));
                }
            } else {
                lrc = moqr_core_source_finish(
                    b->core, cn->pubs[i].track, cn->pubs[i].track_gen,
                    bind_upstream_pd(b, cn,
                                     ev->u.publish_finished.status_code),
                    now_us);
            }
            if (lrc != MOQR_OK && lrc != MOQR_ERR_STALE_HANDLE &&
                lrc != MOQR_ERR_WRONG_STATE) {
                b->session_errors++;
            }
            cn->pubs[i].used = false;
        }
        break;
    }
    case MOQ_EVENT_NS_SUB_REQUEST: {
        const moq_ns_sub_request_event_t *nq = &ev->u.ns_sub_request;
        moqr_ns_t prefix =
            ns_from_event(&nq->track_namespace_prefix, nsbuf);
        uint64_t nerr = 0;
        uint64_t ntkt = 0;
        bind_auth_result_t nar =
            bind_authorize(b, cn, MOQR_AUTH_SUBSCRIBE_NAMESPACE, prefix,
                           (moq_bytes_t){ NULL, 0 }, nq->tokens,
                           nq->token_count, now_us, &nerr, &ntkt, NULL);
        if (nar != BIND_AUTH_ALLOW) {
            moqr_park_req_t pr;
            bind_park_init(&pr, cn, b, MOQR_AUTH_SUBSCRIBE_NAMESPACE, prefix,
                           (moq_bytes_t){ NULL, 0 }, nq->handle._opaque);
            bind_refuse_or_park(b, cn, nar, &pr, nq->tokens, nq->token_count,
                                ntkt, nerr, now_us);
            break;
        }
        moqr_result_t nrc;
        bind_stash_ftn(b, &prefix, NULL);
        BIND_CALL_RETRY(nrc, moqr_core_ns_subscribe(
            b->core, cn->binding, prefix, nq->handle._opaque));
        if (nrc == MOQR_OK) {
            bind_retain_session_tokens(b, cn, nq->handle._opaque,
                                       nq->tokens, nq->token_count);
            bind_accept(b, cn, B_ACCEPT_NS_SUB, nq->handle._opaque, now_us);
        } else {
            bind_reject_admission(b, cn, MOQR_AUTH_SUBSCRIBE_NAMESPACE,
                                  nq->handle._opaque, nrc, now_us);
        }
        break;
    }
    case MOQ_EVENT_TRACK_STATUS_REQUEST: {
        const moq_track_status_request_event_t *tq =
            &ev->u.track_status_request;
        moqr_ns_t ns = ns_from_event(&tq->track_namespace, nsbuf);
        uint64_t terr = 0;
        uint64_t ttkt = 0;
        bind_auth_result_t tar =
            bind_authorize(b, cn, MOQR_AUTH_TRACK_STATUS, ns, tq->track_name,
                           tq->tokens, tq->token_count, now_us, &terr, &ttkt,
                           NULL);
        if (tar != BIND_AUTH_ALLOW) {
            moqr_park_req_t pr;
            bind_park_init(&pr, cn, b, MOQR_AUTH_TRACK_STATUS, ns,
                           tq->track_name, tq->handle._opaque);
            bind_refuse_or_park(b, cn, tar, &pr, tq->tokens, tq->token_count,
                                ttkt, terr, now_us);
            break;
        }
        moqr_result_t trc;
        moq_bytes_t tname = tq->track_name;
        bind_stash_ftn(b, &ns, &tname);
        BIND_CALL_RETRY(trc, moqr_core_track_status(
            b->core, cn->binding, ns, tname, tq->handle._opaque));
        if (trc != MOQR_OK) {
            /* A ring-full status request would otherwise leave the peer
             * waiting forever; the invariant + drain makes trc==OK, but
             * if it ever weren't, reject on the wire rather than drop. */
            bind_refuse(b, cn, MOQR_AUTH_TRACK_STATUS, tq->handle._opaque,
                        MOQ_REQUEST_ERROR_INTERNAL_ERROR, false, 0, now_us);
        }
        break;
    }
    case MOQ_EVENT_PUBLISH_REQUEST: {
        const moq_publish_request_event_t *pq = &ev->u.publish_request;
        moqr_ns_t ns = ns_from_event(&pq->track_namespace, nsbuf);
        b_meta_publish_t *p = bind_meta_pub_reserve(b, cn, pq->pub._opaque);
        if (p == NULL) {
            b->session_errors++;   /* headroom invariant, no consumed orphan */
            break;
        }
        uint64_t perr = 0;
        uint64_t ptkt = 0;
        bind_auth_result_t par =
            bind_authorize(b, cn, MOQR_AUTH_PUBLISH, ns, pq->track_name,
                           pq->tokens, pq->token_count, now_us, &perr, &ptkt,
                           NULL);
        if (par != BIND_AUTH_ALLOW && par != BIND_AUTH_DEFER) {
            bind_meta_pub_refuse(b, cn, p, perr != 0 ? perr :
                                  MOQ_REQUEST_ERROR_UNAUTHORIZED, now_us);
            break;
        }
        moqr_bind_meta_ref_t input = bind_meta_input(
            cn, pq->pub._opaque, 0, pq->track_properties);
        if (bind_meta_copy(b, &input, &p->snapshot) != MOQR_OK) {
            bind_meta_pub_refuse(b, cn, p, MOQ_REQUEST_ERROR_INTERNAL_ERROR, now_us);
            break;
        }
        if (par != BIND_AUTH_ALLOW) {
            moqr_park_req_t pr;
            bind_park_init(&pr, cn, b, MOQR_AUTH_PUBLISH, ns, pq->track_name,
                           pq->pub._opaque);
            p->state = B_META_PUB_AUTH;
            p->ticket = ptkt;
            if (bind_park(b, &pr, pq->tokens, pq->token_count, ptkt, now_us) !=
                MOQR_OK) {
                bind_meta_pub_refuse(b, cn, p, MOQ_REQUEST_ERROR_UNAUTHORIZED, now_us);
            }
            break;
        }
        moqr_track_t track;
        moqr_result_t prc;
        moq_bytes_t pname = pq->track_name;
        bind_stash_ftn(b, &ns, &pname);
        BIND_CALL_RETRY(prc, moqr_core_publish_open(
            b->core, cn->binding, ns, pname, pq->pub._opaque, &track));
        if (prc != MOQR_OK) {
            /* A drain can retire this source; do not resurrect its stage. */
            p = bind_meta_pub_find(b, cn, pq->pub._opaque);
            if (p != NULL) {
                if (prc == MOQR_ERR_CAPACITY) {
                    bind_meta_pub_refuse_ex(b, cn, p, bind_capacity_error(cn),
                                            true,
                                            bind_capacity_retry_ms(pq->pub._opaque),
                                            now_us);
                } else {
                    bind_meta_pub_refuse(b, cn, p, MOQ_REQUEST_ERROR_INTERNAL_ERROR, now_us);
                }
            }
        }
        break;
    }
    case MOQ_EVENT_FETCH_OK:
    case MOQ_EVENT_FETCH_OBJECT:
    case MOQ_EVENT_FETCH_GAP:
    case MOQ_EVENT_FETCH_COMPLETE:
    case MOQ_EVENT_FETCH_ERROR:
    case MOQ_EVENT_FETCH_RESET:
        /* This relay as the FETCHER of a forwarded response. An event that
         * names no live transaction is left to the pump's event cleanup. */
        (void)fwd_on_up_event(b, cn, ev, now_us);
        break;
    case MOQ_EVENT_FETCH_CANCELLED:
        if (!bind_refusal_drop(b, cn, ev->u.fetch_cancelled.fetch._opaque) &&
            !bind_standalone_cancelled(b, cn, ev->u.fetch_cancelled.fetch)) {
            (void)fwd_on_down_cancelled(b, cn, ev->u.fetch_cancelled.fetch._opaque);
        }
        break;
    case MOQ_EVENT_FETCH_REQUEST: {
        const moq_fetch_request_event_t *fq = &ev->u.fetch_request;
        moqr_ns_t fns = ns_from_event(&fq->track_namespace, nsbuf);
        /* Authorize FIRST so unsupported fetch shapes are never a pre-auth
         * support oracle. A DEFER also fails closed (no parked fetch yet). */
        uint64_t ferr = 0;
        uint64_t ftkt = 0;
        bind_auth_result_t far =
            bind_authorize(b, cn, MOQR_AUTH_FETCH, fns, fq->track_name,
                           fq->tokens, fq->token_count, now_us, &ferr, &ftkt,
                           NULL);
        if (far != BIND_AUTH_ALLOW) {
            bind_fetch_refuse(b, cn, fq->fetch,
                              ferr != 0 ? ferr : MOQ_REQUEST_ERROR_UNAUTHORIZED, now_us);
            break;
        }
        /* Then reject unsupported shapes: a join of a PUBLISH-initiated
         * subscription, and a descending join. */
        if (fq->joining_pub._opaque != 0 ||
            (fq->group_order == MOQ_GROUP_ORDER_DESCENDING &&
             fq->joining_sub._opaque != 0)) {
            bind_fetch_refuse(b, cn, fq->fetch, MOQ_REQUEST_ERROR_NOT_SUPPORTED, now_us);
            break;
        }
        /* An ascending join of a SUBSCRIBE-initiated subscription is forwarded
         * upstream as a bounded transaction (its identity is resolved and
         * authorized there; the empty-identity authorization above cannot
         * admit a join by itself). */
        if (fq->joining_sub._opaque != 0) {
            fwd_admit(b, cn, fq, bind_fetch_options(ev), now_us);
            break;
        }
        /* A range stated relative to the Largest Object resolves against
         * this relay's own Largest, in the same call that plans it. */
        const moq_fetch_request_options_t *fo = bind_fetch_options(ev);
        moqr_fetch_req_t freq;
        moqr_fetch_req_init(&freq);
        freq.ns = fns;
        freq.name = fq->track_name;
        freq.start_group = fq->start_group;
        freq.start_object = fq->start_object;
        freq.end_group = fq->end_group;
        freq.end_object = fq->end_object;
        freq.group_order = (moqr_group_order_t)fq->group_order;
        freq.subscriber_priority = fq->subscriber_priority;
        freq.cookie = fq->fetch._opaque;
        /* Every Location of the response is served or marked: the log's
         * holes and the runs it has no evidence about come back as items
         * (bind_fetch_pump marks them). */
        freq.report_gaps = true;
        freq.report_unknown = true;
        /* An Object still arriving is waited for, up to the deadline. */
        freq.report_pending = true;
        bind_fetch_range(fo, &freq);
        /* Served from cache and, for what it does not hold, upstream, on
         * every draft: the responder serves the available Objects in order
         * and waits for upstream confirmation of the ones it does not hold
         * (draft-16 9.16.3, draft-18 10.12.3, draft-22 3.2.3). The shard owning
         * the track answers it when that is another shard. */
        bind_standalone_request(b, cn, fq, &freq, fo, NULL, now_us);
        break;
    }
    case MOQ_EVENT_FILL_REQUEST:
        bind_fill_request(b, cn, &ev->u.fill_request, now_us);
        break;
    case MOQ_EVENT_FILL_TERMINAL:
        bind_fill_terminal(b, cn, ev->u.fill_terminal.fill);
        break;
    case MOQ_EVENT_SUBSCRIBE_TRACKS_REQUEST: {
        const moq_subscribe_tracks_request_event_t *sq =
            &ev->u.subscribe_tracks_request;
        const moq_subscribe_tracks_options_t *to = bind_tracks_options(ev);
        if (cn->wire_draft != B_DRAFT_22 || to == NULL) {
            /* Track discovery is served on draft-22 only. */
            bind_refuse(b, cn, B_REFUSE_SUBSCRIBE_TRACKS, sq->handle._opaque,
                        MOQ_REQUEST_ERROR_NOT_SUPPORTED, false, 0, now_us);
            break;
        }
        /* §3.6: the subscriber must be authorized for this namespace
         * subscription; each track it discovers is authorized again. */
        moqr_ns_t prefix = ns_from_event(&sq->track_namespace_prefix, nsbuf);
        uint64_t terr = 0, ttkt = 0;
        bind_auth_result_t tar =
            bind_authorize(b, cn, MOQR_AUTH_SUBSCRIBE_NAMESPACE, prefix,
                           (moq_bytes_t){ NULL, 0 }, sq->tokens,
                           sq->token_count, now_us, &terr, &ttkt, NULL);
        if (tar != BIND_AUTH_ALLOW) {
            moqr_park_req_t pr;
            bind_park_init(&pr, cn, b, MOQR_AUTH_SUBSCRIBE_NAMESPACE, prefix,
                           (moq_bytes_t){ NULL, 0 }, sq->handle._opaque);
            pr.sub_forward_off = !sq->forward;
            pr.sub_group_order = (uint32_t)to->group_order;
            pr.sub_start_object = to->include_properties ? 1u : 0u;
            bind_refuse_or_park(b, cn, tar, &pr, sq->tokens, sq->token_count,
                                ttkt, terr, now_us);
            break;
        }
        if (sq->token_count > MOQR_AUTH_MAX_TOKENS) {
            bind_tracks_reject(b, cn, sq->handle._opaque,
                               MOQ_REQUEST_ERROR_UNAUTHORIZED, now_us);
            break;
        }
        moqr_auth_token_t mt[MOQR_AUTH_MAX_TOKENS];
        bind_tokens_to_relay(sq->tokens, sq->token_count, mt);
        bind_tracks_admit(b, cn, sq->handle._opaque, prefix, sq->forward,
                          to->include_properties, (uint32_t)to->group_order,
                          sq->token_count > 0 ? mt : NULL, sq->token_count,
                          now_us);
        break;
    }
    case MOQ_EVENT_SUBSCRIBE_TRACKS_CANCELLED: {
        /* Cancelling ends discovery only; subscriptions it created stay. */
        uint64_t h = ev->u.subscribe_tracks_cancelled.handle._opaque;
        bind_nsans_forget(b, cn, h);
        bind_nsu_retire(b, cn, B_NSU_TRACKS, h, now_us);
        break;
    }
    case MOQ_EVENT_NAMESPACE_SUBSCRIPTION_TERMINATED: {
        const moq_ns_subscription_terminated_event_t *nt =
            &ev->u.ns_subscription_terminated;
        if (nt->local_role == MOQ_REQUEST_ROLE_REQUESTER &&
            nt->handle._opaque == cn->cluster_discovery._opaque) {
            bind_cluster_discovery_end(b, cn, nt->handle._opaque, now_us);
            break;
        }
        bind_cluster_adverts_clear(b, cn, nt->handle._opaque);
        if (cn->wire_draft != B_DRAFT_22 ||
            nt->local_role != MOQ_REQUEST_ROLE_RESPONDER) {
            break;
        }
        b_nsans_t *held = bind_nsans_find(b, cn, nt->handle._opaque);
        if (held != NULL) {
            bind_nsans_drop(b, held);
        }
        bind_nsu_retire(b, cn, B_NSU_NS, nt->handle._opaque, now_us);
        break;
    }
    case MOQ_EVENT_NAMESPACE_UPDATE_REQUEST:
        bind_nsu_authorize(b, cn, B_NSU_ANN,
                           ev->u.namespace_update_request.ann._opaque,
                           ev->u.namespace_update_request.update_request_id,
                           MOQR_AUTH_REQUEST_UPDATE, now_us);
        break;
    case MOQ_EVENT_NS_SUB_UPDATE_REQUEST:
        bind_nsu_authorize(b, cn, B_NSU_NS,
                           ev->u.ns_sub_update_request.handle._opaque,
                           ev->u.ns_sub_update_request.update_request_id,
                           MOQR_AUTH_REQUEST_UPDATE, now_us);
        break;
    case MOQ_EVENT_SUBSCRIBE_TRACKS_UPDATE_REQUEST:
        bind_nsu_authorize(b, cn, B_NSU_TRACKS,
                           ev->u.subscribe_tracks_update_request.handle._opaque,
                           ev->u.subscribe_tracks_update_request.update_request_id,
                           MOQR_AUTH_REQUEST_UPDATE, now_us);
        break;
    case MOQ_EVENT_NAMESPACE_UPDATE_RESULT:
    case MOQ_EVENT_NS_SUB_UPDATE_RESULT:
    case MOQ_EVENT_SUBSCRIBE_TRACKS_UPDATE_RESULT: {
        /* The session refused a peer update itself (a token failure,
         * PREFIX_OVERLAP or an unservable filter) and ended the request: the
         * relay stops serving it. The three result layouts are identical. */
        const moq_namespace_update_result_event_t *ur =
            &ev->u.namespace_update_result;
        if (ur->outcome != MOQ_NAMESPACE_UPDATE_OUTCOME_REFUSED) {
            break;
        }
        uint64_t h = ur->ann._opaque;
        bind_nsans_forget(b, cn, h);
        bind_nsu_retire(b, cn, bind_nsu_family(h), h, now_us);
        break;
    }
    case MOQ_EVENT_PUBLISH_OK:
        bind_publication_answered(b, cn, ev->u.publish_ok.pub._opaque, true,
                                  ev->u.publish_ok.send_allowed, now_us);
        break;
    case MOQ_EVENT_PUBLISH_ERROR:
        bind_publication_answered(b, cn, ev->u.publish_error.pub._opaque,
                                  false, false, now_us);
        break;
    case MOQ_EVENT_PUBLISH_UNSUBSCRIBED:
        bind_publication_answered(b, cn, ev->u.publish_unsubscribed.pub._opaque,
                                  false, false, now_us);
        break;
    case MOQ_EVENT_PUBLISH_UPDATED: {
        /* The subscriber's REQUEST_UPDATE following PUBLISH_OK on a
         * discovery publication: an ordinary subscription update (§3.6.2),
         * answered like one -- a selected Location Filter replaces the
         * relay subscription's, a fill it carries is admitted with the
         * acceptance and served like any fill, and Forward applies. */
        const moq_publish_updated_event_t *pu = &ev->u.publish_updated;
        bool owed = ev->detail_size >=
                        offsetof(moq_publish_updated_event_t, update_request_id) +
                            sizeof(pu->update_request_id) &&
                    pu->answer_owed;
        if (owed) {
            bind_update_answer_for(b, cn, pu->pub._opaque,
                                   pu->update_request_id, pu->has_forward,
                                   pu->forward, bind_update_filter_applies(cn),
                                   now_us);
            break;
        }
        b_dsub_t *ds = dsub_find(b, cn, pu->pub._opaque);
        if (ds == NULL || !ds->publication) {
            break;
        }
        const moq_subscription_update_options_t *options =
            ev->detail_size >= offsetof(moq_publish_updated_event_t, options) +
                                   sizeof(pu->options)
                ? pu->options : NULL;
        moq_location_filter_cfg_t loc;
        if (bind_update_filter_applies(cn) && options != NULL &&
            (options->flags & MOQ_SUBSCRIPTION_UPDATE_OPTIONS_LOCATION) &&
            bind_update_resolved_location(options, pu->update_request_id,
                                          &loc) == MOQR_OK) {
            moqr_result_t lrc = moqr_core_sub_set_location_filter(
                b->core, ds->rsub, &loc, now_us);
            if (lrc != MOQR_OK && lrc != MOQR_ERR_STALE_HANDLE) {
                b->session_errors++;
            }
        }
        if (pu->has_forward) {
            moqr_result_t frc = moqr_core_sub_set_forward(
                b->core, ds->rsub, pu->forward, now_us);
            if (frc != MOQR_OK && frc != MOQR_ERR_STALE_HANDLE) {
                b->session_errors++;
            }
        }
        break;
    }
    case MOQ_EVENT_SETUP_COMPLETE: {
        const moq_setup_complete_event_t *su = &ev->u.setup_complete;
        if (bind_authorize(b, cn, MOQR_AUTH_CLIENT_SETUP,
                           (moqr_ns_t){ NULL, 0 }, (moq_bytes_t){ NULL, 0 },
                           su->tokens, su->token_count, now_us, NULL,
                           NULL, NULL) != BIND_AUTH_ALLOW) {
            /* Setup DENY (and DEFER, which fails closed) hard-closes the
             * connection with the session-level UNAUTHORIZED code.
             * moq_session_close queues CLOSE_SESSION — the adapter drains it
             * to the peer — plus SESSION_CLOSED; the session stays
             * adapter-owned, so detaching the binding here is safe and the
             * close still reaches the wire. The reason is a static literal so
             * it outlives the queued action/event. */
            (void)moq_session_close(cn->session, BIND_SESSION_UNAUTHORIZED,
                                    "unauthorized", now_us);
            conn_detach(b, cn, now_us);
        }
        break;
    }
    case MOQ_EVENT_SESSION_CLOSED:
        conn_detach(b, cn, now_us);
        break;
    default:
        break;
    }
}
