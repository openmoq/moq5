#include "bind_internal.h"
#include "authorization_internal.h"
#include "discovery_internal.h"
#include "fetch_joining_internal.h"
#include "intents_internal.h"
#include "metadata_internal.h"
#include "request_answers_internal.h"
#include "subscription_update_internal.h"

static void bind_retain_request_tokens(moqr_bind_t *b, b_conn_t *cn,
                                       uint64_t handle,
                                       const moqr_auth_token_t *tokens,
                                       size_t count);


/* Authorize a control request against the core's auth hook via the shared,
 * unit-tested seam. On DENY/FAIL_CLOSED, *err gets the wire error code; on
 * DEFER, *ticket gets the hook's (nonzero) correlation ticket; on ALLOW, *lease
 * (when non-NULL) gets the verdict's revalidate_after_us — the caller passes a
 * pointer only for the grantable actions (SUBSCRIBE / PUBLISH_NAMESPACE) that
 * create a CAT revalidation grant, and NULL everywhere else. */
bind_auth_result_t
bind_authorize(moqr_bind_t *b, const b_conn_t *cn, moqr_auth_action_t action,
               moqr_ns_t ns, moq_bytes_t name,
               const moq_resolved_token_t *tokens, size_t token_count,
               uint64_t now_us, uint64_t *err, uint64_t *ticket, uint64_t *lease)
{
    moqr_auth_verdict_t v;
    if (!moqr_bind_auth_eval(b->core, action, ns, name, tokens, token_count,
                             (uint64_t)(cn - b->conns) + 1u, now_us, &v)) {
        if (err != NULL) {
            *err = MOQ_REQUEST_ERROR_UNAUTHORIZED;
        }
        return BIND_AUTH_FAIL_CLOSED; /* token set couldn't be mirrored */
    }
    switch (v.decision) {
    case MOQR_AUTH_ALLOW:
        if (lease != NULL) {
            *lease = v.revalidate_after_us;
        }
        return BIND_AUTH_ALLOW;
    case MOQR_AUTH_DEFER:
        if (v.ticket == 0) {
            if (err != NULL) {
                *err = MOQ_REQUEST_ERROR_UNAUTHORIZED;
            }
            return BIND_AUTH_FAIL_CLOSED; /* DEFER needs a nonzero ticket */
        }
        if (ticket != NULL) {
            *ticket = v.ticket;
        }
        return BIND_AUTH_DEFER;
    default: /* canonicalized DENY */
        if (err != NULL) {
            *err = v.error_code != 0 ? v.error_code
                                     : MOQ_REQUEST_ERROR_UNAUTHORIZED;
        }
        return BIND_AUTH_DENY;
    }
}


/* -- Answering downstream subscription updates --------------------------------- *
 * Every attached session leaves the answer to a subscriber's update to the
 * binding (moq_session_set_defer_subscription_updates at attach), so the
 * relay decides before anything is acknowledged: the session sends
 * REQUEST_UPDATE_OK only once the relay has applied what it schedules on,
 * and a refusal is REQUEST_UPDATE_ERROR followed by the subscription's
 * PUBLISH_DONE UPDATE_FAILED, both written by the session. An answer the
 * session cannot take yet is held on the subscription's tracking slot and
 * retried every pump; it never holds up another subscription's work. */

moq_bytes_t bind_subscribe_error_reason(moq_request_error_t code);


void
bind_park_init(moqr_park_req_t *pr, const b_conn_t *cn, const moqr_bind_t *b,
               moqr_auth_action_t action, moqr_ns_t ns, moq_bytes_t name,
               uint64_t session_cookie)
{
    memset(pr, 0, sizeof(*pr));
    pr->action = action;
    pr->binding_cookie = (uint64_t)(cn - b->conns) + 1u;
    pr->session_cookie = session_cookie;
    pr->ns = ns;
    pr->name = name;
}


/* Mirror the event's borrowed tokens (fail closed if the set can't be
 * faithfully mirrored) and hand the request to core parked storage, which
 * deep-copies it. */
moqr_result_t
bind_park(moqr_bind_t *b, moqr_park_req_t *pr,
          const moq_resolved_token_t *tokens, size_t token_count,
          uint64_t ticket, uint64_t now_us)
{
    if (token_count > MOQR_BIND_AUTH_MAX_TOKENS ||
        (token_count > 0 && tokens == NULL)) {
        return MOQR_ERR_INVAL; /* fail closed, no truncation */
    }
    moqr_auth_token_t mt[MOQR_BIND_AUTH_MAX_TOKENS];
    for (size_t i = 0; i < token_count; i++) {
        mt[i].token_type = tokens[i].token_type;
        mt[i].token_value = tokens[i].token_value;
    }
    pr->tokens = token_count > 0 ? mt : NULL;
    pr->token_count = token_count;
    return moqr_core_park_sized(b->core, pr, sizeof(*pr), ticket, now_us);
}


/* Reserve a CAT revalidation grant for an ALLOWed SUBSCRIBE / PUBLISH_NAMESPACE
 * that carries a lease, BEFORE the core op mutates state. `pr` is already
 * populated by bind_park_init (action / cookies / ns / name). A lease of 0 means
 * no grant is wanted: *res is zeroed and MOQR_OK returned. Otherwise the event's
 * borrowed tokens are mirrored (fail closed if the set can't be represented) and
 * the reservation is taken; the core deep-copies the material. Any non-OK return
 * means the caller MUST fail the op closed with no wire accept. On success *res
 * identifies the reservation for a later commit (op OK) or abort (op failed). */
moqr_result_t
bind_grant_reserve(moqr_bind_t *b, moqr_park_req_t *pr,
                   const moq_resolved_token_t *tokens, size_t token_count,
                   uint64_t lease_us, uint64_t now_us, moqr_grant_res_t *res)
{
    memset(res, 0, sizeof(*res));
    if (lease_us == 0) {
        return MOQR_OK; /* no revalidation grant for this ALLOW */
    }
    if (token_count > MOQR_BIND_AUTH_MAX_TOKENS ||
        (token_count > 0 && tokens == NULL)) {
        return MOQR_ERR_INVAL; /* fail closed, no truncation */
    }
    moqr_auth_token_t mt[MOQR_BIND_AUTH_MAX_TOKENS];
    for (size_t i = 0; i < token_count; i++) {
        mt[i].token_type = tokens[i].token_type;
        mt[i].token_value = tokens[i].token_value;
    }
    pr->tokens = token_count > 0 ? mt : NULL;
    pr->token_count = token_count;
    return moqr_core_grant_reserve(b->core, pr, lease_us, now_us, res);
}


/* Fixed diagnostic text only: never expose peer tokens or policy inputs. */
moq_bytes_t
bind_subscribe_error_reason(moq_request_error_t code)
{
    switch (code) {
    case MOQ_REQUEST_ERROR_DOES_NOT_EXIST:
        return MOQ_BYTES_LITERAL("track does not exist");
    case MOQ_REQUEST_ERROR_UNAUTHORIZED:
        return MOQ_BYTES_LITERAL("unauthorized");
    case MOQ_REQUEST_ERROR_TIMEOUT:
        return MOQ_BYTES_LITERAL("timeout");
    case MOQ_REQUEST_ERROR_NOT_SUPPORTED:
        return MOQ_BYTES_LITERAL("not supported");
    case MOQ_REQUEST_ERROR_INVALID_RANGE:
        return MOQ_BYTES_LITERAL("invalid range");
    case MOQ_REQUEST_ERROR_MALFORMED_TRACK:
        return MOQ_BYTES_LITERAL("malformed track");
    case MOQ_REQUEST_ERROR_DUPLICATE_SUBSCRIPTION:
        return MOQ_BYTES_LITERAL("duplicate subscription");
    case MOQ_REQUEST_ERROR_UNINTERESTED:
        return MOQ_BYTES_LITERAL("uninterested");
    case MOQ_REQUEST_ERROR_EXCESSIVE_LOAD:
        return MOQ_BYTES_LITERAL("excessive load");
    case MOQ_REQUEST_ERROR_INTERNAL_ERROR:
        return MOQ_BYTES_LITERAL("internal error");
    default:
        return (moq_bytes_t){ NULL, 0 };
    }
}


void
bind_reject_subscribe_cfg_init(moq_reject_subscribe_cfg_t *cfg,
                               moq_request_error_t code)
{
    moq_reject_subscribe_cfg_init(cfg);
    cfg->error_code = code;
    cfg->reason = bind_subscribe_error_reason(code);
}


/* Retry Interval offered with a capacity refusal: the condition is
 * transient (a live track, subscription or fetch ends and frees its slot).
 * Every client refused by the same full pool would otherwise retry in
 * lockstep, so each request gets its own interval in [MIN, MIN + SPREAD]:
 * "the sender can apply randomization to each retry interval so that retries
 * are spread out over time" (d16 Section 9.8, d18 Section 10.6). Derived
 * from the request handle, so it is deterministic per request. */
#define B_CAPACITY_RETRY_MIN_MS    750u

#define B_CAPACITY_RETRY_SPREAD_MS 500u


uint64_t
bind_capacity_retry_ms(uint64_t request_cookie)
{
    uint64_t z = request_cookie + UINT64_C(0x9E3779B97F4A7C15);   /* SplitMix64 */
    z = (z ^ (z >> 30)) * UINT64_C(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)) * UINT64_C(0x94D049BB133111EB);
    z ^= z >> 31;
    return B_CAPACITY_RETRY_MIN_MS + z % (B_CAPACITY_RETRY_SPREAD_MS + 1u);
}


/* The REQUEST_ERROR code for a request this relay cannot admit because a
 * bounded pool holds only live entries (MOQR_ERR_CAPACITY). draft-18 has a
 * code for exactly this, EXCESSIVE_LOAD (0x9, Section 10.6: "The responder is
 * overloaded and cannot process the request at this time. The sender SHOULD
 * use the Retry Interval to indicate when the request can be retried"), and
 * draft-22 keeps it (0x9, Section 12.3). draft-16's registry (Section 13.4.2) assigns no load code, and 0x9 is
 * unassigned there, so INTERNAL_ERROR is its truthful answer. Either way the
 * refusal carries a nonzero Retry Interval. */
moq_request_error_t
bind_capacity_error(const b_conn_t *cn)
{
    return cn->wire_draft == B_DRAFT_16 ? MOQ_REQUEST_ERROR_INTERNAL_ERROR
                                        : MOQ_REQUEST_ERROR_EXCESSIVE_LOAD;
}


static void
bind_reject_ex(moqr_bind_t *b, b_conn_t *cn, moqr_auth_action_t action,
               uint64_t session_cookie, uint64_t err, bool can_retry,
               uint64_t retry_ms, uint64_t now_us);


/* Refuse a SUBSCRIBE waiting for a publisher that the relay has no room
 * to hold (EXCESSIVE_LOAD: "overloaded and cannot process the request at
 * this time"). Room frees as other subscriptions end, so the request may be
 * retried at once (a Retry Interval of 1). Held like every refusal. */
void
bind_reject_load(moqr_bind_t *b, b_conn_t *cn, moq_subscription_t sub,
                 uint64_t now_us)
{
    bind_reject_ex(b, cn, MOQR_AUTH_SUBSCRIBE, sub._opaque,
                   bind_capacity_error(cn), true, 0, now_us);
}


/* Refuse an authorized SUBSCRIBE whose Location Filter the relay does not
 * serve. A refusal the session cannot take yet is held (without its reason
 * phrase) and retried like every other. */
void
bind_reject_filter(moqr_bind_t *b, b_conn_t *cn, moq_subscription_t sub,
                   uint64_t now_us)
{
    moq_reject_subscribe_cfg_t rj;
    bind_reject_subscribe_cfg_init(&rj, BIND_FILTER_REFUSAL_CODE);
    rj.reason = MOQ_BYTES_LITERAL(BIND_FILTER_REFUSAL_REASON);
    moq_result_t rc = moq_session_reject_subscribe(cn->session, sub, &rj, now_us);
    if (rc == MOQ_ERR_WOULD_BLOCK) {
        b_refusal_t want = {
            .used = true,
            .kind = MOQR_AUTH_SUBSCRIBE,
            .handle = sub._opaque,
            .error_code = BIND_FILTER_REFUSAL_CODE,
        };
        bind_terminal_hold(b, cn, &want);
    } else if (rc != MOQ_OK && !fwd_result_is_gone(rc)) {
        b->session_errors++;
    }
}


/* Reject the pending session request identified by (action, session_cookie)
 * with the wire error code. Shared by the inline refuse path and deferred
 * resolution, reconstructing the handle from the stored cookie. */
void
bind_reject(moqr_bind_t *b, b_conn_t *cn, moqr_auth_action_t action,
            uint64_t session_cookie, uint64_t err, uint64_t now_us)
{
    bind_reject_ex(b, cn, action, session_cookie, err, false, 0, now_us);
}


/* bind_reject for a core admission result: MOQR_ERR_CAPACITY is a capacity
 * refusal (bind_capacity_error, retryable); anything else INTERNAL_ERROR. */
void
bind_reject_admission(moqr_bind_t *b, b_conn_t *cn, moqr_auth_action_t action,
                      uint64_t session_cookie, moqr_result_t rc,
                      uint64_t now_us)
{
    if (rc == MOQR_ERR_CAPACITY) {
        bind_reject_ex(b, cn, action, session_cookie, bind_capacity_error(cn),
                       true, bind_capacity_retry_ms(session_cookie), now_us);
    } else {
        bind_reject(b, cn, action, session_cookie,
                    MOQ_REQUEST_ERROR_INTERNAL_ERROR, now_us);
    }
}


/* Every request family the auth hook gates is refused through bind_refuse,
 * so a refusal the session cannot take yet is held, never lost. */
static void
bind_reject_ex(moqr_bind_t *b, b_conn_t *cn, moqr_auth_action_t action,
               uint64_t session_cookie, uint64_t err, bool can_retry,
               uint64_t retry_ms, uint64_t now_us)
{
    /* A draft-22 track subscription is refused as one, with its own held
     * answer; a PUBLISH with a metadata stage is refused through it. */
    if (action == MOQR_AUTH_SUBSCRIBE_NAMESPACE &&
        bind_cookie_is_track_sub(session_cookie)) {
        bind_tracks_reject(b, cn, session_cookie, err, now_us);
        return;
    }
    if (action == MOQR_AUTH_PUBLISH) {
        b_meta_publish_t *p = bind_meta_pub_find(b, cn, session_cookie);
        if (p != NULL) {
            bind_meta_pub_refuse_ex(b, cn, p, err, can_retry, retry_ms, now_us);
            return;
        }
    }
    switch (action) {
    case MOQR_AUTH_PUBLISH_NAMESPACE:
    case MOQR_AUTH_SUBSCRIBE:
    case MOQR_AUTH_SUBSCRIBE_NAMESPACE:
    case MOQR_AUTH_TRACK_STATUS:
    case MOQR_AUTH_PUBLISH:
    case MOQR_AUTH_FETCH:
        bind_refuse(b, cn, action, session_cookie, err, can_retry, retry_ms,
                    now_us);
        break;
    default:
        break;
    }
}


/* Commit a reserved subscribe grant after moqr_core_subscribe succeeded (no-op
 * when there was no grant). A commit failure is an invariant violation — the
 * reservation was just taken — so revoke the just-created sub: no untracked,
 * unrevalidatable subscription may stay live on the wire. */
void
bind_grant_commit_sub(moqr_bind_t *b, moqr_grant_res_t gres, moqr_sub_t rsub,
                      uint64_t now_us)
{
    if (gres.gen == 0) {
        return; /* no grant (lease 0) */
    }
    if (moqr_core_grant_commit(b->core, gres, rsub._opaque) != MOQR_OK) {
        moqr_result_t vrc;
        /* The grant could not be committed: the truthful terminal is the
         * PUBLISH_DONE registry's INTERNAL_ERROR, not the same-numbered
         * REQUEST_ERROR constant this once borrowed. */
        BIND_CALL_RETRY(vrc, moqr_core_revoke_sub(
                                 b->core, rsub,
                                 bind_local_pd(MOQR_PD_INTERNAL_ERROR),
                                 now_us));
        (void)vrc;
        b->session_errors++;
    }
}


/* Commit a reserved announce grant after moqr_core_announce succeeded. Returns
 * true when the caller should send the wire accept. On the invariant commit
 * failure it undoes the announce and rejects (no untracked namespace stays
 * live) and returns false. A no-grant reservation (lease 0) returns true. */
bool
bind_grant_commit_ann(moqr_bind_t *b, b_conn_t *cn, moqr_grant_res_t gres,
                      moqr_ns_t ns, uint64_t session_cookie, uint64_t now_us)
{
    if (gres.gen == 0) {
        return true; /* no grant (lease 0): proceed to accept */
    }
    if (moqr_core_grant_commit(b->core, gres, 0) != MOQR_OK) {
        moqr_result_t urc;
        BIND_CALL_RETRY(urc, moqr_core_unannounce(b->core, cn->binding, ns));
        (void)urc;
        bind_reject(b, cn, MOQR_AUTH_PUBLISH_NAMESPACE, session_cookie,
                    MOQ_REQUEST_ERROR_INTERNAL_ERROR, now_us);
        b->session_errors++;
        return false;
    }
    return true;
}


/* Resume the original ALLOWed op from a parked view: re-run the same core call
 * + session accept the inline path would have. An ALLOW that carries a CAT
 * lease (`lease` > 0) reserves + commits a revalidation grant around the op,
 * exactly like the synchronous request path — an async ALLOW must be revalidat-
 * able too. On core failure (WOULD_BLOCK exhausted or error), fail closed by
 * rejecting the pending request. */
void
bind_resume(moqr_bind_t *b, b_conn_t *cn, const moqr_park_req_t *v,
            uint64_t lease, uint64_t now_us)
{
    moqr_result_t rc = MOQR_OK;
    switch (v->action) {
    case MOQR_AUTH_PUBLISH_NAMESPACE: {
        /* The pinned view already holds deep-copied moqr_auth_token_t bytes, so
         * reserve straight from it (no session-token mirror). Reserve fails the
         * request closed with no announce; op failure aborts the reservation. */
        moqr_grant_res_t gres;
        memset(&gres, 0, sizeof(gres));
        if (lease > 0 &&
            moqr_core_grant_reserve(b->core, v, lease, now_us, &gres) !=
                MOQR_OK) {
            bind_reject(b, cn, v->action, v->session_cookie,
                        MOQ_REQUEST_ERROR_UNAUTHORIZED, now_us);
            return;
        }
        int ann_slot = ann_store(b, cn, v->session_cookie, v->ns);
        if (ann_slot < 0) {
            if (gres.gen != 0) {
                moqr_core_grant_abort(b->core, gres);
            }
            bind_reject_admission(b, cn, v->action, v->session_cookie,
                                  MOQR_ERR_CAPACITY, now_us);
            return;
        }
        /* The stored copy, not the parked material: retries drain intents. */
        moqr_ns_t ans = cn->anns[ann_slot].ns;
        /* Retained before the announce: its intent drain may end the
         * parked material this view borrows. */
        bind_retain_request_tokens(b, cn, v->session_cookie, v->tokens,
                                   v->token_count);
        rc = bind_announce(b, cn, ans, v->session_cookie, now_us);
        if (rc != MOQR_OK) {
            moqr_core_request_tokens_clear(b->core, cn->binding,
                                           v->session_cookie);
            ann_clear_slot(b, cn, (uint32_t)ann_slot);
            if (gres.gen != 0) {
                moqr_core_grant_abort(b->core, gres);
            }
            break;
        }
        if (bind_grant_commit_ann(b, cn, gres, ans, v->session_cookie,
                                  now_us)) {
            bind_accept(b, cn, B_ACCEPT_NAMESPACE, v->session_cookie, now_us);
        } else {
            moqr_core_request_tokens_clear(b->core, cn->binding,
                                           v->session_cookie);
            ann_clear_slot(b, cn, (uint32_t)ann_slot);
        }
        return;
    }
    case MOQR_AUTH_SUBSCRIBE: {
        if (v->sub_filter_type == BIND_PARKED_FILTER_REFUSED) {
            /* Authorized, but its Location Filter is one the relay does
             * not serve: the refusal it was parked with. */
            bind_reject_filter(b, cn,
                               (moq_subscription_t){ ._opaque =
                                                         v->session_cookie },
                               now_us);
            return;
        }
        moqr_grant_res_t gres;
        memset(&gres, 0, sizeof(gres));
        if (lease > 0 &&
            moqr_core_grant_reserve(b->core, v, lease, now_us, &gres) !=
                MOQR_OK) {
            bind_reject(b, cn, v->action, v->session_cookie,
                        MOQ_REQUEST_ERROR_UNAUTHORIZED, now_us);
            return;
        }
        moqr_subscribe_req_t rq;
        moqr_subscribe_req_init(&rq);
        rq.ns = v->ns;
        rq.name = v->name;
        rq.filter.type = (moqr_filter_type_t)v->sub_filter_type;
        rq.filter.start_group = v->sub_start_group;
        rq.filter.start_object = v->sub_start_object;
        rq.filter.end_group_delta = v->sub_end_group_delta;
        rq.subscriber_priority = v->sub_priority;
        rq.group_order = (moqr_group_order_t)v->sub_group_order;
        rq.cookie = v->session_cookie;
        /* The Forward State the SUBSCRIBE asked for: a paused one is not
         * scheduled until an update turns it on. */
        rq.forward = !v->sub_forward_off;
        rq.range_end_holds = bind_range_end_holds(cn);
        /* The deadline taken at first receipt: a deferred authorization
         * never renews it. */
        rq.rendezvous = v->sub_rendezvous;
        rq.rendezvous_deadline_us = v->sub_rendezvous_deadline_us;
        moq_location_filter_cfg_t location;
        if (cn->wire_draft == B_DRAFT_22) {
            if (bind_read_sub_location(cn,
                    (moq_subscription_t){ ._opaque = v->session_cookie },
                    &location) != MOQR_OK) {
                if (gres.gen != 0) moqr_core_grant_abort(b->core, gres);
                bind_reject_filter(b, cn,
                    (moq_subscription_t){ ._opaque = v->session_cookie }, now_us);
                return;
            }
            rq.location = &location;
        }
        moqr_sub_t rs;
        BIND_CALL_RETRY(rc, moqr_core_subscribe(b->core, cn->binding, &rq, &rs));
        if (rc != MOQR_OK) {
            if (gres.gen != 0) {
                moqr_core_grant_abort(b->core, gres);
            }
            if (rc == MOQR_ERR_CAPACITY && v->sub_rendezvous) {
                bind_reject_load(b, cn,
                    (moq_subscription_t){ ._opaque = v->session_cookie }, now_us);
                return;
            }
            break;
        }
        bind_grant_commit_sub(b, gres, rs, now_us);
        return; /* ACCEPT_SUB / REJECT_SUB flows through intents */
    }
    case MOQR_AUTH_SUBSCRIBE_NAMESPACE:
        if (bind_cookie_is_track_sub(v->session_cookie)) {
            bind_tracks_admit(b, cn, v->session_cookie, v->ns,
                              !v->sub_forward_off, v->sub_start_object != 0,
                              v->sub_group_order, v->tokens, v->token_count,
                              now_us);
            return;
        }
        BIND_CALL_RETRY(rc, moqr_core_ns_subscribe(b->core, cn->binding, v->ns,
                                                   v->session_cookie));
        if (rc == MOQR_OK) {
            bind_retain_request_tokens(b, cn, v->session_cookie, v->tokens,
                                       v->token_count);
            bind_accept(b, cn, B_ACCEPT_NS_SUB, v->session_cookie, now_us);
            return;
        }
        break;
    case MOQR_AUTH_TRACK_STATUS:
        BIND_CALL_RETRY(rc, moqr_core_track_status(b->core, cn->binding, v->ns,
                                                   v->name, v->session_cookie));
        if (rc == MOQR_OK) {
            return;
        }
        break;
    case MOQR_AUTH_PUBLISH: {
        b_meta_publish_t *p = bind_meta_pub_find(b, cn, v->session_cookie);
        if (p == NULL || p->state != B_META_PUB_AUTH) {
            return;   /* cancelled/retired while authorization was outstanding */
        }
        p->state = B_META_PUB_CORE;
        moqr_track_t tr;
        BIND_CALL_RETRY(rc, moqr_core_publish_open(b->core, cn->binding, v->ns,
                                                   v->name, v->session_cookie,
                                                   &tr));
        if (rc != MOQR_OK) {
            p = bind_meta_pub_find(b, cn, v->session_cookie);
            if (p != NULL) bind_meta_pub_refuse(b, cn, p, MOQ_REQUEST_ERROR_INTERNAL_ERROR, now_us);
        }
        return;
    }
    default:
        return;
    }
    /* Resume failed: reject the pending request rather than leave it hanging. */
    bind_reject_admission(b, cn, v->action, v->session_cookie, rc, now_us);
}


/* Handle a non-ALLOW result at a request site: DEFER parks (fail closed if the
 * park itself fails), and DENY/FAIL_CLOSED/park-failure reject. */
void
bind_refuse_or_park(moqr_bind_t *b, b_conn_t *cn, bind_auth_result_t ar,
                    moqr_park_req_t *pr, const moq_resolved_token_t *tokens,
                    size_t token_count, uint64_t ticket, uint64_t err,
                    uint64_t now_us)
{
    if (ar == BIND_AUTH_DEFER &&
        bind_park(b, pr, tokens, token_count, ticket, now_us) == MOQR_OK) {
        /* Parked: leave the session request pending, and reserve the refusal
         * entry its resolution may need. The poll that delivered this request
         * guaranteed an unreserved free entry, and the core's max_parked
         * bounds the reservations the table is sized for. */
        cn->refusals_reserved++;
        return;
    }
    /* DENY / FAIL_CLOSED / park failure -> reject fail-closed. A valid DEFER
     * leaves err == 0 (bind_authorize only fills it for DENY/FAIL_CLOSED), so a
     * park failure (duplicate ticket / capacity / byte budget) must still carry
     * a definite wire code, never 0. */
    if (err == 0) {
        err = MOQ_REQUEST_ERROR_UNAUTHORIZED;
    }
    bind_reject(b, cn, pr->action, pr->session_cookie, err, now_us);
}

/* A draft-22 SUBSCRIBE with a positive RENDEZVOUS_TIMEOUT waits for a
 * publisher until one deadline, taken now, at the request's first receipt,
 * and never renewed. Absent or zero waits not at all. */
bool
bind_sub_rendezvous(const b_conn_t *cn, const moq_event_t *ev, uint64_t now_us,
                    uint64_t *deadline_us)
{
    const moq_subscribe_request_event_t *sq = &ev->u.subscribe_request;
    *deadline_us = 0;
    if (cn->wire_draft != B_DRAFT_22 ||
        ev->detail_size < offsetof(moq_subscribe_request_event_t, options) +
                              sizeof(sq->options) ||
        sq->options == NULL) {
        return false;
    }
    const moq_subscription_admission_options_t *o = sq->options;
    if (o->struct_size <
            offsetof(moq_subscription_admission_options_t,
                     rendezvous_timeout_ms) +
                sizeof(o->rendezvous_timeout_ms) ||
        (o->flags & MOQ_SUBSCRIPTION_ADMISSION_RENDEZVOUS_TIMEOUT) == 0 ||
        o->rendezvous_timeout_ms == 0) {
        return false;
    }
    uint64_t ms = o->rendezvous_timeout_ms < BIND_RENDEZVOUS_MAX_MS
                      ? o->rendezvous_timeout_ms
                      : BIND_RENDEZVOUS_MAX_MS;
    uint64_t us = ms * 1000u;
    *deadline_us = now_us > UINT64_MAX - us ? UINT64_MAX : now_us + us;
    return true;
}


/* Retain a draft-22 namespace request's tokens, so an update that carries
 * none is authorized against them. A failure leaves none retained, which
 * can only narrow what a later update is authorized for. */
static void
bind_retain_request_tokens(moqr_bind_t *b, b_conn_t *cn, uint64_t handle,
                           const moqr_auth_token_t *tokens, size_t count)
{
    if (cn->wire_draft != B_DRAFT_22) {
        return;
    }
    (void)moqr_core_request_tokens_set(b->core, cn->binding, handle,
                                       count > 0 ? tokens : NULL, count);
}


void
bind_retain_session_tokens(moqr_bind_t *b, b_conn_t *cn, uint64_t handle,
                           const moq_resolved_token_t *tokens, size_t count)
{
    if (count > MOQR_AUTH_MAX_TOKENS || (count > 0 && tokens == NULL)) {
        return;
    }
    moqr_auth_token_t mt[MOQR_AUTH_MAX_TOKENS];
    bind_tokens_to_relay(tokens, count, mt);
    bind_retain_request_tokens(b, cn, handle, mt, count);
}


/* The draft-22 SUBSCRIBE_TRACKS options, or NULL. */
const moq_subscribe_tracks_options_t *
bind_tracks_options(const moq_event_t *ev)
{
    const moq_subscribe_tracks_request_event_t *sq =
        &ev->u.subscribe_tracks_request;
    if (ev->detail_size < offsetof(moq_subscribe_tracks_request_event_t,
                                   options) + sizeof(sq->options) ||
        sq->options == NULL ||
        sq->options->struct_size <
            offsetof(moq_subscribe_tracks_options_t, include_properties) +
                sizeof(sq->options->include_properties)) {
        return NULL;
    }
    return sq->options;
}
