#include "bind_internal.h"
#include "request_answers_internal.h"
#include "authorization_internal.h"
#include "discovery_internal.h"
#include "fetch_joining_internal.h"
#include "intents_internal.h"
#include "testing_internal.h"

void bind_fetch_refuse(moqr_bind_t *b, b_conn_t *cn, moq_fetch_t fetch,
                       uint64_t error_code, uint64_t now_us);

void bind_refuse(moqr_bind_t *b, b_conn_t *cn, moqr_auth_action_t kind,
                 uint64_t handle, uint64_t error_code, bool can_retry,
                 uint64_t retry_ms, uint64_t now_us);

void bind_accept(moqr_bind_t *b, b_conn_t *cn, moqr_auth_action_t kind,
                 uint64_t handle, uint64_t now_us);

void bind_terminal_hold(moqr_bind_t *b, b_conn_t *cn,
                        const b_refusal_t *want);

bool bind_kind_is_accept(moqr_auth_action_t kind);

static bool bind_accept_undo(moqr_bind_t *b, b_conn_t *cn, const b_refusal_t *r,
                             uint64_t now_us);

bool bind_ns_sub_accept_held(const moqr_bind_t *b, const b_conn_t *cn,
                             uint64_t handle);

void bind_refusal_pump(moqr_bind_t *b, uint64_t now_us);

bool bind_refusal_drop(moqr_bind_t *b, b_conn_t *cn, uint64_t handle);

bool bind_refusal_can_poll(const moqr_bind_t *b, const b_conn_t *cn);

void bind_refusal_conn_gone(moqr_bind_t *b, b_conn_t *cn);


/* Submit one refusal to the session: the reject call of its request family,
 * carrying the error code and, when set, the Retry Interval. */
moq_result_t
bind_refusal_submit(b_conn_t *cn, const b_refusal_t *r, uint64_t now_us)
{
    switch (r->kind) {
    case MOQR_AUTH_FETCH: {
        moq_reject_fetch_cfg_t c;
        moq_reject_fetch_cfg_init(&c);
        c.error_code = r->error_code;
        c.can_retry = r->can_retry;
        c.retry_after_ms = r->retry_ms;
        return moq_session_reject_fetch(
            cn->session, (moq_fetch_t){ ._opaque = r->handle }, &c, now_us);
    }
    case MOQR_AUTH_PUBLISH_NAMESPACE: {
        moq_reject_namespace_cfg_t c;
        moq_reject_namespace_cfg_init(&c);
        c.error_code = r->error_code;
        c.can_retry = r->can_retry;
        c.retry_after_ms = r->retry_ms;
        return moq_session_reject_namespace(
            cn->session, (moq_announcement_t){ ._opaque = r->handle }, &c,
            now_us);
    }
    case MOQR_AUTH_SUBSCRIBE: {
        moq_reject_subscribe_cfg_t c;
        bind_reject_subscribe_cfg_init(&c, r->error_code);
        c.can_retry = r->can_retry;
        c.retry_after_ms = r->retry_ms;
        return moq_session_reject_subscribe(
            cn->session, (moq_subscription_t){ ._opaque = r->handle }, &c,
            now_us);
    }
    case MOQR_AUTH_SUBSCRIBE_NAMESPACE: {
        moq_reject_ns_sub_cfg_t c;
        moq_reject_ns_sub_cfg_init(&c);
        c.error_code = r->error_code;
        c.can_retry = r->can_retry;
        c.retry_after_ms = r->retry_ms;
        return moq_session_reject_ns_sub(
            cn->session, (moq_ns_sub_handle_t){ ._opaque = r->handle }, &c,
            now_us);
    }
    case MOQR_AUTH_TRACK_STATUS: {
        moq_reject_track_status_cfg_t c;
        moq_reject_track_status_cfg_init(&c);
        c.error_code = r->error_code;
        c.can_retry = r->can_retry;
        c.retry_after_ms = r->retry_ms;
        return moq_session_reject_track_status(
            cn->session, (moq_track_status_handle_t){ ._opaque = r->handle },
            &c, now_us);
    }
    case MOQR_AUTH_PUBLISH: {
        moq_reject_publish_cfg_t c;
        moq_reject_publish_cfg_init(&c);
        c.error_code = r->error_code;
        c.can_retry = r->can_retry;
        c.retry_after_ms = r->retry_ms;
        return moq_session_reject_publish(
            cn->session, (moq_publication_t){ ._opaque = r->handle }, &c,
            now_us);
    }
    case B_ACCEPT_NAMESPACE: {
        moq_accept_namespace_cfg_t c;
        moq_accept_namespace_cfg_init(&c);
        return moq_session_accept_namespace(
            cn->session, (moq_announcement_t){ ._opaque = r->handle }, &c,
            now_us);
    }
    case B_ACCEPT_NS_SUB: {
        moq_accept_ns_sub_cfg_t c;
        moq_accept_ns_sub_cfg_init(&c);
        return moq_session_accept_ns_sub(
            cn->session, (moq_ns_sub_handle_t){ ._opaque = r->handle }, &c,
            now_us);
    }
    case B_ABORT_FILL:
        return moq_session_abort_fill(
            cn->session, (moq_fill_t){ ._opaque = r->handle }, r->error_code,
            now_us);
    case B_REFUSE_SUBSCRIBE_TRACKS: {
        moq_reject_subscribe_tracks_cfg_t c;
        moq_reject_subscribe_tracks_cfg_init(&c);
        c.error_code = r->error_code;
        return moq_session_reject_subscribe_tracks(
            cn->session,
            (moq_track_sub_handle_t){ ._opaque = r->handle }, &c,
            now_us);
    }
    default:
        return MOQ_ERR_INVAL;
    }
}


/* Refuse a pending downstream request. A refusal the session cannot take yet
 * (WOULD_BLOCK) is held and retried every pump until it commits or the
 * request or connection goes away (b_refusal_t), so the request is always
 * answered. */
void
bind_refuse(moqr_bind_t *b, b_conn_t *cn, moqr_auth_action_t kind,
            uint64_t handle, uint64_t error_code, bool can_retry,
            uint64_t retry_ms, uint64_t now_us)
{
    b_refusal_t want = {
        .used = true,
        .can_retry = can_retry,
        .kind = kind,
        .handle = handle,
        .error_code = error_code,
        .retry_ms = can_retry ? retry_ms : 0,
    };
    moq_result_t rc = bind_refusal_submit(cn, &want, now_us);
    if (rc == MOQ_OK) {
        FWD_DBG(b, refusals_sent);
        return;
    }
    if (rc != MOQ_ERR_WOULD_BLOCK) {
        if (!fwd_result_is_gone(rc)) {
            b->session_errors++;
        }
        return;
    }
    FWD_DBG(b, refusals_blocked);
    bind_terminal_hold(b, cn, &want);
}


/* Hold a terminal the session could not take yet in a free entry. */
void
bind_terminal_hold(moqr_bind_t *b, b_conn_t *cn, const b_refusal_t *want)
{
    for (uint32_t i = 0; i < b->n_refusals; i++) {
        b_refusal_t *r = &cn->refusals[i];
        if (!r->used) {
            *r = *want;
            cn->retry_pending |= B_RETRY_REFUSAL;
            FWD_DBG(b, refusals_parked);
            return;
        }
    }
    /* Unreachable: the table is sized for every terminal the binding can
     * owe (b_refusal_t). Counted so a regression is visible. */
    b->session_errors++;
    FWD_DBG(b, refusals_unparked);
}


bool
bind_kind_is_accept(moqr_auth_action_t kind)
{
    return kind == B_ACCEPT_NAMESPACE || kind == B_ACCEPT_NS_SUB;
}


/* Undo the core state of an acceptance whose request is gone before the
 * acceptance committed: the announce (with its binding copy) or the
 * namespace subscription. True when done (or nothing was left to undo);
 * false when the unannounce must wait for intent room (retried next pump). */
static bool
bind_accept_undo(moqr_bind_t *b, b_conn_t *cn, const b_refusal_t *r,
                 uint64_t now_us)
{
    /* The acceptance never stood: no tokens are retained for it. */
    moqr_core_request_tokens_clear(b->core, cn->binding, r->handle);
    if (r->kind == B_ACCEPT_NS_SUB) {
        /* No new NAMESPACE for it, and none of those already parked behind
         * its acceptance may reach the session. */
        (void)moqr_core_ns_unsubscribe(b->core, cn->binding, r->handle);
        bind_pending_purge(b, (uint64_t)(cn - b->conns) + 1u, true,
                           r->handle);
        FWD_DBG(b, accepts_undone);
        return true;
    }
    moqr_result_t urc = bind_ann_withdraw(b, cn, r->handle, now_us);
    if (urc == MOQR_ERR_WOULD_BLOCK) {
        return false;
    }
    FWD_DBG(b, accepts_undone);
    return true;
}


/* Answer a namespace or namespace subscription the core has admitted. An
 * acceptance the session cannot take yet is held like a refusal; one whose
 * request is already gone undoes the admission. */
void
bind_accept(moqr_bind_t *b, b_conn_t *cn, moqr_auth_action_t kind,
            uint64_t handle, uint64_t now_us)
{
    b_refusal_t want = { .used = true, .kind = kind, .handle = handle };
    moq_result_t rc = bind_refusal_submit(cn, &want, now_us);
    if (rc == MOQ_OK) {
        FWD_DBG(b, accepts_sent);
        return;
    }
    if (rc == MOQ_ERR_WOULD_BLOCK) {
        FWD_DBG(b, accepts_blocked);
        bind_terminal_hold(b, cn, &want);
        return;
    }
    if (!fwd_result_is_gone(rc)) {
        b->session_errors++;
        return;
    }
    want.undo = true;
    if (!bind_accept_undo(b, cn, &want, now_us)) {
        bind_terminal_hold(b, cn, &want);
    }
}


/* Whether a namespace subscription's acceptance is still held: no NAMESPACE
 * may be sent on it before its REQUEST_OK. */
bool
bind_ns_sub_accept_held(const moqr_bind_t *b, const b_conn_t *cn,
                        uint64_t handle)
{
    for (uint32_t i = 0; i < b->n_refusals; i++) {
        const b_refusal_t *r = &cn->refusals[i];
        if (r->used && !r->undo && r->kind == B_ACCEPT_NS_SUB &&
            r->handle == handle) {
            return true;
        }
    }
    return false;
}


/* Refuse a downstream FETCH request before any transaction exists. */
void
bind_fetch_refuse(moqr_bind_t *b, b_conn_t *cn, moq_fetch_t fetch,
                  uint64_t error_code, uint64_t now_us)
{
    bind_refuse(b, cn, MOQR_AUTH_FETCH, fetch._opaque, error_code, false, 0,
                now_us);
}


/* Free entries not promised to a parked authorization. */
uint32_t
bind_refusal_free(const moqr_bind_t *b, const b_conn_t *cn)
{
    uint32_t free_slots = b->n_refusals;
    for (uint32_t i = 0; (cn->retry_pending & B_RETRY_REFUSAL) &&
                         i < b->n_refusals; i++) {
        free_slots -= cn->refusals[i].used ? 1u : 0u;
    }
    return free_slots > cn->refusals_reserved
               ? free_slots - cn->refusals_reserved
               : 0u;
}


/* Whether the pump may poll another event from this connection: a request
 * it polls must be ownable, so a poll's worth of unreserved refusal entries
 * has to be free first, and as many metadata publish stages (a consumed
 * PUBLISH owns one even when its first refusal blocks). */
bool
bind_refusal_can_poll(const moqr_bind_t *b, const b_conn_t *cn)
{
    if (bind_refusal_free(b, cn) < BIND_PUMP_EVENT_POLL) {
        return false;
    }
    uint32_t free_slots = 0;
    for (uint32_t i = 0; i < b->metadata_publish_cap; i++) {
        if (b->metadata_publishes[i].state == 0 &&
            ++free_slots >= BIND_PUMP_EVENT_POLL) {
            return true;
        }
    }
    return false;
}


/* Retry every held refusal on every open connection (one attempt each per
 * pump); a request the session no longer holds is simply forgotten. */
void
bind_refusal_pump(moqr_bind_t *b, uint64_t now_us)
{
    for (uint32_t ci = 0; ci < b->max_conns; ci++) {
        b_conn_t *cn = &b->conns[ci];
        if (!cn->used || cn->closed || cn->session == NULL ||
            !(cn->retry_pending & B_RETRY_REFUSAL)) {
            continue;
        }
        for (uint32_t i = 0; i < b->n_refusals; i++) {
            b_refusal_t *r = &cn->refusals[i];
            if (!r->used) {
                continue;
            }
            if (r->undo) {
                if (bind_accept_undo(b, cn, r, now_us)) {
                    memset(r, 0, sizeof(*r));
                }
                continue;
            }
            bool accept = bind_kind_is_accept(r->kind);
            moq_result_t rc;
#ifdef MOQR_BIND_TESTING
            if (accept && atomic_load(&bind_dbg_accept_gone) > 0 &&
                atomic_fetch_sub(&bind_dbg_accept_gone, 1) == 1) {
                rc = MOQ_ERR_STALE_HANDLE;
            } else
#endif
            {
                rc = bind_refusal_submit(cn, r, now_us);
            }
            if (rc == MOQ_ERR_WOULD_BLOCK) {
                FWD_DBG(b, refusals_retried);   /* HOLD */
                continue;
            }
            if (rc == MOQ_OK) {
                if (accept) {
                    FWD_DBG(b, accepts_sent);
                } else {
                    FWD_DBG(b, refusals_sent);
                }
            } else if (fwd_result_is_gone(rc)) {
                FWD_DBG(b, refusals_gone);   /* retired by the peer meanwhile */
                if (accept) {
                    r->undo = true;
                    if (!bind_accept_undo(b, cn, r, now_us)) {
                        continue;   /* undo blocked: retried next pump */
                    }
                }
            } else {
                b->session_errors++;
            }
            memset(r, 0, sizeof(*r));
        }
        /* Re-read rather than tracked through the walk: a retry above may
         * hold another terminal in an entry the walk already passed. */
        bool held = false;
        for (uint32_t i = 0; !held && i < b->n_refusals; i++) {
            held = cn->refusals[i].used;
        }
        cn->retry_pending = (uint8_t)((cn->retry_pending & ~B_RETRY_REFUSAL) |
                                      (held ? B_RETRY_REFUSAL : 0u));
    }
}


/* The peer cancelled a request whose refusal is still held: nothing is owed. */
bool
bind_refusal_drop(moqr_bind_t *b, b_conn_t *cn, uint64_t handle)
{
    for (uint32_t i = 0; i < b->n_refusals; i++) {
        b_refusal_t *r = &cn->refusals[i];
        if (r->used && r->kind == MOQR_AUTH_FETCH && r->handle == handle) {
            memset(r, 0, sizeof(*r));
            FWD_DBG(b, refusals_dropped);
            return true;
        }
    }
    return false;
}


/* The connection is going away: every held refusal on it is released, and
 * its parked authorizations (retired with the binding) need no entry. */
void
bind_refusal_conn_gone(moqr_bind_t *b, b_conn_t *cn)
{
    cn->refusals_reserved = 0;
    for (uint32_t i = 0; i < b->n_refusals; i++) {
        b_refusal_t *r = &cn->refusals[i];
        if (r->used) {
            memset(r, 0, sizeof(*r));
            FWD_DBG(b, refusals_dropped);
        }
    }
}
