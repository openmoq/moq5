#include "relay_internal.h"
#include "auth_internal.h"

/* Validate the borrowed request view and deep-copy its namespace / name / token
 * bytes into *m (freshly allocated). Fails closed with no allocation and no
 * accounting change: MOQR_ERR_INVAL (ns.count > 32, a positive count/len with a
 * NULL pointer, or a length sum that overflows), MOQR_ERR_CAPACITY (would push
 * *used past cap), MOQR_ERR_NOMEM. On success *used += m->bytes_charged. Shared
 * by parked-DEFER storage and revalidation grants. */
moqr_result_t
r_copy_material(moqr_core_t *c, const moqr_park_req_t *req, uint32_t *used,
                uint32_t cap, r_material *m)
{
    memset(m, 0, sizeof(*m));
    if (req->ns.count > 32) {
        return MOQR_ERR_INVAL; /* moqr_ns_t is 0..32 parts; ns_count is uint32_t */
    }
    if (req->token_count > MOQR_AUTH_MAX_TOKENS) {
        return MOQR_ERR_INVAL; /* bound before any tokens[i] walk */
    }
    if ((req->ns.count > 0 && req->ns.parts == NULL) ||
        (req->name.len > 0 && req->name.data == NULL) ||
        (req->token_count > 0 && req->tokens == NULL)) {
        return MOQR_ERR_INVAL;
    }
    size_t ns_bytes_len = 0;
    for (size_t i = 0; i < req->ns.count; i++) {
        size_t len = req->ns.parts[i].len;
        if (len > 0 && req->ns.parts[i].data == NULL) {
            return MOQR_ERR_INVAL;
        }
        if (ns_bytes_len > SIZE_MAX - len) {
            return MOQR_ERR_INVAL;
        }
        ns_bytes_len += len;
    }
    size_t tok_bytes_len = 0;
    for (size_t i = 0; i < req->token_count; i++) {
        size_t len = req->tokens[i].token_value.len;
        if (len > 0 && req->tokens[i].token_value.data == NULL) {
            return MOQR_ERR_INVAL;
        }
        if (tok_bytes_len > SIZE_MAX - len) {
            return MOQR_ERR_INVAL;
        }
        tok_bytes_len += len;
    }
    if ((req->ns.count != 0 &&
         req->ns.count > SIZE_MAX / sizeof(moq_bytes_t)) ||
        (req->token_count != 0 &&
         req->token_count > SIZE_MAX / sizeof(moqr_auth_token_t))) {
        return MOQR_ERR_INVAL;
    }
    const size_t terms[5] = {
        ns_bytes_len,
        req->name.len,
        tok_bytes_len,
        req->ns.count * sizeof(moq_bytes_t),
        req->token_count * sizeof(moqr_auth_token_t),
    };
    size_t charge = 0;
    for (size_t i = 0; i < 5; i++) {
        if (charge > SIZE_MAX - terms[i]) {
            return MOQR_ERR_INVAL;
        }
        charge += terms[i];
    }
    if ((uint64_t)*used + charge > cap) {
        return MOQR_ERR_CAPACITY;
    }
    moq_bytes_t       *ns_parts = NULL;
    uint8_t           *ns_bytes = NULL;
    uint8_t           *name = NULL;
    moqr_auth_token_t *tokens = NULL;
    uint8_t           *tok_bytes = NULL;
    if (req->ns.count > 0) {
        ns_parts = r_alloc(c, req->ns.count * sizeof(moq_bytes_t));
        ns_bytes = ns_bytes_len > 0 ? r_alloc(c, ns_bytes_len) : NULL;
        if (ns_parts == NULL || (ns_bytes_len > 0 && ns_bytes == NULL)) {
            goto nomem;
        }
        size_t off = 0;
        for (size_t i = 0; i < req->ns.count; i++) {
            size_t len = req->ns.parts[i].len;
            if (len > 0) {
                memcpy(ns_bytes + off, req->ns.parts[i].data, len);
                ns_parts[i].data = ns_bytes + off;
            } else {
                ns_parts[i].data = NULL; /* empty part: never NULL + off */
            }
            ns_parts[i].len = len;
            off += len;
        }
    }
    if (req->name.len > 0) {
        name = r_alloc(c, req->name.len);
        if (name == NULL) {
            goto nomem;
        }
        memcpy(name, req->name.data, req->name.len);
    }
    if (req->token_count > 0) {
        tokens = r_alloc(c, req->token_count * sizeof(moqr_auth_token_t));
        tok_bytes = tok_bytes_len > 0 ? r_alloc(c, tok_bytes_len) : NULL;
        if (tokens == NULL || (tok_bytes_len > 0 && tok_bytes == NULL)) {
            goto nomem;
        }
        size_t off = 0;
        for (size_t i = 0; i < req->token_count; i++) {
            size_t len = req->tokens[i].token_value.len;
            tokens[i].token_type = req->tokens[i].token_type;
            if (len > 0) {
                memcpy(tok_bytes + off, req->tokens[i].token_value.data, len);
                tokens[i].token_value.data = tok_bytes + off;
            } else {
                tokens[i].token_value.data = NULL;
            }
            tokens[i].token_value.len = len;
            off += len;
        }
    }
    m->ns_parts = ns_parts;
    m->ns_count = (uint32_t)req->ns.count;
    m->ns_bytes = ns_bytes;
    m->ns_bytes_len = ns_bytes_len;
    m->name = name;
    m->name_len = req->name.len;
    m->tokens = tokens;
    m->token_count = req->token_count;
    m->tok_bytes = tok_bytes;
    m->tok_bytes_len = tok_bytes_len;
    m->bytes_charged = charge;
    *used += (uint32_t)charge;
    return MOQR_OK;
nomem:
    r_free(c, ns_parts, req->ns.count * sizeof(moq_bytes_t));
    r_free(c, ns_bytes, ns_bytes_len);
    r_free(c, name, req->name.len);
    r_free(c, tokens, req->token_count * sizeof(moqr_auth_token_t));
    r_free(c, tok_bytes, tok_bytes_len);
    return MOQR_ERR_NOMEM;
}


void
r_free_material(moqr_core_t *c, r_material *m, uint32_t *used)
{
    r_free(c, m->ns_parts, (size_t)m->ns_count * sizeof(*m->ns_parts));
    r_free(c, m->ns_bytes, m->ns_bytes_len);
    r_free(c, m->name, m->name_len);
    r_free(c, m->tokens, (size_t)m->token_count * sizeof(*m->tokens));
    r_free(c, m->tok_bytes, m->tok_bytes_len);
    if (*used >= m->bytes_charged) {
        *used -= (uint32_t)m->bytes_charged;
    } else {
        *used = 0;
    }
    memset(m, 0, sizeof(*m));
}

void parked_free_slot(moqr_core_t *c, uint32_t slot);

void grant_free_slot(moqr_core_t *c, uint32_t slot);


void
moqr_core_authorize(moqr_core_t *c, const moqr_auth_request_t *req,
                    moqr_auth_verdict_t *out)
{
    if (out == NULL) {
        return;
    }
    /* Default is allow-all: one predictable branch, no hook call, so the
     * idle-auth path carries no measurable cost. */
    out->decision = MOQR_AUTH_ALLOW;
    out->revalidate_after_us = 0;
    out->error_code = 0;
    out->reason = MOQR_AUTH_REASON_OK;
    out->ticket = 0;
    /* NONE by default: only a hook that explicitly states a revocation
     * terminal gets one, and every other path sees "nothing said". */
    out->revoke_terminal = moqr_pd_desc_none();
    if (c == NULL || req == NULL) {
        return;
    }
    if (c->authorize != NULL) {
        c->authorize(c->authorize_ctx, req, out);
    }

    /* Canonicalize the verdict BEFORE any caller — or the trace/stats — sees
     * it: an out-of-range decision fails closed to DENY/POLICY, and an
     * out-of-range reason on a denial becomes POLICY. So a misbehaving hook
     * can never smuggle a garbage decision past the seam. */
    if (out->decision > MOQR_AUTH_DEFER) {
        out->decision = MOQR_AUTH_DENY;
        out->reason = MOQR_AUTH_REASON_POLICY;
    }
    if (out->decision == MOQR_AUTH_DENY &&
        out->reason >= MOQR_AUTH_REASON__COUNT) {
        out->reason = MOQR_AUTH_REASON_POLICY;
    }

    /* Count + trace with the canonical values (action clamped for the index;
     * the trace keeps the raw action as a diagnostic scalar). */
    uint32_t act = req->action < MOQR_AUTH_ACTION__COUNT
                       ? req->action
                       : MOQR_AUTH_ACTION__COUNT - 1u;
    c->auth_decisions[act][out->decision]++;
    if (out->decision == MOQR_AUTH_DENY) {
        c->auth_denials[out->reason]++;
    }
    r_trace(c, MOQR_TRACE_AUTH_DECISION, out->decision, req->action,
            req->binding_cookie, out->reason, 0);
}


/* -- deferred-auth parked storage ---------------------------------------------------------------- *
 * A DEFER verdict parks the request here (deep-copied) keyed by the hook's
 * external ticket. The binding resumes or rejects it later via begin/finish.
 * Per-entry buffers are fixed once copied, so a mutation during resume (e.g.
 * the resumed subscribe interning the pinned namespace) never moves the pinned
 * view. Bounded by max_parked (slots) and parked_bytes (copied bytes). */

void
parked_free_slot(moqr_core_t *c, uint32_t slot)
{
    r_parked *p = &c->parked[slot];
    r_free_material(c, &p->mat, &c->parked_bytes_used);
    uint32_t gen = p->gen;
    memset(p, 0, sizeof(*p));
    p->gen = r_gen_close(gen); /* internal slot liveness only — external ticket
                                * reuse is the verifier's contract (see relay.h) */
}


/* Live (odd-gen) parked slot holding external ticket `t`, or -1. */
static int
parked_find(const moqr_core_t *c, uint64_t t)
{
    if (t == 0) {
        return -1;
    }
    for (uint32_t i = 0; i < c->max_parked; i++) {
        if ((c->parked[i].gen & 1u) != 0 && c->parked[i].ext_ticket == t) {
            return (int)i;
        }
    }
    return -1;
}


/* The sizeof of the park-request layout through sub_group_order: what a
 * caller built before sub_forward_off was appended allocates and passes. */
#define MOQR_PARK_REQ_SIZE_THROUGH_GROUP_ORDER                               \
    ((offsetof(moqr_park_req_t, sub_group_order) +                           \
      sizeof(((moqr_park_req_t *)0)->sub_group_order) +                      \
      (_Alignof(moqr_park_req_t) - 1)) &                                     \
     ~(_Alignof(moqr_park_req_t) - 1))


moqr_result_t
moqr_core_park(moqr_core_t *c, const moqr_park_req_t *req,
               uint64_t ext_ticket, uint64_t now_us)
{
    return moqr_core_park_sized(c, req, MOQR_PARK_REQ_SIZE_THROUGH_GROUP_ORDER,
                                ext_ticket, now_us);
}


moqr_result_t
moqr_core_park_sized(moqr_core_t *c, const moqr_park_req_t *req,
                     size_t req_size, uint64_t ext_ticket, uint64_t now_us)
{
    (void)now_us;
    if (c == NULL || req == NULL ||
        req_size < MOQR_PARK_REQ_SIZE_THROUGH_GROUP_ORDER) {
        return MOQR_ERR_INVAL;
    }
    if (ext_ticket == 0) {
        return MOQR_ERR_INVAL; /* a zero ticket can never be resolved */
    }
    if (parked_find(c, ext_ticket) >= 0) {
        return MOQR_ERR_STALE_HANDLE; /* duplicate live ticket: never merge */
    }
    int slot = -1;
    for (uint32_t i = 0; i < c->max_parked; i++) {
        if (r_gen_openable(c->parked[i].gen)) {
            slot = (int)i;
            break;
        }
    }
    if (slot < 0) {
        return MOQR_ERR_CAPACITY; /* max_parked exhausted */
    }
    r_material m;
    moqr_result_t rc = r_copy_material(c, req, &c->parked_bytes_used,
                                       c->parked_bytes_cap, &m);
    if (rc != MOQR_OK) {
        return rc; /* INVAL / CAPACITY / NOMEM: fail closed, no state change */
    }
    r_parked *p = &c->parked[slot];
    uint32_t  gen = p->gen;
    memset(p, 0, sizeof(*p));
    p->gen = r_gen_open(gen);
    p->ext_ticket = ext_ticket;
    p->binding_cookie = req->binding_cookie;
    p->session_cookie = req->session_cookie;
    p->action = req->action;
    p->mat = m;
    p->sub_filter_type = req->sub_filter_type;
    p->sub_start_group = req->sub_start_group;
    p->sub_start_object = req->sub_start_object;
    p->sub_end_group_delta = req->sub_end_group_delta;
    p->sub_priority = req->sub_priority;
    p->sub_group_order = req->sub_group_order;
    p->sub_forward_off =
        req_size >= offsetof(moqr_park_req_t, sub_forward_off) +
                        sizeof(req->sub_forward_off) &&
        req->sub_forward_off;
    if (req_size >= offsetof(moqr_park_req_t, sub_rendezvous) +
                        sizeof(req->sub_rendezvous)) {
        p->sub_rendezvous = req->sub_rendezvous;
        p->sub_rendezvous_deadline_us = req->sub_rendezvous_deadline_us;
    }
    return MOQR_OK;
}


moqr_result_t
moqr_core_auth_begin_resolve(moqr_core_t *c, uint64_t ext_ticket,
                             const moqr_auth_verdict_t *result, uint64_t now_us,
                             moqr_auth_verdict_t *verdict, moqr_park_req_t *view)
{
    return moqr_core_auth_begin_resolve_sized(
        c, ext_ticket, result, now_us, verdict, view,
        MOQR_PARK_REQ_SIZE_THROUGH_GROUP_ORDER);
}


moqr_result_t
moqr_core_auth_begin_resolve_sized(moqr_core_t *c, uint64_t ext_ticket,
                                   const moqr_auth_verdict_t *result,
                                   uint64_t now_us,
                                   moqr_auth_verdict_t *verdict,
                                   moqr_park_req_t *view, size_t view_size)
{
    (void)now_us;
    /* Fail-closed default: any early/error return leaves DENY, so a caller that
     * ignores the return code can never resume an unresolved request. */
    if (verdict != NULL) {
        memset(verdict, 0, sizeof(*verdict));
        verdict->decision = MOQR_AUTH_DENY;
        verdict->reason = MOQR_AUTH_REASON_POLICY;
    }
    if (c == NULL || view == NULL ||
        view_size < MOQR_PARK_REQ_SIZE_THROUGH_GROUP_ORDER) {
        return MOQR_ERR_INVAL;
    }
    int slot = parked_find(c, ext_ticket);
    if (slot < 0 || c->parked[(uint32_t)slot].pinned) {
        return MOQR_ERR_STALE_HANDLE; /* unknown / retired / already resolving */
    }
    r_parked *p = &c->parked[(uint32_t)slot];
    /* A NULL result, or a DEFER/garbage decision, fails closed to DENY (no
     * recursive parking in v1). Otherwise the verifier's decision/reason/
     * error_code/lease flow through canonicalized. */
    moqr_auth_decision_t in_d = result != NULL ? result->decision
                                               : MOQR_AUTH_DENY;
    moqr_auth_reason_t   reason =
        result != NULL ? result->reason : MOQR_AUTH_REASON_POLICY;
    uint64_t error_code = result != NULL ? result->error_code : 0;
    uint64_t reval = result != NULL ? result->revalidate_after_us : 0;
    moqr_auth_decision_t d =
        in_d == MOQR_AUTH_ALLOW ? MOQR_AUTH_ALLOW : MOQR_AUTH_DENY;
    if (d == MOQR_AUTH_DENY && reason >= MOQR_AUTH_REASON__COUNT) {
        reason = MOQR_AUTH_REASON_POLICY;
    }
    uint32_t act = p->action < MOQR_AUTH_ACTION__COUNT
                       ? p->action
                       : MOQR_AUTH_ACTION__COUNT - 1u;
    c->auth_decisions[act][d]++;
    if (d == MOQR_AUTH_DENY) {
        c->auth_denials[reason]++;
    }
    r_trace(c, MOQR_TRACE_AUTH_DECISION, d, p->action, p->binding_cookie,
            reason, 0);
    if (verdict != NULL) {
        verdict->decision = d; /* CANONICAL: what the binding must branch on */
        verdict->reason = d == MOQR_AUTH_DENY ? reason : MOQR_AUTH_REASON_OK;
        verdict->error_code = d == MOQR_AUTH_DENY ? error_code : 0;
        verdict->revalidate_after_us = d == MOQR_AUTH_ALLOW ? reval : 0;
    }
    p->pinned = true;
    /* Built whole, then copied up to the caller's size: nothing is written
     * past a view that ends before a later field. */
    moqr_park_req_t v;
    memset(&v, 0, sizeof(v));
    v.action = p->action;
    v.binding_cookie = p->binding_cookie;
    v.session_cookie = p->session_cookie;
    v.ns.parts = p->mat.ns_parts;
    v.ns.count = p->mat.ns_count;
    v.name.data = p->mat.name;
    v.name.len = p->mat.name_len;
    v.tokens = p->mat.tokens;
    v.token_count = p->mat.token_count;
    v.sub_filter_type = p->sub_filter_type;
    v.sub_start_group = p->sub_start_group;
    v.sub_start_object = p->sub_start_object;
    v.sub_end_group_delta = p->sub_end_group_delta;
    v.sub_priority = p->sub_priority;
    v.sub_group_order = p->sub_group_order;
    v.sub_forward_off = p->sub_forward_off;
    v.sub_rendezvous = p->sub_rendezvous;
    v.sub_rendezvous_deadline_us = p->sub_rendezvous_deadline_us;
    memcpy(view, &v, view_size < sizeof(v) ? view_size : sizeof(v));
    return MOQR_OK;
}


void
moqr_core_auth_finish_resolve(moqr_core_t *c, uint64_t ext_ticket)
{
    if (c == NULL) {
        return;
    }
    int slot = parked_find(c, ext_ticket);
    if (slot < 0 || !c->parked[(uint32_t)slot].pinned) {
        return; /* not pinned: nothing to finish */
    }
    parked_free_slot(c, (uint32_t)slot);
}


void
moqr_core_retire_parked(moqr_core_t *c, uint64_t binding_cookie)
{
    if (c == NULL) {
        return;
    }
    for (uint32_t i = 0; i < c->max_parked; i++) {
        if ((c->parked[i].gen & 1u) == 0 ||
            c->parked[i].binding_cookie != binding_cookie) {
            continue;
        }
        if (c->parked[i].pinned) {
            /* A resolve is mid-flight holding this view; the begin/finish
             * contract keeps it stable until finish_resolve, which frees a
             * retired entry. */
            c->parked[i].retired = true;
        } else {
            parked_free_slot(c, i);
        }
    }
}


/* -- revalidation grants ------------------------------------------------------------------------- */

void
grant_free_slot(moqr_core_t *c, uint32_t slot)
{
    r_grant *g = &c->grants[slot];
    r_free_material(c, &g->mat, &c->grant_bytes_used);
    uint32_t gen = g->gen;
    memset(g, 0, sizeof(*g));
    g->gen = r_gen_close(gen);
}


moqr_result_t
moqr_core_grant_reserve(moqr_core_t *c, const moqr_park_req_t *req,
                        uint64_t lease_us, uint64_t now_us,
                        moqr_grant_res_t *res_out)
{
    if (res_out != NULL) {
        res_out->slot = 0;
        res_out->gen = 0;
    }
    if (c == NULL || req == NULL || res_out == NULL) {
        return MOQR_ERR_INVAL;
    }
    if (lease_us == 0) {
        return MOQR_ERR_INVAL; /* no lease: the caller must not create a grant */
    }
    /* Only long-lived ops are grantable (subscribe + announce). */
    if (req->action != MOQR_AUTH_SUBSCRIBE &&
        req->action != MOQR_AUTH_PUBLISH_NAMESPACE) {
        return MOQR_ERR_UNSUPPORTED;
    }
    int slot = -1;
    for (uint32_t i = 0; i < c->max_grants; i++) {
        if (r_gen_openable(c->grants[i].gen)) {
            slot = (int)i;
            break;
        }
    }
    if (slot < 0) {
        return MOQR_ERR_CAPACITY; /* max_grants exhausted */
    }
    r_material m;
    moqr_result_t rc = r_copy_material(c, req, &c->grant_bytes_used,
                                       c->grant_bytes_cap, &m);
    if (rc != MOQR_OK) {
        return rc; /* INVAL / CAPACITY / NOMEM: fail closed, no state change */
    }
    r_grant *g = &c->grants[slot];
    uint32_t gen = g->gen;
    memset(g, 0, sizeof(*g));
    g->gen = r_gen_open(gen);      /* live, but committed=false (reserved) */
    g->binding_cookie = req->binding_cookie;
    g->session_cookie = req->session_cookie;
    g->action = req->action;
    g->lease_us = lease_us;
    /* Schedule the first recheck; guard now_us + lease against wrap. */
    g->next_recheck_us =
        now_us > UINT64_MAX - lease_us ? UINT64_MAX : now_us + lease_us;
    g->mat = m;
    res_out->slot = (uint32_t)slot;
    res_out->gen = g->gen;
    return MOQR_OK;
}


moqr_result_t
moqr_core_grant_commit(moqr_core_t *c, moqr_grant_res_t res, uint64_t sub_raw)
{
    if (c == NULL || res.slot >= c->max_grants) {
        return MOQR_ERR_STALE_HANDLE;
    }
    r_grant *g = &c->grants[res.slot];
    if ((g->gen & 1u) == 0 || g->gen != res.gen || g->committed) {
        return MOQR_ERR_STALE_HANDLE; /* not a live, uncommitted reservation */
    }
    if (g->action == MOQR_AUTH_SUBSCRIBE && sub_raw == 0) {
        return MOQR_ERR_INVAL; /* a subscribe grant needs a real sub to revoke */
    }
    g->sub_raw = sub_raw;
    g->committed = true; /* now eligible for revalidation on tick */
    return MOQR_OK;      /* no-fail except stale/misuse */
}


void
moqr_core_grant_abort(moqr_core_t *c, moqr_grant_res_t res)
{
    if (c == NULL || res.slot >= c->max_grants) {
        return;
    }
    r_grant *g = &c->grants[res.slot];
    if ((g->gen & 1u) != 0 && g->gen == res.gen && !g->committed) {
        grant_free_slot(c, res.slot);
    }
}


void
moqr_core_grant_release(moqr_core_t *c, uint64_t binding_cookie,
                        uint64_t session_cookie)
{
    if (c == NULL) {
        return;
    }
    for (uint32_t gi = 0; gi < c->max_grants; gi++) {
        r_grant *g = &c->grants[gi];
        if ((g->gen & 1u) != 0 && g->committed && !g->revoked &&
            g->binding_cookie == binding_cookie &&
            g->session_cookie == session_cookie) {
            grant_free_slot(c, gi);
        }
    }
}


void
moqr_core_retire_grants(moqr_core_t *c, uint64_t binding_cookie)
{
    if (c == NULL) {
        return;
    }
    for (uint32_t i = 0; i < c->max_grants; i++) {
        if ((c->grants[i].gen & 1u) != 0 &&
            c->grants[i].binding_cookie == binding_cookie) {
            grant_free_slot(c, i);
        }
    }
}


size_t
moqr_core_peek_revoked_grants(moqr_core_t *c, moqr_revoked_grant_t *out,
                              size_t max)
{
    if (c == NULL || out == NULL) {
        return 0;
    }
    /* Announce revocations whose NS_GONE fan-out is done (unannounced) but whose
     * publisher-side cancel_namespace the binding must still send. Subscribe
     * revocations are fully core-driven (revoke_sub) and never appear here.
     * NON-draining: the entry stays until moqr_core_ack_revoked_grant frees it,
     * so a cancel that WOULD_BLOCK is retried next pump. */
    size_t n = 0;
    for (uint32_t i = 0; i < c->max_grants && n < max; i++) {
        r_grant *g = &c->grants[i];
        if ((g->gen & 1u) != 0 && g->revoked && g->unannounced) {
            out[n].binding_cookie = g->binding_cookie;
            out[n].session_cookie = g->session_cookie;
            out[n].error_code = g->revoke_request_error; /* defaulted in grant_tick */
            n++;
        }
    }
    /* Force-withdraw publisher cancels, merged after the revoked grants. Same
     * non-draining contract: an entry stays until moqr_core_ack_revoked_grant. */
    for (uint32_t i = 0; i < c->max_cancels && n < max; i++) {
        r_pending_cancel *pc = &c->pending_cancels[i];
        if (pc->used) {
            out[n].binding_cookie = pc->binding_cookie;
            out[n].session_cookie = pc->session_cookie;
            out[n].error_code = pc->error_code;
            n++;
        }
    }
    return n;
}


void
moqr_core_ack_revoked_grant(moqr_core_t *c, uint64_t binding_cookie,
                            uint64_t session_cookie)
{
    if (c == NULL) {
        return;
    }
    for (uint32_t i = 0; i < c->max_grants; i++) {
        r_grant *g = &c->grants[i];
        if ((g->gen & 1u) != 0 && g->revoked && g->unannounced &&
            g->binding_cookie == binding_cookie &&
            g->session_cookie == session_cookie) {
            grant_free_slot(c, i);
            break;   /* at most one grant per {binding, session} */
        }
    }
    /* Retire every matching pending cancel too, across both sources. */
    for (uint32_t i = 0; i < c->max_cancels; i++) {
        r_pending_cancel *pc = &c->pending_cancels[i];
        if (pc->used && pc->binding_cookie == binding_cookie &&
            pc->session_cookie == session_cookie) {
            pc->used = false;
        }
    }
}
