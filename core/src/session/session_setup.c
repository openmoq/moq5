#include "session_internal.h"

moq_result_t handle_start(moq_session_t *s)
{
    return s->profile->start(s);
}

moq_result_t session_setup_workspace_acquire(moq_session_t *s,
                                             moq_setup_workspace_t *workspace)
{
    memset(workspace, 0, sizeof(*workspace));
    /* Both pools are minimum-one co-allocations. Public requests require an
     * active session; early inbound requests reside in the subscription pool. */
    if (s->ns_sub_cap == 0 || s->track_sub_cap == 0 ||
        s->ns_subs[0].state != MOQ_NS_SUB_FREE ||
        s->track_subs[0].state != MOQ_TRACK_SUB_FREE)
        return close_with_error(s, 0x1, "SETUP workspace unavailable");
    workspace->auth = &s->ns_subs[0];
    workspace->decoded = &s->track_subs[0];
    memset(workspace->auth->resolved_tokens, 0,
           sizeof(workspace->auth->resolved_tokens));
    memset(workspace->auth->token_staged, 0,
           sizeof(workspace->auth->token_staged));
    workspace->auth->token_count = 0;
    memset(&workspace->auth->auth_txn, 0, sizeof(workspace->auth->auth_txn));
    memset(&workspace->decoded->auth_txn, 0,
           sizeof(workspace->decoded->auth_txn));
    return MOQ_OK;
}

void session_setup_workspace_release(moq_session_t *s,
                                      moq_setup_workspace_t *workspace)
{
    if (!workspace->auth) return;
    moq_ns_sub_entry_t *auth = workspace->auth;
    process_auth_tokens_free_staging(s, auth->resolved_tokens,
                                     auth->token_staged, auth->token_count);
    process_auth_tokens_abort_txn(s, &auth->auth_txn);
    /* Clear only the borrowed members, never pool state, generation, receive
     * buffers or registry/FIN ownership. No normal pool-free path is involved. */
    memset(auth->resolved_tokens, 0, sizeof(auth->resolved_tokens));
    memset(auth->token_staged, 0, sizeof(auth->token_staged));
    auth->token_count = 0;
    memset(&auth->auth_txn, 0, sizeof(auth->auth_txn));
    memset(&workspace->decoded->auth_txn, 0,
           sizeof(workspace->decoded->auth_txn));
    memset(workspace, 0, sizeof(*workspace));
}

/* Complete the peer's SETUP on a profile that carries authorization tokens as
 * Setup Options. A REGISTER that would not fit the advertised cache is treated
 * as USE_VALUE rather than failing the session (§10.3.1.4); the remaining
 * tokens resolve through the shared cache transaction; the completion event
 * carries the resolved values out of event scratch; the transaction commits
 * last. Retryable until the commit: a refusal leaves no state behind. When
 * the local SETUP is already out, the session is established here and request
 * streams that arrived early are dispatched. */
moq_result_t session_setup_complete_peer(moq_session_t *s,
                                         moq_setup_workspace_t *workspace,
                                         const moq_setup_params_t *peer,
                                         bool *setup_received,
                                         bool setup_sent)
{
    moq_ns_sub_entry_t *auth = workspace->auth;
    moq_decoded_auth_token_t *tokens = workspace->decoded->setup_tokens;
    size_t count = workspace->count;
    {
        size_t proj_bytes = s->peer_token_cache.used_bytes;
        size_t proj_active = 0;
        for (size_t i = 0; i < s->peer_token_cache.cap; i++)
            if (s->peer_token_cache.entries[i].active) proj_active++;
        for (size_t i = 0; i < count; i++) {
            if (tokens[i].op != MOQ_AUTH_OP_REGISTER)
                continue;
            size_t esize = MOQ_TOKEN_ENTRY_OVERHEAD + tokens[i].token_value_len;
            bool fits = esize >= MOQ_TOKEN_ENTRY_OVERHEAD &&
                        proj_bytes <= s->peer_token_cache.max_bytes &&
                        esize <= s->peer_token_cache.max_bytes - proj_bytes &&
                        proj_active < s->peer_token_cache.cap;
            if (fits) {
                proj_bytes += esize;
                proj_active++;
            } else {
                tokens[i].op = MOQ_AUTH_OP_USE_VALUE;
            }
        }
    }
    moq_resolved_token_t *resolved = auth->resolved_tokens;
    bool *staged = auth->token_staged;
    uint64_t reject_code = 0;
    moq_result_t arc = process_auth_tokens(s, tokens, count,
        &(moq_auth_resolve_out_t){
            .tokens = resolved, .token_count = &auth->token_count,
            .max_tokens = MOQ_DECODED_MAX_TOKENS, .reject_code = &reject_code,
            .staged = staged, .txn = &auth->auth_txn });
    if (arc < 0) {
        session_setup_workspace_release(s, workspace);
        return arc;
    }
    /* A close inside the resolution (duplicate alias) returns MOQ_OK with the
     * session CLOSED; the close must stick. */
    if (s->state == MOQ_SESS_CLOSED) {
        session_setup_workspace_release(s, workspace);
        return MOQ_OK;
    }
    size_t token_count = auth->token_count;
    if (reject_code != 0) {
        /* There is no request to reject at SETUP time: an unusable token is a
         * session error. A request-level MALFORMED_AUTH_TOKEN (0x4) maps to the
         * MALFORMED_AUTH_TOKEN session error (0x16); anything else keeps the
         * generic formatting close. */
        session_setup_workspace_release(s, workspace);
        if (reject_code == 0x4)
            return close_with_error(s, 0x16, "malformed auth token in SETUP");
        return close_with_error(s, 0x6, "invalid auth token in SETUP");
    }
    /* Classify the completion event's whole token requirement before copying
     * any of it: a shortfall an empty arena could satisfy is transient, one no
     * empty arena could satisfy can never be retried into success. */
    size_t scratch_saved = s->event_scratch_len;
    {
        moq_event_scratch_verdict_t v =
            setup_event_scratch_classify(s, resolved, token_count);
        if (v != MOQ_EVENT_SCRATCH_FITS) {
            session_setup_workspace_release(s, workspace);
            if (v == MOQ_EVENT_SCRATCH_PERMANENT)
                return close_with_error(s, 0x1,
                    "event scratch permanently too small");
            return MOQ_ERR_WOULD_BLOCK;
        }
    }
    /* The peer's implementation identifier outlives the input buffer, so it
     * is copied before anything is committed; a failed copy is a refusal
     * like a full arena, leaving no state behind. */
    uint8_t *impl_copy = NULL;
    if (peer->has_implementation && peer->implementation.len > 0) {
        impl_copy = s->alloc.alloc(peer->implementation.len, s->alloc.ctx);
        if (!impl_copy) {
            session_setup_workspace_release(s, workspace);
            return MOQ_ERR_NOMEM;
        }
        memcpy(impl_copy, peer->implementation.data, peer->implementation.len);
    }
    moq_resolved_token_t *ev_tokens = NULL;
    for (size_t i = 0; i < token_count; i++) {
        if (resolved[i].token_value.len > 0) {
            const uint8_t *src = resolved[i].token_value.data;
            size_t slen = resolved[i].token_value.len;
            uint8_t *copy = event_scratch_copy(s, src, slen);
            if (staged[i])
                s->alloc.free((void *)(uintptr_t)src, slen, s->alloc.ctx);
            staged[i] = false;
            if (!copy) {
                s->event_scratch_len = scratch_saved;
                session_setup_workspace_release(s, workspace);
                if (impl_copy)
                    s->alloc.free(impl_copy, peer->implementation.len, s->alloc.ctx);
                return close_with_error(s, 0x1,
                    "internal: setup scratch preflight mismatch");
            }
            resolved[i].token_value.data = copy;
        } else {
            resolved[i].token_value.data = NULL;
        }
    }
    if (token_count > 0) {
        ev_tokens = (moq_resolved_token_t *)event_scratch_alloc_aligned(s,
            token_count * sizeof(moq_resolved_token_t),
            _Alignof(moq_resolved_token_t));
        if (!ev_tokens) {
            s->event_scratch_len = scratch_saved;
            session_setup_workspace_release(s, workspace);
            if (impl_copy)
                s->alloc.free(impl_copy, peer->implementation.len, s->alloc.ctx);
            return close_with_error(s, 0x1,
                "internal: setup scratch preflight mismatch");
        }
        memcpy(ev_tokens, resolved, token_count * sizeof(moq_resolved_token_t));
    }
    /* Commit: record peer options, complete the handshake, surface the event
     * with the tokens, then commit the auth transaction (commit-last). */
    if (s->peer_implementation)
        s->alloc.free(s->peer_implementation, s->peer_implementation_len,
                      s->alloc.ctx);
    s->peer_implementation = impl_copy;
    s->peer_implementation_len = impl_copy ? peer->implementation.len : 0;
    s->peer_setup.has_implementation = peer->has_implementation;
    s->peer_setup.implementation.data = impl_copy;
    s->peer_setup.implementation.len = s->peer_implementation_len;
    s->peer_setup.has_path = peer->has_path;
    s->peer_setup.has_authority = peer->has_authority;
    s->peer_setup.has_max_auth_token_cache_size = peer->has_max_auth_token_cache_size;
    s->peer_setup.max_auth_token_cache_size = peer->max_auth_token_cache_size;
    *setup_received = true;
    bool established_now = false;
    if (setup_sent && s->state != MOQ_SESS_ESTABLISHED) {
        s->state = MOQ_SESS_ESTABLISHED;
        established_now = true;
        moq_event_t e;
        memset(&e, 0, sizeof(e));
        e.kind = MOQ_EVENT_SETUP_COMPLETE;
        e.detail_size = (uint32_t)sizeof(moq_setup_complete_event_t);
        e.borrow_epoch = s->borrow_epoch;
        e.u.setup_complete.local_perspective = s->perspective;
        e.u.setup_complete.peer_perspective =
            (s->perspective == MOQ_PERSPECTIVE_CLIENT)
                ? MOQ_PERSPECTIVE_SERVER : MOQ_PERSPECTIVE_CLIENT;
        e.u.setup_complete.tokens = ev_tokens;
        e.u.setup_complete.token_count = ev_tokens ? token_count : 0;
        moq_result_t prc = push_event(s, &e);   /* slot reserved by the caller */
        if (prc < 0) {
            session_setup_workspace_release(s, workspace);
            return prc;
        }
    }
    process_auth_tokens_commit_txn(s, &auth->auth_txn);
    /* Refeed may immediately allocate either slot0. Completion owns release;
     * the profile caller must not clear these members after this handoff. */
    session_setup_workspace_release(s, workspace);
    /* After the token commit (dispatched requests may reference the cache):
     * dispatch request streams that arrived before establishment. */
    if (established_now)
        return request_streams_refeed_deferred(s);
    return MOQ_OK;
}
