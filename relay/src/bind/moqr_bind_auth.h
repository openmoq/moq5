#ifndef MOQR_BIND_AUTH_H
#define MOQR_BIND_AUTH_H

/*
 * Auth-evaluation seam for the session binding (relay-private, NOT installed).
 *
 * This header lives beside moqr_bind.c and is shared — as a `static inline`,
 * so it has internal linkage and exports no symbol from any library (each TU
 * gets its own local copy) — by the production binding and its focused unit
 * test. The one job here is
 * faithful token mirroring: session decoding can surface up to
 * MOQ_DECODED_MAX_TOKENS (16) resolved tokens on a control event, borrowed and
 * valid only for that event. Silently truncating them before the verifier sees
 * them could flip an auth decision, so the mirror never drops a token — a set
 * that cannot be represented within the cap fails closed instead.
 */

#include <moq/relay/relay.h> /* moqr_core_t, moqr_auth_*, moqr_core_authorize   */
#include <moq/session.h>    /* moq_resolved_token_t, moq_bytes_t               */

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

/* Mirror capacity == the session's decoded-token cap, shared with the core's
 * deep-copy bound (MOQR_AUTH_MAX_TOKENS). A request carrying more than this
 * cannot be mirrored without truncation, so it fails closed rather than
 * misrepresent the token set to the verifier. */
#define MOQR_BIND_AUTH_MAX_TOKENS MOQR_AUTH_MAX_TOKENS

/*
 * Evaluate one control request against the core's auth hook.
 *
 * `tokens`/`token_count` are the borrowed resolved tokens from the session
 * event. They are mirrored verbatim into a bounded local array (never
 * truncated) and handed to moqr_core_authorize, which canonicalizes the
 * verdict and records the decision/denial counters and the trace.
 *
 * Fails closed WITHOUT consulting the hook when the token set cannot be
 * faithfully represented — a count past MOQR_BIND_AUTH_MAX_TOKENS, or a
 * positive count with a NULL array: *out becomes DENY/POLICY and the function
 * returns false (hook not consulted, counters untouched). Otherwise it returns
 * true and *out carries the hook's canonicalized verdict.
 */
static inline bool
moqr_bind_auth_eval(moqr_core_t *core, moqr_auth_action_t action, moqr_ns_t ns,
                    moq_bytes_t name, const moq_resolved_token_t *tokens,
                    size_t token_count, uint64_t binding_cookie,
                    uint64_t now_us, moqr_auth_verdict_t *out)
{
    memset(out, 0, sizeof(*out));
    /* Fail closed rather than misrepresent the token set: a count past the
     * mirror, or a positive count with a NULL array, is unauthorized — never a
     * silent truncation or a NULL deref, and the hook is not consulted. */
    if (token_count > MOQR_BIND_AUTH_MAX_TOKENS ||
        (token_count > 0 && tokens == NULL)) {
        out->decision = MOQR_AUTH_DENY;
        out->reason = MOQR_AUTH_REASON_POLICY;
        return false;
    }
    moqr_auth_token_t mt[MOQR_BIND_AUTH_MAX_TOKENS];
    for (size_t i = 0; i < token_count; i++) {
        mt[i].token_type = tokens[i].token_type;
        mt[i].token_value = tokens[i].token_value;
    }
    moqr_auth_request_t req;
    memset(&req, 0, sizeof(req));
    req.struct_size = (uint32_t)sizeof(req);
    req.action = action;
    req.ns = ns;
    req.name = name;
    req.tokens = token_count > 0 ? mt : NULL;
    req.token_count = token_count;
    req.binding_cookie = binding_cookie;
    req.now_us = now_us;
    moqr_core_authorize(core, &req, out);
    return true;
}

#endif /* MOQR_BIND_AUTH_H */
