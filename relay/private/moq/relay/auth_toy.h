#ifndef MOQRELAY_AUTH_TOY_H
#define MOQRELAY_AUTH_TOY_H

/*
 * Static-toy authorizer (bundled, deterministic, NO crypto).
 *
 * A prefix/table policy for tests and simple deployments. Each rule pairs an
 * action with a namespace prefix; the first rule matching BOTH wins and its
 * decision is returned, else the default decision applies. This is NOT a CAT
 * verifier — tokens are ignored entirely (no parsing, no crypto). It plugs
 * into the same moqr_authorize_fn seam a real verifier would, so swapping one
 * in later is a config change, not a code change.
 *
 * The rules array and each rule's prefix bytes are BORROWED: the caller owns
 * them and must keep them alive while the authorizer is installed. Reentrant
 * and clock-free (now_us is ignored), so one config may back many cores, and
 * it never emits DEFER.
 */

#include <moq/relay/auth.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct moqr_auth_toy_rule {
    moqr_auth_action_t   action;     /* action this rule matches            */
    moqr_ns_t            ns_prefix;  /* namespace prefix (count == 0 = any) */
    moqr_auth_decision_t decision;   /* ALLOW on match, else treated as DENY */
    moqr_auth_reason_t   reason;     /* reported when decision == DENY      */
} moqr_auth_toy_rule_t;

typedef struct moqr_auth_toy {
    const moqr_auth_toy_rule_t *rules;      /* borrowed; may be NULL if 0   */
    size_t                      rule_count;
    moqr_auth_decision_t        default_decision; /* when no rule matches   */
    moqr_auth_reason_t          default_reason;    /* for a default DENY     */
} moqr_auth_toy_t;

/* A moqr_authorize_fn over a moqr_auth_toy_t* ctx. Emits ALLOW or DENY only:
 * a rule or default carrying any other decision (e.g. a mis-authored DEFER) is
 * clamped to DENY, so a bad policy fails closed rather than deferring. */
void moqr_auth_toy_authorize(void *ctx, const moqr_auth_request_t *req,
                             moqr_auth_verdict_t *out);

#ifdef __cplusplus
}
#endif

#endif /* MOQRELAY_AUTH_TOY_H */
