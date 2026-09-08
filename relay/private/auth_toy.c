#include <moq/relay/auth_toy.h>

#include <stdbool.h>
#include <string.h>

/* A namespace matches a prefix when the prefix's parts equal the leading
 * parts of the namespace. A zero-length prefix matches everything. */
static bool
ns_prefix_match(moqr_ns_t prefix, moqr_ns_t ns)
{
    if (prefix.count > ns.count) {
        return false;
    }
    for (size_t i = 0; i < prefix.count; i++) {
        if (prefix.parts[i].len != ns.parts[i].len) {
            return false;
        }
        if (prefix.parts[i].len != 0 &&
            memcmp(prefix.parts[i].data, ns.parts[i].data,
                   prefix.parts[i].len) != 0) {
            return false;
        }
    }
    return true;
}

/* The toy contract is ALLOW or DENY only. ALLOW passes through; ANYTHING else
 * — an explicit DENY, or a mis-authored DEFER/garbage decision — becomes a
 * DENY, so a bad rule fails closed rather than deferring. An explicit DENY
 * keeps its reason; a clamped non-ALLOW/non-DENY gets a generic policy reason. */
static void
toy_verdict(moqr_auth_decision_t decision, moqr_auth_reason_t reason,
            moqr_auth_verdict_t *out)
{
    if (decision == MOQR_AUTH_ALLOW) {
        out->decision = MOQR_AUTH_ALLOW;
        out->reason = MOQR_AUTH_REASON_OK;
    } else {
        out->decision = MOQR_AUTH_DENY;
        out->reason = (decision == MOQR_AUTH_DENY) ? reason
                                                   : MOQR_AUTH_REASON_POLICY;
    }
}

void
moqr_auth_toy_authorize(void *ctx, const moqr_auth_request_t *req,
                        moqr_auth_verdict_t *out)
{
    const moqr_auth_toy_t *toy = (const moqr_auth_toy_t *)ctx;
    if (toy == NULL || req == NULL || out == NULL) {
        return; /* leave the caller-provided default verdict untouched */
    }
    for (size_t i = 0; i < toy->rule_count; i++) {
        const moqr_auth_toy_rule_t *r = &toy->rules[i];
        if (r->action != req->action) {
            continue;
        }
        if (!ns_prefix_match(r->ns_prefix, req->ns)) {
            continue;
        }
        toy_verdict(r->decision, r->reason, out);
        return;
    }
    toy_verdict(toy->default_decision, toy->default_reason, out);
}
