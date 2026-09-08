#ifndef MOQRELAY_AUTH_H
#define MOQRELAY_AUTH_H

/*
 * Relay authorization seam (control-plane only).
 *
 * A pluggable, SYNCHRONOUS hook the core invokes before it acts on a control
 * request — setup, announce (PUBLISH_NAMESPACE), namespace subscribe
 * (SUBSCRIBE_NAMESPACE), subscribe, publish, track-status. The data plane
 * never re-checks: a grant is decided once and cached on the binding/cursor.
 *
 * The verdict/result shapes here are fixed to what a real CAT-4-MoQ verifier
 * needs (draft-ietf-moq-c4m-01) so allow-all today and a CAT plug-in later
 * share one seam, without pulling CWT/COSE into the relay:
 *   - tokens are OPAQUE bytes + a type; the verifier owns all parsing.
 *   - ALLOW is a revocable LEASE: `revalidate_after_us` (0 = never) drives a
 *     control-plane re-check; a later DENY tears the grant down.
 *   - DEFER is forward-compat for a verifier that must resolve keys async; the
 *     bundled verifiers (allow-all, static-toy) never emit it.
 *   - now_us is an INPUT; a verifier must not read a wall clock.
 *
 * This is a relay-private header (no libmoq public API), per the boundary
 * rule. Tokens are mirrored as moqr_auth_token_t so the sans-I/O core need
 * not include the session header; the binding translates from
 * moq_resolved_token_t.
 */

#include "types.h"
#include "wire_codes.h"

#ifdef __cplusplus
extern "C" {
#endif

/* -- Action -----------------------------------------------------------
 * Values are the CAT `moqt` action enum verbatim (draft-ietf-moq-c4m-01
 * §2.1 Table 1) so a real verifier's scope matching lines up 1:1. */
typedef uint32_t moqr_auth_action_t;

#define MOQR_AUTH_CLIENT_SETUP        0u
#define MOQR_AUTH_SERVER_SETUP        1u
#define MOQR_AUTH_PUBLISH_NAMESPACE   2u   /* wire ANNOUNCE            */
#define MOQR_AUTH_SUBSCRIBE_NAMESPACE 3u   /* wire SUBSCRIBE_ANNOUNCES */
#define MOQR_AUTH_SUBSCRIBE           4u
#define MOQR_AUTH_REQUEST_UPDATE      5u
#define MOQR_AUTH_PUBLISH             6u
#define MOQR_AUTH_FETCH               7u
#define MOQR_AUTH_TRACK_STATUS        8u
#define MOQR_AUTH_ACTION__COUNT       9u

/* Lower-snake action name (metric/trace label); out-of-range → "unknown". */
MOQR_CORE_API const char *moqr_auth_action_name(moqr_auth_action_t action);

/* -- Token (relay mirror of moq_resolved_token_t) ---------------------
 * BORROWED from session scratch for the duration of the hook call; anything
 * retained past it (DEFER, a revalidation lease) must be deep-copied into
 * bounded relay storage before the next session advance. */
typedef struct moqr_auth_token {
    uint64_t    token_type;    /* CAT is wire type 0x01; opaque here */
    moq_bytes_t token_value;   /* opaque CWT/COSE bytes; BORROWED    */
} moqr_auth_token_t;

/* Max resolved auth tokens on one request: the session decodes at most this
 * many (MOQ_DECODED_MAX_TOKENS). The binding's token mirror and the core's
 * deep-copy (park / grant material) both reject a count above it, so a
 * malformed or hostile count can never drive an out-of-bounds token walk. */
#define MOQR_AUTH_MAX_TOKENS 16u

/* -- Request ----------------------------------------------------------
 * Fixed v1 layout. struct_size describes the initialized request; new verifier
 * context needs a separately versioned API, not growth of embedded copies. */
typedef struct moqr_auth_request {
    uint32_t                 struct_size;
    moqr_auth_action_t       action;
    moqr_ns_t                ns;            /* borrowed; empty for setup     */
    moq_bytes_t              name;          /* borrowed; empty when n/a      */
    const moqr_auth_token_t *tokens;        /* borrowed; NULL if none        */
    size_t                   token_count;
    uint64_t                 binding_cookie; /* the connection identity      */
    uint64_t                 now_us;         /* caller clock; no wall clock  */
} moqr_auth_request_t;

/* -- Verdict ----------------------------------------------------------- */
typedef uint32_t moqr_auth_decision_t;

#define MOQR_AUTH_ALLOW 0u
#define MOQR_AUTH_DENY  1u
#define MOQR_AUTH_DEFER 2u

/* Bounded denial-reason vocabulary — a metric/trace label, append-only. Shaped
 * after the CAT A.6 error taxonomy without implementing a CAT parser. */
typedef uint32_t moqr_auth_reason_t;

#define MOQR_AUTH_REASON_OK            0u   /* allowed / not a denial       */
#define MOQR_AUTH_REASON_NO_TOKEN      1u
#define MOQR_AUTH_REASON_UNSCOPED      2u   /* action×ns/track out of scope */
#define MOQR_AUTH_REASON_EXPIRED       3u
#define MOQR_AUTH_REASON_NOT_YET_VALID 4u
#define MOQR_AUTH_REASON_BAD_ISSUER    5u
#define MOQR_AUTH_REASON_BAD_AUDIENCE  6u
#define MOQR_AUTH_REASON_BAD_SIGNATURE 7u
#define MOQR_AUTH_REASON_POLICY        8u   /* generic policy denial        */
#define MOQR_AUTH_REASON__COUNT        9u

MOQR_CORE_API const char *moqr_auth_reason_name(moqr_auth_reason_t reason);

typedef struct moqr_auth_verdict {
    moqr_auth_decision_t decision;
    /* ALLOW: 0 = permanent; else re-validate this grant after N microseconds
     * (CAT `moqt-reval`). A re-check that DENYs retires the grant. */
    uint64_t             revalidate_after_us;
    /* DENY: the wire error code (MOQ_REQUEST_ERROR_*) and a bounded reason. */
    uint64_t             error_code;
    moqr_auth_reason_t   reason;
    /* DEFER: opaque, relay-lifetime-unique ticket the verifier chooses; the
     * request is parked under it and later completed via moqr_bind_auth_resolve
     * (a request-path DEFER). Setup DEFER is not parked — it hard-closes. */
    uint64_t             ticket;
    /* Revoking an ALREADY-ACTIVE subscribe grant ends a subscription, which is
     * a PUBLISH_DONE — a different registry from `error_code` above. It is a
     * tagged descriptor so the two can never be confused by number: the same
     * integer means different things in the two domains, and one draft's
     * status is not another's. NONE (the default) falls back to the truthful
     * local UNAUTHORIZED. Ignored by initial and deferred request denial, and
     * by namespace-grant revocation, which are REQUEST_ERROR-domain only. */
    moqr_pd_desc_t       revoke_terminal;
} moqr_auth_verdict_t;

/*
 * The hook. Synchronous and pure: fills *out from *req; returns ALLOW / DENY /
 * DEFER. Must not read a wall clock (use req->now_us) nor block. NULL hook on
 * the core config means allow-all.
 */
typedef void (*moqr_authorize_fn)(void *ctx, const moqr_auth_request_t *req,
                                  moqr_auth_verdict_t *out);

#ifdef __cplusplus
}
#endif

#endif /* MOQRELAY_AUTH_H */
