#ifndef MOQ_MEDIA_AUTH_H
#define MOQ_MEDIA_AUTH_H

#include <moq/types.h>

#ifdef __cplusplus
extern "C" {
#endif

#ifndef MOQ_MEDIA_TRACK_T_DEFINED
#define MOQ_MEDIA_TRACK_T_DEFINED
typedef struct moq_media_track moq_media_track_t;
#endif

/* Open action set. Unknown actions must be denied. Resource credentials are
 * independent of SETUP credentials; no alias management or token verification
 * is performed by this interface. */
typedef uint32_t moq_media_auth_action_t;
#define MOQ_MEDIA_AUTH_ACTION_NONE              0u
#define MOQ_MEDIA_AUTH_ACTION_PUBLISH_NAMESPACE 1u
#define MOQ_MEDIA_AUTH_ACTION_PUBLISH           2u
#define MOQ_MEDIA_AUTH_ACTION_SUBSCRIBE         3u
#define MOQ_MEDIA_AUTH_ACTION_FETCH             4u
#define MOQ_MEDIA_AUTH_ACTION_REQUEST_UPDATE    5u

typedef enum moq_media_auth_decision {
    MOQ_MEDIA_AUTH_DENY = 0,
    MOQ_MEDIA_AUTH_ALLOW = 1
} moq_media_auth_decision_t;

/* Library-stamped, callback-duration borrowed identity. Namespace publication
 * has an empty track_name; other actions name the actual resource, including
 * catalog and generated tracks. */
typedef struct moq_media_auth_request {
    uint32_t struct_size;
    moq_media_auth_action_t action;
    moq_namespace_t track_namespace;
    moq_bytes_t track_name;
} moq_media_auth_request_t;

#define MOQ_MEDIA_AUTH_REQUEST_V0_SIZE \
    (offsetof(moq_media_auth_request_t, track_name) + sizeof(moq_bytes_t))

/* Synchronous only. Scratch capacity is 16; count starts at zero and decision
 * at DENY. OK+DENY ignores all token outputs. OK+ALLOW requires 1..16 unique
 * (type, value) pairs. Values are opaque binary, including empty values;
 * NULL data is valid only with zero length. Limits: 16 KiB/value and 32 KiB
 * aggregate value bytes. D16 types fit 62 bits; D18/D22 types fit uint64_t.
 * Opaque values and unique pairs follow draft-ietf-moq-transport-16 section
 * 9.2.2.1, draft-ietf-moq-transport-18 section 10.2.2 and
 * draft-ietf-moq-transport-22 section 8.9. The count/byte budgets
 * and synchronous policy are service limits, not wire requirements.
 *
 * Returned bytes remain application-owned and valid until the next selection
 * for this owner or owner destruction. Never return callback-stack storage.
 * The library copies before the next selection or service pump return and
 * retains that copy across transient request-admission retries. Replacement
 * or cancellation of an unsent intent discards its selection.
 *
 * WOULD_BLOCK and nonnegative non-OK results are invalid selection, not async
 * retry. Other negative results are terminal selector errors. No configured
 * source can select anonymous access; ALLOW with zero tokens is invalid.
 *
 * Both callbacks run on the endpoint network thread, with endpoint mutex held
 * and outside service-private locks. No blocking, issuer I/O, endpoint/service/
 * facade reentry (including queries), or C++ exceptions across C. Shared
 * context locks must not be held by a thread waiting on the endpoint thread. */
typedef moq_result_t (*moq_media_auth_select_fn)(
    void *ctx, const moq_media_auth_request_t *request,
    moq_auth_token_t *out_tokens, size_t token_capacity,
    size_t *out_token_count, moq_media_auth_decision_t *out_decision);

typedef uint32_t moq_media_auth_outcome_t;
#define MOQ_MEDIA_AUTH_OUTCOME_NONE                 0u
#define MOQ_MEDIA_AUTH_OUTCOME_POLICY_DENIED        1u
#define MOQ_MEDIA_AUTH_OUTCOME_SELECTOR_ERROR       2u
#define MOQ_MEDIA_AUTH_OUTCOME_INVALID_SELECTION    3u
#define MOQ_MEDIA_AUTH_OUTCOME_TOKEN_LIMIT          4u
#define MOQ_MEDIA_AUTH_OUTCOME_TOKEN_COPY_FAILED    5u
#define MOQ_MEDIA_AUTH_OUTCOME_UNSUPPORTED_PROFILE  6u

typedef uint32_t moq_media_auth_scope_t;
#define MOQ_MEDIA_AUTH_SCOPE_NONE    0u
#define MOQ_MEDIA_AUTH_SCOPE_SERVICE 1u
#define MOQ_MEDIA_AUTH_SCOPE_TRACK   2u
#define MOQ_MEDIA_AUTH_SCOPE_REQUEST 3u

/* Borrowed callback input; contains no token bytes. Switch on outcome, not
 * result: POLICY_DENIED has result MOQ_OK, which is not successful admission.
 * Unknown outcomes/scopes are not success. Scope determines effect, not track:
 * track is a real public handle, or NULL for private/generated resources.
 * request always identifies the failed operation exactly. */
typedef struct moq_media_auth_outcome_info {
    uint32_t struct_size;
    moq_media_auth_scope_t scope;
    moq_media_auth_outcome_t outcome;
    moq_media_track_t *track;
    const moq_media_auth_request_t *request;
    moq_result_t result;
} moq_media_auth_outcome_info_t;

#define MOQ_MEDIA_AUTH_OUTCOME_INFO_V0_SIZE \
    (offsetof(moq_media_auth_outcome_info_t, result) + sizeof(moq_result_t))

/* Exactly once on terminal local failure, after terminal state is latched and
 * before ordinary service-close notification. Never invoked for success or
 * construction failure. Notification allocates no memory. No peer rejection
 * or delivery event is synthesized. Callback restrictions above also apply.
 *
 * INVALID_SELECTION -> INVAL (including duplicates, bad spans, empty ALLOW,
 * excess count, invalid decision or selector WOULD_BLOCK/non-OK status).
 * TOKEN_LIMIT -> INVAL for service byte budgets; BUFFER for lower-layer
 * complete-message capacity (operational, not a caller-buffer sizing API).
 * TOKEN_COPY_FAILED -> NOMEM; UNSUPPORTED_PROFILE -> UNSUPPORTED. An invalid
 * token type for a supported profile is INVALID_SELECTION/INVAL instead.
 * SELECTOR_ERROR preserves the selector's other negative result exactly. */
typedef void (*moq_media_auth_outcome_fn)(
    void *ctx, const moq_media_auth_outcome_info_t *info);

/* NULL source means anonymous resource requests, without callbacks. A non-NULL
 * source requires the complete V0 prefix, on_outcome, and exactly one of:
 * nonempty tokens/count with select == NULL, or NULL tokens/count == 0 with
 * select != NULL. No empty configured source or anonymous fallback.
 *
 * Owner construction copies this config and all static descriptors/bytes.
 * No cfg/static pointer survives construction; callback code and ctx remain
 * borrowed until owner destruction. Validation/copy failure returns INVAL or
 * NOMEM synchronously, attaches no owner, and invokes no outcome callback.
 * Static type-width validation is deferred to the negotiated profile. */
typedef struct moq_media_auth_cfg {
    uint32_t struct_size;
    void *ctx;
    const moq_auth_token_t *tokens;
    size_t token_count;
    moq_media_auth_select_fn select;
    moq_media_auth_outcome_fn on_outcome;
} moq_media_auth_cfg_t;

#define MOQ_MEDIA_AUTH_CFG_V0_SIZE \
    (offsetof(moq_media_auth_cfg_t, on_outcome) + sizeof(moq_media_auth_outcome_fn))

/* Zero/stamp min(cfg_size, sizeof(*cfg)); NULL or a size too small for the
 * leading struct_size causes no write. The zeroed cfg is not a valid source. */
MOQ_API void moq_media_auth_cfg_init_sized(moq_media_auth_cfg_t *cfg,
                                         size_t cfg_size);
#define moq_media_auth_cfg_init(cfg) \
    moq_media_auth_cfg_init_sized((cfg), sizeof(*(cfg)))

#ifdef __cplusplus
}
#endif
#endif
