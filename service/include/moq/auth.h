#ifndef MOQ_SERVICE_AUTH_H
#define MOQ_SERVICE_AUTH_H

#include <moq/types.h>

#ifdef __cplusplus
extern "C" {
#endif

#define MOQ_AUTH_SOURCE_MAX_TOKENS 16
#define MOQ_AUTH_SOURCE_MAX_TOKEN_BYTES 16384
#define MOQ_AUTH_SOURCE_MAX_TOTAL_BYTES 32768

typedef enum moq_auth_action {
    MOQ_AUTH_CLIENT_SETUP = 0, MOQ_AUTH_SERVER_SETUP = 1,
    MOQ_AUTH_PUBLISH_NAMESPACE = 2, MOQ_AUTH_SUBSCRIBE_NAMESPACE = 3,
    MOQ_AUTH_SUBSCRIBE = 4, MOQ_AUTH_REQUEST_UPDATE = 5,
    MOQ_AUTH_PUBLISH = 6, MOQ_AUTH_FETCH = 7, MOQ_AUTH_TRACK_STATUS = 8
} moq_auth_action_t;

typedef struct moq_auth_request {
    moq_auth_action_t action;
    moq_namespace_t ns;
    moq_bytes_t name;
} moq_auth_request_t;

/* Nonblocking selection of credentials for one actual wire resource. Write
 * descriptors into out_tokens, setting out_count <= capacity. Bytes must remain
 * valid until the next call for this owner or its destruction; the library
 * copies them immediately. Do not return pointers to stack-local bytes.
 * Return MOQ_OK with at least one token, or a negative error to deny. Never
 * block, perform issuer I/O, reenter the owner, or throw through this C API.
 * SETUP selection runs in connect(); other selections run on the owner pump.
 * Shared contexts across owners require caller-provided synchronization. */
typedef moq_result_t (*moq_auth_select_fn)(
    void *ctx, const moq_auth_request_t *request,
    moq_auth_token_t *out_tokens, size_t capacity, size_t *out_count);

typedef struct moq_auth_source {
    uint32_t struct_size;
    const moq_auth_token_t *tokens;
    size_t token_count;
    moq_auth_select_fn select;
    void *ctx;
} moq_auth_source_t;

/* A source is either a nonempty static token list or a selector, never both.
 * NULL source configuration means anonymous operation. A configured source
 * cannot silently fall back to anonymous on empty output, error, or OOM.
 * Static data is copied at connect/create/attach. Selector code/context must
 * outlive the owner. Selected bytes survive retries of the same request.
 * Tokens contain raw serialized bytes, not base64 or MOQT Token envelopes. */
MOQ_API void moq_auth_source_init_sized(moq_auth_source_t *source, size_t size);

#ifdef __cplusplus
}
#endif
#endif
