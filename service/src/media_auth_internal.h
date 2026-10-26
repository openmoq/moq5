#ifndef MOQ_MEDIA_AUTH_INTERNAL_H
#define MOQ_MEDIA_AUTH_INTERNAL_H

#include <moq/media_auth.h>
#include <moq/session.h>

#define MOQ_MEDIA_AUTH_MAX_TOKENS 16u
#define MOQ_MEDIA_AUTH_MAX_TOKEN_BYTES 16384u
#define MOQ_MEDIA_AUTH_MAX_TOTAL_BYTES 32768u

typedef struct moq_media_auth_list {
    moq_auth_token_t tokens[MOQ_MEDIA_AUTH_MAX_TOKENS];
    size_t count;
    uint8_t *storage;
    size_t storage_size;
} moq_media_auth_list_t;

typedef struct moq_media_auth_source {
    bool configured;
    void *ctx;
    moq_media_auth_select_fn select;
    moq_media_auth_outcome_fn on_outcome;
    moq_media_auth_list_t list;
} moq_media_auth_source_t;

typedef struct moq_media_auth_operation {
    bool prepared;
    bool notified;
    moq_result_t status;
    moq_media_auth_outcome_t outcome;
    moq_result_t result;
    moq_media_auth_list_t list;
} moq_media_auth_operation_t;

/* All destinations start zeroed. Copy requires an empty destination and
 * leaves it untouched on failure. Clear with the same allocator used to copy;
 * alloc/free are required, realloc is not used. No callbacks at construction.
 * The service owns serialization and callback thread/lock discipline. */
moq_result_t moq_media_auth_source_copy(moq_media_auth_source_t *out,
    const moq_media_auth_cfg_t *cfg, const moq_alloc_t *alloc);
void moq_media_auth_source_clear(moq_media_auth_source_t *source,
    const moq_alloc_t *alloc);
void moq_media_auth_operation_clear(moq_media_auth_operation_t *op,
    const moq_alloc_t *alloc);

/* One op per immutable logical intent. Caller keeps its resource identity
 * alive through notification, revalidates intent after select (outside its
 * private lock), and clears on replacement/cancellation/destruction. A second
 * prepare returns the cached status/list without inspecting new arguments or
 * calling select again. MOQ_OK means ready (possibly anonymous); MOQ_DONE
 * means policy denied, never admission success; negative means terminal error.
 * Uses the actual negotiated D16/D18/D22 version, not a build-availability probe.
 * Full-message encoding/capacity and operation support remain caller gates. */
moq_result_t moq_media_auth_operation_prepare(moq_media_auth_operation_t *op,
    const moq_media_auth_source_t *source,
    const moq_media_auth_request_t *request, moq_version_t version,
    const moq_alloc_t *alloc);

/* Caller latches the terminal service/track/request state BEFORE this call.
 * Notification is allocation-free, idempotent and uses the original intent's
 * identity. It does not perform that state transition or cancel wire work. */
void moq_media_auth_operation_notify(moq_media_auth_operation_t *op,
    const moq_media_auth_source_t *source,
    const moq_media_auth_request_t *request, moq_media_auth_scope_t scope,
    moq_media_track_t *track);

#endif
