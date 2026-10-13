#ifndef MOQ_SERVICE_AUTH_SOURCE_H
#define MOQ_SERVICE_AUTH_SOURCE_H

#include <moq/auth.h>

/* Private values are zero-initialized before first use. Copy/select require an
 * empty destination. Clear releases bytes with the same allocator used to copy. */
typedef struct moq_auth_owned_list {
    moq_auth_token_t tokens[MOQ_AUTH_SOURCE_MAX_TOKENS];
    size_t count;
    uint8_t *storage;
    size_t storage_size;
} moq_auth_owned_list_t;

typedef struct moq_auth_source_owned {
    bool configured;
    moq_auth_owned_list_t list;
    moq_auth_select_fn select;
    void *ctx;
} moq_auth_source_owned_t;

moq_result_t moq_auth_source_copy(moq_auth_source_owned_t *out,
    const moq_auth_source_t *source, const moq_alloc_t *alloc);
void moq_auth_source_clear(moq_auth_source_owned_t *source,
    const moq_alloc_t *alloc);
moq_result_t moq_auth_source_select(const moq_auth_source_owned_t *source,
    const moq_auth_request_t *request, const moq_alloc_t *alloc,
    moq_auth_owned_list_t *out);
void moq_auth_list_clear(moq_auth_owned_list_t *list, const moq_alloc_t *alloc);

#endif
