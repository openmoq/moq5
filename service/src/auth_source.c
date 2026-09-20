#include "auth_source.h"

#include <string.h>

void moq_auth_source_init_sized(moq_auth_source_t *source, size_t size)
{
    if (!source) return;
    if (size > sizeof(*source)) size = sizeof(*source);
    memset(source, 0, size);
    if (size >= sizeof(source->struct_size))
        source->struct_size = (uint32_t)size;
}

void moq_auth_list_clear(moq_auth_owned_list_t *list, const moq_alloc_t *alloc)
{
    if (!list) return;
    if (list->storage)
        alloc->free(list->storage, list->storage_size, alloc->ctx);
    memset(list, 0, sizeof(*list));
}

void moq_auth_source_clear(moq_auth_source_owned_t *source,
                           const moq_alloc_t *alloc)
{
    if (!source) return;
    moq_auth_list_clear(&source->list, alloc);
    memset(source, 0, sizeof(*source));
}

static moq_result_t list_copy(moq_auth_owned_list_t *out,
    const moq_auth_token_t *tokens, size_t count, const moq_alloc_t *alloc)
{
    if (!tokens || count == 0 || count > MOQ_AUTH_SOURCE_MAX_TOKENS)
        return MOQ_ERR_INVAL;
    size_t total = 0;
    for (size_t i = 0; i < count; ++i) {
        size_t n = tokens[i].token_value.len;
        if (tokens[i].token_type > UINT64_C(0x3fffffffffffffff) ||
            !tokens[i].token_value.data || n == 0 ||
            n > MOQ_AUTH_SOURCE_MAX_TOKEN_BYTES ||
            n > MOQ_AUTH_SOURCE_MAX_TOTAL_BYTES - total)
            return MOQ_ERR_INVAL;
        total += n;
    }
    uint8_t *storage = alloc->alloc(total, alloc->ctx);
    if (!storage) return MOQ_ERR_NOMEM;
    size_t offset = 0;
    for (size_t i = 0; i < count; ++i) {
        size_t n = tokens[i].token_value.len;
        memcpy(storage + offset, tokens[i].token_value.data, n);
        out->tokens[i] = (moq_auth_token_t){tokens[i].token_type,
            {storage + offset, n}};
        offset += n;
    }
    out->storage = storage;
    out->storage_size = total;
    out->count = count;
    return MOQ_OK;
}

moq_result_t moq_auth_source_copy(moq_auth_source_owned_t *out,
    const moq_auth_source_t *source, const moq_alloc_t *alloc)
{
    if (!out || !alloc || !alloc->alloc || !alloc->free)
        return MOQ_ERR_INVAL;
    if (!source) return MOQ_OK;
    const size_t floor = offsetof(moq_auth_source_t, ctx) + sizeof(source->ctx);
    if (source->struct_size < floor)
        return MOQ_ERR_INVAL;
    if (source->select) {
        if (source->tokens || source->token_count)
            return MOQ_ERR_INVAL;
        out->select = source->select;
        out->ctx = source->ctx;
    } else {
        moq_result_t rc = list_copy(&out->list, source->tokens,
                                   source->token_count, alloc);
        if (rc != MOQ_OK) return rc;
    }
    out->configured = true;
    return MOQ_OK;
}

moq_result_t moq_auth_source_select(const moq_auth_source_owned_t *source,
    const moq_auth_request_t *request, const moq_alloc_t *alloc,
    moq_auth_owned_list_t *out)
{
    if (!source || !request || !alloc || !alloc->alloc || !alloc->free || !out)
        return MOQ_ERR_INVAL;
    if (!source->configured) return MOQ_OK;
    if (!source->select)
        return list_copy(out, source->list.tokens, source->list.count, alloc);
    moq_auth_token_t tokens[MOQ_AUTH_SOURCE_MAX_TOKENS] = {0};
    size_t count = 0;
    moq_result_t rc = source->select(source->ctx, request, tokens,
                                   MOQ_AUTH_SOURCE_MAX_TOKENS, &count);
    if (rc != MOQ_OK) return rc < 0 ? rc : MOQ_ERR_INVAL;
    return list_copy(out, tokens, count, alloc);
}
