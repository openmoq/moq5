#include "media_auth_internal.h"

#include <string.h>

void moq_media_auth_cfg_init_sized(moq_media_auth_cfg_t *cfg, size_t size)
{
    if (!cfg || size < sizeof(cfg->struct_size)) return;
    if (size > sizeof(*cfg)) size = sizeof(*cfg);
    memset(cfg, 0, size);
    cfg->struct_size = (uint32_t)size;
}

static void list_clear(moq_media_auth_list_t *list, const moq_alloc_t *alloc)
{
    if (list->storage)
        alloc->free(list->storage, list->storage_size, alloc->ctx);
    memset(list, 0, sizeof(*list));
}

void moq_media_auth_source_clear(moq_media_auth_source_t *source,
                               const moq_alloc_t *alloc)
{
    if (!source) return;
    list_clear(&source->list, alloc);
    memset(source, 0, sizeof(*source));
}

void moq_media_auth_operation_clear(moq_media_auth_operation_t *op,
                                  const moq_alloc_t *alloc)
{
    if (!op) return;
    list_clear(&op->list, alloc);
    memset(op, 0, sizeof(*op));
}

static moq_result_t list_copy(moq_media_auth_list_t *out,
    const moq_auth_token_t *tokens, size_t count, const moq_alloc_t *alloc,
    moq_media_auth_outcome_t *outcome)
{
    *outcome = MOQ_MEDIA_AUTH_OUTCOME_INVALID_SELECTION;
    if (!tokens || !count || count > MOQ_MEDIA_AUTH_MAX_TOKENS)
        return MOQ_ERR_INVAL;

    /* Bound every span before comparing or reading any token bytes. */
    size_t total = 0;
    for (size_t i = 0; i < count; ++i) {
        size_t n = tokens[i].token_value.len;
        if (n && !tokens[i].token_value.data) return MOQ_ERR_INVAL;
        if (n > MOQ_MEDIA_AUTH_MAX_TOKEN_BYTES ||
            n > MOQ_MEDIA_AUTH_MAX_TOTAL_BYTES - total) {
            *outcome = MOQ_MEDIA_AUTH_OUTCOME_TOKEN_LIMIT;
            return MOQ_ERR_INVAL;
        }
        total += n;
    }
    for (size_t i = 0; i < count; ++i) {
        for (size_t j = 0; j < i; ++j) {
            size_t n = tokens[i].token_value.len;
            if (tokens[i].token_type == tokens[j].token_type &&
                n == tokens[j].token_value.len &&
                (!n || memcmp(tokens[i].token_value.data,
                              tokens[j].token_value.data, n) == 0))
                return MOQ_ERR_INVAL;
        }
    }
    uint8_t *storage = total ? alloc->alloc(total, alloc->ctx) : NULL;
    if (total && !storage) {
        *outcome = MOQ_MEDIA_AUTH_OUTCOME_TOKEN_COPY_FAILED;
        return MOQ_ERR_NOMEM;
    }
    size_t offset = 0;
    for (size_t i = 0; i < count; ++i) {
        size_t n = tokens[i].token_value.len;
        if (n) memcpy(storage + offset, tokens[i].token_value.data, n);
        out->tokens[i].token_type = tokens[i].token_type;
        out->tokens[i].token_value.data = n ? storage + offset : NULL;
        out->tokens[i].token_value.len = n;
        offset += n;
    }
    out->storage = storage;
    out->storage_size = total;
    out->count = count;
    *outcome = MOQ_MEDIA_AUTH_OUTCOME_NONE;
    return MOQ_OK;
}

moq_result_t moq_media_auth_source_copy(moq_media_auth_source_t *out,
    const moq_media_auth_cfg_t *cfg, const moq_alloc_t *alloc)
{
    if (!out || !alloc || !alloc->alloc || !alloc->free)
        return MOQ_ERR_INVAL;
    if (!cfg) return MOQ_OK;
    if (cfg->struct_size < MOQ_MEDIA_AUTH_CFG_V0_SIZE || !cfg->on_outcome)
        return MOQ_ERR_INVAL;
    if (cfg->select && (cfg->tokens || cfg->token_count))
        return MOQ_ERR_INVAL;

    moq_media_auth_source_t copy = {0};
    if (!cfg->select) {
        moq_media_auth_outcome_t outcome;
        moq_result_t rc = list_copy(&copy.list, cfg->tokens, cfg->token_count,
                                   alloc, &outcome);
        if (rc != MOQ_OK) return rc;
    }
    copy.ctx = cfg->ctx;
    copy.select = cfg->select;
    copy.on_outcome = cfg->on_outcome;
    copy.configured = true;
    *out = copy;
    return MOQ_OK;
}

static moq_result_t operation_error(moq_media_auth_operation_t *op,
    moq_media_auth_outcome_t outcome, moq_result_t result)
{
    op->prepared = true;
    op->outcome = outcome;
    op->result = result;
    op->status = outcome == MOQ_MEDIA_AUTH_OUTCOME_POLICY_DENIED ? MOQ_DONE : result;
    return op->status;
}

moq_result_t moq_media_auth_operation_prepare(moq_media_auth_operation_t *op,
    const moq_media_auth_source_t *source,
    const moq_media_auth_request_t *request, moq_version_t version,
    const moq_alloc_t *alloc)
{
    if (!op) return MOQ_ERR_INVAL;
    if (op->prepared) return op->status;
    if (!source || !request || !alloc || !alloc->alloc || !alloc->free)
        return MOQ_ERR_INVAL;
    if (request->struct_size < MOQ_MEDIA_AUTH_REQUEST_V0_SIZE ||
        request->action < MOQ_MEDIA_AUTH_ACTION_PUBLISH_NAMESPACE ||
        request->action > MOQ_MEDIA_AUTH_ACTION_REQUEST_UPDATE)
        return operation_error(op, MOQ_MEDIA_AUTH_OUTCOME_INVALID_SELECTION,
                               MOQ_ERR_INVAL);
    if (!source->configured) {
        op->prepared = true;
        return MOQ_OK;
    }
    if (version != MOQ_VERSION_DRAFT_16 && version != MOQ_VERSION_DRAFT_18 &&
        version != MOQ_VERSION_DRAFT_22)
        return operation_error(op, MOQ_MEDIA_AUTH_OUTCOME_UNSUPPORTED_PROFILE,
                               MOQ_ERR_UNSUPPORTED);

    moq_auth_token_t scratch[MOQ_MEDIA_AUTH_MAX_TOKENS] = {0};
    const moq_auth_token_t *tokens = source->list.tokens;
    size_t count = source->list.count;
    if (source->select) {
        count = 0;
        moq_media_auth_decision_t decision = MOQ_MEDIA_AUTH_DENY;
        moq_result_t rc = source->select(source->ctx, request, scratch,
            MOQ_MEDIA_AUTH_MAX_TOKENS, &count, &decision);
        if (rc != MOQ_OK) {
            if (rc == MOQ_ERR_WOULD_BLOCK || rc >= 0)
                return operation_error(op, MOQ_MEDIA_AUTH_OUTCOME_INVALID_SELECTION,
                                       MOQ_ERR_INVAL);
            return operation_error(op, MOQ_MEDIA_AUTH_OUTCOME_SELECTOR_ERROR, rc);
        }
        if (decision == MOQ_MEDIA_AUTH_DENY)
            return operation_error(op, MOQ_MEDIA_AUTH_OUTCOME_POLICY_DENIED, MOQ_OK);
        if (decision != MOQ_MEDIA_AUTH_ALLOW)
            return operation_error(op, MOQ_MEDIA_AUTH_OUTCOME_INVALID_SELECTION,
                                   MOQ_ERR_INVAL);
        tokens = scratch;
    }
    if (!count || count > MOQ_MEDIA_AUTH_MAX_TOKENS)
        return operation_error(op, MOQ_MEDIA_AUTH_OUTCOME_INVALID_SELECTION,
                               MOQ_ERR_INVAL);
    if (version == MOQ_VERSION_DRAFT_16) {
        for (size_t i = 0; i < count; ++i)
            if (tokens[i].token_type > UINT64_C(0x3fffffffffffffff))
                return operation_error(op, MOQ_MEDIA_AUTH_OUTCOME_INVALID_SELECTION,
                                       MOQ_ERR_INVAL);
    }
    moq_media_auth_outcome_t outcome;
    moq_result_t rc = list_copy(&op->list, tokens, count, alloc, &outcome);
    if (rc != MOQ_OK) return operation_error(op, outcome, rc);
    op->prepared = true;
    return MOQ_OK;
}

void moq_media_auth_operation_notify(moq_media_auth_operation_t *op,
    const moq_media_auth_source_t *source,
    const moq_media_auth_request_t *request, moq_media_auth_scope_t scope,
    moq_media_track_t *track)
{
    if (!op || !op->prepared || op->notified || !op->outcome ||
        !source || !source->configured || !source->on_outcome) return;
    moq_media_auth_outcome_info_t info = {
        .struct_size = sizeof(info),
        .scope = scope,
        .outcome = op->outcome,
        .track = track,
        .request = request,
        .result = op->result,
    };
    op->notified = true;
    source->on_outcome(source->ctx, &info);
}
