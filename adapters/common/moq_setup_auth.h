/* Private managed-facade SETUP ownership. */
#ifndef MOQ_MANAGED_SETUP_AUTH_H
#define MOQ_MANAGED_SETUP_AUTH_H
#include <moq/session.h>
#include <string.h>

#define MOQ_MANAGED_SETUP_COPY_BUDGET 65536u

typedef struct moq_managed_setup {
    moq_auth_token_t tokens[16];
    size_t count;
    moq_bytes_t authority;
    moq_bytes_t path;
    uint8_t *storage;
    size_t storage_size;
} moq_managed_setup_t;

static void moq_managed_setup_clear(moq_managed_setup_t *s, const moq_alloc_t *a)
{
    if (s->storage) a->free(s->storage, s->storage_size, a->ctx);
    memset(s, 0, sizeof(*s));
}

static moq_result_t moq_managed_setup_copy(moq_managed_setup_t *s,
    const moq_alloc_t *a, const moq_auth_token_t *tokens, size_t count,
    moq_bytes_t authority, moq_bytes_t path, moq_perspective_t perspective)
{
    size_t total = 0;
    if (count > 16 || (count && !tokens) ||
        (authority.len && !authority.data) || (path.len && !path.data) ||
        (perspective == MOQ_PERSPECTIVE_SERVER && (authority.len || path.len)))
        return MOQ_ERR_INVAL;
    for (size_t i = 0; i < count + 2; ++i) {
        moq_bytes_t span = i < count ? tokens[i].token_value :
            (i == count ? authority : path);
        if (i < count && (tokens[i].token_type > UINT64_C(0x3fffffffffffffff) ||
                         (span.len && !span.data))) return MOQ_ERR_INVAL;
        if (span.len > MOQ_MANAGED_SETUP_COPY_BUDGET - total)
            return MOQ_ERR_BUFFER;
        total += span.len;
    }
    if (total) {
        s->storage = (uint8_t *)a->alloc(total, a->ctx);
        if (!s->storage) return MOQ_ERR_NOMEM;
    }
    s->storage_size = total;
    s->count = count;
    size_t offset = 0;
    for (size_t i = 0; i < count + 2; ++i) {
        moq_bytes_t span = i < count ? tokens[i].token_value :
            (i == count ? authority : path);
        moq_bytes_t copy = {NULL, span.len};
        if (span.len) {
            memcpy(s->storage + offset, span.data, span.len);
            copy.data = s->storage + offset;
            offset += span.len;
        }
        if (i < count) {
            s->tokens[i].token_type = tokens[i].token_type;
            s->tokens[i].token_value = copy;
        } else if (i == count) s->authority = copy;
        else s->path = copy;
    }
    return MOQ_OK;
}

static void moq_managed_setup_apply(const moq_managed_setup_t *s,
                                    moq_session_cfg_t *cfg)
{
    cfg->setup_auth_tokens = s->count ? s->tokens : NULL;
    cfg->setup_auth_token_count = s->count;
    cfg->setup_authority = s->authority;
    cfg->setup_path = s->path;
}

/* Reuse the core encoder's exact wire and send-budget checks before I/O,
 * including for sessions deferred until ALPN negotiation or server accept. */
static moq_result_t moq_managed_setup_preflight(const moq_session_cfg_t *cfg)
{
    if (!cfg->setup_auth_token_count && !cfg->setup_authority.len &&
        !cfg->setup_path.len) return MOQ_OK;
    moq_session_t *session = NULL;
    moq_result_t rc = moq_session_create(cfg, 0, &session);
    if (session) moq_session_destroy(session);
    return rc;
}
#endif
