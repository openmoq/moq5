#include "session_internal.h"

moq_result_t handle_start(moq_session_t *s)
{
    return s->profile->start(s);
}

/* A complete field is required before touching an additive config tail. In
 * particular, a lone token pointer must never be dereferenced. */
#define SETUP_CFG_HAS(c, f) ((c)->struct_size >= \
    offsetof(moq_session_cfg_t, f) + sizeof((c)->f))

moq_result_t session_prepare_setup(moq_session_t *s, const moq_session_cfg_t *cfg)
{
    const moq_auth_token_t *tokens = NULL;
    size_t count = 0;
    moq_bytes_t authority = {0}, path = {0};
    if (SETUP_CFG_HAS(cfg, setup_auth_token_count)) {
        tokens = cfg->setup_auth_tokens;
        count = cfg->setup_auth_token_count;
    }
    if (SETUP_CFG_HAS(cfg, setup_authority)) authority = cfg->setup_authority;
    if (SETUP_CFG_HAS(cfg, setup_path)) path = cfg->setup_path;
    if ((count && !tokens) || count > MOQ_DECODED_MAX_TOKENS ||
        (authority.len && !authority.data) || (path.len && !path.data))
        return MOQ_ERR_INVAL;
    if (s->perspective == MOQ_PERSPECTIVE_SERVER && (authority.len || path.len))
        return MOQ_ERR_INVAL;
    if (authority.len > 65535 || path.len > 65535) return MOQ_ERR_BUFFER;

    for (size_t i = 0; i < count; ++i) {
        const moq_auth_token_t *t = &tokens[i];
        if ((t->token_value.len && !t->token_value.data) ||
            t->token_type > MOQ_QUIC_VARINT_MAX) return MOQ_ERR_INVAL;
        if (t->token_value.len > 65535) return MOQ_ERR_BUFFER;
    }
    return s->profile->prepare_setup(s, tokens, count, authority, path);
}
