#include "session_internal.h"
#include "moq/codec.h"
#include "moq/control_d18.h"

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
    if ((count && !tokens) || count > MOQ_D18_MAX_AUTH_TOKENS ||
        (authority.len && !authority.data) || (path.len && !path.data))
        return MOQ_ERR_INVAL;
    if (s->perspective == MOQ_PERSPECTIVE_SERVER && (authority.len || path.len))
        return MOQ_ERR_INVAL;
    if (authority.len > 65535 || path.len > 65535) return MOQ_ERR_BUFFER;

    bool d18 = s->profile->version == MOQ_VERSION_DRAFT_18;
    bool request_cap = !d18 && SETUP_CFG_HAS(cfg, send_request_capacity) &&
                       cfg->send_request_capacity;
    size_t (*vlen)(uint64_t) = d18 ? moq_vi64_len : moq_quic_varint_len;
    size_t params = count + (authority.len != 0) + (path.len != 0) +
                    s->send_auth_token_cache_size + request_cap;
    /* Match the receiving draft-16 parameter and resolved-token capacities. */
    if (!d18 && params > 16) return MOQ_ERR_INVAL;
    size_t payload = d18 ? 0 : vlen(params);
    size_t token_bytes = 0;
    for (size_t i = 0; i < count; ++i) {
        const moq_auth_token_t *t = &tokens[i];
        if ((t->token_value.len && !t->token_value.data) ||
            t->token_type > MOQ_QUIC_VARINT_MAX) return MOQ_ERR_INVAL;
        if (t->token_value.len > 65535) return MOQ_ERR_BUFFER;
        size_t n = vlen(MOQ_AUTH_TOKEN_USE_VALUE) + vlen(t->token_type) +
                   t->token_value.len;
        token_bytes += n;
        payload += 1 + vlen(n) + n;
    }
    if (authority.len) payload += 1 + vlen(authority.len) + authority.len;
    if (path.len) payload += 1 + vlen(path.len) + path.len;
    if (request_cap) payload += 1 + vlen(cfg->initial_request_capacity);
    if (s->send_auth_token_cache_size)
        payload += 1 + vlen(s->auth_token_cache_size);
    /* All sums above have at most 20 terms individually bounded by 65553. */
    if (payload > 65535) return MOQ_ERR_BUFFER;
    size_t wire_len = payload + 2 + vlen(d18 ? MOQ_D18_STREAM_SETUP :
        (s->perspective == MOQ_PERSPECTIVE_CLIENT ? MOQ_D16_CLIENT_SETUP :
                                                  MOQ_D16_SERVER_SETUP));
    /* Historical configs may intentionally exercise a too-small send buffer
     * at start/receive time. New credentials/routes must fit before startup. */
    if ((count || authority.len || path.len) && wire_len > s->send_cap)
        return MOQ_ERR_BUFFER;
    size_t alloc_len = wire_len + (d18 ? 0 : token_bytes);
    uint8_t *wire = s->alloc.alloc(alloc_len, s->alloc.ctx);
    if (!wire) return MOQ_ERR_NOMEM;
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, wire, wire_len);
    moq_result_t rc;
    if (d18) {
        moq_d18_setup_opts_t opts = {0};
        opts.has_path = path.len != 0;
        opts.has_authority = authority.len != 0;
        opts.has_max_auth_token_cache_size = s->send_auth_token_cache_size;
        opts.max_auth_token_cache_size = s->auth_token_cache_size;
        opts.auth_token_count = count;
        for (size_t i = 0; i < count; ++i) {
            opts.auth_tokens[i].alias_type = MOQ_AUTH_TOKEN_USE_VALUE;
            opts.auth_tokens[i].token_type = tokens[i].token_type;
            opts.auth_tokens[i].token_value = tokens[i].token_value;
        }
        rc = moq_d18_encode_setup_opts_routes(&w, &opts, authority, path);
    } else {
        moq_kvp_entry_t kv[16] = {0};
        size_t n = 0;
        uint8_t cap_buf[8], cache_buf[8];
        if (path.len) kv[n++] = (moq_kvp_entry_t){
            .type = MOQ_SETUP_PARAM_PATH, .value = path.data, .value_len = path.len};
        if (request_cap) kv[n++] = (moq_kvp_entry_t){
            .type = MOQ_SETUP_PARAM_MAX_REQUEST_ID, .value = cap_buf,
            .value_len = moq_quic_varint_encode(cfg->initial_request_capacity,
                                               cap_buf, sizeof(cap_buf)),
            .is_varint = true};
        moq_buf_writer_t tw;
        moq_buf_writer_init(&tw, wire + wire_len, token_bytes);
        for (size_t i = 0; i < count; ++i) {
            size_t start = tw.pos;
            moq_d16_auth_token_t token = {
                .alias_type = MOQ_AUTH_TOKEN_USE_VALUE,
                .token_type = tokens[i].token_type,
                .token_value = tokens[i].token_value.data,
                .token_value_len = tokens[i].token_value.len};
            rc = moq_d16_auth_token_encode(&tw, &token);
            if (rc < 0) goto fail;
            kv[n++] = (moq_kvp_entry_t){.type = MOQ_SETUP_PARAM_AUTHORIZATION_TOKEN,
                .value = wire + wire_len + start, .value_len = tw.pos - start};
        }
        if (s->send_auth_token_cache_size) kv[n++] = (moq_kvp_entry_t){
            .type = MOQ_SETUP_PARAM_MAX_AUTH_TOKEN_CACHE_SIZE, .value = cache_buf,
            .value_len = moq_quic_varint_encode(s->auth_token_cache_size,
                                               cache_buf, sizeof(cache_buf)),
            .is_varint = true};
        if (authority.len) kv[n++] = (moq_kvp_entry_t){
            .type = MOQ_SETUP_PARAM_AUTHORITY, .value = authority.data,
            .value_len = authority.len};
        rc = s->perspective == MOQ_PERSPECTIVE_CLIENT ?
            moq_d16_encode_client_setup(&w, kv, n) :
            moq_d16_encode_server_setup(&w, kv, n);
    }
    if (rc < 0) goto fail;
    s->setup_wire = wire;
    s->setup_wire_len = w.pos;
    s->setup_wire_alloc = alloc_len;
    return MOQ_OK;
fail:
    s->alloc.free(wire, alloc_len, s->alloc.ctx);
    return rc;
}
