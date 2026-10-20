/*
 * Transport binding: how the session is carried, told once before start.
 * The profile declares which facts it needs; this file validates and stores
 * the host's answer and the connection target it names. Nothing here reads
 * or writes the wire.
 */
#include "session_internal.h"
#include "transport_binding_internal.h"

void moq_transport_binding_init_sized(moq_transport_binding_t *b, size_t size)
{
    if (!b) return;
    size_t n = size < sizeof(*b) ? size : sizeof(*b);
    memset(b, 0, n);
    if (n >= sizeof(b->struct_size)) b->struct_size = (uint32_t)n;
}

uint32_t moq_session_binding_requirements(const moq_session_t *s)
{
    return s && s->profile ? s->profile->binding_requirements : 0;
}

uint32_t moq_version_binding_requirements(moq_version_t version)
{
    const moq_profile_ops_t *p = moq_profile_lookup(version);
    return p ? p->binding_requirements : 0;
}

moq_result_t moq_session_bind_transport(moq_session_t *s,
                                        const moq_transport_binding_t *b,
                                        size_t size)
{
    if (!s || !b || size < MOQ_TRANSPORT_BINDING_V0_SIZE) return MOQ_ERR_INVAL;
    if (b->struct_size < MOQ_TRANSPORT_BINDING_V0_SIZE || b->struct_size > size)
        return MOQ_ERR_INVAL;
    if (s->state != MOQ_SESS_IDLE || s->binding.bound) return MOQ_ERR_WRONG_STATE;
    uint32_t required = moq_session_binding_requirements(s);
    if (((b->facts_known | b->facts_supported) & ~MOQ_BINDING_FACTS_ALL) ||
        (b->facts_supported & ~b->facts_known) ||
        (b->target_present & ~MOQ_BINDING_TARGET_COMPLETE))
        return MOQ_ERR_INVAL;
    if (b->kind == MOQ_BINDING_WEBTRANSPORT_H2) return MOQ_ERR_UNSUPPORTED;
    if (b->kind != MOQ_BINDING_RAW_QUIC && b->kind != MOQ_BINDING_WEBTRANSPORT_H3)
        return MOQ_ERR_INVAL;
    if ((b->facts_known & required) != required) return MOQ_ERR_WOULD_BLOCK;
    if ((b->facts_supported & required) != required) return MOQ_ERR_UNSUPPORTED;
    if (b->authority.len > UINT16_MAX ||
        b->path_query.len > UINT16_MAX - b->authority.len)
        return MOQ_ERR_INVAL;

    bool server = b->kind == MOQ_BINDING_RAW_QUIC &&
                  s->perspective == MOQ_PERSPECTIVE_SERVER;
    if (server) {
        if (!b->accept_uri || b->target_present ||
            b->authority.len || b->path_query.len)
            return MOQ_ERR_INVAL;
    } else {
        if (b->accept_uri || b->accept_uri_ctx ||
            b->target_present != MOQ_BINDING_TARGET_COMPLETE ||
            !moq_uri_authority_valid(b->authority) ||
            !moq_uri_path_query_valid(b->path_query))
            return MOQ_ERR_INVAL;
    }

    size_t n = b->authority.len + b->path_query.len;
    uint8_t *copy = n ? s->alloc.alloc(n, s->alloc.ctx) : NULL;
    if (n && !copy) return MOQ_ERR_NOMEM;

    moq_binding_state_t state;
    memset(&state, 0, sizeof(state));
    memcpy(&state.value, b, MOQ_TRANSPORT_BINDING_V0_SIZE);
    state.value.struct_size = (uint32_t)MOQ_TRANSPORT_BINDING_V0_SIZE;
    state.value.authority.data = NULL;
    state.value.path_query.data = NULL;
    if (b->authority.len) {
        memcpy(copy, b->authority.data, b->authority.len);
        state.value.authority.data = copy;
    }
    if (b->path_query.len) {
        memcpy(copy + b->authority.len, b->path_query.data, b->path_query.len);
        state.value.path_query.data = copy + b->authority.len;
    }
    state.owned_target = copy;
    state.owned_size = n;
    state.bound = true;
    s->binding = state;
    return MOQ_OK;
}

bool moq_binding_required_unbound(const moq_session_t *s)
{
    return moq_session_binding_requirements(s) != 0 && !s->binding.bound;
}

void moq_binding_destroy(moq_session_t *s)
{
    if (s->binding.owned_target)
        s->alloc.free(s->binding.owned_target, s->binding.owned_size,
                      s->alloc.ctx);
    memset(&s->binding, 0, sizeof(s->binding));
}

bool moq_binding_sends_target(const moq_session_t *s)
{
    return s->binding.bound && s->binding.value.kind == MOQ_BINDING_RAW_QUIC &&
           s->perspective == MOQ_PERSPECTIVE_CLIENT;
}

/* Close codes shared by every profile that carries PATH and AUTHORITY:
 * INVALID_PATH, MALFORMED_PATH, INVALID_AUTHORITY, MALFORMED_AUTHORITY. */
#define BINDING_INVALID_PATH        UINT64_C(0x8)
#define BINDING_MALFORMED_PATH      UINT64_C(0x9)
#define BINDING_INVALID_AUTHORITY   UINT64_C(0x19)
#define BINDING_MALFORMED_AUTHORITY UINT64_C(0x1a)

uint64_t moq_binding_judge_peer_target(const moq_session_t *s,
                                       const moq_binding_peer_target_t *target,
                                       const char **reason)
{
    const bool has_path = target->has_path;
    const bool has_authority = target->has_authority;
    const moq_bytes_t path = target->path;
    const moq_bytes_t authority = target->authority;
    bool peer_is_client = s->perspective == MOQ_PERSPECTIVE_SERVER;
    if (has_path && !peer_is_client) {
        *reason = "PATH from server";
        return BINDING_INVALID_PATH;
    }
    if (has_authority && !peer_is_client) {
        *reason = "AUTHORITY from server";
        return BINDING_INVALID_AUTHORITY;
    }
    const moq_transport_binding_t *binding = &s->binding.value;
    if (s->binding.bound && binding->kind == MOQ_BINDING_WEBTRANSPORT_H3) {
        if (has_authority) {
            *reason = "AUTHORITY over WebTransport";
            return BINDING_INVALID_AUTHORITY;
        }
        if (has_path) {
            *reason = "PATH over WebTransport";
            return BINDING_INVALID_PATH;
        }
    }
    if (has_authority && !moq_uri_authority_valid(authority)) {
        *reason = "malformed AUTHORITY";
        return BINDING_MALFORMED_AUTHORITY;
    }
    if (has_path && !moq_uri_path_query_valid(path)) {
        *reason = "malformed PATH";
        return BINDING_MALFORMED_PATH;
    }
    if (s->binding.bound && binding->kind == MOQ_BINDING_RAW_QUIC &&
        peer_is_client && binding->accept_uri) {
        uint32_t present = (has_authority ? MOQ_BINDING_AUTHORITY_PRESENT : 0) |
                           (has_path ? MOQ_BINDING_PATH_PRESENT : 0);
        uint32_t verdict = binding->accept_uri(binding->accept_uri_ctx, present,
                                               has_authority ? authority
                                                             : (moq_bytes_t){0},
                                               has_path ? path : (moq_bytes_t){0});
        if (verdict == MOQ_URI_INVALID_AUTHORITY) {
            *reason = "unsupported AUTHORITY";
            return BINDING_INVALID_AUTHORITY;
        }
        if (verdict == MOQ_URI_INVALID_PATH) {
            *reason = "unsupported PATH";
            return BINDING_INVALID_PATH;
        }
        if (verdict != MOQ_URI_ACCEPT) {
            *reason = "unknown URI policy verdict";
            return UINT64_C(0x1);
        }
    }
    return 0;
}
