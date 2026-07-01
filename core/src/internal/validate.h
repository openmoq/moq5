#ifndef MOQ_INTERNAL_VALIDATE_H
#define MOQ_INTERNAL_VALIDATE_H

#include <moq/types.h>
#include <moq/wire.h>

#ifdef __GNUC__
#define MOQ_VALIDATE_UNUSED __attribute__((unused))
#else
#define MOQ_VALIDATE_UNUSED
#endif

MOQ_VALIDATE_UNUSED
static moq_result_t moq_validate_namespace(const moq_namespace_t *ns)
{
    if (!ns) return MOQ_ERR_INVAL;
    if (ns->count == 0 || ns->count > 32)
        return MOQ_ERR_INVAL;
    if (!ns->parts) return MOQ_ERR_INVAL;
    size_t total = 0;
    for (size_t i = 0; i < ns->count; i++) {
        if (ns->parts[i].len == 0) return MOQ_ERR_INVAL;
        if (!ns->parts[i].data) return MOQ_ERR_INVAL;
        if (ns->parts[i].len > MOQ_FULL_TRACK_NAME_MAX - total)
            return MOQ_ERR_INVAL;
        total += ns->parts[i].len;
    }
    return MOQ_OK;
}

MOQ_VALIDATE_UNUSED
static moq_result_t moq_validate_full_track_name(
    const moq_namespace_t *ns, moq_bytes_t track_name)
{
    moq_result_t rc = moq_validate_namespace(ns);
    if (rc < 0) return rc;
    if (track_name.len > 0 && !track_name.data)
        return MOQ_ERR_INVAL;
    size_t ns_total = 0;
    for (size_t i = 0; i < ns->count; i++)
        ns_total += ns->parts[i].len;
    if (track_name.len > MOQ_FULL_TRACK_NAME_MAX - ns_total)
        return MOQ_ERR_INVAL;
    return MOQ_OK;
}

MOQ_VALIDATE_UNUSED
static moq_result_t moq_validate_auth_tokens(const moq_auth_token_t *tokens,
                                               size_t count)
{
    if (count > 0 && !tokens) return MOQ_ERR_INVAL;
    for (size_t i = 0; i < count; i++) {
        if (tokens[i].token_type > MOQ_QUIC_VARINT_MAX)
            return MOQ_ERR_INVAL;
        if (tokens[i].token_value.len > 0 && !tokens[i].token_value.data)
            return MOQ_ERR_INVAL;
    }
    return MOQ_OK;
}

#endif /* MOQ_INTERNAL_VALIDATE_H */
