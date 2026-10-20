/*
 * Wire-profile registry: which versions this build speaks and the table for
 * each. The public queries let transports and the service tier offer exactly
 * the versions whose profile is complete, without naming any of them.
 */

#include "session_internal.h"
#include "../spi/core_spi.h"

/* Newest first: the client's offer order is what ALPN and the WebTransport
 * protocol field honour, so this order is what peers see. */
static const moq_version_t registry_versions[] = {
    MOQ_VERSION_DRAFT_22,
    MOQ_VERSION_DRAFT_18,
    MOQ_VERSION_DRAFT_16,
};

const moq_profile_ops_t *moq_profile_lookup(moq_version_t version)
{
    switch (version) {
    case MOQ_VERSION_DRAFT_16:
        return moq_d16_profile_ops();
    case MOQ_VERSION_DRAFT_18:
        return moq_d18_profile_ops();
    case MOQ_VERSION_DRAFT_22:
        return moq_d22_profile_ops();
    default:
        return NULL;
    }
}

size_t moq_profile_versions(moq_version_t *out, size_t cap)
{
    size_t n = 0;
    for (size_t i = 0; i < sizeof(registry_versions) / sizeof(registry_versions[0]); i++) {
        if (n >= cap) break;
        out[n++] = registry_versions[i];
    }
    return n;
}

bool moq_version_supported(moq_version_t version)
{
    const moq_profile_ops_t *p = moq_profile_lookup(version);
    return p != NULL && p->wire_ready;
}

bool moq_core_internal_track_max_cache_duration(moq_version_t version,
                                                const uint8_t *props, size_t len,
                                                uint64_t *out_ms)
{
    const moq_profile_ops_t *p = moq_profile_lookup(version);
    if (!p || !p->track_properties_max_cache || !out_ms || (!props && len))
        return false;
    uint64_t ms = 0;
    if (!p->track_properties_max_cache(props, len, &ms)) return false;
    *out_ms = ms;
    return true;
}

moq_result_t moq_profile_check_track_properties(const moq_profile_ops_t *p,
                                                const uint8_t *data, size_t len)
{
    if (len == 0) return MOQ_OK;
    if (!data) return MOQ_ERR_INVAL;
    moq_dt_scan_t dtscan;
    uint8_t priority = 128;
    if ((p->validate_track_properties && p->validate_track_properties(data, len) < 0) ||
        p->scan_delivery_timeouts(data, len, true, &dtscan) < 0 ||
        (p->scan_default_publisher_priority &&
         p->scan_default_publisher_priority(data, len, &priority) < 0))
        return MOQ_ERR_INVAL;
    return MOQ_OK;
}

moq_result_t moq_profile_build_track_properties(const moq_profile_ops_t *p,
                                                const moq_alloc_t *a,
                                                const uint8_t *in, size_t len,
                                                bool dynamic_groups,
                                                moq_group_order_t group_order,
                                                uint8_t **out, size_t *out_cap,
                                                size_t *out_len)
{
    *out = NULL;
    *out_cap = *out_len = 0;
    bool ordered = group_order != MOQ_GROUP_ORDER_DEFAULT;
    if ((len > 0 && !in) || group_order > MOQ_GROUP_ORDER_DESCENDING)
        return MOQ_ERR_INVAL;
    /* Each added pair is two integers, and the delta that follows it can only
     * shrink: 2 x 8 bytes cover it in either integer encoding. */
    size_t extra = (dynamic_groups ? 16 : 0) + (ordered ? 16 : 0);
    if (len > SIZE_MAX - extra) return MOQ_ERR_INVAL;
    size_t cap = len + extra;
    if (cap == 0) return MOQ_OK;
    uint8_t *buf = (uint8_t *)a->alloc(cap, a->ctx);
    if (!buf) return MOQ_ERR_NOMEM;
    /* The adders copy, so adding both goes through a second buffer. */
    uint8_t *tmp = NULL;
    if (dynamic_groups && ordered && !(tmp = (uint8_t *)a->alloc(cap, a->ctx))) {
        a->free(buf, cap, a->ctx);
        return MOQ_ERR_NOMEM;
    }
    size_t n = len;
    moq_result_t rc = MOQ_OK;
    if (!dynamic_groups && !ordered)
        memcpy(buf, in, len);
    if (dynamic_groups) {
        uint8_t *dst = ordered ? tmp : buf;
        rc = p->track_properties_add_dynamic_groups(in, len, dst, cap, &n);
        in = dst;
        len = n;
    }
    if (rc == MOQ_OK && ordered)
        rc = p->track_properties_add_group_order(in, len, (uint8_t)group_order,
                                                 buf, cap, &n);
    if (tmp) a->free(tmp, cap, a->ctx);
    if (rc == MOQ_OK && moq_profile_check_track_properties(p, buf, n) < 0)
        rc = MOQ_ERR_INVAL;
    if (rc < 0) {
        a->free(buf, cap, a->ctx);
        return MOQ_ERR_INVAL;
    }
    *out = buf;
    *out_cap = cap;
    *out_len = n;
    return MOQ_OK;
}

moq_result_t moq_core_internal_track_properties_validate(moq_version_t version,
                                                         const uint8_t *props,
                                                         size_t len,
                                                         bool dynamic_groups,
                                                         moq_group_order_t group_order)
{
    if ((len > 0 && !props) || group_order > MOQ_GROUP_ORDER_DESCENDING)
        return MOQ_ERR_INVAL;
    if (!moq_version_supported(version)) return MOQ_ERR_UNSUPPORTED;
    const moq_profile_ops_t *p = moq_profile_lookup(version);
    if (!dynamic_groups && group_order == MOQ_GROUP_ORDER_DEFAULT)
        return moq_profile_check_track_properties(p, props, len);
    /* As the facade publishes it. */
    const moq_alloc_t *a = moq_alloc_default();
    uint8_t *buf;
    size_t cap, n;
    moq_result_t rc = moq_profile_build_track_properties(p, a, props, len,
        dynamic_groups, group_order, &buf, &cap, &n);
    if (buf) a->free(buf, cap, a->ctx);
    return rc;
}

moq_result_t moq_track_properties_validate(moq_version_t version,
                                           const uint8_t *props, size_t len,
                                           bool dynamic_groups)
{
    return moq_core_internal_track_properties_validate(version, props, len,
        dynamic_groups, MOQ_GROUP_ORDER_DEFAULT);
}

size_t moq_versions_supported(moq_version_t *out, size_t cap)
{
    moq_version_t all[sizeof(registry_versions) / sizeof(registry_versions[0])];
    size_t total = moq_profile_versions(all, sizeof(all) / sizeof(all[0]));
    size_t n = 0;
    for (size_t i = 0; i < total; i++) {
        if (!moq_version_supported(all[i])) continue;
        if (out && n < cap) out[n] = all[i];
        n++;
    }
    return n;
}
