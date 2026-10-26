#ifndef MOQR_CLUSTER_INTERNAL_H
#define MOQR_CLUSTER_INTERNAL_H

#include <moq/relay/relay.h>
#include <string.h>

static inline bool moqr_cluster_read(const void *src, size_t floor,
                                     void *dst, size_t current)
{
    uint32_t size;
    if (src == NULL) return false;
    memcpy(&size, src, sizeof(size));
    if (size < floor) return false;
    memcpy(dst, src, size < current ? size : current);
    return true;
}

static inline void moqr_cluster_write(void *out, size_t out_size,
                                      const void *value, size_t current)
{
    size_t n = out_size < current ? out_size : current;
    uint32_t written = (uint32_t)n;
    memcpy(out, value, n);
    memcpy(out, &written, sizeof(written));
}

/* Same-build, owner-thread SPI. Not installed. Transfer slots/revisions are
 * implementation details; the public advertisement ID is not a capability.
 * No mirror may be resolved as the original owner of a forced demand. */
typedef struct moqr_cluster_export {
    uint32_t source_index;
    uint64_t revision;
    moqr_ns_t ns;
    moq_bytes_t ns_parts[32];
    moqr_cluster_route_t route;
    uint64_t source_session_cookie;
    uint64_t source_peer_hop_id;
    uint64_t source_correlation_id;
} moqr_cluster_export_t;

MOQR_CORE_API moqr_result_t moqr_core_internal_cluster_export(
    const moqr_core_t *core, uint32_t source_index, moqr_cluster_export_t *out);
MOQR_CORE_API moqr_result_t moqr_core_internal_cluster_import(
    moqr_core_t *core, moqr_binding_t mirror_binding,
    const moqr_cluster_export_t *record, bool withdraw);
MOQR_CORE_API moqr_result_t moqr_core_internal_cluster_upstream_route(
    const moqr_core_t *core, moqr_track_t track, uint64_t track_gen,
    moqr_cluster_advertisement_id_t *out, uint64_t *requester_hop_id,
    uint64_t *requester_correlation_id);
MOQR_CORE_API moqr_result_t moqr_core_internal_subscribe_cluster(
    moqr_core_t *core, moqr_binding_t binding, const moqr_subscribe_req_t *req,
    moqr_cluster_advertisement_id_t source, uint64_t requester_hop_id,
    uint64_t requester_correlation_id, moqr_sub_t *out);

/* Internal only; ordinary relay APIs map exhaustion to their existing
 * policy refusal. New public cluster APIs preserve the stronger LIMIT. */
#define MOQR_INTERNAL_LIMIT (-100)

static inline moq_result_t moqr_result_public(moqr_result_t rc)
{
    switch (rc) {
    case MOQR_OK: return MOQ_OK;
    case MOQR_DONE: return MOQ_DONE;
    case MOQR_ERR_NOMEM: return MOQ_ERR_NOMEM;
    case MOQR_ERR_INVAL: return MOQ_ERR_INVAL;
    case MOQR_ERR_WOULD_BLOCK: return MOQ_ERR_WOULD_BLOCK;
    case MOQR_ERR_STALE_HANDLE: return MOQ_ERR_STALE_HANDLE;
    case MOQR_ERR_WRONG_STATE: return MOQ_ERR_WRONG_STATE;
    case MOQR_ERR_CAPACITY: return MOQ_ERR_CAPACITY;
    case MOQR_ERR_UNSUPPORTED: return MOQ_ERR_UNSUPPORTED;
    case MOQR_INTERNAL_LIMIT: return MOQ_ERR_LIMIT;
    default: return MOQ_ERR_INTERNAL;
    }
}

static inline moqr_result_t moqr_result_legacy(moq_result_t rc)
{
    switch (rc) {
    case MOQ_OK: return MOQR_OK;
    case MOQ_DONE: return MOQR_DONE;
    case MOQ_ERR_NOMEM: return MOQR_ERR_NOMEM;
    case MOQ_ERR_INVAL: return MOQR_ERR_INVAL;
    case MOQ_ERR_WOULD_BLOCK: return MOQR_ERR_WOULD_BLOCK;
    case MOQ_ERR_STALE_HANDLE: return MOQR_ERR_STALE_HANDLE;
    case MOQ_ERR_WRONG_STATE: return MOQR_ERR_WRONG_STATE;
    case MOQ_ERR_CAPACITY: case MOQ_ERR_LIMIT: return MOQR_ERR_CAPACITY;
    case MOQ_ERR_UNSUPPORTED: return MOQR_ERR_UNSUPPORTED;
    default: return MOQR_ERR_INTERNAL;
    }
}

#endif
