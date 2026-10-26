/* Windows has no lane-group backend. Keep the private closure and public
 * stats symbol without creating a partial group or acquiring resources. */
#include "pq_lane_group.h"
#include <stddef.h>
#include <stdint.h>

static bool unsupported_cfg_has(const moq_pq_threaded_cfg_t *cfg,
                                 size_t offset, size_t size)
{
    return cfg->struct_size >= offset && size <= cfg->struct_size - offset;
}
#define UCFG_HAS(cfg, field) \
    unsupported_cfg_has((cfg), offsetof(moq_pq_threaded_cfg_t, field), \
                        sizeof((cfg)->field))

moq_result_t pq_lane_group_create(const moq_pq_threaded_cfg_t *cfg,
                                  moq_pq_threaded_t **out)
{
    if (out) *out = NULL;
    if (!cfg || !out) return MOQ_ERR_INVAL;
    if (!UCFG_HAS(cfg, lane_count) || cfg->lane_count < 2 ||
        cfg->lane_count > MOQ_PQ_THREADED_MAX_LANES)
        return MOQ_ERR_INVAL;
    if (!UCFG_HAS(cfg, perspective) || cfg->perspective != MOQ_PERSPECTIVE_SERVER)
        return MOQ_ERR_UNSUPPORTED;
    if (!cfg->alloc || !cfg->alloc->alloc || !cfg->alloc->free ||
        !cfg->alloc->realloc || !cfg->on_lane_pump ||
        cfg->port <= 0 || cfg->port > 65535 ||
        !cfg->cert_path || !cfg->cert_path[0] ||
        !cfg->key_path || !cfg->key_path[0])
        return MOQ_ERR_INVAL;
    if (UCFG_HAS(cfg, socket_buffer_bytes) &&
        cfg->socket_buffer_bytes > (uint32_t)INT32_MAX)
        return MOQ_ERR_INVAL;
    if (UCFG_HAS(cfg, bind_address) && cfg->bind_address &&
        cfg->bind_address->sa_family != AF_INET &&
        cfg->bind_address->sa_family != AF_INET6)
        return MOQ_ERR_INVAL;
    return MOQ_ERR_UNSUPPORTED;
}

/* No group or group lane can be produced on this platform. Operations refuse
 * work; observers expose no lanes/connections, and admission always refuses.
 * The void cleanup hooks have no owned resources to release. */
uint32_t pq_lane_group_count(const struct pq_lane_group *g) { (void)g; return 0; }
moq_pq_threaded_t *pq_lane_group_lane(struct pq_lane_group *g, uint32_t i)
{ (void)g; (void)i; return NULL; }
size_t pq_lane_group_conn_count(const struct pq_lane_group *g) { (void)g; return 0; }
int pq_lane_group_drain_state(struct pq_lane_group *g) { (void)g; return -1; }
bool pq_lane_group_is_fatal(const struct pq_lane_group *g) { (void)g; return true; }
uint64_t pq_lane_group_fatal_code(const struct pq_lane_group *g) { (void)g; return 0; }
moq_result_t pq_lane_group_wake_all(struct pq_lane_group *g)
{ (void)g; return MOQ_ERR_UNSUPPORTED; }
moq_result_t pq_lane_group_stop(struct pq_lane_group *g)
{ (void)g; return MOQ_ERR_UNSUPPORTED; }
void pq_lane_group_destroy(struct pq_lane_group *g) { (void)g; }
void pq_lane_group_lane_stop(moq_pq_threaded_t *lane) { (void)lane; }
bool pq_lane_group_admit(moq_pq_threaded_t *lane) { (void)lane; return false; }
void pq_lane_group_release(moq_pq_threaded_t *lane) { (void)lane; }

moq_result_t moq_picoquic_threaded_lane_get_stats(
    const moq_pq_threaded_lane_t *lane,
    moq_picoquic_threaded_lane_stats_t *out, size_t out_size)
{
    if (!lane || !out || out_size < MOQ_PICOQUIC_THREADED_LANE_STATS_V0_SIZE)
        return MOQ_ERR_INVAL;
    return MOQ_ERR_UNSUPPORTED;
}
