/*
 * Installed-consumer smoke for the managed Network.framework client:
 * find_package(libmoq COMPONENTS adapter-wtquic-network-managed) must
 * resolve the facade AND its transitive deps (moq::adapter-wtquic,
 * wtq::network, Network.framework). Linking this binary forces real
 * symbol resolution across all three installed archives; the calls
 * exercise the facade — including the unified lane interface — without
 * dialing.
 */

#include <moq/wtquic_network_managed.h>

int main(void)
{
    moq_wtquic_network_managed_cfg_t cfg;
    moq_wtquic_network_managed_t *m = NULL;

    moq_wtquic_network_managed_cfg_init(&cfg);
    if (cfg.struct_size != sizeof(cfg))
        return 1;
    /* the size-aware initializer resolves from the installed archive too */
    moq_wtquic_network_managed_cfg_init_sized(&cfg, sizeof(cfg));
    if (cfg.struct_size != sizeof(cfg))
        return 1;
    /* invalid on purpose (no host): must refuse without side effects */
    if (moq_wtquic_network_managed_create(&cfg, &m) != MOQ_ERR_INVAL)
        return 1;
    if (m != NULL)
        return 1;
    if (moq_wtquic_network_managed_is_fatal(NULL) ||
        moq_wtquic_network_managed_negotiated_version(NULL) != 0)
        return 1;
    /* the lane interface resolves and is NULL-safe */
    if (moq_wtquic_network_managed_lane_count(NULL) != 0 ||
        moq_wtquic_network_managed_lane(NULL, 0) != NULL ||
        moq_wtquic_network_lane_index(NULL) != 0 ||
        moq_wtquic_network_lane_next_conn(NULL, NULL) != NULL ||
        moq_wtquic_network_lane_wake(NULL) != MOQ_ERR_INVAL ||
        moq_wtquic_network_managed_conn_session(NULL) != NULL ||
        moq_wtquic_network_managed_conn_adapter(NULL) != NULL ||
        moq_wtquic_network_managed_conn_lane(NULL) != NULL)
        return 1;
    return 0;
}
