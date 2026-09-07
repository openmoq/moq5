#include "wtcfg.h"

#include <stddef.h>

/*
 * The two transport callees, named once.
 *
 * Production compiles these to the facade's own exported functions; there is no
 * indirection, no function table and no branch at run time. A deterministic
 * test build substitutes typed functions of the same signature at compile time,
 * so the configuration can be captured AT the create call rather than inferred
 * from a value returned earlier.
 */
#ifndef MOQR_WT_CFG_INIT_SIZED
#define MOQR_WT_CFG_INIT_SIZED moq_wtquic_msquic_managed_cfg_init_sized
#endif
#ifndef MOQR_WT_CREATE
#define MOQR_WT_CREATE moq_wtquic_msquic_managed_create
#endif

moq_result_t
moqr_cli_wt_listener_create(const moqr_cli_config_t *cfg,
                            uint32_t lane_count,
                            uint32_t max_connections,
                            moq_wtquic_msquic_lane_pump_fn on_lane_pump,
                            void *on_lane_pump_user,
                            moq_wtquic_msquic_managed_t **out)
{
    moq_wtquic_msquic_managed_cfg_t wcfg;

    MOQR_WT_CFG_INIT_SIZED(&wcfg, sizeof(wcfg));
    wcfg.alloc = moq_alloc_default();
    wcfg.perspective = MOQ_PERSPECTIVE_SERVER;
    wcfg.host = cfg->wt.host;
    wcfg.port = (uint16_t)cfg->wt.port;
    wcfg.cert_path = cfg->wt.cert;
    wcfg.key_path = cfg->wt.key;
    wcfg.send_request_capacity = true;
    wcfg.initial_request_capacity = 1024;
    wcfg.streaming_objects = true;
    wcfg.wt_path = cfg->wt.path;
    wcfg.wt_protocols = cfg->wt.subprotos;
    wcfg.wt_protocol_count = cfg->wt.version_count;
    wcfg.webtransport_profile = (uint32_t)cfg->wt.profile;
    wcfg.origin_policy = cfg->wt.origin_policy;
    /*
     * A list travels with the ALLOWLIST policy and with nothing else. The
     * facade refuses a non-NULL array under any other policy, and the config's
     * embedded array has a real address even when it holds nothing -- so an
     * unconditional assignment would turn every default and allow-any
     * configuration into a refusal at create.
     */
    if (cfg->wt.origin_policy == MOQR_CLI_ORIGIN_POLICY_ALLOWLIST) {
        wcfg.allowed_origins = cfg->wt.origins;
        wcfg.allowed_origin_count = cfg->wt.origin_count;
    } else {
        wcfg.allowed_origins = NULL;
        wcfg.allowed_origin_count = 0;
    }
    wcfg.lane_count = lane_count;
    wcfg.max_connections = max_connections;
    wcfg.on_lane_pump = on_lane_pump;
    wcfg.on_lane_pump_user = on_lane_pump_user;

    return MOQR_WT_CREATE(&wcfg, out);
}
