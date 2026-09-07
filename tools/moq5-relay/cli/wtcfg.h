/*
 * The WebTransport listener's construction-and-create step.
 *
 * The relay builds one facade configuration from the resolved CLI config and
 * immediately hands it to the transport. Splitting those two apart would leave
 * the interesting half untested: what matters is not the struct a mapper
 * returns but the bytes the transport is actually given, so this owns the
 * whole step -- initialize, fill, create -- and returns exactly what the
 * transport's create returned.
 *
 * Available only where the WebTransport facade is, so a raw-only build neither
 * compiles nor links it.
 */
#ifndef MOQR_CLI_WTCFG_H
#define MOQR_CLI_WTCFG_H

#include <stdint.h>

#include "moq/wtquic_msquic_managed.h"

#include "config.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Build the WebTransport facade configuration from `cfg` and create the
 * listener.
 *
 * `cfg` must stay alive for the duration of the call: the configuration
 * borrows its host, credential paths, path, subprotocol array and Origin
 * allowlist, and the transport takes its own copies during create.
 *
 * `lane_count` and `max_connections` are this listener's share of the process
 * plan, never the combined totals. `on_lane_pump` and `on_lane_pump_user` are
 * passed through unchanged.
 *
 * Returns the transport's own result. Nothing is mapped, softened or retried.
 */
moq_result_t moqr_cli_wt_listener_create(
    const moqr_cli_config_t *cfg,
    uint32_t lane_count,
    uint32_t max_connections,
    moq_wtquic_msquic_lane_pump_fn on_lane_pump,
    void *on_lane_pump_user,
    moq_wtquic_msquic_managed_t **out);

#ifdef __cplusplus
}
#endif

#endif /* MOQR_CLI_WTCFG_H */
