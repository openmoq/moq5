/*
 * The serve log helpers: every readiness, operating-point, attribution and
 * stop record of BOTH serve compositions, over copied rows and configuration
 * only. Nothing here touches a live facade, shard runtime, bind or core, so
 * the production emission can be driven by a test with fixture rows and a
 * scripted sink. The sink itself stays a local of the serve function.
 */
#ifndef MOQR_CLI_SERVELOG_H
#define MOQR_CLI_SERVELOG_H

#include "config.h"
#include "logevent.h"

#include <moq/relay/moqr_bind.h>
#include <moq/relay/moqr_shards.h>

#include <moq/msquic_managed.h>

#include <stdbool.h>
#include <stdint.h>

/* The readiness sequence, in the order both paths publish it: the raw
 * listener, the admin endpoint when one is bound (admin_port < 0 otherwise),
 * the WebTransport listener when the plan has WT lanes, the operating point
 * from the resolved shard configuration, the signal contract, then the one
 * readiness flush. */
void moqr_cli_serve_log_readiness(moqr_cli_log_t *log,
                                  const moqr_cli_config_t *cfg,
                                  uint32_t composition, int admin_port,
                                  uint32_t wt_lanes,
                                  const moqr_shards_cfg_t *scfg);

/* One lane's attribution record; ss == NULL is the lanes=1 composition (its
 * shard-plane fields are true zeros). A refused getter is an explicit
 * refusal record. */
void moqr_cli_serve_log_lane(moqr_cli_log_t *log, uint32_t lane, bool ad_ok,
                             const moq_msquic_lane_stats_t *ad, bool sh_ok,
                             const moqr_shards_stats_t *ss);

/* One directed-pair record, or its refusal. */
void moqr_cli_serve_log_pair(moqr_cli_log_t *log, uint32_t src, uint32_t dst,
                             bool ok, const moqr_shards_pair_stats_t *pst);

/* The stop records: the single-facade one, one per shard (poisoned when its
 * post-join snapshot is unreadable), and the multi-shard total. */
void moqr_cli_serve_log_stop_k1(moqr_cli_log_t *log,
                                const moqr_bind_stats_t *bs,
                                const moqr_core_stats_t *cs);
void moqr_cli_serve_log_stop_shard(moqr_cli_log_t *log, uint32_t shard,
                                   const moqr_bind_stats_t *bs,
                                   const moqr_core_stats_t *cs,
                                   uint64_t pump_turns, uint64_t wakes,
                                   bool ss_ok, const moqr_shards_stats_t *ss);
void moqr_cli_serve_log_stop_total(moqr_cli_log_t *log, uint32_t shards,
                                   uint32_t conns, uint32_t tracks,
                                   uint64_t ingested, uint64_t delivered,
                                   uint64_t session_errors);

#endif /* MOQR_CLI_SERVELOG_H */
