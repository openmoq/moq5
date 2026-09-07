#include "servelog.h"

#include <string.h>

/* The strict versioned operating-point record from the RESOLVED limits (the
 * resolver's output, never the request): the summarizer joins these four
 * values into the workload fingerprint, so runs at different operating
 * points can never pool. Emitted once, at serve start, on every serve path.
 * A resolve failure is unreachable here (the preflight already validated
 * this config), but fail closed with a refusal record anyway. */
static void
serve_log_run_config(moqr_cli_log_t *log, const moqr_shards_cfg_t *scfg)
{
    moqr_shards_limits_t lim;
    if (moqr_shards_cfg_resolve(scfg, &lim) != MOQR_OK) {
        moqr_cli_log_run_config_refused(log, "resolver");
        return;
    }
    moqr_cli_run_config_row_t rc;
    memset(&rc, 0, sizeof(rc));
    rc.pump_turn_messages = lim.pump_turn_msgs;
    rc.pump_turn_bytes = lim.pump_turn_bytes;
    rc.demand_channel_entries = lim.dch_cap;
    rc.demand_channel_bytes = lim.dch_byte_cap;
    moqr_cli_log_run_config(log, &rc);
}

/* The readiness sequence, in the order both paths publish it: the raw
 * listener, the admin endpoint when one is bound (admin_port < 0 otherwise),
 * the WebTransport listener when the plan has WT lanes, the operating point,
 * the signal contract, then the one readiness flush. */
void
moqr_cli_serve_log_readiness(moqr_cli_log_t *log, const moqr_cli_config_t *cfg,
                    uint32_t composition, int admin_port, uint32_t wt_lanes,
                    const moqr_shards_cfg_t *scfg)
{
    moqr_cli_log_ready_t v;

    memset(&v, 0, sizeof(v));
    v.kind = MOQR_CLI_LOG_READY_RAW;
    v.composition = composition;
    v.host = cfg->host;
    v.host_n = strlen(cfg->host);
    v.port = cfg->port;
    v.alpn_set = cfg->alpn_set;
    v.alpn_n = strlen(cfg->alpn_set);
    v.lanes = cfg->lanes;
    moqr_cli_log_ready(log, &v);
    if (admin_port >= 0) {
        memset(&v, 0, sizeof(v));
        v.kind = MOQR_CLI_LOG_READY_ADMIN;
        v.composition = composition;
        v.host = cfg->admin.host;
        v.host_n = strlen(cfg->admin.host);
        v.port = admin_port;
        moqr_cli_log_ready(log, &v);
    }
    if (wt_lanes > 0) {
        /* The WebTransport listener's OWN offered set and its configured
         * profile under the closed mapping: a profile outside the declared
         * set has no name, and the record is refused rather than defaulted. */
        const char *profile = moqr_cli_wt_profile_name(cfg->wt.profile);
        memset(&v, 0, sizeof(v));
        v.kind = MOQR_CLI_LOG_READY_WEBTRANSPORT;
        v.composition = composition;
        v.host = cfg->wt.host;
        v.host_n = strlen(cfg->wt.host);
        v.port = cfg->wt.port;
        v.path = cfg->wt.path;
        v.path_n = strlen(cfg->wt.path);
        v.alpn_set = cfg->wt.alpn_set;
        v.alpn_n = strlen(cfg->wt.alpn_set);
        v.profile = profile;
        v.profile_n = profile != NULL ? strlen(profile) : 0u;
        v.lanes = wt_lanes;
        moqr_cli_log_ready(log, &v);
    }
    serve_log_run_config(log, scfg);
    memset(&v, 0, sizeof(v));
    v.kind = MOQR_CLI_LOG_READY_SIGNALS;
    v.composition = composition;
    moqr_cli_log_ready(log, &v);
    moqr_cli_log_readiness_flush(log);
}

/* One lane's attribution record: the adapter's doorbell counters joined with
 * the same lane's shard counters. The lanes=1 composition has no shard
 * runtime, so its shard-plane fields are TRUE zeros (ss == NULL: structurally
 * inert, not refused). A refused getter emits an explicit REFUSAL record,
 * never zeros. */
void
moqr_cli_serve_log_lane(moqr_cli_log_t *log, uint32_t lane, bool ad_ok,
               const moq_msquic_lane_stats_t *ad, bool sh_ok,
               const moqr_shards_stats_t *ss)
{
    moqr_cli_lane_stats_row_t row;

    if (!ad_ok || !sh_ok) {
        moqr_cli_log_lane_refused(log, lane, !ad_ok ? "adapter" : "shard");
        return;
    }
    memset(&row, 0, sizeof(row));
    row.lane = lane;
    row.wakes_same_lane = ad->wakes_same_lane;
    row.wakes_cross_lane = ad->wakes_cross_lane;
    row.wakes_external = ad->wakes_external;
    row.wakes_coalesced = ad->wakes_coalesced;
    row.pump_sweeps = ad->pump_sweeps;
    row.deadline_sweeps = ad->deadline_sweeps;
    row.idle_cap_wakes = ad->idle_cap_wakes;
    row.wake_to_pump_max_us = ad->wake_to_pump_max_us;
    row.wake_to_pump_total_us = ad->wake_to_pump_total_us;
    row.wake_to_pump_samples = ad->wake_to_pump_samples;
    row.service_passes = ad->service_passes;
    row.flush_sends = ad->flush_sends;
    row.flush_bytes = ad->flush_bytes;
    if (ss != NULL) {
        row.pump_turns = ss->pump_turns;
        row.pump_messages = ss->pump_messages;
        row.pump_bytes = ss->pump_bytes;
        row.wake_requests_push = ss->wake_requests_push;
        row.wake_requests_credit = ss->wake_requests_credit;
        row.wake_requests_local = ss->wake_requests_local;
        row.enq_demand = ss->enqueued[MOQR_SHARDS_MSG_DEMAND];
        row.enq_undemand = ss->enqueued[MOQR_SHARDS_MSG_UNDEMAND];
        row.enq_done = ss->enqueued[MOQR_SHARDS_MSG_DONE];
        row.enq_ack = ss->enqueued[MOQR_SHARDS_MSG_ACK];
        row.enq_obj = ss->enqueued[MOQR_SHARDS_MSG_OBJ];
        row.enq_obj_open = ss->enqueued[MOQR_SHARDS_MSG_OBJ_OPEN];
        row.enq_obj_chunk = ss->enqueued[MOQR_SHARDS_MSG_OBJ_CHUNK];
        row.enq_obj_end = ss->enqueued[MOQR_SHARDS_MSG_OBJ_END];
        row.enq_obj_reset = ss->enqueued[MOQR_SHARDS_MSG_OBJ_RESET];
        row.enq_grp_reset = ss->enqueued[MOQR_SHARDS_MSG_GRP_RESET];
        row.enq_grp_evict = ss->enqueued[MOQR_SHARDS_MSG_GRP_EVICT];
        row.enq_sg_seal = ss->enqueued[MOQR_SHARDS_MSG_SG_SEAL];
        row.channel_entries_hwm = ss->channel_entries_hwm;
        row.channel_bytes_hwm = ss->channel_bytes_hwm;
        row.turns_msg_budget = ss->turns_msg_budget;
        row.turns_byte_budget = ss->turns_byte_budget;
        row.turns_blocked = ss->turns_blocked;
        row.turns_drained = ss->turns_drained;
        row.turns_with_messages = ss->turns_with_messages;
        row.arb_class_refusals = ss->arb_class_refusals;
    }
    moqr_cli_log_lane(log, &row);
}

/* One directed-pair record, or its refusal when the getter refused. */
void
moqr_cli_serve_log_pair(moqr_cli_log_t *log, uint32_t src, uint32_t dst, bool ok,
               const moqr_shards_pair_stats_t *pst)
{
    moqr_cli_pair_stats_row_t pr;

    if (!ok) {
        moqr_cli_log_pair_refused(log, src, dst, "shard");
        return;
    }
    memset(&pr, 0, sizeof(pr));
    pr.src = src;
    pr.dst = dst;
    pr.data_messages = pst->data_messages;
    pr.data_bytes = pst->data_bytes;
    pr.control_messages = pst->control_messages;
    pr.refused_entries = pst->refused_entries;
    pr.refused_bytes = pst->refused_bytes;
    moqr_cli_log_pair(log, &pr);
}

/* The single-facade stop record. */
void
moqr_cli_serve_log_stop_k1(moqr_cli_log_t *log, const moqr_bind_stats_t *bs,
                  const moqr_core_stats_t *cs)
{
    moqr_cli_log_stop_t v;
    memset(&v, 0, sizeof(v));
    v.conns = bs->conns;
    v.tracks = cs->tracks;
    v.ingested = cs->ingested_total;
    v.delivered = cs->delivered_total;
    v.session_errors = bs->session_errors;
    moqr_cli_log_stop(log, &v);
}

/* One shard's stop record: the serve context's own pump-turn and wake
 * counters beside the bind/core totals, with the shard-plane wake requests
 * when its post-join snapshot is readable and an explicit refusal when it is
 * poisoned -- never zeros that look valid. */
void
moqr_cli_serve_log_stop_shard(moqr_cli_log_t *log, uint32_t shard,
                     const moqr_bind_stats_t *bs, const moqr_core_stats_t *cs,
                     uint64_t pump_turns, uint64_t wakes, bool ss_ok,
                     const moqr_shards_stats_t *ss)
{
    moqr_cli_log_stop_t v;
    memset(&v, 0, sizeof(v));
    v.per_shard = true;
    v.shard = shard;
    v.conns = bs->conns;
    v.tracks = cs->tracks;
    v.ingested = cs->ingested_total;
    v.delivered = cs->delivered_total;
    v.session_errors = bs->session_errors;
    v.pump_turns = pump_turns;
    v.wakes = wakes;
    if (ss_ok) {
        v.have_wake_requests = true;
        v.wr_push = ss->wake_requests_push;
        v.wr_credit = ss->wake_requests_credit;
        v.wr_local = ss->wake_requests_local;
    } else {
        v.poisoned = true;
    }
    moqr_cli_log_stop(log, &v);
}

/* The multi-shard total, after every per-shard record. */
void
moqr_cli_serve_log_stop_total(moqr_cli_log_t *log, uint32_t shards, uint32_t conns,
                     uint32_t tracks, uint64_t ingested, uint64_t delivered,
                     uint64_t session_errors)
{
    moqr_cli_log_stop_t v;
    memset(&v, 0, sizeof(v));
    v.total = true;
    v.shards = shards;
    v.conns = conns;
    v.tracks = tracks;
    v.ingested = ingested;
    v.delivered = delivered;
    v.session_errors = session_errors;
    moqr_cli_log_stop(log, &v);
}
