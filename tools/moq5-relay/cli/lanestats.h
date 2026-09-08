#ifndef MOQR_CLI_LANESTATS_H
#define MOQR_CLI_LANESTATS_H

#include <moq/relay/types.h>

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/*
 * Per-lane attribution row: the managed-MsQuic adapter's doorbell counters
 * joined with the same lane's shard-plane counters, emitted once per lane
 * after the facade has stopped (post-join, coordinator thread). One stable
 * machine-readable line per lane under a versioned prefix; a getter refusal
 * emits an explicit REFUSAL line — never zeros that look like a valid row.
 *
 * The formatter/parser is pure over this plain struct (no adapter types), so
 * the row grammar is testable in trees that do not build MsQuic. Average
 * wake-to-pump latency is wake_to_pump_total_us / wake_to_pump_samples;
 * zero samples means UNAVAILABLE, never zero.
 */

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* V2 appended the shard turn-outcome fields; V3 appends the third wake
 * cause (the step-local continuation). The summarizer parses V1 and V2
 * (for historical logs) and V3, but never pools rows of different
 * versions. */
#define MOQR_LANE_STATS_PREFIX "RELAY_LANE_STATS_V3"
#define MOQR_PAIR_STATS_PREFIX "RELAY_PAIR_STATS_V1"
#define MOQR_RUN_CONFIG_PREFIX "RELAY_RUN_CONFIG_V1"

/* One lane's joined counters (adapter doorbell + shard plane). */
typedef struct moqr_cli_lane_stats_row {
    uint32_t lane;
    /* adapter (moq_msquic_lane_get_stats) */
    uint64_t wakes_same_lane;
    uint64_t wakes_cross_lane;
    uint64_t wakes_external;
    uint64_t wakes_coalesced;
    uint64_t pump_sweeps;
    uint64_t deadline_sweeps;
    uint64_t idle_cap_wakes;
    uint64_t wake_to_pump_max_us;
    uint64_t wake_to_pump_total_us;
    uint64_t wake_to_pump_samples;
    uint64_t service_passes;
    uint64_t flush_sends;
    uint64_t flush_bytes;
    /* shard plane (moqr_shards_get_stats), same lane index */
    uint64_t pump_turns;
    uint64_t pump_messages;
    uint64_t pump_bytes;
    uint64_t wake_requests_push;
    uint64_t wake_requests_credit;
    uint64_t enq_demand, enq_undemand, enq_done, enq_ack;
    uint64_t enq_obj, enq_obj_open, enq_obj_chunk, enq_obj_end;
    uint64_t enq_obj_reset, enq_grp_reset, enq_grp_evict, enq_sg_seal;
    uint64_t channel_entries_hwm;
    uint64_t channel_bytes_hwm;
    /* V2 appends: the turn-outcome partition + arbiter refusals */
    uint64_t turns_msg_budget;
    uint64_t turns_byte_budget;
    uint64_t turns_blocked;
    uint64_t turns_drained;
    uint64_t turns_with_messages;
    uint64_t arb_class_refusals;
    /* V3 appends: the local-continuation wake cause — self-wakes a lane
     * requests after applying inbound data or ending a delivery pass on
     * the budget guard; a distinct cause, never folded into push/credit */
    uint64_t wake_requests_local;
} moqr_cli_lane_stats_row_t;

/* One directed-pair row (src -> dst demand channel), emitted once per
 * non-self ordered pair after stop. A valid record holds EXACTLY
 * K*(K-1) rows; anything else fails closed. */
typedef struct moqr_cli_pair_stats_row {
    uint32_t src, dst;
    uint64_t data_messages;
    uint64_t data_bytes;
    uint64_t control_messages;
    uint64_t refused_entries;
    uint64_t refused_bytes;
} moqr_cli_pair_stats_row_t;

/* The resolved operating point, emitted ONCE at serve start: the four
 * budget/capacity values AS RESOLVED (the resolver's output, never the
 * request), so no two runs at different operating points can pool. */
typedef struct moqr_cli_run_config_row {
    uint64_t pump_turn_messages;
    uint64_t pump_turn_bytes;
    uint64_t demand_channel_entries;
    uint64_t demand_channel_bytes;
} moqr_cli_run_config_row_t;

/* Serialize one row (no trailing newline). Returns the snprintf would-be
 * length so the caller detects truncation (>= n means the row did not fit
 * and must not be published). */
int moqr_cli_lane_stats_format(char *buf, size_t n,
                               const moqr_cli_lane_stats_row_t *row);

/* Serialize a REFUSAL record for a lane whose adapter or shard getter did
 * not return OK ("adapter" | "shard"). Same truncation contract. */
int moqr_cli_lane_stats_format_refused(char *buf, size_t n, uint32_t lane,
                                       const char *which);

typedef enum {
    MOQR_LANE_STATS_ROW = 0,       /* parsed into *row                    */
    MOQR_LANE_STATS_REFUSED = 1,   /* an explicit refusal record          */
    MOQR_LANE_STATS_MALFORMED = 2, /* not a valid V1 line                 */
} moqr_cli_lane_stats_kind_t;

/* Strict single-line parse: the versioned prefix, every field exactly once
 * in the emission order, u64 values, nothing trailing. */
moqr_cli_lane_stats_kind_t moqr_cli_lane_stats_parse(
    const char *line, moqr_cli_lane_stats_row_t *row, uint32_t *refused_lane);

/* Validate one rep's RECORD: exactly `expected_lanes` well-formed rows,
 * lane indices 0..expected_lanes-1 each appearing exactly once and in
 * ascending order (deterministic), no refusal and no malformed line.
 * Returns true and fills rows[0..expected_lanes-1] on success. */
bool moqr_cli_lane_stats_record(const char *const *lines, size_t nlines,
                                uint32_t expected_lanes,
                                moqr_cli_lane_stats_row_t *rows);

/* Average wake-to-pump latency. False when samples == 0: the average is
 * UNAVAILABLE (never reported as zero). */
bool moqr_cli_lane_stats_avg_us(const moqr_cli_lane_stats_row_t *row,
                                double *out_avg_us);

/* Pair rows: same snprintf would-be-length + strict-grammar contracts as
 * the lane rows (every key exactly once in order, bounded u64, ,eor=1). */
int moqr_cli_pair_stats_format(char *buf, size_t n,
                               const moqr_cli_pair_stats_row_t *row);
moqr_cli_lane_stats_kind_t moqr_cli_pair_stats_parse(
    const char *line, moqr_cli_pair_stats_row_t *row);

/* Validate one rep's PAIR record: exactly shards*(shards-1) well-formed
 * rows covering every non-self ordered (src,dst) exactly once, both
 * indices < shards, in the deterministic emission order (src-major,
 * ascending, self skipped). Missing, duplicate, self, out-of-range, or
 * malformed rows fail closed. rows[] must hold shards*(shards-1). */
bool moqr_cli_pair_stats_record(const char *const *lines, size_t nlines,
                                uint32_t shards,
                                moqr_cli_pair_stats_row_t *rows);

/* The run-config record (one per relay log, emitted at serve start). */
int moqr_cli_run_config_format(char *buf, size_t n,
                               const moqr_cli_run_config_row_t *row);

/*
 * The SAME three records as JSON events, driven by the same key tables and
 * getters as the text formatters (so a key exists in both outputs or in
 * neither). `elapsed_valid`/`elapsed_us` are supplied by the caller and
 * rendered as an unsigned integer, or `null` when not valid; no clock is
 * read here. Body length excluding the terminating NUL in *out_len on
 * MOQR_OK; MOQR_ERR_CAPACITY when the body does not fit `cap` (with room
 * for the NUL); MOQR_ERR_INVAL for a NULL argument or a refusal reason that
 * is not one of the closed vocabulary. Nothing usable is left on refusal.
 */
moqr_result_t moqr_cli_lane_stats_json(char *buf, size_t cap,
                                       bool elapsed_valid, uint64_t elapsed_us,
                                       const moqr_cli_lane_stats_row_t *row,
                                       size_t *out_len);
moqr_result_t moqr_cli_lane_stats_refused_json(char *buf, size_t cap,
                                               bool elapsed_valid,
                                               uint64_t elapsed_us,
                                               uint32_t lane,
                                               const char *which,
                                               size_t *out_len);
moqr_result_t moqr_cli_pair_stats_json(char *buf, size_t cap,
                                       bool elapsed_valid, uint64_t elapsed_us,
                                       const moqr_cli_pair_stats_row_t *row,
                                       size_t *out_len);
moqr_result_t moqr_cli_pair_stats_refused_json(char *buf, size_t cap,
                                               bool elapsed_valid,
                                               uint64_t elapsed_us,
                                               uint32_t src, uint32_t dst,
                                               const char *which,
                                               size_t *out_len);
moqr_result_t moqr_cli_run_config_json(char *buf, size_t cap,
                                       bool elapsed_valid, uint64_t elapsed_us,
                                       const moqr_cli_run_config_row_t *row,
                                       size_t *out_len);
moqr_result_t moqr_cli_run_config_refused_json(char *buf, size_t cap,
                                               bool elapsed_valid,
                                               uint64_t elapsed_us,
                                               const char *which,
                                               size_t *out_len);
/* The number of numeric fields each record carries (39 / 5 / 4). */
size_t moqr_cli_lane_stats_field_count(void);
size_t moqr_cli_pair_stats_field_count(void);
size_t moqr_cli_run_config_field_count(void);
moqr_cli_lane_stats_kind_t moqr_cli_run_config_parse(
    const char *line, moqr_cli_run_config_row_t *row);

#ifdef __cplusplus
}
#endif

#endif /* MOQR_CLI_LANESTATS_H */
