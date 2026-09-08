#ifndef MOQR_OBS_H
#define MOQR_OBS_H

/*
 * Relay observability serializers.
 *
 * Pure renderers over stat snapshots: no I/O, no allocation, no clock. The
 * relay executable (or a test) snapshots core + binding stats on the network
 * thread, then renders here; the surface that actually writes bytes to a
 * socket/file/signal pipe lives in the CLI, never in this layer.
 *
 * Metric cardinality is bounded by construction: labels are closed-
 * vocabulary tokens (shard, transport, draft version, and per-series enum
 * reason/state) — never namespace, track, session, or cursor identity. Per-
 * entity detail lives in the route dump and the trace ring, not in metrics.
 */

#include <moq/relay/relay.h>

#include <moq/relay/moqr_bind.h>
#include <moq/relay/moqr_shards.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Bounded labels stamped on every series. `transport` and `version` are
 * closed-set tokens supplied by the caller (e.g. "picoquic"/"draft-18"); NULL
 * renders as an empty value. */
typedef struct moqr_obs_labels {
    uint16_t    shard;
    const char *transport;
    const char *version;
} moqr_obs_labels_t;

/*
 * Exposition SYNTAX. The renderer emits bytes and nothing else: mapping a
 * format to a media type belongs to whatever surface writes those bytes, not
 * to this layer.
 *
 * A format selects syntax only. It does NOT select which metric families
 * appear -- that is fixed by which entry point is called (see below).
 *
 * PROMETHEUS_004 is the Prometheus text format 0.0.4 syntax.
 * OPENMETRICS_100 differs in three ways the spec requires: a MetricFamily
 * name carries no `_total` suffix (the suffix belongs to the sample, so
 * `# TYPE moqrelay_x counter` with a `moqrelay_x_total{...}` line), a
 * `# UNIT` line accompanies a family whose name ends in a base unit, and
 * the document ends with a terminal `# EOF`.
 */
typedef uint32_t moqr_obs_format_t;

#define MOQR_OBS_FMT_PROMETHEUS_004  0u
#define MOQR_OBS_FMT_OPENMETRICS_100 1u
#define MOQR_OBS_FMT__COUNT          2u

/*
 * Render Prometheus text-format exposition for a snapshot of core (and,
 * when non-NULL, binding) stats into `buf`. Pure and deterministic for a
 * given snapshot. `*written` is the content length (excluding the NUL).
 * Returns MOQR_OK when content + NUL fit; MOQR_ERR_CAPACITY on truncation,
 * with `*written` set to the length the full output needs (grow to
 * *written + 1 and retry).
 */
MOQR_API moqr_result_t moqr_metrics_write_prometheus(const moqr_core_stats_t *core,
                                            const moqr_bind_stats_t *bind,
                                            const moqr_obs_labels_t *labels,
                                            char *buf, size_t cap,
                                            size_t *written);

/*
 * -- Two family sets, and why the APIs are not interchangeable -------------
 *
 * `moqr_metrics_write_prometheus{,_multi}` render the FROZEN family set: the
 * families the accepted build emitted, byte for byte, for consumers already
 * parsing them. They are closed to new families.
 *
 * `moqr_metrics_write_ex` / `moqr_metrics_write_multi_ex` render the EXTENDED
 * (admin-facing) family set in the requested syntax. Because the format
 * selects syntax and not content, `_ex` with MOQR_OBS_FMT_PROMETHEUS_004 is
 * deliberately NOT byte-equal to the legacy writer above: it carries
 * additional families, and a family whose meaning changed when they were
 * added carries corrected help text. That is how a new series reaches a
 * scrape without moving ground under an existing consumer.
 *
 * An unknown format is MOQR_ERR_INVAL.
 */
MOQR_API moqr_result_t moqr_metrics_write_ex(const moqr_core_stats_t *core,
                                    const moqr_bind_stats_t *bind,
                                    const moqr_obs_labels_t *labels,
                                    moqr_obs_format_t fmt, char *buf,
                                    size_t cap, size_t *written);

/*
 * One shard's stat snapshot inside a multi-shard exposition: the core
 * snapshot (required), the binding and cross-shard-plane snapshots
 * (optional), the per-shard labels, and the CLI's own count of actual
 * lane_wake calls (the merged-mask number — distinct from the mask-level
 * wake_requests counters inside `shard`).
 */
typedef struct moqr_snapshot_view {
    const moqr_core_stats_t   *core;    /* required                        */
    const moqr_bind_stats_t   *bind;    /* optional                        */
    const moqr_shards_stats_t *shard;   /* optional (K>1 cross-shard plane) */
    moqr_obs_labels_t          labels;
    uint64_t                   lane_wakes;   /* rendered only with `shard` */
} moqr_snapshot_view_t;

/*
 * Render ONE Prometheus exposition over every shard's snapshot: HELP/TYPE
 * exactly once per metric, per-shard series under the existing names and
 * shard/transport/version labels, then process aggregates under separate
 * moqrelay_process_* names (counters and gauges sum, high-water marks take
 * the max, histogram buckets/count/sum sum). route_epoch and journal_epoch
 * are per-shard identities and are never aggregated.
 *
 * Internal-entity exclusion: when a view carries shard stats, the
 * user-facing gauges subtract like from like — subscriptions{state} minus
 * pump subscriptions of the SAME state, bindings minus the manager's
 * binding slots, namespace subscriptions minus the manager watcher — and
 * the internals surface separately (moqrelay_internal_bindings,
 * moqrelay_pump_subscriptions{state}). Any subtraction that would go
 * negative is a broken invariant: the whole exposition is suppressed
 * (MOQR_ERR_INVAL, nothing written) rather than floored or invented.
 *
 * Same truncation contract as the single-snapshot writer.
 */
MOQR_API moqr_result_t moqr_metrics_write_prometheus_multi(
    const moqr_snapshot_view_t *views, uint32_t count, char *buf, size_t cap,
    size_t *written);

/* The multi-shard exposition in the EXTENDED family set (see above), in the
 * requested syntax. An unknown format is MOQR_ERR_INVAL. */
MOQR_API moqr_result_t moqr_metrics_write_multi_ex(const moqr_snapshot_view_t *views,
                                          uint32_t count,
                                          moqr_obs_format_t fmt, char *buf,
                                          size_t cap, size_t *written);

/*
 * Worst-case exposition length, in bytes, for `lanes` views in `fmt` --
 * excluding the terminating NUL, so a buffer of bound + 1 can never
 * truncate. Pure: a function of the shape of the document, never of any
 * snapshot's values, so a caller can size a fixed buffer once at startup
 * and reuse it for every render.
 *
 * This sizes the EXTENDED (`_ex`) document. The frozen legacy document is
 * smaller, so the bound remains safe for it but is no longer exact.
 *
 * `with_bind` and `with_shard` say whether any view may carry binding and
 * cross-shard-plane stats; passing true when unsure is always safe, since
 * the bound only grows. Saturating arithmetic throughout: an overflow
 * poisons to UINT64_MAX rather than wrapping, and is reported as
 * MOQR_ERR_CAPACITY so the caller refuses instead of under-allocating.
 *
 * MOQR_ERR_INVAL on a NULL out, an unknown format, or lanes outside
 * [1, MOQR_SHARDS_MAX].
 */
MOQR_API moqr_result_t moqr_metrics_bound(uint32_t lanes, moqr_obs_format_t fmt,
                                 bool with_bind, bool with_shard,
                                 uint64_t *out_bytes);

#ifdef __cplusplus
}
#endif

#endif /* MOQR_OBS_H */
