/*
 * CLI-private per-lane snapshot rows: storage, lifecycle, the byte
 * descriptor the capacity model counts, and the publish/collect protocol
 * the multi-lane metrics dump rides.
 *
 * Threading contract: a row is written only by its OWNING lane, inside its
 * pump, under that row's dump-only mutex (`mu` is never on the data path).
 * The coordinator copies rows under those same mutexes and never touches
 * any core, bind, journal, route, or trace belonging to a live lane — the
 * row is the only thing that crosses. Collection targets the NEWEST
 * requested epoch: an aggregate renders only when every copied row carries
 * exactly that epoch, and a copy made obsolete by a newer request between
 * copy and render is discarded, never rendered.
 */
#ifndef MOQR_CLI_SNAPSHOT_H
#define MOQR_CLI_SNAPSHOT_H

#include <moq/relay/relay.h>

#include <moq/relay/moqr_bind.h>
#ifdef MOQR_BIND_TESTING
#include <moq/relay/moqr_bind_test.h>
#endif
#include <moq/relay/moqr_obs.h>
#include <moq/relay/moqr_shards.h>

#include <pthread.h>
#include <stddef.h>
#include <stdint.h>

/*
 * A lane's attestation about its cross-shard plane. Three outcomes, not two,
 * because "no shard runtime exists" and "the shard runtime refused" are
 * different facts and only one of them is an error:
 *
 *   REFUSED  the lane should have supplied shard stats and could not. The
 *            epoch is poisoned; nothing renders.
 *   ABSENT   there is no shard runtime (the single-lane composition). The
 *            shard-plane families are OMITTED. They are not rendered from a
 *            zeroed struct, because `pump_turns 0` would be a fabricated
 *            measurement rather than a missing one.
 *   VALID    the shard runtime supplied stats.
 *
 * REFUSED is deliberately the ZERO value: a row that was never populated, or
 * only partly populated, then poisons its epoch instead of quietly presenting
 * as a legitimate ABSENT composition. Any value outside this enum is rejected
 * rather than defaulted.
 */
typedef uint32_t moqr_cli_cap_t;

#define MOQR_CLI_CAP_REFUSED 0u
#define MOQR_CLI_CAP_ABSENT  1u
#define MOQR_CLI_CAP_VALID   2u
#define MOQR_CLI_CAP__COUNT  3u

/* What one lane publishes: its shard's core/bind/cross-shard snapshots
 * plus the CLI's own merged-mask lane-wake counter. */
typedef struct moqr_cli_snapshot_stats {
    moqr_core_stats_t   core;
    moqr_bind_stats_t   bind;
    moqr_shards_stats_t shard;
    moqr_cli_cap_t      shard_cap;
    uint64_t            lane_wakes;   /* actual lane_wake calls issued */
#ifdef MOQR_BIND_TESTING
    /* Verify build only (moq-relay-verify): the lane's LIVE blocked-reason
     * aggregate for this epoch. blocked_valid is the lane's attestation that
     * moqr_bind_debug_blocked_aggregate succeeded; a false makes the
     * coordinator emit an explicit refusal row for the lane. The plain
     * moq5-relay build never compiles these fields, so its snapshot struct —
     * and the cli_runtime capacity term — are byte-identical to before. */
    moqr_bind_blocked_agg_t blocked;
    bool                    blocked_valid;
#endif
} moqr_cli_snapshot_stats_t;

typedef struct moqr_cli_snapshot_row {
    pthread_mutex_t           mu;     /* dump-only; never on the data path */
    uint64_t                  epoch;  /* last published epoch; 0 = never   */
    moqr_cli_snapshot_stats_t st;
} moqr_cli_snapshot_row_t;

typedef struct moqr_cli_snapshot {
    uint32_t                 lanes;
    moqr_cli_snapshot_row_t *rows;   /* [lanes], counted allocator          */
    const moq_alloc_t       *alloc;  /* borrowed                            */
} moqr_cli_snapshot_t;

/* The exact bytes moqr_cli_snapshot_init requests for `lanes` rows — the
 * capacity model's cli_runtime term. Pure. */
size_t moqr_cli_snapshot_bytes(uint32_t lanes);

/* lanes must be 1..MOQR_SHARDS_MAX (the shard runtime's own cap, which the
 * coordinator's fixed-size view assembly relies on); outside it, INVAL. */
moqr_result_t moqr_cli_snapshot_init(moqr_cli_snapshot_t *s, uint32_t lanes,
                                     const moq_alloc_t *alloc);
void moqr_cli_snapshot_destroy(moqr_cli_snapshot_t *s);

/* Publish lane `lane`'s freshly built stats at `epoch`, under that row's
 * mutex. Owning-lane only; a bad lane index is a no-op. */
void moqr_cli_snapshot_publish(moqr_cli_snapshot_t *s, uint32_t lane,
                               const moqr_cli_snapshot_stats_t *st,
                               uint64_t epoch);

/* The coordinator's source of "what is the newest requested epoch": read
 * both before the copy (to pick the target) and again after it (to catch a
 * request that landed between copy and render). */
typedef uint64_t (*moqr_cli_epoch_fn)(void *ctx);

/*
 * Copy every row (under its mutex) targeting the NEWEST requested epoch.
 * On MOQR_OK, `rows[lanes]` holds one coherent all-`*out_epoch` set — safe
 * to render. MOQR_ERR_WOULD_BLOCK means the newest epoch is not complete
 * yet ("epoch incomplete"; `*out_epoch` names it) — the caller retries
 * later and renders nothing. If a NEWER epoch is requested between the
 * copy and the post-copy re-read, the copied set is DISCARDED and the copy
 * retries against the newer epoch — an all-E aggregate is never handed out
 * once E+1 exists. MOQR_ERR_INVAL on bad arguments.
 */
moqr_result_t moqr_cli_snapshot_collect(moqr_cli_snapshot_t *s,
                                        moqr_cli_epoch_fn requested,
                                        void *rctx,
                                        moqr_cli_snapshot_stats_t *rows,
                                        uint64_t *out_epoch);

/* Serialize one coherent row set into caller-owned storage — produce only,
 * NEVER emit; the emission decision belongs to moqr_cli_snapshot_render's
 * final epoch re-read below. */
typedef moqr_result_t (*moqr_cli_produce_fn)(
    void *ctx, const moqr_cli_snapshot_stats_t *rows, uint64_t epoch);

/*
 * The full coordinator cycle: collect a coherent newest-epoch set, call
 * `produce` to SERIALIZE it (into ctx-owned storage), then re-read the
 * requested epoch one final time — if a newer epoch was requested at any
 * point up to here, the produced output is DISCARDED and the cycle retries
 * against the newer epoch. Only MOQR_OK licenses the caller to emit what
 * `produce` built: no newer epoch existed when production finished, which
 * shrinks the request-vs-emission window to the emission call itself.
 * MOQR_ERR_WOULD_BLOCK: the newest epoch is incomplete (or kept advancing)
 * — emit nothing, retry later. A complete set containing a row whose
 * shard_cap is REFUSED (or an unknown value) is POISONED: MOQR_ERR_INVAL
 * without calling `produce` (the epoch completed but must be suppressed,
 * never rendered from zeroed stand-ins). A row whose shard_cap is ABSENT is
 * rendered with its shard-plane families omitted. Any non-OK from `produce` (e.g. the renderer's
 * suppression) returns verbatim, with *out_epoch naming the produced
 * epoch.
 */
moqr_result_t moqr_cli_snapshot_render(moqr_cli_snapshot_t *s,
                                       moqr_cli_epoch_fn requested,
                                       void *rctx,
                                       moqr_cli_snapshot_stats_t *rows,
                                       moqr_cli_produce_fn produce,
                                       void *pctx, uint64_t *out_epoch);

/* Deliver one licensed, fully-serialized exposition document (`doc`,
 * NUL-terminated, `len` content bytes) for `epoch` — production writes it
 * to stderr; tests capture it. */
typedef void (*moqr_cli_emit_fn)(void *ctx, const char *doc, size_t len,
                                 uint64_t epoch);

/*
 * THE production coordinator flow, whole: one attempt at the outstanding
 * metrics epoch. Runs the collect → produce → final-re-read cycle with the
 * multi-snapshot Prometheus renderer as the producer (views are built over
 * the COPIED rows and the per-shard `labels[snap->lanes]`), and calls
 * `emit` exactly once, only when the cycle licensed emission. By
 * construction this coordinator can touch nothing but the snapshot rows:
 * it is not handed the shard runtime at all, so route/journal/core/bind
 * traversal is impossible without changing this signature. Returns the
 * cycle's result verbatim (WOULD_BLOCK = epoch incomplete/superseded,
 * INVAL = poisoned row or renderer suppression — nothing emitted), with
 * *out_epoch as moqr_cli_snapshot_render reports it. Transient buffers
 * come from the snapshot's allocator and are released before returning.
 */
moqr_result_t moqr_cli_coord_dump(moqr_cli_snapshot_t *snap,
                                  const moqr_obs_labels_t *labels,
                                  moqr_cli_epoch_fn requested, void *rctx,
                                  moqr_cli_emit_fn emit, void *ectx,
                                  uint64_t *out_epoch);

#endif /* MOQR_CLI_SNAPSHOT_H */
