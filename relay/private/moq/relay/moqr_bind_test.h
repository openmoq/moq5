#ifndef MOQR_BIND_TEST_H
#define MOQR_BIND_TEST_H
#include <moq/relay/moqr_bind.h>
#ifdef __cplusplus
extern "C" {
#endif
#ifdef MOQR_BIND_TESTING
/* Test-only probe gauge (exists ONLY in the test-internals build): total
 * moqr_core_next_delivery calls issued by the delivery half. */
uint64_t moqr_bind_debug_delivery_probes(void);
void moqr_bind_debug_dl_state(const moqr_bind_t *b, uint32_t slot,
                              bool *ready, bool *parked, uint8_t *reason);
void moqr_bind_debug_fail_confirm(int nth);
void moqr_bind_debug_fail_probe(int nth, moqr_result_t result);
/* Force the Nth next SUB_DONE begun-subgroup reset to report a hard failure
 * WITHOUT calling the session, so the production close -> detach -> purge
 * path runs from inside an ordered-ring retry. */
void moqr_bind_debug_fail_sg_reset(int nth);
/* Observe the instant an internal detach is retained for retry. */
typedef void (*moqr_bind_debug_detach_pending_fn)(moqr_bind_t *b,
                                                  moq_session_t *session,
                                                  void *ctx);
void moqr_bind_debug_on_detach_pending(moqr_bind_debug_detach_pending_fn fn,
                                       void *ctx);
/* Make the next N downstream SUB_DONE terminal writes observe WOULD_BLOCK
 * without reaching the session, so the terminal is genuinely unsent and the
 * production defer/retry arm must carry the retained descriptor to the wire. */
void moqr_bind_debug_block_sub_done(int n);
int moqr_bind_debug_sub_done_blocks_left(void);
/* The first live upstream subscription: {conn slot, session handle}. */
bool moqr_bind_debug_first_usub(const moqr_bind_t *b, uint32_t *slot,
                                uint64_t *handle_raw);
/* Drive the production upstream-termination dispatch directly, for the two
 * cause/state combinations draft-18 makes unreachable over the wire.
 * `redirect` selects REDIRECT over GOAWAY. */
void moqr_bind_debug_upstream_terminated(moqr_bind_t *b, uint32_t slot,
                                         uint32_t family, uint64_t handle_raw,
                                         int redirect, uint64_t now_us);
/* Force the Nth next retained-namespace store to fail, exactly as a byte
 * allocation failure or an exhausted slot would, so the production
 * fail-closed path runs. */
void moqr_bind_debug_fail_nsu_store(int nth);
/* Make the Nth next ordered-ring insertion of the given kind observe a FULL
 * ring. This drives the SAME predicate production uses, so the injected and
 * genuinely-full cases cannot diverge; it does not bypass the fail-closed arm.
 * is_nsu selects namespace insertions, else scalar. */
void moqr_bind_debug_ring_full(int nth, bool is_nsu);
/* Drive the production ann_store validator directly; true == accepted. */
bool moqr_bind_debug_ann_store_probe(moqr_bind_t *b, uint32_t slot,
                                     uint64_t raw, moqr_ns_t ns);
uint64_t moqr_bind_debug_sg_attempts(void);
void moqr_bind_debug_conn_blocked_counts(const moqr_bind_t *b,
                                         uint32_t slot, uint64_t out[3]);

/* BIND_SG track attribution for one conn slot: the TRACK whose delivery hit
 * this conn's subgroup-pool refusal (the FIRST refusal latches it; returns
 * false with zeroed outs when none recorded). NOT the delivery's sub_cookie —
 * that is the downstream subscription handle, a different id space from the
 * demand. Demands are keyed by track, so the shard layer resolves the track
 * to the demand id (moqr_shards_debug_track_demand); *ambiguous = a later
 * refusal named a DIFFERENT track (a different demand) or the sub handle was
 * unresolvable — the join must then fail closed. Cleared on slot claim/reuse.
 * Same synchronization contract as blocked_counts. */
bool moqr_bind_debug_conn_bind_sg_track(const moqr_bind_t *b, uint32_t slot,
                                        uint64_t *track_raw,
                                        uint64_t *track_gen, bool *ambiguous);
uint32_t moqr_bind_debug_sg_park_count(const moqr_bind_t *b);

/* Owning-lane blocked-reason aggregate over LIVE (used && !closed) conns
 * only — a detached slot keeps its counters until reuse, so external
 * per-slot scanning could count a dead connection; this accessor excludes
 * them internally. parked_* are the CURRENT parked populations by reason
 * (the exceptional set), distinct from the historical *_total/conns_*. */
typedef struct moqr_bind_blocked_agg {
    uint32_t live_conns;
    uint32_t conns_action_cap;
    uint32_t conns_session_sg;
    uint32_t conns_bind_sg;
    uint64_t action_cap_total;
    uint64_t session_sg_total;
    uint64_t bind_sg_total;
    uint32_t parked_action_cap;
    uint32_t parked_session_sg;
} moqr_bind_blocked_agg_t;

/* Fail-closed LIVE aggregate. Rejects NULL b/out with MOQR_ERR_INVAL and
 * zeroes *out; sums the three u64 totals with checked adds and returns
 * MOQR_ERR_INTERNAL (again zeroing *out) if any would overflow, so the lane
 * emits a refusal row rather than a wrapped total. Callers MUST NOT infer
 * liveness by scanning per-slot counters — this accessor is the only correct
 * source (a detached slot keeps its counters until the slot is reused). */
moqr_result_t moqr_bind_debug_blocked_aggregate(const moqr_bind_t *b,
                                                moqr_bind_blocked_agg_t *out);

/* Test-only: overwrite one LIVE slot's three blockage counters, to drive the
 * aggregate overflow path and lifecycle assertions. false for NULL/out-of-
 * range/non-live slot. */
bool moqr_bind_debug_seed_blocked(moqr_bind_t *b, uint32_t slot,
                                  const uint64_t counts[3]);

/* Test-only: how many downstream subgroup slots conn `slot` currently holds
 * open (the bind-pool occupancy the BIND_SG class refuses against). */
uint32_t moqr_bind_debug_conn_open_sgs(const moqr_bind_t *b, uint32_t slot);

/* Test-only: the RESOLVED conn-slot capacity (diagnostic sweeps iterate
 * exactly this range, never a hard-coded bound). */
uint32_t moqr_bind_debug_max_conns(const moqr_bind_t *b);

/* Test-only: number of retry helpers that hit WOULD_BLOCK while the bind-owned
 * intent scratch was already active and therefore had to defer to the caller's
 * normal retry path instead of draining recursively. */
uint64_t moqr_bind_debug_intent_retry_suppressed(const moqr_bind_t *b);
#endif


#ifdef __cplusplus
}
#endif
#endif
