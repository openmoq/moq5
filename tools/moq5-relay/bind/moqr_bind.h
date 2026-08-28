#ifndef MOQR_BIND_H
#define MOQR_BIND_H

/*
 * The session binding: connects real moq_session_t instances to the relay
 * core. Relay-side session events translate into core inputs; core intents
 * and pulled deliveries translate back into public session calls.
 *
 * Transport-agnostic on purpose: the binding sees only moq_session_t
 * pointers. The same binding runs over SimPair sessions in the parity
 * tests and over managed-adapter sessions in the relay executable — the
 * parity suite therefore proves the production code path, not a copy.
 *
 * Threading: none of its own. Every call must happen where the sessions
 * live (the adapter network thread inside on_pump, or the test's driver
 * loop). One binding per shard, single writer, like everything else.
 *
 * Semantics preserved from the proven harness: reserve-before-mutate is
 * the core's contract (WOULD_BLOCK leaves state untouched; the binding
 * retries next pump); delivery uses the pull model (a WOULD_BLOCK write
 * leaves the cursor in place); refused downstream requests are answered
 * on the wire (reject/error) rather than dropped.
 */

#include <moqrelay/relay.h>

#include <moq/session.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct moqr_bind moqr_bind_t;

/*
 * Intent router (multi-shard runtime). When set, the binding hands any output
 * intent whose binding_cookie is >= router_cookie_base to this callback instead
 * of routing it to a session — those cookies belong to the shard manager's
 * cross-shard pseudo-bindings (MOQR_SHARD_COOKIE_BASE), not real connections.
 *   return true  => the router consumed the intent (delivered it to a mailbox,
 *                   or failed it closed). Borrowed-view kinds (UPSTREAM_SUBSCRIBE,
 *                   NS_FOUND, NS_GONE) MUST be consumed in-call — their ns/name
 *                   spans borrow core-interned storage valid only until the next
 *                   advancing core call.
 *   return false => could not take it now; retry next pump. Legal ONLY for
 *                   scalar-safe (deferrable) kinds, which the binding parks in
 *                   its pending ring exactly like a WOULD_BLOCKed session write.
 * Without a router (the single-shard default), reserved-cookie intents are moot
 * exactly as an unknown cookie is today — behaviour is unchanged.
 */
typedef bool (*moqr_bind_intent_router_fn)(void *ctx, const moqr_intent_t *it,
                                           uint64_t now_us);

/* Default per-connection open-subgroup table size, shared with the shard
 * runtime's per-demand subgroup-progress default. */
#define MOQR_BIND_DEF_OPEN_SUBGROUPS 64u

/* The pure, prefix-safe resolution moqr_bind_create applies to a config
 * against a RESOLVED set of core limits (moqr_core_limits_resolve output —
 * never a live core, so create and describe share one source). */
typedef struct moqr_bind_limits {
    uint32_t max_conns;
    uint32_t n_dsubs;
    uint32_t n_sgs;
    uint32_t n_usubs;
    uint32_t n_pubs;
    uint32_t n_anns;
    uint32_t n_fetches;              /* follows core max_fetches           */
    uint32_t pending_cap;            /* derived deferral ring              */
    uint32_t nsu_cap;                /* derived retained namespace ring    */
    uint32_t request_grant_window;
} moqr_bind_limits_t;

/* Bind-tier capacity: the allocation-request ceiling of one binding's
 * tables. structure_bytes = the eager create-time tables; announce_bytes =
 * the copied peer namespace bytes ceiling (bounded by the CORE's ns-node
 * pool — a slot only exists once the core accepted the announce — times the
 * shared 4096-byte namespace cap). */
typedef struct moqr_bind_capacity {
    uint64_t structure_bytes;
    uint64_t announce_bytes;
    uint64_t total_bytes;
} moqr_bind_capacity_t;

typedef struct moqr_bind_cfg {
    uint32_t           struct_size;
    const moq_alloc_t *alloc;      /* required                            */
    moqr_core_t       *core;       /* required; NOT owned                 */
    uint32_t           max_conns;  /* 0 = default 64                      */
    /* Per-connection map capacities (0 = core limits / defaults).
     * max_open_subgroups defaults to MOQR_BIND_DEF_OPEN_SUBGROUPS; the shard
     * runtime derives its per-demand progress capacity from the same value,
     * so the default lives here, named, instead of being mirrored. */
    uint32_t           max_downstream_subs;
    uint32_t           max_open_subgroups;
    uint32_t           max_upstream_subs;
    uint32_t           max_publishes;
    uint32_t           max_announces;
    /* Draft-16 request-capacity (MAX_REQUEST_ID) auto-grant window, in request-
     * ID units (0 = default). As a peer consumes its granted request-id credit,
     * the binding raises the ceiling by this window before the credit runs out,
     * so a long-lived connection never stalls sending requests. Draft-18 has no
     * MAX_REQUEST_ID (request IDs are stream-flow-controlled), so this is inert
     * there. */
    uint32_t           request_grant_window;

    /* Intent router (multi-shard runtime; NULL = single-shard, no routing).
     * Intents whose binding_cookie >= router_cookie_base go to `router` instead
     * of a session. Set router_cookie_base to MOQR_SHARD_COOKIE_BASE when a
     * router is installed so real connection cookies (1..max_conns) still route
     * to sessions. */
    uint64_t                    router_cookie_base;
    moqr_bind_intent_router_fn  router;
    void                       *router_ctx;
} moqr_bind_cfg_t;

void moqr_bind_cfg_init_sized(moqr_bind_cfg_t *cfg, size_t cfg_size,
                              const moq_alloc_t *alloc);

moqr_result_t moqr_bind_cfg_resolve(const moqr_bind_cfg_t *cfg,
                                    const moqr_core_limits_t *core_limits,
                                    moqr_bind_limits_t *out);

moqr_result_t moqr_bind_capacity_describe(const moqr_bind_cfg_t *cfg,
                                          const moqr_core_limits_t *core_limits,
                                          moqr_bind_capacity_t *out);

moqr_result_t moqr_bind_create(const moqr_bind_cfg_t *cfg,
                               moqr_bind_t **out);
void moqr_bind_destroy(moqr_bind_t *b);

/*
 * Attach a session. The binding opens a core binding for it and starts
 * routing its events on the next pump. The session is NOT owned and must
 * stay valid until moqr_bind_conn_close or a SESSION_CLOSED event (the
 * binding detaches itself on either). Duplicate attach is INVAL.
 *
 * `version` is this connection's negotiated MoQ draft. One listener may carry
 * both drafts at once, and the status/reset registries disagree on several
 * codepoints, so every terminal this binding emits has to be encoded for the
 * peer that will read it. The version is a parameter rather than a later
 * setter so a new attach site cannot forget it and silently emit another
 * draft's numbering.
 */
moqr_result_t moqr_bind_conn_open(moqr_bind_t *b, moq_session_t *session,
                                  moq_version_t version);

/* This connection's negotiated draft, or MOQ_VERSION_DRAFT_16 if unattached. */
moq_version_t moqr_bind_conn_version(const moqr_bind_t *b,
                                     const moq_session_t *session);

/* True if this session is currently attached. */
bool moqr_bind_conn_is_open(const moqr_bind_t *b,
                            const moq_session_t *session);

/* Detach explicitly (e.g. the transport reaped the connection without a
 * session close event having been pumped). Idempotent per session. */
moqr_result_t moqr_bind_conn_close(moqr_bind_t *b, moq_session_t *session);

/*
 * One pump: for every attached session, drain its events into the core;
 * tick the core; execute queued intents; pull and write deliveries.
 * Call after the transport has delivered inbound bytes (managed adapters:
 * inside on_pump). now_us is the caller's clock (virtual in tests).
 */
moqr_result_t moqr_bind_pump(moqr_bind_t *b, uint64_t now_us);

/* Whether the LAST moqr_bind_pump ended with any connection's delivery pass
 * expired on the per-pass delivery guard with work remaining. The runtime's
 * stepper reads this to request one further coalesced self-wake, so a batch
 * larger than a single pass drains across a FINITE continuation chain
 * instead of stranding behind a doorbell nobody rings — a guard-bounded
 * batch may be the only traffic there is, so nothing else is guaranteed to
 * earn the next pump. Cleared at each pump's start. RETRY/park outcomes
 * deliberately never set it: their leftovers also self-rearm dl_ready, but
 * they arise mid-anomaly during active traffic and are served by the next
 * pump ANY event earns — the ambient pump cadence, not a dedicated signal;
 * that standing assumption predates and is preserved by this flag. */
bool moqr_bind_pump_budget_pending(const moqr_bind_t *b);

/* Whether the LAST moqr_bind_pump left work a self-continuation should carry
 * forward, REASON-AWARE: any connection still in the ready set (a mark that
 * landed during the pass and re-set its bit), OR a connection parked on
 * SESSION_SG (its one-per-pump re-attempt needs a pump and has no external edge
 * to earn one). A connection parked on ACTION_CAP is EXCLUDED — it waits for a
 * downstream transport-capacity edge (the managed adapter arms the lane pump
 * from a transport-event batch, a cause distinct from a shard push/credit
 * wake), so a continuation would only spin. The runtime's stepper uses this to gate
 * the applied-inbound self-continuation on ACTUAL forward-progressable work: a
 * pass that drained everything (or left only ACTION_CAP backpressure) leaves
 * this false, so an applied batch that fully delivered earns no wake. */
bool moqr_bind_pump_continuation_pending(const moqr_bind_t *b);

/*
 * Resolve a deferred (DEFER) auth ticket with the verifier's `result`
 * (decision + reason + wire error_code). ALLOW resumes the original request
 * (current-state validation + reserve-before-mutate); DENY rejects it with the
 * result's error_code (or UNAUTHORIZED). An unknown/retired/stale ticket, or a
 * connection that closed meanwhile, is a safe no-op. Call on the network thread
 * (managed adapters: inside on_pump), like moqr_bind_pump.
 */
moqr_result_t moqr_bind_auth_resolve(moqr_bind_t *b, uint64_t ticket,
                                     const moqr_auth_verdict_t *result,
                                     uint64_t now_us);

/*
 * Fixed-bucket ingest→delivery latency histogram. Buckets are compile-time
 * (no dynamic config) and measured in the caller's clock (virtual in sim, so
 * the distribution is deterministic under a seed). Only objects successfully
 * written downstream are observed; WOULD_BLOCK and STREAM_ERROR are not.
 * bucket[i] is the NON-cumulative count of observations whose latency falls
 * in (bound[i-1], bound[i]]; bucket[MOQR_LATENCY_BUCKETS] is the +Inf
 * overflow. The exporter cumulates for the Prometheus `_bucket{le}` shape.
 */
#define MOQR_LATENCY_BUCKETS 12u

typedef struct moqr_latency_hist {
    uint64_t bucket[MOQR_LATENCY_BUCKETS + 1u];   /* [last] = +Inf */
    uint64_t sum_us;
    uint64_t count;
} moqr_latency_hist_t;

/* Upper bound, in microseconds, of finite bucket i (0..MOQR_LATENCY_BUCKETS-1).
 * i out of range returns 0. */
uint64_t moqr_latency_bound_us(uint32_t i);

/* Fold one latency observation into the histogram (pure; the test seam that
 * the binding uses on each successful delivery). */
void moqr_latency_observe(moqr_latency_hist_t *h, uint64_t latency_us);

typedef struct moqr_bind_stats {
    uint32_t conns;
    uint64_t events_translated;
    uint64_t deliveries_written;
    uint64_t ingest_refusals;    /* TOO_OLD / ordering INVALs at the log */
    uint64_t session_errors;     /* unexpected session-call failures     */
    moqr_latency_hist_t forward_latency;   /* ingest→delivery, us          */
    uint32_t pending_high_water; /* max deferred-output ring depth seen   */
    uint32_t pending_nonscalar_blocked; /* borrowed kind hit defer path (BUG:
                                         * must stay 0 — see relay.h:102)  */
    uint32_t nsu_high_water;     /* max retained namespace-update depth    */
    uint32_t nsu_live;           /* sidecar slots holding an occupied payload
                                  * (a valid zero-byte suffix IS occupied)  */
    uint64_t nsu_retained;       /* namespace updates held for retry       */
    uint64_t nsu_failed_closed;  /* a NAMESPACE update could not be honoured
                                  * (hard send error, or its payload could
                                  * not be retained): conn closed          */
    uint64_t ordered_failed_closed; /* a SCALAR ordered-ring insertion failed
                                     * for a conn: closed. A namespace that
                                     * cannot be queued counts in
                                     * nsu_failed_closed instead — the two
                                     * domains never overlap.               */
} moqr_bind_stats_t;

void moqr_bind_get_stats(const moqr_bind_t *b, moqr_bind_stats_t *out);

#ifdef __cplusplus
}
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

#endif /* MOQR_BIND_H */
