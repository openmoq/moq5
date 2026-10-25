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

#include <moq/relay/relay.h>

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
    uint32_t n_refusals;             /* held-terminal entries per
                                      * connection: 1 + core max_parked    */
} moqr_bind_limits_t;

/* Bind-tier capacity: the allocation-request ceiling of one binding's
 * tables. structure_bytes = the eager create-time tables (including the
 * bind-wide fetch-forwarding transaction pool and each connection's
 * pending fetch-refusal table); announce_bytes = the copied
 * peer namespace bytes ceiling (bounded by the CORE's ns-node pool — a slot
 * only exists once the core accepted the announce — times the shared
 * 4096-byte namespace cap). total_bytes additionally carries three variable
 * ceilings that have no field of their own in this frozen layout:
 * the owned track-key copies on the upstream-subscription slots (one
 * 4096-byte namespace+name cap per slot) and the aggregate
 * fetch_forward_bytes pool, and a separate fixed 1 MiB metadata allocation
 * budget (headers plus complete serialized sets, at most 64 KiB per set).
 * Metadata includes PUBLISH/auth staging, latest track state and old reply
 * snapshots still pinned across backpressure; sharing a local pin does not
 * duplicate its charge. Replacement overlap counts, so sixteen full 64 KiB
 * sets plus headers cannot fit. Eager directory/staging/pin tables are in
 * structure_bytes. So total_bytes >= structure_bytes + announce_bytes, with
 * the difference being exactly those three terms. These private limits do
 * not change the embedded config layout or the Joining FETCH byte budget. */
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

    /* Appended: aggregate transient staging-byte bound shared by forwarded
     * Joining FETCHes (collected before downstream FETCH_OK) and missing-span
     * retrieval for subscription fills, including a pending cached object's
     * owned bytes through commit/close. Includes owned track keys, object
     * payload/properties, upstream Track Properties and error reasons. Fill
     * retrieval owns exact-sized rcbuf copies and charges their allocation
     * headers as well as data; Joining FETCH retains its payload-byte charges.
     * Fixed collector/cursor storage and the bind-owned cached-borrow table
     * (max_fetches slots) are included in capacity_describe's table sizes,
     * not this pool. Permanent latest-set metadata uses its separate
     * fixed budget, never this field. 0 (or an absent prefix field) means
     * MOQR_BIND_DEF_FETCH_FORWARD_BYTES (1 MiB). Shared across the binding,
     * never per connection; exhaustion rejects a Joining FETCH before OK or
     * fails only the affected fill stream. Counts/key limits are unchanged.
     * Values above UINT32_MAX are rejected at create/describe, not truncated. */
    uint64_t                    fetch_forward_bytes;

    /* No field may be appended here: moqr_shards_cfg_t embeds this struct
     * by value, ahead of its own appended fields, so growing it would move
     * every one of those. Later binding settings are setters (the fill wait,
     * moqr_bind_set_fill_timeouts) and, for the sharded runtime, fields
     * appended to moqr_shards_cfg_t. */
} moqr_bind_cfg_t;

#define MOQR_BIND_DEF_FETCH_FORWARD_BYTES (1u << 20)
#define MOQR_BIND_DEF_FILL_TIMEOUT_MS      1000u
#define MOQR_BIND_DEF_FILL_TIMEOUT_MAX_MS  10000u

MOQR_API void moqr_bind_cfg_init_sized(moqr_bind_cfg_t *cfg, size_t cfg_size,
                              const moq_alloc_t *alloc);

MOQR_API moqr_result_t moqr_bind_cfg_resolve(const moqr_bind_cfg_t *cfg,
                                    const moqr_core_limits_t *core_limits,
                                    moqr_bind_limits_t *out);

MOQR_API moqr_result_t moqr_bind_capacity_describe(const moqr_bind_cfg_t *cfg,
                                          const moqr_core_limits_t *core_limits,
                                          moqr_bind_capacity_t *out);

MOQR_API moqr_result_t moqr_bind_create(const moqr_bind_cfg_t *cfg,
                               moqr_bind_t **out);
/* Memory-only destruction: no session/core calls, transport shutdown or
 * callbacks. Quiesce all users first. A stopped adapter may already have
 * released its sessions; remaining binding work is abandoned, not delivered. */
MOQR_API void moqr_bind_destroy(moqr_bind_t *b);

/*
 * Attach a session. The binding opens a core binding for it and starts
 * routing its events on the next pump. The session is NOT owned and must
 * stay valid until moqr_bind_conn_is_open returns false. An explicit close or
 * SESSION_CLOSED event starts detachment, which can remain pending while
 * ordered relay output drains. Duplicate attach is INVAL.
 *
 * Attaching switches the session to deferred subscription-update answers
 * (moq_session_set_defer_subscription_updates): the binding answers every
 * downstream subscriber's update itself, after deciding and applying it, so
 * the caller must not answer those updates. It also leaves FETCH ranges to
 * the relay (moq_session_set_defer_fetch_ranges): a range stated relative to
 * the Largest Object is resolved against the relay's own log, not the
 * session's record of what it carried.
 *
 * `version` is this connection's negotiated MoQ draft. One listener may carry
 * every supported draft at once, and the status/reset registries disagree on
 * several codepoints, so every terminal this binding emits has to be encoded
 * for the peer that will read it. The version is a parameter rather than a later
 * setter so a new attach site cannot forget it and silently emit another
 * draft's numbering.
 */
MOQR_API moqr_result_t moqr_bind_conn_open(moqr_bind_t *b, moq_session_t *session,
                                  moq_version_t version);

/* This connection's negotiated draft, or MOQ_VERSION_DRAFT_16 if unattached. */
MOQR_API moq_version_t moqr_bind_conn_version(const moqr_bind_t *b,
                                     const moq_session_t *session);

/* True if this session is currently attached. */
MOQR_API bool moqr_bind_conn_is_open(const moqr_bind_t *b,
                            const moq_session_t *session);

/* Detach explicitly (e.g. the transport reaped the connection without a
 * session close event having been pumped). Idempotent per session. Returns
 * WOULD_BLOCK while teardown is retained for a later pump; keep the session
 * valid and pump until moqr_bind_conn_is_open returns false. */
MOQR_API moqr_result_t moqr_bind_conn_close(moqr_bind_t *b, moq_session_t *session);

/*
 * One pump: for every attached session, drain its events into the core;
 * tick the core; execute queued intents; pull and write deliveries.
 * Call after the transport has delivered inbound bytes (managed adapters:
 * inside on_pump). now_us is the caller's clock (virtual in tests).
 */
MOQR_API moqr_result_t moqr_bind_pump(moqr_bind_t *b, uint64_t now_us);

/* Whether the LAST moqr_bind_pump exhausted a delivery/discard work budget,
 * or recovered refusal room with an interrupted event drain still owed.
 * The runtime's stepper requests one further coalesced self-wake, so a batch
 * larger than a single pass drains across a FINITE continuation chain
 * instead of stranding behind a doorbell nobody rings — a guard-bounded
 * batch may be the only traffic there is, so nothing else is guaranteed to
 * earn the next pump. Cleared at each pump's start. RETRY/park outcomes
 * deliberately never set it: their leftovers also self-rearm dl_ready, but
 * they arise mid-anomaly during active traffic and are served by the next
 * pump ANY event earns — the ambient pump cadence, not a dedicated signal;
 * that standing assumption predates and is preserved by this flag. */
MOQR_API bool moqr_bind_pump_budget_pending(const moqr_bind_t *b);

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
 * this false, so an applied batch that fully delivered earns no wake. An
 * interrupted event drain gates every delivery retry; its retained ready bit
 * earns a continuation only after refusal room permits polling again. */
MOQR_API bool moqr_bind_pump_continuation_pending(const moqr_bind_t *b);

/* The fill wait: how long a fill fetch stream waits for upstream to deliver
 * an Object inside its range that the retained log does not hold before the
 * relay reports the run as a Timed-Out gap and moves on. A fill that carries
 * FILL_TIMEOUT waits min(requested, max_ms); one without it waits default_ms
 * (the relay's own duration). A requested timeout above the maximum is
 * shortened without telling the subscriber. Zero selects the named default
 * (MOQR_BIND_DEF_FILL_TIMEOUT_MS, MOQR_BIND_DEF_FILL_TIMEOUT_MAX_MS), which is
 * also what a binding uses until this is called. Milliseconds, as on the
 * wire.
 *
 * MOQR_ERR_INVAL for a NULL binding, a default (after defaulting) above the
 * maximum, or a maximum too large to express in microseconds; the binding
 * keeps its previous pair. Call on the binding's own thread, outside a pump
 * callback; the pair applies to fills that start waiting after the call. The
 * sharded runtime applies moqr_shards_cfg_t's fill_timeout_default_ms /
 * fill_timeout_max_ms to every shard's binding with this. */
MOQR_API moqr_result_t moqr_bind_set_fill_timeouts(moqr_bind_t *b,
                                                   uint64_t default_ms,
                                                   uint64_t max_ms);

/* The earliest instant at which a pump would make progress on time alone: the
 * nearest fill-timeout deadline a held fill fetch stream is waiting out, or
 * the fill source's own next deadline when it is earlier, in the caller's
 * clock (the now_us the pumps carry). UINT64_MAX when nothing is waiting on
 * time. A runtime arms its next wake from this so a fill on a quiet
 * track ends at its deadline instead of at the next unrelated event. */
MOQR_API uint64_t moqr_bind_next_deadline_us(const moqr_bind_t *b);

/*
 * Fill source (multi-shard runtime). A fill fetch stream is normally served
 * from the retained log of the shard whose binding holds the subscription.
 * When that shard's log cannot hold part of the fill range -- its feed of the
 * track started after that part was published, because another shard owns
 * the track -- a runtime can serve that part from elsewhere. The binding asks
 * the source first, every time it would open the fill's cursor:
 *
 *   open(ctx, query, claim) -> MOQR_OK: the source serves the range from the
 *     query's start through claim->end_group/end_object (inclusive, never past
 *     the query's end); the binding then reads that part through peek/commit
 *     with claim->handle and serves anything after it from its own log.
 *     MOQR_DONE: the source has nothing to do with this range; the log serves
 *     all of it. MOQR_ERR_WOULD_BLOCK: the source would serve it but cannot
 *     start now; the binding treats the range as not yet held and asks again
 *     on a later pump, until the fill's deadline.
 *   peek(ctx, handle, now_us, item) -> the head item, with the core fetch
 *     cursor's contract (moqr_core_fetch_peek): OBJECT (its record view valid
 *     until commit or close), GAP (Locations not held, through rec.group_id /
 *     rec.object_id; the binding waits for the deadline, then writes them as
 *     one End of Timed-Out Range), UNKNOWN (Locations without evidence
 *     either way, same shape) or MARKER (an evicted prefix), both written at
 *     once as an Unknown range before the deadline and a Timed-Out range
 *     from it, or END (the claimed part is exhausted).
 *     MOQR_ERR_WOULD_BLOCK holds the fill and is asked again on a later pump;
 *     any other error fails the fill on its stream.
 *   commit(ctx, handle) advances past the peeked item.
 *   close(ctx, handle) ends the claim; called exactly once per successful
 *     open, whether or not END was reached. Nothing is read after it.
 *   next_deadline_us(ctx), optional (NULL, or a table whose struct_size
 *     does not reach it, means none): the earliest instant at which the
 *     source has work only time will advance -- a claimed part it will
 *     report Timed-Out at the fill's deadline, or an answer it holds back
 *     until a fill's deadline, for another shard's fill included. The
 *     binding folds it into moqr_bind_next_deadline_us in place of the
 *     deadlines of the fills reading through the source (the source knows
 *     whether the part it serves still waits on time), so the runtime wakes
 *     this binding's shard exactly when the source needs it. UINT64_MAX
 *     when nothing waits on time. Without it, those fills' own deadlines
 *     are reported as for any other fill.
 *
 * All of them run on the binding's own thread, inside moqr_bind_pump (close
 * also inside moqr_bind_conn_close, for the fills of a connection going away;
 * next_deadline_us wherever moqr_bind_next_deadline_us is called), and must
 * not call back into the binding. The source is not owned; it must outlive every
 * fill that claimed through it. Every pointer is required.
 */
typedef struct moqr_bind_fill_query {
    uint32_t           struct_size;
    moqr_track_t       track;            /* this shard's track of the fill */
    uint64_t           track_gen;
    uint64_t           start_group;      /* inclusive                      */
    uint64_t           start_object;
    uint64_t           end_group;        /* inclusive; an end_object of    */
    uint64_t           end_object;       /* UINT64_MAX is the whole group  */
    moqr_group_order_t group_order;
    uint8_t            subscriber_priority;
    /* The instant the fill stops waiting for Objects the relay does not
     * hold (the binding's clock, the now_us its pumps carry). */
    uint64_t           deadline_us;
} moqr_bind_fill_query_t;

typedef struct moqr_bind_fill_claim {
    uint64_t handle;
    uint64_t end_group;                  /* inclusive end of the claimed   */
    uint64_t end_object;                 /* part, as in the query          */
} moqr_bind_fill_claim_t;

typedef struct moqr_bind_fill_source {
    uint32_t       struct_size;
    void          *ctx;
    moqr_result_t (*open)(void *ctx, const moqr_bind_fill_query_t *query,
                          moqr_bind_fill_claim_t *claim);
    moqr_result_t (*peek)(void *ctx, uint64_t handle, uint64_t now_us,
                          moqr_fetch_item_t *out);
    moqr_result_t (*commit)(void *ctx, uint64_t handle);
    void          (*close)(void *ctx, uint64_t handle);
    uint64_t      (*next_deadline_us)(void *ctx);
} moqr_bind_fill_source_t;

/* Install (or with NULL remove) the fill source. The binding copies the
 * table. MOQR_ERR_INVAL for a NULL binding, a table too small to hold
 * open..close, or a missing one of those four; MOQR_ERR_WRONG_STATE while
 * any fill is reading through the current source or a pump is running. */
MOQR_API moqr_result_t moqr_bind_set_fill_source(
    moqr_bind_t *b, const moqr_bind_fill_source_t *source);

/*
 * Resolve a deferred (DEFER) auth ticket with the verifier's `result`
 * (decision + reason + wire error_code). ALLOW resumes the original request
 * (current-state validation + reserve-before-mutate); DENY rejects it with the
 * result's error_code (or UNAUTHORIZED). An unknown/retired/stale ticket, or a
 * connection that closed meanwhile, is a safe no-op. Call on the network thread
 * (managed adapters: inside on_pump), like moqr_bind_pump.
 */
MOQR_API moqr_result_t moqr_bind_auth_resolve(moqr_bind_t *b, uint64_t ticket,
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
MOQR_API uint64_t moqr_latency_bound_us(uint32_t i);

/* Fold one latency observation into the histogram (pure; the test seam that
 * the binding uses on each successful delivery). */
MOQR_API void moqr_latency_observe(moqr_latency_hist_t *h, uint64_t latency_us);

typedef struct moqr_bind_stats {
    uint32_t conns;
    uint64_t events_translated;
    uint64_t deliveries_written;
    uint64_t ingest_refusals;    /* ingest operations the log refused:
                                  * objects or chunks too old, out of
                                  * order or over capacity (one object
                                  * can count more than once)          */
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

MOQR_API void moqr_bind_get_stats(const moqr_bind_t *b, moqr_bind_stats_t *out);
/* Same tail-preserving sized contract as moqr_core_get_stats_sized. */
MOQR_API moqr_result_t moqr_bind_get_stats_sized(
    const moqr_bind_t *bind, moqr_bind_stats_t *out, size_t out_size);

#ifdef __cplusplus
}
#endif

#endif /* MOQR_BIND_H */
