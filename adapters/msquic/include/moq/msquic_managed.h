#ifndef MOQ_MSQUIC_MANAGED_H
#define MOQ_MSQUIC_MANAGED_H

/*
 * Stability: pre-1.0, transport-specific adapter API. May change
 * before 1.0.
 *
 * moq_msquic_managed: owns the MsQuic lifecycle (API table,
 * registration, configuration, listener/connection, MoQ session +
 * attach adapter) behind a small C facade. Parallel in spirit to
 * moq_pq_threaded_t and moq_mvfst_managed_t — with one structural
 * difference: MsQuic owns its worker threads, so there is no facade
 * network thread. Transport events run on MsQuic's serialized
 * per-connection worker; the facade adds a mutex so app-driven pumps
 * and those workers exclude each other, plus a small doorbell thread
 * whose only jobs are wake() delivery and MoQ session deadlines — it
 * never dispatches transport events.
 *
 * Threading model — LANE OWNERSHIP (app-visible contract): the facade
 * exposes `lane_count` lanes, each an independent lock domain with its
 * own doorbell. Every accepted connection is assigned to exactly one
 * lane at accept (cfg.choose_lane, or round-robin) and stays on that
 * lane for its whole life. A lane's `on_lane_pump` is the EXCLUSIVE
 * access window for every session assigned to that lane: moq_session_*
 * calls are legal only inside the pump of the lane that owns the
 * connection. A lane pump owns and services MANY connections in one
 * pass (iterate them with moq_msquic_lane_next_conn) — it is not one
 * callback per connection, so one application object (e.g. a relay
 * shard's binding) can drive every session in the lane under a single
 * exclusive window. Pumps for DIFFERENT lanes may run concurrently;
 * MsQuic worker callbacks feed+service their connection under that
 * connection's lane lock, so distinct lanes never contend. A worker
 * batch ARMS the lane's pump; the lane's doorbell runs on_lane_pump
 * once (coalesced), and again after lane_wake() or a session deadline.
 *
 * Applications must not share non-thread-safe session or rcbuf
 * ownership across lanes; an object that crosses lanes must go through
 * an explicit clone or a mailbox boundary (moq_rcbuf refcounts are
 * non-atomic — single-lane confinement is a hard requirement).
 *
 * Callback rules:
 *   choose_lane:  server accept placement; returns the lane index that
 *                 owns a new connection. Runs with NO adapter locks;
 *                 must not touch sessions. Optional (NULL = round-
 *                 robin). An out-of-range return REFUSES the connection.
 *   on_lane_pump: may call moq_session_* on the lane's connections, the
 *                 facade accessors, and lane_wake(). Return 0 to
 *                 continue, nonzero for clean termination.
 *   on_activity:  signal-only (wake a blocked thread); must NOT call
 *                 mutation APIs. May be NULL.
 *   From inside on_lane_pump/on_activity, stop() and wait() are refused
 *   with MOQ_ERR_WRONG_STATE. The terminal accessors and lane_wake()
 *   detect the callback context and are safe there.
 *
 * Client mode: create() opens the transport and starts the connect;
 * the MoQ session exists from create() (single ALPN, so the version is
 * known eagerly) and starts at the transport CONNECTED event.
 *
 * Server mode: create() starts the listener (port 0 = ephemeral;
 * moq_msquic_managed_port() reports the bound port). Each accepted
 * client connection gets its own MoQ session + adapter, created
 * lazily, up to cfg.max_connections (default 1) across all lanes;
 * connections beyond the cap — or after drain/stop — are refused at the
 * listener. Each accepted connection is placed on a lane and, inside
 * that lane's on_lane_pump, the application iterates the lane's
 * connections via moq_msquic_lane_next_conn() and drives their
 * sessions. A connection whose transport reached its terminal stays
 * visible to its lane's pump for the batch that delivers its final
 * events (poll SESSION_CLOSED there), and is then reclaimed.
 *
 * Compatibility view (CLIENT ONLY): the facade-level accessors
 * (_session, is_fatal/is_closed/codes) describe the single client
 * connection. On a SERVER they are NOT a per-connection correctness
 * API — a server observes terminal by polling SESSION_CLOSED inside the
 * lane pump. The pending probes aggregate across all live connections
 * (zero at quiescence) and are valid for both.
 *
 * Version: exact-version. The facade offers exactly one MoQ ALPN,
 * chosen by cfg.version (default draft-16 / "moqt-16"), and creates
 * the session at that version eagerly. A client/server version
 * mismatch therefore fails ALPN negotiation at the handshake: the
 * client observes a bounded fatal terminal with no MoQ setup, and the
 * server never accepts the connection. Deferred multi-version offers
 * (a real ALPN list) are a later addition.
 *
 * Teardown: stop() refuses new accepts, shuts every live connection
 * down, waits for each transport to quiesce (all pending sends and
 * datagrams complete or cancel first — the drain probes end at zero),
 * and only then closes the MsQuic handles — never while holding a lane
 * mutex (MsQuic's blocking closes drain callbacks that need it). Peers
 * of still-active connections observe an orderly close.
 * destroy() after stop() frees everything. moq_msquic_managed_drain()
 * is the graceful half: refuse new accepts while existing connections
 * run to their own conclusion (watch conn_count + the pending probes
 * for quiescence).
 */

#include <stddef.h>

#include <moq/msquic.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct moq_msquic_managed moq_msquic_managed_t;
/* A lane: an independent lock domain owning a set of connections. */
typedef struct moq_msquic_managed_lane moq_msquic_managed_lane_t;
/* An accepted connection (server) or the single client connection. */
typedef struct moq_msquic_managed_conn moq_msquic_managed_conn_t;

/* Information available at server accept, for lane placement. Sparse
 * for now (only struct_size); peer address / negotiated ALPN / version
 * land later behind struct_size without breaking choose_lane. */
typedef struct moq_msquic_accept_info {
    uint32_t struct_size;
} moq_msquic_accept_info_t;

/* Server accept placement: return the lane index (0..lane_count-1) that
 * owns a newly accepted connection. Runs with NO adapter locks; must
 * not touch sessions. Optional — NULL uses round-robin. An out-of-range
 * return REFUSES the connection (no silent clamp). */
typedef uint32_t (*moq_msquic_choose_lane_fn)(
    moq_msquic_managed_t *m, const moq_msquic_accept_info_t *info,
    void *user);

/* A lane's exclusive access window. Iterate the lane's connections with
 * moq_msquic_lane_next_conn and drive their sessions here. Return 0 to
 * continue, nonzero for clean termination of the whole facade. */
typedef int (*moq_msquic_lane_pump_fn)(moq_msquic_managed_t *m,
                                       moq_msquic_managed_lane_t *lane,
                                       uint64_t now_us, void *user);

typedef void (*moq_msquic_activity_fn)(moq_msquic_managed_t *m,
                                       void *ctx);

typedef struct moq_msquic_managed_cfg {
    uint32_t struct_size;
    const moq_alloc_t *alloc;      /* required; must be thread-safe */
    moq_perspective_t perspective; /* required: CLIENT or SERVER */
    const char *host;              /* client: remote + SNI (required);
                                      server: bind address
                                      (default 0.0.0.0) */
    uint16_t port;                 /* client: remote (required);
                                      server: listen, 0 = ephemeral */
    const char *cert_path;         /* server: required */
    const char *key_path;          /* server: required */
    bool insecure_skip_verify;     /* client: skip cert validation
                                      (loopback/self-signed) */
    uint32_t idle_timeout_ms;      /* 0 = transport default */
    moq_msquic_lane_pump_fn on_lane_pump; /* required */
    void *on_lane_pump_user;
    moq_msquic_activity_fn on_activity;   /* optional, signal-only */
    void *on_activity_ctx;
    /* session tuning */
    bool send_request_capacity;
    uint32_t initial_request_capacity; /* 0 = session default */
    uint32_t max_events;               /* 0 = session default */
    uint32_t max_actions;              /* 0 = session default */
    /* appended (enabled by struct_size): the MoQ version this endpoint
     * offers and speaks — exactly one ALPN. 0 = MOQ_VERSION_DRAFT_16.
     * An unrecognized version fails create() with
     * MOQ_ERR_UNSUPPORTED. */
    moq_version_t version;
    /* appended: server connection cap (facade-wide, across all lanes).
     * 0 = 1 (the pre-multi-conn behavior). Ignored for clients. */
    uint32_t max_connections;
    /* appended: lane count. 0 = 1. Each lane is an independent lock
     * domain + doorbell; accepted connections are partitioned across
     * lanes by choose_lane. Clients always use exactly one lane
     * (server-only knob). */
    uint32_t lane_count;
    moq_msquic_choose_lane_fn choose_lane; /* optional; NULL = round-robin */
    void *choose_lane_user;
    /* appended: a RECEIVE-SIDE session rendering knob, forwarded verbatim to
     * moq_session_cfg_t.streaming_objects. It does NOT change transport framing —
     * it only selects how this endpoint surfaces received objects to its consumer.
     * false (default, and for prefix-sized callers): each received object is one
     * whole-object OBJECT_RECEIVED event. true: received objects are surfaced as
     * OBJECT_CHUNK slices (begin/data/end) for incremental ingest. */
    bool streaming_objects;
    /* appended: MoQ SESSION idle timeout, forwarded verbatim to
     * moq_session_cfg_t.idle_timeout_us (0 = disabled — the default, and
     * what prefix-sized callers get). Distinct from idle_timeout_ms above,
     * which is the QUIC transport's idle setting: this one arms a SESSION
     * deadline (visible to moq_session_next_deadline_us), so the lane
     * doorbell's deadline-driven service path becomes reachable and idle
     * sessions close at the MoQ layer. */
    uint64_t session_idle_timeout_us;

    /* appended: optional application service-deadline query — ONE ABI block,
     * read only when struct_size covers THROUGH app_deadline_ctx (the callback
     * and its context are honored together or not at all). Returns the app's
     * earliest pending time-based deadline in this facade's clock domain
     * (CLOCK_MONOTONIC µs), or UINT64_MAX for none. Each lane doorbell folds it
     * with the min session deadline before waiting, so a purely time-based
     * deadline (e.g. a periodic catalog refresh) wakes an otherwise-idle lane.
     * MUST be pure/non-blocking and MUST NOT re-enter the facade. NULL = none.
     * The fold happens only while computing a lane's next wait, so when
     * application state changes the value this query would return, the owner
     * MUST issue a facade/lane wake (moq_msquic_managed_wake /
     * moq_msquic_lane_wake) so the next loop recomputes the wait; an
     * idle lane will not observe the new deadline on its own. Set via
     * moq_msquic_managed_cfg_init_sized. */
    uint64_t (*app_deadline_us)(void *ctx);
    void *app_deadline_ctx;
} moq_msquic_managed_cfg_t;

MOQ_API void moq_msquic_managed_cfg_init_sized(
    moq_msquic_managed_cfg_t *cfg, size_t size);

MOQ_API moq_result_t moq_msquic_managed_create(
    const moq_msquic_managed_cfg_t *cfg, moq_msquic_managed_t **out);

/* Quiesce the transport and stop every lane doorbell. Idempotent.
 * Refused with MOQ_ERR_WRONG_STATE from inside on_lane_pump/on_activity. */
MOQ_API moq_result_t moq_msquic_managed_stop(moq_msquic_managed_t *m);

/* After stop(). */
MOQ_API void moq_msquic_managed_destroy(moq_msquic_managed_t *m);

/* CLIENT ONLY: the single client connection's session, valid to use
 * ONLY inside on_lane_pump. NULL before connect, and ALWAYS NULL on a
 * server (servers use moq_msquic_lane_next_conn — this is not a server
 * correctness path). */
MOQ_API moq_session_t *moq_msquic_managed_session(
    moq_msquic_managed_t *m);

/* -- Lanes ---------------------------------------------------------- */

/* The number of lanes the facade exposes (>= 1). */
MOQ_API uint32_t moq_msquic_managed_lane_count(
    const moq_msquic_managed_t *m);

/* The lane at `index` (0..lane_count-1), or NULL if out of range.
 * Handles are stable for the facade's lifetime. */
MOQ_API moq_msquic_managed_lane_t *moq_msquic_managed_lane(
    moq_msquic_managed_t *m, uint32_t index);

/* A lane's index. */
MOQ_API uint32_t moq_msquic_lane_index(
    const moq_msquic_managed_lane_t *lane);

/* Iterate the connections OWNED BY THIS LANE: prev = NULL starts,
 * returns NULL past the end. ONLY valid inside THIS lane's on_lane_pump
 * (returns NULL elsewhere, or from a different lane's pump); returned
 * handles are valid for the current pump only. */
MOQ_API moq_msquic_managed_conn_t *moq_msquic_lane_next_conn(
    moq_msquic_managed_lane_t *lane, moq_msquic_managed_conn_t *prev);

/* Ask the adapter to pump this lane (schedules on_lane_pump on the
 * lane's doorbell). Does not grant session access outside the callback.
 * Safe from any thread, including inside a lane pump. */
MOQ_API moq_result_t moq_msquic_lane_wake(
    moq_msquic_managed_lane_t *lane);

/* Per-lane doorbell statistics: monotonic counters over the lane's
 * wake -> pump -> service -> flush path since create. Pure observation —
 * collecting them changes no scheduling or send behavior.
 *
 * Append discipline: set struct_size before calling; the getter fills at
 * most the declared prefix and zeroes the caller's remainder when the
 * caller declares MORE than this library knows (unknown appended fields
 * read zero). A prefix-sized caller never observes appended fields.
 *
 * A "pending wake cycle" opens at the first accepted explicit
 * moq_msquic_lane_wake since the last consuming pump (that call records
 * the monotonic arm timestamp) and closes when a pump sweep consumes it
 * (exactly one latency sample per cycle). Re-arming an already-open
 * cycle counts wakes_coalesced and adds no sample. Average wake-to-pump
 * latency is wake_to_pump_total_us / wake_to_pump_samples — NEVER
 * divided by pump_sweeps, which includes deadline/idle sweeps that
 * consumed no armed wake. */
typedef struct moq_msquic_lane_stats {
    uint32_t struct_size;          /* stamped by the call: bytes written  */

    /* accepted moq_msquic_lane_wake calls, by caller context */
    uint64_t wakes_same_lane;      /* from this lane's own callback      */
    uint64_t wakes_cross_lane;     /* from a DIFFERENT lane's callback   */
    uint64_t wakes_external;       /* from an app/coordinator thread     */
    uint64_t wakes_coalesced;      /* found an open pending cycle        */

    /* doorbell activity */
    uint64_t pump_sweeps;          /* on_lane_pump sweep entries (all
                                    * causes)                            */
    uint64_t deadline_sweeps;      /* sweeps caused by a due session
                                    * deadline (no explicit wake pending) */
    uint64_t idle_cap_wakes;       /* idle-cap wait expiries with no work */

    /* explicit-wake service latency (see the cycle rule above) */
    uint64_t wake_to_pump_max_us;
    uint64_t wake_to_pump_total_us;
    uint64_t wake_to_pump_samples;

    /* downstream work */
    uint64_t service_passes;       /* per-connection service invocations */
    uint64_t flush_sends;          /* ACCEPTED StreamSend batches (this
                                    * lane's connections, lifetime totals
                                    * — reclaimed connections included;
                                    * staging attempts and synchronous
                                    * send failures are never counted)   */
    uint64_t flush_bytes;          /* bytes in those accepted batches    */
} moq_msquic_lane_stats_t;

/* Frozen v0 layout floor: the smallest out_size moq_msquic_lane_get_stats
 * accepts. Any later fields are appended past this; the floor never moves,
 * so an old caller passing the v0 size keeps working against a newer lib. */
#define MOQ_MSQUIC_LANE_STATS_V0_SIZE \
    (offsetof(moq_msquic_lane_stats_t, flush_bytes) + sizeof(uint64_t))

/* Snapshot one lane's stats into `*out` (sized-copy by `out_size`,
 * stamping out->struct_size with the bytes written). Caller-context rule
 * (mirrors moq_msquic_lane_wake's arms):
 *   - an external/coordinator thread: snapshots under the lane lock;
 *   - THIS lane's own callback (on_lane_pump / a transport-event guard
 *     on this lane): reads directly — the lock is already held;
 *   - a DIFFERENT lane's callback on this thread: REFUSED with
 *     MOQ_ERR_WRONG_STATE (blocking on another lane's lock from a
 *     callback risks an A->B/B->A deadlock; read from your own lane or
 *     an external thread instead).
 * Returns MOQ_OK, or MOQ_ERR_INVAL for a NULL lane/out or an out_size
 * below MOQ_MSQUIC_LANE_STATS_V0_SIZE. Counters are monotonic from
 * create; take deltas by subtraction. Valid until destroy(). */
MOQ_API moq_result_t moq_msquic_lane_get_stats(
    moq_msquic_managed_lane_t *lane, moq_msquic_lane_stats_t *out,
    size_t out_size);

/* -- Connections ---------------------------------------------------- */

MOQ_API moq_session_t *moq_msquic_managed_conn_session(
    moq_msquic_managed_conn_t *conn);

/* The lane that owns this connection (stable for the connection's
 * whole life). */
MOQ_API moq_msquic_managed_lane_t *moq_msquic_managed_conn_lane(
    moq_msquic_managed_conn_t *conn);

/* Opaque per-connection application slot (the facade never reads it).
 * Session or connection POINTER VALUES are not stable identities
 * across a connection's lifetime and its successor's — a reclaimed
 * child's memory may be reused — so per-connection app state belongs
 * here, not in address-keyed maps. */
MOQ_API void moq_msquic_managed_conn_set_user(
    moq_msquic_managed_conn_t *conn, void *user);
MOQ_API void *moq_msquic_managed_conn_user(
    const moq_msquic_managed_conn_t *conn);

/* Shut one connection's transport down (async, orderly; code is the
 * application close code). The connection's final events arrive on a
 * later pump. */
MOQ_API void moq_msquic_managed_conn_close(
    moq_msquic_managed_conn_t *conn, uint64_t code);

/* Live accepted connections (safe from any thread). */
MOQ_API size_t moq_msquic_managed_conn_count(
    const moq_msquic_managed_t *m);

/* Graceful drain: refuse new connections; existing ones continue until
 * they close on their own (watch conn_count and the pending probes).
 * Safe from any thread, idempotent. */
MOQ_API void moq_msquic_managed_drain(moq_msquic_managed_t *m);

/* Server: the bound listen port (after create). */
MOQ_API uint16_t moq_msquic_managed_port(const moq_msquic_managed_t *m);

/* Coalesced cross-thread signaling: wake() schedules a pump on EVERY
 * lane (use moq_msquic_lane_wake to target one lane); wait() blocks
 * until activity (MOQ_OK), timeout (MOQ_DONE; 0 = poll, UINT64_MAX =
 * indefinite) or a terminal state (MOQ_ERR_CLOSED). */
MOQ_API moq_result_t moq_msquic_managed_wake(moq_msquic_managed_t *m);
MOQ_API moq_result_t moq_msquic_managed_wait(moq_msquic_managed_t *m,
                                             uint64_t timeout_us);

/* Outbound drain probes (safe from any thread, including inside
 * on_lane_pump/on_activity): accepted stream sends not yet completed and
 * datagram sends not yet finalized. Both are zero once the transport
 * reached its terminal. */
MOQ_API int moq_msquic_managed_pending_sends(
    const moq_msquic_managed_t *m);
MOQ_API int moq_msquic_managed_pending_datagrams(
    const moq_msquic_managed_t *m);

/* Terminal observation — CLIENT convenience (the single connection).
 * On a SERVER these are NOT a per-connection correctness API (a server
 * observes terminal via SESSION_CLOSED in the lane pump). Safe from any
 * thread, including inside on_lane_pump/on_activity. Fatal wins over
 * closed when both latch. */
MOQ_API bool moq_msquic_managed_is_fatal(const moq_msquic_managed_t *m);
MOQ_API uint64_t moq_msquic_managed_fatal_code(
    const moq_msquic_managed_t *m);
MOQ_API bool moq_msquic_managed_is_closed(const moq_msquic_managed_t *m);
MOQ_API uint64_t moq_msquic_managed_close_code(
    const moq_msquic_managed_t *m);

/* The MoQ version this facade speaks. Exact-version: the single offered
 * ALPN fixes it at create(), so this is the negotiated version from that
 * point (0 only for a NULL facade). Parity with the other managed
 * adapters' negotiated_version accessor. */
MOQ_API moq_version_t moq_msquic_managed_negotiated_version(
    const moq_msquic_managed_t *m);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_MSQUIC_MANAGED_H */
