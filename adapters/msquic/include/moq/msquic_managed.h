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
 * sessions.
 *
 * Terminal children (SERVER): a connection whose transport reached its
 * terminal stays visible to its lane's pump — across as many pumps as the
 * application needs — until the application both polls its SESSION_CLOSED
 * and acknowledges it with moq_msquic_managed_conn_ack_terminal(). Only
 * then may it be reclaimed. A BOUNDED pump may therefore return without
 * draining that event and still find the same child, its session and its
 * `user` state on a later pump. Until it acknowledges, the child keeps
 * consuming one of cfg.max_connections; see that function for the full
 * contract and for what forced teardown does.
 *
 * Compatibility view (CLIENT ONLY): the facade-level accessors
 * (_session, is_fatal/is_closed/codes) describe the single client
 * connection. On a SERVER they are NOT a per-connection correctness
 * API — a server observes terminal by polling SESSION_CLOSED inside the
 * lane pump. The pending probes aggregate across all live connections
 * (zero at quiescence) and are valid for both. The client connection is
 * likewise not acknowledged: the facade owns its lifetime, and
 * moq_msquic_managed_session() is pump-scoped and simply reads NULL once
 * that connection is gone.
 *
 * Version: clients are exact-version and offer exactly one MoQ ALPN,
 * chosen by cfg.version (default draft-16 / "moqt-16"). Servers may
 * either offer that same exact single ALPN, or append a version list in
 * cfg.versions/cfg.version_count and accept several draft ALPNs on one
 * listener. The server list is ordered by preference, most preferred first;
 * the selected version is the first mutually supported entry in that server
 * list. A multi-version server creates each child session at the version
 * MsQuic reports for that accepted connection.
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

#define MOQ_MSQUIC_MANAGED_MAX_VERSIONS 8u

/* Information available at server accept, for lane placement. Sparse
 * for now (only struct_size). The negotiated MoQ version is available
 * later from moq_msquic_managed_conn_negotiated_version() once the
 * accepted connection is visible in its lane pump. */
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

    /* appended: outgoing subgroup pool per
     * session, forwarded verbatim to moq_session_cfg_t.max_open_subgroups
     * (0 = session default -- and what prefix-sized callers get). A small value
     * makes the SESSION's write pool the sender-side blocker independent of any
     * consumer table. Honored only when struct_size covers the field ENTIRELY;
     * a size landing inside it reads as 0, never as its low bytes.
     *
     * Contract: 0 selects the session default; legal explicit values are
     * 1..0xffff. A FULL-SIZED config carrying a larger value makes
     * moq_msquic_managed_create return MOQ_ERR_INVAL before any allocation or
     * transport setup -- the core's own limit, reported at the facade rather
     * than surfacing later as an internal or transport error. */
    uint32_t max_open_subgroups;
    /* appended LAST: SERVER-only list of MoQ versions to offer as ALPNs on one
     * listener, ordered by server preference (most preferred first). The
     * selected version is the first mutually supported entry in this server
     * list. The list is one ABI block: honored only when struct_size covers
     * both pointer and count entirely; prefix/partial callers behave as absent.
     *
     * When version_count == 0, the exact-version cfg.version field above remains
     * authoritative (0 = draft-16). When version_count > 0:
     *   - perspective must be SERVER;
     *   - cfg.version must be 0 (no exact-version/list conflict);
     *   - versions must point at 1..MOQ_MSQUIC_MANAGED_MAX_VERSIONS entries;
     *   - entries must be known MoQ versions and must not duplicate.
     *
     * create() deep-copies the list and ALPN buffer array before starting
     * transport; the caller may free or mutate the source array after create().
     * A multi-version SERVER returns 0 from the facade-level
     * moq_msquic_managed_negotiated_version(); use
     * moq_msquic_managed_conn_negotiated_version() inside the lane pump for an
     * accepted connection's immutable selected draft. */
    const moq_version_t *versions;
    size_t version_count;
} moq_msquic_managed_cfg_t;

MOQ_API void moq_msquic_managed_cfg_init_sized(
    moq_msquic_managed_cfg_t *cfg, size_t size);

/* Process-global MTU note (SERVER perspective): creating a managed SERVER
 * lowers MsQuic's PROCESS-GLOBAL minimum MTU floor to a 1280-byte IP path MTU
 * (a one-time SetParam on QUIC_PARAM_GLOBAL_SETTINGS). This is required because
 * a server's first reply to a bare Initial is sized on the pre-configuration
 * CONNECTION path: the new connection copies MsQuic's library-global settings
 * and seeds its path MTU before a configuration is attached, so that first reply
 * is sized from the process-global default (1288) -- the listener configuration
 * alone does NOT govern it.
 *
 * There is no registration-scoped settings parameter, so the floor is
 * necessarily process-wide. LibMoQ performs NO destroy-time restore: the floor
 * remains in force while MsQuic stays initialized and another open API table or
 * registration may depend on it. (MsQuic itself owns global-settings lifetime --
 * it loads them at the first MsQuicOpen2 reference and may reset them on a later
 * MsQuicOpen2 after the last MsQuicClose; the next managed-server create simply
 * reapplies the floor. LibMoQ adds no refcount and no restore.)
 *
 * A managed CLIENT create does NOT issue this write, but a client created in the
 * same process AFTER a managed server inherits the conservative 1280 initial
 * floor; the client's own configuration still declares no MTU override and no
 * process-global MAXIMUM is imposed, so DPLPMTUD can still grow the path. If the
 * global write is rejected, the SERVER create fails closed (MOQ_ERR_INTERNAL,
 * *out left NULL). */
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
#ifdef MOQ_MSQUIC_TESTING
    /* Test-only future-field discriminator. It makes sizeof(current) larger
     * than the frozen v0 floor in white-box builds, so a regression that
     * validates against sizeof(current) is observable before a real public
     * field is appended. Never part of the production ABI. */
    uint64_t test_appended;

    /* Diagnostic-only, appended AFTER the frozen v0 prefix so no existing
     * field offset moves and MOQ_MSQUIC_LANE_STATS_V0_SIZE stays put.
     *
     * `deadline_sweeps` counts a deadline that FIRED. The no-work path first
     * queries every present connection, and each query walks that session's
     * capacity tables; nothing counted that scan. These do:
     *   deadline_queries  one per connection per no-work scan
     *   deadline_class_*  that query's outcome, partitioning the queries
     * Classification is relative to ONE scan-entry instant sampled before the
     * loop, not to a moving `now`: none = no deadline at all, future = later
     * than that instant, due = at or before it. Cumulative; never read by the
     * product.
     *
     * These are plain uint64_t and wrap modulo 2^64; readers take differences
     * with unsigned modular subtraction, which stays correct across a wrap.
     * The scan path deliberately carries no saturation branch. */
    uint64_t deadline_queries;
    uint64_t deadline_class_none;
    uint64_t deadline_class_future;
    uint64_t deadline_class_due;
#endif
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

/* SERVER: tell the facade this terminal child may be reclaimed.
 *
 * WHAT IT ASSERTS. Terminal processing for this connection is complete: the
 * application no longer needs the child to remain iterable, and it will not use
 * the borrowed conn/session handles after the acknowledging callback returns.
 * Independently owned application state and retained moq_rcbuf_t are
 * unaffected, and nothing reference-counted is released by the call. The
 * assertion is not mechanically verifiable — the library records that it was
 * made.
 *
 * WHY IT EXISTS. A lane pump is free to be BOUNDED: to service part of its work
 * and return before draining every session. Without an acknowledgment the
 * facade could only guess, from a pump having had the OPPORTUNITY to poll, that
 * the terminal was consumed — and reclaim a child whose session a bounded
 * consumer was still going to read.
 *
 * WHEN IT MAY BE CALLED. Only from the owning lane's on_lane_pump, and only for
 * a connection that callback was presented. MOQ_EVENT_SESSION_CLOSED must
 * ALREADY have been transferred to the application by a poll of this
 * connection's session: a queued-but-unpolled terminal is not enough, and
 * neither is a completed transport shutdown. Because observation is a durable
 * session fact rather than a queue read, the acknowledging callback need NOT be
 * the one that polled — a later pump is fine.
 *
 * RESULTS.
 *   MOQ_OK               accepted; also returned for a duplicate call while the
 *                        handle is still valid in the current callback.
 *   MOQ_ERR_INVAL        conn == NULL.
 *   MOQ_ERR_WRONG_STATE  the single client connection (it is not reclaimed
 *                        per-child); outside the owning lane's pump; or the
 *                        terminal has not been observed yet.
 *
 * HANDLE LIFETIME. The handle is valid only for the current callback. After a
 * successful acknowledgment the connection may be reclaimed as soon as that
 * callback returns — do not retain the pointer. A stale or released handle is
 * invalid input: no rejection is promised and none is attempted.
 *
 * IF IT IS NEVER CALLED. A terminal server child stays linked, iterable and
 * counted against cfg.max_connections; it is never reclaimed because time
 * passed, so once the reserve is exhausted new accepts are refused at the
 * listener. Every accepted child has a MoQ session from the moment it exists
 * (it is created before the transport is wired), so there is no
 * nothing-to-acknowledge class here. Forced facade teardown (stop()/destroy())
 * still reclaims without acknowledgment, behind the documented callback/pump
 * exclusion barrier; that is the forced path, not the normal one. */
MOQ_API moq_result_t moq_msquic_managed_conn_ack_terminal(
    moq_msquic_managed_conn_t *conn);

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
 * Safe from any thread, idempotent. A terminal SERVER child still counts
 * until the application acknowledges it, so conn_count reaches zero only
 * for a server that calls moq_msquic_managed_conn_ack_terminal; stop() is
 * the unconditional path. */
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

/* The MoQ version this facade speaks. Exact-version endpoints know it at
 * create(). A multi-version SERVER returns 0 here because version is
 * per-accepted-connection; use the conn accessor from the lane pump. */
MOQ_API moq_version_t moq_msquic_managed_negotiated_version(
    const moq_msquic_managed_t *m);

/* The immutable MoQ version selected for an accepted server connection or the
 * single client connection. Valid only for an application-owned connection
 * pointer inside its owning on_lane_pump; returns 0 for NULL or outside that
 * pump window. */
MOQ_API moq_version_t moq_msquic_managed_conn_negotiated_version(
    const moq_msquic_managed_conn_t *conn);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_MSQUIC_MANAGED_H */
