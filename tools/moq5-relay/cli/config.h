#ifndef MOQR_CLI_CONFIG_H
#define MOQR_CLI_CONFIG_H

/*
 * moq5-relay configuration: a small, strict JSON schema.
 *
 * {
 *   "listener": {
 *     "transport": "msquic",            // optional; only value accepted
 *     "host": "0.0.0.0",                // optional
 *     "port": 4443,                     // required, 1..65535
 *     "cert": "/path/cert.pem",         // required for serve
 *     "key":  "/path/key.pem",          // required for serve
 *     "versions": [18],                 // optional; default [18]. An ORDERED,
 *                                       // non-empty set of supported drafts,
 *                                       // most preferred first; entries must be
 *                                       // exact integers from {18,16}, unique.
 *                                       // One entry keeps exact-listener
 *                                       // semantics; several offer several
 *                                       // ALPNs on ONE listener and ALPN
 *                                       // selects the per-connection draft.
 *     "lanes": 1,                       // optional; default 1, valid 1..64.
 *                                       // >1 partitions accepted connections
 *                                       // across N independent lock-domain
 *                                       // lanes (one relay shard each). 1 is
 *                                       // the single-core production path.
 *     "insecure_skip_verify": false     // optional; demos/tests only
 *   },
 *   "budgets": {                        // all optional; 0 = library default
 *     "max_tracks": 64, "max_subs": 256, "max_bindings": 64,
 *     "max_ns_nodes": 256, "max_ns_subs": 64, "max_intents": 64,
 *     "name_intern_bytes": 65536,
 *     "log": { "max_groups": 8, "max_bytes": 8388608, "max_age_us": 0 },
 *     "log_max_subgroups": 16, "log_max_objects": 4096,
 *     "log_max_cursors": 64, "log_max_chunk_nodes": 8192,
 *     "cross_shard": {                  // optional; the lanes>1 pool bounds
 *                                       // (accepted but structurally inert
 *                                       // at lanes == 1); 0 = library
 *                                       // default; entries bounded by 2^20,
 *                                       // bytes by 2^40
 *       "journal_entries": 256,         // per-shard namespace journal
 *       "mailbox_entries": 256,         // per directed announce mailbox
 *       "demand_channel_entries": 64,   // per directed demand channel
 *       "demand_channel_bytes": 8388608,// per-channel data-byte gauge; an
 *                                       // explicit value below one resolved
 *                                       // log record is INVAL at any lane
 *                                       // count
 *       "pending_demands": 64,          // per-shard remote-demand table
 *       "subgroup_slots": 64            // per-demand chunk-progress slots
 *     }
 *   },
 *   "telemetry": {                      // all optional; observability knobs
 *     "trace_ring_records": 4096        // flight-recorder depth; 0 = default
 *   },
 *   "auth": {                           // optional; default { "mode": "allow_all" }
 *     "mode": "toy",                    // "allow_all" (default) or "toy"
 *     "default": "allow",               // toy only: fallthrough "allow"|"deny"
 *     "rules": [                        // toy only: first match wins
 *       { "action": "subscribe",        // CAT action token
 *         "namespace_prefix": ["live"], // 0..8 parts; empty = any
 *         "decision": "deny",           // "allow"|"deny" (no "defer")
 *         "reason": "unscoped" }        // optional; reported on deny
 *     ]
 *   },
 *   "linger_us": 500
 * }
 *
 * Unknown keys are rejected (config typos fail loudly, not silently).
 * Entry counts are capped at 1,048,576 and byte budgets at 1 TiB —
 * anything larger is treated as a config error, not a request. The toy auth
 * policy is a bounded, no-crypto action×prefix table (the same seam a real
 * verifier plugs into); "defer" is not accepted in config.
 */

#include <string.h>

#include <moqrelay/relay.h>

#include <moqr_shards.h>
#include <moqrelay/auth_toy.h>

#ifdef __cplusplus
extern "C" {
#endif

#define MOQR_CLI_MAX_VERSIONS 4

/* Upper bound on listener.lanes. Kept decoupled from the shard runtime here;
 * main.c _Static_asserts it equals MOQR_SHARDS_MAX where both headers meet. */
#define MOQR_CLI_MAX_LANES 64u

/* Toy-auth policy bounds (config errors past these, never silent truncation). */
#define MOQR_CLI_MAX_AUTH_RULES   32   /* rules in a toy policy              */
#define MOQR_CLI_MAX_NS_PARTS      8   /* prefix parts per rule              */
#define MOQR_CLI_MAX_NS_PART_LEN  64   /* bytes per prefix part              */

/* Observability knobs (the "telemetry" object). The trace ring is created
 * by the CLI and handed to the core as cfg.trace; this sizes it. */
typedef struct moqr_cli_telemetry {
    uint32_t trace_ring_records;   /* flight-recorder depth; 0 = default */
} moqr_cli_telemetry_t;

typedef enum moqr_cli_auth_mode {
    MOQR_CLI_AUTH_ALLOW_ALL = 0,   /* default: no hook installed         */
    MOQR_CLI_AUTH_TOY       = 1,   /* bundled static-toy verifier        */
} moqr_cli_auth_mode_t;

/* Owned backing for one rule's namespace prefix; parts[] point into bytes[]. */
typedef struct moqr_cli_auth_ns {
    uint8_t     bytes[MOQR_CLI_MAX_NS_PARTS][MOQR_CLI_MAX_NS_PART_LEN];
    moq_bytes_t parts[MOQR_CLI_MAX_NS_PARTS];
    size_t      count;
} moqr_cli_auth_ns_t;

/*
 * Parsed authorization policy. When mode == TOY, parsing wires a
 * self-consistent moqr_auth_toy_t: `toy.rules` points at `rules[]`, and each
 * rule's `ns_prefix` points at the matching `ns[]` storage. Because of those
 * internal self-pointers, a moqr_cli_config_t must be used IN PLACE after
 * parsing — do NOT shallow-copy or move it. The toy authorization hook is one
 * such pointer; the listener/WebTransport subprotocol arrays and the
 * WebTransport Origin allowlist are others, each pointing into buffers inside
 * this same object. A copy's pointers would still address the original, so a
 * moved config is silently wrong rather than obviously broken. Parse into the
 * object the caller keeps alive; a FAILED parse leaves a config that must not
 * be used to start anything.
 */
typedef struct moqr_cli_auth {
    moqr_cli_auth_mode_t mode;
    moqr_cli_auth_ns_t   ns[MOQR_CLI_MAX_AUTH_RULES];
    moqr_auth_toy_rule_t rules[MOQR_CLI_MAX_AUTH_RULES];
    moqr_auth_toy_t      toy;   /* ctx for moqr_auth_toy_authorize (mode==TOY) */
} moqr_cli_auth_t;

/*
 * The optional WebTransport listener.
 *
 * A second listener on the SAME relay: MoQ carried over WebTransport by
 * wtquic's MsQuic backend, so a browser can reach the runtime the raw MsQuic
 * listener already serves. It is an explicit object rather than a
 * listener.transport value, because it carries its own address, TLS material,
 * lane count and HTTP/3 surface -- and because a config that never mentions it
 * must keep behaving exactly as it does today.
 */

/*
 * The WebTransport-over-HTTP/3 wire profile. A transport dialect, NOT a MoQ
 * version: the MoQ draft is chosen by the subprotocol list below. The two are
 * mutually exclusive and never auto-negotiated.
 */
typedef enum moqr_cli_wt_profile {
    /* ":protocol = webtransport-h3", the current WebTransport-H3 draft */
    MOQR_CLI_WT_PROFILE_CURRENT = 0,
    /* ":protocol = webtransport" plus the drafts-13/14 max-sessions signal,
     * what the proxygen/moxygen and picoquic h3zero families speak */
    MOQR_CLI_WT_PROFILE_D13_14_COMPAT = 1,
    /* ":protocol = webtransport" plus the draft-02 request marker and RFC
     * 9297 quarter-stream-ID datagrams. Described by what it emits; which
     * profile a peer selects is not asserted here, and no browser is
     * promised. Opt-in -- "current" stays the default. */
    MOQR_CLI_WT_PROFILE_D02_RFC9297_COMPAT = 2,
} moqr_cli_wt_profile_t;

/* The closed name of a configured WebTransport profile, or NULL for any value
 * outside the declared set (never a default name). The one mapping every
 * published surface uses. */
static inline const char *
moqr_cli_wt_profile_name(moqr_cli_wt_profile_t p)
{
    switch (p) {
    case MOQR_CLI_WT_PROFILE_CURRENT:       return "current";
    case MOQR_CLI_WT_PROFILE_D13_14_COMPAT: return "d13_14_compat";
    case MOQR_CLI_WT_PROFILE_D02_RFC9297_COMPAT: return "d02_rfc9297_compat";
    default:                                return NULL;
    }
}

/* How a served WebTransport path authorizes the client's serialized Origin.
 * The names mirror the facade's closed set; the stored value is a uint32_t so
 * the config layout does not depend on an enum's size. */
typedef enum moqr_cli_origin_policy {
    MOQR_CLI_ORIGIN_POLICY_UNSET = 0,
    MOQR_CLI_ORIGIN_POLICY_ALLOW_ANY_NON_OPAQUE = 1,
    MOQR_CLI_ORIGIN_POLICY_ALLOWLIST = 2,
    MOQR_CLI_ORIGIN_POLICY_ALLOW_ANY_INCLUDING_NULL = 3
} moqr_cli_origin_policy_t;

/* The longest name moqr_cli_wt_profile_name can return.
 *
 * The maximum-width renderers size themselves against this rather than against
 * a literal of their own, so there is one place to change when a dialect is
 * added and no way for a bound to be computed from a label the vocabulary has
 * outgrown. moqr_cli_wt_profile_name_max_is_longest pins the relationship. */
#define MOQR_CLI_WT_PROFILE_NAME_MAX "d02_rfc9297_compat"

/* Is the assumed maximum still the longest name in the vocabulary? */
static inline bool
moqr_cli_wt_profile_name_max_is_longest(void)
{
    size_t max = sizeof(MOQR_CLI_WT_PROFILE_NAME_MAX) - 1u;
    bool reached = false;
    for (uint32_t p = 0; p <= (uint32_t)MOQR_CLI_WT_PROFILE_D02_RFC9297_COMPAT;
         p++) {
        const char *n = moqr_cli_wt_profile_name((moqr_cli_wt_profile_t)p);
        size_t len = (n == NULL) ? 0u : strlen(n);
        if (len > max) {
            return false;
        }
        if (len == max) {
            reached = true;
        }
    }
    return reached;
}

/* The closed name of a configured policy, or NULL outside the declared set. */
static inline const char *
moqr_cli_origin_policy_name(uint32_t p)
{
    switch (p) {
    case MOQR_CLI_ORIGIN_POLICY_UNSET:                return "unset";
    case MOQR_CLI_ORIGIN_POLICY_ALLOW_ANY_NON_OPAQUE: return "allow_any_non_opaque";
    case MOQR_CLI_ORIGIN_POLICY_ALLOWLIST:            return "allowlist";
    case MOQR_CLI_ORIGIN_POLICY_ALLOW_ANY_INCLUDING_NULL:
        return "allow_any_including_null";
    default:                                          return NULL;
    }
}

/* Origin allowlist limits, matching the facade's: at most 8 entries, each at
 * most 320 bytes EXCLUDING its NUL, and at most 512 copied bytes INCLUDING
 * every NUL. The budget already bounds the sum, so one 512-byte buffer holds
 * any configuration the limits admit. */
#define MOQR_CLI_MAX_ORIGINS 8u
#define MOQR_CLI_MAX_ORIGIN_BYTES 320u
#define MOQR_CLI_ORIGIN_COPY_BUDGET 512u

#define MOQR_CLI_WT_MAX_PATH 256

typedef struct moqr_cli_wt {
    bool     enabled;             /* a "webtransport" object was present    */
    char     host[256];
    int      port;
    char     cert[1024];
    char     key[1024];
    uint32_t lanes;               /* own lock-domain lanes; 1..64, def 1    */
    char     path[MOQR_CLI_WT_MAX_PATH];   /* request path, default "/moq"  */
    /* Ordered MoQ drafts offered as WebTransport subprotocols, most preferred
     * first; the peer selects one and the session is created at that version. */
    moq_version_t versions[MOQR_CLI_MAX_VERSIONS];
    size_t   version_count;
    char     subproto_buf[MOQR_CLI_MAX_VERSIONS][16];
    const char *subprotos[MOQR_CLI_MAX_VERSIONS];
    /* Deterministic ordered label for the set THIS listener offers; the raw
     * listener's set is configured separately and may differ. */
    char     alpn_set[MOQR_CLI_MAX_VERSIONS * 17];
    moqr_cli_wt_profile_t profile;
    /* Origin authorization for the one served path. The entries live in
     * origin_buf and `origins` points into it, so the whole contract moves
     * with the config object and needs no teardown -- see the use-in-place
     * rule on moqr_cli_config_t. A policy other than ALLOWLIST carries no
     * list at all: count 0 and, at the facade boundary, a NULL array. */
    uint32_t origin_policy;                     /* moqr_cli_origin_policy_t */
    char     origin_buf[MOQR_CLI_ORIGIN_COPY_BUDGET];
    const char *origins[MOQR_CLI_MAX_ORIGINS];
    size_t   origin_count;
} moqr_cli_wt_t;

/* -- the admin endpoint ---------------------------------------------------
 *
 * Disabled by default. v1 accepts exactly ONE endpoint shape: a loopback TCP
 * listener.
 *
 * Pathname UDS is DEFERRED, not merely unimplemented. Portable POSIX cannot
 * make `unlink` target the inode this process created -- `lstat` then `unlink`
 * has a substitution window -- and `umask` is process-global, so it is the
 * wrong primitive once any worker thread can exist. Shipping that behind a
 * protected-parent-directory assumption would be a guarantee the code does not
 * provide. `admin.uds` therefore fails through the ordinary unknown-key rule
 * until a UDS slice arrives with its own threat model and API.
 *
 * TCP is LOOPBACK-ONLY, checked by parsing the literal address rather than
 * resolving a name. Remote exposure is a separate, explicit security decision,
 * and a permissive resolver is how it happens by accident.
 *
 * Client capacity and deadlines are compile-time constants in v1 (see
 * moqr_admin.h); promoting any of them to configuration means wiring it into
 * moqr_cli_describe_capacity in the same change. */
typedef uint32_t moqr_cli_admin_mode_t;

#define MOQR_CLI_ADMIN_OFF 0u
#define MOQR_CLI_ADMIN_TCP 1u

/* The admin thread's fixed stack. Small because the thread runs one bounded
 * state machine with no recursion and no large frames; fixed because a
 * reservation the capacity model cannot name is a reservation it cannot
 * report. */
#define MOQR_CLI_ADMIN_STACK_BYTES (256u * 1024u)

/* Structured stdout logging. `text` is the accepted default: today's bytes,
 * streams and flush points. `json` makes a serve's stdout JSON-only (one
 * event per line) and moves the serve-start capacity prose to stderr; the
 * signal documents and diagnostics on stderr are unchanged in both modes.
 * Strict: unknown keys, duplicates and any other value are refused. */
typedef uint32_t moqr_cli_log_format_t;

#define MOQR_CLI_LOG_TEXT 0u
#define MOQR_CLI_LOG_JSON 1u

typedef struct moqr_cli_logging {
    moqr_cli_log_format_t format;
} moqr_cli_logging_t;

typedef struct moqr_cli_admin {
    bool                  enabled;
    moqr_cli_admin_mode_t mode;
    char                  host[64];
    int                   port;
} moqr_cli_admin_t;

/*
 * The resolved configuration.
 *
 * USE IN PLACE. Several members point into this same object -- the listener
 * and WebTransport subprotocol arrays, the WebTransport Origin allowlist, and
 * the toy authorization rules. Shallow-copying or moving the struct leaves
 * those pointers addressing the original, which is silently wrong rather than
 * obviously broken, so there is no movable-config API: parse into the object
 * the caller keeps alive for as long as anything borrows from it. A failed
 * parse must not be used to start a listener.
 */
typedef struct moqr_cli_config {
    char     host[256];
    int      port;
    char     cert[1024];
    char     key[1024];
    char     alpn_buf[MOQR_CLI_MAX_VERSIONS][16];
    const char *alpns[MOQR_CLI_MAX_VERSIONS];
    size_t   alpn_count;          /* == version_count after a successful parse */
    uint32_t version;             /* FIRST listener draft number (16 or 18);
                                   * authoritative only when version_count == 1 */
    /* The ordered supported set, most preferred first. Always >= 1 entry after
     * a successful parse; count 1 is the exact-listener case. */
    moq_version_t versions[MOQR_CLI_MAX_VERSIONS];
    size_t   version_count;
    /* Deterministic, parser-safe label for the whole offered set: the single
     * ALPN when there is one, else the ordered ALPNs joined by '+'. Startup and
     * telemetry name the SET, never just the first entry. */
    char     alpn_set[MOQR_CLI_MAX_VERSIONS * 17];
    uint32_t lanes;               /* connection lock-domain lanes; 1..64, def 1 */
    bool     insecure_skip_verify;
    moqr_cli_telemetry_t telemetry;
    moqr_cli_auth_t      auth;
    /* Core budgets; .alloc left NULL for the caller to fill. */
    moqr_core_relay_cfg_t core;
    /* budgets.cross_shard — the K>1 pool bounds, all zero by default (an
     * absent or empty object resolves exactly like today: the shared shard
     * resolver supplies every default and stays authoritative for the
     * cross-field rules, e.g. an explicit demand_channel_bytes below one
     * resolved log record is INVAL at resolve time). lanes == 1 accepts the
     * object; the pools stay structurally inert there. No admission or
     * pump-tuning key exists here by design. */
    /* the optional second listener; .enabled is false unless configured */
    moqr_cli_wt_t wt;
    /* the optional admin endpoint; .enabled is false unless configured */
    moqr_cli_admin_t admin;
    /* stdout logging; format is TEXT unless configured */
    moqr_cli_logging_t logging;
    struct {
        uint32_t journal_entries;
        uint32_t mailbox_entries;
        uint32_t demand_channel_entries;
        uint32_t pending_demands;
        uint32_t subgroup_slots;
        uint64_t demand_channel_bytes;
    } cross_shard;
} moqr_cli_config_t;

/*
 * How the two listeners share ONE shard runtime.
 *
 * Each facade owns a disjoint, contiguous range of global shards: raw first,
 * WebTransport after it. A lane index is meaningful only inside its own
 * facade, so every crossing between a lane and the runtime goes through this
 * plan -- that is what keeps one facade's pump callback off a session the
 * other facade owns, while both still publish into the same cross-shard plane.
 */
typedef struct moqr_cli_shard_plan {
    uint32_t total_shards;   /* raw_count + wt_count; <= MOQR_SHARDS_MAX */
    uint32_t raw_first;      /* always 0 */
    uint32_t raw_count;      /* == config.lanes                          */
    uint32_t wt_first;       /* == raw_count                             */
    uint32_t wt_count;       /* == config.wt.lanes, or 0 when disabled   */
} moqr_cli_shard_plan_t;

/* Shards the runtime must allocate: one per lane across BOTH listeners. The
 * capacity model and serve both size themselves from this, so a dual-listener
 * config reports the ceiling it will actually request. */
uint32_t moqr_cli_total_lanes(const moqr_cli_config_t *cfg);

/* Per-facade admission caps, from the same resolved limits the combined cap
 * comes from. They PARTITION that total: handing each facade the combined cap
 * would let the two listeners admit twice the capacity whose backing shards
 * were sized and reported. `wt_cap` is 0 when there is no WebTransport
 * listener. */
moqr_result_t moqr_cli_facade_caps(const moqr_cli_config_t *cfg,
                                   const moq_alloc_t *alloc,
                                   uint32_t *out_raw_cap,
                                   uint32_t *out_wt_cap);

/* True when the parsed config carries a WebTransport listener. */
bool moqr_cli_config_has_webtransport(const moqr_cli_config_t *cfg);

/* Resolve the plan, or refuse with a reason. Refuses before anything is
 * allocated: a combined lane count past the runtime cap, or a listener with no
 * lanes at all, is a configuration error rather than a startup surprise. */
moqr_result_t moqr_cli_shard_plan(const moqr_cli_config_t *cfg,
                                  moqr_cli_shard_plan_t *out, char *err,
                                  size_t err_len);

/* Global shard index for a lane of each facade. Out-of-range lanes return
 * UINT32_MAX rather than a plausible-looking neighbour. */
uint32_t moqr_cli_shard_of_raw_lane(const moqr_cli_shard_plan_t *p,
                                    uint32_t lane);
uint32_t moqr_cli_shard_of_wt_lane(const moqr_cli_shard_plan_t *p,
                                   uint32_t lane);


/*
 * Parse a config document. On failure returns MOQR_ERR_INVAL and writes a
 * one-line reason into err (always NUL-terminated). Pure: no I/O.
 */
moqr_result_t moqr_cli_config_parse(const char *json, size_t len,
                                    moqr_cli_config_t *out, char *err,
                                    size_t err_len);

/* Load + parse a file (CLI tier owns file I/O). */
moqr_result_t moqr_cli_config_load(const char *path, moqr_cli_config_t *out,
                                   char *err, size_t err_len);

/*
 * Assemble the live relay core from a parsed config: create the flight-
 * recorder trace ring (depth = telemetry.trace_ring_records; 0 = library
 * default — tracing is always on), attach it as cfg.core.trace, and create
 * the core. On MOQR_OK both out-params are owned by the caller, which must
 * destroy the CORE first, then the TRACE. On failure both are NULL and
 * nothing needs cleanup. `alloc` is borrowed for their lifetime.
 *
 * This is the one place the telemetry knob becomes real: serve() and the
 * wiring test go through it, so an attached-and-recording trace is proven,
 * not just a parsed field.
 */
moqr_result_t moqr_cli_build_core(const moqr_cli_config_t *cfg,
                                  const moq_alloc_t *alloc,
                                  moqr_trace_t **trace_out,
                                  moqr_core_t **core_out);

/*
 * Map a parsed config's budgets + authorize hook into a core cfg: copies the
 * parsed `core` budgets, sets `alloc`, and installs the toy authorizer when
 * configured (allow-all leaves the hook NULL). `trace` is left untouched — the
 * caller attaches one (build_core: one ring; the multi-shard runtime: one per
 * shard). Shared so the single-lane and multi-lane cores cannot drift.
 */
void moqr_cli_core_cfg_from(const moqr_cli_config_t *cfg,
                            const moq_alloc_t *alloc,
                            moqr_core_relay_cfg_t *out);

/*
 * The relay-state allocation-request ceiling for this config, per lane
 * count, as the production paths actually allocate it:
 *   lanes == 1 — the direct composition: one core + one binding + the trace
 *     ring, NO shard container, plus the one permanent snapshot row
 *     the single-lane serve keeps (cli_runtime_bytes).
 *   lanes > 1  — the full shard runtime (moqr_shards_capacity_describe)
 *     plus the multi-lane CLI's own context and snapshot rows
 *     (cli_runtime_bytes, printed as its own line, included in the total).
 * Models allocation REQUESTS only: malloc metadata, size-class rounding,
 * fragmentation, thread stacks, transient diagnostic buffers, and memory
 * that remains adapter/session-owned are outside it (stated in the print).
 */
typedef struct moqr_cli_capacity {
    uint64_t core_structure_bytes;   /* per shard */
    uint64_t core_payload_bytes;     /* per shard */
    uint64_t bind_structure_bytes;   /* per shard (incl. announce bytes)  */
    uint64_t trace_bytes;            /* per shard */
    uint64_t cross_shard_bytes;      /* lanes>1: runtime + channels/canon/
                                        staging ceilings; 0 at lanes == 1 */
    uint64_t cli_runtime_bytes;      /* snapshot rows, plus the serve
                                      * context at lanes > 1        */
    /* Allocator-owned admin endpoint storage: broker and admin context, the
     * fixed client slots with their request/header/response bookkeeping, and
     * both body banks with Prometheus and OpenMetrics storage sized from the
     * checked renderer bound. Zero when the endpoint is disabled. Included in
     * cli_runtime_bytes and therefore in total_bytes. */
    uint64_t admin_bytes;
    /* The admin thread's FIXED stack reservation, reported separately: it is a
     * pthread attribute, not an allocator request, so folding it into the
     * ceiling would misreport what the allocator must satisfy. Kernel socket
     * buffers stay excluded for the same reason and are not modelled here. */
    uint64_t admin_thread_stack_bytes;
    uint64_t total_bytes;            /* the printed ceiling               */
    uint32_t usable_bindings_per_shard;
} moqr_cli_capacity_t;

/* `serve_ctx_bytes` is the multi-lane CLI context's own size (the caller
 * owns that type); it joins cli_runtime_bytes at lanes > 1 and is ignored
 * at lanes == 1 (which allocates no such context). */
moqr_result_t moqr_cli_describe_capacity(const moqr_cli_config_t *cfg,
                                         const moq_alloc_t *alloc,
                                         size_t serve_ctx_bytes,
                                         moqr_cli_capacity_t *out);

/*
 * The ONE shard-config builder both `capacity` and `serve` consume, so the
 * two can never disagree about the effective multi-lane config. It sets
 * admit_remote_demand = (lanes > 1): production admission is automatically
 * on for a multi-lane relay and off for the single-lane M1 path. This is
 * the sole place that rule lives — no user-facing admission key exists.
 */
void moqr_cli_build_shards_cfg(const moqr_cli_config_t *cfg,
                               const moq_alloc_t *alloc,
                               moqr_shards_cfg_t *out);

/*
 * CLI-private serve composition: the shard config AND the transport facade
 * admission cap a multi-lane `serve` builds from a parsed CLI config,
 * EXACTLY as cmd_serve_lanes does — the shared builder output (so admission
 * and the per-lane bind clamp are the production rule) plus the facade
 * max_connections = lanes * usable_bindings_per_shard (the strict per-lane
 * clamp, not the old coarse lanes * max_bindings). Factored so the
 * serve-shaped regression consumes the real builder output and transport
 * mapping, never a hand-authored parallel config. Returns MOQR_ERR_INVAL
 * (no output written) when the config does not resolve.
 */
moqr_result_t moqr_cli_serve_compose(const moqr_cli_config_t *cfg,
                                     const moq_alloc_t *alloc,
                                     moqr_shards_cfg_t *out_scfg,
                                     uint32_t *out_max_connections);

/*
 * Shared semantic validation, run BEFORE any command executes: builds the
 * effective shard config through the ONE builder and resolves it, so a
 * cross-field violation (e.g. an explicit demand_channel_bytes below one
 * resolved log record) refuses `capacity` AND both serve paths, at every
 * lane count, before anything is allocated or listening.
 */
moqr_result_t moqr_cli_config_validate(const moqr_cli_config_t *cfg,
                                       const moq_alloc_t *alloc);

#ifdef MOQR_VERIFY_SEAM
/* The verify and measure compositions serve text only: their stdout carries
 * the RELAY_BLOCKED_V0 / seam rows with their own contracts. A `serve` under
 * logging.format = json is refused HERE, before any preflight, allocation,
 * listener or thread; `capacity` is unaffected. MOQR_ERR_INVAL with a message
 * on refusal, MOQR_OK otherwise. */
moqr_result_t moqr_cli_verify_refuse_json_logging(const moqr_cli_config_t *cfg,
                                                  char *err, size_t err_len);
#endif

#ifdef MOQR_VERIFY_SEAM
/*
 * Blocked-scenario constraint seam. Compiled into exactly TWO binaries:
 *   moq-relay-verify  (MOQR_VERIFY_SEAM + MOQR_BIND_TESTING) — the reason
 *                     proof: constrained pools + RELAY_BLOCKED_V0 emission
 *                     over the bind test-internals seam.
 *   moq-relay-measure (MOQR_VERIFY_SEAM only) — the timing binary: the SAME
 *                     constrained pools over the PRODUCTION bind library, no
 *                     debug counters, no blocked records — so baseline and
 *                     candidate timing run the exact traffic shape the
 *                     verify preflight proved.
 * The plain moq5-relay compiles neither: the environment is inert there
 * (pinned byte-identical by the hygiene test).
 *
 * Two overrides, each read once by moqr_cli_verify_env_load:
 *
 *   MOQR_VERIFY_BIND_MAX_OPEN_SUBGROUPS     -> bind_cfg.max_open_subgroups
 *   MOQR_VERIFY_SESSION_MAX_OPEN_SUBGROUPS  -> managed session subgroup pool
 *
 * Strict values only: decimal 1..65535, no leading zeros, no junk — anything
 * else fails the load CLOSED (the process refuses to start rather than run
 * an unconstrained scenario whose acceptance would then time out). Unset =
 * 0 = library default. The bind value is applied inside the ONE shard-config
 * builder, so `capacity` describes exactly what `serve` runs — the described
 * bind ceiling shrinking with the pool is the transport-free proof the
 * constraint reached the resolver.
 */
moqr_result_t moqr_cli_verify_env_load(char *err, size_t err_len);
uint32_t moqr_cli_verify_bind_max_sgs(void);
uint32_t moqr_cli_verify_session_max_sgs(void);
#endif

#ifdef __cplusplus
}
#endif


/* The config -> transport version mapping, decided ONCE in production code so
 * both serve compositions cannot drift apart. It is deliberately transport
 * neutral (no msquic types) because this translation unit is also linked into
 * ungated test binaries.
 *
 *   count == 0 : the EXACT-version plan. `exact` is the one negotiated version
 *                and `list` is NULL, matching the managed adapter's legacy
 *                exact-version representation, where a zero list count means
 *                "use cfg.version".
 *   count >  1 : the ordered multi-version plan. `exact` is 0 and `list`
 *                carries `count` versions, most preferred first.
 *   count == 1 is never produced: a single configured version IS the exact
 *   plan, so there is exactly one representation of it. After a successful
 *   parse an exact plan always has a non-zero `exact`. */
typedef struct moqr_cli_version_plan {
    moq_version_t        exact;
    const moq_version_t *list;
    size_t               count;
} moqr_cli_version_plan_t;

void moqr_cli_version_plan(const moqr_cli_config_t *cfg,
                           moqr_cli_version_plan_t *out);

#endif /* MOQR_CLI_CONFIG_H */
