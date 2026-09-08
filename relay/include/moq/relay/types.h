#ifndef MOQRELAY_TYPES_H
#define MOQRELAY_TYPES_H

/*
 * moq5-relay core types.
 *
 * The relay core is a sans-I/O, single-writer-per-shard state machine built
 * on the public libmoq session API. This header defines the relay's handle
 * universe, result codes, epoch triple, response vocabulary, trace-record
 * layout, and configuration skeletons. No data structures live here — this header
 * ships types only (see relay/docs/architecture.md).
 *
 * Hard rules:
 *   - Sans-I/O: no sockets, threads, clocks, or globals in core/.
 *   - Public libmoq API only (enforced by scripts/check_relay_boundary.sh).
 *   - Borrow discipline (borrow discipline): anything retained from a polled session
 *     event is copied into core-owned storage during apply(); trace records
 *     carry handles and scalars only, never borrowed pointers.
 *   - Everything bounded: every capacity in moqr_core_cfg_t feeds the
 *     closed-form memory ceiling (moq/relay/capacity.h).
 */

#include <moq/types.h>
#include <moq/relay/export.h>

#ifdef __cplusplus
extern "C" {
#endif

/* -- Result codes ---------------------------------------------------
 * Same convention as the session core: MOQR_OK == 0, errors negative,
 * `if (rc < 0)` is the universal error check.
 */

typedef int moqr_result_t;

#define MOQR_OK                0
#define MOQR_DONE              1    /* sentinel: nothing more (not an error) */

#define MOQR_ERR_NOMEM        (-1)
#define MOQR_ERR_INVAL        (-2)
#define MOQR_ERR_WOULD_BLOCK  (-3)   /* transient; caller retries after drain */
#define MOQR_ERR_STALE_HANDLE (-4)
#define MOQR_ERR_WRONG_STATE  (-5)
/* Admission refused: accepting would exceed a configured bound. Distinct from
 * NOMEM (allocator failure) — capacity refusal is policy, not failure, and is
 * typically answered on the wire via the response vocabulary below. */
#define MOQR_ERR_CAPACITY     (-6)
#define MOQR_ERR_UNSUPPORTED  (-7)
/* The referenced content aged past the retention horizon (e.g. a late object
 * for an already-evicted group). Normal under reordering — callers usually
 * drop and count rather than treat it as failure. */
#define MOQR_ERR_TOO_OLD      (-8)
#define MOQR_ERR_INTERNAL     (-99)

MOQR_CORE_API const char *moqr_strerror(moqr_result_t rc);

/* -- Epoch triple  ------------------------
 * Stamped on every trace record and dump header so any observability
 * artifact is unambiguous about which cluster/config generation produced
 * it. node_epoch/shard_epoch advance only via control inputs ;
 * route_epoch advances on every route mutation within a shard.
 */

typedef struct moqr_epochs {
    uint64_t node_epoch;
    uint64_t shard_epoch;
    uint64_t route_epoch;
} moqr_epochs_t;

#ifdef __cplusplus
#define MOQR_EPOCHS_INIT (moqr_epochs_t{0, 0, 0})
#else
#define MOQR_EPOCHS_INIT ((moqr_epochs_t){ 0, 0, 0 })
#endif

/* -- Entity handles --------------------------------------------------
 * Relay-private handle universe using the session core's public 64-bit
 * packed layout (moq_handle_pack: [pool:4][tag:16][generation:28][slot:16],
 * odd generation = live). The 16-bit tag field carries the SHARD TAG
 * (shard index + 1; nonzero as the pack contract requires) so cross-shard
 * handle misuse is structurally detectable, mirroring the session core's
 * cross-session safety.
 *
 * Pool tags below are relay-scope; they share no registry with the session
 * core's MOQ_HANDLE_POOL_* values (separate universes, never compared).
 */

typedef struct moqr_track   { uint64_t _opaque; } moqr_track_t;
typedef struct moqr_cursor  { uint64_t _opaque; } moqr_cursor_t;
typedef struct moqr_binding { uint64_t _opaque; } moqr_binding_t;   /* one relay<->session attachment */
typedef struct moqr_parked  { uint64_t _opaque; } moqr_parked_t;    /* deferred request (join/auth) */

#define MOQR_HANDLE_POOL_TRACK   1u
#define MOQR_HANDLE_POOL_CURSOR  2u
#define MOQR_HANDLE_POOL_BINDING 3u
#define MOQR_HANDLE_POOL_PARKED  4u
#define MOQR_HANDLE_POOL_SUB     5u   /* downstream subscription (relay.h) */
#define MOQR_HANDLE_POOL_FETCH   6u   /* retained-hit fetch cursor (relay.h) */
/* values 7..15 reserved; append-only, never renumber */

#ifdef __cplusplus
#define MOQR_TRACK_INVALID   (moqr_track_t{0})
#define MOQR_CURSOR_INVALID  (moqr_cursor_t{0})
#define MOQR_BINDING_INVALID (moqr_binding_t{0})
#define MOQR_PARKED_INVALID  (moqr_parked_t{0})
#else
#define MOQR_TRACK_INVALID   ((moqr_track_t){ 0 })
#define MOQR_CURSOR_INVALID  ((moqr_cursor_t){ 0 })
#define MOQR_BINDING_INVALID ((moqr_binding_t){ 0 })
#define MOQR_PARKED_INVALID  ((moqr_parked_t){ 0 })
#endif

/* -- Namespace / name views -------------------------------------------
 * Byte views BORROWED for the duration of a call (borrow discipline): the
 * core copies what it retains into interned storage. Lives here (not relay.h)
 * so the auth header can reference it without a relay.h include cycle. */
typedef struct moqr_ns {
    const moq_bytes_t *parts;
    size_t             count;   /* 1..32 for announces; 0..32 for prefixes */
} moqr_ns_t;

MOQR_CORE_API bool     moqr_track_is_valid(moqr_track_t h);
MOQR_CORE_API bool     moqr_track_eq(moqr_track_t a, moqr_track_t b);
MOQR_CORE_API bool     moqr_cursor_is_valid(moqr_cursor_t h);
MOQR_CORE_API bool     moqr_cursor_eq(moqr_cursor_t a, moqr_cursor_t b);
MOQR_CORE_API bool     moqr_binding_is_valid(moqr_binding_t h);
MOQR_CORE_API bool     moqr_binding_eq(moqr_binding_t a, moqr_binding_t b);
MOQR_CORE_API bool     moqr_parked_is_valid(moqr_parked_t h);
MOQR_CORE_API bool     moqr_parked_eq(moqr_parked_t a, moqr_parked_t b);

/* -- Response vocabulary  ----------------
 * The closed set of pressure/lifecycle responses. Every policy that reacts
 * to overload, drain, auth revocation, or lag picks from this enum; tests
 * enumerate it exhaustively; new pressure sources reuse it. Fixed-width and
 * append-only for the same ABI reasons as session action/event kinds.
 */

typedef uint32_t moqr_response_t;

#define MOQR_RESPONSE_ADMIT           0u
#define MOQR_RESPONSE_REJECT          1u
#define MOQR_RESPONSE_REJECT_REDIRECT 2u  /* reject + REDIRECT target uri     */
#define MOQR_RESPONSE_SESSION_GOAWAY  3u  /* drain whole session (d16 + d18)  */
#define MOQR_RESPONSE_REQUEST_GOAWAY  4u  /* migrate one request (d18 §10.4)  */
#define MOQR_RESPONSE_CURSOR_SKIP     5u  /* jump to a group boundary         */
#define MOQR_RESPONSE_CURSOR_RETIRE   6u  /* terminate one subscription       */
#define MOQR_RESPONSE_EVICT           7u  /* evict a group from a track log   */

/* -- Trace records (deterministic observability) ---------------
 * Fixed-size, scalar-only (scalar-only rule: never borrowed pointers). shard_seq is
 * a per-shard monotonic counter — no wall clock — so a trace stream hashes
 * identically across replays of the same input stream. Records are folded
 * into a running hash as emitted; the bounded ring retains the recent tail
 * as a flight recorder.
 */

typedef uint32_t moqr_trace_kind_t;

#define MOQR_TRACE_INGEST              1u
#define MOQR_TRACE_APPEND              2u
#define MOQR_TRACE_GROUP_CLOSE         3u
#define MOQR_TRACE_EVICT               4u
#define MOQR_TRACE_CURSOR_CREATE       5u
#define MOQR_TRACE_CURSOR_ADVANCE      6u
#define MOQR_TRACE_CURSOR_SKIP         7u
#define MOQR_TRACE_CURSOR_RETIRE       8u
#define MOQR_TRACE_DELIVERY            9u
#define MOQR_TRACE_BLOCKED            10u
#define MOQR_TRACE_RESUMED            11u
#define MOQR_TRACE_ROUTE_ADD          12u
#define MOQR_TRACE_ROUTE_REMOVE       13u
#define MOQR_TRACE_UPSTREAM_SUBSCRIBE 14u
#define MOQR_TRACE_UPSTREAM_RESOLVE   15u
#define MOQR_TRACE_AUTH_DECISION      16u
#define MOQR_TRACE_REDIRECT           17u
#define MOQR_TRACE_GOAWAY             18u
#define MOQR_TRACE_MAILBOX_PUSH       19u
#define MOQR_TRACE_MAILBOX_POP        20u
#define MOQR_TRACE_CAPACITY           21u
/* append-only; values are never renumbered or removed */

typedef struct moqr_trace_rec {
    moqr_trace_kind_t kind;
    uint32_t          detail;     /* kind-specific discriminator/reason      */
    uint64_t          shard_seq;  /* per-shard monotonic, replay-stable      */
    moqr_epochs_t     epochs;     /* every record names its generation */
    uint64_t          e0, e1, e2, e3;  /* kind-specific scalars (handles, ids,
                                        * counts); layout per kind is frozen
                                        * once a kind ships */
} moqr_trace_rec_t;

/* -- Per-track log budget --------------------------------------------
 * The log IS the cache: these three bounds are the retention policy, and
 * they are the per-track term of the closed-form capacity model.
 */

typedef struct moqr_log_budget {
    uint32_t max_groups;   /* retained group entries per track (0 = default) */
    uint64_t max_bytes;    /* retained payload+properties bytes   (0 = default) */
    uint64_t max_age_us;   /* retention horizon; 0 = no age limit             */
} moqr_log_budget_t;

/* -- Core configuration (skeleton) ------------------------------------
 * Fixed v1 layout with prefix-safe configuration handling. Every count/byte
 * field below is a term in the capacity ceiling
 * (moqr_capacity_describe): if it isn't bounded here, it doesn't exist.
 */

typedef struct moqr_core_cfg {
    uint32_t           struct_size;
    const moq_alloc_t *alloc;            /* required; per-shard (may be the
                                          * recycling pool allocator) */
    uint16_t           shard_index;      /* this shard within the node      */
    uint16_t           shard_count;      /* local shard set size (v1: 1)    */

    /* Pools (entries). 0 = library default. */
    uint32_t           max_tracks;
    uint32_t           max_cursors;
    uint32_t           max_bindings;
    uint32_t           max_parked;         /* deferred joins + auth tickets */
    uint32_t           max_ns_nodes;       /* namespace trie nodes          */

    /* Copied-borrow storage (the borrow-discipline rule; bytes). 0 = library default. */
    uint32_t           name_intern_bytes;  /* interned namespaces + track names */
    uint32_t           parked_bytes;       /* parked requests incl. deep-copied
                                            * auth token values */

    /* Per-track log defaults (overridable per track class later). */
    moqr_log_budget_t  log_default;

    /* Observability. 0 = library default. */
    uint32_t           trace_ring_records;

    /* Lifecycle policy. */
    uint64_t           linger_us;          /* keep upstream after last cursor */
} moqr_core_cfg_t;

/*
 * Size-aware initializer (the only one — there is no unsized variant to
 * outgrow). Clears and stamps min(cfg_size, sizeof current struct); pass
 * sizeof(moqr_core_cfg_t). Fields left zero select library defaults.
 *
 * Prefix-safe: a non-zero default (alloc, shard_count) is written only when
 * the caller's cfg_size fully contains that field — a caller compiled
 * against an older, smaller layout never takes a write past its own sizeof.
 * Bytes beyond cfg_size are never touched. alloc is required by
 * moqr_core_create, so a cfg_size too small to hold it
 * yields a config that create will reject, never a hidden default.
 */
MOQR_CORE_API void moqr_core_cfg_init_sized(moqr_core_cfg_t *cfg, size_t cfg_size,
                              const moq_alloc_t *alloc);

#ifdef __cplusplus
}
#endif

#endif /* MOQRELAY_TYPES_H */
