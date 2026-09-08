#ifndef MOQRELAY_RELAY_H
#define MOQRELAY_RELAY_H

/*
 * The relay control plane namespace trie, track table with
 * get-or-create/coalescing, subscription lifecycle, and pull-model
 * delivery over the log/cursor primitive in log.h.
 *
 * Shape: same discipline as the session core. Typed advancing calls feed
 * the state machine (each is one deterministic input); OUTPUTS are pulled
 * — control intents via moqr_core_poll_intents (things the binding must
 * do on sessions), deliveries via moqr_core_next_delivery /
 * moqr_core_delivery_done (the pull model: the cursor advances
 * only on a confirmed outcome, so WOULD_BLOCK retains no retry state
 * beyond the cursor itself).
 *
 * Reserve-before-mutate: any call that queues intents checks intent-ring
 * space first and returns MOQR_ERR_WOULD_BLOCK with NO state change; the
 * binding drains intents and retries. Pool exhaustion is
 * MOQR_ERR_CAPACITY (admission refusal, wired to the reject vocabulary).
 *
 * -- Spec anchors (verified against local Specs/, 2026-07-02) --
 *
 *  - Subscription filters are Location predicates
 *    (draft-ietf-moq-transport-18 :2040-2100): Largest Object (0x2)
 *    starts at {LargestGroup, LargestObject+1}; Next Group Start (0x1)
 *    at {LargestGroup+1, 0}; AbsoluteStart (0x3) explicit (MAY be below
 *    largest — serve retained); AbsoluteRange (0x4) adds End Group =
 *    start.group + delta (delta 0 = remainder of start group). Late
 *    objects below the filter start do NOT pass (:2061-2064) — hence a
 *    per-record predicate, not merely a cursor origin.
 *  - Largest Location: largest seen from this publisher's perspective,
 *    updated as objects arrive (:2040-2046, :1950); communicated in
 *    SUBSCRIBE_OK for filter types 0x1/0x2.
 *  - Relays aggregate upstream subscriptions with the Largest Object
 *    filter (:2699) — UPSTREAM_SUBSCRIBE intents carry that filter.
 *  - TRACK_STATUS is handled like SUBSCRIBE without creating
 *    subscription state or sending objects; answered with what
 *    SUBSCRIBE_OK would carry (:4655-4677). policy within the spec's
 *    MAY: answer from ACTIVE/WARM track state, else reject
 *    (the forward-upstream option is future work).
 *  - PUBLISH (push ingest) fields and UNINTERESTED rejection
 *    (:4155-4195).
 *  - Properties/extensions forward opaquely in BOTH drafts (d18 §2.5
 *    :1142-1165; d16 :935, :2159-2169): unsupported properties MUST NOT
 *    be modified, MUST be forwarded, MUST be cached. The log already
 *    retains them opaquely; nothing here inspects them.
 *
 * Thread safety: none — one core per shard, single writer .
 */

#include "auth.h"
#include "wire_codes.h"
#include "log.h"
#include "trace.h"
#include "types.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct moqr_core moqr_core_t;

/* Subscription handle (downstream request bound to a track cursor). */
typedef struct moqr_sub { uint64_t _opaque; } moqr_sub_t;

#ifdef __cplusplus
#define MOQR_SUB_INVALID (moqr_sub_t{0})
#else
#define MOQR_SUB_INVALID ((moqr_sub_t){ 0 })
#endif

MOQR_CORE_API bool moqr_sub_is_valid(moqr_sub_t h);
MOQR_CORE_API bool moqr_sub_eq(moqr_sub_t a, moqr_sub_t b);

/* Namespace / name views (moqr_ns_t) now live in types.h. */

/* -- Subscription filter (spec anchors above) --------------------------- */

typedef uint32_t moqr_filter_type_t;

#define MOQR_FILTER_NEXT_GROUP_START 1u  /* wire 0x1 */
#define MOQR_FILTER_LARGEST_OBJECT   2u  /* wire 0x2 */
#define MOQR_FILTER_ABSOLUTE_START   3u  /* wire 0x3 */
#define MOQR_FILTER_ABSOLUTE_RANGE   4u  /* wire 0x4 */

typedef struct moqr_filter {
    moqr_filter_type_t type;
    uint64_t           start_group;     /* ABSOLUTE_* only                */
    uint64_t           start_object;    /* ABSOLUTE_* only                */
    uint64_t           end_group_delta; /* ABSOLUTE_RANGE only            */
} moqr_filter_t;

/* Group delivery order preference for ranking (v1: DEFAULT == ASC). */
typedef uint32_t moqr_group_order_t;

#define MOQR_GROUP_ORDER_DEFAULT    0u
#define MOQR_GROUP_ORDER_ASCENDING  1u
#define MOQR_GROUP_ORDER_DESCENDING 2u

/* -- Control intents ----------------------------------------------------- *
 * What the binding must perform on its sessions. Drained via
 * moqr_core_poll_intents; the ring is bounded (reserve-before-mutate).
 * Namespace/name views in intents BORROW from core-interned storage and
 * are valid until the next mutating core call — consume within the pump.
 */

typedef uint32_t moqr_intent_kind_t;

#define MOQR_INTENT_ACCEPT_SUB          1u
#define MOQR_INTENT_REJECT_SUB          2u
#define MOQR_INTENT_SUB_DONE            3u  /* range complete / terminated */
#define MOQR_INTENT_UPSTREAM_SUBSCRIBE  4u  /* filter: Largest Object (:2699) */
#define MOQR_INTENT_UPSTREAM_UNSUBSCRIBE 5u
#define MOQR_INTENT_ACCEPT_PUBLISH      6u
#define MOQR_INTENT_REJECT_PUBLISH      7u
#define MOQR_INTENT_NS_FOUND            8u
#define MOQR_INTENT_NS_GONE             9u
#define MOQR_INTENT_TRACK_STATUS_OK    10u
#define MOQR_INTENT_TRACK_STATUS_ERROR 11u
/* 12u retired: request-capacity auto-grant is a per-session flow-control
 * concern the binding owns directly (no core routing state) — it is not a core
 * intent. See bind_maybe_grant_capacity. */

typedef struct moqr_intent {
    moqr_intent_kind_t kind;
    uint64_t           error_code;      /* REJECT_* / *_ERROR (REQUEST_ERROR
                                         * domain; SUB_DONE uses `pd`)     */
    /* SUB_DONE: the PUBLISH_DONE terminal, tagged with the registry it was
     * written in. A status forwarded from an upstream keeps that upstream's
     * draft; a terminal this relay chose is a local meaning. */
    moqr_pd_desc_t     pd;
    uint64_t           cookie;          /* caller correlation (request)    */
    uint64_t           binding_cookie;  /* which session this targets      */
    moqr_sub_t         sub;             /* ACCEPT_SUB / SUB_DONE           */
    moqr_track_t       track;           /* UPSTREAM_* / ACCEPT_PUBLISH     */
    uint64_t           track_gen;       /* upstream identity guard         */
    bool               has_largest;
    uint64_t           largest_group;
    uint64_t           largest_object;
    uint64_t           value;           /* prefix length in parts for
                                         * NS_FOUND / NS_GONE              */
    /* Namespace parts, self-contained so polled intents copy cleanly; the
     * byte spans themselves BORROW core-interned storage (see above).
     *
     * ns_count is the FULL announced Track Namespace part count -- never the
     * suffix length. With `value` (the subscribed prefix length) it is what
     * separates a true ROOT announcement (ns_count 0, value 0) from a non-root
     * namespace equal to its prefix (ns_count N, value N), which merely has an
     * empty wire suffix. Both meanings must survive anywhere an intent is
     * copied or retained: a consumer that rewrites ns_count to the suffix
     * length destroys that distinction. The wire suffix is always
     * ns_parts[value, ns_count). */
    uint32_t           ns_count;
    moq_bytes_t        ns_parts[32];
    moq_bytes_t        name;            /* BORROWED                        */
} moqr_intent_t;

/* Binding-cookie partition for the multi-shard runtime. Real session bindings
 * use cookies in the low range (the binding layer assigns 1..max_conns); the
 * shard manager opens its cross-shard pseudo-bindings with cookies at or above
 * this base, so an intent targeting a pseudo-binding is distinguishable by its
 * cookie alone and can be routed to the manager instead of a session. The core
 * treats binding cookies as opaque; this is a convention shared by the binding
 * layer (its intent router) and the shard manager, not core policy. */
#define MOQR_SHARD_COOKIE_BASE (1ull << 63)

/* -- Configuration -------------------------------------------------------- */

typedef struct moqr_core_relay_cfg {
    uint32_t           struct_size;
    const moq_alloc_t *alloc;          /* required                         */
    moqr_trace_t      *trace;          /* optional; borrowed               */

    /* Pools (0 = default). Every field is a capacity-model term. */
    uint32_t max_bindings;             /* def 64                           */
    uint32_t max_tracks;               /* def 64                           */
    uint32_t max_subs;                 /* def 256                          */
    uint32_t max_ns_nodes;             /* trie nodes, def 256              */
    uint32_t max_ns_subs;              /* namespace subscriptions, def 64  */
    uint32_t max_intents;              /* intent ring, def 64              */
    uint32_t name_intern_bytes;        /* namespaces+names, def 64 KiB     */

    /* Per-track log budgets (every track gets these; see log.h). */
    moqr_log_budget_t log_budget;
    uint32_t          log_max_subgroups;
    uint32_t          log_max_objects_per_group;
    uint32_t          log_max_cursors;

    /* Lifecycle. linger_us: keep upstream after the last subscriber
     * leaves; 0 = unsubscribe immediately at the next tick. */
    uint64_t linger_us;

    /* Authorization hook (control-plane only). NULL = allow-all. Invoked by
     * moqr_core_authorize before the core acts on a control request; the
     * binding translates the session's borrowed tokens into the request. */
    moqr_authorize_fn authorize;
    void             *authorize_ctx;

    /* Deferred-auth parked requests (a DEFER verdict is held here until an
     * async result arrives). max_parked bounds the slot count; parked_bytes
     * bounds the total deep-copied request bytes. 0 = library default. */
    uint32_t max_parked;               /* def 64                           */
    uint32_t parked_bytes;             /* def 64 KiB                       */

    /* Active revalidation grants (a subscribe/announce ALLOW carrying a CAT
     * lease is tracked here and re-checked on tick). Separate lifetime from
     * parked DEFERs. max_grants bounds the slot count; grant_bytes bounds the
     * total deep-copied grant material. 0 = library default. */
    uint32_t max_grants;               /* def 64                           */
    uint32_t grant_bytes;              /* def 64 KiB                       */

    /* Retained-hit FETCH (relay.h fetch API). max_fetches bounds concurrent
     * fetch cursors. fetch_pin_bytes bounds the total payload bytes durably
     * pinned by in-flight fetches (a peeked object is incref-pinned until the
     * write commits, and can survive log eviction) — this keeps the closed-form
     * memory ceiling: the effective value is floored to the per-track log byte
     * budget so a single retained object always fits, guaranteeing forward
     * progress (never an infinite pin wait). 0 = library default. */
    uint32_t max_fetches;              /* def 64                           */
    uint32_t fetch_pin_bytes;          /* def 8 MiB; floored >= log max_bytes */

    /* Shard identity for the multi-shard runtime (single-shard default: index
     * 0, count 1 — bit-identical to a build that never sets these). The core
     * packs shard_index + 1 into every handle it mints and refuses handles
     * carrying any other shard tag, so cross-shard handle misuse is caught
     * structurally. Requires shard_index < shard_count. */
    uint16_t shard_index;              /* def 0                            */
    uint16_t shard_count;              /* def 1 (0 is treated as 1)        */

    /* Pending publisher-cancel slots for force-withdraw (a bounded queue merged
     * into moqr_core_peek_revoked_grants). A full queue is transient backpressure
     * (WOULD_BLOCK), not a refusal. 0 = default (max_bindings). */
    uint32_t max_cancels;              /* def max_bindings                 */

    /* OPEN-record chunk-node pool for every per-track log. 0 = log default
     * (8192). This bounds chunk-through retained records and the relay's
     * per-binding chunk-pin array without changing retention semantics. */
    uint32_t log_max_chunk_nodes;       /* def 8192                         */
} moqr_core_relay_cfg_t;

MOQR_CORE_API void moqr_core_relay_cfg_init_sized(moqr_core_relay_cfg_t *cfg,
                                    size_t cfg_size,
                                    const moq_alloc_t *alloc);

/* -- Lifecycle -------------------------------------------------------------- */

MOQR_CORE_API moqr_result_t moqr_core_create(const moqr_core_relay_cfg_t *cfg,
                               moqr_core_t **out);
MOQR_CORE_API void moqr_core_destroy(moqr_core_t *core);

/* Advance virtual time: fires linger deadlines, ages logs. */
MOQR_CORE_API moqr_result_t moqr_core_tick(moqr_core_t *core, uint64_t now_us);

/* Set the control-plane epoch generations (node + shard) that stamp trace
 * records and route dumps. A control input (§21.2): the shard manager sets
 * these from membership / config-generation changes. They are preserved across
 * route mutations (route_epoch, the per-mutation counter, is core-owned and not
 * settable here). Single-shard cores leave both 0. */
MOQR_CORE_API void moqr_core_set_epochs(moqr_core_t *core, uint64_t node_epoch,
                          uint64_t shard_epoch);

/* -- Bindings (one per session attachment) ---------------------------------- */

MOQR_CORE_API moqr_result_t moqr_core_binding_open(moqr_core_t *core,
                                     uint64_t session_cookie,
                                     moqr_binding_t *out);

/* Retires everything the binding owns: subscriptions (cursors),
 * announcements (with NS_GONE fan-out), namespace subscriptions, pending
 * publishes. Tracks whose upstream was this binding become WARM and keep
 * serving retained content. Queues no intents targeting the closed
 * binding. */
MOQR_CORE_API moqr_result_t moqr_core_binding_close(moqr_core_t *core, moqr_binding_t b,
                                      uint64_t now_us);

/* -- Authorization (control-plane) ------------------------------------------- *
 * Run the configured auth hook (allow-all if none) for `req`, emit an
 * AUTH_DECISION trace, and count the decision. Pure w.r.t. the request; fills
 * *out. The binding builds `req` from a control event's borrowed tokens and
 * acts on the verdict: ALLOW proceeds, DENY rejects with req's error_code
 * (setup DENY closes the session). A request-path DEFER is parked and resolved
 * later (moqr_core_park + moqr_bind_auth_resolve); a setup DEFER is NOT parked
 * — it hard-closes like a setup DENY. Deterministic and wall-clock-free; the
 * hook must use req->now_us. */
MOQR_CORE_API void moqr_core_authorize(moqr_core_t *core, const moqr_auth_request_t *req,
                         moqr_auth_verdict_t *out);

/* -- Deferred (DEFER) auth: bounded parked-request storage ------------------- *
 * On a DEFER verdict the binding parks the request here (deep-copied) keyed by
 * the hook's external ticket, then resumes or rejects it once an async result
 * arrives. Setup DEFER is NOT parked (it stays a hard-close). The op material a
 * resume needs is the common fields for every action plus SUBSCRIBE-only filter
 * params. Bounded by cfg.max_parked (slots) and cfg.parked_bytes (copied
 * bytes). Everything referenced is BORROWED on park (deep-copied by the core);
 * on begin_resolve the view points at stable per-entry storage. */
typedef struct moqr_park_req {
    moqr_auth_action_t       action;
    uint64_t                 binding_cookie; /* owner connection identity      */
    uint64_t                 session_cookie; /* wire request handle _opaque    */
    moqr_ns_t                ns;             /* namespace / prefix             */
    moq_bytes_t              name;           /* track name; empty when n/a     */
    const moqr_auth_token_t *tokens;         /* deep-copied; NULL if none      */
    size_t                   token_count;
    /* SUBSCRIBE op params (ignored for other actions). */
    uint32_t                 sub_filter_type;
    uint64_t                 sub_start_group;
    uint64_t                 sub_start_object;
    uint64_t                 sub_end_group_delta;
    uint8_t                  sub_priority;
    uint32_t                 sub_group_order;
} moqr_park_req_t;

/* Park a DEFERred request under external ticket `ext_ticket` (nonzero). Deep-
 * copies req->ns/name/tokens + scalars into bounded storage. Validates the
 * borrowed view first and fails closed with no state change: MOQR_ERR_INVAL
 * (ticket 0, or a positive count/len with a NULL pointer, or a length sum that
 * overflows), MOQR_ERR_STALE_HANDLE (duplicate LIVE ticket — never overwrite/
 * merge), MOQR_ERR_CAPACITY (max_parked or parked_bytes exhausted),
 * MOQR_ERR_NOMEM.
 *
 * Ticket ownership: the verifier allocates tickets and MUST keep `ext_ticket`
 * unique for the relay's lifetime. Resolution keys on the ticket alone, so
 * reusing one after its original was finished/retired would resolve a DIFFERENT
 * request (an ABA the core does not, and for a bounded scheme cannot, detect).
 * The duplicate-LIVE-ticket check only guards against two concurrently-parked
 * requests sharing a ticket. */
MOQR_CORE_API moqr_result_t moqr_core_park(moqr_core_t *core, const moqr_park_req_t *req,
                             uint64_t ext_ticket, uint64_t now_us);

/* Begin resolving a parked ticket. `result` is the async verifier's outcome
 * (decision + reason + wire error_code + lease). Validates the ticket (0 /
 * unknown / retired / already-resolving -> MOQR_ERR_STALE_HANDLE, no mutation).
 * Otherwise clamps result->decision to ALLOW/DENY (DEFER or garbage -> DENY),
 * counts the FINAL decision + emits AUTH_DECISION, PINS the entry, writes the
 * CANONICAL verdict to *verdict (fail-closed to DENY on any non-OK return;
 * carries the reject error_code on DENY so the binding rejects with the same
 * wire rules as a synchronous denial, and the lease on ALLOW), and fills *view
 * with pointers
 * into stable per-entry storage valid until moqr_core_auth_finish_resolve. The
 * binding MUST branch on verdict->decision (never its own inputs): ALLOW
 * resumes, DENY rejects; then it finishes. `verdict` may be NULL only if the
 * caller does not need the canonical result; `view` is required. */
MOQR_CORE_API moqr_result_t moqr_core_auth_begin_resolve(moqr_core_t *core,
                                           uint64_t ext_ticket,
                                           const moqr_auth_verdict_t *result,
                                           uint64_t now_us,
                                           moqr_auth_verdict_t *verdict,
                                           moqr_park_req_t *view);

/* Finish a resolve started by begin_resolve: unpin + free the entry. No-op if
 * the ticket is not currently pinned. After this the external ticket is no
 * longer live, so a later begin_resolve of it is a stale no-op — provided the
 * verifier honors the relay-lifetime-unique ticket contract (see park). */
MOQR_CORE_API void moqr_core_auth_finish_resolve(moqr_core_t *core, uint64_t ext_ticket);

/* Retire every parked request owned by `binding_cookie` (called from binding
 * close): frees copied bytes and invalidates those tickets. */
MOQR_CORE_API void moqr_core_retire_parked(moqr_core_t *core, uint64_t binding_cookie);

/* -- Revalidation grants (CAT moqt-reval lease re-check) --------------------- *
 * When an initial ALLOW for a long-lived op carries revalidate_after_us > 0,
 * the binding creates a grant AFTER the op succeeds. The core deep-copies the
 * request material and schedules a recheck at now_us + lease; on tick the hook
 * is re-invoked and the grant is rescheduled (ALLOW) or revoked (DENY). Only
 * SUBSCRIBE and PUBLISH_NAMESPACE are grantable. */

/* Grant creation is two-step so capacity is reserved BEFORE the subscribe/
 * announce mutates core state: reserve (may fail) -> run the op -> commit (on
 * success, no-fail) or abort (on op failure). This makes a grant-storage
 * exhaustion a clean CAT rejection with no half-active state. */
typedef struct moqr_grant_res {
    uint32_t slot;
    uint32_t gen; /* reserved slot's generation; 0 = invalid reservation */
} moqr_grant_res_t;

/* Reserve a grant for an op the hook ALLOWed with a lease, BEFORE running it.
 * `req` carries action / binding_cookie / session_cookie (wire op handle) / ns
 * / name / tokens; `lease_us` is the verdict's revalidate_after_us (> 0). Deep-
 * copies the material into bounded storage and schedules now_us + lease_us
 * (wrap-guarded). Fails closed with no state change: MOQR_ERR_INVAL (lease 0 /
 * malformed), MOQR_ERR_UNSUPPORTED (action not grantable), MOQR_ERR_CAPACITY
 * (max_grants / grant_bytes), NOMEM — and the caller MUST then fail the op
 * closed (CAT: a recipient that cannot track a revalidatable token rejects it).
 * On success *res_out identifies the reservation for commit/abort. */
MOQR_CORE_API moqr_result_t moqr_core_grant_reserve(moqr_core_t *core,
                                      const moqr_park_req_t *req,
                                      uint64_t lease_us, uint64_t now_us,
                                      moqr_grant_res_t *res_out);

/* Commit a reservation once the op succeeded, recording the core moqr_sub_t
 * (`sub_raw`; 0 for announce). Now eligible for revalidation on tick. No-fail
 * except MOQR_ERR_STALE_HANDLE (not a live, uncommitted reservation) or
 * MOQR_ERR_INVAL (a SUBSCRIBE grant committed with sub_raw == 0 — no handle to
 * revoke with) — both caller bugs. */
MOQR_CORE_API moqr_result_t moqr_core_grant_commit(moqr_core_t *core, moqr_grant_res_t res,
                                     uint64_t sub_raw);

/* Abort a reservation whose op failed: frees the copied material. No-op if the
 * reservation is stale. */
MOQR_CORE_API void moqr_core_grant_abort(moqr_core_t *core, moqr_grant_res_t res);

/* Retire every grant owned by `binding_cookie` (called from binding close):
 * frees copied bytes; a later scheduled recheck can no longer touch it. */
MOQR_CORE_API void moqr_core_retire_grants(moqr_core_t *core, uint64_t binding_cookie);

/* An announce grant revoked on revalidation whose NS_GONE fan-out is done but
 * whose publisher-side cancel the binding must still send. */
typedef struct moqr_revoked_grant {
    uint64_t binding_cookie; /* which connection                            */
    uint64_t session_cookie; /* the announce (wire) handle _opaque to cancel */
    uint64_t error_code;     /* denial code for cancel_namespace cfg.error_code
                              * (custom DENY code, else UNAUTHORIZED; never 0) */
} moqr_revoked_grant_t;

/* Peek announce revocations pending a publisher-side cancel (moqr_core_tick
 * already cleared the relay's namespace state and fanned NS_GONE to watchers).
 * The binding sends moq_session_cancel_namespace for each. Subscribe
 * revocations are fully core-driven (SUB_DONE) and never appear here. Peek is
 * NON-draining: an entry keeps being returned until the binding acks it, so a
 * cancel that returns WOULD_BLOCK is simply retried on the next pump (the grant
 * is the durable record, bounded by max_grants — the binding needs no queue of
 * its own). Returns the count written (<= max). Call after moqr_core_tick. */
MOQR_CORE_API size_t moqr_core_peek_revoked_grants(moqr_core_t *core,
                                     moqr_revoked_grant_t *out, size_t max);

/* Ack (free) the revoked grant for (binding_cookie, session_cookie) once its
 * publisher-side cancel has been sent (or is moot — the connection is gone).
 * The pair uniquely identifies a revoked announce; a no-match is a no-op. */
MOQR_CORE_API void moqr_core_ack_revoked_grant(moqr_core_t *core, uint64_t binding_cookie,
                                 uint64_t session_cookie);

/* -- Announce / namespace discovery ------------------------------------------ */

MOQR_CORE_API moqr_result_t moqr_core_announce(moqr_core_t *core, moqr_binding_t b,
                                 moqr_ns_t ns);
/* Like moqr_core_announce, but records the peer's wire announce handle
 * (session_cookie) on the node so force-withdraw can wire-cancel the publisher.
 * Real-publisher announces pass their ann_raw; mirror announces pass 0. */
MOQR_CORE_API moqr_result_t moqr_core_announce_ex(moqr_core_t *core, moqr_binding_t b,
                                    moqr_ns_t ns, uint64_t session_cookie);
MOQR_CORE_API moqr_result_t moqr_core_unannounce(moqr_core_t *core, moqr_binding_t b,
                                   moqr_ns_t ns);

/* Relay-topology force-withdraw of an announced namespace (e.g. a split-brain
 * loser): purge every track the announce sourced (per-state terminals, fetch
 * invalidation, slot free), clear the announce + fan NS_GONE, and queue a
 * publisher cancel_namespace (peekable via peek_revoked_grants).
 * request_error is a REQUEST_ERROR-registry code; it reaches exactly the two
 * REQUEST_ERROR-domain messages (parked REJECT_SUB, publisher cancel). Active
 * subscribers are terminated with PUBLISH_DONE TRACK_ENDED — a distinct wire
 * registry never fed by this parameter.
 * RESUMABLE, not atomic. The purge advances one track at a time, and each
 * completed track is DURABLE: its terminals are queued, its slot is freed, and
 * a retry will not produce them again. A short intent ring or a full cancel
 * queue therefore returns MOQR_ERR_WOULD_BLOCK with PARTIAL PROGRESS ALREADY
 * COMMITTED — the caller must drain the intents and call again until it gets
 * MOQR_OK, exactly like binding_close.
 *
 * The one narrower guarantee that does hold: if there is not room for the
 * FIRST track unit, the call returns WOULD_BLOCK before mutating anything.
 *
 * The FINAL namespace effects — clearing the announce, the NS_GONE fan-out and
 * the publisher cancel — happen only after every track unit has completed, so
 * the namespace never stops existing while a track it sourced still stands.
 * Until then the announce is still installed and a caller must not treat the
 * namespace as withdrawn.
 *
 * No-op MOQR_OK on a missing / no-announce / mirror-owned node, so repeating a
 * completed withdrawal is idempotent. Unlike unannounce, it does NOT require
 * the announcer's own handle. */
MOQR_CORE_API moqr_result_t moqr_core_force_withdraw(moqr_core_t *core, moqr_ns_t ns,
                                       uint64_t request_error,
                                       uint64_t now_us);

/* Prefix subscription: NS_FOUND intents for current + future matches,
 * NS_GONE on withdrawal. cookie correlates the binding's wire handle. */
MOQR_CORE_API moqr_result_t moqr_core_ns_subscribe(moqr_core_t *core, moqr_binding_t b,
                                     moqr_ns_t prefix, uint64_t cookie);
MOQR_CORE_API moqr_result_t moqr_core_ns_unsubscribe(moqr_core_t *core, moqr_binding_t b,
                                       uint64_t cookie);

/* -- Downstream subscribe ------------------------------------------------------ */

typedef struct moqr_subscribe_req {
    uint32_t           struct_size;
    moqr_ns_t          ns;             /* borrowed for the call            */
    moq_bytes_t        name;           /* borrowed for the call            */
    moqr_filter_t      filter;
    uint8_t            subscriber_priority;   /* lower = higher (:2341)    */
    moqr_group_order_t group_order;
    uint64_t           cookie;         /* echoed on ACCEPT/REJECT          */
    /* Initial downstream Forward state (draft §10.2.12): true = deliver,
     * false = established but object transmission suppressed until a later
     * SUBSCRIBE_UPDATE(Forward=1). moqr_subscribe_req_init defaults it true;
     * the binding carries the request's forward bit here so a subscription
     * created paused is never scheduled (and so never retired for it). */
    bool               forward;
} moqr_subscribe_req_t;

MOQR_CORE_API void moqr_subscribe_req_init(moqr_subscribe_req_t *req);

/*
 * Get-or-create + coalescing (REGISTRY semantics):
 *  - Track ACTIVE/WARM: ACCEPT_SUB intent immediately (largest from track
 *    state); retained content serves at once; WARM re-issues
 *    UPSTREAM_SUBSCRIBE.
 *  - No track: longest-prefix announced publisher found in the trie →
 *    track PENDING_UPSTREAM, ONE UPSTREAM_SUBSCRIBE intent, the sub parks.
 *  - Track PENDING: the sub parks (no duplicate upstream intent).
 *  - No publisher: REJECT_SUB intent (DOES_NOT_EXIST-shaped error_code).
 */
MOQR_CORE_API moqr_result_t moqr_core_subscribe(moqr_core_t *core, moqr_binding_t b,
                                  const moqr_subscribe_req_t *req,
                                  moqr_sub_t *out);

MOQR_CORE_API moqr_result_t moqr_core_unsubscribe(moqr_core_t *core, moqr_sub_t sub,
                                    uint64_t now_us);

/*
 * Set a live subscription's downstream Forward state (draft §10.2.12: a
 * SUBSCRIBE_UPDATE Forward change). Forward=0 PAUSES ordinary object
 * transmission for this subscription only — its records are never selected
 * for delivery, range-completion retirement is held, and any held outstanding
 * RECORD delivery of its own is released (unpinned, cursor NOT advanced) so
 * the binding keeps serving its siblings and nothing is written to the paused
 * stream (which would draw MOQ_ERR_WRONG_STATE and wrongly retire it).
 * Required stream MAINTENANCE is NOT suppressed while paused: a pending SEAL
 * close, an eviction watermark, and the begun-evicted reset sweep stay
 * eligible (and a held maintenance delivery is NOT dropped) so a downstream
 * FIN/RESET is never deferred to resume, retaining subgroup slots against the
 * sibling subscriptions. It does NOT terminate the subscription: control
 * (unsubscribe, SUB_DONE, force-withdraw) stays functional. Forward=1
 * RESUMES: re-arms the owning binding so pending eligible work (including the
 * released delivery, re-derived from the unadvanced cursor) is reconsidered at
 * once — resume delivers exactly once. Idempotent: setting the current value
 * is a no-op (no spurious re-arm). MOQR_ERR_STALE_HANDLE for a
 * retired/unknown/foreign sub (fail closed).
 */
MOQR_CORE_API moqr_result_t moqr_core_sub_set_forward(moqr_core_t *core, moqr_sub_t sub,
                                        bool forward, uint64_t now_us);

/*
 * Checkpoint the exact absolute chunk count the bind has emitted downstream for
 * the binding's current outstanding CHUNKED delivery, WITHOUT advancing the
 * cursor or releasing the hold. The bind calls this each time it holds a
 * chunked write on WOULD_BLOCK, so the core's begun-downstream watermark always
 * reflects true progress — not the STALLED-only assumption that the whole
 * pinned batch shipped. This is what makes releasing a held partial safe under
 * a Forward pause: a later abandon/eviction of a begun-but-unfinished object
 * then correctly surfaces its required reset instead of skipping it as
 * never-begun. The call marks the object BEGUN downstream regardless of count:
 * a begin_object that shipped the header but zero chunks (emitted==0) is still
 * begun and must reset if abandoned/evicted, and begun is tracked separately
 * from the count so re-derivation never skips chunk 0. `emitted` is the count
 * of fully-written chunks and MUST be 0..chunk_count of the outstanding record;
 * an out-of-range value is rejected MOQR_ERR_INVAL with state unchanged. No-op
 * MOQR_OK when the binding has no outstanding chunked delivery.
 * MOQR_ERR_STALE_HANDLE for an unknown binding.
 */
MOQR_CORE_API moqr_result_t moqr_core_delivery_note_emitted(moqr_core_t *core,
                                              moqr_binding_t binding,
                                              uint32_t emitted, uint64_t now_us);

/*
 * The track a live subscription is attached to: its handle plus the current
 * track generation (valid for gen-guarded calls until the track's identity
 * next changes). STALE_HANDLE once the subscription is retired. Lets a
 * consumer that only holds the subscription — a shard manager's pump-sub —
 * reach the track-level upstream primitives (upstream_cancel, source_done).
 */
MOQR_CORE_API moqr_result_t moqr_core_sub_track(moqr_core_t *core, moqr_sub_t sub,
                                  moqr_track_t *track, uint64_t *track_gen);

/* A live subscription's delivery state: PARKED (awaiting upstream
 * resolution) or ACTIVE (serving from a track log). */
typedef enum {
    MOQR_SUB_PARKED,
    MOQR_SUB_ACTIVE
} moqr_sub_state_t;

/*
 * Read a subscription's state without touching it. Generation-safe: a
 * retired, foreign, or never-issued handle is MOQR_ERR_STALE_HANDLE (the
 * handle carries its pool generation), never a stale value. Pure — no
 * mutation, no intents — so an observability layer can classify its own
 * pump-subs (a stale handle maps to "gone": in neither state).
 */
MOQR_CORE_API moqr_result_t moqr_core_sub_state(const moqr_core_t *core, moqr_sub_t sub,
                                  moqr_sub_state_t *out);

/* Relay-initiated subscription termination (e.g. an auth grant revalidated to
 * DENY). Unlike moqr_core_unsubscribe (a silent local retire), this reserves
 * intent space, queues MOQR_INTENT_SUB_DONE carrying `status` (the
 * SUBSCRIBE_DONE status_code the binding forwards via moq_session_done_subscribe),
 * then retires the sub — so the subscriber is told on the wire. Returns
 * MOQR_ERR_STALE_HANDLE (unknown sub) or MOQR_ERR_WOULD_BLOCK (intent ring
 * full; retry after a drain). */
MOQR_CORE_API moqr_result_t moqr_core_revoke_sub(moqr_core_t *core, moqr_sub_t sub,
                                   moqr_pd_desc_t status, uint64_t now_us);

/* -- Upstream resolution (answers an UPSTREAM_SUBSCRIBE intent) ---------------- */

MOQR_CORE_API moqr_result_t moqr_core_upstream_ok(moqr_core_t *core, moqr_track_t track,
                                    uint64_t track_gen,
                                    uint64_t upstream_cookie,
                                    bool has_largest,
                                    uint64_t largest_group,
                                    uint64_t largest_object);

/* now_us is the deterministic clock: rejecting the parked subscribers retires
 * them, and a retire may arm the warm-track linger deadline (now_us +
 * linger_us). */
MOQR_CORE_API moqr_result_t moqr_core_upstream_error(moqr_core_t *core, moqr_track_t track,
                                       uint64_t track_gen,
                                       uint64_t error_code, uint64_t now_us);

/*
 * Retire a PENDING track's outstanding upstream attempt once nothing wants
 * it: valid ONLY on a live PENDING track with ZERO subscribers of ANY state
 * (a WARM rejoin accepts its subscriber and only then re-enters PENDING, so
 * "no parked subs" alone would not be safe). Emits exactly one
 * UPSTREAM_UNSUBSCRIBE toward the track's upstream binding — carrying the
 * track handle, generation, and the stored upstream cookie — preflighted:
 * MOQR_ERR_WOULD_BLOCK with ZERO mutation when the intent ring is full
 * (retry later). On OK the track identity is bumped (a late upstream answer
 * is STALE_HANDLE) and the release mirrors the source-release rules: with no
 * retained records and no open fetches the slot is freed; otherwise the
 * track transitions to WARM, preserving retained content and fetches.
 * WRONG_STATE when the track is not PENDING or any subscriber remains;
 * STALE_HANDLE via the usual generation guard.
 */
MOQR_CORE_API moqr_result_t moqr_core_upstream_cancel(moqr_core_t *core, moqr_track_t track,
                                        uint64_t track_gen, uint64_t now_us);

/* -- Push ingest (PUBLISH, :4155) ---------------------------------------------- */

MOQR_CORE_API moqr_result_t moqr_core_publish_open(moqr_core_t *core, moqr_binding_t b,
                                     moqr_ns_t ns, moq_bytes_t name,
                                     uint64_t cookie, moqr_track_t *out);

/* -- Object ingest --------------------------------------------------------------- *
 * track is the handle from UPSTREAM_SUBSCRIBE/ACCEPT_PUBLISH intents.
 * Ownership of payload/properties transfers on MOQR_OK (log semantics);
 * refusals leave the caller's refs untouched. MOQR_ERR_TOO_OLD and
 * ordering INVALs surface for the binding to count/escalate. */
MOQR_CORE_API moqr_result_t moqr_core_ingest(moqr_core_t *core, moqr_track_t track,
                               const moqr_log_append_desc_t *desc);

/* Chunk-through ingest: a NORMAL object may be admitted incrementally.
 * moqr_core_ingest with desc->obj_state == MOQR_OBJ_OPEN opens the record (does
 * NOT advance the track largest); then append its payload slices and either
 * complete it (advances largest) or abandon it. All address the OPEN record by
 * (group, subgroup, object) -- the identity every OBJECT_CHUNK carries. Errors
 * mirror the log's OPEN-record APIs (STALE_HANDLE / WRONG_STATE / TOO_OLD /
 * INVAL / CAPACITY). The log increfs `chunk`; the caller keeps its ref. */
MOQR_CORE_API moqr_result_t moqr_core_append_chunk(moqr_core_t *core, moqr_track_t track,
                                     uint64_t group_id, uint64_t subgroup_id,
                                     uint64_t object_id, moq_rcbuf_t *chunk);
MOQR_CORE_API moqr_result_t moqr_core_complete_record(moqr_core_t *core, moqr_track_t track,
                                        uint64_t group_id, uint64_t subgroup_id,
                                        uint64_t object_id);
/* Mark an OPEN record ABANDONED, storing reset_code as the terminal cause the
 * relay forwards downstream via moq_session_reset_subgroup. A genuine upstream
 * RESET passes the publisher's code; terminals with no peer code use a
 * relay-defined MOQR_RESET_CODE_* below. */
MOQR_CORE_API moqr_result_t moqr_core_abandon_record(moqr_core_t *core, moqr_track_t track,
                                       uint64_t group_id, uint64_t subgroup_id,
                                       uint64_t object_id,
                                       moqr_reset_desc_t reset);

/* Relay-defined subgroup-reset codes for terminals that carry no peer reset
 * code (moq_session_reset_subgroup takes a u64 application code). A genuine
 * upstream RESET forwards the publisher's code instead of these. */
#define MOQR_RESET_CODE_EVICTED  0x1001u  /* truncated by capacity eviction   */
#define MOQR_RESET_CODE_STOPPED  0x1002u  /* upstream STOP terminal, no code   */

/* Seal a subgroup on `track`: the upstream subgroup stream (group_id,
 * subgroup_id) FIN'd, so no more objects will arrive for it. Each ACTIVE
 * subscription is told exactly once, via an acknowledged SEAL notice from
 * moqr_core_next_delivery (see MOQR_DELIVERY_NOTICE_*) that becomes
 * eligible once the sub has consumed the subgroup's last retained record —
 * whether the seal landed before or after that record was delivered (the
 * plain-FIN counterpart to EOG, and the durable one: the notice's close
 * retries under WOULD_BLOCK). Records still carry an advisory
 * moqr_delivery_t.subgroup_end. No allocation, no intents; idempotent.
 * MOQR_OK when sealed, MOQR_DONE when the subgroup is gone (evicted / never
 * seen / empty-header FIN), MOQR_ERR_STALE_HANDLE on a dead track. */
MOQR_CORE_API moqr_result_t moqr_core_seal_subgroup(moqr_core_t *core, moqr_track_t track,
                                      uint64_t group_id, uint64_t subgroup_id);

/* Terminate a subgroup on `track` abnormally: the upstream subgroup stream
 * (group_id, subgroup_id) was RESET with `reset_code`, so no more objects
 * will arrive for it — the reset-flavored counterpart of the seal, for
 * whole-object ingest where the dropped partial leaves no OPEN record to
 * abandon. Retained COMPLETE records stay servable and are delivered first:
 * each ACTIVE subscription is told through the same acknowledged notice as
 * the seal (delivered once it has consumed the subgroup's last retained
 * record), whose close resets the downstream subgroup with `reset_code`
 * instead of FINing it. Never resolves header-EOG finality. Same results
 * as the seal: MOQR_OK / MOQR_DONE / MOQR_ERR_STALE_HANDLE; idempotent,
 * first terminal wins. */
MOQR_CORE_API moqr_result_t moqr_core_reset_subgroup(moqr_core_t *core, moqr_track_t track,
                                       uint64_t group_id,
                                       uint64_t subgroup_id,
                                       moqr_reset_desc_t reset);

/* Apply an upstream eviction watermark to `track`'s journal (see
 * moqr_log_note_evicted_below): groups below `oldest_retained` become
 * authoritatively closed, local survivors below it are evicted through the
 * ordinary path, and the existing per-subscription eviction machinery
 * (position reclaim, EVICT_WATERMARK notices, downstream closes) does the
 * rest on the following delivery scans. Idempotent and monotonic.
 * MOQR_ERR_STALE_HANDLE on a dead track. */
MOQR_CORE_API moqr_result_t moqr_core_note_track_evicted_below(moqr_core_t *core,
                                                 moqr_track_t track,
                                                 uint64_t oldest_retained);

/* Abandon a dead group's OPEN-tail records in `track`'s journal (see
 * moqr_log_abandon_group_open — defensive: tracked slots normally cover
 * every partial). Idempotent; MOQR_ERR_STALE_HANDLE on a dead track. */
MOQR_CORE_API moqr_result_t moqr_core_abandon_group_open(moqr_core_t *core,
                                           moqr_track_t track,
                                           uint64_t group_id,
                                           moqr_reset_desc_t reset);

/* -- Track status (:4655) ---------------------------------------------------------- */

MOQR_CORE_API moqr_result_t moqr_core_track_status(moqr_core_t *core, moqr_binding_t b,
                                     moqr_ns_t ns, moq_bytes_t name,
                                     uint64_t cookie);

/* -- Pull-model delivery (pull model) -------------------------------------------------- */

/* Recordless acknowledged notifications, surfaced through the same pull as
 * record deliveries (out->notice != NONE; nothing payload-shaped is set).
 * A notice IS the binding's outstanding delivery: confirm it with
 * delivery_done — DELIVERED acknowledges it (the core clears the underlying
 * pending state only then), WOULD_BLOCK holds it for an idempotent re-peek.
 * A notice precedes record work for the AFFECTED subscription only and
 * enters the ordinary cross-subscription schedule: subscriber priority
 * first, a notice outranking a record at EQUAL priority, notices ordering
 * among themselves by priority, then lowest subscription slot, then group,
 * then lowest wire subgroup id.
 *  - SEAL: this sub has consumed every retained record of the sealed
 *    (rec.group_id, rec.subgroup_id) — close the downstream subgroup stream
 *    now and acknowledge. This is the ONLY close path for a FIN'd subgroup
 *    (records carry subgroup_end as advisory metadata): the close is
 *    retryable under WOULD_BLOCK here, where a close piggybacked on a
 *    record delivery would be swallowed by the record's acknowledgement.
 *  - EVICT_WATERMARK: an eviction jumped this sub's positions; oldest_group
 *    is the track's oldest RETAINED group. Any open downstream subgroup of a
 *    group BELOW it is evicted (no more records ever) and must be closed;
 *    groups at/above it are live. An unacknowledged watermark blocks record
 *    selection for this sub only — never for its binding siblings. */
#define MOQR_DELIVERY_NOTICE_NONE            0u
#define MOQR_DELIVERY_NOTICE_SEAL            1u
#define MOQR_DELIVERY_NOTICE_EVICT_WATERMARK 2u

typedef struct moqr_delivery {
    moqr_sub_t         sub;
    uint64_t           sub_cookie;      /* the subscribe cookie             */
    moqr_record_view_t rec;             /* BORROWED (log borrow rules)      */
    bool               skipped;         /* always false — superseded by the
                                         * EVICT_WATERMARK notice; retained
                                         * in the fixed v1 layout           */
    bool               subgroup_end;    /* last record of a SEALED subgroup
                                         * (advisory on records — the SEAL
                                         * notice is the durable close
                                         * signal; set on the notice too)   */
    uint64_t           oldest_group;    /* EVICT_WATERMARK notice payload
                                         * (see above); UINT64_MAX when the
                                         * log is empty                     */
    bool               evicted_reset;   /* obj_state==ABANDONED + this set: the
                                         * whole group (rec.group_id) was evicted
                                         * while begun downstream — the binding
                                         * must RESET every begun subgroup of the
                                         * group (the records are gone, so there
                                         * is no per-object identity to match)  */
    uint8_t            notice;          /* MOQR_DELIVERY_NOTICE_*; when set,
                                         * rec carries only the addressing
                                         * fields the notice names          */
    bool               seal_reset;      /* SEAL notice flavor: the subgroup's
                                         * terminal was an upstream RESET —
                                         * close downstream via
                                         * moq_session_reset_subgroup with
                                         * seal_reset_code, never a clean
                                         * FIN (subgroup_end is false)      */
    moqr_reset_desc_t  seal_reset_desc;  /* the upstream RESET cause, tagged
                                          * with the registry it arrived in */
} moqr_delivery_t;

typedef uint32_t moqr_delivery_outcome_t;

#define MOQR_DELIVERY_DELIVERED    1u
#define MOQR_DELIVERY_WOULD_BLOCK  2u   /* cursor holds; retry later        */
#define MOQR_DELIVERY_STREAM_ERROR 3u   /* terminal for this subscription   */
/* A backpressure refusal that provably preceded ANY downstream write for the
 * outstanding RECORD — the binding's subgroup-slot pool refused before the
 * downstream object was opened, so there is no chunk cursor to preserve.
 * Identical to WOULD_BLOCK for a whole record; for a CHUNKED record the core
 * RELEASES the outstanding delivery (re-derived on a later selection) instead
 * of holding it — a held pre-begin record would keep occupying the binding's
 * one outstanding-delivery slot and thereby starve the SEAL notices whose
 * closes free the very subgroup slot it is waiting for. Only records: a
 * notice or an abandoned-reset has no pre-begin refusal site (INVAL). The
 * core also fail-closes the claim itself where it is provably false: a
 * chunked delivery whose pinned batch resumes at a nonzero chunk index
 * already began downstream in an earlier batch, and confirming it
 * REFUSED_UNBEGUN is INVAL (a release there could re-deliver chunks). */
#define MOQR_DELIVERY_REFUSED_UNBEGUN 6u
/* Live-edge stall: every chunk currently exposed by an OPEN record was
 * written downstream but it is not COMPLETE yet, so no end_object was sent. The
 * core records the per-sub emitted watermark, releases the outstanding delivery
 * and its pins (does NOT hold like WOULD_BLOCK), does NOT advance the record
 * cursor, and does NOT count a delivery. The head is not re-selected until it
 * grows past the watermark or flips COMPLETE/ABANDONED — so one publisher-
 * stalled object never head-of-line-blocks other subscriptions on the binding. */
#define MOQR_DELIVERY_STALLED      4u
/* Abandoned after a downstream object was begun: the bind has reset the
 * downstream subgroup (moq_session_reset_subgroup); the core advances past the
 * abandoned head WITHOUT counting a delivery. (An ABANDONED head with nothing
 * begun downstream is skipped internally and never surfaces this outcome.) */
#define MOQR_DELIVERY_ABANDONED    5u

/*
 * Peek the highest-ranked deliverable record across the binding's ACTIVE
 * subscriptions. Per subscription, the schedulable set is the head of
 * every retained (group, subgroup) stream plus the datagram list, ranked
 * per spec §7.1.1: publisher priority (lower = higher), then the sub's
 * group order, then lowest subgroup, then arrival. Across subscriptions:
 * subscriber priority first, then the same fields with ascending groups
 * (cross-request order past the priorities is implementation-dependent
 * per the spec; ties resolve deterministically). Datagram-preference
 * candidates rank after subgroup candidates at equal priority and are
 * delivered in arrival order (datagrams carry no ordering obligation;
 * the rule-4 lowest-object tie-break is approximated). Records failing a
 * sub's filter predicate are consumed internally (never deliverable).
 *
 * Returns MOQR_OK with *out filled (exactly one outstanding delivery per
 * binding — confirm it via moqr_core_delivery_done before the next call,
 * else MOQR_ERR_WRONG_STATE); MOQR_DONE when nothing is deliverable. A
 * recordless NOTICE (out->notice != NONE, defined above the struct) is a
 * delivery like any other: it precedes record work, holds verbatim across a
 * WOULD_BLOCK, is consumed only by a DELIVERED acknowledgement, and admits
 * no STALLED/ABANDONED outcome (MOQR_ERR_INVAL).
 *
 * COMPLETE records are served whole-object (rec.chunk_count == 0, payload in
 * rec.payload) or chunked (rec.chunk_count > 0, payload NULL — read the chunks
 * in order via moqr_core_delivery_chunk, and stream them downstream with
 * begin_object / write_object_data / end_object). A chunked delivery HOLDS
 * across a MOQR_DELIVERY_WOULD_BLOCK: the next next_delivery re-peeks the SAME
 * record verbatim (its chunks stay pinned past eviction) so the binding resumes
 * its sub-object write cursor without duplicating or skipping a chunk. OPEN
 * records are delivered at the live edge: available chunks stream now and
 * the object reports MOQR_DELIVERY_STALLED until it grows or completes. An OPEN
 * record ABANDONED after this sub began it downstream is surfaced (rec.obj_state
 * == MOQR_OBJ_ABANDONED) so the bind resets the downstream subgroup and reports
 * MOQR_DELIVERY_ABANDONED; an ABANDONED record never begun downstream is skipped.
 *
 * Both calls take now_us (the deterministic clock) because completing a
 * subscription's range retires its subscriber HERE (inside next_delivery),
 * and delivery_done retires it on a terminal stream error — either retire may
 * arm the warm-track linger deadline (now_us + linger_us), so a stale clock
 * would warm the track in the past.
 */
MOQR_CORE_API moqr_result_t moqr_core_next_delivery(moqr_core_t *core, moqr_binding_t b,
                                      uint64_t now_us, moqr_delivery_t *out);

MOQR_CORE_API moqr_result_t moqr_core_delivery_done(moqr_core_t *core, moqr_binding_t b,
                                      moqr_delivery_outcome_t outcome,
                                      uint64_t now_us);

/*
 * Read chunk `idx` (0-based, append order) of the binding's outstanding CHUNKED
 * delivery. *out_buf is a BORROWED slice (pinned for the delivery — valid until
 * delivery_done, even if the source record is evicted meanwhile) and *out_len
 * its byte length. Returns MOQR_OK; MOQR_DONE when idx >= chunk_count;
 * MOQR_ERR_WRONG_STATE when no chunked delivery is outstanding. The binding must
 * not reach into the log to read chunks — this is the only accessor.
 */
MOQR_CORE_API moqr_result_t moqr_core_delivery_chunk(moqr_core_t *core, moqr_binding_t b,
                                       uint32_t idx,
                                       const moq_rcbuf_t **out_buf,
                                       uint64_t *out_len);

/* -- Retained-hit FETCH (standalone, ascending, fully-retained) ----------------------- *
 * A read-only cursor over a track's already-retained log, answering a standalone
 * FETCH from local retained state. Same pull discipline as delivery: peek pins the
 * head object (idempotent; the pin survives a WOULD_BLOCK downstream write across
 * pumps), commit advances the scalar cursor and unpins. Distinct handle so a
 * binding can run several concurrent fetches.
 *
 * A live fetch is a track READER: while it is open the track is pinned against
 * retention AGE-eviction and free, so the accepted fully-retained range cannot
 * age out from under it (capacity eviction from new ingest still surfaces as the
 * MOQR_ERR_TOO_OLD terminal); aging resumes once the fetch closes.
 *
 * Scope: standalone only; ASCENDING only; NORMAL records only (status-only
 * records are skipped); FULLY-RETAINED ranges only (a start below the eviction
 * horizon is refused). No gap markers, no joining fetch, no chunk-through — those
 * are future work; the seams here don't change for them.
 */
typedef struct moqr_fetch { uint64_t _opaque; } moqr_fetch_t;

#ifdef __cplusplus
#define MOQR_FETCH_INVALID (moqr_fetch_t{0})
#else
#define MOQR_FETCH_INVALID ((moqr_fetch_t){ 0 })
#endif

MOQR_CORE_API bool moqr_fetch_is_valid(moqr_fetch_t h);
MOQR_CORE_API bool moqr_fetch_eq(moqr_fetch_t a, moqr_fetch_t b);

/* Fetch request. start/end are RAW WIRE: [start .. end) with end_object == 0
 * meaning "the whole end_group" (draft-16/18 End Location encoding). */
typedef struct moqr_fetch_req {
    uint32_t           struct_size;
    moqr_ns_t          ns;             /* borrowed for the call            */
    moq_bytes_t        name;           /* borrowed for the call            */
    uint64_t           start_group;    /* inclusive                        */
    uint64_t           start_object;   /* inclusive                        */
    uint64_t           end_group;      /* RAW WIRE                         */
    uint64_t           end_object;     /* RAW WIRE: last+1; 0 = whole group */
    moqr_group_order_t group_order;    /* ASCENDING only (DEFAULT == ASC)  */
    uint8_t            subscriber_priority;
    uint64_t           cookie;         /* caller correlation               */
} moqr_fetch_req_t;

MOQR_CORE_API void moqr_fetch_req_init(moqr_fetch_req_t *req);

/* fetch_open's decision, so the binding emits FETCH_OK / REQUEST_ERROR without
 * re-deriving. On ACCEPT, end_group/end_object are the FETCH_OK End Location
 * (RAW WIRE, capped to the track largest). */
typedef uint32_t moqr_fetch_admit_t;
#define MOQR_FETCH_ACCEPT 1u
#define MOQR_FETCH_REJECT 2u

typedef struct moqr_fetch_plan {
    moqr_fetch_admit_t admit;
    uint64_t           error_code;     /* REJECT: wire REQUEST_ERROR code   */
    bool               end_of_track;   /* ACCEPT: FETCH_OK End Of Track      */
    uint64_t           end_group;      /* ACCEPT: FETCH_OK End Location (RAW) */
    uint64_t           end_object;
} moqr_fetch_plan_t;

typedef uint32_t moqr_fetch_item_kind_t;
#define MOQR_FETCH_ITEM_OBJECT 1u   /* a NORMAL object to write            */
#define MOQR_FETCH_ITEM_END    2u   /* range exhausted; FIN the fetch      */
#define MOQR_FETCH_ITEM_MARKER 3u   /* leading evicted-prefix gap marker   */

typedef struct moqr_fetch_item {
    moqr_fetch_item_kind_t kind;
    moqr_record_view_t     rec;        /* OBJECT: view valid while pinned. rec.payload
                                        * is the single object rcbuf; a chunked
                                        * COMPLETE record is coalesced into an owned
                                        * buffer, so rec.chunk_count
                                        * is presented as 0 — see fetch_peek.       */
    /* MARKER: emit an UNKNOWN end-of-range marker IMMEDIATELY BEFORE this
     * (first retained) group -- i.e. moq_session_write_fetch_range_before_group
     * with MOQ_FETCH_RANGE_UNKNOWN. Covers the evicted prefix the fetch's Start
     * fell into; the marker carries no payload (no pin). */
    uint64_t               marker_group;
} moqr_fetch_item_t;

/*
 * Open a fetch. Resolves the track, validates the range, and (on ACCEPT)
 * reserves a cursor slot. *out_plan carries the admission decision:
 *  - REJECT with DOES_NOT_EXIST: no such (servable) track.
 *  - REJECT with INVALID_RANGE: empty track, Start past the largest object, a
 *    range whose Start is below the retention horizon AND whose End does not
 *    reach retained data (nothing to serve), or a fully-evicted log.
 *  - ACCEPT: *out is a live handle; end_group/end_object/end_of_track fill the
 *    FETCH_OK; drive peek/commit until MOQR_FETCH_ITEM_END, then close. A Start
 *    below the horizon whose range still intersects retained data is accepted
 *    and served from the oldest retained group, led by one MOQR_FETCH_ITEM_MARKER
 *    (UNKNOWN) for the evicted prefix.
 * Returns MOQR_OK with a plan (REJECT plans allocate no slot), MOQR_ERR_INVAL on
 * bad args, MOQR_ERR_STALE_HANDLE on a dead binding, or MOQR_ERR_CAPACITY when
 * the fetch pool is exhausted (counted as MOQR_REFUSE_FETCHES).
 */
MOQR_CORE_API moqr_result_t moqr_core_fetch_open(moqr_core_t *core, moqr_binding_t b,
                                   const moqr_fetch_req_t *req, uint64_t now_us,
                                   moqr_fetch_t *out, moqr_fetch_plan_t *out_plan);

/*
 * Peek the fetch's head item WITHOUT advancing. Idempotent: repeated peeks
 * return the same item until commit, so a downstream write that returns
 * WOULD_BLOCK is retried by peeking again next pump. On MOQR_OK, *out is one of:
 *   - MOQR_FETCH_ITEM_MARKER: a leading UNKNOWN gap marker for an evicted prefix
 *     (no pin; carries marker_group). Precedes all objects when present.
 *   - MOQR_FETCH_ITEM_OBJECT: a retained NORMAL object, pinned until commit.
 *     rec.payload is one rcbuf: a whole-object record pins its retained payload
 *     (zero copy); a chunked COMPLETE record (streaming-ingest) is coalesced
 *     into one owned buffer here, charged as the pin.
 *   - MOQR_FETCH_ITEM_END: the range is exhausted; FIN the fetch.
 * Otherwise: MOQR_ERR_WOULD_BLOCK when the pin budget is momentarily full
 * (another fetch holds it — retry after it commits), MOQR_ERR_CAPACITY when a
 * single object exceeds the whole pin budget (a terminal the caller resets the
 * fetch stream on — unreachable while fetch_pin_bytes is floored to the log
 * budget, kept as defense), MOQR_ERR_TOO_OLD when the range fell below the
 * retention horizon mid-fetch (terminal), or MOQR_ERR_STALE_HANDLE.
 */
MOQR_CORE_API moqr_result_t moqr_core_fetch_peek(moqr_core_t *core, moqr_fetch_t fetch,
                                   uint64_t now_us, moqr_fetch_item_t *out);

/* Advance past the peeked head item: an OBJECT advances the cursor and unpins it;
 * a MARKER is cleared without unpinning (it holds no pin, and the cursor already
 * starts at the first retained group). MOQR_ERR_WRONG_STATE without a prior peek
 * of a MARKER or OBJECT; MOQR_ERR_STALE_HANDLE on a dead fetch. */
MOQR_CORE_API moqr_result_t moqr_core_fetch_commit(moqr_core_t *core, moqr_fetch_t fetch);

/* Retire the fetch: unpin, release the pin-byte budget, invalidate the handle. */
MOQR_CORE_API moqr_result_t moqr_core_fetch_close(moqr_core_t *core, moqr_fetch_t fetch);

/* -- Outputs ------------------------------------------------------------------------- */

MOQR_CORE_API size_t moqr_core_poll_intents(moqr_core_t *core, moqr_intent_t *out,
                              size_t cap);

/* Drain up to cap ready-binding cookies in ascending binding-slot order.
 * The core marks a binding ready at every transition that can make it
 * deliverable (activation, ingest/chunk/completion/abandon, seal, eviction
 * skip, subscription retirement); a drained binding is no longer marked and
 * new work re-marks it. Marks are deduplicated (one bit per binding);
 * pseudo-bindings (cookie >= MOQR_SHARD_COOKIE_BASE) never enter the set.
 * cap == 0 drains nothing and mutates nothing; undrained bindings stay
 * marked for the next call. Single-caller like every core API. Returns the
 * number of cookies written. A ready mark is a hint, not a guarantee: the
 * binding may have nothing deliverable NOW (moqr_core_next_delivery still
 * decides). */
MOQR_CORE_API uint32_t moqr_core_drain_ready(moqr_core_t *core, uint64_t *cookies,
                               uint32_t cap);

/* Closed-form memory ceiling for a whole core under this config: fixed
 * pools + name-intern budget + max_tracks x the per-track log ceiling.
 * A relay process prints this at startup — retained memory is a function
 * of configuration, computed before anything runs. */
typedef struct moqr_core_capacity {
    uint64_t structure_bytes;   /* pools, rings, per-track structures     */
    uint64_t payload_bytes;     /* max_tracks x per-log payload budget    */
    uint64_t total_bytes;
} moqr_core_capacity_t;

MOQR_CORE_API moqr_result_t moqr_core_capacity_describe(const moqr_core_relay_cfg_t *cfg,
                                          moqr_core_capacity_t *out);

/* Resolved pool limits of a live core, so consumers (the session binding)
 * can size their own tables to make under-provisioning impossible. */
typedef struct moqr_core_limits {
    uint32_t max_bindings;
    uint32_t max_tracks;
    uint32_t max_subs;
    uint32_t max_ns_nodes;
    uint32_t max_ns_subs;
    uint32_t max_intents;          /* post atomicity clamp                 */
    uint32_t max_fetches;
    uint32_t max_parked;
    uint32_t max_grants;
    uint32_t max_cancels;
    uint32_t name_intern_bytes;
    uint32_t parked_bytes;
    uint32_t grant_bytes;
    uint32_t fetch_pin_bytes;      /* pre log-floor (create floors later)  */
} moqr_core_limits_t;

MOQR_CORE_API void moqr_core_get_limits(const moqr_core_t *core, moqr_core_limits_t *out);

/*
 * The pure, prefix-safe resolution create() applies to a config: defaults
 * filled, the max_cancels/max_bindings coupling applied, and the intent-ring
 * atomicity clamp folded in. Shared single source for create,
 * capacity_describe, and every downstream (bind/shard) resolver — the two
 * sides can never disagree about what a config means. Touches nothing.
 */
MOQR_CORE_API moqr_result_t moqr_core_limits_resolve(const moqr_core_relay_cfg_t *cfg,
                                       moqr_core_limits_t *out);

/*
 * Declare an ACTIVE track's upstream lost (e.g. the binding accepted a
 * PUBLISH it cannot track, or the transport dropped the source without a
 * session close). The track keeps serving retained content (WARM) or is
 * freed when empty and subscriber-less. Generation-guarded like the other
 * upstream resolutions.
 */
MOQR_CORE_API moqr_result_t moqr_core_upstream_lost(moqr_core_t *core, moqr_track_t track,
                                      uint64_t track_gen);

/*
 * The track's SOURCE finished gracefully — an upstream subscription that got
 * SUBSCRIBE_DONE, or a push publisher that finished (PUBLISH_FINISHED):
 * terminate every downstream subscriber on the track with a wire SUBSCRIBE_DONE
 * carrying `status` (so none hangs behind a sourceless ACTIVE track), then
 * release the source like moqr_core_upstream_lost (WARM if retained content
 * remains, else freed). Generation-guarded; MOQR_ERR_STALE_HANDLE for a stale
 * track, MOQR_ERR_WRONG_STATE when not ACTIVE, MOQR_ERR_WOULD_BLOCK when the
 * intent ring can't hold one SUB_DONE per downstream sub (no state change —
 * retry after a drain).
 */
MOQR_CORE_API moqr_result_t moqr_core_source_done(moqr_core_t *core, moqr_track_t track,
                                    uint64_t track_gen, moqr_pd_desc_t status,
                                    uint64_t now_us);

/*
 * Admission-refusal reasons — the bounded label set for the refusals
 * metric. Tagged by the exhausted *resource* (not the operation), so a
 * subscribe refused for name-intern bytes is distinguishable from one
 * refused for the subscription pool. Append-only, never renumbered; new
 * resources get a new trailing value and bump _COUNT.
 */
typedef uint32_t moqr_refuse_reason_t;

#define MOQR_REFUSE_TRACKS     0u
#define MOQR_REFUSE_SUBS       1u
#define MOQR_REFUSE_NS_NODES   2u
#define MOQR_REFUSE_NS_SUBS    3u
#define MOQR_REFUSE_BINDINGS   4u
#define MOQR_REFUSE_NAME_BYTES 5u
#define MOQR_REFUSE_FETCHES    6u
#define MOQR_REFUSE__COUNT     7u

/* Stable lower-snake label token for a reason (e.g. "name_bytes"); the
 * enum's own name for an out-of-range value is "unknown". */
MOQR_CORE_API const char *moqr_refuse_reason_name(moqr_refuse_reason_t reason);

typedef struct moqr_core_stats {
    uint32_t bindings;
    uint32_t tracks;
    uint32_t subs;              /* all live subscriptions                 */
    uint32_t subs_parked;       /* subset awaiting upstream resolution     */
    uint32_t subs_active;       /* subset serving from a track log         */
    uint32_t ns_nodes;
    uint32_t ns_subs;
    uint64_t retained_bytes;    /* across all track logs (gauge)          */
    uint64_t ingested_total;
    uint64_t delivered_total;
    uint64_t evict_sweeps;      /* times the evict-reset sweep body ran; stays
                                 * flat under whole-object delivery (gated off
                                 * unless a sub has begun a live-edge object)   */
    uint64_t evicted_total;     /* records evicted, incl. from freed tracks */
    uint64_t note_emitted_total;/* accepted moqr_core_delivery_note_emitted calls
                                 * (one per bind chunked-write WOULD_BLOCK hold) —
                                 * a call counter, not a per-begun-object count:
                                 * the same object contributes once per hold      */
    uint32_t intent_highwater;  /* peak intent-ring occupancy (gauge)     */
    uint64_t route_epoch;
    /* Admission refusals, indexed by moqr_refuse_reason_t. */
    uint64_t refusals[MOQR_REFUSE__COUNT];
    /* Authorization decisions [action][ALLOW|DENY|DEFER], and denials broken
     * out by reason. Bounded-cardinality (action×3 + reason). */
    uint64_t auth_decisions[MOQR_AUTH_ACTION__COUNT][3];
    uint64_t auth_denials[MOQR_AUTH_REASON__COUNT];
} moqr_core_stats_t;

MOQR_CORE_API void moqr_core_get_stats(const moqr_core_t *core, moqr_core_stats_t *out);

/* Version-1 stats layouts are frozen. Sized access requires the entire v1
 * object, preserves bytes beyond it, and leaves output unchanged on refusal.
 * Call on the owning shard, or after all its writers have stopped. */
MOQR_CORE_API moqr_result_t moqr_core_get_stats_sized(
    const moqr_core_t *core, moqr_core_stats_t *out, size_t out_size);

/*
 * Render a deterministic, entity-detailed route snapshot into `buf`: the
 * epoch triple, announced namespaces, namespace watchers, and every live
 * track with its log watermarks and per-subscription cursor state. Each
 * entity names its bind-layer connection `binding` cookie (the operator id,
 * not the internal pool slot). Per subscription:
 *   lag_groups   retained in-range groups still owing the cursor a record
 *   frontier_group  the oldest such group (null when caught up / parked)
 *   skip_pending    the latched flag that the NEXT delivery will report a
 *                   forced skip — set only when a delivery pass reclaimed an
 *                   evicted group's tracked position. It is NOT a complete
 *                   loss signal: content evicted before any pull, or before
 *                   a position entry existed, does not set it.
 *   start_before_retention  a read-only fact: the subscription's start group
 *                   is older than the oldest retained group, so content it
 *                   requested below the retention window has been evicted
 *                   (a loss unless it was delivered before eviction). This
 *                   catches the pre-pull eviction case skip_pending misses.
 * `_json` emits one compact JSON object; `_text` an indented tree. Both are
 * pure — no mutation, no advancing calls, no allocation — and ordered by
 * pool slot, so identical state renders identically. Same truncation
 * contract as the metrics/trace serializers: MOQR_OK when content + NUL fit,
 * else MOQR_ERR_CAPACITY with `*written` set to the full length needed.
 */
MOQR_CORE_API moqr_result_t moqr_core_route_dump_json(const moqr_core_t *core, char *buf,
                                        size_t cap, size_t *written);
MOQR_CORE_API moqr_result_t moqr_core_route_dump_text(const moqr_core_t *core, char *buf,
                                        size_t cap, size_t *written);

#ifdef __cplusplus
}
#endif

#endif /* MOQRELAY_RELAY_H */
