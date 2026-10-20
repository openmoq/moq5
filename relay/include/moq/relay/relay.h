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
#include <moq/location.h>
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
/* The current group from its first object: {Largest Object.Group, 0}, open-
 * ended. No draft-16/18 wire form; it is the draft-22 relative Location
 * Filter at StartGroup=1 (the relative form at 0 is NEXT_GROUP_START). */
#define MOQR_FILTER_CURRENT_GROUP_START 5u
/* A start `start_group` groups before the Next Group, open-ended:
 * {Largest Object.Group + 1 - start_group, 0}, clamped at group 0 -- the
 * draft-22 relative Location Filter at any StartGroup (0 is
 * NEXT_GROUP_START's start, 1 CURRENT_GROUP_START's). start_object is
 * ignored. */
#define MOQR_FILTER_RELATIVE_START   6u

typedef struct moqr_filter {
    moqr_filter_type_t type;
    uint64_t           start_group;     /* ABSOLUTE_*; RELATIVE_START: the
                                         * relative StartGroup              */
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

/* The REQUEST_ERROR meaning "this relay cannot admit the request right now
 * for lack of capacity" (a pool that holds only live entries), carried as an
 * intent's error_code where a wire number would otherwise go: the binding
 * encodes it for the connection's draft (draft-18 EXCESSIVE_LOAD, draft-16
 * INTERNAL_ERROR, each with a Retry Interval). It lies outside every draft's
 * REQUEST_ERROR space (draft-16 varints stop at 2^62 - 1; draft-18 maps
 * unregistered codes to INTERNAL_ERROR), so no peer can send it. */
#define MOQR_REQUEST_ERROR_CAPACITY UINT64_MAX

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
    moqr_track_t       track;           /* UPSTREAM_* / ACCEPT_PUBLISH /
                                         * TRACK_STATUS_OK                 */
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

    /* Lifecycle, in microseconds of the caller's clock.
     *
     * linger_us: keep a pull track's UPSTREAM subscription after its last
     * subscriber leaves; 0 = unsubscribe at the next tick. The track then
     * turns WARM: its source is released and its retained objects are cache.
     *
     * warm_idle_us: how long a reclaimable track (WARM, no subscription, no
     * fetch) keeps that cache before a tick frees it with its log and
     * interned name. Relays MAY cache objects, and without
     * MAX_CACHE_DURATION they can be cached until implementation constraints
     * evict them. Independent of this bound, a full track table reclaims the
     * least recently used reclaimable track for every new one.
     * 0 = default (60 s); UINT64_MAX = never age out. */
    uint64_t linger_us;
    uint64_t warm_idle_us;

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

/* Advance virtual time: fires linger deadlines, ages logs, frees tracks past
 * the warm idle bound, resumes or times out held rendezvous subscriptions. */
MOQR_CORE_API moqr_result_t moqr_core_tick(moqr_core_t *core, uint64_t now_us);

/*
 * The earliest now_us at which moqr_core_tick has time-driven work: a held
 * rendezvous subscription's deadline, a pull track's upstream linger
 * expiring, or a reclaimable track reaching warm_idle_us (0 when a reclaimable track still needs the tick that arms
 * its idle clock). UINT64_MAX = none. Pure; same clock domain as the now_us
 * the caller passes to tick. A runtime that sleeps while idle folds this into
 * its wake deadline so retention bounds hold without traffic.
 */
MOQR_CORE_API uint64_t moqr_core_next_deadline_us(const moqr_core_t *core);

/* An epoch that moves whenever admission room may have appeared: a pool
 * released capacity (a track slot, a trie node, interned name bytes, a fetch
 * slot) or capacity became reclaimable (an announce withdrawn; a track left
 * with no source, subscription or fetch). It never moves for a change that
 * only consumes room (an announce, a new track, a subscription), so a caller
 * parked on a capacity refusal retries only when the retry could succeed.
 * 0 for NULL. */
MOQR_CORE_API uint64_t moqr_core_room_epoch(const moqr_core_t *core);

/* Whether announcing ns would find room now, without announcing or making
 * room: MOQR_OK (room, possibly after reclaiming unreferenced trie nodes or
 * cache), MOQR_ERR_WOULD_BLOCK (only once queued intents are polled), or
 * MOQR_ERR_CAPACITY (no room). It checks room only: an announce it calls OK
 * can still meet a full intent ring (WOULD_BLOCK) or an occupied namespace
 * (WRONG_STATE). Pure: nothing changes and no refusal is counted, so a caller
 * retrying a refused announce can wait for room without inflating the
 * refusal metrics. MOQR_ERR_INVAL for a bad view. */
MOQR_CORE_API moqr_result_t moqr_core_announce_room(const moqr_core_t *core,
                                                    moqr_ns_t ns);

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

/* Initialize min(options_size, sizeof current), stamp bytes initialized and
 * default flags to zero. NULL or fewer than 4 bytes leaves storage untouched.
 * The open call accepts a frozen 8-byte prefix, not sizeof a future layout.
 * NULL/default options retain binding_open's property-less direct-source
 * behavior. Unknown flags or a short prefix return INVAL before pool/trace
 * mutation and leave output unchanged. No existing public layout grows. */
MOQR_CORE_API void moqr_binding_options_init_sized(
    moqr_binding_options_t *options, size_t options_size);
MOQR_CORE_API moqr_result_t moqr_core_binding_open_with_options(
    moqr_core_t *core, uint64_t session_cookie,
    const moqr_binding_options_t *options, moqr_binding_t *out);

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
    /* Appended: the SUBSCRIBE asked for Forward State 0, so the subscription
     * it resumes into must not forward until an update turns it on. False is
     * the default (forwarding), so a request that never sets it keeps the
     * behavior of the layout before it. The struct carries no struct_size, so
     * the field is reached only through moqr_core_park_sized() and
     * moqr_core_auth_begin_resolve_sized(), which take the caller's size;
     * moqr_core_park() and moqr_core_auth_begin_resolve() read and write the
     * layout through sub_group_order and never touch it. */
    bool                     sub_forward_off;
    /* Appended: the SUBSCRIBE asked to wait for a publisher (draft-22
     * RENDEZVOUS_TIMEOUT). sub_rendezvous_deadline_us is the one absolute
     * deadline the binding computed at the request's first receipt, so a
     * resume after deferred authorization keeps it. Read and written only
     * through the _sized calls when req_size covers sub_rendezvous; the
     * deadline comes first so that neither sits in the tail padding of the
     * layout through sub_forward_off. */
    uint64_t                 sub_rendezvous_deadline_us;
    bool                     sub_rendezvous;
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
/* Park with the size of the caller's moqr_park_req_t (sizeof, for a caller
 * built against this header): fields past it read as their defaults.
 * moqr_core_park() is this with the layout through sub_group_order.
 * MOQR_ERR_INVAL, with no state change, for a req_size below that layout. */
MOQR_CORE_API moqr_result_t moqr_core_park_sized(moqr_core_t *core,
                                                 const moqr_park_req_t *req,
                                                 size_t req_size,
                                                 uint64_t ext_ticket,
                                                 uint64_t now_us);

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
/* Begin a resolve into a view of the caller's size (sizeof, for a caller
 * built against this header): the library writes min(view_size, its own
 * sizeof) bytes and nothing past them. moqr_core_auth_begin_resolve() is
 * this with the layout through sub_group_order. MOQR_ERR_INVAL (verdict left
 * DENY, nothing pinned) for a view_size below that layout. */
MOQR_CORE_API moqr_result_t moqr_core_auth_begin_resolve_sized(
    moqr_core_t *core, uint64_t ext_ticket, const moqr_auth_verdict_t *result,
    uint64_t now_us, moqr_auth_verdict_t *verdict, moqr_park_req_t *view,
    size_t view_size);

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

/* Free the committed grant of the op (binding_cookie, session_cookie) -- an
 * announce whose authorization an accepted update replaced -- so a later
 * recheck cannot act on authorization that no longer applies. A grant already
 * revoked and awaiting its cancel is kept. No-op when none matches. */
MOQR_CORE_API void moqr_core_grant_release(moqr_core_t *core,
                                           uint64_t binding_cookie,
                                           uint64_t session_cookie);

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

/*
 * Replace a namespace subscription's prefix (a draft-22 update, §9.5.2), in
 * two steps around the update's acceptance on the wire (the acceptance is
 * the barrier at which the new prefix takes effect):
 *  - prepare does everything that can fail -- MOQR_ERR_STALE_HANDLE for an
 *    unknown (binding, cookie), MOQR_ERR_CAPACITY when the new prefix does not
 *    fit the name-byte budget, MOQR_ERR_NOMEM, MOQR_ERR_WOULD_BLOCK when the
 *    intent ring cannot take the NS_FOUND fan-out -- and on MOQR_OK holds the
 *    new prefix and that intent room in *prep, changing nothing else;
 *  - commit installs it and cannot fail for want of memory or intent room:
 *    the new prefix governs every later NS_FOUND / NS_GONE; announces it
 *    matches that the old prefix did not are reported with NS_FOUND; queued
 *    NS_FOUND / NS_GONE intents for the subscription that have not been polled
 *    are re-expressed against the new prefix (`value`), or withdrawn when
 *    their namespace lies outside it. Namespaces both prefixes match are not
 *    reported again; one only the old prefix matched is not reported as gone
 *    (the subscriber drops it at the same barrier). MOQR_ERR_STALE_HANDLE
 *    when the subscription ended since (the prep is released);
 *  - abort releases a prepared update that will not be committed.
 * A prepared update must be committed or aborted before any other core call
 * on the binding.
 */
typedef struct moqr_ns_update_prep {
    bool           staged;
    moqr_binding_t binding;
    uint64_t       cookie;
    uint8_t       *prefix_buf;   /* owned until commit or abort */
    uint32_t       prefix_len;
    uint32_t       intents;      /* intent room held for the fan-out */
} moqr_ns_update_prep_t;
MOQR_CORE_API moqr_result_t moqr_core_ns_update_prepare(moqr_core_t *core,
    moqr_binding_t b, uint64_t cookie, moqr_ns_t prefix,
    moqr_ns_update_prep_t *prep);
MOQR_CORE_API moqr_result_t moqr_core_ns_update_commit(moqr_core_t *core,
    moqr_ns_update_prep_t *prep);
MOQR_CORE_API void moqr_core_ns_update_abort(moqr_core_t *core,
    moqr_ns_update_prep_t *prep);

/* -- Track discovery (draft-22 SUBSCRIBE_TRACKS, §3.6, §7.6, §9.18) -------- *
 * A binding's track subscription asks for a PUBLISH for every track the relay
 * knows under a namespace prefix -- now and as tracks appear. The core keeps
 * the interest and which (subscription, track source generation) pairs have
 * been offered; the binding pulls the next unoffered match, authorizes it,
 * sends the PUBLISH (or PUBLISH_SKIPPED) and reports the outcome. Bounded by
 * max_ns_subs track subscriptions (an independent pool from the namespace
 * subscriptions) and the name-byte budget for their prefixes. */

typedef struct moqr_tracks_req {
    uint32_t           struct_size;
    moqr_ns_t          prefix;            /* borrowed for the call           */
    uint64_t           cookie;            /* the binding's wire handle       */
    bool               forward;           /* FORWARD of the PUBLISHes        */
    bool               include_properties;
    moqr_group_order_t group_order;       /* MOQR_GROUP_ORDER_*              */
    /* Appended: nothing is offered until moqr_core_tracks_answered says the
     * request's acceptance is on the wire (§3.6: a PUBLISH follows the
     * REQUEST_OK). */
    bool               held;
} moqr_tracks_req_t;

/* Clears and stamps min(req_size, sizeof current); defaults forward and
 * include_properties to true. */
MOQR_CORE_API void moqr_tracks_req_init_sized(moqr_tracks_req_t *req,
                                              size_t req_size);

/* MOQR_ERR_STALE_HANDLE (binding), MOQR_ERR_INVAL (malformed prefix or a
 * duplicate cookie on the binding), MOQR_ERR_CAPACITY (pool or name bytes;
 * counted as MOQR_REFUSE_NS_SUBS / _NAME_BYTES), MOQR_ERR_NOMEM. No intents. */
MOQR_CORE_API moqr_result_t moqr_core_tracks_subscribe(moqr_core_t *core,
                                         moqr_binding_t b,
                                         const moqr_tracks_req_t *req);

/* A held track subscription's acceptance is on the wire: its matches are
 * offered from now on. MOQR_ERR_STALE_HANDLE for an unknown one. */
MOQR_CORE_API moqr_result_t moqr_core_tracks_answered(moqr_core_t *core,
    moqr_binding_t b, uint64_t cookie);

/* Changes affect only future offers (§9.5.2, §9.20.18): PREFIX replaces the
 * prefix; FORWARD replaces the FORWARD of later PUBLISHes; TOKENS says the
 * authorization tokens changed, so every match whose authorization was
 * decided, declined or still pending under the old tokens is decided again
 * under the new ones (input_gen advances; a verdict for the old tokens is
 * stale). Prepared and committed around the update's acceptance as
 * moqr_core_ns_update_prepare / _commit / _abort: prepare holds the new
 * prefix's bytes, commit cannot fail for want of memory. */
#define MOQR_TRACKS_UPDATE_PREFIX  0x01u
#define MOQR_TRACKS_UPDATE_FORWARD 0x02u
#define MOQR_TRACKS_UPDATE_TOKENS  0x04u
typedef struct moqr_tracks_update_prep {
    bool           staged;
    moqr_binding_t binding;
    uint64_t       cookie;
    uint32_t       flags;
    bool           forward;
    uint8_t       *prefix_buf;   /* owned until commit or abort (PREFIX) */
    uint32_t       prefix_len;
} moqr_tracks_update_prep_t;
MOQR_CORE_API moqr_result_t moqr_core_tracks_update_prepare(moqr_core_t *core,
    moqr_binding_t b, uint64_t cookie, uint32_t flags, moqr_ns_t prefix,
    bool forward, moqr_tracks_update_prep_t *prep);
MOQR_CORE_API moqr_result_t moqr_core_tracks_update_commit(moqr_core_t *core,
    moqr_tracks_update_prep_t *prep);
MOQR_CORE_API void moqr_core_tracks_update_abort(moqr_core_t *core,
    moqr_tracks_update_prep_t *prep);

/* Ends the interest only: PUBLISHes already sent are untouched (§3.6). */
MOQR_CORE_API moqr_result_t moqr_core_tracks_unsubscribe(moqr_core_t *core,
                                           moqr_binding_t b, uint64_t cookie);

/* One unoffered match: a track with a live source whose namespace the
 * subscription's prefix matches, excluding tracks the binding itself sources
 * (§3.6). Views BORROW core storage until the next mutating core call. */
typedef struct moqr_discovery {
    uint32_t           struct_size;       /* written: the size filled in     */
    uint64_t           cookie;            /* the track subscription          */
    moqr_track_t       track;
    uint64_t           track_gen;         /* the source generation offered   */
    moqr_ns_t          ns;                /* views ns_parts                  */
    moq_bytes_t        ns_parts[32];
    moq_bytes_t        name;
    uint32_t           prefix_count;      /* PUBLISH_SKIPPED suffix start    */
    bool               forward;
    bool               include_properties;
    moqr_group_order_t group_order;
    bool               has_largest;
    uint64_t           largest_group;
    uint64_t           largest_object;
    /* Appended: the subscriber's authorization for this track was already
     * decided (MOQR_DISCOVERY_ALLOWED after a deferral): offer it without
     * asking again. Written only when out_size covers it. */
    bool               authorized;
    /* Appended: the generation of the track subscription's authorization
     * inputs (its tokens) this match is decided under; reported back with
     * the outcome. Written only when out_size covers it. */
    uint32_t           input_gen;
} moqr_discovery_t;

/* The complete first layout; a later field is appended past it. */
#define MOQR_DISCOVERY_V0_SIZE \
    (offsetof(moqr_discovery_t, largest_object) + sizeof(uint64_t))

/* MOQR_OK with *out filled, MOQR_DONE when nothing is unoffered, or
 * MOQR_ERR_STALE_HANDLE. The same match is returned until it is reported.
 * out_size is the caller's sizeof(moqr_discovery_t); fields past it are not
 * written, and struct_size reports the size filled in. MOQR_ERR_INVAL for an
 * out_size below MOQR_DISCOVERY_V0_SIZE. */
MOQR_CORE_API moqr_result_t moqr_core_next_discovery(moqr_core_t *core,
                                       moqr_binding_t b,
                                       moqr_discovery_t *out,
                                       size_t out_size);

/* The outcome of an offered match, for that source generation only (a new
 * PUBLISH from the source is a new generation, §3.6.3). OFFERED: a PUBLISH was
 * sent. SKIPPED: PUBLISH_SKIPPED was sent, never followed by a PUBLISH for
 * that generation. DECLINED: not authorized; nothing sent, offered again
 * after a token update. SUPPRESSED (set by the core): a SUBSCRIBE this binding
 * held for the track was resolved by its PUBLISH instead (§7.6). */
#define MOQR_DISCOVERY_OFFERED    1u
#define MOQR_DISCOVERY_SKIPPED    2u
#define MOQR_DISCOVERY_DECLINED   3u
#define MOQR_DISCOVERY_SUPPRESSED 4u
/* PENDING: its authorization was deferred; it is not returned again until
 * the verdict is reported -- ALLOWED (returned once more, `authorized` set)
 * or DECLINED. An outcome reported for an older input_gen, or for a source
 * generation that is no longer the track's, is MOQR_ERR_STALE_HANDLE and
 * changes nothing. */
#define MOQR_DISCOVERY_PENDING    5u
#define MOQR_DISCOVERY_ALLOWED    6u
MOQR_CORE_API moqr_result_t moqr_core_discovery_done(moqr_core_t *core,
                                       moqr_binding_t b, uint64_t cookie,
                                       moqr_track_t track, uint64_t track_gen,
                                       uint32_t input_gen, uint32_t outcome);

/* The prefix in effect of a binding's namespace subscription (tracks false)
 * or track subscription (tracks true): views into core storage, written to
 * `parts` (parts_cap entries) and valid until the next mutating core call.
 * MOQR_ERR_STALE_HANDLE for an unknown (binding, cookie). Pure. */
MOQR_CORE_API moqr_result_t moqr_core_subscription_prefix(moqr_core_t *core,
    moqr_binding_t b, uint64_t cookie, bool tracks, moq_bytes_t *parts,
    size_t parts_cap, moqr_ns_t *out);

/* -- Request tokens ---------------------------------------------------------- *
 * The authorization tokens a binding's long-lived request (an announce, a
 * namespace or track subscription) currently carries, deep-copied, so an
 * update without tokens is authorized against the ones in effect (§9.5: an
 * omitted parameter keeps its value) and discovery authorizes each track with
 * them. Keyed by (binding, cookie); retired with the binding. Bytes are
 * charged to the name-byte budget; at most 2 * max_ns_subs + max_ns_nodes
 * entries. */
MOQR_CORE_API moqr_result_t moqr_core_request_tokens_set(moqr_core_t *core,
    moqr_binding_t b, uint64_t cookie, const moqr_auth_token_t *tokens,
    size_t token_count);
/* Borrowed until the next mutating core call. MOQR_DONE when none. */
MOQR_CORE_API moqr_result_t moqr_core_request_tokens_get(moqr_core_t *core,
    moqr_binding_t b, uint64_t cookie, const moqr_auth_token_t **tokens,
    size_t *token_count);
MOQR_CORE_API void moqr_core_request_tokens_clear(moqr_core_t *core,
    moqr_binding_t b, uint64_t cookie);

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
    /* Reserved, never read: it fills the tail padding after `forward`, so
     * range_end_holds starts at or past the sizeof of the layout that ends at
     * `forward` -- a struct_size that caller passes never covers it. */
    uint32_t           _reserved_range_end_holds;
    /* Appended (prefix-safe; absent means false): what reaching the end of a
     * bounded filter range does to the subscription. false retires it with
     * PUBLISH_DONE SUBSCRIPTION_ENDED once every in-range record has been
     * delivered (the draft-16/18 contract). true keeps it established and
     * idle -- nothing further is scheduled for it, the track stays demanded,
     * and only the subscriber or a track end finishes it -- which is what a
     * draft-22 subscription requires: its publisher does not end it because
     * Largest Object advanced past the end of its Location Filter. */
    bool               range_end_holds;
    /* Preserve the complete prior extent, including its uint64_t alignment
     * padding on ILP32. This field is never read. */
    uint32_t           _reserved_location;
    /* Whole-pointer gated, borrowed for the call and copied on success.
     * Non-NULL selects the semantic filter instead of `filter`, including
     * NONE (unfiltered). NULL preserves the legacy admission behavior. */
    const moq_location_filter_cfg_t *location;
    /* Appended: wait for a publisher when none is known (draft-22
     * RENDEZVOUS_TIMEOUT, §9.20.6). With rendezvous true and neither a track
     * nor an announced publisher, the subscription is held -- no ACCEPT_SUB,
     * no REJECT_SUB -- until an announce or a PUBLISH supplies a source (then
     * ordinary admission resumes, never an early accept) or moqr_core_tick
     * reaches rendezvous_deadline_us (REJECT_SUB with TIMEOUT). The one
     * absolute deadline is the binding's: it is never renewed here. A held
     * subscription takes a track slot and a subscription slot like any
     * other. False keeps the immediate REJECT_SUB DOES_NOT_EXIST. The
     * deadline comes first so that no part of the pair sits in the tail
     * padding of the layout through location; both are read only when
     * struct_size covers rendezvous. */
    uint64_t                         rendezvous_deadline_us;
    bool                             rendezvous;
    /* Appended: take a subscription slot reserved earlier by the same-build
     * binding (consumed when the request succeeds, held again when it
     * fails). */
    bool                             reserved;
} moqr_subscribe_req_t;

/*
 * The symbol clears and stamps the layout through `forward` -- the struct as a
 * caller built before range_end_holds was appended holds it -- and sets the
 * defaults (Largest Object filter, subscriber priority 128, forward), so it
 * never writes past such a caller's storage. A caller built against this
 * header reaches the whole struct: the function-like macro below passes its
 * sizeof to moqr_subscribe_req_init_sized(), which clears and stamps
 * min(req_size, sizeof current) and sets the same defaults.
 */
MOQR_CORE_API void moqr_subscribe_req_init(moqr_subscribe_req_t *req);
MOQR_CORE_API void moqr_subscribe_req_init_sized(moqr_subscribe_req_t *req,
                                                 size_t req_size);
#define moqr_subscribe_req_init(req) \
    moqr_subscribe_req_init_sized((req), sizeof(*(req)))

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

/* The live subscription a binding made with `cookie` (moqr_subscribe_req_t),
 * answered or not -- so a request withdrawn before its answer, such as a
 * SUBSCRIBE held for a publisher, can be ended with moqr_core_unsubscribe.
 * MOQR_ERR_STALE_HANDLE when there is none. Pure. */
MOQR_CORE_API moqr_result_t moqr_core_sub_find(const moqr_core_t *core,
                                               moqr_binding_t b, uint64_t cookie,
                                               moqr_sub_t *out);

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
 * Replace a live subscription's filter (a subscription update that carries a
 * new Location Filter). The new filter is resolved exactly as the request's
 * own would be: against the track's Largest Object when the subscription is
 * active, or remembered and resolved at upstream acceptance while it is still
 * parked. On an active subscription the window applies to what its cursor has
 * not yet passed: a start at or above the current one narrows delivery from
 * there on, the end is replaced freely (a subscription idle at the end of a
 * bounded range resumes when the new window extends it, and a bounded window
 * already passed completes as usual), and a start below the current one keeps
 * the current one -- records the cursor passed, delivered or consumed below
 * the former start, are never revisited (re-sending them would also break
 * the ascending object order of an open downstream subgroup stream). A
 * subscriber that lowers its start retrieves Objects at or below Largest
 * Object through a fill or a FETCH, which this relay serves from the same
 * retained log. Records already written outside the new window are not
 * recalled. Same argument contract as moqr_core_subscribe for `filter`
 * (MOQR_ERR_INVAL for an unknown type or an overflowing ABSOLUTE_RANGE);
 * MOQR_ERR_STALE_HANDLE for a retired, unknown or foreign subscription.
 */
MOQR_CORE_API moqr_result_t moqr_core_sub_set_filter(moqr_core_t *core, moqr_sub_t sub,
                                       const moqr_filter_t *filter,
                                       uint64_t now_us);

/* Replace the semantic Location Filter, copied before return (also while
 * parked). Active relative/Next Object filters resolve against current Largest;
 * parked filters resolve on upstream acceptance. NONE removes the constraint,
 * not delivery history: delivered or previously excluded retained records are
 * never replayed, but new arrivals can pass even below Largest. Begun objects
 * retain their completion/reset obligation. An unbegun held delivery excluded
 * by the replacement is released, as with Forward=0. Inclusive object ends and
 * equal-group empty ranges are supported without end+1 arithmetic.
 * NULL, a record shorter than the 48-byte v0 floor, an unknown kind or reversed
 * group bounds return INVAL before effects; stale/foreign handles return
 * STALE_HANDLE. NOMEM leaves the subscription untouched. This does not change
 * moqr_core_sub_set_filter's legacy monotonic-start contract. */
MOQR_CORE_API moqr_result_t moqr_core_sub_set_location_filter(
    moqr_core_t *core, moqr_sub_t sub,
    const moq_location_filter_cfg_t *location, uint64_t now_us);

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

/*
 * Resolve a full retained-track handle in any live state (PENDING, ACTIVE or
 * WARM) and return its current source generation. NULL core/output is INVAL;
 * an invalid, retired or replaced handle is STALE_HANDLE. Failure leaves the
 * output unchanged. Pure O(1) owner-lane observation: no allocation, time
 * advance, mutation, intents, borrowed storage or lifetime pin. A changed
 * source generation does not mean the retained identity was retired. Handles
 * must belong to this core; numeric handle tags are not cross-core identity.
 */
MOQR_CORE_API moqr_result_t moqr_core_track_current_generation(
    const moqr_core_t *core, moqr_track_t track, uint64_t *out_track_gen);

/* Resolve retained SOURCE metadata provenance, not the downstream binding's
 * flag. Managed remains true through detach, WARM, failover and rejoin; only
 * a new retained identity/log initializes it again from its selected source.
 * NULL core/output is INVAL; a dead/replaced full handle is STALE_HANDLE.
 * Failure leaves output unchanged. Pure O(1) owner-lane query, no allocation,
 * mutation, intents or pin. False alone is not proof of an available reply:
 * source admission and any explicit metadata obligation still apply. */
MOQR_CORE_API moqr_result_t moqr_core_track_metadata_managed(
    const moqr_core_t *core, moqr_track_t track, bool *out_managed);

/*
 * A live track's full identity: its Track Namespace parts and Track Name,
 * as BORROWED views into core-interned storage, valid until the next
 * mutating core call. The parts are written into the caller's `parts`
 * array (`parts_cap` entries) and `ns` is set to view them; MOQR_ERR_CAPACITY
 * when the namespace has more parts than fit (nothing written). The
 * generation guard matches moqr_core_sub_track: MOQR_ERR_STALE_HANDLE for a
 * retired track or a stale `track_gen`. Pure -- no mutation, no intents --
 * so a consumer holding only a subscription (through moqr_core_sub_track)
 * can name the track to the retained-log fetch path.
 */
MOQR_CORE_API moqr_result_t moqr_core_track_key(const moqr_core_t *core, moqr_track_t track,
                                  uint64_t track_gen, moq_bytes_t *parts,
                                  size_t parts_cap, moqr_ns_t *ns,
                                  moq_bytes_t *name);

/*
 * A live track's Largest Object as this core knows it: the larger of the
 * Largest its upstream reported on acceptance and the greatest Location
 * ingested on it. An Object counts from its first byte: an in-progress
 * record (moqr_core_ingest with MOQR_OBJ_OPEN) is the Largest Object while
 * it is the greatest, so the Largest can name an Object still arriving.
 * *has_largest is false while there is neither. The generation
 * guard matches moqr_core_track_key. Pure -- no mutation, no intents -- so
 * a binding can state the relay's Largest to a session that answers for it.
 */
MOQR_CORE_API moqr_result_t moqr_core_track_largest(const moqr_core_t *core,
                                                    moqr_track_t track,
                                                    uint64_t track_gen,
                                                    bool *has_largest,
                                                    uint64_t *largest_group,
                                                    uint64_t *largest_object);

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
 * subscription-terminal descriptor the binding maps through
 * moq_session_done_subscribe for the subscriber's wire profile),
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
 * moqr_core_ingest with desc->obj_state == MOQR_OBJ_OPEN opens the record, and
 * the track's Largest Object advances to it then, on its first byte; then
 * append its payload slices and either complete it or abandon it (neither
 * moves the Largest back). All address the OPEN record by
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
#define MOQR_RESET_CODE_EVICTED  0x1001u  /* truncated by capacity eviction or
                                            * a refused ingest               */
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
 * horizon is refused). Gaps inside the range are passed over silently unless the
 * request sets report_gaps (and report_unknown), which surfaces them as items
 * for the caller to mark.
 * No joining fetch, no chunk-through — those are future work; the seams here
 * don't change for them.
 */
typedef struct moqr_fetch { uint64_t _opaque; } moqr_fetch_t;

#ifdef __cplusplus
#define MOQR_FETCH_INVALID (moqr_fetch_t{0})
#else
#define MOQR_FETCH_INVALID ((moqr_fetch_t){ 0 })
#endif

MOQR_CORE_API bool moqr_fetch_is_valid(moqr_fetch_t h);
MOQR_CORE_API bool moqr_fetch_eq(moqr_fetch_t a, moqr_fetch_t b);

/* The parts of a fetch range stated relative to the track's Largest Object
 * {lg, lo} (moqr_fetch_req_t.range_form), resolved by moqr_core_fetch_open
 * against the core's own Largest in the same call that plans the range. */
#define MOQR_FETCH_START_RELATIVE    0x1u  /* start {lg + 1 - relative_start, 0},
                                            * clamped at group 0               */
#define MOQR_FETCH_START_NEXT_OBJECT 0x2u  /* start {lg, lo + 1}                */
#define MOQR_FETCH_END_AT_LARGEST    0x4u  /* end at {lg, lo}, inclusive        */

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
    /* Appended (prefix-safe; absent means false): report the Locations inside
     * the range that the log does not hold but has evidence for as
     * MOQR_FETCH_ITEM_GAP items instead of passing over them silently. A gap
     * is a run of Locations within a retained group's extent (at or below the
     * greatest id any record of the group carries) that no record accounts
     * for -- a status object there is a known non-object and is skipped; an
     * abandoned record counts as not held, and an Object still arriving is
     * waited for, not reported (see report_pending). Locations
     * past a group's extent, and groups the log never saw, carry no
     * evidence either way and are not reported as gaps. */
    bool               report_gaps;
    /* Appended (prefix-safe; absent means false; read only with
     * report_gaps): also report the Locations inside the range the log has
     * no evidence about as MOQR_FETCH_ITEM_UNKNOWN items -- the Locations
     * past the extent of a group whose final object is not known
     * (moqr_log_group_final), every Location of a group the log never
     * saw, and every Object retained past the source's MAX_CACHE_DURATION
     * (expired from the cache: never served, and not known not to exist). A Location past a known group final does not exist and is never
     * reported, so with both flags set every Location of the range is
     * accounted for: served, reported, or known not to exist. It sits in the
     * tail padding after report_gaps, so the sizeof is unchanged;
     * moqr_fetch_req_init clears it. */
    bool               report_unknown;
    /* Appended (prefix-safe; absent means zero): with
     * MOQR_FETCH_START_RELATIVE, the start's distance in groups before the
     * group after the Largest Object (0 is that next group, 1 the Largest
     * Object's own group). */
    uint64_t           relative_start;
    /* Appended (prefix-safe; absent means 0): MOQR_FETCH_* bits naming the
     * parts of the range stated relative to the track's Largest Object; the
     * start_* (end_*) fields are ignored for a part a bit names. The range
     * resolves against the Largest Object when the fetch is opened, and is
     * then checked against it exactly as an explicit one is: no Largest, or
     * a start past it, is INVALID_RANGE, and the plan's End is the resolved
     * end. */
    uint32_t           range_form;
    /* Appended (prefix-safe; absent means false): report an Object still
     * arriving at the cursor as a MOQR_FETCH_ITEM_PENDING item instead of
     * answering MOQR_ERR_WOULD_BLOCK until it is whole. */
    bool               report_pending;
} moqr_fetch_req_t;

/*
 * The symbol clears and stamps the layout through `cookie` -- the struct as a
 * caller built before report_gaps was appended holds it -- so it never writes
 * past such a caller's storage. A caller built against this header reaches
 * the whole struct: the function-like macro below passes its sizeof to
 * moqr_fetch_req_init_sized(), which clears and stamps
 * min(req_size, sizeof current).
 */
MOQR_CORE_API void moqr_fetch_req_init(moqr_fetch_req_t *req);
MOQR_CORE_API void moqr_fetch_req_init_sized(moqr_fetch_req_t *req,
                                             size_t req_size);
#define moqr_fetch_req_init(req) \
    moqr_fetch_req_init_sized((req), sizeof(*(req)))

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
#define MOQR_FETCH_ITEM_GAP    4u   /* report_gaps: Locations not held      */
#define MOQR_FETCH_ITEM_UNKNOWN 5u  /* report_unknown: Locations without
                                     * evidence either way                 */
#define MOQR_FETCH_ITEM_PENDING 6u  /* report_pending: an Object still
                                     * arriving                            */

typedef struct moqr_fetch_item {
    moqr_fetch_item_kind_t kind;
    moqr_record_view_t     rec;        /* OBJECT: view valid while pinned. rec.payload
                                        * is the single object rcbuf; a chunked
                                        * COMPLETE record is coalesced into an owned
                                        * buffer, so rec.chunk_count
                                        * is presented as 0 — see fetch_peek.       */
    /* MARKER: emit an end-of-range marker IMMEDIATELY BEFORE this (first
     * retained) group -- i.e. moq_session_write_fetch_range_before_group with
     * MOQ_FETCH_RANGE_UNKNOWN (or Timed-Out where the request's wait budget
     * is spent). Covers the evicted prefix the fetch's Start fell into; the
     * marker carries no payload (no pin).
     * GAP (report_gaps fetches only): rec.group_id / rec.object_id carry the
     * INCLUSIVE end Location of a run of Locations the log does not hold,
     * starting right after the last committed item (or at the range Start);
     * every other rec field is zero and there is no pin.
     * UNKNOWN (report_unknown fetches only): the same shape as GAP, for a run
     * the log has no evidence about. An object id of UINT64_MAX ends the run
     * with its group.
     * PENDING (report_pending fetches only): rec.group_id / rec.object_id is
     * the Location of an Object whose first bytes are in the log and whose
     * last are not; every other rec field is zero and there is no pin. */
    uint64_t               marker_group;
} moqr_fetch_item_t;

/*
 * Open a fetch. Resolves the track, resolves the parts of the range stated
 * relative to its Largest Object (range_form) against the Largest this core
 * holds at the call, validates the range, and (on ACCEPT) reserves a cursor
 * slot. *out_plan carries the admission decision:
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
  *   - MOQR_FETCH_ITEM_GAP (report_gaps only): the Locations from the cursor up
 *     to and including rec.{group_id,object_id} are not held (an abandoned
 *     record is not held; an Object still arriving is not a gap). Nothing is
 *     pinned and nothing is remembered: a re-peek recomputes the gap from the
 *     log as it stands, so an object that arrives meanwhile shrinks or closes
 *     it and is then served in order. Commit skips past the reported end.
 *   - MOQR_FETCH_ITEM_UNKNOWN (report_unknown only): the Locations from the
 *     cursor up to and including rec.{group_id,object_id} are ones the log
 *     has no evidence about. It runs through to just before the next group
 *     the log holds, or to the range end; a run of Objects expired from the
 *     cache runs through the last of them. It is recomputed on every peek
 *     and committed like a GAP.
 *   - MOQR_FETCH_ITEM_PENDING (report_pending only): the next Location to
 *     serve holds an Object still arriving (its first bytes are in, it is not
 *     yet complete): the Largest Object can be such an Object, and a range
 *     that includes it includes it in full. Reported once whatever precedes
 *     it is served or reported; a re-peek serves the Object once it is whole,
 *     or reports its Location as a GAP once its record is abandoned. A caller
 *     that will not wait any longer commits it, which skips its one Location
 *     like a GAP ending there.
 *   - MOQR_FETCH_ITEM_END: the range is exhausted; FIN the fetch.
 * Otherwise: MOQR_ERR_WOULD_BLOCK when the pin budget is momentarily full
 * (another fetch holds it — retry after it commits), or, without
 * report_pending, while the next Location holds an Object still arriving
 * (retry once it completes or is abandoned), MOQR_ERR_CAPACITY when a
 * single object exceeds the whole pin budget (a terminal the caller resets the
 * fetch stream on — unreachable while fetch_pin_bytes is floored to the log
 * budget, kept as defense), MOQR_ERR_TOO_OLD when the range fell below the
 * retention horizon mid-fetch (terminal), or MOQR_ERR_STALE_HANDLE.
 */
MOQR_CORE_API moqr_result_t moqr_core_fetch_peek(moqr_core_t *core, moqr_fetch_t fetch,
                                   uint64_t now_us, moqr_fetch_item_t *out);

/* Advance past the peeked head item: an OBJECT advances the cursor and unpins it;
 * a MARKER is cleared without unpinning (it holds no pin, and the cursor already
 * starts at the first retained group); a GAP, UNKNOWN or PENDING moves the cursor
 * to the Location after its reported end (the range is exhausted when that end
 * was the range end). MOQR_ERR_WRONG_STATE without a prior peek of a MARKER,
 * GAP, UNKNOWN, PENDING or OBJECT;
 * MOQR_ERR_STALE_HANDLE on a dead fetch. */
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
 * its terminal, or a push publisher that finished (PUBLISH_FINISHED):
 * terminate every downstream subscriber on the track with PUBLISH_DONE
 * carrying `status` for its wire profile (so none hangs behind a sourceless
 * ACTIVE track), then release the source like moqr_core_upstream_lost (WARM
 * if retained content remains, else freed). Generation-guarded;
 * MOQR_ERR_STALE_HANDLE for a stale track, MOQR_ERR_WRONG_STATE when not
 * ACTIVE, MOQR_ERR_WOULD_BLOCK when the intent ring can't hold one SUB_DONE
 * per downstream sub (no state change — retry after a drain).
 */
MOQR_CORE_API moqr_result_t moqr_core_source_done(moqr_core_t *core, moqr_track_t track,
                                    uint64_t track_gen, moqr_pd_desc_t status,
                                    uint64_t now_us);

/*
 * The track's SOURCE finished gracefully and no further objects will be
 * admitted for it: an upstream subscription that got its terminal, or a
 * push publisher that finished (PUBLISH_FINISHED). Unlike
 * moqr_core_source_done, which terminates every downstream subscriber at
 * once (for failures such as a malformed track), each forwarding subscriber
 * first receives what was already admitted -- records, seals and FINs in
 * their usual order -- and is then terminated with PUBLISH_DONE carrying
 * `status` for its wire profile from moqr_core_next_delivery, so the terminal
 * follows the data it closes. A paused subscriber (Forward=0) asked for
 * nothing further and is terminated by its binding's next delivery scan.
 * The source is released as by moqr_core_source_done. Every terminal comes
 * from the delivery scans, so this call queues no intent and never returns
 * MOQR_ERR_WOULD_BLOCK: MOQR_OK, MOQR_ERR_INVAL, MOQR_ERR_STALE_HANDLE or
 * MOQR_ERR_WRONG_STATE (the track is not ACTIVE).
 */
MOQR_CORE_API moqr_result_t moqr_core_source_finish(moqr_core_t *core, moqr_track_t track,
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
    /* Subset of tracks that are pure cache and would be reclaimed first:
     * WARM (source released), no subscription, no fetch. A table full of
     * reclaimable tracks still admits every new track. */
    uint32_t tracks_reclaimable;
    uint32_t subs;              /* all live subscriptions                 */
    uint32_t subs_parked;       /* subset awaiting upstream resolution     */
    uint32_t subs_active;       /* subset serving from a track log         */
    uint32_t ns_nodes;
    /* Subset of ns_nodes that no announce, live track or unpolled intent
     * references; freed when an announce needs the room. */
    uint32_t ns_nodes_reclaimable;
    uint32_t ns_subs;
    uint64_t retained_bytes;    /* across all track logs (gauge)          */
    uint64_t ingested_total;
    uint64_t delivered_total;
    uint64_t evict_sweeps;      /* times the evict-reset sweep body ran; stays
                                 * flat under whole-object delivery (gated off
                                 * unless a sub has begun a live-edge object)   */
    uint64_t evicted_total;     /* records evicted, incl. from freed tracks
                                 * and the cache of reclaimed tracks          */
    /* Reclaimable tracks freed: to admit a new track into a full table
     * (least recently used first), or after warm_idle_us without interest. */
    uint64_t tracks_evicted_capacity;
    uint64_t tracks_evicted_idle;
    /* Namespace trie nodes freed once nothing referenced them. */
    uint64_t ns_nodes_reclaimed;
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

/* Sized access requires the entire record as declared by these headers,
 * preserves bytes beyond it, and leaves output unchanged on refusal. Call on
 * the owning shard, or after all its writers have stopped. */
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
