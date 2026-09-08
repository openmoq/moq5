#ifndef MOQRELAY_LOG_H
#define MOQRELAY_LOG_H

/*
 * A bounded, immutable, per-track object log, and cursors —
 * the only consumer abstraction (see docs/architecture.md).
 *
 * One log holds one track's recent objects as refcounted records grouped
 * into group entries. Appending takes ownership of the caller's payload/
 * properties rcbuf refs (one retain at ingest — typically the owned ref a
 * session event poll transferred). Every consumer — live subscriber, fetch,
 * cache retention, cross-shard pump — is a cursor holding a POSITION only;
 * cursors never retain payloads, so retained memory is a function of log
 * budgets, never of consumer count.
 *
 * -- Spec anchors (verified against local Specs/, 2026-07-02) --
 *
 *  - Payload opacity: a relay MUST treat object payloads as opaque and
 *    MUST NOT combine, split, or otherwise modify them; object fields are
 *    forwarded unmodified (except Object Properties per its §2.5).
 *    draft-ietf-moq-transport-18 §9.7 (:2876-2881); -16 §8.7 (:2164-2173).
 *    The log stores rcbuf handles and never reads payload bytes.
 *  - Ordering: objects within a group, and within a subgroup, are in
 *    ascending Object ID order (-18 :809, :848; -16 :605, :641). Append
 *    enforces strictly ascending Object IDs per subgroup; a violation is
 *    refused (the binding decides whether it is a malformed track,
 *    -18 :1044-1080).
 *  - Forwarding preference: an object with preference Datagram "does not
 *    belong to a Subgroup in any way" (-18 :854-857) — such records live
 *    in a per-group datagram list, never a synthetic subgroup.
 *  - Priorities: lower number = higher priority, 0 highest (-18 :2341).
 *    The log stores publisher_priority verbatim; ranked delivery is the
 *    scheduling seam in relay.h (-18 §7.1.1 :2380-2400).
 *  - End of group / track: the final object in a group is signalled by an
 *    END_OF_GROUP status object, a subgroup EOG bit before FIN
 *    (-18 :5289), or a datagram EOG bit (-18 :5145); END_OF_TRACK ends the
 *    track (-18 :1071). Appends beyond a known final are refused.
 *
 * -- Iteration model --
 *
 * Cursors traverse records in ARRIVAL ORDER (a per-log monotonic
 * arrival_seq). Arrival order preserves within-subgroup wire order by
 * construction (ascending Object IDs), is live-correct while groups are
 * still open, and is fully deterministic. Group-order / priority
 * re-ordering across cursors is deliberately NOT the log's job — that is
 * the delivery-ranking seam in relay.h (see relay.h delivery ranking). Fetch-style group-indexed
 * access arrives with fetch backfill (future work).
 *
 * -- Eviction --
 *
 * Group-granular, oldest group first, never the group currently being
 * appended. Eviction decrefs every retained payload/properties ref exactly
 * once and tombstones the group's journal slots; a cursor positioned in an
 * evicted range observes `skipped` on its next read and resumes at the
 * oldest retained record (group-boundary-clean by construction). Age
 * eviction runs from moqr_log_tick(now_us) — time is an input, never read.
 *
 * -- Borrowing --
 *
 * moqr_record_view_t borrows from the log: pointers are valid until the
 * next mutating call on the same log (append / tick / evict / destroy).
 * A consumer that needs the payload past that point increfs it (the session
 * binding hands it to moq_session_write_object_ex, which increfs
 * internally — no copy either way).
 *
 * Thread safety: none. One log belongs to one shard (single writer).
 */

#include "trace.h"
#include "types.h"
#include "wire_codes.h"

/* Full definition in <moq/rcbuf.h> (included by users of the payload). */
typedef struct moq_rcbuf moq_rcbuf_t;

#ifdef __cplusplus
extern "C" {
#endif

typedef struct moqr_log moqr_log_t;

/* Semantic object status (values mirror the session core's
 * moq_object_status_t; kept separate so log.h does not pull session.h). */
typedef uint8_t moqr_obj_status_t;

#define MOQR_OBJ_NORMAL        ((moqr_obj_status_t)0)
#define MOQR_OBJ_END_OF_GROUP  ((moqr_obj_status_t)1)
#define MOQR_OBJ_END_OF_TRACK  ((moqr_obj_status_t)2)

/* Object completeness. the current ingest populates COMPLETE records only; OPEN is the
 * chunk-through seam (chunk-through streaming, future work) — the field exists so enabling streaming
 * ingest later changes no record layout. */
typedef uint8_t moqr_obj_state_t;

#define MOQR_OBJ_COMPLETE   ((moqr_obj_state_t)0)
#define MOQR_OBJ_OPEN       ((moqr_obj_state_t)1)
#define MOQR_OBJ_ABANDONED  ((moqr_obj_state_t)2)

/* -- Configuration ---------------------------------------------------- */

typedef struct moqr_log_cfg {
    uint32_t           struct_size;
    const moq_alloc_t *alloc;        /* required; the shard allocator      */
    moqr_log_budget_t  budget;       /* groups / bytes / age (0 = default) */
    uint32_t           max_subgroups_per_group;  /* 0 = default 16         */
    uint32_t           max_objects_per_group;    /* subgroup records +
                                                  * datagrams; 0 = def 4096 */
    uint32_t           max_cursors;              /* 0 = default 64         */
    uint32_t           max_chunk_nodes;          /* OPEN-record chunk-node
                                                  * pool; 0 = default 8192  */
    moqr_trace_t      *trace;        /* optional; BORROWED, must outlive   */
    uint64_t           trace_id;     /* scalar track id in trace records   */
} moqr_log_cfg_t;

/* Prefix-safe sized initializer (same contract as moqr_core_cfg_init_sized:
 * clears+stamps min(cfg_size, sizeof), writes a default only into fields the
 * declared size fully contains, never touches bytes past cfg_size). */
MOQR_CORE_API void moqr_log_cfg_init_sized(moqr_log_cfg_t *cfg, size_t cfg_size,
                             const moq_alloc_t *alloc);

/* -- Lifecycle --------------------------------------------------------- */

/*
 * Create a log. All pools (group slots, journal ring, cursor pool) are
 * allocated up front from cfg->alloc and sized from the cfg — the closed
 * form is moqr_log_capacity_describe() in capacity.h. Per-group record
 * vectors grow lazily up to the configured caps.
 */
MOQR_CORE_API moqr_result_t moqr_log_create(const moqr_log_cfg_t *cfg, moqr_log_t **out);

/* Destroy: decrefs every retained payload/properties ref exactly once and
 * frees all structure through the creating allocator. NULL is a no-op. */
MOQR_CORE_API void moqr_log_destroy(moqr_log_t *log);

/* -- Append (ingest) ---------------------------------------------------- */

typedef struct moqr_log_append_desc {
    uint32_t          struct_size;
    uint64_t          group_id;
    uint64_t          subgroup_id;   /* ignored when datagram_pref          */
    uint64_t          object_id;
    uint8_t           publisher_priority;  /* raw; lower = higher (§7.1)   */
    moqr_obj_status_t status;
    bool              datagram_pref; /* preference Datagram: no subgroup    */
    bool              end_of_group;  /* EOG evidence on this record (bit or
                                      * status; see spec anchors above)     */
    moq_rcbuf_t      *payload;       /* ownership TRANSFERS on success;
                                      * NULL for status-only objects        */
    moq_rcbuf_t      *properties;    /* ownership TRANSFERS on success;
                                      * NULL if none                        */
    uint64_t          now_us;        /* arrival time (virtual ok)           */
    /* Chunk-through: default 0 == COMPLETE whole-object append. Set
     * obj_state = MOQR_OBJ_OPEN (with payload == NULL) via moqr_log_open_record
     * to begin an incrementally-filled record; declared_len is the wire payload
     * length known up front. Ignored for COMPLETE appends. */
    moqr_obj_state_t  obj_state;
    uint64_t          declared_len;
} moqr_log_append_desc_t;

MOQR_CORE_API void moqr_log_append_desc_init(moqr_log_append_desc_t *d);

/*
 * Append one complete object record.
 *
 * On MOQR_OK the log owns desc->payload/properties (released on eviction
 * or destroy). On ANY error the caller keeps its refs.
 *
 * Byte/group/record budgets are enforced by evicting oldest groups first
 * (never the group being appended). Errors:
 *   MOQR_ERR_CAPACITY  budgets cannot admit this record even after
 *                      eviction (e.g. it targets the oldest group, or the
 *                      object alone exceeds max_bytes).
 *   MOQR_ERR_TOO_OLD   the record's group was already evicted (late
 *                      arrival past the retention horizon) — callers
 *                      typically drop and count.
 *   MOQR_ERR_INVAL     ordering violation: non-ascending Object ID within
 *                      the subgroup, duplicate datagram Object ID, append
 *                      beyond a known group-final / end-of-track, or a
 *                      malformed descriptor.
 */
MOQR_CORE_API moqr_result_t moqr_log_append(moqr_log_t *log,
                              const moqr_log_append_desc_t *desc);

/* -- Chunk-through: OPEN records --------------------------------------------
 *
 * A large object may be admitted incrementally instead of whole: open a record
 * (declared payload length known up front, per the wire), append its payload as
 * zero-copy rcbuf slices, then complete or abandon it. OPEN and ABANDONED
 * records are retained and visible to readers via the record view (obj_state),
 * but the delivery cursor does NOT deliver them — it skips non-COMPLETE
 * records (incremental delivery is a separate path). Object identity/order
 * rules apply at open (a duplicate object id is rejected then, and the OPEN
 * record occupies its ascending subgroup slot immediately). Records are
 * addressed by (group, subgroup, object) — the wire identity.
 */

/*
 * Open an incrementally-filled record. `desc` must carry obj_state ==
 * MOQR_OBJ_OPEN, payload == NULL, and datagram_pref == false; declared_len is
 * the wire payload length. The record is admitted OPEN with only its properties
 * bytes accounted; chunk bytes are added by moqr_log_append_chunk as accepted.
 * Errors mirror moqr_log_append (INVAL / TOO_OLD / CAPACITY); CAPACITY if
 * declared_len + properties cannot fit max_bytes even alone.
 */
MOQR_CORE_API moqr_result_t moqr_log_open_record(moqr_log_t *log,
                                   const moqr_log_append_desc_t *desc);

/*
 * Append one payload chunk (zero-copy rcbuf slice) to an OPEN record. The log
 * increfs `chunk` and retains it; its bytes count against retained_bytes as
 * accepted. Errors: MOQR_ERR_TOO_OLD (the record's group was evicted),
 * MOQR_ERR_WRONG_STATE (no OPEN record at that identity), MOQR_ERR_INVAL
 * (bytes would exceed declared_len), MOQR_ERR_CAPACITY (chunk-node pool
 * exhausted, or bytes cannot fit even after evicting older groups). On any
 * error the caller keeps its ref and the record stays OPEN.
 */
MOQR_CORE_API moqr_result_t moqr_log_append_chunk(moqr_log_t *log, uint64_t group_id,
                                    uint64_t subgroup_id, uint64_t object_id,
                                    moq_rcbuf_t *chunk);

/*
 * Finalize an OPEN record: validates accumulated payload == declared_len and
 * flips it to COMPLETE. MOQR_ERR_INVAL if bytes != declared_len (still OPEN),
 * MOQR_ERR_WRONG_STATE if no OPEN record, MOQR_ERR_TOO_OLD if evicted.
 */
MOQR_CORE_API moqr_result_t moqr_log_complete_record(moqr_log_t *log, uint64_t group_id,
                                       uint64_t subgroup_id,
                                       uint64_t object_id);

/*
 * Abandon an OPEN record (upstream reset/stop mid-object): releases its retained
 * chunks (decref + return nodes, retained bytes drop) and flips it to ABANDONED.
 * The record slot remains as an ABANDONED tombstone (holds its ascending
 * object-id slot; readers see obj_state == ABANDONED and skip it).
 * MOQR_ERR_WRONG_STATE if no OPEN record, MOQR_ERR_TOO_OLD if evicted.
 */
MOQR_CORE_API moqr_result_t moqr_log_abandon_record(moqr_log_t *log, uint64_t group_id,
                                      uint64_t subgroup_id, uint64_t object_id,
                                      moqr_reset_desc_t reset);

/*
 * Advance virtual time: evicts groups whose age exceeds budget.max_age_us
 * (whole groups, oldest first). Returns the number of groups evicted.
 */
MOQR_CORE_API size_t moqr_log_tick(moqr_log_t *log, uint64_t now_us);

/* Explicitly evict up to `max_groups` oldest groups. Returns evicted. */
MOQR_CORE_API size_t moqr_log_evict_groups(moqr_log_t *log, size_t max_groups);

/* Record an authoritative upstream eviction watermark: every group below
 * `oldest_retained` is gone at the source, so nothing more will ever arrive
 * for that range. Locally retained groups below it remain SERVABLE — their
 * unsealed subgroup lists are sealed through the ordinary seal path (which
 * drives the usual late-FIN notices and downstream closes) — while the
 * log's eviction visibility (`any_evicted` / `max_evicted_group_id`)
 * advances over the ABSENT range so probes there answer MOQR_ERR_TOO_OLD —
 * "never will" — instead of MOQR_DONE — "not yet". Monotonic: a stale or
 * duplicate watermark is a no-op. Returns the number of lists sealed. */
MOQR_CORE_API size_t moqr_log_note_evicted_below(moqr_log_t *log, uint64_t oldest_retained);

/* The first group id NOT covered by eviction knowledge: 0 when nothing was
 * ever evicted, else max_evicted_group_id + 1. Every group below the floor
 * is authoritatively closed for this log. */
MOQR_CORE_API uint64_t moqr_log_evicted_floor(const moqr_log_t *log);

/* Abandon every OPEN-tail record of a retained group with `reset_code` —
 * the group is dead at its source, so no completion can ever arrive. The
 * shard layer's tracked slots normally cover every partial (a partial is
 * applied through a slot), making this a defensive no-op; it exists so a
 * dead group can never retain an OPEN record the slot walk missed. Complete
 * retained records are preserved — never an eviction, never a seal.
 * Idempotent; MOQR_DONE when the group is not retained. */
MOQR_CORE_API moqr_result_t moqr_log_abandon_group_open(moqr_log_t *log, uint64_t group_id,
                                          moqr_reset_desc_t reset,
                                          uint32_t *out_abandoned);

/* -- Cursors ------------------------------------------------------------ */

typedef enum moqr_cursor_origin {
    MOQR_CURSOR_FROM_OLDEST = 1,  /* start at oldest retained record       */
    MOQR_CURSOR_FROM_LIVE   = 2,  /* start at the live edge (records
                                   * appended after open)                  */
} moqr_cursor_origin_t;

MOQR_CORE_API moqr_result_t moqr_log_cursor_open(moqr_log_t *log,
                                   moqr_cursor_origin_t origin,
                                   moqr_cursor_t *out);

/* Close/retire a cursor; the pool slot is recycled (generation bumped). */
MOQR_CORE_API moqr_result_t moqr_log_cursor_close(moqr_log_t *log, moqr_cursor_t cursor);

/* Borrowed view of one record; valid until the next mutating log call. */
typedef struct moqr_record_view {
    uint64_t           arrival_seq;
    uint64_t           arrival_us;   /* ingest time; 0 if unset by origin  */
    uint64_t           group_id;
    uint64_t           subgroup_id;  /* meaningless when datagram_pref     */
    uint64_t           object_id;
    uint8_t            publisher_priority;
    moqr_obj_status_t  status;
    moqr_obj_state_t   obj_state;    /* COMPLETE for whole-object ingest;
                                      * OPEN/ABANDONED for chunked records  */
    bool               datagram_pref;
    bool               end_of_group;
    const moq_rcbuf_t *payload;      /* BORROWED; NULL for status/chunked   */
    const moq_rcbuf_t *properties;   /* BORROWED; NULL if none             */
    /* Chunked records (chunk_count > 0): payload is NULL and the retained
     * bytes-so-far live in a chunk list read via moqr_log_view_chunk;
     * declared_len is the wire payload length. Whole-object records have
     * chunk_count == 0 and use `payload`. */
    uint32_t           chunk_count;
    uint64_t           declared_len;
    uint32_t           _chunk_head;  /* opaque pool index; use view_chunk   */
    moqr_reset_desc_t  reset;        /* ABANDONED: tagged terminal cause    */
} moqr_record_view_t;

/*
 * Read chunk `idx` (0-based, in append order) of a chunked record view. Sets
 * *out_buf to the BORROWED slice and *out_len to its length. Returns MOQR_OK,
 * MOQR_DONE when idx >= chunk_count, or MOQR_ERR_INVAL on bad args. The view
 * (and the returned slice) is valid until the next mutating log call.
 */
MOQR_CORE_API moqr_result_t moqr_log_view_chunk(const moqr_log_t *log,
                                  const moqr_record_view_t *view,
                                  uint32_t idx,
                                  const moq_rcbuf_t **out_buf,
                                  uint64_t *out_len);

/*
 * Read the next record in arrival order.
 *
 * Returns MOQR_OK with *out filled when a record is produced; MOQR_DONE
 * when the cursor is caught up (nothing new); MOQR_ERR_STALE_HANDLE /
 * MOQR_ERR_INVAL on a bad cursor.
 *
 * *out_skipped is set true when eviction removed records between the
 * cursor's position and the record returned (or the caught-up point) —
 * the cursor resumed at the oldest retained record. Group-granular
 * eviction makes every such resume a group boundary. The cursor remains
 * fully usable; skipping is reported, never silent
 * (MOQR_TRACE_CURSOR_SKIP is emitted).
 */
MOQR_CORE_API moqr_result_t moqr_log_cursor_next(moqr_log_t *log, moqr_cursor_t cursor,
                                   moqr_record_view_t *out,
                                   bool *out_skipped);

/*
 * Two-phase read for the pull-model delivery in relay.h: peek returns what next
 * would return WITHOUT advancing; commit advances past the last peeked
 * record. peek/peek is idempotent; commit without a prior peek on the
 * current position is MOQR_ERR_WRONG_STATE. Eviction between peek and
 * commit re-resolves safely (commit advances by sequence, not pointer).
 */
MOQR_CORE_API moqr_result_t moqr_log_cursor_peek(moqr_log_t *log, moqr_cursor_t cursor,
                                   moqr_record_view_t *out,
                                   bool *out_skipped);
MOQR_CORE_API moqr_result_t moqr_log_cursor_commit(moqr_log_t *log, moqr_cursor_t cursor);

/* -- Group-indexed reads -------------------------------------------------- *
 * Random access for schedulers (and, later, fetch): enumerate retained
 * groups, then read a group's per-subgroup lists (append order, which is
 * wire order) and its datagram list by index. Views borrow as usual.
 */

/* List slot addressing a group's datagram list (subgroup lists are
 * 0..list_count-1 in stable append order). */
#define MOQR_LOG_LIST_DATAGRAM UINT32_MAX

MOQR_CORE_API uint32_t moqr_log_max_groups(const moqr_log_t *log);
MOQR_CORE_API uint32_t moqr_log_max_subgroups(const moqr_log_t *log);

/* Retained group ids, ascending. Returns the count copied (<= cap). */
MOQR_CORE_API size_t moqr_log_group_ids(const moqr_log_t *log, uint64_t *out, size_t cap);

/* Indexed enumeration — no caller buffer, no window: group_count retained
 * groups, id_at(i) for i in [0, count), ascending; UINT64_MAX past the
 * end. Positions are only stable until the next mutating log call. */
MOQR_CORE_API uint32_t moqr_log_group_count(const moqr_log_t *log);

/* Monotonic count of groups evicted over the log's lifetime (never decreases).
 * A cheap O(1) eviction watermark — cursors gate their reclaim scan on it. */
MOQR_CORE_API uint64_t moqr_log_evicted_groups_total(const moqr_log_t *log);
MOQR_CORE_API uint64_t moqr_log_group_id_at(const moqr_log_t *log, uint32_t idx);

/* Number of subgroup lists a retained group currently has; 0 if the
 * group is not retained. */
MOQR_CORE_API uint32_t moqr_log_group_list_count(const moqr_log_t *log, uint64_t group_id);

/*
 * Read record `rec_idx` of a list. Returns MOQR_OK with a borrowed view;
 * MOQR_DONE when the list has no record at that index (yet); and
 * MOQR_ERR_TOO_OLD when the group was evicted (or never admitted below
 * the eviction watermark).
 */
MOQR_CORE_API moqr_result_t moqr_log_read_rec(const moqr_log_t *log, uint64_t group_id,
                                uint32_t list_slot, uint32_t rec_idx,
                                moqr_record_view_t *out);

/* Mark a subgroup list as SEALED: the upstream subgroup stream FIN'd, so no
 * further objects will be appended to (group_id, subgroup_id). Idempotent.
 * MOQR_OK when marked; MOQR_DONE when the group is evicted/gone or no such
 * subgroup list exists (e.g. an empty-header FIN with no resolved subgroup). */
MOQR_CORE_API moqr_result_t moqr_log_seal_subgroup(moqr_log_t *log, uint64_t group_id,
                                     uint64_t subgroup_id);

/* Mark a subgroup list terminally RESET(code): the upstream subgroup stream
 * ended abnormally, so no further objects will arrive -- the abnormal
 * counterpart of the seal. Retained COMPLETE records stay servable; the
 * downstream terminal is an abnormal close carrying `reset_code`, never a
 * clean FIN, and the list never reports subgroup_is_final (no FIN advisory
 * piggybacks on its record deliveries). Never resolves header-EOG finality:
 * a truncated stream cannot testify to the group's final object. First
 * terminal wins -- an already-sealed list (either flavor) keeps its flavor.
 * Same result contract as the seal: MOQR_OK when marked (or already
 * terminal), MOQR_DONE when the group is evicted/gone or no such subgroup
 * list exists. */
MOQR_CORE_API moqr_result_t moqr_log_reset_subgroup(moqr_log_t *log, uint64_t group_id,
                                      uint64_t subgroup_id,
                                      moqr_reset_desc_t reset);

/* True iff (group_id, list_slot) is SEALED and object_id is its last retained
 * object — i.e. delivering this record completes a sealed subgroup, so the
 * downstream subgroup stream can be closed. False for the datagram list. */
MOQR_CORE_API bool moqr_log_subgroup_is_final(const moqr_log_t *log, uint64_t group_id,
                                uint32_t list_slot, uint64_t object_id);

/* True iff (group_id, list_slot) is a retained subgroup list marked SEALED,
 * outputting its wire subgroup id. False for the datagram list, an evicted or
 * absent group, or an unsealed list. Pairs with moqr_log_read_rec returning
 * MOQR_DONE at a cursor's index to answer "sealed and fully consumed" — the
 * condition under which a downstream subgroup stream owes its FIN even though
 * no further record delivery will ever carry the final-record flag. */
MOQR_CORE_API bool moqr_log_subgroup_sealed(const moqr_log_t *log, uint64_t group_id,
                              uint32_t list_slot, uint64_t *subgroup_id);

/* True iff (group_id, list_slot) is a retained subgroup list whose terminal
 * was a RESET, outputting its stored application code. subgroup_sealed
 * answers true for both flavors (eligibility is shared); this query is the
 * flavor discriminator. */
MOQR_CORE_API bool moqr_log_subgroup_reset(const moqr_log_t *log, uint64_t group_id,
                             uint32_t list_slot,
                             moqr_reset_desc_t *out_reset);

/* -- Introspection ------------------------------------------------------ */

typedef struct moqr_log_stats {
    uint64_t retained_bytes;    /* payload+properties bytes retained       */
    uint32_t group_count;       /* retained group entries                  */
    uint64_t record_count;      /* live records across all groups          */
    uint32_t cursor_count;      /* open cursors                            */
    uint64_t appended_total;
    uint64_t evicted_groups_total;
    uint64_t evicted_records_total;
    uint64_t oldest_group_id;   /* UINT64_MAX when empty                   */
    uint64_t newest_group_id;   /* UINT64_MAX when empty                   */
    bool     end_of_track;      /* END_OF_TRACK observed                   */
} moqr_log_stats_t;

MOQR_CORE_API void moqr_log_get_stats(const moqr_log_t *log, moqr_log_stats_t *out);
MOQR_CORE_API moqr_result_t moqr_log_get_stats_sized(
    const moqr_log_t *log, moqr_log_stats_t *out, size_t out_size);

#ifdef __cplusplus
}
#endif

#endif /* MOQRELAY_LOG_H */
