#ifndef MOQR_RETRIEVE_INTERNAL_H
#define MOQR_RETRIEVE_INTERNAL_H

#include <moq/relay/relay.h>
#include "cluster_internal.h"

/*
 * Same-build internal interface between relay-core and the relay runtime.
 * Never installed and not an SDK API: the runtime and the core it calls come
 * from the same build. Owner-thread only, like every core call. Values only;
 * the owner is an ordinary generation-checked moqr_fetch_t.
 *
 * A standalone FETCH that this relay may complete from upstream is admitted
 * once by moqr_core_internal_retrieve_open. The returned owner holds one slot
 * of the core's FETCH pool (counted against max_fetches exactly like
 * moqr_core_fetch_open) for the whole request, keeps the track pinned against
 * age eviction like any open FETCH, and is released with
 * moqr_core_fetch_close. Its admitted range and the track generation it was
 * admitted against never change.
 *
 * The owner is also the request's only cache reader: the caller positions
 * its view with moqr_core_internal_retrieve_view and reads it with
 * moqr_core_fetch_peek / moqr_core_fetch_commit. A view reports every
 * Location it does not serve (report_gaps, report_unknown and
 * report_pending are always on), including Locations below the retention
 * horizon, which a view reports as unknown instead of failing TOO_OLD: the
 * caller retrieves them upstream.
 */
typedef struct moqr_core_retrieve_plan {
    /* The admission decision and the FETCH_OK fields, exactly as
     * moqr_core_fetch_open reports them. */
    moqr_fetch_plan_t wire;
    /* ACCEPT: the admitted track and its source generation. */
    moqr_track_t track;
    uint64_t     track_gen;
    /* ACCEPT: the resolved range, both ends inclusive; an end_object of
     * UINT64_MAX is the whole end group. */
    uint64_t     start_group, start_object;
    uint64_t     end_group, end_object;
    /* REJECT only: nonzero when another shard of this relay owns the track
     * (this core holds no copy of it, or holds a copy that shard feeds). It is
     * the cookie of that shard's binding here; the request is that shard's to
     * answer, and the error code is the one a request this relay cannot route
     * gets. */
    uint64_t     remote_cookie;
} moqr_core_retrieve_plan_t;

/*
 * Admit a standalone FETCH for retrieval. The request is resolved and
 * checked as moqr_core_fetch_open does (range form against this core's
 * Largest Object, DOES_NOT_EXIST, INVALID_RANGE for no Largest Object, a
 * start past it or an empty range), with three differences: DESCENDING
 * group order is accepted; a range this core's log does not hold (evicted
 * or never retained) is accepted rather than refused; and a track another
 * shard owns is refused with remote_cookie set. An ACCEPT plan's owner view
 * starts at the admitted start and covers the admitted range.
 * Returns MOQR_OK with a plan (REJECT allocates nothing), MOQR_ERR_INVAL,
 * MOQR_ERR_UNSUPPORTED for an unknown group order, MOQR_ERR_STALE_HANDLE for
 * a dead binding, or MOQR_ERR_CAPACITY when the FETCH pool is exhausted.
 */
MOQR_CORE_API moqr_result_t moqr_core_internal_retrieve_open(
    moqr_core_t *core, moqr_binding_t binding, const moqr_fetch_req_t *req,
    uint64_t now_us, moqr_fetch_t *owner, moqr_core_retrieve_plan_t *plan);

/* Same admission with original source generation and requester identity pinned
 * across a shard mailbox. Never admits cached data from another publisher. */
MOQR_CORE_API moqr_result_t moqr_core_internal_retrieve_open_cluster(
    moqr_core_t *core, moqr_binding_t binding, const moqr_fetch_req_t *req,
    moqr_cluster_advertisement_id_t source,
    uint64_t requester_hop_id, uint64_t requester_correlation_id,
    uint64_t now_us, moqr_fetch_t *owner, moqr_core_retrieve_plan_t *plan);

/*
 * Move the owner's view to [start .. end] (inclusive, end_object UINT64_MAX
 * for the whole end group), which must lie inside the admitted range. Drops
 * whatever the previous view had peeked (its pin and its reported run) and
 * nothing else: the admission and its slot stay. MOQR_ERR_INVAL for a view
 * outside the admitted range or an inverted one; MOQR_ERR_STALE_HANDLE when
 * the owner is not live, is not a retrieval owner, or its track is no longer
 * the generation it was admitted against.
 */
MOQR_CORE_API moqr_result_t moqr_core_internal_retrieve_view(
    moqr_core_t *core, moqr_fetch_t owner, uint64_t start_group,
    uint64_t start_object, uint64_t end_group, uint64_t end_object);

/*
 * The track an open FETCH (moqr_core_fetch_open) or retrieval owner reads,
 * and that track's current source generation, so the runtime can state the
 * track's properties in the answer. MOQR_ERR_STALE_HANDLE when the fetch is
 * not live or its track is no longer the one it was opened on. Pure: no
 * mutation, no intents.
 */
MOQR_CORE_API moqr_result_t moqr_core_internal_fetch_track(
    const moqr_core_t *core, moqr_fetch_t fetch, moqr_track_t *track,
    uint64_t *track_gen);

/*
 * The outstanding record delivery's per-record marks, which the public
 * record view does not carry: the upstream subgroup header's PROPERTIES
 * form, and whether the record is not known to be the next object of the
 * stream its list continues (an earlier object of the subgroup was refused
 * at ingest or expired from cache; draft-22 §11.3.2). WRONG_STATE when the
 * binding has no outstanding record delivery (none, or a notice). Pure.
 */
MOQR_CORE_API moqr_result_t moqr_core_internal_delivery_flags(
    const moqr_core_t *core, moqr_binding_t binding,
    bool *subgroup_properties, bool *follows_gap, moqr_reset_desc_t *gap_reset,
    uint32_t *list);

/* Reconcile only this owner's locally ended outgoing subgroup, including a
 * STALLED or paused head with no outstanding delivery. `list` is the stable
 * position captured from delivery_flags when that stream opened; it also
 * identifies its begun ledger after group eviction. Consume at most budget
 * records (OPEN heads included), with DISCARDED accounting. An unrelated
 * held delivery and all shared log data remain untouched. `more` requests
 * another bounded pass; `retired` proves authoritative eviction or that the
 * exact subscription owner is gone. A seal alone never retires the numeric
 * key. No notice is acked. STALE_HANDLE for a gone binding/owner/track (only
 * the gone owner also sets retired), INVAL for a foreign binding or bad
 * position, WRONG_STATE for an absent key above the eviction horizon,
 * CAPACITY for unavailable cursor storage. Outputs are borrowed for the call.
 */
MOQR_CORE_API moqr_result_t moqr_core_internal_discard_subgroup(
    moqr_core_t *core, moqr_binding_t binding, moqr_sub_t sub,
    uint64_t group, uint64_t subgroup, uint32_t list, uint32_t budget,
    uint64_t now_us, bool *retired, bool *more);

/* Under demonstrated binder-slot pressure only, omit records that cannot
 * acquire a downstream stream. The synchronous predicate is pure and must
 * not reenter the core; its pointers are borrowed for the call. Notices and
 * evict resets remain eligible, and the original ranking applies among all
 * eligible work, including records already owning a binder slot. Existing
 * held work is preserved. Ordinary next_delivery is unchanged. */
typedef bool (*moqr_core_delivery_eligible_fn)(void *ctx, uint64_t sub_cookie,
                                               const moqr_record_view_t *rec);
MOQR_CORE_API moqr_result_t moqr_core_internal_next_eligible(
    moqr_core_t *core, moqr_binding_t binding, uint64_t now_us,
    moqr_delivery_t *out, moqr_core_delivery_eligible_fn eligible, void *ctx);

/* End every held rendezvous SUBSCRIBE whose deadline has passed at `now_us`
 * with TIMEOUT (one queued now, or owed until the intent ring takes it). The
 * binding calls it before it consumes a pump's events, so a source arriving
 * at or after a deadline never resumes that subscription. */
MOQR_CORE_API moqr_result_t moqr_core_internal_rendezvous_expire(moqr_core_t *core,
                                                                 uint64_t now_us);

/* True only for the same retained track immediately after one ordinary
 * source release. Permits queued old OPEN cleanup after idle linger, while
 * refusing active/pending replacement generations and replaced track handles. */
MOQR_CORE_API bool moqr_core_internal_source_idle(const moqr_core_t *core,
    moqr_track_t track, uint64_t old_source_gen);

/* The immediate end of a failed source for when moqr_core_source_done
 * cannot queue its terminals (MOQR_ERR_WOULD_BLOCK that a drain did not
 * clear): every subscription of the track is retired with `status` by its
 * binding's next delivery scan, before anything else is delivered for it,
 * and the source is released now, exactly as by moqr_core_source_done. It
 * queues no intent and is never refused for room: MOQR_OK, MOQR_ERR_INVAL,
 * MOQR_ERR_STALE_HANDLE or MOQR_ERR_WRONG_STATE (the track is not ACTIVE). */
MOQR_CORE_API moqr_result_t moqr_core_internal_source_abort(
    moqr_core_t *core, moqr_track_t track, uint64_t track_gen,
    moqr_pd_desc_t status);

/* End one ACTIVE subscription alone with `status` (moqr_core_internal_
 * source_abort's per-subscription form): its binding's next delivery scan
 * retires it with a SUB_DONE before delivering anything else for it, and
 * keeps it marked while the intent ring is full. MOQR_OK,
 * MOQR_ERR_INVAL, MOQR_ERR_STALE_HANDLE or MOQR_ERR_WRONG_STATE (not
 * ACTIVE). */
MOQR_CORE_API moqr_result_t moqr_core_internal_sub_fail(moqr_core_t *core,
                                                       moqr_sub_t sub,
                                                       moqr_pd_desc_t status);

/* Hold one free subscription slot for a later moqr_core_subscribe made with
 * req->reserved, so that no other request takes it meanwhile: the binding
 * reserves before it offers a discovered track on the wire, and subscribes
 * after. MOQR_ERR_CAPACITY when none is free. Each successful reservation is
 * consumed by such a subscribe or returned with
 * moqr_core_internal_sub_release_reserve. */
MOQR_CORE_API moqr_result_t moqr_core_internal_sub_reserve(moqr_core_t *core);
MOQR_CORE_API void moqr_core_internal_sub_release_reserve(moqr_core_t *core);

/* An object for `track`'s (group_id, subgroup_id) was not retained (its
 * ingest was refused): see moqr_log_note_gap. Downstream, the subgroup's
 * next record continues on a new stream, and the subgroup ends in a reset
 * rather than a FIN (draft-22 §7.4: a relay "MUST NOT reorder or drop
 * objects received on a multi-object stream"; §11.3.2). MOQR_OK / MOQR_DONE
 * as the log answers; MOQR_ERR_STALE_HANDLE on a dead track. */
MOQR_CORE_API moqr_result_t moqr_core_internal_note_gap(moqr_core_t *core, moqr_track_t track,
                                               uint64_t group_id,
                                               uint64_t subgroup_id);

/* How an object refused by ingest relates to the record already retained at
 * its Location (see moqr_log_duplicate). */
#define MOQR_LOG_DUP_NONE      0u  /* nothing retained at that Location to
                                    * compare with                           */
#define MOQR_LOG_DUP_IDENTICAL 1u  /* same Delivery Mode, Subgroup ID,
                                    * Priority, status and payload bytes     */
#define MOQR_LOG_DUP_DIFFERS   2u  /* any of those differs: "An endpoint that
                                    * receives a duplicate Object with a
                                    * different Delivery Mode, Subgroup ID,
                                    * Priority or Payload MUST treat the track
                                    * as Malformed"                          */
#define MOQR_LOG_DUP_UPDATE    3u  /* a permitted update of the retained
                                    * object: existing to not existing       */
#define MOQR_LOG_DUP_PENDING   4u  /* a streamed copy whose metadata and
                                    * declared length match: its payload is
                                    * compared chunk by chunk                */
#define MOQR_LOG_DUP_UNVERIFIED 5u /* a whole copy equal in metadata, length
                                    * and every byte retained so far, against
                                    * a record still arriving: the rest is
                                    * not retained to compare, and the copy
                                    * is ignored (§7.1 lets a relay ignore a
                                    * later copy)                            */
MOQR_CORE_API uint32_t moqr_core_internal_duplicate(moqr_core_t *core, moqr_track_t track,
                                           const moqr_log_append_desc_t *desc);

/* The ingest source (`conn`, `src`, as the caller names it) that opened the
 * OPEN record at (group, subgroup, object), and whether a source is another
 * one: its chunks are a copy's, never the record's (see moqr_log_set_opener,
 * moqr_log_open_by_other). */
MOQR_CORE_API moqr_result_t moqr_core_internal_set_opener(
    moqr_core_t *core, moqr_track_t track, uint64_t group_id, uint64_t subgroup_id,
    uint64_t object_id, uint32_t conn, uint64_t src);
MOQR_CORE_API bool moqr_core_internal_open_by_other(
    moqr_core_t *core, moqr_track_t track, uint64_t group_id, uint64_t subgroup_id,
    uint64_t object_id, uint32_t conn, uint64_t src);

/* A streamed copy's bytes against the retained payload (see
 * moqr_log_duplicate_range). */
#define MOQR_LOG_RANGE_EQUAL   0u
#define MOQR_LOG_RANGE_DIFFERS 1u  /* other bytes, or past the payload's end  */
#define MOQR_LOG_RANGE_UNKNOWN 2u  /* not retained (yet) to compare with      */
MOQR_CORE_API uint32_t moqr_core_internal_duplicate_range(
    moqr_core_t *core, moqr_track_t track, uint64_t group_id,
    uint64_t subgroup_id, uint64_t object_id, uint64_t offset,
    const uint8_t *data, size_t len);

/* The MAX_CACHE_DURATION Track Property (draft-22 §10.3, draft-18 §12.3,
 * draft-16 §11.1.1) of `track`'s source, in milliseconds, or none. "If
 * present, the relay MUST NOT start forwarding any individual Object received
 * through this subscription or fetch after the specified number of
 * milliseconds has elapsed since the beginning of the Object was received."
 * Delivery does not start an Object whose time has passed: a subscription
 * skips it (the subgroup's next record then starts a new downstream stream,
 * and a stream that lost its tail this way ends in a reset) and a FETCH
 * served from the log omits it. MOQR_ERR_STALE_HANDLE on a dead track. */
MOQR_CORE_API moqr_result_t moqr_core_internal_set_max_cache_duration(moqr_core_t *core,
                                                             moqr_track_t track,
                                                             bool has_duration,
                                                             uint64_t duration_ms);

#endif
