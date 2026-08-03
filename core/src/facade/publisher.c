#include <moq/publisher.h>
#include <moq/rcbuf.h>
#include <moq/wire.h>
#include "../internal/validate.h"
#include "../session/session_internal.h"   /* track-history registry seam */
#include <stddef.h>
#include <string.h>

typedef enum {
    PUB_NS_NONE = 0,
    PUB_NS_PENDING,
    PUB_NS_ACCEPTED,
    PUB_NS_TERMINAL,
} pub_ns_state_t;

#ifdef MOQ_PUB_TESTING
/* White-box admission counters, compiled ONLY into moq-core-test-internals
 * (never the shipping library). They let a test prove exactly-once admission
 * across a WOULD_BLOCK retry: a new op bumps commit + merge once; a
 * resume/retry bumps neither; each op releases its retained snapshot exactly
 * once. Not exported from any shipping artifact (verified with nm). */
unsigned long moq_pub_test_commit_count = 0;
unsigned long moq_pub_test_merge_count = 0;
unsigned long moq_pub_test_release_count = 0;
#define MOQ_PUB_TEST_BUMP(c) ((c)++)
#else
#define MOQ_PUB_TEST_BUMP(c) ((void)0)
#endif

/* Per-subscriber send state. Each track holds a heap-allocated array
 * of one slot. The array shape is internal prep; session core allows
 * at most one same-track subscription per session. */
/* One retained object: an element of a track's retained GROUP (objects 0..N of
 * the latest catalog group) and of the per-slot / FETCH replay snapshots. */
typedef struct {
    uint64_t     object_id;
    bool         end_of_group;
    moq_rcbuf_t *payload;       /* incref'd */
    moq_rcbuf_t *properties;    /* incref'd or NULL */
} pub_retained_obj_t;

/* A fan-out slot targets EITHER an accepted subscription or a publication we
 * initiated (kind); the subgroup send path is identical for both. */
typedef enum {
    PUB_SLOT_SUBSCRIPTION = 0,
    PUB_SLOT_PUBLICATION  = 1,
} pub_slot_kind_t;

typedef struct {
    bool                  active;
    pub_slot_kind_t       kind;
    moq_subscription_t    sub;   /* valid when kind == PUB_SLOT_SUBSCRIPTION */
    moq_publication_t     pub;   /* valid when kind == PUB_SLOT_PUBLICATION  */
    moq_subgroup_handle_t sg;
    bool                  sg_open;
    bool                  sg_has_extensions;
    bool                  sg_end_of_group;
    bool                  streaming;
    uint64_t              cur_group;
    /* Subscription Forward state (drafts 16/18: a publisher sends no objects
     * while Forward is 0; the session rejects such writes WRONG_STATE). Kept
     * per slot through immediate/pending/deferred acceptance and updated on
     * SUBSCRIBE_UPDATED. Publication slots use the track's publish_forward. */
    bool                  forward;
    /* Lazy retire: a Forward->0 transition with wire state open cannot be
     * handled inside event processing (reset can WOULD_BLOCK), so it is
     * flagged here and drained retryably at op start / tick. Retirement is
     * ALWAYS an explicit RESET_STREAM: omission via Forward 0 must never be
     * published as a cleanly finished subgroup (drafts 16/18). */
    bool                  needs_reset;
    /* Fan-out cursors. target_op: membership snapshot -- the slot is part of
     * the track's current operation iff target_op == track->cur_op, stamped
     * once when the operation STARTS (a slot installed or re-enabled mid
     * operation has a stale target_op and joins the NEXT operation).
     * done_op == track->cur_op marks this slot served (retries skip it).
     * track_clear_slot zeroes both; cur_op starts at 1, so a reused slot can
     * neither false-skip nor false-join. */
    uint64_t              target_op;
    uint64_t              done_op;
    /* Streams (subgroups) the facade opened for this destination -- every
     * open goes through the facade, so the count is EXACT and a terminal
     * done can report it instead of the unknown sentinel. Cleared with the
     * slot. */
    uint64_t              streams_opened;
    /* Resolved subscription-filter window, COPIED by value from the session's
     * internal accessor (moq_session_{sub,pub}_resolved_window) when the slot
     * is installed and whenever an update carries a filter -- never a retained
     * pointer into a mutable session entry. has_window == false (including
     * after the install-time accessor returned NULL) means unfiltered.
     * Membership is evaluated when an object operation is admitted and the
     * destination snapshot is stamped; a widening installed later never joins
     * an already pending operation or an open streaming bracket. */
    moq_resolved_window_t window;
} pub_sub_slot_t;

/* One in-flight fan-out operation per track. The op's payload/properties are
 * RETAINED here at op start and every destination write reads from this
 * snapshot -- never from the caller's (transient) pointers -- so partial
 * completion under WOULD_BLOCK can never put divergent bytes on the wire, and
 * callers that decref + re-encode a fresh identical buffer per retry (the
 * media-sender pattern) resume correctly. Released exactly once: on completion,
 * reset_group, remove_track, SESSION_CLOSED, or destroy. */
typedef enum {
    PUB_OP_NONE = 0,
    PUB_OP_WRITE_OBJECT,
    PUB_OP_BEGIN_OBJECT,
    PUB_OP_WRITE_DATA,
    PUB_OP_END_OBJECT,
    PUB_OP_END_GROUP,
    PUB_OP_END_TRACK,
} pub_op_kind_t;

typedef struct {
    pub_op_kind_t kind;         /* PUB_OP_NONE == no operation in flight */
    /* Wire-visible identity (WRITE_OBJECT / BEGIN_OBJECT). */
    uint64_t      group_id;
    uint64_t      object_id;
    bool          datagram;
    bool          has_status;
    moq_object_status_t status;
    bool          end_of_group;
    uint64_t      payload_length;   /* BEGIN_OBJECT declared length */
    /* Retained content snapshot (NULL when the op carries none). */
    moq_rcbuf_t  *payload;          /* WRITE_OBJECT payload / WRITE_DATA chunk */
    moq_rcbuf_t  *properties;
} pub_pending_op_t;

typedef struct pub_ns_entry {
    struct pub_ns_entry *next;
    uint8_t            *id_buf;
    size_t              id_len;
    moq_bytes_t        *id_parts;
    size_t              id_count;
    moq_announcement_t  handle;
    pub_ns_state_t      state;
    size_t              refcount;
} pub_ns_entry_t;

struct moq_pub_track {
    moq_pub_track_t    *next;
    moq_publisher_t    *pub;

    uint8_t            *ns_buf;
    size_t              ns_buf_len;
    moq_bytes_t        *ns_parts;
    size_t              ns_count;
    uint8_t            *name_buf;
    size_t              name_len;

    pub_ns_entry_t     *ns_entry;

    pub_sub_slot_t     *slots;
    size_t              slot_cap;

    /* Fan-out operation state (see pub_pending_op_t / slot target_op). cur_op
     * starts at 1 and increments when an operation STARTS; op.kind != NONE
     * while one is mid-retry. streaming_active is the TRACK-level begin/end
     * bracket (per-slot `streaming` only marks slots participating in it). */
    uint64_t            cur_op;
    pub_pending_op_t    op;
    bool                streaming_active;

    uint8_t             priority;

    bool                ended;       /* moq_pub_end_track succeeded; no more
                                        new objects accepted on this track */

    /* -- §10: explicit completion declaration (B) ---------------- *
     * declared_through is the app's standing promise that no object with
     * group <= it will ever be published on this track again. Monotone
     * (max-merge, never rolled back); enforcement rejects sealed writes
     * before any mutation; the completion sweep terminates every finite
     * window it covers with SUBSCRIPTION_ENDED (0x3). streaming_group is
     * the open bracket's group, valid only while streaming_active -- a
     * successful begin releases track->op (its identity is later
     * overwritten by chunk ops), so the bracket group must live here for
     * the declare preflight. */
    bool                has_declared;
    uint64_t            declared_through;
    uint64_t            streaming_group;

    /* -- §10: monotonic-groups evidence (A) ---------------------- *
     * monotonic mirrors cfg->monotonic_groups (immutable). begun_high is
     * the highest ADMITTED group (stamped exactly once at op commit,
     * status and zero-destination writes included): under the monotonic
     * promise, beginning group g proves every group below g complete.
     * eog_armed/eog_group track the last COMPLETED stream one-shot's
     * END_OF_GROUP flag; the candidate itself lives on the pending op and
     * is applied only when that op fully completes. has_eog/eog_high is
     * the promoted evidence: advanced by a clean end_group/end_track, by
     * a higher-group STREAM op completing (its group change FINs the old
     * subgroups), or by a status 0x3 admission (the object itself asserts
     * the nonexistence; not FIN-qualified) -- never by RESET. */
    bool                monotonic;
    bool                has_begun;
    uint64_t            begun_high;
    bool                eog_armed;
    uint64_t            eog_group;
    bool                has_eog;
    uint64_t            eog_high;
    bool                wrote_object; /* a live object was written: the retained
                                        object is no longer the track's largest,
                                        so it must not be advertised as Largest */

    /* Retained group (origin-local Joining-FETCH cache): objects 0..N of the
     * latest group, by ascending object_id. has_retained means count>0. */
    bool                has_retained;
    uint64_t            retained_group_id;
    pub_retained_obj_t *retained;
    size_t              retained_count;
    uint64_t            max_retained_bytes;

    /* Publisher-initiated PUBLISH state, independent of advertise/subscribe
     * above (both may be live on one track). The data slot opens on PUBLISH_OK. */
    bool                publish_requested;
    bool                publish_ok;
    bool                publish_forward;
    moq_publication_t   publication;

    /* Private reservation in the session's track-largest history registry,
     * obtained (capacity-checked) once at add_track and released at teardown.
     * Per-object admission merges the object's location into it with an
     * internal, non-allocating call -- the hot path never allocates and never
     * fails for history capacity. Always non-NULL on a live track: history is
     * mandatory, so add_track fails (MOQ_ERR_NOMEM) rather than linking a
     * track without a reservation. */
    moq_track_hist_t   *hist;
};

typedef struct {
    bool                active;
    moq_subscription_t  sub;
    moq_pub_track_t    *track;
    bool                accept;
    moq_request_error_t reject_code;
    bool                forward;   /* the request's Forward state, for the slot */
} pub_pending_t;

/* A single bounded retry slot for answering one FETCH from a track's retained
 * object (see serve_retained_fetch). The retained object is SNAPSHOTTED here at
 * stage time (refs held, location/priority copied) so the response is
 * independent of the track's mutable storage -- a later clear/end/remove cannot
 * strand or corrupt an accepted fetch. Phased so a full action queue defers
 * instead of leaving a half-served fetch: accepted/written make each retry
 * idempotent. reject_code is set only BEFORE accept (an accepted fetch must
 * finish, never REQUEST_ERROR). Mirrors pub_pending_t for subscribe. */
typedef struct {
    bool                active;
    moq_fetch_t         fetch;
    moq_request_error_t reject_code; /* non-zero: reject (pre-accept only) */
    bool                accepted;   /* accept_fetch (FETCH_OK) done */
    /* Snapshot of the retained GROUP (serve case; reject case leaves these 0).
     * Objects 0..count are written in order from next_idx; End Location is the
     * last object_id + 1. */
    uint64_t            group_id;
    uint64_t            end_object;  /* last object_id + 1 (FETCH_OK End) */
    uint8_t             priority;
    pub_retained_obj_t *objs;        /* incref'd snapshot; released on finish */
    size_t              obj_count;
    size_t              next_idx;    /* write cursor (resumes on WOULD_BLOCK) */
} pub_pending_fetch_t;

struct moq_pub_deferred {
    bool                active;
    uint64_t            generation;
    moq_subscription_t  sub;
    moq_pub_track_t    *track;
    bool                forward;   /* the request's Forward state, for the slot */
};

struct moq_publisher {
    moq_session_t      *session;
    moq_alloc_t         alloc;
    moq_pub_cfg_t       cfg;
    moq_pub_callbacks_t callbacks;
    bool                closed;
    bool                draining;
    moq_pub_track_t    *tracks;
    size_t              track_count;
    pub_ns_entry_t     *ns_entries;
    size_t              sub_slot_cap;
    pub_pending_t       pending;
    pub_pending_fetch_t pending_fetch;
    moq_pub_deferred_t  deferred;
};

/* -- Subscriber slot helpers (fan-out boundary) ------------------- */

static void track_clear_slot(moq_publisher_t *pub, pub_sub_slot_t *s)
{
    (void)pub;
    memset(s, 0, sizeof(*s));
}

static pub_sub_slot_t *track_find_slot_by_sub(moq_pub_track_t *t,
                                                moq_subscription_t sub)
{
    /* Kind check FIRST: `sub` and `pub` are separate fields, so a
     * publication slot carries an inactive/default `sub` value -- which
     * must never be compared against (it could accidentally match a live
     * subscription handle). */
    for (size_t i = 0; i < t->slot_cap; i++)
        if (t->slots[i].active &&
            t->slots[i].kind == PUB_SLOT_SUBSCRIPTION &&
            moq_subscription_eq(t->slots[i].sub, sub))
            return &t->slots[i];
    return NULL;
}

static pub_sub_slot_t *track_find_free_slot(moq_pub_track_t *t)
{
    for (size_t i = 0; i < t->slot_cap; i++)
        if (!t->slots[i].active)
            return &t->slots[i];
    return NULL;
}

static size_t track_active_count(const moq_pub_track_t *t)
{
    size_t n = 0;
    for (size_t i = 0; i < t->slot_cap; i++)
        if (t->slots[i].active && t->slots[i].kind == PUB_SLOT_SUBSCRIPTION)
            n++;
    return n;
}

static bool track_has_subscriber(const moq_pub_track_t *t)
{
    return track_active_count(t) > 0;
}

static void slot_install_sub_window(moq_publisher_t *pub, pub_sub_slot_t *sl);
static void slot_install_pub_window(moq_publisher_t *pub, pub_sub_slot_t *sl);
static moq_result_t track_run_completions(moq_publisher_t *pub,
                                          moq_pub_track_t *t,
                                          uint64_t now_us);

static void track_set_subscriber(moq_publisher_t *pub, moq_pub_track_t *t,
                                 moq_subscription_t sub, bool forward)
{
    pub_sub_slot_t *s = track_find_free_slot(t);
    if (!s) return;
    memset(s, 0, sizeof(*s));
    s->active = true;
    s->kind = PUB_SLOT_SUBSCRIPTION;
    s->sub = sub;
    s->forward = forward;
    /* Copy the accept-resolved window at installation : a slot can
     * never inherit an old window (memset above) nor miss its own. */
    slot_install_sub_window(pub, s);
}

static void track_set_publication(moq_publisher_t *fpub, moq_pub_track_t *t,
                                  moq_publication_t pub)
{
    pub_sub_slot_t *s = track_find_free_slot(t);
    if (!s) return;
    memset(s, 0, sizeof(*s));
    s->active = true;
    s->kind = PUB_SLOT_PUBLICATION;
    s->pub = pub;
    /* Copy the PUBLISH_OK-resolved window at installation. */
    slot_install_pub_window(fpub, s);
}

static pub_sub_slot_t *track_find_publication_slot(moq_pub_track_t *t)
{
    for (size_t i = 0; i < t->slot_cap; i++)
        if (t->slots[i].active && t->slots[i].kind == PUB_SLOT_PUBLICATION)
            return &t->slots[i];
    return NULL;
}

/* -- Fan-out engine ------------------------------------------------ *
 * Destination eligibility, membership snapshot, retained-snapshot pending
 * operation, and the lazy Forward-0 retire. See the pub_pending_op_t and
 * slot target_op/done_op comments for the model. */

/* A Forward 1 -> 0 transition with wire state open cannot retire inline
 * (reset can WOULD_BLOCK; event processing cannot retry), so it arms the
 * slot's lazy-retire flag -- the SAME rule for both slot kinds, and always
 * a RESET (see slot comment). */
static void slot_forward_drop(moq_pub_track_t *t, pub_sub_slot_t *sl)
{
    /* Drafts 16/18: omitting objects due to Forward 0 requires RESET_STREAM.
     * A FIN would assert every object of the subgroup was delivered -- which
     * nothing proves while the subgroup is open: even an END_OF_GROUP-
     * advertised subgroup (it only marks the group's LARGEST object) can
     * still be awaiting further objects (the media sender's one-subgroup
     * groups do exactly that). So ANY open subgroup resets. */
    if (sl->streaming || sl->sg_open) sl->needs_reset = true;
    /* A destination dropping mid-operation is excluded from that
     * operation PERMANENTLY -- returning to Forward 1 before the retry must
     * join the NEXT operation, never resurrect into this one. */
    if (t->op.kind != PUB_OP_NONE && sl->target_op == t->cur_op)
        sl->done_op = t->cur_op;
}

/* A slot may receive objects: drafts 16/18 forbid sending at Forward 0 (and
 * the session enforces it), so Forward-0 destinations are SKIPPED, never
 * errors -- a Forward-0 publication must not break a Forward-1 subscription
 * and vice versa. */
static bool slot_eligible(const moq_pub_track_t *t, const pub_sub_slot_t *sl)
{
    if (!sl->active || sl->needs_reset) return false;
    if (sl->kind == PUB_SLOT_PUBLICATION)
        return t->publish_ok && t->publish_forward;
    return sl->forward;
}

/* Centralized location membership against a resolved subscription-filter
 * window (drafts 16/18 §5.1.2): a Location passes iff it is >= the Start
 * Location (lexicographic, inclusive) and, when a finite end is present,
 * its Group ID is <= End Group (inclusive). No window means unfiltered; an
 * unsatisfiable window excludes everything. "A publisher MUST NOT send
 * objects from outside the requested range" -- this applies to EVERY object,
 * including status objects (the specs carve out no exception). */
static bool window_admits(const moq_resolved_window_t *w,
                          uint64_t group_id, uint64_t object_id)
{
    if (!w->has_window) return true;
    if (w->unsatisfiable) return false;
    if (group_id < w->start_group ||
        (group_id == w->start_group && object_id < w->start_object))
        return false;
    if (w->has_end && group_id > w->end_group) return false;
    return true;
}

/* The window admits SOME location in `group_id` (used for reset-on-cut: an
 * open subgroup in a group the new window excludes ENTIRELY can never carry
 * another passing object). A group equal to start_group still qualifies --
 * later object ids in it can pass. */
static bool window_admits_group(const moq_resolved_window_t *w,
                                uint64_t group_id)
{
    if (!w->has_window) return true;
    if (w->unsatisfiable) return false;
    if (group_id < w->start_group) return false;
    if (w->has_end && group_id > w->end_group) return false;
    return true;
}

/* Install (or clear) a slot's window from the session's resolved copy --
 * ALWAYS by value; the session entry is mutable and its pointer must not be
 * retained. A NULL accessor result (no filter negotiated / stale) means
 * unfiltered. */
static void slot_install_sub_window(moq_publisher_t *pub, pub_sub_slot_t *sl)
{
    const moq_resolved_window_t *w =
        moq_session_sub_resolved_window(pub->session, sl->sub);
    if (w) sl->window = *w;
    else memset(&sl->window, 0, sizeof(sl->window));
}

static void slot_install_pub_window(moq_publisher_t *pub, pub_sub_slot_t *sl)
{
    const moq_resolved_window_t *w =
        moq_session_pub_resolved_window(pub->session, sl->pub);
    if (w) sl->window = *w;
    else memset(&sl->window, 0, sizeof(sl->window));
}

/* Reset-on-cut + pending-op reconciliation after a filter update replaced
 * this slot's window.
 *
 * (1) An UNSENT pending location op the new window excludes must never go
 * out after the update: the drafts' narrowing allowance covers only objects
 * sent BEFORE the update was processed (d16 §9.11.1 / d18 §10.9.1), so a
 * WOULD_BLOCK'd one-shot/datagram/begin still awaiting this destination is
 * marked done for the current op -- a byte-identical retry sends it to the
 * OTHER snapshotted destinations only. A destination the object already
 * reached (done_op == cur_op, including a streaming bracket whose begin was
 * already written) is past sending and stays bracket-frozen.
 *
 * (2) A narrowing whose new window excludes the open bracket's ENTIRE group
 * can never deliver another object on that subgroup, so it takes the
 * existing Forward-drop path -- exclude from the in-flight operation, arm
 * exactly one lazy RESET (never FIN), keep the slot active; delivery
 * resumes on a later admitted object. A narrowing within the same group
 * (start_object ahead) keeps the subgroup: earlier ids were sent before the
 * update (allowed) and later ids are gated per-object at stamp time. */
static void slot_window_cut_check(moq_pub_track_t *t, pub_sub_slot_t *sl)
{
    if ((t->op.kind == PUB_OP_WRITE_OBJECT ||
         t->op.kind == PUB_OP_BEGIN_OBJECT) &&
        sl->target_op == t->cur_op && sl->done_op != t->cur_op &&
        !window_admits(&sl->window, t->op.group_id, t->op.object_id))
        sl->done_op = t->cur_op;
    if (!sl->streaming && !sl->sg_open) return;
    if (!window_admits_group(&sl->window, sl->cur_group))
        slot_forward_drop(t, sl);
}

/* Drain the lazy Forward-0 retire flags. Retryable: WOULD_BLOCK leaves the
 * flag set and the slot ineligible; a later call resumes. Retirement is
 * always an EXPLICIT RESET_STREAM (error code 0): Forward-0 omission must
 * never be published as a cleanly finished subgroup. */
static moq_result_t track_run_retires(moq_publisher_t *pub,
                                      moq_pub_track_t *t, uint64_t now_us)
{
    for (size_t i = 0; i < t->slot_cap; i++) {
        pub_sub_slot_t *sl = &t->slots[i];
        if (!sl->active || !sl->needs_reset) continue;
        moq_result_t rc = moq_session_reset_subgroup(pub->session, sl->sg, 0,
                                                     now_us);
        /* Mutate ONLY on success: a WOULD_BLOCK or error leaves the flags
         * set (slot stays ineligible) so a later pass retries -- a failed
         * retirement must never be published as done. A STALE subgroup
         * handle means the wire stream is already gone (peer STOP / prior
         * reset), NOT that the subscription/publication is: normalize the
         * subgroup-local flags and PRESERVE the destination -- only a stale
         * terminal-control handle may retire the slot. */
        if (rc < 0 && rc != MOQ_ERR_STALE_HANDLE) return rc;
        sl->sg_open = false;
        sl->streaming = false;
        sl->needs_reset = false;
    }
    return MOQ_OK;
}

/* §10: finite-window completion sweep for one track. Every active
 * destination whose finite filter end is covered by the declared watermark
 * terminates: FIN any clean open subgroup FIRST (streams close before the
 * terminal control message, d16:3436/d18:4227), then SUBSCRIBE_DONE /
 * PUBLISH_DONE with SUBSCRIPTION_ENDED (0x3, the finite end was genuinely
 * reached) and the EXACT per-destination stream count. Retry-idempotent
 * with the finish_subscribers discipline: state mutates only after a
 * successful/stale session call, a cleared slot goes inactive so retries
 * skip it, and the watermark driving eligibility is monotone. Deferrals:
 * a streaming slot waits for its in-flight object to land (reachable in
 * via mid-bracket higher-group datagrams; unreachable under B alone,
 * whose declare preflight refuses to cover an open bracket -- the guard is
 * normative either way), and a needs_reset slot waits for
 * track_run_retires (ordered before this sweep at every drive point: a
 * required RESET must never be converted into this sweep's clean FIN). */
/* §10: the track's completion watermark -- the highest group provably
 * complete. B: the explicit declaration. A (monotonic tracks): promoted
 * EOG/status evidence, and begun_high - 1 (beginning a group seals all
 * below it under the promise). Plain tracks have none. */
static bool track_complete_through(const moq_pub_track_t *t, uint64_t *out)
{
    bool has = false;
    uint64_t w = 0;
    if (t->has_declared) { w = t->declared_through; has = true; }
    if (t->monotonic) {
        if (t->has_eog && (!has || t->eog_high > w)) {
            w = t->eog_high; has = true;
        }
        if (t->has_begun && t->begun_high > 0 &&
            (!has || t->begun_high - 1 > w)) {
            w = t->begun_high - 1; has = true;
        }
    }
    if (has) *out = w;
    return has;
}

static moq_result_t track_run_completions(moq_publisher_t *pub,
                                          moq_pub_track_t *t,
                                          uint64_t now_us)
{
    uint64_t through;
    if (!track_complete_through(t, &through)) return MOQ_OK;
    for (size_t si = 0; si < t->slot_cap; si++) {
        pub_sub_slot_t *sl = &t->slots[si];
        if (!sl->active) continue;
        if (!sl->window.has_window || !sl->window.has_end) continue;
        if (sl->window.end_group > through) continue;
        if (sl->streaming) continue;      /* defer: in-flight object lands */
        if (sl->needs_reset) continue;    /* defer: retire sweep owns it */
        if (sl->sg_open) {
            moq_result_t rc = moq_session_close_subgroup(pub->session,
                                                         sl->sg, now_us);
            if (rc == MOQ_ERR_WOULD_BLOCK) return MOQ_ERR_WOULD_BLOCK;
            /* STALE = the wire stream is already gone, not the
             * subscription/publication: normalize sg_open and CONTINUE to
             * the terminal done below (a stale terminal handle -- not a
             * stale subgroup -- is what retires the slot). */
            if (rc < 0 && rc != MOQ_ERR_STALE_HANDLE) return rc;
            sl->sg_open = false;
        }
        if (sl->kind == PUB_SLOT_PUBLICATION) {
            moq_finish_publish_cfg_t fcfg;
            moq_finish_publish_cfg_init(&fcfg);
            fcfg.status_code = 0x3;   /* SUBSCRIPTION_ENDED */
            fcfg.stream_count = sl->streams_opened;
            moq_result_t rc = moq_session_finish_publish(pub->session,
                t->publication, &fcfg, now_us);
            if (rc == MOQ_ERR_WOULD_BLOCK) return MOQ_ERR_WOULD_BLOCK;
            if (rc != MOQ_OK && rc != MOQ_ERR_STALE_HANDLE) return rc;
            t->publish_requested = false;
            t->publish_ok = false;
            track_clear_slot(pub, sl);
            continue;
        }
        moq_done_subscribe_cfg_t dcfg;
        moq_done_subscribe_cfg_init(&dcfg);
        dcfg.status_code = 0x3;       /* SUBSCRIPTION_ENDED */
        dcfg.stream_count = sl->streams_opened;
        moq_result_t rc = moq_session_done_subscribe(pub->session, sl->sub,
                                                     &dcfg, now_us);
        if (rc == MOQ_ERR_WOULD_BLOCK) return MOQ_ERR_WOULD_BLOCK;
        if (rc != MOQ_OK && rc != MOQ_ERR_STALE_HANDLE) return rc;
        track_clear_slot(pub, sl);
    }
    return MOQ_OK;
}

/* Release the pending operation's retained buffers (exactly-once: idempotent
 * via NULL/kind). */
static void track_op_release(moq_pub_track_t *t)
{
    if (t->op.kind != PUB_OP_NONE) MOQ_PUB_TEST_BUMP(moq_pub_test_release_count);
    if (t->op.payload) { moq_rcbuf_decref(t->op.payload); t->op.payload = NULL; }
    if (t->op.properties) {
        moq_rcbuf_decref(t->op.properties);
        t->op.properties = NULL;
    }
    t->op.kind = PUB_OP_NONE;
}

/* Byte-exact buffer identity for retry validation: pointer fast-path, then
 * length + memcmp. Length-only matching would let a same-length CHANGED
 * buffer be silently ignored on retry. */
static bool op_buf_same(const moq_rcbuf_t *a, const moq_rcbuf_t *b)
{
    if (a == b) return true;
    if (!a || !b) return false;
    size_t la = moq_rcbuf_len(a), lb = moq_rcbuf_len(b);
    if (la != lb) return false;
    if (la == 0) return true;   /* distinct empty buffers: data may be NULL */
    return memcmp(moq_rcbuf_data(a), moq_rcbuf_data(b), la) == 0;
}

/*
 * Admission is split into a non-mutating peek and a mutating commit so that
 * per-object validation (and, in a later slice, completion-promise checks)
 * runs BEFORE any state is touched, and the exactly-once history merge runs
 * only on a genuinely new op. track_op_peek returns:
 *   MOQ_OK               a byte-identical op is already pending (retry/resume)
 *   1                    no op pending -- the caller should commit a new one
 *   MOQ_ERR_WRONG_STATE  a DIFFERENT op is pending (identity/kind mismatch):
 *                        complete the pending call first, or abandon via
 *                        moq_pub_reset_group.
 * It performs NO mutation: on MOQ_ERR_WRONG_STATE or before a matching commit
 * nothing is retained, cur_op is unchanged, and no slot is stamped.
 */
static moq_result_t track_op_peek(moq_pub_track_t *t, pub_op_kind_t kind,
                                  uint64_t group_id, uint64_t object_id,
                                  bool datagram, bool has_status,
                                  moq_object_status_t status,
                                  bool end_of_group, uint64_t payload_length,
                                  moq_rcbuf_t *payload,
                                  moq_rcbuf_t *properties)
{
    if (t->op.kind == PUB_OP_NONE) return 1;    /* new op */
    if (t->op.kind == kind &&
        t->op.group_id == group_id && t->op.object_id == object_id &&
        t->op.datagram == datagram && t->op.has_status == has_status &&
        t->op.status == status && t->op.end_of_group == end_of_group &&
        t->op.payload_length == payload_length &&
        op_buf_same(t->op.payload, payload) &&
        op_buf_same(t->op.properties, properties))
        return MOQ_OK;                          /* legitimate retry: resume */
    return MOQ_ERR_WRONG_STATE;                 /* divergent retry: refuse */
}

/* Commit a new fan-out op (only after track_op_peek returned 1 and all
 * validation passed): RETAIN payload/properties into the snapshot and stamp
 * every eligible slot target_op = ++cur_op. */
static void track_op_commit(moq_pub_track_t *t, pub_op_kind_t kind,
                            uint64_t group_id, uint64_t object_id,
                            bool datagram, bool has_status,
                            moq_object_status_t status,
                            bool end_of_group, uint64_t payload_length,
                            moq_rcbuf_t *payload,
                            moq_rcbuf_t *properties)
{
    t->op.kind = kind;
    t->op.group_id = group_id;
    t->op.object_id = object_id;
    t->op.datagram = datagram;
    t->op.has_status = has_status;
    t->op.status = status;
    t->op.end_of_group = end_of_group;
    t->op.payload_length = payload_length;
    t->op.payload = payload ? moq_rcbuf_incref(payload) : NULL;
    t->op.properties = properties ? moq_rcbuf_incref(properties) : NULL;
    t->cur_op++;
    /* Membership snapshot. Ops that carry a real object Location additionally
     * require the slot's resolved window to admit it -- evaluated HERE, once,
     * so the frozen snapshot is also the window decision (a widening
     * installed later cannot join this operation). Bracket-continuation and
     * close ops (WRITE_DATA / END_OBJECT / END_GROUP) carry no Location and
     * stamp as before: their membership was fixed by the bracket's begin (the
     * per-slot streaming flag) or is a close that any open subgroup needs. */
    bool locate = (kind == PUB_OP_WRITE_OBJECT || kind == PUB_OP_BEGIN_OBJECT);
    for (size_t i = 0; i < t->slot_cap; i++)
        if (slot_eligible(t, &t->slots[i]) &&
            (!locate || window_admits(&t->slots[i].window,
                                      group_id, object_id)))
            t->slots[i].target_op = t->cur_op;
    MOQ_PUB_TEST_BUMP(moq_pub_test_commit_count);
}

/* Combined peek+commit for ops that admit no new object location (end-track,
 * write-data, end-object, end-group): same return contract as the old
 * track_op_begin (MOQ_OK resume / 1 new / WRONG_STATE divergent). */
static moq_result_t track_op_begin(moq_pub_track_t *t, pub_op_kind_t kind,
                                   uint64_t group_id, uint64_t object_id,
                                   bool datagram, bool has_status,
                                   moq_object_status_t status,
                                   bool end_of_group, uint64_t payload_length,
                                   moq_rcbuf_t *payload,
                                   moq_rcbuf_t *properties)
{
    moq_result_t st = track_op_peek(t, kind, group_id, object_id, datagram,
        has_status, status, end_of_group, payload_length, payload, properties);
    if (st == 1)
        track_op_commit(t, kind, group_id, object_id, datagram, has_status,
            status, end_of_group, payload_length, payload, properties);
    return st;
}

/* Deterministic, destination-independent validation for a newly admitted
 * object: the location must be varint-encodable, and any object properties
 * must satisfy the profile's rule (draft-18 forbids a Mandatory Track Property
 * carried as an object property). Run BEFORE any mutation so that -- even with
 * zero eligible destinations -- an unencodable location or malformed
 * properties is rejected (MOQ_ERR_INVAL) without advancing Largest, stamping
 * slots, or emitting wire. Mirrors the session write path's checks. */
static moq_result_t validate_new_object(moq_pub_track_t *t,
                                        uint64_t group_id, uint64_t object_id,
                                        uint64_t payload_length,
                                        const moq_rcbuf_t *properties)
{
    /* NOTE (profile-ceiling reconciliation): the object-write data plane caps
     * locations at the QUIC-varint bound (2^62-1) regardless of the negotiated
     * profile -- a DELIBERATE, coherent limitation shared with the session write
     * path (session_subgroup.c) and the object-header encoders, which are not yet
     * vi64-wide on the write side. The largest-object NEGOTIATION path
     * (moq_session_note_object_published / accept / REQUEST_UPDATE) is
     * profile-specific (draft-18 vi64, full 2^64-1); a draft-18 producer that
     * needs to WRITE object IDs above 2^62-1 is not supported here.
     * Keeping this a single shared bound (rather than making one facade check
     * profile-specific in isolation) avoids a data plane that accepts locations
     * its own encoders cannot emit. */
    if (group_id > MOQ_QUIC_VARINT_MAX || object_id > MOQ_QUIC_VARINT_MAX)
        return MOQ_ERR_INVAL;
    /* Payload length is varint-encoded on the wire. A one-shot write derives
     * it from the payload rcbuf's logical length (which moq_rcbuf_wrap may set
     * larger than any real buffer); the session path rejects an over-limit
     * length only when it reaches an encoder, so a zero-destination/Forward-0
     * write must reject it here or it would poison Largest. */
    if (payload_length > MOQ_QUIC_VARINT_MAX) return MOQ_ERR_INVAL;
    if (properties) {
        size_t plen = moq_rcbuf_len(properties);
        moq_session_t *s = t->pub->session;
        if (plen > 0 && s->profile->validate_object_properties &&
            s->profile->validate_object_properties(
                s, moq_rcbuf_data(properties), plen) < 0)
            return MOQ_ERR_INVAL;
    }
    return MOQ_OK;
}

/* A slot participates in the current op's remaining work: snapshotted as a
 * target, not yet served, still active -- and still allowed to receive (a
 * Forward drop DURING the op removes the destination; the op completes
 * without it, per the drafts' send-nothing rule). */
static bool slot_op_pending(moq_pub_track_t *t, pub_sub_slot_t *sl)
{
    if (!sl->active || sl->target_op != t->cur_op) return false;
    if (sl->done_op == t->cur_op) return false;
    if (!slot_eligible(t, sl)) {
        /* Dropped out mid-op (Forward->0 / retire flagged): exclude. */
        sl->done_op = t->cur_op;
        return false;
    }
    return true;
}

/* Release a retained-object vector (decref each element's refs, free array). */
static void pub_retained_free(const moq_alloc_t *alloc,
                              pub_retained_obj_t *arr, size_t n)
{
    if (!arr) return;
    for (size_t i = 0; i < n; i++) {
        if (arr[i].payload) moq_rcbuf_decref(arr[i].payload);
        if (arr[i].properties) moq_rcbuf_decref(arr[i].properties);
    }
    alloc->free(arr, n * sizeof(*arr), alloc->ctx);
}

/* Deep-snapshot a retained vector (incref each element's refs). Returns NULL on
 * NOMEM or n==0 (caller treats NULL as "nothing to replay/serve"). */
static pub_retained_obj_t *pub_retained_snapshot(const moq_alloc_t *alloc,
                                                 const pub_retained_obj_t *src,
                                                 size_t n)
{
    if (n == 0) return NULL;
    pub_retained_obj_t *out =
        (pub_retained_obj_t *)alloc->alloc(n * sizeof(*out), alloc->ctx);
    if (!out) return NULL;
    for (size_t i = 0; i < n; i++) {
        out[i] = src[i];
        if (out[i].payload) moq_rcbuf_incref(out[i].payload);
        if (out[i].properties) moq_rcbuf_incref(out[i].properties);
    }
    return out;
}

static void track_release_retained(moq_pub_track_t *t)
{
    if (t->has_retained) {
        pub_retained_free(&t->pub->alloc, t->retained, t->retained_count);
        t->retained = NULL;
        t->retained_count = 0;
        t->has_retained = false;
    }
}

/* -- Retained group (origin-local Joining-FETCH cache) -------------- *
 * The origin keeps the latest catalog GROUP per track -- objects 0..N a late
 * subscriber needs (independent object 0 + deltaUpdate objects 1..N). It is the
 * cache an EXPLICIT Joining FETCH is answered from (serve_retained_fetch); it is
 * NOT pushed to plain SUBSCRIBE joiners. objects[0] is the independent base; the
 * last object bounds the served range / advertised Largest. */
static bool pub_track_has_retained(const moq_pub_track_t *t)
{ return t->has_retained && t->retained_count > 0; }
static uint64_t pub_track_retained_group(const moq_pub_track_t *t)
{ return t->retained_group_id; }
/* The last retained object_id -- the Largest the FETCH End Location is built on
 * (last + 1) and the joining range is bounded by. */
static uint64_t pub_track_retained_last_object_id(const moq_pub_track_t *t)
{ return t->retained_count ? t->retained[t->retained_count - 1].object_id : 0; }
/* The first (independent) retained object_id -- objects[0], which a Joining
 * FETCH range MUST include (catalog correctness: object 0 + deltas). */
static uint64_t pub_track_retained_first_object_id(const moq_pub_track_t *t)
{ return t->retained_count ? t->retained[0].object_id : 0; }

/* Is location (g,o) inside the FETCH request range [start, end)? End is
 * exclusive; end_object == 0 means the whole end group is included (§9.16). */
static bool loc_in_fetch_range(uint64_t g, uint64_t o,
                               uint64_t sg, uint64_t so,
                               uint64_t eg, uint64_t eo)
{
    if (g < sg || (g == sg && o < so)) return false;   /* before start */
    if (g > eg) return false;                           /* past end group */
    if (g == eg && eo != 0 && o >= eo) return false;    /* at/after exclusive end */
    return true;
}

/* Whether the retained object may be advertised as the subscription's Largest
 * Location. It must be genuinely the track's largest (a retained object, no live
 * writes -- §5.1 defines Largest as the largest actually published; the facade
 * does not track a live largest, so once a live object is written it advertises
 * none rather than a stale one), AND its Location must be encodable INCLUDING
 * the {group, object + 1} End Location a FETCH_OK would carry. Advertising a
 * Largest we cannot answer would let a subscriber issue a Joining FETCH we
 * cannot serve, so the advertise check and the serve guard agree. */
static bool retained_can_advertise_largest(const moq_pub_track_t *t)
{
    return t && pub_track_has_retained(t) && !t->wrote_object &&
           pub_track_retained_group(t) <= MOQ_QUIC_VARINT_MAX &&
           pub_track_retained_last_object_id(t) < MOQ_QUIC_VARINT_MAX;
}

/* Initialize a subscribe-accept cfg, advertising the retained object as the
 * subscription's Largest Location only when retained_can_advertise_largest().
 * That is what lets a subscriber issue a Joining FETCH for the retained catalog
 * object (§9.16.2 requires a known Largest). */
static void pub_init_accept_cfg(const moq_pub_track_t *t,
                                moq_accept_subscribe_cfg_t *acc)
{
    moq_accept_subscribe_cfg_init(acc);
    if (retained_can_advertise_largest(t)) {
        acc->has_largest = true;
        acc->largest_group = pub_track_retained_group(t);
        acc->largest_object = pub_track_retained_last_object_id(t);
    }
}

/* Finish the pending fetch: release the snapshot refs and clear the slot. */
static void pending_fetch_clear(moq_publisher_t *pub)
{
    pub_pending_fetch_t *pf = &pub->pending_fetch;
    pub_retained_free(&pub->alloc, pf->objs, pf->obj_count);
    memset(pf, 0, sizeof(*pf));
}

/* Answer one spec FETCH from the snapshotted retained GROUP: FETCH_OK (End
 * Location = last object + 1), then each object 0..N as a FETCH_OBJECT in order,
 * then FIN. Driven by pub->pending_fetch. Phased and capacity-gated so a full
 * action queue defers to a later tick rather than leaving a half-served fetch;
 * the accepted flag + next_idx cursor make every retry idempotent (no duplicate
 * or skipped object). A reject is only ever issued BEFORE accept. The objects
 * are served from the snapshot, so a clear/end/remove of the source track after
 * staging does not affect the response. */
static void serve_retained_fetch(moq_publisher_t *pub, uint64_t now_us)
{
    pub_pending_fetch_t *pf = &pub->pending_fetch;
    if (!pf->active) return;

    if (!pf->accepted && pf->reject_code != 0) {
        moq_reject_fetch_cfg_t rej;
        moq_reject_fetch_cfg_init(&rej);
        rej.error_code = pf->reject_code;
        moq_result_t rc = moq_session_reject_fetch(pub->session, pf->fetch,
                                                   &rej, now_us);
        if (rc == MOQ_ERR_WOULD_BLOCK) return;   /* retry the reject */
        pending_fetch_clear(pub);
        return;
    }

    if (!pf->accepted) {
        if (moq_session_action_capacity(pub->session) < 2) return;  /* defer */
        moq_accept_fetch_cfg_t acc;
        moq_accept_fetch_cfg_init(&acc);
        acc.end_of_track = false;
        acc.end_group = pf->group_id;
        acc.end_object = pf->end_object;   /* End Location: last object + 1 */
        moq_result_t rc = moq_session_accept_fetch(pub->session, pf->fetch,
                                                   &acc, now_us);
        if (rc == MOQ_ERR_WOULD_BLOCK) return;   /* defer; not accepted */
        if (rc < 0) { pending_fetch_clear(pub); return; }
        pf->accepted = true;
    }

    /* Accepted: write each remaining object from the cursor, then FIN. Each
     * write costs 2 actions with properties, 1 without; reserve per object so a
     * deferred write resumes at the same cursor (no duplicate/skip). */
    while (pf->next_idx < pf->obj_count) {
        const pub_retained_obj_t *o = &pf->objs[pf->next_idx];
        size_t need = (o->properties ? 2u : 1u);
        if (moq_session_action_capacity(pub->session) < need) return;  /* defer */
        moq_fetch_object_cfg_t oc;
        moq_fetch_object_cfg_init(&oc);
        oc.group_id = pf->group_id;
        oc.subgroup_id = 0;
        oc.object_id = o->object_id;
        oc.publisher_priority = pf->priority;
        oc.payload = o->payload;
        oc.properties = o->properties;
        moq_result_t rc = moq_session_write_fetch_object(pub->session, pf->fetch,
                                                         &oc, now_us);
        if (rc == MOQ_ERR_WOULD_BLOCK) return;   /* resume at cursor */
        if (rc < 0) {
            (void)moq_session_end_fetch(pub->session, pf->fetch, now_us);
            pending_fetch_clear(pub);
            return;
        }
        pf->next_idx++;
    }
    if (moq_session_action_capacity(pub->session) < 1) return;   /* defer the FIN */
    moq_result_t rc = moq_session_end_fetch(pub->session, pf->fetch, now_us);
    if (rc == MOQ_ERR_WOULD_BLOCK) return;
    pending_fetch_clear(pub);
}

static void track_clear_subscriber(moq_pub_track_t *t)
{
    for (size_t i = 0; i < t->slot_cap; i++)
        track_clear_slot(t->pub, &t->slots[i]);
}

/* True when the caller's callbacks struct is large enough to include the field
 * at byte offset `off` of width `sz`. */
static bool cb_has_field(const moq_pub_callbacks_t *cb, size_t off, size_t sz)
{
    return cb->struct_size >= off + sz;
}

#define CB_HAS(cb, field) \
    (cb_has_field((cb), offsetof(moq_pub_callbacks_t, field), \
                  sizeof((cb)->field)) && (cb)->field)

/* -- Helpers ------------------------------------------------------ */

static void *pub_alloc(moq_publisher_t *p, size_t sz) {
    return p->alloc.alloc(sz, p->alloc.ctx);
}

static void pub_free(moq_publisher_t *p, void *ptr, size_t sz) {
    p->alloc.free(ptr, sz, p->alloc.ctx);
}

static uint8_t *dup_bytes(moq_publisher_t *p, const uint8_t *data,
                           size_t len)
{
    if (!len) return NULL;
    uint8_t *buf = (uint8_t *)pub_alloc(p, len);
    if (buf) memcpy(buf, data, len);
    return buf;
}

/* -- Namespace refcounting helpers --------------------------------- */

/* Find a *reusable* advertisement for `ns`: a PENDING or ACCEPTED entry whose
 * advertisement a new track can share. A terminal (rejected/cancelled) entry may
 * still linger while an older track holds its refcount; it is NOT reusable -- a
 * new advertised track for the same namespace must emit a fresh
 * PUBLISH_NAMESPACE, so terminal entries are skipped here. */
static pub_ns_entry_t *find_ns_entry(moq_publisher_t *pub,
                                      const moq_namespace_t *ns)
{
    for (pub_ns_entry_t *e = pub->ns_entries; e; e = e->next) {
        if (e->state == PUB_NS_TERMINAL) continue;
        if (e->id_count != ns->count) continue;
        bool match = true;
        for (size_t i = 0; i < ns->count; i++) {
            if (e->id_parts[i].len != ns->parts[i].len ||
                memcmp(e->id_parts[i].data, ns->parts[i].data,
                       ns->parts[i].len) != 0) {
                match = false;
                break;
            }
        }
        if (match) return e;
    }
    return NULL;
}

static pub_ns_entry_t *create_ns_entry(moq_publisher_t *pub,
                                        const moq_namespace_t *ns)
{
    pub_ns_entry_t *e = (pub_ns_entry_t *)pub_alloc(pub, sizeof(*e));
    if (!e) return NULL;
    memset(e, 0, sizeof(*e));

    size_t total = 0;
    for (size_t i = 0; i < ns->count; i++)
        total += ns->parts[i].len;

    e->id_parts = (moq_bytes_t *)pub_alloc(pub,
        ns->count * sizeof(moq_bytes_t));
    if (!e->id_parts) {
        pub_free(pub, e, sizeof(*e));
        return NULL;
    }
    if (total > 0) {
        e->id_buf = (uint8_t *)pub_alloc(pub, total);
        if (!e->id_buf) {
            pub_free(pub, e->id_parts, ns->count * sizeof(moq_bytes_t));
            pub_free(pub, e, sizeof(*e));
            return NULL;
        }
    }
    e->id_len = total;
    e->id_count = ns->count;
    size_t off = 0;
    for (size_t i = 0; i < ns->count; i++) {
        e->id_parts[i].data = e->id_buf + off;
        e->id_parts[i].len = ns->parts[i].len;
        if (ns->parts[i].len > 0)
            memcpy(e->id_buf + off, ns->parts[i].data, ns->parts[i].len);
        off += ns->parts[i].len;
    }

    e->next = pub->ns_entries;
    pub->ns_entries = e;
    return e;
}

static void free_ns_entry(moq_publisher_t *pub, pub_ns_entry_t *e)
{
    pub_ns_entry_t **pp = &pub->ns_entries;
    while (*pp) {
        if (*pp == e) { *pp = e->next; break; }
        pp = &(*pp)->next;
    }
    if (e->id_buf)   pub_free(pub, e->id_buf, e->id_len);
    if (e->id_parts) pub_free(pub, e->id_parts,
        e->id_count * sizeof(moq_bytes_t));
    pub_free(pub, e, sizeof(*e));
}

/* -- Track helpers ------------------------------------------------ */

static bool ns_matches(const moq_pub_track_t *t, const moq_namespace_t *ns) {
    if (t->ns_count != ns->count) return false;
    for (size_t i = 0; i < ns->count; i++) {
        if (t->ns_parts[i].len != ns->parts[i].len) return false;
        if (t->ns_parts[i].len > 0 &&
            memcmp(t->ns_parts[i].data, ns->parts[i].data,
                   ns->parts[i].len) != 0)
            return false;
    }
    return true;
}

static bool name_matches(const moq_pub_track_t *t, moq_bytes_t name) {
    if (t->name_len != name.len) return false;
    if (name.len == 0) return true;
    return memcmp(t->name_buf, name.data, name.len) == 0;
}

static moq_pub_track_t *find_track(moq_publisher_t *pub,
                                     const moq_namespace_t *ns,
                                     moq_bytes_t name)
{
    for (moq_pub_track_t *t = pub->tracks; t; t = t->next)
        if (ns_matches(t, ns) && name_matches(t, name))
            return t;
    return NULL;
}

static moq_pub_track_t *find_track_by_ann(moq_publisher_t *pub,
                                            moq_announcement_t ann)
{
    for (moq_pub_track_t *t = pub->tracks; t; t = t->next)
        if (t->ns_entry && t->ns_entry->state != PUB_NS_NONE &&
            moq_announcement_eq(t->ns_entry->handle, ann))
            return t;
    return NULL;
}

/* The advertised-namespace entry for an announcement handle (shared across
 * every track that advertised it). Namespace-terminal events resolve here so
 * the transition is per-NAMESPACE, not per-track. */
static pub_ns_entry_t *find_ns_entry_by_ann(moq_publisher_t *pub,
                                            moq_announcement_t ann)
{
    for (pub_ns_entry_t *e = pub->ns_entries; e; e = e->next)
        if (e->state != PUB_NS_NONE &&
            moq_announcement_eq(e->handle, ann))
            return e;
    return NULL;
}

static moq_pub_track_t *find_track_by_sub(moq_publisher_t *pub,
                                           moq_subscription_t sub)
{
    for (moq_pub_track_t *t = pub->tracks; t; t = t->next)
        if (track_find_slot_by_sub(t, sub))
            return t;
    return NULL;
}

static moq_pub_track_t *find_track_by_pub(moq_publisher_t *pub,
                                           moq_publication_t pub_handle)
{
    for (moq_pub_track_t *t = pub->tracks; t; t = t->next)
        if (t->publish_requested &&
            moq_publication_eq(t->publication, pub_handle))
            return t;
    return NULL;
}


static void free_track_state(moq_publisher_t *pub, moq_pub_track_t *t) {
    track_op_release(t);   /* exactly-once: idempotent via NULL/kind */
    track_release_retained(t);
    /* Drop the track's history reservation: an empty (unpublished) record is
     * reclaimed here; a record that observed a largest stays pinned in the
     * session registry until the session is destroyed. */
    if (t->hist) { track_hist_release(pub->session, t->hist); t->hist = NULL; }
    if (t->slots)
        pub_free(pub, t->slots, t->slot_cap * sizeof(pub_sub_slot_t));
    if (t->ns_buf)   pub_free(pub, t->ns_buf, t->ns_buf_len);
    if (t->ns_parts) pub_free(pub, t->ns_parts, t->ns_count * sizeof(moq_bytes_t));
    if (t->name_buf) pub_free(pub, t->name_buf, t->name_len);
    pub_free(pub, t, sizeof(moq_pub_track_t));
}

static void unlink_track(moq_publisher_t *pub, moq_pub_track_t *t) {
    moq_pub_track_t **pp = &pub->tracks;
    while (*pp) {
        if (*pp == t) { *pp = t->next; pub->track_count--; return; }
        pp = &(*pp)->next;
    }
}

static moq_result_t flush_pending(moq_publisher_t *pub, uint64_t now_us,
                                   moq_pub_track_t **out_joined)
{
    if (out_joined) *out_joined = NULL;
    pub_pending_t *p = &pub->pending;
    if (!p->active) return MOQ_OK;

    /* A track ended while its accept was pending: reject the late subscriber
     * instead of completing the accept onto a terminal track. */
    if (p->accept && p->track && p->track->ended) {
        p->accept = false;
        p->reject_code = MOQ_REQUEST_ERROR_DOES_NOT_EXIST;
    }

    moq_result_t rc;
    if (p->accept) {
        moq_accept_subscribe_cfg_t acc;
        pub_init_accept_cfg(p->track, &acc);
        rc = moq_session_accept_subscribe(pub->session, p->sub, &acc, now_us);
    } else {
        moq_reject_subscribe_cfg_t rej;
        moq_reject_subscribe_cfg_init(&rej);
        rej.error_code = p->reject_code;
        rc = moq_session_reject_subscribe(pub->session, p->sub, &rej, now_us);
    }

    if (rc == MOQ_ERR_WOULD_BLOCK) return MOQ_ERR_WOULD_BLOCK;

    if (rc == MOQ_OK && p->accept && p->track) {
        track_set_subscriber(pub, p->track, p->sub, p->forward);
        if (out_joined) *out_joined = p->track;
    }

    p->active = false;
    return rc < 0 ? rc : MOQ_OK;
}

/*
 * Canonical local close: observe a session close ONCE. Tears down every piece
 * of local state so nothing is stranded until destroy -- pending accept/reject
 * and deferred slots, the retained-FETCH snapshot (refs released here, not at
 * destroy), per-track fan-out snapshots, and subscriber slots -- and fires
 * on_closed exactly once. Idempotent (guarded on pub->closed). Shared by the
 * event dispatcher and drain_closed_event so there is one close state machine.
 */
static void pub_close_local(moq_publisher_t *pub, uint64_t code)
{
    if (pub->closed) return;
    pub->closed = true;
    pub->deferred.active = false;
    pub->pending.active = false;
    pending_fetch_clear(pub);   /* release retained-FETCH snapshot refs now */
    for (moq_pub_track_t *t = pub->tracks; t; t = t->next) {
        track_clear_subscriber(t);
        track_op_release(t);    /* wrapped-buffer release must not wait for
                                   destroy */
    }
    if (pub->callbacks.on_closed)
        pub->callbacks.on_closed(pub->callbacks.ctx, code);
}

/*
 * Progress ALL staged, action-queue-limited work: a pending subscribe accept/
 * reject (firing on_subscriber_joined exactly once on completion), a deferred
 * retained FETCH, and lazy Forward-0 subgroup retirements. Idempotent under
 * retry -- flush_pending guards on pending.active, serve_retained_fetch and
 * track_run_retires mutate only on success and re-arm on WOULD_BLOCK -- so a
 * repeated call never doubles a callback or a wire action. Shared by
 * moq_pub_flush (manual mode) and moq_pub_tick.
 */
static moq_result_t pub_progress_staged(moq_publisher_t *pub, uint64_t now_us)
{
    moq_pub_track_t *joined = NULL;
    moq_result_t rc = flush_pending(pub, now_us, &joined);
    if (joined && pub->callbacks.on_subscriber_joined)
        pub->callbacks.on_subscriber_joined(pub->callbacks.ctx, joined);
    if (rc < 0) return rc;   /* WOULD_BLOCK or error: retry drains + reflushes */

    if (pub->pending_fetch.active) {
        serve_retained_fetch(pub, now_us);
        if (pub->pending_fetch.active) return MOQ_ERR_WOULD_BLOCK;
    }

    for (moq_pub_track_t *t = pub->tracks; t; t = t->next) {
        moq_result_t rrc = track_run_retires(pub, t, now_us);
        if (rrc == MOQ_ERR_WOULD_BLOCK) return MOQ_ERR_WOULD_BLOCK;
        if (rrc < 0 && rrc != MOQ_ERR_STALE_HANDLE) return rrc;
    }

    /* §10: finite-window completion, strictly AFTER the retire sweep
     * (a required RESET is never converted into completion's clean FIN). */
    for (moq_pub_track_t *t = pub->tracks; t; t = t->next) {
        moq_result_t crc = track_run_completions(pub, t, now_us);
        if (crc == MOQ_ERR_WOULD_BLOCK) return MOQ_ERR_WOULD_BLOCK;
        if (crc < 0 && crc != MOQ_ERR_STALE_HANDLE) return crc;
    }
    return MOQ_OK;
}

/* -- Config field access ------------------------------------------- */

static bool pub_cfg_has_field(const moq_pub_cfg_t *cfg,
                               size_t offset, size_t size)
{
    return cfg->struct_size >= offset && size <= cfg->struct_size - offset;
}

/* -- Public API --------------------------------------------------- */

/* Frozen v0 callbacks prefix: the layout through on_subscriber_updated --
 * everything before the appended on_publish_* fields. A caller compiled
 * before those fields hands the pointer-only initializer exactly this much
 * storage, so it must never clear past it. */
#define MOQ_PUB_CALLBACKS_V0_SIZE \
    offsetof(moq_pub_callbacks_t, on_publish_ok)

void moq_pub_callbacks_init(moq_pub_callbacks_t *cb) {
    if (!cb) return;
    /* Clear and stamp ONLY the frozen v0 prefix: writing sizeof(*cb) would
     * overflow an old caller's smaller storage. The appended on_publish_*
     * fields stay disabled (struct_size == prefix); callers that set them
     * use moq_pub_callbacks_init_sized(). */
    memset(cb, 0, MOQ_PUB_CALLBACKS_V0_SIZE);
    cb->struct_size = (uint32_t)MOQ_PUB_CALLBACKS_V0_SIZE;
}

void moq_pub_callbacks_init_sized(moq_pub_callbacks_t *cb, size_t cb_size) {
    if (!cb) return;
    /* Clear exactly what the caller allocated, never more than this library's
     * struct knows about (same contract as moq_pub_cfg_init_sized). */
    size_t n = cb_size < sizeof(*cb) ? cb_size : sizeof(*cb);
    if (n < sizeof(cb->struct_size)) return;  /* too small to even stamp */
    memset(cb, 0, n);
    cb->struct_size = (uint32_t)n;
}

/* Frozen v0 prefix: the original layout (struct_size, accept_mode,
 * default_publisher_priority) -- everything before the appended on_subscribe /
 * on_subscribe_ctx / callbacks fields. NOTE: the original struct had no pointer
 * members (4-byte alignment), so the old caller's sizeof is SMALLER than
 * offsetof(on_subscribe) in the current (8-aligned) struct. The safe frozen
 * prefix is therefore the documented create() minimum (through
 * default_publisher_priority), not offsetof of the first appended field. The
 * pointer-only initializer cannot know the caller's storage size, so it touches
 * only this prefix. */
#define MOQ_PUB_CFG_V0_SIZE \
    (offsetof(moq_pub_cfg_t, default_publisher_priority) + \
     sizeof(((moq_pub_cfg_t *)0)->default_publisher_priority))

void moq_pub_cfg_init(moq_pub_cfg_t *cfg) {
    if (!cfg) return;
    /* Clear and stamp ONLY the frozen prefix: writing sizeof(*cfg) here would
     * overflow a caller compiled against the original (smaller) struct. Appended
     * fields (on_subscribe, on_subscribe_ctx, callbacks) stay disabled
     * (struct_size == prefix); callers that want them use
     * moq_pub_cfg_init_sized(). */
    memset(cfg, 0, MOQ_PUB_CFG_V0_SIZE);
    cfg->struct_size = (uint32_t)MOQ_PUB_CFG_V0_SIZE;
    cfg->accept_mode = MOQ_PUB_REJECT_ALL;
    cfg->default_publisher_priority = 128;
}

void moq_pub_cfg_init_sized(moq_pub_cfg_t *cfg, size_t cfg_size) {
    if (!cfg) return;
    /* Clear exactly what the caller allocated, never more than this library's
     * struct knows about. An older caller passes a smaller cfg_size (we clear/
     * stamp that prefix); a newer caller's extra trailing fields are left to its
     * own initializer. Set each default only when it lies within cfg_size. */
    size_t n = cfg_size < sizeof(*cfg) ? cfg_size : sizeof(*cfg);
    if (n < sizeof(cfg->struct_size)) return;  /* too small to even stamp */
    memset(cfg, 0, n);
    cfg->struct_size = (uint32_t)n;
    if (n >= offsetof(moq_pub_cfg_t, accept_mode) + sizeof(cfg->accept_mode))
        cfg->accept_mode = MOQ_PUB_REJECT_ALL;
    if (n >= offsetof(moq_pub_cfg_t, default_publisher_priority) +
             sizeof(cfg->default_publisher_priority))
        cfg->default_publisher_priority = 128;
    /* Initialize as much of the nested callbacks struct as the caller's
     * storage covers, so a full-size caller can set the appended
     * on_publish_* fields directly after this call. */
    if (n > offsetof(moq_pub_cfg_t, callbacks))
        moq_pub_callbacks_init_sized(&cfg->callbacks,
                                     n - offsetof(moq_pub_cfg_t, callbacks));
}

#define PUB_CFG_MIN_SIZE MOQ_PUB_CFG_V0_SIZE

/* Largest prefix of moq_pub_callbacks_t (in bytes) that contains only WHOLE
 * fields and fits in `avail`. moq_pub_create copies exactly this many bytes so a
 * mis-sized cfg->struct_size can never install a partially-copied (truncated,
 * non-NULL) callback/ctx pointer into the zeroed publisher -- the existing
 * `if (ptr)` dispatch sites then stay safe without per-field struct_size gates. */
static size_t pub_callbacks_copy_size(size_t avail)
{
    static const size_t field_end[] = {
        offsetof(moq_pub_callbacks_t, ctx),                   /* struct_size */
        offsetof(moq_pub_callbacks_t, on_subscriber_joined),  /* ctx */
        offsetof(moq_pub_callbacks_t, on_subscriber_left),    /* joined */
        offsetof(moq_pub_callbacks_t, on_draining),           /* left */
        offsetof(moq_pub_callbacks_t, on_closed),             /* draining */
        offsetof(moq_pub_callbacks_t, on_subscriber_updated), /* closed */
        offsetof(moq_pub_callbacks_t, on_publish_ok),         /* updated */
        offsetof(moq_pub_callbacks_t, on_publish_error),      /* publish_ok */
        offsetof(moq_pub_callbacks_t, on_publish_forward_changed), /* error */
        offsetof(moq_pub_callbacks_t, on_publish_finished),   /* fwd_changed */
        offsetof(moq_pub_callbacks_t, on_namespace_terminal), /* finished */
        sizeof(moq_pub_callbacks_t),                          /* ns_terminal */
    };
    size_t copy = 0;
    for (size_t i = 0; i < sizeof(field_end) / sizeof(field_end[0]); i++)
        if (field_end[i] <= avail) copy = field_end[i];
    return copy;
}

moq_result_t moq_pub_create(moq_session_t *session,
                              const moq_alloc_t *alloc,
                              const moq_pub_cfg_t *cfg,
                              moq_publisher_t **out)
{
    if (!session || !alloc || !cfg || !out) return MOQ_ERR_INVAL;
    *out = NULL;

    if (cfg->struct_size < PUB_CFG_MIN_SIZE)
        return MOQ_ERR_INVAL;

    moq_pub_subscribe_cb on_subscribe = NULL;
    void *on_subscribe_ctx = NULL;
    if (pub_cfg_has_field(cfg, offsetof(moq_pub_cfg_t, on_subscribe),
                          sizeof(cfg->on_subscribe)))
        on_subscribe = cfg->on_subscribe;
    if (pub_cfg_has_field(cfg, offsetof(moq_pub_cfg_t, on_subscribe_ctx),
                          sizeof(cfg->on_subscribe_ctx)))
        on_subscribe_ctx = cfg->on_subscribe_ctx;

    if (cfg->accept_mode == MOQ_PUB_CALLBACK) {
        if (!on_subscribe) return MOQ_ERR_INVAL;
    } else if (cfg->accept_mode != MOQ_PUB_REJECT_ALL &&
               cfg->accept_mode != MOQ_PUB_ACCEPT_ALL) {
        return MOQ_ERR_INVAL;
    }

    moq_publisher_t *p = (moq_publisher_t *)alloc->alloc(
        sizeof(moq_publisher_t), alloc->ctx);
    if (!p) return MOQ_ERR_NOMEM;
    memset(p, 0, sizeof(*p));
    p->session = session;
    p->alloc = *alloc;
    p->cfg.struct_size = sizeof(moq_pub_cfg_t);
    p->cfg.accept_mode = cfg->accept_mode;
    p->cfg.default_publisher_priority = cfg->default_publisher_priority;
    p->cfg.on_subscribe = on_subscribe;
    p->cfg.on_subscribe_ctx = on_subscribe_ctx;
    {
        size_t cb_off = offsetof(moq_pub_cfg_t, callbacks);
        if (cfg->struct_size > cb_off) {
            /* Effective prefix = min(outer bytes available, the caller's
             * nested callbacks.struct_size, this library's struct). The
             * nested size is readable only when the outer prefix fully
             * covers that field; without the nested bound, a full outer
             * cfg would install callback bytes past the caller's DECLARED
             * nested prefix -- and several v0 callbacks dispatch without a
             * size gate, so an installed stale pointer would be called. */
            size_t avail = cfg->struct_size - cb_off;
            if (avail > sizeof(p->callbacks))
                avail = sizeof(p->callbacks);
            if (avail >= sizeof(cfg->callbacks.struct_size) &&
                cfg->callbacks.struct_size < avail)
                avail = cfg->callbacks.struct_size;
            /* Copy only whole fields -- never a partial (truncated) pointer. */
            memcpy(&p->callbacks, &cfg->callbacks,
                   pub_callbacks_copy_size(avail));
        }
    }

    p->sub_slot_cap = 1;

    *out = p;
    return MOQ_OK;
}

void moq_pub_destroy(moq_publisher_t *pub) {
    if (!pub) return;
    pending_fetch_clear(pub);   /* release any snapshot refs held for a fetch */
    moq_pub_track_t *t = pub->tracks;
    while (t) {
        moq_pub_track_t *next = t->next;
        t->ns_entry = NULL;
        free_track_state(pub, t);
        t = next;
    }
    while (pub->ns_entries) {
        pub_ns_entry_t *nse = pub->ns_entries;
        pub->ns_entries = nse->next;
        if (nse->id_buf)   pub_free(pub, nse->id_buf, nse->id_len);
        if (nse->id_parts) pub_free(pub, nse->id_parts,
            nse->id_count * sizeof(moq_bytes_t));
        pub_free(pub, nse, sizeof(*nse));
    }
    moq_alloc_t alloc = pub->alloc;
    alloc.free(pub, sizeof(moq_publisher_t), alloc.ctx);
}

/* Frozen original prefix: struct_size .. publisher_priority (the layout
 * before max_retained_bytes, has_publisher_priority, and monotonic_groups
 * were appended; it matches the add_track minimum struct_size,
 * offsetof(max_retained_bytes)). The pointer-only initializer touches only
 * this prefix, so an old caller that allocated the original-sized struct
 * is never overflowed, and every appended field stays disabled unless
 * _init_sized opts in. */
/* §10 ABI boundary. The v0 sizeof is defined INDEPENDENTLY of the
 * appended field -- the align-rounded end of has_publisher_priority, the
 * old struct's last member -- so the pin cannot follow a drifting
 * monotonic_groups offset. Equality (not >=): the appended field must sit
 * EXACTLY at the old sizeof boundary; starting later would silently grow
 * dead padding, starting earlier would read an old full-size caller's
 * uninitialized trailing padding as an explicit declaration. */
#define MOQ_PUB_TRACK_CFG_PRE_S3_SIZE \
    ((offsetof(moq_pub_track_cfg_t, has_publisher_priority) + \
      sizeof(((moq_pub_track_cfg_t *)0)->has_publisher_priority) + \
      (_Alignof(moq_pub_track_cfg_t) - 1)) & \
     ~(size_t)(_Alignof(moq_pub_track_cfg_t) - 1))
_Static_assert(offsetof(moq_pub_track_cfg_t, monotonic_groups) ==
               MOQ_PUB_TRACK_CFG_PRE_S3_SIZE,
               "monotonic_groups must sit exactly at the v0 sizeof");

#define MOQ_PUB_TRACK_CFG_V0_SIZE \
    (offsetof(moq_pub_track_cfg_t, max_retained_bytes))

void moq_pub_track_cfg_init(moq_pub_track_cfg_t *cfg) {
    if (!cfg) return;
    memset(cfg, 0, MOQ_PUB_TRACK_CFG_V0_SIZE);
    cfg->struct_size = (uint32_t)MOQ_PUB_TRACK_CFG_V0_SIZE;
    cfg->publisher_priority = 128;
}

void moq_pub_track_cfg_init_sized(moq_pub_track_cfg_t *cfg, size_t cfg_size) {
    if (!cfg) return;
    /* Clear exactly what the caller allocated, never beyond this library's
     * struct (clamp down for a caller newer than the library). */
    size_t n = cfg_size < sizeof(*cfg) ? cfg_size : sizeof(*cfg);
    if (n < sizeof(cfg->struct_size)) return;  /* too small to even stamp */
    memset(cfg, 0, n);
    cfg->struct_size = (uint32_t)n;
    if (n >= offsetof(moq_pub_track_cfg_t, publisher_priority) +
             sizeof(cfg->publisher_priority))
        cfg->publisher_priority = 128;
}

moq_result_t moq_pub_add_track(moq_publisher_t *pub,
                                 const moq_pub_track_cfg_t *cfg,
                                 uint64_t now_us,
                                 moq_pub_track_t **out)
{
    if (!pub || !cfg || !out) return MOQ_ERR_INVAL;
    *out = NULL;

    if (pub->closed) return MOQ_ERR_CLOSED;
    if (cfg->struct_size < offsetof(moq_pub_track_cfg_t, max_retained_bytes))
        return MOQ_ERR_INVAL;

    /* Validate namespace + track name against full-track-name budget. */
    if (moq_validate_full_track_name(&cfg->track_namespace,
                                      cfg->track_name) < 0)
        return MOQ_ERR_INVAL;

    /* Reject duplicate track identity. */
    if (find_track(pub, &cfg->track_namespace, cfg->track_name))
        return MOQ_ERR_INVAL;


    moq_pub_track_t *t = (moq_pub_track_t *)pub_alloc(pub,
        sizeof(moq_pub_track_t));
    if (!t) return MOQ_ERR_NOMEM;
    memset(t, 0, sizeof(*t));
    t->pub = pub;
    t->slot_cap = pub->sub_slot_cap + 1;
    t->slots = (pub_sub_slot_t *)pub_alloc(pub,
        t->slot_cap * sizeof(pub_sub_slot_t));
    if (!t->slots) {
        pub_free(pub, t, sizeof(moq_pub_track_t));
        return MOQ_ERR_NOMEM;
    }
    memset(t->slots, 0, t->slot_cap * sizeof(pub_sub_slot_t));
    {
        bool has_prio = false;
        size_t prio_off = offsetof(moq_pub_track_cfg_t, has_publisher_priority);
        if (cfg->struct_size >= prio_off + sizeof(cfg->has_publisher_priority))
            has_prio = cfg->has_publisher_priority;
        t->priority = has_prio ? cfg->publisher_priority
                               : pub->cfg.default_publisher_priority;
    }
    {
        uint64_t msb = 0;
        if (cfg->struct_size >= offsetof(moq_pub_track_cfg_t, max_retained_bytes) +
            sizeof(cfg->max_retained_bytes))
            msb = cfg->max_retained_bytes;
        t->max_retained_bytes = (msb == 0) ? (1024u * 1024u) : msb;
    }
    /* Whole-field gate: an old caller's struct_size can never cover the
     * appended bool (it starts at the old sizeof boundary, see the
     * _Static_assert at the initializers). */
    if (cfg->struct_size >= offsetof(moq_pub_track_cfg_t, monotonic_groups) +
        sizeof(cfg->monotonic_groups))
        t->monotonic = cfg->monotonic_groups;

    /* Sum namespace bytes with overflow check. */
    size_t total_ns_bytes = 0;
    for (size_t i = 0; i < cfg->track_namespace.count; i++) {
        size_t plen = cfg->track_namespace.parts[i].len;
        if (plen > SIZE_MAX - total_ns_bytes) {
            free_track_state(pub, t);
            return MOQ_ERR_INVAL;
        }
        total_ns_bytes += plen;
    }

    t->ns_count = cfg->track_namespace.count;
    t->ns_parts = (moq_bytes_t *)pub_alloc(pub,
        t->ns_count * sizeof(moq_bytes_t));
    if (!t->ns_parts) {
        free_track_state(pub, t);
        return MOQ_ERR_NOMEM;
    }

    t->ns_buf_len = total_ns_bytes;
    if (total_ns_bytes > 0) {
        t->ns_buf = (uint8_t *)pub_alloc(pub, total_ns_bytes);
        if (!t->ns_buf) {
            free_track_state(pub, t);
            return MOQ_ERR_NOMEM;
        }
    }

    size_t off = 0;
    for (size_t i = 0; i < t->ns_count; i++) {
        size_t plen = cfg->track_namespace.parts[i].len;
        t->ns_parts[i].data = t->ns_buf + off;
        t->ns_parts[i].len = plen;
        memcpy(t->ns_buf + off, cfg->track_namespace.parts[i].data, plen);
        off += plen;
    }

    /* Deep-copy track name. */
    t->name_len = cfg->track_name.len;
    t->name_buf = dup_bytes(pub, cfg->track_name.data, cfg->track_name.len);
    if (cfg->track_name.len > 0 && !t->name_buf) {
        free_track_state(pub, t);
        return MOQ_ERR_NOMEM;
    }

    /* Reserve this track's largest-location history record up front, so the
     * per-object merge on the write hot path is allocation-free and
     * infallible. History is mandatory: this is the sole capacity-failure
     * point for the registry, and a full registry, a temporary-key OOM, or a
     * record-key-copy OOM fails add_track with MOQ_ERR_NOMEM before the track
     * is linked or its namespace advertised. (The registry is always sized >=
     * 1 -- the cfg default is max_subscriptions+publishes+track_statuses -- so
     * there is no untracked-track bypass.) */
    {
        moq_namespace_t hns = { t->ns_parts, t->ns_count };
        moq_bytes_t hname = { t->name_buf, t->name_len };
        size_t hklen = 0;
        uint8_t *hkey = moq_build_track_key(pub->session, &hns, hname, &hklen);
        if (!hkey && hklen > 0) {       /* temporary-key OOM */
            free_track_state(pub, t);
            return MOQ_ERR_NOMEM;
        }
        t->hist = track_hist_reserve(pub->session, hkey, hklen);
        if (hkey) pub->session->alloc.free(hkey, hklen, pub->session->alloc.ctx);
        if (!t->hist) {                 /* registry full or record-key-copy OOM */
            free_track_state(pub, t);
            return MOQ_ERR_NOMEM;
        }
    }

    /* Namespace advertisement with refcounting. */
    if (cfg->advertise_namespace) {
        moq_namespace_t ns = { t->ns_parts, t->ns_count };
        pub_ns_entry_t *nse = find_ns_entry(pub, &ns);
        if (nse) {
            nse->refcount++;
            t->ns_entry = nse;
        } else {
            nse = create_ns_entry(pub, &ns);
            if (!nse) {
                free_track_state(pub, t);
                return MOQ_ERR_NOMEM;
            }
            moq_publish_namespace_cfg_t nscfg;
            moq_publish_namespace_cfg_init(&nscfg);
            nscfg.track_namespace = ns;
            moq_result_t rc = moq_session_publish_namespace(pub->session,
                &nscfg, now_us, &nse->handle);
            if (rc < 0) {
                free_ns_entry(pub, nse);
                free_track_state(pub, t);
                return rc;
            }
            nse->state = PUB_NS_PENDING;
            nse->refcount = 1;
            t->ns_entry = nse;
        }
    }

    t->next = pub->tracks;
    pub->tracks = t;
    pub->track_count++;
    *out = t;
    return MOQ_OK;
}

moq_result_t moq_pub_remove_track(moq_publisher_t *pub,
                                    moq_pub_track_t *track,
                                    uint64_t now_us)
{
    if (!pub || !track) return MOQ_ERR_INVAL;
    if (track->pub != pub) return MOQ_ERR_INVAL;

    /* Pre-flight checks before any side effects. */
    if (pub->deferred.active && pub->deferred.track == track)
        return MOQ_ERR_WOULD_BLOCK;
    if (pub->pending.active && pub->pending.track == track)
        return MOQ_ERR_WOULD_BLOCK;
    /* A pending retained FETCH is served from its own snapshot, so removing the
     * source track here is safe -- no guard needed. */
    if (track->ns_entry && track->ns_entry->state == PUB_NS_PENDING &&
        track->ns_entry->refcount <= 1)
        return MOQ_ERR_WRONG_STATE;

    if (track->publish_requested) {
        pub_sub_slot_t *psl = track_find_publication_slot(track);
        /* Removal is an ABANDONMENT path: when the pending op still has an
         * admitted-but-unsent object for this destination, or a Forward/
         * filter narrowing already armed a RESET, the subgroup omits
         * admitted work -- the drafts require RESET, never FIN (and a
         * required RESET must never become a FIN). Those cases proceed even
         * MID-STREAM: the armed/omitting bracket is exactly what the RESET
         * exists for. Only a clean mid-stream object (nothing armed, nothing
         * omitted) refuses -- finish or abandon it first. */
        bool omitted = psl &&
            (track->op.kind == PUB_OP_WRITE_OBJECT ||
             track->op.kind == PUB_OP_BEGIN_OBJECT) &&
            psl->target_op == track->cur_op &&
            psl->done_op != track->cur_op;
        bool must_reset = psl && (omitted || psl->needs_reset);
        if (psl && psl->streaming && !must_reset) return MOQ_ERR_WRONG_STATE;
        if (psl && psl->sg_open) {
            /* Only a subgroup with nothing omitted closes cleanly. The
             * reported stream count is EXACT either way; the peer's
             * completed-stream gate counts identifiable resets like FINs.
             * Slot state -- INCLUDING streaming, so a later finish-stage
             * WOULD_BLOCK retry cannot wedge on the mid-stream guard --
             * mutates only after the action queues (retry-idempotent). */
            moq_result_t rc;
            if (must_reset)
                rc = moq_session_reset_subgroup(pub->session, psl->sg, 0x0,
                                                now_us);
            else
                rc = moq_session_close_subgroup(pub->session, psl->sg,
                                                now_us);
            if (rc == MOQ_ERR_WOULD_BLOCK) return MOQ_ERR_WOULD_BLOCK;
            if (rc != MOQ_OK && rc != MOQ_ERR_STALE_HANDLE) return rc;
            psl->sg_open = false;
            psl->streaming = false;
            psl->needs_reset = false;
            if (omitted) psl->done_op = track->cur_op;
        }
        moq_finish_publish_cfg_t fcfg;
        moq_finish_publish_cfg_init(&fcfg);
        fcfg.stream_count = psl ? psl->streams_opened : 0;   /* exact */
        moq_result_t rc = moq_session_finish_publish(pub->session,
            track->publication, &fcfg, now_us);
        if (rc == MOQ_ERR_WOULD_BLOCK) return MOQ_ERR_WOULD_BLOCK;
        if (rc != MOQ_OK && rc != MOQ_ERR_STALE_HANDLE) return rc;
        track->publish_requested = false;
        track->publish_ok = false;
        if (psl) track_clear_slot(pub, psl);
    }

    /* Reset the live streaming subgroup on all active slots and retire each
     * publisher-side subscription, so the session frees its subscription entry
     * before the track is dropped. Without the retire step the session keeps the
     * accepted subscription alive after the facade loses the handle -- pinning the
     * subscription pool and rejecting a re-add of the same track as a duplicate.
     * Retryable + idempotent: a full action queue returns WOULD_BLOCK with the
     * slot still active and its subgroup already in its resolved state, so the
     * retry resumes mid-slot without re-sending or skipping (track_clear_slot
     * runs only after the done is queued). */
    for (size_t si = 0; si < track->slot_cap; si++) {
        pub_sub_slot_t *sl = &track->slots[si];
        if (!sl->active) continue;
        /* Publication slots are finished above via PUBLISH_DONE, not retired as
         * subscriptions; skip them so done_subscribe only sees real subscribers. */
        if (sl->kind == PUB_SLOT_PUBLICATION) continue;
        if (sl->sg_open) {
            moq_result_t rc = moq_session_reset_subgroup(pub->session,
                sl->sg, 0x0, now_us);
            if (rc == MOQ_ERR_WOULD_BLOCK)
                return MOQ_ERR_WOULD_BLOCK;
            if (rc != MOQ_OK && rc != MOQ_ERR_STALE_HANDLE)
                return rc;
            sl->sg_open = false;
        }
        /* The subgroup is reset above, so the done is valid (it requires no open
         * data stream). Stream count is EXACT -- the facade opened every stream
         * for this subscription itself (zero for datagram-only delivery). On
         * success the session frees the subscription; clear our slot to match. */
        moq_done_subscribe_cfg_t dcfg;
        moq_done_subscribe_cfg_init(&dcfg);
        dcfg.status_code = MOQ_PUB_DONE_TRACK_ENDED;
        dcfg.stream_count = sl->streams_opened;
        moq_result_t rc = moq_session_done_subscribe(pub->session, sl->sub,
                                                     &dcfg, now_us);
        if (rc == MOQ_ERR_WOULD_BLOCK)
            return MOQ_ERR_WOULD_BLOCK;
        if (rc != MOQ_OK && rc != MOQ_ERR_STALE_HANDLE)
            return rc;
        track_clear_slot(pub, sl);
    }

    if (track->ns_entry) {
        pub_ns_entry_t *nse = track->ns_entry;
        nse->refcount--;
        if (nse->refcount == 0 && nse->state == PUB_NS_ACCEPTED) {
            moq_result_t rc = moq_session_publish_namespace_done(
                pub->session, nse->handle, now_us);
            if (rc == MOQ_ERR_WOULD_BLOCK) {
                nse->refcount++;
                return MOQ_ERR_WOULD_BLOCK;
            }
            if (rc != MOQ_OK && rc != MOQ_ERR_STALE_HANDLE) {
                nse->refcount++;
                return rc;
            }
            nse->state = PUB_NS_TERMINAL;
        }
        if (nse->refcount == 0)
            free_ns_entry(pub, nse);
        track->ns_entry = NULL;
    }

    unlink_track(pub, track);
    free_track_state(pub, track);
    return MOQ_OK;
}

/* -- Retained group (origin-local Joining-FETCH cache) -------------- */

/* Install a retained group from `src` (object_id strictly ascending; src[0] is
 * the independent base). Validates bounds/order/byte-budget, deep-copies
 * (incref), replaces the prior retained set, and resets wrote_object -- the
 * retained group's last object is now the track's latest published location, so
 * Largest may be advertised. src is NOT pre-increffed (the snapshot increfs). On
 * any error the prior retained set is left intact. */
static moq_result_t track_install_retained(moq_pub_track_t *t, uint64_t group_id,
                                           const pub_retained_obj_t *src, size_t n)
{
    if (n == 0 || n > MOQ_PUB_RETAINED_MAX_OBJECTS) return MOQ_ERR_INVAL;
    uint64_t total = 0;
    for (size_t i = 0; i < n; i++) {
        if (!src[i].payload) return MOQ_ERR_INVAL;
        if (i > 0 && src[i].object_id <= src[i - 1].object_id)
            return MOQ_ERR_INVAL;   /* strictly ascending object_id */
        size_t pl = moq_rcbuf_len(src[i].payload);
        size_t pr = src[i].properties ? moq_rcbuf_len(src[i].properties) : 0;
        if (pl > t->max_retained_bytes - total) return MOQ_ERR_INVAL;
        total += pl;
        if (pr > t->max_retained_bytes - total) return MOQ_ERR_INVAL;
        total += pr;
    }
    pub_retained_obj_t *vec = pub_retained_snapshot(&t->pub->alloc, src, n);
    if (!vec) return MOQ_ERR_NOMEM;
    track_release_retained(t);
    t->retained = vec;
    t->retained_count = n;
    t->retained_group_id = group_id;
    t->has_retained = true;
    t->wrote_object = false;   /* retained group is now the latest published loc */
    return MOQ_OK;
}

void moq_pub_retained_group_cfg_init(moq_pub_retained_group_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = sizeof(*cfg);
}

moq_result_t moq_pub_set_retained_group(moq_publisher_t *pub,
                                        moq_pub_track_t *track,
                                        const moq_pub_retained_group_cfg_t *cfg)
{
    if (!pub || !track || !cfg) return MOQ_ERR_INVAL;
    if (track->pub != pub) return MOQ_ERR_INVAL;
    if (cfg->struct_size < sizeof(moq_pub_retained_group_cfg_t))
        return MOQ_ERR_INVAL;
    if (track->ended) return MOQ_ERR_WRONG_STATE;
    if (!cfg->objects || cfg->object_count == 0) return MOQ_ERR_INVAL;
    if (cfg->object_count > MOQ_PUB_RETAINED_MAX_OBJECTS) return MOQ_ERR_INVAL;

    /* Map the public objects into the internal element shape, then install. The
     * group API requires a DENSE 0..N catalog shape: objects[i].object_id == i,
     * objects[0] the independent base, no gaps (a FETCH/replay must reconstruct
     * the catalog from object 0 through the last delta). */
    pub_retained_obj_t tmp[MOQ_PUB_RETAINED_MAX_OBJECTS];
    for (size_t i = 0; i < cfg->object_count; i++) {
        if (cfg->objects[i].object_id != (uint64_t)i) return MOQ_ERR_INVAL;
        tmp[i].object_id    = cfg->objects[i].object_id;
        tmp[i].end_of_group = cfg->objects[i].end_of_group;
        tmp[i].payload      = cfg->objects[i].payload;
        tmp[i].properties   = cfg->objects[i].properties;
    }
    return track_install_retained(track, cfg->group_id, tmp, cfg->object_count);
}

moq_result_t moq_pub_clear_retained_group(moq_publisher_t *pub,
                                          moq_pub_track_t *track)
{
    if (!pub || !track) return MOQ_ERR_INVAL;
    if (track->pub != pub) return MOQ_ERR_INVAL;
    track_release_retained(track);
    return MOQ_OK;
}

/* Frozen v0 object-cfg prefix: through `status` (the write gate's floor);
 * _reserved_obj and the appended end_of_group lie past an old caller's
 * storage. */
#define MOQ_PUB_OBJECT_CFG_V0_SIZE \
    offsetof(moq_pub_object_cfg_t, _reserved_obj)

void moq_pub_object_cfg_init(moq_pub_object_cfg_t *cfg) {
    if (!cfg) return;
    memset(cfg, 0, MOQ_PUB_OBJECT_CFG_V0_SIZE);
    cfg->struct_size = (uint32_t)MOQ_PUB_OBJECT_CFG_V0_SIZE;
}

void moq_pub_object_cfg_init_sized(moq_pub_object_cfg_t *cfg,
                                   size_t cfg_size) {
    if (!cfg) return;
    size_t n = cfg_size < sizeof(*cfg) ? cfg_size : sizeof(*cfg);
    if (n < sizeof(cfg->struct_size)) return;
    memset(cfg, 0, n);
    cfg->struct_size = (uint32_t)n;
}

static moq_result_t write_stream_object(moq_publisher_t *pub,
    moq_pub_track_t *track, pub_sub_slot_t *slot,
    const moq_pub_object_cfg_t *obj, uint64_t now_us)
{
    bool need_ext = (obj->properties != NULL);

    bool want_eog = false;
    if (obj->struct_size >= offsetof(moq_pub_object_cfg_t, end_of_group) +
        sizeof(obj->end_of_group))
        want_eog = obj->end_of_group;

    /* Same-group extension mode mismatch: can't reopen mid-stream. */
    if (slot->sg_open && slot->cur_group == obj->group_id &&
        need_ext && !slot->sg_has_extensions)
        return MOQ_ERR_WRONG_STATE;

    /* Same-group end_of_group mismatch: header already on the wire. */
    if (slot->sg_open && slot->cur_group == obj->group_id &&
        want_eog != slot->sg_end_of_group)
        return MOQ_ERR_WRONG_STATE;

    /* Close subgroup if group changed. */
    if (slot->sg_open && slot->cur_group != obj->group_id) {
        moq_result_t rc = moq_session_close_subgroup(pub->session,
            slot->sg, now_us);
        if (rc == MOQ_ERR_WOULD_BLOCK) return MOQ_ERR_WOULD_BLOCK;
        if (rc == MOQ_ERR_STALE_HANDLE) {
            track_clear_slot(pub, slot);
            return MOQ_OK;
        }
        if (rc < 0) return rc;
        slot->sg_open = false;
    }

    if (!slot->sg_open) {
        moq_subgroup_cfg_t sgcfg;
        moq_subgroup_cfg_init(&sgcfg);
        sgcfg.group_id = obj->group_id;
        sgcfg.subgroup_id = 0;
        sgcfg.publisher_priority = track->priority;
        sgcfg.object_properties = need_ext;
        sgcfg.end_of_group = want_eog;

        moq_result_t rc = (slot->kind == PUB_SLOT_PUBLICATION)
            ? moq_session_open_pub_subgroup(pub->session,
                slot->pub, &sgcfg, now_us, &slot->sg)
            : moq_session_open_subgroup(pub->session,
                slot->sub, &sgcfg, now_us, &slot->sg);

        if (rc == MOQ_ERR_WOULD_BLOCK) return MOQ_ERR_WOULD_BLOCK;
        if (rc == MOQ_ERR_STALE_HANDLE) {
            track_clear_slot(pub, slot);
            return MOQ_OK;
        }
        if (rc < 0) return rc;
        slot->sg_open = true;
        slot->streams_opened++;
        slot->sg_has_extensions = need_ext;
        slot->sg_end_of_group = want_eog;
        slot->cur_group = obj->group_id;
    }

    if (need_ext || slot->sg_has_extensions) {
        moq_object_cfg_t ocfg;
        moq_object_cfg_init(&ocfg);
        ocfg.object_id = obj->object_id;
        ocfg.payload = obj->payload;
        ocfg.properties = obj->properties;
        moq_result_t rc = moq_session_write_object_ex(pub->session,
            slot->sg, &ocfg, now_us);
        if (rc == MOQ_ERR_WOULD_BLOCK) return MOQ_ERR_WOULD_BLOCK;
        if (rc == MOQ_ERR_STALE_HANDLE) {
            track_clear_slot(pub, slot);
            return MOQ_OK;
        }
        return rc;
    }

    moq_result_t rc = moq_session_write_object(pub->session,
        slot->sg, obj->object_id, obj->payload, now_us);
    if (rc == MOQ_ERR_WOULD_BLOCK) return MOQ_ERR_WOULD_BLOCK;
    if (rc == MOQ_ERR_STALE_HANDLE) {
        track_clear_slot(pub, slot);
        return MOQ_OK;
    }
    return rc;
}

static moq_result_t write_datagram_object(moq_publisher_t *pub,
    moq_pub_track_t *track, pub_sub_slot_t *slot,
    const moq_pub_object_cfg_t *obj, uint64_t now_us)
{
    if (obj->has_status) {
        moq_result_t rc = (slot->kind == PUB_SLOT_PUBLICATION)
            ? moq_session_send_pub_status_datagram(pub->session,
                slot->pub, obj->group_id, obj->object_id, track->priority,
                obj->status, now_us)
            : moq_session_send_status_datagram(pub->session,
                slot->sub, obj->group_id, obj->object_id, track->priority,
                obj->status, now_us);
        if (rc == MOQ_ERR_STALE_HANDLE) {
            track_clear_slot(pub, slot);
            return MOQ_OK;
        }
        return rc;
    }

    const uint8_t *props = NULL;
    size_t props_len = 0;
    if (obj->properties) {
        props = moq_rcbuf_data(obj->properties);
        props_len = moq_rcbuf_len(obj->properties);
    }

    moq_result_t rc = (slot->kind == PUB_SLOT_PUBLICATION)
        ? moq_session_send_pub_object_datagram(pub->session,
            slot->pub, obj->group_id, obj->object_id, track->priority,
            false, obj->payload, props, props_len, now_us)
        : moq_session_send_object_datagram(pub->session,
            slot->sub, obj->group_id, obj->object_id, track->priority,
            false, obj->payload, props, props_len, now_us);
    if (rc == MOQ_ERR_STALE_HANDLE) {
        track_clear_slot(pub, slot);
        return MOQ_OK;
    }
    return rc;
}

moq_result_t moq_pub_write_object_ex(moq_publisher_t *pub,
                                       moq_pub_track_t *track,
                                       const moq_pub_object_cfg_t *obj,
                                       uint64_t now_us)
{
    if (!pub || !track || !obj) return MOQ_ERR_INVAL;
    if (track->pub != pub) return MOQ_ERR_INVAL;
    if (obj->struct_size < offsetof(moq_pub_object_cfg_t, _reserved_obj))
        return MOQ_ERR_INVAL;
    if (pub->closed) return MOQ_ERR_CLOSED;
    if (track->ended) return MOQ_ERR_WRONG_STATE;

    if (obj->has_status && obj->payload) return MOQ_ERR_INVAL;
    if (obj->has_status && !obj->datagram) return MOQ_ERR_INVAL;
    if (!obj->has_status && !obj->payload) return MOQ_ERR_INVAL;
    if (obj->has_status &&
        obj->status != MOQ_OBJECT_NORMAL &&
        obj->status != MOQ_OBJECT_END_OF_GROUP &&
        obj->status != MOQ_OBJECT_END_OF_TRACK)
        return MOQ_ERR_INVAL;

    if (track->streaming_active && !obj->datagram) return MOQ_ERR_WRONG_STATE;

    /* end_of_group is APPENDED past the v0 gate: read it only when the
     * caller's struct_size covers it -- an exactly-old-sized cfg must never
     * be read past its storage. */
    bool want_eog = false;
    if (obj->struct_size >= offsetof(moq_pub_object_cfg_t, end_of_group) +
                                sizeof(obj->end_of_group))
        want_eog = obj->end_of_group;

    /* Admission order (Design v8 §1): peek FIRST (no mutation); a divergent
     * retry is refused untouched -- before any retirement or state change.
     * On a genuinely new op, run all deterministic validation, then drive the
     * lazy Forward-0 retirements, then commit and merge Largest exactly once.
     * A retry/resume skips validation and the merge (already done) but still
     * drives retirements and re-drives the fan-out. Identity is byte-exact (a
     * retry may carry a FRESH buffer with identical bytes -- the media-sender
     * re-encode pattern -- but never different bytes). The object is admitted
     * regardless of how many destinations are eligible: a zero-destination or
     * Forward-0 write still advances Largest. */
    moq_result_t st = track_op_peek(track, PUB_OP_WRITE_OBJECT,
        obj->group_id, obj->object_id, obj->datagram, obj->has_status,
        obj->has_status ? obj->status : MOQ_OBJECT_NORMAL,
        want_eog, 0, obj->payload, obj->properties);
    if (st == MOQ_ERR_WRONG_STATE) return MOQ_ERR_WRONG_STATE;
    if (st == 1) {
        uint64_t plen = obj->payload ? moq_rcbuf_len(obj->payload) : 0;
        moq_result_t vr = validate_new_object(track, obj->group_id,
            obj->object_id, plen, obj->properties);
        if (vr < 0) return vr;                  /* no mutation on rejection */
        /* §10 (B): the declared watermark seals groups at or below it.
         * Rejected BEFORE any mutation -- not even retire drainage runs.
         * Resumes (st == MOQ_OK) are never re-checked: the declare
         * preflight refuses to cover a pending op, so an in-flight op is
         * always above the watermark. */
        if (track->has_declared && obj->group_id <= track->declared_through)
            return MOQ_ERR_WRONG_STATE;
        /* §10 (A): the monotonic promise is enforced, which is what
         * makes the evidence trustworthy. Decreasing groups reject; a
         * group with recorded End-of-Group evidence rejects; SAME-group
         * writes stay legal until actual evidence exists. */
        if (track->monotonic) {
            if (track->has_begun && obj->group_id < track->begun_high)
                return MOQ_ERR_WRONG_STATE;
            if (track->has_eog && obj->group_id <= track->eog_high)
                return MOQ_ERR_WRONG_STATE;
        }
    }
    /* Drain any Forward-0 retires (retryable). */
    {
        moq_result_t rr = track_run_retires(pub, track, now_us);
        if (rr < 0) return rr;
    }
    if (st == 1) {
        track_op_commit(track, PUB_OP_WRITE_OBJECT,
            obj->group_id, obj->object_id, obj->datagram, obj->has_status,
            obj->has_status ? obj->status : MOQ_OBJECT_NORMAL,
            want_eog, 0, obj->payload, obj->properties);
        track_hist_merge(track->hist, obj->group_id, obj->object_id);
        MOQ_PUB_TEST_BUMP(moq_pub_test_merge_count);
        if (track->monotonic) {
            /* begun_high stamps exactly once at commit (the retry-safe
             * point history already uses), status objects and
             * zero-destination writes included. */
            if (!track->has_begun || obj->group_id > track->begun_high) {
                track->has_begun = true;
                track->begun_high = obj->group_id;
            }
            /* Status 0x3 promotes at ADMISSION: the object itself asserts
             * no higher object exists in its group (not FIN-qualified).
             * Status 0x4 asserts nonexistence ABOVE a location, not
             * completeness below it -- never evidence. */
            if (obj->has_status && obj->status == MOQ_OBJECT_END_OF_GROUP &&
                (!track->has_eog || obj->group_id > track->eog_high)) {
                track->has_eog = true;
                track->eog_high = obj->group_id;
            }
        }
    }

    /* Every destination writes the RETAINED snapshot, never the caller's
     * (transient) buffers: partial completion can never diverge on the wire.
     * Built FIELD-BY-FIELD (a whole-struct copy of *obj would read an
     * old-ABI caller's storage out of bounds). */
    moq_pub_object_cfg_t snap;
    moq_pub_object_cfg_init_sized(&snap, sizeof(snap));
    snap.group_id = obj->group_id;
    snap.object_id = obj->object_id;
    snap.datagram = obj->datagram;
    snap.has_status = obj->has_status;
    snap.status = track->op.status;
    snap.end_of_group = want_eog;
    snap.payload = track->op.payload;
    snap.properties = track->op.properties;

    for (size_t si = 0; si < track->slot_cap; si++) {
        pub_sub_slot_t *slot = &track->slots[si];
        if (!slot_op_pending(track, slot)) continue;
        moq_result_t rc = obj->datagram
            ? write_datagram_object(pub, track, slot, &snap, now_us)
            : write_stream_object(pub, track, slot, &snap, now_us);
        if (rc == MOQ_ERR_WOULD_BLOCK) return MOQ_ERR_WOULD_BLOCK;
        if (rc < 0) return rc;               /* op stays pending; retryable */
        if (slot->active) slot->done_op = track->cur_op;
    }

    /* §10: the EOG candidate rides the op and applies only now that
     * the op FULLY completed (every participating destination served; a
     * higher-group stream op has FINned the prior group's subgroups on all
     * of them). Ordering matters: a crossed previously-armed group
     * promotes FIRST, then the completed op's own flag arms/disarms its
     * group. Datagram ops close no subgroups: they neither promote nor
     * touch the armed state. A blocked/failed/abandoned op never reaches
     * here. */
    if (track->monotonic && !track->op.datagram && !track->op.has_status) {
        if (track->eog_armed && track->op.group_id > track->eog_group) {
            if (!track->has_eog || track->eog_group > track->eog_high) {
                track->has_eog = true;
                track->eog_high = track->eog_group;
            }
            track->eog_armed = false;
        }
        track->eog_armed = track->op.end_of_group;
        track->eog_group = track->op.group_id;
    }
    track_op_release(track);
    track->wrote_object = true;
    return MOQ_OK;
}

moq_result_t moq_pub_write_object(moq_publisher_t *pub,
                                    moq_pub_track_t *track,
                                    uint64_t group_id,
                                    uint64_t object_id,
                                    moq_rcbuf_t *payload,
                                    uint64_t now_us)
{
    if (!pub || !track) return MOQ_ERR_INVAL;
    moq_pub_object_cfg_t obj;
    moq_pub_object_cfg_init(&obj);
    obj.group_id = group_id;
    obj.object_id = object_id;
    obj.payload = payload;
    return moq_pub_write_object_ex(pub, track, &obj, now_us);
}

moq_result_t moq_pub_end_track(moq_publisher_t *pub, moq_pub_track_t *track,
                               uint64_t now_us)
{
    if (!pub || !track) return MOQ_ERR_INVAL;
    if (track->pub != pub) return MOQ_ERR_INVAL;
    if (pub->closed) return MOQ_ERR_CLOSED;
    if (track->ended) return MOQ_OK;          /* idempotent: already terminated */

    /* Preflight BEFORE any state mutation: a WRONG_STATE return must not
     * have already sent PUBLISH_DONE. Only a pending END_TRACK retry may
     * proceed past here. */
    if (track->streaming_active) return MOQ_ERR_WRONG_STATE;  /* mid object */
    if (track->op.kind != PUB_OP_NONE && track->op.kind != PUB_OP_END_TRACK)
        return MOQ_ERR_WRONG_STATE;

    {
        moq_result_t rr = track_run_retires(pub, track, now_us);
        if (rr < 0) return rr;
    }

    /* CHECKED terminal plan, made BEFORE the unpublish / any history
     * mutation and retained across retries in the op identity (has_status
     * records the chosen mode). The canonical terminal Location is TRACK-
     * WIDE -- {last published group + 1, 0} from track history ({0,0} for an
     * empty track) -- never a destination-local cursor. It must also be
     * REPRESENTABLE through the current data plane (object group ids are
     * QUIC varints in both profiles, cap 2^62-1; draft-18 history can reach
     * UINT64_MAX, where +1 would wrap to zero): when no fresh terminal
     * Location exists, NO END_OF_TRACK is fabricated and NOTHING is merged
     * into history -- streams close and every subscription terminates with
     * PUBLISH_DONE(TRACK_ENDED) instead. */
    bool term_fresh = true;
    uint64_t term_g = 0, term_o = 0;
    if (track->op.kind == PUB_OP_END_TRACK) {
        term_fresh = track->op.has_status;     /* mode retained across retries */
        term_g = track->op.group_id;
        term_o = track->op.object_id;
    } else {
        /* The terminal group is strictly above BOTH the published history
         * and the declared completion watermark (a status object at or
         * below the watermark would publish inside a sealed range); at the
         * varint ceiling it degrades to the no-status mode. */
        uint64_t base = 0;
        bool has_base = false;
        if (track->hist && track->hist->has_largest) {
            base = track->hist->largest_group;
            has_base = true;
        }
        if (track->has_declared &&
            (!has_base || track->declared_through > base)) {
            base = track->declared_through;
            has_base = true;
        }
        if (has_base) {
            if (base >= MOQ_QUIC_VARINT_MAX)
                term_fresh = false;
            else
                term_g = base + 1;
        }
    }

    /* End the publication (PUBLISH_DONE) first, then the subscription slots. */
    {
        moq_result_t rc = moq_pub_unpublish_track(pub, track, now_us);
        if (rc != MOQ_OK) return rc;
    }

    moq_result_t st = track_op_begin(track, PUB_OP_END_TRACK,
        term_g, term_o, false, term_fresh,
        term_fresh ? MOQ_OBJECT_END_OF_TRACK : MOQ_OBJECT_NORMAL,
        false, 0, NULL, NULL);
    if (st == MOQ_ERR_WRONG_STATE) return MOQ_ERR_WRONG_STATE;
    /* Merge the terminal Location into history exactly once: on the fresh
     * commit only -- a WOULD_BLOCK retry resumes without re-merging -- and
     * ONLY when a fresh terminal actually exists (never wrap/regress). */
    if (st == 1 && term_fresh && track->hist)
        track_hist_merge(track->hist, term_g, term_o);

    for (size_t si = 0; si < track->slot_cap; si++) {
        pub_sub_slot_t *slot = &track->slots[si];
        if (!slot->active) continue;
        /* Publication termination is PUBLISH_DONE (the unpublish above),
         * never an END_OF_TRACK status object. */
        if (slot->kind == PUB_SLOT_PUBLICATION) {
            slot->done_op = track->cur_op;
            continue;
        }
        if (slot->done_op == track->cur_op) continue;   /* served this op */
        /* In the operation = stamped at admission AND still eligible. A slot
         * OUTSIDE the operation (Forward 0 at admission, or lazily retired)
         * cannot receive the status OBJECT -- but both drafts require the
         * terminal control message REGARDLESS of Forward state, so it takes
         * the terminal-control path below instead of being skipped. */
        bool in_op = (slot->target_op == track->cur_op) &&
                     slot_eligible(track, slot);

        if (!term_fresh || !in_op ||
            !window_admits(&slot->window, term_g, term_o)) {
            /* Terminal control: close any open subgroup FIRST (streams close
             * before the done, same normative ordering as
             * finish_subscribers; a Forward-0 slot's open subgroup was
             * already RESET by the retire sweep above), then
             * SUBSCRIBE_DONE/PUBLISH_DONE. Status is exact wire semantics:
             * SUBSCRIPTION_ENDED (0x3) ONLY when the subscription's FINITE
             * filter end was genuinely passed by the terminal Location;
             * everything else -- open-ended windows the track never
             * reached, Forward-0 pauses -- is TRACK_ENDED (0x2): the track
             * is no longer being published. Stream count is EXACT: the
             * facade opened every stream for this subscription itself.
             * Retry-safe: sg_open clears only on a successful close, the
             * slot clears only after a successful/stale done, so a
             * WOULD_BLOCK retry resumes exactly where it stopped and never
             * doubles the done. */
            if (slot->sg_open) {
                moq_result_t rc = moq_session_close_subgroup(pub->session,
                                                             slot->sg, now_us);
                if (rc == MOQ_ERR_WOULD_BLOCK) return MOQ_ERR_WOULD_BLOCK;
                if (rc == MOQ_ERR_STALE_HANDLE) {
                    track_clear_slot(pub, slot);
                    continue;
                }
                if (rc < 0) return rc;
                slot->sg_open = false;
            }
            moq_done_subscribe_cfg_t dcfg;
            moq_done_subscribe_cfg_init(&dcfg);
            dcfg.status_code =
                (term_fresh &&
                 slot->window.has_window && slot->window.has_end &&
                 term_g > slot->window.end_group)
                    ? 0x3    /* SUBSCRIPTION_ENDED: past the finite end */
                    : 0x2;   /* TRACK_ENDED (incl. the no-fresh-terminal
                              * degraded mode: the track just ended) */
            dcfg.stream_count = slot->streams_opened;
            moq_result_t rc = moq_session_done_subscribe(pub->session,
                                                         slot->sub, &dcfg,
                                                         now_us);
            if (rc == MOQ_ERR_WOULD_BLOCK) return MOQ_ERR_WOULD_BLOCK;
            if (rc != MOQ_OK && rc != MOQ_ERR_STALE_HANDLE) return rc;
            track_clear_slot(pub, slot);   /* subscription freed */
            continue;
        }

        /* The terminal END_OF_TRACK goes on a fresh non-extension subgroup
         * at the canonical terminal Location: close any open subgroup (it
         * may carry extensions) first. Closing the prior subgroup is
         * independently retry-safe -- it clears sg_open, so a retry skips
         * this block. */
        if (slot->sg_open) {
            moq_result_t rc = moq_session_close_subgroup(pub->session,
                slot->sg, now_us);
            if (rc == MOQ_ERR_WOULD_BLOCK) return MOQ_ERR_WOULD_BLOCK;
            if (rc == MOQ_ERR_STALE_HANDLE) {
                track_clear_slot(pub, slot);
                continue;
            }
            if (rc < 0) return rc;
            slot->sg_open = false;
        }

        /* Open -> status object -> FIN must queue atomically per slot
         * (open emits its header eagerly; a mid-triple WOULD_BLOCK would
         * strand an empty terminal subgroup). Preflight the triple. */
        if (moq_session_action_capacity(pub->session) < 3)
            return MOQ_ERR_WOULD_BLOCK;

        moq_subgroup_cfg_t sgcfg;
        moq_subgroup_cfg_init(&sgcfg);
        sgcfg.group_id = term_g;
        sgcfg.subgroup_id = 0;
        sgcfg.publisher_priority = track->priority;
        sgcfg.object_properties = false;
        moq_subgroup_handle_t sg;
        moq_result_t rc = moq_session_open_subgroup(pub->session, slot->sub,
            &sgcfg, now_us, &sg);
        if (rc == MOQ_ERR_STALE_HANDLE) {
            track_clear_slot(pub, slot);
            continue;
        }
        if (rc < 0) return rc;
        slot->streams_opened++;

        /* Capacity reserved: the remaining two actions cannot WOULD_BLOCK. */
        rc = moq_session_write_status_object(pub->session, sg, term_o,
                                             MOQ_OBJECT_END_OF_TRACK, now_us);
        if (rc < 0) {
            (void)moq_session_close_subgroup(pub->session, sg, now_us);
            return rc;
        }
        slot->cur_group = sgcfg.group_id;
        (void)moq_session_close_subgroup(pub->session, sg, now_us);  /* FIN */
        if (slot->active) slot->done_op = track->cur_op;
    }

    /* §10: end_track's per-slot closes are clean FINs -- promote the
     * armed evidence like end_group. */
    if (track->monotonic && track->eog_armed) {
        if (!track->has_eog || track->eog_group > track->eog_high) {
            track->has_eog = true;
            track->eog_high = track->eog_group;
        }
        track->eog_armed = false;
    }
    track_op_release(track);
    track->ended = true;
    return MOQ_OK;
}

moq_result_t moq_pub_finish_subscribers(moq_publisher_t *pub,
                                        moq_pub_track_t *track,
                                        uint64_t status_code, uint64_t now_us)
{
    if (!pub || !track) return MOQ_ERR_INVAL;
    if (track->pub != pub) return MOQ_ERR_INVAL;
    if (pub->closed) return MOQ_ERR_CLOSED;

    /* Finish every active subscriber, but DO NOT terminalize the track: no
     * track->ended, no unlink, no retained-group teardown -- the track stays
     * registered and a later subscribe + explicit Joining FETCH can still pull
     * the retained group (MSF 11.3 step 1, live -> VOD). Idempotent: a slot freed
     * below goes inactive, so a retry skips it (no duplicate done). WOULD_BLOCK on
     * any step returns with the slot still active and its subgroup already in its
     * resolved state, so the retry resumes mid-slot without re-sending/skipping. */
    for (size_t si = 0; si < track->slot_cap; si++) {
        pub_sub_slot_t *slot = &track->slots[si];
        if (!slot->active) continue;
        /* Publication slots are finished via PUBLISH_DONE (unpublish/
         * end_track), never retired as subscriptions: skip BEFORE any state
         * checks so a publication's open/streaming subgroup neither blocks
         * nor is touched by this walk (mirrors remove_track's retire loop). */
        if (slot->kind == PUB_SLOT_PUBLICATION) continue;
        if (slot->streaming) return MOQ_ERR_WRONG_STATE;  /* mid object; retry */

        /* SUBSCRIBE_DONE requires no open data stream for the subscription. Close
         * (FIN) the live subgroup so delivered objects land cleanly. Retry-safe:
         * sg_open clears only on success, so a WOULD_BLOCK retry re-runs the same
         * close and the done is never reached twice. */
        if (slot->sg_open) {
            moq_result_t rc = moq_session_close_subgroup(pub->session,
                                                         slot->sg, now_us);
            if (rc == MOQ_ERR_WOULD_BLOCK) return MOQ_ERR_WOULD_BLOCK;
            if (rc != MOQ_OK && rc != MOQ_ERR_STALE_HANDLE) return rc;
            slot->sg_open = false;
        }

        /* Send the session-level done with the caller's status code. Stream
         * count is EXACT -- the facade opened every stream for this
         * subscription itself (zero for datagram-only delivery). On success
         * the session frees the subscription, so clear our slot to match
         * (and to stay idempotent). */
        moq_done_subscribe_cfg_t dcfg;
        moq_done_subscribe_cfg_init(&dcfg);
        dcfg.status_code = status_code;
        dcfg.stream_count = slot->streams_opened;
        moq_result_t rc = moq_session_done_subscribe(pub->session, slot->sub,
                                                     &dcfg, now_us);
        if (rc == MOQ_ERR_WOULD_BLOCK) return MOQ_ERR_WOULD_BLOCK;
        if (rc != MOQ_OK && rc != MOQ_ERR_STALE_HANDLE) return rc;
        track_clear_slot(pub, slot);   /* subscription freed (or already gone) */
    }
    return MOQ_OK;
}

/* -- Streaming writes --------------------------------------------- */

/* Frozen v0 begin-cfg prefix: through payload_length (the begin gate's
 * floor); the appended properties field lies past an old caller's storage. */
#define MOQ_PUB_BEGIN_CFG_V0_SIZE \
    (offsetof(moq_pub_begin_object_cfg_t, payload_length) + \
     sizeof(((moq_pub_begin_object_cfg_t *)0)->payload_length))

void moq_pub_begin_object_cfg_init(moq_pub_begin_object_cfg_t *cfg) {
    if (!cfg) return;
    memset(cfg, 0, MOQ_PUB_BEGIN_CFG_V0_SIZE);
    cfg->struct_size = (uint32_t)MOQ_PUB_BEGIN_CFG_V0_SIZE;
}

void moq_pub_begin_object_cfg_init_sized(moq_pub_begin_object_cfg_t *cfg,
                                         size_t cfg_size) {
    if (!cfg) return;
    size_t n = cfg_size < sizeof(*cfg) ? cfg_size : sizeof(*cfg);
    if (n < sizeof(cfg->struct_size)) return;
    memset(cfg, 0, n);
    cfg->struct_size = (uint32_t)n;
}

static moq_result_t begin_object_impl(moq_publisher_t *pub,
                                      moq_pub_track_t *track,
                                      const moq_pub_begin_object_cfg_t *cfg,
                                      uint64_t now_us)
{
    if (!pub || !track || !cfg) return MOQ_ERR_INVAL;
    if (track->pub != pub) return MOQ_ERR_INVAL;
    if (cfg->struct_size < offsetof(moq_pub_begin_object_cfg_t, payload_length) +
        sizeof(cfg->payload_length)) return MOQ_ERR_INVAL;
    if (pub->closed) return MOQ_ERR_CLOSED;
    if (track->ended) return MOQ_ERR_WRONG_STATE;
    if (cfg->payload_length > MOQ_QUIC_VARINT_MAX) return MOQ_ERR_INVAL;

    if (track->streaming_active) return MOQ_ERR_WRONG_STATE;  /* double begin */

    moq_rcbuf_t *begin_props = NULL;
    if (cfg->struct_size >= offsetof(moq_pub_begin_object_cfg_t, properties) +
        sizeof(cfg->properties))
        begin_props = cfg->properties;

    /* Admission order (Design v8 §1): peek FIRST (no mutation); a divergent
     * retry is refused untouched. On a new op, deterministic validation
     * (location + object properties) precedes retirements, commit, and the
     * exactly-once Largest merge. Identity covers group/object/declared length
     * AND the properties bytes, so a different begin can never resume a
     * partially completed one. */
    moq_result_t st = track_op_peek(track, PUB_OP_BEGIN_OBJECT,
        cfg->group_id, cfg->object_id, false, false, MOQ_OBJECT_NORMAL,
        false, cfg->payload_length, NULL, begin_props);
    if (st == MOQ_ERR_WRONG_STATE) return MOQ_ERR_WRONG_STATE;
    if (st == 1) {
        moq_result_t vr = validate_new_object(track, cfg->group_id,
            cfg->object_id, cfg->payload_length, begin_props);
        if (vr < 0) return vr;                  /* no mutation on rejection */
        /* §10 (B): sealed groups reject before any mutation (see
         * moq_pub_write_object_ex). */
        if (track->has_declared && cfg->group_id <= track->declared_through)
            return MOQ_ERR_WRONG_STATE;
        /* §10 (A): same monotonicity rules as the one-shot path. */
        if (track->monotonic) {
            if (track->has_begun && cfg->group_id < track->begun_high)
                return MOQ_ERR_WRONG_STATE;
            if (track->has_eog && cfg->group_id <= track->eog_high)
                return MOQ_ERR_WRONG_STATE;
        }
    }
    {
        moq_result_t rr = track_run_retires(pub, track, now_us);
        if (rr < 0) return rr;
    }
    if (st == 1) {
        track_op_commit(track, PUB_OP_BEGIN_OBJECT,
            cfg->group_id, cfg->object_id, false, false, MOQ_OBJECT_NORMAL,
            false, cfg->payload_length, NULL, begin_props);
        track_hist_merge(track->hist, cfg->group_id, cfg->object_id);
        MOQ_PUB_TEST_BUMP(moq_pub_test_merge_count);
        if (track->monotonic &&
            (!track->has_begun || cfg->group_id > track->begun_high)) {
            track->has_begun = true;
            track->begun_high = cfg->group_id;
        }
    }
    bool need_ext = (track->op.properties != NULL);

    for (size_t si = 0; si < track->slot_cap; si++) {
        pub_sub_slot_t *slot = &track->slots[si];
        if (!slot_op_pending(track, slot)) continue;
        if (slot->streaming) {              /* this slot's begin already done */
            slot->done_op = track->cur_op;
            continue;
        }

        if (slot->sg_open && slot->cur_group != cfg->group_id) {
            moq_result_t rc = moq_session_close_subgroup(pub->session,
                slot->sg, now_us);
            if (rc == MOQ_ERR_WOULD_BLOCK) return MOQ_ERR_WOULD_BLOCK;
            if (rc == MOQ_ERR_STALE_HANDLE) {
                track_clear_slot(pub, slot);
                continue;
            }
            if (rc < 0) return rc;
            slot->sg_open = false;
        }

        if (!slot->sg_open) {
            moq_subgroup_cfg_t sgcfg;
            moq_subgroup_cfg_init(&sgcfg);
            sgcfg.group_id = cfg->group_id;
            sgcfg.subgroup_id = 0;
            sgcfg.publisher_priority = track->priority;
            sgcfg.object_properties = need_ext;
            moq_result_t rc = (slot->kind == PUB_SLOT_PUBLICATION)
                ? moq_session_open_pub_subgroup(pub->session,
                    slot->pub, &sgcfg, now_us, &slot->sg)
                : moq_session_open_subgroup(pub->session,
                    slot->sub, &sgcfg, now_us, &slot->sg);
            if (rc == MOQ_ERR_WOULD_BLOCK) return MOQ_ERR_WOULD_BLOCK;
            if (rc == MOQ_ERR_STALE_HANDLE) {
                track_clear_slot(pub, slot);
                continue;
            }
            if (rc < 0) return rc;
            slot->sg_open = true;
            slot->streams_opened++;
            slot->sg_has_extensions = need_ext;
            slot->cur_group = cfg->group_id;
        }

        moq_result_t rc;
        if (need_ext || slot->sg_has_extensions) {
            moq_begin_object_cfg_t bcfg;
            moq_begin_object_cfg_init(&bcfg);
            bcfg.object_id = cfg->object_id;
            bcfg.payload_length = cfg->payload_length;
            bcfg.properties = track->op.properties;   /* retained snapshot */
            rc = moq_session_begin_object_ex(pub->session,
                slot->sg, &bcfg, now_us);
        } else {
            rc = moq_session_begin_object(pub->session,
                slot->sg, cfg->object_id, cfg->payload_length, now_us);
        }
        if (rc == MOQ_ERR_WOULD_BLOCK) return MOQ_ERR_WOULD_BLOCK;
        if (rc == MOQ_ERR_STALE_HANDLE) {
            track_clear_slot(pub, slot);
            continue;
        }
        if (rc < 0) return rc;

        slot->streaming = true;
        if (slot->active) slot->done_op = track->cur_op;
    }

    /* §10: a higher-group begin closed the prior group's subgroups on
     * every participating destination -- promote a crossed armed group.
     * The bracket itself cannot carry END_OF_GROUP (no field in the begin
     * cfg -- kept that way deliberately), so the armed state for the NEW group
     * is disarmed. */
    if (track->monotonic) {
        if (track->eog_armed && track->op.group_id > track->eog_group) {
            if (!track->has_eog || track->eog_group > track->eog_high) {
                track->has_eog = true;
                track->eog_high = track->eog_group;
            }
        }
        track->eog_armed = false;
        track->eog_group = track->op.group_id;
    }
    track_op_release(track);
    track->streaming_active = true;
    /* The bracket group must be TRACK state: the op was just released and
     * later WRITE_DATA/END_OBJECT ops overwrite its identity. Valid only
     * while streaming_active. */
    track->streaming_group = cfg->group_id;
    return MOQ_OK;
}

moq_result_t moq_pub_begin_object(moq_publisher_t *pub,
                                    moq_pub_track_t *track,
                                    const moq_pub_begin_object_cfg_t *cfg,
                                    uint64_t now_us)
{
    moq_result_t rc = begin_object_impl(pub, track, cfg, now_us);
    /* A successful begin (incl. the no-subscriber OK path) produced a live
     * object: the retained object is no longer the track's largest. WOULD_BLOCK
     * and errors did not, so they must not suppress the retained Largest. */
    if (rc == MOQ_OK && track) track->wrote_object = true;
    return rc;
}

moq_result_t moq_pub_write_data(moq_publisher_t *pub,
                                  moq_pub_track_t *track,
                                  moq_rcbuf_t *chunk,
                                  uint64_t now_us)
{
    if (!pub || !track || !chunk) return MOQ_ERR_INVAL;
    if (track->pub != pub) return MOQ_ERR_INVAL;
    if (pub->closed) return MOQ_ERR_CLOSED;
    if (track->ended) return MOQ_ERR_WRONG_STATE;  /* terminal: no writes */
    /* Misuse (write_data without begin_object) is a TRACK-level check now:
     * per-slot streaming only marks which slots participate (a subscriber
     * that joined mid-object is skipped until the next begin). */
    if (!track->streaming_active) return MOQ_ERR_WRONG_STATE;

    {
        moq_result_t rr = track_run_retires(pub, track, now_us);
        if (rr < 0) return rr;
    }
    /* Chunks have no wire identity; the retained snapshot IS the pending
     * chunk. A retry must carry the SAME bytes (fresh buffer allowed); a
     * caller advancing to the NEXT chunk while one is pending gets
     * WRONG_STATE -- never a silent MOQ_OK for an unsent chunk. */
    moq_result_t st = track_op_begin(track, PUB_OP_WRITE_DATA,
        0, 0, false, false, MOQ_OBJECT_NORMAL, false, 0, chunk, NULL);
    if (st == MOQ_ERR_WRONG_STATE) return MOQ_ERR_WRONG_STATE;

    for (size_t si = 0; si < track->slot_cap; si++) {
        pub_sub_slot_t *slot = &track->slots[si];
        if (!slot_op_pending(track, slot)) continue;
        if (!slot->streaming) {             /* joined mid-object: next begin */
            slot->done_op = track->cur_op;
            continue;
        }
        moq_result_t rc = moq_session_write_object_data(pub->session,
            slot->sg, track->op.payload, now_us);
        if (rc == MOQ_ERR_WOULD_BLOCK) return MOQ_ERR_WOULD_BLOCK;
        if (rc == MOQ_ERR_STALE_HANDLE) {
            track_clear_slot(pub, slot);
            continue;
        }
        if (rc < 0) return rc;
        if (slot->active) slot->done_op = track->cur_op;
    }

    track_op_release(track);
    return MOQ_OK;
}

moq_result_t moq_pub_end_object(moq_publisher_t *pub,
                                  moq_pub_track_t *track,
                                  uint64_t now_us)
{
    if (!pub || !track) return MOQ_ERR_INVAL;
    if (track->pub != pub) return MOQ_ERR_INVAL;
    if (pub->closed) return MOQ_ERR_CLOSED;
    if (track->ended) return MOQ_ERR_WRONG_STATE;  /* terminal: no writes */
    if (!track->streaming_active) return MOQ_ERR_WRONG_STATE;

    {
        moq_result_t rr = track_run_retires(pub, track, now_us);
        if (rr < 0) return rr;
    }
    moq_result_t st = track_op_begin(track, PUB_OP_END_OBJECT,
        0, 0, false, false, MOQ_OBJECT_NORMAL, false, 0, NULL, NULL);
    if (st == MOQ_ERR_WRONG_STATE) return MOQ_ERR_WRONG_STATE;

    for (size_t si = 0; si < track->slot_cap; si++) {
        pub_sub_slot_t *slot = &track->slots[si];
        if (!slot_op_pending(track, slot)) continue;
        if (!slot->streaming) {             /* joined mid-object */
            slot->done_op = track->cur_op;
            continue;
        }
        moq_result_t rc = moq_session_end_object(pub->session,
            slot->sg, now_us);
        if (rc == MOQ_ERR_WOULD_BLOCK) return MOQ_ERR_WOULD_BLOCK;
        if (rc == MOQ_ERR_STALE_HANDLE) {
            track_clear_slot(pub, slot);
            continue;
        }
        if (rc < 0) return rc;
        slot->streaming = false;
        if (slot->active) slot->done_op = track->cur_op;
    }

    track_op_release(track);
    track->streaming_active = false;
    return MOQ_OK;
}

moq_result_t moq_pub_end_group(moq_publisher_t *pub,
                                 moq_pub_track_t *track,
                                 uint64_t now_us)
{
    if (!pub || !track) return MOQ_ERR_INVAL;
    if (track->pub != pub) return MOQ_ERR_INVAL;
    if (pub->closed) return MOQ_ERR_CLOSED;
    if (track->streaming_active) return MOQ_ERR_WRONG_STATE;  /* mid object */

    {
        moq_result_t rr = track_run_retires(pub, track, now_us);
        if (rr < 0) return rr;
    }
    moq_result_t st = track_op_begin(track, PUB_OP_END_GROUP,
        0, 0, false, false, MOQ_OBJECT_NORMAL, false, 0, NULL, NULL);
    if (st == MOQ_ERR_WRONG_STATE) return MOQ_ERR_WRONG_STATE;

    for (size_t si = 0; si < track->slot_cap; si++) {
        pub_sub_slot_t *slot = &track->slots[si];
        if (!slot_op_pending(track, slot)) continue;
        if (!slot->sg_open) {               /* nothing open on this slot */
            slot->done_op = track->cur_op;
            continue;
        }
        moq_result_t rc = moq_session_close_subgroup(pub->session,
            slot->sg, now_us);
        if (rc == MOQ_ERR_WOULD_BLOCK) return MOQ_ERR_WOULD_BLOCK;
        if (rc == MOQ_ERR_STALE_HANDLE) {
            track_clear_slot(pub, slot);
            continue;
        }
        if (rc < 0) return rc;
        slot->sg_open = false;
        if (slot->active) slot->done_op = track->cur_op;
    }

    /* §10: every open subgroup just closed with a clean FIN -- promote
     * the armed END_OF_GROUP evidence, if any. */
    if (track->monotonic && track->eog_armed) {
        if (!track->has_eog || track->eog_group > track->eog_high) {
            track->has_eog = true;
            track->eog_high = track->eog_group;
        }
        track->eog_armed = false;
    }
    track_op_release(track);
    return MOQ_OK;
}

moq_result_t moq_pub_declare_groups_complete_through(moq_publisher_t *pub,
                                                      moq_pub_track_t *track,
                                                      uint64_t group,
                                                      uint64_t now_us)
{
    if (!pub || !track) return MOQ_ERR_INVAL;
    if (track->pub != pub) return MOQ_ERR_INVAL;
    if (pub->closed) return MOQ_ERR_CLOSED;

    if (!(track->has_declared && group <= track->declared_through)) {
        /* Raising declaration. Preflight BEFORE any mutation: never cover
         * an admitted-but-unsent object (a pending location op) or an open
         * streaming bracket -- finish or abandon it, then redeclare. A
         * lower-or-equal redeclaration skips both the merge and this check
         * (those groups were sealed when the watermark committed) but
         * still runs the sweep below: redeclaration is the documented
         * resume path after MOQ_ERR_WOULD_BLOCK. */
        if ((track->op.kind == PUB_OP_WRITE_OBJECT ||
             track->op.kind == PUB_OP_BEGIN_OBJECT) &&
            track->op.group_id <= group)
            return MOQ_ERR_WRONG_STATE;
        /* A blocked end_track retains its checked terminal Location in the
         * op; covering it would make the retry publish the terminal status
         * inside the sealed range. */
        if (track->op.kind == PUB_OP_END_TRACK && track->op.has_status &&
            track->op.group_id <= group)
            return MOQ_ERR_WRONG_STATE;
        if (track->streaming_active && track->streaming_group <= group)
            return MOQ_ERR_WRONG_STATE;
        track->has_declared = true;
        track->declared_through = group;   /* monotone: raising path only */
    }

    /* The watermark (if any was merged) is COMMITTED from here on: a
     * WOULD_BLOCK below reports blocked completion work, never a rollback.
     * Required RESETs drain first -- completion must never convert an
     * armed RESET into a clean FIN. */
    {
        moq_result_t rr = track_run_retires(pub, track, now_us);
        if (rr < 0) return rr;
    }
    return track_run_completions(pub, track, now_us);
}

moq_result_t moq_pub_reset_group(moq_publisher_t *pub,
                                   moq_pub_track_t *track,
                                   uint64_t error_code, uint64_t now_us)
{
    if (!pub || !track) return MOQ_ERR_INVAL;
    if (track->pub != pub) return MOQ_ERR_INVAL;
    if (pub->closed) return MOQ_ERR_CLOSED;

    /* Abandon the open subgroup on every active slot (same loop as
     * remove_track's teardown). WOULD_BLOCK is retryable: a slot already
     * reset has sg_open cleared, so a re-drive skips it. */
    for (size_t si = 0; si < track->slot_cap; si++) {
        pub_sub_slot_t *sl = &track->slots[si];
        if (!sl->active || !sl->sg_open) continue;
        moq_result_t rc = moq_session_reset_subgroup(pub->session,
            sl->sg, error_code, now_us);
        if (rc == MOQ_ERR_WOULD_BLOCK) return MOQ_ERR_WOULD_BLOCK;
        if (rc == MOQ_ERR_STALE_HANDLE) { track_clear_slot(pub, sl); continue; }
        if (rc < 0) return rc;
        sl->sg_open = false;
        sl->streaming = false;   /* any in-flight streaming object is gone */
        sl->needs_reset = false;
    }
    /* The documented ABANDON path for a pending fan-out operation: release
     * its retained snapshot and clear the streaming bracket. The armed
     * END_OF_GROUP candidate dies with it: the resets above tore down the
     * armed group's EOG-advertised subgroups, and both drafts void the EOG
     * inference for reset streams -- promotion now requires a REPLACEMENT
     * completed EOG write. (Destination-local Forward/window retires do
     * NOT clear it: promotion only ever rides clean FINs.) */
    track->eog_armed = false;
    track_op_release(track);
    track->streaming_active = false;
    return MOQ_OK;
}

/*
 * Canonical facade event dispatcher: the single state machine behind BOTH the
 * public manual-forwarding API (moq_pub_handle_event) and the internal pump
 * (moq_pub_tick). Every event a facade cares about is handled here exactly
 * once, so the two entry points can never diverge.
 *
 * Result contract (see the header on moq_pub_handle_event):
 *   - a facade-EXCLUSIVE event that matched a track/slot -> CONSUMED
 *   - an unmatched or unrecognized event                 -> IGNORED
 *   - a shared session BROADCAST (SESSION_CLOSED / GOAWAY) -> the facade
 *     observes it locally (updates state, fires callbacks) but leaves the
 *     result IGNORED so a multiplexing caller keeps forwarding it to peers.
 *
 * WOULD_BLOCK pairs with the result to say what happened:
 *   - WOULD_BLOCK + CONSUMED: this event WAS taken and staged owned work (a
 *     pending accept/reject, a deferred retained FETCH, or an armed subgroup
 *     retirement) that could not complete against a full action queue. Recover
 *     by draining session actions and calling moq_pub_flush -- NOT by replaying
 *     the event; replaying would double the state transition.
 *   - WOULD_BLOCK + IGNORED: the event was NOT taken because the single pending
 *     slot is still occupied by earlier staged work. Under the serialized
 *     contract this cannot arise (the prior event is flushed to MOQ_OK before
 *     the next poll); it only signals a serialization violation by the caller.
 * See the moq_pub_handle_event header for the full serialized drain+flush
 * recovery. `*result` is assumed pre-initialized by the caller.
 */
static moq_result_t pub_dispatch_event(moq_publisher_t *pub,
                                       const moq_event_t *event,
                                       uint64_t now_us,
                                       moq_pub_event_result_t *result)
{
    if (!pub || !event || !result) return MOQ_ERR_INVAL;
    *result = MOQ_PUB_EVENT_IGNORED;

    if (event->kind == MOQ_EVENT_SESSION_CLOSED) {
        /* Shared broadcast: fully tear down local state and notify (canonical
         * close helper), but keep the result IGNORED so the event stays
         * forwardable to other facades on a multiplexed session. */
        pub_close_local(pub, event->u.closed.code);
        return MOQ_OK;
    }

    if (event->kind == MOQ_EVENT_GOAWAY) {
        /* Shared broadcast: observe draining locally, stay forwardable. */
        pub->draining = true;
        if (pub->callbacks.on_draining)
            pub->callbacks.on_draining(pub->callbacks.ctx);
        return MOQ_OK;
    }

    /* Namespace lifecycle events. */
    if (event->kind == MOQ_EVENT_NAMESPACE_ACCEPTED) {
        moq_pub_track_t *t = find_track_by_ann(pub,
            event->u.namespace_accepted.ann);
        if (t && t->ns_entry && t->ns_entry->state == PUB_NS_PENDING) {
            t->ns_entry->state = PUB_NS_ACCEPTED;
            *result = MOQ_PUB_EVENT_CONSUMED;
        }
        return MOQ_OK;
    }
    if (event->kind == MOQ_EVENT_NAMESPACE_REJECTED) {
        pub_ns_entry_t *e = find_ns_entry_by_ann(pub,
            event->u.namespace_rejected.ann);
        /* Fire on_namespace_terminal exactly once PER NAMESPACE: only on the
         * FIRST transition into terminal, so a duplicate/stale event, or a
         * second track sharing the advertisement, never re-notifies. */
        if (e && e->state != PUB_NS_TERMINAL) {
            e->state = PUB_NS_TERMINAL;
            *result = MOQ_PUB_EVENT_CONSUMED;
            if (CB_HAS(&pub->callbacks, on_namespace_terminal)) {
                const moq_namespace_rejected_event_t *rj =
                    &event->u.namespace_rejected;
                moq_pub_namespace_terminal_info_t info;
                memset(&info, 0, sizeof(info));
                info.struct_size = (uint32_t)sizeof(info);
                info.kind = MOQ_PUB_NAMESPACE_REJECTED;
                info.namespace_ = (moq_namespace_t){ e->id_parts, e->id_count };
                info.error_code = rj->error_code;
                info.can_retry = rj->can_retry;
                info.retry_after_ms = rj->can_retry ? rj->retry_after_ms : 0;
                info.reason = rj->reason;
                pub->callbacks.on_namespace_terminal(
                    pub->callbacks.ctx, &info);
            }
        } else if (e) {
            *result = MOQ_PUB_EVENT_CONSUMED;   /* matched, already terminal */
        }
        return MOQ_OK;
    }
    if (event->kind == MOQ_EVENT_NAMESPACE_CANCELLED) {
        pub_ns_entry_t *e = find_ns_entry_by_ann(pub,
            event->u.namespace_cancelled.ann);
        if (e && e->state != PUB_NS_TERMINAL) {
            e->state = PUB_NS_TERMINAL;
            *result = MOQ_PUB_EVENT_CONSUMED;
            if (CB_HAS(&pub->callbacks, on_namespace_terminal)) {
                const moq_namespace_cancelled_event_t *cn =
                    &event->u.namespace_cancelled;
                moq_pub_namespace_terminal_info_t info;
                memset(&info, 0, sizeof(info));
                info.struct_size = (uint32_t)sizeof(info);
                info.kind = MOQ_PUB_NAMESPACE_CANCELLED;
                info.namespace_ = (moq_namespace_t){ e->id_parts, e->id_count };
                info.error_code = cn->error_code;
                info.can_retry = false;         /* cancellation is not retryable */
                info.retry_after_ms = 0;
                info.reason = cn->reason;
                pub->callbacks.on_namespace_terminal(
                    pub->callbacks.ctx, &info);
            }
        } else if (e) {
            *result = MOQ_PUB_EVENT_CONSUMED;
        }
        return MOQ_OK;
    }

    /* The peer unsubscribed: the core has freed the subscription entry, so the
     * facade must retire the matching per-track slot. Otherwise
     * moq_pub_active_subscriptions() keeps reporting a subscriber that no longer
     * exists and a later SUBSCRIBE_REQUEST for the same track is rejected as a
     * duplicate. Also drop a not-yet-resolved pending/deferred accept for the
     * same (now-freed) subscription so we never act on a stale handle. */
    if (event->kind == MOQ_EVENT_UNSUBSCRIBED) {
        moq_subscription_t sub = event->u.unsubscribed.sub;
        for (moq_pub_track_t *t = pub->tracks; t; t = t->next) {
            pub_sub_slot_t *sl = track_find_slot_by_sub(t, sub);
            if (!sl) continue;
            track_clear_slot(pub, sl);
            *result = MOQ_PUB_EVENT_CONSUMED;
            if (pub->callbacks.on_subscriber_left)
                pub->callbacks.on_subscriber_left(pub->callbacks.ctx, t);
            break;
        }
        if (pub->pending.active && moq_subscription_eq(pub->pending.sub, sub)) {
            pub->pending.active = false;
            *result = MOQ_PUB_EVENT_CONSUMED;
        }
        if (pub->deferred.active && moq_subscription_eq(pub->deferred.sub, sub)) {
            pub->deferred.active = false;
            *result = MOQ_PUB_EVENT_CONSUMED;
        }
        return MOQ_OK;
    }

    /* Answer an explicit FETCH for the track's retained GROUP (objects 0..N)
     * from the retained cache. Scope is deliberately narrow: a JOINING fetch
     * (matched by its joining subscription) OR a bounded STANDALONE fetch
     * (matched by explicit namespace/name, e.g. the relay/origin pull shape a
     * relay like moqx emits) whose range covers the whole retained group. This
     * is origin-local and not a general object store -- the range guard below
     * keeps standalone serve bounded to exactly the retained group. */
    if (event->kind == MOQ_EVENT_FETCH_REQUEST) {
        if (pub->pending_fetch.active)
            return MOQ_ERR_WOULD_BLOCK;   /* one retained FETCH at a time */
        const moq_fetch_request_event_t *fr = &event->u.fetch_request;
        *result = MOQ_PUB_EVENT_CONSUMED;

        moq_request_error_t reject_code = 0;

        /* Resolve the retained track: by the joining subscription for a Joining
         * FETCH, or by explicit namespace/name for a standalone FETCH. */
        const bool standalone = !moq_subscription_is_valid(fr->joining_sub);
        moq_pub_track_t *track =
            standalone
                ? find_track(pub, &fr->track_namespace, fr->track_name)
                : find_track_by_sub(pub, fr->joining_sub);

        /* Standalone-FETCH authorization. A standalone FETCH resolves a track by
         * explicit namespace/name and would otherwise serve its retained objects
         * to anyone who knows the name -- bypassing the publisher's accept policy
         * and leaking retained catalog/media under REJECT_ALL or a rejecting
         * callback. Serve only when the publisher accepts unconditionally
         * (ACCEPT_ALL) or the track already has an accepted subscription on this
         * session (track_has_subscriber). The latter is the legitimate relay
         * shape: the app accepted a SUBSCRIBE via callback, then the relay pulls
         * the retained catalog with a standalone FETCH. Otherwise reject
         * UNAUTHORIZED -- regardless of whether the track exists or has a
         * retained group, so a protected track's existence is not leaked via
         * DOES_NOT_EXIST. A Joining FETCH is unaffected: its joining subscription
         * is itself proof of an accepted subscription. This check consults only
         * publisher state; it never invokes the subscribe callback. */
        if (standalone &&
            pub->cfg.accept_mode != MOQ_PUB_ACCEPT_ALL &&
            !(track && track_has_subscriber(track))) {
            reject_code = MOQ_REQUEST_ERROR_UNAUTHORIZED;
        } else if (!track || track->ended || !pub_track_has_retained(track)) {
            /* Nothing to serve: the track may be unknown, ended, or exist with
             * no retained group (never set, or cleared after its Largest was
             * advertised) -- this cache does not track a Largest after a clear,
             * so it cannot bound an empty-range FETCH response. Reject
             * DOES_NOT_EXIST rather than synthesize an empty fetch. */
            reject_code = MOQ_REQUEST_ERROR_DOES_NOT_EXIST;
        } else if (pub_track_retained_group(track) > MOQ_QUIC_VARINT_MAX ||
                   pub_track_retained_last_object_id(track) >= MOQ_QUIC_VARINT_MAX) {
            /* FETCH_OK End Location is {group, last_object + 1}; refuse
             * rather than emit an unencodable Location (matches the
             * advertise-side retained_can_advertise_largest check). */
            reject_code = MOQ_REQUEST_ERROR_INTERNAL_ERROR;
        } else if (!loc_in_fetch_range(pub_track_retained_group(track),
                                       pub_track_retained_first_object_id(track),
                                       fr->start_group, fr->start_object,
                                       fr->end_group, fr->end_object) ||
                   !loc_in_fetch_range(pub_track_retained_group(track),
                                       pub_track_retained_last_object_id(track),
                                       fr->start_group, fr->start_object,
                                       fr->end_group, fr->end_object)) {
            /* LOCKED policy: the range MUST cover the WHOLE retained group,
             * object 0 (the independent) through the last delta. A range that
             * omits object 0 cannot reconstruct the catalog from deltas
             * alone; a range that omits later deltas would have us serve
             * objects outside it. The whole group is contiguous in one
             * group, so checking both endpoints are in range suffices. Both a
             * Joining-FETCH(offset 0) and a standalone FETCH covering 0..last
             * satisfy it. */
            reject_code = MOQ_REQUEST_ERROR_NOT_SUPPORTED;
        }

        /* Stage. For the serve case, SNAPSHOT the whole retained group (hold
         * refs) so the response is independent of a later clear/end/remove or a
         * set_retained_group that replaces the track's vector. */
        pending_fetch_clear(pub);
        pub->pending_fetch.active = true;
        pub->pending_fetch.fetch = fr->fetch;
        pub->pending_fetch.reject_code = reject_code;
        if (reject_code == 0) {
            pub_retained_obj_t *snap = pub_retained_snapshot(&pub->alloc,
                track->retained, track->retained_count);
            if (!snap) {
                /* NOMEM staging the snapshot: reject rather than half-serve. */
                pub->pending_fetch.reject_code = MOQ_REQUEST_ERROR_INTERNAL_ERROR;
            } else {
                pub->pending_fetch.objs = snap;
                pub->pending_fetch.obj_count = track->retained_count;
                pub->pending_fetch.next_idx = 0;
                pub->pending_fetch.group_id = pub_track_retained_group(track);
                pub->pending_fetch.end_object =
                    pub_track_retained_last_object_id(track) + 1;
                pub->pending_fetch.priority = track->priority;
            }
        }
        serve_retained_fetch(pub, now_us);
        if (pub->pending_fetch.active)
            return MOQ_ERR_WOULD_BLOCK;   /* deferred; the tick drains it */
        return MOQ_OK;
    }

    if (event->kind == MOQ_EVENT_SUBSCRIBE_UPDATED) {
        moq_pub_track_t *t = find_track_by_sub(pub,
            event->u.subscribe_updated.sub);
        if (!t) return MOQ_OK;
        *result = MOQ_PUB_EVENT_CONSUMED;
        /* Track the subscription's Forward state on its slot. A drop to
         * Forward 0 with wire state open arms a subgroup retire (RESET_STREAM,
         * never FIN), which is then DRIVEN below -- inline if the queue has
         * room, else signalled as WOULD_BLOCK so the caller flushes. Forward 0
         * is a PAUSE: the slot stays installed. */
        bool armed_retire = false;
        {
            pub_sub_slot_t *sl = track_find_slot_by_sub(t,
                event->u.subscribe_updated.sub);
            if (sl && event->u.subscribe_updated.has_forward) {
                bool fwd = event->u.subscribe_updated.forward;
                if (!fwd && sl->forward) slot_forward_drop(t, sl);
                sl->forward = fwd;
            }
            /* A filter on the update replaced the resolved window (the session
             * re-resolved at acknowledge time): re-copy it. An update WITHOUT
             * a filter leaves the existing window untouched. A narrowing that
             * excludes the open bracket's whole group takes the same lazy
             * RESET path as a Forward drop (never FIN, slot stays active,
             * delivery resumes on a later admitted object). */
            if (sl && event->u.subscribe_updated.has_filter) {
                slot_install_sub_window(pub, sl);
                slot_window_cut_check(t, sl);
            }
            if (sl) armed_retire = sl->needs_reset;
        }
        if (CB_HAS(&pub->callbacks, on_subscriber_updated) &&
            pub->callbacks.on_subscriber_updated) {
            moq_pub_subscribe_update_info_t info;
            memset(&info, 0, sizeof(info));
            info.has_subscriber_priority =
                event->u.subscribe_updated.has_subscriber_priority;
            info.subscriber_priority =
                event->u.subscribe_updated.subscriber_priority;
            info.has_forward = event->u.subscribe_updated.has_forward;
            info.forward = event->u.subscribe_updated.forward;
            info.has_delivery_timeout =
                event->u.subscribe_updated.has_delivery_timeout;
            info.delivery_timeout_us =
                event->u.subscribe_updated.delivery_timeout_us;
            pub->callbacks.on_subscriber_updated(pub->callbacks.ctx, t, &info);
        }
        /* A Forward-0 drop that armed a subgroup RESET is staged work: drive
         * it now (RESET_STREAM, never FIN). If the action queue is full the
         * event is still CONSUMED (state committed) but returns WOULD_BLOCK so
         * the caller drains + moq_pub_flush completes the RESET -- the manual
         * path can't otherwise know to flush. */
        if (armed_retire) {
            moq_result_t rr = track_run_retires(pub, t, now_us);
            if (rr == MOQ_ERR_WOULD_BLOCK) return MOQ_ERR_WOULD_BLOCK;
            if (rr < 0 && rr != MOQ_ERR_STALE_HANDLE) return rr;
        }
        /* §10: a narrowing may have brought this slot's finite end
         * under the declared watermark -- drive completion now (state
         * committed; a blocked sweep surfaces WOULD_BLOCK so the manual
         * caller drains + flushes, never replays the event). */
        {
            moq_result_t cr = track_run_completions(pub, t, now_us);
            if (cr == MOQ_ERR_WOULD_BLOCK) return MOQ_ERR_WOULD_BLOCK;
            if (cr < 0 && cr != MOQ_ERR_STALE_HANDLE) return cr;
        }
        return MOQ_OK;
    }

    if (event->kind == MOQ_EVENT_PUBLISH_OK) {
        moq_pub_track_t *t = find_track_by_pub(pub, event->u.publish_ok.pub);
        if (!t) return MOQ_OK;
        *result = MOQ_PUB_EVENT_CONSUMED;
        t->publish_ok = true;
        t->publish_forward = event->u.publish_ok.send_allowed;
        if (!track_find_publication_slot(t))
            track_set_publication(pub, t, t->publication);
        if (CB_HAS(&pub->callbacks, on_publish_ok))
            pub->callbacks.on_publish_ok(pub->callbacks.ctx, t,
                t->publish_forward);
        /* §10: the PUBLISH_OK-resolved window may already be covered
         * by the declared watermark -- complete the fresh slot now. */
        {
            moq_result_t cr = track_run_completions(pub, t, now_us);
            if (cr == MOQ_ERR_WOULD_BLOCK) return MOQ_ERR_WOULD_BLOCK;
            if (cr < 0 && cr != MOQ_ERR_STALE_HANDLE) return cr;
        }
        return MOQ_OK;
    }

    if (event->kind == MOQ_EVENT_PUBLISH_UPDATED) {
        moq_pub_track_t *t = find_track_by_pub(pub,
            event->u.publish_updated.pub);
        if (!t) return MOQ_OK;
        *result = MOQ_PUB_EVENT_CONSUMED;
        bool armed_retire = false;
        if (event->u.publish_updated.has_forward) {
            bool fwd = event->u.publish_updated.forward;
            if (!fwd && t->publish_forward) {
                pub_sub_slot_t *sl = track_find_publication_slot(t);
                if (sl) { slot_forward_drop(t, sl); armed_retire = sl->needs_reset; }
            }
            t->publish_forward = fwd;
            if (CB_HAS(&pub->callbacks, on_publish_forward_changed))
                pub->callbacks.on_publish_forward_changed(
                    pub->callbacks.ctx, t, t->publish_forward);
        }
        /* A filter on the update replaced the resolved window: re-copy it and
         * run the same reset-on-cut as the subscription path. No filter on
         * the update = window unchanged. */
        if (event->u.publish_updated.has_filter) {
            pub_sub_slot_t *sl = track_find_publication_slot(t);
            if (sl) {
                slot_install_pub_window(pub, sl);
                slot_window_cut_check(t, sl);
                if (sl->needs_reset) armed_retire = true;
            }
        }
        if (armed_retire) {
            moq_result_t rr = track_run_retires(pub, t, now_us);
            if (rr == MOQ_ERR_WOULD_BLOCK) return MOQ_ERR_WOULD_BLOCK;
            if (rr < 0 && rr != MOQ_ERR_STALE_HANDLE) return rr;
        }
        /* §10: same narrowing-completion drive as SUBSCRIBE_UPDATED. */
        {
            moq_result_t cr = track_run_completions(pub, t, now_us);
            if (cr == MOQ_ERR_WOULD_BLOCK) return MOQ_ERR_WOULD_BLOCK;
            if (cr < 0 && cr != MOQ_ERR_STALE_HANDLE) return cr;
        }
        return MOQ_OK;
    }

    if (event->kind == MOQ_EVENT_PUBLISH_ERROR) {
        moq_pub_track_t *t = find_track_by_pub(pub, event->u.publish_error.pub);
        if (!t) return MOQ_OK;
        *result = MOQ_PUB_EVENT_CONSUMED;
        /* Publication terminated before acceptance: clear so a later
         * moq_pub_publish_track may retry, and drop any data slot. */
        t->publish_requested = false;
        t->publish_ok = false;
        pub_sub_slot_t *sl = track_find_publication_slot(t);
        if (sl) track_clear_slot(pub, sl);
        if (CB_HAS(&pub->callbacks, on_publish_error))
            pub->callbacks.on_publish_error(pub->callbacks.ctx, t,
                event->u.publish_error.error_code);
        return MOQ_OK;
    }

    if (event->kind == MOQ_EVENT_PUBLISH_UNSUBSCRIBED) {
        /* Only UNSUBSCRIBED can match a facade track: find_track_by_pub tracks
         * locally initiated (publisher-role) publications, and the session
         * emits PUBLISH_FINISHED to the SUBSCRIBER role only -- a
         * publisher-role entry ended by the peer surfaces here. */
        moq_pub_track_t *t = find_track_by_pub(pub,
            event->u.publish_unsubscribed.pub);
        if (!t) return MOQ_OK;
        *result = MOQ_PUB_EVENT_CONSUMED;
        /* Return the track to the UN-published state (like PUBLISH_ERROR): a
         * peer-terminated publication must be restartable -- leaving
         * publish_requested set would make a later moq_pub_publish_track an
         * idempotent no-op that puts no PUBLISH on the wire. */
        t->publish_requested = false;
        t->publish_ok = false;
        pub_sub_slot_t *sl = track_find_publication_slot(t);
        if (sl) track_clear_slot(pub, sl);
        if (CB_HAS(&pub->callbacks, on_publish_finished))
            pub->callbacks.on_publish_finished(pub->callbacks.ctx, t);
        return MOQ_OK;
    }

    /* MOQ_EVENT_PUBLISH_FINISHED is intentionally NOT handled: the session
     * emits it to the SUBSCRIBER role only, so it can never match a
     * publisher-role facade track. It falls through to IGNORED. */

    if (event->kind != MOQ_EVENT_SUBSCRIBE_REQUEST)
        return MOQ_OK;

    const moq_subscribe_request_event_t *req = &event->u.subscribe_request;
    moq_pub_track_t *track = find_track(pub, &req->track_namespace,
                                          req->track_name);
    if (!track)
        return MOQ_OK;

    if (pub->pending.active)
        return MOQ_ERR_WOULD_BLOCK;

    *result = MOQ_PUB_EVENT_CONSUMED;

    bool want_accept;
    moq_request_error_t reject_code = 0;

    if (track->ended) {
        /* The track is terminal (END_OF_TRACK already emitted / ended locally).
         * A late subscriber must not join it -- reject deterministically rather
         * than installing a slot that would receive neither media nor a
         * terminal. Checked before the app callback so it is never consulted. */
        want_accept = false;
        reject_code = MOQ_REQUEST_ERROR_DOES_NOT_EXIST;
    } else if (track_has_subscriber(track)) {
        want_accept = false;
        reject_code = MOQ_REQUEST_ERROR_DUPLICATE_SUBSCRIPTION;
    } else if (pub->cfg.accept_mode == MOQ_PUB_REJECT_ALL) {
        want_accept = false;
        reject_code = MOQ_REQUEST_ERROR_UNAUTHORIZED;
    } else if (pub->cfg.accept_mode == MOQ_PUB_CALLBACK) {
        moq_pub_subscribe_info_t info;
        memset(&info, 0, sizeof(info));
        info.track = track;
        info.track_namespace = req->track_namespace;
        info.track_name = req->track_name;
        info.filter = req->filter;
        info.subscriber_priority = req->subscriber_priority;
        info.group_order = req->group_order;
        info.forward = req->forward;
        info.start_group = req->start_group;
        info.start_object = req->start_object;
        info.end_group = req->end_group;
        info.delivery_timeout_us = req->delivery_timeout_us;
        info.tokens = req->tokens;
        info.token_count = req->token_count;
        info.deferred = pub->deferred.active ? NULL : &pub->deferred;
        info.deferred_id = pub->deferred.generation + 1;

        moq_request_error_t cb_error = 0;
        moq_pub_accept_decision_t decision =
            pub->cfg.on_subscribe(pub->cfg.on_subscribe_ctx,
                                   &info, &cb_error);

        if (decision == MOQ_PUB_DECISION_ACCEPT) {
            want_accept = true;
        } else if (decision == MOQ_PUB_DECISION_REJECT) {
            want_accept = false;
            reject_code = cb_error ? cb_error
                                    : MOQ_REQUEST_ERROR_UNAUTHORIZED;
        } else if (decision == MOQ_PUB_DECISION_DEFER) {
            if (pub->deferred.active) {
                want_accept = false;
                reject_code = MOQ_REQUEST_ERROR_INTERNAL_ERROR;
            } else {
                pub->deferred.generation++;
                pub->deferred.active = true;
                pub->deferred.sub = req->sub;
                pub->deferred.track = track;
                pub->deferred.forward = req->forward;
                return MOQ_OK;
            }
        } else {
            want_accept = false;
            reject_code = MOQ_REQUEST_ERROR_INTERNAL_ERROR;
        }
    } else {
        want_accept = true;
    }

    moq_result_t rc;
    if (want_accept) {
        moq_accept_subscribe_cfg_t acc;
        pub_init_accept_cfg(track, &acc);
        rc = moq_session_accept_subscribe(pub->session, req->sub,
            &acc, now_us);
    } else {
        moq_reject_subscribe_cfg_t rej;
        moq_reject_subscribe_cfg_init(&rej);
        rej.error_code = reject_code;
        rc = moq_session_reject_subscribe(pub->session, req->sub,
            &rej, now_us);
    }

    if (rc == MOQ_ERR_WOULD_BLOCK) {
        pub->pending.active = true;
        pub->pending.sub = req->sub;
        pub->pending.track = want_accept ? track : NULL;
        pub->pending.accept = want_accept;
        pub->pending.reject_code = reject_code;
        pub->pending.forward = req->forward;
        return MOQ_ERR_WOULD_BLOCK;
    }
    if (rc < 0) {
        *result = MOQ_PUB_EVENT_ERROR;
        return rc;
    }

    if (want_accept) {
        track_set_subscriber(pub, track, req->sub, req->forward);
        /* Immediate accept: fire the joined callback here, exactly once. The
         * WOULD_BLOCK / deferred paths returned above without accepting; they
         * fire it later via flush_pending when the accept completes, so the
         * callback never doubles. */
        if (track_has_subscriber(track) && pub->callbacks.on_subscriber_joined)
            pub->callbacks.on_subscriber_joined(pub->callbacks.ctx, track);
        /* §10: the fresh slot's finite end may already be covered by
         * the declared watermark. The accept is committed (event stays
         * CONSUMED); a blocked sweep surfaces WOULD_BLOCK so the manual
         * caller drains + flushes -- never replays the event. */
        {
            moq_result_t cr = track_run_completions(pub, track, now_us);
            if (cr == MOQ_ERR_WOULD_BLOCK) return MOQ_ERR_WOULD_BLOCK;
            if (cr < 0 && cr != MOQ_ERR_STALE_HANDLE) return cr;
        }
    }
    return MOQ_OK;
}

/* Public manual event-forwarding API: a thin wrapper over the canonical
 * dispatcher (parity with the internal pump is structural, not maintained by
 * hand). */
moq_result_t moq_pub_handle_event(moq_publisher_t *pub,
                                    const moq_event_t *event,
                                    uint64_t now_us,
                                    moq_pub_event_result_t *result)
{
    if (!pub || !event || !result) return MOQ_ERR_INVAL;
    *result = MOQ_PUB_EVENT_IGNORED;
    return pub_dispatch_event(pub, event, now_us, result);
}

moq_result_t moq_pub_resolve_deferred(moq_publisher_t *pub,
                                        moq_pub_deferred_t *deferred,
                                        uint64_t deferred_id,
                                        bool accept,
                                        moq_request_error_t error_code,
                                        uint64_t now_us)
{
    if (!pub || !deferred) return MOQ_ERR_INVAL;
    if (deferred != &pub->deferred) return MOQ_ERR_INVAL;
    if (!deferred->active || deferred->generation != deferred_id)
        return MOQ_ERR_STALE_HANDLE;

    /* The facade has a single pending accept/reject slot. If an earlier
     * backpressured accept/reject still occupies it, resolving now would have to
     * overwrite pub->pending -- silently dropping that earlier request and
     * orphaning its subscription (stuck in PENDING_PUBLISHER with no facade state
     * left to accept/reject it). Refuse: keep the deferred request active and
     * return WOULD_BLOCK so the caller flushes the pending slot and retries. */
    if (pub->pending.active)
        return MOQ_ERR_WOULD_BLOCK;

    /* The track was ended after the defer was issued: the late subscriber must
     * not join a terminal track. Override the accept into a reject. */
    if (accept && deferred->track && deferred->track->ended) {
        accept = false;
        error_code = MOQ_REQUEST_ERROR_DOES_NOT_EXIST;
    }

    moq_result_t rc;
    if (accept) {
        moq_accept_subscribe_cfg_t acc;
        pub_init_accept_cfg(deferred->track, &acc);
        rc = moq_session_accept_subscribe(pub->session, deferred->sub,
            &acc, now_us);
    } else {
        moq_reject_subscribe_cfg_t rej;
        moq_reject_subscribe_cfg_init(&rej);
        rej.error_code = error_code;
        rc = moq_session_reject_subscribe(pub->session, deferred->sub,
            &rej, now_us);
    }

    if (rc == MOQ_ERR_WOULD_BLOCK) {
        pub->pending.active = true;
        pub->pending.sub = deferred->sub;
        pub->pending.track = accept ? deferred->track : NULL;
        pub->pending.accept = accept;
        pub->pending.reject_code = error_code;
        pub->pending.forward = deferred->forward;
        deferred->active = false;
        return MOQ_ERR_WOULD_BLOCK;
    }
    if (rc < 0) {
        deferred->active = false;
        return rc;
    }

    if (accept && deferred->track) {
        track_set_subscriber(pub, deferred->track, deferred->sub,
                             deferred->forward);
        if (pub->callbacks.on_subscriber_joined)
            pub->callbacks.on_subscriber_joined(pub->callbacks.ctx,
                deferred->track);
        /* §10: complete a fresh covered slot; the acceptance is
         * committed either way -- WOULD_BLOCK here means "flush to finish
         * the completion", never "retry the resolution". */
        deferred->active = false;
        {
            moq_result_t cr = track_run_completions(pub, deferred->track,
                                                    now_us);
            if (cr == MOQ_ERR_WOULD_BLOCK) return MOQ_ERR_WOULD_BLOCK;
            if (cr < 0 && cr != MOQ_ERR_STALE_HANDLE) return cr;
        }
        return MOQ_OK;
    }
    deferred->active = false;
    return MOQ_OK;
}

moq_result_t moq_pub_flush(moq_publisher_t *pub, uint64_t now_us) {
    if (!pub) return MOQ_ERR_INVAL;
    /* All staged manual-mode work runs through the one shared progression
     * helper (also used by moq_pub_tick), so the two never drift. */
    return pub_progress_staged(pub, now_us);
}

size_t moq_pub_active_subscriptions(const moq_publisher_t *pub,
                                      const moq_pub_track_t *track)
{
    if (!pub || !track) return 0;
    if (track->pub != pub) return 0;
    return track_active_count(track);
}

bool moq_pub_has_subscriber(const moq_publisher_t *pub,
                              const moq_pub_track_t *track)
{
    return moq_pub_active_subscriptions(pub, track) > 0;
}

bool moq_pub_namespace_accepted(const moq_publisher_t *pub,
                                  const moq_pub_track_t *track)
{
    if (!pub || !track) return false;
    if (track->pub != pub) return false;
    return track->ns_entry != NULL &&
           track->ns_entry->state == PUB_NS_ACCEPTED;
}

void moq_pub_publish_cfg_init(moq_pub_publish_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = sizeof(*cfg);
    cfg->has_forward = true;
    cfg->forward = true;
}

moq_result_t moq_pub_publish_track(moq_publisher_t *pub,
                                   moq_pub_track_t *track,
                                   const moq_pub_publish_cfg_t *cfg,
                                   uint64_t now_us)
{
    if (!pub || !track || !cfg) return MOQ_ERR_INVAL;
    if (track->pub != pub) return MOQ_ERR_INVAL;
    if (cfg->struct_size < sizeof(moq_pub_publish_cfg_t)) return MOQ_ERR_INVAL;
    if (pub->closed) return MOQ_ERR_CLOSED;
    if (track->ended) return MOQ_ERR_WRONG_STATE;
    if (track->publish_requested) return MOQ_OK;   /* idempotent */

    moq_publish_cfg_t pcfg;
    /* Sets the auth-token tail past the frozen v0 prefix, so the sized init is
     * required (the pointer init would leave it reader-ignored). */
    moq_publish_cfg_init_sized(&pcfg, sizeof(pcfg));
    pcfg.track_namespace = (moq_namespace_t){ track->ns_parts, track->ns_count };
    pcfg.track_name = (moq_bytes_t){ track->name_buf, track->name_len };
    pcfg.has_track_alias = cfg->has_track_alias;
    pcfg.track_alias = cfg->track_alias;
    pcfg.has_forward = cfg->has_forward;
    pcfg.forward = cfg->forward;
    pcfg.track_properties = cfg->track_properties;
    pcfg.auth_tokens = cfg->auth_tokens;
    pcfg.auth_token_count = cfg->auth_token_count;

    moq_publication_t handle;
    moq_result_t rc = moq_session_publish(pub->session, &pcfg, now_us, &handle);
    if (rc < 0) return rc;   /* REQUEST_BLOCKED / WOULD_BLOCK: nothing bound, retry */

    track->publication = handle;
    track->publish_requested = true;
    track->publish_forward = cfg->has_forward ? cfg->forward : true;
    return MOQ_OK;
}

moq_result_t moq_pub_unpublish_track(moq_publisher_t *pub,
                                     moq_pub_track_t *track,
                                     uint64_t now_us)
{
    if (!pub || !track) return MOQ_ERR_INVAL;
    if (track->pub != pub) return MOQ_ERR_INVAL;
    if (pub->closed) return MOQ_ERR_CLOSED;
    if (!track->publish_requested) return MOQ_OK;   /* not published: no-op */

    /* Clean-end contract: an admitted-but-unsent object for this publication
     * must be completed (retry the write) or abandoned explicitly
     * (moq_pub_reset_group / moq_pub_remove_track) first -- unpublish never
     * FINs truncated work. Pure check, no mutation. */
    pub_sub_slot_t *psl = track_find_publication_slot(track);
    if (psl &&
        (track->op.kind == PUB_OP_WRITE_OBJECT ||
         track->op.kind == PUB_OP_BEGIN_OBJECT) &&
        psl->target_op == track->cur_op && psl->done_op != track->cur_op)
        return MOQ_ERR_WRONG_STATE;

    /* Drive any armed retire BEFORE the mid-stream guard: a Forward/filter
     * cut during a streaming object leaves streaming set WITH needs_reset
     * armed, and that bracket must progress to its RESET here -- only a
     * CLEAN mid-stream object (nothing armed) refuses below. The sweep
     * clears streaming/sg_open on success and may clear a stale slot, so
     * re-resolve psl. */
    {
        moq_result_t rr = track_run_retires(pub, track, now_us);
        if (rr < 0) return rr;
        psl = track_find_publication_slot(track);
    }
    if (psl && psl->streaming) return MOQ_ERR_WRONG_STATE;

    /* Close the data subgroup cleanly before PUBLISH_DONE (finish needs no open
     * stream). Retryable: WOULD_BLOCK at any step lets the caller retry. */
    if (psl && psl->sg_open) {
        moq_result_t rc = moq_session_close_subgroup(pub->session,
            psl->sg, now_us);
        if (rc == MOQ_ERR_WOULD_BLOCK) return MOQ_ERR_WOULD_BLOCK;
        if (rc != MOQ_OK && rc != MOQ_ERR_STALE_HANDLE) return rc;
        psl->sg_open = false;
    }

    moq_finish_publish_cfg_t fcfg;
    moq_finish_publish_cfg_init(&fcfg);
    /* Exact stream count: the facade opened every stream for this
     * publication; a zero would let the peer finalize before in-flight
     * streams arrive. */
    fcfg.stream_count = psl ? psl->streams_opened : 0;
    moq_result_t rc = moq_session_finish_publish(pub->session,
        track->publication, &fcfg, now_us);
    if (rc == MOQ_ERR_WOULD_BLOCK) return MOQ_ERR_WOULD_BLOCK;
    if (rc != MOQ_OK && rc != MOQ_ERR_STALE_HANDLE) return rc;

    track->publish_requested = false;
    track->publish_ok = false;
    if (psl) track_clear_slot(pub, psl);
    return MOQ_OK;
}

bool moq_pub_track_is_published(const moq_publisher_t *pub,
                                const moq_pub_track_t *track)
{
    if (!pub || !track) return false;
    if (track->pub != pub) return false;
    return track->publish_ok;
}

bool moq_pub_track_forward(const moq_publisher_t *pub,
                           const moq_pub_track_t *track)
{
    if (!pub || !track) return false;
    if (track->pub != pub) return false;
    return track->publish_ok && track->publish_forward;
}

bool moq_pub_is_draining(const moq_publisher_t *pub)
{
    if (!pub) return false;
    return pub->draining;
}


/* Absorb a session-close that surfaced as MOQ_ERR_CLOSED from an inner call:
 * poll for the SESSION_CLOSED event (if still queued) for its code, then run
 * the one canonical local-close path. */
static void drain_closed_event(moq_publisher_t *pub)
{
    if (pub->closed) return;
    uint64_t code = 0;
    bool saw_close = false;
    moq_event_t ev;
    while (moq_session_poll_events(pub->session, &ev, 1) == 1) {
        if (ev.kind == MOQ_EVENT_SESSION_CLOSED) {
            code = ev.u.closed.code;   /* code 0 is a valid close code */
            saw_close = true;
        }
        moq_event_cleanup(&ev);
        if (saw_close) break;
    }
    pub_close_local(pub, code);
}

moq_result_t moq_pub_tick(moq_publisher_t *pub, uint64_t now_us)
{
    if (!pub) return MOQ_ERR_INVAL;

    /* Progress previously-staged work: a pending accept (-> on_subscriber_
     * joined), a deferred retained FETCH, and retirements armed in earlier
     * ticks. Shared with moq_pub_flush so the two never drift. */
    moq_result_t frc = pub_progress_staged(pub, now_us);
    if (frc == MOQ_ERR_WOULD_BLOCK) return MOQ_ERR_WOULD_BLOCK;
    if (frc < 0) {
        if (frc == MOQ_ERR_CLOSED) drain_closed_event(pub);
        return frc;
    }

    moq_event_t ev;
    while (moq_session_poll_events(pub->session, &ev, 1) == 1) {
        /* One canonical dispatcher for every event -- identical handling to
         * the public moq_pub_handle_event path. A WOULD_BLOCK means the event
         * staged work (pending accept / deferred FETCH / armed retirement);
         * the next tick, via pub_progress_staged, drains it. */
        moq_pub_event_result_t res = MOQ_PUB_EVENT_IGNORED;
        moq_result_t drc = pub_dispatch_event(pub, &ev, now_us, &res);
        if (drc == MOQ_ERR_WOULD_BLOCK) {
            moq_event_cleanup(&ev);
            return MOQ_ERR_WOULD_BLOCK;
        }
        if (drc < 0) {
            moq_event_cleanup(&ev);
            if (drc == MOQ_ERR_CLOSED)
                drain_closed_event(pub);
            return drc;
        }
        moq_event_cleanup(&ev);
    }

    /* Progress work armed by events processed THIS tick (Forward-0
     * retirements etc.). */
    return pub_progress_staged(pub, now_us);
}
