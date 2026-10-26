#include <moq/publisher.h>
#include "fill_identity_internal.h"
#include "migration_internal.h"
#include "../session/publisher_fill_receipt.h"
#include "../spi/core_spi.h"
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

#define PUB_GROUP_RECORDS     16
#define PUB_GROUP_GENERATIONS 8
_Static_assert(PUB_GROUP_GENERATIONS <= 8, "stopped-key mask covers every subgroup generation");

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
    /* The Subgroup ID the object was assigned (its identity, draft-22
     * section 2.3: a replay must never change it). Resolved from the track's
     * group history when the object is installed, and copied with it into
     * every FETCH / fill snapshot, which therefore never reads track state. */
    uint64_t     subgroup_id;
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
    /* The open subgroup's identity is (cur_group, cur_gen): the generation
     * index is its Subgroup ID. sg_last is the last Object written on it;
     * sg_omitted records that an Object assigned to that subgroup and at or
     * after this destination's Start Location was not written on it, so the
     * stream must end with RESET rather than FIN (draft-22 section 11.3.2). */
    uint8_t               cur_gen;
    bool                  sg_omitted;
    uint64_t              sg_last;
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
    /* The destination is gone but the session kept its data streams (a
     * per-request GOAWAY moves the request, not its data): the slot is
     * inactive and keeps only its open subgroup until that subgroup's reset
     * is queued (needs_reset, track_run_retires). Never reused before. */
    bool                  retiring;
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
    /* An end_track terminal in progress on this destination: the open
     * subgroup (sg) is the terminal one (eot_sg), its END_OF_TRACK has been
     * queued (eot_written), its FIN has been queued (eot_done). Each step
     * of open -> status object -> FIN is taken once, so a refusal for room
     * resumes at the next step. A terminal stream a Forward or filter cut
     * reset before its FIN (eot_sg without sg_open or eot_done) is not
     * continued: the destination ends by its terminal control message. */
    bool                  eot_sg;
    bool                  eot_written;
    bool                  eot_done;
    /* Resolved subscription-filter window, COPIED by value from the session's
     * internal accessor (moq_session_{sub,pub}_resolved_window) when the slot
     * is installed and whenever an update carries a filter -- never a retained
     * pointer into a mutable session entry. has_window == false (including
     * after the install-time accessor returned NULL) means unfiltered.
     * Membership is evaluated when an object operation is admitted and the
     * destination snapshot is stamped; a widening installed later never joins
     * an already pending operation or an open streaming bracket. */
    moq_resolved_window_t window;
    /* Record index is only storage: group and the generation bit identify
     * the stopped key. Cleared when that record is sealed or this owner ends. */
    struct {
        uint64_t group;
        uint8_t generations;
    } stopped[PUB_GROUP_RECORDS];
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
    /* Stream ops: the Subgroup ID (generation) assigned at admission, shared
     * by every destination and every retry of the op; first_in_gen when the
     * object is its generation's first Object (FIRST_OBJECT); has_below /
     * below: the largest Object known to be assigned in the generation below
     * this one (exact, ival_max_below), the witness of what a destination opening
     * the subgroup mid-way, or skipping ahead on it, has omitted. */
    uint8_t       subgroup;
    bool          first_in_gen;
    bool          has_below;
    uint64_t      below;
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

/* Group history bounds (local policy, not wire limits). A track keeps an exact
 * record for at most PUB_GROUP_RECORDS groups; a group revisited after its
 * subgroup closed gets a new generation (Subgroup ID), at most
 * PUB_GROUP_GENERATIONS per group (IDs 0..7), and stays recorded for the
 * track's life ("pinned"); at most PUB_GROUP_PINS groups may be pinned, so one
 * record always rolls. A record leaving the rolling set is compacted into one
 * of PUB_SEALED_RUNS runs of sealed groups, which accept nothing further. */
#define PUB_GROUP_PINS        15
#define PUB_SEALED_RUNS       16
/* Exact assignment membership: at most PUB_GROUP_INTERVALS owner-tagged
 * closed intervals per group record, shared by its stream generations and
 * its datagrams. An admission that would need one more interval after merging
 * is refused with MOQ_ERR_NOMEM before any effect. */
#define PUB_GROUP_INTERVALS   16
#define PUB_OWNER_DATAGRAM    0xFFu   /* interval owner: datagram Objects;
                                         otherwise the generation index */

enum {                       /* pub_gen_t.end */
    PUB_GEN_OPEN = 0,
    PUB_GEN_FIN,             /* closed cleanly (group switch / end_group) */
    PUB_GEN_RESET,           /* abandoned by moq_pub_reset_group */
};
enum {                       /* pub_group_rec_t.final_kind */
    PUB_FINAL_NONE = 0,
    PUB_FINAL_LAST,          /* nothing above final_at (END_OF_GROUP header
                                + FIN, or a d16 whole-group FETCH) */
    PUB_FINAL_FROM,          /* nothing at or above final_at (status 0x3) */
    PUB_FINAL_SEALED,        /* moq_pub_end_group: nothing further */
};
enum {                       /* pub_sealed_run_t.flags */
    PUB_SEALED_STREAM   = 1u << 0,
    PUB_SEALED_RETAINED = 1u << 1,
    PUB_SEALED_DATAGRAM = 1u << 2,
};

/* One generation of a group: Objects assigned to Subgroup ID == its index.
 * first is its first assigned Object; last the highest assigned. */
typedef struct {
    uint64_t first, last;
    uint8_t  end;
    bool     eog_armed;      /* a stream op of this generation carried the
                                END_OF_GROUP header bit */
    bool     fin_seen;       /* a destination ended a stream of this
                                subgroup with a FIN, which fixed its final
                                Object for that receiver (section 12.1) */
} pub_gen_t;

/* Exact identity record of one group. Assignment happens at admission (live
 * stream, datagram, or retained install), never at delivery. */
typedef struct {
    bool      used;
    uint64_t  group;
    uint64_t  seq;           /* latest admission order (rolling eviction) */
    bool      has_max;
    uint64_t  max;           /* highest Object assigned, any mode */
    /* Every assigned Object, exactly: sorted, non-overlapping closed
     * intervals ival[0..nival), each owned by one generation or by the
     * datagram mode (ival_owner). Intervals of different owners never merge,
     * so an Object's (Subgroup ID or datagram) identity is always known. */
    uint8_t   nival;
    uint8_t   ival_owner[PUB_GROUP_INTERVALS];
    struct { uint64_t lo, hi; } ival[PUB_GROUP_INTERVALS];
    bool      retained_origin;   /* generation 0 began from a retained install */
    uint8_t   ngen;
    pub_gen_t gen[PUB_GROUP_GENERATIONS];
    uint8_t   final_kind;
    uint64_t  final_at;
} pub_group_rec_t;

typedef struct {
    uint64_t lo, hi, first;
    uint8_t  flags;
} pub_sealed_run_t;

struct moq_pub_track {
    moq_pub_track_t    *next;
    moq_publisher_t    *pub;
    /* Where the track's publication went when a REDIRECT or a per-request
     * GOAWAY ended it; cleared when the track is published again. */
    facade_migration_t  migration;

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
     * before any mutation; on drafts with SUBSCRIPTION_ENDED (0x3) the
     * completion sweep terminates every finite window it covers with that
     * status. streaming_group is
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
    /* Group history: every Object the track assigns keeps its (group,
     * Subgroup ID, delivery mode) for the track's life (draft-22 sections 2.1
     * and 7.1). Exact records for the rolling and pinned groups, sealed runs
     * for compacted ones. See hist_* below. */
    pub_group_rec_t     recs[PUB_GROUP_RECORDS];
    uint64_t            rec_seq;
    pub_sealed_run_t    sealed[PUB_SEALED_RUNS];
    size_t              sealed_count;
    /* The current logical stream position: the group and generation of the
     * last admitted stream op (track-wide, whatever the destinations). */
    bool                has_cur;
    uint64_t            cur_group;
    uint8_t             cur_gen;
    /* An END_OF_TRACK status admitted at (eot_group, eot_object): nothing at
     * or beyond it is admitted. */
    bool                has_eot;
    uint64_t            eot_group, eot_object;
    /* The highest Object assigned in the retained group when its record was
     * compacted into the sealed runs (a sealed group takes nothing more, so
     * the value is final). Valid while has_sealed_retained_max and
     * sealed_retained_group is the retained group. */
    bool                has_sealed_retained_max;
    uint64_t            sealed_retained_group, sealed_retained_max;

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

    /* The Track Properties every answer for this track carries (an owned
     * copy, DYNAMIC_GROUPS added when declared; NULL when none), the size of
     * that allocation, and whether they advertise DYNAMIC_GROUPS. */
    uint8_t            *props;
    size_t              props_len;
    size_t              props_cap;
    bool                dynamic_groups;
};

typedef struct pub_pending_fill pub_pending_fill_t;

typedef struct {
    bool                active;
    moq_subscription_t  sub;
    moq_pub_track_t    *track;
    bool                accept;
    moq_request_error_t reject_code;
    bool                forward;   /* the request's Forward state, for the slot */
    bool                fill_requested;
    pub_pending_fill_t *fill_job;
    bool                update;
    moq_publication_t   publication;
    uint64_t            update_id;
    bool                has_filter;
    moq_pub_subscribe_update_info_t update_info;
} pub_pending_t;

/* One scalar owner per parent. Legacy cumulative UPDATEs replace its snapshot
 * when their FIFO event arrives; D22 permits one unanswered UPDATE. No borrowed
 * event data or snapshot references live outside a fill job. */
typedef struct {
    uint64_t parent, update_id, reject_code, delivery_timeout_us;
    moq_pub_track_t *track;
    pub_pending_fill_t *fill_job;
    uint32_t prev, next, flags, priority;
} pub_admission_t;

typedef struct {
    pub_admission_t *cells;
    size_t cap;
    uint32_t head, tail;
} pub_admissions_t;

#define PUB_ADMISSION_NONE UINT32_MAX
enum {
    PUB_ADMISSION_ACTIVE = 1u << 0,
    PUB_ADMISSION_UPDATE = 1u << 1,
    PUB_ADMISSION_PUBLICATION = 1u << 2,
    PUB_ADMISSION_ACCEPT = 1u << 3,
    PUB_ADMISSION_DEFERRED = 1u << 4,
    PUB_ADMISSION_FILL = 1u << 5,
    PUB_ADMISSION_FILTER = 1u << 6,
    PUB_ADMISSION_FORWARD = 1u << 7,
    PUB_ADMISSION_HAS_FORWARD = 1u << 8,
    PUB_ADMISSION_HAS_PRIORITY = 1u << 9,
    PUB_ADMISSION_HAS_TIMEOUT = 1u << 10
};

/* A bounded retry owner for answering one FETCH from a track's retained
 * object (see serve_retained_fetch). The retained object is SNAPSHOTTED here at
 * stage time (refs held, location/priority copied) so the response is
 * independent of the track's mutable storage -- a later clear/end/remove cannot
 * strand or corrupt an accepted fetch. Phased so a full action queue defers
 * instead of leaving a half-served fetch: accepted/written make each retry
 * idempotent. reject_code is set only BEFORE accept (an accepted fetch must
 * finish, never REQUEST_ERROR). Mirrors pub_pending_t for subscribe. */
typedef struct {
    bool                active;
    bool                reject;     /* refused: INTERNAL_ERROR is code zero */
    moq_fetch_t         fetch;
    moq_request_error_t reject_code; /* rejection wire code (pre-accept only) */
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
    /* The served track, for the Track Properties its FETCH_OK states; cleared
     * when the track is removed first (the answer then carries none). */
    moq_pub_track_t    *track;
    moq_subscription_t joining_sub;
    moq_publication_t joining_pub;
} pub_pending_fetch_t;

typedef struct {
    pub_pending_fetch_t pending;
    uint32_t prev, next;
} pub_fetch_owner_t;

typedef struct {
    pub_fetch_owner_t *cells;
    size_t cap;
    uint32_t head, tail;
} pub_fetch_owners_t;

#define PUB_FETCH_NONE UINT32_MAX

/* One fill request in flight (draft-22 section 3.4). Mirrors
 * pub_pending_fetch_t: the retained group is SNAPSHOTTED at stage time so the
 * fill is independent of later track mutation, and the phases (opened /
 * write cursor / FIN) make every retry idempotent. `abort` marks a request the
 * facade cannot serve; it is answered on the wire as a header followed by
 * RESET (section 3.4.1), never left pending. */
struct pub_pending_fill {
    bool                active;
    moq_fill_t          fill;
    moq_pub_fill_t      token;
    moq_pub_track_t    *track;
    moq_fill_request_event_t receipt;
    bool                requested;   /* real FILL_REQUEST delivered */
    bool                deferred;
    bool                terminal_queued;
    bool                abort;
    uint64_t            abort_code;
    bool                opened;      /* open_fill (FETCH_HEADER) done */
    uint64_t            group_id;
    uint8_t             priority;
    pub_retained_obj_t  objs[MOQ_PUB_RETAINED_MAX_OBJECTS];
    uint64_t            charge;
    size_t              obj_count;
    size_t              next_idx;    /* write cursor (resumes on WOULD_BLOCK) */
};

struct moq_pub_deferred {
    bool                active;
    uint64_t            generation;
    moq_subscription_t  sub;
    moq_pub_track_t    *track;
    bool                forward;   /* the request's Forward state, for the slot */
    bool                fill_requested;
    /* The request's NEW_GROUP_REQUEST, reported once it is accepted. */
    bool                has_new_group;
    uint64_t            new_group;
};

/* An answer this publisher owes a request it took (a TRACK_STATUS for one of
 * its tracks) or that only it will see (tick mode refuses what it does not
 * serve), held while the session cannot take it. track names the accepted
 * TRACK_STATUS; otherwise the request is refused with code. */
typedef struct pub_owed_answer {
    moq_event_kind_t    kind;
    uint64_t            handle;
    moq_pub_track_t    *track;
    moq_request_error_t code;
} pub_owed_answer_t;

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
    pub_admissions_t admissions;
    pub_fetch_owners_t fetch_owners;
    pub_pending_fill_t *pending_fills;
    size_t              pending_fill_cap;
    uint64_t            fill_bytes;
    bool                in_fill_callback;
    moq_pub_deferred_t  deferred;
    /* One answer per eligible session owner, in arrival order. */
    pub_owed_answer_t  *owed;
    size_t              owed_count;
    size_t              owed_cap;
};

static sub_fill_identity_allocator_t pub_fill_identities = SUB_FILL_IDENTITY_ALLOCATOR_INIT;

/* -- Subscriber slot helpers (fan-out boundary) ------------------- */

static void track_clear_slot(moq_publisher_t *pub, pub_sub_slot_t *s)
{
    (void)pub;
    memset(s, 0, sizeof(*s));
}

static void track_observe_terminals(moq_publisher_t *pub, moq_pub_track_t *t);
static bool slot_key_stopped(const moq_pub_track_t *t, const pub_sub_slot_t *sl,
                              uint64_t group, uint64_t subgroup);
static moq_result_t slot_stale_subgroup(moq_publisher_t *pub,
                                         moq_pub_track_t *t,
                                         pub_sub_slot_t *sl);

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
        if (!t->slots[i].active && !t->slots[i].retiring)
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

static void *pub_alloc(moq_publisher_t *p, size_t sz);
static void pub_free(moq_publisher_t *p, void *ptr, size_t sz);

/* Where concurrent subscriptions to a track are allowed (draft-22 §3.1),
 * each one is a destination slot of its own. Before a subscription is
 * accepted, make sure a free slot will be waiting for it at ACK commit.
 * The install after session accept therefore cannot find the slots full.
 * Slots are values,
 * so growing the array moves no live reference. */
static moq_result_t track_reserve_sub_slots(moq_publisher_t *pub,
                                            moq_pub_track_t *t)
{
    size_t need = 1;
    size_t free_slots = 0;
    for (size_t i = 0; i < t->slot_cap; i++)
        if (!t->slots[i].active && !t->slots[i].retiring) free_slots++;
    if (free_slots >= need) return MOQ_OK;
    size_t grow = need - free_slots;
    if (t->slot_cap > SIZE_MAX / sizeof(pub_sub_slot_t) - grow)
        return MOQ_ERR_NOMEM;
    size_t cap = t->slot_cap + grow;
    pub_sub_slot_t *ns = (pub_sub_slot_t *)pub_alloc(pub, cap * sizeof(pub_sub_slot_t));
    if (!ns) return MOQ_ERR_NOMEM;
    memcpy(ns, t->slots, t->slot_cap * sizeof(pub_sub_slot_t));
    memset(ns + t->slot_cap, 0, grow * sizeof(pub_sub_slot_t));
    pub_free(pub, t->slots, t->slot_cap * sizeof(pub_sub_slot_t));
    t->slots = ns;
    t->slot_cap = cap;
    return MOQ_OK;
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
    if (moq_session_supports_fill(pub->session)) {
        int slot = sub_resolve_handle(pub->session, sub);
        if (slot >= 0) pub->session->subs[slot].facade_defer_updates = true;
    }
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
    if (w->has_end_object && group_id == w->end_group &&
        object_id > w->end_object)
        return false;
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

/* Subgroup header admission for a new stream object operation. The
 * END_OF_GROUP bit and the presence of object properties are fields of the
 * subgroup header Type (drafts 16 and 18), fixed when the subgroup opens. A
 * destination this operation would serve that already has the object's
 * Group open must be able to carry the object under the header it sent;
 * otherwise the operation is refused before it is committed, so nothing is
 * left pending. */
static bool track_header_admits(const moq_pub_track_t *t, uint64_t group_id,
                                uint64_t object_id, bool need_ext,
                                bool check_eog, bool want_eog)
{
    for (size_t i = 0; i < t->slot_cap; i++) {
        const pub_sub_slot_t *sl = &t->slots[i];
        if (!slot_eligible(t, sl) ||
            !window_admits(&sl->window, group_id, object_id))
            continue;
        if (!sl->sg_open || sl->cur_group != group_id) continue;
        if (need_ext && !sl->sg_has_extensions) return false;
        if (check_eog && want_eog != sl->sg_end_of_group) return false;
    }
    return true;
}

/* Closure completeness, kept apart from delivery eligibility (window_admits):
 * the Objects of its subgroup a destination's stream must carry before it may
 * end with a FIN. Only Objects below the subscription's Start Location are
 * exempt (draft-22 section 11.3.2; draft-18 and draft-16 the same). An Object
 * the window's end excludes is never delivered, but it is still owed, so a
 * stream that ends without it ends with a RESET. */
static bool window_owes(const moq_resolved_window_t *w,
                        uint64_t group_id, uint64_t object_id)
{
    if (!w->has_window || w->unsatisfiable) return true;
    return !(group_id < w->start_group ||
             (group_id == w->start_group && object_id < w->start_object));
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
static moq_result_t slot_close_subgroup(moq_publisher_t *pub,
                                        moq_pub_track_t *t, pub_sub_slot_t *sl,
                                        uint64_t now_us);
static moq_result_t track_run_retires(moq_publisher_t *pub,
                                      moq_pub_track_t *t, uint64_t now_us)
{
    track_observe_terminals(pub, t);
    for (size_t i = 0; i < t->slot_cap; i++) {
        pub_sub_slot_t *sl = &t->slots[i];
        if (!(sl->active || sl->retiring) || !sl->needs_reset) continue;
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
        if (sl->retiring) track_clear_slot(pub, sl);
    }
    return MOQ_OK;
}

/* The destination of `sl` left while the session keeps its data streams.
 * A subgroup still open did not deliver everything it began, so it is reset,
 * never finished (draft-22 §11.3.2): the slot retires holding only that
 * subgroup, outside every operation, and track_run_retires queues the reset
 * and frees the slot, also under action pressure. Otherwise the slot is
 * free at once. */
static void track_retire_slot(moq_publisher_t *pub, moq_pub_track_t *t,
                              pub_sub_slot_t *sl)
{
    if (!sl->sg_open && !sl->streaming) {
        track_clear_slot(pub, sl);
        return;
    }
    slot_forward_drop(t, sl);
    sl->active = false;
    sl->retiring = true;
}

/* §10: finite-window completion sweep for one track, on drafts whose
 * PUBLISH_DONE has SUBSCRIPTION_ENDED. Every active destination whose finite
 * filter end is covered by the declared watermark terminates: FIN any clean
 * open subgroup FIRST (streams close before the
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
    /* Only a draft with SUBSCRIPTION_ENDED ends a subscription at its filter
     * end. Draft-22 has no such status (§16.11.3), and "A publisher does not
     * end a subscription solely because the Largest Object advances past the
     * end of the current Location Filter" (§3.3.1): the window keeps
     * excluding later objects and the subscription stays established. */
    if (!pub->session->profile->publish_done_subscription_ended) return MOQ_OK;
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
            moq_result_t rc = slot_close_subgroup(pub, t, sl, now_us);
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
 * per-object validation runs BEFORE any state is touched, and the exactly-once history merge runs
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

/* -- Group history (identity of every assigned Object) --------------- *
 *
 * Every Object the track admits is assigned (group, Subgroup ID, delivery
 * mode) at admission -- live stream, datagram or retained install, with or
 * without destinations -- and keeps it for the track's life: a duplicate with
 * another Subgroup ID or delivery mode is malformed (draft-22 sections 2.1 and
 * 7.1). A subgroup ends with a FIN when the track moves to another group;
 * that FIN fixes the subgroup's final Object (section 12.1), so a later Object
 * of the same group goes into a NEW subgroup (Subgroup IDs are the
 * publisher's choice within a group, section 2.2) and must be above every
 * Object already assigned there. All checks run before any effect; a
 * refusal is MOQ_ERR_WRONG_STATE for history / finality / mode conflicts and
 * MOQ_ERR_NOMEM when the fixed bookkeeping cannot hold the result. */

typedef enum {
    HIST_STREAM = 0,
    HIST_DATAGRAM,
    HIST_STATUS_EOG,
    HIST_STATUS_EOT,
} hist_kind_t;

typedef struct {
    int      rec;        /* record holding the group after the op, or -1 */
    bool     alloc;      /* a free or evicted record is (re)initialized */
    int      victim;     /* record compacted into the sealed runs first */
    bool     new_gen;    /* stream: the op starts a new generation */
    uint8_t  gen;        /* stream: the generation (Subgroup ID) */
} hist_plan_t;

static int hist_find(const moq_pub_track_t *t, uint64_t group)
{
    for (int i = 0; i < PUB_GROUP_RECORDS; i++)
        if (t->recs[i].used && t->recs[i].group == group) return i;
    return -1;
}

static bool slot_key_stopped(const moq_pub_track_t *t, const pub_sub_slot_t *sl,
                              uint64_t group, uint64_t subgroup)
{
    int ri = hist_find(t, group);
    return ri >= 0 && subgroup < PUB_GROUP_GENERATIONS &&
        sl->stopped[ri].group == group &&
        (sl->stopped[ri].generations & (1u << subgroup)) != 0;
}

static bool track_note_terminal(moq_publisher_t *pub, moq_pub_track_t *t,
                                 const moq_event_t *event)
{
    if (event->kind != MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED ||
        event->detail_size < MOQ_OUTGOING_SUBGROUP_TERMINATED_EVENT_V0_SIZE)
        return false;
    const moq_outgoing_subgroup_terminated_event_t *d =
        &event->u.outgoing_subgroup_terminated;
    bool subscription = moq_subscription_is_valid(d->sub);
    if (subscription == moq_publication_is_valid(d->pub) ||
        moq_handle_pool_tag(d->subgroup._opaque) != MOQ_HANDLE_POOL_SUBGROUP ||
        moq_handle_session_tag(d->subgroup._opaque) != pub->session->session_tag ||
        (d->cause != MOQ_OUTGOING_SUBGROUP_TERMINAL_PEER_STOP_SENDING &&
         d->cause != MOQ_OUTGOING_SUBGROUP_TERMINAL_DELIVERY_TIMEOUT))
        return false;
    for (size_t i = 0; i < t->slot_cap; i++) {
        pub_sub_slot_t *sl = &t->slots[i];
        if (!sl->active && !sl->retiring) continue;
        if (subscription ? (sl->kind != PUB_SLOT_SUBSCRIPTION ||
                             !moq_subscription_eq(sl->sub, d->sub))
                         : (sl->kind != PUB_SLOT_PUBLICATION ||
                             !moq_publication_eq(sl->pub, d->pub)))
            continue;
        int ri = hist_find(t, d->group_id);
        if (ri >= 0 && d->subgroup_id < t->recs[ri].ngen) {
            if (sl->stopped[ri].group != d->group_id) {
                sl->stopped[ri].group = d->group_id;
                sl->stopped[ri].generations = 0;
            }
            sl->stopped[ri].generations |= (uint8_t)(1u << d->subgroup_id);
        }
        bool exact = moq_subgroup_eq(sl->sg, d->subgroup);
        bool object_op = (t->op.kind == PUB_OP_WRITE_OBJECT ||
                          t->op.kind == PUB_OP_BEGIN_OBJECT) &&
            !t->op.datagram && t->op.group_id == d->group_id &&
            t->op.subgroup == d->subgroup_id;
        bool bracket_op = exact && sl->streaming &&
            (t->op.kind == PUB_OP_WRITE_DATA || t->op.kind == PUB_OP_END_OBJECT);
        if (sl->target_op == t->cur_op && (object_op || bracket_op))
            sl->done_op = t->cur_op;
        if (exact) {
            sl->sg_open = false;
            sl->streaming = false;
            sl->needs_reset = false;
            if (sl->retiring) track_clear_slot(pub, sl);
        }
        return true;
    }
    return false;
}

/* Scalar notices can precede their normal FIFO dispatch while a caller is
 * retrying production. Observe them without taking another facade's events,
 * advancing the session, or invoking callbacks under a service lock. */
static void track_observe_terminals(moq_publisher_t *pub, moq_pub_track_t *t)
{
    const moq_session_t *s = pub->session;
    for (size_t i = s->event_head; i < s->event_tail; i++)
        (void)track_note_terminal(pub, t, &s->events[i % s->event_cap]);
}

static moq_result_t slot_stale_subgroup(moq_publisher_t *pub,
                                         moq_pub_track_t *t,
                                         pub_sub_slot_t *sl)
{
    track_observe_terminals(pub, t);
    if (!sl->active || slot_key_stopped(t, sl, sl->cur_group, sl->cur_gen))
        return MOQ_OK;
    int owner = sl->kind == PUB_SLOT_PUBLICATION
        ? pub_resolve_handle(pub->session, sl->pub)
        : sub_resolve_handle(pub->session, sl->sub);
    if (owner < 0) {
        track_clear_slot(pub, sl);
        return MOQ_OK;
    }
    return MOQ_ERR_STALE_HANDLE;
}

static bool hist_sealed(const moq_pub_track_t *t, uint64_t group)
{
    for (size_t i = 0; i < t->sealed_count; i++)
        if (group >= t->sealed[i].lo && group <= t->sealed[i].hi) return true;
    return false;
}

static size_t hist_pins(const moq_pub_track_t *t)
{
    size_t n = 0;
    for (int i = 0; i < PUB_GROUP_RECORDS; i++)
        if (t->recs[i].used && t->recs[i].ngen >= 2) n++;
    return n;
}

static bool ival_has_owner(const pub_group_rec_t *r, uint8_t owner);

/* The sealed-run form of record r. */
static pub_sealed_run_t hist_run_of(const pub_group_rec_t *r)
{
    pub_sealed_run_t run;
    run.lo = run.hi = r->group;
    run.first = r->ngen ? r->gen[0].first : 0;
    run.flags = (uint8_t)((r->ngen ? PUB_SEALED_STREAM : 0) |
                          (r->retained_origin ? PUB_SEALED_RETAINED : 0) |
                          (ival_has_owner(r, PUB_OWNER_DATAGRAM)
                               ? PUB_SEALED_DATAGRAM : 0));
    return run;
}

/* Index of a sealed run that `run` (a single group) extends, or -1. */
static int hist_run_neighbour(const moq_pub_track_t *t,
                              const pub_sealed_run_t *run)
{
    for (size_t i = 0; i < t->sealed_count; i++) {
        const pub_sealed_run_t *x = &t->sealed[i];
        if (x->first != run->first || x->flags != run->flags) continue;
        if ((x->hi != UINT64_MAX && x->hi + 1 == run->lo) ||
            (x->lo != 0 && x->lo - 1 == run->hi))
            return (int)i;
    }
    return -1;
}

static bool hist_seal_fits(const moq_pub_track_t *t, const pub_group_rec_t *r)
{
    pub_sealed_run_t run = hist_run_of(r);
    return hist_run_neighbour(t, &run) >= 0 ||
           t->sealed_count < PUB_SEALED_RUNS;
}

/* Record r leaves the exact set: an open subgroup of it on any destination is
 * judged complete or not NOW (its record goes away), then the group joins
 * the sealed runs. Capacity was priced by hist_seal_fits. */
static void hist_seal(moq_pub_track_t *t, int ri)
{
    pub_group_rec_t *r = &t->recs[ri];
    for (size_t i = 0; i < t->slot_cap; i++) {
        pub_sub_slot_t *sl = &t->slots[i];
        memset(&sl->stopped[ri], 0, sizeof(sl->stopped[ri]));
        if (!sl->sg_open || sl->cur_group != r->group || sl->cur_gen >= r->ngen)
            continue;
        const pub_gen_t *g = &r->gen[sl->cur_gen];
        if (g->last > sl->sg_last && window_owes(&sl->window, r->group, g->last))
            sl->sg_omitted = true;
    }
    if (t->has_retained && t->retained_group_id == r->group && r->has_max) {
        t->has_sealed_retained_max = true;
        t->sealed_retained_group = r->group;
        t->sealed_retained_max = r->max;
    }
    pub_sealed_run_t run = hist_run_of(r);
    int n = hist_run_neighbour(t, &run);
    if (n < 0) {
        t->sealed[t->sealed_count++] = run;
    } else {
        pub_sealed_run_t *x = &t->sealed[n];
        if (run.lo < x->lo) x->lo = run.lo;
        if (run.hi > x->hi) x->hi = run.hi;
        /* The run may now meet another with the same shape. */
        for (size_t i = 0; i < t->sealed_count; i++) {
            pub_sealed_run_t *y = &t->sealed[i];
            if ((int)i == n || y->first != x->first || y->flags != x->flags)
                continue;
            if ((x->hi != UINT64_MAX && x->hi + 1 == y->lo) ||
                (y->hi != UINT64_MAX && y->hi + 1 == x->lo)) {
                if (y->lo < x->lo) x->lo = y->lo;
                if (y->hi > x->hi) x->hi = y->hi;
                t->sealed[i] = t->sealed[--t->sealed_count];
                break;
            }
        }
    }
    if (t->has_cur && t->cur_group == r->group) t->has_cur = false;
    memset(r, 0, sizeof(*r));
}

/* Plan a record for `group`: the existing one, a free one, or the least
 * recently admitted unpinned record other than `keep` (the group current
 * after the op), compacted into the sealed runs. With `compact_keep`, when no
 * other record is free to compact, `keep` itself is compacted (its open
 * destinations keep their completeness verdict, see hist_seal). Sealed-run
 * capacity is priced here, before any effect. */
static moq_result_t hist_plan_record(const moq_pub_track_t *t, uint64_t group,
                                     bool has_keep, uint64_t keep,
                                     bool compact_keep, hist_plan_t *pl)
{
    pl->rec = hist_find(t, group);
    pl->alloc = false;
    pl->victim = -1;
    if (pl->rec >= 0) return MOQ_OK;
    if (hist_sealed(t, group)) return MOQ_ERR_WRONG_STATE;
    for (int i = 0; i < PUB_GROUP_RECORDS; i++)
        if (!t->recs[i].used) { pl->rec = i; pl->alloc = true; return MOQ_OK; }
    int v = -1;
    for (int i = 0; i < PUB_GROUP_RECORDS; i++) {
        const pub_group_rec_t *r = &t->recs[i];
        if (r->ngen >= 2) continue;                   /* pinned */
        if (has_keep && r->group == keep) continue;
        if (v < 0 || r->seq < t->recs[v].seq) v = i;
    }
    if (v < 0 && has_keep && compact_keep) {
        int k = hist_find(t, keep);
        if (k >= 0 && t->recs[k].ngen < 2) v = k;
    }
    if (v < 0 || !hist_seal_fits(t, &t->recs[v])) return MOQ_ERR_NOMEM;
    pl->rec = v;
    pl->alloc = true;
    pl->victim = v;
    return MOQ_OK;
}

static void hist_take_record(moq_pub_track_t *t, const hist_plan_t *pl,
                             uint64_t group)
{
    if (pl->victim >= 0) hist_seal(t, pl->victim);
    pub_group_rec_t *r = &t->recs[pl->rec];
    if (pl->alloc) {
        memset(r, 0, sizeof(*r));
        r->used = true;
        r->group = group;
    }
    r->seq = ++t->rec_seq;
}

static bool hist_loc_at_or_after(uint64_t g, uint64_t o, uint64_t eg, uint64_t eo)
{
    return g > eg || (g == eg && o >= eo);
}

/* Finality and END_OF_TRACK: may Object `obj` be assigned in record r? */
static bool hist_final_admits(const moq_pub_track_t *t,
                              const pub_group_rec_t *r, uint64_t group,
                              uint64_t obj)
{
    if (t->has_eot &&
        hist_loc_at_or_after(group, obj, t->eot_group, t->eot_object))
        return false;
    if (!r) return true;
    switch (r->final_kind) {
    case PUB_FINAL_SEALED: return false;
    case PUB_FINAL_LAST:   return obj <= r->final_at;
    case PUB_FINAL_FROM:   return obj < r->final_at;
    default:               return true;
    }
}

/* -- Exact membership ------------------------------------------------- *
 *
 * Every assigned Object of a group is in exactly one closed interval of the
 * record, owned by the generation (Subgroup ID) it was assigned to or by the
 * datagram mode: an Object belongs to exactly one Subgroup or is a datagram
 * (draft-22 section 2.1; draft-18 2.1, draft-16 2.4). Intervals stay sorted
 * and non-overlapping and merge only with the same owner. Queries are exact,
 * inclusive and bounded; no bound is formed by wrapping at 0 or UINT64_MAX. */

static int ival_find(const pub_group_rec_t *r, uint64_t obj)
{
    for (uint8_t i = 0; i < r->nival; i++)
        if (obj >= r->ival[i].lo && obj <= r->ival[i].hi) return (int)i;
    return -1;
}

static bool ival_has_owner(const pub_group_rec_t *r, uint8_t owner)
{
    for (uint8_t i = 0; i < r->nival; i++)
        if (r->ival_owner[i] == owner) return true;
    return false;
}

/* Interval x and [lo, hi] overlap, or touch end to end. */
static bool ival_touches(uint64_t xlo, uint64_t xhi, uint64_t lo, uint64_t hi)
{
    if (xlo <= hi && xhi >= lo) return true;
    if (xhi != UINT64_MAX && xhi + 1 == lo) return true;
    if (hi != UINT64_MAX && hi + 1 == xlo) return true;
    return false;
}

/* Price assigning [lo, hi] (lo <= hi) to `owner`: false when an interval of
 * another owner overlaps it (the identity is taken); otherwise *count is the
 * number of intervals afterwards. Mutates nothing. */
static bool ival_plan(const pub_group_rec_t *r, uint64_t lo, uint64_t hi,
                      uint8_t owner, int *count)
{
    int merged = 0;
    for (uint8_t i = 0; i < r->nival; i++) {
        uint64_t xlo = r->ival[i].lo, xhi = r->ival[i].hi;
        if (r->ival_owner[i] != owner) {
            if (xlo <= hi && xhi >= lo) return false;
            continue;
        }
        if (ival_touches(xlo, xhi, lo, hi)) merged++;
    }
    *count = (int)r->nival - merged + 1;
    return true;
}

/* Apply a union priced by ival_plan (no conflict, room for the result). */
static void ival_apply(pub_group_rec_t *r, uint64_t lo, uint64_t hi,
                       uint8_t owner)
{
    uint8_t n = 0;
    for (uint8_t i = 0; i < r->nival; i++) {
        bool merge = r->ival_owner[i] == owner &&
                     ival_touches(r->ival[i].lo, r->ival[i].hi, lo, hi);
        if (merge) {
            if (r->ival[i].lo < lo) lo = r->ival[i].lo;
            if (r->ival[i].hi > hi) hi = r->ival[i].hi;
            continue;
        }
        r->ival[n] = r->ival[i];
        r->ival_owner[n] = r->ival_owner[i];
        n++;
    }
    uint8_t at = 0;
    while (at < n && r->ival[at].lo < lo) at++;
    for (uint8_t i = n; i > at; i--) {
        r->ival[i] = r->ival[i - 1];
        r->ival_owner[i] = r->ival_owner[i - 1];
    }
    r->ival[at].lo = lo;
    r->ival[at].hi = hi;
    r->ival_owner[at] = owner;
    r->nival = (uint8_t)(n + 1);
}

/* The largest Object owned by `owner` below `obj`, if any (exact). */
static bool ival_max_below(const pub_group_rec_t *r, uint8_t owner,
                           uint64_t obj, uint64_t *out)
{
    bool has = false;
    uint64_t best = 0;
    if (obj == 0) return false;
    for (uint8_t i = 0; i < r->nival; i++) {
        if (r->ival_owner[i] != owner || r->ival[i].lo >= obj) continue;
        uint64_t c = r->ival[i].hi < obj - 1 ? r->ival[i].hi : obj - 1;
        if (!has || c > best) { best = c; has = true; }
    }
    if (has) *out = best;
    return has;
}

static bool hist_known_stream(const pub_group_rec_t *r, uint64_t obj)
{
    int i = ival_find(r, obj);
    return i >= 0 && r->ival_owner[i] != PUB_OWNER_DATAGRAM;
}

static bool hist_known_datagram(const pub_group_rec_t *r, uint64_t obj)
{
    int i = ival_find(r, obj);
    return i >= 0 && r->ival_owner[i] == PUB_OWNER_DATAGRAM;
}

static void hist_note_max(pub_group_rec_t *r, uint64_t obj)
{
    if (!r->has_max || obj > r->max) r->max = obj;
    r->has_max = true;
}

/* A generation's subgroup ends: with the END_OF_GROUP header bit armed, the
 * clean end fixes the group's final Object (draft-22 section 11.3.1). */
static void hist_end_gen(pub_group_rec_t *r, pub_gen_t *g)
{
    if (g->end != PUB_GEN_OPEN) return;
    g->end = PUB_GEN_FIN;
    if (g->eog_armed && r->final_kind == PUB_FINAL_NONE) {
        r->final_kind = PUB_FINAL_LAST;
        r->final_at = g->last;
    }
}

/* The track leaves its current generation for another group (or seals it):
 * that generation's subgroup ends with a FIN. */
static void hist_close_current(moq_pub_track_t *t)
{
    if (!t->has_cur) return;
    int ri = hist_find(t, t->cur_group);
    if (ri < 0) return;
    pub_group_rec_t *r = &t->recs[ri];
    if (t->cur_gen >= r->ngen) return;
    hist_end_gen(r, &r->gen[t->cur_gen]);
}

/* moq_pub_end_group: the current group is sealed (a local declaration that
 * nothing more is assigned in it; on the wire its subgroups simply end). */
static void hist_end_group(moq_pub_track_t *t)
{
    if (!t->has_cur) return;
    hist_close_current(t);
    int ri = hist_find(t, t->cur_group);
    if (ri >= 0) t->recs[ri].final_kind = PUB_FINAL_SEALED;
}

/* moq_pub_reset_group: the current generation is abandoned. Its assignments
 * stay; it fixes no final (section 11.3.1), and the group's next Object
 * starts a new generation above every Object already assigned. */
static void hist_reset_current(moq_pub_track_t *t)
{
    if (!t->has_cur) return;
    int ri = hist_find(t, t->cur_group);
    if (ri < 0 || t->cur_gen >= t->recs[ri].ngen) return;
    pub_gen_t *g = &t->recs[ri].gen[t->cur_gen];
    if (g->end == PUB_GEN_OPEN) g->end = PUB_GEN_RESET;
    g->eog_armed = false;
}

/* Plan a stream Object (one-shot or streaming begin). */
static moq_result_t hist_plan_stream(const moq_pub_track_t *t, uint64_t group,
                                     uint64_t obj, hist_plan_t *pl)
{
    moq_result_t rc = hist_plan_record(t, group, true, group, false, pl);
    if (rc < 0) return rc;
    const pub_group_rec_t *r = pl->alloc ? NULL : &t->recs[pl->rec];
    if (!hist_final_admits(t, r, group, obj)) return MOQ_ERR_WRONG_STATE;
    pl->new_gen = false;
    pl->gen = 0;
    int count = 1;
    if (!r || r->ngen == 0) {
        if (r && !ival_plan(r, obj, obj, 0, &count))
            return MOQ_ERR_WRONG_STATE;          /* a datagram Object */
        if (count > PUB_GROUP_INTERVALS) return MOQ_ERR_NOMEM;
        pl->new_gen = true;                         /* generation 0 */
        return MOQ_OK;
    }
    uint8_t k = (uint8_t)(r->ngen - 1);
    const pub_gen_t *g = &r->gen[k];
    int at = ival_find(r, obj);
    bool replay = at >= 0 && r->ival_owner[at] == k;
    if (g->end == PUB_GEN_OPEN && (!g->fin_seen || replay)) {
        /* Continue: the current generation, or a retained-origin generation
         * never yet closed. Generations hold disjoint ascending ranges: an
         * Object at or below the previous generation's highest belongs to an
         * earlier subgroup (or may), so it is never assigned here. Once a
         * destination ended this subgroup with a FIN, its final Object is
         * fixed for that receiver (draft-22 section 12.1), so only the
         * replay of an Object already assigned to it continues it. */
        if (k >= 1 && obj <= r->gen[k - 1].last) return MOQ_ERR_WRONG_STATE;
        if (!ival_plan(r, obj, obj, k, &count)) return MOQ_ERR_WRONG_STATE;
        if (count > PUB_GROUP_INTERVALS) return MOQ_ERR_NOMEM;
        pl->gen = k;
        return MOQ_OK;
    }
    /* A new subgroup for a genuinely new, larger Object: a revisit after the
     * subgroup closed or was reset, or the next generation after a
     * destination's FIN. Subgroups of one group are the publisher's choice
     * (draft-22 section 2.2). A subgroup whose header carried END_OF_GROUP
     * holds the group's largest Object, so nothing may follow it. */
    if (r->has_max && obj <= r->max) return MOQ_ERR_WRONG_STATE;
    if (g->end == PUB_GEN_OPEN && g->eog_armed) return MOQ_ERR_WRONG_STATE;
    if (r->ngen >= PUB_GROUP_GENERATIONS) return MOQ_ERR_NOMEM;
    if (r->ngen == 1 && hist_pins(t) >= PUB_GROUP_PINS) return MOQ_ERR_NOMEM;
    if (!ival_plan(r, obj, obj, r->ngen, &count)) return MOQ_ERR_WRONG_STATE;
    if (count > PUB_GROUP_INTERVALS) return MOQ_ERR_NOMEM;
    pl->new_gen = true;
    pl->gen = r->ngen;
    return MOQ_OK;
}

/* Commit a planned stream Object; the op is already committed (slots
 * stamped), so destinations holding this generation open and not part of the
 * op are marked as having omitted it, and destinations part of it that will
 * skip an earlier assigned Object they owe are marked too. */
static void hist_commit_stream(moq_pub_track_t *t, const hist_plan_t *pl,
                               uint64_t group, uint64_t obj, bool eog)
{
    if (!(t->has_cur && t->cur_group == group))
        hist_close_current(t);
    hist_take_record(t, pl, group);
    pub_group_rec_t *r = &t->recs[pl->rec];
    t->op.subgroup = pl->gen;
    t->op.has_below = false;
    if (pl->new_gen) {
        /* The generation before it, still open in this group (a FIN by a
         * destination ended it), ends now; its destinations finish it. */
        if (pl->gen >= 1)
            hist_end_gen(r, &r->gen[pl->gen - 1]);
        pub_gen_t *g = &r->gen[pl->gen];
        memset(g, 0, sizeof(*g));
        g->first = g->last = obj;
        r->ngen = (uint8_t)(pl->gen + 1);
        t->op.first_in_gen = true;
    } else {
        pub_gen_t *g = &r->gen[pl->gen];
        /* Before this Object joins: the largest earlier assigned Object of
         * the generation, the witness a destination must have carried. */
        t->op.has_below = ival_max_below(r, pl->gen, obj, &t->op.below);
        t->op.first_in_gen = (obj == g->first);
        if (obj > g->last) g->last = obj;
    }
    ival_apply(r, obj, obj, pl->gen);
    if (eog) r->gen[pl->gen].eog_armed = true;
    hist_note_max(r, obj);
    t->has_cur = true;
    t->cur_group = group;
    t->cur_gen = pl->gen;
    for (size_t i = 0; i < t->slot_cap; i++) {
        pub_sub_slot_t *sl = &t->slots[i];
        if (!sl->sg_open || sl->cur_group != group || sl->cur_gen != pl->gen)
            continue;
        if (sl->target_op == t->cur_op) {
            /* Part of this op: writing `obj` next on its stream skips every
             * assigned Object between its last write and `obj`; owed ones
             * (at or after its Start Location) are omitted. */
            if (t->op.has_below && t->op.below > sl->sg_last &&
                window_owes(&sl->window, group, t->op.below))
                sl->sg_omitted = true;
            continue;
        }
        if (window_owes(&sl->window, group, obj)) sl->sg_omitted = true;
    }
}

/* Plan a datagram Object or status. Under record pressure the current
 * stream group may itself be compacted into the sealed runs (its open
 * destinations keep their completeness verdict; later writes to it are
 * refused). */
static moq_result_t hist_plan_datagram(const moq_pub_track_t *t,
                                       hist_kind_t kind, uint64_t group,
                                       uint64_t obj, hist_plan_t *pl)
{
    moq_result_t rc = hist_plan_record(t, group, t->has_cur, t->cur_group,
                                       true, pl);
    if (rc < 0) return rc;
    const pub_group_rec_t *r = pl->alloc ? NULL : &t->recs[pl->rec];
    if (!hist_final_admits(t, r, group, obj)) return MOQ_ERR_WRONG_STATE;
    if (r && hist_known_stream(r, obj)) return MOQ_ERR_WRONG_STATE;
    /* A status names a boundary below which Objects exist and at or above
     * which none do (section 11.1.1): one already assigned at or above it
     * contradicts it. A status is not an assigned Object. */
    if (kind == HIST_STATUS_EOG && r && r->has_max && r->max >= obj &&
        !(r->max == obj && hist_known_datagram(r, obj)))
        return MOQ_ERR_WRONG_STATE;
    if (kind == HIST_DATAGRAM && r) {
        int count;
        if (!ival_plan(r, obj, obj, PUB_OWNER_DATAGRAM, &count))
            return MOQ_ERR_WRONG_STATE;
        if (count > PUB_GROUP_INTERVALS) return MOQ_ERR_NOMEM;
    }
    return MOQ_OK;
}

static void hist_commit_datagram(moq_pub_track_t *t, const hist_plan_t *pl,
                                 hist_kind_t kind, uint64_t group, uint64_t obj)
{
    hist_take_record(t, pl, group);
    pub_group_rec_t *r = &t->recs[pl->rec];
    if (kind == HIST_STATUS_EOT) {
        if (!t->has_eot ||
            !hist_loc_at_or_after(group, obj, t->eot_group, t->eot_object)) {
            t->has_eot = true;
            t->eot_group = group;
            t->eot_object = obj;
        }
        return;
    }
    if (kind == HIST_STATUS_EOG) {
        if (r->final_kind != PUB_FINAL_SEALED &&
            (r->final_kind == PUB_FINAL_NONE ||
             (r->final_kind == PUB_FINAL_FROM && obj < r->final_at))) {
            r->final_kind = PUB_FINAL_FROM;
            r->final_at = obj;
        }
        return;
    }
    ival_apply(r, obj, obj, PUB_OWNER_DATAGRAM);
    hist_note_max(r, obj);
}

/* A retained install of dense Objects 0..last in `group`. Allowed for a group
 * with no history, and for a group whose only assignments are generation 0
 * (no datagram, no revisit): when 0..last are all already generation-0
 * Objects it assigns nothing new and is allowed even after a FIN; otherwise
 * it extends generation 0's interval starting at Object 0 over unassigned
 * Objects, which only a subgroup no stream of which has ended with a FIN may
 * take -- a FIN fixed the subgroup's final Object (section 12.1, conditions 2
 * and 3). */
static moq_result_t hist_plan_retained(const moq_pub_track_t *t, uint64_t group,
                                       uint64_t last, hist_plan_t *pl)
{
    moq_result_t rc = hist_plan_record(t, group, t->has_cur, t->cur_group,
                                       false, pl);
    if (rc < 0) return rc;
    pl->new_gen = false;
    pl->gen = 0;
    if (!hist_final_admits(t, pl->alloc ? NULL : &t->recs[pl->rec], group, last))
        return MOQ_ERR_WRONG_STATE;
    if (pl->alloc) return MOQ_OK;
    const pub_group_rec_t *r = &t->recs[pl->rec];
    if (ival_has_owner(r, PUB_OWNER_DATAGRAM) || r->ngen > 1)
        return MOQ_ERR_WRONG_STATE;
    if (r->ngen == 0) return MOQ_OK;
    int at = ival_find(r, 0);
    if (at < 0 || r->ival_owner[at] != 0) return MOQ_ERR_WRONG_STATE;
    if (last <= r->ival[at].hi) return MOQ_OK;      /* nothing new */
    if (r->gen[0].end == PUB_GEN_FIN || r->gen[0].fin_seen)
        return MOQ_ERR_WRONG_STATE;
    /* The extension takes only unassigned Objects. */
    for (uint8_t i = 0; i < r->nival; i++)
        if ((int)i != at && r->ival[i].lo <= last)
            return MOQ_ERR_WRONG_STATE;
    int count;
    if (!ival_plan(r, 0, last, 0, &count)) return MOQ_ERR_WRONG_STATE;
    if (count > PUB_GROUP_INTERVALS) return MOQ_ERR_NOMEM;
    return MOQ_OK;
}

static void hist_commit_retained(moq_pub_track_t *t, const hist_plan_t *pl,
                                 uint64_t group, uint64_t last)
{
    hist_take_record(t, pl, group);
    pub_group_rec_t *r = &t->recs[pl->rec];
    if (r->ngen == 0) {
        memset(&r->gen[0], 0, sizeof(r->gen[0]));
        r->gen[0].first = 0;
        r->gen[0].last = last;
        r->ngen = 1;
        r->retained_origin = true;
    } else if (last > r->gen[0].last) {
        r->gen[0].last = last;
        /* Destinations holding generation 0 open never saw these. */
        for (size_t i = 0; i < t->slot_cap; i++) {
            pub_sub_slot_t *sl = &t->slots[i];
            if (sl->sg_open && sl->cur_group == group && sl->cur_gen == 0 &&
                window_owes(&sl->window, group, last))
                sl->sg_omitted = true;
        }
    }
    ival_apply(r, 0, last, 0);
    hist_note_max(r, last);
}

/* The Subgroup ID Object `obj` of `group` was assigned (0 for a group the
 * track has no record of). */
static uint64_t hist_subgroup_of(const moq_pub_track_t *t, uint64_t group,
                                 uint64_t obj)
{
    int ri = hist_find(t, group);
    if (ri < 0) return 0;
    int at = ival_find(&t->recs[ri], obj);
    if (at < 0 || t->recs[ri].ival_owner[at] == PUB_OWNER_DATAGRAM) return 0;
    return t->recs[ri].ival_owner[at];
}

/* Draft-16 only: the last Object sent in a FETCH that requested the entire
 * Group is that Group's final Object (draft-16 section 2.4.2, condition 7).
 * A whole-group response served from the retained Objects 0..last may be
 * staged only when no Object above `last` was ever assigned in the group --
 * live, retained or datagram, including an admitted Object not yet sent. */
static bool hist_whole_group_fetch_admits(const moq_pub_track_t *t,
                                          uint64_t group, uint64_t last)
{
    int ri = hist_find(t, group);
    if (ri >= 0)
        return !(t->recs[ri].has_max && t->recs[ri].max > last);
    if (hist_sealed(t, group))
        return t->has_sealed_retained_max &&
               t->sealed_retained_group == group &&
               t->sealed_retained_max <= last;
    return true;
}

/* A staged draft-16 whole-group response fixes the group's final Object at
 * `last` at once: nothing above it is assigned from now on, so the response
 * cannot be contradicted by a later admission while it waits to be sent. A
 * sealed group already takes nothing more. */
static void hist_whole_group_fetch_seal(moq_pub_track_t *t, uint64_t group,
                                        uint64_t last)
{
    int ri = hist_find(t, group);
    if (ri < 0) return;
    pub_group_rec_t *r = &t->recs[ri];
    if (r->final_kind == PUB_FINAL_SEALED) return;
    if (r->final_kind == PUB_FINAL_LAST && r->final_at <= last) return;
    r->final_kind = PUB_FINAL_LAST;
    r->final_at = last;
}

/* Close a destination's open subgroup: FIN only when it carried every Object
 * assigned to that subgroup at or after its Start Location (window_owes);
 * otherwise RESET (a FIN would claim the omitted Objects do not exist,
 * draft-22 section 11.3.2). A
 * FIN that is queued is recorded on the generation: it fixed the subgroup's
 * final Object for that receiver, so nothing above it may join the subgroup
 * later (section 12.1). */
static moq_result_t slot_close_subgroup(moq_publisher_t *pub,
                                        moq_pub_track_t *t, pub_sub_slot_t *sl,
                                        uint64_t now_us)
{
    bool reset = sl->sg_omitted;
    pub_gen_t *g = NULL;
    int ri = hist_find(t, sl->cur_group);
    if (ri >= 0 && sl->cur_gen < t->recs[ri].ngen)
        g = &t->recs[ri].gen[sl->cur_gen];
    if (!reset && g && g->last > sl->sg_last &&
        window_owes(&sl->window, sl->cur_group, g->last))
        reset = true;
    moq_result_t rc = reset
        ? moq_session_reset_subgroup(pub->session, sl->sg, 0x0, now_us)
        : moq_session_close_subgroup(pub->session, sl->sg, now_us);
    if (rc == MOQ_ERR_STALE_HANDLE) {
        /* Closing an already ended stream completes only that close. The
         * owner and its stream count remain until terminal control ends it. */
        track_observe_terminals(pub, t);
        sl->streaming = false;
        sl->sg_open = false;
        sl->needs_reset = false;
        return MOQ_OK;
    }
    if (rc == MOQ_OK && !reset && g) g->fin_seen = true;
    return rc;
}

/* A destination opens the op's subgroup at Object `obj`: it begins the
 * subgroup (FIRST_OBJECT) only at the generation's first Object, and an
 * earlier assigned Object at or after its Start Location is one it has
 * omitted. */
static void slot_open_note(moq_pub_track_t *t, pub_sub_slot_t *sl,
                           uint64_t group, uint64_t obj)
{
    sl->cur_gen = t->op.subgroup;
    sl->sg_last = obj;
    sl->sg_omitted = t->op.has_below &&
                     window_owes(&sl->window, group, t->op.below);
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

/* Whether a FETCH starting at (sg, so) names a published Object of the
 * track: something was published on it (its Largest Object is known to the
 * session, or a retained group holds objects) and the start is not past the
 * Largest Object. */
static bool fetch_start_published(const moq_pub_track_t *t,
                                  uint64_t sg, uint64_t so)
{
    if (t->hist && t->hist->has_largest) {
        uint64_t lg = t->hist->largest_group, lo = t->hist->largest_object;
        return sg < lg || (sg == lg && so <= lo);
    }
    return pub_track_has_retained(t);
}

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
 * object (§9.16.2 requires a known Largest).
 *
 * NOTE (pre-existing, deliberately out of scope here): this is NARROWER than
 * the track's largest history. §10.2.11 says a publisher that has published
 * Objects MUST report the largest Location it observed, and §5.1.2 makes that
 * a monotonic high-water mark independent of what is still cached -- which is
 * what PUBLISH reports (see track_install_retained). SUBSCRIBE_OK instead
 * reports only what the retained cache can still SERVE, so the advertise check
 * and the serve guard stay in step for the Joining-FETCH path. The two can
 * therefore disagree after the retained group is cleared or replaced by a
 * lower group. Reconciling them is its own change. */
static void pub_init_accept_cfg(const moq_pub_track_t *t,
                                moq_accept_subscribe_cfg_t *acc)
{
    moq_accept_subscribe_cfg_init(acc);
    if (retained_can_advertise_largest(t)) {
        acc->has_largest = true;
        acc->largest_group = pub_track_retained_group(t);
        acc->largest_object = pub_track_retained_last_object_id(t);
    }
    acc->track_properties = (moq_bytes_t){ t->props, t->props_len };
}

/* -- Owed request answers ---------------------------------------- */

/* Pure eligibility checks: a live generation that was already answered is
 * not an answer owner. ANN and NS_SUB have file-local session resolvers, so
 * their identity checks are mirrored here without advancing the session. */
static bool pub_answer_pending(moq_publisher_t *pub, const pub_owed_answer_t *a)
{
    moq_session_t *s = pub->session;
    switch (a->kind) {
    case MOQ_EVENT_SUBSCRIBE_REQUEST: {
        int slot = sub_resolve_handle(s, (moq_subscription_t){a->handle});
        if (slot < 0) return false;
        const moq_sub_entry_t *e = &s->subs[slot];
        return e->handle._opaque == a->handle && e->role == MOQ_SUB_ROLE_PUBLISHER &&
            e->state == MOQ_SUB_PENDING_PUBLISHER && !e->cancel_pending;
    }
    case MOQ_EVENT_PUBLISH_REQUEST: {
        int slot = pub_resolve_handle(s, (moq_publication_t){a->handle});
        if (slot < 0) return false;
        const moq_pub_entry_t *e = &s->publishes[slot];
        return e->handle._opaque == a->handle && e->role == MOQ_PUB_ROLE_SUBSCRIBER &&
            e->state == MOQ_PUB_PENDING_SUBSCRIBER && !e->done_pending;
    }
    case MOQ_EVENT_NAMESPACE_PUBLISHED: {
        size_t slot = moq_handle_slot(a->handle);
        if (moq_handle_pool_tag(a->handle) != MOQ_HANDLE_POOL_ANNOUNCEMENT ||
            moq_handle_session_tag(a->handle) != s->session_tag || slot >= s->ann_cap)
            return false;
        const moq_ann_entry_t *e = &s->announcements[slot];
        if (e->generation != moq_handle_generation(a->handle) ||
            e->handle._opaque != a->handle || e->role != MOQ_ANN_ROLE_RECEIVER ||
            e->state != MOQ_ANN_PENDING_RECEIVER)
            return false;
        return !s->profile->premature_fin_fails_request ||
            (!e->terminal_cause && !e->response_validated &&
             moq_request_lifetime_check_initial_response(&e->rs.lifetime, true) == MOQ_OK);
    }
    case MOQ_EVENT_NS_SUB_REQUEST: {
        size_t slot = moq_handle_slot(a->handle);
        if (moq_handle_pool_tag(a->handle) != MOQ_HANDLE_POOL_NAMESPACE_SUB ||
            moq_handle_session_tag(a->handle) != s->session_tag || slot >= s->ns_sub_cap)
            return false;
        const moq_ns_sub_entry_t *e = &s->ns_subs[slot];
        return e->generation == moq_handle_generation(a->handle) &&
            e->handle._opaque == a->handle && !e->local_subscriber &&
            e->state == MOQ_NS_SUB_PENDING_PUBLISHER && !e->terminal.cause;
    }
    case MOQ_EVENT_SUBSCRIBE_TRACKS_REQUEST: {
        int slot = track_sub_resolve_handle(s, (moq_track_sub_handle_t){a->handle});
        if (slot < 0) return false;
        const moq_track_sub_entry_t *e = &s->track_subs[slot];
        return e->handle._opaque == a->handle && e->role == MOQ_TRACK_SUB_ROLE_PUBLISHER &&
            e->state == MOQ_TRACK_SUB_PENDING_PUBLISHER;
    }
    case MOQ_EVENT_TRACK_STATUS_REQUEST: {
        int slot = ts_resolve_handle(s, (moq_track_status_handle_t){a->handle});
        if (slot < 0) return false;
        const moq_ts_entry_t *e = &s->track_statuses[slot];
        if (e->handle._opaque != a->handle || e->role != MOQ_TS_ROLE_PUBLISHER ||
            e->state != MOQ_TS_PENDING_PUBLISHER || e->goaway_sent)
            return false;
        return !s->profile->premature_fin_fails_request ||
            (!e->terminal_cause && !e->response_validated &&
             moq_request_lifetime_check_initial_response(&e->rs.lifetime, true) == MOQ_OK);
    }
    default:
        return false;
    }
}

/* Prune every retired owner, including tails behind a blocked live head.
 * Stable compaction does not send answers or reorder the remaining owners. */
static void pub_owed_prune(moq_publisher_t *pub)
{
    size_t keep = 0;
    for (size_t i = 0; i < pub->owed_count; i++) {
        if (!pub_answer_pending(pub, &pub->owed[i])) continue;
        if (keep != i) pub->owed[keep] = pub->owed[i];
        keep++;
    }
    pub->owed_count = keep;
}

/* The answer is settled: given, or its request is gone (a stale handle, or
 * no longer awaiting an answer). WOULD_BLOCK and CLOSED are told apart by
 * the callers; anything else is a local failure to answer. */
static bool pub_answer_settled(moq_result_t rc)
{
    return rc == MOQ_OK || rc == MOQ_ERR_STALE_HANDLE || rc == MOQ_ERR_WRONG_STATE;
}

/* One answer: a TRACK_STATUS accepted with what a SUBSCRIBE_OK for the track
 * would carry (§9.13), or a refusal. An acceptance the session cannot encode
 * (Track Properties larger than it can ever send) still owes the request its
 * answer: it becomes an INTERNAL_ERROR refusal, which *a then holds, so a
 * retry sends the refusal. */
static moq_result_t pub_answer_request(moq_publisher_t *pub,
                                       pub_owed_answer_t *a, uint64_t now_us)
{
    moq_session_t *s = pub->session;
    switch (a->kind) {
    case MOQ_EVENT_TRACK_STATUS_REQUEST: {
        moq_track_status_handle_t h = { a->handle };
        if (a->track) {
            moq_accept_track_status_cfg_t acc;
            moq_accept_track_status_cfg_init(&acc);
            if (retained_can_advertise_largest(a->track)) {
                acc.has_largest = true;
                acc.largest_group = pub_track_retained_group(a->track);
                acc.largest_object = pub_track_retained_last_object_id(a->track);
            }
            acc.track_properties = (moq_bytes_t){ a->track->props, a->track->props_len };
            moq_result_t rc = moq_session_accept_track_status(s, h, &acc, now_us);
            if (pub_answer_settled(rc) || rc == MOQ_ERR_WOULD_BLOCK ||
                rc == MOQ_ERR_CLOSED)
                return rc;
            a->track = NULL;
            a->code = MOQ_REQUEST_ERROR_INTERNAL_ERROR;
        }
        moq_reject_track_status_cfg_t rej;
        moq_reject_track_status_cfg_init(&rej);
        rej.error_code = a->code;
        return moq_session_reject_track_status(s, h, &rej, now_us);
    }
    case MOQ_EVENT_SUBSCRIBE_REQUEST: {
        moq_reject_subscribe_cfg_t rej;
        moq_reject_subscribe_cfg_init(&rej);
        rej.error_code = a->code;
        return moq_session_reject_subscribe(s, (moq_subscription_t){ a->handle },
                                            &rej, now_us);
    }
    case MOQ_EVENT_PUBLISH_REQUEST: {
        moq_reject_publish_cfg_t rej;
        moq_reject_publish_cfg_init(&rej);
        rej.error_code = a->code;
        return moq_session_reject_publish(s, (moq_publication_t){ a->handle },
                                          &rej, now_us);
    }
    case MOQ_EVENT_NAMESPACE_PUBLISHED: {
        moq_reject_namespace_cfg_t rej;
        moq_reject_namespace_cfg_init(&rej);
        rej.error_code = a->code;
        return moq_session_reject_namespace(s, (moq_announcement_t){ a->handle },
                                            &rej, now_us);
    }
    case MOQ_EVENT_NS_SUB_REQUEST: {
        moq_reject_ns_sub_cfg_t rej;
        moq_reject_ns_sub_cfg_init(&rej);
        rej.error_code = a->code;
        return moq_session_reject_ns_sub(s, (moq_ns_sub_handle_t){ a->handle },
                                         &rej, now_us);
    }
    case MOQ_EVENT_SUBSCRIBE_TRACKS_REQUEST: {
        moq_reject_subscribe_tracks_cfg_t rej;
        moq_reject_subscribe_tracks_cfg_init(&rej);
        rej.error_code = a->code;
        return moq_session_reject_subscribe_tracks(
            s, (moq_track_sub_handle_t){ a->handle }, &rej, now_us);
    }
    default:
        return MOQ_OK;
    }
}

/* Retry owed answers in arrival order. Only WOULD_BLOCK keeps an answer; one
 * whose request has gone is dropped (there is nothing left to answer), and
 * one the session refuses outright is dropped and reported. */
static moq_result_t pub_owed_drive(moq_publisher_t *pub, uint64_t now_us)
{
    pub_owed_prune(pub);
    size_t done = 0;
    moq_result_t out = MOQ_OK;
    while (done < pub->owed_count) {
        /* Answering advances the session and may retire another owner. */
        if (!pub_answer_pending(pub, &pub->owed[done])) { done++; continue; }
        moq_result_t rc = pub_answer_request(pub, &pub->owed[done], now_us);
        if (rc == MOQ_ERR_WOULD_BLOCK) {
            if (!pub_answer_pending(pub, &pub->owed[done])) { done++; continue; }
            if (out == MOQ_OK) out = rc;
            break;
        }
        if (rc == MOQ_ERR_CLOSED) { done = pub->owed_count; out = rc; break; }
        if (!pub_answer_settled(rc) && out == MOQ_OK) out = rc;
        done++;
    }
    if (done > 0) {
        memmove(pub->owed, pub->owed + done,
                (pub->owed_count - done) * sizeof(pub->owed[0]));
        pub->owed_count -= done;
    }
    pub_owed_prune(pub);
    return out;
}

/* Answer now, or hold the answer when the session cannot take it yet (also
 * when earlier answers are still held, so answers keep their order). Sets
 * *held when it was held. Each exact kind/handle owns one immutable decision
 * and FIFO position. The fixed six-pool sum bounds all eligible owners. */
static moq_result_t pub_answer_or_owe(moq_publisher_t *pub, const pub_owed_answer_t *in,
                                      uint64_t now_us, bool *held)
{
    *held = false;
    pub_owed_prune(pub);
    if (!pub_answer_pending(pub, in)) return MOQ_OK;
    for (size_t i = 0; i < pub->owed_count; i++) {
        if (pub->owed[i].kind == in->kind && pub->owed[i].handle == in->handle) {
            *held = true;
            return MOQ_OK;
        }
    }
    pub_owed_answer_t cur = *in;
    if (pub->owed_count == 0) {
        moq_result_t rc = pub_answer_request(pub, &cur, now_us);
        if (rc != MOQ_ERR_WOULD_BLOCK)
            return pub_answer_settled(rc) ? MOQ_OK : rc;
        /* The failed attempt still advanced the session. */
        if (!pub_answer_pending(pub, &cur)) return MOQ_OK;
    }
    if (pub->owed_count >= pub->owed_cap) return MOQ_ERR_INTERNAL;
    pub->owed[pub->owed_count++] = cur;
    *held = true;
    return MOQ_OK;
}

/* Tick mode is the session's only consumer: a request nothing took is
 * answered here instead of being left open. A SUBSCRIBE or TRACK_STATUS
 * reached this point because the track is not this publisher's; every other
 * kind is one the facade does not serve (draft-22 §1.6: "Limited endpoints
 * SHOULD respond to any unsupported messages with the appropriate
 * NOT_SUPPORTED error code"). */
static moq_result_t pub_refuse_unserved(moq_publisher_t *pub, const moq_event_t *ev,
                                        uint64_t now_us)
{
    pub_owed_answer_t a = { .kind = ev->kind, .handle = 0, .track = NULL,
                            .code = MOQ_REQUEST_ERROR_NOT_SUPPORTED };
    switch (ev->kind) {
    case MOQ_EVENT_SUBSCRIBE_REQUEST:
        a.handle = ev->u.subscribe_request.sub._opaque;
        a.code = MOQ_REQUEST_ERROR_DOES_NOT_EXIST;
        break;
    case MOQ_EVENT_TRACK_STATUS_REQUEST:
        a.handle = ev->u.track_status_request.handle._opaque;
        a.code = MOQ_REQUEST_ERROR_DOES_NOT_EXIST;
        break;
    case MOQ_EVENT_PUBLISH_REQUEST:
        a.handle = ev->u.publish_request.pub._opaque;
        break;
    case MOQ_EVENT_NAMESPACE_PUBLISHED:
        a.handle = ev->u.namespace_published.ann._opaque;
        break;
    case MOQ_EVENT_NS_SUB_REQUEST:
        a.handle = ev->u.ns_sub_request.handle._opaque;
        break;
    case MOQ_EVENT_SUBSCRIBE_TRACKS_REQUEST:
        a.handle = ev->u.subscribe_tracks_request.handle._opaque;
        break;
    default:
        return MOQ_OK;
    }
    bool held;
    return pub_answer_or_owe(pub, &a, now_us, &held);
}


/* Finish the pending fetch: release the snapshot refs and clear the slot. */
static void pending_fetch_clear(moq_publisher_t *pub, pub_pending_fetch_t *pf)
{
    if (pf->active) {
        pub_fetch_owners_t *owners = &pub->fetch_owners;
        pub_fetch_owner_t *cell = &owners->cells[moq_handle_slot(pf->fetch._opaque)];
        if (cell->prev != PUB_FETCH_NONE)
            owners->cells[cell->prev].next = cell->next;
        else owners->head = cell->next;
        if (cell->next != PUB_FETCH_NONE)
            owners->cells[cell->next].prev = cell->prev;
        else owners->tail = cell->prev;
    }
    pub_retained_free(&pub->alloc, pf->objs, pf->obj_count);
    memset(pf, 0, sizeof(*pf));
}

static void pending_fetches_clear(moq_publisher_t *pub)
{
    while (pub->fetch_owners.head != PUB_FETCH_NONE)
        pending_fetch_clear(pub,
            &pub->fetch_owners.cells[pub->fetch_owners.head].pending);
}

/* Answer one spec FETCH from the snapshotted retained GROUP: FETCH_OK (End
 * Location = last object + 1), then each object 0..N as a FETCH_OBJECT in order,
 * then FIN. Phased and capacity-gated so a full
 * action queue defers to a later tick rather than leaving a half-served fetch;
 * the accepted flag + next_idx cursor make every retry idempotent (no duplicate
 * or skipped object). A reject is only ever issued BEFORE accept. The objects
 * are served from the snapshot, so a clear/end/remove of the source track after
 * staging does not affect the response. */
static moq_result_t serve_retained_fetch(moq_publisher_t *pub,
                                         pub_pending_fetch_t *pf, uint64_t now_us)
{
    if (!pf->active) return MOQ_OK;
    bool d22 = moq_session_supports_fill(pub->session);
    /* Cancellation must release references even with no action credit. The
     * resolver checks pool, session, slot and generation, without advancing. */
    if (fetch_resolve_handle(pub->session, pf->fetch) < 0) {
        pending_fetch_clear(pub, pf);
        return MOQ_OK;
    }
    /* Independent FETCH owners may progress, but a Joining FETCH's answer
     * cannot overtake an unanswered update of its own parent. */
    if (!pf->accepted) {
        int sub = sub_resolve_handle(pub->session, pf->joining_sub);
        int publication = pub_resolve_handle(pub->session, pf->joining_pub);
        if ((sub >= 0 && pub->session->subs[sub].owed_update.owed) ||
            (publication >= 0 && pub->session->publishes[publication].owed_update.owed))
            return MOQ_ERR_WOULD_BLOCK;
    }

    /* The reject flag, not the code, decides: INTERNAL_ERROR is code zero
     * on every draft. */
    if (!pf->accepted && pf->reject) {
        moq_reject_fetch_cfg_t rej;
        moq_reject_fetch_cfg_init(&rej);
        rej.error_code = pf->reject_code;
        moq_result_t rc = moq_session_reject_fetch(pub->session, pf->fetch,
                                                   &rej, now_us);
        if (rc == MOQ_ERR_WOULD_BLOCK) return rc;
        if (d22 && rc < 0 && rc != MOQ_ERR_STALE_HANDLE && rc != MOQ_ERR_CLOSED)
            return rc;   /* still owns an unanswered request */
        pending_fetch_clear(pub, pf);
        return d22 && rc == MOQ_ERR_CLOSED ? rc : MOQ_OK;
    }

    if (!pf->accepted) {
        /* FETCH_OK + FETCH_HEADER, and the data stream's scheduling key. */
        size_t need = pub->session->data_priority_updates ? 3 : 2;
        if (moq_session_action_capacity(pub->session) < need) return MOQ_ERR_WOULD_BLOCK;
        moq_accept_fetch_cfg_t acc;
        moq_accept_fetch_cfg_init(&acc);
        acc.end_of_track = false;
        acc.end_group = pf->group_id;
        acc.end_object = pf->end_object;   /* End Location: last object + 1 */
        if (pf->track)
            acc.track_properties = (moq_bytes_t){ pf->track->props, pf->track->props_len };
        moq_result_t rc = moq_session_accept_fetch(pub->session, pf->fetch,
                                                   &acc, now_us);
        if (rc == MOQ_ERR_WOULD_BLOCK) return rc;
        if (rc < 0) {
            if (d22 && rc != MOQ_ERR_STALE_HANDLE && rc != MOQ_ERR_CLOSED)
                return rc;
            pending_fetch_clear(pub, pf);
            return d22 && rc == MOQ_ERR_CLOSED ? rc : MOQ_OK;
        }
        pf->accepted = true;
        /* An early refused UPDATE can terminate the FETCH at accept commit. */
        if (d22 && fetch_resolve_handle(pub->session, pf->fetch) < 0) {
            pending_fetch_clear(pub, pf);
            return MOQ_OK;
        }
    }

    /* Accepted: write each remaining object from the cursor, then FIN. Each
     * write costs 2 actions with properties, 1 without; reserve per object so a
     * deferred write resumes at the same cursor (no duplicate/skip). */
    while (pf->next_idx < pf->obj_count) {
        const pub_retained_obj_t *o = &pf->objs[pf->next_idx];
        size_t need = (o->properties ? 2u : 1u);
        if (moq_session_action_capacity(pub->session) < need) return MOQ_ERR_WOULD_BLOCK;
        moq_fetch_object_cfg_t oc;
        moq_fetch_object_cfg_init(&oc);
        oc.group_id = pf->group_id;
        oc.subgroup_id = o->subgroup_id;
        oc.object_id = o->object_id;
        oc.publisher_priority = pf->priority;
        oc.payload = o->payload;
        oc.properties = o->properties;
        moq_result_t rc = moq_session_write_fetch_object(pub->session, pf->fetch,
                                                         &oc, now_us);
        if (rc == MOQ_ERR_WOULD_BLOCK) return rc;
        if (rc < 0) {
            if (d22 && rc != MOQ_ERR_STALE_HANDLE && rc != MOQ_ERR_CLOSED)
                return rc;
            (void)moq_session_end_fetch(pub->session, pf->fetch, now_us);
            pending_fetch_clear(pub, pf);
            return d22 && rc == MOQ_ERR_CLOSED ? rc : MOQ_OK;
        }
        pf->next_idx++;
    }
    if (moq_session_action_capacity(pub->session) < 1) return MOQ_ERR_WOULD_BLOCK;
    moq_result_t rc = moq_session_end_fetch(pub->session, pf->fetch, now_us);
    if (rc == MOQ_ERR_WOULD_BLOCK) return rc;
    if (d22 && rc < 0 && rc != MOQ_ERR_STALE_HANDLE && rc != MOQ_ERR_CLOSED)
        return rc;
    pending_fetch_clear(pub, pf);
    return d22 && rc == MOQ_ERR_CLOSED ? rc : MOQ_OK;
}

/* One arrival-ordered pass, including later independent owners when an
 * earlier one waits for event or drain credit. No polling or retry loop. */
static moq_result_t serve_retained_fetches(moq_publisher_t *pub, uint64_t now_us)
{
    pub_fetch_owners_t *owners = &pub->fetch_owners;
    size_t remaining = owners->cap;
    uint32_t last = owners->tail;
    for (uint32_t i = owners->head; i != PUB_FETCH_NONE && remaining--;) {
        uint32_t next = owners->cells[i].next;
        moq_result_t rc = serve_retained_fetch(pub, &owners->cells[i].pending, now_us);
        if (rc < 0 && rc != MOQ_ERR_WOULD_BLOCK) return rc;
        if (i == last) break;
        i = next;
    }
    return owners->head == PUB_FETCH_NONE ? MOQ_OK : MOQ_ERR_WOULD_BLOCK;
}

/* Finish one pending fill: release the snapshot refs and clear the slot. */
static void pending_fill_clear(moq_publisher_t *pub, pub_pending_fill_t *pf)
{
    for (size_t i = 0; i < pf->obj_count; i++) {
        moq_rcbuf_decref(pf->objs[i].payload);
        moq_rcbuf_decref(pf->objs[i].properties);
    }
    pub->fill_bytes -= pf->charge;
    memset(pf, 0, sizeof(*pf));
}

static void pending_fills_clear_all(moq_publisher_t *pub)
{
    for (size_t i = 0; i < pub->pending_fill_cap; i++)
        if (pub->pending_fills[i].active)
            pending_fill_clear(pub, &pub->pending_fills[i]);
}

static pub_pending_fill_t *pending_fill_alloc(moq_publisher_t *pub)
{
    for (size_t i = 0; i < pub->pending_fill_cap; i++)
        if (!pub->pending_fills[i].active)
            return &pub->pending_fills[i];
    return NULL;
}

static pub_pending_fill_t *pending_fill_find(moq_publisher_t *pub, moq_fill_t fill)
{
    for (size_t i = 0; i < pub->pending_fill_cap; i++)
        if (pub->pending_fills[i].active &&
            moq_fill_eq(pub->pending_fills[i].fill, fill))
            return &pub->pending_fills[i];
    return NULL;
}

static bool pending_fill_work(const pub_pending_fill_t *pf)
{
    return pf->active && pf->requested && !pf->terminal_queued &&
           (!pf->deferred || pf->abort);
}

static bool pending_fills_active(const moq_publisher_t *pub)
{
    for (size_t i = 0; i < pub->pending_fill_cap; i++)
        if (pending_fill_work(&pub->pending_fills[i])) return true;
    return false;
}

static moq_result_t pending_fill_reserve(moq_publisher_t *pub, moq_pub_track_t *track,
                                         pub_pending_fill_t **out)
{
    if (*out) return MOQ_OK; /* ACK retries never read the mutable cache again. */
    if (pub->session->action_cap < 3 || pub->session->event_cap < 2)
        return MOQ_ERR_BUFFER;
    pub_pending_fill_t *pf = pending_fill_alloc(pub);
    if (!pf) return MOQ_ERR_WOULD_BLOCK;
    uint64_t cost = 0;
    bool permanent = false;
    for (size_t i = 0; i < track->retained_count; i++) {
        uint64_t payload = moq_rcbuf_len(track->retained[i].payload);
        uint64_t props = moq_rcbuf_len(track->retained[i].properties);
        if (payload > UINT64_MAX - cost || props > UINT64_MAX - cost - payload) {
            permanent = true;
            break;
        }
        cost += payload + props;
    }
    if (cost > pub->cfg.max_fill_snapshot_bytes) permanent = true;
    if (!permanent && cost > pub->cfg.max_fill_snapshot_bytes - pub->fill_bytes)
        return MOQ_ERR_WOULD_BLOCK;
    uint64_t identity;
    if (!sub_fill_identity_next(&pub_fill_identities, &identity)) return MOQ_ERR_LIMIT;
    pf->active = true;
    pf->token._opaque = identity;
    pf->track = track;
    pf->group_id = track->retained_group_id;
    pf->priority = track->priority;
    if (permanent) {
        pf->abort = true;
        pf->abort_code = MOQ_STREAM_ERROR_INTERNAL_ERROR;
    } else {
        pf->obj_count = track->retained_count;
        for (size_t i = 0; i < pf->obj_count; i++) {
            pf->objs[i] = track->retained[i];
            moq_rcbuf_incref(pf->objs[i].payload);
            moq_rcbuf_incref(pf->objs[i].properties);
        }
        pf->charge = cost;
        pub->fill_bytes += cost;
    }
    *out = pf;
    return MOQ_OK;
}

static void pending_fill_bind(moq_publisher_t *pub, pub_pending_fill_t *pf,
                              const moq_pub_fill_receipt_t *receipt)
{
    if (!pf) return;
    if (receipt->outcome != 2) {
        pending_fill_clear(pub, pf);
        return;
    }
    pf->receipt = receipt->request;
    pf->fill = receipt->request.fill;
}

/* Answer one fill from its snapshot: the fill fetch stream's header, each
 * retained object in order, then a FIN -- or, for a request the facade
 * cannot serve, the header followed by RESET (the session opens an unopened
 * child before resetting it). Capacity-gated and phased like
 * serve_retained_fetch so a full action queue defers to a later tick without
 * a duplicate or skipped object. A child the session already ended (its
 * parent cancelled, say) is stale here and simply released. */
static void serve_pending_fill(moq_publisher_t *pub, pub_pending_fill_t *pf,
                               uint64_t now_us)
{
    if (!pending_fill_work(pf)) return;

    if (pf->abort) {
        moq_result_t rc = moq_session_abort_fill(pub->session, pf->fill,
                                                 pf->abort_code, now_us);
        if (rc == MOQ_ERR_WOULD_BLOCK) return;   /* retry the abort */
        pf->terminal_queued = true;
        return;
    }

    if (!pf->opened) {
        moq_result_t rc = moq_session_open_fill(pub->session, pf->fill, now_us);
        if (rc == MOQ_ERR_WOULD_BLOCK) return;   /* defer; not opened */
        if (rc == MOQ_ERR_STALE_HANDLE || rc == MOQ_ERR_CLOSED) {
            pf->terminal_queued = true;
            return;
        }
        if (rc < 0) {
            pf->abort = true;
            pf->abort_code = MOQ_STREAM_ERROR_INTERNAL_ERROR;
            return;
        }
        pf->opened = true;
    }

    while (pf->next_idx < pf->obj_count &&
           pf->objs[pf->next_idx].object_id <= pf->receipt.end_object) {
        const pub_retained_obj_t *o = &pf->objs[pf->next_idx];
        size_t need = (o->properties ? 2u : 1u);
        if (moq_session_action_capacity(pub->session) < need) return;  /* defer */
        moq_fetch_object_cfg_t oc;
        moq_fetch_object_cfg_init(&oc);
        oc.group_id = pf->group_id;
        oc.subgroup_id = o->subgroup_id;
        oc.object_id = o->object_id;
        oc.publisher_priority = pf->priority;
        oc.payload = o->payload;
        oc.properties = o->properties;
        moq_result_t rc = moq_session_write_fill_object(pub->session, pf->fill,
                                                        &oc, now_us);
        if (rc == MOQ_ERR_WOULD_BLOCK) return;   /* resume at cursor */
        if (rc < 0) {
            /* A half-written fill is never finished as complete: fail it. */
            pf->abort = true;
            pf->abort_code = MOQ_STREAM_ERROR_INTERNAL_ERROR;
            if (rc == MOQ_ERR_STALE_HANDLE || rc == MOQ_ERR_CLOSED)
                pf->terminal_queued = true;
            return;
        }
        pf->next_idx++;
    }
    if (moq_session_action_capacity(pub->session) < 1) return;   /* defer the FIN */
    moq_result_t rc = moq_session_end_fill(pub->session, pf->fill, now_us);
    if (rc == MOQ_ERR_WOULD_BLOCK) return;
    if (rc == MOQ_OK || rc == MOQ_ERR_STALE_HANDLE || rc == MOQ_ERR_CLOSED)
        pf->terminal_queued = true;
    else {
        pf->abort = true;
        pf->abort_code = MOQ_STREAM_ERROR_INTERNAL_ERROR;
    }
}

static void serve_pending_fills(moq_publisher_t *pub, uint64_t now_us)
{
    for (size_t i = 0; i < pub->pending_fill_cap; i++)
        serve_pending_fill(pub, &pub->pending_fills[i], now_us);
}

static moq_result_t pub_fill_lookup(moq_publisher_t *pub, moq_pub_fill_t fill,
                                    pub_pending_fill_t **out)
{
    if (!pub) return MOQ_ERR_INVAL;
    if (pub->in_fill_callback) return MOQ_ERR_WRONG_STATE;
    for (size_t i = 0; i < pub->pending_fill_cap; i++) {
        pub_pending_fill_t *pf = &pub->pending_fills[i];
        if (!pf->active || !moq_pub_fill_eq(pf->token, fill)) continue;
        if (pf->terminal_queued) return MOQ_ERR_STALE_HANDLE;
        if (!pf->requested || !pf->deferred) return MOQ_ERR_WRONG_STATE;
        *out = pf;
        return MOQ_OK;
    }
    return MOQ_ERR_STALE_HANDLE;
}

moq_result_t moq_pub_open_fill(moq_publisher_t *pub, moq_pub_fill_t fill, uint64_t now_us)
{
    pub_pending_fill_t *pf;
    moq_result_t rc = pub_fill_lookup(pub, fill, &pf);
    if (rc < 0) return rc;
    if (pf->abort) return MOQ_ERR_WRONG_STATE;
    rc = moq_session_open_fill(pub->session, pf->fill, now_us);
    if (rc == MOQ_OK) pf->opened = true;
    return rc;
}

moq_result_t moq_pub_write_fill_object(moq_publisher_t *pub, moq_pub_fill_t fill,
    const moq_fetch_object_cfg_t *cfg, uint64_t now_us)
{
    pub_pending_fill_t *pf;
    moq_result_t rc = pub_fill_lookup(pub, fill, &pf);
    if (rc < 0) return rc;
    if (pf->abort) return MOQ_ERR_WRONG_STATE;
    return moq_session_write_fill_object(pub->session, pf->fill, cfg, now_us);
}

moq_result_t moq_pub_write_fill_range(moq_publisher_t *pub, moq_pub_fill_t fill,
    moq_fetch_range_kind_t kind, uint64_t group_id, uint64_t object_id, uint64_t now_us)
{
    pub_pending_fill_t *pf;
    moq_result_t rc = pub_fill_lookup(pub, fill, &pf);
    if (rc < 0) return rc;
    if (pf->abort) return MOQ_ERR_WRONG_STATE;
    return moq_session_write_fill_range(pub->session, pf->fill, kind,
                                         group_id, object_id, now_us);
}

moq_result_t moq_pub_end_fill(moq_publisher_t *pub, moq_pub_fill_t fill, uint64_t now_us)
{
    pub_pending_fill_t *pf;
    moq_result_t rc = pub_fill_lookup(pub, fill, &pf);
    if (rc < 0) return rc;
    if (pf->abort) return MOQ_ERR_WRONG_STATE;
    rc = moq_session_end_fill(pub->session, pf->fill, now_us);
    if (rc == MOQ_OK || rc == MOQ_ERR_STALE_HANDLE) pf->terminal_queued = true;
    return rc;
}

moq_result_t moq_pub_abort_fill(moq_publisher_t *pub, moq_pub_fill_t fill,
                                uint64_t error_code, uint64_t now_us)
{
    pub_pending_fill_t *pf;
    moq_result_t rc = pub_fill_lookup(pub, fill, &pf);
    if (rc < 0) return rc;
    if (pf->abort && pf->abort_code != error_code) return MOQ_ERR_WRONG_STATE;
    rc = moq_session_abort_fill(pub->session, pf->fill, error_code, now_us);
    if (rc == MOQ_OK || rc == MOQ_ERR_STALE_HANDLE) pf->terminal_queued = true;
    if (rc == MOQ_ERR_WOULD_BLOCK) {
        pf->abort = true;
        pf->abort_code = error_code;
    }
    return rc;
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


/* A removed track can no longer be stated: an owed TRACK_STATUS answer for it
 * becomes DOES_NOT_EXIST, and a retained FETCH still to be answered carries no
 * Track Properties. */
static void pub_forget_track(moq_publisher_t *pub, const moq_pub_track_t *t)
{
    for (size_t i = 0; i < pub->owed_count; i++) {
        if (pub->owed[i].track != t) continue;
        pub->owed[i].track = NULL;
        pub->owed[i].code = MOQ_REQUEST_ERROR_DOES_NOT_EXIST;
    }
    if (pub->fetch_owners.cells) {
        for (size_t i = 0; i < pub->fetch_owners.cap; i++)
            if (pub->fetch_owners.cells[i].pending.track == t)
                pub->fetch_owners.cells[i].pending.track = NULL;
    }
}

static void free_track_state(moq_publisher_t *pub, moq_pub_track_t *t) {
    pub_forget_track(pub, t);
    facade_migration_clear(&t->migration, &pub->alloc);
    if (t->props) pub_free(pub, t->props, t->props_cap);
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

static moq_result_t pending_update_apply(moq_publisher_t *pub,
                                         const pub_pending_t *p, uint64_t now_us)
{
    moq_pub_track_t *t = p->track;
    const moq_pub_subscribe_update_info_t *u = &p->update_info;
    bool publication = moq_publication_is_valid(p->publication);
    pub_sub_slot_t *sl = publication ? track_find_publication_slot(t)
                                    : track_find_slot_by_sub(t, p->sub);
    if (sl && u->has_forward) {
        bool prior = publication ? t->publish_forward : sl->forward;
        if (!u->forward && prior) slot_forward_drop(t, sl);
        if (publication) t->publish_forward = u->forward;
        else sl->forward = u->forward;
    }
    if (sl && p->has_filter) {
        if (publication) slot_install_pub_window(pub, sl);
        else slot_install_sub_window(pub, sl);
        slot_window_cut_check(t, sl);
    }
    if (publication) {
        if (u->has_forward && CB_HAS(&pub->callbacks, on_publish_forward_changed))
            pub->callbacks.on_publish_forward_changed(pub->callbacks.ctx, t, u->forward);
    } else if (CB_HAS(&pub->callbacks, on_subscriber_updated)) {
        pub->callbacks.on_subscriber_updated(pub->callbacks.ctx, t, u);
    }
    moq_result_t rc = track_run_retires(pub, t, now_us);
    if (rc < 0 && rc != MOQ_ERR_STALE_HANDLE) return rc;
    return track_run_completions(pub, t, now_us);
}

static bool admission_live(moq_publisher_t *pub, uint64_t parent,
                           uint32_t flags, uint64_t update_id)
{
    const moq_owed_update_t *owed;
    if (flags & PUB_ADMISSION_PUBLICATION) {
        int slot = pub_resolve_handle(pub->session, (moq_publication_t){parent});
        if (slot < 0) return false;
        const moq_pub_entry_t *e = &pub->session->publishes[slot];
        if (e->role != MOQ_PUB_ROLE_PUBLISHER || e->state != MOQ_PUB_ESTABLISHED)
            return false;
        owed = &e->owed_update;
    } else {
        int slot = sub_resolve_handle(pub->session, (moq_subscription_t){parent});
        if (slot < 0) return false;
        const moq_sub_entry_t *e = &pub->session->subs[slot];
        if (e->role != MOQ_SUB_ROLE_PUBLISHER) return false;
        if (!(flags & PUB_ADMISSION_UPDATE))
            return e->state == MOQ_SUB_PENDING_PUBLISHER;
        if (e->state != MOQ_SUB_ESTABLISHED) return false;
        owed = &e->owed_update;
    }
    return owed->owed && owed->rec.request_id == update_id;
}

/* A later cumulative legacy UPDATE still has to reach the FIFO dispatcher.
 * Keep the parent owner, but never ACK its old ID or apply an old snapshot. */
static bool admission_update_waits(moq_publisher_t *pub, const pub_admission_t *a)
{
    if (pub->session->profile->request_updates_max == 1 || !(a->flags & PUB_ADMISSION_UPDATE))
        return false;
    const moq_owed_update_t *owed;
    if (a->flags & PUB_ADMISSION_PUBLICATION) {
        int i = pub_resolve_handle(pub->session, (moq_publication_t){a->parent});
        if (i < 0 || pub->session->publishes[i].role != MOQ_PUB_ROLE_PUBLISHER ||
            pub->session->publishes[i].state != MOQ_PUB_ESTABLISHED) return false;
        owed = &pub->session->publishes[i].owed_update;
    } else {
        int i = sub_resolve_handle(pub->session, (moq_subscription_t){a->parent});
        if (i < 0 || pub->session->subs[i].role != MOQ_SUB_ROLE_PUBLISHER ||
            pub->session->subs[i].state != MOQ_SUB_ESTABLISHED) return false;
        owed = &pub->session->subs[i].owed_update;
    }
    return owed->owed && owed->rec.request_id != a->update_id;
}

static pub_admission_t *admission_cell(moq_publisher_t *pub, uint64_t parent,
                                      bool publication)
{
    size_t slot = moq_handle_slot(parent);
    if (slot >= (publication ? pub->session->pub_cap : pub->session->sub_cap))
        return NULL;
    if (publication) slot += pub->session->sub_cap;
    return &pub->admissions.cells[slot];
}

static void admission_clear(moq_publisher_t *pub, pub_admission_t *a)
{
    pub_admissions_t *q = &pub->admissions;
    if (a->prev != PUB_ADMISSION_NONE) q->cells[a->prev].next = a->next;
    else q->head = a->next;
    if (a->next != PUB_ADMISSION_NONE) q->cells[a->next].prev = a->prev;
    else q->tail = a->prev;
    /* Bound jobs have already been detached at ACK commit. */
    if (a->fill_job) pending_fill_clear(pub, a->fill_job);
    if ((a->flags & PUB_ADMISSION_DEFERRED) && pub->deferred.active &&
        pub->deferred.sub._opaque == a->parent)
        pub->deferred.active = false;
    memset(a, 0, sizeof(*a));
}

static bool admission_cancel(moq_publisher_t *pub, uint64_t parent, bool publication)
{
    pub_admission_t *a = admission_cell(pub, parent, publication);
    if (!a || !(a->flags & PUB_ADMISSION_ACTIVE) || a->parent != parent)
        return false;
    admission_clear(pub, a);
    return true;
}

static void admissions_clear(moq_publisher_t *pub)
{
    while (pub->admissions.head != PUB_ADMISSION_NONE)
        admission_clear(pub, &pub->admissions.cells[pub->admissions.head]);
}

/* Claim only a live obligation. Replayed/stale events never authorize, and a
 * new generation can reclaim its cell without touching an old bound child. */
static moq_result_t admission_claim(moq_publisher_t *pub, uint64_t parent,
    uint32_t flags, uint64_t update_id, pub_admission_t **out, bool *fresh)
{
    *out = NULL;
    *fresh = false;
    pub_admission_t *a = admission_cell(pub, parent,
                                        (flags & PUB_ADMISSION_PUBLICATION) != 0);
    if (!a) return MOQ_OK;
    if ((a->flags & PUB_ADMISSION_ACTIVE) &&
        !admission_live(pub, a->parent, a->flags, a->update_id) &&
        !admission_update_waits(pub, a))
        admission_clear(pub, a);
    if (!admission_live(pub, parent, flags, update_id)) return MOQ_OK;
    if (a->flags & PUB_ADMISSION_ACTIVE) {
        if (pub->session->profile->request_updates_max != 1 && a->parent == parent &&
            (a->flags & flags & PUB_ADMISSION_UPDATE) && a->update_id != update_id) {
            /* The event contains the session's cumulative scalar projection.
             * Legacy has no fill reservation to detach or borrowed spans. */
            a->update_id = update_id;
            a->flags = flags | PUB_ADMISSION_ACTIVE;
            *fresh = true;
            *out = a;
            return MOQ_OK;
        }
        if (a->parent != parent || a->update_id != update_id ||
            ((a->flags ^ flags) & PUB_ADMISSION_UPDATE))
            return MOQ_ERR_WRONG_STATE;
        *out = a;
        return MOQ_OK;
    }
    pub_admissions_t *q = &pub->admissions;
    uint32_t index = (uint32_t)(a - q->cells);
    a->parent = parent;
    a->update_id = update_id;
    a->flags = flags | PUB_ADMISSION_ACTIVE;
    a->prev = q->tail;
    a->next = PUB_ADMISSION_NONE;
    if (q->tail != PUB_ADMISSION_NONE) q->cells[q->tail].next = index;
    else q->head = index;
    q->tail = index;
    *fresh = true;
    *out = a;
    return MOQ_OK;
}

static moq_result_t flush_admission(moq_publisher_t *pub, pub_admission_t *a,
                                     uint64_t now_us)
{
    if (!admission_live(pub, a->parent, a->flags, a->update_id)) {
        if (admission_update_waits(pub, a)) return MOQ_ERR_WOULD_BLOCK;
        admission_clear(pub, a);
        return MOQ_OK;
    }
    if (a->flags & PUB_ADMISSION_DEFERRED) return MOQ_ERR_WOULD_BLOCK;
    bool update = (a->flags & PUB_ADMISSION_UPDATE) != 0;
    bool publication = (a->flags & PUB_ADMISSION_PUBLICATION) != 0;
    if (!update && (a->flags & PUB_ADMISSION_ACCEPT) && a->track->ended) {
        a->flags &= ~PUB_ADMISSION_ACCEPT;
        a->reject_code = MOQ_REQUEST_ERROR_DOES_NOT_EXIST;
    }
    if (!update && (a->flags & PUB_ADMISSION_ACCEPT) &&
        !moq_session_supports_concurrent_subscriptions(pub->session) && track_has_subscriber(a->track)) {
        a->flags &= ~PUB_ADMISSION_ACCEPT;
        a->reject_code = MOQ_REQUEST_ERROR_DUPLICATE_SUBSCRIPTION;
    }
    bool accept = (a->flags & PUB_ADMISSION_ACCEPT) != 0;
    moq_result_t rc;
    if (accept && !update) {
        /* No intervening poll/callback before ACK + slot installation. Many
         * queued decisions must not promise the same destination slot. */
        rc = track_reserve_sub_slots(pub, a->track);
        if (rc < 0) return rc;
    }
    if (accept && (a->flags & PUB_ADMISSION_FILL)) {
        rc = pending_fill_reserve(pub, a->track, &a->fill_job);
        if (rc < 0) return rc;
    }
    moq_pub_fill_receipt_t receipt = {0};
    if (update) {
        moq_accept_subscription_update_cfg_t acc;
        moq_accept_subscription_update_cfg_init_sized(&acc, sizeof(acc));
        acc.update_request_id = a->update_id;
        rc = publication
            ? session_accept_publication_update_receipt(pub->session,
                (moq_publication_t){a->parent}, &acc, now_us, &receipt)
            : session_accept_subscription_update_receipt(pub->session,
                (moq_subscription_t){a->parent}, &acc, now_us, &receipt);
    } else if (accept) {
        moq_accept_subscribe_cfg_t acc;
        pub_init_accept_cfg(a->track, &acc);
        rc = session_accept_subscribe_receipt(pub->session,
                (moq_subscription_t){a->parent}, &acc, now_us, &receipt);
    } else {
        moq_reject_subscribe_cfg_t rej;
        moq_reject_subscribe_cfg_init(&rej);
        rej.error_code = a->reject_code;
        rc = moq_session_reject_subscribe(pub->session,
                (moq_subscription_t){a->parent}, &rej, now_us);
    }
    if (rc < 0) return rc; /* Retain the decision and immutable reservation. */
    if (a->fill_job && accept) {
        pending_fill_bind(pub, a->fill_job, &receipt);
        a->fill_job = NULL; /* Only real terminal dispatch releases a bound job. */
    }
    moq_pub_track_t *t = a->track;
    pub_pending_t committed = {
        .track = t, .sub = {publication ? 0 : a->parent},
        .publication = {publication ? a->parent : 0},
        .has_filter = (a->flags & PUB_ADMISSION_FILTER) != 0,
        .update_info = {
            .has_subscriber_priority = (a->flags & PUB_ADMISSION_HAS_PRIORITY) != 0,
            .subscriber_priority = (uint8_t)a->priority,
            .has_forward = (a->flags & PUB_ADMISSION_HAS_FORWARD) != 0,
            .forward = (a->flags & PUB_ADMISSION_FORWARD) != 0,
            .has_delivery_timeout = (a->flags & PUB_ADMISSION_HAS_TIMEOUT) != 0,
            .delivery_timeout_us = a->delivery_timeout_us
        }
    };
    if (accept && !update)
        track_set_subscriber(pub, t, (moq_subscription_t){a->parent},
                              (a->flags & PUB_ADMISSION_FORWARD) != 0);
    admission_clear(pub, a);
    if (update) return pending_update_apply(pub, &committed, now_us);
    if (accept) {
        if (pub->callbacks.on_subscriber_joined)
            pub->callbacks.on_subscriber_joined(pub->callbacks.ctx, t);
        return track_run_completions(pub, t, now_us);
    }
    return MOQ_OK;
}

static moq_result_t flush_admissions(moq_publisher_t *pub, uint64_t now_us)
{
    pub_admissions_t *q = &pub->admissions;
    uint32_t last = q->tail;
    size_t remaining = q->cap;
    moq_result_t result = MOQ_OK;
    /* Visit this pass's occupied list once, oldest first. A credit waiter or
     * unresolved authorizer must not prevent a later no-fill ACK. */
    for (uint32_t i = q->head; i != PUB_ADMISSION_NONE && remaining--;) {
        pub_admission_t *a = &q->cells[i];
        if (!(a->flags & PUB_ADMISSION_ACTIVE)) break;
        uint32_t next = a->next;
        moq_result_t rc = flush_admission(pub, a, now_us);
        if (rc == MOQ_ERR_WOULD_BLOCK) result = rc;
        else if (rc < 0) return rc;
        if (i == last) break;
        i = next;
    }
    return result;
}

static moq_result_t stage_update(moq_publisher_t *pub, const pub_pending_t *p,
                                  uint64_t now_us)
{
    bool publication = moq_publication_is_valid(p->publication);
    pub_admission_t *a;
    bool fresh;
    moq_result_t rc = admission_claim(pub,
        publication ? p->publication._opaque : p->sub._opaque,
        PUB_ADMISSION_UPDATE | (publication ? PUB_ADMISSION_PUBLICATION : 0),
        p->update_id, &a, &fresh);
    if (rc < 0 || !a) return rc;
    if (fresh) {
        const moq_pub_subscribe_update_info_t *u = &p->update_info;
        a->track = p->track;
        a->flags |= PUB_ADMISSION_ACCEPT |
            (p->fill_requested ? PUB_ADMISSION_FILL : 0) |
            (p->has_filter ? PUB_ADMISSION_FILTER : 0) |
            (u->has_forward ? PUB_ADMISSION_HAS_FORWARD : 0) |
            (u->forward ? PUB_ADMISSION_FORWARD : 0) |
            (u->has_subscriber_priority ? PUB_ADMISSION_HAS_PRIORITY : 0) |
            (u->has_delivery_timeout ? PUB_ADMISSION_HAS_TIMEOUT : 0);
        a->priority = u->subscriber_priority;
        a->delivery_timeout_us = u->delivery_timeout_us;
    }
    return flush_admission(pub, a, now_us);
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
    pub->owed_count = 0;
    admissions_clear(pub);
    pending_fetches_clear(pub);   /* release retained-FETCH snapshot refs now */
    pending_fills_clear_all(pub);
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
 * retained FETCH, pending fills, and lazy Forward-0 subgroup retirements.
 * Idempotent under retry -- admission cells unlink at commit,
 * serve_retained_fetch, serve_pending_fills and track_run_retires mutate only
 * on success and re-arm on WOULD_BLOCK -- so a repeated call never doubles
 * a callback or a wire action. Shared by
 * moq_pub_flush (manual mode) and moq_pub_tick.
 */
static moq_result_t pub_progress_staged(moq_publisher_t *pub, uint64_t now_us)
{
    /* Answers already owed go first, in the order the requests came. */
    moq_result_t orc = pub_owed_drive(pub, now_us);
    if (orc == MOQ_ERR_CLOSED) return orc;
    bool owed_waiting = orc == MOQ_ERR_WOULD_BLOCK;
    /* An owed answer the session refused outright is reported once the
     * rest has progressed. */
    moq_result_t owed_err = orc < 0 && !owed_waiting ? orc : MOQ_OK;
    /* Progress held children even when the next parent waits for their credit.
     * Waiting for a real terminal is not action-queue-limited work. */
    serve_pending_fills(pub, now_us);
    moq_result_t rc = flush_admissions(pub, now_us);
    bool waiting = rc == MOQ_ERR_WOULD_BLOCK || owed_waiting;
    if (rc < 0 && rc != MOQ_ERR_WOULD_BLOCK) return rc;

    rc = serve_retained_fetches(pub, now_us);
    if (rc < 0 && rc != MOQ_ERR_WOULD_BLOCK) return rc;
    waiting |= rc == MOQ_ERR_WOULD_BLOCK;

    if (pending_fills_active(pub)) {
        serve_pending_fills(pub, now_us);
        waiting |= pending_fills_active(pub);
    }

    for (moq_pub_track_t *t = pub->tracks; t; t = t->next) {
        moq_result_t rrc = track_run_retires(pub, t, now_us);
        if (rrc == MOQ_ERR_WOULD_BLOCK) waiting = true;
        else if (rrc < 0 && rrc != MOQ_ERR_STALE_HANDLE) return rrc;
    }

    /* §10: finite-window completion, strictly AFTER the retire sweep
     * (a required RESET is never converted into completion's clean FIN). */
    for (moq_pub_track_t *t = pub->tracks; t; t = t->next) {
        moq_result_t crc = track_run_completions(pub, t, now_us);
        if (crc == MOQ_ERR_WOULD_BLOCK) waiting = true;
        else if (crc < 0 && crc != MOQ_ERR_STALE_HANDLE) return crc;
    }
    if (owed_err != MOQ_OK) return owed_err;
    return waiting ? MOQ_ERR_WOULD_BLOCK : MOQ_OK;
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

/* Older configs can have weaker alignment than the current type. Keep caller
 * storage byte-addressed; only the local current record gets typed accesses. */
static void pub_cfg_init_bytes(void *storage, size_t n)
{
    moq_pub_cfg_t cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.struct_size = (uint32_t)n;
    if (n >= offsetof(moq_pub_cfg_t, accept_mode) + sizeof(cfg.accept_mode))
        cfg.accept_mode = MOQ_PUB_REJECT_ALL;
    if (n >= MOQ_PUB_CFG_V0_SIZE)
        cfg.default_publisher_priority = 128;
    if (n > offsetof(moq_pub_cfg_t, callbacks))
        moq_pub_callbacks_init_sized(&cfg.callbacks,
                                     n - offsetof(moq_pub_cfg_t, callbacks));
    memcpy(storage, &cfg, n);
}

static bool pub_cfg_normalize(const void *storage, moq_pub_cfg_t *cfg)
{
    uint32_t size;
    memcpy(&size, storage, sizeof(size));
    if (size < MOQ_PUB_CFG_V0_SIZE) return false;
    size_t n = size < sizeof(*cfg) ? size : sizeof(*cfg);
    memset(cfg, 0, sizeof(*cfg));
    memcpy(cfg, storage, n);
    return true;
}

void moq_pub_cfg_init(moq_pub_cfg_t *cfg) {
    if (!cfg) return;
    /* Clear and stamp ONLY the frozen prefix: writing sizeof(*cfg) here would
     * overflow a caller compiled against the original (smaller) struct. Appended
     * fields (on_subscribe, on_subscribe_ctx, callbacks) stay disabled
     * (struct_size == prefix); callers that want them use
     * moq_pub_cfg_init_sized(). */
    pub_cfg_init_bytes((void *)cfg, MOQ_PUB_CFG_V0_SIZE);
}

void moq_pub_cfg_init_sized(moq_pub_cfg_t *cfg, size_t cfg_size) {
    if (!cfg) return;
    /* Clear exactly what the caller allocated, never more than this library's
     * struct knows about. An older caller passes a smaller cfg_size (we clear/
     * stamp that prefix); a newer caller's extra trailing fields are left to its
     * own initializer. Set each default only when it lies within cfg_size. */
    size_t n = cfg_size < sizeof(*cfg) ? cfg_size : sizeof(*cfg);
    if (n < sizeof(cfg->struct_size)) return;  /* too small to even stamp */
    pub_cfg_init_bytes((void *)cfg, n);
}

/* Pending fill slots when the config names none. The pool is never smaller
 * than the session's own fill pool (see moq_pub_create). */
#define PUB_DEFAULT_MAX_FILLS 16u

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

    moq_pub_cfg_t normalized;
    if (!pub_cfg_normalize((const void *)cfg, &normalized)) return MOQ_ERR_INVAL;
    cfg = &normalized;

    moq_pub_subscribe_cb on_subscribe = NULL;
    void *on_subscribe_ctx = NULL;
    if (pub_cfg_has_field(cfg, offsetof(moq_pub_cfg_t, on_subscribe),
                          sizeof(cfg->on_subscribe)))
        on_subscribe = cfg->on_subscribe;
    if (pub_cfg_has_field(cfg, offsetof(moq_pub_cfg_t, on_subscribe_ctx),
                          sizeof(cfg->on_subscribe_ctx)))
        on_subscribe_ctx = cfg->on_subscribe_ctx;

    moq_pub_subscribe_v1_cb on_subscribe_v1 = NULL;
    void *on_subscribe_v1_ctx = NULL;
    bool has_v1 = pub_cfg_has_field(cfg, offsetof(moq_pub_cfg_t, on_subscribe_v1_ctx),
                                    sizeof(cfg->on_subscribe_v1_ctx));
    /* Never install or ignore a torn authorizer block, even in a legacy mode. */
    if (cfg->struct_size > offsetof(moq_pub_cfg_t, on_subscribe_v1) && !has_v1)
        return MOQ_ERR_INVAL;
    if (has_v1) {
        on_subscribe_v1 = cfg->on_subscribe_v1;
        on_subscribe_v1_ctx = cfg->on_subscribe_v1_ctx;
    }
    if (cfg->accept_mode == MOQ_PUB_CALLBACK_V1) {
        if (!has_v1 || !on_subscribe_v1 || on_subscribe) return MOQ_ERR_INVAL;
    } else {
        if (on_subscribe_v1 || on_subscribe_v1_ctx) return MOQ_ERR_INVAL;
        if (cfg->accept_mode == MOQ_PUB_CALLBACK) {
            if (!on_subscribe) return MOQ_ERR_INVAL;
        } else if (cfg->accept_mode != MOQ_PUB_REJECT_ALL &&
                   cfg->accept_mode != MOQ_PUB_ACCEPT_ALL) {
            return MOQ_ERR_INVAL;
        }
    }

    size_t req_begin = offsetof(moq_pub_cfg_t, on_fill_request);
    size_t req_end = offsetof(moq_pub_cfg_t, on_fill_request_ctx) + sizeof(void *);
    size_t term_begin = offsetof(moq_pub_cfg_t, on_fill_terminal);
    size_t term_end = offsetof(moq_pub_cfg_t, on_fill_terminal_ctx) + sizeof(void *);
    size_t ngr_begin = offsetof(moq_pub_cfg_t, on_new_group_request);
    size_t ngr_end = offsetof(moq_pub_cfg_t, on_new_group_request_ctx) + sizeof(void *);
    if ((cfg->struct_size > req_begin && cfg->struct_size < req_end) ||
        (cfg->struct_size > term_begin && cfg->struct_size < term_end) ||
        (cfg->struct_size > ngr_begin && cfg->struct_size < ngr_end))
        return MOQ_ERR_INVAL;
    if (!moq_session_supports_fill(session) &&
        ((cfg->struct_size >= req_end && (cfg->on_fill_request || cfg->on_fill_request_ctx)) ||
         (cfg->struct_size >= term_end && (cfg->on_fill_terminal || cfg->on_fill_terminal_ctx)) ||
         (pub_cfg_has_field(cfg, offsetof(moq_pub_cfg_t, max_fill_snapshot_bytes),
                           sizeof(cfg->max_fill_snapshot_bytes)) && cfg->max_fill_snapshot_bytes)))
        return MOQ_ERR_UNSUPPORTED;

    if (moq_session_supports_fill(session) && session->profile->request_updates_max != 1)
        return MOQ_ERR_UNSUPPORTED;
    if (session->pub_cap > SIZE_MAX - session->sub_cap)
        return MOQ_ERR_LIMIT;
    size_t admission_cap = session->sub_cap + session->pub_cap;
    if (admission_cap >= PUB_ADMISSION_NONE ||
        admission_cap > SIZE_MAX / sizeof(pub_admission_t))
        return MOQ_ERR_LIMIT;
    size_t fetch_cap = session->fetch_cap;
    if (fetch_cap >= PUB_FETCH_NONE ||
        fetch_cap > SIZE_MAX / sizeof(pub_fetch_owner_t))
        return MOQ_ERR_LIMIT;
    size_t owed_cap = admission_cap;
    const size_t answer_caps[] = {
        session->ann_cap, session->ns_sub_cap, session->track_sub_cap, session->ts_cap
    };
    for (size_t i = 0; i < sizeof(answer_caps) / sizeof(answer_caps[0]); i++) {
        if (answer_caps[i] > SIZE_MAX - owed_cap) return MOQ_ERR_LIMIT;
        owed_cap += answer_caps[i];
    }
    if (owed_cap > SIZE_MAX / sizeof(pub_owed_answer_t)) return MOQ_ERR_LIMIT;

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
    p->cfg.on_subscribe_v1 = on_subscribe_v1;
    p->cfg.on_subscribe_v1_ctx = on_subscribe_v1_ctx;
    p->cfg.max_fill_snapshot_bytes = UINT64_C(16) * 1024 * 1024;
    if (pub_cfg_has_field(cfg, offsetof(moq_pub_cfg_t, max_fill_snapshot_bytes),
                          sizeof(cfg->max_fill_snapshot_bytes)) && cfg->max_fill_snapshot_bytes)
        p->cfg.max_fill_snapshot_bytes = cfg->max_fill_snapshot_bytes;
    if (cfg->struct_size >= req_end) {
        p->cfg.on_fill_request = cfg->on_fill_request;
        p->cfg.on_fill_request_ctx = cfg->on_fill_request_ctx;
    }
    if (cfg->struct_size >= term_end) {
        p->cfg.on_fill_terminal = cfg->on_fill_terminal;
        p->cfg.on_fill_terminal_ctx = cfg->on_fill_terminal_ctx;
    }
    if (cfg->struct_size >= ngr_end) {
        p->cfg.on_new_group_request = cfg->on_new_group_request;
        p->cfg.on_new_group_request_ctx = cfg->on_new_group_request_ctx;
    }
    if (pub_cfg_has_field(cfg, offsetof(moq_pub_cfg_t, on_goaway),
                          sizeof(cfg->on_goaway)))
        p->cfg.on_goaway = cfg->on_goaway;
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

    /* Fill slots exist only on a draft that can carry a fill request; on
     * the others the session never surfaces one, so nothing is reserved. */
    if (moq_session_supports_fill(session)) {
        uint32_t nfills = 0;
        if (pub_cfg_has_field(cfg, offsetof(moq_pub_cfg_t, max_fills),
                              sizeof(cfg->max_fills)))
            nfills = cfg->max_fills;
        if (nfills == 0) nfills = PUB_DEFAULT_MAX_FILLS;
        if ((uint64_t)nfills * sizeof(pub_pending_fill_t) > SIZE_MAX) {
            alloc->free(p, sizeof(*p), alloc->ctx);
            return MOQ_ERR_LIMIT;
        }
        p->pending_fills = (pub_pending_fill_t *)alloc->alloc(
            (size_t)nfills * sizeof(pub_pending_fill_t), alloc->ctx);
        if (!p->pending_fills) {
            alloc->free(p, sizeof(moq_publisher_t), alloc->ctx);
            return MOQ_ERR_NOMEM;
        }
        memset(p->pending_fills, 0, (size_t)nfills * sizeof(pub_pending_fill_t));
        p->pending_fill_cap = nfills;
    }
    {
        p->admissions.head = p->admissions.tail = PUB_ADMISSION_NONE;
        p->admissions.cap = admission_cap;
        if (admission_cap) {
            p->admissions.cells = alloc->alloc(
                admission_cap * sizeof(pub_admission_t), alloc->ctx);
            if (!p->admissions.cells) {
                if (p->pending_fills)
                    alloc->free(p->pending_fills,
                        p->pending_fill_cap * sizeof(pub_pending_fill_t), alloc->ctx);
                alloc->free(p, sizeof(*p), alloc->ctx);
                return MOQ_ERR_NOMEM;
            }
            memset(p->admissions.cells, 0, admission_cap * sizeof(pub_admission_t));
        }
        p->fetch_owners.head = p->fetch_owners.tail = PUB_FETCH_NONE;
        p->fetch_owners.cap = fetch_cap;
        if (fetch_cap) {
            p->fetch_owners.cells = alloc->alloc(
                fetch_cap * sizeof(pub_fetch_owner_t), alloc->ctx);
            if (!p->fetch_owners.cells) {
                if (p->admissions.cells)
                    alloc->free(p->admissions.cells,
                        admission_cap * sizeof(pub_admission_t), alloc->ctx);
                if (p->pending_fills)
                    alloc->free(p->pending_fills,
                        p->pending_fill_cap * sizeof(pub_pending_fill_t), alloc->ctx);
                alloc->free(p, sizeof(*p), alloc->ctx);
                return MOQ_ERR_NOMEM;
            }
            memset(p->fetch_owners.cells, 0, fetch_cap * sizeof(pub_fetch_owner_t));
        }
    }
    p->owed_cap = owed_cap;
    if (owed_cap) {
        p->owed = alloc->alloc(owed_cap * sizeof(pub_owed_answer_t), alloc->ctx);
        if (!p->owed) {
            if (p->fetch_owners.cells)
                alloc->free(p->fetch_owners.cells,
                    fetch_cap * sizeof(pub_fetch_owner_t), alloc->ctx);
            if (p->admissions.cells)
                alloc->free(p->admissions.cells,
                    admission_cap * sizeof(pub_admission_t), alloc->ctx);
            if (p->pending_fills)
                alloc->free(p->pending_fills,
                    p->pending_fill_cap * sizeof(pub_pending_fill_t), alloc->ctx);
            alloc->free(p, sizeof(*p), alloc->ctx);
            return MOQ_ERR_NOMEM;
        }
    }

    *out = p;
    return MOQ_OK;
}

void moq_pub_destroy(moq_publisher_t *pub) {
    if (!pub) return;
    if (pub->in_fill_callback) return;
    admissions_clear(pub);
    pending_fetches_clear(pub);   /* release any snapshot refs held for a fetch */
    pending_fills_clear_all(pub);
    moq_pub_track_t *t = pub->tracks;
    while (t) {
        moq_pub_track_t *next = t->next;
        for (size_t i = 0; i < t->slot_cap; i++) {
            const pub_sub_slot_t *sl = &t->slots[i];
            if (!sl->active || sl->kind != PUB_SLOT_SUBSCRIPTION) continue;
            int slot = sub_resolve_handle(pub->session, sl->sub);
            if (slot >= 0) pub->session->subs[slot].facade_defer_updates = false;
        }
        if (t->publish_requested) {
            int slot = pub_resolve_handle(pub->session, t->publication);
            if (slot >= 0) pub->session->publishes[slot].facade_defer_updates = false;
        }
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
    if (pub->owed)
        alloc.free(pub->owed, pub->owed_cap * sizeof(pub->owed[0]), alloc.ctx);
    if (pub->fetch_owners.cells)
        alloc.free(pub->fetch_owners.cells,
                   pub->fetch_owners.cap * sizeof(pub_fetch_owner_t), alloc.ctx);
    if (pub->admissions.cells)
        alloc.free(pub->admissions.cells,
                   pub->admissions.cap * sizeof(pub_admission_t), alloc.ctx);
    if (pub->pending_fills)
        alloc.free(pub->pending_fills,
                   pub->pending_fill_cap * sizeof(pub_pending_fill_t), alloc.ctx);
    alloc.free(pub, sizeof(moq_publisher_t), alloc.ctx);
}

/* Frozen original prefix: struct_size .. publisher_priority (the layout
 * before max_retained_bytes, has_publisher_priority, and monotonic_groups
 * were appended; it matches the add_track minimum struct_size,
 * offsetof(max_retained_bytes)). The pointer-only initializer touches only
 * this prefix, so an old caller that allocated the original-sized struct
 * is never overflowed, and every appended field stays disabled unless
 * _init_sized opts in. */
/* Historical storage and frozen current placement are separate guards:
 * pre-monotonic sizeof is 64 on LP64, 36 on i686, and 40 on ARM ILP32.
 * Never derive the old allocation from the current type's layout. */
typedef struct pub_track_cfg_pre_monotonic {
    uint32_t struct_size;
    moq_namespace_t track_namespace;
    moq_bytes_t track_name;
    bool advertise_namespace;
    uint8_t publisher_priority;
    uint64_t max_retained_bytes;
    bool has_publisher_priority;
} pub_track_cfg_pre_monotonic_t;
_Static_assert(offsetof(moq_pub_track_cfg_t, monotonic_groups) ==
               (sizeof(void *) == 8 ? 64 : 40),
               "monotonic_groups must retain its frozen offset");
_Static_assert(offsetof(moq_pub_track_cfg_t, monotonic_groups) >=
               sizeof(pub_track_cfg_pre_monotonic_t),
               "monotonic_groups must not reuse historical storage");

/* Group order follows the complete namespace-auth allocation, including
 * any target-specific tail padding. */
_Static_assert(offsetof(moq_pub_track_cfg_t, group_order) ==
    ((offsetof(moq_pub_track_cfg_t, namespace_auth_token_count) + sizeof(size_t) +
      _Alignof(moq_pub_track_cfg_t) - 1) &
     ~(size_t)(_Alignof(moq_pub_track_cfg_t) - 1)),
    "group order must start beyond the complete namespace-auth allocation");

#define MOQ_PUB_TRACK_CFG_V0_SIZE (sizeof(void *) == 8 ? 48u : 24u)
_Static_assert(offsetof(moq_pub_track_cfg_t, max_retained_bytes) ==
               MOQ_PUB_TRACK_CFG_V0_SIZE, "frozen original track cfg prefix");

_Static_assert(offsetof(moq_pub_track_cfg_t, namespace_auth_tokens) ==
    ((offsetof(moq_pub_track_cfg_t, dynamic_groups) + sizeof(bool) +
      _Alignof(moq_pub_track_cfg_t) - 1) &
     ~(size_t)(_Alignof(moq_pub_track_cfg_t) - 1)),
    "namespace credentials must start beyond the complete D22 allocation");

/* Original ARM callers have only four-byte alignment, even though the
 * appended uint64_t makes the current type eight-aligned. */
static void pub_track_cfg_init_bytes(void *storage, size_t n)
{
    moq_pub_track_cfg_t cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.struct_size = (uint32_t)n;
    if (n >= offsetof(moq_pub_track_cfg_t, publisher_priority) +
             sizeof(cfg.publisher_priority))
        cfg.publisher_priority = 128;
    memcpy(storage, &cfg, n);
}

static bool pub_track_cfg_normalize(const void *storage, moq_pub_track_cfg_t *cfg)
{
    uint32_t size;
    memcpy(&size, storage, sizeof(size));
    if (size < MOQ_PUB_TRACK_CFG_V0_SIZE) return false;
    memset(cfg, 0, sizeof(*cfg));
    memcpy(cfg, storage, size < sizeof(*cfg) ? size : sizeof(*cfg));
    return true;
}

/* Own the track's Track Properties: the caller's set, with DYNAMIC_GROUPS = 1
 * and DEFAULT_PUBLISHER_GROUP_ORDER added in the session's encoding when
 * declared, validated as the session validates an accept's properties.
 * MOQ_ERR_INVAL for a set the session would refuse; MOQ_ERR_NOMEM. Nothing is
 * allocated for an empty set. */
static moq_result_t track_set_properties(moq_publisher_t *pub, moq_pub_track_t *t,
                                         moq_bytes_t in, bool dynamic,
                                         moq_group_order_t group_order)
{
    const moq_profile_ops_t *p = pub->session->profile;
    uint8_t *buf;
    size_t cap, n;
    moq_result_t rc = moq_profile_build_track_properties(p, &pub->alloc,
        in.data, in.len, dynamic, group_order, &buf, &cap, &n);
    if (rc < 0 || !buf) return rc;
    t->props = buf;
    t->props_len = n;
    t->props_cap = cap;
    t->dynamic_groups = p->track_properties_dynamic_groups(buf, n);
    return MOQ_OK;
}

void moq_pub_track_cfg_init(moq_pub_track_cfg_t *cfg) {
    if (!cfg) return;
    pub_track_cfg_init_bytes(cfg, MOQ_PUB_TRACK_CFG_V0_SIZE);
}

void moq_pub_track_cfg_init_sized(moq_pub_track_cfg_t *cfg, size_t cfg_size) {
    if (!cfg) return;
    /* Clear exactly what the caller allocated, never beyond this library's
     * struct (clamp down for a caller newer than the library). */
    size_t n = cfg_size < sizeof(*cfg) ? cfg_size : sizeof(*cfg);
    if (n < sizeof(uint32_t)) return;  /* too small to even stamp */
    pub_track_cfg_init_bytes(cfg, n);
}

moq_result_t moq_pub_add_track(moq_publisher_t *pub,
                                 const moq_pub_track_cfg_t *cfg,
                                 uint64_t now_us,
                                 moq_pub_track_t **out)
{
    if (!pub || !cfg || !out) return MOQ_ERR_INVAL;
    if (pub->in_fill_callback) return MOQ_ERR_WRONG_STATE;
    *out = NULL;

    if (pub->closed) return MOQ_ERR_CLOSED;
    moq_pub_track_cfg_t normalized_cfg;
    if (!pub_track_cfg_normalize(cfg, &normalized_cfg)) return MOQ_ERR_INVAL;
    cfg = &normalized_cfg;
    const size_t auth_end = offsetof(moq_pub_track_cfg_t, namespace_auth_token_count) +
                            sizeof(cfg->namespace_auth_token_count);
    if (cfg->struct_size > offsetof(moq_pub_track_cfg_t, namespace_auth_tokens) &&
        cfg->struct_size < auth_end)
        return MOQ_ERR_INVAL;

    /* Validate namespace + track name against full-track-name budget. */
    if (moq_validate_full_track_name_min_fields(
            &cfg->track_namespace, cfg->track_name,
            pub->session->profile->min_track_namespace_fields) < 0)
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
     * appended bool (it starts at or beyond the old sizeof boundary, see
     * the independent guards at the initializers). */
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

    /* The Track Properties every answer for the track carries, checked as
     * an accept would check them, so no later answer fails on them. */
    {
        moq_bytes_t in = { NULL, 0 };
        bool dynamic = false;
        moq_group_order_t order = MOQ_GROUP_ORDER_DEFAULT;
        if (cfg->struct_size >= offsetof(moq_pub_track_cfg_t, track_properties) +
                                sizeof(cfg->track_properties))
            in = cfg->track_properties;
        if (cfg->struct_size >= offsetof(moq_pub_track_cfg_t, dynamic_groups) +
                                sizeof(cfg->dynamic_groups))
            dynamic = cfg->dynamic_groups;
        if (cfg->struct_size >= offsetof(moq_pub_track_cfg_t, group_order) +
                                sizeof(cfg->group_order))
            order = cfg->group_order;
        moq_result_t prc = track_set_properties(pub, t, in, dynamic, order);
        if (prc < 0) {
            free_track_state(pub, t);
            return prc;
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
            if (cfg->struct_size >= auth_end) {
                nscfg.auth_tokens = cfg->namespace_auth_tokens;
                nscfg.auth_token_count = cfg->namespace_auth_token_count;
            }
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
    if (pub->in_fill_callback) return MOQ_ERR_WRONG_STATE;
    if (track->pub != pub) return MOQ_ERR_INVAL;

    for (size_t i = 0; i < pub->pending_fill_cap; i++)
        if (pub->pending_fills[i].active && pub->pending_fills[i].track == track)
            return MOQ_ERR_WOULD_BLOCK;

    /* Pre-flight checks before any side effects. */
    if (pub->deferred.active && pub->deferred.track == track)
        return MOQ_ERR_WOULD_BLOCK;
    for (uint32_t i = pub->admissions.head; i != PUB_ADMISSION_NONE;
         i = pub->admissions.cells[i].next)
        if (pub->admissions.cells[i].track == track)
            return MOQ_ERR_WOULD_BLOCK;
    /* A pending retained FETCH is served from its own snapshot, so removing the
     * source track here is safe -- no guard needed. */
    if (track->ns_entry && track->ns_entry->state == PUB_NS_PENDING &&
        track->ns_entry->refcount <= 1)
        return MOQ_ERR_WRONG_STATE;

    /* Subgroups of departed destinations are reset before the track goes. */
    {
        moq_result_t rr = track_run_retires(pub, track, now_us);
        if (rr == MOQ_ERR_WOULD_BLOCK) return MOQ_ERR_WOULD_BLOCK;
        if (rr < 0 && rr != MOQ_ERR_STALE_HANDLE) return rr;
    }

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
                rc = slot_close_subgroup(pub, track, psl, now_us);
            if (rc == MOQ_ERR_WOULD_BLOCK) return MOQ_ERR_WOULD_BLOCK;
            if (rc != MOQ_OK && rc != MOQ_ERR_STALE_HANDLE) return rc;
            psl->sg_open = false;
            psl->streaming = false;
            psl->needs_reset = false;
            if (omitted) psl->done_op = track->cur_op;
        }
        moq_finish_publish_cfg_t fcfg;
        moq_finish_publish_cfg_init(&fcfg);
        fcfg.status_code = MOQ_PUB_DONE_TRACK_ENDED;   /* not INTERNAL_ERROR (0) */
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
    /* Group history: a retained install assigns Objects 0..n-1 of the group
     * generation 0 (Subgroup ID 0), so it must agree with what the group
     * already holds; checked before any effect. */
    hist_plan_t hp;
    moq_result_t hr = hist_plan_retained(t, group_id, src[n - 1].object_id, &hp);
    if (hr < 0) return hr;
    pub_retained_obj_t *vec = pub_retained_snapshot(&t->pub->alloc, src, n);
    if (!vec) return MOQ_ERR_NOMEM;
    hist_commit_retained(t, &hp, group_id, src[n - 1].object_id);
    for (size_t i = 0; i < n; i++)
        vec[i].subgroup_id = hist_subgroup_of(t, group_id, vec[i].object_id);
    track_release_retained(t);
    t->retained = vec;
    t->retained_count = n;
    t->retained_group_id = group_id;
    t->has_retained = true;
    t->wrote_object = false;   /* retained group is now the latest published loc */
    if (retained_can_advertise_largest(t))
        track_hist_merge(t->hist, group_id,
                         pub_track_retained_last_object_id(t));
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
    if (pub->in_fill_callback) return MOQ_ERR_WRONG_STATE;
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
        tmp[i].subgroup_id  = 0;
    }
    return track_install_retained(track, cfg->group_id, tmp, cfg->object_count);
}

moq_result_t moq_pub_clear_retained_group(moq_publisher_t *pub,
                                          moq_pub_track_t *track)
{
    if (!pub || !track) return MOQ_ERR_INVAL;
    if (pub->in_fill_callback) return MOQ_ERR_WRONG_STATE;
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
    if (slot_key_stopped(track, slot, obj->group_id, track->op.subgroup))
        return MOQ_OK;
    bool need_ext = (obj->properties != NULL);

    bool want_eog = false;
    if (obj->struct_size >= offsetof(moq_pub_object_cfg_t, end_of_group) +
        sizeof(obj->end_of_group))
        want_eog = obj->end_of_group;

    /* The op's subgroup: (group, generation assigned at admission). */
    bool same_sg = slot->sg_open && slot->cur_group == obj->group_id &&
                   slot->cur_gen == track->op.subgroup;

    /* Same-subgroup extension mode mismatch: can't reopen mid-stream. */
    if (same_sg && need_ext && !slot->sg_has_extensions)
        return MOQ_ERR_WRONG_STATE;

    /* Same-subgroup end_of_group mismatch: header already on the wire. */
    if (same_sg && want_eog != slot->sg_end_of_group)
        return MOQ_ERR_WRONG_STATE;

    /* Close the open subgroup if the op belongs to another one. */
    if (slot->sg_open && !same_sg) {
        moq_result_t rc = slot_close_subgroup(pub, track, slot, now_us);
        if (rc == MOQ_ERR_WOULD_BLOCK) return MOQ_ERR_WOULD_BLOCK;
        if (rc < 0) return rc;
        slot->sg_open = false;
    }

    if (!slot->sg_open) {
        moq_subgroup_cfg_t sgcfg;
        moq_subgroup_cfg_init(&sgcfg);
        sgcfg.group_id = obj->group_id;
        sgcfg.subgroup_id = track->op.subgroup;
        sgcfg.publisher_priority = track->priority;
        sgcfg.object_properties = need_ext;
        sgcfg.end_of_group = want_eog;
        sgcfg.first_object = track->op.first_in_gen;

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
        slot_open_note(track, slot, obj->group_id, obj->object_id);
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
            return slot_stale_subgroup(pub, track, slot);
        }
        if (rc == MOQ_OK) slot->sg_last = obj->object_id;
        return rc;
    }

    moq_result_t rc = moq_session_write_object(pub->session,
        slot->sg, obj->object_id, obj->payload, now_us);
    if (rc == MOQ_ERR_WOULD_BLOCK) return MOQ_ERR_WOULD_BLOCK;
    if (rc == MOQ_ERR_STALE_HANDLE) {
        return slot_stale_subgroup(pub, track, slot);
    }
    if (rc == MOQ_OK) slot->sg_last = obj->object_id;
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
    if (pub->in_fill_callback) return MOQ_ERR_WRONG_STATE;
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

    track_observe_terminals(pub, track);

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
    hist_plan_t hp;
    hist_kind_t hk = HIST_STREAM;
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
        /* Group history: identity, finality and capacity, before any
         * effect. */
        hk = !obj->datagram ? HIST_STREAM
           : !obj->has_status ? HIST_DATAGRAM
           : obj->status == MOQ_OBJECT_END_OF_GROUP ? HIST_STATUS_EOG
           : obj->status == MOQ_OBJECT_END_OF_TRACK ? HIST_STATUS_EOT
           : HIST_DATAGRAM;
        moq_result_t hr = hk == HIST_STREAM
            ? hist_plan_stream(track, obj->group_id, obj->object_id, &hp)
            : hist_plan_datagram(track, hk, obj->group_id, obj->object_id, &hp);
        if (hr < 0) return hr;
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
        /* The object must fit the header of a subgroup already open for
         * its Group. */
        if (!obj->datagram &&
            !track_header_admits(track, obj->group_id, obj->object_id,
                                 obj->properties != NULL, true, want_eog))
            return MOQ_ERR_WRONG_STATE;
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
        if (hk == HIST_STREAM)
            hist_commit_stream(track, &hp, obj->group_id, obj->object_id,
                               want_eog);
        else
            hist_commit_datagram(track, &hp, hk, obj->group_id,
                                 obj->object_id);
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

/* One destination's END_OF_TRACK for an end_track operation: one empty
 * status object at the frozen terminal Location, alone on a fresh subgroup
 * that is FIN'd. Every step -- closing the data subgroup still open, opening
 * the terminal one, queuing the status object, the FIN -- is recorded as it
 * succeeds (eot_sg, eot_written), so a refusal for room at any of them
 * resumes at that step and nothing is sent twice, however small the action
 * queue. MOQ_OK once the FIN is queued. A stale parent clears the slot;
 * a stale terminal subgroup leaves its parent and stream count intact, with
 * sg_open false, for the caller's control-only end. Otherwise return the refusal,
 * WOULD_BLOCK included. */
static moq_result_t slot_end_of_track(moq_publisher_t *pub, moq_pub_track_t *track,
                                      pub_sub_slot_t *slot, uint64_t term_g,
                                      uint64_t term_o, uint64_t now_us)
{
    if (slot->sg_open && !slot->eot_sg) {
        moq_result_t rc = slot_close_subgroup(pub, track, slot, now_us);
        if (rc < 0 && rc != MOQ_ERR_STALE_HANDLE) return rc;
        slot->sg_open = false;
    }
    if (!slot->eot_sg) {
        moq_subgroup_cfg_t sgcfg;
        moq_subgroup_cfg_init(&sgcfg);
        sgcfg.group_id = term_g;
        sgcfg.subgroup_id = 0;
        sgcfg.publisher_priority = track->priority;
        sgcfg.object_properties = false;
        /* The terminal status object starts its subgroup unless a stream
         * object was already published in that group. */
        sgcfg.first_object = hist_find(track, term_g) < 0 &&
                             !hist_sealed(track, term_g);
        moq_result_t rc = (slot->kind == PUB_SLOT_PUBLICATION)
            ? moq_session_open_pub_subgroup(pub->session, slot->pub, &sgcfg,
                                            now_us, &slot->sg)
            : moq_session_open_subgroup(pub->session, slot->sub, &sgcfg,
                                        now_us, &slot->sg);
        if (rc == MOQ_ERR_STALE_HANDLE) { track_clear_slot(pub, slot); return rc; }
        if (rc < 0) return rc;
        slot->sg_open = true;
        slot->eot_sg = true;
        slot->cur_group = term_g;
        slot->sg_omitted = false;   /* fresh terminal, not the old data subgroup */
        slot->streams_opened++;
    }
    if (!slot->eot_written) {
        moq_result_t rc = moq_session_write_status_object(pub->session, slot->sg,
            term_o, MOQ_OBJECT_END_OF_TRACK, now_us);
        if (rc == MOQ_ERR_STALE_HANDLE) { slot->sg_open = false; return rc; }
        if (rc < 0) return rc;
        slot->eot_written = true;
    }
    moq_result_t rc = moq_session_close_subgroup(pub->session, slot->sg, now_us);
    if (rc < 0 && rc != MOQ_ERR_STALE_HANDLE) return rc;
    slot->sg_open = false;
    if (rc == MOQ_ERR_STALE_HANDLE) return rc;
    slot->eot_done = true;
    return MOQ_OK;
}

/* The publication's part of an end_track operation, before its
 * PUBLISH_DONE: the END_OF_TRACK (slot_end_of_track) when a fresh terminal
 * exists and the publication was a destination of the operation that still
 * admits the Location (accepted, Forward 1, inside its window); otherwise a
 * control-only end. The slot is then marked served for the operation. */
static moq_result_t publication_end_of_track(moq_publisher_t *pub,
                                             moq_pub_track_t *track,
                                             bool term_fresh, uint64_t term_g,
                                             uint64_t term_o, uint64_t now_us)
{
    pub_sub_slot_t *psl = track_find_publication_slot(track);
    if (!psl || !term_fresh || psl->eot_done) return MOQ_OK;
    if (psl->eot_sg) {
        /* Reset by a cut before its FIN: PUBLISH_DONE alone ends it. */
        if (!psl->sg_open) return MOQ_OK;
    } else {
        if (psl->target_op != track->cur_op || psl->done_op == track->cur_op)
            return MOQ_OK;
        if (!slot_eligible(track, psl)) {
            psl->done_op = track->cur_op;   /* control-only end */
            return MOQ_OK;
        }
    }
    /* A header already sent does not grandfather an unsent Object across
     * a same-group Start Location update. Unpublish settles the open stream
     * under window_owes, including its below-Start FIN exception. */
    if (!psl->eot_written && !window_admits(&psl->window, term_g, term_o)) {
        psl->done_op = track->cur_op;
        return MOQ_OK;
    }
    moq_result_t rc = slot_end_of_track(pub, track, psl, term_g, term_o, now_us);
    if (rc == MOQ_ERR_STALE_HANDLE) return MOQ_OK;
    if (rc != MOQ_OK) return rc;
    psl->done_op = track->cur_op;
    return MOQ_OK;
}

moq_result_t moq_pub_end_track(moq_publisher_t *pub, moq_pub_track_t *track,
                               uint64_t now_us)
{
    if (!pub || !track) return MOQ_ERR_INVAL;
    if (pub->in_fill_callback) return MOQ_ERR_WRONG_STATE;
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

    /* The operation (and with it the terminal plan and its destinations)
     * begins before any effect, so every retry resumes the same one. */
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

    /* The publication ends on the data plane first -- its END_OF_TRACK status
     * object on a stream of its own -- then with PUBLISH_DONE, whose Stream
     * Count includes that stream. Then the subscription slots. */
    {
        moq_result_t rc = publication_end_of_track(pub, track, term_fresh,
                                                   term_g, term_o, now_us);
        if (rc != MOQ_OK) return rc;
        rc = moq_pub_unpublish_track(pub, track, now_us);
        if (rc != MOQ_OK) return rc;
    }

    for (size_t si = 0; si < track->slot_cap; si++) {
        pub_sub_slot_t *slot = &track->slots[si];
        if (!slot->active) continue;
        /* The publication was ended above (its END_OF_TRACK, then
         * PUBLISH_DONE). */
        if (slot->kind == PUB_SLOT_PUBLICATION) {
            slot->done_op = track->cur_op;
            continue;
        }
        /* Served: its END_OF_TRACK was FIN'd. A destination merely
         * excluded from the operation (done_op, e.g. a Forward cut mid-op)
         * still owes its terminal control message below. */
        if (slot->eot_done) continue;
        /* In the operation = stamped at admission AND still eligible. A slot
         * OUTSIDE the operation (Forward 0 at admission, or lazily retired)
         * cannot receive the status OBJECT -- but both drafts require the
         * terminal control message REGARDLESS of Forward state, so it takes
         * the terminal-control path below instead of being skipped. */
        bool in_op = slot->eot_sg
            ? slot->sg_open    /* a terminal under way, unless a cut reset it */
            : (slot->target_op == track->cur_op &&
               slot->done_op != track->cur_op && slot_eligible(track, slot));

        if (term_fresh && in_op && window_admits(&slot->window, term_g, term_o)) {
            moq_result_t rc = slot_end_of_track(pub, track, slot, term_g, term_o,
                                               now_us);
            if (rc == MOQ_OK) {
                slot->done_op = track->cur_op;
                continue;
            }
            if (rc != MOQ_ERR_STALE_HANDLE) return rc;
            if (!slot->active) continue;   /* parent, not just its stream, gone */
            /* A peer-stopped terminal still owes its parent's done. */
        }
        {
            /* Terminal control: close any open subgroup FIRST (streams close
             * before the done, same normative ordering as
             * finish_subscribers; a Forward-0 slot's open subgroup was
             * already RESET by the retire sweep above), then
             * SUBSCRIBE_DONE/PUBLISH_DONE. Status is exact wire semantics:
             * SUBSCRIPTION_ENDED (0x3) ONLY on a draft that defines it and
             * when the subscription's FINITE filter end was genuinely passed
             * by the terminal Location;
             * everything else -- open-ended windows the track never
             * reached, Forward-0 pauses -- is TRACK_ENDED (0x2): the track
             * is no longer being published. Stream count is EXACT: the
             * facade opened every stream for this subscription itself.
             * Retry-safe: sg_open clears only on a successful close, the
             * slot clears only after a successful/stale done, so a
             * WOULD_BLOCK retry resumes exactly where it stopped and never
             * doubles the done. */
            if (slot->sg_open) {
                moq_result_t rc = slot_close_subgroup(pub, track, slot, now_us);
                if (rc == MOQ_ERR_WOULD_BLOCK) return MOQ_ERR_WOULD_BLOCK;
                if (rc < 0 && rc != MOQ_ERR_STALE_HANDLE) return rc;
                slot->sg_open = false;
            }
            moq_done_subscribe_cfg_t dcfg;
            moq_done_subscribe_cfg_init(&dcfg);
            dcfg.status_code =
                (pub->session->profile->publish_done_subscription_ended &&
                 term_fresh &&
                 slot->window.has_window && slot->window.has_end &&
                 term_g > slot->window.end_group)
                    ? 0x3    /* SUBSCRIPTION_ENDED: past the finite end */
                    : 0x2;   /* TRACK_ENDED (incl. the no-fresh-terminal
                              * degraded mode: the track just ended, and
                              * every end on a draft without
                              * SUBSCRIPTION_ENDED) */
            dcfg.stream_count = slot->streams_opened;
            moq_result_t rc = moq_session_done_subscribe(pub->session,
                                                         slot->sub, &dcfg,
                                                         now_us);
            if (rc == MOQ_ERR_WOULD_BLOCK) return MOQ_ERR_WOULD_BLOCK;
            if (rc != MOQ_OK && rc != MOQ_ERR_STALE_HANDLE) return rc;
            track_clear_slot(pub, slot);   /* subscription freed */
            continue;
        }
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
    if (pub->in_fill_callback) return MOQ_ERR_WRONG_STATE;
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
            moq_result_t rc = slot_close_subgroup(pub, track, slot, now_us);
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

    track_observe_terminals(pub, track);

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
    hist_plan_t hp;
    if (st == 1) {
        moq_result_t vr = validate_new_object(track, cfg->group_id,
            cfg->object_id, cfg->payload_length, begin_props);
        if (vr < 0) return vr;                  /* no mutation on rejection */
        /* §10 (B): sealed groups reject before any mutation (see
         * moq_pub_write_object_ex). */
        if (track->has_declared && cfg->group_id <= track->declared_through)
            return MOQ_ERR_WRONG_STATE;
        moq_result_t hr = hist_plan_stream(track, cfg->group_id,
                                           cfg->object_id, &hp);
        if (hr < 0) return hr;
        /* §10 (A): same monotonicity rules as the one-shot path. */
        if (track->monotonic) {
            if (track->has_begun && cfg->group_id < track->begun_high)
                return MOQ_ERR_WRONG_STATE;
            if (track->has_eog && cfg->group_id <= track->eog_high)
                return MOQ_ERR_WRONG_STATE;
        }
        /* Properties need a subgroup opened with them. */
        if (!track_header_admits(track, cfg->group_id, cfg->object_id,
                                 begin_props != NULL, false, false))
            return MOQ_ERR_WRONG_STATE;
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
        hist_commit_stream(track, &hp, cfg->group_id, cfg->object_id, false);
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
        if (slot_key_stopped(track, slot, cfg->group_id, track->op.subgroup)) {
            slot->done_op = track->cur_op;
            continue;
        }
        if (slot->streaming) {              /* this slot's begin already done */
            slot->done_op = track->cur_op;
            continue;
        }

        if (slot->sg_open && (slot->cur_group != cfg->group_id ||
                              slot->cur_gen != track->op.subgroup)) {
            moq_result_t rc = slot_close_subgroup(pub, track, slot, now_us);
            if (rc == MOQ_ERR_WOULD_BLOCK) return MOQ_ERR_WOULD_BLOCK;
            if (rc < 0) return rc;
            slot->sg_open = false;
        }

        if (!slot->sg_open) {
            moq_subgroup_cfg_t sgcfg;
            moq_subgroup_cfg_init(&sgcfg);
            sgcfg.group_id = cfg->group_id;
            sgcfg.subgroup_id = track->op.subgroup;
            sgcfg.publisher_priority = track->priority;
            sgcfg.object_properties = need_ext;
            sgcfg.first_object = track->op.first_in_gen;
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
            slot_open_note(track, slot, cfg->group_id, cfg->object_id);
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
            rc = slot_stale_subgroup(pub, track, slot);
            if (rc < 0) return rc;
            continue;
        }
        if (rc < 0) return rc;

        slot->streaming = true;
        slot->sg_last = cfg->object_id;
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
    if (pub && pub->in_fill_callback) return MOQ_ERR_WRONG_STATE;
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
    if (pub->in_fill_callback) return MOQ_ERR_WRONG_STATE;
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
            rc = slot_stale_subgroup(pub, track, slot);
            if (rc < 0) return rc;
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
    if (pub->in_fill_callback) return MOQ_ERR_WRONG_STATE;
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
            rc = slot_stale_subgroup(pub, track, slot);
            if (rc < 0) return rc;
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
    if (pub->in_fill_callback) return MOQ_ERR_WRONG_STATE;
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
    /* The current logical group is sealed at once, whatever the
     * destinations: its subgroup ends, and nothing more is assigned in it. */
    if (st == 1) hist_end_group(track);

    for (size_t si = 0; si < track->slot_cap; si++) {
        pub_sub_slot_t *slot = &track->slots[si];
        if (!slot_op_pending(track, slot)) continue;
        if (!slot->sg_open) {               /* nothing open on this slot */
            slot->done_op = track->cur_op;
            continue;
        }
        moq_result_t rc = slot_close_subgroup(pub, track, slot, now_us);
        if (rc == MOQ_ERR_WOULD_BLOCK) return MOQ_ERR_WOULD_BLOCK;
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
    if (pub->in_fill_callback) return MOQ_ERR_WRONG_STATE;
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

static moq_result_t pub_reset_group(moq_publisher_t *pub,
                                     moq_pub_track_t *track,
                                     uint64_t error_code, uint64_t now_us,
                                     bool *out_did_reset)
{
    *out_did_reset = false;
    if (!pub || !track) return MOQ_ERR_INVAL;
    if (pub->in_fill_callback) return MOQ_ERR_WRONG_STATE;
    if (track->pub != pub) return MOQ_ERR_INVAL;
    if (pub->closed) return MOQ_ERR_CLOSED;

    track_observe_terminals(pub, track);

    /* Abandon the open subgroup on every active slot (same loop as
     * remove_track's teardown). WOULD_BLOCK is retryable: a slot already
     * reset has sg_open cleared, so a re-drive skips it. */
    for (size_t si = 0; si < track->slot_cap; si++) {
        pub_sub_slot_t *sl = &track->slots[si];
        if (!sl->active || !sl->sg_open) continue;
        moq_result_t rc = moq_session_reset_subgroup(pub->session,
            sl->sg, error_code, now_us);
        if (rc == MOQ_ERR_WOULD_BLOCK) return MOQ_ERR_WOULD_BLOCK;
        if (rc < 0 && rc != MOQ_ERR_STALE_HANDLE) return rc;
        if (rc == MOQ_OK) *out_did_reset = true;
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
    hist_reset_current(track);
    track_op_release(track);
    track->streaming_active = false;
    return MOQ_OK;
}

moq_result_t moq_pub_reset_group(moq_publisher_t *pub,
                                   moq_pub_track_t *track,
                                   uint64_t error_code, uint64_t now_us)
{
    bool did_reset;
    return pub_reset_group(pub, track, error_code, now_us, &did_reset);
}

moq_result_t moq_core_internal_pub_reset_group(moq_publisher_t *pub,
                                                moq_pub_track_t *track,
                                                uint64_t error_code,
                                                uint64_t now_us,
                                                bool *out_did_reset)
{
    if (!out_did_reset) return MOQ_ERR_INVAL;
    return pub_reset_group(pub, track, error_code, now_us, out_did_reset);
}

/* NEW_GROUP_REQUEST on a track that advertises DYNAMIC_GROUPS: surfaced when
 * it warrants a new group, its value 0 or larger than the current group
 * (§9.20.19). A track without DYNAMIC_GROUPS ignores it. */
static void pub_new_group_request(moq_publisher_t *pub, moq_pub_track_t *t,
                                  uint64_t value)
{
    if (!t || !t->dynamic_groups || !pub->cfg.on_new_group_request) return;
    moq_pub_new_group_request_info_t info;
    memset(&info, 0, sizeof(info));
    info.struct_size = (uint32_t)sizeof(info);
    info.requested_group = value;
    if (t->hist && t->hist->has_largest) {
        info.has_current_group = true;
        info.current_group = t->hist->largest_group;
    }
    if (value != 0 && info.has_current_group && value <= info.current_group) return;
    pub->cfg.on_new_group_request(pub->cfg.on_new_group_request_ctx, t, &info);
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
 *     retirement). Drain actions, call moq_pub_flush once and continue bounded
 *     FIFO event dispatch to return event/fill credit. Never replay an owned
 *     event. flush itself does not poll.
 *   - WOULD_BLOCK + IGNORED: the event was NOT taken; route it to other owners
 *     before another advancing call invalidates borrowed spans.
 * See the moq_pub_handle_event header for the progress-round contract.
 * `*result` is assumed pre-initialized by the caller.
 */
static moq_result_t pub_dispatch_event(moq_publisher_t *pub,
                                       const moq_event_t *event,
                                       uint64_t now_us,
                                       moq_pub_event_result_t *result)
{
    if (!pub || !event || !result) return MOQ_ERR_INVAL;
    *result = MOQ_PUB_EVENT_IGNORED;

    if (event->kind == MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED) {
        for (moq_pub_track_t *t = pub->tracks; t; t = t->next)
            if (track_note_terminal(pub, t, event)) {
                *result = MOQ_PUB_EVENT_CONSUMED;
                break;
            }
        return MOQ_OK;
    }

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
        if (pub->cfg.on_goaway)
            pub->cfg.on_goaway(pub->callbacks.ctx, event->u.goaway.new_session_uri);
        else if (pub->callbacks.on_draining)
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

    if (event->kind == MOQ_EVENT_NAMESPACE_TERMINATED) {
        if (event->detail_size < sizeof(moq_namespace_terminated_event_t))
            return MOQ_ERR_INVAL;
        const moq_namespace_terminated_event_t *terminal =
            &event->u.namespace_terminated;
        pub_ns_entry_t *e = find_ns_entry_by_ann(pub, terminal->ann);
        if (e && e->state != PUB_NS_TERMINAL) {
            e->state = PUB_NS_TERMINAL;
            *result = MOQ_PUB_EVENT_CONSUMED;
            if (CB_HAS(&pub->callbacks, on_namespace_terminal)) {
                moq_pub_namespace_terminal_info_t info;
                memset(&info, 0, sizeof(info));
                info.struct_size = (uint32_t)sizeof(info);
                info.kind = MOQ_PUB_NAMESPACE_TERMINATED;
                info.namespace_ = (moq_namespace_t){ e->id_parts, e->id_count };
                info.cause = terminal->cause;
                info.local_role = terminal->local_role;
                info.stream_error_code = terminal->stream_error_code;
                pub->callbacks.on_namespace_terminal(pub->callbacks.ctx, &info);
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
        if (admission_cancel(pub, sub._opaque, false))
            *result = MOQ_PUB_EVENT_CONSUMED;
        if (pub->deferred.active && moq_subscription_eq(pub->deferred.sub, sub)) {
            pub->deferred.active = false;
            *result = MOQ_PUB_EVENT_CONSUMED;
        }
        return MOQ_OK;
    }

    if (event->kind == MOQ_EVENT_FETCH_CANCELLED) {
        moq_fetch_t fetch = event->u.fetch_cancelled.fetch;
        uint32_t slot = moq_handle_slot(fetch._opaque);
        if (slot < pub->fetch_owners.cap) {
            pub_pending_fetch_t *pf = &pub->fetch_owners.cells[slot].pending;
            if (pf->active && moq_fetch_eq(pf->fetch, fetch)) {
                pending_fetch_clear(pub, pf);
                *result = MOQ_PUB_EVENT_CONSUMED;
            }
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
        const moq_fetch_request_event_t *fr = &event->u.fetch_request;
        pub_pending_fetch_t *pf;
        int slot = fetch_resolve_handle(pub->session, fr->fetch);
        if (slot < 0) return MOQ_OK;   /* stale event owns no references */
        pf = &pub->fetch_owners.cells[slot].pending;
        if (pf->active && moq_fetch_eq(pf->fetch, fr->fetch)) {
            *result = MOQ_PUB_EVENT_CONSUMED;
            return serve_retained_fetches(pub, now_us);
        }
        /* Resolve the generation before retiring an older slot's snapshot. */
        if (pf->active) pending_fetch_clear(pub, pf);

        moq_request_error_t reject_code = 0;
        bool reject = true;
        bool whole_group_d16 = false;

        /* Resolve the retained track: by the joining subscription for a Joining
         * FETCH, or by explicit namespace/name for a standalone FETCH. */
        /* Three shapes: a Joining FETCH on a PUBLISH-initiated subscription
         * (draft-18 5.1), a Joining FETCH on a SUBSCRIBE-initiated one, or a
         * bounded standalone FETCH. The session guarantees exactly one joining
         * discriminator is set. */
        const bool pub_join = moq_publication_is_valid(fr->joining_pub);
        const bool standalone =
            !pub_join && !moq_subscription_is_valid(fr->joining_sub);
        moq_pub_track_t *track =
            pub_join     ? find_track_by_pub(pub, fr->joining_pub)
            : standalone ? find_track(pub, &fr->track_namespace, fr->track_name)
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
         * is itself proof of an accepted subscription, and a publication join is
         * proof the PEER accepted this publisher's PUBLISH. This check consults
         * only publisher state; it never invokes the subscribe callback. */
        if (standalone &&
            pub->cfg.accept_mode != MOQ_PUB_ACCEPT_ALL &&
            !(track && track_has_subscriber(track))) {
            reject_code = MOQ_REQUEST_ERROR_UNAUTHORIZED;
        } else if (!track) {
            reject_code = MOQ_REQUEST_ERROR_DOES_NOT_EXIST;
        } else if (!fetch_start_published(track, fr->start_group,
                                          fr->start_object)) {
            /* The track exists but nothing was published on it, or the start
             * lies past its Largest Object: INVALID_RANGE. Draft-22 §3.2 and
             * draft-18 §10.12.3 require both. Draft-16 §9.16.3 states only the
             * start-past-Largest case (and §9.16.2 the no-object Joining
             * Fetch); answering a no-object standalone FETCH the same way is
             * this library's policy there. An ended track keeps its objects,
             * so the same rule applies. */
            reject_code = MOQ_REQUEST_ERROR_INVALID_RANGE;
        } else if (track->ended || !pub_track_has_retained(track)) {
            /* Objects were published, but there is nothing to serve them from:
             * the track ended, or its retained group was never set or was
             * cleared after its Largest was advertised -- this cache does not
             * keep the objects of a cleared group, so it cannot answer the
             * range. Reject DOES_NOT_EXIST rather than synthesize an empty
             * fetch. */
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
        } else {
            /* Draft-16: a FETCH that requests the entire Group (End Location
             * Object 0, or a later end Group; draft-16 section 9.16.1) makes
             * the last Object it sends the Group's final Object (section
             * 2.4.2). When the track assigned an Object above the retained
             * ones, the snapshot cannot answer it: the request is refused with
             * REQUEST_ERROR NOT_SUPPORTED, like any range the retained group
             * cannot cover, leaving history unchanged. */
            whole_group_d16 =
                moq_session_version(pub->session) == MOQ_VERSION_DRAFT_16 &&
                (fr->end_group > pub_track_retained_group(track) ||
                 fr->end_object == 0);
            if (whole_group_d16 &&
                !hist_whole_group_fetch_admits(track,
                    pub_track_retained_group(track),
                    pub_track_retained_last_object_id(track)))
                reject_code = MOQ_REQUEST_ERROR_NOT_SUPPORTED;
            else
                reject = false;
        }

        /* Stage. For the serve case, SNAPSHOT the whole retained group (hold
         * refs) so the response is independent of a later clear/end/remove or a
         * set_retained_group that replaces the track's vector. */
        memset(pf, 0, sizeof(*pf));
        pf->active = true;
        pf->fetch = fr->fetch;
        pf->joining_sub = fr->joining_sub;
        pf->joining_pub = fr->joining_pub;
        pf->reject_code = reject_code;
        pf->reject = reject;
        {
            pub_fetch_owners_t *owners = &pub->fetch_owners;
            pub_fetch_owner_t *cell = &owners->cells[slot];
            cell->prev = owners->tail;
            cell->next = PUB_FETCH_NONE;
            if (owners->tail != PUB_FETCH_NONE)
                owners->cells[owners->tail].next = (uint32_t)slot;
            else owners->head = (uint32_t)slot;
            owners->tail = (uint32_t)slot;
        }
        if (!reject) {
            pub_retained_obj_t *snap = pub_retained_snapshot(&pub->alloc,
                track->retained, track->retained_count);
            if (!snap) {
                /* NOMEM staging the snapshot: reject rather than half-serve. */
                pf->reject_code = MOQ_REQUEST_ERROR_INTERNAL_ERROR;
                pf->reject = true;
            } else {
                pf->objs = snap;
                pf->obj_count = track->retained_count;
                pf->group_id = pub_track_retained_group(track);
                pf->end_object =
                    pub_track_retained_last_object_id(track) + 1;
                pf->priority = track->priority;
                pf->track = track;
                /* Committed together with the snapshot's ownership: a
                 * staging that failed above fixes nothing. */
                if (whole_group_d16)
                    hist_whole_group_fetch_seal(track, pf->group_id,
                                                pf->end_object - 1);
            }
        }
        /* No advancing call precedes ownership of every borrowed input. */
        *result = MOQ_PUB_EVENT_CONSUMED;
        return serve_retained_fetches(pub, now_us);
    }

    /* A fill request on a subscription this facade serves (draft-22 section
     * 3.4): AUTO_RETAINED's snapshot must cover the entire authoritative
     * inclusive range; send only that range, or fail with header then RESET
     * (section 3.4.1). A strict subset never finishes successfully. This is not
     * the explicit-FETCH whole-group policy or catalog-base requirement. The
     * subscription itself is never touched. A request for a subscription this
     * facade does not know stays IGNORED for a multiplexing caller. */
    if (event->kind == MOQ_EVENT_FILL_REQUEST) {
        const moq_fill_request_event_t *fr = &event->u.fill_request;
        moq_pub_track_t *track = moq_publication_is_valid(fr->pub)
            ? find_track_by_pub(pub, fr->pub)
            : find_track_by_sub(pub, fr->sub);
        if (!track) return MOQ_OK;
        *result = MOQ_PUB_EVENT_CONSUMED;

        pub_pending_fill_t *pf = pending_fill_find(pub, fr->fill);
        if (!pf || !moq_subscription_eq(fr->sub, pf->receipt.sub) ||
            !moq_publication_eq(fr->pub, pf->receipt.pub) ||
            fr->initiating_request_id != pf->receipt.initiating_request_id ||
            fr->start_group != pf->receipt.start_group ||
            fr->start_object != pf->receipt.start_object ||
            fr->end_group != pf->receipt.end_group ||
            fr->end_object != pf->receipt.end_object ||
            fr->processed_at_us != pf->receipt.processed_at_us ||
            fr->group_order != pf->receipt.group_order ||
            fr->subscriber_priority != pf->receipt.subscriber_priority ||
            fr->has_fill_timeout != pf->receipt.has_fill_timeout ||
            fr->fill_timeout_ms != pf->receipt.fill_timeout_ms) {
            *result = MOQ_PUB_EVENT_ERROR;
            return MOQ_ERR_WRONG_STATE;
        }
        if (pf->requested) return MOQ_OK;
        pf->requested = true;
        moq_pub_fill_decision_t decision = MOQ_PUB_FILL_AUTO_RETAINED;
        if (pub->cfg.on_fill_request) {
            moq_pub_fill_request_t info = {
                .struct_size = sizeof(info), .fill = pf->token,
                .sub = fr->sub, .pub = fr->pub,
                .initiating_request_id = fr->initiating_request_id,
                .start_group = fr->start_group, .start_object = fr->start_object,
                .end_group = fr->end_group, .end_object = fr->end_object,
                .processed_at_us = fr->processed_at_us,
                .fill_timeout_ms = fr->fill_timeout_ms,
                .group_order = (uint8_t)fr->group_order,
                .subscriber_priority = fr->subscriber_priority,
                .has_fill_timeout = fr->has_fill_timeout
            };
            pub->in_fill_callback = true;
            decision = pub->cfg.on_fill_request(pub->cfg.on_fill_request_ctx, track, &info);
            pub->in_fill_callback = false;
        }
        if (!pf->abort && decision == MOQ_PUB_FILL_DEFER) {
            pf->deferred = true;
            return MOQ_OK;
        }
        if (!pf->abort && decision != MOQ_PUB_FILL_AUTO_RETAINED) {
            pf->abort = true;
            pf->abort_code = MOQ_STREAM_ERROR_INTERNAL_ERROR;
        }
        /* The snapshot must cover the ENTIRE authoritative inclusive range,
         * not merely fit inside it. No end+1 arithmetic at UINT64_MAX. */
        if (!pf->abort && (pf->obj_count == 0 ||
            fr->start_group != pf->group_id || fr->end_group != pf->group_id ||
            fr->start_object < pf->objs[0].object_id ||
            fr->end_object > pf->objs[pf->obj_count - 1].object_id)) {
            pf->abort = true;
            pf->abort_code = MOQ_STREAM_ERROR_UNKNOWN_OBJECT_STATUS;
        }
        while (pf->next_idx < pf->obj_count &&
               pf->objs[pf->next_idx].object_id < fr->start_object) pf->next_idx++;
        serve_pending_fill(pub, pf, now_us);
        if (pending_fill_work(pf))
            return MOQ_ERR_WOULD_BLOCK;   /* deferred; the tick drains it */
        return MOQ_OK;
    }

    /* The session ended a fill this facade was still answering (the parent
     * was cancelled or ended, or the peer stopped the stream): drop the
     * snapshot. Nothing to send -- the child's teardown is the session's. */
    if (event->kind == MOQ_EVENT_FILL_TERMINAL) {
        pub_pending_fill_t *pf = pending_fill_find(pub, event->u.fill_terminal.fill);
        if (!pf) return MOQ_OK;
        const moq_fill_terminal_event_t *term = &event->u.fill_terminal;
        if (!moq_subscription_eq(term->sub, pf->receipt.sub) ||
            !moq_publication_eq(term->pub, pf->receipt.pub) ||
            term->initiating_request_id != pf->receipt.initiating_request_id)
            return MOQ_ERR_WRONG_STATE;
        if (pub->cfg.on_fill_terminal) {
            moq_pub_fill_terminal_t info = {
                .struct_size = sizeof(info), .fill = pf->token,
                .sub = term->sub, .pub = term->pub,
                .initiating_request_id = term->initiating_request_id,
                .reason = term->reason, .error_domain = term->error_domain,
                .error_code = term->error_code
            };
            pub->in_fill_callback = true;
            pub->cfg.on_fill_terminal(pub->cfg.on_fill_terminal_ctx, pf->track, &info);
            pub->in_fill_callback = false;
        }
        pending_fill_clear(pub, pf);
        *result = MOQ_PUB_EVENT_CONSUMED;
        return MOQ_OK;
    }

    if (event->kind == MOQ_EVENT_SUBSCRIBE_UPDATED) {
        moq_pub_track_t *t = find_track_by_sub(pub,
            event->u.subscribe_updated.sub);
        if (!t) return MOQ_OK;
        const moq_subscribe_updated_event_t *u = &event->u.subscribe_updated;
        if (event->detail_size >= offsetof(moq_subscribe_updated_event_t, new_group_request) +
                                  sizeof(u->new_group_request) &&
            u->has_new_group_request)
            pub_new_group_request(pub, t, u->new_group_request);
        if (event->detail_size >= sizeof(*u) && u->answer_owed) {
            pub_pending_t local = {0};
            pub_pending_t *p = &local;
            p->active = p->accept = p->update = true;
            p->track = t;
            p->sub = u->sub;
            p->update_id = u->update_request_id;
            p->fill_requested = u->fill_requested;
            p->has_filter = u->has_filter || u->options != NULL;
            p->update_info = (moq_pub_subscribe_update_info_t){
                .has_subscriber_priority = u->has_subscriber_priority,
                .subscriber_priority = u->subscriber_priority,
                .has_forward = u->has_forward, .forward = u->forward,
                .has_delivery_timeout = u->has_delivery_timeout,
                .delivery_timeout_us = u->delivery_timeout_us
            };
            *result = MOQ_PUB_EVENT_CONSUMED;
            return stage_update(pub, p, now_us);
        }
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
        const moq_publish_updated_event_t *u = &event->u.publish_updated;
        if (event->detail_size >= offsetof(moq_publish_updated_event_t, new_group_request) +
                                  sizeof(u->new_group_request) &&
            u->has_new_group_request)
            pub_new_group_request(pub, t, u->new_group_request);
        if (event->detail_size >= sizeof(*u) && u->answer_owed) {
            pub_pending_t local = {0};
            pub_pending_t *p = &local;
            p->active = p->accept = p->update = true;
            p->track = t;
            p->publication = u->pub;
            p->update_id = u->update_request_id;
            p->fill_requested = u->fill_requested;
            p->has_filter = u->has_filter || u->options != NULL;
            p->update_info = (moq_pub_subscribe_update_info_t){
                .has_subscriber_priority = u->has_subscriber_priority,
                .subscriber_priority = u->subscriber_priority,
                .has_forward = u->has_forward, .forward = u->forward,
                .has_delivery_timeout = u->has_delivery_timeout,
                .delivery_timeout_us = u->delivery_timeout_us
            };
            *result = MOQ_PUB_EVENT_CONSUMED;
            return stage_update(pub, p, now_us);
        }
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

    /* The peer refused the PUBLISH with REDIRECT: it ends as PUBLISH_ERROR
     * does, and the track keeps where it went (moq_pub_get_track_migration). */
    if (event->kind == MOQ_EVENT_REQUEST_REDIRECT &&
        event->u.request_redirect.family == MOQ_REQUEST_FAMILY_PUBLISH) {
        const moq_request_redirect_event_t *rd = &event->u.request_redirect;
        moq_pub_track_t *t = find_track_by_pub(pub, rd->handle.publication);
        if (!t) return MOQ_OK;
        *result = MOQ_PUB_EVENT_CONSUMED;
        facade_migration_set(&t->migration, &pub->alloc, true, rd->can_retry,
            rd->retry_after_ms, 0, rd->connect_uri, &rd->track_namespace,
            rd->track_name);
        t->publish_requested = false;
        t->publish_ok = false;
        pub_sub_slot_t *sl = track_find_publication_slot(t);
        if (sl) track_clear_slot(pub, sl);
        if (CB_HAS(&pub->callbacks, on_publish_error))
            pub->callbacks.on_publish_error(pub->callbacks.ctx, t, rd->error_code);
        return MOQ_OK;
    }

    /* The subscriber migrated an accepted subscription with a GOAWAY on its
     * request stream (draft-22 §9.2, draft-18 §10.4): the session has retired
     * it but left its data streams, so it leaves like an UNSUBSCRIBE except
     * that an open subgroup is reset here (track_retire_slot). Only that
     * subscription and its pending accept/update work go. */
    if (event->kind == MOQ_EVENT_REQUEST_GOAWAY &&
        event->u.request_goaway.family == MOQ_REQUEST_FAMILY_SUBSCRIBE) {
        moq_subscription_t sub = event->u.request_goaway.handle.subscription;
        for (moq_pub_track_t *t = pub->tracks; t; t = t->next) {
            pub_sub_slot_t *sl = track_find_slot_by_sub(t, sub);
            if (!sl) continue;
            track_retire_slot(pub, t, sl);
            *result = MOQ_PUB_EVENT_CONSUMED;
            if (pub->callbacks.on_subscriber_left)
                pub->callbacks.on_subscriber_left(pub->callbacks.ctx, t);
            break;
        }
        if (admission_cancel(pub, sub._opaque, false))
            *result = MOQ_PUB_EVENT_CONSUMED;
        if (pub->deferred.active && moq_subscription_eq(pub->deferred.sub, sub)) {
            pub->deferred.active = false;
            *result = MOQ_PUB_EVENT_CONSUMED;
        }
        return MOQ_OK;
    }

    /* The peer migrated the publication with a GOAWAY on its request stream
     * (draft-22 §9.2): it ends as a peer-terminated publication does, to be
     * published again at the new session, and the track keeps where. */
    if (event->kind == MOQ_EVENT_REQUEST_GOAWAY &&
        event->u.request_goaway.family == MOQ_REQUEST_FAMILY_PUBLISH) {
        const moq_request_goaway_event_t *ga = &event->u.request_goaway;
        moq_pub_track_t *t = find_track_by_pub(pub, ga->handle.publication);
        if (!t) return MOQ_OK;
        (void)admission_cancel(pub, ga->handle.publication._opaque, true);
        *result = MOQ_PUB_EVENT_CONSUMED;
        facade_migration_set(&t->migration, &pub->alloc, false, false, 0,
            ga->timeout_ms, ga->new_session_uri, NULL, (moq_bytes_t){ NULL, 0 });
        t->publish_requested = false;
        t->publish_ok = false;
        pub_sub_slot_t *sl = track_find_publication_slot(t);
        if (sl) track_retire_slot(pub, t, sl);
        if (CB_HAS(&pub->callbacks, on_publish_finished))
            pub->callbacks.on_publish_finished(pub->callbacks.ctx, t);
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
        (void)admission_cancel(pub, event->u.publish_unsubscribed.pub._opaque, true);
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

    /* A TRACK_STATUS for one of this publisher's tracks is answered as a
     * SUBSCRIBE would be (§9.13), without subscription state: the Largest
     * Object and Track Properties a SUBSCRIBE_OK would carry, or the refusal
     * a SUBSCRIBE would get. The callback is not consulted (it authorizes
     * subscriptions); in a callback mode the track must already have an
     * accepted subscription on this session, the rule a standalone FETCH
     * follows. A track that is not this publisher's stays IGNORED. */
    if (event->kind == MOQ_EVENT_TRACK_STATUS_REQUEST) {
        const moq_track_status_request_event_t *q = &event->u.track_status_request;
        moq_pub_track_t *track = find_track(pub, &q->track_namespace, q->track_name);
        if (!track) return MOQ_OK;
        *result = MOQ_PUB_EVENT_CONSUMED;
        pub_owed_answer_t a = { .kind = event->kind, .handle = q->handle._opaque,
                                .track = NULL, .code = 0 };
        if (track->ended)
            a.code = MOQ_REQUEST_ERROR_DOES_NOT_EXIST;
        else if (pub->cfg.accept_mode == MOQ_PUB_REJECT_ALL ||
                 (pub->cfg.accept_mode != MOQ_PUB_ACCEPT_ALL &&
                  !track_has_subscriber(track)))
            a.code = MOQ_REQUEST_ERROR_UNAUTHORIZED;
        else
            a.track = track;
        bool held = false;
        moq_result_t rc = pub_answer_or_owe(pub, &a, now_us, &held);
        if (rc < 0) return rc;
        return held ? MOQ_ERR_WOULD_BLOCK : MOQ_OK;
    }

    if (event->kind != MOQ_EVENT_SUBSCRIBE_REQUEST)
        return MOQ_OK;

    const moq_subscribe_request_event_t *req = &event->u.subscribe_request;
    bool fill_requested = event->detail_size >=
        offsetof(moq_subscribe_request_event_t, options) + sizeof(req->options) &&
        req->options && req->options->struct_size >= MOQ_SUBSCRIPTION_ADMISSION_OPTIONS_V0_SIZE &&
        (req->options->flags & MOQ_SUBSCRIPTION_ADMISSION_FILL) != 0;
    moq_pub_track_t *track = find_track(pub, &req->track_namespace,
                                          req->track_name);
    if (!track)
        return MOQ_OK;
    const bool new_group = event->detail_size >=
        offsetof(moq_subscribe_request_event_t, new_group_request) +
        sizeof(req->new_group_request) && req->has_new_group_request;

    *result = MOQ_PUB_EVENT_CONSUMED;
    pub_admission_t *admission = NULL;
    {
        bool fresh;
        moq_result_t rc = admission_claim(pub, req->sub._opaque, 0, 0,
                                           &admission, &fresh);
        if (rc < 0 || !admission) return rc;
        if (!fresh) return flush_admission(pub, admission, now_us);
        admission->track = track;
        admission->flags |= (req->forward ? PUB_ADMISSION_FORWARD : 0) |
                            (fill_requested ? PUB_ADMISSION_FILL : 0);
    }

    bool want_accept;
    moq_request_error_t reject_code = 0;

    if (track->ended) {
        /* The track is terminal (END_OF_TRACK already emitted / ended locally).
         * A late subscriber must not join it -- reject deterministically rather
         * than installing a slot that would receive neither media nor a
         * terminal. Checked before the app callback so it is never consulted. */
        want_accept = false;
        reject_code = MOQ_REQUEST_ERROR_DOES_NOT_EXIST;
    } else if (!moq_session_supports_concurrent_subscriptions(pub->session) &&
               track_has_subscriber(track)) {
        want_accept = false;
        reject_code = MOQ_REQUEST_ERROR_DUPLICATE_SUBSCRIPTION;
    } else if (pub->cfg.accept_mode == MOQ_PUB_REJECT_ALL) {
        want_accept = false;
        reject_code = MOQ_REQUEST_ERROR_UNAUTHORIZED;
    } else if (pub->cfg.accept_mode == MOQ_PUB_CALLBACK ||
               pub->cfg.accept_mode == MOQ_PUB_CALLBACK_V1) {
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
        moq_pub_accept_decision_t decision;
        if (pub->cfg.accept_mode == MOQ_PUB_CALLBACK_V1) {
            moq_pub_subscribe_info_v1_t v1 = {
                .struct_size = sizeof(v1),
                .track = info.track,
                .track_namespace = info.track_namespace,
                .track_name = info.track_name,
                .filter = info.filter,
                .subscriber_priority = info.subscriber_priority,
                .group_order = info.group_order,
                .forward = info.forward,
                .start_group = info.start_group,
                .start_object = info.start_object,
                .end_group = info.end_group,
                .delivery_timeout_us = info.delivery_timeout_us,
                .tokens = info.tokens,
                .token_count = info.token_count,
                .deferred = info.deferred,
                .deferred_id = info.deferred_id,
                .options = NULL,
            };
            if (event->detail_size >= offsetof(moq_subscribe_request_event_t, options) +
                                      sizeof(req->options))
                v1.options = req->options;
            decision = pub->cfg.on_subscribe_v1(pub->cfg.on_subscribe_v1_ctx,
                                               &v1, &cb_error);
        } else {
            decision = pub->cfg.on_subscribe(pub->cfg.on_subscribe_ctx,
                                            &info, &cb_error);
        }

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
                pub->deferred.fill_requested = fill_requested;
                pub->deferred.has_new_group = new_group;
                pub->deferred.new_group = new_group ? req->new_group_request : 0;
                if (admission) admission->flags |= PUB_ADMISSION_DEFERRED;
                return MOQ_OK;
            }
        } else {
            want_accept = false;
            reject_code = MOQ_REQUEST_ERROR_INTERNAL_ERROR;
        }
    } else {
        want_accept = true;
    }

    if (want_accept && new_group)
        pub_new_group_request(pub, track, req->new_group_request);

    if (want_accept) admission->flags |= PUB_ADMISSION_ACCEPT;
    admission->reject_code = reject_code;
    return flush_admission(pub, admission, now_us);
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
    if (pub->in_fill_callback) return MOQ_ERR_WRONG_STATE;
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
    if (pub->in_fill_callback) return MOQ_ERR_WRONG_STATE;
    if (deferred != &pub->deferred) return MOQ_ERR_INVAL;
    if (!deferred->active || deferred->generation != deferred_id)
        return MOQ_ERR_STALE_HANDLE;

    pub_admission_t *a = admission_cell(pub, deferred->sub._opaque, false);
    if (!a || !(a->flags & PUB_ADMISSION_DEFERRED) ||
        a->parent != deferred->sub._opaque)
        return MOQ_ERR_STALE_HANDLE;
    if (!admission_live(pub, a->parent, a->flags, a->update_id)) {
        admission_clear(pub, a);
        return MOQ_ERR_STALE_HANDLE;
    }
    a->flags &= ~PUB_ADMISSION_DEFERRED;
    if (accept) a->flags |= PUB_ADMISSION_ACCEPT;
    a->reject_code = error_code;
    deferred->active = false;
    if (accept && deferred->has_new_group)
        pub_new_group_request(pub, deferred->track, deferred->new_group);
    return flush_admission(pub, a, now_us);
}

moq_result_t moq_pub_flush(moq_publisher_t *pub, uint64_t now_us) {
    if (!pub) return MOQ_ERR_INVAL;
    if (pub->in_fill_callback) return MOQ_ERR_WRONG_STATE;
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
    if (pub->in_fill_callback) return MOQ_ERR_WRONG_STATE;
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
    /* The publish cfg's properties when it has some, else the track's. */
    pcfg.track_properties = cfg->track_properties.len > 0
        ? cfg->track_properties
        : (moq_bytes_t){ track->props, track->props_len };
    pcfg.auth_tokens = cfg->auth_tokens;
    pcfg.auth_token_count = cfg->auth_token_count;

    moq_publication_t handle;
    moq_result_t rc = moq_session_publish(pub->session, &pcfg, now_us, &handle);
    if (rc < 0) return rc;   /* REQUEST_BLOCKED / WOULD_BLOCK: nothing bound, retry */

    track->publication = handle;
    if (moq_session_supports_fill(pub->session)) {
        int slot = pub_resolve_handle(pub->session, handle);
        if (slot >= 0) pub->session->publishes[slot].facade_defer_updates = true;
    }
    facade_migration_clear(&track->migration, &pub->alloc);
    track->publish_requested = true;
    track->publish_forward = cfg->has_forward ? cfg->forward : true;
    return MOQ_OK;
}

moq_result_t moq_pub_unpublish_track(moq_publisher_t *pub,
                                     moq_pub_track_t *track,
                                     uint64_t now_us)
{
    if (!pub || !track) return MOQ_ERR_INVAL;
    if (pub->in_fill_callback) return MOQ_ERR_WRONG_STATE;
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
        moq_result_t rc = slot_close_subgroup(pub, track, psl, now_us);
        if (rc == MOQ_ERR_WOULD_BLOCK) return MOQ_ERR_WOULD_BLOCK;
        if (rc != MOQ_OK && rc != MOQ_ERR_STALE_HANDLE) return rc;
        psl->sg_open = false;
    }

    moq_finish_publish_cfg_t fcfg;
    moq_finish_publish_cfg_init(&fcfg);
    /* A normal end: the track is no longer being published (TRACK_ENDED).
     * The zeroed default is INTERNAL_ERROR, which would report this normal
     * end as an error. */
    fcfg.status_code = MOQ_PUB_DONE_TRACK_ENDED;
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

moq_result_t moq_pub_get_track_migration(const moq_publisher_t *pub,
    const moq_pub_track_t *track, moq_request_migration_t *out, size_t out_size)
{
    if (!pub || !track) return MOQ_ERR_INVAL;
    for (const moq_pub_track_t *t = pub->tracks; t; t = t->next)
        if (t == track) return facade_migration_get(&t->migration, out, out_size);
    return MOQ_ERR_INVAL;
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
    if (pub->in_fill_callback) return MOQ_ERR_WRONG_STATE;

    /* Progress previously-staged work: a pending accept (-> on_subscriber_
     * joined), a deferred retained FETCH, and retirements armed in earlier
     * ticks. Shared with moq_pub_flush so the two never drift. */
    moq_result_t frc = pub_progress_staged(pub, now_us);
    if (frc < 0 && frc != MOQ_ERR_WOULD_BLOCK) {
        if (frc == MOQ_ERR_CLOSED) drain_closed_event(pub);
        return frc;
    }

    moq_event_t ev;
    size_t remaining = pub->session->event_cap;
    while (remaining--) {
        size_t count = 0;
        moq_result_t prc = moq_session_poll_events_ex(pub->session, &ev, 1,
                                                      sizeof(ev), &count);
        if (prc < 0) return prc;
        if (!count) break;
        /* One canonical dispatcher for every event -- identical handling to
         * the public moq_pub_handle_event path. A WOULD_BLOCK means the event
         * staged work (pending accept / deferred FETCH / armed retirement);
         * the next tick, via pub_progress_staged, drains it. */
        moq_pub_event_result_t res = MOQ_PUB_EVENT_IGNORED;
        moq_result_t drc = pub_dispatch_event(pub, &ev, now_us, &res);
        if (drc == MOQ_ERR_WOULD_BLOCK && res != MOQ_PUB_EVENT_CONSUMED) {
            moq_event_cleanup(&ev);
            return MOQ_ERR_WOULD_BLOCK;
        }
        if (drc < 0 && drc != MOQ_ERR_WOULD_BLOCK) {
            moq_event_cleanup(&ev);
            if (drc == MOQ_ERR_CLOSED)
                drain_closed_event(pub);
            return drc;
        }
        /* Nothing else polls this session: answer a request nobody took. */
        if (drc == MOQ_OK && res == MOQ_PUB_EVENT_IGNORED) {
            moq_result_t urc = pub_refuse_unserved(pub, &ev, now_us);
            if (urc < 0) {
                moq_event_cleanup(&ev);
                if (urc == MOQ_ERR_CLOSED) drain_closed_event(pub);
                return urc;
            }
        }
        moq_event_cleanup(&ev);
    }

    /* Progress work armed by events processed THIS tick (Forward-0
     * retirements etc.). */
    return pub_progress_staged(pub, now_us);
}

#ifdef MOQ_PUB_TESTING
/* White-box observation of a track's subscription destination slot,
 * compiled ONLY into moq-core-test-internals: the resolved window installed
 * for it and its terminal stream state. The track's first active
 * subscription slot; false once it has none. */
bool moq_pub_test_sub_slot(moq_pub_track_t *t, moq_resolved_window_t *window,
                           bool *eot_sg, bool *eot_done, bool *sg_open);
bool moq_pub_test_sub_slot(moq_pub_track_t *t, moq_resolved_window_t *window,
                           bool *eot_sg, bool *eot_done, bool *sg_open)
{
    pub_sub_slot_t *sl = NULL;
    for (size_t i = 0; t && !sl && i < t->slot_cap; i++)
        if (t->slots[i].active && t->slots[i].kind == PUB_SLOT_SUBSCRIPTION)
            sl = &t->slots[i];
    if (!sl) return false;
    *window = sl->window;
    *eot_sg = sl->eot_sg;
    *eot_done = sl->eot_done;
    *sg_open = sl->sg_open;
    return true;
}
#endif
