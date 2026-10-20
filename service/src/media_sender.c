#if defined(__linux__) && !defined(_POSIX_C_SOURCE)
#define _POSIX_C_SOURCE 200809L
#endif

/*
 * moq_media_sender_t — send-side media facade over an endpoint
 * (MEDIA_SERVICE_DESIGN.md §7). Tracks, catalog, readiness, and the media
 * write path: objects written from the app thread are enqueued in decode
 * order into a bounded, per-track sync-anchored queue and drained to the
 * session on the network thread, generating the standard LOC properties
 * for the sender's companion (legacy LOC-01 on drafts16/18, LOC-04 metadata
 * on draft22; AUTO picks the one matching the negotiated draft), applying
 * backpressure policy, and
 * abandoning open subgroups wire-correctly (reset_group) on a drop.
 *
 * Threading mirrors the receiver: the publisher lives on the endpoint's
 * network thread, driven from the SENDER pump hook (run under the endpoint
 * mutex; no public endpoint API re-entry). The application calls
 * add_track / write from its own thread; the mutex below is the
 * happens-before edge between the two.
 */

#include <moq/media_sender.h>
#include <moq/publisher.h>
#include <moq/msf.h>
#include <moq/loc.h>
#include <moq/wire.h>   /* MOQ_QUIC_VARINT_MAX */

#include "endpoint_internal.h"
#include "../../core/src/spi/core_spi.h"

#ifdef MOQ_SERVICE_HAVE_MPEG2TS
#include <moq/mpeg2ts.h>
#endif

#include <errno.h>
#include <limits.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stddef.h>
#include <string.h>
#include <time.h>

#define SENDER_DEFAULT_PRE_READY_OBJECTS 256u
#define SENDER_DEFAULT_PRE_READY_BYTES   (32u * 1024u * 1024u)
#define SENDER_DEFAULT_QUEUE_OBJECTS     256u
#define SENDER_DEFAULT_QUEUE_BYTES       (32u * 1024u * 1024u)
#define SENDER_DEFAULT_BLOCK_TIMEOUT_US  1000000ull

/* Frozen v0 base size for the caller-filled stats struct (see media_receiver.c):
 * the validation floor, computed from the last v0 field so it stays constant as
 * fields are appended. Must NOT change when fields are added. */
#define MEDIA_SENDER_STATS_V0_SIZE \
    (offsetof(moq_media_sender_stats_t, last_error) + sizeof(moq_result_t))

/* Minimum accepted moq_media_track_cfg_t size: the prefix through the last
 * pre-V3 field (everything before has_track_duration). An add_track caller must
 * supply at least this; appended fields default absent for an older/smaller
 * cfg, and a newer/larger cfg's unknown trailing fields are ignored. */
#define MEDIA_TRACK_CFG_MIN_SIZE \
    MOQ_MEDIA_TRACK_CFG_V0_SIZE

#define MEDIA_TRACK_CFG_SIZE_THROUGH_ALT_GROUP \
    ((offsetof(moq_media_track_cfg_t, alt_group) + \
      sizeof(((moq_media_track_cfg_t *)0)->alt_group) + \
      _Alignof(moq_media_track_cfg_t) - 1u) & \
     ~((size_t)_Alignof(moq_media_track_cfg_t) - 1u))
_Static_assert(offsetof(moq_media_track_cfg_t, has_publisher_priority) >=
               MEDIA_TRACK_CFG_SIZE_THROUGH_ALT_GROUP,
               "publisher priority must follow the complete historical layout");
/* The add_track floor ends where the first appended field begins. */
_Static_assert(MOQ_MEDIA_TRACK_CFG_V0_SIZE ==
                   offsetof(moq_media_track_cfg_t, sap_timeline_history_groups) +
                   sizeof(uint32_t),
               "the track cfg floor is the layout through "
               "sap_timeline_history_groups");
/* The packaging-extension block starts at or past the struct size the
 * layout had through publisher_priority, so no older caller's storage is read
 * as part of it. */
_Static_assert(offsetof(moq_media_track_cfg_t, extensions) >=
                   ((offsetof(moq_media_track_cfg_t, publisher_priority) +
                     sizeof(uint8_t) + _Alignof(uint64_t) - 1u) &
                    ~((size_t)_Alignof(uint64_t) - 1u)),
               "extensions must follow the complete layout through "
               "publisher_priority");
/* track_properties starts at the uint64_t-aligned boundary after the MPEG2TS
 * block, so a caller whose struct ends with that block (its sizeof, padding
 * included) never covers any part of it. */
#define MEDIA_TRACK_CFG_MPEG2TS_END \
    (offsetof(moq_media_track_cfg_t, mpeg2ts_forward_before_random_access) + \
     sizeof(((moq_media_track_cfg_t *)0)->mpeg2ts_forward_before_random_access))
_Static_assert(offsetof(moq_media_track_cfg_t, track_properties) ==
               ((MEDIA_TRACK_CFG_MPEG2TS_END + _Alignof(uint64_t) - 1u) &
                ~((size_t)_Alignof(uint64_t) - 1u)),
               "track_properties must begin at the aligned end of the MPEG2TS block");
_Static_assert(offsetof(moq_media_track_cfg_t, dynamic_groups) ==
               offsetof(moq_media_track_cfg_t, track_properties) +
               sizeof(moq_bytes_t),
               "dynamic_groups must directly follow track_properties");
_Static_assert(offsetof(moq_media_track_cfg_t, group_order) ==
               ((offsetof(moq_media_track_cfg_t, dynamic_groups) + sizeof(bool) +
                 _Alignof(uint64_t) - 1u) & ~((size_t)_Alignof(uint64_t) - 1u)),
               "group_order must begin at the aligned end of dynamic_groups");

/* Minimum accepted moq_media_sender_cfg_t size: the prefix through the last
 * pre-callbacks field. A create/attach caller must supply at least this; the
 * appended `callbacks` field is read only when struct_size covers it, so an
 * older/smaller cfg keeps working (no callbacks) and a newer/larger cfg's
 * unknown trailing fields are ignored. Must NOT change when fields are added. */
#define MEDIA_SENDER_CFG_MIN_SIZE \
    offsetof(moq_media_sender_cfg_t, callbacks)

/* Automatic catalog refresh is OPT-IN. MSF-01 §5: a catalog object SHOULD be
 * published only when track availability changes, or after enough time that
 * the prior object might have left a delivery-network cache. A cfg field of 0
 * (or an old-size caller whose struct_size predates it) therefore resolves to
 * the disabled sentinel below; only an explicit finite interval republishes on
 * a timer. */
#define SENDER_CATALOG_REFRESH_DISABLED UINT64_MAX

/* ABI pin for the appended catalog_refresh_interval_us. The useful, portable
 * contract is that the field begins exactly at the aligned boundary AFTER the
 * previous last field (drop_without_demand) -- derived independently here, no
 * hard-coded offset (which would reject otherwise-valid ABIs on other targets).
 * This keeps a caller built against the pre-refresh struct bit-compatible and
 * makes the whole-field struct_size gate exact. */
#define SENDER_CFG_PRE_REFRESH_END \
    (offsetof(moq_media_sender_cfg_t, drop_without_demand) + \
     sizeof(((moq_media_sender_cfg_t *)0)->drop_without_demand))
#define SENDER_CFG_REFRESH_ALIGN _Alignof(uint64_t)
_Static_assert(
    offsetof(moq_media_sender_cfg_t, catalog_refresh_interval_us) ==
        ((SENDER_CFG_PRE_REFRESH_END + SENDER_CFG_REFRESH_ALIGN - 1) &
         ~(size_t)(SENDER_CFG_REFRESH_ALIGN - 1)),
    "catalog_refresh_interval_us must begin at the old aligned sizeof boundary");

#define SENDER_DEFAULT_SAP_HISTORY_GROUPS 8u
#define SENDER_DEFAULT_MEDIA_TIMELINE_HISTORY_GROUPS 8u

/* -- Internal types --------------------------------------------------- */

/* One SAP-timeline record (CMSF §3.6.1): the SAP type and earliest
 * presentation time of the media Object at the given media Location. Kept in a
 * generated eventtimeline track's bounded history and serialized to the wire as
 * {"l":[group,object],"data":[sapType,ept_ms]}. */
typedef struct sender_sap_rec {
    uint64_t media_group;
    uint64_t media_object;
    uint64_t ept_ms;
    uint8_t  sap_type;        /* 0..3 */
} sender_sap_rec_t;

struct moq_media_track {
    /* Catalog-relevant cfg, deep-copied into `strings` (stable for the
     * sender's life so the MSF encode -- which borrows -- is safe). */
    moq_bytes_t           name;
    moq_media_type_t      media_type;
    moq_media_packaging_t packaging;
    moq_bytes_t           codec;
    uint32_t              timescale;
    moq_bytes_t           init_data;        /* raw (pre-base64) */
    /* The initDataList id this track's initRef names (CMAF, and every
     * init-bearing track of the draft-22 companion). Assigned once at
     * add_track and never changed: tracks with identical init bytes share one
     * id, which stays bound to those bytes for the sender's life, so a
     * surviving track's initRef does not move when the track that introduced
     * the entry is removed (MSF-01 5.3). Borrows the introducing track's name
     * (track objects live until destroy) or init_ref_alloc. Empty = none. */
    moq_bytes_t           init_ref;
    uint8_t              *init_ref_alloc;   /* owned id when the name is taken */
    size_t                init_ref_alloc_size;
    bool                  has_cmaf_init_track_id; /* parsed from a CMAF init seg */
    uint32_t              cmaf_init_track_id;     /* for §3.3 track_ID validation */
    moq_bytes_t           role;
    moq_bytes_t           lang;
    bool                  is_live;
    uint32_t              width, height;
    uint64_t              framerate_millis;
    uint32_t              samplerate;
    moq_bytes_t           channel_config;
    uint64_t              bitrate;
    bool                  has_track_duration;  /* MSF §5.2.35 (non-live only) */
    uint64_t              track_duration_ms;
    bool                  has_alt_group;       /* CMSF §3.2 (switching set) */
    int                   alt_group;
    bool                  has_publisher_priority;
    uint8_t               publisher_priority;
    moq_group_order_t     group_order;         /* MOQT 7.1; DEFAULT = none */

    /* CMSF authoring metadata (copied at add_track). cp_ref_ids is its own
     * allocation; the id bytes live in `strings` with the other spans. */
    bool                  has_max_grp_sap;
    uint32_t              max_grp_sap;
    bool                  has_max_obj_sap;
    uint32_t              max_obj_sap;
    moq_bytes_t          *cp_ref_ids;
    size_t                cp_ref_id_count;

    uint8_t              *strings;           /* owned backing for the spans */
    size_t                strings_size;

    moq_bytes_t           track_properties;  /* in `strings`; empty = none */
    /* Whether the Track Properties declare a LOC-04 Timescale, which a
     * draft-22 receiver applies to every object without its own
     * (sender_track_scale_of). Set at add_track; immutable. */
    uint8_t               track_scale;
    bool                  dynamic_groups;
    /* A subscriber's new-group request not yet taken (mu). */
    bool                  new_group_pending;
    uint64_t              new_group_value;

    moq_pub_track_t      *pub_track;         /* NULL until the hook adds it */
    size_t                active_subs;       /* mirrored subscriber count (mu);
                                                reconciled on the network thread
                                                from moq_pub_active_subscriptions()
                                                for demand visibility (§7.2) */
    bool                  is_catalog;        /* the catalog track */
    bool                  pub_closed_fired;  /* on_track_closed fired: latched so a
                                                repeated PUBLISH_FINISHED does not re-fire */
    bool                  removed;           /* remove_track called: excluded from
                                                future catalog builds, writes
                                                refused; handle kept until destroy */
    bool                  in_published;       /* track is part of the last committed
                                                catalog generation -- the baseline
                                                the next deltaUpdate diffs against
                                                . */
    bool                  catalog_meta_dirty; /* a surviving (still-published) tuple's
                                                catalog metadata changed in place
                                                (e.g. live->VOD) -- a change a
                                                deltaUpdate's add/remove ops cannot
                                                express, so the next generation
                                                carrying it MUST be a full independent
                                                catalog. Set by the mutating API under
                                                mu; cleared when that generation is
                                                staged (sender_commit_baseline). */
    bool                  vod_converted;      /* convert_to_vod() flipped it to VOD
                                                (V4): isLive=false + trackDuration,
                                                writes refused (no new content). */
    bool                  vod_finish_pending; /* convert_to_vod() armed the §11.3
                                                step-1 finish: active subscribers
                                                must be sent Track Ended before the
                                                conversion catalog may publish.
                                                Set on the app thread under mu,
                                                cleared by the network pump once
                                                the finish succeeds. */
    bool                  pub_ended;         /* removed track's pub_track ended once */
    bool                  teardown_done;     /* removed track fully torn down (pub
                                                ended + removed, or never had one);
                                                mu-published so a same-name re-add
                                                knows the name is free again */

    /* Service-assigned grouping (§7.3), written by the app thread under mu
     * when enqueuing. */
    uint64_t              group_seq;         /* current group id */
    bool                  group_started;     /* any group ever assigned */
    bool                  group_open;        /* current group not yet ended */
    bool                  end_requested;     /* end_track called (app thread, mu);
                                                further writes are refused and a
                                                terminal entry is queued once */
    bool                  cur_group_anchored;/* current group opened by a sync
                                                point (the anchor may already
                                                be emitted, not just queued) */

    /* Wire-emission state (network thread, but read by the app thread under
     * mu to decide whether a drop must abandon the open subgroup). */
    uint64_t              emit_group;        /* group currently open on wire */
    bool                  emit_open;         /* a subgroup is open on the wire */
    uint64_t              emit_obj;          /* next object id in emit_group */
    bool                  pending_reset;     /* a drop abandoned emit_group;
                                                the drain must reset it */
    bool                  skip_open;         /* a no-demand drop cut skip_group
                                                short, so the rest of it is
                                                dropped even once demand returns */
    uint64_t              skip_group;

    /* Per-pass drain scratch (network thread, mu held). Cleared at the top of
     * every sender_drain pass; never observable outside it.
     *   drain_hold    -- this track has a retained entry, so EVERY later entry
     *                    of the track (END_OF_TRACK included) must be retained
     *                    too, which is what preserves per-track FIFO order and
     *                    stops a terminal overtaking its own held media.
     *   drain_dropped -- this track already armed its once-per-pass wire RESET
     *                    for a no-demand discard. Drop COUNTING is cumulative
     *                    and lives below, not here.
     *   drain_demand  -- cached demand answer: -1 unknown, 0 no, 1 yes, so the
     *                    pass makes at most one demand query per track. */
    bool                  drain_hold;
    bool                  drain_dropped;
    /* This pass already ATTEMPTED this track's pending reset (from the queue
     * path). Distinct from drain_dropped, which is no-demand drop accounting.
     * Nothing frees capacity between the queue path and the post-pass sweep,
     * so a second attempt in the same pass would be futile: the sweep skips a
     * track already attempted, and the obligation stays retryable for the next
     * production hook. */
    bool                  drain_reset_tried;
    int8_t                drain_demand;

    /* CUMULATIVE dropped-group identity for this track, for the whole track
     * lifetime -- NOT per pass. `groups_dropped` is a cumulative distinct
     * (track, group_seq) count, so two objects of the same group discarded in
     * different drain passes (or by different discard paths) must count once.
     * group_seq is monotonic per track, so the last one is sufficient. */
    bool                  drop_group_valid;
    uint64_t              drop_group_last;

    /* CMSF SAP event-timeline generation (§3.6.1 / MSF §8.3). A media track
     * with emit_sap_timeline owns a generated sibling eventtimeline track
     * (sap_timeline); that timeline track has is_event_timeline set and is the
     * one that carries the fields below. The two are linked so a write to the
     * media track can record into the timeline's history. */
    bool                  is_event_timeline; /* this IS a generated timeline track */
    moq_media_track_t    *sap_timeline;      /* media track -> its timeline (NULL otherwise) */
    moq_bytes_t          *depends;           /* timeline: [media track name]; freed by track_free */
    size_t                depends_count;

    /* Timeline history + emission (network thread, mu held). Records are
     * appended at media emit time (so they match the real wire Location and a
     * dropped Object yields no record), capped to sap_history_groups media
     * groups. Emission is subscriber-gated and never enters the media FIFO. */
    sender_sap_rec_t     *sap_recs;
    uint32_t              sap_rec_count, sap_rec_cap;
    uint32_t              sap_history_groups; /* K (>=1) */
    uint64_t              sap_base_seq;        /* monotonic seq of sap_recs[0] */
    uint64_t              sap_lead;            /* records already introduced on the wire */
    bool                  sap_open;            /* a timeline group is open on the wire */
    uint64_t              sap_obj_id;          /* next object id in the open timeline group */
    uint64_t              sap_cur_media_group; /* media group of the open timeline group
                                                  (the timeline shares media group ids) */

    /* MSF §7 media-timeline generation (§7.1.1/§7.3), kept separate from the SAP
     * state above. A media track with emit_media_timeline owns a generated
     * sibling mediatimeline track (media_timeline) with is_media_timeline set,
     * which carries the fields below. Records are explicit §7.1.1 entries stored
     * directly in codec form; the sender records one per media group-start. */
    bool                  is_media_timeline; /* this IS a generated mediatimeline track */
    moq_media_track_t    *media_timeline;    /* media track -> its mediatimeline (else NULL) */
    moq_msf_media_timeline_record_t *mt_recs;
    uint32_t              mt_rec_count, mt_rec_cap;
    uint32_t              mt_history_groups;  /* K (>=1) */
    uint64_t              mt_base_seq;        /* monotonic seq of mt_recs[0] */
    uint64_t              mt_lead;            /* records already introduced on the wire */
    bool                  mt_open;            /* a timeline group is open on the wire */
    uint64_t              mt_obj_id;          /* next object id in the open timeline group */
    uint64_t              mt_cur_media_group; /* media group of the open timeline group */

    /* MSF packaging-extension records, deep-copied at add_track into one
     * block (entries, then each record with its SI PIDs and names); the
     * catalog encode borrows them. */
    moq_msf_track_ext_t  *ext;
    size_t                ext_count;
    size_t                ext_block_size;

    /* Generic per-track counters (mu), maintained at the same chokepoints as
     * the sender-wide stats; the mpeg2ts_ counters are filled at snapshot. */
    moq_media_sender_track_stats_t tstats;

    /* MPEG2TS. ts_app_objects: Objects come through write(); otherwise the
     * byte-stream state below (allocated at add_track) forms them.
     * ts_random_access: Group starts must hold a random access point
     * (app-Object mode, when the record declares it). */
    bool                  ts_app_objects;
    bool                  ts_random_access;
    void                 *ts;
};

/* One queued media object (owns its refs until drained or freed). */
typedef struct sender_preq_entry {
    moq_media_track_t *track;
    bool               is_end;              /* terminal END_OF_TRACK marker
                                               (payload/properties NULL) */
    moq_rcbuf_t       *payload;
    moq_rcbuf_t       *properties;          /* app EXTRA props (passthrough) */
    bool               is_sync;             /* a stream anchor (see admit) */
    bool               keyframe;            /* holds a sync point */
    bool               starts_group;
    bool               ends_group;
    uint64_t           group_seq;
    uint64_t           pts_us;              /* presentation time; LOC ts fallback */
    bool               has_capture_time;    /* legacy LOC-01 capture presence */
    uint64_t           capture_time_us;     /* legacy UNIX microseconds */
    size_t             bytes;
    /* App-declared SAP type for the generated timeline (when the track has one).
     * Recorded at emit time so the timeline record carries the real Location. */
    bool               has_sap_type;
    uint8_t            sap_type;            /* 0..3 when has_sap_type */
} sender_preq_entry_t;

struct moq_media_sender {
    moq_alloc_t      alloc;
    moq_endpoint_t  *ep;
    bool             owns_endpoint;

    /* The negotiated transport draft, latched from the LIVE SESSION at the
     * top of every sender_hook and read under mu by the property encoder.
     * It is NOT taken from endpoint configuration or an offered preference:
     * only the established session knows what was actually negotiated.
     * Zero means "not yet observed", and encoding fails closed on it -- the
     * sender never falls back to a draft. */
    moq_version_t    transport_version;
    /* The companion in effect, under mu. Explicit LEGACY/D22 is fixed at
     * creation. AUTO (companion_auto) stays MOQ_MEDIA_COMPANION_AUTO until
     * the first latch of a draft-16/18/22 session, which replaces it once
     * with the matching companion (sender_resolve_companion_locked). */
    moq_media_companion_profile_t companion_profile;
    bool             companion_auto;
    /* AUTO resolved to draft-22 while a registered CMAF track has no
     * init_data (CMSF-01 3.1). The companion is then unusable, reported as
     * MOQ_ERR_UNSUPPORTED, until such tracks are removed. Under mu. */
    bool             companion_blocked;

    /* Namespace + catalog track name, deep-copied (same idiom as the
     * receiver): [ns part bytes...][catalog name bytes]. */
    moq_bytes_t     *ns_parts;
    size_t           ns_data_size;
    uint8_t         *ns_data;
    moq_namespace_t  namespace_;
    const uint8_t   *catalog_name;
    size_t           catalog_name_len;

    /* CMSF root contentProtections, deep-copied from cfg at create/attach.
     * `cp_strings` backs every nested string; each entry's default_kids array
     * is its own allocation (freed per entry). See free_root_cps(). */
    moq_cmsf_content_protection_t *content_protections;
    size_t           content_protection_count;
    uint8_t         *cp_strings;
    size_t           cp_strings_size;

    /* Demand-visibility callbacks (§7.2), copied from cfg at create/attach.
     * Fired (on the network thread, outside s->mu) when a subscriber joins or
     * leaves an app-visible media track. */
    moq_media_sender_callbacks_t callbacks;

    moq_media_send_backpressure_t backpressure;
    bool             validate_cmaf;       /* CMSF §3.3/§3.4 object validation;
                                             strict by default (cfg_init) */
    bool             publish_tracks;      /* publisher-initiated PUBLISH per
                                             track + live initial catalog */
    bool             drop_without_demand; /* drop queued media without demand
                                             (live edge); default: hold */
    uint64_t         block_timeout_us;
    uint32_t         preq_cap;            /* pre-ready object bound */
    uint64_t         preq_byte_cap;
    uint32_t         queue_cap;           /* post-ready (send) object bound */
    uint64_t         queue_byte_cap;
    uint32_t         ring_cap;            /* physical ring = max(both)+1 */

    /* App-visible state (mu-protected). */
    pthread_mutex_t  mu;
    pthread_cond_t   space_cv;            /* BLOCK_TIMEOUT: drain wakes waiters */

    moq_media_track_t **tracks;           /* app-built track list */
    size_t            track_count, track_cap;
    bool              tracks_frozen;      /* hook started building publisher */
    /* The session was already draining when this sender first saw it: the
     * publisher facade exists only to own the session's events (and so
     * record the GOAWAY), and nothing is set up on the draining session. */
    bool              observe_only;
#ifdef MOQ_MEDIA_SENDER_TESTING
    /* One-shot fault seam: force the catalog-republish live-write loop to
     * behave as if object `test_block_republish_obj` hit WOULD_BLOCK, and
     * snapshot the loop state at that instant (all under s->mu). Test-only:
     * never compiled into shipping libraries. */
    int64_t           test_block_republish_obj;   /* -1 = off */
    bool              test_block_fired;
    size_t            test_block_cursor;
    size_t            test_block_count;
    bool              test_block_retained_set;
    unsigned          test_retained_installs;   /* successful catalog
                                                   retained-group installs */
    bool              test_block_retained_once; /* one-shot: force the retained
                                                   install (set_retained_group)
                                                   to WOULD_BLOCK once, AFTER the
                                                   live writes -- models
                                                   retained-install backpressure */
#endif

    sender_preq_entry_t *preq;            /* ring; entries own their refs */
    uint32_t          preq_head, preq_tail;
    /* Queued MEDIA objects, maintained O(1) at the single enqueue/release
     * chokepoints. The PHYSICAL depth is preq_tail - preq_head, which also
     * counts a queued END_OF_TRACK marker; the public objects_queued counter
     * belongs to the media population and uses this instead. */
    uint32_t          preq_media;
#ifdef MOQ_MEDIA_SENDER_TESTING
    /* Test-only: attempts of the pending-reset transition. Never in the
     * shipping library (this whole field is behind the testing define). */
    uint32_t          test_reset_attempts;
    /* Test-only injected catalog clock. MSF §5.1.2 generatedAt is a wallclock
     * millisecond, so a test that needs two builds to differ -- or to be
     * identical -- cannot get there by waiting without becoming timing
     * dependent. Instance-owned rather than global: two senders in one process
     * must not share it. Never compiled into shipping libraries. */
    bool              test_clock_set;
    uint64_t          test_clock_ms;
#endif
    uint64_t          preq_bytes;

    bool              ready;
    bool              fatal;
    uint64_t          fatal_code;

    /* Latches so on_ready / on_closed each fire at most once (network thread). */
    bool              ready_fired;
    bool              closed_fired;

    moq_media_sender_stats_t stats;       /* counters (mu-protected) */

    /* Catalog republish . catalog_dirty is set by add_track/remove_track
     * under s->mu; the hook coalesces pending mutations into ONE new independent
     * generation per cycle. The rest is network-thread-only. */
    bool              catalog_dirty;

    /* Completion (V3 / MSF §11.3). Set by moq_media_sender_complete() under
     * s->mu (latched, terminal): all user tracks are marked removed and the
     * next catalog build emits isComplete:true + empty tracks. Once set,
     * add_track/write/remove_track/end_track are refused with WRONG_STATE. */
    bool              completing;

    /* Network-thread-only. */
    moq_publisher_t  *pub;
    moq_media_track_t *catalog_track;
    bool              catalog_published;     /* initial retained catalog installed */
    bool              live_catalog_sent;     /* initial catalog emitted live (publish) */
    uint64_t          catalog_group;        /* last published generation (the
                                               group base = initial) */
    uint64_t          group_base;           /* first Group ID of every track:
                                               resolved and range-checked before
                                               the sender exists (sender_prepare);
                                               an AUTO sender resolving to legacy
                                               resets it to 0 (under mu) */
    moq_rcbuf_t      *published_catalog;     /* last committed catalog bytes (the
                                                independent base / no-op dedup) */
    /* MSF §5.1.2 generatedAt of the bytes in published_catalog. Paired with the
     * buffer, never re-read from the clock: a staged generation may wait across
     * clock movement, so a stamp taken at commit would disagree with the bytes
     * it labels and the next semantic comparison would see a difference that is
     * not there. Private implementation state -- no public ABI. Validity
     * follows the paired buffer; 0 is a legal epoch value. */
    uint64_t          published_generated_at;
    /* Staged in-flight generation . A post-ready generation is a NEW group:
     * object 0 = the prior committed catalog (independent base), objects 1..N =
     * deltaUpdate objects (removes then adds) transforming it to the current
     * catalog. The dense object list is live-written in order then installed as
     * the retained group, retained across ticks so a transport WOULD_BLOCK
     * retries the unfinished step (no duplicate/skipped object). pending_current
     * is the new full catalog that becomes published_catalog on commit. */
    moq_rcbuf_t      *pending_objs[3];       /* obj0 base, [removes], [adds] */
    size_t            pending_obj_count;
    size_t            pending_obj_cursor;    /* next object to live-write */
    bool              pending_retained_set;
    moq_rcbuf_t      *pending_current;       /* new baseline (commit -> published) */
    uint64_t          pending_generated_at;  /* generatedAt of pending_current */
    /* Automatic independent catalog refresh: a late viewer joining through a
     * relay that resolves Joining FETCHes locally would otherwise never see the
     * catalog on the subscribe path, so the sender periodically republishes it.
     * Network-thread-only.
     * interval_us is resolved at create: SENDER_CATALOG_REFRESH_DISABLED
     * (UINT64_MAX) when the cfg field is 0/absent or explicitly UINT64_MAX
     * (opt-in per MSF-01 §5), else the caller's value. deadline_us is the next
     * refresh time (UINT64_MAX = not
     * armed / disabled); armed when the initial catalog installs and re-armed
     * on every generation commit so a real mutation resets the cadence. */
    uint64_t          catalog_refresh_interval_us;
    uint64_t          catalog_refresh_deadline_us;
    bool              catalog_had_demand;
    /* Cached next service-deadline for the managed-adapter wake query, recomputed
     * at the end of every sender_hook (where facade access is legal) and read by
     * sender_next_deadline_us as a pure scalar under s->mu -- so the deadline the
     * idle managed loop wakes on never re-enters the publisher/session/adapter.
     * UINT64_MAX when refresh is not timer-waiting (disabled, unarmed, not ready,
     * fatal/closed, at the group ceiling, a generation pending/dirty, or no
     * catalog demand). */
    uint64_t          refresh_wake_deadline_us;
    uint64_t          pending_group;
};

/* -- small helpers ---------------------------------------------------- */

static void sender_set_fatal_locked(moq_media_sender_t *s, uint64_t code)
{
    if (!s->fatal) {
        s->fatal = true;
        s->fatal_code = code;
    }
    s->refresh_wake_deadline_us = UINT64_MAX;   /* terminal: no managed wake */
    pthread_cond_broadcast(&s->space_cv);   /* unblock BLOCK_TIMEOUT waiters */
}

/* Fire on_closed at most once. Claims closed_fired under the lock, then invokes
 * the callback OUTSIDE it (non-reentrant contract). Network thread. */
static void sender_fire_closed(moq_media_sender_t *s, bool is_fatal,
                               uint64_t fatal_code)
{
    pthread_mutex_lock(&s->mu);
    if (s->closed_fired) { pthread_mutex_unlock(&s->mu); return; }
    s->closed_fired = true;
    /* Terminal (incl. a CLEAN close): disarm the managed wake atomically with
     * the close latch, so a cached finite deadline cannot survive to a cycle
     * that returns early (e.g. session == NULL) before the next recompute. */
    s->refresh_wake_deadline_us = UINT64_MAX;
    void *cbctx = s->callbacks.ctx;
    void (*cb)(void *, moq_media_sender_t *, bool, uint64_t)
        = s->callbacks.on_closed;
    pthread_mutex_unlock(&s->mu);

    if (cb) cb(cbctx, s, is_fatal, fatal_code);
}

/* Fire on_ready at most once, on the network thread, OUTSIDE s->mu. Claims the
 * ready_fired latch under the lock, then invokes the callback after release. */
static void sender_fire_ready(moq_media_sender_t *s)
{
    pthread_mutex_lock(&s->mu);
    if (s->ready_fired) { pthread_mutex_unlock(&s->mu); return; }
    s->ready_fired = true;
    void *cbctx = s->callbacks.ctx;
    void (*cb)(void *, moq_media_sender_t *) = s->callbacks.on_ready;
    pthread_mutex_unlock(&s->mu);

    if (cb) cb(cbctx, s);
}

static void sender_set_fatal(moq_media_sender_t *s, uint64_t code)
{
    pthread_mutex_lock(&s->mu);
    sender_set_fatal_locked(s, code);
    pthread_mutex_unlock(&s->mu);
    /* A fatal is a terminal close: surface it as on_closed(is_fatal=true). */
    sender_fire_closed(s, true, code);
}

static bool sender_terminal(const moq_media_sender_t *s)
{
    moq_media_sender_t *ms = (moq_media_sender_t *)(uintptr_t)s;
    pthread_mutex_lock(&ms->mu);
    bool fatal = ms->fatal;
    pthread_mutex_unlock(&ms->mu);
    return fatal || (s->ep && moq_endpoint_is_closed(s->ep));
}

/* "Endpoint closed" check that does NOT take ep->mu, for use while s->mu is held
 * (the BLOCK_TIMEOUT loop). The endpoint hook lock order is ep->mu -> s->mu, so
 * a holder of s->mu reachable from a hook MUST NOT take ep->mu -- the public
 * moq_endpoint_is_closed() does, so the BLOCK_TIMEOUT loop uses this internal
 * ep->mu-free variant instead (the lock-inversion fix). Shared with the test
 * seam so a regression back to an ep->mu-taking endpoint API here is caught. */
static bool sender_ep_closed(const moq_media_sender_t *s)
{
    return s->ep && moq_endpoint_is_closed_internal(s->ep);
}

static moq_bytes_t default_role(moq_media_type_t t)
{
    if (t == MOQ_MEDIA_TYPE_AUDIO)
        return (moq_bytes_t){ (const uint8_t *)"audio", 5 };
    return (moq_bytes_t){ (const uint8_t *)"video", 5 };
}

/* Whether the companion in effect can encode for the latched draft. An
 * unresolved AUTO companion is usable on none. CALLER MUST HOLD s->mu. */
static bool sender_version_usable(const moq_media_sender_t *s)
{
    if (s->companion_profile == MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC)
        return s->transport_version == MOQ_VERSION_DRAFT_22 &&
               !s->companion_blocked;
    if (s->companion_profile == MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01)
        return s->transport_version == MOQ_VERSION_DRAFT_16 ||
               s->transport_version == MOQ_VERSION_DRAFT_18;
    return false;
}

/* Whether a media write is refused as a companion/negotiated-version
 * mismatch: the latched draft is one the companion in effect cannot use
 * (including an AUTO companion blocked on draft 22), or an explicit D22
 * companion has not yet observed draft 22. An unresolved AUTO companion
 * before any draft is latched is not refused. CALLER MUST HOLD s->mu. */
static bool sender_companion_refused_locked(const moq_media_sender_t *s)
{
    if (sender_version_usable(s)) return false;
    return s->transport_version != 0 ||
           s->companion_profile == MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC;
}

/* The largest LOC timestamp write() admits for the latched draft `v`. It is
 * read from the LOC version table moq_loc_encode itself checks against, so a
 * value admitted here is one the drain path can encode: the whole uint64
 * range on legacy draft18's vi64 codec,
 * MOQ_QUIC_VARINT_MAX where it is the QUIC variable-length integer (draft
 * 16). Before the session's draft has been observed (or for a draft the
 * table does not register, which the drain refuses anyway) the bound is the
 * narrowest of the drafts the sender can encode: a write that has not yet
 * met its session must not be admitted on a promise only some drafts could
 * keep. */
static uint64_t sender_loc_ts_max(moq_version_t v)
{
    uint64_t max = moq_loc_value_max_for_version(v);
    if (max != 0) return max;
    static const moq_version_t k_drafts[] = {
        MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18,
    };
    max = UINT64_MAX;
    for (size_t i = 0; i < sizeof(k_drafts) / sizeof(k_drafts[0]); i++) {
        uint64_t m = moq_loc_value_max_for_version(k_drafts[i]);
        if (m < max) max = m;
    }
    return max;
}

/* -- Send queue (mu held by caller) ----------------------------------- *
 * One ring serves both phases: the pre-ready bound applies until ready,
 * the (typically larger) send-queue bound after. The ring is sized to the
 * max of both so the bound is purely a policy gate, never a realloc. */

/* The single release chokepoint: every path that removes an entry from the ring
 * comes through here, so the media count is maintained in exactly one place.
 * A non-marker entry was necessarily counted at enqueue, so the decrement is
 * EXACT -- deliberately not clamped at zero, because saturating there would
 * hide a double release or a missed enqueue instead of surfacing it. */
static void preq_entry_release(moq_media_sender_t *s, sender_preq_entry_t *e)
{
    if (!e->is_end) s->preq_media--;
    if (e->payload)    moq_rcbuf_decref(e->payload);
    if (e->properties) moq_rcbuf_decref(e->properties);
    memset(e, 0, sizeof(*e));
}

/* THE drop-accounting chokepoint: one queued MEDIA entry is being discarded.
 * EVERY discard path goes through here -- the no-demand policy, lifecycle
 * removal/completion, and capacity-policy eviction -- so none of them can drift
 * from the others on bytes, objects, keyframes, or groups.
 *
 * `groups_dropped` counts CUMULATIVE DISTINCT (track, group_seq) values from
 * which at least one queued media object was discarded. group_seq is monotonic
 * per track, so remembering the last dropped group for the track's LIFETIME is
 * sufficient and makes the count independent of how the discards are split
 * across drain passes or across discard paths.
 *
 * This is purely the cumulative fact. The per-pass "first discard for this
 * track" fact that the no-demand path needs to arm its wire RESET once is
 * `drain_dropped`, handled at that call site. Markers are NOT media and must
 * never reach here. */
static void preq_note_dropped(moq_media_sender_t *s, moq_media_track_t *t,
                              const sender_preq_entry_t *e)
{
    s->preq_bytes -= e->bytes;
    s->stats.objects_dropped++;
    t->tstats.objects_dropped++;
    t->tstats.objects_queued--;
    t->tstats.bytes_queued -= e->bytes;
    if (e->keyframe) {
        s->stats.keyframes_dropped++;
        t->tstats.keyframes_dropped++;
    }
    if (!t->drop_group_valid || t->drop_group_last != e->group_seq) {
        s->stats.groups_dropped++;
        t->tstats.groups_dropped++;
    }
    t->drop_group_valid = true;
    t->drop_group_last = e->group_seq;
}

/* The single enqueue chokepoint for the media count. It reads the COMMITTED
 * entry rather than taking a caller-supplied flag, so the two chokepoints
 * cannot disagree about what an entry is. */
static void preq_note_enqueued(moq_media_sender_t *s,
                               const sender_preq_entry_t *e)
{
    if (!e->is_end) s->preq_media++;
}

static uint32_t active_obj_cap(const moq_media_sender_t *s)
{
    return s->ready ? s->queue_cap : s->preq_cap;
}
static uint64_t active_byte_cap(const moq_media_sender_t *s)
{
    return s->ready ? s->queue_byte_cap : s->preq_byte_cap;
}

static bool preq_fits(const moq_media_sender_t *s, size_t need)
{
    uint32_t depth = s->preq_tail - s->preq_head;
    return depth < active_obj_cap(s) &&
           s->preq_bytes + need <= active_byte_cap(s);
}

/* Evict one specific group (every queued entry of track t + group g,
 * wherever it sits). With multiple tracks the streams interleave, so a
 * group's entries are not necessarily contiguous -- dropping only the
 * contiguous head run would strand same-group entries behind a later sync
 * point (the corruption class the receiver slice fixed). Compact the ring,
 * keeping order. Counts the drop and, if the group is currently open on
 * the wire, arms the track's pending_reset so the drain abandons the
 * subgroup wire-correctly rather than truncating it. mu held. */
static void preq_evict_group(moq_media_sender_t *s,
                             moq_media_track_t *t, uint64_t g)
{
    uint32_t rd = s->preq_head, wr = s->preq_head;
    bool dropped_any = false;
    while (rd != s->preq_tail) {
        sender_preq_entry_t *e = &s->preq[rd % s->ring_cap];
        /* Drop this group's media entries, but PRESERVE any terminal
         * END_OF_TRACK marker (is_end) even when it shares the dropped group --
         * it is the only reliable EOS and must survive the eviction. */
        if (e->track == t && e->group_seq == g && !e->is_end) {
            preq_note_dropped(s, t, e);   /* THE shared accounting chokepoint */
            preq_entry_release(s, e);     /* clears the slot */
            dropped_any = true;
        } else {
            if (wr != rd) s->preq[wr % s->ring_cap] = *e;
            wr++;
        }
        rd++;
    }
    s->preq_tail = wr;
    /* A Group that lost queued media to eviction cannot be continued: a later
     * Object would follow a hole in it, and once its wire subgroup is reset
     * the drain would reopen the Group at Object 0 and repeat a Location. It
     * is closed for writes, so the next write must be a sync point, which
     * opens a new Group. (A no-demand drop instead leaves the Group open and
     * discards the rest of it in the drain; see skip_open.) */
    if (dropped_any && t->group_open && g == t->group_seq) {
        t->group_open = false;
        t->cur_group_anchored = false;
    }
    if (dropped_any) {
        /* Counting is done by preq_note_dropped above. What remains here is
         * this path's own wire semantics: if the subgroup for this group is
         * open, its tail just went away -- the drain must RESET it, not leave
         * it truncated. */
        if (t->emit_open && t->emit_group == g)
            t->pending_reset = true;
    }
}

/* The most recent sync-starting group seq for `track`, or false when that
 * track has none queued. The sync anchor is PER TRACK: each track's
 * retained suffix must independently begin at a sync point. mu held. */
static bool preq_track_anchor(const moq_media_sender_t *s,
                              const moq_media_track_t *track,
                              uint64_t *out_seq)
{
    bool found = false;
    for (uint32_t i = s->preq_head; i != s->preq_tail; i++) {
        const sender_preq_entry_t *e = &s->preq[i % s->ring_cap];
        if (e->track == track && e->starts_group && e->is_sync) {
            *out_seq = e->group_seq;
            found = true;
        }
    }
    return found;
}

/* Whether an entry's group is evictable for an incoming object of
 * `in_track` (in_anchors = the incoming object re-anchors its own track,
 * making all of that track's queued groups droppable). The one eviction
 * rule: preq_evict_one and preq_make_room's feasibility count both use it.
 * mu held. */
static bool entry_evictable(moq_media_sender_t *s,
                            const sender_preq_entry_t *e,
                            const moq_media_track_t *in_track, bool in_anchors)
{
    /* A queued END_OF_TRACK marker is the ONLY chance to emit EOS reliably
     * (end_track is idempotent and later writes are refused), so it is never
     * evictable -- a drop policy must surface WOULD_BLOCK rather than discard
     * it. Excluding it here keeps preq_make_room's feasibility count honest. */
    if (e->is_end) return false;
    if (e->track == in_track) {
        if (in_anchors) return true;
        /* The incoming object continues its track's open Group, so that
         * Group is kept: evicting it would close the Group the object is
         * about to join. */
        if (in_track->group_open && e->group_seq == in_track->group_seq)
            return false;
    }
    uint64_t aseq = 0;
    bool found = preq_track_anchor(s, e->track, &aseq);
    return !found || e->group_seq < aseq;
}

/* Evict the oldest group that can go without stranding any track's
 * retained GOP: a group (track T, seq g) is evictable iff g precedes T's
 * retained anchor. The incoming object's own track, when the incoming
 * object is itself a fresh anchor, has ALL its queued groups evictable
 * (the new keyframe re-anchors it). Returns false when nothing is
 * evictable (caller surfaces WOULD_BLOCK). mu held. */
static bool preq_evict_one(moq_media_sender_t *s,
                           const moq_media_track_t *in_track, bool in_anchors)
{
    for (uint32_t i = s->preq_head; i != s->preq_tail; i++) {
        sender_preq_entry_t *e = &s->preq[i % s->ring_cap];
        if (entry_evictable(s, e, in_track, in_anchors)) {
            preq_evict_group(s, e->track, e->group_seq);
            return true;
        }
    }
    return false;
}

/* Make room for one entry of `need` bytes per the backpressure policy,
 * preserving every track's sync anchor (§7.2). Drop policies first prove
 * the object CAN fit after evicting everything evictable -- only then do
 * they mutate the queue. If it can never fit (a retained GOP plus the
 * object exceed the bound, or the object alone exceeds the byte budget),
 * return false with the queue untouched, honoring the no-mutation
 * contract for the WOULD_BLOCK the caller will surface. mu held. */
static bool preq_make_room(moq_media_sender_t *s,
                           const moq_media_track_t *track,
                           bool incoming_anchors, size_t need)
{
    if (preq_fits(s, need)) return true;

    bool drop = (s->backpressure == MOQ_MEDIA_SEND_BP_DROP_TO_KEYFRAME ||
                 s->backpressure == MOQ_MEDIA_SEND_BP_DROP_GROUP);
    if (!drop)
        return false;   /* lossless/would-block: never evict */

    /* Feasibility (no mutation): sum what eviction could free, then check
     * the retained remainder + the incoming object fits both bounds. */
    uint64_t free_bytes = 0;
    uint32_t free_cnt = 0;
    for (uint32_t i = s->preq_head; i != s->preq_tail; i++) {
        const sender_preq_entry_t *e = &s->preq[i % s->ring_cap];
        if (entry_evictable(s, e, track, incoming_anchors)) {
            free_bytes += e->bytes;
            free_cnt++;
        }
    }
    uint32_t used_cnt = s->preq_tail - s->preq_head;
    if (used_cnt - free_cnt + 1 > active_obj_cap(s)) return false;
    if (s->preq_bytes - free_bytes + need > active_byte_cap(s)) return false;

    while (!preq_fits(s, need)) {
        if (!preq_evict_one(s, track, incoming_anchors))
            return false;   /* unreachable: feasibility proved room exists */
    }
    return true;
}

/* -- Catalog build (network thread) ----------------------------------- */

/* MSF §5.1.2 generatedAt: the wallclock time the catalog instance was generated,
 * in milliseconds since the Unix epoch. The service tier may read the wall clock
 * (the sans-I/O core/media layers never do). clock_gettime(CLOCK_REALTIME) does
 * not fail for a valid, always-present clock id; on the practically-unreachable
 * error we fall back to 0 rather than invent a new failure path for an optional
 * catalog field. */
static uint64_t sender_now_epoch_ms(void)
{
    struct timespec ts;
    if (clock_gettime(CLOCK_REALTIME, &ts) != 0)
        return 0;
    return (uint64_t)ts.tv_sec * 1000u + (uint64_t)ts.tv_nsec / 1000000u;
}

/* The wallclock a catalog instance is stamped with. Identical to
 * sender_now_epoch_ms() in a shipping build; under the testing define an
 * instance may pin it, which is what lets a test state the two timestamps it
 * expects instead of racing a millisecond boundary. */
static uint64_t sender_catalog_now_ms(const moq_media_sender_t *s)
{
#ifdef MOQ_MEDIA_SENDER_TESTING
    if (s->test_clock_set)
        return s->test_clock_ms;
#else
    (void)s;
#endif
    return sender_now_epoch_ms();
}

static void track_fill_msf(const moq_media_track_t *t, moq_msf_track_t *m)
{
    memset(m, 0, sizeof(*m));
    m->struct_size = sizeof(*m);
    m->name = t->name;

    /* Generated MSF §7 media-timeline track (§7.2): packaging "mediatimeline",
     * application/json mimeType, the 'depends' list, and NO eventType. */
    if (t->is_media_timeline) {
        m->packaging = (moq_bytes_t){ (const uint8_t *)"mediatimeline", 13 };
        m->is_live = t->is_live;
        m->has_mime_type = true;
        m->mime_type = (moq_bytes_t){ (const uint8_t *)"application/json", 16 };
        m->depends = t->depends;
        m->depends_count = t->depends_count;
        return;
    }

    /* Generated CMSF SAP timeline track (CMSF §3.6.1, MSF §8.2): a fixed
     * eventtimeline shape carrying eventType, the application/json mimeType and
     * the 'depends' list (the single media track it applies to). */
    if (t->is_event_timeline) {
        m->packaging = (moq_bytes_t){ (const uint8_t *)"eventtimeline", 13 };
        m->is_live = t->is_live;
        m->has_event_type = true;
        m->event_type =
            (moq_bytes_t){ (const uint8_t *)"org.ietf.moq.cmsf.sap", 21 };
        m->has_mime_type = true;
        m->mime_type = (moq_bytes_t){ (const uint8_t *)"application/json", 16 };
        m->depends = t->depends;
        m->depends_count = t->depends_count;
        return;
    }

    if (t->packaging == MOQ_MEDIA_PACKAGING_CMAF)
        m->packaging = (moq_bytes_t){ (const uint8_t *)"cmaf", 4 };
    else if (t->packaging == MOQ_MEDIA_PACKAGING_MPEG2TS)
        m->packaging = (moq_bytes_t){ (const uint8_t *)"mpeg2ts", 7 };
    else
        m->packaging = (moq_bytes_t){ (const uint8_t *)"loc", 3 };
    /* draft-gregoire-moq-msfts: the media type of a TS track, and its
     * mpeg2ts* fields from the sender-owned record copy. */
    if (t->packaging == MOQ_MEDIA_PACKAGING_MPEG2TS) {
        m->has_mime_type = true;
        m->mime_type = (moq_bytes_t){ (const uint8_t *)"video/mp2t", 10 };
    }
    m->extensions = t->ext;
    m->extension_count = t->ext_count;
    m->is_live = t->is_live;
    m->has_role = true;
    m->role = t->role.len ? t->role : default_role(t->media_type);
    if (t->codec.len) { m->has_codec = true; m->codec = t->codec; }
    /* init data is wired by the caller: CMAF -> initDataList[] + initRef,
     * LOC -> inline initData (legacy). */
    if (t->width)  { m->has_width = true;  m->width = t->width; }
    if (t->height) { m->has_height = true; m->height = t->height; }
    if (t->samplerate) { m->has_samplerate = true; m->samplerate = t->samplerate; }
    if (t->bitrate)    { m->has_bitrate = true;    m->bitrate = t->bitrate; }
    if (t->framerate_millis) {
        m->has_framerate = true;
        m->framerate_millis = t->framerate_millis;
    }
    if (t->timescale) { m->has_timescale = true; m->timescale = t->timescale; }
    if (t->channel_config.len) {
        m->has_channel_config = true;
        m->channel_config = t->channel_config;
    }
    if (t->lang.len) { m->has_lang = true; m->lang = t->lang; }
    /* MSF §5.2.35 track duration (VOD/non-live track; add_track enforces that it
     * is never set together with is_live). */
    if (t->has_track_duration) {
        m->has_track_duration = true;
        m->track_duration_ms = t->track_duration_ms;
    }
    /* CMSF §3.2 alternate group: carried verbatim onto the catalog track entry
     * (the lower MSF encoder emits "altGroup"). */
    if (t->has_alt_group) { m->has_alt_group = true; m->alt_group = t->alt_group; }
    /* CMSF authoring metadata (borrows the track's owned copies). */
    if (t->has_max_grp_sap) { m->has_max_grp_sap = true; m->max_grp_sap = t->max_grp_sap; }
    if (t->has_max_obj_sap) { m->has_max_obj_sap = true; m->max_obj_sap = t->max_obj_sap; }
    if (t->cp_ref_id_count) {
        m->cp_ref_ids = t->cp_ref_ids;
        m->cp_ref_id_count = t->cp_ref_id_count;
    }
}

/* Encode the configured tracks into an MSF catalog JSON rcbuf. Returns
 * MOQ_OK with *out set, or an error (catalog tracks excluded). */
/* A track belongs in the published catalog when it is a non-catalog, non-removed
 * track. With only_registered (the post-ready publish path), it must ALSO have a
 * registered publisher track -- the catalog must never advertise a track
 * moq_pub_add_track has not yet accepted (e.g. a same-name replacement whose
 * registration is still deferred). The white-box build path passes false. */
static bool track_in_catalog(const moq_media_track_t *t, bool only_registered)
{
    if (t->is_catalog || t->removed) return false;
    if (only_registered && !t->pub_track) return false;
    return true;
}

/* Snapshot the track POINTER vector under s->mu. The network thread walks tracks
 * outside s->mu (it makes publisher calls that must not hold the lock), but the
 * app thread can realloc s->tracks (sender_tracks_push) concurrently -- which
 * would free the vector storage a bare unlocked walk is mid-iteration over.
 * Track OBJECTS live until sender destroy, so copying just the pointers under the
 * lock is enough: the snapshot's entries stay valid for the whole walk. Free with
 * sender_tracks_release().
 *
 * Returns false ONLY on allocation failure -- distinct from a genuine zero-track
 * snapshot (true, *out_snap == NULL, *out_count == 0). Callers MUST NOT treat a
 * NOMEM failure as "no tracks": doing so would silently publish an empty catalog,
 * skip teardown/registration/demand resync, or let a conversion catalog publish
 * before Track Ended. On false a caller fatals or preserves its gate. */
static bool sender_tracks_snapshot(moq_media_sender_t *s,
                                   moq_media_track_t ***out_snap,
                                   size_t *out_count)
{
    pthread_mutex_lock(&s->mu);
    size_t n = s->track_count;
    moq_media_track_t **snap = NULL;
    bool ok = true;
    if (n > 0) {
        snap = (moq_media_track_t **)s->alloc.alloc(
            n * sizeof(*snap), s->alloc.ctx);
        if (snap) memcpy(snap, s->tracks, n * sizeof(*snap));
        else ok = false;   /* alloc failure -- NOT "zero tracks" */
    }
    pthread_mutex_unlock(&s->mu);
    *out_snap  = ok ? snap : NULL;
    *out_count = ok ? n : 0;
    return ok;
}

static void sender_tracks_release(moq_media_sender_t *s,
                                  moq_media_track_t **snap, size_t n)
{
    if (snap) s->alloc.free(snap, n * sizeof(*snap), s->alloc.ctx);
}

/* Builds the catalog JSON from s->tracks at an EXPLICIT generatedAt. CALLER
 * MUST HOLD s->mu: it walks s->tracks and reads per-track fields (name/desc/
 * removed/pub_track), so the lock keeps it from observing a half-applied
 * app-thread add/remove.
 *
 * The stamp is a parameter rather than a clock read so the two kinds of build
 * stay distinguishable: a REAL build (initial/mutation/refresh) chooses one
 * fresh stamp and carries it with the buffer it encoded, while a COMPARISON
 * build is handed the published stamp and never consults the clock. */
/* Whether this sender publishes the draft-22 companion's MSF-01 catalog:
 * version "draft-01" (5.1.1), every track's initialization data in the root
 * initDataList referenced by initRef (5.1.7, 5.2.13), and Group IDs based on
 * the wallclock at creation, the approach 6.1 suggests. That base is
 * best-effort: it does not by itself guarantee 6.1's restart rule that a new
 * starting Group ID exceeds every earlier one (draft-ietf-moq-msf-01 6.1,
 * lines 2778-2790), which stays the application's responsibility. The legacy
 * companion keeps its catalog and group numbering unchanged. */
static bool sender_msf01_catalog(const moq_media_sender_t *s)
{
    return s->companion_profile == MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC;
}

/* Whether t's initialization data is carried in the root initDataList and
 * referenced by initRef, rather than inline: a CMAF init segment (CMSF 3.1),
 * an mpeg2ts track's PSI (Use of MSF Initialization Data), and every track of
 * the draft-22 companion (MSF-01 5.1.7, 5.2.13). The one rule for the
 * independent catalog, id assignment and the deltaUpdate fallback. */
static bool sender_init_by_reference(const moq_media_sender_t *s,
                                     const moq_media_track_t *t)
{
    return t->init_data.len > 0 &&
           (t->packaging == MOQ_MEDIA_PACKAGING_CMAF ||
            t->packaging == MOQ_MEDIA_PACKAGING_MPEG2TS ||
            sender_msf01_catalog(s));
}

/* The first Group ID of every track of this sender instance, media,
 * generated timeline and catalog alike (sender_resolve_group_base). */
static uint64_t sender_group_base(const moq_media_sender_t *s)
{
    return s->group_base;
}

/* Whether a post-ready catalog change can still be given a Group ID. A change
 * made while a generation is already owed (catalog_dirty) coalesces into it;
 * otherwise it needs the group after the last committed or staged one, which
 * must not pass MOQ_QUIC_VARINT_MAX (the publisher's object write-path bound
 * on every profile; the ID space never wraps). Checked by every catalog-mutating call before it
 * changes anything, so an exhausted catalog refuses the change with
 * MOQ_ERR_LIMIT instead of failing later on the network thread.
 * CALLER MUST HOLD s->mu. */
static bool sender_catalog_group_available(const moq_media_sender_t *s)
{
    if (!s->ready || s->catalog_dirty) return true;
    uint64_t last = s->pending_obj_count > 0 ? s->pending_group
                                             : s->catalog_group;
    return last < MOQ_QUIC_VARINT_MAX;
}

static moq_result_t sender_build_catalog_at(moq_media_sender_t *s,
                                            moq_rcbuf_t **out,
                                            bool only_registered,
                                            uint64_t generated_at)
{
    *out = NULL;
    /* Count the tracks that belong in the catalog. */
    size_t n = 0;
    for (size_t i = 0; i < s->track_count; i++)
        if (track_in_catalog(s->tracks[i], only_registered)) n++;

    size_t cap = n ? n : 1;
    moq_msf_track_t *mt = (moq_msf_track_t *)s->alloc.alloc(
        cap * sizeof(moq_msf_track_t), s->alloc.ctx);
    moq_rcbuf_t **b64 = (moq_rcbuf_t **)s->alloc.alloc(
        cap * sizeof(moq_rcbuf_t *), s->alloc.ctx);
    moq_msf_init_data_entry_t *idl = (moq_msf_init_data_entry_t *)s->alloc.alloc(
        cap * sizeof(moq_msf_init_data_entry_t), s->alloc.ctx);
    if (!mt || !b64 || !idl) {
        if (mt)  s->alloc.free(mt, cap * sizeof(moq_msf_track_t), s->alloc.ctx);
        if (b64) s->alloc.free(b64, cap * sizeof(moq_rcbuf_t *), s->alloc.ctx);
        if (idl) s->alloc.free(idl,
            cap * sizeof(moq_msf_init_data_entry_t), s->alloc.ctx);
        return MOQ_ERR_NOMEM;
    }
    memset(b64, 0, cap * sizeof(moq_rcbuf_t *));

    moq_result_t rc = MOQ_OK;
    size_t k = 0;
    size_t idl_count = 0;   /* initDataList entries (cat.init_data_count) */
    size_t nb = 0;          /* base64 buffers used in b64[] (indexing/cleanup) */
    for (size_t i = 0; i < s->track_count && rc == MOQ_OK; i++) {
        moq_media_track_t *t = s->tracks[i];
        if (!track_in_catalog(t, only_registered)) continue;
        track_fill_msf(t, &mt[k]);
        if (t->init_data.len > 0) {
            if (sender_init_by_reference(s, t)) {
                /* CMSF §3.1, and MSF-01 5.1.7/5.2.13 for every track of the
                 * draft-22 companion: the init data lives in initDataList[];
                 * the track references it via initRef. An mpeg2ts track's PSI
                 * packets go the same way, as the "inline" entry the draft
                 * requires (Use of MSF Initialization Data). Tracks with
                 * identical init bytes share the one id assigned at add_track,
                 * so the catalog emits one entry per id, whichever of its
                 * tracks is still included. */
                size_t e = idl_count;
                for (size_t j = 0; j < idl_count; j++) {
                    if (idl[j].id.len == t->init_ref.len &&
                        memcmp(idl[j].id.data, t->init_ref.data,
                               t->init_ref.len) == 0) {
                        e = j;
                        break;
                    }
                }
                if (e == idl_count) {
                    rc = moq_msf_encode_init_data(&s->alloc, t->init_data,
                                                  &b64[nb]);
                    if (rc < 0) break;
                    idl[idl_count].id   = t->init_ref;
                    idl[idl_count].type =
                        (moq_bytes_t){ (const uint8_t *)"inline", 6 };
                    idl[idl_count].data = (moq_bytes_t){
                        moq_rcbuf_data(b64[nb]), moq_rcbuf_len(b64[nb]) };
                    nb++;
                    idl_count++;
                }
                mt[k].has_init_ref = true;
                mt[k].init_ref = idl[e].id;
            } else {
                /* Legacy LOC: inline initData (base64), not shared. */
                rc = moq_msf_encode_init_data(&s->alloc, t->init_data, &b64[nb]);
                if (rc < 0) break;
                if (moq_rcbuf_len(b64[nb]) > 0) {
                    mt[k].has_init_data = true;
                    mt[k].init_data = (moq_bytes_t){
                        moq_rcbuf_data(b64[nb]), moq_rcbuf_len(b64[nb]) };
                    nb++;
                }
            }
        }
        k++;
    }

    if (rc == MOQ_OK) {
        moq_msf_catalog_t cat;
        memset(&cat, 0, sizeof(cat));
        cat.struct_size = sizeof(cat);
        cat.version = MOQ_MSF_VERSION;
        cat.tracks = mt;
        cat.track_count = k;
        cat.init_data_list = idl;
        cat.init_data_count = idl_count;
        /* Root contentProtections: the encoder borrows the sender's deep copy
         * (stable for the sender's life). */
        cat.content_protections = s->content_protections;
        cat.content_protection_count = s->content_protection_count;
        cat.has_generated_at = true;
        cat.generated_at = generated_at;
        /* MSF §5.1.3 / §11.3: a terminal (completing) catalog commits the
         * broadcast complete. All tracks are marked removed at complete(), so
         * the loop above already yields an empty tracks array. */
        cat.is_complete = s->completing;
        if (sender_msf01_catalog(s))
            cat.version_form = MOQ_MSF_VERSION_FORM_DRAFT_NAME;   /* 5.1.1 */
        rc = moq_msf_catalog_encode(&s->alloc, &cat, out);
    }

    for (size_t i = 0; i < cap; i++)
        if (b64[i]) moq_rcbuf_decref(b64[i]);
    s->alloc.free(mt, cap * sizeof(moq_msf_track_t), s->alloc.ctx);
    s->alloc.free(b64, cap * sizeof(moq_rcbuf_t *), s->alloc.ctx);
    s->alloc.free(idl, cap * sizeof(moq_msf_init_data_entry_t), s->alloc.ctx);
    return rc;
}

/* A REAL catalog build: reads the clock exactly ONCE and returns the stamp it
 * encoded alongside the buffer, so the caller can install the two together.
 * out_stamp is optional (the test build helper has nowhere to keep it).
 * CALLER MUST HOLD s->mu. */
static moq_result_t sender_build_catalog_fresh(moq_media_sender_t *s,
                                               moq_rcbuf_t **out,
                                               bool only_registered,
                                               uint64_t *out_stamp)
{
    uint64_t stamp = sender_catalog_now_ms(s);
    moq_result_t rc = sender_build_catalog_at(s, out, only_registered, stamp);
    if (out_stamp) *out_stamp = stamp;
    return rc;
}

/* moq_media_track.track_scale. */
#define SENDER_TRACK_SCALE_NONE    0u  /* LOC-04 parse found no Timescale */
#define SENDER_TRACK_SCALE_PRESENT 1u  /* LOC-04 Timescale, any value */
#define SENDER_TRACK_SCALE_UNKNOWN 2u  /* not parseable as LOC-04 metadata */

/* Classify a track's own copy of its Track Properties as draft-22 LOC-04
 * metadata. The parser searches the mutable list and any immutable inner
 * list. A failed parse proves nothing about a Timescale, so it is UNKNOWN,
 * never NONE; the parser publishes no output on failure, and none is read. */
static uint8_t sender_track_scale_of(moq_bytes_t track_properties)
{
    if (track_properties.len == 0) return SENDER_TRACK_SCALE_NONE;
    moq_loc_metadata_t m;
    if (moq_loc_parse_metadata(MOQ_VERSION_DRAFT_22, MOQ_LOC_PROFILE_04,
                               track_properties, &m, sizeof(m)) != MOQ_OK)
        return SENDER_TRACK_SCALE_UNKNOWN;
    return (m.present & MOQ_LOC_METADATA_HAS_TIMESCALE)
               ? SENDER_TRACK_SCALE_PRESENT : SENDER_TRACK_SCALE_NONE;
}

/* Whether this AUTO sender is, or may still become, the draft-22 companion:
 * resolved to it, or unresolved with draft 22 in the endpoint's offer. The
 * offer is fixed when the endpoint is created. CALLER MUST HOLD s->mu. */
static bool sender_auto_d22_possible_locked(const moq_media_sender_t *s)
{
    if (!s->companion_auto) return false;
    if (s->companion_profile == MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC)
        return true;
    if (s->companion_profile != MOQ_MEDIA_COMPANION_AUTO) return false;
    moq_version_t versions[MOQ_ENDPOINT_MAX_VERSIONS];
    size_t n = moq_endpoint_offered_versions_internal(s->ep, versions,
                                                      MOQ_ENDPOINT_MAX_VERSIONS);
    for (size_t i = 0; i < n && i < MOQ_ENDPOINT_MAX_VERSIONS; i++)
        if (versions[i] == MOQ_VERSION_DRAFT_22) return true;
    return false;
}

/* -- Drain: emit queued objects (network thread, s->mu held) ---------- */

/* The LOC-04 block an AUTO sender resolved to draft 22 generates for a RAW
 * object written without a metadata snapshot, from the typed fields:
 *   - a capture time is wall-clock time: an unscaled Timestamp (0x10), which
 *     LOC-04 2.3.1.1 defines as microseconds since the Unix epoch;
 *   - otherwise the presentation time is media time: the Timestamp with a
 *     Timescale (0x08) of 1000000, which LOC-04 2.3.1.2 makes media time with
 *     an application-defined anchor -- never a presentation time passed off
 *     as a Unix wall clock;
 *   - on video, a one-byte frame marking (0x09) whose I bit is the object's
 *     sync flag. */
static bool sender_build_typed_loc04(moq_media_sender_t *s,
                                     const sender_preq_entry_t *e,
                                     moq_rcbuf_t **out)
{
    moq_loc_metadata_t m;
    moq_loc_metadata_init_sized(&m, sizeof(m));
    m.present = MOQ_LOC_METADATA_HAS_TIMESTAMP;
    if (e->has_capture_time) {
        m.timestamp = e->capture_time_us;
    } else {
        m.timestamp = e->pts_us;
        m.present |= MOQ_LOC_METADATA_HAS_TIMESCALE;
        m.timescale = 1000000u;
    }
    if (e->track->media_type == MOQ_MEDIA_TYPE_VIDEO) {
        m.present |= MOQ_LOC_METADATA_HAS_MARKING;
        m.marking_len = 1;
        m.marking[0] = e->keyframe ? 0x20u : 0x00u;   /* RFC 9626 I bit */
    }
    return moq_loc_encode_metadata(&s->alloc, MOQ_VERSION_DRAFT_22,
                                   MOQ_LOC_PROFILE_04, &m, out) == MOQ_OK &&
           *out;
}

/* Legacy RAW generates LOC-01 capture/PTS fallback plus video sync marking.
 * D22 RAW reuses the already-owned metadata snapshot, or, for an AUTO sender's
 * object written without one, generates LOC-04 from the typed fields; CMAF
 * passes its complete app block unchanged. RAW extras remain refused. *out is
 * a new ref (or NULL) the caller decrefs. Invalid legacy eligibility or
 * allocation fails closed. */
static bool sender_build_props(moq_media_sender_t *s,
                               const sender_preq_entry_t *e,
                               moq_rcbuf_t **out)
{
    *out = NULL;
    moq_media_track_t *t = e->track;

    if (t->packaging == MOQ_MEDIA_PACKAGING_RAW &&
        s->companion_profile == MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC &&
        !e->properties && s->companion_auto) {
        if (!sender_version_usable(s)) return false;
        return sender_build_typed_loc04(s, e, out);
    }
    if (t->packaging != MOQ_MEDIA_PACKAGING_RAW ||
        s->companion_profile == MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC) {
        if (e->properties) *out = moq_rcbuf_incref(e->properties);
        return true;
    }

    moq_loc_headers_t h;
    moq_loc_headers_init(&h);
    h.has_timestamp = true;
    /* Legacy LOC-01 Capture Timestamp (0x02) on drafts16/18. Emit the
     * app-supplied capture_time_us verbatim when present (no clamp/scale --
     * large epoch values are valid); otherwise fall back to
     * presentation_time_us so callers without a capture clock keep a
     * timestamped LOC header. */
    h.timestamp = e->has_capture_time ? e->capture_time_us : e->pts_us;
    if (t->media_type == MOQ_MEDIA_TYPE_VIDEO) {
        h.has_video_frame_marking = true;
        h.video_frame_marking.independent = e->keyframe;
    }
    /* Fail closed on an unobserved or unsupported draft: an object may not
     * be encoded before the session's version has been latched, and there
     * is deliberately no draft-16 fallback here. */
    if (!sender_version_usable(s))
        return false;
    moq_rcbuf_t *loc = NULL;
    if (moq_loc_encode(&s->alloc, s->transport_version,
                       MOQ_LOC_PROFILE_01, &h, &loc) != MOQ_OK)
        return false;
    *out = loc;   /* has_timestamp is always set, so loc is non-NULL */
    return true;
}

/* Only representability is needed here, not a converted clock or anchor.
 * Exact fractional scaling uses bounded quotient/remainder arithmetic. */
static bool sender_loc_time_usable(uint64_t ticks, uint64_t scale)
{
    if (!scale) return false;
    uint64_t whole = ticks / scale, rem = ticks % scale;
    if (whole > UINT64_MAX / 1000000u) return false;
    uint64_t frac = 0, r = 0;
    for (uint32_t bit = UINT32_C(1) << 19; bit; bit >>= 1) {
        frac *= 2;
        if (r >= scale - r) { r -= scale - r; ++frac; }
        else r += r;
        if (UINT32_C(1000000) & bit) {
            if (r >= scale - rem) { r -= scale - rem; ++frac; }
            else r += rem;
        }
    }
    return frac <= UINT64_MAX - whole * 1000000u;
}

static moq_result_t sender_encode_metadata(moq_media_sender_t *s,
    const moq_media_send_object_t *obj, moq_rcbuf_t **out)
{
    if (obj->struct_size < offsetof(moq_media_send_object_t, metadata) +
                           sizeof(obj->metadata) || !obj->metadata)
        return MOQ_ERR_INVAL;
    const moq_media_metadata_t *m = obj->metadata;
    if (m->struct_size < MOQ_MEDIA_METADATA_V0_SIZE ||
        m->loc.struct_size < MOQ_LOC_METADATA_V0_SIZE ||
        !(m->loc.present & MOQ_LOC_METADATA_HAS_TIMESTAMP))
        return MOQ_ERR_INVAL;
    if ((m->loc.present & MOQ_LOC_METADATA_HAS_TIMESCALE) &&
        !sender_loc_time_usable(m->loc.timestamp, m->loc.timescale))
        return MOQ_ERR_INVAL;
    /* LOC-04 pairs only with draft 22, so its codec is known even before an
     * AUTO sender has latched the session's draft. */
    return moq_loc_encode_metadata(&s->alloc, MOQ_VERSION_DRAFT_22,
                                   MOQ_LOC_PROFILE_04, &m->loc, out);
}

/* -- Generated SAP timeline emission (network thread, s->mu held) ----- */

static size_t u64_to_dec(uint8_t *dst, uint64_t v)
{
    char tmp[20];
    size_t i = 0;
    if (v == 0) { dst[0] = '0'; return 1; }
    while (v) { tmp[i++] = (char)('0' + (int)(v % 10)); v /= 10; }
    for (size_t j = 0; j < i; j++) dst[j] = (uint8_t)tmp[i - 1 - j];
    return i;
}

/* Serialize records[start,end) as a CMSF §3.6.1 SAP timeline JSON array:
 * [{"l":[group,object],"data":[sapType,ept_ms]},...]. *out is a new rcbuf. */
static moq_result_t sap_encode_records(const moq_alloc_t *alloc,
                                       const sender_sap_rec_t *recs,
                                       uint32_t start, uint32_t end,
                                       moq_rcbuf_t **out)
{
    *out = NULL;
    /* Conservative upper bound: the "[]" wrapper (2) + per record 20 bytes of
     * fixed punctuation and a generous 20 decimal digits budgeted for each of
     * the three integers -- the uint64 worst case; sap_type is only 0-3, so this
     * deliberately over-reserves -- + one comma BETWEEN records (count - 1) + 1
     * byte of slack. */
    size_t count = (size_t)(end - start);
    size_t cap = 2 + count * (20 + 3 * 20) + (count ? count - 1 : 0) + 1;
    uint8_t *buf = (uint8_t *)alloc->alloc(cap, alloc->ctx);
    if (!buf) return MOQ_ERR_NOMEM;
    size_t n = 0;
    buf[n++] = '[';
    for (uint32_t i = start; i < end; i++) {
        if (i > start) buf[n++] = ',';
        memcpy(buf + n, "{\"l\":[", 6); n += 6;
        n += u64_to_dec(buf + n, recs[i].media_group);
        buf[n++] = ',';
        n += u64_to_dec(buf + n, recs[i].media_object);
        memcpy(buf + n, "],\"data\":[", 10); n += 10;
        n += u64_to_dec(buf + n, recs[i].sap_type);
        buf[n++] = ',';
        n += u64_to_dec(buf + n, recs[i].ept_ms);
        memcpy(buf + n, "]}", 2); n += 2;
    }
    buf[n++] = ']';
    moq_result_t rc = moq_rcbuf_create(alloc, buf, n, out);
    alloc->free(buf, cap, alloc->ctx);
    return rc;
}

/* CMSF-01 3.6.1: the SAP-type timeline EPT is the earliest media presentation
 * timestamp "rounded to the nearest millisecond" -- round, not floor (the
 * MSF media-timeline mediaTime in 7 is floor-integral ms and stays floored).
 * Overflow-safe near UINT64_MAX: compute quotient + remainder rather than
 * (pts_us + 500), which would wrap for pts_us > UINT64_MAX - 500. */
static uint64_t sap_ept_ms_from_us(uint64_t pts_us)
{
    uint64_t ms = pts_us / 1000u;
    if (pts_us % 1000u >= 500u) ms++;   /* ms++ cannot overflow: ms <= ~1.8e16 */
    return ms;
}

/* Append one record to timeline track `tl`'s bounded history, evicting whole
 * leading media groups once more than sap_history_groups are present. Records
 * arrive in (group, object) order, so a group's records form a contiguous run.
 * s->mu held. A realloc failure drops the record rather than failing media. */
static void sap_history_append(moq_media_sender_t *s, moq_media_track_t *tl,
                               uint64_t media_group, uint64_t media_object,
                               uint8_t sap_type, uint64_t ept_ms)
{
    if (tl->sap_rec_count == tl->sap_rec_cap) {
        uint32_t nc = tl->sap_rec_cap ? tl->sap_rec_cap * 2 : 8;
        sender_sap_rec_t *nr = (sender_sap_rec_t *)s->alloc.realloc(
            tl->sap_recs, tl->sap_rec_cap * sizeof(sender_sap_rec_t),
            nc * sizeof(sender_sap_rec_t), s->alloc.ctx);
        if (!nr) return;
        tl->sap_recs = nr;
        tl->sap_rec_cap = nc;
    }
    tl->sap_recs[tl->sap_rec_count].media_group  = media_group;
    tl->sap_recs[tl->sap_rec_count].media_object = media_object;
    tl->sap_recs[tl->sap_rec_count].ept_ms       = ept_ms;
    tl->sap_recs[tl->sap_rec_count].sap_type     = sap_type;
    tl->sap_rec_count++;

    uint32_t k = tl->sap_history_groups ? tl->sap_history_groups
                                        : SENDER_DEFAULT_SAP_HISTORY_GROUPS;
    uint32_t distinct = 0;
    uint64_t prev = 0;
    for (uint32_t i = 0; i < tl->sap_rec_count; i++)
        if (i == 0 || tl->sap_recs[i].media_group != prev) {
            distinct++; prev = tl->sap_recs[i].media_group;
        }
    if (distinct <= k) return;

    /* Keep from the start of group index (distinct - k). */
    uint32_t to_drop = distinct - k, gseen = 0, keep_from = tl->sap_rec_count;
    uint64_t cur = 0;
    for (uint32_t i = 0; i < tl->sap_rec_count; i++)
        if (i == 0 || tl->sap_recs[i].media_group != cur) {
            cur = tl->sap_recs[i].media_group;
            if (gseen == to_drop) { keep_from = i; break; }
            gseen++;
        }
    if (keep_from == 0 || keep_from >= tl->sap_rec_count) return;
    memmove(tl->sap_recs, tl->sap_recs + keep_from,
            (tl->sap_rec_count - keep_from) * sizeof(sender_sap_rec_t));
    tl->sap_rec_count -= keep_from;
    tl->sap_base_seq  += keep_from;
    s->stats.sap_records_evicted += keep_from;
    if (tl->sap_lead < tl->sap_base_seq) tl->sap_lead = tl->sap_base_seq;
}

/* Whether a track wants data on the wire: a subscriber (pull), or the
 * publication established with forward on (publish-initiated). */
static bool sender_track_demand(moq_media_sender_t *s, moq_media_track_t *t)
{
    if (!t || !t->pub_track) return false;
    if (moq_pub_has_subscriber(s->pub, t->pub_track)) return true;
    if (s->publish_tracks)
        return moq_pub_track_forward(s->pub, t->pub_track);
    return false;
}

/* Emit timeline objects for `tl` from its side history, gated on the timeline
 * track's OWN subscriber so it can never head-of-line-block media (MSF §8.3).
 * The timeline shares the media group ids: each new media group boundary emits
 * its own independent Object 0 carrying ALL retained accessible records up to
 * that group (so several media groups draining in one pump do NOT collapse into
 * one object); later records of an already-open group emit as incrementals. The
 * loop catches up across every pending boundary; a transport WOULD_BLOCK stops
 * it with the cursor intact for the next pump. s->mu held. */
static void sender_emit_sap_for(moq_media_sender_t *s, moq_media_track_t *tl,
                                uint64_t now_us)
{
    if (!tl->pub_track) return;
    if (!sender_track_demand(s, tl)) return;

    uint64_t next_seq = tl->sap_base_seq + tl->sap_rec_count;
    while (tl->sap_lead < next_seq) {
        uint32_t start = (uint32_t)(tl->sap_lead - tl->sap_base_seq);
        uint64_t g = tl->sap_recs[start].media_group;
        bool independent = !tl->sap_open || g != tl->sap_cur_media_group;

        uint32_t from, to;
        uint64_t obj;
        if (independent) {
            /* Object 0 of timeline group g: all retained records through g. */
            from = 0;
            to = 0;
            while (to < tl->sap_rec_count &&
                   tl->sap_recs[to].media_group <= g) to++;
            obj = 0;
        } else {
            /* Incremental: the records of g not yet emitted. */
            from = start;
            to = start;
            while (to < tl->sap_rec_count &&
                   tl->sap_recs[to].media_group == g) to++;
            obj = tl->sap_obj_id;
        }

        moq_rcbuf_t *payload = NULL;
        if (sap_encode_records(&s->alloc, tl->sap_recs, from, to,
                               &payload) != MOQ_OK)
            return;
        moq_pub_object_cfg_t ocfg;
        moq_pub_object_cfg_init_sized(&ocfg, sizeof(ocfg));
        ocfg.group_id = g;
        ocfg.object_id = obj;
        ocfg.payload = payload;
        ocfg.end_of_group = true;   /* one subgroup per group, mirroring media */
        moq_result_t wrc = moq_pub_write_object_ex(s->pub, tl->pub_track, &ocfg,
                                                   now_us);
        moq_rcbuf_decref(payload);
        if (wrc == MOQ_ERR_WOULD_BLOCK) return;  /* retry next pump, cursor intact */
        if (wrc < 0) {
            sender_set_fatal_locked(s, MOQ_MEDIA_SENDER_FATAL_SETUP_FAILED);
            return;
        }

        if (independent) {
            tl->sap_open = true;
            tl->sap_cur_media_group = g;
            tl->sap_obj_id = 1;
        } else {
            tl->sap_obj_id++;
        }
        tl->sap_lead = tl->sap_base_seq + to;
    }
}

/* Append one explicit §7.1.1 record to media-timeline track `tl`'s bounded
 * history, evicting whole leading media groups once more than mt_history_groups
 * are present. Records arrive in (group, object) order. s->mu held. A realloc
 * failure drops the record rather than failing media. Mirrors sap_history_append
 * with the media-timeline record shape. */
static void mt_history_append(moq_media_sender_t *s, moq_media_track_t *tl,
                              uint64_t media_group, uint64_t media_object,
                              uint64_t media_time_ms, uint64_t wallclock_ms)
{
    if (tl->mt_rec_count == tl->mt_rec_cap) {
        uint32_t nc = tl->mt_rec_cap ? tl->mt_rec_cap * 2 : 8;
        moq_msf_media_timeline_record_t *nr =
            (moq_msf_media_timeline_record_t *)s->alloc.realloc(
                tl->mt_recs, tl->mt_rec_cap * sizeof(*nr),
                nc * sizeof(*nr), s->alloc.ctx);
        if (!nr) return;
        tl->mt_recs = nr;
        tl->mt_rec_cap = nc;
    }
    tl->mt_recs[tl->mt_rec_count].media_time_ms = media_time_ms;
    tl->mt_recs[tl->mt_rec_count].group         = media_group;
    tl->mt_recs[tl->mt_rec_count].object        = media_object;
    tl->mt_recs[tl->mt_rec_count].wallclock_ms  = wallclock_ms;
    tl->mt_rec_count++;

    uint32_t k = tl->mt_history_groups ? tl->mt_history_groups
                            : SENDER_DEFAULT_MEDIA_TIMELINE_HISTORY_GROUPS;
    uint32_t distinct = 0;
    uint64_t prev = 0;
    for (uint32_t i = 0; i < tl->mt_rec_count; i++)
        if (i == 0 || tl->mt_recs[i].group != prev) {
            distinct++; prev = tl->mt_recs[i].group;
        }
    if (distinct <= k) return;

    uint32_t to_drop = distinct - k, gseen = 0, keep_from = tl->mt_rec_count;
    uint64_t cur = 0;
    for (uint32_t i = 0; i < tl->mt_rec_count; i++)
        if (i == 0 || tl->mt_recs[i].group != cur) {
            cur = tl->mt_recs[i].group;
            if (gseen == to_drop) { keep_from = i; break; }
            gseen++;
        }
    if (keep_from == 0 || keep_from >= tl->mt_rec_count) return;
    memmove(tl->mt_recs, tl->mt_recs + keep_from,
            (tl->mt_rec_count - keep_from) * sizeof(*tl->mt_recs));
    tl->mt_rec_count -= keep_from;
    tl->mt_base_seq  += keep_from;
    if (tl->mt_lead < tl->mt_base_seq) tl->mt_lead = tl->mt_base_seq;
}

/* Emit media-timeline objects for `tl` from its side history, gated on the
 * timeline track's OWN subscriber so it never head-of-line-blocks media (MSF
 * §7.3). Mirrors sender_emit_sap_for: the timeline shares media group ids and
 * each new media-group boundary emits an independent Object 0 carrying ALL
 * retained accessible records through that group. Since the sender records one
 * entry per media group, the incremental branch is not normally exercised, but
 * it is kept for structural parity and safety. Serialization uses the explicit
 * §7.1.1 codec. s->mu held. */
static void sender_emit_mt_for(moq_media_sender_t *s, moq_media_track_t *tl,
                               uint64_t now_us)
{
    if (!tl->pub_track) return;
    if (!sender_track_demand(s, tl)) return;

    uint64_t next_seq = tl->mt_base_seq + tl->mt_rec_count;
    while (tl->mt_lead < next_seq) {
        uint32_t start = (uint32_t)(tl->mt_lead - tl->mt_base_seq);
        uint64_t g = tl->mt_recs[start].group;
        bool independent = !tl->mt_open || g != tl->mt_cur_media_group;

        uint32_t from, to;
        uint64_t obj;
        if (independent) {
            from = 0;
            to = 0;
            while (to < tl->mt_rec_count && tl->mt_recs[to].group <= g) to++;
            obj = 0;
        } else {
            from = start;
            to = start;
            while (to < tl->mt_rec_count && tl->mt_recs[to].group == g) to++;
            obj = tl->mt_obj_id;
        }

        /* Serialize records[from,to) via the §7.1.1 codec: size, then encode. */
        size_t needed = 0;
        if (moq_msf_media_timeline_encode(tl->mt_recs + from, to - from,
                                          NULL, 0, &needed) != MOQ_ERR_BUFFER)
            return;
        uint8_t *buf = (uint8_t *)s->alloc.alloc(needed, s->alloc.ctx);
        if (!buf) return;
        size_t wrote = 0;
        if (moq_msf_media_timeline_encode(tl->mt_recs + from, to - from,
                                          buf, needed, &wrote) != MOQ_OK) {
            s->alloc.free(buf, needed, s->alloc.ctx);
            return;
        }
        moq_rcbuf_t *payload = NULL;
        moq_result_t crc = moq_rcbuf_create(&s->alloc, buf, wrote, &payload);
        s->alloc.free(buf, needed, s->alloc.ctx);
        if (crc != MOQ_OK) return;

        moq_pub_object_cfg_t ocfg;
        moq_pub_object_cfg_init_sized(&ocfg, sizeof(ocfg));
        ocfg.group_id = g;
        ocfg.object_id = obj;
        ocfg.payload = payload;
        ocfg.end_of_group = true;
        moq_result_t wrc = moq_pub_write_object_ex(s->pub, tl->pub_track, &ocfg,
                                                   now_us);
        moq_rcbuf_decref(payload);
        if (wrc == MOQ_ERR_WOULD_BLOCK) return;
        if (wrc < 0) {
            sender_set_fatal_locked(s, MOQ_MEDIA_SENDER_FATAL_SETUP_FAILED);
            return;
        }

        if (independent) {
            tl->mt_open = true;
            tl->mt_cur_media_group = g;
            tl->mt_obj_id = 1;
        } else {
            tl->mt_obj_id++;
        }
        tl->mt_lead = tl->mt_base_seq + to;
    }
}

/* THE pending-reset state transition. An armed `pending_reset` is lifecycle
 * work OWED by the track: the abandoned open subgroup must be RESET on the wire
 * before anything can turn it into a clean FIN.
 *
 *   MOQ_OK        -- and only then -- clears the obligation, clears emit_open,
 *                    and counts the abandonment exactly once.
 *   WOULD_BLOCK   -- shared capacity: EVERY field, the obligation and the code
 *                    are preserved exactly for retry, and nothing is counted.
 *   any other <0  -- fatal; the obligation is NOT cleared and no successful
 *                    abandonment is counted.
 *
 * Returns MOQ_OK when there was nothing to do, so callers can treat "not owed"
 * and "discharged" alike. s->mu held. */
static moq_result_t sender_flush_reset(moq_media_sender_t *s,
                                       moq_media_track_t *t, uint64_t now_us)
{
    if (!t->pending_reset) return MOQ_OK;
    if (!t->pub_track) return MOQ_OK;   /* nothing on the wire to abandon */
    t->drain_reset_tried = true;
#ifdef MOQ_MEDIA_SENDER_TESTING
    s->test_reset_attempts++;
#endif
    moq_result_t rrc = moq_pub_reset_group(s->pub, t->pub_track, 0x10, now_us);
    if (rrc == MOQ_ERR_WOULD_BLOCK) return rrc;      /* retryable, untouched */
    if (rrc < 0) {
        sender_set_fatal_locked(s, MOQ_MEDIA_SENDER_FATAL_SETUP_FAILED);
        return rrc;                                   /* obligation retained */
    }
    t->pending_reset = false;
    t->emit_open = false;
    s->stats.groups_abandoned++;
    t->tstats.groups_abandoned++;
    return MOQ_OK;
}

/* True for a service-owned generated timeline track (SAP eventtimeline or MSF
 * media timeline): the app never writes to or removes it directly. */
static bool track_is_generated_timeline(const moq_media_track_t *t)
{
    return t->is_event_timeline || t->is_media_timeline;
}

/* Emit queued objects in decode order until the queue drains or the
 * transport backpressures. s->mu held; the session is network-thread
 * confined, so facade calls are safe under this lock (nothing the facade
 * touches re-enters s->mu). */
static void sender_drain(moq_media_sender_t *s, uint64_t now_us)
{
    /* ONE forward compaction pass over [preq_head, preq_tail), using the same
     * read/write-cursor idiom as preq_evict_group:
     * an entry that can be emitted now is emitted and released, an entry that
     * cannot is RETAINED IN PLACE by copying it to the write cursor. preq_head
     * never moves; preq_tail becomes the write cursor at the end. Work per
     * invocation is therefore one pass over the current depth -- no rotation,
     * no restart, no nested full-ring helper, no allocation.
     *
     * Lack of demand is TRACK-LOCAL and may be skipped: an undemanded track
     * must not stop demanded media behind it. A real MOQ_ERR_WOULD_BLOCK from
     * a facade/session operation is SHARED output-capacity backpressure: the
     * refused entry and the whole unprocessed suffix are retained verbatim and
     * this invocation stops. */
    uint32_t rd = s->preq_head, wr = s->preq_head;
    const uint32_t depth0 = s->preq_tail - s->preq_head;
    bool stop = false;          /* shared capacity refused, or fatal */

    /* Per-pass scratch, cleared before the pass. */
    for (size_t i = 0; i < s->track_count; i++) {
        s->tracks[i]->drain_hold = false;
        s->tracks[i]->drain_dropped = false;
        s->tracks[i]->drain_reset_tried = false;
        s->tracks[i]->drain_demand = -1;
    }

    while (rd != s->preq_tail) {
        sender_preq_entry_t *e = &s->preq[rd % s->ring_cap];
        moq_media_track_t *t = e->track;

        /* Shared capacity already refused: retain this and every later entry
         * exactly as they are. */
        if (stop) {
            if (wr != rd) s->preq[wr % s->ring_cap] = *e;
            wr++; rd++;
            continue;
        }

        /* A removed track's queued work is discarded: the catalog no longer
         * declares it and its pub track is ended by the hook. This reaches
         * both moq_media_sender_remove_track() and moq_media_sender_complete()
         * (which marks every track removed), and the discard is COUNTED like
         * any other -- the public conservation identity is unconditional.
         * The private terminal marker is not media and is not counted. No wire
         * RESET is armed here: removed-track teardown runs before the drain
         * and owns its terminal semantics. */
        if (t->removed) {
            if (!e->is_end) preq_note_dropped(s, t, e);
            preq_entry_release(s, e);
            rd++;
            continue;
        }

        /* This track already retained an entry in this pass: everything later
         * on the same track stays behind it, END_OF_TRACK included. That is
         * what keeps per-track FIFO exact and stops a terminal overtaking its
         * own held media. */
        if (t->drain_hold) {
            if (wr != rd) s->preq[wr % s->ring_cap] = *e;
            wr++; rd++;
            continue;
        }

        if (t->drain_demand < 0)
            t->drain_demand = sender_track_demand(s, t) ? 1 : 0;
        bool has_sub = t->drain_demand == 1;

        /* Terminal marker: emit a reliable END_OF_TRACK after this track's
         * queued objects (per-track order is preserved by drain_hold above).
         * The facade closes any open subgroup and FINs a fresh one; the session
         * stays alive.
         *
         * Handled BEFORE the no-demand branch below: moq_pub_end_track()
         * treats "no subscriber" as local terminal success, so a track ended
         * before a relay subscribes (or after the subscriber leaves) drains
         * its terminal here -- and because nothing of that track is still
         * held, it cannot overtake anything. */
        if (e->is_end) {
            /* A drop abandoned an open group: the obligation is discharged
             * through THE one transition before the terminal can turn the same
             * subgroup into a clean FIN -- independent of the cached demand
             * answer, which is not what decides whether the abandon is owed. */
            {
                moq_result_t rrc = sender_flush_reset(s, t, now_us);
                if (rrc != MOQ_OK) {   /* shared capacity, or fatal */
                    t->drain_hold = true;
                    stop = true;
                    if (wr != rd) s->preq[wr % s->ring_cap] = *e;
                    wr++; rd++;
                    continue;
                }
            }
            moq_result_t erc = moq_pub_end_track(s->pub, t->pub_track, now_us);
            if (erc != MOQ_OK) {
                /* WOULD_BLOCK is shared capacity, NOT a fatal: retry later. */
                if (erc != MOQ_ERR_WOULD_BLOCK)
                    sender_set_fatal_locked(s, MOQ_MEDIA_SENDER_FATAL_SETUP_FAILED);
                t->drain_hold = true;
                stop = true;
                if (wr != rd) s->preq[wr % s->ring_cap] = *e;
                wr++; rd++;
                continue;
            }
            t->emit_open = false;
            /* pending_reset is NOT cleared here: the only transition that may
             * clear it is sender_flush_reset(), which ran above. */
            preq_entry_release(s, e);
            rd++;
            continue;
        }

        /* No demand: drop_without_demand releases this track's media as it is
         * encountered (staying at the live edge) and the pass CONTINUES with
         * other tracks; the default retains it for its own track only. */
        if (!has_sub) {
            if (s->drop_without_demand) {
                preq_note_dropped(s, t, e);
                t->skip_group = e->group_seq;
                t->skip_open = true;
                /* Arm the wire RESET once per affected track in this pass: an
                 * open wire subgroup just lost its tail and must not be
                 * truncated. */
                if (!t->drain_dropped) {
                    t->drain_dropped = true;
                    if (t->emit_open) t->pending_reset = true;
                }
                preq_entry_release(s, e);
                rd++;
                continue;
            }
            t->drain_hold = true;
            if (wr != rd) s->preq[wr % s->ring_cap] = *e;
            wr++; rd++;
            continue;
        }

        /* Demand arrived inside a group whose head was dropped. Sending the
         * rest would start the subscriber on a mid-group object numbered 0,
         * so it waits for the next group start instead. */
        if (t->skip_open && e->group_seq == t->skip_group) {
            preq_note_dropped(s, t, e);
            preq_entry_release(s, e);
            rd++;
            continue;
        }
        t->skip_open = false;

        /* A drop abandoned this track's open group: reset it on the wire
         * before emitting the next (newer) group. */
        if (t->pending_reset) {
            moq_result_t rrc = sender_flush_reset(s, t, now_us);
            if (rrc != MOQ_OK) {   /* shared capacity, or fatal */
                t->drain_hold = true;
                stop = true;
                if (wr != rd) s->preq[wr % s->ring_cap] = *e;
                wr++; rd++;
                continue;
            }
        }

        moq_rcbuf_t *props = NULL;
        if (!sender_build_props(s, e, &props)) {
            sender_set_fatal_locked(s, MOQ_MEDIA_SENDER_FATAL_SETUP_FAILED);
            t->drain_hold = true;
            stop = true;
            if (wr != rd) s->preq[wr % s->ring_cap] = *e;
            wr++; rd++;
            continue;
        }

        bool new_group = !t->emit_open || t->emit_group != e->group_seq;
        uint64_t oid = new_group ? 0 : t->emit_obj;

        moq_pub_object_cfg_t ocfg;
        moq_pub_object_cfg_init_sized(&ocfg, sizeof(ocfg));
        ocfg.group_id = e->group_seq;
        ocfg.object_id = oid;
        ocfg.payload = e->payload;
        ocfg.properties = props;
        /* v0 emits one subgroup per group, so each subgroup IS its group's
         * only (hence last) subgroup: END_OF_GROUP is a subgroup-header bit
         * set at open and required uniform across the subgroup, so it is
         * true for every object. The group is delimited on the wire by the
         * group_id change to the next group. */
        ocfg.end_of_group = true;
        moq_result_t wrc = moq_pub_write_object_ex(s->pub, t->pub_track,
                                                   &ocfg, now_us);
        if (props) moq_rcbuf_decref(props);   /* facade took its own ref */

        if (wrc != MOQ_OK) {
            /* WOULD_BLOCK is shared capacity, NOT a fatal: retry later. */
            if (wrc != MOQ_ERR_WOULD_BLOCK)
                sender_set_fatal_locked(s, MOQ_MEDIA_SENDER_FATAL_SETUP_FAILED);
            t->drain_hold = true;
            stop = true;
            if (wr != rd) s->preq[wr % s->ring_cap] = *e;
            wr++; rd++;
            continue;
        }

        t->emit_group = e->group_seq;
        t->emit_open = true;          /* facade subgroup stays open until the
                                         next group; complete groups have no
                                         queued tail, so no spurious reset */
        t->emit_obj = oid + 1;
        s->stats.objects_sent++;
        t->tstats.objects_sent++;
        t->tstats.objects_queued--;
        t->tstats.bytes_queued -= e->bytes;

        /* Generated SAP timeline: record this Object's declared SAP at its real
         * wire Location (group, oid). Group starts always record (write()
         * already required a declared TYPE_1/2); a mid-group Object records only
         * an actual SAP (declared type != NONE). A dropped Object never reaches
         * here, so it correctly yields no record. */
        if (t->sap_timeline && e->has_sap_type &&
            (oid == 0 || e->sap_type != (uint8_t)MOQ_SAP_NONE))
            sap_history_append(s, t->sap_timeline, e->group_seq, oid,
                               e->sap_type, sap_ept_ms_from_us(e->pts_us));

        /* Generated MSF §7 media timeline: record one explicit entry per media
         * group, at the group-start Object (oid == 0), at its real wire Location.
         * mediaTime is the object's presentation time (ms); wallclock is the emit
         * time (ms) for a live track, 0 once the track is non-live/VOD. */
        if (t->media_timeline && oid == 0)
            mt_history_append(s, t->media_timeline, e->group_seq, oid,
                              e->pts_us / 1000u,
                              t->is_live ? now_us / 1000u : 0u);

        s->preq_bytes -= e->bytes;
        preq_entry_release(s, e);     /* decrefs the entry's refs */
        rd++;
    }

    s->preq_tail = wr;
    /* Any slot freed before a refusal must still wake blocked writers. */
    if ((s->preq_tail - s->preq_head) < depth0)
        pthread_cond_broadcast(&s->space_cv);

    /* An armed pending_reset is owed EVEN IF the track has no queued entry for
     * the pass above to hang it off -- an abandoned tail with nothing behind it
     * must still be RESET, not left open. Sweep every track once (bounded
     * O(track_count)), including any obligation the no-demand discards just
     * armed in this same pass, but SKIP a track the queue path already
     * attempted: nothing frees capacity in between, so a second attempt in the
     * same pass is futile. Either way the obligation stays retryable for the
     * next production hook; a fatal is already latched. */
    if (!s->fatal) {
        for (size_t i = 0; i < s->track_count; i++) {
            moq_media_track_t *t = s->tracks[i];
            if (!t->pending_reset || t->removed || t->drain_reset_tried)
                continue;
            if (sender_flush_reset(s, t, now_us) != MOQ_OK) break;
        }
    }

    /* Emit generated timelines from their side history. They never enter the
     * media FIFO above, so an unsubscribed timeline track cannot stall media;
     * each is gated on its own subscriber (SAP MSF §8.3 / media §7.3). */
    for (size_t i = 0; i < s->track_count; i++) {
        if (s->tracks[i]->is_event_timeline)
            sender_emit_sap_for(s, s->tracks[i], now_us);
        else if (s->tracks[i]->is_media_timeline)
            sender_emit_mt_for(s, s->tracks[i], now_us);
    }
}

/* -- The endpoint pump hook (network thread, under endpoint mu) -------- */

static bool removed_pubtrack_with_name(moq_media_sender_t *s,
                                       moq_media_track_t *const *tracks,
                                       size_t n, moq_bytes_t name);  /* below */

/* Register a publisher track for t, writing t->pub_track. t->pub_track is
 * network-thread-confined (read only by network-thread paths: drain,
 * build_catalog, resync/finish/republish, removed_pubtrack_with_name; no
 * app-thread call touches it). removed/pub_ended/teardown_done/catalog_dirty/
 * active_subs ARE app-touched and are accessed under s->mu.
 *
 * CALLER MUST HOLD s->mu and must re-check (!t->removed && !t->pub_track) under
 * that SAME lock before calling: removed is app-written (and monotonic), so the
 * "a removed/pre-catalog track must not get a publisher track" invariant only
 * holds if the check and the registration are atomic against a concurrent
 * remove_track. Uses sender_set_fatal_locked on failure -- calling the unlocked
 * sender_set_fatal while holding s->mu would deadlock. Holding s->mu across
 * moq_pub_add_track is safe: the sender installs no publisher callbacks, so the
 * facade cannot re-enter s->mu (same discipline as sender_drain / republish). */
static void sender_add_pub_track(moq_media_sender_t *s, moq_media_track_t *t,
                                 bool advertise, uint64_t now_us)
{
    moq_pub_track_cfg_t tcfg;
    /* Sized init: the catalog branch below sets the APPENDED
     * has_publisher_priority tail field, which moq_pub_add_track reads only
     * when struct_size covers it -- the pointer-only init's frozen v0
     * struct_size would silently drop it (default priority 128, not 0). */
    moq_pub_track_cfg_init_sized(&tcfg, sizeof(tcfg));
    tcfg.track_namespace = s->namespace_;
    tcfg.track_name = t->is_catalog
        ? (moq_bytes_t){ s->catalog_name, s->catalog_name_len }
        : t->name;
    tcfg.advertise_namespace = advertise;

    if (t->is_catalog) {
        tcfg.has_publisher_priority = true;
        tcfg.publisher_priority = 0;
    } else if (t->has_publisher_priority) {
        tcfg.has_publisher_priority = true;
        tcfg.publisher_priority = t->publisher_priority;
    }
    tcfg.group_order = t->group_order;
    /* The sender's production is strictly monotonic on every track: media
     * emission drains group_seq in order (eviction only drops OLDER groups
     * before emission; an abandoned group never rewinds), and catalog
     * generations only advance, with delta objects appended to the OPEN
     * generation group. Declaring it lets finite subscription filters
     * auto-complete once production passes their end group. */
    tcfg.monotonic_groups = true;
    tcfg.track_properties = t->track_properties;
    tcfg.dynamic_groups = t->dynamic_groups;

    if (moq_pub_add_track(s->pub, &tcfg, now_us, &t->pub_track) != MOQ_OK) {
        sender_set_fatal_locked(s, MOQ_MEDIA_SENDER_FATAL_SETUP_FAILED);
        return;
    }

    if (s->publish_tracks && t->pub_track) {
        moq_pub_publish_cfg_t pcfg;
        moq_pub_publish_cfg_init(&pcfg);
        if (moq_pub_publish_track(s->pub, t->pub_track, &pcfg, now_us) != MOQ_OK)
            sender_set_fatal_locked(s, MOQ_MEDIA_SENDER_FATAL_SETUP_FAILED);
    }
}

static bool rcbuf_bytes_eq(const moq_rcbuf_t *a, const moq_rcbuf_t *b)
{
    if (!a || !b) return false;
    size_t la = moq_rcbuf_len(a), lb = moq_rcbuf_len(b);
    return la == lb && memcmp(moq_rcbuf_data(a), moq_rcbuf_data(b), la) == 0;
}

/* Build one deltaUpdate object (ADD or REMOVE) from `tracks`, encoded to an
 * rcbuf. An ADD op carries full, self-contained tracks (packaging/isLive forced
 * on so the encoder emits them, and any init data inlined base64 so the
 * receiver's apply_delta builds a valid effective without a root initDataList);
 * a REMOVE op carries only the track name (MSF 5.1.6). Returns MOQ_OK with *out
 * set. */
static moq_result_t sender_build_delta_op(moq_media_sender_t *s,
                                          moq_msf_delta_op_kind_t kind,
                                          moq_media_track_t *const *tracks,
                                          size_t n, moq_rcbuf_t **out)
{
    *out = NULL;
    if (n == 0) return MOQ_ERR_INVAL;
    moq_msf_track_t *mt = (moq_msf_track_t *)s->alloc.alloc(
        n * sizeof(moq_msf_track_t), s->alloc.ctx);
    moq_rcbuf_t **b64 = (moq_rcbuf_t **)s->alloc.alloc(
        n * sizeof(moq_rcbuf_t *), s->alloc.ctx);
    if (!mt || !b64) {
        if (mt) s->alloc.free(mt, n * sizeof(moq_msf_track_t), s->alloc.ctx);
        if (b64) s->alloc.free(b64, n * sizeof(moq_rcbuf_t *), s->alloc.ctx);
        return MOQ_ERR_NOMEM;
    }
    memset(b64, 0, n * sizeof(moq_rcbuf_t *));

    moq_result_t rc = MOQ_OK;
    for (size_t i = 0; i < n; i++) {
        if (kind == MOQ_MSF_DELTA_OP_ADD) {
            track_fill_msf(tracks[i], &mt[i]);
            mt[i].has_packaging = true;   /* encoder emits packaging + isLive */
            mt[i].has_is_live = true;
            /* Inline the init segment (base64) so the add is self-contained --
             * a delta has no root initDataList for an initRef to resolve. This
             * matches the LOC track's canonical inline-init shape; CMAF init-
             * bearing adds never reach here (the caller falls back to a full
             * independent generation so the track keeps its initRef shape). */
            if (tracks[i]->init_data.len > 0) {
                rc = moq_msf_encode_init_data(&s->alloc, tracks[i]->init_data,
                                              &b64[i]);
                if (rc < 0) break;
                mt[i].has_init_data = true;
                mt[i].init_data = (moq_bytes_t){ moq_rcbuf_data(b64[i]),
                                                 moq_rcbuf_len(b64[i]) };
                mt[i].has_init_ref = false;   /* inline, not a list reference */
                mt[i].init_ref = (moq_bytes_t){ NULL, 0 };
            }
        } else { /* REMOVE: name only (MSF 5.1.6). */
            memset(&mt[i], 0, sizeof(mt[i]));
            mt[i].struct_size = sizeof(mt[i]);
            mt[i].name = tracks[i]->name;
        }
    }

    if (rc == MOQ_OK) {
        moq_msf_delta_op_t op = { kind, mt, n };
        moq_msf_catalog_t cat;
        memset(&cat, 0, sizeof(cat));
        cat.struct_size = sizeof(cat);
        cat.delta_update = &op;
        cat.delta_update_count = 1;
        rc = moq_msf_catalog_encode(&s->alloc, &cat, out);
    }

    for (size_t i = 0; i < n; i++) if (b64[i]) moq_rcbuf_decref(b64[i]);
    s->alloc.free(b64, n * sizeof(moq_rcbuf_t *), s->alloc.ctx);
    s->alloc.free(mt, n * sizeof(moq_msf_track_t), s->alloc.ctx);
    return rc;
}

/* Free a staged but uncommitted generation's object refs. */
static void sender_pending_clear(moq_media_sender_t *s)
{
    for (size_t i = 0; i < s->pending_obj_count; i++)
        if (s->pending_objs[i]) { moq_rcbuf_decref(s->pending_objs[i]); s->pending_objs[i] = NULL; }
    s->pending_obj_count = 0;
    s->pending_obj_cursor = 0;
    s->pending_retained_set = false;
    if (s->pending_current) { moq_rcbuf_decref(s->pending_current); s->pending_current = NULL; }
    s->pending_generated_at = 0;              /* cleared with its buffer */
}

/* Adopt the current resolved tuple set + metadata as the published baseline:
 * the next deltaUpdate diffs against in_published, and an in-place metadata
 * change is considered published once it lands in a staged generation. Called
 * from every sender_stage_generation success path so the two stay in lockstep. */
static void sender_commit_baseline(moq_media_sender_t *s)
{
    for (size_t i = 0; i < s->track_count; i++) {
        s->tracks[i]->in_published = track_in_catalog(s->tracks[i], true);
        s->tracks[i]->catalog_meta_dirty = false;
    }
}

/* Is the catalog the sender would build right now the same DOCUMENT as the last
 * committed one? MSF §5.1.2 generatedAt is a fresh wallclock millisecond on
 * every build, so comparing complete encoded bytes against a freshly stamped
 * build could only ever match inside a single millisecond. The comparison build
 * therefore carries the PUBLISHED stamp and never reads the clock, which makes
 * the byte comparison ask exactly the intended question: identical modulo
 * generatedAt. Everything else -- isComplete included -- still differs on the
 * wire and still cuts a generation.
 *
 * The answer is separate from the result so a failure to allocate or encode
 * cannot be read as "changed" and let the caller carry on. With no published
 * baseline there is nothing to match: unchanged is false, and no comparison
 * build is made. CALLER MUST HOLD s->mu; the comparison buffer never leaves
 * this function and is never written to the wire. */
static moq_result_t sender_catalog_semantically_unchanged(
    moq_media_sender_t *s, bool only_registered, bool *out_unchanged)
{
    *out_unchanged = false;
    if (!s->published_catalog) return MOQ_OK;
    moq_rcbuf_t *cmp = NULL;
    moq_result_t rc = sender_build_catalog_at(s, &cmp, only_registered,
                                              s->published_generated_at);
    if (rc != MOQ_OK || !cmp) {
        if (cmp) moq_rcbuf_decref(cmp);
        return rc != MOQ_OK ? rc : MOQ_ERR_INTERNAL;
    }
    *out_unchanged = rcbuf_bytes_eq(cmp, s->published_catalog);
    moq_rcbuf_decref(cmp);
    return MOQ_OK;
}

/* Stage a post-ready generation (caller holds s->mu, pending is empty). Returns
 * true if a generation was staged into s->pending_*, false if nothing to do
 * (no-op dedup) -- on a build/encode failure it sets fatal and returns false. */
static bool sender_stage_generation(moq_media_sender_t *s)
{
    /* Decide first, and stage nothing until the answer is known: a comparison
     * failure leaves no half-staged generation behind. */
    bool unchanged = false;
    if (sender_catalog_semantically_unchanged(s, true, &unchanged) != MOQ_OK) {
        sender_set_fatal_locked(s, MOQ_MEDIA_SENDER_FATAL_CATALOG_ENCODE);
        return false;
    }
    if (unchanged) return false;              /* no-op dedup */

    /* Changed: build the catalog that actually goes on the wire, with its own
     * fresh stamp. The comparison buffer above is never reused here -- it
     * carries the PRIOR generation's timestamp. */
    moq_rcbuf_t *current = NULL;
    uint64_t stamp = 0;
    if (sender_build_catalog_fresh(s, &current, true, &stamp) != MOQ_OK ||
        !current) {
        sender_set_fatal_locked(s, MOQ_MEDIA_SENDER_FATAL_CATALOG_ENCODE);
        return false;
    }

    /* Every app-driven change was refused at its call once no group was left
     * (sender_catalog_group_available); a change raised on the network thread
     * alone (a deferred registration) still never wraps or reuses an ID. */
    if (s->catalog_group >= MOQ_QUIC_VARINT_MAX) {
        moq_rcbuf_decref(current);
        sender_set_fatal_locked(s, MOQ_MEDIA_SENDER_FATAL_CATALOG_ENCODE);
        return false;
    }
    s->pending_group = s->catalog_group + 1;
    s->pending_obj_cursor = 0;
    s->pending_retained_set = false;
    s->pending_current = current;             /* becomes published on commit */
    s->pending_generated_at = stamp;          /* ... with the stamp it encoded */

    /* A terminal (completing) generation is a single independent object; no
     * prior base and no deltas (MSF §11.3). The initial generation has no prior
     * base to diff against either, so fall back to independent. */
    if (s->completing || !s->published_catalog) {
        moq_rcbuf_incref(current);
        s->pending_objs[0] = current;
        s->pending_obj_count = 1;
        sender_commit_baseline(s);
        return true;
    }

    /* Diff the prior committed catalog (in_published flags) against the current
     * build: added tuples vs removed tuples. */
    moq_media_track_t **adds = (moq_media_track_t **)s->alloc.alloc(
        (s->track_count ? s->track_count : 1) * sizeof(*adds), s->alloc.ctx);
    moq_media_track_t **rems = (moq_media_track_t **)s->alloc.alloc(
        (s->track_count ? s->track_count : 1) * sizeof(*rems), s->alloc.ctx);
    if (!adds || !rems) {
        if (adds) s->alloc.free(adds, s->track_count * sizeof(*adds), s->alloc.ctx);
        if (rems) s->alloc.free(rems, s->track_count * sizeof(*rems), s->alloc.ctx);
        sender_pending_clear(s);
        sender_set_fatal_locked(s, MOQ_MEDIA_SENDER_FATAL_CATALOG_ENCODE);
        return false;
    }
    size_t na = 0, nr = 0;
    bool init_ref_add = false;
    bool meta_changed = false;
    for (size_t i = 0; i < s->track_count; i++) {
        moq_media_track_t *t = s->tracks[i];
        bool in_cat = track_in_catalog(t, true);
        if (in_cat && t->in_published && t->catalog_meta_dirty) {
            /* A surviving tuple whose metadata changed in place (e.g. a
             * live->VOD conversion): the deltaUpdate vocabulary is only
             * add/remove, so base + add/remove deltas cannot carry this change.
             * Force a full independent generation so the converted metadata
             * actually reaches receivers (and joiners via the retained group). */
            meta_changed = true;
        }
        if (in_cat && !t->in_published) {
            adds[na++] = t;
            /* Initialization in the independent catalog's root initDataList
             * (referenced by initRef) is a shape a deltaUpdate add cannot
             * carry. Emitting it inline here would diverge from the track's
             * canonical (initRef) shape in every later independent catalog,
             * which the receiver's immutable-tuple check rejects. So a
             * generation that adds such a track -- an init-bearing CMAF or
             * mpeg2ts track, and every init-bearing track of the draft-22
             * companion (MSF-01 has no per-track initData) -- falls back to
             * a full independent generation. Legacy LOC inline init, and
             * init-less tracks, stay on the delta path. */
            if (sender_init_by_reference(s, t))
                init_ref_add = true;
        } else if (!in_cat && t->in_published) {
            rems[nr++] = t;
        }
    }

    if (init_ref_add || meta_changed) {
        s->alloc.free(adds, s->track_count * sizeof(*adds), s->alloc.ctx);
        s->alloc.free(rems, s->track_count * sizeof(*rems), s->alloc.ctx);
        moq_rcbuf_incref(current);
        s->pending_objs[0] = current;     /* single independent generation */
        s->pending_obj_count = 1;
        sender_commit_baseline(s);
        return true;
    }

    moq_result_t rc = MOQ_OK;
    moq_rcbuf_t *base = NULL, *rem_obj = NULL, *add_obj = NULL;
    /* object 0 = the prior committed catalog (the independent base the deltas
     * apply onto; base + deltas == current). */
    base = s->published_catalog;
    moq_rcbuf_incref(base);
    /* REMOVE object before ADD object so the receiver observes remove-then-add
     * per tuple (immutable-tuple safety for same-name reuse). */
    if (nr > 0) rc = sender_build_delta_op(s, MOQ_MSF_DELTA_OP_REMOVE, rems, nr, &rem_obj);
    if (rc == MOQ_OK && na > 0)
        rc = sender_build_delta_op(s, MOQ_MSF_DELTA_OP_ADD, adds, na, &add_obj);

    s->alloc.free(adds, s->track_count * sizeof(*adds), s->alloc.ctx);
    s->alloc.free(rems, s->track_count * sizeof(*rems), s->alloc.ctx);

    if (rc != MOQ_OK || (nr == 0 && na == 0)) {
        /* Encode failure, or bytes differ but no tuple diff (should not happen
         * for the deterministic builder) -> fall back to a single independent
         * generation (object 0 = current). */
        if (base) moq_rcbuf_decref(base);
        if (rem_obj) moq_rcbuf_decref(rem_obj);
        if (add_obj) moq_rcbuf_decref(add_obj);
        if (rc != MOQ_OK) {   /* genuine encode failure is fatal */
            sender_pending_clear(s);
            sender_set_fatal_locked(s, MOQ_MEDIA_SENDER_FATAL_CATALOG_ENCODE);
            return false;
        }
        moq_rcbuf_incref(current);
        s->pending_objs[0] = current;
        s->pending_obj_count = 1;
    } else {
        size_t k = 0;
        s->pending_objs[k++] = base;             /* object 0 (prior, base) */
        if (rem_obj) s->pending_objs[k++] = rem_obj;
        if (add_obj) s->pending_objs[k++] = add_obj;
        s->pending_obj_count = k;
    }
    /* Commit the new published tuple set (under mu; staging is atomic vs the
     * app thread). A track added after this point lands in the next diff. */
    sender_commit_baseline(s);
    return true;
}

/* Arm the next automatic-refresh deadline from now (caller holds s->mu, or is
 * on the network thread before the sender is live). UINT64_MAX is reserved
 * EXCLUSIVELY for the disabled/unarmed state (what sender_refresh_due reads as
 * "never"), so a finite interval must never resolve to it: a finite deadline
 * that would overflow saturates to UINT64_MAX-1 (still armed, fires eventually).
 * Refresh is also disarmed once the generation counter reaches the varint
 * ceiling (MOQ_QUIC_VARINT_MAX): the next group would be unencodable, so rather
 * than re-arm and churn (staging a group the facade would reject as fatal) the
 * deadline is parked at UINT64_MAX. */
static void sender_arm_refresh(moq_media_sender_t *s, uint64_t now_us)
{
    if (s->catalog_refresh_interval_us == UINT64_MAX ||
        s->catalog_group >= MOQ_QUIC_VARINT_MAX) {
        s->catalog_refresh_deadline_us = UINT64_MAX;   /* disabled / ceiling */
        return;
    }
    uint64_t d = now_us + s->catalog_refresh_interval_us;
    /* Saturate to UINT64_MAX-1 on overflow AND on the exact-max collision:
     * now+interval == UINT64_MAX does not overflow but would alias the
     * disabled/unarmed sentinel, so a finite interval could silently disable. */
    if (d < now_us || d == UINT64_MAX) d = UINT64_MAX - 1;
    s->catalog_refresh_deadline_us = d;
}

/* Recompute the cached managed-adapter wake deadline (caller holds s->mu; run at
 * the end of sender_hook, where facade access is legal). It is the armed refresh
 * deadline iff refresh is genuinely TIMER-WAITING; otherwise UINT64_MAX, so an
 * expired deadline can never keep an idle managed loop immediately-due while the
 * hook does no work. Terminality is read from the cached closed_fired/fatal
 * latches, never sender_ep_closed() (which would reach the adapter). */
static void sender_recompute_wake_deadline(moq_media_sender_t *s)
{
    uint64_t d = UINT64_MAX;
    if (s->ready && !s->fatal && !s->closed_fired &&
        s->catalog_refresh_interval_us != UINT64_MAX &&
        s->catalog_refresh_deadline_us != UINT64_MAX &&
        s->catalog_group < MOQ_QUIC_VARINT_MAX &&
        s->pending_obj_count == 0 && !s->catalog_dirty &&
        s->catalog_track && sender_track_demand(s, s->catalog_track))
        d = s->catalog_refresh_deadline_us;
    s->refresh_wake_deadline_us = d;
}

/* Managed-adapter wake query (moq_endpoint_hook_ops.next_deadline_us): a PURE
 * cached-scalar read under s->mu -- no publisher/session/endpoint/adapter
 * re-entry, safe to call after the pump's exclusive window has closed. */
static uint64_t sender_next_deadline_us(void *ctx)
{
    moq_media_sender_t *s = (moq_media_sender_t *)ctx;
    pthread_mutex_lock(&s->mu);
    uint64_t d = s->refresh_wake_deadline_us;
    pthread_mutex_unlock(&s->mu);
    return d;
}

/* True when an automatic independent catalog refresh is due (caller holds
 * s->mu). Disabled interval or an unarmed deadline is never due. */
static bool sender_refresh_due(const moq_media_sender_t *s, uint64_t now_us)
{
    if (s->catalog_refresh_interval_us == UINT64_MAX) return false;
    if (s->catalog_refresh_deadline_us == UINT64_MAX) return false;
    return now_us >= s->catalog_refresh_deadline_us;
}

/* Stage a refresh generation (caller holds s->mu, pending is empty): a NEW
 * group whose sole object 0 is the COMPLETE current catalog (independent,
 * never a delta), even when its bytes match the last committed catalog -- the
 * point is to place a fresh independent object on the subscribe path for late
 * joiners, not to signal a content change. Reuses the same pending_* cursor /
 * retained-install machinery as a mutation generation, so a WOULD_BLOCK is
 * exactly-once and retryable. Returns true if staged; false at the varint group
 * ceiling (the next group would be unencodable) or on a build failure (fatal). */
static bool sender_stage_refresh(moq_media_sender_t *s)
{
    /* The next group (catalog_group + 1) must be an encodable QUIC varint; at
     * the ceiling refuse cleanly rather than stage a group the facade rejects. */
    if (s->catalog_group >= MOQ_QUIC_VARINT_MAX) return false;
    /* Deliberately NOT routed through the semantic comparison: the refresh
     * exists to put a fresh independent catalog on the subscribe path for late
     * joiners, so it stages even when its document is unchanged. Its stamp is
     * still freshly chosen here and still travels with these exact bytes. */
    moq_rcbuf_t *current = NULL;
    uint64_t stamp = 0;
    if (sender_build_catalog_fresh(s, &current, true, &stamp) != MOQ_OK ||
        !current) {
        sender_set_fatal_locked(s, MOQ_MEDIA_SENDER_FATAL_CATALOG_ENCODE);
        return false;
    }
    s->pending_group = s->catalog_group + 1;
    s->pending_obj_cursor = 0;
    s->pending_retained_set = false;
    s->pending_current = current;             /* becomes published on commit */
    s->pending_generated_at = stamp;
    moq_rcbuf_incref(current);
    s->pending_objs[0] = current;             /* single independent object */
    s->pending_obj_count = 1;
    sender_commit_baseline(s);                /* no tuple change; keeps in sync */
    return true;
}

/* Post-ready catalog republish (network thread). Coalesces pending add/
 * remove mutations into ONE new generation in a NEW group: object 0 is the
 * prior committed catalog (independent base) and objects 1..N are deltaUpdate
 * objects (removes then adds) transforming it to the current catalog
 * (base + deltas == current). The dense object list is live-written in order to
 * active subscribers, then installed as the publisher's retained group so a
 * Joining FETCH replays objects 0..N. The staged generation is retained across
 * ticks until every live object AND the retained-group install finish, so a
 * transport WOULD_BLOCK retries the unfinished step without re-writing an
 * already-written object or skipping a generation. complete() and the initial
 * generation stage a single independent object instead of deltas. */
static void sender_republish_catalog(moq_media_sender_t *s, uint64_t now_us)
{
    pthread_mutex_lock(&s->mu);

    if (s->pending_obj_count == 0) {
        /* A real add/remove/conversion generation takes precedence. */
        if (s->catalog_dirty) {
            s->catalog_dirty = false;   /* a later mutation re-sets it */
            (void)sender_stage_generation(s);   /* stages, no-op dedups, or fatal */
            if (s->fatal) { pthread_mutex_unlock(&s->mu); return; }
        }
        bool cat_demand = sender_track_demand(s, s->catalog_track);
        if (cat_demand != s->catalog_had_demand) {
            s->catalog_had_demand = cat_demand;
            if (cat_demand) sender_arm_refresh(s, now_us);
        }
        /* No mutation generation this cycle (dedup, or nothing was dirty): if a
         * periodic refresh is due AND the catalog has demand, stage exactly one
         * independent refresh. No demand -> no refresh / no group advance. Never
         * a redundant second generation in one cycle (a mutation above leaves
         * pending_obj_count > 0, skipping this). */
        if (s->pending_obj_count == 0 &&
            sender_refresh_due(s, now_us) &&
            cat_demand) {
            (void)sender_stage_refresh(s);
            if (s->fatal) { pthread_mutex_unlock(&s->mu); return; }
        }
        if (s->pending_obj_count == 0) { pthread_mutex_unlock(&s->mu); return; }
    }

    /* Live-write objects 0..N-1 in order wherever the catalog has demand --
     * a subscriber (pull) OR an established forward-on publication (push),
     * the same sender_track_demand the initial catalog and the media drain
     * use. With no demand the live writes are skipped (the retained group
     * below serves any future joiner). A WOULD_BLOCK retries from the cursor
     * (no duplicate / skipped object). */
    if (sender_track_demand(s, s->catalog_track)) {
        while (s->pending_obj_cursor < s->pending_obj_count) {
            size_t k = s->pending_obj_cursor;
#ifdef MOQ_MEDIA_SENDER_TESTING
            if (s->test_block_republish_obj >= 0 &&
                (int64_t)k == s->test_block_republish_obj) {
                /* Snapshot the exact mid-generation state, then return as a
                 * WOULD_BLOCK would: cursor untouched, nothing committed. */
                s->test_block_republish_obj = -1;   /* one-shot */
                s->test_block_fired = true;
                s->test_block_cursor = s->pending_obj_cursor;
                s->test_block_count = s->pending_obj_count;
                s->test_block_retained_set = s->pending_retained_set;
                pthread_mutex_unlock(&s->mu);
                return;
            }
#endif
            moq_pub_object_cfg_t ocfg;
            moq_pub_object_cfg_init_sized(&ocfg, sizeof(ocfg));
            ocfg.group_id = s->pending_group;
            ocfg.object_id = k;
            ocfg.payload = s->pending_objs[k];
            /* One subgroup per generation group: end_of_group is a per-subgroup
             * header flag, so every object in the group carries it (mirroring the
             * media drain) -- the group ends after this single subgroup. */
            ocfg.end_of_group = true;
            moq_result_t wrc = moq_pub_write_object_ex(
                s->pub, s->catalog_track->pub_track, &ocfg, now_us);
            if (wrc == MOQ_ERR_WOULD_BLOCK) { pthread_mutex_unlock(&s->mu); return; }
            if (wrc < 0) {
                sender_set_fatal_locked(s, MOQ_MEDIA_SENDER_FATAL_SETUP_FAILED);
                pthread_mutex_unlock(&s->mu);
                return;
            }
            s->pending_obj_cursor++;
        }
    } else {
        s->pending_obj_cursor = s->pending_obj_count;
    }

    /* Install the dense retained group (objects 0..N) after the live publish, so
     * a joiner reconstructs the same generation a FETCH would. */
    if (!s->pending_retained_set) {
#ifdef MOQ_MEDIA_SENDER_TESTING
        if (s->test_block_retained_once) {
            /* One-shot: behave exactly as a set_retained_group WOULD_BLOCK --
             * the live objects are already written, but the install (and thus
             * the commit + deadline re-arm) is deferred to the next pump with
             * pending state intact. */
            s->test_block_retained_once = false;
            pthread_mutex_unlock(&s->mu);
            return;
        }
#endif
        moq_pub_retained_object_t robjs[3];
        for (size_t i = 0; i < s->pending_obj_count; i++) {
            robjs[i].object_id = i;
            robjs[i].payload = s->pending_objs[i];
            robjs[i].properties = NULL;
            robjs[i].end_of_group = (i + 1 == s->pending_obj_count);
        }
        moq_pub_retained_group_cfg_t gcfg;
        moq_pub_retained_group_cfg_init(&gcfg);
        gcfg.group_id = s->pending_group;
        gcfg.objects = robjs;
        gcfg.object_count = s->pending_obj_count;
        moq_result_t src = moq_pub_set_retained_group(
            s->pub, s->catalog_track->pub_track, &gcfg);
        if (src == MOQ_ERR_WOULD_BLOCK) { pthread_mutex_unlock(&s->mu); return; }
        if (src < 0) {
            sender_set_fatal_locked(s, MOQ_MEDIA_SENDER_FATAL_SETUP_FAILED);
            pthread_mutex_unlock(&s->mu);
            return;
        }
        s->pending_retained_set = true;
#ifdef MOQ_MEDIA_SENDER_TESTING
        s->test_retained_installs++;
#endif
    }

    /* Commit the generation: advance the group and adopt the new baseline. Any
     * committed generation (mutation OR refresh) resets the refresh cadence, so
     * the next automatic refresh is one full interval away and a WOULD_BLOCK
     * retry across ticks never drifts the cadence (the deadline advances only
     * here, at commit, not while a generation is mid-flight). */
    s->catalog_group = s->pending_group;
    sender_arm_refresh(s, now_us);
    if (s->published_catalog) moq_rcbuf_decref(s->published_catalog);
    s->published_catalog = s->pending_current;   /* move ref */
    /* The stamp moves WITH the bytes it labels. Reading the clock here instead
     * would mislabel a generation that waited across clock movement, and the
     * next semantic comparison would then see a difference that is not in the
     * document. */
    s->published_generated_at = s->pending_generated_at;
    s->pending_current = NULL;
    s->pending_generated_at = 0;
    for (size_t i = 0; i < s->pending_obj_count; i++)
        if (s->pending_objs[i]) { moq_rcbuf_decref(s->pending_objs[i]); s->pending_objs[i] = NULL; }
    s->pending_obj_count = 0;
    s->pending_obj_cursor = 0;
    s->pending_retained_set = false;
    pthread_mutex_unlock(&s->mu);
}

/* MSF §11.3 step 1: send Track Ended (status 0x2) to the active subscribers of
 * every converting track BEFORE its now-VOD catalog may publish. The publisher
 * track is left registered and retained -- NOT ended -- so the converted content
 * stays subscribable/joinable for VOD (the retained-replay guarantee is the
 * publisher's, from moq_pub_finish_subscribers). Returns true when no conversion
 * finish remains pending (the catalog republish may proceed); false if a finish
 * still needs a retry (the conversion catalog stays gated and catalog_dirty is
 * preserved for the next pump). A converting track that never registered a
 * publisher track had no live subscribers, so its finish is trivially complete.
 * Runs on the network thread, after the drain and before the republish. */
static bool sender_finish_conversions(moq_media_sender_t *s, uint64_t now_us)
{
    size_t n = 0;
    moq_media_track_t **snap = NULL;
    /* NOMEM: preserve the conversion gate (NOT all_done) so the conversion
     * catalog cannot publish before Track Ended; retry next pump. */
    if (!sender_tracks_snapshot(s, &snap, &n)) return false;
    bool all_done = true;
    for (size_t i = 0; i < n; i++) {
        moq_media_track_t *t = snap[i];
        pthread_mutex_lock(&s->mu);
        bool pending = t->vod_finish_pending;
        pthread_mutex_unlock(&s->mu);
        if (!pending) continue;
        if (!t->pub_track) {
            /* Never advertised -> no live subscribers to finish. */
            pthread_mutex_lock(&s->mu);
            t->vod_finish_pending = false;
            pthread_mutex_unlock(&s->mu);
            continue;
        }
        moq_result_t fr = moq_pub_finish_subscribers(
            s->pub, t->pub_track, MOQ_PUB_DONE_TRACK_ENDED, now_us);
        if (fr == MOQ_OK) {
            pthread_mutex_lock(&s->mu);
            t->vod_finish_pending = false;
            pthread_mutex_unlock(&s->mu);
        } else if (fr == MOQ_ERR_WOULD_BLOCK || fr == MOQ_ERR_WRONG_STATE) {
            all_done = false;   /* retry next pump; keep the conversion gated */
        } else {
            sender_set_fatal(s, MOQ_MEDIA_SENDER_FATAL_SETUP_FAILED);
            all_done = false;
            break;
        }
    }
    sender_tracks_release(s, snap, n);
    return all_done;
}

/* -- Demand visibility (§7.2): subscriber join/left mirroring -------- */

/* True for an app-visible media track: a user-added track -- not the internal
 * catalog and not a generated SAP/media-timeline sibling. */
static bool track_is_app_media(const moq_media_track_t *t)
{
    return !t->is_catalog && !t->is_event_timeline && !t->is_media_timeline;
}

/* Reconcile one track's mirrored subscriber count with the publisher's
 * AUTHORITATIVE count and fire the service callback on a change. Polling rather
 * than relying on the core on_subscriber_left is required because the publisher
 * clears subscriber slots WITHOUT a left callback on session close
 * (MOQ_EVENT_SESSION_CLOSED) and on local finish paths (end_track / VOD finish);
 * an increment/decrement mirror would go stale after those. Network thread: the
 * authoritative count is read outside s->mu (a const query, never re-entering
 * the lock), the mirror is updated under s->mu, and the app callback fires
 * OUTSIDE s->mu for app-visible media tracks (the non-reentrant contract). */
static void sender_resync_track_demand(moq_media_sender_t *s,
                                       moq_media_track_t *t)
{
    size_t real = (s->pub && t->pub_track)
        ? moq_pub_active_subscriptions(s->pub, t->pub_track) : 0;

    pthread_mutex_lock(&s->mu);
    size_t old = t->active_subs;
    if (real == old) { pthread_mutex_unlock(&s->mu); return; }
    t->active_subs = real;
    bool fire = track_is_app_media(t);
    void *cbctx = s->callbacks.ctx;
    void (*joined_cb)(void *, moq_media_sender_t *, moq_media_track_t *, size_t)
        = s->callbacks.on_subscriber_joined;
    void (*left_cb)(void *, moq_media_sender_t *, moq_media_track_t *, size_t)
        = s->callbacks.on_subscriber_left;
    pthread_mutex_unlock(&s->mu);

    if (!fire) return;
    if (real > old) { if (joined_cb) joined_cb(cbctx, s, t, real); }
    else            { if (left_cb)   left_cb(cbctx, s, t, real); }
}

/* Resync demand across all tracks. Called after moq_pub_tick() and after the
 * service teardown paths (end/remove/VOD finish) that can clear subscriptions.
 * The catalog track is private (not in s->tracks[]); resync it too so its
 * mirror is accurate, though it never fires an app callback. */
static void sender_resync_demand(moq_media_sender_t *s)
{
    if (!s->pub) return;
    if (s->catalog_track) sender_resync_track_demand(s, s->catalog_track);
    size_t n = 0;
    moq_media_track_t **snap = NULL;
    /* NOMEM: skip this resync pass -- the mirror reconciles next pump. */
    if (!sender_tracks_snapshot(s, &snap, &n)) return;
    for (size_t i = 0; i < n; i++)
        sender_resync_track_demand(s, snap[i]);
    sender_tracks_release(s, snap, n);
}

/* Peer rejected a catalog or media-track PUBLISH: the sender can never
 * deliver on it, so become fatal (PUBLISH_REJECTED) -- surfaced once via
 * on_closed(is_fatal=true) and waking any blocked writer. No auto-retry:
 * the service has no auth/policy context to redecide; the app may recreate
 * the sender. */
static void sender_pub_on_publish_error(void *ctx, moq_pub_track_t *track,
                                        moq_request_error_t error_code)
{
    (void)track; (void)error_code;
    moq_media_sender_t *s = (moq_media_sender_t *)ctx;
    sender_set_fatal(s, MOQ_MEDIA_SENDER_FATAL_PUBLISH_REJECTED);
}

/* Loss of the advertised namespace is sender-local. Keep operation closure
 * distinct from a peer REQUEST_ERROR and use the existing once-only fatal path. */
static void sender_pub_on_namespace_terminal(void *ctx,
    const moq_pub_namespace_terminal_info_t *info)
{
    moq_media_sender_t *s = (moq_media_sender_t *)ctx;
    uint64_t code = info->kind == MOQ_PUB_NAMESPACE_TERMINATED
        ? MOQ_MEDIA_SENDER_FATAL_NAMESPACE_TERMINATED
        : info->kind == MOQ_PUB_NAMESPACE_CANCELLED
            ? MOQ_MEDIA_SENDER_FATAL_NAMESPACE_CANCELLED
            : MOQ_MEDIA_SENDER_FATAL_NAMESPACE_REJECTED;
    sender_set_fatal(s, code);
}

/* A subscriber asked for a new group on a dynamic-groups track: latch it for
 * moq_media_sender_take_new_group_request. Fires on the network thread from
 * moq_pub_tick, which runs without s->mu. */
/* Publisher callback on the network thread inside moq_pub_tick (sender_hook,
 * ep->mu held): the peer's session-wide GOAWAY. The endpoint keeps its New
 * Session URI for moq_endpoint_get_goaway_uri(). */
static void sender_pub_on_goaway(void *ctx, moq_bytes_t new_session_uri)
{
    moq_media_sender_t *s = (moq_media_sender_t *)ctx;
    moq_endpoint_note_goaway_locked(s->ep, new_session_uri);
}

static void sender_pub_on_new_group_request(
    void *ctx, moq_pub_track_t *track, const moq_pub_new_group_request_info_t *info)
{
    moq_media_sender_t *s = (moq_media_sender_t *)ctx;
    pthread_mutex_lock(&s->mu);
    for (size_t i = 0; i < s->track_count; i++) {
        moq_media_track_t *t = s->tracks[i];
        if (t->pub_track != track) continue;
        t->new_group_pending = true;
        t->new_group_value = info->requested_group;
        break;
    }
    pthread_mutex_unlock(&s->mu);
}

static void sender_pub_on_closed(void *ctx, uint64_t error_code)
{
    (void)error_code;   /* the peer's session error is not the sender fatal code */
    moq_media_sender_t *s = (moq_media_sender_t *)ctx;
    sender_fire_closed(s, false, 0);
}

static void sender_pub_on_publish_finished(void *ctx, moq_pub_track_t *pt)
{
    moq_media_sender_t *s = (moq_media_sender_t *)ctx;

    pthread_mutex_lock(&s->mu);

    /* The catalog track is private (not in s->tracks[]); check it first. */
    if (s->catalog_track && s->catalog_track->pub_track == pt) {
        pthread_mutex_unlock(&s->mu);
        sender_fire_closed(s, false, 0);   /* no catalog -> session is done */
        return;
    }

    moq_media_track_t *t = NULL;
    for (size_t i = 0; i < s->track_count; i++) {
        if (s->tracks[i]->pub_track == pt) { t = s->tracks[i]; break; }
    }
    /* Fire once per track, for app-visible media only. */
    bool fire = t && track_is_app_media(t) && !t->pub_closed_fired;
    if (fire) t->pub_closed_fired = true;
    void *cbctx = s->callbacks.ctx;
    void (*cb)(void *, moq_media_sender_t *, moq_media_track_t *)
        = s->callbacks.on_track_closed;
    pthread_mutex_unlock(&s->mu);

    if (fire && cb) cb(cbctx, s, t);
}

static bool sender_assign_init_ref(moq_media_sender_t *s, moq_media_track_t *t);

/* Resolve an AUTO companion against the draft just latched: draft 22 selects
 * D22_MSF01_LOC04_PUBLIC, drafts 16/18 LEGACY_MSF01_LOC01, and any other draft
 * leaves it unresolved, which no draft can use. The latch runs before this
 * session's publisher, catalog or first object exists, so nothing reaches the
 * wire under an unresolved companion. Resolution happens once:
 *   draft-22: tracks registered meanwhile take the initDataList ids the
 *     draft-22 companion gives every init-bearing track. Group IDs already
 *     assigned keep the wallclock base read at creation.
 *   legacy: Group IDs assigned meanwhile were counted from that base; they
 *     are moved down by it, so the legacy companion still starts at 0.
 * Until the track list is frozen, each pass also re-derives companion_blocked.
 * Returns false only when an initDataList id cannot be allocated.
 * CALLER MUST HOLD s->mu. */
static bool sender_resolve_companion_locked(moq_media_sender_t *s)
{
    if (s->companion_profile == MOQ_MEDIA_COMPANION_AUTO) {
        moq_version_t v = s->transport_version;
        if (v == MOQ_VERSION_DRAFT_22) {
            s->companion_profile = MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC;
            for (size_t i = 0; i < s->track_count; i++) {
                if (s->tracks[i]->init_ref.len == 0 &&
                    !sender_assign_init_ref(s, s->tracks[i]))
                    return false;
            }
        } else if (v == MOQ_VERSION_DRAFT_16 || v == MOQ_VERSION_DRAFT_18) {
            s->companion_profile = MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01;
            uint64_t base = s->group_base;
            for (size_t i = 0; i < s->track_count; i++) {
                moq_media_track_t *t = s->tracks[i];
                if (t->group_started) t->group_seq -= base;
                if (t->drop_group_valid) t->drop_group_last -= base;
            }
            for (uint32_t i = s->preq_head; i != s->preq_tail; i++) {
                sender_preq_entry_t *e = &s->preq[i % s->ring_cap];
                if (e->track && e->track->group_started) e->group_seq -= base;
            }
            s->group_base = 0;
        }
    }
    if (s->companion_auto && !s->tracks_frozen) {
        bool blocked = false;
        if (s->companion_profile == MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC) {
            for (size_t i = 0; i < s->track_count && !blocked; i++) {
                const moq_media_track_t *t = s->tracks[i];
                blocked = !t->removed &&
                          t->packaging == MOQ_MEDIA_PACKAGING_CMAF &&
                          t->init_data.len == 0;
            }
        }
        s->companion_blocked = blocked;
    }
    return true;
}

static void sender_hook(moq_endpoint_t *ep, moq_session_t *session,
                        uint64_t now_us, void *ctx)
{
    (void)ep;
    moq_media_sender_t *s = (moq_media_sender_t *)ctx;

    pthread_mutex_lock(&s->mu);
    bool fatal = s->fatal;
    uint64_t fatal_code = s->fatal_code;
    pthread_mutex_unlock(&s->mu);
    if (fatal) {
        sender_fire_closed(s, true, fatal_code);
        return;
    }

    if (sender_ep_closed(s)) {
        sender_fire_closed(s, moq_endpoint_is_fatal_internal(s->ep),
                           moq_endpoint_fatal_code_internal(s->ep));
    }
    if (!session) return;

    /* Read the negotiated draft from the live session, under our own mutex,
     * before any work in this pass can encode an object. The session's
     * version is fixed at creation and immutable, so the latch cannot go
     * stale. Endpoint v0 owns one session per endpoint and never reconnects
     * (MOQ_ENDPOINT_RECONNECTING is reserved but unentered), so re-reading
     * each pass is redundant today -- it is the ordering that stays correct
     * when reconnect lands. */
    {
        moq_version_t v = moq_session_version(session);
        pthread_mutex_lock(&s->mu);
        s->transport_version = v;
        bool resolved = sender_resolve_companion_locked(s);
        bool usable = resolved && sender_version_usable(s);
        if (resolved && !usable) s->stats.last_error = MOQ_ERR_UNSUPPORTED;
        pthread_mutex_unlock(&s->mu);
        if (!resolved) {
            sender_set_fatal(s, MOQ_MEDIA_SENDER_FATAL_SETUP_FAILED);
            return;
        }
        if (!usable) return;
    }

    if (!s->pub) {
        moq_session_state_t st = moq_session_state(session);
        /* SETUP and a session-wide GOAWAY can both precede this sender's
         * first pass (or the sender is attached after them): the facade is
         * still created, as the session's one event owner, so the GOAWAY and
         * its New Session URI are observed; nothing is published on a
         * session that is going away. */
        bool observe = st == MOQ_SESS_DRAINING;
        if (st != MOQ_SESS_ESTABLISHED && !observe)
            return;

        moq_pub_cfg_t pcfg;
        moq_pub_cfg_init_sized(&pcfg, sizeof(pcfg));
        pcfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
        pcfg.callbacks.ctx = s;
        pcfg.callbacks.on_closed = sender_pub_on_closed;
        pcfg.callbacks.on_publish_finished = sender_pub_on_publish_finished;
        pcfg.callbacks.on_publish_error = sender_pub_on_publish_error;
        pcfg.callbacks.on_namespace_terminal =
            sender_pub_on_namespace_terminal;
        pcfg.on_new_group_request = sender_pub_on_new_group_request;
        pcfg.on_new_group_request_ctx = s;
        pcfg.on_goaway = sender_pub_on_goaway;
        if (moq_pub_create(session, &s->alloc, &pcfg, &s->pub) != MOQ_OK) {
            sender_set_fatal(s, MOQ_MEDIA_SENDER_FATAL_SETUP_FAILED);
            return;
        }
        s->observe_only = observe;
    }
    if (s->observe_only) {
        (void)moq_pub_tick(s->pub, now_us);
        return;
    }
    if (!s->tracks_frozen) {

        /* Freeze the app's track list and build pub tracks from it. The
         * catalog track carries the single namespace advertisement; media
         * tracks share the namespace (already announced) so the publisher
         * answers their subscribes without re-advertising. */
        pthread_mutex_lock(&s->mu);
        s->tracks_frozen = true;
        pthread_mutex_unlock(&s->mu);

        moq_pub_track_cfg_t ccfg;
        /* Sized init: has_publisher_priority below is an appended tail field
         * (see sender_add_pub_track). */
        moq_pub_track_cfg_init_sized(&ccfg, sizeof(ccfg));
        ccfg.track_namespace = s->namespace_;
        ccfg.track_name = (moq_bytes_t){ s->catalog_name, s->catalog_name_len };
        ccfg.advertise_namespace = true;
        ccfg.has_publisher_priority = true;
        ccfg.publisher_priority = 0;   /* catalog leads delivery */
        /* Catalog generations are monotone; deltas append to the open
         * generation group (legal: no completion evidence forms while the
         * group's bracket never cleanly closes). */
        ccfg.monotonic_groups = true;
        if (moq_pub_add_track(s->pub, &ccfg, now_us,
                              &s->catalog_track->pub_track) != MOQ_OK) {
            sender_set_fatal(s, MOQ_MEDIA_SENDER_FATAL_SETUP_FAILED);
            return;
        }

        /* Walk a snapshot so a concurrent app-thread realloc of s->tracks cannot
         * free the vector mid-walk; the per-track registration below takes s->mu
         * to decide and create the publisher track atomically. NOMEM here is
         * fatal -- proceeding
         * as "no tracks" would publish an empty initial catalog. */
        size_t n = 0;
        moq_media_track_t **snap = NULL;
        if (!sender_tracks_snapshot(s, &snap, &n)) {
            sender_set_fatal(s, MOQ_MEDIA_SENDER_FATAL_SETUP_FAILED);
            return;
        }
        for (size_t i = 0; i < n; i++) {
            moq_media_track_t *t = snap[i];
            if (t->is_catalog) continue;   /* creation-time immutable */
            /* A track removed before ready must not get a publisher track (it is
             * absent from the initial catalog -- otherwise it would be an
             * undeclared live track). removed is app-written and monotonic, so
             * the decision and the registration must be atomic under s->mu, or a
             * concurrent remove_track in the gap could leave a removed track with
             * a publisher track. */
            pthread_mutex_lock(&s->mu);
            if (!t->removed && !t->pub_track)
                sender_add_pub_track(s, t, false, now_us);
            pthread_mutex_unlock(&s->mu);
        }
        sender_tracks_release(s, snap, n);
        pthread_mutex_lock(&s->mu);
        fatal = s->fatal;
        pthread_mutex_unlock(&s->mu);
        if (fatal) return;

        /* Derive the initial catalog and install it as the retained group
         * (generation 0, object 0) so an explicit Joining FETCH(offset 0) can
         * pull it. A plain SUBSCRIBE delivers no retained objects; the receiver
         * obtains the catalog via SUBSCRIBE + Joining FETCH (MSF-01 §5).
         * sender_build_catalog_fresh requires s->mu (consistent track view). */
        moq_rcbuf_t *json = NULL;
        uint64_t json_stamp = 0;
        pthread_mutex_lock(&s->mu);
        moq_result_t bcrc = sender_build_catalog_fresh(s, &json, true,
                                                       &json_stamp);
        uint64_t first_group = sender_group_base(s);
        pthread_mutex_unlock(&s->mu);
        if (bcrc != MOQ_OK || !json) {
            sender_set_fatal(s, MOQ_MEDIA_SENDER_FATAL_CATALOG_ENCODE);
            return;
        }
        moq_pub_retained_object_t robj = { .object_id = 0, .payload = json };
        moq_pub_retained_group_cfg_t rg;
        moq_pub_retained_group_cfg_init(&rg);
        rg.group_id = first_group;
        rg.objects = &robj;
        rg.object_count = 1;
        moq_result_t src = moq_pub_set_retained_group(
            s->pub, s->catalog_track->pub_track, &rg);
        if (src != MOQ_OK) {
            moq_rcbuf_decref(json);
            sender_set_fatal(s, MOQ_MEDIA_SENDER_FATAL_SETUP_FAILED);
            return;
        }
#ifdef MOQ_MEDIA_SENDER_TESTING
        pthread_mutex_lock(&s->mu);
        s->test_retained_installs++;   /* the initial catalog's install */
        pthread_mutex_unlock(&s->mu);
#endif
        /* Keep the ref as the published baseline (generation 0) for the no-op
         * dedup and the deltaUpdate diff in sender_republish_catalog, with the
         * stamp these exact bytes carry. */
        s->published_catalog = json;
        s->published_generated_at = json_stamp;
        s->catalog_group = first_group;
        s->catalog_published = true;

        if (s->publish_tracks) {
            moq_pub_publish_cfg_t cpcfg;
            moq_pub_publish_cfg_init(&cpcfg);
            if (moq_pub_publish_track(s->pub, s->catalog_track->pub_track,
                                      &cpcfg, now_us) != MOQ_OK) {
                sender_set_fatal(s, MOQ_MEDIA_SENDER_FATAL_SETUP_FAILED);
                return;
            }
        }
        /* Start the automatic-refresh clock from the initial catalog install. */
        sender_arm_refresh(s, now_us);
        /* Record the initial catalog's track set as the published baseline. */
        pthread_mutex_lock(&s->mu);
        for (size_t i = 0; i < s->track_count; i++)
            s->tracks[i]->in_published = track_in_catalog(s->tracks[i], true);
        pthread_mutex_unlock(&s->mu);
    }

    (void)moq_pub_tick(s->pub, now_us);

    /* A dispatched event (namespace rejection/cancellation, or a rejected
     * PUBLISH) can have turned the sender fatal inside the tick above. That
     * is terminal: stop this cycle before any further work -- especially the
     * readiness block below, which would otherwise fire on_ready after the
     * fatal on_closed. The fatal path already fired on_closed and woke
     * waiters. */
    {
        pthread_mutex_lock(&s->mu);
        bool now_fatal = s->fatal;
        pthread_mutex_unlock(&s->mu);
        if (now_fatal) return;
    }

    /* Reconcile demand right after the tick so a fresh subscribe fires the join
     * callback in the same cycle it is accepted (before the drain runs). */
    sender_resync_demand(s);

    /* Publish-initiated: once the peer accepts the catalog PUBLISH, push the
     * initial catalog object live so a relay caches it without a FETCH. */
    if (s->publish_tracks && !s->live_catalog_sent && s->catalog_published &&
        sender_track_demand(s, s->catalog_track) &&
        s->published_catalog) {
        moq_pub_object_cfg_t ocfg;
        moq_pub_object_cfg_init_sized(&ocfg, sizeof(ocfg));

        /* Same (first group, object 0) as the retained group: it is the same
         * catalog object, so a live subscriber and a FETCH joiner see one
         * identity. */
        pthread_mutex_lock(&s->mu);
        ocfg.group_id = sender_group_base(s);
        pthread_mutex_unlock(&s->mu);
        ocfg.object_id = 0;
        ocfg.payload = s->published_catalog;
        ocfg.end_of_group = true;
        moq_result_t wrc = moq_pub_write_object_ex(
            s->pub, s->catalog_track->pub_track, &ocfg, now_us);
        /* The entry gate above already required demand, and nothing between it
         * and here can withdraw it: moq_pub_write_object_ex() never touches
         * publish_ok / publish_forward and drains no events, so a second
         * sender_track_demand() read could not differ. */
        if (wrc == MOQ_OK)
            s->live_catalog_sent = true;
    }

    /* Ready: catalog published AND accepted by the peer - via the namespace
     * advertisement (pull) or the catalog's PUBLISH (publish). */
    if (!s->ready && s->catalog_published &&
        (moq_pub_namespace_accepted(s->pub, s->catalog_track->pub_track) ||
         (s->publish_tracks &&
          moq_pub_track_is_published(s->pub, s->catalog_track->pub_track)))) {
        pthread_mutex_lock(&s->mu);
        s->ready = true;
        pthread_cond_broadcast(&s->space_cv);  /* bound widened to queue_cap */
        pthread_mutex_unlock(&s->mu);
        sender_fire_ready(s);
    }

    /* Post-ready track maintenance: tear down removed tracks (end-of-track, then
     * remove the publisher track) and register a pub track for any post-ready
     * add. The walk is over a snapshot; publisher calls run without s->mu except
     * the registration decision+create, which is taken under s->mu so it is
     * atomic against a concurrent remove_track. Both honor MOQ_ERR_WOULD_BLOCK
     * (retry next
     * pump). Runs before the drain so a track written immediately after a
     * post-ready add_track has its wire track ready when its queue drains. */
    if (s->ready && !s->fatal) {
        size_t n = 0;
        moq_media_track_t **snap = NULL;
        /* On NOMEM the snapshot yields snap=NULL/n=0, so this idempotent pass is
         * simply skipped and retried next pump -- it advances no state, which is
         * the correct "preserve" behavior here (distinct from a real empty list,
         * which also legitimately does nothing). */
        (void)sender_tracks_snapshot(s, &snap, &n);
        for (size_t i = 0; i < n; i++) {
            moq_media_track_t *t = snap[i];
            if (t->is_catalog) continue;   /* creation-time immutable */
            /* `removed` is app-written under s->mu; read it under the lock.
             * The pending-reset PREFLIGHT is taken in the SAME critical
             * section: an app-thread capacity eviction (or removal) cannot arm
             * the obligation in a gap and then have this teardown turn the
             * abandoned subgroup into a clean FIN. `owed` therefore reflects
             * the obligation as of the same instant as `removed`. */
            pthread_mutex_lock(&s->mu);
            bool removed = t->removed;
            moq_result_t owed = MOQ_OK;
            if (removed && t->pending_reset)
                owed = sender_flush_reset(s, t, now_us);
            pthread_mutex_unlock(&s->mu);
            /* The abandon must complete BEFORE any end_track/remove can close
             * the same subgroup: on refusal skip this track's teardown for now
             * and retry on a later hook. */
            if (owed != MOQ_OK) continue;
            if (removed) {
                if (!t->pub_track) {
                    /* No publisher track (pre-ready removal or never registered)
                     * -> nothing to tear down; the name is already free. */
                    if (!t->teardown_done) {
                        pthread_mutex_lock(&s->mu);
                        t->teardown_done = true;
                        pthread_mutex_unlock(&s->mu);
                    }
                    continue;
                }
                /* 1) reliable END_OF_TRACK to active subscribers. */
                if (!t->pub_ended) {
                    moq_result_t e = moq_pub_end_track(s->pub, t->pub_track,
                                                       now_us);
                    if (e == MOQ_OK) t->pub_ended = true;
                    else if (e == MOQ_ERR_WOULD_BLOCK ||
                             e == MOQ_ERR_WRONG_STATE) continue;  /* retry */
                    else { sender_set_fatal(s, MOQ_MEDIA_SENDER_FATAL_SETUP_FAILED); break; }
                }
                /* 2) remove the publisher track so its name can be reused. */
                moq_result_t r = moq_pub_remove_track(s->pub, t->pub_track,
                                                      now_us);
                if (r == MOQ_OK) {
                    t->pub_track = NULL;
                    pthread_mutex_lock(&s->mu);
                    t->teardown_done = true;   /* name free; a re-add may proceed */
                    pthread_mutex_unlock(&s->mu);
                }
                else if (r == MOQ_ERR_WOULD_BLOCK) continue;     /* retry */
                else { sender_set_fatal(s, MOQ_MEDIA_SENDER_FATAL_SETUP_FAILED); break; }
                continue;
            }
            if (!t->pub_track) {
                /* Defer registering a replacement until the old same-name
                 * publisher track (if any) is actually gone. */
                if (removed_pubtrack_with_name(s, snap, n, t->name)) continue;
                /* Decide and register atomically under s->mu: re-check removed
                 * (app-written, monotonic) in the same lock that creates the
                 * publisher track so a concurrent remove_track cannot leave a
                 * removed track advertised. A track only becomes catalog-eligible
                 * once registered (only_registered build); registration does not
                 * otherwise dirty the catalog, so a track excluded from the prior
                 * publish while unregistered must trigger a republish now or it
                 * could never be advertised -- set catalog_dirty under the same
                 * lock. */
                pthread_mutex_lock(&s->mu);
                if (!t->removed && !t->pub_track) {
                    sender_add_pub_track(s, t, false, now_us);
                    if (t->pub_track) s->catalog_dirty = true;
                }
                pthread_mutex_unlock(&s->mu);
            }
        }
        sender_tracks_release(s, snap, n);
    }

    /* Drain queued media once ready. Holds s->mu across emission: the
     * session is network-thread confined and nothing the facade touches
     * re-enters s->mu, so this cannot deadlock with the app thread. */
    pthread_mutex_lock(&s->mu);
    if (s->ready && !s->fatal)
        sender_drain(s, now_us);
    pthread_mutex_unlock(&s->mu);

    /* MSF §11.3 step 1: finish converting tracks' live subscribers (Track Ended)
     * BEFORE the conversion catalog publishes. Strict ordering -- the done is
     * sent first, then the independent conversion catalog. A still-pending finish
     * gates the republish and preserves catalog_dirty for the next pump, so the
     * conversion catalog is never emitted early. */
    bool conv_done = true;
    if (s->ready && !s->fatal)
        conv_done = sender_finish_conversions(s, now_us);

    /* Coalesced independent catalog republish for post-ready add/remove/convert. */
    if (s->ready && !s->fatal && conv_done)
        sender_republish_catalog(s, now_us);

    (void)moq_pub_tick(s->pub, now_us);

    /* Reconcile demand AFTER the tick + the teardown/finish work above, so the
     * mirror and app callbacks reflect subscriptions cleared without a left
     * event (session close / end / VOD finish). */
    sender_resync_demand(s);

    /* Cache the next managed-adapter wake deadline from the now-settled state
     * (facade access is legal on this path); sender_next_deadline_us returns it
     * as a pure scalar so an idle managed loop wakes to fire the refresh. */
    pthread_mutex_lock(&s->mu);
    sender_recompute_wake_deadline(s);
    pthread_mutex_unlock(&s->mu);
}

/* Publisher teardown posted to the network thread (ctx = publisher alone;
 * the sender may be freed before the task runs). */
static moq_result_t sender_destroy_pub_task(moq_endpoint_t *ep,
                                            moq_session_t *session,
                                            uint64_t now_us, void *ctx)
{
    (void)ep; (void)session; (void)now_us;
    moq_pub_destroy((moq_publisher_t *)ctx);
    return MOQ_OK;
}

/* -- cfg -------------------------------------------------------------- */

/* Frozen v0 floors. The callbacks prefix runs through on_subscriber_left --
 * everything before the appended on_ready/on_closed/on_track_closed fields.
 * The cfg prefix runs through the v0 PORTION of the embedded callbacks (the
 * pre-publish layout ended with the nested callbacks as its last field), so
 * an old caller's sizeof is offsetof(callbacks) + the v0 callbacks size --
 * NOT offsetof(callbacks) + sizeof(current callbacks), which includes the
 * appended callback fields. Pointer-only initializers must never clear past
 * these floors. */
#define MEDIA_SENDER_CALLBACKS_V0_SIZE \
    offsetof(moq_media_sender_callbacks_t, on_ready)
#define MEDIA_SENDER_CFG_V0_SIZE \
    (offsetof(moq_media_sender_cfg_t, callbacks) + \
     MEDIA_SENDER_CALLBACKS_V0_SIZE)

void moq_media_sender_cfg_init(moq_media_sender_cfg_t *cfg)
{
    if (!cfg) return;
    /* Clear and stamp ONLY the frozen v0 prefix: writing sizeof(*cfg) would
     * overflow a caller compiled against the pre-publish (smaller) struct.
     * Appended fields (the on_ready/on_closed/on_track_closed callbacks,
     * publish_tracks, drop_without_demand, catalog_refresh_interval_us) stay
     * disabled/default; callers that set them use the _sized initializers. */
    memset(cfg, 0, MEDIA_SENDER_CFG_V0_SIZE);
    cfg->struct_size = (uint32_t)MEDIA_SENDER_CFG_V0_SIZE;
    /* Nested struct_size: the v0 portion this initializer cleared. */
    cfg->callbacks.struct_size = (uint32_t)MEDIA_SENDER_CALLBACKS_V0_SIZE;
    /* CMSF §3.3/§3.4: validate CMAF output by default (strict). Affects CMAF
     * tracks only; RAW/LOC is unaffected. Callers wanting passthrough set
     * validate_cmaf = false explicitly after init. */
    cfg->validate_cmaf = true;
    /* backpressure stays UNSET on purpose: forced choice (§7.5). */
}

void moq_media_sender_callbacks_init(moq_media_sender_callbacks_t *cb)
{
    if (!cb) return;
    /* Frozen v0 prefix only (see MEDIA_SENDER_CALLBACKS_V0_SIZE): the
     * appended lifecycle callbacks need moq_media_sender_callbacks_init_sized. */
    memset(cb, 0, MEDIA_SENDER_CALLBACKS_V0_SIZE);
    cb->struct_size = (uint32_t)MEDIA_SENDER_CALLBACKS_V0_SIZE;
}

void moq_media_sender_cfg_init_live(moq_media_sender_cfg_t *cfg)
{
    moq_media_sender_cfg_init(cfg);
    if (cfg) cfg->backpressure = MOQ_MEDIA_SEND_BP_DROP_TO_KEYFRAME;
}

void moq_media_sender_cfg_init_lossless(moq_media_sender_cfg_t *cfg)
{
    moq_media_sender_cfg_init(cfg);
    if (!cfg) return;
    cfg->backpressure = MOQ_MEDIA_SEND_BP_BLOCK_TIMEOUT;
    cfg->block_timeout_us = SENDER_DEFAULT_BLOCK_TIMEOUT_US;
}

void moq_media_sender_callbacks_init_sized(moq_media_sender_callbacks_t *cb,
                                           size_t cb_size)
{
    if (!cb) return;
    /* Clear exactly what the caller allocated, never more than this library's
     * struct knows about (same contract as moq_pub_cfg_init_sized). */
    size_t n = cb_size < sizeof(*cb) ? cb_size : sizeof(*cb);
    if (n < sizeof(cb->struct_size)) return;  /* too small to even stamp */
    memset(cb, 0, n);
    cb->struct_size = (uint32_t)n;
}

void moq_media_sender_cfg_init_sized(moq_media_sender_cfg_t *cfg,
                                     size_t cfg_size)
{
    if (!cfg) return;
    size_t n = cfg_size < sizeof(*cfg) ? cfg_size : sizeof(*cfg);
    if (n < sizeof(cfg->struct_size)) return;  /* too small to even stamp */
    memset(cfg, 0, n);
    cfg->struct_size = (uint32_t)n;
    /* Initialize as much of the nested callbacks struct as the caller's
     * storage covers (bounded by BOTH the outer bytes available and the
     * library's own callbacks size). */
    {
        size_t cb_off = offsetof(moq_media_sender_cfg_t, callbacks);
        if (n > cb_off)
            moq_media_sender_callbacks_init_sized(&cfg->callbacks, n - cb_off);
    }
    if (n >= offsetof(moq_media_sender_cfg_t, validate_cmaf) +
             sizeof(cfg->validate_cmaf))
        cfg->validate_cmaf = true;
    if (n >= offsetof(moq_media_sender_cfg_t, companion_profile) +
             sizeof(cfg->companion_profile))
        cfg->companion_profile = MOQ_MEDIA_COMPANION_AUTO;
    /* backpressure stays UNSET on purpose: forced choice (§7.5). */
}

/* The caller-storage prefix the preset wrappers may touch. Computed from
 * cfg_size, NEVER read back from cfg->struct_size: when cfg_size is smaller
 * than the struct_size field itself, the base initializer cannot stamp it,
 * so the field holds whatever (possibly poisoned) bytes the caller had --
 * gating on it could write a preset default out of bounds. */
static size_t sender_cfg_prefix(size_t cfg_size)
{
    return cfg_size < sizeof(moq_media_sender_cfg_t)
               ? cfg_size : sizeof(moq_media_sender_cfg_t);
}

void moq_media_sender_cfg_init_live_sized(moq_media_sender_cfg_t *cfg,
                                          size_t cfg_size)
{
    moq_media_sender_cfg_init_sized(cfg, cfg_size);
    if (!cfg) return;
    size_t n = sender_cfg_prefix(cfg_size);
    if (n >= offsetof(moq_media_sender_cfg_t, backpressure) +
                 sizeof(cfg->backpressure))
        cfg->backpressure = MOQ_MEDIA_SEND_BP_DROP_TO_KEYFRAME;
}

void moq_media_sender_cfg_init_lossless_sized(moq_media_sender_cfg_t *cfg,
                                              size_t cfg_size)
{
    moq_media_sender_cfg_init_sized(cfg, cfg_size);
    if (!cfg) return;
    size_t n = sender_cfg_prefix(cfg_size);
    if (n >= offsetof(moq_media_sender_cfg_t, backpressure) +
                 sizeof(cfg->backpressure))
        cfg->backpressure = MOQ_MEDIA_SEND_BP_BLOCK_TIMEOUT;
    if (n >= offsetof(moq_media_sender_cfg_t, block_timeout_us) +
                 sizeof(cfg->block_timeout_us))
        cfg->block_timeout_us = SENDER_DEFAULT_BLOCK_TIMEOUT_US;
}

void moq_media_track_cfg_init_sized(moq_media_track_cfg_t *cfg, size_t cfg_size)
{
    if (!cfg || cfg_size < sizeof(cfg->struct_size)) return;
    size_t n = cfg_size < sizeof(*cfg) ? cfg_size : sizeof(*cfg);
    memset(cfg, 0, n);
    cfg->struct_size = (uint32_t)n;
    if (n >= offsetof(moq_media_track_cfg_t, is_live) + sizeof(cfg->is_live))
        cfg->is_live = true;
}

void (moq_media_track_cfg_init)(moq_media_track_cfg_t *cfg)
{
    moq_media_track_cfg_init_sized(cfg, MEDIA_TRACK_CFG_SIZE_THROUGH_ALT_GROUP);
}

/* -- CMSF content-protection authoring (deep copy) -------------------- *
 * Root contentProtections are deep-copied from the cfg at create/attach so no
 * caller buffer outlives the call. The copy lives in three places freed by
 * free_root_cps(): the entry array, one shared `cp_strings` byte buffer for
 * every nested string, and a default_kids[] array per entry. The helpers below
 * keep the byte arithmetic out of sender_new. */

/* A present URL must carry a non-empty url; a typed URL must carry a non-empty
 * type. (An absent url is not copied, so its spans are not inspected.) Rejecting
 * an empty-but-present span here keeps a bad span from reaching cp_put's memcpy
 * during the deep-copy and from failing later in catalog encode. */
static moq_result_t cp_url_valid(const moq_cmsf_url_t *u)
{
    if (!u->present) return MOQ_OK;
    if (u->url.len == 0 || !u->url.data) return MOQ_ERR_INVAL;
    if (u->has_type && (u->type.len == 0 || !u->type.data))
        return MOQ_ERR_INVAL;
    return MOQ_OK;
}

/* Shape-only validation (no DRM/UUID/URL semantics). Authoring is stricter than
 * the receive parser: at least one non-empty defaultKID is required here. Every
 * span that the deep-copy will copy -- required AND any optional field marked
 * present -- is checked so a malformed entry fails create/attach with
 * MOQ_ERR_INVAL rather than crashing the copy or failing late in encode. */
/* CMSF §4.1.1.2 / §4.1.1.4.1: a contentProtection UUID string is exactly
 * "8-4-4-4-12" hex digits with hyphens (defaultKID entries and drmSystem
 * systemID). Case-insensitive hex -- the draft's format placeholder is
 * lowercase but does not restrict case. Syntax only: the UUID's value and
 * registry meaning are not interpreted. */
static bool cp_is_uuid(moq_bytes_t v)
{
    if (!v.data || v.len != 36) return false;
    for (size_t i = 0; i < 36; i++) {
        uint8_t c = v.data[i];
        if (i == 8 || i == 13 || i == 18 || i == 23) {
            if (c != '-') return false;
        } else if (!((c >= '0' && c <= '9') ||
                     (c >= 'a' && c <= 'f') ||
                     (c >= 'A' && c <= 'F'))) {
            return false;
        }
    }
    return true;
}

/* CMSF §4.1.1.4.5: pssh is a Base64-encoded PSSH box. Local, non-allocating
 * syntax check (not a public API) that mirrors the MSF encoder's b64_syntax_ok,
 * so a content protection accepted here also passes the strict catalog encode
 * (otherwise a bad config would be accepted at create/attach and then fail late
 * during catalog publication). */
static int cp_b64_val(uint8_t c)
{
    if (c >= 'A' && c <= 'Z') return c - 'A';
    if (c >= 'a' && c <= 'z') return c - 'a' + 26;
    if (c >= '0' && c <= '9') return c - '0' + 52;
    if (c == '+') return 62;
    if (c == '/') return 63;
    return -1;
}

static bool cp_b64_syntax_ok(moq_bytes_t b)
{
    if (b.len == 0) return true;
    if (!b.data || b.len % 4 != 0) return false;
    size_t pad = 0;
    if (b.data[b.len - 1] == '=') pad++;
    if (b.len >= 2 && b.data[b.len - 2] == '=') pad++;
    for (size_t i = 0; i < b.len; i++) {
        uint8_t c = b.data[i];
        if (c == '=') { if (i < b.len - pad) return false; }
        else if (cp_b64_val(c) < 0) return false;
    }
    if (pad == 2) { if (cp_b64_val(b.data[b.len - 3]) & 0x0F) return false; }
    else if (pad == 1) { if (cp_b64_val(b.data[b.len - 2]) & 0x03) return false; }
    return true;
}

static moq_result_t cp_validate_one(const moq_cmsf_content_protection_t *cp)
{
    if (cp->ref_id.len == 0 || !cp->ref_id.data) return MOQ_ERR_INVAL;
    /* CMSF §4.1.1.3: scheme is the CENC protection scheme -- a closed enum of
     * "cenc" or "cbcs". (Syntax only; no DRM-system semantics.) */
    if (!(cp->scheme.data && cp->scheme.len == 4 &&
          (memcmp(cp->scheme.data, "cenc", 4) == 0 ||
           memcmp(cp->scheme.data, "cbcs", 4) == 0)))
        return MOQ_ERR_INVAL;
    /* CMSF §4.1.1.4.1: drmSystem.systemID is a UUID string (syntax only). */
    if (!cp_is_uuid(cp->drm_system.system_id)) return MOQ_ERR_INVAL;
    if (cp->default_kid_count == 0 || !cp->default_kids) return MOQ_ERR_INVAL;
    /* CMSF §4.1.1.2: each defaultKID is a UUID string (syntax only). */
    for (size_t i = 0; i < cp->default_kid_count; i++)
        if (!cp_is_uuid(cp->default_kids[i]))
            return MOQ_ERR_INVAL;
    /* Optional drmSystem fields: validated only when marked present. */
    moq_result_t ur;
    if ((ur = cp_url_valid(&cp->drm_system.la_url))   != MOQ_OK) return ur;
    if ((ur = cp_url_valid(&cp->drm_system.cert_url)) != MOQ_OK) return ur;
    if ((ur = cp_url_valid(&cp->drm_system.auth_url)) != MOQ_OK) return ur;
    /* CMSF §4.1.1.4.5: a present pssh must be a non-empty, syntactically valid
     * Base64 PSSH box (matches the strict MSF catalog encoder). */
    if (cp->drm_system.has_pssh &&
        (cp->drm_system.pssh.len == 0 || !cp->drm_system.pssh.data ||
         !cp_b64_syntax_ok(cp->drm_system.pssh)))
        return MOQ_ERR_INVAL;
    if (cp->drm_system.has_robustness &&
        (cp->drm_system.robustness.len == 0 ||
         !cp->drm_system.robustness.data))
        return MOQ_ERR_INVAL;
    return MOQ_OK;
}

static size_t cp_url_bytes(const moq_cmsf_url_t *u)
{
    if (!u->present) return 0;
    return u->url.len + (u->has_type ? u->type.len : 0);
}

/* Total nested-string bytes one entry contributes to the shared buffer. */
static size_t cp_entry_bytes(const moq_cmsf_content_protection_t *cp)
{
    size_t n = cp->ref_id.len + cp->scheme.len + cp->drm_system.system_id.len;
    for (size_t i = 0; i < cp->default_kid_count; i++)
        n += cp->default_kids[i].len;
    n += cp_url_bytes(&cp->drm_system.la_url);
    n += cp_url_bytes(&cp->drm_system.cert_url);
    n += cp_url_bytes(&cp->drm_system.auth_url);
    if (cp->drm_system.has_pssh) n += cp->drm_system.pssh.len;
    if (cp->drm_system.has_robustness) n += cp->drm_system.robustness.len;
    return n;
}

/* Copy src into buf+*off, return a span into buf, advance *off (empty -> {0}). */
static moq_bytes_t cp_put(uint8_t *buf, size_t *off, moq_bytes_t src)
{
    if (src.len == 0) return (moq_bytes_t){ NULL, 0 };
    memcpy(buf + *off, src.data, src.len);
    moq_bytes_t r = { buf + *off, src.len };
    *off += src.len;
    return r;
}

static moq_cmsf_url_t cp_copy_url(uint8_t *buf, size_t *off,
                                  const moq_cmsf_url_t *u)
{
    moq_cmsf_url_t r;
    memset(&r, 0, sizeof(r));
    if (!u->present) return r;
    r.present = true;
    r.url = cp_put(buf, off, u->url);
    r.has_type = u->has_type;
    if (u->has_type) r.type = cp_put(buf, off, u->type);
    return r;
}

/* Robust against partial copies: default_kids[i] is NULL (zeroed) for entries
 * not yet populated, so it is skipped. content_protection_count must already
 * reflect the array length. */
static void free_root_cps(moq_media_sender_t *s)
{
    if (s->content_protections) {
        for (size_t i = 0; i < s->content_protection_count; i++) {
            moq_cmsf_content_protection_t *cp = &s->content_protections[i];
            if (cp->default_kids)
                s->alloc.free(cp->default_kids,
                    cp->default_kid_count * sizeof(moq_bytes_t), s->alloc.ctx);
        }
        s->alloc.free(s->content_protections,
            s->content_protection_count * sizeof(moq_cmsf_content_protection_t),
            s->alloc.ctx);
        s->content_protections = NULL;
    }
    if (s->cp_strings) {
        s->alloc.free(s->cp_strings, s->cp_strings_size, s->alloc.ctx);
        s->cp_strings = NULL;
    }
    s->content_protection_count = 0;
    s->cp_strings_size = 0;
}

/* Deep-copy `count` validated entries into sender-owned storage. On any failure
 * leaves no partial state (frees whatever was allocated). */
static moq_result_t sender_copy_root_cps(moq_media_sender_t *s,
    const moq_cmsf_content_protection_t *src, size_t count)
{
    if (count == 0) return MOQ_OK;
    if (!src) return MOQ_ERR_INVAL;

    size_t strbytes = 0;
    for (size_t i = 0; i < count; i++)
        strbytes += cp_entry_bytes(&src[i]);

    moq_cmsf_content_protection_t *arr = (moq_cmsf_content_protection_t *)
        s->alloc.alloc(count * sizeof(*arr), s->alloc.ctx);
    uint8_t *strs = strbytes
        ? (uint8_t *)s->alloc.alloc(strbytes, s->alloc.ctx) : NULL;
    if (!arr || (strbytes && !strs)) {
        if (arr) s->alloc.free(arr, count * sizeof(*arr), s->alloc.ctx);
        if (strs) s->alloc.free(strs, strbytes, s->alloc.ctx);
        return MOQ_ERR_NOMEM;
    }
    memset(arr, 0, count * sizeof(*arr));
    s->content_protections = arr;
    s->content_protection_count = count;   /* set up front for free_root_cps */
    s->cp_strings = strs;
    s->cp_strings_size = strbytes;

    size_t off = 0;
    for (size_t i = 0; i < count; i++) {
        const moq_cmsf_content_protection_t *in = &src[i];
        moq_cmsf_content_protection_t *out = &arr[i];

        moq_bytes_t *kids = (moq_bytes_t *)s->alloc.alloc(
            in->default_kid_count * sizeof(moq_bytes_t), s->alloc.ctx);
        if (!kids) { free_root_cps(s); return MOQ_ERR_NOMEM; }
        out->default_kids = kids;
        out->default_kid_count = in->default_kid_count;

        out->ref_id = cp_put(strs, &off, in->ref_id);
        out->scheme = cp_put(strs, &off, in->scheme);
        for (size_t j = 0; j < in->default_kid_count; j++)
            kids[j] = cp_put(strs, &off, in->default_kids[j]);
        out->drm_system.system_id =
            cp_put(strs, &off, in->drm_system.system_id);
        out->drm_system.la_url   = cp_copy_url(strs, &off, &in->drm_system.la_url);
        out->drm_system.cert_url = cp_copy_url(strs, &off, &in->drm_system.cert_url);
        out->drm_system.auth_url = cp_copy_url(strs, &off, &in->drm_system.auth_url);
        out->drm_system.has_pssh = in->drm_system.has_pssh;
        if (in->drm_system.has_pssh)
            out->drm_system.pssh = cp_put(strs, &off, in->drm_system.pssh);
        out->drm_system.has_robustness = in->drm_system.has_robustness;
        if (in->drm_system.has_robustness)
            out->drm_system.robustness =
                cp_put(strs, &off, in->drm_system.robustness);
    }
    return MOQ_OK;
}

/* Resolve a track contentProtectionRefID against the configured root entries. */
static bool sender_has_cp_ref(const moq_media_sender_t *s, moq_bytes_t ref)
{
    for (size_t i = 0; i < s->content_protection_count; i++) {
        moq_bytes_t r = s->content_protections[i].ref_id;
        if (r.len == ref.len && ref.len &&
            memcmp(r.data, ref.data, ref.len) == 0)
            return true;
    }
    return false;
}

/* -- Construction ----------------------------------------------------- */

#ifdef MOQ_MEDIA_SENDER_TESTING
/* Test-only allocator injection for construction-arithmetic oracles: lets a
 * counting/refusing allocator observe every request sender_new makes, in
 * order, and refuse a nominated one. NULL restores the default. Never compiled
 * into the shipping library. */
static const moq_alloc_t *g_test_alloc;
void moq_media_sender_test_set_alloc(const moq_alloc_t *alloc);
void moq_media_sender_test_set_alloc(const moq_alloc_t *alloc)
{
    g_test_alloc = alloc;
}
void moq_media_sender_test_entry_sizes(size_t *ring_entry, size_t *track);
void moq_media_sender_test_entry_sizes(size_t *ring_entry, size_t *track)
{
    if (ring_entry) *ring_entry = sizeof(sender_preq_entry_t);
    if (track)      *track      = sizeof(moq_media_track_t);
}
#define SENDER_CTOR_ALLOC() (g_test_alloc ? g_test_alloc : moq_alloc_default())
/* Test-only pre-traversal observation for configuration validation: called
 * with the namespace part count immediately before the parts are walked.
 * Returning false stops validation with MOQ_ERR_INVAL, which keeps a fixture
 * with an intrinsically unrepresentable count bounded (no element is read)
 * while still recording that the walk was about to start. NULL disables. */
static bool (*g_test_parts_walk)(size_t count, void *ctx);
static void *g_test_parts_walk_ctx;
void moq_media_sender_test_set_parts_walk_hook(bool (*fn)(size_t, void *), void *ctx);
void moq_media_sender_test_set_parts_walk_hook(bool (*fn)(size_t, void *), void *ctx)
{
    g_test_parts_walk = fn;
    g_test_parts_walk_ctx = ctx;
}
#define SENDER_CFG_PARTS_WALK_HOOK(count) \
    do { if (g_test_parts_walk && !g_test_parts_walk((count), g_test_parts_walk_ctx)) \
             return MOQ_ERR_INVAL; } while (0)
/* Test-only observation that a BLOCK_TIMEOUT write has entered its blocking
 * path (called with s->mu held, after the stall is counted and before the
 * deadline is constructed; the hook must only signal, never call back into
 * the sender). NULL disables. */
static void (*g_test_wait_entry)(void *ctx);
static void *g_test_wait_entry_ctx;
void moq_media_sender_test_set_wait_entry_hook(void (*fn)(void *), void *ctx);
void moq_media_sender_test_set_wait_entry_hook(void (*fn)(void *), void *ctx)
{
    g_test_wait_entry = fn;
    g_test_wait_entry_ctx = ctx;
}
#define SENDER_WAIT_ENTRY_HOOK() \
    do { if (g_test_wait_entry) g_test_wait_entry(g_test_wait_entry_ctx); } while (0)
/* Test-only gap observation in write_mpeg2ts: called while the writer has
 * released s->mu between Objects (sender_ts_yield), so a test can latch a
 * session into that gap. NULL disables. */
static void (*g_test_ts_yield)(void *ctx);
static void *g_test_ts_yield_ctx;
void moq_media_sender_test_set_ts_yield_hook(void (*fn)(void *), void *ctx);
void moq_media_sender_test_set_ts_yield_hook(void (*fn)(void *), void *ctx)
{
    g_test_ts_yield = fn;
    g_test_ts_yield_ctx = ctx;
}
#define SENDER_TS_YIELD_HOOK() \
    do { if (g_test_ts_yield) g_test_ts_yield(g_test_ts_yield_ctx); } while (0)
/* Test-only gap observation in add_track: called after argument validation
 * and the initial state checks, before any allocation or the registration
 * critical section, so a test can drive a terminal transition into the gap. */
static void (*g_test_add_gap)(void *ctx);
static void *g_test_add_gap_ctx;
void moq_media_sender_test_set_add_track_gap_hook(void (*fn)(void *), void *ctx);
void moq_media_sender_test_set_add_track_gap_hook(void (*fn)(void *), void *ctx)
{
    g_test_add_gap = fn;
    g_test_add_gap_ctx = ctx;
}
#define SENDER_ADD_TRACK_GAP_HOOK() \
    do { if (g_test_add_gap) g_test_add_gap(g_test_add_gap_ctx); } while (0)
/* Test-only pre-traversal observation for add_track's content-protection
 * ref-id walk: called with the ref count immediately before the ids are
 * read. Returning false stops add_track with MOQ_ERR_INVAL, which keeps a
 * fixture with an intrinsically unrepresentable count bounded (no element is
 * read) while still recording that the walk was about to start. */
static bool (*g_test_refs_walk)(size_t count, void *ctx);
static void *g_test_refs_walk_ctx;
void moq_media_sender_test_set_refs_walk_hook(bool (*fn)(size_t, void *), void *ctx);
void moq_media_sender_test_set_refs_walk_hook(bool (*fn)(size_t, void *), void *ctx)
{
    g_test_refs_walk = fn;
    g_test_refs_walk_ctx = ctx;
}
#define SENDER_REFS_WALK_HOOK(count) \
    do { if (g_test_refs_walk && !g_test_refs_walk((count), g_test_refs_walk_ctx)) \
             return MOQ_ERR_INVAL; } while (0)
/* Test-only indirection for the BLOCK_TIMEOUT wait's clock reads and timed
 * wait, so a scripted oracle can declare exact clock values and wait results
 * and observe the exact absolute time the product passes to the wait. With no
 * script installed both expand to the real calls, so behaviour is unchanged. */
static int (*g_test_clock)(struct timespec *ts, void *ctx);
static void *g_test_clock_ctx;
void moq_media_sender_test_set_clock_hook(int (*fn)(struct timespec *, void *), void *ctx);
void moq_media_sender_test_set_clock_hook(int (*fn)(struct timespec *, void *), void *ctx)
{
    g_test_clock = fn;
    g_test_clock_ctx = ctx;
}
static int (*g_test_cond_wait)(const struct timespec *abs, void *ctx);
static void *g_test_cond_wait_ctx;
void moq_media_sender_test_set_cond_wait_hook(int (*fn)(const struct timespec *, void *), void *ctx);
void moq_media_sender_test_set_cond_wait_hook(int (*fn)(const struct timespec *, void *), void *ctx)
{
    g_test_cond_wait = fn;
    g_test_cond_wait_ctx = ctx;
}
#define SENDER_CLOCK_REALTIME(ts) \
    (g_test_clock ? g_test_clock((ts), g_test_clock_ctx) \
                  : clock_gettime(CLOCK_REALTIME, (ts)))
#define SENDER_COND_TIMEDWAIT(cv, mu, abs) \
    (g_test_cond_wait ? g_test_cond_wait((abs), g_test_cond_wait_ctx) \
                      : pthread_cond_timedwait((cv), (mu), (abs)))
#else
#define SENDER_CTOR_ALLOC() moq_alloc_default()
#define SENDER_CFG_PARTS_WALK_HOOK(count) ((void)0)
#define SENDER_WAIT_ENTRY_HOOK() ((void)0)
#define SENDER_TS_YIELD_HOOK() ((void)0)
#define SENDER_ADD_TRACK_GAP_HOOK() ((void)0)
#define SENDER_REFS_WALK_HOOK(count) ((void)0)
#define SENDER_CLOCK_REALTIME(ts) clock_gettime(CLOCK_REALTIME, (ts))
#define SENDER_COND_TIMEDWAIT(cv, mu, abs) pthread_cond_timedwait((cv), (mu), (abs))
#endif

/* The configured companion. An absent or partially covered field is AUTO,
 * the same default the sized initializers stamp. */
static moq_media_companion_profile_t sender_cfg_companion(
    const moq_media_sender_cfg_t *cfg)
{
    if (cfg->struct_size >= offsetof(moq_media_sender_cfg_t, companion_profile) +
                            sizeof(cfg->companion_profile))
        return cfg->companion_profile;
    return MOQ_MEDIA_COMPANION_AUTO;
}

static moq_result_t sender_validate_cfg(const moq_media_sender_cfg_t *cfg)
{
    if (!cfg) return MOQ_ERR_INVAL;
    if (cfg->struct_size < MEDIA_SENDER_CFG_MIN_SIZE)
        return MOQ_ERR_INVAL;
    moq_media_companion_profile_t companion = sender_cfg_companion(cfg);
    if (companion != MOQ_MEDIA_COMPANION_AUTO &&
        companion != MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01 &&
        companion != MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC)
        return MOQ_ERR_INVAL;
    if (cfg->namespace_.count == 0 || !cfg->namespace_.parts)
        return MOQ_ERR_INVAL;
    /* count * sizeof(moq_bytes_t) must be representable before a single part
     * is read: an intrinsically impossible count is refused, not traversed. */
    if (cfg->namespace_.count > SIZE_MAX / sizeof(moq_bytes_t))
        return MOQ_ERR_INVAL;
    SENDER_CFG_PARTS_WALK_HOOK(cfg->namespace_.count);
    for (size_t i = 0; i < cfg->namespace_.count; i++) {
        if (cfg->namespace_.parts[i].len == 0 ||
            !cfg->namespace_.parts[i].data)
            return MOQ_ERR_INVAL;
    }
    if (cfg->catalog_track.len > 0 && !cfg->catalog_track.data)
        return MOQ_ERR_INVAL;
    switch (cfg->backpressure) {
    case MOQ_MEDIA_SEND_BP_DROP_TO_KEYFRAME:
    case MOQ_MEDIA_SEND_BP_DROP_GROUP:
    case MOQ_MEDIA_SEND_BP_BLOCK_TIMEOUT:
    case MOQ_MEDIA_SEND_BP_RETURN_WOULD_BLOCK:
        break;
    default:
        return MOQ_ERR_INVAL;   /* UNSET: the choice is forced */
    }
    /* Root contentProtections shape (validated before connect so create()
     * rejects a bad cfg without standing up an endpoint). */
    if (cfg->content_protection_count > 0 && !cfg->content_protections)
        return MOQ_ERR_INVAL;
    for (size_t i = 0; i < cfg->content_protection_count; i++) {
        moq_result_t cr = cp_validate_one(&cfg->content_protections[i]);
        if (cr != MOQ_OK) return cr;
    }
    /* CMSF §4.1.1.1: each contentProtection refID is unique across the array
     * (information MUST NOT be duplicated). Reject a duplicate at create/attach
     * so track refID resolution is unambiguous. */
    for (size_t i = 0; i < cfg->content_protection_count; i++)
        for (size_t j = i + 1; j < cfg->content_protection_count; j++) {
            moq_bytes_t a = cfg->content_protections[i].ref_id;
            moq_bytes_t b = cfg->content_protections[j].ref_id;
            if (a.len == b.len && a.data && b.data &&
                memcmp(a.data, b.data, a.len) == 0)
                return MOQ_ERR_INVAL;
        }
    return MOQ_OK;
}

static void sender_ts_free(moq_media_sender_t *s, moq_media_track_t *t);

static void track_free(moq_media_sender_t *s, moq_media_track_t *t)
{
    if (!t) return;
    sender_ts_free(s, t);
    if (t->ext)
        s->alloc.free(t->ext, t->ext_block_size, s->alloc.ctx);
    if (t->cp_ref_ids)
        s->alloc.free(t->cp_ref_ids,
            t->cp_ref_id_count * sizeof(moq_bytes_t), s->alloc.ctx);
    /* Generated timeline allocations (the depends bytes themselves belong to
     * the media track's `strings`, so only the index array is freed here). */
    if (t->depends)
        s->alloc.free(t->depends,
            t->depends_count * sizeof(moq_bytes_t), s->alloc.ctx);
    if (t->sap_recs)
        s->alloc.free(t->sap_recs,
            t->sap_rec_cap * sizeof(sender_sap_rec_t), s->alloc.ctx);
    if (t->mt_recs)
        s->alloc.free(t->mt_recs,
            t->mt_rec_cap * sizeof(moq_msf_media_timeline_record_t),
            s->alloc.ctx);
    if (t->init_ref_alloc)
        s->alloc.free(t->init_ref_alloc, t->init_ref_alloc_size, s->alloc.ctx);
    if (t->strings) s->alloc.free(t->strings, t->strings_size, s->alloc.ctx);
    s->alloc.free(t, sizeof(*t), s->alloc.ctx);
}

/* Build (but do not register) a generated timeline track for `media`. With
 * media_timeline=false this is the SAP eventtimeline "<media>.sap"; with
 * media_timeline=true it is the MSF §7 mediatimeline "<media>.timeline". Both
 * 'depend' on the media track. Returns NULL on allocation failure. Borrows
 * media->name (stable for the sender's life). */
/* The generated sibling names are the media name plus one of these suffixes.
 * add_track derives and CHECKS each enabled sibling's name length before any
 * allocation and passes it in, so the addition is computed once. */
static const char SENDER_SAP_SUFFIX[4] = { '.', 's', 'a', 'p' };
static const char SENDER_MT_SUFFIX[9] =
    { '.', 't', 'i', 'm', 'e', 'l', 'i', 'n', 'e' };

static moq_media_track_t *sender_make_timeline(moq_media_sender_t *s,
                                               moq_media_track_t *media,
                                               uint32_t history_groups,
                                               bool media_timeline,
                                               size_t namelen)
{
    const char *suffix = media_timeline ? SENDER_MT_SUFFIX : SENDER_SAP_SUFFIX;
    size_t suffixlen = media_timeline ? sizeof(SENDER_MT_SUFFIX)
                                      : sizeof(SENDER_SAP_SUFFIX);
    moq_media_track_t *tl = (moq_media_track_t *)s->alloc.alloc(
        sizeof(*tl), s->alloc.ctx);
    uint8_t *nbuf = (uint8_t *)s->alloc.alloc(namelen, s->alloc.ctx);
    moq_bytes_t *deps = (moq_bytes_t *)s->alloc.alloc(
        sizeof(moq_bytes_t), s->alloc.ctx);
    if (!tl || !nbuf || !deps) {
        if (tl) s->alloc.free(tl, sizeof(*tl), s->alloc.ctx);
        if (nbuf) s->alloc.free(nbuf, namelen, s->alloc.ctx);
        if (deps) s->alloc.free(deps, sizeof(moq_bytes_t), s->alloc.ctx);
        return NULL;
    }
    memset(tl, 0, sizeof(*tl));
    memcpy(nbuf, media->name.data, media->name.len);
    memcpy(nbuf + media->name.len, suffix, suffixlen);
    tl->strings = nbuf;
    tl->strings_size = namelen;
    tl->name = (moq_bytes_t){ nbuf, namelen };
    tl->is_live = media->is_live;
    if (media_timeline) {
        tl->is_media_timeline = true;
        tl->mt_history_groups = history_groups ? history_groups
                            : SENDER_DEFAULT_MEDIA_TIMELINE_HISTORY_GROUPS;
    } else {
        tl->is_event_timeline = true;
        tl->sap_history_groups = history_groups ? history_groups
                                                : SENDER_DEFAULT_SAP_HISTORY_GROUPS;
    }
    deps[0] = media->name;   /* borrows the media track's stable name bytes */
    tl->depends = deps;
    tl->depends_count = 1;
    return tl;
}

/* True if a track named `name` is already registered (mu held). Catalog track
 * names must be unique, so this guards both app tracks and generated ones. */
static bool sender_init_ref_taken(const moq_media_sender_t *s, moq_bytes_t id)
{
    for (size_t i = 0; i < s->track_count; i++) {
        moq_bytes_t r = s->tracks[i]->init_ref;
        if (r.len == id.len && r.len > 0 && memcmp(r.data, id.data, r.len) == 0)
            return true;
    }
    return false;
}

/* Assign t's initDataList id (see moq_media_track.init_ref). Every track ever
 * registered on this sender -- removed ones included, since their objects and
 * ids outlive removal -- is considered, so an id is never rebound to
 * different bytes: identical init bytes share the existing id; otherwise the
 * id is the track's name, or "<name>~<n>" with the least n that is free when
 * the name already identifies other bytes (a removed name reused for new
 * init data). Returns false on allocation failure.
 * CALLER MUST HOLD s->mu; t is not yet registered. */
static bool sender_assign_init_ref(moq_media_sender_t *s, moq_media_track_t *t)
{
    if (!sender_init_by_reference(s, t))
        return true;
    for (size_t i = 0; i < s->track_count; i++) {
        const moq_media_track_t *u = s->tracks[i];
        if (u->init_ref.len > 0 && u->init_data.len == t->init_data.len &&
            memcmp(u->init_data.data, t->init_data.data,
                   t->init_data.len) == 0) {
            t->init_ref = u->init_ref;
            return true;
        }
    }
    if (!sender_init_ref_taken(s, t->name)) {
        t->init_ref = t->name;
        return true;
    }
    /* name + '~' + up to 20 decimal digits. A free suffix exists within
     * track_count + 1 tries, since at most track_count ids are taken. */
    size_t cap = t->name.len + 1 + 20;
    uint8_t *buf = (uint8_t *)s->alloc.alloc(cap, s->alloc.ctx);
    if (!buf) return false;
    memcpy(buf, t->name.data, t->name.len);
    for (uint64_t n = 1;; n++) {
        uint8_t digits[20];
        size_t nd = 0;
        for (uint64_t v = n; v > 0; v /= 10)
            digits[nd++] = (uint8_t)('0' + v % 10);
        size_t w = t->name.len;
        buf[w++] = '~';
        while (nd > 0) buf[w++] = digits[--nd];
        moq_bytes_t id = { buf, w };
        if (!sender_init_ref_taken(s, id)) {
            t->init_ref = id;
            t->init_ref_alloc = buf;
            t->init_ref_alloc_size = cap;
            return true;
        }
    }
}

static bool sender_name_taken(const moq_media_sender_t *s, moq_bytes_t name)
{
    for (size_t i = 0; i < s->track_count; i++) {
        if (s->tracks[i]->removed) continue;   /* a removed name may be reused */
        moq_bytes_t n = s->tracks[i]->name;
        if (n.len == name.len && memcmp(n.data, name.data, n.len) == 0)
            return true;
    }
    return false;
}

/* True if a REMOVED track named `name` still holds a publisher track (its
 * teardown -- end + remove -- has not finished). A replacement same-name track
 * must defer its own registration until the old publisher track is gone, or
 * moq_pub_add_track would reject the duplicate identity. Network thread: walks a
 * caller-held track snapshot (so a concurrent app-thread realloc cannot free the
 * vector mid-scan) and reads `removed` (app-written under s->mu) under the lock.
 * pub_track is network-confined and name bytes are immutable. The caller does not
 * hold s->mu, so taking it here is safe. */
static bool removed_pubtrack_with_name(moq_media_sender_t *s,
                                       moq_media_track_t *const *tracks,
                                       size_t n, moq_bytes_t name)
{
    bool found = false;
    pthread_mutex_lock(&s->mu);
    for (size_t i = 0; i < n; i++) {
        const moq_media_track_t *t = tracks[i];
        if (!t->removed || !t->pub_track) continue;
        if (t->name.len == name.len &&
            memcmp(t->name.data, name.data, name.len) == 0) {
            found = true;
            break;
        }
    }
    pthread_mutex_unlock(&s->mu);
    return found;
}

/* True if a removed track named `name` has not finished tearing down (mu held).
 * A post-ready same-name re-add must wait (retryable) until the old track is
 * fully gone, so the removal generation is published before the replacement and
 * the receiver sees remove-then-add rather than a changed tuple. */
static bool removed_samename_pending(const moq_media_sender_t *s,
                                     moq_bytes_t name)
{
    for (size_t i = 0; i < s->track_count; i++) {
        const moq_media_track_t *t = s->tracks[i];
        if (!t->removed || t->teardown_done) continue;
        if (t->name.len == name.len &&
            memcmp(t->name.data, name.data, name.len) == 0)
            return true;
    }
    return false;
}

/* Append one moq_media_track_t* to s->tracks (mu held). Returns false on
 * realloc failure (the track is NOT freed -- the caller owns cleanup). */
static bool sender_tracks_push(moq_media_sender_t *s, moq_media_track_t *t)
{
    if (s->track_count == s->track_cap) {
        size_t nc = s->track_cap ? s->track_cap * 2 : 4;
        moq_media_track_t **nt = (moq_media_track_t **)s->alloc.realloc(
            s->tracks, s->track_cap * sizeof(*nt), nc * sizeof(*nt),
            s->alloc.ctx);
        if (!nt) return false;
        s->tracks = nt;
        s->track_cap = nc;
    }
    s->tracks[s->track_count++] = t;
    return true;
}

static void sender_free(moq_media_sender_t *s)
{
    if (s->tracks) {
        for (size_t i = 0; i < s->track_count; i++)
            track_free(s, s->tracks[i]);
        s->alloc.free(s->tracks, s->track_cap * sizeof(moq_media_track_t *),
                      s->alloc.ctx);
    }
    if (s->preq) {
        while (s->preq_head != s->preq_tail) {
            preq_entry_release(s, &s->preq[s->preq_head % s->ring_cap]);
            s->preq_head++;
        }
        s->alloc.free(s->preq, s->ring_cap * sizeof(sender_preq_entry_t),
                      s->alloc.ctx);
    }
    if (s->ns_parts)
        s->alloc.free(s->ns_parts, s->namespace_.count * sizeof(moq_bytes_t),
                      s->alloc.ctx);
    if (s->ns_data)
        s->alloc.free(s->ns_data, s->ns_data_size, s->alloc.ctx);
    if (s->published_catalog) moq_rcbuf_decref(s->published_catalog);
    for (size_t pi = 0; pi < s->pending_obj_count; pi++)
        if (s->pending_objs[pi]) moq_rcbuf_decref(s->pending_objs[pi]);
    if (s->pending_current) moq_rcbuf_decref(s->pending_current);
    free_root_cps(s);
    pthread_cond_destroy(&s->space_cv);
    pthread_mutex_destroy(&s->mu);
    s->alloc.free(s, sizeof(*s), s->alloc.ctx);
}

/* Copy the cfg's appended tail (callbacks + publish_tracks +
 * drop_without_demand) into the sender, prefix-safely. The effective
 * callbacks prefix is bounded by BOTH the outer bytes the caller's
 * struct_size makes available past offsetof(callbacks) AND the nested
 * callbacks.struct_size -- so an old caller whose (complete, for its build)
 * callbacks block is smaller than this library's keeps every field it set,
 * and a mis-sized outer struct_size can never install bytes past the
 * caller's real storage. Field gates are whole-field: a partially-covered
 * pointer is never copied. s->callbacks must already be zeroed. */
static void sender_copy_cfg_tail(moq_media_sender_t *s,
                                 const moq_media_sender_cfg_t *cfg)
{
    s->companion_profile = sender_cfg_companion(cfg);
    s->companion_auto = s->companion_profile == MOQ_MEDIA_COMPANION_AUTO;
    size_t cb_off = offsetof(moq_media_sender_cfg_t, callbacks);
    if (cfg->struct_size > cb_off) {
        const moq_media_sender_callbacks_t *cb = &cfg->callbacks;
        size_t avail = cfg->struct_size - cb_off;
        if (avail > sizeof(*cb)) avail = sizeof(*cb);
        /* The nested struct_size is only readable when the outer prefix
         * covers it; then it further bounds the effective prefix. */
        size_t eff = 0;
        if (avail >= offsetof(moq_media_sender_callbacks_t, ctx))
            eff = cb->struct_size < avail ? cb->struct_size : avail;
#define SENDER_CB_FIELD_IN(name) \
        (eff >= offsetof(moq_media_sender_callbacks_t, name) + \
                sizeof(cb->name))
        if (SENDER_CB_FIELD_IN(ctx))
            s->callbacks.ctx = cb->ctx;
        if (SENDER_CB_FIELD_IN(on_subscriber_joined))
            s->callbacks.on_subscriber_joined = cb->on_subscriber_joined;
        if (SENDER_CB_FIELD_IN(on_subscriber_left))
            s->callbacks.on_subscriber_left = cb->on_subscriber_left;
        if (SENDER_CB_FIELD_IN(on_ready))
            s->callbacks.on_ready = cb->on_ready;
        if (SENDER_CB_FIELD_IN(on_closed))
            s->callbacks.on_closed = cb->on_closed;
        if (SENDER_CB_FIELD_IN(on_track_closed))
            s->callbacks.on_track_closed = cb->on_track_closed;
#undef SENDER_CB_FIELD_IN
        s->callbacks.struct_size = sizeof(s->callbacks);
    }
    if (cfg->struct_size >= offsetof(moq_media_sender_cfg_t, publish_tracks) +
                                sizeof(cfg->publish_tracks))
        s->publish_tracks = cfg->publish_tracks;
    if (cfg->struct_size >= offsetof(moq_media_sender_cfg_t,
                                     drop_without_demand) +
                                sizeof(cfg->drop_without_demand))
        s->drop_without_demand = cfg->drop_without_demand;
    /* Whole-field gate: an old caller whose struct_size predates this field
     * reads as 0. 0 -> disabled (the default: MSF-01 §5 says publish only on
     * availability change or cache staleness), UINT64_MAX -> disabled, else the
     * caller's opt-in interval. The deadline is armed later, when the initial
     * catalog installs. */
    {
        uint64_t raw = 0;
        if (cfg->struct_size >= offsetof(moq_media_sender_cfg_t,
                                         catalog_refresh_interval_us) +
                                    sizeof(cfg->catalog_refresh_interval_us))
            raw = cfg->catalog_refresh_interval_us;
        s->catalog_refresh_interval_us =
            (raw == 0) ? SENDER_CATALOG_REFRESH_DISABLED : raw;
    }
    s->catalog_refresh_deadline_us = UINT64_MAX;   /* not armed yet */
    s->refresh_wake_deadline_us = UINT64_MAX;      /* no managed wake yet */
}

/* Every size the constructor derives from a validated configuration, checked
 * for representability BEFORE the first allocation or endpoint effect: the
 * ring capacity (uint32), the ring allocation product and the namespace
 * storage sum plus the catalog-name addition (size_t). An unrepresentable
 * value is a configuration error (MOQ_ERR_INVAL), never a wrapped request. */
typedef struct sender_sizes {
    uint32_t queue_cap;
    uint32_t preq_cap;
    uint32_t ring_cap;
    size_t   ring_bytes;
    size_t   ns_bytes;
    size_t   ns_data_size;    /* namespace bytes + catalog name bytes */
    uint64_t group_base;      /* sender_resolve_group_base */
} sender_sizes_t;

/* count * elem fits in size_t. Taking the count as uint64_t keeps the check
 * meaningful on a 32-bit size_t without a tautological comparison on LP64. */
static bool sender_product_fits(uint64_t count, size_t elem)
{
    return count <= (uint64_t)(SIZE_MAX / elem);
}

/* -- BLOCK_TIMEOUT budget arithmetic ---------------------------------------
 * The wait keeps the REALTIME origin of the first clock read and the caller's
 * full uint64 microsecond duration as its reference; the absolute deadline is
 * never materialised, so no duration is narrowed and no budget is clamped.
 * Each iteration classifies what is left and forms only the NEXT bounded
 * absolute wait, which is the one value that must fit time_t.
 *
 * Representation, assumed and checked below: time_t is a signed two's
 * complement integer with no padding bits and no wider than uint64_t. Under
 * exactly those assumptions an ordered seconds difference can be taken in
 * unsigned arithmetic (converting a negative value to uint64_t is well
 * defined, and the difference of two converted values is the true difference
 * for any in-range ordered pair), and the maximum below is the value with
 * every non-sign bit set. The assertions check signedness, width and two's
 * complement; absence of padding bits is an ASSUMPTION of the supported
 * platforms, not something they verify. Narrower supported widths are covered
 * by the fixture's separate arithmetic model, not by this host's execution. */
#define SENDER_SLICE_NS 50000000L
#define SENDER_NS_PER_S 1000000000L
_Static_assert((time_t)-1 < 0, "time_t must be a signed type");
_Static_assert(sizeof(time_t) <= sizeof(uint64_t),
               "time_t must fit uint64_t for the ordered-difference arithmetic");
_Static_assert((time_t)-1 == (time_t)~(time_t)0,
               "time_t must be two's complement");
static const time_t SENDER_TIME_T_MAX =
    (time_t)(((uintmax_t)1 << (sizeof(time_t) * CHAR_BIT - 1)) - 1);

typedef enum {
    SENDER_BUDGET_EXPIRED,
    SENDER_BUDGET_UNDER_SLICE,     /* less than one slice remains */
    SENDER_BUDGET_AT_LEAST_SLICE
} sender_budget_t;

static bool ts_ge(struct timespec a, struct timespec b)
{
    return a.tv_sec > b.tv_sec ||
           (a.tv_sec == b.tv_sec && a.tv_nsec >= b.tv_nsec);
}

/* a - b for a >= b, as (seconds, nanoseconds). The seconds subtraction is
 * unsigned, so it cannot overflow for any pair of representable instants. */
static void ts_diff(struct timespec a, struct timespec b,
                    uint64_t *out_sec, long *out_nsec)
{
    uint64_t sec = (uint64_t)a.tv_sec - (uint64_t)b.tv_sec;
    long nsec = a.tv_nsec - b.tv_nsec;
    if (nsec < 0) { nsec += SENDER_NS_PER_S; sec -= 1u; }
    *out_sec = sec;
    *out_nsec = nsec;
}

/* How much of the requested budget is left at `now`. On UNDER_SLICE the exact
 * remainder (in nanoseconds, sub-microsecond precision preserved) is written
 * to *rem_ns; the other classes need no value. */
static sender_budget_t sender_budget(struct timespec origin, uint64_t dur_sec,
                                     long dur_nsec, struct timespec now,
                                     long *rem_ns)
{
    uint64_t rs;
    long rn;
    if (ts_ge(now, origin)) {
        uint64_t el_sec; long el_nsec;
        ts_diff(now, origin, &el_sec, &el_nsec);
        if (el_sec > dur_sec ||
            (el_sec == dur_sec && el_nsec >= dur_nsec))
            return SENDER_BUDGET_EXPIRED;
        rs = dur_sec - el_sec;
        rn = dur_nsec - el_nsec;
        if (rn < 0) { rn += SENDER_NS_PER_S; rs--; }
    } else {
        /* The clock moved BEFORE the origin. That distance is still to cover,
         * so the remaining budget is the requested duration plus it. A whole
         * second or more of backward distance already exceeds a slice, so the
         * class is known without forming a sum that could overflow. */
        uint64_t be_sec; long be_nsec;
        ts_diff(origin, now, &be_sec, &be_nsec);
        if (be_sec > 0) return SENDER_BUDGET_AT_LEAST_SLICE;
        rs = dur_sec;
        rn = dur_nsec + be_nsec;                 /* both < 1e9 */
        if (rn >= SENDER_NS_PER_S) { rn -= SENDER_NS_PER_S; rs++; }
    }
    if (rs > 0 || rn >= SENDER_SLICE_NS) return SENDER_BUDGET_AT_LEAST_SLICE;
    *rem_ns = rn;
    return SENDER_BUDGET_UNDER_SLICE;
}

/* acc + add fits in size_t; on success acc carries the sum. */
static bool sender_sum_fits(size_t *acc, size_t add)
{
    if (add > SIZE_MAX - *acc) return false;
    *acc += add;
    return true;
}

/* MSF-01 6.1: a publisher that restarts MUST start each track's Group IDs
 * above any it published before. The draft-22 companion takes the approach
 * 6.1 suggests, best-effort: the first Group ID of every track of the instance
 * is the wallclock in milliseconds since the Unix epoch, read once before the
 * sender exists, and later groups count up from it. This guarantees the 6.1
 * ordering only while the clock moves forward between instances and an
 * instance never runs ahead of it (more groups than elapsed milliseconds);
 * neither holds in general, so restart identity remains the application's
 * responsibility. The legacy companion starts at 0. An AUTO companion reads
 * the base the same way, failing creation the same way, because it may
 * resolve to draft 22 and objects written before then are numbered from it.
 *
 * The base must be a Group ID: a clock that cannot be read (or reads before
 * the epoch) is MOQ_ERR_INTERNAL, and a reading above MOQ_QUIC_VARINT_MAX is
 * MOQ_ERR_LIMIT, both before any allocation or endpoint effect. A base of
 * exactly MOQ_QUIC_VARINT_MAX is valid; the next group is then refused with
 * MOQ_ERR_LIMIT where it is requested. */
static moq_result_t sender_resolve_group_base(bool msf01, uint64_t *out)
{
    *out = 0;
    if (!msf01) return MOQ_OK;
    struct timespec ts;
    if (SENDER_CLOCK_REALTIME(&ts) != 0 || ts.tv_sec < 0 || ts.tv_nsec < 0 ||
        ts.tv_nsec >= SENDER_NS_PER_S)
        return MOQ_ERR_INTERNAL;
    uint64_t sec = (uint64_t)ts.tv_sec;
    if (sec > MOQ_QUIC_VARINT_MAX / 1000u) return MOQ_ERR_LIMIT;
    uint64_t ms = sec * 1000u + (uint64_t)ts.tv_nsec / 1000000u;
    if (ms > MOQ_QUIC_VARINT_MAX) return MOQ_ERR_LIMIT;
    *out = ms;
    return MOQ_OK;
}

static moq_result_t sender_prepare(const moq_media_sender_cfg_t *cfg,
                                   sender_sizes_t *sz)
{
    moq_result_t rc = sender_validate_cfg(cfg);
    if (rc < 0) return rc;
    memset(sz, 0, sizeof(*sz));
    sz->queue_cap = cfg->queue_max_objects
        ? cfg->queue_max_objects : SENDER_DEFAULT_QUEUE_OBJECTS;
    sz->preq_cap = cfg->pre_ready_max_objects
        ? cfg->pre_ready_max_objects : SENDER_DEFAULT_PRE_READY_OBJECTS;
    /* One ring serves both phases; size it for the larger bound (+1 so a
     * full ring is distinguishable from empty). The +1 is taken in 64 bits:
     * the largest representable capacity is UINT32_MAX-1 (ring_cap
     * UINT32_MAX); UINT32_MAX itself would wrap the ring to zero. */
    uint64_t ring = (uint64_t)(sz->queue_cap > sz->preq_cap ? sz->queue_cap
                                                             : sz->preq_cap) + 1u;
    if (ring > UINT32_MAX) return MOQ_ERR_INVAL;
    sz->ring_cap = (uint32_t)ring;
    if (!sender_product_fits(sz->ring_cap, sizeof(sender_preq_entry_t)))
        return MOQ_ERR_INVAL;
    sz->ring_bytes = (size_t)sz->ring_cap * sizeof(sender_preq_entry_t);
    for (size_t i = 0; i < cfg->namespace_.count; i++) {
        size_t len = cfg->namespace_.parts[i].len;
        if (len > SIZE_MAX - sz->ns_bytes) return MOQ_ERR_INVAL;
        sz->ns_bytes += len;
    }
    size_t catalog_len = cfg->catalog_track.len
        ? cfg->catalog_track.len : (size_t)MOQ_MSF_CATALOG_TRACK_NAME_LEN;
    if (catalog_len > SIZE_MAX - sz->ns_bytes) return MOQ_ERR_INVAL;
    sz->ns_data_size = sz->ns_bytes + catalog_len;
    /* AUTO reads the base too: objects written before the draft is known
     * are numbered from it, and a legacy resolution moves them down by it. */
    bool msf01 = sender_cfg_companion(cfg) !=
                 MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01;
    return sender_resolve_group_base(msf01, &sz->group_base);
}

static moq_result_t sender_new(moq_endpoint_t *ep, bool owns,
                               const moq_media_sender_cfg_t *cfg,
                               const sender_sizes_t *sz,
                               moq_media_sender_t **out)
{
    const moq_alloc_t *alloc = SENDER_CTOR_ALLOC();
    moq_media_sender_t *s = (moq_media_sender_t *)alloc->alloc(
        sizeof(*s), alloc->ctx);
    if (!s) return MOQ_ERR_NOMEM;
    memset(s, 0, sizeof(*s));
#ifdef MOQ_MEDIA_SENDER_TESTING
    s->test_block_republish_obj = -1;
#endif
    s->alloc = *alloc;
    s->ep = ep;
    s->owns_endpoint = owns;
    s->backpressure = cfg->backpressure;
    s->validate_cmaf = cfg->validate_cmaf;
    sender_copy_cfg_tail(s, cfg);
    s->group_base = sz->group_base;
    s->block_timeout_us = cfg->block_timeout_us
        ? cfg->block_timeout_us : SENDER_DEFAULT_BLOCK_TIMEOUT_US;
    s->queue_cap = sz->queue_cap;
    s->queue_byte_cap = cfg->queue_max_bytes
        ? cfg->queue_max_bytes : SENDER_DEFAULT_QUEUE_BYTES;
    s->preq_cap = sz->preq_cap;
    s->preq_byte_cap = cfg->pre_ready_max_bytes
        ? cfg->pre_ready_max_bytes : SENDER_DEFAULT_PRE_READY_BYTES;
    s->ring_cap = sz->ring_cap;
    /* Cleanup metadata is recorded before the buffers it describes are
     * requested, so a partial construction frees each block with the exact
     * size it was allocated with (sender_free reads these fields). */
    s->namespace_ = (moq_namespace_t){ NULL, cfg->namespace_.count };
    s->ns_data_size = sz->ns_data_size;
    s->stats.struct_size = (uint32_t)sizeof(s->stats);
    pthread_mutex_init(&s->mu, NULL);
    pthread_cond_init(&s->space_cv, NULL);

    /* Deep-copy the root content protections (validated in sender_validate_cfg;
     * sender_copy_root_cps leaves no partial state on failure). */
    {
        moq_result_t cprc = sender_copy_root_cps(s, cfg->content_protections,
                                                 cfg->content_protection_count);
        if (cprc < 0) { sender_free(s); return cprc; }
    }

    moq_bytes_t catalog = cfg->catalog_track;
    if (catalog.len == 0)
        catalog = (moq_bytes_t){
            (const uint8_t *)MOQ_MSF_CATALOG_TRACK_NAME,
            MOQ_MSF_CATALOG_TRACK_NAME_LEN };
    s->ns_parts = (moq_bytes_t *)s->alloc.alloc(
        cfg->namespace_.count * sizeof(moq_bytes_t), s->alloc.ctx);
    if (!s->ns_parts) { sender_free(s); return MOQ_ERR_NOMEM; }
    s->ns_data = (uint8_t *)s->alloc.alloc(sz->ns_data_size, s->alloc.ctx);
    if (!s->ns_data) { sender_free(s); return MOQ_ERR_NOMEM; }
    size_t off = 0;
    for (size_t i = 0; i < cfg->namespace_.count; i++) {
        memcpy(s->ns_data + off, cfg->namespace_.parts[i].data,
               cfg->namespace_.parts[i].len);
        s->ns_parts[i] = (moq_bytes_t){ s->ns_data + off,
                                        cfg->namespace_.parts[i].len };
        off += cfg->namespace_.parts[i].len;
    }
    memcpy(s->ns_data + off, catalog.data, catalog.len);
    s->catalog_name = s->ns_data + off;
    s->catalog_name_len = catalog.len;
    s->namespace_ = (moq_namespace_t){ s->ns_parts, cfg->namespace_.count };

    s->preq = (sender_preq_entry_t *)s->alloc.alloc(sz->ring_bytes, s->alloc.ctx);
    if (!s->preq) { sender_free(s); return MOQ_ERR_NOMEM; }
    memset(s->preq, 0, sz->ring_bytes);

    /* A private catalog track handle (not in the app's list; carries the
     * namespace advertisement and the retained catalog). */
    s->catalog_track = (moq_media_track_t *)s->alloc.alloc(
        sizeof(moq_media_track_t), s->alloc.ctx);
    if (!s->catalog_track) { sender_free(s); return MOQ_ERR_NOMEM; }
    memset(s->catalog_track, 0, sizeof(*s->catalog_track));
    s->catalog_track->is_catalog = true;

    /* ep == NULL is the test seam (moq_media_sender_test_new_cfg): the test
     * drives sender_hook directly, so skip the endpoint attach. Production
     * create/attach always passes a live endpoint. */
    if (ep) {
        moq_endpoint_hook_ops_t ops = {
            .pump = sender_hook,
            .next_deadline_us = sender_next_deadline_us,
        };
        moq_result_t arc = moq_endpoint_attach_hook(
            ep, MOQ_ENDPOINT_HOOK_SENDER, &ops, s);
        if (arc < 0) {
            track_free(s, s->catalog_track);
            s->catalog_track = NULL;
            sender_free(s);
            return arc;
        }
    }
    *out = s;
    return MOQ_OK;
}

moq_result_t moq_media_sender_attach(moq_endpoint_t *ep,
                                     const moq_media_sender_cfg_t *cfg,
                                     moq_media_sender_t **out)
{
    if (!ep || !out) return MOQ_ERR_INVAL;
    *out = NULL;
    sender_sizes_t sz;
    moq_result_t rc = sender_prepare(cfg, &sz);
    if (rc < 0) return rc;
    if (cfg->endpoint != NULL) return MOQ_ERR_INVAL;   /* attach borrows */
    return sender_new(ep, false, cfg, &sz, out);
}

moq_result_t moq_media_sender_create(const moq_media_sender_cfg_t *cfg,
                                     moq_media_sender_t **out)
{
    if (!out) return MOQ_ERR_INVAL;
    *out = NULL;
    sender_sizes_t sz;
    moq_result_t rc = sender_prepare(cfg, &sz);   /* before any endpoint effect */
    if (rc < 0) return rc;
    if (cfg->endpoint == NULL) return MOQ_ERR_INVAL;   /* create owns */

    moq_endpoint_t *ep = NULL;
    rc = moq_endpoint_connect(cfg->endpoint, &ep);
    if (rc < 0) return rc;
    rc = sender_new(ep, true, cfg, &sz, out);
    if (rc < 0) {
        (void)moq_endpoint_stop(ep);
        moq_endpoint_destroy(ep);
        return rc;
    }
    return MOQ_OK;
}

void moq_media_sender_destroy(moq_media_sender_t *s)
{
    if (!s) return;

    moq_endpoint_detach_hook(s->ep, MOQ_ENDPOINT_HOOK_SENDER, s);

    /* The publisher lives on the network thread: tear it down there if the
     * endpoint is still live, else inline (single-threaded once detached
     * from a terminal endpoint -- the hook can no longer run). */
    if (s->pub) {
        if (moq_endpoint_post(s->ep, sender_destroy_pub_task,
                              s->pub) != MOQ_OK)
            moq_pub_destroy(s->pub);
        s->pub = NULL;
    }

    if (s->owns_endpoint) {
        (void)moq_endpoint_stop(s->ep);
        moq_endpoint_destroy(s->ep);
    }
    if (s->catalog_track) track_free(s, s->catalog_track);
    sender_free(s);
}

/* -- Track management (app thread) ------------------------------------ */

/* -- Packaging extensions and MPEG2TS (add_track) ----------------------- */

/* The first published size of a draft-gregoire-moq-msfts record: through
 * `malformed`. */
#define SENDER_TS_RECORD_V0_SIZE \
    (offsetof(moq_msf_mpeg2ts_t, malformed) + sizeof(uint32_t))
#define SENDER_TS_DEFAULT_OBJECT_PACKETS 64u
/* Objects one packetizer call can hand over, and so the most a track holds
 * back under backpressure: a push ends the open Object and then fills at
 * most one more (a lead-in is shorter than an Object), and a flush ends the
 * Object the lead-in completes and then the Group's last one. */
#define SENDER_TS_STAGE_MAX 2u
/* SI PIDs the per-program filter holds (no byte-stream track is added in a
 * build without moq::mpeg2ts). */
#ifdef MOQ_SERVICE_HAVE_MPEG2TS
#define SENDER_TS_MAX_SI_PIDS MOQ_MPEG2TS_FILTER_MAX_SI_PIDS
#else
#define SENDER_TS_MAX_SI_PIDS 0u
#endif

static size_t sender_align8(size_t n) { return (n + 7u) & ~(size_t)7u; }

/* The list rules (the catalog encoder's, restated for add_track): an array
 * when count > 0, a known kind once, a record of at least its first
 * published size with coherent spans, and only on the packaging the kind
 * belongs to. Sets *rec to the MPEG2TS record (or NULL). No allocation. */
static moq_result_t sender_check_extensions(const moq_media_track_cfg_t *cfg,
                                            const moq_msf_mpeg2ts_t **rec)
{
    *rec = NULL;
    if (cfg->extension_count > 0 && !cfg->extensions) return MOQ_ERR_INVAL;
    if (!sender_product_fits(cfg->extension_count, sizeof(moq_msf_track_ext_t)))
        return MOQ_ERR_INVAL;
    for (size_t i = 0; i < cfg->extension_count; i++) {
        const moq_msf_track_ext_t *e = &cfg->extensions[i];
        if (e->kind != MOQ_MSF_EXT_MPEG2TS) return MOQ_ERR_INVAL;
        if (*rec) return MOQ_ERR_INVAL;                   /* duplicate kind */
        const moq_msf_mpeg2ts_t *m = e->u.mpeg2ts;
        if (!m || m->struct_size < SENDER_TS_RECORD_V0_SIZE)
            return MOQ_ERR_INVAL;
        if ((m->si_pid_count > 0 && !m->si_pids) ||
            (m->mode_name.len > 0 && !m->mode_name.data) ||
            (m->timestamp_mode_name.len > 0 && !m->timestamp_mode_name.data))
            return MOQ_ERR_INVAL;
        if (!sender_product_fits(m->si_pid_count, sizeof(uint32_t)))
            return MOQ_ERR_INVAL;
        *rec = m;
    }
    bool is_ts = cfg->packaging == MOQ_MEDIA_PACKAGING_MPEG2TS;
    if ((*rec != NULL) != is_ts) return MOQ_ERR_INVAL;
    return MOQ_OK;
}

/* Deep-copy the checked list into one block (entries, record, SI PIDs,
 * names), so the caller's buffers need not outlive add_track. */
static moq_result_t sender_copy_extensions(moq_media_sender_t *s,
                                           moq_media_track_t *t,
                                           const moq_msf_mpeg2ts_t *src)
{
    if (!src) return MOQ_OK;
    size_t pids = src->si_pid_count * sizeof(uint32_t);
    size_t need = sender_align8(sizeof(moq_msf_track_ext_t));
    if (!sender_sum_fits(&need, sender_align8(sizeof(moq_msf_mpeg2ts_t))) ||
        pids > SIZE_MAX - 8u ||
        !sender_sum_fits(&need, sender_align8(pids)) ||
        src->mode_name.len > SIZE_MAX - 8u ||
        !sender_sum_fits(&need, sender_align8(src->mode_name.len)) ||
        src->timestamp_mode_name.len > SIZE_MAX - 8u ||
        !sender_sum_fits(&need, sender_align8(src->timestamp_mode_name.len)))
        return MOQ_ERR_INVAL;
    uint8_t *blk = (uint8_t *)s->alloc.alloc(need, s->alloc.ctx);
    if (!blk) return MOQ_ERR_NOMEM;
    memset(blk, 0, need);
    moq_msf_track_ext_t *list = (moq_msf_track_ext_t *)(void *)blk;
    size_t off = sender_align8(sizeof(moq_msf_track_ext_t));
    moq_msf_mpeg2ts_t *rec = (moq_msf_mpeg2ts_t *)(void *)(blk + off);
    off += sender_align8(sizeof(*rec));
    moq_msf_mpeg2ts_init(rec);
    memcpy(rec, src, src->struct_size < sizeof(*rec) ? src->struct_size
                                                     : sizeof(*rec));
    rec->struct_size = (uint32_t)sizeof(*rec);
    rec->si_pids = NULL;
    rec->mode_name = (moq_bytes_t){ NULL, 0 };
    rec->timestamp_mode_name = (moq_bytes_t){ NULL, 0 };
    if (pids > 0) {
        memcpy(blk + off, src->si_pids, pids);
        rec->si_pids = (uint32_t *)(void *)(blk + off);
        off += sender_align8(pids);
    }
    if (src->mode_name.len > 0) {
        memcpy(blk + off, src->mode_name.data, src->mode_name.len);
        rec->mode_name = (moq_bytes_t){ blk + off, src->mode_name.len };
        off += sender_align8(src->mode_name.len);
    }
    if (src->timestamp_mode_name.len > 0) {
        memcpy(blk + off, src->timestamp_mode_name.data,
               src->timestamp_mode_name.len);
        rec->timestamp_mode_name =
            (moq_bytes_t){ blk + off, src->timestamp_mode_name.len };
    }
    list[0].kind = MOQ_MSF_EXT_MPEG2TS;
    list[0].u.mpeg2ts = rec;
    t->ext = list;
    t->ext_count = 1;
    t->ext_block_size = need;
    return MOQ_OK;
}

/* The sender-owned record of a track (NULL for other packagings). */
static moq_msf_mpeg2ts_t *sender_ts_record(moq_media_track_t *t)
{
    return t->ext_count ? (moq_msf_mpeg2ts_t *)(uintptr_t)t->ext[0].u.mpeg2ts
                        : NULL;
}

/* Whole source packets of `ps` octets with their sync bytes, at least one. */
static bool sender_ts_whole_packets(moq_bytes_t b, uint32_t ps)
{
    if (b.len == 0 || b.len % ps != 0) return false;
    size_t sync = ps == 192u ? 4u : 0u;
    for (size_t off = 0; off < b.len; off += ps)
        if (b.data[off + sync] != 0x47u) return false;
    return true;
}

#ifdef MOQ_SERVICE_HAVE_MPEG2TS
/* One Object the packetizer emitted, copied out of its buffer, or (lost)
 * the place of one that could not be copied. */
typedef struct sender_ts_obj {
    moq_rcbuf_t *payload;        /* NULL when lost */
    bool         new_group;
    bool         random_access;  /* holds a random access point */
    bool         lost;
} sender_ts_obj_t;

/* Byte-stream state of a TS track (allocated at add_track, freed with the
 * track). Touched only under s->mu by the one writer the busy latch admits. */
typedef struct sender_ts {
    moq_alloc_t                   alloc;
    uint32_t                      ps;
    moq_mpeg2ts_packetizer_t      pz;
    moq_mpeg2ts_program_filter_t *filter;     /* per-program record only */
    uint8_t                      *obj_buf;
    size_t                        obj_cap;
    uint8_t                      *lead_buf;
    size_t                        lead_cap;
    uint8_t                       carry[MOQ_MPEG2TS_M2TS_PACKET_SIZE];
    size_t                        carry_len;  /* bytes of an unfinished packet */
    sender_ts_obj_t               staged[SENDER_TS_STAGE_MAX];
    size_t                        staged_n;   /* emitted by the current call */
    sender_ts_obj_t               parked[SENDER_TS_STAGE_MAX];
    size_t                        parked_n;   /* held back under backpressure */
    bool                          emit_lost;  /* an Object could not be copied */
    bool                          broken;     /* the open Group lost an Object */
    bool                          busy;       /* a writer is in the call */
    bool                          ending;     /* end_track flushed the source */
    uint64_t                      partial_bytes_discarded;
    uint64_t                      continuation_discarded;
} sender_ts_t;

/* Packetizer emit callback: runs inside packetizer calls, under s->mu. The
 * payload is valid only during the callback, so it is copied. */
static void sender_ts_emit(void *ctx, const moq_mpeg2ts_emitted_object_t *obj)
{
    moq_media_track_t *t = (moq_media_track_t *)ctx;
    sender_ts_t *ts = (sender_ts_t *)t->ts;
    moq_rcbuf_t *b = NULL;
    if (ts->staged_n == SENDER_TS_STAGE_MAX) {
        /* Beyond the packetizer's bound: the last staged Object gives up its
         * place so the loss still lands in order. */
        sender_ts_obj_t *last = &ts->staged[SENDER_TS_STAGE_MAX - 1u];
        if (last->payload) moq_rcbuf_decref(last->payload);
        last->payload = NULL;
        last->lost = true;
        last->new_group = last->new_group || obj->new_group;
        ts->emit_lost = true;
        return;
    }
    sender_ts_obj_t *o = &ts->staged[ts->staged_n++];
    o->new_group = obj->new_group;
    o->random_access = obj->random_access;
    o->lost = moq_rcbuf_create(&ts->alloc, obj->payload.data,
                               obj->payload.len, &b) != MOQ_OK;
    o->payload = b;
    if (o->lost) ts->emit_lost = true;
}

static moq_result_t sender_ts_new(moq_media_sender_t *s, moq_media_track_t *t,
                                  const moq_media_track_cfg_t *cfg)
{
    const moq_msf_mpeg2ts_t *rec = sender_ts_record(t);
    uint32_t op = cfg->mpeg2ts_object_packets
                      ? cfg->mpeg2ts_object_packets
                      : SENDER_TS_DEFAULT_OBJECT_PACKETS;
    if (!sender_product_fits(op, rec->packet_size)) return MOQ_ERR_INVAL;
    sender_ts_t *ts = (sender_ts_t *)s->alloc.alloc(sizeof(*ts), s->alloc.ctx);
    if (!ts) return MOQ_ERR_NOMEM;
    memset(ts, 0, sizeof(*ts));
    ts->alloc = s->alloc;
    ts->ps = rec->packet_size;
    ts->obj_cap = (size_t)op * rec->packet_size;
    ts->lead_cap = (size_t)(op - 1u) * rec->packet_size;
    ts->obj_buf = (uint8_t *)s->alloc.alloc(ts->obj_cap, s->alloc.ctx);
    if (ts->lead_cap)
        ts->lead_buf = (uint8_t *)s->alloc.alloc(ts->lead_cap, s->alloc.ctx);
    bool per_program = rec->has_mode &&
                       rec->mode == MOQ_MSF_MPEG2TS_MODE_PER_PROGRAM;
    if (per_program)
        ts->filter = (moq_mpeg2ts_program_filter_t *)s->alloc.alloc(
            sizeof(*ts->filter), s->alloc.ctx);
    t->ts = ts;
    if (!ts->obj_buf || (ts->lead_cap && !ts->lead_buf) ||
        (per_program && !ts->filter)) {
        sender_ts_free(s, t);
        return MOQ_ERR_NOMEM;
    }

    moq_mpeg2ts_packetizer_cfg_t pc;
    moq_mpeg2ts_packetizer_cfg_init(&pc);
    pc.packet_size = rec->packet_size;
    pc.object_packets = op;
    pc.group_at_random_access = true;
    pc.program_number = rec->has_program_number
                            ? (uint16_t)rec->program_number : 0;
    pc.min_group_ms = cfg->mpeg2ts_min_group_ms;
    pc.wait_for_random_access = !cfg->mpeg2ts_forward_before_random_access;
    if (rec->has_pcr_pid) pc.pcr_pid = (uint16_t)rec->pcr_pid;
    pc.object_buf = ts->obj_buf;
    pc.object_cap = ts->obj_cap;
    pc.lead_buf = ts->lead_buf;
    pc.lead_cap = ts->lead_cap;
    pc.emit = sender_ts_emit;
    pc.ctx = t;
    moq_result_t rc = moq_mpeg2ts_packetizer_init(&ts->pz, sizeof(ts->pz), &pc);
    if (rc == MOQ_OK && per_program) {
        /* Per-Program: the record's program, its SI PIDs, and the CAT. */
        uint16_t si[MOQ_MPEG2TS_FILTER_MAX_SI_PIDS];
        for (size_t i = 0; i < rec->si_pid_count; i++)
            si[i] = (uint16_t)rec->si_pids[i];
        moq_mpeg2ts_program_filter_cfg_t fc;
        moq_mpeg2ts_program_filter_cfg_init(&fc);
        fc.packet_size = rec->packet_size;
        fc.program_number = (uint16_t)rec->program_number;
        fc.si_pids = rec->si_pid_count ? si : NULL;
        fc.si_pid_count = rec->si_pid_count;
        rc = moq_mpeg2ts_program_filter_init(ts->filter, sizeof(*ts->filter),
                                             &fc);
    }
    if (rc != MOQ_OK) {
        sender_ts_free(s, t);
        return MOQ_ERR_INVAL;
    }
    return MOQ_OK;
}

static void sender_ts_drop_list(sender_ts_obj_t *list, size_t n)
{
    for (size_t i = 0; i < n; i++)
        if (list[i].payload) moq_rcbuf_decref(list[i].payload);
}

static void sender_ts_free(moq_media_sender_t *s, moq_media_track_t *t)
{
    sender_ts_t *ts = (sender_ts_t *)t->ts;
    if (!ts) return;
    sender_ts_drop_list(ts->staged, ts->staged_n);
    sender_ts_drop_list(ts->parked, ts->parked_n);
    if (ts->obj_buf) s->alloc.free(ts->obj_buf, ts->obj_cap, s->alloc.ctx);
    if (ts->lead_buf) s->alloc.free(ts->lead_buf, ts->lead_cap, s->alloc.ctx);
    if (ts->filter)
        s->alloc.free(ts->filter, sizeof(*ts->filter), s->alloc.ctx);
    s->alloc.free(ts, sizeof(*ts), s->alloc.ctx);
    t->ts = NULL;
}
#else
static void sender_ts_free(moq_media_sender_t *s, moq_media_track_t *t)
{
    (void)s; (void)t;
}
#endif

/* add_track's MPEG2TS rules (draft-gregoire-moq-msfts) and the record the
 * service authors: everything checked before the track is registered. */
static moq_result_t sender_ts_prepare(moq_media_sender_t *s,
                                      moq_media_track_t *t,
                                      const moq_media_track_cfg_t *cfg)
{
    moq_msf_mpeg2ts_t *rec = sender_ts_record(t);
    /* ES-packets authoring needs the multi-track split and the PAT/PMT
     * tracks, which stay helper-level; timelines need per-Object
     * presentation times, which TS keeps inside the packets. */
    if (rec->has_mode && rec->mode == MOQ_MSF_MPEG2TS_MODE_ES_PACKETS)
        return MOQ_ERR_INVAL;
    if (cfg->emit_sap_timeline || cfg->emit_media_timeline)
        return MOQ_ERR_INVAL;
    if (!rec->has_packet_size ||
        (rec->packet_size != 188u && rec->packet_size != 192u))
        return MOQ_ERR_INVAL;
    if (t->init_data.len > 0 &&
        !sender_ts_whole_packets(t->init_data, rec->packet_size))
        return MOQ_ERR_INVAL;

    if (!cfg->mpeg2ts_app_objects) {
        /* Byte stream: the program filter the record implies needs its
         * program number, and SI PIDs it can hold. */
        if (rec->has_mode && rec->mode == MOQ_MSF_MPEG2TS_MODE_PER_PROGRAM &&
            (!rec->has_program_number ||
             rec->si_pid_count > SENDER_TS_MAX_SI_PIDS))
            return MOQ_ERR_INVAL;
        /* mpeg2tsRandomAccess is the service's to declare: Groups open at
         * random access points, so it holds unless packets before the first
         * one are forwarded. An unmodified multiplex without a reference
         * program names no stream to hold it on (the field is absent). */
        bool multiplex_unref =
            rec->has_mode &&
            rec->mode == MOQ_MSF_MPEG2TS_MODE_UNMODIFIED_MULTIPLEX &&
            !rec->has_program_number;
        bool derived = !cfg->mpeg2ts_forward_before_random_access;
        if (rec->has_random_access &&
            (multiplex_unref || rec->random_access != derived))
            return MOQ_ERR_INVAL;
        if (!multiplex_unref) {
            rec->has_random_access = true;
            rec->random_access = derived;
        }
    } else {
        t->ts_app_objects = true;
        t->ts_random_access = rec->has_random_access && rec->random_access;
    }

    /* The track as the catalog will carry it must pass the subscriber's
     * check, so a bad track fails here rather than at catalog encode. */
    moq_msf_track_t m;
    track_fill_msf(t, &m);
    if (moq_msf_track_mpeg2ts_check(&m, NULL) != MOQ_OK) return MOQ_ERR_INVAL;

#ifdef MOQ_SERVICE_HAVE_MPEG2TS
    if (!cfg->mpeg2ts_app_objects) return sender_ts_new(s, t, cfg);
#else
    (void)s;
#endif
    return MOQ_OK;
}

/* Every size add_track derives from a validated track cfg, each checked for
 * representability BEFORE the first allocation, copy or registration: the
 * six media spans plus the resolved ref-id bytes (one strings buffer), the
 * ref-id index product, and each ENABLED generated sibling's name. An
 * unrepresentable configuration is a cfg error (MOQ_ERR_INVAL), never a
 * wrapped request. */
typedef struct track_sizes {
    size_t strings;       /* media spans + resolved ref-id bytes */
    size_t cpref_bytes;
    size_t cpref_index;   /* count * sizeof(moq_bytes_t) */
    size_t sap_name;      /* 0 when the sibling is disabled */
    size_t mt_name;
} track_sizes_t;

static moq_result_t track_derive_sizes(const moq_media_track_cfg_t *cfg,
                                       track_sizes_t *sz)
{
    memset(sz, 0, sizeof(*sz));
    size_t media = 0;
    if (!sender_sum_fits(&media, cfg->name.len) ||
        !sender_sum_fits(&media, cfg->codec.len) ||
        !sender_sum_fits(&media, cfg->init_data.len) ||
        !sender_sum_fits(&media, cfg->role.len) ||
        !sender_sum_fits(&media, cfg->lang.len) ||
        !sender_sum_fits(&media, cfg->channel_config.len) ||
        !sender_sum_fits(&media, cfg->track_properties.len))
        return MOQ_ERR_INVAL;
    /* Each resolved ref id contributes its bytes to the SAME buffer. Every
     * subtotal addition is checked, so the final sum below can never receive
     * an already-wrapped subtotal. */
    for (size_t i = 0; i < cfg->content_protection_ref_id_count; i++)
        if (!sender_sum_fits(&sz->cpref_bytes,
                             cfg->content_protection_ref_ids[i].len))
            return MOQ_ERR_INVAL;
    sz->strings = media;
    if (!sender_sum_fits(&sz->strings, sz->cpref_bytes))
        return MOQ_ERR_INVAL;
    /* The count product was checked before the ids were traversed. */
    sz->cpref_index = cfg->content_protection_ref_id_count * sizeof(moq_bytes_t);
    if (cfg->emit_sap_timeline) {
        sz->sap_name = cfg->name.len;
        if (!sender_sum_fits(&sz->sap_name, sizeof(SENDER_SAP_SUFFIX)))
            return MOQ_ERR_INVAL;
    }
    if (cfg->emit_media_timeline) {
        sz->mt_name = cfg->name.len;
        if (!sender_sum_fits(&sz->mt_name, sizeof(SENDER_MT_SUFFIX)))
            return MOQ_ERR_INVAL;
    }
    return MOQ_OK;
}

/* Read a caller's track cfg into a full current struct: the v0 prefix, then
 * each appended field only when struct_size covers all of it, and a has_X /
 * X pair or an array / count pair only when it covers the whole pair. Fields
 * left out keep their zero default. The caller has checked struct_size >=
 * MEDIA_TRACK_CFG_MIN_SIZE. */
static void sender_track_cfg_read(const moq_media_track_cfg_t *in,
                                  moq_media_track_cfg_t *out)
{
    memset(out, 0, sizeof(*out));
    memcpy(out, in, MEDIA_TRACK_CFG_MIN_SIZE);
#define TCFG_HAS(f) \
    (in->struct_size >= offsetof(moq_media_track_cfg_t, f) + sizeof(in->f))
    if (TCFG_HAS(track_duration_ms)) {
        out->has_track_duration = in->has_track_duration;
        out->track_duration_ms = in->track_duration_ms;
    }
    if (TCFG_HAS(emit_media_timeline))
        out->emit_media_timeline = in->emit_media_timeline;
    if (TCFG_HAS(media_timeline_history_groups))
        out->media_timeline_history_groups = in->media_timeline_history_groups;
    if (TCFG_HAS(alt_group)) {
        out->has_alt_group = in->has_alt_group;
        out->alt_group = in->alt_group;
    }
    if (TCFG_HAS(publisher_priority)) {
        out->has_publisher_priority = in->has_publisher_priority;
        out->publisher_priority = in->publisher_priority;
    }
    if (TCFG_HAS(extension_count)) {
        out->extensions = in->extensions;
        out->extension_count = in->extension_count;
    }
    if (TCFG_HAS(mpeg2ts_app_objects))
        out->mpeg2ts_app_objects = in->mpeg2ts_app_objects;
    if (TCFG_HAS(mpeg2ts_object_packets))
        out->mpeg2ts_object_packets = in->mpeg2ts_object_packets;
    if (TCFG_HAS(mpeg2ts_min_group_ms))
        out->mpeg2ts_min_group_ms = in->mpeg2ts_min_group_ms;
    if (TCFG_HAS(mpeg2ts_forward_before_random_access))
        out->mpeg2ts_forward_before_random_access =
            in->mpeg2ts_forward_before_random_access;
    if (TCFG_HAS(track_properties))
        out->track_properties = in->track_properties;
    if (TCFG_HAS(dynamic_groups))
        out->dynamic_groups = in->dynamic_groups;
    if (TCFG_HAS(group_order))
        out->group_order = in->group_order;
#undef TCFG_HAS
    out->struct_size = (uint32_t)sizeof(*out);
}

moq_result_t moq_media_sender_add_track(moq_media_sender_t *s,
                                        const moq_media_track_cfg_t *cfg,
                                        moq_media_track_t **out)
{
    if (out) *out = NULL;
    if (!s || !cfg || !out) return MOQ_ERR_INVAL;
    if (cfg->struct_size < MEDIA_TRACK_CFG_MIN_SIZE)
        return MOQ_ERR_INVAL;
    /* Prefix-safe by struct_size: read the caller's (possibly older/smaller,
     * or newer/larger) cfg into a zero-filled local, so appended fields an
     * older caller lacks default to absent and a newer caller's unknown
     * trailing fields are ignored. All reads below use the local copy. */
    moq_media_track_cfg_t cfg_local;
    sender_track_cfg_read(cfg, &cfg_local);
    cfg = &cfg_local;
    if (cfg->name.len == 0 || !cfg->name.data) return MOQ_ERR_INVAL;
    if (cfg->media_type != MOQ_MEDIA_TYPE_VIDEO &&
        cfg->media_type != MOQ_MEDIA_TYPE_AUDIO)
        return MOQ_ERR_INVAL;
    bool is_ts = cfg->packaging == MOQ_MEDIA_PACKAGING_MPEG2TS;
    if (cfg->packaging != MOQ_MEDIA_PACKAGING_RAW &&
        cfg->packaging != MOQ_MEDIA_PACKAGING_CMAF && !is_ts)
        return MOQ_ERR_INVAL;
#ifndef MOQ_SERVICE_HAVE_MPEG2TS
    if (is_ts) return MOQ_ERR_UNSUPPORTED;   /* not in this build */
#endif
    const moq_msf_mpeg2ts_t *ts_rec = NULL;
    {
        moq_result_t erc = sender_check_extensions(cfg, &ts_rec);
        if (erc < 0) return erc;
    }
    /* Every optional span must be coherent: a non-zero length with a NULL
     * data pointer would crash the deep-copy below. */
    if ((cfg->codec.len && !cfg->codec.data) ||
        (cfg->init_data.len && !cfg->init_data.data) ||
        (cfg->role.len && !cfg->role.data) ||
        (cfg->lang.len && !cfg->lang.data) ||
        (cfg->channel_config.len && !cfg->channel_config.data) ||
        (cfg->track_properties.len && !cfg->track_properties.data))
        return MOQ_ERR_INVAL;
    /* MSF-01: an audio/video track has an inherent codec, so it MUST carry the
     * conditional catalog fields for that codec -- the sender never emits a
     * track that is invalid by construction. codec (5.2.18) and maximum bitrate
     * (5.2.22) are required for both media types; samplerate (5.2.28) and
     * channelConfig (5.2.29) are additionally required for audio. media_type is
     * always VIDEO or AUDIO here (checked above). Generated timeline tracks
     * (.sap/.timeline) are synthesized on a separate internal path and never
     * reach this public entry, so they remain exempt. */
    /* An MPEG2TS track is a multiplex: it has no inherent codec, and
     * samplerate / channelConfig describe one elementary stream, not it. */
    if (cfg->codec.len == 0 && !is_ts) return MOQ_ERR_INVAL;   /* 5.2.18 */
    if (cfg->bitrate == 0) return MOQ_ERR_INVAL;         /* 5.2.22 max bitrate */
    if (cfg->media_type == MOQ_MEDIA_TYPE_AUDIO && !is_ts) {
        if (cfg->samplerate == 0) return MOQ_ERR_INVAL;          /* 5.2.28 */
        if (cfg->channel_config.len == 0) return MOQ_ERR_INVAL;  /* 5.2.29 */
    }
    /* CMSF-01 3.1: a CMAF track's initialization (its moov) MUST reach the
     * catalog; without it no moof is decodable. Enforced for the draft-22
     * companion, whose catalog carries it in the root initDataList. An
     * explicit companion is fixed at creation, so this read needs no lock;
     * an AUTO sender is checked under the lock at registration. */
    bool cmaf_needs_init =
        cfg->packaging == MOQ_MEDIA_PACKAGING_CMAF && cfg->init_data.len == 0;
    if (cmaf_needs_init && !s->companion_auto && sender_msf01_catalog(s))
        return MOQ_ERR_INVAL;
    /* contentProtectionRefIDs: each id must be coherent AND resolve to a
     * configured root contentProtections entry (catalog coherence, not DRM
     * semantics). Root entries are immutable after construction, so this read
     * needs no lock. */
    if (cfg->content_protection_ref_id_count > 0 &&
        !cfg->content_protection_ref_ids)
        return MOQ_ERR_INVAL;
    /* The index product must be representable before a single id is read: an
     * intrinsically impossible count is refused, not traversed. */
    if (!sender_product_fits(cfg->content_protection_ref_id_count,
                             sizeof(moq_bytes_t)))
        return MOQ_ERR_INVAL;
    SENDER_REFS_WALK_HOOK(cfg->content_protection_ref_id_count);
    for (size_t i = 0; i < cfg->content_protection_ref_id_count; i++) {
        moq_bytes_t ref = cfg->content_protection_ref_ids[i];
        if (ref.len == 0 || !ref.data) return MOQ_ERR_INVAL;
        if (!sender_has_cp_ref(s, ref)) return MOQ_ERR_INVAL;
    }
    /* Derive every allocation size now, with the other cfg-shape checks and
     * before any state check, allocation, copy or registration. */
    track_sizes_t sz;
    {
        moq_result_t drc = track_derive_sizes(cfg, &sz);
        if (drc < 0) return drc;
    }
    /* MSF §5.2.35: trackDuration MUST NOT appear on a live track. */
    if (cfg->has_track_duration && cfg->is_live) return MOQ_ERR_INVAL;
    /* Also when no version is known yet to check the properties against. */
    if (cfg->group_order > MOQ_GROUP_ORDER_DESCENDING) return MOQ_ERR_INVAL;

    /* A terminal sender (fatal, or its endpoint closed) registers nothing:
     * checked here outside s->mu (sender_terminal takes it), and again inside
     * the registration critical section below with the ep->mu-free probe, so
     * a transition in between cannot slip a track in. */
    if (sender_terminal(s)) return MOQ_ERR_CLOSED;

    /* add_track is legal both before and after READY : a post-ready add
     * registers the track and triggers an independent catalog republish. Once
     * completion has been requested (§11.3) the sender is terminal. */
    pthread_mutex_lock(&s->mu);
    if (s->fatal) { pthread_mutex_unlock(&s->mu); return MOQ_ERR_CLOSED; }
    if (s->completing) { pthread_mutex_unlock(&s->mu); return MOQ_ERR_WRONG_STATE; }
    moq_version_t negotiated = s->transport_version;
    pthread_mutex_unlock(&s->mu);
    SENDER_ADD_TRACK_GAP_HOOK();

    /* Track Properties are checked here, as the publisher facade will check
     * them, so an unpublishable set is refused at the call rather than
     * failing the sender when the track activates: against the negotiated
     * version once known, before that against every version the endpoint
     * offers (any may be negotiated). */
    if (cfg->track_properties.len > 0 || cfg->dynamic_groups ||
        cfg->group_order != MOQ_GROUP_ORDER_DEFAULT) {
        moq_version_t versions[MOQ_ENDPOINT_MAX_VERSIONS];
        size_t nv = 1;
        if (negotiated) versions[0] = negotiated;
        else nv = moq_endpoint_offered_versions_internal(s->ep, versions,
                                                         MOQ_ENDPOINT_MAX_VERSIONS);
        for (size_t i = 0; i < nv && i < MOQ_ENDPOINT_MAX_VERSIONS; i++) {
            moq_result_t vrc = moq_core_internal_track_properties_validate(
                versions[i], cfg->track_properties.data,
                cfg->track_properties.len, cfg->dynamic_groups,
                cfg->group_order);
            if (vrc == MOQ_ERR_INVAL || vrc == MOQ_ERR_NOMEM) return vrc;
        }
    }

    /* Deep-copy every span into one buffer so the cfg need not outlive the
     * call and the catalog encode borrows stable bytes. The contentProtection
     * ref-id bytes share that buffer; their moq_bytes_t[] index is a separate
     * (aligned) allocation. */
    size_t ncpref = cfg->content_protection_ref_id_count;
    size_t need = sz.strings;          /* derived and checked above */
    moq_media_track_t *t = (moq_media_track_t *)s->alloc.alloc(
        sizeof(*t), s->alloc.ctx);
    uint8_t *buf = need ? (uint8_t *)s->alloc.alloc(need, s->alloc.ctx) : NULL;
    moq_bytes_t *cprefs = ncpref
        ? (moq_bytes_t *)s->alloc.alloc(sz.cpref_index, s->alloc.ctx)
        : NULL;
    if (!t || (need && !buf) || (ncpref && !cprefs)) {
        if (t) s->alloc.free(t, sizeof(*t), s->alloc.ctx);
        if (buf) s->alloc.free(buf, need, s->alloc.ctx);
        if (cprefs) s->alloc.free(cprefs, sz.cpref_index, s->alloc.ctx);
        return MOQ_ERR_NOMEM;
    }
    memset(t, 0, sizeof(*t));
    t->strings = buf;
    t->strings_size = need;
    t->cp_ref_ids = cprefs;
    t->cp_ref_id_count = ncpref;
    size_t off = 0;
#define COPY_SPAN(dst, src) do { \
        if ((src).len) { memcpy(buf + off, (src).data, (src).len); \
            (dst) = (moq_bytes_t){ buf + off, (src).len }; off += (src).len; } \
    } while (0)
    COPY_SPAN(t->name, cfg->name);
    COPY_SPAN(t->codec, cfg->codec);
    COPY_SPAN(t->init_data, cfg->init_data);
    COPY_SPAN(t->role, cfg->role);
    COPY_SPAN(t->lang, cfg->lang);
    COPY_SPAN(t->channel_config, cfg->channel_config);
    COPY_SPAN(t->track_properties, cfg->track_properties);
#undef COPY_SPAN
    /* Read from the owned copy. Only a RAW track of a sender that can
     * synthesize LOC-04 (AUTO) needs it; it gates typed capture times in
     * write(). */
    if (cfg->packaging == MOQ_MEDIA_PACKAGING_RAW && s->companion_auto)
        t->track_scale = sender_track_scale_of(t->track_properties);
    /* contentProtection ref ids: copy bytes into the shared buffer, index via
     * the dedicated array (validated above; each ref.len > 0). */
    for (size_t i = 0; i < ncpref; i++) {
        moq_bytes_t r = cfg->content_protection_ref_ids[i];
        memcpy(buf + off, r.data, r.len);
        cprefs[i] = (moq_bytes_t){ buf + off, r.len };
        off += r.len;
    }
    t->media_type = cfg->media_type;
    t->packaging = cfg->packaging;
    /* A TS track's timing travels in the packets: its catalog carries a
     * timescale only when the app gives one. */
    t->timescale = cfg->timescale ? cfg->timescale : is_ts ? 0u : 1000000u;
    t->is_live = cfg->is_live;
    t->has_track_duration = cfg->has_track_duration;
    t->track_duration_ms = cfg->track_duration_ms;
    t->has_alt_group = cfg->has_alt_group;
    t->alt_group = cfg->alt_group;
    t->has_publisher_priority = cfg->has_publisher_priority;
    t->publisher_priority = cfg->publisher_priority;
    t->group_order = cfg->group_order;
    t->has_max_grp_sap = cfg->has_max_grp_sap;
    t->max_grp_sap = cfg->max_grp_sap;
    t->has_max_obj_sap = cfg->has_max_obj_sap;
    t->max_obj_sap = cfg->max_obj_sap;
    t->dynamic_groups = cfg->dynamic_groups;

    /* Parse a CMAF init segment once to cache its track_ID, so CMAF
     * validation can check object track_IDs against the init (CMSF §3.3), and
     * to enforce protected-track coherence (below). A non-CMAF track or an
     * unparseable init simply leaves the track_ID absent. */
    bool cmaf_init_parsed = false;
    moq_cmaf_init_info_t ii;
    moq_cmaf_init_info_init(&ii);
    if (t->packaging == MOQ_MEDIA_PACKAGING_CMAF && t->init_data.len > 0) {
        cmaf_init_parsed = (moq_cmaf_parse_init(t->init_data, &ii) == MOQ_OK);
        if (cmaf_init_parsed && ii.track_id != 0) {
            t->has_cmaf_init_track_id = true;
            t->cmaf_init_track_id = ii.track_id;
        }
    }

    /* CMSF §4.2: a CMAF track that declares contentProtectionRefIDs is
     * CENC-encrypted (§4.1.2), so its init data MUST carry the CENC boxes
     * (sinf/schm/schi/tenc). Authoring an encrypted track without protected
     * init would emit an internally inconsistent catalog, so this is always-on
     * catalog coherence -- like the ref-id-resolution check above -- not gated
     * under validate_cmaf. (No defaultKID<->tenc comparison or DRM semantics.) */
    if (t->packaging == MOQ_MEDIA_PACKAGING_CMAF && ncpref > 0) {
        if (t->init_data.len == 0 || !cmaf_init_parsed || !ii.has_cenc) {
            track_free(s, t);
            return MOQ_ERR_INVAL;
        }
    }
    t->width = cfg->width;
    t->height = cfg->height;
    t->framerate_millis = cfg->framerate_millis;
    t->samplerate = cfg->samplerate;
    t->bitrate = cfg->bitrate;

    /* The packaging-extension records, and an MPEG2TS track's own rules. */
    {
        moq_result_t xrc = sender_copy_extensions(s, t, ts_rec);
        if (xrc == MOQ_OK && is_ts) xrc = sender_ts_prepare(s, t, cfg);
        if (xrc < 0) { track_free(s, t); return xrc; }
    }

    /* Build the generated timeline siblings before the lock (they borrow the
     * media track's stable name bytes). A media track may opt into both the SAP
     * eventtimeline and the MSF media timeline; each is an independent generated
     * track. Registered atomically with the media track below so a collision /
     * NOMEM leaves the sender unchanged. */
    moq_media_track_t *tl = NULL;    /* SAP eventtimeline "<name>.sap" */
    moq_media_track_t *mtl = NULL;   /* media timeline "<name>.timeline" */
    if (cfg->emit_sap_timeline) {
        tl = sender_make_timeline(s, t, cfg->sap_timeline_history_groups, false,
                                  sz.sap_name);
        if (!tl) { track_free(s, t); return MOQ_ERR_NOMEM; }
        t->sap_timeline = tl;
    }
    if (cfg->emit_media_timeline) {
        mtl = sender_make_timeline(s, t, cfg->media_timeline_history_groups,
                                   true, sz.mt_name);
        if (!mtl) {
            if (tl) track_free(s, tl);
            track_free(s, t);
            return MOQ_ERR_NOMEM;
        }
        t->media_timeline = mtl;
    }

    pthread_mutex_lock(&s->mu);
    /* Re-check terminal state under the lock (ep->mu-free probe: s->mu is
     * held and the hook order is ep->mu -> s->mu). */
    if (s->fatal || sender_ep_closed(s)) {
        pthread_mutex_unlock(&s->mu);
        if (mtl) track_free(s, mtl);
        if (tl) track_free(s, tl);
        track_free(s, t);
        return MOQ_ERR_CLOSED;
    }
    /* Post-ready same-name reuse: if a removed track with this name is still
     * tearing down, the replacement must wait so the removal generation is
     * published first (and its publisher track is freed before the new one
     * registers). Retryable -- the caller re-adds once teardown completes. */
    if (s->ready &&
        (removed_samename_pending(s, t->name) ||
         (tl && removed_samename_pending(s, tl->name)) ||
         (mtl && removed_samename_pending(s, mtl->name)))) {
        pthread_mutex_unlock(&s->mu);
        if (mtl) track_free(s, mtl);
        if (tl) track_free(s, tl);
        track_free(s, t);
        return MOQ_ERR_WOULD_BLOCK;
    }
    /* Catalog coherence: no duplicate track names. This rejects both an app
     * track whose name collides with an existing track (including a previously
     * generated "<x>.sap" / "<x>.timeline") and a generated timeline name that
     * collides with an existing app track. */
    if (sender_name_taken(s, t->name) ||
        (tl && sender_name_taken(s, tl->name)) ||
        (mtl && sender_name_taken(s, mtl->name))) {
        pthread_mutex_unlock(&s->mu);
        if (mtl) track_free(s, mtl);
        if (tl) track_free(s, tl);
        track_free(s, t);
        return MOQ_ERR_INVAL;
    }
    /* An AUTO sender already resolved to draft 22 refuses it like an
     * explicit one. Before resolution it is accepted; resolving to draft 22
     * with it still registered blocks the companion (companion_blocked). */
    if (cmaf_needs_init && sender_msf01_catalog(s)) {
        pthread_mutex_unlock(&s->mu);
        if (mtl) track_free(s, mtl);
        if (tl) track_free(s, tl);
        track_free(s, t);
        return MOQ_ERR_INVAL;
    }
    /* A post-ready add needs a catalog generation to advertise it. */
    if (!sender_catalog_group_available(s)) {
        pthread_mutex_unlock(&s->mu);
        if (mtl) track_free(s, mtl);
        if (tl) track_free(s, tl);
        track_free(s, t);
        return MOQ_ERR_LIMIT;
    }
    if (!sender_assign_init_ref(s, t) || !sender_tracks_push(s, t)) {
        pthread_mutex_unlock(&s->mu);
        if (mtl) track_free(s, mtl);
        if (tl) track_free(s, tl);
        track_free(s, t);
        return MOQ_ERR_NOMEM;
    }
    if (tl && !sender_tracks_push(s, tl)) {
        /* Roll the media track back out so nothing is half-registered. */
        s->track_count--;
        pthread_mutex_unlock(&s->mu);
        if (mtl) track_free(s, mtl);
        track_free(s, tl);
        track_free(s, t);
        return MOQ_ERR_NOMEM;
    }
    if (mtl && !sender_tracks_push(s, mtl)) {
        /* Roll back the media track (and the SAP sibling if pushed). */
        s->track_count -= tl ? 2 : 1;
        pthread_mutex_unlock(&s->mu);
        track_free(s, mtl);
        if (tl) track_free(s, tl);
        track_free(s, t);
        return MOQ_ERR_NOMEM;
    }
    /* Post-ready: a new track requires a fresh independent catalog generation;
     * the hook coalesces concurrent mutations into one republish. (Pre-ready
     * the initial catalog has not been built yet, so no dirty flag is needed.) */
    bool wake = s->ready;
    if (s->ready) s->catalog_dirty = true;
    pthread_mutex_unlock(&s->mu);
    if (wake && s->ep) moq_endpoint_wake(s->ep);
    *out = t;
    return MOQ_OK;
}

/* What the queue needs to admit one media object. */
typedef struct sender_admit {
    moq_rcbuf_t *payload;            /* transferred on MOQ_OK */
    moq_rcbuf_t *properties;         /* transferred on MOQ_OK; may be NULL */
    bool         is_sync;            /* holds a sync point */
    bool         anchors_group;      /* opens a Group as its anchor without
                                        one (a TS Group forwarded before the
                                        first random access point) */
    bool         starts_group;
    bool         ends_group;
    uint64_t     pts_us;
    bool         has_capture_time;
    uint64_t     capture_time_us;
    bool         has_sap_type;
    uint8_t      sap_type;
    /* Draft-22 companion RAW: the object whose metadata generates the LOC
     * property block, after the anchor checks and before any room is made.
     * properties is then NULL. */
    const moq_media_send_object_t *metadata_src;
} sender_admit_t;

/* Admit one media object of `track` into the send queue: the sync-anchor
 * rule, the backpressure policy, the Group id, and the enqueue. Called and
 * returns with s->mu held; a BLOCK_TIMEOUT wait releases it while waiting.
 * On MOQ_OK the queue owns the object's refs; on any other result nothing
 * was taken. The caller wakes the endpoint after unlocking when ready. */
static moq_result_t sender_admit_locked(moq_media_sender_t *s,
                                        moq_media_track_t *track,
                                        const sender_admit_t *a)
{
    /* The queue takes ownership of both refs, so both count toward the
     * byte budget. */
    size_t need = moq_rcbuf_len(a->payload);
    size_t property_bytes = moq_rcbuf_len(a->properties);
    if (property_bytes > SIZE_MAX - need) {
        s->stats.last_error = MOQ_ERR_INVAL;
        return MOQ_ERR_INVAL;
    }
    need += property_bytes;

    /* Would this object open a new group? (Pure -- no mutation until the
     * enqueue is committed.) A sync point that opens a group is a stream
     * anchor: every group must begin with one. */
    bool opened = a->starts_group || !track->group_open;
    bool anchor = a->is_sync || (a->anchors_group && a->starts_group);
    bool incoming_anchors = anchor && opened;

    /* Sync-anchor invariant (§7.2), PER TRACK, against EMITTED-or-queued
     * anchor state (not just the queue): a group opened by a sync point
     * stays anchored even after that keyframe has drained off the ring, so
     * later deltas of a still-open group are accepted. A new group must be
     * opened by a sync point; a delta may only continue an already-anchored
     * group. Otherwise the object would be a non-decodable lead -- refuse,
     * taking no ownership. */
    if (opened) {
        if (!anchor) {
            s->stats.last_error = MOQ_ERR_WOULD_BLOCK;
            return MOQ_ERR_WOULD_BLOCK;   /* a group cannot lead with a delta */
        }
        /* The new group's ID must exist: after MOQ_QUIC_VARINT_MAX the track
         * has no further group (never a wrap or a reused ID). Refused here,
         * before anything is evicted or owned. */
        if (track->group_started && track->group_seq >= MOQ_QUIC_VARINT_MAX) {
            s->stats.last_error = MOQ_ERR_LIMIT;
            return MOQ_ERR_LIMIT;
        }
    } else if (!track->cur_group_anchored) {
        s->stats.last_error = MOQ_ERR_WOULD_BLOCK;
        return MOQ_ERR_WOULD_BLOCK;
    }

    moq_rcbuf_t *generated = NULL;
    if (a->metadata_src) {
        moq_result_t rc = sender_encode_metadata(s, a->metadata_src, &generated);
        if (rc != MOQ_OK) {
            s->stats.last_error = rc;
            return rc;
        }
        size_t n = moq_rcbuf_len(generated);
        if (n > SIZE_MAX - need) {
            moq_rcbuf_decref(generated);
            s->stats.last_error = MOQ_ERR_INVAL;
            return MOQ_ERR_INVAL;
        }
        need += n;
    }

    /* Make room per the backpressure policy. Drop policies evict (and may
     * arm a wire reset); RETURN_WOULD_BLOCK refuses immediately;
     * BLOCK_TIMEOUT waits for the drain to free space, breaking promptly on
     * terminal/interrupt and surfacing WOULD_BLOCK on timeout -- never a
     * silent drop. */
    if (!preq_make_room(s, track, incoming_anchors, need)) {
        if (s->backpressure != MOQ_MEDIA_SEND_BP_BLOCK_TIMEOUT) {
            moq_rcbuf_decref(generated);
            s->stats.last_error = MOQ_ERR_WOULD_BLOCK;
            return MOQ_ERR_WOULD_BLOCK;
        }
        s->stats.backpressure_stalls++;
        SENDER_WAIT_ENTRY_HOOK();
        moq_result_t wres = MOQ_OK;
        struct timespec origin;
        if (SENDER_CLOCK_REALTIME(&origin) != 0) {
            /* The budget cannot be anchored: report the system failure rather
             * than waiting against an unknown origin. */
            wres = MOQ_ERR_INTERNAL;
        } else {
            const uint64_t dur_sec = s->block_timeout_us / 1000000ull;
            const long dur_nsec =
                (long)((s->block_timeout_us % 1000000ull) * 1000ull);
            for (;;) {
                if (preq_fits(s, need)) { wres = MOQ_OK; break; }
                if (s->fatal) { wres = MOQ_ERR_CLOSED; break; }
                /* ep->mu-free closed check (sender_ep_closed): this loop holds s->mu,
                 * and the endpoint hook lock order is ep->mu -> s->mu, so calling the
                 * public moq_endpoint_is_closed() (which takes ep->mu) here would
                 * invert it and can deadlock the network thread. */
                if (sender_ep_closed(s)) { wres = MOQ_ERR_CLOSED; break; }
                if (moq_endpoint_interrupted_internal(s->ep)) {
                    wres = MOQ_ERR_INTERRUPTED; break;
                }
                struct timespec now;
                if (SENDER_CLOCK_REALTIME(&now) != 0) {
                    wres = MOQ_ERR_INTERNAL; break;
                }
                long rem_ns = 0;
                sender_budget_t cls = sender_budget(origin, dur_sec, dur_nsec,
                                                    now, &rem_ns);
                if (cls == SENDER_BUDGET_EXPIRED) {
                    /* The capacity check at the top of this iteration already
                     * ran under this same lock hold, so it cannot have changed
                     * since: the budget is spent with no room. */
                    wres = MOQ_ERR_WOULD_BLOCK;
                    break;
                }
                /* Bounded slice so external terminal/interrupt (which do not
                 * signal space_cv) are observed promptly, not at full timeout;
                 * a shorter remainder is waited exactly, to the nanosecond. */
                struct timespec until = now;
                until.tv_nsec += (cls == SENDER_BUDGET_AT_LEAST_SLICE)
                                 ? SENDER_SLICE_NS : rem_ns;
                if (until.tv_nsec >= SENDER_NS_PER_S) {
                    if (until.tv_sec >= SENDER_TIME_T_MAX) {
                        /* The next required instant is not representable. A
                         * maximum second is legal; going past it is not, and
                         * clamping would either expire the wait early or make
                         * no progress. */
                        wres = MOQ_ERR_INTERNAL; break;
                    }
                    until.tv_sec++;
                    until.tv_nsec -= SENDER_NS_PER_S;
                }
                int wrc = SENDER_COND_TIMEDWAIT(&s->space_cv, &s->mu, &until);
                if (wrc != 0 && wrc != ETIMEDOUT) {
                    wres = MOQ_ERR_INTERNAL; break;
                }
                /* A wake or a slice timeout re-enters the loop, where capacity,
                 * terminal state, the interrupt latch and then expiry are all
                 * re-evaluated in that order. */
            }
        }
        if (wres != MOQ_OK) {
            moq_rcbuf_decref(generated);
            s->stats.last_error = wres;
            return wres;   /* no ownership taken */
        }
        /* The wait released s->mu. The track may have been ended or removed
         * meanwhile, or its open Group closed by a drop; the object is then
         * refused as it would be now, and the caller retries. */
        if (track->end_requested || track->removed || track->vod_converted) {
            moq_rcbuf_decref(generated);
            s->stats.last_error = MOQ_ERR_WRONG_STATE;
            return MOQ_ERR_WRONG_STATE;
        }
        if ((a->starts_group || !track->group_open) != opened) {
            moq_rcbuf_decref(generated);
            s->stats.last_error = MOQ_ERR_WOULD_BLOCK;
            return MOQ_ERR_WOULD_BLOCK;
        }
    }

    /* Commit the service group id: a new group bumps the seq (the first
     * group ever is the instance's group base, sender_group_base). group_open
     * tracks completion separately so an ends_group does not make the next
     * group reuse the same id.
     * cur_group_anchored records that this group was opened by a sync
     * point, so a later delta is accepted even after the keyframe drains. */
    if (opened) {
        track->group_seq = track->group_started ? track->group_seq + 1
                                                : sender_group_base(s);
        track->group_started = true;
        track->group_open = true;
        track->cur_group_anchored = anchor;
    }

    sender_preq_entry_t *e = &s->preq[s->preq_tail % s->ring_cap];
    memset(e, 0, sizeof(*e));
    e->track = track;
    e->payload = a->payload;            /* ownership transfers */
    e->properties = generated ? generated : a->properties;
    e->is_sync = anchor;
    e->keyframe = a->is_sync;
    e->starts_group = opened;
    e->ends_group = a->ends_group;
    e->group_seq = track->group_seq;
    e->pts_us = a->pts_us;
    e->has_capture_time = a->has_capture_time;
    e->capture_time_us = a->capture_time_us;
    e->bytes = need;
    /* Carry the SAP declaration so the timeline record is generated at emit
     * time with the real wire Location (a dropped Object then yields no record). */
    if (track->sap_timeline && a->has_sap_type) {
        e->has_sap_type = true;
        e->sap_type = a->sap_type;
    }
    s->preq_bytes += need;
    s->preq_tail++;
    preq_note_enqueued(s, e);
    s->stats.objects_written++;
    track->tstats.objects_written++;
    track->tstats.objects_queued++;
    track->tstats.bytes_queued += need;
    if (a->ends_group) track->group_open = false;
    return MOQ_OK;
}


/* -- Write (app thread) ----------------------------------------------- */

moq_result_t moq_media_sender_write(moq_media_sender_t *s,
                                    moq_media_track_t *track,
                                    const moq_media_send_object_t *obj)
{
    /* Only obj fields are touched here; the opaque track (shared with the
     * receiver API) is NOT dereferenced until proven sender-owned below. */
    if (!s || !track || !obj) return MOQ_ERR_INVAL;
    if (obj->struct_size < MOQ_MEDIA_SEND_OBJECT_V0_SIZE)
        return MOQ_ERR_INVAL;
    if (!obj->payload) return MOQ_ERR_INVAL;
    /* A declared SAP type must be concrete (CMSF §3.6.1: 0/1/2/3); the internal
     * UNKNOWN sentinel is not a serializable/declarable value. */
    if (obj->has_sap_type &&
        obj->sap_type != MOQ_SAP_NONE && obj->sap_type != MOQ_SAP_TYPE_1 &&
        obj->sap_type != MOQ_SAP_TYPE_2 && obj->sap_type != MOQ_SAP_TYPE_3)
        return MOQ_ERR_INVAL;

    if (s->ep && moq_endpoint_interrupted_internal(s->ep))
        return MOQ_ERR_INTERRUPTED;
    if (sender_terminal(s)) return MOQ_ERR_CLOSED;

    pthread_mutex_lock(&s->mu);
    if (s->fatal) { pthread_mutex_unlock(&s->mu); return MOQ_ERR_CLOSED; }

    /* Verify the track belongs to this sender BEFORE touching any of its
     * fields -- a foreign / receiver / stale handle must not be
     * dereferenced. (The private catalog track is not in s->tracks, so it
     * is rejected here too.) */
    bool owned = false;
    for (size_t i = 0; i < s->track_count; i++)
        if (s->tracks[i] == track) { owned = true; break; }
    if (!owned) { pthread_mutex_unlock(&s->mu); return MOQ_ERR_INVAL; }

    /* A generated timeline track (SAP or media) is service-owned: the app never
     * writes to it (its objects are synthesized from the media track). */
    if (track_is_generated_timeline(track)) {
        pthread_mutex_unlock(&s->mu);
        return MOQ_ERR_INVAL;
    }

    /* No writes after the track has been ended, removed from the catalog, or
     * converted to VOD (V4: isLive=false means no new content). */
    if (track->end_requested || track->removed || track->vod_converted) {
        pthread_mutex_unlock(&s->mu);
        return MOQ_ERR_WRONG_STATE;
    }

    if (sender_companion_refused_locked(s)) {
        s->stats.last_error = MOQ_ERR_UNSUPPORTED;
        pthread_mutex_unlock(&s->mu);
        return MOQ_ERR_UNSUPPORTED;
    }

    /* The legacy LOC-01 Capture Timestamp on drafts16/18 is emitted by
     * moq_loc_encode, but ONLY
     * for RAW packaging (CMAF passes the app's property block through
     * untouched, so its timestamp fields are never encoded here). A value the
     * negotiated draft's codec cannot represent cannot be encoded; left to the
     * drain path it would fatal the sender AFTER the object was queued and its
     * payload ownership taken. Reject it synchronously -- before any enqueue /
     * ownership transfer. The encoded value is capture_time_us when
     * has_capture_time, else the presentation_time_us fallback
     * (sender_build_props), so validate whichever will be encoded, against
     * sender_loc_ts_max (the encoder's own bound for the latched draft). */
    /* An unresolved AUTO companion may still become legacy, so its RAW
     * timestamps are held to the same bound. */
    if (track->packaging == MOQ_MEDIA_PACKAGING_RAW &&
        s->companion_profile != MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC) {
        uint64_t loc_ts = obj->has_capture_time ? obj->capture_time_us
                                                : obj->presentation_time_us;
        if (loc_ts > sender_loc_ts_max(s->transport_version)) {
            s->stats.last_error = MOQ_ERR_INVAL;
            pthread_mutex_unlock(&s->mu);
            return MOQ_ERR_INVAL;
        }
    }

    /* CMAF validation (CMSF §3.3/§3.4), STRICT BY DEFAULT (cfg_init sets
     * validate_cmaf = true; set it false for explicit passthrough). When
     * the track has a parsed CMAF init, the object track_ID is matched against
     * it. Without an app-declared SAP type, a group-start object MUST start with
     * a SYNC sample (§3.4): a non-sync first sample is rejected even when its
     * sap_type is UNKNOWN (an open-GOP independent sample could only be SAP
     * type 3, which is not allowed at a group start). An explicit declaration of
     * TYPE_1/2 overrides this. No ownership is taken on rejection. */
    if (s->validate_cmaf && track->packaging == MOQ_MEDIA_PACKAGING_CMAF) {
        moq_bytes_t obj_bytes = {
            moq_rcbuf_data(obj->payload), moq_rcbuf_len(obj->payload) };
        moq_cmaf_init_info_t vinit;
        moq_cmaf_init_info_init(&vinit);
        const moq_cmaf_init_info_t *initp = NULL;
        if (track->has_cmaf_init_track_id) {
            vinit.track_id = track->cmaf_init_track_id;
            initp = &vinit;
        }
        moq_cmaf_object_report_t rep;
        bool ok = (moq_cmaf_validate_object(initp, obj_bytes, &rep) == MOQ_OK);
        if (ok && obj->starts_group) {
            if (obj->has_sap_type) {
                /* §3.4: with a declared SAP type, a group MUST start with type
                 * 1 or 2 (the app declares the exact type; we never infer it). */
                if (obj->sap_type != MOQ_SAP_TYPE_1 &&
                    obj->sap_type != MOQ_SAP_TYPE_2)
                    ok = false;
            } else if (!rep.starts_with_sync) {
                /* No declaration: §3.4 requires a group to start with a SAP of
                 * type 1 or 2. Only a SYNC sample qualifies without an explicit
                 * declaration (its exact type is UNKNOWN but necessarily 1/2).
                 * A known non-SAP (P/B frame, sap_type NONE) AND an open-GOP
                 * non-sync independent sample (sap_type UNKNOWN, which could
                 * only be SAP type 3) both have starts_with_sync=false and are
                 * rejected -- the app must declare TYPE_1/2 to start a group
                 * with either. */
                ok = false;
            }
        }
        if (!ok) {
            s->stats.last_error = MOQ_ERR_PROTO;
            pthread_mutex_unlock(&s->mu);
            return MOQ_ERR_PROTO;
        }
    }

    /* MPEG2TS: write() is the app-Object mode; a byte-stream track takes
     * write_mpeg2ts() only. Each Object is whole source packets with their
     * sync bytes (Object Payload Format), and with mpeg2tsRandomAccess
     * declared, a Group's first Object holds a random access point (Random
     * Access). No ownership is taken on rejection. */
    if (track->packaging == MOQ_MEDIA_PACKAGING_MPEG2TS) {
        if (!track->ts_app_objects) {
            pthread_mutex_unlock(&s->mu);
            return MOQ_ERR_WRONG_STATE;
        }
        bool ok = true;
#ifdef MOQ_SERVICE_HAVE_MPEG2TS
        const moq_msf_mpeg2ts_t *rec = sender_ts_record(track);
        moq_bytes_t pl = { moq_rcbuf_data(obj->payload),
                           moq_rcbuf_len(obj->payload) };
        moq_mpeg2ts_object_rules_t rules;
        moq_mpeg2ts_object_rules_init(&rules);
        rules.packet_size = rec->packet_size;
        moq_mpeg2ts_object_report_t rep;
        moq_mpeg2ts_object_report_init(&rep);
        ok = moq_mpeg2ts_validate_object(&rules, pl, &rep, sizeof(rep)) == MOQ_OK;
        if (ok && track->ts_random_access &&
            (obj->starts_group || !track->group_open)) {
            moq_mpeg2ts_inspect_cfg_t ip;
            moq_mpeg2ts_inspect_cfg_init(&ip);
            ip.packet_size = rec->packet_size;
            ip.random_access_pid = MOQ_MPEG2TS_PID_AUTO;
            if (rec->has_program_number)
                ip.program_number = (uint16_t)rec->program_number;
            moq_mpeg2ts_object_info_t info;
            moq_mpeg2ts_object_info_init(&info);
            ok = moq_mpeg2ts_inspect_object(&ip, pl, &info, sizeof(info)) == MOQ_OK &&
                 info.has_random_access;
        }
#endif
        if (!ok) {
            s->stats.last_error = MOQ_ERR_PROTO;
            pthread_mutex_unlock(&s->mu);
            return MOQ_ERR_PROTO;
        }
    }

    /* LOC ownership (§7.4): for RAW/LOC media the service GENERATES the
     * whole property block from the typed fields. App-supplied `properties`
     * cannot be carried on a RAW object in v0: the property block is a
     * delta-coded KVP sequence, so the generated LOC block and an
     * independently-encoded app block cannot simply be concatenated (the
     * app keys' deltas would decode relative to the LOC block's last key).
     * A correct ordered KVP merge is a follow-up; until then RAW extras are
     * rejected (the service owns RAW/LOC properties anyway). CMAF carries
     * the app's complete block through unchanged -- no generation, no
     * merge. */
    if (track->packaging == MOQ_MEDIA_PACKAGING_RAW && obj->properties) {
        s->stats.last_error = MOQ_ERR_INVAL;
        pthread_mutex_unlock(&s->mu);
        return MOQ_ERR_INVAL;   /* no ownership taken */
    }

    /* Would this object open a new group? (Pure.) */
    bool opened = obj->starts_group || !track->group_open;

    /* CMSF §3.6.1 / §3.4: when this track generates a SAP timeline, every
     * group-start Object MUST declare a concrete SAP type of 1 or 2 (the value
     * recorded at [group,0]). A missing declaration or NONE/UNKNOWN/TYPE_3 at a
     * group start is rejected with no ownership taken. (UNKNOWN was already
     * rejected at the top of write().) */
    if (track->sap_timeline && opened &&
        (!obj->has_sap_type ||
         (obj->sap_type != MOQ_SAP_TYPE_1 && obj->sap_type != MOQ_SAP_TYPE_2))) {
        s->stats.last_error = MOQ_ERR_INVAL;
        pthread_mutex_unlock(&s->mu);
        return MOQ_ERR_INVAL;
    }

    sender_admit_t a;
    memset(&a, 0, sizeof(a));
    a.payload = obj->payload;
    a.properties = obj->properties;
    a.is_sync = obj->is_sync;
    a.starts_group = obj->starts_group;
    a.ends_group = obj->ends_group;
    a.pts_us = obj->presentation_time_us;
    a.has_capture_time = obj->has_capture_time;
    a.capture_time_us = obj->capture_time_us;
    /* Explicit D22 RAW requires the metadata snapshot. An AUTO sender that
     * is (or may still become) draft-22 encodes one when the object carries
     * it, and otherwise generates LOC-04 from the typed fields at drain; a
     * sender resolved to legacy ignores it, as explicit legacy does. */
    if (track->packaging == MOQ_MEDIA_PACKAGING_RAW) {
        bool has_metadata =
            obj->struct_size >= offsetof(moq_media_send_object_t, metadata) +
                                sizeof(obj->metadata) && obj->metadata;
        /* A typed capture time is sent as an unscaled LOC-04 Timestamp, Unix
         * microseconds (LOC-04 2.3.1.1). A Timescale among the Track
         * Properties applies to every object without its own (2.3.1.2), so
         * the receiver would read that capture time as media time at the
         * track's scale. Refused, before anything is evicted or owned,
         * whenever the object may go out as draft 22; a track whose
         * properties do not parse as LOC-04 metadata may carry a Timescale
         * and is refused the same way. Explicit metadata and the
         * presentation-time form are unaffected. */
        if (!has_metadata && obj->has_capture_time &&
            track->track_scale != SENDER_TRACK_SCALE_NONE &&
            sender_auto_d22_possible_locked(s)) {
            s->stats.last_error = MOQ_ERR_INVAL;
            pthread_mutex_unlock(&s->mu);
            return MOQ_ERR_INVAL;
        }
        if (s->companion_auto
                ? s->companion_profile !=
                      MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01 && has_metadata
                : s->companion_profile ==
                      MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC)
            a.metadata_src = obj;
    }
    if (track->sap_timeline && obj->has_sap_type) {
        a.has_sap_type = true;
        a.sap_type = (uint8_t)obj->sap_type;
    }
    moq_result_t rc = sender_admit_locked(s, track, &a);
    bool wake = rc == MOQ_OK && s->ready;  /* post-ready: kick the drain */
    pthread_mutex_unlock(&s->mu);

    /* Wake the network thread so the drain runs without waiting for unrelated
     * traffic. Non-allocating moq_endpoint_wake() (queues no task, cannot fail);
     * called OUTSIDE s->mu. Pre-ready writes need no wake -- the readiness
     * transition itself drives the first drain. */
    if (wake)
        moq_endpoint_wake(s->ep);
    return rc;
}

/* -- MPEG-2 TS byte stream (app thread) -------------------------------- */

/* The first published size of moq_media_send_mpeg2ts_t: through
 * end_object. */
#define MEDIA_SEND_MPEG2TS_V0_SIZE \
    (offsetof(moq_media_send_mpeg2ts_t, end_object) + sizeof(bool))

#ifdef MOQ_SERVICE_HAVE_MPEG2TS
/* The track's open Group lost an Object that never reached the queue:
 * drop what is still queued of it (counted, as any drop) and, when its wire
 * subgroup is open, arm the reset that abandons it. The Group is closed for
 * writes. s->mu held. */
static void sender_abandon_open_group_locked(moq_media_sender_t *s,
                                             moq_media_track_t *t)
{
    if (!t->group_open) return;
    uint64_t g = t->group_seq;
    preq_evict_group(s, t, g);
    if (t->emit_open && t->emit_group == g) t->pending_reset = true;
    t->group_open = false;
    t->cur_group_anchored = false;
}

/* Queue the Objects of `list` (the staged or the parked ones) in order,
 * through the same admission as write(). A continuation of a Group that lost
 * media is discarded and counted, never queued: the next Group start ends
 * that. When an Object cannot be queued, it and every later one are parked,
 * in order, and the next call queues them first -- unless the sender is
 * closed or the track can take no more writes (MOQ_ERR_CLOSED,
 * MOQ_ERR_WRONG_STATE), when they are released. So a refusal that can pass
 * (WOULD_BLOCK, an interrupt, a clock failure in the wait) never leaves a
 * hole in a Group. s->mu held (a BLOCK_TIMEOUT wait releases it while
 * waiting). */
static moq_result_t sender_ts_queue(moq_media_sender_t *s,
                                    moq_media_track_t *t,
                                    sender_ts_obj_t *list, size_t *n,
                                    bool *queued_any)
{
    sender_ts_t *ts = (sender_ts_t *)t->ts;
    moq_result_t rc = MOQ_OK;
    size_t i = 0;
    for (; i < *n; i++) {
        sender_ts_obj_t *o = &list[i];
        if (o->lost) {
            /* The rest of its Group is discarded (broken). A lost
             * continuation abandons the open Group: what is still queued of
             * it is dropped and an open wire subgroup is reset, so the
             * truncated Group is never finished as if complete. A lost Group
             * start takes its Group ID with it, so the next Group follows a
             * visible hole, and leaves the Group before it, already
             * complete, alone. */
            ts->broken = true;
            if (o->new_group) {
                t->group_seq = t->group_started ? t->group_seq + 1
                                                : sender_group_base(s);
                t->group_started = true;
                t->group_open = false;
                t->cur_group_anchored = false;
            } else {
                sender_abandon_open_group_locked(s, t);
            }
            o->lost = false;
            continue;
        }
        if (o->new_group) {
            ts->broken = false;
        } else if (ts->broken || !t->group_open) {
            moq_rcbuf_decref(o->payload);
            o->payload = NULL;
            ts->continuation_discarded++;
            continue;
        }
        sender_admit_t a;
        memset(&a, 0, sizeof(a));
        a.payload = o->payload;
        a.is_sync = o->random_access;
        a.anchors_group = o->new_group; /* every Group start is the anchor */
        a.starts_group = o->new_group;
        rc = sender_admit_locked(s, t, &a);
        if (rc != MOQ_OK) break;
        o->payload = NULL;
        *queued_any = true;
    }
    size_t rest = *n - i;
    *n = 0;
    if (rc != MOQ_OK && rc != MOQ_ERR_CLOSED && rc != MOQ_ERR_WRONG_STATE) {
        /* Staged Objects are queued only once nothing is parked, so the
         * parked list is either this list or empty. */
        sender_ts_obj_t keep[SENDER_TS_STAGE_MAX];
        memcpy(keep, &list[i], rest * sizeof(*keep));
        memcpy(ts->parked, keep, rest * sizeof(*keep));
        ts->parked_n = rest;
    } else {
        sender_ts_drop_list(&list[i], rest);
    }
    return rc;
}

/* Queue what the packetizer just emitted (an Object it could not copy is a
 * local loss of its Group). */
static moq_result_t sender_ts_queue_staged(moq_media_sender_t *s,
                                           moq_media_track_t *t,
                                           bool *queued_any)
{
    sender_ts_t *ts = (sender_ts_t *)t->ts;
    moq_result_t rc = sender_ts_queue(s, t, ts->staged, &ts->staged_n,
                                      queued_any);
    if (rc == MOQ_OK && ts->emit_lost) {
        ts->emit_lost = false;
        return MOQ_ERR_NOMEM;
    }
    return rc;
}

/* Filter (per-program) and packetize one whole source packet, then queue
 * what it completed. */
static moq_result_t sender_ts_packet(moq_media_sender_t *s,
                                     moq_media_track_t *t,
                                     const uint8_t *pkt, bool *queued_any)
{
    sender_ts_t *ts = (sender_ts_t *)t->ts;
    moq_bytes_t in = { pkt, ts->ps };
    uint8_t out[MOQ_MPEG2TS_M2TS_PACKET_SIZE];
    if (ts->filter) {
        moq_mpeg2ts_filter_action_t act = MOQ_MPEG2TS_FILTER_DROP;
        /* A packet the filter cannot parse is one it does not keep. */
        if (moq_mpeg2ts_program_filter_push(ts->filter, in, out, sizeof(out),
                                            &act) != MOQ_OK ||
            act == MOQ_MPEG2TS_FILTER_DROP)
            return MOQ_OK;
        in = (moq_bytes_t){ out, ts->ps };
    }
    if (moq_mpeg2ts_packetizer_push(&ts->pz, in) != MOQ_OK)
        return MOQ_ERR_PROTO;
    return sender_ts_queue_staged(s, t, queued_any);
}

/* After an Object was queued the writer lets go of s->mu, so the drain is
 * never shut out for longer than one Object; the busy latch keeps the track
 * to this writer meanwhile. Returns non-OK when the track or sender changed
 * state in between, including a companion the draft latched meanwhile
 * refuses: no further byte is then taken. */
static moq_result_t sender_ts_yield(moq_media_sender_t *s,
                                    moq_media_track_t *t, bool *queued_any)
{
    if (!*queued_any) return MOQ_OK;
    *queued_any = false;
    bool wake = s->ready;
    pthread_mutex_unlock(&s->mu);
    if (wake) moq_endpoint_wake(s->ep);
    SENDER_TS_YIELD_HOOK();
    pthread_mutex_lock(&s->mu);
    if (s->fatal || sender_ep_closed(s)) return MOQ_ERR_CLOSED;
    if (t->removed || t->vod_converted) return MOQ_ERR_WRONG_STATE;
    if (sender_companion_refused_locked(s)) return MOQ_ERR_UNSUPPORTED;
    return MOQ_OK;
}
#endif

moq_result_t moq_media_sender_write_mpeg2ts(moq_media_sender_t *s,
                                            moq_media_track_t *track,
                                            const moq_media_send_mpeg2ts_t *in,
                                            size_t *consumed)
{
    if (consumed) *consumed = 0;
    if (!s || !track || !in || !consumed) return MOQ_ERR_INVAL;
    if (in->struct_size < MEDIA_SEND_MPEG2TS_V0_SIZE) return MOQ_ERR_INVAL;
    if (in->data.len > 0 && !in->data.data) return MOQ_ERR_INVAL;
#ifndef MOQ_SERVICE_HAVE_MPEG2TS
    (void)track;
    return MOQ_ERR_UNSUPPORTED;
#else
    if (s->ep && moq_endpoint_interrupted_internal(s->ep))
        return MOQ_ERR_INTERRUPTED;
    if (sender_terminal(s)) return MOQ_ERR_CLOSED;

    pthread_mutex_lock(&s->mu);
    if (s->fatal) { pthread_mutex_unlock(&s->mu); return MOQ_ERR_CLOSED; }
    bool owned = false;
    for (size_t i = 0; i < s->track_count; i++)
        if (s->tracks[i] == track) { owned = true; break; }
    if (!owned || track->packaging != MOQ_MEDIA_PACKAGING_MPEG2TS) {
        pthread_mutex_unlock(&s->mu);
        return MOQ_ERR_INVAL;
    }
    sender_ts_t *ts = (sender_ts_t *)track->ts;
    if (!ts || ts->busy || ts->ending || track->end_requested ||
        track->removed || track->vod_converted) {
        pthread_mutex_unlock(&s->mu);
        return MOQ_ERR_WRONG_STATE;
    }
    /* The same companion refusal as write(), before any carry, parked
     * Object, packetizer or queue state is touched: nothing is consumed. */
    if (sender_companion_refused_locked(s)) {
        s->stats.last_error = MOQ_ERR_UNSUPPORTED;
        pthread_mutex_unlock(&s->mu);
        return MOQ_ERR_UNSUPPORTED;
    }
    ts->busy = true;

    bool queued_any = false, progressed = false;
    /* What a previous call could not queue goes first. */
    moq_result_t rc = sender_ts_queue(s, track, ts->parked, &ts->parked_n,
                                      &queued_any);
    const uint8_t *data = in->data.data;
    size_t len = in->data.len, off = 0;
    const size_t ps = ts->ps;
    const size_t sync = ps == MOQ_MPEG2TS_M2TS_PACKET_SIZE
                            ? MOQ_MPEG2TS_PREFIX_SIZE : 0u;
    while (rc == MOQ_OK && off < len) {
        if (ts->carry_len > 0 || len - off < ps) {
            /* Assemble a packet that spans calls. Its sync byte is checked
             * as it arrives, before any byte of it is taken. */
            size_t take = ps - ts->carry_len;
            if (take > len - off) take = len - off;
            if (ts->carry_len <= sync && ts->carry_len + take > sync &&
                data[off + (sync - ts->carry_len)] != MOQ_MPEG2TS_SYNC_BYTE) {
                /* The bytes kept from earlier calls belong to the bad
                 * packet: discarded and counted, so the next call starts at
                 * a packet boundary. */
                ts->partial_bytes_discarded += ts->carry_len;
                ts->carry_len = 0;
                rc = MOQ_ERR_PROTO;
                break;
            }
            memcpy(ts->carry + ts->carry_len, data + off, take);
            ts->carry_len += take;
            off += take;
            *consumed = off;
            if (ts->carry_len < ps) break;
            ts->carry_len = 0;
            rc = sender_ts_packet(s, track, ts->carry, &queued_any);
        } else {
            if (data[off + sync] != MOQ_MPEG2TS_SYNC_BYTE) {
                rc = MOQ_ERR_PROTO;
                break;
            }
            const uint8_t *pkt = data + off;
            off += ps;
            *consumed = off;
            rc = sender_ts_packet(s, track, pkt, &queued_any);
        }
        if (queued_any) progressed = true;
        if (rc == MOQ_OK) rc = sender_ts_yield(s, track, &queued_any);
    }
    /* end_object: this call's last packet closes an Object. */
    if (rc == MOQ_OK && in->end_object) {
        progressed = true;
        if (moq_mpeg2ts_packetizer_end_object(&ts->pz) != MOQ_OK)
            rc = MOQ_ERR_INTERNAL;
        else
            rc = sender_ts_queue_staged(s, track, &queued_any);
    }
    /* A short write: Objects held back after any progress are reported by
     * *consumed and the parked count, not by an error. */
    if (queued_any || *consumed > 0) progressed = true;
    if (rc == MOQ_ERR_WOULD_BLOCK && progressed) rc = MOQ_OK;
    if (rc != MOQ_OK) s->stats.last_error = rc;
    ts->busy = false;
    bool wake = queued_any && s->ready;
    pthread_mutex_unlock(&s->mu);
    if (wake) moq_endpoint_wake(s->ep);
    return rc;
#endif
}

moq_result_t moq_media_sender_end_track(moq_media_sender_t *s,
                                        moq_media_track_t *track)
{
    if (!s || !track) return MOQ_ERR_INVAL;
    if (moq_endpoint_interrupted_internal(s->ep)) return MOQ_ERR_INTERRUPTED;
    if (sender_terminal(s)) return MOQ_ERR_CLOSED;

    pthread_mutex_lock(&s->mu);
    if (s->fatal) { pthread_mutex_unlock(&s->mu); return MOQ_ERR_CLOSED; }

    bool owned = false;
    for (size_t i = 0; i < s->track_count; i++)
        if (s->tracks[i] == track) { owned = true; break; }
    if (!owned) { pthread_mutex_unlock(&s->mu); return MOQ_ERR_INVAL; }

    /* A removed track is terminal: it is being torn down (END_OF_TRACK + pub
     * removal) by the hook, so end_track is refused rather than queuing a
     * terminal marker the drain would drop. */
    if (track->removed) { pthread_mutex_unlock(&s->mu); return MOQ_ERR_WRONG_STATE; }

    /* Idempotent: a second end_track is a no-op success (the terminal is
     * already queued/sent). */
    if (track->end_requested) { pthread_mutex_unlock(&s->mu); return MOQ_OK; }

#ifdef MOQ_SERVICE_HAVE_MPEG2TS
    /* A byte-stream TS track ends its source first: the Objects held back,
     * then the packetizer's last ones, each through the normal admission. A
     * WOULD_BLOCK on the way is retried by the next end_track. */
    if (track->ts) {
        sender_ts_t *ts = (sender_ts_t *)track->ts;
        if (ts->busy) { pthread_mutex_unlock(&s->mu); return MOQ_ERR_WRONG_STATE; }
        ts->busy = true;
        bool queued_any = false;
        moq_result_t rc = sender_ts_queue(s, track, ts->parked, &ts->parked_n,
                                          &queued_any);
        if (rc == MOQ_OK && !ts->ending) {
            ts->ending = true;
            if (moq_mpeg2ts_packetizer_flush(&ts->pz) != MOQ_OK)
                rc = MOQ_ERR_INTERNAL;
            else
                rc = sender_ts_queue_staged(s, track, &queued_any);
        }
        ts->busy = false;
        /* MOQ_ERR_NOMEM (a last Object lost) is reported like any other
         * failure: the track is not ended yet, and the next end_track
         * queues the END_OF_TRACK. */
        if (rc != MOQ_OK) {
            bool wake = queued_any && s->ready;
            pthread_mutex_unlock(&s->mu);
            if (wake) moq_endpoint_wake(s->ep);
            return rc;
        }
        /* A partial packet is not a packet: discarded and counted. */
        ts->partial_bytes_discarded += ts->carry_len;
        ts->carry_len = 0;
    }
#endif

    /* The terminal marker needs one slot against the ACTIVE queue OBJECT bound
     * -- the same bound write() is held to (preq_fits) -- not against the
     * larger physical allocation, which is an implementation detail. It is a
     * zero-byte entry, so the byte budget does not apply. Nothing is latched
     * on refusal: end_requested stays clear and the caller retries. */
    if ((s->preq_tail - s->preq_head) >= active_obj_cap(s)) {
        pthread_mutex_unlock(&s->mu);
        return MOQ_ERR_WOULD_BLOCK;   /* queue full; retry after it drains */
    }

    sender_preq_entry_t *e = &s->preq[s->preq_tail % s->ring_cap];
    memset(e, 0, sizeof(*e));
    e->track = track;
    e->is_end = true;
    e->group_seq = track->group_seq;
    s->preq_tail++;
    preq_note_enqueued(s, e);   /* a marker is not queued media */

    track->end_requested = true;
    track->group_open = false;   /* no further objects open a group */
    bool wake = s->ready;
    pthread_mutex_unlock(&s->mu);

    if (wake)
        moq_endpoint_wake(s->ep);
    return MOQ_OK;
}

moq_result_t moq_media_sender_remove_track(moq_media_sender_t *s,
                                           moq_media_track_t *track)
{
    if (!s || !track) return MOQ_ERR_INVAL;
    if (s->ep && moq_endpoint_interrupted_internal(s->ep)) return MOQ_ERR_INTERRUPTED;
    if (sender_terminal(s)) return MOQ_ERR_CLOSED;

    pthread_mutex_lock(&s->mu);
    if (s->fatal) { pthread_mutex_unlock(&s->mu); return MOQ_ERR_CLOSED; }

    bool owned = false;
    for (size_t i = 0; i < s->track_count; i++)
        if (s->tracks[i] == track) { owned = true; break; }
    if (!owned) { pthread_mutex_unlock(&s->mu); return MOQ_ERR_INVAL; }

    /* A generated/internal track (SAP or media timeline) is not user-removable:
     * it is coupled to its media track and removed with it. */
    if (track_is_generated_timeline(track)) {
        pthread_mutex_unlock(&s->mu);
        return MOQ_ERR_INVAL;
    }
    /* Removing an already-removed handle is a terminal-state error (the handle
     * stays valid but inert until sender destroy). */
    if (track->removed) {
        pthread_mutex_unlock(&s->mu);
        return MOQ_ERR_WRONG_STATE;
    }
    if (!sender_catalog_group_available(s)) {
        pthread_mutex_unlock(&s->mu);
        return MOQ_ERR_LIMIT;   /* no Group ID left for the removal catalog */
    }

    track->removed = true;
    if (track->sap_timeline) track->sap_timeline->removed = true;  /* couple */
    if (track->media_timeline) track->media_timeline->removed = true;  /* couple */
    /* Pre-ready the initial catalog has not been built, so removal just keeps
     * the track out of it (no republish needed). Post-ready, mark dirty. */
    bool wake = s->ready;
    if (s->ready) s->catalog_dirty = true;
    pthread_mutex_unlock(&s->mu);

    if (wake && s->ep)
        moq_endpoint_wake(s->ep);
    return MOQ_OK;
}

moq_result_t moq_media_sender_complete(moq_media_sender_t *s)
{
    if (!s) return MOQ_ERR_INVAL;
    if (s->ep && moq_endpoint_interrupted_internal(s->ep))
        return MOQ_ERR_INTERRUPTED;
    if (sender_terminal(s)) return MOQ_ERR_CLOSED;

    pthread_mutex_lock(&s->mu);
    if (s->fatal) { pthread_mutex_unlock(&s->mu); return MOQ_ERR_CLOSED; }
    /* Idempotent: a second complete() is a no-op success (§11.3 is terminal). */
    if (s->completing) { pthread_mutex_unlock(&s->mu); return MOQ_OK; }
    /* Completion is meaningful only once the catalog is established. */
    if (!s->ready) { pthread_mutex_unlock(&s->mu); return MOQ_ERR_WRONG_STATE; }
    if (!sender_catalog_group_available(s)) {
        pthread_mutex_unlock(&s->mu);
        return MOQ_ERR_LIMIT;   /* no Group ID left for the terminal catalog */
    }

    /* MSF §11.3 permanent termination: mark every user + generated track removed
     * (the hook ends their publisher tracks reliably, then removes them), and
     * request the terminal catalog generation. completing makes the next build
     * emit isComplete:true with an empty tracks array; once set, add/write/
     * remove/end are refused (removed tracks reject writes/end/remove, and
     * add_track checks completing). The endpoint/namespace stay up so the retained
     * terminal catalog remains retrievable until destroy. */
    s->completing = true;
    for (size_t i = 0; i < s->track_count; i++)
        s->tracks[i]->removed = true;   /* media + generated timeline tracks */
    s->catalog_dirty = true;
    pthread_mutex_unlock(&s->mu);

    moq_endpoint_wake(s->ep);
    return MOQ_OK;
}

moq_result_t moq_media_sender_convert_to_vod(moq_media_sender_t *s,
                                             const moq_media_vod_track_t *items,
                                             size_t count)
{
    if (!s || !items || count == 0) return MOQ_ERR_INVAL;
    if (s->ep && moq_endpoint_interrupted_internal(s->ep))
        return MOQ_ERR_INTERRUPTED;
    if (sender_terminal(s)) return MOQ_ERR_CLOSED;

    pthread_mutex_lock(&s->mu);
    if (s->fatal) { pthread_mutex_unlock(&s->mu); return MOQ_ERR_CLOSED; }
    if (s->completing || !s->ready) {
        pthread_mutex_unlock(&s->mu);
        return MOQ_ERR_WRONG_STATE;     /* post-ready, pre-completion only */
    }
    /* Validate the WHOLE batch first so the conversion is atomic -- a single bad
     * item converts nothing. Reject duplicates within the batch too. */
    for (size_t i = 0; i < count; i++) {
        moq_media_track_t *t = items[i].track;
        if (!t) { pthread_mutex_unlock(&s->mu); return MOQ_ERR_INVAL; }
        bool owned = false;
        for (size_t j = 0; j < s->track_count; j++)
            if (s->tracks[j] == t) { owned = true; break; }
        if (!owned || t->is_catalog || track_is_generated_timeline(t) ||
            t->removed || t->end_requested ||
            !t->is_live || t->has_track_duration) {
            pthread_mutex_unlock(&s->mu);
            return MOQ_ERR_INVAL;       /* foreign/internal/removed/ended/not-
                                         * live/already-converted */
        }
        for (size_t j = 0; j < i; j++)
            if (items[j].track == t) {
                pthread_mutex_unlock(&s->mu);
                return MOQ_ERR_INVAL;   /* duplicate in the batch */
            }
    }
    if (!sender_catalog_group_available(s)) {
        pthread_mutex_unlock(&s->mu);
        return MOQ_ERR_LIMIT;   /* no Group ID left for the conversion catalog */
    }
    /* Apply: flip each to VOD (isLive false + trackDuration), arm the §11.3
     * step-1 subscriber finish, and dirty the catalog. The metadata change is an
     * in-place mutation of a surviving tuple, which a deltaUpdate's add/remove
     * ops cannot express; catalog_meta_dirty forces the next generation carrying
     * it to a full INDEPENDENT catalog even when coalesced with an add/remove
     * (which the receiver accepts as a live->VOD conversion). The network pump
     * sends Track Ended to each track's active subscribers before that catalog
     * publishes (vod_finish_pending gates it). */
    for (size_t i = 0; i < count; i++) {
        moq_media_track_t *t = items[i].track;
        t->is_live = false;
        t->has_track_duration = true;
        t->track_duration_ms = items[i].duration_ms;
        t->vod_converted = true;
        t->vod_finish_pending = true;
        t->catalog_meta_dirty = true;
    }
    s->catalog_dirty = true;
    pthread_mutex_unlock(&s->mu);

    moq_endpoint_wake(s->ep);
    return MOQ_OK;
}

moq_result_t moq_media_sender_get_stats(const moq_media_sender_t *s,
                                        moq_media_sender_stats_t *out,
                                        size_t out_size)
{
    if (!s || !out || out_size < MEDIA_SENDER_STATS_V0_SIZE)
        return MOQ_ERR_INVAL;
    moq_media_sender_t *ms = (moq_media_sender_t *)(uintptr_t)s;
    /* Snapshot into a full local, then copy the clamped prefix to the caller
     * (frozen v0 floor + min(caller, current) copy; same contract as the
     * receiver outputs). */
    moq_media_sender_stats_t snap;
    pthread_mutex_lock(&ms->mu);
    snap = ms->stats;
    /* MEDIA accepted by write() and still queued -- the same population as
     * objects_written / objects_sent / objects_dropped / bytes_queued. A
     * private zero-byte END_OF_TRACK work item occupies a physical ring slot
     * but is NOT part of that population, so the physical depth
     * (preq_tail - preq_head, which capacity checks keep using) is not the
     * public answer. O(1): maintained at the enqueue/release chokepoints. */
    snap.objects_queued = ms->preq_media;
    snap.bytes_queued = ms->preq_bytes;
    pthread_mutex_unlock(&ms->mu);
    size_t n = out_size < sizeof(*out) ? out_size : sizeof(*out);
    snap.struct_size = (uint32_t)n;
    memcpy(out, &snap, n);
    return MOQ_OK;
}

/* The first published size of moq_media_sender_track_stats_t: the whole
 * struct as first released, through mpeg2ts_continuation_objects_discarded. */
#define MEDIA_SENDER_TRACK_STATS_V0_SIZE \
    (offsetof(moq_media_sender_track_stats_t, \
              mpeg2ts_continuation_objects_discarded) + sizeof(uint64_t))

moq_result_t moq_media_sender_get_track_stats(
    const moq_media_sender_t *s, const moq_media_track_t *track,
    moq_media_sender_track_stats_t *out, size_t out_size)
{
    if (!s || !track || !out || out_size < MEDIA_SENDER_TRACK_STATS_V0_SIZE)
        return MOQ_ERR_INVAL;
    moq_media_sender_t *ms = (moq_media_sender_t *)(uintptr_t)s;
    moq_media_sender_track_stats_t snap;
    pthread_mutex_lock(&ms->mu);
    /* Only a handle of this sender is dereferenced. */
    moq_media_track_t *t = NULL;
    for (size_t i = 0; i < ms->track_count; i++)
        if (ms->tracks[i] == track) { t = ms->tracks[i]; break; }
    if (!t) {
        pthread_mutex_unlock(&ms->mu);
        return MOQ_ERR_INVAL;
    }
    snap = t->tstats;
#ifdef MOQ_SERVICE_HAVE_MPEG2TS
    if (t->ts) {
        const sender_ts_t *ts = (const sender_ts_t *)t->ts;
        moq_mpeg2ts_packetizer_stats_t ps;
        if (moq_mpeg2ts_packetizer_get_stats(&ts->pz, &ps, sizeof(ps))
                == MOQ_OK) {
            snap.mpeg2ts_groups = ps.groups;
            snap.mpeg2ts_objects = ps.objects;
            snap.mpeg2ts_packets_in = ps.packets_in;
            snap.mpeg2ts_packets_dropped_before_random_access =
                ps.packets_dropped_before_random_access;
            snap.mpeg2ts_max_group_27mhz = ps.max_group_27mhz;
            snap.mpeg2ts_groups_over_2s = ps.groups_over_2s;
            snap.mpeg2ts_peak_rate_bps = ps.peak_rate_bps;
            snap.mpeg2ts_source_pcr_discontinuities =
                ps.source_pcr_discontinuities;
        }
        const moq_msf_mpeg2ts_t *rec = sender_ts_record(t);
        snap.mpeg2ts_mux_rate_below_peak =
            rec->has_mux_rate && rec->mux_rate < snap.mpeg2ts_peak_rate_bps;
        snap.mpeg2ts_program_gone =
            ts->filter && moq_mpeg2ts_program_filter_program_gone(ts->filter);
        snap.mpeg2ts_objects_parked = ts->parked_n;
        snap.mpeg2ts_partial_bytes_discarded = ts->partial_bytes_discarded;
        snap.mpeg2ts_continuation_objects_discarded = ts->continuation_discarded;
    }
#endif
    pthread_mutex_unlock(&ms->mu);
    size_t n = out_size < sizeof(*out) ? out_size : sizeof(*out);
    snap.struct_size = (uint32_t)n;
    memcpy(out, &snap, n);
    return MOQ_OK;
}

/* -- State accessors -------------------------------------------------- */

bool moq_media_sender_is_ready(const moq_media_sender_t *s)
{
    if (!s) return false;
    moq_media_sender_t *ms = (moq_media_sender_t *)(uintptr_t)s;
    pthread_mutex_lock(&ms->mu);
    /* A fatal sender is terminal, never ready: a rejected/cancelled namespace
     * is no longer announced, so readiness cannot hold once the sender is
     * fatal (and a not-yet-ready sender can never flip ready afterwards). */
    bool r = ms->ready && !ms->fatal;
    pthread_mutex_unlock(&ms->mu);
    return r;
}

/* The wait level (s->mu held): ready AND the active queue has headroom for
 * at least one more object under both bounds. Advisory -- see the header. */
static bool sender_wait_level(const moq_media_sender_t *s)
{
    uint32_t depth = s->preq_tail - s->preq_head;
    return s->ready && depth < active_obj_cap(s) &&
           s->preq_bytes < active_byte_cap(s);
}

/* Level-triggered wait, the sender twin of moq_media_receiver_wait: check
 * the level, block in the ENDPOINT wait (queue drain and the readiness flip
 * both happen on pump cycles, which mark the endpoint's coalesced activity),
 * re-check. PRIORITY before every MOQ_OK, pre- and post-wait: interrupt
 * latch, then terminal (sender fatal / endpoint closed), then the level --
 * write() refuses MOQ_ERR_INTERRUPTED under the latch and MOQ_ERR_CLOSED
 * once terminal, so reporting "write now" in either state would be a lie
 * (unlike the receiver, whose polls stay legal while latched and keep
 * draining queued items after terminal). Lock order: s->mu is never held
 * across the endpoint calls (hook order is ep->mu -> s->mu; see the
 * BLOCK_TIMEOUT comment above); the latch reads use the ep->mu-free
 * internal probe. */
moq_result_t moq_media_sender_wait(moq_media_sender_t *s, uint64_t timeout_us)
{
    if (!s) return MOQ_ERR_INVAL;

    if (moq_endpoint_interrupted_internal(s->ep)) return MOQ_ERR_INTERRUPTED;

    pthread_mutex_lock(&s->mu);
    bool level = sender_wait_level(s);
    bool fatal = s->fatal;
    bool mismatch = s->transport_version && !sender_version_usable(s);
    pthread_mutex_unlock(&s->mu);
    if (fatal || moq_endpoint_is_closed(s->ep)) return MOQ_ERR_CLOSED;
    if (mismatch) return MOQ_ERR_UNSUPPORTED;
    if (level) return MOQ_OK;

    moq_result_t rc = moq_endpoint_wait(s->ep, timeout_us);
    if (rc == MOQ_ERR_INTERRUPTED || rc == MOQ_DONE) return rc;
    if (moq_endpoint_interrupted_internal(s->ep)) return MOQ_ERR_INTERRUPTED;

    pthread_mutex_lock(&s->mu);
    level = sender_wait_level(s);
    fatal = s->fatal;
    mismatch = s->transport_version && !sender_version_usable(s);
    pthread_mutex_unlock(&s->mu);
    if (fatal || moq_endpoint_is_closed(s->ep)) return MOQ_ERR_CLOSED;
    if (mismatch) return MOQ_ERR_UNSUPPORTED;
    if (level) return MOQ_OK;
    return rc < 0 ? rc : MOQ_OK;
}

size_t moq_media_sender_track_subscriptions(const moq_media_sender_t *s,
                                            const moq_media_track_t *track)
{
    if (!s || !track) return 0;
    moq_media_sender_t *ms = (moq_media_sender_t *)(uintptr_t)s;
    size_t n = 0;
    pthread_mutex_lock(&ms->mu);
    /* Validate ownership (the catalog track is private, not in tracks[]). */
    bool owned = (ms->catalog_track == track);
    if (!owned)
        for (size_t i = 0; i < ms->track_count; i++)
            if (ms->tracks[i] == track) { owned = true; break; }
    if (owned) n = track->active_subs;
    pthread_mutex_unlock(&ms->mu);
    return n;
}

moq_result_t moq_media_sender_take_new_group_request(moq_media_sender_t *s,
                                                     moq_media_track_t *track,
                                                     uint64_t *requested_group)
{
    if (!s || !track) return MOQ_ERR_INVAL;
    moq_result_t rc = MOQ_ERR_INVAL;
    pthread_mutex_lock(&s->mu);
    for (size_t i = 0; i < s->track_count; i++) {
        if (s->tracks[i] != track) continue;
        rc = MOQ_DONE;
        if (track->new_group_pending) {
            track->new_group_pending = false;
            if (requested_group) *requested_group = track->new_group_value;
            rc = MOQ_OK;
        }
        break;
    }
    pthread_mutex_unlock(&s->mu);
    return rc;
}

bool moq_media_sender_track_has_subscriber(const moq_media_sender_t *s,
                                           const moq_media_track_t *track)
{
    return moq_media_sender_track_subscriptions(s, track) > 0;
}

bool moq_media_sender_has_media_subscriber(const moq_media_sender_t *s)
{
    if (!s) return false;
    moq_media_sender_t *ms = (moq_media_sender_t *)(uintptr_t)s;
    bool any = false;
    pthread_mutex_lock(&ms->mu);
    for (size_t i = 0; i < ms->track_count; i++) {
        moq_media_track_t *t = ms->tracks[i];
        if (t->removed || !track_is_app_media(t)) continue;
        if (t->active_subs > 0) { any = true; break; }
    }
    pthread_mutex_unlock(&ms->mu);
    return any;
}

bool moq_media_sender_is_closed(const moq_media_sender_t *s)
{
    if (!s) return true;
    return moq_endpoint_is_closed(s->ep);
}

bool moq_media_sender_is_fatal(const moq_media_sender_t *s)
{
    if (!s) return false;
    moq_media_sender_t *ms = (moq_media_sender_t *)(uintptr_t)s;
    pthread_mutex_lock(&ms->mu);
    bool f = ms->fatal;
    pthread_mutex_unlock(&ms->mu);
    return f || moq_endpoint_is_fatal(s->ep);
}

uint64_t moq_media_sender_fatal_code(const moq_media_sender_t *s)
{
    if (!s) return 0;
    moq_media_sender_t *ms = (moq_media_sender_t *)(uintptr_t)s;
    pthread_mutex_lock(&ms->mu);
    bool f = ms->fatal;
    uint64_t code = ms->fatal_code;
    pthread_mutex_unlock(&ms->mu);
    if (f) return code;
    return moq_endpoint_fatal_code(s->ep);
}

#ifdef MOQ_MEDIA_SENDER_TESTING
/* -- White-box test seam (not built into the shipping library) ---------- *
 * Tests the catalog-content model deterministically with no endpoint/network:
 * add/remove tracks, then build the catalog the sender would publish. The
 * pump-driven wire republish (group numbering, retained group, staged
 * backpressure) is covered by the loopback tests. */

/* The exact config gate moq_media_sender_create()/attach() run before any
 * networking, exposed so authoring validation (contentProtections syntax) is
 * testable without standing up an endpoint. */
moq_result_t moq_media_sender_test_validate_cfg(const moq_media_sender_cfg_t *cfg)
{
    return sender_validate_cfg(cfg);
}

/* Build a sender from a real cfg with NO endpoint (mirrors the receiver's
 * scripted seam): moq_media_sender_add_track and the real sender_hook can
 * then be driven against a caller-supplied SimPair session, so rejection
 * paths are exercised through the PRODUCTION hook -- not a handler stub. */
moq_media_sender_t *moq_media_sender_test_new_cfg(
    const moq_media_sender_cfg_t *cfg)
{
    sender_sizes_t sz;
    if (sender_prepare(cfg, &sz) < 0) return NULL;
    moq_media_sender_t *s = NULL;
    if (sender_new(NULL, false, cfg, &sz, &s) != MOQ_OK) return NULL;
    return s;
}

/* The same construction as moq_media_sender_test_new_cfg, surfacing the REAL
 * result (INVAL from validation vs NOMEM from the allocator) so a construction
 * oracle can distinguish a refusal from an allocation failure. No endpoint. */
moq_result_t moq_media_sender_test_construct(const moq_media_sender_cfg_t *cfg,
                                             moq_media_sender_t **out);
moq_result_t moq_media_sender_test_construct(const moq_media_sender_cfg_t *cfg,
                                             moq_media_sender_t **out)
{
    if (!out) return MOQ_ERR_INVAL;
    *out = NULL;
    sender_sizes_t sz;
    moq_result_t rc = sender_prepare(cfg, &sz);
    if (rc < 0) return rc;
    return sender_new(NULL, false, cfg, &sz, out);
}

/* Test-only: how many times the pending-reset transition has been ATTEMPTED.
 * Behind MOQ_MEDIA_SENDER_TESTING, so it is absent from the shipping library. */
uint32_t moq_media_sender_test_reset_attempts(const moq_media_sender_t *s)
{
    return s ? s->test_reset_attempts : 0u;
}

/* Drive the production sender_hook with a caller-controlled session (the hook
 * ignores its endpoint argument -- (void)ep). */
void moq_media_sender_test_pump(moq_media_sender_t *s,
                                moq_session_t *session, uint64_t now_us)
{
    sender_hook(NULL, session, now_us, s);
}

moq_media_sender_t *moq_media_sender_test_new(void)
{
    const moq_alloc_t *alloc = moq_alloc_default();
    moq_media_sender_t *s = (moq_media_sender_t *)alloc->alloc(
        sizeof(*s), alloc->ctx);
    if (!s) return NULL;
    memset(s, 0, sizeof(*s));
    s->test_block_republish_obj = -1;
    s->alloc = *alloc;
    s->ep = NULL;
    s->backpressure = MOQ_MEDIA_SEND_BP_DROP_TO_KEYFRAME;
    /* Production-default queue bounds: a zeroed cap is not a reachable
     * production state (sender_new always applies the defaults), and the
     * wait-level predicate reads them. */
    s->queue_cap      = SENDER_DEFAULT_QUEUE_OBJECTS;
    s->queue_byte_cap = SENDER_DEFAULT_QUEUE_BYTES;
    s->preq_cap       = SENDER_DEFAULT_PRE_READY_OBJECTS;
    s->preq_byte_cap  = SENDER_DEFAULT_PRE_READY_BYTES;
    pthread_mutex_init(&s->mu, NULL);
    pthread_cond_init(&s->space_cv, NULL);
    return s;
}

void moq_media_sender_test_free(moq_media_sender_t *s)
{
    if (!s) return;
    /* The pumped hook creates the publisher facade (moq_pub_create), and
     * sender_free does not own it -- moq_media_sender_destroy does. Tear it
     * down here for the same reason the catalog track is released below, or a
     * seam test that pumped even once retains the whole publisher graph.
     *
     * Inline destruction is correct HERE: this endpoint-free seam is driven
     * and destroyed on its owning test thread, so the hook cannot be running
     * concurrently. Production posts sender_destroy_pub_task instead because
     * its facade lives on the endpoint's network thread. */
    if (s->pub) { moq_pub_destroy(s->pub); s->pub = NULL; }
    /* A test may have lazily created the private catalog track (via
     * moq_media_sender_test_catalog_track); sender_free does not own it (destroy
     * does), so release it here to keep the test leak-free. */
    if (s->catalog_track) { track_free(s, s->catalog_track); s->catalog_track = NULL; }
    sender_free(s);
}

/* Force or clear the sender-fatal flag, so a wait-contract test can prove
 * terminal state beats the ready+space level. Clearing is test-only (fatal is
 * one-way in production; sender_set_fatal_locked never clears). */
void moq_media_sender_test_set_fatal(moq_media_sender_t *s, bool fatal)
{
    pthread_mutex_lock(&s->mu);
    if (fatal) {
        sender_set_fatal_locked(s, 0x1);
    } else {
        s->fatal = false;
        s->fatal_code = 0;
    }
    pthread_mutex_unlock(&s->mu);
}

/* Point a test sender at a (bare) endpoint so the BLOCK_TIMEOUT closed-check can
 * be exercised; pair with set_ep(s, NULL) before free so sender_free does not
 * touch a test-owned endpoint. */
void moq_media_sender_test_set_ep(moq_media_sender_t *s, moq_endpoint_t *ep)
{
    s->ep = ep;
}

/* Run the exact closed-check the BLOCK_TIMEOUT loop uses. The lock-order
 * regression calls this from a worker while the test holds ep->mu: it must
 * return WITHOUT blocking on ep->mu (taking ep->mu here would deadlock against
 * a caller that holds it). */
bool moq_media_sender_test_endpoint_closed(moq_media_sender_t *s)
{
    return sender_ep_closed(s);
}

/* Release the oldest queued entry. PRECONDITION: s->mu is already held by the
 * caller (the scripted wait hook runs inside the write's critical section), so
 * this must not lock. Mirrors the drain's release + byte accounting. */
void moq_media_sender_test_release_one_locked(moq_media_sender_t *s);
void moq_media_sender_test_release_one_locked(moq_media_sender_t *s)
{
    if (!s || s->preq_head == s->preq_tail) return;
    sender_preq_entry_t *e = &s->preq[s->preq_head % s->ring_cap];
    s->preq_bytes -= e->bytes;
    preq_entry_release(s, e);
    s->preq_head++;
}

/* Registered track handles (app media + generated), read under s->mu. */
size_t moq_media_sender_test_track_count(moq_media_sender_t *s);
size_t moq_media_sender_test_track_count(moq_media_sender_t *s)
{
    pthread_mutex_lock(&s->mu);
    size_t n = s->track_count;
    pthread_mutex_unlock(&s->mu);
    return n;
}

/* Simulate the post-ready transition (so add/remove dirty the catalog). */
/* The i-th queued entry, oldest first: its track, payload (borrowed), Group
 * id, and whether it opens its Group. False past the end. */
bool moq_media_sender_test_queue_entry(moq_media_sender_t *s, size_t i,
                                       const moq_media_track_t **track,
                                       const moq_rcbuf_t **payload,
                                       uint64_t *group, bool *starts_group);
bool moq_media_sender_test_queue_entry(moq_media_sender_t *s, size_t i,
                                       const moq_media_track_t **track,
                                       const moq_rcbuf_t **payload,
                                       uint64_t *group, bool *starts_group)
{
    pthread_mutex_lock(&s->mu);
    bool ok = i < (size_t)(s->preq_tail - s->preq_head);
    if (ok) {
        const sender_preq_entry_t *e =
            &s->preq[(s->preq_head + (uint32_t)i) % s->ring_cap];
        if (track) *track = e->track;
        if (payload) *payload = e->payload;
        if (group) *group = e->group_seq;
        if (starts_group) *starts_group = e->starts_group;
    }
    pthread_mutex_unlock(&s->mu);
    return ok;
}

/* Take the queue head as the drain would after a successful write to the
 * session: the wire state (open Group, next Object id) and the sent counters
 * advance, and the payload reference passes to the caller. False when the
 * queue is empty or the head is the END_OF_TRACK marker. */
bool moq_media_sender_test_emit_head(moq_media_sender_t *s,
                                     moq_rcbuf_t **payload);
bool moq_media_sender_test_emit_head(moq_media_sender_t *s,
                                     moq_rcbuf_t **payload)
{
    pthread_mutex_lock(&s->mu);
    if (s->preq_head == s->preq_tail ||
        s->preq[s->preq_head % s->ring_cap].is_end) {
        pthread_mutex_unlock(&s->mu);
        return false;
    }
    sender_preq_entry_t *e = &s->preq[s->preq_head % s->ring_cap];
    moq_media_track_t *t = e->track;
    bool new_group = !t->emit_open || t->emit_group != e->group_seq;
    t->emit_obj = (new_group ? 0 : t->emit_obj) + 1;
    t->emit_group = e->group_seq;
    t->emit_open = true;
    s->stats.objects_sent++;
    t->tstats.objects_sent++;
    t->tstats.objects_queued--;
    t->tstats.bytes_queued -= e->bytes;
    s->preq_bytes -= e->bytes;
    *payload = e->payload;
    e->payload = NULL;
    preq_entry_release(s, e);
    s->preq_head++;
    pthread_cond_broadcast(&s->space_cv);
    pthread_mutex_unlock(&s->mu);
    return true;
}

/* Discard every queued object of `t` through the drop path, as a policy
 * would. Caller holds s->mu (e.g. from the cond-wait hook). */
void moq_media_sender_test_drop_track_locked(moq_media_sender_t *s,
                                             moq_media_track_t *t);
void moq_media_sender_test_drop_track_locked(moq_media_sender_t *s,
                                             moq_media_track_t *t)
{
    for (;;) {
        bool any = false;
        uint64_t g = 0;
        for (uint32_t i = s->preq_head; i != s->preq_tail; i++) {
            const sender_preq_entry_t *e = &s->preq[i % s->ring_cap];
            if (e->track == t && !e->is_end) { any = true; g = e->group_seq; break; }
        }
        if (!any) return;
        preq_evict_group(s, t, g);
    }
}

void moq_media_sender_test_set_ready(moq_media_sender_t *s)
{
    pthread_mutex_lock(&s->mu);
    s->ready = true;
    pthread_mutex_unlock(&s->mu);
}

/* Install lifecycle callbacks on a test sender (no endpoint), so the fire
 * seams below can be observed. Mirrors the negotiated copy sender_new does. */
void moq_media_sender_test_set_callbacks(moq_media_sender_t *s,
                                         const moq_media_sender_callbacks_t *cb)
{
    pthread_mutex_lock(&s->mu);
    s->callbacks.ctx = cb->ctx;
    s->callbacks.on_ready = cb->on_ready;
    s->callbacks.on_closed = cb->on_closed;
    s->callbacks.on_track_closed = cb->on_track_closed;
    s->callbacks.struct_size = sizeof(s->callbacks);
    pthread_mutex_unlock(&s->mu);
}

/* Drive the core on_publish_finished handler for a track, mapping by pub_track
 * exactly as the network thread does. The test assigns the track a sentinel
 * pub_track (its own address) so the production mapping loop resolves it; then
 * the real handler runs (app-media filter + per-track latch + catalog->on_closed
 * routing). Pass the catalog track to exercise the catalog-termination path. */
void moq_media_sender_test_fire_publish_finished(moq_media_sender_t *s,
                                                 moq_media_track_t *track)
{
    track->pub_track = (moq_pub_track_t *)track;   /* sentinel for the map */
    sender_pub_on_publish_finished(s, track->pub_track);
}

/* Expose the catalog track handle so the test can drive its termination. */
moq_media_track_t *moq_media_sender_test_catalog_track(moq_media_sender_t *s)
{
    if (!s->catalog_track) {
        s->catalog_track = (moq_media_track_t *)s->alloc.alloc(
            sizeof(moq_media_track_t), s->alloc.ctx);
        if (s->catalog_track) {
            memset(s->catalog_track, 0, sizeof(*s->catalog_track));
            s->catalog_track->is_catalog = true;
        }
    }
    return s->catalog_track;
}

/* Drive the PRODUCTION fire helpers (same code the network thread runs), so the
 * test exercises the real single-fire latch + callback dispatch, not a copy. */
void moq_media_sender_test_fire_ready(moq_media_sender_t *s)
{
    sender_fire_ready(s);
}

void moq_media_sender_test_fire_closed(moq_media_sender_t *s, bool is_fatal,
                                       uint64_t fatal_code)
{
    sender_fire_closed(s, is_fatal, fatal_code);
}

/* Drive the fatal path end-to-end: sender_set_fatal marks fatal AND fires
 * on_closed(is_fatal=true) inline, exactly as a network-thread fatal does. */
void moq_media_sender_test_fire_fatal(moq_media_sender_t *s, uint64_t code)
{
    sender_set_fatal(s, code);
}

/* Simulate complete() state without an endpoint: set completing + mark every
 * track removed, so the next build emits isComplete:true + empty tracks. */
void moq_media_sender_test_complete(moq_media_sender_t *s)
{
    pthread_mutex_lock(&s->mu);
    s->completing = true;
    for (size_t i = 0; i < s->track_count; i++)
        s->tracks[i]->removed = true;
    s->catalog_dirty = true;
    pthread_mutex_unlock(&s->mu);
}

/* Build the catalog the sender would currently publish (skips removed tracks). */
moq_result_t moq_media_sender_test_build_catalog(moq_media_sender_t *s,
                                                 moq_rcbuf_t **out)
{
    pthread_mutex_lock(&s->mu);
    moq_result_t rc = sender_build_catalog_fresh(s, out, false, NULL);
    pthread_mutex_unlock(&s->mu);
    return rc;
}

bool moq_media_sender_test_catalog_dirty(const moq_media_sender_t *s)
{
    moq_media_sender_t *ms = (moq_media_sender_t *)(uintptr_t)s;
    pthread_mutex_lock(&ms->mu);
    bool d = ms->catalog_dirty;
    pthread_mutex_unlock(&ms->mu);
    return d;
}

/* The last committed catalog generation number (0 = the initial catalog). Each
 * add/remove/conversion or automatic refresh that commits advances it by one. */
uint64_t moq_media_sender_test_catalog_group(const moq_media_sender_t *s)
{
    moq_media_sender_t *ms = (moq_media_sender_t *)(uintptr_t)s;
    pthread_mutex_lock(&ms->mu);
    uint64_t g = ms->catalog_group;
    pthread_mutex_unlock(&ms->mu);
    return g;
}

/* Force the last-committed catalog generation number, so a test can drive the
 * group ceiling (UINT64_MAX) without publishing 2^64 generations. */
void moq_media_sender_test_set_catalog_group(moq_media_sender_t *s, uint64_t g)
{
    pthread_mutex_lock(&s->mu);
    s->catalog_group = g;
    pthread_mutex_unlock(&s->mu);
}

/* The cached managed-adapter wake deadline (what sender_next_deadline_us
 * returns): a pure scalar, UINT64_MAX when refresh is not timer-waiting. */
uint64_t moq_media_sender_test_next_deadline_us(moq_media_sender_t *s)
{
    return sender_next_deadline_us(s);
}

/* The RESOLVED automatic-refresh interval (UINT64_MAX when disabled, which
 * is the default for 0/absent as well as explicit UINT64_MAX). Lets a config
 * test prove the default / custom / disable resolution without standing up an
 * endpoint. */
uint64_t moq_media_sender_test_refresh_interval(const moq_media_sender_t *s)
{
    moq_media_sender_t *ms = (moq_media_sender_t *)(uintptr_t)s;
    pthread_mutex_lock(&ms->mu);
    uint64_t iv = ms->catalog_refresh_interval_us;
    pthread_mutex_unlock(&ms->mu);
    return iv;
}

/* Count of successful catalog retained-group installs (initial + every
 * committed generation). A refresh that advances the group installs exactly
 * one, so this distinguishes a real group advance from a no-op. */
unsigned moq_media_sender_test_retained_installs(const moq_media_sender_t *s)
{
    moq_media_sender_t *ms = (moq_media_sender_t *)(uintptr_t)s;
    pthread_mutex_lock(&ms->mu);
    unsigned n = ms->test_retained_installs;
    pthread_mutex_unlock(&ms->mu);
    return n;
}

/* Snapshot the current catalog as the "published" baseline (simulates a prior
 * committed generation). */
void moq_media_sender_test_mark_published(moq_media_sender_t *s)
{
    pthread_mutex_lock(&s->mu);
    moq_rcbuf_t *j = NULL;
    uint64_t stamp = 0;
    if (sender_build_catalog_fresh(s, &j, false, &stamp) == MOQ_OK && j) {
        if (s->published_catalog) moq_rcbuf_decref(s->published_catalog);
        s->published_catalog = j;
        s->published_generated_at = stamp;    /* the stamp THESE bytes carry */
        /* A committed generation also adopts the current tuple set as the
         * deltaUpdate baseline (in_published) -- mirror that so the next stage
         * diffs against it instead of treating every track as a fresh add. */
        sender_commit_baseline(s);
    }
    pthread_mutex_unlock(&s->mu);
}

/* Select the media companion of an endpoint-free test sender (one built by
 * moq_media_sender_test_new), as moq_media_sender_cfg_t.companion_profile
 * does at creation. Call before adding tracks: track-level state derived from
 * the profile (initDataList ids) is fixed at add_track. Test-only; declared
 * by the tests that use it. */
void moq_media_sender_test_set_companion(moq_media_sender_t *s,
                                         moq_media_companion_profile_t profile);
void moq_media_sender_test_set_companion(moq_media_sender_t *s,
                                         moq_media_companion_profile_t profile)
{
    pthread_mutex_lock(&s->mu);
    s->companion_profile = profile;
    s->companion_auto = profile == MOQ_MEDIA_COMPANION_AUTO;
    (void)sender_resolve_group_base(
        profile != MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01, &s->group_base);
    pthread_mutex_unlock(&s->mu);
}

/* The companion currently in effect: MOQ_MEDIA_COMPANION_AUTO until an AUTO
 * sender's first draft-16/18/22 latch, then the resolved companion. */
moq_media_companion_profile_t moq_media_sender_test_companion(
    const moq_media_sender_t *s);
moq_media_companion_profile_t moq_media_sender_test_companion(
    const moq_media_sender_t *s)
{
    moq_media_sender_t *ms = (moq_media_sender_t *)(uintptr_t)s;
    pthread_mutex_lock(&ms->mu);
    moq_media_companion_profile_t c = ms->companion_profile;
    pthread_mutex_unlock(&ms->mu);
    return c;
}

/* Pin (or release) this sender's catalog clock. `set` false restores the real
 * wallclock. Instance-owned: another sender in the same process is unaffected.
 * Declared by the tests that use it -- there is no header for it, so it adds no
 * public API or ABI surface, and it exists only under MOQ_MEDIA_SENDER_TESTING. */
void moq_media_sender_test_set_clock_ms(moq_media_sender_t *s,
                                        bool set, uint64_t ms)
{
    pthread_mutex_lock(&s->mu);
    s->test_clock_set = set;
    s->test_clock_ms = ms;
    pthread_mutex_unlock(&s->mu);
}

/* Whether a republish now would actually cut a new generation: true iff the
 * resolved catalog differs from the published baseline. Mirrors the no-op
 * dedup decision in sender_republish_catalog. */
bool moq_media_sender_test_build_changed(moq_media_sender_t *s)
{
    pthread_mutex_lock(&s->mu);
    bool unchanged = false;
    /* The SAME helper production staging uses, with the only_registered choice
     * this query owns -- so the two cannot drift apart. This seam has no error
     * channel, so a comparison failure conservatively reports "changed"; the
     * production path treats the same failure as fatal instead. */
    moq_result_t rc = sender_catalog_semantically_unchanged(s, false,
                                                            &unchanged);
    bool changed = (rc != MOQ_OK) || !unchanged;
    pthread_mutex_unlock(&s->mu);
    return changed;
}

/* True iff `track` still has its §11.3 step-1 subscriber finish armed (the
 * conversion catalog is gated on this track). */
bool moq_media_sender_test_track_finish_pending(const moq_media_sender_t *s,
                                                const moq_media_track_t *track)
{
    moq_media_sender_t *ms = (moq_media_sender_t *)(uintptr_t)s;
    pthread_mutex_lock(&ms->mu);
    bool p = track && track->vod_finish_pending;
    pthread_mutex_unlock(&ms->mu);
    return p;
}

/* True iff ANY track still has a pending conversion finish -- i.e. the pump
 * would gate the conversion catalog republish right now. */
bool moq_media_sender_test_conversion_finish_pending(const moq_media_sender_t *s)
{
    moq_media_sender_t *ms = (moq_media_sender_t *)(uintptr_t)s;
    pthread_mutex_lock(&ms->mu);
    bool any = false;
    for (size_t i = 0; i < ms->track_count; i++)
        if (ms->tracks[i]->vod_finish_pending) { any = true; break; }
    pthread_mutex_unlock(&ms->mu);
    return any;
}

/* Run the network-pump's conversion-finish step once (no endpoint). Without
 * registered publisher tracks there are no live subscribers to finish, so each
 * pending flag clears and this returns true -- exercising the gate/flag
 * lifecycle and the catalog_dirty-preservation invariant deterministically. */
bool moq_media_sender_test_run_finish_conversions(moq_media_sender_t *s)
{
    return sender_finish_conversions(s, 0);
}

/* Stamp a sentinel publisher-track handle onto every advertised track so the
 * post-ready publish path (track_in_catalog with only_registered) treats them as
 * registered. The handle is never dereferenced on the white-box build/stage path
 * -- it only gates catalog membership -- so a non-NULL sentinel suffices. */
void moq_media_sender_test_mark_registered(moq_media_sender_t *s)
{
    pthread_mutex_lock(&s->mu);
    for (size_t i = 0; i < s->track_count; i++) {
        moq_media_track_t *t = s->tracks[i];
        if (!t->is_catalog && !t->removed && !t->pub_track)
            t->pub_track = (moq_pub_track_t *)(uintptr_t)0x1;
    }
    pthread_mutex_unlock(&s->mu);
}

/* Stage one catalog generation and hand the dense wire object list (object 0 =
 * independent base, objects 1..N = deltaUpdate objects) to the caller, which
 * reconstructs the receiver-effective catalog exactly as a peer would. Transfers
 * a ref on each object into objs[0..return) (caller decrefs) and leaves the
 * sender's pending generation cleared, so this inspects staging without
 * committing a wire send. Returns 0 if staging produced no generation (e.g. the
 * resolved catalog matched the published baseline). */
size_t moq_media_sender_test_stage(moq_media_sender_t *s,
                                   moq_rcbuf_t **objs, size_t cap)
{
    pthread_mutex_lock(&s->mu);
    if (!sender_stage_generation(s)) { pthread_mutex_unlock(&s->mu); return 0; }
    size_t n = s->pending_obj_count, out = 0;
    for (size_t i = 0; i < n; i++) {
        if (out < cap) objs[out++] = s->pending_objs[i];   /* move ref */
        else if (s->pending_objs[i]) moq_rcbuf_decref(s->pending_objs[i]);
        s->pending_objs[i] = NULL;
    }
    if (s->pending_current) { moq_rcbuf_decref(s->pending_current); s->pending_current = NULL; }
    s->pending_generated_at = 0;
    s->pending_obj_count = 0;
    s->pending_obj_cursor = 0;
    s->pending_retained_set = false;
    pthread_mutex_unlock(&s->mu);
    return out;
}
/* Run the PRODUCTION cfg-tail copier (callbacks + publish_tracks +
 * drop_without_demand) against a test sender, so the ABI prefix rules are
 * tested on the exact code create/attach runs -- not a test-only mirror. */
void moq_media_sender_test_apply_cfg_tail(moq_media_sender_t *s,
                                          const moq_media_sender_cfg_t *cfg)
{
    pthread_mutex_lock(&s->mu);
    memset(&s->callbacks, 0, sizeof(s->callbacks));
    s->publish_tracks = false;
    s->drop_without_demand = false;
    sender_copy_cfg_tail(s, cfg);
    pthread_mutex_unlock(&s->mu);
}

/* Which callbacks the copier installed (1 joined, 2 left, 4 ready, 8 closed,
 * 16 track_closed, 32 ctx) plus the copied flags (256 publish_tracks,
 * 512 drop_without_demand). */
unsigned moq_media_sender_test_cfg_tail_mask(const moq_media_sender_t *s)
{
    unsigned m = 0;
    if (s->callbacks.on_subscriber_joined) m |= 1u;
    if (s->callbacks.on_subscriber_left)   m |= 2u;
    if (s->callbacks.on_ready)             m |= 4u;
    if (s->callbacks.on_closed)            m |= 8u;
    if (s->callbacks.on_track_closed)      m |= 16u;
    if (s->callbacks.ctx)                  m |= 32u;
    if (s->publish_tracks)                 m |= 256u;
    if (s->drop_without_demand)            m |= 512u;
    return m;
}

/* Arm the one-shot republish fault: the live-write loop returns (as on
 * WOULD_BLOCK) immediately BEFORE writing `object_id` of the next pending
 * generation, snapshotting the loop state. */
void moq_media_sender_test_block_republish_before(moq_media_sender_t *s,
                                                  uint64_t object_id)
{
    pthread_mutex_lock(&s->mu);
    s->test_block_republish_obj = (int64_t)object_id;
    s->test_block_fired = false;
    pthread_mutex_unlock(&s->mu);
}

/* Arm a one-shot WOULD_BLOCK at the retained-install step (set_retained_group),
 * AFTER the live objects are written -- exercises retained-install backpressure
 * distinct from an object-write block. */
void moq_media_sender_test_block_retained_install_once(moq_media_sender_t *s)
{
    pthread_mutex_lock(&s->mu);
    s->test_block_retained_once = true;
    pthread_mutex_unlock(&s->mu);
}

/* Whether the armed fault fired; on true, returns the snapshot: the cursor
 * (== the blocked object id: everything before it was written exactly once),
 * the pending count (nonzero == the generation is NOT committed), and
 * whether the retained group had been installed at that instant. */
bool moq_media_sender_test_block_republish_state(moq_media_sender_t *s,
                                                 size_t *cursor,
                                                 size_t *count,
                                                 bool *retained_set)
{
    pthread_mutex_lock(&s->mu);
    bool fired = s->test_block_fired;
    if (fired) {
        if (cursor) *cursor = s->test_block_cursor;
        if (count) *count = s->test_block_count;
        if (retained_set) *retained_set = s->test_block_retained_set;
    }
    pthread_mutex_unlock(&s->mu);
    return fired;
}

/* Live republish state under the sender lock: the CURRENT pending cursor and
 * count (count == 0 means the last generation committed) and how many
 * retained-group installs have succeeded so far. */
void moq_media_sender_test_republish_live(moq_media_sender_t *s,
                                          size_t *cursor, size_t *count,
                                          unsigned *retained_installs)
{
    pthread_mutex_lock(&s->mu);
    if (cursor) *cursor = s->pending_obj_cursor;
    if (count) *count = s->pending_obj_count;
    if (retained_installs) *retained_installs = s->test_retained_installs;
    pthread_mutex_unlock(&s->mu);
}

#endif /* MOQ_MEDIA_SENDER_TESTING */
