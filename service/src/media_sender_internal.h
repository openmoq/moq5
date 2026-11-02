#ifndef MOQ_MEDIA_SENDER_INTERNAL_H
#define MOQ_MEDIA_SENDER_INTERNAL_H

#if defined(__linux__) && !defined(_POSIX_C_SOURCE)
#define _POSIX_C_SOURCE 200809L
#endif


/*
 * moq_media_sender_t — send-side media facade over an endpoint. Tracks,
 * catalog, readiness, and the media write path: objects written from the
 * app thread are enqueued in decode order into a bounded, per-track
 * sync-anchored queue and drained to the session on the network thread,
 * generating the standard LOC properties for the sender's companion (legacy
 * LOC-01 on drafts16/18, LOC-04 metadata on draft22; AUTO picks the one
 * matching the negotiated draft), applying backpressure policy, and
 * abandoning open subgroups wire-correctly (reset_group) on a drop.
 *
 * Threading mirrors the receiver: the publisher lives on the endpoint's
 * network thread, driven from the SENDER pump hook (run under the endpoint
 * mutex; no public endpoint API re-entry). The application calls
 * add_track / write from its own thread; the mutex below is the
 * happens-before edge between the two.
 */

#include <moq/media_sender.h>

#include "media_auth_internal.h"

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

#include "../../adapters/common/moq_posix_compat.h"   /* pthread + clock_gettime (Windows shim) */

#include <stdatomic.h>

#include <stddef.h>

#include <string.h>

#include <time.h>


#define SENDER_DEFAULT_PRE_READY_OBJECTS 256u

#define SENDER_DEFAULT_PRE_READY_BYTES   (32u * 1024u * 1024u)

#define SENDER_DEFAULT_QUEUE_OBJECTS     256u

#define SENDER_DEFAULT_QUEUE_BYTES       (32u * 1024u * 1024u)


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

#define MEDIA_TRACK_CFG_SIZE_THROUGH_DYNAMIC_GROUPS \
    ((offsetof(moq_media_track_cfg_t, dynamic_groups) + \
      sizeof(((moq_media_track_cfg_t *)0)->dynamic_groups) + \
      _Alignof(moq_media_track_cfg_t) - 1u) & \
     ~((size_t)_Alignof(moq_media_track_cfg_t) - 1u))

_Static_assert(offsetof(moq_media_track_cfg_t, has_render_group) >=
               MEDIA_TRACK_CFG_SIZE_THROUGH_DYNAMIC_GROUPS,
               "render group must follow the complete dynamic-groups layout");

_Static_assert(offsetof(moq_media_track_cfg_t, has_label) >=
               offsetof(moq_media_track_cfg_t, render_group) + sizeof(int),
               "label must follow the complete render-group pair");

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
               ((offsetof(moq_media_track_cfg_t, label) + sizeof(moq_bytes_t) +
                 _Alignof(uint64_t) - 1u) & ~((size_t)_Alignof(uint64_t) - 1u)),
               "group_order must begin at the aligned end of label");

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

_Static_assert(offsetof(moq_media_sender_cfg_t, resource_auth) >=
    ((offsetof(moq_media_sender_cfg_t, companion_profile) +
      sizeof(moq_media_companion_profile_t) + _Alignof(moq_media_sender_cfg_t) - 1u) &
     ~((size_t)_Alignof(moq_media_sender_cfg_t) - 1u)),
    "resource_auth must follow the complete companion sender allocation");


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
    bool                  has_label;
    moq_bytes_t           label;
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
    bool                  has_render_group;
    int                   render_group;
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
    moq_media_track_t     *closed_next;       /* terminal demand callbacks;
                                                network-thread-only, no allocation */
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
    bool                  auth_failed;       /* terminal local resource failure */
    bool                  publish_submitted; /* actual PUBLISH accepted locally */
    bool                  publish_resolved;  /* peer resolved this single intent;
                                                sticky through partial end retries */
    moq_media_auth_operation_t publish_auth; /* network-executor owned */
#ifdef MOQ_MEDIA_SENDER_TESTING
    uint64_t              test_publish_attempts;
    moq_result_t          test_publish_result;
    bool                  test_demand_set;
    size_t                test_demand;
#endif
    bool                  complete_end_owed; /* complete() owes this track its
                                                END_OF_TRACK: sent by the drain once
                                                nothing of the track is left queued */
    bool                  end_sent;          /* the track's END_OF_TRACK went to the
                                                publisher (an end_track marker or
                                                the owed completion end) */
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
    bool                  pending_reset;     /* a drop abandoned a held group;
                                                reset/release owed by the drain */
    bool                  reset_had_effect;  /* some slot reset successfully;
                                                retained across partial retries */
    bool                  write_blocked;     /* the facade holds a write of
                                                blocked_group refused with
                                                WOULD_BLOCK, pending retry */
    uint64_t              blocked_group;
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

    /* A timeline write the facade refused for room (either kind). The facade
     * holds it partial and takes only a byte-identical retry, while the
     * history may have grown or been evicted since: the encoded payload, its
     * Location and the record cursor it advances to are kept and retried
     * as they are. */
    moq_rcbuf_t          *tl_pend;
    uint64_t              tl_pend_group, tl_pend_obj, tl_pend_lead;
    bool                  tl_pend_independent;

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
    moq_media_auth_source_t resource_auth;
    moq_media_auth_operation_t namespace_auth;
    bool             startup_complete;

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
    bool              test_refuse_catalog_build; /* one-shot: the next catalog
                                                  build's first allocation
                                                  is refused */
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
    /* Test-only: refuse this many removed-track teardown END attempts as
     * moq_pub_end_track's WOULD_BLOCK would, leaving the teardown to retry. */
    uint32_t          test_refuse_teardown_end;
    /* Test-only: refuse this many pending-reset attempts as
     * moq_pub_reset_group's WOULD_BLOCK would, keeping the obligation. */
    uint32_t          test_refuse_reset;
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

    /* Completion (V3 / MSF §11.3). completing is set by
     * moq_media_sender_complete() under s->mu (latched, terminal): every
     * track's accepted media stays queued and each track ends after it
     * (complete_end_owed); once every track's end is sent, the drain sets
     * complete_finalized, marks the tracks removed and dirties the catalog,
     * whose next build emits isComplete:true + empty tracks. Once completing,
     * add_track/write/remove_track/end_track are refused with WRONG_STATE. */
    bool              completing;
    bool              complete_finalized;
    /* An Object complete() owed the wire was lost (a TS track's last Objects
     * could not be copied): the completion is not finalized -- no
     * END_OF_TRACK for that track and no terminal catalog. */
    bool              complete_failed;
    /* The terminal catalog generation committed -- written, retained and its
     * live deliveries finished: the completion is done, and no later failure
     * is the completion's (stats.completion_error). */
    bool              complete_terminal_committed;

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
    bool              pending_terminal;      /* the staged generation is complete()'s */
    /* A terminal generation's finish, after its retained install and before
     * its commit: the catalog's subscriptions, then its publication. Each is
     * set once that step succeeds, so a refusal retries only what is left. */
    bool              pending_subs_finished;
    bool              pending_pub_finished;
    /* A catalog subscription was accepted once this terminal generation's
     * live write had begun and before its subscriptions were finished: the
     * write's destinations were fixed when it was admitted, so the newcomer
     * is not among them. Once that write is complete the generation is
     * written again as a new group, rather than end that subscription
     * without it. */
    bool              pending_rejoined;
    /* The facade refused the live write of pending_objs[pending_obj_cursor]
     * for room and holds it: only the identical call completes it, whatever
     * the demand now is, and no other write may come first. */
    bool              pending_write_open;
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


/* moq_media_track.track_scale. */
#define SENDER_TRACK_SCALE_NONE    0u  /* LOC-04 parse found no Timescale */


#ifdef MOQ_MEDIA_SENDER_TESTING

#define SENDER_CLOCK_REALTIME(ts) \
    (g_test_clock ? g_test_clock((ts), g_test_clock_ctx) \
                  : clock_gettime(CLOCK_REALTIME, (ts)))


#else
#define SENDER_CLOCK_REALTIME(ts) clock_gettime(CLOCK_REALTIME, (ts))


#endif


/* Every size the constructor derives from a validated configuration, checked
 * for representability BEFORE the first allocation or endpoint effect: the
 * ring capacity (uint32), the ring allocation product and the namespace
 * storage sum plus the catalog-name addition (size_t). An unrepresentable
 * value is a configuration error (MOQ_ERR_INVAL), never a wrapped request. */
typedef struct sender_sizes {
    moq_alloc_t alloc;
    moq_media_auth_source_t resource_auth;
    uint32_t queue_cap;
    uint32_t preq_cap;
    uint32_t ring_cap;
    size_t   ring_bytes;
    size_t   ns_bytes;
    size_t   ns_data_size;    /* namespace bytes + catalog name bytes */
    uint64_t group_base;      /* sender_resolve_group_base */
} sender_sizes_t;
#define SENDER_NS_PER_S 1000000000L

_Static_assert((time_t)-1 < 0, "time_t must be a signed type");

_Static_assert(sizeof(time_t) <= sizeof(uint64_t),
               "time_t must fit uint64_t for the ordered-difference arithmetic");

_Static_assert((time_t)-1 == (time_t)~(time_t)0,
               "time_t must be two's complement");
/* Objects one packetizer call can hand over, and so the most a track holds
 * back under backpressure: a push ends the open Object and then fills at
 * most one more (a lead-in is shorter than an Object), and a flush ends the
 * Object the lead-in completes and then the Group's last one. */
#define SENDER_TS_STAGE_MAX 2u


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
    bool                          complete_flushed; /* complete() finished it */
    uint64_t                      partial_bytes_discarded;
    uint64_t                      continuation_discarded;
} sender_ts_t;

#endif


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
    /* Accepted input that complete() owes the wire (a TS track's source
     * being finished): it neither evicts queued media nor waits for room,
     * and is refused MOQ_ERR_WOULD_BLOCK while none is free. */
    bool         completing;
} sender_admit_t;

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility push(hidden)
#endif

void sender_set_fatal_locked(moq_media_sender_t *s, uint64_t code);

bool track_is_app_media(const moq_media_track_t *t);

bool sender_terminal(const moq_media_sender_t *s);

bool sender_ep_closed(const moq_media_sender_t *s);

bool sender_version_usable(const moq_media_sender_t *s);

bool sender_companion_refused_locked(const moq_media_sender_t *s);

#ifdef MOQ_MEDIA_SENDER_TESTING

extern int (*g_test_clock)(struct timespec *ts, void *ctx);

extern void *g_test_clock_ctx;

#endif

moq_result_t sender_validate_cfg(const moq_media_sender_cfg_t *cfg);

void sender_free(moq_media_sender_t *s);

void sender_copy_cfg_tail(moq_media_sender_t *s,
                          const moq_media_sender_cfg_t *cfg);

bool sender_product_fits(uint64_t count, size_t elem);

moq_result_t sender_resolve_group_base(bool msf01, uint64_t *out);

bool sender_utf8_valid(moq_bytes_t b);

moq_result_t sender_prepare(const moq_media_sender_cfg_t *cfg,
                            sender_sizes_t *sz);

moq_result_t sender_new(moq_endpoint_t *ep, bool owns,
                        const moq_media_sender_cfg_t *cfg,
                        sender_sizes_t *sz,
                        moq_media_sender_t **out);
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility pop
#endif

#include "media_sender_catalog_internal.h"

#include "media_sender_complete_internal.h"

#include "media_sender_mpeg2ts_internal.h"

#include "media_sender_pump_internal.h"

#include "media_sender_timeline_internal.h"

#include "media_sender_tracks_internal.h"

#include "media_sender_write_internal.h"

#endif /* MOQ_MEDIA_SENDER_INTERNAL_H */
