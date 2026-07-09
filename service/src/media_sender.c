/*
 * moq_media_sender_t — send-side media facade over an endpoint
 * (MEDIA_SERVICE_DESIGN.md §7). Tracks, catalog, readiness, and the media
 * write path: objects written from the app thread are enqueued in decode
 * order into a bounded, per-track sync-anchored queue and drained to the
 * session on the network thread, generating standard LOC-01 properties
 * from the typed fields, applying the configured backpressure policy, and
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

#include <errno.h>
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
    offsetof(moq_media_track_cfg_t, has_track_duration)

/* Minimum accepted moq_media_sender_cfg_t size: the prefix through the last
 * pre-callbacks field. A create/attach caller must supply at least this; the
 * appended `callbacks` field is read only when struct_size covers it, so an
 * older/smaller cfg keeps working (no callbacks) and a newer/larger cfg's
 * unknown trailing fields are ignored. Must NOT change when fields are added. */
#define MEDIA_SENDER_CFG_MIN_SIZE \
    offsetof(moq_media_sender_cfg_t, callbacks)

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
                                                (S2b). */
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
     * directly in codec form; this slice records one per media group-start. */
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
};

/* One queued media object (owns its refs until drained or freed). */
typedef struct sender_preq_entry {
    moq_media_track_t *track;
    bool               is_end;              /* terminal END_OF_TRACK marker
                                               (payload/properties NULL) */
    moq_rcbuf_t       *payload;
    moq_rcbuf_t       *properties;          /* app EXTRA props (passthrough) */
    bool               is_sync;
    bool               starts_group;
    bool               ends_group;
    uint64_t           group_seq;
    uint64_t           pts_us;              /* presentation time; LOC ts fallback */
    bool               has_capture_time;    /* app-supplied LOC Capture Timestamp */
    uint64_t           capture_time_us;     /* wall-clock us since epoch (LOC-01 2.3.1.1) */
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

    sender_preq_entry_t *preq;            /* ring; entries own their refs */
    uint32_t          preq_head, preq_tail;
    uint64_t          preq_bytes;

    bool              ready;
    bool              fatal;
    uint64_t          fatal_code;

    /* Latches so on_ready / on_closed each fire at most once (network thread). */
    bool              ready_fired;
    bool              closed_fired;

    moq_media_sender_stats_t stats;       /* counters (mu-protected) */

    /* Catalog republish (S1). catalog_dirty is set by add_track/remove_track
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
    uint64_t          catalog_group;        /* last published generation (0 = initial) */
    moq_rcbuf_t      *published_catalog;     /* last committed catalog bytes (the
                                                independent base / no-op dedup) */
    /* Staged in-flight generation (S2b). A post-ready generation is a NEW group:
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
    uint64_t          pending_group;
};

/* -- small helpers ---------------------------------------------------- */

static void sender_set_fatal_locked(moq_media_sender_t *s, uint64_t code)
{
    if (!s->fatal) {
        s->fatal = true;
        s->fatal_code = code;
    }
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

/* The LOC profile for the session. LOC-01 is the only profile the loc
 * library implements (both parse and encode), and the receiver's decode
 * path is LOC-01 too, so the whole media stack is LOC-01 today regardless
 * of the negotiated MoQ draft. LOC-02 (the draft-18 KVP profile) is a
 * coordinated future item -- encode, decode, and this selector must move
 * together; this is the single place the sender picks it up. The
 * standard-key validation in write() and the (next slice's) property
 * generation both key off this, so they can never disagree on the
 * profile. */
static moq_loc_profile_t sender_loc_profile(const moq_media_sender_t *s)
{
    (void)s;   /* version-driven once LOC-02 lands; LOC-01-only for now */
    return MOQ_LOC_PROFILE_01;
}

/* -- Send queue (mu held by caller) ----------------------------------- *
 * One ring serves both phases: the pre-ready bound applies until ready,
 * the (typically larger) send-queue bound after. The ring is sized to the
 * max of both so the bound is purely a policy gate, never a realloc. */

static void preq_entry_release(moq_media_sender_t *s, sender_preq_entry_t *e)
{
    (void)s;
    if (e->payload)    moq_rcbuf_decref(e->payload);
    if (e->properties) moq_rcbuf_decref(e->properties);
    memset(e, 0, sizeof(*e));
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
            s->preq_bytes -= e->bytes;
            s->stats.objects_dropped++;
            if (e->is_sync) s->stats.keyframes_dropped++;
            preq_entry_release(s, e);   /* clears the slot */
            dropped_any = true;
        } else {
            if (wr != rd) s->preq[wr % s->ring_cap] = *e;
            wr++;
        }
        rd++;
    }
    s->preq_tail = wr;
    if (dropped_any) {
        s->stats.groups_dropped++;
        /* If the wire subgroup for this group is open, its tail just went
         * away -- the drain must RESET it, not leave it truncated. */
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

/* Drop all queued media of `track` while it has no demand, keeping the sender at
 * the live edge (the peer accepts a mid-GOP resume gap for minimal latency).
 * Preserves the terminal END_OF_TRACK (is_end) and arms pending_reset if a wire
 * subgroup is open. Single compaction pass. mu held. */
static void preq_drop_all_for_track(moq_media_sender_t *s,
                                    moq_media_track_t *t)
{
    uint32_t rd = s->preq_head, wr = s->preq_head;
    bool dropped_any = false;
    while (rd != s->preq_tail) {
        sender_preq_entry_t *e = &s->preq[rd % s->ring_cap];
        if (e->track == t && !e->is_end) {
            s->preq_bytes -= e->bytes;
            s->stats.objects_dropped++;
            if (e->is_sync) s->stats.keyframes_dropped++;
            preq_entry_release(s, e);   /* clears the slot */
            dropped_any = true;
        } else {
            if (wr != rd) s->preq[wr % s->ring_cap] = *e;
            wr++;
        }
        rd++;
    }
    s->preq_tail = wr;
    if (dropped_any) {
        s->stats.groups_dropped++;
        /* An open wire subgroup for this track just lost its tail - the drain
         * must RESET it, not leave it truncated. */
        if (t->emit_open)
            t->pending_reset = true;
        pthread_cond_broadcast(&s->space_cv);   /* freed space: wake writers */
    }
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
        if (e->is_end) continue;   /* terminal marker: never an eviction target */
        bool evictable;
        if (e->track == in_track && in_anchors) {
            evictable = true;
        } else {
            uint64_t aseq = 0;
            bool found = preq_track_anchor(s, e->track, &aseq);
            evictable = !found || e->group_seq < aseq;
        }
        if (evictable) {
            preq_evict_group(s, e->track, e->group_seq);
            return true;
        }
    }
    return false;
}

/* Whether an entry's group is evictable for an incoming object of
 * `in_track` (in_anchors = the incoming object re-anchors its own track,
 * making all of that track's queued groups droppable). Same rule as
 * preq_evict_one. mu held. */
static bool entry_evictable(moq_media_sender_t *s,
                            const sender_preq_entry_t *e,
                            const moq_media_track_t *in_track, bool in_anchors)
{
    /* A queued END_OF_TRACK marker is the ONLY chance to emit EOS reliably
     * (end_track is idempotent and later writes are refused), so it is never
     * evictable -- a drop policy must surface WOULD_BLOCK rather than discard
     * it. Excluding it here keeps preq_make_room's feasibility count honest. */
    if (e->is_end) return false;
    if (e->track == in_track && in_anchors) return true;
    uint64_t aseq = 0;
    bool found = preq_track_anchor(s, e->track, &aseq);
    return !found || e->group_seq < aseq;
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

    m->packaging = (t->packaging == MOQ_MEDIA_PACKAGING_CMAF)
        ? (moq_bytes_t){ (const uint8_t *)"cmaf", 4 }
        : (moq_bytes_t){ (const uint8_t *)"loc", 3 };
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

/* Builds the catalog JSON from s->tracks. CALLER MUST HOLD s->mu: it walks
 * s->tracks and reads per-track fields (name/desc/removed/pub_track), so the
 * lock keeps it from observing a half-applied app-thread add/remove. The hook's
 * initial-setup call and sender_stage_generation both hold s->mu around it. */
static moq_result_t sender_build_catalog(moq_media_sender_t *s,
                                         moq_rcbuf_t **out, bool only_registered)
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
    /* Raw (pre-base64) init bytes per initDataList entry, for dedup compares. */
    moq_bytes_t *idl_raw = (moq_bytes_t *)s->alloc.alloc(
        cap * sizeof(moq_bytes_t), s->alloc.ctx);
    if (!mt || !b64 || !idl || !idl_raw) {
        if (mt)  s->alloc.free(mt, cap * sizeof(moq_msf_track_t), s->alloc.ctx);
        if (b64) s->alloc.free(b64, cap * sizeof(moq_rcbuf_t *), s->alloc.ctx);
        if (idl) s->alloc.free(idl,
            cap * sizeof(moq_msf_init_data_entry_t), s->alloc.ctx);
        if (idl_raw) s->alloc.free(idl_raw, cap * sizeof(moq_bytes_t),
            s->alloc.ctx);
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
            if (t->packaging == MOQ_MEDIA_PACKAGING_CMAF) {
                /* CMSF §3.1: the init segment lives in initDataList[]; the track
                 * references it via initRef. MSF-01 lets several tracks share one
                 * entry, so reuse an existing entry whose raw init bytes are
                 * identical instead of duplicating it; the entry id is the name
                 * of the track that introduced it. */
                size_t e = idl_count;
                for (size_t j = 0; j < idl_count; j++) {
                    if (idl_raw[j].len == t->init_data.len &&
                        (t->init_data.len == 0 ||
                         memcmp(idl_raw[j].data, t->init_data.data,
                                t->init_data.len) == 0)) {
                        e = j;
                        break;
                    }
                }
                if (e == idl_count) {
                    rc = moq_msf_encode_init_data(&s->alloc, t->init_data,
                                                  &b64[nb]);
                    if (rc < 0) break;
                    idl[idl_count].id   = t->name;
                    idl[idl_count].type =
                        (moq_bytes_t){ (const uint8_t *)"inline", 6 };
                    idl[idl_count].data = (moq_bytes_t){
                        moq_rcbuf_data(b64[nb]), moq_rcbuf_len(b64[nb]) };
                    idl_raw[idl_count] = t->init_data;
                    nb++;
                    idl_count++;
                }
                mt[k].has_init_ref = true;
                mt[k].init_ref = idl[e].id;
            } else {
                /* LOC: legacy inline initData (base64), not shared. */
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
        cat.generated_at = sender_now_epoch_ms();
        /* MSF §5.1.3 / §11.3: a terminal (completing) catalog commits the
         * broadcast complete. All tracks are marked removed at complete(), so
         * the loop above already yields an empty tracks array. */
        cat.is_complete = s->completing;
        rc = moq_msf_catalog_encode(&s->alloc, &cat, out);
    }

    for (size_t i = 0; i < cap; i++)
        if (b64[i]) moq_rcbuf_decref(b64[i]);
    s->alloc.free(mt, cap * sizeof(moq_msf_track_t), s->alloc.ctx);
    s->alloc.free(b64, cap * sizeof(moq_rcbuf_t *), s->alloc.ctx);
    s->alloc.free(idl, cap * sizeof(moq_msf_init_data_entry_t), s->alloc.ctx);
    s->alloc.free(idl_raw, cap * sizeof(moq_bytes_t), s->alloc.ctx);
    return rc;
}

/* -- Drain: emit queued objects (network thread, s->mu held) ---------- */

/* Build the object's property block. RAW/LOC: generate the whole standard
 * LOC block from the typed fields for the session's profile -- the Capture
 * Timestamp is the app-supplied capture_time_us when has_capture_time, else it
 * falls back to presentation_time_us; plus video sync marking. RAW objects
 * carry no app extras in v0 (rejected at write(), see the note there). CMAF:
 * pass the app's complete block through unchanged. *out is a NEW ref the caller
 * decrefs (NULL = none). Returns false only on allocation failure. */
static bool sender_build_props(moq_media_sender_t *s,
                               const sender_preq_entry_t *e,
                               moq_rcbuf_t **out)
{
    *out = NULL;
    moq_media_track_t *t = e->track;

    if (t->packaging != MOQ_MEDIA_PACKAGING_RAW) {
        if (e->properties) *out = moq_rcbuf_incref(e->properties);
        return true;
    }

    moq_loc_headers_t h;
    moq_loc_headers_init(&h);
    h.has_timestamp = true;
    /* LOC-01 2.3.1.1: the Capture Timestamp is wall-clock microseconds since the
     * Unix epoch. Emit the app-supplied capture_time_us verbatim when present
     * (no clamp/scale -- large epoch values are valid); otherwise fall back to
     * presentation_time_us so legacy callers keep a timestamped LOC header. */
    h.timestamp = e->has_capture_time ? e->capture_time_us : e->pts_us;
    if (t->media_type == MOQ_MEDIA_TYPE_VIDEO) {
        h.has_video_frame_marking = true;
        h.video_frame_marking.independent = e->is_sync;
    }
    moq_rcbuf_t *loc = NULL;
    if (moq_loc_encode(&s->alloc, sender_loc_profile(s), &h, &loc) != MOQ_OK)
        return false;
    *out = loc;   /* has_timestamp is always set, so loc is non-NULL */
    return true;
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
        moq_pub_object_cfg_init(&ocfg);
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
 * retained accessible records through that group. Since this slice records one
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
        moq_pub_object_cfg_init(&ocfg);
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
    while (s->preq_head != s->preq_tail) {
        sender_preq_entry_t *e = &s->preq[s->preq_head % s->ring_cap];
        moq_media_track_t *t = e->track;

        /* A removed track's queued work is dropped: the catalog no longer
         * declares it and its pub track is ended by the hook. */
        if (t->removed) {
            s->preq_bytes -= e->bytes;
            preq_entry_release(s, e);
            s->preq_head++;
            pthread_cond_broadcast(&s->space_cv);
            continue;
        }

        bool has_sub = sender_track_demand(s, t);

        /* Terminal marker: emit a reliable END_OF_TRACK after this track's
         * queued objects (FIFO preserves the ordering). The facade closes any
         * open subgroup and FINs a fresh one; the session stays alive.
         *
         * Handled BEFORE the no-subscriber hold below: moq_pub_end_track()
         * treats "no subscriber" as local terminal success, so a track ended
         * before a relay subscribes (or after the subscriber leaves) drains
         * its terminal here instead of wedging the head and blocking every
         * entry behind it (including other tracks). */
        if (e->is_end) {
            /* A drop abandoned an open group: reset it on the wire first --
             * only meaningful while a live subscriber/stream exists. */
            if (has_sub && t->pending_reset) {
                moq_result_t rrc = moq_pub_reset_group(s->pub, t->pub_track,
                                                       0x10, now_us);
                if (rrc == MOQ_ERR_WOULD_BLOCK) break;   /* retry next pump */
                t->pending_reset = false;
                t->emit_open = false;
                s->stats.groups_abandoned++;
            }
            moq_result_t erc = moq_pub_end_track(s->pub, t->pub_track, now_us);
            if (erc == MOQ_ERR_WOULD_BLOCK) break;     /* keep entry, retry */
            if (erc < 0) {
                sender_set_fatal_locked(s, MOQ_MEDIA_SENDER_FATAL_SETUP_FAILED);
                break;
            }
            t->emit_open = false;
            t->pending_reset = false;   /* terminal supersedes any pending reset */
            preq_entry_release(s, e);
            s->preq_head++;
            pthread_cond_broadcast(&s->space_cv);
            continue;
        }

        /* No demand: the facade would no-op the write, so drop the queued media
         * outright to stay at the live edge (head still blocks behind it, v0). */
        if (!has_sub) {
            preq_drop_all_for_track(s, t);
            break;
        }

        /* A drop abandoned this track's open group: reset it on the wire
         * before emitting the next (newer) group. */
        if (t->pending_reset) {
            moq_result_t rrc = moq_pub_reset_group(s->pub, t->pub_track,
                                                   0x10, now_us);
            if (rrc == MOQ_ERR_WOULD_BLOCK) break;   /* retry next pump */
            t->pending_reset = false;
            t->emit_open = false;
            s->stats.groups_abandoned++;
        }

        moq_rcbuf_t *props = NULL;
        if (!sender_build_props(s, e, &props)) {
            sender_set_fatal_locked(s, MOQ_MEDIA_SENDER_FATAL_SETUP_FAILED);
            break;
        }

        bool new_group = !t->emit_open || t->emit_group != e->group_seq;
        uint64_t oid = new_group ? 0 : t->emit_obj;

        moq_pub_object_cfg_t ocfg;
        moq_pub_object_cfg_init(&ocfg);
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

        if (wrc == MOQ_ERR_WOULD_BLOCK) break;       /* keep entry, retry */
        if (wrc < 0) {
            sender_set_fatal_locked(s, MOQ_MEDIA_SENDER_FATAL_SETUP_FAILED);
            break;
        }

        t->emit_group = e->group_seq;
        t->emit_open = true;          /* facade subgroup stays open until the
                                         next group; complete groups have no
                                         queued tail, so no spurious reset */
        t->emit_obj = oid + 1;
        s->stats.objects_sent++;

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
        s->preq_head++;
        pthread_cond_broadcast(&s->space_cv);   /* space freed for writers */
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
    moq_pub_track_cfg_init(&tcfg);
    tcfg.track_namespace = s->namespace_;
    tcfg.track_name = t->is_catalog
        ? (moq_bytes_t){ s->catalog_name, s->catalog_name_len }
        : t->name;
    tcfg.advertise_namespace = advertise;

    if (t->is_catalog) {
        tcfg.has_publisher_priority = true;
        tcfg.publisher_priority = 0;
    }

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

/* Stage a post-ready generation (caller holds s->mu, pending is empty). Returns
 * true if a generation was staged into s->pending_*, false if nothing to do
 * (no-op dedup) -- on a build/encode failure it sets fatal and returns false. */
static bool sender_stage_generation(moq_media_sender_t *s)
{
    moq_rcbuf_t *current = NULL;
    if (sender_build_catalog(s, &current, true) != MOQ_OK || !current) {
        sender_set_fatal_locked(s, MOQ_MEDIA_SENDER_FATAL_CATALOG_ENCODE);
        return false;
    }
    /* No-op dedup: resolved catalog identical to the last committed one. */
    if (rcbuf_bytes_eq(current, s->published_catalog)) {
        moq_rcbuf_decref(current);
        return false;
    }

    s->pending_group = s->catalog_group + 1;
    s->pending_obj_cursor = 0;
    s->pending_retained_set = false;
    s->pending_current = current;             /* becomes published on commit */

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
    bool cmaf_init_add = false;
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
            /* A CMAF init segment lives in the independent catalog's root
             * initDataList (referenced by initRef), a shape a deltaUpdate add
             * cannot carry. Emitting it inline here would diverge from the
             * track's canonical (initRef) shape in every later independent
             * catalog, which the receiver's immutable-tuple check rejects. So a
             * generation that adds a CMAF init-bearing track falls back to a
             * full independent generation (LOC inline init, and init-less
             * tracks, stay on the delta path). */
            if (t->packaging == MOQ_MEDIA_PACKAGING_CMAF &&
                t->init_data.len > 0)
                cmaf_init_add = true;
        } else if (!in_cat && t->in_published) {
            rems[nr++] = t;
        }
    }

    if (cmaf_init_add || meta_changed) {
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

/* Post-ready catalog republish (S2b, network thread). Coalesces pending add/
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
        if (!s->catalog_dirty) { pthread_mutex_unlock(&s->mu); return; }
        s->catalog_dirty = false;   /* a later mutation re-sets it */
        if (!sender_stage_generation(s)) { pthread_mutex_unlock(&s->mu); return; }
    }

    /* Live-write objects 0..N-1 in order to active subscribers. With no
     * subscriber the live writes are skipped (the retained group below serves
     * any future joiner) -- same discipline as the media drain. A WOULD_BLOCK
     * retries from the cursor (no duplicate / skipped object). */
    if (moq_pub_has_subscriber(s->pub, s->catalog_track->pub_track)) {
        while (s->pending_obj_cursor < s->pending_obj_count) {
            size_t k = s->pending_obj_cursor;
            moq_pub_object_cfg_t ocfg;
            moq_pub_object_cfg_init(&ocfg);
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
    }

    /* Commit the generation: advance the group and adopt the new baseline. */
    s->catalog_group = s->pending_group;
    if (s->published_catalog) moq_rcbuf_decref(s->published_catalog);
    s->published_catalog = s->pending_current;   /* move ref */
    s->pending_current = NULL;
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

    if (!s->pub) {
        if (moq_session_state(session) != MOQ_SESS_ESTABLISHED)
            return;

        moq_pub_cfg_t pcfg;
        moq_pub_cfg_init_sized(&pcfg, sizeof(pcfg));
        pcfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
        pcfg.callbacks.ctx = s;
        pcfg.callbacks.on_closed = sender_pub_on_closed;
        pcfg.callbacks.on_publish_finished = sender_pub_on_publish_finished;
        if (moq_pub_create(session, &s->alloc, &pcfg, &s->pub) != MOQ_OK) {
            sender_set_fatal(s, MOQ_MEDIA_SENDER_FATAL_SETUP_FAILED);
            return;
        }

        /* Freeze the app's track list and build pub tracks from it. The
         * catalog track carries the single namespace advertisement; media
         * tracks share the namespace (already announced) so the publisher
         * answers their subscribes without re-advertising. */
        pthread_mutex_lock(&s->mu);
        s->tracks_frozen = true;
        pthread_mutex_unlock(&s->mu);

        moq_pub_track_cfg_t ccfg;
        moq_pub_track_cfg_init(&ccfg);
        ccfg.track_namespace = s->namespace_;
        ccfg.track_name = (moq_bytes_t){ s->catalog_name, s->catalog_name_len };
        ccfg.advertise_namespace = true;
        ccfg.has_publisher_priority = true;
        ccfg.publisher_priority = 0;   /* catalog leads delivery */
        if (moq_pub_add_track(s->pub, &ccfg, now_us,
                              &s->catalog_track->pub_track) != MOQ_OK) {
            sender_set_fatal(s, MOQ_MEDIA_SENDER_FATAL_SETUP_FAILED);
            return;
        }
        /* PUBLISH the catalog track (media tracks are published in
         * sender_add_pub_track below). */
        if (s->publish_tracks) {
            moq_pub_publish_cfg_t cpcfg;
            moq_pub_publish_cfg_init(&cpcfg);
            if (moq_pub_publish_track(s->pub, s->catalog_track->pub_track,
                                      &cpcfg, now_us) != MOQ_OK) {
                sender_set_fatal(s, MOQ_MEDIA_SENDER_FATAL_SETUP_FAILED);
                return;
            }
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
         * sender_build_catalog requires s->mu (consistent track view). */
        moq_rcbuf_t *json = NULL;
        pthread_mutex_lock(&s->mu);
        moq_result_t bcrc = sender_build_catalog(s, &json, true);
        pthread_mutex_unlock(&s->mu);
        if (bcrc != MOQ_OK || !json) {
            sender_set_fatal(s, MOQ_MEDIA_SENDER_FATAL_CATALOG_ENCODE);
            return;
        }
        moq_pub_retained_object_t robj = { .object_id = 0, .payload = json };
        moq_pub_retained_group_cfg_t rg;
        moq_pub_retained_group_cfg_init(&rg);
        rg.group_id = 0;
        rg.objects = &robj;
        rg.object_count = 1;
        moq_result_t src = moq_pub_set_retained_group(
            s->pub, s->catalog_track->pub_track, &rg);
        if (src != MOQ_OK) {
            moq_rcbuf_decref(json);
            sender_set_fatal(s, MOQ_MEDIA_SENDER_FATAL_SETUP_FAILED);
            return;
        }
        /* Keep the ref as the published baseline (generation 0) for the no-op
         * dedup and the deltaUpdate diff in sender_republish_catalog. */
        s->published_catalog = json;
        s->catalog_group = 0;
        s->catalog_published = true;
        /* Record the initial catalog's track set as the published baseline. */
        pthread_mutex_lock(&s->mu);
        for (size_t i = 0; i < s->track_count; i++)
            s->tracks[i]->in_published = track_in_catalog(s->tracks[i], true);
        pthread_mutex_unlock(&s->mu);
    }

    (void)moq_pub_tick(s->pub, now_us);

    /* Reconcile demand right after the tick so a fresh subscribe fires the join
     * callback in the same cycle it is accepted (before the drain runs). */
    sender_resync_demand(s);

    /* Publish-initiated: once the peer accepts the catalog PUBLISH, push the
     * initial catalog object live so a relay caches it without a FETCH. */
    if (s->publish_tracks && !s->live_catalog_sent && s->catalog_published &&
        sender_track_demand(s, s->catalog_track) &&
        s->published_catalog) {
        moq_pub_object_cfg_t ocfg;
        moq_pub_object_cfg_init(&ocfg);

        /* Same (group 0, object 0) as the retained group: it is the same catalog
         * object, so a live subscriber and a FETCH joiner see one identity. */
        ocfg.group_id = 0;
        ocfg.object_id = 0;
        ocfg.payload = s->published_catalog;
        ocfg.end_of_group = true;
        moq_result_t wrc = moq_pub_write_object_ex(
            s->pub, s->catalog_track->pub_track, &ocfg, now_us);
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
            /* `removed` is app-written under s->mu; read it under the lock. */
            pthread_mutex_lock(&s->mu);
            bool removed = t->removed;
            pthread_mutex_unlock(&s->mu);
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

void moq_media_sender_cfg_init(moq_media_sender_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = sizeof(*cfg);
    moq_media_sender_callbacks_init(&cfg->callbacks);  /* nested struct_size */
    /* CMSF §3.3/§3.4: validate CMAF output by default (strict). Affects CMAF
     * tracks only; RAW/LOC is unaffected. Callers wanting passthrough set
     * validate_cmaf = false explicitly after init. */
    cfg->validate_cmaf = true;
    /* backpressure stays UNSET on purpose: forced choice (§7.5). */
}

void moq_media_sender_callbacks_init(moq_media_sender_callbacks_t *cb)
{
    if (!cb) return;
    memset(cb, 0, sizeof(*cb));
    cb->struct_size = sizeof(*cb);
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

void moq_media_track_cfg_init(moq_media_track_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = sizeof(*cfg);
    cfg->is_live = true;
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

static moq_result_t sender_validate_cfg(const moq_media_sender_cfg_t *cfg)
{
    if (!cfg) return MOQ_ERR_INVAL;
    if (cfg->struct_size < MEDIA_SENDER_CFG_MIN_SIZE)
        return MOQ_ERR_INVAL;
    if (cfg->namespace_.count == 0 || !cfg->namespace_.parts)
        return MOQ_ERR_INVAL;
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

static void track_free(moq_media_sender_t *s, moq_media_track_t *t)
{
    if (!t) return;
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
    if (t->strings) s->alloc.free(t->strings, t->strings_size, s->alloc.ctx);
    s->alloc.free(t, sizeof(*t), s->alloc.ctx);
}

/* Build (but do not register) a generated timeline track for `media`. With
 * media_timeline=false this is the SAP eventtimeline "<media>.sap"; with
 * media_timeline=true it is the MSF §7 mediatimeline "<media>.timeline". Both
 * 'depend' on the media track. Returns NULL on allocation failure. Borrows
 * media->name (stable for the sender's life). */
static moq_media_track_t *sender_make_timeline(moq_media_sender_t *s,
                                               moq_media_track_t *media,
                                               uint32_t history_groups,
                                               bool media_timeline)
{
    static const char sap_suffix[4] = { '.', 's', 'a', 'p' };
    static const char mt_suffix[9] =
        { '.', 't', 'i', 'm', 'e', 'l', 'i', 'n', 'e' };
    const char *suffix = media_timeline ? mt_suffix : sap_suffix;
    size_t suffixlen = media_timeline ? sizeof(mt_suffix) : sizeof(sap_suffix);
    size_t namelen = media->name.len + suffixlen;
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

static moq_result_t sender_new(moq_endpoint_t *ep, bool owns,
                               const moq_media_sender_cfg_t *cfg,
                               moq_media_sender_t **out)
{
    const moq_alloc_t *alloc = moq_alloc_default();
    moq_media_sender_t *s = (moq_media_sender_t *)alloc->alloc(
        sizeof(*s), alloc->ctx);
    if (!s) return MOQ_ERR_NOMEM;
    memset(s, 0, sizeof(*s));
    s->alloc = *alloc;
    s->ep = ep;
    s->owns_endpoint = owns;
    s->backpressure = cfg->backpressure;
    s->validate_cmaf = cfg->validate_cmaf;
    /* Demand-visibility callbacks: read only when the OUTER cfg struct_size
     * reaches the appended field (older/smaller cfg => no callbacks). The nested
     * callbacks struct is then copied PREFIX-SAFELY by its own struct_size
     * (0 = none), so a future appended callback field stays ABI-safe.
     * s->callbacks is already zeroed by the memset above. */
    if (cfg->struct_size >= offsetof(moq_media_sender_cfg_t, callbacks) +
                                sizeof(cfg->callbacks)) {
        const moq_media_sender_callbacks_t *cb = &cfg->callbacks;
        if (cb->struct_size >= offsetof(moq_media_sender_callbacks_t, ctx) +
                                   sizeof(cb->ctx))
            s->callbacks.ctx = cb->ctx;
        if (cb->struct_size >= offsetof(moq_media_sender_callbacks_t,
                                        on_subscriber_joined) +
                                   sizeof(cb->on_subscriber_joined))
            s->callbacks.on_subscriber_joined = cb->on_subscriber_joined;
        if (cb->struct_size >= offsetof(moq_media_sender_callbacks_t,
                                        on_subscriber_left) +
                                   sizeof(cb->on_subscriber_left))
            s->callbacks.on_subscriber_left = cb->on_subscriber_left;
        if (cb->struct_size >= offsetof(moq_media_sender_callbacks_t,
                                        on_ready) + sizeof(cb->on_ready))
            s->callbacks.on_ready = cb->on_ready;
        if (cb->struct_size >= offsetof(moq_media_sender_callbacks_t,
                                        on_closed) + sizeof(cb->on_closed))
            s->callbacks.on_closed = cb->on_closed;
        if (cb->struct_size >= offsetof(moq_media_sender_callbacks_t,
                                        on_track_closed) +
                                   sizeof(cb->on_track_closed))
            s->callbacks.on_track_closed = cb->on_track_closed;
        s->callbacks.struct_size = sizeof(s->callbacks);
    }
    if (cfg->struct_size >= offsetof(moq_media_sender_cfg_t, publish_tracks) +
                                sizeof(cfg->publish_tracks))
        s->publish_tracks = cfg->publish_tracks;
    s->block_timeout_us = cfg->block_timeout_us
        ? cfg->block_timeout_us : SENDER_DEFAULT_BLOCK_TIMEOUT_US;
    s->queue_cap = cfg->queue_max_objects
        ? cfg->queue_max_objects : SENDER_DEFAULT_QUEUE_OBJECTS;
    s->queue_byte_cap = cfg->queue_max_bytes
        ? cfg->queue_max_bytes : SENDER_DEFAULT_QUEUE_BYTES;
    s->preq_cap = cfg->pre_ready_max_objects
        ? cfg->pre_ready_max_objects : SENDER_DEFAULT_PRE_READY_OBJECTS;
    s->preq_byte_cap = cfg->pre_ready_max_bytes
        ? cfg->pre_ready_max_bytes : SENDER_DEFAULT_PRE_READY_BYTES;
    /* One ring serves both phases; size it for the larger bound (+1 so a
     * full ring is distinguishable from empty). */
    s->ring_cap = (s->queue_cap > s->preq_cap ? s->queue_cap : s->preq_cap)
                  + 1;
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
    size_t ns_bytes = 0;
    for (size_t i = 0; i < cfg->namespace_.count; i++)
        ns_bytes += cfg->namespace_.parts[i].len;
    s->ns_parts = (moq_bytes_t *)s->alloc.alloc(
        cfg->namespace_.count * sizeof(moq_bytes_t), s->alloc.ctx);
    s->ns_data = (uint8_t *)s->alloc.alloc(ns_bytes + catalog.len,
                                           s->alloc.ctx);
    if (!s->ns_parts || !s->ns_data) { sender_free(s); return MOQ_ERR_NOMEM; }
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
    s->ns_data_size = ns_bytes + catalog.len;

    s->preq = (sender_preq_entry_t *)s->alloc.alloc(
        s->ring_cap * sizeof(sender_preq_entry_t), s->alloc.ctx);
    if (!s->preq) { sender_free(s); return MOQ_ERR_NOMEM; }
    memset(s->preq, 0, s->ring_cap * sizeof(sender_preq_entry_t));

    /* A private catalog track handle (not in the app's list; carries the
     * namespace advertisement and the retained catalog). */
    s->catalog_track = (moq_media_track_t *)s->alloc.alloc(
        sizeof(moq_media_track_t), s->alloc.ctx);
    if (!s->catalog_track) { sender_free(s); return MOQ_ERR_NOMEM; }
    memset(s->catalog_track, 0, sizeof(*s->catalog_track));
    s->catalog_track->is_catalog = true;

    moq_result_t arc = moq_endpoint_attach_hook(
        ep, MOQ_ENDPOINT_HOOK_SENDER, sender_hook, s);
    if (arc < 0) {
        track_free(s, s->catalog_track);
        s->catalog_track = NULL;
        sender_free(s);
        return arc;
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
    moq_result_t rc = sender_validate_cfg(cfg);
    if (rc < 0) return rc;
    if (cfg->endpoint != NULL) return MOQ_ERR_INVAL;   /* attach borrows */
    return sender_new(ep, false, cfg, out);
}

moq_result_t moq_media_sender_create(const moq_media_sender_cfg_t *cfg,
                                     moq_media_sender_t **out)
{
    if (!out) return MOQ_ERR_INVAL;
    *out = NULL;
    moq_result_t rc = sender_validate_cfg(cfg);
    if (rc < 0) return rc;
    if (cfg->endpoint == NULL) return MOQ_ERR_INVAL;   /* create owns */

    moq_endpoint_t *ep = NULL;
    rc = moq_endpoint_connect(cfg->endpoint, &ep);
    if (rc < 0) return rc;
    rc = sender_new(ep, true, cfg, out);
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

moq_result_t moq_media_sender_add_track(moq_media_sender_t *s,
                                        const moq_media_track_cfg_t *cfg,
                                        moq_media_track_t **out)
{
    if (out) *out = NULL;
    if (!s || !cfg || !out) return MOQ_ERR_INVAL;
    if (cfg->struct_size < MEDIA_TRACK_CFG_MIN_SIZE)
        return MOQ_ERR_INVAL;
    /* Prefix-safe by struct_size: copy the caller's (possibly older/smaller, or
     * newer/larger) cfg into a zero-filled local clamped to our struct, so
     * appended fields an older caller lacks default to absent and a newer
     * caller's unknown trailing fields are ignored. All reads below use the
     * local copy. */
    moq_media_track_cfg_t cfg_local;
    memset(&cfg_local, 0, sizeof(cfg_local));
    memcpy(&cfg_local, cfg,
           cfg->struct_size < sizeof(cfg_local) ? cfg->struct_size
                                                : sizeof(cfg_local));
    cfg_local.struct_size = sizeof(cfg_local);
    cfg = &cfg_local;
    if (cfg->name.len == 0 || !cfg->name.data) return MOQ_ERR_INVAL;
    if (cfg->media_type != MOQ_MEDIA_TYPE_VIDEO &&
        cfg->media_type != MOQ_MEDIA_TYPE_AUDIO)
        return MOQ_ERR_INVAL;
    if (cfg->packaging != MOQ_MEDIA_PACKAGING_RAW &&
        cfg->packaging != MOQ_MEDIA_PACKAGING_CMAF)
        return MOQ_ERR_INVAL;
    /* Every optional span must be coherent: a non-zero length with a NULL
     * data pointer would crash the deep-copy below. */
    if ((cfg->codec.len && !cfg->codec.data) ||
        (cfg->init_data.len && !cfg->init_data.data) ||
        (cfg->role.len && !cfg->role.data) ||
        (cfg->lang.len && !cfg->lang.data) ||
        (cfg->channel_config.len && !cfg->channel_config.data))
        return MOQ_ERR_INVAL;
    /* MSF-01: an audio/video track has an inherent codec, so it MUST carry the
     * conditional catalog fields for that codec -- the sender never emits a
     * track that is invalid by construction. codec (5.2.18) and maximum bitrate
     * (5.2.22) are required for both media types; samplerate (5.2.28) and
     * channelConfig (5.2.29) are additionally required for audio. media_type is
     * always VIDEO or AUDIO here (checked above). Generated timeline tracks
     * (.sap/.timeline) are synthesized on a separate internal path and never
     * reach this public entry, so they remain exempt. */
    if (cfg->codec.len == 0) return MOQ_ERR_INVAL;       /* 5.2.18 codec */
    if (cfg->bitrate == 0) return MOQ_ERR_INVAL;         /* 5.2.22 max bitrate */
    if (cfg->media_type == MOQ_MEDIA_TYPE_AUDIO) {
        if (cfg->samplerate == 0) return MOQ_ERR_INVAL;          /* 5.2.28 */
        if (cfg->channel_config.len == 0) return MOQ_ERR_INVAL;  /* 5.2.29 */
    }
    /* contentProtectionRefIDs: each id must be coherent AND resolve to a
     * configured root contentProtections entry (catalog coherence, not DRM
     * semantics). Root entries are immutable after construction, so this read
     * needs no lock. */
    if (cfg->content_protection_ref_id_count > 0 &&
        !cfg->content_protection_ref_ids)
        return MOQ_ERR_INVAL;
    for (size_t i = 0; i < cfg->content_protection_ref_id_count; i++) {
        moq_bytes_t ref = cfg->content_protection_ref_ids[i];
        if (ref.len == 0 || !ref.data) return MOQ_ERR_INVAL;
        if (!sender_has_cp_ref(s, ref)) return MOQ_ERR_INVAL;
    }
    /* MSF §5.2.35: trackDuration MUST NOT appear on a live track. */
    if (cfg->has_track_duration && cfg->is_live) return MOQ_ERR_INVAL;

    /* add_track is legal both before and after READY (S1): a post-ready add
     * registers the track and triggers an independent catalog republish. Once
     * completion has been requested (§11.3) the sender is terminal. */
    pthread_mutex_lock(&s->mu);
    if (s->fatal) { pthread_mutex_unlock(&s->mu); return MOQ_ERR_CLOSED; }
    if (s->completing) { pthread_mutex_unlock(&s->mu); return MOQ_ERR_WRONG_STATE; }
    pthread_mutex_unlock(&s->mu);

    /* Deep-copy every span into one buffer so the cfg need not outlive the
     * call and the catalog encode borrows stable bytes. The contentProtection
     * ref-id bytes share that buffer; their moq_bytes_t[] index is a separate
     * (aligned) allocation. */
    size_t ncpref = cfg->content_protection_ref_id_count;
    size_t cpref_bytes = 0;
    for (size_t i = 0; i < ncpref; i++)
        cpref_bytes += cfg->content_protection_ref_ids[i].len;
    size_t need = cfg->name.len + cfg->codec.len + cfg->init_data.len +
                  cfg->role.len + cfg->lang.len + cfg->channel_config.len +
                  cpref_bytes;
    moq_media_track_t *t = (moq_media_track_t *)s->alloc.alloc(
        sizeof(*t), s->alloc.ctx);
    uint8_t *buf = need ? (uint8_t *)s->alloc.alloc(need, s->alloc.ctx) : NULL;
    moq_bytes_t *cprefs = ncpref
        ? (moq_bytes_t *)s->alloc.alloc(ncpref * sizeof(moq_bytes_t),
                                        s->alloc.ctx)
        : NULL;
    if (!t || (need && !buf) || (ncpref && !cprefs)) {
        if (t) s->alloc.free(t, sizeof(*t), s->alloc.ctx);
        if (buf) s->alloc.free(buf, need, s->alloc.ctx);
        if (cprefs) s->alloc.free(cprefs, ncpref * sizeof(moq_bytes_t),
                                  s->alloc.ctx);
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
#undef COPY_SPAN
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
    t->timescale = cfg->timescale ? cfg->timescale : 1000000u;
    t->is_live = cfg->is_live;
    t->has_track_duration = cfg->has_track_duration;
    t->track_duration_ms = cfg->track_duration_ms;
    t->has_alt_group = cfg->has_alt_group;
    t->alt_group = cfg->alt_group;
    t->has_max_grp_sap = cfg->has_max_grp_sap;
    t->max_grp_sap = cfg->max_grp_sap;
    t->has_max_obj_sap = cfg->has_max_obj_sap;
    t->max_obj_sap = cfg->max_obj_sap;

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

    /* Build the generated timeline siblings before the lock (they borrow the
     * media track's stable name bytes). A media track may opt into both the SAP
     * eventtimeline and the MSF media timeline; each is an independent generated
     * track. Registered atomically with the media track below so a collision /
     * NOMEM leaves the sender unchanged. */
    moq_media_track_t *tl = NULL;    /* SAP eventtimeline "<name>.sap" */
    moq_media_track_t *mtl = NULL;   /* media timeline "<name>.timeline" */
    if (cfg->emit_sap_timeline) {
        tl = sender_make_timeline(s, t, cfg->sap_timeline_history_groups, false);
        if (!tl) { track_free(s, t); return MOQ_ERR_NOMEM; }
        t->sap_timeline = tl;
    }
    if (cfg->emit_media_timeline) {
        mtl = sender_make_timeline(s, t, cfg->media_timeline_history_groups,
                                   true);
        if (!mtl) {
            if (tl) track_free(s, tl);
            track_free(s, t);
            return MOQ_ERR_NOMEM;
        }
        t->media_timeline = mtl;
    }

    pthread_mutex_lock(&s->mu);
    if (s->fatal) {
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
    if (!sender_tracks_push(s, t)) {
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

/* -- Write (app thread) ----------------------------------------------- */

moq_result_t moq_media_sender_write(moq_media_sender_t *s,
                                    moq_media_track_t *track,
                                    const moq_media_send_object_t *obj)
{
    /* Only obj fields are touched here; the opaque track (shared with the
     * receiver API) is NOT dereferenced until proven sender-owned below. */
    if (!s || !track || !obj) return MOQ_ERR_INVAL;
    if (obj->struct_size < sizeof(moq_media_send_object_t))
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

    /* The LOC Capture Timestamp is emitted as a QUIC varint by moq_loc_encode,
     * but ONLY for RAW packaging (CMAF passes the app's property block through
     * untouched, so its timestamp fields are never encoded here). A value beyond
     * the varint range cannot be encoded; left to the drain path it would fatal
     * the sender AFTER the object was queued and its payload ownership taken.
     * Reject it synchronously -- before any enqueue / ownership transfer. The
     * encoded value is capture_time_us when has_capture_time, else the
     * presentation_time_us fallback (sender_build_props), so validate whichever
     * will be encoded. MOQ_QUIC_VARINT_MAX itself is encodable (accepted). */
    if (track->packaging == MOQ_MEDIA_PACKAGING_RAW) {
        uint64_t loc_ts = obj->has_capture_time ? obj->capture_time_us
                                                : obj->presentation_time_us;
        if (loc_ts > MOQ_QUIC_VARINT_MAX) {
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

    /* The queue takes ownership of both refs, so both count toward the
     * byte budget. */
    size_t need = moq_rcbuf_len(obj->payload);
    if (obj->properties) need += moq_rcbuf_len(obj->properties);

    /* Would this object open a new group? (Pure -- no mutation until the
     * enqueue is committed.) A sync point that opens a group is a stream
     * anchor: every group must begin with one. */
    bool opened = obj->starts_group || !track->group_open;
    bool incoming_anchors = obj->is_sync && opened;

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

    /* Sync-anchor invariant (§7.2), PER TRACK, against EMITTED-or-queued
     * anchor state (not just the queue): a group opened by a sync point
     * stays anchored even after that keyframe has drained off the ring, so
     * later deltas of a still-open group are accepted. A new group must be
     * opened by a sync point; a delta may only continue an already-anchored
     * group. Otherwise the object would be a non-decodable lead -- refuse,
     * taking no ownership. */
    if (opened) {
        if (!obj->is_sync) {
            s->stats.last_error = MOQ_ERR_WOULD_BLOCK;
            pthread_mutex_unlock(&s->mu);
            return MOQ_ERR_WOULD_BLOCK;   /* a group cannot lead with a delta */
        }
    } else if (!track->cur_group_anchored) {
        s->stats.last_error = MOQ_ERR_WOULD_BLOCK;
        pthread_mutex_unlock(&s->mu);
        return MOQ_ERR_WOULD_BLOCK;
    }

    /* Make room per the backpressure policy. Drop policies evict (and may
     * arm a wire reset); RETURN_WOULD_BLOCK refuses immediately;
     * BLOCK_TIMEOUT waits for the drain to free space, breaking promptly on
     * terminal/interrupt and surfacing WOULD_BLOCK on timeout -- never a
     * silent drop. */
    if (!preq_make_room(s, track, incoming_anchors, need)) {
        if (s->backpressure != MOQ_MEDIA_SEND_BP_BLOCK_TIMEOUT) {
            s->stats.last_error = MOQ_ERR_WOULD_BLOCK;
            pthread_mutex_unlock(&s->mu);
            return MOQ_ERR_WOULD_BLOCK;
        }
        s->stats.backpressure_stalls++;
        struct timespec deadline;
        clock_gettime(CLOCK_REALTIME, &deadline);
        uint64_t add = s->block_timeout_us;
        deadline.tv_sec  += (time_t)(add / 1000000ull);
        deadline.tv_nsec += (long)((add % 1000000ull) * 1000ull);
        if (deadline.tv_nsec >= 1000000000L) {
            deadline.tv_sec++; deadline.tv_nsec -= 1000000000L;
        }
        moq_result_t wres = MOQ_OK;
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
            /* Bounded slice so external terminal/interrupt (which do not
             * signal space_cv) are observed promptly, not at full timeout. */
            struct timespec slice;
            clock_gettime(CLOCK_REALTIME, &slice);
            slice.tv_nsec += 50 * 1000000L;
            if (slice.tv_nsec >= 1000000000L) {
                slice.tv_sec++; slice.tv_nsec -= 1000000000L;
            }
            bool past = (slice.tv_sec > deadline.tv_sec) ||
                (slice.tv_sec == deadline.tv_sec &&
                 slice.tv_nsec >= deadline.tv_nsec);
            struct timespec *until = past ? &deadline : &slice;
            int wrc = pthread_cond_timedwait(&s->space_cv, &s->mu, until);
            if (wrc == ETIMEDOUT && past) {
                wres = preq_fits(s, need) ? MOQ_OK : MOQ_ERR_WOULD_BLOCK;
                break;
            }
        }
        if (wres != MOQ_OK) {
            s->stats.last_error = wres;
            pthread_mutex_unlock(&s->mu);
            return wres;   /* no ownership taken */
        }
    }

    /* Commit the service group id: a new group bumps the seq (the first
     * group ever is 0). group_open tracks completion separately so an
     * ends_group does not make the next group reuse the same id.
     * cur_group_anchored records that this group was opened by a sync
     * point, so a later delta is accepted even after the keyframe drains. */
    if (opened) {
        track->group_seq = track->group_started ? track->group_seq + 1 : 0;
        track->group_started = true;
        track->group_open = true;
        track->cur_group_anchored = obj->is_sync;
    }

    sender_preq_entry_t *e = &s->preq[s->preq_tail % s->ring_cap];
    memset(e, 0, sizeof(*e));
    e->track = track;
    e->payload = obj->payload;          /* ownership transfers */
    e->properties = obj->properties;
    e->is_sync = obj->is_sync;
    e->starts_group = opened;
    e->ends_group = obj->ends_group;
    e->group_seq = track->group_seq;
    e->pts_us = obj->presentation_time_us;
    e->has_capture_time = obj->has_capture_time;
    e->capture_time_us = obj->capture_time_us;
    e->bytes = need;
    /* Carry the SAP declaration so the timeline record is generated at emit
     * time with the real wire Location (a dropped Object then yields no record). */
    if (track->sap_timeline && obj->has_sap_type) {
        e->has_sap_type = true;
        e->sap_type = (uint8_t)obj->sap_type;
    }
    s->preq_bytes += need;
    s->preq_tail++;
    s->stats.objects_written++;
    if (obj->ends_group) track->group_open = false;
    bool wake = s->ready;   /* post-ready: kick the drain (see below) */
    pthread_mutex_unlock(&s->mu);

    /* Wake the network thread so the drain runs without waiting for unrelated
     * traffic. Non-allocating moq_endpoint_wake() (queues no task, cannot fail);
     * called OUTSIDE s->mu. Pre-ready writes need no wake -- the readiness
     * transition itself drives the first drain. */
    if (wake)
        moq_endpoint_wake(s->ep);
    return MOQ_OK;
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

    /* Need one physical ring slot for the terminal marker (0 bytes, so it does
     * not count against the byte budget). */
    if ((s->preq_tail - s->preq_head) >= s->ring_cap) {
        pthread_mutex_unlock(&s->mu);
        return MOQ_ERR_WOULD_BLOCK;   /* queue full; retry after it drains */
    }

    sender_preq_entry_t *e = &s->preq[s->preq_tail % s->ring_cap];
    memset(e, 0, sizeof(*e));
    e->track = track;
    e->is_end = true;
    e->group_seq = track->group_seq;
    s->preq_tail++;

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
    snap.objects_queued = ms->preq_tail - ms->preq_head;
    snap.bytes_queued = ms->preq_bytes;
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
    bool r = ms->ready;
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
    pthread_mutex_unlock(&s->mu);
    if (fatal || moq_endpoint_is_closed(s->ep)) return MOQ_ERR_CLOSED;
    if (level) return MOQ_OK;

    moq_result_t rc = moq_endpoint_wait(s->ep, timeout_us);
    if (rc == MOQ_ERR_INTERRUPTED || rc == MOQ_DONE) return rc;
    if (moq_endpoint_interrupted_internal(s->ep)) return MOQ_ERR_INTERRUPTED;

    pthread_mutex_lock(&s->mu);
    level = sender_wait_level(s);
    fatal = s->fatal;
    pthread_mutex_unlock(&s->mu);
    if (fatal || moq_endpoint_is_closed(s->ep)) return MOQ_ERR_CLOSED;
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

moq_media_sender_t *moq_media_sender_test_new(void)
{
    const moq_alloc_t *alloc = moq_alloc_default();
    moq_media_sender_t *s = (moq_media_sender_t *)alloc->alloc(
        sizeof(*s), alloc->ctx);
    if (!s) return NULL;
    memset(s, 0, sizeof(*s));
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

/* Simulate the post-ready transition (so add/remove dirty the catalog). */
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
    moq_result_t rc = sender_build_catalog(s, out, false);
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

/* Snapshot the current catalog as the "published" baseline (simulates a prior
 * committed generation). */
void moq_media_sender_test_mark_published(moq_media_sender_t *s)
{
    pthread_mutex_lock(&s->mu);
    moq_rcbuf_t *j = NULL;
    if (sender_build_catalog(s, &j, false) == MOQ_OK && j) {
        if (s->published_catalog) moq_rcbuf_decref(s->published_catalog);
        s->published_catalog = j;
        /* A committed generation also adopts the current tuple set as the
         * deltaUpdate baseline (in_published) -- mirror that so the next stage
         * diffs against it instead of treating every track as a fresh add. */
        sender_commit_baseline(s);
    }
    pthread_mutex_unlock(&s->mu);
}

/* Whether a republish now would actually cut a new generation: true iff the
 * resolved catalog differs from the published baseline. Mirrors the no-op
 * dedup decision in sender_republish_catalog. */
bool moq_media_sender_test_build_changed(moq_media_sender_t *s)
{
    pthread_mutex_lock(&s->mu);
    moq_rcbuf_t *j = NULL;
    bool changed = true;
    if (sender_build_catalog(s, &j, false) == MOQ_OK && j) {
        changed = !rcbuf_bytes_eq(j, s->published_catalog);
        moq_rcbuf_decref(j);
    }
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
    s->pending_obj_count = 0;
    s->pending_obj_cursor = 0;
    s->pending_retained_set = false;
    pthread_mutex_unlock(&s->mu);
    return out;
}
#endif /* MOQ_MEDIA_SENDER_TESTING */
