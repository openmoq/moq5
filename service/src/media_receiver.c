/*
 * moq_media_receiver_t — receive-side media facade (design §6).
 *
 * Threading model: the receiver registers an internal endpoint pump hook
 * (endpoint_internal.h) that runs on the endpoint's network thread. ALL
 * session/subscriber work happens inside that hook. The application thread
 * only touches the mutex-protected track-event queue, the terminal flags,
 * and the endpoint's public wait/latch surface. The effective catalog and the
 * handle list are written exclusively by the hook (including across catalog
 * updates: handles are appended under r->mu and individual handles' desc bytes
 * are handle-owned and immutable once built); destroy frees everything after
 * detaching (no concurrency remains, which is also what makes the non-atomic
 * rcbuf refcounts safe to release on the destroying thread).
 */

#include <moq/media_receiver.h>
#include <moq/subscriber.h>
#include <moq/msf.h>
#include <moq/msf_media.h>
#include <moq/media_object.h>

#include "endpoint_internal.h"

#include <pthread.h>
#include <stdatomic.h>
#include <stddef.h>
#include <string.h>

#define RECEIVER_DEFAULT_TRACK_EVENTS 64u
#define RECEIVER_SUB_MAX_TRACKS  32u  /* catalog + auto-subscribed tracks */
#define RECEIVER_SUB_MAX_OBJECTS 64u
#define RECEIVER_DEFAULT_MAX_OBJECTS 256u
#define RECEIVER_DEFAULT_MAX_BYTES   (32u * 1024u * 1024u)
#define RECEIVER_PARSE_STACK_SAMPLES 64u
#define RECEIVER_DEFAULT_SAP_RECORDS 256u  /* bounded SAP-record queue depth */
#define RECEIVER_DEFAULT_MT_RECORDS  256u  /* bounded media-timeline queue depth */
/* Receiver-lifetime caps on retained catalog-update state (availability): a peer
 * sending many valid independent catalog generations must not grow stable track
 * handles or retained CP snapshots without bound. Conservative defaults --
 * legitimate catalogs hold a handful of stable tracks and few CP generations;
 * these only bite a churning/abusive peer. */
#define RECEIVER_DEFAULT_MAX_STABLE_HANDLES 1024u
#define RECEIVER_DEFAULT_MAX_CP_SNAPS         64u
#define RECEIVER_DEFAULT_MAX_RETAINED_BYTES (16u * 1024u * 1024u)

/* SUBSCRIBE_DONE "Track Ended" status (draft-ietf-moq-transport SUBSCRIBE_DONE
 * status codes; the publisher emits it as MOQ_PUB_DONE_TRACK_ENDED for an MSF
 * §11.3 live->VOD conversion). Non-terminal: the track stays joinable. */
#define RECEIVER_DONE_TRACK_ENDED 0x2u

/* Frozen v0 base sizes for the caller-filled output structs: the minimum a
 * caller must provide. Computed from the last v0 field's offset so they stay
 * platform-correct AND constant when future versions APPEND fields -- an older
 * caller passing its smaller size still receives the v0 prefix. These must NOT
 * change when fields are appended; new fields get their own (larger) base.
 * Validation uses these (not sizeof(current)); writes are clamped to
 * min(caller_size, sizeof(current)). */
#define MEDIA_TRACK_EVENT_V0_SIZE \
    (offsetof(moq_media_track_event_t, config_generation) + sizeof(uint32_t))
#define MEDIA_OBJECT_V0_SIZE \
    (offsetof(moq_media_object_t, samples_owned) + sizeof(moq_cmaf_sample_t *))
#define MEDIA_RECEIVER_STATS_V0_SIZE \
    (offsetof(moq_media_receiver_stats_t, paused) + sizeof(bool))
/* Appended-field generations (the v0 prefix stays frozen at `paused`).
 * get_stats validates against v0 and clamps writes to min(caller, current), so
 * a caller passing one of these sizes gets exactly that prefix. */
#define MEDIA_RECEIVER_STATS_CATALOG_DROPS_SIZE \
    (offsetof(moq_media_receiver_stats_t, catalog_drops) + sizeof(uint64_t))
#define MEDIA_RECEIVER_STATS_CATALOG_COMPLETE_SIZE \
    (offsetof(moq_media_receiver_stats_t, catalog_complete) + sizeof(bool))
#define MEDIA_SAP_RECORD_V0_SIZE \
    (offsetof(moq_media_sap_record_t, ept_ms) + sizeof(uint64_t))
#define MEDIA_TIMELINE_RECORD_V0_SIZE \
    (offsetof(moq_media_timeline_record_t, wallclock_ms) + sizeof(uint64_t))

/* -- Internal types --------------------------------------------------- */

struct moq_media_track {
    moq_media_track_desc_t desc;
    moq_rcbuf_t           *init_buf;   /* owned; desc.init/init_data borrow */

    /* Handle-owned desc backing (so a handle outlives the effective catalog it
     * was built from -- catalogs are replaced on each update). desc string
     * spans point into desc_strings; desc.depends / .content_protection_ref_ids
     * point into the owned arrays. */
    uint8_t          *desc_strings;
    size_t            desc_strings_size;
    moq_bytes_t      *desc_depends_arr;
    moq_bytes_t      *desc_cp_refs_arr;

    /* Owned MSF tuple key (namespace + name), the catalog identity used to match
     * a handle across updates. Namespace is not a desc field, so it is kept here
     * (bytes live in desc_strings). MSF allows the same local name in different
     * namespaces, so matching MUST use the full tuple, never name alone. */
    bool              key_has_ns;
    moq_bytes_t       key_ns;          /* points into desc_strings */

    bool              removed;         /* TRACK_REMOVED emitted; terminal handle */
    bool              recon_seen;       /* reconcile-walk scratch (network thread) */

    /* Desired subscription state (app intent). Written by the public
     * subscribe/unsubscribe calls under r->mu; read by the network-thread
     * hook which reconciles it against the actual subscription. */
    bool             desired_subscribed;
    moq_subscribe_filter_t desired_filter;   /* start point at (re)subscribe */
    bool             desired_has_priority;
    uint8_t          desired_priority;

    /* Data path (network-thread state). */
    moq_sub_track_t *sub_track;        /* non-NULL while subscribed */
    bool             ended;            /* TRACK_ENDED emitted */
    /* SUBSCRIBE_DONE handling (network thread). on_subscribe_done records the
     * completion here and the pump processes it AFTER draining objects, so media
     * delivered in the same tick as the done is not lost. vod_rearmed is a
     * one-shot guard: a "Track Ended" (live->VOD step 1) re-subscribes once for
     * the now-VOD content; any further done is treated as terminal, so a peer
     * that keeps finishing cannot spin a re-subscribe loop (MSF §11.3). */
    bool             done_pending;
    uint64_t         done_status;
    bool             vod_rearmed;
    bool             has_drop_filter;  /* monotonic: arrivals with
                                          group_id <= drop_below_group are
                                          stragglers of evicted content.
                                          DROP_TO_KEYFRAME exempts keyframes
                                          (an arriving sync point restarts
                                          the chain and clears the filter);
                                          the group policies drop the whole
                                          group. */
    uint64_t         drop_below_group;
    bool             walk_key_seen;    /* compaction-walk scratch */
    uint64_t         walk_key_group;
    bool             forward_sent;     /* last forward state sent (pause) */
    bool             forward_desired;

    /* Subscription lifecycle, mu-published so the public track_state accessor
     * never races the network-thread-only sub_track pointer:
     *   requested   -- a SUBSCRIBE has been issued (reconcile); PENDING until OK.
     *   established  -- SUBSCRIBE_OK received (on_subscribed callback); ACTIVE.
     * Both persist once set: a disable keeps the subscription (paused), so a
     * re-enable stays ACTIVE rather than going back to PENDING. */
    bool             requested;
    bool             established;

    /* Timeline classification (network thread). is_event_timeline is any
     * packaging=="eventtimeline" track; is_media_timeline is any
     * packaging=="mediatimeline" track (MSF §7.2). Neither is auto-subscribed
     * and neither is fed to the media object parser. is_sap_timeline is the CMSF
     * SAP subtype of an event timeline (eventType "org.ietf.moq.cmsf.sap" +
     * mimeType "application/json"); ONLY those route to the SAP parser and
     * surface via poll_sap. Any other timeline object is dropped (not SAP, not
     * media -- the explicit media-timeline payload codec is not implemented).
     * sap_last tracks the highest media Location surfaced so §8.3 independent
     * re-sends de-dup. */
    bool             is_event_timeline;
    bool             is_media_timeline;
    bool             is_sap_timeline;
    bool             sap_has_last;
    uint64_t         sap_last_group;
    uint64_t         sap_last_object;
    /* §7.3 media-timeline dedup: last surfaced media Location for this track, so
     * an independent re-send of accumulated records does not re-surface them. */
    bool             mt_has_last;
    uint64_t         mt_last_group;
    uint64_t         mt_last_object;
};

typedef struct receiver_event {
    moq_media_track_event_kind_t kind;
    moq_media_track_t           *track;
} receiver_event_t;

struct moq_media_receiver {
    moq_alloc_t     alloc;
    moq_endpoint_t *ep;
    bool            owns_endpoint;

    /* Owned copies of the configuration inputs. */
    moq_bytes_t    *ns_parts;
    uint8_t        *ns_data;
    size_t          ns_data_size;
    moq_namespace_t namespace_;
    uint8_t        *catalog_name;
    size_t          catalog_name_len;
    bool            auto_subscribe;       /* stored; data path acts on it */
    moq_media_time_mode_t time_mode;
    moq_media_overflow_cfg_t overflow;    /* stored; data path consumes it */

    /* Network-thread state. */
    moq_subscriber_t  *sub;
    moq_sub_track_t   *catalog_sub;
    moq_sub_fetch_req_t *catalog_fetch;   /* joining FETCH(offset=0); MSF-01 §5 */
    bool               catalog_fetch_issued;  /* issued exactly once */

    /* The current effective independent catalog (MSF-01 §5). `has_effective`
     * gates first-catalog handling; `effective` is the base for the next update
     * and what find_content_protection resolves against. Replaced on each
     * accepted update; the previous one is freed unless it carried
     * contentProtections, in which case it is retained in `cp_snaps` so any
     * pointer find_content_protection previously returned stays valid for the
     * receiver's life. */
    bool               has_effective;
    moq_msf_catalog_t  effective;
    moq_msf_catalog_t *cp_snaps;          /* retained old effectives w/ CP */
    size_t             cp_snap_count, cp_snap_cap;

    /* Catalog-update ordering (§5): the latest group seen and whether we are
     * desynced (a dropped/failed update; in-group deltas are then ignored until
     * the next independent object 0 re-syncs). */
    uint64_t           latest_group;
    bool               have_latest_group;
    bool               desynced;
    uint64_t           cur_obj;       /* highest catalog object id applied in
                                         latest_group (0 after its independent;
                                         deltas advance it 1,2,3...) */

    /* Handle list: an array of individually-allocated handles, so the array can
     * grow across updates without moving a handle a caller holds. */
    moq_media_track_t **tracks;
    size_t             track_count, track_cap;

    /* Receiver-lifetime caps on retained catalog-update state (availability).
     * Stable handles (incl. removed ones, kept alive per the public lifetime
     * contract) and CP snapshots are never freed before destroy, so a peer
     * feeding endless valid generations could grow them without bound. Caps are
     * preflighted before any mutation; once an effective catalog exists, an
     * over-cap update is a bounded drop (catalog_drops, current effective kept),
     * and a first catalog over cap terminalizes CATALOG_UNUSABLE. retained_bytes
     * is the estimated bytes held by stable handles + CP snapshots; it only ever
     * grows. Defaults are conservative; unit tests lower them via the seam. */
    size_t             max_stable_handles;
    size_t             max_cp_snaps;
    uint64_t           max_retained_bytes;
    uint64_t           retained_bytes;

    /* Data-path config + network-thread state. */
    uint32_t         obj_cap;     /* policy threshold */
    uint32_t         ring_cap;    /* physical ring size (>= obj_cap;
                                     FLOW_CONTROL's absorb ceiling) */
    uint64_t         byte_cap;
    bool             subscribed_media;   /* >= 1 media subscription issued */
    bool             paused;             /* FLOW_CONTROL pause in effect */
    bool             resume_nudged;      /* wake task posted, not yet seen
                                            by the hook (mu-protected) */
    bool             epoch_set;          /* SHARED_EPOCH anchor latched */
    uint64_t         epoch_us;

    /* App-visible state (mu-protected). */
    pthread_mutex_t  mu;
    receiver_event_t *events;
    uint32_t         ev_cap, ev_head, ev_tail;
    struct receiver_obj_entry *objs;     /* ring; entries own their refs */
    uint64_t         obj_sizes_total;    /* queued payload bytes */
    uint32_t         obj_head, obj_tail;

    /* Bounded SAP-record queue (CMSF §3.6.1). Plain value records (no refs);
     * drops oldest when the app under-polls. */
    moq_media_sap_record_t *saps;
    uint32_t         sap_cap, sap_head, sap_tail;
    /* Bounded media-timeline record queue (MSF §7.1.1). Same shape as the SAP
     * queue: plain values, drop-oldest under-poll. */
    moq_media_timeline_record_t *mts;
    uint32_t         mt_cap, mt_head, mt_tail;
    bool             fatal;
    uint64_t         fatal_code;
    moq_media_receiver_stats_t stats;    /* counter fields only */
};

typedef struct receiver_obj_entry {
    moq_media_object_t obj;
    uint64_t           group_id;   /* wire identity for group-aware drops */
} receiver_obj_entry_t;

/* Release one queue entry's owned buffers (any thread that has exclusive
 * access: the network thread for evictions, the destroying thread once
 * detached). */
static void obj_entry_release(moq_media_receiver_t *r,
                              moq_media_object_t *o)
{
    if (o->payload_ref)    moq_rcbuf_decref(o->payload_ref);
    if (o->properties_ref) moq_rcbuf_decref(o->properties_ref);
    if (o->samples_owned)
        r->alloc.free(o->samples_owned,
                      o->sample_count * sizeof(moq_cmaf_sample_t),
                      r->alloc.ctx);
    memset(o, 0, sizeof(*o));
}

static size_t obj_entry_bytes(const moq_media_object_t *o)
{
    return o->payload_ref ? moq_rcbuf_len(o->payload_ref) : 0;
}

/* -- Event queue (r->mu) ----------------------------------------------- */

static bool rq_push(moq_media_receiver_t *r, moq_media_track_event_kind_t kind,
                    moq_media_track_t *track)
{
    pthread_mutex_lock(&r->mu);
    if (r->ev_tail - r->ev_head >= r->ev_cap) {
        /* Losing a discovery event is unrecoverable miswiring (§6.3):
         * terminalize, never drop silently. */
        r->fatal = true;
        r->fatal_code = MOQ_MEDIA_RECEIVER_FATAL_EVENT_OVERFLOW;
        pthread_mutex_unlock(&r->mu);
        return false;
    }
    receiver_event_t *e = &r->events[r->ev_tail % r->ev_cap];
    e->kind = kind;
    e->track = track;
    r->ev_tail++;
    pthread_mutex_unlock(&r->mu);
    return true;
}

static void receiver_set_fatal(moq_media_receiver_t *r, uint64_t code)
{
    pthread_mutex_lock(&r->mu);
    if (!r->fatal) {
        r->fatal = true;
        r->fatal_code = code;
    }
    pthread_mutex_unlock(&r->mu);
}

/* -- Object queue + overflow (network thread; r->mu inside) ------------ */

static moq_media_track_t *track_for_sub(moq_media_receiver_t *r,
                                        moq_sub_track_t *st)
{
    for (size_t i = 0; i < r->track_count; i++)
        if (r->tracks[i]->sub_track == st) return r->tracks[i];
    return NULL;
}

/* Evict the queue head, counting it, and arm the owning track's
 * straggler filter (monotonic: every group at or below the evicted one
 * is dropped content). Caller holds r->mu. */
static void obj_evict_head_locked(moq_media_receiver_t *r)
{
    receiver_obj_entry_t *h = &r->objs[r->obj_head % r->ring_cap];
    moq_media_track_t *t = h->obj.track;
    if (t && (!t->has_drop_filter || h->group_id > t->drop_below_group)) {
        t->has_drop_filter = true;
        t->drop_below_group = h->group_id;
    }
    r->obj_sizes_total -= obj_entry_bytes(&h->obj);
    r->stats.objects_dropped++;
    if (h->obj.keyframe) r->stats.keyframes_dropped++;
    obj_entry_release(r, &h->obj);
    r->obj_head++;
}

/* Drop one mid-queue entry during a compaction walk (counting). Caller
 * holds r->mu. */
static void obj_release_entry_locked(moq_media_receiver_t *r,
                                     receiver_obj_entry_t *e)
{
    r->obj_sizes_total -= obj_entry_bytes(&e->obj);
    r->stats.objects_dropped++;
    if (e->obj.keyframe) r->stats.keyframes_dropped++;
    obj_entry_release(r, &e->obj);
}

/* Compact the queue, keeping only entries `keep()` accepts; relative
 * order is preserved. Subgroup streams deliver concurrently, so entries
 * of an evicted group can sit ANYWHERE in the queue, not just in the
 * head run -- every purge must walk the whole ring. Caller holds r->mu. */
typedef bool (*obj_keep_fn)(moq_media_receiver_t *r,
                            const receiver_obj_entry_t *e, void *ctx);

static void obj_compact_locked(moq_media_receiver_t *r,
                               obj_keep_fn keep, void *ctx)
{
    uint32_t rd = r->obj_head, wr = r->obj_head;
    while (rd != r->obj_tail) {
        receiver_obj_entry_t *e = &r->objs[rd % r->ring_cap];
        if (keep(r, e, ctx)) {
            if (wr != rd) {
                r->objs[wr % r->ring_cap] = *e;
                memset(e, 0, sizeof(*e));
            }
            wr++;
        } else {
            obj_release_entry_locked(r, e);
            memset(e, 0, sizeof(*e));
        }
        rd++;
    }
    r->obj_tail = wr;
}

typedef struct {
    moq_media_track_t *track;
    uint64_t           group;
} purge_group_ctx_t;

static bool keep_not_group(moq_media_receiver_t *r,
                           const receiver_obj_entry_t *e, void *ctx)
{
    (void)r;
    const purge_group_ctx_t *p = (const purge_group_ctx_t *)ctx;
    return e->obj.track != p->track || e->group_id != p->group;
}

/* Keep everything except entries belonging to `ctx` (a track handle): used to
 * purge queued objects when a track is unsubscribed. */
static bool keep_not_track(moq_media_receiver_t *r,
                           const receiver_obj_entry_t *e, void *ctx)
{
    (void)r;
    return e->obj.track != (const moq_media_track_t *)ctx;
}

/* Evict the head's entire group, wherever its entries sit in the queue
 * (a partial group is useless to a decoder), and arm the straggler
 * filter so in-flight remnants are discarded too. Caller holds r->mu. */
static void obj_evict_head_group_locked(moq_media_receiver_t *r,
                                        uint64_t *out_group,
                                        moq_media_track_t **out_track)
{
    receiver_obj_entry_t *h = &r->objs[r->obj_head % r->ring_cap];
    moq_media_track_t *track = h->obj.track;
    *out_track = track;
    *out_group = h->group_id;
    purge_group_ctx_t ctx = { track, h->group_id };
    obj_compact_locked(r, keep_not_group, &ctx);
    r->stats.groups_dropped++;
}

/* DROP_TO_KEYFRAME chain compaction: after a prefix eviction, a delta is
 * decodable only if its group's chain is still intact -- through a
 * keyframe kept earlier in the walk, or because the group was never
 * touched by an eviction (its older objects were already delivered).
 * Caller holds r->mu; uses the per-track walk scratch. */
static bool keep_decodable(moq_media_receiver_t *r,
                           const receiver_obj_entry_t *e, void *ctx)
{
    (void)r; (void)ctx;
    moq_media_track_t *t = e->obj.track;
    if (!t) return true;
    if (e->obj.keyframe) {
        t->walk_key_seen = true;
        t->walk_key_group = e->group_id;
        return true;
    }
    if (t->walk_key_seen && e->group_id == t->walk_key_group)
        return true;
    return !t->has_drop_filter || e->group_id > t->drop_below_group;
}

static void obj_compact_to_keyframes_locked(moq_media_receiver_t *r)
{
    for (size_t i = 0; i < r->track_count; i++)
        r->tracks[i]->walk_key_seen = false;
    obj_compact_locked(r, keep_decodable, NULL);
}

/* The per-track straggler filter, applied to an arriving (already
 * parsed) object. DROP_TO_KEYFRAME exempts keyframes: an arriving sync
 * point restarts the decode chain and clears the filter. The group
 * policies drop everything of a dropped-or-older group; a newer group
 * clears. Caller holds r->mu. */
static bool obj_filtered_locked(moq_media_receiver_t *r,
                                moq_media_track_t *track,
                                uint64_t group_id, bool keyframe)
{
    if (!track->has_drop_filter) return false;
    if (r->overflow.policy == MOQ_MEDIA_OVERFLOW_DROP_TO_KEYFRAME) {
        if (keyframe) {
            track->has_drop_filter = false;
            return false;
        }
        return group_id <= track->drop_below_group;
    }
    if (group_id > track->drop_below_group) {
        track->has_drop_filter = false;
        return false;
    }
    return true;
}

/* Make room for one incoming object of `need` bytes per the configured
 * policy. Returns true when the object should be enqueued, false when the
 * policy says discard it (already counted). Caller holds r->mu. */
static bool obj_make_room_locked(moq_media_receiver_t *r,
                                 moq_media_track_t *track,
                                 uint64_t group_id, bool keyframe,
                                 size_t need)
{
    bool full = (r->obj_tail - r->obj_head >= r->obj_cap) ||
                (r->obj_sizes_total + need > r->byte_cap);
    if (!full) return true;

    r->stats.overflow_events++;

    switch (r->overflow.policy) {
    case MOQ_MEDIA_OVERFLOW_DROP_TO_KEYFRAME:
        /* Evict from the oldest end until there is room, advance the
         * head to a decodable start (keyframe), then purge mid-queue
         * deltas whose chain broke (subgroup streams interleave, so an
         * evicted group's remnants can sit behind a newer keyframe).
         * Never plain drop-oldest: a keyframe is never evicted while
         * its deltas stay. */
        while (r->obj_tail != r->obj_head &&
               ((r->obj_tail - r->obj_head >= r->obj_cap) ||
                (r->obj_sizes_total + need > r->byte_cap)))
            obj_evict_head_locked(r);
        while (r->obj_tail != r->obj_head &&
               !r->objs[r->obj_head % r->ring_cap].obj.keyframe)
            obj_evict_head_locked(r);
        obj_compact_to_keyframes_locked(r);
        break;

    case MOQ_MEDIA_OVERFLOW_DROP_GROUP:
        /* Discard the oldest whole group (wherever its entries sit);
         * in-flight stragglers are discarded by the per-track filter. */
        while (r->obj_tail != r->obj_head &&
               ((r->obj_tail - r->obj_head >= r->obj_cap) ||
                (r->obj_sizes_total + need > r->byte_cap))) {
            uint64_t g; moq_media_track_t *t;
            obj_evict_head_group_locked(r, &g, &t);
            if (t && (!t->has_drop_filter || g > t->drop_below_group)) {
                t->has_drop_filter = true;
                t->drop_below_group = g;
            }
        }
        break;

    case MOQ_MEDIA_OVERFLOW_FLOW_CONTROL: {
        /* Pause upstream (handled by the hook seeing r->paused) and absorb
         * the in-flight overshoot up to a soft ceiling standing in for the
         * session receive budget; past it the receiver degrades to a
         * counted group drop -- never a connection teardown. */
        if (!r->paused) {
            r->paused = true;
            r->stats.pause_transitions++;
        }
        uint32_t ceiling = r->obj_cap + r->obj_cap / 2;
        uint64_t byte_ceiling = r->byte_cap + r->byte_cap / 2;
        if ((r->obj_tail - r->obj_head < ceiling) &&
            (r->obj_sizes_total + need <= byte_ceiling))
            return true;
        if (r->obj_tail != r->obj_head) {
            uint64_t g; moq_media_track_t *t;
            obj_evict_head_group_locked(r, &g, &t);
            if (t && (!t->has_drop_filter || g > t->drop_below_group)) {
                t->has_drop_filter = true;
                t->drop_below_group = g;
            }
        }
        break;
    }

    default:
        return true;   /* unreachable: policy validated at create */
    }

    /* The eviction may have taken the incoming object's own chain with
     * it -- it is subject to the same straggler rule as a late arrival. */
    if (obj_filtered_locked(r, track, group_id, keyframe))
        return false;

    /* Hard budget: eviction empties at most the whole queue, so an
     * object that alone exceeds the byte budget (or the FLOW_CONTROL
     * ceiling) can never fit. Drop it and arm the straggler filter --
     * the rest of its group is undecodable without it. */
    uint64_t byte_limit = r->byte_cap;
    if (r->overflow.policy == MOQ_MEDIA_OVERFLOW_FLOW_CONTROL)
        byte_limit = r->byte_cap + r->byte_cap / 2;
    if (r->obj_sizes_total + need > byte_limit) {
        if (!track->has_drop_filter || group_id > track->drop_below_group) {
            track->has_drop_filter = true;
            track->drop_below_group = group_id;
        }
        return false;
    }
    return true;
}

/* -- Catalog processing (network thread) -------------------------------- */

static bool span_eq(moq_bytes_t a, moq_bytes_t b)
{
    if (a.len != b.len) return false;
    return a.len == 0 || memcmp(a.data, b.data, a.len) == 0;
}

static bool span_eq_lit(moq_bytes_t a, const char *lit, size_t len)
{
    return a.len == len && memcmp(a.data, lit, len) == 0;
}

static void receiver_track_free(moq_media_receiver_t *r, moq_media_track_t *t)
{
    if (!t) return;
    if (t->init_buf) moq_rcbuf_decref(t->init_buf);
    if (t->desc_depends_arr)
        r->alloc.free(t->desc_depends_arr,
                      t->desc.depends_count * sizeof(moq_bytes_t), r->alloc.ctx);
    if (t->desc_cp_refs_arr)
        r->alloc.free(t->desc_cp_refs_arr,
                      t->desc.content_protection_ref_id_count *
                          sizeof(moq_bytes_t), r->alloc.ctx);
    if (t->desc_strings)
        r->alloc.free(t->desc_strings, t->desc_strings_size, r->alloc.ctx);
    r->alloc.free(t, sizeof(*t), r->alloc.ctx);
}

/* Build a handle from a catalog track, copying every desc span into
 * handle-owned storage so the handle outlives the effective catalog. `src` is
 * the source catalog used to resolve an initRef. Returns NULL on allocation
 * failure. */
static moq_media_track_t *receiver_track_build(moq_media_receiver_t *r,
                                               const moq_msf_track_t *mt,
                                               const moq_msf_catalog_t *src)
{
    moq_media_track_t *t = (moq_media_track_t *)r->alloc.alloc(
        sizeof(*t), r->alloc.ctx);
    if (!t) return NULL;
    memset(t, 0, sizeof(*t));
    moq_media_track_desc_t *d = &t->desc;
    d->struct_size = (uint32_t)sizeof(*d);

    /* One owned buffer for all string spans + the depends/cp-ref bytes + the
     * tuple-key namespace. */
    size_t need = mt->name.len + mt->packaging.len;
    if (mt->has_namespace) need += mt->namespace_.len;
    if (mt->has_role) need += mt->role.len;
    if (mt->has_codec) need += mt->codec.len;
    if (mt->has_lang) need += mt->lang.len;
    if (mt->has_label) need += mt->label.len;
    if (mt->has_channel_config) need += mt->channel_config.len;
    if (mt->has_event_type) need += mt->event_type.len;
    if (mt->has_mime_type) need += mt->mime_type.len;
    for (size_t i = 0; i < mt->depends_count; i++) need += mt->depends[i].len;
    for (size_t i = 0; i < mt->cp_ref_id_count; i++) need += mt->cp_ref_ids[i].len;

    if (need > 0) {
        t->desc_strings = (uint8_t *)r->alloc.alloc(need, r->alloc.ctx);
        if (!t->desc_strings) { receiver_track_free(r, t); return NULL; }
        t->desc_strings_size = need;
    }
    if (mt->depends_count > 0) {
        t->desc_depends_arr = (moq_bytes_t *)r->alloc.alloc(
            mt->depends_count * sizeof(moq_bytes_t), r->alloc.ctx);
        if (!t->desc_depends_arr) { receiver_track_free(r, t); return NULL; }
    }
    if (mt->cp_ref_id_count > 0) {
        t->desc_cp_refs_arr = (moq_bytes_t *)r->alloc.alloc(
            mt->cp_ref_id_count * sizeof(moq_bytes_t), r->alloc.ctx);
        if (!t->desc_cp_refs_arr) { receiver_track_free(r, t); return NULL; }
    }

    size_t off = 0;
#define COPY_DESC_SPAN(dst, src_span) do { \
        if ((src_span).len) { \
            memcpy(t->desc_strings + off, (src_span).data, (src_span).len); \
            (dst) = (moq_bytes_t){ t->desc_strings + off, (src_span).len }; \
            off += (src_span).len; \
        } } while (0)
    COPY_DESC_SPAN(d->name, mt->name);
    COPY_DESC_SPAN(d->packaging, mt->packaging);
    if (mt->has_namespace) {
        t->key_has_ns = true;
        COPY_DESC_SPAN(t->key_ns, mt->namespace_);
    }
    if (mt->has_role) COPY_DESC_SPAN(d->role, mt->role);
    if (mt->has_codec) COPY_DESC_SPAN(d->codec, mt->codec);
    if (mt->has_lang) COPY_DESC_SPAN(d->lang, mt->lang);
    if (mt->has_label) COPY_DESC_SPAN(d->label, mt->label);
    if (mt->has_channel_config) COPY_DESC_SPAN(d->channel_config, mt->channel_config);
    if (mt->has_event_type) COPY_DESC_SPAN(d->event_type, mt->event_type);
    if (mt->has_mime_type) COPY_DESC_SPAN(d->mime_type, mt->mime_type);
    for (size_t i = 0; i < mt->depends_count; i++)
        COPY_DESC_SPAN(t->desc_depends_arr[i], mt->depends[i]);
    for (size_t i = 0; i < mt->cp_ref_id_count; i++)
        COPY_DESC_SPAN(t->desc_cp_refs_arr[i], mt->cp_ref_ids[i]);
#undef COPY_DESC_SPAN

    d->depends = t->desc_depends_arr;
    d->depends_count = mt->depends_count;
    d->content_protection_ref_ids = t->desc_cp_refs_arr;
    d->content_protection_ref_id_count = mt->cp_ref_id_count;

    d->has_width = mt->has_width;             d->width = mt->width;
    d->has_height = mt->has_height;           d->height = mt->height;
    d->has_samplerate = mt->has_samplerate;   d->samplerate = mt->samplerate;
    d->has_framerate = mt->has_framerate;     d->framerate_millis = mt->framerate_millis;
    d->has_bitrate = mt->has_bitrate;         d->bitrate = mt->bitrate;
    d->has_max_grp_sap = mt->has_max_grp_sap; d->max_grp_sap = mt->max_grp_sap;
    d->has_max_obj_sap = mt->has_max_obj_sap; d->max_obj_sap = mt->max_obj_sap;
    /* MSF §7.4 inline template: a plain value, no backing allocation. */
    d->has_template = mt->has_template;       d->template_ = mt->template_;
    /* MSF §5.2.35 track duration (VOD/ended tracks). */
    d->has_track_duration = mt->has_track_duration;
    d->track_duration_ms = mt->track_duration_ms;
    d->is_live = mt->is_live;                 /* §5.2.7 (mutable via §11.3) */

    t->is_event_timeline = span_eq_lit(d->packaging, "eventtimeline", 13);
    t->is_media_timeline = span_eq_lit(d->packaging, "mediatimeline", 13);
    t->is_sap_timeline = t->is_event_timeline &&
        mt->has_event_type && span_eq_lit(d->event_type, "org.ietf.moq.cmsf.sap", 21) &&
        mt->has_mime_type && span_eq_lit(d->mime_type, "application/json", 16);

    moq_media_track_info_init(&d->info);
    moq_cmaf_init_info_init(&d->init);

    /* Resolve init: inline initData wins, else follow initRef into src's
     * initDataList. The decoded bytes are owned via init_buf, so init/init_data
     * stay valid after src is freed. */
    moq_msf_track_t rt = *mt;
    if (!(rt.has_init_data && rt.init_data.len > 0) && rt.has_init_ref) {
        const moq_msf_init_data_entry_t *e =
            moq_msf_catalog_find_init_data(src, rt.init_ref);
        if (e && e->data.len > 0) { rt.has_init_data = true; rt.init_data = e->data; }
    }
    moq_result_t irc;
    if (rt.has_init_data && rt.init_data.len > 0) {
        irc = moq_msf_track_to_media_info(&r->alloc, &rt, &d->info,
                                          &d->init, &t->init_buf);
        if (irc == MOQ_OK && t->init_buf) {
            d->has_init = true;
            d->init_data = (moq_bytes_t){ moq_rcbuf_data(t->init_buf),
                                          moq_rcbuf_len(t->init_buf) };
        }
    } else {
        irc = moq_msf_track_to_media_info(&r->alloc, &rt, &d->info, NULL, NULL);
    }
    if (irc != MOQ_OK) {
        moq_media_track_info_init(&d->info);
        moq_cmaf_init_info_init(&d->init);
        d->has_init = false;
    }
    return t;
}

/* Append a handle to the growable handle pointer array. */
static bool receiver_tracks_push(moq_media_receiver_t *r, moq_media_track_t *t)
{
    if (r->track_count == r->track_cap) {
        size_t nc = r->track_cap ? r->track_cap * 2 : 8;
        moq_media_track_t **na = (moq_media_track_t **)r->alloc.alloc(
            nc * sizeof(moq_media_track_t *), r->alloc.ctx);
        if (!na) return false;
        if (r->tracks) {
            memcpy(na, r->tracks, r->track_count * sizeof(moq_media_track_t *));
            r->alloc.free(r->tracks, r->track_cap * sizeof(moq_media_track_t *),
                          r->alloc.ctx);
        }
        r->tracks = na;
        r->track_cap = nc;
    }
    r->tracks[r->track_count++] = t;
    return true;
}

/* Find a live (non-removed) handle by the full MSF tuple (namespace, name),
 * compared against each handle's own stored key. MSF allows the same local name
 * in different namespaces, so the namespace presence and bytes must both match. */
static moq_media_track_t *receiver_find_live(moq_media_receiver_t *r,
                                             moq_bytes_t name,
                                             bool has_ns, moq_bytes_t ns)
{
    for (size_t i = 0; i < r->track_count; i++) {
        moq_media_track_t *t = r->tracks[i];
        if (t->removed) continue;
        if (!span_eq(t->desc.name, name)) continue;
        if (t->key_has_ns != has_ns) continue;
        if (has_ns && !span_eq(t->key_ns, ns)) continue;
        return t;
    }
    return NULL;
}

/* Compare two catalog tracks' metadata EXCEPT the two fields a live->VOD
 * conversion (§11.3) may change -- is_live and trackDuration. Everything else is
 * immutable (§5.3); classify_track_change() layers the VOD exception on top. */
static bool meta_equal_except_vod(const moq_msf_track_t *a,
                                  const moq_msf_track_t *b)
{
    if (a->has_namespace != b->has_namespace ||
        (a->has_namespace && !span_eq(a->namespace_, b->namespace_))) return false;
    if (!span_eq(a->packaging, b->packaging)) return false;
    if (a->has_role != b->has_role || (a->has_role && !span_eq(a->role, b->role))) return false;
    if (a->has_codec != b->has_codec || (a->has_codec && !span_eq(a->codec, b->codec))) return false;
    if (a->has_init_data != b->has_init_data || (a->has_init_data && !span_eq(a->init_data, b->init_data))) return false;
    if (a->has_init_track != b->has_init_track || (a->has_init_track && !span_eq(a->init_track, b->init_track))) return false;
    if (a->has_init_ref != b->has_init_ref || (a->has_init_ref && !span_eq(a->init_ref, b->init_ref))) return false;
    if (a->has_event_type != b->has_event_type || (a->has_event_type && !span_eq(a->event_type, b->event_type))) return false;
    if (a->has_mime_type != b->has_mime_type || (a->has_mime_type && !span_eq(a->mime_type, b->mime_type))) return false;
    if (a->has_channel_config != b->has_channel_config || (a->has_channel_config && !span_eq(a->channel_config, b->channel_config))) return false;
    if (a->has_lang != b->has_lang || (a->has_lang && !span_eq(a->lang, b->lang))) return false;
    if (a->has_label != b->has_label || (a->has_label && !span_eq(a->label, b->label))) return false;
    if (a->has_width != b->has_width || a->width != b->width) return false;
    if (a->has_height != b->has_height || a->height != b->height) return false;
    if (a->has_samplerate != b->has_samplerate || a->samplerate != b->samplerate) return false;
    if (a->has_bitrate != b->has_bitrate || a->bitrate != b->bitrate) return false;
    if (a->has_framerate != b->has_framerate || a->framerate_millis != b->framerate_millis) return false;
    if (a->has_render_group != b->has_render_group || a->render_group != b->render_group) return false;
    if (a->has_alt_group != b->has_alt_group || a->alt_group != b->alt_group) return false;
    if (a->has_timescale != b->has_timescale || a->timescale != b->timescale) return false;
    if (a->has_target_latency != b->has_target_latency || a->target_latency != b->target_latency) return false;
    if (a->has_max_grp_sap != b->has_max_grp_sap || a->max_grp_sap != b->max_grp_sap) return false;
    if (a->has_max_obj_sap != b->has_max_obj_sap || a->max_obj_sap != b->max_obj_sap) return false;
    if (a->depends_count != b->depends_count) return false;
    for (size_t i = 0; i < a->depends_count; i++)
        if (!span_eq(a->depends[i], b->depends[i])) return false;
    if (a->cp_ref_id_count != b->cp_ref_id_count) return false;
    for (size_t i = 0; i < a->cp_ref_id_count; i++)
        if (!span_eq(a->cp_ref_ids[i], b->cp_ref_ids[i])) return false;
    /* §7.4.2: the media timeline template is part of the immutable tuple too --
     * a later catalog that adds/removes/changes a surviving track's template is
     * an immutable-tuple violation. */
    if (a->has_template != b->has_template) return false;
    if (a->has_template &&
        (a->template_.start_media_ms != b->template_.start_media_ms ||
         a->template_.delta_media_ms != b->template_.delta_media_ms ||
         a->template_.start_group != b->template_.start_group ||
         a->template_.start_object != b->template_.start_object ||
         a->template_.delta_group != b->template_.delta_group ||
         a->template_.delta_object != b->template_.delta_object ||
         a->template_.start_wallclock_ms != b->template_.start_wallclock_ms ||
         a->template_.delta_wallclock_ms != b->template_.delta_wallclock_ms))
        return false;
    return true;
}

/* Classify how a surviving tuple's metadata changed from `old` to `new`:
 *  - TM_SAME: identical (the normal survivor case).
 *  - TM_VOD: the only changes are the sanctioned live->VOD conversion (§11.3 /
 *    §5.2.7) -- is_live true->false and/or trackDuration absent->present, all
 *    other fields unchanged, and trackDuration is never added while still live.
 *  - TM_VIOLATION: any other change (§5.3 immutable-tuple violation), including
 *    isLive false->true, changing or removing an existing trackDuration, or
 *    adding trackDuration to a track that stays live. */
typedef enum { TM_SAME, TM_VOD, TM_VIOLATION } track_meta_class_t;

static track_meta_class_t classify_track_change(const moq_msf_track_t *old,
                                                const moq_msf_track_t *new)
{
    if (!meta_equal_except_vod(old, new)) return TM_VIOLATION;
    bool live_changed = old->is_live != new->is_live;
    bool live_to_vod = old->is_live && !new->is_live;
    if (live_changed && !live_to_vod)
        return TM_VIOLATION;   /* only true->false is allowed */
    if (old->has_track_duration && new->has_track_duration &&
        old->track_duration_ms != new->track_duration_ms)
        return TM_VIOLATION;   /* changing an existing duration */
    if (old->has_track_duration && !new->has_track_duration)
        return TM_VIOLATION;   /* removing a duration */
    bool dur_added = !old->has_track_duration && new->has_track_duration;
    if (dur_added && new->is_live)
        return TM_VIOLATION;   /* duration added while still live (§5.2.35) */
    if (live_to_vod && !new->has_track_duration)
        return TM_VIOLATION;   /* §11.3: a conversion MUST add a trackDuration --
                                * isLive true->false without one is a half
                                * conversion, not a sanctioned change */
    if (!live_changed && !dur_added) return TM_SAME;
    return TM_VOD;
}

/* §5.3: a declared Namespace|Name tuple's attributes are immutable, except the
 * §11.3 live->VOD conversion. Returns true if any tuple present in both `cand`
 * and the current effective catalog changed in a forbidden way. */
static bool receiver_immutable_violation(const moq_msf_catalog_t *eff,
                                         const moq_msf_catalog_t *cand)
{
    for (size_t i = 0; i < cand->track_count; i++) {
        const moq_msf_track_t *c = &cand->tracks[i];
        for (size_t j = 0; j < eff->track_count; j++) {
            const moq_msf_track_t *e = &eff->tracks[j];
            if (!span_eq(c->name, e->name)) continue;
            if (c->has_namespace != e->has_namespace) continue;
            if (c->has_namespace && !span_eq(c->namespace_, e->namespace_)) continue;
            if (classify_track_change(e, c) == TM_VIOLATION) return true;
            break;
        }
    }
    return false;
}

/* Saturating add: never wraps, so byte budgets stay sound even against
 * adversarially huge inputs (returns UINT64_MAX on overflow, which trips any
 * finite cap). */
static uint64_t sat_add(uint64_t a, uint64_t b)
{
    return a > UINT64_MAX - b ? UINT64_MAX : a + b;
}

/* Conservative estimate of the bytes a handle built from `mt` retains for the
 * receiver's life: the handle struct, the desc string arena (every copied span,
 * matching receiver_track_build's `need`), the depends / cp-ref arrays, and the
 * decoded init buffer. The init estimate MUST mirror receiver_track_build's
 * resolution -- inline initData wins, else initRef is resolved against `src`'s
 * initDataList -- otherwise an initRef-bearing track retains a large init_buf
 * while charging near-zero. The encoded init length is >= the decoded init_buf
 * length, so charging it is conservative. Reads only parsed MSF fields. */
static uint64_t est_track_retained_bytes(const moq_msf_track_t *mt,
                                         const moq_msf_catalog_t *src)
{
    uint64_t b = sizeof(moq_media_track_t);
    b = sat_add(b, (uint64_t)mt->name.len + mt->packaging.len);
    if (mt->has_namespace)      b = sat_add(b, mt->namespace_.len);
    if (mt->has_role)           b = sat_add(b, mt->role.len);
    if (mt->has_codec)          b = sat_add(b, mt->codec.len);
    if (mt->has_lang)           b = sat_add(b, mt->lang.len);
    if (mt->has_label)          b = sat_add(b, mt->label.len);
    if (mt->has_channel_config) b = sat_add(b, mt->channel_config.len);
    if (mt->has_event_type)     b = sat_add(b, mt->event_type.len);
    if (mt->has_mime_type)      b = sat_add(b, mt->mime_type.len);
    for (size_t i = 0; i < mt->depends_count; i++)
        b = sat_add(b, mt->depends[i].len);
    for (size_t i = 0; i < mt->cp_ref_id_count; i++)
        b = sat_add(b, mt->cp_ref_ids[i].len);
    b = sat_add(b, (uint64_t)mt->depends_count   * sizeof(moq_bytes_t));
    b = sat_add(b, (uint64_t)mt->cp_ref_id_count * sizeof(moq_bytes_t));
    /* Init: inline wins; else resolve initRef against src (mirrors build). */
    if (mt->has_init_data && mt->init_data.len > 0) {
        b = sat_add(b, mt->init_data.len);
    } else if (mt->has_init_ref && src) {
        const moq_msf_init_data_entry_t *e =
            moq_msf_catalog_find_init_data(src, mt->init_ref);
        if (e) b = sat_add(b, e->data.len);
    }
    return b;
}

/* Conservative estimate of the bytes a retained CP snapshot (a whole prior
 * effective catalog moved into cp_snaps) holds. Strings borrow from the parse
 * arena, so the dominant term is `_dom_size` (the whole DOM, freed by
 * moq_msf_catalog_cleanup); the remaining terms are the parser-owned arrays
 * that cleanup also frees (tracks, init-data list, content protections + their
 * defaultKIDs, per-track depends / cp-ref arrays, delta ops). This mirrors the
 * exact allocations msf.c releases, so the cap reflects real retained memory. */
static uint64_t est_catalog_retained_bytes(const moq_msf_catalog_t *c)
{
    uint64_t b = sizeof(moq_msf_catalog_t);
    b = sat_add(b, c->_dom_size);                       /* all borrowed strings */
    b = sat_add(b, (uint64_t)c->track_count * sizeof(moq_msf_track_t));
    for (size_t i = 0; i < c->track_count; i++) {
        const moq_msf_track_t *t = &c->tracks[i];
        b = sat_add(b, (uint64_t)t->depends_count   * sizeof(moq_bytes_t));
        b = sat_add(b, (uint64_t)t->cp_ref_id_count * sizeof(moq_bytes_t));
    }
    b = sat_add(b, (uint64_t)c->init_data_count *
                   sizeof(moq_msf_init_data_entry_t));
    b = sat_add(b, (uint64_t)c->content_protection_count *
                   sizeof(moq_cmsf_content_protection_t));
    for (size_t i = 0; i < c->content_protection_count; i++)
        b = sat_add(b, (uint64_t)c->content_protections[i].default_kid_count *
                       sizeof(moq_bytes_t));
    b = sat_add(b, (uint64_t)c->delta_update_count * sizeof(moq_msf_delta_op_t));
    return b;
}

/* Diff a candidate catalog against the live handles: add new tuples, remove
 * gone ones, leave survivors untouched. Returns MOQ_ERR_NOMEM on a build/grow
 * failure (caller terminalizes). */
static moq_result_t receiver_reconcile(moq_media_receiver_t *r,
                                       const moq_msf_catalog_t *cand,
                                       uint64_t now_us)
{
    for (size_t i = 0; i < r->track_count; i++) r->tracks[i]->recon_seen = false;

    for (size_t i = 0; i < cand->track_count; i++) {
        const moq_msf_track_t *mt = &cand->tracks[i];
        moq_media_track_t *live = receiver_find_live(
            r, mt->name, mt->has_namespace, mt->namespace_);
        if (live) {
            live->recon_seen = true;
            /* live->VOD conversion (§11.3): the immutable check already proved
             * any change is the sanctioned is_live true->false / trackDuration
             * absent->present. If is_live or duration actually changed, update
             * the handle's desc IN PLACE (scalars; no allocation) and emit
             * TRACK_UPDATED so the app learns the new VOD metadata. The desc
             * writes precede the rq_push, so an app that polls the event sees
             * them. */
            if (mt->is_live != live->desc.is_live ||
                mt->has_track_duration != live->desc.has_track_duration ||
                mt->track_duration_ms != live->desc.track_duration_ms) {
                live->desc.is_live = mt->is_live;
                live->desc.has_track_duration = mt->has_track_duration;
                live->desc.track_duration_ms = mt->track_duration_ms;
                if (!rq_push(r, MOQ_MEDIA_TRACK_UPDATED, live))
                    return MOQ_OK;   /* overflow already terminalized */
            }
            continue;   /* survivor */
        }

        moq_media_track_t *t = receiver_track_build(r, mt, cand);
        if (!t) return MOQ_ERR_NOMEM;
        t->recon_seen = true;
        pthread_mutex_lock(&r->mu);
        bool pushed = receiver_tracks_push(r, t);
        /* A track that declares its own namespace (MSF §5.2.2 override) is not
         * auto-subscribed: the MSF namespace string has no defined mapping to a
         * MoQT namespace tuple here, so subscribing under the receiver namespace
         * would target the wrong namespace. See subscribe_track (refused). */
        if (pushed && r->auto_subscribe && !t->is_event_timeline &&
            !t->is_media_timeline && !t->key_has_ns) {
            t->desired_subscribed = true;
            t->desired_filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        }
        pthread_mutex_unlock(&r->mu);
        if (!pushed) { receiver_track_free(r, t); return MOQ_ERR_NOMEM; }
        /* Account the new stable handle's retained bytes (room was preflighted
         * in receiver_adopt). initRef is resolved against `cand` exactly as the
         * build above did. Never decreases: the handle outlives every catalog
         * per the lifetime contract. */
        r->retained_bytes = sat_add(r->retained_bytes,
                                    est_track_retained_bytes(mt, cand));
        if (!rq_push(r, MOQ_MEDIA_TRACK_ADDED, t))
            return MOQ_OK;   /* overflow already terminalized */
    }

    /* Removed tuples: live, non-removed, not seen this pass. */
    for (size_t i = 0; i < r->track_count; i++) {
        moq_media_track_t *t = r->tracks[i];
        if (t->removed || t->recon_seen) continue;
        pthread_mutex_lock(&r->mu);
        t->removed = true;
        t->desired_subscribed = false;
        obj_compact_locked(r, keep_not_track, t);   /* purge its queued objects */
        pthread_mutex_unlock(&r->mu);
        if (t->sub_track) {
            (void)moq_sub_unsubscribe(r->sub, t->sub_track, now_us);
            t->sub_track = NULL;
        }
        if (!rq_push(r, MOQ_MEDIA_TRACK_REMOVED, t))
            return MOQ_OK;   /* overflow already terminalized */
    }
    return MOQ_OK;
}

/* Reserve room for one more CP snapshot when the current effective carries
 * contentProtections, so the commit below cannot fail (no visible state is
 * mutated until after this succeeds). A no-op when there is nothing to retain. */
static moq_result_t receiver_reserve_cp_snap(moq_media_receiver_t *r)
{
    if (!r->has_effective || r->effective.content_protection_count == 0)
        return MOQ_OK;
    if (r->cp_snap_count < r->cp_snap_cap) return MOQ_OK;
    size_t nc = r->cp_snap_cap ? r->cp_snap_cap * 2 : 4;
    moq_msf_catalog_t *na = (moq_msf_catalog_t *)r->alloc.alloc(
        nc * sizeof(moq_msf_catalog_t), r->alloc.ctx);
    if (!na) return MOQ_ERR_NOMEM;
    if (r->cp_snaps) {
        memcpy(na, r->cp_snaps, r->cp_snap_count * sizeof(moq_msf_catalog_t));
        r->alloc.free(r->cp_snaps, r->cp_snap_cap * sizeof(moq_msf_catalog_t),
                      r->alloc.ctx);
    }
    r->cp_snaps = na;
    r->cp_snap_cap = nc;
    return MOQ_OK;
}

/* Commit a candidate as the new effective catalog. NO-FAIL: cp_snaps capacity
 * was reserved before reconcile, so this only moves pointers. The previous
 * effective is retained in cp_snaps if it carried contentProtections (so any
 * pointer find_content_protection returned stays valid), else freed. Takes
 * ownership of *cand (caller must not clean it up afterward). */
static void receiver_commit_effective(moq_media_receiver_t *r,
                                      moq_msf_catalog_t *cand)
{
    if (r->has_effective) {
        if (r->effective.content_protection_count > 0) {
            /* Retain for CP-pointer validity. Account its bytes (slot + cap
             * room were preflighted in receiver_adopt). */
            r->retained_bytes = sat_add(r->retained_bytes,
                                        est_catalog_retained_bytes(&r->effective));
            r->cp_snaps[r->cp_snap_count++] = r->effective;   /* room reserved */
        } else {
            moq_msf_catalog_cleanup(&r->alloc, &r->effective);
        }
    }
    r->effective = *cand;     /* move ownership */
    r->has_effective = true;
    /* MSF §5.1.3: latch terminal completion for app observation (get_stats). */
    if (r->effective.is_complete) {
        pthread_mutex_lock(&r->mu);
        r->stats.catalog_complete = true;
        pthread_mutex_unlock(&r->mu);
    }
}

static void receiver_count_drop(moq_media_receiver_t *r)
{
    pthread_mutex_lock(&r->mu);
    r->stats.catalog_drops++;
    pthread_mutex_unlock(&r->mu);
}

/* Drop the latest group's remaining deltas: count once on the transition into
 * desync (a burst in an already-desynced group must not inflate the stat). */
static void receiver_desync(moq_media_receiver_t *r)
{
    if (!r->desynced) {
        r->desynced = true;
        receiver_count_drop(r);
    }
}

typedef enum {
    ADOPT_OK = 0,       /* committed; cand ownership moved into r->effective */
    ADOPT_VIOLATION,    /* §5.3 immutable-tuple violation; cand still owned */
    ADOPT_NOMEM,        /* allocation failure; cand still owned (caller fatals) */
    ADOPT_LIMIT,        /* a receiver-lifetime retained-state cap would be
                           exceeded; cand still owned. After an effective exists
                           this is a bounded drop; on the first catalog it is
                           fatal (CATALOG_UNUSABLE). */
} adopt_result_t;

/* Preflight the retained-state growth this candidate would cause, BEFORE any
 * mutation (no-partial-mutation rule). Counts the NEW stable handles it adds
 * (candidate tuples with no live handle) and their estimated bytes, plus the CP
 * snapshot the commit would retain (the current effective, if it carries
 * contentProtections) and its bytes. Returns false if adding them would exceed
 * the handle-count, CP-snapshot-count, or retained-byte cap. Read-only:
 * mirrors receiver_reconcile's add set and receiver_commit_effective's snapshot
 * rule so the accounting added after commit matches what is checked here. */
static bool receiver_update_within_limits(const moq_media_receiver_t *r,
                                          const moq_msf_catalog_t *cand)
{
    size_t   add_handles = 0;
    uint64_t add_bytes   = 0;
    for (size_t i = 0; i < cand->track_count; i++) {
        const moq_msf_track_t *mt = &cand->tracks[i];
        if (receiver_find_live((moq_media_receiver_t *)r, mt->name,
                               mt->has_namespace, mt->namespace_))
            continue;   /* survivor: reuses its existing handle */
        add_handles++;
        add_bytes = sat_add(add_bytes, est_track_retained_bytes(mt, cand));
    }

    bool     will_snap  = r->has_effective &&
                          r->effective.content_protection_count > 0;
    uint64_t snap_bytes = will_snap
                          ? est_catalog_retained_bytes(&r->effective) : 0;

    if (r->track_count + add_handles > r->max_stable_handles)         return false;
    if (r->cp_snap_count + (will_snap ? 1u : 0u) > r->max_cp_snaps)   return false;
    /* Overflow-safe projected total vs the byte cap. */
    if (sat_add(sat_add(r->retained_bytes, add_bytes), snap_bytes) >
        r->max_retained_bytes)
        return false;
    return true;
}

/* Shared independent/delta adoption: immutable check, then the no-fail commit
 * (CP slot reserved before reconcile, per R1's no-mutation-before-fallible-
 * commit rule). On ADOPT_OK the caller must NOT clean up cand. */
static adopt_result_t receiver_adopt(moq_media_receiver_t *r,
                                     moq_msf_catalog_t *cand, uint64_t now_us)
{
    if (r->has_effective && receiver_immutable_violation(&r->effective, cand))
        return ADOPT_VIOLATION;
    /* Bound cumulative retained state before mutating anything. */
    if (!receiver_update_within_limits(r, cand)) return ADOPT_LIMIT;
    if (receiver_reserve_cp_snap(r) != MOQ_OK) return ADOPT_NOMEM;
    if (receiver_reconcile(r, cand, now_us) != MOQ_OK) return ADOPT_NOMEM;
    receiver_commit_effective(r, cand);   /* no-fail (slot reserved) */
    return ADOPT_OK;
}

/* MSF §5.2.35: a live track MUST NOT carry trackDuration. The encode path
 * enforces this (validate_track), but inbound catalogs are parsed and adopted
 * without re-encoding, so a malicious relay/publisher could otherwise expose a
 * live track with a finite VOD duration through the public track descriptor
 * (media_receiver.h documents track_duration_ms as non-live/VOD-only). The
 * receiver enforces this inbound semantic invariant before adoption -- on both
 * the independent and the delta-applied candidate -- so the parser stays lenient
 * for raw/fuzz users while the receiver contract holds. (Survivor tracks that
 * change live/duration are already caught by receiver_immutable_violation; this
 * additionally catches NEW live+duration tracks, e.g. a first/later independent
 * catalog or a delta `add`.) */
static bool catalog_has_live_track_with_duration(const moq_msf_catalog_t *c)
{
    for (size_t i = 0; i < c->track_count; i++)
        if (c->tracks[i].is_live && c->tracks[i].has_track_duration)
            return true;
    return false;
}

/* Ingest one catalog object (network thread). object 0 = an independent catalog
 * generation (parse → adopt). object >= 1 in the latest group = a delta applied
 * in strict sequence onto the current effective catalog
 * (moq_msf_catalog_apply_delta → adopt). Any failure keeps the current
 * effective, counts a drop, and desyncs the group until the next independent
 * (object 0 of a newer group) re-syncs. */
static void receiver_ingest_catalog(moq_media_receiver_t *r,
                                    uint64_t group_id, uint64_t object_id,
                                    moq_rcbuf_t *payload, uint64_t now_us)
{
    if (!payload) return;

    /* §5: ignore updates preceding the first Object of the latest Group. */
    if (r->have_latest_group && group_id < r->latest_group) return;

    bool first = !r->has_effective;
    moq_bytes_t json = { moq_rcbuf_data(payload), moq_rcbuf_len(payload) };

    if (object_id == 0) {
        /* Dedupe this generation's independent object (SUBSCRIBE and the Joining
         * FETCH both deliver object 0 of the latest group). */
        if (r->have_latest_group && group_id == r->latest_group) return;

        /* MSF §5.1.3: a complete catalog is terminal -- a later independent
         * generation admits no new tracks/content, so drop it (re-delivery of
         * the same generation was already deduped above, so this never inflates
         * the count on benign SUBSCRIBE/FETCH re-sends). A post-complete delta is
         * likewise refused by apply_delta and counted via the desync path. */
        if (r->has_effective && r->effective.is_complete) {
            receiver_count_drop(r);
            return;
        }

        moq_msf_catalog_t cand;
        moq_result_t rc = moq_msf_catalog_parse(&r->alloc, json, &cand);
        if (rc != MOQ_OK || cand.delta_update) {        /* delta on object 0 */
            if (rc == MOQ_OK) moq_msf_catalog_cleanup(&r->alloc, &cand);
            if (first) { receiver_set_fatal(r, MOQ_MEDIA_RECEIVER_FATAL_CATALOG_UNUSABLE); return; }
            receiver_count_drop(r);
            return;
        }
        /* §5.2.35: reject a live track carrying trackDuration before adoption.
         * Treated like a malformed independent catalog: fatal on the first,
         * a counted drop (current effective preserved) on a later generation. */
        if (catalog_has_live_track_with_duration(&cand)) {
            moq_msf_catalog_cleanup(&r->alloc, &cand);
            if (first) { receiver_set_fatal(r, MOQ_MEDIA_RECEIVER_FATAL_CATALOG_UNUSABLE); return; }
            receiver_count_drop(r);
            return;
        }
        adopt_result_t a = receiver_adopt(r, &cand, now_us);
        if (a != ADOPT_OK) {
            moq_msf_catalog_cleanup(&r->alloc, &cand);
            if (a == ADOPT_NOMEM) { receiver_set_fatal(r, MOQ_MEDIA_RECEIVER_FATAL_CATALOG_UNUSABLE); return; }
            /* A first catalog that overflows a retained-state cap leaves no
             * usable effective: terminalize rather than hang unready. After an
             * effective exists, an over-cap (or immutable-violation) update is a
             * bounded drop that preserves the current effective. */
            if (a == ADOPT_LIMIT && first) { receiver_set_fatal(r, MOQ_MEDIA_RECEIVER_FATAL_CATALOG_UNUSABLE); return; }
            receiver_desync(r);   /* ADOPT_VIOLATION / post-ready ADOPT_LIMIT: drop */
            return;
        }
        r->latest_group = group_id;
        r->have_latest_group = true;
        r->cur_obj = 0;
        r->desynced = false;
        if (first) (void)rq_push(r, MOQ_MEDIA_CATALOG_READY, NULL);
        return;
    }

    /* object_id >= 1: a delta object. */
    if (first) {
        /* No independent base ever seen -- the broadcast must start with an
         * independent catalog. */
        receiver_set_fatal(r, MOQ_MEDIA_RECEIVER_FATAL_CATALOG_UNUSABLE);
        return;
    }
    if (group_id > r->latest_group) {
        /* A delta for a newer group whose independent object 0 we have not seen
         * (out-of-order delivery): we cannot apply it. Drop + desync once; the
         * group's object 0, when it arrives, re-syncs via the object_id==0 path. */
        receiver_desync(r);
        return;
    }
    /* group_id == r->latest_group */
    if (r->desynced) return;                 /* ignore until next independent */
    if (object_id <= r->cur_obj) return;     /* duplicate already-applied delta */
    if (object_id != r->cur_obj + 1) {       /* gap / out-of-order */
        receiver_desync(r);
        return;
    }

    moq_msf_catalog_t delta;
    moq_result_t rc = moq_msf_catalog_parse(&r->alloc, json, &delta);
    if (rc != MOQ_OK || !delta.delta_update) {   /* not a delta object */
        if (rc == MOQ_OK) moq_msf_catalog_cleanup(&r->alloc, &delta);
        receiver_desync(r);
        return;
    }
    moq_msf_catalog_t cand;
    rc = moq_msf_catalog_apply_delta(&r->alloc, &r->effective, &delta, &cand);
    moq_msf_catalog_cleanup(&r->alloc, &delta);
    if (rc != MOQ_OK) { receiver_desync(r); return; }   /* apply failure */

    /* §5.2.35: a delta that yields a live track with trackDuration (e.g. a
     * delta `add` of a new live track carrying a duration) is dropped; the
     * current effective is preserved and the group desyncs until the next
     * independent catalog. */
    if (catalog_has_live_track_with_duration(&cand)) {
        moq_msf_catalog_cleanup(&r->alloc, &cand);
        receiver_desync(r);
        return;
    }
    adopt_result_t a = receiver_adopt(r, &cand, now_us);
    if (a != ADOPT_OK) {
        moq_msf_catalog_cleanup(&r->alloc, &cand);
        if (a == ADOPT_NOMEM) { receiver_set_fatal(r, MOQ_MEDIA_RECEIVER_FATAL_CATALOG_UNUSABLE); return; }
        receiver_desync(r);
        return;
    }
    r->cur_obj = object_id;   /* advance within the group */
}

/* Catalog Joining FETCH (network thread). MSF-01 §5 requires the catalog be
 * retrieved with SUBSCRIBE + a Joining FETCH(offset=0) so a subscriber that
 * joins after the catalog was published still obtains the latest complete
 * catalog instead of waiting for the next update. Issued once the catalog
 * subscription is active (SUBSCRIBE_OK received, so it carries a Largest Object
 * the relative offset resolves against); FETCH-delivered objects feed the same
 * ingest path as subscribe delivery, which orders them by (group, object) and
 * dedupes objects already applied from either source.
 *
 * The Joining FETCH obtains the latest retained catalog generation (object 0 +
 * any deltas); the SUBSCRIBE carries subsequent live catalog updates. A rejected
 * catalog SUBSCRIBE is the authoritative "no catalog" signal that terminalizes
 * the receiver (receiver_on_subscribe_error). A relay/origin that does not serve
 * FETCH either errors it or leaves it unanswered; neither is fatal here -- the
 * SUBSCRIBE still delivers live catalog updates, and the unanswered request is
 * bounded (issued exactly once). */
static void receiver_pump_catalog_fetch(moq_media_receiver_t *r,
                                        uint64_t now_us)
{
    if (!r->sub || !r->catalog_sub) return;

    if (!r->catalog_fetch_issued && moq_sub_track_is_active(r->catalog_sub)) {
        moq_sub_joining_fetch_cfg_t fcfg;
        moq_sub_joining_fetch_cfg_init(&fcfg);
        fcfg.track = r->catalog_sub;
        fcfg.relative = true;     /* offset measured from the largest group */
        fcfg.joining_start = 0;   /* §5: offset = 0 -> the latest group */
        moq_result_t rc = moq_sub_joining_fetch(r->sub, &fcfg, now_us,
                                                &r->catalog_fetch);
        /* WOULD_BLOCK (no fetch slot / action queue full) is transient: leave
         * the flag clear so the next pump cycle retries. Any other error means
         * the FETCH cannot be placed -- give up and rely on the SUBSCRIBE path
         * rather than spin. */
        if (rc != MOQ_ERR_WOULD_BLOCK)
            r->catalog_fetch_issued = true;
    }

    moq_sub_fetch_item_t item;
    while (moq_sub_poll_fetch(r->sub, &item) == MOQ_OK) {
        if (item.request == r->catalog_fetch) {
            if (item.kind == MOQ_SUB_FETCH_OBJECT)
                receiver_ingest_catalog(r, item.u.object.group_id,
                                        item.u.object.object_id,
                                        item.u.object.payload, now_us);
            /* moq_sub_poll_fetch already released the request slot for terminal
             * items; drop our handle (only on our own fetch) so a recycled slot
             * can't later match. */
            if (item.kind == MOQ_SUB_FETCH_ERROR ||
                item.kind == MOQ_SUB_FETCH_COMPLETE)
                r->catalog_fetch = NULL;
        }
        moq_sub_fetch_item_cleanup(&item);
    }
}

/* Mark a media track ended (network thread) and emit TRACK_ENDED exactly once.
 * `ended` is read cross-thread by the public subscribe_track(), so the RMW is
 * done under r->mu; rq_push takes the lock itself, so emit after releasing. */
static void receiver_mark_track_ended(moq_media_receiver_t *r,
                                      moq_media_track_t *t)
{
    pthread_mutex_lock(&r->mu);
    bool first = !t->ended;
    t->ended = true;
    pthread_mutex_unlock(&r->mu);
    if (first)
        (void)rq_push(r, MOQ_MEDIA_TRACK_ENDED, t);
}

/* Subscriber callback (network thread): a media track's SUBSCRIBE_OK landed, so
 * it is now ACTIVE (not just requested). Publish `established` under r->mu for
 * the track_state accessor. The catalog subscription's OK is irrelevant here. */
static void receiver_on_subscribed(void *ctx, moq_sub_track_t *track)
{
    moq_media_receiver_t *r = (moq_media_receiver_t *)ctx;
    if (track == r->catalog_sub) return;
    moq_media_track_t *t = track_for_sub(r, track);
    if (!t) return;
    pthread_mutex_lock(&r->mu);
    t->established = true;
    pthread_mutex_unlock(&r->mu);
}

/* Subscriber callback (fires synchronously inside moq_sub_tick on the
 * network thread): a rejected catalog subscription is terminal -- the
 * receiver can never discover anything, and silently idling forever would
 * strand wait()/poll_track() in DONE. */
static void receiver_on_subscribe_error(void *ctx, moq_sub_track_t *track,
                                        moq_request_error_t error_code,
                                        moq_bytes_t reason)
{
    (void)error_code; (void)reason;
    moq_media_receiver_t *r = (moq_media_receiver_t *)ctx;
    if (track == r->catalog_sub) {
        receiver_set_fatal(r, MOQ_MEDIA_RECEIVER_FATAL_CATALOG_REJECTED);
        return;
    }
    /* A refused media-track subscription is track data, never receiver
     * EOF: the track ends before it began. */
    moq_media_track_t *t = track_for_sub(r, track);
    if (t)
        receiver_mark_track_ended(r, t);
}

/* Subscriber callback (network thread, inside moq_sub_tick): a media track's
 * subscription completed (SUBSCRIBE_DONE) with `status_code`. The done is only
 * RECORDED here; the pump processes it in receiver_process_subscription_dones()
 * AFTER draining objects, so media delivered in the same tick as the done is not
 * dropped. The catalog subscription is not converted, so its done is ignored
 * (status quo -- the catalog's own FETCH/SUBSCRIBE delivery path stands). */
static void receiver_on_subscribe_done(void *ctx, moq_sub_track_t *track,
                                       uint64_t status_code)
{
    moq_media_receiver_t *r = (moq_media_receiver_t *)ctx;
    if (track == r->catalog_sub) return;
    moq_media_track_t *t = track_for_sub(r, track);
    if (!t) return;
    t->done_pending = true;
    t->done_status = status_code;
}

/* -- Media object intake (network thread) -------------------------------- */

/* Parse one facade object for a media track and enqueue it. Takes over the
 * facade object's payload/properties refs on success or releases them on
 * drop -- the caller must NOT also call moq_sub_object_cleanup. */
static void receiver_on_media_object(moq_media_receiver_t *r,
                                     moq_media_track_t *track,
                                     moq_sub_object_t *so, uint64_t now_us)
{
    (void)now_us;

    /* A track disabled via unsubscribe keeps its facade subscription alive
     * (delivery is paused with forward=false rather than torn down). Objects
     * already in flight when the pause took effect must not surface -- drop them
     * here without the cost of parsing. The one exception is a terminal
     * END_OF_TRACK status: a track the publisher ended must still surface
     * TRACK_ENDED even while disabled, so let it through to the parse + terminal
     * branch below (so->status carries the wire status before parse). A late
     * non-terminal straggler that slips past this is still dropped at queue
     * admission. */
    pthread_mutex_lock(&r->mu);
    bool wanted = track->desired_subscribed;
    if (!wanted && so->status != MOQ_OBJECT_END_OF_TRACK) {
        /* Count it as received-then-dropped so objects_dropped stays a subset of
         * objects_received (we skip the parse only as an optimization). */
        r->stats.objects_received++;
        r->stats.objects_dropped++;
        pthread_mutex_unlock(&r->mu);
        moq_sub_object_cleanup(so);
        return;
    }
    pthread_mutex_unlock(&r->mu);

    moq_media_object_input_t in;
    moq_media_object_input_init(&in);
    if (moq_media_object_input_from_sub_object(so, &in) != MOQ_OK) {
        pthread_mutex_lock(&r->mu);
        r->stats.parse_drops++;
        pthread_mutex_unlock(&r->mu);
        moq_sub_object_cleanup(so);
        return;
    }

    moq_cmaf_sample_t stack_samples[RECEIVER_PARSE_STACK_SAMPLES];
    moq_cmaf_sample_t *samples = stack_samples;
    size_t sample_cap = RECEIVER_PARSE_STACK_SAMPLES;
    moq_cmaf_sample_t *heap_samples = NULL;

    moq_media_parsed_object_t parsed;
    moq_media_parsed_object_init(&parsed);
    moq_media_drop_reason_t why;
    moq_result_t rc = moq_media_object_parse(&track->desc.info, &in,
                                             samples, sample_cap,
                                             &parsed, &why);
    if (rc == MOQ_ERR_BUFFER && parsed.sample_count > sample_cap) {
        sample_cap = parsed.sample_count;
        heap_samples = (moq_cmaf_sample_t *)r->alloc.alloc(
            sample_cap * sizeof(moq_cmaf_sample_t), r->alloc.ctx);
        if (heap_samples) {
            samples = heap_samples;
            rc = moq_media_object_parse(&track->desc.info, &in, samples,
                                        sample_cap, &parsed, &why);
        }
    }
    if (rc != MOQ_OK) {
        pthread_mutex_lock(&r->mu);
        r->stats.parse_drops++;
        pthread_mutex_unlock(&r->mu);
        if (heap_samples)
            r->alloc.free(heap_samples,
                          sample_cap * sizeof(moq_cmaf_sample_t),
                          r->alloc.ctx);
        moq_sub_object_cleanup(so);
        return;
    }

    /* A terminal-status object is track-event data, never an object. */
    if (parsed.status == MOQ_OBJECT_END_OF_TRACK) {
        receiver_mark_track_ended(r, track);
        if (heap_samples)
            r->alloc.free(heap_samples,
                          sample_cap * sizeof(moq_cmaf_sample_t),
                          r->alloc.ctx);
        moq_sub_object_cleanup(so);
        return;
    }

    /* SHARED_EPOCH: rebase the reported timestamp fields against one
     * receiver-wide anchor (the first parsed object's decode time);
     * payload bytes are never modified. */
    if (r->time_mode == MOQ_MEDIA_TIME_SHARED_EPOCH) {
        if (!r->epoch_set) {
            r->epoch_set = true;
            r->epoch_us = parsed.decode_time_us;
        }
        uint64_t e = r->epoch_us;
        parsed.decode_time_us =
            parsed.decode_time_us > e ? parsed.decode_time_us - e : 0;
        parsed.presentation_time_us =
            parsed.presentation_time_us > e ? parsed.presentation_time_us - e
                                            : 0;
        if (parsed.has_capture_time)
            parsed.capture_time_us =
                parsed.capture_time_us > e ? parsed.capture_time_us - e : 0;
    }

    /* Per-object owned sample table (the parse wrote into stack/heap
     * scratch; queue entries must own theirs). */
    moq_cmaf_sample_t *owned = NULL;
    if (parsed.sample_count > 0) {
        if (heap_samples && samples == heap_samples) {
            owned = heap_samples;            /* take it over as-is */
            heap_samples = NULL;
        } else {
            owned = (moq_cmaf_sample_t *)r->alloc.alloc(
                parsed.sample_count * sizeof(moq_cmaf_sample_t),
                r->alloc.ctx);
            if (!owned) {
                pthread_mutex_lock(&r->mu);
                r->stats.parse_drops++;
                pthread_mutex_unlock(&r->mu);
                moq_sub_object_cleanup(so);
                return;
            }
            memcpy(owned, samples,
                   parsed.sample_count * sizeof(moq_cmaf_sample_t));
        }
    }
    if (heap_samples)
        r->alloc.free(heap_samples, sample_cap * sizeof(moq_cmaf_sample_t),
                      r->alloc.ctx);

    size_t need = so->payload ? moq_rcbuf_len(so->payload) : 0;

    pthread_mutex_lock(&r->mu);
    /* Every successfully parsed object is "received" (the stat counts parsed
     * objects whether they end up queued or dropped); the drop counters below
     * are then a subset. */
    r->stats.objects_received++;
    /* Disable check under the same lock the unsubscribe purge uses: a non-
     * terminal object for a track disabled (or disabled while this object was
     * in flight) must not land in the queue after the purge ran. Terminal
     * END_OF_TRACK was already handled above, so a disabled track still ends. */
    if (!track->desired_subscribed) {
        r->stats.objects_dropped++;
        if (parsed.keyframe) r->stats.keyframes_dropped++;
        pthread_mutex_unlock(&r->mu);
        if (owned)
            r->alloc.free(owned,
                          parsed.sample_count * sizeof(moq_cmaf_sample_t),
                          r->alloc.ctx);
        moq_sub_object_cleanup(so);
        return;
    }
    /* A straggler of evicted content is dropped even when there is room;
     * otherwise the overflow policy makes room and re-checks the incoming
     * object against the filter the eviction may have armed. */
    bool keep = !obj_filtered_locked(r, track, so->group_id,
                                     parsed.keyframe) &&
                obj_make_room_locked(r, track, so->group_id,
                                     parsed.keyframe, need);
    if (!keep) {
        r->stats.objects_dropped++;
        if (parsed.keyframe) r->stats.keyframes_dropped++;
        pthread_mutex_unlock(&r->mu);
        if (owned)
            r->alloc.free(owned,
                          parsed.sample_count * sizeof(moq_cmaf_sample_t),
                          r->alloc.ctx);
        moq_sub_object_cleanup(so);
        return;
    }

    receiver_obj_entry_t *e = &r->objs[r->obj_tail % r->ring_cap];
    memset(e, 0, sizeof(*e));
    e->group_id = so->group_id;
    moq_media_object_t *o = &e->obj;
    o->track = track;
    o->config_generation = 0;
    o->packaging = parsed.packaging;
    o->status = parsed.status;
    o->end_of_group = parsed.end_of_group;
    o->datagram = parsed.datagram;
    o->keyframe = parsed.keyframe;
    o->has_capture_time = parsed.has_capture_time;
    o->capture_time_us = parsed.capture_time_us;
    o->decode_time_us = parsed.decode_time_us;
    o->composition_offset_us = parsed.composition_offset_us;
    o->presentation_time_us = parsed.presentation_time_us;
    o->payload = parsed.payload;
    o->fragment = parsed.fragment;
    o->mdat_offset = parsed.mdat_offset;
    o->mdat_len = parsed.mdat_len;
    o->sample_count = parsed.sample_count;
    o->samples = owned;
    o->samples_owned = owned;
    /* Steal the facade object's refs (no moq_sub_object_cleanup). */
    o->payload_ref = so->payload;
    o->properties_ref = so->properties;
    r->obj_sizes_total += need;
    r->obj_tail++;
    pthread_mutex_unlock(&r->mu);
}

/* -- SAP event-timeline intake (network thread) ------------------------- *
 * A self-contained, tolerant scanner for the fixed CMSF §3.6.1 payload: a JSON
 * array of {"l":[group,object],"data":[sapType,ept_ms]} records. It rejects
 * malformed input (the whole object is then a parse drop) rather than pulling
 * the msf-module's vendored JSON parser across the module boundary. */

typedef struct { const char *p, *end; } sap_cur_t;

static void sap_ws(sap_cur_t *c)
{
    while (c->p < c->end) {
        char ch = *c->p;
        if (ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r') c->p++;
        else break;
    }
}

static bool sap_ch(sap_cur_t *c, char ch)
{
    sap_ws(c);
    if (c->p < c->end && *c->p == ch) { c->p++; return true; }
    return false;
}

static bool sap_uint(sap_cur_t *c, uint64_t *out)
{
    sap_ws(c);
    if (c->p >= c->end || *c->p < '0' || *c->p > '9') return false;
    uint64_t v = 0;
    int digits = 0;
    while (c->p < c->end && *c->p >= '0' && *c->p <= '9') {
        if (digits >= 19) return false;   /* overflow guard */
        v = v * 10u + (uint64_t)(*c->p - '0');
        c->p++; digits++;
    }
    *out = v;
    return true;
}

static bool sap_pair(sap_cur_t *c, uint64_t *a, uint64_t *b)
{
    return sap_ch(c, '[') && sap_uint(c, a) && sap_ch(c, ',') &&
           sap_uint(c, b) && sap_ch(c, ']');
}

/* Skip a JSON string starting at the opening quote (already positioned by the
 * caller via sap_ws). Returns false on an unterminated string. */
static bool sap_skip_string(sap_cur_t *c)
{
    c->p++;   /* opening quote */
    while (c->p < c->end && *c->p != '"') {
        if (*c->p == '\\') { c->p++; if (c->p >= c->end) return false; }
        c->p++;
    }
    if (c->p >= c->end) return false;
    c->p++;   /* closing quote */
    return true;
}

/* Skip an arbitrary JSON value (for unknown record keys): string, array/object
 * (balanced, string-aware), or a bareword number/true/false/null. */
static bool sap_skip_value(sap_cur_t *c)
{
    sap_ws(c);
    if (c->p >= c->end) return false;
    char ch = *c->p;
    if (ch == '"') return sap_skip_string(c);
    if (ch == '[' || ch == '{') {
        char open = ch, close = (ch == '[') ? ']' : '}';
        int depth = 0;
        while (c->p < c->end) {
            char x = *c->p;
            if (x == '"') { if (!sap_skip_string(c)) return false; continue; }
            if (x == open) depth++;
            else if (x == close) { depth--; c->p++; if (depth == 0) return true; continue; }
            c->p++;
        }
        return false;
    }
    while (c->p < c->end) {
        char x = *c->p;
        if (x == ',' || x == ']' || x == '}' ||
            x == ' ' || x == '\t' || x == '\n' || x == '\r') break;
        c->p++;
    }
    return true;
}

/* Parse one record object into (group, object, sap_type, ept_ms). Accepts "l"
 * and "data" in any order and tolerates (skips) unknown keys. */
static bool sap_record(sap_cur_t *c, uint64_t *g, uint64_t *o,
                       uint64_t *st, uint64_t *ept)
{
    if (!sap_ch(c, '{')) return false;
    sap_ws(c);
    if (c->p < c->end && *c->p == '}') return false;  /* empty: missing keys */
    bool has_l = false, has_d = false;
    for (;;) {
        sap_ws(c);
        if (c->p >= c->end || *c->p != '"') return false;
        const char *kstart = c->p + 1;
        if (!sap_skip_string(c)) return false;
        size_t klen = (size_t)((c->p - 1) - kstart);   /* between the quotes */
        if (!sap_ch(c, ':')) return false;
        if (klen == 1 && kstart[0] == 'l') {
            if (!sap_pair(c, g, o)) return false;
            has_l = true;
        } else if (klen == 4 && memcmp(kstart, "data", 4) == 0) {
            if (!sap_pair(c, st, ept)) return false;
            has_d = true;
        } else if (!sap_skip_value(c)) {
            return false;
        }
        if (sap_ch(c, ',')) continue;
        if (sap_ch(c, '}')) break;
        return false;
    }
    if (!has_l || !has_d || *st > 3) return false;
    return true;
}

/* Append one record to the bounded SAP queue (r->mu held). Drops the oldest
 * record when the app under-polls -- harmless because the next §8.3 independent
 * object re-sends the accessible history (and the receiver's per-track dedup
 * keeps that from looping). */
static void sap_queue_push(moq_media_receiver_t *r, moq_media_track_t *track,
                           uint64_t group, uint64_t object,
                           moq_sap_type_t sap_type, uint64_t ept_ms)
{
    if (r->sap_tail - r->sap_head >= r->sap_cap)
        r->sap_head++;   /* drop oldest */
    moq_media_sap_record_t *rec = &r->saps[r->sap_tail % r->sap_cap];
    memset(rec, 0, sizeof(*rec));
    rec->struct_size = (uint32_t)sizeof(*rec);
    rec->track = track;
    rec->group = group;
    rec->object = object;
    rec->sap_type = sap_type;
    rec->ept_ms = ept_ms;
    r->sap_tail++;
}

/* Parse a SAP timeline object and enqueue its NEW records. Takes over / releases
 * the facade object's refs (the caller must NOT also cleanup). */
static void receiver_on_sap_object(moq_media_receiver_t *r,
                                   moq_media_track_t *track,
                                   moq_sub_object_t *so, uint64_t now_us)
{
    (void)now_us;

    /* A terminal END_OF_TRACK ends the timeline track, like the media path. */
    if (so->status == MOQ_OBJECT_END_OF_TRACK) {
        receiver_mark_track_ended(r, track);
        moq_sub_object_cleanup(so);
        return;
    }

    pthread_mutex_lock(&r->mu);
    bool wanted = track->desired_subscribed;
    pthread_mutex_unlock(&r->mu);
    if (!wanted) { moq_sub_object_cleanup(so); return; }
    if (!so->payload) {
        pthread_mutex_lock(&r->mu);
        r->stats.parse_drops++;
        pthread_mutex_unlock(&r->mu);
        moq_sub_object_cleanup(so);
        return;
    }

    /* Parse the WHOLE array into a temporary list first; enqueue and advance the
     * dedup cursor only after the entire payload validates, so a valid record
     * followed by a malformed tail leaves nothing visible and does not poison
     * sap_last_* (which would suppress a later independent re-send). */
    moq_media_sap_record_t stack_tmp[64];
    moq_media_sap_record_t *tmp = stack_tmp;
    moq_media_sap_record_t *heap_tmp = NULL;
    size_t tmp_cap = sizeof(stack_tmp) / sizeof(stack_tmp[0]), tmp_n = 0;

    sap_cur_t c = { (const char *)moq_rcbuf_data(so->payload),
                    (const char *)moq_rcbuf_data(so->payload)
                        + moq_rcbuf_len(so->payload) };
    bool malformed = false;
    if (!sap_ch(&c, '[')) {
        malformed = true;
    } else {
        sap_ws(&c);
        if (c.p < c.end && *c.p == ']') {
            c.p++;   /* empty array: valid, no records */
        } else {
            for (;;) {
                uint64_t g, o, st, ept;
                if (!sap_record(&c, &g, &o, &st, &ept)) { malformed = true; break; }
                if (tmp_n == tmp_cap) {
                    size_t nc = tmp_cap * 2;
                    moq_media_sap_record_t *grown =
                        (moq_media_sap_record_t *)r->alloc.realloc(
                            heap_tmp, heap_tmp ? tmp_cap * sizeof(*tmp) : 0,
                            nc * sizeof(*tmp), r->alloc.ctx);
                    if (!grown) { malformed = true; break; }  /* OOM: drop whole */
                    if (!heap_tmp) memcpy(grown, stack_tmp, tmp_cap * sizeof(*tmp));
                    heap_tmp = grown; tmp = grown; tmp_cap = nc;
                }
                tmp[tmp_n].group = g;
                tmp[tmp_n].object = o;
                tmp[tmp_n].sap_type = (moq_sap_type_t)st;
                tmp[tmp_n].ept_ms = ept;
                tmp_n++;
                if (sap_ch(&c, ',')) continue;
                if (sap_ch(&c, ']')) break;
                malformed = true; break;
            }
        }
    }

    pthread_mutex_lock(&r->mu);
    if (malformed) {
        r->stats.parse_drops++;   /* nothing enqueued, dedup cursor untouched */
    } else {
        for (size_t i = 0; i < tmp_n; i++) {
            uint64_t g = tmp[i].group, o = tmp[i].object;
            /* Dedup the §8.3 independent re-sends: surface a media Location only
             * once, in (group, object) order. */
            bool newer = !track->sap_has_last ||
                g > track->sap_last_group ||
                (g == track->sap_last_group && o > track->sap_last_object);
            if (newer) {
                sap_queue_push(r, track, g, o, tmp[i].sap_type, tmp[i].ept_ms);
                track->sap_has_last = true;
                track->sap_last_group = g;
                track->sap_last_object = o;
            }
        }
    }
    pthread_mutex_unlock(&r->mu);
    if (heap_tmp) r->alloc.free(heap_tmp, tmp_cap * sizeof(*tmp), r->alloc.ctx);
    moq_sub_object_cleanup(so);
}

/* Append one media-timeline record to the bounded queue (r->mu held). Drops the
 * oldest when the app sustainedly under-polls; because the per-track dedup cursor
 * has already advanced past a surfaced Location, a dropped record is NOT
 * re-surfaced by a later §7.3 independent re-send (same trade-off as the SAP
 * queue: bound memory over guaranteeing delivery to a stalled consumer). */
static void mt_queue_push(moq_media_receiver_t *r, moq_media_track_t *track,
                          const moq_msf_media_timeline_record_t *src)
{
    if (r->mt_tail - r->mt_head >= r->mt_cap)
        r->mt_head++;   /* drop oldest */
    moq_media_timeline_record_t *rec = &r->mts[r->mt_tail % r->mt_cap];
    memset(rec, 0, sizeof(*rec));
    rec->struct_size = (uint32_t)sizeof(*rec);
    rec->track = track;
    rec->media_time_ms = src->media_time_ms;
    rec->group = src->group;
    rec->object = src->object;
    rec->wallclock_ms = src->wallclock_ms;
    r->mt_tail++;
}

/* Parse an MSF §7.1.1 explicit media-timeline object and enqueue its NEW records
 * (those with a media Location past this track's last surfaced one). Takes over /
 * releases the facade object's refs (the caller must NOT also cleanup).
 *
 * All-or-nothing: a malformed payload enqueues nothing, counts one parse_drop,
 * leaves the dedup cursor untouched (so a later independent re-send still
 * surfaces), and is never fatal. */
static void receiver_on_media_timeline_object(moq_media_receiver_t *r,
                                              moq_media_track_t *track,
                                              moq_sub_object_t *so,
                                              uint64_t now_us)
{
    (void)now_us;

    /* A terminal END_OF_TRACK ends the timeline track, like the media path. */
    if (so->status == MOQ_OBJECT_END_OF_TRACK) {
        receiver_mark_track_ended(r, track);
        moq_sub_object_cleanup(so);
        return;
    }

    pthread_mutex_lock(&r->mu);
    bool wanted = track->desired_subscribed;
    pthread_mutex_unlock(&r->mu);
    if (!wanted) { moq_sub_object_cleanup(so); return; }
    if (!so->payload) {
        pthread_mutex_lock(&r->mu);
        r->stats.parse_drops++;
        pthread_mutex_unlock(&r->mu);
        moq_sub_object_cleanup(so);
        return;
    }

    /* Decode the whole payload into a temp array first (stack, growing to heap
     * once for a large history), then enqueue+advance the dedup cursor only if it
     * fully validated -- so a malformed tail leaves nothing visible and does not
     * poison mt_last_* (which would suppress a later independent re-send). */
    moq_bytes_t json = { moq_rcbuf_data(so->payload),
                         moq_rcbuf_len(so->payload) };
    moq_msf_media_timeline_record_t stack_tmp[64];
    moq_msf_media_timeline_record_t *tmp = stack_tmp;
    moq_msf_media_timeline_record_t *heap_tmp = NULL;
    size_t cap = sizeof(stack_tmp) / sizeof(stack_tmp[0]), n = 0;

    moq_result_t rc = moq_msf_media_timeline_decode(json, tmp, cap, &n);
    if (rc == MOQ_ERR_BUFFER) {
        /* More records than the stack buffer: grow once to the needed count. */
        size_t need = n;
        heap_tmp = (moq_msf_media_timeline_record_t *)r->alloc.alloc(
            need * sizeof(*heap_tmp), r->alloc.ctx);
        if (!heap_tmp) {
            pthread_mutex_lock(&r->mu);
            r->stats.parse_drops++;   /* OOM: drop whole object, non-fatal */
            pthread_mutex_unlock(&r->mu);
            moq_sub_object_cleanup(so);
            return;
        }
        tmp = heap_tmp;
        cap = need;
        rc = moq_msf_media_timeline_decode(json, tmp, cap, &n);
    }
    if (rc != MOQ_OK) {
        pthread_mutex_lock(&r->mu);
        r->stats.parse_drops++;   /* malformed: nothing enqueued */
        pthread_mutex_unlock(&r->mu);
        if (heap_tmp) r->alloc.free(heap_tmp, cap * sizeof(*heap_tmp),
                                    r->alloc.ctx);
        moq_sub_object_cleanup(so);
        return;
    }

    pthread_mutex_lock(&r->mu);
    for (size_t i = 0; i < n; i++) {
        uint64_t g = tmp[i].group, o = tmp[i].object;
        /* Dedup the §7.3 independent re-sends by media Location (not by the
         * timeline object's own location): surface each Location once, in order. */
        bool newer = !track->mt_has_last ||
            g > track->mt_last_group ||
            (g == track->mt_last_group && o > track->mt_last_object);
        if (newer) {
            mt_queue_push(r, track, &tmp[i]);
            track->mt_has_last = true;
            track->mt_last_group = g;
            track->mt_last_object = o;
        }
    }
    pthread_mutex_unlock(&r->mu);
    if (heap_tmp) r->alloc.free(heap_tmp, cap * sizeof(*heap_tmp), r->alloc.ctx);
    moq_sub_object_cleanup(so);
}

/* Process recorded SUBSCRIBE_DONEs (network thread), AFTER objects were drained
 * this tick so media delivered alongside the done is not lost.
 *
 * "Track Ended" (status 0x2) is the MSF §11.3 live->VOD step 1: the live phase
 * ended but the publisher keeps the track registered/retained/joinable, so the
 * track is NOT terminalized -- already-delivered media stays queued, no
 * TRACK_ENDED is emitted, and the conversion catalog (isLive:false +
 * trackDuration) lands separately on the live catalog SUBSCRIBE and emits
 * TRACK_UPDATED on the SAME handle. The old facade slot is freed and sub_track
 * cleared so reconcile re-subscribes for the VOD content (auto- or
 * explicitly-subscribed alike -- both reconcile on desired_subscribed).
 * vod_rearmed makes that re-arm one-shot, so a peer that keeps finishing before
 * the VOD catalog arrives cannot spin a re-subscribe loop. Any other status, or
 * a repeat done after re-arm, is terminal: the track ends exactly once. */
static void receiver_process_subscription_dones(moq_media_receiver_t *r)
{
    for (size_t i = 0; i < r->track_count; i++) {
        moq_media_track_t *t = r->tracks[i];
        if (!t->done_pending) continue;
        t->done_pending = false;
        uint64_t status = t->done_status;

        /* The subscription is already complete: free its facade slot (no
         * UNSUBSCRIBE is sent) and drop our handle so reconcile may re-subscribe. */
        if (t->sub_track) {
            (void)moq_sub_release_track(r->sub, t->sub_track);
            t->sub_track = NULL;
        }

        if (status == RECEIVER_DONE_TRACK_ENDED && !t->vod_rearmed && !t->ended) {
            /* Re-arm for VOD: keep desired_subscribed; reset the lifecycle flags
             * so the track_state accessor reads PENDING again until the VOD
             * SUBSCRIBE_OK lands. Delivered media stays in the object queue. */
            t->vod_rearmed = true;
            pthread_mutex_lock(&r->mu);
            t->established = false;
            t->requested = false;
            pthread_mutex_unlock(&r->mu);
        } else {
            receiver_mark_track_ended(r, t);   /* terminal: end exactly once */
        }
    }
}

/* auto_subscribe seeding now happens per-track inside receiver_reconcile when a
 * new media tuple is discovered (any generation), so a track added after the
 * initial catalog is auto-subscribed too. The cross-queue guarantee still
 * holds: a new track's TRACK_ADDED is enqueued before reconcile_subscriptions
 * issues any SUBSCRIBE for it. */

/* Establish a facade subscription for every track the app wants that does not
 * have one yet, on the network thread. Idempotent: a track that already has a
 * handle is left alone -- disabling a track does NOT tear its subscription down
 * (see receiver_reconcile_delivery), so repeated enable/disable never allocates
 * a fresh handle and the facade's bounded track table is never exhausted by
 * toggling. A subscribe that hits a transient WRONG_STATE/WOULD_BLOCK is retried
 * on a later cycle; a hard failure is receiver-fatal (as the auto path was). */
static void receiver_reconcile_subscriptions(moq_media_receiver_t *r,
                                             uint64_t now_us)
{
    for (size_t i = 0; i < r->track_count; i++) {
        moq_media_track_t *t = r->tracks[i];
        if (t->removed) continue;
        /* Never subscribe a namespace-override track under the receiver
         * namespace (it is also refused at subscribe_track). */
        if (t->key_has_ns) continue;

        pthread_mutex_lock(&r->mu);
        bool want = t->desired_subscribed;
        moq_subscribe_filter_t filt = t->desired_filter;
        bool has_prio = t->desired_has_priority;
        uint8_t prio = t->desired_priority;
        pthread_mutex_unlock(&r->mu);

        if (!want || t->sub_track || t->ended) continue;

        moq_sub_track_cfg_t tcfg;
        moq_sub_track_cfg_init(&tcfg);
        tcfg.track_namespace = r->namespace_;
        tcfg.track_name = t->desc.name;
        tcfg.filter = filt ? filt : MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        tcfg.has_subscriber_priority = has_prio;
        tcfg.subscriber_priority = prio;
        moq_result_t rc = moq_sub_subscribe(r->sub, &tcfg, now_us,
                                            &t->sub_track);
        if (rc == MOQ_OK) {
            t->forward_sent = true;     /* a fresh SUBSCRIBE forwards by default */
            t->forward_desired = true;
            t->has_drop_filter = false;
            r->subscribed_media = true;
            /* SUBSCRIBE issued, not yet accepted -> PENDING. Published under mu
             * for the app-thread track_state accessor; established is set later
             * from the on_subscribed (SUBSCRIBE_OK) callback. */
            pthread_mutex_lock(&r->mu);
            t->requested = true;
            pthread_mutex_unlock(&r->mu);
        } else if (rc == MOQ_ERR_WRONG_STATE || rc == MOQ_ERR_WOULD_BLOCK) {
            t->sub_track = NULL;        /* transient: retry next cycle */
        } else {
            receiver_set_fatal(r, MOQ_MEDIA_RECEIVER_FATAL_SETUP_FAILED);
            return;
        }
    }
}

/* Reconcile per-track delivery (the subscription's Forward State) with the two
 * things that can suppress it: an app unsubscribe (desired_subscribed=false) and
 * FLOW_CONTROL backpressure. A disabled track keeps its handle but pauses with
 * forward=false and has its queued objects purged once, so re-enabling is a cheap
 * forward=true with no new subscribe. The facade's one-outstanding-update rule is
 * tolerated by retrying on later cycles. */
static void receiver_reconcile_delivery(moq_media_receiver_t *r,
                                        uint64_t now_us)
{
    /* FLOW_CONTROL is the only policy that pauses; it pauses every subscribed
     * track together off the shared queue depth. */
    bool flow_allows = true;
    if (r->overflow.policy == MOQ_MEDIA_OVERFLOW_FLOW_CONTROL &&
        r->subscribed_media) {
        pthread_mutex_lock(&r->mu);
        bool paused = r->paused;
        uint32_t depth = r->obj_tail - r->obj_head;
        r->resume_nudged = false;   /* the hook ran; polls may nudge again */
        if (paused && depth <= r->obj_cap / 2) {
            r->paused = false;      /* drained below low-water: resume */
            r->stats.pause_transitions++;
            paused = false;
        }
        pthread_mutex_unlock(&r->mu);
        flow_allows = !paused;
    }

    for (size_t i = 0; i < r->track_count; i++) {
        moq_media_track_t *t = r->tracks[i];
        if (!t->sub_track || t->ended || t->removed) continue;

        pthread_mutex_lock(&r->mu);
        bool want = t->desired_subscribed;
        pthread_mutex_unlock(&r->mu);
        /* Queued objects for a disabled track are purged in unsubscribe_track
         * (independent of forward state); here we only drive the Forward State. */

        bool target = want && flow_allows;
        t->forward_desired = target;
        if (t->forward_desired == t->forward_sent) continue;
        moq_sub_update_cfg_t ucfg;
        moq_sub_update_cfg_init(&ucfg);
        ucfg.has_forward = true;
        ucfg.forward = t->forward_desired;
        /* WRONG_STATE = an update is still outstanding; retry next cycle. */
        if (moq_sub_update_subscription(r->sub, t->sub_track, &ucfg,
                                        now_us) == MOQ_OK)
            t->forward_sent = t->forward_desired;
    }
}

/* The endpoint pump hook: runs on the network thread under the endpoint
 * mutex (endpoint_internal.h contract -- no public endpoint API calls). */
static void receiver_hook(moq_endpoint_t *ep, moq_session_t *session,
                          uint64_t now_us, void *ctx)
{
    (void)ep;
    moq_media_receiver_t *r = (moq_media_receiver_t *)ctx;

    pthread_mutex_lock(&r->mu);
    bool fatal = r->fatal;
    pthread_mutex_unlock(&r->mu);
    if (fatal || !session) return;

    if (!r->sub) {
        if (moq_session_state(session) != MOQ_SESS_ESTABLISHED)
            return;
        moq_sub_cfg_t scfg;
        /* Full current struct: this receiver sets the appended on_subscribe_done
         * hook, so it must use the sized initializer -- the pointer-only init
         * stamps only the frozen prefix and would disable that callback. */
        moq_sub_cfg_init_sized(&scfg, sizeof(scfg));
        scfg.max_tracks = RECEIVER_SUB_MAX_TRACKS;
        scfg.max_objects = RECEIVER_SUB_MAX_OBJECTS;
        moq_sub_callbacks_init(&scfg.callbacks);
        scfg.callbacks.ctx = r;
        scfg.callbacks.on_subscribed = receiver_on_subscribed;
        scfg.callbacks.on_subscribe_error = receiver_on_subscribe_error;
        scfg.on_subscribe_done = receiver_on_subscribe_done;
        if (moq_sub_create(session, &r->alloc, &scfg, &r->sub) != MOQ_OK) {
            receiver_set_fatal(r, MOQ_MEDIA_RECEIVER_FATAL_SETUP_FAILED);
            return;
        }
        moq_sub_track_cfg_t tcfg;
        moq_sub_track_cfg_init(&tcfg);
        /* LargestObject is mandatory for the catalog: MSF-01 §5 requires the
         * catalog be obtained via SUBSCRIBE + a Joining FETCH(offset=0), and a
         * joining FETCH can only attach to a LargestObject subscription
         * (session_fetch.c). The SUBSCRIBE then carries subsequent live catalog
         * updates. */
        tcfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        tcfg.track_namespace = r->namespace_;
        tcfg.track_name = (moq_bytes_t){ r->catalog_name,
                                         r->catalog_name_len };
        if (moq_sub_subscribe(r->sub, &tcfg, now_us,
                              &r->catalog_sub) != MOQ_OK) {
            receiver_set_fatal(r, MOQ_MEDIA_RECEIVER_FATAL_SETUP_FAILED);
            return;
        }
    }

    (void)moq_sub_tick(r->sub, now_us);

    moq_sub_object_t obj;
    while (moq_sub_poll_object(r->sub, &obj) == MOQ_OK) {
        if (obj.track == r->catalog_sub) {
            receiver_ingest_catalog(r, obj.group_id, obj.object_id,
                                    obj.payload, now_us);
            moq_sub_object_cleanup(&obj);
            continue;
        }
        moq_media_track_t *t = track_for_sub(r, obj.track);
        if (!t) {
            moq_sub_object_cleanup(&obj);
            continue;
        }
        /* Ownership of obj's refs passes to the intake. A CMSF SAP timeline is
         * JSON metadata parsed to typed SAP records (poll_sap); an MSF §7 media
         * timeline is JSON parsed to typed records (poll_media_timeline) once
         * explicitly subscribed; any OTHER event timeline is neither SAP nor
         * media, so it is dropped (not surfaced and not leaked into poll_object). */
        if (t->is_sap_timeline)
            receiver_on_sap_object(r, t, &obj, now_us);
        else if (t->is_media_timeline)
            receiver_on_media_timeline_object(r, t, &obj, now_us);
        else if (t->is_event_timeline)
            moq_sub_object_cleanup(&obj);
        else
            receiver_on_media_object(r, t, &obj, now_us);
    }

    /* Process subscription completions recorded during the tick, now that this
     * tick's objects are drained: a "Track Ended" (live->VOD) re-arms the track
     * for re-subscribe below; any other done ends it. */
    receiver_process_subscription_dones(r);

    /* Issue + drain the catalog Joining FETCH (MSF-01 §5). Runs after the
     * subscribe poll so a SUBSCRIBE-delivered catalog update dedupes a later
     * FETCH object; may itself terminalize (catalog unusable), caught below. */
    receiver_pump_catalog_fetch(r, now_us);

    /* Catalog handling above may have terminalized (e.g. track-event
     * queue overflow): a terminal receiver must not start media
     * subscriptions in the same pump cycle. */
    pthread_mutex_lock(&r->mu);
    fatal = r->fatal;
    pthread_mutex_unlock(&r->mu);
    if (fatal) return;

    /* auto_subscribe desired state is seeded per-track in receiver_reconcile
     * (any generation), so the hook only needs to issue the subscriptions. */
    if (r->has_effective)
        receiver_reconcile_subscriptions(r, now_us);

    receiver_reconcile_delivery(r, now_us);
}

/* Subscriber teardown must happen where session calls are legal: posted to the
 * network thread. The context is independent of the receiver (which may be freed
 * before a queued task drains): it owns the subscriber plus a snapshot of its
 * live (PENDING/ACTIVE) subscription handles -- the catalog subscription and
 * every still-subscribed media/timeline track. The handles live inside the
 * subscriber, not the receiver, so they stay valid until moq_sub_destroy. */
typedef struct receiver_teardown {
    moq_alloc_t        alloc;
    moq_subscriber_t  *sub;
    moq_sub_track_t  **handles;   /* live handles to cancel; owned (or NULL) */
    size_t             count;
    size_t             cursor;    /* next handle to cancel (resume after retry) */
} receiver_teardown_t;

static void receiver_teardown_free(receiver_teardown_t *td)
{
    moq_alloc_t alloc = td->alloc;
    if (td->handles)
        alloc.free(td->handles, td->count * sizeof(*td->handles), alloc.ctx);
    alloc.free(td, sizeof(*td), alloc.ctx);
}

/* Subscriber teardown on the network thread (an endpoint post() task).
 *
 * Cancel every still-live receiver-owned subscription (moq_sub_unsubscribe ->
 * UNSUBSCRIBE, freeing the session entry) BEFORE destroying the facade. Without
 * this, a destroyed-receiver-but-still-alive-endpoint peer could keep streaming
 * objects for subscriptions nothing drains -- the facade hook is already gone.
 * moq_sub_destroy alone (the old behavior) only frees facade-local state and
 * leaves those session subscriptions active.
 *
 * Resumable via the task-return contract: a transiently full session queue
 * yields MOQ_ERR_WOULD_BLOCK (action queue full) or MOQ_ERR_BUFFER (the control
 * send scratch is momentarily full of not-yet-flushed actions -- and an
 * UNSUBSCRIBE, always far smaller than the buffer, can never be the permanent
 * "cannot ever fit" overflow). Both are transient: the task preserves its cursor
 * (and the subscriber + remaining handles) and returns MOQ_ERR_WOULD_BLOCK,
 * which tells the pump to REQUEUE this same task node for a later cycle -- no
 * re-allocation, no dependence on a second post() succeeding. The subscriber is
 * destroyed only after every live handle is cancelled, or the session is gone.
 *
 * session == NULL is the terminal drain (stop()) or an endpoint that never
 * produced a session: nothing live to leak into, so destroy locally and return
 * OK (the terminal drain must never requeue). WRONG_STATE (already terminal/DONE)
 * and STALE_HANDLE (entry already freed) are nothing-to-do; advance past them. */
static moq_result_t receiver_destroy_sub_task(moq_endpoint_t *ep,
                                              moq_session_t *session,
                                              uint64_t now_us, void *ctx)
{
    receiver_teardown_t *td = (receiver_teardown_t *)ctx;
    (void)ep;

    if (session) {
        while (td->cursor < td->count) {
            moq_sub_track_t *h = td->handles[td->cursor];
            if (h) {
                moq_result_t rc = moq_sub_unsubscribe(td->sub, h, now_us);
                if (rc == MOQ_ERR_WOULD_BLOCK || rc == MOQ_ERR_BUFFER)
                    return MOQ_ERR_WOULD_BLOCK;   /* keep state; retry next cycle */
            }
            td->cursor++;
        }
    }

    moq_sub_destroy(td->sub);
    receiver_teardown_free(td);
    return MOQ_OK;
}

/* Snapshot the receiver's subscriber + its live subscription handles into an
 * owned teardown context, moving ownership of r->sub to it (caller nulls r->sub).
 * Returns NULL with r->sub untouched if there is no subscriber or on allocation
 * failure (the caller then destroys r->sub locally, forgoing cancellation -- an
 * OOM-only degradation). A subscriber with no live handles yields a valid
 * zero-handle context (the task just destroys the facade). Runs on the app
 * thread after the hook is detached, so r is no longer touched by the network
 * thread and this read is single-threaded. */
static receiver_teardown_t *receiver_take_teardown(moq_media_receiver_t *r)
{
    if (!r->sub) return NULL;

    size_t n = 0;
    if (r->catalog_sub) n++;
    for (size_t i = 0; i < r->track_count; i++)
        if (r->tracks[i]->sub_track) n++;

    receiver_teardown_t *td =
        (receiver_teardown_t *)r->alloc.alloc(sizeof(*td), r->alloc.ctx);
    if (!td) return NULL;
    moq_sub_track_t **handles = NULL;
    if (n) {
        handles = (moq_sub_track_t **)r->alloc.alloc(
            n * sizeof(*handles), r->alloc.ctx);
        if (!handles) {
            r->alloc.free(td, sizeof(*td), r->alloc.ctx);
            return NULL;
        }
    }

    td->alloc  = r->alloc;
    td->sub    = r->sub;
    td->handles = handles;
    td->count  = n;
    td->cursor = 0;

    size_t k = 0;
    if (r->catalog_sub) handles[k++] = r->catalog_sub;
    for (size_t i = 0; i < r->track_count; i++)
        if (r->tracks[i]->sub_track) handles[k++] = r->tracks[i]->sub_track;
    return td;
}

/* -- cfg ---------------------------------------------------------------- */

void moq_media_receiver_cfg_init(moq_media_receiver_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = sizeof(*cfg);
    /* overflow.policy stays UNSET on purpose: forced choice (§6.4). */
}

void moq_media_receiver_cfg_init_live(moq_media_receiver_cfg_t *cfg)
{
    moq_media_receiver_cfg_init(cfg);
    if (cfg) cfg->overflow.policy = MOQ_MEDIA_OVERFLOW_DROP_TO_KEYFRAME;
}

void moq_media_receiver_cfg_init_flow_control(moq_media_receiver_cfg_t *cfg)
{
    moq_media_receiver_cfg_init(cfg);
    if (cfg) cfg->overflow.policy = MOQ_MEDIA_OVERFLOW_FLOW_CONTROL;
}

/* -- Construction -------------------------------------------------------- */

static moq_result_t receiver_validate_cfg(const moq_media_receiver_cfg_t *cfg)
{
    if (!cfg) return MOQ_ERR_INVAL;
    if (cfg->struct_size < sizeof(moq_media_receiver_cfg_t))
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
    if (cfg->time_mode != MOQ_MEDIA_TIME_RAW &&
        cfg->time_mode != MOQ_MEDIA_TIME_SHARED_EPOCH)
        return MOQ_ERR_INVAL;
    switch (cfg->overflow.policy) {
    case MOQ_MEDIA_OVERFLOW_DROP_TO_KEYFRAME:
    case MOQ_MEDIA_OVERFLOW_DROP_GROUP:
    case MOQ_MEDIA_OVERFLOW_FLOW_CONTROL:
        break;
    default:
        return MOQ_ERR_INVAL;   /* UNSET: the overflow choice is forced */
    }
    return MOQ_OK;
}

static void receiver_free(moq_media_receiver_t *r)
{
    for (size_t i = 0; i < r->track_count; i++)
        receiver_track_free(r, r->tracks[i]);
    if (r->tracks)
        r->alloc.free(r->tracks, r->track_cap * sizeof(moq_media_track_t *),
                      r->alloc.ctx);
    if (r->has_effective)
        moq_msf_catalog_cleanup(&r->alloc, &r->effective);
    for (size_t i = 0; i < r->cp_snap_count; i++)
        moq_msf_catalog_cleanup(&r->alloc, &r->cp_snaps[i]);
    if (r->cp_snaps)
        r->alloc.free(r->cp_snaps, r->cp_snap_cap * sizeof(moq_msf_catalog_t),
                      r->alloc.ctx);
    if (r->events)
        r->alloc.free(r->events, r->ev_cap * sizeof(receiver_event_t),
                      r->alloc.ctx);
    if (r->objs) {
        while (r->obj_head != r->obj_tail) {
            obj_entry_release(r, &r->objs[r->obj_head % r->ring_cap].obj);
            r->obj_head++;
        }
        r->alloc.free(r->objs, r->ring_cap * sizeof(receiver_obj_entry_t),
                      r->alloc.ctx);
    }
    if (r->saps)
        r->alloc.free(r->saps, r->sap_cap * sizeof(moq_media_sap_record_t),
                      r->alloc.ctx);
    if (r->mts)
        r->alloc.free(r->mts, r->mt_cap * sizeof(moq_media_timeline_record_t),
                      r->alloc.ctx);
    if (r->ns_parts)
        r->alloc.free(r->ns_parts,
                      r->namespace_.count * sizeof(moq_bytes_t),
                      r->alloc.ctx);
    if (r->ns_data)
        r->alloc.free(r->ns_data, r->ns_data_size, r->alloc.ctx);
    pthread_mutex_destroy(&r->mu);
    r->alloc.free(r, sizeof(*r), r->alloc.ctx);
}

static moq_result_t receiver_new(moq_endpoint_t *ep, bool owns,
                                 const moq_media_receiver_cfg_t *cfg,
                                 moq_media_receiver_t **out)
{
    const moq_alloc_t *alloc = moq_alloc_default();
    moq_media_receiver_t *r = (moq_media_receiver_t *)alloc->alloc(
        sizeof(*r), alloc->ctx);
    if (!r) return MOQ_ERR_NOMEM;
    memset(r, 0, sizeof(*r));
    r->alloc = *alloc;
    r->ep = ep;
    r->owns_endpoint = owns;
    r->auto_subscribe = cfg->auto_subscribe;
    r->time_mode = cfg->time_mode;
    r->overflow = cfg->overflow;
    r->max_stable_handles = RECEIVER_DEFAULT_MAX_STABLE_HANDLES;
    r->max_cp_snaps       = RECEIVER_DEFAULT_MAX_CP_SNAPS;
    r->max_retained_bytes = RECEIVER_DEFAULT_MAX_RETAINED_BYTES;
    pthread_mutex_init(&r->mu, NULL);

    /* Deep-copy the namespace and catalog track name into one buffer:
     * [ns part bytes...][catalog name bytes]. */
    moq_bytes_t catalog = cfg->catalog_track;
    if (catalog.len == 0)
        catalog = (moq_bytes_t){
            (const uint8_t *)MOQ_MSF_CATALOG_TRACK_NAME,
            MOQ_MSF_CATALOG_TRACK_NAME_LEN };
    size_t ns_bytes = 0;
    for (size_t i = 0; i < cfg->namespace_.count; i++)
        ns_bytes += cfg->namespace_.parts[i].len;

    r->ns_parts = (moq_bytes_t *)r->alloc.alloc(
        cfg->namespace_.count * sizeof(moq_bytes_t), r->alloc.ctx);
    r->ns_data = (uint8_t *)r->alloc.alloc(ns_bytes + catalog.len,
                                           r->alloc.ctx);
    if (!r->ns_parts || !r->ns_data) {
        if (r->ns_parts)
            r->alloc.free(r->ns_parts,
                          cfg->namespace_.count * sizeof(moq_bytes_t),
                          r->alloc.ctx);
        if (r->ns_data)
            r->alloc.free(r->ns_data, ns_bytes + catalog.len, r->alloc.ctx);
        r->ns_parts = NULL;
        r->ns_data = NULL;
        pthread_mutex_destroy(&r->mu);
        r->alloc.free(r, sizeof(*r), r->alloc.ctx);
        return MOQ_ERR_NOMEM;
    }
    size_t off = 0;
    for (size_t i = 0; i < cfg->namespace_.count; i++) {
        memcpy(r->ns_data + off, cfg->namespace_.parts[i].data,
               cfg->namespace_.parts[i].len);
        r->ns_parts[i] = (moq_bytes_t){ r->ns_data + off,
                                        cfg->namespace_.parts[i].len };
        off += cfg->namespace_.parts[i].len;
    }
    memcpy(r->ns_data + off, catalog.data, catalog.len);
    r->catalog_name = r->ns_data + off;
    r->catalog_name_len = catalog.len;
    r->namespace_ = (moq_namespace_t){ r->ns_parts, cfg->namespace_.count };
    /* receiver_free() releases ns_data with its full size. */
    r->ns_data_size = ns_bytes + catalog.len;

    r->ev_cap = cfg->max_track_events ? cfg->max_track_events
                                      : RECEIVER_DEFAULT_TRACK_EVENTS;
    r->events = (receiver_event_t *)r->alloc.alloc(
        r->ev_cap * sizeof(receiver_event_t), r->alloc.ctx);
    if (!r->events) {
        receiver_free(r);
        return MOQ_ERR_NOMEM;
    }

    r->obj_cap = cfg->overflow.max_objects ? cfg->overflow.max_objects
                                           : RECEIVER_DEFAULT_MAX_OBJECTS;
    r->byte_cap = cfg->overflow.max_bytes ? cfg->overflow.max_bytes
                                          : RECEIVER_DEFAULT_MAX_BYTES;
    /* FLOW_CONTROL absorbs in-flight overshoot past the threshold (up to
     * the soft ceiling); the physical ring must hold it. */
    r->ring_cap = (cfg->overflow.policy == MOQ_MEDIA_OVERFLOW_FLOW_CONTROL)
                      ? r->obj_cap + r->obj_cap / 2 + 1
                      : r->obj_cap;
    r->objs = (receiver_obj_entry_t *)r->alloc.alloc(
        r->ring_cap * sizeof(receiver_obj_entry_t), r->alloc.ctx);
    if (!r->objs) {
        receiver_free(r);
        return MOQ_ERR_NOMEM;
    }
    memset(r->objs, 0, r->ring_cap * sizeof(receiver_obj_entry_t));

    r->sap_cap = RECEIVER_DEFAULT_SAP_RECORDS;
    r->saps = (moq_media_sap_record_t *)r->alloc.alloc(
        r->sap_cap * sizeof(moq_media_sap_record_t), r->alloc.ctx);
    if (!r->saps) {
        receiver_free(r);
        return MOQ_ERR_NOMEM;
    }
    memset(r->saps, 0, r->sap_cap * sizeof(moq_media_sap_record_t));

    r->mt_cap = RECEIVER_DEFAULT_MT_RECORDS;
    r->mts = (moq_media_timeline_record_t *)r->alloc.alloc(
        r->mt_cap * sizeof(moq_media_timeline_record_t), r->alloc.ctx);
    if (!r->mts) {
        receiver_free(r);
        return MOQ_ERR_NOMEM;
    }
    memset(r->mts, 0, r->mt_cap * sizeof(moq_media_timeline_record_t));
    r->stats.struct_size = (uint32_t)sizeof(r->stats);

    /* Production always passes a real endpoint. The MOQ_MEDIA_RECEIVER_TESTING
     * seam (below) calls receiver_new with ep == NULL to build a fully
     * initialized receiver with no endpoint, so the scripted-peer harness can
     * drive receiver_hook against a session it controls. The guard is a no-op
     * in shipping builds (ep is never NULL there). */
    if (ep) {
        moq_result_t arc = moq_endpoint_attach_hook(
            ep, MOQ_ENDPOINT_HOOK_RECEIVER, receiver_hook, r);
        if (arc < 0) {
            receiver_free(r);
            return arc;
        }
    }

    *out = r;
    return MOQ_OK;
}

moq_result_t moq_media_receiver_attach(moq_endpoint_t *ep,
                                       const moq_media_receiver_cfg_t *cfg,
                                       moq_media_receiver_t **out)
{
    if (!ep || !out) return MOQ_ERR_INVAL;
    *out = NULL;
    moq_result_t rc = receiver_validate_cfg(cfg);
    if (rc < 0) return rc;
    if (cfg->endpoint != NULL) return MOQ_ERR_INVAL;  /* attach borrows */
    return receiver_new(ep, false, cfg, out);
}

moq_result_t moq_media_receiver_create(const moq_media_receiver_cfg_t *cfg,
                                       moq_media_receiver_t **out)
{
    if (!out) return MOQ_ERR_INVAL;
    *out = NULL;
    moq_result_t rc = receiver_validate_cfg(cfg);
    if (rc < 0) return rc;
    if (cfg->endpoint == NULL) return MOQ_ERR_INVAL;  /* create owns */

    moq_endpoint_t *ep = NULL;
    rc = moq_endpoint_connect(cfg->endpoint, &ep);
    if (rc < 0) return rc;

    rc = receiver_new(ep, true, cfg, out);
    if (rc < 0) {
        (void)moq_endpoint_stop(ep);
        moq_endpoint_destroy(ep);
        return rc;
    }
    return MOQ_OK;
}

void moq_media_receiver_destroy(moq_media_receiver_t *r)
{
    if (!r) return;

    moq_endpoint_detach_hook(r->ep, MOQ_ENDPOINT_HOOK_RECEIVER, r);

    /* Tear the subscriber down where session calls are legal: snapshot its live
     * subscription handles and post a task that cancels each (UNSUBSCRIBE) before
     * destroying the facade. A refused post is almost always the terminal case
     * (MOQ_ERR_CLOSED): the network thread can no longer touch the subscriber and
     * the session is gone, so the inline NULL-session teardown is single-threaded
     * and has nothing live to cancel. The only other refusal is MOQ_ERR_NOMEM
     * (cannot allocate the task node) while the endpoint is still live; session
     * calls are illegal off the network thread, so the best we can do is the same
     * local destroy -- an OOM-only degradation that forgoes cancellation. */
    if (r->sub) {
        receiver_teardown_t *td = receiver_take_teardown(r);
        if (td) {
            if (moq_endpoint_post_retryable(r->ep, receiver_destroy_sub_task, td)
                    != MOQ_OK)
                (void)receiver_destroy_sub_task(r->ep, NULL, 0, td);
        } else {
            /* No subscriber went missing here (checked above); a NULL means
             * the handle snapshot could not be allocated. Degrade to a local
             * facade destroy without cancellation (OOM-only path). */
            moq_sub_destroy(r->sub);
        }
        r->sub = NULL;
    }

    if (r->owns_endpoint) {
        (void)moq_endpoint_stop(r->ep);
        moq_endpoint_destroy(r->ep);
    }
    receiver_free(r);
}

/* -- App-thread surface --------------------------------------------------- */

static bool receiver_terminal(const moq_media_receiver_t *r)
{
    moq_media_receiver_t *mr = (moq_media_receiver_t *)(uintptr_t)r;
    pthread_mutex_lock(&mr->mu);
    bool fatal = mr->fatal;
    pthread_mutex_unlock(&mr->mu);
    return fatal || moq_endpoint_is_closed(r->ep);
}

moq_result_t moq_media_receiver_wait(moq_media_receiver_t *r,
                                     uint64_t timeout_us)
{
    if (!r) return MOQ_ERR_INVAL;

    pthread_mutex_lock(&r->mu);
    bool have = (r->ev_tail != r->ev_head) || (r->obj_tail != r->obj_head) ||
                (r->sap_tail != r->sap_head) || (r->mt_tail != r->mt_head);
    pthread_mutex_unlock(&r->mu);
    if (have) return MOQ_OK;
    if (receiver_terminal(r)) return MOQ_ERR_CLOSED;

    moq_result_t rc = moq_endpoint_wait(r->ep, timeout_us);
    if (rc == MOQ_ERR_INTERRUPTED || rc == MOQ_DONE) return rc;

    pthread_mutex_lock(&r->mu);
    have = (r->ev_tail != r->ev_head) || (r->obj_tail != r->obj_head) ||
           (r->sap_tail != r->sap_head) || (r->mt_tail != r->mt_head);
    pthread_mutex_unlock(&r->mu);
    if (have) return MOQ_OK;
    if (receiver_terminal(r)) return MOQ_ERR_CLOSED;
    return rc < 0 ? rc : MOQ_OK;
}

moq_result_t moq_media_receiver_poll_track(moq_media_receiver_t *r,
                                           moq_media_track_event_t *ev,
                                           size_t ev_size)
{
    /* Require the frozen v0 floor (not sizeof(current)), so an older caller is
     * still served by a future library; copy only n = min(caller, current)
     * bytes so a newer library never writes past an older caller's struct. */
    if (!r || !ev || ev_size < MEDIA_TRACK_EVENT_V0_SIZE) return MOQ_ERR_INVAL;
    size_t n = ev_size < sizeof(*ev) ? ev_size : sizeof(*ev);
    memset(ev, 0, n);

    pthread_mutex_lock(&r->mu);
    if (r->ev_tail == r->ev_head) {
        bool fatal = r->fatal;
        pthread_mutex_unlock(&r->mu);
        if (fatal || moq_endpoint_is_closed(r->ep))
            return MOQ_ERR_CLOSED;
        return MOQ_DONE;
    }
    receiver_event_t e = r->events[r->ev_head % r->ev_cap];
    r->ev_head++;
    pthread_mutex_unlock(&r->mu);

    moq_media_track_event_t full;
    memset(&full, 0, sizeof(full));
    full.struct_size = (uint32_t)n;
    full.kind = e.kind;
    full.track = e.track;
    full.desc = e.track ? &e.track->desc : NULL;
    full.config_generation = 0;
    memcpy(ev, &full, n);
    return MOQ_OK;
}

const moq_media_track_desc_t *moq_media_track_desc_get(
    const moq_media_track_t *track)
{
    return track ? &track->desc : NULL;
}

const moq_cmsf_content_protection_t *
moq_media_receiver_find_content_protection(
    const moq_media_receiver_t *r, moq_bytes_t ref_id)
{
    if (!r || !r->has_effective) return NULL;
    return moq_msf_catalog_find_content_protection(&r->effective, ref_id);
}

/* -- Per-track subscription control (app thread) ---------------------- *
 * These only validate + record desired state under r->mu, then nudge the
 * network thread with the internal non-allocating moq_endpoint_wake() (it
 * queues no task and cannot fail, so the command is atomic -- no receiver
 * pointer is handed to a deferred task either). The hook reconciles desired
 * state against the actual subscriptions, so no facade call ever runs
 * off-thread. */

void moq_media_receiver_track_subscribe_cfg_init(
    moq_media_receiver_track_subscribe_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = sizeof(*cfg);
    cfg->start = MOQ_MEDIA_START_CURRENT;
}

/* True when `track` is one of this receiver's catalog track handles. The
 * handle list grows across catalog updates (network thread, under r->mu), so
 * the read happens under r->mu to order against the building hook. */
static bool receiver_owns_track(moq_media_receiver_t *r,
                                const moq_media_track_t *track)
{
    for (size_t i = 0; i < r->track_count; i++)
        if (r->tracks[i] == track) return true;
    return false;
}

moq_result_t moq_media_receiver_subscribe_track(
    moq_media_receiver_t *r, moq_media_track_t *track,
    const moq_media_receiver_track_subscribe_cfg_t *cfg)
{
    if (!r || !track) return MOQ_ERR_INVAL;
    if (r->ep && moq_endpoint_is_closed(r->ep)) return MOQ_ERR_CLOSED;

    moq_subscribe_filter_t filt = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    bool has_prio = false;
    uint8_t prio = 0;
    if (cfg) {
        /* ABI gate: reject a struct too small to hold the fields we read; a
         * larger (future-extended) struct is fine -- we only touch the base. */
        if (cfg->struct_size < sizeof(*cfg)) return MOQ_ERR_INVAL;
        if (cfg->start != MOQ_MEDIA_START_CURRENT &&
            cfg->start != MOQ_MEDIA_START_NEXT_GROUP)
            return MOQ_ERR_INVAL;
        if (cfg->start == MOQ_MEDIA_START_NEXT_GROUP)
            filt = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        has_prio = cfg->has_priority;
        prio = cfg->priority;
    }

    pthread_mutex_lock(&r->mu);
    if (r->fatal) { pthread_mutex_unlock(&r->mu); return MOQ_ERR_CLOSED; }
    if (!receiver_owns_track(r, track)) {
        pthread_mutex_unlock(&r->mu);
        return MOQ_ERR_INVAL;
    }
    /* A track that has ended (peer-rejected, or end-of-track) or been removed
     * from the catalog can never be re-established: the reconcile permanently
     * skips such tracks, so refuse here rather than recording desired state
     * that will never take effect. */
    if (track->ended || track->removed) {
        pthread_mutex_unlock(&r->mu);
        return MOQ_ERR_WRONG_STATE;
    }
    /* MSF §5.2.2 namespace override: a track that declares its own namespace
     * cannot be subscribed yet -- the MSF namespace string has no defined
     * mapping to a MoQT namespace tuple, and subscribing under the receiver
     * namespace would target the wrong namespace. Refuse explicitly rather than
     * subscribe the wrong thing. (Tracks that inherit the catalog namespace --
     * the common case -- subscribe normally.) */
    if (track->key_has_ns) {
        pthread_mutex_unlock(&r->mu);
        return MOQ_ERR_UNSUPPORTED;
    }
    /* Record start mode + priority ONLY on the off->on transition. They
     * configure the subscription when it is first established; a redundant or
     * rapid second subscribe_track (still "wanted", whether pending, active, or
     * paused) is an idempotent enable and must not overwrite them before the
     * network hook issues the first SUBSCRIBE (header contract). */
    if (!track->desired_subscribed) {
        track->desired_filter = filt;
        track->desired_has_priority = has_prio;
        track->desired_priority = prio;
    }
    track->desired_subscribed = true;
    pthread_mutex_unlock(&r->mu);

    /* Non-allocating wake: the desired state is recorded; nudge the network
     * thread to reconcile it. This cannot fail, so the command is atomic --
     * MOQ_OK means the state change is committed and scheduled. */
    moq_endpoint_wake(r->ep);
    return MOQ_OK;
}

moq_result_t moq_media_receiver_unsubscribe_track(
    moq_media_receiver_t *r, moq_media_track_t *track)
{
    if (!r || !track) return MOQ_ERR_INVAL;
    if (r->ep && moq_endpoint_is_closed(r->ep)) return MOQ_ERR_CLOSED;

    pthread_mutex_lock(&r->mu);
    if (r->fatal) { pthread_mutex_unlock(&r->mu); return MOQ_ERR_CLOSED; }
    if (!receiver_owns_track(r, track)) {
        pthread_mutex_unlock(&r->mu);
        return MOQ_ERR_INVAL;
    }
    if (track->removed) {
        pthread_mutex_unlock(&r->mu);
        return MOQ_ERR_WRONG_STATE;
    }
    track->desired_subscribed = false;
    /* Purge the track's queued objects now, under the same lock that guards the
     * queue (a pure data operation -- no session calls -- so it is safe off the
     * network thread). Doing it here rather than in the delivery reconcile makes
     * the purge independent of the current forward state: a track already paused
     * by FLOW_CONTROL (forward already false) is purged just the same. Objects
     * that arrive before the forward=false update lands are dropped at intake. */
    obj_compact_locked(r, keep_not_track, track);
    track->has_drop_filter = false;
    pthread_mutex_unlock(&r->mu);

    moq_endpoint_wake(r->ep);
    return MOQ_OK;
}

moq_result_t moq_media_receiver_track_state(
    const moq_media_receiver_t *r, const moq_media_track_t *track,
    moq_media_track_state_t *out)
{
    if (!r || !track || !out) return MOQ_ERR_INVAL;

    /* Snapshot endpoint closure OUTSIDE r->mu (it takes the endpoint mutex; the
     * pump takes endpoint-mu then r->mu, so locking the other way could deadlock).
     * A clean close is terminal -> ENDED, same as a receiver-fatal. */
    bool closed = r->ep && moq_endpoint_is_closed(r->ep);

    /* The mutex is logically mutable; this is a read-only snapshot. Every field
     * read below is mu-protected: ended / requested / established /
     * desired_subscribed are written under r->mu, and paused is the FLOW_CONTROL
     * flag. */
    moq_media_receiver_t *rr = (moq_media_receiver_t *)r;
    pthread_mutex_lock(&rr->mu);

    bool owns = false;
    for (size_t i = 0; i < rr->track_count; i++)
        if (rr->tracks[i] == track) { owns = true; break; }
    if (!owns) {
        pthread_mutex_unlock(&rr->mu);
        return MOQ_ERR_INVAL;                 /* foreign handle: programmer error */
    }
    if (track->removed) {
        pthread_mutex_unlock(&rr->mu);
        return MOQ_ERR_WRONG_STATE;           /* removed from catalog: terminal */
    }

    moq_media_track_state_t st;
    bool issued = track->requested || track->established;
    if (rr->fatal || closed || track->ended) {
        st = MOQ_MEDIA_TRACK_STATE_ENDED;
    } else if (!track->desired_subscribed) {
        /* Disabled: PAUSED_APP if it ever had a subscription issued (it persists,
         * paused), else never subscribed. */
        st = issued ? MOQ_MEDIA_TRACK_STATE_PAUSED_APP
                    : MOQ_MEDIA_TRACK_STATE_DISCOVERED;
    } else if (!track->established) {
        st = MOQ_MEDIA_TRACK_STATE_PENDING;   /* wanted; SUBSCRIBE not yet accepted */
    } else if (rr->overflow.policy == MOQ_MEDIA_OVERFLOW_FLOW_CONTROL &&
               rr->paused) {
        st = MOQ_MEDIA_TRACK_STATE_PAUSED_FLOW;
    } else {
        st = MOQ_MEDIA_TRACK_STATE_ACTIVE;
    }

    pthread_mutex_unlock(&rr->mu);
    *out = st;
    return MOQ_OK;
}

moq_result_t moq_media_receiver_poll_object(moq_media_receiver_t *r,
                                            moq_media_object_t *obj,
                                            size_t obj_size)
{
    /* See poll_track: frozen v0 floor, copy clamped to min(caller, current). The
     * ref fields live in the v0 prefix, so n >= v0 always carries ownership. */
    if (!r || !obj || obj_size < MEDIA_OBJECT_V0_SIZE) return MOQ_ERR_INVAL;
    size_t n = obj_size < sizeof(*obj) ? obj_size : sizeof(*obj);
    memset(obj, 0, n);

    if (moq_endpoint_interrupted_internal(r->ep))
        return MOQ_ERR_INTERRUPTED;

    pthread_mutex_lock(&r->mu);
    if (r->obj_tail == r->obj_head) {
        bool fatal = r->fatal;
        pthread_mutex_unlock(&r->mu);
        if (fatal || moq_endpoint_is_closed(r->ep))
            return MOQ_ERR_CLOSED;
        return MOQ_DONE;
    }
    receiver_obj_entry_t *e = &r->objs[r->obj_head % r->ring_cap];
    memcpy(obj, &e->obj, n);            /* exclusive ownership transfers */
    obj->struct_size = (uint32_t)n;
    r->obj_sizes_total -= obj_entry_bytes(&e->obj);
    memset(e, 0, sizeof(*e));
    r->obj_head++;
    /* FLOW_CONTROL: a paused subscription stops the traffic that would
     * otherwise drive pump cycles, so crossing the low-water mark must
     * nudge the network thread to run the resume reconcile. */
    bool nudge = false;
    if (r->overflow.policy == MOQ_MEDIA_OVERFLOW_FLOW_CONTROL &&
        r->paused && !r->resume_nudged &&
        r->obj_tail - r->obj_head <= r->obj_cap / 2) {
        r->resume_nudged = true;
        nudge = true;
    }
    pthread_mutex_unlock(&r->mu);
    if (nudge)
        moq_endpoint_wake(r->ep);
    return MOQ_OK;
}

void moq_media_object_cleanup(moq_media_object_t *obj)
{
    if (!obj) return;
    /* The ref fields live in the v0 prefix, so a poll-filled obj (struct_size >=
     * v0) or a zeroed obj (NULL refs) both read safely. Bound the final zeroing
     * by the stamped struct_size so a smaller caller's struct is never overrun. */
    if (obj->payload_ref)    moq_rcbuf_decref(obj->payload_ref);
    if (obj->properties_ref) moq_rcbuf_decref(obj->properties_ref);
    if (obj->samples_owned) {
        const moq_alloc_t *a = moq_alloc_default();
        a->free(obj->samples_owned,
                obj->sample_count * sizeof(moq_cmaf_sample_t), a->ctx);
    }
    size_t n = obj->struct_size;
    if (n > sizeof(*obj)) n = sizeof(*obj);
    if (n) memset(obj, 0, n);
}

moq_result_t moq_media_receiver_poll_sap(moq_media_receiver_t *r,
                                         moq_media_sap_record_t *out,
                                         size_t out_size)
{
    if (!r || !out || out_size < MEDIA_SAP_RECORD_V0_SIZE) return MOQ_ERR_INVAL;
    size_t n = out_size < sizeof(*out) ? out_size : sizeof(*out);
    memset(out, 0, n);

    pthread_mutex_lock(&r->mu);
    if (r->sap_tail == r->sap_head) {
        bool fatal = r->fatal;
        pthread_mutex_unlock(&r->mu);
        if (fatal || moq_endpoint_is_closed(r->ep)) return MOQ_ERR_CLOSED;
        return MOQ_DONE;
    }
    moq_media_sap_record_t rec = r->saps[r->sap_head % r->sap_cap];
    r->sap_head++;
    pthread_mutex_unlock(&r->mu);
    rec.struct_size = (uint32_t)n;
    memcpy(out, &rec, n);
    return MOQ_OK;
}

moq_result_t moq_media_receiver_poll_media_timeline(
    moq_media_receiver_t *r,
    moq_media_timeline_record_t *out,
    size_t out_size)
{
    if (!r || !out || out_size < MEDIA_TIMELINE_RECORD_V0_SIZE)
        return MOQ_ERR_INVAL;
    size_t n = out_size < sizeof(*out) ? out_size : sizeof(*out);
    memset(out, 0, n);

    pthread_mutex_lock(&r->mu);
    if (r->mt_tail == r->mt_head) {
        bool fatal = r->fatal;
        pthread_mutex_unlock(&r->mu);
        if (fatal || moq_endpoint_is_closed(r->ep)) return MOQ_ERR_CLOSED;
        return MOQ_DONE;
    }
    moq_media_timeline_record_t rec = r->mts[r->mt_head % r->mt_cap];
    r->mt_head++;
    pthread_mutex_unlock(&r->mu);
    rec.struct_size = (uint32_t)n;
    memcpy(out, &rec, n);
    return MOQ_OK;
}

moq_result_t moq_media_receiver_get_stats(const moq_media_receiver_t *r,
                                          moq_media_receiver_stats_t *out,
                                          size_t out_size)
{
    if (!r || !out || out_size < MEDIA_RECEIVER_STATS_V0_SIZE)
        return MOQ_ERR_INVAL;
    moq_media_receiver_t *mr = (moq_media_receiver_t *)(uintptr_t)r;
    /* Snapshot into a full local, then copy the clamped prefix to the caller. */
    moq_media_receiver_stats_t snap;
    pthread_mutex_lock(&mr->mu);
    snap = mr->stats;
    snap.objects_queued = mr->obj_tail - mr->obj_head;
    snap.bytes_queued = mr->obj_sizes_total;
    snap.paused = mr->paused;
    pthread_mutex_unlock(&mr->mu);
    size_t n = out_size < sizeof(*out) ? out_size : sizeof(*out);
    snap.struct_size = (uint32_t)n;
    memcpy(out, &snap, n);
    return MOQ_OK;
}

bool moq_media_receiver_is_closed(const moq_media_receiver_t *r)
{
    if (!r) return true;
    return moq_endpoint_is_closed(r->ep);
}

bool moq_media_receiver_is_fatal(const moq_media_receiver_t *r)
{
    if (!r) return false;
    moq_media_receiver_t *mr = (moq_media_receiver_t *)(uintptr_t)r;
    pthread_mutex_lock(&mr->mu);
    bool fatal = mr->fatal;
    pthread_mutex_unlock(&mr->mu);
    return fatal || moq_endpoint_is_fatal(r->ep);
}

uint64_t moq_media_receiver_fatal_code(const moq_media_receiver_t *r)
{
    if (!r) return 0;
    moq_media_receiver_t *mr = (moq_media_receiver_t *)(uintptr_t)r;
    pthread_mutex_lock(&mr->mu);
    bool fatal = mr->fatal;
    uint64_t code = mr->fatal_code;
    pthread_mutex_unlock(&mr->mu);
    if (fatal) return code;
    return moq_endpoint_fatal_code(r->ep);
}

#ifdef MOQ_MEDIA_RECEIVER_TESTING
/* -- White-box test seam (not built into the shipping library) ---------- *
 * Drives the catalog ingest/reconcile path with crafted (group,object,json)
 * objects, with no endpoint or network. The receiver's per-handle public APIs
 * tolerate a NULL endpoint (treated as not-closed), so subscribe/unsubscribe/
 * track_state can be exercised against a test receiver too. */

moq_media_receiver_t *moq_media_receiver_test_new(bool auto_subscribe)
{
    const moq_alloc_t *alloc = moq_alloc_default();
    moq_media_receiver_t *r = (moq_media_receiver_t *)alloc->alloc(
        sizeof(*r), alloc->ctx);
    if (!r) return NULL;
    memset(r, 0, sizeof(*r));
    r->alloc = *alloc;
    r->ep = NULL;
    r->auto_subscribe = auto_subscribe;
    r->max_stable_handles = RECEIVER_DEFAULT_MAX_STABLE_HANDLES;
    r->max_cp_snaps       = RECEIVER_DEFAULT_MAX_CP_SNAPS;
    r->max_retained_bytes = RECEIVER_DEFAULT_MAX_RETAINED_BYTES;
    pthread_mutex_init(&r->mu, NULL);
    r->ev_cap = RECEIVER_DEFAULT_TRACK_EVENTS;
    r->events = (receiver_event_t *)alloc->alloc(
        r->ev_cap * sizeof(receiver_event_t), alloc->ctx);
    r->obj_cap = RECEIVER_DEFAULT_MAX_OBJECTS;
    r->ring_cap = r->obj_cap;
    r->byte_cap = RECEIVER_DEFAULT_MAX_BYTES;
    r->objs = (receiver_obj_entry_t *)alloc->alloc(
        r->ring_cap * sizeof(receiver_obj_entry_t), alloc->ctx);
    if (!r->events || !r->objs) { receiver_free(r); return NULL; }
    memset(r->objs, 0, r->ring_cap * sizeof(receiver_obj_entry_t));
    return r;
}

void moq_media_receiver_test_free(moq_media_receiver_t *r)
{
    if (r) receiver_free(r);
}

void moq_media_receiver_test_ingest(moq_media_receiver_t *r, uint64_t group,
                                    uint64_t object, const char *json,
                                    size_t len)
{
    moq_rcbuf_t *buf = NULL;
    if (moq_rcbuf_create(&r->alloc, (const uint8_t *)json, len, &buf) != MOQ_OK)
        return;
    receiver_ingest_catalog(r, group, object, buf, 0);
    moq_rcbuf_decref(buf);
}

bool moq_media_receiver_test_poll(moq_media_receiver_t *r,
                                  moq_media_track_event_kind_t *kind,
                                  moq_media_track_t **track)
{
    pthread_mutex_lock(&r->mu);
    if (r->ev_tail == r->ev_head) { pthread_mutex_unlock(&r->mu); return false; }
    receiver_event_t e = r->events[r->ev_head % r->ev_cap];
    r->ev_head++;
    pthread_mutex_unlock(&r->mu);
    if (kind) *kind = e.kind;
    if (track) *track = e.track;
    return true;
}

uint64_t moq_media_receiver_test_catalog_drops(const moq_media_receiver_t *r)
{
    return r->stats.catalog_drops;
}

bool moq_media_receiver_test_is_fatal(const moq_media_receiver_t *r)
{
    return r->fatal;
}

uint64_t moq_media_receiver_test_fatal_code(const moq_media_receiver_t *r)
{
    return r->fatal_code;
}

/* Lower the receiver-lifetime retained-state caps so a unit test can exercise
 * the bounded-drop / first-catalog-fatal paths without feeding thousands of
 * generations. */
void moq_media_receiver_test_set_limits(moq_media_receiver_t *r,
                                        size_t max_stable_handles,
                                        size_t max_cp_snaps,
                                        uint64_t max_retained_bytes)
{
    r->max_stable_handles = max_stable_handles;
    r->max_cp_snaps       = max_cp_snaps;
    r->max_retained_bytes = max_retained_bytes;
}

size_t moq_media_receiver_test_track_count(const moq_media_receiver_t *r)
{
    return r->track_count;
}

/* Enqueue one media-timeline record directly (the internal mt_queue_push under
 * r->mu), so a wait-contract test can make the media-timeline queue the ONLY
 * ready queue without driving a full §7 timeline ingest. The minimal
 * test_new() receiver has no mt ring; allocate a small one lazily here
 * (receiver_free releases it like the production ring). */
void moq_media_receiver_test_push_media_timeline(moq_media_receiver_t *r,
                                                 uint64_t group,
                                                 uint64_t object,
                                                 uint64_t media_time_ms,
                                                 uint64_t wallclock_ms)
{
    if (!r->mts) {
        r->mt_cap = 8;
        r->mts = (moq_media_timeline_record_t *)r->alloc.alloc(
            r->mt_cap * sizeof(moq_media_timeline_record_t), r->alloc.ctx);
        if (!r->mts) { r->mt_cap = 0; return; }
        memset(r->mts, 0, r->mt_cap * sizeof(moq_media_timeline_record_t));
    }
    moq_msf_media_timeline_record_t src;
    memset(&src, 0, sizeof(src));
    src.group = group;
    src.object = object;
    src.media_time_ms = media_time_ms;
    src.wallclock_ms = wallclock_ms;
    pthread_mutex_lock(&r->mu);
    mt_queue_push(r, NULL, &src);
    pthread_mutex_unlock(&r->mu);
}

/* Build a fully-initialized receiver (namespace, catalog name, overflow ring,
 * event/object/sap/mt queues) from a cfg, with NO endpoint -- so a deterministic
 * scripted-peer harness can drive the REAL receiver_hook (auto-subscribe,
 * catalog ingest, object routing) against a session it owns. receiver_new with
 * ep == NULL skips the endpoint hook attach (see the if (ep) guard there). */
moq_media_receiver_t *moq_media_receiver_test_new_cfg(
    const moq_media_receiver_cfg_t *cfg)
{
    if (receiver_validate_cfg(cfg) < 0) return NULL;
    moq_media_receiver_t *r = NULL;
    if (receiver_new(NULL, false, cfg, &r) != MOQ_OK) return NULL;
    return r;
}

/* Drive the real endpoint pump hook with a caller-controlled session. The hook
 * itself ignores the endpoint argument (receiver_hook does (void)ep;), so a
 * NULL endpoint is sound here. */
void moq_media_receiver_test_pump(moq_media_receiver_t *r,
                                  moq_session_t *session, uint64_t now_us)
{
    receiver_hook(NULL, session, now_us, r);
}

/* Run the production subscriber-teardown path (live-handle snapshot + per-handle
 * UNSUBSCRIBE + facade destroy) against a caller-supplied session, then free the
 * receiver. The no-endpoint analogue of moq_media_receiver_destroy()'s
 * network-thread teardown post: production posts receiver_destroy_sub_task to the
 * endpoint thread; the scriptable harness has no endpoint, so it supplies the
 * session directly. Exercises receiver_take_teardown + receiver_destroy_sub_task
 * exactly as production does. */
void moq_media_receiver_test_destroy_with_session(moq_media_receiver_t *r,
                                                  moq_session_t *session,
                                                  uint64_t now_us)
{
    if (r->sub) {
        receiver_teardown_t *td = receiver_take_teardown(r);
        if (td)
            (void)receiver_destroy_sub_task(NULL, session, now_us, td);
        else
            moq_sub_destroy(r->sub);
        r->sub = NULL;
    }
    receiver_free(r);
}

/* Post the production teardown task to a (test) endpoint exactly as
 * moq_media_receiver_destroy() does, then free the receiver shell. Ownership of
 * the subscriber + handle snapshot moves into the task node; driving the
 * endpoint's pump (moq_endpoint_test_drain_posted) then exercises the real
 * requeue-on-WOULD_BLOCK retry across cycles. Returns the post result. */
moq_result_t moq_media_receiver_test_post_teardown(moq_media_receiver_t *r,
                                                   moq_endpoint_t *ep)
{
    moq_result_t rc = MOQ_OK;
    if (r->sub) {
        receiver_teardown_t *td = receiver_take_teardown(r);
        if (td) {
            rc = moq_endpoint_post_retryable(ep, receiver_destroy_sub_task, td);
            if (rc != MOQ_OK)
                (void)receiver_destroy_sub_task(ep, NULL, 0, td);
        } else {
            moq_sub_destroy(r->sub);
        }
        r->sub = NULL;
    }
    receiver_free(r);
    return rc;
}
#endif /* MOQ_MEDIA_RECEIVER_TESTING */
