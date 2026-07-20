#include <moq/publisher.h>
#include <moq/rcbuf.h>
#include <moq/wire.h>
#include "../internal/validate.h"
#include <stddef.h>
#include <string.h>

typedef enum {
    PUB_NS_NONE = 0,
    PUB_NS_PENDING,
    PUB_NS_ACCEPTED,
    PUB_NS_TERMINAL,
} pub_ns_state_t;

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
} pub_sub_slot_t;

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

    uint8_t             priority;

    bool                ended;       /* moq_pub_end_track succeeded; no more
                                        new objects accepted on this track */
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
};

typedef struct {
    bool                active;
    moq_subscription_t  sub;
    moq_pub_track_t    *track;
    bool                accept;
    moq_request_error_t reject_code;
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
    for (size_t i = 0; i < t->slot_cap; i++)
        if (t->slots[i].active && moq_subscription_eq(t->slots[i].sub, sub))
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

static pub_sub_slot_t *track_first_active_slot(moq_pub_track_t *t)
{
    for (size_t i = 0; i < t->slot_cap; i++)
        if (t->slots[i].active)
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

static void track_set_subscriber(moq_pub_track_t *t, moq_subscription_t sub)
{
    pub_sub_slot_t *s = track_find_free_slot(t);
    if (!s) return;
    s->active = true;
    s->kind = PUB_SLOT_SUBSCRIPTION;
    s->sub = sub;
    s->sg_open = false;
    s->streaming = false;
    s->cur_group = 0;
}

static void track_set_publication(moq_pub_track_t *t, moq_publication_t pub)
{
    pub_sub_slot_t *s = track_find_free_slot(t);
    if (!s) return;
    s->active = true;
    s->kind = PUB_SLOT_PUBLICATION;
    s->pub = pub;
    s->sg_open = false;
    s->streaming = false;
    s->cur_group = 0;
}

static pub_sub_slot_t *track_find_publication_slot(moq_pub_track_t *t)
{
    for (size_t i = 0; i < t->slot_cap; i++)
        if (t->slots[i].active && t->slots[i].kind == PUB_SLOT_PUBLICATION)
            return &t->slots[i];
    return NULL;
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
    track_release_retained(t);
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
        track_set_subscriber(p->track, p->sub);
        if (out_joined) *out_joined = p->track;
    }

    p->active = false;
    return rc < 0 ? rc : MOQ_OK;
}

/* -- Config field access ------------------------------------------- */

static bool pub_cfg_has_field(const moq_pub_cfg_t *cfg,
                               size_t offset, size_t size)
{
    return cfg->struct_size >= offset && size <= cfg->struct_size - offset;
}

/* -- Public API --------------------------------------------------- */

void moq_pub_callbacks_init(moq_pub_callbacks_t *cb) {
    if (!cb) return;
    memset(cb, 0, sizeof(*cb));
    cb->struct_size = sizeof(*cb);
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
    if (n >= offsetof(moq_pub_cfg_t, callbacks) + sizeof(cfg->callbacks))
        moq_pub_callbacks_init(&cfg->callbacks);
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
        sizeof(moq_pub_callbacks_t),                          /* updated */
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
            size_t avail = cfg->struct_size - cb_off;
            if (avail > sizeof(p->callbacks))
                avail = sizeof(p->callbacks);
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
 * before max_retained_bytes and has_publisher_priority were appended; it
 * matches the add_track minimum struct_size, offsetof(max_retained_bytes)).
 * The pointer-only initializer touches only this prefix, so an old caller
 * that allocated the original-sized struct is never overflowed, and BOTH
 * appended fields stay disabled unless _init_sized opts in. */
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
        if (psl && psl->streaming) return MOQ_ERR_WRONG_STATE;
        if (psl && psl->sg_open) {
            moq_result_t rc = moq_session_reset_subgroup(pub->session,
                psl->sg, 0x0, now_us);
            if (rc == MOQ_ERR_WOULD_BLOCK) return MOQ_ERR_WOULD_BLOCK;
            if (rc != MOQ_OK && rc != MOQ_ERR_STALE_HANDLE) return rc;
            psl->sg_open = false;
        }
        moq_finish_publish_cfg_t fcfg;
        moq_finish_publish_cfg_init(&fcfg);
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
         * data stream). The facade does not track per-subscription stream counts,
         * so report the unknown sentinel (2^62 - 1) per the transport spec. On
         * success the session frees the subscription; clear our slot to match. */
        moq_done_subscribe_cfg_t dcfg;
        moq_done_subscribe_cfg_init(&dcfg);
        dcfg.status_code = MOQ_PUB_DONE_TRACK_ENDED;
        dcfg.stream_count = (UINT64_C(1) << 62) - 1;  /* unknown stream count */
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

void moq_pub_object_cfg_init(moq_pub_object_cfg_t *cfg) {
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = sizeof(*cfg);
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

    /* Fan-out: today this sends to the first active slot only.
     * Looping over all slots requires a write cursor to handle
     * WOULD_BLOCK without re-sending to already-written slots. */
    pub_sub_slot_t *slot = track_first_active_slot(track);
    if (!slot) {
        /* No subscriber to send to, but the app produced a live object the
         * facade does not retain: the retained object is no longer the track's
         * largest (see pub_init_accept_cfg). */
        track->wrote_object = true;
        return MOQ_OK;
    }
    if (slot->streaming && !obj->datagram) return MOQ_ERR_WRONG_STATE;

    moq_result_t rc = obj->datagram
        ? write_datagram_object(pub, track, slot, obj, now_us)
        : write_stream_object(pub, track, slot, obj, now_us);
    /* Mark the track live only on success: WOULD_BLOCK / errors did not produce
     * an object and must not permanently suppress the retained Largest. */
    if (rc == MOQ_OK) track->wrote_object = true;
    return rc;
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

    /* End the publication (PUBLISH_DONE) first, then any subscription slot. */
    {
        moq_result_t rc = moq_pub_unpublish_track(pub, track, now_us);
        if (rc != MOQ_OK) return rc;
    }

    /* Single active slot, like the write fan-out (documented v0 limit). */
    pub_sub_slot_t *slot = track_first_active_slot(track);
    if (!slot) { track->ended = true; return MOQ_OK; }  /* no subscriber: end locally */
    if (slot->streaming) return MOQ_ERR_WRONG_STATE;  /* mid begin/end object; retry */

    /* The terminal END_OF_TRACK goes on a fresh non-extension subgroup: close
     * any open subgroup (it may carry extensions) first, then open a new group
     * one past the last so object/group ordering stays monotonic. Closing the
     * prior subgroup is independently retry-safe -- it transitions the entry to
     * CLOSING and clears sg_open, so a later retry skips this block. */
    if (slot->sg_open) {
        moq_result_t rc = moq_session_close_subgroup(pub->session, slot->sg,
                                                     now_us);
        if (rc == MOQ_ERR_WOULD_BLOCK) return MOQ_ERR_WOULD_BLOCK;
        if (rc == MOQ_ERR_STALE_HANDLE) {
            track_clear_slot(pub, slot); track->ended = true; return MOQ_OK;
        }
        if (rc < 0) return rc;
        slot->sg_open = false;
    }

    /* The terminal subgroup is open -> status object -> FIN: three actions that
     * must be queued atomically. open_subgroup emits its header eagerly, so a
     * mid-sequence WOULD_BLOCK would strand an empty terminal subgroup on the
     * wire and a retry would re-open the same group_id (a duplicate subgroup).
     * Preflight the whole triple. */
    if (moq_session_action_capacity(pub->session) < 3) return MOQ_ERR_WOULD_BLOCK;

    moq_subgroup_cfg_t sgcfg;
    moq_subgroup_cfg_init(&sgcfg);
    sgcfg.group_id = slot->cur_group + 1;
    sgcfg.subgroup_id = 0;
    sgcfg.publisher_priority = track->priority;
    sgcfg.object_properties = false;
    moq_subgroup_handle_t sg;
    moq_result_t rc = moq_session_open_subgroup(pub->session, slot->sub, &sgcfg,
                                                now_us, &sg);
    if (rc == MOQ_ERR_STALE_HANDLE) {
        track_clear_slot(pub, slot); track->ended = true; return MOQ_OK;
    }
    if (rc < 0) return rc;

    /* Capacity is reserved, so the remaining two actions cannot WOULD_BLOCK. */
    rc = moq_session_write_status_object(pub->session, sg, 0,
                                         MOQ_OBJECT_END_OF_TRACK, now_us);
    if (rc < 0) { (void)moq_session_close_subgroup(pub->session, sg, now_us);
                  return rc; }

    slot->cur_group = sgcfg.group_id;
    (void)moq_session_close_subgroup(pub->session, sg, now_us);  /* FIN */
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

        /* Send the session-level done with the caller's status code. The facade
         * does not track per-subscription stream counts, so report the unknown
         * sentinel (2^62 - 1) per the transport spec; subscribers time out
         * rather than wait for an exact count. On success the session frees the
         * subscription, so clear our slot to match (and to stay idempotent). */
        moq_done_subscribe_cfg_t dcfg;
        moq_done_subscribe_cfg_init(&dcfg);
        dcfg.status_code = status_code;
        dcfg.stream_count = (UINT64_C(1) << 62) - 1;  /* unknown stream count */
        moq_result_t rc = moq_session_done_subscribe(pub->session, slot->sub,
                                                     &dcfg, now_us);
        if (rc == MOQ_ERR_WOULD_BLOCK) return MOQ_ERR_WOULD_BLOCK;
        if (rc != MOQ_OK && rc != MOQ_ERR_STALE_HANDLE) return rc;
        track_clear_slot(pub, slot);   /* subscription freed (or already gone) */
    }
    return MOQ_OK;
}

/* -- Streaming writes --------------------------------------------- */

void moq_pub_begin_object_cfg_init(moq_pub_begin_object_cfg_t *cfg) {
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = sizeof(*cfg);
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

    pub_sub_slot_t *slot = track_first_active_slot(track);
    if (!slot) return MOQ_OK;
    if (slot->streaming) return MOQ_ERR_WRONG_STATE;

    if (slot->sg_open && slot->cur_group != cfg->group_id) {
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

    moq_rcbuf_t *begin_props = NULL;
    if (cfg->struct_size >= offsetof(moq_pub_begin_object_cfg_t, properties) +
        sizeof(cfg->properties))
        begin_props = cfg->properties;
    bool need_ext = (begin_props != NULL);

    if (!slot->sg_open) {
        moq_subgroup_cfg_t sgcfg;
        moq_subgroup_cfg_init(&sgcfg);
        sgcfg.group_id = cfg->group_id;
        sgcfg.subgroup_id = 0;
        sgcfg.publisher_priority = track->priority;
        sgcfg.object_properties = need_ext;
        moq_result_t rc = moq_session_open_subgroup(pub->session,
            slot->sub, &sgcfg, now_us, &slot->sg);
        if (rc == MOQ_ERR_WOULD_BLOCK) return MOQ_ERR_WOULD_BLOCK;
        if (rc == MOQ_ERR_STALE_HANDLE) {
            track_clear_slot(pub, slot);
            return MOQ_OK;
        }
        if (rc < 0) return rc;
        slot->sg_open = true;
        slot->sg_has_extensions = need_ext;
        slot->cur_group = cfg->group_id;
    }

    moq_result_t rc;
    if (need_ext || slot->sg_has_extensions) {
        moq_begin_object_cfg_t bcfg;
        moq_begin_object_cfg_init(&bcfg);
        bcfg.object_id = cfg->object_id;
        bcfg.payload_length = cfg->payload_length;
        bcfg.properties = begin_props;
        rc = moq_session_begin_object_ex(pub->session,
            slot->sg, &bcfg, now_us);
    } else {
        rc = moq_session_begin_object(pub->session,
            slot->sg, cfg->object_id, cfg->payload_length, now_us);
    }
    if (rc == MOQ_ERR_WOULD_BLOCK) return MOQ_ERR_WOULD_BLOCK;
    if (rc == MOQ_ERR_STALE_HANDLE) {
        track_clear_slot(pub, slot);
        return MOQ_OK;
    }
    if (rc < 0) return rc;

    slot->streaming = true;
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
    {
        pub_sub_slot_t *slot = track_first_active_slot(track);
        /* No subscriber to stream to: a no-op success, consistent with
         * moq_pub_begin_object and moq_pub_write_object_ex (so the natural
         * begin/write/end sequence does not spuriously fail on a track that
         * never had, or just lost, a subscriber). A live slot that is not
         * streaming is genuine misuse (write_data without begin_object). */
        if (!slot) return MOQ_OK;
        if (!slot->streaming) return MOQ_ERR_WRONG_STATE;

        moq_result_t rc = moq_session_write_object_data(pub->session,
            slot->sg, chunk, now_us);
        if (rc == MOQ_ERR_WOULD_BLOCK) return MOQ_ERR_WOULD_BLOCK;
        if (rc == MOQ_ERR_STALE_HANDLE) {
            track_clear_slot(pub, slot);
            return MOQ_OK;
        }
        return rc;
    }
}

moq_result_t moq_pub_end_object(moq_publisher_t *pub,
                                  moq_pub_track_t *track,
                                  uint64_t now_us)
{
    if (!pub || !track) return MOQ_ERR_INVAL;
    if (track->pub != pub) return MOQ_ERR_INVAL;
    if (pub->closed) return MOQ_ERR_CLOSED;
    if (track->ended) return MOQ_ERR_WRONG_STATE;  /* terminal: no writes */
    {
        pub_sub_slot_t *slot = track_first_active_slot(track);
        /* No subscriber: no-op success, consistent with begin_object/write_data
         * (a live slot that is not streaming is genuine misuse). */
        if (!slot) return MOQ_OK;
        if (!slot->streaming) return MOQ_ERR_WRONG_STATE;

        moq_result_t rc = moq_session_end_object(pub->session,
            slot->sg, now_us);
        if (rc == MOQ_ERR_STALE_HANDLE) {
            track_clear_slot(pub, slot);
            return MOQ_OK;
        }
        if (rc < 0) return rc;

        slot->streaming = false;
        return MOQ_OK;
    }
}

moq_result_t moq_pub_end_group(moq_publisher_t *pub,
                                 moq_pub_track_t *track,
                                 uint64_t now_us)
{
    if (!pub || !track) return MOQ_ERR_INVAL;
    if (track->pub != pub) return MOQ_ERR_INVAL;
    if (pub->closed) return MOQ_ERR_CLOSED;
    {
        pub_sub_slot_t *slot = track_first_active_slot(track);
        if (!slot) return MOQ_OK;
        if (slot->streaming) return MOQ_ERR_WRONG_STATE;
        if (!slot->sg_open) return MOQ_OK;

        moq_result_t rc = moq_session_close_subgroup(pub->session,
            slot->sg, now_us);
        if (rc == MOQ_ERR_WOULD_BLOCK) return MOQ_ERR_WOULD_BLOCK;
        if (rc == MOQ_ERR_STALE_HANDLE) {
            track_clear_slot(pub, slot);
            return MOQ_OK;
        }
        if (rc < 0) return rc;
        slot->sg_open = false;
        return MOQ_OK;
    }
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
    }
    return MOQ_OK;
}

moq_result_t moq_pub_handle_event(moq_publisher_t *pub,
                                    const moq_event_t *event,
                                    uint64_t now_us,
                                    moq_pub_event_result_t *result)
{
    if (!pub || !event || !result) return MOQ_ERR_INVAL;
    *result = MOQ_PUB_EVENT_IGNORED;

    if (event->kind == MOQ_EVENT_SESSION_CLOSED) {
        pub->closed = true;
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
        moq_pub_track_t *t = find_track_by_ann(pub,
            event->u.namespace_rejected.ann);
        if (t && t->ns_entry && t->ns_entry->state != PUB_NS_NONE) {
            t->ns_entry->state = PUB_NS_TERMINAL;
            *result = MOQ_PUB_EVENT_CONSUMED;
        }
        return MOQ_OK;
    }
    if (event->kind == MOQ_EVENT_NAMESPACE_CANCELLED) {
        moq_pub_track_t *t = find_track_by_ann(pub,
            event->u.namespace_cancelled.ann);
        if (t && t->ns_entry && t->ns_entry->state != PUB_NS_NONE) {
            t->ns_entry->state = PUB_NS_TERMINAL;
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
        return MOQ_ERR_WOULD_BLOCK;
    }
    if (rc < 0) {
        *result = MOQ_PUB_EVENT_ERROR;
        return rc;
    }

    if (want_accept)
        track_set_subscriber(track, req->sub);
    return MOQ_OK;
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
        deferred->active = false;
        return MOQ_ERR_WOULD_BLOCK;
    }
    if (rc < 0) {
        deferred->active = false;
        return rc;
    }

    if (accept && deferred->track) {
        track_set_subscriber(deferred->track, deferred->sub);
        if (pub->callbacks.on_subscriber_joined)
            pub->callbacks.on_subscriber_joined(pub->callbacks.ctx,
                deferred->track);
    }
    deferred->active = false;
    return MOQ_OK;
}

moq_result_t moq_pub_flush(moq_publisher_t *pub, uint64_t now_us) {
    if (!pub) return MOQ_ERR_INVAL;
    moq_pub_track_t *joined = NULL;
    moq_result_t rc = flush_pending(pub, now_us, &joined);
    if (joined && pub->callbacks.on_subscriber_joined)
        pub->callbacks.on_subscriber_joined(pub->callbacks.ctx, joined);
    return rc;
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
    moq_publish_cfg_init(&pcfg);
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

    /* Close the data subgroup cleanly before PUBLISH_DONE (finish needs no open
     * stream). Retryable: WOULD_BLOCK at any step lets the caller retry. */
    pub_sub_slot_t *psl = track_find_publication_slot(track);
    if (psl && psl->streaming) return MOQ_ERR_WRONG_STATE;
    if (psl && psl->sg_open) {
        moq_result_t rc = moq_session_close_subgroup(pub->session,
            psl->sg, now_us);
        if (rc == MOQ_ERR_WOULD_BLOCK) return MOQ_ERR_WOULD_BLOCK;
        if (rc != MOQ_OK && rc != MOQ_ERR_STALE_HANDLE) return rc;
        psl->sg_open = false;
    }

    moq_finish_publish_cfg_t fcfg;
    moq_finish_publish_cfg_init(&fcfg);
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


static void drain_closed_event(moq_publisher_t *pub)
{
    if (pub->closed) return;
    moq_event_t ev;
    while (moq_session_poll_events(pub->session, &ev, 1) == 1) {
        bool is_close = (ev.kind == MOQ_EVENT_SESSION_CLOSED);
        uint64_t code = is_close ? ev.u.closed.code : 0;
        moq_event_cleanup(&ev);
        if (is_close) {
            pub->closed = true;
            pub->deferred.active = false;
            for (moq_pub_track_t *t = pub->tracks; t; t = t->next)
                track_clear_subscriber(t);
            if (pub->callbacks.on_closed)
                pub->callbacks.on_closed(pub->callbacks.ctx, code);
            return;
        }
    }
    pub->closed = true;
    pub->deferred.active = false;
    for (moq_pub_track_t *t = pub->tracks; t; t = t->next)
        track_clear_subscriber(t);
}

moq_result_t moq_pub_tick(moq_publisher_t *pub, uint64_t now_us)
{
    if (!pub) return MOQ_ERR_INVAL;

    moq_pub_track_t *joined_track = NULL;
    moq_result_t frc = flush_pending(pub, now_us, &joined_track);
    if (frc == MOQ_ERR_WOULD_BLOCK) return MOQ_ERR_WOULD_BLOCK;
    if (frc < 0) {
        if (frc == MOQ_ERR_CLOSED)
            drain_closed_event(pub);
        return frc;
    }

    /* Drain a deferred retained-object FETCH before processing new events;
     * a still-full action queue blocks the tick (same model as flush_pending). */
    if (pub->pending_fetch.active) {
        serve_retained_fetch(pub, now_us);
        if (pub->pending_fetch.active) return MOQ_ERR_WOULD_BLOCK;
    }

    moq_event_t ev;
    while (moq_session_poll_events(pub->session, &ev, 1) == 1) {
        switch (ev.kind) {
        case MOQ_EVENT_SESSION_CLOSED:
            pub->closed = true;
            pub->deferred.active = false;
            for (moq_pub_track_t *t = pub->tracks; t; t = t->next)
                track_clear_subscriber(t);
            if (pub->callbacks.on_closed)
                pub->callbacks.on_closed(pub->callbacks.ctx,
                    ev.u.closed.code);
            break;

        case MOQ_EVENT_GOAWAY:
            pub->draining = true;
            if (pub->callbacks.on_draining)
                pub->callbacks.on_draining(pub->callbacks.ctx);
            break;

        case MOQ_EVENT_UNSUBSCRIBED: {
            if (pub->deferred.active &&
                moq_subscription_eq(pub->deferred.sub,
                    ev.u.unsubscribed.sub)) {
                pub->deferred.active = false;
                break;
            }
            moq_pub_track_t *t = find_track_by_sub(pub,
                ev.u.unsubscribed.sub);
            if (t) {
                pub_sub_slot_t *sl = track_find_slot_by_sub(t,
                    ev.u.unsubscribed.sub);
                if (sl) track_clear_slot(pub, sl);
                if (pub->callbacks.on_subscriber_left)
                    pub->callbacks.on_subscriber_left(
                        pub->callbacks.ctx, t);
            }
            break;
        }

        case MOQ_EVENT_SUBSCRIBE_REQUEST: {
            moq_pub_track_t *t = find_track(pub,
                &ev.u.subscribe_request.track_namespace,
                ev.u.subscribe_request.track_name);
            moq_pub_event_result_t res;
            moq_result_t hrc = moq_pub_handle_event(pub, &ev, now_us, &res);
            if (hrc == MOQ_ERR_WOULD_BLOCK) {
                moq_event_cleanup(&ev);
                return MOQ_ERR_WOULD_BLOCK;
            }
            if (hrc < 0) {
                moq_event_cleanup(&ev);
                if (hrc == MOQ_ERR_CLOSED)
                    drain_closed_event(pub);
                return hrc;
            }
            if (res == MOQ_PUB_EVENT_CONSUMED &&
                t && track_has_subscriber(t) &&
                pub->callbacks.on_subscriber_joined)
                pub->callbacks.on_subscriber_joined(
                    pub->callbacks.ctx, t);
            break;
        }

        case MOQ_EVENT_FETCH_REQUEST: {
            moq_pub_event_result_t res;
            moq_result_t hrc = moq_pub_handle_event(pub, &ev, now_us, &res);
            if (hrc == MOQ_ERR_WOULD_BLOCK) {
                moq_event_cleanup(&ev);
                return MOQ_ERR_WOULD_BLOCK;
            }
            if (hrc < 0) {
                moq_event_cleanup(&ev);
                if (hrc == MOQ_ERR_CLOSED)
                    drain_closed_event(pub);
                return hrc;
            }
            break;
        }

        case MOQ_EVENT_NAMESPACE_ACCEPTED:
        case MOQ_EVENT_NAMESPACE_REJECTED:
        case MOQ_EVENT_NAMESPACE_CANCELLED: {
            moq_pub_event_result_t res;
            moq_pub_handle_event(pub, &ev, now_us, &res);
            break;
        }

        case MOQ_EVENT_SUBSCRIBE_UPDATED: {
            moq_pub_track_t *t = find_track_by_sub(pub,
                ev.u.subscribe_updated.sub);
            if (t &&
                pub->callbacks.struct_size >=
                    offsetof(moq_pub_callbacks_t, on_subscriber_updated) +
                    sizeof(pub->callbacks.on_subscriber_updated) &&
                pub->callbacks.on_subscriber_updated) {
                moq_pub_subscribe_update_info_t info;
                memset(&info, 0, sizeof(info));
                info.has_subscriber_priority =
                    ev.u.subscribe_updated.has_subscriber_priority;
                info.subscriber_priority =
                    ev.u.subscribe_updated.subscriber_priority;
                info.has_forward = ev.u.subscribe_updated.has_forward;
                info.forward = ev.u.subscribe_updated.forward;
                info.has_delivery_timeout =
                    ev.u.subscribe_updated.has_delivery_timeout;
                info.delivery_timeout_us =
                    ev.u.subscribe_updated.delivery_timeout_us;
                pub->callbacks.on_subscriber_updated(
                    pub->callbacks.ctx, t, &info);
            }
            break;
        }

        case MOQ_EVENT_PUBLISH_OK: {
            moq_pub_track_t *t = find_track_by_pub(pub,
                ev.u.publish_ok.pub);
            if (t) {
                t->publish_ok = true;
                t->publish_forward = ev.u.publish_ok.send_allowed;
                if (!track_find_publication_slot(t))
                    track_set_publication(t, t->publication);
                if (CB_HAS(&pub->callbacks, on_publish_ok))
                    pub->callbacks.on_publish_ok(pub->callbacks.ctx, t,
                        t->publish_forward);
            }
            break;
        }

        case MOQ_EVENT_PUBLISH_UPDATED: {
            moq_pub_track_t *t = find_track_by_pub(pub,
                ev.u.publish_updated.pub);
            if (t && ev.u.publish_updated.has_forward) {
                t->publish_forward = ev.u.publish_updated.forward;
                if (CB_HAS(&pub->callbacks, on_publish_forward_changed))
                    pub->callbacks.on_publish_forward_changed(
                        pub->callbacks.ctx, t, t->publish_forward);
            }
            break;
        }

        case MOQ_EVENT_PUBLISH_ERROR: {
            moq_pub_track_t *t = find_track_by_pub(pub,
                ev.u.publish_error.pub);
            if (t) {
                /* Publication terminated before acceptance: clear so a later
                 * moq_pub_publish_track may retry, and drop any data slot. */
                t->publish_requested = false;
                t->publish_ok = false;
                pub_sub_slot_t *sl = track_find_publication_slot(t);
                if (sl) track_clear_slot(pub, sl);
                if (CB_HAS(&pub->callbacks, on_publish_error))
                    pub->callbacks.on_publish_error(pub->callbacks.ctx, t,
                        ev.u.publish_error.error_code);
            }
            break;
        }

        case MOQ_EVENT_PUBLISH_FINISHED:
        case MOQ_EVENT_PUBLISH_UNSUBSCRIBED: {
            moq_publication_t ph = (ev.kind == MOQ_EVENT_PUBLISH_FINISHED)
                ? ev.u.publish_finished.pub
                : ev.u.publish_unsubscribed.pub;
            moq_pub_track_t *t = find_track_by_pub(pub, ph);
            if (t) {
                t->publish_ok = false;
                pub_sub_slot_t *sl = track_find_publication_slot(t);
                if (sl) track_clear_slot(pub, sl);
                if (CB_HAS(&pub->callbacks, on_publish_finished))
                    pub->callbacks.on_publish_finished(pub->callbacks.ctx, t);
            }
            break;
        }

        default:
            break;
        }

        moq_event_cleanup(&ev);
    }

    if (joined_track && !pub->closed && track_has_subscriber(joined_track) &&
        pub->callbacks.on_subscriber_joined)
        pub->callbacks.on_subscriber_joined(
            pub->callbacks.ctx, joined_track);

    return MOQ_OK;
}
