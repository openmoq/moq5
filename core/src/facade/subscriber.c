#include <moq/subscriber.h>
#include <moq/rcbuf.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#define SUB_DEFAULT_MAX_TRACKS  16
#define SUB_DEFAULT_MAX_OBJECTS 256
/* Default facade byte budget; mirrors the session's MOQ_DEFAULT_MAX_RECV_BUF. */
#define SUB_DEFAULT_MAX_QUEUED_BYTES (16u * 1024 * 1024)

typedef enum {
    SUB_TRACK_FREE = 0,
    SUB_TRACK_PENDING,
    SUB_TRACK_ACTIVE,
    SUB_TRACK_ERROR,
    SUB_TRACK_DONE,
} sub_track_state_t;

struct moq_sub_track {
    sub_track_state_t     state;
    moq_subscription_t    handle;
    moq_subscriber_t     *sub;
    moq_request_error_t   error_code;
};

#define SUB_DEFAULT_MAX_STATUS 16

typedef enum {
    SUB_STATUS_FREE = 0,
    SUB_STATUS_PENDING,
    SUB_STATUS_DONE,
} sub_status_state_t;

struct moq_sub_status_req {
    sub_status_state_t          state;
    moq_track_status_handle_t   handle;
    moq_subscriber_t           *sub;
};

#define SUB_DEFAULT_MAX_FETCHES    16
#define SUB_DEFAULT_MAX_FETCH_ITEMS 256
#define SUB_DEFAULT_MAX_CHUNKS     256

typedef enum {
    SUB_FETCH_FREE = 0,
    SUB_FETCH_PENDING,
    SUB_FETCH_ACTIVE,
    SUB_FETCH_DONE,
} sub_fetch_state_t;

struct moq_sub_fetch_req {
    sub_fetch_state_t   state;
    moq_fetch_t         handle;
    moq_subscriber_t   *sub;
};

struct moq_subscriber {
    moq_session_t      *session;
    moq_alloc_t         alloc;
    moq_sub_callbacks_t callbacks;
    void              (*on_goaway)(void *ctx, moq_bytes_t new_session_uri);
    void              (*on_subscribe_done)(void *ctx, moq_sub_track_t *track,
                                           uint64_t status_code);
    bool                closed;
    bool                draining;

    /* Byte budget for payload+properties rcbufs retained in the object and
     * fetch-object ring queues. Counts bytes currently in the rings only; the
     * one-deep pending slots are tracked off-budget (see the byte-gate note in
     * moq_sub_tick), so total retained is bounded by max_queued_bytes plus at
     * most two maximum-size objects. */
    size_t              max_queued_bytes;
    size_t              queued_bytes;

    moq_sub_track_t    *tracks;
    size_t              track_cap;

    moq_sub_object_t   *objects;
    size_t              obj_cap;
    size_t              obj_head;
    size_t              obj_tail;

    bool                has_pending;
    moq_sub_object_t    pending;

    moq_sub_status_req_t *status_reqs;
    size_t                status_cap;

    moq_sub_status_result_t *status_results;
    size_t                   sr_cap;
    size_t                   sr_head;
    size_t                   sr_tail;

    moq_sub_fetch_req_t  *fetch_reqs;
    size_t                fetch_cap;

    moq_sub_fetch_item_t *fetch_items;
    size_t                fi_cap;
    size_t                fi_head;
    size_t                fi_tail;

    bool                  has_pending_fi;
    moq_sub_fetch_item_t  pending_fi;

    bool                streaming;
    moq_sub_chunk_t    *chunks;
    size_t              chunk_cap;
    size_t              chunk_head;
    size_t              chunk_tail;

    bool                has_pending_chunk;
    moq_sub_chunk_t     pending_chunk;
};

/* -- Helpers -------------------------------------------------------- */

static moq_sub_track_t *find_track_by_handle(moq_subscriber_t *s,
                                               moq_subscription_t h)
{
    for (size_t i = 0; i < s->track_cap; i++)
        if (s->tracks[i].state != SUB_TRACK_FREE &&
            moq_subscription_eq(s->tracks[i].handle, h))
            return &s->tracks[i];
    return NULL;
}

static moq_sub_track_t *alloc_track(moq_subscriber_t *s)
{
    for (size_t i = 0; i < s->track_cap; i++)
        if (s->tracks[i].state == SUB_TRACK_FREE)
            return &s->tracks[i];
    return NULL;
}

static moq_sub_status_req_t *find_status_by_handle(moq_subscriber_t *s,
    moq_track_status_handle_t h)
{
    for (size_t i = 0; i < s->status_cap; i++)
        if (s->status_reqs[i].state != SUB_STATUS_FREE &&
            moq_track_status_handle_eq(s->status_reqs[i].handle, h))
            return &s->status_reqs[i];
    return NULL;
}

static moq_sub_status_req_t *alloc_status_req(moq_subscriber_t *s)
{
    for (size_t i = 0; i < s->status_cap; i++)
        if (s->status_reqs[i].state == SUB_STATUS_FREE)
            return &s->status_reqs[i];
    return NULL;
}

static bool sr_queue_full(const moq_subscriber_t *s)
{
    return (s->sr_tail - s->sr_head) >= s->sr_cap;
}

static moq_sub_status_result_t *sr_push(moq_subscriber_t *s)
{
    if (sr_queue_full(s)) return NULL;
    moq_sub_status_result_t *r = &s->status_results[s->sr_tail % s->sr_cap];
    s->sr_tail++;
    return r;
}

/* -- Retained-byte accounting --------------------------------------- */

static size_t sub_sat_add(size_t a, size_t b)
{
    return b > SIZE_MAX - a ? SIZE_MAX : a + b;
}

/* Bytes a queued object/fetch-object retains: payload + properties. */
static size_t rcbuf_pair_bytes(const moq_rcbuf_t *payload,
                               const moq_rcbuf_t *properties)
{
    size_t n = payload ? moq_rcbuf_len(payload) : 0;
    if (properties)
        n = sub_sat_add(n, moq_rcbuf_len(properties));
    return n;
}

static void sub_bytes_add(moq_subscriber_t *s, size_t n)
{
    s->queued_bytes = sub_sat_add(s->queued_bytes, n);
}

static void sub_bytes_sub(moq_subscriber_t *s, size_t n)
{
    s->queued_bytes = n <= s->queued_bytes ? s->queued_bytes - n : 0;
}

/* High-water gate: true once retained bytes have reached the cap. New objects
 * are admitted while strictly under the cap (so the ring may overshoot by one
 * object); once at/over the cap they divert to the one-deep pending slot. The
 * pending slot's bytes are tracked off-budget, which keeps the pending-flush
 * gate from deadlocking on an object larger than the cap. */
static bool obj_bytes_over_budget(const moq_subscriber_t *s)
{
    return s->queued_bytes >= s->max_queued_bytes;
}

static bool obj_queue_full(const moq_subscriber_t *s)
{
    return (s->obj_tail - s->obj_head) >= s->obj_cap;
}

static moq_sub_object_t *obj_push(moq_subscriber_t *s)
{
    if (obj_queue_full(s)) return NULL;
    moq_sub_object_t *o = &s->objects[s->obj_tail % s->obj_cap];
    s->obj_tail++;
    return o;
}

static moq_sub_fetch_req_t *find_fetch_by_handle(moq_subscriber_t *s,
    moq_fetch_t h)
{
    for (size_t i = 0; i < s->fetch_cap; i++)
        if (s->fetch_reqs[i].state != SUB_FETCH_FREE &&
            moq_fetch_eq(s->fetch_reqs[i].handle, h))
            return &s->fetch_reqs[i];
    return NULL;
}

static moq_sub_fetch_req_t *alloc_fetch_req(moq_subscriber_t *s)
{
    for (size_t i = 0; i < s->fetch_cap; i++)
        if (s->fetch_reqs[i].state == SUB_FETCH_FREE)
            return &s->fetch_reqs[i];
    return NULL;
}

static bool fi_queue_full(const moq_subscriber_t *s)
{
    return (s->fi_tail - s->fi_head) >= s->fi_cap;
}

static moq_sub_fetch_item_t *fi_push(moq_subscriber_t *s)
{
    if (fi_queue_full(s)) return NULL;
    moq_sub_fetch_item_t *f = &s->fetch_items[s->fi_tail % s->fi_cap];
    s->fi_tail++;
    return f;
}

static void fetch_item_release_refs(moq_sub_fetch_item_t *item)
{
    if (item->kind == MOQ_SUB_FETCH_OBJECT) {
        if (item->u.object.payload) {
            moq_rcbuf_decref(item->u.object.payload);
            item->u.object.payload = NULL;
        }
        if (item->u.object.properties) {
            moq_rcbuf_decref(item->u.object.properties);
            item->u.object.properties = NULL;
        }
    }
}

/* Discard every queued/pending fetch item that belongs to `req`. Called on
 * cancel before the request slot is freed and reused: the request pointer is
 * the application-visible correlation token, so stale items left behind would
 * be re-attributed to a later fetch that reuses the slot. Releases rcbuf refs
 * and returns retained bytes to the budget for removed queued objects; the
 * one-deep pending slot is tracked off-budget, so it only releases refs. Ring
 * order of the surviving items is preserved. */
static void purge_fetch_items_for(moq_subscriber_t *sub,
                                  const moq_sub_fetch_req_t *req)
{
    if (sub->has_pending_fi && sub->pending_fi.request == req) {
        fetch_item_release_refs(&sub->pending_fi);
        memset(&sub->pending_fi, 0, sizeof(sub->pending_fi));
        sub->has_pending_fi = false;
    }

    size_t write = sub->fi_head;
    for (size_t read = sub->fi_head; read < sub->fi_tail; read++) {
        moq_sub_fetch_item_t *src = &sub->fetch_items[read % sub->fi_cap];
        if (src->request == req) {
            if (src->kind == MOQ_SUB_FETCH_OBJECT)
                sub_bytes_sub(sub, rcbuf_pair_bytes(src->u.object.payload,
                                                    src->u.object.properties));
            fetch_item_release_refs(src);
            memset(src, 0, sizeof(*src));
            continue;
        }
        if (write != read) {
            sub->fetch_items[write % sub->fi_cap] = *src;
            memset(src, 0, sizeof(*src));
        }
        write++;
    }
    sub->fi_tail = write;
}

static bool chunk_queue_full(const moq_subscriber_t *s)
{
    return (s->chunk_tail - s->chunk_head) >= s->chunk_cap;
}

static moq_sub_chunk_t *chunk_push(moq_subscriber_t *s)
{
    if (chunk_queue_full(s)) return NULL;
    moq_sub_chunk_t *c = &s->chunks[s->chunk_tail % s->chunk_cap];
    s->chunk_tail++;
    return c;
}

static void chunk_release_refs(moq_sub_chunk_t *c)
{
    if (c->chunk) { moq_rcbuf_decref(c->chunk); c->chunk = NULL; }
    if (c->properties) { moq_rcbuf_decref(c->properties); c->properties = NULL; }
}

/* -- Public API ----------------------------------------------------- */

void moq_sub_callbacks_init(moq_sub_callbacks_t *cb)
{
    if (!cb) return;
    memset(cb, 0, sizeof(*cb));
    cb->struct_size = sizeof(*cb);
}

/* Frozen v0 prefix size: the layout before the first appended field
 * (on_goaway). The pointer-only initializer cannot know the caller's storage
 * size, so it touches only this prefix -- safe for a caller compiled against the
 * original (smaller) struct. */
#define MOQ_SUB_CFG_V0_SIZE (offsetof(moq_sub_cfg_t, on_goaway))

void moq_sub_cfg_init(moq_sub_cfg_t *cfg)
{
    if (!cfg) return;
    /* Clear and stamp ONLY the frozen v0 prefix: writing sizeof(*cfg) here would
     * overflow a caller that allocated the smaller original struct. Appended
     * fields (on_goaway, on_subscribe_done, max_queued_object_bytes) stay
     * disabled (struct_size == prefix); callers that want them use
     * moq_sub_cfg_init_sized(). callbacks is inside the prefix. */
    memset(cfg, 0, MOQ_SUB_CFG_V0_SIZE);
    cfg->struct_size = (uint32_t)MOQ_SUB_CFG_V0_SIZE;
    moq_sub_callbacks_init(&cfg->callbacks);
}

void moq_sub_cfg_init_sized(moq_sub_cfg_t *cfg, size_t cfg_size)
{
    if (!cfg) return;
    /* Clear exactly what the caller allocated, never more than this library's
     * struct knows about. An older caller passes a smaller cfg_size (we clear/
     * stamp that prefix); a newer caller's extra trailing fields are left to its
     * own initializer. */
    size_t n = cfg_size < sizeof(*cfg) ? cfg_size : sizeof(*cfg);
    if (n < sizeof(cfg->struct_size)) return;  /* too small to even stamp */
    memset(cfg, 0, n);
    cfg->struct_size = (uint32_t)n;
    if (n >= offsetof(moq_sub_cfg_t, callbacks) + sizeof(cfg->callbacks))
        moq_sub_callbacks_init(&cfg->callbacks);
}

moq_result_t moq_sub_create(moq_session_t *session,
                              const moq_alloc_t *alloc,
                              const moq_sub_cfg_t *cfg,
                              moq_subscriber_t **out)
{
    if (!session || !alloc || !cfg || !out) return MOQ_ERR_INVAL;
    *out = NULL;
    if (cfg->struct_size < offsetof(moq_sub_cfg_t, callbacks) +
        sizeof(cfg->callbacks))
        return MOQ_ERR_INVAL;

#define SUB_CFG_HAS(field) \
    (cfg->struct_size >= offsetof(moq_sub_cfg_t, field) + sizeof(cfg->field))

    size_t tc = cfg->max_tracks ? cfg->max_tracks : SUB_DEFAULT_MAX_TRACKS;
    size_t oc = cfg->max_objects ? cfg->max_objects : SUB_DEFAULT_MAX_OBJECTS;
    size_t fc = SUB_CFG_HAS(max_fetches) && cfg->max_fetches
        ? cfg->max_fetches : SUB_DEFAULT_MAX_FETCHES;
    size_t fic = SUB_CFG_HAS(max_fetch_items) && cfg->max_fetch_items
        ? cfg->max_fetch_items : SUB_DEFAULT_MAX_FETCH_ITEMS;
    size_t cc = SUB_CFG_HAS(max_chunks) && cfg->max_chunks
        ? cfg->max_chunks : SUB_DEFAULT_MAX_CHUNKS;
    bool streaming = SUB_CFG_HAS(streaming_objects) && cfg->streaming_objects;
    size_t qbytes = SUB_DEFAULT_MAX_QUEUED_BYTES;
    if (SUB_CFG_HAS(max_queued_object_bytes) && cfg->max_queued_object_bytes) {
        /* Clamp (not truncate) a configured value that cannot fit in size_t. */
        qbytes = cfg->max_queued_object_bytes > (uint64_t)SIZE_MAX
                     ? SIZE_MAX
                     : (size_t)cfg->max_queued_object_bytes;
    }

    moq_subscriber_t *s = (moq_subscriber_t *)alloc->alloc(
        sizeof(moq_subscriber_t), alloc->ctx);
    if (!s) return MOQ_ERR_NOMEM;
    memset(s, 0, sizeof(*s));

    s->tracks = (moq_sub_track_t *)alloc->alloc(
        tc * sizeof(moq_sub_track_t), alloc->ctx);
    if (!s->tracks) {
        alloc->free(s, sizeof(moq_subscriber_t), alloc->ctx);
        return MOQ_ERR_NOMEM;
    }
    memset(s->tracks, 0, tc * sizeof(moq_sub_track_t));

    s->objects = (moq_sub_object_t *)alloc->alloc(
        oc * sizeof(moq_sub_object_t), alloc->ctx);
    if (!s->objects) {
        alloc->free(s->tracks, tc * sizeof(moq_sub_track_t), alloc->ctx);
        alloc->free(s, sizeof(moq_subscriber_t), alloc->ctx);
        return MOQ_ERR_NOMEM;
    }
    memset(s->objects, 0, oc * sizeof(moq_sub_object_t));

    s->session = session;
    s->alloc = *alloc;
    size_t sc = SUB_DEFAULT_MAX_STATUS;
    s->status_reqs = (moq_sub_status_req_t *)alloc->alloc(
        sc * sizeof(moq_sub_status_req_t), alloc->ctx);
    if (!s->status_reqs) {
        alloc->free(s->objects, oc * sizeof(moq_sub_object_t), alloc->ctx);
        alloc->free(s->tracks, tc * sizeof(moq_sub_track_t), alloc->ctx);
        alloc->free(s, sizeof(moq_subscriber_t), alloc->ctx);
        return MOQ_ERR_NOMEM;
    }
    memset(s->status_reqs, 0, sc * sizeof(moq_sub_status_req_t));

    s->status_results = (moq_sub_status_result_t *)alloc->alloc(
        sc * sizeof(moq_sub_status_result_t), alloc->ctx);
    if (!s->status_results) {
        alloc->free(s->status_reqs, sc * sizeof(moq_sub_status_req_t), alloc->ctx);
        alloc->free(s->objects, oc * sizeof(moq_sub_object_t), alloc->ctx);
        alloc->free(s->tracks, tc * sizeof(moq_sub_track_t), alloc->ctx);
        alloc->free(s, sizeof(moq_subscriber_t), alloc->ctx);
        return MOQ_ERR_NOMEM;
    }
    memset(s->status_results, 0, sc * sizeof(moq_sub_status_result_t));

    s->fetch_reqs = (moq_sub_fetch_req_t *)alloc->alloc(
        fc * sizeof(moq_sub_fetch_req_t), alloc->ctx);
    if (!s->fetch_reqs) {
        alloc->free(s->status_results,
            sc * sizeof(moq_sub_status_result_t), alloc->ctx);
        alloc->free(s->status_reqs,
            sc * sizeof(moq_sub_status_req_t), alloc->ctx);
        alloc->free(s->objects, oc * sizeof(moq_sub_object_t), alloc->ctx);
        alloc->free(s->tracks, tc * sizeof(moq_sub_track_t), alloc->ctx);
        alloc->free(s, sizeof(moq_subscriber_t), alloc->ctx);
        return MOQ_ERR_NOMEM;
    }
    memset(s->fetch_reqs, 0, fc * sizeof(moq_sub_fetch_req_t));

    s->fetch_items = (moq_sub_fetch_item_t *)alloc->alloc(
        fic * sizeof(moq_sub_fetch_item_t), alloc->ctx);
    if (!s->fetch_items) {
        alloc->free(s->fetch_reqs,
            fc * sizeof(moq_sub_fetch_req_t), alloc->ctx);
        alloc->free(s->status_results,
            sc * sizeof(moq_sub_status_result_t), alloc->ctx);
        alloc->free(s->status_reqs,
            sc * sizeof(moq_sub_status_req_t), alloc->ctx);
        alloc->free(s->objects, oc * sizeof(moq_sub_object_t), alloc->ctx);
        alloc->free(s->tracks, tc * sizeof(moq_sub_track_t), alloc->ctx);
        alloc->free(s, sizeof(moq_subscriber_t), alloc->ctx);
        return MOQ_ERR_NOMEM;
    }
    memset(s->fetch_items, 0, fic * sizeof(moq_sub_fetch_item_t));

    s->chunks = (moq_sub_chunk_t *)alloc->alloc(
        cc * sizeof(moq_sub_chunk_t), alloc->ctx);
    if (!s->chunks) {
        alloc->free(s->fetch_items,
            fic * sizeof(moq_sub_fetch_item_t), alloc->ctx);
        alloc->free(s->fetch_reqs,
            fc * sizeof(moq_sub_fetch_req_t), alloc->ctx);
        alloc->free(s->status_results,
            sc * sizeof(moq_sub_status_result_t), alloc->ctx);
        alloc->free(s->status_reqs,
            sc * sizeof(moq_sub_status_req_t), alloc->ctx);
        alloc->free(s->objects, oc * sizeof(moq_sub_object_t), alloc->ctx);
        alloc->free(s->tracks, tc * sizeof(moq_sub_track_t), alloc->ctx);
        alloc->free(s, sizeof(moq_subscriber_t), alloc->ctx);
        return MOQ_ERR_NOMEM;
    }
    memset(s->chunks, 0, cc * sizeof(moq_sub_chunk_t));

    s->callbacks = cfg->callbacks;
    s->track_cap = tc;
    s->obj_cap = oc;
    s->status_cap = sc;
    s->sr_cap = sc;
    s->fetch_cap = fc;
    s->fi_cap = fic;
    s->chunk_cap = cc;
    s->streaming = streaming;
    s->max_queued_bytes = qbytes;
    if (SUB_CFG_HAS(on_goaway))
        s->on_goaway = cfg->on_goaway;
    if (SUB_CFG_HAS(on_subscribe_done))
        s->on_subscribe_done = cfg->on_subscribe_done;

#undef SUB_CFG_HAS

    for (size_t i = 0; i < tc; i++)
        s->tracks[i].sub = s;
    for (size_t i = 0; i < sc; i++)
        s->status_reqs[i].sub = s;
    for (size_t i = 0; i < fc; i++)
        s->fetch_reqs[i].sub = s;

    *out = s;
    return MOQ_OK;
}

void moq_sub_destroy(moq_subscriber_t *sub)
{
    if (!sub) return;

    /* Release pending and queued object refs. */
    if (sub->has_pending) {
        if (sub->pending.payload) moq_rcbuf_decref(sub->pending.payload);
        if (sub->pending.properties) moq_rcbuf_decref(sub->pending.properties);
    }
    while (sub->obj_head < sub->obj_tail) {
        moq_sub_object_t *o = &sub->objects[sub->obj_head % sub->obj_cap];
        if (o->payload) moq_rcbuf_decref(o->payload);
        if (o->properties) moq_rcbuf_decref(o->properties);
        sub->obj_head++;
    }

    if (sub->has_pending_fi)
        fetch_item_release_refs(&sub->pending_fi);
    while (sub->fi_head < sub->fi_tail) {
        moq_sub_fetch_item_t *f = &sub->fetch_items[sub->fi_head % sub->fi_cap];
        fetch_item_release_refs(f);
        sub->fi_head++;
    }

    if (sub->has_pending_chunk)
        chunk_release_refs(&sub->pending_chunk);
    while (sub->chunk_head < sub->chunk_tail) {
        moq_sub_chunk_t *c = &sub->chunks[sub->chunk_head % sub->chunk_cap];
        chunk_release_refs(c);
        sub->chunk_head++;
    }

    moq_alloc_t alloc = sub->alloc;
    alloc.free(sub->chunks,
        sub->chunk_cap * sizeof(moq_sub_chunk_t), alloc.ctx);
    alloc.free(sub->fetch_items,
        sub->fi_cap * sizeof(moq_sub_fetch_item_t), alloc.ctx);
    alloc.free(sub->fetch_reqs,
        sub->fetch_cap * sizeof(moq_sub_fetch_req_t), alloc.ctx);
    alloc.free(sub->status_results,
        sub->sr_cap * sizeof(moq_sub_status_result_t), alloc.ctx);
    alloc.free(sub->status_reqs,
        sub->status_cap * sizeof(moq_sub_status_req_t), alloc.ctx);
    alloc.free(sub->objects,
        sub->obj_cap * sizeof(moq_sub_object_t), alloc.ctx);
    alloc.free(sub->tracks,
        sub->track_cap * sizeof(moq_sub_track_t), alloc.ctx);
    alloc.free(sub, sizeof(moq_subscriber_t), alloc.ctx);
}

void moq_sub_track_cfg_init(moq_sub_track_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = sizeof(*cfg);
    cfg->filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
}

moq_result_t moq_sub_subscribe(moq_subscriber_t *sub,
                                 const moq_sub_track_cfg_t *cfg,
                                 uint64_t now_us,
                                 moq_sub_track_t **out)
{
    if (!sub || !cfg || !out) return MOQ_ERR_INVAL;
    *out = NULL;
#define STCFG_MIN offsetof(moq_sub_track_cfg_t, auth_tokens)
    if (cfg->struct_size < STCFG_MIN) return MOQ_ERR_INVAL;
#define STCFG_HAS(f) \
    (cfg->struct_size >= offsetof(moq_sub_track_cfg_t, f) + sizeof(cfg->f))
    if (sub->closed) return MOQ_ERR_CLOSED;

    moq_sub_track_t *t = alloc_track(sub);
    if (!t) return MOQ_ERR_WOULD_BLOCK;

    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = cfg->track_namespace;
    scfg.track_name = cfg->track_name;
    scfg.filter = cfg->filter;
    if (cfg->has_subscriber_priority) {
        scfg.has_subscriber_priority = true;
        scfg.subscriber_priority = cfg->subscriber_priority;
    }
    if (STCFG_HAS(auth_token_count) && cfg->auth_token_count > 0) {
        scfg.auth_tokens = cfg->auth_tokens;
        scfg.auth_token_count = cfg->auth_token_count;
    }
#undef STCFG_HAS
#undef STCFG_MIN

    moq_subscription_t h;
    moq_result_t rc = moq_session_subscribe(sub->session, &scfg, now_us, &h);
    if (rc < 0) return rc;

    t->state = SUB_TRACK_PENDING;
    t->handle = h;
    *out = t;
    return MOQ_OK;
}

moq_result_t moq_sub_unsubscribe(moq_subscriber_t *sub,
                                   moq_sub_track_t *track,
                                   uint64_t now_us)
{
    if (!sub || !track) return MOQ_ERR_INVAL;
    if (track->sub != sub) return MOQ_ERR_INVAL;
    if (track->state != SUB_TRACK_ACTIVE &&
        track->state != SUB_TRACK_PENDING)
        return MOQ_ERR_WRONG_STATE;

    moq_result_t rc = moq_session_unsubscribe(sub->session,
        track->handle, now_us);
    if (rc < 0) return rc;

    track->state = SUB_TRACK_DONE;
    return MOQ_OK;
}

moq_result_t moq_sub_release_track(moq_subscriber_t *sub,
                                   moq_sub_track_t *track)
{
    if (!sub || !track) return MOQ_ERR_INVAL;
    if (track->sub != sub) return MOQ_ERR_INVAL;
    /* Only a terminal track may be released; a live one must be unsubscribed
     * first so the session sends UNSUBSCRIBE. The subscription is already
     * complete here, so no session message is needed -- just free the slot. */
    if (track->state != SUB_TRACK_DONE && track->state != SUB_TRACK_ERROR)
        return MOQ_ERR_WRONG_STATE;
    track->state = SUB_TRACK_FREE;
    track->error_code = 0;
    return MOQ_OK;
}

void moq_sub_update_cfg_init(moq_sub_update_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = sizeof(*cfg);
}

moq_result_t moq_sub_update_subscription(moq_subscriber_t *sub,
                                           moq_sub_track_t *track,
                                           const moq_sub_update_cfg_t *cfg,
                                           uint64_t now_us)
{
    if (!sub || !track || !cfg) return MOQ_ERR_INVAL;
    if (track->sub != sub) return MOQ_ERR_INVAL;
    if (cfg->struct_size < sizeof(moq_sub_update_cfg_t)) return MOQ_ERR_INVAL;
    if (sub->closed) return MOQ_ERR_CLOSED;
    if (track->state != SUB_TRACK_ACTIVE) return MOQ_ERR_WRONG_STATE;

    moq_subscription_update_cfg_t ucfg;
    moq_subscription_update_cfg_init(&ucfg);
    ucfg.has_subscriber_priority = cfg->has_subscriber_priority;
    ucfg.subscriber_priority = cfg->subscriber_priority;
    ucfg.has_forward = cfg->has_forward;
    ucfg.forward = cfg->forward;
    ucfg.has_delivery_timeout = cfg->has_delivery_timeout;
    ucfg.delivery_timeout_us = cfg->delivery_timeout_us;

    return moq_session_update_subscription(sub->session, track->handle,
                                            &ucfg, now_us);
}

static void fill_sub_object(moq_sub_object_t *o, moq_sub_track_t *t,
                             const moq_object_received_event_t *obj)
{
    o->track = t;
    o->group_id = obj->group_id;
    o->subgroup_id = obj->subgroup_id;
    o->object_id = obj->object_id;
    o->publisher_priority = obj->publisher_priority;
    o->status = obj->status;
    o->end_of_group = obj->end_of_group;
    o->datagram = obj->datagram;
    o->payload = obj->payload;
    o->properties = obj->properties;
}

moq_result_t moq_sub_tick(moq_subscriber_t *sub, uint64_t now_us)
{
    if (!sub) return MOQ_ERR_INVAL;
    (void)now_us;

    if (sub->has_pending) {
        if (obj_queue_full(sub) || obj_bytes_over_budget(sub))
            return MOQ_ERR_WOULD_BLOCK;
        moq_sub_object_t *o = obj_push(sub);
        *o = sub->pending;
        sub_bytes_add(sub, rcbuf_pair_bytes(o->payload, o->properties));
        memset(&sub->pending, 0, sizeof(sub->pending));
        sub->has_pending = false;
    }

    if (sub->has_pending_fi) {
        bool fi_is_object = sub->pending_fi.kind == MOQ_SUB_FETCH_OBJECT;
        if (fi_queue_full(sub) ||
            (fi_is_object && obj_bytes_over_budget(sub)))
            return MOQ_ERR_WOULD_BLOCK;
        moq_sub_fetch_item_t *f = fi_push(sub);
        *f = sub->pending_fi;
        if (fi_is_object)
            sub_bytes_add(sub, rcbuf_pair_bytes(f->u.object.payload,
                                                f->u.object.properties));
        memset(&sub->pending_fi, 0, sizeof(sub->pending_fi));
        sub->has_pending_fi = false;
    }

    if (sub->has_pending_chunk) {
        if (chunk_queue_full(sub)) return MOQ_ERR_WOULD_BLOCK;
        moq_sub_chunk_t *c = chunk_push(sub);
        *c = sub->pending_chunk;
        memset(&sub->pending_chunk, 0, sizeof(sub->pending_chunk));
        sub->has_pending_chunk = false;
    }

    moq_event_t ev;
    while (moq_session_poll_events(sub->session, &ev, 1) == 1) {
        switch (ev.kind) {
        case MOQ_EVENT_SUBSCRIBE_OK: {
            moq_sub_track_t *t = find_track_by_handle(sub,
                ev.u.subscribe_ok.sub);
            if (t && t->state == SUB_TRACK_PENDING) {
                t->state = SUB_TRACK_ACTIVE;
                if (sub->callbacks.on_subscribed)
                    sub->callbacks.on_subscribed(sub->callbacks.ctx, t);
            }
            break;
        }

        case MOQ_EVENT_SUBSCRIBE_ERROR: {
            moq_sub_track_t *t = find_track_by_handle(sub,
                ev.u.subscribe_error.sub);
            if (t && t->state == SUB_TRACK_PENDING) {
                t->state = SUB_TRACK_ERROR;
                t->error_code = ev.u.subscribe_error.error_code;
                if (sub->callbacks.on_subscribe_error)
                    sub->callbacks.on_subscribe_error(
                        sub->callbacks.ctx, t,
                        ev.u.subscribe_error.error_code,
                        ev.u.subscribe_error.reason);
            }
            break;
        }

        case MOQ_EVENT_OBJECT_RECEIVED: {
            moq_object_received_event_t *obj = &ev.u.object_received;
            moq_sub_track_t *t = find_track_by_handle(sub, obj->sub);
            if (t && t->state == SUB_TRACK_ACTIVE) {
                if (obj_queue_full(sub) || obj_bytes_over_budget(sub)) {
                    fill_sub_object(&sub->pending, t, obj);
                    sub->has_pending = true;
                    ev.u.object_received.payload = NULL;
                    ev.u.object_received.properties = NULL;
                    moq_event_cleanup(&ev);
                    return MOQ_ERR_WOULD_BLOCK;
                }
                moq_sub_object_t *o = obj_push(sub);
                fill_sub_object(o, t, obj);
                sub_bytes_add(sub,
                    rcbuf_pair_bytes(o->payload, o->properties));
                ev.u.object_received.payload = NULL;
                ev.u.object_received.properties = NULL;
            }
            break;
        }

        case MOQ_EVENT_OBJECT_CHUNK: {
            if (!sub->streaming) {
                moq_event_cleanup(&ev);
                return MOQ_ERR_WRONG_STATE;
            }
            moq_object_chunk_event_t *ck = &ev.u.object_chunk;
            moq_sub_track_t *t = find_track_by_handle(sub, ck->sub);
            if (t && t->state == SUB_TRACK_ACTIVE) {
                moq_sub_chunk_t sc;
                memset(&sc, 0, sizeof(sc));
                sc.track = t;
                sc.group_id = ck->group_id;
                sc.subgroup_id = ck->subgroup_id;
                sc.object_id = ck->object_id;
                sc.payload_length = ck->payload_length;
                sc.publisher_priority = ck->publisher_priority;
                sc.status = ck->status;
                sc.terminal = ck->terminal;
                sc.begin = ck->begin;
                sc.end = ck->end;
                sc.end_of_group = ck->end_of_group;
                sc.chunk = ck->chunk;
                sc.properties = ck->properties;
                if (chunk_queue_full(sub)) {
                    sub->pending_chunk = sc;
                    sub->has_pending_chunk = true;
                    ck->chunk = NULL;
                    ck->properties = NULL;
                    moq_event_cleanup(&ev);
                    return MOQ_ERR_WOULD_BLOCK;
                }
                moq_sub_chunk_t *dst = chunk_push(sub);
                *dst = sc;
                ck->chunk = NULL;
                ck->properties = NULL;
            }
            break;
        }

        /* Result ring capacity == status request capacity, and a
         * request slot stays occupied until its result is polled,
         * so the ring can never be full when a request exists. */
        case MOQ_EVENT_TRACK_STATUS_OK: {
            moq_sub_status_req_t *r = find_status_by_handle(sub,
                ev.u.track_status_ok.handle);
            if (r) {
                if (sr_queue_full(sub)) {
                    moq_event_cleanup(&ev);
                    return MOQ_ERR_INTERNAL;
                }
                moq_sub_status_result_t *sr = sr_push(sub);
                sr->kind = MOQ_SUB_STATUS_OK;
                sr->request = r;
                sr->has_largest = ev.u.track_status_ok.has_largest;
                sr->largest_group = ev.u.track_status_ok.largest_group;
                sr->largest_object = ev.u.track_status_ok.largest_object;
                sr->has_expires = ev.u.track_status_ok.has_expires;
                sr->expires_ms = ev.u.track_status_ok.expires_ms;
                r->state = SUB_STATUS_DONE;
            }
            break;
        }

        case MOQ_EVENT_TRACK_STATUS_ERROR: {
            moq_sub_status_req_t *r = find_status_by_handle(sub,
                ev.u.track_status_error.handle);
            if (r) {
                if (sr_queue_full(sub)) {
                    moq_event_cleanup(&ev);
                    return MOQ_ERR_INTERNAL;
                }
                moq_sub_status_result_t *sr = sr_push(sub);
                sr->kind = MOQ_SUB_STATUS_ERROR;
                sr->request = r;
                sr->error_code = ev.u.track_status_error.error_code;
                sr->can_retry = ev.u.track_status_error.can_retry;
                sr->retry_after_ms = ev.u.track_status_error.retry_after_ms;
                r->state = SUB_STATUS_DONE;
            }
            break;
        }

        case MOQ_EVENT_FETCH_OK: {
            moq_sub_fetch_req_t *r = find_fetch_by_handle(sub,
                ev.u.fetch_ok.fetch);
            if (r && r->state == SUB_FETCH_PENDING) {
                if (fi_queue_full(sub)) {
                    sub->pending_fi.kind = MOQ_SUB_FETCH_OK;
                    sub->pending_fi.request = r;
                    sub->pending_fi.u.ok.end_of_track = ev.u.fetch_ok.end_of_track;
                    sub->pending_fi.u.ok.end_group = ev.u.fetch_ok.end_group;
                    sub->pending_fi.u.ok.end_object = ev.u.fetch_ok.end_object;
                    sub->has_pending_fi = true;
                    r->state = SUB_FETCH_ACTIVE;
                    moq_event_cleanup(&ev);
                    return MOQ_ERR_WOULD_BLOCK;
                }
                moq_sub_fetch_item_t *f = fi_push(sub);
                f->kind = MOQ_SUB_FETCH_OK;
                f->request = r;
                f->u.ok.end_of_track = ev.u.fetch_ok.end_of_track;
                f->u.ok.end_group = ev.u.fetch_ok.end_group;
                f->u.ok.end_object = ev.u.fetch_ok.end_object;
                r->state = SUB_FETCH_ACTIVE;
            }
            break;
        }

        case MOQ_EVENT_FETCH_ERROR: {
            moq_sub_fetch_req_t *r = find_fetch_by_handle(sub,
                ev.u.fetch_error.fetch);
            if (r) {
                if (fi_queue_full(sub)) {
                    sub->pending_fi.kind = MOQ_SUB_FETCH_ERROR;
                    sub->pending_fi.request = r;
                    sub->pending_fi.u.error.error_code = ev.u.fetch_error.error_code;
                    sub->pending_fi.u.error.can_retry = ev.u.fetch_error.can_retry;
                    sub->pending_fi.u.error.retry_after_ms = ev.u.fetch_error.retry_after_ms;
                    sub->has_pending_fi = true;
                    r->state = SUB_FETCH_DONE;
                    moq_event_cleanup(&ev);
                    return MOQ_ERR_WOULD_BLOCK;
                }
                moq_sub_fetch_item_t *f = fi_push(sub);
                f->kind = MOQ_SUB_FETCH_ERROR;
                f->request = r;
                f->u.error.error_code = ev.u.fetch_error.error_code;
                f->u.error.can_retry = ev.u.fetch_error.can_retry;
                f->u.error.retry_after_ms = ev.u.fetch_error.retry_after_ms;
                r->state = SUB_FETCH_DONE;
            }
            break;
        }

        case MOQ_EVENT_FETCH_OBJECT: {
            moq_sub_fetch_req_t *r = find_fetch_by_handle(sub,
                ev.u.fetch_object.fetch);
            /* FETCH_OK (draft-16 §9.17) may arrive at any time relative to the
             * FETCH (§9.16) object delivery -- a relay can FIN the FETCH object
             * stream before its FETCH_OK reaches us. Queue objects that arrive
             * while still PENDING; FETCH_OK still owns the PENDING->ACTIVE
             * transition (we do NOT mark ACTIVE here). */
            if (r && (r->state == SUB_FETCH_PENDING ||
                      r->state == SUB_FETCH_ACTIVE)) {
                if (fi_queue_full(sub) || obj_bytes_over_budget(sub)) {
                    sub->pending_fi.kind = MOQ_SUB_FETCH_OBJECT;
                    sub->pending_fi.request = r;
                    sub->pending_fi.u.object.group_id = ev.u.fetch_object.group_id;
                    sub->pending_fi.u.object.object_id = ev.u.fetch_object.object_id;
                    sub->pending_fi.u.object.publisher_priority = ev.u.fetch_object.publisher_priority;
                    sub->pending_fi.u.object.payload = ev.u.fetch_object.payload;
                    sub->pending_fi.u.object.properties = ev.u.fetch_object.properties;
                    sub->has_pending_fi = true;
                    ev.u.fetch_object.payload = NULL;
                    ev.u.fetch_object.properties = NULL;
                    moq_event_cleanup(&ev);
                    return MOQ_ERR_WOULD_BLOCK;
                }
                moq_sub_fetch_item_t *f = fi_push(sub);
                f->kind = MOQ_SUB_FETCH_OBJECT;
                f->request = r;
                f->u.object.group_id = ev.u.fetch_object.group_id;
                f->u.object.object_id = ev.u.fetch_object.object_id;
                f->u.object.publisher_priority = ev.u.fetch_object.publisher_priority;
                f->u.object.payload = ev.u.fetch_object.payload;
                f->u.object.properties = ev.u.fetch_object.properties;
                sub_bytes_add(sub,
                    rcbuf_pair_bytes(f->u.object.payload,
                                     f->u.object.properties));
                ev.u.fetch_object.payload = NULL;
                ev.u.fetch_object.properties = NULL;
            }
            break;
        }

        case MOQ_EVENT_FETCH_GAP: {
            moq_sub_fetch_req_t *r = find_fetch_by_handle(sub,
                ev.u.fetch_gap.fetch);
            /* Same as FETCH_OBJECT: a gap can arrive on the object stream before
             * FETCH_OK. Queue it while PENDING; FETCH_OK owns the transition. */
            if (r && (r->state == SUB_FETCH_PENDING ||
                      r->state == SUB_FETCH_ACTIVE)) {
                if (fi_queue_full(sub)) {
                    sub->pending_fi.kind = MOQ_SUB_FETCH_GAP;
                    sub->pending_fi.request = r;
                    sub->pending_fi.u.gap.range_kind = ev.u.fetch_gap.range_kind;
                    sub->pending_fi.u.gap.group_id = ev.u.fetch_gap.group_id;
                    sub->pending_fi.u.gap.object_id = ev.u.fetch_gap.object_id;
                    sub->has_pending_fi = true;
                    moq_event_cleanup(&ev);
                    return MOQ_ERR_WOULD_BLOCK;
                }
                moq_sub_fetch_item_t *f = fi_push(sub);
                f->kind = MOQ_SUB_FETCH_GAP;
                f->request = r;
                f->u.gap.range_kind = ev.u.fetch_gap.range_kind;
                f->u.gap.group_id = ev.u.fetch_gap.group_id;
                f->u.gap.object_id = ev.u.fetch_gap.object_id;
            }
            break;
        }

        case MOQ_EVENT_FETCH_COMPLETE: {
            moq_sub_fetch_req_t *r = find_fetch_by_handle(sub,
                ev.u.fetch_complete.fetch);
            if (r && r->state == SUB_FETCH_ACTIVE) {
                if (fi_queue_full(sub)) {
                    sub->pending_fi.kind = MOQ_SUB_FETCH_COMPLETE;
                    sub->pending_fi.request = r;
                    sub->has_pending_fi = true;
                    r->state = SUB_FETCH_DONE;
                    moq_event_cleanup(&ev);
                    return MOQ_ERR_WOULD_BLOCK;
                }
                moq_sub_fetch_item_t *f = fi_push(sub);
                f->kind = MOQ_SUB_FETCH_COMPLETE;
                f->request = r;
                r->state = SUB_FETCH_DONE;
            }
            break;
        }

        case MOQ_EVENT_FETCH_CANCELLED:
            break;

        case MOQ_EVENT_UNSUBSCRIBED: {
            moq_sub_track_t *t = find_track_by_handle(sub,
                ev.u.unsubscribed.sub);
            if (t) t->state = SUB_TRACK_DONE;
            break;
        }

        case MOQ_EVENT_SUBSCRIBE_DONE: {
            moq_sub_track_t *t = find_track_by_handle(sub,
                ev.u.subscribe_done.sub);
            if (t) {
                t->state = SUB_TRACK_DONE;
                if (sub->on_subscribe_done)
                    sub->on_subscribe_done(sub->callbacks.ctx, t,
                                           ev.u.subscribe_done.status_code);
            }
            break;
        }

        case MOQ_EVENT_GOAWAY:
            sub->draining = true;
            if (sub->on_goaway) {
                sub->on_goaway(sub->callbacks.ctx,
                    ev.u.goaway.new_session_uri);
            } else if (sub->callbacks.on_draining) {
                sub->callbacks.on_draining(sub->callbacks.ctx);
            }
            break;

        case MOQ_EVENT_SESSION_CLOSED:
            sub->closed = true;
            for (size_t i = 0; i < sub->track_cap; i++)
                if (sub->tracks[i].state == SUB_TRACK_ACTIVE ||
                    sub->tracks[i].state == SUB_TRACK_PENDING)
                    sub->tracks[i].state = SUB_TRACK_DONE;
            for (size_t i = 0; i < sub->status_cap; i++)
                if (sub->status_reqs[i].state == SUB_STATUS_PENDING)
                    sub->status_reqs[i].state = SUB_STATUS_DONE;
            for (size_t i = 0; i < sub->fetch_cap; i++)
                if (sub->fetch_reqs[i].state == SUB_FETCH_PENDING ||
                    sub->fetch_reqs[i].state == SUB_FETCH_ACTIVE)
                    sub->fetch_reqs[i].state = SUB_FETCH_DONE;
            if (sub->callbacks.on_closed)
                sub->callbacks.on_closed(sub->callbacks.ctx,
                    ev.u.closed.code);
            break;

        default:
            break;
        }

        moq_event_cleanup(&ev);
    }

    return MOQ_OK;
}

moq_result_t moq_sub_poll_object(moq_subscriber_t *sub,
                                   moq_sub_object_t *out)
{
    if (!sub || !out) return MOQ_ERR_INVAL;
    if (sub->obj_head >= sub->obj_tail) return MOQ_DONE;

    *out = sub->objects[sub->obj_head % sub->obj_cap];
    sub->obj_head++;
    sub_bytes_sub(sub, rcbuf_pair_bytes(out->payload, out->properties));
    return MOQ_OK;
}

void moq_sub_object_cleanup(moq_sub_object_t *obj)
{
    if (!obj) return;
    if (obj->payload) { moq_rcbuf_decref(obj->payload); obj->payload = NULL; }
    if (obj->properties) { moq_rcbuf_decref(obj->properties); obj->properties = NULL; }
}

void moq_sub_status_cfg_init(moq_sub_status_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = sizeof(*cfg);
}

moq_result_t moq_sub_track_status(moq_subscriber_t *sub,
                                    const moq_sub_status_cfg_t *cfg,
                                    uint64_t now_us,
                                    moq_sub_status_req_t **out)
{
    if (!sub || !cfg || !out) return MOQ_ERR_INVAL;
    *out = NULL;
#define SSCFG_MIN offsetof(moq_sub_status_cfg_t, auth_tokens)
    if (cfg->struct_size < SSCFG_MIN) return MOQ_ERR_INVAL;
#define SSCFG_HAS(f) \
    (cfg->struct_size >= offsetof(moq_sub_status_cfg_t, f) + sizeof(cfg->f))
    if (sub->closed) return MOQ_ERR_CLOSED;

    moq_sub_status_req_t *r = alloc_status_req(sub);
    if (!r) return MOQ_ERR_WOULD_BLOCK;

    moq_track_status_cfg_t scfg;
    moq_track_status_cfg_init(&scfg);
    scfg.track_namespace = cfg->track_namespace;
    scfg.track_name = cfg->track_name;
    if (SSCFG_HAS(auth_token_count) && cfg->auth_token_count > 0) {
        scfg.auth_tokens = cfg->auth_tokens;
        scfg.auth_token_count = cfg->auth_token_count;
    }
#undef SSCFG_HAS
#undef SSCFG_MIN

    moq_track_status_handle_t h;
    moq_result_t rc = moq_session_track_status(sub->session, &scfg, now_us, &h);
    if (rc < 0) return rc;

    r->state = SUB_STATUS_PENDING;
    r->handle = h;
    *out = r;
    return MOQ_OK;
}

moq_result_t moq_sub_poll_status(moq_subscriber_t *sub,
                                   moq_sub_status_result_t *out)
{
    if (!sub || !out) return MOQ_ERR_INVAL;
    if (sub->sr_head >= sub->sr_tail) return MOQ_DONE;
    *out = sub->status_results[sub->sr_head % sub->sr_cap];
    if (out->request) out->request->state = SUB_STATUS_FREE;
    sub->sr_head++;
    return MOQ_OK;
}

bool moq_sub_is_draining(const moq_subscriber_t *sub)
{
    return sub ? sub->draining : false;
}

moq_sub_track_state_t moq_sub_track_get_state(const moq_sub_track_t *track)
{
    if (!track) return MOQ_SUB_TRACK_DONE;
    switch (track->state) {
    case SUB_TRACK_PENDING: return MOQ_SUB_TRACK_PENDING;
    case SUB_TRACK_ACTIVE:  return MOQ_SUB_TRACK_ACTIVE;
    case SUB_TRACK_ERROR:   return MOQ_SUB_TRACK_ERROR;
    default:                return MOQ_SUB_TRACK_DONE;
    }
}

bool moq_sub_track_is_active(const moq_sub_track_t *track)
{
    return track && track->state == SUB_TRACK_ACTIVE;
}

bool moq_sub_track_get_error(const moq_sub_track_t *track,
                              moq_request_error_t *out_code)
{
    if (!track || track->state != SUB_TRACK_ERROR) return false;
    if (out_code) *out_code = track->error_code;
    return true;
}

/* -- Fetch ---------------------------------------------------------- */

void moq_sub_fetch_cfg_init(moq_sub_fetch_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = sizeof(*cfg);
}

moq_result_t moq_sub_fetch(moq_subscriber_t *sub,
                             const moq_sub_fetch_cfg_t *cfg,
                             uint64_t now_us,
                             moq_sub_fetch_req_t **out)
{
    if (!sub || !cfg || !out) return MOQ_ERR_INVAL;
    *out = NULL;
#define SFCFG_MIN offsetof(moq_sub_fetch_cfg_t, auth_tokens)
    if (cfg->struct_size < SFCFG_MIN) return MOQ_ERR_INVAL;
#define SFCFG_HAS(f) \
    (cfg->struct_size >= offsetof(moq_sub_fetch_cfg_t, f) + sizeof(cfg->f))
    if (sub->closed) return MOQ_ERR_CLOSED;

    moq_sub_fetch_req_t *r = alloc_fetch_req(sub);
    if (!r) return MOQ_ERR_WOULD_BLOCK;

    moq_fetch_cfg_t fcfg;
    moq_fetch_cfg_init(&fcfg);
    fcfg.track_namespace = cfg->track_namespace;
    fcfg.track_name = cfg->track_name;
    fcfg.start_group = cfg->start_group;
    fcfg.start_object = cfg->start_object;
    fcfg.end_group = cfg->end_group;
    fcfg.end_object = cfg->end_object;
    fcfg.group_order = cfg->group_order;
    fcfg.has_subscriber_priority = cfg->has_subscriber_priority;
    fcfg.subscriber_priority = cfg->subscriber_priority;
    if (SFCFG_HAS(auth_token_count) && cfg->auth_token_count > 0) {
        fcfg.auth_tokens = cfg->auth_tokens;
        fcfg.auth_token_count = cfg->auth_token_count;
    }
#undef SFCFG_HAS
#undef SFCFG_MIN

    moq_fetch_t h;
    moq_result_t rc = moq_session_fetch(sub->session, &fcfg, now_us, &h);
    if (rc < 0) return rc;

    r->state = SUB_FETCH_PENDING;
    r->handle = h;
    *out = r;
    return MOQ_OK;
}

/* -- Joining fetch -------------------------------------------------- */

void moq_sub_joining_fetch_cfg_init(moq_sub_joining_fetch_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = sizeof(*cfg);
}

moq_result_t moq_sub_joining_fetch(moq_subscriber_t *sub,
                                     const moq_sub_joining_fetch_cfg_t *cfg,
                                     uint64_t now_us,
                                     moq_sub_fetch_req_t **out)
{
    if (!sub || !cfg || !out) return MOQ_ERR_INVAL;
    *out = NULL;
    if (cfg->struct_size < sizeof(moq_sub_joining_fetch_cfg_t))
        return MOQ_ERR_INVAL;
    if (!cfg->track) return MOQ_ERR_INVAL;
    if (cfg->track->sub != sub) return MOQ_ERR_INVAL;
    if (cfg->track->state != SUB_TRACK_ACTIVE) return MOQ_ERR_WRONG_STATE;
    if (sub->closed) return MOQ_ERR_CLOSED;

    moq_sub_fetch_req_t *r = alloc_fetch_req(sub);
    if (!r) return MOQ_ERR_WOULD_BLOCK;

    moq_fetch_cfg_t fcfg;
    moq_fetch_cfg_init(&fcfg);
    fcfg.is_joining = true;
    fcfg.joining_sub = cfg->track->handle;
    fcfg.joining_relative = cfg->relative;
    fcfg.joining_start = cfg->joining_start;
    fcfg.group_order = cfg->group_order;
    fcfg.has_subscriber_priority = cfg->has_subscriber_priority;
    fcfg.subscriber_priority = cfg->subscriber_priority;

    moq_fetch_t h;
    moq_result_t rc = moq_session_fetch(sub->session, &fcfg, now_us, &h);
    if (rc < 0) return rc;

    r->state = SUB_FETCH_PENDING;
    r->handle = h;
    *out = r;
    return MOQ_OK;
}

moq_result_t moq_sub_cancel_fetch(moq_subscriber_t *sub,
                                    moq_sub_fetch_req_t *req,
                                    uint64_t now_us)
{
    if (!sub || !req) return MOQ_ERR_INVAL;
    if (req->sub != sub) return MOQ_ERR_INVAL;
    if (req->state != SUB_FETCH_PENDING &&
        req->state != SUB_FETCH_ACTIVE)
        return MOQ_ERR_WRONG_STATE;

    moq_result_t rc = moq_session_fetch_cancel(sub->session,
        req->handle, now_us);
    if (rc < 0) return rc;

    /* Drop any queued/pending items for this request before the slot becomes
     * reusable, so a later fetch reusing the slot pointer cannot inherit them. */
    purge_fetch_items_for(sub, req);

    req->state = SUB_FETCH_FREE;
    return MOQ_OK;
}

moq_result_t moq_sub_poll_fetch(moq_subscriber_t *sub,
                                  moq_sub_fetch_item_t *out)
{
    if (!sub || !out) return MOQ_ERR_INVAL;
    if (sub->fi_head >= sub->fi_tail) return MOQ_DONE;

    *out = sub->fetch_items[sub->fi_head % sub->fi_cap];
    memset(&sub->fetch_items[sub->fi_head % sub->fi_cap], 0,
        sizeof(moq_sub_fetch_item_t));
    sub->fi_head++;
    if (out->kind == MOQ_SUB_FETCH_OBJECT)
        sub_bytes_sub(sub, rcbuf_pair_bytes(out->u.object.payload,
                                            out->u.object.properties));

    if (out->request &&
        (out->kind == MOQ_SUB_FETCH_ERROR ||
         out->kind == MOQ_SUB_FETCH_COMPLETE))
        out->request->state = SUB_FETCH_FREE;

    return MOQ_OK;
}

void moq_sub_fetch_item_cleanup(moq_sub_fetch_item_t *item)
{
    if (!item) return;
    fetch_item_release_refs(item);
}

/* -- Chunk poll ----------------------------------------------------- */

moq_result_t moq_sub_poll_chunk(moq_subscriber_t *sub,
                                  moq_sub_chunk_t *out)
{
    if (!sub || !out) return MOQ_ERR_INVAL;
    if (sub->chunk_head >= sub->chunk_tail) return MOQ_DONE;

    *out = sub->chunks[sub->chunk_head % sub->chunk_cap];
    memset(&sub->chunks[sub->chunk_head % sub->chunk_cap], 0,
        sizeof(moq_sub_chunk_t));
    sub->chunk_head++;
    return MOQ_OK;
}

void moq_sub_chunk_cleanup(moq_sub_chunk_t *chunk)
{
    if (!chunk) return;
    chunk_release_refs(chunk);
}
