/*
 * Local media demo — relay.
 *
 * Connects upstream to the publisher as a MoQ client and listens
 * downstream for the client as a MoQ server. Accepts downstream
 * subscribe requests and mirrors them upstream. Forwards objects
 * and properties from upstream to downstream per-track.
 *
 * Usage: moq_demo_relay <cert.pem> <key.pem> [pub-host] [pub-port]
 *                       [listen-port] [--pool]
 */

#include <moq/picoquic.h>
#include <moq/session.h>
#include <moq/rcbuf.h>
#include <picoquic_packet_loop.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <netinet/in.h>
#include <arpa/inet.h>

static volatile int running = 1;
static void sigint_handler(int sig) { (void)sig; running = 0; }

/* ------------------------------------------------------------------ *
 * Optional single-shard recycling allocator (opt-in demo).
 *
 * This is an EXAMPLE-LOCAL allocator, not a libmoq API. It shows the
 * zero-allocation relay pattern: on a single-shard relay, the inbound object
 * rcbuf slab is recycled through a per-shard pool, and fan-out is an rcbuf
 * incref (no payload clone). After warm-up the libmoq allocator does no
 * malloc/free on the hot path. See examples/local-media/README.md.
 *
 * Scope of the claim: this counts allocations that flow through libmoq's
 * moq_alloc_t (sessions + the picoquic adapter wrappers). picoquic's own
 * internal allocator and any other process-wide malloc are NOT counted.
 *
 * Blocks are grouped into power-of-two size classes so the variable-sized
 * media objects of a real stream map onto a bounded set of buckets that all
 * warm up. A freed block is cached (its first bytes hold the free-list link)
 * and reused for the next same-class allocation; real malloc()/free() happen
 * only on a cache miss / at shutdown. The relay runs on one packet-loop
 * thread, so the non-atomic free-list and rcbuf refcounts are safe with no
 * locks. */
typedef struct pool_node { struct pool_node *next; } pool_node_t;

#define POOL_MAX_CLASS 40 /* Ample for this demo; oversize requests fail cleanly. */

typedef struct {
    pool_node_t *free_head[POOL_MAX_CLASS];
    int64_t      real_allocs;   /* actual malloc() calls */
    int64_t      real_frees;    /* actual free() calls (cache miss / destroy) */
    int64_t      live_bytes;    /* class-rounded bytes handed out */
    int64_t      peak_bytes;
    int64_t      baseline_bytes;/* live_bytes at the last reset_stats */
    int          oom;
} relay_pool_t;

/* Round up to a power-of-two slot >= max(size, sizeof(node)); return its
 * class index. Deterministic: the same requested size always maps to the
 * same class, so alloc and free agree. */
static int pool_class(size_t size, size_t *slot_out)
{
    size_t slot = sizeof(pool_node_t);
    int cls = 0;
    while (slot < size && cls < POOL_MAX_CLASS - 1) { slot <<= 1; cls++; }
    if (slot < size) return -1;
    *slot_out = slot;
    return cls;
}

static void *relay_pool_alloc(size_t size, void *ctx)
{
    relay_pool_t *p = (relay_pool_t *)ctx;
    if (size == 0) size = 1;
    size_t slot; int cls = pool_class(size, &slot);
    if (cls < 0) { /* request exceeds the largest size class: fail cleanly */
        p->oom = 1;
        return NULL;
    }
    void *ret;
    if (p->free_head[cls]) {
        pool_node_t *n = p->free_head[cls];
        p->free_head[cls] = n->next;
        ret = (void *)n;
    } else {
        ret = malloc(slot);
        if (!ret) { p->oom = 1; return NULL; }
        p->real_allocs++;
    }
    p->live_bytes += (int64_t)slot;
    if (p->live_bytes > p->peak_bytes) p->peak_bytes = p->live_bytes;
    return ret;
}

static void relay_pool_free(void *ptr, size_t size, void *ctx)
{
    relay_pool_t *p = (relay_pool_t *)ctx;
    if (!ptr) return;
    if (size == 0) size = 1;
    size_t slot; int cls = pool_class(size, &slot);
    if (cls < 0) { p->oom = 1; return; } /* impossible for pool-owned blocks */
    p->live_bytes -= (int64_t)slot;
    pool_node_t *n = (pool_node_t *)ptr;
    n->next = p->free_head[cls];
    p->free_head[cls] = n;
}

static void *relay_pool_realloc(void *ptr, size_t old_sz, size_t new_sz, void *ctx)
{
    if (!ptr) return relay_pool_alloc(new_sz, ctx);
    if (new_sz == 0) { relay_pool_free(ptr, old_sz, ctx); return NULL; }
    size_t old_slot, new_slot;
    int old_cls = pool_class(old_sz ? old_sz : 1, &old_slot);
    int new_cls = pool_class(new_sz, &new_slot);
    relay_pool_t *p = (relay_pool_t *)ctx;
    if (old_cls < 0 || new_cls < 0) { p->oom = 1; return NULL; }
    if (old_slot == new_slot) return ptr; /* same class: grow in place */
    void *np = relay_pool_alloc(new_sz, ctx);
    if (!np) return NULL;
    memcpy(np, ptr, old_sz < new_sz ? old_sz : new_sz);
    relay_pool_free(ptr, old_sz, ctx);
    return np;
}

static void relay_pool_reset_stats(relay_pool_t *p)
{
    p->real_allocs = 0;
    p->real_frees = 0;
    p->peak_bytes = p->live_bytes;
    p->baseline_bytes = p->live_bytes;
}

static void relay_pool_destroy(relay_pool_t *p)
{
    for (int c = 0; c < POOL_MAX_CLASS; c++) {
        pool_node_t *n = p->free_head[c];
        while (n) { pool_node_t *next = n->next; p->real_frees++; free(n); n = next; }
        p->free_head[c] = NULL;
    }
}

#define MAX_RELAY_TRACKS 4

/* Adaptive warm-up: the pool's hot-path counters are reset only once the
 * allocator has been QUIET (no new real malloc) for this many consecutive
 * forwarded objects, and after at least the minimum below. This is
 * deterministic where a fixed object count is not: media object sizes and the
 * per-group subgroup state first-touch their size classes at times that race a
 * fixed window, so we instead wait until first-touch growth has demonstrably
 * ceased. The window must exceed the stream's group/keyframe cadence so a
 * full group cycle is observed before measuring. */
#define RELAY_POOL_WARMUP_STABLE_FORWARDS 24
#define RELAY_POOL_WARMUP_MIN_FORWARDS    8

/* The well-known MSF catalog track name (mirrors MOQ_MSF_CATALOG_TRACK_NAME;
 * defined locally so the toy relay need not link moq::msf). A catalog group is
 * retained at the origin and served only by an explicit Joining FETCH -- it is
 * never pushed to a plain SUBSCRIBE -- so the relay must FETCH it upstream and
 * forward it downstream as a normal object for the plain-subscribing client. */
#define RELAY_CATALOG_NAME     "catalog"
#define RELAY_CATALOG_NAME_LEN 7

typedef struct {
    moq_pq_conn_t  *adapter;
    moq_session_t   *session;
    picoquic_cnx_t  *cnx;
} relay_conn_t;

typedef struct {
    bool                   active;
    bool                   is_catalog;    /* retained track: pull via Joining FETCH */
    /* Downstream (client→relay) subscription. */
    moq_subscription_t     down_sub;
    bool                   down_accepted;
    /* Upstream (relay→publisher) subscription. */
    moq_subscription_t     up_sub;
    bool                   up_subscribed;
    bool                   up_accepted;
    /* Upstream Joining FETCH for a retained (catalog) group. */
    bool                   up_has_largest; /* SUBSCRIBE_OK advertised a Largest */
    bool                   fetch_started;
    bool                   fetch_done;
    moq_fetch_t            up_fetch;
    /* Downstream publish state. */
    moq_subgroup_handle_t  sg;
    bool                   sg_open;
    bool                   sg_has_ext;
    uint64_t               sg_group;
} relay_track_t;

static bool relay_name_is_catalog(const uint8_t *name, size_t len)
{
    return len == RELAY_CATALOG_NAME_LEN &&
           memcmp(name, RELAY_CATALOG_NAME, RELAY_CATALOG_NAME_LEN) == 0;
}

typedef struct {
    moq_alloc_t    alloc;
    relay_conn_t   upstream;
    relay_conn_t   downstream;

    bool           up_established;
    bool           up_closed;

    relay_track_t  tracks[MAX_RELAY_TRACKS];
    uint64_t       forwarded;

    /* Optional single-shard pool demo (see relay_pool_t above). */
    relay_pool_t  *pool;             /* NULL unless the pool is enabled */
    bool           pool_warm;        /* hot-path stats reset done */
    uint64_t       pool_warmup_excluded;
    int64_t        pool_seen_allocs; /* real_allocs at last observed growth */
    uint64_t       pool_stable_fwd;  /* forwarded count when growth last seen */

    /* Namespace/name storage for upstream subscribes. Copied from
     * downstream event borrows before calling advancing APIs. */
    uint8_t        ns_data[MAX_RELAY_TRACKS][128];
    moq_bytes_t    ns_parts[MAX_RELAY_TRACKS][8];
    size_t         ns_count[MAX_RELAY_TRACKS];
    uint8_t        name_buf[MAX_RELAY_TRACKS][128];
    size_t         name_len[MAX_RELAY_TRACKS];
} relay_ctx_t;

#define RELAY_MAX_NAME_LEN 128
#define RELAY_MAX_NS_PARTS 8

/* Best-effort flush. Callers inside event handlers ignore the return
 * value; the main loop_callback checks service errors each iteration. */
static int service_conn(relay_conn_t *c, uint64_t now) {
    if (!c->adapter) return 0;
    return moq_pq_service(c->adapter, now);
}

static relay_track_t *find_track_by_down_sub(relay_ctx_t *ctx,
                                              moq_subscription_t sub)
{
    for (int i = 0; i < MAX_RELAY_TRACKS; i++)
        if (ctx->tracks[i].active &&
            moq_subscription_eq(ctx->tracks[i].down_sub, sub))
            return &ctx->tracks[i];
    return NULL;
}

static relay_track_t *find_track_by_up_sub(relay_ctx_t *ctx,
                                            moq_subscription_t sub)
{
    for (int i = 0; i < MAX_RELAY_TRACKS; i++)
        if (ctx->tracks[i].active && ctx->tracks[i].up_subscribed &&
            moq_subscription_eq(ctx->tracks[i].up_sub, sub))
            return &ctx->tracks[i];
    return NULL;
}

static relay_track_t *find_track_by_up_fetch(relay_ctx_t *ctx,
                                              moq_fetch_t fetch)
{
    for (int i = 0; i < MAX_RELAY_TRACKS; i++)
        if (ctx->tracks[i].active && ctx->tracks[i].fetch_started &&
            moq_fetch_eq(ctx->tracks[i].up_fetch, fetch))
            return &ctx->tracks[i];
    return NULL;
}

static relay_track_t *alloc_track(relay_ctx_t *ctx)
{
    for (int i = 0; i < MAX_RELAY_TRACKS; i++)
        if (!ctx->tracks[i].active) return &ctx->tracks[i];
    return NULL;
}

/* -- Upstream (client) callbacks ------------------------------------- */

static int upstream_callback(picoquic_cnx_t *cnx,
    uint64_t stream_id, uint8_t *bytes, size_t length,
    picoquic_call_back_event_t event, void *callback_ctx,
    void *stream_ctx)
{
    relay_ctx_t *ctx = (relay_ctx_t *)callback_ctx;
    if (!ctx->upstream.adapter) return 0;
    return moq_pq_callback(cnx, stream_id, bytes, length,
                            event, ctx->upstream.adapter, stream_ctx);
}

/* -- Downstream (server) callbacks ----------------------------------- */

static int downstream_callback(picoquic_cnx_t *cnx,
    uint64_t stream_id, uint8_t *bytes, size_t length,
    picoquic_call_back_event_t event, void *callback_ctx,
    void *stream_ctx)
{
    relay_ctx_t *ctx = (relay_ctx_t *)callback_ctx;

    if (event == picoquic_callback_almost_ready ||
        event == picoquic_callback_ready) {
        if (ctx->downstream.adapter) {
            /* Single downstream session: refuse any additional connection
             * rather than mixing its events into the first session (a later
             * almost_ready/ready for the SAME connection is fine). */
            if (cnx != ctx->downstream.cnx)
                picoquic_close(cnx, PICOQUIC_TRANSPORT_SERVER_BUSY);
            return 0;
        }

        uint64_t now = picoquic_get_quic_time(picoquic_get_quic_ctx(cnx));
        moq_session_cfg_t scfg;
        moq_session_cfg_init_sized(&scfg, sizeof(scfg), &ctx->alloc, MOQ_PERSPECTIVE_SERVER);
        scfg.send_request_capacity = true;
        scfg.initial_request_capacity = 64;

        if (moq_session_create(&scfg, now, &ctx->downstream.session) != MOQ_OK)
            return -1;

        moq_pq_conn_cfg_t acfg;
        moq_pq_conn_cfg_init(&acfg);
        acfg.session = ctx->downstream.session;
        acfg.cnx = cnx;
        acfg.alloc = &ctx->alloc;
        if (moq_pq_conn_create(&acfg, &ctx->downstream.adapter) != 0) {
            moq_session_destroy(ctx->downstream.session);
            ctx->downstream.session = NULL;
            return -1;
        }
        ctx->downstream.cnx = cnx;
        fprintf(stderr, "  relay: downstream connected\n");
        return 0;
    }

    /* Drive the downstream session only from its owning connection; refuse
     * events from any other connection instead of feeding the wrong session. */
    if (ctx->downstream.adapter && cnx == ctx->downstream.cnx)
        return moq_pq_callback(cnx, stream_id, bytes, length,
                                event, ctx->downstream.adapter, stream_ctx);
    if (ctx->downstream.adapter && cnx != ctx->downstream.cnx)
        picoquic_close(cnx, PICOQUIC_TRANSPORT_SERVER_BUSY);
    return 0;
}

/* -- Subscribe upstream for a downstream request ---------------------- */

static void subscribe_upstream_for(relay_ctx_t *ctx, int idx, uint64_t now)
{
    relay_track_t *t = &ctx->tracks[idx];
    if (t->up_subscribed || !ctx->up_established) return;

    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace.parts = ctx->ns_parts[idx];
    scfg.track_namespace.count = ctx->ns_count[idx];
    scfg.track_name = (moq_bytes_t){ ctx->name_buf[idx], ctx->name_len[idx] };
    /* The catalog is a retained group pulled via a Joining FETCH, which requires
     * the subscription to carry the LARGEST_OBJECT filter (so SUBSCRIBE_OK
     * advertises the retained Largest the fetch joins to). Live tracks use
     * NEXT_GROUP as before. */
    scfg.filter = t->is_catalog ? MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT
                                : MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;

    moq_result_t rc = moq_session_subscribe(
        ctx->upstream.session, &scfg, now, &t->up_sub);
    if (rc == MOQ_OK) {
        t->up_subscribed = true;
        fprintf(stderr, "  relay: upstream subscribe sent for \"%.*s\"\n",
            (int)ctx->name_len[idx], (const char *)ctx->name_buf[idx]);
    } else {
        fprintf(stderr, "  relay: upstream subscribe failed: %d\n", rc);
    }
    service_conn(&ctx->upstream, now);
}

/* -- Event pumps ----------------------------------------------------- */

/* Forward one object downstream on the track's subscription. Shared by the
 * live SUBSCRIBE path (forward_object) and the retained Joining-FETCH path
 * (catalog): a fetched object is republished to the plain-subscribing client as
 * an ordinary subgroup object, so the client needs no fetch path of its own. */
static void forward_one(relay_ctx_t *ctx, relay_track_t *t,
                        uint64_t group_id, uint64_t object_id,
                        uint8_t publisher_priority, moq_rcbuf_t *payload,
                        moq_rcbuf_t *properties, bool end_of_group,
                        uint64_t now)
{
    if (!t->down_accepted || !payload) return;

    size_t pplen = properties ? moq_rcbuf_len(properties) : 0;
    bool need_ext = pplen > 0;

    if (t->sg_open && t->sg_group != group_id) {
        moq_session_close_subgroup(ctx->downstream.session, t->sg, now);
        t->sg_open = false;
    }
    if (t->sg_open && need_ext && !t->sg_has_ext) {
        moq_session_close_subgroup(ctx->downstream.session, t->sg, now);
        t->sg_open = false;
    }

    if (!t->sg_open) {
        moq_subgroup_cfg_t sgcfg;
        moq_subgroup_cfg_init(&sgcfg);
        sgcfg.group_id = group_id;
        sgcfg.subgroup_id = 0;
        sgcfg.publisher_priority = publisher_priority;
        sgcfg.object_properties = need_ext;
        sgcfg.end_of_group = end_of_group;

        moq_result_t rc = moq_session_open_subgroup(
            ctx->downstream.session, t->down_sub,
            &sgcfg, now, &t->sg);
        if (rc != MOQ_OK) return;
        t->sg_open = true;
        t->sg_has_ext = need_ext;
        t->sg_group = group_id;
    }

    moq_result_t wrc;
    if (need_ext || t->sg_has_ext) {
        moq_object_cfg_t ocfg;
        moq_object_cfg_init(&ocfg);
        ocfg.object_id = object_id;
        ocfg.payload = payload;
        ocfg.properties = properties;
        wrc = moq_session_write_object_ex(
            ctx->downstream.session, t->sg, &ocfg, now);
    } else {
        wrc = moq_session_write_object(
            ctx->downstream.session, t->sg,
            object_id, payload, now);
    }
    if (wrc == MOQ_OK) {
        ctx->forwarded++;
        fprintf(stderr, "  relay: fwd g=%llu o=%llu\n",
            (unsigned long long)group_id,
            (unsigned long long)object_id);
    }
    service_conn(&ctx->downstream, now);
}

static void forward_object(relay_ctx_t *ctx, relay_track_t *t,
                            moq_object_received_event_t *obj, uint64_t now)
{
    forward_one(ctx, t, obj->group_id, obj->object_id, obj->publisher_priority,
                obj->payload, obj->properties, obj->end_of_group, now);
}

/* Issue the upstream Joining FETCH that pulls the retained catalog group. The
 * subscription must already be established (SUBSCRIBE_OK seen) with a current
 * Largest, so call this only once up_has_largest is set. Retried each pump tick
 * until it lands (request capacity may not be granted on the first attempt). */
static void maybe_start_catalog_fetch(relay_ctx_t *ctx, relay_track_t *t,
                                      uint64_t now)
{
    if (!t->is_catalog || t->fetch_started || !t->up_has_largest) return;

    moq_fetch_cfg_t fcfg;
    moq_fetch_cfg_init(&fcfg);
    fcfg.is_joining = true;
    fcfg.joining_relative = true;    /* offset 0 from Largest -> latest retained group */
    fcfg.joining_sub = t->up_sub;
    fcfg.joining_start = 0;

    moq_result_t rc = moq_session_fetch(
        ctx->upstream.session, &fcfg, now, &t->up_fetch);
    if (rc == MOQ_OK) {
        t->fetch_started = true;
        fprintf(stderr, "  relay: upstream joining fetch sent for catalog\n");
    } else if (rc != MOQ_ERR_REQUEST_BLOCKED) {
        fprintf(stderr, "  relay: upstream joining fetch failed: %d\n", rc);
    }
    service_conn(&ctx->upstream, now);
}

static void pump_upstream(relay_ctx_t *ctx, uint64_t now)
{
    relay_conn_t *up = &ctx->upstream;
    if (!up->session) return;

    /* Retry any pending catalog Joining FETCH (request capacity may not have
     * been granted at SUBSCRIBE_OK time). */
    for (int i = 0; i < MAX_RELAY_TRACKS; i++)
        if (ctx->tracks[i].active)
            maybe_start_catalog_fetch(ctx, &ctx->tracks[i], now);

    moq_event_t ev;
    while (moq_session_poll_events(up->session, &ev, 1) == 1) {
        switch (ev.kind) {

        case MOQ_EVENT_SETUP_COMPLETE:
            ctx->up_established = true;
            fprintf(stderr, "  relay: upstream established\n");
            for (int i = 0; i < MAX_RELAY_TRACKS; i++)
                if (ctx->tracks[i].active && !ctx->tracks[i].up_subscribed)
                    subscribe_upstream_for(ctx, i, now);
            break;

        case MOQ_EVENT_SUBSCRIBE_OK: {
            relay_track_t *t = find_track_by_up_sub(ctx, ev.u.subscribe_ok.sub);
            if (t) {
                t->up_accepted = true;
                t->up_has_largest = ev.u.subscribe_ok.has_largest;
                fprintf(stderr, "  relay: upstream subscribe ok%s\n",
                    t->up_has_largest ? " (largest advertised)" : "");
                /* Retained catalog: pull the retained group via Joining FETCH. */
                maybe_start_catalog_fetch(ctx, t, now);
            }
            break;
        }

        case MOQ_EVENT_SUBSCRIBE_ERROR:
            fprintf(stderr, "  relay: upstream subscribe error\n");
            break;

        case MOQ_EVENT_OBJECT_RECEIVED: {
            moq_object_received_event_t *obj = &ev.u.object_received;
            size_t plen = obj->payload ? moq_rcbuf_len(obj->payload) : 0;
            size_t pplen = obj->properties ? moq_rcbuf_len(obj->properties) : 0;

            fprintf(stderr, "  relay: rx g=%llu o=%llu %zuB props=%zu\n",
                (unsigned long long)obj->group_id,
                (unsigned long long)obj->object_id, plen, pplen);

            relay_track_t *t = find_track_by_up_sub(ctx, obj->sub);
            if (t) forward_object(ctx, t, obj, now);
            break;
        }

        case MOQ_EVENT_FETCH_OK: {
            relay_track_t *t = find_track_by_up_fetch(ctx, ev.u.fetch_ok.fetch);
            if (t)
                fprintf(stderr, "  relay: upstream fetch ok (catalog)\n");
            break;
        }

        case MOQ_EVENT_FETCH_OBJECT: {
            moq_fetch_object_event_t *fo = &ev.u.fetch_object;
            relay_track_t *t = find_track_by_up_fetch(ctx, fo->fetch);
            if (t) {
                fprintf(stderr, "  relay: fetch obj g=%llu o=%llu\n",
                    (unsigned long long)fo->group_id,
                    (unsigned long long)fo->object_id);
                /* Republish the retained object(s) downstream as normal objects.
                 * A retained group may carry multiple objects, and a fetch event
                 * carries no end-of-group signal, so never mark end_of_group
                 * here -- the subgroup is closed (FIN) on FETCH_COMPLETE. */
                forward_one(ctx, t, fo->group_id, fo->object_id,
                            fo->publisher_priority, fo->payload, fo->properties,
                            false, now);
            }
            break;
        }

        case MOQ_EVENT_FETCH_COMPLETE: {
            relay_track_t *t =
                find_track_by_up_fetch(ctx, ev.u.fetch_complete.fetch);
            if (t) {
                t->fetch_done = true;
                if (t->sg_open) {
                    moq_session_close_subgroup(ctx->downstream.session,
                                               t->sg, now);
                    t->sg_open = false;
                }
                fprintf(stderr, "  relay: upstream fetch complete (catalog)\n");
                service_conn(&ctx->downstream, now);
            }
            break;
        }

        case MOQ_EVENT_FETCH_ERROR: {
            relay_track_t *t =
                find_track_by_up_fetch(ctx, ev.u.fetch_error.fetch);
            if (t)
                fprintf(stderr, "  relay: upstream fetch error: %u\n",
                    ev.u.fetch_error.error_code);
            break;
        }

        case MOQ_EVENT_SESSION_CLOSED:
            fprintf(stderr, "  relay: upstream closed\n");
            ctx->up_closed = true;
            break;

        default:
            break;
        }
        moq_event_cleanup(&ev);
    }
}

static void pump_downstream(relay_ctx_t *ctx, uint64_t now)
{
    relay_conn_t *down = &ctx->downstream;
    if (!down->session) return;

    moq_event_t ev;
    while (moq_session_poll_events(down->session, &ev, 1) == 1) {
        switch (ev.kind) {

        case MOQ_EVENT_SETUP_COMPLETE:
            fprintf(stderr, "  relay: downstream established\n");
            break;

        case MOQ_EVENT_SUBSCRIBE_REQUEST: {
            moq_subscribe_request_event_t *req = &ev.u.subscribe_request;
            moq_subscription_t sub_handle = req->sub;

            /* Validate before accept: check slot + name length. */
            relay_track_t *t = alloc_track(ctx);
            bool reject = false;
            int idx = -1;

            if (!t) {
                fprintf(stderr, "  relay: no track slots, rejecting\n");
                reject = true;
            } else if (req->track_name.len > RELAY_MAX_NAME_LEN) {
                fprintf(stderr, "  relay: track name too long, rejecting\n");
                reject = true;
            } else if (req->track_namespace.count > RELAY_MAX_NS_PARTS) {
                fprintf(stderr, "  relay: too many namespace parts, rejecting\n");
                reject = true;
            } else {
                idx = (int)(t - ctx->tracks);

                /* Copy namespace parts before advancing call. */
                size_t ns_off = 0;
                ctx->ns_count[idx] = req->track_namespace.count;
                for (size_t pi = 0; pi < req->track_namespace.count; pi++) {
                    size_t plen = req->track_namespace.parts[pi].len;
                    if (ns_off + plen > sizeof(ctx->ns_data[idx])) {
                        reject = true;
                        fprintf(stderr, "  relay: namespace too long, rejecting\n");
                        break;
                    }
                    memcpy(ctx->ns_data[idx] + ns_off,
                           req->track_namespace.parts[pi].data, plen);
                    ctx->ns_parts[idx][pi] = (moq_bytes_t){
                        ctx->ns_data[idx] + ns_off, plen };
                    ns_off += plen;
                }

                if (!reject) {
                    memcpy(ctx->name_buf[idx], req->track_name.data,
                           req->track_name.len);
                    ctx->name_len[idx] = req->track_name.len;
                }
            }

            if (reject) {
                moq_reject_subscribe_cfg_t rej;
                moq_reject_subscribe_cfg_init(&rej);
                rej.error_code = MOQ_REQUEST_ERROR_INTERNAL_ERROR;
                moq_session_reject_subscribe(
                    down->session, sub_handle, &rej, now);
                service_conn(down, now);
                break;
            }

            moq_accept_subscribe_cfg_t acc;
            moq_accept_subscribe_cfg_init(&acc);
            moq_result_t rc = moq_session_accept_subscribe(
                down->session, sub_handle, &acc, now);
            if (rc == MOQ_OK) {
                memset(t, 0, sizeof(*t));
                t->active = true;
                t->down_sub = sub_handle;
                t->down_accepted = true;
                t->is_catalog = relay_name_is_catalog(
                    ctx->name_buf[idx], ctx->name_len[idx]);

                fprintf(stderr, "  relay: downstream subscribe accepted \"%.*s\"%s\n",
                    (int)ctx->name_len[idx],
                    (const char *)ctx->name_buf[idx],
                    t->is_catalog ? " (retained: Joining FETCH)" : "");

                if (ctx->up_established)
                    subscribe_upstream_for(ctx, idx, now);
            }
            service_conn(down, now);
            break;
        }

        case MOQ_EVENT_UNSUBSCRIBED: {
            relay_track_t *t = find_track_by_down_sub(ctx, ev.u.unsubscribed.sub);
            if (t) {
                if (t->sg_open) {
                    moq_session_close_subgroup(
                        down->session, t->sg, now);
                    t->sg_open = false;
                }
                t->active = false;
                fprintf(stderr, "  relay: downstream unsubscribed\n");
            }
            break;
        }

        case MOQ_EVENT_SESSION_CLOSED:
            fprintf(stderr, "  relay: downstream closed\n");
            running = 0;
            break;

        default:
            break;
        }
        moq_event_cleanup(&ev);
    }
}

/* -- Picoquic callbacks ---------------------------------------------- */

static int server_callback_dispatch(picoquic_cnx_t *cnx,
    uint64_t stream_id, uint8_t *bytes, size_t length,
    picoquic_call_back_event_t event, void *callback_ctx,
    void *stream_ctx)
{
    return downstream_callback(cnx, stream_id, bytes, length,
                                event, callback_ctx, stream_ctx);
}

static int loop_callback(picoquic_quic_t *quic,
    picoquic_packet_loop_cb_enum cb_mode,
    void *callback_ctx, void *callback_arg)
{
    relay_ctx_t *ctx = (relay_ctx_t *)callback_ctx;
    (void)callback_arg;

    if (cb_mode != picoquic_packet_loop_after_receive &&
        cb_mode != picoquic_packet_loop_after_send)
        return 0;

    uint64_t now = picoquic_get_quic_time(quic);

    if (service_conn(&ctx->upstream, now) < 0 ||
        service_conn(&ctx->downstream, now) < 0) {
        running = 0;
        return PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP;
    }

    pump_upstream(ctx, now);
    pump_downstream(ctx, now);

    if (service_conn(&ctx->upstream, now) < 0 ||
        service_conn(&ctx->downstream, now) < 0) {
        running = 0;
        return PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP;
    }

    /* Adaptive warm-up: reset the pool's hot-path counters once first-touch
     * growth has ceased -- the allocator has made no new real malloc for
     * RELAY_POOL_WARMUP_STABLE_FORWARDS consecutive forwarded objects. Runs on
     * this single packet-loop thread, so it is ordered with all allocations. */
    if (ctx->pool && !ctx->pool_warm) {
        if (ctx->pool->real_allocs != ctx->pool_seen_allocs) {
            ctx->pool_seen_allocs = ctx->pool->real_allocs;
            ctx->pool_stable_fwd = ctx->forwarded;
        } else if (ctx->forwarded >= RELAY_POOL_WARMUP_MIN_FORWARDS &&
                   ctx->forwarded - ctx->pool_stable_fwd >=
                       RELAY_POOL_WARMUP_STABLE_FORWARDS) {
            relay_pool_reset_stats(ctx->pool);
            ctx->pool_warm = true;
            ctx->pool_warmup_excluded = ctx->forwarded;
        }
    }

    bool any_active = false;
    for (int i = 0; i < MAX_RELAY_TRACKS; i++)
        if (ctx->tracks[i].active) { any_active = true; break; }
    if (ctx->up_closed && !any_active)
        running = 0;

    if (!running) return PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP;
    return 0;
}

static void cleanup_conn(relay_conn_t *c)
{
    if (c->adapter) {
        moq_session_t *s = moq_pq_conn_session(c->adapter);
        moq_pq_conn_destroy(c->adapter);
        if (s) moq_session_destroy(s);
    }
}

int main(int argc, char *argv[])
{
    /* Separate options from positional args first: --pool is a flag and must
     * not be assigned to a positional slot (cert/key/pub-host/...). */
    bool pool_enabled = false;
    const char *pos[8];
    int npos = 0;
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--pool") == 0) { pool_enabled = true; continue; }
        if (npos < (int)(sizeof(pos) / sizeof(pos[0]))) pos[npos++] = argv[i];
    }
    {
        const char *env = getenv("MOQ_DEMO_RELAY_POOL");
        if (env && env[0] == '1') pool_enabled = true;
    }

    if (npos < 2) {
        fprintf(stderr,
            "Usage: %s <cert.pem> <key.pem> [pub-host] [pub-port] [listen-port] "
            "[--pool]\n", argv[0]);
        return 1;
    }

    const char *cert_path = pos[0];
    const char *key_path  = pos[1];
    const char *pub_host  = npos > 2 ? pos[2] : "localhost";
    int pub_port    = npos > 3 ? atoi(pos[3]) : 4443;
    int listen_port = npos > 4 ? atoi(pos[4]) : 4444;
    signal(SIGINT, sigint_handler);
    signal(SIGTERM, sigint_handler); /* clean shutdown on `kill`, so the pool
                                      * report still prints (demo harness) */

    relay_pool_t pool;
    memset(&pool, 0, sizeof(pool));

    relay_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    if (pool_enabled) {
        ctx.pool = &pool;
        ctx.alloc.ctx = &pool;
        ctx.alloc.alloc = relay_pool_alloc;
        ctx.alloc.realloc = relay_pool_realloc;
        ctx.alloc.free = relay_pool_free;
    } else {
        ctx.alloc = *moq_alloc_default();
    }

    uint64_t now = picoquic_current_time();

    picoquic_quic_t *quic = picoquic_create(
        8, cert_path, key_path, NULL, MOQ_PQ_ALPN_DEFAULT,
        server_callback_dispatch, &ctx,
        NULL, NULL, NULL, now, NULL, NULL, NULL, 0);
    if (!quic) { fprintf(stderr, "picoquic_create failed\n"); return 1; }
    picoquic_set_null_verifier(quic);

    /* Upstream: connect to publisher as MoQ client. */
    {
        struct sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_port = htons((uint16_t)pub_port);
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

        picoquic_cnx_t *cnx = picoquic_create_client_cnx(
            quic, (struct sockaddr *)&addr, now, 0,
            pub_host, MOQ_PQ_ALPN_DEFAULT, upstream_callback, &ctx);
        if (!cnx) { picoquic_free(quic); return 1; }

        moq_session_cfg_t scfg;
        moq_session_cfg_init_sized(&scfg, sizeof(scfg), &ctx.alloc, MOQ_PERSPECTIVE_CLIENT);
        scfg.send_request_capacity = true;
        scfg.initial_request_capacity = 64;

        if (moq_session_create(&scfg, now, &ctx.upstream.session) != MOQ_OK) {
            picoquic_free(quic);
            return 1;
        }

        moq_pq_conn_cfg_t acfg;
        moq_pq_conn_cfg_init(&acfg);
        acfg.session = ctx.upstream.session;
        acfg.cnx = cnx;
        acfg.alloc = &ctx.alloc;
        if (moq_pq_conn_create(&acfg, &ctx.upstream.adapter) != 0) {
            moq_session_destroy(ctx.upstream.session);
            picoquic_free(quic);
            return 1;
        }
        ctx.upstream.cnx = cnx;

        if (moq_session_start(ctx.upstream.session, now) != MOQ_OK) {
            cleanup_conn(&ctx.upstream);
            picoquic_free(quic);
            return 1;
        }
        service_conn(&ctx.upstream, now);
        /* picoquic_create_client_cnx already started the TLS handshake. */
    }

    fprintf(stderr, "Relay: upstream=%s:%d, listen=%d\n",
        pub_host, pub_port, listen_port);

    int rc = picoquic_packet_loop(quic, listen_port, 0, 0, 0, 0,
                                   loop_callback, &ctx);

    fprintf(stderr, "  relay: forwarded %llu objects\n",
        (unsigned long long)ctx.forwarded);

    /* Hot-path allocator report (captured before teardown frees sessions). */
    if (pool_enabled) {
        int64_t live_at_shutdown = pool.live_bytes;
        if (ctx.pool_warm) {
            fprintf(stderr,
                "  relay: pool ENABLED. libmoq allocator hot path after %llu "
                "warmup objects (adaptive; reset once allocation growth ceased):\n"
                "         real mallocs=%lld  real frees=%lld  peak delta=%lld B  "
                "live at shutdown=%lld B\n"
                "         (counts libmoq's moq_alloc_t only -- picoquic's own "
                "allocator and process-wide malloc are NOT included)\n",
                (unsigned long long)ctx.pool_warmup_excluded,
                (long long)pool.real_allocs, (long long)pool.real_frees,
                (long long)(pool.peak_bytes - pool.baseline_bytes),
                (long long)live_at_shutdown);
        } else {
            fprintf(stderr,
                "  relay: pool ENABLED but warm-up did not settle (%llu forwarded, "
                "allocator still growing); steady-state stats not printed\n",
                (unsigned long long)ctx.forwarded);
        }
    } else {
        fprintf(stderr,
            "  relay: pool disabled (default allocator); pass --pool or set "
            "MOQ_DEMO_RELAY_POOL=1 to measure the zero-alloc hot path\n");
    }

    cleanup_conn(&ctx.downstream);
    cleanup_conn(&ctx.upstream);
    picoquic_free(quic);

    if (pool_enabled) relay_pool_destroy(&pool);

    fprintf(stderr, "relay done (rc=%d)\n", rc);
    return rc;
}
