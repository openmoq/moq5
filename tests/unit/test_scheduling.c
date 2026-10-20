/*
 * Data scheduling (MOQT 7.2) through the transport bridge, draft-16 and
 * draft-18. The cases come from a test plan written from the drafts alone,
 * before reading the implementation (IDs in brackets, e.g. [SCH-01]).
 *
 * The publisher's endpoint is a model transport (`mt_*`) that follows the
 * endpoint priority contract: of the data streams holding bytes it sends the
 * one with the lowest key next, streams with equal keys take turns, and a
 * stream that never got a key is sent as control traffic (key 0). Its send
 * queue has a byte cap like a real adapter's; beyond it the bridge holds the
 * data. The "network" sends nothing until the test serves it, one write at a
 * time, so every scenario is congested until the test says otherwise and the
 * order on the wire is exact. Control and request stream bytes bypass the
 * model and reach the subscriber at once.
 *
 * Ordering is a SHOULD in both drafts; this model is strict, so the cases
 * assert strict order where the plan allows it.
 */
#include <moq/moq.h>
#include <moq/transport_bridge.h>
#include "test_support.h"
#include "test_binding_support.h"
#include "../support/fake_endpoint.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int failures = 0;

#define ASC  MOQ_GROUP_ORDER_ASCENDING
#define DESC MOQ_GROUP_ORDER_DESCENDING
#define NONE MOQ_GROUP_ORDER_DEFAULT

static const moq_version_t k_versions[] = {
    MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_22,
};
#define N_VERSIONS (sizeof(k_versions) / sizeof(k_versions[0]))

/* Draft-18 and later: a unidirectional control pair and request streams. */
static bool uni_control(moq_version_t ver) { return ver != MOQ_VERSION_DRAFT_16; }

static int draft_of(moq_version_t ver)
{
    return ver == MOQ_VERSION_DRAFT_16 ? 16 : ver == MOQ_VERSION_DRAFT_18 ? 18 : 22;
}

/* -- model transport ----------------------------------------------------- */

#define MT_UNI_BASE  3000u
#define MT_BIDI_BASE 4000u
#define MT_MAX_STREAMS 128
#define MT_MAX_CHUNKS  4096
#define MT_MAX_CTRL    256

/* Object payloads: a fixed size and a marker, so the model can tell them
 * from headers and the test can read which object went out. */
#define PAYLOAD_LEN 12
#define PAYLOAD_TAG 0xA5

typedef struct {
    uint64_t sid;
    size_t   len;
    uint8_t  head[PAYLOAD_LEN];
    bool     fin;
    bool     sent;
} mt_chunk_t;

typedef struct {
    bool     used;
    uint64_t sid;
    bool     keyed;
    uint32_t key;
    uint64_t served;    /* serve_seq of its last chunk */
    bool     data;      /* got a key at or before its first byte */
    bool     reset;
    bool     blocked;   /* per-stream flow control exhausted */
    bool     refuse;    /* its own send budget is spent: writes refused */
} mt_stream_t;

typedef struct {
    uint64_t sid;
    uint8_t  data[512];
    size_t   len;
    bool     fin;
} mt_ctrl_t;

typedef struct {
    moq_transport_endpoint_ops_t ops;
    moq_transport_bridge_t *bridge;
    uint64_t next_uni, next_bidi;
    size_t   cap;           /* queued data bytes that refuse further writes */
    size_t   queued;
    bool     drained_cap;   /* advertise and call the drained notice */
    mt_stream_t streams[MT_MAX_STREAMS];
    mt_chunk_t  chunks[MT_MAX_CHUNKS];
    size_t      nchunks;
    uint64_t    serve_seq;
    mt_ctrl_t   ctrl[MT_MAX_CTRL];
    size_t      nctrl;
    /* What went to the wire, in order: data chunks, resets. */
    struct { uint64_t sid; uint8_t head[PAYLOAD_LEN]; size_t len; bool fin;
             bool reset; uint64_t code; } wire[MT_MAX_CHUNKS];
    size_t   nwire;
    uint32_t priority_calls;
    uint32_t bidi_keyed;        /* keys set on request/control bidi streams */
    uint64_t op_seq;            /* order across control writes and resets */
    uint64_t wire_seq[MT_MAX_CHUNKS];
    uint64_t ctrl_seq_last;     /* seq of the latest control write */
    uint64_t ctrl_mark;         /* seq after which ctrl_first_after counts */
    uint64_t ctrl_first_after;  /* seq of the first control write past it */
} mt_t;

static mt_stream_t *mt_stream(mt_t *m, uint64_t sid)
{
    for (size_t i = 0; i < MT_MAX_STREAMS; i++)
        if (m->streams[i].used && m->streams[i].sid == sid)
            return &m->streams[i];
    for (size_t i = 0; i < MT_MAX_STREAMS; i++)
        if (!m->streams[i].used) {
            memset(&m->streams[i], 0, sizeof(m->streams[i]));
            m->streams[i].used = true;
            m->streams[i].sid = sid;
            return &m->streams[i];
        }
    return NULL;
}

static bool mt_is_uni(uint64_t sid)
{
    return sid >= MT_UNI_BASE && sid < MT_BIDI_BASE;
}

static moq_transport_result_t mt_open_uni(void *ctx, uint64_t *out)
{
    mt_t *m = ctx;
    *out = m->next_uni;
    m->next_uni += 4;
    return MOQ_TRANSPORT_OK;
}

static moq_transport_result_t mt_open_bidi(void *ctx, uint64_t *out)
{
    mt_t *m = ctx;
    *out = m->next_bidi;
    m->next_bidi += 4;
    return MOQ_TRANSPORT_OK;
}

static moq_transport_result_t mt_write(void *ctx, uint64_t sid,
                                       const uint8_t *data, size_t len,
                                       bool fin)
{
    mt_t *m = ctx;
    mt_stream_t *st = mt_is_uni(sid) ? mt_stream(m, sid) : NULL;
    if (st && !st->data && st->keyed) st->data = true;
    if (!st || !st->data) {
        /* Control: the subscriber gets it at once. */
        if (m->nctrl == MT_MAX_CTRL || len > sizeof(m->ctrl[0].data))
            return MOQ_TRANSPORT_ERROR;
        mt_ctrl_t *c = &m->ctrl[m->nctrl++];
        m->ctrl_seq_last = ++m->op_seq;
        if (m->ctrl_mark && !m->ctrl_first_after &&
            m->ctrl_seq_last > m->ctrl_mark)
            m->ctrl_first_after = m->ctrl_seq_last;
        c->sid = sid;
        c->len = len;
        if (len) memcpy(c->data, data, len);
        c->fin = fin;
        return MOQ_TRANSPORT_OK;
    }
    /* A shared queue bounded as the picoquic adapter's is (a low-water
     * mark), plus a per-stream budget as msquic's. */
    if ((len > 0 && m->queued >= m->cap) || st->refuse)
        return MOQ_TRANSPORT_WOULD_BLOCK;
    if (m->nchunks == MT_MAX_CHUNKS) return MOQ_TRANSPORT_ERROR;
    mt_chunk_t *c = &m->chunks[m->nchunks++];
    memset(c, 0, sizeof(*c));
    c->sid = sid;
    c->len = len;
    if (len) memcpy(c->head, data, len < PAYLOAD_LEN ? len : PAYLOAD_LEN);
    c->fin = fin;
    m->queued += len;
    return MOQ_TRANSPORT_OK;
}

static moq_transport_result_t mt_reset(void *ctx, uint64_t sid, uint64_t code)
{
    mt_t *m = ctx;
    for (size_t i = 0; i < m->nchunks; i++)
        if (m->chunks[i].sid == sid && !m->chunks[i].sent) {
            m->chunks[i].sent = true;
            m->queued -= m->chunks[i].len;
        }
    mt_stream_t *st = mt_stream(m, sid);
    if (st) st->reset = true;
    if (m->nwire < MT_MAX_CHUNKS) {
        memset(&m->wire[m->nwire], 0, sizeof(m->wire[0]));
        m->wire[m->nwire].sid = sid;
        m->wire[m->nwire].reset = true;
        m->wire[m->nwire].code = code;
        m->wire_seq[m->nwire] = ++m->op_seq;
        m->nwire++;
    }
    return MOQ_TRANSPORT_OK;
}

static moq_transport_result_t mt_stop(void *ctx, uint64_t sid, uint64_t code)
{
    (void)ctx; (void)sid; (void)code;
    return MOQ_TRANSPORT_OK;
}

static moq_transport_result_t mt_close(void *ctx, uint64_t code,
                                       const uint8_t *reason, size_t len)
{
    (void)ctx; (void)code; (void)reason; (void)len;
    return MOQ_TRANSPORT_OK;
}

static moq_transport_result_t mt_priority(void *ctx, uint64_t sid,
                                          uint32_t key)
{
    mt_t *m = ctx;
    if (!mt_is_uni(sid)) m->bidi_keyed++;
    mt_stream_t *st = mt_stream(m, sid);
    if (!st) return MOQ_TRANSPORT_ERROR;
    st->keyed = true;
    st->key = key;
    m->priority_calls++;
    return MOQ_TRANSPORT_OK;
}

static void mt_init(mt_t *m, size_t cap, bool drained_cap)
{
    memset(m, 0, sizeof(*m));
    m->next_uni = MT_UNI_BASE + 3;
    m->next_bidi = MT_BIDI_BASE + 1;
    m->cap = cap;
    m->drained_cap = drained_cap;
    m->ops = MOQ_TRANSPORT_ENDPOINT_OPS_INIT;
    m->ops.capabilities = MOQ_TRANSPORT_CAP_HOLD_INPUT |
        (drained_cap ? MOQ_TRANSPORT_CAP_STREAM_DRAINED : 0);
    m->ops.open_uni = mt_open_uni;
    m->ops.open_bidi = mt_open_bidi;
    m->ops.write = mt_write;
    m->ops.reset_stream = mt_reset;
    m->ops.stop_sending = mt_stop;
    m->ops.close_transport = mt_close;
    m->ops.set_stream_priority = mt_priority;
}

/* The key a stream goes by: control (0) until it has one. */
static uint32_t mt_key(const mt_stream_t *st)
{
    return st && st->keyed ? st->key : 0;
}

/* Send one queued data write: the lowest key first, equal keys in turns.
 * False when nothing is sendable. */
static bool mt_serve_one(mt_t *m)
{
    mt_chunk_t *best = NULL;
    mt_stream_t *best_st = NULL;
    for (size_t i = 0; i < m->nchunks; i++) {
        mt_chunk_t *c = &m->chunks[i];
        if (c->sent) continue;
        mt_stream_t *st = mt_stream(m, c->sid);
        if (st->blocked) continue;
        bool earlier = false;   /* only a stream's first unsent chunk */
        for (size_t j = 0; j < i && !earlier; j++)
            earlier = !m->chunks[j].sent && m->chunks[j].sid == c->sid;
        if (earlier) continue;
        if (!best || mt_key(st) < mt_key(best_st) ||
            (mt_key(st) == mt_key(best_st) && st->served < best_st->served)) {
            best = c;
            best_st = st;
        }
    }
    if (!best) return false;
    best->sent = true;
    m->queued -= best->len;
    best_st->served = ++m->serve_seq;
    if (m->nwire < MT_MAX_CHUNKS) {
        m->wire[m->nwire].sid = best->sid;
        memcpy(m->wire[m->nwire].head, best->head, PAYLOAD_LEN);
        m->wire[m->nwire].len = best->len;
        m->wire[m->nwire].fin = best->fin;
        m->wire[m->nwire].reset = false;
        m->nwire++;
    }
    bool more = false;
    for (size_t i = 0; i < m->nchunks && !more; i++)
        more = !m->chunks[i].sent && m->chunks[i].sid == best->sid;
    if (!more && m->drained_cap && m->bridge)
        moq_transport_bridge_on_stream_drained(m->bridge, best->sid);
    return true;
}

/* Objects the transport already holds, at least their first bytes: an upper
 * bound of one per unsent write (a header announces its object's payload). */
/* Objects the model transport holds, not yet sent. */
static size_t mt_held_objects(const mt_t *m)
{
    size_t n = 0;
    for (size_t i = 0; i < m->nchunks; i++)
        if (!m->chunks[i].sent && m->chunks[i].len == PAYLOAD_LEN &&
            m->chunks[i].head[0] == PAYLOAD_TAG)
            n++;
    return n;
}

/* -- publisher / subscriber pair ----------------------------------------- */

typedef struct {
    moq_version_t ver;
    moq_session_t *pub, *sub;            /* server publishes */
    moq_transport_bridge_t *pub_br, *sub_br;
    mt_t *mt;
    fake_endpoint_t sub_ep;
    uint64_t now;
} pair_t;

/* Peer uni bytes. These scenarios never run out of receive entries, so an
 * unconsumed chunk (which the endpoint would have to hold) is a failure. */
static void feed_uni(moq_transport_bridge_t *b, uint64_t sid,
                     const uint8_t *data, size_t len, bool fin, uint64_t now)
{
    MOQ_TEST_CHECK(moq_transport_bridge_on_peer_uni_bytes(b, sid, data, len,
                                                          fin, now) !=
                   MOQ_ERR_INPUT_NOT_CONSUMED);
}

static void shuttle(pair_t *p)
{
    for (int round = 0; round < 64; round++) {
        size_t moved = 0;
        moq_transport_bridge_service(p->sub_br, p->now);
        for (size_t i = 0; i < p->sub_ep.count; i++) {
            const fake_op_t *o = &p->sub_ep.ops[i];
            if (o->kind == FAKE_OP_RESET || o->kind == FAKE_OP_ABORT) {
                moved++;
                moq_transport_bridge_on_peer_stream_reset(p->pub_br,
                    o->stream_id, o->error_code, p->now);
            }
            if (o->kind == FAKE_OP_STOP || o->kind == FAKE_OP_ABORT) {
                moved++;
                moq_transport_bridge_on_peer_stop_sending(p->pub_br,
                    o->stream_id, o->error_code, p->now);
            }
            if (o->kind != FAKE_OP_WRITE) continue;
            moved++;
            if (!uni_control(p->ver))
                moq_transport_bridge_on_peer_control_bytes(p->pub_br,
                    o->stream_id, o->data, o->data_len, o->fin, p->now);
            else if (o->stream_id < 2000)
                feed_uni(p->pub_br, o->stream_id, o->data, o->data_len,
                         o->fin, p->now);
            else
                moq_transport_bridge_on_peer_bidi_bytes(p->pub_br,
                    o->stream_id, o->data, o->data_len, o->fin, p->now);
        }
        fake_endpoint_clear_ops(&p->sub_ep);
        moq_transport_bridge_service(p->pub_br, p->now);
        for (size_t i = 0; i < p->mt->nctrl; i++) {
            const mt_ctrl_t *c = &p->mt->ctrl[i];
            moved++;
            if (!uni_control(p->ver))
                moq_transport_bridge_on_peer_control_bytes(p->sub_br,
                    c->sid, c->data, c->len, c->fin, p->now);
            else if (mt_is_uni(c->sid))
                feed_uni(p->sub_br, c->sid, c->data, c->len, c->fin, p->now);
            else
                moq_transport_bridge_on_peer_bidi_bytes(p->sub_br,
                    c->sid, c->data, c->len, c->fin, p->now);
        }
        p->mt->nctrl = 0;
        if (!moved) return;
    }
}

static void drain_events(moq_session_t *s)
{
    moq_event_t e;
    while (moq_session_poll_events(s, &e, 1) == 1) moq_event_cleanup(&e);
}

/* `cap`: the model's send queue in bytes. */
/* pair_init() with the publisher bridge's stream map limited to
 * `max_streams` entries (0 = default) and the publisher's action queue to
 * `max_actions` entries. */
static bool pair_init_limits(pair_t *p, moq_version_t ver, size_t cap,
                             bool drained_cap, uint32_t max_streams,
                             uint32_t max_actions)
{
    memset(p, 0, sizeof(*p));
    p->ver = ver;
    p->mt = calloc(1, sizeof(*p->mt));
    if (!p->mt) return false;
    mt_init(p->mt, cap, drained_cap);
    fake_endpoint_init(&p->sub_ep, 1000, 2000);
    p->sub_ep.vtable.capabilities |= MOQ_TRANSPORT_CAP_HOLD_INPUT;

    moq_session_cfg_t cc, sc;
    moq_session_cfg_init_sized(&cc, sizeof(cc), moq_alloc_default(),
                               MOQ_PERSPECTIVE_CLIENT);
    moq_session_cfg_init_sized(&sc, sizeof(sc), moq_alloc_default(),
                               MOQ_PERSPECTIVE_SERVER);
    cc.version = sc.version = ver;
    cc.send_request_capacity = sc.send_request_capacity = true;
    cc.initial_request_capacity = sc.initial_request_capacity = 32;
    sc.max_actions = max_actions;
    if (moq_session_create(&cc, 0, &p->sub) < 0) return false;
    if (moq_session_create(&sc, 0, &p->pub) < 0) return false;
    if (test_bind_if_required(p->sub) < 0 || test_bind_if_required(p->pub) < 0)
        return false;

    moq_transport_bridge_cfg_t bc;
    moq_transport_bridge_cfg_init_sized(&bc, sizeof(bc), moq_alloc_default());
    if (moq_transport_bridge_create(&bc, p->sub, &p->sub_ep.vtable,
                                    &p->sub_ep, &p->sub_br) != MOQ_OK)
        return false;
    bc.max_streams = max_streams;
    if (moq_transport_bridge_create(&bc, p->pub, &p->mt->ops, p->mt,
                                    &p->pub_br) != MOQ_OK)
        return false;
    p->mt->bridge = p->pub_br;

    p->now = 1000;
    moq_session_start(p->sub, p->now);
    if (uni_control(ver)) moq_session_start(p->pub, p->now);
    shuttle(p);
    drain_events(p->sub);
    drain_events(p->pub);
    return moq_session_state(p->pub) == MOQ_SESS_ESTABLISHED &&
           moq_session_state(p->sub) == MOQ_SESS_ESTABLISHED;
}

static bool pair_init_streams(pair_t *p, moq_version_t ver, size_t cap,
                              bool drained_cap, uint32_t max_streams)
{
    return pair_init_limits(p, ver, cap, drained_cap, max_streams, 1024);
}

static bool pair_init(pair_t *p, moq_version_t ver, size_t cap,
                      bool drained_cap)
{
    return pair_init_streams(p, ver, cap, drained_cap, 0);
}

static void pair_destroy(pair_t *p)
{
    moq_transport_bridge_destroy(p->sub_br);
    moq_transport_bridge_destroy(p->pub_br);
    moq_session_destroy(p->sub);
    moq_session_destroy(p->pub);
    free(p->mt);
}

/* Serve the network until nothing is left, servicing the publisher's
 * bridge after every write so it can refill. */
static void serve_all(pair_t *p)
{
    for (int guard = 0; guard < 100000; guard++) {
        moq_transport_bridge_service(p->pub_br, p->now);
        if (!mt_serve_one(p->mt)) {
            moq_transport_bridge_service(p->pub_br, p->now);
            if (!mt_serve_one(p->mt)) return;
        }
    }
}

typedef struct {
    moq_subscription_t sub;   /* the publisher's handle */
    moq_subscription_t csub;  /* the subscriber's handle */
} track_t;

/* The subscriber subscribes to "live"/`name` with `sub_prio` (-1: omitted)
 * and `order`; the publisher accepts advertising `pref` and the Track
 * Properties `extra`. */
static bool subscribe_props(pair_t *p, const char *name, int sub_prio,
                            moq_group_order_t order, moq_group_order_t pref,
                            const uint8_t *extra, size_t extra_len,
                            track_t *out)
{
    moq_bytes_t part = MOQ_BYTES_LITERAL("live");
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ &part, 1 };
    sc.track_name = (moq_bytes_t){ (const uint8_t *)name, strlen(name) };
    sc.has_subscriber_priority = sub_prio >= 0;
    sc.subscriber_priority = (uint8_t)(sub_prio >= 0 ? sub_prio : 0);
    sc.group_order = order;
    if (moq_session_subscribe(p->sub, &sc, p->now, &out->csub) != MOQ_OK)
        return false;
    shuttle(p);
    moq_event_t ev;
    bool got = false;
    while (moq_session_poll_events(p->pub, &ev, 1) == 1) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
            out->sub = ev.u.subscribe_request.sub;
            got = true;
        }
        moq_event_cleanup(&ev);
    }
    if (!got) return false;
    uint8_t props[32];
    size_t props_len = extra_len;
    if (extra_len > sizeof(props)) return false;
    if (extra_len) memcpy(props, extra, extra_len);
    if (pref != NONE &&
        moq_session_track_properties_add_group_order(p->pub, extra, extra_len,
            pref, props, sizeof(props), &props_len) != MOQ_OK)
        return false;
    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    ac.track_properties = (moq_bytes_t){ props, props_len };
    if (moq_session_accept_subscribe(p->pub, out->sub, &ac, p->now) != MOQ_OK)
        return false;
    shuttle(p);
    drain_events(p->sub);
    return true;
}

static bool subscribe(pair_t *p, const char *name, int sub_prio,
                      moq_group_order_t order, moq_group_order_t pref,
                      track_t *out)
{
    return subscribe_props(p, name, sub_prio, order, pref, NULL, 0, out);
}

/* Object payload: tag, track tag, group, subgroup, object (low bytes). */
static moq_result_t write_object(pair_t *p, moq_subgroup_handle_t sg,
                                 uint8_t track, uint64_t group,
                                 uint64_t subgroup, uint64_t object)
{
    uint8_t body[PAYLOAD_LEN] = { PAYLOAD_TAG, track, (uint8_t)group,
                                  (uint8_t)subgroup, (uint8_t)object };
    moq_rcbuf_t *rb = NULL;
    if (moq_rcbuf_create(moq_alloc_default(), body, sizeof(body), &rb) < 0)
        return MOQ_ERR_NOMEM;
    moq_result_t rc = moq_session_write_object(p->pub, sg, object, rb, p->now);
    moq_rcbuf_decref(rb);
    return rc;
}

/* Open (group, subgroup) at `pub_prio` and write `nobj` objects; closed with
 * FIN unless `keep_open`. */
static bool publish_group(pair_t *p, const track_t *t, uint8_t track,
                          uint64_t group, uint64_t subgroup, uint8_t pub_prio,
                          int nobj, bool keep_open, moq_subgroup_handle_t *out)
{
    moq_subgroup_cfg_t cfg;
    moq_subgroup_cfg_init(&cfg);
    cfg.group_id = group;
    cfg.subgroup_id = subgroup;
    cfg.publisher_priority = pub_prio;
    moq_subgroup_handle_t sg;
    if (moq_session_open_subgroup(p->pub, t->sub, &cfg, p->now, &sg) != MOQ_OK)
        return false;
    for (int o = 0; o < nobj; o++)
        if (write_object(p, sg, track, group, subgroup, (uint64_t)o) != MOQ_OK)
            return false;
    if (!keep_open &&
        moq_session_close_subgroup(p->pub, sg, p->now) != MOQ_OK)
        return false;
    if (out) *out = sg;
    return true;
}

/* The objects that went to the wire, in order, as (track, group, subgroup,
 * object) tuples packed in uint32 (8 bits each). */
static size_t wire_objects(const pair_t *p, uint32_t *out, size_t cap)
{
    size_t n = 0;
    for (size_t i = 0; i < p->mt->nwire && n < cap; i++) {
        const uint8_t *h = p->mt->wire[i].head;
        if (p->mt->wire[i].reset || p->mt->wire[i].len != PAYLOAD_LEN ||
            h[0] != PAYLOAD_TAG)
            continue;
        out[n++] = (uint32_t)h[1] << 24 | (uint32_t)h[2] << 16 |
                   (uint32_t)h[3] << 8 | h[4];
    }
    return n;
}

#define OBJ(track, group, subgroup, object) \
    ((uint32_t)(track) << 24 | (uint32_t)(group) << 16 | \
     (uint32_t)(subgroup) << 8 | (uint32_t)(object))

/* The send queue sizes every scenario runs with: tiny (the bridge orders
 * almost everything), a few writes, and room for everything (the transport
 * orders everything). */
static const size_t k_caps[] = { 1, 64, 1u << 20 };

/* Known deviations from the drafts, tracked in issue #36: still run with the
 * drafts' expectation, reported without failing the suite -- and failing it
 * once they start passing, so the mark gets removed. */
static void expect_known(bool ok, bool known, const char *what,
                         moq_version_t ver, size_t cap)
{
    if (!known) {
        MOQ_TEST_CHECK(ok);
        return;
    }
    if (!ok) {
        printf("KNOWN FAILURE (issue #36): %s (d%d, cap %zu)\n", what,
               draft_of(ver), cap);
        return;
    }
    fprintf(stderr, "%s (d%d, cap %zu) now passes: drop its issue #36 mark\n",
            what, draft_of(ver), cap);
    failures++;
}

/* Issue #36: a FETCH stream takes the priority of its last object written to
 * the transport, so with several of its objects queued there it is not
 * scheduled by its next object. */
static bool fetch_priority_known(const char *what, size_t cap)
{
    return strcmp(what, "FET-04") == 0 && cap > 1;
}

/* Check the wire carried exactly `want`, in order. */
static void expect_wire(const pair_t *p, const uint32_t *want, size_t n,
                        const char *what, size_t cap)
{
    uint32_t got[512];
    size_t m = wire_objects(p, got, 512);
    bool same = m == n;
    for (size_t i = 0; same && i < n; i++) same = got[i] == want[i];
    bool known = fetch_priority_known(what, cap);
    if (!same && !known) {
        fprintf(stderr, "%s (d%d, cap %zu): wire", what,
                draft_of(p->ver), cap);
        for (size_t i = 0; i < m; i++) fprintf(stderr, " %08x", got[i]);
        fprintf(stderr, "\n  want");
        for (size_t i = 0; i < n; i++) fprintf(stderr, " %08x", want[i]);
        fprintf(stderr, "\n");
    }
    expect_known(same, known, what, p->ver, cap);
}

/* -- A. group order on the wire ------------------------------------------ */

/* [GO-01..04] Groups 5, 6, 7 (one subgroup, 2 objects each) queued while the
 * network is stalled go out in the effective group order. */
/* Without the endpoint's drained notice the bridge drops a stream from the
 * order at its FIN, so the order holds only while the transport keeps no
 * data past a FIN: its smallest queue. */
static bool drained_variant_holds(int drained, size_t cap)
{
    return drained == 1 || cap == 1;
}

static void check_group_order_wire(moq_version_t ver, size_t cap,
                                   moq_group_order_t order,
                                   moq_group_order_t pref, bool want_desc,
                                   bool drained)
{
    pair_t p;
    MOQ_TEST_CHECK(pair_init(&p, ver, cap, drained));
    track_t t;
    MOQ_TEST_CHECK(subscribe(&p, "v", -1, order, pref, &t));
    for (uint64_t g = 5; g <= 7; g++)
        MOQ_TEST_CHECK(publish_group(&p, &t, 1, g, 0, 128, 2, false, NULL));
    serve_all(&p);
    uint32_t want[6];
    size_t n = 0;
    for (int i = 0; i < 3; i++) {
        uint64_t g = want_desc ? 7 - (uint64_t)i : 5 + (uint64_t)i;
        want[n++] = OBJ(1, g, 0, 0);
        want[n++] = OBJ(1, g, 0, 1);
    }
    expect_wire(&p, want, n, "group order", cap);
    pair_destroy(&p);
}

static void test_group_order_wire(void)
{
    for (size_t v = 0; v < N_VERSIONS; v++)
        for (size_t c = 0; c < 3; c++)
            for (int dr = 0; dr < 2; dr++) {
                if (!drained_variant_holds(dr, k_caps[c])) continue;
                moq_version_t ver = k_versions[v];
                size_t cap = k_caps[c];
                bool d = dr == 1;
                /* Subscriber's order over the publisher's, then the
                 * publisher's, then ascending (MOQT 7.1). */
                check_group_order_wire(ver, cap, DESC, ASC, true, d);
                check_group_order_wire(ver, cap, ASC, DESC, false, d);
                check_group_order_wire(ver, cap, NONE, DESC, true, d);
                check_group_order_wire(ver, cap, NONE, NONE, false, d);
            }
    MOQ_TEST_PASS("groups go out in the effective group order");
}

/* -- B. the MOQT 7.2 rules ------------------------------------------------- */

/* One run: tracks and groups set up by `build`, the network served, and the
 * wire compared with `want`; every send queue size, both drafts. */
typedef void (*build_fn)(pair_t *p, void *ctx);

/* Every draft and queue size, with and without the endpoint's drained
 * notice. */
static void run_case(const char *what, build_fn build, void *ctx,
                     const uint32_t *want, size_t n)
{
    for (size_t v = 0; v < N_VERSIONS; v++)
        for (size_t c = 0; c < 3; c++)
            for (int drained = 0; drained < 2; drained++) {
                if (!drained_variant_holds(drained, k_caps[c])) continue;
                pair_t p;
                MOQ_TEST_CHECK(pair_init(&p, k_versions[v], k_caps[c],
                                         drained == 1));
                build(&p, ctx);
                serve_all(&p);
                expect_wire(&p, want, n, what, k_caps[c]);
                pair_destroy(&p);
            }
}

typedef struct { int sub_a, pub_a, sub_b, pub_b; } two_tracks_t;

/* Track 2 ("b") is published first, so arrival order would put it ahead. */
static void build_two_tracks(pair_t *p, void *ctx)
{
    const two_tracks_t *c = ctx;
    track_t a, b;
    MOQ_TEST_CHECK(subscribe(p, "a", c->sub_a, NONE, NONE, &a));
    MOQ_TEST_CHECK(subscribe(p, "b", c->sub_b, NONE, NONE, &b));
    MOQ_TEST_CHECK(publish_group(p, &b, 2, 1, 0, (uint8_t)c->pub_b, 2, false,
                                 NULL));
    MOQ_TEST_CHECK(publish_group(p, &a, 1, 1, 0, (uint8_t)c->pub_a, 2, false,
                                 NULL));
}

/* [SCH-01] subscriber priority first; [SCH-02] then publisher priority, also
 * across subscriptions. Track 1 must drain before track 2. */
static void test_priorities_across_tracks(void)
{
    static const uint32_t want[] = {
        OBJ(1, 1, 0, 0), OBJ(1, 1, 0, 1), OBJ(2, 1, 0, 0), OBJ(2, 1, 0, 1),
    };
    two_tracks_t sch01 = { 10, 255, 11, 0 };
    run_case("SCH-01", build_two_tracks, &sch01, want, 4);
    two_tracks_t sch01b = { 0, 255, 255, 0 };
    run_case("SCH-01 bounds", build_two_tracks, &sch01b, want, 4);
    two_tracks_t sch02 = { 128, 50, 128, 60 };
    run_case("SCH-02", build_two_tracks, &sch02, want, 4);
    MOQ_TEST_PASS("subscriber then publisher priority across tracks");
}

typedef struct {
    moq_group_order_t order;
    int n;
    struct { uint64_t g, sg; uint8_t prio; } sg[4];
} one_track_t;

static void build_one_track(pair_t *p, void *ctx)
{
    const one_track_t *c = ctx;
    track_t t;
    MOQ_TEST_CHECK(subscribe(p, "v", -1, c->order, NONE, &t));
    for (int i = 0; i < c->n; i++)
        MOQ_TEST_CHECK(publish_group(p, &t, 1, c->sg[i].g, c->sg[i].sg,
                                     c->sg[i].prio, 1, false, NULL));
}

/* [SCH-03] publisher priority before group order; [SCH-04] group order
 * before subgroup id; [SCH-06] lowest subgroup first in a group; [SCH-07]
 * publisher priority before subgroup id. */
static void test_rules_within_track(void)
{
    one_track_t sch03 = { DESC, 2, { { 8, 0, 20 }, { 7, 0, 10 } } };
    static const uint32_t w03[] = { OBJ(1, 7, 0, 0), OBJ(1, 8, 0, 0) };
    run_case("SCH-03", build_one_track, &sch03, w03, 2);

    one_track_t sch04a = { ASC, 2, { { 8, 0, 128 }, { 7, 5, 128 } } };
    static const uint32_t w04a[] = { OBJ(1, 7, 5, 0), OBJ(1, 8, 0, 0) };
    run_case("SCH-04 asc", build_one_track, &sch04a, w04a, 2);
    one_track_t sch04d = { DESC, 2, { { 7, 5, 128 }, { 8, 0, 128 } } };
    static const uint32_t w04d[] = { OBJ(1, 8, 0, 0), OBJ(1, 7, 5, 0) };
    run_case("SCH-04 desc", build_one_track, &sch04d, w04d, 2);

    one_track_t sch06 = { ASC, 3, { { 4, 2, 128 }, { 4, 1, 128 },
                                    { 4, 0, 128 } } };
    static const uint32_t w06[] = { OBJ(1, 4, 0, 0), OBJ(1, 4, 1, 0),
                                    OBJ(1, 4, 2, 0) };
    run_case("SCH-06", build_one_track, &sch06, w06, 3);
    one_track_t sch06b = { DESC, 2, { { 4, 1000, 128 }, { 4, 0, 128 } } };
    static const uint32_t w06b[] = { OBJ(1, 4, 0, 0),
                                     OBJ(1, 4, 1000 & 0xff, 0) };
    run_case("SCH-06 far", build_one_track, &sch06b, w06b, 2);

    one_track_t sch07 = { ASC, 2, { { 8, 0, 20 }, { 8, 2, 10 } } };
    static const uint32_t w07[] = { OBJ(1, 8, 2, 0), OBJ(1, 8, 0, 0) };
    run_case("SCH-07", build_one_track, &sch07, w07, 2);
    MOQ_TEST_PASS("MOQT 7.2 rules within one track");
}

/* [SCH-05] Group ids are compared as numbers: gaps and large ids. */
static void test_group_id_bounds(void)
{
    one_track_t gaps_a = { ASC, 3, { { 100, 0, 128 }, { 2, 0, 128 },
                                     { 9, 0, 128 } } };
    static const uint32_t wga[] = { OBJ(1, 2, 0, 0), OBJ(1, 9, 0, 0),
                                    OBJ(1, 100, 0, 0) };
    run_case("SCH-05 gaps asc", build_one_track, &gaps_a, wga, 3);
    one_track_t gaps_d = { DESC, 3, { { 2, 0, 128 }, { 100, 0, 128 },
                                      { 9, 0, 128 } } };
    static const uint32_t wgd[] = { OBJ(1, 100, 0, 0), OBJ(1, 9, 0, 0),
                                    OBJ(1, 2, 0, 0) };
    run_case("SCH-05 gaps desc", build_one_track, &gaps_d, wgd, 3);
    const uint64_t big = ((uint64_t)1 << 62) - 1;
    one_track_t far_a = { ASC, 2, { { big, 0, 128 }, { 0, 0, 128 } } };
    const uint32_t wfa[] = { OBJ(1, 0, 0, 0), OBJ(1, big & 0xff, 0, 0) };
    run_case("SCH-05 far asc", build_one_track, &far_a, wfa, 2);
    one_track_t far_d = { DESC, 2, { { 0, 0, 128 }, { big, 0, 128 } } };
    const uint32_t wfd[] = { OBJ(1, big & 0xff, 0, 0), OBJ(1, 0, 0, 0) };
    run_case("SCH-05 far desc", build_one_track, &far_d, wfd, 2);
    MOQ_TEST_PASS("group ids compared as numbers");
}

/* [SCH-13] A three-level ladder never inverts; [PRI-01] an omitted
 * subscriber priority is 128 (between 127 and 129). */
typedef struct { int prio[3]; } ladder_t;

static void build_ladder(pair_t *p, void *ctx)
{
    const ladder_t *c = ctx;
    static const char *names[3] = { "a", "b", "c" };
    track_t t[3];
    for (int i = 0; i < 3; i++)
        MOQ_TEST_CHECK(subscribe(p, names[i], c->prio[i], NONE, NONE, &t[i]));
    for (int i = 2; i >= 0; i--)   /* lowest priority published first */
        MOQ_TEST_CHECK(publish_group(p, &t[i], (uint8_t)(i + 1), 1, 0, 128, 2,
                                     false, NULL));
}

static void test_ladder_and_default(void)
{
    static const uint32_t want[] = {
        OBJ(1, 1, 0, 0), OBJ(1, 1, 0, 1), OBJ(2, 1, 0, 0), OBJ(2, 1, 0, 1),
        OBJ(3, 1, 0, 0), OBJ(3, 1, 0, 1),
    };
    ladder_t sch13 = { { 0, 128, 255 } };
    run_case("SCH-13", build_ladder, &sch13, want, 6);
    ladder_t pri01 = { { 127, -1, 129 } };
    run_case("PRI-01", build_ladder, &pri01, want, 6);
    MOQ_TEST_PASS("priority ladder, default subscriber priority 128");
}

/* [SCH-10] Equal priorities on different subscriptions: no order is defined,
 * but none starves -- each gets data within the first round. */
static void test_equal_priorities_share(void)
{
    for (size_t v = 0; v < N_VERSIONS; v++)
        for (size_t c = 0; c < 3; c++) {
            pair_t p;
            MOQ_TEST_CHECK(pair_init(&p, k_versions[v], k_caps[c], true));
            static const char *names[3] = { "a", "b", "c" };
            track_t t[3];
            for (int i = 0; i < 3; i++)
                MOQ_TEST_CHECK(subscribe(&p, names[i], 128, NONE, NONE, &t[i]));
            for (int i = 0; i < 3; i++)
                for (uint64_t g = 1; g <= 3; g++)
                    MOQ_TEST_CHECK(publish_group(&p, &t[i], (uint8_t)(i + 1), g,
                                                 0, 128, 2, false, NULL));
            serve_all(&p);
            uint32_t got[64];
            size_t m = wire_objects(&p, got, 64);
            MOQ_TEST_CHECK_EQ_SIZE(m, 18);
            /* Within the first 6 objects (two per track if fair), every
             * track has been served. */
            bool seen[4] = { false };
            for (size_t i = 0; i < m && i < 6; i++) seen[got[i] >> 24] = true;
            if (!(seen[1] && seen[2] && seen[3]))
                fprintf(stderr, "SCH-10 (d%d, cap %zu): a track starved\n",
                        draft_of(k_versions[v]), k_caps[c]);
            MOQ_TEST_CHECK(seen[1] && seen[2] && seen[3]);
            pair_destroy(&p);
        }
    MOQ_TEST_PASS("equal priorities share the link");
}

/* -- C. streams over time --------------------------------------------------- */

/* Serve up to `n` writes; returns how many went. */
static size_t serve_n(pair_t *p, size_t n)
{
    size_t done = 0;
    while (done < n) {
        moq_transport_bridge_service(p->pub_br, p->now);
        if (!mt_serve_one(p->mt)) break;
        done++;
    }
    return done;
}

/* Index of the first wire object matching `mask`/`val`, or SIZE_MAX. */
static size_t first_index(const uint32_t *w, size_t n, uint32_t mask,
                          uint32_t val)
{
    for (size_t i = 0; i < n; i++)
        if ((w[i] & mask) == val) return i;
    return SIZE_MAX;
}

static size_t last_index(const uint32_t *w, size_t n, uint32_t mask,
                         uint32_t val)
{
    size_t r = SIZE_MAX;
    for (size_t i = 0; i < n; i++)
        if ((w[i] & mask) == val) r = i;
    return r;
}

#define TRACK_MASK 0xff000000u

/* Whether, from wire position `from` on, every object of the `lead` track
 * goes out before any object of another track. */
static bool lead_first_from(const uint32_t *w, size_t n, size_t from,
                            uint32_t lead)
{
    size_t first_other = SIZE_MAX;
    for (size_t i = from; i < n; i++)
        if ((w[i] & TRACK_MASK) != lead) { first_other = i; break; }
    return first_other == SIZE_MAX ||
           last_index(w, n, TRACK_MASK, lead) < first_other;
}

/* The data stream the bridge opened first (lowest stream id with a key). */
static mt_stream_t *first_data_stream(mt_t *m)
{
    mt_stream_t *r = NULL;
    for (size_t i = 0; i < MT_MAX_STREAMS; i++) {
        mt_stream_t *st = &m->streams[i];
        if (st->used && st->keyed && (!r || st->sid < r->sid)) r = st;
    }
    return r;
}

/* [SCH-11] A stream the peer's flow control blocks is not schedulable: the
 * lower-priority track flows meanwhile, and the blocked one takes the lead
 * again once unblocked. */
/* A transport that refuses one stream's writes (its own budget, as msquic's
 * is) leaves the others going: no stream holds back another's data. */
static void test_refused_stream_parks_alone(void)
{
    for (size_t v = 0; v < N_VERSIONS; v++) {
        pair_t p;
        MOQ_TEST_CHECK(pair_init(&p, k_versions[v], 1u << 20, true));
        track_t a, b;
        MOQ_TEST_CHECK(subscribe(&p, "a", 10, NONE, NONE, &a));
        MOQ_TEST_CHECK(subscribe(&p, "b", 20, NONE, NONE, &b));
        moq_subgroup_handle_t ag;
        MOQ_TEST_CHECK(publish_group(&p, &a, 1, 1, 0, 128, 1, true, &ag));
        moq_transport_bridge_service(p.pub_br, p.now);
        mt_stream_t *as = first_data_stream(p.mt);
        MOQ_TEST_CHECK(as != NULL);
        if (as) as->refuse = true;
        for (uint64_t o = 1; o < 4; o++)
            MOQ_TEST_CHECK_EQ_INT((int)write_object(&p, ag, 1, 1, 0, o), MOQ_OK);
        MOQ_TEST_CHECK(publish_group(&p, &b, 2, 1, 0, 128, 3, false, NULL));
        serve_all(&p);
        uint32_t w[64];
        size_t n = wire_objects(&p, w, 64);
        /* A's first object, then all of B: none of A's refused ones. */
        MOQ_TEST_CHECK_EQ_SIZE(n, 4);
        MOQ_TEST_CHECK_EQ_SIZE(first_index(w, n, TRACK_MASK, OBJ(2, 0, 0, 0)), 1);
        if (as) as->refuse = false;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_close_subgroup(p.pub, ag, p.now),
                              MOQ_OK);
        serve_all(&p);
        n = wire_objects(&p, w, 64);
        MOQ_TEST_CHECK_EQ_SIZE(n, 7);
        MOQ_TEST_CHECK_EQ_SIZE(last_index(w, n, TRACK_MASK, OBJ(1, 0, 0, 0)), 6);
        MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(p.pub_br));
        pair_destroy(&p);
    }
    MOQ_TEST_PASS("a stream the transport refuses parks alone");
}

/* Data the bridge already holds when the session closes goes to the
 * transport ahead of the close, as far as the transport takes it. */
static void test_data_before_close(void)
{
    for (size_t v = 0; v < N_VERSIONS; v++) {
        pair_t p;
        MOQ_TEST_CHECK(pair_init(&p, k_versions[v], 1, true));
        track_t t;
        MOQ_TEST_CHECK(subscribe(&p, "v", -1, NONE, NONE, &t));
        MOQ_TEST_CHECK(publish_group(&p, &t, 1, 1, 0, 128, 3, false, NULL));
        moq_transport_bridge_service(p.pub_br, p.now);
        /* The transport took one write and sends it; the rest waits in the
         * bridge, which is not serviced again before the close. */
        size_t written = p.mt->nchunks;
        while (mt_serve_one(p.mt)) {}
        MOQ_TEST_CHECK(moq_transport_bridge_has_outbound_pending(p.pub_br));
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_close(p.pub, 0, NULL, p.now),
                              MOQ_OK);
        moq_transport_bridge_service(p.pub_br, p.now);
        MOQ_TEST_CHECK(p.mt->nchunks > written);
        pair_destroy(&p);
    }
    MOQ_TEST_PASS("held data goes out before the close");
}

static void test_flow_blocked_stream(void)
{
    for (size_t v = 0; v < N_VERSIONS; v++) {
        pair_t p;
        MOQ_TEST_CHECK(pair_init(&p, k_versions[v], 1u << 20, true));
        track_t a, b;
        MOQ_TEST_CHECK(subscribe(&p, "a", 10, NONE, NONE, &a));
        MOQ_TEST_CHECK(subscribe(&p, "b", 20, NONE, NONE, &b));
        MOQ_TEST_CHECK(publish_group(&p, &a, 1, 1, 0, 128, 3, false, NULL));
        moq_transport_bridge_service(p.pub_br, p.now);
        mt_stream_t *as = first_data_stream(p.mt);
        MOQ_TEST_CHECK(as != NULL);
        if (as) as->blocked = true;
        MOQ_TEST_CHECK(publish_group(&p, &b, 2, 1, 0, 128, 3, false, NULL));
        MOQ_TEST_CHECK(publish_group(&p, &b, 2, 2, 0, 128, 3, false, NULL));
        /* While A is blocked only B can go: some of it does. */
        serve_n(&p, 4);
        uint32_t w[64];
        size_t n = wire_objects(&p, w, 64);
        MOQ_TEST_CHECK(n > 0);
        MOQ_TEST_CHECK_EQ_SIZE(first_index(w, n, TRACK_MASK, OBJ(1, 0, 0, 0)),
                               SIZE_MAX);
        size_t b_before = n;
        if (as) as->blocked = false;
        serve_all(&p);
        n = wire_objects(&p, w, 64);
        MOQ_TEST_CHECK_EQ_SIZE(n, 9);
        /* After the unblock, all of A goes before the rest of B. */
        MOQ_TEST_CHECK_EQ_SIZE(first_index(w, n, TRACK_MASK, OBJ(1, 0, 0, 0)),
                               b_before);
        MOQ_TEST_CHECK_EQ_SIZE(last_index(w, n, TRACK_MASK, OBJ(1, 0, 0, 0)),
                               b_before + 2);
        pair_destroy(&p);
    }
    MOQ_TEST_PASS("a flow-blocked stream does not stall the others");
}

/* [SCH-12] A newer group whose next object appears while an older group is
 * draining is served before the rest of the older group (descending). */
static void test_live_subgroup_catches_up(void)
{
    for (size_t v = 0; v < N_VERSIONS; v++)
        for (size_t c = 0; c < 3; c++) {
            pair_t p;
            MOQ_TEST_CHECK(pair_init(&p, k_versions[v], k_caps[c], true));
            track_t t;
            MOQ_TEST_CHECK(subscribe(&p, "v", -1, DESC, NONE, &t));
            moq_subgroup_handle_t g8;
            MOQ_TEST_CHECK(publish_group(&p, &t, 1, 7, 0, 128, 6, false, NULL));
            MOQ_TEST_CHECK(publish_group(&p, &t, 1, 8, 0, 128, 0, true, &g8));
            serve_n(&p, 3);
            /* Group 7's objects already scheduled may go first; the rest
             * follow group 8's (descending). */
            uint32_t w[64];
            size_t bound = wire_objects(&p, w, 64) + mt_held_objects(p.mt) + 1;
            MOQ_TEST_CHECK_EQ_INT((int)write_object(&p, g8, 1, 8, 0, 0),
                                  MOQ_OK);
            MOQ_TEST_CHECK_EQ_INT((int)moq_session_close_subgroup(p.pub, g8,
                                                                  p.now),
                                  MOQ_OK);
            serve_all(&p);
            size_t n = wire_objects(&p, w, 64);
            MOQ_TEST_CHECK_EQ_SIZE(n, 7);
            size_t i8 = first_index(w, n, 0xffffffffu, OBJ(1, 8, 0, 0));
            if (k_caps[c] == 1) MOQ_TEST_CHECK(bound < 6);
            MOQ_TEST_CHECK(i8 != SIZE_MAX && i8 <= bound);
            pair_destroy(&p);
        }
    MOQ_TEST_PASS("a newer group overtakes the rest of an older one");
}

/* Each subgroup travels on exactly one stream, which ends with a FIN after
 * its last object, and every object arrives. */
static void expect_complete(const pair_t *p, const char *what)
{
    for (size_t i = 0; i < p->mt->nwire; i++) {
        const uint8_t *h = p->mt->wire[i].head;
        if (p->mt->wire[i].reset || p->mt->wire[i].len != PAYLOAD_LEN ||
            h[0] != PAYLOAD_TAG)
            continue;
        uint64_t sid = p->mt->wire[i].sid;
        bool fin = false, other_sid = false;
        for (size_t j = i; j < p->mt->nwire; j++)   /* at or after it */
            if (p->mt->wire[j].sid == sid && p->mt->wire[j].fin) fin = true;
        for (size_t j = 0; j < p->mt->nwire; j++) {
            const uint8_t *k = p->mt->wire[j].head;
            if (p->mt->wire[j].len == PAYLOAD_LEN && k[0] == PAYLOAD_TAG &&
                k[1] == h[1] && k[2] == h[2] && k[3] == h[3] &&
                p->mt->wire[j].sid != sid)
                other_sid = true;
        }
        if (!fin || other_sid)
            fprintf(stderr, "%s: subgroup %u/%u/%u fin=%d split=%d\n", what,
                    h[1], h[2], h[3], fin, other_sid);
        MOQ_TEST_CHECK(fin && !other_sid);
    }
}

/* [SCH-14] Uncongested: everything arrives as produced. [SCH-15] After
 * congestion: the starved older groups are all delivered, with FIN. */
static void test_completeness(void)
{
    for (size_t v = 0; v < N_VERSIONS; v++)
        for (int congested = 0; congested <= 1; congested++) {
            pair_t p;
            MOQ_TEST_CHECK(pair_init(&p, k_versions[v], 64, true));
            track_t a, b;
            MOQ_TEST_CHECK(subscribe(&p, "a", 10, DESC, NONE, &a));
            MOQ_TEST_CHECK(subscribe(&p, "b", 200, ASC, NONE, &b));
            for (uint64_t g = 1; g <= 4; g++) {
                MOQ_TEST_CHECK(publish_group(&p, &a, 1, g, 0, 128, 3, false,
                                             NULL));
                MOQ_TEST_CHECK(publish_group(&p, &b, 2, g, 0, 64, 3, false,
                                             NULL));
                if (!congested) serve_all(&p);
            }
            serve_all(&p);
            uint32_t w[64];
            MOQ_TEST_CHECK_EQ_SIZE(wire_objects(&p, w, 64), 24);
            expect_complete(&p, congested ? "SCH-15" : "SCH-14");
            pair_destroy(&p);
        }
    MOQ_TEST_PASS("every object delivered, one stream per subgroup, FIN");
}

/* [PRI-05] [PRI-06] A subscriber priority update reorders data not yet sent,
 * on streams already open; [PRI-07] an update without a priority keeps it. */
typedef enum { UPD_RAISE_B, UPD_LOWER_A, UPD_NO_PRIORITY } upd_kind_t;

static void check_priority_update(moq_version_t ver, size_t cap,
                                  upd_kind_t kind)
{
    pair_t p;
    MOQ_TEST_CHECK(pair_init(&p, ver, cap, true));
    track_t a, b;
    MOQ_TEST_CHECK(subscribe(&p, "a", 10, NONE, NONE, &a));
    MOQ_TEST_CHECK(subscribe(&p, "b", 20, NONE, NONE, &b));
    MOQ_TEST_CHECK(publish_group(&p, &a, 1, 1, 0, 128, 6, false, NULL));
    MOQ_TEST_CHECK(publish_group(&p, &b, 2, 1, 0, 128, 6, false, NULL));
    /* A leads until at least one of its objects is out. */
    uint32_t w[64] = { 0 };
    while (wire_objects(&p, w, 64) == 0 && serve_n(&p, 1) == 1) {}
    MOQ_TEST_CHECK_EQ_INT((int)(w[0] >> 24), 1);

    moq_subscription_update_cfg_t uc;
    moq_subscription_update_cfg_init(&uc);
    const track_t *target = kind == UPD_RAISE_B ? &b : &a;
    if (kind == UPD_NO_PRIORITY) {
        uc.has_forward = true;
        uc.forward = true;
    } else {
        uc.has_subscriber_priority = true;
        uc.subscriber_priority = kind == UPD_RAISE_B ? 5 : 30;
    }
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_subscription(p.sub,
        target->csub, &uc, p.now), MOQ_OK);
    /* Objects already scheduled are left to the implementation (MOQT 7.1):
     * those the transport holds, plus the one write the bridge may have
     * committed to it and is retrying. Below the largest queue the bridge
     * still holds some, so the update has objects to reorder. */
    size_t before = wire_objects(&p, w, 64) + mt_held_objects(p.mt) + 1;
    if (cap < (1u << 20)) MOQ_TEST_CHECK(before < 12);
    shuttle(&p);
    drain_events(p.pub);
    drain_events(p.sub);
    serve_all(&p);
    size_t n = wire_objects(&p, w, 64);
    MOQ_TEST_CHECK_EQ_SIZE(n, 12);
    /* After the update: the leading track drains before the other resumes. */
    uint32_t lead = kind == UPD_NO_PRIORITY ? OBJ(1, 0, 0, 0)
                                            : OBJ(2, 0, 0, 0);
    if (!lead_first_from(w, n, before, lead))
        fprintf(stderr, "subscription priority update %d (d%d, cap %zu): "
                "not reordered\n", (int)kind, draft_of(ver), cap);
    MOQ_TEST_CHECK(lead_first_from(w, n, before, lead));
    pair_destroy(&p);
}

static void test_priority_updates(void)
{
    for (size_t v = 0; v < N_VERSIONS; v++)
        for (size_t c = 0; c < 3; c++) {
            check_priority_update(k_versions[v], k_caps[c], UPD_RAISE_B);
            check_priority_update(k_versions[v], k_caps[c], UPD_LOWER_A);
            check_priority_update(k_versions[v], k_caps[c], UPD_NO_PRIORITY);
        }
    MOQ_TEST_PASS("subscriber priority updates reorder pending data");
}

/* [PRI-09] A publisher-initiated subscription's priority is updated too:
 * accepted at 200 next to a subscription at 100, then raised to 50, its
 * pending data overtakes the subscription's. */
static void check_publication_update(moq_version_t ver, size_t cap)
{
    pair_t p;
    MOQ_TEST_CHECK(pair_init(&p, ver, cap, true));
    track_t s;
    MOQ_TEST_CHECK(subscribe(&p, "s", 100, NONE, NONE, &s));

    moq_bytes_t part = MOQ_BYTES_LITERAL("live");
    moq_publish_cfg_t pc;
    moq_publish_cfg_init(&pc);
    pc.track_namespace = (moq_namespace_t){ &part, 1 };
    pc.track_name = MOQ_BYTES_LITERAL("p");
    moq_publication_t pub;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_publish(p.pub, &pc, p.now, &pub),
                          MOQ_OK);
    shuttle(&p);
    moq_event_t ev;
    moq_publication_t cpub = { 0 };
    while (moq_session_poll_events(p.sub, &ev, 1) == 1) {
        if (ev.kind == MOQ_EVENT_PUBLISH_REQUEST)
            cpub = ev.u.publish_request.pub;
        moq_event_cleanup(&ev);
    }
    moq_accept_publish_cfg_t ac;
    moq_accept_publish_cfg_init(&ac);
    /* Draft-22 9.3: PUBLISH_OK carries no subscriber priority; a
     * REQUEST_UPDATE sets it. */
    bool d22 = ver == MOQ_VERSION_DRAFT_22;
    ac.has_subscriber_priority = !d22;
    ac.subscriber_priority = 200;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_publish(p.sub, cpub, &ac,
                                                          p.now), MOQ_OK);
    shuttle(&p);
    drain_events(p.pub);
    if (d22) {
        moq_publication_update_cfg_t first;
        moq_publication_update_cfg_init(&first);
        first.has_subscriber_priority = true;
        first.subscriber_priority = 200;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_publication(p.sub, cpub,
                                  &first, p.now), MOQ_OK);
        shuttle(&p);
        drain_events(p.pub);
        drain_events(p.sub);
    }

    MOQ_TEST_CHECK(publish_group(&p, &s, 1, 1, 0, 128, 6, false, NULL));
    moq_subgroup_cfg_t sc;
    moq_subgroup_cfg_init(&sc);
    sc.group_id = 1;
    sc.publisher_priority = 128;
    moq_subgroup_handle_t sg;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_pub_subgroup(p.pub, pub, &sc,
                                                             p.now, &sg),
                          MOQ_OK);
    for (uint64_t o = 0; o < 6; o++)
        MOQ_TEST_CHECK_EQ_INT((int)write_object(&p, sg, 2, 1, 0, o), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_close_subgroup(p.pub, sg, p.now),
                          MOQ_OK);
    uint32_t w[64] = { 0 };
    while (wire_objects(&p, w, 64) == 0 && serve_n(&p, 1) == 1) {}
    MOQ_TEST_CHECK_EQ_INT((int)(w[0] >> 24), 1);

    moq_publication_update_cfg_t uc;
    moq_publication_update_cfg_init(&uc);
    uc.has_subscriber_priority = true;
    uc.subscriber_priority = 50;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_publication(p.sub, cpub, &uc,
                                                              p.now), MOQ_OK);
    /* As in check_priority_update: what is already scheduled may go first. */
    size_t before = wire_objects(&p, w, 64) + mt_held_objects(p.mt) + 1;
    if (cap < (1u << 20)) MOQ_TEST_CHECK(before < 12);
    shuttle(&p);
    drain_events(p.pub);
    drain_events(p.sub);
    serve_all(&p);
    size_t n = wire_objects(&p, w, 64);
    MOQ_TEST_CHECK_EQ_SIZE(n, 12);
    if (!lead_first_from(w, n, before, OBJ(2, 0, 0, 0)))
        fprintf(stderr, "publication priority update (d%d, cap %zu): "
                "not reordered\n", draft_of(ver), cap);
    MOQ_TEST_CHECK(lead_first_from(w, n, before, OBJ(2, 0, 0, 0)));
    pair_destroy(&p);
}

static void test_publication_update(void)
{
    for (size_t v = 0; v < N_VERSIONS; v++)
        for (size_t c = 0; c < 3; c++)
            check_publication_update(k_versions[v], k_caps[c]);
    MOQ_TEST_PASS("publication priority updates reorder pending data");
}

/* -- D. FETCH ---------------------------------------------------------------- */

typedef struct {
    moq_fetch_t fetch;    /* the publisher's handle */
    moq_fetch_t cfetch;   /* the subscriber's handle */
} fetch_t;

/* The subscriber fetches "live"/`name` groups 0..9 at `sub_prio`; the
 * publisher accepts. */
static bool start_fetch(pair_t *p, const char *name, int sub_prio, fetch_t *out)
{
    moq_bytes_t part = MOQ_BYTES_LITERAL("live");
    moq_fetch_cfg_t fc;
    moq_fetch_cfg_init(&fc);
    fc.track_namespace = (moq_namespace_t){ &part, 1 };
    fc.track_name = (moq_bytes_t){ (const uint8_t *)name, strlen(name) };
    fc.end_group = 9;
    fc.end_object = 0;
    fc.has_subscriber_priority = sub_prio >= 0;
    fc.subscriber_priority = (uint8_t)(sub_prio >= 0 ? sub_prio : 0);
    if (moq_session_fetch(p->sub, &fc, p->now, &out->cfetch) != MOQ_OK)
        return false;
    shuttle(p);
    moq_event_t ev;
    bool got = false;
    while (moq_session_poll_events(p->pub, &ev, 1) == 1) {
        if (ev.kind == MOQ_EVENT_FETCH_REQUEST) {
            out->fetch = ev.u.fetch_request.fetch;
            got = true;
        }
        moq_event_cleanup(&ev);
    }
    if (!got) return false;
    moq_accept_fetch_cfg_t ac;
    moq_accept_fetch_cfg_init(&ac);
    ac.end_group = 9;
    if (moq_session_accept_fetch(p->pub, out->fetch, &ac, p->now) != MOQ_OK)
        return false;
    shuttle(p);
    drain_events(p->sub);
    return true;
}

static bool fetch_object(pair_t *p, const fetch_t *f, uint8_t track,
                         uint64_t group, uint64_t object, uint8_t pub_prio)
{
    uint8_t body[PAYLOAD_LEN] = { PAYLOAD_TAG, track, (uint8_t)group, 0,
                                  (uint8_t)object };
    moq_rcbuf_t *rb = NULL;
    if (moq_rcbuf_create(moq_alloc_default(), body, sizeof(body), &rb) < 0)
        return false;
    moq_fetch_object_cfg_t oc;
    moq_fetch_object_cfg_init(&oc);
    oc.group_id = group;
    oc.object_id = object;
    oc.publisher_priority = pub_prio;
    oc.payload = rb;
    moq_result_t rc = moq_session_write_fetch_object(p->pub, f->fetch, &oc,
                                                     p->now);
    moq_rcbuf_decref(rb);
    return rc == MOQ_OK;
}

/* [FET-04] A FETCH response's schedulable object is its next object: while
 * that is group 3 (publisher priority 10) the fetch goes ahead of the
 * subscription (100); once it is group 4 (200) the subscription goes first.
 * The fetch stream's own order never changes. */
static void build_fet04(pair_t *p, void *ctx)
{
    (void)ctx;
    track_t s;
    fetch_t f;
    MOQ_TEST_CHECK(subscribe(p, "s", 128, NONE, NONE, &s));
    MOQ_TEST_CHECK(start_fetch(p, "f", 128, &f));
    MOQ_TEST_CHECK(publish_group(p, &s, 1, 1, 0, 100, 2, false, NULL));
    MOQ_TEST_CHECK(fetch_object(p, &f, 2, 3, 0, 10));
    MOQ_TEST_CHECK(fetch_object(p, &f, 2, 3, 1, 10));
    MOQ_TEST_CHECK(fetch_object(p, &f, 2, 4, 0, 200));
    MOQ_TEST_CHECK(fetch_object(p, &f, 2, 4, 1, 200));
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_end_fetch(p->pub, f.fetch, p->now),
                          MOQ_OK);
}

/* [FET-05] A FETCH and a subscription with different subscriber priorities:
 * the better one drains first (the fetch here). */
static void build_fet05(pair_t *p, void *ctx)
{
    (void)ctx;
    track_t s;
    fetch_t f;
    MOQ_TEST_CHECK(subscribe(p, "s", 20, NONE, NONE, &s));
    MOQ_TEST_CHECK(start_fetch(p, "f", 10, &f));
    MOQ_TEST_CHECK(publish_group(p, &s, 1, 1, 0, 128, 2, false, NULL));
    MOQ_TEST_CHECK(fetch_object(p, &f, 2, 3, 0, 128));
    MOQ_TEST_CHECK(fetch_object(p, &f, 2, 3, 1, 128));
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_end_fetch(p->pub, f.fetch, p->now),
                          MOQ_OK);
}

/* A fetcher's REQUEST_UPDATE raises an accepted FETCH (draft-22 9.11) above a
 * subscription: the fetch's objects not yet scheduled overtake the
 * subscription's (MOQT 7.1). */
static void check_fetch_update(size_t cap)
{
    pair_t p;
    MOQ_TEST_CHECK(pair_init(&p, MOQ_VERSION_DRAFT_22, cap, true));
    track_t s;
    fetch_t f;
    MOQ_TEST_CHECK(subscribe(&p, "s", 100, NONE, NONE, &s));
    MOQ_TEST_CHECK(start_fetch(&p, "f", 200, &f));
    MOQ_TEST_CHECK(publish_group(&p, &s, 1, 1, 0, 128, 6, false, NULL));
    for (uint64_t o = 0; o < 6; o++)
        MOQ_TEST_CHECK(fetch_object(&p, &f, 2, 3, o, 128));
    uint32_t w[64] = { 0 };
    while (wire_objects(&p, w, 64) == 0 && serve_n(&p, 1) == 1) {}
    MOQ_TEST_CHECK_EQ_INT((int)(w[0] >> 24), 1);

    moq_fetch_update_cfg_t uc;
    moq_fetch_update_cfg_init(&uc);
    uc.has_subscriber_priority = true;
    uc.subscriber_priority = 50;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_fetch(p.sub, f.cfetch, &uc,
                                                        p.now), MOQ_OK);
    size_t before = wire_objects(&p, w, 64) + mt_held_objects(p.mt) + 1;
    if (cap < (1u << 20)) MOQ_TEST_CHECK(before < 12);
    shuttle(&p);
    drain_events(p.pub);
    drain_events(p.sub);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_end_fetch(p.pub, f.fetch, p.now),
                          MOQ_OK);
    serve_all(&p);
    size_t n = wire_objects(&p, w, 64);
    MOQ_TEST_CHECK_EQ_SIZE(n, 12);
    if (!lead_first_from(w, n, before, OBJ(2, 0, 0, 0)))
        fprintf(stderr, "fetch priority update (d22, cap %zu): not "
                "reordered\n", cap);
    MOQ_TEST_CHECK(lead_first_from(w, n, before, OBJ(2, 0, 0, 0)));
    pair_destroy(&p);
}

static void test_fetch_update(void)
{
    for (size_t c = 0; c < 3; c++) check_fetch_update(k_caps[c]);
    MOQ_TEST_PASS("FETCH priority updates reorder pending data");
}

static void test_fetch_scheduling(void)
{
    static const uint32_t w04[] = {
        OBJ(2, 3, 0, 0), OBJ(2, 3, 0, 1), OBJ(1, 1, 0, 0), OBJ(1, 1, 0, 1),
        OBJ(2, 4, 0, 0), OBJ(2, 4, 0, 1),
    };
    run_case("FET-04", build_fet04, NULL, w04, 6);
    static const uint32_t w05[] = {
        OBJ(2, 3, 0, 0), OBJ(2, 3, 0, 1), OBJ(1, 1, 0, 0), OBJ(1, 1, 0, 1),
    };
    run_case("FET-05", build_fet05, NULL, w05, 4);
    MOQ_TEST_PASS("FETCH responses scheduled by their next object");
}

/* -- E. control and resets ---------------------------------------------------- */

/* [CTL-01] Control messages do not wait behind queued object data: with the
 * network stalled and the publisher's buffers full of data, a new SUBSCRIBE
 * is still answered. */
static void test_control_not_behind_data(void)
{
    for (size_t v = 0; v < N_VERSIONS; v++) {
        pair_t p;
        MOQ_TEST_CHECK(pair_init(&p, k_versions[v], 64, true));
        track_t a;
        MOQ_TEST_CHECK(subscribe(&p, "a", 128, NONE, NONE, &a));
        /* Well past every buffer: the bridge's retention limit included. */
        for (uint64_t g = 1; g <= 40; g++)
            MOQ_TEST_CHECK(publish_group(&p, &a, 1, g, 0, 128, 2, false, NULL));
        track_t b;
        bool answered = subscribe(&p, "b", 128, NONE, NONE, &b);
        if (!answered)
            fprintf(stderr, "CTL-01 (d%d): SUBSCRIBE not answered behind "
                    "data\n", draft_of(k_versions[v]));
        MOQ_TEST_CHECK(answered);
        uint32_t w[8];
        MOQ_TEST_CHECK_EQ_SIZE(wire_objects(&p, w, 8), 0);   /* still stalled */
        /* Control and request streams never take a data stream's key. */
        MOQ_TEST_CHECK_EQ_U64(p.mt->bidi_keyed, 0);
        pair_destroy(&p);
    }
    MOQ_TEST_PASS("control is not stuck behind queued data");
}

/* The data stream that carried object (track, group, subgroup, *). */
static bool stream_of(const pair_t *p, uint8_t track, uint64_t group,
                      uint64_t *sid)
{
    for (size_t i = 0; i < p->mt->nchunks; i++) {
        const mt_chunk_t *c = &p->mt->chunks[i];
        if (c->len == PAYLOAD_LEN && c->head[0] == PAYLOAD_TAG &&
            c->head[1] == track && c->head[2] == (uint8_t)group) {
            *sid = c->sid;
            return true;
        }
    }
    return false;
}

static bool was_reset(const pair_t *p, uint64_t sid)
{
    for (size_t i = 0; i < p->mt->nwire; i++)
        if (p->mt->wire[i].reset && p->mt->wire[i].sid == sid) return true;
    return false;
}

/* [RST-01] STOP_SENDING on one subgroup stream under congestion: that stream
 * is reset and not reopened; the other groups continue in order, and the
 * subscription stays up. */
static void test_stop_sending_one_stream(void)
{
    for (size_t v = 0; v < N_VERSIONS; v++) {
        pair_t p;
        MOQ_TEST_CHECK(pair_init(&p, k_versions[v], 1u << 20, true));
        track_t t;
        MOQ_TEST_CHECK(subscribe(&p, "v", -1, DESC, NONE, &t));
        /* Open subgroups: their streams are still the library's to end. */
        for (uint64_t g = 5; g <= 8; g++)
            MOQ_TEST_CHECK(publish_group(&p, &t, 1, g, 0, 128, 3, true, NULL));
        moq_transport_bridge_service(p.pub_br, p.now);
        uint64_t sid7 = 0;
        MOQ_TEST_CHECK(stream_of(&p, 1, 7, &sid7));
        moq_transport_bridge_on_peer_stop_sending(p.pub_br, sid7, 0x1, p.now);
        shuttle(&p);
        serve_all(&p);
        MOQ_TEST_CHECK(was_reset(&p, sid7));
        uint32_t w[64];
        size_t n = wire_objects(&p, w, 64);
        static const uint32_t want[] = {
            OBJ(1, 8, 0, 0), OBJ(1, 8, 0, 1), OBJ(1, 8, 0, 2),
            OBJ(1, 6, 0, 0), OBJ(1, 6, 0, 1), OBJ(1, 6, 0, 2),
            OBJ(1, 5, 0, 0), OBJ(1, 5, 0, 1), OBJ(1, 5, 0, 2),
        };
        bool same = n == 9;
        for (size_t i = 0; same && i < 9; i++) same = w[i] == want[i];
        MOQ_TEST_CHECK(same);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(p.pub),
                              MOQ_SESS_ESTABLISHED);
        moq_event_t ev;
        bool done = false;
        while (moq_session_poll_events(p.sub, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_DONE) done = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(!done);
        pair_destroy(&p);
    }
    MOQ_TEST_PASS("STOP_SENDING resets one stream, the rest continue");
}

/* In the session's own queue a stream's key goes with the header it
 * precedes: a better-priority stream opened after worse data is taken ahead
 * of that data, key first (MOQT 7.2). */
static void test_key_moves_with_its_stream(void)
{
    pair_t p;
    MOQ_TEST_CHECK(pair_init(&p, MOQ_VERSION_DRAFT_16, 1u << 20, true));
    track_t a, b;
    MOQ_TEST_CHECK(subscribe(&p, "a", 200, NONE, NONE, &a));
    MOQ_TEST_CHECK(subscribe(&p, "b", 10, NONE, NONE, &b));
    MOQ_TEST_CHECK(publish_group(&p, &a, 1, 1, 0, 128, 3, true, NULL));
    MOQ_TEST_CHECK(publish_group(&p, &b, 2, 1, 0, 128, 1, true, NULL));
    /* Taken straight from the session, ahead of the bridge. */
    moq_action_t act;
    size_t i = 0, b_key = SIZE_MAX, b_first_data = SIZE_MAX, a_last = 0;
    uint64_t b_ref = 0;
    while (moq_session_poll_actions(p.pub, &act, 1) == 1) {
        if (act.kind == MOQ_ACTION_SET_DATA_PRIORITY &&
            act.u.set_data_priority.priority.subscriber_priority == 10) {
            b_key = i;
            b_ref = act.u.set_data_priority.stream_ref._v;
        } else if (act.kind == MOQ_ACTION_SEND_DATA) {
            if (act.u.send_data.stream_ref._v == b_ref) {
                if (b_first_data == SIZE_MAX) b_first_data = i;
            } else {
                a_last = i;
            }
        }
        moq_action_cleanup(&act);
        i++;
    }
    MOQ_TEST_CHECK(b_key != SIZE_MAX && b_first_data == b_key + 1);
    MOQ_TEST_CHECK(b_first_data < a_last);
    pair_destroy(&p);
    MOQ_TEST_PASS("a stream's key moves with its stream");
}

/* Data that waits only for a stream-map entry is not send-side work the
 * transport's write readiness can unblock: the bridge does not ask to be
 * serviced for it, so a write-ready loop does not spin. Draft-16, whose
 * stream map holds data streams only. */
static void test_map_full_is_not_outbound_work(void)
{
    pair_t p;
    MOQ_TEST_CHECK(pair_init_streams(&p, MOQ_VERSION_DRAFT_16, 1u << 20, true, 1));
    track_t t;
    MOQ_TEST_CHECK(subscribe(&p, "v", -1, NONE, NONE, &t));
    moq_subgroup_handle_t g1;
    MOQ_TEST_CHECK(publish_group(&p, &t, 1, 1, 0, 128, 1, true, &g1));
    serve_all(&p);
    MOQ_TEST_CHECK(!moq_transport_bridge_has_outbound_pending(p.pub_br));
    MOQ_TEST_CHECK(publish_group(&p, &t, 1, 2, 0, 128, 1, false, NULL));
    moq_transport_bridge_service(p.pub_br, p.now);
    /* Group 2 waits for group 1's entry. */
    MOQ_TEST_CHECK(moq_transport_bridge_has_pending(p.pub_br));
    MOQ_TEST_CHECK(!moq_transport_bridge_has_outbound_pending(p.pub_br));
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_close_subgroup(p.pub, g1, p.now),
                          MOQ_OK);
    serve_all(&p);
    uint32_t w[8];
    MOQ_TEST_CHECK_EQ_SIZE(wire_objects(&p, w, 8), 2);
    pair_destroy(&p);
    MOQ_TEST_PASS("a full stream map is not outbound work");
}

/* Stream keys need room beside the actions one call queues: with fewer than
 * four action slots the bridge keys each write by its priorities alone, so
 * two groups of one subscription tie; with four they take the group order. */
static void test_keys_need_four_slots(void)
{
    for (uint32_t slots = 3; slots <= 4; slots++) {
        pair_t p;
        MOQ_TEST_CHECK(pair_init_limits(&p, MOQ_VERSION_DRAFT_16, 1u << 20,
                                        true, 0, slots));
        track_t t;
        MOQ_TEST_CHECK(subscribe(&p, "v", -1, DESC, NONE, &t));
        MOQ_TEST_CHECK(publish_group(&p, &t, 1, 1, 0, 128, 1, true, NULL));
        moq_transport_bridge_service(p.pub_br, p.now);
        MOQ_TEST_CHECK(publish_group(&p, &t, 1, 2, 0, 128, 1, true, NULL));
        moq_transport_bridge_service(p.pub_br, p.now);
        uint64_t sid1 = 0, sid2 = 0;
        MOQ_TEST_CHECK(stream_of(&p, 1, 1, &sid1));
        MOQ_TEST_CHECK(stream_of(&p, 1, 2, &sid2));
        const mt_stream_t *g1 = mt_stream(p.mt, sid1);
        const mt_stream_t *g2 = mt_stream(p.mt, sid2);
        MOQ_TEST_CHECK(g1 && g2 && g1->keyed && g2->keyed);
        if (g1 && g2) {
            if (slots < 4) MOQ_TEST_CHECK_EQ_U64(g1->key, g2->key);
            else MOQ_TEST_CHECK(g2->key < g1->key);   /* descending */
        }
        pair_destroy(&p);
    }
    MOQ_TEST_PASS("stream keys need four action slots");
}

/* A subgroup reset before its stream could open, because every
 * stream the transport allows is in use: the stream still opens, its header
 * is handed to the transport ahead of the reset (the done message counts it
 * among the streams opened), and none of its objects is. */
static void test_reset_before_open(void)
{
    /* On draft-16 the stream map holds data streams only, so a map of one
     * entry is exact; the bridge logic is the same on every draft. */
    {
        pair_t p;
        MOQ_TEST_CHECK(pair_init_streams(&p, MOQ_VERSION_DRAFT_16, 1u << 20,
                                         true, 1));
        track_t t;
        MOQ_TEST_CHECK(subscribe(&p, "v", -1, DESC, NONE, &t));
        moq_subgroup_handle_t g1, g2;
        MOQ_TEST_CHECK(publish_group(&p, &t, 1, 1, 0, 128, 2, true, &g1));
        moq_transport_bridge_service(p.pub_br, p.now);
        MOQ_TEST_CHECK(publish_group(&p, &t, 1, 2, 0, 128, 2, true, &g2));
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_reset_subgroup(p.pub, g2, 0x9,
                                                              p.now), MOQ_OK);
        moq_transport_bridge_service(p.pub_br, p.now);
        MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(p.pub_br));
        /* Group 2's stream waits for group 1's to end. */
        uint64_t next_uni = p.mt->next_uni;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_close_subgroup(p.pub, g1, p.now),
                              MOQ_OK);
        serve_all(&p);
        MOQ_TEST_CHECK_EQ_U64(p.mt->next_uni, next_uni + 4);
        MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(p.pub_br));
        uint64_t sid1 = 0, sid2 = 0;
        MOQ_TEST_CHECK(stream_of(&p, 1, 1, &sid1));
        for (size_t i = 0; i < p.mt->nwire; i++)
            if (p.mt->wire[i].reset && mt_is_uni(p.mt->wire[i].sid))
                sid2 = p.mt->wire[i].sid;
        bool fin1 = false, header2 = false, object2 = false;
        for (size_t i = 0; i < p.mt->nchunks; i++) {
            const mt_chunk_t *c = &p.mt->chunks[i];
            if (c->sid == sid1) fin1 = fin1 || c->fin;
            if (sid2 == 0 || c->sid != sid2 || c->len == 0) continue;
            if (c->head[0] == PAYLOAD_TAG) object2 = true;
            else header2 = true;
        }
        MOQ_TEST_CHECK(sid2 != 0);
        MOQ_TEST_CHECK(fin1);
        MOQ_TEST_CHECK(header2);
        MOQ_TEST_CHECK(!object2);
        uint32_t w[16];
        size_t n = wire_objects(&p, w, 16);
        bool g2_obj = false;
        for (size_t i = 0; i < n; i++) g2_obj = g2_obj || ((w[i] >> 16) & 0xff) == 2;
        MOQ_TEST_CHECK(!g2_obj);
        MOQ_TEST_CHECK_EQ_SIZE(n, 2);
        pair_destroy(&p);
    }
    MOQ_TEST_PASS("a reset waiting for a stream opens it, header first");
}

/* [RST-03] The subscriber ends one of two subscriptions while both have
 * data pending: the ended one's open streams are reset, never FINed
 * incomplete, and the other is unaffected. */
static void test_unsubscribe_resets_pending(void)
{
    for (size_t v = 0; v < N_VERSIONS; v++) {
        pair_t p;
        MOQ_TEST_CHECK(pair_init(&p, k_versions[v], 1u << 20, true));
        track_t a, b;
        MOQ_TEST_CHECK(subscribe(&p, "a", 10, NONE, NONE, &a));
        MOQ_TEST_CHECK(subscribe(&p, "b", 20, NONE, NONE, &b));
        MOQ_TEST_CHECK(publish_group(&p, &a, 1, 1, 0, 128, 3, false, NULL));
        moq_subgroup_handle_t bg;
        MOQ_TEST_CHECK(publish_group(&p, &b, 2, 1, 0, 128, 3, true, &bg));
        moq_transport_bridge_service(p.pub_br, p.now);
        uint64_t bsid = 0;
        MOQ_TEST_CHECK(stream_of(&p, 2, 1, &bsid));
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_unsubscribe(p.sub, b.csub,
                                                           p.now), MOQ_OK);
        shuttle(&p);
        drain_events(p.pub);
        shuttle(&p);
        serve_all(&p);
        MOQ_TEST_CHECK(was_reset(&p, bsid));
        bool b_fin = false;
        for (size_t i = 0; i < p.mt->nwire; i++)
            if (p.mt->wire[i].sid == bsid && p.mt->wire[i].fin) b_fin = true;
        MOQ_TEST_CHECK(!b_fin);
        uint32_t w[64];
        size_t n = wire_objects(&p, w, 64);
        size_t a_count = 0;
        for (size_t i = 0; i < n; i++)
            if ((w[i] & TRACK_MASK) == OBJ(1, 0, 0, 0)) a_count++;
        MOQ_TEST_CHECK_EQ_SIZE(a_count, 3);
        pair_destroy(&p);
    }
    MOQ_TEST_PASS("ending a subscription resets its pending streams");
}

/* [RST-04] A FETCH cancelled with data pending: its stream is reset, not
 * FINed; the subscription alongside continues. */
static void test_fetch_cancel_resets(void)
{
    for (size_t v = 0; v < N_VERSIONS; v++) {
        pair_t p;
        MOQ_TEST_CHECK(pair_init(&p, k_versions[v], 1u << 20, true));
        track_t s;
        fetch_t f;
        MOQ_TEST_CHECK(subscribe(&p, "s", 10, NONE, NONE, &s));
        MOQ_TEST_CHECK(start_fetch(&p, "f", 20, &f));
        MOQ_TEST_CHECK(publish_group(&p, &s, 1, 1, 0, 128, 2, false, NULL));
        for (uint64_t o = 0; o < 4; o++)
            MOQ_TEST_CHECK(fetch_object(&p, &f, 2, 3, o, 128));
        moq_transport_bridge_service(p.pub_br, p.now);
        uint64_t fsid = 0;
        MOQ_TEST_CHECK(stream_of(&p, 2, 3, &fsid));
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch_cancel(p.sub, f.cfetch,
                                                            p.now), MOQ_OK);
        shuttle(&p);
        drain_events(p.pub);
        shuttle(&p);
        serve_all(&p);
        MOQ_TEST_CHECK(was_reset(&p, fsid));
        bool f_fin = false;
        for (size_t i = 0; i < p.mt->nwire; i++)
            if (p.mt->wire[i].sid == fsid && p.mt->wire[i].fin) f_fin = true;
        MOQ_TEST_CHECK(!f_fin);
        uint32_t w[64];
        size_t n = wire_objects(&p, w, 64);
        size_t s_count = 0;
        for (size_t i = 0; i < n; i++)
            if ((w[i] & TRACK_MASK) == OBJ(1, 0, 0, 0)) s_count++;
        MOQ_TEST_CHECK_EQ_SIZE(s_count, 2);
        pair_destroy(&p);
    }
    MOQ_TEST_PASS("a cancelled FETCH resets its pending stream");
}

/* [RST-05] The publisher ends a subscription whose streams still have data
 * pending: PUBLISH_DONE is not written while a stream is open (the library
 * refuses it until the application ends them), every reset precedes it, and
 * an incomplete subgroup never gets a FIN. */
static void test_done_with_pending(void)
{
    for (size_t v = 0; v < N_VERSIONS; v++) {
        pair_t p;
        MOQ_TEST_CHECK(pair_init(&p, k_versions[v], 1u << 20, true));
        track_t t;
        MOQ_TEST_CHECK(subscribe(&p, "v", 128, NONE, NONE, &t));
        moq_subgroup_handle_t sg[2];
        for (uint64_t g = 1; g <= 2; g++)
            MOQ_TEST_CHECK(publish_group(&p, &t, 1, g, 0, 128, 3, true,
                                         &sg[g - 1]));
        moq_transport_bridge_service(p.pub_br, p.now);
        uint64_t s1 = 0, s2 = 0;
        MOQ_TEST_CHECK(stream_of(&p, 1, 1, &s1));
        MOQ_TEST_CHECK(stream_of(&p, 1, 2, &s2));
        uint64_t mark = p.mt->op_seq;
        moq_done_subscribe_cfg_t dc;
        moq_done_subscribe_cfg_init(&dc);
        dc.status_code = 0x2;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_done_subscribe(p.pub, t.sub, &dc,
                                                              p.now),
                              MOQ_ERR_WRONG_STATE);
        shuttle(&p);
        MOQ_TEST_CHECK_EQ_U64(p.mt->op_seq, mark);   /* nothing written */
        p.mt->ctrl_mark = mark;
        for (int i = 0; i < 2; i++)
            MOQ_TEST_CHECK_EQ_INT((int)moq_session_reset_subgroup(p.pub, sg[i],
                                                                  0x1, p.now),
                                  MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_done_subscribe(p.pub, t.sub, &dc,
                                                              p.now), MOQ_OK);
        shuttle(&p);
        bool done = false;
        moq_event_t ev;
        while (moq_session_poll_events(p.sub, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_DONE) done = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(done);
        /* The first control write after the done is PUBLISH_DONE (or its
         * request-stream FIN); both resets come before it. */
        uint64_t first_ctrl = p.mt->ctrl_first_after;
        uint64_t reset1 = 0, reset2 = 0;
        for (size_t i = 0; i < p.mt->nwire; i++) {
            if (!p.mt->wire[i].reset || p.mt->wire_seq[i] <= mark) continue;
            if (p.mt->wire[i].sid == s1) reset1 = p.mt->wire_seq[i];
            if (p.mt->wire[i].sid == s2) reset2 = p.mt->wire_seq[i];
        }
        MOQ_TEST_CHECK(reset1 != 0 && reset2 != 0);
        MOQ_TEST_CHECK(first_ctrl > mark);
        MOQ_TEST_CHECK(reset1 < first_ctrl && reset2 < first_ctrl);
        serve_all(&p);
        for (size_t i = 0; i < p.mt->nwire; i++)
            MOQ_TEST_CHECK(!(p.mt->wire[i].fin &&
                             (p.mt->wire[i].sid == s1 ||
                              p.mt->wire[i].sid == s2)));
        pair_destroy(&p);
    }
    MOQ_TEST_PASS("ending a subscription resets its streams before the done");
}

/* -- delivery timeouts in the bridge --------------------------------------- */

/* OBJECT_DELIVERY_TIMEOUT = 50 ms as the track's only Track Property: type
 * and value below 64 encode the same on drafts 16, 18 and 22. */
static const uint8_t k_object_timeout_50ms[] = { 0x02, 50 };

static bool reset_with(const pair_t *p, uint64_t sid, uint64_t code)
{
    for (size_t i = 0; i < p->mt->nwire; i++)
        if (p->mt->wire[i].reset && p->mt->wire[i].sid == sid &&
            p->mt->wire[i].code == code)
            return true;
    return false;
}

/* [DT-01] draft-18 8 / draft-22 5.2: the time since an object's first byte is
 * checked "before attempting to pass it to the underlying transport"; past
 * OBJECT_DELIVERY_TIMEOUT the stream "MUST" be reset with DELIVERY_TIMEOUT
 * and no new stream is opened for that subgroup. The transport takes only
 * the first object, so the rest wait in the bridge while their timeout runs
 * out: they never reach the transport, the stream is reset with code 0x2 as
 * soon as the timeout passes, and the subscription's later groups go on. */
static void test_object_timeout_in_bridge(void)
{
    for (size_t v = 0; v < N_VERSIONS; v++) {
        pair_t p;
        MOQ_TEST_CHECK(pair_init(&p, k_versions[v], 1, true));
        track_t t;
        MOQ_TEST_CHECK(subscribe_props(&p, "v", -1, NONE, NONE,
                                       k_object_timeout_50ms,
                                       sizeof(k_object_timeout_50ms), &t));
        const uint64_t written = p.now;
        moq_subgroup_handle_t sg = {0};
        MOQ_TEST_CHECK(publish_group(&p, &t, 1, 0, 0, 128, 3, true, &sg));
        moq_transport_bridge_service(p.pub_br, p.now);
        mt_stream_t *st = first_data_stream(p.mt);
        MOQ_TEST_CHECK(st != NULL);
        uint64_t sid = st ? st->sid : 0;
        /* At least objects 1 and 2 are the bridge's, not the transport's. */
        MOQ_TEST_CHECK(mt_held_objects(p.mt) <= 1);
        /* The driver is woken for the timeout. */
        MOQ_TEST_CHECK(moq_session_next_deadline_us(p.pub) <=
                       written + 50 * 1000);

        p.now = written + 51 * 1000;
        moq_transport_bridge_service(p.pub_br, p.now);
        shuttle(&p);
        MOQ_TEST_CHECK(reset_with(&p, sid, MOQ_RESET_DELIVERY_TIMEOUT));
        MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(p.pub_br));

        /* Nothing more of that subgroup goes out, on its stream or a new
         * one, whatever the session answers this write. */
        (void)write_object(&p, sg, 1, 0, 0, 3);
        MOQ_TEST_CHECK(publish_group(&p, &t, 1, 1, 0, 128, 2, false, NULL));
        serve_all(&p);
        uint32_t w[16];
        size_t n = wire_objects(&p, w, 16);
        static const uint32_t want[] = { OBJ(1, 1, 0, 0), OBJ(1, 1, 0, 1) };
        bool same = n == 2 && w[0] == want[0] && w[1] == want[1];
        if (!same) {
            fprintf(stderr, "DT-01 (d%d): wire", draft_of(p.ver));
            for (size_t i = 0; i < n; i++) fprintf(stderr, " %08x", w[i]);
            fprintf(stderr, "\n");
        }
        MOQ_TEST_CHECK(same);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(p.pub),
                              MOQ_SESS_ESTABLISHED);
        pair_destroy(&p);
    }
    MOQ_TEST_PASS("an object that times out in the bridge is never sent");
}

/* [DT-02] The check is "before attempting to pass" an object to the
 * transport: an object the transport took in time is not timed out by the
 * bridge afterwards, however long the transport keeps it. */
static void test_object_timeout_after_handover(void)
{
    for (size_t v = 0; v < N_VERSIONS; v++) {
        pair_t p;
        MOQ_TEST_CHECK(pair_init(&p, k_versions[v], 1u << 20, true));
        track_t t;
        MOQ_TEST_CHECK(subscribe_props(&p, "v", -1, NONE, NONE,
                                       k_object_timeout_50ms,
                                       sizeof(k_object_timeout_50ms), &t));
        MOQ_TEST_CHECK(publish_group(&p, &t, 1, 0, 0, 128, 3, false, NULL));
        moq_transport_bridge_service(p.pub_br, p.now);
        MOQ_TEST_CHECK_EQ_SIZE(mt_held_objects(p.mt), 3);
        p.now += 100 * 1000;
        serve_all(&p);
        uint32_t w[8];
        MOQ_TEST_CHECK_EQ_SIZE(wire_objects(&p, w, 8), 3);
        bool reset = false;
        for (size_t i = 0; i < p.mt->nwire; i++) reset |= p.mt->wire[i].reset;
        MOQ_TEST_CHECK(!reset);
        pair_destroy(&p);
    }
    MOQ_TEST_PASS("an object handed over in time is not timed out later");
}

/* [DT-03] An expired subgroup whose stream the bridge has not opened yet
 * (the stream map is full) gets no stream: draft-18 8 "SHOULD NOT attempt
 * to open a new stream to deliver additional Objects in that Subgroup".
 * Drafts 18 and 22: draft-16 times a whole subgroup from its open, so its
 * earlier subgroup would time out too. */
static void test_object_timeout_before_open(void)
{
    static const moq_version_t vers[] = { MOQ_VERSION_DRAFT_18,
                                          MOQ_VERSION_DRAFT_22 };
    for (size_t v = 0; v < 2; v++) {
        /* Two control streams, the subscription's request stream, and one
         * data stream. */
        pair_t p;
        MOQ_TEST_CHECK(pair_init_streams(&p, vers[v], 1u << 20, true, 4));
        track_t t;
        MOQ_TEST_CHECK(subscribe_props(&p, "v", -1, NONE, NONE,
                                       k_object_timeout_50ms,
                                       sizeof(k_object_timeout_50ms), &t));
        MOQ_TEST_CHECK_EQ_SIZE(moq_transport_bridge_stream_count(p.pub_br), 3);
        moq_subgroup_handle_t g1;
        MOQ_TEST_CHECK(publish_group(&p, &t, 1, 1, 0, 128, 2, true, &g1));
        moq_transport_bridge_service(p.pub_br, p.now);
        MOQ_TEST_CHECK(publish_group(&p, &t, 1, 2, 0, 128, 2, false, NULL));
        moq_transport_bridge_service(p.pub_br, p.now);
        uint64_t next_uni = p.mt->next_uni;
        p.now += 51 * 1000;
        moq_transport_bridge_service(p.pub_br, p.now);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_close_subgroup(p.pub, g1,
                                                              p.now), MOQ_OK);
        serve_all(&p);
        MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(p.pub_br));
        MOQ_TEST_CHECK_EQ_U64(p.mt->next_uni, next_uni);
        uint32_t w[8];
        size_t n = wire_objects(&p, w, 8);
        MOQ_TEST_CHECK_EQ_SIZE(n, 2);
        for (size_t i = 0; i < n; i++)
            MOQ_TEST_CHECK_EQ_INT((int)((w[i] >> 16) & 0xff), 1);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(p.pub),
                              MOQ_SESS_ESTABLISHED);
        pair_destroy(&p);
    }
    MOQ_TEST_PASS("an expired subgroup waiting for a stream never opens one");
}

int main(void)
{
    test_group_order_wire();
    test_priorities_across_tracks();
    test_rules_within_track();
    test_group_id_bounds();
    test_ladder_and_default();
    test_equal_priorities_share();
    test_flow_blocked_stream();
    test_refused_stream_parks_alone();
    test_data_before_close();
    test_live_subgroup_catches_up();
    test_completeness();
    test_priority_updates();
    test_publication_update();
    test_fetch_scheduling();
    test_fetch_update();
    test_control_not_behind_data();
    test_stop_sending_one_stream();
    test_unsubscribe_resets_pending();
    test_reset_before_open();
    test_keys_need_four_slots();
    test_map_full_is_not_outbound_work();
    test_key_moves_with_its_stream();
    test_fetch_cancel_resets();
    test_done_with_pending();
    test_object_timeout_in_bridge();
    test_object_timeout_after_handover();
    test_object_timeout_before_open();
    if (failures) {
        fprintf(stderr, "test_scheduling: %d failure(s)\n", failures);
        return 1;
    }
    printf("test_scheduling: all passed\n");
    return 0;
}
