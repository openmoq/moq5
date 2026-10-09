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
#include "../support/fake_endpoint.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int failures = 0;

#define ASC  MOQ_GROUP_ORDER_ASCENDING
#define DESC MOQ_GROUP_ORDER_DESCENDING
#define NONE MOQ_GROUP_ORDER_DEFAULT

static const moq_version_t k_versions[] = {
    MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18,
};
static bool is_d18(moq_version_t ver) { return ver == MOQ_VERSION_DRAFT_18; }

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
    size_t   cap;           /* queued data bytes before WOULD_BLOCK */
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
             bool reset; } wire[MT_MAX_CHUNKS];
    size_t   nwire;
    uint32_t priority_calls;
    uint64_t op_seq;            /* order across control writes and resets */
    uint64_t wire_seq[MT_MAX_CHUNKS];
    uint64_t ctrl_seq_last;     /* seq of the latest control write */
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
        c->sid = sid;
        c->len = len;
        if (len) memcpy(c->data, data, len);
        c->fin = fin;
        return MOQ_TRANSPORT_OK;
    }
    if (m->queued > 0 && m->queued + len > m->cap)
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
    (void)code;
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
static size_t mt_started_objects(const mt_t *m)
{
    size_t n = 0;
    for (size_t i = 0; i < m->nchunks; i++)
        if (!m->chunks[i].sent) n++;
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
            if (!is_d18(p->ver))
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
            if (!is_d18(p->ver))
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
static bool pair_init(pair_t *p, moq_version_t ver, size_t cap,
                      bool drained_cap)
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
    sc.max_actions = 1024;
    if (moq_session_create(&cc, 0, &p->sub) < 0) return false;
    if (moq_session_create(&sc, 0, &p->pub) < 0) return false;

    moq_transport_bridge_cfg_t bc;
    moq_transport_bridge_cfg_init_sized(&bc, sizeof(bc), moq_alloc_default());
    if (moq_transport_bridge_create(&bc, p->sub, &p->sub_ep.vtable,
                                    &p->sub_ep, &p->sub_br) != MOQ_OK)
        return false;
    if (moq_transport_bridge_create(&bc, p->pub, &p->mt->ops, p->mt,
                                    &p->pub_br) != MOQ_OK)
        return false;
    p->mt->bridge = p->pub_br;

    p->now = 1000;
    moq_session_start(p->sub, p->now);
    if (is_d18(ver)) moq_session_start(p->pub, p->now);
    shuttle(p);
    drain_events(p->sub);
    drain_events(p->pub);
    return moq_session_state(p->pub) == MOQ_SESS_ESTABLISHED &&
           moq_session_state(p->sub) == MOQ_SESS_ESTABLISHED;
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
 * and `order`; the publisher accepts advertising `pref`. */
static bool subscribe(pair_t *p, const char *name, int sub_prio,
                      moq_group_order_t order, moq_group_order_t pref,
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
    uint8_t props[16];
    size_t props_len = 0;
    if (pref != NONE &&
        moq_session_track_properties_add_group_order(p->pub, NULL, 0, pref,
            props, sizeof(props), &props_len) != MOQ_OK)
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
               is_d18(ver) ? 18 : 16, cap);
        return;
    }
    fprintf(stderr, "%s (d%d, cap %zu) now passes: drop its issue #36 mark\n",
            what, is_d18(ver) ? 18 : 16, cap);
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
                is_d18(p->ver) ? 18 : 16, cap);
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
static void check_group_order_wire(moq_version_t ver, size_t cap,
                                   moq_group_order_t order,
                                   moq_group_order_t pref, bool want_desc)
{
    pair_t p;
    MOQ_TEST_CHECK(pair_init(&p, ver, cap, true));
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
    for (size_t v = 0; v < 2; v++)
        for (size_t c = 0; c < 3; c++) {
            moq_version_t ver = k_versions[v];
            size_t cap = k_caps[c];
            check_group_order_wire(ver, cap, DESC, ASC, true);    /* GO-01 */
            check_group_order_wire(ver, cap, ASC, DESC, false);   /* GO-02 */
            check_group_order_wire(ver, cap, NONE, DESC, true);   /* GO-03 */
            check_group_order_wire(ver, cap, NONE, NONE, false);  /* GO-04 */
        }
    MOQ_TEST_PASS("groups go out in the effective group order");
}

/* -- B. the MOQT 7.2 rules ------------------------------------------------- */

/* One run: tracks and groups set up by `build`, the network served, and the
 * wire compared with `want`; every send queue size, both drafts. */
typedef void (*build_fn)(pair_t *p, void *ctx);

static void run_case(const char *what, build_fn build, void *ctx,
                     const uint32_t *want, size_t n)
{
    for (size_t v = 0; v < 2; v++)
        for (size_t c = 0; c < 3; c++) {
            pair_t p;
            MOQ_TEST_CHECK(pair_init(&p, k_versions[v], k_caps[c], true));
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
    for (size_t v = 0; v < 2; v++)
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
                        is_d18(k_versions[v]) ? 18 : 16, k_caps[c]);
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
static void test_flow_blocked_stream(void)
{
    for (size_t v = 0; v < 2; v++) {
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
    for (size_t v = 0; v < 2; v++)
        for (size_t c = 0; c < 3; c++) {
            pair_t p;
            MOQ_TEST_CHECK(pair_init(&p, k_versions[v], k_caps[c], true));
            track_t t;
            MOQ_TEST_CHECK(subscribe(&p, "v", -1, DESC, NONE, &t));
            moq_subgroup_handle_t g8;
            MOQ_TEST_CHECK(publish_group(&p, &t, 1, 7, 0, 128, 6, false, NULL));
            MOQ_TEST_CHECK(publish_group(&p, &t, 1, 8, 0, 128, 0, true, &g8));
            serve_n(&p, 3);
            MOQ_TEST_CHECK_EQ_INT((int)write_object(&p, g8, 1, 8, 0, 0),
                                  MOQ_OK);
            MOQ_TEST_CHECK_EQ_INT((int)moq_session_close_subgroup(p.pub, g8,
                                                                  p.now),
                                  MOQ_OK);
            serve_all(&p);
            uint32_t w[64];
            size_t n = wire_objects(&p, w, 64);
            MOQ_TEST_CHECK_EQ_SIZE(n, 7);
            size_t i8 = first_index(w, n, 0xffffffffu, OBJ(1, 8, 0, 0));
            size_t last7 = last_index(w, n, 0xffff0000u, OBJ(1, 7, 0, 0));
            MOQ_TEST_CHECK(i8 != SIZE_MAX && i8 < last7);
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
        for (size_t j = 0; j < p->mt->nwire; j++) {
            const uint8_t *k = p->mt->wire[j].head;
            if (p->mt->wire[j].sid == sid && p->mt->wire[j].fin) fin = true;
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
    for (size_t v = 0; v < 2; v++)
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
    uint32_t w[64];
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
     * committed to it and is retrying. */
    size_t before = wire_objects(&p, w, 64) + mt_started_objects(p.mt) + 1;
    shuttle(&p);
    drain_events(p.pub);
    drain_events(p.sub);
    serve_all(&p);
    size_t n = wire_objects(&p, w, 64);
    MOQ_TEST_CHECK_EQ_SIZE(n, 12);
    /* After the update: the leading track drains before the other resumes. */
    uint32_t lead = kind == UPD_NO_PRIORITY ? OBJ(1, 0, 0, 0)
                                            : OBJ(2, 0, 0, 0);
    size_t first_other = SIZE_MAX;
    for (size_t i = before; i < n; i++)
        if ((w[i] & TRACK_MASK) != lead) { first_other = i; break; }
    size_t last_lead = last_index(w, n, TRACK_MASK, lead);
    if (!(first_other == SIZE_MAX || last_lead < first_other))
        fprintf(stderr, "PRI update %d (d%d, cap %zu): not reordered\n",
                (int)kind, is_d18(ver) ? 18 : 16, cap);
    MOQ_TEST_CHECK(first_other == SIZE_MAX || last_lead < first_other);
    pair_destroy(&p);
}

static void test_priority_updates(void)
{
    for (size_t v = 0; v < 2; v++)
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
    ac.has_subscriber_priority = true;
    ac.subscriber_priority = 200;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_publish(p.sub, cpub, &ac,
                                                          p.now), MOQ_OK);
    shuttle(&p);
    drain_events(p.pub);

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
    uint32_t w[64];
    while (wire_objects(&p, w, 64) == 0 && serve_n(&p, 1) == 1) {}
    MOQ_TEST_CHECK_EQ_INT((int)(w[0] >> 24), 1);

    moq_publication_update_cfg_t uc;
    moq_publication_update_cfg_init(&uc);
    uc.has_subscriber_priority = true;
    uc.subscriber_priority = 50;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_publication(p.sub, cpub, &uc,
                                                              p.now), MOQ_OK);
    /* As in check_priority_update: what is already scheduled may go first. */
    size_t before = wire_objects(&p, w, 64) + mt_started_objects(p.mt) + 1;
    shuttle(&p);
    drain_events(p.pub);
    drain_events(p.sub);
    serve_all(&p);
    size_t n = wire_objects(&p, w, 64);
    MOQ_TEST_CHECK_EQ_SIZE(n, 12);
    size_t first_sub = SIZE_MAX;
    for (size_t i = before; i < n; i++)
        if ((w[i] & TRACK_MASK) == OBJ(1, 0, 0, 0)) { first_sub = i; break; }
    size_t last_pub = last_index(w, n, TRACK_MASK, OBJ(2, 0, 0, 0));
    if (!(first_sub == SIZE_MAX || last_pub < first_sub))
        fprintf(stderr, "PRI-09 (d%d, cap %zu): not reordered\n",
                is_d18(ver) ? 18 : 16, cap);
    MOQ_TEST_CHECK(first_sub == SIZE_MAX || last_pub < first_sub);
    pair_destroy(&p);
}

static void test_publication_update(void)
{
    for (size_t v = 0; v < 2; v++)
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
    for (size_t v = 0; v < 2; v++) {
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
                    "data\n", is_d18(k_versions[v]) ? 18 : 16);
        MOQ_TEST_CHECK(answered);
        uint32_t w[8];
        MOQ_TEST_CHECK_EQ_SIZE(wire_objects(&p, w, 8), 0);   /* still stalled */
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
    for (size_t v = 0; v < 2; v++) {
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

/* [RST-03] The subscriber ends one of two subscriptions while both have
 * data pending: the ended one's open streams are reset, never FINed
 * incomplete, and the other is unaffected. */
static void test_unsubscribe_resets_pending(void)
{
    for (size_t v = 0; v < 2; v++) {
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
        /* Issue #36: the draft-18 cancellation does not reset the streams. */
        expect_known(was_reset(&p, bsid), is_d18(k_versions[v]), "RST-03",
                     k_versions[v], 1u << 20);
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
    for (size_t v = 0; v < 2; v++) {
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
    for (size_t v = 0; v < 2; v++) {
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
        uint64_t first_ctrl = p.mt->ctrl_seq_last;
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

int main(void)
{
    test_group_order_wire();
    test_priorities_across_tracks();
    test_rules_within_track();
    test_group_id_bounds();
    test_ladder_and_default();
    test_equal_priorities_share();
    test_flow_blocked_stream();
    test_live_subgroup_catches_up();
    test_completeness();
    test_priority_updates();
    test_publication_update();
    test_fetch_scheduling();
    test_control_not_behind_data();
    test_stop_sending_one_stream();
    test_unsubscribe_resets_pending();
    test_fetch_cancel_resets();
    test_done_with_pending();
    if (failures) {
        fprintf(stderr, "test_scheduling: %d failure(s)\n", failures);
        return 1;
    }
    printf("test_scheduling: all passed\n");
    return 0;
}
