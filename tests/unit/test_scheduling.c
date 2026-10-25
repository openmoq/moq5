/*
 * Data scheduling through the transport bridge, draft-16/18 (MOQT 7.2)
 * and draft-22 (5.1.2). The cases come from a test plan written from the drafts alone,
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
 * Ordering is a SHOULD in these drafts; this model is strict, so the cases
 * assert strict order where the plan allows it.
 */
#include <moq/moq.h>
#include <moq/codec.h>
#include <moq/transport_bridge.h>
#include "test_support.h"
#include "test_binding_support.h"
#include "../support/fake_endpoint.h"
#include "../support/occupancy_audit.h"
#include "../../core/src/session/session_internal.h"
#include "../../core/src/bridge/transport_bridge_internal.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int failures = 0;

/* Every model transport advertises MOQ_TRANSPORT_CAP_SHARED_ADMISSION
 * (--shared-all). Its write refuses data only at its one queue cap, as the
 * capability requires, unless a row sets a per-stream refusal; rows that do
 * are not run with it. */
static bool g_shared_all = false;
/* Data writes refused at the cap while the capability was advertised: that
 * the --shared-all rows reach a spent budget at all. */
static uint32_t g_shared_cap_refusals = 0;

/* Pool sizes both sessions of the next pair are created with (0: the
 * session's default). A row sizes them, makes its pair and puts them back. */
typedef struct {
    uint32_t subscriptions, subgroups, publishes;
    uint64_t done_wait_us;   /* the PUBLISH_DONE stream wait (0: default) */
} pools_t;
static pools_t g_pools;

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
    uint64_t accepted_seq;
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
    bool     refuse_payload; /* accept headers, refuse marked object bodies */
    bool     refuse_fin;     /* refuse only the stream's normal FIN */
    uint32_t write_attempts;
    uint32_t fin_attempts;
    size_t   first_write_len;
    uint8_t  first_write_head[PAYLOAD_LEN]; /* includes a refused header */
    uint32_t refused_writes;
    uint32_t writes_after_reset;
    bool     refuse_reset;
    uint32_t reset_attempts;
    uint64_t reset_code;
    uint64_t reset_reliable;
} mt_stream_t;

typedef struct {
    uint64_t sid;
    uint64_t accepted_seq;
    uint8_t  data[512];
    size_t   len;
    bool     fin;
} mt_ctrl_t;

/* Detached-retry observation: copied inside an earlier stream's endpoint
 * write, with no callback into the bridge and no product-state mutation. */
typedef struct {
    uint64_t sid;
    size_t fill_payload_writes;
    moq_action_meta_t fill_order_meta;
    uint32_t sibling_pos, sibling_key;
    bool fill_found, sibling_found, sibling_keyed, sibling_held;
    bool fill_order_active;
} mt_retry_sample_t;

typedef struct {
    bool armed;
    uint64_t earlier_sid[2], fill_sid, sibling_sid;
    moq_stream_ref_t fill_ref, sibling_ref;
    size_t sample_count, fill_payload_writes;
    mt_retry_sample_t samples[2];
} mt_retry_probe_t;

typedef struct {
    moq_transport_endpoint_ops_t ops;
    moq_transport_bridge_t *bridge;
    uint64_t next_uni, next_bidi;
    size_t   cap;           /* queued data bytes that refuse further writes */
    size_t   queued;
    bool     drained_cap;   /* advertise and call the drained notice */
    bool     refuse_control;
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
    uint32_t refuse_run;        /* refused writes since the last acceptance */
    uint32_t worst_refuse_run;
    uint32_t bidi_keyed;        /* keys set on request/control bidi streams */
    uint64_t op_seq;            /* endpoint acceptance: data, control, resets */
    uint64_t wire_seq[MT_MAX_CHUNKS];
    uint64_t ctrl_seq_last;     /* seq of the latest control write */
    uint64_t ctrl_mark;         /* seq after which ctrl_first_after counts */
    uint64_t ctrl_first_after;  /* seq of the first control write past it */
    /* Read-only capture at the first reset attempt, before admission can
     * drain surviving core data. Used by the wrapped-ring retention row. */
    const moq_session_t *capture_core;
    moq_stream_ref_t capture_ref;
    moq_action_meta_t captured_meta[8];
    size_t captured_count;
    mt_retry_probe_t retry_probe;
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

static const bridge_dstream_t *mt_retry_dstream(const moq_transport_bridge_t *b,
                                               moq_stream_ref_t ref)
{
    for (uint32_t i = 0; i < b->dstream_cap; i++)
        if (b->dstreams[i].active && b->dstreams[i].ref._v == ref._v)
            return &b->dstreams[i];
    return NULL;
}

static void mt_retry_observe(mt_t *m, uint64_t sid, const uint8_t *data, size_t len)
{
    mt_retry_probe_t *p = &m->retry_probe;
    if (!p->armed || len != PAYLOAD_LEN || !data || data[0] != PAYLOAD_TAG) return;
    if (sid == p->fill_sid) {
        p->fill_payload_writes++;
        return;
    }
    if (sid != p->earlier_sid[0] && sid != p->earlier_sid[1]) return;
    const size_t at = p->sample_count++;
    if (at >= sizeof(p->samples) / sizeof(p->samples[0])) return;
    mt_retry_sample_t *sample = &p->samples[at];
    sample->sid = sid;
    sample->fill_payload_writes = p->fill_payload_writes;
    const bridge_dstream_t *f = mt_retry_dstream(m->bridge, p->fill_ref);
    const bridge_dstream_t *s = mt_retry_dstream(m->bridge, p->sibling_ref);
    sample->fill_found = f != NULL;
    sample->sibling_found = s != NULL;
    if (f) {
        sample->fill_order_meta = f->order_meta;
        sample->fill_order_active = f->order_active;
    }
    if (s) sample->sibling_pos = s->pos;
    for (size_t i = 0; i < MT_MAX_STREAMS; i++) {
        const mt_stream_t *st = &m->streams[i];
        if (st->used && st->sid == p->sibling_sid) {
            sample->sibling_keyed = st->keyed;
            sample->sibling_key = st->key;
        }
    }
    for (size_t i = 0; i < m->nchunks; i++) {
        const mt_chunk_t *c = &m->chunks[i];
        if (!c->sent && c->sid == p->sibling_sid && c->len == PAYLOAD_LEN &&
            c->head[0] == PAYLOAD_TAG) sample->sibling_held = true;
    }
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
        if (m->refuse_control) return MOQ_TRANSPORT_WOULD_BLOCK;
        if (m->nctrl == MT_MAX_CTRL || len > sizeof(m->ctrl[0].data))
            return MOQ_TRANSPORT_ERROR;
        mt_ctrl_t *c = &m->ctrl[m->nctrl++];
        m->refuse_run = 0;
        m->ctrl_seq_last = ++m->op_seq;
        c->accepted_seq = m->ctrl_seq_last;
        if (m->ctrl_mark && !m->ctrl_first_after &&
            m->ctrl_seq_last > m->ctrl_mark)
            m->ctrl_first_after = m->ctrl_seq_last;
        c->sid = sid;
        c->len = len;
        if (len) memcpy(c->data, data, len);
        c->fin = fin;
        return MOQ_TRANSPORT_OK;
    }
    mt_retry_observe(m, sid, data, len);
    if (st->write_attempts == 0) {
        st->first_write_len = len;
        if (len) memcpy(st->first_write_head, data,
                        len < PAYLOAD_LEN ? len : PAYLOAD_LEN);
    }
    st->write_attempts++;
    if (fin) st->fin_attempts++;
    /* A shared queue bounded as the picoquic adapter's is (a low-water
     * mark), plus a per-stream budget as msquic's. */
    if (st->reset) st->writes_after_reset++;
    const bool at_cap = len > 0 && m->queued >= m->cap;
    if (at_cap && (m->ops.capabilities & MOQ_TRANSPORT_CAP_SHARED_ADMISSION))
        g_shared_cap_refusals++;
    if (at_cap || st->refuse ||
        (fin && st->refuse_fin) ||
        (st->refuse_payload && len == PAYLOAD_LEN && data && data[0] == PAYLOAD_TAG)) {
        st->refused_writes++;
        if (++m->refuse_run > m->worst_refuse_run)
            m->worst_refuse_run = m->refuse_run;
        return MOQ_TRANSPORT_WOULD_BLOCK;
    }
    if (m->nchunks == MT_MAX_CHUNKS) return MOQ_TRANSPORT_ERROR;
    m->refuse_run = 0;
    mt_chunk_t *c = &m->chunks[m->nchunks++];
    memset(c, 0, sizeof(*c));
    c->sid = sid;
    c->accepted_seq = ++m->op_seq;
    c->len = len;
    if (len) memcpy(c->head, data, len < PAYLOAD_LEN ? len : PAYLOAD_LEN);
    c->fin = fin;
    m->queued += len;
    return MOQ_TRANSPORT_OK;
}

static moq_transport_result_t mt_reset(void *ctx, uint64_t sid, uint64_t code)
{
    mt_t *m = ctx;
    mt_stream_t *st = mt_stream(m, sid);
    if (!st) return MOQ_TRANSPORT_ERROR;
    if (m->capture_core && st->reset_attempts == 0) {
        const moq_session_t *s = m->capture_core;
        for (size_t i = s->action_head; i < s->action_tail; ++i) {
            const size_t idx = i % s->action_cap;
            const moq_action_t *a = &s->actions[idx];
            if (a->kind == MOQ_ACTION_SEND_DATA &&
                a->u.send_data.stream_ref._v == m->capture_ref._v &&
                m->captured_count < sizeof(m->captured_meta) / sizeof(m->captured_meta[0]))
                m->captured_meta[m->captured_count++] = s->action_meta[idx];
        }
    }
    st->reset_attempts++;
    st->reset_code = code;
    if (st->refuse_reset) return MOQ_TRANSPORT_WOULD_BLOCK;
    m->refuse_run = 0;
    for (size_t i = 0; i < m->nchunks; i++)
        if (m->chunks[i].sid == sid && !m->chunks[i].sent) {
            m->chunks[i].sent = true;
            m->queued -= m->chunks[i].len;
        }
    st->reset = true;
    if (m->nwire < MT_MAX_CHUNKS) {
        memset(&m->wire[m->nwire], 0, sizeof(m->wire[0]));
        m->wire[m->nwire].sid = sid;
        m->wire[m->nwire].reset = true;
        m->wire_seq[m->nwire] = ++m->op_seq;
        m->nwire++;
    }
    return MOQ_TRANSPORT_OK;
}

static moq_transport_result_t mt_reset_at(void *ctx, uint64_t sid,
                                         uint64_t code, uint64_t reliable)
{
    mt_t *m = ctx;
    mt_stream_t *st = mt_stream(m, sid);
    if (!st) return MOQ_TRANSPORT_ERROR;
    st->reset_reliable = reliable;
    return mt_reset(ctx, sid, code);
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
        (drained_cap ? MOQ_TRANSPORT_CAP_STREAM_DRAINED : 0) |
        (g_shared_all ? MOQ_TRANSPORT_CAP_SHARED_ADMISSION : 0);
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

/* The session's queued actions with a delivery deadline, and those that are
 * not data, counted from its ring: what its summary (action_timed,
 * action_nondata) must hold. Data is a datagram, a SEND_DATA, or a stream's
 * own key; an owner-wide key, a reset and every control action are not. */
static void session_queue_recount(const moq_session_t *s, size_t *timed,
                                  size_t *nondata)
{
    *timed = *nondata = 0;
    for (size_t i = s->action_head; i < s->action_tail; i++) {
        const moq_action_t *a = &s->actions[i % s->action_cap];
        if (s->action_meta[i % s->action_cap].deadline_us != UINT64_MAX)
            ++*timed;
        const bool data = a->kind == MOQ_ACTION_SEND_DATAGRAM ||
            a->kind == MOQ_ACTION_SEND_DATA ||
            (a->kind == MOQ_ACTION_SET_DATA_PRIORITY &&
             a->u.set_data_priority.stream_ref._v != 0);
        if (!data) ++*nondata;
    }
}

#define CHECK_SESSION_COUNTS(s) do { \
    size_t timed_, nondata_; \
    session_queue_recount((s), &timed_, &nondata_); \
    MOQ_TEST_CHECK_EQ_INT((int)(s)->action_timed, (int)timed_); \
    MOQ_TEST_CHECK_EQ_INT((int)(s)->action_nondata, (int)nondata_); \
} while (0)

/* Peer uni bytes. These scenarios never run out of receive entries, so an
 * unconsumed chunk (which the endpoint would have to hold) is a failure. */
static void feed_uni(moq_transport_bridge_t *b, uint64_t sid,
                     const uint8_t *data, size_t len, bool fin, uint64_t now)
{
    MOQ_TEST_CHECK(moq_transport_bridge_on_peer_uni_bytes(b, sid, data, len,
                                                          fin, now) !=
                   MOQ_ERR_INPUT_NOT_CONSUMED);
}

/* The subscriber's bridge is serviced and what it wrote reaches the
 * publisher's bridge, which is not serviced: the first half of a shuttle
 * round. Returns how many operations moved. */
static size_t shuttle_sub_to_pub(pair_t *p)
{
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
    return moved;
}

static void shuttle(pair_t *p)
{
    for (int round = 0; round < 64; round++) {
        size_t moved = shuttle_sub_to_pub(p);
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
        if (!moved) break;
    }
    CHECK_SESSION_COUNTS(p->pub);
    CHECK_SESSION_COUNTS(p->sub);
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
static bool pair_init_retained(pair_t *p, moq_version_t ver, size_t cap,
                               bool drained_cap, uint32_t max_streams,
                               uint32_t max_actions, uint64_t retained_cap,
                               uint32_t max_pending, bool reliable_reset)
{
    memset(p, 0, sizeof(*p));
    p->ver = ver;
    p->mt = calloc(1, sizeof(*p->mt));
    if (!p->mt) return false;
    mt_init(p->mt, cap, drained_cap);
    if (reliable_reset) {
        p->mt->ops.capabilities |= MOQ_TRANSPORT_CAP_RELIABLE_RESET;
        p->mt->ops.reset_stream_at = mt_reset_at;
    }
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
    cc.max_subscriptions = sc.max_subscriptions = g_pools.subscriptions;
    cc.max_open_subgroups = sc.max_open_subgroups = g_pools.subgroups;
    cc.max_publishes = sc.max_publishes = g_pools.publishes;
    cc.done_wait_timeout_us = sc.done_wait_timeout_us = g_pools.done_wait_us;
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
    bc.max_retained_bytes = retained_cap;
    bc.max_pending = max_pending;
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

static bool pair_init_limits(pair_t *p, moq_version_t ver, size_t cap,
                             bool drained_cap, uint32_t max_streams,
                             uint32_t max_actions)
{
    return pair_init_retained(p, ver, cap, drained_cap, max_streams,
                              max_actions, 0, 0, false);
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

/* Queued data items with a delivery deadline, counted from the queues: what
 * the bridge's dq_timed must hold. */
static uint32_t dq_timed_recount(const moq_transport_bridge_t *b)
{
    uint32_t n = 0;
    for (uint32_t i = 0; i < b->dstream_cap; i++) {
        const bridge_dstream_t *d = &b->dstreams[i];
        for (uint32_t j = 0; d->active && j < d->q_len; j++)
            if (d->q[(d->q_head + j) % d->q_cap].meta.deadline_us != UINT64_MAX)
                n++;
    }
    return n;
}

/* Active dstreams holding a deferred reset, counted from the table: what
 * the bridge's dreset_count must hold. */
static uint32_t dreset_recount(const moq_transport_bridge_t *b)
{
    uint32_t n = 0;
    for (uint32_t i = 0; i < b->dstream_cap; i++)
        if (b->dstreams[i].active && b->dstreams[i].has_reset) n++;
    return n;
}

/* Both table counts against their recounts, and the session's summary. */
#define CHECK_TABLE_COUNTS(br) do { \
    MOQ_TEST_CHECK_EQ_INT((int)(br)->dq_timed, (int)dq_timed_recount(br)); \
    MOQ_TEST_CHECK_EQ_INT((int)(br)->dreset_count, (int)dreset_recount(br)); \
    CHECK_SESSION_COUNTS((br)->session); \
} while (0)

/* Serve the network until nothing is left, servicing the publisher's
 * bridge after every write so it can refill. The bridge's counts of timed
 * items and deferred resets must then match its table. */
static void serve_all(pair_t *p)
{
    for (int guard = 0; guard < 100000; guard++) {
        moq_transport_bridge_service(p->pub_br, p->now);
        if (!mt_serve_one(p->mt)) {
            moq_transport_bridge_service(p->pub_br, p->now);
            if (!mt_serve_one(p->mt)) break;
        }
    }
    CHECK_TABLE_COUNTS(p->pub_br);
}

typedef struct {
    moq_subscription_t sub;   /* the publisher's handle */
    moq_subscription_t csub;  /* the subscriber's handle */
} track_t;

/* The subscriber subscribes to "live"/`name` with `sub_prio` (-1: omitted)
 * and `order`; the publisher accepts advertising `pref`. */
static bool subscribe_timed(pair_t *p, const char *name, int sub_prio,
                            moq_group_order_t order, moq_group_order_t pref,
                            uint64_t object_timeout_ms, track_t *out)
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
    uint8_t timeout[16], props[32];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, timeout, sizeof(timeout));
    if (object_timeout_ms != 0 &&
        (moq_buf_write_vi64(&w, 2) != MOQ_OK ||
         moq_buf_write_vi64(&w, object_timeout_ms) != MOQ_OK))
        return false;
    const size_t timeout_len = moq_buf_writer_offset(&w);
    size_t props_len = 0;
    if (pref != NONE) {
        if (moq_session_track_properties_add_group_order(p->pub, timeout,
            timeout_len, pref,
            props, sizeof(props), &props_len) != MOQ_OK)
            return false;
    } else {
        memcpy(props, timeout, timeout_len);
        props_len = timeout_len;
    }
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
    return subscribe_timed(p, name, sub_prio, order, pref, 0, out);
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

#define OBJ(track, group, subgroup, object) \
    ((uint32_t)(track) << 24 | (uint32_t)(group) << 16 | \
     (uint32_t)(subgroup) << 8 | (uint32_t)(object))

/* The send queue sizes every scenario runs with: tiny (the bridge orders
 * almost everything), a few writes, and room for everything (the transport
 * orders everything). */
static const size_t k_caps[] = { 1, 64, 1u << 20 };

/* Check the wire carried exactly `want`, in order. */
static void expect_wire(const pair_t *p, const uint32_t *want, size_t n,
                        const char *what, size_t cap)
{
    uint32_t got[512];
    size_t m = wire_objects(p, got, 512);
    bool same = m == n;
    for (size_t i = 0; same && i < n; i++) same = got[i] == want[i];
    if (!same) {
        fprintf(stderr, "%s (d%d, cap %zu): wire", what,
                draft_of(p->ver), cap);
        for (size_t i = 0; i < m; i++) fprintf(stderr, " %08x", got[i]);
        fprintf(stderr, "\n  want");
        for (size_t i = 0; i < n; i++) fprintf(stderr, " %08x", want[i]);
        fprintf(stderr, "\n");
    }
    MOQ_TEST_CHECK(same);
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
    const bool increasing = ctx && *(const bool *)ctx;
    track_t s;
    fetch_t f;
    MOQ_TEST_CHECK(subscribe(p, "s", 128, NONE, NONE, &s));
    MOQ_TEST_CHECK(start_fetch(p, "f", 128, &f));
    MOQ_TEST_CHECK(publish_group(p, &s, 1, 1, 0, 100, 2, false, NULL));
    MOQ_TEST_CHECK(fetch_object(p, &f, 2, 3, 0, increasing ? 200 : 10));
    MOQ_TEST_CHECK(fetch_object(p, &f, 2, 3, 1, increasing ? 200 : 10));
    MOQ_TEST_CHECK(fetch_object(p, &f, 2, 4, 0, increasing ? 10 : 200));
    MOQ_TEST_CHECK(fetch_object(p, &f, 2, 4, 1, increasing ? 10 : 200));
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
    /* A later urgent object must not promote the earlier low-priority bytes
     * on that same FETCH stream. The subscription can drain first. */
    static const uint32_t w06[] = {
        OBJ(1, 1, 0, 0), OBJ(1, 1, 0, 1), OBJ(2, 3, 0, 0), OBJ(2, 3, 0, 1),
        OBJ(2, 4, 0, 0), OBJ(2, 4, 0, 1),
    };
    bool increasing = true;
    run_case("FET-06", build_fet04, &increasing, w06, 6);
    static const uint32_t w05[] = {
        OBJ(2, 3, 0, 0), OBJ(2, 3, 0, 1), OBJ(1, 1, 0, 0), OBJ(1, 1, 0, 1),
    };
    run_case("FET-05", build_fet05, NULL, w05, 4);
    MOQ_TEST_PASS("FETCH responses scheduled by their next object");
}

/* A FETCH waiting for its old priority class must neither hold up a healthy
 * sibling nor prevent cancellation. Exercise both release paths on every
 * draft. No drained-notice endpoints must also complete, without an ordering
 * promise their interface cannot support. */
static void test_fetch_priority_boundary(void)
{
    for (size_t v = 0; v < N_VERSIONS; v++)
        for (unsigned row = 0; row < 3; ++row) {
            const bool cancel = row == 1;
            const bool drained = row != 2;
            pair_t p;
            MOQ_TEST_CHECK(pair_init(&p, k_versions[v], 1u << 20, drained));
            track_t s;
            fetch_t f;
            MOQ_TEST_CHECK(subscribe(&p, "s", 128, NONE, NONE, &s));
            MOQ_TEST_CHECK(start_fetch(&p, "f", 128, &f));
            MOQ_TEST_CHECK(publish_group(&p, &s, 1, 1, 0, 100, 2, false, NULL));
            for (uint64_t o = 0; o < 4; ++o)
                MOQ_TEST_CHECK(fetch_object(&p, &f, 2, 3, o, o < 2 ? 10 : 200));
            moq_transport_bridge_service(p.pub_br, p.now);
            uint64_t sid = 0;
            MOQ_TEST_CHECK(stream_of(&p, 2, 3, &sid));
            mt_stream_t *st = mt_stream(p.mt, sid);
            MOQ_TEST_CHECK(st != NULL);
            if (!st) { pair_destroy(&p); continue; }
            if (drained) {
                size_t held = 0;
                for (size_t i = 0; i < p.mt->nchunks; ++i) {
                    const mt_chunk_t *c = &p.mt->chunks[i];
                    if (c->sid == sid && !c->sent && c->len == PAYLOAD_LEN &&
                        c->head[0] == PAYLOAD_TAG)
                        held++;
                }
                MOQ_TEST_CHECK_EQ_SIZE(held, 2);
                MOQ_TEST_CHECK_EQ_INT((int)((st->key >> 16) & 0xffu), 10);
                MOQ_TEST_CHECK(moq_transport_bridge_has_pending(p.pub_br));
                /* It awaits this stream's drain, not generic write readiness. */
                MOQ_TEST_CHECK(!moq_transport_bridge_has_outbound_pending(p.pub_br));
            }
            st->blocked = true;
            serve_all(&p);
            const uint32_t sibling[] = { OBJ(1, 1, 0, 0), OBJ(1, 1, 0, 1) };
            expect_wire(&p, sibling, 2, "FETCH blocked sibling", 1u << 20);
            if (cancel) {
                MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch_cancel(
                    p.sub, f.cfetch, p.now), MOQ_OK);
                shuttle(&p);
                drain_events(p.pub);
                shuttle(&p);
                MOQ_TEST_CHECK(st->reset);
                MOQ_TEST_CHECK_EQ_INT((int)st->reset_attempts, 1);
                expect_wire(&p, sibling, 2, "FETCH cancelled boundary", 1u << 20);
            } else {
                st->blocked = false;
                if (drained) {
                    for (unsigned i = 0; i < 32 && p.mt->queued > 0; ++i)
                        MOQ_TEST_CHECK(mt_serve_one(p.mt));
                    MOQ_TEST_CHECK_EQ_SIZE(p.mt->queued, 0);
                    /* The notice alone makes the waiting priority eligible. */
                    MOQ_TEST_CHECK(moq_transport_bridge_has_outbound_pending(p.pub_br));
                }
                MOQ_TEST_CHECK_EQ_INT((int)moq_session_end_fetch(
                    p.pub, f.fetch, p.now), MOQ_OK);
                serve_all(&p);
                const uint32_t complete[] = {
                    OBJ(1, 1, 0, 0), OBJ(1, 1, 0, 1), OBJ(2, 3, 0, 0),
                    OBJ(2, 3, 0, 1), OBJ(2, 3, 0, 2), OBJ(2, 3, 0, 3)
                };
                expect_wire(&p, complete, 6, "FETCH resumed boundary", 1u << 20);
            }
            /* A duplicate/late notice cannot resurrect a completed stream. */
            moq_transport_bridge_on_stream_drained(p.pub_br, sid);
            moq_transport_bridge_service(p.pub_br, p.now);
            MOQ_TEST_CHECK(!moq_transport_bridge_has_outbound_pending(p.pub_br));
            MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(p.pub_br));
            pair_destroy(&p);
        }
    MOQ_TEST_PASS("FETCH priority boundaries preserve sibling and terminal progress");
}

/* -- E. control and resets ---------------------------------------------------- */

/* Draining an object's accepted header can clear the endpoint key while its
 * refused payload still belongs to the pending FIFO. On retry, the accepted
 * payload must reestablish the old class before the next object is eligible.
 * Rows: demotion, promotion, same-class batching, no-drain fallback. */
static void test_fetch_priority_payload_retry(void)
{
    for (size_t v = 0; v < N_VERSIONS; ++v)
        for (unsigned row = 0; row < 4; ++row) {
            const bool drained = row != 3;
            const uint8_t first = row == 1 ? 200 : 10;
            const uint8_t next = row == 0 || row == 3 ? 200 : 10;
            pair_t p;
            MOQ_TEST_CHECK(pair_init(&p, k_versions[v], 1u << 20, drained));
            track_t s;
            fetch_t f;
            MOQ_TEST_CHECK(subscribe(&p, "s", 128, NONE, NONE, &s));
            MOQ_TEST_CHECK(start_fetch(&p, "f", 128, &f));
            serve_all(&p); /* drain only the FETCH preamble */
            mt_stream_t *st = first_data_stream(p.mt);
            MOQ_TEST_CHECK(st != NULL);
            if (!st) { pair_destroy(&p); continue; }
            p.mt->cap = 1;
            MOQ_TEST_CHECK(fetch_object(&p, &f, 2, 3, 0, first));
            MOQ_TEST_CHECK(fetch_object(&p, &f, 2, 4, 0, next));
            moq_transport_bridge_service(p.pub_br, p.now);
            MOQ_TEST_CHECK_EQ_SIZE(mt_held_objects(p.mt), 0);
            MOQ_TEST_CHECK_EQ_INT((int)p.pub_br->pending_count, 1);
            MOQ_TEST_CHECK(p.mt->queued > 0);
            MOQ_TEST_CHECK(mt_serve_one(p.mt)); /* the actual object header */
            MOQ_TEST_CHECK_EQ_SIZE(p.mt->queued, 0);

            p.mt->cap = 1u << 20;
            MOQ_TEST_CHECK(publish_group(&p, &s, 1, 1, 0, 100, 2, false, NULL));
            moq_transport_bridge_service(p.pub_br, p.now);
            size_t held = 0;
            for (size_t i = 0; i < p.mt->nchunks; ++i) {
                const mt_chunk_t *c = &p.mt->chunks[i];
                if (!c->sent && c->sid == st->sid && c->len == PAYLOAD_LEN &&
                    c->head[0] == PAYLOAD_TAG)
                    held++;
            }
            if (drained) {
                MOQ_TEST_CHECK_EQ_INT((int)((st->key >> 16) & 0xffu), first);
                MOQ_TEST_CHECK_EQ_SIZE(held, first == next ? 2 : 1);
            }
            MOQ_TEST_CHECK_EQ_INT((int)moq_session_end_fetch(
                p.pub, f.fetch, p.now), MOQ_OK);
            serve_all(&p);
            if (drained) {
                const uint32_t demote[] = { OBJ(2, 3, 0, 0), OBJ(1, 1, 0, 0),
                    OBJ(1, 1, 0, 1), OBJ(2, 4, 0, 0) };
                const uint32_t promote[] = { OBJ(1, 1, 0, 0), OBJ(1, 1, 0, 1),
                    OBJ(2, 3, 0, 0), OBJ(2, 4, 0, 0) };
                const uint32_t equal[] = { OBJ(2, 3, 0, 0), OBJ(2, 4, 0, 0),
                    OBJ(1, 1, 0, 0), OBJ(1, 1, 0, 1) };
                expect_wire(&p, row == 0 ? demote : row == 1 ? promote : equal,
                            4, "FETCH split payload", 1u << 20);
            } else {
                uint32_t w[8];
                size_t n = wire_objects(&p, w, 8), seen = 0;
                MOQ_TEST_CHECK_EQ_SIZE(n, 4);
                for (size_t i = 0; i < n; ++i)
                    if ((w[i] & TRACK_MASK) == OBJ(2, 0, 0, 0)) {
                        MOQ_TEST_CHECK_EQ_U64(w[i], OBJ(2, 3 + seen, 0, 0));
                        ++seen;
                    }
                MOQ_TEST_CHECK_EQ_SIZE(seen, 2);
            }
            MOQ_TEST_CHECK(!moq_transport_bridge_has_pending(p.pub_br));
            MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(p.pub_br));
            pair_destroy(&p);
        }
    MOQ_TEST_PASS("FETCH split payload retries preserve the current object priority");
}

/* -- D22 fills: public request ingress through the same bridge/model -------- */

typedef struct {
    track_t track;
    moq_fill_t fill;
    moq_group_order_t parent_order, fill_order;
    uint8_t subscriber_priority;
} fill_track_t;

static const char *fill_row_name;
static size_t fill_row_cap;

static bool fill_check(bool ok, const char *expr, int line)
{
    if (!ok) {
        fprintf(stderr, "fill %s cap=%zu line=%d: %s\n",
                fill_row_name, fill_row_cap, line, expr);
        failures++;
    }
    return ok;
}
#define FILL_REQUIRE(expr) do { \
    if (!fill_check((expr), #expr, __LINE__)) return false; \
} while (0)
#define FILL_TRY(expr) do { if (!(expr)) return false; } while (0)

static bool fill_sync(pair_t *p)
{
    const int before = failures;
    shuttle(p);
    if (failures != before) return false;
    FILL_REQUIRE(!p->sub_ep.overflowed && !p->sub_ep.truncated);
    FILL_REQUIRE(moq_session_state(p->pub) == MOQ_SESS_ESTABLISHED &&
                 moq_session_state(p->sub) == MOQ_SESS_ESTABLISHED);
    FILL_REQUIRE(!moq_transport_bridge_is_terminal(p->pub_br) &&
                 !moq_transport_bridge_is_terminal(p->sub_br));
    return true;
}

/* Each setup phase has exactly the named event; unexpected events cannot be
 * silently drained into a successful prerequisite. */
static bool fill_event(moq_session_t *s, moq_event_kind_t kind, size_t detail,
                        moq_event_t *out)
{
    size_t n = 0;
    memset(out, 0, sizeof(*out));
    FILL_REQUIRE(moq_session_poll_events_ex(s, out, 1, sizeof(*out), &n) == MOQ_OK);
    bool ok = n == 1 && out->kind == kind && out->detail_size >= detail;
    if (!ok) moq_event_cleanup(out);
    FILL_REQUIRE(ok);
    return true;
}

static bool fill_no_events(moq_session_t *s)
{
    moq_event_t e;
    size_t n = 0;
    FILL_REQUIRE(moq_session_poll_events_ex(s, &e, 1, sizeof(e), &n) == MOQ_OK);
    if (n) moq_event_cleanup(&e);
    FILL_REQUIRE(n == 0);
    return true;
}

static bool fill_track_name(moq_namespace_t ns, moq_bytes_t name)
{
    return ns.count == 1 && ns.parts && ns.parts[0].data && ns.parts[0].len == 4 &&
        memcmp(ns.parts[0].data, "live", 4) == 0 && name.len == 10 && name.data &&
        memcmp(name.data, "fill-order", 10) == 0;
}

/* A real SUBSCRIBE with a fill, range {0,0}..{6,1}, on "live"/"fill-order".
 * `fill_priority < 0` inherits; otherwise FILL_PARAMETERS overrides it. */
static bool start_fill(pair_t *p, moq_group_order_t parent_order,
                       moq_group_order_t fill_order, uint8_t parent_priority,
                       int fill_priority, fill_track_t *out)
{
    memset(out, 0, sizeof(*out));
    out->parent_order = parent_order;
    out->fill_order = fill_order;
    out->subscriber_priority = fill_priority < 0 ? parent_priority : (uint8_t)fill_priority;
    moq_fill_cfg_t fc;
    moq_fill_cfg_init(&fc);
    fc.has_group_order = true;
    fc.group_order = (uint32_t)fill_order;
    fc.has_subscriber_priority = fill_priority >= 0;
    fc.subscriber_priority = out->subscriber_priority;
    moq_bytes_t part = MOQ_BYTES_LITERAL("live");
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ &part, 1 };
    sc.track_name = MOQ_BYTES_LITERAL("fill-order");
    sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    sc.has_subscriber_priority = true;
    sc.subscriber_priority = parent_priority;
    sc.group_order = parent_order;
    sc.has_forward = sc.forward = true;
    sc.fill = &fc;
    FILL_REQUIRE(moq_session_subscribe(p->sub, &sc, p->now, &out->track.csub) == MOQ_OK);
    moq_event_t e;
    FILL_TRY(fill_event(p->sub, MOQ_EVENT_FILL_CREATED, sizeof(moq_fill_created_event_t), &e));
    bool valid = moq_fill_is_valid(e.u.fill_created.fill) &&
        moq_subscription_eq(e.u.fill_created.sub, out->track.csub) &&
        !moq_publication_is_valid(e.u.fill_created.pub);
    uint64_t initiating_id = e.u.fill_created.initiating_request_id;
    moq_event_cleanup(&e);
    FILL_REQUIRE(valid);
    FILL_TRY(fill_no_events(p->sub));
    FILL_TRY(fill_sync(p));
    FILL_TRY(fill_event(p->pub, MOQ_EVENT_SUBSCRIBE_REQUEST,
                        sizeof(moq_subscribe_request_event_t), &e));
    out->track.sub = e.u.subscribe_request.sub;
    valid = moq_subscription_is_valid(out->track.sub) &&
        fill_track_name(e.u.subscribe_request.track_namespace, e.u.subscribe_request.track_name) &&
        e.u.subscribe_request.subscriber_priority == parent_priority &&
        e.u.subscribe_request.group_order == parent_order &&
        e.u.subscribe_request.filter == MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START &&
        e.u.subscribe_request.start_group == 0 && e.u.subscribe_request.start_object == 0 &&
        e.u.subscribe_request.forward;
    moq_event_cleanup(&e);
    FILL_REQUIRE(valid);
    FILL_TRY(fill_no_events(p->pub));
    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    ac.has_largest = true;
    ac.largest_group = 6;
    ac.largest_object = 1;
    FILL_REQUIRE(moq_session_accept_subscribe(p->pub, out->track.sub, &ac, p->now) == MOQ_OK);
    FILL_TRY(fill_sync(p));
    FILL_TRY(fill_event(p->pub, MOQ_EVENT_FILL_REQUEST, sizeof(moq_fill_request_event_t), &e));
    const moq_fill_request_event_t fr = e.u.fill_request;
    out->fill = fr.fill;
    moq_event_cleanup(&e);
    FILL_REQUIRE(moq_fill_is_valid(fr.fill) && moq_subscription_eq(fr.sub, out->track.sub) &&
                 !moq_publication_is_valid(fr.pub) && fr.initiating_request_id == initiating_id);
    FILL_REQUIRE(fr.start_group == 0 && fr.start_object == 0 &&
                 fr.end_group == 6 && fr.end_object == 1 && !fr.has_fill_timeout &&
                 fr.group_order == (uint32_t)fill_order &&
                 fr.subscriber_priority == out->subscriber_priority);
    FILL_TRY(fill_no_events(p->pub));
    FILL_TRY(fill_event(p->sub, MOQ_EVENT_SUBSCRIBE_OK, sizeof(moq_subscribe_ok_event_t), &e));
    valid = moq_subscription_eq(e.u.subscribe_ok.sub, out->track.csub) &&
        e.u.subscribe_ok.has_largest && e.u.subscribe_ok.largest_group == 6 &&
        e.u.subscribe_ok.largest_object == 1;
    moq_event_cleanup(&e);
    FILL_REQUIRE(valid);
    FILL_TRY(fill_no_events(p->sub));
    FILL_REQUIRE(moq_session_subscription_group_order(p->pub, out->track.sub) == parent_order);
    return true;
}

static bool fill_open_group(pair_t *p, const fill_track_t *f, uint8_t group,
                            moq_subgroup_handle_t *out)
{
    moq_subgroup_cfg_t cfg;
    moq_subgroup_cfg_init(&cfg);
    cfg.group_id = group;
    cfg.publisher_priority = 128;
    FILL_REQUIRE(moq_session_open_subgroup(p->pub, f->track.sub, &cfg, p->now, out) == MOQ_OK);
    FILL_REQUIRE(out->_opaque != 0);
    return true;
}

/* Uses the existing payload marker format. Properties force the producer's
 * two-action encoding, in addition to the bridge's header/payload phases. */
static bool fill_object(pair_t *p, const fill_track_t *f, uint8_t group,
                        uint8_t object, bool properties)
{
    uint8_t body[PAYLOAD_LEN] = { PAYLOAD_TAG, 2, group, 0, object };
    moq_rcbuf_t *payload = NULL, *props = NULL;
    FILL_REQUIRE(moq_rcbuf_create(moq_alloc_default(), body, sizeof(body), &payload) == MOQ_OK);
    if (properties) {
        uint8_t bytes[16];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, bytes, sizeof(bytes));
        /* Valid Object Delivery Timeout KVP, also used by test_d18_object. */
        moq_result_t rc = moq_buf_write_vi64(&w, 2);
        if (rc == MOQ_OK) rc = moq_buf_write_vi64(&w, 65);
        if (rc == MOQ_OK)
            rc = moq_rcbuf_create(moq_alloc_default(), bytes, moq_buf_writer_offset(&w), &props);
        if (rc != MOQ_OK) {
            moq_rcbuf_decref(payload);
            FILL_REQUIRE(rc == MOQ_OK);
        }
    }
    moq_fetch_object_cfg_t cfg;
    moq_fetch_object_cfg_init(&cfg);
    cfg.group_id = group;
    cfg.object_id = object;
    cfg.publisher_priority = 128;
    cfg.payload = payload;
    cfg.properties = props;
    moq_result_t rc = moq_session_write_fill_object(p->pub, f->fill, &cfg, p->now);
    moq_rcbuf_decref(payload);
    if (props) moq_rcbuf_decref(props);
    FILL_REQUIRE(rc == MOQ_OK);
    return true;
}

/* Finite completion of queued work, including a stranded bridge head. Streams
 * may remain open; their destruction belongs to pair_destroy as in other rows. */
static bool fill_drain(pair_t *p)
{
    for (unsigned guard = 0; guard < 4096; guard++) {
        FILL_TRY(fill_sync(p));
        if (mt_serve_one(p->mt)) continue;
        FILL_TRY(fill_sync(p));
        if (mt_serve_one(p->mt)) continue;
        FILL_REQUIRE(p->mt->queued == 0 && p->pub->action_head == p->pub->action_tail &&
                     p->pub_br->dq_items == 0 && p->pub_br->pending_count == 0 &&
                     !moq_transport_bridge_has_outbound_pending(p->pub_br));
        for (size_t i = 0; i < p->mt->nchunks; i++) FILL_REQUIRE(p->mt->chunks[i].sent);
        return true;
    }
    return fill_check(false, "4096-write completion bound", __LINE__);
}

/* Read-only pre-poll facts: the producer has the mode/owner/location/order that
 * the downstream scheduling decision needs. No private state is synthesized. */
static bool fill_metadata(pair_t *p, const fill_track_t *f, uint8_t tag,
                          uint64_t owner, uint8_t mode, uint8_t priority,
                          size_t expected)
{
    size_t count = 0;
    moq_stream_ref_t ref = moq_stream_ref_from_u64(0);
    for (size_t i = p->pub->action_head; i < p->pub->action_tail; i++) {
        const size_t at = i % p->pub->action_cap;
        const moq_action_t *a = &p->pub->actions[at];
        if (a->kind != MOQ_ACTION_SEND_DATA || !a->u.send_data.payload ||
            moq_rcbuf_len(a->u.send_data.payload) != PAYLOAD_LEN) continue;
        const uint8_t *v = moq_rcbuf_data(a->u.send_data.payload);
        if (v[0] != PAYLOAD_TAG || v[1] != tag) continue;
        const moq_action_meta_t *m = &p->pub->action_meta[at];
        count++;
        FILL_REQUIRE(m->mode == mode && m->owner == owner &&
                     m->subscriber_priority == priority && m->publisher_priority == 128 &&
                     m->deadline_us == UINT64_MAX);
        if (mode == MOQ_SCHED_MODE_FILL || mode == MOQ_SCHED_MODE_SUBGROUP) {
            FILL_REQUIRE(m->has_location && m->group_id == v[2] &&
                         m->group_order == (uint8_t)f->parent_order);
            if (mode == MOQ_SCHED_MODE_FILL) {
                FILL_REQUIRE(m->object_id == v[4] && m->fill_group_order == (uint8_t)f->fill_order);
                FILL_REQUIRE(!ref._v || ref._v == a->u.send_data.stream_ref._v);
                ref = a->u.send_data.stream_ref;
            } else FILL_REQUIRE(m->subgroup_id == 0);
        }
    }
    FILL_REQUIRE(count == expected);
    return true;
}

static bool fill_complete_objects(const pair_t *p, const uint32_t *want, size_t n)
{
    uint32_t got[64];
    size_t m = wire_objects(p, got, 64);
    FILL_REQUIRE(m == n);
    for (size_t i = 0; i < n; i++) {
        size_t count = 0;
        for (size_t j = 0; j < m; j++) if (got[j] == want[i]) count++;
        FILL_REQUIRE(count == 1);
        if ((want[i] & 0xffu) != 0)
            FILL_REQUIRE(first_index(got, m, UINT32_MAX, want[i] - 1) <
                         first_index(got, m, UINT32_MAX, want[i]));
    }
    return true;
}

typedef struct {
    const char *name;
    moq_group_order_t parent_order, fill_order;
    uint8_t sub_group, fill_group, next_group;
    bool fill_first, fetch;
} fill_order_case_t;

/* The private review probe's 30 rows: 24 order assertions and six independent
 * FETCH-owner controls. Preambles drain first to isolate object scheduling. */
static bool fill_order_row(pair_t *p, const void *ctx)
{
    const fill_order_case_t *c = ctx;
    fill_track_t f;
    FILL_TRY(start_fill(p, c->parent_order, c->fill_order, 128, -1, &f));
    fetch_t fetch = { MOQ_FETCH_INVALID, MOQ_FETCH_INVALID };
    if (c->fetch) FILL_REQUIRE(start_fetch(p, "fill-order", 128, &fetch));
    moq_subgroup_handle_t sg = MOQ_SUBGROUP_INVALID;
    FILL_TRY(fill_open_group(p, &f, c->sub_group, &sg));
    FILL_REQUIRE(moq_session_open_fill(p->pub, f.fill, p->now) == MOQ_OK);
    FILL_TRY(fill_drain(p));
    uint32_t empty[1];
    FILL_REQUIRE(wire_objects(p, empty, 1) == 0 && mt_held_objects(p->mt) == 0);
    for (unsigned side = 0; side < 2; side++) {
        bool put_fill = c->fill_first ? side == 1 : side == 0;
        if (put_fill) {
            for (uint8_t o = 0; o < 2; o++) {
                if (c->fetch) FILL_REQUIRE(fetch_object(p, &fetch, 3, c->fill_group, o, 128));
                else FILL_TRY(fill_object(p, &f, c->fill_group, o, false));
            }
            if (c->next_group)
                for (uint8_t o = 0; o < 2; o++) FILL_TRY(fill_object(p, &f, c->next_group, o, false));
        } else {
            for (uint64_t o = 0; o < 2; o++)
                FILL_REQUIRE(write_object(p, sg, 1, c->sub_group, 0, o) == MOQ_OK);
        }
    }
    FILL_TRY(fill_metadata(p, &f, 1, f.track.sub._opaque, MOQ_SCHED_MODE_SUBGROUP, 128, 2));
    if (c->fetch) {
        FILL_REQUIRE(fetch.fetch._opaque != f.track.sub._opaque);
        FILL_TRY(fill_metadata(p, &f, 3, fetch.fetch._opaque, MOQ_SCHED_MODE_FETCH, 128, 2));
    } else {
        FILL_TRY(fill_metadata(p, &f, 2, f.track.sub._opaque, MOQ_SCHED_MODE_FILL, 128,
                               c->next_group ? 4 : 2));
    }
    FILL_REQUIRE(wire_objects(p, empty, 1) == 0 && mt_held_objects(p->mt) == 0);
    FILL_TRY(fill_drain(p));
    uint32_t want[6];
    size_t n = 0;
    for (unsigned side = 0; side < 2; side++) {
        bool fill = c->fill_first ? side == 0 : side == 1;
        uint8_t tag = fill ? (c->fetch ? 3 : 2) : 1;
        uint8_t group = fill ? c->fill_group : c->sub_group;
        for (uint8_t o = 0; o < 2; o++) want[n++] = OBJ(tag, group, 0, o);
    }
    if (c->next_group)
        for (uint8_t o = 0; o < 2; o++) want[n++] = OBJ(2, c->next_group, 0, o);
    FILL_TRY(fill_complete_objects(p, want, n));
    if (c->fetch) {
        uint64_t sub_sid = 0, fetch_sid = 0;
        FILL_REQUIRE(stream_of(p, 1, c->sub_group, &sub_sid) &&
                     stream_of(p, 3, c->fill_group, &fetch_sid) && sub_sid != fetch_sid);
        size_t owned = 0;
        for (uint32_t i = 0; i < p->pub_br->dstream_cap; i++) {
            const bridge_dstream_t *d = &p->pub_br->dstreams[i];
            if (d->active && d->has_sid && d->sid == fetch_sid) {
                FILL_REQUIRE(d->key.owner == fetch.fetch._opaque);
                owned++;
            }
        }
        FILL_REQUIRE(owned == 1);
        /* Cross-request equal-priority order is deliberately not asserted. */
    } else expect_wire(p, want, n, c->name, fill_row_cap);
    return true;
}

typedef struct {
    moq_group_order_t order;
    uint8_t updated_priority;
    bool properties, reverse;
} fill_boundary_case_t;

/* The preamble's range starts at group 0, but the first actual object is in
 * group 4. Keep that preamble in the provider while both competitors arrive. */
static bool fill_first_group_row(pair_t *p, const void *ctx)
{
    const fill_boundary_case_t *c = ctx;
    fill_track_t f;
    FILL_TRY(start_fill(p, c->order, c->order, 128, -1, &f));
    FILL_REQUIRE(moq_session_open_fill(p->pub, f.fill, p->now) == MOQ_OK);
    FILL_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
    FILL_REQUIRE(p->mt->queued > 0 && mt_held_objects(p->mt) == 0);
    moq_subgroup_handle_t sg = MOQ_SUBGROUP_INVALID;
    FILL_TRY(fill_open_group(p, &f, 2, &sg));
    for (uint8_t o = 0; o < 2; o++) {
        FILL_REQUIRE(write_object(p, sg, 1, 2, 0, o) == MOQ_OK);
        FILL_TRY(fill_object(p, &f, 4, o, false));
    }
    FILL_TRY(fill_metadata(p, &f, 2, f.track.sub._opaque, MOQ_SCHED_MODE_FILL, 128, 2));
    FILL_TRY(fill_drain(p));
    const uint32_t asc[] = { OBJ(1, 2, 0, 0), OBJ(1, 2, 0, 1),
                             OBJ(2, 4, 0, 0), OBJ(2, 4, 0, 1) };
    const uint32_t desc[] = { OBJ(2, 4, 0, 0), OBJ(2, 4, 0, 1),
                              OBJ(1, 2, 0, 0), OBJ(1, 2, 0, 1) };
    FILL_TRY(fill_complete_objects(p, asc, 4));
    expect_wire(p, c->order == ASC ? asc : desc, 4, fill_row_name, fill_row_cap);
    return true;
}

/* An explicit child priority is not an owner-wide subscription update. Check
 * the held class before supplying more objects with the child's original value. */
static bool fill_priority_override_row(pair_t *p, const void *ctx)
{
    const fill_boundary_case_t *c = ctx;
    fill_track_t f;
    FILL_TRY(start_fill(p, c->order, c->order, 100, 10, &f));
    moq_subgroup_handle_t sg = MOQ_SUBGROUP_INVALID;
    FILL_TRY(fill_open_group(p, &f, 3, &sg));
    FILL_REQUIRE(moq_session_open_fill(p->pub, f.fill, p->now) == MOQ_OK);
    FILL_TRY(fill_drain(p));
    for (uint8_t o = 0; o < 2; o++) {
        FILL_REQUIRE(write_object(p, sg, 1, 3, 0, o) == MOQ_OK);
        FILL_TRY(fill_object(p, &f, 3, o, false));
    }
    FILL_TRY(fill_metadata(p, &f, 2, f.track.sub._opaque, MOQ_SCHED_MODE_FILL, 10, 2));
    FILL_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
    uint64_t sub_sid = 0, fill_sid = 0;
    FILL_REQUIRE(stream_of(p, 1, 3, &sub_sid) && stream_of(p, 2, 3, &fill_sid));
    mt_stream_t *sub = mt_stream(p->mt, sub_sid), *child = mt_stream(p->mt, fill_sid);
    FILL_REQUIRE(sub && child && sub_sid != fill_sid && sub->keyed && child->keyed);
    FILL_REQUIRE((sub->key >> 24) == 100 && (child->key >> 24) == 10);
    moq_subscription_update_cfg_t uc;
    moq_subscription_update_cfg_init(&uc);
    uc.has_subscriber_priority = true;
    uc.subscriber_priority = c->updated_priority;
    FILL_REQUIRE(moq_session_update_subscription(p->sub, f.track.csub, &uc, p->now) == MOQ_OK);
    FILL_TRY(fill_sync(p));
    moq_event_t e;
    FILL_TRY(fill_event(p->pub, MOQ_EVENT_SUBSCRIBE_UPDATED,
                        sizeof(moq_subscribe_updated_event_t), &e));
    bool valid = moq_subscription_eq(e.u.subscribe_updated.sub, f.track.sub) &&
        e.u.subscribe_updated.has_subscriber_priority &&
        e.u.subscribe_updated.subscriber_priority == c->updated_priority &&
        !e.u.subscribe_updated.answer_owed;
    moq_event_cleanup(&e);
    FILL_REQUIRE(valid);
    FILL_TRY(fill_no_events(p->pub));
    FILL_TRY(fill_event(p->sub, MOQ_EVENT_SUBSCRIPTION_UPDATE_OK,
                        sizeof(moq_subscription_update_ok_event_t), &e));
    valid = moq_subscription_eq(e.u.subscription_update_ok.sub, f.track.csub);
    moq_event_cleanup(&e);
    FILL_REQUIRE(valid);
    FILL_TRY(fill_no_events(p->sub));
    FILL_REQUIRE((sub->key >> 24) == c->updated_priority && (child->key >> 24) == 10);
    for (uint8_t o = 2; o < 4; o++) FILL_TRY(fill_object(p, &f, 3, o, false));
    FILL_TRY(fill_metadata(p, &f, 2, f.track.sub._opaque, MOQ_SCHED_MODE_FILL, 10, 2));
    FILL_TRY(fill_drain(p));
    const uint32_t parent_first[] = { OBJ(1, 3, 0, 0), OBJ(1, 3, 0, 1),
        OBJ(2, 3, 0, 0), OBJ(2, 3, 0, 1), OBJ(2, 3, 0, 2), OBJ(2, 3, 0, 3) };
    const uint32_t child_first[] = { OBJ(2, 3, 0, 0), OBJ(2, 3, 0, 1),
        OBJ(2, 3, 0, 2), OBJ(2, 3, 0, 3), OBJ(1, 3, 0, 0), OBJ(1, 3, 0, 1) };
    FILL_TRY(fill_complete_objects(p, parent_first, 6));
    expect_wire(p, c->updated_priority < 10 ? parent_first : child_first,
                6, fill_row_name, fill_row_cap);
    return true;
}

static size_t fill_held_objects(const mt_t *m, uint64_t sid, uint8_t group)
{
    size_t n = 0;
    for (size_t i = 0; i < m->nchunks; i++) {
        const mt_chunk_t *c = &m->chunks[i];
        if (!c->sent && c->sid == sid && c->len == PAYLOAD_LEN &&
            c->head[0] == PAYLOAD_TAG && c->head[1] == 2 && c->head[2] == group) n++;
    }
    return n;
}

/* Same publisher priority, different groups: draining an accepted object
 * header must not let the next group's descriptor replace its retained payload.
 * The properties variant exercises both fragments of the fill producer too. */
static bool fill_payload_retry_row(pair_t *p, const void *ctx)
{
    const fill_boundary_case_t *c = ctx;
    const uint8_t first = c->order == ASC ? 2 : 4;
    const uint8_t next = c->order == ASC ? 4 : 2;
    fill_track_t f;
    FILL_TRY(start_fill(p, c->order, c->order, 128, -1, &f));
    FILL_REQUIRE(moq_session_open_fill(p->pub, f.fill, p->now) == MOQ_OK);
    FILL_TRY(fill_drain(p));
    mt_stream_t *child = first_data_stream(p->mt); /* only the fill exists */
    FILL_REQUIRE(child != NULL);
    moq_subgroup_handle_t sg = MOQ_SUBGROUP_INVALID;
    FILL_TRY(fill_open_group(p, &f, 3, &sg));
    FILL_TRY(fill_drain(p));
    p->mt->cap = 1;
    FILL_TRY(fill_object(p, &f, first, 0, c->properties));
    FILL_TRY(fill_object(p, &f, next, 0, false));
    FILL_TRY(fill_metadata(p, &f, 2, f.track.sub._opaque, MOQ_SCHED_MODE_FILL, 128, 2));
    FILL_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
    FILL_REQUIRE(mt_held_objects(p->mt) == 0 && p->pub_br->pending_count == 1 && p->mt->queued > 0);
    FILL_REQUIRE(mt_serve_one(p->mt)); /* first object's header, not its payload */
    FILL_REQUIRE(p->mt->queued == 0);
    p->mt->cap = 1u << 20;
    for (uint64_t o = 0; o < 2; o++) FILL_REQUIRE(write_object(p, sg, 1, 3, 0, o) == MOQ_OK);
    FILL_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
    uint64_t sub_sid = 0;
    FILL_REQUIRE(stream_of(p, 1, 3, &sub_sid));
    mt_stream_t *sub = mt_stream(p->mt, sub_sid);
    FILL_REQUIRE(sub && child->keyed && sub->keyed && child->key < sub->key);
    FILL_REQUIRE(fill_held_objects(p->mt, child->sid, first) == 1 &&
                 fill_held_objects(p->mt, child->sid, next) == 0);
    FILL_REQUIRE(moq_transport_bridge_has_pending(p->pub_br));
    FILL_TRY(fill_drain(p));
    const uint32_t want[] = { OBJ(2, first, 0, 0), OBJ(1, 3, 0, 0),
                              OBJ(1, 3, 0, 1), OBJ(2, next, 0, 0) };
    FILL_TRY(fill_complete_objects(p, want, 4));
    expect_wire(p, want, 4, fill_row_name, fill_row_cap);
    return true;
}

static bool fill_stream_queued(const mt_t *m, uint64_t sid)
{
    for (size_t i = 0; i < m->nchunks; i++)
        if (!m->chunks[i].sent && m->chunks[i].sid == sid) return true;
    return false;
}

/* D22's opposite-order cycle is a logical tie, not an impossible strict
 * ordering assertion. The earlier subscription group is an acyclic predecessor
 * of the entire component. Reverse admission checks that this is a logical
 * class, not an accident of queue/slot order. */
static bool fill_cycle_row(pair_t *p, const void *ctx)
{
    const fill_boundary_case_t *c = ctx;
    const uint8_t same = c->order == ASC ? 2 : 4;
    const uint8_t other = c->order == ASC ? 4 : 2;
    const uint8_t external = c->order == ASC ? 0 : 6;
    const uint8_t groups[] = { external, same, other };
    fill_track_t f;
    FILL_TRY(start_fill(p, c->order, c->order == ASC ? DESC : ASC, 128, -1, &f));
    moq_subgroup_handle_t sg[3] = { MOQ_SUBGROUP_INVALID, MOQ_SUBGROUP_INVALID, MOQ_SUBGROUP_INVALID };
    if (c->reverse) FILL_REQUIRE(moq_session_open_fill(p->pub, f.fill, p->now) == MOQ_OK);
    for (unsigned k = 0; k < 3; k++) {
        unsigned j = c->reverse ? 2 - k : k;
        FILL_TRY(fill_open_group(p, &f, groups[j], &sg[j]));
    }
    if (!c->reverse) FILL_REQUIRE(moq_session_open_fill(p->pub, f.fill, p->now) == MOQ_OK);
    FILL_TRY(fill_drain(p));
    for (unsigned side = 0; side < 2; side++) {
        if ((side == 0) == c->reverse) {
            for (uint8_t o = 0; o < 4; o++) FILL_TRY(fill_object(p, &f, same, o, false));
        } else {
            for (unsigned k = 0; k < 3; k++) {
                unsigned j = c->reverse ? 2 - k : k;
                for (uint8_t o = 0; o < (j == 0 ? 2 : 4); o++)
                    FILL_REQUIRE(write_object(p, sg[j], 1, groups[j], 0, o) == MOQ_OK);
            }
        }
    }
    FILL_TRY(fill_metadata(p, &f, 2, f.track.sub._opaque, MOQ_SCHED_MODE_FILL, 128, 4));
    FILL_TRY(fill_metadata(p, &f, 1, f.track.sub._opaque, MOQ_SCHED_MODE_SUBGROUP, 128, 10));
    FILL_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
    uint64_t ids[4] = { 0 };
    FILL_REQUIRE(stream_of(p, 2, same, &ids[0]) && stream_of(p, 1, same, &ids[1]) &&
                 stream_of(p, 1, other, &ids[2]) && stream_of(p, 1, external, &ids[3]));
    mt_stream_t *st[4];
    for (size_t i = 0; i < 4; i++) {
        st[i] = mt_stream(p->mt, ids[i]);
        FILL_REQUIRE(st[i] && st[i]->keyed);
        for (size_t j = 0; j < i; j++) FILL_REQUIRE(ids[i] != ids[j]);
    }
    FILL_REQUIRE(st[0]->key == st[1]->key && st[1]->key == st[2]->key && st[3]->key < st[0]->key);
    const uint32_t tied = st[0]->key, predecessor = st[3]->key;
    for (unsigned i = 0; i < 3; i++) {
        FILL_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
        FILL_REQUIRE(st[0]->key == tied && st[1]->key == tied && st[2]->key == tied &&
                     st[3]->key == predecessor);
    }
    unsigned writes = 0;
    for (; writes < 4096; writes++) {
        FILL_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
        if (fill_stream_queued(p->mt, ids[0]) && fill_stream_queued(p->mt, ids[1]) &&
            fill_stream_queued(p->mt, ids[2]))
            FILL_REQUIRE(st[0]->key == st[1]->key && st[1]->key == st[2]->key);
        if (!mt_serve_one(p->mt)) break;
    }
    FILL_REQUIRE(writes < 4096);
    FILL_TRY(fill_drain(p));
    uint32_t want[14], got[16];
    size_t n = 0;
    for (uint8_t o = 0; o < 2; o++) want[n++] = OBJ(1, external, 0, o);
    for (uint8_t o = 0; o < 4; o++) want[n++] = OBJ(2, same, 0, o);
    for (uint8_t o = 0; o < 4; o++) want[n++] = OBJ(1, same, 0, o);
    for (uint8_t o = 0; o < 4; o++) want[n++] = OBJ(1, other, 0, o);
    FILL_TRY(fill_complete_objects(p, want, n));
    FILL_REQUIRE(wire_objects(p, got, 16) == n && got[0] == want[0] && got[1] == want[1]);
    unsigned served[3] = { 0 };
    for (size_t i = 2; i < 11; i++) { /* three full fair turns, in any order */
        unsigned j = (got[i] >> 24) == 2 ? 0 : ((got[i] >> 16) & 0xffu) == same ? 1 : 2;
        served[j]++;
        unsigned low = served[0], high = served[0];
        for (j = 1; j < 3; j++) {
            if (served[j] < low) low = served[j];
            if (served[j] > high) high = served[j];
        }
        FILL_REQUIRE(high - low <= 1);
    }
    return true;
}

typedef bool (*fill_row_fn)(pair_t *, const void *);

/* -- Detached pending retries: observe before the affected stream retries -- */

typedef struct {
    const char *name;
    bool drain_header;
    uint8_t successor_group; /* zero: no successor; first object is group 1 */
} fill_retry_case_t;

static const bridge_dstream_t *fill_retry_locate(const pair_t *p, uint64_t owner,
                                                uint8_t mode, uint64_t group)
{
    const bridge_dstream_t *found = NULL;
    for (uint32_t i = 0; i < p->pub_br->dstream_cap; i++) {
        const bridge_dstream_t *d = &p->pub_br->dstreams[i];
        if (!d->active || d->meta.owner != owner || d->meta.mode != mode ||
            !d->meta.has_location || d->meta.group_id != group) continue;
        if (found) return NULL;
        found = d;
    }
    return found;
}

static size_t fill_retry_unsent(const mt_t *m, uint64_t sid)
{
    size_t n = 0;
    for (size_t i = 0; i < m->nchunks; i++)
        if (!m->chunks[i].sent && m->chunks[i].sid == sid) n++;
    return n;
}

static bool fill_retry_first_class(const moq_action_meta_t *m, uint64_t owner)
{
    return m->owner == owner && m->mode == MOQ_SCHED_MODE_FILL &&
        m->has_location && m->group_id == 1 && m->subgroup_id == 0 &&
        m->group_order == ASC && m->fill_group_order == ASC &&
        m->subscriber_priority == 128 && m->publisher_priority == 128;
}

static bool fill_retry_pending(const pair_t *p, uint32_t at,
                               moq_stream_ref_t ref, uint64_t sid,
                               uint64_t owner, uint8_t mode, uint8_t tag,
                               uint8_t group)
{
    FILL_REQUIRE(at < p->pub_br->pending_count);
    const bridge_pending_item_t *item = &p->pub_br->pending[at];
    FILL_REQUIRE(item->kind == PENDING_PAYLOAD_ONLY && item->owns_action &&
                 item->stream_ref._v == ref._v && item->has_stream_id && item->stream_id == sid);
    FILL_REQUIRE(item->act.kind == MOQ_ACTION_SEND_DATA && item->act.u.send_data.header_len > 0 &&
                 !item->act.u.send_data.fin && item->act.u.send_data.payload &&
                 moq_rcbuf_len(item->act.u.send_data.payload) == PAYLOAD_LEN);
    const uint8_t *body = moq_rcbuf_data(item->act.u.send_data.payload);
    FILL_REQUIRE(body[0] == PAYLOAD_TAG && body[1] == tag && body[2] == group &&
                 body[3] == 0 && body[4] == 0);
    FILL_REQUIRE(item->meta.owner == owner && item->meta.mode == mode &&
                 item->meta.has_location && item->meta.group_id == group &&
                 item->meta.subscriber_priority == 128 && item->meta.publisher_priority == 128);
    const bridge_dstream_t *d = mt_retry_dstream(p->pub_br, ref);
    FILL_REQUIRE(d && d->has_sid && d->sid == sid && d->retained == 1 && !d->dispatching);
    return true;
}

/* Two regular streams each retain an actual object payload before a fill in
 * the pending FIFO. The fill's header may drain while its payload is retained.
 * A sibling holds same-group bytes. Capture at BOTH earlier payload writes:
 * checking after the fill's retry would miss the transient graph/key change. */
static bool fill_detached_retry_row(pair_t *p, const void *ctx)
{
    const fill_retry_case_t *c = ctx;
    fill_track_t f;
    FILL_TRY(start_fill(p, ASC, ASC, 128, -1, &f));
    track_t r;
    FILL_REQUIRE(subscribe(p, "retry", 128, ASC, NONE, &r));
    FILL_REQUIRE(moq_subscription_is_valid(r.sub) && r.sub._opaque != f.track.sub._opaque &&
                 moq_session_subscription_group_order(p->pub, r.sub) == ASC);
    FILL_TRY(fill_no_events(p->pub));
    FILL_TRY(fill_no_events(p->sub));
    moq_subgroup_handle_t rs[2], ss = MOQ_SUBGROUP_INVALID;
    for (uint8_t i = 0; i < 2; i++)
        FILL_REQUIRE(publish_group(p, &r, 3, i, 0, 128, 0, true, &rs[i]));
    FILL_TRY(fill_open_group(p, &f, 1, &ss));
    FILL_REQUIRE(moq_session_open_fill(p->pub, f.fill, p->now) == MOQ_OK);
    FILL_TRY(fill_drain(p)); /* every stream preamble is already drained */

    const uint64_t owners[] = { r.sub._opaque, r.sub._opaque, f.track.sub._opaque,
                               f.track.sub._opaque };
    const uint8_t modes[] = { MOQ_SCHED_MODE_SUBGROUP, MOQ_SCHED_MODE_SUBGROUP,
                             MOQ_SCHED_MODE_FILL, MOQ_SCHED_MODE_SUBGROUP };
    const uint8_t groups[] = { 0, 1, 0, 1 };
    moq_stream_ref_t refs[4];
    uint64_t ids[4];
    mt_stream_t *st[4];
    for (size_t i = 0; i < 4; i++) {
        const bridge_dstream_t *d = fill_retry_locate(p, owners[i], modes[i], groups[i]);
        FILL_REQUIRE(d && d->has_sid && d->ref._v != 0 && !d->keyed && d->retained == 0);
        refs[i] = d->ref;
        ids[i] = d->sid;
        st[i] = mt_stream(p->mt, ids[i]);
        FILL_REQUIRE(st[i] && st[i]->data && !st[i]->reset && fill_retry_unsent(p->mt, ids[i]) == 0);
        for (size_t j = 0; j < i; j++) FILL_REQUIRE(ids[i] != ids[j] && refs[i]._v != refs[j]._v);
    }
    FILL_REQUIRE(p->mt->drained_cap && p->mt->queued == 0 && p->pub_br->max_pending >= 3);
    uint32_t empty[1];
    FILL_REQUIRE(wire_objects(p, empty, 1) == 0);

    /* The sibling is fully handed but unsent. Refuse only the marked bodies
     * of both regular streams and the fill, so their headers are accepted
     * partial writes. */
    FILL_REQUIRE(write_object(p, ss, 1, 1, 0, 0) == MOQ_OK);
    FILL_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
    FILL_REQUIRE(p->pub_br->pending_count == 0 && mt_held_objects(p->mt) == 1 &&
                 fill_retry_unsent(p->mt, ids[3]) == 2);
    for (uint8_t i = 0; i < 2; i++) {
        st[i]->refuse_payload = true;
        FILL_REQUIRE(write_object(p, rs[i], 3, i, 0, 0) == MOQ_OK);
        FILL_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
        FILL_REQUIRE(p->pub_br->pending_count == (uint32_t)i + 1);
        FILL_TRY(fill_retry_pending(p, i, refs[i], ids[i], r.sub._opaque,
                                    MOQ_SCHED_MODE_SUBGROUP, 3, i));
    }
    st[2]->refuse_payload = true;
    FILL_TRY(fill_object(p, &f, 1, 0, false));
    if (c->successor_group)
        FILL_TRY(fill_object(p, &f, c->successor_group,
                             c->successor_group == 1 ? 1 : 0, false));
    FILL_TRY(fill_metadata(p, &f, 2, f.track.sub._opaque, MOQ_SCHED_MODE_FILL, 128,
                           c->successor_group ? 2 : 1));
    FILL_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
    FILL_REQUIRE(p->pub_br->pending_count == 3 && p->pub->action_head == p->pub->action_tail);
    for (uint8_t i = 0; i < 3; i++) {
        FILL_TRY(fill_retry_pending(p, i, refs[i], ids[i], owners[i], modes[i],
                                    i == 2 ? 2 : 3, i == 2 ? 1 : i));
        FILL_REQUIRE(st[i]->refused_writes > 0 && fill_retry_unsent(p->mt, ids[i]) == 1);
    }
    const bridge_dstream_t *fd = mt_retry_dstream(p->pub_br, refs[2]);
    FILL_REQUIRE(fd && fd->keyed && !fd->prefix_only && fd->q_len == (c->successor_group ? 1u : 0u));
    if (c->successor_group) {
        const bridge_data_item_t *next = &fd->q[fd->q_head];
        FILL_REQUIRE(next->action.kind == MOQ_ACTION_SEND_DATA &&
                     next->meta.mode == MOQ_SCHED_MODE_FILL && next->meta.has_location &&
                     next->meta.group_id == c->successor_group);
    }
    FILL_REQUIRE(p->pub_br->dq_items == (c->successor_group ? 1u : 0u));
    const size_t before_wire = p->mt->nwire;
    if (c->drain_header) {
        st[0]->blocked = st[1]->blocked = st[3]->blocked = true;
        FILL_REQUIRE(mt_serve_one(p->mt));
        st[0]->blocked = st[1]->blocked = st[3]->blocked = false;
        FILL_REQUIRE(p->mt->nwire == before_wire + 1 &&
                     p->mt->wire[before_wire].sid == ids[2] &&
                     !p->mt->wire[before_wire].reset && !p->mt->wire[before_wire].fin &&
                     p->mt->wire[before_wire].len == p->pub_br->pending[2].act.u.send_data.header_len);
    } else FILL_REQUIRE(p->mt->nwire == before_wire);
    fd = mt_retry_dstream(p->pub_br, refs[2]);
    const bridge_dstream_t *sd = mt_retry_dstream(p->pub_br, refs[3]);
    const uint32_t sibling_key = UINT32_C(0x80800002); /* priority 128/128, rank 1 */
    FILL_REQUIRE(fd && fd->keyed == !c->drain_header && fd->retained == 1 &&
                 fd->order_active && fill_retry_first_class(&fd->order_meta, f.track.sub._opaque));
    FILL_REQUIRE(sd && sd->keyed && sd->order_active && sd->pos == 1 &&
                 st[3]->key == sibling_key && fill_retry_unsent(p->mt, ids[3]) == 2);
    FILL_REQUIRE(fill_retry_unsent(p->mt, ids[2]) == (c->drain_header ? 0u : 1u) &&
                 mt_held_objects(p->mt) == 1 && wire_objects(p, empty, 1) == 0);
    FILL_REQUIRE(p->pub_br->pending_count == 3);
    FILL_TRY(fill_retry_pending(p, 2, refs[2], ids[2], f.track.sub._opaque,
                                MOQ_SCHED_MODE_FILL, 2, 1));

    mt_retry_probe_t *probe = &p->mt->retry_probe;
    probe->earlier_sid[0] = ids[0];
    probe->earlier_sid[1] = ids[1];
    probe->fill_sid = ids[2];
    probe->sibling_sid = ids[3];
    probe->fill_ref = refs[2];
    probe->sibling_ref = refs[3];
    for (size_t i = 0; i < 3; i++) st[i]->refuse_payload = false;
    printf("READY %s: regular payloads precede fill; fill header drained=%d; successor=%u\n",
           c->name, (int)c->drain_header, (unsigned)c->successor_group);
    probe->armed = true;
    moq_result_t rc = moq_transport_bridge_service(p->pub_br, p->now);
    probe->armed = false;
    FILL_REQUIRE(rc == MOQ_OK && probe->sample_count == 2 && probe->fill_payload_writes >= 1 &&
                 p->pub_br->pending_count == 0);
    bool correct = true;
    for (size_t i = 0; i < 2; i++) {
        const mt_retry_sample_t *sample = &probe->samples[i];
        FILL_REQUIRE(sample->sid == ids[i] && sample->fill_payload_writes == 0 &&
                     sample->fill_found && sample->sibling_found &&
                     sample->sibling_keyed && sample->sibling_held);
        printf("OBSERVE %s R%zu: F active=%d group=%llu; S pos=%u key=%08x; F writes=%zu\n",
               c->name, i + 1, (int)sample->fill_order_active,
               (unsigned long long)sample->fill_order_meta.group_id,
               (unsigned)sample->sibling_pos, (unsigned)sample->sibling_key,
               sample->fill_payload_writes);
        if (!fill_check(sample->fill_order_active &&
                        fill_retry_first_class(&sample->fill_order_meta, f.track.sub._opaque),
                        "F retains its group-1 representative before its own retry", __LINE__)) correct = false;
        if (!fill_check(sample->sibling_pos == 1,
                        "S keeps logical rank 1 at earlier R retry", __LINE__)) correct = false;
        if (!fill_check(sample->sibling_key == sibling_key,
                        "S keeps its published key at earlier R retry", __LINE__)) correct = false;
    }
    FILL_TRY(fill_drain(p));
    uint32_t want[5] = { OBJ(3, 0, 0, 0), OBJ(3, 1, 0, 0),
                         OBJ(2, 1, 0, 0), OBJ(1, 1, 0, 0) };
    size_t n = 4;
    if (c->successor_group)
        want[n++] = OBJ(2, c->successor_group, 0, c->successor_group == 1 ? 1 : 0);
    FILL_TRY(fill_complete_objects(p, want, n));
    if (c->successor_group) {
        uint32_t got[8];
        size_t count = wire_objects(p, got, 8);
        FILL_REQUIRE(first_index(got, count, UINT32_MAX, want[2]) <
                     first_index(got, count, UINT32_MAX, want[4]));
    }
    return correct;
}

static void run_fill_row(const char *name, size_t cap, fill_row_fn row, const void *ctx)
{
    fill_row_name = name;
    fill_row_cap = cap;
    pair_t p;
    const int before = failures;
    bool ready = pair_init(&p, MOQ_VERSION_DRAFT_22, cap, true);
    if (fill_check(ready && failures == before, "pair initialization", __LINE__))
        (void)row(&p, ctx);
    pair_destroy(&p);
    printf("fill %s cap=%zu failures=%d\n", name, cap, failures - before);
}

static void test_fill_order(void)
{
    static const fill_order_case_t cases[] = {
        { "same-ASC", ASC, ASC, 3, 3, 0, true, false },
        { "same-DESC", DESC, DESC, 3, 3, 0, true, false },
        { "matching-ASC", ASC, ASC, 4, 2, 0, true, false },
        { "matching-DESC", DESC, DESC, 2, 4, 0, true, false },
        { "opposite-ASC", ASC, DESC, 4, 2, 0, false, false },
        { "opposite-DESC", DESC, ASC, 2, 4, 0, false, false },
        { "crossing-ASC", ASC, ASC, 3, 2, 4, true, false },
        { "crossing-DESC", DESC, DESC, 3, 4, 2, true, false },
        { "FETCH-owner-ASC", ASC, DESC, 3, 3, 0, false, true },
        { "FETCH-owner-DESC", DESC, ASC, 3, 3, 0, false, true },
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++)
        for (size_t k = 0; k < sizeof(k_caps) / sizeof(k_caps[0]); k++)
            run_fill_row(cases[i].name, k_caps[k], fill_order_row, &cases[i]);
    for (unsigned order = 0; order < 2; order++) {
        fill_boundary_case_t c = { .order = order == 0 ? ASC : DESC };
        for (size_t k = 0; k < sizeof(k_caps) / sizeof(k_caps[0]); k++)
            run_fill_row(order == 0 ? "first-group-ASC" : "first-group-DESC",
                         k_caps[k], fill_first_group_row, &c);
        for (unsigned direction = 0; direction < 2; direction++) {
            c.updated_priority = direction == 0 ? 5 : 200;
            run_fill_row(order == 0 ? (direction == 0 ? "override-ASC-raise" : "override-ASC-lower")
                                   : (direction == 0 ? "override-DESC-raise" : "override-DESC-lower"),
                         1u << 20, fill_priority_override_row, &c);
            c.properties = direction != 0;
            run_fill_row(order == 0 ? (c.properties ? "split-properties-ASC" : "split-ASC")
                                   : (c.properties ? "split-properties-DESC" : "split-DESC"),
                         1u << 20, fill_payload_retry_row, &c);
            c.reverse = direction != 0;
            run_fill_row(order == 0 ? (c.reverse ? "cycle-ASC-reverse" : "cycle-ASC")
                                   : (c.reverse ? "cycle-DESC-reverse" : "cycle-DESC"),
                         1u << 20, fill_cycle_row, &c);
        }
    }
    static const fill_retry_case_t retries[] = {
        { "detached-next-group", true, 2 },
        { "detached-no-successor", true, 0 },
        { "detached-no-drain-control", false, 2 },
        { "detached-same-class-control", true, 1 },
    };
    /* These refuse one stream's payload, which a transport advertising shared
     * admission never does. */
    for (size_t i = 0; !g_shared_all && i < sizeof(retries) / sizeof(retries[0]); i++)
        run_fill_row(retries[i].name, 1u << 20, fill_detached_retry_row, &retries[i]);
    MOQ_TEST_PASS("D22 fill ordering, current groups, priority overrides and cycle fairness");
}

#undef FILL_REQUIRE
#undef FILL_TRY

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

/* -- Cold-review regressions: terminal ordering and retained expiry ------- */

/* These fixtures use public session/bridge calls and endpoint observations.
 * A failed setup stops its row, so an absent request/stream cannot turn into
 * an uninitialized handle, an invented model stream, or a misleading pass. */
#define REG_REQUIRE(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
        return false; \
    } \
} while (0)

typedef struct {
    bool publication;
    track_t track;
    moq_publication_t pub, peer_pub;
} regression_owner_t;

static bool regression_sync(pair_t *p)
{
    const int before = failures;
    shuttle(p);
    REG_REQUIRE(failures == before);
    REG_REQUIRE(!p->sub_ep.overflowed && !p->sub_ep.truncated);
    REG_REQUIRE(moq_session_state(p->pub) == MOQ_SESS_ESTABLISHED);
    REG_REQUIRE(moq_session_state(p->sub) == MOQ_SESS_ESTABLISHED);
    REG_REQUIRE(!moq_transport_bridge_is_terminal(p->pub_br));
    REG_REQUIRE(!moq_transport_bridge_is_terminal(p->sub_br));
    REG_REQUIRE(!moq_transport_bridge_has_outbound_pending(p->pub_br));
    REG_REQUIRE(!moq_transport_bridge_has_outbound_pending(p->sub_br));
    return true;
}

static bool regression_event(moq_session_t *s, moq_event_kind_t kind,
                              moq_event_t *out)
{
    size_t n = 0;
    memset(out, 0, sizeof(*out));
    moq_result_t rc = moq_session_poll_events_ex(s, out, 1, sizeof(*out), &n);
    bool ok = rc == MOQ_OK && n == 1 && out->kind == kind;
    if (!ok) moq_event_cleanup(out);
    REG_REQUIRE(ok);
    return true;
}

static bool regression_no_events(moq_session_t *s)
{
    moq_event_t e = {0};
    size_t n = 0;
    moq_result_t rc = moq_session_poll_events_ex(s, &e, 1, sizeof(e), &n);
    moq_event_cleanup(&e);
    REG_REQUIRE(rc == MOQ_OK && n == 0);
    return true;
}

static bool regression_name(moq_namespace_t ns, moq_bytes_t name,
                             const char *want)
{
    return ns.count == 1 && ns.parts && ns.parts[0].data &&
        ns.parts[0].len == 4 && memcmp(ns.parts[0].data, "live", 4) == 0 &&
        name.data && name.len == strlen(want) &&
        memcmp(name.data, want, name.len) == 0;
}

static bool regression_owner(pair_t *p, bool publication, const char *name,
                              uint8_t priority, uint64_t timeout_ms,
                              regression_owner_t *out)
{
    memset(out, 0, sizeof(*out));
    out->publication = publication;
    moq_bytes_t part = MOQ_BYTES_LITERAL("live");
    moq_bytes_t track_name = { (const uint8_t *)name, strlen(name) };
    if (publication) {
        REG_REQUIRE(timeout_ms == 0); /* timeout rows use SUBSCRIBE */
        moq_publish_cfg_t pc;
        moq_publish_cfg_init_sized(&pc, sizeof(pc));
        pc.track_namespace = (moq_namespace_t){ &part, 1 };
        pc.track_name = track_name;
        pc.has_subscriber_priority = p->ver == MOQ_VERSION_DRAFT_22;
        pc.subscriber_priority = priority;
        REG_REQUIRE(moq_session_publish(p->pub, &pc, p->now, &out->pub) == MOQ_OK);
    } else {
        moq_subscribe_cfg_t sc;
        moq_subscribe_cfg_init(&sc);
        sc.track_namespace = (moq_namespace_t){ &part, 1 };
        sc.track_name = track_name;
        sc.has_subscriber_priority = true;
        sc.subscriber_priority = priority;
        REG_REQUIRE(moq_session_subscribe(p->sub, &sc, p->now,
                                          &out->track.csub) == MOQ_OK);
    }
    if (!regression_sync(p)) return false;
    moq_event_t e;
    if (!regression_event(publication ? p->sub : p->pub,
            publication ? MOQ_EVENT_PUBLISH_REQUEST : MOQ_EVENT_SUBSCRIBE_REQUEST,
            &e)) return false;
    bool valid;
    if (publication) {
        out->peer_pub = e.u.publish_request.pub;
        valid = moq_publication_is_valid(out->peer_pub) &&
            regression_name(e.u.publish_request.track_namespace,
                             e.u.publish_request.track_name, name);
    } else {
        out->track.sub = e.u.subscribe_request.sub;
        valid = moq_subscription_is_valid(out->track.sub) &&
            regression_name(e.u.subscribe_request.track_namespace,
                             e.u.subscribe_request.track_name, name) &&
            e.u.subscribe_request.subscriber_priority == priority;
    }
    moq_event_cleanup(&e);
    REG_REQUIRE(valid);
    if (!regression_no_events(p->pub) || !regression_no_events(p->sub)) return false;
    if (publication) {
        moq_accept_publish_cfg_t ac;
        moq_accept_publish_cfg_init(&ac);
        ac.has_subscriber_priority = p->ver != MOQ_VERSION_DRAFT_22;
        ac.subscriber_priority = priority;
        REG_REQUIRE(moq_session_accept_publish(p->sub, out->peer_pub, &ac,
                                               p->now) == MOQ_OK);
    } else {
        uint8_t props[16];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, props, sizeof(props));
        if (timeout_ms) {
            REG_REQUIRE(moq_buf_write_vi64(&w, 0x02) == MOQ_OK);
            REG_REQUIRE(moq_buf_write_vi64(&w, timeout_ms) == MOQ_OK);
        }
        moq_accept_subscribe_cfg_t ac;
        moq_accept_subscribe_cfg_init(&ac);
        ac.track_properties = (moq_bytes_t){ props, moq_buf_writer_offset(&w) };
        REG_REQUIRE(moq_session_accept_subscribe(p->pub, out->track.sub, &ac,
                                                 p->now) == MOQ_OK);
    }
    if (!regression_sync(p)) return false;
    if (!regression_event(publication ? p->pub : p->sub,
            publication ? MOQ_EVENT_PUBLISH_OK : MOQ_EVENT_SUBSCRIBE_OK, &e))
        return false;
    valid = publication ? moq_publication_eq(e.u.publish_ok.pub, out->pub)
                        : moq_subscription_eq(e.u.subscribe_ok.sub, out->track.csub);
    moq_event_cleanup(&e);
    REG_REQUIRE(valid);
    return regression_no_events(p->pub) && regression_no_events(p->sub);
}

/* Open just one stream at a time and identify it from the accepted header,
 * without assuming stream-id arithmetic or consulting the bridge's map. */
static bool regression_open_header(pair_t *p, const regression_owner_t *owner,
                                    uint64_t group, moq_subgroup_handle_t *sg,
                                    uint64_t *sid, size_t *header_len)
{
    const size_t mark = p->mt->nchunks;
    moq_subgroup_cfg_t sc;
    moq_subgroup_cfg_init(&sc);
    sc.group_id = group;
    sc.publisher_priority = 128;
    moq_result_t rc = owner->publication
        ? moq_session_open_pub_subgroup(p->pub, owner->pub, &sc, p->now, sg)
        : moq_session_open_subgroup(p->pub, owner->track.sub, &sc, p->now, sg);
    REG_REQUIRE(rc == MOQ_OK && moq_subgroup_is_valid(*sg));
    REG_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
    REG_REQUIRE(p->mt->nchunks == mark + 1);
    const mt_chunk_t *c = &p->mt->chunks[mark];
    REG_REQUIRE(c->len > 0 && !c->fin && c->accepted_seq > 0);
    *sid = c->sid;
    *header_len = c->len;
    REG_REQUIRE(!moq_session_has_pending_output(p->pub));
    REG_REQUIRE(!moq_transport_bridge_has_outbound_pending(p->pub_br));
    return true;
}

static moq_result_t regression_finish(pair_t *p, const regression_owner_t *owner,
                                       uint64_t streams, const char *reason)
{
    moq_bytes_t text = { (const uint8_t *)reason, strlen(reason) };
    if (owner->publication) {
        moq_finish_publish_cfg_t fc;
        moq_finish_publish_cfg_init(&fc);
        fc.status_code = 0x3; /* SUBSCRIPTION_ENDED */
        fc.stream_count = streams;
        fc.reason = text;
        return moq_session_finish_publish(p->pub, owner->pub, &fc, p->now);
    }
    moq_done_subscribe_cfg_t dc;
    moq_done_subscribe_cfg_init(&dc);
    dc.status_code = 0x3;
    dc.stream_count = streams;
    dc.reason = text;
    return moq_session_done_subscribe(p->pub, owner->track.sub, &dc, p->now);
}

/* All three drafts use the public Type(vi64)/Length(16) envelope and this
 * terminal body; only d16 prepends Request ID. Decode the actual terminal,
 * not merely the first control write (which might be unrelated traffic).
 * The reason identifies the owner even on d16's shared control stream. */
static bool regression_done(const pair_t *p, const char *reason, uint64_t streams,
                              size_t *count, uint64_t *seq)
{
    *count = 0;
    *seq = 0;
    for (size_t i = 0; i < p->mt->nctrl; ++i) {
        const mt_ctrl_t *c = &p->mt->ctrl[i];
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, c->data, c->len);
        while (moq_buf_reader_remaining(&r)) {
            moq_control_envelope_t env;
            REG_REQUIRE(moq_control_decode_envelope(&r, &env) == MOQ_OK);
            if (env.msg_type != MOQ_D16_PUBLISH_DONE) continue;
            moq_buf_reader_t body;
            moq_buf_reader_init(&body, env.payload, env.payload_len);
            uint64_t request_id, status, stream_count, reason_len;
            if (p->ver == MOQ_VERSION_DRAFT_16)
                REG_REQUIRE(moq_buf_read_vi64(&body, &request_id) == MOQ_OK);
            REG_REQUIRE(moq_buf_read_vi64(&body, &status) == MOQ_OK);
            REG_REQUIRE(moq_buf_read_vi64(&body, &stream_count) == MOQ_OK);
            REG_REQUIRE(moq_buf_read_vi64(&body, &reason_len) == MOQ_OK);
            REG_REQUIRE(reason_len == moq_buf_reader_remaining(&body));
            if (reason_len != strlen(reason) ||
                memcmp(moq_buf_reader_ptr(&body), reason, (size_t)reason_len) != 0)
                continue;
            REG_REQUIRE(status == 0x3 && stream_count == streams);
            REG_REQUIRE(c->fin == uni_control(p->ver));
            ++*count;
            *seq = c->accepted_seq;
        }
    }
    return true;
}

static size_t regression_fins(const mt_t *m, uint64_t sid, uint64_t *seq)
{
    size_t n = 0;
    *seq = 0;
    for (size_t i = 0; i < m->nchunks; ++i)
        if (m->chunks[i].sid == sid && m->chunks[i].fin) {
            n++;
            *seq = m->chunks[i].accepted_seq;
        }
    return n;
}

static bool regression_service(pair_t *p)
{
    /* Allow a deferred terminal to run in a subsequent pass; never serve the
     * model network here. FIN handoff, not drain/ACK, releases the terminal. */
    for (unsigned i = 0; i < 4; ++i)
        REG_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
    REG_REQUIRE(!moq_transport_bridge_is_terminal(p->pub_br));
    REG_REQUIRE(moq_session_state(p->pub) == MOQ_SESS_ESTABLISHED);
    CHECK_TABLE_COUNTS(p->pub_br);
    return true;
}

/* d16 9.15 / d18 10.11 / d22 9.9: every subgroup must be closed before
 * PUBLISH_DONE is sent. Cross-stream network arrival order is unconstrained. */
static bool normal_fin_done_row(pair_t *p, bool publication, bool refuse_fin)
{
    regression_owner_t target, other;
    if (!regression_owner(p, publication, "terminal-target", 128, 0, &target) ||
        !regression_owner(p, publication, "terminal-other", 128, 0, &other))
        return false;
    moq_subgroup_handle_t sg[3];
    uint64_t sid[3];
    size_t header_len;
    for (unsigned i = 0; i < 3; ++i) {
        if (!regression_open_header(p, i < 2 ? &target : &other,
                i == 1 ? 2 : 1, &sg[i], &sid[i], &header_len)) return false;
        REG_REQUIRE(write_object(p, sg[i], i < 2 ? 1 : 2,
                                  i == 1 ? 2 : 1, 0, 0) == MOQ_OK);
        serve_all(p);
    }
    REG_REQUIRE(!moq_transport_bridge_has_outbound_pending(p->pub_br));
    mt_stream_t *blocked = mt_stream(p->mt, sid[0]);
    REG_REQUIRE(blocked && blocked->data && !blocked->reset);
    blocked->refuse_fin = refuse_fin;
    const uint64_t mark = p->mt->op_seq;
    /* No bridge service between these calls: the causal regression. */
    REG_REQUIRE(moq_session_close_subgroup(p->pub, sg[0], p->now) == MOQ_OK);
    REG_REQUIRE(moq_session_close_subgroup(p->pub, sg[1], p->now) == MOQ_OK);
    REG_REQUIRE(regression_finish(p, &target, 2, "target") == MOQ_OK);
    /* Work queued AFTER the blocked terminal must still make progress for
     * another owner. D16 preserves shared-control FIFO; d18/22 can also
     * send the other terminal on its independent request stream. */
    REG_REQUIRE(write_object(p, sg[2], 2, 1, 0, 1) == MOQ_OK);
    REG_REQUIRE(moq_session_close_subgroup(p->pub, sg[2], p->now) == MOQ_OK);
    REG_REQUIRE(regression_finish(p, &other, 1, "other") == MOQ_OK);
    if (!regression_service(p)) return false;
    size_t target_count, other_count;
    uint64_t target_seq, other_seq, fin_seq[3];
    if (!regression_done(p, "target", 2, &target_count, &target_seq) ||
        !regression_done(p, "other", 1, &other_count, &other_seq)) return false;
    const bool other_control_held = refuse_fin && p->ver == MOQ_VERSION_DRAFT_16;
    MOQ_TEST_CHECK_EQ_SIZE(other_count, other_control_held ? 0 : 1);
    MOQ_TEST_CHECK_EQ_SIZE(regression_fins(p->mt, sid[1], &fin_seq[1]), 1);
    MOQ_TEST_CHECK_EQ_SIZE(regression_fins(p->mt, sid[2], &fin_seq[2]), 1);
    MOQ_TEST_CHECK(fin_seq[2] > mark);
    if (!other_control_held) MOQ_TEST_CHECK(fin_seq[2] < other_seq);
    size_t other_payloads = 0;
    for (size_t i = 0; i < p->mt->nchunks; ++i) {
        const mt_chunk_t *c = &p->mt->chunks[i];
        if (c->sid == sid[2] && c->accepted_seq > mark &&
            c->len == PAYLOAD_LEN && c->head[0] == PAYLOAD_TAG)
            other_payloads++;
    }
    MOQ_TEST_CHECK_EQ_SIZE(other_payloads, 1);
    if (refuse_fin) {
        MOQ_TEST_CHECK(blocked->fin_attempts > 0 && blocked->refused_writes > 0);
        MOQ_TEST_CHECK_EQ_SIZE(regression_fins(p->mt, sid[0], &fin_seq[0]), 0);
        MOQ_TEST_CHECK_EQ_SIZE(target_count, 0);
        MOQ_TEST_CHECK(moq_transport_bridge_has_outbound_pending(p->pub_br));
        const uint32_t attempts = blocked->fin_attempts;
        blocked->refuse_fin = false;
        if (!regression_service(p)) return false;
        MOQ_TEST_CHECK(blocked->fin_attempts > attempts);
    }
    if (!regression_done(p, "target", 2, &target_count, &target_seq) ||
        !regression_done(p, "other", 1, &other_count, &other_seq)) return false;
    MOQ_TEST_CHECK_EQ_SIZE(target_count, 1);
    MOQ_TEST_CHECK_EQ_SIZE(other_count, 1);
    if (p->ver == MOQ_VERSION_DRAFT_16) MOQ_TEST_CHECK(target_seq < other_seq);
    for (unsigned i = 0; i < 3; ++i) {
        MOQ_TEST_CHECK_EQ_SIZE(regression_fins(p->mt, sid[i], &fin_seq[i]), 1);
        MOQ_TEST_CHECK(fin_seq[i] > mark && fin_seq[i] < (i < 2 ? target_seq : other_seq));
        mt_stream_t *st = mt_stream(p->mt, sid[i]);
        REG_REQUIRE(st != NULL);
        MOQ_TEST_CHECK_EQ_INT((int)st->reset_attempts, 0);
        MOQ_TEST_CHECK_EQ_INT((int)st->writes_after_reset, 0);
    }
    MOQ_TEST_CHECK(!moq_transport_bridge_has_pending(p->pub_br));
    MOQ_TEST_CHECK(!moq_transport_bridge_has_outbound_pending(p->pub_br));
    MOQ_TEST_CHECK(!moq_session_has_pending_output(p->pub));
    serve_all(p);
    uint32_t objects[8];
    MOQ_TEST_CHECK_EQ_SIZE(wire_objects(p, objects, 8), 4);
    return true;
}

static void test_normal_fin_before_done(void)
{
    for (size_t v = 0; v < N_VERSIONS; ++v)
        for (unsigned pub = 0; pub < 2; ++pub)
            for (unsigned refused = 0; refused < 2; ++refused) {
                const int before = failures;
                pair_t p;
                if (pair_init(&p, k_versions[v], 1u << 20, true))
                    (void)normal_fin_done_row(&p, pub != 0, refused != 0);
                else MOQ_TEST_CHECK(false);
                pair_destroy(&p);
                printf("normal FIN before DONE d%d publish=%u refused=%u failures=%d\n",
                       draft_of(k_versions[v]), pub, refused, failures - before);
            }
}

/* d18 8 / d22 5.2 require an object-timeout check BEFORE transport handoff.
 * Header and whole payload are supplied together, so both drafts' clocks
 * start at t0. The exact-deadline row pins the core's existing <= policy;
 * the +1 row also covers the specs' literal "exceeds" wording. */
static bool retained_expiry_row(pair_t *p, uint64_t release_after_us,
                                 moq_rcbuf_t *payload[3])
{
    regression_owner_t a, b;
    if (!regression_owner(p, false, "untimed", 0, 0, &a) ||
        !regression_owner(p, false, "expires", 128, 1, &b)) return false;
    moq_subgroup_handle_t asg, bsg;
    uint64_t asid, bsid;
    size_t a_header, b_header;
    if (!regression_open_header(p, &a, 1, &asg, &asid, &a_header) ||
        !regression_open_header(p, &b, 1, &bsg, &bsid, &b_header)) return false;
    serve_all(p);
    mt_stream_t *ast = mt_stream(p->mt, asid), *bst = mt_stream(p->mt, bsid);
    REG_REQUIRE(ast && bst && ast->data && bst->data);
    const uint32_t b_attempts = bst->write_attempts;
    const size_t chunks_before = p->mt->nchunks;
    const uint64_t t0 = p->now, deadline = t0 + 1000;
    for (unsigned i = 0; i < 3; ++i) {
        uint8_t bytes[PAYLOAD_LEN] = { PAYLOAD_TAG, i == 0 ? 1 : 2, 1, 0,
                                       i == 2 ? 1 : 0 };
        REG_REQUIRE(moq_rcbuf_create(moq_alloc_default(), bytes, sizeof(bytes),
                                     &payload[i]) == MOQ_OK);
        REG_REQUIRE(moq_session_write_object(p->pub, i == 0 ? asg : bsg,
                i == 2 ? 1 : 0, payload[i], p->now) == MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_rcbuf_refcount(payload[i]), 2);
    }
    REG_REQUIRE(moq_session_close_subgroup(p->pub, bsg, p->now) == MOQ_OK);
    REG_REQUIRE(moq_session_next_deadline_us(p->pub) == deadline);
    ast->refuse_payload = true;
    REG_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
    /* A's object header went out, its payload was refused, and the public
     * max_pending=1 is now occupied. B's objects and FIN have left the core,
     * but even their object headers have not been attempted at the endpoint. */
    REG_REQUIRE(ast->refused_writes > 0);
    REG_REQUIRE(p->mt->nchunks == chunks_before + 1);
    REG_REQUIRE(p->mt->chunks[chunks_before].sid == asid);
    REG_REQUIRE(!moq_session_has_pending_output(p->pub));
    REG_REQUIRE(moq_transport_bridge_has_pending(p->pub_br));
    REG_REQUIRE(bst->write_attempts == b_attempts && bst->reset_attempts == 0);
    /* The retained deadline must still wake the adapter. This observation is
     * nonfatal so an old baseline also reaches the late-delivery assertions. */
    MOQ_TEST_CHECK_EQ_U64(moq_session_next_deadline_us(p->pub), deadline);
    for (unsigned i = 0; i < 3; ++i)
        MOQ_TEST_CHECK_EQ_INT((int)moq_rcbuf_refcount(payload[i]), 2);

    p->now = deadline - 1;
    REG_REQUIRE(moq_session_tick(p->pub, p->now) == MOQ_OK);
    REG_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)bst->write_attempts, (int)b_attempts);
    MOQ_TEST_CHECK_EQ_INT((int)bst->reset_attempts, 0);
    for (unsigned i = 0; i < 3; ++i)
        MOQ_TEST_CHECK_EQ_INT((int)moq_rcbuf_refcount(payload[i]), 2);

    p->now = t0 + release_after_us;
    REG_REQUIRE(moq_session_tick(p->pub, p->now) == MOQ_OK);
    ast->refuse_payload = false;
    if (!regression_service(p)) return false;
    const bool expired = release_after_us >= 1000;
    uint64_t fin_seq;
    MOQ_TEST_CHECK_EQ_SIZE(regression_fins(p->mt, bsid, &fin_seq), expired ? 0 : 1);
    MOQ_TEST_CHECK_EQ_INT((int)bst->reset_attempts, expired ? 1 : 0);
    if (expired) {
        MOQ_TEST_CHECK_EQ_INT((int)bst->write_attempts, (int)b_attempts);
        MOQ_TEST_CHECK(bst->reset);
        MOQ_TEST_CHECK_EQ_U64(bst->reset_code, MOQ_RESET_DELIVERY_TIMEOUT);
        MOQ_TEST_CHECK_EQ_U64(bst->reset_reliable, b_header);
        for (size_t i = chunks_before; i < p->mt->nchunks; ++i)
            MOQ_TEST_CHECK(p->mt->chunks[i].sid != bsid);
    }
    MOQ_TEST_CHECK_EQ_INT((int)ast->reset_attempts, 0);
    MOQ_TEST_CHECK_EQ_INT((int)bst->writes_after_reset, 0);
    for (unsigned i = 0; i < 3; ++i)
        MOQ_TEST_CHECK_EQ_INT((int)moq_rcbuf_refcount(payload[i]), 1);
    MOQ_TEST_CHECK(!moq_session_has_pending_output(p->pub));
    MOQ_TEST_CHECK(!moq_transport_bridge_has_pending(p->pub_br));
    MOQ_TEST_CHECK(!moq_transport_bridge_has_outbound_pending(p->pub_br));
    MOQ_TEST_CHECK_EQ_U64(moq_session_next_deadline_us(p->pub), UINT64_MAX);
    serve_all(p);
    const uint32_t want[] = { OBJ(1, 1, 0, 0), OBJ(2, 1, 0, 0), OBJ(2, 1, 0, 1) };
    expect_wire(p, want, expired ? 1 : 3, "retained expiry", 1u << 20);
    return true;
}

static void test_retained_expiry(void)
{
    static const uint64_t release_after_us[] = { 999, 1000, 1001 };
    for (size_t v = 1; v < N_VERSIONS; ++v)
        for (size_t t = 0; t < sizeof(release_after_us) / sizeof(release_after_us[0]); ++t) {
            const int before = failures;
            pair_t p;
            moq_rcbuf_t *payload[3] = { NULL, NULL, NULL };
            if (pair_init_retained(&p, k_versions[v], 1u << 20, true,
                                   0, 1024, 1u << 20, 1, true))
                (void)retained_expiry_row(&p, release_after_us[t], payload);
            else MOQ_TEST_CHECK(false);
            pair_destroy(&p);
            for (unsigned i = 0; i < 3; ++i)
                if (payload[i]) {
                    MOQ_TEST_CHECK_EQ_INT((int)moq_rcbuf_refcount(payload[i]), 1);
                    moq_rcbuf_decref(payload[i]);
                }
            printf("retained expiry d%d release=%llu us failures=%d\n",
                   draft_of(k_versions[v]), (unsigned long long)release_after_us[t],
                   failures - before);
        }
}

typedef enum {
    EXPIRY_PARTIAL,
    EXPIRY_PARTIAL_RESET_REFUSED,
    EXPIRY_HEADER_REFUSED,
    EXPIRY_CANCEL_FIRST,
} expiry_successor_t;

static size_t regression_resets(const mt_t *m, uint64_t sid, uint64_t *seq)
{
    size_t n = 0;
    *seq = 0;
    for (size_t i = 0; i < m->nwire; ++i)
        if (m->wire[i].sid == sid && m->wire[i].reset) {
            n++;
            *seq = m->wire_seq[i];
        }
    return n;
}

static bool regression_done_absent(const pair_t *p)
{
    size_t count;
    uint64_t seq;
    if (!regression_done(p, "successor", 1, &count, &seq)) return false;
    MOQ_TEST_CHECK_EQ_SIZE(count, 0);
    return true;
}

/* Only ONE timed object: an expired queued sibling must not mask a missing
 * expiry check for PENDING_PAYLOAD_ONLY. The header-refused variant instead
 * exercises an expired object behind its still-unaccepted subgroup prefix.
 * MOQT d18 11.4.3 / d22 11.3.2 recommend preserving that prefix. Once the
 * library selects it, RESET_STREAM_AT must not claim bytes never accepted. */
static bool expiry_successor_row(pair_t *p, expiry_successor_t which,
                                  moq_rcbuf_t **payload)
{
    const bool header_refused = which == EXPIRY_HEADER_REFUSED;
    const bool cancel_first = which == EXPIRY_CANCEL_FIRST;
    const bool reset_refused = which == EXPIRY_PARTIAL_RESET_REFUSED || cancel_first;
    const uint64_t cause = cancel_first ? 0x1 /* CANCELLED */ : MOQ_RESET_DELIVERY_TIMEOUT;
    regression_owner_t owner;
    if (!regression_owner(p, false, "expiry-successor", 128, 1, &owner)) return false;
    moq_subgroup_handle_t sg;
    uint64_t sid = 0;
    size_t header_len = 0;
    mt_stream_t *st = NULL;
    if (header_refused) {
        p->mt->cap = 0; /* control remains writable; no data write fits */
        moq_subgroup_cfg_t sc;
        moq_subgroup_cfg_init(&sc);
        sc.group_id = 1;
        sc.publisher_priority = 128;
        REG_REQUIRE(moq_session_open_subgroup(p->pub, owner.track.sub, &sc,
                                              p->now, &sg) == MOQ_OK);
    } else {
        if (!regression_open_header(p, &owner, 1, &sg, &sid, &header_len)) return false;
        serve_all(p);
        st = mt_stream(p->mt, sid);
        REG_REQUIRE(st && st->data);
        st->refuse_payload = true;
    }
    const size_t chunks_before = p->mt->nchunks;
    const uint64_t deadline = p->now + 1000;
    const uint8_t bytes[PAYLOAD_LEN] = { PAYLOAD_TAG, 1, 1, 0, 0 };
    REG_REQUIRE(moq_rcbuf_create(moq_alloc_default(), bytes, sizeof(bytes), payload) == MOQ_OK);
    REG_REQUIRE(moq_session_write_object(p->pub, sg, 0, *payload, p->now) == MOQ_OK);
    if (!cancel_first) {
        REG_REQUIRE(moq_session_close_subgroup(p->pub, sg, p->now) == MOQ_OK);
        REG_REQUIRE(regression_finish(p, &owner, 1, "successor") == MOQ_OK);
    }
    REG_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
    if (header_refused) {
        /* Identify the sole attempted data stream from endpoint observations,
         * without predicting the next id or fabricating a model stream. */
        for (size_t i = 0; i < MT_MAX_STREAMS; ++i)
            if (p->mt->streams[i].used && p->mt->streams[i].data &&
                p->mt->streams[i].write_attempts) {
                REG_REQUIRE(st == NULL);
                st = &p->mt->streams[i];
            }
        REG_REQUIRE(st != NULL);
        sid = st->sid;
        header_len = st->first_write_len;
        REG_REQUIRE(header_len > 0 && header_len <= PAYLOAD_LEN);
        REG_REQUIRE(p->mt->nchunks == chunks_before);
    } else {
        /* Object header accepted, body refused: exactly one new chunk. */
        REG_REQUIRE(p->mt->nchunks == chunks_before + 1);
        REG_REQUIRE(p->mt->chunks[chunks_before].sid == sid);
        REG_REQUIRE(p->mt->chunks[chunks_before].len > 0 &&
                     !p->mt->chunks[chunks_before].fin);
        REG_REQUIRE(moq_transport_bridge_has_pending(p->pub_br));
    }
    REG_REQUIRE(st->refused_writes > 0 && st->reset_attempts == 0);
    MOQ_TEST_CHECK_EQ_INT((int)moq_rcbuf_refcount(*payload), 2);
    MOQ_TEST_CHECK_EQ_U64(moq_session_next_deadline_us(p->pub), deadline);
    if (!regression_done_absent(p)) return false;

    p->now = deadline - 1;
    REG_REQUIRE(moq_session_tick(p->pub, p->now) == MOQ_OK);
    REG_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)st->reset_attempts, 0);
    MOQ_TEST_CHECK_EQ_INT((int)moq_rcbuf_refcount(*payload), 2);
    MOQ_TEST_CHECK_EQ_SIZE(p->mt->nchunks, chunks_before + (header_refused ? 0 : 1));
    if (!regression_done_absent(p)) return false;
    if (cancel_first) {
        /* Admit cancellation BEFORE expiry, but leave its RESET core-owned
         * until the overdue service pass. Expiry must transfer that cause,
         * not replace it with DELIVERY_TIMEOUT. */
        REG_REQUIRE(moq_session_reset_subgroup(p->pub, sg, cause, p->now) == MOQ_OK);
        REG_REQUIRE(regression_finish(p, &owner, 1, "successor") == MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)st->reset_attempts, 0);
    }
    st->refuse_reset = reset_refused;
    st->refuse_payload = false; /* a stale retry would now be accepted */
    const uint32_t writes_before_expiry = st->write_attempts;
    /* Behind a header the transport accepted, the expiring object waits in
     * the pending FIFO, not in a stream queue: nothing is counted for the
     * queued-expiry walk, and the expiry below comes from the pending
     * FIFO's own. Behind its refused header it is still queued, counted. */
    MOQ_TEST_CHECK_EQ_INT((int)p->pub_br->dq_timed, header_refused ? 1 : 0);
    p->now = deadline + 1;
    /* Advance through the bridge, without a separate due session tick:
     * timeout handling must precede its pending-payload retry. */
    if (!regression_service(p)) return false;
    MOQ_TEST_CHECK_EQ_INT((int)moq_rcbuf_refcount(*payload), 1);
    uint64_t reset_seq = 0, fin_seq = 0;
    if (header_refused) {
        MOQ_TEST_CHECK_EQ_SIZE(p->mt->nchunks, chunks_before);
        MOQ_TEST_CHECK_EQ_INT((int)st->reset_attempts, 0);
        MOQ_TEST_CHECK(moq_transport_bridge_has_outbound_pending(p->pub_br));
        if (!regression_done_absent(p)) return false;
        p->mt->cap = 1u << 20;
        if (!regression_service(p)) return false;
        /* The only newly accepted bytes are the exact previously refused
         * subgroup prefix. Neither an object header nor its body may pass. */
        MOQ_TEST_CHECK_EQ_SIZE(p->mt->nchunks, chunks_before + 1);
        REG_REQUIRE(p->mt->nchunks > chunks_before);
        const mt_chunk_t *c = &p->mt->chunks[chunks_before];
        MOQ_TEST_CHECK_EQ_U64(c->sid, sid);
        MOQ_TEST_CHECK_EQ_SIZE(c->len, header_len);
        MOQ_TEST_CHECK(memcmp(c->head, st->first_write_head, header_len) == 0);
        MOQ_TEST_CHECK(!c->fin);
        MOQ_TEST_CHECK_EQ_SIZE(regression_resets(p->mt, sid, &reset_seq), 1);
        MOQ_TEST_CHECK(c->accepted_seq < reset_seq);
    } else {
        MOQ_TEST_CHECK_EQ_INT((int)st->write_attempts, (int)writes_before_expiry);
        MOQ_TEST_CHECK_EQ_SIZE(p->mt->nchunks, chunks_before + 1);
    }
    if (reset_refused) {
        MOQ_TEST_CHECK(st->reset_attempts > 0 && !st->reset);
        MOQ_TEST_CHECK_EQ_U64(st->reset_code, cause);
        MOQ_TEST_CHECK_EQ_U64(st->reset_reliable, header_len);
        MOQ_TEST_CHECK_EQ_SIZE(regression_resets(p->mt, sid, &reset_seq), 0);
        MOQ_TEST_CHECK(moq_transport_bridge_has_outbound_pending(p->pub_br));
        if (!regression_done_absent(p)) return false;
        const uint32_t attempts = st->reset_attempts;
        st->refuse_reset = false;
        if (!regression_service(p)) return false;
        MOQ_TEST_CHECK(st->reset_attempts > attempts);
    }
    MOQ_TEST_CHECK_EQ_SIZE(regression_resets(p->mt, sid, &reset_seq), 1);
    MOQ_TEST_CHECK_EQ_SIZE(regression_fins(p->mt, sid, &fin_seq), 0);
    MOQ_TEST_CHECK(st->reset);
    MOQ_TEST_CHECK_EQ_U64(st->reset_code, cause);
    MOQ_TEST_CHECK_EQ_U64(st->reset_reliable, header_len);
    size_t done_count;
    uint64_t done_seq;
    if (!regression_done(p, "successor", 1, &done_count, &done_seq)) return false;
    MOQ_TEST_CHECK_EQ_SIZE(done_count, 1);
    MOQ_TEST_CHECK(reset_seq > 0 && reset_seq < done_seq);
    MOQ_TEST_CHECK(!moq_transport_bridge_has_pending(p->pub_br));
    MOQ_TEST_CHECK(!moq_transport_bridge_has_outbound_pending(p->pub_br));
    MOQ_TEST_CHECK(!moq_session_has_pending_output(p->pub));
    MOQ_TEST_CHECK_EQ_U64(moq_session_next_deadline_us(p->pub), UINT64_MAX);

    const uint32_t final_writes = st->write_attempts, final_resets = st->reset_attempts;
    const size_t final_chunks = p->mt->nchunks;
    if (!regression_service(p)) return false;
    serve_all(p);
    MOQ_TEST_CHECK_EQ_INT((int)st->write_attempts, (int)final_writes);
    MOQ_TEST_CHECK_EQ_INT((int)st->reset_attempts, (int)final_resets);
    MOQ_TEST_CHECK_EQ_INT((int)st->writes_after_reset, 0);
    MOQ_TEST_CHECK_EQ_SIZE(p->mt->nchunks, final_chunks);
    for (size_t i = 0; i < p->mt->nchunks; ++i)
        MOQ_TEST_CHECK(!(p->mt->chunks[i].len == PAYLOAD_LEN &&
                         p->mt->chunks[i].head[0] == PAYLOAD_TAG));
    uint32_t objects[2];
    MOQ_TEST_CHECK_EQ_SIZE(wire_objects(p, objects, 2), 0);
    if (!regression_done(p, "successor", 1, &done_count, &done_seq)) return false;
    MOQ_TEST_CHECK_EQ_SIZE(done_count, 1);
    MOQ_TEST_CHECK_EQ_INT((int)moq_rcbuf_refcount(*payload), 1);
    return true;
}

static void test_expiry_successors(void)
{
    static const char *names[] = { "partial", "partial-reset-refused",
                                   "header-refused", "cancel-first" };
    for (size_t v = 1; v < N_VERSIONS; ++v)
        for (unsigned row = 0; row < sizeof(names) / sizeof(names[0]); ++row) {
            const int before = failures;
            pair_t p;
            moq_rcbuf_t *payload = NULL;
            if (pair_init_retained(&p, k_versions[v], 1u << 20, true,
                                   0, 1024, 1u << 20, 1, true))
                (void)expiry_successor_row(&p, (expiry_successor_t)row, &payload);
            else MOQ_TEST_CHECK(false);
            pair_destroy(&p);
            if (payload) {
                MOQ_TEST_CHECK_EQ_INT((int)moq_rcbuf_refcount(payload), 1);
                moq_rcbuf_decref(payload);
            }
            printf("expiry successor d%d %s failures=%d\n",
                   draft_of(k_versions[v]), names[row], failures - before);
        }
}

/* Expiry that starts in the core's action ring with no keyed dstream
 * (core_owned), or in a keyed bridge pending payload, with no FIN/DONE or
 * unrelated work to obscure the reset-only pending-query contract. */
static bool expiry_review_row(pair_t *p, bool core_owned, moq_rcbuf_t **payload)
{
    regression_owner_t owner;
    if (!regression_owner(p, false, "expiry-review", 128, 1, &owner)) return false;
    moq_subgroup_handle_t sg;
    uint64_t sid;
    size_t header_len;
    if (!regression_open_header(p, &owner, 1, &sg, &sid, &header_len)) return false;
    serve_all(p);
    mt_stream_t *st = mt_stream(p->mt, sid);
    REG_REQUIRE(st && st->data);
    const size_t chunks_before = p->mt->nchunks;
    const size_t slots_before = moq_session_action_capacity(p->pub);
    const uint32_t header_writes = st->write_attempts;
    const uint64_t deadline = p->now + 1000;
    const uint8_t bytes[PAYLOAD_LEN] = { PAYLOAD_TAG, 1, 1, 0, 0 };
    REG_REQUIRE(moq_rcbuf_create(moq_alloc_default(), bytes, sizeof(bytes), payload) == MOQ_OK);
    REG_REQUIRE(moq_session_write_object(p->pub, sg, 0, *payload, p->now) == MOQ_OK);
    REG_REQUIRE(moq_session_has_pending_output(p->pub));
    REG_REQUIRE(moq_rcbuf_refcount(*payload) == 2);
    st->refuse_reset = true;
    if (core_owned) {
        /* No bridge service/poll: the object still occupies one core slot.
         * Tick must release its payload and replace it with RESET_DATA before
         * DONE is admitted. Spare pending slots select normal reset dispatch. */
        REG_REQUIRE(moq_session_action_capacity(p->pub) + 1 == slots_before);
        REG_REQUIRE(st->write_attempts == header_writes);
        p->now = deadline + 1;
        REG_REQUIRE(moq_session_tick(p->pub, p->now) == MOQ_OK);
        REG_REQUIRE(moq_rcbuf_refcount(*payload) == 1);
        REG_REQUIRE(moq_session_has_pending_output(p->pub));
        REG_REQUIRE(st->reset_attempts == 0 && st->write_attempts == header_writes);
        REG_REQUIRE(regression_finish(p, &owner, 1, "successor") == MOQ_OK);
    } else {
        st->refuse_payload = true;
        REG_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
        REG_REQUIRE(p->mt->nchunks == chunks_before + 1);
        REG_REQUIRE(st->refused_writes > 0 && st->reset_attempts == 0);
        REG_REQUIRE(moq_rcbuf_refcount(*payload) == 2);
        REG_REQUIRE(!moq_session_has_pending_output(p->pub));
        REG_REQUIRE(moq_transport_bridge_has_pending(p->pub_br));
        st->refuse_payload = false;
        p->now = deadline + 1;
    }
    const uint32_t writes_before_reset = st->write_attempts;
    if (!regression_service(p)) return false;
    MOQ_TEST_CHECK(st->reset_attempts > 0 && !st->reset);
    MOQ_TEST_CHECK_EQ_U64(st->reset_code, MOQ_RESET_DELIVERY_TIMEOUT);
    MOQ_TEST_CHECK_EQ_U64(st->reset_reliable, header_len);
    MOQ_TEST_CHECK_EQ_INT((int)moq_rcbuf_refcount(*payload), 1);
    MOQ_TEST_CHECK_EQ_INT((int)st->write_attempts, (int)writes_before_reset);
    MOQ_TEST_CHECK_EQ_SIZE(p->mt->nchunks, chunks_before + (core_owned ? 0 : 1));
    uint64_t reset_seq, fin_seq;
    MOQ_TEST_CHECK_EQ_SIZE(regression_resets(p->mt, sid, &reset_seq), 0);
    MOQ_TEST_CHECK_EQ_SIZE(regression_fins(p->mt, sid, &fin_seq), 0);
    if (!regression_done_absent(p)) return false;
    if (!core_owned) {
        MOQ_TEST_CHECK(!moq_session_has_pending_output(p->pub));
        MOQ_TEST_CHECK_EQ_SIZE(p->mt->nctrl, 0);
    }
    /* R3's essential observations are nonfatal: still release/reset the
     * stream and verify the inverse transition on the captured broken code. */
    MOQ_TEST_CHECK(moq_transport_bridge_has_pending(p->pub_br));
    MOQ_TEST_CHECK(moq_transport_bridge_has_outbound_pending(p->pub_br));
    const uint32_t refused_attempts = st->reset_attempts;
    st->refuse_reset = false;
    if (!regression_service(p)) return false;
    MOQ_TEST_CHECK(st->reset && st->reset_attempts > refused_attempts);
    MOQ_TEST_CHECK_EQ_SIZE(regression_resets(p->mt, sid, &reset_seq), 1);
    MOQ_TEST_CHECK_EQ_U64(st->reset_code, MOQ_RESET_DELIVERY_TIMEOUT);
    MOQ_TEST_CHECK_EQ_U64(st->reset_reliable, header_len);
    size_t done_count;
    uint64_t done_seq;
    if (!regression_done(p, "successor", 1, &done_count, &done_seq)) return false;
    MOQ_TEST_CHECK_EQ_SIZE(done_count, core_owned ? 1 : 0);
    if (core_owned) MOQ_TEST_CHECK(reset_seq > 0 && reset_seq < done_seq);
    MOQ_TEST_CHECK(!moq_session_has_pending_output(p->pub));
    MOQ_TEST_CHECK(!moq_transport_bridge_has_pending(p->pub_br));
    MOQ_TEST_CHECK(!moq_transport_bridge_has_outbound_pending(p->pub_br));
    MOQ_TEST_CHECK_EQ_U64(moq_session_next_deadline_us(p->pub), UINT64_MAX);
    serve_all(p);
    MOQ_TEST_CHECK_EQ_INT((int)st->write_attempts, (int)writes_before_reset);
    MOQ_TEST_CHECK_EQ_INT((int)st->writes_after_reset, 0);
    MOQ_TEST_CHECK_EQ_SIZE(p->mt->nchunks, chunks_before + (core_owned ? 0 : 1));
    MOQ_TEST_CHECK_EQ_SIZE(regression_resets(p->mt, sid, &reset_seq), 1);
    MOQ_TEST_CHECK_EQ_SIZE(regression_fins(p->mt, sid, &fin_seq), 0);
    uint32_t objects[2];
    MOQ_TEST_CHECK_EQ_SIZE(wire_objects(p, objects, 2), 0);
    MOQ_TEST_CHECK_EQ_INT((int)moq_rcbuf_refcount(*payload), 1);
    return true;
}

static void test_expiry_review(unsigned finding)
{
    for (size_t v = 1; v < N_VERSIONS; ++v) {
        const int before = failures;
        pair_t p;
        moq_rcbuf_t *payload = NULL;
        /* The partial-successor and core-ring rows exercise the supported
         * max_actions<4 fallback, which cannot opt in to keyed data actions.
         * The core-ring row needs spare pending slots so the pressure-reset
         * path cannot mask the missing owner. */
        if (pair_init_retained(&p, k_versions[v], 1u << 20, true, 0,
                               finding == 3 ? 1024 : 3, 1u << 20,
                               finding == 2 ? 4 : 1, true)) {
            if (finding == 1)
                (void)expiry_successor_row(&p, EXPIRY_PARTIAL, &payload);
            else
                (void)expiry_review_row(&p, finding == 2, &payload);
        } else MOQ_TEST_CHECK(false);
        pair_destroy(&p);
        if (payload) {
            MOQ_TEST_CHECK_EQ_INT((int)moq_rcbuf_refcount(payload), 1);
            moq_rcbuf_decref(payload);
        }
        printf("expiry review R%u d%d failures=%d\n",
               finding, draft_of(k_versions[v]), failures - before);
    }
}

#undef REG_REQUIRE

/* [RST-06] A retained-data budget cannot wait for the very RESET that will
 * release it. D18 5.1.1 / D22 3.1.2 request cancellation must reach the data
 * stream even with queued sibling data and additional cancelled-stream data
 * ahead of that reset. All requests and peer cancellation use real ingress.
 * Public bridge configuration selects the cap; no private state is changed.
 */
static void retained_cancel_row(moq_version_t ver, uint64_t retained_cap,
                                bool peer_cancel, bool queued_tail,
                                bool reset_blocked, bool reliable_reset)
{
    const int before = failures;
    pair_t p;
    moq_rcbuf_t *payload = NULL;
    uint8_t *bytes = NULL;
    if (!pair_init_retained(&p, ver, 4096, true, 0, 1024, retained_cap,
                           0, reliable_reset)) {
        MOQ_TEST_CHECK(false);
        goto done;
    }
    track_t blocked, healthy;
    moq_subgroup_handle_t blocked_sg, healthy_sg;
    if (!subscribe(&p, "blocked", 128, NONE, NONE, &blocked) ||
        !subscribe(&p, "healthy", 128, NONE, NONE, &healthy) ||
        !publish_group(&p, &blocked, 1, 1, 0, 128, 1, true, &blocked_sg) ||
        !publish_group(&p, &healthy, 2, 1, 0, 128, 1, true, &healthy_sg)) {
        MOQ_TEST_CHECK(false);
        goto done;
    }
    serve_all(&p);
    uint64_t blocked_sid = 0, healthy_sid = 0;
    if (!stream_of(&p, 1, 1, &blocked_sid) ||
        !stream_of(&p, 2, 1, &healthy_sid)) {
        MOQ_TEST_CHECK(false);
        goto done;
    }
    mt_stream_t *st = mt_stream(p.mt, blocked_sid);
    if (!st) { MOQ_TEST_CHECK(false); goto done; }
    st->refuse = true;
    st->refuse_reset = reset_blocked;
    size_t header_len = 0;
    for (size_t i = 0; i < p.mt->nchunks; ++i)
        if (p.mt->chunks[i].sid == blocked_sid) {
            header_len = p.mt->chunks[i].len;
            break;
        }
    MOQ_TEST_CHECK(header_len != 0 && header_len < PAYLOAD_LEN);
    const size_t wire_before = p.mt->nwire;
    const size_t payload_len = retained_cap == MOQ_TRANSPORT_BRIDGE_RETAINED_DEFAULT
        ? (size_t)retained_cap : PAYLOAD_LEN;
    bytes = calloc(1, payload_len);
    if (!bytes) { MOQ_TEST_CHECK(false); goto done; }
    bytes[0] = PAYLOAD_TAG;
    bytes[1] = 1;
    if (moq_rcbuf_create(moq_alloc_default(), bytes, payload_len, &payload) != MOQ_OK) {
        MOQ_TEST_CHECK(false);
        goto done;
    }
    free(bytes);
    bytes = NULL;
    if (moq_session_write_object(p.pub, blocked_sg, 1, payload, p.now) != MOQ_OK ||
        moq_transport_bridge_service(p.pub_br, p.now) != MOQ_OK) {
        MOQ_TEST_CHECK(false);
        goto done;
    }
    /* The write was refused and still owns its payload before cancellation. */
    MOQ_TEST_CHECK(st->refused_writes != 0);
    MOQ_TEST_CHECK_EQ_INT((int)moq_rcbuf_refcount(payload), 2);
    if (queued_tail)
        MOQ_TEST_CHECK_EQ_INT((int)write_object(&p, blocked_sg, 1, 1, 0, 2), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)write_object(&p, healthy_sg, 2, 1, 0, 1), MOQ_OK);
    const moq_result_t cancelled = peer_cancel
        ? moq_session_unsubscribe(p.sub, blocked.csub, p.now)
        : moq_session_reset_subgroup(p.pub, blocked_sg, 1, p.now);
    if (cancelled != MOQ_OK) { MOQ_TEST_CHECK(false); goto done; }
    if (reset_blocked) {
        /* This control follows RESET; healthy data precedes it. Refusal may
         * hold the control, but must neither hide the reset nor hold B. */
        p.mt->ctrl_mark = p.mt->op_seq;
        moq_done_subscribe_cfg_t dc;
        moq_done_subscribe_cfg_init(&dc);
        dc.status_code = 2;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_done_subscribe(p.pub, blocked.sub,
                                                              &dc, p.now), MOQ_OK);
    }
    if (peer_cancel) shuttle(&p);
    for (unsigned i = 0; i < 3; ++i)
        MOQ_TEST_CHECK_EQ_INT((int)moq_transport_bridge_service(p.pub_br, p.now), MOQ_OK);
    if (reset_blocked) {
        MOQ_TEST_CHECK(!was_reset(&p, blocked_sid));
        MOQ_TEST_CHECK(st->reset_attempts != 0);
        MOQ_TEST_CHECK_EQ_INT((int)moq_rcbuf_refcount(payload), 1);
        MOQ_TEST_CHECK_EQ_U64(st->reset_code, 1);
        MOQ_TEST_CHECK_EQ_U64(st->reset_reliable, reliable_reset ? header_len : 0);
        MOQ_TEST_CHECK(moq_transport_bridge_has_pending(p.pub_br));
        MOQ_TEST_CHECK(moq_transport_bridge_has_outbound_pending(p.pub_br));
        size_t healthy_accepted = 0;
        for (size_t i = 0; i < p.mt->nchunks; ++i)
            if (p.mt->chunks[i].sid == healthy_sid &&
                p.mt->chunks[i].len == PAYLOAD_LEN &&
                p.mt->chunks[i].head[0] == PAYLOAD_TAG &&
                p.mt->chunks[i].head[4] == 1)
                healthy_accepted++;
        MOQ_TEST_CHECK_EQ_SIZE(healthy_accepted, 1);
        MOQ_TEST_CHECK_EQ_U64(p.mt->ctrl_first_after, 0);
        MOQ_TEST_CHECK(write_object(&p, blocked_sg, 1, 1, 0, 3) != MOQ_OK);
        /* Late notifications cannot erase or replace the retained reset. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_transport_bridge_on_local_stream_committed(
            p.pub_br, blocked_sid, p.now), MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_transport_bridge_on_peer_stop_sending(
            p.pub_br, blocked_sid, 0x55, p.now), MOQ_OK);
        MOQ_TEST_CHECK(moq_transport_bridge_has_outbound_pending(p.pub_br));
        st->refuse_reset = false;
        for (unsigned i = 0; i < 3 &&
             moq_transport_bridge_has_outbound_pending(p.pub_br); ++i)
            MOQ_TEST_CHECK_EQ_INT((int)moq_transport_bridge_service(p.pub_br, p.now), MOQ_OK);
        MOQ_TEST_CHECK(!moq_transport_bridge_has_pending(p.pub_br));
        MOQ_TEST_CHECK(!moq_transport_bridge_has_outbound_pending(p.pub_br));
        MOQ_TEST_CHECK(p.mt->ctrl_first_after > p.mt->ctrl_mark);
    }
    MOQ_TEST_CHECK(was_reset(&p, blocked_sid));
    MOQ_TEST_CHECK_EQ_INT((int)moq_rcbuf_refcount(payload), 1);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(p.pub_br));
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(p.pub), MOQ_SESS_ESTABLISHED);

    /* Becoming writable later must not resurrect the cancelled tail. */
    st->refuse = false;
    serve_all(&p);
    unsigned reset_count = 0, healthy_objects = 0;
    for (size_t i = wire_before; i < p.mt->nwire; ++i) {
        if (p.mt->wire[i].sid == blocked_sid) {
            if (p.mt->wire[i].reset) reset_count++;
            else MOQ_TEST_CHECK(false);
        }
        if (p.mt->wire[i].sid == healthy_sid &&
            p.mt->wire[i].len == PAYLOAD_LEN &&
            p.mt->wire[i].head[0] == PAYLOAD_TAG &&
            p.mt->wire[i].head[4] == 1)
            healthy_objects++;
    }
    MOQ_TEST_CHECK_EQ_INT((int)reset_count, 1);
    MOQ_TEST_CHECK_EQ_INT((int)healthy_objects, 1);
    MOQ_TEST_CHECK_EQ_INT((int)st->writes_after_reset, 0);
    MOQ_TEST_CHECK_EQ_INT((int)moq_rcbuf_refcount(payload), 1);
done:
    pair_destroy(&p);
    free(bytes);
    if (payload) {
        MOQ_TEST_CHECK_EQ_INT((int)moq_rcbuf_refcount(payload), 1);
        moq_rcbuf_decref(payload);
    }
    printf("retained cancellation d%d cap=%llu peer=%d tail=%d reset_blocked=%d reliable=%d failures=%d\n",
           draft_of(ver), (unsigned long long)retained_cap,
           (int)peer_cancel, (int)queued_tail, (int)reset_blocked,
           (int)reliable_reset, failures - before);
}

/* An unhanded header is a real prerequisite, even under byte pressure. The
 * pressure path must not lower Reliable Size or report reset acceptance until
 * that header has been accepted. This is a model endpoint refusal, not loss. */
static void retained_header_row(moq_version_t ver, bool reliable_reset)
{
    const int before = failures;
    pair_t p;
    if (!pair_init_retained(&p, ver, 4096, true, 0, 1024, 1, 0, reliable_reset)) {
        MOQ_TEST_CHECK(false);
        goto done;
    }
    track_t t;
    moq_subgroup_handle_t sg;
    if (!subscribe(&p, "header", 128, NONE, NONE, &t)) {
        MOQ_TEST_CHECK(false);
        goto done;
    }
    const uint64_t sid = p.mt->next_uni;
    mt_stream_t *st = mt_stream(p.mt, sid);
    if (!st) { MOQ_TEST_CHECK(false); goto done; }
    st->refuse = true;
    if (!publish_group(&p, &t, 1, 1, 0, 128, 1, true, &sg) ||
        moq_session_reset_subgroup(p.pub, sg, 1, p.now) != MOQ_OK) {
        MOQ_TEST_CHECK(false);
        goto done;
    }
    for (unsigned i = 0; i < 3; ++i)
        MOQ_TEST_CHECK_EQ_INT((int)moq_transport_bridge_service(p.pub_br, p.now), MOQ_OK);
    MOQ_TEST_CHECK(st->refused_writes != 0);
    MOQ_TEST_CHECK_EQ_INT((int)st->reset_attempts, 0);
    MOQ_TEST_CHECK(!was_reset(&p, sid));
    for (size_t i = 0; i < p.mt->nchunks; ++i)
        MOQ_TEST_CHECK(p.mt->chunks[i].sid != sid);
    st->refuse = false;
    for (unsigned i = 0; i < 3; ++i)
        MOQ_TEST_CHECK_EQ_INT((int)moq_transport_bridge_service(p.pub_br, p.now), MOQ_OK);
    size_t headers = 0, header_len = 0;
    for (size_t i = 0; i < p.mt->nchunks; ++i)
        if (p.mt->chunks[i].sid == sid) {
            headers++;
            header_len = p.mt->chunks[i].len;
        }
    MOQ_TEST_CHECK_EQ_SIZE(headers, 1);
    MOQ_TEST_CHECK(header_len != 0 && header_len < PAYLOAD_LEN);
    MOQ_TEST_CHECK(was_reset(&p, sid));
    MOQ_TEST_CHECK_EQ_INT((int)st->reset_attempts, 1);
    MOQ_TEST_CHECK_EQ_U64(st->reset_reliable, reliable_reset ? header_len : 0);
    MOQ_TEST_CHECK_EQ_INT((int)st->writes_after_reset, 0);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(p.pub_br));
done:
    pair_destroy(&p);
    printf("retained header d%d reliable=%d failures=%d\n",
           draft_of(ver), (int)reliable_reset, failures - before);
}

/* A full core ring leaves peer cancellation on its subgroup owner. Production
 * ingress creates RESET_OWED; private fields are observed, never synthesized.
 * Interleaved survivors wrap the action ring and retain their metadata. */
static void retained_owed_row(moq_version_t ver, bool expire, bool refuse_reset)
{
    const int before = failures;
    pair_t p;
    if (!pair_init_retained(&p, ver, 4096, true, 0, 8, 1, 0, true)) {
        MOQ_TEST_CHECK(false);
        goto done;
    }
    track_t a, b;
    moq_subgroup_handle_t asg, bsg;
    if (!subscribe_timed(&p, "owed", 128, NONE, NONE,
                          ver == MOQ_VERSION_DRAFT_16 ? 0 : 1, &a) ||
        !subscribe(&p, "survivor", 64, DESC, NONE, &b) ||
        !publish_group(&p, &a, 1, 1, 0, 128, 1, true, &asg) ||
        !publish_group(&p, &b, 2, 1, 0, 128, 1, true, &bsg)) {
        MOQ_TEST_CHECK(false);
        goto done;
    }
    serve_all(&p);
    uint64_t asid = 0, bsid = 0;
    if (!stream_of(&p, 1, 1, &asid) || !stream_of(&p, 2, 1, &bsid)) {
        MOQ_TEST_CHECK(false);
        goto done;
    }
    mt_stream_t *st = mt_stream(p.mt, asid);
    if (!st) { MOQ_TEST_CHECK(false); goto done; }
    uint64_t next_b = 1;
    for (unsigned i = 0; i < 8 && p.pub->action_head % p.pub->action_cap != 6; ++i) {
        MOQ_TEST_CHECK_EQ_INT((int)write_object(&p, bsg, 2, 1, 0, next_b++), MOQ_OK);
        serve_all(&p);
    }
    MOQ_TEST_CHECK_EQ_SIZE(p.pub->action_head % p.pub->action_cap, 6);
    st->refuse = true;
    st->refuse_reset = refuse_reset;
    MOQ_TEST_CHECK_EQ_INT((int)write_object(&p, asg, 1, 1, 0, 1), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_transport_bridge_service(p.pub_br, p.now), MOQ_OK);
    MOQ_TEST_CHECK(st->refused_writes != 0);
    MOQ_TEST_CHECK_EQ_SIZE(p.pub->action_tail - p.pub->action_head, 0);
    const size_t chunks_before = p.mt->nchunks;
    const size_t head = p.pub->action_head;
    moq_action_meta_t survivor_meta[4];
    for (unsigned i = 0; i < 8; ++i) {
        moq_result_t rc = (i % 2 == 0)
            ? write_object(&p, asg, 1, 1, 0, 2 + i / 2)
            : write_object(&p, bsg, 2, 1, 0, next_b++);
        if (rc != MOQ_OK) { MOQ_TEST_CHECK(false); goto done; }
        if (i % 2 != 0)
            survivor_meta[i / 2] = p.pub->action_meta[(head + i) % p.pub->action_cap];
    }
    MOQ_TEST_CHECK_EQ_SIZE(p.pub->action_tail - p.pub->action_head, p.pub->action_cap);
    MOQ_TEST_CHECK(head % p.pub->action_cap != 0);
    moq_sg_entry_t *sg = NULL;
    const moq_sub_entry_t *sub = NULL;
    for (size_t i = 0; i < p.pub->sg_cap; ++i)
        if (moq_subscription_eq(p.pub->subgroups[i].sub, a.sub)) {
            sg = &p.pub->subgroups[i];
        } else if (moq_subscription_eq(p.pub->subgroups[i].sub, b.sub)) {
            p.mt->capture_ref = p.pub->subgroups[i].stream_ref;
        }
    for (size_t i = 0; i < p.pub->sub_cap; ++i)
        if (moq_subscription_eq(p.pub->subs[i].handle, a.sub)) sub = &p.pub->subs[i];
    if (!sg || !sub) { MOQ_TEST_CHECK(false); goto done; }
    p.mt->capture_core = p.pub;
    const uint64_t header_len = sg->header_len;
    if (expire) p.now += 2000;
    if (ver == MOQ_VERSION_DRAFT_16) {
        /* Encode the actual UNSUBSCRIBE, without polling publisher output. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_unsubscribe(p.sub, a.csub, p.now), MOQ_OK);
        moq_action_t action;
        if (moq_session_poll_actions(p.sub, &action, 1) != 1) {
            MOQ_TEST_CHECK(false);
            goto done;
        }
        MOQ_TEST_CHECK_EQ_INT((int)action.kind, MOQ_ACTION_SEND_CONTROL);
        const moq_result_t rc = action.kind == MOQ_ACTION_SEND_CONTROL
            ? moq_session_on_control_bytes(p.pub, action.u.send_control.data,
                                            action.u.send_control.len, p.now)
            : MOQ_ERR_INTERNAL;
        moq_action_cleanup(&action);
        MOQ_TEST_CHECK_EQ_INT((int)rc, MOQ_OK);
    } else {
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_bidi_stream_reset(p.pub,
            sub->rs.ref, 1, p.now), MOQ_OK);
    }
    MOQ_TEST_CHECK_EQ_INT((int)sg->state, MOQ_SG_RESET_OWED);
    MOQ_TEST_CHECK_EQ_U64(sg->owed_reset_code, 1);
    MOQ_TEST_CHECK_EQ_SIZE(p.pub->action_tail - p.pub->action_head, p.pub->action_cap);
    size_t expired = 0;
    for (size_t i = p.pub->action_head; i < p.pub->action_tail; ++i) {
        const size_t idx = i % p.pub->action_cap;
        MOQ_TEST_CHECK_EQ_INT((int)p.pub->actions[idx].kind, MOQ_ACTION_SEND_DATA);
        if (p.pub->action_meta[idx].deadline_us <= p.now) expired++;
    }
    MOQ_TEST_CHECK_EQ_SIZE(expired, expire ? 4 : 0);
    for (unsigned i = 0; i < 3; ++i)
        MOQ_TEST_CHECK_EQ_INT((int)moq_transport_bridge_service(p.pub_br, p.now), MOQ_OK);
    MOQ_TEST_CHECK(st->reset_attempts != 0);
    MOQ_TEST_CHECK_EQ_U64(st->reset_code, 1);
    MOQ_TEST_CHECK_EQ_U64(st->reset_reliable, header_len);
    size_t survivors = 0;
    for (size_t i = chunks_before; i < p.mt->nchunks; ++i) {
        MOQ_TEST_CHECK(p.mt->chunks[i].sid != asid);
        if (p.mt->chunks[i].sid == bsid && p.mt->chunks[i].len == PAYLOAD_LEN &&
            p.mt->chunks[i].head[0] == PAYLOAD_TAG) survivors++;
    }
    MOQ_TEST_CHECK_EQ_SIZE(survivors, 4);
    MOQ_TEST_CHECK_EQ_SIZE(p.mt->captured_count, 4);
    for (size_t i = 0; i < p.mt->captured_count && i < 4; ++i) {
        const moq_action_meta_t *actual = &p.mt->captured_meta[i];
        MOQ_TEST_CHECK_EQ_INT(actual->subscriber_priority, 64);
        MOQ_TEST_CHECK_EQ_INT(actual->group_order, DESC);
        MOQ_TEST_CHECK_EQ_U64(actual->owner, survivor_meta[i].owner);
        MOQ_TEST_CHECK_EQ_U64(actual->group_id, survivor_meta[i].group_id);
        MOQ_TEST_CHECK_EQ_INT(actual->mode, survivor_meta[i].mode);
        MOQ_TEST_CHECK_EQ_U64(actual->deadline_us, UINT64_MAX);
    }
    if (refuse_reset) {
        MOQ_TEST_CHECK(!was_reset(&p, asid));
        MOQ_TEST_CHECK(moq_transport_bridge_has_pending(p.pub_br));
        MOQ_TEST_CHECK(moq_transport_bridge_has_outbound_pending(p.pub_br));
        st->refuse_reset = false;
        for (unsigned i = 0; i < 3 &&
             moq_transport_bridge_has_outbound_pending(p.pub_br); ++i)
            MOQ_TEST_CHECK_EQ_INT((int)moq_transport_bridge_service(p.pub_br, p.now), MOQ_OK);
    }
    MOQ_TEST_CHECK(was_reset(&p, asid));
    MOQ_TEST_CHECK_EQ_U64(st->reset_code, 1);
    MOQ_TEST_CHECK(!moq_transport_bridge_has_pending(p.pub_br));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(p.pub_br));
    size_t resets = 0;
    for (size_t i = 0; i < p.mt->nwire; ++i)
        if (p.mt->wire[i].sid == asid && p.mt->wire[i].reset) resets++;
    MOQ_TEST_CHECK_EQ_SIZE(resets, 1);
done:
    pair_destroy(&p);
    printf("retained owed d%d expire=%d refused=%d failures=%d\n",
           draft_of(ver), (int)expire, (int)refuse_reset, failures - before);
}

/* Full pending capacity is distinct from the data-byte budget. The partial
 * object is real (header accepted, payload refused); only its own reset may
 * discard it. An older connection-control item remains a barrier. */
static void retained_pending_row(moq_version_t ver, bool same_stream,
                                  bool connection_barrier)
{
    const int before = failures;
    pair_t p;
    if (!pair_init_retained(&p, ver, 4096, true, 0, 1024, 0, 1, true)) {
        MOQ_TEST_CHECK(false);
        goto done;
    }
    track_t a, b;
    moq_subgroup_handle_t asg, bsg;
    if (!subscribe(&p, "cancelled", 128, NONE, NONE, &a) ||
        !subscribe(&p, "pending", 128, NONE, NONE, &b) ||
        !publish_group(&p, &a, 1, 1, 0, 128, 1, true, &asg) ||
        !publish_group(&p, &b, 2, 1, 0, 128, 1, true, &bsg)) {
        MOQ_TEST_CHECK(false);
        goto done;
    }
    serve_all(&p);
    uint64_t asid = 0, bsid = 0;
    if (!stream_of(&p, 1, 1, &asid) || !stream_of(&p, 2, 1, &bsid)) {
        MOQ_TEST_CHECK(false);
        goto done;
    }
    mt_stream_t *st = mt_stream(p.mt, asid);
    if (!st) { MOQ_TEST_CHECK(false); goto done; }
    const size_t chunks_before = p.mt->nchunks;
    if (connection_barrier) {
        p.mt->refuse_control = true;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_grant_request_capacity(p.pub,
            moq_session_local_request_capacity(p.pub) + 1, p.now), MOQ_OK);
    } else {
        p.mt->cap = 1;
        MOQ_TEST_CHECK_EQ_INT((int)write_object(&p, same_stream ? asg : bsg,
            same_stream ? 1 : 2, 1, 0, 1), MOQ_OK);
    }
    MOQ_TEST_CHECK_EQ_INT((int)moq_transport_bridge_service(p.pub_br, p.now), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)p.pub_br->pending_count, 1);
    MOQ_TEST_CHECK_EQ_INT((int)p.pub_br->max_pending, 1);
    MOQ_TEST_CHECK(p.pub_br->dq_bytes < p.pub_br->max_retained_bytes);
    if (!connection_barrier) {
        MOQ_TEST_CHECK_EQ_INT((int)p.pub_br->pending[0].kind, PENDING_PAYLOAD_ONLY);
        MOQ_TEST_CHECK_EQ_U64(p.pub_br->pending[0].stream_id, same_stream ? asid : bsid);
    }
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_reset_subgroup(p.pub, asg, 1, p.now), MOQ_OK);
    for (unsigned i = 0; i < 3; ++i)
        MOQ_TEST_CHECK_EQ_INT((int)moq_transport_bridge_service(p.pub_br, p.now), MOQ_OK);
    if (connection_barrier) {
        MOQ_TEST_CHECK_EQ_INT((int)st->reset_attempts, 0);
        MOQ_TEST_CHECK(!was_reset(&p, asid));
        MOQ_TEST_CHECK_EQ_INT((int)p.pub_br->pending_count, 1);
        MOQ_TEST_CHECK(moq_transport_bridge_has_outbound_pending(p.pub_br));
        p.mt->refuse_control = false;
    } else {
        MOQ_TEST_CHECK(was_reset(&p, asid));
        MOQ_TEST_CHECK_EQ_INT((int)p.pub_br->pending_count, same_stream ? 0 : 1);
    }
    p.mt->cap = 4096;
    serve_all(&p);
    MOQ_TEST_CHECK(was_reset(&p, asid));
    MOQ_TEST_CHECK_EQ_INT((int)st->reset_attempts, 1);
    size_t payloads = 0;
    for (size_t i = chunks_before; i < p.mt->nchunks; ++i) {
        const mt_chunk_t *c = &p.mt->chunks[i];
        if (c->len == PAYLOAD_LEN && c->head[0] == PAYLOAD_TAG) {
            MOQ_TEST_CHECK_EQ_U64(c->sid, bsid);
            payloads++;
        }
    }
    MOQ_TEST_CHECK_EQ_SIZE(payloads, !same_stream && !connection_barrier ? 1 : 0);
    MOQ_TEST_CHECK_EQ_INT((int)st->writes_after_reset, 0);
    MOQ_TEST_CHECK(!moq_transport_bridge_has_pending(p.pub_br));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(p.pub_br));
done:
    pair_destroy(&p);
    printf("retained pending d%d same=%d connection_barrier=%d failures=%d\n",
           draft_of(ver), (int)same_stream, (int)connection_barrier, failures - before);
}

static void test_retained_cancellation(void)
{
    static const uint64_t caps[] = { 4096, 1, MOQ_TRANSPORT_BRIDGE_RETAINED_DEFAULT };
    for (size_t v = 0; v < N_VERSIONS; ++v)
        for (size_t c = 0; c < sizeof(caps) / sizeof(caps[0]); ++c)
            for (unsigned peer = 0; peer < 2; ++peer)
                for (unsigned tail = 0; tail < 2; ++tail)
                    retained_cancel_row(k_versions[v], caps[c], peer != 0,
                                        tail != 0, false, false);
    for (size_t v = 0; v < N_VERSIONS; ++v)
        for (unsigned reliable = 0; reliable < 2; ++reliable) {
            retained_cancel_row(k_versions[v], MOQ_TRANSPORT_BRIDGE_RETAINED_DEFAULT,
                                false, true, true, reliable != 0);
            retained_header_row(k_versions[v], reliable != 0);
        }
    for (size_t v = 0; v < N_VERSIONS; ++v)
        for (unsigned refused = 0; refused < 2; ++refused) {
            retained_owed_row(k_versions[v], false, refused != 0);
            if (k_versions[v] != MOQ_VERSION_DRAFT_16)
                retained_owed_row(k_versions[v], true, refused != 0);
        }
    for (size_t v = 0; v < N_VERSIONS; ++v)
        for (unsigned same = 0; same < 2; ++same)
            retained_pending_row(k_versions[v], same != 0, false);
    retained_pending_row(MOQ_VERSION_DRAFT_16, false, true);
}

/* -- Shared admission (MOQ_TRANSPORT_CAP_SHARED_ADMISSION) ------------------
 *
 * The model's send queue has one byte cap for the whole connection, as the
 * picoquic adapters' does, and it refuses a write only at that cap (these
 * rows set no per-stream refusal); a FIN-only write still goes. That is the
 * capability's contract, so each row also runs with it advertised. */

static void mt_set_shared(mt_t *m, bool shared)
{
    if (shared) m->ops.capabilities |= MOQ_TRANSPORT_CAP_SHARED_ADMISSION;
    else m->ops.capabilities &= ~(uint32_t)MOQ_TRANSPORT_CAP_SHARED_ADMISSION;
}

static uint32_t mt_refused(const mt_t *m)
{
    uint32_t n = 0;
    for (size_t i = 0; i < MT_MAX_STREAMS; i++)
        if (m->streams[i].used) n += m->streams[i].refused_writes;
    return n;
}

static uint32_t mt_attempts(const mt_t *m)
{
    uint32_t n = 0;
    for (size_t i = 0; i < MT_MAX_STREAMS; i++)
        if (m->streams[i].used) n += m->streams[i].write_attempts;
    return n;
}

#define SA_GROUPS 6

typedef struct {
    uint32_t services, refused, attempts, worst_refused, worst_run;
    size_t nchunks, nwire, nctrl;
} sa_run_t;

/* Three tracks of SA_GROUPS groups -- one ahead of two equal ones, closed
 * subgroups, subgroups closed later (FIN-only writes), then every
 * subscription done -- served one write at a time. Every service's refused
 * writes are counted, and its longest run of refusals with no write, control
 * write or reset accepted between them. */
static bool sa_scenario(pair_t *p, sa_run_t *r)
{
    memset(r, 0, sizeof(*r));
    track_t hi, mid, lo;
    if (!subscribe(p, "hi", 10, NONE, NONE, &hi) ||
        !subscribe(p, "mid", 20, NONE, NONE, &mid) ||
        !subscribe(p, "lo", 20, NONE, NONE, &lo)) return false;
    p->mt->nctrl = 0;
    moq_subgroup_handle_t open[SA_GROUPS];
    for (uint64_t g = 1; g <= SA_GROUPS; g++) {
        if (!publish_group(p, &hi, 1, g, 0, 128, 3, false, NULL) ||
            !publish_group(p, &mid, 2, g, 0, 128, 2, true, &open[g - 1]) ||
            !publish_group(p, &lo, 3, g, 0, 128, 2, false, NULL)) return false;
    }
    bool closed = false, done = false;
    for (int step = 0; step < 4000; step++) {
        if (step == 6 && !closed) {
            for (size_t g = 0; g < SA_GROUPS; g++)
                if (moq_session_close_subgroup(p->pub, open[g], p->now) != MOQ_OK)
                    return false;
            closed = true;
        }
        if (step == 12 && !done) {
            const track_t *t[3] = { &hi, &mid, &lo };
            for (size_t i = 0; i < 3; i++) {
                moq_done_subscribe_cfg_t dc;
                moq_done_subscribe_cfg_init(&dc);
                dc.status_code = 0x3;
                dc.stream_count = SA_GROUPS;
                if (moq_session_done_subscribe(p->pub, t[i]->sub, &dc,
                                               p->now) != MOQ_OK)
                    return false;
            }
            done = true;
        }
        const uint32_t before = mt_refused(p->mt);
        p->mt->refuse_run = p->mt->worst_refuse_run = 0;
        if (moq_transport_bridge_service(p->pub_br, p->now) != MOQ_OK) return false;
        r->services++;
        const uint32_t refused = mt_refused(p->mt) - before;
        if (refused > r->worst_refused) r->worst_refused = refused;
        if (p->mt->worst_refuse_run > r->worst_run)
            r->worst_run = p->mt->worst_refuse_run;
        if (!mt_serve_one(p->mt) && closed && done &&
            !moq_transport_bridge_has_outbound_pending(p->pub_br) &&
            !moq_session_has_pending_output(p->pub))
            break;
    }
    r->refused = mt_refused(p->mt);
    r->attempts = mt_attempts(p->mt);
    r->nchunks = p->mt->nchunks;
    r->nwire = p->mt->nwire;
    r->nctrl = p->mt->nctrl;
    return closed && done;
}

/* What the endpoint accepted, in acceptance order, and what it sent. */
static bool sa_same_output(const mt_t *a, const mt_t *b)
{
    if (a->nchunks != b->nchunks || a->nwire != b->nwire || a->nctrl != b->nctrl)
        return false;
    for (size_t i = 0; i < a->nchunks; i++) {
        const mt_chunk_t *x = &a->chunks[i], *y = &b->chunks[i];
        if (x->sid != y->sid || x->len != y->len || x->fin != y->fin ||
            x->accepted_seq != y->accepted_seq ||
            memcmp(x->head, y->head, PAYLOAD_LEN) != 0)
            return false;
    }
    for (size_t i = 0; i < a->nwire; i++)
        if (a->wire[i].sid != b->wire[i].sid || a->wire[i].len != b->wire[i].len ||
            a->wire[i].fin != b->wire[i].fin || a->wire[i].reset != b->wire[i].reset ||
            memcmp(a->wire[i].head, b->wire[i].head, PAYLOAD_LEN) != 0 ||
            a->wire_seq[i] != b->wire_seq[i])
            return false;
    for (size_t i = 0; i < a->nctrl; i++)
        if (a->ctrl[i].sid != b->ctrl[i].sid || a->ctrl[i].len != b->ctrl[i].len ||
            a->ctrl[i].fin != b->ctrl[i].fin ||
            a->ctrl[i].accepted_seq != b->ctrl[i].accepted_seq ||
            memcmp(a->ctrl[i].data, b->ctrl[i].data, a->ctrl[i].len) != 0)
            return false;
    return true;
}

/* A spent budget costs one refused write a hand-off, and changes nothing the
 * endpoint accepts or sends: every write the bridge stops offering would
 * have been refused. Without the capability every ready stream is tried.
 * A service repeats its round -- the pending FIFO, which already stops at its
 * first refusal, then a hand-off of queued data -- after a round that moved
 * something; nothing else moves here (no peer input, retirement or
 * deadline). So at most three refusals come in a row: one ending a hand-off
 * that moved something, then the next round's pending head and hand-off. */
static void test_shared_admission_refusals(void)
{
    for (size_t v = 0; v < N_VERSIONS; v++)
        for (size_t c = 0; c < 2; c++) {
            const size_t cap = k_caps[c];
            pair_t plain, shared;
            sa_run_t rp, rs;
            bool ok = pair_init(&plain, k_versions[v], cap, true) &&
                      pair_init(&shared, k_versions[v], cap, true);
            MOQ_TEST_CHECK(ok);
            if (ok) {
                mt_set_shared(shared.mt, true);
                MOQ_TEST_CHECK(sa_scenario(&plain, &rp));
                MOQ_TEST_CHECK(sa_scenario(&shared, &rs));
                MOQ_TEST_CHECK(sa_same_output(plain.mt, shared.mt));
                MOQ_TEST_CHECK_EQ_SIZE(rp.nwire, rs.nwire);
                MOQ_TEST_CHECK(rs.nwire > 0);
                MOQ_TEST_CHECK(rs.worst_run <= 3);
                MOQ_TEST_CHECK(rp.worst_run > 3);
                MOQ_TEST_CHECK(rs.refused < rp.refused);
                MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(shared.pub_br));
                printf("shared admission d%d cap %zu: refused %u -> %u "
                       "(a service at worst %u -> %u, in a row %u -> %u), "
                       "attempts %u -> %u, %zu writes accepted, %zu sent\n",
                       draft_of(k_versions[v]), cap, rp.refused, rs.refused,
                       rp.worst_refused, rs.worst_refused, rp.worst_run,
                       rs.worst_run, rp.attempts, rs.attempts, rs.nchunks,
                       rs.nwire);
            }
            pair_destroy(&plain);
            pair_destroy(&shared);
        }
    MOQ_TEST_PASS("a spent shared budget is refused once a hand-off");
}

/* While the budget is spent, a FIN-only write still goes in the same
 * hand-off: the endpoint may mark it on bytes it holds. */
static void test_shared_admission_fin(void)
{
    for (size_t v = 0; v < N_VERSIONS; v++) {
        pair_t p;
        MOQ_TEST_CHECK(pair_init(&p, k_versions[v], 1, true));
        mt_set_shared(p.mt, true);
        track_t a, b;
        MOQ_TEST_CHECK(subscribe(&p, "a", 10, NONE, NONE, &a));
        MOQ_TEST_CHECK(subscribe(&p, "b", 20, NONE, NONE, &b));
        moq_subgroup_handle_t bg;
        MOQ_TEST_CHECK(publish_group(&p, &b, 2, 1, 0, 128, 1, true, &bg));
        serve_all(&p);
        uint64_t bsid = 0;
        MOQ_TEST_CHECK(stream_of(&p, 2, 1, &bsid));
        mt_stream_t *bst = mt_stream(p.mt, bsid);
        MOQ_TEST_CHECK(bst != NULL && bst->fin_attempts == 0);
        /* A, ahead of B, spends the budget; B has only its FIN left. */
        MOQ_TEST_CHECK(publish_group(&p, &a, 1, 1, 0, 128, 3, true, NULL));
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_close_subgroup(p.pub, bg, p.now),
                              MOQ_OK);
        const uint32_t refused = mt_refused(p.mt);
        const size_t chunks = p.mt->nchunks;
        MOQ_TEST_CHECK_EQ_INT((int)moq_transport_bridge_service(p.pub_br, p.now),
                              MOQ_OK);
        MOQ_TEST_CHECK(mt_refused(p.mt) > refused);       /* spent */
        bool b_fin = false;
        for (size_t i = chunks; i < p.mt->nchunks; i++)
            if (p.mt->chunks[i].sid == bsid && p.mt->chunks[i].fin &&
                p.mt->chunks[i].len == 0)
                b_fin = true;
        MOQ_TEST_CHECK(b_fin);
        if (bst) MOQ_TEST_CHECK_EQ_INT((int)bst->fin_attempts, 1);
        serve_all(&p);
        uint32_t w[16];
        MOQ_TEST_CHECK_EQ_SIZE(wire_objects(&p, w, 16), 4);
        MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(p.pub_br));
        pair_destroy(&p);
    }
    MOQ_TEST_PASS("a FIN-only write goes while the shared budget is spent");
}

/* A reset accepted while the budget is spent may have given capacity back:
 * a stream not yet tried in that hand-off gets its bytes offered. Four
 * tracks of equal priority take turns, B, A, C then D, and any accepted
 * write spends the budget (cap 1). B's subgroup header goes and its object
 * is refused, so A, never opened, keeps its writes; A's subgroup is reset,
 * and the reset waits behind A's subgroup header, its reliable prefix. Next
 * hand-off: A's header goes, C is refused, and A's reset, now due, is
 * accepted and gives the budget back; D, not yet tried, goes. */
static bool sa_reset_row(moq_version_t ver, bool shared, bool *d_went)
{
    pair_t p;
    bool ok = pair_init_retained(&p, ver, 1, true, 0, 1024, 0, 0, true);
    MOQ_TEST_CHECK(ok);
    if (!ok) { pair_destroy(&p); return false; }
    mt_set_shared(p.mt, shared);
    track_t a, b, c, d;
    ok = subscribe(&p, "b", 20, NONE, NONE, &b) &&
         subscribe(&p, "a", 20, NONE, NONE, &a) &&
         subscribe(&p, "c", 20, NONE, NONE, &c) &&
         subscribe(&p, "d", 20, NONE, NONE, &d);
    MOQ_TEST_CHECK(ok);
    moq_subgroup_handle_t ag;
    ok = ok && publish_group(&p, &b, 2, 1, 0, 128, 1, true, NULL) &&
         publish_group(&p, &a, 1, 1, 0, 128, 1, true, &ag);
    MOQ_TEST_CHECK(ok);
    if (ok) {
        MOQ_TEST_CHECK_EQ_INT((int)moq_transport_bridge_service(p.pub_br, p.now),
                              MOQ_OK);
        ok = moq_session_reset_subgroup(p.pub, ag, 0x7, p.now) == MOQ_OK &&
             publish_group(&p, &c, 3, 1, 0, 128, 1, true, NULL) &&
             publish_group(&p, &d, 4, 1, 0, 128, 1, true, NULL);
        MOQ_TEST_CHECK(ok);
    }
    size_t mark = 0, end = 0;
    if (ok) {
        while (mt_serve_one(p.mt)) {}
        mark = p.mt->nchunks;
        MOQ_TEST_CHECK_EQ_INT((int)moq_transport_bridge_service(p.pub_br, p.now),
                              MOQ_OK);
        end = p.mt->nchunks;
        uint64_t asid = 0;
        for (size_t i = mark; i < end && !asid; i++) asid = p.mt->chunks[i].sid;
        const mt_stream_t *ast = asid ? mt_stream(p.mt, asid) : NULL;
        MOQ_TEST_CHECK(ast != NULL && ast->reset);   /* A: its prefix, then its reset */
        serve_all(&p);
        uint64_t dsid = 0;
        MOQ_TEST_CHECK(stream_of(&p, 4, 1, &dsid));
        *d_went = false;
        for (size_t i = mark; i < end; i++)
            if (p.mt->chunks[i].sid == dsid) *d_went = true;
        uint32_t w[16];
        MOQ_TEST_CHECK_EQ_SIZE(wire_objects(&p, w, 16), 3);   /* B, C and D */
        MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(p.pub_br));
    }
    pair_destroy(&p);
    return ok;
}

static void test_shared_admission_reset(void)
{
    for (size_t v = 0; v < N_VERSIONS; v++) {
        bool plain_d = true, shared_d = false;
        MOQ_TEST_CHECK(sa_reset_row(k_versions[v], false, &plain_d));
        MOQ_TEST_CHECK(sa_reset_row(k_versions[v], true, &shared_d));
        /* Without the capability D was tried, and refused, before A's reset
         * gave the budget back; with it D waits for that reset and goes. */
        MOQ_TEST_CHECK(!plain_d);
        MOQ_TEST_CHECK(shared_d);
        printf("shared admission reset d%d: D in the same hand-off %d -> %d\n",
               draft_of(k_versions[v]), (int)plain_d, (int)shared_d);
    }
    MOQ_TEST_PASS("a reset accepted while the shared budget is spent reopens it");
}

#define SA_REQUIRE(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
        return false; \
    } \
} while (0)

/* A deadline passes while the shared budget is spent. A (the most urgent)
 * spends the budget, and C's object and B's two wait; B's carry a 1 ms
 * delivery timeout. Only the budget holds data back -- the model refuses
 * data at its one cap -- so the row runs with the capability and without.
 * Until the deadline the held objects keep it as the session's next one. At
 * the deadline the bridge's expiry drops B's objects and resets B, keeping
 * its subgroup header, though the budget is still spent; that service's
 * hand-off still offers C, which is refused. B's PUBLISH_DONE, a control
 * write, then goes while data is still refused. Draft-18 and later, as the
 * retained expiry rows. */
static bool shared_expiry_row(pair_t *p, moq_rcbuf_t *payload[4],
                              uint32_t *refused)
{
    regression_owner_t a, c, b;
    if (!regression_owner(p, false, "fills", 0, 0, &a) ||
        !regression_owner(p, false, "ahead", 64, 0, &c) ||
        !regression_owner(p, false, "expires", 128, 1, &b)) return false;
    moq_subgroup_handle_t asg, csg, bsg;
    uint64_t asid, csid, bsid;
    size_t a_header, c_header, b_header;
    if (!regression_open_header(p, &a, 1, &asg, &asid, &a_header) ||
        !regression_open_header(p, &c, 1, &csg, &csid, &c_header) ||
        !regression_open_header(p, &b, 1, &bsg, &bsid, &b_header)) return false;
    serve_all(p);
    mt_stream_t *bst = mt_stream(p->mt, bsid), *cst = mt_stream(p->mt, csid);
    SA_REQUIRE(bst && cst && bst->data && cst->data);
    const size_t chunks_before = p->mt->nchunks;
    const uint64_t t0 = p->now, deadline = t0 + 1000;
    p->mt->cap = 1; /* one accepted data write spends the budget */
    static const struct { uint8_t track; uint8_t object; } objs[4] = {
        { 1, 0 }, { 3, 0 }, { 2, 0 }, { 2, 1 },
    };
    for (unsigned i = 0; i < 4; ++i) {
        uint8_t bytes[PAYLOAD_LEN] = { PAYLOAD_TAG, objs[i].track, 1, 0,
                                       objs[i].object };
        SA_REQUIRE(moq_rcbuf_create(moq_alloc_default(), bytes, sizeof(bytes),
                                     &payload[i]) == MOQ_OK);
        const moq_subgroup_handle_t sg = objs[i].track == 1 ? asg
                                       : objs[i].track == 3 ? csg : bsg;
        SA_REQUIRE(moq_session_write_object(p->pub, sg, objs[i].object,
                                             payload[i], p->now) == MOQ_OK);
    }
    SA_REQUIRE(moq_session_close_subgroup(p->pub, bsg, p->now) == MOQ_OK);
    SA_REQUIRE(moq_session_next_deadline_us(p->pub) == deadline);
    SA_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
    /* Only A's writes went; the budget is spent. */
    SA_REQUIRE(p->mt->nchunks > chunks_before && p->mt->queued >= p->mt->cap);
    for (size_t i = chunks_before; i < p->mt->nchunks; ++i)
        SA_REQUIRE(p->mt->chunks[i].sid == asid);
    /* The held objects' deadline still wakes the adapter, exactly. */
    MOQ_TEST_CHECK_EQ_U64(moq_session_next_deadline_us(p->pub), deadline);

    p->now = deadline - 1;
    SA_REQUIRE(moq_session_tick(p->pub, p->now) == MOQ_OK);
    SA_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)bst->reset_attempts, 0);

    p->now = deadline;
    SA_REQUIRE(moq_session_tick(p->pub, p->now) == MOQ_OK);
    const uint32_t c_refused = cst->refused_writes;
    SA_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
    MOQ_TEST_CHECK(cst->refused_writes > c_refused);
    MOQ_TEST_CHECK_EQ_INT((int)bst->reset_attempts, 1);
    MOQ_TEST_CHECK(bst->reset);
    MOQ_TEST_CHECK_EQ_U64(bst->reset_code, MOQ_RESET_DELIVERY_TIMEOUT);
    MOQ_TEST_CHECK_EQ_U64(bst->reset_reliable, b_header);
    for (size_t i = chunks_before; i < p->mt->nchunks; ++i)
        MOQ_TEST_CHECK(p->mt->chunks[i].sid != bsid);
    SA_REQUIRE(p->mt->queued >= p->mt->cap);

    const size_t ctrl_before = p->mt->nctrl;
    SA_REQUIRE(regression_finish(p, &b, 1, "expired") == MOQ_OK);
    if (!regression_service(p)) return false;
    size_t count;
    uint64_t seq;
    if (!regression_done(p, "expired", 1, &count, &seq)) return false;
    MOQ_TEST_CHECK_EQ_SIZE(count, 1);
    MOQ_TEST_CHECK(p->mt->nctrl > ctrl_before);
    SA_REQUIRE(p->mt->queued >= p->mt->cap);

    *refused = mt_refused(p->mt);
    p->mt->cap = 1u << 20;
    serve_all(p);
    const uint32_t want[] = { OBJ(1, 1, 0, 0), OBJ(3, 1, 0, 0) };
    expect_wire(p, want, 2, "shared expiry", 1u << 20);
    MOQ_TEST_CHECK(!moq_transport_bridge_has_outbound_pending(p->pub_br));
    return true;
}

#undef SA_REQUIRE

static void test_shared_admission_expiry(void)
{
    for (size_t v = 1; v < N_VERSIONS; ++v) {
        pair_t plain, shared;
        moq_rcbuf_t *pp[4] = { NULL, NULL, NULL, NULL };
        moq_rcbuf_t *ps[4] = { NULL, NULL, NULL, NULL };
        uint32_t rp = 0, rs = 0;
        /* The bridge's own pending limit: a full pending FIFO would hold the
         * hand-off and the control write back whatever the budget. */
        const bool ok =
            pair_init_retained(&plain, k_versions[v], 1u << 20, true, 0, 1024,
                               1u << 20, 0, true) &&
            pair_init_retained(&shared, k_versions[v], 1u << 20, true, 0, 1024,
                               1u << 20, 0, true);
        MOQ_TEST_CHECK(ok);
        if (ok) {
            mt_set_shared(shared.mt, true);
            MOQ_TEST_CHECK(shared_expiry_row(&plain, pp, &rp));
            MOQ_TEST_CHECK(shared_expiry_row(&shared, ps, &rs));
            MOQ_TEST_CHECK(sa_same_output(plain.mt, shared.mt));
            MOQ_TEST_CHECK(rs < rp);
            printf("shared admission expiry d%d: refused %u -> %u, %zu writes "
                   "accepted, %zu control writes\n", draft_of(k_versions[v]),
                   rp, rs, shared.mt->nchunks, shared.mt->nctrl);
        }
        pair_destroy(&plain);
        pair_destroy(&shared);
        for (unsigned i = 0; i < 4; ++i) {
            if (pp[i]) {
                MOQ_TEST_CHECK_EQ_INT((int)moq_rcbuf_refcount(pp[i]), 1);
                moq_rcbuf_decref(pp[i]);
            }
            if (ps[i]) {
                MOQ_TEST_CHECK_EQ_INT((int)moq_rcbuf_refcount(ps[i]), 1);
                moq_rcbuf_decref(ps[i]);
            }
        }
    }
    MOQ_TEST_PASS("an expiry reset and a control write go while the shared budget is spent");
}

/* -- Per-pass bookkeeping ----------------------------------------------------
 *
 * A service pass costs what its work costs, not what the bridge's tables can
 * hold: the deadline walk reads the stream table only while a queued item has
 * a delivery deadline, and the per-pass reset attempt needs no sweep of it.
 * Neither may cost a wake-up or an attempt: the published deadline stays the
 * exact earliest through refusal, expiry, cancellation and the pending FIFO,
 * and a refused reset is tried once in every pass, in a reused slot too. */

#define PC_REQUIRE(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
        return false; \
    } \
} while (0)

#define PC_STREAMS 512u   /* the stream table, far above the streams in use */

/* The per-pass table walks' exact work: in a build with the bridge's probe
 * counters (transport_bridge_internal.h) the rows also assert what each
 * walk read, as deltas around the calls; elsewhere they assert outcomes
 * and table counts only. */
#ifdef MOQ_SESSION_SWEEP_TESTING
typedef struct {
    uint64_t expiry_slots, expiry_items, reset_slots, query_slots;
    /* The session's (session_internal.h): the next-deadline query's pool
     * walks and its queued-deadline scan, expiry's search and compaction,
     * the reset offer's queue scan and the stream-to-subgroup lookup. */
    uint64_t dl_sg, dl_sub, dl_pub, queued_dl, expiry_search,
             expiry_compact, reset_offer, sg_find;
} walk_t;

static walk_t walk_now(void)
{
    walk_t w = {
        .expiry_slots = bridge_work_expiry_slots,
        .expiry_items = bridge_work_expiry_items,
        .reset_slots = bridge_work_reset_slots,
        .query_slots = bridge_work_query_slots,
        .dl_sg = session_work_deadline_sg_probes,
        .dl_sub = session_work_deadline_sub_probes,
        .dl_pub = session_work_deadline_pub_probes,
        .queued_dl = session_work_queued_deadline,
        .expiry_search = session_work_expiry_search,
        .expiry_compact = session_work_expiry_compact,
        .reset_offer = session_work_reset_offer,
        .sg_find = session_work_sg_stream_find,
    };
    return w;
}

static walk_t walk_since(walk_t w0)
{
    walk_t w = walk_now();
    w.expiry_slots -= w0.expiry_slots;
    w.expiry_items -= w0.expiry_items;
    w.reset_slots -= w0.reset_slots;
    w.query_slots -= w0.query_slots;
    w.dl_sg -= w0.dl_sg;
    w.dl_sub -= w0.dl_sub;
    w.dl_pub -= w0.dl_pub;
    w.queued_dl -= w0.queued_dl;
    w.expiry_search -= w0.expiry_search;
    w.expiry_compact -= w0.expiry_compact;
    w.reset_offer -= w0.reset_offer;
    w.sg_find -= w0.sg_find;
    return w;
}

/* `slots` is a whole number of complete walks over a `cap`-slot table, at
 * least one. */
static bool whole_walks(uint64_t slots, uint32_t cap)
{
    return cap > 0 && slots >= cap && slots % cap == 0;
}

#define WALK_MARK(w) walk_t w = walk_now()
#define WALK_CHECK(expr) MOQ_TEST_CHECK(expr)
/* A walk the row cannot go on without: it returns at the first miss. */
#define WALK_REQUIRE(expr) PC_REQUIRE(expr)
#else
#define WALK_MARK(w) do { } while (0)
#define WALK_CHECK(expr) do { } while (0)
#define WALK_REQUIRE(expr) do { } while (0)
#endif

/* Untimed objects held at the budget publish no deadline and read no table
 * slot; one held object with a 1 ms timeout makes the next deadline exactly
 * its own, read through the table; once it expires, nothing is read again. */
static bool pass_cost_deadline_row(pair_t *p, moq_rcbuf_t *payload[3])
{
    regression_owner_t u, t;
    if (!regression_owner(p, false, "untimed", 0, 0, &u) ||
        !regression_owner(p, false, "timed", 128, 1, &t)) return false;
    moq_subgroup_handle_t usg, tsg;
    uint64_t usid, tsid;
    size_t u_header, t_header;
    if (!regression_open_header(p, &u, 1, &usg, &usid, &u_header) ||
        !regression_open_header(p, &t, 1, &tsg, &tsid, &t_header)) return false;
    serve_all(p);
    PC_REQUIRE(p->pub_br->dstream_cap >= PC_STREAMS);
    p->mt->cap = 1;
    for (unsigned i = 0; i < 2; ++i) {
        uint8_t bytes[PAYLOAD_LEN] = { PAYLOAD_TAG, 1, 1, 0, (uint8_t)i };
        PC_REQUIRE(moq_rcbuf_create(moq_alloc_default(), bytes, sizeof(bytes),
                                    &payload[i]) == MOQ_OK);
        PC_REQUIRE(moq_session_write_object(p->pub, usg, i, payload[i],
                                            p->now) == MOQ_OK);
    }
    uint64_t slots = p->pub_br->deadline_slots;
    WALK_MARK(w0);
    for (unsigned i = 0; i < 3; ++i)
        PC_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
    MOQ_TEST_CHECK(p->pub_br->dq_items > 0);
    MOQ_TEST_CHECK_EQ_INT((int)p->pub_br->dq_timed, 0);
    MOQ_TEST_CHECK_EQ_U64(p->pub_br->deadline_slots, slots);
    MOQ_TEST_CHECK_EQ_U64(moq_session_next_deadline_us(p->pub), UINT64_MAX);
    WALK_CHECK(walk_since(w0).expiry_slots == 0 &&
               walk_since(w0).expiry_items == 0 &&
               walk_since(w0).reset_slots == 0);

    const uint64_t deadline = p->now + 1000;
    uint8_t bytes[PAYLOAD_LEN] = { PAYLOAD_TAG, 2, 1, 0, 0 };
    PC_REQUIRE(moq_rcbuf_create(moq_alloc_default(), bytes, sizeof(bytes),
                                &payload[2]) == MOQ_OK);
    PC_REQUIRE(moq_session_write_object(p->pub, tsg, 0, payload[2],
                                        p->now) == MOQ_OK);
    slots = p->pub_br->deadline_slots;
    PC_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)p->pub_br->dq_timed, 1);
    MOQ_TEST_CHECK_EQ_INT((int)dq_timed_recount(p->pub_br), 1);
    MOQ_TEST_CHECK(p->pub_br->deadline_slots - slots >= p->pub_br->dstream_cap);
    MOQ_TEST_CHECK_EQ_U64((p->pub_br->deadline_slots - slots) %
                          p->pub_br->dstream_cap, 0);
    MOQ_TEST_CHECK_EQ_U64(moq_session_next_deadline_us(p->pub), deadline);
    {
        /* The timed object is queued now (a pass expires before it drains
         * new actions): each later walk reads the whole table and every
         * queued item once, and expires none of them before the deadline. */
        WALK_MARK(w1);
        PC_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)p->pub_br->dq_timed, 1);
        WALK_CHECK(whole_walks(walk_since(w1).expiry_slots,
                               p->pub_br->dstream_cap) &&
                   walk_since(w1).expiry_items ==
                       walk_since(w1).expiry_slots / p->pub_br->dstream_cap *
                           p->pub_br->dq_items);
    }

    p->now = deadline;
    PC_REQUIRE(moq_session_tick(p->pub, p->now) == MOQ_OK);
    if (!regression_service(p)) return false;
    mt_stream_t *tst = mt_stream(p->mt, tsid);
    PC_REQUIRE(tst != NULL);
    MOQ_TEST_CHECK(tst->reset);
    MOQ_TEST_CHECK_EQ_U64(tst->reset_code, MOQ_RESET_DELIVERY_TIMEOUT);
    MOQ_TEST_CHECK_EQ_INT((int)p->pub_br->dq_timed, 0);
    slots = p->pub_br->deadline_slots;
    WALK_MARK(w2);
    PC_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(p->pub_br->deadline_slots, slots);
    MOQ_TEST_CHECK_EQ_U64(moq_session_next_deadline_us(p->pub), UINT64_MAX);
    WALK_CHECK(walk_since(w2).expiry_slots == 0 &&
               walk_since(w2).reset_slots == 0);
    p->mt->cap = 1u << 20;
    serve_all(p);
    return true;
}

/* A refused deferred reset is tried once in each service pass, and in every
 * one: no attempt from an earlier pass blocks it, and no second attempt runs
 * within one. The reset is deferred behind its reliable prefix, the subgroup
 * header, which the budget holds back when the reset arrives; once the header
 * goes the reset is due, and the transport refuses it until released. A pass
 * that hands the header over repeats its round, so a missing per-pass guard
 * would try the reset twice there. */
static bool pass_cost_reset_row(pair_t *p)
{
    regression_owner_t owner;
    if (!regression_owner(p, false, "reset", 128, 0, &owner)) return false;
    p->mt->cap = 0; /* control stays writable; no data write fits */
    moq_subgroup_cfg_t sc;
    moq_subgroup_cfg_init(&sc);
    sc.group_id = 1;
    sc.publisher_priority = 128;
    moq_subgroup_handle_t sg;
    PC_REQUIRE(moq_session_open_subgroup(p->pub, owner.track.sub, &sc, p->now,
                                         &sg) == MOQ_OK);
    PC_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
    mt_stream_t *st = NULL;
    for (size_t i = 0; i < MT_MAX_STREAMS; ++i)
        if (p->mt->streams[i].used && p->mt->streams[i].write_attempts > 0 &&
            p->mt->streams[i].refused_writes > 0)
            st = &p->mt->streams[i];
    PC_REQUIRE(st != NULL && !st->reset);
    PC_REQUIRE(moq_session_reset_subgroup(p->pub, sg, 0x1 /* CANCELLED */,
                                          p->now) == MOQ_OK);
    st->refuse_reset = true;
    p->mt->cap = 1u << 20;
    const uint32_t before = st->reset_attempts;
    for (unsigned i = 1; i <= 5; ++i) {
        WALK_MARK(w);
        PC_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)(st->reset_attempts - before), (int)i);
        /* The cancellation's debt is the one counted. The first pass
         * drains the cancellation after its walk and tries the reset in
         * the stream's turn; every later pass's walk reads the table for
         * it. */
        MOQ_TEST_CHECK_EQ_INT((int)p->pub_br->dreset_count, 1);
        CHECK_TABLE_COUNTS(p->pub_br);
        WALK_CHECK(i == 1 ||
                   whole_walks(walk_since(w).reset_slots,
                               p->pub_br->dstream_cap));
    }
    MOQ_TEST_CHECK(!st->reset);
    /* The refused reset is the only work left, and it keeps the bridge
     * pending: no data is queued or pending, no control or close is owed. */
    MOQ_TEST_CHECK_EQ_INT((int)p->pub_br->dq_items, 0);
    MOQ_TEST_CHECK_EQ_INT((int)p->pub_br->pending_count, 0);
    MOQ_TEST_CHECK(!p->pub_br->pending_control && !p->pub_br->needs_close);
    {
        WALK_MARK(q);
        MOQ_TEST_CHECK(moq_transport_bridge_has_pending(p->pub_br));
        WALK_CHECK(walk_since(q).query_slots > 0);
    }
    st->refuse_reset = false;
    PC_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
    MOQ_TEST_CHECK(st->reset);
    MOQ_TEST_CHECK_EQ_INT((int)(st->reset_attempts - before), 6);
    MOQ_TEST_CHECK_EQ_U64(st->reset_code, 0x1);
    MOQ_TEST_CHECK_EQ_U64(st->reset_reliable, st->first_write_len);
    /* Accepted: the release took the debt away, and nothing reads the
     * table for one again. */
    MOQ_TEST_CHECK_EQ_INT((int)p->pub_br->dreset_count, 0);
    CHECK_TABLE_COUNTS(p->pub_br);
    {
        WALK_MARK(w);
        PC_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
        (void)moq_transport_bridge_has_pending(p->pub_br);
        WALK_CHECK(walk_since(w).reset_slots == 0 &&
                   walk_since(w).query_slots == 0);
    }
    return true;
}

/* Two timed streams held at the budget, the later deadline's opened first so
 * that it is the first the table walk meets: the next deadline is exactly
 * the earlier one; once that stream expires, exactly the later one, with the
 * count down to the survivor's; once that expires too, none, and the table
 * is no longer read. */
static bool pass_cost_earliest_row(pair_t *p)
{
    regression_owner_t late, soon;
    if (!regression_owner(p, false, "late", 128, 3, &late) ||
        !regression_owner(p, false, "soon", 128, 1, &soon)) return false;
    moq_subgroup_handle_t lsg, ssg;
    uint64_t lsid, ssid;
    size_t l_header, s_header;
    if (!regression_open_header(p, &late, 1, &lsg, &lsid, &l_header) ||
        !regression_open_header(p, &soon, 1, &ssg, &ssid, &s_header))
        return false;
    serve_all(p);
    p->mt->cap = 0; /* no data write fits */
    const uint64_t t0 = p->now;
    PC_REQUIRE(write_object(p, lsg, 1, 1, 0, 0) == MOQ_OK);
    PC_REQUIRE(write_object(p, ssg, 2, 1, 0, 0) == MOQ_OK);
    PC_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)p->pub_br->dq_timed, 2);
    MOQ_TEST_CHECK_EQ_INT((int)dq_timed_recount(p->pub_br), 2);
    MOQ_TEST_CHECK_EQ_U64(moq_session_next_deadline_us(p->pub), t0 + 1000);
    {
        /* Before either deadline: the walk reads both items and leaves
         * them, in every pass. */
        WALK_MARK(w);
        PC_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
        const mt_stream_t *l0 = mt_stream(p->mt, lsid);
        const mt_stream_t *s0 = mt_stream(p->mt, ssid);
        PC_REQUIRE(l0 != NULL && s0 != NULL);
        MOQ_TEST_CHECK(!l0->reset && !s0->reset);
        WALK_CHECK(whole_walks(walk_since(w).expiry_slots,
                               p->pub_br->dstream_cap) &&
                   walk_since(w).expiry_items ==
                       walk_since(w).expiry_slots / p->pub_br->dstream_cap * 2u);
    }

    p->now = t0 + 1000;
    PC_REQUIRE(moq_session_tick(p->pub, p->now) == MOQ_OK);
    WALK_MARK(w1);
    if (!regression_service(p)) return false;
    mt_stream_t *lst = mt_stream(p->mt, lsid), *sst = mt_stream(p->mt, ssid);
    PC_REQUIRE(lst != NULL && sst != NULL);
    MOQ_TEST_CHECK(sst->reset && !lst->reset);
    MOQ_TEST_CHECK_EQ_U64(sst->reset_code, MOQ_RESET_DELIVERY_TIMEOUT);
    MOQ_TEST_CHECK_EQ_INT((int)p->pub_br->dq_timed, 1);
    MOQ_TEST_CHECK_EQ_U64(moq_session_next_deadline_us(p->pub), t0 + 3000);
    /* The expiry was the walk's own work; the survivor keeps it walking. */
    WALK_CHECK(walk_since(w1).expiry_slots > 0 &&
               walk_since(w1).expiry_items > 0);

    p->now = t0 + 3000;
    PC_REQUIRE(moq_session_tick(p->pub, p->now) == MOQ_OK);
    if (!regression_service(p)) return false;
    MOQ_TEST_CHECK(lst->reset);
    MOQ_TEST_CHECK_EQ_U64(lst->reset_code, MOQ_RESET_DELIVERY_TIMEOUT);
    MOQ_TEST_CHECK_EQ_INT((int)p->pub_br->dq_timed, 0);
    MOQ_TEST_CHECK_EQ_U64(moq_session_next_deadline_us(p->pub), UINT64_MAX);
    const uint64_t slots = p->pub_br->deadline_slots;
    WALK_MARK(w2);
    PC_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(p->pub_br->deadline_slots, slots);
    WALK_CHECK(walk_since(w2).expiry_slots == 0 &&
               walk_since(w2).expiry_items == 0);
    p->mt->cap = 1u << 20;
    serve_all(p);
    return true;
}

/* A refused timed write goes back to its queue still counted: each pass pops
 * it, the transport refuses it, it is put back, and the count, the table read
 * and the exact deadline survive every round. Once the write is taken,
 * nothing is counted or read. */
static bool pass_cost_unpop_row(pair_t *p)
{
    regression_owner_t o;
    if (!regression_owner(p, false, "unpop", 128, 2, &o)) return false;
    moq_subgroup_handle_t sg;
    uint64_t sid;
    size_t header;
    if (!regression_open_header(p, &o, 1, &sg, &sid, &header)) return false;
    serve_all(p);
    mt_stream_t *st = mt_stream(p->mt, sid);
    PC_REQUIRE(st != NULL);
    p->mt->cap = 0;
    const uint64_t deadline = p->now + 2000;
    PC_REQUIRE(write_object(p, sg, 1, 1, 0, 0) == MOQ_OK);
    for (unsigned i = 0; i < 4; ++i) {
        const uint32_t refused = st->refused_writes;
        const uint64_t slots = p->pub_br->deadline_slots;
        WALK_MARK(w);
        PC_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
        MOQ_TEST_CHECK(st->refused_writes > refused);
        MOQ_TEST_CHECK_EQ_INT((int)p->pub_br->pending_count, 0);
        MOQ_TEST_CHECK_EQ_INT((int)p->pub_br->dq_items, 1);
        MOQ_TEST_CHECK_EQ_INT((int)p->pub_br->dq_timed, 1);
        MOQ_TEST_CHECK_EQ_INT((int)dq_timed_recount(p->pub_br), 1);
        MOQ_TEST_CHECK(p->pub_br->deadline_slots - slots >=
                       p->pub_br->dstream_cap);
        MOQ_TEST_CHECK_EQ_U64(moq_session_next_deadline_us(p->pub), deadline);
        /* The refused write is back in its queue, still counted: once it
         * is queued (after the first pass's walk), the expiry walk keeps
         * reading it in every pass. */
        WALK_CHECK(i == 0 ||
                   (whole_walks(walk_since(w).expiry_slots,
                                p->pub_br->dstream_cap) &&
                    walk_since(w).expiry_items ==
                        walk_since(w).expiry_slots / p->pub_br->dstream_cap));
    }
    p->mt->cap = 1u << 20;
    const size_t chunks = p->mt->nchunks;
    PC_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
    MOQ_TEST_CHECK(p->mt->nchunks > chunks);
    MOQ_TEST_CHECK_EQ_INT((int)p->pub_br->dq_timed, 0);
    MOQ_TEST_CHECK_EQ_U64(moq_session_next_deadline_us(p->pub), UINT64_MAX);
    const uint64_t slots = p->pub_br->deadline_slots;
    WALK_MARK(w);
    PC_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(p->pub_br->deadline_slots, slots);
    WALK_CHECK(walk_since(w).expiry_slots == 0);
    serve_all(p);
    return true;
}

/* Cancelling a subgroup takes its queued timed objects out of the count at
 * once, both ways a queued stream can go: freed outright when its header has
 * already been sent, or cut back to the reliable prefix it still owes when
 * its header is held back too. The next deadline falls back to the surviving
 * stream's, the cancelled objects are released, and the owed reset goes out
 * once its prefix does. */
static bool pass_cost_cancel_row(pair_t *p, moq_rcbuf_t *payload[5])
{
    regression_owner_t sent, held, kept;
    if (!regression_owner(p, false, "sent", 128, 1, &sent) ||
        !regression_owner(p, false, "held", 128, 1, &held) ||
        !regression_owner(p, false, "kept", 128, 4, &kept)) return false;
    moq_subgroup_handle_t ssg, hsg, ksg;
    uint64_t ssid, ksid;
    size_t s_header, k_header;
    if (!regression_open_header(p, &sent, 1, &ssg, &ssid, &s_header) ||
        !regression_open_header(p, &kept, 1, &ksg, &ksid, &k_header))
        return false;
    serve_all(p);
    p->mt->cap = 0; /* no data write fits, held's header included */
    moq_subgroup_cfg_t sc;
    moq_subgroup_cfg_init(&sc);
    sc.group_id = 1;
    sc.publisher_priority = 128;
    PC_REQUIRE(moq_session_open_subgroup(p->pub, held.track.sub, &sc, p->now,
                                         &hsg) == MOQ_OK);
    const uint64_t t0 = p->now;
    const moq_subgroup_handle_t to[5] = { ssg, ssg, hsg, hsg, ksg };
    for (unsigned i = 0; i < 5; ++i) {
        uint8_t bytes[PAYLOAD_LEN] = { PAYLOAD_TAG, 3, 1, 0, (uint8_t)i };
        PC_REQUIRE(moq_rcbuf_create(moq_alloc_default(), bytes, sizeof(bytes),
                                    &payload[i]) == MOQ_OK);
        PC_REQUIRE(moq_session_write_object(p->pub, to[i], i == 4 ? 0 : i % 2,
                                            payload[i], p->now) == MOQ_OK);
    }
    PC_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)p->pub_br->dq_timed, 5);
    MOQ_TEST_CHECK_EQ_INT((int)dq_timed_recount(p->pub_br), 5);
    MOQ_TEST_CHECK_EQ_U64(moq_session_next_deadline_us(p->pub), t0 + 1000);
    mt_stream_t *hst = NULL;
    for (size_t i = 0; i < MT_MAX_STREAMS; ++i)
        if (p->mt->streams[i].used && p->mt->streams[i].sid != ssid &&
            p->mt->streams[i].sid != ksid && p->mt->streams[i].data &&
            p->mt->streams[i].refused_writes > 0 &&
            p->mt->streams[i].first_write_len > 0)
            hst = &p->mt->streams[i];
    PC_REQUIRE(hst != NULL);
    const size_t h_header = hst->first_write_len;

    PC_REQUIRE(moq_session_reset_subgroup(p->pub, ssg, 0x1 /* CANCELLED */,
                                          p->now) == MOQ_OK);
    PC_REQUIRE(moq_session_reset_subgroup(p->pub, hsg, 0x1, p->now) == MOQ_OK);
    PC_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)p->pub_br->dq_timed, 1);
    MOQ_TEST_CHECK_EQ_INT((int)dq_timed_recount(p->pub_br), 1);
    MOQ_TEST_CHECK_EQ_U64(moq_session_next_deadline_us(p->pub), t0 + 4000);
    /* Freed outright, the sent stream owes nothing; the held one owes its
     * reset behind the prefix it kept. */
    MOQ_TEST_CHECK_EQ_INT((int)p->pub_br->dreset_count, 1);
    CHECK_TABLE_COUNTS(p->pub_br);
    for (unsigned i = 0; i < 4; ++i)
        MOQ_TEST_CHECK_EQ_INT((int)moq_rcbuf_refcount(payload[i]), 1);
    MOQ_TEST_CHECK_EQ_INT((int)moq_rcbuf_refcount(payload[4]), 2);
    mt_stream_t *sst = mt_stream(p->mt, ssid);
    PC_REQUIRE(sst != NULL);
    MOQ_TEST_CHECK(sst->reset);
    MOQ_TEST_CHECK_EQ_U64(sst->reset_code, 0x1);
    MOQ_TEST_CHECK(!hst->reset);

    p->mt->cap = 1u << 20;
    if (!regression_service(p)) return false;
    MOQ_TEST_CHECK(hst->reset);
    MOQ_TEST_CHECK_EQ_U64(hst->reset_code, 0x1);
    MOQ_TEST_CHECK_EQ_U64(hst->reset_reliable, h_header);
    MOQ_TEST_CHECK_EQ_INT((int)p->pub_br->dq_timed, 0);
    MOQ_TEST_CHECK_EQ_INT((int)p->pub_br->dreset_count, 0);
    MOQ_TEST_CHECK_EQ_U64(moq_session_next_deadline_us(p->pub), UINT64_MAX);
    serve_all(p);
    return true;
}

/* A timed object whose header went out and whose payload the transport
 * refused waits in the pending FIFO, not in a stream queue: the count stays
 * 0 and no table slot is read, yet the next deadline is exactly the
 * object's, and at it the stream is reset for the delivery timeout. */
static bool pass_cost_pending_row(pair_t *p)
{
    regression_owner_t o;
    if (!regression_owner(p, false, "pending", 128, 1, &o)) return false;
    moq_subgroup_handle_t sg;
    uint64_t sid;
    size_t header;
    if (!regression_open_header(p, &o, 1, &sg, &sid, &header)) return false;
    serve_all(p);
    mt_stream_t *st = mt_stream(p->mt, sid);
    PC_REQUIRE(st != NULL);
    st->refuse_payload = true;
    const uint64_t deadline = p->now + 1000;
    PC_REQUIRE(write_object(p, sg, 1, 1, 0, 0) == MOQ_OK);
    const uint64_t slots = p->pub_br->deadline_slots;
    PC_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
    MOQ_TEST_CHECK(st->refused_writes > 0);
    MOQ_TEST_CHECK(p->pub_br->pending_count > 0);
    MOQ_TEST_CHECK_EQ_INT((int)p->pub_br->dq_items, 0);
    MOQ_TEST_CHECK_EQ_INT((int)p->pub_br->dq_timed, 0);
    MOQ_TEST_CHECK_EQ_U64(p->pub_br->deadline_slots, slots);
    MOQ_TEST_CHECK_EQ_U64(moq_session_next_deadline_us(p->pub), deadline);

    /* The pending FIFO expires on its own: with no queued timed item, the
     * queued-expiry walk reads nothing, yet the expiry still happens, and
     * the reset it leaves the stream is settled in the same service. */
    p->now = deadline;
    PC_REQUIRE(moq_session_tick(p->pub, p->now) == MOQ_OK);
    WALK_MARK(w);
    if (!regression_service(p)) return false;
    MOQ_TEST_CHECK(st->reset);
    MOQ_TEST_CHECK_EQ_U64(st->reset_code, MOQ_RESET_DELIVERY_TIMEOUT);
    MOQ_TEST_CHECK_EQ_INT((int)p->pub_br->pending_count, 0);
    MOQ_TEST_CHECK_EQ_U64(moq_session_next_deadline_us(p->pub), UINT64_MAX);
    MOQ_TEST_CHECK_EQ_INT((int)p->pub_br->dq_timed, 0);
    MOQ_TEST_CHECK_EQ_INT((int)p->pub_br->dreset_count, 0);
    WALK_CHECK(walk_since(w).expiry_slots == 0 &&
               walk_since(w).expiry_items == 0);
    st->refuse_payload = false;
    serve_all(p);
    return true;
}

/* A stream slot freed in a pass and taken again in that same pass starts
 * with its reset untried. Stream A's deferred reset is tried and taken in the
 * pass, which frees its slot; B, held back until then by the retained budget
 * that another stream's objects fill, takes the slot, and B's own deferred
 * reset is still tried in that pass. */
static bool pass_cost_reuse_row(pair_t *p)
{
    regression_owner_t o, fill;
    if (!regression_owner(p, false, "reuse", 128, 0, &o) ||
        !regression_owner(p, false, "fill", 128, 0, &fill)) return false;
    p->mt->cap = 0; /* no data write fits */
    moq_subgroup_cfg_t sc;
    moq_subgroup_cfg_init(&sc);
    sc.group_id = 1;
    sc.publisher_priority = 128;
    moq_subgroup_handle_t asg, fsg, bsg;
    PC_REQUIRE(moq_session_open_subgroup(p->pub, o.track.sub, &sc, p->now,
                                         &asg) == MOQ_OK);
    PC_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
    uint32_t slot = UINT32_MAX;
    for (uint32_t i = 0; i < p->pub_br->dstream_cap; ++i)
        if (p->pub_br->dstreams[i].active) {
            PC_REQUIRE(slot == UINT32_MAX);
            slot = i;
        }
    PC_REQUIRE(slot != UINT32_MAX);
    mt_stream_t *ast = NULL;
    for (size_t i = 0; i < MT_MAX_STREAMS; ++i)
        if (p->mt->streams[i].used && p->mt->streams[i].data &&
            p->mt->streams[i].refused_writes > 0)
            ast = &p->mt->streams[i];
    PC_REQUIRE(ast != NULL);
    PC_REQUIRE(moq_session_reset_subgroup(p->pub, asg, 0x1 /* CANCELLED */,
                                          p->now) == MOQ_OK);
    PC_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
    PC_REQUIRE(p->pub_br->dstreams[slot].active &&
               p->pub_br->dstreams[slot].has_reset);
    MOQ_TEST_CHECK_EQ_INT((int)p->pub_br->dreset_count, 1);
    CHECK_TABLE_COUNTS(p->pub_br);
    PC_REQUIRE(moq_session_open_subgroup(p->pub, fill.track.sub, &sc, p->now,
                                         &fsg) == MOQ_OK);
    for (unsigned i = 0; i < 8; ++i)
        PC_REQUIRE(write_object(p, fsg, 4, 1, 0, i) == MOQ_OK);
    PC_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
    PC_REQUIRE(p->pub_br->dq_bytes >= p->pub_br->max_retained_bytes);
    /* Opened and cancelled with no service between: its actions wait in
     * the core behind the budget. It opens the next transport stream. */
    sc.group_id = 2;
    PC_REQUIRE(moq_session_open_subgroup(p->pub, o.track.sub, &sc, p->now,
                                         &bsg) == MOQ_OK);
    PC_REQUIRE(moq_session_reset_subgroup(p->pub, bsg, 0x1, p->now) == MOQ_OK);
    mt_stream_t *bst = mt_stream(p->mt, p->mt->next_uni);
    PC_REQUIRE(bst != NULL && bst != ast);
    bst->refuse_reset = true;
    p->mt->cap = 1u << 20;
    const uint64_t pass = p->pub_br->pass_seq + 1;
    WALK_MARK(w);
    PC_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
    PC_REQUIRE(p->pub_br->pass_seq == pass);
    MOQ_TEST_CHECK(ast->reset);
    MOQ_TEST_CHECK_EQ_INT((int)ast->reset_attempts, 1);
    const bridge_dstream_t *d = &p->pub_br->dstreams[slot];
    MOQ_TEST_CHECK(d->active && d->has_reset && d->has_sid &&
                   d->sid == bst->sid);
    MOQ_TEST_CHECK_EQ_U64(d->reset_pass, pass);
    MOQ_TEST_CHECK_EQ_INT((int)bst->reset_attempts, 1);
    MOQ_TEST_CHECK(!bst->reset);
    /* A's release took one debt away and B's slot added one back. */
    MOQ_TEST_CHECK_EQ_INT((int)p->pub_br->dreset_count, 1);
    CHECK_TABLE_COUNTS(p->pub_br);
    WALK_CHECK(walk_since(w).reset_slots > 0);
    bst->refuse_reset = false;
    PC_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
    MOQ_TEST_CHECK(bst->reset);
    MOQ_TEST_CHECK_EQ_INT((int)bst->reset_attempts, 2);
    MOQ_TEST_CHECK(!p->pub_br->dstreams[slot].active);
    MOQ_TEST_CHECK_EQ_INT((int)p->pub_br->dreset_count, 0);
    CHECK_TABLE_COUNTS(p->pub_br);
    serve_all(p);
    return true;
}

/* An untimed backlog held at the budget, with no reset owed: however large
 * the table or the backlog, service reads no table slot for expiry and none
 * for deferred resets, and objects still go out once the budget frees.
 * With nothing left at all, the pending query reaches its reset check and
 * reads no slot either. */
static bool scan_zero_row(pair_t *p, unsigned backlog)
{
    regression_owner_t u;
    if (!regression_owner(p, false, "untimed", 128, 0, &u)) return false;
    moq_subgroup_handle_t sg;
    uint64_t sid;
    size_t header;
    if (!regression_open_header(p, &u, 1, &sg, &sid, &header)) return false;
    serve_all(p);
    p->mt->cap = 1;
    for (unsigned i = 0; i < backlog; ++i)
        PC_REQUIRE(write_object(p, sg, 1, 1, 0, i) == MOQ_OK);
    WALK_MARK(w);
    for (unsigned i = 0; i < 4; ++i)
        PC_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
    MOQ_TEST_CHECK(p->pub_br->dq_items > 0);
    MOQ_TEST_CHECK_EQ_INT((int)p->pub_br->dq_timed, 0);
    MOQ_TEST_CHECK_EQ_INT((int)p->pub_br->dreset_count, 0);
    CHECK_TABLE_COUNTS(p->pub_br);
    WALK_CHECK(walk_since(w).expiry_slots == 0 &&
               walk_since(w).expiry_items == 0 &&
               walk_since(w).reset_slots == 0);
    p->mt->cap = 1u << 20;
    const size_t chunks = p->mt->nchunks;
    serve_all(p);
    MOQ_TEST_CHECK(p->mt->nchunks >= chunks + backlog);
    PC_REQUIRE(p->pub_br->pending_count == 0 && p->pub_br->dq_items == 0 &&
               !p->pub_br->pending_control && !p->pub_br->needs_close);
    WALK_MARK(q);
    MOQ_TEST_CHECK(!moq_transport_bridge_has_pending(p->pub_br));
    WALK_CHECK(walk_since(q).query_slots == 0);
    return true;
}

/* A deferred reset made by expiry, the other way a stream comes to owe one,
 * counted and kept through the transport's refusals: the walk tries it once
 * in each pass, it is the sole pending work, and the release that follows
 * its acceptance takes it away. */
static bool scan_expiry_debt_row(pair_t *p)
{
    regression_owner_t o;
    if (!regression_owner(p, false, "expiry-debt", 128, 1, &o)) return false;
    moq_subgroup_handle_t sg;
    uint64_t sid;
    size_t header;
    if (!regression_open_header(p, &o, 1, &sg, &sid, &header)) return false;
    serve_all(p);
    mt_stream_t *st = mt_stream(p->mt, sid);
    PC_REQUIRE(st != NULL);
    p->mt->cap = 0;
    const uint64_t deadline = p->now + 1000;
    PC_REQUIRE(write_object(p, sg, 1, 1, 0, 0) == MOQ_OK);
    PC_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)p->pub_br->dq_timed, 1);
    MOQ_TEST_CHECK_EQ_INT((int)p->pub_br->dreset_count, 0);
    st->refuse_reset = true;
    p->now = deadline;
    PC_REQUIRE(moq_session_tick(p->pub, p->now) == MOQ_OK);
    const uint32_t before = st->reset_attempts;
    for (unsigned i = 1; i <= 3; ++i) {
        WALK_MARK(w);
        PC_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)(st->reset_attempts - before), (int)i);
        MOQ_TEST_CHECK(!st->reset);
        MOQ_TEST_CHECK_EQ_INT((int)p->pub_br->dq_timed, 0);
        MOQ_TEST_CHECK_EQ_INT((int)p->pub_br->dreset_count, 1);
        CHECK_TABLE_COUNTS(p->pub_br);
        WALK_CHECK(walk_since(w).reset_slots > 0);
    }
    MOQ_TEST_CHECK(p->pub_br->dq_items == 0 && p->pub_br->pending_count == 0);
    MOQ_TEST_CHECK(moq_transport_bridge_has_pending(p->pub_br));
    st->refuse_reset = false;
    PC_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
    MOQ_TEST_CHECK(st->reset);
    MOQ_TEST_CHECK_EQ_U64(st->reset_code, MOQ_RESET_DELIVERY_TIMEOUT);
    MOQ_TEST_CHECK_EQ_INT((int)p->pub_br->dreset_count, 0);
    CHECK_TABLE_COUNTS(p->pub_br);
    p->mt->cap = 1u << 20;
    serve_all(p);
    return true;
}

/* A reset the transport refuses for a stream with nothing queued waits in
 * the pending FIFO, not as a stream's deferred reset: it is never counted
 * as one, the bridge stays pending through the FIFO, and its retry sends
 * it. */
static bool scan_pending_reset_row(pair_t *p)
{
    regression_owner_t o;
    if (!regression_owner(p, false, "fifo-reset", 128, 0, &o)) return false;
    moq_subgroup_handle_t sg;
    uint64_t sid;
    size_t header;
    if (!regression_open_header(p, &o, 1, &sg, &sid, &header)) return false;
    serve_all(p);
    mt_stream_t *st = mt_stream(p->mt, sid);
    PC_REQUIRE(st != NULL);
    st->refuse_reset = true;
    PC_REQUIRE(moq_session_reset_subgroup(p->pub, sg, 0x1 /* CANCELLED */,
                                          p->now) == MOQ_OK);
    for (unsigned i = 0; i < 3; ++i) {
        PC_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
        MOQ_TEST_CHECK(!st->reset && st->reset_attempts > 0);
        MOQ_TEST_CHECK(p->pub_br->pending_count > 0);
        MOQ_TEST_CHECK_EQ_INT((int)p->pub_br->dq_items, 0);
        MOQ_TEST_CHECK_EQ_INT((int)p->pub_br->dreset_count, 0);
        CHECK_TABLE_COUNTS(p->pub_br);
        MOQ_TEST_CHECK(moq_transport_bridge_has_pending(p->pub_br));
    }
    st->refuse_reset = false;
    PC_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
    MOQ_TEST_CHECK(st->reset);
    MOQ_TEST_CHECK_EQ_U64(st->reset_code, 0x1);
    MOQ_TEST_CHECK_EQ_INT((int)p->pub_br->pending_count, 0);
    MOQ_TEST_CHECK_EQ_INT((int)p->pub_br->dreset_count, 0);
    serve_all(p);
    return true;
}

/* Debt held while the table doubles: the count follows the slots it names
 * into the larger table, and settles to zero after. */
static bool scan_growth_debt_row(pair_t *p)
{
    regression_owner_t o;
    if (!regression_owner(p, false, "growth", 128, 0, &o)) return false;
    p->mt->cap = 0;
    moq_subgroup_cfg_t sc;
    moq_subgroup_cfg_init(&sc);
    sc.group_id = 1;
    sc.publisher_priority = 128;
    moq_subgroup_handle_t asg;
    PC_REQUIRE(moq_session_open_subgroup(p->pub, o.track.sub, &sc, p->now,
                                         &asg) == MOQ_OK);
    PC_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
    mt_stream_t *ast = NULL;
    for (size_t i = 0; i < MT_MAX_STREAMS; ++i)
        if (p->mt->streams[i].used && p->mt->streams[i].data &&
            p->mt->streams[i].refused_writes > 0)
            ast = &p->mt->streams[i];
    PC_REQUIRE(ast != NULL);
    PC_REQUIRE(moq_session_reset_subgroup(p->pub, asg, 0x1 /* CANCELLED */,
                                          p->now) == MOQ_OK);
    PC_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)p->pub_br->dreset_count, 1);
    const uint32_t cap0 = p->pub_br->dstream_cap;
    /* More queued streams than the table holds, the debt outstanding. */
    moq_subgroup_handle_t more[16];
    PC_REQUIRE(cap0 < 16);
    for (unsigned i = 0; i < cap0; ++i) {
        sc.group_id = 2 + i;
        PC_REQUIRE(moq_session_open_subgroup(p->pub, o.track.sub, &sc, p->now,
                                             &more[i]) == MOQ_OK);
        PC_REQUIRE(write_object(p, more[i], 1, sc.group_id, 0, 0) == MOQ_OK);
    }
    PC_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
    MOQ_TEST_CHECK(p->pub_br->dstream_cap > cap0);
    MOQ_TEST_CHECK_EQ_INT((int)p->pub_br->dreset_count, 1);
    CHECK_TABLE_COUNTS(p->pub_br);
    p->mt->cap = 1u << 20;
    serve_all(p);
    MOQ_TEST_CHECK(ast->reset);
    MOQ_TEST_CHECK_EQ_INT((int)p->pub_br->dreset_count, 0);
    return true;
}

/* Several debts outstanding when the session closes, or when the bridge
 * latches fatal: either clears the table, the debts with it, and the count
 * ends at zero. */
static bool scan_close_debts_row(pair_t *p, bool fatal)
{
    regression_owner_t o;
    if (!regression_owner(p, false, "close", 128, 0, &o)) return false;
    p->mt->cap = 0;
    moq_subgroup_cfg_t sc;
    moq_subgroup_cfg_init(&sc);
    sc.publisher_priority = 128;
    for (unsigned i = 0; i < 3; ++i) {
        sc.group_id = 1 + i;
        moq_subgroup_handle_t sg;
        PC_REQUIRE(moq_session_open_subgroup(p->pub, o.track.sub, &sc, p->now,
                                             &sg) == MOQ_OK);
        PC_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
        PC_REQUIRE(moq_session_reset_subgroup(p->pub, sg, 0x1 /* CANCELLED */,
                                              p->now) == MOQ_OK);
    }
    for (size_t i = 0; i < MT_MAX_STREAMS; ++i)
        if (p->mt->streams[i].used && p->mt->streams[i].data)
            p->mt->streams[i].refuse_reset = true;
    p->mt->cap = 1u << 20;
    PC_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)p->pub_br->dreset_count, 3);
    CHECK_TABLE_COUNTS(p->pub_br);
    if (fatal) {
#ifdef MOQ_SESSION_SWEEP_TESTING
        bridge_set_fatal(p->pub_br, 0x1);
#endif
    } else {
        PC_REQUIRE(moq_session_close(p->pub, 0x0, "done", p->now) == MOQ_OK);
        for (unsigned i = 0; i < 4; ++i)
            (void)moq_transport_bridge_service(p->pub_br, p->now);
    }
    MOQ_TEST_CHECK(moq_transport_bridge_is_terminal(p->pub_br));
    MOQ_TEST_CHECK_EQ_INT((int)p->pub_br->dreset_count, 0);
    CHECK_TABLE_COUNTS(p->pub_br);
    return true;
}

/* Subscribe to live/`name` and accept it: true once the subscriber holds
 * the SUBSCRIBE_OK, which only the publisher's control write carries. */
static bool scan_answered(pair_t *p, const char *name)
{
    moq_bytes_t part = MOQ_BYTES_LITERAL("live");
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ &part, 1 };
    sc.track_name = (moq_bytes_t){ (const uint8_t *)name, strlen(name) };
    moq_subscription_t csub;
    if (moq_session_subscribe(p->sub, &sc, p->now, &csub) != MOQ_OK)
        return false;
    shuttle(p);
    moq_event_t ev;
    moq_subscription_t sub;
    memset(&sub, 0, sizeof(sub));
    bool got = false;
    while (moq_session_poll_events(p->pub, &ev, 1) == 1) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
            sub = ev.u.subscribe_request.sub;
            got = true;
        }
        moq_event_cleanup(&ev);
    }
    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    if (!got || moq_session_accept_subscribe(p->pub, sub, &ac, p->now) != MOQ_OK)
        return false;
    shuttle(p);
    bool ok = false;
    while (moq_session_poll_events(p->sub, &ev, 1) == 1) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_OK &&
            moq_subscription_eq(ev.u.subscribe_ok.sub, csub))
            ok = true;
        moq_event_cleanup(&ev);
    }
    return ok;
}

/* Control is written while untimed data waits, with no deferred reset and
 * then with one the transport refuses: neither skipped walk may hold back the
 * control work that runs after it in the pass. The reset is deferred behind
 * its subgroup's reliable header, which the shared budget holds back; the
 * header then goes while the untimed stream's own budget keeps its data
 * waiting, so the reset is due, tried and refused while the subscription is
 * answered. */
static bool scan_control_progress_row(pair_t *p)
{
    regression_owner_t u;
    if (!regression_owner(p, false, "control", 128, 0, &u)) return false;
    moq_subgroup_handle_t sg;
    uint64_t sid;
    size_t header;
    if (!regression_open_header(p, &u, 1, &sg, &sid, &header)) return false;
    serve_all(p);
    mt_stream_t *ust = mt_stream(p->mt, sid);
    PC_REQUIRE(ust != NULL);
    p->mt->cap = 0;
    PC_REQUIRE(write_object(p, sg, 1, 1, 0, 0) == MOQ_OK);
    PC_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
    MOQ_TEST_CHECK(p->pub_br->dq_items > 0);
    MOQ_TEST_CHECK_EQ_INT((int)p->pub_br->dq_timed, 0);
    MOQ_TEST_CHECK_EQ_INT((int)p->pub_br->dreset_count, 0);

    /* No deferred reset. */
    MOQ_TEST_CHECK(scan_answered(p, "ctl-a"));
    MOQ_TEST_CHECK(p->pub_br->dq_items > 0);   /* the data still waits */
    MOQ_TEST_CHECK_EQ_INT((int)p->pub_br->dq_timed, 0);
    MOQ_TEST_CHECK_EQ_INT((int)p->pub_br->dreset_count, 0);
    CHECK_TABLE_COUNTS(p->pub_br);

    /* A second subgroup, cancelled while the budget holds back its header:
     * the reset is a deferred one, behind that reliable prefix. */
    moq_subgroup_cfg_t sc;
    moq_subgroup_cfg_init(&sc);
    sc.group_id = 2;
    sc.publisher_priority = 128;
    moq_subgroup_handle_t rsg;
    PC_REQUIRE(moq_session_open_subgroup(p->pub, u.track.sub, &sc, p->now,
                                         &rsg) == MOQ_OK);
    PC_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
    mt_stream_t *rst = NULL;
    for (size_t i = 0; i < MT_MAX_STREAMS; ++i)
        if (p->mt->streams[i].used && p->mt->streams[i].data &&
            p->mt->streams[i].sid != sid) {
            PC_REQUIRE(rst == NULL);
            rst = &p->mt->streams[i];
        }
    PC_REQUIRE(rst != NULL && rst->refused_writes > 0 &&
               rst->first_write_len > 0);
    rst->refuse_reset = true;
    PC_REQUIRE(moq_session_reset_subgroup(p->pub, rsg, 0x1 /* CANCELLED */,
                                          p->now) == MOQ_OK);
    PC_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
    PC_REQUIRE(p->pub_br->dreset_count == 1);
    PC_REQUIRE(rst->reset_attempts == 0);   /* not due behind its header */

    /* The header goes and the untimed stream's own budget keeps its data
     * waiting: the reset is due, and the transport refuses it. */
    ust->refuse = true;
    p->mt->cap = 1u << 20;
    for (unsigned i = 0; i < 2; ++i)
        PC_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
    PC_REQUIRE(rst->reset_attempts > 0);
    PC_REQUIRE(!rst->reset);
    PC_REQUIRE(p->pub_br->dreset_count == 1);
    PC_REQUIRE(p->pub_br->dq_timed == 0);
    MOQ_TEST_CHECK(p->pub_br->dq_items > 0);
    CHECK_TABLE_COUNTS(p->pub_br);

    /* Answered while the refused reset is held, and tried again meanwhile. */
    const uint32_t attempts = rst->reset_attempts;
    MOQ_TEST_CHECK(scan_answered(p, "ctl-b"));
    MOQ_TEST_CHECK(rst->reset_attempts > attempts);
    MOQ_TEST_CHECK(!rst->reset);
    MOQ_TEST_CHECK_EQ_INT((int)p->pub_br->dreset_count, 1);
    MOQ_TEST_CHECK(p->pub_br->dq_items > 0);
    MOQ_TEST_CHECK_EQ_INT((int)p->pub_br->dq_timed, 0);
    CHECK_TABLE_COUNTS(p->pub_br);

    /* Accepted: the release takes exactly that debt away. */
    rst->refuse_reset = false;
    PC_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
    MOQ_TEST_CHECK(rst->reset);
    MOQ_TEST_CHECK_EQ_U64(rst->reset_code, 0x1);
    MOQ_TEST_CHECK_EQ_U64(rst->reset_reliable, rst->first_write_len);
    MOQ_TEST_CHECK_EQ_INT((int)p->pub_br->dreset_count, 0);
    MOQ_TEST_CHECK(p->pub_br->dq_items > 0);   /* the data still waits */
    CHECK_TABLE_COUNTS(p->pub_br);
    ust->refuse = false;
    serve_all(p);
    MOQ_TEST_CHECK_EQ_INT((int)p->pub_br->dq_items, 0);
    MOQ_TEST_CHECK_EQ_INT((int)p->pub_br->dreset_count, 0);
    return true;
}

/* Only the queued-expiry walk expires a queued item. While one with a finite
 * deadline waits, every pass before the deadline reads the table and that
 * item, and expires nothing. The row requires those reads pass by pass and
 * returns at the first missing one, before any service at or past the
 * deadline: a pass with a due item and no walk to act on it does not return.
 * With the reads there, it lets the deadline pass and requires the expiry. */
static bool expiry_walk_bound_row(pair_t *p)
{
    regression_owner_t t;
    if (!regression_owner(p, false, "bounded", 128, 1, &t)) return false;
    moq_subgroup_handle_t sg;
    uint64_t sid;
    size_t header;
    if (!regression_open_header(p, &t, 1, &sg, &sid, &header)) return false;
    serve_all(p);
    mt_stream_t *st = mt_stream(p->mt, sid);
    PC_REQUIRE(st != NULL);
    p->mt->cap = 0; /* no data write fits */
    const uint64_t deadline = p->now + 1000;
    PC_REQUIRE(write_object(p, sg, 1, 1, 0, 0) == MOQ_OK);
    PC_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
    PC_REQUIRE(p->pub_br->dq_timed == 1);
    PC_REQUIRE(moq_session_next_deadline_us(p->pub) == deadline);
    /* Queued from that pass on (a pass expires before it drains new
     * actions): read now, and again just before the deadline. */
    const uint64_t before[2] = { p->now, deadline - 1 };
    for (unsigned i = 0; i < 2; ++i) {
        p->now = before[i];
        PC_REQUIRE(moq_session_tick(p->pub, p->now) == MOQ_OK);
        WALK_MARK(w);
        PC_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
        WALK_REQUIRE(whole_walks(walk_since(w).expiry_slots,
                                 p->pub_br->dstream_cap) &&
                     walk_since(w).expiry_items ==
                         walk_since(w).expiry_slots / p->pub_br->dstream_cap);
        PC_REQUIRE(!st->reset);
        PC_REQUIRE(p->pub_br->dq_timed == 1 && p->pub_br->dq_items == 1);
    }
    p->now = deadline;
    PC_REQUIRE(moq_session_tick(p->pub, p->now) == MOQ_OK);
    if (!regression_service(p)) return false;
    MOQ_TEST_CHECK(st->reset);
    MOQ_TEST_CHECK_EQ_U64(st->reset_code, MOQ_RESET_DELIVERY_TIMEOUT);
    MOQ_TEST_CHECK_EQ_INT((int)p->pub_br->dq_timed, 0);
    p->mt->cap = 1u << 20;
    serve_all(p);
    return true;
}

#undef PC_REQUIRE

/* The table walks a pass skips when it has no corresponding work, and the
 * counts that decide it, against the table itself. */
static void test_scan_work(void)
{
    for (size_t v = 1; v < N_VERSIONS; ++v) {
        pair_t p;
        static const uint32_t tables[] = { PC_STREAMS, 4u * PC_STREAMS };
        static const unsigned backlogs[] = { 2, 16 };
        for (size_t t = 0; t < 2; ++t) {
            for (size_t k = 0; k < 2; ++k) {
                if (pair_init_retained(&p, k_versions[v], 1u << 20, true,
                                       tables[t], 1024, 1u << 20, 0, true))
                    MOQ_TEST_CHECK(scan_zero_row(&p, backlogs[k]));
                else MOQ_TEST_CHECK(false);
                pair_destroy(&p);
            }
        }
        static bool (*const rows[])(pair_t *) = {
            scan_expiry_debt_row, scan_pending_reset_row,
            scan_control_progress_row,
        };
        for (size_t r = 0; r < sizeof(rows) / sizeof(rows[0]); ++r) {
            if (pair_init_retained(&p, k_versions[v], 1u << 20, true,
                                   PC_STREAMS, 1024, 1u << 20, 0, true))
                MOQ_TEST_CHECK(rows[r](&p));
            else MOQ_TEST_CHECK(false);
            pair_destroy(&p);
        }
        /* The fatal latch is internal to the bridge: only the build linked
         * against the test-internals core can reach it. */
#ifdef MOQ_SESSION_SWEEP_TESTING
        const int ends = 2;
#else
        const int ends = 1;
#endif
        for (int fatal = 0; fatal < ends; ++fatal) {
            if (pair_init_retained(&p, k_versions[v], 1u << 20, true,
                                   PC_STREAMS, 1024, 1u << 20, 0, true))
                MOQ_TEST_CHECK(scan_close_debts_row(&p, fatal != 0));
            else MOQ_TEST_CHECK(false);
            pair_destroy(&p);
        }
        /* A table small enough to double. */
        if (pair_init_retained(&p, k_versions[v], 1u << 20, true, 8, 1024,
                               1u << 20, 0, true))
            MOQ_TEST_CHECK(scan_growth_debt_row(&p));
        else MOQ_TEST_CHECK(false);
        pair_destroy(&p);
        printf("scan work d%d: checked\n", draft_of(k_versions[v]));
    }
    MOQ_TEST_PASS("a pass reads the stream table for expiry only while a "
                  "queued item has a deadline and for deferred resets only "
                  "while one is held; both counts match the table through "
                  "refusal, expiry, release, reuse, growth and close, and "
                  "control still goes out");
}

/* The queued-expiry walk before a finite deadline, on its own: the row
 * returns at the first pass that does not read, so a run here ends with a
 * failure, not a service call that never returns. */
static void test_expiry_walk(void)
{
    for (size_t v = 1; v < N_VERSIONS; ++v) {
        pair_t p;
        if (pair_init_retained(&p, k_versions[v], 1u << 20, true, PC_STREAMS,
                               1024, 1u << 20, 0, true))
            MOQ_TEST_CHECK(expiry_walk_bound_row(&p));
        else MOQ_TEST_CHECK(false);
        pair_destroy(&p);
        printf("expiry walk d%d: checked\n", draft_of(k_versions[v]));
    }
    MOQ_TEST_PASS("while a queued item with a finite deadline waits, every "
                  "pass before the deadline reads the stream table and that "
                  "item and expires nothing; at the deadline it expires");
}

static void test_pass_costs(void)
{
    for (size_t v = 1; v < N_VERSIONS; ++v) {
        moq_rcbuf_t *payload[3] = { NULL, NULL, NULL };
        pair_t p;
        if (pair_init_retained(&p, k_versions[v], 1u << 20, true, PC_STREAMS,
                               1024, 1u << 20, 0, true))
            MOQ_TEST_CHECK(pass_cost_deadline_row(&p, payload));
        else MOQ_TEST_CHECK(false);
        pair_destroy(&p);
        for (unsigned i = 0; i < 3; ++i)
            if (payload[i]) {
                MOQ_TEST_CHECK_EQ_INT((int)moq_rcbuf_refcount(payload[i]), 1);
                moq_rcbuf_decref(payload[i]);
            }
        if (pair_init_retained(&p, k_versions[v], 1u << 20, true, PC_STREAMS,
                               1024, 1u << 20, 0, true))
            MOQ_TEST_CHECK(pass_cost_reset_row(&p));
        else MOQ_TEST_CHECK(false);
        pair_destroy(&p);
        static bool (*const rows[])(pair_t *) = {
            pass_cost_earliest_row, pass_cost_unpop_row, pass_cost_pending_row,
        };
        for (size_t r = 0; r < sizeof(rows) / sizeof(rows[0]); ++r) {
            if (pair_init_retained(&p, k_versions[v], 1u << 20, true,
                                   PC_STREAMS, 1024, 1u << 20, 0, true))
                MOQ_TEST_CHECK(rows[r](&p));
            else MOQ_TEST_CHECK(false);
            pair_destroy(&p);
        }
        moq_rcbuf_t *cancelled[5] = { NULL, NULL, NULL, NULL, NULL };
        if (pair_init_retained(&p, k_versions[v], 1u << 20, true, PC_STREAMS,
                               1024, 1u << 20, 0, true))
            MOQ_TEST_CHECK(pass_cost_cancel_row(&p, cancelled));
        else MOQ_TEST_CHECK(false);
        pair_destroy(&p);
        for (unsigned i = 0; i < 5; ++i)
            if (cancelled[i]) {
                MOQ_TEST_CHECK_EQ_INT((int)moq_rcbuf_refcount(cancelled[i]), 1);
                moq_rcbuf_decref(cancelled[i]);
            }
        /* A retained budget a few objects fill. */
        if (pair_init_retained(&p, k_versions[v], 1u << 20, true, PC_STREAMS,
                               1024, 64, 0, true))
            MOQ_TEST_CHECK(pass_cost_reuse_row(&p));
        else MOQ_TEST_CHECK(false);
        pair_destroy(&p);
        printf("pass costs d%d: checked\n", draft_of(k_versions[v]));
    }
    MOQ_TEST_PASS("deadline walks read the stream table only for timed items "
                  "and keep the exact earliest deadline through refusal, "
                  "expiry, cancellation and the pending FIFO; a refused reset "
                  "is tried once a pass, a reused slot's reset included");
}

/* -- session queue inspection ----------------------------------------------- */

/* The session's deadline query walks its live subgroups, subscriptions and
 * publications rather than their configured pools, and reads its queued
 * actions only while one carries a deadline; expiry searches the queue for a
 * due action, and the reset offer for the first control action, only where
 * the queue can hold one. Each row holds its live set fixed and asserts the
 * exact deadline, the wire and the session's queue summary; in the build with
 * the session's probe counters it also asserts what each walk read. */

/* A step the row cannot go on without: it returns at the first miss. */
#define SS_REQUIRE(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
        return false; \
    } \
} while (0)

#define SS_TIMEOUT_MS 7u
#define SS_TIMEOUT_US ((uint64_t)SS_TIMEOUT_MS * 1000u)
#define SS_LONG_MS 20u
#define SS_LONG_US ((uint64_t)SS_LONG_MS * 1000u)
#define SS_DONE_WAIT_US 40000u

/* Pools far larger than any row's live set, and the defaults, both with an
 * explicit PUBLISH_DONE stream wait. */
static const pools_t k_ss_sparse = { 1024, 1024, 256, SS_DONE_WAIT_US };
static const pools_t k_ss_default = { 0, 0, 0, SS_DONE_WAIT_US };

static bool ss_pair(pair_t *p, moq_version_t ver, const pools_t *pools,
                    uint32_t max_actions, uint64_t retained)
{
    g_pools = *pools;
    const bool ok = pair_init_retained(p, ver, 1u << 20, true, PC_STREAMS,
                                       max_actions, retained, 0, true);
    memset(&g_pools, 0, sizeof(g_pools));
    return ok;
}

/* Objects stream `sid` carried to the wire, recognized as wire_objects
 * recognizes them: by the payload's length and marker. */
static size_t ss_objects_on(const pair_t *p, uint64_t sid)
{
    size_t n = 0;
    for (size_t i = 0; i < p->mt->nwire; i++)
        if (p->mt->wire[i].sid == sid && !p->mt->wire[i].reset &&
            p->mt->wire[i].len == PAYLOAD_LEN &&
            p->mt->wire[i].head[0] == PAYLOAD_TAG)
            n++;
    return n;
}

/* Stream `sid` carried exactly `want` to the wire, each once and in order.
 * An object is identified as wire_objects identifies it, by its payload's
 * length, marker and four identifying bytes, so a duplicate, a loss or a
 * swap between the stream's objects shows; the rest of the payload is not
 * compared. */
static bool ss_stream_carried(const pair_t *p, uint64_t sid,
                              const uint32_t *want, size_t n)
{
    size_t seen = 0;
    bool in_order = true;
    for (size_t i = 0; i < p->mt->nwire; i++) {
        const uint8_t *h = p->mt->wire[i].head;
        if (p->mt->wire[i].sid != sid || p->mt->wire[i].reset ||
            p->mt->wire[i].len != PAYLOAD_LEN || h[0] != PAYLOAD_TAG)
            continue;
        const uint32_t id = (uint32_t)h[1] << 24 | (uint32_t)h[2] << 16 |
                            (uint32_t)h[3] << 8 | h[4];
        if (seen >= n || id != want[seen]) in_order = false;
        seen++;
    }
    if (!in_order || seen != n)
        fprintf(stderr, "stream %llu carried %zu objects, %zu expected%s\n",
                (unsigned long long)sid, seen, n,
                in_order ? "" : ", not the expected ones in order");
    return in_order && seen == n;
}

/* Actions waiting in the session's ring. */
static size_t ss_queued(const moq_session_t *s)
{
    return s->action_tail - s->action_head;
}

/* Live owners, counted over the whole of each pool: what a walk over the
 * live owners alone must read. */
static size_t ss_live_subgroups(const moq_session_t *s)
{
    size_t n = 0;
    for (size_t i = 0; i < s->sg_cap; i++)
        if (s->subgroups[i].state != MOQ_SG_FREE) n++;
    return n;
}

static size_t ss_live_subs(const moq_session_t *s)
{
    size_t n = 0;
    for (size_t i = 0; i < s->sub_cap; i++)
        if (s->subs[i].state != MOQ_SUB_FREE) n++;
    return n;
}

static size_t ss_live_pubs(const moq_session_t *s)
{
    size_t n = 0;
    for (size_t i = 0; i < s->pub_cap; i++)
        if (s->publishes[i].state != MOQ_PUB_FREE) n++;
    return n;
}

/* The occupancy lists the walks follow are whole and exact. */
static bool ss_occupancy_ok(const moq_session_t *s)
{
    const char *pool = NULL;
    size_t slot = 0;
    const occ_fault_t f = occ_check_all(s, &pool, &slot);
    if (f != OCC_OK)
        fprintf(stderr, "occupancy: %s pool, slot %zu: %s\n", pool, slot,
                occ_fault_name(f));
    return f == OCC_OK;
}

/* A deadline relative to the row's start; none stays none. */
static uint64_t ss_rel(uint64_t d, uint64_t t0)
{
    return d == UINT64_MAX ? UINT64_MAX : d - t0;
}

/* What a sparse row observed, compared between pool sizes: its deadlines
 * and, in the probe build, what each walk read. The wire is checked in the
 * row itself. */
typedef struct {
    uint64_t pub_dl, sub_dl, drained_dl, closed_dl;
#ifdef MOQ_SESSION_SWEEP_TESTING
    walk_t pub_walk, sub_walk;
#endif
} ss_image_t;

/* Three owners -- a subscription whose objects carry a 7 ms delivery
 * timeout, an untimed subscription and a publication -- each open one
 * subgroup, and objects wait in the publisher's session. Its deadline is the
 * timed objects' (draft-16's one DELIVERY TIMEOUT also bounds that stream,
 * whose timer started when it opened, with them); once they are written,
 * each stream carrying its own objects once and in order, only that
 * draft-16 subgroup timer is left, and once the subgroups close none is. The
 * subscriber arms nothing. Each walk reads the live owners: three subgroups,
 * two subscriptions and one publication on the publisher; two subscriptions
 * and one publication on the subscriber. */
static bool ss_sparse_row(pair_t *p, ss_image_t *img)
{
    memset(img, 0, sizeof(*img));
    const uint64_t t0 = p->now;
    regression_owner_t timed, plain, pubo;
    if (!regression_owner(p, false, "ss-timed", 128, SS_TIMEOUT_MS, &timed) ||
        !regression_owner(p, false, "ss-plain", 128, 0, &plain) ||
        !regression_owner(p, true, "ss-pub", 128, 0, &pubo))
        return false;
    const regression_owner_t *owner[3] = { &timed, &plain, &pubo };
    moq_subgroup_handle_t sg[3];
    uint64_t sid[3];
    size_t header[3];
    for (int i = 0; i < 3; i++)
        if (!regression_open_header(p, owner[i], 1, &sg[i], &sid[i],
                                    &header[i]))
            return false;
    for (uint64_t o = 0; o < 2; o++)
        SS_REQUIRE(write_object(p, sg[0], 1, 1, 0, o) == MOQ_OK);
    SS_REQUIRE(write_object(p, sg[1], 2, 1, 0, 0) == MOQ_OK);
    SS_REQUIRE(write_object(p, sg[2], 3, 1, 0, 0) == MOQ_OK);
    CHECK_SESSION_COUNTS(p->pub);
    SS_REQUIRE(ss_queued(p->pub) >= 4);
    SS_REQUIRE(ss_live_subgroups(p->pub) == 3 && ss_live_subs(p->pub) == 2 &&
               ss_live_pubs(p->pub) == 1);
    SS_REQUIRE(ss_live_subgroups(p->sub) == 0 && ss_live_subs(p->sub) == 2 &&
               ss_live_pubs(p->sub) == 1);
    MOQ_TEST_CHECK(ss_occupancy_ok(p->pub) && ss_occupancy_ok(p->sub));
#ifdef MOQ_SESSION_SWEEP_TESTING
    const uint64_t queued = ss_queued(p->pub);
#endif
    WALK_MARK(w);
    img->pub_dl = ss_rel(moq_session_next_deadline_us(p->pub), t0);
#ifdef MOQ_SESSION_SWEEP_TESTING
    img->pub_walk = walk_since(w);
    WALK_CHECK(img->pub_walk.dl_sg == 3 && img->pub_walk.dl_sub == 2 &&
               img->pub_walk.dl_pub == 1 && img->pub_walk.queued_dl == queued);
#endif
    MOQ_TEST_CHECK_EQ_U64(img->pub_dl, SS_TIMEOUT_US);
    WALK_MARK(v);
    img->sub_dl = ss_rel(moq_session_next_deadline_us(p->sub), t0);
#ifdef MOQ_SESSION_SWEEP_TESTING
    img->sub_walk = walk_since(v);
    WALK_CHECK(img->sub_walk.dl_sg == 0 && img->sub_walk.dl_sub == 2 &&
               img->sub_walk.dl_pub == 1 && img->sub_walk.queued_dl == 0);
#endif
    MOQ_TEST_CHECK_EQ_U64(img->sub_dl, UINT64_MAX);
#ifdef MOQ_SESSION_SWEEP_TESTING
    /* A stream no subgroup carries: the lookup reads the three live ones
     * and finds none. */
    WALK_MARK(f);
    MOQ_TEST_CHECK_EQ_INT(sg_find_by_stream_ref(p->pub,
                              moq_stream_ref_from_u64(UINT64_MAX)), -1);
    WALK_CHECK(walk_since(f).sg_find == 3);
#endif

    serve_all(p);
    static const uint32_t timed_objs[] = { OBJ(1, 1, 0, 0), OBJ(1, 1, 0, 1) };
    static const uint32_t plain_objs[] = { OBJ(2, 1, 0, 0) };
    static const uint32_t pub_objs[] = { OBJ(3, 1, 0, 0) };
    MOQ_TEST_CHECK(ss_stream_carried(p, sid[0], timed_objs, 2));
    MOQ_TEST_CHECK(ss_stream_carried(p, sid[1], plain_objs, 1));
    MOQ_TEST_CHECK(ss_stream_carried(p, sid[2], pub_objs, 1));
    img->drained_dl = ss_rel(moq_session_next_deadline_us(p->pub), t0);
    MOQ_TEST_CHECK_EQ_U64(img->drained_dl, p->ver == MOQ_VERSION_DRAFT_16
                                               ? SS_TIMEOUT_US : UINT64_MAX);
    for (int i = 0; i < 3; i++)
        SS_REQUIRE(moq_session_close_subgroup(p->pub, sg[i], p->now) == MOQ_OK);
    serve_all(p);
    img->closed_dl = ss_rel(moq_session_next_deadline_us(p->pub), t0);
    MOQ_TEST_CHECK_EQ_U64(img->closed_dl, UINT64_MAX);
    return true;
}

/* Untimed objects wait in the session behind a network that takes one write
 * at a time and a bridge whose budget admits one. No pass searches them for
 * expiry, scans them for a reset, or reads them for a deadline, and the
 * session publishes none; once the network opens every object goes out, in
 * order. */
static bool ss_untimed_row(pair_t *p)
{
    regression_owner_t o;
    if (!regression_owner(p, false, "ss-untimed", 128, 0, &o)) return false;
    moq_subgroup_handle_t sg;
    uint64_t sid;
    size_t header;
    if (!regression_open_header(p, &o, 1, &sg, &sid, &header)) return false;
    p->mt->cap = 1;
    for (uint64_t i = 0; i < 6; i++)
        SS_REQUIRE(write_object(p, sg, 1, 1, 0, i) == MOQ_OK);
    for (unsigned pass = 0; pass < 4; pass++) {
        WALK_MARK(w);
        SS_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
        WALK_CHECK(walk_since(w).expiry_search == 0 &&
                   walk_since(w).reset_offer == 0 &&
                   walk_since(w).queued_dl == 0);
        CHECK_TABLE_COUNTS(p->pub_br);
    }
    SS_REQUIRE(ss_queued(p->pub) > 0);
    SS_REQUIRE(p->pub->action_timed == 0 && p->pub->action_nondata == 0);
    MOQ_TEST_CHECK_EQ_U64(moq_session_next_deadline_us(p->pub), UINT64_MAX);
    p->mt->cap = 1u << 20;
    serve_all(p);
    uint32_t want[6];
    for (unsigned i = 0; i < 6; i++) want[i] = OBJ(1, 1, 0, i);
    expect_wire(p, want, 6, "untimed objects held in the session", 1);
    MOQ_TEST_CHECK_EQ_INT((int)ss_objects_on(p, sid), 6);
    return true;
}

/* Objects with a 7 ms timeout wait in the session behind a stalled network
 * (the bridge's budget admits one). Before their deadline, bridge service may
 * skip expiry after inspecting deadlines; an explicit session tick searches
 * without compacting. At the deadline the stream is reset with DELIVERY_TIMEOUT,
 * its header kept; afterwards nothing timed is queued or due. */
static bool ss_expiry_row(pair_t *p)
{
    const uint64_t t0 = p->now;
    regression_owner_t o;
    if (!regression_owner(p, false, "ss-expiry", 128, SS_TIMEOUT_MS, &o))
        return false;
    moq_subgroup_handle_t sg;
    uint64_t sid;
    size_t header;
    if (!regression_open_header(p, &o, 1, &sg, &sid, &header)) return false;
    p->mt->cap = 0;
    for (uint64_t i = 0; i < 3; i++)
        SS_REQUIRE(write_object(p, sg, 1, 1, 0, i) == MOQ_OK);
    SS_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
    CHECK_TABLE_COUNTS(p->pub_br);
    SS_REQUIRE(ss_queued(p->pub) > 0 && p->pub->action_timed > 0);
    MOQ_TEST_CHECK_EQ_U64(moq_session_next_deadline_us(p->pub),
                          t0 + SS_TIMEOUT_US);

    p->now = t0 + SS_TIMEOUT_US - 1;
    WALK_MARK(w);
    SS_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
    WALK_CHECK(walk_since(w).expiry_compact == 0);
    WALK_MARK(tick_work);
    SS_REQUIRE(moq_session_tick(p->pub, p->now) == MOQ_OK);
    WALK_CHECK(walk_since(tick_work).expiry_search > 0 &&
               walk_since(tick_work).expiry_compact == 0);
    const mt_stream_t *st = mt_stream(p->mt, sid);
    SS_REQUIRE(st != NULL);
    MOQ_TEST_CHECK(!st->reset);
    CHECK_TABLE_COUNTS(p->pub_br);
    MOQ_TEST_CHECK_EQ_U64(moq_session_next_deadline_us(p->pub),
                          t0 + SS_TIMEOUT_US);

    p->now = t0 + SS_TIMEOUT_US;
    SS_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
    CHECK_TABLE_COUNTS(p->pub_br);
    p->mt->cap = 1u << 20;
    serve_all(p);
    MOQ_TEST_CHECK(st->reset);
    MOQ_TEST_CHECK_EQ_U64(st->reset_code, MOQ_RESET_DELIVERY_TIMEOUT);
    MOQ_TEST_CHECK_EQ_U64(st->reset_reliable, header);
    MOQ_TEST_CHECK_EQ_INT((int)ss_objects_on(p, sid), 0);
    MOQ_TEST_CHECK_EQ_INT((int)p->pub->action_timed, 0);

    p->now = t0 + SS_TIMEOUT_US + 1;
    WALK_MARK(q);
    SS_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(moq_session_next_deadline_us(p->pub), UINT64_MAX);
    WALK_CHECK(walk_since(q).expiry_search == 0 &&
               walk_since(q).queued_dl == 0);
    return true;
}

/* The deadline moves earlier, then later. Two subscriptions, with 20 ms and
 * 7 ms delivery timeouts, each open a subgroup at the same instant behind a
 * network that takes no data. The longer one's object is queued first and
 * the shorter one's after it, which moves the deadline earlier, to the
 * shorter one's (draft-16's subgroup timers run from the open, so there it
 * is the deadline from the start). Just before it nothing changes; at it the
 * shorter stream is reset with DELIVERY_TIMEOUT after its header, and the
 * deadline moves later, to the longer object's. That object then goes out,
 * and once its subgroup closes nothing is due. */
static bool ss_earlier_row(pair_t *p)
{
    const uint64_t t0 = p->now;
    const bool d16 = p->ver == MOQ_VERSION_DRAFT_16;
    regression_owner_t lo, sh;
    if (!regression_owner(p, false, "ss-long", 128, SS_LONG_MS, &lo) ||
        !regression_owner(p, false, "ss-short", 128, SS_TIMEOUT_MS, &sh))
        return false;
    moq_subgroup_handle_t sgl, sgs;
    uint64_t sidl, sids;
    size_t hl, hs;
    if (!regression_open_header(p, &lo, 1, &sgl, &sidl, &hl) ||
        !regression_open_header(p, &sh, 1, &sgs, &sids, &hs))
        return false;
    p->mt->cap = 0;
    SS_REQUIRE(write_object(p, sgl, 1, 1, 0, 0) == MOQ_OK);
    CHECK_SESSION_COUNTS(p->pub);
    MOQ_TEST_CHECK_EQ_U64(moq_session_next_deadline_us(p->pub),
                          t0 + (d16 ? SS_TIMEOUT_US : SS_LONG_US));
    SS_REQUIRE(write_object(p, sgs, 2, 1, 0, 0) == MOQ_OK);
    CHECK_SESSION_COUNTS(p->pub);
    SS_REQUIRE(ss_queued(p->pub) == 2 && p->pub->action_timed == 2);
    WALK_MARK(w);
    MOQ_TEST_CHECK_EQ_U64(moq_session_next_deadline_us(p->pub),
                          t0 + SS_TIMEOUT_US);
    WALK_CHECK(walk_since(w).queued_dl == 2);

    p->now = t0 + SS_TIMEOUT_US - 1;
    SS_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
    CHECK_TABLE_COUNTS(p->pub_br);
    const mt_stream_t *st = mt_stream(p->mt, sids);
    SS_REQUIRE(st != NULL);
    MOQ_TEST_CHECK(!st->reset);
    MOQ_TEST_CHECK_EQ_U64(moq_session_next_deadline_us(p->pub),
                          t0 + SS_TIMEOUT_US);

    p->now = t0 + SS_TIMEOUT_US;
    SS_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
    CHECK_TABLE_COUNTS(p->pub_br);
    MOQ_TEST_CHECK(st->reset);
    MOQ_TEST_CHECK_EQ_U64(st->reset_code, MOQ_RESET_DELIVERY_TIMEOUT);
    MOQ_TEST_CHECK_EQ_U64(st->reset_reliable, hs);
    MOQ_TEST_CHECK_EQ_U64(moq_session_next_deadline_us(p->pub),
                          t0 + SS_LONG_US);

    p->mt->cap = 1u << 20;
    serve_all(p);
    MOQ_TEST_CHECK_EQ_INT((int)ss_objects_on(p, sidl), 1);
    MOQ_TEST_CHECK_EQ_INT((int)ss_objects_on(p, sids), 0);
    MOQ_TEST_CHECK_EQ_U64(moq_session_next_deadline_us(p->pub),
                          d16 ? t0 + SS_LONG_US : UINT64_MAX);
    SS_REQUIRE(moq_session_close_subgroup(p->pub, sgl, p->now) == MOQ_OK);
    serve_all(p);
    MOQ_TEST_CHECK_EQ_U64(moq_session_next_deadline_us(p->pub), UINT64_MAX);
    return true;
}

/* A reset offer observes only: even at an object's deadline it leaves a
 * data-only ring unchanged. Explicit outgoing service recognizes the timeout
 * before a later offer can return its reset. The untimed object still goes
 * out; the timed stream ends with DELIVERY_TIMEOUT after its header. */
static bool ss_offer_row(pair_t *p)
{
    const uint64_t t0 = p->now;
    regression_owner_t due, later;
    if (!regression_owner(p, false, "ss-due", 128, SS_TIMEOUT_MS, &due) ||
        !regression_owner(p, false, "ss-later", 128, 0, &later))
        return false;
    moq_subgroup_handle_t sgd, sgl;
    uint64_t sidd, sidl;
    size_t hd, hl;
    if (!regression_open_header(p, &due, 1, &sgd, &sidd, &hd) ||
        !regression_open_header(p, &later, 1, &sgl, &sidl, &hl))
        return false;
    p->mt->cap = 0;
    SS_REQUIRE(write_object(p, sgd, 1, 1, 0, 0) == MOQ_OK);
    p->now = t0 + SS_TIMEOUT_US;
    SS_REQUIRE(write_object(p, sgl, 2, 1, 0, 0) == MOQ_OK);
    SS_REQUIRE(ss_queued(p->pub) == 2 && p->pub->action_timed == 1 &&
               p->pub->action_nondata == 0);
#ifdef MOQ_SESSION_SWEEP_TESTING
    /* A passive offer must not turn timed data into a terminal as a side
     * effect. The bridge's explicit service call owns that transition. */
    moq_reset_data_action_t r;
    memset(&r, 0, sizeof(r));
    const size_t events_before = p->pub->event_tail - p->pub->event_head;
    MOQ_TEST_CHECK(!bridge_session_offer_data_reset(p->pub, &r));
    MOQ_TEST_CHECK_EQ_INT((int)ss_queued(p->pub), 2);
    MOQ_TEST_CHECK_EQ_INT((int)p->pub->action_nondata, 0);
    MOQ_TEST_CHECK_EQ_INT((int)p->pub->action_timed, 1);
    MOQ_TEST_CHECK_EQ_U64(p->pub->event_tail - p->pub->event_head, events_before);
    SS_REQUIRE(bridge_session_service_outgoing(p->pub, p->now) == MOQ_OK);
    MOQ_TEST_CHECK(bridge_session_offer_data_reset(p->pub, &r));
    MOQ_TEST_CHECK_EQ_U64(r.error_code, MOQ_RESET_DELIVERY_TIMEOUT);
    MOQ_TEST_CHECK_EQ_U64(r.reliable_size, hd);
    CHECK_SESSION_COUNTS(p->pub);
    MOQ_TEST_CHECK_EQ_INT((int)p->pub->action_nondata, 1);
    MOQ_TEST_CHECK_EQ_INT((int)p->pub->action_timed, 0);
#endif
    p->mt->cap = 1u << 20;
    serve_all(p);
    const mt_stream_t *st = mt_stream(p->mt, sidd);
    SS_REQUIRE(st != NULL);
    MOQ_TEST_CHECK(st->reset);
    MOQ_TEST_CHECK_EQ_U64(st->reset_code, MOQ_RESET_DELIVERY_TIMEOUT);
    MOQ_TEST_CHECK_EQ_U64(st->reset_reliable, hd);
    MOQ_TEST_CHECK_EQ_INT((int)ss_objects_on(p, sidd), 0);
    MOQ_TEST_CHECK_EQ_INT((int)ss_objects_on(p, sidl), 1);
    return true;
}

/* The subscriber cancels an owner whose stream fills the publisher's
 * eight-slot ring behind another owner's object: with no slot for the reset,
 * the subgroup owes it (CANCELLED). The offer finds the owed reset without
 * scanning the ring, which holds data alone; the bridge drops the stream's
 * queued objects and resets it after its header, and the other owner's
 * object still goes out. */
static bool ss_owed_row(pair_t *p)
{
    regression_owner_t o, other;
    if (!regression_owner(p, false, "ss-owed", 128, 0, &o) ||
        !regression_owner(p, false, "ss-other", 128, 0, &other))
        return false;
    moq_subgroup_handle_t sg, sgo;
    uint64_t sid, sido;
    size_t h, ho;
    if (!regression_open_header(p, &o, 1, &sg, &sid, &h) ||
        !regression_open_header(p, &other, 1, &sgo, &sido, &ho))
        return false;
    p->mt->cap = 0;
    SS_REQUIRE(write_object(p, sgo, 2, 1, 0, 0) == MOQ_OK);
    uint64_t written = 0;
    while (written < 64 && write_object(p, sg, 1, 1, 0, written) == MOQ_OK)
        written++;
    SS_REQUIRE(written > 0 && written < 64);
    SS_REQUIRE(ss_queued(p->pub) == p->pub->action_cap);
    SS_REQUIRE(moq_session_unsubscribe(p->sub, o.track.csub, p->now) ==
               MOQ_OK);
    SS_REQUIRE(shuttle_sub_to_pub(p) > 0);
    SS_REQUIRE(ss_queued(p->pub) == p->pub->action_cap &&
               p->pub->action_nondata == 0);
#ifdef MOQ_SESSION_SWEEP_TESTING
    WALK_MARK(w);
    moq_reset_data_action_t r;
    memset(&r, 0, sizeof(r));
    MOQ_TEST_CHECK(bridge_session_offer_data_reset(p->pub, &r));
    WALK_CHECK(walk_since(w).reset_offer == 0);
    MOQ_TEST_CHECK_EQ_U64(r.error_code, 0x1);
    MOQ_TEST_CHECK_EQ_U64(r.reliable_size, h);
#endif
    SS_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
    CHECK_TABLE_COUNTS(p->pub_br);
    p->mt->cap = 1u << 20;
    serve_all(p);
    shuttle(p);
    drain_events(p->pub);
    drain_events(p->sub);
    const mt_stream_t *st = mt_stream(p->mt, sid);
    SS_REQUIRE(st != NULL);
    MOQ_TEST_CHECK(st->reset);
    MOQ_TEST_CHECK_EQ_U64(st->reset_code, 0x1);
    MOQ_TEST_CHECK_EQ_U64(st->reset_reliable, h);
    MOQ_TEST_CHECK_EQ_INT((int)ss_objects_on(p, sid), 0);
    MOQ_TEST_CHECK_EQ_INT((int)ss_objects_on(p, sido), 1);
    return true;
}

/* A four-slot ring carries two dozen objects, so its head and tail pass the
 * end of the ring again and again; another owner's PUBLISH_DONE queued among
 * them is a control action the summary counts, and so is the reset queued at
 * the end with room to spare. The summary matches its recount after every
 * step; the wire carries every object once and in order, then the reset, and
 * the PUBLISH_DONE goes out once. */
static bool ss_wrap_row(pair_t *p)
{
    regression_owner_t o, done;
    if (!regression_owner(p, false, "ss-wrap", 128, 0, &o) ||
        !regression_owner(p, false, "ss-wrap-done", 128, 0, &done))
        return false;
    moq_subgroup_handle_t sg;
    uint64_t sid;
    size_t header;
    if (!regression_open_header(p, &o, 1, &sg, &sid, &header)) return false;
    uint32_t want[24];
    unsigned n = 0;
    for (unsigned round = 0; round < 12; round++) {
        SS_REQUIRE(write_object(p, sg, 1, 1, 0, n) == MOQ_OK);
        want[n] = OBJ(1, 1, 0, n);
        n++;
        if (round == 5) {
            SS_REQUIRE(regression_finish(p, &done, 0, "ss-wrap-done") == MOQ_OK);
            SS_REQUIRE(p->pub->action_nondata > 0);
        }
        SS_REQUIRE(write_object(p, sg, 1, 1, 0, n) == MOQ_OK);
        want[n] = OBJ(1, 1, 0, n);
        n++;
        CHECK_SESSION_COUNTS(p->pub);
        serve_all(p);
        SS_REQUIRE(ss_queued(p->pub) == 0);
    }
    SS_REQUIRE(p->pub->action_head >= 4 * p->pub->action_cap);
    expect_wire(p, want, n, "objects through a wrapping ring", 1u << 20);
    size_t dones = 0;
    uint64_t seq = 0;
    SS_REQUIRE(regression_done(p, "ss-wrap-done", 0, &dones, &seq));
    MOQ_TEST_CHECK_EQ_INT((int)dones, 1);
    SS_REQUIRE(moq_session_reset_subgroup(p->pub, sg, 0x1, p->now) == MOQ_OK);
    CHECK_SESSION_COUNTS(p->pub);
    MOQ_TEST_CHECK(p->pub->action_nondata > 0);
    serve_all(p);
    const mt_stream_t *st = mt_stream(p->mt, sid);
    SS_REQUIRE(st != NULL);
    MOQ_TEST_CHECK(st->reset);
    MOQ_TEST_CHECK_EQ_U64(st->reset_code, 0x1);
    MOQ_TEST_CHECK_EQ_INT((int)ss_objects_on(p, sid), (int)n);
    return true;
}

/* The subscriber cancels one of two owners whose objects wait in the
 * publisher's session, the cancelled owner's timed. The publisher resets
 * that stream after its header and discards its queued objects; the other
 * owner's objects all go out, in order; nothing timed is left queued. */
static bool ss_cancel_row(pair_t *p)
{
    regression_owner_t gone, kept;
    if (!regression_owner(p, false, "ss-gone", 128, SS_TIMEOUT_MS, &gone) ||
        !regression_owner(p, false, "ss-kept", 128, 0, &kept))
        return false;
    moq_subgroup_handle_t sgg, sgk;
    uint64_t sidg, sidk;
    size_t hg, hk;
    if (!regression_open_header(p, &gone, 1, &sgg, &sidg, &hg) ||
        !regression_open_header(p, &kept, 1, &sgk, &sidk, &hk))
        return false;
    p->mt->cap = 0;
    for (uint64_t i = 0; i < 3; i++) {
        SS_REQUIRE(write_object(p, sgg, 1, 1, 0, i) == MOQ_OK);
        SS_REQUIRE(write_object(p, sgk, 2, 1, 0, i) == MOQ_OK);
    }
    SS_REQUIRE(p->pub->action_timed > 0);
    SS_REQUIRE(moq_session_unsubscribe(p->sub, gone.track.csub, p->now) ==
               MOQ_OK);
    shuttle(p);
    drain_events(p->pub);
    drain_events(p->sub);
    CHECK_TABLE_COUNTS(p->pub_br);
    p->mt->cap = 1u << 20;
    serve_all(p);
    const mt_stream_t *st = mt_stream(p->mt, sidg);
    SS_REQUIRE(st != NULL);
    MOQ_TEST_CHECK(st->reset);
    MOQ_TEST_CHECK_EQ_U64(st->reset_reliable, hg);
    MOQ_TEST_CHECK_EQ_INT((int)ss_objects_on(p, sidg), 0);
    const uint32_t want[3] = { OBJ(2, 1, 0, 0), OBJ(2, 1, 0, 1),
                               OBJ(2, 1, 0, 2) };
    expect_wire(p, want, 3, "the kept owner after a cancellation", 1u << 20);
    MOQ_TEST_CHECK_EQ_INT((int)p->pub->action_timed, 0);
    return true;
}

/* A reset the transport refuses stays its stream's and is tried in every
 * pass while the other owner's objects keep going out; once accepted the
 * stream ends after its header with nothing else. */
static bool ss_refused_row(pair_t *p)
{
    regression_owner_t o, other;
    if (!regression_owner(p, false, "ss-refused", 128, 0, &o) ||
        !regression_owner(p, false, "ss-flowing", 128, 0, &other))
        return false;
    moq_subgroup_handle_t sg, sgo;
    uint64_t sid, sido;
    size_t h, ho;
    if (!regression_open_header(p, &o, 1, &sg, &sid, &h) ||
        !regression_open_header(p, &other, 1, &sgo, &sido, &ho))
        return false;
    mt_stream_t *st = mt_stream(p->mt, sid);
    SS_REQUIRE(st != NULL);
    st->refuse_reset = true;
    SS_REQUIRE(moq_session_reset_subgroup(p->pub, sg, 0x1, p->now) == MOQ_OK);
    CHECK_SESSION_COUNTS(p->pub);
    SS_REQUIRE(p->pub->action_nondata > 0);
    const uint32_t before = st->reset_attempts;
    for (uint64_t i = 0; i < 3; i++) {
        SS_REQUIRE(write_object(p, sgo, 2, 1, 0, i) == MOQ_OK);
        SS_REQUIRE(moq_transport_bridge_service(p->pub_br, p->now) == MOQ_OK);
        CHECK_TABLE_COUNTS(p->pub_br);
        while (mt_serve_one(p->mt)) {}
    }
    MOQ_TEST_CHECK(st->reset_attempts - before >= 3);
    MOQ_TEST_CHECK(!st->reset);
    MOQ_TEST_CHECK_EQ_INT((int)ss_objects_on(p, sido), 3);
    st->refuse_reset = false;
    serve_all(p);
    MOQ_TEST_CHECK(st->reset);
    MOQ_TEST_CHECK_EQ_U64(st->reset_code, 0x1);
    MOQ_TEST_CHECK_EQ_U64(st->reset_reliable, h);
    MOQ_TEST_CHECK_EQ_INT((int)ss_objects_on(p, sid), 0);
    return true;
}

/* Subscriptions come and go in the publisher's pools, so later ones take
 * earlier ones' slots. In every round the deadline is the newest timed
 * object's, and the walk reads exactly the live owners. */
static bool ss_reuse_row(pair_t *p)
{
    for (unsigned round = 0; round < 3; round++) {
        char name[16];
        snprintf(name, sizeof(name), "ss-reuse-%u", round);
        regression_owner_t o;
        if (!regression_owner(p, false, name, 128, SS_TIMEOUT_MS, &o))
            return false;
        moq_subgroup_handle_t sg;
        uint64_t sid;
        size_t h;
        if (!regression_open_header(p, &o, 1, &sg, &sid, &h)) return false;
        const uint64_t t = p->now;
        SS_REQUIRE(write_object(p, sg, (uint8_t)(round + 1), 1, 0, 0) ==
                   MOQ_OK);
        MOQ_TEST_CHECK(ss_occupancy_ok(p->pub));
#ifdef MOQ_SESSION_SWEEP_TESTING
        const uint64_t live_sg = ss_live_subgroups(p->pub);
        const uint64_t live_sub = ss_live_subs(p->pub);
#endif
        WALK_MARK(w);
        MOQ_TEST_CHECK_EQ_U64(moq_session_next_deadline_us(p->pub),
                              t + SS_TIMEOUT_US);
        WALK_CHECK(walk_since(w).dl_sg == live_sg &&
                   walk_since(w).dl_sub == live_sub &&
                   walk_since(w).dl_pub == 0);
        serve_all(p);
        MOQ_TEST_CHECK_EQ_INT((int)ss_objects_on(p, sid), 1);
        SS_REQUIRE(moq_session_close_subgroup(p->pub, sg, p->now) == MOQ_OK);
        serve_all(p);
        SS_REQUIRE(moq_session_unsubscribe(p->sub, o.track.csub, p->now) ==
                   MOQ_OK);
        shuttle(p);
        drain_events(p->pub);
        drain_events(p->sub);
        MOQ_TEST_CHECK(ss_occupancy_ok(p->pub));
        p->now += 1000;
    }
    MOQ_TEST_CHECK_EQ_INT((int)ss_live_subs(p->pub), 0);
    WALK_MARK(e);
    MOQ_TEST_CHECK_EQ_U64(moq_session_next_deadline_us(p->pub), UINT64_MAX);
    WALK_CHECK(walk_since(e).dl_sub == 0);
    return true;
}

/* Polls every event the subscriber has queued. Returns how many are the
 * owner's terminal -- SUBSCRIBE_DONE for its subscription, PUBLISH_FINISHED
 * for its publication -- and adds to *right those that carry the publisher's
 * values: SUBSCRIPTION_ENDED, two streams and `reason`. */
static int ss_done_terminals(pair_t *p, const regression_owner_t *o,
                             const char *reason, int *right)
{
    int n = 0;
    moq_event_t ev;
    while (moq_session_poll_events(p->sub, &ev, 1) == 1) {
        uint64_t status = 0, streams = 0;
        moq_bytes_t text = { NULL, 0 };
        bool mine = false;
        if (o->publication && ev.kind == MOQ_EVENT_PUBLISH_FINISHED &&
            moq_publication_eq(ev.u.publish_finished.pub, o->peer_pub)) {
            mine = true;
            status = ev.u.publish_finished.status_code;
            streams = ev.u.publish_finished.stream_count;
            text = ev.u.publish_finished.reason;
        } else if (!o->publication && ev.kind == MOQ_EVENT_SUBSCRIBE_DONE &&
                   moq_subscription_eq(ev.u.subscribe_done.sub,
                                       o->track.csub)) {
            mine = true;
            status = ev.u.subscribe_done.status_code;
            streams = ev.u.subscribe_done.stream_count;
            text = ev.u.subscribe_done.reason;
        }
        if (mine) {
            n++;
            if (status == 0x3 && streams == 2 && text.len == strlen(reason) &&
                memcmp(text.data, reason, text.len) == 0)
                ++*right;
        }
        moq_event_cleanup(&ev);
    }
    return n;
}

/* PUBLISH_DONE, for a subscription or a publication, reaches the
 * subscriber while the two streams it counts have not: its terminal waits
 * for them, at most the 40 ms stream wait, and that wait is the subscriber's
 * deadline. No terminal is delivered on arrival or at deadline-minus-one, and
 * the deadline stands; at it exactly one is, the owner's, with the
 * publisher's status, stream count and reason, and nothing is due; later
 * there is no second. The walk reads the subscriber's one live owner. */
static bool ss_done_wait_row(pair_t *p, bool publication)
{
    regression_owner_t o;
    if (!regression_owner(p, publication, "ss-done-wait", 128, 0, &o))
        return false;
    moq_subgroup_handle_t sg[2];
    uint64_t sid[2];
    size_t h[2];
    for (unsigned i = 0; i < 2; i++)
        if (!regression_open_header(p, &o, i + 1, &sg[i], &sid[i], &h[i]))
            return false;
    for (unsigned i = 0; i < 2; i++) {
        SS_REQUIRE(write_object(p, sg[i], 1, (uint64_t)i + 1, 0, 0) == MOQ_OK);
        SS_REQUIRE(moq_session_close_subgroup(p->pub, sg[i], p->now) ==
                   MOQ_OK);
    }
    serve_all(p);
    SS_REQUIRE(regression_finish(p, &o, 2, "ss-done-wait") == MOQ_OK);
    serve_all(p);
    const uint64_t t1 = p->now;
    shuttle(p);
    SS_REQUIRE(ss_live_subs(p->sub) == (publication ? 0u : 1u) &&
               ss_live_pubs(p->sub) == (publication ? 1u : 0u));
    int right = 0;
    MOQ_TEST_CHECK_EQ_INT(ss_done_terminals(p, &o, "ss-done-wait", &right), 0);
    WALK_MARK(w);
    MOQ_TEST_CHECK_EQ_U64(moq_session_next_deadline_us(p->sub),
                          t1 + SS_DONE_WAIT_US);
    WALK_CHECK(walk_since(w).dl_sub == (publication ? 0u : 1u) &&
               walk_since(w).dl_pub == (publication ? 1u : 0u) &&
               walk_since(w).dl_sg == 0);
    p->now = t1 + SS_DONE_WAIT_US - 1;
    SS_REQUIRE(moq_session_tick(p->sub, p->now) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(ss_done_terminals(p, &o, "ss-done-wait", &right), 0);
    MOQ_TEST_CHECK_EQ_U64(moq_session_next_deadline_us(p->sub),
                          t1 + SS_DONE_WAIT_US);
    p->now = t1 + SS_DONE_WAIT_US;
    SS_REQUIRE(moq_session_tick(p->sub, p->now) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(ss_done_terminals(p, &o, "ss-done-wait", &right), 1);
    MOQ_TEST_CHECK_EQ_INT(right, 1);
    MOQ_TEST_CHECK_EQ_U64(moq_session_next_deadline_us(p->sub), UINT64_MAX);
    p->now = t1 + 2 * SS_DONE_WAIT_US;
    SS_REQUIRE(moq_session_tick(p->sub, p->now) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(ss_done_terminals(p, &o, "ss-done-wait", &right), 0);
    return true;
}

/* At the saturated clock, UINT64_MAX, an untimed queued object compares as
 * due, as it always has: the object becomes its stream's DELIVERY_TIMEOUT
 * reset, in place. The expiry check's shortcut for an untimed ring stops
 * short of that clock. */
static bool ss_saturated_row(pair_t *p)
{
    regression_owner_t o;
    if (!regression_owner(p, false, "ss-saturated", 128, 0, &o)) return false;
    moq_subgroup_handle_t sg;
    uint64_t sid;
    size_t header;
    if (!regression_open_header(p, &o, 1, &sg, &sid, &header)) return false;
    p->mt->cap = 0;
    SS_REQUIRE(write_object(p, sg, 1, 1, 0, 0) == MOQ_OK);
    SS_REQUIRE(ss_queued(p->pub) == 1 && p->pub->action_timed == 0);
    SS_REQUIRE(moq_session_tick(p->pub, UINT64_MAX) == MOQ_OK);
    CHECK_SESSION_COUNTS(p->pub);
    SS_REQUIRE(ss_queued(p->pub) == 1);
    const moq_action_t *a =
        &p->pub->actions[p->pub->action_head % p->pub->action_cap];
    MOQ_TEST_CHECK(a->kind == MOQ_ACTION_RESET_DATA);
    MOQ_TEST_CHECK_EQ_U64(a->u.reset_data.error_code,
                          MOQ_RESET_DELIVERY_TIMEOUT);
    MOQ_TEST_CHECK_EQ_U64(a->u.reset_data.reliable_size, header);
    MOQ_TEST_CHECK_EQ_INT((int)p->pub->action_nondata, 1);
    return true;
}

/* Closing a session empties its ring at once, timed objects and all, by the
 * application's close or its transport's: the summary empties with it (a
 * close the session queues counts afresh), and a closed session has no
 * deadline. */
static bool ss_close_row(pair_t *p, bool by_transport)
{
    regression_owner_t o;
    if (!regression_owner(p, false, "ss-close", 128, SS_TIMEOUT_MS, &o))
        return false;
    moq_subgroup_handle_t sg;
    uint64_t sid;
    size_t h;
    if (!regression_open_header(p, &o, 1, &sg, &sid, &h)) return false;
    p->mt->cap = 0;
    for (uint64_t i = 0; i < 3; i++)
        SS_REQUIRE(write_object(p, sg, 1, 1, 0, i) == MOQ_OK);
    SS_REQUIRE(p->pub->action_timed > 0);
    if (by_transport)
        SS_REQUIRE(moq_session_on_transport_close(p->pub, 0, p->now) == MOQ_OK);
    else
        SS_REQUIRE(moq_session_close(p->pub, 0, "ss-close", p->now) == MOQ_OK);
    SS_REQUIRE(moq_session_state(p->pub) == MOQ_SESS_CLOSED);
    CHECK_SESSION_COUNTS(p->pub);
    MOQ_TEST_CHECK_EQ_INT((int)p->pub->action_timed, 0);
    MOQ_TEST_CHECK_EQ_U64(moq_session_next_deadline_us(p->pub), UINT64_MAX);
    return true;
}

#undef SS_REQUIRE

static void test_session_scans(void)
{
    for (size_t v = 0; v < N_VERSIONS; ++v) {
        const moq_version_t ver = k_versions[v];
        pair_t p;
        ss_image_t img[2];
        bool completed[2] = { false, false };
        memset(img, 0, sizeof(img));
        const pools_t *pools[2] = { &k_ss_default, &k_ss_sparse };
        for (int k = 0; k < 2; k++) {
            if (ss_pair(&p, ver, pools[k], 1024, 1u << 20)) {
                completed[k] = ss_sparse_row(&p, &img[k]);
                MOQ_TEST_CHECK(completed[k]);
            } else MOQ_TEST_CHECK(false);
            pair_destroy(&p);
        }
        /* Only images whose rows ran to the end are compared or shown. */
        if (completed[0] && completed[1])
            MOQ_TEST_CHECK(memcmp(&img[0], &img[1], sizeof(img[0])) == 0);
#ifdef MOQ_SESSION_SWEEP_TESTING
        for (int k = 0; k < 2; k++)
            if (completed[k])
                printf("session scans d%d, %s pools: the publisher's deadline "
                       "query read %llu subgroup, %llu subscription and %llu "
                       "publication slots and %llu queued actions\n",
                       draft_of(ver), k ? "sparse" : "default",
                       (unsigned long long)img[k].pub_walk.dl_sg,
                       (unsigned long long)img[k].pub_walk.dl_sub,
                       (unsigned long long)img[k].pub_walk.dl_pub,
                       (unsigned long long)img[k].pub_walk.queued_dl);
#endif
        /* Rows over a bridge whose budget admits one object, so the rest
         * wait in the session's ring. */
        static bool (*const held[])(pair_t *) = {
            ss_untimed_row, ss_expiry_row, ss_earlier_row, ss_offer_row,
            ss_cancel_row,
        };
        for (size_t r = 0; r < sizeof(held) / sizeof(held[0]); ++r) {
            for (int k = 0; k < 2; k++) {
                if (ss_pair(&p, ver, pools[k], 1024, 1))
                    MOQ_TEST_CHECK(held[r](&p));
                else MOQ_TEST_CHECK(false);
                pair_destroy(&p);
            }
        }
        if (ss_pair(&p, ver, &k_ss_sparse, 8, 1))
            MOQ_TEST_CHECK(ss_owed_row(&p));
        else MOQ_TEST_CHECK(false);
        pair_destroy(&p);
        if (ss_pair(&p, ver, &k_ss_sparse, 4, 1u << 20))
            MOQ_TEST_CHECK(ss_wrap_row(&p));
        else MOQ_TEST_CHECK(false);
        pair_destroy(&p);
        static bool (*const open[])(pair_t *) = {
            ss_refused_row, ss_reuse_row, ss_saturated_row,
        };
        for (size_t r = 0; r < sizeof(open) / sizeof(open[0]); ++r) {
            if (ss_pair(&p, ver, &k_ss_sparse, 1024, 1u << 20))
                MOQ_TEST_CHECK(open[r](&p));
            else MOQ_TEST_CHECK(false);
            pair_destroy(&p);
        }
        for (int publication = 0; publication < 2; publication++) {
            if (ss_pair(&p, ver, &k_ss_sparse, 1024, 1u << 20))
                MOQ_TEST_CHECK(ss_done_wait_row(&p, publication != 0));
            else MOQ_TEST_CHECK(false);
            pair_destroy(&p);
        }
        for (int by_transport = 0; by_transport < 2; by_transport++) {
            if (ss_pair(&p, ver, &k_ss_sparse, 1024, 1))
                MOQ_TEST_CHECK(ss_close_row(&p, by_transport != 0));
            else MOQ_TEST_CHECK(false);
            pair_destroy(&p);
        }
        printf("session scans d%d: checked\n", draft_of(ver));
    }
    MOQ_TEST_PASS("the deadline query walks live owners and reads queued "
                  "actions only while one is timed, expiry and the reset "
                  "offer skip a queue that cannot hold what they look for, "
                  "and each row's deadline, wire and queue-summary checks "
                  "hold through expiry, a deadline moving earlier and later, "
                  "cancellation, refusal, a full and a wrapping ring, slot "
                  "reuse, close and the saturated clock");
}

int main(int argc, char **argv)
{
    if (argc == 2 && strcmp(argv[1], "--shared-admission") == 0) {
        test_shared_admission_refusals();
        test_shared_admission_fin();
        test_shared_admission_reset();
        test_shared_admission_expiry();
        return failures ? 1 : 0;
    }
    if (argc == 2 && strcmp(argv[1], "--pass-costs") == 0) {
        test_pass_costs();
        return failures ? 1 : 0;
    }
    if (argc == 2 && strcmp(argv[1], "--scan-work") == 0) {
        test_scan_work();
        return failures ? 1 : 0;
    }
    if (argc == 2 && strcmp(argv[1], "--expiry-walk") == 0) {
        test_expiry_walk();
        return failures ? 1 : 0;
    }
    if (argc == 2 && strcmp(argv[1], "--session-scans") == 0) {
        test_session_scans();
        return failures ? 1 : 0;
    }
    if (argc == 2 && strcmp(argv[1], "--shared-all") == 0) {
        /* The default rows with the capability advertised. Left out, because
         * they refuse per stream: refused_stream_parks_alone,
         * fetch_priority_payload_retry, normal_fin_before_done,
         * retained_expiry, expiry_successors, expiry_review,
         * retained_cancellation, and fill_order's detached-retry rows. */
        g_shared_all = true;
        test_group_order_wire();
        test_priorities_across_tracks();
        test_rules_within_track();
        test_group_id_bounds();
        test_ladder_and_default();
        test_equal_priorities_share();
        test_flow_blocked_stream();
        test_data_before_close();
        test_live_subgroup_catches_up();
        test_completeness();
        test_priority_updates();
        test_publication_update();
        test_fetch_scheduling();
        test_fetch_update();
        test_fetch_priority_boundary();
        test_fill_order();
        test_control_not_behind_data();
        test_stop_sending_one_stream();
        test_unsubscribe_resets_pending();
        test_reset_before_open();
        test_keys_need_four_slots();
        test_map_full_is_not_outbound_work();
        test_key_moves_with_its_stream();
        test_fetch_cancel_resets();
        test_done_with_pending();
        MOQ_TEST_CHECK(g_shared_cap_refusals > 0);
        printf("shared-all: %u data writes refused at the cap while advertised\n",
               g_shared_cap_refusals);
        if (failures) {
            fprintf(stderr, "test_scheduling --shared-all: %d failure(s)\n",
                    failures);
            return 1;
        }
        printf("test_scheduling --shared-all: all passed\n");
        return 0;
    }
    if (argc == 2 && strcmp(argv[1], "--fill-order") == 0) {
        test_fill_order();
        return failures ? 1 : 0;
    }
    if (argc == 2 && strcmp(argv[1], "--fetch-priority") == 0) {
        test_fetch_scheduling();
        test_fetch_update();
        test_fetch_priority_boundary();
        test_fetch_priority_payload_retry();
        return failures ? 1 : 0;
    }
    if (argc == 2 && strcmp(argv[1], "--retention-cancel") == 0) {
        test_retained_cancellation();
        return failures ? 1 : 0;
    }
    if (argc == 2 && strcmp(argv[1], "--fin-done") == 0) {
        test_normal_fin_before_done();
        return failures ? 1 : 0;
    }
    if (argc == 2 && strcmp(argv[1], "--retained-expiry") == 0) {
        test_retained_expiry();
        return failures ? 1 : 0;
    }
    if (argc == 2 && strcmp(argv[1], "--expiry-successors") == 0) {
        test_expiry_successors();
        return failures ? 1 : 0;
    }
    if (argc == 2 && strcmp(argv[1], "--expiry-r1") == 0) {
        test_expiry_review(1);
        return failures ? 1 : 0;
    }
    if (argc == 2 && strcmp(argv[1], "--expiry-r2") == 0) {
        test_expiry_review(2);
        return failures ? 1 : 0;
    }
    if (argc == 2 && strcmp(argv[1], "--expiry-r3") == 0) {
        test_expiry_review(3);
        return failures ? 1 : 0;
    }
    if (argc != 1) return 2;
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
    test_fetch_priority_boundary();
    test_fetch_priority_payload_retry();
    test_fill_order();
    test_control_not_behind_data();
    test_stop_sending_one_stream();
    test_unsubscribe_resets_pending();
    test_reset_before_open();
    test_keys_need_four_slots();
    test_map_full_is_not_outbound_work();
    test_key_moves_with_its_stream();
    test_fetch_cancel_resets();
    test_done_with_pending();
    test_normal_fin_before_done();
    test_retained_expiry();
    test_expiry_successors();
    test_expiry_review(1);
    test_expiry_review(2);
    test_expiry_review(3);
    test_retained_cancellation();
    if (failures) {
        fprintf(stderr, "test_scheduling: %d failure(s)\n", failures);
        return 1;
    }
    printf("test_scheduling: all passed\n");
    return 0;
}
