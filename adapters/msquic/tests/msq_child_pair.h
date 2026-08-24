#ifndef MSQ_CHILD_PAIR_H
#define MSQ_CHILD_PAIR_H

/*
 * A managed CHILD driven through a real MoQ flow, with no transport.
 *
 * The facade comes from the lanes-only testing constructor and its single
 * child from the production child path, so everything the child does runs
 * through the code a real accept would build. The peer is a raw session +
 * bridge over a fake endpoint, and wire bytes are relayed between the two
 * fakes BY THE MAIN THREAD while the child's lane is parked at its pre-wait
 * barrier -- so no doorbell is running concurrently and every
 * moq_session_* call on the child still happens inside its on_lane_pump.
 *
 * Callers supply their own pump: what the child does with its events is the
 * thing under test, not part of the rig.
 *
 * Header-only and test-only. Include exactly once per test binary.
 */

#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#include "msquic_internal.h"

#include "support/fake_msq_table.h"
#include "support/fake_endpoint.h"

#include <moq/msquic_managed.h>
#include <moq/rcbuf.h>
#include <moq/session.h>
#include <moq/transport_bridge.h>

/* MOQ_MSQUIC_TESTING-only seams. */
extern void (*moq_msq_test_prewait)(moq_msquic_managed_lane_t *lane);
extern moq_result_t moq_msq_test_managed_create_lanes_only_api(
    const moq_msquic_managed_cfg_t *cfg, const QUIC_API_TABLE *api,
    moq_msquic_managed_t **out);
extern moq_result_t moq_msq_test_lane_inject_live_child(
    moq_msquic_managed_lane_t *lane, HQUIC connection,
    moq_msquic_managed_conn_t **out);
extern moq_msquic_conn_t *moq_msq_test_managed_conn_adapter(
    moq_msquic_managed_conn_t *conn);

enum { MSQ_GUARD_US = 5 * 1000 * 1000 };

/* The deterministic object payload, shared by the peer publisher and every
 * consumer that verifies it. */
static void payload_fill(uint8_t *dst, size_t len)
{
    for (size_t i = 0; i < len; i++)
        dst[i] = (uint8_t)(i * 131u + 47u);
}

/* Every bounded wait here is a fail-closed hang guard, never a verdict. */
static void guard_deadline(struct timespec *abs)
{
    clock_gettime(CLOCK_REALTIME, abs);
    abs->tv_sec += MSQ_GUARD_US / 1000000;
}

/* --- pre-wait barrier: the main thread relays only while parked -------- */

static struct {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    bool enabled;
    uint64_t entered;
    uint64_t released;
    bool expired;
} g_gate = {
    .mu = PTHREAD_MUTEX_INITIALIZER,
    .cv = PTHREAD_COND_INITIALIZER,
};

static void gate_prepare(void)
{
    pthread_mutex_lock(&g_gate.mu);
    g_gate.enabled = true;
    g_gate.entered = 0;
    g_gate.released = 0;
    g_gate.expired = false;
    pthread_mutex_unlock(&g_gate.mu);
}

static void prewait_hook(moq_msquic_managed_lane_t *lane)
{
    struct timespec abs;

    (void)lane;
    guard_deadline(&abs);
    pthread_mutex_lock(&g_gate.mu);
    if (!g_gate.enabled) {
        pthread_mutex_unlock(&g_gate.mu);
        return;
    }
    uint64_t mine = ++g_gate.entered;
    pthread_cond_broadcast(&g_gate.cv);
    while (g_gate.enabled && g_gate.released < mine)
        if (pthread_cond_timedwait(&g_gate.cv, &g_gate.mu, &abs) != 0) {
            g_gate.expired = true;
            g_gate.enabled = false;
            pthread_cond_broadcast(&g_gate.cv);
            break;
        }
    pthread_mutex_unlock(&g_gate.mu);
}

static bool gate_await(uint64_t want)
{
    struct timespec abs;
    bool ok;

    guard_deadline(&abs);
    pthread_mutex_lock(&g_gate.mu);
    while (g_gate.entered < want && !g_gate.expired)
        if (pthread_cond_timedwait(&g_gate.cv, &g_gate.mu, &abs) != 0)
            break;
    ok = g_gate.entered >= want && !g_gate.expired;
    pthread_mutex_unlock(&g_gate.mu);
    return ok;
}

static void gate_release(uint64_t entry)
{
    pthread_mutex_lock(&g_gate.mu);
    if (g_gate.released < entry)
        g_gate.released = entry;
    pthread_cond_broadcast(&g_gate.cv);
    pthread_mutex_unlock(&g_gate.mu);
}

static void gate_disable(void)
{
    pthread_mutex_lock(&g_gate.mu);
    g_gate.enabled = false;
    g_gate.released = UINT64_MAX;
    pthread_cond_broadcast(&g_gate.cv);
    pthread_mutex_unlock(&g_gate.mu);
}

/* --- the pair ---------------------------------------------------------- */

static fake_msq_t g_table_fake;
static fake_msq_t g_conn_fake;

struct pair {
    moq_msquic_managed_t *m;
    moq_msquic_managed_lane_t *lane;
    moq_msquic_managed_conn_t *mc;
    moq_msquic_conn_t *adapter;

    moq_session_t *peer;               /* raw SERVER publisher */
    moq_transport_bridge_t *peer_bridge;
    fake_endpoint_t peer_ep;

    fake_msq_stream_t *ctrl;           /* the child's control bidi */
    uint64_t peer_uni_id;
    fake_msq_stream_t *peer_uni;       /* the peer's subgroup uni */
    size_t child_send_cur;
    size_t peer_op_cur;
    uint64_t gen;                      /* barrier entries released so far */
};

static fake_msq_stream_t *deliver_peer_stream(struct pair *p, uint64_t id,
                                              bool uni)
{
    fake_msq_stream_t *st = fake_msq_peer_stream(&g_conn_fake, id, uni);
    QUIC_CONNECTION_EVENT ev;

    if (st == NULL)
        return NULL;
    memset(&ev, 0, sizeof(ev));
    ev.Type = QUIC_CONNECTION_EVENT_PEER_STREAM_STARTED;
    ev.PEER_STREAM_STARTED.Stream = (HQUIC)st;
    ev.PEER_STREAM_STARTED.Flags =
        uni ? QUIC_STREAM_OPEN_FLAG_UNIDIRECTIONAL
            : QUIC_STREAM_OPEN_FLAG_NONE;
    (void)moq_msquic_conn_callback()(fake_msq_conn_handle(&g_conn_fake),
                                     p->adapter, &ev);
    return st;
}

static void deliver_conn_simple(struct pair *p,
                                QUIC_CONNECTION_EVENT_TYPE type)
{
    QUIC_CONNECTION_EVENT ev;

    memset(&ev, 0, sizeof(ev));
    ev.Type = type;
    if (type == QUIC_CONNECTION_EVENT_STREAMS_AVAILABLE) {
        ev.STREAMS_AVAILABLE.BidirectionalCount = 8;
        ev.STREAMS_AVAILABLE.UnidirectionalCount = 8;
    }
    (void)moq_msquic_conn_callback()(fake_msq_conn_handle(&g_conn_fake),
                                     p->adapter, &ev);
}

/*
 * One relay step, run on the main thread with the lane parked: move the
 * child's new outbound bytes to the peer, service the peer, and move the
 * peer's new output back to the child.
 */
static void relay(struct pair *p)
{
    for (; p->child_send_cur < (size_t)g_conn_fake.send_count;
         p->child_send_cur++) {
        fake_msq_send_t *s = &g_conn_fake.sends[p->child_send_cur];
        bool fin = (s->flags & QUIC_SEND_FLAG_FIN) != 0;

        (void)moq_transport_bridge_on_peer_control_bytes(
            p->peer_bridge, s->stream->id, s->bytes, s->bytes_len, fin, 0);
    }
    while (fake_msq_deliver_send_complete(&g_conn_fake, false))
        ;
    (void)moq_transport_bridge_service(p->peer_bridge, 0);

    for (; p->peer_op_cur < p->peer_ep.count; p->peer_op_cur++) {
        fake_op_t *o = &p->peer_ep.ops[p->peer_op_cur];

        if (o->kind == FAKE_OP_OPEN_UNI) {
            p->peer_uni_id = o->stream_id;
            p->peer_uni = deliver_peer_stream(p, o->stream_id, true);
        } else if (o->kind == FAKE_OP_WRITE) {
            if (p->ctrl != NULL && o->stream_id == p->ctrl->id)
                fake_msq_deliver_receive(p->ctrl, o->data, o->data_len,
                                         o->fin);
            else if (p->peer_uni != NULL &&
                     o->stream_id == p->peer_uni_id)
                fake_msq_deliver_receive(p->peer_uni, o->data, o->data_len,
                                         o->fin);
        }
    }
    fake_endpoint_clear_ops(&p->peer_ep);
    p->peer_op_cur = 0;
}

/* Relay, then let the child's lane take exactly one more generation. */
static bool step(struct pair *p)
{
    relay(p);
    gate_release(++p->gen);
    return gate_await(p->gen + 1);
}

static bool pair_up(struct pair *p, bool streaming_objects,
                    size_t cfg_size, moq_msquic_lane_pump_fn pump)
{
    moq_msquic_managed_cfg_t cfg;

    memset(p, 0, sizeof(*p));
    gate_prepare();
    fake_msq_init(&g_table_fake, true);
    fake_msq_init(&g_conn_fake, true);

    moq_msquic_managed_cfg_init_sized(&cfg, cfg_size);
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = "127.0.0.1";
    cfg.port = 4433;
    cfg.on_lane_pump = pump;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 16;
    /* Written whether or not struct_size covers it: when it does not, a
     * correct derivation must ignore it. */
    cfg.streaming_objects = streaming_objects;
    if (moq_msq_test_managed_create_lanes_only_api(
            &cfg, fake_msq_table(&g_table_fake), &p->m) != MOQ_OK)
        return false;
    p->lane = moq_msquic_managed_lane(p->m, 0);
    if (p->lane == NULL || !gate_await(1))
        return false;
    if (moq_msq_test_lane_inject_live_child(
            p->lane, fake_msq_conn_handle(&g_conn_fake), &p->mc) != MOQ_OK)
        return false;
    p->adapter = moq_msq_test_managed_conn_adapter(p->mc);
    if (p->adapter == NULL)
        return false;

    /* the raw peer: a SERVER session over a fake endpoint */
    moq_session_cfg_t scfg;

    moq_session_cfg_init_sized(&scfg, sizeof(scfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_SERVER);
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 16;
    if (moq_session_create(&scfg, 0, &p->peer) < 0)
        return false;
    fake_endpoint_init(&p->peer_ep, 3, 1);

    moq_transport_bridge_cfg_t bcfg;

    moq_transport_bridge_cfg_init(&bcfg, moq_alloc_default());
    if (moq_transport_bridge_create(&bcfg, p->peer, &p->peer_ep.vtable,
                                    &p->peer_ep, &p->peer_bridge) != MOQ_OK)
        return false;

    /* the child's transport comes up: it opens its control bidi and sends
     * SETUP on the next generation */
    deliver_conn_simple(p, QUIC_CONNECTION_EVENT_STREAMS_AVAILABLE);
    deliver_conn_simple(p, QUIC_CONNECTION_EVENT_CONNECTED);
    p->ctrl = fake_msq_stream_at(&g_conn_fake, 0);
    if (p->ctrl == NULL)
        return false;
    fake_msq_deliver_start_complete(p->ctrl, QUIC_STATUS_SUCCESS);
    return true;
}

static void pair_down(struct pair *p)
{
    gate_disable();
    if (p->adapter != NULL) {
        QUIC_CONNECTION_EVENT ev;

        memset(&ev, 0, sizeof(ev));
        ev.Type = QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE;
        (void)moq_msquic_conn_callback()(fake_msq_conn_handle(&g_conn_fake),
                                         p->adapter, &ev);
    }
    if (p->m != NULL) {
        (void)moq_msquic_managed_stop(p->m);
        moq_msquic_managed_destroy(p->m);
    }
    if (p->peer_bridge != NULL)
        moq_transport_bridge_destroy(p->peer_bridge);
    if (p->peer != NULL)
        moq_session_destroy(p->peer);
}


#endif /* MSQ_CHILD_PAIR_H */
