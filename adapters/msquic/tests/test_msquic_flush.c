/* Current core/bridge/managed executor over a fake provider, no sockets.
 * The production transport guard is also the fixture's exclusive executor for
 * deliberately retaining accepted output before a bridge service pass. No
 * session, bridge, adapter or facade state is fabricated for these checks. */
#if defined(__linux__) && !defined(_POSIX_C_SOURCE)
#define _POSIX_C_SOURCE 200809L
#endif
#include "msquic_internal.h"
#include "support/fake_msq_managed.h"
#include "support/msq_test_seams.h"
#include "../../../core/src/session/session_outbound_internal.h"
#include "../../../core/src/session/request_stream_internal.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static _Atomic unsigned failures;
static unsigned cases;
#define CHECK(x) do { if (!(x)) { \
    fprintf(stderr, "FAIL %s:%d: %s\n", __func__, __LINE__, #x); \
    ++failures; } } while (0)
#define REQUIRE(x) do { if (!(x)) { \
    fprintf(stderr, "PREREQUISITE %s:%d: %s\n", __func__, __LINE__, #x); \
    exit(2); } } while (0)

struct memory { size_t calls; ptrdiff_t balance; };
static void *count_alloc(size_t n, void *ctx)
{
    struct memory *m = ctx;
    ++m->calls;
    void *p = malloc(n);
    if (p) ++m->balance;
    return p;
}
static void *count_realloc(void *p, size_t old, size_t n, void *ctx)
{
    (void)old;
    struct memory *m = ctx;
    ++m->calls;
    return realloc(p, n);
}
static void count_free(void *p, size_t n, void *ctx)
{
    (void)n;
    struct memory *m = ctx;
    if (p) --m->balance;
    free(p);
}

struct gate {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    bool entered, release;
};

static void gate_init(struct gate *g)
{
    memset(g, 0, sizeof(*g));
    REQUIRE(pthread_mutex_init(&g->mu, NULL) == 0);
    REQUIRE(pthread_cond_init(&g->cv, NULL) == 0);
}

static void gate_wait(struct gate *g, bool entered)
{
    struct timespec deadline;
    REQUIRE(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
    deadline.tv_sec += 5;
    pthread_mutex_lock(&g->mu);
    while (!(entered ? g->entered : g->release))
        REQUIRE(pthread_cond_timedwait(&g->cv, &g->mu, &deadline) == 0);
    pthread_mutex_unlock(&g->mu);
}

static void gate_set(struct gate *g, bool entered)
{
    pthread_mutex_lock(&g->mu);
    if (entered) g->entered = true;
    else g->release = true;
    pthread_cond_broadcast(&g->cv);
    pthread_mutex_unlock(&g->mu);
}

static void gate_destroy(struct gate *g)
{
    REQUIRE(pthread_cond_destroy(&g->cv) == 0);
    REQUIRE(pthread_mutex_destroy(&g->mu) == 0);
}

struct rig {
    fake_mgd_t provider;
    fake_mgd_conn_t *client;
    moq_msquic_managed_t *m;
    moq_msquic_managed_lane_t *lane;
    moq_msquic_conn_t *adapter;
    struct memory memory;
    moq_alloc_t alloc;
    bool callback_checks;
    unsigned pumps, activities;
    struct gate *pump_gate;
};

static uint64_t clock_us = 1000000;
static uint64_t test_now(void) { return clock_us; }

static int on_pump(moq_msquic_managed_t *m, moq_msquic_managed_lane_t *lane,
                   uint64_t now, void *user)
{
    (void)lane; (void)now;
    struct rig *r = user;
    ++r->pumps;
    if (r->callback_checks)
        CHECK(moq_msquic_managed_poll_flush(m) == MOQ_ERR_WRONG_STATE);
    if (r->pump_gate) {
        gate_set(r->pump_gate, true);
        gate_wait(r->pump_gate, false);
    }
    return 0;
}

static void on_activity(moq_msquic_managed_t *m, void *user)
{
    struct rig *r = user;
    ++r->activities;
    if (r->callback_checks)
        CHECK(moq_msquic_managed_poll_flush(m) == MOQ_ERR_WRONG_STATE);
}

static struct rig *rig_create_limits(moq_version_t version, bool server,
                                     uint32_t max_events, uint32_t max_actions)
{
    struct rig *r = calloc(1, sizeof(*r));
    REQUIRE(r != NULL);
    fake_mgd_init(&r->provider);
    r->alloc = (moq_alloc_t){ .ctx = &r->memory, .alloc = count_alloc,
        .realloc = count_realloc, .free = count_free };
    moq_msquic_managed_cfg_t cfg;
    moq_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = &r->alloc;
    cfg.version = version;
    cfg.perspective = server ? MOQ_PERSPECTIVE_SERVER : MOQ_PERSPECTIVE_CLIENT;
    cfg.host = server ? NULL : "test.invalid";
    cfg.port = 443;
    cfg.max_events = max_events;
    cfg.max_actions = max_actions;
    cfg.session_idle_timeout_us = 10000000;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 64;
    cfg.cert_path = "fake-cert";
    cfg.key_path = "fake-key";
    cfg.on_lane_pump = on_pump;
    cfg.on_lane_pump_user = r;
    cfg.on_activity = on_activity;
    cfg.on_activity_ctx = r;
    moq_msq_test_api_override = fake_mgd_table(&r->provider);
    moq_msq_test_no_doorbell = true;
    REQUIRE(moq_msquic_managed_create(&cfg, &r->m) == MOQ_OK);
    r->lane = moq_msquic_managed_lane(r->m, 0);
    REQUIRE(r->lane != NULL);
    if (!server) {
        REQUIRE(r->provider.conn_count == 1);
        r->client = &r->provider.conns[0];
        r->adapter = r->client->ctx;
        REQUIRE(r->client->configured && r->adapter != NULL);
        r->callback_checks = true;
    }
    return r;
}

static struct rig *rig_create_capacity(moq_version_t version, bool server,
                                       uint32_t max_events)
{
    return rig_create_limits(version, server, max_events, 0);
}

static struct rig *rig_create(moq_version_t version, bool server)
{
    return rig_create_capacity(version, server, 0);
}

static void connection_event(struct rig *r, QUIC_CONNECTION_EVENT *ev)
{
    REQUIRE(r->client->cb((HQUIC)&r->client->fake, r->client->ctx, ev) == QUIC_STATUS_SUCCESS);
}

static void credit(struct rig *r, uint16_t n)
{
    QUIC_CONNECTION_EVENT ev = { .Type = QUIC_CONNECTION_EVENT_STREAMS_AVAILABLE };
    ev.STREAMS_AVAILABLE.BidirectionalCount = n;
    ev.STREAMS_AVAILABLE.UnidirectionalCount = n;
    connection_event(r, &ev);
}

static void stream_event(fake_msq_stream_t *st, QUIC_STREAM_EVENT_TYPE type, bool graceful)
{
    QUIC_STREAM_EVENT ev = { .Type = type };
    if (type == QUIC_STREAM_EVENT_SEND_SHUTDOWN_COMPLETE)
        ev.SEND_SHUTDOWN_COMPLETE.Graceful = graceful ? TRUE : FALSE;
    REQUIRE(st->cb((HQUIC)st, st->ctx, &ev) == QUIC_STATUS_SUCCESS);
}

static void complete_sends(struct rig *r)
{
    unsigned count = 0;
    while (fake_msq_deliver_send_complete(&r->client->fake, false))
        REQUIRE(++count < FAKE_MSQ_MAX_SENDS);
}

static void rig_destroy(struct rig *r)
{
    if (r->client && r->client->in_use) {
        fake_mgd_deliver_peer_close(r->client, 0);
        while (fake_msq_deliver_send_complete(&r->client->fake, true)) {}
        void *ctx;
        while ((ctx = fake_msq_next_dgram_ctx(&r->client->fake)) != NULL) {
            QUIC_CONNECTION_EVENT ev = { .Type = QUIC_CONNECTION_EVENT_DATAGRAM_SEND_STATE_CHANGED };
            ev.DATAGRAM_SEND_STATE_CHANGED.State = QUIC_DATAGRAM_SEND_CANCELED;
            ev.DATAGRAM_SEND_STATE_CHANGED.ClientContext = ctx;
            connection_event(r, &ev);
        }
        fake_mgd_deliver_shutdown_complete(r->client);
    }
    moq_msquic_managed_destroy(r->m);
    CHECK(r->memory.balance == 0);
    free(r);
    moq_msq_test_api_override = NULL;
}

static void executor_enter(struct rig *r)
{
    r->adapter->guard_enter(r->adapter->guard_user);
}

static void executor_leave(struct rig *r)
{
    r->adapter->guard_leave(r->adapter->guard_user);
}

static void check_query(struct rig *r, moq_result_t expected)
{
    size_t allocs = r->memory.calls;
    uint64_t epoch = r->adapter->session->borrow_epoch;
    int sends = r->client->fake.send_count;
    unsigned pumps = r->pumps;
    for (unsigned i = 0; i < 3; ++i)
        CHECK(moq_msquic_managed_poll_flush(r->m) == expected);
    CHECK(r->memory.calls == allocs);
    CHECK(r->adapter->session->borrow_epoch == epoch);
    CHECK(r->client->fake.send_count == sends && r->pumps == pumps);
}

static const char *alpn(moq_version_t v)
{
    return v == MOQ_VERSION_DRAFT_16 ? "moqt-16" :
           v == MOQ_VERSION_DRAFT_18 ? "moqt-18" : "moqt-22";
}

struct peer {
    fake_msq_t fake;
    moq_session_t *session;
    moq_msquic_conn_t *adapter;
    moq_connection_target_t target;
    int client_relayed, server_relayed;
};

static fake_msq_stream_t *relay_stream(fake_msq_t *fake, moq_msquic_conn_t *adapter,
                                      uint64_t id, bool uni)
{
    for (int i = 0; i < fake->stream_count; ++i)
        if (fake->streams[i].id == id) return &fake->streams[i];
    fake_msq_stream_t *st = fake_msq_peer_stream(fake, id, uni);
    REQUIRE(st != NULL);
    QUIC_CONNECTION_EVENT ev = { .Type = QUIC_CONNECTION_EVENT_PEER_STREAM_STARTED };
    ev.PEER_STREAM_STARTED.Stream = (HQUIC)st;
    ev.PEER_STREAM_STARTED.Flags = uni ? QUIC_STREAM_OPEN_FLAG_UNIDIRECTIONAL : QUIC_STREAM_OPEN_FLAG_NONE;
    REQUIRE(moq_msquic_conn_callback()((HQUIC)fake, adapter, &ev) == QUIC_STATUS_SUCCESS);
    return st;
}

static void relay_sends(fake_msq_t *from, int *cursor,
                        fake_msq_t *to, moq_msquic_conn_t *adapter)
{
    while (*cursor < from->send_count) {
        fake_msq_send_t *send = &from->sends[(*cursor)++];
        REQUIRE(send->bytes_len == send->total);
        fake_msq_stream_t *st = relay_stream(to, adapter, send->stream->id, send->stream->uni);
        fake_msq_deliver_receive(st, send->bytes, send->bytes_len,
                                (send->flags & QUIC_SEND_FLAG_FIN) != 0);
        REQUIRE(fake_msq_deliver_send_complete(from, false));
        if (send->flags & QUIC_SEND_FLAG_FIN)
            stream_event(send->stream, QUIC_STREAM_EVENT_SEND_SHUTDOWN_COMPLETE, true);
    }
}

static void exchange(struct rig *r, struct peer *p)
{
    for (unsigned n = 0; n < 32; ++n) {
        executor_enter(r);
        moq_msquic_conn_service(r->adapter);
        executor_leave(r);
        moq_msquic_conn_service(p->adapter);
        relay_sends(&r->client->fake, &p->client_relayed, &p->fake, p->adapter);
        relay_sends(&p->fake, &p->server_relayed, &r->client->fake, r->adapter);
        if (p->client_relayed == r->client->fake.send_count &&
            p->server_relayed == p->fake.send_count) return;
    }
    REQUIRE(false);
}

static struct peer *establish(struct rig *r, moq_version_t version)
{
    struct peer *p = calloc(1, sizeof(*p));
    REQUIRE(p != NULL);
    fake_msq_init(&p->fake, false);
    moq_session_cfg_t sc;
    moq_session_cfg_init_sized(&sc, sizeof(sc), moq_alloc_default(), MOQ_PERSPECTIVE_SERVER);
    sc.version = version;
    sc.send_request_capacity = true;
    sc.initial_request_capacity = 64;
    REQUIRE(moq_session_create(&sc, test_now(), &p->session) == MOQ_OK);
    p->target.struct_size = sizeof(p->target);
    p->target.target_present = MOQ_BINDING_TARGET_COMPLETE;
    p->target.authority = MOQ_BYTES_LITERAL("test.invalid:443");
    moq_msquic_conn_cfg_t ac;
    moq_msquic_conn_cfg_init_sized(&ac, sizeof(ac));
    ac.alloc = moq_alloc_default();
    ac.session = p->session;
    ac.api = fake_msq_table(&p->fake);
    ac.target = &p->target;
    REQUIRE(moq_msquic_conn_create(&ac, &p->adapter) == MOQ_OK);
    REQUIRE(moq_msquic_conn_bind(p->adapter, (HQUIC)&p->fake) == MOQ_OK);
    QUIC_CONNECTION_EVENT ev = { .Type = QUIC_CONNECTION_EVENT_CONNECTED };
    REQUIRE(moq_msquic_conn_callback()((HQUIC)&p->fake, p->adapter, &ev) == QUIC_STATUS_SUCCESS);
    fake_mgd_deliver_connected(r->client, alpn(version));
    exchange(r, p);
    REQUIRE(moq_session_state(p->session) == MOQ_SESS_ESTABLISHED);
    REQUIRE(moq_session_state(r->adapter->session) == MOQ_SESS_ESTABLISHED);
    return p;
}

static void peer_destroy(struct peer *p)
{
    while (fake_msq_deliver_send_complete(&p->fake, true)) {}
    moq_msquic_conn_destroy(p->adapter);
    moq_session_destroy(p->session);
    free(p);
}

static void t_core_and_receive(moq_version_t version)
{
    struct rig *r = rig_create(version, false);
    struct peer *p = establish(r, version);
    CHECK(r->adapter->session->event_head != r->adapter->session->event_tail);
    check_query(r, MOQ_OK); /* established, live, with unrelated receive events */
    executor_enter(r);
    REQUIRE(moq_session_goaway(r->adapter->session, NULL, 0, test_now()) == MOQ_OK);
    executor_leave(r);
    CHECK(session_has_outbound_obligation(r->adapter->session));
    CHECK(!moq_transport_bridge_has_outbound_pending(r->adapter->bridge));
    CHECK(r->adapter->pending_sends == 0 && r->adapter->building == NULL);
    check_query(r, MOQ_DONE);
    exchange(r, p);
    CHECK(moq_session_next_deadline_us(r->adapter->session) != UINT64_MAX);
    check_query(r, MOQ_OK); /* future session timer is not accepted output */
    peer_destroy(p);
    rig_destroy(r);
    printf("CASE core-only/established/receive/timer D%u\n", (unsigned)version);
    ++cases;
}

static moq_subscription_t subscribe_sender_properties(struct rig *r, struct peer *p,
                                                       moq_bytes_t properties)
{
    moq_subscribe_cfg_t cfg;
    moq_subscribe_cfg_init(&cfg);
    moq_bytes_t ns = MOQ_BYTES_LITERAL("flush");
    cfg.track_namespace = (moq_namespace_t){ &ns, 1 };
    cfg.track_name = MOQ_BYTES_LITERAL("track");
    cfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    moq_subscription_t peer_sub, sub = MOQ_SUBSCRIPTION_INVALID;
    REQUIRE(moq_session_subscribe(p->session, &cfg, test_now(), &peer_sub) == MOQ_OK);
    exchange(r, p);
    executor_enter(r);
    moq_event_t ev;
    while (moq_session_poll_events(r->adapter->session, &ev, 1) != 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) sub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
    }
    REQUIRE(sub._opaque != MOQ_SUBSCRIPTION_INVALID._opaque);
    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    ac.track_properties = properties;
    REQUIRE(moq_session_accept_subscribe(r->adapter->session, sub, &ac, test_now()) == MOQ_OK);
    executor_leave(r);
    exchange(r, p);
    return sub;
}

static moq_subscription_t subscribe_sender(struct rig *r, struct peer *p)
{
    return subscribe_sender_properties(r, p, (moq_bytes_t){NULL, 0});
}

static void t_inbound_backpressure(moq_version_t version)
{
    struct rig *r = rig_create_capacity(version, false, 2);
    struct peer *p = establish(r, version);
    const moq_bytes_t names[] = {MOQ_BYTES_LITERAL("one"),
        MOQ_BYTES_LITERAL("two"), MOQ_BYTES_LITERAL("three")};
    for (size_t i = 0; i < sizeof(names) / sizeof(names[0]); ++i) {
        moq_subscribe_cfg_t cfg;
        moq_subscribe_cfg_init(&cfg);
        moq_bytes_t ns = MOQ_BYTES_LITERAL("inbound");
        cfg.track_namespace = (moq_namespace_t){ &ns, 1 };
        cfg.track_name = names[i];
        cfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
        moq_subscription_t sub;
        REQUIRE(moq_session_subscribe(p->session, &cfg, test_now(), &sub) == MOQ_OK);
        exchange(r, p);
    }
    CHECK(moq_transport_bridge_has_pending(r->adapter->bridge));
    CHECK(!moq_transport_bridge_has_outbound_pending(r->adapter->bridge));
    CHECK(!session_has_outbound_obligation(r->adapter->session));
    check_query(r, MOQ_OK);
    peer_destroy(p);
    rig_destroy(r);
    printf("CASE inbound-only backpressure D%u\n", (unsigned)version);
    ++cases;
}

static void t_retained_namespace_response(bool reject, bool split_fin)
{
    struct rig *r = rig_create_capacity(MOQ_VERSION_DRAFT_22, false, 1);
    struct peer *p = establish(r, MOQ_VERSION_DRAFT_22);
    moq_publish_namespace_cfg_t cfg;
    moq_publish_namespace_cfg_init(&cfg);
    moq_bytes_t part = MOQ_BYTES_LITERAL("retained-flush");
    cfg.track_namespace = (moq_namespace_t){&part, 1};
    moq_announcement_t local, remote = MOQ_ANNOUNCEMENT_INVALID;
    executor_enter(r);
    REQUIRE(moq_session_publish_namespace(r->adapter->session, &cfg, test_now(), &local) == MOQ_OK);
    executor_leave(r);
    exchange(r, p);
    moq_event_t ev;
    while (moq_session_poll_events(p->session, &ev, 1)) {
        if (ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED)
            remote = ev.u.namespace_published.ann;
        moq_event_cleanup(&ev);
    }
    REQUIRE(moq_announcement_is_valid(remote));
    if (reject) {
        moq_reject_namespace_cfg_t response;
        moq_reject_namespace_cfg_init(&response);
        response.error_code = MOQ_REQUEST_ERROR_INTERNAL_ERROR;
        REQUIRE(moq_session_reject_namespace(p->session, remote, &response, test_now()) == MOQ_OK);
    } else {
        moq_accept_namespace_cfg_t response;
        moq_accept_namespace_cfg_init(&response);
        REQUIRE(moq_session_accept_namespace(p->session, remote, &response, test_now()) == MOQ_OK);
    }
    moq_msquic_conn_service(p->adapter);
    while (p->server_relayed < p->fake.send_count) {
        fake_msq_send_t *send = &p->fake.sends[p->server_relayed++];
        REQUIRE(((send->flags & QUIC_SEND_FLAG_FIN) != 0) == reject);
        fake_msq_stream_t *st = relay_stream(&r->client->fake, r->adapter,
                                            send->stream->id, false);
        fake_msq_deliver_receive(st, send->bytes, send->bytes_len, reject && !split_fin);
        if (reject && split_fin)
            stream_event(st, QUIC_STREAM_EVENT_PEER_SEND_SHUTDOWN, false);
        REQUIRE(fake_msq_deliver_send_complete(&p->fake, false));
        if (reject)
            stream_event(send->stream, QUIC_STREAM_EVENT_SEND_SHUTDOWN_COMPLETE, true);
    }
    moq_session_t *s = r->adapter->session;
    const moq_ann_entry_t *ann = NULL;
    for (size_t i = 0; i < s->ann_cap; ++i)
        if (moq_announcement_eq(s->announcements[i].handle, local))
            ann = &s->announcements[i];
    REQUIRE(ann != NULL && ann->response_validated);
    REQUIRE(reject || !ann->response_fin);
    REQUIRE(s->action_head == s->action_tail);
    REQUIRE(r->adapter->pending_sends == 0);
    REQUIRE(moq_transport_bridge_has_pending(r->adapter->bridge));
    REQUIRE(!moq_transport_bridge_has_outbound_pending(r->adapter->bridge));
    moq_result_t result = moq_msquic_managed_poll_flush(r->m);
    CHECK(result == (reject ? MOQ_DONE : MOQ_OK));
    check_query(r, result);

    /* Event credit alone commits the retained response; no new app command. */
    executor_enter(r);
    unsigned events = 0;
    while (moq_session_poll_events(s, &ev, 1)) {
        REQUIRE(++events < 8);
        moq_event_cleanup(&ev);
    }
    executor_leave(r);
    REQUIRE(session_has_outbound_obligation(s) == reject);
    check_query(r, reject ? MOQ_DONE : MOQ_OK);
    executor_enter(r);
    moq_msquic_conn_service(r->adapter);
    executor_leave(r);
    complete_sends(r);
    unsigned fins = 0;
    for (int i = 0; i < r->client->fake.stream_count; ++i) {
        fake_msq_stream_t *st = &r->client->fake.streams[i];
        struct moq_msq_stream *ms = moq_msquic_test_stream_find(r->adapter, st->id);
        if (ms && ms->fin_sent && !ms->fin_acked) {
            ++fins;
            stream_event(st, QUIC_STREAM_EVENT_SEND_SHUTDOWN_COMPLETE, true);
        }
    }
    REQUIRE(fins == (reject ? 1u : 0u));
    check_query(r, MOQ_OK);
    peer_destroy(p);
    rig_destroy(r);
    printf("CASE retained namespace %s %s: events=%u FINs=%u\n",
           reject ? "rejection" : "receive-only OK",
           split_fin ? "split-FIN" : "combined", events, fins);
    ++cases;
}

enum outcome { ACCEPT, REJECT, REDIRECT, MIGRATE };

static void t_retained_owner_outcome(bool status, enum outcome outcome, bool peer_fin,
                                      unsigned deferred_signals)
{
    struct rig *r = rig_create_capacity(MOQ_VERSION_DRAFT_22, false, 1);
    struct peer *p = establish(r, MOQ_VERSION_DRAFT_22);
    moq_bytes_t part = MOQ_BYTES_LITERAL("retained-owner");
    moq_namespace_t ns = {&part, 1};
    moq_announcement_t local_ann = MOQ_ANNOUNCEMENT_INVALID;
    moq_announcement_t remote_ann = MOQ_ANNOUNCEMENT_INVALID;
    moq_track_status_handle_t local_ts = MOQ_TRACK_STATUS_HANDLE_INVALID;
    moq_track_status_handle_t remote_ts = MOQ_TRACK_STATUS_HANDLE_INVALID;
    int request_send = r->client->fake.send_count;
    executor_enter(r);
    if (status) {
        moq_track_status_cfg_t cfg;
        moq_track_status_cfg_init(&cfg);
        cfg.track_namespace = ns;
        cfg.track_name = MOQ_BYTES_LITERAL("track");
        REQUIRE(moq_session_track_status(r->adapter->session, &cfg, test_now(), &local_ts) == MOQ_OK);
    } else {
        moq_publish_namespace_cfg_t cfg;
        moq_publish_namespace_cfg_init(&cfg);
        cfg.track_namespace = ns;
        REQUIRE(moq_session_publish_namespace(r->adapter->session, &cfg, test_now(), &local_ann) == MOQ_OK);
    }
    executor_leave(r);
    exchange(r, p);
    REQUIRE(r->client->fake.send_count == request_send + 1);
    fake_msq_send_t *request = &r->client->fake.sends[request_send];
    REQUIRE(request->completed);
    REQUIRE(((request->flags & QUIC_SEND_FLAG_FIN) != 0) == status);
    /* relay_sends delivered the initial request's graceful completion iff FIN
     * was present. Track fixture-delivered events, not the query's answer. */
    bool send_completed = status;
    moq_event_t ev;
    while (moq_session_poll_events(p->session, &ev, 1)) {
        if (ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED)
            remote_ann = ev.u.namespace_published.ann;
        if (ev.kind == MOQ_EVENT_TRACK_STATUS_REQUEST)
            remote_ts = ev.u.track_status_request.handle;
        moq_event_cleanup(&ev);
    }
    if (status) REQUIRE(moq_track_status_handle_is_valid(remote_ts));
    else REQUIRE(moq_announcement_is_valid(remote_ann));
    if (outcome == MIGRATE && !status) {
        executor_enter(r);
        while (moq_session_poll_events(r->adapter->session, &ev, 1))
            moq_event_cleanup(&ev);
        executor_leave(r);
        moq_accept_namespace_cfg_t cfg;
        moq_accept_namespace_cfg_init(&cfg);
        REQUIRE(moq_session_accept_namespace(p->session, remote_ann, &cfg, test_now()) == MOQ_OK);
        exchange(r, p); /* Retain NAMESPACE_ACCEPTED as the event-credit blocker. */
    }
    if (outcome == MIGRATE) {
        moq_request_goaway_cfg_t cfg;
        moq_request_goaway_cfg_init(&cfg);
        REQUIRE((status
            ? moq_session_request_goaway_track_status(p->session, remote_ts, &cfg, test_now())
            : moq_session_request_goaway_namespace(p->session, remote_ann, &cfg, test_now())) == MOQ_OK);
    } else if (status) {
        if (outcome == ACCEPT) {
            moq_accept_track_status_cfg_t cfg;
            moq_accept_track_status_cfg_init(&cfg);
            REQUIRE(moq_session_accept_track_status(p->session, remote_ts, &cfg, test_now()) == MOQ_OK);
        } else {
            moq_reject_track_status_cfg_t cfg;
            moq_reject_track_status_cfg_init(&cfg);
            cfg.error_code = outcome == REDIRECT ? MOQ_REQUEST_ERROR_REDIRECT : MOQ_REQUEST_ERROR_INTERNAL_ERROR;
            REQUIRE(moq_session_reject_track_status(p->session, remote_ts, &cfg, test_now()) == MOQ_OK);
        }
    } else if (outcome == ACCEPT) {
        moq_accept_namespace_cfg_t cfg;
        moq_accept_namespace_cfg_init(&cfg);
        REQUIRE(moq_session_accept_namespace(p->session, remote_ann, &cfg, test_now()) == MOQ_OK);
    } else {
        moq_reject_namespace_cfg_t cfg;
        moq_reject_namespace_cfg_init(&cfg);
        cfg.error_code = outcome == REDIRECT ? MOQ_REQUEST_ERROR_REDIRECT : MOQ_REQUEST_ERROR_INTERNAL_ERROR;
        REQUIRE(moq_session_reject_namespace(p->session, remote_ann, &cfg, test_now()) == MOQ_OK);
    }
    moq_msquic_conn_service(p->adapter);
    fake_msq_stream_t *target = NULL;
    unsigned delivered = 0;
    while (p->server_relayed < p->fake.send_count) {
        fake_msq_send_t *send = &p->fake.sends[p->server_relayed++];
        target = relay_stream(&r->client->fake, r->adapter, send->stream->id, false);
        fake_msq_deliver_receive(target, send->bytes, send->bytes_len, peer_fin);
        REQUIRE(fake_msq_deliver_send_complete(&p->fake, false));
        if (send->flags & QUIC_SEND_FLAG_FIN)
            stream_event(send->stream, QUIC_STREAM_EVENT_SEND_SHUTDOWN_COMPLETE, true);
        ++delivered;
    }
    REQUIRE(delivered == 1 && target != NULL);
    REQUIRE(target == request->stream);
    struct moq_msq_stream *target_ms = moq_msquic_test_stream_find(r->adapter, target->id);
    REQUIRE(target_ms != NULL);
    CHECK(target_ms->send_shutdown_complete == send_completed);
    int terminal_shutdowns = target->shutdown_calls;
    if (deferred_signals & 1u)
        stream_event(target, QUIC_STREAM_EVENT_PEER_SEND_ABORTED, false);
    if (deferred_signals & 2u)
        stream_event(target, QUIC_STREAM_EVENT_PEER_RECEIVE_ABORTED, false);
    REQUIRE(target->shutdown_calls - terminal_shutdowns == ((deferred_signals & 2u) ? 1 : 0));
    if (deferred_signals & 2u) {
        REQUIRE(target->last_shutdown_flags == QUIC_STREAM_SHUTDOWN_FLAG_ABORT_SEND);
        CHECK(target_ms->send_shutdown_complete == send_completed);
        if (!send_completed) {
            check_query(r, MOQ_DONE);
            stream_event(target, QUIC_STREAM_EVENT_SEND_SHUTDOWN_COMPLETE, false);
            send_completed = true;
        }
        CHECK(target_ms->send_shutdown_complete);
    }
    moq_session_t *s = r->adapter->session;
    bool validated = false;
    if (status) {
        for (size_t i = 0; i < s->ts_cap; ++i)
            if (s->track_statuses[i].handle._opaque == local_ts._opaque)
                validated = s->track_statuses[i].response_validated;
    } else {
        for (size_t i = 0; i < s->ann_cap; ++i)
            if (moq_announcement_eq(s->announcements[i].handle, local_ann))
                validated = s->announcements[i].response_validated;
    }
    REQUIRE(validated && s->action_head == s->action_tail);
    REQUIRE(r->adapter->pending_sends == 0 && r->adapter->building == NULL);
    REQUIRE(!moq_transport_bridge_has_outbound_pending(r->adapter->bridge));
    bool peer_open = !peer_fin && !(deferred_signals & 1u);
    bool output = status ? outcome == MIGRATE && peer_open : outcome != ACCEPT || peer_fin;
    moq_result_t result = moq_msquic_managed_poll_flush(r->m);
    printf("OWNER %s outcome=%u peer-FIN=%u signals=%u query=%d expected=%d\n",
           status ? "status" : "announcement", outcome, peer_fin, deferred_signals, result,
           output ? MOQ_DONE : MOQ_OK);
    CHECK(result == (output ? MOQ_DONE : MOQ_OK));
    check_query(r, result);
    int shutdowns = target->shutdown_calls;
    int sends = r->client->fake.send_count;
    executor_enter(r);
    unsigned events = 0;
    while (moq_session_poll_events(s, &ev, 1)) {
        REQUIRE(++events < 8);
        moq_event_cleanup(&ev);
    }
    moq_msquic_conn_service(r->adapter);
    executor_leave(r);
    complete_sends(r);
    unsigned fins = 0;
    for (int i = 0; i < r->client->fake.stream_count; ++i) {
        fake_msq_stream_t *st = &r->client->fake.streams[i];
        struct moq_msq_stream *ms = moq_msquic_test_stream_find(r->adapter, st->id);
        if (ms && ms->fin_sent && !ms->fin_acked) {
            ++fins;
            stream_event(st, QUIC_STREAM_EVENT_SEND_SHUTDOWN_COMPLETE, true);
            if (st == target) send_completed = true;
        }
    }
    bool emitted = fins != 0 || target->shutdown_calls != shutdowns || r->client->fake.send_count != sends;
    if (outcome == MIGRATE) {
        /* Peer STOP's bridge RESET was accepted before event recovery. */
        unsigned owed = (!status && !(deferred_signals & 2u)) + peer_open;
        REQUIRE(target->shutdown_calls - shutdowns == (int)owed);
        REQUIRE(emitted == (owed != 0));
    } else {
        REQUIRE(emitted == output);
    }
    CHECK(target_ms->send_shutdown_complete == send_completed);
    if (!send_completed) {
        REQUIRE(outcome == MIGRATE && !status && target_ms->reset_sent);
        check_query(r, MOQ_DONE);
        stream_event(target, QUIC_STREAM_EVENT_SEND_SHUTDOWN_COMPLETE, false);
    }
    CHECK(target_ms->send_shutdown_complete);
    check_query(r, MOQ_OK);
    printf("OWNER recovery: events=%u FINs=%u shutdowns=%d sends=%d\n",
           events, fins, target->shutdown_calls - shutdowns, r->client->fake.send_count - sends);
    peer_destroy(p);
    rig_destroy(r);
    ++cases;
}

static uint64_t open_request(moq_session_t *s, moq_request_family_t family)
{
    moq_bytes_t part = MOQ_BYTES_LITERAL("retained-family");
    moq_namespace_t ns = {&part, 1};
    switch (family) {
    case MOQ_REQUEST_FAMILY_SUBSCRIBE: {
        moq_subscribe_cfg_t cfg;
        moq_subscribe_cfg_init(&cfg);
        cfg.track_namespace = ns;
        cfg.track_name = MOQ_BYTES_LITERAL("track");
        cfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
        moq_subscription_t h;
        REQUIRE(moq_session_subscribe(s, &cfg, test_now(), &h) == MOQ_OK);
        return h._opaque;
    }
    case MOQ_REQUEST_FAMILY_FETCH: {
        moq_fetch_cfg_t cfg;
        moq_fetch_cfg_init_sized(&cfg, sizeof(cfg));
        cfg.track_namespace = ns;
        cfg.track_name = MOQ_BYTES_LITERAL("track");
        cfg.end_group = 1;
        cfg.end_object = 1;
        moq_fetch_t h;
        REQUIRE(moq_session_fetch(s, &cfg, test_now(), &h) == MOQ_OK);
        return h._opaque;
    }
    case MOQ_REQUEST_FAMILY_PUBLISH: {
        moq_publish_cfg_t cfg;
        moq_publish_cfg_init_sized(&cfg, sizeof(cfg));
        cfg.track_namespace = ns;
        cfg.track_name = MOQ_BYTES_LITERAL("track");
        moq_publication_t h;
        REQUIRE(moq_session_publish(s, &cfg, test_now(), &h) == MOQ_OK);
        return h._opaque;
    }
    case MOQ_REQUEST_FAMILY_ANNOUNCEMENT: {
        moq_publish_namespace_cfg_t cfg;
        moq_publish_namespace_cfg_init(&cfg);
        cfg.track_namespace = ns;
        moq_announcement_t h;
        REQUIRE(moq_session_publish_namespace(s, &cfg, test_now(), &h) == MOQ_OK);
        return h._opaque;
    }
    case MOQ_REQUEST_FAMILY_NS_SUB: {
        moq_subscribe_namespace_cfg_t cfg;
        moq_subscribe_namespace_cfg_init(&cfg);
        cfg.track_namespace_prefix = ns;
        cfg.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
        moq_ns_sub_handle_t h;
        REQUIRE(moq_session_subscribe_namespace(s, &cfg, test_now(), &h) == MOQ_OK);
        return h._opaque;
    }
    case MOQ_REQUEST_FAMILY_SUBSCRIBE_TRACKS: {
        moq_subscribe_tracks_cfg_t cfg;
        moq_subscribe_tracks_cfg_init(&cfg);
        cfg.track_namespace_prefix = ns;
        moq_track_sub_handle_t h;
        REQUIRE(moq_session_subscribe_tracks(s, &cfg, test_now(), &h) == MOQ_OK);
        return h._opaque;
    }
    default: REQUIRE(false); return 0;
    }
}

static uint64_t request_event_handle(const moq_event_t *ev)
{
    switch (ev->kind) {
    case MOQ_EVENT_SUBSCRIBE_REQUEST: return ev->u.subscribe_request.sub._opaque;
    case MOQ_EVENT_FETCH_REQUEST: return ev->u.fetch_request.fetch._opaque;
    case MOQ_EVENT_PUBLISH_REQUEST: return ev->u.publish_request.pub._opaque;
    case MOQ_EVENT_NAMESPACE_PUBLISHED: return ev->u.namespace_published.ann._opaque;
    case MOQ_EVENT_NS_SUB_REQUEST: return ev->u.ns_sub_request.handle._opaque;
    case MOQ_EVENT_SUBSCRIBE_TRACKS_REQUEST: return ev->u.subscribe_tracks_request.handle._opaque;
    default: return 0;
    }
}

static void reject_request(moq_session_t *s, moq_request_family_t family,
                           uint64_t handle, bool redirect)
{
    moq_request_error_t error = redirect ? MOQ_REQUEST_ERROR_REDIRECT : MOQ_REQUEST_ERROR_INTERNAL_ERROR;
    switch (family) {
    case MOQ_REQUEST_FAMILY_SUBSCRIBE: {
        moq_reject_subscribe_cfg_t cfg;
        moq_reject_subscribe_cfg_init(&cfg);
        cfg.error_code = error;
        REQUIRE(moq_session_reject_subscribe(s, (moq_subscription_t){handle}, &cfg, test_now()) == MOQ_OK);
        break;
    }
    case MOQ_REQUEST_FAMILY_FETCH: {
        moq_reject_fetch_cfg_t cfg;
        moq_reject_fetch_cfg_init(&cfg);
        cfg.error_code = error;
        REQUIRE(moq_session_reject_fetch(s, (moq_fetch_t){handle}, &cfg, test_now()) == MOQ_OK);
        break;
    }
    case MOQ_REQUEST_FAMILY_PUBLISH: {
        moq_reject_publish_cfg_t cfg;
        moq_reject_publish_cfg_init(&cfg);
        cfg.error_code = error;
        REQUIRE(moq_session_reject_publish(s, (moq_publication_t){handle}, &cfg, test_now()) == MOQ_OK);
        break;
    }
    case MOQ_REQUEST_FAMILY_ANNOUNCEMENT: {
        moq_reject_namespace_cfg_t cfg;
        moq_reject_namespace_cfg_init(&cfg);
        cfg.error_code = error;
        REQUIRE(moq_session_reject_namespace(s, (moq_announcement_t){handle}, &cfg, test_now()) == MOQ_OK);
        break;
    }
    case MOQ_REQUEST_FAMILY_NS_SUB: {
        moq_reject_ns_sub_cfg_t cfg;
        moq_reject_ns_sub_cfg_init(&cfg);
        cfg.error_code = error;
        REQUIRE(moq_session_reject_ns_sub(s, (moq_ns_sub_handle_t){handle}, &cfg, test_now()) == MOQ_OK);
        break;
    }
    case MOQ_REQUEST_FAMILY_SUBSCRIBE_TRACKS: {
        moq_reject_subscribe_tracks_cfg_t cfg;
        moq_reject_subscribe_tracks_cfg_init(&cfg);
        cfg.error_code = error;
        REQUIRE(moq_session_reject_subscribe_tracks(s, (moq_track_sub_handle_t){handle}, &cfg, test_now()) == MOQ_OK);
        break;
    }
    default: REQUIRE(false);
    }
}

static uint8_t request_output(const moq_session_t *s, moq_request_family_t family,
                              uint64_t handle)
{
    size_t slot = moq_handle_slot(handle);
    switch (family) {
    case MOQ_REQUEST_FAMILY_SUBSCRIBE: return s->subs[slot].rs.retained_output;
    case MOQ_REQUEST_FAMILY_FETCH: return s->fetches[slot].rs.retained_output;
    case MOQ_REQUEST_FAMILY_PUBLISH: return s->publishes[slot].rs.retained_output;
    case MOQ_REQUEST_FAMILY_ANNOUNCEMENT: return s->announcements[slot].rs.retained_output;
    case MOQ_REQUEST_FAMILY_NS_SUB: return s->ns_subs[slot].retained_output;
    case MOQ_REQUEST_FAMILY_SUBSCRIBE_TRACKS: return s->track_subs[slot].rs.retained_output;
    default: REQUIRE(false); return MOQ_RETAINED_OUTPUT_NONE;
    }
}

static void t_retained_family_reuse(moq_version_t version,
                                   moq_request_family_t family, bool redirect)
{
    struct rig *r = rig_create_capacity(version, false, 1);
    struct peer *p = establish(r, version);
    uint64_t previous = 0;
    for (unsigned cycle = 0; cycle < 2; ++cycle) {
        if (cycle) {
            moq_track_status_cfg_t cfg;
            moq_track_status_cfg_init(&cfg);
            moq_bytes_t part = MOQ_BYTES_LITERAL("event-credit");
            cfg.track_namespace = (moq_namespace_t){&part, 1};
            cfg.track_name = MOQ_BYTES_LITERAL("track");
            moq_track_status_handle_t h;
            REQUIRE(moq_session_track_status(p->session, &cfg, test_now(), &h) == MOQ_OK);
            exchange(r, p); /* Future app answer only; its event occupies credit. */
        }
        executor_enter(r);
        uint64_t local = open_request(r->adapter->session, family);
        REQUIRE(request_output(r->adapter->session, family, local) == MOQ_RETAINED_OUTPUT_NONE);
        executor_leave(r);
        if (cycle) {
            REQUIRE(moq_handle_slot(local) == moq_handle_slot(previous));
            REQUIRE(moq_handle_generation(local) != moq_handle_generation(previous));
        }
        previous = local;
        exchange(r, p);
        check_query(r, MOQ_OK);
        uint64_t remote = 0;
        moq_event_t ev;
        while (moq_session_poll_events(p->session, &ev, 1)) {
            uint64_t h = request_event_handle(&ev);
            if (h) remote = h;
            moq_event_cleanup(&ev);
        }
        REQUIRE(remote != 0);
        int response_start = p->fake.send_count;
        reject_request(p->session, family, remote, redirect);
        exchange(r, p);
        REQUIRE(p->fake.send_count == response_start + 1);
        fake_msq_send_t *response = &p->fake.sends[response_start];
        if (!(response->flags & QUIC_SEND_FLAG_FIN)) {
            struct moq_msq_stream *ms = moq_msquic_test_stream_find(p->adapter, response->stream->id);
            REQUIRE(ms != NULL && ms->fin_sent);
            stream_event(relay_stream(&r->client->fake, r->adapter,
                response->stream->id, false), QUIC_STREAM_EVENT_PEER_SEND_SHUTDOWN, false);
            stream_event(response->stream, QUIC_STREAM_EVENT_SEND_SHUTDOWN_COMPLETE, true);
        }
        REQUIRE(request_output(r->adapter->session, family, local) == MOQ_RETAINED_OUTPUT_FIN);
        REQUIRE(r->adapter->session->action_head == r->adapter->session->action_tail);
        REQUIRE(!moq_transport_bridge_has_outbound_pending(r->adapter->bridge));
        REQUIRE(r->adapter->pending_sends == 0);
        check_query(r, MOQ_DONE);
        executor_enter(r);
        unsigned events = 0;
        while (moq_session_poll_events(r->adapter->session, &ev, 1)) {
            REQUIRE(++events < 8);
            moq_event_cleanup(&ev);
        }
        moq_msquic_conn_service(r->adapter);
        executor_leave(r);
        complete_sends(r);
        unsigned fins = 0;
        for (int i = 0; i < r->client->fake.stream_count; ++i) {
            fake_msq_stream_t *st = &r->client->fake.streams[i];
            struct moq_msq_stream *ms = moq_msquic_test_stream_find(r->adapter, st->id);
            if (ms && ms->fin_sent && !ms->fin_acked) {
                ++fins;
                stream_event(st, QUIC_STREAM_EVENT_SEND_SHUTDOWN_COMPLETE, true);
            }
        }
        REQUIRE(fins == 1);
        REQUIRE(request_output(r->adapter->session, family, local) == MOQ_RETAINED_OUTPUT_NONE);
        check_query(r, MOQ_OK);
        stream_event(response->stream, QUIC_STREAM_EVENT_PEER_SEND_SHUTDOWN, false);
        executor_enter(r);
        while (moq_session_poll_events(r->adapter->session, &ev, 1)) {
            REQUIRE(++events < 8);
            moq_event_cleanup(&ev);
        }
        executor_leave(r);
    }
    peer_destroy(p);
    rig_destroy(r);
    printf("CASE retained rejection/reuse D%u family=%u redirect=%u\n",
           (unsigned)version, family, redirect);
    ++cases;
}

static void t_retained_automatic_update(moq_version_t version,
                                         moq_request_family_t family)
{
    struct rig *r = rig_create_limits(version, false, 0, 2);
    struct peer *p = establish(r, version);
    (void)open_request(p->session, family);
    exchange(r, p);
    uint64_t local = 0;
    moq_event_t ev;
    executor_enter(r);
    moq_session_t *s = r->adapter->session;
    while (moq_session_poll_events(s, &ev, 1)) {
        uint64_t h = request_event_handle(&ev);
        if (h) local = h;
        moq_event_cleanup(&ev);
    }
    REQUIRE(local != 0);
    size_t slot = moq_handle_slot(local);
    moq_stream_ref_t ref;
    uint64_t request_id;
    if (family == MOQ_REQUEST_FAMILY_ANNOUNCEMENT) {
        moq_accept_namespace_cfg_t cfg;
        moq_accept_namespace_cfg_init(&cfg);
        REQUIRE(moq_session_accept_namespace(s, (moq_announcement_t){local}, &cfg, test_now()) == MOQ_OK);
        ref = s->announcements[slot].rs.ref;
        request_id = s->announcements[slot].request_id;
    } else {
        moq_accept_subscribe_tracks_cfg_t cfg;
        moq_accept_subscribe_tracks_cfg_init(&cfg);
        REQUIRE(moq_session_accept_subscribe_tracks(s, (moq_track_sub_handle_t){local}, &cfg, test_now()) == MOQ_OK);
        ref = s->track_subs[slot].rs.ref;
        request_id = s->track_subs[slot].request_id;
    }
    executor_leave(r);
    exchange(r, p);
    check_query(r, MOQ_OK);
    uint8_t bytes[128];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, bytes, sizeof(bytes));
    moq_request_update_encode_args_t update = {
        .request_id = request_id + 2, .existing_request_id = request_id
    };
    REQUIRE(p->session->profile->encode_request_update(p->session, &w, &update) == MOQ_OK);
    int start = r->client->fake.send_count;
    executor_enter(r);
    (void)open_request(s, MOQ_REQUEST_FAMILY_SUBSCRIBE);
    (void)open_request(s, MOQ_REQUEST_FAMILY_FETCH);
    REQUIRE(moq_session_action_capacity(s) == 0);
    REQUIRE(moq_session_on_bidi_stream_bytes(s, ref, bytes, moq_buf_writer_offset(&w),
                                              false, test_now()) == MOQ_ERR_WOULD_BLOCK);
    REQUIRE(request_output(s, family, local) == MOQ_RETAINED_OUTPUT_RESPONSE);
    executor_leave(r);
    check_query(r, MOQ_DONE);
    executor_enter(r);
    moq_msquic_conn_service(r->adapter);
    executor_leave(r);
    complete_sends(r);
    if (request_output(s, family, local) == MOQ_RETAINED_OUTPUT_RESPONSE) {
        /* Legacy/track-sub ingress retries only when its transport refeeds. */
        REQUIRE(s->action_head == s->action_tail);
        REQUIRE(!moq_transport_bridge_has_outbound_pending(r->adapter->bridge));
        REQUIRE(r->adapter->pending_sends == 0);
        check_query(r, MOQ_DONE);
        executor_enter(r);
        REQUIRE(moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, false, test_now()) == MOQ_OK);
        moq_msquic_conn_service(r->adapter);
        executor_leave(r);
        complete_sends(r);
    }
    REQUIRE(moq_session_state(s) == MOQ_SESS_ESTABLISHED);
    REQUIRE(request_output(s, family, local) == MOQ_RETAINED_OUTPUT_NONE);
    unsigned responses = 0, fins = 0;
    for (int i = start; i < r->client->fake.send_count; ++i) {
        fake_msq_send_t *send = &r->client->fake.sends[i];
        moq_request_envelope_t env;
        size_t consumed = 0;
        const moq_request_codec_t *c = s->profile->request_codec;
        REQUIRE(c->decode_envelope(send->bytes, send->bytes_len, &env, &consumed) == MOQ_OK);
        if (env.msg_type != c->type.request_error) continue;
        moq_request_error_msg_t error;
        moq_decoded_redirect_t redirect;
        moq_bytes_t parts[MOQ_DECODED_MAX_NAMESPACE_PARTS];
        REQUIRE(c->decode_request_error(env.payload, env.payload_len,
            parts, MOQ_DECODED_MAX_NAMESPACE_PARTS, &error, &redirect) == MOQ_OK);
        REQUIRE(error.error_code == MOQ_REQUEST_ERROR_NOT_SUPPORTED);
        REQUIRE(consumed == send->bytes_len && (send->flags & QUIC_SEND_FLAG_FIN));
        ++responses;
    }
    for (int i = 0; i < r->client->fake.stream_count; ++i) {
        fake_msq_stream_t *st = &r->client->fake.streams[i];
        struct moq_msq_stream *ms = moq_msquic_test_stream_find(r->adapter, st->id);
        if (ms && ms->fin_sent && !ms->fin_acked) {
            ++fins;
            stream_event(st, QUIC_STREAM_EVENT_SEND_SHUTDOWN_COMPLETE, true);
        }
    }
    REQUIRE(responses == 1 && fins == 1);
    check_query(r, MOQ_OK);
    peer_destroy(p);
    rig_destroy(r);
    printf("CASE automatic update rejection D%u family=%u\n", (unsigned)version, family);
    ++cases;
}

static void t_retained_namespace_goaway(moq_version_t version, bool own_goaway)
{
    struct rig *r = rig_create_capacity(version, false, 1);
    struct peer *p = establish(r, version);
    executor_enter(r);
    moq_session_t *s = r->adapter->session;
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1)) moq_event_cleanup(&ev);
    uint64_t local = own_goaway ? 0 : open_request(s, MOQ_REQUEST_FAMILY_NS_SUB);
    executor_leave(r);
    uint64_t remote = own_goaway ? open_request(p->session, MOQ_REQUEST_FAMILY_NS_SUB) : 0;
    exchange(r, p);
    executor_enter(r);
    while (moq_session_poll_events(own_goaway ? s : p->session, &ev, 1)) {
        uint64_t h = request_event_handle(&ev);
        if (h) {
            if (own_goaway) local = h;
            else remote = h;
        }
        moq_event_cleanup(&ev);
    }
    REQUIRE(local != 0 && remote != 0);
    moq_accept_ns_sub_cfg_t ac;
    moq_accept_ns_sub_cfg_init(&ac);
    REQUIRE(moq_session_accept_ns_sub(own_goaway ? s : p->session,
        (moq_ns_sub_handle_t){own_goaway ? local : remote}, &ac, test_now()) == MOQ_OK);
    executor_leave(r);
    exchange(r, p); /* NS_SUB_OK occupies event credit after establishment. */
    moq_request_goaway_cfg_t gc;
    moq_request_goaway_cfg_init(&gc);
    if (own_goaway) {
        moq_track_status_cfg_t cfg;
        moq_track_status_cfg_init(&cfg);
        moq_bytes_t part = MOQ_BYTES_LITERAL("event-credit");
        cfg.track_namespace = (moq_namespace_t){&part, 1};
        cfg.track_name = MOQ_BYTES_LITERAL("track");
        moq_track_status_handle_t h;
        REQUIRE(moq_session_track_status(p->session, &cfg, test_now(), &h) == MOQ_OK);
        exchange(r, p); /* Future app answer occupies publisher-side credit. */
        executor_enter(r);
        REQUIRE(moq_session_request_goaway_ns_sub(s, (moq_ns_sub_handle_t){local}, &gc, test_now()) == MOQ_OK);
        moq_msquic_conn_service(r->adapter);
        executor_leave(r);
        complete_sends(r); /* Crossed GOAWAYs: peer has not received ours yet. */
    }
    check_query(r, MOQ_OK);
    REQUIRE(moq_session_request_goaway_ns_sub(p->session, (moq_ns_sub_handle_t){remote}, &gc, test_now()) == MOQ_OK);
    moq_msquic_conn_service(p->adapter);
    relay_sends(&p->fake, &p->server_relayed, &r->client->fake, r->adapter);
    REQUIRE(request_output(s, MOQ_REQUEST_FAMILY_NS_SUB, local) == MOQ_RETAINED_OUTPUT_FIN);
    REQUIRE(s->action_head == s->action_tail && r->adapter->pending_sends == 0);
    REQUIRE(!moq_transport_bridge_has_outbound_pending(r->adapter->bridge));
    check_query(r, MOQ_DONE);
    executor_enter(r);
    unsigned migrations = 0;
    while (moq_session_poll_events(s, &ev, 1)) {
        if (ev.kind == MOQ_EVENT_REQUEST_GOAWAY) ++migrations;
        moq_event_cleanup(&ev);
    }
    moq_msquic_conn_service(r->adapter);
    while (moq_session_poll_events(s, &ev, 1)) {
        if (ev.kind == MOQ_EVENT_REQUEST_GOAWAY) ++migrations;
        moq_event_cleanup(&ev);
    }
    executor_leave(r);
    complete_sends(r);
    unsigned fins = 0;
    for (int i = 0; i < r->client->fake.stream_count; ++i) {
        fake_msq_stream_t *st = &r->client->fake.streams[i];
        struct moq_msq_stream *ms = moq_msquic_test_stream_find(r->adapter, st->id);
        if (ms && ms->fin_sent && !ms->fin_acked) {
            ++fins;
            stream_event(st, QUIC_STREAM_EVENT_SEND_SHUTDOWN_COMPLETE, true);
        }
    }
    printf("NS GOAWAY D%u crossed=%u migrations=%u FINs=%u\n",
           (unsigned)version, own_goaway, migrations, fins);
    REQUIRE(migrations == 1 && fins == 1);
    REQUIRE(request_output(s, MOQ_REQUEST_FAMILY_NS_SUB, local) == MOQ_RETAINED_OUTPUT_NONE);
    check_query(r, MOQ_OK);
    peer_destroy(p);
    rig_destroy(r);
    printf("CASE retained namespace GOAWAY D%u crossed=%u\n", (unsigned)version, own_goaway);
    ++cases;
}

struct crossed_namespace {
    struct rig *r;
    struct peer *p;
    uint64_t local, remote;
    moq_stream_ref_t ref;
    fake_msq_stream_t *stream;
};

struct crossed_events { unsigned migrations, found; };

static void crossed_poll(struct crossed_namespace *c, struct crossed_events *seen)
{
    moq_event_t ev;
    unsigned n = 0;
    while (moq_session_poll_events(c->r->adapter->session, &ev, 1)) {
        REQUIRE(++n < 8);
        if (ev.kind == MOQ_EVENT_REQUEST_GOAWAY) {
            CHECK(ev.u.request_goaway.family == MOQ_REQUEST_FAMILY_NS_SUB);
            ++seen->migrations;
        }
        if (ev.kind == MOQ_EVENT_NAMESPACE_FOUND) ++seen->found;
        moq_event_cleanup(&ev);
    }
}

static struct crossed_namespace crossed_create_limits(moq_version_t version,
    bool subscriber, bool event_pressure, uint32_t max_events)
{
    struct crossed_namespace c = {0};
    c.r = rig_create_limits(version, false, max_events, 2);
    c.p = establish(c.r, version);
    moq_session_t *s = c.r->adapter->session;
    moq_event_t ev;
    executor_enter(c.r);
    while (moq_session_poll_events(s, &ev, 1)) moq_event_cleanup(&ev);
    int request_start = subscriber ? c.r->client->fake.send_count : c.p->fake.send_count;
    if (subscriber) c.local = open_request(s, MOQ_REQUEST_FAMILY_NS_SUB);
    else c.remote = open_request(c.p->session, MOQ_REQUEST_FAMILY_NS_SUB);
    executor_leave(c.r);
    exchange(c.r, c.p);
    executor_enter(c.r);
    moq_session_t *responder = subscriber ? c.p->session : s;
    while (moq_session_poll_events(responder, &ev, 1)) {
        uint64_t h = request_event_handle(&ev);
        if (h) {
            if (subscriber) c.remote = h;
            else c.local = h;
        }
        moq_event_cleanup(&ev);
    }
    REQUIRE(c.local != 0 && c.remote != 0);
    moq_accept_ns_sub_cfg_t ac;
    moq_accept_ns_sub_cfg_init(&ac);
    REQUIRE(moq_session_accept_ns_sub(responder,
        (moq_ns_sub_handle_t){subscriber ? c.remote : c.local}, &ac, test_now()) == MOQ_OK);
    executor_leave(c.r);
    exchange(c.r, c.p);
    executor_enter(c.r);
    while (moq_session_poll_events(s, &ev, 1)) moq_event_cleanup(&ev);
    executor_leave(c.r);
    while (moq_session_poll_events(c.p->session, &ev, 1)) moq_event_cleanup(&ev);
    c.ref = s->ns_subs[moq_handle_slot(c.local)].stream_ref;
    fake_msq_send_t *request = subscriber ? &c.r->client->fake.sends[request_start]
                                         : &c.p->fake.sends[request_start];
    c.stream = relay_stream(&c.r->client->fake, c.r->adapter, request->stream->id, false);
    REQUIRE(c.ref._v != 0);
    if (event_pressure) {
        moq_track_status_cfg_t cfg;
        moq_track_status_cfg_init(&cfg);
        moq_bytes_t part = MOQ_BYTES_LITERAL("crossed-credit");
        cfg.track_namespace = (moq_namespace_t){&part, 1};
        cfg.track_name = MOQ_BYTES_LITERAL("track");
        moq_track_status_handle_t h;
        REQUIRE(moq_session_track_status(c.p->session, &cfg, test_now(), &h) == MOQ_OK);
        exchange(c.r, c.p);
    }
    moq_request_goaway_cfg_t gc;
    moq_request_goaway_cfg_init(&gc);
    executor_enter(c.r);
    REQUIRE(moq_session_request_goaway_ns_sub(s, (moq_ns_sub_handle_t){c.local}, &gc, test_now()) == MOQ_OK);
    moq_msquic_conn_service(c.r->adapter);
    executor_leave(c.r);
    complete_sends(c.r); /* Do not deliver our GOAWAY: the peer's one crosses it. */
    REQUIRE(s->ns_subs[moq_handle_slot(c.local)].state == MOQ_NS_SUB_ESTABLISHED);
    REQUIRE(c.stream->shutdown_calls == 0);
    check_query(c.r, MOQ_OK); /* A sent GOAWAY is not a received migration. */
    return c;
}

static struct crossed_namespace crossed_create(moq_version_t version,
                                                bool subscriber, bool event_pressure)
{
    return crossed_create_limits(version, subscriber, event_pressure, 1);
}

static fake_msq_send_t *crossed_goaway(struct crossed_namespace *c)
{
    int start = c->p->fake.send_count;
    moq_request_goaway_cfg_t gc;
    moq_request_goaway_cfg_init(&gc);
    REQUIRE(moq_session_request_goaway_ns_sub(c->p->session,
        (moq_ns_sub_handle_t){c->remote}, &gc, test_now()) == MOQ_OK);
    moq_msquic_conn_service(c->p->adapter);
    REQUIRE(c->p->fake.send_count == start + 1);
    return &c->p->fake.sends[start];
}

static void crossed_destroy(struct crossed_namespace *c)
{
    peer_destroy(c->p);
    rig_destroy(c->r);
}

/* pressure: 0 available credit, 1 event credit, 2 action credit. */
static void t_crossed_namespace_retry(moq_version_t version, bool subscriber,
                                       unsigned pressure, bool empty_fin)
{
    struct crossed_namespace c = crossed_create(version, subscriber, pressure == 1);
    moq_session_t *s = c.r->adapter->session;
    fake_msq_send_t *send = crossed_goaway(&c);
    if (pressure == 2) {
        executor_enter(c.r);
        (void)open_request(s, MOQ_REQUEST_FAMILY_SUBSCRIBE);
        (void)open_request(s, MOQ_REQUEST_FAMILY_FETCH);
        REQUIRE(moq_session_action_capacity(s) == 0);
        REQUIRE(moq_session_on_bidi_stream_bytes(s, c.ref, send->bytes, send->bytes_len,
            false, test_now()) == MOQ_ERR_WOULD_BLOCK);
        if (empty_fin)
            (void)moq_session_on_bidi_stream_bytes(s, c.ref, NULL, 0, true, test_now());
        executor_leave(c.r);
    } else {
        fake_msq_deliver_receive(c.stream, send->bytes, send->bytes_len, false);
        if (empty_fin) stream_event(c.stream, QUIC_STREAM_EVENT_PEER_SEND_SHUTDOWN, false);
    }
    REQUIRE(fake_msq_deliver_send_complete(&c.p->fake, false));
    if (pressure) {
        REQUIRE(s->ns_subs[moq_handle_slot(c.local)].retained_output == MOQ_RETAINED_OUTPUT_FIN);
        REQUIRE(c.stream->shutdown_calls == 0);
    }
    check_query(c.r, MOQ_DONE);
    struct crossed_events seen = {0};
    for (unsigned i = 0; i < 4; ++i) {
        executor_enter(c.r);
        crossed_poll(&c, &seen);
        moq_msquic_conn_service(c.r->adapter);
        if (pressure == 2 && s->ns_subs[moq_handle_slot(c.local)].state != MOQ_NS_SUB_FREE)
            (void)moq_session_on_bidi_stream_bytes(s, c.ref, NULL, 0, false, test_now());
        moq_msquic_conn_service(c.r->adapter);
        crossed_poll(&c, &seen);
        executor_leave(c.r);
    }
    complete_sends(c.r);
    CHECK(seen.migrations == 1 && seen.found == 0);
    CHECK(c.stream->shutdown_calls == 1);
    CHECK(s->ns_subs[moq_handle_slot(c.local)].state == MOQ_NS_SUB_FREE);
    CHECK(request_output(s, MOQ_REQUEST_FAMILY_NS_SUB, c.local) == MOQ_RETAINED_OUTPUT_NONE);
    check_query(c.r, MOQ_DONE); /* Byte completion is not reciprocal FIN ACK. */
    struct moq_msq_stream *ms = moq_msquic_test_stream_find(c.r->adapter, c.stream->id);
    CHECK(ms != NULL && ms->fin_sent && !ms->fin_acked);
    if (ms && ms->fin_sent) stream_event(c.stream, QUIC_STREAM_EVENT_SEND_SHUTDOWN_COMPLETE, true);
    check_query(c.r, MOQ_OK);
    for (unsigned i = 0; i < 3; ++i) {
        executor_enter(c.r);
        moq_msquic_conn_service(c.r->adapter);
        crossed_poll(&c, &seen);
        executor_leave(c.r);
    }
    CHECK(seen.migrations == 1 && c.stream->shutdown_calls == 1);
    CHECK(moq_session_state(s) == MOQ_SESS_ESTABLISHED);
    printf("CASE crossed retry D%u subscriber=%u pressure=%u empty-FIN=%u migrations=%u shutdowns=%d\n",
        (unsigned)version, subscriber, pressure, empty_fin, seen.migrations, c.stream->shutdown_calls);
    crossed_destroy(&c);
    ++cases;
}

static void t_crossed_namespace_empty_fin(moq_version_t version, bool subscriber)
{
    struct crossed_namespace c = crossed_create(version, subscriber, false);
    stream_event(c.stream, QUIC_STREAM_EVENT_PEER_SEND_SHUTDOWN, false);
    struct crossed_events seen = {0};
    executor_enter(c.r);
    for (unsigned i = 0; i < 3; ++i) {
        moq_msquic_conn_service(c.r->adapter);
        crossed_poll(&c, &seen);
    }
    executor_leave(c.r);
    CHECK(seen.migrations == 0 && seen.found == 0);
    CHECK(c.r->adapter->session->ns_subs[moq_handle_slot(c.local)].state == MOQ_NS_SUB_FREE);
    CHECK(c.stream->shutdown_calls == 0);
    check_query(c.r, MOQ_OK);
    crossed_destroy(&c);
    printf("CASE crossed empty-buffer FIN D%u subscriber=%u\n", (unsigned)version, subscriber);
    ++cases;
}

static void t_crossed_namespace_inflight(moq_version_t version, bool pressure)
{
    struct crossed_namespace c = crossed_create(version, true, pressure);
    moq_bytes_t part = MOQ_BYTES_LITERAL("in-flight");
    moq_namespace_t suffix = {&part, 1};
    REQUIRE(moq_session_send_namespace(c.p->session, (moq_ns_sub_handle_t){c.remote},
        &suffix, test_now()) == MOQ_OK);
    moq_msquic_conn_service(c.p->adapter);
    relay_sends(&c.p->fake, &c.p->server_relayed, &c.r->client->fake, c.r->adapter);
    check_query(c.r, MOQ_OK); /* Namespace delivery is receive-only. */
    struct crossed_events seen = {0};
    executor_enter(c.r);
    for (unsigned i = 0; i < 4; ++i) {
        crossed_poll(&c, &seen);
        moq_msquic_conn_service(c.r->adapter);
    }
    crossed_poll(&c, &seen);
    executor_leave(c.r);
    CHECK(seen.found == 1 && seen.migrations == 0);
    CHECK(c.r->adapter->session->ns_subs[moq_handle_slot(c.local)].state == MOQ_NS_SUB_ESTABLISHED);
    CHECK(c.stream->shutdown_calls == 0);
    check_query(c.r, MOQ_OK);
    crossed_destroy(&c);
    printf("CASE crossed in-flight namespace D%u pressure=%u\n", (unsigned)version, pressure);
    ++cases;
}

struct crossed_terminal_events { unsigned found, gone, migrations, other; };

static void crossed_terminal_poll(struct crossed_namespace *c,
                                   struct crossed_terminal_events *seen)
{
    moq_event_t ev;
    unsigned n = 0;
    while (moq_session_poll_events(c->r->adapter->session, &ev, 1)) {
        REQUIRE(++n < 8);
        if (ev.kind == MOQ_EVENT_NAMESPACE_FOUND) {
            CHECK(ev.u.namespace_found.handle._opaque == c->local);
            ++seen->found;
        } else if (ev.kind == MOQ_EVENT_NAMESPACE_GONE) {
            CHECK(ev.u.namespace_gone.handle._opaque == c->local);
            CHECK(seen->found == 1 && seen->gone == 0);
            ++seen->gone;
        } else if (ev.kind == MOQ_EVENT_REQUEST_GOAWAY) {
            ++seen->migrations;
        } else {
            ++seen->other;
        }
        moq_event_cleanup(&ev);
    }
}

/* pressure: 0 available terminal credit, 1 event refusal after FOUND,
 * 2 action refusal with room for GONE. FIN retries use public ingress. */
static void t_crossed_namespace_pending_fin(moq_version_t version,
    unsigned pressure, bool with_fin, bool repeat_fin)
{
    struct crossed_namespace c = crossed_create_limits(version, true,
        pressure == 1, pressure == 1 ? 1 : 2);
    moq_session_t *s = c.r->adapter->session;
    size_t slot = moq_handle_slot(c.local);
    uint32_t generation = s->ns_subs[slot].generation;
    moq_bytes_t part = MOQ_BYTES_LITERAL("in-flight");
    moq_namespace_t suffix = {&part, 1};
    int send_start = c.p->fake.send_count;
    REQUIRE(moq_session_send_namespace(c.p->session, (moq_ns_sub_handle_t){c.remote},
        &suffix, test_now()) == MOQ_OK);
    moq_msquic_conn_service(c.p->adapter);
    REQUIRE(c.p->fake.send_count == send_start + 1);
    fake_msq_send_t *send = &c.p->fake.sends[send_start];
    if (pressure == 2) {
        executor_enter(c.r);
        (void)open_request(s, MOQ_REQUEST_FAMILY_SUBSCRIBE);
        (void)open_request(s, MOQ_REQUEST_FAMILY_FETCH);
        REQUIRE(moq_session_action_capacity(s) == 0);
        REQUIRE(moq_session_on_bidi_stream_bytes(s, c.ref, send->bytes,
            send->bytes_len, with_fin, test_now()) == (with_fin ? MOQ_ERR_WOULD_BLOCK : MOQ_OK));
        REQUIRE(moq_session_action_capacity(s) == 0);
        REQUIRE(event_queue_avail(s) == 1);
        executor_leave(c.r);
    } else if (repeat_fin) {
        executor_enter(c.r);
        REQUIRE(moq_session_on_bidi_stream_bytes(s, c.ref, send->bytes,
            send->bytes_len, with_fin, test_now()) == MOQ_ERR_WOULD_BLOCK);
        executor_leave(c.r);
    } else {
        fake_msq_deliver_receive(c.stream, send->bytes, send->bytes_len, with_fin);
    }
    REQUIRE(fake_msq_deliver_send_complete(&c.p->fake, false));

    if (pressure == 1) {
        REQUIRE(s->ns_subs[slot].recv_len > 0);
        REQUIRE(s->ns_subs[slot].pending_fin == with_fin);
        check_query(c.r, with_fin ? MOQ_DONE : MOQ_OK);
        executor_enter(c.r);
        moq_event_t blocker;
        REQUIRE(moq_session_poll_events(s, &blocker, 1) == 1);
        REQUIRE(blocker.kind == MOQ_EVENT_TRACK_STATUS_REQUEST);
        moq_event_cleanup(&blocker);
        if (repeat_fin)
            REQUIRE(moq_session_on_bidi_stream_bytes(s, c.ref, NULL, 0,
                false, test_now()) == MOQ_ERR_WOULD_BLOCK);
        moq_msquic_conn_service(c.r->adapter);
        executor_leave(c.r);
    }
    if (pressure) {
        /* FOUND has committed and consumed every byte. Only the separately
         * latched FIN can bring the terminal cursor back to the parser. */
        REQUIRE(s->ns_subs[slot].recv_len == 0);
        REQUIRE(s->ns_subs[slot].pending_fin == with_fin);
        REQUIRE(s->ns_subs[slot].terminal.cause == 0);
        REQUIRE(s->ns_subs[slot].state == MOQ_NS_SUB_ESTABLISHED);
        REQUIRE(s->ns_subs[slot].generation == generation);
        REQUIRE(c.stream->shutdown_calls == 0);
        REQUIRE(event_queue_avail(s) == (pressure == 1 ? 0u : 1u));
        if (with_fin) check_query(c.r, MOQ_DONE);
    }

    struct crossed_terminal_events seen = {0};
    executor_enter(c.r);
    crossed_terminal_poll(&c, &seen);
    if (pressure) REQUIRE(seen.found == 1 && seen.gone == 0);
    if (pressure == 2) {
        /* Direct core ingress has no bridge retry mark. Service only frees
         * the two real action slots before the explicit terminal refeed. */
        moq_msquic_conn_service(c.r->adapter);
        REQUIRE(moq_session_action_capacity(s) == 2);
    }
    if (repeat_fin) {
        REQUIRE(with_fin && pressure != 0);
        /* Re-presenting the retained FIN is not a fresh empty-stream close. */
        CHECK(moq_session_on_bidi_stream_bytes(s, c.ref, NULL, 0, true, test_now()) == MOQ_OK);
    }
    for (unsigned i = 0; i < 6; ++i) {
        if (pressure == 2 && s->ns_subs[slot].state != MOQ_NS_SUB_FREE)
            CHECK(moq_session_on_bidi_stream_bytes(s, c.ref, NULL, 0, false, test_now()) == MOQ_OK);
        moq_msquic_conn_service(c.r->adapter);
        crossed_terminal_poll(&c, &seen);
    }
    executor_leave(c.r);
    complete_sends(c.r);
    CHECK(seen.found == 1 && seen.gone == (with_fin ? 1u : 0u));
    CHECK(seen.migrations == 0 && seen.other == 0);
    CHECK(s->ns_subs[slot].state == (with_fin ? MOQ_NS_SUB_FREE : MOQ_NS_SUB_ESTABLISHED));
    CHECK(s->ns_subs[slot].generation == generation + (with_fin ? 1u : 0u));
    CHECK(c.stream->shutdown_calls == (with_fin ? 1 : 0));
    if (with_fin) {
        check_query(c.r, MOQ_DONE);
        struct moq_msq_stream *ms = moq_msquic_test_stream_find(c.r->adapter, c.stream->id);
        CHECK(ms != NULL && ms->fin_sent && !ms->fin_acked);
        if (ms && ms->fin_sent)
            stream_event(c.stream, QUIC_STREAM_EVENT_SEND_SHUTDOWN_COMPLETE, true);
    }
    check_query(c.r, MOQ_OK);
    executor_enter(c.r);
    for (unsigned i = 0; i < 3; ++i) {
        moq_msquic_conn_service(c.r->adapter);
        crossed_terminal_poll(&c, &seen);
    }
    executor_leave(c.r);
    CHECK(seen.found == 1 && seen.gone == (with_fin ? 1u : 0u));
    CHECK(seen.migrations == 0 && seen.other == 0);
    CHECK(s->ns_subs[slot].generation == generation + (with_fin ? 1u : 0u));
    CHECK(c.stream->shutdown_calls == (with_fin ? 1 : 0));
    CHECK(moq_session_state(s) == MOQ_SESS_ESTABLISHED);
    printf("CASE crossed pending FIN D%u pressure=%u FIN=%u repeat-FIN=%u found=%u gone=%u shutdowns=%d\n",
        (unsigned)version, pressure, with_fin, repeat_fin, seen.found, seen.gone, c.stream->shutdown_calls);
    crossed_destroy(&c);
    ++cases;
}

/* invalid: 0 duplicate GOAWAY, 1 malformed body, 2 truncated at split FIN. */
static void t_crossed_namespace_invalid(moq_version_t version, bool subscriber,
                                        unsigned invalid)
{
    struct crossed_namespace c = crossed_create(version, subscriber, false);
    fake_msq_send_t *send = crossed_goaway(&c);
    uint8_t bytes[256];
    REQUIRE(send->bytes_len > 1 && send->bytes_len * 2 <= sizeof(bytes));
    memcpy(bytes, send->bytes, send->bytes_len);
    size_t len = send->bytes_len;
    if (invalid == 0) {
        memcpy(bytes + len, bytes, len);
        len *= 2;
    } else if (invalid == 1) {
        moq_request_envelope_t env;
        size_t used = 0;
        REQUIRE(c.p->session->profile->request_codec->decode_envelope(bytes, len, &env, &used) == MOQ_OK);
        bytes[env.payload - bytes] = 63; /* URI length exceeds the framed body. */
    } else {
        --len;
    }
    fake_msq_deliver_receive(c.stream, bytes, len, false);
    if (invalid == 2) stream_event(c.stream, QUIC_STREAM_EVENT_PEER_SEND_SHUTDOWN, false);
    REQUIRE(fake_msq_deliver_send_complete(&c.p->fake, false));
    CHECK(moq_session_state(c.r->adapter->session) == MOQ_SESS_CLOSED);
    CHECK(moq_msquic_managed_poll_flush(c.r->m) == MOQ_ERR_CLOSED);
    crossed_destroy(&c);
    printf("CASE crossed invalid GOAWAY D%u subscriber=%u invalid=%u\n", (unsigned)version, subscriber, invalid);
    ++cases;
}

static void t_data_and_fin_blocked(moq_version_t version, bool block_open)
{
    struct rig *r = rig_create(version, false);
    struct peer *p = establish(r, version);
    moq_subscription_t sub = subscribe_sender(r, p);
    if (block_open) credit(r, 0);
    uint8_t *bytes = calloc(MOQ_MSQ_SEND_BUDGET_MIN, 1);
    REQUIRE(bytes != NULL);
    moq_rcbuf_t *payload;
    REQUIRE(moq_rcbuf_create(&r->alloc, bytes,
        block_open ? 7 : MOQ_MSQ_SEND_BUDGET_MIN, &payload) == MOQ_OK);
    free(bytes);
    executor_enter(r);
    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    moq_subgroup_handle_t sg;
    REQUIRE(moq_session_open_subgroup(r->adapter->session, sub, &sgc, test_now(), &sg) == MOQ_OK);
    REQUIRE(moq_session_write_object(r->adapter->session, sg, 0, payload, test_now()) == MOQ_OK);
    if (!block_open)
        REQUIRE(moq_session_write_object(r->adapter->session, sg, 1, payload, test_now()) == MOQ_OK);
    REQUIRE(moq_session_close_subgroup(r->adapter->session, sg, test_now()) == MOQ_OK);
    moq_msquic_conn_service(r->adapter);
    executor_leave(r);
    moq_rcbuf_decref(payload);
    CHECK(moq_transport_bridge_has_outbound_pending(r->adapter->bridge));
    check_query(r, MOQ_DONE);
    if (block_open) {
        CHECK(r->adapter->pending_sends == 0);
        credit(r, 8);
    }
    complete_sends(r); /* completion/credit retries bytes and the trailing FIN */
    CHECK(!moq_transport_bridge_has_outbound_pending(r->adapter->bridge));
    CHECK(r->adapter->pending_sends == 0);
    check_query(r, MOQ_DONE);
    fake_msq_stream_t *st = r->client->fake.sends[r->client->fake.send_count - 1].stream;
    stream_event(st, QUIC_STREAM_EVENT_SEND_SHUTDOWN_COMPLETE, true);
    check_query(r, MOQ_OK);
    peer_destroy(p);
    rig_destroy(r);
    printf("CASE data/FIN %s recovery D%u\n", block_open ? "credit" : "write", (unsigned)version);
    ++cases;
}

static void t_credit_and_stale(moq_version_t version)
{
    struct rig *r = rig_create(version, false);
    credit(r, 0);
    fake_mgd_deliver_connected(r->client, alpn(version));
    CHECK(moq_transport_bridge_has_outbound_pending(r->adapter->bridge));
    CHECK(moq_msquic_managed_pending_sends(r->m) == 0);
    CHECK(moq_msquic_managed_pending_datagrams(r->m) == 0);
    check_query(r, MOQ_DONE);
    credit(r, 8);
    CHECK(!moq_transport_bridge_has_outbound_pending(r->adapter->bridge));
    CHECK(r->adapter->pending_sends > 0);
    CHECK(moq_msquic_managed_pending_sends(r->m) == 0); /* stale empty mirror */
    check_query(r, MOQ_DONE);
    REQUIRE(moq_msq_test_lane_step(r->lane) == MOQ_MSQ_TEST_STEP_PUMPED);
    CHECK(moq_msquic_managed_pending_sends(r->m) > 0);
    complete_sends(r);
    CHECK(moq_msquic_managed_pending_sends(r->m) > 0); /* stale nonempty mirror */
    CHECK(moq_msquic_managed_conn_count(r->m) == 1);
    check_query(r, MOQ_OK);
    CHECK(r->pumps > 0 && r->activities > 0);
    rig_destroy(r);
    printf("CASE credit/stale/empty D%u\n", (unsigned)version);
    ++cases;
}

static void t_staging_fin(moq_version_t version, unsigned kind, bool pure_fin)
{
    struct rig *r = rig_create(version, false);
    const moq_transport_endpoint_ops_t *ops = moq_msquic_test_ops(r->adapter);
    uint64_t id = UINT64_MAX;
    bool bidi = kind != 0;
    fake_msq_stream_t *st = NULL;
    if (kind == 2) {
        id = 1; /* server-initiated bidi, created by the provider callback */
        st = relay_stream(&r->client->fake, r->adapter, id, false);
    }
    executor_enter(r);
    if (kind != 2)
        REQUIRE((bidi ? ops->open_bidi(r->adapter, &id) : ops->open_uni(r->adapter, &id)) == MOQ_TRANSPORT_OK);
    if (!pure_fin)
        REQUIRE(ops->write(r->adapter, id, (const uint8_t *)"bytes", 5, false) == MOQ_TRANSPORT_OK);
    executor_leave(r);
    if (!st) st = fake_msq_stream_at(&r->client->fake, 0);
    REQUIRE(st != NULL);
    if (!pure_fin) {
        CHECK(r->adapter->building != NULL && r->adapter->pending_sends == 0);
        check_query(r, MOQ_DONE);
        executor_enter(r);
        REQUIRE(ops->write(r->adapter, id, (const uint8_t *)"tail", 4, true) == MOQ_TRANSPORT_OK);
        executor_leave(r);
        check_query(r, MOQ_DONE);
        complete_sends(r);
    } else {
        executor_enter(r);
        REQUIRE(ops->write(r->adapter, id, NULL, 0, true) == MOQ_TRANSPORT_OK);
        executor_leave(r);
    }
    CHECK(r->adapter->pending_sends == 0);
    check_query(r, MOQ_DONE); /* byte completion is not FIN completion */
    stream_event(st, QUIC_STREAM_EVENT_SEND_SHUTDOWN_COMPLETE, true);
    CHECK(moq_msquic_test_stream_find(r->adapter, id)->fin_acked);
    check_query(r, MOQ_OK);
    stream_event(st, QUIC_STREAM_EVENT_SEND_SHUTDOWN_COMPLETE, true);
    check_query(r, MOQ_OK);
    rig_destroy(r);
    printf("CASE staging/FIN D%u kind=%u pure=%u\n", (unsigned)version, kind, pure_fin);
    ++cases;
}

static void t_unconfirmed_fin(bool aborted)
{
    struct rig *r = rig_create(MOQ_VERSION_DRAFT_22, false);
    const moq_transport_endpoint_ops_t *ops = moq_msquic_test_ops(r->adapter);
    uint64_t id;
    executor_enter(r);
    REQUIRE(ops->open_bidi(r->adapter, &id) == MOQ_TRANSPORT_OK);
    REQUIRE(ops->write(r->adapter, id, NULL, 0, true) == MOQ_TRANSPORT_OK);
    executor_leave(r);
    fake_msq_stream_t *st = fake_msq_stream_at(&r->client->fake, 0);
    if (aborted) {
        stream_event(st, QUIC_STREAM_EVENT_SEND_SHUTDOWN_COMPLETE, false);
        stream_event(st, QUIC_STREAM_EVENT_SEND_SHUTDOWN_COMPLETE, true);
        CHECK(!moq_msquic_test_stream_find(r->adapter, id)->fin_acked);
        check_query(r, MOQ_ERR_INTERNAL);
    }
    stream_event(st, QUIC_STREAM_EVENT_SHUTDOWN_COMPLETE, false);
    CHECK(moq_msquic_test_stream_find(r->adapter, id) == NULL);
    check_query(r, MOQ_ERR_INTERNAL); /* Retirement cannot repair a lost FIN. */
    CHECK(!moq_msquic_managed_is_closed(r->m));
    CHECK(!moq_msquic_managed_is_fatal(r->m));
    rig_destroy(r);
    ++cases;
}

static void *pump_thread(void *arg)
{
    struct rig *r = arg;
    REQUIRE(moq_msq_test_lane_step(r->lane) == MOQ_MSQ_TEST_STEP_PUMPED);
    return NULL;
}

static void t_pump_contention(void)
{
    struct rig *r = rig_create(MOQ_VERSION_DRAFT_22, false);
    struct gate g;
    gate_init(&g);
    r->pump_gate = &g;
    REQUIRE(moq_msquic_managed_wake(r->m) == MOQ_OK);
    pthread_t thread;
    REQUIRE(pthread_create(&thread, NULL, pump_thread, r) == 0);
    gate_wait(&g, true);
    CHECK(moq_msquic_managed_poll_flush(r->m) == MOQ_DONE);
    gate_set(&g, false);
    REQUIRE(pthread_join(thread, NULL) == 0);
    r->pump_gate = NULL;
    check_query(r, MOQ_OK);
    gate_destroy(&g);
    rig_destroy(r);
    ++cases;
}

static struct rig *race_rig;
static struct gate observed_gate, stop_gate;
extern void (*moq_msq_test_stop_latched)(moq_msquic_managed_t *m);

static void terminal_before_lock(moq_msquic_managed_t *m)
{
    REQUIRE(m == race_rig->m);
    fake_mgd_deliver_peer_close(race_rig->client, 0);
}

static void query_observed(moq_msquic_managed_t *m)
{
    REQUIRE(m == race_rig->m);
    gate_set(&observed_gate, true);
    gate_wait(&observed_gate, false);
}

static void stop_latched(moq_msquic_managed_t *m)
{
    REQUIRE(m == race_rig->m);
    gate_set(&stop_gate, true);
    gate_wait(&stop_gate, false);
}

static void *query_thread(void *arg)
{
    moq_result_t *result = arg;
    *result = moq_msquic_managed_poll_flush(race_rig->m);
    return NULL;
}

static void *stop_thread(void *arg)
{
    moq_result_t *result = arg;
    *result = moq_msquic_managed_stop(race_rig->m);
    return NULL;
}

static void t_terminal_stop_races(void)
{
    struct rig *r = rig_create(MOQ_VERSION_DRAFT_22, false);
    race_rig = r;
    check_query(r, MOQ_OK);
    moq_msq_test_flush_pre_lock = terminal_before_lock;
    CHECK(moq_msquic_managed_poll_flush(r->m) == MOQ_ERR_CLOSED);
    moq_msq_test_flush_pre_lock = NULL;
    CHECK(!moq_msquic_managed_is_closed(r->m)); /* mirror awaits SHUTDOWN_COMPLETE */
    CHECK(moq_msquic_managed_poll_flush(r->m) == MOQ_ERR_CLOSED);
    rig_destroy(r);

    r = rig_create(MOQ_VERSION_DRAFT_22, false);
    race_rig = r;
    gate_init(&observed_gate);
    gate_init(&stop_gate);
    moq_msq_test_flush_observed = query_observed;
    moq_msq_test_stop_latched = stop_latched;
    moq_result_t qr = MOQ_ERR_INTERNAL, sr = MOQ_ERR_INTERNAL;
    pthread_t query, stop;
    REQUIRE(pthread_create(&query, NULL, query_thread, &qr) == 0);
    gate_wait(&observed_gate, true); /* query already observed an empty lane */
    REQUIRE(pthread_create(&stop, NULL, stop_thread, &sr) == 0);
    gate_wait(&stop_gate, true); /* stop atomically latched; query still holds lane */
    CHECK(moq_msquic_managed_poll_flush(r->m) == MOQ_ERR_CLOSED);
    gate_set(&observed_gate, false);
    REQUIRE(pthread_join(query, NULL) == 0);
    CHECK(qr == MOQ_ERR_CLOSED);
    moq_msq_test_flush_observed = NULL;
    fake_mgd_deliver_shutdown_complete(r->client);
    gate_set(&stop_gate, false);
    REQUIRE(pthread_join(stop, NULL) == 0);
    CHECK(sr == MOQ_OK);
    CHECK(moq_msquic_managed_poll_flush(r->m) == MOQ_ERR_CLOSED);
    moq_msq_test_stop_latched = NULL;
    gate_destroy(&observed_gate);
    gate_destroy(&stop_gate);
    rig_destroy(r);
    race_rig = NULL;
    ++cases;
}

static void t_datagram_and_reset(void)
{
    struct rig *r = rig_create(MOQ_VERSION_DRAFT_22, false);
    QUIC_CONNECTION_EVENT ev = { .Type = QUIC_CONNECTION_EVENT_DATAGRAM_STATE_CHANGED };
    ev.DATAGRAM_STATE_CHANGED.SendEnabled = TRUE;
    ev.DATAGRAM_STATE_CHANGED.MaxSendLength = 1200;
    connection_event(r, &ev);
    const moq_transport_endpoint_ops_t *ops = moq_msquic_test_ops(r->adapter);
    executor_enter(r);
    REQUIRE(ops->send_datagram(r->adapter, (const uint8_t *)"datagram", 8) == MOQ_TRANSPORT_OK);
    executor_leave(r);
    CHECK(moq_msquic_managed_pending_datagrams(r->m) == 0);
    check_query(r, MOQ_DONE);
    memset(&ev, 0, sizeof(ev));
    ev.Type = QUIC_CONNECTION_EVENT_DATAGRAM_SEND_STATE_CHANGED;
    ev.DATAGRAM_SEND_STATE_CHANGED.ClientContext = fake_msq_next_dgram_ctx(&r->client->fake);
    REQUIRE(ev.DATAGRAM_SEND_STATE_CHANGED.ClientContext != NULL);
    ev.DATAGRAM_SEND_STATE_CHANGED.State = QUIC_DATAGRAM_SEND_ACKNOWLEDGED;
    connection_event(r, &ev);
    check_query(r, MOQ_OK);

    executor_enter(r);
    REQUIRE(ops->send_datagram(r->adapter, (const uint8_t *)"datagram", 8) == MOQ_TRANSPORT_OK);
    executor_leave(r);
    check_query(r, MOQ_DONE);
    ev.DATAGRAM_SEND_STATE_CHANGED.ClientContext = fake_msq_next_dgram_ctx(&r->client->fake);
    REQUIRE(ev.DATAGRAM_SEND_STATE_CHANGED.ClientContext != NULL);
    ev.DATAGRAM_SEND_STATE_CHANGED.State = QUIC_DATAGRAM_SEND_CANCELED;
    connection_event(r, &ev);
    check_query(r, MOQ_OK); /* A lossy datagram cancellation is not reliable loss. */

    uint64_t id;
    executor_enter(r);
    REQUIRE(ops->open_bidi(r->adapter, &id) == MOQ_TRANSPORT_OK);
    REQUIRE(ops->write(r->adapter, id, NULL, 0, true) == MOQ_TRANSPORT_OK);
    REQUIRE(ops->reset_stream(r->adapter, id, 1) == MOQ_TRANSPORT_OK);
    executor_leave(r);
    fake_msq_stream_t *st = fake_msq_stream_at(&r->client->fake, 0);
    check_query(r, MOQ_DONE); /* Reset accepted, no shutdown completion yet. */
    stream_event(st, QUIC_STREAM_EVENT_SEND_SHUTDOWN_COMPLETE, true);
    CHECK(!moq_msquic_test_stream_find(r->adapter, id)->fin_acked);
    check_query(r, MOQ_OK); /* Either shutdown kind settles; not delivered FIN. */
    int shutdowns = st->shutdown_calls;
    executor_enter(r);
    REQUIRE(ops->reset_stream(r->adapter, id, 1) == MOQ_TRANSPORT_OK);
    executor_leave(r);
    CHECK(st->shutdown_calls == shutdowns);
    check_query(r, MOQ_OK); /* No second completion is owed by an idempotent reset. */
    stream_event(st, QUIC_STREAM_EVENT_SHUTDOWN_COMPLETE, false);
    check_query(r, MOQ_OK);
    executor_enter(r);
    REQUIRE(ops->reset_stream(r->adapter, id, 1) == MOQ_TRANSPORT_OK);
    executor_leave(r);
    CHECK(st->shutdown_calls == shutdowns);
    check_query(r, MOQ_OK);
    rig_destroy(r);
    ++cases;
}

static void t_failed_send(void)
{
    struct rig *r = rig_create(MOQ_VERSION_DRAFT_22, false);
    const moq_transport_endpoint_ops_t *ops = moq_msquic_test_ops(r->adapter);
    r->client->fake.stream_send_fails = 1;
    uint64_t id;
    executor_enter(r);
    REQUIRE(ops->open_bidi(r->adapter, &id) == MOQ_TRANSPORT_OK);
    REQUIRE(ops->write(r->adapter, id, (const uint8_t *)"fail", 4, true) == MOQ_TRANSPORT_ERROR);
    executor_leave(r);
    CHECK(r->adapter->flush_failed);
    CHECK(r->adapter->pending_sends == 0);
    check_query(r, MOQ_ERR_CLOSED);
    executor_enter(r);
    moq_msquic_conn_service(r->adapter);
    executor_leave(r);
    CHECK(moq_msquic_conn_is_fatal(r->adapter));
    check_query(r, MOQ_ERR_CLOSED);
    rig_destroy(r);
    ++cases;
}

/* The retained-payload probe doubles as a settlement oracle. The
 * provider's downcalls/events and the public core commands supply every fact;
 * private stream fields are inspected, never assigned. */
static const uint8_t settlement_bytes[] = "msq-cancel-probe";

struct payload_probe {
    moq_subgroup_handle_t subgroup;
    fake_msq_send_t *send;
    uint64_t id;
    unsigned releases;
};

static void payload_released(void *ctx, const uint8_t *data, size_t len)
{
    struct payload_probe *p = ctx;
    CHECK(data == settlement_bytes && len == sizeof(settlement_bytes) - 1);
    CHECK(++p->releases == 1);
}

static moq_rcbuf_t *payload_buffer(struct rig *r, struct payload_probe *p)
{
    moq_rcbuf_t *buf = NULL;
    REQUIRE(moq_rcbuf_wrap(&r->alloc, settlement_bytes, sizeof(settlement_bytes) - 1,
                           payload_released, p, &buf) == MOQ_OK);
    return buf;
}

static void payload_observe(struct rig *r, struct payload_probe *p, int first, bool fin)
{
    REQUIRE(r->client->fake.send_count == first + 1);
    p->send = &r->client->fake.sends[first];
    p->id = p->send->stream->id;
    struct moq_msq_stream *ms = moq_msquic_test_stream_find(r->adapter, p->id);
    REQUIRE(ms != NULL && !p->send->completed && p->send->client_ctx != NULL);
    REQUIRE(p->send->bytes_len == p->send->total);
    REQUIRE(p->send->bytes_len >= sizeof(settlement_bytes) - 1);
    REQUIRE(memcmp(p->send->bytes + p->send->bytes_len - (sizeof(settlement_bytes) - 1),
                   settlement_bytes, sizeof(settlement_bytes) - 1) == 0);
    REQUIRE(ms->fin_sent == fin && !ms->reset_sent && !ms->send_canceled);
    REQUIRE(ms->acked_total == 0 && ms->inflight_bytes >= p->send->total);
    REQUIRE(r->adapter->pending_sends != 0 && p->releases == 0);
}

static void payload_start(struct rig *r, moq_subscription_t sub, uint64_t group,
                           bool fin, struct payload_probe *p)
{
    memset(p, 0, sizeof(*p));
    moq_rcbuf_t *buf = payload_buffer(r, p);
    int first = r->client->fake.send_count;
    executor_enter(r);
    moq_subgroup_cfg_t cfg;
    moq_subgroup_cfg_init(&cfg);
    cfg.group_id = group;
    REQUIRE(moq_session_open_subgroup(r->adapter->session, sub, &cfg,
                                      test_now(), &p->subgroup) == MOQ_OK);
    REQUIRE(moq_session_write_object(r->adapter->session, p->subgroup, 0,
                                     buf, test_now()) == MOQ_OK);
    if (fin)
        REQUIRE(moq_session_close_subgroup(r->adapter->session, p->subgroup, test_now()) == MOQ_OK);
    moq_msquic_conn_service(r->adapter);
    REQUIRE(moq_rcbuf_refcount(buf) == 2); /* caller plus accepted provider batch */
    executor_leave(r);
    moq_rcbuf_decref(buf);
    payload_observe(r, p, first, fin);
}

static void payload_checkpoint(struct rig *r, const struct payload_probe *p,
                                const char *phase, moq_result_t expected)
{
    const struct moq_msq_stream *ms = moq_msquic_test_stream_find(r->adapter, p->id);
    printf("SETTLEMENT stream=%llu phase=%s pending=%u releases=%u acked=%llu reset=%u query=%d expected=%d\n",
           (unsigned long long)p->id, phase, (unsigned)r->adapter->pending_sends,
           p->releases, (unsigned long long)(ms ? ms->acked_total : 0),
           (unsigned)(ms && ms->reset_sent), moq_msquic_managed_poll_flush(r->m), expected);
    check_query(r, expected);
}

static void payload_live(struct rig *r)
{
    CHECK(moq_session_state(r->adapter->session) == MOQ_SESS_ESTABLISHED);
    CHECK(!moq_msquic_managed_is_closed(r->m) && !moq_msquic_managed_is_fatal(r->m));
    CHECK(!moq_msquic_conn_is_fatal(r->adapter) && !r->adapter->flush_failed);
    CHECK(r->client->shutdowns == 0);
}

static void payload_finish_shutdown(struct rig *r, struct payload_probe *p,
                                     moq_result_t expected)
{
    fake_msq_stream_t *st = p->send->stream;
    REQUIRE(p->releases == 1 && p->send->completed);
    /* The provider can return canceled bytes before a unique graceful event
     * when FIN ACK wins the reset race. No second abortive callback is owed. */
    bool graceful = (p->send->flags & QUIC_SEND_FLAG_FIN) != 0;
    stream_event(st, QUIC_STREAM_EVENT_SEND_SHUTDOWN_COMPLETE, graceful);
    CHECK(!moq_msquic_test_stream_find(r->adapter, p->id)->fin_acked);
    payload_checkpoint(r, p, graceful ? "graceful-shutdown-complete" : "abort-complete", expected);
    stream_event(st, QUIC_STREAM_EVENT_SHUTDOWN_COMPLETE, false);
    CHECK(moq_msquic_test_stream_find(r->adapter, p->id) == NULL);
    CHECK(st->closed && p->releases == 1);
    payload_checkpoint(r, p, "retired", expected);
}

static void payload_later_group(struct rig *r, moq_subscription_t sub,
                                 const struct payload_probe *old, moq_result_t expected)
{
    struct payload_probe next;
    payload_start(r, sub, 1, true, &next);
    CHECK(next.id != old->id);
    CHECK(next.subgroup._opaque != old->subgroup._opaque);
    CHECK(moq_handle_slot(next.subgroup._opaque) == moq_handle_slot(old->subgroup._opaque));
    payload_checkpoint(r, &next, "reused-core-slot-pending",
                       expected == MOQ_OK ? MOQ_DONE : expected);
    REQUIRE(fake_msq_deliver_send_complete(&r->client->fake, false));
    CHECK(next.releases == 1 && r->adapter->pending_sends == 0);
    payload_checkpoint(r, &next, "unrelated-bytes-complete",
                       expected == MOQ_OK ? MOQ_DONE : expected);
    stream_event(next.send->stream, QUIC_STREAM_EVENT_SEND_SHUTDOWN_COMPLETE, true);
    CHECK(moq_msquic_test_stream_find(r->adapter, next.id)->fin_acked);
    payload_checkpoint(r, &next, "unrelated-FIN-complete", expected);
    stream_event(next.send->stream, QUIC_STREAM_EVENT_SHUTDOWN_COMPLETE, false);
    payload_checkpoint(r, &next, "unrelated-retired", expected);
    payload_live(r);
}

enum payload_mode { PAYLOAD_NORMAL, PAYLOAD_UNEXPLAINED, PAYLOAD_APP_RESET, PAYLOAD_PEER_STOP };

static void t_payload_settlement(moq_version_t version, enum payload_mode mode, bool fin)
{
    printf("CASE payload settlement D%u mode=%u FIN=%u\n",
           (unsigned)version, (unsigned)mode, (unsigned)fin);
    struct rig *r = rig_create(version, false);
    struct peer *peer = establish(r, version);
    moq_subscription_t sub = subscribe_sender(r, peer);
    struct payload_probe p;
    payload_start(r, sub, 0, fin, &p);
    struct moq_msq_stream *ms = moq_msquic_test_stream_find(r->adapter, p.id);
    fake_msq_stream_t *st = p.send->stream;
    uint64_t commits = r->adapter->test_stream_commit_reports;
    REQUIRE(r->adapter->pending_sends == 1 && ms->sent_total == p.send->total);
    payload_checkpoint(r, &p, "accepted", MOQ_DONE);
    if (mode == PAYLOAD_APP_RESET) {
        REQUIRE(!fin);
        executor_enter(r);
        REQUIRE(moq_session_reset_subgroup(r->adapter->session, p.subgroup, 1, test_now()) == MOQ_OK);
        executor_leave(r);
        CHECK(!ms->reset_sent && st->shutdown_calls == 0 && p.releases == 0);
        payload_checkpoint(r, &p, "reset-queued-only", MOQ_DONE);
        executor_enter(r);
        moq_msquic_conn_service(r->adapter);
        executor_leave(r);
    } else if (mode == PAYLOAD_PEER_STOP) {
        stream_event(st, QUIC_STREAM_EVENT_PEER_RECEIVE_ABORTED, false);
    }
    if (mode == PAYLOAD_APP_RESET || mode == PAYLOAD_PEER_STOP) {
        REQUIRE(ms->reset_sent && st->shutdown_calls == 1);
        REQUIRE(st->last_shutdown_flags == QUIC_STREAM_SHUTDOWN_FLAG_ABORT_SEND);
        CHECK(p.releases == 0 && r->adapter->pending_sends == 1);
        payload_checkpoint(r, &p, "reset-accepted-borrow-held", MOQ_DONE);
    }
    REQUIRE(fake_msq_deliver_send_complete(&r->client->fake, mode != PAYLOAD_NORMAL));
    REQUIRE(!fake_msq_deliver_send_complete(&r->client->fake, false));
    CHECK(p.releases == 1 && r->adapter->pending_sends == 0 && ms->inflight_bytes == 0);
    CHECK(ms->acked_total == (mode == PAYLOAD_NORMAL ? ms->sent_total : 0));
    CHECK(ms->send_canceled == (mode != PAYLOAD_NORMAL));
    CHECK(r->adapter->test_stream_commit_reports == commits);
    payload_live(r);
    if (mode == PAYLOAD_NORMAL) {
        payload_checkpoint(r, &p, "normal-byte-complete", fin ? MOQ_DONE : MOQ_OK);
        if (fin) {
            stream_event(st, QUIC_STREAM_EVENT_SEND_SHUTDOWN_COMPLETE, true);
            CHECK(ms->fin_acked && r->adapter->test_stream_commit_reports == commits + 1);
            payload_checkpoint(r, &p, "normal-FIN-complete", MOQ_OK);
        }
    } else if (mode == PAYLOAD_UNEXPLAINED) {
        payload_checkpoint(r, &p, "unexplained-cancel", MOQ_ERR_INTERNAL);
        if (fin) {
            stream_event(st, QUIC_STREAM_EVENT_PEER_RECEIVE_ABORTED, false);
        } else {
            executor_enter(r);
            REQUIRE(moq_session_reset_subgroup(r->adapter->session, p.subgroup, 1, test_now()) == MOQ_OK);
            moq_msquic_conn_service(r->adapter);
            executor_leave(r);
        }
        REQUIRE(ms->reset_sent && st->last_shutdown_flags == QUIC_STREAM_SHUTDOWN_FLAG_ABORT_SEND);
        payload_checkpoint(r, &p, "later-reset-cannot-repair", MOQ_ERR_INTERNAL);
        payload_finish_shutdown(r, &p, MOQ_ERR_INTERNAL);
        CHECK(r->adapter->test_stream_commit_reports == commits);
        payload_later_group(r, sub, &p, MOQ_ERR_INTERNAL);
        fake_mgd_deliver_peer_close(r->client, 0);
        payload_checkpoint(r, &p, "actual-terminal-precedence", MOQ_ERR_CLOSED);
    } else {
        payload_checkpoint(r, &p, "borrow-released-abort-owed", MOQ_DONE);
        payload_finish_shutdown(r, &p, MOQ_OK);
        CHECK(r->adapter->test_stream_commit_reports == commits);
        payload_later_group(r, sub, &p, MOQ_OK);
    }
    peer_destroy(peer);
    rig_destroy(r);
    CHECK(p.releases == 1);
    ++cases;
}

static void t_payload_reset_refused(moq_version_t version)
{
    printf("CASE refused public payload reset D%u\n", (unsigned)version);
    struct rig *r = rig_create(version, false);
    struct peer *peer = establish(r, version);
    moq_subscription_t sub = subscribe_sender(r, peer);
    struct payload_probe p;
    payload_start(r, sub, 0, false, &p);
    r->client->fake.stream_shutdown_status = QUIC_STATUS_OUT_OF_MEMORY;
    executor_enter(r);
    REQUIRE(moq_session_reset_subgroup(r->adapter->session, p.subgroup, 1, test_now()) == MOQ_OK);
    moq_msquic_conn_service(r->adapter);
    executor_leave(r);
    CHECK(p.send->stream->shutdown_calls == 1);
    CHECK(!moq_msquic_test_stream_find(r->adapter, p.id)->reset_sent);
    CHECK(p.releases == 0 && r->adapter->pending_sends == 1);
    CHECK(moq_transport_bridge_is_fatal(r->adapter->bridge));
    payload_checkpoint(r, &p, "native-reset-refused-real-fatal", MOQ_ERR_CLOSED);
    REQUIRE(fake_msq_deliver_send_complete(&r->client->fake, true));
    CHECK(p.releases == 1 && r->adapter->pending_sends == 0);
    payload_checkpoint(r, &p, "refusal-cancel-complete", MOQ_ERR_CLOSED);
    peer_destroy(peer);
    rig_destroy(r);
    CHECK(p.releases == 1);
    ++cases;
}

static void t_receive_half_not_withdrawal(moq_version_t version, bool peer_reset, bool fin)
{
    printf("CASE receive-only payload control D%u peer-reset=%u FIN=%u\n",
           (unsigned)version, (unsigned)peer_reset, (unsigned)fin);
    struct rig *r = rig_create(version, false);
    struct peer *peer = establish(r, version);
    const moq_transport_endpoint_ops_t *ops = moq_msquic_test_ops(r->adapter);
    struct payload_probe p = {0};
    moq_rcbuf_t *buf = payload_buffer(r, &p);
    int first = r->client->fake.send_count;
    executor_enter(r);
    REQUIRE(ops->open_bidi(r->adapter, &p.id) == MOQ_TRANSPORT_OK);
    REQUIRE(ops->write_payload(r->adapter, p.id, buf, fin) == MOQ_TRANSPORT_OK);
    moq_msquic_conn_service(r->adapter);
    REQUIRE(moq_rcbuf_refcount(buf) == 2);
    executor_leave(r);
    moq_rcbuf_decref(buf);
    payload_observe(r, &p, first, fin);
    REQUIRE(version != MOQ_VERSION_DRAFT_16 || p.id != r->adapter->ctrl_id);
    if (peer_reset) {
        stream_event(p.send->stream, QUIC_STREAM_EVENT_PEER_SEND_ABORTED, false);
    } else {
        executor_enter(r);
        REQUIRE(ops->stop_sending(r->adapter, p.id, 1) == MOQ_TRANSPORT_OK);
        executor_leave(r);
        CHECK(p.send->stream->last_shutdown_flags == QUIC_STREAM_SHUTDOWN_FLAG_ABORT_RECEIVE);
    }
    CHECK(!moq_msquic_test_stream_find(r->adapter, p.id)->reset_sent && p.releases == 0);
    payload_checkpoint(r, &p, "receive-half-only", MOQ_DONE);
    REQUIRE(fake_msq_deliver_send_complete(&r->client->fake, true));
    CHECK(p.releases == 1 && r->adapter->pending_sends == 0);
    payload_checkpoint(r, &p, "receive-half-cannot-explain-send-loss", MOQ_ERR_INTERNAL);
    payload_live(r);
    peer_destroy(peer);
    rig_destroy(r);
    CHECK(p.releases == 1);
    ++cases;
}

static void t_control_output_loss(moq_version_t version, unsigned mode)
{
    printf("CASE critical SETUP output D%u mode=%u\n", (unsigned)version, mode);
    struct rig *r = rig_create(version, false);
    fake_mgd_deliver_connected(r->client, alpn(version));
    REQUIRE(r->client->fake.send_count == 1 && r->adapter->pending_sends == 1);
    fake_msq_send_t *send = &r->client->fake.sends[0];
    REQUIRE(send->total != 0 && !send->completed);
    fake_msq_stream_t *st = send->stream;
    struct moq_msq_stream *ms = moq_msquic_test_stream_find(r->adapter, st->id);
    REQUIRE(ms != NULL && ms->sent_total == send->total && ms->acked_total == 0);
    if (mode == 1) {
        const moq_transport_endpoint_ops_t *ops = moq_msquic_test_ops(r->adapter);
        executor_enter(r);
        REQUIRE(ops->reset_stream(r->adapter, st->id, 1) == MOQ_TRANSPORT_OK);
        executor_leave(r);
        REQUIRE(ms->reset_sent && st->last_shutdown_flags == QUIC_STREAM_SHUTDOWN_FLAG_ABORT_SEND);
    } else if (mode == 2) {
        stream_event(st, QUIC_STREAM_EVENT_PEER_RECEIVE_ABORTED, false);
    }
    /* A peer STOP of a session's control stream closes the session in the
     * real bridge on every draft: that stream must stay open for the
     * session's lifetime. Control output lost any other way is reported; it
     * is never exempt. */
    bool terminal = mode == 2;
    check_query(r, terminal ? MOQ_ERR_CLOSED : MOQ_DONE);
    REQUIRE(fake_msq_deliver_send_complete(&r->client->fake, true));
    CHECK(ms->acked_total == 0 && ms->send_canceled && r->adapter->pending_sends == 0);
    check_query(r, terminal ? MOQ_ERR_CLOSED : MOQ_ERR_INTERNAL);
    stream_event(st, QUIC_STREAM_EVENT_SEND_SHUTDOWN_COMPLETE, false);
    check_query(r, terminal ? MOQ_ERR_CLOSED : MOQ_ERR_INTERNAL);
    stream_event(st, QUIC_STREAM_EVENT_SHUTDOWN_COMPLETE, false);
    check_query(r, terminal ? MOQ_ERR_CLOSED : MOQ_ERR_INTERNAL);
    if (!terminal) {
        CHECK(!moq_msquic_managed_is_closed(r->m) && !moq_msquic_managed_is_fatal(r->m));
        CHECK(r->client->shutdowns == 0);
    }
    fake_mgd_deliver_peer_close(r->client, 0);
    check_query(r, MOQ_ERR_CLOSED);
    rig_destroy(r);
    ++cases;
}

static void t_payload_reliable_prefix(moq_version_t version, bool cancel_prefix)
{
    printf("CASE reliable-prefix settlement D%u canceled-prefix=%u\n",
           (unsigned)version, (unsigned)cancel_prefix);
    struct rig *r = rig_create(version, false);
    struct peer *peer = establish(r, version);
    moq_subscription_t sub = subscribe_sender(r, peer);
    QUIC_CONNECTION_EVENT negotiated = { .Type = QUIC_CONNECTION_EVENT_RELIABLE_RESET_NEGOTIATED };
    negotiated.RELIABLE_RESET_NEGOTIATED.IsNegotiated = TRUE;
    connection_event(r, &negotiated);
    REQUIRE(r->adapter->reliable_reset);
    struct payload_probe p = {0};
    int header_at = r->client->fake.send_count;
    executor_enter(r);
    moq_subgroup_cfg_t cfg;
    moq_subgroup_cfg_init(&cfg);
    REQUIRE(moq_session_open_subgroup(r->adapter->session, sub, &cfg, test_now(), &p.subgroup) == MOQ_OK);
    moq_msquic_conn_service(r->adapter);
    executor_leave(r);
    REQUIRE(r->client->fake.send_count == header_at + 1);
    fake_msq_send_t *header = &r->client->fake.sends[header_at];
    REQUIRE(header->total != 0 && !header->completed);
    moq_rcbuf_t *buf = payload_buffer(r, &p);
    int first = r->client->fake.send_count;
    executor_enter(r);
    REQUIRE(moq_session_write_object(r->adapter->session, p.subgroup, 0, buf, test_now()) == MOQ_OK);
    moq_msquic_conn_service(r->adapter);
    REQUIRE(moq_rcbuf_refcount(buf) == 2);
    executor_leave(r);
    moq_rcbuf_decref(buf);
    payload_observe(r, &p, first, false);
    REQUIRE(header->stream == p.send->stream && r->adapter->pending_sends == 2);
    executor_enter(r);
    REQUIRE(moq_session_reset_subgroup(r->adapter->session, p.subgroup, 1, test_now()) == MOQ_OK);
    moq_msquic_conn_service(r->adapter);
    executor_leave(r);
    CHECK(moq_transport_bridge_has_outbound_pending(r->adapter->bridge));
    CHECK(p.send->stream->shutdown_calls == 0 && p.releases == 0);
    payload_checkpoint(r, &p, "prefix-WOULD_BLOCK-no-withdrawal", MOQ_DONE);
    REQUIRE(fake_msq_deliver_send_complete(&r->client->fake, cancel_prefix));
    struct moq_msq_stream *ms = moq_msquic_test_stream_find(r->adapter, p.id);
    REQUIRE(header->completed && !p.send->completed && ms->reset_sent);
    CHECK(ms->acked_total == (cancel_prefix ? 0 : header->total));
    CHECK(p.send->stream->shutdown_calls == 1 && p.releases == 0 && r->adapter->pending_sends == 1);
    CHECK(r->adapter->reliable_resets == (cancel_prefix ? 0u : 1u));
    CHECK(r->client->fake.stream_setparam_calls == 0);
    payload_checkpoint(r, &p, cancel_prefix ? "loss-before-fallback-reset" : "prefix-ACK-reset-accepted",
                       cancel_prefix ? MOQ_ERR_INTERNAL : MOQ_DONE);
    REQUIRE(fake_msq_deliver_send_complete(&r->client->fake, true));
    CHECK(p.releases == 1 && r->adapter->pending_sends == 0);
    payload_checkpoint(r, &p, "suffix-borrow-released", cancel_prefix ? MOQ_ERR_INTERNAL : MOQ_DONE);
    payload_finish_shutdown(r, &p, cancel_prefix ? MOQ_ERR_INTERNAL : MOQ_OK);
    CHECK(r->adapter->test_stream_commit_reports == 0);
    payload_live(r);
    peer_destroy(peer);
    rig_destroy(r);
    CHECK(p.releases == 1);
    ++cases;
}

static void t_payload_core_timeout(moq_version_t version, bool subgroup_timeout)
{
    printf("CASE core %s timeout settlement D%u\n",
           subgroup_timeout ? "subgroup" : "object", (unsigned)version);
    uint64_t saved_clock = clock_us;
    struct rig *r = rig_create(version, false);
    struct peer *peer = establish(r, version);
    uint8_t properties[16];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, properties, sizeof(properties));
    moq_buf_write_vi64(&w, subgroup_timeout ? 0x06 : 0x02);
    moq_buf_write_vi64(&w, 10); /* Track Property: ten milliseconds. */
    moq_subscription_t sub = subscribe_sender_properties(r, peer,
        (moq_bytes_t){properties, moq_buf_writer_offset(&w)});
    struct payload_probe p, expired = {0};
    payload_start(r, sub, 0, subgroup_timeout, &p);
    if (!subgroup_timeout) {
        moq_rcbuf_t *buf = payload_buffer(r, &expired);
        executor_enter(r);
        REQUIRE(moq_session_write_object(r->adapter->session, p.subgroup, 1, buf, test_now()) == MOQ_OK);
        executor_leave(r);
        moq_rcbuf_decref(buf);
        REQUIRE(expired.releases == 0);
    }
    clock_us += 10000;
    executor_enter(r);
    REQUIRE(moq_session_tick(r->adapter->session, test_now()) == MOQ_OK);
    executor_leave(r);
    CHECK(p.send->stream->shutdown_calls == 0 && p.releases == 0);
    CHECK(expired.releases == (subgroup_timeout ? 0u : 1u));
    CHECK(session_has_outbound_obligation(r->adapter->session));
    payload_checkpoint(r, &p, "timeout-reset-queued", MOQ_DONE);
    executor_enter(r);
    moq_msquic_conn_service(r->adapter);
    executor_leave(r);
    REQUIRE(moq_msquic_test_stream_find(r->adapter, p.id)->reset_sent);
    REQUIRE(p.send->stream->shutdown_calls == 1);
    CHECK(p.send->stream->last_shutdown_flags == QUIC_STREAM_SHUTDOWN_FLAG_ABORT_SEND);
    CHECK(p.send->stream->last_shutdown_code == MOQ_RESET_DELIVERY_TIMEOUT);
    CHECK(p.releases == 0 && r->adapter->pending_sends == 1);
    payload_checkpoint(r, &p, "timeout-reset-accepted", MOQ_DONE);
    REQUIRE(fake_msq_deliver_send_complete(&r->client->fake, true));
    CHECK(p.releases == 1 && r->adapter->pending_sends == 0);
    payload_checkpoint(r, &p, "timeout-borrow-released-abort-owed", MOQ_DONE);
    payload_finish_shutdown(r, &p, MOQ_OK);
    CHECK(r->adapter->test_stream_commit_reports == 0);
    payload_later_group(r, sub, &p, MOQ_OK);
    peer_destroy(peer);
    rig_destroy(r);
    CHECK(p.releases == 1 && expired.releases == (subgroup_timeout ? 0u : 1u));
    clock_us = saved_clock;
    ++cases;
}

struct reset_submission {
    struct rig *r;
    struct payload_probe *payload;
    struct gate gate;
    QUIC_STREAM_SHUTDOWN_FN next;
};
static struct reset_submission *active_submission;

static QUIC_STATUS QUIC_API held_reset(HQUIC stream, QUIC_STREAM_SHUTDOWN_FLAGS flags,
                                       QUIC_UINT62 code)
{
    struct reset_submission *s = active_submission;
    REQUIRE(s != NULL && flags == QUIC_STREAM_SHUTDOWN_FLAG_ABORT_SEND);
    QUIC_STATUS result = s->next(stream, flags, code);
    gate_set(&s->gate, true);
    gate_wait(&s->gate, false);
    return result;
}

static void *reset_submission_thread(void *arg)
{
    struct reset_submission *s = arg;
    executor_enter(s->r);
    REQUIRE(moq_session_reset_subgroup(s->r->adapter->session, s->payload->subgroup,
                                      1, test_now()) == MOQ_OK);
    moq_msquic_conn_service(s->r->adapter);
    executor_leave(s->r);
    return NULL;
}

static void t_reset_submission(bool pending_status)
{
    printf("CASE reset submission unresolved, native %s\n", pending_status ? "PENDING" : "SUCCESS");
    struct rig *r = rig_create(MOQ_VERSION_DRAFT_22, false);
    struct peer *peer = establish(r, MOQ_VERSION_DRAFT_22);
    moq_subscription_t sub = subscribe_sender(r, peer);
    struct payload_probe p;
    payload_start(r, sub, 0, false, &p);
    r->client->fake.stream_shutdown_status = pending_status ? QUIC_STATUS_PENDING : QUIC_STATUS_SUCCESS;
    struct reset_submission submission = { .r = r, .payload = &p, .next = r->provider.api.StreamShutdown };
    REQUIRE(r->adapter->api == &r->provider.api);
    gate_init(&submission.gate);
    active_submission = &submission;
    r->provider.api.StreamShutdown = held_reset;
    pthread_t thread;
    REQUIRE(pthread_create(&thread, NULL, reset_submission_thread, &submission) == 0);
    gate_wait(&submission.gate, true);
    CHECK(p.send->stream->shutdown_calls == 1 && p.releases == 0);
    CHECK(!moq_msquic_test_stream_find(r->adapter, p.id)->reset_sent);
    check_query(r, MOQ_DONE); /* The real lane is held until native return. */
    gate_set(&submission.gate, false);
    REQUIRE(pthread_join(thread, NULL) == 0);
    r->provider.api.StreamShutdown = submission.next;
    active_submission = NULL;
    gate_destroy(&submission.gate);
    REQUIRE(moq_msquic_test_stream_find(r->adapter, p.id)->reset_sent);
    payload_checkpoint(r, &p, "native-accepted-not-completed", MOQ_DONE);
    REQUIRE(fake_msq_deliver_send_complete(&r->client->fake, true));
    CHECK(p.releases == 1);
    payload_checkpoint(r, &p, "submission-borrow-released-abort-owed", MOQ_DONE);
    payload_finish_shutdown(r, &p, MOQ_OK);
    payload_live(r);
    peer_destroy(peer);
    rig_destroy(r);
    CHECK(p.releases == 1);
    ++cases;
}

int main(void)
{
    moq_msq_test_now_us = test_now;
    CHECK(moq_msquic_managed_poll_flush(NULL) == MOQ_ERR_INVAL);
    struct rig *server = rig_create(MOQ_VERSION_DRAFT_22, true);
    CHECK(moq_msquic_managed_poll_flush(server->m) == MOQ_ERR_UNSUPPORTED);
    rig_destroy(server);
    const moq_version_t versions[] = {MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_22};
    for (size_t i = 0; i < sizeof(versions) / sizeof(versions[0]); ++i) {
        t_credit_and_stale(versions[i]);
        t_core_and_receive(versions[i]);
        t_inbound_backpressure(versions[i]);
        t_data_and_fin_blocked(versions[i], true);
        t_data_and_fin_blocked(versions[i], false);
        for (unsigned kind = 0; kind < 3; ++kind)
            for (unsigned pure = 0; pure < 2; ++pure)
                t_staging_fin(versions[i], kind, pure != 0);
    }
    t_retained_namespace_response(true, false);
    t_retained_namespace_response(true, true);
    t_retained_namespace_response(false, false);
    t_retained_owner_outcome(false, REJECT, false, 0);
    t_retained_owner_outcome(false, REDIRECT, true, 0);
    t_retained_owner_outcome(false, MIGRATE, false, 0);
    t_retained_owner_outcome(false, ACCEPT, true, 0);
    t_retained_owner_outcome(true, ACCEPT, true, 0);
    t_retained_owner_outcome(true, REJECT, true, 0);
    t_retained_owner_outcome(true, REDIRECT, true, 0);
    t_retained_owner_outcome(true, MIGRATE, false, 0);
    t_retained_owner_outcome(true, MIGRATE, true, 0);
    t_retained_owner_outcome(false, MIGRATE, true, 0);
    for (unsigned signals = 1; signals <= 3; ++signals) {
        t_retained_owner_outcome(false, MIGRATE, false, signals);
        t_retained_owner_outcome(true, MIGRATE, false, signals);
    }
    const moq_request_family_t families[] = {MOQ_REQUEST_FAMILY_SUBSCRIBE,
        MOQ_REQUEST_FAMILY_FETCH, MOQ_REQUEST_FAMILY_PUBLISH,
        MOQ_REQUEST_FAMILY_ANNOUNCEMENT, MOQ_REQUEST_FAMILY_NS_SUB,
        MOQ_REQUEST_FAMILY_SUBSCRIBE_TRACKS};
    for (size_t i = 0; i < sizeof(families) / sizeof(families[0]); ++i) {
        t_retained_family_reuse(MOQ_VERSION_DRAFT_18, families[i], false);
        t_retained_family_reuse(MOQ_VERSION_DRAFT_22, families[i], false);
        t_retained_family_reuse(MOQ_VERSION_DRAFT_22, families[i], true);
    }
    t_retained_automatic_update(MOQ_VERSION_DRAFT_18, MOQ_REQUEST_FAMILY_ANNOUNCEMENT);
    t_retained_automatic_update(MOQ_VERSION_DRAFT_22, MOQ_REQUEST_FAMILY_ANNOUNCEMENT);
    t_retained_automatic_update(MOQ_VERSION_DRAFT_18, MOQ_REQUEST_FAMILY_SUBSCRIBE_TRACKS);
    t_retained_automatic_update(MOQ_VERSION_DRAFT_22, MOQ_REQUEST_FAMILY_SUBSCRIBE_TRACKS);
    t_retained_namespace_goaway(MOQ_VERSION_DRAFT_18, false);
    t_retained_namespace_goaway(MOQ_VERSION_DRAFT_18, true);
    t_retained_namespace_goaway(MOQ_VERSION_DRAFT_22, false);
    t_retained_namespace_goaway(MOQ_VERSION_DRAFT_22, true);
    for (unsigned v = 0; v < 2; ++v) {
        moq_version_t version = v ? MOQ_VERSION_DRAFT_22 : MOQ_VERSION_DRAFT_18;
        for (unsigned subscriber = 0; subscriber < 2; ++subscriber) {
            for (unsigned pressure = 0; pressure < 3; ++pressure)
                for (unsigned fin = 0; fin < 2; ++fin)
                    t_crossed_namespace_retry(version, subscriber != 0, pressure, fin != 0);
            t_crossed_namespace_empty_fin(version, subscriber != 0);
            t_crossed_namespace_invalid(version, subscriber != 0, 0);
            t_crossed_namespace_invalid(version, subscriber != 0, 1);
        }
        t_crossed_namespace_inflight(version, false);
        t_crossed_namespace_inflight(version, true);
        t_crossed_namespace_invalid(version, true, 2);
        for (unsigned pressure = 0; pressure < 3; ++pressure) {
            t_crossed_namespace_pending_fin(version, pressure, false, false);
            t_crossed_namespace_pending_fin(version, pressure, true, false);
            if (pressure)
                t_crossed_namespace_pending_fin(version, pressure, true, true);
        }
    }
    t_unconfirmed_fin(false);
    t_unconfirmed_fin(true);
    t_pump_contention();
    t_terminal_stop_races();
    t_datagram_and_reset();
    t_failed_send();
    for (size_t i = 0; i < sizeof(versions) / sizeof(versions[0]); ++i) {
        for (unsigned fin = 0; fin < 2; ++fin) {
            t_payload_settlement(versions[i], PAYLOAD_NORMAL, fin != 0);
            t_payload_settlement(versions[i], PAYLOAD_UNEXPLAINED, fin != 0);
            t_payload_settlement(versions[i], PAYLOAD_PEER_STOP, fin != 0);
            t_receive_half_not_withdrawal(versions[i], false, fin != 0);
            t_receive_half_not_withdrawal(versions[i], true, fin != 0);
        }
        t_payload_settlement(versions[i], PAYLOAD_APP_RESET, false);
        t_payload_reset_refused(versions[i]);
        for (unsigned mode = 0; mode < 3; ++mode)
            t_control_output_loss(versions[i], mode);
        t_payload_reliable_prefix(versions[i], false);
        t_payload_reliable_prefix(versions[i], true);
    }
    t_payload_core_timeout(MOQ_VERSION_DRAFT_18, false);
    t_payload_core_timeout(MOQ_VERSION_DRAFT_18, true);
    t_payload_core_timeout(MOQ_VERSION_DRAFT_22, false);
    t_payload_core_timeout(MOQ_VERSION_DRAFT_22, true);
    t_reset_submission(false);
    t_reset_submission(true);
    moq_msq_test_now_us = NULL;
    printf("msquic managed flush: %u cases, %u failures\n", cases, failures);
    return failures ? 1 : 0;
}
