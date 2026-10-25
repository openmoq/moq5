/*
 * The server application pump runs on every after_receive, as
 * <moq/picoquic_threaded.h> promises (on_lane_pump: "after receiving, after
 * sending, on a wake ... with or without accepted connections"), whether or
 * not the receive brought any connection input. What a receive without input
 * saves is internal: the service pass before the pump. The service after the
 * pump, the drain publication and the activity mark run on every pass.
 *
 * The first row runs the real packet-loop callback over a server facade with
 * no accepted connection. A connection's input reaches the facade through the
 * server connection hook (moq_pq_conn_cfg_t.after_callback), driven here by
 * real picoquic callbacks into the adapter. No socket, thread, handshake or
 * sleep is used. A separate live-record row expires real admission holds and
 * checks the resulting provider STOP and exact deadline/drain publication.
 */
#include "../pq_threaded_internal.h"
#include "../../../core/src/session/session_internal.h"
#include "../../../core/src/bridge/transport_bridge_internal.h"
#include "../../../tests/unit/test_binding_support.h"
#include <moq/control_d18.h>
#include <moq/vi64.h>
#include <picoquic_internal.h>

#include <stdio.h>
#include <string.h>

void moq_pq_threaded_test_conn_hook(moq_pq_conn_t *conn, void *user_ctx);

static int failures;
#define CHECK(expr) do { if (!(expr)) { \
    fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
    failures++; \
} } while (0)

static int count_pump(moq_pq_threaded_t *t, moq_pq_threaded_lane_t *lane,
                      uint64_t now_us, void *ctx)
{
    (void)t;
    (void)lane;
    (void)now_us;
    (*(unsigned *)ctx)++;
    return 0;
}

static int loop(moq_pq_threaded_t *t, picoquic_quic_t *quic,
                picoquic_packet_loop_cb_enum mode)
{
    size_t bytes = 0;
    return moq_pq_threaded_loop_callback(quic, mode, t,
        mode == picoquic_packet_loop_wake_up ? NULL : &bytes);
}

/* What one loop callback did: pumps, service passes over every connection,
 * pre-pump services skipped, and whether it marked activity. */
typedef struct {
    unsigned pumps;
    uint64_t services, quiet;
    bool activity;
} pass_t;

static pass_t pass(moq_pq_threaded_t *t, picoquic_quic_t *quic,
                   picoquic_packet_loop_cb_enum mode, const unsigned *pumps)
{
    pthread_mutex_lock(&t->mutex);
    t->activity_pending = false;
    pthread_mutex_unlock(&t->mutex);
    const unsigned p0 = *pumps;
    const uint64_t s0 = t->st_service_all, q0 = t->st_quiet_recv;
    CHECK(loop(t, quic, mode) == 0);
    pass_t r;
    r.pumps = *pumps - p0;
    r.services = t->st_service_all - s0;
    r.quiet = t->st_quiet_recv - q0;
    pthread_mutex_lock(&t->mutex);
    r.activity = t->activity_pending;
    pthread_mutex_unlock(&t->mutex);
    return r;
}

/* A full pass: one pump, both service passes, activity. */
static bool full(pass_t r)
{
    return r.pumps == 1 && r.services == 2 && r.quiet == 0 && r.activity;
}

/* A receive without input: one pump, the post-pump service only, activity. */
static bool quiet(pass_t r)
{
    return r.pumps == 1 && r.services == 1 && r.quiet == 1 && r.activity;
}

static void test_server_quiet_receive(void)
{
    uint64_t now_us = 0;
    picoquic_quic_t *quic = picoquic_create(1, NULL, NULL, NULL,
        MOQ_PQ_ALPN_DEFAULT, NULL, NULL, NULL, NULL, NULL,
        now_us, &now_us, NULL, NULL, 0);
    CHECK(quic != NULL);
    if (!quic) return;
    struct sockaddr_in peer;
    memset(&peer, 0, sizeof(peer));
    peer.sin_family = AF_INET;
    peer.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    peer.sin_port = htons(4433);
    picoquic_cnx_t *cnx = picoquic_create_client_cnx(quic,
        (struct sockaddr *)&peer, now_us, 0, "localhost",
        MOQ_PQ_ALPN_DEFAULT, NULL, NULL);
    CHECK(cnx != NULL);

    moq_pq_threaded_t t;
    memset(&t, 0, sizeof(t));
    t.alloc = *moq_alloc_default();
    t.perspective = MOQ_PERSPECTIVE_SERVER;
    t.quic = quic;
    t.lane.owner = &t;
    t.stats_enabled = true;   /* the service and quiet-pass counters */
    unsigned pumps = 0;
    t.on_lane_pump = count_pump;
    t.on_lane_pump_ctx = &pumps;
    CHECK(pthread_mutex_init(&t.mutex, NULL) == 0);
    CHECK(pthread_cond_init(&t.cond, NULL) == 0);

    /* A connection whose callbacks reach the facade through the server
     * hook, as an accepted connection's do. */
    struct moq_pq_threaded_conn rec;
    memset(&rec, 0, sizeof(rec));
    rec.parent = &t;
    rec.cnx = cnx;
    moq_session_t *session = NULL;
    moq_pq_conn_t *conn = NULL;
    if (cnx) {
        moq_session_cfg_t scfg;
        moq_session_cfg_init(&scfg, moq_alloc_default(), MOQ_PERSPECTIVE_CLIENT);
        CHECK(moq_session_create(&scfg, now_us, &session) == MOQ_OK);
    }
    if (session) {
        moq_pq_conn_cfg_t ccfg;
        moq_pq_conn_cfg_init_sized(&ccfg, sizeof(ccfg));
        ccfg.session = session;
        ccfg.cnx = cnx;
        ccfg.alloc = moq_alloc_default();
        ccfg.user_ctx = &rec;
        ccfg.after_callback = moq_pq_threaded_test_conn_hook;
        CHECK(moq_pq_conn_create(&ccfg, &conn) == 0);
    }
    const uint64_t seq0 = t.input_seq;

    /* after_send is a full pass; a receive with no input since the last pass
     * began still pumps, publishes and marks activity. */
    CHECK(full(pass(&t, quic, picoquic_packet_loop_after_send, &pumps)));
    CHECK(quiet(pass(&t, quic, picoquic_packet_loop_after_receive, &pumps)));
    CHECK(quiet(pass(&t, quic, picoquic_packet_loop_after_receive, &pumps)));

    /* A connection callback other than prepare_to_send is input: the next
     * receive is a full pass, and the one after it is quiet again. */
    if (conn) {
        CHECK(moq_pq_callback(cnx, 0, NULL, 0, picoquic_callback_pacing_changed,
                              conn, NULL) == 0);
        CHECK(t.input_seq == seq0 + 1);
        CHECK(moq_pq_conn_event_count(conn) == 1);
        CHECK(full(pass(&t, quic, picoquic_packet_loop_after_receive, &pumps)));
        CHECK(quiet(pass(&t, quic, picoquic_packet_loop_after_receive, &pumps)));
        /* The service pass's own hook call is not input. */
        CHECK(moq_pq_service(conn, now_us) == 0);
        CHECK(t.input_seq == seq0 + 1);
        CHECK(quiet(pass(&t, quic, picoquic_packet_loop_after_receive, &pumps)));
    }
    CHECK(moq_pq_conn_event_count(NULL) == 0);

    /* An accept advances the sequence (server_callback); the same advance. */
    t.input_seq++;
    CHECK(full(pass(&t, quic, picoquic_packet_loop_after_receive, &pumps)));

    /* A rerun owed while the loop is out of select is served by the next
     * pass, a receive without input included. */
    pthread_mutex_lock(&t.mutex);
    CHECK(pq_wake_sched_on_wake(&t.sched) == PQ_WAKE_RERUN);
    pthread_mutex_unlock(&t.mutex);
    CHECK(quiet(pass(&t, quic, picoquic_packet_loop_after_receive, &pumps)));
    CHECK(!t.sched.rerun_requested);

    /* after_send and wake_up are full passes whatever the input. */
    CHECK(full(pass(&t, quic, picoquic_packet_loop_after_send, &pumps)));
    CHECK(full(pass(&t, quic, picoquic_packet_loop_wake_up, &pumps)));

    /* A stopping facade takes the close path on a receive. */
    pthread_mutex_lock(&t.mutex);
    t.stopped = true;
    pthread_mutex_unlock(&t.mutex);
    (void)loop(&t, quic, picoquic_packet_loop_after_receive);
    CHECK(t.local_close_started);

    moq_pq_conn_destroy(conn);
    moq_session_destroy(session);
    picoquic_free(quic);
    CHECK(pthread_cond_destroy(&t.cond) == 0);
    CHECK(pthread_mutex_destroy(&t.mutex) == 0);
    printf("server receive without input still pumps: checked\n");
}

/* This row owns a real facade record and actual provider streams, without a
 * handshake or a packet loop. Before SETUP, complete padding/data types still
 * obey #45's control reserve and bounded hold-age cancellation. Two staggered
 * holds make the next published deadline observable after only one expires. */
typedef struct {
    moq_transport_bridge_t *bridge;
    uint64_t first_sid, later_sid;
    uint64_t first_due, later_due;
    unsigned pumps;
    int stage;
} quiet_hold_t;

static const bridge_hold_t *quiet_hold(const moq_transport_bridge_t *b,
                                       uint64_t sid)
{
    for (uint32_t i = 0; i < b->max_holds; i++)
        if (b->holds[i].state != BRIDGE_HOLD_FREE &&
            b->holds[i].transport_id == sid)
            return &b->holds[i];
    return NULL;
}

static unsigned quiet_holds(const moq_transport_bridge_t *b)
{
    unsigned n = 0;
    for (uint32_t i = 0; i < b->max_holds; i++)
        if (b->holds[i].state != BRIDGE_HOLD_FREE) n++;
    return n;
}

static int quiet_hold_pump(moq_pq_threaded_t *t, moq_pq_threaded_lane_t *lane,
                           uint64_t now_us, void *ctx)
{
    quiet_hold_t *q = ctx;
    q->pumps++;
    CHECK(lane == &t->lane && t->in_lane_pump);
    CHECK(t->conn_count == 1 && now_us == q->first_due);
    if (t->conn_count != 1) return 0;
    CHECK(quiet_holds(q->bridge) == 2);
    CHECK(quiet_hold(q->bridge, q->first_sid) != NULL);
    CHECK(moq_session_next_deadline_us(t->conns[0]->session) == q->first_due);
    return 0;
}

static void quiet_hold_stage(moq_pq_threaded_t *t, int stage, void *ctx)
{
    quiet_hold_t *q = ctx;
    CHECK(t->conn_count == 1 && stage == q->stage + 1);
    q->stage = stage;
    if (t->conn_count != 1) return;  /* a failed service may have pruned it */
    struct moq_pq_threaded_conn *c = t->conns[0];
    CHECK(c->session->transport_owner == q->bridge && c->cnx != NULL);
    if (!c->cnx) return;
    if (stage == 1) {
        CHECK(quiet_holds(q->bridge) == 2);
        CHECK(quiet_hold(q->bridge, q->first_sid) != NULL);
        CHECK(c->cnx->app_wake_time == q->first_due);
        CHECK(t->tx_drained);      /* deliberately stale until publication */
    } else {
        CHECK(stage == 2 && quiet_holds(q->bridge) == 1);
        CHECK(quiet_hold(q->bridge, q->first_sid) == NULL);
        const bridge_hold_t *later = quiet_hold(q->bridge, q->later_sid);
        CHECK(later && later->state == BRIDGE_HOLD_RECORDED &&
              !later->mapped && !later->offered && later->held_payload &&
              later->first_refusal_us + BRIDGE_HOLD_AGE_US == q->later_due);
        CHECK(moq_session_next_deadline_us(c->session) == q->later_due);
        CHECK(c->cnx->app_wake_time == q->later_due);
        CHECK(!t->tx_drained);
    }
}

#define HOLD_REQUIRE(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
        goto cleanup; \
    } \
} while (0)

static void test_quiet_held_deadline(void)
{
    const int before = failures;
    uint64_t now_us = 1000;
    picoquic_quic_t *quic = picoquic_create(1, NULL, NULL, NULL,
        MOQ_PQ_ALPN_DEFAULT, NULL, NULL, NULL, NULL, NULL,
        now_us, &now_us, NULL, NULL, 0);
    CHECK(quic != NULL);
    if (!quic) return;
    struct sockaddr_in peer;
    memset(&peer, 0, sizeof(peer));
    peer.sin_family = AF_INET;
    peer.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    peer.sin_port = htons(4433);
    picoquic_cnx_t *cnx = picoquic_create_client_cnx(quic,
        (struct sockaddr *)&peer, now_us, 0, "localhost",
        MOQ_PQ_ALPN_DEFAULT, NULL, NULL);
    CHECK(cnx != NULL);
    if (!cnx) { picoquic_free(quic); return; }

    moq_pq_threaded_t t;
    memset(&t, 0, sizeof(t));
    t.alloc = *moq_alloc_default();
    t.perspective = MOQ_PERSPECTIVE_SERVER;
    t.quic = quic;
    t.lane.owner = &t;
    t.stats_enabled = true;
    int rc = pthread_mutex_init(&t.mutex, NULL);
    CHECK(rc == 0);
    if (rc != 0) { picoquic_free(quic); return; }
    rc = pthread_cond_init(&t.cond, NULL);
    CHECK(rc == 0);
    if (rc != 0) {
        CHECK(pthread_mutex_destroy(&t.mutex) == 0);
        picoquic_free(quic);
        return;
    }

    /* Match server_conns_append/server_conn_free ownership. If a failed
     * service prunes this heap record, cleanup uses the surviving list only. */
    t.conns = t.alloc.alloc(sizeof(*t.conns), t.alloc.ctx);
    HOLD_REQUIRE(t.conns != NULL);
    t.conn_cap = 1;
    struct moq_pq_threaded_conn *rec = t.alloc.alloc(sizeof(*rec), t.alloc.ctx);
    HOLD_REQUIRE(rec != NULL);
    memset(rec, 0, sizeof(*rec));
    t.conns[0] = rec;
    t.conn_count = 1;
    rec->parent = &t;
    rec->cnx = cnx;
    t.active_cnx = cnx;

    const picoquic_tp_t *current_tp = picoquic_get_transport_parameters(cnx, 1);
    HOLD_REQUIRE(current_tp != NULL);
    picoquic_tp_t tp = *current_tp;
    tp.initial_max_stream_id_unidir = BRIDGE_DEFAULT_MAX_STREAMS;
    tp.initial_max_stream_data_uni = 1024;
    picoquic_set_transport_parameters(cnx, &tp);

    moq_session_cfg_t scfg;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_CLIENT);
    scfg.version = MOQ_VERSION_DRAFT_18;
    scfg.idle_timeout_us = 0;     /* isolate the bridge's two hold deadlines */
    HOLD_REQUIRE(moq_session_create(&scfg, now_us, &rec->session) == MOQ_OK);
    HOLD_REQUIRE(test_bind_if_required(rec->session) == MOQ_OK);
    moq_pq_conn_cfg_t ccfg;
    moq_pq_conn_cfg_init_sized(&ccfg, sizeof(ccfg));
    ccfg.session = rec->session;
    ccfg.cnx = cnx;
    ccfg.alloc = moq_alloc_default();
    ccfg.user_ctx = rec;
    ccfg.after_callback = moq_pq_threaded_test_conn_hook;
    HOLD_REQUIRE(moq_pq_conn_create(&ccfg, &rec->conn) == 0);
    t.session = rec->session;
    t.conn = rec->conn;
    moq_transport_bridge_t *b = (moq_transport_bridge_t *)rec->session->transport_owner;
    HOLD_REQUIRE(b != NULL && b->holds != NULL);
    HOLD_REQUIRE(b->max_streams == BRIDGE_DEFAULT_MAX_STREAMS &&
                 b->control_mode == BRIDGE_CONTROL_UNI_PAIR);

    /* Two maps remain reserved for MoQ control. Every padding record comes
     * from a real callback; no bridge slots or readiness flags are fabricated. */
    const uint32_t padding_count = b->max_streams - 2;
    uint8_t padding[MOQ_VI64_MAX_LEN];
    const size_t padding_len = moq_vi64_encode(MOQ_D18_STREAM_PADDING,
                                               padding, sizeof(padding));
    HOLD_REQUIRE(padding_len > 0);
    for (uint32_t i = 0; i < padding_count; i++) {
        const uint64_t sid = 3u + 4u * i;
        HOLD_REQUIRE(picoquic_create_stream(cnx, sid) != NULL);
        HOLD_REQUIRE(moq_pq_callback(cnx, sid, padding, padding_len,
            picoquic_callback_stream_data, rec->conn, NULL) == 0);
        HOLD_REQUIRE(!moq_pq_conn_is_fatal(rec->conn));
    }
    HOLD_REQUIRE(moq_transport_bridge_stream_count(b) == padding_count);

    quiet_hold_t q = { 0 };
    q.bridge = b;
    q.first_sid = 3u + 4u * padding_count;
    q.later_sid = q.first_sid + 4;
    q.first_due = now_us + BRIDGE_HOLD_AGE_US;
    uint8_t type = 0x10;          /* known subgroup, incomplete header */
    for (unsigned i = 0; i < 2; i++) {
        const uint64_t sid = i == 0 ? q.first_sid : q.later_sid;
        HOLD_REQUIRE(picoquic_create_stream(cnx, sid) != NULL);
        HOLD_REQUIRE(moq_pq_callback(cnx, sid, &type, 1,
            picoquic_callback_stream_data, rec->conn, NULL) == 0);
        HOLD_REQUIRE(!moq_pq_conn_is_fatal(rec->conn));
        const bridge_hold_t *h = quiet_hold(b, sid);
        HOLD_REQUIRE(h && h->state == BRIDGE_HOLD_RECORDED &&
                     !h->mapped && !h->offered && h->known_data_type &&
                     h->held_payload && h->first_refusal_us == now_us);
        if (i == 0) now_us += 1000;
    }
    q.later_due = now_us + BRIDGE_HOLD_AGE_US;
    HOLD_REQUIRE(quiet_holds(b) == 2);
    HOLD_REQUIRE(moq_transport_bridge_stream_count(b) == padding_count);

    /* An ordinary pass consumes the callback sequence and publishes the first
     * age. No input arrives between it and the quiet pass at that deadline. */
    unsigned pumps = 0;
    t.on_lane_pump = count_pump;
    t.on_lane_pump_ctx = &pumps;
    HOLD_REQUIRE(full(pass(&t, quic, picoquic_packet_loop_after_send, &pumps)));
    HOLD_REQUIRE(t.conn_count == 1 && t.conns[0] == rec);
    HOLD_REQUIRE(t.input_seq == t.input_served && quiet_holds(b) == 2);
    HOLD_REQUIRE(moq_session_next_deadline_us(rec->session) == q.first_due &&
                 cnx->app_wake_time == q.first_due);
    now_us = q.first_due;
    t.on_lane_pump = quiet_hold_pump;
    t.on_lane_pump_ctx = &q;
    t.test_stage = quiet_hold_stage;
    t.test_stage_ctx = &q;
    t.tx_drained = true;          /* publication must replace this old value */
    const uint64_t sequence = t.input_seq, pass_seq = b->pass_seq;
    HOLD_REQUIRE(quiet(pass(&t, quic, picoquic_packet_loop_after_receive, &q.pumps)));
    HOLD_REQUIRE(t.conn_count == 1 && t.conns[0] == rec);
    CHECK(q.stage == 2 && q.pumps == 1 && b->pass_seq == pass_seq + 1);
    CHECK(t.input_seq == sequence && t.input_served == sequence);
    CHECK(!b->in_service && !b->ready_due && !moq_pq_conn_is_fatal(rec->conn));
    CHECK(quiet_holds(b) == 1 && quiet_hold(b, q.first_sid) == NULL);
    CHECK(moq_transport_bridge_stream_count(b) == padding_count);

    unsigned stops = 0;
    for (picoquic_stream_head_t *st = picoquic_first_stream(cnx); st;
         st = picoquic_next_stream(st)) {
        CHECK(!st->reset_requested);
        if (st->stop_sending_requested) {
            stops++;
            CHECK(st->stream_id == q.first_sid &&
                  st->local_stop_error == MOQ_STREAM_ERROR_EXCESSIVE_LOAD);
        }
    }
    CHECK(stops == 1);
    CHECK(moq_session_next_deadline_us(rec->session) == q.later_due &&
          cnx->app_wake_time == q.later_due && !t.tx_drained);
cleanup:
    /* Detach before destroying anything: a raw destructor's hook must never
     * find a freed record through the facade, nor may prune own it afterward. */
    t.test_stage = NULL;
    if (t.conn_count != 0) {
        struct moq_pq_threaded_conn *owned = t.conns[0];
        t.conn_count = 0;
        t.session = NULL; t.conn = NULL; t.active_cnx = NULL;
        moq_pq_conn_destroy(owned->conn);
        moq_session_destroy(owned->session);
        t.alloc.free(owned, sizeof(*owned), t.alloc.ctx);
    }
    if (t.conns) t.alloc.free(t.conns, t.conn_cap * sizeof(*t.conns), t.alloc.ctx);
    picoquic_free(quic);
    CHECK(pthread_cond_destroy(&t.cond) == 0);
    CHECK(pthread_mutex_destroy(&t.mutex) == 0);
    if (failures == before)
        printf("quiet real record: one post-pump pass expired one hold; next age published\n");
}

#undef HOLD_REQUIRE

int main(void)
{
    test_server_quiet_receive();
    test_quiet_held_deadline();
    if (failures) {
        fprintf(stderr, "%d failure(s)\n", failures);
        return 1;
    }
    printf("threaded quiet receive: all passed\n");
    return 0;
}
