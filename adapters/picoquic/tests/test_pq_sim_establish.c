/*
 * MoQ establishment over the REAL picoquic transport in the deterministic
 * simulator (tls_api): genuine QUIC packets, real stream ids, no fake
 * endpoints. This is the seam the fake-pair conformance suite cannot cover:
 * the adapter's inbound routing against actual QUIC stream id allocation.
 *
 * Scenarios:
 *   A. draft-16 establish + subscribe/object (harness control: the
 *      bidi-control profile, client-driven handshake).
 *   B. draft-18 establish: symmetric start (both endpoints open their own
 *      unidirectional control channel and send SETUP).
 *   C. draft-18 subscribe + object delivery: the client's first request
 *      bidi is QUIC stream 0 -- exactly the id the draft-16 model treats
 *      as the control stream, so this pins the adapter's mode-aware
 *      routing on both sides (request bidi each way, subgroup data uni).
 */

#include <moq/moq.h>
#include <moq/picoquic.h>

#include <picoquictest_internal.h>
#include <picoquic_utils.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int failures = 0;
static const char *scenario = "";
#define CHECK(cond) do { \
    if (!(cond)) { \
        fprintf(stderr, "FAIL[%s]: %s:%d: %s\n", scenario, __FILE__, \
                __LINE__, #cond); \
        failures++; \
    } } while (0)

/* -- sim pair ---------------------------------------------------------- */

typedef struct {
    picoquic_test_tls_api_ctx_t *test_ctx;
    uint64_t now;
    uint64_t loss;
    moq_version_t version;

    moq_session_t *client_session;
    moq_session_t *server_session;
    moq_pq_conn_t *client_conn;
    moq_pq_conn_t *server_conn;
    int            server_create_failed;
} pq_sim_t;

static int sim_create_session(moq_version_t version, moq_perspective_t persp,
                              moq_session_t **out)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), persp);
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 16;
    cfg.version = version;
    return moq_session_create(&cfg, 0, out) == MOQ_OK ? 0 : -1;
}

static int sim_attach_conn(moq_session_t *session, picoquic_cnx_t *cnx,
                           moq_pq_conn_t **out)
{
    moq_pq_conn_cfg_t cfg;
    moq_pq_conn_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.session = session;
    cfg.cnx = cnx;
    cfg.alloc = moq_alloc_default();
    return moq_pq_conn_create(&cfg, out);
}

/* Server-side lazy attach: the first callback for the inbound connection
 * creates the server session + adapter (the adapter re-points the cnx
 * callback at moq_pq_callback, so this fires exactly once). Mirrors the
 * managed facade's lazy server pattern. */
static int sim_server_cb(picoquic_cnx_t *cnx,
    uint64_t stream_id, uint8_t *bytes, size_t length,
    picoquic_call_back_event_t event, void *callback_ctx,
    void *stream_ctx)
{
    pq_sim_t *s = (pq_sim_t *)callback_ctx;
    (void)stream_id; (void)bytes; (void)length; (void)stream_ctx;

    if (event != picoquic_callback_almost_ready &&
        event != picoquic_callback_ready)
        return 0;
    if (s->server_conn)
        return 0;

    if (sim_create_session(s->version, MOQ_PERSPECTIVE_SERVER,
                           &s->server_session) != 0 ||
        sim_attach_conn(s->server_session, cnx, &s->server_conn) != 0) {
        s->server_create_failed = 1;
        return -1;
    }
    return 0;
}

static int sim_setup(pq_sim_t *s, uint8_t cid_byte, moq_version_t version,
                     uint32_t datagram_max)
{
    memset(s, 0, sizeof(*s));
    s->version = version;
    picoquic_solution_dir = PICOQUIC_SOURCE_DIR;

    picoquic_connection_id_t cid = {
        {0x6d, 0x71, 0x18, cid_byte, 0, 0, 0, 0}, 8};

    if (tls_api_init_ctx_ex(&s->test_ctx, PICOQUIC_INTERNAL_TEST_VERSION_1,
                            PICOQUIC_TEST_SNI, PICOQUIC_TEST_ALPN, &s->now,
                            NULL, NULL, 0, 1, 0, &cid) != 0)
        return -1;

    picoquic_set_default_idle_timeout(s->test_ctx->qclient, 30000);
    picoquic_set_default_idle_timeout(s->test_ctx->qserver, 30000);

    if (datagram_max > 0) {
        /* Mirror what the managed threaded adapter configures on its quic
         * contexts so QUIC DATAGRAM is negotiated to `datagram_max`: the server
         * cnx (created during the handshake) inherits the qserver default; the
         * client cnx already exists, so set its local transport parameter
         * directly. */
        picoquic_set_default_tp_value(s->test_ctx->qserver,
            picoquic_tp_max_datagram_frame_size, datagram_max);
        picoquic_tp_t tp =
            *picoquic_get_transport_parameters(s->test_ctx->cnx_client, 1);
        tp.max_datagram_frame_size = datagram_max;
        picoquic_set_transport_parameters(s->test_ctx->cnx_client, &tp);
    }

    if (sim_create_session(version, MOQ_PERSPECTIVE_CLIENT,
                           &s->client_session) != 0)
        return -1;
    if (sim_attach_conn(s->client_session, s->test_ctx->cnx_client,
                        &s->client_conn) != 0)
        return -1;

    picoquic_set_default_callback(s->test_ctx->qserver, sim_server_cb, s);

    if (picoquic_start_client_cnx(s->test_ctx->cnx_client) != 0)
        return -1;
    if (tls_api_connection_loop(s->test_ctx, &s->loss, 0, &s->now) != 0)
        return -1;
    if (s->server_create_failed || !s->server_conn)
        return -1;
    return 0;
}

/* One sim round: service both adapters, move packets, service again. */
static int sim_round(pq_sim_t *s, uint64_t time_limit, int *was_active)
{
    if (s->client_conn) moq_pq_service(s->client_conn, s->now);
    if (s->server_conn) moq_pq_service(s->server_conn, s->now);

    int wa = 0;
    int rc = tls_api_one_sim_round(s->test_ctx, &s->now, time_limit, &wa);

    if (s->client_conn) moq_pq_service(s->client_conn, s->now);
    if (s->server_conn) moq_pq_service(s->server_conn, s->now);

    if (was_active) *was_active = wa;
    return rc;
}

/* Pump up to `ms` of sim time or until 10 consecutive idle rounds. */
static void sim_pump(pq_sim_t *s, uint64_t ms)
{
    uint64_t limit = s->now + ms * 1000;
    int inactive = 0;
    for (int i = 0; i < 100000; i++) {
        if (s->now > limit) return;
        int wa = 0;
        if (sim_round(s, limit, &wa) != 0) return;
        if (!wa) { if (++inactive > 10) return; }
        else inactive = 0;
    }
}

/* Start the MoQ handshake and drive to SETUP_COMPLETE on both ends.
 * Draft-18 is a symmetric handshake over a unidirectional control-channel
 * pair: BOTH endpoints start and send SETUP. Draft-16 starts the client
 * alone; the server is reactive. */
static int sim_handshake(pq_sim_t *s)
{
    if (moq_session_start(s->client_session, s->now) < 0) return -1;
    if (s->version == MOQ_VERSION_DRAFT_18 &&
        moq_session_start(s->server_session, s->now) < 0) return -1;

    int cd = 0, sd = 0;
    uint64_t tl = s->now + 5000000;
    for (int i = 0; i < 100000 && !(cd && sd); i++) {
        if (s->now > tl) break;
        int wa = 0;
        if (sim_round(s, tl, &wa) != 0) break;
        moq_event_t ev;
        while (moq_session_poll_events(s->client_session, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SETUP_COMPLETE) cd = 1;
            moq_event_cleanup(&ev);
        }
        while (moq_session_poll_events(s->server_session, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SETUP_COMPLETE) sd = 1;
            moq_event_cleanup(&ev);
        }
    }
    return (cd && sd) ? 0 : -1;
}

static void sim_cleanup(pq_sim_t *s)
{
    if (s->client_conn) { moq_pq_conn_destroy(s->client_conn);
                          s->client_conn = NULL; }
    if (s->server_conn) { moq_pq_conn_destroy(s->server_conn);
                          s->server_conn = NULL; }
    if (s->client_session) { moq_session_destroy(s->client_session);
                             s->client_session = NULL; }
    if (s->server_session) { moq_session_destroy(s->server_session);
                             s->server_session = NULL; }
    if (s->test_ctx) { tls_api_delete_ctx(s->test_ctx);
                       s->test_ctx = NULL; }
}

/* -- subscribe + object over the established pair ----------------------- */

static int run_subscribe_object(pq_sim_t *s)
{
    int local_failures = 0;
#define SCHECK(cond) do { \
    if (!(cond)) { \
        fprintf(stderr, "FAIL[%s]: %s:%d: %s\n", scenario, __FILE__, \
                __LINE__, #cond); \
        local_failures++; \
    } } while (0)

    /* Client subscribes (draft-18: opens a request bidi -- QUIC stream 0,
     * the id the draft-16 model would mistake for the control stream). */
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    moq_bytes_t ns[] = {{(const uint8_t *)"t", 1}};
    sc.track_namespace.parts = ns;
    sc.track_namespace.count = 1;
    sc.track_name = (moq_bytes_t){(const uint8_t *)"v", 1};
    sc.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
    moq_subscription_t sub;
    SCHECK(moq_session_subscribe(s->client_session, &sc, s->now, &sub) >= 0);

    sim_pump(s, 200);

    /* Server sees the request and accepts. */
    moq_subscription_t ss = {0};
    bool got_req = false;
    moq_event_t ev;
    while (moq_session_poll_events(s->server_session, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
            ss = ev.u.subscribe_request.sub;
            got_req = true;
        }
        moq_event_cleanup(&ev);
    }
    SCHECK(got_req);
    if (!got_req) return local_failures;

    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    SCHECK(moq_session_accept_subscribe(s->server_session, ss, &ac,
                                        s->now) >= 0);
    sim_pump(s, 200);

    bool sub_ok = false;
    while (moq_session_poll_events(s->client_session, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_OK) sub_ok = true;
        moq_event_cleanup(&ev);
    }
    SCHECK(sub_ok);
    if (!sub_ok) return local_failures;

    /* Server publishes one object in a subgroup (uni data stream -- in
     * draft-18 it must classify as DATA alongside the uni control pair). */
    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = 0;
    sgc.publisher_priority = 200;
    moq_subgroup_handle_t sg;
    SCHECK(moq_session_open_subgroup(s->server_session, ss, &sgc,
                                     s->now, &sg) >= 0);

    const uint8_t payload[] = {0xCA, 0xFE};
    moq_rcbuf_t *buf = NULL;
    SCHECK(moq_rcbuf_create(moq_alloc_default(), payload, 2, &buf) >= 0);
    SCHECK(moq_session_write_object(s->server_session, sg, 0, buf,
                                    s->now) >= 0);
    moq_rcbuf_decref(buf);
    SCHECK(moq_session_close_subgroup(s->server_session, sg, s->now) >= 0);

    sim_pump(s, 200);

    bool got_obj = false;
    while (moq_session_poll_events(s->client_session, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) {
            moq_object_received_event_t *o = &ev.u.object_received;
            if (o->payload && moq_rcbuf_len(o->payload) == 2) {
                const uint8_t *d = moq_rcbuf_data(o->payload);
                if (d[0] == 0xCA && d[1] == 0xFE) got_obj = true;
            }
        }
        moq_event_cleanup(&ev);
    }
    SCHECK(got_obj);

    SCHECK(!moq_pq_conn_is_fatal(s->client_conn));
    SCHECK(!moq_pq_conn_is_fatal(s->server_conn));
#undef SCHECK
    return local_failures;
}

/* -- object datagram over the established pair --------------------------- *
 * Subscribe + accept, then the server sends an OBJECT DATAGRAM (not a subgroup
 * stream). With QUIC DATAGRAM negotiated the client receives it as a datagram
 * object; without negotiation the adapter drops it honestly (never delivered,
 * connection stays healthy). Also pins the max-size gate: an oversized datagram
 * is not delivered and does not fault the connection. */

static int sim_subscribe_accept(pq_sim_t *s, moq_subscription_t *ss_out)
{
    int lf = 0;
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    moq_bytes_t ns[] = {{(const uint8_t *)"t", 1}};
    sc.track_namespace.parts = ns;
    sc.track_namespace.count = 1;
    sc.track_name = (moq_bytes_t){(const uint8_t *)"v", 1};
    sc.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
    moq_subscription_t sub;
    if (moq_session_subscribe(s->client_session, &sc, s->now, &sub) < 0) lf++;
    sim_pump(s, 200);

    moq_subscription_t ss = {0};
    bool got_req = false;
    moq_event_t ev;
    while (moq_session_poll_events(s->server_session, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
            ss = ev.u.subscribe_request.sub; got_req = true;
        }
        moq_event_cleanup(&ev);
    }
    if (!got_req) { lf++; return lf; }

    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    if (moq_session_accept_subscribe(s->server_session, ss, &ac, s->now) < 0) lf++;
    sim_pump(s, 200);

    bool sub_ok = false;
    while (moq_session_poll_events(s->client_session, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_OK) sub_ok = true;
        moq_event_cleanup(&ev);
    }
    if (!sub_ok) lf++;
    *ss_out = ss;
    return lf;
}

/* Drain client events; record which datagram object ids arrived and the
 * largest datagram payload seen. */
typedef struct { bool got_obj[8]; size_t max_payload; int count; } dg_rx_t;

static void drain_datagrams(pq_sim_t *s, dg_rx_t *rx)
{
    moq_event_t ev;
    while (moq_session_poll_events(s->client_session, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED &&
            ev.u.object_received.datagram) {
            moq_object_received_event_t *o = &ev.u.object_received;
            if (o->object_id < 8) rx->got_obj[(size_t)o->object_id] = true;
            size_t l = o->payload ? moq_rcbuf_len(o->payload) : 0;
            if (l > rx->max_payload) rx->max_payload = l;
            rx->count++;
        }
        moq_event_cleanup(&ev);
    }
}

/* Send an object datagram (object_id `oid`) carrying `len` bytes of `fill`. */
static int send_dg(pq_sim_t *s, moq_subscription_t ss, uint64_t oid,
                   size_t len, uint8_t fill)
{
    uint8_t *p = (uint8_t *)malloc(len);
    memset(p, fill, len);
    moq_rcbuf_t *buf = NULL;
    int rc = -1;
    if (moq_rcbuf_create(moq_alloc_default(), p, len, &buf) >= 0) {
        rc = (int)moq_session_send_object_datagram(s->server_session, ss, 0, oid,
            200, true, buf, NULL, 0, s->now);
        moq_rcbuf_decref(buf);
    }
    free(p);
    return rc;
}

/* Round-trip (datagram_max > 0) vs honest-drop (datagram_max == 0). */
static void run_datagram(uint8_t cid_byte, uint32_t datagram_max)
{
    bool enable = datagram_max > 0;
    scenario = enable ? "datagram" : "datagram-no-nego";
    pq_sim_t s;
    CHECK(sim_setup(&s, cid_byte, MOQ_VERSION_DRAFT_16, datagram_max) == 0);
    if (!(s.test_ctx && s.client_conn && s.server_conn)) { sim_cleanup(&s); return; }
    CHECK(sim_handshake(&s) == 0);

    moq_subscription_t ss = {0};
    failures += sim_subscribe_accept(&s, &ss);

    CHECK(send_dg(&s, ss, 0, 2, 0xD0) >= 0);
    sim_pump(&s, 200);

    dg_rx_t rx = {0};
    drain_datagrams(&s, &rx);
    /* Negotiated -> delivered as a datagram; not negotiated -> honestly
     * dropped (nothing delivered), connection healthy either way. */
    CHECK(rx.got_obj[0] == enable);
    if (!enable) CHECK(rx.count == 0);

    CHECK(!moq_pq_conn_is_fatal(s.client_conn));
    CHECK(!moq_pq_conn_is_fatal(s.server_conn));
    CHECK(moq_session_state(s.client_session) == MOQ_SESS_ESTABLISHED);
    CHECK(moq_session_state(s.server_session) == MOQ_SESS_ESTABLISHED);
    sim_cleanup(&s);
}

/* Max-size gate. Negotiate a deliberately small datagram max (64) and send an
 * oversized object (300 bytes) that is still BELOW picoquic's cautious-length
 * threshold -- picoquic's own queue would NOT reject it, so only the adapter's
 * explicit size gate stops it. If the gate were removed, picoquic would send it
 * and the peer (local max 64) would connection-error on receive
 * (picoquic_decode_datagram_frame). Assert the oversized object never arrives,
 * the small ones do, and the connection stays healthy. */
static void run_datagram_gate(uint8_t cid_byte)
{
    scenario = "datagram-gate";
    pq_sim_t s;
    CHECK(sim_setup(&s, cid_byte, MOQ_VERSION_DRAFT_16, 64) == 0);
    if (!(s.test_ctx && s.client_conn && s.server_conn)) { sim_cleanup(&s); return; }
    CHECK(sim_handshake(&s) == 0);

    moq_subscription_t ss = {0};
    failures += sim_subscribe_accept(&s, &ss);

    CHECK(send_dg(&s, ss, 0, 2, 0xA0) >= 0);   /* fits the negotiated 64 */
    (void)send_dg(&s, ss, 1, 300, 0x5A);        /* > 64, < cautious -> gated */
    CHECK(send_dg(&s, ss, 2, 2, 0xC0) >= 0);   /* fits the negotiated 64 */
    sim_pump(&s, 200);

    dg_rx_t rx = {0};
    drain_datagrams(&s, &rx);
    CHECK(rx.got_obj[0]);          /* small datagram delivered */
    CHECK(rx.got_obj[2]);          /* small datagram delivered */
    CHECK(!rx.got_obj[1]);         /* oversized datagram NOT delivered */
    CHECK(rx.max_payload < 64);    /* nothing over the negotiated max arrived */

    /* Gate dropped the oversized datagram at send -> no over-limit frame ever
     * hit the wire, so the peer did not connection-error. */
    CHECK(!moq_pq_conn_is_fatal(s.client_conn));
    CHECK(!moq_pq_conn_is_fatal(s.server_conn));
    CHECK(moq_session_state(s.client_session) == MOQ_SESS_ESTABLISHED);
    CHECK(moq_session_state(s.server_session) == MOQ_SESS_ESTABLISHED);
    sim_cleanup(&s);
}

/* -- scenarios ----------------------------------------------------------- */

static void run_version(uint8_t cid_byte, moq_version_t version)
{
    scenario = (version == MOQ_VERSION_DRAFT_18) ? "d18" : "d16";
    pq_sim_t s;
    CHECK(sim_setup(&s, cid_byte, version, 0) == 0);
    if (s.test_ctx && s.client_conn && s.server_conn) {
        CHECK(sim_handshake(&s) == 0);
        CHECK(!moq_pq_conn_is_fatal(s.client_conn));
        CHECK(!moq_pq_conn_is_fatal(s.server_conn));
        CHECK(moq_session_state(s.client_session) == MOQ_SESS_ESTABLISHED);
        CHECK(moq_session_state(s.server_session) == MOQ_SESS_ESTABLISHED);
        failures += run_subscribe_object(&s);
    }
    sim_cleanup(&s);
}

int main(void)
{
    /* A. draft-16: harness control. */
    run_version(0x16, MOQ_VERSION_DRAFT_16);

    /* B + C. draft-18: symmetric uni-control establish + request-bidi
     * stream-0 routing + subgroup data delivery. */
    run_version(0x18, MOQ_VERSION_DRAFT_18);

    /* D. object datagram over real picoquic with QUIC DATAGRAM negotiated:
     * the peer receives it as a datagram. */
    run_datagram(0x4A, PICOQUIC_MAX_PACKET_SIZE);

    /* E. honesty: without negotiation the datagram is dropped, not delivered,
     * and the connection stays healthy (no silent claim-then-wedge). */
    run_datagram(0x4B, 0);

    /* F. the adapter's explicit max-size gate drops an oversized datagram
     * (below picoquic's own cautious-length threshold) without faulting. */
    run_datagram_gate(0x4C);

    if (failures == 0)
        printf("PASS: pq_sim_establish\n");
    return failures != 0;
}
