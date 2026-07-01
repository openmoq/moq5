/*
 * Picoquic WebTransport loopback test.
 *
 * Uses picoquic's deterministic simulation to prove MoQ protocol
 * flows over WebTransport/H3: setup handshake, subscribe, object
 * delivery, plus endpoint-op and close/drain edge cases. Both sides
 * use the real picoquic QUIC + h3zero + picowt stack. The shared
 * lifecycle (CONNECT, session/adapter pair, pump, teardown) lives in
 * pico_wt_harness.{c,h}.
 */

#include "pico_wt_harness.h"
#include <moq/moq.h>

#include <stdio.h>
#include <string.h>

static int failures = 0;

#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

static void drain_events(moq_session_t *s)
{
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0)
        moq_event_cleanup(&ev);
}

/* -- Full protocol flow: handshake → subscribe → object ------------- */

static void test_pico_wt_loopback(void)
{
    pico_wt_harness_t h;
    pico_wt_harness_cfg_t cfg = { .cid_byte = 0x01, .request_capacity = 10 };
    if (pico_wt_harness_setup(&h, &cfg) != 0 ||
        pico_wt_harness_handshake(&h) != 0) {
        CHECK(0);
        pico_wt_harness_cleanup(&h);
        return;
    }
    CHECK(!moq_pico_wt_conn_is_fatal(h.client_conn));
    CHECK(!moq_pico_wt_conn_is_fatal(h.server_conn));

    /* === Subscribe === */
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    moq_bytes_t ns[] = {
        {(const uint8_t *)"pico", 4},
        {(const uint8_t *)"wt", 2}
    };
    sc.track_namespace.parts = ns;
    sc.track_namespace.count = 2;
    sc.track_name = (moq_bytes_t){(const uint8_t *)"video", 5};
    sc.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
    moq_subscription_t client_sub;
    int ret = moq_session_subscribe(h.client_session, &sc, 0, &client_sub);
    CHECK(ret >= 0);
    if (ret < 0) goto cleanup;

    moq_pico_wt_service(h.client_conn, h.now);
    CHECK(pico_wt_harness_pump(&h, 2000) == 0);

    /* Server accepts */
    moq_subscription_t server_sub = MOQ_SUBSCRIPTION_INVALID;
    {
        moq_event_t ev;
        while (moq_session_poll_events(h.server_session, &ev, 1) > 0){
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                server_sub = ev.u.subscribe_request.sub;
                moq_accept_subscribe_cfg_t acfg;
                moq_accept_subscribe_cfg_init(&acfg);
                ret = moq_session_accept_subscribe(h.server_session,
                    server_sub, &acfg, 0);
                CHECK(ret >= 0);
                if (ret < 0) server_sub = MOQ_SUBSCRIPTION_INVALID;
            }
            moq_event_cleanup(&ev);
        }
    }
    CHECK(moq_subscription_is_valid(server_sub));
    if (!moq_subscription_is_valid(server_sub)) goto cleanup;

    moq_pico_wt_service(h.server_conn, h.now);
    CHECK(pico_wt_harness_pump(&h, 2000) == 0);

    /* Client receives SUBSCRIBE_OK */
    {
        int sub_ok = 0;
        moq_event_t ev;
        while (moq_session_poll_events(h.client_session, &ev, 1) > 0){
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_OK) sub_ok = 1;
            moq_event_cleanup(&ev);
        }
        CHECK(sub_ok);
        if (!sub_ok) goto cleanup;
    }

    /* === Object delivery === */
    moq_subgroup_cfg_t sgcfg;
    moq_subgroup_cfg_init(&sgcfg);
    sgcfg.group_id = 0;
    sgcfg.publisher_priority = 200;
    moq_subgroup_handle_t sg;
    ret = moq_session_open_subgroup(h.server_session, server_sub,
                                     &sgcfg, 0, &sg);
    CHECK(ret >= 0);
    if (ret < 0) goto cleanup;

    const char *payload = "hello-pico-wt";
    moq_rcbuf_t *buf = NULL;
    ret = moq_rcbuf_create(moq_alloc_default(),
                            (const uint8_t *)payload,
                            strlen(payload), &buf);
    CHECK(ret >= 0);
    if (ret < 0 || !buf) goto cleanup;
    ret = moq_session_write_object(h.server_session, sg, 0, buf, 0);
    CHECK(ret >= 0);
    moq_rcbuf_decref(buf);
    ret = moq_session_close_subgroup(h.server_session, sg, 0);
    CHECK(ret >= 0);

    moq_pico_wt_service(h.server_conn, h.now);
    CHECK(pico_wt_harness_pump(&h, 2000) == 0);

    /* Client receives object */
    {
        int obj_received = 0;
        uint8_t received[64];
        size_t received_len = 0;
        moq_event_t ev;
        while (moq_session_poll_events(h.client_session, &ev, 1) > 0){
            if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED &&
                ev.u.object_received.payload) {
                const uint8_t *d = moq_rcbuf_data(
                    ev.u.object_received.payload);
                size_t l = moq_rcbuf_len(
                    ev.u.object_received.payload);
                if (l <= sizeof(received)) {
                    memcpy(received, d, l);
                    received_len = l;
                }
                obj_received = 1;
            }
            moq_event_cleanup(&ev);
        }
        CHECK(obj_received);
        if (obj_received) {
            CHECK(received_len == strlen(payload));
            CHECK(memcmp(received, payload, received_len) == 0);
        }
    }

    CHECK(!moq_pico_wt_conn_is_fatal(h.client_conn));
    CHECK(!moq_pico_wt_conn_is_fatal(h.server_conn));

cleanup:
    pico_wt_harness_cleanup(&h);
}

/* ================================================================== */
/* Edge tests: endpoint ops and callback paths                        */
/* ================================================================== */

static void test_datagram_too_large(void)
{
    /* Prove send_datagram TOO_LARGE and DROPPED via the real vtable
     * on a live loopback endpoint. */
    pico_wt_harness_t h;
    pico_wt_harness_cfg_t cfg = { .cid_byte = 0x10, .request_capacity = 10 };
    if (pico_wt_harness_setup(&h, &cfg) != 0) {
        CHECK(0);
        pico_wt_harness_cleanup(&h);
        return;
    }

    moq_transport_endpoint_ops_t *ops = &h.server_conn->endpoint_ops;
    void *ctx = &h.server_conn->endpoint_ctx;

    /* Oversized datagram (> 2048 inline buffer). */
    uint8_t big[2049];
    memset(big, 0xAA, sizeof(big));
    moq_transport_result_t r = ops->send_datagram(ctx, big, sizeof(big));
    CHECK(r == MOQ_TRANSPORT_TOO_LARGE);

    /* First send fits — fills the single-slot buffer. */
    uint8_t small[10];
    memset(small, 0xBB, sizeof(small));
    r = ops->send_datagram(ctx, small, sizeof(small));
    CHECK(r == MOQ_TRANSPORT_OK);

    /* Second send without pumping — slot still occupied. */
    r = ops->send_datagram(ctx, small, sizeof(small));
    CHECK(r == MOQ_TRANSPORT_DROPPED);

    pico_wt_harness_cleanup(&h);
}

static void test_stop_sending_round_trip(void)
{
    /* Client sends STOP_SENDING via the endpoint vtable on a
     * server-origin uni stream. Proves the outbound endpoint op
     * and the inbound picohttp_callback_stop_sending path. */
    pico_wt_harness_t h;
    pico_wt_harness_cfg_t cfg = { .cid_byte = 0x20, .request_capacity = 10 };
    if (pico_wt_harness_setup(&h, &cfg) != 0 ||
        pico_wt_harness_handshake(&h) != 0) {
        CHECK(0);
        pico_wt_harness_cleanup(&h);
        return;
    }

    /* Subscribe so the server can open a uni stream. */
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    moq_bytes_t ns[] = {{(const uint8_t *)"s", 1}};
    sc.track_namespace.parts = ns;
    sc.track_namespace.count = 1;
    sc.track_name = (moq_bytes_t){(const uint8_t *)"v", 1};
    sc.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
    moq_subscription_t csub;
    CHECK(moq_session_subscribe(h.client_session, &sc, 0, &csub) >= 0);
    moq_pico_wt_service(h.client_conn, h.now);
    pico_wt_harness_pump(&h, 2000);

    moq_subscription_t ssub = MOQ_SUBSCRIPTION_INVALID;
    {
        moq_event_t ev;
        while (moq_session_poll_events(h.server_session, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                ssub = ev.u.subscribe_request.sub;
                moq_accept_subscribe_cfg_t acfg;
                moq_accept_subscribe_cfg_init(&acfg);
                moq_session_accept_subscribe(h.server_session,
                    ssub, &acfg, 0);
            }
            moq_event_cleanup(&ev);
        }
    }
    CHECK(moq_subscription_is_valid(ssub));
    if (!moq_subscription_is_valid(ssub)) {
        pico_wt_harness_cleanup(&h);
        return;
    }
    moq_pico_wt_service(h.server_conn, h.now);
    pico_wt_harness_pump(&h, 2000);
    drain_events(h.client_session);

    /* Server writes object → opens uni stream. Do NOT close_subgroup
     * so the uni stream remains active (FIN not yet sent). */
    moq_subgroup_cfg_t sgcfg;
    moq_subgroup_cfg_init(&sgcfg);
    sgcfg.group_id = 0;
    sgcfg.publisher_priority = 200;
    moq_subgroup_handle_t sg;
    CHECK(moq_session_open_subgroup(h.server_session, ssub,
                                     &sgcfg, 0, &sg) >= 0);
    moq_rcbuf_t *buf = NULL;
    CHECK(moq_rcbuf_create(moq_alloc_default(),
        (const uint8_t *)"x", 1, &buf) >= 0);
    if (!buf) { pico_wt_harness_cleanup(&h); return; }
    CHECK(moq_session_write_object(h.server_session, sg, 0,
                                    buf, 0) >= 0);
    moq_rcbuf_decref(buf);
    moq_pico_wt_service(h.server_conn, h.now);
    pico_wt_harness_pump(&h, 2000);
    drain_events(h.client_session);

    /* Deterministic server-origin uni stream ID. */
    uint64_t uni_id = h.server_conn->last_opened_uni_id;
    CHECK(uni_id != UINT64_MAX);

    /* Client sends STOP_SENDING through the endpoint vtable. */
    moq_transport_result_t r =
        h.client_conn->endpoint_ops.stop_sending(
            &h.client_conn->endpoint_ctx, uni_id, 0x77);
    CHECK(r == MOQ_TRANSPORT_OK);

    /* Pump to deliver STOP_SENDING to server. */
    pico_wt_harness_pump(&h, 2000);

    /* Server adapter received the inbound stop_sending callback. */
    CHECK(!moq_pico_wt_conn_is_fatal(h.server_conn));
    CHECK(!moq_pico_wt_conn_is_fatal(h.client_conn));
    CHECK(h.server_conn->stop_sending_count == 1);
    CHECK(h.server_conn->last_stop_sending_stream_id == uni_id);

    pico_wt_harness_cleanup(&h);
}

static void test_close_double_close(void)
{
    /* After WT CONNECT + adapter attach, call close_transport twice.
     * First should succeed, second should return ERROR
     * (FIN already sent). Then destroy normally under ASAN. */
    pico_wt_harness_t h;
    pico_wt_harness_cfg_t cfg = { .cid_byte = 0x30, .request_capacity = 10 };
    if (pico_wt_harness_setup(&h, &cfg) != 0) {
        CHECK(0);
        pico_wt_harness_cleanup(&h);
        return;
    }

    /* First close: should succeed (sends CLOSE_SESSION capsule). */
    moq_transport_result_t rc1 =
        h.client_conn->endpoint_ops.close_transport(
            &h.client_conn->endpoint_ctx, 0, NULL, 0);
    CHECK(rc1 == MOQ_TRANSPORT_OK);

    /* Second close: should fail (FIN already sent). */
    moq_transport_result_t rc2 =
        h.client_conn->endpoint_ops.close_transport(
            &h.client_conn->endpoint_ctx, 0, NULL, 0);
    CHECK(rc2 == MOQ_TRANSPORT_ERROR);

    /* Destroy normally — ASAN verifies no UAF. */
    pico_wt_harness_cleanup(&h);
}

static void test_goaway_close_not_fatal(void)
{
    /* GOAWAY-driven close sets closed but not fatal.
     * Requires nonzero goaway_timeout_us so the drain timer fires
     * and the session emits CLOSE_SESSION. */
    pico_wt_harness_t h;
    pico_wt_harness_cfg_t cfg = { .cid_byte = 0x40, .request_capacity = 10,
                                  .server_goaway_timeout_us = 100000 };
    if (pico_wt_harness_setup(&h, &cfg) != 0 ||
        pico_wt_harness_handshake(&h) != 0) {
        CHECK(0);
        pico_wt_harness_cleanup(&h);
        return;
    }

    /* Server sends GOAWAY → enters DRAINING. */
    moq_result_t rc = moq_session_goaway(h.server_session, NULL, 0, h.now);
    CHECK(rc == MOQ_OK);
    moq_pico_wt_service(h.server_conn, h.now);

    /* Pump until the drain timeout fires and CLOSE_SESSION is sent.
     * Uses pump_until_closed because the normal pump exits on
     * quiescence before the 100ms drain timer expires. */
    pico_wt_harness_pump_until_closed(&h, 5000, h.server_conn);

    /* Server bridge should be closed, not fatal, with the GOAWAY
     * drain timeout close code (not some other clean close path). */
    CHECK(moq_pico_wt_conn_is_closed(h.server_conn));
    CHECK(!moq_pico_wt_conn_is_fatal(h.server_conn));
    CHECK(!moq_pico_wt_conn_is_fatal(h.client_conn));
    CHECK(moq_pico_wt_conn_close_code(h.server_conn)
          == MOQ_CLOSE_GOAWAY_TIMEOUT);

    /* Peer-observable close: the server's CLOSE_WEBTRANSPORT_SESSION
     * capsule reaches the client's WT control stream, and the client
     * adapter parses it into a clean close — closed (not fatal) with the
     * close code propagated through the capsule. */
    pico_wt_harness_pump_until_closed(&h, 5000, h.client_conn);
    CHECK(moq_pico_wt_conn_is_closed(h.client_conn));
    CHECK(!moq_pico_wt_conn_is_fatal(h.client_conn));
    CHECK(moq_pico_wt_conn_close_code(h.client_conn)
          == MOQ_CLOSE_GOAWAY_TIMEOUT);

    /* ASAN-clean teardown. */
    pico_wt_harness_cleanup(&h);
}

static void test_deregister_after_close_not_fatal(void)
{
    /* After close, the subsequent destroy + tls_api_delete_ctx
     * triggers picohttp_callback_deregister. The adapter must not
     * flip to fatal. ASAN verifies no UAF. */
    pico_wt_harness_t h;
    pico_wt_harness_cfg_t cfg = { .cid_byte = 0x50, .request_capacity = 10,
                                  .server_goaway_timeout_us = 100000 };
    if (pico_wt_harness_setup(&h, &cfg) != 0 ||
        pico_wt_harness_handshake(&h) != 0) {
        CHECK(0);
        pico_wt_harness_cleanup(&h);
        return;
    }

    /* Server sends GOAWAY → drain → CLOSE_SESSION. */
    moq_result_t rc = moq_session_goaway(h.server_session, NULL, 0, h.now);
    CHECK(rc == MOQ_OK);
    moq_pico_wt_service(h.server_conn, h.now);
    pico_wt_harness_pump_until_closed(&h, 5000, h.server_conn);

    CHECK(moq_pico_wt_conn_is_closed(h.server_conn));
    CHECK(!moq_pico_wt_conn_is_fatal(h.server_conn));
    CHECK(moq_pico_wt_conn_close_code(h.server_conn)
          == MOQ_CLOSE_GOAWAY_TIMEOUT);

    /* Destroy server adapter while bridge is in closed state.
     * detach_from_picoquic clears all h3zero callbacks, then
     * picowt_deregister + h3zero_delete_stream_prefix run.
     * Must not trip ASAN or flip fatal. */
    moq_pico_wt_conn_destroy(h.server_conn);
    h.server_conn = NULL;

    /* Client adapter still alive — destroy it next. */
    CHECK(!moq_pico_wt_conn_is_fatal(h.client_conn));

    /* Normal cleanup handles remaining resources. */
    pico_wt_harness_cleanup(&h);
}

static void test_drain_then_close_recovers(void)
{
    /* An inbound DRAIN must NOT fault the session, AND the capsule
     * accumulator must recover so a later valid CLOSE still parses. In
     * this picoquic build picowt_send_drain_session_message emits a
     * zero-length close-type capsule that picowt_receive_capsule reports
     * malformed (length < 4) — indistinguishable from a short/malformed
     * close — so the adapter ignores it (no fatal, no close) and resets
     * the parser. Delivery uses the same h3zero capsule path that
     * test_goaway_close_not_fatal proves carries a real close. (True
     * DRAIN semantics are deferred — see plan 14c/14d.) */
    pico_wt_harness_t h;
    pico_wt_harness_cfg_t cfg = { .cid_byte = 0x60, .request_capacity = 10 };
    if (pico_wt_harness_setup(&h, &cfg) != 0 ||
        pico_wt_harness_handshake(&h) != 0) {
        CHECK(0);
        pico_wt_harness_cleanup(&h);
        return;
    }

    /* Server → client direction (the receiver path the adapter parses as
     * post_data; cf. test_goaway_close_not_fatal). The server-as-CONNECT
     * peer receiving a control-stream close surfaces it via deregister,
     * not the capsule path, so the capsule parser is exercised on the
     * client receiver here. */

    /* 1. Server sends DRAIN on its WT control stream toward the client. */
    CHECK(picowt_send_drain_session_message(
              h.test_ctx->cnx_server, h.server_wt.ctrl_ctx) == 0);

    /* Pump (also separates the DRAIN from the later CLOSE into distinct
     * deliveries); neither side may go fatal or closed from the DRAIN. */
    pico_wt_harness_pump(&h, 1000);
    CHECK(!moq_pico_wt_conn_is_fatal(h.client_conn));
    CHECK(!moq_pico_wt_conn_is_closed(h.client_conn));
    CHECK(!moq_pico_wt_conn_is_fatal(h.server_conn));
    CHECK(!moq_pico_wt_conn_is_closed(h.server_conn));

    /* 2. Now a valid CLOSE with a distinctive code. The client's parser
     * must have recovered from the ignored DRAIN and reach a clean close
     * with the code propagated. */
    CHECK(picowt_send_close_session_message(
              h.test_ctx->cnx_server, h.server_wt.ctrl_ctx, 0x42, NULL) == 0);
    pico_wt_harness_pump_until_closed(&h, 5000, h.client_conn);
    CHECK(moq_pico_wt_conn_is_closed(h.client_conn));
    CHECK(!moq_pico_wt_conn_is_fatal(h.client_conn));
    CHECK(moq_pico_wt_conn_close_code(h.client_conn) == 0x42);

    pico_wt_harness_cleanup(&h);
}

static void test_control_stream_empty_fin(void)
{
    /* A zero-length FIN on the WT control stream (bytes == NULL,
     * length == 0) must not crash (ASAN) and must yield a clean close,
     * not fatal. Exercises the zero-length-payload guard in
     * handle_control_capsule. */
    pico_wt_harness_t h;
    pico_wt_harness_cfg_t cfg = { .cid_byte = 0x62, .request_capacity = 10 };
    if (pico_wt_harness_setup(&h, &cfg) != 0 ||
        pico_wt_harness_handshake(&h) != 0) {
        CHECK(0);
        pico_wt_harness_cleanup(&h);
        return;
    }

    /* Bare FIN (no capsule bytes) on the server's control stream toward
     * the client — the client adapter parses control-stream events, so
     * this drives the zero-length post_fin into handle_control_capsule. */
    CHECK(picoquic_add_to_stream(h.test_ctx->cnx_server,
              h.server_wt.ctrl_ctx->stream_id, NULL, 0, 1 /* set_fin */) == 0);

    pico_wt_harness_pump_until_closed(&h, 5000, h.client_conn);
    CHECK(moq_pico_wt_conn_is_closed(h.client_conn));
    CHECK(!moq_pico_wt_conn_is_fatal(h.client_conn));

    pico_wt_harness_cleanup(&h);
}

/*
 * Regression: inbound WOULD_BLOCK must propagate to h3zero.
 *
 * h3zero cannot pause reads. When the bridge has unresolvable
 * inbound pending state (session event queue full, nobody draining),
 * deliver_stream_bytes returns -1. The callback must propagate that
 * to h3zero, which closes the connection. Silently continuing
 * would violate the bridge's inbound WOULD_BLOCK contract.
 *
 * Strategy: small max_events (3), subscribe + accept, then blast
 * objects without draining client events until the queue fills.
 */
static void test_inbound_would_block_propagated(void)
{
    pico_wt_harness_t h;
    pico_wt_harness_cfg_t cfg = {
        .cid_byte = 0x70,
        .request_capacity = 10,
        .max_events = 3,
    };
    if (pico_wt_harness_setup(&h, &cfg) != 0 ||
        pico_wt_harness_handshake(&h) != 0) {
        CHECK(0);
        pico_wt_harness_cleanup(&h);
        return;
    }

    /* Subscribe from client. */
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    moq_bytes_t ns[] = {
        {(const uint8_t *)"bp", 2},
        {(const uint8_t *)"test", 4}
    };
    sc.track_namespace.parts = ns;
    sc.track_namespace.count = 2;
    sc.track_name = (moq_bytes_t){(const uint8_t *)"v", 1};
    sc.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
    moq_subscription_t client_sub;
    int ret = moq_session_subscribe(h.client_session, &sc, 0, &client_sub);
    CHECK(ret >= 0);
    if (ret < 0) goto cleanup;

    moq_pico_wt_service(h.client_conn, h.now);
    CHECK(pico_wt_harness_pump(&h, 2000) == 0);

    /* Server accepts. */
    moq_subscription_t server_sub = MOQ_SUBSCRIPTION_INVALID;
    {
        moq_event_t ev;
        while (moq_session_poll_events(h.server_session, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                server_sub = ev.u.subscribe_request.sub;
                moq_accept_subscribe_cfg_t acfg;
                moq_accept_subscribe_cfg_init(&acfg);
                moq_session_accept_subscribe(h.server_session,
                    server_sub, &acfg, 0);
            }
            moq_event_cleanup(&ev);
        }
    }
    CHECK(moq_subscription_is_valid(server_sub));
    if (!moq_subscription_is_valid(server_sub)) goto cleanup;

    moq_pico_wt_service(h.server_conn, h.now);
    CHECK(pico_wt_harness_pump(&h, 2000) == 0);

    /* Drain client events (SUBSCRIBE_OK) so we start from 0. */
    {
        moq_event_t ev;
        while (moq_session_poll_events(h.client_session, &ev, 1) > 0)
            moq_event_cleanup(&ev);
    }

    /* Open ONE subgroup (one uni stream) and write many objects to it.
     * The per-stream inbound WOULD_BLOCK contract only trips when more
     * bytes arrive on a stream that still has pending state - so all
     * objects must share a stream. Do NOT drain client events: the
     * event queue fills, inbound delivery returns WOULD_BLOCK, pending
     * remains, and the next bytes on that stream force the callback to
     * return -1 -> h3zero tears down the connection. */
    moq_subgroup_cfg_t sgcfg;
    moq_subgroup_cfg_init(&sgcfg);
    sgcfg.group_id = 0;
    sgcfg.publisher_priority = 200;
    moq_subgroup_handle_t sg;
    ret = moq_session_open_subgroup(h.server_session, server_sub,
                                     &sgcfg, 0, &sg);
    CHECK(ret >= 0);
    if (ret < 0) goto cleanup;

    for (uint64_t obj = 0; obj < 40; obj++) {
        if (moq_transport_bridge_is_terminal(h.client_conn->bridge))
            break;

        moq_rcbuf_t *buf = NULL;
        moq_rcbuf_create(moq_alloc_default(),
                          (const uint8_t *)"bp", 2, &buf);
        if (buf) {
            /* Server-side action queue may also fill once the client
             * stops draining; that's expected. Stop writing, then let
             * the inbound side play out below. */
            int wret = moq_session_write_object(
                h.server_session, sg, obj, buf, 0);
            moq_rcbuf_decref(buf);
            if (wret < 0) break;
        }

        moq_pico_wt_service(h.server_conn, h.now);
        pico_wt_harness_pump(&h, 500);
    }

    /* The client adapter must have taken the hard-failure path: on the
     * first delivery that left inbound pending unresolved, the callback
     * returns -1 (tearing down the connection) and marks the bridge
     * fatal with the adapter's own teardown code. */
    CHECK(moq_pico_wt_conn_is_fatal(h.client_conn));
    CHECK(moq_transport_bridge_is_terminal(h.client_conn->bridge));

    /* The fatal code must be the adapter's hard-failure teardown sentinel
     * (0x10), which is deliberately distinct from the bridge's generic
     * internal-fatal code (0x1) AND its "bytes arrived on a stream with
     * pending inbound" contract-violation code (0x3). Code 0x3 would mean
     * the adapter swallowed the -1 and kept feeding bytes to a pending
     * stream - exactly what this guards. The 0x10 check makes the
     * adapter-initiated teardown unambiguous (not a generic bridge fatal). */
    uint64_t fc = moq_transport_bridge_fatal_code(h.client_conn->bridge);
    CHECK(fc == 0x10 /* adapter hard-failure teardown sentinel */);
    CHECK(fc != 0x3 /* bridge inbound-pending contract violation */);

cleanup:
    pico_wt_harness_cleanup(&h);
}

int main(void)
{
    test_pico_wt_loopback();
    test_datagram_too_large();
    test_stop_sending_round_trip();
    test_close_double_close();
    test_goaway_close_not_fatal();
    test_deregister_after_close_not_fatal();
    test_drain_then_close_recovers();
    test_control_stream_empty_fin();
    test_inbound_would_block_propagated();

    if (failures == 0)
        printf("test_pico_wt_loopback: PASS (%d tests)\n", 9);
    else
        fprintf(stderr, "test_pico_wt_loopback: %d failure(s)\n",
                failures);
    return failures ? 1 : 0;
}
