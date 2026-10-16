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
#include "../pico_wt_adapter.h"

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

static void test_control_stream_retired_detaches(void)
{
    /* The server ends the session (GOAWAY, then CLOSE capsule + FIN on the
     * WT control stream) and keeps its adapter. The client sees the close
     * and releases its session, which sends its own FIN. Once both
     * directions are acknowledged the server's control stream retires and
     * h3zero frees its context while the server adapter still exists. The
     * adapter must detach right then and keep no pointer to the freed
     * context, so the later destroy is safe (ASAN). */
    pico_wt_harness_t h;
    pico_wt_harness_cfg_t cfg = { .cid_byte = 0x32, .request_capacity = 10,
                                  .server_goaway_timeout_us = 100000 };
    if (pico_wt_harness_setup(&h, &cfg) != 0 ||
        pico_wt_harness_handshake(&h) != 0) {
        CHECK(0);
        pico_wt_harness_cleanup(&h);
        return;
    }
    uint64_t ctrl_id = h.server_conn->control_stream_id;
    h3zero_callback_ctx_t *server_h3 = h.server_conn->h3_ctx;
    CHECK(moq_session_goaway(h.server_session, NULL, 0, h.now) == MOQ_OK);
    moq_pico_wt_service(h.server_conn, h.now);
    pico_wt_harness_pump_until_closed(&h, 5000, h.server_conn);
    CHECK(moq_pico_wt_conn_is_closed(h.server_conn));
    CHECK(!moq_pico_wt_conn_is_fatal(h.server_conn));
    CHECK(h.server_conn->control_stream_ctx != NULL);

    /* The client lets go of its session: its FIN completes the stream. */
    pico_wt_harness_pump_until_closed(&h, 5000, h.client_conn);
    moq_pico_wt_conn_destroy(h.client_conn);
    h.client_conn = NULL;
    pico_wt_harness_pump(&h, 2000);

    /* The control stream really retired... */
    CHECK(h3zero_find_stream(server_h3, ctrl_id) == NULL);
    /* ...and the adapter let go of it without turning the clean close into
     * a failure. */
    CHECK(h.server_conn->control_stream_ctx == NULL);
    CHECK(h.server_conn->endpoint_ctx.control_stream_ctx == NULL);
    CHECK(h.server_conn->cnx == NULL);
    CHECK(moq_pico_wt_conn_is_closed(h.server_conn));
    CHECK(!moq_pico_wt_conn_is_fatal(h.server_conn));

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
 * Inbound WOULD_BLOCK must pause the stream, never tear the connection down.
 *
 * h3zero cannot decline a WT data callback, so an adapter with no receive
 * flow control has only two options when the session refuses bytes: drop them
 * or kill the connection. This adapter does neither -- it owns each WT data
 * stream's QUIC receive window, withholds credit while the stream still has
 * retained bridge work, and retains in-window bytes for ordered replay. A
 * small event queue must therefore SLOW the peer, not terminate it.
 *
 * Strategy: small max_events (3), subscribe + accept, then blast objects
 * without draining client events until the queue fills.
 */
static void test_inbound_would_block_pauses(void)
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
     * Receive backpressure is per stream, so all objects must share one.
     * Do NOT drain client events: the event queue fills, inbound delivery
     * returns WOULD_BLOCK, and the stream keeps retained work -- the state
     * in which the adapter must withhold credit and pause. */
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

    /* An undrained receiver must survive. The adapter pauses the stream by
     * withholding credit; it must NOT raise its teardown sentinel (0x10), and
     * it must not have swallowed the refusal and kept feeding a pending stream
     * (which the bridge reports as 0x3). */
    CHECK(!moq_pico_wt_conn_is_fatal(h.client_conn));
    CHECK(!moq_transport_bridge_is_terminal(h.client_conn->bridge));

    uint64_t fc = moq_transport_bridge_fatal_code(h.client_conn->bridge);
    CHECK(fc != 0x10 /* adapter hard-failure teardown sentinel */);
    CHECK(fc != 0x3 /* bridge inbound-pending contract violation */);

    /* The backpressure must be real rather than unbounded local buffering:
     * some tracked stream is holding retained work while the app does not
     * drain. (The adapter tracks the control stream too, so scan rather than
     * assume a slot.) */
    bool any_pending = false;
    for (size_t i = 0; i < h.client_conn->rx_count; i++) {
        if (!h.client_conn->rx[i].active) continue;
        if (moq_transport_bridge_stream_has_pending(
                h.client_conn->bridge, h.client_conn->rx[i].stream_id))
            any_pending = true;
    }
    CHECK(any_pending);

    /* And it must be recoverable: once the application drains, the retained
     * bytes replay in order and the session keeps delivering. */
    uint32_t drained = 0;
    for (int i = 0; i < 200; i++) {
        moq_event_t ev;
        while (moq_session_poll_events(h.client_session, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) drained++;
            moq_event_cleanup(&ev);
        }
        moq_pico_wt_service(h.client_conn, h.now);
        pico_wt_harness_pump(&h, 200);
    }
    CHECK(drained > 0);
    CHECK(!moq_pico_wt_conn_is_fatal(h.client_conn));

cleanup:
    pico_wt_harness_cleanup(&h);
}

/* ================================================================== */
/* Datagrams behind a wrapped connection callback context             */
/* ================================================================== */

/* The managed client installs its own connection callback, whose context is
 * the facade rather than an h3zero context, and forwards every event to
 * h3zero_callback with the real h3zero context. This wrapper reproduces that
 * ownership with a context that is deliberately NOT an h3zero context: its
 * leading bytes are a poison pattern, so any helper that reaches the h3zero
 * context through picoquic_get_callback_context() instead of the endpoint's
 * own context reads garbage (and ASan reports it). */
typedef struct {
    uint8_t poison[sizeof(h3zero_callback_ctx_t) + 64];
    h3zero_callback_ctx_t *h3;
    unsigned calls;
} wrapped_cnx_ctx_t;

static int wrapped_cnx_callback(picoquic_cnx_t *cnx, uint64_t stream_id,
    uint8_t *bytes, size_t length, picoquic_call_back_event_t ev,
    void *callback_ctx, void *v_stream_ctx)
{
    wrapped_cnx_ctx_t *w = (wrapped_cnx_ctx_t *)callback_ctx;
    w->calls++;
    return h3zero_callback(cnx, stream_id, bytes, length, ev, w->h3,
                           v_stream_ctx);
}

/* Outlives the harness: picoquic may still call the wrapper while the
 * connection is deleted during cleanup. */
static wrapped_cnx_ctx_t g_wrapped;

static void test_datagram_wrapped_cnx_context(moq_version_t version)
{
    pico_wt_harness_t h;
    pico_wt_harness_cfg_t cfg = {
        .cid_byte = version == MOQ_VERSION_DRAFT_18 ? 0x33 : 0x31,
        .request_capacity = 10, .version = version };
    if (pico_wt_harness_setup(&h, &cfg) != 0 ||
        pico_wt_harness_handshake(&h) != 0) {
        CHECK(0);
        pico_wt_harness_cleanup(&h);
        return;
    }
    picoquic_cnx_t *ccnx = h.client_conn->endpoint_ctx.cnx;
    memset(&g_wrapped, 0xA5, sizeof(g_wrapped));
    g_wrapped.h3 = h.client_conn->endpoint_ctx.h3_ctx;
    g_wrapped.calls = 0;
    CHECK(picoquic_get_callback_context(ccnx) == (void *)g_wrapped.h3);
    picoquic_set_callback(ccnx, wrapped_cnx_callback, &g_wrapped);

    /* The server subscribes to the wrapped client, which publishes one
     * object as a datagram. */
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    moq_bytes_t ns[] = { {(const uint8_t *)"dg", 2} };
    sc.track_namespace.parts = ns;
    sc.track_namespace.count = 1;
    sc.track_name = (moq_bytes_t){(const uint8_t *)"t", 1};
    sc.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
    moq_subscription_t ssub;
    CHECK(moq_session_subscribe(h.server_session, &sc, h.now, &ssub) >= 0);
    moq_pico_wt_service(h.server_conn, h.now);
    CHECK(pico_wt_harness_pump(&h, 2000) == 0);

    moq_subscription_t csub = MOQ_SUBSCRIPTION_INVALID;
    {
        moq_event_t ev;
        while (moq_session_poll_events(h.client_session, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                csub = ev.u.subscribe_request.sub;
                moq_accept_subscribe_cfg_t ac;
                moq_accept_subscribe_cfg_init(&ac);
                CHECK(moq_session_accept_subscribe(h.client_session, csub,
                                                   &ac, h.now) >= 0);
            }
            moq_event_cleanup(&ev);
        }
    }
    CHECK(moq_subscription_is_valid(csub));
    if (!moq_subscription_is_valid(csub)) goto cleanup;
    moq_pico_wt_service(h.client_conn, h.now);
    CHECK(pico_wt_harness_pump(&h, 2000) == 0);
    drain_events(h.server_session);

    static const char payload[] = "datagram-behind-wrapper";
    moq_rcbuf_t *buf = NULL;
    CHECK(moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)payload,
                           sizeof(payload) - 1, &buf) == MOQ_OK);
    if (!buf) goto cleanup;
    CHECK(moq_session_send_object_datagram(h.client_session, csub, 0, 0, 128,
              false, buf, NULL, 0, h.now) == MOQ_OK);
    moq_rcbuf_decref(buf);
    moq_pico_wt_service(h.client_conn, h.now);
    CHECK(pico_wt_harness_pump(&h, 2000) == 0);
    {
        int got = 0;
        moq_event_t ev;
        while (moq_session_poll_events(h.server_session, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED &&
                ev.u.object_received.datagram &&
                ev.u.object_received.payload &&
                moq_rcbuf_len(ev.u.object_received.payload) ==
                    sizeof(payload) - 1 &&
                memcmp(moq_rcbuf_data(ev.u.object_received.payload),
                       payload, sizeof(payload) - 1) == 0)
                got++;
            moq_event_cleanup(&ev);
        }
        CHECK(got == 1);
    }
    CHECK(g_wrapped.calls > 0);

    /* The endpoint's own edges, on the wrapped connection: an oversized
     * datagram is refused, a second one while the slot is occupied is
     * dropped, and the slot is released once flushed. */
    {
        moq_transport_endpoint_ops_t *ops = &h.client_conn->endpoint_ops;
        pico_wt_endpoint_ctx_t *ep = &h.client_conn->endpoint_ctx;
        uint8_t big[2049];
        memset(big, 0x11, sizeof(big));
        CHECK(ops->send_datagram(ep, big, sizeof(big)) == MOQ_TRANSPORT_TOO_LARGE);
        CHECK(ep->pending_dg_len == 0);
        uint8_t small[8];
        memset(small, 0x22, sizeof(small));
        CHECK(ops->send_datagram(ep, small, sizeof(small)) == MOQ_TRANSPORT_OK);
        CHECK(ops->send_datagram(ep, small, sizeof(small)) == MOQ_TRANSPORT_DROPPED);
        CHECK(pico_wt_harness_pump(&h, 200) == 0);
        CHECK(ep->pending_dg_len == 0);

        /* An endpoint whose control stream has no registered prefix cannot
         * become datagram-ready: refused, and nothing is left buffered. */
        pico_wt_endpoint_ctx_t lone;
        moq_transport_endpoint_ops_t lone_ops;
        h3zero_stream_ctx_t lone_ctrl;
        memset(&lone_ctrl, 0, sizeof(lone_ctrl));
        lone_ctrl.stream_id = 4000004;
        CHECK(pico_wt_endpoint_init(&lone_ops, &lone, ep->cnx, ep->h3_ctx,
                                    &lone_ctrl, moq_alloc_default()) == 0);
        CHECK(lone_ops.send_datagram(&lone, small, sizeof(small)) ==
              MOQ_TRANSPORT_DROPPED);
        CHECK(lone.pending_dg_len == 0);
        pico_wt_endpoint_cleanup(&lone);
    }
    CHECK(!moq_pico_wt_conn_is_fatal(h.client_conn));
    CHECK(!moq_pico_wt_conn_is_fatal(h.server_conn));

cleanup:
    /* Teardown keeps the wrapper installed: detaching the adapter must not
     * reach the h3zero context through the connection either. */
    pico_wt_harness_cleanup(&h);
}

/* Control: the same datagram path with h3zero's own connection context (the
 * attach and server shape) delivers too, and a detached endpoint refuses
 * instead of touching a connection it no longer owns. */
static void test_datagram_detached_endpoint_refuses(void)
{
    pico_wt_harness_t h;
    pico_wt_harness_cfg_t cfg = { .cid_byte = 0x32, .request_capacity = 10 };
    if (pico_wt_harness_setup(&h, &cfg) != 0 ||
        pico_wt_harness_handshake(&h) != 0) {
        CHECK(0);
        pico_wt_harness_cleanup(&h);
        return;
    }
    pico_wt_endpoint_ctx_t *ep = &h.server_conn->endpoint_ctx;
    moq_transport_endpoint_ops_t *ops = &h.server_conn->endpoint_ops;
    uint8_t small[8];
    memset(small, 0x33, sizeof(small));
    CHECK(ops->send_datagram(ep, small, sizeof(small)) == MOQ_TRANSPORT_OK);
    CHECK(pico_wt_harness_pump(&h, 200) == 0);
    CHECK(ep->pending_dg_len == 0);

    picoquic_cnx_t *cnx = ep->cnx;
    h3zero_callback_ctx_t *h3 = ep->h3_ctx;
    ep->cnx = NULL;
    ep->h3_ctx = NULL;
    CHECK(ops->send_datagram(ep, small, sizeof(small)) == MOQ_TRANSPORT_DROPPED);
    CHECK(ep->pending_dg_len == 0);
    ep->cnx = cnx;
    ep->h3_ctx = h3;
    pico_wt_harness_cleanup(&h);
}

/* ================================================================== */
/* Inbound reset codes (draft-ietf-webtrans-http3-15 §4.4, §6)         */
/* ================================================================== */

/* WT_SESSION_GONE (§9.5): the code an endpoint resets every WT stream with
 * when the session ends. It lies outside the WebTransport application-error
 * range, so it carries no MoQ code. */
#define WT_SESSION_GONE 0x170d7b68ull
#define H3_REQUEST_CANCELLED 0x10cull

static int reset_setup(pico_wt_harness_t *h, uint8_t cid)
{
    pico_wt_harness_cfg_t cfg = { .cid_byte = cid, .request_capacity = 10 };
    if (pico_wt_harness_setup(h, &cfg) != 0 || pico_wt_harness_handshake(h) != 0) {
        CHECK(0);
        pico_wt_harness_cleanup(h);
        return -1;
    }
    CHECK(h->client_conn->moq_control_stream_id != UINT64_MAX);
    return 0;
}

/* The peer resets its half of the MoQ control stream. */
static void reset_moq_control(pico_wt_harness_t *h, uint64_t raw_code)
{
    CHECK(picoquic_reset_stream(h->test_ctx->cnx_server,
              h->client_conn->moq_control_stream_id, raw_code) == 0);
    CHECK(pico_wt_harness_pump(h, 200) == 0);
}

/* WT_SESSION_GONE on the MoQ control stream is the session ending, not a MoQ
 * code: the verdict comes from the WebTransport session terminal. Alone it
 * changes nothing; a following clean CONNECT FIN without a close capsule is a
 * clean close with code 0 (§6). The raw code is never the MoQ close code. */
static void test_reset_session_gone_then_connect_fin(void)
{
    pico_wt_harness_t h;
    if (reset_setup(&h, 0x71) != 0) return;
    reset_moq_control(&h, WT_SESSION_GONE);
    CHECK(!moq_pico_wt_conn_is_fatal(h.client_conn));
    CHECK(!moq_pico_wt_conn_is_closed(h.client_conn));
    CHECK(picoquic_add_to_stream(h.test_ctx->cnx_server,
              h.server_wt.ctrl_ctx->stream_id, NULL, 0, 1) == 0);
    pico_wt_harness_pump_until_closed(&h, 5000, h.client_conn);
    CHECK(moq_pico_wt_conn_is_closed(h.client_conn));
    CHECK(!moq_pico_wt_conn_is_fatal(h.client_conn));
    CHECK(moq_pico_wt_conn_close_code(h.client_conn) == 0);
    CHECK(moq_transport_bridge_fatal_code(h.client_conn->bridge) != WT_SESSION_GONE);
    pico_wt_harness_cleanup(&h);
}

/* A close capsule that trails the reset stays authoritative. */
static void test_reset_session_gone_then_close_capsule(void)
{
    pico_wt_harness_t h;
    if (reset_setup(&h, 0x72) != 0) return;
    reset_moq_control(&h, WT_SESSION_GONE);
    CHECK(picowt_send_close_session_message(h.test_ctx->cnx_server,
              h.server_wt.ctrl_ctx, 7, NULL) == 0);
    pico_wt_harness_pump_until_closed(&h, 5000, h.client_conn);
    CHECK(moq_pico_wt_conn_is_closed(h.client_conn));
    CHECK(!moq_pico_wt_conn_is_fatal(h.client_conn));
    CHECK(moq_pico_wt_conn_close_code(h.client_conn) == 7);
    pico_wt_harness_cleanup(&h);
}

/* An abrupt session end -- the CONNECT stream reset, no capsule, no FIN -- is
 * a transport-origin failure without a MoQ code: fatal with the adapter's
 * generic code, never the HTTP/3 code and never a clean close. */
static void test_reset_session_gone_then_connect_reset(void)
{
    pico_wt_harness_t h;
    if (reset_setup(&h, 0x73) != 0) return;
    reset_moq_control(&h, WT_SESSION_GONE);
    CHECK(picoquic_reset_stream(h.test_ctx->cnx_server,
              h.server_wt.ctrl_ctx->stream_id, H3_REQUEST_CANCELLED) == 0);
    pico_wt_harness_pump_until_closed(&h, 5000, h.client_conn);
    CHECK(moq_pico_wt_conn_is_fatal(h.client_conn));
    CHECK(!moq_pico_wt_conn_is_closed(h.client_conn));
    CHECK(moq_transport_bridge_fatal_code(h.client_conn->bridge) == 0x1);
    pico_wt_harness_cleanup(&h);
}

/* Control: a genuine WebTransport application code on the MoQ control stream
 * still carries its MoQ code (the existing behaviour). */
static void test_reset_mapped_code_on_control(void)
{
    pico_wt_harness_t h;
    if (reset_setup(&h, 0x74) != 0) return;
    uint64_t wt = 0;
    CHECK(pico_wt_moq_err_to_wt(0x3, &wt));
    reset_moq_control(&h, wt);
    CHECK(moq_pico_wt_conn_is_fatal(h.client_conn));
    CHECK(moq_transport_bridge_fatal_code(h.client_conn->bridge) == 0x3);
    pico_wt_harness_cleanup(&h);
}

/* A data stream reset whose code is not a WebTransport application code is
 * still a reset, delivered with no application code -- INTERNAL_ERROR (0x0),
 * the generic code -- rather than the raw HTTP/3 value; a mapped code is
 * decoded. */
static void test_reset_data_stream_codes(void)
{
    pico_wt_harness_t h;
    if (reset_setup(&h, 0x75) != 0) return;
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    moq_bytes_t ns[] = { {(const uint8_t *)"rst", 3} };
    sc.track_namespace.parts = ns;
    sc.track_namespace.count = 1;
    sc.track_name = (moq_bytes_t){(const uint8_t *)"t", 1};
    sc.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
    moq_subscription_t csub;
    CHECK(moq_session_subscribe(h.client_session, &sc, h.now, &csub) >= 0);
    moq_pico_wt_service(h.client_conn, h.now);
    CHECK(pico_wt_harness_pump(&h, 2000) == 0);
    moq_subscription_t ssub = MOQ_SUBSCRIPTION_INVALID;
    {
        moq_event_t ev;
        while (moq_session_poll_events(h.server_session, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                ssub = ev.u.subscribe_request.sub;
                moq_accept_subscribe_cfg_t ac;
                moq_accept_subscribe_cfg_init(&ac);
                CHECK(moq_session_accept_subscribe(h.server_session, ssub,
                                                   &ac, h.now) >= 0);
            }
            moq_event_cleanup(&ev);
        }
    }
    CHECK(moq_subscription_is_valid(ssub));
    if (!moq_subscription_is_valid(ssub)) goto cleanup;
    moq_pico_wt_service(h.server_conn, h.now);
    CHECK(pico_wt_harness_pump(&h, 2000) == 0);
    drain_events(h.client_session);

    const uint64_t FIRST = H3ZERO_WEBTRANSPORT_APPLICATION_ERROR(0);
    uint64_t mapped5 = 0;
    CHECK(pico_wt_moq_err_to_wt(5, &mapped5));
    /* The no-code stand-in is INTERNAL_ERROR, pinned by value. */
    CHECK(PICO_WT_RESET_NO_APP_CODE == 0x0);
    const struct { uint64_t raw; uint64_t want; } rows[] = {
        { WT_SESSION_GONE, PICO_WT_RESET_NO_APP_CODE },
        { FIRST + 0x1e,    PICO_WT_RESET_NO_APP_CODE },   /* reserved GREASE */
        { 0x3,             PICO_WT_RESET_NO_APP_CODE },   /* bare, not a WT code */
        { mapped5,         0x5 },
    };
    for (size_t i = 0; i < sizeof(rows) / sizeof(rows[0]); i++) {
        moq_subgroup_cfg_t sg;
        moq_subgroup_cfg_init(&sg);
        sg.group_id = i;
        moq_subgroup_handle_t sgh;
        CHECK(moq_session_open_subgroup(h.server_session, ssub, &sg, h.now, &sgh) >= 0);
        moq_rcbuf_t *buf = NULL;
        CHECK(moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)"x", 1, &buf) == MOQ_OK);
        CHECK(moq_session_write_object(h.server_session, sgh, 0, buf, h.now) >= 0);
        moq_rcbuf_decref(buf);
        moq_pico_wt_service(h.server_conn, h.now);
        CHECK(pico_wt_harness_pump(&h, 500) == 0);
        uint64_t uni = h.server_conn->last_opened_uni_id;
        CHECK(picoquic_reset_stream(h.test_ctx->cnx_server, uni, rows[i].raw) == 0);
        CHECK(pico_wt_harness_pump(&h, 500) == 0);
        int resets = 0;
        uint64_t code = UINT64_MAX;
        moq_event_t ev;
        while (moq_session_poll_events(h.client_session, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBGROUP_RESET) {
                resets++;
                code = ev.u.subgroup_reset.error_code;
            }
            moq_event_cleanup(&ev);
        }
        CHECK(resets == 1);
        CHECK(code == rows[i].want);
        if (code != rows[i].want)
            fprintf(stderr, "  row %zu raw=0x%llx got=0x%llx\n", i,
                    (unsigned long long)rows[i].raw, (unsigned long long)code);
    }
    CHECK(!moq_pico_wt_conn_is_fatal(h.client_conn));
cleanup:
    pico_wt_harness_cleanup(&h);
}

/* ================================================================== */
/* Control-stream terminal arbitration, both drafts, both perspectives */
/* ================================================================== */

/* One side (the sender) acts on the other (the receiver). The MoQ control
 * channel is the client's control bidi on draft-16 and a unidirectional pair
 * on draft-18; each side's own control stream is captured right after the
 * handshake, before anything else is opened. */
typedef struct {
    pico_wt_harness_t h;
    bool              recv_server;
    picoquic_cnx_t   *snd_cnx, *rcv_cnx;
    h3zero_stream_ctx_t *snd_connect;     /* the sender's CONNECT stream */
    moq_pico_wt_conn_t  *rcv;
    uint64_t snd_ctrl, rcv_ctrl;          /* each side's own control stream */
} arb_t;

static int arb_setup(arb_t *a, moq_version_t v, bool recv_server, uint8_t cid)
{
    memset(a, 0, sizeof(*a));
    pico_wt_harness_cfg_t cfg = { .cid_byte = cid, .request_capacity = 10, .version = v };
    if (pico_wt_harness_setup(&a->h, &cfg) != 0 || pico_wt_harness_handshake(&a->h) != 0) {
        CHECK(0);
        pico_wt_harness_cleanup(&a->h);
        return -1;
    }
    a->recv_server = recv_server;
    moq_pico_wt_conn_t *snd = recv_server ? a->h.client_conn : a->h.server_conn;
    a->rcv = recv_server ? a->h.server_conn : a->h.client_conn;
    a->snd_cnx = recv_server ? a->h.test_ctx->cnx_client : a->h.test_ctx->cnx_server;
    a->rcv_cnx = recv_server ? a->h.test_ctx->cnx_server : a->h.test_ctx->cnx_client;
    a->snd_connect = recv_server ? a->h.client_ctrl_ctx : a->h.server_wt.ctrl_ctx;
    if (v == MOQ_VERSION_DRAFT_18) {
        CHECK(snd->opened_uni_count == 1 && a->rcv->opened_uni_count == 1);
        a->snd_ctrl = snd->last_opened_uni_id;
        a->rcv_ctrl = a->rcv->last_opened_uni_id;
    } else {
        a->snd_ctrl = a->rcv_ctrl = a->h.client_conn->moq_control_stream_id;
    }
    return 0;
}

static void arb_pump(arb_t *a) { CHECK(pico_wt_harness_pump(&a->h, 300) == 0); }

/* The sender ends its half of its own control stream with RESET. */
static void arb_reset_ctrl(arb_t *a, uint64_t code)
{
    CHECK(picoquic_reset_stream(a->snd_cnx, a->snd_ctrl, code) == 0);
    arb_pump(a);
}

/* The sender asks the receiver to stop sending on the receiver's control. */
static void arb_stop_ctrl(arb_t *a, uint64_t code)
{
    CHECK(picoquic_stop_sending(a->snd_cnx, a->rcv_ctrl, code) == 0);
    arb_pump(a);
}

static void arb_expect_open(arb_t *a)
{
    CHECK(!moq_pico_wt_conn_is_fatal(a->rcv));
    CHECK(!moq_pico_wt_conn_is_closed(a->rcv));
}

typedef enum {
    ARB_CAPSULE, ARB_CONNECT_FIN, ARB_CONNECT_RESET, ARB_LOSS_ORDERLY, ARB_LOSS_ABRUPT
} arb_end_t;

static void arb_end(arb_t *a, arb_end_t how)
{
    /* A receiver that already ended the connection leaves nothing to send on. */
    if (picoquic_get_cnx_state(a->snd_cnx) >= picoquic_state_disconnecting) {
        CHECK(0 && "sender connection already closing");
        return;
    }
    switch (how) {
    case ARB_CAPSULE:
        CHECK(picowt_send_close_session_message(a->snd_cnx, a->snd_connect, 7, NULL) == 0);
        break;
    case ARB_CONNECT_FIN:
        CHECK(picoquic_add_to_stream(a->snd_cnx, a->snd_connect->stream_id, NULL, 0, 1) == 0);
        break;
    case ARB_CONNECT_RESET:
        CHECK(picoquic_reset_stream(a->snd_cnx, a->snd_connect->stream_id,
                                    H3_REQUEST_CANCELLED) == 0);
        break;
    case ARB_LOSS_ORDERLY:
        /* The connection ends by the peer's orderly close, with no
         * WebTransport session terminal before it. */
        CHECK(picoquic_close(a->snd_cnx, 0) == 0);
        break;
    case ARB_LOSS_ABRUPT:
        /* The connection ends in error, with no WebTransport terminal. */
        CHECK(picoquic_close(a->snd_cnx, H3_REQUEST_CANCELLED) == 0);
        break;
    }
    CHECK(pico_wt_harness_pump(&a->h, 1000) == 0);
}

static const char *arb_name(arb_t *a, moq_version_t v)
{
    static char b[64];
    snprintf(b, sizeof b, "d%d %s receiver", v == MOQ_VERSION_DRAFT_18 ? 18 : 16,
             a->recv_server ? "server" : "client");
    return b;
}

#define ARB_CHECK(a, v, cond) do { if (!(cond)) { \
    fprintf(stderr, "FAIL: %s:%d: %s [%s]\n", __FILE__, __LINE__, #cond, arb_name(a, v)); \
    failures++; } } while (0)

#define ARB_CLOSED(a, v, code) do { \
    ARB_CHECK(a, v, moq_pico_wt_conn_is_closed((a)->rcv)); \
    ARB_CHECK(a, v, !moq_pico_wt_conn_is_fatal((a)->rcv)); \
    ARB_CHECK(a, v, moq_pico_wt_conn_close_code((a)->rcv) == (code)); } while (0)

#define ARB_FATAL(a, v, code) do { \
    ARB_CHECK(a, v, moq_pico_wt_conn_is_fatal((a)->rcv)); \
    ARB_CHECK(a, v, !moq_pico_wt_conn_is_closed((a)->rcv)); \
    ARB_CHECK(a, v, moq_transport_bridge_fatal_code((a)->rcv->bridge) == (code)); } while (0)

/* A RESET without a WebTransport application code (WT_SESSION_GONE, which a
 * peer resets every stream with as its session ends) or a STOP_SENDING (whose
 * code is not readable here) on the MoQ control channel decides nothing: the
 * WebTransport session terminal does. Both receivers get the close capsule
 * (h3zero's server delivers CONNECT-stream body bytes to the adapter). A bare
 * CONNECT FIN with no capsule reaches only a client: h3zero's server passes
 * a zero-length FIN on the CONNECT stream to no callback.
 *
 * The loss rows end the connection with no WebTransport terminal. They run
 * with a server receiver, where h3zero deregisters the session as the
 * connection closes and the adapter's deregistration path classifies the
 * close; the managed client's own loss path is pinned by the managed
 * arbitration test. */
static void test_control_arbitration(moq_version_t v, bool recv_server)
{
    uint8_t base = (uint8_t)(0x80 + (v == MOQ_VERSION_DRAFT_18 ? 0x20 : 0) + (recv_server ? 0x10 : 0));
    arb_t a;
    uint64_t mapped3 = 0;
    CHECK(pico_wt_moq_err_to_wt(0x3, &mapped3));

    for (int stop = 0; stop < 2; stop++) {
        /* GONE reset of the sender's control half, or STOP of the receiver's:
         * nothing decided; the close capsule then wins. */
        if (arb_setup(&a, v, recv_server, (uint8_t)(base + 1 + stop)) == 0) {
            if (stop) arb_stop_ctrl(&a, WT_SESSION_GONE);
            else      arb_reset_ctrl(&a, WT_SESSION_GONE);
            arb_expect_open(&a);
            arb_end(&a, ARB_CAPSULE);
            ARB_CLOSED(&a, v, 7);
            pico_wt_harness_cleanup(&a.h);
        }
    }
    /* GONE reset, then a clean CONNECT FIN: closed with code 0 (§6). */
    if (!recv_server && arb_setup(&a, v, recv_server, base + 3) == 0) {
        arb_reset_ctrl(&a, WT_SESSION_GONE);
        arb_end(&a, ARB_CONNECT_FIN);
        ARB_CLOSED(&a, v, 0);
        pico_wt_harness_cleanup(&a.h);
    }
    /* GONE reset, then a CONNECT reset: a transport-origin failure. */
    if (arb_setup(&a, v, recv_server, base + 4) == 0) {
        arb_reset_ctrl(&a, WT_SESSION_GONE);
        arb_end(&a, ARB_CONNECT_RESET);
        ARB_FATAL(&a, v, 0x1);
        pico_wt_harness_cleanup(&a.h);
    }
    /* GONE reset, then the connection ends in error: a transport-origin
     * failure, never a clean close. */
    if (recv_server && arb_setup(&a, v, recv_server, base + 5) == 0) {
        arb_reset_ctrl(&a, WT_SESSION_GONE);
        arb_end(&a, ARB_LOSS_ABRUPT);
        ARB_FATAL(&a, v, 0x1);
        pico_wt_harness_cleanup(&a.h);
    }
    /* GONE reset, then the peer closes the connection orderly: no
     * WebTransport terminal and no error, so a clean close with code 0. */
    if (recv_server && arb_setup(&a, v, recv_server, base + 8) == 0) {
        arb_reset_ctrl(&a, WT_SESSION_GONE);
        arb_end(&a, ARB_LOSS_ORDERLY);
        ARB_CLOSED(&a, v, 0);
        pico_wt_harness_cleanup(&a.h);
    }
    /* A close capsule, then the connection ends in error: the accepted
     * capsule code stays. */
    if (recv_server && arb_setup(&a, v, recv_server, base + 6) == 0) {
        arb_end(&a, ARB_CAPSULE);
        arb_end(&a, ARB_LOSS_ABRUPT);
        ARB_CLOSED(&a, v, 7);
        pico_wt_harness_cleanup(&a.h);
    }
    /* Control: a WebTransport application code on the control stream is
     * still a MoQ-level abort carrying its code. */
    if (arb_setup(&a, v, recv_server, base + 7) == 0) {
        arb_reset_ctrl(&a, mapped3);
        ARB_FATAL(&a, v, 0x3);
        pico_wt_harness_cleanup(&a.h);
    }
    /* Control: on draft-18 a GONE reset of an ordinary request stream is a
     * request-level event, never a control-channel teardown -- the receiver's
     * session stays established. */
    if (v == MOQ_VERSION_DRAFT_18 && arb_setup(&a, v, recv_server, base + 9) == 0) {
        moq_session_t *snd_s = recv_server ? a.h.client_session : a.h.server_session;
        moq_session_t *rcv_s = recv_server ? a.h.server_session : a.h.client_session;
        moq_pico_wt_conn_t *snd = recv_server ? a.h.client_conn : a.h.server_conn;
        moq_subscribe_cfg_t sc;
        moq_subscribe_cfg_init(&sc);
        moq_bytes_t ns[] = { {(const uint8_t *)"arb", 3} };
        sc.track_namespace.parts = ns;
        sc.track_namespace.count = 1;
        sc.track_name = (moq_bytes_t){(const uint8_t *)"t", 1};
        sc.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t sub;
        ARB_CHECK(&a, v, moq_session_subscribe(snd_s, &sc, a.h.now, &sub) >= 0);
        moq_pico_wt_service(snd, a.h.now);
        arb_pump(&a);
        int requests = 0;
        moq_event_t ev;
        while (moq_session_poll_events(rcv_s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) requests++;
            moq_event_cleanup(&ev);
        }
        ARB_CHECK(&a, v, requests == 1);
        ARB_CHECK(&a, v, snd->last_opened_bidi_id != UINT64_MAX);
        ARB_CHECK(&a, v, picoquic_reset_stream(a.snd_cnx, snd->last_opened_bidi_id,
                                               WT_SESSION_GONE) == 0);
        arb_pump(&a);
        arb_expect_open(&a);
        ARB_CHECK(&a, v, moq_session_state(rcv_s) == MOQ_SESS_ESTABLISHED);
        pico_wt_harness_cleanup(&a.h);
    }
}

int main(void)
{
    test_pico_wt_loopback();
    test_datagram_too_large();
    test_stop_sending_round_trip();
    test_close_double_close();
    test_control_stream_retired_detaches();
    test_goaway_close_not_fatal();
    test_deregister_after_close_not_fatal();
    test_drain_then_close_recovers();
    test_control_stream_empty_fin();
    test_inbound_would_block_pauses();
    test_datagram_wrapped_cnx_context(MOQ_VERSION_DRAFT_16);
    test_datagram_wrapped_cnx_context(MOQ_VERSION_DRAFT_18);
    test_datagram_detached_endpoint_refuses();
    test_reset_session_gone_then_connect_fin();
    test_reset_session_gone_then_close_capsule();
    test_reset_session_gone_then_connect_reset();
    test_reset_mapped_code_on_control();
    test_reset_data_stream_codes();
    for (int v = 0; v < 2; v++)
        for (int srv = 0; srv < 2; srv++)
            test_control_arbitration(v ? MOQ_VERSION_DRAFT_18 : MOQ_VERSION_DRAFT_16, srv != 0);

    if (failures == 0)
        printf("test_pico_wt_loopback: PASS (%d tests)\n", 22);
    else
        fprintf(stderr, "test_pico_wt_loopback: %d failure(s)\n",
                failures);
    return failures ? 1 : 0;
}
