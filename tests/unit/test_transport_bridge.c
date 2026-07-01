#include <moq/moq.h>
#include <moq/transport_bridge.h>
#include <moq/control.h>
#include <moq/buf.h>
#include "test_support.h"
#include "test_oom_support.h"
#include "../support/fake_endpoint.h"
#include <string.h>

/*
 * Bridge tests targeting the specific bugs and contracts found during
 * Codex review. Each test creates a bridge with a fake endpoint and
 * exercises one scenario.
 */

/* -- Helpers -------------------------------------------------------- */

typedef struct {
    moq_session_t *client;
    moq_session_t *server;
    fake_endpoint_t client_ep;
    fake_endpoint_t server_ep;
    moq_transport_bridge_t *client_bridge;
    moq_transport_bridge_t *server_bridge;
} test_pair_t;

static int test_pair_init_ex(test_pair_t *tp, uint32_t client_max_events)
{
    memset(tp, 0, sizeof(*tp));

    moq_session_cfg_t ccfg;
    moq_session_cfg_init_sized(&ccfg, sizeof(ccfg), moq_alloc_default(), MOQ_PERSPECTIVE_CLIENT);
    ccfg.send_request_capacity = true;
    ccfg.initial_request_capacity = 10;
    /* A tiny client event queue lets a test force receive backpressure
     * (a second object on a data stream blocks in PENDING_EMIT). 0 = default. */
    if (client_max_events) ccfg.max_events = client_max_events;

    moq_session_cfg_t scfg;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), moq_alloc_default(), MOQ_PERSPECTIVE_SERVER);
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 10;

    if (moq_session_create(&ccfg, 0, &tp->client) < 0) return -1;
    if (moq_session_create(&scfg, 0, &tp->server) < 0) {
        moq_session_destroy(tp->client);
        return -1;
    }

    fake_endpoint_init(&tp->client_ep, 1000, 2000);
    fake_endpoint_init(&tp->server_ep, 3000, 4000);

    moq_transport_bridge_cfg_t bcfg;
    moq_transport_bridge_cfg_init(&bcfg, moq_alloc_default());

    if (moq_transport_bridge_create(&bcfg, tp->client,
            &tp->client_ep.vtable, &tp->client_ep,
            &tp->client_bridge) < 0) {
        moq_session_destroy(tp->server);
        moq_session_destroy(tp->client);
        return -1;
    }
    if (moq_transport_bridge_create(&bcfg, tp->server,
            &tp->server_ep.vtable, &tp->server_ep,
            &tp->server_bridge) < 0) {
        moq_transport_bridge_destroy(tp->client_bridge);
        moq_session_destroy(tp->server);
        moq_session_destroy(tp->client);
        return -1;
    }

    return 0;
}

static int test_pair_init(test_pair_t *tp)
{
    return test_pair_init_ex(tp, 0);
}

static void test_pair_destroy(test_pair_t *tp)
{
    moq_transport_bridge_destroy(tp->client_bridge);
    moq_transport_bridge_destroy(tp->server_bridge);
    moq_session_destroy(tp->client);
    moq_session_destroy(tp->server);
}

/*
 * Pump: deliver client endpoint ops to server bridge, and vice versa.
 * Returns number of ops delivered.
 */
static size_t pump_once(test_pair_t *tp, uint64_t now)
{
    size_t delivered = 0;

    moq_transport_bridge_service(tp->client_bridge, now);

    for (size_t i = 0; i < tp->client_ep.count; i++) {
        fake_op_t *o = &tp->client_ep.ops[i];
        switch (o->kind) {
        case FAKE_OP_OPEN_BIDI:
            break;
        case FAKE_OP_WRITE:
            if (tp->server_bridge) {
                if (!tp->server_ep.next_bidi_id &&
                    o->stream_id >= 2000 && o->stream_id < 3000) {
                    moq_transport_bridge_on_peer_control_bytes(
                        tp->server_bridge, o->stream_id,
                        o->data, o->data_len, o->fin, now);
                } else if (o->stream_id >= 1000 && o->stream_id < 2000) {
                    moq_transport_bridge_on_peer_uni_bytes(
                        tp->server_bridge, o->stream_id,
                        o->data, o->data_len, o->fin, now);
                } else {
                    moq_transport_bridge_on_peer_control_bytes(
                        tp->server_bridge, o->stream_id,
                        o->data, o->data_len, o->fin, now);
                }
            }
            delivered++;
            break;
        case FAKE_OP_CLOSE:
            if (tp->server_bridge)
                moq_transport_bridge_on_transport_close(
                    tp->server_bridge, o->error_code, now);
            delivered++;
            break;
        default:
            delivered++;
            break;
        }
    }
    fake_endpoint_clear_ops(&tp->client_ep);

    moq_transport_bridge_service(tp->server_bridge, now);

    for (size_t i = 0; i < tp->server_ep.count; i++) {
        fake_op_t *o = &tp->server_ep.ops[i];
        switch (o->kind) {
        case FAKE_OP_WRITE:
            if (tp->client_bridge) {
                if (o->stream_id >= 2000 && o->stream_id < 3000) {
                    moq_transport_bridge_on_peer_control_bytes(
                        tp->client_bridge, o->stream_id,
                        o->data, o->data_len, o->fin, now);
                }
            }
            delivered++;
            break;
        default:
            delivered++;
            break;
        }
    }
    fake_endpoint_clear_ops(&tp->server_ep);

    return delivered;
}

static int pump_until_quiescent(test_pair_t *tp, int max, uint64_t now)
{
    for (int i = 0; i < max; i++) {
        if (pump_once(tp, now) == 0) return i;
    }
    return max;
}

static bool setup_handshake(test_pair_t *tp)
{
    moq_session_start(tp->client, 0);
    pump_until_quiescent(tp, 20, 0);

    moq_event_t ev;
    bool c_setup = false, s_setup = false;
    while (moq_session_poll_events(tp->client, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SETUP_COMPLETE) c_setup = true;
        moq_event_cleanup(&ev);
    }
    while (moq_session_poll_events(tp->server, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SETUP_COMPLETE) s_setup = true;
        moq_event_cleanup(&ev);
    }
    return c_setup && s_setup;
}

/* == Tests ========================================================== */

static int test_create_destroy(void)
{
    int failures = 0;
    fake_endpoint_t ep;
    fake_endpoint_init(&ep, 100, 200);

    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), MOQ_PERSPECTIVE_CLIENT);
    moq_session_t *s = NULL;
    MOQ_TEST_CHECK(moq_session_create(&cfg, 0, &s) == MOQ_OK);

    moq_transport_bridge_cfg_t bcfg;
    moq_transport_bridge_cfg_init(&bcfg, moq_alloc_default());
    moq_transport_bridge_t *b = NULL;
    MOQ_TEST_CHECK(moq_transport_bridge_create(&bcfg, s, &ep.vtable, &ep, &b) == MOQ_OK);
    MOQ_TEST_CHECK(b != NULL);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(b));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_closed(b));
    MOQ_TEST_CHECK(moq_transport_bridge_stream_count(b) == 0);

    moq_transport_bridge_destroy(b);
    moq_session_destroy(s);
    return failures;
}

static int test_create_rejects_bad_ops(void)
{
    int failures = 0;
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), MOQ_PERSPECTIVE_CLIENT);
    moq_session_t *s = NULL;
    moq_session_create(&cfg, 0, &s);

    moq_transport_endpoint_ops_t bad = MOQ_TRANSPORT_ENDPOINT_OPS_INIT;
    moq_transport_bridge_cfg_t bcfg;
    moq_transport_bridge_cfg_init(&bcfg, moq_alloc_default());
    moq_transport_bridge_t *b = NULL;

    MOQ_TEST_CHECK(moq_transport_bridge_create(&bcfg, s, &bad, &bad, &b) == MOQ_ERR_INVAL);
    MOQ_TEST_CHECK(b == NULL);

    moq_session_destroy(s);
    return failures;
}

static int test_setup_handshake(void)
{
    int failures = 0;
    test_pair_t tp;
    if (test_pair_init(&tp) < 0) { failures++; return failures; }

    MOQ_TEST_CHECK(setup_handshake(&tp));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.client_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.server_bridge));

    test_pair_destroy(&tp);
    return failures;
}

static int test_control_write_backpressure(void)
{
    int failures = 0;
    test_pair_t tp;
    if (test_pair_init(&tp) < 0) { failures++; return failures; }

    tp.client_ep.block_write = true;
    moq_session_start(tp.client, 0);
    moq_transport_bridge_service(tp.client_bridge, 0);

    MOQ_TEST_CHECK(moq_transport_bridge_has_pending(tp.client_bridge));
    MOQ_TEST_CHECK(tp.client_ep.block_count > 0);

    tp.client_ep.block_write = false;
    tp.client_ep.block_count = 0;

    moq_transport_bridge_service(tp.client_bridge, 0);
    MOQ_TEST_CHECK(!moq_transport_bridge_has_pending(tp.client_bridge));

    test_pair_destroy(&tp);
    return failures;
}

static int test_transport_close(void)
{
    int failures = 0;
    test_pair_t tp;
    if (test_pair_init(&tp) < 0) { failures++; return failures; }

    moq_transport_bridge_on_transport_close(tp.client_bridge, 0x42, 0);

    MOQ_TEST_CHECK(moq_transport_bridge_is_closed(tp.client_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_has_pending(tp.client_bridge));
    MOQ_TEST_CHECK(moq_transport_bridge_stream_count(tp.client_bridge) == 0);

    test_pair_destroy(&tp);
    return failures;
}

static int test_datagram_inbound_not_fatal(void)
{
    int failures = 0;
    test_pair_t tp;
    if (test_pair_init(&tp) < 0) { failures++; return failures; }
    if (!setup_handshake(&tp)) { failures++; test_pair_destroy(&tp); return failures; }

    moq_result_t rc = moq_transport_bridge_on_peer_datagram(
        tp.server_bridge, (const uint8_t *)"test", 4, 0);
    MOQ_TEST_CHECK(rc >= 0);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.server_bridge));

    test_pair_destroy(&tp);
    return failures;
}

static int test_inbound_uni_after_setup(void)
{
    int failures = 0;
    test_pair_t tp;
    if (test_pair_init(&tp) < 0) { failures++; return failures; }
    if (!setup_handshake(&tp)) { failures++; test_pair_destroy(&tp); return failures; }

    /* After handshake, deliver some uni bytes to the server.
     * The session should accept them (it's established). */
    uint8_t dummy[4] = {0x01, 0x02, 0x03, 0x04};
    moq_result_t rc = moq_transport_bridge_on_peer_uni_bytes(
        tp.server_bridge, 5000, dummy, 4, false, 0);
    MOQ_TEST_CHECK(rc >= 0 || rc == MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.server_bridge));

    test_pair_destroy(&tp);
    return failures;
}

static moq_transport_result_t error_close_transport(void *ctx, uint64_t code,
                                                     const uint8_t *r, size_t l)
{
    (void)ctx; (void)code; (void)r; (void)l;
    return MOQ_TRANSPORT_ERROR;
}

static int test_close_error_is_fatal_not_closed(void)
{
    int failures = 0;
    test_pair_t tp;
    if (test_pair_init(&tp) < 0) { failures++; return failures; }
    if (!setup_handshake(&tp)) { failures++; test_pair_destroy(&tp); return failures; }

    /* Replace close_transport with one that always returns ERROR */
    tp.client_ep.vtable.close_transport = error_close_transport;

    /* Deliver control FIN to client using the control stream ID
     * that was established during handshake (client opened bidi 2000) */
    moq_transport_bridge_on_peer_control_bytes(
        tp.client_bridge, 2000,
        NULL, 0, true, 0);

    /* service() tries close_transport → ERROR → fatal, not closed */
    moq_transport_bridge_service(tp.client_bridge, 0);

    MOQ_TEST_CHECK(moq_transport_bridge_is_fatal(tp.client_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_closed(tp.client_bridge));

    test_pair_destroy(&tp);
    return failures;
}

/* -- Regression: empty uni on unknown stream ----------------------- */

static int test_empty_uni_no_ghost_stream(void)
{
    int failures = 0;
    test_pair_t tp;
    if (test_pair_init(&tp) < 0) { failures++; return failures; }
    if (!setup_handshake(&tp)) { failures++; test_pair_destroy(&tp); return failures; }

    size_t before = moq_transport_bridge_stream_count(tp.server_bridge);

    moq_result_t rc = moq_transport_bridge_on_peer_uni_bytes(
        tp.server_bridge, 9999, NULL, 0, false, 0);
    MOQ_TEST_CHECK(rc == MOQ_OK);
    MOQ_TEST_CHECK(moq_transport_bridge_stream_count(tp.server_bridge) == before);

    test_pair_destroy(&tp);
    return failures;
}

/* -- Regression: truncated vtable rejected -------------------------- */

static int test_truncated_vtable_rejected(void)
{
    int failures = 0;
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), MOQ_PERSPECTIVE_CLIENT);
    moq_session_t *s = NULL;
    moq_session_create(&cfg, 0, &s);

    moq_transport_endpoint_ops_t trunc = MOQ_TRANSPORT_ENDPOINT_OPS_INIT;
    trunc.open_uni = fake_open_uni;
    trunc.open_bidi = fake_open_bidi;
    trunc.write = fake_write;
    trunc.reset_stream = fake_reset;
    trunc.stop_sending = fake_stop;
    trunc.close_transport = fake_close;
    /* Shrink struct_size so close_transport is outside declared bounds.
     * The old bug would still read close_transport and accept it. The
     * fix uses HAS_FIELD() and rejects because struct_size is too small. */
    trunc.struct_size = (uint32_t)offsetof(moq_transport_endpoint_ops_t,
                                            close_transport);

    moq_transport_bridge_cfg_t bcfg;
    moq_transport_bridge_cfg_init(&bcfg, moq_alloc_default());
    moq_transport_bridge_t *b = NULL;

    moq_result_t rc = moq_transport_bridge_create(
        &bcfg, s, &trunc, &trunc, &b);
    MOQ_TEST_CHECK(rc == MOQ_ERR_INVAL);
    MOQ_TEST_CHECK(b == NULL);

    moq_session_destroy(s);
    return failures;
}

/* Bogus outbound datagram result test deferred to conformance suite —
 * triggering SEND_DATAGRAM requires a full subscribe+datagram flow.
 * The sanitizer switch in transport_bridge.c is verified by inspection
 * and will be covered by conformance scenarios. */

/* -- Hard retry: control write blocked then close ------------------- */

static int test_close_retry_after_blocked_control(void)
{
    int failures = 0;
    test_pair_t tp;
    if (test_pair_init(&tp) < 0) { failures++; return failures; }

    /* Block writes so control setup goes pending */
    tp.client_ep.block_write = true;
    moq_session_start(tp.client, 0);
    moq_transport_bridge_service(tp.client_bridge, 0);
    MOQ_TEST_CHECK(moq_transport_bridge_has_pending(tp.client_bridge));

    /* Deliver control FIN while write is still blocked.
     * The bridge should notify the session and schedule deferred close. */
    moq_transport_bridge_on_peer_control_bytes(
        tp.client_bridge, 2000, NULL, 0, true, 0);

    /* Unblock and service — close must happen */
    tp.client_ep.block_write = false;
    moq_transport_bridge_service(tp.client_bridge, 0);

    /* Bridge must be closed (not fatal), with endpoint close observed */
    MOQ_TEST_CHECK(moq_transport_bridge_is_closed(tp.client_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.client_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_has_pending(tp.client_bridge));
    MOQ_TEST_CHECK(fake_endpoint_find(&tp.client_ep, FAKE_OP_CLOSE) != NULL);

    test_pair_destroy(&tp);
    return failures;
}

/* -- Hard retry: close_transport WOULD_BLOCK then succeeds ---------- */

static int test_close_retry_would_block(void)
{
    int failures = 0;
    test_pair_t tp;
    if (test_pair_init(&tp) < 0) { failures++; return failures; }
    if (!setup_handshake(&tp)) { failures++; test_pair_destroy(&tp); return failures; }

    /* Block close, then trigger control FIN */
    tp.client_ep.block_close = true;
    moq_transport_bridge_on_peer_control_bytes(
        tp.client_bridge, 2000, NULL, 0, true, 0);

    moq_transport_bridge_service(tp.client_bridge, 0);
    /* Close should be pending (WOULD_BLOCK) */
    MOQ_TEST_CHECK(moq_transport_bridge_has_pending(tp.client_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_closed(tp.client_bridge));

    /* Unblock and retry */
    tp.client_ep.block_close = false;
    fake_endpoint_clear_ops(&tp.client_ep);
    moq_transport_bridge_service(tp.client_bridge, 0);

    MOQ_TEST_CHECK(moq_transport_bridge_is_closed(tp.client_bridge));
    MOQ_TEST_CHECK(fake_endpoint_find(&tp.client_ep, FAKE_OP_CLOSE) != NULL);
    MOQ_TEST_CHECK(!moq_transport_bridge_has_pending(tp.client_bridge));

    test_pair_destroy(&tp);
    return failures;
}

/* -- Reset on unknown stream is no-op ------------------------------- */

static int test_reset_on_unknown_stream(void)
{
    int failures = 0;
    test_pair_t tp;
    if (test_pair_init(&tp) < 0) { failures++; return failures; }
    if (!setup_handshake(&tp)) { failures++; test_pair_destroy(&tp); return failures; }

    /* Reset for an unknown stream should be a no-op (no stream to reset) */
    moq_result_t rc = moq_transport_bridge_on_peer_stream_reset(
        tp.server_bridge, 9999, 0x42, 0);
    MOQ_TEST_CHECK(rc == MOQ_OK);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.server_bridge));

    test_pair_destroy(&tp);
    return failures;
}

/* -- Transport close clears all state ------------------------------- */

static int test_transport_close_clears_state(void)
{
    int failures = 0;
    test_pair_t tp;
    if (test_pair_init(&tp) < 0) { failures++; return failures; }

    /* Create real pending state: block writes then start (no handshake).
     * session_start emits SEND_CONTROL which the bridge tries to send.
     * With writes blocked, the control data goes to the pending queue. */
    tp.client_ep.block_write = true;
    moq_session_start(tp.client, 0);
    moq_transport_bridge_service(tp.client_bridge, 0);
    MOQ_TEST_CHECK(moq_transport_bridge_has_pending(tp.client_bridge));

    /* Transport close should clear everything */
    moq_transport_bridge_on_transport_close(tp.client_bridge, 0x1, 0);

    MOQ_TEST_CHECK(moq_transport_bridge_is_closed(tp.client_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_has_pending(tp.client_bridge));
    MOQ_TEST_CHECK(moq_transport_bridge_stream_count(tp.client_bridge) == 0);
    MOQ_TEST_CHECK(moq_transport_bridge_tombstone_count(tp.client_bridge) == 0);

    test_pair_destroy(&tp);
    return failures;
}

/* -- Regression: dropped inbound uni stream is discarded, not misparsed -----
 *
 * When the session drops a peer uni data stream -- here, a subgroup for a
 * track_alias nobody subscribed to -- it frees its rx entry and issues
 * STOP_DATA. Bytes already in flight on that stream must be DISCARDED by the
 * bridge: feeding them onward would have the session open a fresh rx entry and
 * parse mid-stream bytes as a leading stream type ("unknown data stream type",
 * 0x3), fataling the transport. This is the shape that previously closed the
 * connection during a live->VOD catalog conversion (the publisher finishes the
 * media subscription while its last objects are still arriving).
 *
 * The same contract holds on both inbound entry points (byte and rcbuf), so the
 * body is shared and run through each via this delivery shim. */
static moq_result_t deliver_uni(moq_transport_bridge_t *b, bool use_rcbuf,
                                uint64_t sid, const uint8_t *data, size_t len,
                                bool fin)
{
    if (!use_rcbuf)
        return moq_transport_bridge_on_peer_uni_bytes(b, sid, data, len, fin, 0);

    moq_rcbuf_t *buf = NULL;
    if (moq_rcbuf_create(moq_alloc_default(), data, len, &buf) < 0)
        return MOQ_ERR_NOMEM;
    moq_result_t rc = moq_transport_bridge_on_peer_uni_rcbuf(b, sid, buf, fin, 0);
    moq_rcbuf_decref(buf);
    return rc;
}

static int run_inbound_uni_dropped_then_discarded(bool use_rcbuf)
{
    int failures = 0;
    test_pair_t tp;
    if (test_pair_init(&tp) < 0) { failures++; return failures; }
    if (!setup_handshake(&tp)) { failures++; test_pair_destroy(&tp); return failures; }

    /* A valid subgroup header for an unsubscribed track_alias: the session
     * classifies it, fails to bind, and stops the stream within this call. */
    uint8_t hdr[32];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, hdr, sizeof(hdr));
    moq_d16_subgroup_header_t sh;
    memset(&sh, 0, sizeof(sh));
    sh.type = 0x14;
    sh.subgroup_id_mode = MOQ_SUBGROUP_ID_MODE_PRESENT;
    sh.track_alias = 9999;
    sh.group_id = 0;
    sh.subgroup_id = 0;
    sh.publisher_priority = 128;
    MOQ_TEST_CHECK(moq_d16_encode_subgroup_header(&w, &sh) == MOQ_OK);
    size_t hdr_len = moq_buf_writer_offset(&w);

    const uint64_t sid = 5000;

    /* First delivery: the session drops the stream (STOP_DATA), so the bridge
     * marks it for discard. Not a protocol violation. */
    moq_result_t rc = deliver_uni(tp.server_bridge, use_rcbuf, sid,
                                  hdr, hdr_len, false);
    MOQ_TEST_CHECK(rc >= 0 || rc == MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.server_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_closed(tp.server_bridge));

    /* More in-flight bytes on the same stream. Their leading byte (0x20)
     * classifies as an UNKNOWN data stream type: if the bridge re-fed them as a
     * fresh stream the session would fatal with 0x3. With the discard guard
     * they are swallowed and the bridge stays healthy. */
    uint8_t more[4] = { 0x20, 0x00, 0x00, 0x00 };
    rc = deliver_uni(tp.server_bridge, use_rcbuf, sid, more, sizeof(more), false);
    MOQ_TEST_CHECK(rc == MOQ_OK);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.server_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_closed(tp.server_bridge));

    /* Final in-flight bytes with FIN: still no fatal, and the discard entry is
     * retired so it leaves no ghost stream behind. */
    size_t before = moq_transport_bridge_stream_count(tp.server_bridge);
    MOQ_TEST_CHECK(before >= 1);
    rc = deliver_uni(tp.server_bridge, use_rcbuf, sid, more, sizeof(more), true);
    MOQ_TEST_CHECK(rc == MOQ_OK);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.server_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_closed(tp.server_bridge));
    MOQ_TEST_CHECK(moq_transport_bridge_stream_count(tp.server_bridge) ==
                   before - 1);

    test_pair_destroy(&tp);
    return failures;
}

static int test_inbound_uni_dropped_then_discarded(void)
{
    return run_inbound_uni_dropped_then_discarded(false);
}

static int test_inbound_uni_rcbuf_dropped_then_discarded(void)
{
    return run_inbound_uni_dropped_then_discarded(true);
}

/* -- Regression: bytes delivered during inbound pending_retry are kept ------
 *
 * A peer fills the client's event queue so a data stream backs up into
 * PENDING_EMIT (bridge pending_retry). The peer then delivers another object
 * (with FIN) on the SAME stream while pending_retry is set. The transport does
 * not re-deliver stream bytes, so the bridge/session must RETAIN those bytes
 * across the WOULD_BLOCK and deliver the object after the queue drains.
 *
 * Pre-fix: handle_data_bytes_impl retried the pending emit and returned
 * WOULD_BLOCK before appending the new bytes, silently dropping the object.
 * Run through both inbound entry points via deliver_uni(). */
static int run_pending_retry_keeps_bytes(bool use_rcbuf_extra)
{
    int failures = 0;
    test_pair_t tp;
    /* max_events = 1: object 0 fills the queue, object 1 -> PENDING_EMIT. */
    if (test_pair_init_ex(&tp, 1) < 0) { failures++; return failures; }
    if (!setup_handshake(&tp)) { failures++; test_pair_destroy(&tp); return failures; }

    /* Client subscribes; server accepts (carry the control both ways via pump). */
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_namespace_t ns = { ns_parts, 1 };
    moq_subscribe_cfg_t sub_cfg;
    moq_subscribe_cfg_init(&sub_cfg);
    sub_cfg.track_namespace = ns;
    sub_cfg.track_name = MOQ_BYTES_LITERAL("video");
    sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    moq_subscription_t sub;
    MOQ_TEST_CHECK(moq_session_subscribe(tp.client, &sub_cfg, 0, &sub) == MOQ_OK);
    pump_until_quiescent(&tp, 20, 0);

    moq_event_t ev;
    moq_subscription_t server_sub = MOQ_SUBSCRIPTION_INVALID;
    while (moq_session_poll_events(tp.server, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST)
            server_sub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK(moq_subscription_is_valid(server_sub));
    moq_accept_subscribe_cfg_t acc;
    moq_accept_subscribe_cfg_init(&acc);
    MOQ_TEST_CHECK(moq_session_accept_subscribe(tp.server, server_sub, &acc, 0)
                   == MOQ_OK);
    pump_until_quiescent(&tp, 20, 0);
    /* Drain the client's SUBSCRIBE_OK so the (size-1) event queue starts empty. */
    while (moq_session_poll_events(tp.client, &ev, 1) > 0)
        moq_event_cleanup(&ev);

    /* Server writes obj0+obj1 to one subgroup; capture the produced uni wire
     * bytes (header + obj0 + obj1) from the server endpoint into buf1. */
    moq_subgroup_cfg_t sg_cfg;
    moq_subgroup_cfg_init(&sg_cfg);
    sg_cfg.group_id = 0;
    sg_cfg.subgroup_id = 0;
    sg_cfg.publisher_priority = 200;
    moq_subgroup_handle_t sg;
    MOQ_TEST_CHECK(moq_session_open_subgroup(tp.server, server_sub, &sg_cfg, 0, &sg)
                   == MOQ_OK);
    const char *want[3] = { "AAA", "BBB", "CCC" };
    for (int i = 0; i < 2; i++) {
        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)want[i], 3, &p);
        MOQ_TEST_CHECK(moq_session_write_object(tp.server, sg, (uint64_t)i, p, 0)
                       == MOQ_OK);
        moq_rcbuf_decref(p);
    }
    fake_endpoint_clear_ops(&tp.server_ep);
    moq_transport_bridge_service(tp.server_bridge, 0);

    uint8_t buf1[512]; size_t len1 = 0;
    uint64_t uni_sid = 0; bool have_uni = false;
    for (size_t i = 0; i < tp.server_ep.count; i++) {
        fake_op_t *o = &tp.server_ep.ops[i];
        if (o->kind == FAKE_OP_OPEN_UNI) { uni_sid = o->stream_id; have_uni = true; }
        else if (o->kind == FAKE_OP_WRITE && have_uni && o->stream_id == uni_sid &&
                 len1 + o->data_len <= sizeof(buf1)) {
            memcpy(buf1 + len1, o->data, o->data_len);
            len1 += o->data_len;
        }
    }
    MOQ_TEST_CHECK(have_uni && len1 > 0);
    fake_endpoint_clear_ops(&tp.server_ep);

    /* Server writes obj2 and closes; capture obj2's bytes into buf2. */
    {
        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)want[2], 3, &p);
        MOQ_TEST_CHECK(moq_session_write_object(tp.server, sg, 2, p, 0) == MOQ_OK);
        moq_rcbuf_decref(p);
    }
    MOQ_TEST_CHECK(moq_session_close_subgroup(tp.server, sg, 0) == MOQ_OK);
    moq_transport_bridge_service(tp.server_bridge, 0);

    uint8_t buf2[512]; size_t len2 = 0;
    for (size_t i = 0; i < tp.server_ep.count; i++) {
        fake_op_t *o = &tp.server_ep.ops[i];
        if (o->kind == FAKE_OP_WRITE && o->stream_id == uni_sid &&
            len2 + o->data_len <= sizeof(buf2)) {
            memcpy(buf2 + len2, o->data, o->data_len);
            len2 += o->data_len;
        }
    }
    MOQ_TEST_CHECK(len2 > 0);

    /* Deliver buf1 (header+obj0+obj1) to the CLIENT bridge as a peer uni stream.
     * obj0 emits and fills the size-1 event queue; obj1 backs up into
     * PENDING_EMIT, so the bridge returns WOULD_BLOCK (pending_retry). */
    const uint64_t client_sid = 5000;
    moq_result_t rc = moq_transport_bridge_on_peer_uni_bytes(
        tp.client_bridge, client_sid, buf1, len1, false, 0);
    MOQ_TEST_CHECK(rc == MOQ_ERR_WOULD_BLOCK);

    /* While pending_retry is set, deliver obj2 + FIN on the SAME stream. The
     * bytes must be retained (not dropped) even though this also WOULD_BLOCKs. */
    rc = deliver_uni(tp.client_bridge, use_rcbuf_extra, client_sid,
                     buf2, len2, true);
    MOQ_TEST_CHECK(rc == MOQ_ERR_WOULD_BLOCK);

    /* Drain + service-retry until quiescent, collecting delivered objects. */
    bool got[3] = { false, false, false };
    for (int iter = 0; iter < 16; iter++) {
        while (moq_session_poll_events(tp.client, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) {
                uint64_t oid = ev.u.object_received.object_id;
                moq_rcbuf_t *pl = ev.u.object_received.payload;
                if (oid < 3 && pl && moq_rcbuf_len(pl) == 3 &&
                    memcmp(moq_rcbuf_data(pl), want[oid], 3) == 0)
                    got[(size_t)oid] = true;
            }
            moq_event_cleanup(&ev);
        }
        moq_transport_bridge_service(tp.client_bridge, 0);
    }

    MOQ_TEST_CHECK(got[0]);
    MOQ_TEST_CHECK(got[1]);
    MOQ_TEST_CHECK(got[2]);   /* must not be dropped during pending_retry */
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.client_bridge));

    test_pair_destroy(&tp);
    return failures;
}

static int test_pending_retry_keeps_bytes(void)
{
    return run_pending_retry_keeps_bytes(false);
}

static int test_pending_retry_keeps_rcbuf(void)
{
    return run_pending_retry_keeps_bytes(true);
}

/* == Main =========================================================== */

int main(void)
{
    int failures = 0;

    failures += test_create_destroy();
    failures += test_create_rejects_bad_ops();
    failures += test_setup_handshake();
    failures += test_control_write_backpressure();
    failures += test_transport_close();
    failures += test_datagram_inbound_not_fatal();
    failures += test_inbound_uni_after_setup();
    failures += test_close_error_is_fatal_not_closed();

    /* Regression tests */
    failures += test_empty_uni_no_ghost_stream();
    failures += test_truncated_vtable_rejected();
    failures += test_inbound_uni_dropped_then_discarded();
    failures += test_inbound_uni_rcbuf_dropped_then_discarded();
    failures += test_pending_retry_keeps_bytes();
    failures += test_pending_retry_keeps_rcbuf();

    /* Hard retry tests */
    failures += test_close_retry_after_blocked_control();
    failures += test_close_retry_would_block();
    failures += test_reset_on_unknown_stream();
    failures += test_transport_close_clears_state();

    if (failures == 0)
        printf("test_transport_bridge: all tests passed\n");
    else
        fprintf(stderr, "test_transport_bridge: %d failure(s)\n", failures);

    return failures;
}
