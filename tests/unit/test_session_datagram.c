#include <moq/codec.h>
#include "test_session_support.h"
#include "../../core/src/session/session_internal.h"
#include <string.h>

int main(void)
{
    int failures = 0;

    /* ============================================================== */
    /* 1. Pub-targeted datagram API: end-to-end                       */
    /* ============================================================== */

    /* == Pub datagram → receive with pub handle valid =============== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_publish_cfg_t pc; moq_publish_cfg_init(&pc);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("test") };
        pc.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        pc.track_name = MOQ_BYTES_LITERAL("vid");
        moq_publication_t cpub;
        MOQ_TEST_CHECK(moq_session_publish(c, &pc, 1000, &cpub) == MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_PUBLISH_REQUEST);
        moq_publication_t spub = ev.u.publish_request.pub;
        moq_event_cleanup(&ev);
        moq_accept_publish_cfg_t apc; moq_accept_publish_cfg_init(&apc);
        MOQ_TEST_CHECK(moq_session_accept_publish(sv, spub, &apc, 1000)
            == MOQ_OK);
        pump_actions_to_peer(sv, c, 1000);
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_PUBLISH_OK);
        moq_event_cleanup(&ev);

        moq_rcbuf_t *pay = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"pubdata", 7, &pay);
        uint8_t props[] = { 0xAA, 0xBB };
        MOQ_TEST_CHECK(moq_session_send_pub_object_datagram(c, cpub,
            5, 3, 200, true, pay, props, 2, 2000) == MOQ_OK);
        moq_rcbuf_decref(pay);

        pump_actions_to_peer(c, sv, 2000);

        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
        moq_object_received_event_t *obj = &ev.u.object_received;
        MOQ_TEST_CHECK(obj->datagram == true);
        MOQ_TEST_CHECK(obj->pub._opaque != 0);
        MOQ_TEST_CHECK(obj->sub._opaque == 0);
        MOQ_TEST_CHECK(obj->group_id == 5);
        MOQ_TEST_CHECK(obj->object_id == 3);
        MOQ_TEST_CHECK(obj->publisher_priority == 200);
        MOQ_TEST_CHECK(obj->end_of_group == true);
        MOQ_TEST_CHECK(obj->payload != NULL);
        MOQ_TEST_CHECK(moq_rcbuf_len(obj->payload) == 7);
        MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(obj->payload), "pubdata", 7) == 0);
        MOQ_TEST_CHECK(obj->properties != NULL);
        MOQ_TEST_CHECK(moq_rcbuf_len(obj->properties) == 2);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Pub status datagram ======================================= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_publish_cfg_t pc; moq_publish_cfg_init(&pc);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("test") };
        pc.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        pc.track_name = MOQ_BYTES_LITERAL("vid");
        moq_publication_t cpub;
        moq_session_publish(c, &pc, 1000, &cpub);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_publication_t spub = ev.u.publish_request.pub;
        moq_event_cleanup(&ev);
        moq_accept_publish_cfg_t apc; moq_accept_publish_cfg_init(&apc);
        moq_session_accept_publish(sv, spub, &apc, 1000);
        pump_actions_to_peer(sv, c, 1000);
        moq_session_poll_events(c, &ev, 1);
        moq_event_cleanup(&ev);

        MOQ_TEST_CHECK(moq_session_send_pub_status_datagram(c, cpub,
            1, 0, 128, MOQ_OBJECT_END_OF_TRACK, 2000) == MOQ_OK);

        pump_actions_to_peer(c, sv, 2000);
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
        MOQ_TEST_CHECK(ev.u.object_received.datagram == true);
        MOQ_TEST_CHECK(ev.u.object_received.status == MOQ_OBJECT_END_OF_TRACK);
        MOQ_TEST_CHECK(ev.u.object_received.payload == NULL);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Pub datagram: wrong role (subscriber) rejected ============= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_publish_cfg_t pc; moq_publish_cfg_init(&pc);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("test") };
        pc.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        pc.track_name = MOQ_BYTES_LITERAL("vid");
        moq_publication_t cpub;
        moq_session_publish(c, &pc, 1000, &cpub);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_publication_t spub = ev.u.publish_request.pub;
        moq_event_cleanup(&ev);
        moq_accept_publish_cfg_t apc; moq_accept_publish_cfg_init(&apc);
        moq_session_accept_publish(sv, spub, &apc, 1000);
        pump_actions_to_peer(sv, c, 1000);
        moq_session_poll_events(c, &ev, 1);
        moq_event_cleanup(&ev);

        moq_rcbuf_t *pay = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"x", 1, &pay);
        MOQ_TEST_CHECK(moq_session_send_pub_object_datagram(sv, spub,
            0, 1, 128, false, pay, NULL, 0, 2000) == MOQ_ERR_WRONG_STATE);
        moq_rcbuf_decref(pay);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Pub datagram: stale handle rejected ======================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_publication_t bad = { 0xDEADBEEF };
        moq_rcbuf_t *pay = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"x", 1, &pay);
        MOQ_TEST_CHECK(moq_session_send_pub_object_datagram(c, bad,
            0, 1, 128, false, pay, NULL, 0, 1000) == MOQ_ERR_STALE_HANDLE);
        moq_rcbuf_decref(pay);

        MOQ_TEST_CHECK(moq_session_send_pub_status_datagram(c, bad,
            0, 1, 128, MOQ_OBJECT_NORMAL, 1000) == MOQ_ERR_STALE_HANDLE);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* ============================================================== */
    /* 2. Receive budget accounting                                   */
    /* ============================================================== */

    /* == Datagram over max_recv_buf returns WB, no event ============ */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t c_extra = MOQ_SESSION_CFG_INIT;
        c_extra.max_receive_buffer_bytes = 8;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &c_extra, NULL);

        moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        scfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        scfg.track_name = MOQ_BYTES_LITERAL("t");
        scfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t sub;
        moq_session_subscribe(c, &scfg, 1000, &sub);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t sv_sub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
        moq_accept_subscribe_cfg_t asub; moq_accept_subscribe_cfg_init(&asub);
        moq_session_accept_subscribe(sv, sv_sub, &asub, 1000);
        pump_actions_to_peer(sv, c, 1000);
        moq_session_poll_events(c, &ev, 1);
        moq_event_cleanup(&ev);

        moq_rcbuf_t *big = NULL;
        uint8_t bigdata[20];
        memset(bigdata, 'X', sizeof(bigdata));
        moq_rcbuf_create(&alloc, bigdata, sizeof(bigdata), &big);
        MOQ_TEST_CHECK(moq_session_send_object_datagram(sv, sv_sub,
            0, 1, 128, false, big, NULL, 0, 2000) == MOQ_OK);
        moq_rcbuf_decref(big);
        pump_actions_to_peer(sv, c, 2000);

        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 0);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Budget released after polling event ======================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t c_extra = MOQ_SESSION_CFG_INIT;
        c_extra.max_receive_buffer_bytes = 32;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &c_extra, NULL);

        moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        scfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        scfg.track_name = MOQ_BYTES_LITERAL("t");
        scfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t sub;
        moq_session_subscribe(c, &scfg, 1000, &sub);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t sv_sub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
        moq_accept_subscribe_cfg_t asub; moq_accept_subscribe_cfg_init(&asub);
        moq_session_accept_subscribe(sv, sv_sub, &asub, 1000);
        pump_actions_to_peer(sv, c, 1000);
        moq_session_poll_events(c, &ev, 1);
        moq_event_cleanup(&ev);

        uint8_t d1[16];
        memset(d1, 'A', sizeof(d1));
        moq_rcbuf_t *p1 = NULL;
        moq_rcbuf_create(&alloc, d1, sizeof(d1), &p1);
        moq_session_send_object_datagram(sv, sv_sub,
            0, 1, 128, false, p1, NULL, 0, 2000);
        moq_rcbuf_decref(p1);
        pump_actions_to_peer(sv, c, 2000);

        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
        moq_event_cleanup(&ev);

        uint8_t d2[16];
        memset(d2, 'B', sizeof(d2));
        moq_rcbuf_t *p2 = NULL;
        moq_rcbuf_create(&alloc, d2, sizeof(d2), &p2);
        moq_session_send_object_datagram(sv, sv_sub,
            0, 2, 128, false, p2, NULL, 0, 3000);
        moq_rcbuf_decref(p2);
        pump_actions_to_peer(sv, c, 3000);

        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Datagram over per-object payload cap dropped, no event ====== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t c_extra = MOQ_SESSION_CFG_INIT;
        c_extra.max_object_payload_size = 16;
        c_extra.max_receive_buffer_bytes = 128;   /* aggregate not the limiter */
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &c_extra, NULL);

        moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        scfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        scfg.track_name = MOQ_BYTES_LITERAL("t");
        scfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t sub;
        moq_session_subscribe(c, &scfg, 1000, &sub);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t sv_sub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
        moq_accept_subscribe_cfg_t asub; moq_accept_subscribe_cfg_init(&asub);
        moq_session_accept_subscribe(sv, sv_sub, &asub, 1000);
        pump_actions_to_peer(sv, c, 1000);
        moq_session_poll_events(c, &ev, 1);
        moq_event_cleanup(&ev);

        /* 64-byte payload exceeds the 16-byte per-object cap but fits the
         * 128-byte aggregate budget; the receiver must drop it. */
        uint8_t big[64]; memset(big, 'X', sizeof(big));
        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(&alloc, big, sizeof(big), &p);
        MOQ_TEST_CHECK(moq_session_send_object_datagram(sv, sv_sub,
            0, 1, 128, false, p, NULL, 0, 2000) == MOQ_OK);
        moq_rcbuf_decref(p);
        pump_actions_to_peer(sv, c, 2000);

        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 0);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Datagram payload+properties over aggregate budget dropped ==== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t c_extra = MOQ_SESSION_CFG_INIT;
        c_extra.max_object_payload_size = 64;     /* payload alone is allowed */
        c_extra.max_receive_buffer_bytes = 32;    /* payload+props exceed this */
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &c_extra, NULL);

        moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        scfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        scfg.track_name = MOQ_BYTES_LITERAL("t");
        scfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t sub;
        moq_session_subscribe(c, &scfg, 1000, &sub);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t sv_sub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
        moq_accept_subscribe_cfg_t asub; moq_accept_subscribe_cfg_init(&asub);
        moq_session_accept_subscribe(sv, sv_sub, &asub, 1000);
        pump_actions_to_peer(sv, c, 1000);
        moq_session_poll_events(c, &ev, 1);
        moq_event_cleanup(&ev);

        /* payload 8 (<= per-object cap) + properties 40 = 48 > 32 aggregate. */
        uint8_t pay[8]; memset(pay, 'A', sizeof(pay));
        uint8_t props[40]; memset(props, 'P', sizeof(props));
        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(&alloc, pay, sizeof(pay), &p);
        MOQ_TEST_CHECK(moq_session_send_object_datagram(sv, sv_sub,
            0, 1, 128, false, p, props, sizeof(props), 2000) == MOQ_OK);
        moq_rcbuf_decref(p);
        pump_actions_to_peer(sv, c, 2000);

        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 0);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* ============================================================== */
    /* 3. Unknown alias and malformed close                           */
    /* ============================================================== */

    /* == Unknown alias → drop silently, no event =================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        uint8_t wire[32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, wire, sizeof(wire));
        moq_d16_object_datagram_t dg;
        memset(&dg, 0, sizeof(dg));
        dg.track_alias = 9999;
        dg.group_id = 0;
        dg.object_id = 1;
        dg.publisher_priority = 128;
        dg.payload = (const uint8_t *)"x";
        dg.payload_len = 1;
        moq_d16_encode_object_datagram(&w, &dg);

        MOQ_TEST_CHECK(moq_session_on_datagram(c, wire,
            moq_buf_writer_offset(&w), 1000) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);

        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 0);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Invalid type closes session with 0x3 ====================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        uint8_t bad[] = { 0x10, 0x01, 0x00, 0x01, 128, 'x' };
        moq_session_on_datagram(c, bad, sizeof(bad), 1000);

        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_CLOSED);
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SESSION_CLOSED);
        MOQ_TEST_CHECK(ev.u.closed.code == 0x3);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Zero-payload non-status closes session ===================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        uint8_t bad[] = { 0x00, 0x01, 0x00, 0x01, 128 };
        moq_session_on_datagram(c, bad, sizeof(bad), 1000);

        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_CLOSED);
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SESSION_CLOSED);
        MOQ_TEST_CHECK(ev.u.closed.code == 0x3);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* ============================================================== */
    /* 4. Large datagram encode (exceeds old 2048 stack cap)          */
    /* ============================================================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t c_extra = MOQ_SESSION_CFG_INIT;
        c_extra.send_buffer_size = 8192;
        moq_session_cfg_t s_extra = MOQ_SESSION_CFG_INIT;
        s_extra.max_receive_buffer_bytes = 8192;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &c_extra, &s_extra);

        moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        scfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        scfg.track_name = MOQ_BYTES_LITERAL("t");
        scfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t sub;
        moq_session_subscribe(c, &scfg, 1000, &sub);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_subscription_t sv_sub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
        moq_accept_subscribe_cfg_t asub; moq_accept_subscribe_cfg_init(&asub);
        moq_session_accept_subscribe(sv, sv_sub, &asub, 1000);
        pump_actions_to_peer(sv, c, 1000);
        moq_session_poll_events(c, &ev, 1);
        moq_event_cleanup(&ev);

        uint8_t bigdata[3000];
        memset(bigdata, 'Z', sizeof(bigdata));
        moq_rcbuf_t *big = NULL;
        moq_rcbuf_create(&alloc, bigdata, sizeof(bigdata), &big);
        MOQ_TEST_CHECK(moq_session_send_object_datagram(sv, sv_sub,
            0, 1, 128, false, big, NULL, 0, 2000) == MOQ_OK);
        moq_rcbuf_decref(big);

        moq_action_t acts[4];
        size_t na = moq_session_poll_actions(sv, acts, 4);
        MOQ_TEST_CHECK(na >= 1);
        bool found_dg = false;
        for (size_t i = 0; i < na; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_DATAGRAM) {
                MOQ_TEST_CHECK(acts[i].u.send_datagram.len > 2048);
                found_dg = true;
            }
            moq_action_cleanup(&acts[i]);
        }
        MOQ_TEST_CHECK(found_dg);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    MOQ_TEST_PASS("test_session_datagram");
    return failures;
}
