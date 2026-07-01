#include <moq/subscriber.h>
#include <moq/sim.h>
#include <moq/publisher.h>
#include <moq/codec.h>
#include <moq/control.h>
#include "test_session_support.h"
#include <stdio.h>
#include <string.h>

static int failures = 0;

typedef struct {
    int subscribed;
    int error;
    int draining;
    int closed;
    int done;
    moq_sub_track_t *last_track;
    moq_sub_track_t *last_done_track;
    moq_request_error_t last_error;
    uint64_t close_code;
    uint64_t last_done_status;
} sub_cb_state_t;

static void on_subscribed(void *ctx, moq_sub_track_t *track) {
    sub_cb_state_t *s = (sub_cb_state_t *)ctx;
    s->subscribed++; s->last_track = track;
}
static void on_error(void *ctx, moq_sub_track_t *track,
                      moq_request_error_t code, moq_bytes_t reason) {
    sub_cb_state_t *s = (sub_cb_state_t *)ctx;
    (void)track; (void)reason;
    s->error++; s->last_error = code;
}
static void on_draining(void *ctx) {
    sub_cb_state_t *s = (sub_cb_state_t *)ctx;
    s->draining++;
}
static void on_closed(void *ctx, uint64_t code) {
    sub_cb_state_t *s = (sub_cb_state_t *)ctx;
    s->closed++; s->close_code = code;
}
static void on_subscribe_done(void *ctx, moq_sub_track_t *track,
                              uint64_t status_code) {
    sub_cb_state_t *s = (sub_cb_state_t *)ctx;
    s->done++; s->last_done_track = track; s->last_done_status = status_code;
}

/* -- Helpers -------------------------------------------------------- */

static void setup_pair(test_alloc_state_t *as, moq_alloc_t *alloc,
    moq_simpair_t **sp, moq_publisher_t **pub, moq_pub_track_t **track)
{
    *as = (test_alloc_state_t){0};
    *alloc = test_allocator(as);
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = alloc; cfg.seed = 42; cfg.initial_now_us = 1000;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 16;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 16;
    moq_simpair_create(&cfg, sp);
    moq_simpair_start(*sp);
    moq_simpair_run_until_quiescent(*sp, 8, NULL);
    moq_event_t ev;
    moq_session_poll_events(moq_simpair_client(*sp), &ev, 1);
    moq_session_poll_events(moq_simpair_server(*sp), &ev, 1);

    moq_pub_cfg_t pcfg; moq_pub_cfg_init_sized(&pcfg, sizeof(pcfg));
    pcfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    moq_pub_create(moq_simpair_server(*sp), alloc, &pcfg, pub);
    moq_pub_track_cfg_t tcfg; moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("test") };
    tcfg.track_namespace.parts = ns;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_pub_add_track(*pub, &tcfg, moq_simpair_now_us(*sp), track);
}

/* -- Tests ---------------------------------------------------------- */

static void test_subscribe_accepted(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *ptrack;
    setup_pair(&as, &alloc, &sp, &pub, &ptrack);

    sub_cb_state_t cbs = {0};
    moq_sub_cfg_t cfg; moq_sub_cfg_init(&cfg);
    cfg.callbacks.ctx = &cbs;
    cfg.callbacks.on_subscribed = on_subscribed;

    moq_subscriber_t *sub = NULL;
    MOQ_TEST_CHECK(moq_sub_create(moq_simpair_client(sp), &alloc,
        &cfg, &sub) == MOQ_OK);

    moq_sub_track_cfg_t tcfg; moq_sub_track_cfg_init(&tcfg);
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("test") };
    tcfg.track_namespace.parts = ns;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_sub_track_t *track = NULL;
    MOQ_TEST_CHECK(moq_sub_subscribe(sub, &tcfg,
        moq_simpair_now_us(sp), &track) == MOQ_OK);

    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_pub_tick(pub, moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_sub_tick(sub, moq_simpair_now_us(sp));

    MOQ_TEST_CHECK(cbs.subscribed == 1);
    MOQ_TEST_CHECK(cbs.last_track == track);

    moq_sub_destroy(sub);
    moq_pub_destroy(pub);
    { moq_event_t ev;
      while (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev);
      while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev); }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("subscribe_accepted");
}

static void test_subscribe_rejected(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *ptrack;
    setup_pair(&as, &alloc, &sp, &pub, &ptrack);

    sub_cb_state_t cbs = {0};
    moq_sub_cfg_t cfg; moq_sub_cfg_init(&cfg);
    cfg.callbacks.ctx = &cbs;
    cfg.callbacks.on_subscribe_error = on_error;

    moq_subscriber_t *sub = NULL;
    moq_sub_create(moq_simpair_client(sp), &alloc, &cfg, &sub);

    /* Use REJECT_ALL publisher to get a definite rejection. */
    moq_pub_destroy(pub);
    moq_pub_cfg_t rpcfg; moq_pub_cfg_init_sized(&rpcfg, sizeof(rpcfg));
    rpcfg.accept_mode = MOQ_PUB_REJECT_ALL;
    moq_pub_create(moq_simpair_server(sp), &alloc, &rpcfg, &pub);
    moq_pub_track_cfg_t rtcfg; moq_pub_track_cfg_init(&rtcfg);
    moq_bytes_t rns[] = { MOQ_BYTES_LITERAL("test") };
    rtcfg.track_namespace.parts = rns;
    rtcfg.track_namespace.count = 1;
    rtcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_pub_add_track(pub, &rtcfg, moq_simpair_now_us(sp), &ptrack);

    moq_sub_track_cfg_t tcfg; moq_sub_track_cfg_init(&tcfg);
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("test") };
    tcfg.track_namespace.parts = ns;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_sub_track_t *track = NULL;
    moq_sub_subscribe(sub, &tcfg, moq_simpair_now_us(sp), &track);

    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_pub_tick(pub, moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_sub_tick(sub, moq_simpair_now_us(sp));

    MOQ_TEST_CHECK(cbs.error == 1);

    moq_sub_destroy(sub);
    moq_pub_destroy(pub);
    { moq_event_t ev;
      while (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev);
      while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev); }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("subscribe_rejected");
}

static void test_object_received(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *ptrack;
    setup_pair(&as, &alloc, &sp, &pub, &ptrack);

    moq_sub_cfg_t cfg; moq_sub_cfg_init(&cfg);
    moq_subscriber_t *sub = NULL;
    moq_sub_create(moq_simpair_client(sp), &alloc, &cfg, &sub);

    moq_sub_track_cfg_t tcfg; moq_sub_track_cfg_init(&tcfg);
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("test") };
    tcfg.track_namespace.parts = ns;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_sub_track_t *track = NULL;
    moq_sub_subscribe(sub, &tcfg, moq_simpair_now_us(sp), &track);

    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_pub_tick(pub, moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_sub_tick(sub, moq_simpair_now_us(sp));

    moq_rcbuf_t *pay = NULL;
    moq_rcbuf_create(&alloc, (const uint8_t *)"hello", 5, &pay);
    moq_pub_write_object(pub, ptrack, 0, 0, pay, moq_simpair_now_us(sp));
    moq_rcbuf_decref(pay);
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    moq_sub_tick(sub, moq_simpair_now_us(sp));

    moq_sub_object_t obj;
    MOQ_TEST_CHECK(moq_sub_poll_object(sub, &obj) == MOQ_OK);
    MOQ_TEST_CHECK(obj.track == track);
    MOQ_TEST_CHECK(obj.group_id == 0);
    MOQ_TEST_CHECK(obj.object_id == 0);
    MOQ_TEST_CHECK(obj.payload != NULL);
    MOQ_TEST_CHECK(moq_rcbuf_len(obj.payload) == 5);
    MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(obj.payload), "hello", 5) == 0);
    moq_sub_object_cleanup(&obj);

    MOQ_TEST_CHECK(moq_sub_poll_object(sub, &obj) == MOQ_DONE);

    moq_sub_destroy(sub);
    moq_pub_destroy(pub);
    { moq_event_t ev;
      while (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev); }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("object_received");
}

static void test_session_closed(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *ptrack;
    setup_pair(&as, &alloc, &sp, &pub, &ptrack);

    sub_cb_state_t cbs = {0};
    moq_sub_cfg_t cfg; moq_sub_cfg_init(&cfg);
    cfg.callbacks.ctx = &cbs;
    cfg.callbacks.on_closed = on_closed;

    moq_subscriber_t *sub = NULL;
    moq_sub_create(moq_simpair_client(sp), &alloc, &cfg, &sub);

    uint8_t bad[] = { 0x3F, 0x00, 0x01, 0x00 };
    moq_session_on_control_bytes(moq_simpair_client(sp), bad,
        sizeof(bad), moq_simpair_now_us(sp));
    moq_sub_tick(sub, moq_simpair_now_us(sp));

    MOQ_TEST_CHECK(cbs.closed == 1);

    moq_sub_destroy(sub);
    moq_pub_destroy(pub);
    { moq_event_t ev;
      while (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev); }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("sub_session_closed");
}

static void test_object_cleanup_releases_refs(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *ptrack;
    setup_pair(&as, &alloc, &sp, &pub, &ptrack);

    moq_sub_cfg_t cfg; moq_sub_cfg_init(&cfg);
    moq_subscriber_t *sub = NULL;
    moq_sub_create(moq_simpair_client(sp), &alloc, &cfg, &sub);

    moq_sub_track_cfg_t tcfg; moq_sub_track_cfg_init(&tcfg);
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("test") };
    tcfg.track_namespace.parts = ns;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_sub_track_t *track = NULL;
    moq_sub_subscribe(sub, &tcfg, moq_simpair_now_us(sp), &track);

    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_pub_tick(pub, moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_sub_tick(sub, moq_simpair_now_us(sp));

    moq_rcbuf_t *pay = NULL;
    moq_rcbuf_create(&alloc, (const uint8_t *)"data", 4, &pay);
    moq_pub_write_object(pub, ptrack, 0, 0, pay, moq_simpair_now_us(sp));
    moq_pub_write_object(pub, ptrack, 0, 1, pay, moq_simpair_now_us(sp));
    moq_rcbuf_decref(pay);
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    moq_sub_tick(sub, moq_simpair_now_us(sp));

    moq_sub_object_t o1, o2;
    MOQ_TEST_CHECK(moq_sub_poll_object(sub, &o1) == MOQ_OK);
    MOQ_TEST_CHECK(moq_sub_poll_object(sub, &o2) == MOQ_OK);
    MOQ_TEST_CHECK(moq_sub_poll_object(sub, &o1) == MOQ_DONE);
    moq_sub_object_cleanup(&o1);
    moq_sub_object_cleanup(&o2);

    moq_sub_destroy(sub);
    moq_pub_destroy(pub);
    { moq_event_t ev;
      while (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev); }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("object_cleanup_releases_refs");
}

static void test_object_queue_full_pending_retry(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *ptrack;
    setup_pair(&as, &alloc, &sp, &pub, &ptrack);

    moq_sub_cfg_t cfg; moq_sub_cfg_init(&cfg);
    cfg.max_objects = 1;
    moq_subscriber_t *sub = NULL;
    moq_sub_create(moq_simpair_client(sp), &alloc, &cfg, &sub);

    moq_sub_track_cfg_t tcfg; moq_sub_track_cfg_init(&tcfg);
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("test") };
    tcfg.track_namespace.parts = ns;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_sub_track_t *track = NULL;
    moq_sub_subscribe(sub, &tcfg, moq_simpair_now_us(sp), &track);

    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_pub_tick(pub, moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_sub_tick(sub, moq_simpair_now_us(sp));

    moq_rcbuf_t *p1 = NULL, *p2 = NULL;
    moq_rcbuf_create(&alloc, (const uint8_t *)"first", 5, &p1);
    moq_rcbuf_create(&alloc, (const uint8_t *)"second", 6, &p2);
    moq_pub_write_object(pub, ptrack, 0, 0, p1, moq_simpair_now_us(sp));
    moq_pub_write_object(pub, ptrack, 0, 1, p2, moq_simpair_now_us(sp));
    moq_rcbuf_decref(p1);
    moq_rcbuf_decref(p2);
    moq_simpair_run_until_quiescent(sp, 16, NULL);

    /* First tick: queue cap=1, first object queued, second goes
     * to pending → WOULD_BLOCK. */
    moq_result_t rc = moq_sub_tick(sub, moq_simpair_now_us(sp));
    MOQ_TEST_CHECK(rc == MOQ_ERR_WOULD_BLOCK);

    /* Poll the first object to free the slot. */
    moq_sub_object_t obj;
    MOQ_TEST_CHECK(moq_sub_poll_object(sub, &obj) == MOQ_OK);
    MOQ_TEST_CHECK(obj.object_id == 0);
    MOQ_TEST_CHECK(moq_rcbuf_len(obj.payload) == 5);
    MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(obj.payload), "first", 5) == 0);
    moq_sub_object_cleanup(&obj);

    /* Retry tick: pending flushed into queue. */
    rc = moq_sub_tick(sub, moq_simpair_now_us(sp));
    MOQ_TEST_CHECK(rc == MOQ_OK);

    MOQ_TEST_CHECK(moq_sub_poll_object(sub, &obj) == MOQ_OK);
    MOQ_TEST_CHECK(obj.object_id == 1);
    MOQ_TEST_CHECK(moq_rcbuf_len(obj.payload) == 6);
    MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(obj.payload), "second", 6) == 0);
    moq_sub_object_cleanup(&obj);

    MOQ_TEST_CHECK(moq_sub_poll_object(sub, &obj) == MOQ_DONE);

    moq_sub_destroy(sub);
    moq_pub_destroy(pub);
    { moq_event_t ev;
      while (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev); }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("object_queue_full_pending_retry");
}

static void test_streaming_chunk_rejected(void) {
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);

    moq_simpair_cfg_t spcfg = MOQ_SIMPAIR_CFG_INIT;
    spcfg.alloc = &alloc; spcfg.seed = 42; spcfg.initial_now_us = 1000;
    spcfg.client_send_request_capacity = true;
    spcfg.client_initial_request_capacity = 16;
    spcfg.server_send_request_capacity = true;
    spcfg.server_initial_request_capacity = 16;
    spcfg.client_streaming_objects = true;
    moq_simpair_t *sp = NULL;
    moq_simpair_create(&spcfg, &sp);
    moq_simpair_start(sp);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      moq_session_poll_events(moq_simpair_client(sp), &ev, 1);
      moq_session_poll_events(moq_simpair_server(sp), &ev, 1); }

    moq_pub_cfg_t pcfg; moq_pub_cfg_init_sized(&pcfg, sizeof(pcfg));
    pcfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    moq_publisher_t *pub = NULL;
    moq_pub_create(moq_simpair_server(sp), &alloc, &pcfg, &pub);
    moq_pub_track_cfg_t tcfg; moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("test") };
    tcfg.track_namespace.parts = ns;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_pub_track_t *ptrack = NULL;
    moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &ptrack);

    moq_sub_cfg_t cfg; moq_sub_cfg_init(&cfg);
    moq_subscriber_t *sub = NULL;
    moq_sub_create(moq_simpair_client(sp), &alloc, &cfg, &sub);

    moq_sub_track_cfg_t stcfg; moq_sub_track_cfg_init(&stcfg);
    stcfg.track_namespace.parts = ns;
    stcfg.track_namespace.count = 1;
    stcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_sub_track_t *track = NULL;
    moq_sub_subscribe(sub, &stcfg, moq_simpair_now_us(sp), &track);

    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_pub_tick(pub, moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_sub_tick(sub, moq_simpair_now_us(sp));

    /* Write a streamed object: begin + data + end. */
    moq_pub_begin_object_cfg_t bcfg;
    moq_pub_begin_object_cfg_init(&bcfg);
    bcfg.group_id = 0; bcfg.object_id = 0; bcfg.payload_length = 3;
    moq_pub_begin_object(pub, ptrack, &bcfg, moq_simpair_now_us(sp));
    moq_rcbuf_t *chunk = NULL;
    moq_rcbuf_create(&alloc, (const uint8_t *)"abc", 3, &chunk);
    moq_pub_write_data(pub, ptrack, chunk, moq_simpair_now_us(sp));
    moq_rcbuf_decref(chunk);
    moq_pub_end_object(pub, ptrack, moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 16, NULL);

    /* Tick should hit OBJECT_CHUNK and return WRONG_STATE. */
    moq_result_t rc = moq_sub_tick(sub, moq_simpair_now_us(sp));
    MOQ_TEST_CHECK(rc == MOQ_ERR_WRONG_STATE);

    /* No fake objects should be queued. */
    moq_sub_object_t obj;
    MOQ_TEST_CHECK(moq_sub_poll_object(sub, &obj) == MOQ_DONE);

    moq_sub_destroy(sub);
    moq_pub_destroy(pub);
    { moq_event_t ev;
      while (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev);
      while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev); }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("streaming_chunk_rejected");
}

static void test_datagram_object_received(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *ptrack;
    setup_pair(&as, &alloc, &sp, &pub, &ptrack);

    moq_sub_cfg_t cfg; moq_sub_cfg_init(&cfg);
    moq_subscriber_t *sub = NULL;
    moq_sub_create(moq_simpair_client(sp), &alloc, &cfg, &sub);

    moq_sub_track_cfg_t tcfg; moq_sub_track_cfg_init(&tcfg);
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("test") };
    tcfg.track_namespace.parts = ns;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_sub_track_t *track = NULL;
    moq_sub_subscribe(sub, &tcfg, moq_simpair_now_us(sp), &track);

    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_pub_tick(pub, moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_sub_tick(sub, moq_simpair_now_us(sp));

    moq_rcbuf_t *pay = NULL;
    moq_rcbuf_create(&alloc, (const uint8_t *)"dgdata", 6, &pay);
    moq_pub_object_cfg_t obj; moq_pub_object_cfg_init(&obj);
    obj.group_id = 1;
    obj.object_id = 0;
    obj.payload = pay;
    obj.datagram = true;
    obj.properties = NULL;
    moq_pub_write_object_ex(pub, ptrack, &obj, moq_simpair_now_us(sp));
    moq_rcbuf_decref(pay);

    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_sub_tick(sub, moq_simpair_now_us(sp));

    moq_sub_object_t o;
    MOQ_TEST_CHECK(moq_sub_poll_object(sub, &o) == MOQ_OK);
    MOQ_TEST_CHECK(o.datagram == true);
    MOQ_TEST_CHECK(o.group_id == 1);
    MOQ_TEST_CHECK(o.object_id == 0);
    MOQ_TEST_CHECK(o.payload != NULL);
    MOQ_TEST_CHECK(moq_rcbuf_len(o.payload) == 6);
    MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(o.payload), "dgdata", 6) == 0);
    moq_sub_object_cleanup(&o);

    moq_sub_destroy(sub);
    moq_pub_destroy(pub);
    { moq_event_t ev;
      while (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev); }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("datagram_object_received");
}

static void test_status_datagram_received(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *ptrack;
    setup_pair(&as, &alloc, &sp, &pub, &ptrack);

    moq_sub_cfg_t cfg; moq_sub_cfg_init(&cfg);
    moq_subscriber_t *sub = NULL;
    moq_sub_create(moq_simpair_client(sp), &alloc, &cfg, &sub);

    moq_sub_track_cfg_t tcfg; moq_sub_track_cfg_init(&tcfg);
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("test") };
    tcfg.track_namespace.parts = ns;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_sub_track_t *track = NULL;
    moq_sub_subscribe(sub, &tcfg, moq_simpair_now_us(sp), &track);

    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_pub_tick(pub, moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_sub_tick(sub, moq_simpair_now_us(sp));

    moq_pub_object_cfg_t obj; moq_pub_object_cfg_init(&obj);
    obj.group_id = 0;
    obj.object_id = 0;
    obj.datagram = true;
    obj.has_status = true;
    obj.status = MOQ_OBJECT_END_OF_GROUP;
    moq_pub_write_object_ex(pub, ptrack, &obj, moq_simpair_now_us(sp));

    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_sub_tick(sub, moq_simpair_now_us(sp));

    moq_sub_object_t o;
    MOQ_TEST_CHECK(moq_sub_poll_object(sub, &o) == MOQ_OK);
    MOQ_TEST_CHECK(o.datagram == true);
    MOQ_TEST_CHECK(o.payload == NULL);
    MOQ_TEST_CHECK(o.status == MOQ_OBJECT_END_OF_GROUP);
    moq_sub_object_cleanup(&o);

    moq_sub_destroy(sub);
    moq_pub_destroy(pub);
    { moq_event_t ev;
      while (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev); }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("status_datagram_received");
}

static void test_datagram_with_properties(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *ptrack;
    setup_pair(&as, &alloc, &sp, &pub, &ptrack);

    moq_sub_cfg_t cfg; moq_sub_cfg_init(&cfg);
    moq_subscriber_t *sub = NULL;
    moq_sub_create(moq_simpair_client(sp), &alloc, &cfg, &sub);

    moq_sub_track_cfg_t tcfg; moq_sub_track_cfg_init(&tcfg);
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("test") };
    tcfg.track_namespace.parts = ns;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_sub_track_t *track = NULL;
    moq_sub_subscribe(sub, &tcfg, moq_simpair_now_us(sp), &track);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_pub_tick(pub, moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_sub_tick(sub, moq_simpair_now_us(sp));

    moq_rcbuf_t *pay = NULL, *props = NULL;
    moq_rcbuf_create(&alloc, (const uint8_t *)"body", 4, &pay);
    moq_rcbuf_create(&alloc, (const uint8_t *)"\x01\x01\xAA", 3, &props);
    moq_pub_object_cfg_t obj; moq_pub_object_cfg_init(&obj);
    obj.group_id = 0; obj.object_id = 0;
    obj.payload = pay; obj.properties = props;
    obj.datagram = true;
    moq_pub_write_object_ex(pub, ptrack, &obj, moq_simpair_now_us(sp));
    moq_rcbuf_decref(pay);
    moq_rcbuf_decref(props);

    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_sub_tick(sub, moq_simpair_now_us(sp));

    moq_sub_object_t o;
    MOQ_TEST_CHECK(moq_sub_poll_object(sub, &o) == MOQ_OK);
    MOQ_TEST_CHECK(o.datagram == true);
    MOQ_TEST_CHECK(o.payload != NULL);
    MOQ_TEST_CHECK(moq_rcbuf_len(o.payload) == 4);
    MOQ_TEST_CHECK(o.properties != NULL);
    MOQ_TEST_CHECK(moq_rcbuf_len(o.properties) == 3);
    MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(o.properties), "\x01\x01\xAA", 3) == 0);
    moq_sub_object_cleanup(&o);

    moq_sub_destroy(sub);
    moq_pub_destroy(pub);
    { moq_event_t ev;
      while (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev); }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("datagram_with_properties");
}

static void test_pending_retry_with_properties(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *ptrack;
    setup_pair(&as, &alloc, &sp, &pub, &ptrack);

    moq_sub_cfg_t cfg; moq_sub_cfg_init(&cfg);
    cfg.max_objects = 1;
    moq_subscriber_t *sub = NULL;
    moq_sub_create(moq_simpair_client(sp), &alloc, &cfg, &sub);

    moq_sub_track_cfg_t tcfg; moq_sub_track_cfg_init(&tcfg);
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("test") };
    tcfg.track_namespace.parts = ns;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_sub_track_t *track = NULL;
    moq_sub_subscribe(sub, &tcfg, moq_simpair_now_us(sp), &track);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_pub_tick(pub, moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_sub_tick(sub, moq_simpair_now_us(sp));

    moq_rcbuf_t *p1 = NULL, *p2 = NULL, *props = NULL;
    moq_rcbuf_create(&alloc, (const uint8_t *)"obj1", 4, &p1);
    moq_rcbuf_create(&alloc, (const uint8_t *)"obj2", 4, &p2);
    moq_rcbuf_create(&alloc, (const uint8_t *)"\x01\x01\xBB", 3, &props);

    moq_pub_object_cfg_t o1; moq_pub_object_cfg_init(&o1);
    o1.group_id = 0; o1.object_id = 0;
    o1.payload = p1; o1.datagram = true;
    moq_pub_write_object_ex(pub, ptrack, &o1, moq_simpair_now_us(sp));

    moq_pub_object_cfg_t o2; moq_pub_object_cfg_init(&o2);
    o2.group_id = 0; o2.object_id = 1;
    o2.payload = p2; o2.properties = props; o2.datagram = true;
    moq_pub_write_object_ex(pub, ptrack, &o2, moq_simpair_now_us(sp));
    moq_rcbuf_decref(p1);
    moq_rcbuf_decref(p2);
    moq_rcbuf_decref(props);

    moq_simpair_run_until_quiescent(sp, 16, NULL);

    moq_result_t rc = moq_sub_tick(sub, moq_simpair_now_us(sp));
    MOQ_TEST_CHECK(rc == MOQ_ERR_WOULD_BLOCK);

    moq_sub_object_t obj;
    MOQ_TEST_CHECK(moq_sub_poll_object(sub, &obj) == MOQ_OK);
    MOQ_TEST_CHECK(obj.object_id == 0);
    moq_sub_object_cleanup(&obj);

    rc = moq_sub_tick(sub, moq_simpair_now_us(sp));
    MOQ_TEST_CHECK(rc == MOQ_OK);

    MOQ_TEST_CHECK(moq_sub_poll_object(sub, &obj) == MOQ_OK);
    MOQ_TEST_CHECK(obj.object_id == 1);
    MOQ_TEST_CHECK(obj.properties != NULL);
    MOQ_TEST_CHECK(moq_rcbuf_len(obj.properties) == 3);
    MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(obj.properties), "\x01\x01\xBB", 3) == 0);
    moq_sub_object_cleanup(&obj);

    moq_sub_destroy(sub);
    moq_pub_destroy(pub);
    { moq_event_t ev;
      while (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev); }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("pending_retry_with_properties");
}

/* Byte-budget regression: the facade caps payload+properties bytes retained in
 * its object queue, independent of the object COUNT cap. The session releases
 * its own receive budget once an event is polled, so without this cap a peer
 * could park unbounded bytes in the (count-only) facade queue. With a default
 * count cap (256) but a tiny byte cap, the second whole object must divert to
 * the pending slot (WOULD_BLOCK), and must flush after the first is polled. */
static void test_object_byte_budget_payload_only(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *ptrack;
    setup_pair(&as, &alloc, &sp, &pub, &ptrack);

    moq_sub_cfg_t cfg; moq_sub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.max_queued_object_bytes = 8;  /* count cap stays at default 256 */
    moq_subscriber_t *sub = NULL;
    moq_sub_create(moq_simpair_client(sp), &alloc, &cfg, &sub);

    moq_sub_track_cfg_t tcfg; moq_sub_track_cfg_init(&tcfg);
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("test") };
    tcfg.track_namespace.parts = ns;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_sub_track_t *track = NULL;
    moq_sub_subscribe(sub, &tcfg, moq_simpair_now_us(sp), &track);

    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_pub_tick(pub, moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_sub_tick(sub, moq_simpair_now_us(sp));

    /* Two 8-byte objects: the first fills the byte budget, the second must
     * divert to pending rather than grow retained bytes without bound. */
    moq_rcbuf_t *p1 = NULL, *p2 = NULL;
    moq_rcbuf_create(&alloc, (const uint8_t *)"AAAAAAAA", 8, &p1);
    moq_rcbuf_create(&alloc, (const uint8_t *)"BBBBBBBB", 8, &p2);
    moq_pub_write_object(pub, ptrack, 0, 0, p1, moq_simpair_now_us(sp));
    moq_pub_write_object(pub, ptrack, 0, 1, p2, moq_simpair_now_us(sp));
    moq_rcbuf_decref(p1);
    moq_rcbuf_decref(p2);
    moq_simpair_run_until_quiescent(sp, 16, NULL);

    moq_result_t rc = moq_sub_tick(sub, moq_simpair_now_us(sp));
    MOQ_TEST_CHECK(rc == MOQ_ERR_WOULD_BLOCK);

    moq_sub_object_t obj;
    MOQ_TEST_CHECK(moq_sub_poll_object(sub, &obj) == MOQ_OK);
    MOQ_TEST_CHECK(obj.object_id == 0);
    MOQ_TEST_CHECK(moq_rcbuf_len(obj.payload) == 8);
    moq_sub_object_cleanup(&obj);

    /* Polling the first frees its bytes; retry flushes the pending object. */
    rc = moq_sub_tick(sub, moq_simpair_now_us(sp));
    MOQ_TEST_CHECK(rc == MOQ_OK);

    MOQ_TEST_CHECK(moq_sub_poll_object(sub, &obj) == MOQ_OK);
    MOQ_TEST_CHECK(obj.object_id == 1);
    MOQ_TEST_CHECK(moq_rcbuf_len(obj.payload) == 8);
    moq_sub_object_cleanup(&obj);

    MOQ_TEST_CHECK(moq_sub_poll_object(sub, &obj) == MOQ_DONE);

    moq_sub_destroy(sub);
    moq_pub_destroy(pub);
    { moq_event_t ev;
      while (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev); }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("object_byte_budget_payload_only");
}

/* Properties bytes count against the same budget as payload. With a cap of 8
 * and a first object of payload(5)+properties(3)=8, the second object must
 * divert to pending. If properties were not counted the first object would
 * measure 5 bytes and the second would slip under the cap -- so this also
 * regresses against forgetting to charge properties. */
static void test_object_byte_budget_with_properties(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *ptrack;
    setup_pair(&as, &alloc, &sp, &pub, &ptrack);

    moq_sub_cfg_t cfg; moq_sub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.max_queued_object_bytes = 8;
    moq_subscriber_t *sub = NULL;
    moq_sub_create(moq_simpair_client(sp), &alloc, &cfg, &sub);

    moq_sub_track_cfg_t tcfg; moq_sub_track_cfg_init(&tcfg);
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("test") };
    tcfg.track_namespace.parts = ns;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_sub_track_t *track = NULL;
    moq_sub_subscribe(sub, &tcfg, moq_simpair_now_us(sp), &track);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_pub_tick(pub, moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_sub_tick(sub, moq_simpair_now_us(sp));

    moq_rcbuf_t *p1 = NULL, *p2 = NULL, *props = NULL;
    moq_rcbuf_create(&alloc, (const uint8_t *)"55555", 5, &p1);
    moq_rcbuf_create(&alloc, (const uint8_t *)"55555", 5, &p2);
    moq_rcbuf_create(&alloc, (const uint8_t *)"\x01\x01\xAA", 3, &props);

    moq_pub_object_cfg_t o1; moq_pub_object_cfg_init(&o1);
    o1.group_id = 0; o1.object_id = 0;
    o1.payload = p1; o1.properties = props; o1.datagram = true;
    moq_pub_write_object_ex(pub, ptrack, &o1, moq_simpair_now_us(sp));

    moq_pub_object_cfg_t o2; moq_pub_object_cfg_init(&o2);
    o2.group_id = 0; o2.object_id = 1;
    o2.payload = p2; o2.datagram = true;
    moq_pub_write_object_ex(pub, ptrack, &o2, moq_simpair_now_us(sp));
    moq_rcbuf_decref(p1);
    moq_rcbuf_decref(p2);
    moq_rcbuf_decref(props);
    moq_simpair_run_until_quiescent(sp, 16, NULL);

    moq_result_t rc = moq_sub_tick(sub, moq_simpair_now_us(sp));
    MOQ_TEST_CHECK(rc == MOQ_ERR_WOULD_BLOCK);

    moq_sub_object_t obj;
    MOQ_TEST_CHECK(moq_sub_poll_object(sub, &obj) == MOQ_OK);
    MOQ_TEST_CHECK(obj.object_id == 0);
    MOQ_TEST_CHECK(moq_rcbuf_len(obj.properties) == 3);
    moq_sub_object_cleanup(&obj);

    rc = moq_sub_tick(sub, moq_simpair_now_us(sp));
    MOQ_TEST_CHECK(rc == MOQ_OK);

    MOQ_TEST_CHECK(moq_sub_poll_object(sub, &obj) == MOQ_OK);
    MOQ_TEST_CHECK(obj.object_id == 1);
    moq_sub_object_cleanup(&obj);

    MOQ_TEST_CHECK(moq_sub_poll_object(sub, &obj) == MOQ_DONE);

    moq_sub_destroy(sub);
    moq_pub_destroy(pub);
    { moq_event_t ev;
      while (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev); }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("object_byte_budget_with_properties");
}

/* Real old-storage ABI canary: a caller compiled against the original struct
 * allocated only the frozen v0 prefix (it ended before the first appended field,
 * on_goaway), then its own bytes immediately after. Pointer-only cfg_init must
 * clear/stamp ONLY that prefix -- the old memset(sizeof current) would write the
 * appended region and clobber the byte right after the old struct. The union's
 * first member forces alignment for the cfg* cast; the cfg is never accessed
 * through the full struct type (the storage is old-sized). */
static void test_sub_cfg_init_old_prefix_no_overflow(void) {
    union {
        moq_sub_cfg_t aligner;   /* alignment only */
        struct {
            unsigned char prefix[offsetof(moq_sub_cfg_t, on_goaway)];
            uint64_t canary;     /* the old caller's next bytes */
        } box;
    } u;
    memset(&u, 0xAB, sizeof(u));
    moq_sub_cfg_init((moq_sub_cfg_t *)&u.box);

    uint32_t ss;
    memcpy(&ss, u.box.prefix + offsetof(moq_sub_cfg_t, struct_size), sizeof(ss));
    MOQ_TEST_CHECK(ss == (uint32_t)offsetof(moq_sub_cfg_t, on_goaway));
    MOQ_TEST_CHECK(u.box.canary == 0xABABABABABABABABULL);  /* not overflowed */

    /* Sized init on the full struct clears + stamps everything and enables the
     * appended fields. */
    moq_sub_cfg_t full;
    memset(&full, 0xAB, sizeof(full));
    moq_sub_cfg_init_sized(&full, sizeof(full));
    MOQ_TEST_CHECK(full.struct_size == sizeof(full));
    MOQ_TEST_CHECK(full.on_goaway == NULL);
    MOQ_TEST_CHECK(full.max_queued_object_bytes == 0);
    MOQ_TEST_PASS("sub_cfg_init_old_prefix_no_overflow");
}

/* A cfg whose struct_size predates max_queued_object_bytes must still create a
 * subscriber and fall back to the default byte cap (16 MiB), so a normal small
 * object flows through without diverting to pending. */
static void test_byte_budget_old_struct_size(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *ptrack;
    setup_pair(&as, &alloc, &sp, &pub, &ptrack);

    moq_sub_cfg_t cfg; moq_sub_cfg_init(&cfg);
    /* Truncate to a prefix that excludes the new trailing field. */
    cfg.struct_size = offsetof(moq_sub_cfg_t, max_queued_object_bytes);
    cfg.max_queued_object_bytes = 8;  /* must be ignored (outside struct_size) */
    moq_subscriber_t *sub = NULL;
    MOQ_TEST_CHECK(moq_sub_create(moq_simpair_client(sp), &alloc,
        &cfg, &sub) == MOQ_OK);

    moq_sub_track_cfg_t tcfg; moq_sub_track_cfg_init(&tcfg);
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("test") };
    tcfg.track_namespace.parts = ns;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_sub_track_t *track = NULL;
    moq_sub_subscribe(sub, &tcfg, moq_simpair_now_us(sp), &track);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_pub_tick(pub, moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_sub_tick(sub, moq_simpair_now_us(sp));

    /* 16 bytes > the (ignored) cfg value of 8 -- default cap is in effect. */
    moq_rcbuf_t *pay = NULL;
    moq_rcbuf_create(&alloc, (const uint8_t *)"0123456789ABCDEF", 16, &pay);
    moq_pub_write_object(pub, ptrack, 0, 0, pay, moq_simpair_now_us(sp));
    moq_rcbuf_decref(pay);
    moq_simpair_run_until_quiescent(sp, 16, NULL);

    MOQ_TEST_CHECK(moq_sub_tick(sub, moq_simpair_now_us(sp)) == MOQ_OK);

    moq_sub_object_t obj;
    MOQ_TEST_CHECK(moq_sub_poll_object(sub, &obj) == MOQ_OK);
    MOQ_TEST_CHECK(moq_rcbuf_len(obj.payload) == 16);
    moq_sub_object_cleanup(&obj);

    moq_sub_destroy(sub);
    moq_pub_destroy(pub);
    { moq_event_t ev;
      while (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev); }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("byte_budget_old_struct_size");
}

static void test_track_error_query(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *ptrack;
    setup_pair(&as, &alloc, &sp, &pub, &ptrack);

    moq_sub_cfg_t cfg; moq_sub_cfg_init(&cfg);
    moq_subscriber_t *sub = NULL;
    moq_sub_create(moq_simpair_client(sp), &alloc, &cfg, &sub);

    /* Recreate publisher as REJECT_ALL. */
    moq_pub_destroy(pub);
    moq_pub_cfg_t rpcfg; moq_pub_cfg_init_sized(&rpcfg, sizeof(rpcfg));
    rpcfg.accept_mode = MOQ_PUB_REJECT_ALL;
    moq_pub_create(moq_simpair_server(sp), &alloc, &rpcfg, &pub);
    moq_pub_track_cfg_t ptcfg; moq_pub_track_cfg_init(&ptcfg);
    moq_bytes_t rns[] = { MOQ_BYTES_LITERAL("test") };
    ptcfg.track_namespace.parts = rns;
    ptcfg.track_namespace.count = 1;
    ptcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_pub_add_track(pub, &ptcfg, moq_simpair_now_us(sp), &ptrack);

    moq_sub_track_cfg_t tcfg; moq_sub_track_cfg_init(&tcfg);
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("test") };
    tcfg.track_namespace.parts = ns;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_sub_track_t *track = NULL;
    moq_sub_subscribe(sub, &tcfg, moq_simpair_now_us(sp), &track);

    MOQ_TEST_CHECK(moq_sub_track_get_state(track) == MOQ_SUB_TRACK_PENDING);
    moq_request_error_t err_code = 0;
    MOQ_TEST_CHECK(!moq_sub_track_get_error(track, &err_code));

    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_pub_tick(pub, moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_sub_tick(sub, moq_simpair_now_us(sp));

    MOQ_TEST_CHECK(moq_sub_track_get_state(track) == MOQ_SUB_TRACK_ERROR);
    MOQ_TEST_CHECK(moq_sub_track_get_error(track, &err_code));
    MOQ_TEST_CHECK(!moq_sub_track_is_active(track));

    moq_sub_destroy(sub);
    moq_pub_destroy(pub);
    { moq_event_t ev;
      while (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev);
      while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev); }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("track_error_query");
}

static void test_track_state_after_close(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *ptrack;
    setup_pair(&as, &alloc, &sp, &pub, &ptrack);

    moq_sub_cfg_t cfg; moq_sub_cfg_init(&cfg);
    moq_subscriber_t *sub = NULL;
    moq_sub_create(moq_simpair_client(sp), &alloc, &cfg, &sub);

    moq_sub_track_cfg_t tcfg; moq_sub_track_cfg_init(&tcfg);
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("test") };
    tcfg.track_namespace.parts = ns;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_sub_track_t *track = NULL;
    moq_sub_subscribe(sub, &tcfg, moq_simpair_now_us(sp), &track);

    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_pub_tick(pub, moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_sub_tick(sub, moq_simpair_now_us(sp));
    MOQ_TEST_CHECK(moq_sub_track_is_active(track));

    uint8_t bad[] = { 0x3F, 0x00, 0x01, 0x00 };
    moq_session_on_control_bytes(moq_simpair_client(sp), bad,
        sizeof(bad), moq_simpair_now_us(sp));
    moq_sub_tick(sub, moq_simpair_now_us(sp));

    MOQ_TEST_CHECK(!moq_sub_track_is_active(track));
    MOQ_TEST_CHECK(moq_sub_track_get_state(track) == MOQ_SUB_TRACK_DONE);

    moq_sub_destroy(sub);
    moq_pub_destroy(pub);
    { moq_event_t ev;
      while (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev); }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("track_state_after_close");
}

static void test_track_status_ok(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *ptrack;
    setup_pair(&as, &alloc, &sp, &pub, &ptrack);

    moq_sub_cfg_t cfg; moq_sub_cfg_init(&cfg);
    moq_subscriber_t *sub = NULL;
    moq_sub_create(moq_simpair_client(sp), &alloc, &cfg, &sub);

    moq_sub_status_cfg_t scfg;
    moq_sub_status_cfg_init(&scfg);
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("test") };
    scfg.track_namespace.parts = ns;
    scfg.track_namespace.count = 1;
    scfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_sub_status_req_t *req = NULL;
    MOQ_TEST_CHECK(moq_sub_track_status(sub, &scfg,
        moq_simpair_now_us(sp), &req) == MOQ_OK);
    MOQ_TEST_CHECK(req != NULL);

    moq_simpair_run_until_quiescent(sp, 8, NULL);

    /* Server accepts via raw session API. */
    moq_event_t ev;
    MOQ_TEST_CHECK(moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1);
    MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_TRACK_STATUS_REQUEST);
    moq_accept_track_status_cfg_t acfg;
    moq_accept_track_status_cfg_init(&acfg);
    moq_session_accept_track_status(moq_simpair_server(sp),
        ev.u.track_status_request.handle, &acfg, moq_simpair_now_us(sp));
    moq_event_cleanup(&ev);

    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_sub_tick(sub, moq_simpair_now_us(sp));

    moq_sub_status_result_t res;
    MOQ_TEST_CHECK(moq_sub_poll_status(sub, &res) == MOQ_OK);
    MOQ_TEST_CHECK(res.kind == MOQ_SUB_STATUS_OK);
    MOQ_TEST_CHECK(res.request == req);
    /* Accept without largest/expires → both false. */
    MOQ_TEST_CHECK(!res.has_largest);
    MOQ_TEST_CHECK(!res.has_expires);
    MOQ_TEST_CHECK(moq_sub_poll_status(sub, &res) == MOQ_DONE);

    moq_sub_destroy(sub);
    moq_pub_destroy(pub);
    { moq_event_t d;
      while (moq_session_poll_events(moq_simpair_server(sp), &d, 1) == 1)
          moq_event_cleanup(&d); }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("track_status_ok");
}

static void test_track_status_error(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *ptrack;
    setup_pair(&as, &alloc, &sp, &pub, &ptrack);

    moq_sub_cfg_t cfg; moq_sub_cfg_init(&cfg);
    moq_subscriber_t *sub = NULL;
    moq_sub_create(moq_simpair_client(sp), &alloc, &cfg, &sub);

    moq_sub_status_cfg_t scfg;
    moq_sub_status_cfg_init(&scfg);
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("test") };
    scfg.track_namespace.parts = ns;
    scfg.track_namespace.count = 1;
    scfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_sub_status_req_t *req = NULL;
    moq_sub_track_status(sub, &scfg, moq_simpair_now_us(sp), &req);

    moq_simpair_run_until_quiescent(sp, 8, NULL);

    moq_event_t ev;
    MOQ_TEST_CHECK(moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1);
    MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_TRACK_STATUS_REQUEST);
    moq_reject_track_status_cfg_t rcfg;
    moq_reject_track_status_cfg_init(&rcfg);
    rcfg.error_code = 0x10;
    moq_session_reject_track_status(moq_simpair_server(sp),
        ev.u.track_status_request.handle, &rcfg, moq_simpair_now_us(sp));
    moq_event_cleanup(&ev);

    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_sub_tick(sub, moq_simpair_now_us(sp));

    moq_sub_status_result_t res;
    MOQ_TEST_CHECK(moq_sub_poll_status(sub, &res) == MOQ_OK);
    MOQ_TEST_CHECK(res.kind == MOQ_SUB_STATUS_ERROR);
    MOQ_TEST_CHECK(res.error_code == 0x10);
    MOQ_TEST_CHECK(!res.can_retry);
    MOQ_TEST_CHECK(res.retry_after_ms == 0);
    MOQ_TEST_CHECK(res.request == req);

    moq_sub_destroy(sub);
    moq_pub_destroy(pub);
    { moq_event_t d;
      while (moq_session_poll_events(moq_simpair_server(sp), &d, 1) == 1)
          moq_event_cleanup(&d); }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("track_status_error");
}

static void test_track_status_slot_reuse(void) {
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    moq_simpair_cfg_t spcfg = MOQ_SIMPAIR_CFG_INIT;
    spcfg.alloc = &alloc; spcfg.seed = 42; spcfg.initial_now_us = 1000;
    spcfg.client_send_request_capacity = true;
    spcfg.client_initial_request_capacity = 128;
    spcfg.server_send_request_capacity = true;
    spcfg.server_initial_request_capacity = 128;
    moq_simpair_t *sp = NULL;
    moq_simpair_create(&spcfg, &sp);
    moq_simpair_start(sp);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      moq_session_poll_events(moq_simpair_client(sp), &ev, 1);
      moq_session_poll_events(moq_simpair_server(sp), &ev, 1); }
    moq_publisher_t *pub = NULL; moq_pub_track_t *ptrack = NULL;
    (void)ptrack;

    moq_sub_cfg_t cfg; moq_sub_cfg_init(&cfg);
    moq_subscriber_t *sub = NULL;
    moq_sub_create(moq_simpair_client(sp), &alloc, &cfg, &sub);

    /* Issue 20 sequential status queries (pool=16), each accepted
     * and polled before the next. Proves slot reuse. */
    for (int i = 0; i < 20; i++) {
        moq_sub_status_cfg_t scfg;
        moq_sub_status_cfg_init(&scfg);
        moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("test") };
        scfg.track_namespace.parts = ns;
        scfg.track_namespace.count = 1;
        scfg.track_name = MOQ_BYTES_LITERAL("video");
        moq_sub_status_req_t *req = NULL;
        moq_result_t rc = moq_sub_track_status(sub, &scfg,
            moq_simpair_now_us(sp), &req);
        MOQ_TEST_CHECK(rc == MOQ_OK);

        moq_simpair_run_until_quiescent(sp, 8, NULL);
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(moq_simpair_server(sp),
            &ev, 1) == 1);
        moq_accept_track_status_cfg_t acfg;
        moq_accept_track_status_cfg_init(&acfg);
        moq_session_accept_track_status(moq_simpair_server(sp),
            ev.u.track_status_request.handle, &acfg,
            moq_simpair_now_us(sp));
        moq_event_cleanup(&ev);

        moq_simpair_run_until_quiescent(sp, 8, NULL);
        moq_sub_tick(sub, moq_simpair_now_us(sp));

        moq_sub_status_result_t res;
        MOQ_TEST_CHECK(moq_sub_poll_status(sub, &res) == MOQ_OK);
        MOQ_TEST_CHECK(res.kind == MOQ_SUB_STATUS_OK);
    }

    moq_sub_destroy(sub);
    moq_pub_destroy(pub);
    { moq_event_t d;
      while (moq_session_poll_events(moq_simpair_server(sp), &d, 1) == 1)
          moq_event_cleanup(&d); }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("track_status_slot_reuse");
}

static void test_track_status_session_close(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *ptrack;
    setup_pair(&as, &alloc, &sp, &pub, &ptrack);

    moq_sub_cfg_t cfg; moq_sub_cfg_init(&cfg);
    moq_subscriber_t *sub = NULL;
    moq_sub_create(moq_simpair_client(sp), &alloc, &cfg, &sub);

    moq_sub_status_cfg_t scfg;
    moq_sub_status_cfg_init(&scfg);
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("test") };
    scfg.track_namespace.parts = ns;
    scfg.track_namespace.count = 1;
    scfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_sub_status_req_t *req = NULL;
    moq_sub_track_status(sub, &scfg, moq_simpair_now_us(sp), &req);

    /* Close session before status response arrives. */
    uint8_t bad[] = { 0x3F, 0x00, 0x01, 0x00 };
    moq_session_on_control_bytes(moq_simpair_client(sp), bad,
        sizeof(bad), moq_simpair_now_us(sp));
    moq_sub_tick(sub, moq_simpair_now_us(sp));

    /* No status result should be queued. */
    moq_sub_status_result_t res;
    MOQ_TEST_CHECK(moq_sub_poll_status(sub, &res) == MOQ_DONE);

    /* Further requests should fail. */
    moq_sub_status_req_t *req2 = NULL;
    MOQ_TEST_CHECK(moq_sub_track_status(sub, &scfg,
        moq_simpair_now_us(sp), &req2) == MOQ_ERR_CLOSED);

    moq_sub_destroy(sub);
    moq_pub_destroy(pub);
    { moq_event_t d;
      while (moq_session_poll_events(moq_simpair_server(sp), &d, 1) == 1)
          moq_event_cleanup(&d); }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("track_status_session_close");
}

static void test_track_status_ok_with_fields(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *ptrack;
    setup_pair(&as, &alloc, &sp, &pub, &ptrack);
    (void)ptrack;

    moq_sub_cfg_t cfg; moq_sub_cfg_init(&cfg);
    moq_subscriber_t *sub = NULL;
    moq_sub_create(moq_simpair_client(sp), &alloc, &cfg, &sub);

    moq_sub_status_cfg_t scfg;
    moq_sub_status_cfg_init(&scfg);
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("test") };
    scfg.track_namespace.parts = ns;
    scfg.track_namespace.count = 1;
    scfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_sub_status_req_t *req = NULL;
    MOQ_TEST_CHECK(moq_sub_track_status(sub, &scfg,
        moq_simpair_now_us(sp), &req) == MOQ_OK);
    MOQ_TEST_CHECK(req != NULL);

    moq_simpair_run_until_quiescent(sp, 8, NULL);

    /* Server receives the TRACK_STATUS_REQUEST, then we inject a
     * raw REQUEST_OK with LARGEST_OBJECT and EXPIRES params. */
    moq_event_t ev;
    MOQ_TEST_CHECK(moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1);
    MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_TRACK_STATUS_REQUEST);
    moq_event_cleanup(&ev);

    /* Build REQUEST_OK with LARGEST_OBJECT(g=10,o=5) + EXPIRES(3000ms). */
    uint8_t ok_buf[128];
    moq_buf_writer_t ow;
    moq_buf_writer_init(&ow, ok_buf, sizeof(ok_buf));
    uint8_t loc_buf[16];
    moq_buf_writer_t lw;
    moq_buf_writer_init(&lw, loc_buf, sizeof(loc_buf));
    moq_buf_write_varint(&lw, 10);
    moq_buf_write_varint(&lw, 5);
    uint8_t exp_buf[8];
    size_t exp_len = moq_quic_varint_encode(3000, exp_buf, sizeof(exp_buf));
    moq_kvp_entry_t params[2] = {
        { .type = MOQ_MSG_PARAM_EXPIRES,
          .value = exp_buf, .value_len = exp_len,
          .is_varint = true },
        { .type = MOQ_MSG_PARAM_LARGEST_OBJECT,
          .value = loc_buf, .value_len = moq_buf_writer_offset(&lw),
          .is_varint = false },
    };
    MOQ_TEST_CHECK(moq_d16_encode_request_ok(&ow, 0, params, 2) == MOQ_OK);

    MOQ_TEST_CHECK(moq_session_on_control_bytes(moq_simpair_client(sp),
        ok_buf, moq_buf_writer_offset(&ow),
        moq_simpair_now_us(sp) + 1) == MOQ_OK);
    MOQ_TEST_CHECK(moq_session_state(moq_simpair_client(sp)) ==
        MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK(moq_sub_tick(sub, moq_simpair_now_us(sp) + 2) == MOQ_OK);

    moq_sub_status_result_t res;
    MOQ_TEST_CHECK(moq_sub_poll_status(sub, &res) == MOQ_OK);
    MOQ_TEST_CHECK(res.kind == MOQ_SUB_STATUS_OK);
    MOQ_TEST_CHECK(res.request == req);
    MOQ_TEST_CHECK(res.has_largest);
    MOQ_TEST_CHECK(res.largest_group == 10);
    MOQ_TEST_CHECK(res.largest_object == 5);
    MOQ_TEST_CHECK(res.has_expires);
    MOQ_TEST_CHECK(res.expires_ms == 3000);

    moq_sub_destroy(sub);
    moq_pub_destroy(pub);
    { moq_event_t d;
      while (moq_session_poll_events(moq_simpair_server(sp), &d, 1) == 1)
          moq_event_cleanup(&d); }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("track_status_ok_with_fields");
}

static void test_track_status_error_with_retry(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *ptrack;
    setup_pair(&as, &alloc, &sp, &pub, &ptrack);

    moq_sub_cfg_t cfg; moq_sub_cfg_init(&cfg);
    moq_subscriber_t *sub = NULL;
    moq_sub_create(moq_simpair_client(sp), &alloc, &cfg, &sub);

    moq_sub_status_cfg_t scfg;
    moq_sub_status_cfg_init(&scfg);
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("test") };
    scfg.track_namespace.parts = ns;
    scfg.track_namespace.count = 1;
    scfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_sub_status_req_t *req = NULL;
    MOQ_TEST_CHECK(moq_sub_track_status(sub, &scfg,
        moq_simpair_now_us(sp), &req) == MOQ_OK);
    MOQ_TEST_CHECK(req != NULL);

    moq_simpair_run_until_quiescent(sp, 8, NULL);

    moq_event_t ev;
    MOQ_TEST_CHECK(moq_session_poll_events(
        moq_simpair_server(sp), &ev, 1) == 1);
    MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_TRACK_STATUS_REQUEST);
    moq_event_cleanup(&ev);

    uint8_t err_buf[128];
    moq_buf_writer_t ew;
    moq_buf_writer_init(&ew, err_buf, sizeof(err_buf));
    MOQ_TEST_CHECK(moq_d16_encode_request_error(&ew, 0, 0x42,
        5001, (const uint8_t *)"nope", 4) == MOQ_OK);

    MOQ_TEST_CHECK(moq_session_on_control_bytes(moq_simpair_client(sp),
        err_buf, moq_buf_writer_offset(&ew),
        moq_simpair_now_us(sp) + 1) == MOQ_OK);
    MOQ_TEST_CHECK(moq_sub_tick(sub, moq_simpair_now_us(sp) + 2) == MOQ_OK);

    moq_sub_status_result_t res;
    MOQ_TEST_CHECK(moq_sub_poll_status(sub, &res) == MOQ_OK);
    MOQ_TEST_CHECK(res.kind == MOQ_SUB_STATUS_ERROR);
    MOQ_TEST_CHECK(res.error_code == 0x42);
    MOQ_TEST_CHECK(res.can_retry == true);
    MOQ_TEST_CHECK(res.retry_after_ms == 5000);
    MOQ_TEST_CHECK(res.request == req);

    moq_sub_destroy(sub);
    moq_pub_destroy(pub);
    { moq_event_t d;
      while (moq_session_poll_events(moq_simpair_server(sp), &d, 1) == 1)
          moq_event_cleanup(&d); }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("track_status_error_with_retry");
}

/* -- Fetch helpers -------------------------------------------------- */

static void setup_fetch_pair(test_alloc_state_t *as, moq_alloc_t *alloc,
    moq_simpair_t **sp)
{
    *as = (test_alloc_state_t){0};
    *alloc = test_allocator(as);
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = alloc; cfg.seed = 42; cfg.initial_now_us = 1000;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 16;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 16;
    moq_simpair_create(&cfg, sp);
    moq_simpair_start(*sp);
    moq_simpair_run_until_quiescent(*sp, 8, NULL);
    moq_event_t ev;
    moq_session_poll_events(moq_simpair_client(*sp), &ev, 1);
    moq_session_poll_events(moq_simpair_server(*sp), &ev, 1);
}

static void server_accept_and_write(moq_simpair_t *sp, moq_alloc_t *alloc,
    const uint8_t *payload_data, size_t payload_len)
{
    moq_session_t *sv = moq_simpair_server(sp);
    uint64_t now = moq_simpair_now_us(sp);

    moq_event_t ev;
    MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
    MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_FETCH_REQUEST);
    moq_fetch_t sv_fetch = ev.u.fetch_request.fetch;

    moq_accept_fetch_cfg_t acfg;
    moq_accept_fetch_cfg_init(&acfg);
    acfg.end_group = 1;
    MOQ_TEST_CHECK(moq_session_accept_fetch(sv, sv_fetch, &acfg, now) == MOQ_OK);

    moq_rcbuf_t *pay = NULL;
    moq_rcbuf_create(alloc, payload_data, payload_len, &pay);
    moq_fetch_object_cfg_t ocfg;
    moq_fetch_object_cfg_init(&ocfg);
    ocfg.group_id = 0;
    ocfg.object_id = 0;
    ocfg.publisher_priority = 128;
    ocfg.payload = pay;
    MOQ_TEST_CHECK(moq_session_write_fetch_object(sv, sv_fetch, &ocfg, now) == MOQ_OK);
    moq_rcbuf_decref(pay);

    MOQ_TEST_CHECK(moq_session_end_fetch(sv, sv_fetch, now) == MOQ_OK);
}

/* -- Fetch tests ---------------------------------------------------- */

static void test_fetch_ok_object_complete(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    setup_fetch_pair(&as, &alloc, &sp);

    moq_sub_cfg_t cfg; moq_sub_cfg_init(&cfg);
    moq_subscriber_t *sub = NULL;
    MOQ_TEST_CHECK(moq_sub_create(moq_simpair_client(sp), &alloc,
        &cfg, &sub) == MOQ_OK);

    moq_sub_fetch_cfg_t fcfg;
    moq_sub_fetch_cfg_init(&fcfg);
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("test") };
    fcfg.track_namespace.parts = ns;
    fcfg.track_namespace.count = 1;
    fcfg.track_name = MOQ_BYTES_LITERAL("video");
    fcfg.end_group = 1;
    moq_sub_fetch_req_t *req = NULL;
    MOQ_TEST_CHECK(moq_sub_fetch(sub, &fcfg, moq_simpair_now_us(sp),
        &req) == MOQ_OK);
    MOQ_TEST_CHECK(req != NULL);

    moq_simpair_run_until_quiescent(sp, 8, NULL);
    server_accept_and_write(sp, &alloc, (const uint8_t *)"hello", 5);
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    MOQ_TEST_CHECK(moq_sub_tick(sub, moq_simpair_now_us(sp)) == MOQ_OK);

    moq_sub_fetch_item_t item;

    MOQ_TEST_CHECK(moq_sub_poll_fetch(sub, &item) == MOQ_OK);
    MOQ_TEST_CHECK(item.kind == MOQ_SUB_FETCH_OK);
    MOQ_TEST_CHECK(item.request == req);
    moq_sub_fetch_item_cleanup(&item);

    MOQ_TEST_CHECK(moq_sub_poll_fetch(sub, &item) == MOQ_OK);
    MOQ_TEST_CHECK(item.kind == MOQ_SUB_FETCH_OBJECT);
    MOQ_TEST_CHECK(item.request == req);
    MOQ_TEST_CHECK(item.u.object.group_id == 0);
    MOQ_TEST_CHECK(item.u.object.object_id == 0);
    MOQ_TEST_CHECK(item.u.object.publisher_priority == 128);
    MOQ_TEST_CHECK(item.u.object.payload != NULL);
    MOQ_TEST_CHECK(moq_rcbuf_len(item.u.object.payload) == 5);
    MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(item.u.object.payload),
        "hello", 5) == 0);
    moq_sub_fetch_item_cleanup(&item);

    MOQ_TEST_CHECK(moq_sub_poll_fetch(sub, &item) == MOQ_OK);
    MOQ_TEST_CHECK(item.kind == MOQ_SUB_FETCH_COMPLETE);
    MOQ_TEST_CHECK(item.request == req);
    moq_sub_fetch_item_cleanup(&item);

    MOQ_TEST_CHECK(moq_sub_poll_fetch(sub, &item) == MOQ_DONE);

    moq_sub_destroy(sub);
    { moq_event_t d;
      while (moq_session_poll_events(moq_simpair_server(sp), &d, 1) == 1)
          moq_event_cleanup(&d); }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("fetch_ok_object_complete");
}

/* A FETCH object (or gap) may arrive on the object stream BEFORE FETCH_OK
 * (draft-16 FETCH §9.16 / FETCH_OK §9.17: FETCH_OK may come at any time relative
 * to object delivery; a relay can FIN the object stream ahead of its FETCH_OK).
 * The subscriber must queue and surface such early objects while the request is
 * still PENDING, then surface the later FETCH_OK and FETCH_COMPLETE. (Without
 * this, an early object was dropped and a FETCH-delivered catalog never
 * reached the receiver.) */
static void test_fetch_object_before_ok(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    setup_fetch_pair(&as, &alloc, &sp);
    moq_session_t *client = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);

    moq_sub_cfg_t cfg; moq_sub_cfg_init(&cfg);
    moq_subscriber_t *sub = NULL;
    MOQ_TEST_CHECK(moq_sub_create(client, &alloc, &cfg, &sub) == MOQ_OK);

    moq_sub_fetch_cfg_t fcfg; moq_sub_fetch_cfg_init(&fcfg);
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("test") };
    fcfg.track_namespace.parts = ns; fcfg.track_namespace.count = 1;
    fcfg.track_name = MOQ_BYTES_LITERAL("video");
    fcfg.end_group = 1;
    moq_sub_fetch_req_t *req = NULL;
    MOQ_TEST_CHECK(moq_sub_fetch(sub, &fcfg, now, &req) == MOQ_OK);

    /* Learn the wire request_id from the client's outbound FETCH action, and
     * consume the action so the simpair server does not auto-respond -- this
     * test injects the FETCH response bytes directly, in object-before-OK order. */
    uint64_t fetch_rid = 0; bool have_rid = false;
    { moq_action_t acts[16]; size_t n;
      while ((n = moq_session_poll_actions(client, acts, 16)) > 0)
          for (size_t i = 0; i < n; i++) {
              if (!have_rid && acts[i].kind == MOQ_ACTION_SEND_CONTROL) {
                  moq_control_envelope_t env; moq_buf_reader_t r;
                  moq_buf_reader_init(&r, acts[i].u.send_control.data,
                                      acts[i].u.send_control.len);
                  if (moq_control_decode_envelope(&r, &env) >= 0 &&
                      env.msg_type == MOQ_D16_FETCH) {
                      moq_bytes_t nsp[8]; moq_kvp_entry_t p[8];
                      moq_d16_fetch_t f; memset(&f, 0, sizeof(f));
                      f.params = p; f.params_cap = 8;
                      if (moq_d16_decode_fetch(env.payload, env.payload_len,
                                               nsp, 8, &f) >= 0) {
                          fetch_rid = f.request_id; have_rid = true;
                      }
                  }
              }
              moq_action_cleanup(&acts[i]);
          } }
    MOQ_TEST_CHECK(have_rid);

    /* Inject the FETCH object stream BEFORE FETCH_OK: FETCH_HEADER(rid) + one
     * object { group 0, object 0, "hello" }, then FIN. */
    uint8_t data[256];
    moq_buf_writer_t w; moq_buf_writer_init(&w, data, sizeof(data));
    MOQ_TEST_CHECK(moq_d16_encode_fetch_header(&w, fetch_rid) == MOQ_OK);
    moq_d16_fetch_object_t obj; memset(&obj, 0, sizeof(obj));
    obj.group_id = 0; obj.object_id = 0; obj.publisher_priority = 0;
    obj.payload = (const uint8_t *)"hello"; obj.payload_len = 5;
    MOQ_TEST_CHECK(moq_d16_encode_fetch_object(&w, &obj, NULL) == MOQ_OK);
    moq_stream_ref_t ref = moq_stream_ref_from_u64(77);
    MOQ_TEST_CHECK(moq_session_on_data_bytes(client, ref, data,
        moq_buf_writer_offset(&w), /*fin=*/true, now) >= 0);
    MOQ_TEST_CHECK(moq_sub_tick(sub, now) == MOQ_OK);

    moq_sub_fetch_item_t item;

    /* The early object is surfaced even though FETCH_OK has not arrived. */
    MOQ_TEST_CHECK(moq_sub_poll_fetch(sub, &item) == MOQ_OK);
    MOQ_TEST_CHECK(item.kind == MOQ_SUB_FETCH_OBJECT);
    MOQ_TEST_CHECK(item.request == req);
    MOQ_TEST_CHECK(item.u.object.payload != NULL &&
        moq_rcbuf_len(item.u.object.payload) == 5 &&
        memcmp(moq_rcbuf_data(item.u.object.payload), "hello", 5) == 0);
    moq_sub_fetch_item_cleanup(&item);

    /* Nothing more until FETCH_OK arrives. */
    MOQ_TEST_CHECK(moq_sub_poll_fetch(sub, &item) == MOQ_DONE);

    /* FETCH_OK (End=[0,1]); the object stream already FIN'd, so the session
     * follows with FETCH_COMPLETE. */
    uint8_t ctrl[64];
    moq_buf_writer_t cw; moq_buf_writer_init(&cw, ctrl, sizeof(ctrl));
    moq_d16_fetch_ok_t ok; memset(&ok, 0, sizeof(ok));
    ok.request_id = fetch_rid; ok.end_group = 0; ok.end_object = 1;
    MOQ_TEST_CHECK(moq_d16_encode_fetch_ok(&cw, &ok) == MOQ_OK);
    MOQ_TEST_CHECK(moq_session_on_control_bytes(client, ctrl,
        moq_buf_writer_offset(&cw), now) >= 0);
    MOQ_TEST_CHECK(moq_sub_tick(sub, now) == MOQ_OK);

    MOQ_TEST_CHECK(moq_sub_poll_fetch(sub, &item) == MOQ_OK);
    MOQ_TEST_CHECK(item.kind == MOQ_SUB_FETCH_OK);
    MOQ_TEST_CHECK(item.request == req);
    moq_sub_fetch_item_cleanup(&item);

    MOQ_TEST_CHECK(moq_sub_poll_fetch(sub, &item) == MOQ_OK);
    MOQ_TEST_CHECK(item.kind == MOQ_SUB_FETCH_COMPLETE);
    moq_sub_fetch_item_cleanup(&item);

    MOQ_TEST_CHECK(moq_sub_poll_fetch(sub, &item) == MOQ_DONE);

    moq_sub_destroy(sub);
    { moq_event_t d;
      while (moq_session_poll_events(moq_simpair_server(sp), &d, 1) == 1)
          moq_event_cleanup(&d); }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("fetch_object_before_ok");
}

static void test_fetch_gap(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    setup_fetch_pair(&as, &alloc, &sp);

    moq_sub_cfg_t cfg; moq_sub_cfg_init(&cfg);
    moq_subscriber_t *sub = NULL;
    MOQ_TEST_CHECK(moq_sub_create(moq_simpair_client(sp), &alloc,
        &cfg, &sub) == MOQ_OK);

    moq_sub_fetch_cfg_t fcfg;
    moq_sub_fetch_cfg_init(&fcfg);
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("test") };
    fcfg.track_namespace.parts = ns;
    fcfg.track_namespace.count = 1;
    fcfg.track_name = MOQ_BYTES_LITERAL("video");
    fcfg.end_group = 3;
    moq_sub_fetch_req_t *req = NULL;
    MOQ_TEST_CHECK(moq_sub_fetch(sub, &fcfg, moq_simpair_now_us(sp),
        &req) == MOQ_OK);

    moq_simpair_run_until_quiescent(sp, 8, NULL);

    /* Server accepts, writes gap, then ends. */
    moq_session_t *sv = moq_simpair_server(sp);
    moq_event_t ev;
    MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
    moq_fetch_t sv_fetch = ev.u.fetch_request.fetch;

    moq_accept_fetch_cfg_t acfg;
    moq_accept_fetch_cfg_init(&acfg);
    acfg.end_group = 3;
    MOQ_TEST_CHECK(moq_session_accept_fetch(sv, sv_fetch, &acfg,
        moq_simpair_now_us(sp)) == MOQ_OK);

    MOQ_TEST_CHECK(moq_session_write_fetch_range(sv, sv_fetch,
        MOQ_FETCH_RANGE_NON_EXISTENT, 1, 0,
        moq_simpair_now_us(sp)) == MOQ_OK);

    MOQ_TEST_CHECK(moq_session_end_fetch(sv, sv_fetch,
        moq_simpair_now_us(sp)) == MOQ_OK);

    moq_simpair_run_until_quiescent(sp, 16, NULL);
    MOQ_TEST_CHECK(moq_sub_tick(sub, moq_simpair_now_us(sp)) == MOQ_OK);

    moq_sub_fetch_item_t item;

    MOQ_TEST_CHECK(moq_sub_poll_fetch(sub, &item) == MOQ_OK);
    MOQ_TEST_CHECK(item.kind == MOQ_SUB_FETCH_OK);
    moq_sub_fetch_item_cleanup(&item);

    MOQ_TEST_CHECK(moq_sub_poll_fetch(sub, &item) == MOQ_OK);
    MOQ_TEST_CHECK(item.kind == MOQ_SUB_FETCH_GAP);
    MOQ_TEST_CHECK(item.u.gap.range_kind == MOQ_FETCH_RANGE_NON_EXISTENT);
    MOQ_TEST_CHECK(item.u.gap.group_id == 1);
    MOQ_TEST_CHECK(item.u.gap.object_id == 0);
    moq_sub_fetch_item_cleanup(&item);

    MOQ_TEST_CHECK(moq_sub_poll_fetch(sub, &item) == MOQ_OK);
    MOQ_TEST_CHECK(item.kind == MOQ_SUB_FETCH_COMPLETE);
    moq_sub_fetch_item_cleanup(&item);

    moq_sub_destroy(sub);
    { moq_event_t d;
      while (moq_session_poll_events(sv, &d, 1) == 1)
          moq_event_cleanup(&d); }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("fetch_gap");
}

static void test_fetch_error_retry_fields(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    setup_fetch_pair(&as, &alloc, &sp);

    moq_sub_cfg_t cfg; moq_sub_cfg_init(&cfg);
    moq_subscriber_t *sub = NULL;
    MOQ_TEST_CHECK(moq_sub_create(moq_simpair_client(sp), &alloc,
        &cfg, &sub) == MOQ_OK);

    moq_sub_fetch_cfg_t fcfg;
    moq_sub_fetch_cfg_init(&fcfg);
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("test") };
    fcfg.track_namespace.parts = ns;
    fcfg.track_namespace.count = 1;
    fcfg.track_name = MOQ_BYTES_LITERAL("video");
    fcfg.end_group = 1;
    moq_sub_fetch_req_t *req = NULL;
    MOQ_TEST_CHECK(moq_sub_fetch(sub, &fcfg, moq_simpair_now_us(sp),
        &req) == MOQ_OK);

    moq_simpair_run_until_quiescent(sp, 8, NULL);

    moq_session_t *sv = moq_simpair_server(sp);
    moq_event_t ev;
    MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
    moq_fetch_t sv_fetch = ev.u.fetch_request.fetch;

    moq_reject_fetch_cfg_t rej;
    moq_reject_fetch_cfg_init(&rej);
    rej.error_code = 0x42;
    rej.can_retry = true;
    rej.retry_after_ms = 5000;
    rej.reason = MOQ_BYTES_LITERAL("not now");
    MOQ_TEST_CHECK(moq_session_reject_fetch(sv, sv_fetch, &rej,
        moq_simpair_now_us(sp)) == MOQ_OK);

    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK(moq_sub_tick(sub, moq_simpair_now_us(sp)) == MOQ_OK);

    moq_sub_fetch_item_t item;
    MOQ_TEST_CHECK(moq_sub_poll_fetch(sub, &item) == MOQ_OK);
    MOQ_TEST_CHECK(item.kind == MOQ_SUB_FETCH_ERROR);
    MOQ_TEST_CHECK(item.request == req);
    MOQ_TEST_CHECK(item.u.error.error_code == 0x42);
    MOQ_TEST_CHECK(item.u.error.can_retry == true);
    MOQ_TEST_CHECK(item.u.error.retry_after_ms == 5000);
    moq_sub_fetch_item_cleanup(&item);

    moq_sub_destroy(sub);
    { moq_event_t d;
      while (moq_session_poll_events(sv, &d, 1) == 1)
          moq_event_cleanup(&d); }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("fetch_error_retry_fields");
}

static void test_fetch_cancel(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    setup_fetch_pair(&as, &alloc, &sp);

    moq_sub_cfg_t cfg; moq_sub_cfg_init(&cfg);
    moq_subscriber_t *sub = NULL;
    MOQ_TEST_CHECK(moq_sub_create(moq_simpair_client(sp), &alloc,
        &cfg, &sub) == MOQ_OK);

    moq_sub_fetch_cfg_t fcfg;
    moq_sub_fetch_cfg_init(&fcfg);
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("test") };
    fcfg.track_namespace.parts = ns;
    fcfg.track_namespace.count = 1;
    fcfg.track_name = MOQ_BYTES_LITERAL("video");
    fcfg.end_group = 1;
    moq_sub_fetch_req_t *req = NULL;
    MOQ_TEST_CHECK(moq_sub_fetch(sub, &fcfg, moq_simpair_now_us(sp),
        &req) == MOQ_OK);

    MOQ_TEST_CHECK(moq_sub_cancel_fetch(sub, req,
        moq_simpair_now_us(sp)) == MOQ_OK);

    moq_simpair_run_until_quiescent(sp, 8, NULL);

    moq_session_t *sv = moq_simpair_server(sp);
    moq_event_t evts[4];
    size_t ne = moq_session_poll_events(sv, evts, 4);
    bool saw_cancel = false;
    for (size_t i = 0; i < ne; i++) {
        if (evts[i].kind == MOQ_EVENT_FETCH_CANCELLED) saw_cancel = true;
        moq_event_cleanup(&evts[i]);
    }
    MOQ_TEST_CHECK(saw_cancel);

    moq_sub_destroy(sub);
    { moq_event_t d;
      while (moq_session_poll_events(sv, &d, 1) == 1)
          moq_event_cleanup(&d); }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("fetch_cancel");
}

static void test_fetch_queue_full_pending_retry(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    setup_fetch_pair(&as, &alloc, &sp);

    moq_sub_cfg_t cfg; moq_sub_cfg_init(&cfg);
    cfg.max_fetch_items = 2;
    moq_subscriber_t *sub = NULL;
    MOQ_TEST_CHECK(moq_sub_create(moq_simpair_client(sp), &alloc,
        &cfg, &sub) == MOQ_OK);

    moq_sub_fetch_cfg_t fcfg;
    moq_sub_fetch_cfg_init(&fcfg);
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("test") };
    fcfg.track_namespace.parts = ns;
    fcfg.track_namespace.count = 1;
    fcfg.track_name = MOQ_BYTES_LITERAL("video");
    fcfg.end_group = 1;
    moq_sub_fetch_req_t *req = NULL;
    MOQ_TEST_CHECK(moq_sub_fetch(sub, &fcfg, moq_simpair_now_us(sp),
        &req) == MOQ_OK);

    moq_simpair_run_until_quiescent(sp, 8, NULL);
    server_accept_and_write(sp, &alloc, (const uint8_t *)"hi", 2);
    moq_simpair_run_until_quiescent(sp, 16, NULL);

    /* Queue cap=2. OK + OBJECT fill it. COMPLETE gets pending. */
    moq_result_t rc = moq_sub_tick(sub, moq_simpair_now_us(sp));
    MOQ_TEST_CHECK(rc == MOQ_ERR_WOULD_BLOCK);

    /* Poll one item to make room. */
    moq_sub_fetch_item_t item;
    MOQ_TEST_CHECK(moq_sub_poll_fetch(sub, &item) == MOQ_OK);
    MOQ_TEST_CHECK(item.kind == MOQ_SUB_FETCH_OK);
    moq_sub_fetch_item_cleanup(&item);

    /* Retry tick — pending should flush. */
    MOQ_TEST_CHECK(moq_sub_tick(sub, moq_simpair_now_us(sp) + 1) == MOQ_OK);

    /* Now drain remaining items. */
    MOQ_TEST_CHECK(moq_sub_poll_fetch(sub, &item) == MOQ_OK);
    MOQ_TEST_CHECK(item.kind == MOQ_SUB_FETCH_OBJECT);
    moq_sub_fetch_item_cleanup(&item);

    MOQ_TEST_CHECK(moq_sub_poll_fetch(sub, &item) == MOQ_OK);
    MOQ_TEST_CHECK(item.kind == MOQ_SUB_FETCH_COMPLETE);
    moq_sub_fetch_item_cleanup(&item);

    moq_sub_destroy(sub);
    { moq_event_t d;
      while (moq_session_poll_events(moq_simpair_server(sp), &d, 1) == 1)
          moq_event_cleanup(&d); }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("fetch_queue_full_pending_retry");
}

/* Accept a fetch and write `count` objects of the given payload, then end. */
static void server_accept_and_write_n(moq_simpair_t *sp, moq_alloc_t *alloc,
    const uint8_t *payload_data, size_t payload_len, int count)
{
    moq_session_t *sv = moq_simpair_server(sp);
    uint64_t now = moq_simpair_now_us(sp);

    moq_event_t ev;
    MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
    MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_FETCH_REQUEST);
    moq_fetch_t sv_fetch = ev.u.fetch_request.fetch;

    moq_accept_fetch_cfg_t acfg;
    moq_accept_fetch_cfg_init(&acfg);
    acfg.end_group = 1;
    MOQ_TEST_CHECK(moq_session_accept_fetch(sv, sv_fetch, &acfg, now) == MOQ_OK);

    for (int i = 0; i < count; i++) {
        moq_rcbuf_t *pay = NULL;
        moq_rcbuf_create(alloc, payload_data, payload_len, &pay);
        moq_fetch_object_cfg_t ocfg;
        moq_fetch_object_cfg_init(&ocfg);
        ocfg.group_id = 0;
        ocfg.object_id = (uint64_t)i;
        ocfg.publisher_priority = 128;
        ocfg.payload = pay;
        MOQ_TEST_CHECK(moq_session_write_fetch_object(sv, sv_fetch, &ocfg,
            now) == MOQ_OK);
        moq_rcbuf_decref(pay);
    }

    MOQ_TEST_CHECK(moq_session_end_fetch(sv, sv_fetch, now) == MOQ_OK);
}

/* The FETCH_OBJECT path also retains payload/properties rcbufs outside session
 * accounting, so it must honor the same facade byte budget. With a tiny byte
 * cap (but default item count cap), the second fetch object diverts to pending
 * (WOULD_BLOCK) and flushes once the first object is polled. */
static void test_fetch_object_byte_budget(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    setup_fetch_pair(&as, &alloc, &sp);

    moq_sub_cfg_t cfg; moq_sub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.max_queued_object_bytes = 8;  /* item count cap stays at default 256 */
    moq_subscriber_t *sub = NULL;
    MOQ_TEST_CHECK(moq_sub_create(moq_simpair_client(sp), &alloc,
        &cfg, &sub) == MOQ_OK);

    moq_sub_fetch_cfg_t fcfg;
    moq_sub_fetch_cfg_init(&fcfg);
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("test") };
    fcfg.track_namespace.parts = ns;
    fcfg.track_namespace.count = 1;
    fcfg.track_name = MOQ_BYTES_LITERAL("video");
    fcfg.end_group = 1;
    moq_sub_fetch_req_t *req = NULL;
    MOQ_TEST_CHECK(moq_sub_fetch(sub, &fcfg, moq_simpair_now_us(sp),
        &req) == MOQ_OK);

    moq_simpair_run_until_quiescent(sp, 8, NULL);
    /* Two 8-byte fetch objects: the first fills the byte budget. */
    server_accept_and_write_n(sp, &alloc, (const uint8_t *)"AAAAAAAA", 8, 2);
    moq_simpair_run_until_quiescent(sp, 16, NULL);

    moq_result_t rc = moq_sub_tick(sub, moq_simpair_now_us(sp));
    MOQ_TEST_CHECK(rc == MOQ_ERR_WOULD_BLOCK);

    moq_sub_fetch_item_t item;
    /* FETCH_OK first (carries no bytes), then the first object. */
    MOQ_TEST_CHECK(moq_sub_poll_fetch(sub, &item) == MOQ_OK);
    MOQ_TEST_CHECK(item.kind == MOQ_SUB_FETCH_OK);
    moq_sub_fetch_item_cleanup(&item);

    MOQ_TEST_CHECK(moq_sub_poll_fetch(sub, &item) == MOQ_OK);
    MOQ_TEST_CHECK(item.kind == MOQ_SUB_FETCH_OBJECT);
    MOQ_TEST_CHECK(item.u.object.object_id == 0);
    MOQ_TEST_CHECK(moq_rcbuf_len(item.u.object.payload) == 8);
    moq_sub_fetch_item_cleanup(&item);

    /* Retry flushes the pending second object and surfaces COMPLETE. */
    MOQ_TEST_CHECK(moq_sub_tick(sub, moq_simpair_now_us(sp) + 1) == MOQ_OK);

    MOQ_TEST_CHECK(moq_sub_poll_fetch(sub, &item) == MOQ_OK);
    MOQ_TEST_CHECK(item.kind == MOQ_SUB_FETCH_OBJECT);
    MOQ_TEST_CHECK(item.u.object.object_id == 1);
    MOQ_TEST_CHECK(moq_rcbuf_len(item.u.object.payload) == 8);
    moq_sub_fetch_item_cleanup(&item);

    MOQ_TEST_CHECK(moq_sub_poll_fetch(sub, &item) == MOQ_OK);
    MOQ_TEST_CHECK(item.kind == MOQ_SUB_FETCH_COMPLETE);
    moq_sub_fetch_item_cleanup(&item);

    moq_sub_destroy(sub);
    { moq_event_t d;
      while (moq_session_poll_events(moq_simpair_server(sp), &d, 1) == 1)
          moq_event_cleanup(&d); }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("fetch_object_byte_budget");
}

static void test_fetch_slot_reuse(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    setup_fetch_pair(&as, &alloc, &sp);

    moq_sub_cfg_t cfg; moq_sub_cfg_init(&cfg);
    cfg.max_fetches = 2;
    moq_subscriber_t *sub = NULL;
    MOQ_TEST_CHECK(moq_sub_create(moq_simpair_client(sp), &alloc,
        &cfg, &sub) == MOQ_OK);

    for (int round = 0; round < 5; round++) {
        moq_sub_fetch_cfg_t fcfg;
        moq_sub_fetch_cfg_init(&fcfg);
        moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("test") };
        fcfg.track_namespace.parts = ns;
        fcfg.track_namespace.count = 1;
        fcfg.track_name = MOQ_BYTES_LITERAL("video");
        fcfg.end_group = 1;
        moq_sub_fetch_req_t *req = NULL;
        MOQ_TEST_CHECK(moq_sub_fetch(sub, &fcfg,
            moq_simpair_now_us(sp), &req) == MOQ_OK);

        moq_simpair_run_until_quiescent(sp, 8, NULL);
        server_accept_and_write(sp, &alloc, (const uint8_t *)"x", 1);
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        MOQ_TEST_CHECK(moq_sub_tick(sub, moq_simpair_now_us(sp)) == MOQ_OK);

        moq_sub_fetch_item_t item;
        while (moq_sub_poll_fetch(sub, &item) == MOQ_OK)
            moq_sub_fetch_item_cleanup(&item);
    }

    moq_sub_destroy(sub);
    { moq_event_t d;
      while (moq_session_poll_events(moq_simpair_server(sp), &d, 1) == 1)
          moq_event_cleanup(&d); }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("fetch_slot_reuse");
}

static void test_fetch_cancel_slot_reuse(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    setup_fetch_pair(&as, &alloc, &sp);

    moq_sub_cfg_t cfg; moq_sub_cfg_init(&cfg);
    cfg.max_fetches = 1;
    moq_subscriber_t *sub = NULL;
    MOQ_TEST_CHECK(moq_sub_create(moq_simpair_client(sp), &alloc,
        &cfg, &sub) == MOQ_OK);

    moq_sub_fetch_cfg_t fcfg;
    moq_sub_fetch_cfg_init(&fcfg);
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("test") };
    fcfg.track_namespace.parts = ns;
    fcfg.track_namespace.count = 1;
    fcfg.track_name = MOQ_BYTES_LITERAL("video");
    fcfg.end_group = 1;

    moq_sub_fetch_req_t *req = NULL;
    MOQ_TEST_CHECK(moq_sub_fetch(sub, &fcfg, moq_simpair_now_us(sp),
        &req) == MOQ_OK);

    MOQ_TEST_CHECK(moq_sub_cancel_fetch(sub, req,
        moq_simpair_now_us(sp)) == MOQ_OK);

    /* Slot should be freed immediately; a new fetch must succeed. */
    moq_sub_fetch_req_t *req2 = NULL;
    MOQ_TEST_CHECK(moq_sub_fetch(sub, &fcfg, moq_simpair_now_us(sp),
        &req2) == MOQ_OK);
    MOQ_TEST_CHECK(req2 != NULL);

    MOQ_TEST_CHECK(moq_sub_cancel_fetch(sub, req2,
        moq_simpair_now_us(sp)) == MOQ_OK);

    moq_simpair_run_until_quiescent(sp, 8, NULL);

    moq_sub_destroy(sub);
    { moq_event_t d;
      while (moq_session_poll_events(moq_simpair_server(sp), &d, 1) == 1)
          moq_event_cleanup(&d); }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("fetch_cancel_slot_reuse");
}

/* Learn the wire request_id of the just-issued FETCH from the client's outbound
 * action and drain client actions, so the simpair server does not auto-respond
 * (these tests inject FETCH responses directly). */
static uint64_t p28_learn_rid(moq_session_t *client)
{
    uint64_t rid = 0; bool have = false;
    moq_action_t acts[16]; size_t n;
    while ((n = moq_session_poll_actions(client, acts, 16)) > 0)
        for (size_t i = 0; i < n; i++) {
            if (!have && acts[i].kind == MOQ_ACTION_SEND_CONTROL) {
                moq_control_envelope_t env; moq_buf_reader_t r;
                moq_buf_reader_init(&r, acts[i].u.send_control.data,
                                    acts[i].u.send_control.len);
                if (moq_control_decode_envelope(&r, &env) >= 0 &&
                    env.msg_type == MOQ_D16_FETCH) {
                    moq_bytes_t nsp[8]; moq_kvp_entry_t p[8];
                    moq_d16_fetch_t f; memset(&f, 0, sizeof(f));
                    f.params = p; f.params_cap = 8;
                    if (moq_d16_decode_fetch(env.payload, env.payload_len,
                                             nsp, 8, &f) >= 0) {
                        rid = f.request_id; have = true;
                    }
                }
            }
            moq_action_cleanup(&acts[i]);
        }
    MOQ_TEST_CHECK(have);
    return rid;
}

/* Inject one fetch object on the object stream (no FIN, so the fetch stays open
 * and cancelable). `with_header` prefixes FETCH_HEADER(rid) at the start. */
static void p28_inject_object(moq_session_t *client, uint64_t rid,
    uint64_t stream_id, bool with_header, uint64_t grp, uint64_t obj,
    const char *pay, uint64_t now)
{
    uint8_t data[256]; moq_buf_writer_t w;
    moq_buf_writer_init(&w, data, sizeof(data));
    if (with_header)
        MOQ_TEST_CHECK(moq_d16_encode_fetch_header(&w, rid) == MOQ_OK);
    moq_d16_fetch_object_t o; memset(&o, 0, sizeof(o));
    o.group_id = grp; o.object_id = obj; o.publisher_priority = 0;
    o.payload = (const uint8_t *)pay; o.payload_len = strlen(pay);
    MOQ_TEST_CHECK(moq_d16_encode_fetch_object(&w, &o, NULL) == MOQ_OK);
    moq_stream_ref_t ref = moq_stream_ref_from_u64(stream_id);
    MOQ_TEST_CHECK(moq_session_on_data_bytes(client, ref, data,
        moq_buf_writer_offset(&w), false, now) >= 0);
}

/* Cancel must drop a back-pressured pending_fi item for the canceled fetch, so a
 * later fetch reusing the slot pointer never inherits it. */
static void test_fetch_cancel_purges_pending_item(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    setup_fetch_pair(&as, &alloc, &sp);
    moq_session_t *client = moq_simpair_client(sp);

    moq_sub_cfg_t cfg; moq_sub_cfg_init(&cfg);
    cfg.max_fetches = 1;
    cfg.max_fetch_items = 1;
    moq_subscriber_t *sub = NULL;
    MOQ_TEST_CHECK(moq_sub_create(client, &alloc, &cfg, &sub) == MOQ_OK);

    moq_sub_fetch_cfg_t fcfg; moq_sub_fetch_cfg_init(&fcfg);
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("test") };
    fcfg.track_namespace.parts = ns; fcfg.track_namespace.count = 1;
    fcfg.track_name = MOQ_BYTES_LITERAL("video");
    fcfg.end_group = 5;

    uint64_t now = moq_simpair_now_us(sp);
    moq_sub_fetch_req_t *req1 = NULL;
    MOQ_TEST_CHECK(moq_sub_fetch(sub, &fcfg, now, &req1) == MOQ_OK);
    uint64_t rid1 = p28_learn_rid(client);

    /* One object fills the single-slot ring; a second back-pressures into
     * pending_fi -- both belong to fetch #1. */
    p28_inject_object(client, rid1, 77, true, 0, 0, "OLD1A", now);
    MOQ_TEST_CHECK(moq_sub_tick(sub, now) == MOQ_OK);
    p28_inject_object(client, rid1, 77, false, 0, 1, "OLD1B", now);
    MOQ_TEST_CHECK(moq_sub_tick(sub, now) == MOQ_ERR_WOULD_BLOCK);

    /* Cancel fetch #1, then reuse the slot for fetch #2. */
    MOQ_TEST_CHECK(moq_sub_cancel_fetch(sub, req1, now) == MOQ_OK);
    moq_sub_fetch_req_t *req2 = NULL;
    MOQ_TEST_CHECK(moq_sub_fetch(sub, &fcfg, now, &req2) == MOQ_OK);
    MOQ_TEST_CHECK(req2 == req1);   /* slot pointer reused */
    (void)p28_learn_rid(client);    /* drain fetch #2's outbound FETCH */

    /* Flush pending + drain: fetch #2 has no injected response, so any item is
     * a stale fetch #1 item misattributed to the reused pointer. */
    MOQ_TEST_CHECK(moq_sub_tick(sub, now) == MOQ_OK);
    moq_sub_fetch_item_t item;
    bool saw_stale = false;
    while (moq_sub_poll_fetch(sub, &item) == MOQ_OK) {
        saw_stale = true;
        moq_sub_fetch_item_cleanup(&item);
    }
    MOQ_TEST_CHECK(!saw_stale);

    moq_sub_destroy(sub);
    { moq_event_t d;
      while (moq_session_poll_events(moq_simpair_server(sp), &d, 1) == 1)
          moq_event_cleanup(&d); }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("fetch_cancel_purges_pending_item");
}

/* Cancel must drop queued payload-carrying items for the canceled fetch (ref
 * release + byte accounting), so a reused slot does not deliver them. */
static void test_fetch_cancel_purges_queued_items(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    setup_fetch_pair(&as, &alloc, &sp);
    moq_session_t *client = moq_simpair_client(sp);

    moq_sub_cfg_t cfg; moq_sub_cfg_init(&cfg);
    cfg.max_fetches = 1;
    cfg.max_fetch_items = 4;
    moq_subscriber_t *sub = NULL;
    MOQ_TEST_CHECK(moq_sub_create(client, &alloc, &cfg, &sub) == MOQ_OK);

    moq_sub_fetch_cfg_t fcfg; moq_sub_fetch_cfg_init(&fcfg);
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("test") };
    fcfg.track_namespace.parts = ns; fcfg.track_namespace.count = 1;
    fcfg.track_name = MOQ_BYTES_LITERAL("video");
    fcfg.end_group = 5;

    uint64_t now = moq_simpair_now_us(sp);
    moq_sub_fetch_req_t *req1 = NULL;
    MOQ_TEST_CHECK(moq_sub_fetch(sub, &fcfg, now, &req1) == MOQ_OK);
    uint64_t rid1 = p28_learn_rid(client);

    /* Queue two payload-carrying objects for fetch #1 (no pending). */
    p28_inject_object(client, rid1, 77, true, 0, 0, "OLD2A", now);
    p28_inject_object(client, rid1, 77, false, 0, 1, "OLD2B", now);
    MOQ_TEST_CHECK(moq_sub_tick(sub, now) == MOQ_OK);

    /* Cancel before polling, then reuse the slot. */
    MOQ_TEST_CHECK(moq_sub_cancel_fetch(sub, req1, now) == MOQ_OK);
    moq_sub_fetch_req_t *req2 = NULL;
    MOQ_TEST_CHECK(moq_sub_fetch(sub, &fcfg, now, &req2) == MOQ_OK);
    MOQ_TEST_CHECK(req2 == req1);
    (void)p28_learn_rid(client);

    MOQ_TEST_CHECK(moq_sub_tick(sub, now) == MOQ_OK);
    moq_sub_fetch_item_t item;
    bool saw_stale = false;
    while (moq_sub_poll_fetch(sub, &item) == MOQ_OK) {
        saw_stale = true;
        moq_sub_fetch_item_cleanup(&item);
    }
    MOQ_TEST_CHECK(!saw_stale);

    moq_sub_destroy(sub);
    { moq_event_t d;
      while (moq_session_poll_events(moq_simpair_server(sp), &d, 1) == 1)
          moq_event_cleanup(&d); }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("fetch_cancel_purges_queued_items");
}

/* Positive control: cancel purges only the canceled request's items, leaving a
 * concurrent fetch's queued items intact and in order. */
static void test_fetch_cancel_keeps_other_fetch_items(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    setup_fetch_pair(&as, &alloc, &sp);
    moq_session_t *client = moq_simpair_client(sp);

    moq_sub_cfg_t cfg; moq_sub_cfg_init(&cfg);
    cfg.max_fetches = 2;
    cfg.max_fetch_items = 8;
    moq_subscriber_t *sub = NULL;
    MOQ_TEST_CHECK(moq_sub_create(client, &alloc, &cfg, &sub) == MOQ_OK);

    moq_sub_fetch_cfg_t fcfg; moq_sub_fetch_cfg_init(&fcfg);
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("test") };
    fcfg.track_namespace.parts = ns; fcfg.track_namespace.count = 1;
    fcfg.track_name = MOQ_BYTES_LITERAL("video");
    fcfg.end_group = 5;

    uint64_t now = moq_simpair_now_us(sp);
    moq_sub_fetch_req_t *req1 = NULL;
    MOQ_TEST_CHECK(moq_sub_fetch(sub, &fcfg, now, &req1) == MOQ_OK);
    uint64_t rid1 = p28_learn_rid(client);
    moq_sub_fetch_req_t *req2 = NULL;
    MOQ_TEST_CHECK(moq_sub_fetch(sub, &fcfg, now, &req2) == MOQ_OK);
    uint64_t rid2 = p28_learn_rid(client);
    MOQ_TEST_CHECK(req1 != req2);

    /* Queue one object for each fetch on its own stream. */
    p28_inject_object(client, rid1, 71, true, 0, 0, "AAA", now);
    p28_inject_object(client, rid2, 72, true, 0, 0, "BBB", now);
    MOQ_TEST_CHECK(moq_sub_tick(sub, now) == MOQ_OK);

    /* Cancel only fetch #1. */
    MOQ_TEST_CHECK(moq_sub_cancel_fetch(sub, req1, now) == MOQ_OK);

    /* Only fetch #2's object survives, attributed to req2. */
    moq_sub_fetch_item_t item;
    int n_items = 0; bool saw_req2_obj = false, saw_req1 = false;
    while (moq_sub_poll_fetch(sub, &item) == MOQ_OK) {
        n_items++;
        if (item.request == req1) saw_req1 = true;
        if (item.request == req2 && item.kind == MOQ_SUB_FETCH_OBJECT &&
            item.u.object.payload &&
            moq_rcbuf_len(item.u.object.payload) == 3 &&
            memcmp(moq_rcbuf_data(item.u.object.payload), "BBB", 3) == 0)
            saw_req2_obj = true;
        moq_sub_fetch_item_cleanup(&item);
    }
    MOQ_TEST_CHECK(!saw_req1);
    MOQ_TEST_CHECK(saw_req2_obj);
    MOQ_TEST_CHECK(n_items == 1);

    moq_sub_destroy(sub);
    { moq_event_t d;
      while (moq_session_poll_events(moq_simpair_server(sp), &d, 1) == 1)
          moq_event_cleanup(&d); }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("fetch_cancel_keeps_other_fetch_items");
}

static void test_fetch_cancel_after_active(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    setup_fetch_pair(&as, &alloc, &sp);

    moq_sub_cfg_t cfg; moq_sub_cfg_init(&cfg);
    moq_subscriber_t *sub = NULL;
    MOQ_TEST_CHECK(moq_sub_create(moq_simpair_client(sp), &alloc,
        &cfg, &sub) == MOQ_OK);

    moq_sub_fetch_cfg_t fcfg;
    moq_sub_fetch_cfg_init(&fcfg);
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("test") };
    fcfg.track_namespace.parts = ns;
    fcfg.track_namespace.count = 1;
    fcfg.track_name = MOQ_BYTES_LITERAL("video");
    fcfg.end_group = 3;
    moq_sub_fetch_req_t *req = NULL;
    MOQ_TEST_CHECK(moq_sub_fetch(sub, &fcfg, moq_simpair_now_us(sp),
        &req) == MOQ_OK);

    moq_simpair_run_until_quiescent(sp, 8, NULL);

    /* Server accepts the fetch (non-empty). */
    moq_session_t *sv = moq_simpair_server(sp);
    moq_event_t ev;
    MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
    MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_FETCH_REQUEST);
    moq_fetch_t sv_fetch = ev.u.fetch_request.fetch;

    moq_accept_fetch_cfg_t acfg;
    moq_accept_fetch_cfg_init(&acfg);
    acfg.end_group = 3;
    MOQ_TEST_CHECK(moq_session_accept_fetch(sv, sv_fetch, &acfg,
        moq_simpair_now_us(sp)) == MOQ_OK);

    moq_simpair_run_until_quiescent(sp, 16, NULL);
    MOQ_TEST_CHECK(moq_sub_tick(sub, moq_simpair_now_us(sp)) == MOQ_OK);

    /* Drain FETCH_OK to confirm fetch is active. */
    moq_sub_fetch_item_t item;
    MOQ_TEST_CHECK(moq_sub_poll_fetch(sub, &item) == MOQ_OK);
    MOQ_TEST_CHECK(item.kind == MOQ_SUB_FETCH_OK);
    moq_sub_fetch_item_cleanup(&item);

    /* Cancel while active. */
    MOQ_TEST_CHECK(moq_sub_cancel_fetch(sub, req,
        moq_simpair_now_us(sp)) == MOQ_OK);

    moq_simpair_run_until_quiescent(sp, 8, NULL);

    /* Server should receive FETCH_CANCELLED. */
    moq_event_t sevts[4];
    size_t ne = moq_session_poll_events(sv, sevts, 4);
    bool saw_cancel = false;
    for (size_t i = 0; i < ne; i++) {
        if (sevts[i].kind == MOQ_EVENT_FETCH_CANCELLED) saw_cancel = true;
        moq_event_cleanup(&sevts[i]);
    }
    MOQ_TEST_CHECK(saw_cancel);

    moq_sub_destroy(sub);
    { moq_event_t d;
      while (moq_session_poll_events(sv, &d, 1) == 1)
          moq_event_cleanup(&d); }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("fetch_cancel_after_active");
}

/* -- Streaming helpers ---------------------------------------------- */

static void setup_streaming_pair(test_alloc_state_t *as, moq_alloc_t *alloc,
    moq_simpair_t **sp, moq_publisher_t **pub, moq_pub_track_t **track)
{
    *as = (test_alloc_state_t){0};
    *alloc = test_allocator(as);
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = alloc; cfg.seed = 42; cfg.initial_now_us = 1000;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 16;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 16;
    cfg.client_streaming_objects = true;
    moq_simpair_create(&cfg, sp);
    moq_simpair_start(*sp);
    moq_simpair_run_until_quiescent(*sp, 8, NULL);
    moq_event_t ev;
    moq_session_poll_events(moq_simpair_client(*sp), &ev, 1);
    moq_session_poll_events(moq_simpair_server(*sp), &ev, 1);

    moq_pub_cfg_t pcfg; moq_pub_cfg_init_sized(&pcfg, sizeof(pcfg));
    pcfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    moq_pub_create(moq_simpair_server(*sp), alloc, &pcfg, pub);
    moq_pub_track_cfg_t tcfg; moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("test") };
    tcfg.track_namespace.parts = ns;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_pub_add_track(*pub, &tcfg, moq_simpair_now_us(*sp), track);
}

/* -- Streaming tests ------------------------------------------------ */

static void test_streaming_chunk_received(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *ptrack;
    setup_streaming_pair(&as, &alloc, &sp, &pub, &ptrack);

    moq_sub_cfg_t cfg; moq_sub_cfg_init(&cfg);
    cfg.streaming_objects = true;
    moq_subscriber_t *sub = NULL;
    MOQ_TEST_CHECK(moq_sub_create(moq_simpair_client(sp), &alloc,
        &cfg, &sub) == MOQ_OK);

    moq_sub_track_cfg_t tcfg; moq_sub_track_cfg_init(&tcfg);
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("test") };
    tcfg.track_namespace.parts = ns;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_sub_track_t *track = NULL;
    moq_sub_subscribe(sub, &tcfg, moq_simpair_now_us(sp), &track);

    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_pub_tick(pub, moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_sub_tick(sub, moq_simpair_now_us(sp));

    moq_rcbuf_t *pay = NULL;
    moq_rcbuf_create(&alloc, (const uint8_t *)"hello", 5, &pay);
    moq_pub_write_object(pub, ptrack, 0, 0, pay, moq_simpair_now_us(sp));
    moq_rcbuf_decref(pay);
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    MOQ_TEST_CHECK(moq_sub_tick(sub, moq_simpair_now_us(sp)) == MOQ_OK);

    moq_sub_chunk_t ck;
    MOQ_TEST_CHECK(moq_sub_poll_chunk(sub, &ck) == MOQ_OK);
    MOQ_TEST_CHECK(ck.track == track);
    MOQ_TEST_CHECK(ck.group_id == 0);
    MOQ_TEST_CHECK(ck.object_id == 0);
    MOQ_TEST_CHECK(ck.begin == true);
    MOQ_TEST_CHECK(ck.payload_length == 5);
    moq_sub_chunk_cleanup(&ck);

    MOQ_TEST_CHECK(moq_sub_poll_chunk(sub, &ck) == MOQ_OK);
    MOQ_TEST_CHECK(ck.end == true);
    MOQ_TEST_CHECK(ck.chunk != NULL);
    MOQ_TEST_CHECK(moq_rcbuf_len(ck.chunk) == 5);
    MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(ck.chunk), "hello", 5) == 0);
    moq_sub_chunk_cleanup(&ck);

    MOQ_TEST_CHECK(moq_sub_poll_chunk(sub, &ck) == MOQ_DONE);

    moq_sub_destroy(sub);
    moq_pub_destroy(pub);
    { moq_event_t d;
      while (moq_session_poll_events(moq_simpair_server(sp), &d, 1) == 1)
          moq_event_cleanup(&d); }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("streaming_chunk_received");
}

static void test_streaming_chunk_queue_full_pending_retry(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *ptrack;
    setup_streaming_pair(&as, &alloc, &sp, &pub, &ptrack);

    moq_sub_cfg_t cfg; moq_sub_cfg_init(&cfg);
    cfg.streaming_objects = true;
    cfg.max_chunks = 1;
    moq_subscriber_t *sub = NULL;
    MOQ_TEST_CHECK(moq_sub_create(moq_simpair_client(sp), &alloc,
        &cfg, &sub) == MOQ_OK);

    moq_sub_track_cfg_t tcfg; moq_sub_track_cfg_init(&tcfg);
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("test") };
    tcfg.track_namespace.parts = ns;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_sub_track_t *track = NULL;
    moq_sub_subscribe(sub, &tcfg, moq_simpair_now_us(sp), &track);
    (void)track;

    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_pub_tick(pub, moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_sub_tick(sub, moq_simpair_now_us(sp));

    moq_rcbuf_t *pay = NULL;
    moq_rcbuf_create(&alloc, (const uint8_t *)"hi", 2, &pay);
    moq_pub_write_object(pub, ptrack, 0, 0, pay, moq_simpair_now_us(sp));
    moq_rcbuf_decref(pay);
    moq_simpair_run_until_quiescent(sp, 16, NULL);

    /* max_chunks=1: first chunk fills queue, second gets pending. */
    moq_result_t rc = moq_sub_tick(sub, moq_simpair_now_us(sp));
    MOQ_TEST_CHECK(rc == MOQ_ERR_WOULD_BLOCK);

    moq_sub_chunk_t ck;
    MOQ_TEST_CHECK(moq_sub_poll_chunk(sub, &ck) == MOQ_OK);
    MOQ_TEST_CHECK(ck.begin == true);
    moq_sub_chunk_cleanup(&ck);

    MOQ_TEST_CHECK(moq_sub_tick(sub, moq_simpair_now_us(sp) + 1) == MOQ_OK);

    MOQ_TEST_CHECK(moq_sub_poll_chunk(sub, &ck) == MOQ_OK);
    MOQ_TEST_CHECK(ck.end == true);
    MOQ_TEST_CHECK(ck.chunk != NULL);
    moq_sub_chunk_cleanup(&ck);

    moq_sub_destroy(sub);
    moq_pub_destroy(pub);
    { moq_event_t d;
      while (moq_session_poll_events(moq_simpair_server(sp), &d, 1) == 1)
          moq_event_cleanup(&d); }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("streaming_chunk_queue_full_pending_retry");
}

/* -- Joining fetch tests -------------------------------------------- */

static void test_joining_fetch_relative_success(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    setup_fetch_pair(&as, &alloc, &sp);

    moq_sub_cfg_t cfg; moq_sub_cfg_init(&cfg);
    moq_subscriber_t *sub = NULL;
    MOQ_TEST_CHECK(moq_sub_create(moq_simpair_client(sp), &alloc,
        &cfg, &sub) == MOQ_OK);

    moq_sub_track_cfg_t tcfg; moq_sub_track_cfg_init(&tcfg);
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("test") };
    tcfg.track_namespace.parts = ns;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    tcfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    moq_sub_track_t *track = NULL;
    MOQ_TEST_CHECK(moq_sub_subscribe(sub, &tcfg,
        moq_simpair_now_us(sp), &track) == MOQ_OK);

    moq_simpair_run_until_quiescent(sp, 8, NULL);

    moq_session_t *sv = moq_simpair_server(sp);
    moq_event_t ev;
    MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
    MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
    moq_subscription_t ssub = ev.u.subscribe_request.sub;
    moq_event_cleanup(&ev);

    moq_accept_subscribe_cfg_t acc;
    moq_accept_subscribe_cfg_init(&acc);
    acc.has_largest = true;
    acc.largest_group = 10;
    acc.largest_object = 5;
    MOQ_TEST_CHECK(moq_session_accept_subscribe(sv, ssub, &acc,
        moq_simpair_now_us(sp)) == MOQ_OK);

    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK(moq_sub_tick(sub, moq_simpair_now_us(sp)) == MOQ_OK);

    moq_sub_joining_fetch_cfg_t jcfg;
    moq_sub_joining_fetch_cfg_init(&jcfg);
    jcfg.track = track;
    jcfg.relative = true;
    jcfg.joining_start = 3;
    moq_sub_fetch_req_t *freq = NULL;
    MOQ_TEST_CHECK(moq_sub_joining_fetch(sub, &jcfg,
        moq_simpair_now_us(sp), &freq) == MOQ_OK);
    MOQ_TEST_CHECK(freq != NULL);

    moq_simpair_run_until_quiescent(sp, 8, NULL);

    MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
    MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_FETCH_REQUEST);
    MOQ_TEST_CHECK(ev.u.fetch_request.start_group == 7);
    MOQ_TEST_CHECK(ev.u.fetch_request.start_object == 0);
    MOQ_TEST_CHECK(ev.u.fetch_request.end_group == 10);
    MOQ_TEST_CHECK(ev.u.fetch_request.end_object == 6);
    moq_fetch_t svfh = ev.u.fetch_request.fetch;
    moq_event_cleanup(&ev);

    moq_accept_fetch_cfg_t afcfg;
    moq_accept_fetch_cfg_init(&afcfg);
    afcfg.end_group = 10;
    afcfg.end_object = 6;
    afcfg.empty = true;
    MOQ_TEST_CHECK(moq_session_accept_fetch(sv, svfh, &afcfg,
        moq_simpair_now_us(sp)) == MOQ_OK);

    moq_simpair_run_until_quiescent(sp, 16, NULL);
    MOQ_TEST_CHECK(moq_sub_tick(sub, moq_simpair_now_us(sp)) == MOQ_OK);

    moq_sub_fetch_item_t item;
    MOQ_TEST_CHECK(moq_sub_poll_fetch(sub, &item) == MOQ_OK);
    MOQ_TEST_CHECK(item.kind == MOQ_SUB_FETCH_OK);
    MOQ_TEST_CHECK(item.request == freq);
    moq_sub_fetch_item_cleanup(&item);

    MOQ_TEST_CHECK(moq_sub_poll_fetch(sub, &item) == MOQ_OK);
    MOQ_TEST_CHECK(item.kind == MOQ_SUB_FETCH_COMPLETE);
    moq_sub_fetch_item_cleanup(&item);

    moq_sub_destroy(sub);
    { moq_event_t d;
      while (moq_session_poll_events(sv, &d, 1) == 1)
          moq_event_cleanup(&d); }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("joining_fetch_relative_success");
}

static void test_joining_fetch_absolute_success(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    setup_fetch_pair(&as, &alloc, &sp);

    moq_sub_cfg_t cfg; moq_sub_cfg_init(&cfg);
    moq_subscriber_t *sub = NULL;
    MOQ_TEST_CHECK(moq_sub_create(moq_simpair_client(sp), &alloc,
        &cfg, &sub) == MOQ_OK);

    moq_sub_track_cfg_t tcfg; moq_sub_track_cfg_init(&tcfg);
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("test") };
    tcfg.track_namespace.parts = ns;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    tcfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    moq_sub_track_t *track = NULL;
    moq_sub_subscribe(sub, &tcfg, moq_simpair_now_us(sp), &track);

    moq_simpair_run_until_quiescent(sp, 8, NULL);

    moq_session_t *sv = moq_simpair_server(sp);
    moq_event_t ev;
    moq_session_poll_events(sv, &ev, 1);
    moq_subscription_t ssub = ev.u.subscribe_request.sub;
    moq_event_cleanup(&ev);

    moq_accept_subscribe_cfg_t acc;
    moq_accept_subscribe_cfg_init(&acc);
    acc.has_largest = true;
    acc.largest_group = 20;
    acc.largest_object = 3;
    moq_session_accept_subscribe(sv, ssub, &acc, moq_simpair_now_us(sp));

    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_sub_tick(sub, moq_simpair_now_us(sp));

    moq_sub_joining_fetch_cfg_t jcfg;
    moq_sub_joining_fetch_cfg_init(&jcfg);
    jcfg.track = track;
    jcfg.relative = false;
    jcfg.joining_start = 5;
    moq_sub_fetch_req_t *freq = NULL;
    MOQ_TEST_CHECK(moq_sub_joining_fetch(sub, &jcfg,
        moq_simpair_now_us(sp), &freq) == MOQ_OK);

    moq_simpair_run_until_quiescent(sp, 8, NULL);

    MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
    MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_FETCH_REQUEST);
    MOQ_TEST_CHECK(ev.u.fetch_request.start_group == 5);
    MOQ_TEST_CHECK(ev.u.fetch_request.start_object == 0);
    MOQ_TEST_CHECK(ev.u.fetch_request.end_group == 20);
    MOQ_TEST_CHECK(ev.u.fetch_request.end_object == 4);
    moq_event_cleanup(&ev);

    moq_sub_cancel_fetch(sub, freq, moq_simpair_now_us(sp));

    moq_sub_destroy(sub);
    { moq_event_t d;
      while (moq_session_poll_events(sv, &d, 1) == 1)
          moq_event_cleanup(&d); }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("joining_fetch_absolute_success");
}

static void test_joining_fetch_invalid_track(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    setup_fetch_pair(&as, &alloc, &sp);

    moq_sub_cfg_t cfg; moq_sub_cfg_init(&cfg);
    moq_subscriber_t *sub = NULL;
    MOQ_TEST_CHECK(moq_sub_create(moq_simpair_client(sp), &alloc,
        &cfg, &sub) == MOQ_OK);

    moq_sub_joining_fetch_cfg_t jcfg;
    moq_sub_joining_fetch_cfg_init(&jcfg);
    moq_sub_fetch_req_t *freq = NULL;

    /* NULL track. */
    jcfg.track = NULL;
    MOQ_TEST_CHECK(moq_sub_joining_fetch(sub, &jcfg,
        moq_simpair_now_us(sp), &freq) == MOQ_ERR_INVAL);

    /* Subscribe but don't accept — track is PENDING. */
    moq_sub_track_cfg_t tcfg; moq_sub_track_cfg_init(&tcfg);
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("test") };
    tcfg.track_namespace.parts = ns;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_sub_track_t *track = NULL;
    moq_sub_subscribe(sub, &tcfg, moq_simpair_now_us(sp), &track);

    jcfg.track = track;
    MOQ_TEST_CHECK(moq_sub_joining_fetch(sub, &jcfg,
        moq_simpair_now_us(sp), &freq) == MOQ_ERR_WRONG_STATE);

    moq_sub_destroy(sub);
    { moq_event_t d;
      while (moq_session_poll_events(moq_simpair_server(sp), &d, 1) == 1)
          moq_event_cleanup(&d); }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("joining_fetch_invalid_track");
}

static void test_joining_fetch_slot_reuse(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    setup_fetch_pair(&as, &alloc, &sp);

    moq_sub_cfg_t cfg; moq_sub_cfg_init(&cfg);
    cfg.max_fetches = 1;
    moq_subscriber_t *sub = NULL;
    MOQ_TEST_CHECK(moq_sub_create(moq_simpair_client(sp), &alloc,
        &cfg, &sub) == MOQ_OK);

    moq_sub_track_cfg_t tcfg; moq_sub_track_cfg_init(&tcfg);
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("test") };
    tcfg.track_namespace.parts = ns;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    tcfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    moq_sub_track_t *track = NULL;
    moq_sub_subscribe(sub, &tcfg, moq_simpair_now_us(sp), &track);

    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_session_t *sv = moq_simpair_server(sp);
    moq_event_t ev;
    moq_session_poll_events(sv, &ev, 1);
    moq_subscription_t ssub = ev.u.subscribe_request.sub;
    moq_event_cleanup(&ev);

    moq_accept_subscribe_cfg_t acc;
    moq_accept_subscribe_cfg_init(&acc);
    acc.has_largest = true;
    acc.largest_group = 10;
    acc.largest_object = 5;
    moq_session_accept_subscribe(sv, ssub, &acc, moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_sub_tick(sub, moq_simpair_now_us(sp));

    for (int round = 0; round < 3; round++) {
        moq_sub_joining_fetch_cfg_t jcfg;
        moq_sub_joining_fetch_cfg_init(&jcfg);
        jcfg.track = track;
        jcfg.relative = true;
        jcfg.joining_start = 1;
        moq_sub_fetch_req_t *freq = NULL;
        MOQ_TEST_CHECK(moq_sub_joining_fetch(sub, &jcfg,
            moq_simpair_now_us(sp), &freq) == MOQ_OK);

        moq_simpair_run_until_quiescent(sp, 8, NULL);
        moq_session_poll_events(sv, &ev, 1);
        moq_fetch_t svfh = ev.u.fetch_request.fetch;
        moq_event_cleanup(&ev);

        moq_accept_fetch_cfg_t afcfg;
        moq_accept_fetch_cfg_init(&afcfg);
        afcfg.end_group = 10;
        afcfg.empty = true;
        moq_session_accept_fetch(sv, svfh, &afcfg, moq_simpair_now_us(sp));
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        moq_sub_tick(sub, moq_simpair_now_us(sp));

        moq_sub_fetch_item_t item;
        while (moq_sub_poll_fetch(sub, &item) == MOQ_OK)
            moq_sub_fetch_item_cleanup(&item);
    }

    moq_sub_destroy(sub);
    { moq_event_t d;
      while (moq_session_poll_events(sv, &d, 1) == 1)
          moq_event_cleanup(&d); }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("joining_fetch_slot_reuse");
}

/* -- Subscription update tests -------------------------------------- */

static void setup_active_track(test_alloc_state_t *as, moq_alloc_t *alloc,
    moq_simpair_t **sp, moq_publisher_t **pub, moq_pub_track_t **ptrack,
    moq_subscriber_t **sub_out, moq_sub_track_t **track_out,
    sub_cb_state_t *cbs)
{
    setup_pair(as, alloc, sp, pub, ptrack);
    moq_sub_cfg_t cfg; moq_sub_cfg_init(&cfg);
    cfg.callbacks.ctx = cbs;
    cfg.callbacks.on_subscribed = on_subscribed;
    cfg.callbacks.on_subscribe_error = on_error;
    cfg.callbacks.on_closed = on_closed;
    MOQ_TEST_CHECK_EQ_INT(moq_sub_create(moq_simpair_client(*sp), alloc,
        &cfg, sub_out), MOQ_OK);

    moq_sub_track_cfg_t tcfg; moq_sub_track_cfg_init(&tcfg);
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("test") };
    tcfg.track_namespace.parts = ns;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    MOQ_TEST_CHECK_EQ_INT(moq_sub_subscribe(*sub_out, &tcfg,
        moq_simpair_now_us(*sp), track_out), MOQ_OK);
    moq_simpair_run_until_quiescent(*sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_INT(moq_pub_tick(*pub, moq_simpair_now_us(*sp)),
        MOQ_OK);
    moq_simpair_run_until_quiescent(*sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_INT(moq_sub_tick(*sub_out, moq_simpair_now_us(*sp)),
        MOQ_OK);
}

static void teardown_update_test(moq_subscriber_t *sub, moq_publisher_t *pub,
    moq_simpair_t *sp)
{
    moq_sub_destroy(sub);
    moq_pub_destroy(pub);
    { moq_event_t d;
      while (moq_session_poll_events(moq_simpair_server(sp), &d, 1) == 1)
          moq_event_cleanup(&d);
      while (moq_session_poll_events(moq_simpair_client(sp), &d, 1) == 1)
          moq_event_cleanup(&d); }
    moq_simpair_destroy(sp);
}

static void test_update_happy_path(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *ptrack;
    moq_subscriber_t *sub = NULL; moq_sub_track_t *track = NULL;
    sub_cb_state_t cbs = {0};
    setup_active_track(&as, &alloc, &sp, &pub, &ptrack, &sub, &track, &cbs);
    MOQ_TEST_CHECK(moq_sub_track_is_active(track));

    moq_sub_update_cfg_t ucfg; moq_sub_update_cfg_init(&ucfg);
    ucfg.has_subscriber_priority = true;
    ucfg.subscriber_priority = 200;
    MOQ_TEST_CHECK_EQ_INT(moq_sub_update_subscription(sub, track, &ucfg,
        moq_simpair_now_us(sp)), MOQ_OK);

    moq_simpair_run_until_quiescent(sp, 8, NULL);

    moq_event_t ev;
    MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(
        moq_simpair_server(sp), &ev, 1), 1);
    MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_UPDATED);
    MOQ_TEST_CHECK(ev.u.subscribe_updated.has_subscriber_priority);
    MOQ_TEST_CHECK_EQ_U64(ev.u.subscribe_updated.subscriber_priority, 200);
    MOQ_TEST_CHECK(!ev.u.subscribe_updated.has_forward);
    MOQ_TEST_CHECK(!ev.u.subscribe_updated.has_delivery_timeout);
    moq_event_cleanup(&ev);

    MOQ_TEST_CHECK(moq_session_state(moq_simpair_client(sp))
        == MOQ_SESS_ESTABLISHED);

    teardown_update_test(sub, pub, sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("update_happy_path");
}

static void test_update_all_fields(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *ptrack;
    moq_subscriber_t *sub = NULL; moq_sub_track_t *track = NULL;
    sub_cb_state_t cbs = {0};
    setup_active_track(&as, &alloc, &sp, &pub, &ptrack, &sub, &track, &cbs);

    moq_sub_update_cfg_t ucfg; moq_sub_update_cfg_init(&ucfg);
    ucfg.has_subscriber_priority = true;
    ucfg.subscriber_priority = 42;
    ucfg.has_forward = true;
    ucfg.forward = false;
    ucfg.has_delivery_timeout = true;
    ucfg.delivery_timeout_us = 5000000;
    MOQ_TEST_CHECK_EQ_INT(moq_sub_update_subscription(sub, track, &ucfg,
        moq_simpair_now_us(sp)), MOQ_OK);

    moq_simpair_run_until_quiescent(sp, 8, NULL);

    moq_event_t ev;
    MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(
        moq_simpair_server(sp), &ev, 1), 1);
    MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_UPDATED);
    MOQ_TEST_CHECK(ev.u.subscribe_updated.has_subscriber_priority);
    MOQ_TEST_CHECK_EQ_U64(ev.u.subscribe_updated.subscriber_priority, 42);
    MOQ_TEST_CHECK(ev.u.subscribe_updated.has_forward);
    MOQ_TEST_CHECK(!ev.u.subscribe_updated.forward);
    MOQ_TEST_CHECK(ev.u.subscribe_updated.has_delivery_timeout);
    MOQ_TEST_CHECK_EQ_U64(ev.u.subscribe_updated.delivery_timeout_us,
        5000000);
    moq_event_cleanup(&ev);

    teardown_update_test(sub, pub, sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("update_all_fields");
}

static void test_update_pending_wrong_state(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *ptrack;
    moq_subscriber_t *sub = NULL; moq_sub_track_t *track = NULL;
    sub_cb_state_t cbs = {0};
    setup_active_track(&as, &alloc, &sp, &pub, &ptrack, &sub, &track, &cbs);

    moq_sub_update_cfg_t ucfg; moq_sub_update_cfg_init(&ucfg);
    ucfg.has_subscriber_priority = true;
    ucfg.subscriber_priority = 50;
    MOQ_TEST_CHECK_EQ_INT(moq_sub_update_subscription(sub, track, &ucfg,
        moq_simpair_now_us(sp)), MOQ_OK);

    /* Second update before peer responds must fail. Do NOT call
     * run_until_quiescent — that would deliver the auto-ack and
     * clear the pending flag. */
    ucfg.subscriber_priority = 100;
    MOQ_TEST_CHECK_EQ_INT(moq_sub_update_subscription(sub, track, &ucfg,
        moq_simpair_now_us(sp)), MOQ_ERR_WRONG_STATE);

    /* Deliver the auto-ack and clear pending. */
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t d;
      while (moq_session_poll_events(moq_simpair_server(sp), &d, 1) == 1)
          moq_event_cleanup(&d); }

    /* Now another update succeeds. */
    ucfg.subscriber_priority = 100;
    MOQ_TEST_CHECK_EQ_INT(moq_sub_update_subscription(sub, track, &ucfg,
        moq_simpair_now_us(sp)), MOQ_OK);

    teardown_update_test(sub, pub, sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("update_pending_wrong_state");
}

static void test_update_error_clears_pending(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *ptrack;
    moq_subscriber_t *sub = NULL; moq_sub_track_t *track = NULL;
    sub_cb_state_t cbs = {0};
    setup_active_track(&as, &alloc, &sp, &pub, &ptrack, &sub, &track, &cbs);

    moq_sub_update_cfg_t ucfg; moq_sub_update_cfg_init(&ucfg);
    ucfg.has_subscriber_priority = true;
    ucfg.subscriber_priority = 50;
    MOQ_TEST_CHECK_EQ_INT(moq_sub_update_subscription(sub, track, &ucfg,
        moq_simpair_now_us(sp)), MOQ_OK);

    /* Drain the client action (REQUEST_UPDATE) without delivering
     * to the server, then inject a raw REQUEST_ERROR for update id=2. */
    { moq_action_t da[8]; size_t dn;
      while ((dn = moq_session_poll_actions(moq_simpair_client(sp),
              da, 8)) > 0)
          for (size_t i = 0; i < dn; i++) moq_action_cleanup(&da[i]); }

    uint8_t err_buf[64];
    moq_buf_writer_t ew;
    moq_buf_writer_init(&ew, err_buf, sizeof(err_buf));
    MOQ_TEST_CHECK_EQ_INT(moq_d16_encode_request_error(&ew, 2, 0x10,
        0, NULL, 0), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(moq_session_on_control_bytes(
        moq_simpair_client(sp), err_buf,
        moq_buf_writer_offset(&ew), moq_simpair_now_us(sp)), MOQ_OK);
    MOQ_TEST_CHECK(moq_session_state(moq_simpair_client(sp))
        == MOQ_SESS_ESTABLISHED);

    /* After error clears pending, another update succeeds. */
    ucfg.subscriber_priority = 100;
    MOQ_TEST_CHECK_EQ_INT(moq_sub_update_subscription(sub, track, &ucfg,
        moq_simpair_now_us(sp)), MOQ_OK);

    teardown_update_test(sub, pub, sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("update_error_clears_pending");
}

static void test_update_invalid_states(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *ptrack;
    moq_subscriber_t *sub = NULL; moq_sub_track_t *track = NULL;
    sub_cb_state_t cbs = {0};
    setup_active_track(&as, &alloc, &sp, &pub, &ptrack, &sub, &track, &cbs);

    moq_sub_update_cfg_t ucfg; moq_sub_update_cfg_init(&ucfg);

    /* NULL sub */
    ucfg.has_subscriber_priority = true;
    ucfg.subscriber_priority = 50;
    MOQ_TEST_CHECK_EQ_INT(moq_sub_update_subscription(NULL, track, &ucfg,
        0), MOQ_ERR_INVAL);

    /* NULL track */
    MOQ_TEST_CHECK_EQ_INT(moq_sub_update_subscription(sub, NULL, &ucfg,
        0), MOQ_ERR_INVAL);

    /* NULL cfg */
    MOQ_TEST_CHECK_EQ_INT(moq_sub_update_subscription(sub, track, NULL,
        0), MOQ_ERR_INVAL);

    /* Empty cfg (no fields set) */
    moq_sub_update_cfg_t empty; moq_sub_update_cfg_init(&empty);
    MOQ_TEST_CHECK_EQ_INT(moq_sub_update_subscription(sub, track, &empty,
        moq_simpair_now_us(sp)), MOQ_ERR_INVAL);

    /* delivery_timeout_us < 1000 */
    moq_sub_update_cfg_t bad_dt; moq_sub_update_cfg_init(&bad_dt);
    bad_dt.has_delivery_timeout = true;
    bad_dt.delivery_timeout_us = 500;
    MOQ_TEST_CHECK_EQ_INT(moq_sub_update_subscription(sub, track, &bad_dt,
        moq_simpair_now_us(sp)), MOQ_ERR_INVAL);

    /* Pending track: use the same subscriber, subscribe a second track
     * and leave it pending (don't tick). */
    {
        moq_sub_track_cfg_t tcfg2; moq_sub_track_cfg_init(&tcfg2);
        moq_bytes_t ns2[] = { MOQ_BYTES_LITERAL("test") };
        tcfg2.track_namespace.parts = ns2;
        tcfg2.track_namespace.count = 1;
        tcfg2.track_name = MOQ_BYTES_LITERAL("audio");
        moq_sub_track_t *pend = NULL;
        MOQ_TEST_CHECK_EQ_INT(moq_sub_subscribe(sub, &tcfg2,
            moq_simpair_now_us(sp), &pend), MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(moq_sub_update_subscription(sub, pend,
            &ucfg, moq_simpair_now_us(sp)), MOQ_ERR_WRONG_STATE);
    }

    /* Foreign track: separate simpair so it has its own session. */
    {
        test_alloc_state_t as2 = {0};
        moq_alloc_t alloc2 = test_allocator(&as2);
        moq_simpair_t *sp2; moq_publisher_t *pub2; moq_pub_track_t *pt2;
        moq_subscriber_t *sub2 = NULL; moq_sub_track_t *track2 = NULL;
        sub_cb_state_t cbs2 = {0};
        setup_active_track(&as2, &alloc2, &sp2, &pub2, &pt2,
            &sub2, &track2, &cbs2);
        MOQ_TEST_CHECK_EQ_INT(moq_sub_update_subscription(sub2, track,
            &ucfg, moq_simpair_now_us(sp2)), MOQ_ERR_INVAL);
        teardown_update_test(sub2, pub2, sp2);
        MOQ_TEST_CHECK(as2.balance == 0);
    }

    /* Closed subscriber: close transport, tick sub so it sees
     * SESSION_CLOSED, then update the original sub/track. */
    moq_session_on_transport_close(moq_simpair_client(sp), 0,
        moq_simpair_now_us(sp));
    moq_sub_tick(sub, moq_simpair_now_us(sp));
    MOQ_TEST_CHECK_EQ_INT(moq_sub_update_subscription(sub, track,
        &ucfg, moq_simpair_now_us(sp)), MOQ_ERR_CLOSED);

    moq_sub_destroy(sub);
    moq_pub_destroy(pub);
    { moq_event_t d;
      while (moq_session_poll_events(moq_simpair_server(sp), &d, 1) == 1)
          moq_event_cleanup(&d);
      while (moq_session_poll_events(moq_simpair_client(sp), &d, 1) == 1)
          moq_event_cleanup(&d); }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("update_invalid_states");
}

static void test_update_would_block(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *ptrack;
    moq_subscriber_t *sub = NULL; moq_sub_track_t *track = NULL;
    sub_cb_state_t cbs = {0};
    setup_active_track(&as, &alloc, &sp, &pub, &ptrack, &sub, &track, &cbs);

    test_session_fill_action_queue(moq_simpair_client(sp));

    moq_sub_update_cfg_t ucfg; moq_sub_update_cfg_init(&ucfg);
    ucfg.has_subscriber_priority = true;
    ucfg.subscriber_priority = 50;
    MOQ_TEST_CHECK_EQ_INT(moq_sub_update_subscription(sub, track, &ucfg,
        moq_simpair_now_us(sp)), MOQ_ERR_WOULD_BLOCK);

    /* Drain and retry. */
    { moq_action_t da[64]; size_t dn;
      while ((dn = moq_session_poll_actions(moq_simpair_client(sp),
              da, 64)) > 0)
          for (size_t i = 0; i < dn; i++) moq_action_cleanup(&da[i]); }
    MOQ_TEST_CHECK_EQ_INT(moq_sub_update_subscription(sub, track, &ucfg,
        moq_simpair_now_us(sp)), MOQ_OK);

    teardown_update_test(sub, pub, sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("update_would_block");
}

/* -- Auth token facade tests ---------------------------------------- */

static void test_subscribe_with_auth_token(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *ptrack;
    setup_pair(&as, &alloc, &sp, &pub, &ptrack);

    sub_cb_state_t cbs = {0};
    moq_sub_cfg_t cfg; moq_sub_cfg_init(&cfg);
    cfg.callbacks.ctx = &cbs;
    cfg.callbacks.on_subscribed = on_subscribed;
    moq_subscriber_t *sub = NULL;
    MOQ_TEST_CHECK_EQ_INT(moq_sub_create(moq_simpair_client(sp), &alloc,
        &cfg, &sub), MOQ_OK);

    moq_sub_track_cfg_t tcfg; moq_sub_track_cfg_init(&tcfg);
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("test") };
    tcfg.track_namespace.parts = ns;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    uint8_t tok_val[] = { 0xCA, 0xFE };
    moq_auth_token_t tok = {
        .token_type = 42,
        .token_value = { tok_val, sizeof(tok_val) },
    };
    tcfg.auth_tokens = &tok;
    tcfg.auth_token_count = 1;

    moq_sub_track_t *track = NULL;
    MOQ_TEST_CHECK_EQ_INT(moq_sub_subscribe(sub, &tcfg,
        moq_simpair_now_us(sp), &track), MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    /* Poll server session directly to see tokens before pub_tick
     * consumes the event. */
    moq_event_t ev;
    MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(
        moq_simpair_server(sp), &ev, 1), 1);
    MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
    MOQ_TEST_CHECK_EQ_SIZE(ev.u.subscribe_request.token_count, 1);
    MOQ_TEST_CHECK_EQ_U64(ev.u.subscribe_request.tokens[0].token_type, 42);
    MOQ_TEST_CHECK_EQ_SIZE(
        ev.u.subscribe_request.tokens[0].token_value.len, 2);
    MOQ_TEST_CHECK(
        ev.u.subscribe_request.tokens[0].token_value.data[0] == 0xCA);
    moq_event_cleanup(&ev);

    moq_sub_destroy(sub);
    moq_pub_destroy(pub);
    { moq_event_t d;
      while (moq_session_poll_events(moq_simpair_server(sp), &d, 1) == 1)
          moq_event_cleanup(&d);
      while (moq_session_poll_events(moq_simpair_client(sp), &d, 1) == 1)
          moq_event_cleanup(&d); }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("subscribe_with_auth_token");
}

static void test_fetch_with_auth_token(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *ptrack;
    setup_pair(&as, &alloc, &sp, &pub, &ptrack);

    sub_cb_state_t cbs = {0};
    moq_sub_cfg_t cfg; moq_sub_cfg_init(&cfg);
    cfg.callbacks.ctx = &cbs;
    moq_subscriber_t *sub = NULL;
    MOQ_TEST_CHECK_EQ_INT(moq_sub_create(moq_simpair_client(sp), &alloc,
        &cfg, &sub), MOQ_OK);

    moq_sub_fetch_cfg_t fcfg; moq_sub_fetch_cfg_init(&fcfg);
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("test") };
    fcfg.track_namespace.parts = ns;
    fcfg.track_namespace.count = 1;
    fcfg.track_name = MOQ_BYTES_LITERAL("video");
    fcfg.end_group = 1;
    uint8_t tok_val[] = { 0xBE, 0xEF };
    moq_auth_token_t tok = {
        .token_type = 7,
        .token_value = { tok_val, sizeof(tok_val) },
    };
    fcfg.auth_tokens = &tok;
    fcfg.auth_token_count = 1;

    moq_sub_fetch_req_t *freq = NULL;
    MOQ_TEST_CHECK_EQ_INT(moq_sub_fetch(sub, &fcfg,
        moq_simpair_now_us(sp), &freq), MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    moq_event_t ev;
    MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(
        moq_simpair_server(sp), &ev, 1), 1);
    MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_FETCH_REQUEST);
    MOQ_TEST_CHECK_EQ_SIZE(ev.u.fetch_request.token_count, 1);
    MOQ_TEST_CHECK_EQ_U64(ev.u.fetch_request.tokens[0].token_type, 7);
    MOQ_TEST_CHECK_EQ_SIZE(
        ev.u.fetch_request.tokens[0].token_value.len, 2);
    MOQ_TEST_CHECK(ev.u.fetch_request.tokens[0].token_value.data[0] == 0xBE);
    moq_event_cleanup(&ev);

    moq_sub_destroy(sub);
    moq_pub_destroy(pub);
    { moq_event_t d;
      while (moq_session_poll_events(moq_simpair_server(sp), &d, 1) == 1)
          moq_event_cleanup(&d);
      while (moq_session_poll_events(moq_simpair_client(sp), &d, 1) == 1)
          moq_event_cleanup(&d); }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("fetch_with_auth_token");
}

static void test_track_status_with_auth_token(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *ptrack;
    setup_pair(&as, &alloc, &sp, &pub, &ptrack);

    sub_cb_state_t cbs = {0};
    moq_sub_cfg_t cfg; moq_sub_cfg_init(&cfg);
    cfg.callbacks.ctx = &cbs;
    moq_subscriber_t *sub = NULL;
    MOQ_TEST_CHECK_EQ_INT(moq_sub_create(moq_simpair_client(sp), &alloc,
        &cfg, &sub), MOQ_OK);

    moq_sub_status_cfg_t scfg; moq_sub_status_cfg_init(&scfg);
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("test") };
    scfg.track_namespace.parts = ns;
    scfg.track_namespace.count = 1;
    scfg.track_name = MOQ_BYTES_LITERAL("video");
    uint8_t tok_val[] = { 0xAA };
    moq_auth_token_t tok = {
        .token_type = 55,
        .token_value = { tok_val, sizeof(tok_val) },
    };
    scfg.auth_tokens = &tok;
    scfg.auth_token_count = 1;

    moq_sub_status_req_t *sreq = NULL;
    MOQ_TEST_CHECK_EQ_INT(moq_sub_track_status(sub, &scfg,
        moq_simpair_now_us(sp), &sreq), MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    moq_event_t ev;
    MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(
        moq_simpair_server(sp), &ev, 1), 1);
    MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_TRACK_STATUS_REQUEST);
    MOQ_TEST_CHECK_EQ_SIZE(ev.u.track_status_request.token_count, 1);
    MOQ_TEST_CHECK_EQ_U64(
        ev.u.track_status_request.tokens[0].token_type, 55);
    MOQ_TEST_CHECK_EQ_SIZE(
        ev.u.track_status_request.tokens[0].token_value.len, 1);
    MOQ_TEST_CHECK(
        ev.u.track_status_request.tokens[0].token_value.data[0] == 0xAA);
    moq_event_cleanup(&ev);

    moq_sub_destroy(sub);
    moq_pub_destroy(pub);
    { moq_event_t d;
      while (moq_session_poll_events(moq_simpair_server(sp), &d, 1) == 1)
          moq_event_cleanup(&d);
      while (moq_session_poll_events(moq_simpair_client(sp), &d, 1) == 1)
          moq_event_cleanup(&d); }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("track_status_with_auth_token");
}

static void test_subscribe_auth_token_invalid(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *ptrack;
    setup_pair(&as, &alloc, &sp, &pub, &ptrack);

    sub_cb_state_t cbs = {0};
    moq_sub_cfg_t cfg; moq_sub_cfg_init(&cfg);
    cfg.callbacks.ctx = &cbs;
    moq_subscriber_t *sub = NULL;
    MOQ_TEST_CHECK_EQ_INT(moq_sub_create(moq_simpair_client(sp), &alloc,
        &cfg, &sub), MOQ_OK);

    moq_sub_track_cfg_t tcfg; moq_sub_track_cfg_init(&tcfg);
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("test") };
    tcfg.track_namespace.parts = ns;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    tcfg.auth_tokens = NULL;
    tcfg.auth_token_count = 1;

    moq_sub_track_t *track = NULL;
    MOQ_TEST_CHECK_EQ_INT(moq_sub_subscribe(sub, &tcfg,
        moq_simpair_now_us(sp), &track), MOQ_ERR_INVAL);
    MOQ_TEST_CHECK(track == NULL);

    moq_sub_destroy(sub);
    moq_pub_destroy(pub);
    { moq_event_t d;
      while (moq_session_poll_events(moq_simpair_server(sp), &d, 1) == 1)
          moq_event_cleanup(&d);
      while (moq_session_poll_events(moq_simpair_client(sp), &d, 1) == 1)
          moq_event_cleanup(&d); }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("subscribe_auth_token_invalid");
}

static void test_subscribe_old_struct_size(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *ptrack;
    setup_pair(&as, &alloc, &sp, &pub, &ptrack);

    sub_cb_state_t cbs = {0};
    moq_sub_cfg_t cfg; moq_sub_cfg_init(&cfg);
    cfg.callbacks.ctx = &cbs;
    cfg.callbacks.on_subscribed = on_subscribed;
    moq_subscriber_t *sub = NULL;
    MOQ_TEST_CHECK_EQ_INT(moq_sub_create(moq_simpair_client(sp), &alloc,
        &cfg, &sub), MOQ_OK);

    moq_sub_track_cfg_t tcfg; moq_sub_track_cfg_init(&tcfg);
    tcfg.struct_size = offsetof(moq_sub_track_cfg_t, auth_tokens);
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("test") };
    tcfg.track_namespace.parts = ns;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");

    moq_sub_track_t *track = NULL;
    MOQ_TEST_CHECK_EQ_INT(moq_sub_subscribe(sub, &tcfg,
        moq_simpair_now_us(sp), &track), MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    moq_event_t ev;
    MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(
        moq_simpair_server(sp), &ev, 1), 1);
    MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
    MOQ_TEST_CHECK_EQ_SIZE(ev.u.subscribe_request.token_count, 0);
    moq_event_cleanup(&ev);

    moq_sub_destroy(sub);
    moq_pub_destroy(pub);
    { moq_event_t d;
      while (moq_session_poll_events(moq_simpair_server(sp), &d, 1) == 1)
          moq_event_cleanup(&d);
      while (moq_session_poll_events(moq_simpair_client(sp), &d, 1) == 1)
          moq_event_cleanup(&d); }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("subscribe_old_struct_size");
}

static void test_fetch_old_struct_size(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *ptrack;
    setup_pair(&as, &alloc, &sp, &pub, &ptrack);

    moq_sub_cfg_t cfg; moq_sub_cfg_init(&cfg);
    moq_subscriber_t *sub = NULL;
    MOQ_TEST_CHECK_EQ_INT(moq_sub_create(moq_simpair_client(sp), &alloc,
        &cfg, &sub), MOQ_OK);

    moq_sub_fetch_cfg_t fcfg; moq_sub_fetch_cfg_init(&fcfg);
    fcfg.struct_size = offsetof(moq_sub_fetch_cfg_t, auth_tokens);
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("test") };
    fcfg.track_namespace.parts = ns;
    fcfg.track_namespace.count = 1;
    fcfg.track_name = MOQ_BYTES_LITERAL("video");
    fcfg.end_group = 1;

    moq_sub_fetch_req_t *freq = NULL;
    MOQ_TEST_CHECK_EQ_INT(moq_sub_fetch(sub, &fcfg,
        moq_simpair_now_us(sp), &freq), MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    moq_event_t ev;
    MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(
        moq_simpair_server(sp), &ev, 1), 1);
    MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_FETCH_REQUEST);
    MOQ_TEST_CHECK_EQ_SIZE(ev.u.fetch_request.token_count, 0);
    moq_event_cleanup(&ev);

    moq_sub_destroy(sub);
    moq_pub_destroy(pub);
    { moq_event_t d;
      while (moq_session_poll_events(moq_simpair_server(sp), &d, 1) == 1)
          moq_event_cleanup(&d);
      while (moq_session_poll_events(moq_simpair_client(sp), &d, 1) == 1)
          moq_event_cleanup(&d); }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("fetch_old_struct_size");
}

static void test_track_status_old_struct_size(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *ptrack;
    setup_pair(&as, &alloc, &sp, &pub, &ptrack);

    moq_sub_cfg_t cfg; moq_sub_cfg_init(&cfg);
    moq_subscriber_t *sub = NULL;
    MOQ_TEST_CHECK_EQ_INT(moq_sub_create(moq_simpair_client(sp), &alloc,
        &cfg, &sub), MOQ_OK);

    moq_sub_status_cfg_t scfg; moq_sub_status_cfg_init(&scfg);
    scfg.struct_size = offsetof(moq_sub_status_cfg_t, auth_tokens);
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("test") };
    scfg.track_namespace.parts = ns;
    scfg.track_namespace.count = 1;
    scfg.track_name = MOQ_BYTES_LITERAL("video");

    moq_sub_status_req_t *sreq = NULL;
    MOQ_TEST_CHECK_EQ_INT(moq_sub_track_status(sub, &scfg,
        moq_simpair_now_us(sp), &sreq), MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    moq_event_t ev;
    MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(
        moq_simpair_server(sp), &ev, 1), 1);
    MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_TRACK_STATUS_REQUEST);
    MOQ_TEST_CHECK_EQ_SIZE(ev.u.track_status_request.token_count, 0);
    moq_event_cleanup(&ev);

    moq_sub_destroy(sub);
    moq_pub_destroy(pub);
    { moq_event_t d;
      while (moq_session_poll_events(moq_simpair_server(sp), &d, 1) == 1)
          moq_event_cleanup(&d);
      while (moq_session_poll_events(moq_simpair_client(sp), &d, 1) == 1)
          moq_event_cleanup(&d); }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("track_status_old_struct_size");
}

static void test_subscribe_done_marks_track_done(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *ptrack;
    setup_pair(&as, &alloc, &sp, &pub, &ptrack);

    sub_cb_state_t cbs = {0};
    moq_sub_cfg_t cfg; moq_sub_cfg_init(&cfg);
    cfg.callbacks.ctx = &cbs;
    cfg.callbacks.on_subscribed = on_subscribed;
    moq_subscriber_t *sub = NULL;
    MOQ_TEST_CHECK_EQ_INT(moq_sub_create(moq_simpair_client(sp), &alloc,
        &cfg, &sub), MOQ_OK);

    moq_sub_track_cfg_t tcfg; moq_sub_track_cfg_init(&tcfg);
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("test") };
    tcfg.track_namespace.parts = ns;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_sub_track_t *track = NULL;
    MOQ_TEST_CHECK_EQ_INT(moq_sub_subscribe(sub, &tcfg,
        moq_simpair_now_us(sp), &track), MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_pub_tick(pub, moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_sub_tick(sub, moq_simpair_now_us(sp));
    MOQ_TEST_CHECK(moq_sub_track_is_active(track));

    /* Publisher terminates the subscription via done_subscribe.
     * Poll server events to find the subscription handle. */
    moq_event_t ev;
    while (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1)
        moq_event_cleanup(&ev);

    moq_session_t *sv = moq_simpair_server(sp);

    /* Find the publisher-role subscription on the server. */
    moq_subscription_t sv_handle = MOQ_SUBSCRIPTION_INVALID;
    for (size_t i = 0; i < sv->sub_cap; i++) {
        if (sv->subs[i].state == MOQ_SUB_ESTABLISHED &&
            sv->subs[i].role == MOQ_SUB_ROLE_PUBLISHER) {
            sv_handle = sv->subs[i].handle;
            break;
        }
    }
    MOQ_TEST_CHECK(!moq_subscription_eq(sv_handle, MOQ_SUBSCRIPTION_INVALID));

    moq_done_subscribe_cfg_t dcfg;
    moq_done_subscribe_cfg_init(&dcfg);
    dcfg.status_code = 0x0;
    MOQ_TEST_CHECK_EQ_INT(moq_session_done_subscribe(sv, sv_handle,
        &dcfg, moq_simpair_now_us(sp)), MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_sub_tick(sub, moq_simpair_now_us(sp));

    MOQ_TEST_CHECK_EQ_INT(moq_sub_track_get_state(track),
        MOQ_SUB_TRACK_DONE);

    moq_sub_destroy(sub);
    moq_pub_destroy(pub);
    { moq_event_t d;
      while (moq_session_poll_events(moq_simpair_server(sp), &d, 1) == 1)
          moq_event_cleanup(&d);
      while (moq_session_poll_events(moq_simpair_client(sp), &d, 1) == 1)
          moq_event_cleanup(&d); }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("subscribe_done_marks_track_done");
}

/* Reproducer for the Slice-3 SUBSCRIBE_DONE transport blocker: the publisher
 * facade's finish_subscribers() (Track Ended, 0x2) delivered to a subscriber
 * facade must NOT close the session, must fire on_subscribe_done exactly once
 * with the status, and the freed slot must be reusable by a later subscribe. */
static void test_finish_subscribers_keeps_session(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *ptrack;
    setup_pair(&as, &alloc, &sp, &pub, &ptrack);

    sub_cb_state_t cbs = {0};
    moq_sub_cfg_t cfg; moq_sub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.callbacks.ctx = &cbs;
    cfg.callbacks.on_subscribed = on_subscribed;
    cfg.on_subscribe_done = on_subscribe_done;
    moq_subscriber_t *sub = NULL;
    MOQ_TEST_CHECK_EQ_INT(moq_sub_create(moq_simpair_client(sp), &alloc,
        &cfg, &sub), MOQ_OK);

    moq_sub_track_cfg_t tcfg; moq_sub_track_cfg_init(&tcfg);
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("test") };
    tcfg.track_namespace.parts = ns;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_sub_track_t *track = NULL;
    MOQ_TEST_CHECK_EQ_INT(moq_sub_subscribe(sub, &tcfg,
        moq_simpair_now_us(sp), &track), MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_pub_tick(pub, moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_sub_tick(sub, moq_simpair_now_us(sp));
    MOQ_TEST_CHECK(moq_sub_track_is_active(track));

    /* Write a live object so finish() must close an OPEN subgroup (the real
     * live->VOD scenario), then drain it to the subscriber. */
    uint8_t data[] = { 0xAA, 0xBB };
    moq_rcbuf_t *p0 = NULL;
    moq_rcbuf_create(&alloc, data, sizeof(data), &p0);
    MOQ_TEST_CHECK_EQ_INT(moq_pub_write_object(pub, ptrack, 0, 0, p0,
        moq_simpair_now_us(sp)), MOQ_OK);
    moq_rcbuf_decref(p0);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_sub_tick(sub, moq_simpair_now_us(sp));
    { moq_sub_object_t o;
      while (moq_sub_poll_object(sub, &o) == MOQ_OK) moq_sub_object_cleanup(&o); }

    /* Finish the subscriber with Track Ended (0x2) via the publisher facade. */
    MOQ_TEST_CHECK_EQ_INT(moq_pub_finish_subscribers(pub, ptrack,
        MOQ_PUB_DONE_TRACK_ENDED, moq_simpair_now_us(sp)), MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_sub_tick(sub, moq_simpair_now_us(sp));

    /* The session must stay open on BOTH sides (no PROTOCOL_VIOLATION). */
    MOQ_TEST_CHECK_EQ_INT(moq_session_state(moq_simpair_client(sp)),
        MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK_EQ_INT(moq_session_state(moq_simpair_server(sp)),
        MOQ_SESS_ESTABLISHED);
    /* on_subscribe_done fired exactly once with status 0x2; track is DONE. */
    MOQ_TEST_CHECK_EQ_INT(cbs.done, 1);
    MOQ_TEST_CHECK(cbs.last_done_track == track);
    MOQ_TEST_CHECK_EQ_U64(cbs.last_done_status, MOQ_PUB_DONE_TRACK_ENDED);
    MOQ_TEST_CHECK_EQ_INT(moq_sub_track_get_state(track), MOQ_SUB_TRACK_DONE);
    MOQ_TEST_CHECK(!cbs.closed);

    /* Release the DONE slot; a later subscribe reuses it (no leak/stall). */
    MOQ_TEST_CHECK_EQ_INT(moq_sub_release_track(sub, track), MOQ_OK);
    moq_sub_track_t *track2 = NULL;
    MOQ_TEST_CHECK_EQ_INT(moq_sub_subscribe(sub, &tcfg,
        moq_simpair_now_us(sp), &track2), MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_pub_tick(pub, moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_sub_tick(sub, moq_simpair_now_us(sp));
    MOQ_TEST_CHECK(moq_sub_track_is_active(track2));
    MOQ_TEST_CHECK_EQ_INT(moq_session_state(moq_simpair_client(sp)),
        MOQ_SESS_ESTABLISHED);

    moq_sub_destroy(sub);
    moq_pub_destroy(pub);
    { moq_event_t d;
      while (moq_session_poll_events(moq_simpair_server(sp), &d, 1) == 1)
          moq_event_cleanup(&d);
      while (moq_session_poll_events(moq_simpair_client(sp), &d, 1) == 1)
          moq_event_cleanup(&d); }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("finish_subscribers_keeps_session");
}

/* -- GOAWAY callback tests ------------------------------------------ */

typedef struct {
    int count;
    uint8_t uri_buf[256];
    size_t uri_len;
} goaway_cb_state_t;

static void on_goaway_cb(void *ctx, moq_bytes_t uri) {
    goaway_cb_state_t *g = (goaway_cb_state_t *)ctx;
    g->count++;
    if (uri.len > 0 && uri.len <= sizeof(g->uri_buf)) {
        memcpy(g->uri_buf, uri.data, uri.len);
        g->uri_len = uri.len;
    } else {
        g->uri_len = 0;
    }
}

static void test_goaway_callback_fires(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *ptrack;
    setup_pair(&as, &alloc, &sp, &pub, &ptrack);

    goaway_cb_state_t gcbs = {0};
    moq_sub_cfg_t cfg; moq_sub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.callbacks.ctx = &gcbs;
    cfg.on_goaway = on_goaway_cb;
    moq_subscriber_t *sub = NULL;
    MOQ_TEST_CHECK_EQ_INT(moq_sub_create(moq_simpair_client(sp), &alloc,
        &cfg, &sub), MOQ_OK);

    moq_session_goaway(moq_simpair_server(sp),
        (const uint8_t *)"https://new.example", 19,
        moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_sub_tick(sub, moq_simpair_now_us(sp));

    MOQ_TEST_CHECK_EQ_INT(gcbs.count, 1);
    MOQ_TEST_CHECK_EQ_SIZE(gcbs.uri_len, 19);
    MOQ_TEST_CHECK(memcmp(gcbs.uri_buf, "https://new.example", 19) == 0);
    MOQ_TEST_CHECK(moq_sub_is_draining(sub));

    moq_sub_destroy(sub);
    moq_pub_destroy(pub);
    { moq_event_t d;
      while (moq_session_poll_events(moq_simpair_server(sp), &d, 1) == 1)
          moq_event_cleanup(&d);
      while (moq_session_poll_events(moq_simpair_client(sp), &d, 1) == 1)
          moq_event_cleanup(&d); }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("goaway_callback_fires");
}

static void test_goaway_empty_uri(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *ptrack;
    setup_pair(&as, &alloc, &sp, &pub, &ptrack);

    goaway_cb_state_t gcbs = {0};
    moq_sub_cfg_t cfg; moq_sub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.callbacks.ctx = &gcbs;
    cfg.on_goaway = on_goaway_cb;
    moq_subscriber_t *sub = NULL;
    moq_sub_create(moq_simpair_client(sp), &alloc, &cfg, &sub);

    moq_session_goaway(moq_simpair_server(sp), NULL, 0,
        moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_sub_tick(sub, moq_simpair_now_us(sp));

    MOQ_TEST_CHECK_EQ_INT(gcbs.count, 1);
    MOQ_TEST_CHECK_EQ_SIZE(gcbs.uri_len, 0);
    MOQ_TEST_CHECK(moq_sub_is_draining(sub));

    moq_sub_destroy(sub);
    moq_pub_destroy(pub);
    { moq_event_t d;
      while (moq_session_poll_events(moq_simpair_server(sp), &d, 1) == 1)
          moq_event_cleanup(&d);
      while (moq_session_poll_events(moq_simpair_client(sp), &d, 1) == 1)
          moq_event_cleanup(&d); }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("goaway_empty_uri");
}

static void test_goaway_old_callback_struct_size(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *ptrack;
    setup_pair(&as, &alloc, &sp, &pub, &ptrack);

    sub_cb_state_t cbs = {0};
    moq_sub_cfg_t cfg; moq_sub_cfg_init(&cfg);
    cfg.callbacks.ctx = &cbs;
    cfg.callbacks.on_draining = on_draining;
    cfg.struct_size = offsetof(moq_sub_cfg_t, on_goaway);

    moq_subscriber_t *sub = NULL;
    moq_sub_create(moq_simpair_client(sp), &alloc, &cfg, &sub);

    moq_session_goaway(moq_simpair_server(sp), NULL, 0,
        moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_sub_tick(sub, moq_simpair_now_us(sp));

    MOQ_TEST_CHECK_EQ_INT(cbs.draining, 1);
    MOQ_TEST_CHECK(moq_sub_is_draining(sub));

    moq_sub_destroy(sub);
    moq_pub_destroy(pub);
    { moq_event_t d;
      while (moq_session_poll_events(moq_simpair_server(sp), &d, 1) == 1)
          moq_event_cleanup(&d);
      while (moq_session_poll_events(moq_simpair_client(sp), &d, 1) == 1)
          moq_event_cleanup(&d); }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("goaway_old_callback_struct_size");
}

int main(void) {
    test_subscribe_accepted();
    test_subscribe_rejected();
    test_object_received();
    test_session_closed();
    test_object_cleanup_releases_refs();
    test_object_queue_full_pending_retry();
    test_streaming_chunk_rejected();
    test_datagram_object_received();
    test_status_datagram_received();
    test_datagram_with_properties();
    test_pending_retry_with_properties();
    test_object_byte_budget_payload_only();
    test_object_byte_budget_with_properties();
    test_sub_cfg_init_old_prefix_no_overflow();
    test_byte_budget_old_struct_size();
    test_track_error_query();
    test_track_state_after_close();
    test_track_status_ok();
    test_track_status_error();
    test_track_status_slot_reuse();
    test_track_status_session_close();
    test_track_status_ok_with_fields();
    test_track_status_error_with_retry();
    test_fetch_ok_object_complete();
    test_fetch_object_before_ok();
    test_fetch_gap();
    test_fetch_error_retry_fields();
    test_fetch_cancel();
    test_fetch_queue_full_pending_retry();
    test_fetch_object_byte_budget();
    test_fetch_slot_reuse();
    test_fetch_cancel_slot_reuse();
    test_fetch_cancel_purges_pending_item();
    test_fetch_cancel_purges_queued_items();
    test_fetch_cancel_keeps_other_fetch_items();
    test_fetch_cancel_after_active();
    test_streaming_chunk_received();
    test_streaming_chunk_queue_full_pending_retry();
    test_joining_fetch_relative_success();
    test_joining_fetch_absolute_success();
    test_joining_fetch_invalid_track();
    test_joining_fetch_slot_reuse();

    test_update_happy_path();
    test_update_all_fields();
    test_update_pending_wrong_state();
    test_update_error_clears_pending();
    test_update_invalid_states();
    test_update_would_block();

    test_subscribe_with_auth_token();
    test_fetch_with_auth_token();
    test_track_status_with_auth_token();
    test_subscribe_auth_token_invalid();
    test_subscribe_old_struct_size();
    test_fetch_old_struct_size();
    test_track_status_old_struct_size();
    test_subscribe_done_marks_track_done();
    test_finish_subscribers_keeps_session();
    test_goaway_callback_fires();
    test_goaway_empty_uri();
    test_goaway_old_callback_struct_size();

    if (failures == 0) {
        printf("PASS: all subscriber tests\n");
        return 0;
    }
    printf("FAIL: %d subscriber test failures\n", failures);
    return 1;
}
