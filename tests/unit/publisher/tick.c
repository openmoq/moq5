#include "test_cases.h"

void test_tick_subscribe_accept(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    tick_cb_state_t cbs = {0};
    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    cfg.callbacks.ctx = &cbs;
    cfg.callbacks.on_subscriber_joined = tick_on_joined;
    cfg.callbacks.on_subscriber_left = tick_on_left;
    cfg.callbacks.on_draining = tick_on_draining;
    cfg.callbacks.on_closed = tick_on_closed;

    moq_publisher_t *pub = NULL;
    moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub);

    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");

    moq_pub_track_t *track = NULL;
    moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &track);

    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace.parts = ns_parts;
    scfg.track_namespace.count = 1;
    scfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_subscription_t sub_h;
    moq_session_subscribe(moq_simpair_client(sp), &scfg,
        moq_simpair_now_us(sp), &sub_h);
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    moq_pub_tick(pub, moq_simpair_now_us(sp));

    MOQ_TEST_CHECK(cbs.joined == 1);
    MOQ_TEST_CHECK(cbs.joined_track == track);
    MOQ_TEST_CHECK(moq_pub_has_subscriber(pub, track));

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("tick_subscribe_accept");
}

/* Drive a subscribe so on_subscriber_joined would dispatch; returns the joined
 * count observed in cbs. Used by the partial-callback-copy test. */
static int partial_cb_joined_count(moq_simpair_t *sp, moq_alloc_t *alloc,
                                   uint32_t outer_struct_size) {
    tick_cb_state_t cbs = {0};
    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    cfg.callbacks.ctx = &cbs;
    cfg.callbacks.on_subscriber_joined = tick_on_joined;
    cfg.struct_size = outer_struct_size;   /* may truncate the callbacks block */

    moq_publisher_t *pub = NULL;
    MOQ_TEST_CHECK(moq_pub_create(moq_simpair_server(sp), alloc, &cfg, &pub)
                   == MOQ_OK);

    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_pub_track_t *track = NULL;
    moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &track);

    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace.parts = ns_parts;
    scfg.track_namespace.count = 1;
    scfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_subscription_t sub_h;
    moq_session_subscribe(moq_simpair_client(sp), &scfg,
                          moq_simpair_now_us(sp), &sub_h);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_pub_tick(pub, moq_simpair_now_us(sp));

    /* Subscription accepted regardless of whether the callback was installed. */
    MOQ_TEST_CHECK(moq_pub_has_subscriber(pub, track));
    int joined = cbs.joined;
    moq_pub_destroy(pub);
    return joined;
}

/* moq_pub_create must never install a partially-copied callback pointer: an outer
 * struct_size ending mid-pointer would otherwise leave a truncated, non-NULL
 * pointer that dispatch later calls (a crash under ASan). */
void test_pub_create_partial_callbacks(void) {
    const size_t cb_off = offsetof(moq_pub_cfg_t, callbacks);

    /* (1) struct_size ends one byte into on_subscriber_joined -> the callback is
     * dropped (left NULL), not truncated; the subscribe still succeeds. */
    {
        test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
        simpair_setup(&as, &alloc, &sp);
        uint32_t ss = (uint32_t)(cb_off +
            offsetof(moq_pub_callbacks_t, on_subscriber_joined) + 1);
        MOQ_TEST_CHECK(partial_cb_joined_count(sp, &alloc, ss) == 0);
        drain_all(sp);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* (2) struct_size ending exactly at a whole-field boundary (the v0 callback
     * size, through on_closed) still copies and fires the v0 callbacks. */
    {
        test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
        simpair_setup(&as, &alloc, &sp);
        uint32_t ss = (uint32_t)(cb_off +
            offsetof(moq_pub_callbacks_t, on_subscriber_updated));
        MOQ_TEST_CHECK(partial_cb_joined_count(sp, &alloc, ss) == 1);
        drain_all(sp);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    MOQ_TEST_PASS("pub_create_partial_callbacks");
}

/* The streaming begin/write/end sequence must no-op consistently when the track
 * has no subscriber: begin_object already returns OK, so write_data/end_object
 * must too (matching the non-streaming moq_pub_write_object path) rather than
 * reporting WRONG_STATE for a sequence that began successfully. */
void test_pub_streaming_no_subscriber_noop(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    moq_publisher_t *pub = NULL;
    moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub);

    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_pub_track_t *track = NULL;
    moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &track);

    /* No subscriber. */
    MOQ_TEST_CHECK(!moq_pub_has_subscriber(pub, track));

    /* Non-streaming write is already a no-subscriber no-op (the reference). */
    moq_rcbuf_t *p0 = NULL;
    moq_rcbuf_create(&alloc, (const uint8_t *)"x", 1, &p0);
    moq_pub_object_cfg_t ocfg;
    moq_pub_object_cfg_init_sized(&ocfg, sizeof(ocfg));
    ocfg.group_id = 0;
    ocfg.object_id = 0;
    ocfg.payload = p0;
    MOQ_TEST_CHECK(moq_pub_write_object_ex(pub, track, &ocfg,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_rcbuf_decref(p0);

    /* Streaming sequence must be consistent: all three no-op OK. */
    moq_pub_begin_object_cfg_t bcfg;
    moq_pub_begin_object_cfg_init_sized(&bcfg, sizeof(bcfg));
    bcfg.group_id = 1;
    bcfg.object_id = 0;
    bcfg.payload_length = 4;
    MOQ_TEST_CHECK(moq_pub_begin_object(pub, track, &bcfg,
        moq_simpair_now_us(sp)) == MOQ_OK);

    moq_rcbuf_t *chunk = NULL;
    moq_rcbuf_create(&alloc, (const uint8_t *)"data", 4, &chunk);
    MOQ_TEST_CHECK(moq_pub_write_data(pub, track, chunk,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_rcbuf_decref(chunk);

    MOQ_TEST_CHECK(moq_pub_end_object(pub, track,
        moq_simpair_now_us(sp)) == MOQ_OK);

    /* After end_track the terminal-track invariant must win over the
     * no-subscriber no-op: write_data/end_object are WRONG_STATE, not OK. (With
     * a subscriber the not-streaming slot would mask this; with none, the
     * track->ended guard must precede the no-slot check.) */
    MOQ_TEST_CHECK(moq_pub_end_track(pub, track,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_rcbuf_t *c2 = NULL;
    moq_rcbuf_create(&alloc, (const uint8_t *)"data", 4, &c2);
    MOQ_TEST_CHECK(moq_pub_write_data(pub, track, c2,
        moq_simpair_now_us(sp)) == MOQ_ERR_WRONG_STATE);
    moq_rcbuf_decref(c2);
    MOQ_TEST_CHECK(moq_pub_end_object(pub, track,
        moq_simpair_now_us(sp)) == MOQ_ERR_WRONG_STATE);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("pub_streaming_no_subscriber_noop");
}

void test_tick_unsubscribe_clears(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    tick_cb_state_t cbs = {0};
    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    cfg.callbacks.ctx = &cbs;
    cfg.callbacks.on_subscriber_joined = tick_on_joined;
    cfg.callbacks.on_subscriber_left = tick_on_left;

    moq_publisher_t *pub = NULL;
    moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub);

    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");

    moq_pub_track_t *track = NULL;
    moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &track);

    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace.parts = ns_parts;
    scfg.track_namespace.count = 1;
    scfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_subscription_t sub_h;
    moq_session_subscribe(moq_simpair_client(sp), &scfg,
        moq_simpair_now_us(sp), &sub_h);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_pub_tick(pub, moq_simpair_now_us(sp));
    MOQ_TEST_CHECK(moq_pub_has_subscriber(pub, track));

    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev); }

    moq_session_unsubscribe(moq_simpair_client(sp), sub_h,
        moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_pub_tick(pub, moq_simpair_now_us(sp));

    MOQ_TEST_CHECK(cbs.left == 1);
    MOQ_TEST_CHECK(cbs.left_track == track);
    MOQ_TEST_CHECK(!moq_pub_has_subscriber(pub, track));

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("tick_unsubscribe_clears");
}

void test_tick_goaway_draining(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    tick_cb_state_t cbs = {0};
    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    cfg.callbacks.ctx = &cbs;
    cfg.callbacks.on_draining = tick_on_draining;

    moq_publisher_t *pub = NULL;
    moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub);

    MOQ_TEST_CHECK(!moq_pub_is_draining(pub));

    moq_session_goaway(moq_simpair_client(sp), NULL, 0,
        moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_pub_tick(pub, moq_simpair_now_us(sp));

    MOQ_TEST_CHECK(moq_pub_is_draining(pub));
    MOQ_TEST_CHECK(cbs.draining == 1);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("tick_goaway_draining");
}

void test_tick_session_closed(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    tick_cb_state_t cbs = {0};
    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    cfg.callbacks.ctx = &cbs;
    cfg.callbacks.on_closed = tick_on_closed;
    cfg.callbacks.on_subscriber_left = tick_on_left;

    moq_publisher_t *pub = NULL;
    moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub);

    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_pub_track_t *track = NULL;
    moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &track);

    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace.parts = ns_parts;
    scfg.track_namespace.count = 1;
    scfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_subscription_t sub_h;
    moq_session_subscribe(moq_simpair_client(sp), &scfg,
        moq_simpair_now_us(sp), &sub_h);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_pub_tick(pub, moq_simpair_now_us(sp));

    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev); }

    /* Send a complete but unknown control message to close the session.
     * Type 0x3F (unknown), length 0x0001, payload 0x00. */
    uint8_t bad[] = { 0x3F, 0x00, 0x01, 0x00 };
    moq_session_on_control_bytes(moq_simpair_server(sp), bad, sizeof(bad),
        moq_simpair_now_us(sp));
    moq_pub_tick(pub, moq_simpair_now_us(sp));

    MOQ_TEST_CHECK(cbs.closed == 1);
    MOQ_TEST_CHECK(!moq_pub_has_subscriber(pub, track));

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("tick_session_closed");
}

void test_tick_pending_retry_fires_joined(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);
    moq_session_t *sv = moq_simpair_server(sp);
    moq_session_t *cl = moq_simpair_client(sp);

    tick_cb_state_t cbs = {0};
    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    cfg.callbacks.ctx = &cbs;
    cfg.callbacks.on_subscriber_joined = tick_on_joined;

    moq_publisher_t *pub = NULL;
    moq_pub_create(sv, &alloc, &cfg, &pub);

    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_pub_track_t *track = NULL;
    moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &track);

    moq_subscribe_cfg_t sub_cfg;
    moq_subscribe_cfg_init(&sub_cfg);
    sub_cfg.track_namespace.parts = ns_parts;
    sub_cfg.track_namespace.count = 1;
    sub_cfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_subscription_t sub_h;
    moq_session_subscribe(cl, &sub_cfg, moq_simpair_now_us(sp), &sub_h);
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    /* Fill server action queue so accept_subscribe WBs. */
    test_session_fill_action_queue(sv);

    moq_result_t rc = moq_pub_tick(pub, moq_simpair_now_us(sp));
    MOQ_TEST_CHECK(rc == MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(cbs.joined == 0);
    MOQ_TEST_CHECK(!moq_pub_has_subscriber(pub, track));

    /* Drain all filler actions. */
    { moq_action_t acts[64]; size_t na;
      while ((na = moq_session_poll_actions(sv, acts, 64)) > 0)
          for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]); }

    /* Retry: flush_pending succeeds, on_subscriber_joined fires. */
    rc = moq_pub_tick(pub, moq_simpair_now_us(sp));
    MOQ_TEST_CHECK(rc == MOQ_OK);
    MOQ_TEST_CHECK(cbs.joined == 1);
    MOQ_TEST_CHECK(cbs.joined_track == track);
    MOQ_TEST_CHECK(moq_pub_has_subscriber(pub, track));

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("tick_pending_retry_fires_joined");
}

void test_tick_pending_then_closed(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);
    moq_session_t *sv = moq_simpair_server(sp);
    moq_session_t *cl = moq_simpair_client(sp);

    tick_cb_state_t cbs = {0};
    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    cfg.callbacks.ctx = &cbs;
    cfg.callbacks.on_subscriber_joined = tick_on_joined;
    cfg.callbacks.on_closed = tick_on_closed;

    moq_publisher_t *pub = NULL;
    moq_pub_create(sv, &alloc, &cfg, &pub);

    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_pub_track_t *track = NULL;
    moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &track);

    moq_subscribe_cfg_t sub_cfg;
    moq_subscribe_cfg_init(&sub_cfg);
    sub_cfg.track_namespace.parts = ns_parts;
    sub_cfg.track_namespace.count = 1;
    sub_cfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_subscription_t sub_h;
    moq_session_subscribe(cl, &sub_cfg, moq_simpair_now_us(sp), &sub_h);
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    /* Fill queue → tick WBs with pending accept. */
    test_session_fill_action_queue(sv);
    moq_result_t rc = moq_pub_tick(pub, moq_simpair_now_us(sp));
    MOQ_TEST_CHECK(rc == MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(cbs.joined == 0);

    /* Drain filler, then close server session. */
    { moq_action_t acts[64]; size_t na;
      while ((na = moq_session_poll_actions(sv, acts, 64)) > 0)
          for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]); }
    uint8_t bad[] = { 0x3F, 0x00, 0x01, 0x00 };
    moq_session_on_control_bytes(sv, bad, sizeof(bad),
        moq_simpair_now_us(sp));

    /* Retry: pending flush may succeed, but the event loop processes
     * SESSION_CLOSED and clears subscriber state. The joined callback
     * must NOT fire because the session is closed. */
    rc = moq_pub_tick(pub, moq_simpair_now_us(sp));
    MOQ_TEST_CHECK(cbs.joined == 0);
    MOQ_TEST_CHECK(cbs.closed == 1);
    MOQ_TEST_CHECK(!moq_pub_has_subscriber(pub, track));

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("tick_pending_then_closed");
}
