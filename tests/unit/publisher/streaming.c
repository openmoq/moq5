#include "test_cases.h"

/* ================================================================== */
/* write_object_ex tests                                              */
/* ================================================================== */

void test_write_object_ex_stream(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    moq_pub_cfg_t cfg; moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    moq_publisher_t *pub = NULL;
    moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub);

    moq_pub_track_cfg_t tcfg; moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_pub_track_t *track = NULL;
    moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &track);

    moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
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

    moq_rcbuf_t *pay = NULL;
    moq_rcbuf_create(&alloc, (const uint8_t *)"cfg_obj", 7, &pay);
    moq_pub_object_cfg_t obj; moq_pub_object_cfg_init_sized(&obj, sizeof(obj));
    obj.group_id = 0;
    obj.object_id = 0;
    obj.payload = pay;
    MOQ_TEST_CHECK(moq_pub_write_object_ex(pub, track, &obj,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_rcbuf_decref(pay);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("write_object_ex_stream");
}

void test_write_object_ex_datagram(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    moq_pub_cfg_t cfg; moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    moq_publisher_t *pub = NULL;
    moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub);

    moq_pub_track_cfg_t tcfg; moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_pub_track_t *track = NULL;
    moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &track);

    moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
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

    moq_rcbuf_t *pay = NULL;
    moq_rcbuf_create(&alloc, (const uint8_t *)"dg_data", 7, &pay);
    moq_pub_object_cfg_t obj; moq_pub_object_cfg_init_sized(&obj, sizeof(obj));
    obj.group_id = 0;
    obj.object_id = 1;
    obj.payload = pay;
    obj.datagram = true;
    MOQ_TEST_CHECK(moq_pub_write_object_ex(pub, track, &obj,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_rcbuf_decref(pay);

    /* Verify SEND_DATAGRAM action was queued. */
    moq_action_t acts[4];
    size_t na = moq_session_poll_actions(moq_simpair_server(sp), acts, 4);
    bool found_dg = false;
    for (size_t i = 0; i < na; i++) {
        if (acts[i].kind == MOQ_ACTION_SEND_DATAGRAM) found_dg = true;
        moq_action_cleanup(&acts[i]);
    }
    MOQ_TEST_CHECK(found_dg);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("write_object_ex_datagram");
}

void test_write_object_ex_status_datagram(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    moq_pub_cfg_t cfg; moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    moq_publisher_t *pub = NULL;
    moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub);

    moq_pub_track_cfg_t tcfg; moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_pub_track_t *track = NULL;
    moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &track);

    moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
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

    moq_pub_object_cfg_t obj; moq_pub_object_cfg_init_sized(&obj, sizeof(obj));
    obj.group_id = 0;
    obj.object_id = 0;
    obj.datagram = true;
    obj.has_status = true;
    obj.status = MOQ_OBJECT_END_OF_GROUP;
    MOQ_TEST_CHECK(moq_pub_write_object_ex(pub, track, &obj,
        moq_simpair_now_us(sp)) == MOQ_OK);

    moq_action_t acts[4];
    size_t na = moq_session_poll_actions(moq_simpair_server(sp), acts, 4);
    bool found_dg = false;
    for (size_t i = 0; i < na; i++) {
        if (acts[i].kind == MOQ_ACTION_SEND_DATAGRAM) found_dg = true;
        moq_action_cleanup(&acts[i]);
    }
    MOQ_TEST_CHECK(found_dg);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("write_object_ex_status_datagram");
}

void test_write_object_ex_invalid_combos(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    moq_pub_cfg_t cfg; moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    moq_publisher_t *pub = NULL;
    moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub);

    moq_pub_track_cfg_t tcfg; moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_pub_track_t *track = NULL;
    moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &track);

    moq_rcbuf_t *pay = NULL;
    moq_rcbuf_create(&alloc, (const uint8_t *)"x", 1, &pay);

    /* status + payload = invalid */
    {
        moq_pub_object_cfg_t obj; moq_pub_object_cfg_init_sized(&obj, sizeof(obj));
        obj.has_status = true;
        obj.status = MOQ_OBJECT_END_OF_GROUP;
        obj.payload = pay;
        MOQ_TEST_CHECK(moq_pub_write_object_ex(pub, track, &obj,
            moq_simpair_now_us(sp)) == MOQ_ERR_INVAL);
    }
    /* non-datagram, non-status, no payload = invalid */
    {
        moq_pub_object_cfg_t obj; moq_pub_object_cfg_init_sized(&obj, sizeof(obj));
        MOQ_TEST_CHECK(moq_pub_write_object_ex(pub, track, &obj,
            moq_simpair_now_us(sp)) == MOQ_ERR_INVAL);
    }
    /* properties on non-datagram: now valid (opens subgroup with extensions) */
    {
        moq_pub_object_cfg_t obj; moq_pub_object_cfg_init_sized(&obj, sizeof(obj));
        obj.payload = pay;
        obj.properties = pay;
        MOQ_TEST_CHECK(moq_pub_write_object_ex(pub, track, &obj,
            moq_simpair_now_us(sp)) == MOQ_OK);
    }
    /* NULL args */
    {
        moq_pub_object_cfg_t obj; moq_pub_object_cfg_init_sized(&obj, sizeof(obj));
        obj.payload = pay;
        MOQ_TEST_CHECK(moq_pub_write_object_ex(NULL, track, &obj,
            0) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_pub_write_object_ex(pub, NULL, &obj,
            0) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_pub_write_object_ex(pub, track, NULL,
            0) == MOQ_ERR_INVAL);
    }

    /* begin_object: invalid payload_length even without subscriber */
    {
        moq_pub_begin_object_cfg_t bcfg;
        moq_pub_begin_object_cfg_init_sized(&bcfg, sizeof(bcfg));
        bcfg.payload_length = UINT64_MAX;
        MOQ_TEST_CHECK(moq_pub_begin_object(pub, track, &bcfg,
            moq_simpair_now_us(sp)) == MOQ_ERR_INVAL);
    }
    /* non-datagram status = invalid (not supported) */
    {
        moq_pub_object_cfg_t obj; moq_pub_object_cfg_init_sized(&obj, sizeof(obj));
        obj.has_status = true;
        obj.status = MOQ_OBJECT_END_OF_GROUP;
        MOQ_TEST_CHECK(moq_pub_write_object_ex(pub, track, &obj,
            moq_simpair_now_us(sp)) == MOQ_ERR_INVAL);
    }
    /* invalid status enum = invalid even without subscriber */
    {
        moq_pub_object_cfg_t obj; moq_pub_object_cfg_init_sized(&obj, sizeof(obj));
        obj.datagram = true;
        obj.has_status = true;
        obj.status = (moq_object_status_t)99;
        MOQ_TEST_CHECK(moq_pub_write_object_ex(pub, track, &obj,
            moq_simpair_now_us(sp)) == MOQ_ERR_INVAL);
    }

    moq_rcbuf_decref(pay);
    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("write_object_ex_invalid_combos");
}

/* ================================================================== */
/* Streaming write tests                                              */
/* ================================================================== */

void test_streaming_lifecycle(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    moq_pub_cfg_t cfg; moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    moq_publisher_t *pub = NULL;
    moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub);

    moq_pub_track_cfg_t tcfg; moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_pub_track_t *track = NULL;
    moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &track);

    moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
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

    /* begin → write_data → end_object */
    moq_pub_begin_object_cfg_t bcfg;
    moq_pub_begin_object_cfg_init_sized(&bcfg, sizeof(bcfg));
    bcfg.group_id = 0;
    bcfg.object_id = 0;
    bcfg.payload_length = 5;
    MOQ_TEST_CHECK(moq_pub_begin_object(pub, track, &bcfg,
        moq_simpair_now_us(sp)) == MOQ_OK);

    moq_rcbuf_t *chunk = NULL;
    moq_rcbuf_create(&alloc, (const uint8_t *)"hello", 5, &chunk);
    MOQ_TEST_CHECK(moq_pub_write_data(pub, track, chunk,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_rcbuf_decref(chunk);

    MOQ_TEST_CHECK(moq_pub_end_object(pub, track,
        moq_simpair_now_us(sp)) == MOQ_OK);

    /* Verify data reached the client. */
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    { moq_event_t ev; bool got_obj = false;
      while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1) {
          if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED &&
              ev.u.object_received.payload &&
              moq_rcbuf_len(ev.u.object_received.payload) == 5 &&
              memcmp(moq_rcbuf_data(ev.u.object_received.payload),
                     "hello", 5) == 0)
              got_obj = true;
          moq_event_cleanup(&ev);
      }
      MOQ_TEST_CHECK(got_obj);
    }

    /* After end, complete write should work again. */
    moq_rcbuf_t *pay = NULL;
    moq_rcbuf_create(&alloc, (const uint8_t *)"next", 4, &pay);
    MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 0, 1, pay,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_rcbuf_decref(pay);

    /* Group change during streaming is allowed at begin_object. */
    moq_pub_begin_object_cfg_init_sized(&bcfg, sizeof(bcfg));
    bcfg.group_id = 1;
    bcfg.object_id = 0;
    bcfg.payload_length = 0;
    MOQ_TEST_CHECK(moq_pub_begin_object(pub, track, &bcfg,
        moq_simpair_now_us(sp)) == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_end_object(pub, track,
        moq_simpair_now_us(sp)) == MOQ_OK);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("streaming_lifecycle");
}

void test_streaming_invalid_order(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    moq_pub_cfg_t cfg; moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    moq_publisher_t *pub = NULL;
    moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub);

    moq_pub_track_cfg_t tcfg; moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_pub_track_t *track = NULL;
    moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &track);

    moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
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

    moq_rcbuf_t *chunk = NULL;
    moq_rcbuf_create(&alloc, (const uint8_t *)"x", 1, &chunk);

    /* write_data before begin → WRONG_STATE */
    MOQ_TEST_CHECK(moq_pub_write_data(pub, track, chunk,
        moq_simpair_now_us(sp)) == MOQ_ERR_WRONG_STATE);

    /* end_object before begin → WRONG_STATE */
    MOQ_TEST_CHECK(moq_pub_end_object(pub, track,
        moq_simpair_now_us(sp)) == MOQ_ERR_WRONG_STATE);

    /* end_group while streaming → WRONG_STATE */
    moq_pub_begin_object_cfg_t bcfg;
    moq_pub_begin_object_cfg_init_sized(&bcfg, sizeof(bcfg));
    bcfg.group_id = 0;
    bcfg.object_id = 0;
    bcfg.payload_length = 1;
    MOQ_TEST_CHECK(moq_pub_begin_object(pub, track, &bcfg,
        moq_simpair_now_us(sp)) == MOQ_OK);

    /* double begin → WRONG_STATE */
    MOQ_TEST_CHECK(moq_pub_begin_object(pub, track, &bcfg,
        moq_simpair_now_us(sp)) == MOQ_ERR_WRONG_STATE);

    /* complete write while streaming → WRONG_STATE */
    MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 0, 1, chunk,
        moq_simpair_now_us(sp)) == MOQ_ERR_WRONG_STATE);

    /* end_group while streaming → WRONG_STATE */
    MOQ_TEST_CHECK(moq_pub_end_group(pub, track,
        moq_simpair_now_us(sp)) == MOQ_ERR_WRONG_STATE);

    /* Datagram while streaming is OK (independent transport). */
    {
        moq_pub_object_cfg_t dobj; moq_pub_object_cfg_init(&dobj);
        dobj.group_id = 0;
        dobj.object_id = 99;
        dobj.payload = chunk;
        dobj.datagram = true;
        MOQ_TEST_CHECK(moq_pub_write_object_ex(pub, track, &dobj,
            moq_simpair_now_us(sp)) == MOQ_OK);
    }

    /* Finish streaming to leave clean state. */
    MOQ_TEST_CHECK(moq_pub_write_data(pub, track, chunk,
        moq_simpair_now_us(sp)) == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_end_object(pub, track,
        moq_simpair_now_us(sp)) == MOQ_OK);

    moq_rcbuf_decref(chunk);
    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("streaming_invalid_order");
}

void test_remove_track_while_streaming(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    moq_pub_cfg_t cfg; moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    moq_publisher_t *pub = NULL;
    moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub);

    moq_pub_track_cfg_t tcfg; moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_pub_track_t *track = NULL;
    moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &track);

    moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
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

    /* Begin streaming and write a partial chunk. */
    moq_pub_begin_object_cfg_t bcfg;
    moq_pub_begin_object_cfg_init_sized(&bcfg, sizeof(bcfg));
    bcfg.group_id = 0;
    bcfg.object_id = 0;
    bcfg.payload_length = 10;
    MOQ_TEST_CHECK(moq_pub_begin_object(pub, track, &bcfg,
        moq_simpair_now_us(sp)) == MOQ_OK);

    moq_rcbuf_t *chunk = NULL;
    moq_rcbuf_create(&alloc, (const uint8_t *)"part", 4, &chunk);
    MOQ_TEST_CHECK(moq_pub_write_data(pub, track, chunk,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_rcbuf_decref(chunk);

    /* Remove track while streaming. Should reset subgroup. */
    MOQ_TEST_CHECK(moq_pub_remove_track(pub, track,
        moq_simpair_now_us(sp)) == MOQ_ERR_WOULD_BLOCK);

    /* Verify RESET_DATA was queued. */
    moq_action_t acts[16];
    size_t na = moq_session_poll_actions(moq_simpair_server(sp), acts, 16);
    bool found_reset = false;
    for (size_t i = 0; i < na; i++) {
        if (acts[i].kind == MOQ_ACTION_RESET_DATA) found_reset = true;
        moq_action_cleanup(&acts[i]);
    }
    MOQ_TEST_CHECK(found_reset);
    MOQ_TEST_CHECK(moq_pub_remove_track(pub, track,
        moq_simpair_now_us(sp)) == MOQ_OK);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("remove_track_while_streaming");
}

void test_unsubscribe_then_write_noop(void) {
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
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev); }
    MOQ_TEST_CHECK(moq_pub_has_subscriber(pub, track));

    moq_session_unsubscribe(moq_simpair_client(sp), sub_h,
        moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_pub_tick(pub, moq_simpair_now_us(sp));
    MOQ_TEST_CHECK(!moq_pub_has_subscriber(pub, track));

    moq_rcbuf_t *buf = NULL;
    moq_rcbuf_create(&alloc, (const uint8_t *)"x", 1, &buf);
    MOQ_TEST_CHECK_EQ_INT(moq_pub_write_object(pub, track,
        0, 0, buf, moq_simpair_now_us(sp)), MOQ_OK);
    moq_rcbuf_decref(buf);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("unsubscribe_then_write_noop");
}

void test_slot_reuse_after_unsub(void) {
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

    /* First subscriber joins and writes succeed. */
    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace.parts = ns_parts;
    scfg.track_namespace.count = 1;
    scfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_subscription_t sub1;
    moq_session_subscribe(moq_simpair_client(sp), &scfg,
        moq_simpair_now_us(sp), &sub1);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_pub_tick(pub, moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev); }
    MOQ_TEST_CHECK(moq_pub_has_subscriber(pub, track));

    moq_rcbuf_t *buf = NULL;
    moq_rcbuf_create(&alloc, (const uint8_t *)"a", 1, &buf);
    MOQ_TEST_CHECK_EQ_INT(moq_pub_write_object(pub, track, 0, 0, buf,
        moq_simpair_now_us(sp)), MOQ_OK);
    moq_rcbuf_decref(buf);
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    /* Unsubscribe clears the slot. */
    moq_session_unsubscribe(moq_simpair_client(sp), sub1,
        moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_pub_tick(pub, moq_simpair_now_us(sp));
    MOQ_TEST_CHECK(!moq_pub_has_subscriber(pub, track));
    MOQ_TEST_CHECK_EQ_INT(cbs.left, 1);

    /* Write with no subscriber is a no-op. */
    moq_rcbuf_create(&alloc, (const uint8_t *)"b", 1, &buf);
    MOQ_TEST_CHECK_EQ_INT(moq_pub_write_object(pub, track, 1, 0, buf,
        moq_simpair_now_us(sp)), MOQ_OK);
    moq_rcbuf_decref(buf);

    /* Resubscribe — slot is reused with clean state. */
    moq_subscription_t sub2;
    moq_session_subscribe(moq_simpair_client(sp), &scfg,
        moq_simpair_now_us(sp), &sub2);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_pub_tick(pub, moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev); }
    MOQ_TEST_CHECK(moq_pub_has_subscriber(pub, track));
    MOQ_TEST_CHECK_EQ_INT(cbs.joined, 2);

    /* Write to resubscribed track succeeds with fresh subgroup. */
    moq_rcbuf_create(&alloc, (const uint8_t *)"c", 1, &buf);
    MOQ_TEST_CHECK_EQ_INT(moq_pub_write_object(pub, track, 2, 0, buf,
        moq_simpair_now_us(sp)), MOQ_OK);
    moq_rcbuf_decref(buf);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("slot_reuse_after_unsub");
}

void test_second_subscriber_rejected_v1(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    tick_cb_state_t cbs = {0};
    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    cfg.callbacks.ctx = &cbs;
    cfg.callbacks.on_subscriber_joined = tick_on_joined;

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
    moq_subscription_t sub1;
    moq_session_subscribe(moq_simpair_client(sp), &scfg,
        moq_simpair_now_us(sp), &sub1);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_pub_tick(pub, moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev); }
    MOQ_TEST_CHECK_EQ_INT(cbs.joined, 1);
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track), 1);

    /* Second subscribe to same track is rejected by the client session
     * itself (duplicate track detection), before reaching the server. */
    moq_subscription_t sub2;
    MOQ_TEST_CHECK_EQ_INT(moq_session_subscribe(moq_simpair_client(sp),
        &scfg, moq_simpair_now_us(sp), &sub2), MOQ_ERR_INVAL);

    MOQ_TEST_CHECK_EQ_INT(cbs.joined, 1);
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track), 1);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("second_subscriber_rejected_v1");
}

void test_unsub_clears_slot_and_resubscribe(void) {
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

    /* Subscribe. */
    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace.parts = ns_parts;
    scfg.track_namespace.count = 1;
    scfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_subscription_t sub1;
    moq_session_subscribe(moq_simpair_client(sp), &scfg,
        moq_simpair_now_us(sp), &sub1);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_pub_tick(pub, moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev); }
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track), 1);

    /* Unsubscribe — slot must clear to active_count 0. */
    moq_session_unsubscribe(moq_simpair_client(sp), sub1,
        moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_pub_tick(pub, moq_simpair_now_us(sp));
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track), 0);
    MOQ_TEST_CHECK_EQ_INT(cbs.left, 1);

    /* Resubscribe — slot is reused, active_count goes back to 1. */
    moq_subscription_t sub2;
    moq_session_subscribe(moq_simpair_client(sp), &scfg,
        moq_simpair_now_us(sp), &sub2);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_pub_tick(pub, moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev); }
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track), 1);
    MOQ_TEST_CHECK_EQ_INT(cbs.joined, 2);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("unsub_clears_slot_and_resubscribe");
}

void test_session_rejects_same_track_duplicate(void) {
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

    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace.parts = ns_parts;
    scfg.track_namespace.count = 1;
    scfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_subscription_t sub1;
    moq_session_subscribe(moq_simpair_client(sp), &scfg,
        moq_simpair_now_us(sp), &sub1);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_pub_tick(pub, moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev); }
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track), 1);

    /* Second raw subscribe for the same track: session core rejects
     * with duplicate-track detection before the publisher facade sees
     * a SUBSCRIBE_REQUEST event. Same-session same-track fan-out is
     * blocked at the session layer, not the facade. */
    feed_subscribe(moq_simpair_server(sp), 2, "live", "video", NULL, 0);

    size_t ev_count = 0;
    { moq_event_t ev;
      while (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
          if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) ev_count++;
          moq_event_cleanup(&ev);
      } }
    MOQ_TEST_CHECK_EQ_SIZE(ev_count, 0);
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track), 1);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("session_rejects_same_track_duplicate");
}

void test_unsub_event_clears_slot(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    tick_cb_state_t cbs = {0};
    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    cfg.callbacks.ctx = &cbs;
    cfg.callbacks.on_subscriber_joined = tick_on_joined;

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
    moq_subscription_t sub1;
    moq_session_subscribe(moq_simpair_client(sp), &scfg,
        moq_simpair_now_us(sp), &sub1);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_pub_tick(pub, moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev); }
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track), 1);

    /* Write first object to open a subgroup. */
    moq_rcbuf_t *buf = NULL;
    moq_rcbuf_create(&alloc, (const uint8_t *)"a", 1, &buf);
    MOQ_TEST_CHECK_EQ_INT(moq_pub_write_object(pub, track, 0, 0, buf,
        moq_simpair_now_us(sp)), MOQ_OK);
    moq_rcbuf_decref(buf);

    /* Unsubscribe on client side — server slot becomes stale. */
    moq_session_unsubscribe(moq_simpair_client(sp), sub1,
        moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_pub_tick(pub, moq_simpair_now_us(sp));

    /* Slot is cleared by the UNSUBSCRIBED event handler. */
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track), 0);

    /* Write after slot cleared is a no-op. */
    moq_rcbuf_create(&alloc, (const uint8_t *)"b", 1, &buf);
    MOQ_TEST_CHECK_EQ_INT(moq_pub_write_object(pub, track, 1, 0, buf,
        moq_simpair_now_us(sp)), MOQ_OK);
    moq_rcbuf_decref(buf);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("unsub_event_clears_slot");
}
