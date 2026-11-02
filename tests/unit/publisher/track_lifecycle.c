#include "test_cases.h"


void test_strict_alloc_sizes(void) {
    /* Use byte-counting allocator that tracks live_bytes. If any free
     * passes size=0 for a nonzero allocation, live_bytes will drift. */
    byte_alloc_state_t bas = {0};
    moq_alloc_t alloc = { &bas, byte_alloc, byte_realloc, byte_free };

    moq_simpair_cfg_t scfg = MOQ_SIMPAIR_CFG_INIT;
    scfg.alloc = &alloc;
    scfg.seed = 99;
    scfg.initial_now_us = 1000;
    scfg.client_send_request_capacity = true;
    scfg.client_initial_request_capacity = 16;
    scfg.server_send_request_capacity = true;
    scfg.server_initial_request_capacity = 16;
    moq_simpair_t *sp = NULL;
    moq_simpair_create(&scfg, &sp);
    moq_simpair_start(sp);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev);
      if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev);
    }

    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    moq_publisher_t *pub = NULL;
    moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub);

    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live"),
                                MOQ_BYTES_LITERAL("stream") };
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 2;
    tcfg.track_name = MOQ_BYTES_LITERAL("video-track");
    tcfg.advertise_namespace = true;

    moq_pub_track_t *track = NULL;
    moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &track);

    moq_pub_remove_track(pub, track, moq_simpair_now_us(sp));
    moq_pub_destroy(pub);

    drain_all(sp);
    moq_simpair_destroy(sp);

    MOQ_TEST_CHECK(bas.balance == 0);
    MOQ_TEST_CHECK(bas.live_bytes == 0);
    MOQ_TEST_PASS("strict_alloc_sizes");
}

void test_reject_all_mode(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_REJECT_ALL;

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

    /* Subscribe → should be rejected by facade. */
    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace.parts = ns_parts;
    scfg.track_namespace.count = 1;
    scfg.track_name = MOQ_BYTES_LITERAL("video");

    moq_subscription_t sub_h;
    moq_session_subscribe(moq_simpair_client(sp), &scfg,
        moq_simpair_now_us(sp), &sub_h);
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    moq_event_t ev;
    if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
        moq_pub_event_result_t res;
        moq_result_t rc = moq_pub_handle_event(pub, &ev,
            moq_simpair_now_us(sp), &res);
        MOQ_TEST_CHECK(rc == MOQ_OK);
        MOQ_TEST_CHECK(res == MOQ_PUB_EVENT_CONSUMED);
        moq_event_cleanup(&ev);
    }

    MOQ_TEST_CHECK(moq_pub_active_subscriptions(pub, track) == 0);

    /* Pump rejection to client. */
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1) {
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_ERROR);
        moq_event_cleanup(&ev);
    }

    moq_pub_remove_track(pub, track, moq_simpair_now_us(sp));
    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("reject_all_mode");
}

void test_duplicate_track_rejected(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    moq_publisher_t *pub = NULL;
    moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub);

    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");

    moq_pub_track_t *t1 = NULL;
    MOQ_TEST_CHECK(moq_pub_add_track(pub, &tcfg, 0, &t1) == MOQ_OK);
    MOQ_TEST_CHECK(t1 != NULL);

    moq_pub_track_t *t2 = NULL;
    MOQ_TEST_CHECK(moq_pub_add_track(pub, &tcfg, 0, &t2) == MOQ_ERR_INVAL);
    MOQ_TEST_CHECK(t2 == NULL);

    moq_pub_remove_track(pub, t1, 0);
    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("duplicate_track_rejected");
}

void test_reset_group(void) {
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

    /* NULL / foreign-track / pre-subscribe (no open group) cases. */
    MOQ_TEST_CHECK(moq_pub_reset_group(NULL, track, 0,
        moq_simpair_now_us(sp)) == MOQ_ERR_INVAL);
    MOQ_TEST_CHECK(moq_pub_reset_group(pub, NULL, 0,
        moq_simpair_now_us(sp)) == MOQ_ERR_INVAL);
    /* No subscriber / no open group: no-op OK. */
    MOQ_TEST_CHECK(moq_pub_reset_group(pub, track, 0,
        moq_simpair_now_us(sp)) == MOQ_OK);
    bool did_reset = true;
    MOQ_TEST_CHECK(moq_core_internal_pub_reset_group(NULL, track, 0,
        moq_simpair_now_us(sp), &did_reset) == MOQ_ERR_INVAL);
    MOQ_TEST_CHECK(!did_reset);
    did_reset = true;
    MOQ_TEST_CHECK(moq_core_internal_pub_reset_group(pub, NULL, 0,
        moq_simpair_now_us(sp), &did_reset) == MOQ_ERR_INVAL);
    MOQ_TEST_CHECK(!did_reset);
    did_reset = true;
    MOQ_TEST_CHECK(moq_core_internal_pub_reset_group(pub, track, 0,
        moq_simpair_now_us(sp), &did_reset) == MOQ_OK);
    MOQ_TEST_CHECK(!did_reset);   /* no subscriber/open subgroup */

    /* Subscribe + accept. */
    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace.parts = ns_parts;
    scfg.track_namespace.count = 1;
    scfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_subscription_t sub_h;
    moq_session_subscribe(moq_simpair_client(sp), &scfg,
        moq_simpair_now_us(sp), &sub_h);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
          moq_pub_event_result_t res;
          moq_pub_handle_event(pub, &ev, moq_simpair_now_us(sp), &res);
          moq_event_cleanup(&ev);
      } }
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev); }

    /* Open group 0 with one object, pump it out. */
    uint8_t d0[] = { 1, 2, 3, 4 };
    moq_rcbuf_t *p0 = NULL;
    moq_rcbuf_create(&alloc, d0, sizeof(d0), &p0);
    MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 0, 0, p0,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_rcbuf_decref(p0);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t evts[8];
      size_t ne = moq_session_poll_events(moq_simpair_client(sp), evts, 8);
      for (size_t i = 0; i < ne; i++) moq_event_cleanup(&evts[i]); }

    /* Abandon the open group: the publisher must emit a RESET_DATA action
     * (RESET_STREAM on the wire), not a clean close. */
    /* A missing private output is rejected without abandoning the group;
     * the existing public call below must still perform its reset. */
    MOQ_TEST_CHECK(moq_core_internal_pub_reset_group(pub, track, 0x10,
        moq_simpair_now_us(sp), NULL) == MOQ_ERR_INVAL);
    did_reset = true;
    MOQ_TEST_CHECK(moq_core_internal_pub_reset_group(pub, track,
        MOQ_QUIC_VARINT_MAX + 1, moq_simpair_now_us(sp), &did_reset)
        == MOQ_ERR_INVAL);
    MOQ_TEST_CHECK(!did_reset);
    MOQ_TEST_CHECK(moq_pub_reset_group(pub, track, 0x10,
        moq_simpair_now_us(sp)) == MOQ_OK);
    bool saw_reset = false;
    { moq_action_t acts[8];
      size_t na = moq_session_poll_actions(moq_simpair_server(sp), acts, 8);
      for (size_t i = 0; i < na; i++) {
          if (acts[i].kind == MOQ_ACTION_RESET_DATA) saw_reset = true;
          moq_action_cleanup(&acts[i]);
      } }
    MOQ_TEST_CHECK(saw_reset);

    /* The track stays writable: a fresh group opens a new subgroup. */
    uint8_t d1[] = { 5, 6, 7, 8 };
    moq_rcbuf_t *p1 = NULL;
    moq_rcbuf_create(&alloc, d1, sizeof(d1), &p1);
    MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 1, 0, p1,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_rcbuf_decref(p1);
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    /* Same implementation via the private entry: true for an actual reset,
     * false on the next call, rather than a cumulative publisher count. */
    did_reset = false;
    MOQ_TEST_CHECK(moq_core_internal_pub_reset_group(pub, track, 0x10,
        moq_simpair_now_us(sp), &did_reset) == MOQ_OK);
    MOQ_TEST_CHECK(did_reset);
    did_reset = true;
    MOQ_TEST_CHECK(moq_core_internal_pub_reset_group(pub, track, 0x10,
        moq_simpair_now_us(sp), &did_reset) == MOQ_OK);
    MOQ_TEST_CHECK(!did_reset);

    /* A second reset with nothing open is a no-op again. */
    MOQ_TEST_CHECK(moq_pub_reset_group(pub, track, 0,
        moq_simpair_now_us(sp)) == MOQ_OK);

    moq_simpair_run_until_quiescent(sp, 8, NULL);
    /* Retire a subgroup through the public session API before the facade
     * observes it. The facade then encounters STALE_HANDLE: not a new reset. */
    moq_rcbuf_t *p2 = NULL;
    moq_rcbuf_create(&alloc, d1, sizeof(d1), &p2);
    MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 2, 0, p2,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_rcbuf_decref(p2);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_session_t *session = moq_simpair_server(sp);
    moq_subgroup_handle_t subgroup = MOQ_SUBGROUP_INVALID;
    size_t open = 0;
    for (size_t i = 0; i < session->sg_cap; i++) {
        const moq_sg_entry_t *sg = &session->subgroups[i];
        if (sg->state != MOQ_SG_OPEN || sg->group_id != 2) continue;
        subgroup._opaque = moq_handle_pack(MOQ_HANDLE_POOL_SUBGROUP,
            session->session_tag, sg->generation, (uint32_t)i);
        open++;
    }
    MOQ_TEST_CHECK_EQ_U64(open, 1);
    MOQ_TEST_CHECK(moq_session_reset_subgroup(session, subgroup, 0x10,
        moq_simpair_now_us(sp)) == MOQ_OK);
    did_reset = true;
    MOQ_TEST_CHECK(moq_core_internal_pub_reset_group(pub, track, 0x10,
        moq_simpair_now_us(sp), &did_reset) == MOQ_OK);
    MOQ_TEST_CHECK(!did_reset);
    size_t resets = 0;
    { moq_action_t acts[8];
      size_t na = moq_session_poll_actions(session, acts, 8);
      for (size_t i = 0; i < na; i++) {
          if (acts[i].kind == MOQ_ACTION_RESET_DATA) resets++;
          moq_action_cleanup(&acts[i]);
      } }
    MOQ_TEST_CHECK_EQ_U64(resets, 1);   /* only the direct session reset */

    moq_pub_remove_track(pub, track, moq_simpair_now_us(sp));
    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("reset_group");
}

void test_flush_no_pending(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    moq_publisher_t *pub = NULL;
    moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub);

    /* Flush with nothing pending → OK. */
    moq_result_t rc = moq_pub_flush(pub, moq_simpair_now_us(sp));
    MOQ_TEST_CHECK(rc == MOQ_OK);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("flush_no_pending");
}

void test_flush_accept_would_block(void) {
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);

    /* Server with 1-slot action queue. */
    moq_session_cfg_t ccfg = MOQ_SESSION_CFG_INIT;
    ccfg.alloc = &alloc;
    ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    ccfg.send_request_capacity = true;
    ccfg.initial_request_capacity = 16;

    moq_session_cfg_t scfg = MOQ_SESSION_CFG_INIT;
    scfg.alloc = &alloc;
    scfg.perspective = MOQ_PERSPECTIVE_SERVER;
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 16;
    scfg.max_actions = 1;

    moq_session_t *client = NULL, *server = NULL;
    moq_session_create(&ccfg, 0, &client);
    moq_session_create(&scfg, 0, &server);
    moq_session_start(client, 0);
    pump_actions_to_peer(client, server, 0);
    pump_actions_to_peer(server, client, 0);
    { moq_event_t ev;
      if (moq_session_poll_events(client, &ev, 1) == 1)
          moq_event_cleanup(&ev);
      if (moq_session_poll_events(server, &ev, 1) == 1)
          moq_event_cleanup(&ev);
    }

    moq_pub_cfg_t pcfg;
    moq_pub_cfg_init_sized(&pcfg, sizeof(pcfg));
    pcfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    moq_publisher_t *pub = NULL;
    moq_pub_create(server, &alloc, &pcfg, &pub);

    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("t");
    moq_pub_track_t *track = NULL;
    moq_pub_add_track(pub, &tcfg, 0, &track);

    /* Fill the 1-slot queue with publish_namespace BEFORE the
     * subscribe arrives. This is an advancing call but happens
     * before we poll any subscribe events. */
    moq_publish_namespace_cfg_t nscfg;
    moq_publish_namespace_cfg_init(&nscfg);
    moq_bytes_t blocker_ns[] = { MOQ_BYTES_LITERAL("blk") };
    nscfg.track_namespace.parts = blocker_ns;
    nscfg.track_namespace.count = 1;
    moq_announcement_t blocker_ann;
    moq_session_publish_namespace(server, &nscfg, 0, &blocker_ann);

    /* Now subscribe. on_control_bytes is non-advancing so the
     * subscribe event is queued without touching the action slot. */
    moq_subscribe_cfg_t subcfg;
    moq_subscribe_cfg_init(&subcfg);
    subcfg.track_namespace.parts = ns_parts;
    subcfg.track_namespace.count = 1;
    subcfg.track_name = MOQ_BYTES_LITERAL("t");
    moq_subscription_t sub_h;
    moq_session_subscribe(client, &subcfg, 0, &sub_h);
    { moq_action_t acts[4]; size_t na;
      while ((na = moq_session_poll_actions(client, acts, 4)) > 0)
          for (size_t i = 0; i < na; i++) {
              if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                  moq_session_on_control_bytes(server,
                      acts[i].u.send_control.data,
                      acts[i].u.send_control.len, 0);
              moq_action_cleanup(&acts[i]);
          }
    }

    /* Poll the subscribe event — this is safe because on_control_bytes
     * is non-advancing. The action queue is still full. */
    moq_event_t ev;
    MOQ_TEST_CHECK(moq_session_poll_events(server, &ev, 1) == 1);
    MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);

    /* Forward to facade — accept should WOULD_BLOCK. */
    moq_pub_event_result_t res;
    moq_result_t rc = moq_pub_handle_event(pub, &ev, 0, &res);
    MOQ_TEST_CHECK(rc == MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(res == MOQ_PUB_EVENT_CONSUMED);
    MOQ_TEST_CHECK(moq_pub_active_subscriptions(pub, track) == 0);
    moq_event_cleanup(&ev);

    /* Drain the blocking action. */
    { moq_action_t acts[4]; size_t na;
      while ((na = moq_session_poll_actions(server, acts, 4)) > 0)
          for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
    }

    /* Flush should complete the accept. */
    rc = moq_pub_flush(pub, 0);
    MOQ_TEST_CHECK(rc == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_active_subscriptions(pub, track) == 1);

    /* Pump SUBSCRIBE_OK to client. */
    pump_actions_to_peer(server, client, 0);
    { moq_event_t cev;
      if (moq_session_poll_events(client, &cev, 1) == 1)
          moq_event_cleanup(&cev);
    }

    moq_pub_remove_track(pub, track, 0);
    moq_pub_destroy(pub);

    { moq_event_t drain[16]; size_t ne;
      while ((ne = moq_session_poll_events(server, drain, 16)) > 0)
          for (size_t i = 0; i < ne; i++) moq_event_cleanup(&drain[i]);
      while ((ne = moq_session_poll_events(client, drain, 16)) > 0)
          for (size_t i = 0; i < ne; i++) moq_event_cleanup(&drain[i]);
      moq_action_t acts[16]; size_t na;
      while ((na = moq_session_poll_actions(server, acts, 16)) > 0)
          for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
      while ((na = moq_session_poll_actions(client, acts, 16)) > 0)
          for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
    }
    moq_session_destroy(server);
    moq_session_destroy(client);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("flush_accept_would_block");
}

void test_flush_reject_would_block(void) {
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);

    moq_session_cfg_t ccfg = MOQ_SESSION_CFG_INIT;
    ccfg.alloc = &alloc;
    ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    ccfg.send_request_capacity = true;
    ccfg.initial_request_capacity = 16;

    moq_session_cfg_t scfg = MOQ_SESSION_CFG_INIT;
    scfg.alloc = &alloc;
    scfg.perspective = MOQ_PERSPECTIVE_SERVER;
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 16;
    scfg.max_actions = 1;

    moq_session_t *client = NULL, *server = NULL;
    moq_session_create(&ccfg, 0, &client);
    moq_session_create(&scfg, 0, &server);
    moq_session_start(client, 0);
    pump_actions_to_peer(client, server, 0);
    pump_actions_to_peer(server, client, 0);
    { moq_event_t ev;
      if (moq_session_poll_events(client, &ev, 1) == 1)
          moq_event_cleanup(&ev);
      if (moq_session_poll_events(server, &ev, 1) == 1)
          moq_event_cleanup(&ev);
    }

    /* REJECT_ALL facade. */
    moq_pub_cfg_t pcfg;
    moq_pub_cfg_init_sized(&pcfg, sizeof(pcfg));
    pcfg.accept_mode = MOQ_PUB_REJECT_ALL;
    moq_publisher_t *pub = NULL;
    moq_pub_create(server, &alloc, &pcfg, &pub);

    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("t");
    moq_pub_track_t *track = NULL;
    moq_pub_add_track(pub, &tcfg, 0, &track);

    /* Fill 1-slot queue before subscribe arrives. */
    moq_publish_namespace_cfg_t nscfg;
    moq_publish_namespace_cfg_init(&nscfg);
    moq_bytes_t blocker_ns[] = { MOQ_BYTES_LITERAL("blk") };
    nscfg.track_namespace.parts = blocker_ns;
    nscfg.track_namespace.count = 1;
    moq_announcement_t blocker_ann;
    moq_session_publish_namespace(server, &nscfg, 0, &blocker_ann);

    /* Subscribe via non-advancing on_control_bytes. */
    moq_subscribe_cfg_t subcfg;
    moq_subscribe_cfg_init(&subcfg);
    subcfg.track_namespace.parts = ns_parts;
    subcfg.track_namespace.count = 1;
    subcfg.track_name = MOQ_BYTES_LITERAL("t");
    moq_subscription_t sub_h;
    moq_session_subscribe(client, &subcfg, 0, &sub_h);
    { moq_action_t acts[4]; size_t na;
      while ((na = moq_session_poll_actions(client, acts, 4)) > 0)
          for (size_t i = 0; i < na; i++) {
              if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                  moq_session_on_control_bytes(server,
                      acts[i].u.send_control.data,
                      acts[i].u.send_control.len, 0);
              moq_action_cleanup(&acts[i]);
          }
    }

    moq_event_t ev;
    MOQ_TEST_CHECK(moq_session_poll_events(server, &ev, 1) == 1);
    MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);

    /* Reject should WOULD_BLOCK (queue full). */
    moq_pub_event_result_t res;
    moq_result_t rc = moq_pub_handle_event(pub, &ev, 0, &res);
    MOQ_TEST_CHECK(rc == MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(res == MOQ_PUB_EVENT_CONSUMED);
    MOQ_TEST_CHECK(moq_pub_active_subscriptions(pub, track) == 0);
    moq_event_cleanup(&ev);

    /* Drain blocking action. */
    { moq_action_t acts[4]; size_t na;
      while ((na = moq_session_poll_actions(server, acts, 4)) > 0)
          for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
    }

    /* Flush completes the reject. */
    rc = moq_pub_flush(pub, 0);
    MOQ_TEST_CHECK(rc == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_active_subscriptions(pub, track) == 0);

    /* Pump REQUEST_ERROR to client. */
    pump_actions_to_peer(server, client, 0);
    { moq_event_t cev;
      if (moq_session_poll_events(client, &cev, 1) == 1) {
          MOQ_TEST_CHECK(cev.kind == MOQ_EVENT_SUBSCRIBE_ERROR);
          moq_event_cleanup(&cev);
      }
    }

    moq_pub_remove_track(pub, track, 0);
    moq_pub_destroy(pub);

    { moq_event_t drain[16]; size_t ne;
      while ((ne = moq_session_poll_events(server, drain, 16)) > 0)
          for (size_t i = 0; i < ne; i++) moq_event_cleanup(&drain[i]);
      while ((ne = moq_session_poll_events(client, drain, 16)) > 0)
          for (size_t i = 0; i < ne; i++) moq_event_cleanup(&drain[i]);
      moq_action_t acts[16]; size_t na;
      while ((na = moq_session_poll_actions(server, acts, 16)) > 0)
          for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
      while ((na = moq_session_poll_actions(client, acts, 16)) > 0)
          for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
    }
    moq_session_destroy(server);
    moq_session_destroy(client);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("flush_reject_would_block");
}

void test_remove_track_pending_accept(void) {
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);

    moq_session_cfg_t ccfg = MOQ_SESSION_CFG_INIT;
    ccfg.alloc = &alloc;
    ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    ccfg.send_request_capacity = true;
    ccfg.initial_request_capacity = 16;

    moq_session_cfg_t scfg = MOQ_SESSION_CFG_INIT;
    scfg.alloc = &alloc;
    scfg.perspective = MOQ_PERSPECTIVE_SERVER;
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 16;
    scfg.max_actions = 1;

    moq_session_t *client = NULL, *server = NULL;
    moq_session_create(&ccfg, 0, &client);
    moq_session_create(&scfg, 0, &server);
    moq_session_start(client, 0);
    pump_actions_to_peer(client, server, 0);
    pump_actions_to_peer(server, client, 0);
    { moq_event_t ev;
      if (moq_session_poll_events(client, &ev, 1) == 1)
          moq_event_cleanup(&ev);
      if (moq_session_poll_events(server, &ev, 1) == 1)
          moq_event_cleanup(&ev);
    }

    moq_pub_cfg_t pcfg;
    moq_pub_cfg_init_sized(&pcfg, sizeof(pcfg));
    pcfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    moq_publisher_t *pub = NULL;
    moq_pub_create(server, &alloc, &pcfg, &pub);

    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("t");
    moq_pub_track_t *track = NULL;
    moq_pub_add_track(pub, &tcfg, 0, &track);

    /* Fill 1-slot queue before subscribe arrives. */
    moq_publish_namespace_cfg_t nscfg;
    moq_publish_namespace_cfg_init(&nscfg);
    moq_bytes_t blocker_ns[] = { MOQ_BYTES_LITERAL("blk") };
    nscfg.track_namespace.parts = blocker_ns;
    nscfg.track_namespace.count = 1;
    moq_announcement_t blocker_ann;
    moq_session_publish_namespace(server, &nscfg, 0, &blocker_ann);

    /* Subscribe arrives via non-advancing on_control_bytes. */
    moq_subscribe_cfg_t subcfg;
    moq_subscribe_cfg_init(&subcfg);
    subcfg.track_namespace.parts = ns_parts;
    subcfg.track_namespace.count = 1;
    subcfg.track_name = MOQ_BYTES_LITERAL("t");
    moq_subscription_t sub_h;
    moq_session_subscribe(client, &subcfg, 0, &sub_h);
    { moq_action_t acts[4]; size_t na;
      while ((na = moq_session_poll_actions(client, acts, 4)) > 0)
          for (size_t i = 0; i < na; i++) {
              if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                  moq_session_on_control_bytes(server,
                      acts[i].u.send_control.data,
                      acts[i].u.send_control.len, 0);
              moq_action_cleanup(&acts[i]);
          }
    }

    moq_event_t ev;
    MOQ_TEST_CHECK(moq_session_poll_events(server, &ev, 1) == 1);

    moq_pub_event_result_t res;
    moq_result_t rc = moq_pub_handle_event(pub, &ev, 0, &res);
    MOQ_TEST_CHECK(rc == MOQ_ERR_WOULD_BLOCK);

    /* remove_track blocked by pending accept. */
    MOQ_TEST_CHECK(moq_pub_remove_track(pub, track, 0)
        == MOQ_ERR_WOULD_BLOCK);

    /* Drain and flush. */
    { moq_action_t acts[4]; size_t na;
      while ((na = moq_session_poll_actions(server, acts, 4)) > 0)
          for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
    }
    rc = moq_pub_flush(pub, 0);
    MOQ_TEST_CHECK(rc == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_active_subscriptions(pub, track) == 1);

    /* Drain the queued SUBSCRIBE_OK so remove_track has room to queue the
     * subscription's PUBLISH_DONE. */
    { moq_action_t acts[4]; size_t na;
      while ((na = moq_session_poll_actions(server, acts, 4)) > 0)
          for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
    }

    /* Now remove_track should succeed. */
    rc = moq_pub_remove_track(pub, track, 0);
    MOQ_TEST_CHECK(rc == MOQ_OK);

    moq_event_cleanup(&ev);
    moq_pub_destroy(pub);

    { moq_event_t drain[16]; size_t ne;
      while ((ne = moq_session_poll_events(server, drain, 16)) > 0)
          for (size_t i = 0; i < ne; i++) moq_event_cleanup(&drain[i]);
      while ((ne = moq_session_poll_events(client, drain, 16)) > 0)
          for (size_t i = 0; i < ne; i++) moq_event_cleanup(&drain[i]);
      moq_action_t acts[16]; size_t na;
      while ((na = moq_session_poll_actions(server, acts, 16)) > 0)
          for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
      while ((na = moq_session_poll_actions(client, acts, 16)) > 0)
          for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
    }
    moq_session_destroy(server);
    moq_session_destroy(client);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("remove_track_pending_accept");
}

void test_create_invalid_cfg(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    moq_publisher_t *pub = NULL;

    /* Too-small struct_size. */
    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.struct_size = 1;
    MOQ_TEST_CHECK(moq_pub_create(moq_simpair_server(sp), &alloc,
        &cfg, &pub) == MOQ_ERR_INVAL);
    MOQ_TEST_CHECK(pub == NULL);

    /* Invalid accept_mode. */
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = (moq_pub_accept_mode_t)99;
    MOQ_TEST_CHECK(moq_pub_create(moq_simpair_server(sp), &alloc,
        &cfg, &pub) == MOQ_ERR_INVAL);
    MOQ_TEST_CHECK(pub == NULL);

    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("create_invalid_cfg");
}

void test_cross_publisher_track(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;

    moq_publisher_t *pub1 = NULL;
    moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub1);
    moq_publisher_t *pub2 = NULL;
    moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub2);

    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("t");

    moq_pub_track_t *track1 = NULL;
    moq_pub_add_track(pub1, &tcfg, 0, &track1);

    /* Use track from pub1 with pub2. */
    MOQ_TEST_CHECK(moq_pub_remove_track(pub2, track1, 0) == MOQ_ERR_INVAL);
    MOQ_TEST_CHECK(moq_pub_write_object(pub2, track1, 0, 0, NULL, 0) == MOQ_ERR_INVAL);
    MOQ_TEST_CHECK(moq_pub_end_group(pub2, track1, 0) == MOQ_ERR_INVAL);
    MOQ_TEST_CHECK(moq_pub_active_subscriptions(pub2, track1) == 0);

    moq_pub_remove_track(pub1, track1, 0);
    moq_pub_destroy(pub1);
    moq_pub_destroy(pub2);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("cross_publisher_track");
}

void test_pending_does_not_block_unmatched(void) {
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);

    /* Server with 1-slot action queue to force WOULD_BLOCK. */
    moq_session_cfg_t ccfg = MOQ_SESSION_CFG_INIT;
    ccfg.alloc = &alloc;
    ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    ccfg.send_request_capacity = true;
    ccfg.initial_request_capacity = 16;

    moq_session_cfg_t scfg = MOQ_SESSION_CFG_INIT;
    scfg.alloc = &alloc;
    scfg.perspective = MOQ_PERSPECTIVE_SERVER;
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 16;
    scfg.max_actions = 1;

    moq_session_t *client = NULL, *server = NULL;
    moq_session_create(&ccfg, 0, &client);
    moq_session_create(&scfg, 0, &server);
    moq_session_start(client, 0);
    pump_actions_to_peer(client, server, 0);
    pump_actions_to_peer(server, client, 0);
    { moq_event_t ev;
      if (moq_session_poll_events(client, &ev, 1) == 1)
          moq_event_cleanup(&ev);
      if (moq_session_poll_events(server, &ev, 1) == 1)
          moq_event_cleanup(&ev);
    }

    moq_pub_cfg_t pcfg;
    moq_pub_cfg_init_sized(&pcfg, sizeof(pcfg));
    pcfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    moq_publisher_t *pub = NULL;
    moq_pub_create(server, &alloc, &pcfg, &pub);

    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("known");
    moq_pub_track_t *track = NULL;
    moq_pub_add_track(pub, &tcfg, 0, &track);

    /* Fill queue, subscribe to known track → WOULD_BLOCK → pending. */
    moq_publish_namespace_cfg_t nscfg;
    moq_publish_namespace_cfg_init(&nscfg);
    moq_bytes_t blocker_ns[] = { MOQ_BYTES_LITERAL("blk") };
    nscfg.track_namespace.parts = blocker_ns;
    nscfg.track_namespace.count = 1;
    moq_announcement_t blocker_ann;
    moq_session_publish_namespace(server, &nscfg, 0, &blocker_ann);

    moq_subscribe_cfg_t sub1cfg;
    moq_subscribe_cfg_init(&sub1cfg);
    sub1cfg.track_namespace.parts = ns_parts;
    sub1cfg.track_namespace.count = 1;
    sub1cfg.track_name = MOQ_BYTES_LITERAL("known");
    moq_subscription_t sub1;
    moq_session_subscribe(client, &sub1cfg, 0, &sub1);
    { moq_action_t acts[4]; size_t na;
      while ((na = moq_session_poll_actions(client, acts, 4)) > 0)
          for (size_t i = 0; i < na; i++) {
              if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                  moq_session_on_control_bytes(server,
                      acts[i].u.send_control.data,
                      acts[i].u.send_control.len, 0);
              moq_action_cleanup(&acts[i]);
          }
    }

    moq_event_t ev;
    MOQ_TEST_CHECK(moq_session_poll_events(server, &ev, 1) == 1);
    moq_pub_event_result_t res;
    moq_result_t rc = moq_pub_handle_event(pub, &ev, 0, &res);
    MOQ_TEST_CHECK(rc == MOQ_ERR_WOULD_BLOCK);
    moq_event_cleanup(&ev);

    /* Now construct a fake SUBSCRIBE_REQUEST for an unknown track.
     * With pending active, this should still return IGNORED. */
    moq_event_t fake;
    memset(&fake, 0, sizeof(fake));
    fake.kind = MOQ_EVENT_SUBSCRIBE_REQUEST;
    moq_bytes_t unk_ns[] = { MOQ_BYTES_LITERAL("ns") };
    fake.u.subscribe_request.track_namespace.parts = unk_ns;
    fake.u.subscribe_request.track_namespace.count = 1;
    fake.u.subscribe_request.track_name = MOQ_BYTES_LITERAL("unknown");

    rc = moq_pub_handle_event(pub, &fake, 0, &res);
    MOQ_TEST_CHECK(rc == MOQ_OK);
    MOQ_TEST_CHECK(res == MOQ_PUB_EVENT_IGNORED);

    /* Drain and flush to clean up. */
    { moq_action_t acts[4]; size_t na;
      while ((na = moq_session_poll_actions(server, acts, 4)) > 0)
          for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
    }
    moq_pub_flush(pub, 0);
    moq_pub_remove_track(pub, track, 0);
    moq_pub_destroy(pub);

    { moq_event_t drain[16]; size_t ne;
      while ((ne = moq_session_poll_events(server, drain, 16)) > 0)
          for (size_t i = 0; i < ne; i++) moq_event_cleanup(&drain[i]);
      while ((ne = moq_session_poll_events(client, drain, 16)) > 0)
          for (size_t i = 0; i < ne; i++) moq_event_cleanup(&drain[i]);
      moq_action_t acts[16]; size_t na;
      while ((na = moq_session_poll_actions(server, acts, 16)) > 0)
          for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
      while ((na = moq_session_poll_actions(client, acts, 16)) > 0)
          for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
    }
    moq_session_destroy(server);
    moq_session_destroy(client);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("pending_does_not_block_unmatched");
}

void test_end_group_stale_handle(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    moq_publisher_t *pub = NULL;
    moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub);

    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("t");
    moq_pub_track_t *track = NULL;
    moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &track);

    /* Subscribe + accept. */
    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace.parts = ns_parts;
    scfg.track_namespace.count = 1;
    scfg.track_name = MOQ_BYTES_LITERAL("t");
    moq_subscription_t sub_h;
    moq_session_subscribe(moq_simpair_client(sp), &scfg,
        moq_simpair_now_us(sp), &sub_h);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
          moq_pub_event_result_t res;
          moq_pub_handle_event(pub, &ev, moq_simpair_now_us(sp), &res);
          moq_event_cleanup(&ev);
      }
    }
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev); }

    /* Write to open subgroup, then capture the stream_ref from the
     * SEND_DATA action before SimPair routes it. */
    uint8_t d[] = { 0xAA };
    moq_rcbuf_t *p = NULL;
    moq_rcbuf_create(&alloc, d, 1, &p);
    moq_pub_write_object(pub, track, 0, 0, p, moq_simpair_now_us(sp));
    moq_rcbuf_decref(p);

    moq_stream_ref_t sender_ref = {0};
    { moq_action_t acts[8];
      size_t na = moq_session_poll_actions(moq_simpair_server(sp), acts, 8);
      for (size_t i = 0; i < na; i++) {
          if (acts[i].kind == MOQ_ACTION_SEND_DATA)
              sender_ref = acts[i].u.send_data.stream_ref;
          moq_action_cleanup(&acts[i]);
      }
    }

    /* Feed on_data_stop to the server using its own stream_ref.
     * This transitions the subgroup to RESETTING, making the
     * handle stale for close_subgroup. */
    moq_session_on_data_stop(moq_simpair_server(sp), sender_ref,
        0x0, moq_simpair_now_us(sp));

    /* Drain the RESET_DATA action that on_data_stop produces. */
    { moq_action_t acts[8]; size_t na;
      while ((na = moq_session_poll_actions(moq_simpair_server(sp), acts, 8)) > 0)
          for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
    }

    /* Ending an already stopped subgroup preserves the live destination. */
    moq_result_t rc = moq_pub_end_group(pub, track,
        moq_simpair_now_us(sp));
    MOQ_TEST_CHECK(rc == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_active_subscriptions(pub, track) == 1);

    /* A later group still opens a stream and writes its object. */
    moq_rcbuf_t *p2 = NULL;
    moq_rcbuf_create(&alloc, d, 1, &p2);
    rc = moq_pub_write_object(pub, track, 1, 0, p2,
        moq_simpair_now_us(sp));
    MOQ_TEST_CHECK(rc == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_active_subscriptions(pub, track) == 1);
    {
        moq_action_t acts[8];
        size_t na = moq_session_poll_actions(moq_simpair_server(sp), acts, 8);
        unsigned headers = 0, payloads = 0;
        for (size_t i = 0; i < na; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_DATA) {
                headers += acts[i].u.send_data.header_len != 0;
                payloads += acts[i].u.send_data.payload != NULL;
            }
            moq_action_cleanup(&acts[i]);
        }
        MOQ_TEST_CHECK(headers == 2); /* subgroup header and object header */
        MOQ_TEST_CHECK(payloads == 1);
    }
    moq_rcbuf_decref(p2);

    moq_pub_remove_track(pub, track, moq_simpair_now_us(sp));
    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("end_group_stale_handle");
}

void test_remove_track_reset_would_block(void) {
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);

    moq_session_cfg_t ccfg = MOQ_SESSION_CFG_INIT;
    ccfg.alloc = &alloc;
    ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    ccfg.send_request_capacity = true;
    ccfg.initial_request_capacity = 16;

    moq_session_cfg_t scfg = MOQ_SESSION_CFG_INIT;
    scfg.alloc = &alloc;
    scfg.perspective = MOQ_PERSPECTIVE_SERVER;
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 16;
    scfg.max_actions = 1;

    moq_session_t *client = NULL, *server = NULL;
    moq_session_create(&ccfg, 0, &client);
    moq_session_create(&scfg, 0, &server);
    moq_session_start(client, 0);
    pump_actions_to_peer(client, server, 0);
    pump_actions_to_peer(server, client, 0);
    { moq_event_t ev;
      if (moq_session_poll_events(client, &ev, 1) == 1)
          moq_event_cleanup(&ev);
      if (moq_session_poll_events(server, &ev, 1) == 1)
          moq_event_cleanup(&ev);
    }

    moq_pub_cfg_t pcfg;
    moq_pub_cfg_init_sized(&pcfg, sizeof(pcfg));
    pcfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    moq_publisher_t *pub = NULL;
    moq_pub_create(server, &alloc, &pcfg, &pub);

    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("t");
    moq_pub_track_t *track = NULL;
    moq_pub_add_track(pub, &tcfg, 0, &track);

    /* Subscribe + accept + write to open subgroup. */
    moq_subscribe_cfg_t sub1cfg;
    moq_subscribe_cfg_init(&sub1cfg);
    sub1cfg.track_namespace.parts = ns_parts;
    sub1cfg.track_namespace.count = 1;
    sub1cfg.track_name = MOQ_BYTES_LITERAL("t");
    moq_subscription_t sub1;
    moq_session_subscribe(client, &sub1cfg, 0, &sub1);
    pump_actions_to_peer(client, server, 0);
    { moq_event_t ev;
      if (moq_session_poll_events(server, &ev, 1) == 1) {
          moq_pub_event_result_t res;
          moq_pub_handle_event(pub, &ev, 0, &res);
          moq_event_cleanup(&ev);
      }
    }
    pump_actions_to_peer(server, client, 0);
    { moq_event_t ev;
      if (moq_session_poll_events(client, &ev, 1) == 1)
          moq_event_cleanup(&ev); }

    uint8_t d[] = { 0xBB };
    moq_rcbuf_t *p = NULL;
    moq_rcbuf_create(&alloc, d, 1, &p);
    moq_pub_write_object(pub, track, 0, 0, p, 0);
    moq_rcbuf_decref(p);

    /* Fill the 1-slot queue so reset_subgroup will WOULD_BLOCK. */
    moq_publish_namespace_cfg_t nscfg;
    moq_publish_namespace_cfg_init(&nscfg);
    moq_bytes_t blocker_ns[] = { MOQ_BYTES_LITERAL("blk") };
    nscfg.track_namespace.parts = blocker_ns;
    nscfg.track_namespace.count = 1;
    moq_announcement_t blocker_ann;
    moq_session_publish_namespace(server, &nscfg, 0, &blocker_ann);

    /* remove_track should WOULD_BLOCK. Track remains valid. */
    moq_result_t rc = moq_pub_remove_track(pub, track, 0);
    MOQ_TEST_CHECK(rc == MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(moq_pub_active_subscriptions(pub, track) == 1);

    /* Drain + retry until complete: remove_track now queues both the subgroup
     * RESET and the subscription's PUBLISH_DONE, which need not both fit in the
     * 1-slot queue at once -- each retry resumes mid-removal without abandoning
     * the open subgroup or re-sending. */
    for (int i = 0; i < 8; i++) {
        moq_action_t acts[4]; size_t na;
        while ((na = moq_session_poll_actions(server, acts, 4)) > 0)
            for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
        rc = moq_pub_remove_track(pub, track, 0);
        if (rc == MOQ_OK) break;
        MOQ_TEST_CHECK(rc == MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK(moq_pub_active_subscriptions(pub, track) == 1);
    }
    MOQ_TEST_CHECK(rc == MOQ_OK);

    moq_pub_destroy(pub);
    { moq_event_t drain[16]; size_t ne;
      while ((ne = moq_session_poll_events(server, drain, 16)) > 0)
          for (size_t i = 0; i < ne; i++) moq_event_cleanup(&drain[i]);
      while ((ne = moq_session_poll_events(client, drain, 16)) > 0)
          for (size_t i = 0; i < ne; i++) moq_event_cleanup(&drain[i]);
      moq_action_t acts[16]; size_t na;
      while ((na = moq_session_poll_actions(server, acts, 16)) > 0)
          for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
      while ((na = moq_session_poll_actions(client, acts, 16)) > 0)
          for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
    }
    moq_session_destroy(server);
    moq_session_destroy(client);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("remove_track_reset_would_block");
}

void test_destroy_with_active_pending(void) {
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);

    moq_session_cfg_t ccfg = MOQ_SESSION_CFG_INIT;
    ccfg.alloc = &alloc;
    ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    ccfg.send_request_capacity = true;
    ccfg.initial_request_capacity = 16;

    moq_session_cfg_t scfg = MOQ_SESSION_CFG_INIT;
    scfg.alloc = &alloc;
    scfg.perspective = MOQ_PERSPECTIVE_SERVER;
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 16;
    scfg.max_actions = 1;

    moq_session_t *client = NULL, *server = NULL;
    moq_session_create(&ccfg, 0, &client);
    moq_session_create(&scfg, 0, &server);
    moq_session_start(client, 0);
    pump_actions_to_peer(client, server, 0);
    pump_actions_to_peer(server, client, 0);
    { moq_event_t ev;
      if (moq_session_poll_events(client, &ev, 1) == 1)
          moq_event_cleanup(&ev);
      if (moq_session_poll_events(server, &ev, 1) == 1)
          moq_event_cleanup(&ev);
    }

    moq_pub_cfg_t pcfg;
    moq_pub_cfg_init_sized(&pcfg, sizeof(pcfg));
    pcfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    moq_publisher_t *pub = NULL;
    moq_pub_create(server, &alloc, &pcfg, &pub);

    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("t");
    moq_pub_track_t *track = NULL;
    moq_pub_add_track(pub, &tcfg, 0, &track);

    /* Fill queue, force pending. */
    moq_publish_namespace_cfg_t nscfg;
    moq_publish_namespace_cfg_init(&nscfg);
    moq_bytes_t blocker_ns[] = { MOQ_BYTES_LITERAL("blk") };
    nscfg.track_namespace.parts = blocker_ns;
    nscfg.track_namespace.count = 1;
    moq_announcement_t blocker_ann;
    moq_session_publish_namespace(server, &nscfg, 0, &blocker_ann);

    moq_subscribe_cfg_t sub1cfg;
    moq_subscribe_cfg_init(&sub1cfg);
    sub1cfg.track_namespace.parts = ns_parts;
    sub1cfg.track_namespace.count = 1;
    sub1cfg.track_name = MOQ_BYTES_LITERAL("t");
    moq_subscription_t sub1;
    moq_session_subscribe(client, &sub1cfg, 0, &sub1);
    { moq_action_t acts[4]; size_t na;
      while ((na = moq_session_poll_actions(client, acts, 4)) > 0)
          for (size_t i = 0; i < na; i++) {
              if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                  moq_session_on_control_bytes(server,
                      acts[i].u.send_control.data,
                      acts[i].u.send_control.len, 0);
              moq_action_cleanup(&acts[i]);
          }
    }

    moq_event_t ev;
    if (moq_session_poll_events(server, &ev, 1) == 1) {
        moq_pub_event_result_t res;
        moq_result_t rc = moq_pub_handle_event(pub, &ev, 0, &res);
        MOQ_TEST_CHECK(rc == MOQ_ERR_WOULD_BLOCK);
        moq_event_cleanup(&ev);
    }

    /* Destroy without flushing. Pending accept is abandoned.
     * This is local cleanup only — no flush after destroy. */
    moq_pub_destroy(pub);

    { moq_event_t drain[16]; size_t ne;
      while ((ne = moq_session_poll_events(server, drain, 16)) > 0)
          for (size_t i = 0; i < ne; i++) moq_event_cleanup(&drain[i]);
      while ((ne = moq_session_poll_events(client, drain, 16)) > 0)
          for (size_t i = 0; i < ne; i++) moq_event_cleanup(&drain[i]);
      moq_action_t acts[16]; size_t na;
      while ((na = moq_session_poll_actions(server, acts, 16)) > 0)
          for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
      while ((na = moq_session_poll_actions(client, acts, 16)) > 0)
          for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
    }
    moq_session_destroy(server);
    moq_session_destroy(client);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("destroy_with_active_pending");
}
