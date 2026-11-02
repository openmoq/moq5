#include "test_cases.h"

static moq_pub_accept_decision_t cb_reject_does_not_exist(
    void *ctx, const moq_pub_subscribe_info_t *info,
    moq_request_error_t *out_error) {
    (void)ctx; (void)info;
    *out_error = MOQ_REQUEST_ERROR_DOES_NOT_EXIST;
    return MOQ_PUB_DECISION_REJECT;
}

static moq_pub_accept_decision_t cb_reject_default(
    void *ctx, const moq_pub_subscribe_info_t *info,
    moq_request_error_t *out_error) {
    (void)ctx; (void)info; (void)out_error;
    return MOQ_PUB_DECISION_REJECT;
}

static moq_pub_accept_decision_t cb_invalid_decision(
    void *ctx, const moq_pub_subscribe_info_t *info,
    moq_request_error_t *out_error) {
    (void)ctx; (void)info; (void)out_error;
    return (moq_pub_accept_decision_t)99;
}

/* -- Callback mode tests ------------------------------------------ */

void test_callback_accept(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_CALLBACK;
    cfg.on_subscribe = cb_accept_all;
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
          moq_result_t rc = moq_pub_handle_event(pub, &ev,
              moq_simpair_now_us(sp), &res);
          MOQ_TEST_CHECK(rc == MOQ_OK);
          MOQ_TEST_CHECK(res == MOQ_PUB_EVENT_CONSUMED);
          moq_event_cleanup(&ev);
      }
    }
    MOQ_TEST_CHECK(moq_pub_active_subscriptions(pub, track) == 1);

    moq_pub_remove_track(pub, track, moq_simpair_now_us(sp));
    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("callback_accept");
}

void test_callback_reject_custom_error(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_CALLBACK;
    cfg.on_subscribe = cb_reject_does_not_exist;
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
          MOQ_TEST_CHECK(res == MOQ_PUB_EVENT_CONSUMED);
          moq_event_cleanup(&ev);
      }
    }
    MOQ_TEST_CHECK(moq_pub_active_subscriptions(pub, track) == 0);

    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1) {
          MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_ERROR);
          MOQ_TEST_CHECK(ev.u.subscribe_error.error_code ==
                          MOQ_REQUEST_ERROR_DOES_NOT_EXIST);
          moq_event_cleanup(&ev);
      }
    }

    moq_pub_remove_track(pub, track, moq_simpair_now_us(sp));
    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("callback_reject_custom_error");
}

void test_callback_reject_default_error(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_CALLBACK;
    cfg.on_subscribe = cb_reject_default;
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
      if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1) {
          MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_ERROR);
          MOQ_TEST_CHECK(ev.u.subscribe_error.error_code ==
                          MOQ_REQUEST_ERROR_UNAUTHORIZED);
          moq_event_cleanup(&ev);
      }
    }

    moq_pub_remove_track(pub, track, moq_simpair_now_us(sp));
    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("callback_reject_default_error");
}

void test_callback_sees_filter_and_ns(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    cb_capture_t cap = {0};
    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_CALLBACK;
    cfg.on_subscribe = cb_capture;
    cfg.on_subscribe_ctx = &cap;
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
    scfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    scfg.has_subscriber_priority = true;
    scfg.subscriber_priority = 42;
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

    MOQ_TEST_CHECK(cap.called);
    MOQ_TEST_CHECK(cap.filter == MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT);
    MOQ_TEST_CHECK(cap.subscriber_priority == 42);
    MOQ_TEST_CHECK(cap.ns_count == 1);
    MOQ_TEST_CHECK(cap.name_len == 5);

    moq_pub_remove_track(pub, track, moq_simpair_now_us(sp));
    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("callback_sees_filter_and_ns");
}

void test_callback_sees_tokens(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    cb_capture_t cap = {0};
    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_CALLBACK;
    cfg.on_subscribe = cb_capture;
    cfg.on_subscribe_ctx = &cap;
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

    /* Build a real AUTH_TOKEN USE_VALUE param. */
    moq_d16_auth_token_t tok;
    memset(&tok, 0, sizeof(tok));
    tok.alias_type = MOQ_AUTH_TOKEN_USE_VALUE;
    tok.token_type = 42;
    tok.token_value = (const uint8_t *)"tok";
    tok.token_value_len = 3;

    uint8_t tok_buf[64];
    moq_buf_writer_t tw;
    moq_buf_writer_init(&tw, tok_buf, sizeof(tok_buf));
    moq_d16_auth_token_encode(&tw, &tok);
    size_t tok_len = moq_buf_writer_offset(&tw);

    moq_kvp_entry_t params[1];
    params[0].type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN;
    params[0].value = tok_buf;
    params[0].value_len = tok_len;
    params[0].is_varint = false;
    params[0].raw = NULL;
    params[0].raw_len = 0;

    /* Inject raw SUBSCRIBE with the token into the server session. */
    feed_subscribe(moq_simpair_server(sp), 0, "ns", "t", params, 1);

    { moq_event_t ev;
      if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
          moq_pub_event_result_t res;
          moq_pub_handle_event(pub, &ev, moq_simpair_now_us(sp), &res);
          moq_event_cleanup(&ev);
      }
    }

    MOQ_TEST_CHECK(cap.called);
    MOQ_TEST_CHECK(cap.token_count == 1);
    MOQ_TEST_CHECK(cap.first_token_type == 42);
    MOQ_TEST_CHECK(cap.first_token_value_len == 3);
    MOQ_TEST_CHECK(memcmp(cap.first_token_value, "tok", 3) == 0);

    moq_pub_remove_track(pub, track, moq_simpair_now_us(sp));
    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("callback_sees_tokens");
}

void test_callback_would_block(void) {
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
    pcfg.accept_mode = MOQ_PUB_CALLBACK;
    pcfg.on_subscribe = cb_accept_all;
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

    /* Fill queue before subscribe arrives. */
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
    MOQ_TEST_CHECK(moq_session_poll_events(server, &ev, 1) == 1);
    moq_pub_event_result_t res;
    moq_result_t rc = moq_pub_handle_event(pub, &ev, 0, &res);
    MOQ_TEST_CHECK(rc == MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(res == MOQ_PUB_EVENT_CONSUMED);
    moq_event_cleanup(&ev);

    { moq_action_t acts[4]; size_t na;
      while ((na = moq_session_poll_actions(server, acts, 4)) > 0)
          for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
    }
    rc = moq_pub_flush(pub, 0);
    MOQ_TEST_CHECK(rc == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_active_subscriptions(pub, track) == 1);

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
    MOQ_TEST_PASS("callback_would_block");
}

void test_callback_null_rejected(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_CALLBACK;
    cfg.on_subscribe = NULL;

    moq_publisher_t *pub = NULL;
    MOQ_TEST_CHECK(moq_pub_create(moq_simpair_server(sp), &alloc,
        &cfg, &pub) == MOQ_ERR_INVAL);
    MOQ_TEST_CHECK(pub == NULL);

    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("callback_null_rejected");
}

void test_callback_invalid_decision(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_CALLBACK;
    cfg.on_subscribe = cb_invalid_decision;
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
    MOQ_TEST_CHECK(moq_pub_active_subscriptions(pub, track) == 0);

    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1) {
          MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_ERROR);
          MOQ_TEST_CHECK(ev.u.subscribe_error.error_code ==
                          MOQ_REQUEST_ERROR_INTERNAL_ERROR);
          moq_event_cleanup(&ev);
      }
    }

    moq_pub_remove_track(pub, track, moq_simpair_now_us(sp));
    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("callback_invalid_decision");
}

void test_callback_not_called_for_duplicate(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    cb_capture_t cap = {0};
    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_CALLBACK;
    cfg.on_subscribe = cb_capture;
    cfg.on_subscribe_ctx = &cap;
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

    /* First subscribe — callback called, accepted. */
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
    MOQ_TEST_CHECK(cap.called);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev); }

    /* Second subscribe — duplicate, callback NOT called. */
    cap.called = false;
    moq_subscription_t sub_h2;
    moq_session_subscribe(moq_simpair_client(sp), &scfg,
        moq_simpair_now_us(sp), &sub_h2);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
          moq_pub_event_result_t res;
          moq_pub_handle_event(pub, &ev, moq_simpair_now_us(sp), &res);
          moq_event_cleanup(&ev);
      }
    }
    MOQ_TEST_CHECK(!cap.called);

    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev); }

    moq_pub_remove_track(pub, track, moq_simpair_now_us(sp));
    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("callback_not_called_for_duplicate");
}

void test_old_struct_size_accept_all(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    /* Simulate an old caller with a smaller struct_size. */
    struct {
        uint32_t              struct_size;
        moq_pub_accept_mode_t accept_mode;
        uint8_t               default_publisher_priority;
    } old_cfg;
    memset(&old_cfg, 0, sizeof(old_cfg));
    old_cfg.struct_size = sizeof(old_cfg);
    old_cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    old_cfg.default_publisher_priority = 128;

    moq_publisher_t *pub = NULL;
    moq_result_t rc = moq_pub_create(moq_simpair_server(sp), &alloc,
        (const moq_pub_cfg_t *)&old_cfg, &pub);
    MOQ_TEST_CHECK(rc == MOQ_OK);
    MOQ_TEST_CHECK(pub != NULL);

    /* CALLBACK with old struct should fail. */
    old_cfg.accept_mode = MOQ_PUB_CALLBACK;
    moq_publisher_t *pub2 = NULL;
    rc = moq_pub_create(moq_simpair_server(sp), &alloc,
        (const moq_pub_cfg_t *)&old_cfg, &pub2);
    MOQ_TEST_CHECK(rc == MOQ_ERR_INVAL);
    MOQ_TEST_CHECK(pub2 == NULL);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("old_struct_size_accept_all");
}
