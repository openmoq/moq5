#include "test_cases.h"

/* when the peer unsubscribes, the core emits UNSUBSCRIBED and
 * frees the subscription. moq_pub_handle_event must retire the matching per-track
 * slot (fire on_subscriber_left), so active-subscription state is accurate and
 * the track can be re-subscribed -- not rejected as a duplicate. */
void test_unsubscribed_retires_slot(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    tick_cb_state_t cbs = {0};
    cfg.callbacks.ctx = &cbs;
    cfg.callbacks.on_subscriber_joined = tick_on_joined;
    cfg.callbacks.on_subscriber_left = tick_on_left;

    moq_publisher_t *pub = NULL;
    moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub);

    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("t");
    tcfg.advertise_namespace = true;
    moq_pub_track_t *track = NULL;
    moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &track);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev); }

    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace.parts = ns_parts;
    scfg.track_namespace.count = 1;
    scfg.track_name = MOQ_BYTES_LITERAL("t");

    /* Subscribe + facade accept. */
    moq_subscription_t sub_h;
    MOQ_TEST_CHECK(moq_session_subscribe(moq_simpair_client(sp), &scfg,
        moq_simpair_now_us(sp), &sub_h) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_event_t ev;
    MOQ_TEST_CHECK(moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1);
    MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
    { moq_pub_event_result_t res;
      MOQ_TEST_CHECK(moq_pub_handle_event(pub, &ev,
          moq_simpair_now_us(sp), &res) == MOQ_OK); }
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track), 1);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev); }

    /* Peer unsubscribes -> server gets UNSUBSCRIBED -> facade must retire slot. */
    MOQ_TEST_CHECK(moq_session_unsubscribe(moq_simpair_client(sp), sub_h,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    bool saw_unsub = false;
    while (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
        if (ev.kind == MOQ_EVENT_UNSUBSCRIBED) saw_unsub = true;
        moq_pub_event_result_t res;
        moq_pub_handle_event(pub, &ev, moq_simpair_now_us(sp), &res);
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK(saw_unsub);
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track), 0);
    MOQ_TEST_CHECK_EQ_INT(cbs.left, 1);
    MOQ_TEST_CHECK(cbs.left_track == track);

    /* Positive control: re-subscribe to the same track is accepted, not
     * duplicate-rejected (the stale slot would have caused a duplicate reject). */
    moq_subscription_t sub_h2;
    MOQ_TEST_CHECK(moq_session_subscribe(moq_simpair_client(sp), &scfg,
        moq_simpair_now_us(sp), &sub_h2) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK(moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1);
    MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
    { moq_pub_event_result_t res;
      MOQ_TEST_CHECK(moq_pub_handle_event(pub, &ev,
          moq_simpair_now_us(sp), &res) == MOQ_OK); }
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track), 1);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    bool saw_ok = false;
    while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_OK) saw_ok = true;
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK(saw_ok);

    moq_pub_remove_track(pub, track, moq_simpair_now_us(sp));
    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("unsubscribed_retires_slot");
}

void test_deferred_accept(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_CALLBACK;
    cfg.on_subscribe = cb_defer;
    tick_cb_state_t cbs = {0};
    cfg.callbacks.ctx = &cbs;
    cfg.callbacks.on_subscriber_joined = tick_on_joined;

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

    last_deferred = NULL;
    { moq_event_t ev;
      if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
          moq_pub_event_result_t res;
          moq_pub_handle_event(pub, &ev, moq_simpair_now_us(sp), &res);
          MOQ_TEST_CHECK(res == MOQ_PUB_EVENT_CONSUMED);
          moq_event_cleanup(&ev);
      } }
    MOQ_TEST_CHECK(last_deferred != NULL);
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track), 0);

    MOQ_TEST_CHECK_EQ_INT(moq_pub_resolve_deferred(pub, last_deferred, last_deferred_id,
        true, 0, moq_simpair_now_us(sp)), MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track), 1);
    MOQ_TEST_CHECK_EQ_INT(cbs.joined, 1);

    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1) {
          MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_OK);
          moq_event_cleanup(&ev);
      } }

    moq_pub_remove_track(pub, track, moq_simpair_now_us(sp));
    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("deferred_accept");
}

void test_deferred_reject(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_CALLBACK;
    cfg.on_subscribe = cb_defer;
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

    last_deferred = NULL;
    { moq_event_t ev;
      if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
          moq_pub_event_result_t res;
          moq_pub_handle_event(pub, &ev, moq_simpair_now_us(sp), &res);
          moq_event_cleanup(&ev);
      } }

    MOQ_TEST_CHECK_EQ_INT(moq_pub_resolve_deferred(pub, last_deferred, last_deferred_id,
        false, MOQ_REQUEST_ERROR_UNAUTHORIZED,
        moq_simpair_now_us(sp)), MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track), 0);

    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1) {
          MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_ERROR);
          moq_event_cleanup(&ev);
      } }

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("deferred_reject");
}

/* Resolving one deferred authorizer transfers the decision to its parent
 * owner even while another parent's ACK is blocked. Flush commits both once,
 * and the public deferred ID cannot be replayed after transfer. */
static int g_defer_then_accept_calls = 0;
static moq_pub_accept_decision_t cb_defer_then_accept(
    void *ctx, const moq_pub_subscribe_info_t *info,
    moq_request_error_t *out_error)
{
    (void)ctx; (void)out_error;
    if (g_defer_then_accept_calls++ == 0) {
        last_deferred = info->deferred;
        last_deferred_id = info->deferred_id;
        return MOQ_PUB_DECISION_DEFER;
    }
    return MOQ_PUB_DECISION_ACCEPT;
}

static void feed_control(moq_session_t *from, moq_session_t *to)
{
    moq_action_t acts[8]; size_t na;
    while ((na = moq_session_poll_actions(from, acts, 8)) > 0)
        for (size_t i = 0; i < na; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(to, acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 0);
            moq_action_cleanup(&acts[i]);
        }
}

void test_deferred_resolution_keeps_multiple_owners(void) {
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);

    moq_session_cfg_t ccfg = MOQ_SESSION_CFG_INIT;
    ccfg.alloc = &alloc; ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    ccfg.send_request_capacity = true; ccfg.initial_request_capacity = 16;
    moq_session_cfg_t scfg = MOQ_SESSION_CFG_INIT;
    scfg.alloc = &alloc; scfg.perspective = MOQ_PERSPECTIVE_SERVER;
    scfg.send_request_capacity = true; scfg.initial_request_capacity = 16;
    scfg.max_actions = 1;   /* 1-slot queue: a second accept backpressures */

    moq_session_t *client = NULL, *server = NULL;
    moq_session_create(&ccfg, 0, &client);
    moq_session_create(&scfg, 0, &server);
    moq_session_start(client, 0);
    pump_actions_to_peer(client, server, 0);
    pump_actions_to_peer(server, client, 0);
    { moq_event_t ev;
      if (moq_session_poll_events(client, &ev, 1) == 1) moq_event_cleanup(&ev);
      if (moq_session_poll_events(server, &ev, 1) == 1) moq_event_cleanup(&ev); }

    g_defer_then_accept_calls = 0;
    last_deferred = NULL;
    moq_pub_cfg_t pcfg;
    moq_pub_cfg_init_sized(&pcfg, sizeof(pcfg));
    pcfg.accept_mode = MOQ_PUB_CALLBACK;
    pcfg.on_subscribe = cb_defer_then_accept;
    moq_publisher_t *pub = NULL;
    moq_pub_create(server, &alloc, &pcfg, &pub);

    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
    moq_pub_track_t *track1 = NULL, *track2 = NULL;
    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init(&tcfg);
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.advertise_namespace = false;   /* keep the 1 action slot free */
    tcfg.track_name = MOQ_BYTES_LITERAL("t1");
    moq_pub_add_track(pub, &tcfg, 0, &track1);
    tcfg.track_name = MOQ_BYTES_LITERAL("t2");
    moq_pub_add_track(pub, &tcfg, 0, &track2);

    moq_subscribe_cfg_t subcfg;
    moq_subscribe_cfg_init(&subcfg);
    subcfg.track_namespace.parts = ns_parts;
    subcfg.track_namespace.count = 1;

    /* Subscribe #1 (track t1) -> DEFER (no action consumed). */
    subcfg.track_name = MOQ_BYTES_LITERAL("t1");
    moq_subscription_t s1;
    moq_session_subscribe(client, &subcfg, 0, &s1);
    feed_control(client, server);
    moq_event_t ev;
    MOQ_TEST_CHECK(moq_session_poll_events(server, &ev, 1) == 1);
    MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
    { moq_pub_event_result_t res;
      MOQ_TEST_CHECK(moq_pub_handle_event(pub, &ev, 0, &res) == MOQ_OK); }
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK(last_deferred != NULL);

    /* Fill the 1-slot action queue so the next accept backpressures. */
    moq_publish_namespace_cfg_t nscfg;
    moq_publish_namespace_cfg_init(&nscfg);
    moq_bytes_t blk[] = { MOQ_BYTES_LITERAL("blk") };
    nscfg.track_namespace.parts = blk; nscfg.track_namespace.count = 1;
    moq_announcement_t blk_ann;
    moq_session_publish_namespace(server, &nscfg, 0, &blk_ann);

    /* Subscribe #2 is accepted into its owner while wire output is full. */
    subcfg.track_name = MOQ_BYTES_LITERAL("t2");
    moq_subscription_t s2;
    moq_session_subscribe(client, &subcfg, 0, &s2);
    feed_control(client, server);
    MOQ_TEST_CHECK(moq_session_poll_events(server, &ev, 1) == 1);
    MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
    { moq_pub_event_result_t res;
      MOQ_TEST_CHECK(moq_pub_handle_event(pub, &ev, 0, &res)
          == MOQ_ERR_WOULD_BLOCK); }
    moq_event_cleanup(&ev);

    /* The decision transfers without overwriting the other parent's owner. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_pub_resolve_deferred(pub, last_deferred,
        last_deferred_id, true, 0, 0), (int)MOQ_ERR_WOULD_BLOCK);

    MOQ_TEST_CHECK_EQ_INT((int)moq_pub_resolve_deferred(pub, last_deferred,
        last_deferred_id, true, 0, 0), (int)MOQ_ERR_STALE_HANDLE);
    moq_result_t rc = MOQ_ERR_WOULD_BLOCK;
    for (unsigned i = 0; i < 4 && rc == MOQ_ERR_WOULD_BLOCK; i++) {
        moq_action_t acts[4]; size_t na;
        while ((na = moq_session_poll_actions(server, acts, 4)) > 0)
            for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
        rc = moq_pub_flush(pub, 0);
    }
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(g_defer_then_accept_calls, 2);

    /* Both subscriptions accepted: neither was orphaned. */
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track1), 1);
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track2), 1);

    moq_pub_destroy(pub);
    { moq_action_t acts[16]; size_t na;
      while ((na = moq_session_poll_actions(server, acts, 16)) > 0)
          for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
      while ((na = moq_session_poll_actions(client, acts, 16)) > 0)
          for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]); }
    { moq_event_t d;
      while (moq_session_poll_events(server, &d, 1) == 1) moq_event_cleanup(&d);
      while (moq_session_poll_events(client, &d, 1) == 1) moq_event_cleanup(&d); }
    moq_session_destroy(client);
    moq_session_destroy(server);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("deferred_resolution_keeps_multiple_owners");
}

/* A track ended before any subscriber must reject a late DIRECT subscribe
 * (terminal track), not install a slot that gets neither media nor terminal. */
void test_end_track_rejects_late_subscribe(void) {
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

    /* End the track before anyone subscribes: no subscriber -> local terminal. */
    MOQ_TEST_CHECK_EQ_INT(moq_pub_end_track(pub, track,
        moq_simpair_now_us(sp)), MOQ_OK);

    /* A late subscribe must be rejected, not accepted. */
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
      } }
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track), 0);

    moq_simpair_run_until_quiescent(sp, 8, NULL);
    bool got_error = false;
    { moq_event_t ev;
      while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1) {
          if (ev.kind == MOQ_EVENT_SUBSCRIBE_ERROR) got_error = true;
          moq_event_cleanup(&ev);
      } }
    MOQ_TEST_CHECK(got_error);

    moq_pub_remove_track(pub, track, moq_simpair_now_us(sp));
    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("end_track_rejects_late_subscribe");
}

/* Same, via the DEFERRED path: a subscribe is deferred, the track ends, then
 * resolve_deferred(accept=true) must still reject (the track went terminal). */
void test_end_track_rejects_deferred_subscribe(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_CALLBACK;
    cfg.on_subscribe = cb_defer;
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

    last_deferred = NULL;
    { moq_event_t ev;
      if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
          moq_pub_event_result_t res;
          moq_pub_handle_event(pub, &ev, moq_simpair_now_us(sp), &res);
          MOQ_TEST_CHECK(res == MOQ_PUB_EVENT_CONSUMED);
          moq_event_cleanup(&ev);
      } }
    MOQ_TEST_CHECK(last_deferred != NULL);
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track), 0);

    /* End the track while the subscribe is still deferred. */
    MOQ_TEST_CHECK_EQ_INT(moq_pub_end_track(pub, track,
        moq_simpair_now_us(sp)), MOQ_OK);

    /* Resolving with accept=true must be overridden to a reject. */
    MOQ_TEST_CHECK_EQ_INT(moq_pub_resolve_deferred(pub, last_deferred,
        last_deferred_id, true, 0, moq_simpair_now_us(sp)), MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track), 0);

    moq_simpair_run_until_quiescent(sp, 8, NULL);
    bool got_error = false;
    { moq_event_t ev;
      while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1) {
          if (ev.kind == MOQ_EVENT_SUBSCRIBE_ERROR) got_error = true;
          moq_event_cleanup(&ev);
      } }
    MOQ_TEST_CHECK(got_error);

    moq_pub_remove_track(pub, track, moq_simpair_now_us(sp));
    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("end_track_rejects_deferred_subscribe");
}

void test_deferred_would_block_then_flush(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_CALLBACK;
    cfg.on_subscribe = cb_defer;
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

    last_deferred = NULL;
    { moq_event_t ev;
      if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
          moq_pub_event_result_t res;
          moq_pub_handle_event(pub, &ev, moq_simpair_now_us(sp), &res);
          moq_event_cleanup(&ev);
      } }

    test_session_fill_action_queue(moq_simpair_server(sp));
    MOQ_TEST_CHECK_EQ_INT(moq_pub_resolve_deferred(pub, last_deferred, last_deferred_id,
        true, 0, moq_simpair_now_us(sp)), MOQ_ERR_WOULD_BLOCK);

    { moq_action_t acts[64]; size_t na;
      while ((na = moq_session_poll_actions(moq_simpair_server(sp),
              acts, 64)) > 0)
          for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]); }
    MOQ_TEST_CHECK_EQ_INT(moq_pub_flush(pub, moq_simpair_now_us(sp)),
        MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track), 1);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("deferred_would_block_then_flush");
}

void test_deferred_remove_track_blocked(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_CALLBACK;
    cfg.on_subscribe = cb_defer;
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

    last_deferred = NULL;
    { moq_event_t ev;
      if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
          moq_pub_event_result_t res;
          moq_pub_handle_event(pub, &ev, moq_simpair_now_us(sp), &res);
          moq_event_cleanup(&ev);
      } }

    MOQ_TEST_CHECK_EQ_INT(moq_pub_remove_track(pub, track,
        moq_simpair_now_us(sp)), MOQ_ERR_WOULD_BLOCK);

    MOQ_TEST_CHECK_EQ_INT(moq_pub_resolve_deferred(pub, last_deferred, last_deferred_id,
        false, MOQ_REQUEST_ERROR_UNAUTHORIZED,
        moq_simpair_now_us(sp)), MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev); }
    MOQ_TEST_CHECK_EQ_INT(moq_pub_remove_track(pub, track,
        moq_simpair_now_us(sp)), MOQ_OK);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("deferred_remove_track_blocked");
}

void test_deferred_session_close_releases(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_CALLBACK;
    cfg.on_subscribe = cb_defer;
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

    last_deferred = NULL;
    { moq_event_t ev;
      if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
          moq_pub_event_result_t res;
          moq_pub_handle_event(pub, &ev, moq_simpair_now_us(sp), &res);
          moq_event_cleanup(&ev);
      } }
    MOQ_TEST_CHECK(last_deferred != NULL);

    moq_session_on_transport_close(moq_simpair_server(sp), 0,
        moq_simpair_now_us(sp));
    moq_pub_tick(pub, moq_simpair_now_us(sp));

    MOQ_TEST_CHECK_EQ_INT(moq_pub_resolve_deferred(pub, last_deferred, last_deferred_id,
        true, 0, moq_simpair_now_us(sp)), MOQ_ERR_STALE_HANDLE);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("deferred_session_close_releases");
}

static moq_pub_accept_decision_t cb_defer_or_accept(
    void *ctx, const moq_pub_subscribe_info_t *info,
    moq_request_error_t *out_error)
{
    (void)out_error;
    int *count = (int *)ctx;
    (*count)++;
    if (info->deferred) {
        last_deferred = info->deferred;
        last_deferred_id = info->deferred_id;
        return MOQ_PUB_DECISION_DEFER;
    }
    return MOQ_PUB_DECISION_ACCEPT;
}

void test_deferred_does_not_block_other_tracks(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    int cb_count = 0;
    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_CALLBACK;
    cfg.on_subscribe = cb_defer_or_accept;
    cfg.on_subscribe_ctx = &cb_count;
    moq_publisher_t *pub = NULL;
    moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub);

    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };

    moq_pub_track_cfg_t tcfg_a;
    moq_pub_track_cfg_init(&tcfg_a);
    tcfg_a.track_namespace.parts = ns_parts;
    tcfg_a.track_namespace.count = 1;
    tcfg_a.track_name = MOQ_BYTES_LITERAL("a");
    moq_pub_track_t *track_a = NULL;
    moq_pub_add_track(pub, &tcfg_a, moq_simpair_now_us(sp), &track_a);

    moq_pub_track_cfg_t tcfg_b;
    moq_pub_track_cfg_init(&tcfg_b);
    tcfg_b.track_namespace.parts = ns_parts;
    tcfg_b.track_namespace.count = 1;
    tcfg_b.track_name = MOQ_BYTES_LITERAL("b");
    moq_pub_track_t *track_b = NULL;
    moq_pub_add_track(pub, &tcfg_b, moq_simpair_now_us(sp), &track_b);

    /* Subscribe to track A — deferred. */
    moq_subscribe_cfg_t scfg_a;
    moq_subscribe_cfg_init(&scfg_a);
    scfg_a.track_namespace.parts = ns_parts;
    scfg_a.track_namespace.count = 1;
    scfg_a.track_name = MOQ_BYTES_LITERAL("a");
    moq_subscription_t sub_a;
    moq_session_subscribe(moq_simpair_client(sp), &scfg_a,
        moq_simpair_now_us(sp), &sub_a);
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    last_deferred = NULL;
    moq_pub_tick(pub, moq_simpair_now_us(sp));
    MOQ_TEST_CHECK(last_deferred != NULL);
    MOQ_TEST_CHECK_EQ_INT(cb_count, 1);
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track_a), 0);

    /* Subscribe to track B while A is deferred. Callback is invoked
     * with deferred=NULL (slot occupied), so it returns ACCEPT. */
    moq_subscribe_cfg_t scfg_b;
    moq_subscribe_cfg_init(&scfg_b);
    scfg_b.track_namespace.parts = ns_parts;
    scfg_b.track_namespace.count = 1;
    scfg_b.track_name = MOQ_BYTES_LITERAL("b");
    moq_subscription_t sub_b;
    moq_session_subscribe(moq_simpair_client(sp), &scfg_b,
        moq_simpair_now_us(sp), &sub_b);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_pub_tick(pub, moq_simpair_now_us(sp));

    MOQ_TEST_CHECK_EQ_INT(cb_count, 2);
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track_b), 1);

    /* Resolve A. */
    MOQ_TEST_CHECK_EQ_INT(moq_pub_resolve_deferred(pub, last_deferred, last_deferred_id,
        true, 0, moq_simpair_now_us(sp)), MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track_a), 1);

    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev); }

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("deferred_does_not_block_other_tracks");
}

void test_deferred_unsub_before_resolve(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_CALLBACK;
    cfg.on_subscribe = cb_defer;
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

    last_deferred = NULL;
    moq_pub_tick(pub, moq_simpair_now_us(sp));
    MOQ_TEST_CHECK(last_deferred != NULL);

    /* Pump pending actions so the client session is quiescent. */
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev); }

    /* Client unsubscribes before deferred is resolved. */
    moq_session_unsubscribe(moq_simpair_client(sp), sub_h,
        moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_pub_tick(pub, moq_simpair_now_us(sp));

    /* Resolving the now-stale deferred should fail. */
    MOQ_TEST_CHECK_EQ_INT(moq_pub_resolve_deferred(pub, last_deferred, last_deferred_id,
        true, 0, moq_simpair_now_us(sp)), MOQ_ERR_STALE_HANDLE);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("deferred_unsub_before_resolve");
}

void test_deferred_stale_id_after_reuse(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_CALLBACK;
    cfg.on_subscribe = cb_defer;
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

    /* Defer A. */
    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace.parts = ns_parts;
    scfg.track_namespace.count = 1;
    scfg.track_name = MOQ_BYTES_LITERAL("t");
    moq_subscription_t sub_a;
    moq_session_subscribe(moq_simpair_client(sp), &scfg,
        moq_simpair_now_us(sp), &sub_a);
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    last_deferred = NULL;
    moq_pub_tick(pub, moq_simpair_now_us(sp));
    moq_pub_deferred_t *handle_a = last_deferred;
    uint64_t id_a = last_deferred_id;
    MOQ_TEST_CHECK(handle_a != NULL);

    /* Reject A so the slot is freed. */
    MOQ_TEST_CHECK_EQ_INT(moq_pub_resolve_deferred(pub, handle_a, id_a,
        false, MOQ_REQUEST_ERROR_UNAUTHORIZED,
        moq_simpair_now_us(sp)), MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev); }

    /* Defer B into the same slot. */
    moq_subscription_t sub_b;
    moq_session_subscribe(moq_simpair_client(sp), &scfg,
        moq_simpair_now_us(sp), &sub_b);
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    last_deferred = NULL;
    moq_pub_tick(pub, moq_simpair_now_us(sp));
    MOQ_TEST_CHECK(last_deferred != NULL);
    uint64_t id_b = last_deferred_id;
    MOQ_TEST_CHECK(id_a != id_b);

    /* Resolve using A's stale id must fail. */
    MOQ_TEST_CHECK_EQ_INT(moq_pub_resolve_deferred(pub, handle_a, id_a,
        true, 0, moq_simpair_now_us(sp)), MOQ_ERR_STALE_HANDLE);
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track), 0);

    /* Resolve using B's id succeeds. */
    MOQ_TEST_CHECK_EQ_INT(moq_pub_resolve_deferred(pub, last_deferred,
        id_b, true, 0, moq_simpair_now_us(sp)), MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track), 1);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("deferred_stale_id_after_reuse");
}

void test_deferred_flush_fires_joined(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    tick_cb_state_t cbs = {0};
    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_CALLBACK;
    cfg.on_subscribe = cb_defer;
    cfg.callbacks.ctx = &cbs;
    cfg.callbacks.on_subscriber_joined = tick_on_joined;

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

    last_deferred = NULL;
    moq_pub_tick(pub, moq_simpair_now_us(sp));

    /* Fill queue so resolve returns WOULD_BLOCK. */
    test_session_fill_action_queue(moq_simpair_server(sp));
    MOQ_TEST_CHECK_EQ_INT(moq_pub_resolve_deferred(pub, last_deferred,
        last_deferred_id, true, 0, moq_simpair_now_us(sp)),
        MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK_EQ_INT(cbs.joined, 0);

    /* Drain and flush — must fire on_subscriber_joined. */
    { moq_action_t acts[64]; size_t na;
      while ((na = moq_session_poll_actions(moq_simpair_server(sp),
              acts, 64)) > 0)
          for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]); }
    MOQ_TEST_CHECK_EQ_INT(moq_pub_flush(pub, moq_simpair_now_us(sp)),
        MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(cbs.joined, 1);
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track), 1);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("deferred_flush_fires_joined");
}

void test_deferred_old_cfg_struct_size(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.struct_size = offsetof(moq_pub_cfg_t, callbacks) +
                      sizeof(cfg.callbacks);
    cfg.accept_mode = MOQ_PUB_CALLBACK;
    cfg.on_subscribe = cb_defer;

    moq_publisher_t *pub = NULL;
    MOQ_TEST_CHECK_EQ_INT(moq_pub_create(moq_simpair_server(sp), &alloc,
        &cfg, &pub), MOQ_OK);

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

    last_deferred = NULL;
    moq_pub_tick(pub, moq_simpair_now_us(sp));
    MOQ_TEST_CHECK(last_deferred != NULL);

    MOQ_TEST_CHECK_EQ_INT(moq_pub_resolve_deferred(pub, last_deferred,
        last_deferred_id, true, 0, moq_simpair_now_us(sp)), MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track), 1);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("deferred_old_cfg_struct_size");
}
