#include "test_cases.h"

/* Headline: a Forward 1->0->1 update forwarded ONLY through the manual API
 * (no tick) PAUSES delivery without retiring the subscription, then resumes.
 * Against the pre-refactor divergent handle_event (which ignored
 * SUBSCRIBE_UPDATED) the paused object would have been delivered. */
void test_pub_manual_forward_zero_pauses(void) {
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
    cfg.callbacks.on_subscriber_updated = tick_on_updated;
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

    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace.parts = ns_parts;
    scfg.track_namespace.count = 1;
    scfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_subscription_t sub_h;
    moq_session_subscribe(cl, &scfg, moq_simpair_now_us(sp), &sub_h);
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    /* Accept via the manual API -- joined fires exactly once from there. */
    manual_forward(pub, sv, moq_simpair_now_us(sp), 0, NULL, NULL);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_INT(cbs.joined, 1);
    MOQ_TEST_CHECK(moq_pub_has_subscriber(pub, track));

    /* Forward=0 update, forwarded ONLY through handle_event. */
    moq_subscription_update_cfg_t ucfg;
    moq_subscription_update_cfg_init(&ucfg);
    ucfg.has_forward = true; ucfg.forward = false;
    MOQ_TEST_CHECK(moq_session_update_subscription(cl, sub_h, &ucfg,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    int seen = 0; moq_pub_event_result_t res = MOQ_PUB_EVENT_IGNORED;
    manual_forward(pub, sv, moq_simpair_now_us(sp),
                   MOQ_EVENT_SUBSCRIBE_UPDATED, &seen, &res);
    MOQ_TEST_CHECK_EQ_INT(seen, 1);
    MOQ_TEST_CHECK(res == MOQ_PUB_EVENT_CONSUMED);   /* matched -> CONSUMED */
    MOQ_TEST_CHECK_EQ_INT(cbs.updated, 1);
    MOQ_TEST_CHECK(cbs.last_update.has_forward && !cbs.last_update.forward);
    /* PAUSE, not retire: the slot is still active. */
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track), 1);

    /* Write while paused: no delivery. */
    uint8_t d[] = { 0x71, 0x72 };
    moq_rcbuf_t *pay = NULL; moq_rcbuf_create(&alloc, d, sizeof(d), &pay);
    MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 5, 0, pay,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_rcbuf_decref(pay);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_INT(fanout_count_objects(sp, 5, 0, d, sizeof(d)), 0);

    /* Resume: Forward=1, forwarded through handle_event, then delivery works. */
    ucfg.forward = true;
    MOQ_TEST_CHECK(moq_session_update_subscription(cl, sub_h, &ucfg,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    manual_forward(pub, sv, moq_simpair_now_us(sp), 0, NULL, NULL);
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track), 1);

    uint8_t d2[] = { 0x73, 0x74 };
    moq_rcbuf_t *pay2 = NULL; moq_rcbuf_create(&alloc, d2, sizeof(d2), &pay2);
    MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 6, 0, pay2,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_rcbuf_decref(pay2);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_INT(fanout_count_objects(sp, 6, 0, d2, sizeof(d2)), 1);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("pub_manual_forward_zero_pauses");
}

/* Manual API: SESSION_CLOSED and GOAWAY update local state and fire callbacks,
 * but return IGNORED (broadcast -- stays forwardable). Unmatched events and
 * PUBLISH_FINISHED (opposite role) are IGNORED with no state change. */
void test_pub_manual_broadcasts_and_results(void) {
    /* GOAWAY: draining + on_draining, result IGNORED. */
    {
        test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
        simpair_setup(&as, &alloc, &sp);
        moq_session_t *sv = moq_simpair_server(sp);
        tick_cb_state_t cbs = {0};
        moq_pub_cfg_t cfg; moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
        cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
        cfg.callbacks.ctx = &cbs;
        cfg.callbacks.on_draining = tick_on_draining;
        cfg.callbacks.on_closed = tick_on_closed;
        moq_publisher_t *pub = NULL;
        moq_pub_create(sv, &alloc, &cfg, &pub);

        moq_event_t goaway;
        memset(&goaway, 0, sizeof(goaway));
        goaway.kind = MOQ_EVENT_GOAWAY;
        moq_pub_event_result_t res = MOQ_PUB_EVENT_CONSUMED;
        MOQ_TEST_CHECK(moq_pub_handle_event(pub, &goaway,
            moq_simpair_now_us(sp), &res) == MOQ_OK);
        MOQ_TEST_CHECK(res == MOQ_PUB_EVENT_IGNORED);   /* broadcast */
        MOQ_TEST_CHECK_EQ_INT(cbs.draining, 1);

        /* SESSION_CLOSED: on_closed fires, result IGNORED. */
        moq_event_t closed;
        memset(&closed, 0, sizeof(closed));
        closed.kind = MOQ_EVENT_SESSION_CLOSED;
        closed.u.closed.code = 7;
        res = MOQ_PUB_EVENT_CONSUMED;
        MOQ_TEST_CHECK(moq_pub_handle_event(pub, &closed,
            moq_simpair_now_us(sp), &res) == MOQ_OK);
        MOQ_TEST_CHECK(res == MOQ_PUB_EVENT_IGNORED);
        MOQ_TEST_CHECK_EQ_INT(cbs.closed, 1);
        MOQ_TEST_CHECK_EQ_U64(cbs.close_code, 7);

        moq_pub_destroy(pub);
        drain_all(sp);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* Unmatched events + PUBLISH_FINISHED are IGNORED. */
    {
        test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
        simpair_setup(&as, &alloc, &sp);
        moq_session_t *sv = moq_simpair_server(sp);
        moq_pub_cfg_t cfg; moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
        cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
        moq_publisher_t *pub = NULL;
        moq_pub_create(sv, &alloc, &cfg, &pub);

        /* PUBLISH_FINISHED: opposite role -> always IGNORED. */
        moq_event_t fin;
        memset(&fin, 0, sizeof(fin));
        fin.kind = MOQ_EVENT_PUBLISH_FINISHED;
        moq_pub_event_result_t res = MOQ_PUB_EVENT_CONSUMED;
        MOQ_TEST_CHECK(moq_pub_handle_event(pub, &fin,
            moq_simpair_now_us(sp), &res) == MOQ_OK);
        MOQ_TEST_CHECK(res == MOQ_PUB_EVENT_IGNORED);

        /* SUBSCRIBE_UPDATED for a sub on no track -> IGNORED (unmatched). */
        moq_event_t upd;
        memset(&upd, 0, sizeof(upd));
        upd.kind = MOQ_EVENT_SUBSCRIBE_UPDATED;
        upd.u.subscribe_updated.has_forward = true;
        upd.u.subscribe_updated.forward = false;
        res = MOQ_PUB_EVENT_CONSUMED;
        MOQ_TEST_CHECK(moq_pub_handle_event(pub, &upd,
            moq_simpair_now_us(sp), &res) == MOQ_OK);
        MOQ_TEST_CHECK(res == MOQ_PUB_EVENT_IGNORED);

        moq_pub_destroy(pub);
        drain_all(sp);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
    }
    MOQ_TEST_PASS("pub_manual_broadcasts_and_results");
}

/* Manual API: a publisher-initiated PUBLISH's PUBLISH_OK is handled through
 * moq_pub_handle_event (previously ignored -> publish_ok never set, track
 * never published). */
void test_pub_manual_publish_ok(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);
    moq_session_t *sv = moq_simpair_server(sp);
    tick_cb_state_t cbs = {0};
    moq_pub_cfg_t cfg; moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    cfg.callbacks.ctx = &cbs;
    cfg.callbacks.on_publish_ok = tick_on_publish_ok;
    moq_publisher_t *pub = NULL;
    moq_pub_create(sv, &alloc, &cfg, &pub);

    moq_pub_track_cfg_t tcfg; moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_pub_track_t *track = NULL;
    moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &track);

    moq_pub_publish_cfg_t pcfg; moq_pub_publish_cfg_init(&pcfg);
    MOQ_TEST_CHECK(moq_pub_publish_track(pub, track, &pcfg,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    for (int i = 0; i < 4; i++) {
        moq_event_t ev;
        if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) != 1) break;
        if (ev.kind == MOQ_EVENT_PUBLISH_REQUEST) {
            moq_accept_publish_cfg_t acc; moq_accept_publish_cfg_init(&acc);
            moq_session_accept_publish(moq_simpair_client(sp),
                ev.u.publish_request.pub, &acc, moq_simpair_now_us(sp));
        }
        moq_event_cleanup(&ev);
    }
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    /* PUBLISH_OK forwarded ONLY through the manual API. */
    int seen = 0; moq_pub_event_result_t res = MOQ_PUB_EVENT_IGNORED;
    manual_forward(pub, sv, moq_simpair_now_us(sp),
                   MOQ_EVENT_PUBLISH_OK, &seen, &res);
    MOQ_TEST_CHECK_EQ_INT(seen, 1);
    MOQ_TEST_CHECK(res == MOQ_PUB_EVENT_CONSUMED);
    MOQ_TEST_CHECK_EQ_INT(cbs.publish_ok, 1);
    MOQ_TEST_CHECK(moq_pub_track_is_published(pub, track));

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("pub_manual_publish_ok");
}

/* Manual API: UNSUBSCRIBED clears the active slot and fires on_subscriber_left
 * (the union cleanup -- pending/deferred variants share the same handler). */
void test_pub_manual_unsubscribe_active(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);
    moq_session_t *sv = moq_simpair_server(sp);
    moq_session_t *cl = moq_simpair_client(sp);
    tick_cb_state_t cbs = {0};
    moq_pub_cfg_t cfg; moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    cfg.callbacks.ctx = &cbs;
    cfg.callbacks.on_subscriber_joined = tick_on_joined;
    cfg.callbacks.on_subscriber_left = tick_on_left;
    moq_publisher_t *pub = NULL;
    moq_pub_create(sv, &alloc, &cfg, &pub);

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
    moq_session_subscribe(cl, &scfg, moq_simpair_now_us(sp), &sub_h);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    manual_forward(pub, sv, moq_simpair_now_us(sp), 0, NULL, NULL);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_INT(cbs.joined, 1);
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track), 1);

    MOQ_TEST_CHECK(moq_session_unsubscribe(cl, sub_h,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    int seen = 0; moq_pub_event_result_t res = MOQ_PUB_EVENT_IGNORED;
    manual_forward(pub, sv, moq_simpair_now_us(sp),
                   MOQ_EVENT_UNSUBSCRIBED, &seen, &res);
    MOQ_TEST_CHECK_EQ_INT(seen, 1);
    MOQ_TEST_CHECK(res == MOQ_PUB_EVENT_CONSUMED);
    MOQ_TEST_CHECK_EQ_INT(cbs.left, 1);
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track), 0);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("pub_manual_unsubscribe_active");
}

/* Equivalence: the same subscribe + Forward-0 scenario driven through the
 * manual API and through moq_pub_tick yields identical callback counts and
 * publisher state. */
static void parity_run(bool use_manual, int *joined, int *updated,
                       size_t *active) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);
    moq_session_t *sv = moq_simpair_server(sp);
    moq_session_t *cl = moq_simpair_client(sp);
    tick_cb_state_t cbs = {0};
    moq_pub_cfg_t cfg; moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    cfg.callbacks.ctx = &cbs;
    cfg.callbacks.on_subscriber_joined = tick_on_joined;
    cfg.callbacks.on_subscriber_updated = tick_on_updated;
    moq_publisher_t *pub = NULL;
    moq_pub_create(sv, &alloc, &cfg, &pub);

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
    moq_session_subscribe(cl, &scfg, moq_simpair_now_us(sp), &sub_h);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    if (use_manual) manual_forward(pub, sv, moq_simpair_now_us(sp), 0, NULL, NULL);
    else moq_pub_tick(pub, moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    moq_subscription_update_cfg_t ucfg; moq_subscription_update_cfg_init(&ucfg);
    ucfg.has_forward = true; ucfg.forward = false;
    moq_session_update_subscription(cl, sub_h, &ucfg, moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    if (use_manual) manual_forward(pub, sv, moq_simpair_now_us(sp), 0, NULL, NULL);
    else moq_pub_tick(pub, moq_simpair_now_us(sp));

    *joined = cbs.joined;
    *updated = cbs.updated;
    *active = moq_pub_active_subscriptions(pub, track);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

void test_pub_manual_vs_tick_parity(void) {
    int mj, mu; size_t ma;
    int tj, tu; size_t ta;
    parity_run(true, &mj, &mu, &ma);
    parity_run(false, &tj, &tu, &ta);
    MOQ_TEST_CHECK_EQ_INT(mj, tj);   /* joined callbacks equal */
    MOQ_TEST_CHECK_EQ_INT(mu, tu);   /* updated callbacks equal */
    MOQ_TEST_CHECK_EQ_SIZE(ma, ta);  /* active subscriptions equal */
    MOQ_TEST_CHECK_EQ_INT(mj, 1);
    MOQ_TEST_CHECK_EQ_INT(mu, 1);
    MOQ_TEST_CHECK_EQ_SIZE(ma, (size_t)1);
    MOQ_TEST_PASS("pub_manual_vs_tick_parity");
}

/* a Forward-0 update on a subscription with an OPEN subgroup must
 * DRIVE the RESET, not merely arm it. This test deliberately fills the action
 * queue first so the retire cannot complete inline: handle_event returns
 * CONSUMED + WOULD_BLOCK, and a later flush (no event replay) drives exactly
 * one RESET. The slot stays active (pause) and Forward=1 resumes delivery.
 * Pre-refactor the manual path only armed and returned MOQ_OK, so no RESET
 * ever went out. */
void test_pub_manual_forward_zero_resets_open_subgroup(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);
    moq_session_t *sv = moq_simpair_server(sp);
    moq_session_t *cl = moq_simpair_client(sp);
    moq_pub_cfg_t cfg; moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    moq_publisher_t *pub = NULL;
    moq_pub_create(sv, &alloc, &cfg, &pub);

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
    moq_session_subscribe(cl, &scfg, moq_simpair_now_us(sp), &sub_h);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    manual_forward(pub, sv, moq_simpair_now_us(sp), 0, NULL, NULL);
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    /* Open a subgroup: write an object at Forward=1. */
    uint8_t d[] = { 0x81, 0x82 };
    moq_rcbuf_t *pay = NULL; moq_rcbuf_create(&alloc, d, sizeof(d), &pay);
    MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 0, 0, pay,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_rcbuf_decref(pay);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_INT(fanout_count_objects(sp, 0, 0, d, sizeof(d)), 1);
    (void)drain_count_resets(sv);   /* clear any residual actions */

    /* Forward=0 with the action queue FULL: the retire cannot complete inline,
     * so handle_event returns CONSUMED + WOULD_BLOCK (exercising the blocked
     * path -- not merely arming). */
    moq_subscription_update_cfg_t ucfg; moq_subscription_update_cfg_init(&ucfg);
    ucfg.has_forward = true; ucfg.forward = false;
    MOQ_TEST_CHECK(moq_session_update_subscription(cl, sub_h, &ucfg,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    test_session_fill_action_queue(sv);

    moq_event_t ev;
    MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
    MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_UPDATED);
    moq_pub_event_result_t res = MOQ_PUB_EVENT_IGNORED;
    moq_result_t rc = moq_pub_handle_event(pub, &ev, moq_simpair_now_us(sp), &res);
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK(rc == MOQ_ERR_WOULD_BLOCK);       /* retire blocked */
    MOQ_TEST_CHECK(res == MOQ_PUB_EVENT_CONSUMED);   /* state committed */

    /* Draining the filler frees the queue; NO RESET went out while blocked. */
    int pre = drain_count_resets(sv);
    MOQ_TEST_CHECK_EQ_INT(pre, 0);
    /* Flush (no event replay) now drives exactly one RESET_DATA to completion. */
    MOQ_TEST_CHECK(moq_pub_flush(pub, moq_simpair_now_us(sp)) == MOQ_OK);
    int resets = drain_count_resets(sv);
    MOQ_TEST_CHECK_EQ_INT(resets, 1);
    /* Flushing again is idempotent: still OK, and no duplicate RESET. */
    MOQ_TEST_CHECK(moq_pub_flush(pub, moq_simpair_now_us(sp)) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(drain_count_resets(sv), 0);
    /* PAUSE, not retire: slot still active. */
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track), 1);

    /* A write while paused delivers nothing. */
    uint8_t dp[] = { 0x8A };
    moq_rcbuf_t *pp = NULL; moq_rcbuf_create(&alloc, dp, sizeof(dp), &pp);
    MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 1, 0, pp,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_rcbuf_decref(pp);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_INT(fanout_count_objects(sp, 1, 0, dp, sizeof(dp)), 0);

    /* Forward=1 resumes delivery on a fresh group. */
    ucfg.forward = true;
    MOQ_TEST_CHECK(moq_session_update_subscription(cl, sub_h, &ucfg,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    manual_forward(pub, sv, moq_simpair_now_us(sp), 0, NULL, NULL);
    uint8_t d2[] = { 0x83, 0x84 };
    moq_rcbuf_t *pay2 = NULL; moq_rcbuf_create(&alloc, d2, sizeof(d2), &pay2);
    MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 2, 0, pay2,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_rcbuf_decref(pay2);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_INT(fanout_count_objects(sp, 2, 0, d2, sizeof(d2)), 1);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("pub_manual_forward_zero_resets_open_subgroup");
}

/* manual PUBLISH_UPDATED / PUBLISH_ERROR / PUBLISH_UNSUBSCRIBED are
 * handled (CONSUMED) through moq_pub_handle_event, driving state + callbacks;
 * an unmatched pub is IGNORED. Uses synthetic events for the terminal cases so
 * the match/result/state transition is asserted directly. */
void test_pub_manual_publish_family_results(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);
    moq_session_t *sv = moq_simpair_server(sp);
    tick_cb_state_t cbs = {0};
    moq_pub_cfg_t cfg; moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    cfg.callbacks.ctx = &cbs;
    cfg.callbacks.on_publish_ok = tick_on_publish_ok;
    moq_publisher_t *pub = NULL;
    moq_pub_create(sv, &alloc, &cfg, &pub);

    moq_pub_track_cfg_t tcfg; moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_pub_track_t *track = NULL;
    moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &track);

    moq_pub_publish_cfg_t pcfg; moq_pub_publish_cfg_init(&pcfg);
    MOQ_TEST_CHECK(moq_pub_publish_track(pub, track, &pcfg,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    for (int i = 0; i < 4; i++) {
        moq_event_t e;
        if (moq_session_poll_events(moq_simpair_client(sp), &e, 1) != 1) break;
        if (e.kind == MOQ_EVENT_PUBLISH_REQUEST) {
            moq_accept_publish_cfg_t acc; moq_accept_publish_cfg_init(&acc);
            moq_session_accept_publish(moq_simpair_client(sp),
                e.u.publish_request.pub, &acc, moq_simpair_now_us(sp));
        }
        moq_event_cleanup(&e);
    }
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    /* PUBLISH_OK via manual API -> published. Capture the publication handle
     * from the event (find_track_by_pub keys on it). */
    moq_publication_t ph; memset(&ph, 0, sizeof(ph));
    { moq_event_t e;
      while (moq_session_poll_events(sv, &e, 1) == 1) {
          if (e.kind == MOQ_EVENT_PUBLISH_OK) ph = e.u.publish_ok.pub;
          moq_pub_event_result_t rr = MOQ_PUB_EVENT_IGNORED;
          moq_pub_handle_event(pub, &e, moq_simpair_now_us(sp), &rr);
          moq_event_cleanup(&e);
      } }
    MOQ_TEST_CHECK(moq_pub_track_is_published(pub, track));

    /* Synthetic PUBLISH_UPDATED Forward=0 -> CONSUMED, no delivery slot. */
    moq_event_t upd; memset(&upd, 0, sizeof(upd));
    upd.kind = MOQ_EVENT_PUBLISH_UPDATED;
    upd.u.publish_updated.pub = ph;
    upd.u.publish_updated.has_forward = true;
    upd.u.publish_updated.forward = false;
    moq_pub_event_result_t r = MOQ_PUB_EVENT_IGNORED;
    MOQ_TEST_CHECK(moq_pub_handle_event(pub, &upd, moq_simpair_now_us(sp), &r)
        == MOQ_OK);
    MOQ_TEST_CHECK(r == MOQ_PUB_EVENT_CONSUMED);

    /* Synthetic PUBLISH_UNSUBSCRIBED -> CONSUMED, publication restartable. */
    moq_event_t un; memset(&un, 0, sizeof(un));
    un.kind = MOQ_EVENT_PUBLISH_UNSUBSCRIBED;
    un.u.publish_unsubscribed.pub = ph;
    r = MOQ_PUB_EVENT_IGNORED;
    MOQ_TEST_CHECK(moq_pub_handle_event(pub, &un, moq_simpair_now_us(sp), &r)
        == MOQ_OK);
    MOQ_TEST_CHECK(r == MOQ_PUB_EVENT_CONSUMED);
    MOQ_TEST_CHECK(!moq_pub_track_is_published(pub, track));   /* reset */

    /* Unmatched publication -> IGNORED. */
    moq_event_t err; memset(&err, 0, sizeof(err));
    err.kind = MOQ_EVENT_PUBLISH_ERROR;
    err.u.publish_error.pub = ph;   /* now stale/unmatched */
    r = MOQ_PUB_EVENT_CONSUMED;
    MOQ_TEST_CHECK(moq_pub_handle_event(pub, &err, moq_simpair_now_us(sp), &r)
        == MOQ_OK);
    MOQ_TEST_CHECK(r == MOQ_PUB_EVENT_IGNORED);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("pub_manual_publish_family_results");
}

/* with the action queue full, a serialized manual loop (process
 * one event -> drain + flush to OK -> next) accepts TWO queued subscribes
 * exactly once each. The first blocks (CONSUMED + WOULD_BLOCK, staged owned in
 * the pending slot); flushing completes it and frees the slot; the second is
 * then accepted. No event is held across a flush, so nothing is lost. */
void test_pub_manual_two_track_serialized_no_loss(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);
    moq_session_t *sv = moq_simpair_server(sp);
    moq_session_t *cl = moq_simpair_client(sp);
    tick_cb_state_t cbs = {0};
    moq_pub_cfg_t cfg; moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    cfg.callbacks.ctx = &cbs;
    cfg.callbacks.on_subscriber_joined = tick_on_joined;
    moq_publisher_t *pub = NULL;
    moq_pub_create(sv, &alloc, &cfg, &pub);

    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_pub_track_cfg_t ta; moq_pub_track_cfg_init(&ta);
    ta.track_namespace.parts = ns_parts; ta.track_namespace.count = 1;
    ta.track_name = MOQ_BYTES_LITERAL("a");
    moq_pub_track_t *trackA = NULL;
    moq_pub_add_track(pub, &ta, moq_simpair_now_us(sp), &trackA);
    moq_pub_track_cfg_t tb; moq_pub_track_cfg_init(&tb);
    tb.track_namespace.parts = ns_parts; tb.track_namespace.count = 1;
    tb.track_name = MOQ_BYTES_LITERAL("b");
    moq_pub_track_t *trackB = NULL;
    moq_pub_add_track(pub, &tb, moq_simpair_now_us(sp), &trackB);

    moq_subscribe_cfg_t sa; moq_subscribe_cfg_init(&sa);
    sa.track_namespace.parts = ns_parts; sa.track_namespace.count = 1;
    sa.track_name = MOQ_BYTES_LITERAL("a");
    moq_subscription_t ha;
    moq_session_subscribe(cl, &sa, moq_simpair_now_us(sp), &ha);
    moq_subscribe_cfg_t sb; moq_subscribe_cfg_init(&sb);
    sb.track_namespace.parts = ns_parts; sb.track_namespace.count = 1;
    sb.track_name = MOQ_BYTES_LITERAL("b");
    moq_subscription_t hb;
    moq_session_subscribe(cl, &sb, moq_simpair_now_us(sp), &hb);
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    /* Fill the server queue so the first accept blocks. */
    test_session_fill_action_queue(sv);

    /* Serialized loop: exactly the contract from the header. */
    int processed = 0;
    moq_event_t ev;
    while (moq_session_poll_events(sv, &ev, 1) == 1) {
        moq_pub_event_result_t res = MOQ_PUB_EVENT_IGNORED;
        moq_result_t rc = moq_pub_handle_event(pub, &ev, moq_simpair_now_us(sp),
                                               &res);
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) processed++;
        moq_event_cleanup(&ev);
        for (int g = 0; rc == MOQ_ERR_WOULD_BLOCK && g < 32; g++) {
            manual_drain_actions(sv);
            rc = moq_pub_flush(pub, moq_simpair_now_us(sp));
        }
        MOQ_TEST_CHECK(rc == MOQ_OK);   /* each event fully resolved */
    }
    MOQ_TEST_CHECK_EQ_INT(processed, 2);
    MOQ_TEST_CHECK_EQ_INT(cbs.joined, 2);           /* each joined once */
    MOQ_TEST_CHECK(moq_pub_has_subscriber(pub, trackA));
    MOQ_TEST_CHECK(moq_pub_has_subscriber(pub, trackB));

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("pub_manual_two_track_serialized_no_loss");
}

/* A second FIFO request has its own bounded owner while the first ACK waits;
 * both consumed events can be cleaned before output capacity returns. */
void test_pub_manual_multiple_owned_answers(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);
    moq_session_t *sv = moq_simpair_server(sp);
    moq_session_t *cl = moq_simpair_client(sp);
    moq_pub_cfg_t cfg; moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    moq_publisher_t *pub = NULL;
    moq_pub_create(sv, &alloc, &cfg, &pub);

    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_pub_track_cfg_t ta; moq_pub_track_cfg_init(&ta);
    ta.track_namespace.parts = ns_parts; ta.track_namespace.count = 1;
    ta.track_name = MOQ_BYTES_LITERAL("a");
    moq_pub_track_t *trackA = NULL;
    moq_pub_add_track(pub, &ta, moq_simpair_now_us(sp), &trackA);
    moq_pub_track_cfg_t tb; moq_pub_track_cfg_init(&tb);
    tb.track_namespace.parts = ns_parts; tb.track_namespace.count = 1;
    tb.track_name = MOQ_BYTES_LITERAL("b");
    moq_pub_track_t *trackB = NULL;
    moq_pub_add_track(pub, &tb, moq_simpair_now_us(sp), &trackB);

    moq_subscribe_cfg_t sa; moq_subscribe_cfg_init(&sa);
    sa.track_namespace.parts = ns_parts; sa.track_namespace.count = 1;
    sa.track_name = MOQ_BYTES_LITERAL("a");
    moq_subscription_t ha;
    moq_session_subscribe(cl, &sa, moq_simpair_now_us(sp), &ha);
    moq_subscribe_cfg_t sb; moq_subscribe_cfg_init(&sb);
    sb.track_namespace.parts = ns_parts; sb.track_namespace.count = 1;
    sb.track_name = MOQ_BYTES_LITERAL("b");
    moq_subscription_t hb;
    moq_session_subscribe(cl, &sb, moq_simpair_now_us(sp), &hb);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    test_session_fill_action_queue(sv);

    /* Poll and transfer both requests while the action queue stays full. */
    moq_event_t e1, e2;
    MOQ_TEST_CHECK(moq_session_poll_events(sv, &e1, 1) == 1);
    MOQ_TEST_CHECK(e1.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
    moq_pub_event_result_t r1 = MOQ_PUB_EVENT_IGNORED;
    moq_result_t rc1 = moq_pub_handle_event(pub, &e1, moq_simpair_now_us(sp), &r1);
    MOQ_TEST_CHECK(rc1 == MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(r1 == MOQ_PUB_EVENT_CONSUMED);   /* staged owned */

    MOQ_TEST_CHECK(moq_session_poll_events(sv, &e2, 1) == 1);
    MOQ_TEST_CHECK(e2.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
    moq_pub_event_result_t r2 = MOQ_PUB_EVENT_CONSUMED;
    moq_result_t rc2 = moq_pub_handle_event(pub, &e2, moq_simpair_now_us(sp), &r2);
    MOQ_TEST_CHECK(rc2 == MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(r2 == MOQ_PUB_EVENT_CONSUMED);
    moq_event_cleanup(&e1);
    moq_event_cleanup(&e2);

    /* Repeated bounded drives install both without replaying either event. */
    for (int g = 0; g < 32; g++) {
        moq_result_t rc = moq_pub_flush(pub, moq_simpair_now_us(sp));
        if (rc != MOQ_ERR_WOULD_BLOCK) break;
        manual_drain_actions(sv);
    }
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, trackA) +
                           moq_pub_active_subscriptions(pub, trackB), 2);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("pub_manual_multiple_owned_answers");
}

/* a blocked (deferred) retained-FETCH snapshot is released AT CLOSE,
 * not only at destroy, and exactly once. The retained group is cleared after
 * the fetch defers, so the pending_fetch snapshot holds the LAST refs to the
 * wrapped objects; SESSION_CLOSED (manual API) must free them. */
void test_pub_manual_close_releases_pending_fetch(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);
    moq_session_t *sv = moq_simpair_server(sp);
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);
    moq_pub_cfg_t cfg; moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;   /* serves standalone FETCH */
    moq_publisher_t *pub = NULL;
    moq_pub_create(sv, &alloc, &cfg, &pub);

    moq_pub_track_cfg_t tcfg; moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    tcfg.track_namespace.parts = ns_parts; tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("catalog");
    moq_pub_track_t *track = NULL;
    moq_pub_add_track(pub, &tcfg, now, &track);

    /* Retain a 3-object group with wrapped (release-counted) payloads. */
    g_wrap_releases = 0;
    static const uint8_t b0[]={'a'}, b1[]={'b'}, b2[]={'c'};
    moq_rcbuf_t *p0=NULL,*p1=NULL,*p2=NULL;
    moq_rcbuf_wrap(&alloc, b0, 1, count_release, NULL, &p0);
    moq_rcbuf_wrap(&alloc, b1, 1, count_release, NULL, &p1);
    moq_rcbuf_wrap(&alloc, b2, 1, count_release, NULL, &p2);
    moq_pub_retained_object_t objs[3] = {
        { .object_id = 0, .payload = p0 },
        { .object_id = 1, .payload = p1 },
        { .object_id = 2, .payload = p2, .end_of_group = true },
    };
    moq_pub_retained_group_cfg_t gc; moq_pub_retained_group_cfg_init(&gc);
    gc.group_id = 7; gc.objects = objs; gc.object_count = 3;
    MOQ_TEST_CHECK(moq_pub_set_retained_group(pub, track, &gc) == MOQ_OK);
    moq_rcbuf_decref(p0); moq_rcbuf_decref(p1); moq_rcbuf_decref(p2);

    /* Client fetches the whole retained group. */
    moq_fetch_cfg_t fc; moq_fetch_cfg_init(&fc);
    fc.track_namespace.parts = ns_parts; fc.track_namespace.count = 1;
    fc.track_name = MOQ_BYTES_LITERAL("catalog");
    fc.start_group = 7; fc.start_object = 0;
    fc.end_group = 7; fc.end_object = 3;
    moq_fetch_t fh;
    MOQ_TEST_CHECK(moq_session_fetch(cl, &fc, now, &fh) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    /* Fill the queue so serving the fetch DEFERS (snapshot held, no output). */
    test_session_fill_action_queue(sv);
    moq_event_t ev;
    bool saw_fetch = false;
    while (moq_session_poll_events(sv, &ev, 1) == 1) {
        if (ev.kind == MOQ_EVENT_FETCH_REQUEST) {
            saw_fetch = true;
            moq_pub_event_result_t r = MOQ_PUB_EVENT_IGNORED;
            moq_result_t rc = moq_pub_handle_event(pub, &ev, now, &r);
            MOQ_TEST_CHECK(rc == MOQ_ERR_WOULD_BLOCK);   /* deferred */
            MOQ_TEST_CHECK(r == MOQ_PUB_EVENT_CONSUMED);
        }
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK(saw_fetch);

    /* Clear the retained group: the pending-fetch snapshot now holds the ONLY
     * refs to the wrapped payloads, so nothing has been released yet. */
    MOQ_TEST_CHECK(moq_pub_clear_retained_group(pub, track) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(g_wrap_releases, 0);

    /* SESSION_CLOSED via the manual API: pub_close_local frees the pending
     * FETCH snapshot -> all three wrapped payloads released, AT CLOSE. */
    moq_event_t close_ev; memset(&close_ev, 0, sizeof(close_ev));
    close_ev.kind = MOQ_EVENT_SESSION_CLOSED;
    moq_pub_event_result_t cr = MOQ_PUB_EVENT_CONSUMED;
    moq_pub_handle_event(pub, &close_ev, now, &cr);
    MOQ_TEST_CHECK(cr == MOQ_PUB_EVENT_IGNORED);   /* broadcast */
    MOQ_TEST_CHECK_EQ_INT(g_wrap_releases, 3);     /* released at close */

    moq_pub_destroy(pub);
    MOQ_TEST_CHECK_EQ_INT(g_wrap_releases, 3);     /* exactly once, no double */
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("pub_manual_close_releases_pending_fetch");
}

/* a MATCHED pre-accept PUBLISH_ERROR (peer rejected the PUBLISH)
 * through the manual API is CONSUMED, fires on_publish_error once, clears the
 * publication state, and a subsequent publish emits a fresh request. */
void test_pub_manual_publish_error_matched(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);
    moq_session_t *sv = moq_simpair_server(sp);
    moq_session_t *cl = moq_simpair_client(sp);
    tick_cb_state_t cbs = {0};
    moq_pub_cfg_t cfg; moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    cfg.callbacks.ctx = &cbs;
    cfg.callbacks.on_publish_error = tick_on_publish_error;
    moq_publisher_t *pub = NULL;
    moq_pub_create(sv, &alloc, &cfg, &pub);

    moq_pub_track_cfg_t tcfg; moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_pub_track_t *track = NULL;
    moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &track);

    moq_pub_publish_cfg_t pcfg; moq_pub_publish_cfg_init(&pcfg);
    MOQ_TEST_CHECK(moq_pub_publish_track(pub, track, &pcfg,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    /* Client REJECTS the PUBLISH -> server sees PUBLISH_ERROR (pre-accept). */
    for (int i = 0; i < 4; i++) {
        moq_event_t e;
        if (moq_session_poll_events(cl, &e, 1) != 1) break;
        if (e.kind == MOQ_EVENT_PUBLISH_REQUEST) {
            moq_reject_publish_cfg_t rej; moq_reject_publish_cfg_init(&rej);
            rej.error_code = MOQ_REQUEST_ERROR_UNAUTHORIZED;
            moq_session_reject_publish(cl, e.u.publish_request.pub, &rej,
                moq_simpair_now_us(sp));
        }
        moq_event_cleanup(&e);
    }
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    /* PUBLISH_ERROR forwarded through the manual API: CONSUMED + callback. */
    int seen = 0; moq_pub_event_result_t res = MOQ_PUB_EVENT_IGNORED;
    manual_forward(pub, sv, moq_simpair_now_us(sp),
                   MOQ_EVENT_PUBLISH_ERROR, &seen, &res);
    MOQ_TEST_CHECK_EQ_INT(seen, 1);
    MOQ_TEST_CHECK(res == MOQ_PUB_EVENT_CONSUMED);
    MOQ_TEST_CHECK_EQ_INT(cbs.publish_error, 1);
    MOQ_TEST_CHECK(!moq_pub_track_is_published(pub, track));   /* state cleared */

    /* A subsequent publish emits a FRESH PUBLISH request (not a no-op). */
    MOQ_TEST_CHECK(moq_pub_publish_track(pub, track, &pcfg,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    bool fresh_request = false;
    { moq_event_t e;
      while (moq_session_poll_events(cl, &e, 1) == 1) {
          if (e.kind == MOQ_EVENT_PUBLISH_REQUEST) fresh_request = true;
          moq_event_cleanup(&e);
      } }
    MOQ_TEST_CHECK(fresh_request);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("pub_manual_publish_error_matched");
}
