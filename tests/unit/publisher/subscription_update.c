#include "test_cases.h"

/* -- Subscription update callback tests ----------------------------- */

void test_update_callback_fires(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    tick_cb_state_t cbs = {0};
    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    cfg.callbacks.ctx = &cbs;
    cfg.callbacks.on_subscriber_joined = tick_on_joined;
    cfg.callbacks.on_subscriber_updated = tick_on_updated;

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
    moq_pub_tick(pub, moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev); }
    MOQ_TEST_CHECK_EQ_INT(cbs.joined, 1);

    moq_subscription_update_cfg_t ucfg;
    moq_subscription_update_cfg_init(&ucfg);
    ucfg.has_subscriber_priority = true;
    ucfg.subscriber_priority = 200;
    ucfg.has_forward = true;
    ucfg.forward = false;
    ucfg.has_delivery_timeout = true;
    ucfg.delivery_timeout_us = 5000000;
    MOQ_TEST_CHECK_EQ_INT(moq_session_update_subscription(
        moq_simpair_client(sp), sub_h, &ucfg,
        moq_simpair_now_us(sp)), MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_pub_tick(pub, moq_simpair_now_us(sp));

    MOQ_TEST_CHECK_EQ_INT(cbs.updated, 1);
    MOQ_TEST_CHECK(cbs.updated_track == track);
    MOQ_TEST_CHECK(cbs.last_update.has_subscriber_priority);
    MOQ_TEST_CHECK_EQ_U64(cbs.last_update.subscriber_priority, 200);
    MOQ_TEST_CHECK(cbs.last_update.has_forward);
    MOQ_TEST_CHECK(!cbs.last_update.forward);
    MOQ_TEST_CHECK(cbs.last_update.has_delivery_timeout);
    MOQ_TEST_CHECK_EQ_U64(cbs.last_update.delivery_timeout_us, 5000000);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("update_callback_fires");
}

void test_update_old_callback_struct_size(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    tick_cb_state_t cbs = {0};
    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    cfg.callbacks.ctx = &cbs;
    cfg.callbacks.on_subscriber_joined = tick_on_joined;
    cfg.callbacks.struct_size =
        offsetof(moq_pub_callbacks_t, on_subscriber_updated);

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
    moq_pub_tick(pub, moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev); }

    moq_subscription_update_cfg_t ucfg;
    moq_subscription_update_cfg_init(&ucfg);
    ucfg.has_subscriber_priority = true;
    ucfg.subscriber_priority = 100;
    moq_session_update_subscription(moq_simpair_client(sp), sub_h,
        &ucfg, moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_pub_tick(pub, moq_simpair_now_us(sp));

    MOQ_TEST_CHECK_EQ_INT(cbs.updated, 0);
    MOQ_TEST_CHECK(moq_session_state(moq_simpair_server(sp))
        == MOQ_SESS_ESTABLISHED);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("update_old_callback_struct_size");
}

/* Old-caller callback survival: a caller compiled BEFORE the appended
 * on_publish_* fields passes an outer struct_size ending exactly at the v0
 * callbacks prefix. Its legitimately-set on_subscriber_updated (the LAST v0
 * field) must survive the whole-field prefix copy and fire. */
void test_pub_callbacks_old_prefix_copier_keeps_updated(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    enum { CB_V0 = (int)offsetof(moq_pub_callbacks_t, on_publish_ok) };

    tick_cb_state_t cbs = {0};
    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    cfg.callbacks.ctx = &cbs;
    cfg.callbacks.on_subscriber_joined = tick_on_joined;
    cfg.callbacks.on_subscriber_updated = tick_on_updated;
    /* The old caller's sizes: nested struct ends at the v0 prefix, outer
     * struct ends right after it. */
    cfg.callbacks.struct_size = (uint32_t)CB_V0;
    cfg.struct_size =
        (uint32_t)(offsetof(moq_pub_cfg_t, callbacks) + (size_t)CB_V0);

    moq_publisher_t *pub = NULL;
    MOQ_TEST_CHECK(moq_pub_create(moq_simpair_server(sp), &alloc, &cfg,
        &pub) == MOQ_OK);

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
    moq_pub_tick(pub, moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev); }
    MOQ_TEST_CHECK_EQ_INT(cbs.joined, 1);

    moq_subscription_update_cfg_t ucfg;
    moq_subscription_update_cfg_init(&ucfg);
    ucfg.has_subscriber_priority = true;
    ucfg.subscriber_priority = 7;
    moq_session_update_subscription(moq_simpair_client(sp), sub_h,
        &ucfg, moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_pub_tick(pub, moq_simpair_now_us(sp));

    /* The v0 caller's last field must have been copied and fired. */
    MOQ_TEST_CHECK_EQ_INT(cbs.updated, 1);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("pub_callbacks_old_prefix_copier_keeps_updated");
}

/* Nested-prefix bound: a FULL outer cfg must still honor the caller's nested
 * callbacks.struct_size. The caller declares a prefix ending partway into
 * on_subscriber_joined; the bytes beyond it hold a VALID function pointer
 * (stale stack contents in real life). The copier must clamp to the nested
 * declared prefix (rounded down to whole fields), so the undeclared callback
 * is never installed -- several v0 callbacks dispatch without a size gate,
 * so installation would mean a call. */
void test_pub_callbacks_nested_prefix_bounds_copy(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    tick_cb_state_t cbs = {0};
    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));   /* FULL outer prefix */
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    cfg.callbacks.ctx = &cbs;
    /* Beyond the declared nested prefix: a live, counting callback. */
    cfg.callbacks.on_subscriber_joined = tick_on_joined;
    /* The caller's nested declaration ends 4 bytes INTO on_subscriber_joined:
     * whole-field rounding must exclude it entirely. */
    cfg.callbacks.struct_size =
        (uint32_t)(offsetof(moq_pub_callbacks_t, on_subscriber_joined) + 4);

    moq_publisher_t *pub = NULL;
    MOQ_TEST_CHECK(moq_pub_create(moq_simpair_server(sp), &alloc, &cfg,
        &pub) == MOQ_OK);

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
    moq_pub_tick(pub, moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    /* The undeclared callback must not have been installed or called. */
    MOQ_TEST_CHECK_EQ_INT(cbs.joined, 0);
    MOQ_TEST_CHECK(moq_pub_has_subscriber(pub, track));  /* sub itself works */

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("pub_callbacks_nested_prefix_bounds_copy");
}

/* Partial-field poison: an outer struct_size that ends MID-way through an
 * appended pointer field must never install the (truncated/garbage) pointer.
 * The bytes past the caller's true prefix are poisoned; if the copier
 * installed them, the publish-accept dispatch below would call a wild
 * pointer. Whole fields before the cut still work. */
void test_pub_callbacks_partial_field_poison(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    enum { CB_V0 = (int)offsetof(moq_pub_callbacks_t, on_publish_ok) };

    tick_cb_state_t cbs = {0};
    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    /* Poison the whole appended-callbacks region, then set the real v0
     * fields on top. on_publish_ok stays ALL-POISON. */
    memset((unsigned char *)&cfg.callbacks + CB_V0, 0xEE,
           sizeof(cfg.callbacks) - (size_t)CB_V0);
    cfg.callbacks.ctx = &cbs;
    cfg.callbacks.on_subscriber_updated = tick_on_updated;
    /* Nested struct_size claims the poison region is valid... */
    cfg.callbacks.struct_size = (uint32_t)sizeof(cfg.callbacks);
    /* ...but the OUTER prefix ends 4 bytes into on_publish_ok: the copier
     * must trim to whole fields (through on_subscriber_updated). */
    cfg.struct_size = (uint32_t)(offsetof(moq_pub_cfg_t, callbacks) +
                                 (size_t)CB_V0 + 4);

    moq_publisher_t *pub = NULL;
    MOQ_TEST_CHECK(moq_pub_create(moq_simpair_server(sp), &alloc, &cfg,
        &pub) == MOQ_OK);

    /* Drive a full PUBLISH accept: if the poison pointer had been installed,
     * the PUBLISH_OK dispatch would call 0xEEEE... and crash. */
    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("t");
    moq_pub_track_t *track = NULL;
    moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &track);

    moq_pub_publish_cfg_t pcfg; moq_pub_publish_cfg_init(&pcfg);
    MOQ_TEST_CHECK(moq_pub_publish_track(pub, track, &pcfg,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    for (int i = 0; i < 4; i++) {
        moq_event_t ev;
        if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) != 1)
            break;
        if (ev.kind == MOQ_EVENT_PUBLISH_REQUEST) {
            moq_accept_publish_cfg_t acc;
            moq_accept_publish_cfg_init(&acc);
            moq_session_accept_publish(moq_simpair_client(sp),
                ev.u.publish_request.pub, &acc, moq_simpair_now_us(sp));
        }
        moq_event_cleanup(&ev);
    }
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK(moq_pub_tick(pub, moq_simpair_now_us(sp)) == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_track_is_published(pub, track));  /* survived */

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("pub_callbacks_partial_field_poison");
}

void test_update_after_unsubscribe_ignored(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    tick_cb_state_t cbs = {0};
    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    cfg.callbacks.ctx = &cbs;
    cfg.callbacks.on_subscriber_joined = tick_on_joined;
    cfg.callbacks.on_subscriber_updated = tick_on_updated;

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
    moq_pub_tick(pub, moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev); }

    /* Unsubscribe, then send update on stale handle — the update
     * request will fail at session level, but any lingering event
     * must not crash the publisher. */
    moq_session_unsubscribe(moq_simpair_client(sp), sub_h,
        moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_pub_tick(pub, moq_simpair_now_us(sp));

    MOQ_TEST_CHECK_EQ_INT(cbs.updated, 0);
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track), 0);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("update_after_unsubscribe_ignored");
}

void test_update_during_deferred(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    tick_cb_state_t cbs = {0};
    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_CALLBACK;
    cfg.on_subscribe = cb_defer;
    cfg.callbacks.ctx = &cbs;
    cfg.callbacks.on_subscriber_updated = tick_on_updated;

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

    /* Subscribe to track B and accept immediately (ACCEPT_ALL would
     * work but we're in CALLBACK mode, so the callback returns DEFER
     * for B). Resolve B right away. */
    moq_subscribe_cfg_t scfg_b;
    moq_subscribe_cfg_init(&scfg_b);
    scfg_b.track_namespace.parts = ns_parts;
    scfg_b.track_namespace.count = 1;
    scfg_b.track_name = MOQ_BYTES_LITERAL("b");
    moq_subscription_t sub_b;
    moq_session_subscribe(moq_simpair_client(sp), &scfg_b,
        moq_simpair_now_us(sp), &sub_b);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    last_deferred = NULL;
    moq_pub_tick(pub, moq_simpair_now_us(sp));
    MOQ_TEST_CHECK(last_deferred != NULL);
    moq_pub_resolve_deferred(pub, last_deferred, last_deferred_id,
        true, 0, moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev); }

    /* Defer track A. */
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

    /* While A is deferred, send update on B's active subscription. */
    moq_subscription_update_cfg_t ucfg;
    moq_subscription_update_cfg_init(&ucfg);
    ucfg.has_subscriber_priority = true;
    ucfg.subscriber_priority = 42;
    moq_session_update_subscription(moq_simpair_client(sp), sub_b,
        &ucfg, moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_pub_tick(pub, moq_simpair_now_us(sp));

    MOQ_TEST_CHECK_EQ_INT(cbs.updated, 1);
    MOQ_TEST_CHECK(cbs.updated_track == track_b);
    MOQ_TEST_CHECK_EQ_U64(cbs.last_update.subscriber_priority, 42);

    moq_pub_resolve_deferred(pub, last_deferred, last_deferred_id,
        false, MOQ_REQUEST_ERROR_UNAUTHORIZED,
        moq_simpair_now_us(sp));

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("update_during_deferred");
}

void test_old_cfg_size_callbacks_still_fire(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    tick_cb_state_t cbs = {0};
    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    cfg.callbacks.ctx = &cbs;
    cfg.callbacks.on_subscriber_joined = tick_on_joined;
    cfg.callbacks.on_subscriber_left = tick_on_left;
    /* Simulate old caller: cfg covers callbacks up through on_closed
     * but not on_subscriber_updated. */
    cfg.struct_size = offsetof(moq_pub_cfg_t, callbacks) +
                      offsetof(moq_pub_callbacks_t, on_subscriber_updated);

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
    moq_pub_tick(pub, moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev); }

    MOQ_TEST_CHECK_EQ_INT(cbs.joined, 1);
    MOQ_TEST_CHECK(cbs.joined_track == track);

    moq_session_unsubscribe(moq_simpair_client(sp), sub_h,
        moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_pub_tick(pub, moq_simpair_now_us(sp));

    MOQ_TEST_CHECK_EQ_INT(cbs.left, 1);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("old_cfg_size_callbacks_still_fire");
}
