#include "test_cases.h"

/* -- Tests -------------------------------------------------------- */

void test_create_destroy(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;

    moq_publisher_t *pub = NULL;
    moq_result_t rc = moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub);
    MOQ_TEST_CHECK(rc == MOQ_OK);
    MOQ_TEST_CHECK(pub != NULL);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("create_destroy");
}

/* Real old-storage ABI canary: a caller compiled against the original struct
 * allocated only the frozen v0 prefix (struct_size, accept_mode,
 * default_publisher_priority), then its own bytes immediately after. Pointer-only
 * cfg_init must clear/stamp ONLY that prefix -- the old memset(sizeof current)
 * would write the appended region (on_subscribe / on_subscribe_ctx / callbacks)
 * and clobber the byte right after the old struct. The union's first member
 * forces alignment for the cfg* cast; the cfg is never accessed through the full
 * struct type (the storage is old-sized). */
void test_pub_cfg_init_old_prefix_no_overflow(void) {
    enum { V0 = (int)(offsetof(moq_pub_cfg_t, default_publisher_priority) +
                      sizeof(((moq_pub_cfg_t *)0)->default_publisher_priority)) };
    union {
        moq_pub_cfg_t aligner;   /* alignment only */
        struct {
            unsigned char prefix[V0];
            uint64_t canary;     /* the old caller's next bytes */
        } box;
    } u;
    memset(&u, 0xAB, sizeof(u));
    moq_pub_cfg_init((moq_pub_cfg_t *)&u.box);

    uint32_t ss;
    memcpy(&ss, u.box.prefix, sizeof(ss));   /* struct_size at offset 0 */
    MOQ_TEST_CHECK(ss == (uint32_t)V0);
    MOQ_TEST_CHECK(u.box.canary == 0xABABABABABABABABULL);  /* not overflowed */

    /* Sized init on the full struct clears + stamps everything and enables the
     * appended fields. */
    moq_pub_cfg_t full;
    memset(&full, 0xAB, sizeof(full));
    moq_pub_cfg_init_sized(&full, sizeof(full));
    MOQ_TEST_CHECK(full.struct_size == sizeof(full));
    MOQ_TEST_CHECK(full.accept_mode == MOQ_PUB_REJECT_ALL);
    MOQ_TEST_CHECK(full.default_publisher_priority == 128);
    MOQ_TEST_CHECK(full.on_subscribe == NULL);
    MOQ_TEST_PASS("pub_cfg_init_old_prefix_no_overflow");
}

/* The pointer-only moq_pub_callbacks_init must clear/stamp ONLY the frozen v0
 * prefix (through on_subscriber_updated): a caller compiled before the
 * appended on_publish_* fields hands it exactly that much storage. */
void test_pub_callbacks_init_old_prefix_no_overflow(void) {
    enum { CB_V0 = (int)offsetof(moq_pub_callbacks_t, on_publish_ok) };
    union {
        moq_pub_callbacks_t aligner;   /* alignment only */
        struct {
            unsigned char prefix[CB_V0];
            uint64_t canary;           /* the old caller's next bytes */
        } box;
    } u;
    memset(&u, 0xAB, sizeof(u));
    moq_pub_callbacks_init((moq_pub_callbacks_t *)&u.box);

    uint32_t ss;
    memcpy(&ss, u.box.prefix, sizeof(ss));   /* struct_size at offset 0 */
    MOQ_TEST_CHECK(ss == (uint32_t)CB_V0);
    MOQ_TEST_CHECK(u.box.canary == 0xABABABABABABABABULL);  /* not overflowed */

    /* Sized init on the full struct enables the appended fields. */
    moq_pub_callbacks_t full;
    memset(&full, 0xAB, sizeof(full));
    moq_pub_callbacks_init_sized(&full, sizeof(full));
    MOQ_TEST_CHECK(full.struct_size == sizeof(full));
    MOQ_TEST_CHECK(full.on_publish_ok == NULL);
    MOQ_TEST_CHECK(full.on_publish_finished == NULL);

    /* Oversized-future caller: clamped to this build's sizeof. */
    moq_pub_callbacks_t over;
    moq_pub_callbacks_init_sized(&over, sizeof(over) + 64);
    MOQ_TEST_CHECK(over.struct_size == sizeof(over));
    MOQ_TEST_PASS("pub_callbacks_init_old_prefix_no_overflow");
}

void test_add_remove_track(void) {
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
    tcfg.advertise_namespace = true;

    moq_pub_track_t *track = NULL;
    moq_result_t rc = moq_pub_add_track(pub, &tcfg,
        moq_simpair_now_us(sp), &track);
    MOQ_TEST_CHECK(rc == MOQ_OK);
    MOQ_TEST_CHECK(track != NULL);

    /* Pump PUBLISH_NAMESPACE to client, client accepts. */
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_event_t ev;
    if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1) {
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED);
        moq_accept_namespace_cfg_t nacc;
        moq_accept_namespace_cfg_init(&nacc);
        moq_session_accept_namespace(moq_simpair_client(sp),
            ev.u.namespace_published.ann, &nacc, moq_simpair_now_us(sp));
        moq_event_cleanup(&ev);
    }

    /* Pump NAMESPACE_ACCEPTED to server, forward to facade. */
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
        moq_pub_event_result_t res;
        moq_pub_handle_event(pub, &ev, moq_simpair_now_us(sp), &res);
        moq_event_cleanup(&ev);
    }

    rc = moq_pub_remove_track(pub, track, moq_simpair_now_us(sp));
    MOQ_TEST_CHECK(rc == MOQ_OK);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("add_remove_track");
}

void test_subscribe_accept_all(void) {
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
    tcfg.advertise_namespace = true;

    moq_pub_track_t *track = NULL;
    moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &track);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev); }

    /* Client subscribes. */
    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace.parts = ns_parts;
    scfg.track_namespace.count = 1;
    scfg.track_name = MOQ_BYTES_LITERAL("video");

    moq_subscription_t sub_h;
    moq_result_t rc = moq_session_subscribe(moq_simpair_client(sp), &scfg,
        moq_simpair_now_us(sp), &sub_h);
    MOQ_TEST_CHECK(rc == MOQ_OK);

    moq_simpair_run_until_quiescent(sp, 8, NULL);

    /* Server gets SUBSCRIBE_REQUEST, forward to facade. */
    moq_event_t ev;
    if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        moq_pub_event_result_t res;
        rc = moq_pub_handle_event(pub, &ev, moq_simpair_now_us(sp), &res);
        MOQ_TEST_CHECK(rc == MOQ_OK);
        MOQ_TEST_CHECK(res == MOQ_PUB_EVENT_CONSUMED);
        moq_event_cleanup(&ev);
    }

    MOQ_TEST_CHECK(moq_pub_active_subscriptions(pub, track) == 1);

    /* Pump SUBSCRIBE_OK to client. */
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1) {
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_OK);
        moq_event_cleanup(&ev);
    }

    moq_pub_remove_track(pub, track, moq_simpair_now_us(sp));
    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("subscribe_accept_all");
}

/* Removing a track must retire its accepted publisher-side subscriptions in the
 * session, not just free the facade track -- otherwise the session subscription
 * stays pinned and, with a small pool, a fresh subscribe is refused. */
void test_remove_track_retires_subscription(void) {
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    moq_simpair_cfg_t spcfg = MOQ_SIMPAIR_CFG_INIT;
    spcfg.alloc = &alloc;
    spcfg.seed = 42;
    spcfg.initial_now_us = 1000;
    spcfg.client_send_request_capacity = true;
    spcfg.client_initial_request_capacity = 16;
    spcfg.server_send_request_capacity = true;
    spcfg.server_initial_request_capacity = 16;
    spcfg.server_max_subscriptions = 1;   /* pool of one: a leak exhausts it */
    moq_simpair_t *sp = NULL;
    MOQ_TEST_CHECK(moq_simpair_create(&spcfg, &sp) == MOQ_OK);
    moq_simpair_start(sp);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev);
      while (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev); }

    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    moq_publisher_t *pub = NULL;
    moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub);

    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };

    /* Two rounds: add track + subscribe + accept, then remove. The second
     * round only succeeds if the first round's subscription was retired in
     * the session (pool size 1). */
    for (int round = 0; round < 2; round++) {
        moq_pub_track_cfg_t tcfg;
        moq_pub_track_cfg_init(&tcfg);
        tcfg.track_namespace.parts = ns_parts;
        tcfg.track_namespace.count = 1;
        tcfg.track_name = MOQ_BYTES_LITERAL("video");
        tcfg.advertise_namespace = true;

        moq_pub_track_t *track = NULL;
        MOQ_TEST_CHECK(moq_pub_add_track(pub, &tcfg,
            moq_simpair_now_us(sp), &track) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);

        /* Client accepts the namespace, server facade consumes the accept. */
        moq_event_t ev;
        while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED) {
                moq_accept_namespace_cfg_t nacc;
                moq_accept_namespace_cfg_init(&nacc);
                moq_session_accept_namespace(moq_simpair_client(sp),
                    ev.u.namespace_published.ann, &nacc, moq_simpair_now_us(sp));
            }
            moq_event_cleanup(&ev);
        }
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        while (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
            moq_pub_event_result_t res;
            moq_pub_handle_event(pub, &ev, moq_simpair_now_us(sp), &res);
            moq_event_cleanup(&ev);
        }

        /* Client subscribes. */
        moq_subscribe_cfg_t scfg;
        moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace.parts = ns_parts;
        scfg.track_namespace.count = 1;
        scfg.track_name = MOQ_BYTES_LITERAL("video");
        moq_subscription_t sub_h;
        MOQ_TEST_CHECK(moq_session_subscribe(moq_simpair_client(sp), &scfg,
            moq_simpair_now_us(sp), &sub_h) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);

        /* Server must surface SUBSCRIBE_REQUEST (pool has room) and the facade
         * accepts it. If the prior round's subscription leaked, the pool is full
         * and the session refuses the subscribe -- no SUBSCRIBE_REQUEST here. */
        bool saw_req = false;
        while (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                saw_req = true;
                moq_pub_event_result_t res;
                moq_pub_handle_event(pub, &ev, moq_simpair_now_us(sp), &res);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(saw_req);
        MOQ_TEST_CHECK(moq_pub_active_subscriptions(pub, track) == 1);

        /* Client must see SUBSCRIBE_OK, not SUBSCRIBE_ERROR. */
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        bool saw_ok = false, saw_err = false;
        while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_OK) saw_ok = true;
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_ERROR) saw_err = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(saw_ok);
        MOQ_TEST_CHECK(!saw_err);

        /* Remove the track; this must retire the session subscription and
         * free the pool slot for the next round. */
        MOQ_TEST_CHECK(moq_pub_remove_track(pub, track,
            moq_simpair_now_us(sp)) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);
    }

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("remove_track_retires_subscription");
}

void test_subscribe_no_match(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;

    moq_publisher_t *pub = NULL;
    moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub);

    /* No tracks added. Forward a SUBSCRIBE_REQUEST. */
    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("audio");
    tcfg.advertise_namespace = false;
    moq_pub_track_t *track = NULL;
    moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &track);

    /* Subscribe to a different name. */
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
        moq_pub_handle_event(pub, &ev, moq_simpair_now_us(sp), &res);
        MOQ_TEST_CHECK(res == MOQ_PUB_EVENT_IGNORED);
        moq_event_cleanup(&ev);
    }

    moq_pub_remove_track(pub, track, moq_simpair_now_us(sp));
    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("subscribe_no_match");
}

void test_subscribe_duplicate_rejected(void) {
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

    /* First subscribe — accepted. */
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
        moq_pub_handle_event(pub, &ev, moq_simpair_now_us(sp), &res);
        MOQ_TEST_CHECK(res == MOQ_PUB_EVENT_CONSUMED);
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK(moq_pub_active_subscriptions(pub, track) == 1);

    /* Pump accept to client, drain. */
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t d;
      if (moq_session_poll_events(moq_simpair_client(sp), &d, 1) == 1)
          moq_event_cleanup(&d); }

    /* Second subscribe — should be rejected (v1: one sub per track). */
    moq_subscription_t sub_h2;
    moq_session_subscribe(moq_simpair_client(sp), &scfg,
        moq_simpair_now_us(sp), &sub_h2);
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
        moq_pub_event_result_t res;
        moq_pub_handle_event(pub, &ev, moq_simpair_now_us(sp), &res);
        MOQ_TEST_CHECK(res == MOQ_PUB_EVENT_CONSUMED);
        moq_event_cleanup(&ev);
    }

    /* Client should get error for second subscribe. */
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
    MOQ_TEST_PASS("subscribe_duplicate_rejected");
}

void test_write_single_sub(void) {
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

    /* Subscribe and accept. */
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
      }
    }
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev); }

    /* Write an object. */
    uint8_t data[] = { 0xDE, 0xAD, 0xBE, 0xEF };
    moq_rcbuf_t *payload = NULL;
    moq_rcbuf_create(&alloc, data, sizeof(data), &payload);

    moq_result_t rc = moq_pub_write_object(pub, track, 0, 0, payload,
        moq_simpair_now_us(sp));
    MOQ_TEST_CHECK(rc == MOQ_OK);
    moq_rcbuf_decref(payload);

    /* Pump data to client. */
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    /* Client should receive the object. */
    bool got_object = false;
    moq_event_t evts[8];
    size_t ne = moq_session_poll_events(moq_simpair_client(sp), evts, 8);
    for (size_t i = 0; i < ne; i++) {
        if (evts[i].kind == MOQ_EVENT_OBJECT_RECEIVED) {
            got_object = true;
            MOQ_TEST_CHECK(evts[i].u.object_received.group_id == 0);
            MOQ_TEST_CHECK(evts[i].u.object_received.object_id == 0);
            MOQ_TEST_CHECK(moq_rcbuf_len(evts[i].u.object_received.payload) == 4);
            MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(evts[i].u.object_received.payload),
                                   data, 4) == 0);
        }
        moq_event_cleanup(&evts[i]);
    }
    MOQ_TEST_CHECK(got_object);

    moq_pub_remove_track(pub, track, moq_simpair_now_us(sp));
    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("write_single_sub");
}

/* Stream keys add an action to END_OF_TRACK's open/object/FIN sequence.
 * With three slots free, end_track retains partial progress and completes
 * after capacity returns; it must not strand the subgroup without FIN. */
void test_end_track_with_stream_keys(void)
{
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    moq_simpair_cfg_t scfg_pair = MOQ_SIMPAIR_CFG_INIT;
    scfg_pair.alloc = &alloc;
    scfg_pair.seed = 42;
    scfg_pair.initial_now_us = 1000;
    scfg_pair.client_send_request_capacity = true;
    scfg_pair.client_initial_request_capacity = 16;
    scfg_pair.server_send_request_capacity = true;
    scfg_pair.server_initial_request_capacity = 16;
    scfg_pair.max_actions = 4;
    moq_simpair_t *sp = NULL;
    MOQ_TEST_CHECK(moq_simpair_create(&scfg_pair, &sp) == MOQ_OK);
    if (!sp) return;
    moq_simpair_start(sp);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_session_t *srv = moq_simpair_server(sp);
    moq_event_t ev;
    while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
        moq_event_cleanup(&ev);
    while (moq_session_poll_events(srv, &ev, 1) == 1) moq_event_cleanup(&ev);
    MOQ_TEST_CHECK(moq_session_set_data_priority_updates(srv, true) == MOQ_OK);

    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    moq_publisher_t *pub = NULL;
    moq_pub_create(srv, &alloc, &cfg, &pub);
    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_pub_track_t *track = NULL;
    moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &track);

    moq_subscribe_cfg_t sub;
    moq_subscribe_cfg_init(&sub);
    sub.track_namespace.parts = ns_parts;
    sub.track_namespace.count = 1;
    sub.track_name = MOQ_BYTES_LITERAL("video");
    moq_subscription_t sub_h;
    moq_session_subscribe(moq_simpair_client(sp), &sub, moq_simpair_now_us(sp),
                          &sub_h);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    while (moq_session_poll_events(srv, &ev, 1) == 1) {
        moq_pub_event_result_t res;
        moq_pub_handle_event(pub, &ev, moq_simpair_now_us(sp), &res);
        moq_event_cleanup(&ev);
    }
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    /* One unrelated action leaves three slots. */
    moq_subscribe_cfg_t other = sub;
    other.track_name = MOQ_BYTES_LITERAL("other");
    moq_subscription_t other_h;
    MOQ_TEST_CHECK(moq_session_subscribe(srv, &other, moq_simpair_now_us(sp),
                                         &other_h) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(moq_session_action_capacity(srv), 3);
    MOQ_TEST_CHECK(moq_pub_end_track(pub, track, moq_simpair_now_us(sp)) ==
                   MOQ_ERR_WOULD_BLOCK);
    moq_action_t act;
    bool fin = false;
    size_t polled = moq_session_poll_actions(srv, &act, 1);
    MOQ_TEST_CHECK_EQ_SIZE(polled, 1);
    if (polled == 1) {
        /* The unrelated SUBSCRIBE is the non-data queue head. Removing it
         * restores room for FIN; no first data header has escaped yet. */
        const bool request_stream = moq_session_version(srv) != MOQ_VERSION_DRAFT_16;
        MOQ_TEST_CHECK_EQ_INT(act.kind, request_stream
                              ? MOQ_ACTION_OPEN_BIDI_STREAM : MOQ_ACTION_SEND_CONTROL);
        const uint8_t *bytes = NULL;
        size_t len = 0;
        if (act.kind == MOQ_ACTION_OPEN_BIDI_STREAM) {
            MOQ_TEST_CHECK(act.u.open_bidi_stream.stream_ref._v != 0);
            bytes = act.u.open_bidi_stream.data;
            len = act.u.open_bidi_stream.len;
        } else if (act.kind == MOQ_ACTION_SEND_CONTROL) {
            bytes = act.u.send_control.data;
            len = act.u.send_control.len;
        }
        MOQ_TEST_CHECK(bytes && len > 0);
        if (bytes && len > 0) {
            moq_buf_reader_t r;
            moq_control_envelope_t env;
            moq_buf_reader_init(&r, bytes, len);
            /* D18/D22 share the vi64 type + uint16 envelope here. */
            moq_result_t rc = request_stream ? moq_d18_decode_envelope(&r, &env)
                                            : moq_control_decode_envelope(&r, &env);
            MOQ_TEST_CHECK(rc == MOQ_OK);
            if (rc == MOQ_OK) MOQ_TEST_CHECK_EQ_U64(env.msg_type, 0x03); /* SUBSCRIBE */
        }
        moq_action_cleanup(&act);
    }
    MOQ_TEST_CHECK_EQ_SIZE(moq_session_action_capacity(srv), 1);
    /* This subscription ends with the EOT object, not a finite DONE. The
     * retry only owes its closing FIN and therefore needs queue capacity. */
    MOQ_TEST_CHECK(moq_pub_end_track(pub, track, moq_simpair_now_us(sp)) == MOQ_OK);
    size_t keys = 0;
    bool first_header = false;
    uint64_t keyed_ref = 0;
    while (moq_session_poll_actions(srv, &act, 1) == 1) {
        if (act.kind == MOQ_ACTION_SET_DATA_PRIORITY) {
            keys++;
            MOQ_TEST_CHECK(!first_header);
            keyed_ref = act.u.set_data_priority.stream_ref._v;
            MOQ_TEST_CHECK(keyed_ref != 0);
        } else if (act.kind == MOQ_ACTION_SEND_DATA) {
            MOQ_TEST_CHECK_EQ_SIZE(keys, 1);
            MOQ_TEST_CHECK_EQ_U64(act.u.send_data.stream_ref._v, keyed_ref);
            if (!first_header) {
                MOQ_TEST_CHECK(act.u.send_data.header_len > 0);
                MOQ_TEST_CHECK(!act.u.send_data.fin);
                first_header = true;
            }
            if (act.u.send_data.fin) fin = true;
        }
        moq_action_cleanup(&act); /* deliberate raw escape, not virtual OPENED */
    }
    MOQ_TEST_CHECK_EQ_SIZE(keys, 1);
    MOQ_TEST_CHECK(first_header);
    MOQ_TEST_CHECK(moq_pub_end_track(pub, track, moq_simpair_now_us(sp)) == MOQ_OK);
    while (moq_session_poll_actions(srv, &act, 1) == 1) moq_action_cleanup(&act);
    MOQ_TEST_CHECK(fin);

    moq_pub_destroy(pub);
    moq_simpair_destroy(sp);
    MOQ_TEST_PASS("end_track_with_stream_keys");
}

/* moq_pub_end_track: reliable terminal + the now-defined write-after-end
 * contract (WRONG_STATE, not undefined) + idempotency, end to end over the
 * SimPair wire. */
void test_end_track(void) {
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

    /* One normal object, then end the track. */
    uint8_t data[] = { 0x01, 0x02 };
    moq_rcbuf_t *p0 = NULL;
    moq_rcbuf_create(&alloc, data, sizeof(data), &p0);
    MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 0, 0, p0,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_rcbuf_decref(p0);

    MOQ_TEST_CHECK(end_track_virtual(sp, pub, track) == MOQ_OK);

    /* Write-after-end is now defined: WRONG_STATE, no ownership taken (we still
     * own the ref and decref it). */
    moq_rcbuf_t *p1 = NULL;
    moq_rcbuf_create(&alloc, data, sizeof(data), &p1);
    MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 1, 0, p1,
        moq_simpair_now_us(sp)) == MOQ_ERR_WRONG_STATE);
    moq_rcbuf_decref(p1);

    /* begin_object and set_retained_group are refused too. */
    moq_pub_begin_object_cfg_t bcfg;
    moq_pub_begin_object_cfg_init_sized(&bcfg, sizeof(bcfg));
    bcfg.group_id = 2; bcfg.object_id = 0; bcfg.payload_length = 4;
    MOQ_TEST_CHECK(moq_pub_begin_object(pub, track, &bcfg,
        moq_simpair_now_us(sp)) == MOQ_ERR_WRONG_STATE);

    /* Streaming write_data / end_object are refused after end_track too -- the
     * terminal-track invariant holds before the no-subscriber no-op path. */
    moq_rcbuf_t *pd = NULL;
    moq_rcbuf_create(&alloc, data, sizeof(data), &pd);
    MOQ_TEST_CHECK(moq_pub_write_data(pub, track, pd,
        moq_simpair_now_us(sp)) == MOQ_ERR_WRONG_STATE);
    moq_rcbuf_decref(pd);
    MOQ_TEST_CHECK(moq_pub_end_object(pub, track,
        moq_simpair_now_us(sp)) == MOQ_ERR_WRONG_STATE);

    moq_rcbuf_t *ps = NULL;
    moq_rcbuf_create(&alloc, data, sizeof(data), &ps);
    moq_pub_retained_object_t rstobj = { .object_id = 0, .payload = ps };
    moq_pub_retained_group_cfg_t stcfg;
    moq_pub_retained_group_cfg_init(&stcfg);
    stcfg.group_id = 3; stcfg.objects = &rstobj; stcfg.object_count = 1;
    MOQ_TEST_CHECK(moq_pub_set_retained_group(pub, track, &stcfg)
        == MOQ_ERR_WRONG_STATE);
    moq_rcbuf_decref(ps);

    /* Repeated end_track is idempotent. */
    MOQ_TEST_CHECK(end_track_virtual(sp, pub, track) == MOQ_OK);

    moq_simpair_run_until_quiescent(sp, 16, NULL);

    /* Client receives the normal object then a terminal END_OF_TRACK; the
     * session stays established. */
    int normal = 0, eot = 0;
    moq_event_t evts[8]; size_t ne;
    while ((ne = moq_session_poll_events(moq_simpair_client(sp), evts, 8)) > 0)
        for (size_t i = 0; i < ne; i++) {
            if (evts[i].kind == MOQ_EVENT_OBJECT_RECEIVED) {
                if (evts[i].u.object_received.status == MOQ_OBJECT_END_OF_TRACK) {
                    eot++;
                    MOQ_TEST_CHECK(evts[i].u.object_received.payload == NULL);
                } else if (evts[i].u.object_received.status == MOQ_OBJECT_NORMAL) {
                    normal++;
                }
            }
            moq_event_cleanup(&evts[i]);
        }
    MOQ_TEST_CHECK(normal == 1);
    MOQ_TEST_CHECK(eot == 1);
    MOQ_TEST_CHECK(moq_session_state(moq_simpair_client(sp))
        == MOQ_SESS_ESTABLISHED);

    moq_pub_remove_track(pub, track, moq_simpair_now_us(sp));
    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("end_track");
}

/* MSF 11.3 step 1: finish active subscribers (SUBSCRIBE_DONE status 0x2) without
 * terminalizing the track -- it stays registered, retained, and joinable. */
void test_finish_subscribers(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);
    uint64_t now = moq_simpair_now_us(sp);

    moq_pub_cfg_t cfg; moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    moq_publisher_t *pub = NULL;
    moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub);

    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("svc") };
    moq_pub_track_cfg_t tcfg; moq_pub_track_cfg_init(&tcfg);
    tcfg.track_namespace.parts = ns_parts; tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("catalog");
    tcfg.advertise_namespace = true;
    moq_pub_track_t *track = NULL;
    moq_pub_add_track(pub, &tcfg, now, &track);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev; if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1) moq_event_cleanup(&ev); }

    /* Retain a 3-object catalog group (independent [0] + deltas [1],[2]). */
    moq_rcbuf_t *p0=NULL,*p1=NULL,*p2=NULL;
    moq_rcbuf_create(&alloc, (const uint8_t*)"obj0", 4, &p0);
    moq_rcbuf_create(&alloc, (const uint8_t*)"obj1", 4, &p1);
    moq_rcbuf_create(&alloc, (const uint8_t*)"obj2", 4, &p2);
    moq_pub_retained_object_t objs[3] = {
        { .object_id = 0, .payload = p0 },
        { .object_id = 1, .payload = p1 },
        { .object_id = 2, .payload = p2, .end_of_group = true },
    };
    moq_pub_retained_group_cfg_t gc; moq_pub_retained_group_cfg_init(&gc);
    gc.group_id = 7; gc.objects = objs; gc.object_count = 3;
    MOQ_TEST_CHECK(moq_pub_set_retained_group(pub, track, &gc) == MOQ_OK);
    moq_rcbuf_decref(p0); moq_rcbuf_decref(p1); moq_rcbuf_decref(p2);

    /* Subscriber 1 joins (LargestObject). */
    moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace.parts = ns_parts; scfg.track_namespace.count = 1;
    scfg.track_name = MOQ_BYTES_LITERAL("catalog");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    moq_subscription_t sub1;
    MOQ_TEST_CHECK(moq_session_subscribe(moq_simpair_client(sp), &scfg, now, &sub1) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
          MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
          moq_pub_event_result_t res; moq_pub_handle_event(pub, &ev, now, &res);
          moq_event_cleanup(&ev);
      } }
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK(moq_pub_active_subscriptions(pub, track) == 1);
    drain_all(sp);   /* discard subscribe-ok */

    /* Finish the live subscriber with status 0x2 (Track Ended). */
    MOQ_TEST_CHECK(moq_pub_finish_subscribers(pub, track,
        MOQ_PUB_DONE_TRACK_ENDED, now) == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_active_subscriptions(pub, track) == 0);  /* slot freed */
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    /* Subscriber 1 surfaces exactly one SUBSCRIBE_DONE with status 0x2. */
    int done_n = 0; uint64_t done_status = ~0ull;
    { moq_event_t d[16]; size_t ne;
      while ((ne = moq_session_poll_events(moq_simpair_client(sp), d, 16)) > 0)
          for (size_t i = 0; i < ne; i++) {
              if (d[i].kind == MOQ_EVENT_SUBSCRIBE_DONE) {
                  done_n++; done_status = d[i].u.subscribe_done.status_code;
              }
              moq_event_cleanup(&d[i]);
          } }
    MOQ_TEST_CHECK(done_n == 1);
    MOQ_TEST_CHECK(done_status == MOQ_PUB_DONE_TRACK_ENDED);

    /* Idempotent: re-finishing with no active subscriber is a no-op MOQ_OK and
     * sends no further DONE. */
    MOQ_TEST_CHECK(moq_pub_finish_subscribers(pub, track,
        MOQ_PUB_DONE_TRACK_ENDED, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { int extra = 0; moq_event_t d[16]; size_t ne;
      while ((ne = moq_session_poll_events(moq_simpair_client(sp), d, 16)) > 0)
          for (size_t i = 0; i < ne; i++) {
              if (d[i].kind == MOQ_EVENT_SUBSCRIBE_DONE) extra++;
              moq_event_cleanup(&d[i]);
          }
      MOQ_TEST_CHECK(extra == 0); }

    /* Track is NOT terminalized: a fresh SUBSCRIBE is accepted (a plain
     * SUBSCRIBE does NOT replay retained objects -- that is pulled by the
     * Joining FETCH below). */
    moq_subscription_t sub2;
    MOQ_TEST_CHECK(moq_session_subscribe(moq_simpair_client(sp), &scfg, now, &sub2) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
          MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
          moq_pub_event_result_t res; moq_pub_handle_event(pub, &ev, now, &res);
          moq_event_cleanup(&ev);
      } }
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK(moq_pub_active_subscriptions(pub, track) == 1);  /* accepted */
    drain_all(sp);   /* no retained re-push on a plain SUBSCRIBE */

    /* Joining FETCH after finish still replays the retained group 0..2 -- a late
     * joiner pulls the VOD content. */
    moq_fetch_cfg_t fcfg; moq_fetch_cfg_init(&fcfg);
    fcfg.is_joining = true; fcfg.joining_relative = true;
    fcfg.joining_start = 0; fcfg.joining_sub = sub2;
    moq_fetch_t fh;
    MOQ_TEST_CHECK(moq_session_fetch(moq_simpair_client(sp), &fcfg, now, &fh) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK(moq_pub_tick(pub, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    int fn = 0; bool ford = true, fcomplete = false;
    { moq_event_t d[16]; size_t ne;
      while ((ne = moq_session_poll_events(moq_simpair_client(sp), d, 16)) > 0)
          for (size_t i = 0; i < ne; i++) {
              if (d[i].kind == MOQ_EVENT_FETCH_OBJECT) {
                  if (d[i].u.fetch_object.group_id != 7 ||
                      d[i].u.fetch_object.object_id != (uint64_t)fn) ford = false;
                  fn++;
              } else if (d[i].kind == MOQ_EVENT_FETCH_COMPLETE) fcomplete = true;
              moq_event_cleanup(&d[i]);
          } }
    MOQ_TEST_CHECK(fn == 3);
    MOQ_TEST_CHECK(ford);
    MOQ_TEST_CHECK(fcomplete);

    moq_pub_remove_track(pub, track, now);
    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("finish_subscribers");
}

/* finish_subscribers is distinct from end_track: it does NOT terminalize the
 * track, so writes (and new subscribes) still work afterward, whereas end_track
 * terminalizes and rejects them. */
void test_finish_subscribers_not_terminal(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);
    uint64_t now = moq_simpair_now_us(sp);

    moq_pub_cfg_t cfg; moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    moq_publisher_t *pub = NULL;
    moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub);

    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_pub_track_cfg_t tcfg; moq_pub_track_cfg_init(&tcfg);
    tcfg.track_namespace.parts = ns_parts; tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_pub_track_t *track = NULL;
    moq_pub_add_track(pub, &tcfg, now, &track);

    moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace.parts = ns_parts; scfg.track_namespace.count = 1;
    scfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_subscription_t sub_h;
    moq_session_subscribe(moq_simpair_client(sp), &scfg, now, &sub_h);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
          moq_pub_event_result_t res; moq_pub_handle_event(pub, &ev, now, &res);
          moq_event_cleanup(&ev);
      } }
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    drain_all(sp);

    uint8_t data[] = { 0x01, 0x02 };
    moq_rcbuf_t *p0 = NULL; moq_rcbuf_create(&alloc, data, sizeof(data), &p0);
    MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 0, 0, p0, now) == MOQ_OK);
    moq_rcbuf_decref(p0);

    /* Resolve the first actual virtual open before this immediate-finish case. */
    MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(sp, 8, NULL) == MOQ_OK);
    /* Finish the subscriber -> the track is still live, not ended. */
    MOQ_TEST_CHECK(moq_pub_finish_subscribers(pub, track,
        MOQ_PUB_DONE_TRACK_ENDED, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    /* Writes still succeed (no subscriber now -> fire-and-forget no-op OK). */
    moq_rcbuf_t *p1 = NULL; moq_rcbuf_create(&alloc, data, sizeof(data), &p1);
    MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 1, 0, p1, now) == MOQ_OK);
    moq_rcbuf_decref(p1);

    /* Now end_track terminalizes: subsequent writes are rejected. This contrasts
     * the two APIs (end_track still terminalizes; finish_subscribers does not). */
    MOQ_TEST_CHECK(moq_pub_end_track(pub, track, now) == MOQ_OK);
    moq_rcbuf_t *p2 = NULL; moq_rcbuf_create(&alloc, data, sizeof(data), &p2);
    MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 2, 0, p2, now) == MOQ_ERR_WRONG_STATE);
    moq_rcbuf_decref(p2);

    moq_pub_remove_track(pub, track, now);
    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("finish_subscribers_not_terminal");
}

/* WOULD_BLOCK-safe: with a 1-slot action queue, finish_subscribers needs more
 * than one queued action (close the live subgroup FIN, then send the DONE), so
 * it returns WOULD_BLOCK and resumes mid-slot on retry -- never duplicating the
 * DONE nor skipping the subscriber. */
void test_finish_subscribers_would_block(void) {
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);

    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = &alloc;
    cfg.max_actions = 1;
    cfg.client_send_request_capacity = cfg.server_send_request_capacity = true;
    cfg.client_initial_request_capacity = cfg.server_initial_request_capacity = 16;
    moq_simpair_t *sp = NULL;
    MOQ_TEST_CHECK(moq_simpair_create(&cfg, &sp) == MOQ_OK);
    if (!sp) return;
    moq_session_t *client = moq_simpair_client(sp), *server = moq_simpair_server(sp);
    MOQ_TEST_CHECK(moq_simpair_start(sp) == MOQ_OK);
    MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(sp, 16, NULL) == MOQ_OK);
    { moq_event_t ev;
      if (moq_session_poll_events(client, &ev, 1) == 1) moq_event_cleanup(&ev);
      if (moq_session_poll_events(server, &ev, 1) == 1) moq_event_cleanup(&ev); }

    moq_pub_cfg_t pcfg; moq_pub_cfg_init_sized(&pcfg, sizeof(pcfg));
    pcfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    moq_publisher_t *pub = NULL;
    moq_pub_create(server, &alloc, &pcfg, &pub);

    moq_pub_track_cfg_t tcfg; moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
    tcfg.track_namespace.parts = ns_parts; tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("t");
    moq_pub_track_t *track = NULL;
    moq_pub_add_track(pub, &tcfg, 0, &track);

    moq_subscribe_cfg_t sub1cfg; moq_subscribe_cfg_init(&sub1cfg);
    sub1cfg.track_namespace.parts = ns_parts; sub1cfg.track_namespace.count = 1;
    sub1cfg.track_name = MOQ_BYTES_LITERAL("t");
    moq_subscription_t sub1;
    moq_session_subscribe(client, &sub1cfg, 0, &sub1);
    MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(sp, 16, NULL) == MOQ_OK);
    { moq_event_t ev;
      if (moq_session_poll_events(server, &ev, 1) == 1) {
          moq_pub_event_result_t res; moq_pub_handle_event(pub, &ev, 0, &res);
          moq_event_cleanup(&ev);
      } }
    MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(sp, 16, NULL) == MOQ_OK);
    { moq_event_t ev;
      if (moq_session_poll_events(client, &ev, 1) == 1) moq_event_cleanup(&ev); }

    /* Open a live subgroup so finish must close it before sending the DONE. */
    uint8_t d[] = { 0xBB };
    moq_rcbuf_t *p = NULL; moq_rcbuf_create(&alloc, d, 1, &p);
    moq_result_t write_rc = MOQ_ERR_WOULD_BLOCK;
    for (unsigned i = 0; i < 9 && write_rc == MOQ_ERR_WOULD_BLOCK; ++i) {
        write_rc = moq_pub_write_object(pub, track, 0, 0, p, 0);
        if (write_rc == MOQ_ERR_WOULD_BLOCK)
            MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(sp, 16, NULL) == MOQ_OK);
    }
    MOQ_TEST_CHECK(write_rc == MOQ_OK);
    moq_rcbuf_decref(p);
    MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(sp, 16, NULL) == MOQ_OK);

    /* Fill the 1-slot queue so finish's first action WOULD_BLOCK. */
    moq_publish_namespace_cfg_t nscfg; moq_publish_namespace_cfg_init(&nscfg);
    moq_bytes_t blocker_ns[] = { MOQ_BYTES_LITERAL("blk") };
    nscfg.track_namespace.parts = blocker_ns; nscfg.track_namespace.count = 1;
    moq_announcement_t blocker_ann;
    MOQ_TEST_CHECK(moq_session_publish_namespace(server, &nscfg, 0, &blocker_ann) == MOQ_OK);

    moq_result_t rc = moq_pub_finish_subscribers(pub, track,
        MOQ_PUB_DONE_TRACK_ENDED, 0);
    MOQ_TEST_CHECK(rc == MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(moq_pub_active_subscriptions(pub, track) == 1);  /* preserved */

    /* Drain (delivering control AND data-stream bytes -- the client gates
     * SUBSCRIBE_DONE on processing the advertised data stream, so the
     * subgroup's bytes and closing FIN must actually arrive) and retry
     * until the finish completes. */
    int done_n = 0;
    for (int k = 0; k < 9 && done_n == 0; k++) {
        MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(sp, 16, NULL) == MOQ_OK);
        { moq_event_t ev[8]; size_t ne;
          while ((ne = moq_session_poll_events(client, ev, 8)) > 0)
              for (size_t i = 0; i < ne; i++) {
                  if (ev[i].kind == MOQ_EVENT_SUBSCRIBE_DONE) {
                      done_n++;
                      MOQ_TEST_CHECK_EQ_U64(ev[i].u.subscribe_done.status_code, MOQ_PUB_DONE_TRACK_ENDED);
                      MOQ_TEST_CHECK_EQ_U64(ev[i].u.subscribe_done.stream_count, 1);
                  }
                  moq_event_cleanup(&ev[i]);
              } }
        if (rc == MOQ_ERR_WOULD_BLOCK)
            rc = moq_pub_finish_subscribers(pub, track,
                MOQ_PUB_DONE_TRACK_ENDED, 0);
    }
    MOQ_TEST_CHECK(rc == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_active_subscriptions(pub, track) == 0);  /* freed once */
    MOQ_TEST_CHECK(done_n == 1);   /* exactly one DONE reached the subscriber */

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
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("finish_subscribers_would_block");
}

void test_write_no_subs(void) {
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

    uint8_t data[] = { 1, 2, 3 };
    moq_rcbuf_t *payload = NULL;
    moq_rcbuf_create(&alloc, data, sizeof(data), &payload);

    moq_result_t rc = moq_pub_write_object(pub, track, 0, 0, payload,
        moq_simpair_now_us(sp));
    MOQ_TEST_CHECK(rc == MOQ_OK);
    moq_rcbuf_decref(payload);

    moq_pub_remove_track(pub, track, moq_simpair_now_us(sp));
    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("write_no_subs");
}

void test_write_group_change(void) {
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
      }
    }
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev); }

    /* Write in group 0. */
    uint8_t d0[] = { 0xAA };
    moq_rcbuf_t *p0 = NULL;
    moq_rcbuf_create(&alloc, d0, 1, &p0);
    moq_pub_write_object(pub, track, 0, 0, p0, moq_simpair_now_us(sp));
    moq_rcbuf_decref(p0);

    /* Write in group 1 — should close group 0's subgroup and open new. */
    uint8_t d1[] = { 0xBB };
    moq_rcbuf_t *p1 = NULL;
    moq_rcbuf_create(&alloc, d1, 1, &p1);
    moq_result_t rc = moq_pub_write_object(pub, track, 1, 0, p1,
        moq_simpair_now_us(sp));
    MOQ_TEST_CHECK(rc == MOQ_OK);
    moq_rcbuf_decref(p1);

    /* Pump and verify both objects arrive. */
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    int obj_count = 0;
    moq_event_t evts[16];
    size_t ne = moq_session_poll_events(moq_simpair_client(sp), evts, 16);
    for (size_t i = 0; i < ne; i++) {
        if (evts[i].kind == MOQ_EVENT_OBJECT_RECEIVED) obj_count++;
        moq_event_cleanup(&evts[i]);
    }
    MOQ_TEST_CHECK(obj_count == 2);

    moq_pub_remove_track(pub, track, moq_simpair_now_us(sp));
    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("write_group_change");
}

void test_end_group(void) {
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
      }
    }
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev); }

    /* Write object, then end_group. */
    uint8_t d[] = { 1 };
    moq_rcbuf_t *p = NULL;
    moq_rcbuf_create(&alloc, d, 1, &p);
    moq_pub_write_object(pub, track, 0, 0, p, moq_simpair_now_us(sp));
    moq_rcbuf_decref(p);
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    moq_result_t rc = moq_pub_end_group(pub, track, moq_simpair_now_us(sp));
    MOQ_TEST_CHECK(rc == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    /* end_group sealed group 0: a later Object there is refused before any
     * effect (the payload stays the caller's), while the next group opens
     * normally. */
    moq_rcbuf_t *p2 = NULL;
    moq_rcbuf_create(&alloc, d, 1, &p2);
    rc = moq_pub_write_object(pub, track, 0, 1, p2, moq_simpair_now_us(sp));
    MOQ_TEST_CHECK(rc == MOQ_ERR_WRONG_STATE);
    MOQ_TEST_CHECK(moq_rcbuf_refcount(p2) == 1);
    rc = moq_pub_write_object(pub, track, 1, 0, p2, moq_simpair_now_us(sp));
    MOQ_TEST_CHECK(rc == MOQ_OK);
    moq_rcbuf_decref(p2);

    moq_pub_remove_track(pub, track, moq_simpair_now_us(sp));
    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("end_group");
}

void test_remove_track_with_open_sg(void) {
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
    tcfg.advertise_namespace = true;

    moq_pub_track_t *track = NULL;
    moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &track);

    /* Client accepts namespace. */
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1) {
          moq_accept_namespace_cfg_t nacc;
          moq_accept_namespace_cfg_init(&nacc);
          moq_session_accept_namespace(moq_simpair_client(sp),
              ev.u.namespace_published.ann, &nacc, moq_simpair_now_us(sp));
          moq_event_cleanup(&ev);
      }
    }
    /* Pump NAMESPACE_ACCEPTED to server. */
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
          moq_pub_event_result_t res;
          moq_pub_handle_event(pub, &ev, moq_simpair_now_us(sp), &res);
          moq_event_cleanup(&ev);
      }
    }

    /* Subscribe. */
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
      }
    }
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev); }

    /* Write to open a subgroup. */
    uint8_t d[] = { 0xFF };
    moq_rcbuf_t *p = NULL;
    moq_rcbuf_create(&alloc, d, 1, &p);
    moq_pub_write_object(pub, track, 0, 0, p, moq_simpair_now_us(sp));
    moq_rcbuf_decref(p);

    /* Remove track with open subgroup — namespace is ACCEPTED. */
    moq_result_t rc = moq_pub_remove_track(pub, track,
        moq_simpair_now_us(sp));
    MOQ_TEST_CHECK(rc == MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(sp, 16, NULL) == MOQ_OK);
    rc = moq_pub_remove_track(pub, track, moq_simpair_now_us(sp));
    MOQ_TEST_CHECK(rc == MOQ_OK);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("remove_track_with_open_sg");
}

void test_destroy_with_live_subs(void) {
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

    /* Subscribe + accept + write. */
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
      }
    }
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev); }

    uint8_t d[] = { 1 };
    moq_rcbuf_t *p = NULL;
    moq_rcbuf_create(&alloc, d, 1, &p);
    moq_pub_write_object(pub, track, 0, 0, p, moq_simpair_now_us(sp));
    moq_rcbuf_decref(p);

    /* Destroy without remove_track. Local cleanup only. */
    moq_pub_destroy(pub);

    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("destroy_with_live_subs");
}

void test_event_ignored(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    moq_publisher_t *pub = NULL;
    moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub);

    moq_event_t fake_ev;
    memset(&fake_ev, 0, sizeof(fake_ev));
    fake_ev.kind = MOQ_EVENT_REQUEST_READY;
    moq_pub_event_result_t res;
    moq_pub_handle_event(pub, &fake_ev, 0, &res);
    MOQ_TEST_CHECK(res == MOQ_PUB_EVENT_IGNORED);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("event_ignored");
}

void test_session_closed_event(void) {
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

    /* Simulate SESSION_CLOSED event. */
    moq_event_t close_ev;
    memset(&close_ev, 0, sizeof(close_ev));
    close_ev.kind = MOQ_EVENT_SESSION_CLOSED;
    moq_pub_event_result_t res;
    moq_pub_handle_event(pub, &close_ev, 0, &res);
    MOQ_TEST_CHECK(res == MOQ_PUB_EVENT_IGNORED);

    /* Writes should now return CLOSED. */
    uint8_t d[] = { 1 };
    moq_rcbuf_t *p = NULL;
    moq_rcbuf_create(&alloc, d, 1, &p);
    moq_result_t rc = moq_pub_write_object(pub, track, 0, 0, p, 0);
    MOQ_TEST_CHECK(rc == MOQ_ERR_CLOSED);
    moq_rcbuf_decref(p);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("session_closed_event");
}

/* -- Publisher priority and configuration ------------------------- */

void test_publisher_priority_zero(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    cfg.default_publisher_priority = 200;
    moq_publisher_t *pub = NULL;
    moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub);

    /* Track with explicit priority 0. */
    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init_sized(&tcfg, sizeof(tcfg));
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("t");
    tcfg.has_publisher_priority = true;
    tcfg.publisher_priority = 0;

    moq_pub_track_t *track = NULL;
    moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &track);

    /* Subscribe + accept + write. */
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

    uint8_t d[] = { 1 };
    moq_rcbuf_t *p = NULL;
    moq_rcbuf_create(&alloc, d, 1, &p);
    moq_pub_write_object(pub, track, 0, 0, p, moq_simpair_now_us(sp));
    moq_rcbuf_decref(p);

    /* Check the SEND_DATA action header for priority. The subgroup
     * header encodes publisher_priority; with priority=0, the byte
     * in the header should be 0 (not default 200). */
    /* The first SEND_DATA is the subgroup-open header. Its last byte
     * is publisher_priority. Check it's 0, not the default 200. */
    { moq_action_t acts[8];
      size_t na = moq_session_poll_actions(moq_simpair_server(sp), acts, 8);
      bool found = false;
      for (size_t i = 0; i < na; i++) {
          if (!found && acts[i].kind == MOQ_ACTION_SEND_DATA &&
              acts[i].u.send_data.header_len > 0) {
              found = true;
              uint8_t prio = acts[i].u.send_data.header[
                  acts[i].u.send_data.header_len - 1];
              MOQ_TEST_CHECK(prio == 0);
          }
          moq_action_cleanup(&acts[i]);
      }
      MOQ_TEST_CHECK(found);
    }

    moq_pub_remove_track(pub, track, moq_simpair_now_us(sp));
    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("publisher_priority_zero");
}

void test_publisher_priority_default(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    cfg.default_publisher_priority = 200;
    moq_publisher_t *pub = NULL;
    moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub);

    /* Track WITHOUT has_publisher_priority. Priority should be
     * the publisher default (200), not the track cfg value. */
    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("t");
    tcfg.has_publisher_priority = false;
    tcfg.publisher_priority = 0;

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
      if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev); }

    uint8_t d[] = { 1 };
    moq_rcbuf_t *p = NULL;
    moq_rcbuf_create(&alloc, d, 1, &p);
    moq_pub_write_object(pub, track, 0, 0, p, moq_simpair_now_us(sp));
    moq_rcbuf_decref(p);

    { moq_action_t acts[8];
      size_t na = moq_session_poll_actions(moq_simpair_server(sp), acts, 8);
      bool found = false;
      for (size_t i = 0; i < na; i++) {
          if (!found && acts[i].kind == MOQ_ACTION_SEND_DATA &&
              acts[i].u.send_data.header_len > 0) {
              found = true;
              uint8_t last = acts[i].u.send_data.header[
                  acts[i].u.send_data.header_len - 1];
              MOQ_TEST_CHECK(last == 200);
          }
          moq_action_cleanup(&acts[i]);
      }
      MOQ_TEST_CHECK(found);
    }

    moq_pub_remove_track(pub, track, moq_simpair_now_us(sp));
    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("publisher_priority_default");
}

void test_publisher_priority_old_struct(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    cfg.default_publisher_priority = 150;
    moq_publisher_t *pub = NULL;
    moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub);

    /* Exact original layout: 48 bytes on LP64, 24 on ILP32. Poison its
     * real trailing padding without adding LP64-only explicit members. */
    test_pub_track_original_t old_tcfg;
    memset(&old_tcfg, 0, sizeof(old_tcfg));
    /* The presence bit must be appended past the original layout, else this
     * discriminator can't work. */
    MOQ_TEST_CHECK(offsetof(moq_pub_track_cfg_t, has_publisher_priority) >=
                   sizeof(old_tcfg));
    old_tcfg.struct_size = sizeof(old_tcfg);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
    old_tcfg.track_namespace.parts = ns_parts;
    old_tcfg.track_namespace.count = 1;
    old_tcfg.track_name = MOQ_BYTES_LITERAL("t");
    old_tcfg.publisher_priority = 0;
    const size_t padding = offsetof(test_pub_track_original_t, publisher_priority) + 1;
    memset((unsigned char *)&old_tcfg + padding, 0xFF, sizeof(old_tcfg) - padding);

    moq_pub_track_t *track = NULL;
    moq_result_t rc = moq_pub_add_track(pub,
        (const moq_pub_track_cfg_t *)&old_tcfg,
        moq_simpair_now_us(sp), &track);
    MOQ_TEST_CHECK(rc == MOQ_OK);

    /* The old struct has no real has_publisher_priority field; the nonzero
     * padding byte must not be misread as one. With publisher_priority=0 the
     * track must still use the pub default (150), not explicit 0. */
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

    uint8_t d[] = { 1 };
    moq_rcbuf_t *p = NULL;
    moq_rcbuf_create(&alloc, d, 1, &p);
    moq_pub_write_object(pub, track, 0, 0, p, moq_simpair_now_us(sp));
    moq_rcbuf_decref(p);

    { moq_action_t acts[8];
      size_t na = moq_session_poll_actions(moq_simpair_server(sp), acts, 8);
      bool found = false;
      for (size_t i = 0; i < na; i++) {
          if (!found && acts[i].kind == MOQ_ACTION_SEND_DATA &&
              acts[i].u.send_data.header_len > 0) {
              found = true;
              uint8_t last = acts[i].u.send_data.header[
                  acts[i].u.send_data.header_len - 1];
              MOQ_TEST_CHECK(last == 150);
          }
          moq_action_cleanup(&acts[i]);
      }
      MOQ_TEST_CHECK(found);
    }

    moq_pub_remove_track(pub, track, moq_simpair_now_us(sp));
    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("publisher_priority_old_struct");
}

/* Forward-ABI: the pointer-only initializer must stamp and clear ONLY the
 * frozen original prefix, never an appended field's storage (which an old
 * caller may not have allocated). The sized initializer clears the whole
 * current struct. */
void test_pub_track_cfg_init_canary(void) {
    const size_t v0 = offsetof(moq_pub_track_cfg_t, max_retained_bytes);

    union {
        moq_pub_track_cfg_t cfg;
        uint8_t bytes[sizeof(moq_pub_track_cfg_t) + 8];
    } box;
    memset(&box, 0xAB, sizeof(box));

    moq_pub_track_cfg_init(&box.cfg);
    /* Stamps exactly the frozen prefix size. */
    MOQ_TEST_CHECK(box.cfg.struct_size == (uint32_t)v0);
    /* The byte at the first appended field (max_retained_bytes) is untouched
     * — pointer-only init never wrote past the frozen prefix. */
    MOQ_TEST_CHECK(box.bytes[v0] == 0xAB);
    MOQ_TEST_CHECK(box.cfg.publisher_priority == 128);

    /* Sized init clears the full struct, including the appended fields. */
    memset(&box, 0xAB, sizeof(box));
    moq_pub_track_cfg_init_sized(&box.cfg, sizeof(box.cfg));
    MOQ_TEST_CHECK(box.cfg.struct_size == (uint32_t)sizeof(moq_pub_track_cfg_t));
    MOQ_TEST_CHECK(box.cfg.max_retained_bytes == 0);
    MOQ_TEST_CHECK(box.cfg.has_publisher_priority == false);
    MOQ_TEST_CHECK(box.cfg.publisher_priority == 128);
    /* The trailing canary past the struct is still untouched. */
    MOQ_TEST_CHECK(box.bytes[sizeof(moq_pub_track_cfg_t)] == 0xAB);

    MOQ_TEST_PASS("pub_track_cfg_init_canary");
}

void test_oversized_namespace_rejected(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    moq_publisher_t *pub = NULL;
    moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub);

    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init(&tcfg);

    /* Single namespace part exceeding MOQ_FULL_TRACK_NAME_MAX (4096). */
    uint8_t big[4097];
    memset(big, 'A', sizeof(big));
    moq_bytes_t big_part = { big, sizeof(big) };
    tcfg.track_namespace.parts = &big_part;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("t");

    moq_pub_track_t *track = NULL;
    moq_result_t rc = moq_pub_add_track(pub, &tcfg, 0, &track);
    MOQ_TEST_CHECK(rc == MOQ_ERR_INVAL);
    MOQ_TEST_CHECK(track == NULL);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("oversized_namespace_rejected");
}

void test_oversized_full_track_name_rejected(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    moq_publisher_t *pub = NULL;
    moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub);

    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init(&tcfg);

    /* Namespace uses 4090 bytes; track name uses 7 → total 4097 > 4096. */
    uint8_t ns_buf[4090];
    memset(ns_buf, 'N', sizeof(ns_buf));
    moq_bytes_t ns_part = { ns_buf, sizeof(ns_buf) };
    tcfg.track_namespace.parts = &ns_part;
    tcfg.track_namespace.count = 1;

    uint8_t name_buf[7];
    memset(name_buf, 'T', sizeof(name_buf));
    tcfg.track_name.data = name_buf;
    tcfg.track_name.len = sizeof(name_buf);

    moq_pub_track_t *track = NULL;
    moq_result_t rc = moq_pub_add_track(pub, &tcfg, 0, &track);
    MOQ_TEST_CHECK(rc == MOQ_ERR_INVAL);
    MOQ_TEST_CHECK(track == NULL);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("oversized_full_track_name_rejected");
}
