#include <moq/publisher.h>
#include <moq/sim.h>
#include <moq/wire.h>   /* MOQ_QUIC_VARINT_MAX */
#include "test_session_support.h"
#include <stddef.h>
#include <stdio.h>
#include <string.h>

static int failures = 0;

/* -- Helpers ------------------------------------------------------ */

static void simpair_setup(test_alloc_state_t *as, moq_alloc_t *alloc,
                           moq_simpair_t **sp)
{
    *as = (test_alloc_state_t){0};
    *alloc = test_allocator(as);
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = alloc;
    cfg.seed = 42;
    cfg.initial_now_us = 1000;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 16;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 16;
    moq_simpair_create(&cfg, sp);
    moq_simpair_start(*sp);
    moq_simpair_run_until_quiescent(*sp, 8, NULL);
    /* Drain setup events. */
    moq_event_t ev;
    if (moq_session_poll_events(moq_simpair_client(*sp), &ev, 1) == 1)
        moq_event_cleanup(&ev);
    if (moq_session_poll_events(moq_simpair_server(*sp), &ev, 1) == 1)
        moq_event_cleanup(&ev);
}

static void drain_all(moq_simpair_t *sp) {
    moq_event_t evts[16]; size_t ne;
    while ((ne = moq_session_poll_events(moq_simpair_client(sp), evts, 16)) > 0)
        for (size_t i = 0; i < ne; i++) moq_event_cleanup(&evts[i]);
    while ((ne = moq_session_poll_events(moq_simpair_server(sp), evts, 16)) > 0)
        for (size_t i = 0; i < ne; i++) moq_event_cleanup(&evts[i]);
    moq_action_t acts[16]; size_t na;
    while ((na = moq_session_poll_actions(moq_simpair_client(sp), acts, 16)) > 0)
        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
    while ((na = moq_session_poll_actions(moq_simpair_server(sp), acts, 16)) > 0)
        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
}

/* -- Tests -------------------------------------------------------- */

static void test_create_destroy(void) {
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
static void test_pub_cfg_init_old_prefix_no_overflow(void) {
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

static void test_add_remove_track(void) {
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

static void test_subscribe_accept_all(void) {
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
static void test_remove_track_retires_subscription(void) {
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

static void test_subscribe_no_match(void) {
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

static void test_subscribe_duplicate_rejected(void) {
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

static void test_write_single_sub(void) {
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

/* moq_pub_end_track: reliable terminal + the now-defined write-after-end
 * contract (WRONG_STATE, not undefined) + idempotency, end to end over the
 * SimPair wire. */
static void test_end_track(void) {
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

    MOQ_TEST_CHECK(moq_pub_end_track(pub, track,
        moq_simpair_now_us(sp)) == MOQ_OK);

    /* Write-after-end is now defined: WRONG_STATE, no ownership taken (we still
     * own the ref and decref it). */
    moq_rcbuf_t *p1 = NULL;
    moq_rcbuf_create(&alloc, data, sizeof(data), &p1);
    MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 1, 0, p1,
        moq_simpair_now_us(sp)) == MOQ_ERR_WRONG_STATE);
    moq_rcbuf_decref(p1);

    /* begin_object and set_retained_group are refused too. */
    moq_pub_begin_object_cfg_t bcfg;
    moq_pub_begin_object_cfg_init(&bcfg);
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
    MOQ_TEST_CHECK(moq_pub_end_track(pub, track,
        moq_simpair_now_us(sp)) == MOQ_OK);

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
static void test_finish_subscribers(void) {
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
static void test_finish_subscribers_not_terminal(void) {
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
static void test_finish_subscribers_would_block(void) {
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
    pump_actions_to_peer(client, server, 0);
    { moq_event_t ev;
      if (moq_session_poll_events(server, &ev, 1) == 1) {
          moq_pub_event_result_t res; moq_pub_handle_event(pub, &ev, 0, &res);
          moq_event_cleanup(&ev);
      } }
    pump_actions_to_peer(server, client, 0);
    { moq_event_t ev;
      if (moq_session_poll_events(client, &ev, 1) == 1) moq_event_cleanup(&ev); }

    /* Open a live subgroup so finish must close it before sending the DONE. */
    uint8_t d[] = { 0xBB };
    moq_rcbuf_t *p = NULL; moq_rcbuf_create(&alloc, d, 1, &p);
    moq_pub_write_object(pub, track, 0, 0, p, 0);
    moq_rcbuf_decref(p);

    /* Fill the 1-slot queue so finish's first action WOULD_BLOCK. */
    moq_publish_namespace_cfg_t nscfg; moq_publish_namespace_cfg_init(&nscfg);
    moq_bytes_t blocker_ns[] = { MOQ_BYTES_LITERAL("blk") };
    nscfg.track_namespace.parts = blocker_ns; nscfg.track_namespace.count = 1;
    moq_announcement_t blocker_ann;
    moq_session_publish_namespace(server, &nscfg, 0, &blocker_ann);

    moq_result_t rc = moq_pub_finish_subscribers(pub, track,
        MOQ_PUB_DONE_TRACK_ENDED, 0);
    MOQ_TEST_CHECK(rc == MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(moq_pub_active_subscriptions(pub, track) == 1);  /* preserved */

    /* Drain (delivering control to the client) and retry until it completes.
     * pump_actions_to_peer forwards the DONE control and ignores stream FINs. */
    int done_n = 0;
    for (int k = 0; k < 8 && rc == MOQ_ERR_WOULD_BLOCK; k++) {
        pump_actions_to_peer(server, client, 0);
        { moq_event_t ev[8]; size_t ne;
          while ((ne = moq_session_poll_events(client, ev, 8)) > 0)
              for (size_t i = 0; i < ne; i++) {
                  if (ev[i].kind == MOQ_EVENT_SUBSCRIBE_DONE) done_n++;
                  moq_event_cleanup(&ev[i]);
              } }
        rc = moq_pub_finish_subscribers(pub, track, MOQ_PUB_DONE_TRACK_ENDED, 0);
    }
    MOQ_TEST_CHECK(rc == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_active_subscriptions(pub, track) == 0);  /* freed once */

    /* Deliver the final DONE and confirm exactly one was sent (no duplicate). */
    pump_actions_to_peer(server, client, 0);
    { moq_event_t ev[8]; size_t ne;
      while ((ne = moq_session_poll_events(client, ev, 8)) > 0)
          for (size_t i = 0; i < ne; i++) {
              if (ev[i].kind == MOQ_EVENT_SUBSCRIBE_DONE) done_n++;
              moq_event_cleanup(&ev[i]);
          } }
    MOQ_TEST_CHECK(done_n == 1);

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
    MOQ_TEST_PASS("finish_subscribers_would_block");
}

static void test_write_no_subs(void) {
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

static void test_write_group_change(void) {
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

static void test_end_group(void) {
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

    /* Writing in same group should open a new subgroup. */
    moq_rcbuf_t *p2 = NULL;
    moq_rcbuf_create(&alloc, d, 1, &p2);
    rc = moq_pub_write_object(pub, track, 0, 1, p2, moq_simpair_now_us(sp));
    MOQ_TEST_CHECK(rc == MOQ_OK);
    moq_rcbuf_decref(p2);

    moq_pub_remove_track(pub, track, moq_simpair_now_us(sp));
    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("end_group");
}

static void test_remove_track_with_open_sg(void) {
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
    MOQ_TEST_CHECK(rc == MOQ_OK);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("remove_track_with_open_sg");
}

static void test_destroy_with_live_subs(void) {
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

static void test_event_ignored(void) {
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

static void test_session_closed_event(void) {
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

/* -- Hardening tests ---------------------------------------------- */

static void test_invalid_namespace_inputs(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    moq_publisher_t *pub = NULL;
    moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub);

    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init(&tcfg);
    moq_pub_track_t *track = NULL;

    /* count=0 → INVAL */
    tcfg.track_namespace.parts = NULL;
    tcfg.track_namespace.count = 0;
    tcfg.track_name = MOQ_BYTES_LITERAL("t");
    MOQ_TEST_CHECK(moq_pub_add_track(pub, &tcfg, 0, &track) == MOQ_ERR_INVAL);

    /* null parts with count>0 → INVAL */
    tcfg.track_namespace.parts = NULL;
    tcfg.track_namespace.count = 1;
    MOQ_TEST_CHECK(moq_pub_add_track(pub, &tcfg, 0, &track) == MOQ_ERR_INVAL);

    /* part with null data but nonzero len → INVAL */
    moq_bytes_t bad_part = { NULL, 5 };
    tcfg.track_namespace.parts = &bad_part;
    tcfg.track_namespace.count = 1;
    MOQ_TEST_CHECK(moq_pub_add_track(pub, &tcfg, 0, &track) == MOQ_ERR_INVAL);

    /* name with null data but nonzero len → INVAL */
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name.data = NULL;
    tcfg.track_name.len = 10;
    MOQ_TEST_CHECK(moq_pub_add_track(pub, &tcfg, 0, &track) == MOQ_ERR_INVAL);

    /* zero-length namespace part → INVAL (aligned with session API). */
    moq_bytes_t empty_part = { NULL, 0 };
    tcfg.track_namespace.parts = &empty_part;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("t");
    MOQ_TEST_CHECK(moq_pub_add_track(pub, &tcfg, 0, &track) == MOQ_ERR_INVAL);

    /* count > 32 → INVAL */
    moq_bytes_t many_parts[33];
    for (int i = 0; i < 33; i++) many_parts[i] = MOQ_BYTES_LITERAL("x");
    tcfg.track_namespace.parts = many_parts;
    tcfg.track_namespace.count = 33;
    MOQ_TEST_CHECK(moq_pub_add_track(pub, &tcfg, 0, &track) == MOQ_ERR_INVAL);

    /* All failure paths leave *out = NULL. */
    MOQ_TEST_CHECK(track == NULL);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("invalid_namespace_inputs");
}

static void test_strict_alloc_sizes(void) {
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

static void test_reject_all_mode(void) {
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

static void test_duplicate_track_rejected(void) {
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

static void test_namespace_accepted_then_remove(void) {
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

    /* Pump PUBLISH_NAMESPACE to client. */
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1) {
          MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED);
          /* Client accepts the namespace. */
          moq_accept_namespace_cfg_t acc;
          moq_accept_namespace_cfg_init(&acc);
          moq_session_accept_namespace(moq_simpair_client(sp),
              ev.u.namespace_published.ann, &acc, moq_simpair_now_us(sp));
          moq_event_cleanup(&ev);
      }
    }

    /* Pump NAMESPACE_ACCEPTED back to server. */
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
          moq_pub_event_result_t res;
          moq_pub_handle_event(pub, &ev, moq_simpair_now_us(sp), &res);
          MOQ_TEST_CHECK(res == MOQ_PUB_EVENT_CONSUMED);
          moq_event_cleanup(&ev);
      }
    }

    /* remove_track should send PUBLISH_NAMESPACE_DONE now. */
    moq_result_t rc = moq_pub_remove_track(pub, track,
        moq_simpair_now_us(sp));
    MOQ_TEST_CHECK(rc == MOQ_OK);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("namespace_accepted_then_remove");
}

static void test_reset_group(void) {
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

    /* A second reset with nothing open is a no-op again. */
    MOQ_TEST_CHECK(moq_pub_reset_group(pub, track, 0,
        moq_simpair_now_us(sp)) == MOQ_OK);

    moq_pub_remove_track(pub, track, moq_simpair_now_us(sp));
    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("reset_group");
}

static void test_namespace_accepted_query(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    moq_publisher_t *pub = NULL;
    moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub);

    /* NULL inputs are false. */
    MOQ_TEST_CHECK(!moq_pub_namespace_accepted(NULL, NULL));
    MOQ_TEST_CHECK(!moq_pub_namespace_accepted(pub, NULL));

    /* A track WITHOUT advertise_namespace is never "accepted". */
    moq_pub_track_cfg_t qcfg;
    moq_pub_track_cfg_init(&qcfg);
    moq_bytes_t q_ns[] = { MOQ_BYTES_LITERAL("quiet") };
    qcfg.track_namespace.parts = q_ns;
    qcfg.track_namespace.count = 1;
    qcfg.track_name = MOQ_BYTES_LITERAL("v");
    qcfg.advertise_namespace = false;
    moq_pub_track_t *quiet = NULL;
    moq_pub_add_track(pub, &qcfg, moq_simpair_now_us(sp), &quiet);
    MOQ_TEST_CHECK(!moq_pub_namespace_accepted(pub, quiet));

    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    tcfg.advertise_namespace = true;
    moq_pub_track_t *track = NULL;
    moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &track);

    /* Pending: announced, not yet accepted. */
    MOQ_TEST_CHECK(!moq_pub_namespace_accepted(pub, track));

    /* A track owned by a different publisher is false. */
    moq_pub_cfg_t cfg2;
    moq_pub_cfg_init_sized(&cfg2, sizeof(cfg2));
    moq_publisher_t *pub2 = NULL;
    moq_pub_create(moq_simpair_server(sp), &alloc, &cfg2, &pub2);
    MOQ_TEST_CHECK(!moq_pub_namespace_accepted(pub2, track));
    moq_pub_destroy(pub2);

    /* Pump PUBLISH_NAMESPACE to client; client accepts. */
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1) {
          MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED);
          moq_accept_namespace_cfg_t acc;
          moq_accept_namespace_cfg_init(&acc);
          moq_session_accept_namespace(moq_simpair_client(sp),
              ev.u.namespace_published.ann, &acc, moq_simpair_now_us(sp));
          moq_event_cleanup(&ev);
      }
    }
    /* Still pending until the facade consumes NAMESPACE_ACCEPTED. */
    MOQ_TEST_CHECK(!moq_pub_namespace_accepted(pub, track));

    /* Pump NAMESPACE_ACCEPTED back to server and feed the facade. */
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
          moq_pub_event_result_t res;
          moq_pub_handle_event(pub, &ev, moq_simpair_now_us(sp), &res);
          moq_event_cleanup(&ev);
      }
    }
    /* Now accepted. */
    MOQ_TEST_CHECK(moq_pub_namespace_accepted(pub, track));

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("namespace_accepted_query");
}

static void test_namespace_accepted_query_rejected(void) {
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
    tcfg.advertise_namespace = true;
    moq_pub_track_t *track = NULL;
    moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &track);

    /* Pump PUBLISH_NAMESPACE; client rejects. */
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1) {
          moq_reject_namespace_cfg_t rej;
          moq_reject_namespace_cfg_init(&rej);
          rej.error_code = MOQ_REQUEST_ERROR_UNAUTHORIZED;
          moq_session_reject_namespace(moq_simpair_client(sp),
              ev.u.namespace_published.ann, &rej, moq_simpair_now_us(sp));
          moq_event_cleanup(&ev);
      }
    }
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
          moq_pub_event_result_t res;
          moq_pub_handle_event(pub, &ev, moq_simpair_now_us(sp), &res);
          moq_event_cleanup(&ev);
      }
    }
    /* Rejected (TERMINAL) is never accepted. */
    MOQ_TEST_CHECK(!moq_pub_namespace_accepted(pub, track));

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("namespace_accepted_query_rejected");
}

static void test_namespace_pending_remove_blocked(void) {
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
    tcfg.advertise_namespace = true;

    moq_pub_track_t *track = NULL;
    moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &track);

    /* Do NOT pump NAMESPACE_ACCEPTED. Namespace is still PENDING. */

    /* remove_track should return WRONG_STATE. */
    moq_result_t rc = moq_pub_remove_track(pub, track,
        moq_simpair_now_us(sp));
    MOQ_TEST_CHECK(rc == MOQ_ERR_WRONG_STATE);

    /* Track still alive — destroy cleans it up. */
    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("namespace_pending_remove_blocked");
}

static void test_namespace_rejected_updates_state(void) {
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
    tcfg.advertise_namespace = true;

    moq_pub_track_t *track = NULL;
    moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &track);

    /* Pump PUBLISH_NAMESPACE to client. */
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1) {
          /* Client rejects the namespace. */
          moq_reject_namespace_cfg_t rej;
          moq_reject_namespace_cfg_init(&rej);
          rej.error_code = MOQ_REQUEST_ERROR_UNAUTHORIZED;
          moq_session_reject_namespace(moq_simpair_client(sp),
              ev.u.namespace_published.ann, &rej, moq_simpair_now_us(sp));
          moq_event_cleanup(&ev);
      }
    }

    /* Pump NAMESPACE_REJECTED back to server. */
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
          moq_pub_event_result_t res;
          moq_pub_handle_event(pub, &ev, moq_simpair_now_us(sp), &res);
          MOQ_TEST_CHECK(res == MOQ_PUB_EVENT_CONSUMED);
          moq_event_cleanup(&ev);
      }
    }

    /* Namespace is now TERMINAL. remove_track should succeed without
     * trying to send PUBLISH_NAMESPACE_DONE. */
    moq_result_t rc = moq_pub_remove_track(pub, track,
        moq_simpair_now_us(sp));
    MOQ_TEST_CHECK(rc == MOQ_OK);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("namespace_rejected_updates_state");
}

/* A namespace whose advertisement went terminal (rejected) must be advertised
 * again when a new advertised track for the same namespace is added, even while
 * the original track still holds the terminal entry's refcount. */
static void test_namespace_terminal_readvertises(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    moq_publisher_t *pub = NULL;
    moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub);

    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init(&tcfg);
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    tcfg.advertise_namespace = true;

    moq_pub_track_t *t1 = NULL;
    moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &t1);

    /* Client rejects the first advertisement -> entry goes terminal. Both setup
     * steps are mandatory: if the advertisement or the rejection were silently
     * dropped, the final poll could observe the still-pending original instead
     * of proving a true re-advertisement from a terminal entry. */
    moq_event_t ev;
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK(moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1);
    MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED);
    {
        moq_reject_namespace_cfg_t rej;
        moq_reject_namespace_cfg_init(&rej);
        rej.error_code = MOQ_REQUEST_ERROR_UNAUTHORIZED;
        moq_session_reject_namespace(moq_simpair_client(sp),
            ev.u.namespace_published.ann, &rej, moq_simpair_now_us(sp));
    }
    moq_event_cleanup(&ev);

    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK(moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1);
    MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NAMESPACE_REJECTED);
    {
        moq_pub_event_result_t res;
        moq_pub_handle_event(pub, &ev, moq_simpair_now_us(sp), &res);
        MOQ_TEST_CHECK(res == MOQ_PUB_EVENT_CONSUMED);
    }
    moq_event_cleanup(&ev);

    /* t1 stays alive (holds the terminal entry). Add a second advertised track
     * for the same namespace: it must emit a fresh PUBLISH_NAMESPACE rather than
     * silently sharing the terminal entry. */
    tcfg.track_name = MOQ_BYTES_LITERAL("audio");
    moq_pub_track_t *t2 = NULL;
    MOQ_TEST_CHECK(moq_pub_add_track(pub, &tcfg,
        moq_simpair_now_us(sp), &t2) == MOQ_OK);
    MOQ_TEST_CHECK(t2 != NULL);

    /* The client must see a new NAMESPACE_PUBLISHED for the re-advertisement. */
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK(moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1);
    MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED);
    moq_event_cleanup(&ev);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("namespace_terminal_readvertises");
}

static void test_flush_no_pending(void) {
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

static void test_flush_accept_would_block(void) {
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

static void test_flush_reject_would_block(void) {
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

static void test_remove_pending_ns_with_open_sg(void) {
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

    /* Pump PUBLISH_NAMESPACE to client but do NOT accept it.
     * Namespace stays PENDING. */
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev); }

    /* Subscribe and accept to get an active sub + open subgroup. */
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
    uint8_t d[] = { 0xAA };
    moq_rcbuf_t *p = NULL;
    moq_rcbuf_create(&alloc, d, 1, &p);
    moq_pub_write_object(pub, track, 0, 0, p, moq_simpair_now_us(sp));
    moq_rcbuf_decref(p);

    /* remove_track should fail with WRONG_STATE because namespace
     * is still PENDING. Crucially, no subgroup reset should have
     * been queued. */
    moq_result_t rc = moq_pub_remove_track(pub, track,
        moq_simpair_now_us(sp));
    MOQ_TEST_CHECK(rc == MOQ_ERR_WRONG_STATE);

    /* Subgroup should still be usable — write another object. */
    moq_rcbuf_t *p2 = NULL;
    moq_rcbuf_create(&alloc, d, 1, &p2);
    rc = moq_pub_write_object(pub, track, 0, 1, p2,
        moq_simpair_now_us(sp));
    MOQ_TEST_CHECK(rc == MOQ_OK);
    moq_rcbuf_decref(p2);

    /* Clean up: destroy without protocol teardown. */
    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("remove_pending_ns_with_open_sg");
}

static void test_remove_track_pending_accept(void) {
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

static void test_create_invalid_cfg(void) {
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

static void test_cross_publisher_track(void) {
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

static void test_namespace_overflow(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    moq_publisher_t *pub = NULL;
    moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub);

    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init(&tcfg);

    /* Two parts whose lengths sum to SIZE_MAX overflow. */
    moq_bytes_t huge_parts[2];
    uint8_t dummy = 0x41;
    huge_parts[0].data = &dummy;
    huge_parts[0].len = SIZE_MAX / 2 + 1;
    huge_parts[1].data = &dummy;
    huge_parts[1].len = SIZE_MAX / 2 + 1;
    tcfg.track_namespace.parts = huge_parts;
    tcfg.track_namespace.count = 2;
    tcfg.track_name = MOQ_BYTES_LITERAL("t");

    moq_pub_track_t *track = NULL;
    moq_result_t rc = moq_pub_add_track(pub, &tcfg, 0, &track);
    MOQ_TEST_CHECK(rc == MOQ_ERR_INVAL);
    MOQ_TEST_CHECK(track == NULL);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("namespace_overflow");
}

static void test_advertised_ns_duplicate(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    moq_publisher_t *pub = NULL;
    moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub);

    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };

    /* Track A with advertise. */
    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init(&tcfg);
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    tcfg.advertise_namespace = true;

    moq_pub_track_t *tA = NULL;
    MOQ_TEST_CHECK(moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp),
        &tA) == MOQ_OK);

    /* Track B same namespace, different name, also advertise → refcount
     * increments, no duplicate PUBLISH_NAMESPACE sent. */
    tcfg.track_name = MOQ_BYTES_LITERAL("audio");
    moq_pub_track_t *tB = NULL;
    MOQ_TEST_CHECK(moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp),
        &tB) == MOQ_OK);
    MOQ_TEST_CHECK(tB != NULL);

    moq_pub_remove_track(pub, tB, moq_simpair_now_us(sp));
    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("advertised_ns_duplicate");
}

static void test_pending_does_not_block_unmatched(void) {
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

static void test_end_group_stale_handle(void) {
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

    /* end_group should hit STALE_HANDLE from close_subgroup,
     * clear sg_open + has_sub, and return OK. */
    moq_result_t rc = moq_pub_end_group(pub, track,
        moq_simpair_now_us(sp));
    MOQ_TEST_CHECK(rc == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_active_subscriptions(pub, track) == 0);

    /* Subsequent write is a no-op (no subscription). */
    moq_rcbuf_t *p2 = NULL;
    moq_rcbuf_create(&alloc, d, 1, &p2);
    rc = moq_pub_write_object(pub, track, 1, 0, p2,
        moq_simpair_now_us(sp));
    MOQ_TEST_CHECK(rc == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_active_subscriptions(pub, track) == 0);
    moq_rcbuf_decref(p2);

    moq_pub_remove_track(pub, track, moq_simpair_now_us(sp));
    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("end_group_stale_handle");
}

static void test_advertised_ns_reuse_after_terminal(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    moq_publisher_t *pub = NULL;
    moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub);

    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };

    /* Track A: advertise → reject → TERMINAL. */
    moq_pub_track_cfg_t ta_cfg;
    moq_pub_track_cfg_init(&ta_cfg);
    ta_cfg.track_namespace.parts = ns_parts;
    ta_cfg.track_namespace.count = 1;
    ta_cfg.track_name = MOQ_BYTES_LITERAL("a");
    ta_cfg.advertise_namespace = true;

    moq_pub_track_t *tA = NULL;
    MOQ_TEST_CHECK(moq_pub_add_track(pub, &ta_cfg,
        moq_simpair_now_us(sp), &tA) == MOQ_OK);

    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1) {
          moq_reject_namespace_cfg_t rej;
          moq_reject_namespace_cfg_init(&rej);
          rej.error_code = MOQ_REQUEST_ERROR_UNAUTHORIZED;
          moq_session_reject_namespace(moq_simpair_client(sp),
              ev.u.namespace_published.ann, &rej, moq_simpair_now_us(sp));
          moq_event_cleanup(&ev);
      }
    }
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
          moq_pub_event_result_t res;
          moq_pub_handle_event(pub, &ev, moq_simpair_now_us(sp), &res);
          moq_event_cleanup(&ev);
      }
    }

    /* Track B: same namespace, different name, advertise. Should succeed
     * because A's namespace is TERMINAL, not PENDING/ACCEPTED. */
    moq_pub_track_cfg_t tb_cfg;
    moq_pub_track_cfg_init(&tb_cfg);
    tb_cfg.track_namespace.parts = ns_parts;
    tb_cfg.track_namespace.count = 1;
    tb_cfg.track_name = MOQ_BYTES_LITERAL("b");
    tb_cfg.advertise_namespace = true;

    moq_pub_track_t *tB = NULL;
    MOQ_TEST_CHECK(moq_pub_add_track(pub, &tb_cfg,
        moq_simpair_now_us(sp), &tB) == MOQ_OK);
    MOQ_TEST_CHECK(tB != NULL);

    /* Clean up. A has TERMINAL ns (remove OK), B has PENDING ns. */
    moq_pub_remove_track(pub, tA, moq_simpair_now_us(sp));
    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("advertised_ns_reuse_after_terminal");
}

static void test_remove_track_reset_would_block(void) {
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

static void test_destroy_with_active_pending(void) {
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

/* -- Callback mode helpers ----------------------------------------- */

static moq_pub_accept_decision_t cb_accept_all(
    void *ctx, const moq_pub_subscribe_info_t *info,
    moq_request_error_t *out_error) {
    (void)ctx; (void)info; (void)out_error;
    return MOQ_PUB_DECISION_ACCEPT;
}

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

typedef struct {
    bool called;
    moq_subscribe_filter_t filter;
    uint8_t subscriber_priority;
    bool forward;
    size_t ns_count;
    size_t name_len;
    size_t token_count;
    uint64_t first_token_type;
    size_t first_token_value_len;
    uint8_t first_token_value[16];
} cb_capture_t;

static moq_pub_accept_decision_t cb_capture(
    void *ctx, const moq_pub_subscribe_info_t *info,
    moq_request_error_t *out_error) {
    cb_capture_t *cap = (cb_capture_t *)ctx;
    (void)out_error;
    cap->called = true;
    cap->filter = info->filter;
    cap->subscriber_priority = info->subscriber_priority;
    cap->forward = info->forward;
    cap->ns_count = info->track_namespace.count;
    cap->name_len = info->track_name.len;
    cap->token_count = info->token_count;
    if (info->token_count > 0) {
        cap->first_token_type = info->tokens[0].token_type;
        cap->first_token_value_len = info->tokens[0].token_value.len;
        size_t copy = info->tokens[0].token_value.len;
        if (copy > sizeof(cap->first_token_value))
            copy = sizeof(cap->first_token_value);
        if (copy > 0 && info->tokens[0].token_value.data)
            memcpy(cap->first_token_value,
                   info->tokens[0].token_value.data, copy);
    }
    return MOQ_PUB_DECISION_ACCEPT;
}

/* -- Callback mode tests ------------------------------------------ */

static void test_callback_accept(void) {
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

static void test_callback_reject_custom_error(void) {
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

static void test_callback_reject_default_error(void) {
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

static void test_callback_sees_filter_and_ns(void) {
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

static void test_callback_sees_tokens(void) {
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

static void test_callback_would_block(void) {
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

static void test_callback_null_rejected(void) {
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

static void test_callback_invalid_decision(void) {
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

static void test_callback_not_called_for_duplicate(void) {
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

static void test_old_struct_size_accept_all(void) {
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

/* -- Main --------------------------------------------------------- */

static void test_publisher_priority_zero(void) {
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

static void test_publisher_priority_default(void) {
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

static void test_publisher_priority_old_struct(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    cfg.default_publisher_priority = 150;
    moq_publisher_t *pub = NULL;
    moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub);

    /* Simulate the original struct layout (before has_publisher_priority
     * was appended). pad_after_prio covers offsets 42..47 — the original
     * trailing padding, exactly where the mis-placed has_publisher_priority
     * once lived. Setting pad_after_prio[0] nonzero models an old caller
     * whose uninitialised padding happens to be set: the appended presence
     * bit now lives beyond this struct, so add_track must NOT read this
     * byte as "has explicit priority". */
    struct {
        uint32_t        struct_size;
        moq_namespace_t track_namespace;
        moq_bytes_t     track_name;
        bool            advertise_namespace;
        uint8_t         publisher_priority;
        uint8_t         pad_after_prio[6];
    } old_tcfg;
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
    old_tcfg.pad_after_prio[0] = 0xFF;  /* old has_publisher_priority slot */

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
static void test_pub_track_cfg_init_canary(void) {
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

static void test_oversized_namespace_rejected(void) {
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

static void test_oversized_full_track_name_rejected(void) {
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

/* ================================================================== */
/* Tick-based tests                                                   */
/* ================================================================== */

typedef struct {
    int joined;
    int left;
    int draining;
    int closed;
    int updated;
    uint64_t close_code;
    moq_pub_track_t *joined_track;
    moq_pub_track_t *left_track;
    moq_pub_track_t *updated_track;
    moq_pub_subscribe_update_info_t last_update;
} tick_cb_state_t;

static void tick_on_joined(void *ctx, moq_pub_track_t *track) {
    tick_cb_state_t *s = (tick_cb_state_t *)ctx;
    s->joined++;
    s->joined_track = track;
}
static void tick_on_left(void *ctx, moq_pub_track_t *track) {
    tick_cb_state_t *s = (tick_cb_state_t *)ctx;
    s->left++;
    s->left_track = track;
}
static void tick_on_draining(void *ctx) {
    tick_cb_state_t *s = (tick_cb_state_t *)ctx;
    s->draining++;
}
static void tick_on_updated(void *ctx, moq_pub_track_t *track,
                             const moq_pub_subscribe_update_info_t *info) {
    tick_cb_state_t *s = (tick_cb_state_t *)ctx;
    s->updated++;
    s->updated_track = track;
    s->last_update = *info;
}
static void tick_on_closed(void *ctx, uint64_t code) {
    tick_cb_state_t *s = (tick_cb_state_t *)ctx;
    s->closed++;
    s->close_code = code;
}

static void test_tick_subscribe_accept(void) {
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
static void test_pub_create_partial_callbacks(void) {
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
static void test_pub_streaming_no_subscriber_noop(void) {
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
    moq_pub_object_cfg_init(&ocfg);
    ocfg.group_id = 0;
    ocfg.object_id = 0;
    ocfg.payload = p0;
    MOQ_TEST_CHECK(moq_pub_write_object_ex(pub, track, &ocfg,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_rcbuf_decref(p0);

    /* Streaming sequence must be consistent: all three no-op OK. */
    moq_pub_begin_object_cfg_t bcfg;
    moq_pub_begin_object_cfg_init(&bcfg);
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

static void test_tick_unsubscribe_clears(void) {
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

static void test_tick_goaway_draining(void) {
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

static void test_tick_session_closed(void) {
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

static void test_tick_pending_retry_fires_joined(void) {
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

static void test_tick_pending_then_closed(void) {
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

/* ================================================================== */
/* write_object_ex tests                                              */
/* ================================================================== */

static void test_write_object_ex_stream(void) {
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
    moq_pub_object_cfg_t obj; moq_pub_object_cfg_init(&obj);
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

static void test_write_object_ex_datagram(void) {
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
    moq_pub_object_cfg_t obj; moq_pub_object_cfg_init(&obj);
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

static void test_write_object_ex_status_datagram(void) {
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

    moq_pub_object_cfg_t obj; moq_pub_object_cfg_init(&obj);
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

static void test_write_object_ex_invalid_combos(void) {
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
        moq_pub_object_cfg_t obj; moq_pub_object_cfg_init(&obj);
        obj.has_status = true;
        obj.status = MOQ_OBJECT_END_OF_GROUP;
        obj.payload = pay;
        MOQ_TEST_CHECK(moq_pub_write_object_ex(pub, track, &obj,
            moq_simpair_now_us(sp)) == MOQ_ERR_INVAL);
    }
    /* non-datagram, non-status, no payload = invalid */
    {
        moq_pub_object_cfg_t obj; moq_pub_object_cfg_init(&obj);
        MOQ_TEST_CHECK(moq_pub_write_object_ex(pub, track, &obj,
            moq_simpair_now_us(sp)) == MOQ_ERR_INVAL);
    }
    /* properties on non-datagram: now valid (opens subgroup with extensions) */
    {
        moq_pub_object_cfg_t obj; moq_pub_object_cfg_init(&obj);
        obj.payload = pay;
        obj.properties = pay;
        MOQ_TEST_CHECK(moq_pub_write_object_ex(pub, track, &obj,
            moq_simpair_now_us(sp)) == MOQ_OK);
    }
    /* NULL args */
    {
        moq_pub_object_cfg_t obj; moq_pub_object_cfg_init(&obj);
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
        moq_pub_begin_object_cfg_init(&bcfg);
        bcfg.payload_length = UINT64_MAX;
        MOQ_TEST_CHECK(moq_pub_begin_object(pub, track, &bcfg,
            moq_simpair_now_us(sp)) == MOQ_ERR_INVAL);
    }
    /* non-datagram status = invalid (not supported in this slice) */
    {
        moq_pub_object_cfg_t obj; moq_pub_object_cfg_init(&obj);
        obj.has_status = true;
        obj.status = MOQ_OBJECT_END_OF_GROUP;
        MOQ_TEST_CHECK(moq_pub_write_object_ex(pub, track, &obj,
            moq_simpair_now_us(sp)) == MOQ_ERR_INVAL);
    }
    /* invalid status enum = invalid even without subscriber */
    {
        moq_pub_object_cfg_t obj; moq_pub_object_cfg_init(&obj);
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

static void test_streaming_lifecycle(void) {
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
    moq_pub_begin_object_cfg_init(&bcfg);
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
    moq_pub_begin_object_cfg_init(&bcfg);
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

static void test_streaming_invalid_order(void) {
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
    moq_pub_begin_object_cfg_init(&bcfg);
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

static void test_remove_track_while_streaming(void) {
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
    moq_pub_begin_object_cfg_init(&bcfg);
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
        moq_simpair_now_us(sp)) == MOQ_OK);

    /* Verify RESET_DATA was queued. */
    moq_action_t acts[16];
    size_t na = moq_session_poll_actions(moq_simpair_server(sp), acts, 16);
    bool found_reset = false;
    for (size_t i = 0; i < na; i++) {
        if (acts[i].kind == MOQ_ACTION_RESET_DATA) found_reset = true;
        moq_action_cleanup(&acts[i]);
    }
    MOQ_TEST_CHECK(found_reset);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("remove_track_while_streaming");
}

static void test_unsubscribe_then_write_noop(void) {
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

static void test_slot_reuse_after_unsub(void) {
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

static void test_second_subscriber_rejected_v1(void) {
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

static void test_unsub_clears_slot_and_resubscribe(void) {
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

static void test_session_rejects_same_track_duplicate(void) {
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

static void test_unsub_event_clears_slot(void) {
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

/* -- Deferred authorization tests --------------------------------- */

static moq_pub_deferred_t *last_deferred = NULL;
static uint64_t last_deferred_id = 0;

static moq_pub_accept_decision_t cb_defer(
    void *ctx, const moq_pub_subscribe_info_t *info,
    moq_request_error_t *out_error)
{
    (void)ctx; (void)out_error;
    last_deferred = info->deferred;
    last_deferred_id = info->deferred_id;
    return MOQ_PUB_DECISION_DEFER;
}

/* RED for p3-54: when the peer unsubscribes, the core emits UNSUBSCRIBED and
 * frees the subscription. moq_pub_handle_event must retire the matching per-track
 * slot (fire on_subscriber_left), so active-subscription state is accurate and
 * the track can be re-subscribed -- not rejected as a duplicate. */
static void test_unsubscribed_retires_slot(void) {
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

static void test_deferred_accept(void) {
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

static void test_deferred_reject(void) {
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

/* RED for p3-53: resolving a deferred subscribe must not clobber an earlier
 * backpressured accept/reject sitting in the single pending slot. Defer
 * subscribe #1, then backpressure subscribe #2's accept into pub->pending, then
 * resolve_deferred(#1) while the queue is still full. The deferred resolution
 * must NOT overwrite pending (which would orphan #2); it returns WOULD_BLOCK
 * with the deferred still active, so a drain + flush + retry resolves both. */
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

static void test_deferred_resolve_no_pending_clobber(void) {
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

    /* Subscribe #2 (track t2) -> ACCEPT, but the queue is full -> parked in
     * pub->pending. */
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

    /* Resolve the deferred #1 while pending(#2) is occupied and the queue full.
     * Must return WOULD_BLOCK WITHOUT clobbering pending(#2). */
    MOQ_TEST_CHECK_EQ_INT((int)moq_pub_resolve_deferred(pub, last_deferred,
        last_deferred_id, true, 0, 0), (int)MOQ_ERR_WOULD_BLOCK);

    /* Drain the blocker, flush pending(#2), then retry the deferred(#1). The
     * deferred must still be active -> MOQ_OK; if it were cleared, #2 would be
     * orphaned, the retry would be STALE_HANDLE, and only one sub would
     * survive. */
    { moq_action_t acts[4]; size_t na;
      while ((na = moq_session_poll_actions(server, acts, 4)) > 0)
          for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]); }
    MOQ_TEST_CHECK_EQ_INT((int)moq_pub_flush(pub, 0), (int)MOQ_OK);
    { moq_action_t acts[4]; size_t na;
      while ((na = moq_session_poll_actions(server, acts, 4)) > 0)
          for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]); }
    MOQ_TEST_CHECK_EQ_INT((int)moq_pub_resolve_deferred(pub, last_deferred,
        last_deferred_id, true, 0, 0), (int)MOQ_OK);

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
    MOQ_TEST_PASS("deferred_resolve_no_pending_clobber");
}

/* A track ended before any subscriber must reject a late DIRECT subscribe
 * (terminal track), not install a slot that gets neither media nor terminal. */
static void test_end_track_rejects_late_subscribe(void) {
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
static void test_end_track_rejects_deferred_subscribe(void) {
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

static void test_deferred_would_block_then_flush(void) {
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

static void test_deferred_remove_track_blocked(void) {
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

static void test_deferred_session_close_releases(void) {
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

static void test_deferred_does_not_block_other_tracks(void) {
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

static void test_deferred_unsub_before_resolve(void) {
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

static void test_deferred_stale_id_after_reuse(void) {
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

static void test_deferred_flush_fires_joined(void) {
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

static void test_deferred_old_cfg_struct_size(void) {
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

/* -- Subscription update callback tests ----------------------------- */

static void test_update_callback_fires(void) {
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

static void test_update_old_callback_struct_size(void) {
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

static void test_update_after_unsubscribe_ignored(void) {
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

static void test_update_during_deferred(void) {
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

static void test_old_cfg_size_callbacks_still_fire(void) {
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

/* -- Namespace refcounting tests ------------------------------------ */

static void test_ns_refcount_shared(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    moq_publisher_t *pub = NULL;
    moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub);

    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init(&tcfg);
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.advertise_namespace = true;

    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_pub_track_t *t1 = NULL;
    MOQ_TEST_CHECK_EQ_INT(moq_pub_add_track(pub, &tcfg,
        moq_simpair_now_us(sp), &t1), MOQ_OK);

    tcfg.track_name = MOQ_BYTES_LITERAL("audio");
    moq_pub_track_t *t2 = NULL;
    MOQ_TEST_CHECK_EQ_INT(moq_pub_add_track(pub, &tcfg,
        moq_simpair_now_us(sp), &t2), MOQ_OK);

    /* Pump namespace to client — only one PUBLISH_NAMESPACE arrives. */
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    {
        moq_event_t evts[4];
        size_t ne = moq_session_poll_events(moq_simpair_client(sp),
            evts, 4);
        int ns_count = 0;
        for (size_t i = 0; i < ne; i++) {
            if (evts[i].kind == MOQ_EVENT_NAMESPACE_PUBLISHED) {
                ns_count++;
                moq_accept_namespace_cfg_t nacc;
                moq_accept_namespace_cfg_init(&nacc);
                moq_session_accept_namespace(moq_simpair_client(sp),
                    evts[i].u.namespace_published.ann, &nacc,
                    moq_simpair_now_us(sp));
            }
            moq_event_cleanup(&evts[i]);
        }
        MOQ_TEST_CHECK_EQ_INT(ns_count, 1);
    }
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_pub_tick(pub, moq_simpair_now_us(sp));
    { moq_action_t da[16]; size_t dn;
      while ((dn = moq_session_poll_actions(moq_simpair_server(sp),
              da, 16)) > 0)
          for (size_t i = 0; i < dn; i++) moq_action_cleanup(&da[i]); }

    /* Removing first track does not send PUBLISH_NAMESPACE_DONE. */
    MOQ_TEST_CHECK_EQ_INT(moq_pub_remove_track(pub, t1,
        moq_simpair_now_us(sp)), MOQ_OK);
    {
        moq_action_t acts[16]; size_t na;
        na = moq_session_poll_actions(moq_simpair_server(sp), acts, 16);
        bool found_done = false;
        for (size_t i = 0; i < na; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL &&
                decode_action_msg_type(&acts[i]) == MOQ_D16_PUBLISH_NAMESPACE_DONE)
                found_done = true;
            moq_action_cleanup(&acts[i]);
        }
        MOQ_TEST_CHECK(!found_done);
    }

    /* Removing second (last) track sends PUBLISH_NAMESPACE_DONE. */
    MOQ_TEST_CHECK_EQ_INT(moq_pub_remove_track(pub, t2,
        moq_simpair_now_us(sp)), MOQ_OK);
    {
        moq_action_t acts[16]; size_t na;
        na = moq_session_poll_actions(moq_simpair_server(sp), acts, 16);
        bool found_done = false;
        for (size_t i = 0; i < na; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL &&
                decode_action_msg_type(&acts[i]) == MOQ_D16_PUBLISH_NAMESPACE_DONE)
                found_done = true;
            moq_action_cleanup(&acts[i]);
        }
        MOQ_TEST_CHECK(found_done);
    }

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("ns_refcount_shared");
}

static void test_ns_refcount_distinct(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    moq_publisher_t *pub = NULL;
    moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub);

    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init(&tcfg);
    tcfg.advertise_namespace = true;

    moq_bytes_t ns1[] = { MOQ_BYTES_LITERAL("live") };
    tcfg.track_namespace.parts = ns1;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_pub_track_t *t1 = NULL;
    moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &t1);

    moq_bytes_t ns2[] = { MOQ_BYTES_LITERAL("archive") };
    tcfg.track_namespace.parts = ns2;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_pub_track_t *t2 = NULL;
    moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &t2);

    /* Two distinct PUBLISH_NAMESPACE actions. */
    {
        moq_action_t acts[16]; size_t na;
        na = moq_session_poll_actions(moq_simpair_server(sp), acts, 16);
        int ns_count = 0;
        for (size_t i = 0; i < na; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL &&
                decode_action_msg_type(&acts[i]) == MOQ_D16_PUBLISH_NAMESPACE)
                ns_count++;
            moq_action_cleanup(&acts[i]);
        }
        MOQ_TEST_CHECK_EQ_INT(ns_count, 2);
    }

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("ns_refcount_distinct");
}

static void test_ns_refcount_would_block_retry(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    moq_publisher_t *pub = NULL;
    moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub);

    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init(&tcfg);
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    tcfg.advertise_namespace = true;
    moq_pub_track_t *track = NULL;
    MOQ_TEST_CHECK_EQ_INT(moq_pub_add_track(pub, &tcfg,
        moq_simpair_now_us(sp), &track), MOQ_OK);

    /* Accept namespace. */
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1) {
          moq_accept_namespace_cfg_t nacc;
          moq_accept_namespace_cfg_init(&nacc);
          moq_session_accept_namespace(moq_simpair_client(sp),
              ev.u.namespace_published.ann, &nacc, moq_simpair_now_us(sp));
          moq_event_cleanup(&ev);
      } }
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_pub_tick(pub, moq_simpair_now_us(sp));
    { moq_action_t da[16]; size_t dn;
      while ((dn = moq_session_poll_actions(moq_simpair_server(sp),
              da, 16)) > 0)
          for (size_t i = 0; i < dn; i++) moq_action_cleanup(&da[i]); }

    /* Fill the action queue so PUBLISH_NAMESPACE_DONE will WOULD_BLOCK. */
    test_session_fill_action_queue(moq_simpair_server(sp));

    MOQ_TEST_CHECK_EQ_INT(moq_pub_remove_track(pub, track,
        moq_simpair_now_us(sp)), MOQ_ERR_WOULD_BLOCK);

    /* Drain and retry — track is still live so retry succeeds. */
    { moq_action_t da[64]; size_t dn;
      while ((dn = moq_session_poll_actions(moq_simpair_server(sp),
              da, 64)) > 0)
          for (size_t i = 0; i < dn; i++) moq_action_cleanup(&da[i]); }
    MOQ_TEST_CHECK_EQ_INT(moq_pub_remove_track(pub, track,
        moq_simpair_now_us(sp)), MOQ_OK);

    /* Verify exactly one PUBLISH_NAMESPACE_DONE was queued. */
    {
        moq_action_t acts[16]; size_t na;
        na = moq_session_poll_actions(moq_simpair_server(sp), acts, 16);
        int done_count = 0;
        for (size_t i = 0; i < na; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL &&
                decode_action_msg_type(&acts[i]) ==
                    MOQ_D16_PUBLISH_NAMESPACE_DONE)
                done_count++;
            moq_action_cleanup(&acts[i]);
        }
        MOQ_TEST_CHECK_EQ_INT(done_count, 1);
    }

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("ns_refcount_would_block_retry");
}

/* -- Publisher-initiated PUBLISH ---------------------------------- */

/* On the client (the subscriber side of the simpair), drain one
 * PUBLISH_REQUEST and accept it, optionally forcing the forward flag. Returns
 * true if a request was accepted. */
static bool client_accept_publish(moq_simpair_t *sp) {
    moq_event_t ev;
    if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) != 1)
        return false;
    bool accepted = false;
    if (ev.kind == MOQ_EVENT_PUBLISH_REQUEST) {
        moq_accept_publish_cfg_t acc;
        moq_accept_publish_cfg_init(&acc);
        moq_result_t rc = moq_session_accept_publish(moq_simpair_client(sp),
            ev.u.publish_request.pub, &acc, moq_simpair_now_us(sp));
        MOQ_TEST_CHECK(rc == MOQ_OK);
        accepted = true;
    }
    moq_event_cleanup(&ev);
    return accepted;
}

static void test_publish_track_accepted(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    moq_pub_cfg_t cfg; moq_pub_cfg_init(&cfg);
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    moq_publisher_t *pub = NULL;
    moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub);

    moq_pub_track_cfg_t tcfg; moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    tcfg.track_namespace.parts = ns_parts; tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_pub_track_t *track = NULL;
    MOQ_TEST_CHECK(moq_pub_add_track(pub, &tcfg,
        moq_simpair_now_us(sp), &track) == MOQ_OK);

    /* Publish the track (publisher-initiated PUBLISH). */
    moq_pub_publish_cfg_t pcfg; moq_pub_publish_cfg_init(&pcfg);
    MOQ_TEST_CHECK(moq_pub_publish_track(pub, track,
        &pcfg, moq_simpair_now_us(sp)) == MOQ_OK);
    MOQ_TEST_CHECK(!moq_pub_track_is_published(pub, track));

    /* Client receives PUBLISH and accepts -> PUBLISH_OK. */
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK(client_accept_publish(sp));

    /* Server tick consumes PUBLISH_OK -> published, forward defaults true. */
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK(moq_pub_tick(pub, moq_simpair_now_us(sp)) == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_track_is_published(pub, track));
    MOQ_TEST_CHECK(moq_pub_track_forward(pub, track));

    /* Idempotent: a second publish_track is a no-op MOQ_OK. */
    MOQ_TEST_CHECK(moq_pub_publish_track(pub, track,
        &pcfg, moq_simpair_now_us(sp)) == MOQ_OK);

    /* A published track is NOT counted as a subscriber. */
    MOQ_TEST_CHECK(moq_pub_active_subscriptions(pub, track) == 0);
    MOQ_TEST_CHECK(!moq_pub_has_subscriber(pub, track));

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("publish_track_accepted");
}

/* Write objects on a published track: they must reach the publication's data
 * stream (the write fan-out routes to the publication slot). */
static void test_publish_track_write(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    moq_pub_cfg_t cfg; moq_pub_cfg_init(&cfg);
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    moq_publisher_t *pub = NULL;
    moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub);

    moq_pub_track_cfg_t tcfg; moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    tcfg.track_namespace.parts = ns_parts; tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_pub_track_t *track = NULL;
    moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &track);

    moq_pub_publish_cfg_t pcfg; moq_pub_publish_cfg_init(&pcfg);
    moq_pub_publish_track(pub, track, &pcfg, moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK(client_accept_publish(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_pub_tick(pub, moq_simpair_now_us(sp));
    MOQ_TEST_CHECK(moq_pub_track_is_published(pub, track));

    /* Write one object; routed to the publication slot. */
    moq_rcbuf_t *payload = NULL;
    moq_rcbuf_create(&alloc, (const uint8_t *)"abcd", 4, &payload);
    moq_result_t rc = moq_pub_write_object(pub, track, 0, 0, payload,
        moq_simpair_now_us(sp));
    MOQ_TEST_CHECK(rc == MOQ_OK);
    moq_rcbuf_decref(payload);

    moq_simpair_run_until_quiescent(sp, 8, NULL);

    /* end_track on a published track finishes the publication (PUBLISH_DONE). */
    rc = moq_pub_end_track(pub, track, moq_simpair_now_us(sp));
    MOQ_TEST_CHECK(rc == MOQ_OK);
    MOQ_TEST_CHECK(!moq_pub_track_is_published(pub, track));

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("publish_track_write");
}

/* Coexistence: one track is BOTH advertised (receives SUBSCRIBE) AND published.
 * Proves PUBLISH is an operation, not an exclusive mode. */
static void test_publish_and_subscribe_coexist(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);

    moq_pub_cfg_t cfg; moq_pub_cfg_init(&cfg);
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    moq_publisher_t *pub = NULL;
    moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub);

    moq_pub_track_cfg_t tcfg; moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    tcfg.track_namespace.parts = ns_parts; tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    tcfg.advertise_namespace = true;          /* announce ... */
    moq_pub_track_t *track = NULL;
    moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &track);

    /* ... AND publish the same track. */
    moq_pub_publish_cfg_t pcfg; moq_pub_publish_cfg_init(&pcfg);
    MOQ_TEST_CHECK(moq_pub_publish_track(pub, track,
        &pcfg, moq_simpair_now_us(sp)) == MOQ_OK);

    moq_simpair_run_until_quiescent(sp, 8, NULL);

    /* Client accepts the namespace and the publish, in whatever order they
     * surface; drain its event queue. */
    for (int i = 0; i < 4; i++) {
        moq_event_t ev;
        if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) != 1)
            break;
        if (ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED) {
            moq_accept_namespace_cfg_t nacc;
            moq_accept_namespace_cfg_init(&nacc);
            moq_session_accept_namespace(moq_simpair_client(sp),
                ev.u.namespace_published.ann, &nacc, moq_simpair_now_us(sp));
        } else if (ev.kind == MOQ_EVENT_PUBLISH_REQUEST) {
            moq_accept_publish_cfg_t acc;
            moq_accept_publish_cfg_init(&acc);
            moq_session_accept_publish(moq_simpair_client(sp),
                ev.u.publish_request.pub, &acc, moq_simpair_now_us(sp));
        }
        moq_event_cleanup(&ev);
    }

    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK(moq_pub_tick(pub, moq_simpair_now_us(sp)) == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_namespace_accepted(pub, track));
    MOQ_TEST_CHECK(moq_pub_track_is_published(pub, track));

    /* Now a SUBSCRIBE for the same track is also accepted (coexists). */
    moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace.parts = ns_parts; scfg.track_namespace.count = 1;
    scfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_subscription_t sub_h;
    MOQ_TEST_CHECK(moq_session_subscribe(moq_simpair_client(sp), &scfg,
        moq_simpair_now_us(sp), &sub_h) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK(moq_pub_tick(pub, moq_simpair_now_us(sp)) == MOQ_OK);

    MOQ_TEST_CHECK(moq_pub_has_subscriber(pub, track));     /* subscription slot */
    MOQ_TEST_CHECK(moq_pub_track_is_published(pub, track)); /* publication intact */

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("publish_and_subscribe_coexist");
}

int main(void) {
    test_create_destroy();
    test_pub_cfg_init_old_prefix_no_overflow();
    test_add_remove_track();
    test_subscribe_accept_all();
    test_remove_track_retires_subscription();
    test_subscribe_no_match();
    test_subscribe_duplicate_rejected();
    test_write_single_sub();
    test_end_track();
    test_finish_subscribers();
    test_finish_subscribers_not_terminal();
    test_finish_subscribers_would_block();
    test_write_no_subs();
    test_write_group_change();
    test_end_group();
    test_remove_track_with_open_sg();
    test_destroy_with_live_subs();
    test_event_ignored();
    test_session_closed_event();
    test_invalid_namespace_inputs();
    test_strict_alloc_sizes();
    test_reject_all_mode();
    test_duplicate_track_rejected();
    test_namespace_accepted_then_remove();
    test_reset_group();
    test_namespace_accepted_query();
    test_namespace_accepted_query_rejected();
    test_namespace_pending_remove_blocked();
    test_namespace_rejected_updates_state();
    test_namespace_terminal_readvertises();
    test_flush_no_pending();
    test_flush_accept_would_block();
    test_flush_reject_would_block();
    test_remove_pending_ns_with_open_sg();
    test_remove_track_pending_accept();
    test_create_invalid_cfg();
    test_cross_publisher_track();
    test_namespace_overflow();
    test_advertised_ns_duplicate();
    test_pending_does_not_block_unmatched();
    test_end_group_stale_handle();
    test_remove_track_reset_would_block();
    test_advertised_ns_reuse_after_terminal();
    test_destroy_with_active_pending();
    test_callback_accept();
    test_callback_reject_custom_error();
    test_callback_reject_default_error();
    test_callback_sees_filter_and_ns();
    test_callback_sees_tokens();
    test_callback_would_block();
    test_callback_null_rejected();
    test_callback_invalid_decision();
    test_callback_not_called_for_duplicate();
    test_old_struct_size_accept_all();
    test_publisher_priority_zero();
    test_publisher_priority_default();
    test_publisher_priority_old_struct();
    test_pub_track_cfg_init_canary();
    test_oversized_namespace_rejected();
    test_oversized_full_track_name_rejected();
    test_tick_subscribe_accept();
    test_pub_create_partial_callbacks();
    test_pub_streaming_no_subscriber_noop();
    test_tick_unsubscribe_clears();
    test_tick_goaway_draining();
    test_tick_session_closed();
    test_tick_pending_retry_fires_joined();
    test_tick_pending_then_closed();
    test_write_object_ex_stream();
    test_write_object_ex_datagram();
    test_write_object_ex_status_datagram();
    test_write_object_ex_invalid_combos();
    test_streaming_lifecycle();
    test_streaming_invalid_order();
    test_remove_track_while_streaming();
    test_unsubscribe_then_write_noop();
    test_slot_reuse_after_unsub();
    test_unsub_clears_slot_and_resubscribe();
    test_second_subscriber_rejected_v1();
    test_session_rejects_same_track_duplicate();
    test_unsub_event_clears_slot();
    test_unsubscribed_retires_slot();
    test_deferred_accept();
    test_deferred_reject();
    test_deferred_resolve_no_pending_clobber();
    test_end_track_rejects_late_subscribe();
    test_end_track_rejects_deferred_subscribe();
    test_deferred_would_block_then_flush();
    test_deferred_remove_track_blocked();
    test_deferred_session_close_releases();
    test_deferred_does_not_block_other_tracks();
    test_deferred_unsub_before_resolve();
    test_deferred_stale_id_after_reuse();
    test_deferred_flush_fires_joined();
    test_deferred_old_cfg_struct_size();
    test_update_callback_fires();
    test_update_old_callback_struct_size();
    test_update_after_unsubscribe_ignored();
    test_update_during_deferred();
    test_old_cfg_size_callbacks_still_fire();
    test_ns_refcount_shared();
    test_ns_refcount_distinct();
    test_ns_refcount_would_block_retry();
    test_publish_track_accepted();
    test_publish_track_write();
    test_publish_and_subscribe_coexist();

    /* == end_of_group ABI: old struct_size must not read end_of_group == */
    {
        test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
        simpair_setup(&as, &alloc, &sp);

        moq_pub_cfg_t pcfg; moq_pub_cfg_init_sized(&pcfg, sizeof(pcfg));
        pcfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
        moq_publisher_t *pub = NULL;
        moq_pub_create(moq_simpair_server(sp), &alloc, &pcfg, &pub);

        moq_pub_track_cfg_t tcfg; moq_pub_track_cfg_init(&tcfg);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
        tcfg.track_namespace.parts = ns_parts; tcfg.track_namespace.count = 1;
        tcfg.track_name = MOQ_BYTES_LITERAL("eog_abi");
        moq_pub_track_t *track = NULL;
        moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &track);

        moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace.parts = ns_parts; scfg.track_namespace.count = 1;
        scfg.track_name = MOQ_BYTES_LITERAL("eog_abi");
        moq_subscription_t sub_h;
        moq_session_subscribe(moq_simpair_client(sp), &scfg,
            moq_simpair_now_us(sp), &sub_h);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        moq_pub_tick(pub, moq_simpair_now_us(sp));
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev;
          while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
              moq_event_cleanup(&ev); }

        /* Old caller: struct_size excludes end_of_group. Dirty padding. */
        moq_pub_object_cfg_t obj;
        memset(&obj, 0xFF, sizeof(obj));
        obj.struct_size = offsetof(moq_pub_object_cfg_t, _reserved_obj);
        obj.group_id = 0; obj.object_id = 0;
        obj.datagram = false; obj.has_status = false;
        uint8_t data[] = "hello";
        moq_rcbuf_t *pl = NULL;
        moq_rcbuf_create(&alloc, data, 5, &pl);
        obj.payload = pl; obj.properties = NULL;

        MOQ_TEST_CHECK(moq_pub_write_object_ex(pub, track, &obj,
            moq_simpair_now_us(sp)) == MOQ_OK);

        /* Verify EOG bit (0x08) is NOT set despite garbage padding. */
        moq_action_t act;
        MOQ_TEST_CHECK(moq_session_poll_actions(moq_simpair_server(sp), &act, 1) == 1);
        MOQ_TEST_CHECK(act.kind == MOQ_ACTION_SEND_DATA);
        MOQ_TEST_CHECK((act.u.send_data.header[0] & 0x08) == 0);
        moq_action_cleanup(&act);
        while (moq_session_poll_actions(moq_simpair_server(sp), &act, 1) == 1)
            moq_action_cleanup(&act);

        moq_rcbuf_decref(pl);
        moq_pub_destroy(pub);
        drain_all(sp);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
        MOQ_TEST_PASS("end_of_group ABI old struct_size");
    }

    /* == end_of_group mismatch: same-group true->false rejected ======= */
    {
        test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
        simpair_setup(&as, &alloc, &sp);

        moq_pub_cfg_t pcfg; moq_pub_cfg_init_sized(&pcfg, sizeof(pcfg));
        pcfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
        moq_publisher_t *pub = NULL;
        moq_pub_create(moq_simpair_server(sp), &alloc, &pcfg, &pub);

        moq_pub_track_cfg_t tcfg; moq_pub_track_cfg_init(&tcfg);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
        tcfg.track_namespace.parts = ns_parts; tcfg.track_namespace.count = 1;
        tcfg.track_name = MOQ_BYTES_LITERAL("eog_mm1");
        moq_pub_track_t *track = NULL;
        moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &track);

        moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace.parts = ns_parts; scfg.track_namespace.count = 1;
        scfg.track_name = MOQ_BYTES_LITERAL("eog_mm1");
        moq_subscription_t sub_h;
        moq_session_subscribe(moq_simpair_client(sp), &scfg,
            moq_simpair_now_us(sp), &sub_h);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        moq_pub_tick(pub, moq_simpair_now_us(sp));
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev;
          while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
              moq_event_cleanup(&ev); }

        uint8_t data[] = "hi";
        moq_rcbuf_t *pl = NULL;
        moq_rcbuf_create(&alloc, data, 2, &pl);

        moq_pub_object_cfg_t obj; moq_pub_object_cfg_init(&obj);
        obj.group_id = 0; obj.object_id = 0;
        obj.payload = pl; obj.end_of_group = true;
        MOQ_TEST_CHECK(moq_pub_write_object_ex(pub, track, &obj,
            moq_simpair_now_us(sp)) == MOQ_OK);

        drain_all(sp);

        /* Same group, object 1 with end_of_group=false: mismatch. */
        moq_rcbuf_t *pl2 = NULL;
        moq_rcbuf_create(&alloc, data, 2, &pl2);
        moq_pub_object_cfg_init(&obj);
        obj.group_id = 0; obj.object_id = 1;
        obj.payload = pl2; obj.end_of_group = false;
        MOQ_TEST_CHECK(moq_pub_write_object_ex(pub, track, &obj,
            moq_simpair_now_us(sp)) == MOQ_ERR_WRONG_STATE);

        moq_rcbuf_decref(pl); moq_rcbuf_decref(pl2);
        moq_pub_destroy(pub);
        drain_all(sp);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
        MOQ_TEST_PASS("end_of_group mismatch true->false");
    }

    /* == end_of_group mismatch: same-group false->true rejected ======= */
    {
        test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
        simpair_setup(&as, &alloc, &sp);

        moq_pub_cfg_t pcfg; moq_pub_cfg_init_sized(&pcfg, sizeof(pcfg));
        pcfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
        moq_publisher_t *pub = NULL;
        moq_pub_create(moq_simpair_server(sp), &alloc, &pcfg, &pub);

        moq_pub_track_cfg_t tcfg; moq_pub_track_cfg_init(&tcfg);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
        tcfg.track_namespace.parts = ns_parts; tcfg.track_namespace.count = 1;
        tcfg.track_name = MOQ_BYTES_LITERAL("eog_mm2");
        moq_pub_track_t *track = NULL;
        moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &track);

        moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace.parts = ns_parts; scfg.track_namespace.count = 1;
        scfg.track_name = MOQ_BYTES_LITERAL("eog_mm2");
        moq_subscription_t sub_h;
        moq_session_subscribe(moq_simpair_client(sp), &scfg,
            moq_simpair_now_us(sp), &sub_h);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        moq_pub_tick(pub, moq_simpair_now_us(sp));
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev;
          while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
              moq_event_cleanup(&ev); }

        uint8_t data[] = "hi";
        moq_rcbuf_t *pl = NULL;
        moq_rcbuf_create(&alloc, data, 2, &pl);

        moq_pub_object_cfg_t obj; moq_pub_object_cfg_init(&obj);
        obj.group_id = 0; obj.object_id = 0;
        obj.payload = pl; obj.end_of_group = false;
        MOQ_TEST_CHECK(moq_pub_write_object_ex(pub, track, &obj,
            moq_simpair_now_us(sp)) == MOQ_OK);

        drain_all(sp);

        moq_rcbuf_t *pl2 = NULL;
        moq_rcbuf_create(&alloc, data, 2, &pl2);
        moq_pub_object_cfg_init(&obj);
        obj.group_id = 0; obj.object_id = 1;
        obj.payload = pl2; obj.end_of_group = true;
        MOQ_TEST_CHECK(moq_pub_write_object_ex(pub, track, &obj,
            moq_simpair_now_us(sp)) == MOQ_ERR_WRONG_STATE);

        moq_rcbuf_decref(pl); moq_rcbuf_decref(pl2);
        moq_pub_destroy(pub);
        drain_all(sp);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
        MOQ_TEST_PASS("end_of_group mismatch false->true");
    }

    /* -- Retained catalog object answered via spec Joining FETCH -------- *
     * The origin advertises its retained object as the subscription's Largest
     * (so a joining FETCH is valid) and answers the FETCH from the retained
     * catalog cache. The spec mechanism is an explicit Joining FETCH; a plain
     * SUBSCRIBE delivers no retained objects. */
    {
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

        /* Register the retained catalog object at (group 0, object 0). */
        static const uint8_t catalog[] = { 'C','A','T', 0, 1, 2, 3, 4 };
        moq_rcbuf_t *payload = NULL;
        moq_rcbuf_create(&alloc, catalog, sizeof(catalog), &payload);
        moq_pub_retained_object_t ro = { .object_id = 0, .payload = payload };
        moq_pub_retained_group_cfg_t st; moq_pub_retained_group_cfg_init(&st);
        st.group_id = 0; st.objects = &ro; st.object_count = 1;
        MOQ_TEST_CHECK(moq_pub_set_retained_group(pub, track, &st) == MOQ_OK);
        moq_rcbuf_decref(payload);   /* set_retained_group retained its own ref */

        /* Client subscribes (LargestObject): SUBSCRIBE_OK advertises the
         * retained object as Largest, making a joining FETCH valid. */
        moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace.parts = ns_parts; scfg.track_namespace.count = 1;
        scfg.track_name = MOQ_BYTES_LITERAL("catalog");
        scfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        moq_subscription_t sub_h;
        MOQ_TEST_CHECK(moq_session_subscribe(moq_simpair_client(sp), &scfg, now, &sub_h) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev;
          if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
              MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
              moq_pub_event_result_t res;
              moq_pub_handle_event(pub, &ev, now, &res);
              moq_event_cleanup(&ev);
          } }
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        /* Drain SUBSCRIBE_OK (a plain SUBSCRIBE delivers no retained objects). */
        { moq_event_t d[16]; size_t ne;
          while ((ne = moq_session_poll_events(moq_simpair_client(sp), d, 16)) > 0)
              for (size_t i = 0; i < ne; i++) moq_event_cleanup(&d[i]); }

        /* Client issues a Relative Joining FETCH(offset=0) for the catalog. */
        moq_fetch_cfg_t fcfg; moq_fetch_cfg_init(&fcfg);
        fcfg.is_joining = true; fcfg.joining_relative = true;
        fcfg.joining_start = 0; fcfg.joining_sub = sub_h;
        moq_fetch_t fh;
        MOQ_TEST_CHECK(moq_session_fetch(moq_simpair_client(sp), &fcfg, now, &fh) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);

        /* Publisher answers the FETCH from the retained object (tick path). */
        MOQ_TEST_CHECK(moq_pub_tick(pub, now) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);

        bool got_object = false, got_complete = false;
        moq_event_t evts[16]; size_t ne;
        while ((ne = moq_session_poll_events(moq_simpair_client(sp), evts, 16)) > 0) {
            for (size_t i = 0; i < ne; i++) {
                if (evts[i].kind == MOQ_EVENT_FETCH_OBJECT) {
                    got_object = true;
                    MOQ_TEST_CHECK(evts[i].u.fetch_object.group_id == 0);
                    MOQ_TEST_CHECK(evts[i].u.fetch_object.object_id == 0);
                    MOQ_TEST_CHECK(evts[i].u.fetch_object.payload != NULL &&
                        moq_rcbuf_len(evts[i].u.fetch_object.payload) == sizeof(catalog) &&
                        memcmp(moq_rcbuf_data(evts[i].u.fetch_object.payload),
                               catalog, sizeof(catalog)) == 0);
                } else if (evts[i].kind == MOQ_EVENT_FETCH_COMPLETE) {
                    got_complete = true;
                }
                moq_event_cleanup(&evts[i]);
            }
        }
        MOQ_TEST_CHECK(got_object);
        MOQ_TEST_CHECK(got_complete);

        moq_pub_remove_track(pub, track, now);
        moq_pub_destroy(pub);
        drain_all(sp);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
        MOQ_TEST_PASS("retained catalog joining fetch");
    }

    /* -- Retained FETCH rejects: standalone unsupported; no retained object - */
    {
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

        static const uint8_t cat[] = { 'X','Y','Z' };
        moq_rcbuf_t *payload = NULL;
        moq_rcbuf_create(&alloc, cat, sizeof(cat), &payload);
        moq_pub_retained_object_t ro = { .object_id = 0, .payload = payload };
        moq_pub_retained_group_cfg_t st; moq_pub_retained_group_cfg_init(&st);
        st.group_id = 0; st.objects = &ro; st.object_count = 1;
        moq_pub_set_retained_group(pub, track, &st);
        moq_rcbuf_decref(payload);

        /* Subscribe (LargestObject) so a Largest is advertised and a joining
         * FETCH is valid. */
        moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace.parts = ns_parts; scfg.track_namespace.count = 1;
        scfg.track_name = MOQ_BYTES_LITERAL("catalog");
        scfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        moq_subscription_t sub_h;
        moq_session_subscribe(moq_simpair_client(sp), &scfg, now, &sub_h);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev;
          if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
              moq_pub_event_result_t res;
              moq_pub_handle_event(pub, &ev, now, &res);
              moq_event_cleanup(&ev);
          } }
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t d[16]; size_t ne;
          while ((ne = moq_session_poll_events(moq_simpair_client(sp), d, 16)) > 0)
              for (size_t i = 0; i < ne; i++) moq_event_cleanup(&d[i]); }

        /* A standalone FETCH is served from the retained cache, but only when
         * its explicit range covers object 0..last (see the standalone-replay
         * regression below). These negative cases must still be rejected.
         *
         * Phase 1 pumps WHILE the retained group is present (so the range and
         * unknown-track rejections are evaluated against a live cache); phase 2
         * clears it first (the no-retained-group rejections). */

        /* (a) Standalone FETCH whose range OMITS the retained group (object 0)
         * -> NOT_SUPPORTED. [1,0)->[2,0) does not cover the group-0 object. */
        moq_fetch_cfg_t f1; moq_fetch_cfg_init(&f1);
        f1.track_namespace.parts = ns_parts; f1.track_namespace.count = 1;
        f1.track_name = MOQ_BYTES_LITERAL("catalog");
        f1.start_group = 1; f1.start_object = 0; f1.end_group = 2; f1.end_object = 0;
        moq_fetch_t fh1;
        MOQ_TEST_CHECK(moq_session_fetch(moq_simpair_client(sp), &f1, now, &fh1) == MOQ_OK);

        /* (a2) Standalone FETCH for an UNKNOWN track -> DOES_NOT_EXIST. */
        moq_fetch_cfg_t f1b; moq_fetch_cfg_init(&f1b);
        f1b.track_namespace.parts = ns_parts; f1b.track_namespace.count = 1;
        f1b.track_name = MOQ_BYTES_LITERAL("nope");
        f1b.start_group = 0; f1b.start_object = 0; f1b.end_group = 0; f1b.end_object = 1;
        moq_fetch_t fh1b;
        MOQ_TEST_CHECK(moq_session_fetch(moq_simpair_client(sp), &f1b, now, &fh1b) == MOQ_OK);

        moq_simpair_run_until_quiescent(sp, 8, NULL);
        MOQ_TEST_CHECK(moq_pub_tick(pub, now) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);

        bool got_not_supported = false, got_does_not_exist = false, got_object = false;
        { moq_event_t evts[16]; size_t ne;
          while ((ne = moq_session_poll_events(moq_simpair_client(sp), evts, 16)) > 0)
              for (size_t i = 0; i < ne; i++) {
                  if (evts[i].kind == MOQ_EVENT_FETCH_ERROR) {
                      moq_request_error_t ec = evts[i].u.fetch_error.error_code;
                      if (ec == MOQ_REQUEST_ERROR_NOT_SUPPORTED) got_not_supported = true;
                      if (ec == MOQ_REQUEST_ERROR_DOES_NOT_EXIST) got_does_not_exist = true;
                  } else if (evts[i].kind == MOQ_EVENT_FETCH_OBJECT) {
                      got_object = true;
                  }
                  moq_event_cleanup(&evts[i]);
              } }
        MOQ_TEST_CHECK(got_not_supported);   /* (a) out-of-range standalone */
        MOQ_TEST_CHECK(got_does_not_exist);  /* (a2) unknown track */
        MOQ_TEST_CHECK(!got_object);         /* nothing served by the rejects */

        /* Phase 2: clear the retained group. The Largest was advertised before
         * the clear, so a joining FETCH is still issuable; the origin simply has
         * nothing to serve. Both a joining and a (range-covering) standalone
         * FETCH now resolve to DOES_NOT_EXIST. */
        moq_pub_clear_retained_group(pub, track);

        /* (b) Joining FETCH after the clear -> DOES_NOT_EXIST. */
        moq_fetch_cfg_t f2; moq_fetch_cfg_init(&f2);
        f2.is_joining = true; f2.joining_relative = true; f2.joining_start = 0;
        f2.joining_sub = sub_h;
        moq_fetch_t fh2;
        MOQ_TEST_CHECK(moq_session_fetch(moq_simpair_client(sp), &f2, now, &fh2) == MOQ_OK);

        /* (c) Standalone FETCH after the clear (no retained group) ->
         * DOES_NOT_EXIST, even with a range that would otherwise have covered. */
        moq_fetch_cfg_t f3; moq_fetch_cfg_init(&f3);
        f3.track_namespace.parts = ns_parts; f3.track_namespace.count = 1;
        f3.track_name = MOQ_BYTES_LITERAL("catalog");
        f3.start_group = 0; f3.start_object = 0; f3.end_group = 0; f3.end_object = 1;
        moq_fetch_t fh3;
        MOQ_TEST_CHECK(moq_session_fetch(moq_simpair_client(sp), &f3, now, &fh3) == MOQ_OK);

        moq_simpair_run_until_quiescent(sp, 8, NULL);
        MOQ_TEST_CHECK(moq_pub_tick(pub, now) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);

        int cleared_errors = 0; bool only_does_not_exist = true, served_after_clear = false;
        { moq_event_t evts[16]; size_t ne;
          while ((ne = moq_session_poll_events(moq_simpair_client(sp), evts, 16)) > 0)
              for (size_t i = 0; i < ne; i++) {
                  if (evts[i].kind == MOQ_EVENT_FETCH_ERROR) {
                      cleared_errors++;
                      if (evts[i].u.fetch_error.error_code != MOQ_REQUEST_ERROR_DOES_NOT_EXIST)
                          only_does_not_exist = false;
                  } else if (evts[i].kind == MOQ_EVENT_FETCH_OBJECT) {
                      served_after_clear = true;
                  }
                  moq_event_cleanup(&evts[i]);
              } }
        MOQ_TEST_CHECK(cleared_errors == 2);     /* (b) joining + (c) standalone */
        MOQ_TEST_CHECK(only_does_not_exist);
        MOQ_TEST_CHECK(!served_after_clear);

        moq_pub_destroy(pub);
        drain_all(sp);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
        MOQ_TEST_PASS("retained catalog fetch rejects");
    }

    /* -- Standalone FETCH replays the retained catalog --- *
     * Captured from moqx: after SUBSCRIBE_OK advertises Largest=[0,0], the relay
     * pulls the retained catalog with a STANDALONE FETCH (type 1) -- explicit
     * namespace/name + range [0,0)->[0,1) -- NOT a Joining FETCH. The retained
     * cache resolves the track by namespace/name and serves the whole retained
     * group from that range (object 0..last), so a catalog-driven player behind
     * a relay discovers its tracks. The range covers the whole one-object group;
     * a standalone range that omitted object 0 would be rejected (see the
     * "retained catalog fetch rejects" block above). */
    {
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

        static const uint8_t cat[] = { 'X','Y','Z' };
        moq_rcbuf_t *payload = NULL;
        moq_rcbuf_create(&alloc, cat, sizeof(cat), &payload);
        moq_pub_retained_object_t ro = { .object_id = 0, .payload = payload };
        moq_pub_retained_group_cfg_t st; moq_pub_retained_group_cfg_init(&st);
        st.group_id = 0; st.objects = &ro; st.object_count = 1;
        moq_pub_set_retained_group(pub, track, &st);
        moq_rcbuf_decref(payload);

        /* Subscribe (LargestObject) so SUBSCRIBE_OK advertises Largest=[0,0]. */
        moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace.parts = ns_parts; scfg.track_namespace.count = 1;
        scfg.track_name = MOQ_BYTES_LITERAL("catalog");
        scfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        moq_subscription_t sub_h;
        moq_session_subscribe(moq_simpair_client(sp), &scfg, now, &sub_h);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev;
          if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
              moq_pub_event_result_t res;
              moq_pub_handle_event(pub, &ev, now, &res);
              moq_event_cleanup(&ev);
          } }
        moq_simpair_run_until_quiescent(sp, 8, NULL);

        /* SUBSCRIBE_OK advertises Largest = [0,0] (this part already works). */
        bool saw_largest = false;
        { moq_event_t d[16]; size_t ne;
          while ((ne = moq_session_poll_events(moq_simpair_client(sp), d, 16)) > 0)
              for (size_t i = 0; i < ne; i++) {
                  if (d[i].kind == MOQ_EVENT_SUBSCRIBE_OK &&
                      d[i].u.subscribe_ok.has_largest &&
                      d[i].u.subscribe_ok.largest_group == 0 &&
                      d[i].u.subscribe_ok.largest_object == 0)
                      saw_largest = true;
                  moq_event_cleanup(&d[i]);
              } }
        MOQ_TEST_CHECK(saw_largest);   /* Largest=[0,0] advertised */

        /* STANDALONE FETCH for the catalog, range [0,0]->[0,1] (the moqx shape).
         * is_joining stays false; the range covers the whole one-object group. */
        moq_fetch_cfg_t fcfg; moq_fetch_cfg_init(&fcfg);
        fcfg.track_namespace.parts = ns_parts; fcfg.track_namespace.count = 1;
        fcfg.track_name = MOQ_BYTES_LITERAL("catalog");
        fcfg.start_group = 0; fcfg.start_object = 0;
        fcfg.end_group = 0; fcfg.end_object = 1;   /* covers object 0 */
        moq_fetch_t fh;
        MOQ_TEST_CHECK(moq_session_fetch(moq_simpair_client(sp), &fcfg, now, &fh) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        MOQ_TEST_CHECK(moq_pub_tick(pub, now) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);

        /* The retained catalog object 0 is served and the fetch completes, with
         * no error. */
        int objs = 0; bool payload_ok = false, complete = false, errored = false;
        moq_request_error_t err_code = 0;
        { moq_event_t d[16]; size_t ne;
          while ((ne = moq_session_poll_events(moq_simpair_client(sp), d, 16)) > 0)
              for (size_t i = 0; i < ne; i++) {
                  if (d[i].kind == MOQ_EVENT_FETCH_OBJECT) {
                      if (d[i].u.fetch_object.group_id == 0 &&
                          d[i].u.fetch_object.object_id == 0 &&
                          d[i].u.fetch_object.payload &&
                          moq_rcbuf_len(d[i].u.fetch_object.payload) == sizeof(cat) &&
                          memcmp(moq_rcbuf_data(d[i].u.fetch_object.payload),
                                 cat, sizeof(cat)) == 0)
                          payload_ok = true;
                      objs++;
                  } else if (d[i].kind == MOQ_EVENT_FETCH_COMPLETE) {
                      complete = true;
                  } else if (d[i].kind == MOQ_EVENT_FETCH_ERROR) {
                      errored = true; err_code = d[i].u.fetch_error.error_code;
                  }
                  moq_event_cleanup(&d[i]);
              } }
        (void)err_code;
        MOQ_TEST_CHECK(!errored);    /* no FETCH_ERROR */
        MOQ_TEST_CHECK(objs == 1);   /* exactly the one retained object served */
        MOQ_TEST_CHECK(payload_ok);  /* catalog object 0 payload "XYZ" */
        MOQ_TEST_CHECK(complete);    /* fetch completes */

        moq_pub_destroy(pub);
        drain_all(sp);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
        MOQ_TEST_PASS("retained_group_standalone_fetch_replays_catalog");
    }

    /* -- Standalone retained FETCH authorization (REJECT_ALL) -------------- *
     * A REJECT_ALL publisher holds a retained catalog but never accepts a
     * subscription. A standalone FETCH that names the track and whose range
     * covers the whole retained group must NOT serve the retained objects: it
     * is rejected UNAUTHORIZED with zero objects. Without this gate the catalog
     * would leak to anyone who knows the track name. */
    {
        test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
        simpair_setup(&as, &alloc, &sp);
        uint64_t now = moq_simpair_now_us(sp);

        moq_pub_cfg_t cfg; moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
        cfg.accept_mode = MOQ_PUB_REJECT_ALL;
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

        static const uint8_t cat[] = { 'X','Y','Z' };
        moq_rcbuf_t *payload = NULL;
        moq_rcbuf_create(&alloc, cat, sizeof(cat), &payload);
        moq_pub_retained_object_t ro = { .object_id = 0, .payload = payload };
        moq_pub_retained_group_cfg_t st; moq_pub_retained_group_cfg_init(&st);
        st.group_id = 0; st.objects = &ro; st.object_count = 1;
        moq_pub_set_retained_group(pub, track, &st);
        moq_rcbuf_decref(payload);

        /* No SUBSCRIBE. Standalone FETCH for the catalog, range covering the
         * whole retained group [0,0]->[0,1]. */
        moq_fetch_cfg_t fcfg; moq_fetch_cfg_init(&fcfg);
        fcfg.track_namespace.parts = ns_parts; fcfg.track_namespace.count = 1;
        fcfg.track_name = MOQ_BYTES_LITERAL("catalog");
        fcfg.start_group = 0; fcfg.start_object = 0;
        fcfg.end_group = 0; fcfg.end_object = 1;
        moq_fetch_t fh;
        MOQ_TEST_CHECK(moq_session_fetch(moq_simpair_client(sp), &fcfg, now, &fh) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        MOQ_TEST_CHECK(moq_pub_tick(pub, now) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);

        int objs = 0; bool unauthorized = false; bool other_error = false;
        { moq_event_t d[16]; size_t ne;
          while ((ne = moq_session_poll_events(moq_simpair_client(sp), d, 16)) > 0)
              for (size_t i = 0; i < ne; i++) {
                  if (d[i].kind == MOQ_EVENT_FETCH_OBJECT) {
                      objs++;
                  } else if (d[i].kind == MOQ_EVENT_FETCH_ERROR) {
                      if (d[i].u.fetch_error.error_code == MOQ_REQUEST_ERROR_UNAUTHORIZED)
                          unauthorized = true;
                      else
                          other_error = true;
                  }
                  moq_event_cleanup(&d[i]);
              } }
        MOQ_TEST_CHECK(unauthorized);   /* rejected UNAUTHORIZED */
        MOQ_TEST_CHECK(!other_error);   /* not DOES_NOT_EXIST etc. */
        MOQ_TEST_CHECK(objs == 0);      /* no retained object leaked */

        moq_pub_destroy(pub);
        drain_all(sp);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
        MOQ_TEST_PASS("retained_standalone_fetch_unauthorized_reject_all");
    }

    /* -- Standalone retained FETCH authorization (CALLBACK, no accept) ----- *
     * A CALLBACK publisher holds a retained catalog. A standalone FETCH arrives
     * with NO prior accepted subscription. It must be rejected UNAUTHORIZED
     * (zero objects), and the FETCH path must NOT invoke the subscribe callback
     * (authorization is a pure state check). */
    {
        test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
        simpair_setup(&as, &alloc, &sp);
        uint64_t now = moq_simpair_now_us(sp);

        cb_capture_t cap; memset(&cap, 0, sizeof(cap));
        moq_pub_cfg_t cfg; moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
        cfg.accept_mode = MOQ_PUB_CALLBACK;
        cfg.on_subscribe = cb_capture;
        cfg.on_subscribe_ctx = &cap;
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

        static const uint8_t cat[] = { 'X','Y','Z' };
        moq_rcbuf_t *payload = NULL;
        moq_rcbuf_create(&alloc, cat, sizeof(cat), &payload);
        moq_pub_retained_object_t ro = { .object_id = 0, .payload = payload };
        moq_pub_retained_group_cfg_t st; moq_pub_retained_group_cfg_init(&st);
        st.group_id = 0; st.objects = &ro; st.object_count = 1;
        moq_pub_set_retained_group(pub, track, &st);
        moq_rcbuf_decref(payload);

        /* No SUBSCRIBE (so the callback never had a chance to accept). */
        moq_fetch_cfg_t fcfg; moq_fetch_cfg_init(&fcfg);
        fcfg.track_namespace.parts = ns_parts; fcfg.track_namespace.count = 1;
        fcfg.track_name = MOQ_BYTES_LITERAL("catalog");
        fcfg.start_group = 0; fcfg.start_object = 0;
        fcfg.end_group = 0; fcfg.end_object = 1;
        moq_fetch_t fh;
        MOQ_TEST_CHECK(moq_session_fetch(moq_simpair_client(sp), &fcfg, now, &fh) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        MOQ_TEST_CHECK(moq_pub_tick(pub, now) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);

        int objs = 0; bool unauthorized = false; bool other_error = false;
        { moq_event_t d[16]; size_t ne;
          while ((ne = moq_session_poll_events(moq_simpair_client(sp), d, 16)) > 0)
              for (size_t i = 0; i < ne; i++) {
                  if (d[i].kind == MOQ_EVENT_FETCH_OBJECT) {
                      objs++;
                  } else if (d[i].kind == MOQ_EVENT_FETCH_ERROR) {
                      if (d[i].u.fetch_error.error_code == MOQ_REQUEST_ERROR_UNAUTHORIZED)
                          unauthorized = true;
                      else
                          other_error = true;
                  }
                  moq_event_cleanup(&d[i]);
              } }
        MOQ_TEST_CHECK(unauthorized);   /* rejected UNAUTHORIZED */
        MOQ_TEST_CHECK(!other_error);
        MOQ_TEST_CHECK(objs == 0);      /* no retained object leaked */
        MOQ_TEST_CHECK(!cap.called);    /* FETCH path did not invoke on_subscribe */

        moq_pub_destroy(pub);
        drain_all(sp);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
        MOQ_TEST_PASS("retained_standalone_fetch_unauthorized_callback");
    }

    /* -- Standalone retained FETCH authorization (CALLBACK accept) --------- *
     * GREEN compatibility for the relay flow: a CALLBACK publisher accepts a
     * SUBSCRIBE via callback, then a moqx-shaped standalone FETCH pulls the
     * retained catalog. The accepted subscription authorizes the standalone
     * FETCH, so the retained object IS served. */
    {
        test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
        simpair_setup(&as, &alloc, &sp);
        uint64_t now = moq_simpair_now_us(sp);

        moq_pub_cfg_t cfg; moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
        cfg.accept_mode = MOQ_PUB_CALLBACK;
        cfg.on_subscribe = cb_accept_all;
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

        static const uint8_t cat[] = { 'X','Y','Z' };
        moq_rcbuf_t *payload = NULL;
        moq_rcbuf_create(&alloc, cat, sizeof(cat), &payload);
        moq_pub_retained_object_t ro = { .object_id = 0, .payload = payload };
        moq_pub_retained_group_cfg_t st; moq_pub_retained_group_cfg_init(&st);
        st.group_id = 0; st.objects = &ro; st.object_count = 1;
        moq_pub_set_retained_group(pub, track, &st);
        moq_rcbuf_decref(payload);

        /* SUBSCRIBE accepted via callback -> track gains an accepted sub. */
        moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace.parts = ns_parts; scfg.track_namespace.count = 1;
        scfg.track_name = MOQ_BYTES_LITERAL("catalog");
        scfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        moq_subscription_t sub_h;
        moq_session_subscribe(moq_simpair_client(sp), &scfg, now, &sub_h);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev;
          if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
              moq_pub_event_result_t res;
              moq_pub_handle_event(pub, &ev, now, &res);
              moq_event_cleanup(&ev);
          } }
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        MOQ_TEST_CHECK(moq_pub_has_subscriber(pub, track));   /* accepted */
        { moq_event_t d[16]; size_t ne;
          while ((ne = moq_session_poll_events(moq_simpair_client(sp), d, 16)) > 0)
              for (size_t i = 0; i < ne; i++) moq_event_cleanup(&d[i]); }

        /* moqx-shaped standalone FETCH for the catalog (is_joining stays false). */
        moq_fetch_cfg_t fcfg; moq_fetch_cfg_init(&fcfg);
        fcfg.track_namespace.parts = ns_parts; fcfg.track_namespace.count = 1;
        fcfg.track_name = MOQ_BYTES_LITERAL("catalog");
        fcfg.start_group = 0; fcfg.start_object = 0;
        fcfg.end_group = 0; fcfg.end_object = 1;
        moq_fetch_t fh;
        MOQ_TEST_CHECK(moq_session_fetch(moq_simpair_client(sp), &fcfg, now, &fh) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        MOQ_TEST_CHECK(moq_pub_tick(pub, now) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);

        int objs = 0; bool payload_ok = false, complete = false, errored = false;
        { moq_event_t d[16]; size_t ne;
          while ((ne = moq_session_poll_events(moq_simpair_client(sp), d, 16)) > 0)
              for (size_t i = 0; i < ne; i++) {
                  if (d[i].kind == MOQ_EVENT_FETCH_OBJECT) {
                      if (d[i].u.fetch_object.group_id == 0 &&
                          d[i].u.fetch_object.object_id == 0 &&
                          d[i].u.fetch_object.payload &&
                          moq_rcbuf_len(d[i].u.fetch_object.payload) == sizeof(cat) &&
                          memcmp(moq_rcbuf_data(d[i].u.fetch_object.payload),
                                 cat, sizeof(cat)) == 0)
                          payload_ok = true;
                      objs++;
                  } else if (d[i].kind == MOQ_EVENT_FETCH_COMPLETE) {
                      complete = true;
                  } else if (d[i].kind == MOQ_EVENT_FETCH_ERROR) {
                      errored = true;
                  }
                  moq_event_cleanup(&d[i]);
              } }
        MOQ_TEST_CHECK(!errored);    /* authorized: no FETCH_ERROR */
        MOQ_TEST_CHECK(objs == 1);   /* retained object served */
        MOQ_TEST_CHECK(payload_ok);  /* catalog object 0 payload "XYZ" */
        MOQ_TEST_CHECK(complete);

        moq_pub_destroy(pub);
        drain_all(sp);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
        MOQ_TEST_PASS("retained_standalone_fetch_authorized_callback_accept");
    }

    /* -- Retained FETCH survives a clear between accept and write ---------- *
     * Constrain action capacity (max_actions=3) so the serve accepts the FETCH
     * then defers the object write. Clear the retained object before the retry;
     * the response must still complete from the snapshot taken at stage time. */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_simpair_cfg_t spcfg = MOQ_SIMPAIR_CFG_INIT;
        spcfg.alloc = &alloc; spcfg.seed = 7; spcfg.initial_now_us = 1000;
        spcfg.client_send_request_capacity = true;
        spcfg.client_initial_request_capacity = 16;
        spcfg.server_send_request_capacity = true;
        spcfg.server_initial_request_capacity = 16;
        spcfg.max_actions = 3;   /* accept (2 actions) fits; write+FIN must defer */
        moq_simpair_t *sp = NULL;
        moq_simpair_create(&spcfg, &sp);
        moq_simpair_start(sp);
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        { moq_event_t ev;
          if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1) moq_event_cleanup(&ev);
          if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) moq_event_cleanup(&ev); }
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
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        { moq_event_t ev; if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1) moq_event_cleanup(&ev); }

        static const uint8_t cat[] = { 'S','N','A','P' };
        moq_rcbuf_t *payload = NULL;
        moq_rcbuf_create(&alloc, cat, sizeof(cat), &payload);
        moq_pub_retained_object_t ro = { .object_id = 0, .payload = payload };
        moq_pub_retained_group_cfg_t st; moq_pub_retained_group_cfg_init(&st);
        st.group_id = 0; st.objects = &ro; st.object_count = 1;
        moq_pub_set_retained_group(pub, track, &st);
        moq_rcbuf_decref(payload);

        moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace.parts = ns_parts; scfg.track_namespace.count = 1;
        scfg.track_name = MOQ_BYTES_LITERAL("catalog");
        scfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        moq_subscription_t sub_h;
        moq_session_subscribe(moq_simpair_client(sp), &scfg, now, &sub_h);
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        /* Drive the publisher (subscribe accept). */
        for (int i = 0; i < 8; i++) {
            moq_pub_tick(pub, now);
            moq_simpair_run_until_quiescent(sp, 16, NULL);
        }
        { moq_event_t d[16]; size_t ne;
          while ((ne = moq_session_poll_events(moq_simpair_client(sp), d, 16)) > 0)
              for (size_t i = 0; i < ne; i++) moq_event_cleanup(&d[i]); }

        /* Joining FETCH; first serve accepts then defers the write. */
        moq_fetch_cfg_t fcfg; moq_fetch_cfg_init(&fcfg);
        fcfg.is_joining = true; fcfg.joining_relative = true;
        fcfg.joining_start = 0; fcfg.joining_sub = sub_h;
        moq_fetch_t fh;
        MOQ_TEST_CHECK(moq_session_fetch(moq_simpair_client(sp), &fcfg, now, &fh) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        MOQ_TEST_CHECK(moq_pub_tick(pub, now) == MOQ_ERR_WOULD_BLOCK); /* accepted, write deferred */
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        /* Clear the retained object while the fetch is accepted-but-unwritten. */
        moq_pub_clear_retained_group(pub, track);

        /* Subsequent ticks complete the write+FIN from the snapshot. */
        for (int i = 0; i < 8; i++) {
            moq_pub_tick(pub, now);
            moq_simpair_run_until_quiescent(sp, 16, NULL);
        }

        bool got_object = false, got_complete = false;
        moq_event_t evts[16]; size_t ne;
        while ((ne = moq_session_poll_events(moq_simpair_client(sp), evts, 16)) > 0) {
            for (size_t i = 0; i < ne; i++) {
                if (evts[i].kind == MOQ_EVENT_FETCH_OBJECT) {
                    got_object = true;
                    MOQ_TEST_CHECK(evts[i].u.fetch_object.payload != NULL &&
                        moq_rcbuf_len(evts[i].u.fetch_object.payload) == sizeof(cat) &&
                        memcmp(moq_rcbuf_data(evts[i].u.fetch_object.payload),
                               cat, sizeof(cat)) == 0);
                } else if (evts[i].kind == MOQ_EVENT_FETCH_COMPLETE) {
                    got_complete = true;
                }
                moq_event_cleanup(&evts[i]);
            }
        }
        MOQ_TEST_CHECK(got_object);    /* snapshot delivered despite the clear */
        MOQ_TEST_CHECK(got_complete);

        moq_pub_destroy(pub);
        drain_all(sp);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
        MOQ_TEST_PASS("retained catalog fetch survives clear");
    }

    /* -- Largest gating: a track that wrote a live object must NOT advertise
     * its retained object as Largest (no stale-largest reintroduced) ------- */
    {
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

        /* A retained object is set... */
        static const uint8_t cat[] = { 'C','A','T' };
        moq_rcbuf_t *cp = NULL; moq_rcbuf_create(&alloc, cat, sizeof(cat), &cp);
        moq_pub_retained_object_t ro = { .object_id = 0, .payload = cp };
        moq_pub_retained_group_cfg_t st; moq_pub_retained_group_cfg_init(&st);
        st.group_id = 0; st.objects = &ro; st.object_count = 1;
        moq_pub_set_retained_group(pub, track, &st);
        moq_rcbuf_decref(cp);

        /* ...but then a live object is written successfully (no subscriber yet:
         * the no-slot OK path marks the track live). The caller retains the
         * payload ref on the no-sub path. */
        static const uint8_t live[] = { 'L','I','V','E' };
        moq_rcbuf_t *lp = NULL; moq_rcbuf_create(&alloc, live, sizeof(live), &lp);
        MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 1, 0, lp, now) == MOQ_OK);
        moq_rcbuf_decref(lp);

        /* Subscribe (LargestObject): since a live object was written, the
         * retained object must NOT be advertised as Largest. */
        moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace.parts = ns_parts; scfg.track_namespace.count = 1;
        scfg.track_name = MOQ_BYTES_LITERAL("catalog");
        scfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        moq_subscription_t sub_h;
        moq_session_subscribe(moq_simpair_client(sp), &scfg, now, &sub_h);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev;
          if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
              moq_pub_event_result_t res;
              moq_pub_handle_event(pub, &ev, now, &res);
              moq_event_cleanup(&ev);
          } }
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t d[16]; size_t ne;
          while ((ne = moq_session_poll_events(moq_simpair_client(sp), d, 16)) > 0)
              for (size_t i = 0; i < ne; i++) moq_event_cleanup(&d[i]); }

        /* No Largest advertised -> a joining FETCH is invalid client-side
         * (the core requires a known Largest, §9.16.2). This is the proof that
         * a stale retained Largest is not reintroduced. */
        moq_fetch_cfg_t fcfg; moq_fetch_cfg_init(&fcfg);
        fcfg.is_joining = true; fcfg.joining_relative = true;
        fcfg.joining_start = 0; fcfg.joining_sub = sub_h;
        moq_fetch_t fh;
        MOQ_TEST_CHECK(moq_session_fetch(moq_simpair_client(sp), &fcfg, now, &fh)
                       == MOQ_ERR_INVAL);

        moq_pub_destroy(pub);
        drain_all(sp);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
        MOQ_TEST_PASS("retained largest gated by live write");
    }

    /* -- Unencodable retained Location is not advertised as Largest -------- *
     * A retained group whose group_id exceeds the QUIC varint max cannot carry
     * an encodable {group, object + 1} End Location, so it must not be advertised
     * as Largest -- and a joining FETCH against it is then invalid client-side. */
    {
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

        static const uint8_t cat[] = { 'C','A','T' };
        moq_rcbuf_t *cp = NULL; moq_rcbuf_create(&alloc, cat, sizeof(cat), &cp);
        moq_pub_retained_object_t ro = { .object_id = 0, .payload = cp };
        moq_pub_retained_group_cfg_t st; moq_pub_retained_group_cfg_init(&st);
        st.group_id = MOQ_QUIC_VARINT_MAX + 1ull;   /* group unencodable */
        st.objects = &ro; st.object_count = 1;
        moq_pub_set_retained_group(pub, track, &st);
        moq_rcbuf_decref(cp);

        moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace.parts = ns_parts; scfg.track_namespace.count = 1;
        scfg.track_name = MOQ_BYTES_LITERAL("catalog");
        scfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        moq_subscription_t sub_h;
        moq_session_subscribe(moq_simpair_client(sp), &scfg, now, &sub_h);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev;
          if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
              moq_pub_event_result_t res;
              moq_pub_handle_event(pub, &ev, now, &res);
              moq_event_cleanup(&ev);
          } }
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t d[16]; size_t ne;
          while ((ne = moq_session_poll_events(moq_simpair_client(sp), d, 16)) > 0)
              for (size_t i = 0; i < ne; i++) moq_event_cleanup(&d[i]); }

        /* No Largest advertised -> joining FETCH invalid client-side. */
        moq_fetch_cfg_t fcfg; moq_fetch_cfg_init(&fcfg);
        fcfg.is_joining = true; fcfg.joining_relative = true;
        fcfg.joining_start = 0; fcfg.joining_sub = sub_h;
        moq_fetch_t fh;
        MOQ_TEST_CHECK(moq_session_fetch(moq_simpair_client(sp), &fcfg, now, &fh)
                       == MOQ_ERR_INVAL);

        moq_pub_destroy(pub);
        drain_all(sp);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
        MOQ_TEST_PASS("retained unencodable largest not advertised");
    }

    /* -- Retained GROUP (objects 0..N): a Joining FETCH replays the whole group
     *    in order; a plain SUBSCRIBE delivers no retained objects. --------- */
    {
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

        /* Retain a group of 3 objects in group 7: independent [0] + deltas [1],[2]. */
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

        moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace.parts = ns_parts; scfg.track_namespace.count = 1;
        scfg.track_name = MOQ_BYTES_LITERAL("catalog");
        scfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        moq_subscription_t sub_h;
        MOQ_TEST_CHECK(moq_session_subscribe(moq_simpair_client(sp), &scfg, now, &sub_h) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev;
          if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
              MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
              moq_pub_event_result_t res; moq_pub_handle_event(pub, &ev, now, &res);
              moq_event_cleanup(&ev);
          } }
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        /* (a) A plain SUBSCRIBE delivers NO retained objects (no re-push). */
        { int objs_n = 0; moq_event_t d[16]; size_t ne;
          while ((ne = moq_session_poll_events(moq_simpair_client(sp), d, 16)) > 0)
              for (size_t i = 0; i < ne; i++) {
                  if (d[i].kind == MOQ_EVENT_OBJECT_RECEIVED) objs_n++;
                  moq_event_cleanup(&d[i]);
              }
          MOQ_TEST_CHECK(objs_n == 0); }

        /* (b) Joining FETCH(offset 0) replays objects 0,1,2 in order + complete. */
        moq_fetch_cfg_t fcfg; moq_fetch_cfg_init(&fcfg);
        fcfg.is_joining = true; fcfg.joining_relative = true;
        fcfg.joining_start = 0; fcfg.joining_sub = sub_h;
        moq_fetch_t fh;
        MOQ_TEST_CHECK(moq_session_fetch(moq_simpair_client(sp), &fcfg, now, &fh) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        MOQ_TEST_CHECK(moq_pub_tick(pub, now) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        int n_obj = 0; bool ord_ok = true, complete = false;
        moq_event_t evts[16]; size_t ne;
        while ((ne = moq_session_poll_events(moq_simpair_client(sp), evts, 16)) > 0)
            for (size_t i = 0; i < ne; i++) {
                if (evts[i].kind == MOQ_EVENT_FETCH_OBJECT) {
                    if (evts[i].u.fetch_object.group_id != 7 ||
                        evts[i].u.fetch_object.object_id != (uint64_t)n_obj) ord_ok = false;
                    n_obj++;
                } else if (evts[i].kind == MOQ_EVENT_FETCH_COMPLETE) complete = true;
                moq_event_cleanup(&evts[i]);
            }
        MOQ_TEST_CHECK(n_obj == 3);   /* object 0 + deltas 1,2 in order */
        MOQ_TEST_CHECK(ord_ok);
        MOQ_TEST_CHECK(complete);

        moq_pub_remove_track(pub, track, now);
        moq_pub_destroy(pub);
        drain_all(sp);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
        MOQ_TEST_PASS("retained group joining fetch 0..N");
    }

    /* -- set_retained_group replaces the prior group: FETCH serves the latest
     *    (only-latest-group retention). -------------------------------- */
    {
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

        /* Gen A: 1 object in group 1. */
        moq_rcbuf_t *a0=NULL; moq_rcbuf_create(&alloc,(const uint8_t*)"A",1,&a0);
        moq_pub_retained_object_t oa[1] = { { .object_id=0, .payload=a0, .end_of_group=true } };
        moq_pub_retained_group_cfg_t ga; moq_pub_retained_group_cfg_init(&ga);
        ga.group_id = 1; ga.objects = oa; ga.object_count = 1;
        MOQ_TEST_CHECK(moq_pub_set_retained_group(pub, track, &ga) == MOQ_OK);
        moq_rcbuf_decref(a0);
        /* Gen B: 2 objects in group 2 -> replaces gen A. */
        moq_rcbuf_t *b0=NULL,*b1=NULL;
        moq_rcbuf_create(&alloc,(const uint8_t*)"B0",2,&b0);
        moq_rcbuf_create(&alloc,(const uint8_t*)"B1",2,&b1);
        moq_pub_retained_object_t ob[2] = {
            { .object_id=0, .payload=b0 },
            { .object_id=1, .payload=b1, .end_of_group=true } };
        moq_pub_retained_group_cfg_t gb; moq_pub_retained_group_cfg_init(&gb);
        gb.group_id = 2; gb.objects = ob; gb.object_count = 2;
        MOQ_TEST_CHECK(moq_pub_set_retained_group(pub, track, &gb) == MOQ_OK);
        moq_rcbuf_decref(b0); moq_rcbuf_decref(b1);

        moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace.parts = ns_parts; scfg.track_namespace.count = 1;
        scfg.track_name = MOQ_BYTES_LITERAL("catalog");
        scfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        moq_subscription_t sub_h;
        MOQ_TEST_CHECK(moq_session_subscribe(moq_simpair_client(sp), &scfg, now, &sub_h) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev;
          if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
              moq_pub_event_result_t res; moq_pub_handle_event(pub, &ev, now, &res);
              moq_event_cleanup(&ev);
          } }
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t d[16]; size_t ne;
          while ((ne = moq_session_poll_events(moq_simpair_client(sp), d, 16)) > 0)
              for (size_t i = 0; i < ne; i++) moq_event_cleanup(&d[i]); }

        moq_fetch_cfg_t fcfg; moq_fetch_cfg_init(&fcfg);
        fcfg.is_joining = true; fcfg.joining_relative = true;
        fcfg.joining_start = 0; fcfg.joining_sub = sub_h;
        moq_fetch_t fh;
        MOQ_TEST_CHECK(moq_session_fetch(moq_simpair_client(sp), &fcfg, now, &fh) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        MOQ_TEST_CHECK(moq_pub_tick(pub, now) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        int n_obj = 0; bool grp_ok = true;
        moq_event_t evts[16]; size_t ne;
        while ((ne = moq_session_poll_events(moq_simpair_client(sp), evts, 16)) > 0)
            for (size_t i = 0; i < ne; i++) {
                if (evts[i].kind == MOQ_EVENT_FETCH_OBJECT) {
                    if (evts[i].u.fetch_object.group_id != 2) grp_ok = false;  /* latest only */
                    n_obj++;
                }
                moq_event_cleanup(&evts[i]);
            }
        MOQ_TEST_CHECK(n_obj == 2);   /* gen B, not gen A */
        MOQ_TEST_CHECK(grp_ok);

        moq_pub_remove_track(pub, track, now);
        moq_pub_destroy(pub);
        drain_all(sp);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
        MOQ_TEST_PASS("retained group replaced by latest");
    }

    /* -- set_retained_group bounds: count cap and byte budget rejected --- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t scfg = MOQ_SESSION_CFG_INIT;
        scfg.alloc = &alloc;
        scfg.perspective = MOQ_PERSPECTIVE_SERVER;
        moq_session_t *server = NULL;
        moq_session_create(&scfg, 0, &server);
        moq_pub_cfg_t cfg; moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
        cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
        moq_publisher_t *pub = NULL;
        moq_pub_create(server, &alloc, &cfg, &pub);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("svc") };
        moq_pub_track_cfg_t tcfg;
        moq_pub_track_cfg_init_sized(&tcfg, sizeof(tcfg));
        tcfg.track_namespace.parts = ns_parts; tcfg.track_namespace.count = 1;
        tcfg.track_name = MOQ_BYTES_LITERAL("catalog");
        tcfg.max_retained_bytes = 8;            /* tiny byte budget */
        moq_pub_track_t *track = NULL;
        moq_pub_add_track(pub, &tcfg, 0, &track);

        /* object_count over the cap -> INVAL. */
        static moq_pub_retained_object_t big[MOQ_PUB_RETAINED_MAX_OBJECTS + 1];
        moq_rcbuf_t *one = NULL; moq_rcbuf_create(&alloc, (const uint8_t*)"x", 1, &one);
        for (size_t i = 0; i < MOQ_PUB_RETAINED_MAX_OBJECTS + 1; i++) {
            big[i].object_id = i; big[i].payload = one;
            big[i].properties = NULL; big[i].end_of_group = false;
        }
        moq_pub_retained_group_cfg_t gc; moq_pub_retained_group_cfg_init(&gc);
        gc.group_id = 0; gc.objects = big;
        gc.object_count = MOQ_PUB_RETAINED_MAX_OBJECTS + 1;
        MOQ_TEST_CHECK(moq_pub_set_retained_group(pub, track, &gc) == MOQ_ERR_INVAL);

        /* total payload bytes over the budget -> INVAL. */
        moq_rcbuf_t *big_pl = NULL; moq_rcbuf_create(&alloc, (const uint8_t*)"123456789", 9, &big_pl);
        moq_pub_retained_object_t over[1] = { { .object_id=0, .payload=big_pl } };
        gc.objects = over; gc.object_count = 1;
        MOQ_TEST_CHECK(moq_pub_set_retained_group(pub, track, &gc) == MOQ_ERR_INVAL);

        /* out-of-order object_ids -> INVAL. */
        moq_pub_retained_object_t ooo[2] = {
            { .object_id=2, .payload=one }, { .object_id=1, .payload=one } };
        gc.objects = ooo; gc.object_count = 2;
        MOQ_TEST_CHECK(moq_pub_set_retained_group(pub, track, &gc) == MOQ_ERR_INVAL);

        /* non-dense: a gap (0,2) -> INVAL (catalog replay needs 0..N). */
        moq_pub_retained_object_t gap[2] = {
            { .object_id=0, .payload=one }, { .object_id=2, .payload=one } };
        gc.objects = gap; gc.object_count = 2;
        MOQ_TEST_CHECK(moq_pub_set_retained_group(pub, track, &gc) == MOQ_ERR_INVAL);

        /* non-dense: missing object 0 (1,2) -> INVAL. */
        moq_pub_retained_object_t no0[2] = {
            { .object_id=1, .payload=one }, { .object_id=2, .payload=one } };
        gc.objects = no0; gc.object_count = 2;
        MOQ_TEST_CHECK(moq_pub_set_retained_group(pub, track, &gc) == MOQ_ERR_INVAL);

        moq_rcbuf_decref(one); moq_rcbuf_decref(big_pl);
        moq_pub_destroy(pub);
        moq_session_destroy(server);
        MOQ_TEST_CHECK(as.balance == 0);
        MOQ_TEST_PASS("retained group bounds rejected");
    }

    /* -- Largest re-advertised after a live write + set_retained_group: the
     *    retained group's last object is the latest published location, so a
     *    joining FETCH is valid (correction: wrote_object must not suppress
     *    Largest for a retained group). --------------------------------- */
    {
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

        /* A live write sets wrote_object (no subscriber yet -> retained, dropped). */
        moq_rcbuf_t *lv = NULL; moq_rcbuf_create(&alloc,(const uint8_t*)"live",4,&lv);
        moq_pub_object_cfg_t ocfg; moq_pub_object_cfg_init(&ocfg);
        ocfg.group_id = 3; ocfg.object_id = 0; ocfg.payload = lv; ocfg.end_of_group = true;
        moq_pub_write_object_ex(pub, track, &ocfg, now);
        moq_rcbuf_decref(lv);

        /* set_retained_group then resets wrote_object: Largest = (9,1). */
        moq_rcbuf_t *c0=NULL,*c1=NULL;
        moq_rcbuf_create(&alloc,(const uint8_t*)"c0",2,&c0);
        moq_rcbuf_create(&alloc,(const uint8_t*)"c1",2,&c1);
        moq_pub_retained_object_t objs[2] = {
            { .object_id=0, .payload=c0 },
            { .object_id=1, .payload=c1, .end_of_group=true } };
        moq_pub_retained_group_cfg_t gc; moq_pub_retained_group_cfg_init(&gc);
        gc.group_id = 9; gc.objects = objs; gc.object_count = 2;
        MOQ_TEST_CHECK(moq_pub_set_retained_group(pub, track, &gc) == MOQ_OK);
        moq_rcbuf_decref(c0); moq_rcbuf_decref(c1);

        moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace.parts = ns_parts; scfg.track_namespace.count = 1;
        scfg.track_name = MOQ_BYTES_LITERAL("catalog");
        scfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        moq_subscription_t sub_h;
        MOQ_TEST_CHECK(moq_session_subscribe(moq_simpair_client(sp), &scfg, now, &sub_h) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev;
          if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
              moq_pub_event_result_t res; moq_pub_handle_event(pub, &ev, now, &res);
              moq_event_cleanup(&ev);
          } }
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t d[16]; size_t ne;
          while ((ne = moq_session_poll_events(moq_simpair_client(sp), d, 16)) > 0)
              for (size_t i = 0; i < ne; i++) moq_event_cleanup(&d[i]); }

        /* A joining FETCH is VALID only if Largest was advertised -> it serves
         * the retained group (group 9). */
        moq_fetch_cfg_t fcfg; moq_fetch_cfg_init(&fcfg);
        fcfg.is_joining = true; fcfg.joining_relative = true;
        fcfg.joining_start = 0; fcfg.joining_sub = sub_h;
        moq_fetch_t fh;
        MOQ_TEST_CHECK(moq_session_fetch(moq_simpair_client(sp), &fcfg, now, &fh) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        MOQ_TEST_CHECK(moq_pub_tick(pub, now) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        int n_obj = 0; bool grp_ok = true, complete = false;
        moq_event_t evts[16]; size_t ne;
        while ((ne = moq_session_poll_events(moq_simpair_client(sp), evts, 16)) > 0)
            for (size_t i = 0; i < ne; i++) {
                if (evts[i].kind == MOQ_EVENT_FETCH_OBJECT) {
                    if (evts[i].u.fetch_object.group_id != 9) grp_ok = false;
                    n_obj++;
                } else if (evts[i].kind == MOQ_EVENT_FETCH_COMPLETE) complete = true;
                moq_event_cleanup(&evts[i]);
            }
        MOQ_TEST_CHECK(n_obj == 2);   /* Largest advertised -> fetch served */
        MOQ_TEST_CHECK(grp_ok);
        MOQ_TEST_CHECK(complete);

        moq_pub_remove_track(pub, track, now);
        moq_pub_destroy(pub);
        drain_all(sp);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
        MOQ_TEST_PASS("retained group largest after live write");
    }

    /* -- Joining FETCH range that does not cover the whole retained group is
     *    rejected (object 0 omitted, or later deltas omitted). The range is
     *    injected by mutating the real FETCH_REQUEST event before handling it,
     *    since a normal offset-0 joining FETCH always covers the group. ---- */
    {
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

        moq_rcbuf_t *p0=NULL,*p1=NULL,*p2=NULL;
        moq_rcbuf_create(&alloc,(const uint8_t*)"o0",2,&p0);
        moq_rcbuf_create(&alloc,(const uint8_t*)"o1",2,&p1);
        moq_rcbuf_create(&alloc,(const uint8_t*)"o2",2,&p2);
        moq_pub_retained_object_t objs[3] = {
            { .object_id=0, .payload=p0 }, { .object_id=1, .payload=p1 },
            { .object_id=2, .payload=p2, .end_of_group=true } };
        moq_pub_retained_group_cfg_t gc; moq_pub_retained_group_cfg_init(&gc);
        gc.group_id = 7; gc.objects = objs; gc.object_count = 3;
        MOQ_TEST_CHECK(moq_pub_set_retained_group(pub, track, &gc) == MOQ_OK);
        moq_rcbuf_decref(p0); moq_rcbuf_decref(p1); moq_rcbuf_decref(p2);

        moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace.parts = ns_parts; scfg.track_namespace.count = 1;
        scfg.track_name = MOQ_BYTES_LITERAL("catalog");
        scfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        moq_subscription_t sub_h;
        MOQ_TEST_CHECK(moq_session_subscribe(moq_simpair_client(sp), &scfg, now, &sub_h) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev;
          if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
              moq_pub_event_result_t res; moq_pub_handle_event(pub, &ev, now, &res);
              moq_event_cleanup(&ev);
          } }
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t d[16]; size_t ne;
          while ((ne = moq_session_poll_events(moq_simpair_client(sp), d, 16)) > 0)
              for (size_t i = 0; i < ne; i++) moq_event_cleanup(&d[i]); }

        /* Two malformed ranges, each injected into the real FETCH_REQUEST. */
        struct { uint64_t so, eg, eo; const char *what; } cases[] = {
            { 1, 7, 0, "omit object 0" },   /* start at object 1 -> 0 not covered */
            { 0, 7, 2, "omit later delta" },/* end at object 2 -> object 2 not covered */
        };
        for (size_t c = 0; c < 2; c++) {
            moq_fetch_cfg_t fcfg; moq_fetch_cfg_init(&fcfg);
            fcfg.is_joining = true; fcfg.joining_relative = true;
            fcfg.joining_start = 0; fcfg.joining_sub = sub_h;
            moq_fetch_t fh;
            MOQ_TEST_CHECK(moq_session_fetch(moq_simpair_client(sp), &fcfg, now, &fh) == MOQ_OK);
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            /* Poll the FETCH_REQUEST, mutate its range to omit part of the group,
             * then hand it to the publisher. */
            bool handled = false;
            moq_event_t ev;
            while (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
                if (ev.kind == MOQ_EVENT_FETCH_REQUEST) {
                    ev.u.fetch_request.start_group = 7;
                    ev.u.fetch_request.start_object = cases[c].so;
                    ev.u.fetch_request.end_group = cases[c].eg;
                    ev.u.fetch_request.end_object = cases[c].eo;
                    moq_pub_event_result_t res;
                    moq_pub_handle_event(pub, &ev, now, &res);
                    handled = true;
                }
                moq_event_cleanup(&ev);
            }
            MOQ_TEST_CHECK(handled);
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            (void)moq_pub_tick(pub, now);
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            bool got_err = false; int objs_seen = 0;
            moq_event_t e2;
            while (moq_session_poll_events(moq_simpair_client(sp), &e2, 1) == 1) {
                if (e2.kind == MOQ_EVENT_FETCH_ERROR &&
                    e2.u.fetch_error.error_code == MOQ_REQUEST_ERROR_NOT_SUPPORTED)
                    got_err = true;
                if (e2.kind == MOQ_EVENT_FETCH_OBJECT) objs_seen++;
                moq_event_cleanup(&e2);
            }
            MOQ_TEST_CHECK(got_err);      /* rejected */
            MOQ_TEST_CHECK(objs_seen == 0);   /* nothing served */
        }

        moq_pub_remove_track(pub, track, now);
        moq_pub_destroy(pub);
        drain_all(sp);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
        MOQ_TEST_PASS("retained group fetch range must cover whole group");
    }

    if (failures == 0) {
        printf("PASS: all publisher tests\n");
        return 0;
    }
    printf("FAIL: %d publisher test failures\n", failures);
    return 1;
}
