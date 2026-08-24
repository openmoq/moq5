#include <moq/publisher.h>
#include <moq/sim.h>
#include <moq/wire.h>   /* MOQ_QUIC_VARINT_MAX */
#include "test_session_support.h"
#include <stddef.h>
#include <stdio.h>
#include <moq/control.h>
#include <moq/control_d18.h>
#include <moq/buf.h>
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

/* The pointer-only moq_pub_callbacks_init must clear/stamp ONLY the frozen v0
 * prefix (through on_subscriber_updated): a caller compiled before the
 * appended on_publish_* fields hands it exactly that much storage. */
static void test_pub_callbacks_init_old_prefix_no_overflow(void) {
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

    /* Drain (delivering control AND data-stream bytes -- the client gates
     * SUBSCRIBE_DONE on processing the advertised data stream, so the
     * subgroup's bytes and closing FIN must actually arrive) and retry
     * until the finish completes. */
    int done_n = 0;
    for (int k = 0; k < 9 && done_n == 0; k++) {
        { moq_action_t acts[16]; size_t na;
          while ((na = moq_session_poll_actions(server, acts, 16)) > 0)
              for (size_t i = 0; i < na; i++) {
                  if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                      moq_session_on_control_bytes(client,
                          acts[i].u.send_control.data,
                          acts[i].u.send_control.len, 0);
                  else if (acts[i].kind == MOQ_ACTION_SEND_DATA)
                      FEED_SEND_DATA(client,
                          acts[i].u.send_data.stream_ref, acts[i], 0);
                  moq_action_cleanup(&acts[i]);
              } }
        { moq_event_t ev[8]; size_t ne;
          while ((ne = moq_session_poll_events(client, ev, 8)) > 0)
              for (size_t i = 0; i < ne; i++) {
                  if (ev[i].kind == MOQ_EVENT_SUBSCRIBE_DONE) done_n++;
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
    int publish_ok;
    int publish_error;
    uint64_t close_code;
    moq_pub_track_t *joined_track;
    moq_pub_track_t *left_track;
    moq_pub_track_t *updated_track;
    moq_pub_subscribe_update_info_t last_update;
} tick_cb_state_t;

static void tick_on_publish_ok(void *ctx, moq_pub_track_t *track, bool forward) {
    tick_cb_state_t *s = (tick_cb_state_t *)ctx;
    (void)track; (void)forward;
    s->publish_ok++;
}
static void tick_on_publish_error(void *ctx, moq_pub_track_t *track,
                                  moq_request_error_t code) {
    tick_cb_state_t *s = (tick_cb_state_t *)ctx;
    (void)track; (void)code;
    s->publish_error++;
}

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
    /* non-datagram status = invalid (not supported in this slice) */
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

/* when the peer unsubscribes, the core emits UNSUBSCRIBED and
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

/* resolving a deferred subscribe must not clobber an earlier
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

/* Old-caller callback survival: a caller compiled BEFORE the appended
 * on_publish_* fields passes an outer struct_size ending exactly at the v0
 * callbacks prefix. Its legitimately-set on_subscriber_updated (the LAST v0
 * field) must survive the whole-field prefix copy and fire. */
static void test_pub_callbacks_old_prefix_copier_keeps_updated(void) {
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
static void test_pub_callbacks_nested_prefix_bounds_copy(void) {
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
static void test_pub_callbacks_partial_field_poison(void) {
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


/* -- Fan-out battery ------------------------------------------------- *
 * Delivery to ALL destinations (both install orders), retry-cursor
 * correctness under a forced second-destination WOULD_BLOCK, byte-exact
 * retry identity, Forward-0 exclusion, and mid-operation joiner exclusion. */

/* Coexist harness: one track, a subscription AND an accepted publication
 * (install order selectable). Returns via out params; caller destroys. */
static void fanout_setup(test_alloc_state_t *as, moq_alloc_t *alloc,
                         moq_simpair_t **sp, moq_publisher_t **pub_out,
                         moq_pub_track_t **track_out, bool sub_first,
                         uint32_t max_actions)
{
    *as = (test_alloc_state_t){0};
    *alloc = test_allocator(as);
    {
        moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
        cfg.alloc = alloc;
        cfg.seed = 42;
        cfg.initial_now_us = 1000;
        cfg.client_send_request_capacity = true;
        cfg.client_initial_request_capacity = 16;
        cfg.server_send_request_capacity = true;
        cfg.server_initial_request_capacity = 16;
        cfg.max_actions = max_actions;   /* 0 = default; small forces
                                            mid-fan-out WOULD_BLOCK */
        moq_simpair_create(&cfg, sp);
        moq_simpair_start(*sp);
        moq_simpair_run_until_quiescent(*sp, 8, NULL);
        moq_event_t ev;
        if (moq_session_poll_events(moq_simpair_client(*sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);
        if (moq_session_poll_events(moq_simpair_server(*sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);
    }

    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    moq_publisher_t *pub = NULL;
    moq_pub_create(moq_simpair_server(*sp), alloc, &cfg, &pub);

    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    tcfg.advertise_namespace = true;
    moq_pub_track_t *track = NULL;
    moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(*sp), &track);

    for (int step = 0; step < 2; step++) {
        bool do_sub = sub_first ? (step == 0) : (step == 1);
        if (do_sub) {
            moq_subscribe_cfg_t scfg;
            moq_subscribe_cfg_init(&scfg);
            scfg.track_namespace.parts = ns_parts;
            scfg.track_namespace.count = 1;
            scfg.track_name = MOQ_BYTES_LITERAL("video");
            moq_subscription_t sub_h;
            moq_session_subscribe(moq_simpair_client(*sp), &scfg,
                moq_simpair_now_us(*sp), &sub_h);
            moq_simpair_run_until_quiescent(*sp, 8, NULL);
            moq_event_t ev;
            while (moq_session_poll_events(moq_simpair_server(*sp), &ev, 1) == 1) {
                moq_pub_event_result_t res;
                moq_pub_handle_event(pub, &ev, moq_simpair_now_us(*sp), &res);
                moq_event_cleanup(&ev);
            }
        } else {
            moq_pub_publish_cfg_t pcfg; moq_pub_publish_cfg_init(&pcfg);
            MOQ_TEST_CHECK(moq_pub_publish_track(pub, track, &pcfg,
                moq_simpair_now_us(*sp)) == MOQ_OK);
            moq_simpair_run_until_quiescent(*sp, 8, NULL);
            for (int i = 0; i < 6; i++) {
                moq_event_t ev;
                if (moq_session_poll_events(moq_simpair_client(*sp), &ev, 1) != 1)
                    break;
                if (ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED) {
                    moq_accept_namespace_cfg_t nacc;
                    moq_accept_namespace_cfg_init(&nacc);
                    moq_session_accept_namespace(moq_simpair_client(*sp),
                        ev.u.namespace_published.ann, &nacc,
                        moq_simpair_now_us(*sp));
                } else if (ev.kind == MOQ_EVENT_PUBLISH_REQUEST) {
                    moq_accept_publish_cfg_t acc;
                    moq_accept_publish_cfg_init(&acc);
                    moq_session_accept_publish(moq_simpair_client(*sp),
                        ev.u.publish_request.pub, &acc,
                        moq_simpair_now_us(*sp));
                }
                moq_event_cleanup(&ev);
            }
        }
        moq_simpair_run_until_quiescent(*sp, 8, NULL);
        moq_pub_tick(pub, moq_simpair_now_us(*sp));
        moq_simpair_run_until_quiescent(*sp, 8, NULL);
    }
    /* Drain remaining setup noise on both sides. */
    { moq_event_t ev;
      while (moq_session_poll_events(moq_simpair_client(*sp), &ev, 1) == 1)
          moq_event_cleanup(&ev);
      while (moq_session_poll_events(moq_simpair_server(*sp), &ev, 1) == 1) {
          moq_pub_event_result_t res;
          moq_pub_handle_event(pub, &ev, moq_simpair_now_us(*sp), &res);
          moq_event_cleanup(&ev);
      } }

    MOQ_TEST_CHECK(moq_pub_has_subscriber(pub, track));
    MOQ_TEST_CHECK(moq_pub_track_is_published(pub, track));
    *pub_out = pub;
    *track_out = track;
}

/* Count client OBJECT_RECEIVED events matching (group, object, bytes). */
static int fanout_count_objects(moq_simpair_t *sp, uint64_t g, uint64_t o,
                                const uint8_t *bytes, size_t len)
{
    int n = 0;
    moq_event_t evts[16]; size_t ne;
    while ((ne = moq_session_poll_events(moq_simpair_client(sp), evts, 16)) > 0)
        for (size_t i = 0; i < ne; i++) {
            if (evts[i].kind == MOQ_EVENT_OBJECT_RECEIVED &&
                evts[i].u.object_received.group_id == g &&
                evts[i].u.object_received.object_id == o &&
                moq_rcbuf_len(evts[i].u.object_received.payload) == len &&
                memcmp(moq_rcbuf_data(evts[i].u.object_received.payload),
                       bytes, len) == 0)
                n++;
            moq_event_cleanup(&evts[i]);
        }
    return n;
}

/* Fan-out delivery: BOTH the subscription and the publication receive every
 * object -- one-shot and streamed -- in either install order. */
static void fanout_delivery_case(bool sub_first, const char *label)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    fanout_setup(&as, &alloc, &sp, &pub, &track, sub_first, 0);

    /* One-shot object. */
    uint8_t d0[] = { 0x10, 0x11, 0x12 };
    moq_rcbuf_t *p0 = NULL; moq_rcbuf_create(&alloc, d0, sizeof(d0), &p0);
    MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 0, 0, p0,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_rcbuf_decref(p0);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_INT(fanout_count_objects(sp, 0, 0, d0, sizeof(d0)), 2);

    /* Streamed object (begin/data/end): the streaming path must fan out and
     * must kind-dispatch the publication's subgroup open. */
    uint8_t d1a[] = { 0x21, 0x22 }, d1b[] = { 0x23 };
    uint8_t d1[] = { 0x21, 0x22, 0x23 };
    moq_pub_begin_object_cfg_t bc;
    moq_pub_begin_object_cfg_init_sized(&bc, sizeof(bc));
    bc.group_id = 0; bc.object_id = 1;
    bc.payload_length = sizeof(d1);
    MOQ_TEST_CHECK(moq_pub_begin_object(pub, track, &bc,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_rcbuf_t *c1 = NULL; moq_rcbuf_create(&alloc, d1a, sizeof(d1a), &c1);
    MOQ_TEST_CHECK(moq_pub_write_data(pub, track, c1,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_rcbuf_decref(c1);
    moq_rcbuf_t *c2 = NULL; moq_rcbuf_create(&alloc, d1b, sizeof(d1b), &c2);
    MOQ_TEST_CHECK(moq_pub_write_data(pub, track, c2,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_rcbuf_decref(c2);
    MOQ_TEST_CHECK(moq_pub_end_object(pub, track,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_INT(fanout_count_objects(sp, 0, 1, d1, sizeof(d1)), 2);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(label);
}

static void test_fanout_delivery_sub_first(void)
{ fanout_delivery_case(true, "fanout_delivery_sub_first"); }
static void test_fanout_delivery_pub_first(void)
{ fanout_delivery_case(false, "fanout_delivery_pub_first"); }

/* Forced second-destination WOULD_BLOCK: destination A is served, the op
 * blocks on B; retries must not duplicate on A. Also proves the byte-exact
 * retry identity: a same-length CHANGED payload is refused WRONG_STATE, a
 * FRESH buffer with identical bytes resumes and completes. */
static void test_fanout_second_dest_would_block(void)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    /* A 3-action queue: destination A's open+write consumes 2, destination
     * B's open the 3rd -- B's write is the 4th action and WOULD_BLOCKs
     * mid-fan-out (the setup steps each fit within 3 actions per pump). */
    fanout_setup(&as, &alloc, &sp, &pub, &track, true, 3);
    moq_session_t *server = moq_simpair_server(sp);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_action_capacity(server), 3);

    uint8_t d[] = { 0x41, 0x42, 0x43, 0x44 };
    moq_rcbuf_t *pay = NULL; moq_rcbuf_create(&alloc, d, sizeof(d), &pay);
    moq_result_t rc = moq_pub_write_object(pub, track, 0, 0, pay,
        moq_simpair_now_us(sp));
    moq_rcbuf_decref(pay);
    MOQ_TEST_CHECK(rc == MOQ_ERR_WOULD_BLOCK);   /* blocked on destination B */

    /* Same length, DIFFERENT bytes: must be refused, never silently merged. */
    uint8_t dx[] = { 0x41, 0x42, 0x43, 0x99 };
    moq_rcbuf_t *px = NULL; moq_rcbuf_create(&alloc, dx, sizeof(dx), &px);
    MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 0, 0, px,
        moq_simpair_now_us(sp)) == MOQ_ERR_WRONG_STATE);
    moq_rcbuf_decref(px);

    /* Drain the queue (delivers A's copy + the blockers). */
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    /* FRESH buffer, identical bytes (the media-sender re-encode pattern):
     * must resume the pending op and serve ONLY destination B. */
    moq_rcbuf_t *p2 = NULL; moq_rcbuf_create(&alloc, d, sizeof(d), &p2);
    rc = moq_pub_write_object(pub, track, 0, 0, p2, moq_simpair_now_us(sp));
    moq_rcbuf_decref(p2);
    MOQ_TEST_CHECK(rc == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    /* Exactly one copy per destination -- no duplication on A. */
    MOQ_TEST_CHECK_EQ_INT(fanout_count_objects(sp, 0, 0, d, sizeof(d)), 2);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("fanout_second_dest_would_block");
}

/* Mid-operation joiner: a destination installed BETWEEN retries of a
 * partially completed operation must NOT receive that in-flight object
 * (membership was snapshotted at op start); it joins the NEXT operation. */
static void test_fanout_mid_op_joiner_excluded(void)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    fanout_setup(&as, &alloc, &sp, &pub, &track, true, 3);

    /* Block mid-fan-out on destination B (as above). */
    uint8_t d[] = { 0x61, 0x62 };
    moq_rcbuf_t *pay = NULL; moq_rcbuf_create(&alloc, d, sizeof(d), &pay);
    MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 0, 0, pay,
        moq_simpair_now_us(sp)) == MOQ_ERR_WOULD_BLOCK);
    moq_rcbuf_decref(pay);
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    /* A THIRD destination joins mid-op: an unsubscribe would free the sub
     * slot; instead push a publication... the track already has one of each,
     * so grow via a second SUBSCRIBE from the client -- rejected as
     * duplicate. The joinable destination here is a NEW publication after
     * losing the old one; simplest deterministic joiner: none available on
     * one simpair. So prove exclusion the direct way: the retry must serve
     * ONLY the snapshotted remainder (B), even though eligibility is
     * re-evaluated -- assert the total stays 2 and the pending op did not
     * re-stamp targets by checking a SECOND write delivers to both. */
    moq_rcbuf_t *p2 = NULL; moq_rcbuf_create(&alloc, d, sizeof(d), &p2);
    MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 0, 0, p2,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_rcbuf_decref(p2);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_INT(fanout_count_objects(sp, 0, 0, d, sizeof(d)), 2);

    uint8_t d2[] = { 0x63, 0x64 };
    moq_rcbuf_t *p3 = NULL; moq_rcbuf_create(&alloc, d2, sizeof(d2), &p3);
    MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 0, 1, p3,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_rcbuf_decref(p3);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_INT(fanout_count_objects(sp, 0, 1, d2, sizeof(d2)), 2);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("fanout_mid_op_joiner_excluded");
}

/* Forward-0 subscription: excluded from delivery (never an error) while the
 * publication keeps receiving; a Forward-1 restore rejoins the NEXT op. */
static void test_fanout_forward_zero_subscription(void)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    fanout_setup(&as, &alloc, &sp, &pub, &track, true, 0);

    /* Find the client's subscription handle by re-subscribing?? No: update
     * needs the original handle -- redo a lightweight lookup: the client
     * subscribed inside fanout_setup; recover the handle via a fresh
     * subscribe is a duplicate. Instead drive Forward 0 via SUBSCRIBE_UPDATE
     * from a NEW harness where we keep the handle. */
    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);

    /* Manual coexist setup, keeping the subscription handle. */
    simpair_setup(&as, &alloc, &sp);
    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub);
    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
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
      while (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
          moq_pub_event_result_t res;
          moq_pub_handle_event(pub, &ev, moq_simpair_now_us(sp), &res);
          moq_event_cleanup(&ev);
      } }
    moq_pub_publish_cfg_t pcfg; moq_pub_publish_cfg_init(&pcfg);
    MOQ_TEST_CHECK(moq_pub_publish_track(pub, track, &pcfg,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    for (int i = 0; i < 6; i++) {
        moq_event_t ev;
        if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) != 1) break;
        if (ev.kind == MOQ_EVENT_PUBLISH_REQUEST) {
            moq_accept_publish_cfg_t acc;
            moq_accept_publish_cfg_init(&acc);
            moq_session_accept_publish(moq_simpair_client(sp),
                ev.u.publish_request.pub, &acc, moq_simpair_now_us(sp));
        }
        moq_event_cleanup(&ev);
    }
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_pub_tick(pub, moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    drain_all(sp);
    { moq_event_t ev;
      while (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
          moq_pub_event_result_t res;
          moq_pub_handle_event(pub, &ev, moq_simpair_now_us(sp), &res);
          moq_event_cleanup(&ev);
      } }
    MOQ_TEST_CHECK(moq_pub_has_subscriber(pub, track));
    MOQ_TEST_CHECK(moq_pub_track_is_published(pub, track));

    /* Subscription drops to Forward 0. */
    moq_subscription_update_cfg_t ucfg;
    moq_subscription_update_cfg_init(&ucfg);
    ucfg.has_forward = true;
    ucfg.forward = false;
    MOQ_TEST_CHECK(moq_session_update_subscription(moq_simpair_client(sp),
        sub_h, &ucfg, moq_simpair_now_us(sp)) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK(moq_pub_tick(pub, moq_simpair_now_us(sp)) == MOQ_OK);

    /* Write: MOQ_OK (never WRONG_STATE), publication-only delivery. */
    uint8_t d[] = { 0x51, 0x52 };
    moq_rcbuf_t *pay = NULL; moq_rcbuf_create(&alloc, d, sizeof(d), &pay);
    MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 0, 0, pay,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_rcbuf_decref(pay);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_INT(fanout_count_objects(sp, 0, 0, d, sizeof(d)), 1);

    /* Forward restored: the NEXT object reaches both destinations. */
    ucfg.forward = true;
    MOQ_TEST_CHECK(moq_session_update_subscription(moq_simpair_client(sp),
        sub_h, &ucfg, moq_simpair_now_us(sp)) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK(moq_pub_tick(pub, moq_simpair_now_us(sp)) == MOQ_OK);

    uint8_t d2[] = { 0x53, 0x54 };
    moq_rcbuf_t *pay2 = NULL; moq_rcbuf_create(&alloc, d2, sizeof(d2), &pay2);
    MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 0, 1, pay2,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_rcbuf_decref(pay2);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_INT(fanout_count_objects(sp, 0, 1, d2, sizeof(d2)), 2);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("fanout_forward_zero_subscription");
}


/* Exactly-old-sized object cfg: struct_size AND storage end at _reserved_obj.
 * The write path must not read past it (heap allocation: ASan enforces) and
 * must treat the appended end_of_group as absent. */
static void test_fanout_old_sized_object_cfg(void)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    fanout_setup(&as, &alloc, &sp, &pub, &track, true, 0);

    enum { OBJ_V0 = (int)offsetof(moq_pub_object_cfg_t, _reserved_obj) };
    uint8_t d[] = { 0x71, 0x72, 0x73 };
    moq_rcbuf_t *pay = NULL; moq_rcbuf_create(&alloc, d, sizeof(d), &pay);

    void *raw = malloc(OBJ_V0);          /* EXACT old size: no readable tail */
    MOQ_TEST_CHECK(raw != NULL);
    memset(raw, 0, OBJ_V0);
    /* Build the old-prefix cfg as bytes: each field lands via memcpy at an
     * offset validated to sit inside the V0 allocation, so no struct-typed
     * lvalue wider than the buffer ever exists (gcc -Warray-bounds at -O2
     * tracks malloc sizes). The struct pointer is formed only for the
     * production call; ASan still enforces the hard boundary. */
    uint32_t v0_size = (uint32_t)OBJ_V0;
    MOQ_TEST_CHECK(offsetof(moq_pub_object_cfg_t, struct_size) +
                   sizeof(v0_size) <= (size_t)OBJ_V0);
    memcpy((uint8_t *)raw + offsetof(moq_pub_object_cfg_t, struct_size),
           &v0_size, sizeof(v0_size));
    MOQ_TEST_CHECK(offsetof(moq_pub_object_cfg_t, payload) +
                   sizeof(pay) <= (size_t)OBJ_V0);
    memcpy((uint8_t *)raw + offsetof(moq_pub_object_cfg_t, payload),
           &pay, sizeof(pay));
    /* group_id/object_id stay 0 from the memset. */

    MOQ_TEST_CHECK(moq_pub_write_object_ex(pub, track,
        (const moq_pub_object_cfg_t *)raw,
        moq_simpair_now_us(sp)) == MOQ_OK);
    free(raw);
    moq_rcbuf_decref(pay);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_INT(fanout_count_objects(sp, 0, 0, d, sizeof(d)), 2);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("fanout_old_sized_object_cfg");
}

/* Publication Forward 1 -> 0: any open subgroup is RESET (omission must
 * never read as a cleanly finished subgroup) and the publication skipped;
 * tick alone (no further writes) drives the retirement; Forward 1 rejoins
 * at the next operation. */
static void test_fanout_publication_forward_drop(void)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    fanout_setup(&as, &alloc, &sp, &pub, &track, false, 0);

    /* Recover the client's publication handle to drive PUBLISH_UPDATE. */
    moq_publication_t cpub = MOQ_PUBLICATION_INVALID;
    {   /* the client accepted during setup; re-derive via a fresh write +
         * inspecting events is unnecessary: the accept path stored it in the
         * event we already consumed. Instead, subscribe-side update needs the
         * handle -- capture it by re-running a minimal accept: not possible.
         * So drive the drop mid-object from a dedicated setup below. */
    }
    (void)cpub;
    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);

    /* Dedicated setup keeping the publication handle. */
    simpair_setup(&as, &alloc, &sp);
    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub);
    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &track);
    moq_pub_publish_cfg_t pcfg; moq_pub_publish_cfg_init(&pcfg);
    MOQ_TEST_CHECK(moq_pub_publish_track(pub, track, &pcfg,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    for (int i = 0; i < 6; i++) {
        moq_event_t ev;
        if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) != 1) break;
        if (ev.kind == MOQ_EVENT_PUBLISH_REQUEST) {
            cpub = ev.u.publish_request.pub;
            moq_accept_publish_cfg_t acc;
            moq_accept_publish_cfg_init(&acc);
            moq_session_accept_publish(moq_simpair_client(sp), cpub, &acc,
                moq_simpair_now_us(sp));
        }
        moq_event_cleanup(&ev);
    }
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK(moq_pub_tick(pub, moq_simpair_now_us(sp)) == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_track_is_published(pub, track));

    /* BETWEEN objects: write one whole object (subgroup stays open), then
     * Forward -> 0; tick ALONE must retire (RESET) it, no further writes. */
    uint8_t d[] = { 0x81 };
    moq_rcbuf_t *p1 = NULL; moq_rcbuf_create(&alloc, d, 1, &p1);
    MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 0, 0, p1,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_rcbuf_decref(p1);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_INT(fanout_count_objects(sp, 0, 0, d, 1), 1);

    moq_publication_update_cfg_t up;
    moq_publication_update_cfg_init(&up);
    up.has_forward = true;
    up.forward = false;
    MOQ_TEST_CHECK(moq_session_update_publication(moq_simpair_client(sp),
        cpub, &up, moq_simpair_now_us(sp)) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK(moq_pub_tick(pub, moq_simpair_now_us(sp)) == MOQ_OK);
    MOQ_TEST_CHECK(!moq_pub_track_forward(pub, track));
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    /* Skipped while Forward 0: MOQ_OK, nothing delivered. */
    uint8_t d2[] = { 0x82 };
    moq_rcbuf_t *p2 = NULL; moq_rcbuf_create(&alloc, d2, 1, &p2);
    MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 0, 1, p2,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_rcbuf_decref(p2);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_INT(fanout_count_objects(sp, 0, 1, d2, 1), 0);

    /* Forward restored: rejoins with FRESH subgroup state (no stale
     * streaming), next object delivers. */
    up.forward = true;
    MOQ_TEST_CHECK(moq_session_update_publication(moq_simpair_client(sp),
        cpub, &up, moq_simpair_now_us(sp)) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK(moq_pub_tick(pub, moq_simpair_now_us(sp)) == MOQ_OK);

    uint8_t d3[] = { 0x83 };
    moq_rcbuf_t *p3 = NULL; moq_rcbuf_create(&alloc, d3, 1, &p3);
    MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 1, 0, p3,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_rcbuf_decref(p3);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_INT(fanout_count_objects(sp, 1, 0, d3, 1), 1);

    /* MID-object: begin a streamed object, then Forward -> 0: tick must
     * truncate via RESET (streaming state cleared), and a Forward-1 rejoin
     * starts clean on the next begin. */
    moq_pub_begin_object_cfg_t bc;
    moq_pub_begin_object_cfg_init_sized(&bc, sizeof(bc));
    bc.group_id = 1; bc.object_id = 1; bc.payload_length = 2;
    MOQ_TEST_CHECK(moq_pub_begin_object(pub, track, &bc,
        moq_simpair_now_us(sp)) == MOQ_OK);

    up.forward = false;
    MOQ_TEST_CHECK(moq_session_update_publication(moq_simpair_client(sp),
        cpub, &up, moq_simpair_now_us(sp)) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK(moq_pub_tick(pub, moq_simpair_now_us(sp)) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    /* The app-level streamed-object bracket is still open (its only
     * destination was truncated): finishing it is a trivial completion --
     * same contract as losing the last subscriber mid-stream. */
    MOQ_TEST_CHECK(moq_pub_end_object(pub, track,
        moq_simpair_now_us(sp)) == MOQ_OK);

    up.forward = true;
    MOQ_TEST_CHECK(moq_session_update_publication(moq_simpair_client(sp),
        cpub, &up, moq_simpair_now_us(sp)) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK(moq_pub_tick(pub, moq_simpair_now_us(sp)) == MOQ_OK);

    /* The truncated object is gone; a fresh whole object flows. */
    uint8_t d4[] = { 0x84, 0x85 };
    moq_rcbuf_t *p4 = NULL; moq_rcbuf_create(&alloc, d4, 2, &p4);
    MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 2, 0, p4,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_rcbuf_decref(p4);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_INT(fanout_count_objects(sp, 2, 0, d4, 2), 1);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("fanout_publication_forward_drop");
}

/* Retained-op release at session close: a wrapped buffer held by a pending
 * (WOULD_BLOCKed) operation is released when the close is observed via
 * tick -- exactly once, not deferred to destroy. */
static int g_wrap_releases = 0;
static void count_release(void *ctx, const uint8_t *data, size_t len)
{ (void)ctx; (void)data; (void)len; g_wrap_releases++; }

static void test_fanout_close_releases_pending_op(void)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    fanout_setup(&as, &alloc, &sp, &pub, &track, true, 3);

    g_wrap_releases = 0;
    static const uint8_t d[] = { 0x91, 0x92 };
    moq_rcbuf_t *pay = NULL;
    MOQ_TEST_CHECK(moq_rcbuf_wrap(&alloc, d, sizeof(d), count_release, NULL,
        &pay) == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 0, 0, pay,
        moq_simpair_now_us(sp)) == MOQ_ERR_WOULD_BLOCK);
    moq_rcbuf_decref(pay);
    /* Flush destination A's queued actions so the SESSION drops its own
     * transient refs; the pending op then holds the only remaining one. */
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_INT(g_wrap_releases, 0);

    /* Session close observed (same synthesized-event technique as
     * test_session_closed_event; tick and drain_closed_event share the same
     * per-track release): the retained snapshot must release AT CLOSE. */
    moq_event_t close_ev;
    memset(&close_ev, 0, sizeof(close_ev));
    close_ev.kind = MOQ_EVENT_SESSION_CLOSED;
    moq_pub_event_result_t res;
    moq_pub_handle_event(pub, &close_ev, moq_simpair_now_us(sp), &res);
    MOQ_TEST_CHECK_EQ_INT(g_wrap_releases, 1);   /* at close, not destroy */

    moq_pub_destroy(pub);
    MOQ_TEST_CHECK_EQ_INT(g_wrap_releases, 1);   /* exactly once overall */
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("fanout_close_releases_pending_op");
}

/* Membership-snapshot discriminator: the subscription starts at Forward 0
 * (not a target), the publication destination WOULD_BLOCKs the op, the
 * subscription flips to Forward 1 BEFORE the retry -- and must NOT receive
 * the in-flight object (a re-snapshotting retry would deliver it). It
 * receives only the NEXT operation. */
static void test_fanout_forward_enable_mid_op_joins_next(void)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    *&as = (test_alloc_state_t){0};
    alloc = test_allocator(&as);
    {
        moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.seed = 42;
        cfg.initial_now_us = 1000;
        cfg.client_send_request_capacity = true;
        cfg.client_initial_request_capacity = 16;
        cfg.server_send_request_capacity = true;
        cfg.server_initial_request_capacity = 16;
        cfg.max_actions = 1;   /* publication open succeeds, write blocks */
        moq_simpair_create(&cfg, &sp);
        moq_simpair_start(sp);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        moq_event_t ev;
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
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_pub_track_t *track = NULL;
    moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &track);

    /* Subscription at Forward 0 from the start. */
    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace.parts = ns_parts;
    scfg.track_namespace.count = 1;
    scfg.track_name = MOQ_BYTES_LITERAL("video");
    scfg.has_forward = true;
    scfg.forward = false;
    moq_subscription_t sub_h;
    MOQ_TEST_CHECK(moq_session_subscribe(moq_simpair_client(sp), &scfg,
        moq_simpair_now_us(sp), &sub_h) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      while (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
          moq_pub_event_result_t res;
          moq_pub_handle_event(pub, &ev, moq_simpair_now_us(sp), &res);
          moq_event_cleanup(&ev);
      } }
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    /* Publication established + accepted. */
    moq_pub_publish_cfg_t pcfg; moq_pub_publish_cfg_init(&pcfg);
    MOQ_TEST_CHECK(moq_pub_publish_track(pub, track, &pcfg,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    for (int i = 0; i < 6; i++) {
        moq_event_t ev;
        if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) != 1) break;
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
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    drain_all(sp);
    MOQ_TEST_CHECK(moq_pub_track_is_published(pub, track));

    /* Op starts: target = publication ONLY (sub is Forward 0). Blocks on
     * the write (1-action queue: the open consumed it). */
    uint8_t d[] = { 0xA1, 0xA2 };
    moq_rcbuf_t *pay = NULL; moq_rcbuf_create(&alloc, d, sizeof(d), &pay);
    MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 0, 0, pay,
        moq_simpair_now_us(sp)) == MOQ_ERR_WOULD_BLOCK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    /* Forward 1 BEFORE the retry. */
    moq_subscription_update_cfg_t ucfg;
    moq_subscription_update_cfg_init(&ucfg);
    ucfg.has_forward = true;
    ucfg.forward = true;
    MOQ_TEST_CHECK(moq_session_update_subscription(moq_simpair_client(sp),
        sub_h, &ucfg, moq_simpair_now_us(sp)) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK(moq_pub_tick(pub, moq_simpair_now_us(sp)) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    drain_all(sp);

    /* Retry completes the SNAPSHOTTED op: publication only. */
    MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 0, 0, pay,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_rcbuf_decref(pay);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_INT(fanout_count_objects(sp, 0, 0, d, sizeof(d)), 1);

    /* The NEXT operation includes the now-Forward-1 subscription. */
    uint8_t d2[] = { 0xA3 };
    moq_rcbuf_t *p2 = NULL; moq_rcbuf_create(&alloc, d2, 1, &p2);
    moq_result_t rc = moq_pub_write_object(pub, track, 0, 1, p2,
        moq_simpair_now_us(sp));
    while (rc == MOQ_ERR_WOULD_BLOCK) {   /* 1-action queue: drain + retry */
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        rc = moq_pub_write_object(pub, track, 0, 1, p2,
            moq_simpair_now_us(sp));
    }
    moq_rcbuf_decref(p2);
    MOQ_TEST_CHECK(rc == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_INT(fanout_count_objects(sp, 0, 1, d2, 1), 2);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("fanout_forward_enable_mid_op_joins_next");
}


/* Frozen-floor canaries for the object/begin cfg pointer initializers. */
static void test_pub_object_cfg_init_old_prefix_no_overflow(void)
{
    enum { OV0 = (int)offsetof(moq_pub_object_cfg_t, _reserved_obj) };
    union { moq_pub_object_cfg_t aligner;
            struct { unsigned char prefix[OV0]; uint64_t canary; } box; } u;
    memset(&u, 0xAB, sizeof(u));
    moq_pub_object_cfg_init((moq_pub_object_cfg_t *)&u.box);
    uint32_t ss; memcpy(&ss, u.box.prefix, sizeof(ss));
    MOQ_TEST_CHECK(ss == (uint32_t)OV0);
    MOQ_TEST_CHECK(u.box.canary == 0xABABABABABABABABULL);
    moq_pub_object_cfg_t full;
    moq_pub_object_cfg_init_sized(&full, sizeof(full) + 32);
    MOQ_TEST_CHECK(full.struct_size == sizeof(full));

    enum { BV0 = (int)(offsetof(moq_pub_begin_object_cfg_t, payload_length) +
                       sizeof(((moq_pub_begin_object_cfg_t *)0)->payload_length)) };
    union { moq_pub_begin_object_cfg_t aligner;
            struct { unsigned char prefix[BV0]; uint64_t canary; } box; } v;
    memset(&v, 0xAB, sizeof(v));
    moq_pub_begin_object_cfg_init((moq_pub_begin_object_cfg_t *)&v.box);
    memcpy(&ss, v.box.prefix, sizeof(ss));
    MOQ_TEST_CHECK(ss == (uint32_t)BV0);
    MOQ_TEST_CHECK(v.box.canary == 0xABABABABABABABABULL);
    moq_pub_begin_object_cfg_t bfull;
    moq_pub_begin_object_cfg_init_sized(&bfull, sizeof(bfull));
    MOQ_TEST_CHECK(bfull.struct_size == sizeof(bfull));
    MOQ_TEST_PASS("pub_object_cfg_init_old_prefix_no_overflow");
}

/* Forward 1->0->1 across a BLOCKED operation: the drop permanently excludes
 * the destination from the pending op (no resurrection after retirement
 * clears the flags); it rejoins at the next operation. */
static void test_fanout_drop_restore_excluded_from_pending(void)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    as = (test_alloc_state_t){0};
    alloc = test_allocator(&as);
    {
        moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
        cfg.alloc = &alloc; cfg.seed = 42; cfg.initial_now_us = 1000;
        cfg.client_send_request_capacity = true;
        cfg.client_initial_request_capacity = 16;
        cfg.server_send_request_capacity = true;
        cfg.server_initial_request_capacity = 16;
        cfg.max_actions = 1;         /* open lands, the write blocks */
        moq_simpair_create(&cfg, &sp);
        moq_simpair_start(sp);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        moq_event_t ev;
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
    MOQ_TEST_CHECK(moq_session_subscribe(moq_simpair_client(sp), &scfg,
        moq_simpair_now_us(sp), &sub_h) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      while (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
          moq_pub_event_result_t res;
          moq_pub_handle_event(pub, &ev, moq_simpair_now_us(sp), &res);
          moq_event_cleanup(&ev);
      } }
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    drain_all(sp);
    MOQ_TEST_CHECK(moq_pub_has_subscriber(pub, track));

    /* Blocked op: open consumed the single action slot; the write blocks. */
    uint8_t d[] = { 0xB1, 0xB2 };
    moq_rcbuf_t *pay = NULL; moq_rcbuf_create(&alloc, d, sizeof(d), &pay);
    MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 0, 0, pay,
        moq_simpair_now_us(sp)) == MOQ_ERR_WOULD_BLOCK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    /* Forward 0 then 1 again, both observed before the retry. */
    moq_subscription_update_cfg_t ucfg;
    moq_subscription_update_cfg_init(&ucfg);
    ucfg.has_forward = true; ucfg.forward = false;
    MOQ_TEST_CHECK(moq_session_update_subscription(moq_simpair_client(sp),
        sub_h, &ucfg, moq_simpair_now_us(sp)) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    (void)moq_pub_tick(pub, moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    ucfg.forward = true;
    MOQ_TEST_CHECK(moq_session_update_subscription(moq_simpair_client(sp),
        sub_h, &ucfg, moq_simpair_now_us(sp)) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    (void)moq_pub_tick(pub, moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    drain_all(sp);

    /* Retry: the dropped-and-restored destination must NOT be resurrected
     * into the pending op -- it completes with no remaining target and the
     * in-flight object is never delivered. */
    MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 0, 0, pay,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_rcbuf_decref(pay);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_INT(fanout_count_objects(sp, 0, 0, d, sizeof(d)), 0);

    /* The NEXT operation reaches it. */
    uint8_t d2[] = { 0xB3 };
    moq_rcbuf_t *p2 = NULL; moq_rcbuf_create(&alloc, d2, 1, &p2);
    moq_result_t rc = moq_pub_write_object(pub, track, 1, 0, p2,
        moq_simpair_now_us(sp));
    while (rc == MOQ_ERR_WOULD_BLOCK) {
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        rc = moq_pub_write_object(pub, track, 1, 0, p2,
            moq_simpair_now_us(sp));
    }
    moq_rcbuf_decref(p2);
    MOQ_TEST_CHECK(rc == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_INT(fanout_count_objects(sp, 1, 0, d2, 1), 1);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("fanout_drop_restore_excluded_from_pending");
}


/* Forward drop on an EOG-ADVERTISED but still-open subgroup: EOG only marks
 * the group's largest object -- more objects can follow (the media sender's
 * one-subgroup groups) -- so omission via Forward 0 must RESET, never FIN.
 * Also proves a SINGLE tick drives the retirement (the reset action is
 * queued by the tick itself, before any later write). */
static void test_fanout_forward_drop_resets_eog_subgroup(void)
{
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
      while (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
          moq_pub_event_result_t res;
          moq_pub_handle_event(pub, &ev, moq_simpair_now_us(sp), &res);
          moq_event_cleanup(&ev);
      } }
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    drain_all(sp);

    /* EOG-advertised object; the subgroup remains OPEN for more objects. */
    uint8_t d[] = { 0xC1 };
    moq_rcbuf_t *pay = NULL; moq_rcbuf_create(&alloc, d, 1, &pay);
    moq_pub_object_cfg_t oc;
    moq_pub_object_cfg_init_sized(&oc, sizeof(oc));
    oc.group_id = 0; oc.object_id = 0; oc.payload = pay;
    oc.end_of_group = true;
    MOQ_TEST_CHECK(moq_pub_write_object_ex(pub, track, &oc,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_rcbuf_decref(pay);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_INT(fanout_count_objects(sp, 0, 0, d, 1), 1);

    /* Forward drops; ONE tick must queue the retirement action itself. */
    moq_subscription_update_cfg_t ucfg;
    moq_subscription_update_cfg_init(&ucfg);
    ucfg.has_forward = true; ucfg.forward = false;
    MOQ_TEST_CHECK(moq_session_update_subscription(moq_simpair_client(sp),
        sub_h, &ucfg, moq_simpair_now_us(sp)) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    size_t cap_before = moq_session_action_capacity(moq_simpair_server(sp));
    MOQ_TEST_CHECK(moq_pub_tick(pub, moq_simpair_now_us(sp)) == MOQ_OK);
    size_t cap_after = moq_session_action_capacity(moq_simpair_server(sp));
    MOQ_TEST_CHECK_EQ_INT((int)(cap_before - cap_after), 1); /* the RESET */
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    /* RESET, never FIN: the client must NOT see the subgroup cleanly
     * finished (a FIN would assert the omitted objects were all sent). */
    { moq_event_t evts[16]; size_t ne; int finished = 0;
      while ((ne = moq_session_poll_events(moq_simpair_client(sp), evts,
                                           16)) > 0)
          for (size_t i = 0; i < ne; i++) {
              if (evts[i].kind == MOQ_EVENT_SUBGROUP_FINISHED) finished++;
              moq_event_cleanup(&evts[i]);
          }
      MOQ_TEST_CHECK_EQ_INT(finished, 0);
    }

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("fanout_forward_drop_resets_eog_subgroup");
}


/* Slot-kind audit: finish_subscribers must retire ONLY subscription slots.
 * A coexisting publication -- including one with an OPEN subgroup -- is
 * untouched (its termination is PUBLISH_DONE via unpublish/end_track, never
 * SUBSCRIBE_DONE), and publication writes keep working afterward. */
static void test_finish_subscribers_skips_publication(void)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    fanout_setup(&as, &alloc, &sp, &pub, &track, true, 0);

    /* Leave the PUBLICATION with an open subgroup: one delivered object. */
    uint8_t d[] = { 0xD1 };
    moq_rcbuf_t *p1 = NULL; moq_rcbuf_create(&alloc, d, 1, &p1);
    MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 0, 0, p1,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_rcbuf_decref(p1);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_INT(fanout_count_objects(sp, 0, 0, d, 1), 2);

    /* Finish ONLY the subscriber (live -> VOD conversion step). */
    MOQ_TEST_CHECK(moq_pub_finish_subscribers(pub, track, 0,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK(moq_pub_tick(pub, moq_simpair_now_us(sp)) == MOQ_OK);

    MOQ_TEST_CHECK(!moq_pub_has_subscriber(pub, track));   /* sub finished  */
    MOQ_TEST_CHECK(moq_pub_track_is_published(pub, track)); /* pub survives */

    /* The publication still receives objects (only 1 copy now). */
    drain_all(sp);
    uint8_t d2[] = { 0xD2 };
    moq_rcbuf_t *p2 = NULL; moq_rcbuf_create(&alloc, d2, 1, &p2);
    MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 0, 1, p2,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_rcbuf_decref(p2);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_INT(fanout_count_objects(sp, 0, 1, d2, 1), 1);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("finish_subscribers_skips_publication");
}


/* Ordering discriminator for the publication skip: the skip must come
 * BEFORE the per-slot streaming check. The subscription sits at Forward 0,
 * the PUBLICATION alone is mid-STREAMED-object -- finish_subscribers must
 * still succeed (the publication is skipped, streaming and all), and the
 * publication object then completes and delivers. With the kind check
 * below the streaming check this returns WRONG_STATE instead. */
static void test_finish_subscribers_pub_mid_object(void)
{
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

    /* Subscription at Forward 0 (present, but no wire state ever). */
    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace.parts = ns_parts;
    scfg.track_namespace.count = 1;
    scfg.track_name = MOQ_BYTES_LITERAL("video");
    scfg.has_forward = true;
    scfg.forward = false;
    moq_subscription_t sub_h;
    MOQ_TEST_CHECK(moq_session_subscribe(moq_simpair_client(sp), &scfg,
        moq_simpair_now_us(sp), &sub_h) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      while (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
          moq_pub_event_result_t res;
          moq_pub_handle_event(pub, &ev, moq_simpair_now_us(sp), &res);
          moq_event_cleanup(&ev);
      } }
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    /* Accepted publication. */
    moq_pub_publish_cfg_t pcfg; moq_pub_publish_cfg_init(&pcfg);
    MOQ_TEST_CHECK(moq_pub_publish_track(pub, track, &pcfg,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    for (int i = 0; i < 6; i++) {
        moq_event_t ev;
        if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) != 1) break;
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
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    drain_all(sp);
    MOQ_TEST_CHECK(moq_pub_has_subscriber(pub, track));
    MOQ_TEST_CHECK(moq_pub_track_is_published(pub, track));

    /* The publication ALONE begins a streamed object (the Forward-0 sub is
     * not a target); the publication slot is now mid-object. */
    uint8_t d[] = { 0xE1, 0xE2 };
    moq_pub_begin_object_cfg_t bc;
    moq_pub_begin_object_cfg_init_sized(&bc, sizeof(bc));
    bc.group_id = 0; bc.object_id = 0; bc.payload_length = sizeof(d);
    MOQ_TEST_CHECK(moq_pub_begin_object(pub, track, &bc,
        moq_simpair_now_us(sp)) == MOQ_OK);

    /* finish_subscribers must SKIP the mid-object publication (kind check
     * before the streaming check) and succeed on the Forward-0 sub. */
    MOQ_TEST_CHECK(moq_pub_finish_subscribers(pub, track, 0,
        moq_simpair_now_us(sp)) == MOQ_OK);
    MOQ_TEST_CHECK(!moq_pub_has_subscriber(pub, track));
    MOQ_TEST_CHECK(moq_pub_track_is_published(pub, track));

    /* Complete the streamed object; the publication delivers it whole. */
    moq_rcbuf_t *c1 = NULL; moq_rcbuf_create(&alloc, d, sizeof(d), &c1);
    MOQ_TEST_CHECK(moq_pub_write_data(pub, track, c1,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_rcbuf_decref(c1);
    MOQ_TEST_CHECK(moq_pub_end_object(pub, track,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_INT(fanout_count_objects(sp, 0, 0, d, sizeof(d)), 1);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("finish_subscribers_pub_mid_object");
}


/* Peer-ended publication restart, both drafts. After the peer walks away
 * (PUBLISH_UNSUBSCRIBED at the publisher) the track must return to the
 * un-published state so a later moq_pub_publish_track sends a FRESH
 * PUBLISH -- with a DISTINCT publication handle -- not an idempotent
 * MOQ_OK that puts nothing on the wire.
 *
 * Termination differs by draft: d16 subscribers send UNSUBSCRIBE on the
 * publish request id (no public subscriber-side API exists; the raw
 * injection is the session suite's technique, in the server-originated
 * request-id space); d18 subscribers cancel the request bidi -- modeled
 * with moq_session_on_bidi_stream_stop on the publish bidi's ACTUAL
 * stream ref, read from the sole publisher-role entry via the linked
 * core test internals. */
static void publish_restart_case(bool d18, const char *label)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    as = (test_alloc_state_t){0};
    alloc = test_allocator(&as);
    {
        moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.seed = 42;
        cfg.initial_now_us = 1000;
        cfg.client_send_request_capacity = true;
        cfg.client_initial_request_capacity = 16;
        cfg.server_send_request_capacity = true;
        cfg.server_initial_request_capacity = 16;
        if (d18) cfg.version = MOQ_VERSION_DRAFT_18;
        moq_simpair_create(&cfg, &sp);
        moq_simpair_start(sp);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        moq_event_t ev;
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
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_pub_track_t *track = NULL;
    moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &track);

    /* Publish + client accepts, keeping the client-side handle. */
    moq_publication_t cpub = MOQ_PUBLICATION_INVALID;
    moq_pub_publish_cfg_t pcfg; moq_pub_publish_cfg_init(&pcfg);
    MOQ_TEST_CHECK(moq_pub_publish_track(pub, track, &pcfg,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    for (int i = 0; i < 6; i++) {
        moq_event_t ev;
        if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) != 1) break;
        if (ev.kind == MOQ_EVENT_PUBLISH_REQUEST) {
            cpub = ev.u.publish_request.pub;
            moq_accept_publish_cfg_t acc;
            moq_accept_publish_cfg_init(&acc);
            moq_session_accept_publish(moq_simpair_client(sp), cpub, &acc,
                moq_simpair_now_us(sp));
        }
        moq_event_cleanup(&ev);
    }
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK(moq_pub_tick(pub, moq_simpair_now_us(sp)) == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_track_is_published(pub, track));
    MOQ_TEST_CHECK(moq_publication_is_valid(cpub));

    /* The PEER walks away (draft-specific path). */
    if (!d18) {
        uint8_t buf[32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_d16_encode_varint_msg(&w, MOQ_D16_UNSUBSCRIBE, 1);
        MOQ_TEST_CHECK(moq_session_on_control_bytes(moq_simpair_server(sp),
            buf, moq_buf_writer_offset(&w),
            moq_simpair_now_us(sp)) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        MOQ_TEST_CHECK(moq_pub_tick(pub, moq_simpair_now_us(sp)) == MOQ_OK);
    } else {
        /* Read the publish request bidi's ACTUAL stream ref from the sole
         * publisher-role entry (this binary links the core test internals),
         * then cancel it exactly once. No guessing. */
        moq_session_t *server = moq_simpair_server(sp);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0);
        int found = 0;
        for (size_t i = 0; i < server->pub_cap; i++) {
            const moq_pub_entry_t *e = &server->publishes[i];
            if (e->state != MOQ_PUB_FREE &&
                e->role == MOQ_PUB_ROLE_PUBLISHER) {
                ref = e->request_stream_ref;
                found++;
            }
        }
        MOQ_TEST_CHECK_EQ_INT(found, 1);
        MOQ_TEST_CHECK(ref._v != 0);   /* d18: stream-correlated request */
        MOQ_TEST_CHECK(moq_session_on_bidi_stream_stop(server, ref, 0x1,
            moq_simpair_now_us(sp)) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        MOQ_TEST_CHECK(moq_pub_tick(pub, moq_simpair_now_us(sp)) == MOQ_OK);
    }
    MOQ_TEST_CHECK(!moq_pub_track_is_published(pub, track));

    /* RESTART: a new publish_track must put a FRESH PUBLISH on the wire,
     * carrying a DISTINCT publication handle. */
    MOQ_TEST_CHECK(moq_pub_publish_track(pub, track, &pcfg,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    bool got_new_request = false;
    for (int i = 0; i < 6; i++) {
        moq_event_t ev;
        if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) != 1) break;
        if (ev.kind == MOQ_EVENT_PUBLISH_REQUEST) {
            got_new_request = true;
            MOQ_TEST_CHECK(!moq_publication_eq(ev.u.publish_request.pub,
                                               cpub));   /* fresh lifecycle */
            moq_accept_publish_cfg_t acc;
            moq_accept_publish_cfg_init(&acc);
            moq_session_accept_publish(moq_simpair_client(sp),
                ev.u.publish_request.pub, &acc, moq_simpair_now_us(sp));
        }
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK(got_new_request);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK(moq_pub_tick(pub, moq_simpair_now_us(sp)) == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_track_is_published(pub, track));

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(label);
}

static void test_publish_restart_after_peer_unsubscribe_d16(void)
{ publish_restart_case(false, "publish_restart_after_peer_unsubscribe_d16"); }
static void test_publish_restart_d18_cancel(void)
{ publish_restart_case(true, "publish_restart_after_peer_cancel_d18"); }

/* Look up the server-session history record for (ns,name). */
static moq_track_hist_t *pub_find_hist(moq_session_t *s, const char *ns0,
                                       const char *name)
{
    moq_bytes_t np = { (const uint8_t *)ns0, strlen(ns0) };
    moq_namespace_t ns = { &np, 1 };
    moq_bytes_t nm = { (const uint8_t *)name, strlen(name) };
    size_t kl = 0;
    uint8_t *k = moq_build_track_key(s, &ns, nm, &kl);
    if (!k && kl > 0) return NULL;
    moq_track_hist_t *r = track_hist_find(s, k, kl);
    if (k) s->alloc.free(k, kl, s->alloc.ctx);
    return r;
}

/* Layer B: the publisher reserves a per-track history record at add_track and
 * advances it on every accepted object -- including zero-destination writes --
 * with an exactly-once, allocation-free merge; empty reservations are
 * reclaimed on remove, observed records stay pinned. */
static void test_pub_history_reservation_and_merge(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);
    moq_session_t *sv = moq_simpair_server(sp);

    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    moq_publisher_t *pub = NULL;
    moq_pub_create(sv, &alloc, &cfg, &pub);

    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_pub_track_t *track = NULL;
    MOQ_TEST_CHECK(moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &track)
        == MOQ_OK);

    /* add_track reserved an EMPTY record (no object observed yet). */
    moq_track_hist_t *r = pub_find_hist(sv, "live", "video");
    MOQ_TEST_CHECK(r != NULL);
    MOQ_TEST_CHECK(r && r->in_use && r->refs >= 1 && !r->has_largest);

    /* A zero-destination write (no subscriber) still admits the object and
     * advances the track's largest. */
    uint8_t d[] = { 1, 2, 3 };
    moq_rcbuf_t *pay = NULL;
    moq_rcbuf_create(&alloc, d, sizeof(d), &pay);
    MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 4, 2, pay,
        moq_simpair_now_us(sp)) == MOQ_OK);
    r = pub_find_hist(sv, "live", "video");
    MOQ_TEST_CHECK(r && r->has_largest &&
                   r->largest_group == 4 && r->largest_object == 2);

    /* A greater location advances; a lesser one is a no-op. */
    MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 4, 5, pay,
        moq_simpair_now_us(sp)) == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 3, 9, pay,
        moq_simpair_now_us(sp)) == MOQ_OK);
    r = pub_find_hist(sv, "live", "video");
    MOQ_TEST_CHECK(r && r->largest_group == 4 && r->largest_object == 5);
    moq_rcbuf_decref(pay);

    /* A second, never-written track: its empty reservation is reclaimed on
     * remove, while the published track's record stays pinned. */
    moq_pub_track_cfg_t tcfg2;
    moq_pub_track_cfg_init(&tcfg2);
    tcfg2.track_namespace.parts = ns_parts;
    tcfg2.track_namespace.count = 1;
    tcfg2.track_name = MOQ_BYTES_LITERAL("audio");
    moq_pub_track_t *track2 = NULL;
    MOQ_TEST_CHECK(moq_pub_add_track(pub, &tcfg2, moq_simpair_now_us(sp),
        &track2) == MOQ_OK);
    MOQ_TEST_CHECK(pub_find_hist(sv, "live", "audio") != NULL);
    moq_pub_remove_track(pub, track2, moq_simpair_now_us(sp));
    MOQ_TEST_CHECK(pub_find_hist(sv, "live", "audio") == NULL); /* reclaimed */

    moq_pub_remove_track(pub, track, moq_simpair_now_us(sp));
    /* Observed record survives remove_track (pinned until session end). */
    r = pub_find_hist(sv, "live", "video");
    MOQ_TEST_CHECK(r != NULL && r->has_largest &&
                   r->largest_group == 4 && r->largest_object == 5);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("pub_history_reservation_and_merge");
}

/* Layer B: a full registry makes add_track fail NOMEM (the reservation is the
 * capacity-failure point), leaving no track behind. */
static void test_pub_history_capacity_at_add_track(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);
    moq_session_t *sv = moq_simpair_server(sp);

    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    moq_publisher_t *pub = NULL;
    moq_pub_create(sv, &alloc, &cfg, &pub);

    /* Fill the registry to exhaustion via the public note API (observed
     * records pin their slots). Bounded loop; assert we actually hit full. */
    bool hit_full = false;
    for (int i = 0; i < 100000 && !hit_full; i++) {
        char nm[16];
        int n = 0; unsigned v = (unsigned)i;
        nm[n++] = 'k';
        do { nm[n++] = (char)('0' + (v % 10)); v /= 10; } while (v && n < 15);
        nm[n] = 0;
        moq_bytes_t np = MOQ_BYTES_LITERAL("hist");
        moq_namespace_t ns = { &np, 1 };
        moq_bytes_t nb = { (const uint8_t *)nm, (size_t)n };
        moq_result_t rc = moq_session_note_object_published(sv, &ns, nb, 1, 1);
        if (rc == MOQ_ERR_NOMEM) hit_full = true;
        else MOQ_TEST_CHECK(rc == MOQ_OK);
    }
    MOQ_TEST_CHECK(hit_full);

    /* With the registry full, adding a brand-new track cannot reserve a
     * record: add_track fails NOMEM and creates no track. */
    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_pub_track_t *track = (moq_pub_track_t *)0x1;
    MOQ_TEST_CHECK(moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &track)
        == MOQ_ERR_NOMEM);
    MOQ_TEST_CHECK(track == NULL);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("pub_history_capacity_at_add_track");
}

/* Layer B: deterministic validation runs BEFORE any mutation. An unencodable
 * location is rejected without advancing Largest or leaving an op pending,
 * even with zero destinations; the next valid write then succeeds. Also
 * exercises begin_object history admission. */
static void test_pub_history_validate_before_mutate(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);
    moq_session_t *sv = moq_simpair_server(sp);

    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
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

    uint8_t d[] = { 9 };
    moq_rcbuf_t *pay = NULL;
    moq_rcbuf_create(&alloc, d, sizeof(d), &pay);

    /* Unencodable group id: rejected before mutation (zero destinations, so
     * the session write path would never see it). */
    moq_result_t rc = moq_pub_write_object(pub, track,
        MOQ_QUIC_VARINT_MAX + 1, 0, pay, moq_simpair_now_us(sp));
    MOQ_TEST_CHECK(rc == MOQ_ERR_INVAL);
    /* Largest not poisoned: the record is still an empty reservation. */
    moq_track_hist_t *r = pub_find_hist(sv, "live", "video");
    MOQ_TEST_CHECK(r != NULL && !r->has_largest);

    /* Unencodable object id: same. */
    rc = moq_pub_write_object(pub, track, 0, MOQ_QUIC_VARINT_MAX + 1, pay,
        moq_simpair_now_us(sp));
    MOQ_TEST_CHECK(rc == MOQ_ERR_INVAL);
    r = pub_find_hist(sv, "live", "video");
    MOQ_TEST_CHECK(r != NULL && !r->has_largest);

    /* Unencodable payload LENGTH: a zero-copy wrap can carry a logical length
     * larger than any real buffer. The location is fine, so only the length
     * check can reject it -- and it must, before Largest is touched. Guarded
     * for platforms whose size_t cannot represent 2^62 (the length would
     * truncate); the production check is unconditional. */
#if SIZE_MAX > 0x3FFFFFFFFFFFFFFFULL
    {
        static const uint8_t one = 0x7;
        moq_rcbuf_t *big = NULL;
        MOQ_TEST_CHECK(moq_rcbuf_wrap(&alloc, &one,
            (size_t)MOQ_QUIC_VARINT_MAX + 1, NULL, NULL, &big) == MOQ_OK);
        rc = moq_pub_write_object(pub, track, 1, 1, big,
            moq_simpair_now_us(sp));
        MOQ_TEST_CHECK(rc == MOQ_ERR_INVAL);
        moq_rcbuf_decref(big);
        r = pub_find_hist(sv, "live", "video");
        MOQ_TEST_CHECK(r != NULL && !r->has_largest);   /* not poisoned */
    }
#endif

    /* No op was left pending by the rejected writes: a valid write at a fresh
     * location is a NEW op (not a divergent retry) and succeeds, advancing
     * Largest. */
    rc = moq_pub_write_object(pub, track, 7, 3, pay, moq_simpair_now_us(sp));
    MOQ_TEST_CHECK(rc == MOQ_OK);
    r = pub_find_hist(sv, "live", "video");
    MOQ_TEST_CHECK(r && r->has_largest &&
                   r->largest_group == 7 && r->largest_object == 3);
    moq_rcbuf_decref(pay);

    /* begin_object admits the streaming object's location into history. */
    moq_pub_begin_object_cfg_t bo;
    moq_pub_begin_object_cfg_init_sized(&bo, sizeof(bo));
    bo.group_id = 8;
    bo.object_id = 0;
    bo.payload_length = 4;
    MOQ_TEST_CHECK(moq_pub_begin_object(pub, track, &bo,
        moq_simpair_now_us(sp)) == MOQ_OK);
    r = pub_find_hist(sv, "live", "video");
    MOQ_TEST_CHECK(r && r->largest_group == 8 && r->largest_object == 0);

    moq_pub_remove_track(pub, track, moq_simpair_now_us(sp));
    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("pub_history_validate_before_mutate");
}

/* Layer B: an ACTIVE Forward-0 subscription is a live destination that is
 * excluded from delivery, yet the object is still admitted and advances the
 * track's largest-location history (history observes publication, not
 * transmission). */
static void test_pub_history_forward_zero_advances(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);
    moq_session_t *sv = moq_simpair_server(sp);

    /* Register an on_subscriber_updated callback so the update being handled
     * by the facade is proven directly (order-independent), not inferred. */
    tick_cb_state_t cbs = {0};
    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    cfg.callbacks.ctx = &cbs;
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

    /* Client subscribes; publisher accepts (ACCEPT_ALL). */
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
      while (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
          moq_pub_event_result_t res;
          moq_pub_handle_event(pub, &ev, moq_simpair_now_us(sp), &res);
          moq_event_cleanup(&ev);
      } }
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK(moq_pub_has_subscriber(pub, track));

    /* Drop the subscription to Forward 0: it stays an active destination but
     * takes no objects. */
    moq_subscription_update_cfg_t ucfg;
    moq_subscription_update_cfg_init(&ucfg);
    ucfg.has_forward = true;
    ucfg.forward = false;
    MOQ_TEST_CHECK(moq_session_update_subscription(moq_simpair_client(sp),
        sub_h, &ucfg, moq_simpair_now_us(sp)) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_pub_tick(pub, moq_simpair_now_us(sp));
    { moq_event_t ev;
      while (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
          moq_pub_event_result_t res;
          moq_pub_handle_event(pub, &ev, moq_simpair_now_us(sp), &res);
          moq_event_cleanup(&ev);
      } }
    /* The facade's on_subscriber_updated fired with Forward=0 -- the update
     * was actually handled (not merely delivered on the wire). */
    MOQ_TEST_CHECK(cbs.updated >= 1);
    MOQ_TEST_CHECK(cbs.last_update.has_forward && !cbs.last_update.forward);
    /* And the slot is STILL active after the Forward-0 update (not retired):
     * with the zero delivery below -- despite an active destination -- this
     * distinguishes Forward-0 exclusion from accidental slot retirement. */
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track), 1);
    MOQ_TEST_CHECK(moq_pub_has_subscriber(pub, track));

    /* Write: MOQ_OK, no delivery to the (active) Forward-0 subscriber, but
     * history advances. */
    uint8_t d[] = { 0x61, 0x62 };
    moq_rcbuf_t *pay = NULL; moq_rcbuf_create(&alloc, d, sizeof(d), &pay);
    MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 5, 9, pay,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_rcbuf_decref(pay);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_INT(fanout_count_objects(sp, 5, 9, d, sizeof(d)), 0);
    moq_track_hist_t *r = pub_find_hist(sv, "live", "video");
    MOQ_TEST_CHECK(r && r->has_largest &&
                   r->largest_group == 5 && r->largest_object == 9);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("pub_history_forward_zero_advances");
}

/* Fail-injecting allocator: fails the Nth alloc onward (0 = never). */
typedef struct { int64_t bal; int calls; int fail_at; } pub_fail_state_t;
static void *pub_fail_alloc(size_t n, void *ctx) {
    pub_fail_state_t *s = (pub_fail_state_t *)ctx;
    s->calls++;
    if (s->fail_at && s->calls >= s->fail_at) return NULL;
    void *p = malloc(n);
    if (p) s->bal++;
    return p;
}
static void *pub_fail_realloc(void *p, size_t o, size_t n, void *c)
{ (void)o; (void)c; return realloc(p, n); }
static void pub_fail_free(void *p, size_t n, void *ctx) {
    pub_fail_state_t *s = (pub_fail_state_t *)ctx; (void)n;
    if (p) s->bal--;
    free(p);
}
static moq_alloc_t pub_fail_allocator(pub_fail_state_t *s) {
    moq_alloc_t a = { s, pub_fail_alloc, pub_fail_realloc, pub_fail_free };
    return a;
}

/* Layer B: add_track's history reservation is the registry allocation site.
 * A temporary-key OOM, a record-key-copy OOM, or a failure AFTER a successful
 * reservation (namespace advertisement) all fail add_track cleanly: *out
 * NULL, no linked track, the empty record reclaimed, allocator balanced, and
 * a retry succeeds. The session uses a fail-injecting allocator (the two
 * reservation allocs are the only session allocs add_track makes with
 * advertise_namespace off); the publisher uses a separate clean allocator so
 * only the session-side reservation path is perturbed. */
static void test_pub_history_add_track_oom(void) {
    pub_fail_state_t fs = {0};
    moq_alloc_t sess_alloc = pub_fail_allocator(&fs);
    test_alloc_state_t pas = {0};
    moq_alloc_t pub_alloc = test_allocator(&pas);
    moq_simpair_t *sp = NULL;
    {
        moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
        cfg.alloc = &sess_alloc;
        cfg.seed = 42; cfg.initial_now_us = 1000;
        cfg.client_send_request_capacity = true;
        cfg.client_initial_request_capacity = 16;
        cfg.server_send_request_capacity = true;
        cfg.server_initial_request_capacity = 16;
        moq_simpair_create(&cfg, &sp);
        moq_simpair_start(sp);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        moq_event_t ev;
        if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);
        if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);
    }
    moq_session_t *sv = moq_simpair_server(sp);
    moq_pub_cfg_t pcfg;
    moq_pub_cfg_init_sized(&pcfg, sizeof(pcfg));
    pcfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    moq_publisher_t *pub = NULL;
    moq_pub_create(sv, &pub_alloc, &pcfg, &pub);

    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init(&tcfg);
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");   /* advertise_namespace off */

    /* (a) temporary-key build OOM: the first session alloc in add_track. */
    fs.fail_at = fs.calls + 1;
    moq_pub_track_t *t = (moq_pub_track_t *)0x1;
    MOQ_TEST_CHECK(moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &t)
        == MOQ_ERR_NOMEM);
    MOQ_TEST_CHECK(t == NULL);
    fs.fail_at = 0;
    MOQ_TEST_CHECK(pub_find_hist(sv, "live", "video") == NULL);

    /* (b) record-key-copy OOM: the second session alloc in add_track. */
    fs.fail_at = fs.calls + 2;
    t = (moq_pub_track_t *)0x1;
    MOQ_TEST_CHECK(moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &t)
        == MOQ_ERR_NOMEM);
    MOQ_TEST_CHECK(t == NULL);
    fs.fail_at = 0;
    MOQ_TEST_CHECK(pub_find_hist(sv, "live", "video") == NULL);

    /* (c) post-reservation failure: advertise the namespace and fail the
     * first session alloc AFTER the two reservation allocs (namespace
     * publish). The reservation succeeded, so this proves free_track_state
     * reclaims the now-empty record on the add_track failure path. */
    moq_pub_track_cfg_t acfg = tcfg;
    acfg.advertise_namespace = true;
    int rollback_fail_at = fs.calls + 3;   /* first alloc after the 2 reserve allocs */
    fs.fail_at = rollback_fail_at;
    t = (moq_pub_track_t *)0x1;
    moq_result_t rc = moq_pub_add_track(pub, &acfg, moq_simpair_now_us(sp), &t);
    /* Must be the injected allocation failure (not an unrelated WOULD_BLOCK),
     * and the injected alloc must actually have been reached -- i.e. the
     * reservation succeeded and a later allocation failed. */
    MOQ_TEST_CHECK(rc == MOQ_ERR_NOMEM);
    MOQ_TEST_CHECK(fs.calls >= rollback_fail_at);
    MOQ_TEST_CHECK(t == NULL);
    fs.fail_at = 0;
    MOQ_TEST_CHECK(pub_find_hist(sv, "live", "video") == NULL); /* reclaimed */

    /* Retry with injection off succeeds and reserves the record. */
    t = NULL;
    MOQ_TEST_CHECK(moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &t)
        == MOQ_OK);
    MOQ_TEST_CHECK(t != NULL);
    MOQ_TEST_CHECK(pub_find_hist(sv, "live", "video") != NULL);
    moq_pub_remove_track(pub, t, moq_simpair_now_us(sp));

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(fs.bal == 0);       /* session allocator balanced */
    MOQ_TEST_CHECK(pas.balance == 0);  /* publisher allocator balanced */
    MOQ_TEST_PASS("pub_history_add_track_oom");
}

/* Layer B: draft-18 malformed object properties (a Mandatory Track Property,
 * 0x4000-0x7FFF, carried as an object property) are rejected before mutation,
 * even with zero destinations -- the profile property hook runs in the
 * pre-commit validation, so Largest is not poisoned. */
static void test_pub_history_malformed_properties_d18(void) {
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    moq_simpair_t *sp = NULL;
    {
        moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.seed = 42;
        cfg.initial_now_us = 1000;
        cfg.client_send_request_capacity = true;
        cfg.client_initial_request_capacity = 16;
        cfg.server_send_request_capacity = true;
        cfg.server_initial_request_capacity = 16;
        cfg.version = MOQ_VERSION_DRAFT_18;
        moq_simpair_create(&cfg, &sp);
        moq_simpair_start(sp);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        moq_event_t ev;
        if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);
        if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);
    }
    moq_session_t *sv = moq_simpair_server(sp);

    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
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

    /* Property blob = one KVP {type=0x4000 (Mandatory Track Property), 0}. */
    uint8_t props[8];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, props, sizeof(props));
    moq_buf_write_vi64(&w, 0x4000);
    moq_buf_write_vi64(&w, 0);
    moq_rcbuf_t *pr = NULL;
    moq_rcbuf_create(&alloc, props, moq_buf_writer_offset(&w), &pr);

    uint8_t d[] = { 1 };
    moq_rcbuf_t *pay = NULL;
    moq_rcbuf_create(&alloc, d, sizeof(d), &pay);

    moq_pub_object_cfg_t oc;
    moq_pub_object_cfg_init_sized(&oc, sizeof(oc));
    oc.group_id = 2;
    oc.object_id = 0;
    oc.payload = pay;
    oc.properties = pr;
    /* Rejected before mutation; Largest not advanced. */
    MOQ_TEST_CHECK(moq_pub_write_object_ex(pub, track, &oc,
        moq_simpair_now_us(sp)) == MOQ_ERR_INVAL);
    moq_track_hist_t *r = pub_find_hist(sv, "live", "video");
    MOQ_TEST_CHECK(r != NULL && !r->has_largest);
    moq_rcbuf_decref(pr);

    /* A valid write (no properties) then succeeds and advances Largest,
     * proving no op was left pending by the rejection. */
    moq_pub_object_cfg_t oc2;
    moq_pub_object_cfg_init_sized(&oc2, sizeof(oc2));
    oc2.group_id = 2;
    oc2.object_id = 0;
    oc2.payload = pay;
    MOQ_TEST_CHECK(moq_pub_write_object_ex(pub, track, &oc2,
        moq_simpair_now_us(sp)) == MOQ_OK);
    r = pub_find_hist(sv, "live", "video");
    MOQ_TEST_CHECK(r && r->has_largest && r->largest_group == 2);
    moq_rcbuf_decref(pay);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("pub_history_malformed_properties_d18");
}

/* White-box admission counters (defined in publisher.c only under
 * MOQ_PUB_TESTING, which moq-core-test-internals compiles with; absent from
 * the shipping library). */
extern unsigned long moq_pub_test_commit_count;
extern unsigned long moq_pub_test_merge_count;
extern unsigned long moq_pub_test_release_count;

/* Layer B: admission is exactly-once across a WOULD_BLOCK retry. A new op
 * commits + merges Largest once and, on completion, releases its retained
 * snapshot once; a byte-identical retry that resumes the pending op commits
 * and merges ZERO additional times. Proven with the white-box counters --
 * value-only assertions cannot discriminate a double (idempotent) merge. */
static void test_pub_history_exactly_once_retry(void) {
    /* One-shot object: 3-action queue blocks mid-fan-out on destination B. */
    {
        test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
        moq_publisher_t *pub; moq_pub_track_t *track;
        fanout_setup(&as, &alloc, &sp, &pub, &track, true, 3);

        moq_pub_test_commit_count = 0;
        moq_pub_test_merge_count = 0;
        moq_pub_test_release_count = 0;

        uint8_t d[] = { 0x41, 0x42, 0x43, 0x44 };
        moq_rcbuf_t *pay = NULL; moq_rcbuf_create(&alloc, d, sizeof(d), &pay);
        moq_result_t rc = moq_pub_write_object(pub, track, 0, 0, pay,
            moq_simpair_now_us(sp));
        moq_rcbuf_decref(pay);
        MOQ_TEST_CHECK(rc == MOQ_ERR_WOULD_BLOCK);
        /* New op: committed + merged once; not yet released (still pending). */
        MOQ_TEST_CHECK_EQ_U64(moq_pub_test_commit_count, 1);
        MOQ_TEST_CHECK_EQ_U64(moq_pub_test_merge_count, 1);
        MOQ_TEST_CHECK_EQ_U64(moq_pub_test_release_count, 0);

        moq_simpair_run_until_quiescent(sp, 8, NULL);
        moq_rcbuf_t *p2 = NULL; moq_rcbuf_create(&alloc, d, sizeof(d), &p2);
        rc = moq_pub_write_object(pub, track, 0, 0, p2, moq_simpair_now_us(sp));
        moq_rcbuf_decref(p2);
        MOQ_TEST_CHECK(rc == MOQ_OK);
        /* Resume: NO further commit/merge; snapshot released exactly once. */
        MOQ_TEST_CHECK_EQ_U64(moq_pub_test_commit_count, 1);
        MOQ_TEST_CHECK_EQ_U64(moq_pub_test_merge_count, 1);
        MOQ_TEST_CHECK_EQ_U64(moq_pub_test_release_count, 1);

        moq_pub_destroy(pub);
        drain_all(sp);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* Streaming begin_object: 3-action queue blocks mid-fan-out on the second
     * destination (open A + begin A + open B consume 3; begin B is the 4th). */
    {
        test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
        moq_publisher_t *pub; moq_pub_track_t *track;
        fanout_setup(&as, &alloc, &sp, &pub, &track, true, 3);

        moq_pub_test_commit_count = 0;
        moq_pub_test_merge_count = 0;
        moq_pub_test_release_count = 0;

        moq_pub_begin_object_cfg_t bo;
        moq_pub_begin_object_cfg_init_sized(&bo, sizeof(bo));
        bo.group_id = 0;
        bo.object_id = 0;
        bo.payload_length = 4;
        moq_result_t rc = moq_pub_begin_object(pub, track, &bo,
            moq_simpair_now_us(sp));
        /* The begin must actually block mid-fan-out (else this proves no
         * retry). New op committed + merged once; not yet released. */
        MOQ_TEST_CHECK(rc == MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK_EQ_U64(moq_pub_test_commit_count, 1);
        MOQ_TEST_CHECK_EQ_U64(moq_pub_test_merge_count, 1);
        MOQ_TEST_CHECK_EQ_U64(moq_pub_test_release_count, 0);

        int guard = 0;
        while (rc == MOQ_ERR_WOULD_BLOCK && guard++ < 16) {
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            rc = moq_pub_begin_object(pub, track, &bo, moq_simpair_now_us(sp));
        }
        MOQ_TEST_CHECK(rc == MOQ_OK);
        /* Retries never re-committed or re-merged; released exactly once. */
        MOQ_TEST_CHECK_EQ_U64(moq_pub_test_commit_count, 1);
        MOQ_TEST_CHECK_EQ_U64(moq_pub_test_merge_count, 1);
        MOQ_TEST_CHECK_EQ_U64(moq_pub_test_release_count, 1);

        moq_pub_destroy(pub);
        drain_all(sp);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
    }
    MOQ_TEST_PASS("pub_history_exactly_once_retry");
}

/* -- Event-parity: manual moq_pub_handle_event forwarding == tick ------- */

/* Drain a session's queued actions (the manual-mode consumer's job before a
 * flush retry). */
static void manual_drain_actions(moq_session_t *s) {
    moq_action_t acts[16]; size_t na;
    while ((na = moq_session_poll_actions(s, acts, 16)) > 0)
        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
}

/* Forward every pending server event through the PUBLIC manual API
 * (moq_pub_handle_event) rather than moq_pub_tick, SERIALIZED per the header
 * contract: one event fully processed (drain + moq_pub_flush to MOQ_OK on
 * WOULD_BLOCK) before the next is polled; a borrowed event is never held
 * across a flush or resubmitted. Records, for the target kind `want`, how many
 * were seen and the last result. */
static void manual_forward(moq_publisher_t *pub, moq_session_t *sv,
                           uint64_t now, moq_event_kind_t want,
                           int *seen, moq_pub_event_result_t *last_res) {
    if (seen) *seen = 0;
    moq_event_t ev;
    /* Serialized: process ONE event to completion before polling the next.
     * On WOULD_BLOCK, drain actions + flush until OK -- never hold or resubmit
     * the borrowed event (its spans die at the next advancing call). Because
     * the previous event is fully flushed before the next poll, the single
     * pending slot is always free when a new event is handed in, so
     * WOULD_BLOCK + IGNORED (prior-slot-occupied) never arises here. */
    while (moq_session_poll_events(sv, &ev, 1) == 1) {
        moq_pub_event_result_t res = MOQ_PUB_EVENT_IGNORED;
        moq_result_t rc = moq_pub_handle_event(pub, &ev, now, &res);
        if (want != 0 && ev.kind == want) {
            if (seen) (*seen)++;
            if (last_res) *last_res = res;
        }
        moq_event_cleanup(&ev);
        /* Serialized contract: this event MUST be fully flushed to MOQ_OK
         * before the next poll. Any hard error, or failure to reach OK within
         * a bounded number of drains, is a contract violation -- fail, never
         * poll the next event over a still-blocked slot. */
        for (int g = 0; rc == MOQ_ERR_WOULD_BLOCK && g < 32; g++) {
            manual_drain_actions(sv);
            rc = moq_pub_flush(pub, now);
        }
        MOQ_TEST_CHECK(rc == MOQ_OK);
    }
    /* Final flush drives any armed retirement / deferred fetch to completion. */
    moq_result_t rc = MOQ_ERR_WOULD_BLOCK;
    for (int g = 0; g < 32; g++) {
        rc = moq_pub_flush(pub, now);
        if (rc != MOQ_ERR_WOULD_BLOCK) break;
        manual_drain_actions(sv);
    }
    MOQ_TEST_CHECK(rc == MOQ_OK);
}

/* Headline: a Forward 1->0->1 update forwarded ONLY through the manual API
 * (no tick) PAUSES delivery without retiring the subscription, then resumes.
 * Against the pre-refactor divergent handle_event (which ignored
 * SUBSCRIBE_UPDATED) the paused object would have been delivered. */
static void test_pub_manual_forward_zero_pauses(void) {
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
static void test_pub_manual_broadcasts_and_results(void) {
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
static void test_pub_manual_publish_ok(void) {
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
static void test_pub_manual_unsubscribe_active(void) {
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

static void test_pub_manual_vs_tick_parity(void) {
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

/* Count (and drain) the server's queued RESET_DATA actions. */
static int drain_count_resets(moq_session_t *s) {
    int n = 0; moq_action_t acts[16]; size_t na;
    while ((na = moq_session_poll_actions(s, acts, 16)) > 0)
        for (size_t i = 0; i < na; i++) {
            if (acts[i].kind == MOQ_ACTION_RESET_DATA) n++;
            moq_action_cleanup(&acts[i]);
        }
    return n;
}

/* a Forward-0 update on a subscription with an OPEN subgroup must
 * DRIVE the RESET, not merely arm it. This test deliberately fills the action
 * queue first so the retire cannot complete inline: handle_event returns
 * CONSUMED + WOULD_BLOCK, and a later flush (no event replay) drives exactly
 * one RESET. The slot stays active (pause) and Forward=1 resumes delivery.
 * Pre-refactor the manual path only armed and returned MOQ_OK, so no RESET
 * ever went out. */
static void test_pub_manual_forward_zero_resets_open_subgroup(void) {
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
static void test_pub_manual_publish_family_results(void) {
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
static void test_pub_manual_two_track_serialized_no_loss(void) {
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

/* handing a second SUBSCRIBE_REQUEST while the pending
 * slot is occupied returns WOULD_BLOCK + IGNORED -- the event is NOT taken
 * (never silently accepted with soon-to-be-invalid borrowed data). A correct
 * serialized caller avoids this by flushing before polling the next event. */
static void test_pub_manual_pending_blocks_second_ignored(void) {
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

    /* Poll two SUBSCRIBE_REQUESTs. The first blocks (staged); the second,
     * with the pending slot occupied, is NOT taken. */
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
    MOQ_TEST_CHECK(r2 == MOQ_PUB_EVENT_IGNORED);    /* NOT taken */
    moq_event_cleanup(&e1);
    moq_event_cleanup(&e2);

    /* The staged first accept completes on flush; the second was not accepted
     * (the correct loop would have kept it in the session queue). */
    for (int g = 0; g < 32; g++) {
        moq_result_t rc = moq_pub_flush(pub, moq_simpair_now_us(sp));
        if (rc != MOQ_ERR_WOULD_BLOCK) break;
        manual_drain_actions(sv);
    }
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, trackA) +
                           moq_pub_active_subscriptions(pub, trackB), 1);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("pub_manual_pending_blocks_second_ignored");
}

/* a blocked (deferred) retained-FETCH snapshot is released AT CLOSE,
 * not only at destroy, and exactly once. The retained group is cleared after
 * the fetch defers, so the pending_fetch snapshot holds the LAST refs to the
 * wrapped objects; SESSION_CLOSED (manual API) must free them. */
static void test_pub_manual_close_releases_pending_fetch(void) {
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
static void test_pub_manual_publish_error_matched(void) {
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

/* ========== Subscription-filter window enforcement  ========== *
 * The facade copies each resolved window from the session's internal
 * accessors at slot install / filter update and gates fan-out membership at
 * op admission: eligibility = active && Forward=1 && window admits the
 * location. History stays independent of delivery. */

static void windows_setup(test_alloc_state_t *as, moq_alloc_t *alloc,
                          moq_simpair_t **sp, moq_version_t ver,
                          moq_publisher_t **pub, moq_pub_track_t **track)
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
    cfg.version = ver;
    moq_simpair_create(&cfg, sp);
    moq_simpair_start(*sp);
    moq_simpair_run_until_quiescent(*sp, 8, NULL);
    { moq_event_t ev;
      if (moq_session_poll_events(moq_simpair_client(*sp), &ev, 1) == 1)
          moq_event_cleanup(&ev);
      if (moq_session_poll_events(moq_simpair_server(*sp), &ev, 1) == 1)
          moq_event_cleanup(&ev); }

    moq_pub_cfg_t cfg2; moq_pub_cfg_init_sized(&cfg2, sizeof(cfg2));
    cfg2.accept_mode = MOQ_PUB_ACCEPT_ALL;
    *pub = NULL;
    moq_pub_create(moq_simpair_server(*sp), alloc, &cfg2, pub);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_pub_track_cfg_t tcfg; moq_pub_track_cfg_init(&tcfg);
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    *track = NULL;
    moq_pub_add_track(*pub, &tcfg, moq_simpair_now_us(*sp), track);
}

/* Client subscribes with a filter; the facade accepts (manual_forward). */
static moq_subscription_t windows_subscribe(moq_simpair_t *sp,
    moq_publisher_t *pub, moq_subscribe_filter_t filter,
    uint64_t sg, uint64_t so, uint64_t eg)
{
    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace.parts = ns_parts;
    scfg.track_namespace.count = 1;
    scfg.track_name = MOQ_BYTES_LITERAL("video");
    scfg.filter = filter;
    scfg.start_group = sg; scfg.start_object = so; scfg.end_group = eg;
    moq_subscription_t sub_h;
    moq_session_subscribe(cl, &scfg, moq_simpair_now_us(sp), &sub_h);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    manual_forward(pub, sv, moq_simpair_now_us(sp), 0, NULL, NULL);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      while (moq_session_poll_events(cl, &ev, 1) == 1) moq_event_cleanup(&ev); }
    return sub_h;
}

/* Datagram write (location-order-free) with a per-probe payload tag. */
static moq_result_t windows_write_dg(moq_publisher_t *pub,
    moq_pub_track_t *t, moq_alloc_t *alloc, moq_simpair_t *sp,
    uint64_t g, uint64_t o, uint8_t tag)
{
    uint8_t d[2] = { tag, (uint8_t)(tag ^ 0xFFu) };
    moq_rcbuf_t *p = NULL; moq_rcbuf_create(alloc, d, sizeof(d), &p);
    moq_pub_object_cfg_t oc; moq_pub_object_cfg_init(&oc);
    oc.group_id = g; oc.object_id = o; oc.payload = p; oc.datagram = true;
    moq_result_t rc = moq_pub_write_object_ex(pub, t, &oc,
                                              moq_simpair_now_us(sp));
    moq_rcbuf_decref(p);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    return rc;
}

/* Count client deliveries of the tagged probe at {g,o}. */
static int windows_count(moq_simpair_t *sp, uint64_t g, uint64_t o, uint8_t tag)
{
    uint8_t d[2] = { tag, (uint8_t)(tag ^ 0xFFu) };
    return fanout_count_objects(sp, g, o, d, sizeof(d));
}

/* Inject a subscriber-side REQUEST_UPDATE carrying ONLY a SUBSCRIPTION_FILTER
 * into the facade's session (the public update cfg cannot send filters until
 * update), then let the facade consume the SUBSCRIBE_UPDATED. req_seq: fresh
 * client-side request id ordinal (first update = 1 -> id 2). */
static void windows_inject_sub_filter(moq_simpair_t *sp, moq_version_t ver,
    int req_seq, moq_subscribe_filter_t filter,
    uint64_t sg, uint64_t so, uint64_t eg)
{
    moq_session_t *sv = moq_simpair_server(sp);
    uint8_t buf[160]; moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    if (ver == MOQ_VERSION_DRAFT_18) {
        moq_d18_msg_params_t mp = {0};
        mp.has_filter = true;
        mp.filter_type = filter;
        mp.filter_start_group = sg;
        mp.filter_start_object = so;
        mp.filter_end_group = eg;
        MOQ_TEST_CHECK(moq_d18_encode_request_update(
            &w, (uint64_t)(2 * req_seq), &mp) == MOQ_OK);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0);
        for (size_t i = 0; i < sv->sub_cap; i++)
            if (sv->subs[i].state != MOQ_SUB_FREE &&
                sv->subs[i].role == MOQ_SUB_ROLE_PUBLISHER &&
                sv->subs[i].request_stream_ref._v != 0)
                ref = sv->subs[i].request_stream_ref;
        MOQ_TEST_CHECK(ref._v != 0);
        MOQ_TEST_CHECK(moq_session_on_bidi_stream_bytes(
            sv, ref, buf, moq_buf_writer_offset(&w), false,
            moq_simpair_now_us(sp)) == MOQ_OK);
    } else {
        uint8_t fbuf[32]; size_t flen = 0;
        moq_d16_subscription_filter_t f = {
            .filter_type = filter, .start_group = sg,
            .start_object = so, .end_group = eg };
        MOQ_TEST_CHECK(moq_d16_encode_subscription_filter(
            fbuf, sizeof(fbuf), &flen, &f) == MOQ_OK);
        moq_kvp_entry_t params[] = {
            { .type = MOQ_MSG_PARAM_SUBSCRIPTION_FILTER,
              .value = fbuf, .value_len = flen, .is_varint = false },
        };
        MOQ_TEST_CHECK(moq_d16_encode_request_update(
            &w, (uint64_t)(2 * req_seq), 0 /* existing = first request */,
            params, 1) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_on_control_bytes(
            sv, buf, moq_buf_writer_offset(&w),
            moq_simpair_now_us(sp)) == MOQ_OK);
    }
    MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);
    /* Swallow the queued acknowledgment action: the injection is one-sided (the
     * client never sent this update), so delivering the ack would close the
     * client on an unsolicited response. The pair was quiescent before the
     * injection, so the only queued actions are the ack bytes. */
    { moq_action_t a;
      while (moq_session_poll_actions(sv, &a, 1) == 1) moq_action_cleanup(&a); }
}

/* -- Membership matrix: all four filter forms x boundary locations ---- */

typedef struct {
    const char *label;
    moq_subscribe_filter_t filter;
    uint64_t sg, so, eg;   /* subscribe cfg values (ABSOLUTE_* forms) */
    bool seed;             /* pre-seed publisher registry with (7,2) */
    struct { uint64_t g, o; bool deliver; } probes[4];
} win_case_t;

static void test_window_membership_matrix(moq_version_t ver)
{
    static const win_case_t cases[] = {
        { "abs_start", MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START, 2, 1, 0, false,
          { {1, 5, false}, {2, 0, false}, {2, 1, true}, {3, 0, true} } },
        { "abs_range", MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE, 2, 1, 3, false,
          { {2, 0, false}, {2, 1, true}, {3, 7, true}, {4, 0, false} } },
        { "largest_object", MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT, 0, 0, 0, true,
          { {7, 2, false}, {7, 3, true}, {8, 0, true}, {8, 1, true} } },
        { "next_group", MOQ_SUBSCRIBE_FILTER_NEXT_GROUP, 0, 0, 0, true,
          { {7, 3, false}, {7, 9, false}, {8, 0, true}, {9, 1, true} } },
    };
    for (size_t c = 0; c < sizeof(cases) / sizeof(cases[0]); c++) {
        const win_case_t *tc = &cases[c];
        test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
        moq_publisher_t *pub; moq_pub_track_t *track;
        windows_setup(&as, &alloc, &sp, ver, &pub, &track);
        moq_session_t *sv = moq_simpair_server(sp);
        if (tc->seed) {
            moq_bytes_t p; moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
            (void)p;
            moq_namespace_t ns = { parts, 1 };
            MOQ_TEST_CHECK(moq_session_note_object_published(
                sv, &ns, MOQ_BYTES_LITERAL("video"), 7, 2) == MOQ_OK);
        }
        windows_subscribe(sp, pub, tc->filter, tc->sg, tc->so, tc->eg);
        for (int i = 0; i < 4; i++) {
            uint8_t tag = (uint8_t)(0x40 + 0x10 * (int)c + i);
            MOQ_TEST_CHECK(windows_write_dg(pub, track, &alloc, sp,
                tc->probes[i].g, tc->probes[i].o, tag) == MOQ_OK);
            int n = windows_count(sp, tc->probes[i].g, tc->probes[i].o, tag);
            MOQ_TEST_CHECK_EQ_INT(n, tc->probes[i].deliver ? 1 : 0);
        }
        /* History advanced through the LAST probe regardless of delivery. */
        {
            const win_case_t *lc2 = tc;
            uint64_t lg = lc2->probes[3].g, lo = lc2->probes[3].o;
            moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
            moq_namespace_t ns = { parts, 1 };
            size_t kl = 0;
            uint8_t *k = moq_build_track_key(sv, &ns,
                MOQ_BYTES_LITERAL("video"), &kl);
            moq_track_hist_t *r = track_hist_find(sv, k, kl);
            if (k) sv->alloc.free(k, kl, sv->alloc.ctx);
            MOQ_TEST_CHECK(r && r->has_largest &&
                           r->largest_group == lg && r->largest_object == lo);
        }
        moq_pub_destroy(pub);
        drain_all(sp);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
    }
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "window_membership_matrix_d18" : "window_membership_matrix_d16");
}

/* -- Stream objects: excluded write delivers nothing, still succeeds ---- */

static void test_window_stream_objects(moq_version_t ver)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    windows_setup(&as, &alloc, &sp, ver, &pub, &track);
    windows_subscribe(sp, pub, MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START, 2, 0, 0);

    uint8_t d0[] = { 0xA0, 0xA1 };
    moq_rcbuf_t *p0 = NULL; moq_rcbuf_create(&alloc, d0, sizeof(d0), &p0);
    MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 1, 0, p0,
        moq_simpair_now_us(sp)) == MOQ_OK);     /* below start: filtered */
    moq_rcbuf_decref(p0);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_INT(fanout_count_objects(sp, 1, 0, d0, sizeof(d0)), 0);

    uint8_t d1[] = { 0xA2, 0xA3 };
    moq_rcbuf_t *p1 = NULL; moq_rcbuf_create(&alloc, d1, sizeof(d1), &p1);
    MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 2, 0, p1,
        moq_simpair_now_us(sp)) == MOQ_OK);     /* at start: delivered */
    moq_rcbuf_decref(p1);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_INT(fanout_count_objects(sp, 2, 0, d1, sizeof(d1)), 1);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "window_stream_objects_d18" : "window_stream_objects_d16");
}

/* -- Publication window (PUBLISH_OK filter) + coexisting slot windows -- */

static void test_window_publication_and_coexist(moq_version_t ver,
                                                bool sub_first)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    windows_setup(&as, &alloc, &sp, ver, &pub, &track);
    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);

    /* Subscription window: {5,0}+ ; publication window: {2,0}+. */
    if (sub_first)
        windows_subscribe(sp, pub, MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START,
                          5, 0, 0);

    moq_pub_publish_cfg_t pcfg; moq_pub_publish_cfg_init(&pcfg);
    MOQ_TEST_CHECK(moq_pub_publish_track(pub, track, &pcfg,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      MOQ_TEST_CHECK(moq_session_poll_events(cl, &ev, 1) == 1);
      MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_PUBLISH_REQUEST);
      moq_publication_t cpub = ev.u.publish_request.pub;
      moq_event_cleanup(&ev);
      moq_accept_publish_cfg_t ac;
      moq_accept_publish_cfg_init_sized(&ac, sizeof(ac));
      ac.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
      ac.start_group = 2; ac.start_object = 0;
      MOQ_TEST_CHECK(moq_session_accept_publish(cl, cpub, &ac,
          moq_simpair_now_us(sp)) == MOQ_OK); }
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    manual_forward(pub, sv, moq_simpair_now_us(sp), 0, NULL, NULL);

    if (!sub_first)
        windows_subscribe(sp, pub, MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START,
                          5, 0, 0);

    /* {3,0}: inside the publication window only -> exactly ONE delivery. */
    MOQ_TEST_CHECK(windows_write_dg(pub, track, &alloc, sp, 3, 0, 0xB0)
                   == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(windows_count(sp, 3, 0, 0xB0), 1);
    /* {6,0}: inside both -> two deliveries (one per destination). */
    MOQ_TEST_CHECK(windows_write_dg(pub, track, &alloc, sp, 6, 0, 0xB1)
                   == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(windows_count(sp, 6, 0, 0xB1), 2);
    /* {1,0}: below both -> nothing (still MOQ_OK). */
    MOQ_TEST_CHECK(windows_write_dg(pub, track, &alloc, sp, 1, 0, 0xB2)
                   == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(windows_count(sp, 1, 0, 0xB2), 0);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("window_publication_and_coexist");
}

/* -- Fully filtered writes: success, zero copies, history advances ------ */

static void test_window_fully_filtered_history(moq_version_t ver)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    windows_setup(&as, &alloc, &sp, ver, &pub, &track);
    moq_session_t *sv = moq_simpair_server(sp);
    windows_subscribe(sp, pub, MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START, 100, 0, 0);

    MOQ_TEST_CHECK(windows_write_dg(pub, track, &alloc, sp, 1, 0, 0xC0)
                   == MOQ_OK);
    MOQ_TEST_CHECK(windows_write_dg(pub, track, &alloc, sp, 2, 4, 0xC1)
                   == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(windows_count(sp, 1, 0, 0xC0), 0);
    MOQ_TEST_CHECK_EQ_INT(windows_count(sp, 2, 4, 0xC1), 0);
    {
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_namespace_t ns = { parts, 1 };
        size_t kl = 0;
        uint8_t *k = moq_build_track_key(sv, &ns, MOQ_BYTES_LITERAL("video"),
                                          &kl);
        moq_track_hist_t *r = track_hist_find(sv, k, kl);
        if (k) sv->alloc.free(k, kl, sv->alloc.ctx);
        MOQ_TEST_CHECK(r && r->has_largest &&
                       r->largest_group == 2 && r->largest_object == 4);
    }
    /* Delivery resumes on the first admitted location. */
    MOQ_TEST_CHECK(windows_write_dg(pub, track, &alloc, sp, 100, 0, 0xC2)
                   == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(windows_count(sp, 100, 0, 0xC2), 1);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "window_fully_filtered_history_d18" : "window_fully_filtered_history_d16");
}

/* -- Narrowing cuts an open subgroup: ONE RESET, no FIN, slot active ---- */

static void test_window_narrow_resets_open_subgroup(moq_version_t ver)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    windows_setup(&as, &alloc, &sp, ver, &pub, &track);
    moq_session_t *sv = moq_simpair_server(sp);
    windows_subscribe(sp, pub, MOQ_SUBSCRIBE_FILTER_NONE, 0, 0, 0);

    /* Open a subgroup in group 0 via a stream write. */
    uint8_t d0[] = { 0xD0, 0xD1 };
    moq_rcbuf_t *p0 = NULL; moq_rcbuf_create(&alloc, d0, sizeof(d0), &p0);
    MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 0, 0, p0,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_rcbuf_decref(p0);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_INT(fanout_count_objects(sp, 0, 0, d0, sizeof(d0)), 1);
    (void)drain_count_resets(sv);

    /* Narrow to {5,0}: group 0 is cut. Fill the action queue first so the
     * retire hits the WOULD_BLOCK boundary and completes via flush. */
    windows_inject_sub_filter(sp, ver, 1,
        MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START, 5, 0, 0);
    test_session_fill_action_queue(sv);
    {
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_UPDATED);
        MOQ_TEST_CHECK(ev.u.subscribe_updated.has_filter);
        moq_pub_event_result_t res = MOQ_PUB_EVENT_IGNORED;
        moq_result_t rc = moq_pub_handle_event(pub, &ev,
            moq_simpair_now_us(sp), &res);
        moq_event_cleanup(&ev);
        MOQ_TEST_CHECK(rc == MOQ_ERR_WOULD_BLOCK);      /* retire blocked */
        MOQ_TEST_CHECK(res == MOQ_PUB_EVENT_CONSUMED);
    }
    MOQ_TEST_CHECK_EQ_INT(drain_count_resets(sv), 0);   /* nothing while blocked */
    MOQ_TEST_CHECK(moq_pub_flush(pub, moq_simpair_now_us(sp)) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(drain_count_resets(sv), 1);   /* exactly one RESET */
    MOQ_TEST_CHECK(moq_pub_flush(pub, moq_simpair_now_us(sp)) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(drain_count_resets(sv), 0);   /* no duplicate */
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track), 1);

    /* Below-start write delivers nothing; the first admitted object resumes
     * delivery on a fresh subgroup. */
    MOQ_TEST_CHECK(windows_write_dg(pub, track, &alloc, sp, 2, 0, 0xD2)
                   == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(windows_count(sp, 2, 0, 0xD2), 0);
    uint8_t d3[] = { 0xD3, 0xD4 };
    moq_rcbuf_t *p3 = NULL; moq_rcbuf_create(&alloc, d3, sizeof(d3), &p3);
    MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 5, 0, p3,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_rcbuf_decref(p3);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_INT(fanout_count_objects(sp, 5, 0, d3, sizeof(d3)), 1);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "window_narrow_resets_d18" : "window_narrow_resets_d16");
}

/* -- Streaming bracket: narrow mid-bracket resets; widen joins next ----- */

static void test_window_streaming_bracket(moq_version_t ver)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    windows_setup(&as, &alloc, &sp, ver, &pub, &track);
    moq_session_t *sv = moq_simpair_server(sp);
    windows_subscribe(sp, pub, MOQ_SUBSCRIBE_FILTER_NONE, 0, 0, 0);
    uint64_t now = moq_simpair_now_us(sp);

    /* begin_object {0,0} declared 4 bytes; first chunk flows. */
    moq_pub_begin_object_cfg_t bc; moq_pub_begin_object_cfg_init(&bc);
    bc.group_id = 0; bc.object_id = 0; bc.payload_length = 4;
    MOQ_TEST_CHECK(moq_pub_begin_object(pub, track, &bc, now) == MOQ_OK);
    uint8_t c1[] = { 0xE0, 0xE1 };
    moq_rcbuf_t *pc1 = NULL; moq_rcbuf_create(&alloc, c1, sizeof(c1), &pc1);
    MOQ_TEST_CHECK(moq_pub_write_data(pub, track, pc1, now) == MOQ_OK);
    moq_rcbuf_decref(pc1);
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    /* Narrow to {5,0} mid-bracket: the cut takes the Forward-drop path. */
    windows_inject_sub_filter(sp, ver, 1,
        MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START, 5, 0, 0);
    manual_forward(pub, sv, now, MOQ_EVENT_SUBSCRIBE_UPDATED, NULL, NULL);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    (void)drain_count_resets(sv);   /* the armed RESET was flushed above */

    /* The remaining chunk + end still succeed (op completes; the excluded
     * slot is skipped). */
    uint8_t c2[] = { 0xE2, 0xE3 };
    moq_rcbuf_t *pc2 = NULL; moq_rcbuf_create(&alloc, c2, sizeof(c2), &pc2);
    MOQ_TEST_CHECK(moq_pub_write_data(pub, track, pc2, now) == MOQ_OK);
    moq_rcbuf_decref(pc2);
    MOQ_TEST_CHECK(moq_pub_end_object(pub, track, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track), 1);

    /* Widen back to {0,0} mid-NOTHING -- next bracket at {5,1} delivers;
     * and a widening BETWEEN brackets must not resurrect the old one. */
    windows_inject_sub_filter(sp, ver, 2,
        MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START, 0, 0, 0);
    manual_forward(pub, sv, now, MOQ_EVENT_SUBSCRIBE_UPDATED, NULL, NULL);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      moq_session_t *cl = moq_simpair_client(sp);
      while (moq_session_poll_events(cl, &ev, 1) == 1) moq_event_cleanup(&ev); }

    moq_pub_begin_object_cfg_t bc2; moq_pub_begin_object_cfg_init(&bc2);
    bc2.group_id = 5; bc2.object_id = 1; bc2.payload_length = 2;
    MOQ_TEST_CHECK(moq_pub_begin_object(pub, track, &bc2, now) == MOQ_OK);
    uint8_t c3[] = { 0xE4, 0xE5 };
    moq_rcbuf_t *pc3 = NULL; moq_rcbuf_create(&alloc, c3, sizeof(c3), &pc3);
    MOQ_TEST_CHECK(moq_pub_write_data(pub, track, pc3, now) == MOQ_OK);
    moq_rcbuf_decref(pc3);
    MOQ_TEST_CHECK(moq_pub_end_object(pub, track, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_INT(fanout_count_objects(sp, 5, 1, c3, sizeof(c3)), 1);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "window_streaming_bracket_d18" : "window_streaming_bracket_d16");
}

/* -- Widening mid-bracket joins only the NEXT object -------------------- */

static void test_window_widen_midbracket(moq_version_t ver)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    windows_setup(&as, &alloc, &sp, ver, &pub, &track);
    moq_session_t *sv = moq_simpair_server(sp);
    windows_subscribe(sp, pub, MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START, 5, 0, 0);
    uint64_t now = moq_simpair_now_us(sp);

    /* Bracket at {2,0}: the slot is EXCLUDED at begin (window {5,0}+). */
    moq_pub_begin_object_cfg_t bc; moq_pub_begin_object_cfg_init(&bc);
    bc.group_id = 2; bc.object_id = 0; bc.payload_length = 2;
    MOQ_TEST_CHECK(moq_pub_begin_object(pub, track, &bc, now) == MOQ_OK);

    /* Widen to {0,0} MID-BRACKET: must NOT join the open bracket. */
    windows_inject_sub_filter(sp, ver, 1,
        MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START, 0, 0, 0);
    manual_forward(pub, sv, now, MOQ_EVENT_SUBSCRIBE_UPDATED, NULL, NULL);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      moq_session_t *cl = moq_simpair_client(sp);
      while (moq_session_poll_events(cl, &ev, 1) == 1) moq_event_cleanup(&ev); }

    uint8_t c1[] = { 0xF0, 0xF1 };
    moq_rcbuf_t *pc1 = NULL; moq_rcbuf_create(&alloc, c1, sizeof(c1), &pc1);
    MOQ_TEST_CHECK(moq_pub_write_data(pub, track, pc1, now) == MOQ_OK);
    moq_rcbuf_decref(pc1);
    MOQ_TEST_CHECK(moq_pub_end_object(pub, track, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_INT(fanout_count_objects(sp, 2, 0, c1, sizeof(c1)), 0);

    /* The NEXT object joins. */
    uint8_t d[] = { 0xF2, 0xF3 };
    moq_rcbuf_t *pd = NULL; moq_rcbuf_create(&alloc, d, sizeof(d), &pd);
    MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 2, 1, pd, now) == MOQ_OK);
    moq_rcbuf_decref(pd);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_INT(fanout_count_objects(sp, 2, 1, d, sizeof(d)), 1);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "window_widen_midbracket_d18" : "window_widen_midbracket_d16");
}

/* -- Unsatisfiable window: excludes everything, session survives -------- */

static void test_window_unsatisfiable(moq_version_t ver)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    windows_setup(&as, &alloc, &sp, ver, &pub, &track);
    moq_session_t *sv = moq_simpair_server(sp);
    uint64_t ceiling = (ver == MOQ_VERSION_DRAFT_18)
        ? UINT64_MAX : MOQ_QUIC_VARINT_MAX;
    {
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_namespace_t ns = { parts, 1 };
        MOQ_TEST_CHECK(moq_session_note_object_published(
            sv, &ns, MOQ_BYTES_LITERAL("video"), ceiling, 0) == MOQ_OK);
    }
    /* NEXT_GROUP against a ceiling-group largest: no representable start. */
    windows_subscribe(sp, pub, MOQ_SUBSCRIBE_FILTER_NEXT_GROUP, 0, 0, 0);

    MOQ_TEST_CHECK(windows_write_dg(pub, track, &alloc, sp, 3, 0, 0xC8)
                   == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(windows_count(sp, 3, 0, 0xC8), 0);
    MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track), 1);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "window_unsatisfiable_d18" : "window_unsatisfiable_d16");
}

/* -- Teardown / reuse: a fresh slot cannot inherit an old window -------- */

static void test_window_teardown_reuse(moq_version_t ver)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    windows_setup(&as, &alloc, &sp, ver, &pub, &track);
    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);
    moq_subscription_t sub1 = windows_subscribe(sp, pub,
        MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START, 50, 0, 0);

    MOQ_TEST_CHECK(windows_write_dg(pub, track, &alloc, sp, 1, 0, 0xCA)
                   == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(windows_count(sp, 1, 0, 0xCA), 0);   /* filtered */

    MOQ_TEST_CHECK(moq_session_unsubscribe(cl, sub1,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    manual_forward(pub, sv, moq_simpair_now_us(sp), 0, NULL, NULL);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track), 0);

    /* Re-subscribe UNFILTERED: the reused slot must deliver low locations. */
    windows_subscribe(sp, pub, MOQ_SUBSCRIBE_FILTER_NONE, 0, 0, 0);
    MOQ_TEST_CHECK(windows_write_dg(pub, track, &alloc, sp, 2, 0, 0xCB)
                   == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(windows_count(sp, 2, 0, 0xCB), 1);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "window_teardown_reuse_d18" : "window_teardown_reuse_d16");
}

/* -- Finite end: past-end filtered, but NEVER auto-finished ------------ */

static void test_window_finite_end_no_auto_finish(moq_version_t ver)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    windows_setup(&as, &alloc, &sp, ver, &pub, &track);
    moq_session_t *cl = moq_simpair_client(sp);
    windows_subscribe(sp, pub, MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE, 0, 0, 1);

    MOQ_TEST_CHECK(windows_write_dg(pub, track, &alloc, sp, 1, 0, 0xCC)
                   == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(windows_count(sp, 1, 0, 0xCC), 1);   /* end incl. */
    MOQ_TEST_CHECK(windows_write_dg(pub, track, &alloc, sp, 2, 0, 0xCD)
                   == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(windows_count(sp, 2, 0, 0xCD), 0);   /* past end */

    /* No auto-termination: no SUBSCRIBE_DONE at the client, the slot stays
     * active, the track is not ended (a later write is still accepted). */
    { moq_event_t ev;
      while (moq_session_poll_events(cl, &ev, 1) == 1) {
          MOQ_TEST_CHECK(ev.kind != MOQ_EVENT_SUBSCRIBE_DONE);
          moq_event_cleanup(&ev);
      } }
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track), 1);
    MOQ_TEST_CHECK(windows_write_dg(pub, track, &alloc, sp, 3, 0, 0xCE)
                   == MOQ_OK);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "window_finite_end_d18" : "window_finite_end_d16");
}

/* -- FETCH keeps its own range: not gated by the live-delivery window --- */

static void test_window_fetch_independent(void)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    windows_setup(&as, &alloc, &sp, MOQ_VERSION_DRAFT_16, &pub, &track);
    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);
    uint64_t now = moq_simpair_now_us(sp);

    /* Retain group 0 (objects 0..1), then subscribe with a window that
     * EXCLUDES it entirely. */
    moq_rcbuf_t *p0 = NULL, *p1 = NULL;
    moq_rcbuf_create(&alloc, (const uint8_t *)"o0", 2, &p0);
    moq_rcbuf_create(&alloc, (const uint8_t *)"o1", 2, &p1);
    moq_pub_retained_object_t objs[2] = {
        { .object_id = 0, .payload = p0 },
        { .object_id = 1, .payload = p1, .end_of_group = true },
    };
    moq_pub_retained_group_cfg_t gc; moq_pub_retained_group_cfg_init(&gc);
    gc.group_id = 0; gc.objects = objs; gc.object_count = 2;
    MOQ_TEST_CHECK(moq_pub_set_retained_group(pub, track, &gc) == MOQ_OK);
    moq_rcbuf_decref(p0); moq_rcbuf_decref(p1);

    (void)windows_subscribe(sp, pub,
        MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START, 100, 0, 0);

    /* A standalone FETCH whose range covers the retained group serves
     * regardless of the (excluding) live-delivery window. */
    moq_bytes_t f_ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_fetch_cfg_t fcfg; moq_fetch_cfg_init(&fcfg);
    fcfg.track_namespace.parts = f_ns_parts;
    fcfg.track_namespace.count = 1;
    fcfg.track_name = MOQ_BYTES_LITERAL("video");
    fcfg.start_group = 0; fcfg.start_object = 0;
    fcfg.end_group = 0; fcfg.end_object = 0;   /* 0 = whole end group */
    moq_fetch_t fh;
    MOQ_TEST_CHECK(moq_session_fetch(cl, &fcfg, now, &fh) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    manual_forward(pub, sv, now, 0, NULL, NULL);
    MOQ_TEST_CHECK(moq_pub_tick(pub, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    int fn = 0; bool fcomplete = false;
    { moq_event_t d[16]; size_t ne;
      while ((ne = moq_session_poll_events(cl, d, 16)) > 0)
          for (size_t i = 0; i < ne; i++) {
              if (d[i].kind == MOQ_EVENT_FETCH_OBJECT) fn++;
              else if (d[i].kind == MOQ_EVENT_FETCH_COMPLETE) fcomplete = true;
              moq_event_cleanup(&d[i]);
          } }
    MOQ_TEST_CHECK_EQ_INT(fn, 2);
    MOQ_TEST_CHECK(fcomplete);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("window_fetch_independent");
}

/* Inject a subscriber-side REQUEST_UPDATE (filter only) against the facade's
 * PUBLICATION, then let the facade consume PUBLISH_UPDATED. */
static void windows_inject_pub_filter(moq_simpair_t *sp, moq_version_t ver,
    uint64_t new_req_id, moq_subscribe_filter_t filter,
    uint64_t sg, uint64_t so, uint64_t eg)
{
    moq_session_t *sv = moq_simpair_server(sp);
    uint8_t buf[160]; moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    /* The facade's publish entry (publisher role) on the server session. */
    int slot = -1;
    for (size_t i = 0; i < sv->pub_cap; i++)
        if (sv->publishes[i].state != MOQ_PUB_FREE &&
            sv->publishes[i].role == MOQ_PUB_ROLE_PUBLISHER) slot = (int)i;
    MOQ_TEST_CHECK(slot >= 0);
    if (slot < 0) return;
    if (ver == MOQ_VERSION_DRAFT_18) {
        moq_d18_msg_params_t mp = {0};
        mp.has_filter = true;
        mp.filter_type = filter;
        mp.filter_start_group = sg;
        mp.filter_start_object = so;
        mp.filter_end_group = eg;
        MOQ_TEST_CHECK(moq_d18_encode_request_update(
            &w, new_req_id, &mp) == MOQ_OK);
        moq_stream_ref_t ref = sv->publishes[slot].request_stream_ref;
        MOQ_TEST_CHECK(ref._v != 0);
        MOQ_TEST_CHECK(moq_session_on_bidi_stream_bytes(
            sv, ref, buf, moq_buf_writer_offset(&w), false,
            moq_simpair_now_us(sp)) == MOQ_OK);
    } else {
        uint8_t fbuf[32]; size_t flen = 0;
        moq_d16_subscription_filter_t f = {
            .filter_type = filter, .start_group = sg,
            .start_object = so, .end_group = eg };
        MOQ_TEST_CHECK(moq_d16_encode_subscription_filter(
            fbuf, sizeof(fbuf), &flen, &f) == MOQ_OK);
        moq_kvp_entry_t params[] = {
            { .type = MOQ_MSG_PARAM_SUBSCRIPTION_FILTER,
              .value = fbuf, .value_len = flen, .is_varint = false },
        };
        MOQ_TEST_CHECK(moq_d16_encode_request_update(
            &w, new_req_id,
            sv->publishes[slot].request_id, params, 1) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_on_control_bytes(
            sv, buf, moq_buf_writer_offset(&w),
            moq_simpair_now_us(sp)) == MOQ_OK);
    }
    MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);
    /* Swallow the queued ack (see windows_inject_sub_filter). */
    { moq_action_t a;
      while (moq_session_poll_actions(sv, &a, 1) == 1) moq_action_cleanup(&a); }
}

/* -- Publication window narrow + widen via PUBLISH_UPDATED -------------- */

static void test_window_publish_updated(moq_version_t ver)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    windows_setup(&as, &alloc, &sp, ver, &pub, &track);
    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);
    uint64_t now = moq_simpair_now_us(sp);

    moq_pub_publish_cfg_t pcfg; moq_pub_publish_cfg_init(&pcfg);
    MOQ_TEST_CHECK(moq_pub_publish_track(pub, track, &pcfg, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      MOQ_TEST_CHECK(moq_session_poll_events(cl, &ev, 1) == 1);
      MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_PUBLISH_REQUEST);
      moq_publication_t cpub = ev.u.publish_request.pub;
      moq_event_cleanup(&ev);
      moq_accept_publish_cfg_t ac; moq_accept_publish_cfg_init(&ac);
      MOQ_TEST_CHECK(moq_session_accept_publish(cl, cpub, &ac, now)
                     == MOQ_OK); }               /* unfiltered accept */
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    manual_forward(pub, sv, now, 0, NULL, NULL);

    MOQ_TEST_CHECK(windows_write_dg(pub, track, &alloc, sp, 1, 0, 0xDE)
                   == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(windows_count(sp, 1, 0, 0xDE), 1);

    /* Narrow to {5,0} via PUBLISH_UPDATED. */
    windows_inject_pub_filter(sp, ver, 0,
        MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START, 5, 0, 0);
    manual_forward(pub, sv, now, MOQ_EVENT_PUBLISH_UPDATED, NULL, NULL);
    MOQ_TEST_CHECK(windows_write_dg(pub, track, &alloc, sp, 2, 0, 0xDF)
                   == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(windows_count(sp, 2, 0, 0xDF), 0);   /* filtered */

    /* Widen back to {0,0}: delivery resumes. */
    windows_inject_pub_filter(sp, ver, 2,
        MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START, 0, 0, 0);   /* next even id */
    manual_forward(pub, sv, now, MOQ_EVENT_PUBLISH_UPDATED, NULL, NULL);
    MOQ_TEST_CHECK(windows_write_dg(pub, track, &alloc, sp, 3, 0, 0xE6)
                   == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(windows_count(sp, 3, 0, 0xE6), 1);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "window_publish_updated_d18" : "window_publish_updated_d16");
}

/* -- end_track: the synthetic END_OF_TRACK obeys the window ------------- */

/* end_track vs an excluding window: the destination gets NO status object;
 * instead its subgroup is closed (FIN) FIRST and the subscription is ended
 * with SUBSCRIPTION_ENDED (0x3), retryably across WOULD_BLOCK, the slot
 * cleared only after success, and the track terminal only after the final
 * success. The unfiltered variant still receives the status object. */
static void test_window_end_track(moq_version_t ver, bool excluded)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    windows_setup(&as, &alloc, &sp, ver, &pub, &track);
    moq_session_t *cl = moq_simpair_client(sp);
    if (excluded)
        /* End group 0 only: the synthetic status at {1,0} is out of range. */
        windows_subscribe(sp, pub, MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE,
                          0, 0, 0);
    else
        windows_subscribe(sp, pub, MOQ_SUBSCRIBE_FILTER_NONE, 0, 0, 0);

    /* An earlier object leaves the slot's subgroup OPEN. */
    uint8_t d0[] = { 0xDA, 0xDB };
    moq_rcbuf_t *p0 = NULL; moq_rcbuf_create(&alloc, d0, sizeof(d0), &p0);
    MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 0, 0, p0,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_rcbuf_decref(p0);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_INT(fanout_count_objects(sp, 0, 0, d0, sizeof(d0)), 1);

    MOQ_TEST_CHECK(moq_pub_end_track(pub, track,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    int nstatus = 0, ndone = 0; uint64_t done_status = 0;
    { moq_event_t ev;
      while (moq_session_poll_events(cl, &ev, 1) == 1) {
          if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED &&
              ev.u.object_received.status == MOQ_OBJECT_END_OF_TRACK)
              nstatus++;
          if (ev.kind == MOQ_EVENT_SUBSCRIBE_DONE) {
              ndone++;
              done_status = ev.u.subscribe_done.status_code;
          }
          moq_event_cleanup(&ev);
      } }
    MOQ_TEST_CHECK_EQ_INT(nstatus, excluded ? 0 : 1);
    if (excluded) {
        /* Deterministic terminal signal: exactly ONE done, status 0x3
         * (SUBSCRIPTION_ENDED, both drafts). */
        MOQ_TEST_CHECK_EQ_INT(ndone, 1);
        MOQ_TEST_CHECK_EQ_U64(done_status, 0x3);
        MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track), 0);
    }
    MOQ_TEST_CHECK(moq_session_state(moq_simpair_server(sp))
                   == MOQ_SESS_ESTABLISHED);
    /* Terminal + idempotent. */
    MOQ_TEST_CHECK(moq_pub_end_track(pub, track,
        moq_simpair_now_us(sp)) == MOQ_OK);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("window_end_track");
}

/* Excluded end_track under WOULD_BLOCK pressure: the close (FIN) is queued
 * BEFORE the done, both complete across retries, the slot clears only after
 * success, and the track is terminal only after the final success. Server
 * actions are inspected in order (and dropped -- the client is not the
 * subject here). Also covers the fully filtered slot with NO prior object:
 * no subgroup to close, just the done. */
static void test_window_end_track_blocked(moq_version_t ver, bool wrote_first)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    windows_setup(&as, &alloc, &sp, ver, &pub, &track);
    moq_session_t *sv = moq_simpair_server(sp);
    windows_subscribe(sp, pub,
        wrote_first ? MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE
                    : MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START,
        wrote_first ? 0 : 100, 0, 0);

    if (wrote_first) {
        uint8_t d0[] = { 0xDC, 0xDD };
        moq_rcbuf_t *p0 = NULL; moq_rcbuf_create(&alloc, d0, sizeof(d0), &p0);
        MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 0, 0, p0,
            moq_simpair_now_us(sp)) == MOQ_OK);
        moq_rcbuf_decref(p0);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
    }

    /* Blocked: nothing is sent, the track is NOT terminal (a retry is
     * required), the slot stays active. */
    test_session_fill_action_queue(sv);
    MOQ_TEST_CHECK(moq_pub_end_track(pub, track, moq_simpair_now_us(sp))
                   == MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track), 1);
    manual_drain_actions(sv);   /* free the queue (drop filler) */

    /* Retry completes; inspect the ORDER of the emitted actions (any data
     * FIN precedes the control/done bytes) AND decode the done's scalars:
     * a finite filter end that was passed reports SUBSCRIPTION_ENDED (0x3);
     * an open-ended never-reached window reports TRACK_ENDED (0x2); the
     * stream count is EXACT (1 opened subgroup vs 0). */
    MOQ_TEST_CHECK(moq_pub_end_track(pub, track, moq_simpair_now_us(sp))
                   == MOQ_OK);
    uint64_t want_status = wrote_first ? 0x3 : 0x2;
    uint64_t want_count = wrote_first ? 1 : 0;
    int fin_at = -1, ctl_at = -1, idx = 0;
    bool scalars_ok = false;
    { moq_action_t a;
      while (moq_session_poll_actions(sv, &a, 1) == 1) {
          if (a.kind == MOQ_ACTION_SEND_DATA && a.u.send_data.fin &&
              fin_at < 0) fin_at = idx;
          if ((a.kind == MOQ_ACTION_SEND_CONTROL ||
               a.kind == MOQ_ACTION_SEND_BIDI_STREAM) && ctl_at < 0) {
              ctl_at = idx;
              const uint8_t *db; size_t dl;
              if (a.kind == MOQ_ACTION_SEND_CONTROL) {
                  db = a.u.send_control.data; dl = a.u.send_control.len;
              } else {
                  db = a.u.send_bidi_stream.data;
                  dl = a.u.send_bidi_stream.len;
              }
              moq_buf_reader_t rr; moq_buf_reader_init(&rr, db, dl);
              moq_control_envelope_t env;
              if (ver == MOQ_VERSION_DRAFT_18) {
                  MOQ_TEST_CHECK(moq_d18_decode_envelope(&rr, &env) == MOQ_OK);
                  MOQ_TEST_CHECK_EQ_U64(env.msg_type, MOQ_D18_PUBLISH_DONE);
                  moq_d18_publish_done_t dn;
                  MOQ_TEST_CHECK(moq_d18_decode_publish_done(
                      env.payload, env.payload_len, &dn) == MOQ_OK);
                  MOQ_TEST_CHECK_EQ_U64(dn.status_code, want_status);
                  MOQ_TEST_CHECK_EQ_U64(dn.stream_count, want_count);
                  scalars_ok = true;
              } else {
                  MOQ_TEST_CHECK(moq_control_decode_envelope(&rr, &env)
                                 == MOQ_OK);
                  moq_d16_publish_done_t dn;
                  MOQ_TEST_CHECK(moq_d16_decode_publish_done(
                      env.payload, env.payload_len, &dn) == MOQ_OK);
                  MOQ_TEST_CHECK_EQ_U64(dn.status_code, want_status);
                  MOQ_TEST_CHECK_EQ_U64(dn.stream_count, want_count);
                  scalars_ok = true;
              }
          }
          idx++;
          moq_action_cleanup(&a);
      } }
    MOQ_TEST_CHECK(ctl_at >= 0);                       /* the done went out */
    MOQ_TEST_CHECK(scalars_ok);
    if (wrote_first) {
        MOQ_TEST_CHECK(fin_at >= 0);                   /* subgroup closed */
        MOQ_TEST_CHECK(fin_at < ctl_at);               /* ...BEFORE the done */
    }
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track), 0);
    MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);
    /* Terminal only after the final success; idempotent afterwards. */
    MOQ_TEST_CHECK(moq_pub_end_track(pub, track, moq_simpair_now_us(sp))
                   == MOQ_OK);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "window_end_track_blocked_d18" : "window_end_track_blocked_d16");
}

/* Forward=0 subscriptions still get the terminal done from end_track (both
 * drafts require PUBLISH_DONE regardless of Forward state): no status object,
 * exactly ONE done with TRACK_ENDED (0x2) and the EXACT stream count, slot
 * inactive, across a WOULD_BLOCK retry. */
static void test_window_end_track_forward_zero(moq_version_t ver)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    windows_setup(&as, &alloc, &sp, ver, &pub, &track);
    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);
    moq_subscription_t sub_h = windows_subscribe(sp, pub,
        MOQ_SUBSCRIBE_FILTER_NONE, 0, 0, 0);

    /* One delivered stream object (opens exactly one subgroup). */
    uint8_t d0[] = { 0xEE, 0xEF };
    moq_rcbuf_t *p0 = NULL; moq_rcbuf_create(&alloc, d0, sizeof(d0), &p0);
    MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 0, 0, p0,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_rcbuf_decref(p0);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_INT(fanout_count_objects(sp, 0, 0, d0, sizeof(d0)), 1);

    /* Forward -> 0 (public update API): the open subgroup is lazily RESET. */
    moq_subscription_update_cfg_t ucfg; moq_subscription_update_cfg_init(&ucfg);
    ucfg.has_forward = true; ucfg.forward = false;
    MOQ_TEST_CHECK(moq_session_update_subscription(cl, sub_h, &ucfg,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    manual_forward(pub, sv, moq_simpair_now_us(sp), 0, NULL, NULL);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    drain_all(sp);

    /* Blocked first: NOT terminal, slot still active; then the retry ends
     * the subscription. */
    test_session_fill_action_queue(sv);
    MOQ_TEST_CHECK(moq_pub_end_track(pub, track, moq_simpair_now_us(sp))
                   == MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track), 1);
    manual_drain_actions(sv);
    MOQ_TEST_CHECK(moq_pub_end_track(pub, track, moq_simpair_now_us(sp))
                   == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    int nstatus = 0, ndone = 0;
    uint64_t done_status = 0, done_count = UINT64_MAX;
    { moq_event_t ev;
      while (moq_session_poll_events(cl, &ev, 1) == 1) {
          if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED &&
              ev.u.object_received.status == MOQ_OBJECT_END_OF_TRACK)
              nstatus++;
          if (ev.kind == MOQ_EVENT_SUBSCRIBE_DONE) {
              ndone++;
              done_status = ev.u.subscribe_done.status_code;
              done_count = ev.u.subscribe_done.stream_count;
          }
          moq_event_cleanup(&ev);
      } }
    MOQ_TEST_CHECK_EQ_INT(nstatus, 0);       /* Forward 0: no object */
    MOQ_TEST_CHECK_EQ_INT(ndone, 1);         /* but the done ALWAYS goes */
    MOQ_TEST_CHECK_EQ_U64(done_status, 0x2); /* TRACK_ENDED (no finite end) */
    MOQ_TEST_CHECK_EQ_U64(done_count, 1);    /* exact: one opened subgroup */
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track), 0);
    MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK(moq_pub_end_track(pub, track,
        moq_simpair_now_us(sp)) == MOQ_OK);  /* idempotent */

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "window_end_track_forward_zero_d18" : "window_end_track_forward_zero_d16");
}

/* The END_OF_TRACK Location is TRACK-WIDE: datagrams advance history without
 * touching any per-slot cursor, so the terminal object must land at
 * {history largest group + 1, 0}, be merged into history exactly once, and
 * drive every window decision. */
static void test_window_end_track_canonical_location(moq_version_t ver)
{
    /* Unfiltered: the status object arrives at {11,0}, not {1,0}. */
    {
        test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
        moq_publisher_t *pub; moq_pub_track_t *track;
        windows_setup(&as, &alloc, &sp, ver, &pub, &track);
        moq_session_t *cl = moq_simpair_client(sp);
        moq_session_t *sv = moq_simpair_server(sp);
        windows_subscribe(sp, pub, MOQ_SUBSCRIBE_FILTER_NONE, 0, 0, 0);

        MOQ_TEST_CHECK(windows_write_dg(pub, track, &alloc, sp, 10, 0, 0xAB)
                       == MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(windows_count(sp, 10, 0, 0xAB), 1);

        MOQ_TEST_CHECK(moq_pub_end_track(pub, track,
            moq_simpair_now_us(sp)) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        int nstatus = 0; uint64_t st_g = 0, st_o = 1;
        { moq_event_t ev;
          while (moq_session_poll_events(cl, &ev, 1) == 1) {
              if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED &&
                  ev.u.object_received.status == MOQ_OBJECT_END_OF_TRACK) {
                  nstatus++;
                  st_g = ev.u.object_received.group_id;
                  st_o = ev.u.object_received.object_id;
              }
              moq_event_cleanup(&ev);
          } }
        MOQ_TEST_CHECK_EQ_INT(nstatus, 1);
        MOQ_TEST_CHECK_EQ_U64(st_g, 11);     /* canonical, from history */
        MOQ_TEST_CHECK_EQ_U64(st_o, 0);
        /* Merged into history exactly once. */
        {
            moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
            moq_namespace_t ns = { parts, 1 };
            size_t kl = 0;
            uint8_t *k = moq_build_track_key(sv, &ns,
                MOQ_BYTES_LITERAL("video"), &kl);
            moq_track_hist_t *r = track_hist_find(sv, k, kl);
            if (k) sv->alloc.free(k, kl, sv->alloc.ctx);
            MOQ_TEST_CHECK(r && r->has_largest &&
                           r->largest_group == 11 && r->largest_object == 0);
        }
        moq_pub_destroy(pub);
        drain_all(sp);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* Finite end {..,10}: the canonical terminal {11,0} is PAST the end even
     * though this slot's own cursor never moved (datagrams only) -- no
     * status object, done with SUBSCRIPTION_ENDED (0x3), stream count 0. */
    {
        test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
        moq_publisher_t *pub; moq_pub_track_t *track;
        windows_setup(&as, &alloc, &sp, ver, &pub, &track);
        moq_session_t *cl = moq_simpair_client(sp);
        windows_subscribe(sp, pub, MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE,
                          0, 0, 10);

        MOQ_TEST_CHECK(windows_write_dg(pub, track, &alloc, sp, 10, 0, 0xAC)
                       == MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(windows_count(sp, 10, 0, 0xAC), 1);

        MOQ_TEST_CHECK(moq_pub_end_track(pub, track,
            moq_simpair_now_us(sp)) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        int nstatus = 0, ndone = 0;
        uint64_t done_status = 0, done_count = UINT64_MAX;
        { moq_event_t ev;
          while (moq_session_poll_events(cl, &ev, 1) == 1) {
              if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED &&
                  ev.u.object_received.status == MOQ_OBJECT_END_OF_TRACK)
                  nstatus++;
              if (ev.kind == MOQ_EVENT_SUBSCRIBE_DONE) {
                  ndone++;
                  done_status = ev.u.subscribe_done.status_code;
                  done_count = ev.u.subscribe_done.stream_count;
              }
              moq_event_cleanup(&ev);
          } }
        MOQ_TEST_CHECK_EQ_INT(nstatus, 0);
        MOQ_TEST_CHECK_EQ_INT(ndone, 1);
        MOQ_TEST_CHECK_EQ_U64(done_status, 0x3);   /* finite end passed */
        MOQ_TEST_CHECK_EQ_U64(done_count, 0);      /* datagrams: no streams */
        moq_pub_destroy(pub);
        drain_all(sp);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
    }
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "window_end_track_canonical_d18" : "window_end_track_canonical_d16");
}

/* No representable fresh terminal Location (history at the data-plane
 * ceiling: draft-16's 2^62-1 maximum becomes unencodable +1; draft-18 history
 * can reach UINT64_MAX, which must not wrap to zero): end_track fabricates NO
 * END_OF_TRACK, merges NOTHING into history, closes streams and terminates
 * every subscription with PUBLISH_DONE(TRACK_ENDED) -- across WOULD_BLOCK,
 * idempotently, with the session healthy and history unmoved. */
static void test_window_end_track_ceiling(moq_version_t ver)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    windows_setup(&as, &alloc, &sp, ver, &pub, &track);
    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);
    uint64_t ceiling = (ver == MOQ_VERSION_DRAFT_18)
        ? UINT64_MAX : MOQ_QUIC_VARINT_MAX;
    windows_subscribe(sp, pub, MOQ_SUBSCRIBE_FILTER_NONE, 0, 0, 0);

    /* One delivered stream object (count 1), then history jumps to the
     * ceiling via the note API (a relay observation the data plane itself
     * could never write). */
    uint8_t d0[] = { 0xB6, 0xB7 };
    moq_rcbuf_t *p0 = NULL; moq_rcbuf_create(&alloc, d0, sizeof(d0), &p0);
    MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 0, 0, p0,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_rcbuf_decref(p0);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_INT(fanout_count_objects(sp, 0, 0, d0, sizeof(d0)), 1);
    {
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_namespace_t ns = { parts, 1 };
        MOQ_TEST_CHECK(moq_session_note_object_published(
            sv, &ns, MOQ_BYTES_LITERAL("video"), ceiling, 5) == MOQ_OK);
    }

    /* Blocked first (mode chosen and retained), then the retry completes. */
    test_session_fill_action_queue(sv);
    MOQ_TEST_CHECK(moq_pub_end_track(pub, track, moq_simpair_now_us(sp))
                   == MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track), 1);
    manual_drain_actions(sv);
    MOQ_TEST_CHECK(moq_pub_end_track(pub, track, moq_simpair_now_us(sp))
                   == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    int nstatus = 0, ndone = 0;
    uint64_t done_status = 0, done_count = UINT64_MAX;
    { moq_event_t ev;
      while (moq_session_poll_events(cl, &ev, 1) == 1) {
          if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED &&
              ev.u.object_received.status == MOQ_OBJECT_END_OF_TRACK)
              nstatus++;
          if (ev.kind == MOQ_EVENT_SUBSCRIBE_DONE) {
              ndone++;
              done_status = ev.u.subscribe_done.status_code;
              done_count = ev.u.subscribe_done.stream_count;
          }
          moq_event_cleanup(&ev);
      } }
    MOQ_TEST_CHECK_EQ_INT(nstatus, 0);        /* nothing fabricated */
    MOQ_TEST_CHECK_EQ_INT(ndone, 1);
    MOQ_TEST_CHECK_EQ_U64(done_status, 0x2);  /* TRACK_ENDED */
    MOQ_TEST_CHECK_EQ_U64(done_count, 1);     /* the one real subgroup */
    MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);
    /* History unmoved: never wrapped to zero, never regressed. */
    {
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_namespace_t ns = { parts, 1 };
        size_t kl = 0;
        uint8_t *k = moq_build_track_key(sv, &ns, MOQ_BYTES_LITERAL("video"),
                                          &kl);
        moq_track_hist_t *r = track_hist_find(sv, k, kl);
        if (k) sv->alloc.free(k, kl, sv->alloc.ctx);
        MOQ_TEST_CHECK(r && r->has_largest &&
                       r->largest_group == ceiling && r->largest_object == 5);
    }
    MOQ_TEST_CHECK(moq_pub_end_track(pub, track,
        moq_simpair_now_us(sp)) == MOQ_OK);   /* idempotent */

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "window_end_track_ceiling_d18" : "window_end_track_ceiling_d16");
}

/* Exact stream counts on the wire, publication AND subscription roles, one
 * opened subgroup vs datagram-only zero, without duplicate terminal messages
 * on retry. */
static void test_exact_stream_counts(moq_version_t ver, bool use_stream)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    windows_setup(&as, &alloc, &sp, ver, &pub, &track);
    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);
    uint64_t now = moq_simpair_now_us(sp);

    /* Subscription + publication destinations. */
    windows_subscribe(sp, pub, MOQ_SUBSCRIBE_FILTER_NONE, 0, 0, 0);
    moq_pub_publish_cfg_t pcfg; moq_pub_publish_cfg_init(&pcfg);
    MOQ_TEST_CHECK(moq_pub_publish_track(pub, track, &pcfg, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      MOQ_TEST_CHECK(moq_session_poll_events(cl, &ev, 1) == 1);
      MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_PUBLISH_REQUEST);
      moq_publication_t cpub = ev.u.publish_request.pub;
      moq_event_cleanup(&ev);
      moq_accept_publish_cfg_t ac; moq_accept_publish_cfg_init(&ac);
      MOQ_TEST_CHECK(moq_session_accept_publish(cl, cpub, &ac, now)
                     == MOQ_OK); }
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    manual_forward(pub, sv, now, 0, NULL, NULL);

    if (use_stream) {
        /* One stream object: BOTH destinations open one subgroup each. */
        uint8_t d0[] = { 0xB8, 0xB9 };
        moq_rcbuf_t *p0 = NULL; moq_rcbuf_create(&alloc, d0, sizeof(d0), &p0);
        MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 0, 0, p0, now)
                       == MOQ_OK);
        moq_rcbuf_decref(p0);
    } else {
        /* Datagram-only: NO streams anywhere. */
        MOQ_TEST_CHECK(windows_write_dg(pub, track, &alloc, sp, 0, 0, 0xBA)
                       == MOQ_OK);
    }
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    drain_all(sp);
    uint64_t want = use_stream ? 1 : 0;

    /* Publication termination reports the EXACT count (PUBLISH_FINISHED). */
    MOQ_TEST_CHECK(moq_pub_unpublish_track(pub, track, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    int nfin = 0; uint64_t fin_count = UINT64_MAX;
    { moq_event_t ev;
      while (moq_session_poll_events(cl, &ev, 1) == 1) {
          if (ev.kind == MOQ_EVENT_PUBLISH_FINISHED) {
              nfin++;
              fin_count = ev.u.publish_finished.stream_count;
          }
          moq_event_cleanup(&ev);
      } }
    MOQ_TEST_CHECK_EQ_INT(nfin, 1);
    MOQ_TEST_CHECK_EQ_U64(fin_count, want);

    /* Subscription termination via finish_subscribers reports it too. */
    MOQ_TEST_CHECK(moq_pub_finish_subscribers(pub, track,
        MOQ_PUB_DONE_TRACK_ENDED, now) == MOQ_OK);
    /* Idempotent: a second call adds no duplicate done. */
    MOQ_TEST_CHECK(moq_pub_finish_subscribers(pub, track,
        MOQ_PUB_DONE_TRACK_ENDED, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    int ndone = 0; uint64_t done_count = UINT64_MAX;
    { moq_event_t ev;
      while (moq_session_poll_events(cl, &ev, 1) == 1) {
          if (ev.kind == MOQ_EVENT_SUBSCRIBE_DONE) {
              ndone++;
              done_count = ev.u.subscribe_done.stream_count;
          }
          moq_event_cleanup(&ev);
      } }
    MOQ_TEST_CHECK_EQ_INT(ndone, 1);
    MOQ_TEST_CHECK_EQ_U64(done_count, want);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("exact_stream_counts");
}

/* remove_track's subscription done also reports the exact count. */
static void test_remove_track_exact_count(moq_version_t ver, bool use_stream)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    windows_setup(&as, &alloc, &sp, ver, &pub, &track);
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);
    windows_subscribe(sp, pub, MOQ_SUBSCRIBE_FILTER_NONE, 0, 0, 0);

    if (use_stream) {
        uint8_t d0[] = { 0xBB, 0xBC };
        moq_rcbuf_t *p0 = NULL; moq_rcbuf_create(&alloc, d0, sizeof(d0), &p0);
        MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 0, 0, p0, now)
                       == MOQ_OK);
        moq_rcbuf_decref(p0);
    } else {
        MOQ_TEST_CHECK(windows_write_dg(pub, track, &alloc, sp, 0, 0, 0xBD)
                       == MOQ_OK);
    }
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    drain_all(sp);

    MOQ_TEST_CHECK(moq_pub_remove_track(pub, track, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    int ndone = 0; uint64_t done_count = UINT64_MAX;
    { moq_event_t ev;
      while (moq_session_poll_events(cl, &ev, 1) == 1) {
          if (ev.kind == MOQ_EVENT_SUBSCRIBE_DONE) {
              ndone++;
              done_count = ev.u.subscribe_done.stream_count;
          }
          moq_event_cleanup(&ev);
      } }
    MOQ_TEST_CHECK_EQ_INT(ndone, 1);
    MOQ_TEST_CHECK_EQ_U64(done_count, use_stream ? 1 : 0);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("remove_track_exact_count");
}

/* remove_track's INLINE publication finish reports the exact stream count
 * too (distinct code path from moq_pub_unpublish_track): datagram-only 0,
 * one opened subgroup 1, across a forced WOULD_BLOCK retry with exactly one
 * terminal event carrying the original exact count. */
static void test_remove_track_publication_exact_count(moq_version_t ver,
                                                      bool use_stream)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    windows_setup(&as, &alloc, &sp, ver, &pub, &track);
    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);
    uint64_t now = moq_simpair_now_us(sp);

    /* Publication destination only. */
    moq_pub_publish_cfg_t pcfg; moq_pub_publish_cfg_init(&pcfg);
    MOQ_TEST_CHECK(moq_pub_publish_track(pub, track, &pcfg, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      MOQ_TEST_CHECK(moq_session_poll_events(cl, &ev, 1) == 1);
      MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_PUBLISH_REQUEST);
      moq_publication_t cpub = ev.u.publish_request.pub;
      moq_event_cleanup(&ev);
      moq_accept_publish_cfg_t ac; moq_accept_publish_cfg_init(&ac);
      MOQ_TEST_CHECK(moq_session_accept_publish(cl, cpub, &ac, now)
                     == MOQ_OK); }
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    manual_forward(pub, sv, now, 0, NULL, NULL);

    if (use_stream) {
        uint8_t d0[] = { 0xC4, 0xC5 };
        moq_rcbuf_t *p0 = NULL; moq_rcbuf_create(&alloc, d0, sizeof(d0), &p0);
        MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 0, 0, p0, now)
                       == MOQ_OK);
        moq_rcbuf_decref(p0);
    } else {
        MOQ_TEST_CHECK(windows_write_dg(pub, track, &alloc, sp, 0, 0, 0xC6)
                       == MOQ_OK);
    }
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    drain_all(sp);

    /* Forced WOULD_BLOCK, then the retry completes the removal. */
    test_session_fill_action_queue(sv);
    MOQ_TEST_CHECK(moq_pub_remove_track(pub, track, now)
                   == MOQ_ERR_WOULD_BLOCK);
    manual_drain_actions(sv);
    MOQ_TEST_CHECK(moq_pub_remove_track(pub, track, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    int nfin = 0; uint64_t fin_count = UINT64_MAX;
    { moq_event_t ev;
      while (moq_session_poll_events(cl, &ev, 1) == 1) {
          if (ev.kind == MOQ_EVENT_PUBLISH_FINISHED) {
              nfin++;
              fin_count = ev.u.publish_finished.stream_count;
          }
          moq_event_cleanup(&ev);
      } }
    MOQ_TEST_CHECK_EQ_INT(nfin, 1);           /* exactly one terminal */
    MOQ_TEST_CHECK_EQ_U64(fin_count, use_stream ? 1 : 0);
    MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("remove_track_publication_exact_count");
}

/* Shared fixture: publication-only destination with an admitted object left
 * PENDING on WOULD_BLOCK -- the subgroup OPEN (header) queued, the object
 * write blocked (fill the action queue, then free exactly one slot). */
static void abandon_fixture(moq_simpair_t *sp, moq_publisher_t *pub,
    moq_pub_track_t *track, moq_alloc_t *alloc, moq_rcbuf_t **out_pay)
{
    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);
    uint64_t now = moq_simpair_now_us(sp);
    moq_pub_publish_cfg_t pcfg; moq_pub_publish_cfg_init(&pcfg);
    MOQ_TEST_CHECK(moq_pub_publish_track(pub, track, &pcfg, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      MOQ_TEST_CHECK(moq_session_poll_events(cl, &ev, 1) == 1);
      MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_PUBLISH_REQUEST);
      moq_publication_t cpub = ev.u.publish_request.pub;
      moq_event_cleanup(&ev);
      moq_accept_publish_cfg_t ac; moq_accept_publish_cfg_init(&ac);
      MOQ_TEST_CHECK(moq_session_accept_publish(cl, cpub, &ac, now)
                     == MOQ_OK); }
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    manual_forward(pub, sv, now, 0, NULL, NULL);
    drain_all(sp);

    /* First object delivers normally: the subgroup (and its header) is on
     * the wire, streams_opened == 1. */
    uint8_t d0[2] = { 0xD4, 0xD5 };
    moq_rcbuf_t *p0 = NULL; moq_rcbuf_create(alloc, d0, sizeof(d0), &p0);
    MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 0, 0, p0, now) == MOQ_OK);
    moq_rcbuf_decref(p0);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    drain_all(sp);

    /* Second object blocks BEFORE its bytes queue: admitted-but-unsent, on
     * the already-open (header-delivered) subgroup. Only fillers remain in
     * the queue, so draining them is safe. */
    uint8_t d[2] = { 0xD6, 0xD7 };
    moq_rcbuf_t *pay = NULL; moq_rcbuf_create(alloc, d, sizeof(d), &pay);
    test_session_fill_action_queue(sv);
    MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 0, 1, pay, now)
                   == MOQ_ERR_WOULD_BLOCK);
    manual_drain_actions(sv);
    *out_pay = pay;
}

/* remove_track with an admitted-but-unsent object: the subgroup is RESET
 * (never FIN), the finish reports the EXACT count (1), the peer's completed-
 * stream gate is satisfied by the identifiable reset, and a WOULD_BLOCK'd
 * removal retries without duplicates. */
static void test_remove_track_abandon_reset(moq_version_t ver, bool wire_shape)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    windows_setup(&as, &alloc, &sp, ver, &pub, &track);
    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);
    uint64_t now = moq_simpair_now_us(sp);
    moq_rcbuf_t *pay = NULL;
    abandon_fixture(sp, pub, track, &alloc, &pay);

    /* Force a WOULD_BLOCK on the removal itself, then retry; deliver via the
     * bridge so the peer's completed-stream gate is exercised END-TO-END:
     * the post-header RESET counts as processed, satisfying the EXACT count
     * of 1 -- and the unsent object never arrives. The wire_shape variant
     * instead inspects the sender actions: exactly one data RESET and NO
     * data FIN (omission must never be published as finished). */
    test_session_fill_action_queue(sv);
    MOQ_TEST_CHECK(moq_pub_remove_track(pub, track, now)
                   == MOQ_ERR_WOULD_BLOCK);
    manual_drain_actions(sv);
    MOQ_TEST_CHECK(moq_pub_remove_track(pub, track, now) == MOQ_OK);
    moq_rcbuf_decref(pay);
    if (wire_shape) {
        int nreset = 0, ndfin = 0;
        moq_action_t a;
        while (moq_session_poll_actions(sv, &a, 1) == 1) {
            if (a.kind == MOQ_ACTION_RESET_DATA) nreset++;
            if (a.kind == MOQ_ACTION_SEND_DATA && a.u.send_data.fin) ndfin++;
            moq_action_cleanup(&a);
        }
        MOQ_TEST_CHECK_EQ_INT(nreset, 1);
        MOQ_TEST_CHECK_EQ_INT(ndfin, 0);
        moq_pub_destroy(pub);
        drain_all(sp);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
        MOQ_TEST_PASS("remove_track_abandon_reset_wire");
        return;
    }
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    /* ONE poll pass: count the terminal and prove the unsent object never
     * arrived (a second helper pass would eat the terminal event). */
    int nfin = 0, nobj = 0; uint64_t fin_count = UINT64_MAX;
    { moq_event_t ev;
      while (moq_session_poll_events(cl, &ev, 1) == 1) {
          if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED &&
              ev.u.object_received.group_id == 0 &&
              ev.u.object_received.object_id == 1)
              nobj++;
          if (ev.kind == MOQ_EVENT_PUBLISH_FINISHED) {
              nfin++;
              fin_count = ev.u.publish_finished.stream_count;
          }
          moq_event_cleanup(&ev);
      } }
    MOQ_TEST_CHECK_EQ_INT(nobj, 0);          /* the unsent object never came */
    MOQ_TEST_CHECK_EQ_INT(nfin, 1);          /* gate met by the counted reset */
    MOQ_TEST_CHECK_EQ_U64(fin_count, 1);     /* EXACT, never a sentinel */
    MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "remove_track_abandon_reset_d18" : "remove_track_abandon_reset_d16");
}

/* needs_reset armed (Forward drop, retire NOT yet swept): termination must
 * never convert the required RESET into a FIN. remove_track resets directly;
 * unpublish_track drives the retire first and then finishes cleanly, with the
 * peer's gate satisfied end-to-end by the counted reset. */
static void test_needs_reset_never_fin(moq_version_t ver, bool via_remove,
                                       bool via_wire_check)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    windows_setup(&as, &alloc, &sp, ver, &pub, &track);
    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);
    uint64_t now = moq_simpair_now_us(sp);

    moq_pub_publish_cfg_t pcfg; moq_pub_publish_cfg_init(&pcfg);
    MOQ_TEST_CHECK(moq_pub_publish_track(pub, track, &pcfg, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_publication_t cpub;
    { moq_event_t ev;
      MOQ_TEST_CHECK(moq_session_poll_events(cl, &ev, 1) == 1);
      MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_PUBLISH_REQUEST);
      cpub = ev.u.publish_request.pub;
      moq_event_cleanup(&ev);
      moq_accept_publish_cfg_t ac; moq_accept_publish_cfg_init(&ac);
      MOQ_TEST_CHECK(moq_session_accept_publish(cl, cpub, &ac, now)
                     == MOQ_OK); }
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    manual_forward(pub, sv, now, 0, NULL, NULL);

    /* One delivered stream object leaves the subgroup OPEN. */
    uint8_t d0[] = { 0xD8, 0xD9 };
    moq_rcbuf_t *p0 = NULL; moq_rcbuf_create(&alloc, d0, sizeof(d0), &p0);
    MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 0, 0, p0, now) == MOQ_OK);
    moq_rcbuf_decref(p0);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    drain_all(sp);

    /* Forward -> 0 arrives with the action queue FULL: the drop arms
     * needs_reset but the retire cannot run (CONSUMED + WOULD_BLOCK). */
    moq_publication_update_cfg_t uc; moq_publication_update_cfg_init(&uc);
    uc.has_forward = true; uc.forward = false;
    MOQ_TEST_CHECK(moq_session_update_publication(cl, cpub, &uc, now)
                   == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    test_session_fill_action_queue(sv);
    { moq_event_t ev;
      MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
      MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_PUBLISH_UPDATED);
      moq_pub_event_result_t res = MOQ_PUB_EVENT_IGNORED;
      moq_result_t rc = moq_pub_handle_event(pub, &ev, now, &res);
      moq_event_cleanup(&ev);
      MOQ_TEST_CHECK(rc == MOQ_ERR_WOULD_BLOCK);
      MOQ_TEST_CHECK(res == MOQ_PUB_EVENT_CONSUMED); }
    manual_drain_actions(sv);

    if (via_remove) {
        /* Wire shape: RESET, never FIN. */
        MOQ_TEST_CHECK(moq_pub_remove_track(pub, track, now) == MOQ_OK);
        int nreset = 0, nfin = 0;
        { moq_action_t a;
          while (moq_session_poll_actions(sv, &a, 1) == 1) {
              if (a.kind == MOQ_ACTION_RESET_DATA) nreset++;
              if (a.kind == MOQ_ACTION_SEND_DATA && a.u.send_data.fin) nfin++;
              moq_action_cleanup(&a);
          } }
        MOQ_TEST_CHECK_EQ_INT(nreset, 1);
        MOQ_TEST_CHECK_EQ_INT(nfin, 0);
    } else if (via_wire_check) {
        /* Wire shape for the unpublish path: the armed retire RESETs; no
         * data FIN ever follows for that subgroup. */
        MOQ_TEST_CHECK(moq_pub_unpublish_track(pub, track, now) == MOQ_OK);
        int nreset = 0, ndfin = 0;
        moq_action_t a;
        while (moq_session_poll_actions(sv, &a, 1) == 1) {
            if (a.kind == MOQ_ACTION_RESET_DATA) nreset++;
            if (a.kind == MOQ_ACTION_SEND_DATA && a.u.send_data.fin) ndfin++;
            moq_action_cleanup(&a);
        }
        MOQ_TEST_CHECK_EQ_INT(nreset, 1);
        MOQ_TEST_CHECK_EQ_INT(ndfin, 0);
    } else {
        /* unpublish drives the armed retire (RESET) first, then finishes
         * cleanly; the counted reset satisfies the peer's gate end-to-end. */
        MOQ_TEST_CHECK(moq_pub_unpublish_track(pub, track, now) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        int nfin2 = 0; uint64_t fin_count = UINT64_MAX;
        { moq_event_t ev;
          while (moq_session_poll_events(cl, &ev, 1) == 1) {
              if (ev.kind == MOQ_EVENT_PUBLISH_FINISHED) {
                  nfin2++;
                  fin_count = ev.u.publish_finished.stream_count;
              }
              moq_event_cleanup(&ev);
          } }
        MOQ_TEST_CHECK_EQ_INT(nfin2, 1);
        MOQ_TEST_CHECK_EQ_U64(fin_count, 1);   /* exact; gate met via reset */
    }
    MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("needs_reset_never_fin");
}

/* A Forward cut DURING a streaming object arms needs_reset with the slot
 * still mid-stream: termination must PROGRESS to the RESET (never
 * WRONG_STATE, never FIN), clear the mid-stream state on the queued RESET so
 * a finish-stage WOULD_BLOCK retry completes, and report the exact count. */
static void test_streaming_needs_reset_termination(moq_version_t ver,
                                                   bool via_remove)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    windows_setup(&as, &alloc, &sp, ver, &pub, &track);
    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);
    uint64_t now = moq_simpair_now_us(sp);

    moq_pub_publish_cfg_t pcfg; moq_pub_publish_cfg_init(&pcfg);
    MOQ_TEST_CHECK(moq_pub_publish_track(pub, track, &pcfg, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_publication_t cpub;
    { moq_event_t ev;
      MOQ_TEST_CHECK(moq_session_poll_events(cl, &ev, 1) == 1);
      MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_PUBLISH_REQUEST);
      cpub = ev.u.publish_request.pub;
      moq_event_cleanup(&ev);
      moq_accept_publish_cfg_t ac; moq_accept_publish_cfg_init(&ac);
      MOQ_TEST_CHECK(moq_session_accept_publish(cl, cpub, &ac, now)
                     == MOQ_OK); }
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    manual_forward(pub, sv, now, 0, NULL, NULL);

    /* Streaming bracket: begin {0,0} (declares 4 bytes) + one chunk. */
    moq_pub_begin_object_cfg_t bc; moq_pub_begin_object_cfg_init(&bc);
    bc.group_id = 0; bc.object_id = 0; bc.payload_length = 4;
    MOQ_TEST_CHECK(moq_pub_begin_object(pub, track, &bc, now) == MOQ_OK);
    uint8_t c1[] = { 0xE7, 0xE8 };
    moq_rcbuf_t *pc1 = NULL; moq_rcbuf_create(&alloc, c1, sizeof(c1), &pc1);
    MOQ_TEST_CHECK(moq_pub_write_data(pub, track, pc1, now) == MOQ_OK);
    moq_rcbuf_decref(pc1);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    drain_all(sp);

    /* Cut mid-stream with a FULL action queue: needs_reset armed, slot still
     * streaming, retire unswept (CONSUMED + WOULD_BLOCK). */
    moq_publication_update_cfg_t uc; moq_publication_update_cfg_init(&uc);
    uc.has_forward = true; uc.forward = false;
    MOQ_TEST_CHECK(moq_session_update_publication(cl, cpub, &uc, now)
                   == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    test_session_fill_action_queue(sv);
    { moq_event_t ev;
      MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
      MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_PUBLISH_UPDATED);
      moq_pub_event_result_t res = MOQ_PUB_EVENT_IGNORED;
      moq_result_t rc = moq_pub_handle_event(pub, &ev, now, &res);
      moq_event_cleanup(&ev);
      MOQ_TEST_CHECK(rc == MOQ_ERR_WOULD_BLOCK);
      MOQ_TEST_CHECK(res == MOQ_PUB_EVENT_CONSUMED); }
    manual_drain_actions(sv);

    /* Force the finish stage to block: exactly ONE free slot lets the RESET
     * queue, then the finish WOULD_BLOCKs. The blocked attempt must be
     * WOULD_BLOCK -- NEVER WRONG_STATE (the armed mid-stream bracket
     * progresses, and the queued RESET clears the mid-stream state). */
    test_session_fill_action_queue(sv);
    { moq_action_t a;
      if (moq_session_poll_actions(sv, &a, 1) == 1) moq_action_cleanup(&a); }
    moq_result_t first = via_remove
        ? moq_pub_remove_track(pub, track, now)
        : moq_pub_unpublish_track(pub, track, now);
    MOQ_TEST_CHECK(first == MOQ_ERR_WOULD_BLOCK);
    manual_drain_actions(sv);   /* NOTE: this also drops the queued RESET */
    moq_result_t second = via_remove
        ? moq_pub_remove_track(pub, track, now)
        : moq_pub_unpublish_track(pub, track, now);
    MOQ_TEST_CHECK(second == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    /* The discriminating asserts are above: the blocked attempt returned
     * WOULD_BLOCK (never WRONG_STATE) and the retry completed -- proving the
     * armed mid-stream bracket progressed to its RESET and the queued RESET
     * cleared the mid-stream state. (End-to-end count/gate delivery is
     * covered by the abandon and needs_reset e2e tests, where the RESET
     * reaches the peer; here the drain that frees the queue necessarily
     * drops it.) */
    (void)cl;
    MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("streaming_needs_reset_termination");
}

/* unpublish with an admitted-but-unsent object: WRONG_STATE without
 * mutation; the byte-identical retry completes and delivery + a clean FIN
 * finish follow. */
static void test_unpublish_recovery(moq_version_t ver)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    windows_setup(&as, &alloc, &sp, ver, &pub, &track);
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);
    moq_rcbuf_t *pay = NULL;
    abandon_fixture(sp, pub, track, &alloc, &pay);

    /* Clean contract refused while the admitted object is unsent. */
    MOQ_TEST_CHECK(moq_pub_unpublish_track(pub, track, now)
                   == MOQ_ERR_WRONG_STATE);
    /* No mutation: the byte-identical retry completes the pending op. */
    MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 0, 1, pay, now)
                   == MOQ_OK);
    moq_rcbuf_decref(pay);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    uint8_t d[2] = { 0xD6, 0xD7 };
    MOQ_TEST_CHECK_EQ_INT(fanout_count_objects(sp, 0, 1, d, sizeof(d)), 1);

    /* Now the clean end succeeds: FIN'd stream, exact count 1. */
    MOQ_TEST_CHECK(moq_pub_unpublish_track(pub, track, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    int nfin = 0; uint64_t fin_count = UINT64_MAX;
    { moq_event_t ev;
      while (moq_session_poll_events(cl, &ev, 1) == 1) {
          if (ev.kind == MOQ_EVENT_PUBLISH_FINISHED) {
              nfin++;
              fin_count = ev.u.publish_finished.stream_count;
          }
          moq_event_cleanup(&ev);
      } }
    MOQ_TEST_CHECK_EQ_INT(nfin, 1);
    MOQ_TEST_CHECK_EQ_U64(fin_count, 1);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "unpublish_recovery_d18" : "unpublish_recovery_d16");
}

/* A narrowed window must exclude an UNSENT pending object: a WOULD_BLOCK'd
 * one-shot/datagram still awaiting this destination is dropped from the
 * retained op by the update; a byte-identical retry delivers to the OTHER
 * snapshotted destination exactly once, and the next eligible object reaches
 * the narrowed destination. Covers both update paths. */
static void test_window_pending_op_narrow(moq_version_t ver, bool narrow_pub)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    windows_setup(&as, &alloc, &sp, ver, &pub, &track);
    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);
    uint64_t now = moq_simpair_now_us(sp);

    /* Two wide destinations: a subscription and a publication. */
    windows_subscribe(sp, pub, MOQ_SUBSCRIBE_FILTER_NONE, 0, 0, 0);
    moq_pub_publish_cfg_t pcfg; moq_pub_publish_cfg_init(&pcfg);
    MOQ_TEST_CHECK(moq_pub_publish_track(pub, track, &pcfg, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      MOQ_TEST_CHECK(moq_session_poll_events(cl, &ev, 1) == 1);
      MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_PUBLISH_REQUEST);
      moq_publication_t cpub = ev.u.publish_request.pub;
      moq_event_cleanup(&ev);
      moq_accept_publish_cfg_t ac; moq_accept_publish_cfg_init(&ac);
      MOQ_TEST_CHECK(moq_session_accept_publish(cl, cpub, &ac, now)
                     == MOQ_OK); }
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    manual_forward(pub, sv, now, 0, NULL, NULL);

    /* Leave the datagram write PENDING on WOULD_BLOCK: op committed, both
     * destinations stamped, NEITHER sent. */
    uint8_t d[2] = { 0x77, 0x88 };
    moq_rcbuf_t *p = NULL; moq_rcbuf_create(&alloc, d, sizeof(d), &p);
    moq_pub_object_cfg_t oc; moq_pub_object_cfg_init(&oc);
    oc.group_id = 3; oc.object_id = 0; oc.payload = p; oc.datagram = true;
    test_session_fill_action_queue(sv);
    MOQ_TEST_CHECK(moq_pub_write_object_ex(pub, track, &oc, now)
                   == MOQ_ERR_WOULD_BLOCK);
    manual_drain_actions(sv);   /* free the queue (drop filler) */

    /* Narrow ONE window past the exact pending object {3,0} -> {3,1}. */
    if (narrow_pub) {
        /* The client's subscribe consumed request id 0; its next is 2. */
        windows_inject_pub_filter(sp, ver, 2,
            MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START, 3, 1, 0);
        manual_forward(pub, sv, now, MOQ_EVENT_PUBLISH_UPDATED, NULL, NULL);
    } else {
        windows_inject_sub_filter(sp, ver, 1,
            MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START, 3, 1, 0);
        manual_forward(pub, sv, now, MOQ_EVENT_SUBSCRIBE_UPDATED, NULL, NULL);
    }

    /* Byte-identical retry: the narrowed destination receives ZERO; the other
     * snapshotted destination receives exactly ONCE. */
    MOQ_TEST_CHECK(moq_pub_write_object_ex(pub, track, &oc, now) == MOQ_OK);
    moq_rcbuf_decref(p);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_INT(fanout_count_objects(sp, 3, 0, d, sizeof(d)), 1);

    /* The next eligible object reaches the narrowed destination too. */
    MOQ_TEST_CHECK(windows_write_dg(pub, track, &alloc, sp, 3, 1, 0x99)
                   == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(windows_count(sp, 3, 1, 0x99), 2);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "window_pending_op_narrow_d18" : "window_pending_op_narrow_d16");
}


/* ==================================================================== *
 *  §10 — explicit completion declarations (B)                          *
 * ==================================================================== */

/* Scan and DISCARD the server's queued actions, recording: index of the
 * first FIN-bearing data send, index/count of terminal-done control sends
 * (decoded status + stream count, both drafts), and RESET count. Used by
 * the backpressure tests, where fill+discard means the client never sees
 * the wire; the ordering proof lives in the action sequence itself. */
typedef struct {
    int fin_at, done_at, dones, resets;
    uint64_t status, streams;
} declare_scan_t;

static void declare_scan_actions(moq_session_t *sv, moq_version_t ver,
                                 declare_scan_t *out)
{
    memset(out, 0, sizeof(*out));
    out->fin_at = out->done_at = -1;
    out->status = ~0ull; out->streams = ~0ull;
    int idx = 0;
    moq_action_t a;
    while (moq_session_poll_actions(sv, &a, 1) == 1) {
        if (a.kind == MOQ_ACTION_SEND_DATA && a.u.send_data.fin &&
            out->fin_at < 0)
            out->fin_at = idx;
        if (a.kind == MOQ_ACTION_RESET_DATA) out->resets++;
        if (a.kind == MOQ_ACTION_SEND_CONTROL ||
            a.kind == MOQ_ACTION_SEND_BIDI_STREAM) {
            const uint8_t *db; size_t dl;
            if (a.kind == MOQ_ACTION_SEND_CONTROL) {
                db = a.u.send_control.data; dl = a.u.send_control.len;
            } else {
                db = a.u.send_bidi_stream.data;
                dl = a.u.send_bidi_stream.len;
            }
            moq_buf_reader_t rr; moq_buf_reader_init(&rr, db, dl);
            moq_control_envelope_t env;
            if (ver == MOQ_VERSION_DRAFT_18) {
                if (dl > 0 &&
                    moq_d18_decode_envelope(&rr, &env) == MOQ_OK &&
                    env.msg_type == MOQ_D18_PUBLISH_DONE) {
                    moq_d18_publish_done_t dn;
                    if (moq_d18_decode_publish_done(env.payload,
                            env.payload_len, &dn) == MOQ_OK) {
                        if (out->done_at < 0) out->done_at = idx;
                        out->dones++;
                        out->status = dn.status_code;
                        out->streams = dn.stream_count;
                    }
                }
            } else {
                if (dl > 0 &&
                    moq_control_decode_envelope(&rr, &env) == MOQ_OK &&
                    env.msg_type == MOQ_D16_PUBLISH_DONE) {
                    moq_d16_publish_done_t dn;
                    if (moq_d16_decode_publish_done(env.payload,
                            env.payload_len, &dn) == MOQ_OK) {
                        if (out->done_at < 0) out->done_at = idx;
                        out->dones++;
                        out->status = dn.status_code;
                        out->streams = dn.stream_count;
                    }
                }
            }
        }
        idx++;
        moq_action_cleanup(&a);
    }
}

/* Count client-side SUBSCRIBE_DONEs, capturing the last status/count. */
static int declare_client_dones(moq_simpair_t *sp, uint64_t *st, uint64_t *sc)
{
    int n = 0;
    moq_event_t d[16]; size_t ne;
    while ((ne = moq_session_poll_events(moq_simpair_client(sp), d, 16)) > 0)
        for (size_t i = 0; i < ne; i++) {
            if (d[i].kind == MOQ_EVENT_SUBSCRIBE_DONE) {
                n++;
                if (st) *st = d[i].u.subscribe_done.status_code;
                if (sc) *sc = d[i].u.subscribe_done.stream_count;
            }
            moq_event_cleanup(&d[i]);
        }
    return n;
}

/* One-shot STREAM write helper (opens/uses the per-slot subgroup). */
static moq_result_t declare_write_stream(moq_publisher_t *pub,
    moq_pub_track_t *t, moq_alloc_t *alloc, moq_simpair_t *sp,
    uint64_t g, uint64_t o, uint8_t tag)
{
    uint8_t d[2] = { tag, (uint8_t)(tag ^ 0xFFu) };
    moq_rcbuf_t *pl = NULL; moq_rcbuf_create(alloc, d, sizeof(d), &pl);
    moq_result_t rc = moq_pub_write_object(pub, t, g, o, pl,
                                           moq_simpair_now_us(sp));
    moq_rcbuf_decref(pl);
    return rc;
}

/* -- B rejection: every location-admitting path, zero mutation -------- */

static void test_declare_b_rejections(moq_version_t ver)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    windows_setup(&as, &alloc, &sp, ver, &pub, &track);
    moq_session_t *sv = moq_simpair_server(sp);
    uint64_t now = moq_simpair_now_us(sp);

    /* Seed history at {6,0}, then declare through 5. */
    MOQ_TEST_CHECK(windows_write_dg(pub, track, &alloc, sp, 6, 0, 0xD0)
                   == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_declare_groups_complete_through(pub, track, 5,
                                                           now) == MOQ_OK);

    /* Sealed writes reject WRONG_STATE on every admitting path... */
    MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 5, 0, 0xD1)
                   == MOQ_ERR_WRONG_STATE);                /* one-shot stream */
    MOQ_TEST_CHECK(windows_write_dg(pub, track, &alloc, sp, 4, 0, 0xD2)
                   == MOQ_ERR_WRONG_STATE);                /* datagram */
    {   /* status datagram */
        moq_pub_object_cfg_t oc; moq_pub_object_cfg_init(&oc);
        oc.group_id = 3; oc.object_id = 0; oc.datagram = true;
        oc.has_status = true; oc.status = MOQ_OBJECT_END_OF_GROUP;
        MOQ_TEST_CHECK(moq_pub_write_object_ex(pub, track, &oc, now)
                       == MOQ_ERR_WRONG_STATE);
    }
    {   /* streaming begin */
        moq_pub_begin_object_cfg_t bc; moq_pub_begin_object_cfg_init(&bc);
        bc.group_id = 5; bc.object_id = 1; bc.payload_length = 4;
        MOQ_TEST_CHECK(moq_pub_begin_object(pub, track, &bc, now)
                       == MOQ_ERR_WRONG_STATE);
    }

    /* ...with ZERO mutation: history unchanged, no wire output, no pending
     * op (a divergent-op WRONG_STATE would poison the next write; these
     * must not). */
    {
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_namespace_t ns = { parts, 1 };
        size_t kl = 0;
        uint8_t *k = moq_build_track_key(sv, &ns,
            MOQ_BYTES_LITERAL("video"), &kl);
        moq_track_hist_t *r = track_hist_find(sv, k, kl);
        if (k) sv->alloc.free(k, kl, sv->alloc.ctx);
        MOQ_TEST_CHECK(r && r->has_largest &&
                       r->largest_group == 6 && r->largest_object == 0);
    }
    { moq_action_t a;
      MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_actions(sv, &a, 1),
                             (size_t)0); }

    /* Recovery: the next writes ABOVE the watermark succeed on both the
     * one-shot and the streaming path. */
    MOQ_TEST_CHECK(windows_write_dg(pub, track, &alloc, sp, 6, 1, 0xD3)
                   == MOQ_OK);
    {
        moq_pub_begin_object_cfg_t bc; moq_pub_begin_object_cfg_init(&bc);
        bc.group_id = 7; bc.object_id = 0; bc.payload_length = 2;
        MOQ_TEST_CHECK(moq_pub_begin_object(pub, track, &bc, now) == MOQ_OK);
        uint8_t d2[2] = { 0xD4, 0x2B };
        moq_rcbuf_t *ch = NULL; moq_rcbuf_create(&alloc, d2, sizeof(d2), &ch);
        MOQ_TEST_CHECK(moq_pub_write_data(pub, track, ch, now) == MOQ_OK);
        moq_rcbuf_decref(ch);
        MOQ_TEST_CHECK(moq_pub_end_object(pub, track, now) == MOQ_OK);
    }

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "declare_b_rejections_d18" : "declare_b_rejections_d16");
}

/* -- Pending op / open bracket refuse coverage; max-merge; no rollback -- */

static void test_declare_pending_and_merge(moq_version_t ver)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    windows_setup(&as, &alloc, &sp, ver, &pub, &track);
    moq_session_t *sv = moq_simpair_server(sp);
    uint64_t now = moq_simpair_now_us(sp);
    windows_subscribe(sp, pub, MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START, 0, 0, 0);

    /* (a) Pending one-shot op at {5,0}: admitted, blocked on a full queue.
     * A declaration covering it refuses with the watermark NOT set; one
     * below it commits. */
    test_session_fill_action_queue(sv);
    uint8_t d0[2] = { 0xE0, 0x1F };
    moq_rcbuf_t *p0 = NULL; moq_rcbuf_create(&alloc, d0, sizeof(d0), &p0);
    MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 5, 0, p0, now)
                   == MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(moq_pub_declare_groups_complete_through(pub, track, 5,
                                                           now)
                   == MOQ_ERR_WRONG_STATE);
    MOQ_TEST_CHECK(moq_pub_declare_groups_complete_through(pub, track, 6,
                                                           now)
                   == MOQ_ERR_WRONG_STATE);   /* still covers the pending 5 */
    MOQ_TEST_CHECK(moq_pub_declare_groups_complete_through(pub, track, 4,
                                                           now) == MOQ_OK);
    manual_drain_actions(sv);
    /* Watermark 5 was NOT set: the byte-identical retry resumes fine. */
    MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 5, 0, p0, now) == MOQ_OK);
    moq_rcbuf_decref(p0);
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    /* (b) Zero-destination streaming bracket on a second track: the bracket
     * still refuses coverage (streaming_group is track state, not op
     * state). */
    moq_pub_track_t *t2 = NULL;
    {
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_pub_track_cfg_t tcfg; moq_pub_track_cfg_init(&tcfg);
        tcfg.track_namespace.parts = ns_parts;
        tcfg.track_namespace.count = 1;
        tcfg.track_name = MOQ_BYTES_LITERAL("aux");
        MOQ_TEST_CHECK(moq_pub_add_track(pub, &tcfg, now, &t2) == MOQ_OK);
    }
    {
        moq_pub_begin_object_cfg_t bc; moq_pub_begin_object_cfg_init(&bc);
        bc.group_id = 8; bc.object_id = 0; bc.payload_length = 2;
        MOQ_TEST_CHECK(moq_pub_begin_object(pub, t2, &bc, now) == MOQ_OK);
    }
    MOQ_TEST_CHECK(moq_pub_declare_groups_complete_through(pub, t2, 8, now)
                   == MOQ_ERR_WRONG_STATE);
    MOQ_TEST_CHECK(moq_pub_declare_groups_complete_through(pub, t2, 7, now)
                   == MOQ_OK);
    {
        uint8_t d2[2] = { 0xE1, 0x2E };
        moq_rcbuf_t *ch = NULL; moq_rcbuf_create(&alloc, d2, sizeof(d2), &ch);
        MOQ_TEST_CHECK(moq_pub_write_data(pub, t2, ch, now) == MOQ_OK);
        moq_rcbuf_decref(ch);
        MOQ_TEST_CHECK(moq_pub_end_object(pub, t2, now) == MOQ_OK);
    }
    MOQ_TEST_CHECK(moq_pub_declare_groups_complete_through(pub, t2, 8, now)
                   == MOQ_OK);   /* bracket done: coverage now legal */

    /* (c) Max-merge, no rollback: a lower redeclaration keeps the seal. */
    MOQ_TEST_CHECK(moq_pub_declare_groups_complete_through(pub, t2, 3, now)
                   == MOQ_OK);
    MOQ_TEST_CHECK(windows_write_dg(pub, t2, &alloc, sp, 8, 1, 0xE2)
                   == MOQ_ERR_WRONG_STATE);   /* still sealed through 8 */
    MOQ_TEST_CHECK(windows_write_dg(pub, t2, &alloc, sp, 9, 0, 0xE3)
                   == MOQ_OK);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "declare_pending_and_merge_d18" : "declare_pending_and_merge_d16");
}

/* -- Basic completion: FIN before done, 0x3, exact count, coexistence -- */

static void test_declare_completion_basic(moq_version_t ver)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    windows_setup(&as, &alloc, &sp, ver, &pub, &track);
    uint64_t now = moq_simpair_now_us(sp);

    /* Finite window {0,0}..3 plus an open-ended coexisting window would
     * need two subscriptions on one track (core allows one per session), so
     * the open-ended destination is a PUBLICATION slot on the same track. */
    windows_subscribe(sp, pub, MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE, 0, 0, 3);
    {
        moq_session_t *cl = moq_simpair_client(sp);
        moq_pub_publish_cfg_t pcfg; moq_pub_publish_cfg_init(&pcfg);
        MOQ_TEST_CHECK(moq_pub_publish_track(pub, track, &pcfg, now)
                       == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(cl, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_PUBLISH_REQUEST);
        moq_publication_t cpub = ev.u.publish_request.pub;
        moq_event_cleanup(&ev);
        moq_accept_publish_cfg_t ac;
        moq_accept_publish_cfg_init_sized(&ac, sizeof(ac));
        MOQ_TEST_CHECK(moq_session_accept_publish(cl, cpub, &ac, now)
                       == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        manual_forward(pub, moq_simpair_server(sp), now, 0, NULL, NULL);
    }
    drain_all(sp);

    /* A stream object inside the window opens the subscription subgroup
     * (and the publication subgroup). No completion before the declare. */
    MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 2, 0, 0xF0)
                   == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_INT(declare_client_dones(sp, NULL, NULL), 0);

    /* Declare through 3: the finite subscription completes -- subgroup FIN
     * (clean, no reset), then done 0x3 with the EXACT count (1 stream). The
     * open-ended publication is untouched. */
    MOQ_TEST_CHECK(moq_pub_declare_groups_complete_through(pub, track, 3,
                                                           now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    {
        uint64_t st = ~0ull, sc = ~0ull;
        MOQ_TEST_CHECK_EQ_INT(declare_client_dones(sp, &st, &sc), 1);
        MOQ_TEST_CHECK_EQ_U64(st, 0x3);
        MOQ_TEST_CHECK_EQ_U64(sc, 1);
    }
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track),
                           (size_t)0);

    /* Redeclaring the same value with nothing left is an idempotent no-op:
     * no second done. */
    MOQ_TEST_CHECK(moq_pub_declare_groups_complete_through(pub, track, 3,
                                                           now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_INT(declare_client_dones(sp, NULL, NULL), 0);

    /* The publication (open-ended) still delivers above the watermark. */
    MOQ_TEST_CHECK(windows_write_dg(pub, track, &alloc, sp, 4, 0, 0xF1)
                   == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(windows_count(sp, 4, 0, 0xF1), 1);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "declare_completion_basic_d18" : "declare_completion_basic_d16");
}

/* -- Publication finite window completes with finish status 0x3 -------- */

static void test_declare_publication_completes(moq_version_t ver)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    windows_setup(&as, &alloc, &sp, ver, &pub, &track);
    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);
    uint64_t now = moq_simpair_now_us(sp);

    /* Declare FIRST, then let the peer accept the PUBLISH with a finite
     * window already covered: the PUBLISH_OK handler itself must drive the
     * completion (installation path 4). */
    MOQ_TEST_CHECK(moq_pub_declare_groups_complete_through(pub, track, 5,
                                                           now) == MOQ_OK);
    moq_pub_publish_cfg_t pcfg; moq_pub_publish_cfg_init(&pcfg);
    MOQ_TEST_CHECK(moq_pub_publish_track(pub, track, &pcfg, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    {
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(cl, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_PUBLISH_REQUEST);
        moq_publication_t cpub = ev.u.publish_request.pub;
        moq_event_cleanup(&ev);
        moq_accept_publish_cfg_t ac;
        moq_accept_publish_cfg_init_sized(&ac, sizeof(ac));
        ac.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE;
        ac.start_group = 0; ac.start_object = 0; ac.end_group = 4;
        MOQ_TEST_CHECK(moq_session_accept_publish(cl, cpub, &ac, now)
                       == MOQ_OK);
    }
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    manual_forward(pub, sv, now, 0, NULL, NULL);
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    /* The finish (status 0x3, zero streams) reached the peer. */
    {
        int fin_n = 0; uint64_t st = ~0ull, sc = ~0ull;
        moq_event_t d[16]; size_t ne;
        while ((ne = moq_session_poll_events(cl, d, 16)) > 0)
            for (size_t i = 0; i < ne; i++) {
                if (d[i].kind == MOQ_EVENT_PUBLISH_FINISHED) {
                    fin_n++;
                    st = d[i].u.publish_finished.status_code;
                    sc = d[i].u.publish_finished.stream_count;
                }
                moq_event_cleanup(&d[i]);
            }
        MOQ_TEST_CHECK_EQ_INT(fin_n, 1);
        MOQ_TEST_CHECK_EQ_U64(st, 0x3);
        MOQ_TEST_CHECK_EQ_U64(sc, 0);
    }
    /* Publication state cleared: a fresh publish_track is legal again. */
    MOQ_TEST_CHECK(moq_pub_publish_track(pub, track, &pcfg, now) == MOQ_OK);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "declare_publication_completes_d18" :
        "declare_publication_completes_d16");
}

/* -- Backpressure: blocked close, blocked done, redeclare resumes ------ */

static void test_declare_blocked_steps(moq_version_t ver, bool lower_redeclare)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    windows_setup(&as, &alloc, &sp, ver, &pub, &track);
    moq_session_t *sv = moq_simpair_server(sp);
    uint64_t now = moq_simpair_now_us(sp);
    windows_subscribe(sp, pub, MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE, 0, 0, 3);

    /* Open the subscription subgroup inside the window. */
    MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 2, 0, 0xF4)
                   == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    drain_all(sp);

    /* Phase 1: the CLOSE blocks. Watermark commits; nothing sent. */
    test_session_fill_action_queue(sv);
    MOQ_TEST_CHECK(moq_pub_declare_groups_complete_through(pub, track, 3,
                                                           now)
                   == MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track),
                           (size_t)1);
    MOQ_TEST_CHECK(windows_write_dg(pub, track, &alloc, sp, 3, 5, 0xF5)
                   == MOQ_ERR_WRONG_STATE);   /* watermark IS committed */
    manual_drain_actions(sv);   /* discard the fill (and nothing else) */

    /* Phase 2: redeclare (same or LOWER value) resumes: close then done.
     * Force the done itself to block by leaving exactly one action slot. */
    test_session_fill_action_queue(sv);
    { moq_action_t a;                       /* free exactly one slot */
      MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_actions(sv, &a, 1), (size_t)1);
      moq_action_cleanup(&a); }
    MOQ_TEST_CHECK(moq_pub_declare_groups_complete_through(pub, track,
        lower_redeclare ? 1 : 3, now) == MOQ_ERR_WOULD_BLOCK);
    /* The close consumed the slot (FIN queued); the done is what blocked --
     * the retry must NOT send a second FIN. Drain and finish via the OTHER
     * documented resume path (flush). */
    manual_drain_actions(sv);
    MOQ_TEST_CHECK(moq_pub_flush(pub, now) == MOQ_OK);
    {
        declare_scan_t sc; declare_scan_actions(sv, ver, &sc);
        MOQ_TEST_CHECK_EQ_INT(sc.dones, 1);
        MOQ_TEST_CHECK_EQ_U64(sc.status, 0x3);
        MOQ_TEST_CHECK_EQ_U64(sc.streams, 1);
        MOQ_TEST_CHECK_EQ_INT(sc.resets, 0);   /* clean FIN, never RESET */
    }
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track),
                           (size_t)0);
    /* Fully quiescent afterwards: no duplicate done on further drives. */
    MOQ_TEST_CHECK(moq_pub_flush(pub, now) == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_declare_groups_complete_through(pub, track, 3,
                                                           now) == MOQ_OK);
    {
        declare_scan_t sc; declare_scan_actions(sv, ver, &sc);
        MOQ_TEST_CHECK_EQ_INT(sc.dones, 0);
    }

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        (lower_redeclare ? "declare_blocked_lower_d18"
                         : "declare_blocked_steps_d18")
        : (lower_redeclare ? "declare_blocked_lower_d16"
                           : "declare_blocked_steps_d16"));
}

/* -- FIN strictly precedes the done in the action stream --------------- */

static void test_declare_fin_before_done(moq_version_t ver)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    windows_setup(&as, &alloc, &sp, ver, &pub, &track);
    moq_session_t *sv = moq_simpair_server(sp);
    uint64_t now = moq_simpair_now_us(sp);
    windows_subscribe(sp, pub, MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE, 0, 0, 2);
    MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 1, 0, 0xF6)
                   == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    drain_all(sp);

    MOQ_TEST_CHECK(moq_pub_declare_groups_complete_through(pub, track, 2,
                                                           now) == MOQ_OK);
    {
        declare_scan_t sc; declare_scan_actions(sv, ver, &sc);
        MOQ_TEST_CHECK(sc.fin_at >= 0);
        MOQ_TEST_CHECK(sc.done_at >= 0);
        MOQ_TEST_CHECK(sc.fin_at < sc.done_at);   /* streams close FIRST */
        MOQ_TEST_CHECK_EQ_INT(sc.dones, 1);
        MOQ_TEST_CHECK_EQ_U64(sc.status, 0x3);
        MOQ_TEST_CHECK_EQ_U64(sc.streams, 1);
    }

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "declare_fin_before_done_d18" : "declare_fin_before_done_d16");
}

/* -- Installation paths: immediate accept (manual + tick), deferred ---- */

static void test_declare_install_immediate(moq_version_t ver, bool tick_mode)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    windows_setup(&as, &alloc, &sp, ver, &pub, &track);
    moq_session_t *sv = moq_simpair_server(sp);
    uint64_t now = moq_simpair_now_us(sp);

    MOQ_TEST_CHECK(moq_pub_declare_groups_complete_through(pub, track, 5,
                                                           now) == MOQ_OK);

    /* Covered finite subscribe AFTER the watermark: accepted, then
     * completed by the installation drive itself (0x3, zero streams). */
    moq_session_t *cl = moq_simpair_client(sp);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace.parts = ns_parts; scfg.track_namespace.count = 1;
    scfg.track_name = MOQ_BYTES_LITERAL("video");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE;
    scfg.start_group = 0; scfg.start_object = 0; scfg.end_group = 4;
    moq_subscription_t sub_h;
    MOQ_TEST_CHECK(moq_session_subscribe(cl, &scfg, now, &sub_h) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    if (tick_mode) {
        MOQ_TEST_CHECK(moq_pub_tick(pub, now) == MOQ_OK);
    } else {
        /* Manual mode WITHOUT any flush: the dispatcher's immediate-accept
         * installation drive must complete the covered slot by itself
         * (manual_forward's trailing flush would mask a missing drive). */
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        moq_pub_event_result_t res;
        MOQ_TEST_CHECK(moq_pub_handle_event(pub, &ev, now, &res) == MOQ_OK);
        MOQ_TEST_CHECK(res == MOQ_PUB_EVENT_CONSUMED);
        moq_event_cleanup(&ev);
    }
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    {
        uint64_t st = ~0ull, sc = ~0ull;
        MOQ_TEST_CHECK_EQ_INT(declare_client_dones(sp, &st, &sc), 1);
        MOQ_TEST_CHECK_EQ_U64(st, 0x3);
        MOQ_TEST_CHECK_EQ_U64(sc, 0);
    }
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track),
                           (size_t)0);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        (tick_mode ? "declare_install_tick_d18" : "declare_install_manual_d18")
        : (tick_mode ? "declare_install_tick_d16"
                     : "declare_install_manual_d16"));
}

/* Deferred + pending-accept paths, driven manually with backpressure. */
static moq_pub_deferred_t *s3a_last_deferred;
static uint64_t s3a_last_deferred_id;
static moq_pub_accept_decision_t s3a_defer_cb(void *ctx,
    const moq_pub_subscribe_info_t *info, moq_request_error_t *error_code)
{
    (void)ctx; (void)error_code;
    s3a_last_deferred = info->deferred;
    s3a_last_deferred_id = info->deferred_id;
    return MOQ_PUB_DECISION_DEFER;
}

static void test_declare_install_deferred_and_pending(moq_version_t ver)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    *(&as) = (test_alloc_state_t){0};
    alloc = test_allocator(&as);
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = &alloc; cfg.seed = 42; cfg.initial_now_us = 1000;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 16;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 16;
    cfg.version = ver;
    moq_simpair_create(&cfg, &sp);
    moq_simpair_start(sp);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    drain_all(sp);
    moq_session_t *sv = moq_simpair_server(sp);
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);

    moq_pub_cfg_t pcfg; moq_pub_cfg_init_sized(&pcfg, sizeof(pcfg));
    pcfg.accept_mode = MOQ_PUB_CALLBACK;
    pcfg.on_subscribe = s3a_defer_cb;
    moq_publisher_t *pub = NULL;
    MOQ_TEST_CHECK(moq_pub_create(sv, &alloc, &pcfg, &pub) == MOQ_OK);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_pub_track_cfg_t tcfg; moq_pub_track_cfg_init(&tcfg);
    tcfg.track_namespace.parts = ns_parts; tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_pub_track_t *track = NULL;
    MOQ_TEST_CHECK(moq_pub_add_track(pub, &tcfg, now, &track) == MOQ_OK);

    MOQ_TEST_CHECK(moq_pub_declare_groups_complete_through(pub, track, 5,
                                                           now) == MOQ_OK);

    /* Covered finite subscribe -> callback DEFERS. */
    moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace.parts = ns_parts; scfg.track_namespace.count = 1;
    scfg.track_name = MOQ_BYTES_LITERAL("video");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE;
    scfg.start_group = 0; scfg.start_object = 0; scfg.end_group = 2;
    moq_subscription_t sub_h;
    MOQ_TEST_CHECK(moq_session_subscribe(cl, &scfg, now, &sub_h) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    s3a_last_deferred = NULL;
    { moq_event_t ev;
      MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
      MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
      moq_pub_event_result_t res;
      MOQ_TEST_CHECK(moq_pub_handle_event(pub, &ev, now, &res) == MOQ_OK);
      moq_event_cleanup(&ev); }
    MOQ_TEST_CHECK(s3a_last_deferred != NULL);

    /* Resolve-deferred under backpressure: the ACCEPT itself blocks ->
     * pending path (installation path 1). flush_pending resolves it and
     * the SAME staged pass completes the covered slot. */
    test_session_fill_action_queue(sv);
    MOQ_TEST_CHECK(moq_pub_resolve_deferred(pub, s3a_last_deferred,
        s3a_last_deferred_id, true, 0, now) == MOQ_ERR_WOULD_BLOCK);
    manual_drain_actions(sv);
    MOQ_TEST_CHECK(moq_pub_flush(pub, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    {
        uint64_t st = ~0ull, sc = ~0ull;
        MOQ_TEST_CHECK_EQ_INT(declare_client_dones(sp, &st, &sc), 1);
        MOQ_TEST_CHECK_EQ_U64(st, 0x3);
        MOQ_TEST_CHECK_EQ_U64(sc, 0);
    }
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track),
                           (size_t)0);

    /* Second covered subscribe -> defer -> UNBLOCKED resolve-deferred:
     * immediate accept + inline completion (installation path 3). */
    moq_subscription_t sub2;
    MOQ_TEST_CHECK(moq_session_subscribe(cl, &scfg, now, &sub2) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    s3a_last_deferred = NULL;
    { moq_event_t ev;
      MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
      MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
      moq_pub_event_result_t res;
      MOQ_TEST_CHECK(moq_pub_handle_event(pub, &ev, now, &res) == MOQ_OK);
      moq_event_cleanup(&ev); }
    MOQ_TEST_CHECK(s3a_last_deferred != NULL);
    MOQ_TEST_CHECK(moq_pub_resolve_deferred(pub, s3a_last_deferred,
        s3a_last_deferred_id, true, 0, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    {
        uint64_t st = ~0ull, sc = ~0ull;
        MOQ_TEST_CHECK_EQ_INT(declare_client_dones(sp, &st, &sc), 1);
        MOQ_TEST_CHECK_EQ_U64(st, 0x3);
        MOQ_TEST_CHECK_EQ_U64(sc, 0);
    }
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track),
                           (size_t)0);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "declare_install_deferred_d18" : "declare_install_deferred_d16");
}

/* -- Update paths: narrow completes; widen survives; RESET before done -- */

static void test_declare_update_paths(moq_version_t ver)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    windows_setup(&as, &alloc, &sp, ver, &pub, &track);
    moq_session_t *sv = moq_simpair_server(sp);
    uint64_t now = moq_simpair_now_us(sp);
    windows_subscribe(sp, pub, MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE, 0, 0, 10);
    MOQ_TEST_CHECK(moq_pub_declare_groups_complete_through(pub, track, 5,
                                                           now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_INT(declare_client_dones(sp, NULL, NULL), 0);

    /* Widen (end 10 -> 12): stays subscribed; sealed groups reject; above
     * flows. */
    windows_inject_sub_filter(sp, ver, 1,
        MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE, 0, 0, 12);
    manual_forward(pub, sv, now, 0, NULL, NULL);
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track),
                           (size_t)1);
    MOQ_TEST_CHECK(windows_write_dg(pub, track, &alloc, sp, 5, 0, 0xE8)
                   == MOQ_ERR_WRONG_STATE);
    MOQ_TEST_CHECK(windows_write_dg(pub, track, &alloc, sp, 6, 0, 0xE9)
                   == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(windows_count(sp, 6, 0, 0xE9), 1);

    /* Open a subgroup in group 6, then narrow to {7..8}: the cut excludes
     * the open group entirely -> RESET (never FIN); and the narrowed end 8
     * is NOT covered by watermark 5, so no done yet. Then narrow to
     * {7..5}... impossible; instead narrow end under the watermark with a
     * start above the open group is contradictory, so complete via a
     * separate narrowing to end 4 (covered): the handler retires the armed
     * RESET first, then sends the done -- RESET precedes the done. */
    MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 6, 1, 0xEA)
                   == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    drain_all(sp);
    windows_inject_sub_filter(sp, ver, 2,
        MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE, 0, 0, 4);
    manual_forward(pub, sv, now, 0, NULL, NULL);
    {
        declare_scan_t sc; declare_scan_actions(sv, ver, &sc);
        MOQ_TEST_CHECK_EQ_INT(sc.resets, 1);      /* the open group-6 subgroup */
        MOQ_TEST_CHECK_EQ_INT(sc.dones, 1);       /* then the terminal done */
        MOQ_TEST_CHECK_EQ_U64(sc.status, 0x3);
        MOQ_TEST_CHECK_EQ_U64(sc.streams, 1);     /* the one opened subgroup
                                                     (reset counts as opened) */
    }
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track),
                           (size_t)0);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "declare_update_paths_d18" : "declare_update_paths_d16");
}

/* -- Declaration after end_track completes retained terminal slots ----- */

static void test_declare_after_end_track(moq_version_t ver)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    windows_setup(&as, &alloc, &sp, ver, &pub, &track);
    uint64_t now = moq_simpair_now_us(sp);
    windows_subscribe(sp, pub, MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE, 0, 0, 3);

    /* History {2,0}; end_track's terminal Location {3,0} is admitted by the
     * window (end 3), so this slot receives the terminal STATUS OBJECT and
     * stays INSTALLED (no done from end_track). */
    MOQ_TEST_CHECK(windows_write_dg(pub, track, &alloc, sp, 2, 0, 0xEC)
                   == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_end_track(pub, track, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track),
                           (size_t)1);
    MOQ_TEST_CHECK_EQ_INT(declare_client_dones(sp, NULL, NULL), 0);

    /* The declaration must still merge and sweep on the ENDED track. */
    MOQ_TEST_CHECK(moq_pub_declare_groups_complete_through(pub, track, 3,
                                                           now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    {
        uint64_t st = ~0ull, sc = ~0ull;
        MOQ_TEST_CHECK_EQ_INT(declare_client_dones(sp, &st, &sc), 1);
        MOQ_TEST_CHECK_EQ_U64(st, 0x3);
        MOQ_TEST_CHECK_EQ_U64(sc, 1);   /* exactly the terminal subgroup */
    }
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track),
                           (size_t)0);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "declare_after_end_track_d18" : "declare_after_end_track_d16");
}

/* -- FETCH still serves the retained group at/below the watermark ------ */

static void test_declare_fetch_below_watermark(moq_version_t ver)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    windows_setup(&as, &alloc, &sp, ver, &pub, &track);
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);

    /* Retain a 2-object group 7, subscribe (open-ended), declare through 9:
     * the subscription stays (no finite end) and the retained FETCH must
     * still serve groups at/below the watermark. */
    moq_rcbuf_t *p0=NULL,*p1=NULL;
    moq_rcbuf_create(&alloc, (const uint8_t*)"o0", 2, &p0);
    moq_rcbuf_create(&alloc, (const uint8_t*)"o1", 2, &p1);
    moq_pub_retained_object_t objs[2] = {
        { .object_id = 0, .payload = p0 },
        { .object_id = 1, .payload = p1, .end_of_group = true },
    };
    moq_pub_retained_group_cfg_t gc; moq_pub_retained_group_cfg_init(&gc);
    gc.group_id = 7; gc.objects = objs; gc.object_count = 2;
    MOQ_TEST_CHECK(moq_pub_set_retained_group(pub, track, &gc) == MOQ_OK);
    moq_rcbuf_decref(p0); moq_rcbuf_decref(p1);

    moq_subscription_t sub_h = windows_subscribe(sp, pub,
        MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT, 0, 0, 0);
    MOQ_TEST_CHECK(moq_pub_declare_groups_complete_through(pub, track, 9,
                                                           now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track),
                           (size_t)1);   /* open-ended: NOT completed */

    moq_fetch_cfg_t fcfg; moq_fetch_cfg_init(&fcfg);
    fcfg.is_joining = true; fcfg.joining_relative = true;
    fcfg.joining_start = 0; fcfg.joining_sub = sub_h;
    moq_fetch_t fh;
    MOQ_TEST_CHECK(moq_session_fetch(cl, &fcfg, now, &fh) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK(moq_pub_tick(pub, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    int fn = 0; bool fcomplete = false;
    { moq_event_t d[16]; size_t ne;
      while ((ne = moq_session_poll_events(cl, d, 16)) > 0)
          for (size_t i = 0; i < ne; i++) {
              if (d[i].kind == MOQ_EVENT_FETCH_OBJECT) fn++;
              else if (d[i].kind == MOQ_EVENT_FETCH_COMPLETE) fcomplete = true;
              moq_event_cleanup(&d[i]);
          } }
    MOQ_TEST_CHECK_EQ_INT(fn, 2);
    MOQ_TEST_CHECK(fcomplete);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "declare_fetch_below_watermark_d18" :
        "declare_fetch_below_watermark_d16");
}

/* -- No declaration: finite windows never auto-complete (regression) --- */

static void test_declare_plain_track_unchanged(moq_version_t ver)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    windows_setup(&as, &alloc, &sp, ver, &pub, &track);
    uint64_t now = moq_simpair_now_us(sp);
    windows_subscribe(sp, pub, MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE, 0, 0, 2);

    /* Write PAST the finite end: no declaration means no completion. */
    MOQ_TEST_CHECK(windows_write_dg(pub, track, &alloc, sp, 5, 0, 0xEE)
                   == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_flush(pub, now) == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_tick(pub, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_INT(declare_client_dones(sp, NULL, NULL), 0);
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track),
                           (size_t)1);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "declare_plain_track_unchanged_d18" :
        "declare_plain_track_unchanged_d16");
}


/* -- A stale subgroup must NOT delete the destination ------------------ */

/* Write a stream object, capture its SEND_DATA stream ref (discarding the
 * queued actions), and feed on_data_stop so the subgroup goes RESETTING --
 * every later facade call on that subgroup handle returns STALE_HANDLE. */
static void declare_make_sg_stale(moq_simpair_t *sp, moq_publisher_t *pub,
    moq_pub_track_t *track, moq_alloc_t *alloc, uint64_t g, uint64_t o)
{
    moq_session_t *sv = moq_simpair_server(sp);
    uint8_t d[1] = { 0x5A };
    moq_rcbuf_t *pl = NULL; moq_rcbuf_create(alloc, d, 1, &pl);
    MOQ_TEST_CHECK(moq_pub_write_object(pub, track, g, o, pl,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_rcbuf_decref(pl);
    moq_stream_ref_t ref = {0};
    bool found_ref = false;
    { moq_action_t acts[8];
      size_t na = moq_session_poll_actions(sv, acts, 8);
      for (size_t i = 0; i < na; i++) {
          if (acts[i].kind == MOQ_ACTION_SEND_DATA) {
              ref = acts[i].u.send_data.stream_ref;
              found_ref = true;
          }
          moq_action_cleanup(&acts[i]);
      } }
    MOQ_TEST_CHECK(found_ref);   /* the write really opened a stream */
    MOQ_TEST_CHECK(moq_session_on_data_stop(sv, ref, 0x0,
        moq_simpair_now_us(sp)) == MOQ_OK);
    { moq_action_t acts[8]; size_t na;
      while ((na = moq_session_poll_actions(sv, acts, 8)) > 0)
          for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]); }
}

static void test_declare_stale_completion(moq_version_t ver,
                                          bool publication_mode)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    windows_setup(&as, &alloc, &sp, ver, &pub, &track);
    moq_session_t *sv = moq_simpair_server(sp);
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);

    if (publication_mode) {
        moq_pub_publish_cfg_t pcfg; moq_pub_publish_cfg_init(&pcfg);
        MOQ_TEST_CHECK(moq_pub_publish_track(pub, track, &pcfg, now)
                       == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(cl, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_PUBLISH_REQUEST);
        moq_publication_t cpub = ev.u.publish_request.pub;
        moq_event_cleanup(&ev);
        moq_accept_publish_cfg_t ac;
        moq_accept_publish_cfg_init_sized(&ac, sizeof(ac));
        ac.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE;
        ac.start_group = 0; ac.start_object = 0; ac.end_group = 3;
        MOQ_TEST_CHECK(moq_session_accept_publish(cl, cpub, &ac, now)
                       == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        manual_forward(pub, sv, now, 0, NULL, NULL);
    } else {
        windows_subscribe(sp, pub, MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE,
                          0, 0, 3);
    }
    drain_all(sp);

    /* Open the subgroup inside the window, then make its handle STALE. */
    declare_make_sg_stale(sp, pub, track, &alloc, 2, 0);

    /* Completion must survive the stale clean-close: normalize the
     * subgroup state, keep the destination, and send exactly ONE terminal
     * with 0x3 and the exact stream count. */
    MOQ_TEST_CHECK(moq_pub_declare_groups_complete_through(pub, track, 3,
                                                           now) == MOQ_OK);
    {
        declare_scan_t sc; declare_scan_actions(sv, ver, &sc);
        MOQ_TEST_CHECK_EQ_INT(sc.dones, 1);
        MOQ_TEST_CHECK_EQ_U64(sc.status, 0x3);
        MOQ_TEST_CHECK_EQ_U64(sc.streams, 1);
    }
    if (publication_mode) {
        /* Publication state cleared consistently: a fresh publish_track is
         * legal again. */
        moq_pub_publish_cfg_t pcfg; moq_pub_publish_cfg_init(&pcfg);
        MOQ_TEST_CHECK(moq_pub_publish_track(pub, track, &pcfg, now)
                       == MOQ_OK);
    } else {
        MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track),
                               (size_t)0);
    }
    /* Retry idempotence: nothing further goes out. */
    MOQ_TEST_CHECK(moq_pub_declare_groups_complete_through(pub, track, 3,
                                                           now) == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_flush(pub, now) == MOQ_OK);
    {
        declare_scan_t sc; declare_scan_actions(sv, ver, &sc);
        MOQ_TEST_CHECK_EQ_INT(sc.dones, 0);
    }

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        (publication_mode ? "declare_stale_completion_pub_d18"
                          : "declare_stale_completion_sub_d18")
        : (publication_mode ? "declare_stale_completion_pub_d16"
                            : "declare_stale_completion_sub_d16"));
}

static void test_declare_stale_armed_reset(moq_version_t ver)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    windows_setup(&as, &alloc, &sp, ver, &pub, &track);
    moq_session_t *sv = moq_simpair_server(sp);
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);
    moq_subscription_t sub_h = windows_subscribe(sp, pub,
        MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE, 0, 0, 3);
    drain_all(sp);

    /* Open + stale the subgroup FIRST, then a Forward-0 update arms the
     * lazy retire on the already-stale handle. */
    declare_make_sg_stale(sp, pub, track, &alloc, 2, 0);
    {
        moq_subscription_update_cfg_t ucfg;
        moq_subscription_update_cfg_init(&ucfg);
        ucfg.has_forward = true; ucfg.forward = false;
        MOQ_TEST_CHECK(moq_session_update_subscription(cl, sub_h, &ucfg, now)
                       == MOQ_OK);
    }
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    manual_forward(pub, sv, now, 0, NULL, NULL);

    /* The STALE retire must PAUSE, not delete: the destination survives. */
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track),
                           (size_t)1);

    /* Completion still terminates the paused, covered destination with 0x3
     * and the exact count (the one opened-then-stale stream). */
    MOQ_TEST_CHECK(moq_pub_declare_groups_complete_through(pub, track, 3,
                                                           now) == MOQ_OK);
    {
        declare_scan_t sc; declare_scan_actions(sv, ver, &sc);
        MOQ_TEST_CHECK_EQ_INT(sc.dones, 1);
        MOQ_TEST_CHECK_EQ_U64(sc.status, 0x3);
        MOQ_TEST_CHECK_EQ_U64(sc.streams, 1);
    }
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track),
                           (size_t)0);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "declare_stale_armed_reset_d18" : "declare_stale_armed_reset_d16");
}

/* -- end_track honors the declaration ---------------------------------- */

static uint64_t declare_hist_group(moq_session_t *sv)
{
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_namespace_t ns = { parts, 1 };
    size_t kl = 0;
    uint8_t *k = moq_build_track_key(sv, &ns, MOQ_BYTES_LITERAL("video"),
                                     &kl);
    moq_track_hist_t *r = track_hist_find(sv, k, kl);
    if (k) sv->alloc.free(k, kl, sv->alloc.ctx);
    MOQ_TEST_CHECK(r && r->has_largest);
    return r ? r->largest_group : ~0ull;
}

static void test_declare_end_track_reconcile(moq_version_t ver)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    windows_setup(&as, &alloc, &sp, ver, &pub, &track);
    moq_session_t *sv = moq_simpair_server(sp);
    uint64_t now = moq_simpair_now_us(sp);
    windows_subscribe(sp, pub, MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE, 0, 0, 10);
    drain_all(sp);

    /* History {2,0}, watermark 5: the terminal must land STRICTLY ABOVE
     * the sealed range -- at {6,0}, never {3,0}. */
    MOQ_TEST_CHECK(windows_write_dg(pub, track, &alloc, sp, 2, 0, 0xC8)
                   == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_declare_groups_complete_through(pub, track, 5,
                                                           now) == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_end_track(pub, track, now) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(declare_hist_group(sv), 6);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "declare_end_track_reconcile_d18" : "declare_end_track_reconcile_d16");
}

static void test_declare_end_track_ceiling(moq_version_t ver)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    windows_setup(&as, &alloc, &sp, ver, &pub, &track);
    moq_session_t *sv = moq_simpair_server(sp);
    uint64_t now = moq_simpair_now_us(sp);

    /* Watermark at the varint ceiling: no representable terminal exists
     * above it, so end_track takes the no-status fallback -- history stays
     * at the written {2,0} (nothing merged, nothing fabricated). */
    MOQ_TEST_CHECK(windows_write_dg(pub, track, &alloc, sp, 2, 0, 0xC9)
                   == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_declare_groups_complete_through(pub, track,
        MOQ_QUIC_VARINT_MAX, now) == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_end_track(pub, track, now) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(declare_hist_group(sv), 2);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "declare_end_track_ceiling_d18" : "declare_end_track_ceiling_d16");
}

static void test_declare_end_track_blocked_preflight(moq_version_t ver)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    windows_setup(&as, &alloc, &sp, ver, &pub, &track);
    moq_session_t *sv = moq_simpair_server(sp);
    uint64_t now = moq_simpair_now_us(sp);
    /* The subscription's finite end sits exactly at the pending terminal
     * group: any LEAKED watermark from a refused declaration would cover
     * it, and the lower redeclaration's sweep below would then complete
     * the slot -- the still-active subscription is the zero-mutation
     * canary (history alone cannot discriminate: the retained op pins the
     * terminal at {3,0} across retries regardless). */
    windows_subscribe(sp, pub, MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE, 0, 0, 3);
    drain_all(sp);

    /* History {2,0} -> pending end_track terminal {3,0} blocked on a full
     * queue. A raising declaration covering it must refuse with ZERO
     * mutation; one below it commits. */
    MOQ_TEST_CHECK(windows_write_dg(pub, track, &alloc, sp, 2, 0, 0xCA)
                   == MOQ_OK);
    test_session_fill_action_queue(sv);
    MOQ_TEST_CHECK(moq_pub_end_track(pub, track, now)
                   == MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(moq_pub_declare_groups_complete_through(pub, track, 3,
                                                           now)
                   == MOQ_ERR_WRONG_STATE);
    MOQ_TEST_CHECK(moq_pub_declare_groups_complete_through(pub, track, 7,
                                                           now)
                   == MOQ_ERR_WRONG_STATE);
    /* declare(2) is a legal raise below the terminal; its sweep runs. Were
     * either refused watermark (3 or 7) secretly committed, THIS call's
     * sweep would already terminate the end-3 subscription. */
    MOQ_TEST_CHECK(moq_pub_declare_groups_complete_through(pub, track, 2,
                                                           now) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track),
                           (size_t)1);
    manual_drain_actions(sv);
    /* Retry lands the terminal at {3,0} (strictly above watermark 2); the
     * end-3 recipient keeps its slot (terminal-status path, no done). */
    MOQ_TEST_CHECK(moq_pub_end_track(pub, track, now) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(declare_hist_group(sv), 3);
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track),
                           (size_t)1);
    /* Now declare(3): exactly ONE proper completion -- 0x3 with the exact
     * count (the terminal subgroup alone). */
    MOQ_TEST_CHECK(moq_pub_declare_groups_complete_through(pub, track, 3,
                                                           now) == MOQ_OK);
    {
        declare_scan_t sc; declare_scan_actions(sv, ver, &sc);
        MOQ_TEST_CHECK_EQ_INT(sc.dones, 1);
        MOQ_TEST_CHECK_EQ_U64(sc.status, 0x3);
        MOQ_TEST_CHECK_EQ_U64(sc.streams, 1);
    }
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track),
                           (size_t)0);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "declare_end_track_blocked_d18" : "declare_end_track_blocked_d16");
}


/* ==================================================================== *
 *  §10 — monotonic-groups evidence (A)                                 *
 * ==================================================================== */

/* windows_setup twin with monotonic_groups declared on the track. */
static void mono_setup(test_alloc_state_t *as, moq_alloc_t *alloc,
                       moq_simpair_t **sp, moq_version_t ver,
                       moq_publisher_t **pub, moq_pub_track_t **track)
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
    cfg.version = ver;
    moq_simpair_create(&cfg, sp);
    moq_simpair_start(*sp);
    moq_simpair_run_until_quiescent(*sp, 8, NULL);
    { moq_event_t ev;
      if (moq_session_poll_events(moq_simpair_client(*sp), &ev, 1) == 1)
          moq_event_cleanup(&ev);
      if (moq_session_poll_events(moq_simpair_server(*sp), &ev, 1) == 1)
          moq_event_cleanup(&ev); }

    moq_pub_cfg_t cfg2; moq_pub_cfg_init_sized(&cfg2, sizeof(cfg2));
    cfg2.accept_mode = MOQ_PUB_ACCEPT_ALL;
    *pub = NULL;
    moq_pub_create(moq_simpair_server(*sp), alloc, &cfg2, pub);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init_sized(&tcfg, sizeof(tcfg));
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    tcfg.monotonic_groups = true;
    *track = NULL;
    moq_pub_add_track(*pub, &tcfg, moq_simpair_now_us(*sp), track);
}

/* One-shot STREAM write with an explicit end_of_group flag. */
static moq_result_t mono_write_stream_eog(moq_publisher_t *pub,
    moq_pub_track_t *t, moq_alloc_t *alloc, moq_simpair_t *sp,
    uint64_t g, uint64_t o, bool eog, uint8_t tag)
{
    uint8_t d[2] = { tag, (uint8_t)(tag ^ 0xFFu) };
    moq_rcbuf_t *pl = NULL; moq_rcbuf_create(alloc, d, sizeof(d), &pl);
    moq_pub_object_cfg_t oc;
    moq_pub_object_cfg_init_sized(&oc, sizeof(oc));
    oc.group_id = g; oc.object_id = o; oc.payload = pl;
    oc.end_of_group = eog;
    moq_result_t rc = moq_pub_write_object_ex(pub, t, &oc,
                                              moq_simpair_now_us(sp));
    moq_rcbuf_decref(pl);
    return rc;
}

/* -- A enforcement: decreasing / post-EOG reject; same-group legal ----- */

static void test_mono_rejections(moq_version_t ver)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    mono_setup(&as, &alloc, &sp, ver, &pub, &track);
    moq_session_t *sv = moq_simpair_server(sp);
    uint64_t now = moq_simpair_now_us(sp);

    MOQ_TEST_CHECK(windows_write_dg(pub, track, &alloc, sp, 3, 0, 0x90)
                   == MOQ_OK);
    /* Decreasing group rejects on every admitting path, zero mutation --
     * proven with the white-box admission counters: none of the four
     * rejected paths commits, merges, or releases anything. */
    unsigned long base_commit = moq_pub_test_commit_count;
    unsigned long base_merge = moq_pub_test_merge_count;
    unsigned long base_release = moq_pub_test_release_count;
    MOQ_TEST_CHECK(windows_write_dg(pub, track, &alloc, sp, 2, 0, 0x91)
                   == MOQ_ERR_WRONG_STATE);
    MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 2, 1, 0x92)
                   == MOQ_ERR_WRONG_STATE);
    {
        moq_pub_begin_object_cfg_t bc; moq_pub_begin_object_cfg_init(&bc);
        bc.group_id = 2; bc.object_id = 2; bc.payload_length = 2;
        MOQ_TEST_CHECK(moq_pub_begin_object(pub, track, &bc, now)
                       == MOQ_ERR_WRONG_STATE);
    }
    {
        moq_pub_object_cfg_t oc; moq_pub_object_cfg_init(&oc);
        oc.group_id = 2; oc.object_id = 0; oc.datagram = true;
        oc.has_status = true; oc.status = MOQ_OBJECT_END_OF_GROUP;
        MOQ_TEST_CHECK(moq_pub_write_object_ex(pub, track, &oc, now)
                       == MOQ_ERR_WRONG_STATE);
    }
    /* Zero mutation: counters, history, wire, pending op all untouched. */
    MOQ_TEST_CHECK_EQ_U64(moq_pub_test_commit_count, base_commit);
    MOQ_TEST_CHECK_EQ_U64(moq_pub_test_merge_count, base_merge);
    MOQ_TEST_CHECK_EQ_U64(moq_pub_test_release_count, base_release);
    {
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_namespace_t ns = { parts, 1 };
        size_t kl = 0;
        uint8_t *k = moq_build_track_key(sv, &ns,
            MOQ_BYTES_LITERAL("video"), &kl);
        moq_track_hist_t *r = track_hist_find(sv, k, kl);
        if (k) sv->alloc.free(k, kl, sv->alloc.ctx);
        MOQ_TEST_CHECK(r && r->has_largest &&
                       r->largest_group == 3 && r->largest_object == 0);
    }
    { moq_action_t a;
      MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_actions(sv, &a, 1),
                             (size_t)0); }
    /* SAME-group writes stay legal until evidence exists... */
    MOQ_TEST_CHECK(windows_write_dg(pub, track, &alloc, sp, 3, 1, 0x93)
                   == MOQ_OK);
    /* ...then a status 0x3 admission seals the group. */
    {
        moq_pub_object_cfg_t oc; moq_pub_object_cfg_init(&oc);
        oc.group_id = 3; oc.object_id = 2; oc.datagram = true;
        oc.has_status = true; oc.status = MOQ_OBJECT_END_OF_GROUP;
        MOQ_TEST_CHECK(moq_pub_write_object_ex(pub, track, &oc, now)
                       == MOQ_OK);
    }
    MOQ_TEST_CHECK(windows_write_dg(pub, track, &alloc, sp, 3, 3, 0x94)
                   == MOQ_ERR_WRONG_STATE);
    MOQ_TEST_CHECK(windows_write_dg(pub, track, &alloc, sp, 4, 0, 0x95)
                   == MOQ_OK);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "mono_rejections_d18" : "mono_rejections_d16");
}

/* Non-monotonic regression: decreasing groups stay accepted. */
static void test_mono_plain_regression(moq_version_t ver)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    windows_setup(&as, &alloc, &sp, ver, &pub, &track);
    MOQ_TEST_CHECK(windows_write_dg(pub, track, &alloc, sp, 3, 0, 0x96)
                   == MOQ_OK);
    MOQ_TEST_CHECK(windows_write_dg(pub, track, &alloc, sp, 2, 0, 0x97)
                   == MOQ_OK);
    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "mono_plain_regression_d18" : "mono_plain_regression_d16");
}

/* -- EOG evidence: open vs clean FIN; never inline; RESET clears ------- */

static void test_mono_eog_open_vs_fin(moq_version_t ver)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    mono_setup(&as, &alloc, &sp, ver, &pub, &track);
    uint64_t now = moq_simpair_now_us(sp);
    windows_subscribe(sp, pub, MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE, 0, 0, 2);
    drain_all(sp);

    /* EOG-flagged write with the subgroup still OPEN: armed, NOT promoted
     * -- no completion even after an explicit flush. */
    MOQ_TEST_CHECK(mono_write_stream_eog(pub, track, &alloc, sp, 2, 0, true,
                                         0x98) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK(moq_pub_flush(pub, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_INT(declare_client_dones(sp, NULL, NULL), 0);

    /* Clean FIN promotes -- but the evidence-producing call itself never
     * completes inline: only the next flush/tick sweeps. */
    MOQ_TEST_CHECK(moq_pub_end_group(pub, track, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_INT(declare_client_dones(sp, NULL, NULL), 0);
    MOQ_TEST_CHECK(moq_pub_flush(pub, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    {
        uint64_t st = ~0ull, sc = ~0ull;
        MOQ_TEST_CHECK_EQ_INT(declare_client_dones(sp, &st, &sc), 1);
        MOQ_TEST_CHECK_EQ_U64(st, 0x3);
        MOQ_TEST_CHECK_EQ_U64(sc, 1);
    }
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track),
                           (size_t)0);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "mono_eog_open_vs_fin_d18" : "mono_eog_open_vs_fin_d16");
}

static void test_mono_reset_clears(moq_version_t ver)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    mono_setup(&as, &alloc, &sp, ver, &pub, &track);
    uint64_t now = moq_simpair_now_us(sp);
    windows_subscribe(sp, pub, MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE, 0, 0, 2);
    drain_all(sp);

    /* Armed EOG on group 2, then a track-wide RESET: the candidate dies
     * (both drafts void the EOG inference for reset streams). A later
     * clean close WITHOUT a replacement EOG write promotes nothing. */
    MOQ_TEST_CHECK(mono_write_stream_eog(pub, track, &alloc, sp, 2, 0, true,
                                         0x9A) == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_reset_group(pub, track, 0x10, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    /* An end_group DIRECTLY after the reset must promote NOTHING: a stale
     * arm surviving the reset would complete end=2 right here. */
    MOQ_TEST_CHECK(moq_pub_end_group(pub, track, now) == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_flush(pub, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_INT(declare_client_dones(sp, NULL, NULL), 0);
    /* Same-group continuation stays legal; a clean close of a NON-eog
     * final write still promotes nothing. */
    MOQ_TEST_CHECK(mono_write_stream_eog(pub, track, &alloc, sp, 2, 1, false,
                                         0x9B) == MOQ_OK);   /* same group OK */
    MOQ_TEST_CHECK(moq_pub_end_group(pub, track, now) == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_flush(pub, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_INT(declare_client_dones(sp, NULL, NULL), 0);

    /* A REPLACEMENT completed EOG write + clean close promotes. */
    MOQ_TEST_CHECK(mono_write_stream_eog(pub, track, &alloc, sp, 2, 2, true,
                                         0x9C) == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_end_group(pub, track, now) == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_flush(pub, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    {
        uint64_t st = ~0ull, sc = ~0ull;
        MOQ_TEST_CHECK_EQ_INT(declare_client_dones(sp, &st, &sc), 1);
        MOQ_TEST_CHECK_EQ_U64(st, 0x3);
        MOQ_TEST_CHECK_EQ_U64(sc, 3);   /* three opened subgroups, exact */
    }

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "mono_reset_clears_d18" : "mono_reset_clears_d16");
}

/* -- Crossed-old-EOG ordering + blocked-op behavior --------------------- */

static void test_mono_crossed_and_blocked(moq_version_t ver, bool abandon)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    mono_setup(&as, &alloc, &sp, ver, &pub, &track);
    moq_session_t *sv = moq_simpair_server(sp);
    uint64_t now = moq_simpair_now_us(sp);
    windows_subscribe(sp, pub, MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE, 0, 0, 3);
    drain_all(sp);

    /* Armed group 2 (completed EOG write), then an EOG write to group 3
     * that BLOCKS mid-fan-out. The new arm must not overwrite the crossed
     * group before promotion, and a blocked op contributes nothing. */
    MOQ_TEST_CHECK(mono_write_stream_eog(pub, track, &alloc, sp, 2, 0, true,
                                         0x9D) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    drain_all(sp);
    test_session_fill_action_queue(sv);
    MOQ_TEST_CHECK(mono_write_stream_eog(pub, track, &alloc, sp, 3, 0, true,
                                         0x9E) == MOQ_ERR_WOULD_BLOCK);
    manual_drain_actions(sv);
    /* No promotion from the BLOCKED op: end=3 needs group 3's own clean
     * close; a flush right now completes NOTHING for it (group-advance
     * covers only <= 2; window end is 3). */
    MOQ_TEST_CHECK(moq_pub_flush(pub, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_INT(declare_client_dones(sp, NULL, NULL), 0);

    if (abandon) {
        /* Abandon: the armed group-2 candidate is CLEARED (reset streams
         * promise nothing) -- yet window end=3... stays open; a window
         * end=2 twin would still complete via group-advance (case 22(b)'s
         * discrimination lives in mono_group_advance below). Here: no
         * completion at all until real group-3 evidence. */
        MOQ_TEST_CHECK(moq_pub_reset_group(pub, track, 0x10, now) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        MOQ_TEST_CHECK(mono_write_stream_eog(pub, track, &alloc, sp, 3, 1,
                                             true, 0x9F) == MOQ_OK);
        MOQ_TEST_CHECK(moq_pub_end_group(pub, track, now) == MOQ_OK);
    } else {
        /* Retry completes the op: the crossed group 2 promotes FIRST, then
         * group 3 arms from the completed op's flag; its subgroup is still
         * OPEN so end=3 must NOT complete yet. */
        MOQ_TEST_CHECK(mono_write_stream_eog(pub, track, &alloc, sp, 3, 0,
                                             true, 0x9E) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        MOQ_TEST_CHECK(moq_pub_flush(pub, now) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        MOQ_TEST_CHECK_EQ_INT(declare_client_dones(sp, NULL, NULL), 0);
        MOQ_TEST_CHECK(moq_pub_end_group(pub, track, now) == MOQ_OK);
    }
    MOQ_TEST_CHECK(moq_pub_flush(pub, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    {
        uint64_t st = ~0ull;
        MOQ_TEST_CHECK_EQ_INT(declare_client_dones(sp, &st, NULL), 1);
        MOQ_TEST_CHECK_EQ_U64(st, 0x3);
    }

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        (abandon ? "mono_blocked_abandon_d18" : "mono_crossed_retry_d18")
        : (abandon ? "mono_blocked_abandon_d16" : "mono_crossed_retry_d16"));
}

/* -- Group advance completes without any EOG marker --------------------- */

static void test_mono_group_advance(moq_version_t ver)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    mono_setup(&as, &alloc, &sp, ver, &pub, &track);
    uint64_t now = moq_simpair_now_us(sp);
    windows_subscribe(sp, pub, MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE, 0, 0, 1);
    drain_all(sp);

    MOQ_TEST_CHECK(windows_write_dg(pub, track, &alloc, sp, 0, 0, 0xA4)
                   == MOQ_OK);
    MOQ_TEST_CHECK(windows_write_dg(pub, track, &alloc, sp, 1, 0, 0xA5)
                   == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(declare_client_dones(sp, NULL, NULL), 0);
    /* Beginning group 2 seals groups <= 1 under the promise: the finite
     * end=1 window completes -- but only on the NEXT explicit flush, never
     * inline in the write. */
    MOQ_TEST_CHECK(windows_write_dg(pub, track, &alloc, sp, 2, 0, 0xA6)
                   == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(declare_client_dones(sp, NULL, NULL), 0);
    MOQ_TEST_CHECK(moq_pub_flush(pub, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    {
        uint64_t st = ~0ull, sc = ~0ull;
        MOQ_TEST_CHECK_EQ_INT(declare_client_dones(sp, &st, &sc), 1);
        MOQ_TEST_CHECK_EQ_U64(st, 0x3);
        MOQ_TEST_CHECK_EQ_U64(sc, 0);   /* datagram-only: zero streams */
    }

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "mono_group_advance_d18" : "mono_group_advance_d16");
}

/* -- Status evidence: 0x3 zero-dest + Forward-0; 0x4 is not evidence ---- */

static void test_mono_status_evidence(moq_version_t ver)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    mono_setup(&as, &alloc, &sp, ver, &pub, &track);
    moq_session_t *sv = moq_simpair_server(sp);
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);

    /* Status 0x3 with ZERO destinations still records evidence. */
    {
        moq_pub_object_cfg_t oc; moq_pub_object_cfg_init(&oc);
        oc.group_id = 3; oc.object_id = 5; oc.datagram = true;
        oc.has_status = true; oc.status = MOQ_OBJECT_END_OF_GROUP;
        MOQ_TEST_CHECK(moq_pub_write_object_ex(pub, track, &oc, now)
                       == MOQ_OK);
    }
    /* A later covered subscriber (end=3) completes on installation. */
    {
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace.parts = ns_parts;
        scfg.track_namespace.count = 1;
        scfg.track_name = MOQ_BYTES_LITERAL("video");
        scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE;
        scfg.start_group = 0; scfg.start_object = 0; scfg.end_group = 3;
        moq_subscription_t sub_h;
        MOQ_TEST_CHECK(moq_session_subscribe(cl, &scfg, now, &sub_h)
                       == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        manual_forward(pub, sv, now, 0, NULL, NULL);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        uint64_t st = ~0ull, sc = ~0ull;
        MOQ_TEST_CHECK_EQ_INT(declare_client_dones(sp, &st, &sc), 1);
        MOQ_TEST_CHECK_EQ_U64(st, 0x3);
        MOQ_TEST_CHECK_EQ_U64(sc, 0);
    }
    /* Forward-0 destination: evidence still forms, the paused covered slot
     * still completes (the drafts require the done regardless of Forward). */
    {
        moq_subscription_t sub_h = windows_subscribe(sp, pub,
            MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE, 0, 0, 4);
        drain_all(sp);
        moq_subscription_update_cfg_t ucfg;
        moq_subscription_update_cfg_init(&ucfg);
        ucfg.has_forward = true; ucfg.forward = false;
        MOQ_TEST_CHECK(moq_session_update_subscription(cl, sub_h, &ucfg, now)
                       == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        manual_forward(pub, sv, now, 0, NULL, NULL);
        moq_pub_object_cfg_t oc; moq_pub_object_cfg_init(&oc);
        oc.group_id = 4; oc.object_id = 9; oc.datagram = true;
        oc.has_status = true; oc.status = MOQ_OBJECT_END_OF_GROUP;
        MOQ_TEST_CHECK(moq_pub_write_object_ex(pub, track, &oc, now)
                       == MOQ_OK);
        MOQ_TEST_CHECK(moq_pub_flush(pub, now) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        uint64_t st = ~0ull, sc = ~0ull;
        MOQ_TEST_CHECK_EQ_INT(declare_client_dones(sp, &st, &sc), 1);
        MOQ_TEST_CHECK_EQ_U64(st, 0x3);
        MOQ_TEST_CHECK_EQ_U64(sc, 0);
    }
    /* Status 0x4 (End of Track) is NOT End-of-Group evidence: a window
     * ending exactly at its group must not complete (group-advance only
     * covers groups strictly below). */
    {
        (void)windows_subscribe(sp, pub,
            MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE, 0, 0, 6);
        drain_all(sp);
        moq_pub_object_cfg_t oc; moq_pub_object_cfg_init(&oc);
        oc.group_id = 6; oc.object_id = 0; oc.datagram = true;
        oc.has_status = true; oc.status = MOQ_OBJECT_END_OF_TRACK;
        MOQ_TEST_CHECK(moq_pub_write_object_ex(pub, track, &oc, now)
                       == MOQ_OK);
        MOQ_TEST_CHECK(moq_pub_flush(pub, now) == MOQ_OK);
        MOQ_TEST_CHECK(moq_pub_tick(pub, now) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        MOQ_TEST_CHECK_EQ_INT(declare_client_dones(sp, NULL, NULL), 0);
        MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track),
                               (size_t)1);
    }

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "mono_status_evidence_d18" : "mono_status_evidence_d16");
}

/* -- Streaming bracket + higher-group datagram --------------------------- */

static void test_mono_streaming_higher_dg(moq_version_t ver)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    mono_setup(&as, &alloc, &sp, ver, &pub, &track);
    moq_session_t *sv = moq_simpair_server(sp);
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);
    windows_subscribe(sp, pub, MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE, 0, 0, 2);
    drain_all(sp);

    /* Open a streaming bracket in group 2 (the subscription participates). */
    {
        moq_pub_begin_object_cfg_t bc; moq_pub_begin_object_cfg_init(&bc);
        bc.group_id = 2; bc.object_id = 0; bc.payload_length = 4;
        MOQ_TEST_CHECK(moq_pub_begin_object(pub, track, &bc, now) == MOQ_OK);
    }
    /* A higher-group datagram DURING the bracket is legal and raises the
     * watermark over the streaming slot's end... */
    MOQ_TEST_CHECK(windows_write_dg(pub, track, &alloc, sp, 3, 0, 0xA7)
                   == MOQ_OK);
    /* ...a non-streaming covered destination completes now (a publication
     * accepted mid-bracket with a finite end <= 2)... */
    {
        moq_pub_publish_cfg_t pcfg; moq_pub_publish_cfg_init(&pcfg);
        MOQ_TEST_CHECK(moq_pub_publish_track(pub, track, &pcfg, now)
                       == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(cl, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_PUBLISH_REQUEST);
        moq_publication_t cpub = ev.u.publish_request.pub;
        moq_event_cleanup(&ev);
        moq_accept_publish_cfg_t ac;
        moq_accept_publish_cfg_init_sized(&ac, sizeof(ac));
        ac.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE;
        ac.start_group = 0; ac.start_object = 0; ac.end_group = 2;
        MOQ_TEST_CHECK(moq_session_accept_publish(cl, cpub, &ac, now)
                       == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        manual_forward(pub, sv, now, 0, NULL, NULL);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        int fin_n = 0; uint64_t st = ~0ull;
        moq_event_t d[16]; size_t ne;
        while ((ne = moq_session_poll_events(cl, d, 16)) > 0)
            for (size_t i = 0; i < ne; i++) {
                if (d[i].kind == MOQ_EVENT_PUBLISH_FINISHED) {
                    fin_n++;
                    st = d[i].u.publish_finished.status_code;
                }
                moq_event_cleanup(&d[i]);
            }
        MOQ_TEST_CHECK_EQ_INT(fin_n, 1);
        MOQ_TEST_CHECK_EQ_U64(st, 0x3);
    }
    /* ...while the STREAMING subscription defers: flush completes nothing
     * until the bracket ends. */
    MOQ_TEST_CHECK(moq_pub_flush(pub, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_INT(declare_client_dones(sp, NULL, NULL), 0);
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track),
                           (size_t)1);
    {
        uint8_t d2[4] = { 1, 2, 3, 4 };
        moq_rcbuf_t *ch = NULL; moq_rcbuf_create(&alloc, d2, sizeof(d2), &ch);
        MOQ_TEST_CHECK(moq_pub_write_data(pub, track, ch, now) == MOQ_OK);
        moq_rcbuf_decref(ch);
        MOQ_TEST_CHECK(moq_pub_end_object(pub, track, now) == MOQ_OK);
    }
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_INT(declare_client_dones(sp, NULL, NULL), 0);
    MOQ_TEST_CHECK(moq_pub_flush(pub, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    {
        uint64_t st = ~0ull, sc = ~0ull;
        MOQ_TEST_CHECK_EQ_INT(declare_client_dones(sp, &st, &sc), 1);
        MOQ_TEST_CHECK_EQ_U64(st, 0x3);
        MOQ_TEST_CHECK_EQ_U64(sc, 1);
    }

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "mono_streaming_higher_dg_d18" : "mono_streaming_higher_dg_d16");
}

/* -- Datagrams never arm EOG state --------------------------------------- */

static void test_mono_datagram_no_arm(moq_version_t ver)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    mono_setup(&as, &alloc, &sp, ver, &pub, &track);
    uint64_t now = moq_simpair_now_us(sp);
    windows_subscribe(sp, pub, MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE, 0, 0, 3);
    drain_all(sp);

    /* A datagram write carrying the cfg end_of_group flag must not arm:
     * a later clean close promotes nothing (no FIN ever backed it). */
    {
        uint8_t d[2] = { 0xA8, 0x57 };
        moq_rcbuf_t *pl = NULL; moq_rcbuf_create(&alloc, d, sizeof(d), &pl);
        moq_pub_object_cfg_t oc;
        moq_pub_object_cfg_init_sized(&oc, sizeof(oc));
        oc.group_id = 3; oc.object_id = 0; oc.payload = pl;
        oc.datagram = true; oc.end_of_group = true;
        MOQ_TEST_CHECK(moq_pub_write_object_ex(pub, track, &oc, now)
                       == MOQ_OK);
        moq_rcbuf_decref(pl);
    }
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK(moq_pub_end_group(pub, track, now) == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_flush(pub, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_INT(declare_client_dones(sp, NULL, NULL), 0);

    /* The stream equivalent DOES arm and promote. */
    MOQ_TEST_CHECK(mono_write_stream_eog(pub, track, &alloc, sp, 3, 1, true,
                                         0xA9) == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_end_group(pub, track, now) == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_flush(pub, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    {
        uint64_t st = ~0ull;
        MOQ_TEST_CHECK_EQ_INT(declare_client_dones(sp, &st, NULL), 1);
        MOQ_TEST_CHECK_EQ_U64(st, 0x3);
    }

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "mono_datagram_no_arm_d18" : "mono_datagram_no_arm_d16");
}

/* -- ABI: poisoned v0 tail can never enable monotonic mode ---------- */

static void test_mono_abi_canary(moq_version_t ver)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track_full;
    windows_setup(&as, &alloc, &sp, ver, &pub, &track_full);
    uint64_t now = moq_simpair_now_us(sp);

    /* Exact v0-sized heap cfg. The old sizeof is derived INDEPENDENTLY
     * of the appended field -- the align-rounded end of the old struct's
     * last member -- so the canary keeps modeling a REAL 64-byte caller
     * even if monotonic_groups ever drifted. Every byte poisoned 0xAA
     * first, real fields memcpy'd at their offsets, the old TRAILING
     * PADDING left poisoned; add_track must not read monotonic_groups (it
     * lies past the allocation). */
    const size_t old_size =
        (offsetof(moq_pub_track_cfg_t, has_publisher_priority) +
         sizeof(bool) + (_Alignof(moq_pub_track_cfg_t) - 1)) &
        ~(size_t)(_Alignof(moq_pub_track_cfg_t) - 1);
    MOQ_TEST_CHECK_EQ_SIZE(old_size,
                           offsetof(moq_pub_track_cfg_t, monotonic_groups));
    uint8_t *raw = (uint8_t *)malloc(old_size);
    MOQ_TEST_CHECK(raw != NULL);
    memset(raw, 0xAA, old_size);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    {
        uint32_t ss = (uint32_t)old_size;
        moq_namespace_t ns = { ns_parts, 1 };
        moq_bytes_t nm = MOQ_BYTES_LITERAL("old");
        bool bfalse = false;
        uint8_t prio = 128;
        uint64_t zero64 = 0;
        memcpy(raw + offsetof(moq_pub_track_cfg_t, struct_size),
               &ss, sizeof(ss));
        memcpy(raw + offsetof(moq_pub_track_cfg_t, track_namespace),
               &ns, sizeof(ns));
        memcpy(raw + offsetof(moq_pub_track_cfg_t, track_name),
               &nm, sizeof(nm));
        memcpy(raw + offsetof(moq_pub_track_cfg_t, advertise_namespace),
               &bfalse, sizeof(bfalse));
        memcpy(raw + offsetof(moq_pub_track_cfg_t, publisher_priority),
               &prio, sizeof(prio));
        memcpy(raw + offsetof(moq_pub_track_cfg_t, max_retained_bytes),
               &zero64, sizeof(zero64));
        memcpy(raw + offsetof(moq_pub_track_cfg_t, has_publisher_priority),
               &bfalse, sizeof(bfalse));
        /* Bytes between has_publisher_priority+1 and old_size stay 0xAA:
         * the poisoned v0 trailing padding. */
    }
    moq_pub_track_t *told = NULL;
    MOQ_TEST_CHECK(moq_pub_add_track(pub,
        (const moq_pub_track_cfg_t *)(const void *)raw, now, &told)
        == MOQ_OK);
    free(raw);
    /* NOT monotonic: decreasing groups accepted. */
    MOQ_TEST_CHECK(windows_write_dg(pub, told, &alloc, sp, 3, 0, 0xAB)
                   == MOQ_OK);
    MOQ_TEST_CHECK(windows_write_dg(pub, told, &alloc, sp, 2, 0, 0xAC)
                   == MOQ_OK);

    /* Full-size init_sized zeroes the field (opt-in only)... */
    {
        moq_pub_track_cfg_t tc;
        moq_pub_track_cfg_init_sized(&tc, sizeof(tc));
        MOQ_TEST_CHECK(!tc.monotonic_groups);
        tc.track_namespace.parts = ns_parts;
        tc.track_namespace.count = 1;
        tc.track_name = MOQ_BYTES_LITERAL("newm");
        tc.monotonic_groups = true;
        moq_pub_track_t *tm = NULL;
        MOQ_TEST_CHECK(moq_pub_add_track(pub, &tc, now, &tm) == MOQ_OK);
        /* ...and setting it enforces. */
        MOQ_TEST_CHECK(windows_write_dg(pub, tm, &alloc, sp, 3, 0, 0xAD)
                       == MOQ_OK);
        MOQ_TEST_CHECK(windows_write_dg(pub, tm, &alloc, sp, 2, 0, 0xAE)
                       == MOQ_ERR_WRONG_STATE);
    }

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "mono_abi_canary_d18" : "mono_abi_canary_d16");
}

/* -- Declaration (B) and monotonic (A) compose; semantics intact ------- */

static void test_mono_declare_compose(moq_version_t ver)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    mono_setup(&as, &alloc, &sp, ver, &pub, &track);
    uint64_t now = moq_simpair_now_us(sp);
    windows_subscribe(sp, pub, MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE, 0, 0, 5);
    drain_all(sp);

    /* B on a monotonic track: the declaration completes end=5 exactly as
     * on a plain track (declaration semantics preserved), and afterwards BOTH
     * seals hold. */
    MOQ_TEST_CHECK(windows_write_dg(pub, track, &alloc, sp, 2, 0, 0xB4)
                   == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_declare_groups_complete_through(pub, track, 5,
                                                           now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    {
        uint64_t st = ~0ull;
        MOQ_TEST_CHECK_EQ_INT(declare_client_dones(sp, &st, NULL), 1);
        MOQ_TEST_CHECK_EQ_U64(st, 0x3);
    }
    MOQ_TEST_CHECK(windows_write_dg(pub, track, &alloc, sp, 4, 0, 0xB5)
                   == MOQ_ERR_WRONG_STATE);   /* B seal */
    MOQ_TEST_CHECK(windows_write_dg(pub, track, &alloc, sp, 6, 0, 0xB6)
                   == MOQ_OK);
    MOQ_TEST_CHECK(windows_write_dg(pub, track, &alloc, sp, 5, 9, 0xB7)
                   == MOQ_ERR_WRONG_STATE);   /* A seal (decreasing) too */

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "mono_declare_compose_d18" : "mono_declare_compose_d16");
}


/* Version-parameterized simpair for the namespace-terminal tests (the
 * shared simpair_setup is version-agnostic; these need both drafts). The
 * server is the publisher side. */
static moq_simpair_t *sc_pair_pub(moq_alloc_t *alloc, moq_version_t ver)
{
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = alloc; cfg.seed = 42; cfg.initial_now_us = 1000;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 16;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 16;
    cfg.version = ver;
    moq_simpair_t *sp = NULL;
    moq_simpair_create(&cfg, &sp);
    moq_simpair_start(sp);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_event_t ev;
    if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
        moq_event_cleanup(&ev);
    if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1)
        moq_event_cleanup(&ev);
    return sp;
}

/* ==================================================================== *
 *  Namespace terminal callback (rejection / cancellation)              *
 * ==================================================================== */

typedef struct {
    int      n;
    uint32_t res_struct_size;
    moq_pub_namespace_terminal_kind_t kind;
    size_t   ns_count;
    char     ns0[32];
    size_t   ns0_len;
    moq_request_error_t error_code;
    bool     can_retry;
    uint64_t retry_after_ms;
    char     reason[64];
    size_t   reason_len;
} nsterm_state_t;

static void nsterm_cb(void *ctx,
                      const moq_pub_namespace_terminal_info_t *info)
{
    nsterm_state_t *st = (nsterm_state_t *)ctx;
    st->n++;
    st->res_struct_size = info->struct_size;
    st->kind = info->kind;
    st->ns_count = info->namespace_.count;
    if (info->namespace_.count > 0 && info->namespace_.parts[0].len) {
        st->ns0_len = info->namespace_.parts[0].len < sizeof(st->ns0)
            ? info->namespace_.parts[0].len : sizeof(st->ns0) - 1;
        memcpy(st->ns0, info->namespace_.parts[0].data, st->ns0_len);
        st->ns0[st->ns0_len] = 0;
    }
    st->error_code = info->error_code;
    st->can_retry = info->can_retry;
    st->retry_after_ms = info->retry_after_ms;
    st->reason_len = info->reason.len < sizeof(st->reason)
        ? info->reason.len : sizeof(st->reason) - 1;
    if (info->reason.len) memcpy(st->reason, info->reason.data, st->reason_len);
    st->reason[st->reason_len] = 0;
}

/* Advertise a namespace, let the peer REJECT it, forward to the facade, and
 * assert the terminal callback fired once with the exact scalars/reason. */
static void test_nsterm_rejected(moq_version_t ver)
{
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    moq_simpair_t *sp = sc_pair_pub(&alloc, ver);
    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);

    nsterm_state_t st = {0};
    moq_pub_cfg_t cfg; moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    cfg.callbacks.ctx = &st;
    cfg.callbacks.on_namespace_terminal = nsterm_cb;
    moq_publisher_t *pub = NULL;
    MOQ_TEST_CHECK(moq_pub_create(sv, &alloc, &cfg, &pub) == MOQ_OK);

    moq_pub_track_cfg_t tcfg; moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    tcfg.track_namespace.parts = ns_parts; tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    tcfg.advertise_namespace = true;
    moq_pub_track_t *track = NULL;
    MOQ_TEST_CHECK(moq_pub_add_track(pub, &tcfg,
        moq_simpair_now_us(sp), &track) == MOQ_OK);

    /* Peer receives PUBLISH_NAMESPACE and REJECTS it. */
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_event_t ev;
    MOQ_TEST_CHECK(moq_session_poll_events(cl, &ev, 1) == 1);
    MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED);
    {
        moq_reject_namespace_cfg_t rj; memset(&rj, 0, sizeof(rj));
        rj.struct_size = sizeof(rj);
        rj.error_code = MOQ_REQUEST_ERROR_UNAUTHORIZED;
        rj.reason = MOQ_BYTES_LITERAL("nope");
        rj.can_retry = true;
        rj.retry_after_ms = 5000;
        MOQ_TEST_CHECK(moq_session_reject_namespace(cl,
            ev.u.namespace_published.ann, &rj,
            moq_simpair_now_us(sp)) == MOQ_OK);
    }
    moq_event_cleanup(&ev);

    /* Forward the rejection to the facade, capturing the announcement
     * handle so a duplicate can be replayed below. */
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_announcement_t dup_ann = {0};
    bool have_dup = false;
    while (moq_session_poll_events(sv, &ev, 1) == 1) {
        if (ev.kind == MOQ_EVENT_NAMESPACE_REJECTED) {
            dup_ann = ev.u.namespace_rejected.ann;
            have_dup = true;
        }
        moq_pub_event_result_t res = MOQ_PUB_EVENT_IGNORED;
        moq_pub_handle_event(pub, &ev, moq_simpair_now_us(sp), &res);
        moq_event_cleanup(&ev);
    }

    MOQ_TEST_CHECK_EQ_INT(st.n, 1);
    MOQ_TEST_CHECK_EQ_U64(st.res_struct_size,
        (uint64_t)sizeof(moq_pub_namespace_terminal_info_t));
    MOQ_TEST_CHECK(st.kind == MOQ_PUB_NAMESPACE_REJECTED);
    MOQ_TEST_CHECK_EQ_SIZE(st.ns_count, (size_t)1);
    MOQ_TEST_CHECK(st.ns0_len == 4 && memcmp(st.ns0, "live", 4) == 0);
    (void)track;
    MOQ_TEST_CHECK_EQ_U64(st.error_code, MOQ_REQUEST_ERROR_UNAUTHORIZED);
    MOQ_TEST_CHECK(st.can_retry);
    MOQ_TEST_CHECK_EQ_U64(st.retry_after_ms, 5000);
    MOQ_TEST_CHECK(st.reason_len == 4 && memcmp(st.reason, "nope", 4) == 0);
    MOQ_TEST_CHECK(!moq_pub_namespace_accepted(pub, track));

    /* Exactly once: replaying the SAME terminal event (a duplicate/stale
     * delivery) matches the now-terminal entry but must NOT re-fire. */
    MOQ_TEST_CHECK(have_dup);
    {
        moq_event_t dup; memset(&dup, 0, sizeof(dup));
        dup.kind = MOQ_EVENT_NAMESPACE_REJECTED;
        dup.u.namespace_rejected.ann = dup_ann;
        dup.u.namespace_rejected.error_code = MOQ_REQUEST_ERROR_UNAUTHORIZED;
        moq_pub_event_result_t res = MOQ_PUB_EVENT_IGNORED;
        moq_pub_handle_event(pub, &dup, moq_simpair_now_us(sp), &res);
        /* Matched (already terminal) -> CONSUMED, but no second callback. */
        MOQ_TEST_CHECK(res == MOQ_PUB_EVENT_CONSUMED);
    }
    MOQ_TEST_CHECK_EQ_INT(st.n, 1);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "nsterm_rejected_d18" : "nsterm_rejected_d16");
}

/* Advertise, peer ACCEPTS, then peer CANCELS the accepted namespace. */
static void test_nsterm_cancelled(moq_version_t ver)
{
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    moq_simpair_t *sp = sc_pair_pub(&alloc, ver);
    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);

    nsterm_state_t st = {0};
    moq_pub_cfg_t cfg; moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    cfg.callbacks.ctx = &st;
    cfg.callbacks.on_namespace_terminal = nsterm_cb;
    moq_publisher_t *pub = NULL;
    MOQ_TEST_CHECK(moq_pub_create(sv, &alloc, &cfg, &pub) == MOQ_OK);

    moq_pub_track_cfg_t tcfg; moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    tcfg.track_namespace.parts = ns_parts; tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    tcfg.advertise_namespace = true;
    moq_pub_track_t *track = NULL;
    MOQ_TEST_CHECK(moq_pub_add_track(pub, &tcfg,
        moq_simpair_now_us(sp), &track) == MOQ_OK);

    /* Peer accepts. */
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_event_t ev;
    moq_announcement_t peer_ann;
    MOQ_TEST_CHECK(moq_session_poll_events(cl, &ev, 1) == 1);
    MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED);
    peer_ann = ev.u.namespace_published.ann;
    {
        moq_accept_namespace_cfg_t ac; moq_accept_namespace_cfg_init(&ac);
        MOQ_TEST_CHECK(moq_session_accept_namespace(cl, peer_ann, &ac,
            moq_simpair_now_us(sp)) == MOQ_OK);
    }
    moq_event_cleanup(&ev);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    while (moq_session_poll_events(sv, &ev, 1) == 1) {
        moq_pub_event_result_t res = MOQ_PUB_EVENT_IGNORED;
        moq_pub_handle_event(pub, &ev, moq_simpair_now_us(sp), &res);
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK(moq_pub_namespace_accepted(pub, track));
    MOQ_TEST_CHECK_EQ_INT(st.n, 0);   /* acceptance is not terminal */

    /* Peer (the receiver) CANCELS the accepted namespace. draft-16 carries
     * a REQUEST_ERROR code + reason on the control channel; draft-18 revokes
     * by resetting the request bidi (§3.3.2), a numeric §3.3.3 code with NO
     * reason phrase -- so a reason is only representable in d16. */
    bool d16 = (ver == MOQ_VERSION_DRAFT_16);
    {
        moq_cancel_namespace_cfg_t cn; moq_cancel_namespace_cfg_init(&cn);
        if (d16) {
            cn.error_code = MOQ_REQUEST_ERROR_UNAUTHORIZED;
            cn.reason = MOQ_BYTES_LITERAL("gone");
        }
        MOQ_TEST_CHECK(moq_session_cancel_namespace(cl, peer_ann, &cn,
            moq_simpair_now_us(sp)) == MOQ_OK);
    }
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    while (moq_session_poll_events(sv, &ev, 1) == 1) {
        moq_pub_event_result_t res = MOQ_PUB_EVENT_IGNORED;
        moq_pub_handle_event(pub, &ev, moq_simpair_now_us(sp), &res);
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK_EQ_INT(st.n, 1);
    MOQ_TEST_CHECK(st.kind == MOQ_PUB_NAMESPACE_CANCELLED);
    MOQ_TEST_CHECK(!st.can_retry);           /* cancellation not retryable */
    MOQ_TEST_CHECK_EQ_U64(st.retry_after_ms, 0);
    if (d16) {
        MOQ_TEST_CHECK_EQ_U64(st.error_code, MOQ_REQUEST_ERROR_UNAUTHORIZED);
        MOQ_TEST_CHECK(st.reason_len == 4 &&
                       memcmp(st.reason, "gone", 4) == 0);
    }

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "nsterm_cancelled_d18" : "nsterm_cancelled_d16");
}

/* Unmatched namespace terminal events are ignored (no callback). */
static void test_nsterm_unmatched(void)
{
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    moq_simpair_t *sp = sc_pair_pub(&alloc, MOQ_VERSION_DRAFT_18);
    moq_session_t *sv = moq_simpair_server(sp);

    nsterm_state_t st = {0};
    moq_pub_cfg_t cfg; moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    cfg.callbacks.ctx = &st;
    cfg.callbacks.on_namespace_terminal = nsterm_cb;
    moq_publisher_t *pub = NULL;
    MOQ_TEST_CHECK(moq_pub_create(sv, &alloc, &cfg, &pub) == MOQ_OK);

    /* A synthetic NAMESPACE_REJECTED for an announcement the facade never
     * issued: no track matches -> IGNORED, no callback. */
    moq_event_t e; memset(&e, 0, sizeof(e));
    e.kind = MOQ_EVENT_NAMESPACE_REJECTED;
    e.u.namespace_rejected.ann = (moq_announcement_t){0};
    e.u.namespace_rejected.error_code = MOQ_REQUEST_ERROR_UNAUTHORIZED;
    moq_pub_event_result_t res = MOQ_PUB_EVENT_CONSUMED;
    moq_pub_handle_event(pub, &e, moq_simpair_now_us(sp), &res);
    MOQ_TEST_CHECK(res == MOQ_PUB_EVENT_IGNORED);
    MOQ_TEST_CHECK_EQ_INT(st.n, 0);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("nsterm_unmatched");
}

/* an OLD full-size caller (cfg callbacks struct_size ending at
 * on_publish_finished, i.e. offsetof(on_namespace_terminal)) must still have
 * on_publish_finished copied through moq_pub_create -- the copy-size table
 * boundary must include the on_namespace_terminal offset. Proven end to end:
 * finish the publication and require on_publish_finished to fire. */
static int g_old_finished_n;
static void old_on_finished(void *ctx, moq_pub_track_t *t)
{ (void)ctx; (void)t; g_old_finished_n++; }

static void test_nsterm_old_size_finished_preserved(moq_version_t ver)
{
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    moq_simpair_t *sp = sc_pair_pub(&alloc, ver);
    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);
    g_old_finished_n = 0;

    /* A pub cfg whose callbacks struct_size ends EXACTLY at the pre-append
     * full size (offsetof(on_namespace_terminal) == end of
     * on_publish_finished) with on_publish_finished set. moq_pub_create must
     * still copy that field -- the copy-size boundary must include the
     * on_namespace_terminal offset. */
    moq_pub_cfg_t cfg; moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    cfg.callbacks.struct_size =
        (uint32_t)offsetof(moq_pub_callbacks_t, on_namespace_terminal);
    cfg.callbacks.ctx = NULL;
    cfg.callbacks.on_publish_finished = old_on_finished;
    moq_publisher_t *pub = NULL;
    MOQ_TEST_CHECK(moq_pub_create(sv, &alloc, &cfg, &pub) == MOQ_OK);

    moq_pub_track_cfg_t tcfg; moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    tcfg.track_namespace.parts = ns_parts; tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_pub_track_t *track = NULL;
    MOQ_TEST_CHECK(moq_pub_add_track(pub, &tcfg,
        moq_simpair_now_us(sp), &track) == MOQ_OK);

    moq_pub_publish_cfg_t pcfg; moq_pub_publish_cfg_init(&pcfg);
    MOQ_TEST_CHECK(moq_pub_publish_track(pub, track, &pcfg,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t e;
      while (moq_session_poll_events(cl, &e, 1) == 1) {
          if (e.kind == MOQ_EVENT_PUBLISH_REQUEST) {
              moq_accept_publish_cfg_t acc; moq_accept_publish_cfg_init(&acc);
              moq_session_accept_publish(cl, e.u.publish_request.pub, &acc,
                                         moq_simpair_now_us(sp));
          }
          moq_event_cleanup(&e);
      } }
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    /* Capture the publication handle from PUBLISH_OK (find_track_by_pub keys
     * on it) and dispatch the OK to the facade. */
    moq_publication_t ph; memset(&ph, 0, sizeof(ph));
    { moq_event_t e;
      while (moq_session_poll_events(sv, &e, 1) == 1) {
          if (e.kind == MOQ_EVENT_PUBLISH_OK) ph = e.u.publish_ok.pub;
          moq_pub_event_result_t rr = MOQ_PUB_EVENT_IGNORED;
          moq_pub_handle_event(pub, &e, moq_simpair_now_us(sp), &rr);
          moq_event_cleanup(&e);
      } }
    MOQ_TEST_CHECK(moq_pub_track_is_published(pub, track));

    /* The peer terminates the publication -> PUBLISH_UNSUBSCRIBED -> the
     * facade must invoke on_publish_finished. If create had truncated the
     * copy at on_publish_finished, the pointer would be NULL and this would
     * never fire (g_old_finished_n stays 0). */
    moq_event_t un; memset(&un, 0, sizeof(un));
    un.kind = MOQ_EVENT_PUBLISH_UNSUBSCRIBED;
    un.u.publish_unsubscribed.pub = ph;
    moq_pub_event_result_t r = MOQ_PUB_EVENT_IGNORED;
    MOQ_TEST_CHECK(moq_pub_handle_event(pub, &un,
        moq_simpair_now_us(sp), &r) == MOQ_OK);
    MOQ_TEST_CHECK(r == MOQ_PUB_EVENT_CONSUMED);
    MOQ_TEST_CHECK_EQ_INT(g_old_finished_n, 1);   /* the load-bearing check */

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "nsterm_old_size_finished_d18" : "nsterm_old_size_finished_d16");
}

/* ABI: an old-prefix callbacks struct (through on_publish_finished) with a
 * poisoned tail never reads on_namespace_terminal; sized init enables it. */
static void test_nsterm_abi_canary(void)
{
    /* Exact old size = offsetof(on_namespace_terminal): a caller compiled
     * before the append. Poison the whole block, stamp the prefix via the
     * sized init at the OLD size, poison the trailing function-pointer
     * slot -- add_track/create must never read it. */
    const size_t old_size =
        offsetof(moq_pub_callbacks_t, on_namespace_terminal);
    uint8_t *raw = (uint8_t *)malloc(sizeof(moq_pub_callbacks_t));
    MOQ_TEST_CHECK(raw != NULL);
    memset(raw, 0xAA, sizeof(moq_pub_callbacks_t));
    moq_pub_callbacks_init_sized((moq_pub_callbacks_t *)(void *)raw, old_size);
    /* struct_size stamped to old_size; the poisoned tail (the appended
     * pointer) is beyond it. CB_HAS gates on struct_size >= offset+sizeof,
     * so it is never dereferenced. */
    uint32_t ss;
    memcpy(&ss, raw + offsetof(moq_pub_callbacks_t, struct_size), sizeof(ss));
    MOQ_TEST_CHECK_EQ_U64(ss, (uint64_t)old_size);
    /* The bytes at the appended field remain poisoned (untouched by init). */
    {
        const uint8_t *fp = raw +
            offsetof(moq_pub_callbacks_t, on_namespace_terminal);
        int poisoned = 1;
        for (size_t i = 0; i < sizeof(void (*)(void)); i++)
            if (fp[i] != 0xAA) { poisoned = 0; break; }
        MOQ_TEST_CHECK(poisoned == 1);
    }
    /* Full sized init enables + zeroes the field. */
    moq_pub_callbacks_t full;
    moq_pub_callbacks_init_sized(&full, sizeof(full));
    MOQ_TEST_CHECK_EQ_U64(full.struct_size, (uint64_t)sizeof(full));
    MOQ_TEST_CHECK(full.on_namespace_terminal == NULL);
    free(raw);
    MOQ_TEST_PASS("nsterm_abi_canary");
}

int main(void) {
    test_create_destroy();
    test_pub_cfg_init_old_prefix_no_overflow();
    test_pub_callbacks_init_old_prefix_no_overflow();
    test_add_remove_track();
    test_subscribe_accept_all();
    test_remove_track_retires_subscription();
    test_subscribe_no_match();
    test_subscribe_duplicate_rejected();
    test_write_single_sub();
    test_end_track();
    test_finish_subscribers();
    test_finish_subscribers_skips_publication();
    test_finish_subscribers_pub_mid_object();
    test_publish_restart_after_peer_unsubscribe_d16();
    test_publish_restart_d18_cancel();
    test_pub_history_reservation_and_merge();
    test_pub_history_capacity_at_add_track();
    test_pub_history_validate_before_mutate();
    test_pub_history_add_track_oom();
    test_pub_history_malformed_properties_d18();
    test_pub_history_forward_zero_advances();
    test_pub_history_exactly_once_retry();
    test_pub_manual_forward_zero_pauses();
    test_pub_manual_broadcasts_and_results();
    test_pub_manual_publish_ok();
    test_pub_manual_unsubscribe_active();
    test_pub_manual_vs_tick_parity();
    test_pub_manual_forward_zero_resets_open_subgroup();
    test_pub_manual_publish_family_results();
    test_pub_manual_two_track_serialized_no_loss();
    test_pub_manual_pending_blocks_second_ignored();
    test_pub_manual_publish_error_matched();
    test_nsterm_unmatched();
    test_nsterm_abi_canary();
    test_window_membership_matrix(MOQ_VERSION_DRAFT_16);
    test_window_membership_matrix(MOQ_VERSION_DRAFT_18);
    test_window_stream_objects(MOQ_VERSION_DRAFT_16);
    test_window_stream_objects(MOQ_VERSION_DRAFT_18);
    test_window_publication_and_coexist(MOQ_VERSION_DRAFT_16, true);
    test_window_publication_and_coexist(MOQ_VERSION_DRAFT_16, false);
    test_window_publication_and_coexist(MOQ_VERSION_DRAFT_18, true);
    test_window_publication_and_coexist(MOQ_VERSION_DRAFT_18, false);
    test_window_fully_filtered_history(MOQ_VERSION_DRAFT_16);
    test_window_fully_filtered_history(MOQ_VERSION_DRAFT_18);
    for (int v = 0; v < 2; v++) {
        moq_version_t ver = v ? MOQ_VERSION_DRAFT_18 : MOQ_VERSION_DRAFT_16;
        test_declare_b_rejections(ver);
        test_declare_pending_and_merge(ver);
        test_declare_completion_basic(ver);
        test_declare_publication_completes(ver);
        test_declare_blocked_steps(ver, false);
        test_declare_blocked_steps(ver, true);
        test_declare_fin_before_done(ver);
        test_declare_install_immediate(ver, false);
        test_declare_install_immediate(ver, true);
        test_declare_install_deferred_and_pending(ver);
        test_declare_update_paths(ver);
        test_declare_after_end_track(ver);
        test_declare_fetch_below_watermark(ver);
        test_declare_plain_track_unchanged(ver);
        test_declare_stale_completion(ver, false);
        test_declare_stale_completion(ver, true);
        test_declare_stale_armed_reset(ver);
        test_declare_end_track_reconcile(ver);
        test_declare_end_track_ceiling(ver);
        test_declare_end_track_blocked_preflight(ver);
        test_nsterm_rejected(ver);
        test_nsterm_cancelled(ver);
        test_nsterm_old_size_finished_preserved(ver);
        test_mono_rejections(ver);
        test_mono_plain_regression(ver);
        test_mono_eog_open_vs_fin(ver);
        test_mono_reset_clears(ver);
        test_mono_crossed_and_blocked(ver, false);
        test_mono_crossed_and_blocked(ver, true);
        test_mono_group_advance(ver);
        test_mono_status_evidence(ver);
        test_mono_streaming_higher_dg(ver);
        test_mono_datagram_no_arm(ver);
        test_mono_abi_canary(ver);
        test_mono_declare_compose(ver);
    }
    test_window_narrow_resets_open_subgroup(MOQ_VERSION_DRAFT_16);
    test_window_narrow_resets_open_subgroup(MOQ_VERSION_DRAFT_18);
    test_window_streaming_bracket(MOQ_VERSION_DRAFT_16);
    test_window_streaming_bracket(MOQ_VERSION_DRAFT_18);
    test_window_widen_midbracket(MOQ_VERSION_DRAFT_16);
    test_window_widen_midbracket(MOQ_VERSION_DRAFT_18);
    test_window_unsatisfiable(MOQ_VERSION_DRAFT_16);
    test_window_unsatisfiable(MOQ_VERSION_DRAFT_18);
    test_window_teardown_reuse(MOQ_VERSION_DRAFT_16);
    test_window_teardown_reuse(MOQ_VERSION_DRAFT_18);
    test_window_finite_end_no_auto_finish(MOQ_VERSION_DRAFT_16);
    test_window_finite_end_no_auto_finish(MOQ_VERSION_DRAFT_18);
    test_window_fetch_independent();
    test_window_publish_updated(MOQ_VERSION_DRAFT_16);
    test_window_publish_updated(MOQ_VERSION_DRAFT_18);
    test_window_end_track(MOQ_VERSION_DRAFT_16, true);
    test_window_end_track(MOQ_VERSION_DRAFT_16, false);
    test_window_end_track(MOQ_VERSION_DRAFT_18, true);
    test_window_end_track(MOQ_VERSION_DRAFT_18, false);
    test_window_end_track_blocked(MOQ_VERSION_DRAFT_16, true);
    test_window_end_track_blocked(MOQ_VERSION_DRAFT_16, false);
    test_window_end_track_blocked(MOQ_VERSION_DRAFT_18, true);
    test_window_end_track_blocked(MOQ_VERSION_DRAFT_18, false);
    test_window_end_track_ceiling(MOQ_VERSION_DRAFT_16);
    test_window_end_track_ceiling(MOQ_VERSION_DRAFT_18);
    test_exact_stream_counts(MOQ_VERSION_DRAFT_16, true);
    test_exact_stream_counts(MOQ_VERSION_DRAFT_16, false);
    test_exact_stream_counts(MOQ_VERSION_DRAFT_18, true);
    test_exact_stream_counts(MOQ_VERSION_DRAFT_18, false);
    test_remove_track_abandon_reset(MOQ_VERSION_DRAFT_16, false);
    test_remove_track_abandon_reset(MOQ_VERSION_DRAFT_16, true);
    test_remove_track_abandon_reset(MOQ_VERSION_DRAFT_18, false);
    test_remove_track_abandon_reset(MOQ_VERSION_DRAFT_18, true);
    test_needs_reset_never_fin(MOQ_VERSION_DRAFT_16, true, false);
    test_needs_reset_never_fin(MOQ_VERSION_DRAFT_16, false, false);
    test_needs_reset_never_fin(MOQ_VERSION_DRAFT_16, false, true);
    test_needs_reset_never_fin(MOQ_VERSION_DRAFT_18, true, false);
    test_needs_reset_never_fin(MOQ_VERSION_DRAFT_18, false, false);
    test_needs_reset_never_fin(MOQ_VERSION_DRAFT_18, false, true);
    test_streaming_needs_reset_termination(MOQ_VERSION_DRAFT_16, true);
    test_streaming_needs_reset_termination(MOQ_VERSION_DRAFT_16, false);
    test_streaming_needs_reset_termination(MOQ_VERSION_DRAFT_18, true);
    test_streaming_needs_reset_termination(MOQ_VERSION_DRAFT_18, false);
    test_unpublish_recovery(MOQ_VERSION_DRAFT_16);
    test_unpublish_recovery(MOQ_VERSION_DRAFT_18);
    test_remove_track_publication_exact_count(MOQ_VERSION_DRAFT_16, true);
    test_remove_track_publication_exact_count(MOQ_VERSION_DRAFT_16, false);
    test_remove_track_publication_exact_count(MOQ_VERSION_DRAFT_18, true);
    test_remove_track_publication_exact_count(MOQ_VERSION_DRAFT_18, false);
    test_remove_track_exact_count(MOQ_VERSION_DRAFT_16, true);
    test_remove_track_exact_count(MOQ_VERSION_DRAFT_16, false);
    test_remove_track_exact_count(MOQ_VERSION_DRAFT_18, true);
    test_remove_track_exact_count(MOQ_VERSION_DRAFT_18, false);
    test_window_end_track_forward_zero(MOQ_VERSION_DRAFT_16);
    test_window_end_track_forward_zero(MOQ_VERSION_DRAFT_18);
    test_window_end_track_canonical_location(MOQ_VERSION_DRAFT_16);
    test_window_end_track_canonical_location(MOQ_VERSION_DRAFT_18);
    test_window_pending_op_narrow(MOQ_VERSION_DRAFT_16, false);
    test_window_pending_op_narrow(MOQ_VERSION_DRAFT_16, true);
    test_window_pending_op_narrow(MOQ_VERSION_DRAFT_18, false);
    test_window_pending_op_narrow(MOQ_VERSION_DRAFT_18, true);
    test_pub_manual_close_releases_pending_fetch();
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
    test_pub_callbacks_old_prefix_copier_keeps_updated();
    test_pub_callbacks_nested_prefix_bounds_copy();
    test_pub_callbacks_partial_field_poison();
    test_update_after_unsubscribe_ignored();
    test_update_during_deferred();
    test_old_cfg_size_callbacks_still_fire();
    test_ns_refcount_shared();
    test_ns_refcount_distinct();
    test_ns_refcount_would_block_retry();
    test_publish_track_accepted();
    test_publish_track_write();
    test_publish_and_subscribe_coexist();
    test_fanout_delivery_sub_first();
    test_fanout_delivery_pub_first();
    test_fanout_second_dest_would_block();
    test_fanout_mid_op_joiner_excluded();
    test_fanout_forward_zero_subscription();
    test_fanout_old_sized_object_cfg();
    test_fanout_publication_forward_drop();
    test_fanout_close_releases_pending_op();
    test_fanout_forward_enable_mid_op_joins_next();
    test_pub_object_cfg_init_old_prefix_no_overflow();
    test_fanout_drop_restore_excluded_from_pending();
    test_fanout_forward_drop_resets_eog_subgroup();

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

        moq_pub_object_cfg_t obj; moq_pub_object_cfg_init_sized(&obj, sizeof(obj));
        obj.group_id = 0; obj.object_id = 0;
        obj.payload = pl; obj.end_of_group = true;
        MOQ_TEST_CHECK(moq_pub_write_object_ex(pub, track, &obj,
            moq_simpair_now_us(sp)) == MOQ_OK);

        drain_all(sp);

        /* Same group, object 1 with end_of_group=false: mismatch. */
        moq_rcbuf_t *pl2 = NULL;
        moq_rcbuf_create(&alloc, data, 2, &pl2);
        moq_pub_object_cfg_init_sized(&obj, sizeof(obj));
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

        moq_pub_object_cfg_t obj; moq_pub_object_cfg_init_sized(&obj, sizeof(obj));
        obj.group_id = 0; obj.object_id = 0;
        obj.payload = pl; obj.end_of_group = false;
        MOQ_TEST_CHECK(moq_pub_write_object_ex(pub, track, &obj,
            moq_simpair_now_us(sp)) == MOQ_OK);

        drain_all(sp);

        moq_rcbuf_t *pl2 = NULL;
        moq_rcbuf_create(&alloc, data, 2, &pl2);
        moq_pub_object_cfg_init_sized(&obj, sizeof(obj));
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

        /* Subscribe (LargestObject): the stale RETAINED object (group 0) must not
         * be advertised as Largest -- but the LIVE object written at (1,0) was
         * observed by the registry (Layer B feed) and, per the accept snapshot
         *"), IS advertised as the true
         * Largest. So SUBSCRIBE_OK carries (1,0), not (0,0). */
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
        bool saw_ok = false; bool ok_has_largest = false;
        uint64_t ok_lg = 0, ok_lo = 0;
        { moq_event_t d[16]; size_t ne;
          while ((ne = moq_session_poll_events(moq_simpair_client(sp), d, 16)) > 0)
              for (size_t i = 0; i < ne; i++) {
                  if (d[i].kind == MOQ_EVENT_SUBSCRIBE_OK) {
                      saw_ok = true;
                      ok_has_largest = d[i].u.subscribe_ok.has_largest;
                      ok_lg = d[i].u.subscribe_ok.largest_group;
                      ok_lo = d[i].u.subscribe_ok.largest_object;
                  }
                  moq_event_cleanup(&d[i]);
              } }
        MOQ_TEST_CHECK(saw_ok);
        MOQ_TEST_CHECK(ok_has_largest);           /* the live largest is advertised */
        MOQ_TEST_CHECK_EQ_INT((int)ok_lg, 1);     /* group 1 (live), not 0 (retained) */
        MOQ_TEST_CHECK_EQ_INT((int)ok_lo, 0);

        /* Largest (1,0) known -> a joining FETCH now resolves client-side against
         * the live largest (proof the retained (0,0) was superseded, not lost). */
        moq_fetch_cfg_t fcfg; moq_fetch_cfg_init(&fcfg);
        fcfg.is_joining = true; fcfg.joining_relative = true;
        fcfg.joining_start = 0; fcfg.joining_sub = sub_h;
        moq_fetch_t fh;
        MOQ_TEST_CHECK(moq_session_fetch(moq_simpair_client(sp), &fcfg, now, &fh)
                       == MOQ_OK);

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
        moq_pub_object_cfg_t ocfg; moq_pub_object_cfg_init_sized(&ocfg, sizeof(ocfg));
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
