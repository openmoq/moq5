#include "test_cases.h"

void test_publish_track_accepted(void) {
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

void test_publish_track_write(void) {
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

    /* end_track on a published track ends it: END_OF_TRACK on its own stream,
     * then PUBLISH_DONE counting both streams. */
    rc = end_track_virtual(sp, pub, track);
    MOQ_TEST_CHECK(rc == MOQ_OK);
    MOQ_TEST_CHECK(!moq_pub_track_is_published(pub, track));
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_U64(client_publication_done_status(sp, 2, true), 0x2);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("publish_track_write");
}


/* end_track on a published track ends it on the data plane before the
 * control plane (draft-16 10.2.1.1, draft-18 and draft-22 11.1.1: Object
 * Status 0x4, End of Track): after the published Objects, one empty
 * END_OF_TRACK status object at the track-wide terminal Location {last
 * group + 1, 0} on its own FIN'd stream, then exactly one PUBLISH_DONE
 * (TRACK_ENDED) whose Stream Count includes that stream. With a small action
 * queue every phase can refuse for room; end_track is retried until it
 * succeeds, and nothing is duplicated. */
static void test_publish_end_track_eot_ver(moq_version_t ver, uint32_t max_actions)
{
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    moq_simpair_t *sp = NULL;
    moq_simpair_cfg_t scfg = MOQ_SIMPAIR_CFG_INIT;
    scfg.alloc = &alloc;
    scfg.seed = 42;
    scfg.initial_now_us = 1000;
    scfg.client_send_request_capacity = true;
    scfg.client_initial_request_capacity = 16;
    scfg.server_send_request_capacity = true;
    scfg.server_initial_request_capacity = 16;
    scfg.version = ver;
    scfg.max_actions = max_actions;
    MOQ_TEST_CHECK(moq_simpair_create(&scfg, &sp) == MOQ_OK);
    moq_simpair_start(sp);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    drain_all(sp);

    moq_pub_cfg_t cfg; moq_pub_cfg_init(&cfg);
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    moq_publisher_t *pub = NULL;
    MOQ_TEST_CHECK(moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub) == MOQ_OK);
    moq_pub_track_cfg_t tcfg; moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    tcfg.track_namespace.parts = ns_parts; tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_pub_track_t *track = NULL;
    MOQ_TEST_CHECK(moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &track) == MOQ_OK);
    moq_pub_publish_cfg_t pcfg; moq_pub_publish_cfg_init(&pcfg);
    MOQ_TEST_CHECK(moq_pub_publish_track(pub, track, &pcfg, moq_simpair_now_us(sp)) == MOQ_OK);
    bool accepted = false;
    for (int i = 0; i < 16 && !accepted; i++) {
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        accepted = client_accept_publish(sp);
    }
    MOQ_TEST_CHECK(accepted);
    for (int i = 0; i < 16 && !moq_pub_track_is_published(pub, track); i++) {
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        (void)moq_pub_tick(pub, moq_simpair_now_us(sp));
    }
    MOQ_TEST_CHECK(moq_pub_track_is_published(pub, track));

    /* Group 0: objects 0, 1; group 1: object 0 (each group its own stream). */
    static const uint64_t locs[3][2] = { { 0, 0 }, { 0, 1 }, { 1, 0 } };
    for (int k = 0; k < 3; k++) {
        moq_rcbuf_t *payload = NULL;
        uint8_t b[3] = { 0xE0, (uint8_t)locs[k][0], (uint8_t)locs[k][1] };
        moq_rcbuf_create(&alloc, b, sizeof(b), &payload);
        moq_result_t rc = MOQ_ERR_WOULD_BLOCK;
        for (int i = 0; i < 64 && rc == MOQ_ERR_WOULD_BLOCK; i++) {
            rc = moq_pub_write_object(pub, track, locs[k][0], locs[k][1], payload,
                                      moq_simpair_now_us(sp));
            if (rc == MOQ_ERR_WOULD_BLOCK) moq_simpair_run_until_quiescent(sp, 8, NULL);
        }
        MOQ_TEST_CHECK_EQ_INT(rc, MOQ_OK);
        moq_rcbuf_decref(payload);
    }
    moq_result_t rc = MOQ_ERR_WOULD_BLOCK;
    int tries = 0;
    for (; tries < 64 && rc == MOQ_ERR_WOULD_BLOCK; tries++) {
        rc = moq_pub_end_track(pub, track, moq_simpair_now_us(sp));
        if (rc == MOQ_ERR_WOULD_BLOCK) moq_simpair_run_until_quiescent(sp, 8, NULL);
    }
    MOQ_TEST_CHECK_EQ_INT(rc, MOQ_OK);
    MOQ_TEST_CHECK(!moq_pub_track_is_published(pub, track));
    MOQ_TEST_CHECK_EQ_INT(moq_pub_end_track(pub, track, moq_simpair_now_us(sp)), MOQ_OK);

    int data = 0, eot = 0, done = 0, eot_before_done = 0, fins_g2 = 0;
    uint64_t done_status = ~0ull, done_count = ~0ull, eot_g = ~0ull, eot_o = ~0ull;
    for (int round = 0; round < 32; round++) {
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        moq_event_t d[16]; size_t ne;
        while ((ne = moq_session_poll_events(moq_simpair_client(sp), d, 16)) > 0)
            for (size_t i = 0; i < ne; i++) {
                if (d[i].kind == MOQ_EVENT_OBJECT_RECEIVED) {
                    const moq_object_received_event_t *o = &d[i].u.object_received;
                    if (o->status == MOQ_OBJECT_END_OF_TRACK) {
                        eot++; eot_g = o->group_id; eot_o = o->object_id;
                        MOQ_TEST_CHECK(o->payload == NULL);
                        if (done == 0) eot_before_done++;
                    } else if (o->status == MOQ_OBJECT_NORMAL) {
                        data++;
                        MOQ_TEST_CHECK(eot == 0);   /* data before the terminal */
                    }
                } else if (d[i].kind == MOQ_EVENT_SUBGROUP_FINISHED) {
                    if (d[i].u.subgroup_finished.group_id == 2) fins_g2++;
                } else if (d[i].kind == MOQ_EVENT_PUBLISH_FINISHED) {
                    done++;
                    done_status = d[i].u.publish_finished.status_code;
                    done_count = d[i].u.publish_finished.stream_count;
                }
                moq_event_cleanup(&d[i]);
            }
    }
    printf("publish_end_track_eot v%d aq=%u: tries=%d data=%d eot=%d at %llu/%llu "
           "fins_g2=%d done=%d status=0x%llx count=%llu eot_before_done=%d\n",
           (int)ver, max_actions, tries, data, eot, (unsigned long long)eot_g,
           (unsigned long long)eot_o, fins_g2, done, (unsigned long long)done_status,
           (unsigned long long)done_count, eot_before_done);
    MOQ_TEST_CHECK_EQ_INT(data, 3);
    MOQ_TEST_CHECK_EQ_INT(eot, 1);
    MOQ_TEST_CHECK_EQ_U64(eot_g, 2);
    MOQ_TEST_CHECK_EQ_U64(eot_o, 0);
    MOQ_TEST_CHECK_EQ_INT(fins_g2, 1);
    MOQ_TEST_CHECK_EQ_INT(done, 1);
    MOQ_TEST_CHECK_EQ_U64(done_status, 0x2);
    MOQ_TEST_CHECK_EQ_U64(done_count, 3);
    MOQ_TEST_CHECK_EQ_INT(eot_before_done, 1);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* end_track's terminal per request kind and boundary: a publication and/or
 * a subscription of the same track; the END_OF_TRACK goes to each request
 * the operation targets, at the track-wide terminal Location, unless no
 * fresh Location exists (the varint ceiling) or the publication asked for
 * nothing (Forward 0): those end by control message alone. */
#define ET_PUB     1u
#define ET_SUB     2u
#define ET_FWD0    4u
#define ET_EMPTY   8u
#define ET_CEILING 16u
static void test_end_track_terminal_case(moq_version_t ver, uint32_t aq, unsigned f)
{
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    moq_simpair_t *sp = NULL;
    moq_simpair_cfg_t scfg = MOQ_SIMPAIR_CFG_INIT;
    scfg.alloc = &alloc;
    scfg.seed = 42;
    scfg.initial_now_us = 1000;
    scfg.client_send_request_capacity = true;
    scfg.client_initial_request_capacity = 16;
    scfg.server_send_request_capacity = true;
    scfg.server_initial_request_capacity = 16;
    scfg.version = ver;
    scfg.max_actions = aq;
    MOQ_TEST_CHECK(moq_simpair_create(&scfg, &sp) == MOQ_OK);
    moq_simpair_start(sp);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    drain_all(sp);
    moq_pub_cfg_t cfg; moq_pub_cfg_init(&cfg);
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    moq_publisher_t *pub = NULL;
    MOQ_TEST_CHECK(moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub) == MOQ_OK);
    moq_pub_track_cfg_t tcfg; moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    tcfg.track_namespace.parts = ns_parts; tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_pub_track_t *track = NULL;
    MOQ_TEST_CHECK(moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &track) == MOQ_OK);
    if (f & ET_PUB) {
        moq_pub_publish_cfg_t pcfg; moq_pub_publish_cfg_init(&pcfg);
        MOQ_TEST_CHECK(moq_pub_publish_track(pub, track, &pcfg, moq_simpair_now_us(sp)) == MOQ_OK);
        bool accepted = false;
        for (int i = 0; i < 16 && !accepted; i++) {
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            moq_event_t ev;
            while (!accepted && moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1) {
                if (ev.kind == MOQ_EVENT_PUBLISH_REQUEST) {
                    moq_accept_publish_cfg_t acc;
                    moq_accept_publish_cfg_init_sized(&acc, sizeof(acc));
                    if (f & ET_FWD0) { acc.has_forward = true; acc.forward = false; }
                    MOQ_TEST_CHECK(moq_session_accept_publish(moq_simpair_client(sp),
                        ev.u.publish_request.pub, &acc, moq_simpair_now_us(sp)) == MOQ_OK);
                    accepted = true;
                }
                moq_event_cleanup(&ev);
            }
        }
        MOQ_TEST_CHECK(accepted);
        for (int i = 0; i < 16 && !moq_pub_track_is_published(pub, track); i++) {
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            (void)moq_pub_tick(pub, moq_simpair_now_us(sp));
        }
        MOQ_TEST_CHECK(moq_pub_track_is_published(pub, track));
    }
    if (f & ET_SUB) {
        moq_subscribe_cfg_t sc; moq_subscribe_cfg_init(&sc);
        sc.track_namespace.parts = ns_parts; sc.track_namespace.count = 1;
        sc.track_name = MOQ_BYTES_LITERAL("video");
        moq_subscription_t sh;
        MOQ_TEST_CHECK(moq_session_subscribe(moq_simpair_client(sp), &sc,
                                             moq_simpair_now_us(sp), &sh) == MOQ_OK);
        for (int i = 0; i < 16 && !moq_pub_has_subscriber(pub, track); i++) {
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            (void)moq_pub_tick(pub, moq_simpair_now_us(sp));
        }
        MOQ_TEST_CHECK(moq_pub_has_subscriber(pub, track));
    }
    uint64_t locs[3][2] = { { 0, 0 }, { 0, 1 }, { 1, 0 } };
    int nlocs = (f & ET_EMPTY) ? 0 : 3;
    if (f & ET_CEILING) { locs[0][0] = MOQ_QUIC_VARINT_MAX; locs[0][1] = 0; nlocs = 1; }
    for (int k = 0; k < nlocs; k++) {
        moq_rcbuf_t *payload = NULL;
        uint8_t b[3] = { 0xE0, (uint8_t)locs[k][0], (uint8_t)locs[k][1] };
        moq_rcbuf_create(&alloc, b, sizeof(b), &payload);
        moq_result_t rc = MOQ_ERR_WOULD_BLOCK;
        for (int i = 0; i < 64 && rc == MOQ_ERR_WOULD_BLOCK; i++) {
            rc = moq_pub_write_object(pub, track, locs[k][0], locs[k][1], payload,
                                      moq_simpair_now_us(sp));
            if (rc == MOQ_ERR_WOULD_BLOCK) moq_simpair_run_until_quiescent(sp, 8, NULL);
        }
        MOQ_TEST_CHECK_EQ_INT(rc, MOQ_OK);
        moq_rcbuf_decref(payload);
    }
    moq_result_t rc = MOQ_ERR_WOULD_BLOCK;
    int tries = 0;
    for (; tries < 64 && rc == MOQ_ERR_WOULD_BLOCK; tries++) {
        rc = moq_pub_end_track(pub, track, moq_simpair_now_us(sp));
        if (rc == MOQ_ERR_WOULD_BLOCK) moq_simpair_run_until_quiescent(sp, 8, NULL);
    }
    MOQ_TEST_CHECK_EQ_INT(rc, MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(moq_pub_end_track(pub, track, moq_simpair_now_us(sp)), MOQ_OK);

    int pdata = 0, sdata = 0, peot = 0, seot = 0, pdone = 0, sdone = 0, late = 0;
    uint64_t pcount = ~0ull, scount = ~0ull, eg = ~0ull, eo = ~0ull, pst = ~0ull, sst = ~0ull;
    for (int round = 0; round < 32; round++) {
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        moq_event_t d[16]; size_t ne;
        while ((ne = moq_session_poll_events(moq_simpair_client(sp), d, 16)) > 0)
            for (size_t i = 0; i < ne; i++) {
                if (d[i].kind == MOQ_EVENT_OBJECT_RECEIVED) {
                    const moq_object_received_event_t *o = &d[i].u.object_received;
                    bool on_pub = o->pub._opaque != 0;
                    if (o->status == MOQ_OBJECT_END_OF_TRACK) {
                        if (on_pub) { peot++; late += pdone; } else { seot++; late += sdone; }
                        eg = o->group_id; eo = o->object_id;
                    } else if (on_pub) {
                        pdata++;
                    } else {
                        sdata++;
                    }
                } else if (d[i].kind == MOQ_EVENT_PUBLISH_FINISHED) {
                    pdone++; pcount = d[i].u.publish_finished.stream_count;
                    pst = d[i].u.publish_finished.status_code;
                } else if (d[i].kind == MOQ_EVENT_SUBSCRIBE_DONE) {
                    sdone++; scount = d[i].u.subscribe_done.stream_count;
                    sst = d[i].u.subscribe_done.status_code;
                }
                moq_event_cleanup(&d[i]);
            }
    }
    bool fresh = !(f & ET_CEILING);
    uint64_t data_streams = (f & ET_EMPTY) ? 0 : (f & ET_CEILING) ? 1 : 2;
    uint64_t want_g = (f & ET_EMPTY) ? 0 : 2;
    printf("end_track_terminal v%d aq=%u f=%u: tries=%d pub data=%d eot=%d done=%d count=%llu "
           "st=0x%llx | sub data=%d eot=%d done=%d count=%llu st=0x%llx | eot at %llu/%llu late=%d\n",
           (int)ver, aq, f, tries, pdata, peot, pdone, (unsigned long long)pcount,
           (unsigned long long)pst, sdata, seot, sdone, (unsigned long long)scount,
           (unsigned long long)sst, (unsigned long long)eg, (unsigned long long)eo, late);
    MOQ_TEST_CHECK_EQ_INT(late, 0);
    if (f & ET_PUB) {
        bool fwd = !(f & ET_FWD0);
        MOQ_TEST_CHECK_EQ_INT(pdata, fwd ? nlocs : 0);
        MOQ_TEST_CHECK_EQ_INT(peot, fwd && fresh ? 1 : 0);
        MOQ_TEST_CHECK_EQ_INT(pdone, 1);
        MOQ_TEST_CHECK_EQ_U64(pst, 0x2);
        MOQ_TEST_CHECK_EQ_U64(pcount, fwd ? data_streams + (fresh ? 1 : 0) : 0);
    } else {
        MOQ_TEST_CHECK_EQ_INT(pdone + peot + pdata, 0);
    }
    if (f & ET_SUB) {
        MOQ_TEST_CHECK_EQ_INT(sdata, nlocs);
        MOQ_TEST_CHECK_EQ_INT(seot, fresh ? 1 : 0);
        MOQ_TEST_CHECK_EQ_INT(sdone, fresh ? 0 : 1);
        if (!fresh) MOQ_TEST_CHECK_EQ_U64(scount, data_streams);
    }
    if (fresh && (peot + seot) > 0) {
        MOQ_TEST_CHECK_EQ_U64(eg, want_g);
        MOQ_TEST_CHECK_EQ_U64(eo, 0);
    }
    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* The d22 filter narrowing of test_end_track_cut_case, observed rather than
 * inferred: whether the publisher accepted the update (the subscriber's
 * SUBSCRIPTION_UPDATE_OK), when the narrowed window was installed on the
 * subscription's destination slot, and whether the slot's terminal stream
 * had begun by then. */
bool moq_pub_test_sub_slot(moq_pub_track_t *t, moq_resolved_window_t *window,
                           bool *eot_sg, bool *eot_done, bool *sg_open);

typedef struct {
    bool     acked, installed, began_before_install, ended_before_install;
    int      ack_at, install_at, end_ok_at, first_eot_at;
    int      peot, seot, pdone, sdone, seot_fin;
    uint64_t pcount, scount;
} fc_obs_t;

static void fc_sample(moq_pub_track_t *track, int at, fc_obs_t *o)
{
    moq_resolved_window_t w;
    bool sg = false, done = false, open = false;
    if (!o->installed && moq_pub_test_sub_slot(track, &w, &sg, &done, &open) &&
        w.has_window && w.has_end && w.end_group == 0) {
        o->installed = true;
        o->install_at = at;
        o->began_before_install = sg;
        o->ended_before_install = done;
    }
}

static void fc_poll(moq_simpair_t *sp, int at, fc_obs_t *o)
{
    moq_event_t d[16]; size_t ne;
    while ((ne = moq_session_poll_events(moq_simpair_client(sp), d, 16)) > 0)
        for (size_t i = 0; i < ne; i++) {
            if (d[i].kind == MOQ_EVENT_SUBSCRIPTION_UPDATE_OK && !o->acked) {
                o->acked = true;
                o->ack_at = at;
            } else if (d[i].kind == MOQ_EVENT_OBJECT_RECEIVED &&
                       d[i].u.object_received.status == MOQ_OBJECT_END_OF_TRACK) {
                if (d[i].u.object_received.pub._opaque != 0) {
                    o->peot++;
                } else {
                    o->seot++;
                    if (o->first_eot_at < 0) o->first_eot_at = at;
                }
            } else if (d[i].kind == MOQ_EVENT_SUBGROUP_FINISHED &&
                       d[i].u.subgroup_finished.group_id == 2 &&
                       d[i].u.subgroup_finished.pub._opaque == 0) {
                o->seot_fin++;
            } else if (d[i].kind == MOQ_EVENT_PUBLISH_FINISHED) {
                o->pdone++;
                o->pcount = d[i].u.publish_finished.stream_count;
            } else if (d[i].kind == MOQ_EVENT_SUBSCRIBE_DONE) {
                o->sdone++;
                o->scount = d[i].u.subscribe_done.stream_count;
            }
            moq_event_cleanup(&d[i]);
        }
}

/* end_track blocked for room (a one- or two-action queue), then a processed
 * Forward 1 -> 0 (or, for the subscription, a filter narrowed to group 0) on
 * one request before the retry: at every step the end can be blocked at. A
 * terminal stream the cut requires resetting is reset, never FIN'd, and the
 * request still gets its terminal control message; the sibling request
 * ends normally. Exactly one PUBLISH_DONE ends the publication, with a
 * Stream Count equal to the streams it was opened. */
#define CUT_SUB    0
#define CUT_PUB    1
#define CUT_FILTER 2
static int test_end_track_cut_case(moq_version_t ver, uint32_t aq, int target, int k,
                                   fc_obs_t *obs)
{
    fc_obs_t local;
    fc_obs_t *o = obs ? obs : &local;
    memset(o, 0, sizeof(*o));
    o->ack_at = o->install_at = o->end_ok_at = o->first_eot_at = -1;
    int at = 0;
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    moq_simpair_t *sp = NULL;
    moq_simpair_cfg_t scfg = MOQ_SIMPAIR_CFG_INIT;
    scfg.alloc = &alloc;
    scfg.seed = 42;
    scfg.initial_now_us = 1000;
    scfg.client_send_request_capacity = true;
    scfg.client_initial_request_capacity = 16;
    scfg.server_send_request_capacity = true;
    scfg.server_initial_request_capacity = 16;
    scfg.version = ver;
    scfg.max_actions = aq;
    MOQ_TEST_CHECK(moq_simpair_create(&scfg, &sp) == MOQ_OK);
    moq_simpair_start(sp);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    drain_all(sp);
    moq_pub_cfg_t cfg; moq_pub_cfg_init(&cfg);
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    moq_publisher_t *pub = NULL;
    MOQ_TEST_CHECK(moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub) == MOQ_OK);
    moq_pub_track_cfg_t tcfg; moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    tcfg.track_namespace.parts = ns_parts; tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_pub_track_t *track = NULL;
    MOQ_TEST_CHECK(moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &track) == MOQ_OK);
    moq_pub_publish_cfg_t pcfg; moq_pub_publish_cfg_init(&pcfg);
    MOQ_TEST_CHECK(moq_pub_publish_track(pub, track, &pcfg, moq_simpair_now_us(sp)) == MOQ_OK);
    moq_publication_t cpub = {0};
    for (int i = 0; i < 16 && cpub._opaque == 0; i++) {
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        moq_event_t ev;
        while (cpub._opaque == 0 && moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_PUBLISH_REQUEST) {
                moq_accept_publish_cfg_t acc;
                moq_accept_publish_cfg_init(&acc);
                cpub = ev.u.publish_request.pub;
                MOQ_TEST_CHECK(moq_session_accept_publish(moq_simpair_client(sp), cpub, &acc,
                                                          moq_simpair_now_us(sp)) == MOQ_OK);
            }
            moq_event_cleanup(&ev);
        }
    }
    for (int i = 0; i < 16 && !moq_pub_track_is_published(pub, track); i++) {
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        (void)moq_pub_tick(pub, moq_simpair_now_us(sp));
    }
    moq_subscribe_cfg_t sc; moq_subscribe_cfg_init(&sc);
    sc.track_namespace.parts = ns_parts; sc.track_namespace.count = 1;
    sc.track_name = MOQ_BYTES_LITERAL("video");
    moq_subscription_t sh;
    MOQ_TEST_CHECK(moq_session_subscribe(moq_simpair_client(sp), &sc, moq_simpair_now_us(sp),
                                         &sh) == MOQ_OK);
    for (int i = 0; i < 16 && !moq_pub_has_subscriber(pub, track); i++) {
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        (void)moq_pub_tick(pub, moq_simpair_now_us(sp));
    }
    MOQ_TEST_CHECK(moq_pub_track_is_published(pub, track) && moq_pub_has_subscriber(pub, track));
    static const uint64_t locs[3][2] = { { 0, 0 }, { 0, 1 }, { 1, 0 } };
    for (int j = 0; j < 3; j++) {
        moq_rcbuf_t *payload = NULL;
        uint8_t b[3] = { 0xE0, (uint8_t)locs[j][0], (uint8_t)locs[j][1] };
        moq_rcbuf_create(&alloc, b, sizeof(b), &payload);
        moq_result_t rc = MOQ_ERR_WOULD_BLOCK;
        for (int i = 0; i < 64 && rc == MOQ_ERR_WOULD_BLOCK; i++) {
            rc = moq_pub_write_object(pub, track, locs[j][0], locs[j][1], payload,
                                      moq_simpair_now_us(sp));
            if (rc == MOQ_ERR_WOULD_BLOCK) moq_simpair_run_until_quiescent(sp, 8, NULL);
        }
        MOQ_TEST_CHECK_EQ_INT(rc, MOQ_OK);
        moq_rcbuf_decref(payload);
    }
    /* Block the end k times, then cut. */
    moq_result_t rc = MOQ_ERR_WOULD_BLOCK;
    int blocked = 0;
    while (blocked < k && rc == MOQ_ERR_WOULD_BLOCK) {
        rc = moq_pub_end_track(pub, track, moq_simpair_now_us(sp));
        if (rc == MOQ_ERR_WOULD_BLOCK) {
            blocked++;
            if (blocked < k) moq_simpair_run_until_quiescent(sp, 8, NULL);
        }
    }
    bool cut = rc == MOQ_ERR_WOULD_BLOCK;
    if (cut) {
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        moq_result_t urc;
        if (target == CUT_PUB) {
            moq_publication_update_cfg_t up;
            moq_publication_update_cfg_init(&up);
            up.has_forward = true;
            up.forward = false;
            urc = moq_session_update_publication(moq_simpair_client(sp), cpub, &up,
                                                 moq_simpair_now_us(sp));
        } else {
            moq_subscription_update_cfg_t uc;
            moq_subscription_update_cfg_init_sized(&uc, sizeof(uc));
            if (target == CUT_FILTER) {
                uc.has_filter = true;
                uc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE;
                uc.start_group = 0;
                uc.start_object = 0;
                uc.end_group = 0;
            } else {
                uc.has_forward = true;
                uc.forward = false;
            }
            urc = moq_session_update_subscription(moq_simpair_client(sp), sh, &uc,
                                                  moq_simpair_now_us(sp));
        }
        if (urc != MOQ_OK) cut = false;   /* not expressible on this draft */
        /* The subscriber's events are taken as they arrive, so the update's
         * acknowledgement is ordered against the terminal; the slot is sampled
         * after every step for the window the update installed. */
        for (int i = 0; i < 16; i++) {
            at++;
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            (void)moq_pub_tick(pub, moq_simpair_now_us(sp));
            fc_sample(track, at, o);
            fc_poll(sp, at, o);
        }
        for (int i = 0; i < 64 && rc == MOQ_ERR_WOULD_BLOCK; i++) {
            at++;
            fc_sample(track, at, o);
            rc = moq_pub_end_track(pub, track, moq_simpair_now_us(sp));
            if (rc == MOQ_ERR_WOULD_BLOCK) {
                moq_simpair_run_until_quiescent(sp, 8, NULL);
                (void)moq_pub_tick(pub, moq_simpair_now_us(sp));
                fc_sample(track, at, o);
                fc_poll(sp, at, o);
            }
        }
    }
    MOQ_TEST_CHECK_EQ_INT(rc, MOQ_OK);
    o->end_ok_at = at;
    for (int round = 0; round < 32; round++) {
        at++;
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        (void)moq_pub_tick(pub, moq_simpair_now_us(sp));
        fc_poll(sp, at, o);
    }
    int peot = o->peot, seot = o->seot, pdone = o->pdone, sdone = o->sdone;
    int seot_fin = o->seot_fin;
    uint64_t pcount = o->pcount, scount = o->scount;
    int bad = 0;
    /* The subscription always reaches a terminal: its completed END_OF_TRACK
     * (FIN'd) or its done. */
    if (!((seot == 1 && seot_fin == 1 && sdone == 0) || sdone == 1)) bad++;
    if (pdone != 1 || peot > 1) bad++;
    if (!cut && (seot != 1 || peot != 1)) bad++;
    if (bad || cut)
        printf("end_track_cut v%d aq=%u target=%d k=%d cut=%d: pub eot=%d done=%d count=%llu | "
               "sub eot=%d fin=%d done=%d count=%llu%s\n", (int)ver, aq, target, k, (int)cut,
               peot, pdone, (unsigned long long)pcount, seot, seot_fin, sdone,
               (unsigned long long)scount, bad ? "  <-- BAD" : "");
    MOQ_TEST_CHECK_EQ_INT(bad, 0);
    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    return cut ? 1 : 0;
}

void test_end_track_cut(void)
{
    static const moq_version_t vers[] = { MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18,
                                          MOQ_VERSION_DRAFT_22 };
    int cuts = 0;
    for (size_t v = 0; v < 3; v++)
        for (uint32_t aq = 1; aq <= 2; aq++)
            for (int target = 0; target < 3; target++)
                for (int k = 1; k <= 8; k++)
                    cuts += test_end_track_cut_case(vers[v], aq, target, k, NULL);
    printf("end_track_cut: %d cut schedules\n", cuts);
    MOQ_TEST_CHECK(cuts > 0);
    MOQ_TEST_PASS("end_track_cut");
}

/* The filter narrowing of test_end_track_cut_case, observed on every draft:
 * the update is processed and its window [0:0, group 0] installed on the
 * subscription's slot before the end completes, so the terminal at group 2
 * is outside the window. A terminal not yet begun is never sent there (the
 * subscription ends with its done, Stream Count 2); one begun before the
 * update was processed is reset, never FIN'd (Stream Count 3). Where the end
 * was not blocked, nothing is cut and the terminal arrives as usual. */
void test_end_track_filter_cut(void)
{
    static const moq_version_t vers[] = { MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18,
                                          MOQ_VERSION_DRAFT_22 };
    int cuts = 0, begun = 0;
    for (size_t v = 0; v < 3; v++)
        for (uint32_t aq = 1; aq <= 2; aq++)
            for (int k = 1; k <= 8; k++) {
                fc_obs_t o;
                int cut = test_end_track_cut_case(vers[v], aq, CUT_FILTER, k, &o);
                int before = failures;
                if (cut) {
                    cuts++;
                    begun += o.began_before_install;
                    MOQ_TEST_CHECK(o.installed && o.install_at <= o.end_ok_at);
                    MOQ_TEST_CHECK(!o.ended_before_install);
                    MOQ_TEST_CHECK_EQ_INT(o.seot, 0);
                    MOQ_TEST_CHECK_EQ_INT(o.seot_fin, 0);
                    MOQ_TEST_CHECK_EQ_INT(o.sdone, 1);
                    MOQ_TEST_CHECK_EQ_U64(o.scount, o.began_before_install ? 3 : 2);
                    if (vers[v] == MOQ_VERSION_DRAFT_22)
                        MOQ_TEST_CHECK(o.acked && o.ack_at <= o.end_ok_at);
                } else {
                    MOQ_TEST_CHECK_EQ_INT(o.seot, 1);
                    MOQ_TEST_CHECK_EQ_INT(o.seot_fin, 1);
                    MOQ_TEST_CHECK_EQ_INT(o.sdone, 0);
                }
                if (failures != before)
                    printf("filter_cut v%d aq=%u k=%d cut=%d acked=%d@%d installed=%d@%d "
                           "began=%d end_ok@%d | sub eot=%d fin=%d done=%d count=%llu\n",
                           (int)vers[v], aq, k, cut, (int)o.acked, o.ack_at,
                           (int)o.installed, o.install_at, (int)o.began_before_install,
                           o.end_ok_at, o.seot, o.seot_fin, o.sdone,
                           (unsigned long long)o.scount);
            }
    printf("end_track_filter_cut: %d cut schedules, %d with the terminal begun\n",
           cuts, begun);
    MOQ_TEST_CHECK(cuts > 0 && begun > 0 && begun < cuts);
    MOQ_TEST_PASS("end_track_filter_cut");
}

void test_publish_end_track_eot(void)
{
    static const moq_version_t vers[] = { MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18,
                                          MOQ_VERSION_DRAFT_22 };
    static const uint32_t aqs[] = { 0, 1, 2, 3, 4, 5 };
    for (size_t v = 0; v < 3; v++)
        for (size_t a = 0; a < sizeof(aqs) / sizeof(aqs[0]); a++)
            test_publish_end_track_eot_ver(vers[v], aqs[a]);
    static const unsigned cases[] = {
        ET_PUB | ET_EMPTY, ET_PUB | ET_CEILING, ET_PUB | ET_FWD0, ET_SUB,
        ET_PUB | ET_SUB, ET_PUB | ET_SUB | ET_CEILING,
    };
    for (size_t v = 0; v < 3; v++)
        for (size_t a = 0; a < sizeof(aqs) / sizeof(aqs[0]); a++)
            for (size_t c = 0; c < sizeof(cases) / sizeof(cases[0]); c++) {
                /* Draft-22 PUBLISH_OK carries no Forward (its 9.3). */
                if ((cases[c] & ET_FWD0) && vers[v] == MOQ_VERSION_DRAFT_22) continue;
                test_end_track_terminal_case(vers[v], aqs[a], cases[c]);
            }
    MOQ_TEST_PASS("publish_end_track_eot");
}

/* Coexistence: one track is BOTH advertised (receives SUBSCRIBE) AND published.
 * Proves PUBLISH is an operation, not an exclusive mode. */
void test_publish_and_subscribe_coexist(void) {
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
                ref = e->rs.ref;
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

void test_publish_restart_after_peer_unsubscribe_d16(void)
{ publish_restart_case(false, "publish_restart_after_peer_unsubscribe_d16"); }
void test_publish_restart_d18_cancel(void)
{ publish_restart_case(true, "publish_restart_after_peer_cancel_d18"); }
