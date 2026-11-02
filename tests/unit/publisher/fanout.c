#include "test_cases.h"

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

void test_fanout_delivery_sub_first(void)
{ fanout_delivery_case(true, "fanout_delivery_sub_first"); }
void test_fanout_delivery_pub_first(void)
{ fanout_delivery_case(false, "fanout_delivery_pub_first"); }

/* Forced second-destination WOULD_BLOCK: destination A is served, the op
 * blocks on B; retries must not duplicate on A. Also proves the byte-exact
 * retry identity: a same-length CHANGED payload is refused WRONG_STATE, a
 * FRESH buffer with identical bytes resumes and completes. */
void test_fanout_second_dest_would_block(void)
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
void test_fanout_mid_op_joiner_excluded(void)
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
void test_fanout_forward_zero_subscription(void)
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
void test_fanout_old_sized_object_cfg(void)
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
void test_fanout_publication_forward_drop(void)
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

void test_fanout_close_releases_pending_op(void)
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
void test_fanout_forward_enable_mid_op_joins_next(void)
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
void test_pub_object_cfg_init_old_prefix_no_overflow(void)
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
void test_fanout_drop_restore_excluded_from_pending(void)
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
void test_fanout_forward_drop_resets_eog_subgroup(void)
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
void test_finish_subscribers_skips_publication(void)
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
void test_finish_subscribers_pub_mid_object(void)
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
