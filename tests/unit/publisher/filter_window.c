#include "test_cases.h"

/* A publisher-initiated publication the application ends normally --
 * end_track, unpublish_track or remove_track -- completes with TRACK_ENDED
 * (0x2) on every draft: the track is no longer being published. Status 0
 * is INTERNAL_ERROR and would mislabel that normal end as an error. The
 * subscriber sees exactly one completion message carrying the exact count
 * of data streams opened (one subgroup here). */
void test_publication_end_status(moq_version_t ver)
{
    for (int how = 0; how < 3; how++) {
        test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
        moq_publisher_t *pub; moq_pub_track_t *track;
        windows_setup(&as, &alloc, &sp, ver, &pub, &track);
        MOQ_TEST_CHECK(pub != NULL && track != NULL);
        if (!pub || !track) { moq_simpair_destroy(sp); continue; }
        moq_pub_publish_cfg_t pcfg; moq_pub_publish_cfg_init(&pcfg);
        MOQ_TEST_CHECK_EQ_INT(moq_pub_publish_track(pub, track, &pcfg,
            moq_simpair_now_us(sp)), MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        MOQ_TEST_CHECK(client_accept_publish(sp));
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        moq_pub_tick(pub, moq_simpair_now_us(sp));
        MOQ_TEST_CHECK(moq_pub_track_is_published(pub, track));
        moq_rcbuf_t *payload = NULL;
        MOQ_TEST_CHECK_EQ_INT(moq_rcbuf_create(&alloc, (const uint8_t *)"abcd",
                                               4, &payload), MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(moq_pub_write_object(pub, track, 0, 0, payload,
            moq_simpair_now_us(sp)), MOQ_OK);
        moq_rcbuf_decref(payload);
        moq_simpair_run_until_quiescent(sp, 8, NULL);

        uint64_t now = moq_simpair_now_us(sp);
        moq_result_t rc = how == 0 ? end_track_virtual(sp, pub, track)
                        : how == 1 ? moq_pub_unpublish_track(pub, track, now)
                                   : moq_pub_remove_track(pub, track, now);
        MOQ_TEST_CHECK_EQ_INT(rc, MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        /* Only end_track asserts the end of the track; unpublish and
         * remove_track end the publication alone. */
        MOQ_TEST_CHECK_EQ_U64(client_publication_done_status(sp, how == 0 ? 2 : 1,
                                                             how == 0), 0x2);

        moq_pub_destroy(pub);
        drain_all(sp);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
    }
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_16 ? "publication_end_status_d16"
                : ver == MOQ_VERSION_DRAFT_18 ? "publication_end_status_d18"
                                              : "publication_end_status_d22");
}

/* -- Membership matrix: all four filter forms x boundary locations ---- */

typedef struct {
    const char *label;
    moq_subscribe_filter_t filter;
    uint64_t sg, so, eg;   /* subscribe cfg values (ABSOLUTE_* forms) */
    bool seed;             /* pre-seed publisher registry with (7,2) */
    struct { uint64_t g, o; bool deliver; } probes[4];
} win_case_t;

void test_window_membership_matrix(moq_version_t ver)
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

void test_window_stream_objects(moq_version_t ver)
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

void test_window_publication_and_coexist(moq_version_t ver,
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

void test_window_fully_filtered_history(moq_version_t ver)
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

void test_window_narrow_resets_open_subgroup(moq_version_t ver)
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

void test_window_streaming_bracket(moq_version_t ver)
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

void test_window_widen_midbracket(moq_version_t ver)
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

void test_window_unsatisfiable(moq_version_t ver)
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

void test_window_teardown_reuse(moq_version_t ver)
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

void test_window_finite_end_no_auto_finish(moq_version_t ver)
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

void test_window_fetch_independent(void)
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
        moq_stream_ref_t ref = sv->publishes[slot].rs.ref;
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

typedef struct {
    bool armed;
    moq_stream_ref_t stream;
    int sends, fins, resets, other_streams;
} end_track_trace_t;

static void end_track_trace(void *ctx, const moq_sim_trace_record_t *r)
{
    end_track_trace_t *t = ctx;
    if (!t->armed || r->kind != MOQ_SIM_TRACE_ACTION ||
        r->from != MOQ_PERSPECTIVE_SERVER) return;
    MOQ_TEST_CHECK(r->struct_size >= offsetof(moq_sim_trace_record_t, fin) + sizeof(r->fin));
    if (r->action_kind == MOQ_ACTION_SEND_DATA) {
        if (t->sends == 0) t->stream = r->stream_ref;
        else if (t->stream._v != r->stream_ref._v) t->other_streams++;
        t->sends++;
        t->fins += r->fin;
    } else if (r->action_kind == MOQ_ACTION_RESET_DATA) {
        MOQ_TEST_CHECK_EQ_U64(r->stream_ref._v, t->stream._v);
        t->resets++;
    }
}

/* Stop or narrow after exactly the terminal header (phase 1), or after its
 * status object but before FIN (phase 2). The trace observes actual actions;
 * the peer observes the original parent handle and its exact stream count. */
void test_end_track_interrupted(moq_version_t ver, bool publication,
                                bool filter_cut, int phase)
{
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    end_track_trace_t trace = {0};
    moq_simpair_cfg_t scfg = MOQ_SIMPAIR_CFG_INIT;
    scfg.alloc = &alloc;
    scfg.seed = 42;
    scfg.initial_now_us = 1000;
    scfg.client_send_request_capacity = scfg.server_send_request_capacity = true;
    scfg.client_initial_request_capacity = scfg.server_initial_request_capacity = 16;
    scfg.version = ver;
    scfg.max_actions = 1;
    scfg.trace_fn = end_track_trace;
    scfg.trace_ctx = &trace;
    moq_simpair_t *sp = NULL;
    MOQ_TEST_CHECK(moq_simpair_create(&scfg, &sp) == MOQ_OK);
    moq_simpair_start(sp);
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    drain_all(sp);
    moq_session_t *sv = moq_simpair_server(sp), *cl = moq_simpair_client(sp);
    moq_pub_cfg_t cfg; moq_pub_cfg_init(&cfg);
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    moq_publisher_t *pub = NULL;
    MOQ_TEST_CHECK(moq_pub_create(sv, &alloc, &cfg, &pub) == MOQ_OK);
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("live") };
    moq_pub_track_cfg_t tc; moq_pub_track_cfg_init(&tc);
    tc.track_namespace = (moq_namespace_t){ .parts = ns, .count = 1 };
    tc.track_name = MOQ_BYTES_LITERAL("video");
    moq_pub_track_t *track = NULL;
    MOQ_TEST_CHECK(moq_pub_add_track(pub, &tc, moq_simpair_now_us(sp), &track) == MOQ_OK);
    moq_publication_t cpub = {0}, spub = {0};
    moq_subscription_t sub = {0};
    if (publication) {
        moq_pub_publish_cfg_t pc; moq_pub_publish_cfg_init(&pc);
        MOQ_TEST_CHECK(moq_pub_publish_track(pub, track, &pc, moq_simpair_now_us(sp)) == MOQ_OK);
        for (int i = 0; i < 16 && !moq_pub_track_is_published(pub, track); i++) {
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            moq_event_t ev;
            while (moq_session_poll_events(cl, &ev, 1) == 1) {
                if (ev.kind == MOQ_EVENT_PUBLISH_REQUEST) {
                    cpub = ev.u.publish_request.pub;
                    moq_accept_publish_cfg_t ac; moq_accept_publish_cfg_init(&ac);
                    MOQ_TEST_CHECK(moq_session_accept_publish(cl, cpub, &ac,
                        moq_simpair_now_us(sp)) == MOQ_OK);
                }
                moq_event_cleanup(&ev);
            }
            while (moq_session_poll_events(sv, &ev, 1) == 1) {
                if (ev.kind == MOQ_EVENT_PUBLISH_OK) spub = ev.u.publish_ok.pub;
                moq_pub_event_result_t handled;
                MOQ_TEST_CHECK(moq_pub_handle_event(pub, &ev, moq_simpair_now_us(sp),
                                                     &handled) == MOQ_OK);
                moq_event_cleanup(&ev);
            }
        }
        MOQ_TEST_CHECK(moq_pub_track_is_published(pub, track) && spub._opaque != 0);
    } else {
        sub = windows_subscribe(sp, pub, MOQ_SUBSCRIBE_FILTER_NONE, 0, 0, 0);
        MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track), 1);
    }
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    drain_all(sp);
    trace.armed = true;
    for (int i = 0; i < phase; i++) {
        MOQ_TEST_CHECK(moq_pub_end_track(pub, track, moq_simpair_now_us(sp)) ==
                       MOQ_ERR_WOULD_BLOCK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
    }
    MOQ_TEST_CHECK_EQ_INT(trace.sends, phase);
    MOQ_TEST_CHECK_EQ_INT(trace.fins + trace.resets, 0);
    if (filter_cut) {
        if (ver == MOQ_VERSION_DRAFT_22) {
            moq_location_filter_cfg_t loc;
            moq_location_filter_cfg_init(&loc);
            loc.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;
            loc.start_object = 1;
            moq_publication_update_cfg_t up; moq_publication_update_cfg_init(&up);
            up.location = &loc;
            MOQ_TEST_CHECK(moq_session_update_publication(cl, cpub, &up,
                moq_simpair_now_us(sp)) == MOQ_OK);
        } else {
            windows_inject_pub_filter(sp, ver, 0, MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START,
                                       0, 1, 0);
        }
        for (int i = 0; i < 16; i++) {
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            moq_result_t trc = moq_pub_tick(pub, moq_simpair_now_us(sp));
            MOQ_TEST_CHECK(trc == MOQ_OK || trc == MOQ_ERR_WOULD_BLOCK);
        }
        const moq_resolved_window_t *w = moq_session_pub_resolved_window(sv, spub);
        MOQ_TEST_CHECK(w != NULL && w->has_window && w->start_group == 0 &&
                       w->start_object == 1);
    } else {
        MOQ_TEST_CHECK(moq_session_on_data_stop(sv, trace.stream, 0,
            moq_simpair_now_us(sp)) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        MOQ_TEST_CHECK_EQ_INT(trace.resets, 1);
    }
    /* Refusal for terminal-control room must retain the original destination. */
    test_session_fill_action_queue(sv);
    MOQ_TEST_CHECK(moq_pub_end_track(pub, track, moq_simpair_now_us(sp)) == MOQ_ERR_WOULD_BLOCK);
    if (publication) MOQ_TEST_CHECK(moq_pub_track_is_published(pub, track));
    else MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track), 1);
    manual_drain_actions(sv);
    moq_result_t rc = MOQ_ERR_WOULD_BLOCK;
    for (int i = 0; i < 32 && rc == MOQ_ERR_WOULD_BLOCK; i++) {
        rc = moq_pub_end_track(pub, track, moq_simpair_now_us(sp));
        moq_simpair_run_until_quiescent(sp, 8, NULL);
    }
    MOQ_TEST_CHECK_EQ_INT(rc, MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_end_track(pub, track, moq_simpair_now_us(sp)) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    int dones = 0, eot = 0, update_acks = 0;
    moq_event_t ev;
    while (moq_session_poll_events(cl, &ev, 1) == 1) {
        if (ev.kind == MOQ_EVENT_PUBLISH_FINISHED) {
            MOQ_TEST_CHECK(publication && moq_publication_eq(ev.u.publish_finished.pub, cpub));
            MOQ_TEST_CHECK_EQ_U64(ev.u.publish_finished.status_code, 0x2);
            MOQ_TEST_CHECK_EQ_U64(ev.u.publish_finished.stream_count, 1);
            dones++;
        } else if (ev.kind == MOQ_EVENT_SUBSCRIBE_DONE) {
            MOQ_TEST_CHECK(!publication && moq_subscription_eq(ev.u.subscribe_done.sub, sub));
            MOQ_TEST_CHECK_EQ_U64(ev.u.subscribe_done.status_code, 0x2);
            MOQ_TEST_CHECK_EQ_U64(ev.u.subscribe_done.stream_count, 1);
            dones++;
        } else if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) {
            MOQ_TEST_CHECK(ev.u.object_received.status == MOQ_OBJECT_END_OF_TRACK);
            eot++;
        } else if (ev.kind == MOQ_EVENT_PUBLICATION_UPDATE_OK) {
            MOQ_TEST_CHECK(moq_publication_eq(ev.u.publication_update_ok.pub, cpub));
            update_acks++;
        } else if (ev.kind == MOQ_EVENT_SESSION_CLOSED) {
            MOQ_TEST_CHECK(false);
        }
        moq_event_cleanup(&ev);
    }
    printf("end_track_interrupted v%d pub=%d filter=%d phase=%d: sends=%d fins=%d "
           "resets=%d eot=%d done=%d update_acks=%d\n", (int)ver, publication, filter_cut,
           phase, trace.sends, trace.fins, trace.resets, eot, dones, update_acks);
    MOQ_TEST_CHECK_EQ_INT(dones, 1);
    MOQ_TEST_CHECK_EQ_INT(eot, phase - 1);
    MOQ_TEST_CHECK_EQ_INT(trace.sends, phase + (filter_cut ? 1 : 0));
    MOQ_TEST_CHECK_EQ_INT(trace.fins, filter_cut ? 1 : 0);
    MOQ_TEST_CHECK_EQ_INT(trace.resets, filter_cut ? 0 : 1);
    MOQ_TEST_CHECK_EQ_INT(trace.other_streams, 0);
    if (filter_cut && ver == MOQ_VERSION_DRAFT_22) MOQ_TEST_CHECK_EQ_INT(update_acks, 1);
    MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED &&
                   moq_session_state(cl) == MOQ_SESS_ESTABLISHED);
    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* -- Publication window narrow + widen via PUBLISH_UPDATED -------------- */

void test_window_publish_updated(moq_version_t ver)
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
void test_window_end_track(moq_version_t ver, bool excluded)
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

    MOQ_TEST_CHECK(end_track_virtual(sp, pub, track) == MOQ_OK);
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
        /* Deterministic terminal signal: exactly ONE done. Drafts 16 and 18
         * report SUBSCRIPTION_ENDED (0x3); draft-22 has no such status
         * (§16.11.3), and the track ended: TRACK_ENDED (0x2). */
        MOQ_TEST_CHECK_EQ_INT(ndone, 1);
        MOQ_TEST_CHECK_EQ_U64(done_status,
                              ver == MOQ_VERSION_DRAFT_22 ? 0x2 : 0x3);
        MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track), 0);
    }
    MOQ_TEST_CHECK(moq_session_state(moq_simpair_server(sp))
                   == MOQ_SESS_ESTABLISHED);
    /* Terminal + idempotent. */
    MOQ_TEST_CHECK(end_track_virtual(sp, pub, track) == MOQ_OK);

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
void test_window_end_track_blocked(moq_version_t ver, bool wrote_first)
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
void test_window_end_track_forward_zero(moq_version_t ver)
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
void test_window_end_track_canonical_location(moq_version_t ver)
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

        MOQ_TEST_CHECK(end_track_virtual(sp, pub, track) == MOQ_OK);
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

        MOQ_TEST_CHECK(end_track_virtual(sp, pub, track) == MOQ_OK);
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
void test_window_end_track_ceiling(moq_version_t ver)
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
void test_exact_stream_counts(moq_version_t ver, bool use_stream)
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
void test_remove_track_exact_count(moq_version_t ver, bool use_stream)
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
void test_remove_track_publication_exact_count(moq_version_t ver,
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
void test_remove_track_abandon_reset(moq_version_t ver, bool wire_shape)
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
void test_needs_reset_never_fin(moq_version_t ver, bool via_remove,
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
void test_streaming_needs_reset_termination(moq_version_t ver,
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
void test_unpublish_recovery(moq_version_t ver)
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
void test_window_pending_op_narrow(moq_version_t ver, bool narrow_pub)
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
