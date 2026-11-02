#include "test_cases.h"

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

/* -- A enforcement: decreasing / post-EOG reject; same-group legal ----- */

void test_mono_rejections(moq_version_t ver)
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
void test_mono_plain_regression(moq_version_t ver)
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

void test_mono_eog_open_vs_fin(moq_version_t ver)
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

/* Subgroup ID of the client's OBJECT_RECEIVED for {g,o}, or UINT64_MAX when
 * none arrived; every other event is dropped. */
static uint64_t mono_received_subgroup(moq_simpair_t *sp, uint64_t g, uint64_t o)
{
    uint64_t sg = UINT64_MAX;
    moq_event_t ev;
    while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1) {
        if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED &&
            ev.u.object_received.group_id == g &&
            ev.u.object_received.object_id == o)
            sg = ev.u.object_received.subgroup_id;
        moq_event_cleanup(&ev);
    }
    return sg;
}

/* Armed EOG on group 2, then moq_pub_reset_group: the candidate dies (both
 * drafts void the EOG inference for reset streams) and the group's Objects
 * keep their assignments. `tail` selects what follows the reset:
 *   0  end_group at once: promotes nothing, and seals group 2, so a later
 *      Object there is refused before any effect;
 *   1  a non-EOG Object 1 opens a fresh subgroup (Subgroup ID 1); a clean
 *      close of it still promotes nothing;
 *   2  a REPLACEMENT EOG Object 1 in the fresh subgroup plus a clean close
 *      promotes, with the exact stream count (the reset stream and the
 *      fresh one). */
static void mono_reset_case(moq_version_t ver, int tail)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    mono_setup(&as, &alloc, &sp, ver, &pub, &track);
    uint64_t now = moq_simpair_now_us(sp);
    windows_subscribe(sp, pub, MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE, 0, 0, 2);
    drain_all(sp);

    MOQ_TEST_CHECK(mono_write_stream_eog(pub, track, &alloc, sp, 2, 0, true,
                                         0x9A) == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_reset_group(pub, track, 0x10, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    (void)mono_received_subgroup(sp, 2, 0);

    if (tail == 0) {
        /* A stale arm surviving the reset would complete end=2 here. */
        MOQ_TEST_CHECK(moq_pub_end_group(pub, track, now) == MOQ_OK);
        MOQ_TEST_CHECK(moq_pub_flush(pub, now) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        MOQ_TEST_CHECK_EQ_INT(declare_client_dones(sp, NULL, NULL), 0);
        MOQ_TEST_CHECK_EQ_INT((int)mono_write_stream_eog(pub, track, &alloc,
                                  sp, 2, 1, true, 0x9B),
                              (int)MOQ_ERR_WRONG_STATE);
        MOQ_TEST_CHECK(moq_pub_flush(pub, now) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        MOQ_TEST_CHECK_EQ_INT(declare_client_dones(sp, NULL, NULL), 0);
    } else {
        MOQ_TEST_CHECK(mono_write_stream_eog(pub, track, &alloc, sp, 2, 1,
                                             tail == 2, 0x9B) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        MOQ_TEST_CHECK_EQ_U64(mono_received_subgroup(sp, 2, 1), 1);
        MOQ_TEST_CHECK(moq_pub_end_group(pub, track, now) == MOQ_OK);
        MOQ_TEST_CHECK(moq_pub_flush(pub, now) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        uint64_t st = ~0ull, sc = ~0ull;
        if (tail == 1) {
            MOQ_TEST_CHECK_EQ_INT(declare_client_dones(sp, NULL, NULL), 0);
        } else {
            MOQ_TEST_CHECK_EQ_INT(declare_client_dones(sp, &st, &sc), 1);
            MOQ_TEST_CHECK_EQ_U64(st, 0x3);
            MOQ_TEST_CHECK_EQ_U64(sc, 2);
        }
    }

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

void test_mono_reset_clears(moq_version_t ver)
{
    mono_reset_case(ver, 0);
    mono_reset_case(ver, 1);
    mono_reset_case(ver, 2);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "mono_reset_clears_d18" : "mono_reset_clears_d16");
}

/* -- Crossed-old-EOG ordering + blocked-op behavior --------------------- */

void test_mono_crossed_and_blocked(moq_version_t ver, bool abandon)
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
    MOQ_TEST_CHECK(flush_virtual(sp, pub) == MOQ_OK);
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

void test_mono_group_advance(moq_version_t ver)
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

void test_mono_status_evidence(moq_version_t ver)
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

void test_mono_streaming_higher_dg(moq_version_t ver)
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

void test_mono_datagram_no_arm(moq_version_t ver)
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
    MOQ_TEST_CHECK(flush_virtual(sp, pub) == MOQ_OK);
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

void test_mono_abi_canary(moq_version_t ver)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track_full;
    windows_setup(&as, &alloc, &sp, ver, &pub, &track_full);
    uint64_t now = moq_simpair_now_us(sp);

    /* Exact historical heap cfg, independent of the current type:
     * 64 bytes on LP64, 36 on i686, 40 on ARM ILP32.
     * Every byte poisoned 0xAA
     * first, real fields memcpy'd at their offsets, the old TRAILING
     * PADDING left poisoned; add_track must not read monotonic_groups (it
     * lies past the allocation). */
    const size_t old_size = sizeof(test_pub_track_pre_monotonic_t);
    MOQ_TEST_CHECK(offsetof(moq_pub_track_cfg_t, monotonic_groups) >= old_size);
    MOQ_TEST_CHECK_EQ_SIZE(offsetof(moq_pub_track_cfg_t, monotonic_groups),
                          PUB_TRACK_DIM(64, 40, 40));
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
        memcpy(raw + offsetof(test_pub_track_pre_monotonic_t, struct_size),
               &ss, sizeof(ss));
        memcpy(raw + offsetof(test_pub_track_pre_monotonic_t, track_namespace),
               &ns, sizeof(ns));
        memcpy(raw + offsetof(test_pub_track_pre_monotonic_t, track_name),
               &nm, sizeof(nm));
        memcpy(raw + offsetof(test_pub_track_pre_monotonic_t, advertise_namespace),
               &bfalse, sizeof(bfalse));
        memcpy(raw + offsetof(test_pub_track_pre_monotonic_t, publisher_priority),
               &prio, sizeof(prio));
        memcpy(raw + offsetof(test_pub_track_pre_monotonic_t, max_retained_bytes),
               &zero64, sizeof(zero64));
        memcpy(raw + offsetof(test_pub_track_pre_monotonic_t, has_publisher_priority),
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

void test_mono_declare_compose(moq_version_t ver)
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
