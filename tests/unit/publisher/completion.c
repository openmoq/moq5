#include "test_cases.h"

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

/* -- B rejection: every location-admitting path, zero mutation -------- */

void test_declare_b_rejections(moq_version_t ver)
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

void test_declare_pending_and_merge(moq_version_t ver)
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

void test_declare_completion_basic(moq_version_t ver)
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

void test_declare_publication_completes(moq_version_t ver)
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

void test_declare_blocked_steps(moq_version_t ver, bool lower_redeclare)
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

void test_declare_fin_before_done(moq_version_t ver)
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

void test_declare_install_immediate(moq_version_t ver, bool tick_mode)
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

void test_declare_install_deferred_and_pending(moq_version_t ver)
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

void test_declare_update_paths(moq_version_t ver)
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

void test_declare_after_end_track(moq_version_t ver)
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

void test_declare_fetch_below_watermark(moq_version_t ver)
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

void test_declare_plain_track_unchanged(moq_version_t ver)
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
    MOQ_TEST_CHECK(found_ref);   /* logical stream ref, escaped raw: not OPENED */
    MOQ_TEST_CHECK(moq_session_on_data_stop(sv, ref, 0x0,
        moq_simpair_now_us(sp)) == MOQ_OK);
    { moq_action_t acts[8]; size_t na;
      while ((na = moq_session_poll_actions(sv, acts, 8)) > 0)
          for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]); }
}

void test_declare_stale_completion(moq_version_t ver,
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
     * with 0x3 and the honest raw-unknown stream count. */
    MOQ_TEST_CHECK(moq_pub_declare_groups_complete_through(pub, track, 3,
                                                           now) == MOQ_OK);
    {
        declare_scan_t sc; declare_scan_actions(sv, ver, &sc);
        MOQ_TEST_CHECK_EQ_INT(sc.dones, 1);
        MOQ_TEST_CHECK_EQ_U64(sc.status, 0x3);
        /* Raw first-header escape above; D16/D18 MUST send 2^62-1. */
        MOQ_TEST_CHECK_EQ_U64(sc.streams, (UINT64_C(1) << 62) - 1);
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

void test_declare_stale_armed_reset(moq_version_t ver)
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
     * and the raw-unknown count (the first header was discarded). */
    MOQ_TEST_CHECK(moq_pub_declare_groups_complete_through(pub, track, 3,
                                                           now) == MOQ_OK);
    {
        declare_scan_t sc; declare_scan_actions(sv, ver, &sc);
        MOQ_TEST_CHECK_EQ_INT(sc.dones, 1);
        MOQ_TEST_CHECK_EQ_U64(sc.status, 0x3);
        /* Raw first-header escape above; D16/D18 MUST send 2^62-1. */
        MOQ_TEST_CHECK_EQ_U64(sc.streams, (UINT64_C(1) << 62) - 1);
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

void test_declare_end_track_reconcile(moq_version_t ver)
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

void test_declare_end_track_ceiling(moq_version_t ver)
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

void test_declare_end_track_blocked_preflight(moq_version_t ver)
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
    /* The status-only end_track queued a new first header. Observe its real
     * virtual admission before declaring the finite terminal. */
    MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(sp, 16, NULL) == MOQ_OK);
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

/* -- Finite filter end and the PUBLISH_DONE registry ------------------- *
 * Drafts 16 and 18 end a subscription whose finite filter end is covered by
 * the completion watermark, with PUBLISH_DONE SUBSCRIPTION_ENDED (0x3).
 * Draft-22 has no such status (§16.11.3) and "A publisher does not end a
 * subscription solely because the Largest Object advances past the end of
 * the current Location Filter" (§3.3.1): the subscription stays established,
 * nothing outside its window is sent, and the track's end finishes it with
 * TRACK_ENDED (0x2). */
static void collect_window_events(moq_simpair_t *sp, int *nobj, uint64_t g,
                                  int *ndone, uint64_t *status)
{
    moq_event_t ev;
    while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1) {
        if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED &&
            ev.u.object_received.group_id == g &&
            ev.u.object_received.status == MOQ_OBJECT_NORMAL)
            (*nobj)++;
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_DONE) {
            (*ndone)++;
            *status = ev.u.subscribe_done.status_code;
        }
        moq_event_cleanup(&ev);
    }
}

void test_window_completion_status(moq_version_t ver)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    windows_setup(&as, &alloc, &sp, ver, &pub, &track);
    uint64_t now = moq_simpair_now_us(sp);
    windows_subscribe(sp, pub, MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE, 0, 0, 0);

    MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 0, 0, 0xE0)
                   == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    int nobj = 0, ndone = 0;
    uint64_t status = UINT64_MAX;
    collect_window_events(sp, &nobj, 0, &ndone, &status);
    MOQ_TEST_CHECK_EQ_INT(nobj, 1);

    MOQ_TEST_CHECK(moq_pub_declare_groups_complete_through(pub, track, 0, now)
                   == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    collect_window_events(sp, &nobj, 0, &ndone, &status);
    if (ver == MOQ_VERSION_DRAFT_22) {
        MOQ_TEST_CHECK_EQ_INT(ndone, 0);
        MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track), 1);
        /* Group 1 lies outside the window: not sent. */
        MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 1, 0, 0xE1)
                       == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        nobj = 0;
        collect_window_events(sp, &nobj, 1, &ndone, &status);
        MOQ_TEST_CHECK_EQ_INT(nobj, 0);
        MOQ_TEST_CHECK_EQ_INT(ndone, 0);
        MOQ_TEST_CHECK(moq_pub_end_track(pub, track, moq_simpair_now_us(sp))
                       == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        collect_window_events(sp, &nobj, 1, &ndone, &status);
        MOQ_TEST_CHECK_EQ_INT(ndone, 1);
        MOQ_TEST_CHECK_EQ_U64(status, 0x2);   /* TRACK_ENDED */
    } else {
        MOQ_TEST_CHECK_EQ_INT(ndone, 1);
        MOQ_TEST_CHECK_EQ_U64(status, 0x3);   /* SUBSCRIPTION_ENDED */
        MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track), 0);
    }
    MOQ_TEST_CHECK(moq_session_state(moq_simpair_server(sp))
                   == MOQ_SESS_ESTABLISHED);
    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_22 ? "window_completion_status_d22"
                  : ver == MOQ_VERSION_DRAFT_18 ? "window_completion_status_d18"
                  : "window_completion_status_d16");
}
