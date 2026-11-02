#include "test_cases.h"

/* -- Namespace refcounting tests ------------------------------------ */

void test_ns_refcount_shared(void) {
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

void test_ns_refcount_distinct(void) {
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

void test_ns_refcount_would_block_retry(void) {
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
void test_nsterm_rejected(moq_version_t ver)
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
void test_nsterm_cancelled(moq_version_t ver)
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
void test_nsterm_unmatched(void)
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

void test_nsterm_old_size_finished_preserved(moq_version_t ver)
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
void test_nsterm_abi_canary(void)
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

/* -- Track and namespace lifecycle -------------------------------- */

void test_invalid_namespace_inputs(void) {
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

void test_namespace_accepted_then_remove(void) {
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

void test_namespace_accepted_query(void) {
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

void test_namespace_accepted_query_rejected(void) {
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

void test_namespace_pending_remove_blocked(void) {
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

void test_namespace_rejected_updates_state(void) {
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
void test_namespace_terminal_readvertises(void) {
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

void test_remove_pending_ns_with_open_sg(void) {
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

void test_namespace_overflow(void) {
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

void test_advertised_ns_duplicate(void) {
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

void test_advertised_ns_reuse_after_terminal(void) {
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
