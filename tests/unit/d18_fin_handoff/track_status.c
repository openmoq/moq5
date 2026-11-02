#include "test_cases.h"

/* -- TRACK_STATUS (P7) ------------------------------------------------ */

/* Like pending-join FETCH, this family already carries the peer's close by a
 * direct req_recv_fin copy, so these rows are REGRESSIONS pinning behaviour
 * the handoff-marker conversion must keep. Both of its terminals -- accept and
 * reject -- consult that latch, so both are exercised. */

typedef struct ts_ctx {
    uint64_t         request_id;
    moq_stream_ref_t ref;
    uint64_t         last_handle;
} ts_ctx_t;


typedef struct ts_hook_state {
    int      present;
    uint64_t handle;
    uint64_t req_stream_ref;
    struct {
        int recv_fin;
    } rs;
    /* The transitional FIN ownership a same-call handoff installs. This family
     * consumes it into req_recv_fin without blocking, so every observation
     * point must find it already clear. */
    int      handoff_fin;
    int      state;
    int      role;
    int      busy;
} ts_hook_state_t;


static int ts_busy_count(const moq_session_t *s)
{
    int n = 0;
    for (size_t i = 0; i < s->ts_cap; i++)
        if (s->track_statuses[i].state != MOQ_TS_FREE) n++;
    return n;
}


static const moq_ts_entry_t *ts_by_ref(const moq_session_t *s,
                                       moq_stream_ref_t ref)
{
    for (size_t i = 0; i < s->ts_cap; i++) {
        const moq_ts_entry_t *e = &s->track_statuses[i];
        if (e->state != MOQ_TS_FREE && e->rs.ref._v == ref._v)
            return e;
    }
    return NULL;
}


static moq_result_t ts_feed(moq_session_t *s, void *ctx, moq_stream_ref_t ref,
                            bool fin)
{
    ts_ctx_t *c = (ts_ctx_t *)ctx;
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live"),
                            MOQ_BYTES_LITERAL("eu") };
    moq_namespace_t ns = { parts, 2 };
    moq_d18_msg_params_t mp;
    memset(&mp, 0, sizeof(mp));
    mp.auth_token_count = 1;
    mp.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_USE_VALUE;
    mp.auth_tokens[0].token_type = 8;
    mp.auth_tokens[0].token_value = MOQ_BYTES_LITERAL("tstok");
    uint8_t m[192];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, m, sizeof(m));
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_d18_encode_track_status(&w, c->request_id, &ns,
                                         MOQ_BYTES_LITERAL("vid"), &mp),
        (int)MOQ_OK);
    return moq_session_on_bidi_stream_bytes(s, ref, m,
                                            moq_buf_writer_offset(&w), fin, 1);
}


static moq_result_t ts_app_reject(moq_session_t *s, void *ctx, uint64_t handle)
{
    (void)ctx;
    moq_track_status_handle_t h;
    h._opaque = handle;
    moq_reject_track_status_cfg_t rj;
    moq_reject_track_status_cfg_init(&rj);
    rj.error_code = MOQ_REQUEST_ERROR_NOT_SUPPORTED;
    return moq_session_reject_track_status(s, h, &rj, 1);
}


/* Non-default status values, so an oracle that ignored the payload could not
 * pass against an empty or zero-filled expectation. */
#define TS_LARGEST_GROUP  17u

#define TS_LARGEST_OBJECT 5u

#define TS_EXPIRES_MS     900u


static moq_result_t ts_app_accept(moq_session_t *s, void *ctx, uint64_t handle)
{
    (void)ctx;
    moq_track_status_handle_t h;
    h._opaque = handle;
    moq_accept_track_status_cfg_t ac;
    moq_accept_track_status_cfg_init(&ac);
    ac.has_largest = true;
    ac.largest_group = TS_LARGEST_GROUP;
    ac.largest_object = TS_LARGEST_OBJECT;
    ac.has_expires = true;
    ac.expires_ms = TS_EXPIRES_MS;
    return moq_session_accept_track_status(s, h, &ac, 1);
}


static void ts_hook_capture(const moq_session_t *s, void *vctx, void *vst)
{
    ts_ctx_t *c = (ts_ctx_t *)vctx;
    ts_hook_state_t *st = (ts_hook_state_t *)vst;
    memset(st, 0, sizeof(*st));
    st->busy = ts_busy_count(s);
    const moq_ts_entry_t *e = ts_by_ref(s, c->ref);
    if (!e) return;
    st->present = 1;
    st->handle = e->handle._opaque;
    st->req_stream_ref = e->rs.ref._v;
    st->rs.recv_fin = e->rs.recv_fin;
    st->handoff_fin = e->handoff_fin_pending;
    st->state = (int)e->state;
    st->role = (int)e->role;
}


static int ts_hook_check(const moq_session_t *s, void *vctx, const void *vst)
{
    ts_hook_state_t now;
    ts_hook_capture(s, vctx, &now);
    const ts_hook_state_t *want = (const ts_hook_state_t *)vst;
    int bad = 0;
    if (now.present != want->present) bad++;
    if (now.handle != want->handle) bad++;
    if (now.req_stream_ref != want->req_stream_ref) bad++;
    if (now.rs.recv_fin != want->rs.recv_fin) bad++;
    if (now.handoff_fin != want->handoff_fin) bad++;
    if (now.state != want->state) bad++;
    if (now.role != want->role) bad++;
    if (now.busy != want->busy) bad++;
    if (bad)
        TXS_DIAG("TXN ts hooks: present %d handle %llu ref %llu fin %d "
                 "state %d busy %d, expected %d/%llu/%llu/%d/%d/%d\n",
                 now.present, (unsigned long long)now.handle,
                 (unsigned long long)now.req_stream_ref, now.rs.recv_fin,
                 now.state, now.busy, want->present,
                 (unsigned long long)want->handle,
                 (unsigned long long)want->req_stream_ref, want->rs.recv_fin,
                 want->state, want->busy);
    return bad;
}


static bool ts_norm_event(const moq_event_t *ev, void *vctx,
                          txs_norm_vec_t *out)
{
    ts_ctx_t *c = (ts_ctx_t *)vctx;
    txs_img_t im;
    txs_img_init(&im);
    switch (ev->kind) {
    case MOQ_EVENT_TRACK_STATUS_REQUEST: {
        const moq_track_status_request_event_t *rq = &ev->u.track_status_request;
        c->last_handle = rq->handle._opaque;
        txs_img_u64(&im, rq->handle._opaque);
        if (!txs_img_parts(&im, "track_status_request namespace", rq->track_namespace.parts,
                           rq->track_namespace.count,
                           MOQ_DECODED_MAX_NAMESPACE_PARTS))
            return false;
        txs_img_bytes(&im, rq->track_name.data, rq->track_name.len);
        if (!txs_img_tokens(&im, "track_status_request tokens", rq->tokens,
                            rq->token_count, MOQ_DECODED_MAX_TOKENS))
            return false;
        break;
    }
    default:
        TXS_DIAG("TXN ts: unnormalized event kind %u\n", (unsigned)ev->kind);
        return false;
    }
    return txs_norm_append_img(out, ev->kind, &im);
}


static void want_ts_request(txs_norm_vec_t *v, uint64_t handle)
{
    txs_img_t im;
    txs_img_init(&im);
    txs_img_u64(&im, handle);
    txs_img_u64(&im, 2);
    txs_img_bytes(&im, (const uint8_t *)"live", 4);
    txs_img_bytes(&im, (const uint8_t *)"eu", 2);
    txs_img_bytes(&im, (const uint8_t *)"vid", 3);
    txs_img_u64(&im, 1);
    txs_img_u64(&im, 8);
    txs_img_bytes(&im, (const uint8_t *)"tstok", 5);
    MOQ_TEST_CHECK(txs_norm_append_img(v, MOQ_EVENT_TRACK_STATUS_REQUEST, &im));
}


/* The accept path answers with a TRACK_STATUS_OK carrying the configured
 * status, and closes our send half. The expectation is encoded independently
 * from those same configured values. */
static void want_ts_ok(txs_norm_vec_t *v, moq_stream_ref_t ref)
{
    moq_d18_msg_params_t p;
    memset(&p, 0, sizeof(p));
    p.has_largest = true;
    p.largest_group = TS_LARGEST_GROUP;
    p.largest_object = TS_LARGEST_OBJECT;
    p.has_expires = true;
    p.expires_ms = TS_EXPIRES_MS;
    uint8_t buf[192];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_d18_encode_track_status_ok(&w, &p, (moq_bytes_t){0}),
        (int)MOQ_OK);
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
    moq_control_envelope_t env;
    MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_envelope(&r, &env), (int)MOQ_OK);
    txs_img_t im;
    txs_img_init(&im);
    txs_img_u64(&im, ref._v);
    txs_img_u64(&im, 1);            /* FIN closes our send half */
    txs_img_u64(&im, env.msg_type);
    txs_img_bytes(&im, env.payload, env.payload_len);
    MOQ_TEST_CHECK(txs_norm_append_img(v, MOQ_ACTION_SEND_BIDI_STREAM, &im));
}


static void check_ts_accept_output(moq_session_t *s, const txs_op_hooks_t *h,
                                   moq_stream_ref_t ref, const char *name)
{
    txs_norm_vec_t got, want;
    txs_norm_init(&got);
    txs_norm_init(&want);
    collect_events(s, h, &got);
    MOQ_TEST_CHECK_EQ_SIZE(got.count, 0);   /* no event, before any action */
    collect_actions(s, h, &got);
    want_ts_ok(&want, ref);
    failures += txs_norm_equals(&got, &want, name);
    txs_norm_free(&got);
    txs_norm_free(&want);
}


static ts_ctx_t ts_ctx = { 0, { 1 }, 0 };


static fin_owner_case_t ts_family(void)
{
    fin_owner_case_t f;
    memset(&f, 0, sizeof(f));
    f.name = "track-status";
    f.ctx = &ts_ctx;
    f.caps = FIN_TERM_APP_REJECT | FIN_TERM_APP_ACCEPT;
    f.feed = ts_feed;
    f.owner_kind = MOQ_REQ_TRACK_STATUS;
    f.owner_role = (int)MOQ_TS_ROLE_PUBLISHER;
    f.owner_state = (int)MOQ_TS_PENDING_PUBLISHER;
    f.app_reject = ts_app_reject;
    f.app_accept = ts_app_accept;
    f.hooks.ctx = &ts_ctx;
    f.hooks.capture = ts_hook_capture;
    f.hooks.check = ts_hook_check;
    f.hooks.normalize_event = ts_norm_event;
    f.hooks.normalize_action = pub_norm_action;
    return f;
}


static int ts_owner_present_problems(const moq_session_t *s,
                                     moq_stream_ref_t ref,
                                     moq_request_kind_t want_kind,
                                     int want_role, int want_state)
{
    int bad = 0;
    const moq_ts_entry_t *e = ts_by_ref(s, ref);
    if (!e) return 1;
    if ((int)e->role != want_role) bad++;
    if ((int)e->state != want_state) bad++;
    if (ts_busy_count(s) != 1) bad++;
    moq_request_endpoint_t ep = request_registry_find_by_streamref(s, ref);
    if ((int)ep.kind != (int)want_kind) bad++;
    else if (ep.slot != (int)(e - s->track_statuses)) bad++;
    else if (!ep.has_stream_ref || ep.stream_ref._v != ref._v) bad++;
    if (bad)
        TXS_DIAG("TXN ts owner: kind %d role %d state %d, expected "
                 "kind %d role %d state %d\n", (int)ep.kind, (int)e->role,
                 (int)e->state, (int)want_kind, want_role, want_state);
    return bad;
}


static void check_ts_retired(moq_session_t *s, moq_stream_ref_t ref)
{
    MOQ_TEST_CHECK_EQ_INT(ts_busy_count(s), 0);
    check_unregistered(s, ref);
}


/* `accept` selects which terminal answers; both consult the FIN latch. */
static void run_ts_case(const fin_owner_case_t *f, bool fin_in_request,
                        bool fill_ring, bool accept)
{
    moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
    ts_ctx_t *c = (ts_ctx_t *)f->ctx;
    moq_stream_ref_t ref = c->ref;
    c->last_handle = 0;

    drain_snap_t ring_pre_feed;
    drain_snap(s, &ring_pre_feed);
    MOQ_TEST_CHECK_EQ_INT((int)f->feed(s, f->ctx, ref, fin_in_request),
                          (int)MOQ_OK);
    {
        drain_snap_t after_feed;
        drain_snap(s, &after_feed);
        failures += drain_multiset_equals(&after_feed, &ring_pre_feed,
                                          "ts feed");
    }

    txs_norm_vec_t got, want;
    txs_norm_init(&got);
    txs_norm_init(&want);
    collect_events(s, &f->hooks, &got);
    want_ts_request(&want, c->last_handle);
    failures += txs_norm_equals(&got, &want, f->name);
    txs_norm_free(&got);
    txs_norm_free(&want);

    failures += ts_owner_present_problems(s, ref, f->owner_kind,
                                          f->owner_role, f->owner_state);
    ts_hook_state_t owner_before;
    f->hooks.capture(s, f->ctx, &owner_before);
    MOQ_TEST_CHECK_EQ_U64(owner_before.handle, c->last_handle);
    MOQ_TEST_CHECK_EQ_U64(owner_before.req_stream_ref, ref._v);
    MOQ_TEST_CHECK_EQ_INT(owner_before.rs.recv_fin, fin_in_request ? 1 : 0);
    /* This family's FIN handling cannot block, so a handed-over FIN is already
     * consumed into the durable latch: the transitional fact never survives
     * the call that installed it, and the terminals below read the latch. */
    MOQ_TEST_CHECK_EQ_INT(owner_before.handoff_fin, 0);
    check_no_output(s, &f->hooks);

    if (fill_ring) fill_drain_ring(s);
    drain_snap_t ring_before;
    drain_snap(s, &ring_before);

    bool expect_drain = !fin_in_request;
    moq_result_t want_rc = (expect_drain && fill_ring) ? MOQ_ERR_WOULD_BLOCK
                                                       : MOQ_OK;
    txs_snapshot_t before;
    txs_capture(s, &ref, 1, &before);
    expect_after_call_prepare(&before);
    moq_result_t rc = accept ? f->app_accept(s, f->ctx, c->last_handle)
                             : f->app_reject(s, f->ctx, c->last_handle);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)want_rc);

    if (want_rc == MOQ_ERR_WOULD_BLOCK) {
        failures += txs_check_eq(s, &ref, 1, &before, f->name);
        failures += f->hooks.check(s, f->ctx, &owner_before);
        failures += ts_owner_present_problems(s, ref, f->owner_kind,
                                              f->owner_role, f->owner_state);
        drain_snap_t now;
        drain_snap(s, &now);
        failures += drain_multiset_equals(&now, &ring_before, "ts blocked");
        check_no_output(s, &f->hooks);
        moq_session_destroy(s);
        return;
    }

    drain_snap_t now, want_ring;
    drain_snap(s, &now);
    if (expect_drain)
        drain_snap_plus(&ring_before, ref, MOQ_DRAIN_NORMAL, &want_ring);
    else
        want_ring = ring_before;
    failures += drain_multiset_equals(&now, &want_ring, "ts granted");
    check_ts_retired(s, ref);
    if (accept) check_ts_accept_output(s, &f->hooks, ref, f->name);
    else        check_reject_only_output(s, &f->hooks, ref, f->name);

    if (expect_drain) {
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
            (int)MOQ_OK);
        drain_snap_t after;
        drain_snap(s, &after);
        failures += drain_multiset_equals(&after, &ring_before, "ts released");
        check_ts_retired(s, ref);
        check_no_output(s, &f->hooks);
    }
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
    moq_session_destroy(s);
}


void test_track_status(void)
{
    fin_owner_case_t f = ts_family();
    MOQ_TEST_CHECK_EQ_INT(fin_owner_problems(&f), 0);
    /* The accept capability and its callback must agree, and the declared
     * role must be the one the entry actually commits. */
    {
        fin_owner_case_t bad = f;
        bad.caps &= ~(unsigned)FIN_TERM_APP_ACCEPT;
        MOQ_TEST_CHECK(fin_owner_problems(&bad) > 0);
        fin_owner_case_t bad2 = f;
        bad2.app_accept = NULL;
        MOQ_TEST_CHECK(fin_owner_problems(&bad2) > 0);
    }
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        ts_ctx_t *c = (ts_ctx_t *)f.ctx;
        MOQ_TEST_CHECK_EQ_INT((int)f.feed(s, f.ctx, c->ref, false),
                              (int)MOQ_OK);
        txs_norm_vec_t v;
        txs_norm_init(&v);
        collect_events(s, &f.hooks, &v);
        txs_norm_free(&v);
        MOQ_TEST_CHECK_EQ_INT(
            ts_owner_present_problems(s, c->ref, f.owner_kind, f.owner_role,
                                      f.owner_state), 0);
        txs_quiet = 1;
        MOQ_TEST_CHECK(ts_owner_present_problems(s, c->ref, f.owner_kind,
                                                 (int)MOQ_TS_ROLE_REQUESTER,
                                                 f.owner_state) > 0);
        txs_quiet = 0;
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
        moq_session_destroy(s);
    }
    /* Reject terminal (T11). */
    run_ts_case(&f, false, false, false);
    run_ts_case(&f, true,  false, false);
    run_ts_case(&f, true,  true,  false);
    run_ts_case(&f, false, true,  false);
    /* Accept terminal (T10), same selector. */
    run_ts_case(&f, false, false, true);
    run_ts_case(&f, true,  false, true);
    run_ts_case(&f, true,  true,  true);
    run_ts_case(&f, false, true,  true);
}
