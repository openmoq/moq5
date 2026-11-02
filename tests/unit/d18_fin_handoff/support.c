#include "test_cases.h"

int og_quiet;
int txs_quiet;

int failures = 0;


/* -- Session fixture ------------------------------------------------ */

moq_session_t *make_session_full(moq_perspective_t persp,
                                 uint32_t max_events,
                                 uint32_t max_actions)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), persp);
    cfg.version = MOQ_VERSION_DRAFT_18;
    if (max_events) cfg.max_events = max_events;
    if (max_actions) cfg.max_actions = max_actions;
    moq_session_t *s = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_create(&cfg, 0, &s), (int)MOQ_OK);
    if (!s) return NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(s, 0), (int)MOQ_OK);
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
    uint8_t setup[16];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, setup, sizeof(setup));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_setup(&w), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_control_bytes(s, setup, moq_buf_writer_offset(&w), 0),
        (int)MOQ_OK);
    moq_event_t e;
    while (moq_session_poll_events(s, &e, 1) > 0) moq_event_cleanup(&e);
    while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
    return s;
}


moq_session_t *make_session_caps(moq_perspective_t persp,
                                 uint32_t max_events)
{
    return make_session_full(persp, max_events, 0);
}


moq_session_t *make_session(moq_perspective_t persp)
{
    return make_session_caps(persp, 0);
}


/* -- Exact output oracles ------------------------------------------- */

/* -- Owner inspection ----------------------------------------------- */

int pub_busy_count(const moq_session_t *s)
{
    int n = 0;
    for (size_t i = 0; i < s->pub_cap; i++)
        if (s->publishes[i].state != MOQ_PUB_FREE) n++;
    return n;
}


const moq_pub_entry_t *pub_by_ref(const moq_session_t *s,
                                  moq_stream_ref_t ref)
{
    for (size_t i = 0; i < s->pub_cap; i++) {
        const moq_pub_entry_t *e = &s->publishes[i];
        if (e->state != MOQ_PUB_FREE && e->rs.ref._v == ref._v)
            return e;
    }
    return NULL;
}


/* The stream-ref registry is an independent key onto the request pool, so
 * rekey and retirement are asserted there as well as in the pool. */
void check_registered(const moq_session_t *s, moq_stream_ref_t ref,
                      moq_request_kind_t kind, int slot)
{
    moq_request_endpoint_t ep = request_registry_find_by_streamref(s, ref);
    MOQ_TEST_CHECK_EQ_INT((int)ep.kind, (int)kind);
    if (kind != MOQ_REQ_NONE) {
        MOQ_TEST_CHECK_EQ_INT(ep.slot, slot);
        MOQ_TEST_CHECK(ep.has_stream_ref);
        MOQ_TEST_CHECK_EQ_U64(ep.stream_ref._v, ref._v);
    }
}


void check_unregistered(const moq_session_t *s, moq_stream_ref_t ref)
{
    check_registered(s, ref, MOQ_REQ_NONE, -1);
}


#define FIN_TERM_CAPS_ALL (FIN_TERM_APP_REJECT | FIN_TERM_INTERNAL_REJECT | \
                           FIN_TERM_APP_ACCEPT)



/* Validators return a problem count so the rejection is itself self-checkable. */
int fin_owner_problems(const fin_owner_case_t *f)
{
    int bad = 0;
    if (!f->name) bad++;
    if (!f->feed) bad++;
    if (f->owner_kind == MOQ_REQ_NONE) bad++;
    if (f->caps == 0) bad++;
    if ((f->caps & ~(unsigned)FIN_TERM_CAPS_ALL) != 0) bad++;
    if (((f->caps & FIN_TERM_APP_REJECT) != 0) != (f->app_reject != NULL))
        bad++;
    if (((f->caps & FIN_TERM_INTERNAL_REJECT) != 0) !=
        (f->internal_reject != NULL))
        bad++;
    if (((f->caps & FIN_TERM_APP_ACCEPT) != 0) != (f->app_accept != NULL))
        bad++;
    /* Exactly the role-less family declares the sentinel, and only it. */
    if ((f->owner_kind == MOQ_REQ_NAMESPACE_SUB) !=
        (f->owner_role == FIN_OWNER_ROLE_NA))
        bad++;
    bad += fin_hooks_problems(&f->hooks);
    return bad;
}


int fin_nondest_problems(const fin_nondest_case_t *f)
{
    int bad = 0;
    if (!f->name) bad++;
    if (!f->arrange) bad++;
    if (!f->terminal) bad++;
    bad += fin_hooks_problems(&f->hooks);
    return bad;
}


/* The generic snapshot records the scratch cursor as it stands, but the next
 * advancing call reclaims it once the event queue has drained
 * (session_call_prepare). Normalizing that -- and only that, and only when
 * the captured queue was empty -- keeps real scratch mutation detectable. */
void expect_after_call_prepare(txs_snapshot_t *snap)
{
    if (snap->event_depth == 0) snap->event_scratch_len = 0;
}


/* Fill the drain ring through the real accounting path. */
void fill_drain_ring(moq_session_t *s)
{
    uint64_t next = 0x4000;
    while (s->drain_ref_count < s->drain_ref_cap)
        MOQ_TEST_CHECK(drain_ref_add(s, moq_stream_ref_from_u64(next++)));
    MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, s->drain_ref_cap);
}


/* -- PUBLISH (P3 producer, T2 terminal) ----------------------------- */

static size_t encode_publish_msg(uint8_t *buf, size_t cap, uint64_t request_id,
                                 uint64_t alias)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    moq_d18_publish_t p = { 0 };
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    p.request_id = request_id;
    p.track_namespace = (moq_namespace_t){ parts, 1 };
    p.track_name = MOQ_BYTES_LITERAL("v");
    p.track_alias = alias;
    MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_publish(&w, &p), (int)MOQ_OK);
    return moq_buf_writer_offset(&w);
}


moq_result_t feed_publish(moq_session_t *s, moq_stream_ref_t ref,
                          uint64_t request_id, uint64_t alias, bool fin)
{
    uint8_t msg[160];
    size_t n = encode_publish_msg(msg, sizeof(msg), request_id, alias);
    return moq_session_on_bidi_stream_bytes(s, ref, msg, n, fin, 1);
}


/* The owner the producer created, checked against the DESCRIPTOR's declared
 * kind, role and state -- pool entry and registry key together -- before any
 * terminal runs. Returns a problem count so the comparison is self-checkable. */
int pub_owner_present_problems(const moq_session_t *s,
                               moq_stream_ref_t ref,
                               moq_request_kind_t want_kind,
                               int want_role, int want_state)
{
    int bad = 0;
    const moq_pub_entry_t *e = pub_by_ref(s, ref);
    if (!e) return 1;
    if ((int)e->role != want_role) bad++;
    if ((int)e->state != want_state) bad++;
    if (pub_busy_count(s) != 1) bad++;
    moq_request_endpoint_t ep = request_registry_find_by_streamref(s, ref);
    if ((int)ep.kind != (int)want_kind) bad++;
    else if (ep.slot != (int)(e - s->publishes)) bad++;
    else if (!ep.has_stream_ref || ep.stream_ref._v != ref._v) bad++;
    if (bad)
        TXS_DIAG("TXN publish owner: kind %d role %d state %d, expected "
                 "kind %d role %d state %d\n", (int)ep.kind, (int)e->role,
                 (int)e->state, (int)want_kind, want_role, want_state);
    return bad;
}


/* Pool slot released and every registry key removed. */
void check_pub_owner_retired(moq_session_t *s, moq_stream_ref_t ref)
{
    MOQ_TEST_CHECK_EQ_INT(pub_busy_count(s), 0);
    check_unregistered(s, ref);
}



/* Drain every event / action of one operation through the family's
 * normalizers, in order. A refused record fails the test. */
void collect_events(moq_session_t *s, const txs_op_hooks_t *h,
                    txs_norm_vec_t *out)
{
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        MOQ_TEST_CHECK(h->normalize_event(&ev, h->ctx, out));
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK(!out->overflowed);
}


void collect_actions(moq_session_t *s, const txs_op_hooks_t *h,
                     txs_norm_vec_t *out)
{
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) {
        MOQ_TEST_CHECK(h->normalize_action(&a, h->ctx, out));
        moq_action_cleanup(&a);
    }
    MOQ_TEST_CHECK(!out->overflowed);
}


/* Nothing was produced by the operation just performed. Drained through the
 * family normalizers, so an unexpected record fails on its own terms rather
 * than merely raising a count. */
void check_no_output(moq_session_t *s, const txs_op_hooks_t *h)
{
    txs_norm_vec_t got;
    txs_norm_init(&got);
    collect_events(s, h, &got);
    collect_actions(s, h, &got);
    MOQ_TEST_CHECK_EQ_SIZE(got.count, 0);
    txs_norm_free(&got);
}


void pub_hook_capture(const moq_session_t *s, void *vctx, void *vst)
{
    pub_ctx_t *c = (pub_ctx_t *)vctx;
    pub_hook_state_t *st = (pub_hook_state_t *)vst;
    memset(st, 0, sizeof(*st));
    st->busy = pub_busy_count(s);
    const moq_pub_entry_t *e = pub_by_ref(s, c->ref);
    if (!e) return;
    st->role = (int)e->role;
    st->handle = e->handle._opaque;
    st->req_stream_ref = e->rs.ref._v;
    st->rs.recv_fin = e->rs.recv_fin;
    st->handoff_fin = e->handoff_fin_pending;
}


int pub_hook_check(const moq_session_t *s, void *vctx, const void *vst)
{
    pub_hook_state_t now;
    pub_hook_capture(s, vctx, &now);
    const pub_hook_state_t *want = (const pub_hook_state_t *)vst;
    int bad = 0;
    if (now.role != want->role) bad++;
    if (now.handle != want->handle) bad++;
    if (now.req_stream_ref != want->req_stream_ref) bad++;
    if (now.rs.recv_fin != want->rs.recv_fin) bad++;
    if (now.handoff_fin != want->handoff_fin) bad++;
    if (now.busy != want->busy) bad++;
    if (bad)
        TXS_DIAG("TXN publish hooks: role %d handle %llu ref %llu fin %d "
                 "handoff %d busy %d, expected role %d handle %llu ref %llu "
                 "fin %d handoff %d busy %d\n",
                 now.role, (unsigned long long)now.handle,
                 (unsigned long long)now.req_stream_ref, now.rs.recv_fin,
                 now.handoff_fin, now.busy,
                 want->role, (unsigned long long)want->handle,
                 (unsigned long long)want->req_stream_ref, want->rs.recv_fin,
                 want->handoff_fin, want->busy);
    return bad;
}


/* An unrecognized kind is a normalization FAILURE, never a kind-only record:
 * a family must describe every output its cases can produce. */
bool pub_norm_event(const moq_event_t *ev, void *vctx,
                    txs_norm_vec_t *out)
{
    pub_ctx_t *c = (pub_ctx_t *)vctx;
    txs_img_t im;
    txs_img_init(&im);
    switch (ev->kind) {
    case MOQ_EVENT_PUBLISH_REQUEST:
        c->last_pub = ev->u.publish_request.pub._opaque;
        txs_img_u64(&im, ev->u.publish_request.pub._opaque);
        break;
    case MOQ_EVENT_PUBLISH_FINISHED:
        txs_img_u64(&im, ev->u.publish_finished.pub._opaque);
        txs_img_u64(&im, ev->u.publish_finished.status_code);
        txs_img_u64(&im, ev->u.publish_finished.stream_count);
        txs_img_bytes(&im, ev->u.publish_finished.reason.data,
                      ev->u.publish_finished.reason.len);
        break;
    default:
        TXS_DIAG("TXN publish: unnormalized event kind %u\n",
                 (unsigned)ev->kind);
        return false;
    }
    return txs_norm_append_img(out, ev->kind, &im);
}


bool pub_norm_action(const moq_action_t *a, void *vctx,
                     txs_norm_vec_t *out)
{
    (void)vctx;
    txs_img_t im;
    txs_img_init(&im);
    switch (a->kind) {
    case MOQ_ACTION_SEND_BIDI_STREAM: {
        txs_img_u64(&im, a->u.send_bidi_stream.stream_ref._v);
        txs_img_u64(&im, a->u.send_bidi_stream.fin ? 1u : 0u);
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, a->u.send_bidi_stream.data,
                            a->u.send_bidi_stream.len);
        moq_control_envelope_t env;
        if (moq_d18_decode_envelope(&r, &env) != MOQ_OK) {
            TXS_DIAG("TXN publish: undecodable bidi action payload\n");
            return false;
        }
        /* One envelope and nothing after it. */
        if (moq_buf_reader_remaining(&r) != 0) {
            TXS_DIAG("TXN publish: %zu trailing bytes after envelope\n",
                     moq_buf_reader_remaining(&r));
            return false;
        }
        txs_img_u64(&im, env.msg_type);
        txs_img_bytes(&im, env.payload, env.payload_len);
        break;
    }
    case MOQ_ACTION_RESET_DATA:
        /* A data-stream cancellation carries no envelope: its whole content is
         * the stream it aborts and the code it aborts with. */
        txs_img_u64(&im, a->u.reset_data.stream_ref._v);
        txs_img_u64(&im, a->u.reset_data.error_code);
        break;
    default:
        TXS_DIAG("TXN publish: unnormalized action kind %u\n",
                 (unsigned)a->kind);
        return false;
    }
    return txs_norm_append_img(out, a->kind, &im);
}


/* Expected images, built at runtime from the handle the request surfaced. */
void want_publish_request(txs_norm_vec_t *v, uint64_t handle)
{
    txs_img_t im;
    txs_img_init(&im);
    txs_img_u64(&im, handle);
    MOQ_TEST_CHECK(txs_norm_append_img(v, MOQ_EVENT_PUBLISH_REQUEST, &im));
}


void want_publish_finished(txs_norm_vec_t *v, uint64_t handle)
{
    txs_img_t im;
    txs_img_init(&im);
    txs_img_u64(&im, handle);
    txs_img_u64(&im, 0);            /* status */
    txs_img_u64(&im, 0);            /* stream count */
    txs_img_bytes(&im, NULL, 0);    /* no reason phrase */
    MOQ_TEST_CHECK(txs_norm_append_img(v, MOQ_EVENT_PUBLISH_FINISHED, &im));
}


void want_reject_error_code(txs_norm_vec_t *v, moq_stream_ref_t ref,
                            uint64_t error_code)
{
    uint8_t buf[128];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_d18_encode_request_error(&w, error_code, 0, (moq_bytes_t){0}),
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


void want_reject_error_reason(txs_norm_vec_t *v, moq_stream_ref_t ref,
                              uint64_t error_code, uint64_t retry,
                              const char *reason)
{
    uint8_t buf[192];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    moq_bytes_t r = { (const uint8_t *)reason, strlen(reason) };
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_d18_encode_request_error(&w, error_code, retry, r), (int)MOQ_OK);
    moq_buf_reader_t rd;
    moq_buf_reader_init(&rd, buf, moq_buf_writer_offset(&w));
    moq_control_envelope_t env;
    MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_envelope(&rd, &env), (int)MOQ_OK);
    txs_img_t im;
    txs_img_init(&im);
    txs_img_u64(&im, ref._v);
    txs_img_u64(&im, 1);            /* FIN closes our send half */
    txs_img_u64(&im, env.msg_type);
    txs_img_bytes(&im, env.payload, env.payload_len);
    MOQ_TEST_CHECK(txs_norm_append_img(v, MOQ_ACTION_SEND_BIDI_STREAM, &im));
}


void want_reject_error(txs_norm_vec_t *v, moq_stream_ref_t ref)
{
    uint8_t buf[128];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_d18_encode_request_error(&w, MOQ_REQUEST_ERROR_NOT_SUPPORTED,
                                          0, (moq_bytes_t){0}),
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


/* A successful rejection produces exactly one REQUEST_ERROR action AND no
 * event at all. Draining both queues is load-bearing: an unexpected event
 * left unpolled would otherwise vanish at session destruction. */
void check_reject_only_output(moq_session_t *s,
                              const txs_op_hooks_t *h,
                              moq_stream_ref_t ref, const char *name)
{
    txs_norm_vec_t got, want;
    txs_norm_init(&got);
    txs_norm_init(&want);
    collect_events(s, h, &got);
    MOQ_TEST_CHECK_EQ_SIZE(got.count, 0);   /* no event, before any action */
    collect_actions(s, h, &got);
    want_reject_error(&want, ref);
    failures += txs_norm_equals(&got, &want, name);
    txs_norm_free(&got);
    txs_norm_free(&want);
}


size_t enc_request_error(uint8_t *buf, size_t cap, uint64_t code,
                         uint64_t retry, const char *reason)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    moq_bytes_t r = { 0 };
    if (reason) { r.data = (const uint8_t *)reason; r.len = strlen(reason); }
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_d18_encode_request_error(&w, code, retry, r), (int)MOQ_OK);
    return moq_buf_writer_offset(&w);
}


/* One control envelope with a caller-chosen type and payload, so the type can
 * be varied while the payload stays byte-identical. */
size_t enc_typed_envelope(uint8_t *buf, size_t cap, uint64_t msg_type,
                          const uint8_t *payload, size_t payload_len)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    size_t len_off = 0;
    MOQ_TEST_CHECK_EQ_INT((int)moq_buf_write_vi64(&w, msg_type), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_buf_reserve_uint16(&w, &len_off),
                          (int)MOQ_OK);
    size_t start = moq_buf_writer_offset(&w);
    MOQ_TEST_CHECK_EQ_INT((int)moq_buf_write_raw(&w, payload, payload_len),
                          (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_buf_patch_uint16(&w, len_off,
                                  (uint16_t)(moq_buf_writer_offset(&w) - start)),
        (int)MOQ_OK);
    return moq_buf_writer_offset(&w);
}
