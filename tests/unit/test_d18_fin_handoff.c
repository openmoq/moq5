/*
 * Draft-18 request-stream FIN ownership: a FIN that arrives in the same chunk
 * as the request must survive the transition from the receiving staging owner
 * to the destination owner the request creates, and every terminal that closes
 * such an owner must consult it before reserving a drain reference.
 */
#include <moq/moq.h>
#include <moq/control_d18.h>
#include <moq/transport_bridge.h>
#include "test_support.h"
#include "../support/ownership_graph.h"
#include "../support/fin_case.h"
#include "../support/txn_snapshot.h"
#include "../../core/src/session/session_internal.h"

static int failures = 0;

/* -- Session fixture ------------------------------------------------ */

static moq_session_t *make_session_full(moq_perspective_t persp,
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

static moq_session_t *make_session_caps(moq_perspective_t persp,
                                        uint32_t max_events)
{
    return make_session_full(persp, max_events, 0);
}

static moq_session_t *make_session(moq_perspective_t persp)
{
    return make_session_caps(persp, 0);
}

/* -- Exact output oracles ------------------------------------------- */

/* -- Owner inspection ----------------------------------------------- */

static int pub_busy_count(const moq_session_t *s)
{
    int n = 0;
    for (size_t i = 0; i < s->pub_cap; i++)
        if (s->publishes[i].state != MOQ_PUB_FREE) n++;
    return n;
}

static const moq_pub_entry_t *pub_by_ref(const moq_session_t *s,
                                         moq_stream_ref_t ref)
{
    for (size_t i = 0; i < s->pub_cap; i++) {
        const moq_pub_entry_t *e = &s->publishes[i];
        if (e->state != MOQ_PUB_FREE && e->request_stream_ref._v == ref._v)
            return e;
    }
    return NULL;
}

/* The stream-ref registry is an independent key onto the request pool, so
 * rekey and retirement are asserted there as well as in the pool. */
static void check_registered(const moq_session_t *s, moq_stream_ref_t ref,
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

static void check_unregistered(const moq_session_t *s, moq_stream_ref_t ref)
{
    check_registered(s, ref, MOQ_REQ_NONE, -1);
}

/* The obligations do not share one shape, so each class is its own top-level
 * descriptor with its own callback signatures and its own validator. A tagged
 * union was rejected: once every member carries populated hook pointers, a
 * descriptor reinterpreted as the wrong member can validate by accident,
 * because the overlapping fields are all non-null. Separate types make that
 * mistake unrepresentable rather than merely detected.
 *
 * The session descriptors prove SEMANTIC FIN delivery and consumption; the
 * bridge descriptor proves PHYSICAL stream retirement. Neither is inferred
 * from the other, and neither is inferred from the owner disappearing. */

/* Which terminals a family offers. A family with no internal pre-visibility
 * terminal (pending-join FETCH has one; PUBLISH does not) declares its
 * absence rather than leaving a callback silently unset, and the validator
 * rejects any disagreement between a bit and its callback. */
typedef enum fin_terminal_caps {
    FIN_TERM_APP_REJECT      = 1u << 0,
    FIN_TERM_INTERNAL_REJECT = 1u << 1,
    FIN_TERM_APP_ACCEPT      = 1u << 2,
} fin_terminal_caps_t;

#define FIN_TERM_CAPS_ALL (FIN_TERM_APP_REJECT | FIN_TERM_INTERNAL_REJECT | \
                           FIN_TERM_APP_ACCEPT)

/* Namespace subscriptions carry no role, so their descriptor declares that
 * explicitly rather than leaving a zero that reads as a real role. */
#define FIN_OWNER_ROLE_NA (-1)

/* Owner-transition case: a producer P1-P7 and one of its role-valid
 * terminals, on a session. */
typedef struct fin_owner_case {
    const char        *name;
    void              *ctx;
    unsigned           caps;
    txs_op_hooks_t     hooks;          /* composed, not replaced */
    moq_result_t     (*feed)(moq_session_t *s, void *ctx,
                             moq_stream_ref_t ref, bool fin);
    moq_request_kind_t owner_kind;     /* destination the producer creates */
    int                owner_role;
    int                owner_state;
    moq_result_t     (*app_reject)(moq_session_t *s, void *ctx,
                                   uint64_t handle);
    moq_result_t     (*internal_reject)(moq_session_t *s, void *ctx,
                                        moq_stream_ref_t ref);
    /* A family whose accepting terminal also consults the FIN latch declares
     * it here rather than having the runner call it behind the model. */
    moq_result_t     (*app_accept)(moq_session_t *s, void *ctx,
                                   uint64_t handle);
} fin_owner_case_t;

/* Non-destination case: an Axis 3 terminal that can never carry a marker, so
 * it has no producer and no destination owner. */
typedef struct fin_nondest_case {
    const char     *name;
    void           *ctx;
    txs_op_hooks_t  hooks;
    moq_result_t  (*arrange)(moq_session_t *s, void *ctx, moq_stream_ref_t ref,
                             bool fin);
    moq_result_t  (*terminal)(moq_session_t *s, void *ctx,
                              moq_stream_ref_t ref);
} fin_nondest_case_t;

/* Validators return a problem count so the rejection is itself self-checkable. */
static int fin_owner_problems(const fin_owner_case_t *f)
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

static int fin_nondest_problems(const fin_nondest_case_t *f)
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
static void expect_after_call_prepare(txs_snapshot_t *snap)
{
    if (snap->event_depth == 0) snap->event_scratch_len = 0;
}

/* Fill the drain ring through the real accounting path. */
static void fill_drain_ring(moq_session_t *s)
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

static moq_result_t feed_publish(moq_session_t *s, moq_stream_ref_t ref,
                                 uint64_t request_id, uint64_t alias, bool fin)
{
    uint8_t msg[160];
    size_t n = encode_publish_msg(msg, sizeof(msg), request_id, alias);
    return moq_session_on_bidi_stream_bytes(s, ref, msg, n, fin, 1);
}

/* The owner the producer created, checked against the DESCRIPTOR's declared
 * kind, role and state -- pool entry and registry key together -- before any
 * terminal runs. Returns a problem count so the comparison is self-checkable. */
static int pub_owner_present_problems(const moq_session_t *s,
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
static void check_pub_owner_retired(moq_session_t *s, moq_stream_ref_t ref)
{
    MOQ_TEST_CHECK_EQ_INT(pub_busy_count(s), 0);
    check_unregistered(s, ref);
}


/* Drain every event / action of one operation through the family's
 * normalizers, in order. A refused record fails the test. */
static void collect_events(moq_session_t *s, const txs_op_hooks_t *h,
                           txs_norm_vec_t *out)
{
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        MOQ_TEST_CHECK(h->normalize_event(&ev, h->ctx, out));
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK(!out->overflowed);
}

static void collect_actions(moq_session_t *s, const txs_op_hooks_t *h,
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
static void check_no_output(moq_session_t *s, const txs_op_hooks_t *h)
{
    txs_norm_vec_t got;
    txs_norm_init(&got);
    collect_events(s, h, &got);
    collect_actions(s, h, &got);
    MOQ_TEST_CHECK_EQ_SIZE(got.count, 0);
    txs_norm_free(&got);
}



/* -- PUBLISH as the first owner-transition descriptor ---------------- */

/* -- PUBLISH family hooks ------------------------------------------- */

/* Mutation inventory beyond the generic snapshot: the publication entry's
 * role, its request-bidi identity, its durable FIN latch, and the transitional
 * FIN ownership a same-call handoff installs. The two FIN facts are captured
 * separately so a refusal cannot silently move the peer's close from one to
 * the other. State and generation are already covered by txs_owner_capture. */
typedef struct pub_hook_state {
    int      role;
    uint64_t handle;
    uint64_t req_stream_ref;
    int      req_recv_fin;
    int      handoff_fin;
    int      busy;
} pub_hook_state_t;

typedef struct pub_ctx {
    uint64_t         request_id;
    uint64_t         alias;
    moq_stream_ref_t ref;
    uint64_t         last_pub;   /* handle the request event surfaced */
} pub_ctx_t;

static void pub_hook_capture(const moq_session_t *s, void *vctx, void *vst)
{
    pub_ctx_t *c = (pub_ctx_t *)vctx;
    pub_hook_state_t *st = (pub_hook_state_t *)vst;
    memset(st, 0, sizeof(*st));
    st->busy = pub_busy_count(s);
    const moq_pub_entry_t *e = pub_by_ref(s, c->ref);
    if (!e) return;
    st->role = (int)e->role;
    st->handle = e->handle._opaque;
    st->req_stream_ref = e->request_stream_ref._v;
    st->req_recv_fin = e->req_recv_fin;
    st->handoff_fin = e->handoff_fin_pending;
}

static int pub_hook_check(const moq_session_t *s, void *vctx, const void *vst)
{
    pub_hook_state_t now;
    pub_hook_capture(s, vctx, &now);
    const pub_hook_state_t *want = (const pub_hook_state_t *)vst;
    int bad = 0;
    if (now.role != want->role) bad++;
    if (now.handle != want->handle) bad++;
    if (now.req_stream_ref != want->req_stream_ref) bad++;
    if (now.req_recv_fin != want->req_recv_fin) bad++;
    if (now.handoff_fin != want->handoff_fin) bad++;
    if (now.busy != want->busy) bad++;
    if (bad)
        TXS_DIAG("TXN publish hooks: role %d handle %llu ref %llu fin %d "
                 "handoff %d busy %d, expected role %d handle %llu ref %llu "
                 "fin %d handoff %d busy %d\n",
                 now.role, (unsigned long long)now.handle,
                 (unsigned long long)now.req_stream_ref, now.req_recv_fin,
                 now.handoff_fin, now.busy,
                 want->role, (unsigned long long)want->handle,
                 (unsigned long long)want->req_stream_ref, want->req_recv_fin,
                 want->handoff_fin, want->busy);
    return bad;
}

/* An unrecognized kind is a normalization FAILURE, never a kind-only record:
 * a family must describe every output its cases can produce. */
static bool pub_norm_event(const moq_event_t *ev, void *vctx,
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

static bool pub_norm_action(const moq_action_t *a, void *vctx,
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
static void want_publish_request(txs_norm_vec_t *v, uint64_t handle)
{
    txs_img_t im;
    txs_img_init(&im);
    txs_img_u64(&im, handle);
    MOQ_TEST_CHECK(txs_norm_append_img(v, MOQ_EVENT_PUBLISH_REQUEST, &im));
}

static void want_publish_finished(txs_norm_vec_t *v, uint64_t handle)
{
    txs_img_t im;
    txs_img_init(&im);
    txs_img_u64(&im, handle);
    txs_img_u64(&im, 0);            /* status */
    txs_img_u64(&im, 0);            /* stream count */
    txs_img_bytes(&im, NULL, 0);    /* no reason phrase */
    MOQ_TEST_CHECK(txs_norm_append_img(v, MOQ_EVENT_PUBLISH_FINISHED, &im));
}

static void want_reject_error_code(txs_norm_vec_t *v, moq_stream_ref_t ref,
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

static void want_reject_error_reason(txs_norm_vec_t *v, moq_stream_ref_t ref,
                                     uint64_t error_code, const char *reason)
{
    uint8_t buf[192];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    moq_bytes_t r = { (const uint8_t *)reason, strlen(reason) };
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_d18_encode_request_error(&w, error_code, 0, r), (int)MOQ_OK);
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

static void want_reject_error(txs_norm_vec_t *v, moq_stream_ref_t ref)
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
static void check_reject_only_output(moq_session_t *s,
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


static moq_result_t pub_feed(moq_session_t *s, void *ctx, moq_stream_ref_t ref,
                             bool fin)
{
    pub_ctx_t *c = (pub_ctx_t *)ctx;
    return feed_publish(s, ref, c->request_id, c->alias, fin);
}

static moq_result_t pub_app_reject(moq_session_t *s, void *ctx, uint64_t handle)
{
    (void)ctx;
    moq_publication_t pub;
    pub._opaque = handle;
    moq_reject_publish_cfg_t rj;
    moq_reject_publish_cfg_init(&rj);
    rj.error_code = MOQ_REQUEST_ERROR_NOT_SUPPORTED;
    return moq_session_reject_publish(s, pub, &rj, 1);
}

/* PUBLISH offers only the application terminal: there is no rejection path
 * reachable before the request is surfaced, so the internal capability bit is
 * deliberately clear (pending-join FETCH will set it). */
static pub_ctx_t pub_ctx = { 0, 7, { 1 }, 0 };

static fin_owner_case_t pub_family(void)
{
    fin_owner_case_t f;
    memset(&f, 0, sizeof(f));
    f.name = "publish";
    f.ctx = &pub_ctx;
    f.caps = FIN_TERM_APP_REJECT;
    f.feed = pub_feed;
    f.owner_kind = MOQ_REQ_PUBLISH;
    f.owner_role = (int)MOQ_PUB_ROLE_SUBSCRIBER;
    f.owner_state = (int)MOQ_PUB_PENDING_SUBSCRIBER;
    f.app_reject = pub_app_reject;
    f.hooks.ctx = &pub_ctx;
    f.hooks.capture = pub_hook_capture;
    f.hooks.check = pub_hook_check;
    f.hooks.normalize_event = pub_norm_event;
    f.hooks.normalize_action = pub_norm_action;
    return f;
}

static void run_pub_case(const fin_owner_case_t *f, uint32_t max_events,
                         bool fin_in_request, bool fill_ring,
                         bool reject_in_window)
{
    moq_session_t *s = make_session_caps(MOQ_PERSPECTIVE_SERVER, max_events);
    pub_ctx_t *c = (pub_ctx_t *)f->ctx;
    moq_stream_ref_t ref = c->ref;
    c->last_pub = 0;

    /* Feeding the request: with the FIN in the same chunk and room for both
     * events the teardown runs immediately; with room for only the request
     * event it defers and reports backpressure. */
    bool defer = fin_in_request && max_events == 1;
    MOQ_TEST_CHECK_EQ_INT((int)f->feed(s, f->ctx, ref, fin_in_request),
                          (int)(defer ? MOQ_ERR_WOULD_BLOCK : MOQ_OK));

    txs_norm_vec_t got, want;
    txs_norm_init(&got);
    txs_norm_init(&want);
    collect_events(s, &f->hooks, &got);
    want_publish_request(&want, c->last_pub);
    if (fin_in_request && !defer)
        want_publish_finished(&want, c->last_pub);
    failures += txs_norm_equals(&got, &want, f->name);
    txs_norm_free(&got);
    txs_norm_free(&want);

    if (fin_in_request && !defer) {
        /* Consumed in the same call: nothing left to own or to answer. */
        check_pub_owner_retired(s, ref);
        MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, 0);
        check_no_output(s, &f->hooks);
        moq_session_destroy(s);
        return;
    }

    failures += pub_owner_present_problems(s, ref, f->owner_kind,
                                           f->owner_role, f->owner_state);
    pub_hook_state_t owner_before;
    f->hooks.capture(s, f->ctx, &owner_before);
    /* The handle the request surfaced is the live owner's handle. */
    MOQ_TEST_CHECK_EQ_U64(owner_before.handle, c->last_pub);
    /* Which FIN fact the owner holds, and that it holds only one. A deferred
     * teardown keeps the peer's close as transitional ownership -- the durable
     * latch is NOT written on the way in, so a terminal that reads the latch
     * alone cannot mistake the deferral for a completed close. */
    MOQ_TEST_CHECK_EQ_INT(owner_before.handoff_fin, defer ? 1 : 0);
    MOQ_TEST_CHECK_EQ_INT(owner_before.req_recv_fin, 0);

    size_t filled = 0;
    if (fill_ring) {
        fill_drain_ring(s);
        filled = s->drain_ref_count;
    }

    if (!reject_in_window) {
        /* Retrying with no new bytes completes the deferred teardown exactly
         * once, without a drain reference and without wire output. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, false, 1),
            (int)MOQ_OK);
        txs_norm_init(&got);
        txs_norm_init(&want);
        collect_events(s, &f->hooks, &got);
        want_publish_finished(&want, c->last_pub);
        failures += txs_norm_equals(&got, &want, f->name);
        txs_norm_free(&got);
        txs_norm_free(&want);
        check_pub_owner_retired(s, ref);
        MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, 0);
        check_no_output(s, &f->hooks);
        moq_session_destroy(s);
        return;
    }

    /* The application answers. With the peer's close already observed the
     * rejection needs no drain reference, so an exhausted ring cannot refuse
     * it; without the close it must reserve one, and a full ring refuses. */
    bool expect_drain = !fin_in_request;
    moq_result_t want_rc = (expect_drain && fill_ring) ? MOQ_ERR_WOULD_BLOCK
                                                       : MOQ_OK;
    txs_snapshot_t before;
    txs_capture(s, &ref, 1, &before);
    expect_after_call_prepare(&before);
    MOQ_TEST_CHECK_EQ_INT((int)f->app_reject(s, f->ctx, c->last_pub),
                          (int)want_rc);

    if (want_rc == MOQ_ERR_WOULD_BLOCK) {
        /* Transactional blockage: the whole session, and the family's own
         * mutation inventory, are exactly as the refused call found them. */
        failures += txs_check_eq(s, &ref, 1, &before, f->name);
        failures += f->hooks.check(s, f->ctx, &owner_before);
        MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, filled);
        failures += pub_owner_present_problems(s, ref, f->owner_kind,
                                               f->owner_role, f->owner_state);
        check_no_output(s, &f->hooks);
        moq_session_destroy(s);
        return;
    }

    MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count,
                           expect_drain ? filled + 1 : filled);
    check_pub_owner_retired(s, ref);
    check_reject_only_output(s, &f->hooks, ref, f->name);

    if (expect_drain) {
        /* The peer's late FIN releases the reference and produces nothing. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, filled);
        check_pub_owner_retired(s, ref);
        check_no_output(s, &f->hooks);
    }
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
    moq_session_destroy(s);
}

/* Clear-on-reuse: a same-call FIN consumed by one publication must not follow
 * its pool slot to the next one. The second request carries no FIN, so the
 * reused owner must hold neither FIN fact -- and its terminal must still
 * reserve a drain reference for the peer's send half, which an exhausted ring
 * refuses. A marker that survived the free would make that rejection succeed
 * with no reference, silently abandoning a live peer half. */
static void run_pub_slot_reuse_case(const fin_owner_case_t *f)
{
    moq_session_t *s = make_session_caps(MOQ_PERSPECTIVE_SERVER, 0);
    pub_ctx_t *c = (pub_ctx_t *)f->ctx;

    /* The physical slot both requests must occupy, and the generations it
     * carries, derived from pool state BEFORE any ingress. Deriving them here
     * is what makes this a same-slot reuse case rather than a case that merely
     * happens to reuse whatever the allocator picked. */
    int slot = -1;
    uint32_t gen_first = 0;
    for (size_t i = 0; i < s->pub_cap; i++)
        if (s->publishes[i].state == MOQ_PUB_FREE) {
            slot = (int)i;
            gen_first = s->publishes[i].generation | 1u;
            break;
        }
    MOQ_TEST_CHECK(slot >= 0);
    if (slot < 0) { moq_session_destroy(s); return; }
    /* pub_free_entry advances the generation once; the reused owner's live
     * generation is that value re-oddified. */
    uint32_t gen_reuse = (gen_first + 1) | 1u;
    uint64_t handle_first = moq_handle_pack(MOQ_HANDLE_POOL_PUBLICATION,
                                            s->session_tag, gen_first,
                                            (uint32_t)slot);
    uint64_t handle_reuse = moq_handle_pack(MOQ_HANDLE_POOL_PUBLICATION,
                                            s->session_tag, gen_reuse,
                                            (uint32_t)slot);
    MOQ_TEST_CHECK(handle_first != handle_reuse);

    /* First request: same-call FIN, torn down immediately, slot released. */
    moq_stream_ref_t first = c->ref;
    MOQ_TEST_CHECK_EQ_INT((int)f->feed(s, f->ctx, first, true), (int)MOQ_OK);
    txs_norm_vec_t got, want;
    txs_norm_init(&got);
    txs_norm_init(&want);
    collect_events(s, &f->hooks, &got);
    want_publish_request(&want, c->last_pub);
    want_publish_finished(&want, c->last_pub);
    failures += txs_norm_equals(&got, &want, "publish reuse first");
    txs_norm_free(&got);
    txs_norm_free(&want);
    /* The handle the first request surfaced is the one the derived slot and
     * generation pack into -- the fixture's topology, not a read-back. */
    MOQ_TEST_CHECK_EQ_U64(c->last_pub, handle_first);
    check_pub_owner_retired(s, first);
    /* That exact slot is free, its generation advanced as declared, and the
     * transitional marker went with it. */
    MOQ_TEST_CHECK_EQ_INT((int)s->publishes[slot].state, (int)MOQ_PUB_FREE);
    MOQ_TEST_CHECK_EQ_U64(s->publishes[slot].generation, gen_reuse - 1u);
    MOQ_TEST_CHECK_EQ_INT(s->publishes[slot].handoff_fin_pending, 0);
    MOQ_TEST_CHECK_EQ_INT(s->publishes[slot].req_recv_fin, 0);

    /* Second request on a fresh bidi, no FIN: it lands in the released slot. */
    moq_stream_ref_t second = moq_stream_ref_from_u64(c->ref._v + 4);
    moq_stream_ref_t saved = c->ref;
    c->ref = second;
    MOQ_TEST_CHECK_EQ_INT(
        (int)feed_publish(s, second, c->request_id + 2, c->alias + 1, false),
        (int)MOQ_OK);
    /* Draining the request event both surfaces the new owner's handle and
     * proves the reused slot produced no second teardown. */
    txs_norm_init(&got);
    txs_norm_init(&want);
    collect_events(s, &f->hooks, &got);
    want_publish_request(&want, c->last_pub);
    failures += txs_norm_equals(&got, &want, "publish reuse second");
    txs_norm_free(&got);
    txs_norm_free(&want);
    failures += pub_owner_present_problems(s, second, f->owner_kind,
                                           f->owner_role, f->owner_state);
    /* The reuse landed in THAT physical slot, carrying the pre-derived
     * generation and handle, and the stream-ref registry edge points at it. */
    {
        const moq_pub_entry_t *e = pub_by_ref(s, second);
        MOQ_TEST_CHECK(e != NULL);
        if (e) {
            MOQ_TEST_CHECK_EQ_INT((int)(e - s->publishes), slot);
            MOQ_TEST_CHECK_EQ_U64(e->generation, gen_reuse);
            MOQ_TEST_CHECK_EQ_U64(e->handle._opaque, handle_reuse);
        }
        MOQ_TEST_CHECK_EQ_U64(c->last_pub, handle_reuse);
        moq_request_endpoint_t ep =
            request_registry_find_by_streamref(s, second);
        MOQ_TEST_CHECK_EQ_INT((int)ep.kind, (int)MOQ_REQ_PUBLISH);
        MOQ_TEST_CHECK_EQ_INT(ep.slot, slot);
    }
    pub_hook_state_t owner;
    f->hooks.capture(s, f->ctx, &owner);
    /* Neither FIN fact is set before the terminal check: the consumed FIN did
     * not follow the slot to its next owner. */
    MOQ_TEST_CHECK_EQ_INT(owner.handoff_fin, 0);
    MOQ_TEST_CHECK_EQ_INT(owner.req_recv_fin, 0);

    /* The peer's send half is still open, so the terminal owes a drain
     * reference; with the ring exhausted it must refuse transactionally. */
    fill_drain_ring(s);
    size_t filled = s->drain_ref_count;
    txs_snapshot_t before;
    txs_capture(s, &second, 1, &before);
    expect_after_call_prepare(&before);
    MOQ_TEST_CHECK_EQ_INT((int)f->app_reject(s, f->ctx, c->last_pub),
                          (int)MOQ_ERR_WOULD_BLOCK);
    failures += txs_check_eq(s, &second, 1, &before, "publish reuse blocked");
    failures += f->hooks.check(s, f->ctx, &owner);
    MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, filled);
    check_no_output(s, &f->hooks);

    c->ref = saved;
    moq_session_destroy(s);
}

static void test_publish(void)
{
    fin_owner_case_t f = pub_family();
    MOQ_TEST_CHECK_EQ_INT(fin_owner_problems(&f), 0);

    /* Control: no FIN in the request chunk -- one drain reference, released
     * by the peer's later FIN. */
    run_pub_case(&f, 0, false, false, true);
    /* Same-call FIN, room for both events: consumed immediately. */
    run_pub_case(&f, 0, true, false, false);
    /* Same-call FIN, room for one: deferred, then completed by the retry. */
    run_pub_case(&f, 1, true, false, false);
    /* Same-call FIN, answered inside the deferral window: no drain. */
    run_pub_case(&f, 1, true, false, true);
    /* The selector against one and the same exhausted ring. */
    run_pub_case(&f, 1, true, true, true);
    run_pub_case(&f, 1, false, true, true);
    /* The consumed FIN does not follow the slot to its next owner. */
    run_pub_slot_reuse_case(&f);
}


/* -- Anti-vacuity self-checks ---------------------------------------- */

/* Every field the normalized images carry must actually discriminate, and the
 * proof has to run through the PRODUCTION normalizers: comparing two
 * hand-built expected images would only show that txs_norm_equals() works,
 * and a normalizer hardcoding a field would survive it. So each pair below is
 * two RAW records differing in exactly one field, normalized by
 * pub_norm_event / pub_norm_action, which must then compare unequal. */

static moq_event_t mk_finish_event(uint64_t handle, uint64_t status,
                                   uint64_t streams, const char *reason)
{
    moq_event_t ev;
    memset(&ev, 0, sizeof(ev));
    ev.kind = MOQ_EVENT_PUBLISH_FINISHED;
    ev.u.publish_finished.pub._opaque = handle;
    ev.u.publish_finished.status_code = status;
    ev.u.publish_finished.stream_count = streams;
    if (reason) {
        ev.u.publish_finished.reason.data = (const uint8_t *)reason;
        ev.u.publish_finished.reason.len = strlen(reason);
    }
    return ev;
}

static moq_event_t mk_request_event(uint64_t handle)
{
    moq_event_t ev;
    memset(&ev, 0, sizeof(ev));
    ev.kind = MOQ_EVENT_PUBLISH_REQUEST;
    ev.u.publish_request.pub._opaque = handle;
    return ev;
}

/* Normalize two raw event lists through the production normalizer and require
 * the results to differ. */
static void expect_events_differ(const moq_event_t *a, size_t na,
                                 const moq_event_t *b, size_t nb,
                                 const char *what)
{
    txs_norm_vec_t va, vb;
    txs_norm_init(&va); txs_norm_init(&vb);
    for (size_t i = 0; i < na; i++)
        MOQ_TEST_CHECK(pub_norm_event(&a[i], &pub_ctx, &va));
    for (size_t i = 0; i < nb; i++)
        MOQ_TEST_CHECK(pub_norm_event(&b[i], &pub_ctx, &vb));
    txs_quiet = 1;
    int diff = txs_norm_equals(&va, &vb, what);
    txs_quiet = 0;
    if (diff == 0)
        fprintf(stderr, "FAIL: %s:%d: normalizer ignores %s\n",
                __FILE__, __LINE__, what);
    MOQ_TEST_CHECK(diff != 0);
    txs_norm_free(&va); txs_norm_free(&vb);
}

static moq_action_t mk_bidi_action(moq_stream_ref_t ref, bool fin,
                                   const uint8_t *data, size_t len)
{
    moq_action_t a;
    memset(&a, 0, sizeof(a));
    a.kind = MOQ_ACTION_SEND_BIDI_STREAM;
    a.u.send_bidi_stream.stream_ref = ref;
    a.u.send_bidi_stream.fin = fin;
    a.u.send_bidi_stream.data = data;
    a.u.send_bidi_stream.len = len;
    return a;
}

static moq_action_t mk_reset_data_action(moq_stream_ref_t ref, uint64_t code)
{
    moq_action_t a;
    memset(&a, 0, sizeof(a));
    a.kind = MOQ_ACTION_RESET_DATA;
    a.u.reset_data.stream_ref = ref;
    a.u.reset_data.error_code = code;
    return a;
}

static size_t enc_request_error(uint8_t *buf, size_t cap, uint64_t code,
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
static size_t enc_typed_envelope(uint8_t *buf, size_t cap, uint64_t msg_type,
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

static void expect_actions_differ(const moq_action_t *a, const moq_action_t *b,
                                  const char *what)
{
    txs_norm_vec_t va, vb;
    txs_norm_init(&va); txs_norm_init(&vb);
    MOQ_TEST_CHECK(pub_norm_action(a, &pub_ctx, &va));
    MOQ_TEST_CHECK(pub_norm_action(b, &pub_ctx, &vb));
    txs_quiet = 1;
    int diff = txs_norm_equals(&va, &vb, what);
    txs_quiet = 0;
    if (diff == 0)
        fprintf(stderr, "FAIL: %s:%d: normalizer ignores %s\n",
                __FILE__, __LINE__, what);
    MOQ_TEST_CHECK(diff != 0);
    txs_norm_free(&va); txs_norm_free(&vb);
}

static void check_image_fields(void)
{
    const uint64_t H = 0x1234;

    /* Event order: the SAME two raw records, normalized in opposite order. */
    {
        moq_event_t fwd[2] = { mk_request_event(H),
                               mk_finish_event(H, 0, 0, NULL) };
        moq_event_t rev[2] = { mk_finish_event(H, 0, 0, NULL),
                               mk_request_event(H) };
        expect_events_differ(fwd, 2, rev, 2, "event order");
    }
    /* One event field at a time. */
    {
        moq_event_t a = mk_finish_event(H, 0, 0, NULL);
        moq_event_t b = mk_finish_event(H + 1, 0, 0, NULL);
        expect_events_differ(&a, 1, &b, 1, "finish handle");
    }
    {
        moq_event_t a = mk_finish_event(H, 0, 0, NULL);
        moq_event_t b = mk_finish_event(H, 7, 0, NULL);
        expect_events_differ(&a, 1, &b, 1, "finish status");
    }
    {
        moq_event_t a = mk_finish_event(H, 0, 0, NULL);
        moq_event_t b = mk_finish_event(H, 0, 3, NULL);
        expect_events_differ(&a, 1, &b, 1, "finish stream count");
    }
    {
        moq_event_t a = mk_finish_event(H, 0, 0, NULL);
        moq_event_t b = mk_finish_event(H, 0, 0, "gone");
        expect_events_differ(&a, 1, &b, 1, "finish reason");
    }
    {
        moq_event_t a = mk_request_event(H);
        moq_event_t b = mk_request_event(H + 1);
        expect_events_differ(&a, 1, &b, 1, "request handle");
    }

    /* One action field at a time. */
    uint8_t base[160], other_code[160], with_reason[160];
    size_t base_len = enc_request_error(base, sizeof(base),
                                        MOQ_REQUEST_ERROR_NOT_SUPPORTED, 0,
                                        NULL);
    size_t other_len = enc_request_error(other_code, sizeof(other_code),
                                         MOQ_REQUEST_ERROR_UNAUTHORIZED, 0,
                                         NULL);
    size_t reason_len = enc_request_error(with_reason, sizeof(with_reason),
                                          MOQ_REQUEST_ERROR_NOT_SUPPORTED, 0,
                                          "no");
    moq_stream_ref_t r1 = moq_stream_ref_from_u64(1);
    moq_stream_ref_t r2 = moq_stream_ref_from_u64(2);
    {
        moq_action_t a = mk_bidi_action(r1, true, base, base_len);
        moq_action_t b = mk_bidi_action(r2, true, base, base_len);
        expect_actions_differ(&a, &b, "action stream ref");
    }
    {
        moq_action_t a = mk_bidi_action(r1, true, base, base_len);
        moq_action_t b = mk_bidi_action(r1, false, base, base_len);
        expect_actions_differ(&a, &b, "action FIN");
    }
    {
        /* Same message type, different payload bytes. */
        moq_action_t a = mk_bidi_action(r1, true, base, base_len);
        moq_action_t b = mk_bidi_action(r1, true, other_code, other_len);
        expect_actions_differ(&a, &b, "action payload");
    }
    {
        moq_action_t a = mk_bidi_action(r1, true, base, base_len);
        moq_action_t b = mk_bidi_action(r1, true, with_reason, reason_len);
        expect_actions_differ(&a, &b, "action payload length");
    }
    {
        /* Message type ALONE: two envelopes whose payload bytes and length are
         * identical, differing only in the type field. Comparing a REQUEST_OK
         * against a REQUEST_ERROR would not isolate it -- their payloads
         * differ too, so an ignored type would still show unequal images. */
        uint8_t t1[64], t2[64];
        static const uint8_t body[] = { 0x01, 0x02, 0x03, 0x04 };
        size_t n1 = enc_typed_envelope(t1, sizeof(t1), MOQ_D18_REQUEST_ERROR,
                                       body, sizeof(body));
        size_t n2 = enc_typed_envelope(t2, sizeof(t2), MOQ_D18_REQUEST_OK,
                                       body, sizeof(body));
        moq_action_t a = mk_bidi_action(r1, true, t1, n1);
        moq_action_t b = mk_bidi_action(r1, true, t2, n2);
        expect_actions_differ(&a, &b, "action message type");
    }
    /* The data-cancellation record's own two fields, one at a time. */
    {
        moq_action_t a = mk_reset_data_action(r1, 0x1);
        moq_action_t b = mk_reset_data_action(r2, 0x1);
        expect_actions_differ(&a, &b, "reset stream ref");
    }
    {
        moq_action_t a = mk_reset_data_action(r1, 0x1);
        moq_action_t b = mk_reset_data_action(r1, 0x2);
        expect_actions_differ(&a, &b, "reset error code");
    }
    {
        /* Kind ALONE. The two action shapes cannot be made to produce the same
         * image through their builders -- their field lists differ -- so the
         * record kind is isolated at the normalization layer instead: one
         * identical image appended under two kinds must not compare equal. */
        txs_img_t im;
        txs_img_init(&im);
        txs_img_u64(&im, r1._v);
        txs_img_u64(&im, 0x1);
        txs_norm_vec_t va, vb;
        txs_norm_init(&va); txs_norm_init(&vb);
        MOQ_TEST_CHECK(txs_norm_append_img(&va, MOQ_ACTION_RESET_DATA, &im));
        MOQ_TEST_CHECK(txs_norm_append_img(&vb, MOQ_ACTION_STOP_DATA, &im));
        txs_quiet = 1;
        MOQ_TEST_CHECK(txs_norm_equals(&va, &vb, "action kind") != 0);
        txs_quiet = 0;
        txs_norm_free(&va); txs_norm_free(&vb);
    }

    /* Trailing bytes after the envelope are refused outright. */
    {
        uint8_t buf[160];
        size_t n = enc_request_error(buf, sizeof(buf) - 1,
                                     MOQ_REQUEST_ERROR_NOT_SUPPORTED, 0, NULL);
        buf[n] = 0x00;
        moq_action_t a = mk_bidi_action(r1, true, buf, n + 1);
        txs_norm_vec_t v;
        txs_norm_init(&v);
        txs_quiet = 1;
        MOQ_TEST_CHECK(!pub_norm_action(&a, &pub_ctx, &v));
        txs_quiet = 0;
        MOQ_TEST_CHECK_EQ_SIZE(v.count, 0);
        txs_norm_free(&v);
    }
}

/* The declared owner kind, role and state are compared, not merely stored:
 * perturbing any one of them must fail the comparison. */
static void check_owner_metadata(void)
{
    fin_owner_case_t f = pub_family();
    pub_ctx_t *c = (pub_ctx_t *)f.ctx;
    moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
    MOQ_TEST_CHECK_EQ_INT((int)f.feed(s, f.ctx, c->ref, false), (int)MOQ_OK);
    txs_norm_vec_t got;
    txs_norm_init(&got);
    collect_events(s, &f.hooks, &got);
    txs_norm_free(&got);

    MOQ_TEST_CHECK_EQ_INT(
        pub_owner_present_problems(s, c->ref, f.owner_kind, f.owner_role,
                                   f.owner_state), 0);
    txs_quiet = 1;
    MOQ_TEST_CHECK(pub_owner_present_problems(s, c->ref, MOQ_REQ_FETCH,
                                              f.owner_role, f.owner_state) > 0);
    MOQ_TEST_CHECK(pub_owner_present_problems(s, c->ref, f.owner_kind,
                                              (int)MOQ_PUB_ROLE_PUBLISHER,
                                              f.owner_state) > 0);
    MOQ_TEST_CHECK(pub_owner_present_problems(s, c->ref, f.owner_kind,
                                              f.owner_role,
                                              (int)MOQ_PUB_ESTABLISHED) > 0);
    txs_quiet = 0;
    moq_session_destroy(s);
}


/* -- FETCH (P1 standalone) ------------------------------------------- */

/* FETCH's owner-present FIN outcome is LATCH AND RETAIN, not teardown: the
 * fetch entry survives the peer's close and records it, because the response
 * and any data stream are still to come. Owner disappearance is therefore
 * never the evidence of FIN consumption here -- the latch is. */

typedef struct fetch_ctx {
    uint64_t         request_id;
    moq_stream_ref_t ref;
    uint64_t         last_fetch;   /* handle the request event surfaced */
} fetch_ctx_t;

typedef struct fetch_hook_state {
    int      present;
    uint64_t handle;
    uint64_t req_stream_ref;
    int      req_recv_fin;
    /* The transitional FIN ownership a same-call handoff installs. Both
     * publisher-role FETCH states consume it synchronously into req_recv_fin,
     * so every observation point must find it already clear. */
    int      handoff_fin;
    int      state;
    int      role;
    int      busy;
} fetch_hook_state_t;

static size_t encode_fetch_msg(uint8_t *buf, size_t cap, uint64_t request_id)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live"),
                            MOQ_BYTES_LITERAL("eu") };
    moq_d18_fetch_t f;
    memset(&f, 0, sizeof(f));
    f.request_id = request_id;
    f.fetch_type = 1;
    f.track_namespace = (moq_namespace_t){ parts, 2 };
    f.track_name = MOQ_BYTES_LITERAL("vid");
    /* Deliberately non-default: a field the normalizer drops would otherwise
     * compare equal against a zero-filled expectation. */
    f.start = (moq_d18_location_t){ 3, 4 };
    f.end = (moq_d18_location_t){ 11, 6 };
    f.params.has_subscriber_priority = true;
    f.params.subscriber_priority = 9;
    f.params.has_group_order = true;
    f.params.group_order = MOQ_GROUP_ORDER_DESCENDING;
    f.params.auth_token_count = 1;
    f.params.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_USE_VALUE;
    f.params.auth_tokens[0].token_type = 5;
    f.params.auth_tokens[0].token_value = MOQ_BYTES_LITERAL("tok");
    MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_fetch(&w, &f), (int)MOQ_OK);
    return moq_buf_writer_offset(&w);
}

static int fetch_busy_count(const moq_session_t *s)
{
    int n = 0;
    for (size_t i = 0; i < s->fetch_cap; i++)
        if (s->fetches[i].state != MOQ_FETCH_FREE) n++;
    return n;
}

static const moq_fetch_entry_t *fetch_by_ref(const moq_session_t *s,
                                             moq_stream_ref_t ref)
{
    for (size_t i = 0; i < s->fetch_cap; i++) {
        const moq_fetch_entry_t *e = &s->fetches[i];
        if (e->state != MOQ_FETCH_FREE && e->request_stream_ref._v == ref._v)
            return e;
    }
    return NULL;
}

/* The FETCH destination owner's identity, DECLARED from pool state before the
 * request is fed. Every field is derived, never read back: the slot the free
 * scan will choose, the live generation that slot will carry, the handle those
 * two pack into, and the request id / stream ref the fixture itself supplies.
 * One family-owned record so P1 and P2 assert the same absolute shape. */
typedef struct fetch_ident {
    int              slot;
    uint32_t         gen;
    uint64_t         handle;
    uint64_t         request_id;
    moq_stream_ref_t ref;
} fetch_ident_t;

static fetch_ident_t fetch_declare_ident(const moq_session_t *s,
                                         uint64_t request_id,
                                         moq_stream_ref_t ref)
{
    fetch_ident_t d;
    memset(&d, 0, sizeof(d));
    d.slot = -1;
    for (size_t i = 0; i < s->fetch_cap; i++)
        if (s->fetches[i].state == MOQ_FETCH_FREE) {
            d.slot = (int)i;
            d.gen = s->fetches[i].generation | 1u;
            break;
        }
    MOQ_TEST_CHECK(d.slot >= 0);
    d.request_id = request_id;
    d.ref = ref;
    if (d.slot >= 0)
        d.handle = moq_handle_pack(MOQ_HANDLE_POOL_FETCH, s->session_tag,
                                   d.gen, (uint32_t)d.slot);
    return d;
}

/* The committed entry and its stream-ref endpoint must match the declaration
 * exactly -- physical slot, generation, handle, request id, role, state, and
 * the registry edge's own ref/slot. Returns a problem count. */
static int fetch_ident_problems(const moq_session_t *s,
                                const fetch_ident_t *d,
                                int want_role, int want_state,
                                const char *what)
{
    int bad = 0;
    const moq_fetch_entry_t *e = fetch_by_ref(s, d->ref);
    if (!e) {
        TXS_DIAG("TXN fetch ident %s: no owner behind ref %llu\n", what,
                 (unsigned long long)d->ref._v);
        return 1;
    }
    if ((int)(e - s->fetches) != d->slot) bad++;
    if (e->generation != d->gen) bad++;
    if (e->handle._opaque != d->handle) bad++;
    if (e->request_id != d->request_id) bad++;
    if ((int)e->role != want_role) bad++;
    if ((int)e->state != want_state) bad++;
    moq_request_endpoint_t ep = request_registry_find_by_streamref(s, d->ref);
    if ((int)ep.kind != (int)MOQ_REQ_FETCH) bad++;
    else {
        if (ep.slot != d->slot) bad++;
        if (!ep.has_stream_ref) bad++;
        else if (ep.stream_ref._v != d->ref._v) bad++;
    }
    if (bad)
        TXS_DIAG("TXN fetch ident %s: slot %d gen %u handle %llu rid %llu "
                 "role %d state %d endpoint(kind %d slot %d), expected "
                 "slot %d gen %u handle %llu rid %llu role %d state %d "
                 "endpoint(kind %d slot %d)\n", what,
                 (int)(e - s->fetches), e->generation,
                 (unsigned long long)e->handle._opaque,
                 (unsigned long long)e->request_id, (int)e->role,
                 (int)e->state, (int)ep.kind, ep.slot,
                 d->slot, d->gen, (unsigned long long)d->handle,
                 (unsigned long long)d->request_id, want_role, want_state,
                 (int)MOQ_REQ_FETCH, d->slot);
    return bad;
}

/* The declared owner's EXACT graph edge set: one stream-ref edge from its
 * request bidi, and nothing else anywhere pointing at it. */
static int fetch_ident_edges(const og_graph_t *g, const fetch_ident_t *d,
                             const char *what)
{
    og_edge_spec_t want[1];
    want[0].domain = OG_DOM_REQ_STREAMREF;
    want[0].key = d->ref._v;
    return og_check_owner_edges(g, (int)MOQ_REQ_FETCH, d->slot, want, 1, what);
}

static moq_result_t fetch_feed(moq_session_t *s, void *ctx,
                               moq_stream_ref_t ref, bool fin)
{
    fetch_ctx_t *c = (fetch_ctx_t *)ctx;
    uint8_t msg[192];
    size_t n = encode_fetch_msg(msg, sizeof(msg), c->request_id);
    return moq_session_on_bidi_stream_bytes(s, ref, msg, n, fin, 1);
}

static moq_result_t fetch_app_reject(moq_session_t *s, void *ctx,
                                     uint64_t handle)
{
    (void)ctx;
    moq_fetch_t fh;
    fh._opaque = handle;
    moq_reject_fetch_cfg_t rj;
    moq_reject_fetch_cfg_init(&rj);
    rj.error_code = MOQ_REQUEST_ERROR_NOT_SUPPORTED;
    return moq_session_reject_fetch(s, fh, &rj, 1);
}

static void fetch_hook_capture(const moq_session_t *s, void *vctx, void *vst)
{
    fetch_ctx_t *c = (fetch_ctx_t *)vctx;
    fetch_hook_state_t *st = (fetch_hook_state_t *)vst;
    memset(st, 0, sizeof(*st));
    st->busy = fetch_busy_count(s);
    const moq_fetch_entry_t *e = fetch_by_ref(s, c->ref);
    if (!e) return;
    st->present = 1;
    st->handle = e->handle._opaque;
    st->req_stream_ref = e->request_stream_ref._v;
    st->req_recv_fin = e->req_recv_fin;
    st->handoff_fin = e->handoff_fin_pending;
    st->state = (int)e->state;
    st->role = (int)e->role;
}

static int fetch_hook_check(const moq_session_t *s, void *vctx,
                            const void *vst)
{
    fetch_hook_state_t now;
    fetch_hook_capture(s, vctx, &now);
    const fetch_hook_state_t *want = (const fetch_hook_state_t *)vst;
    int bad = 0;
    if (now.present != want->present) bad++;
    if (now.handle != want->handle) bad++;
    if (now.req_stream_ref != want->req_stream_ref) bad++;
    if (now.req_recv_fin != want->req_recv_fin) bad++;
    if (now.handoff_fin != want->handoff_fin) bad++;
    if (now.state != want->state) bad++;
    if (now.role != want->role) bad++;
    if (now.busy != want->busy) bad++;
    if (bad)
        TXS_DIAG("TXN fetch hooks: present %d handle %llu ref %llu fin %d "
                 "state %d role %d busy %d, expected present %d handle %llu "
                 "ref %llu fin %d state %d role %d busy %d\n",
                 now.present, (unsigned long long)now.handle,
                 (unsigned long long)now.req_stream_ref, now.req_recv_fin,
                 now.state, now.role, now.busy, want->present,
                 (unsigned long long)want->handle,
                 (unsigned long long)want->req_stream_ref, want->req_recv_fin,
                 want->state, want->role, want->busy);
    return bad;
}

static bool fetch_norm_event(const moq_event_t *ev, void *vctx,
                             txs_norm_vec_t *out)
{
    fetch_ctx_t *c = (fetch_ctx_t *)vctx;
    txs_img_t im;
    txs_img_init(&im);
    switch (ev->kind) {
    case MOQ_EVENT_FETCH_REQUEST: {
        /* The complete surfaced inventory, matching jf_norm_event: anything
         * omitted here could be wrong on the wire and still compare equal. */
        const moq_fetch_request_event_t *fr = &ev->u.fetch_request;
        c->last_fetch = fr->fetch._opaque;
        txs_img_u64(&im, fr->fetch._opaque);
        txs_img_u64(&im, fr->joining_sub._opaque);
        if (!txs_img_parts(&im, "fetch_request namespace",
                           fr->track_namespace.parts,
                           fr->track_namespace.count,
                           MOQ_DECODED_MAX_NAMESPACE_PARTS))
            return false;
        txs_img_bytes(&im, fr->track_name.data, fr->track_name.len);
        txs_img_u64(&im, fr->start_group);
        txs_img_u64(&im, fr->start_object);
        txs_img_u64(&im, fr->end_group);
        txs_img_u64(&im, fr->end_object);
        txs_img_u64(&im, fr->subscriber_priority);
        txs_img_u64(&im, (uint64_t)fr->group_order);
        if (!txs_img_tokens(&im, "fetch_request tokens", fr->tokens,
                            fr->token_count, MOQ_DECODED_MAX_TOKENS))
            return false;
        break;
    }
    case MOQ_EVENT_FETCH_CANCELLED:
        txs_img_u64(&im, ev->u.fetch_cancelled.fetch._opaque);
        break;
    default:
        TXS_DIAG("TXN fetch: unnormalized event kind %u\n",
                 (unsigned)ev->kind);
        return false;
    }
    return txs_norm_append_img(out, ev->kind, &im);
}

static bool fetch_norm_action(const moq_action_t *a, void *vctx,
                              txs_norm_vec_t *out)
{
    return pub_norm_action(a, vctx, out);
}

/* Built independently of the normalizer's own reading of the event, from the
 * values the fixture put on the wire. */
static void want_fetch_request(txs_norm_vec_t *v, uint64_t handle)
{
    txs_img_t im;
    txs_img_init(&im);
    txs_img_u64(&im, handle);
    txs_img_u64(&im, 0);                    /* standalone: no joining sub */
    txs_img_u64(&im, 2);                    /* namespace parts */
    txs_img_bytes(&im, (const uint8_t *)"live", 4);
    txs_img_bytes(&im, (const uint8_t *)"eu", 2);
    txs_img_bytes(&im, (const uint8_t *)"vid", 3);
    txs_img_u64(&im, 3);                    /* start group */
    txs_img_u64(&im, 4);                    /* start object */
    txs_img_u64(&im, 11);                   /* end group */
    txs_img_u64(&im, 6);                    /* end object */
    txs_img_u64(&im, 9);                    /* subscriber priority */
    txs_img_u64(&im, (uint64_t)MOQ_GROUP_ORDER_DESCENDING);
    txs_img_u64(&im, 1);                    /* token count */
    txs_img_u64(&im, 5);                    /* token type */
    txs_img_bytes(&im, (const uint8_t *)"tok", 3);
    MOQ_TEST_CHECK(txs_norm_append_img(v, MOQ_EVENT_FETCH_REQUEST, &im));
}

static fetch_ctx_t fetch_ctx = { 0, { 1 }, 0 };

static fin_owner_case_t fetch_family(void)
{
    fin_owner_case_t f;
    memset(&f, 0, sizeof(f));
    f.name = "fetch";
    f.ctx = &fetch_ctx;
    f.caps = FIN_TERM_APP_REJECT;
    f.feed = fetch_feed;
    f.owner_kind = MOQ_REQ_FETCH;
    f.owner_role = (int)MOQ_FETCH_ROLE_PUBLISHER;
    f.owner_state = (int)MOQ_FETCH_PENDING_PUBLISHER;
    f.app_reject = fetch_app_reject;
    f.hooks.ctx = &fetch_ctx;
    f.hooks.capture = fetch_hook_capture;
    f.hooks.check = fetch_hook_check;
    f.hooks.normalize_event = fetch_norm_event;
    f.hooks.normalize_action = fetch_norm_action;
    return f;
}

/* The declared owner, present with the declared role and state, and keyed in
 * the registry. Returns a problem count. */
static int fetch_owner_present_problems(const moq_session_t *s,
                                        moq_stream_ref_t ref,
                                        moq_request_kind_t want_kind,
                                        int want_role, int want_state)
{
    int bad = 0;
    const moq_fetch_entry_t *e = fetch_by_ref(s, ref);
    if (!e) return 1;
    if ((int)e->role != want_role) bad++;
    if ((int)e->state != want_state) bad++;
    if (fetch_busy_count(s) != 1) bad++;
    moq_request_endpoint_t ep = request_registry_find_by_streamref(s, ref);
    if ((int)ep.kind != (int)want_kind) bad++;
    else if (ep.slot != (int)(e - s->fetches)) bad++;
    else if (!ep.has_stream_ref || ep.stream_ref._v != ref._v) bad++;
    if (bad)
        TXS_DIAG("TXN fetch owner: kind %d role %d state %d, expected "
                 "kind %d role %d state %d\n", (int)ep.kind, (int)e->role,
                 (int)e->state, (int)want_kind, want_role, want_state);
    return bad;
}

static void run_fetch_case(const fin_owner_case_t *f, bool fin_in_request,
                           bool fill_ring, bool reject_now)
{
    moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
    fetch_ctx_t *c = (fetch_ctx_t *)f->ctx;
    moq_stream_ref_t ref = c->ref;
    c->last_fetch = 0;

    /* The destination owner's identity, declared from pool state BEFORE the
     * request is fed, so the checks below are absolute rather than adopted. */
    fetch_ident_t id = fetch_declare_ident(s, c->request_id, ref);

    MOQ_TEST_CHECK_EQ_INT((int)f->feed(s, f->ctx, ref, fin_in_request),
                          (int)MOQ_OK);
    txs_norm_vec_t got, want;
    txs_norm_init(&got);
    txs_norm_init(&want);
    collect_events(s, &f->hooks, &got);
    want_fetch_request(&want, c->last_fetch);
    failures += txs_norm_equals(&got, &want, f->name);
    txs_norm_free(&got);
    txs_norm_free(&want);

    /* Latch and RETAIN: the owner survives the peer's close, and the close is
     * recorded on it. The owner still being present is the precondition here,
     * never the proof that the FIN was consumed. */
    failures += fetch_owner_present_problems(s, ref, f->owner_kind,
                                             f->owner_role, f->owner_state);
    failures += fetch_ident_problems(s, &id, f->owner_role, f->owner_state,
                                     f->name);
    /* The event surfaced the independently packed handle. */
    MOQ_TEST_CHECK_EQ_U64(c->last_fetch, id.handle);
    {   /* and the owner carries exactly its own request-bidi edge. */
        og_graph_t g;
        og_capture(s, &g);
        failures += og_check_integrity(&g, f->name);
        failures += fetch_ident_edges(&g, &id, f->name);
    }
    fetch_hook_state_t owner;
    f->hooks.capture(s, f->ctx, &owner);
    MOQ_TEST_CHECK(owner.present);
    MOQ_TEST_CHECK_EQ_U64(owner.handle, c->last_fetch);
    MOQ_TEST_CHECK_EQ_INT(owner.req_recv_fin, fin_in_request ? 1 : 0);
    /* Both publisher-role FETCH states consume a handed-over FIN synchronously
     * into the durable latch, so the transitional fact never survives the call
     * that installed it and the terminals below read the latch. */
    MOQ_TEST_CHECK_EQ_INT(owner.handoff_fin, 0);
    MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, 0);
    check_no_output(s, &f->hooks);

    size_t filled = 0;
    if (fill_ring) { fill_drain_ring(s); filled = s->drain_ref_count; }
    if (!reject_now) { moq_session_destroy(s); return; }

    bool expect_drain = !fin_in_request;
    moq_result_t want_rc = (expect_drain && fill_ring) ? MOQ_ERR_WOULD_BLOCK
                                                       : MOQ_OK;
    txs_snapshot_t before;
    txs_capture(s, &ref, 1, &before);
    expect_after_call_prepare(&before);
    fetch_hook_state_t owner_before;
    f->hooks.capture(s, f->ctx, &owner_before);
    MOQ_TEST_CHECK_EQ_INT((int)f->app_reject(s, f->ctx, c->last_fetch),
                          (int)want_rc);

    if (want_rc == MOQ_ERR_WOULD_BLOCK) {
        failures += txs_check_eq(s, &ref, 1, &before, f->name);
        failures += f->hooks.check(s, f->ctx, &owner_before);
        MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, filled);
        check_no_output(s, &f->hooks);
        moq_session_destroy(s);
        return;
    }

    MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count,
                           expect_drain ? filled + 1 : filled);
    MOQ_TEST_CHECK_EQ_INT(fetch_busy_count(s), 0);
    check_unregistered(s, ref);
    check_reject_only_output(s, &f->hooks, ref, f->name);

    if (expect_drain) {
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, filled);
        /* Consuming the late FIN must not resurrect the retired owner. */
        MOQ_TEST_CHECK_EQ_INT(fetch_busy_count(s), 0);
        check_unregistered(s, ref);
        check_no_output(s, &f->hooks);
    }
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
    moq_session_destroy(s);
}

/* Clear-on-reuse for the FETCH pool: a same-call FIN consumed by one fetch must
 * not follow its slot to the next one. The declared topology -- slot, both
 * generations and both packed handles -- is derived BEFORE any ingress, so the
 * evidence rests on the fixture rather than on whichever slot the allocator
 * happened to pick. */
static void run_fetch_slot_reuse_case(const fin_owner_case_t *f)
{
    moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
    fetch_ctx_t *c = (fetch_ctx_t *)f->ctx;

    int slot = -1;
    uint32_t gen_first = 0;
    for (size_t i = 0; i < s->fetch_cap; i++)
        if (s->fetches[i].state == MOQ_FETCH_FREE) {
            slot = (int)i;
            gen_first = s->fetches[i].generation | 1u;
            break;
        }
    MOQ_TEST_CHECK(slot >= 0);
    if (slot < 0) { moq_session_destroy(s); return; }
    /* fetch_free_entry advances the generation once. */
    uint32_t gen_reuse = (gen_first + 1) | 1u;
    uint64_t handle_first = moq_handle_pack(MOQ_HANDLE_POOL_FETCH,
                                            s->session_tag, gen_first,
                                            (uint32_t)slot);
    uint64_t handle_reuse = moq_handle_pack(MOQ_HANDLE_POOL_FETCH,
                                            s->session_tag, gen_reuse,
                                            (uint32_t)slot);
    MOQ_TEST_CHECK(handle_first != handle_reuse);

    /* 1. A standalone FETCH carrying FIN lands in the declared slot and
     *    consumes the marker into the durable latch, retaining the owner. */
    moq_stream_ref_t first = c->ref;
    MOQ_TEST_CHECK_EQ_INT((int)f->feed(s, f->ctx, first, true), (int)MOQ_OK);
    txs_norm_vec_t got, want;
    txs_norm_init(&got);
    txs_norm_init(&want);
    collect_events(s, &f->hooks, &got);
    want_fetch_request(&want, c->last_fetch);
    failures += txs_norm_equals(&got, &want, "fetch reuse first");
    txs_norm_free(&got);
    txs_norm_free(&want);
    MOQ_TEST_CHECK_EQ_U64(c->last_fetch, handle_first);
    {
        const moq_fetch_entry_t *e = fetch_by_ref(s, first);
        MOQ_TEST_CHECK(e != NULL);
        if (e) {
            MOQ_TEST_CHECK_EQ_INT((int)(e - s->fetches), slot);
            MOQ_TEST_CHECK_EQ_U64(e->generation, gen_first);
            MOQ_TEST_CHECK_EQ_INT(e->req_recv_fin, 1);
            MOQ_TEST_CHECK_EQ_INT(e->handoff_fin_pending, 0);
        }
    }
    /* Its rejection is drainless -- the peer already closed -- and retires it. */
    MOQ_TEST_CHECK_EQ_INT((int)f->app_reject(s, f->ctx, handle_first),
                          (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, 0);
    /* Its output is the family's EXACT rejection -- one REQUEST_ERROR carrying
     * NOT_SUPPORTED on this ref, and nothing else. Draining it unchecked would
     * let a wrong payload, an extra event or a duplicate action become the
     * baseline the reuse leg then measures against. */
    check_reject_only_output(s, &f->hooks, first, "fetch reuse first reject");
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);

    /* 2. That exact slot is free, its generation advanced, both FIN facts
     *    clear, and no edge anywhere still points at the retired owner. */
    MOQ_TEST_CHECK_EQ_INT((int)s->fetches[slot].state, (int)MOQ_FETCH_FREE);
    MOQ_TEST_CHECK_EQ_U64(s->fetches[slot].generation, gen_reuse - 1u);
    MOQ_TEST_CHECK_EQ_INT(s->fetches[slot].req_recv_fin, 0);
    MOQ_TEST_CHECK_EQ_INT(s->fetches[slot].handoff_fin_pending, 0);
    check_unregistered(s, first);
    {
        og_graph_t g;
        og_capture(s, &g);
        failures += og_check_integrity(&g, "fetch reuse retired");
        failures += og_check_no_edge(&g, OG_DOM_REQ_STREAMREF, first._v,
                                     "fetch reuse retired");
        failures += og_check_owner_unreferenced(&g, (int)MOQ_REQ_FETCH, slot,
                                                "fetch reuse retired");
    }

    /* 3. A distinct no-FIN FETCH reuses that physical slot. */
    moq_stream_ref_t second = moq_stream_ref_from_u64(c->ref._v + 4);
    moq_stream_ref_t saved_ref = c->ref;
    uint64_t saved_id = c->request_id;
    c->ref = second;
    c->request_id = saved_id + 2;
    MOQ_TEST_CHECK_EQ_INT((int)f->feed(s, f->ctx, second, false), (int)MOQ_OK);
    txs_norm_init(&got);
    txs_norm_init(&want);
    collect_events(s, &f->hooks, &got);
    want_fetch_request(&want, c->last_fetch);
    failures += txs_norm_equals(&got, &want, "fetch reuse second");
    txs_norm_free(&got);
    txs_norm_free(&want);
    failures += fetch_owner_present_problems(s, second, f->owner_kind,
                                             f->owner_role, f->owner_state);
    {
        const moq_fetch_entry_t *e = fetch_by_ref(s, second);
        MOQ_TEST_CHECK(e != NULL);
        if (e) {
            MOQ_TEST_CHECK_EQ_INT((int)(e - s->fetches), slot);
            MOQ_TEST_CHECK_EQ_U64(e->generation, gen_reuse);
            MOQ_TEST_CHECK_EQ_U64(e->handle._opaque, handle_reuse);
            MOQ_TEST_CHECK_EQ_INT(e->req_recv_fin, 0);
            MOQ_TEST_CHECK_EQ_INT(e->handoff_fin_pending, 0);
        }
        MOQ_TEST_CHECK_EQ_U64(c->last_fetch, handle_reuse);
        moq_request_endpoint_t ep =
            request_registry_find_by_streamref(s, second);
        MOQ_TEST_CHECK_EQ_INT((int)ep.kind, (int)MOQ_REQ_FETCH);
        MOQ_TEST_CHECK_EQ_INT(ep.slot, slot);
    }

    /* 4. Its peer half is genuinely open, so the rejection owes a drain
     *    reference; an exhausted ring refuses it transactionally. The VALID
     *    second-owner topology is captured first: a snapshot of kind/slot/
     *    state/generation cannot see an undeclared or repointed edge, so the
     *    graph is what proves nothing was inserted during the refusal. */
    fill_drain_ring(s);
    size_t filled = s->drain_ref_count;
    fetch_ident_t id2;
    memset(&id2, 0, sizeof(id2));
    id2.slot = slot; id2.gen = gen_reuse; id2.handle = handle_reuse;
    id2.request_id = c->request_id; id2.ref = second;
    og_graph_t graph_before;
    og_capture(s, &graph_before);
    failures += og_check_integrity(&graph_before, "fetch reuse live");
    failures += fetch_ident_edges(&graph_before, &id2, "fetch reuse live");

    fetch_hook_state_t owner;
    f->hooks.capture(s, f->ctx, &owner);
    txs_snapshot_t before;
    txs_capture(s, &second, 1, &before);
    expect_after_call_prepare(&before);
    MOQ_TEST_CHECK_EQ_INT((int)f->app_reject(s, f->ctx, handle_reuse),
                          (int)MOQ_ERR_WOULD_BLOCK);
    failures += txs_check_eq(s, &second, 1, &before, "fetch reuse blocked");
    failures += f->hooks.check(s, f->ctx, &owner);
    failures += fetch_ident_problems(s, &id2, f->owner_role, f->owner_state,
                                     "fetch reuse blocked");
    MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, filled);
    {
        og_graph_t g;
        og_capture(s, &g);
        failures += og_check_integrity(&g, "fetch reuse blocked");
        failures += fetch_ident_edges(&g, &id2, "fetch reuse blocked");
        failures += og_check_same_topology(&g, &graph_before,
                                           "fetch reuse blocked");
    }
    check_no_output(s, &f->hooks);

    /* 5. The retired first-generation handle stays stale after the reuse. Its
     *    own snapshot: a snapshot may not be carried across two advancing API
     *    calls merely because today's call preparation normalizes to the same
     *    values. */
    txs_snapshot_t before_stale;
    txs_capture(s, &second, 1, &before_stale);
    expect_after_call_prepare(&before_stale);
    MOQ_TEST_CHECK_EQ_INT((int)f->app_reject(s, f->ctx, handle_first),
                          (int)MOQ_ERR_STALE_HANDLE);
    failures += txs_check_eq(s, &second, 1, &before_stale,
                             "fetch stale after reuse");
    failures += f->hooks.check(s, f->ctx, &owner);
    failures += fetch_ident_problems(s, &id2, f->owner_role, f->owner_state,
                                     "fetch stale after reuse");
    MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, filled);
    {
        og_graph_t g;
        og_capture(s, &g);
        failures += og_check_integrity(&g, "fetch stale after reuse");
        failures += fetch_ident_edges(&g, &id2, "fetch stale after reuse");
        failures += og_check_same_topology(&g, &graph_before,
                                           "fetch stale after reuse");
    }
    check_no_output(s, &f->hooks);

    c->ref = saved_ref;
    c->request_id = saved_id;
    moq_session_destroy(s);
}

static void test_fetch(void)
{
    fin_owner_case_t f = fetch_family();
    MOQ_TEST_CHECK_EQ_INT(fin_owner_problems(&f), 0);

    /* Control: no FIN -- the owner has no close recorded, and the rejection
     * reserves one drain reference that the peer's later FIN releases. */
    run_fetch_case(&f, false, false, true);
    /* Same-call FIN: latched on the retained owner; the rejection needs no
     * drain reference. */
    run_fetch_case(&f, true, false, true);
    /* Latch without any terminal at all. */
    run_fetch_case(&f, true, false, false);
    /* The selector against one and the same exhausted ring. */
    run_fetch_case(&f, true, true, true);
    run_fetch_case(&f, false, true, true);
    /* The consumed FIN does not follow the slot to its next owner. */
    run_fetch_slot_reuse_case(&f);
}


/* -- FETCH (P2 pending-join) ----------------------------------------- */

/* A Joining FETCH buffered against a still-pending subscription. Its FIN is
 * carried today by a direct req_recv_fin copy, so these rows are REGRESSIONS,
 * not REDs: they pin the behaviour the handoff-marker conversion must keep.
 * Its two obligations are exercised separately -- the internal rejection
 * itself, reached before the join is ever application-visible, and the
 * drain accounting in the reject-all preflight. */

static moq_subscription_t pj_feed_pending_subscribe(moq_session_t *s,
                                                    moq_stream_ref_t ref,
                                                    uint64_t req_id)
{
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_namespace_t ns = { parts, 1 };
    moq_d18_msg_params_t mp;
    memset(&mp, 0, sizeof(mp));
    mp.has_filter = true;
    mp.filter_type = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    mp.has_forward = true; mp.forward = 1;
    uint8_t m[160];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, m, sizeof(m));
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_d18_encode_subscribe(&w, req_id, &ns, MOQ_BYTES_LITERAL("v0"),
                                      &mp), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, m,
                                              moq_buf_writer_offset(&w),
                                              false, 1), (int)MOQ_OK);
    moq_subscription_t h = MOQ_SUBSCRIPTION_INVALID;
    moq_event_t ev;
    int n_sub = 0, n_other = 0;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
            n_sub++;
            h = ev.u.subscribe_request.sub;
        } else {
            n_other++;
        }
        moq_event_cleanup(&ev);
    }
    /* Exactly the one request event, and nothing else: a duplicate or an
     * unrelated event would otherwise be absorbed by the fixture. */
    MOQ_TEST_CHECK_EQ_INT(n_sub, 1);
    MOQ_TEST_CHECK_EQ_INT(n_other, 0);
    moq_action_t a;
    int n_act = 0;
    while (moq_session_poll_actions(s, &a, 1) > 0) { n_act++; moq_action_cleanup(&a); }
    MOQ_TEST_CHECK_EQ_INT(n_act, 0);
    return h;
}

typedef struct pj_ctx {
    moq_stream_ref_t sref;
    moq_stream_ref_t jref;
    moq_subscription_t sub;   /* the subscription the join buffers against */
} pj_ctx_t;

static pj_ctx_t pj_ctx = { { 1 }, { 5 }, { 0 } };

static moq_result_t pj_feed_join(moq_session_t *s, moq_stream_ref_t ref,
                                 uint64_t req_id, uint64_t join_req_id,
                                 bool fin)
{
    moq_d18_fetch_t f;
    memset(&f, 0, sizeof(f));
    f.request_id = req_id;
    f.fetch_type = 2;
    f.joining_request_id = join_req_id;
    f.joining_start = 1;
    uint8_t m[160];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, m, sizeof(m));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_fetch(&w, &f), (int)MOQ_OK);
    return moq_session_on_bidi_stream_bytes(s, ref, m,
                                            moq_buf_writer_offset(&w), fin, 1);
}

/* Descriptor plumbing: the join is fed on its own bidi, and its terminal is
 * the INTERNAL rejection -- reached by rejecting the subscription, never by
 * an application call naming the join. */
static moq_result_t pj_ops_feed(moq_session_t *s, void *ctx,
                                moq_stream_ref_t ref, bool fin)
{
    (void)ctx;
    return pj_feed_join(s, ref, 2, 0, fin);
}

static moq_result_t pj_ops_internal_reject(moq_session_t *s, void *ctx,
                                           moq_stream_ref_t ref)
{
    pj_ctx_t *c = (pj_ctx_t *)ctx;
    (void)ref;
    moq_reject_subscribe_cfg_t rj;
    moq_reject_subscribe_cfg_init(&rj);
    rj.error_code = MOQ_REQUEST_ERROR_NOT_SUPPORTED;
    return moq_session_reject_subscribe(s, c->sub, &rj, 1);
}

static void pj_hook_capture(const moq_session_t *s, void *vctx, void *vst)
{
    fetch_ctx_t tmp = { 0, ((pj_ctx_t *)vctx)->jref, 0 };
    fetch_hook_capture(s, &tmp, vst);
}

static int pj_hook_check(const moq_session_t *s, void *vctx, const void *vst)
{
    fetch_ctx_t tmp = { 0, ((pj_ctx_t *)vctx)->jref, 0 };
    return fetch_hook_check(s, &tmp, vst);
}

static bool pj_norm_event(const moq_event_t *ev, void *vctx,
                          txs_norm_vec_t *out)
{
    fetch_ctx_t tmp = { 0, ((pj_ctx_t *)vctx)->jref, 0 };
    return fetch_norm_event(ev, &tmp, out);
}

static fin_owner_case_t pj_family(void)
{
    fin_owner_case_t f;
    memset(&f, 0, sizeof(f));
    f.name = "pending-join";
    f.ctx = &pj_ctx;
    /* The only family so far whose terminal is internal. */
    f.caps = FIN_TERM_INTERNAL_REJECT;
    f.feed = pj_ops_feed;
    f.owner_kind = MOQ_REQ_FETCH;
    f.owner_role = (int)MOQ_FETCH_ROLE_PUBLISHER;
    f.owner_state = (int)MOQ_FETCH_PENDING_JOIN;
    f.internal_reject = pj_ops_internal_reject;
    f.hooks.ctx = &pj_ctx;
    f.hooks.capture = pj_hook_capture;
    f.hooks.check = pj_hook_check;
    f.hooks.normalize_event = pj_norm_event;
    f.hooks.normalize_action = pub_norm_action;
    return f;
}


/* The ring's exact contents -- every (reference, reason) pair -- so
 * conservation and release can be proven, not just counted. */
#define DRAIN_SNAP_MAX 512

typedef struct drain_snap {
    size_t   count;
    uint64_t ref[DRAIN_SNAP_MAX];
    uint8_t  reason[DRAIN_SNAP_MAX];
    int      overflow;
} drain_snap_t;

static void drain_snap(const moq_session_t *s, drain_snap_t *d)
{
    memset(d, 0, sizeof(*d));
    if (s->drain_ref_count > DRAIN_SNAP_MAX) {
        /* Record the overflow and copy NOTHING: continuing would write past
         * the array, and the comparator refuses an overflowed snapshot. */
        d->overflow = 1;
        MOQ_TEST_CHECK(!d->overflow);
        return;
    }
    d->count = s->drain_ref_count;
    for (size_t i = 0; i < d->count; i++) {
        d->ref[i] = s->drain_refs[i];
        d->reason[i] = s->drain_ref_reasons[i];
    }
}

/* Multiset comparison: order is an implementation detail (removal swaps), so
 * each pair in `want` must be matched exactly once in `have`. */
static int drain_multiset_equals(const drain_snap_t *have,
                                 const drain_snap_t *want, const char *what)
{
    if (have->overflow || want->overflow) {
        TXS_DIAG("TXN %s: overflowed drain snapshot is incomparable\n", what);
        return 1;
    }
    if (have->count != want->count) {
        TXS_DIAG("TXN %s: %zu drain refs, expected %zu\n", what,
                 have->count, want->count);
        return 1;
    }
    bool used[DRAIN_SNAP_MAX] = { false };
    for (size_t i = 0; i < want->count; i++) {
        bool found = false;
        for (size_t j = 0; j < have->count; j++) {
            if (used[j]) continue;
            if (have->ref[j] == want->ref[i] &&
                have->reason[j] == want->reason[i]) {
                used[j] = true; found = true; break;
            }
        }
        if (!found) {
            TXS_DIAG("TXN %s: missing drain ref %llu reason %u\n", what,
                     (unsigned long long)want->ref[i],
                     (unsigned)want->reason[i]);
            return 1;
        }
    }
    return 0;
}

/* `base` plus exactly the one reference `extra` with the DECLARED reason. The
 * reason is never read back from the live ring: deriving the expectation from
 * the result would accept whatever the product chose, so a strict-versus-
 * normal substitution would pass. */
static void drain_snap_plus(const drain_snap_t *base, moq_stream_ref_t extra,
                            uint8_t reason, drain_snap_t *out)
{
    *out = *base;
    if (out->count >= DRAIN_SNAP_MAX) {
        out->overflow = 1;
        MOQ_TEST_CHECK(out->count < DRAIN_SNAP_MAX);
        return;
    }
    out->ref[out->count] = extra._v;
    out->reason[out->count] = reason;
    out->count++;
}

/* `base` minus exactly the one (ref, reason) pair. The expectation is
 * DECLARED, so a release that removed the wrong reference cannot be adopted
 * as the new baseline. */
static void drain_snap_minus(const drain_snap_t *base, moq_stream_ref_t gone,
                             uint8_t reason, drain_snap_t *out)
{
    memset(out, 0, sizeof(*out));
    out->overflow = base->overflow;
    bool removed = false;
    for (size_t i = 0; i < base->count; i++) {
        if (!removed && base->ref[i] == gone._v && base->reason[i] == reason) {
            removed = true;
            continue;
        }
        if (out->count >= DRAIN_SNAP_MAX) { out->overflow = 1; break; }
        out->ref[out->count] = base->ref[i];
        out->reason[out->count] = base->reason[i];
        out->count++;
    }
    if (!removed)
        TXS_DIAG("TXN: drain ref %llu reason %u absent from the base ring\n",
                 (unsigned long long)gone._v, (unsigned)reason);
    MOQ_TEST_CHECK(removed);
}

/* Present with the declared reason -- membership alone would accept a
 * reference added under the wrong drain reason. */
static void check_drain_ref_reason(const moq_session_t *s,
                                   moq_stream_ref_t ref, uint8_t reason)
{
    MOQ_TEST_CHECK(drain_ref_contains(s, ref));
    bool ok = false;
    for (size_t i = 0; i < s->drain_ref_count; i++)
        if (s->drain_refs[i] == ref._v && s->drain_ref_reasons[i] == reason)
            ok = true;
    if (!ok)
        TXS_DIAG("TXN: drain ref %llu not held under reason %u\n",
                 (unsigned long long)ref._v, (unsigned)reason);
    MOQ_TEST_CHECK(ok);
}

/* Exactly which references the ring holds, not merely how many: adding the
 * wrong stream's reference would keep the count right and the behaviour
 * wrong. */
static void check_drain_members(moq_session_t *s, const moq_stream_ref_t *want,
                                size_t n, size_t base, uint8_t reason)
{
    MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, base + n);
    for (size_t i = 0; i < n; i++)
        check_drain_ref_reason(s, want[i], reason);
}

/* Zero events and exactly the two ordered terminal responses the reject-all
 * path owes: the subscription's configured error on its own bidi, then the
 * joining rejection on the join's. */
static void check_pj_reject_output(moq_session_t *s, const txs_op_hooks_t *h,
                                   moq_stream_ref_t sref,
                                   moq_stream_ref_t jref)
{
    txs_norm_vec_t got, want;
    txs_norm_init(&got);
    txs_norm_init(&want);
    collect_events(s, h, &got);
    MOQ_TEST_CHECK_EQ_SIZE(got.count, 0);
    collect_actions(s, h, &got);
    want_reject_error_code(&want, sref, MOQ_REQUEST_ERROR_NOT_SUPPORTED);
    want_reject_error_code(&want, jref,
                           MOQ_REQUEST_ERROR_INVALID_JOINING_REQUEST_ID);
    failures += txs_norm_equals(&got, &want, "pending-join reject");
    txs_norm_free(&got);
    txs_norm_free(&want);
}

/* The subscription half of the two-owner transaction, read through the
 * bounds-safe registry path: a slot is validated before the pool is indexed,
 * so a stale key cannot become an out-of-range read. */
typedef struct sub_owner_state {
    int      present;
    int      state;
    int      role;
    uint64_t handle;
    uint64_t req_stream_ref;
    int      req_recv_fin;
    int      slot;
    int      has_stream_ref;
    uint64_t registry_ref;
    int      busy;
} sub_owner_state_t;

static int sub_busy_count(const moq_session_t *s)
{
    int n = 0;
    for (size_t i = 0; i < s->sub_cap; i++)
        if (s->subs[i].state != MOQ_SUB_FREE) n++;
    return n;
}

static void sub_owner_capture(const moq_session_t *s, moq_stream_ref_t ref,
                              sub_owner_state_t *st)
{
    memset(st, 0, sizeof(*st));
    st->busy = sub_busy_count(s);
    moq_request_endpoint_t ep = request_registry_find_by_streamref(s, ref);
    if (ep.kind != MOQ_REQ_SUBSCRIPTION) return;
    if (ep.slot < 0 || (size_t)ep.slot >= s->sub_cap) {
        TXS_DIAG("TXN: subscription slot %d out of range (cap %zu)\n",
                 ep.slot, s->sub_cap);
        return;
    }
    const moq_sub_entry_t *e = &s->subs[ep.slot];
    st->present = 1;
    st->slot = ep.slot;
    st->has_stream_ref = ep.has_stream_ref ? 1 : 0;
    st->registry_ref = ep.stream_ref._v;
    st->state = (int)e->state;
    st->role = (int)e->role;
    st->handle = e->handle._opaque;
    st->req_stream_ref = e->request_stream_ref._v;
    st->req_recv_fin = e->req_recv_fin;
}

static int sub_owner_check(const moq_session_t *s, moq_stream_ref_t ref,
                           const sub_owner_state_t *want)
{
    sub_owner_state_t now;
    sub_owner_capture(s, ref, &now);
    int bad = 0;
    if (now.present != want->present) bad++;
    if (now.state != want->state) bad++;
    if (now.role != want->role) bad++;
    if (now.handle != want->handle) bad++;
    if (now.req_stream_ref != want->req_stream_ref) bad++;
    if (now.req_recv_fin != want->req_recv_fin) bad++;
    if (now.slot != want->slot) bad++;
    if (now.has_stream_ref != want->has_stream_ref) bad++;
    if (now.registry_ref != want->registry_ref) bad++;
    if (now.busy != want->busy) bad++;
    if (bad)
        TXS_DIAG("TXN subscription owner: present %d state %d role %d "
                 "handle %llu ref %llu fin %d busy %d, expected %d/%d/%d/"
                 "%llu/%llu/%d/%d\n", now.present, now.state, now.role,
                 (unsigned long long)now.handle,
                 (unsigned long long)now.req_stream_ref, now.req_recv_fin,
                 now.busy, want->present, want->state, want->role,
                 (unsigned long long)want->handle,
                 (unsigned long long)want->req_stream_ref, want->req_recv_fin,
                 want->busy);
    return bad;
}

/* The two owners this fixture depends on, asserted before any snapshot so
 * equality cannot preserve an invalid baseline. The join is checked against
 * the DESCRIPTOR's declared kind, role and state. */
static void check_pj_owners(const fin_owner_case_t *f, moq_session_t *s,
                            moq_stream_ref_t sref, moq_stream_ref_t jref,
                            bool fin_in_request, const fetch_ident_t *id)
{
    /* The subscription's ABSOLUTE shape, so a captured baseline can never
     * preserve an invalid one: an inbound SUBSCRIBE makes us the publisher,
     * on its own request bidi, with a live handle and one pool slot. */
    sub_owner_state_t so;
    sub_owner_capture(s, sref, &so);
    MOQ_TEST_CHECK(so.present);
    MOQ_TEST_CHECK_EQ_INT(so.state, (int)MOQ_SUB_PENDING_PUBLISHER);
    MOQ_TEST_CHECK_EQ_INT(so.role, (int)MOQ_SUB_ROLE_PUBLISHER);
    MOQ_TEST_CHECK_EQ_INT(so.req_recv_fin, 0);
    MOQ_TEST_CHECK(so.handle != 0);
    MOQ_TEST_CHECK_EQ_U64(so.req_stream_ref, sref._v);
    MOQ_TEST_CHECK(so.has_stream_ref);
    MOQ_TEST_CHECK_EQ_U64(so.registry_ref, sref._v);
    MOQ_TEST_CHECK_EQ_INT(so.busy, 1);

    failures += fetch_owner_present_problems(s, jref, f->owner_kind,
                                             f->owner_role, f->owner_state);
    /* The buffered join surfaces no request event, so its own entry handle is
     * what must equal the independently packed value; identity is declared
     * before ingress, never adopted from the committed entry. */
    failures += fetch_ident_problems(s, id, f->owner_role, f->owner_state,
                                     "pending join");
    {
        og_graph_t g;
        og_capture(s, &g);
        failures += og_check_integrity(&g, "pending join");
        failures += fetch_ident_edges(&g, id, "pending join");
    }
    const moq_fetch_entry_t *e = fetch_by_ref(s, jref);
    MOQ_TEST_CHECK(e != NULL);
    if (e) {
        MOQ_TEST_CHECK_EQ_U64(e->handle._opaque, id->handle);
        MOQ_TEST_CHECK_EQ_INT(e->req_recv_fin, fin_in_request ? 1 : 0);
        /* The FIN reaches this owner through the generic destination handoff,
         * which installs it as transitional ownership and drives this family's
         * own FIN handling; that consumption is synchronous, so the marker is
         * already clear -- and with no FIN neither fact is set. */
        MOQ_TEST_CHECK_EQ_INT(e->handoff_fin_pending, 0);
    }
}

/* Both owners retired: both pools empty and both registry keys absent. */
static void check_pj_all_retired(moq_session_t *s, moq_stream_ref_t sref,
                                 moq_stream_ref_t jref)
{
    MOQ_TEST_CHECK_EQ_INT(fetch_busy_count(s), 0);
    MOQ_TEST_CHECK_EQ_INT(sub_busy_count(s), 0);
    check_unregistered(s, jref);
    check_unregistered(s, sref);
}

/* Leave exactly `free_slots` references available in the ring. */
static void fill_drain_ring_leaving(moq_session_t *s, size_t free_slots)
{
    uint64_t next = 0x5000;
    MOQ_TEST_CHECK(s->drain_ref_cap > free_slots);
    while (s->drain_ref_count + free_slots < s->drain_ref_cap)
        MOQ_TEST_CHECK(drain_ref_add(s, moq_stream_ref_from_u64(next++)));
    MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, s->drain_ref_cap - free_slots);
}

/* Obligation 1 -- the INTERNAL rejection, reached before the join was ever
 * application-visible: each join takes a drain reference only if its own
 * close has not been seen, and the reference taken is its own. */
static void run_pj_internal_reject(const fin_owner_case_t *f,
                                   bool fin_in_request)
{
    pj_ctx_t *c = (pj_ctx_t *)f->ctx;
    moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
    c->sub = pj_feed_pending_subscribe(s, c->sref, 0);
    MOQ_TEST_CHECK(c->sub._opaque != MOQ_SUBSCRIPTION_INVALID._opaque);
    fetch_ident_t id = fetch_declare_ident(s, 2, c->jref);
    MOQ_TEST_CHECK_EQ_INT((int)f->feed(s, f->ctx, c->jref, fin_in_request),
                          (int)MOQ_OK);
    check_pj_owners(f, s, c->sref, c->jref, fin_in_request, &id);
    MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, 0);
    /* Never application-visible: the buffered join surfaces nothing. */
    check_no_output(s, &f->hooks);

    MOQ_TEST_CHECK_EQ_INT((int)f->internal_reject(s, f->ctx, c->jref),
                          (int)MOQ_OK);

    /* Exactly which references are held: the subscription's own bidi always,
     * the join's only while its close is outstanding. */
    if (fin_in_request) {
        moq_stream_ref_t want[1] = { c->sref };
        check_drain_members(s, want, 1, 0, MOQ_DRAIN_NORMAL);
        MOQ_TEST_CHECK(!drain_ref_contains(s, c->jref));
    } else {
        moq_stream_ref_t want[2] = { c->sref, c->jref };
        check_drain_members(s, want, 2, 0, MOQ_DRAIN_NORMAL);
    }
    check_pj_all_retired(s, c->sref, c->jref);
    check_pj_reject_output(s, &f->hooks, c->sref, c->jref);

    /* Each outstanding FIN releases ITS OWN reference and no other. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, c->sref, NULL, 0, true, 1),
        (int)MOQ_OK);
    MOQ_TEST_CHECK(!drain_ref_contains(s, c->sref));
    if (!fin_in_request) {
        MOQ_TEST_CHECK(drain_ref_contains(s, c->jref));
        MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, 1);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, c->jref, NULL, 0, true, 1),
            (int)MOQ_OK);
    }
    MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, 0);
    check_no_output(s, &f->hooks);
    moq_session_destroy(s);
}

/* Obligation 2 -- the reject-all PREFLIGHT: its drain accounting decides
 * whether the whole rejection can proceed. */
static void run_pj_preflight(const fin_owner_case_t *f, bool fin_in_request)
{
    pj_ctx_t *c = (pj_ctx_t *)f->ctx;
    moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
    c->sub = pj_feed_pending_subscribe(s, c->sref, 0);
    MOQ_TEST_CHECK(c->sub._opaque != MOQ_SUBSCRIPTION_INVALID._opaque);
    fetch_ident_t id = fetch_declare_ident(s, 2, c->jref);
    MOQ_TEST_CHECK_EQ_INT((int)f->feed(s, f->ctx, c->jref, fin_in_request),
                          (int)MOQ_OK);
    check_pj_owners(f, s, c->sref, c->jref, fin_in_request, &id);
    check_no_output(s, &f->hooks);

    /* Exactly one reference left: enough for the subscription alone. */
    fill_drain_ring_leaving(s, 1);
    drain_snap_t ring_before;
    drain_snap(s, &ring_before);

    moq_stream_ref_t refs[2] = { c->sref, c->jref };
    txs_snapshot_t before;
    txs_capture(s, refs, 2, &before);
    expect_after_call_prepare(&before);
    fetch_hook_state_t owner_before;
    f->hooks.capture(s, f->ctx, &owner_before);
    sub_owner_state_t sub_before;
    sub_owner_capture(s, c->sref, &sub_before);

    moq_result_t want_rc = fin_in_request ? MOQ_OK : MOQ_ERR_WOULD_BLOCK;
    MOQ_TEST_CHECK_EQ_INT((int)f->internal_reject(s, f->ctx, c->jref),
                          (int)want_rc);

    if (want_rc == MOQ_ERR_WOULD_BLOCK) {
        failures += txs_check_eq(s, refs, 2, &before, f->name);
        failures += f->hooks.check(s, f->ctx, &owner_before);
        failures += sub_owner_check(s, c->sref, &sub_before);
        /* Exact conservation: the ring is the identical multiset. */
        drain_snap_t now;
        drain_snap(s, &now);
        failures += drain_multiset_equals(&now, &ring_before, "pj blocked");
        check_pj_owners(f, s, c->sref, c->jref, fin_in_request, &id);
        check_no_output(s, &f->hooks);
        moq_session_destroy(s);
        return;
    }

    /* Exactly the prior multiset plus the subscription's own reference. */
    drain_snap_t now, want_ring;
    drain_snap(s, &now);
    drain_snap_plus(&ring_before, c->sref, MOQ_DRAIN_NORMAL, &want_ring);
    failures += drain_multiset_equals(&now, &want_ring, "pj granted");
    MOQ_TEST_CHECK(!drain_ref_contains(s, c->jref));
    check_pj_all_retired(s, c->sref, c->jref);
    check_pj_reject_output(s, &f->hooks, c->sref, c->jref);

    /* Feeding that FIN restores the prior multiset exactly, with no output. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, c->sref, NULL, 0, true, 1),
        (int)MOQ_OK);
    drain_snap_t after;
    drain_snap(s, &after);
    failures += drain_multiset_equals(&after, &ring_before, "pj released");
    check_no_output(s, &f->hooks);
    moq_session_destroy(s);
}

static void test_pending_join(void)
{
    fin_owner_case_t f = pj_family();
    MOQ_TEST_CHECK_EQ_INT(fin_owner_problems(&f), 0);
    run_pj_internal_reject(&f, false);
    run_pj_internal_reject(&f, true);
    run_pj_preflight(&f, true);
    run_pj_preflight(&f, false);
}


/* -- PUBLISH_NAMESPACE (P4) ------------------------------------------ */

/* Like FETCH, the announcement's owner-present FIN outcome is LATCH AND
 * RETAIN: an inbound PUBLISH_NAMESPACE creates a receiver-role announcement
 * the application still has to answer, so the peer closing its send half is
 * recorded on the owner rather than tearing it down. */

typedef struct pn_ctx {
    uint64_t         request_id;
    moq_stream_ref_t ref;
    uint64_t         last_ann;   /* handle the request event surfaced */
} pn_ctx_t;

typedef struct pn_hook_state {
    int      present;
    uint64_t handle;
    uint64_t req_stream_ref;
    int      req_recv_fin;
    /* The transitional FIN ownership a same-call handoff installs. This family
     * consumes it synchronously into req_recv_fin, so every observation point
     * must find it already clear; captured separately so a refusal cannot move
     * the peer's close from one fact to the other unnoticed. */
    int      handoff_fin;
    int      state;
    int      role;
    int      busy;
} pn_hook_state_t;

static int ann_busy_count(const moq_session_t *s)
{
    int n = 0;
    for (size_t i = 0; i < s->ann_cap; i++)
        if (s->announcements[i].state != MOQ_ANN_FREE) n++;
    return n;
}

static const moq_ann_entry_t *ann_by_ref(const moq_session_t *s,
                                         moq_stream_ref_t ref)
{
    for (size_t i = 0; i < s->ann_cap; i++) {
        const moq_ann_entry_t *e = &s->announcements[i];
        if (e->state != MOQ_ANN_FREE && e->request_stream_ref._v == ref._v)
            return e;
    }
    return NULL;
}

static moq_result_t pn_feed(moq_session_t *s, void *ctx, moq_stream_ref_t ref,
                            bool fin)
{
    pn_ctx_t *c = (pn_ctx_t *)ctx;
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live"),
                            MOQ_BYTES_LITERAL("eu") };
    moq_namespace_t ns = { parts, 2 };
    moq_d18_msg_params_t mp;
    memset(&mp, 0, sizeof(mp));
    /* A non-default token, so a normalizer that drops the token fields cannot
     * compare equal against an empty expectation. */
    mp.auth_token_count = 1;
    mp.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_USE_VALUE;
    mp.auth_tokens[0].token_type = 5;
    mp.auth_tokens[0].token_value = MOQ_BYTES_LITERAL("nstok");
    uint8_t m[192];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, m, sizeof(m));
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_d18_encode_publish_namespace(&w, c->request_id, &ns, &mp),
        (int)MOQ_OK);
    return moq_session_on_bidi_stream_bytes(s, ref, m,
                                            moq_buf_writer_offset(&w), fin, 1);
}

static moq_result_t pn_app_reject(moq_session_t *s, void *ctx, uint64_t handle)
{
    (void)ctx;
    moq_announcement_t ah;
    ah._opaque = handle;
    moq_reject_namespace_cfg_t rj;
    moq_reject_namespace_cfg_init(&rj);
    rj.error_code = MOQ_REQUEST_ERROR_NOT_SUPPORTED;
    return moq_session_reject_namespace(s, ah, &rj, 1);
}

static void pn_hook_capture(const moq_session_t *s, void *vctx, void *vst)
{
    pn_ctx_t *c = (pn_ctx_t *)vctx;
    pn_hook_state_t *st = (pn_hook_state_t *)vst;
    memset(st, 0, sizeof(*st));
    st->busy = ann_busy_count(s);
    const moq_ann_entry_t *e = ann_by_ref(s, c->ref);
    if (!e) return;
    st->present = 1;
    st->handle = e->handle._opaque;
    st->req_stream_ref = e->request_stream_ref._v;
    st->req_recv_fin = e->req_recv_fin;
    st->handoff_fin = e->handoff_fin_pending;
    st->state = (int)e->state;
    st->role = (int)e->role;
}

static int pn_hook_check(const moq_session_t *s, void *vctx, const void *vst)
{
    pn_hook_state_t now;
    pn_hook_capture(s, vctx, &now);
    const pn_hook_state_t *want = (const pn_hook_state_t *)vst;
    int bad = 0;
    if (now.present != want->present) bad++;
    if (now.handle != want->handle) bad++;
    if (now.req_stream_ref != want->req_stream_ref) bad++;
    if (now.req_recv_fin != want->req_recv_fin) bad++;
    if (now.handoff_fin != want->handoff_fin) bad++;
    if (now.state != want->state) bad++;
    if (now.role != want->role) bad++;
    if (now.busy != want->busy) bad++;
    if (bad)
        TXS_DIAG("TXN pn hooks: present %d handle %llu ref %llu fin %d "
                 "state %d role %d busy %d, expected %d/%llu/%llu/%d/%d/%d/%d\n",
                 now.present, (unsigned long long)now.handle,
                 (unsigned long long)now.req_stream_ref, now.req_recv_fin,
                 now.state, now.role, now.busy, want->present,
                 (unsigned long long)want->handle,
                 (unsigned long long)want->req_stream_ref, want->req_recv_fin,
                 want->state, want->role, want->busy);
    return bad;
}

static bool pn_norm_event(const moq_event_t *ev, void *vctx,
                          txs_norm_vec_t *out)
{
    pn_ctx_t *c = (pn_ctx_t *)vctx;
    txs_img_t im;
    txs_img_init(&im);
    switch (ev->kind) {
    case MOQ_EVENT_NAMESPACE_PUBLISHED: {
        const moq_namespace_published_event_t *np = &ev->u.namespace_published;
        c->last_ann = np->ann._opaque;
        txs_img_u64(&im, np->ann._opaque);
        if (!txs_img_parts(&im, "publish_namespace namespace", np->track_namespace.parts,
                           np->track_namespace.count,
                           MOQ_DECODED_MAX_NAMESPACE_PARTS))
            return false;
        if (!txs_img_tokens(&im, "publish_namespace tokens", np->tokens,
                            np->token_count, MOQ_DECODED_MAX_TOKENS))
            return false;
        break;
    }
    default:
        TXS_DIAG("TXN pn: unnormalized event kind %u\n", (unsigned)ev->kind);
        return false;
    }
    return txs_norm_append_img(out, ev->kind, &im);
}

/* Built from the values the fixture put on the wire, not from the event. */
static void want_pn_published(txs_norm_vec_t *v, uint64_t handle)
{
    txs_img_t im;
    txs_img_init(&im);
    txs_img_u64(&im, handle);
    txs_img_u64(&im, 2);
    txs_img_bytes(&im, (const uint8_t *)"live", 4);
    txs_img_bytes(&im, (const uint8_t *)"eu", 2);
    txs_img_u64(&im, 1);                    /* token count */
    txs_img_u64(&im, 5);                    /* token type */
    txs_img_bytes(&im, (const uint8_t *)"nstok", 5);
    MOQ_TEST_CHECK(txs_norm_append_img(v, MOQ_EVENT_NAMESPACE_PUBLISHED, &im));
}

static pn_ctx_t pn_ctx = { 0, { 1 }, 0 };

static fin_owner_case_t pn_family(void)
{
    fin_owner_case_t f;
    memset(&f, 0, sizeof(f));
    f.name = "publish-namespace";
    f.ctx = &pn_ctx;
    f.caps = FIN_TERM_APP_REJECT;
    f.feed = pn_feed;
    f.owner_kind = MOQ_REQ_ANNOUNCEMENT;
    f.owner_role = (int)MOQ_ANN_ROLE_RECEIVER;
    f.owner_state = (int)MOQ_ANN_PENDING_RECEIVER;
    f.app_reject = pn_app_reject;
    f.hooks.ctx = &pn_ctx;
    f.hooks.capture = pn_hook_capture;
    f.hooks.check = pn_hook_check;
    f.hooks.normalize_event = pn_norm_event;
    f.hooks.normalize_action = pub_norm_action;
    return f;
}

static int pn_owner_present_problems(const moq_session_t *s,
                                     moq_stream_ref_t ref,
                                     moq_request_kind_t want_kind,
                                     int want_role, int want_state)
{
    int bad = 0;
    const moq_ann_entry_t *e = ann_by_ref(s, ref);
    if (!e) return 1;
    if ((int)e->role != want_role) bad++;
    if ((int)e->state != want_state) bad++;
    if (ann_busy_count(s) != 1) bad++;
    moq_request_endpoint_t ep = request_registry_find_by_streamref(s, ref);
    if ((int)ep.kind != (int)want_kind) bad++;
    else if (ep.slot != (int)(e - s->announcements)) bad++;
    else if (!ep.has_stream_ref || ep.stream_ref._v != ref._v) bad++;
    if (bad)
        TXS_DIAG("TXN pn owner: kind %d role %d state %d, expected "
                 "kind %d role %d state %d\n", (int)ep.kind, (int)e->role,
                 (int)e->state, (int)want_kind, want_role, want_state);
    return bad;
}

static void run_pn_case(const fin_owner_case_t *f, bool fin_in_request,
                        bool fill_ring, bool reject_now)
{
    moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
    pn_ctx_t *c = (pn_ctx_t *)f->ctx;
    moq_stream_ref_t ref = c->ref;
    c->last_ann = 0;

    /* Slot, live generation and the handle they pack into, all from pool state
     * BEFORE ingress -- so the owner is checked against a declaration, never
     * against whatever the commit happened to produce. */
    int want_slot = -1;
    uint32_t want_gen = 0;
    for (size_t i = 0; i < s->ann_cap; i++)
        if (s->announcements[i].state == MOQ_ANN_FREE) {
            want_slot = (int)i;
            want_gen = s->announcements[i].generation | 1u;
            break;
        }
    MOQ_TEST_CHECK(want_slot >= 0);
    if (want_slot < 0) { moq_session_destroy(s); return; }
    uint64_t want_handle = moq_handle_pack(MOQ_HANDLE_POOL_ANNOUNCEMENT,
                                           s->session_tag, want_gen,
                                           (uint32_t)want_slot);

    MOQ_TEST_CHECK_EQ_INT((int)f->feed(s, f->ctx, ref, fin_in_request),
                          (int)MOQ_OK);
    txs_norm_vec_t got, want;
    txs_norm_init(&got);
    txs_norm_init(&want);
    collect_events(s, &f->hooks, &got);
    want_pn_published(&want, c->last_ann);
    failures += txs_norm_equals(&got, &want, f->name);
    txs_norm_free(&got);
    txs_norm_free(&want);

    /* Latch and RETAIN: the announcement survives and records the close. */
    failures += pn_owner_present_problems(s, ref, f->owner_kind, f->owner_role,
                                          f->owner_state);
    pn_hook_state_t owner;
    f->hooks.capture(s, f->ctx, &owner);
    MOQ_TEST_CHECK(owner.present);
    MOQ_TEST_CHECK_EQ_U64(owner.handle, c->last_ann);
    MOQ_TEST_CHECK_EQ_U64(owner.req_stream_ref, ref._v);
    /* The surfaced NAMESPACE_PUBLISHED handle and the physical owner both
     * match the pre-derived declaration. */
    MOQ_TEST_CHECK_EQ_U64(c->last_ann, want_handle);
    {
        const moq_ann_entry_t *e = ann_by_ref(s, ref);
        MOQ_TEST_CHECK(e != NULL);
        if (e) {
            MOQ_TEST_CHECK_EQ_INT((int)(e - s->announcements), want_slot);
            MOQ_TEST_CHECK_EQ_U64(e->generation, want_gen);
            MOQ_TEST_CHECK_EQ_U64(e->handle._opaque, want_handle);
        }
        check_registered(s, ref, MOQ_REQ_ANNOUNCEMENT, want_slot);
    }
    MOQ_TEST_CHECK_EQ_INT(owner.req_recv_fin, fin_in_request ? 1 : 0);
    /* Consumption is synchronous: a handed-over FIN lives in the durable latch
     * by now, never as transitional ownership. */
    MOQ_TEST_CHECK_EQ_INT(owner.handoff_fin, 0);
    MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, 0);
    check_no_output(s, &f->hooks);

    if (fill_ring) fill_drain_ring(s);   /* no slot left at all */
    drain_snap_t ring_before;
    drain_snap(s, &ring_before);
    if (!reject_now) { moq_session_destroy(s); return; }

    /* With the peer's close observed the rejection needs no reference, so a
     * full ring cannot refuse it; without it, one is required. */
    bool expect_drain = !fin_in_request;
    moq_result_t want_rc = (expect_drain && fill_ring) ? MOQ_ERR_WOULD_BLOCK
                                                       : MOQ_OK;
    txs_snapshot_t before;
    txs_capture(s, &ref, 1, &before);
    expect_after_call_prepare(&before);
    pn_hook_state_t owner_before;
    f->hooks.capture(s, f->ctx, &owner_before);
    MOQ_TEST_CHECK_EQ_INT((int)f->app_reject(s, f->ctx, c->last_ann),
                          (int)want_rc);

    if (want_rc == MOQ_ERR_WOULD_BLOCK) {
        failures += txs_check_eq(s, &ref, 1, &before, f->name);
        failures += f->hooks.check(s, f->ctx, &owner_before);
        /* The refusal must leave the full owner linkage intact, registry
         * kind, slot and stream-ref identity included. */
        failures += pn_owner_present_problems(s, ref, f->owner_kind,
                                              f->owner_role, f->owner_state);
        drain_snap_t now;
        drain_snap(s, &now);
        failures += drain_multiset_equals(&now, &ring_before, "pn blocked");
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
    failures += drain_multiset_equals(&now, &want_ring, "pn granted");
    MOQ_TEST_CHECK_EQ_INT(ann_busy_count(s), 0);
    check_unregistered(s, ref);
    check_reject_only_output(s, &f->hooks, ref, f->name);

    if (expect_drain) {
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
            (int)MOQ_OK);
        drain_snap_t after;
        drain_snap(s, &after);
        failures += drain_multiset_equals(&after, &ring_before, "pn released");
        MOQ_TEST_CHECK_EQ_INT(ann_busy_count(s), 0);
        check_unregistered(s, ref);
        check_no_output(s, &f->hooks);
    }
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
    moq_session_destroy(s);
}

/* Clear-on-reuse for the announcement pool. This family's free is SELECTIVE --
 * it resets named fields rather than the whole record -- so the marker's
 * clearing is an explicit obligation here, not a by-product of a memset. Both
 * generations and both packed handles are derived before ingress. */
static void run_pn_slot_reuse_case(const fin_owner_case_t *f)
{
    moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
    pn_ctx_t *c = (pn_ctx_t *)f->ctx;

    int slot = -1;
    uint32_t gen_first = 0;
    for (size_t i = 0; i < s->ann_cap; i++)
        if (s->announcements[i].state == MOQ_ANN_FREE) {
            slot = (int)i;
            gen_first = s->announcements[i].generation | 1u;
            break;
        }
    MOQ_TEST_CHECK(slot >= 0);
    if (slot < 0) { moq_session_destroy(s); return; }
    uint32_t gen_reuse = (gen_first + 1) | 1u;    /* ann_free_entry advances once */
    uint64_t handle_first = moq_handle_pack(MOQ_HANDLE_POOL_ANNOUNCEMENT,
                                            s->session_tag, gen_first,
                                            (uint32_t)slot);
    uint64_t handle_reuse = moq_handle_pack(MOQ_HANDLE_POOL_ANNOUNCEMENT,
                                            s->session_tag, gen_reuse,
                                            (uint32_t)slot);
    MOQ_TEST_CHECK(handle_first != handle_reuse);

    /* 1. PUBLISH_NAMESPACE + FIN: the marker is consumed into the latch and the
     *    owner is retained for the application's answer. */
    moq_stream_ref_t first = c->ref;
    MOQ_TEST_CHECK_EQ_INT((int)f->feed(s, f->ctx, first, true), (int)MOQ_OK);
    txs_norm_vec_t got, want;
    txs_norm_init(&got);
    txs_norm_init(&want);
    collect_events(s, &f->hooks, &got);
    want_pn_published(&want, c->last_ann);
    failures += txs_norm_equals(&got, &want, "pn reuse first");
    txs_norm_free(&got);
    txs_norm_free(&want);
    MOQ_TEST_CHECK_EQ_U64(c->last_ann, handle_first);
    {
        const moq_ann_entry_t *e = ann_by_ref(s, first);
        MOQ_TEST_CHECK(e != NULL);
        if (e) {
            MOQ_TEST_CHECK_EQ_INT((int)(e - s->announcements), slot);
            MOQ_TEST_CHECK_EQ_U64(e->generation, gen_first);
            MOQ_TEST_CHECK_EQ_INT(e->req_recv_fin, 1);
            MOQ_TEST_CHECK_EQ_INT(e->handoff_fin_pending, 0);
        }
    }
    /* Its rejection is drainless -- the peer already closed -- and retires it. */
    MOQ_TEST_CHECK_EQ_INT((int)f->app_reject(s, f->ctx, handle_first),
                          (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, 0);
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
    {   /* Drain the rejection's own output so the reuse leg measures only the
         * second request; its exact shape is pinned by the P4 matrix rows. */
        txs_norm_vec_t d;
        txs_norm_init(&d);
        collect_events(s, &f->hooks, &d);
        collect_actions(s, &f->hooks, &d);
        txs_norm_free(&d);
    }

    /* 2. The freed record: generation advanced, BOTH FIN facts clear, and no
     *    edge anywhere still points at the retired owner. */
    MOQ_TEST_CHECK_EQ_INT((int)s->announcements[slot].state, (int)MOQ_ANN_FREE);
    MOQ_TEST_CHECK_EQ_U64(s->announcements[slot].generation, gen_reuse - 1u);
    MOQ_TEST_CHECK_EQ_INT(s->announcements[slot].req_recv_fin, 0);
    MOQ_TEST_CHECK_EQ_INT(s->announcements[slot].handoff_fin_pending, 0);
    check_unregistered(s, first);
    {
        og_graph_t g;
        og_capture(s, &g);
        failures += og_check_integrity(&g, "pn reuse retired");
        failures += og_check_no_edge(&g, OG_DOM_REQ_STREAMREF, first._v,
                                     "pn reuse retired");
        failures += og_check_owner_unreferenced(&g, (int)MOQ_REQ_ANNOUNCEMENT,
                                                slot, "pn reuse retired");
    }

    /* 3. A distinct no-FIN PUBLISH_NAMESPACE reuses that physical slot. */
    moq_stream_ref_t second = moq_stream_ref_from_u64(c->ref._v + 4);
    moq_stream_ref_t saved_ref = c->ref;
    uint64_t saved_id = c->request_id;
    c->ref = second;
    c->request_id = saved_id + 2;
    MOQ_TEST_CHECK_EQ_INT((int)f->feed(s, f->ctx, second, false), (int)MOQ_OK);
    txs_norm_init(&got);
    txs_norm_init(&want);
    collect_events(s, &f->hooks, &got);
    want_pn_published(&want, c->last_ann);
    failures += txs_norm_equals(&got, &want, "pn reuse second");
    txs_norm_free(&got);
    txs_norm_free(&want);
    failures += pn_owner_present_problems(s, second, f->owner_kind,
                                          f->owner_role, f->owner_state);
    {
        const moq_ann_entry_t *e = ann_by_ref(s, second);
        MOQ_TEST_CHECK(e != NULL);
        if (e) {
            MOQ_TEST_CHECK_EQ_INT((int)(e - s->announcements), slot);
            MOQ_TEST_CHECK_EQ_U64(e->generation, gen_reuse);
            MOQ_TEST_CHECK_EQ_U64(e->handle._opaque, handle_reuse);
            /* The consumed FIN did not follow the slot to its next owner. */
            MOQ_TEST_CHECK_EQ_INT(e->req_recv_fin, 0);
            MOQ_TEST_CHECK_EQ_INT(e->handoff_fin_pending, 0);
        }
        MOQ_TEST_CHECK_EQ_U64(c->last_ann, handle_reuse);
        check_registered(s, second, MOQ_REQ_ANNOUNCEMENT, slot);
    }

    /* 4. Its peer half is genuinely open, so its rejection owes a drain
     *    reference and an exhausted ring refuses it transactionally. */
    fill_drain_ring(s);
    size_t filled = s->drain_ref_count;
    og_graph_t graph_before;
    og_capture(s, &graph_before);
    failures += og_check_integrity(&graph_before, "pn reuse live");
    pn_hook_state_t owner;
    f->hooks.capture(s, f->ctx, &owner);
    txs_snapshot_t before;
    txs_capture(s, &second, 1, &before);
    expect_after_call_prepare(&before);
    MOQ_TEST_CHECK_EQ_INT((int)f->app_reject(s, f->ctx, handle_reuse),
                          (int)MOQ_ERR_WOULD_BLOCK);
    failures += txs_check_eq(s, &second, 1, &before, "pn reuse blocked");
    failures += f->hooks.check(s, f->ctx, &owner);
    MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, filled);
    {
        og_graph_t g;
        og_capture(s, &g);
        failures += og_check_integrity(&g, "pn reuse blocked");
        failures += og_check_same_topology(&g, &graph_before,
                                           "pn reuse blocked");
    }
    check_no_output(s, &f->hooks);

    /* 5. The retired first-generation handle stays stale after the reuse. */
    txs_snapshot_t before_stale;
    txs_capture(s, &second, 1, &before_stale);
    expect_after_call_prepare(&before_stale);
    MOQ_TEST_CHECK_EQ_INT((int)f->app_reject(s, f->ctx, handle_first),
                          (int)MOQ_ERR_STALE_HANDLE);
    failures += txs_check_eq(s, &second, 1, &before_stale, "pn stale after reuse");
    failures += f->hooks.check(s, f->ctx, &owner);
    MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, filled);
    check_no_output(s, &f->hooks);

    c->ref = saved_ref;
    c->request_id = saved_id;
    moq_session_destroy(s);
}

static void test_publish_namespace(void)
{
    fin_owner_case_t f = pn_family();
    MOQ_TEST_CHECK_EQ_INT(fin_owner_problems(&f), 0);
    run_pn_case(&f, false, false, true);   /* control: drain, then released */
    run_pn_case(&f, true, false, true);    /* same-call FIN: no drain */
    run_pn_case(&f, true, false, false);   /* latch without any terminal */
    run_pn_case(&f, true, true, true);     /* exhausted ring, FIN observed */
    run_pn_case(&f, false, true, true);    /* exhausted ring, no FIN */
    /* The consumed FIN does not follow the slot to its next owner. */
    run_pn_slot_reuse_case(&f);
}


/* -- SUBSCRIBE_TRACKS (P5) ------------------------------------------- */

/* Unlike FETCH and PUBLISH_NAMESPACE, this family's owner-present FIN outcome
 * is TEARDOWN: the peer closing its send half cancels the track subscription,
 * surfacing exactly one cancellation and reclaiming the entry. When the event
 * queue cannot take that cancellation the owner and the observed close must
 * both survive for the retry. */

typedef struct st_ctx {
    uint64_t         request_id;
    moq_stream_ref_t ref;
    uint64_t         last_handle;
} st_ctx_t;

typedef struct st_hook_state {
    int      present;
    uint64_t handle;
    uint64_t req_stream_ref;
    int      req_recv_fin;
    int      handoff_fin;
    int      state;
    int      role;
    int      busy;
} st_hook_state_t;

static int st_busy_count(const moq_session_t *s)
{
    int n = 0;
    for (size_t i = 0; i < s->track_sub_cap; i++)
        if (s->track_subs[i].state != MOQ_TRACK_SUB_FREE) n++;
    return n;
}

/* The DECLARED track-sub owner inventory -- not the whole record. The shared
 * request-family snapshot models only fields common to every owner kind, so it
 * omits this entry's stored prefix, its request-bidi receive buffer, its auth
 * staging and both FIN facts -- exactly the fields a handoff touches, which is
 * what this inventory adds.
 *
 * Deliberately UNMODELLED, named rather than implied:
 *   - the resolved-token array's CONTENTS (`token_count` and the count of
 *     staged entries are modelled; the tokens themselves are not);
 *   - the auth transaction's interior (only `auth_committed` and
 *     `auth_reject_code` are modelled).
 * Prefix allocation topology IS modelled, relationally: see
 * `prefix_topology_ok`.
 *
 * Bounds and NULL inconsistencies make the record INCOMPARABLE before any
 * dereference, and two incomparable records never compare equal. That includes
 * a retained length beyond STO_BUF_MAX -- such a record is INVALID and its
 * comparison FAILS; it is not a silent exclusion of the bytes past the bound. */
#define STO_BUF_MAX 256
#define STO_MAX_PARTS 32            /* draft-18 caps a Track Namespace at 32 */

typedef struct sto_snap {
    int      present;
    int      state;
    int      role;
    uint32_t generation;
    uint64_t handle;
    uint64_t request_id;
    int      ep_kind;
    int      ep_slot;
    int      ep_has_request_id;
    uint64_t ep_request_id;
    int      ep_has_stream_ref;
    uint64_t ep_stream_ref;
    uint64_t stream_ref;
    int      forward;
    int      req_recv_fin;
    int      handoff_fin;
    int      goaway_sent;
    int      auth_committed;
    uint64_t auth_reject_code;
    size_t   token_count;
    size_t   prefix_count;
    size_t   prefix_buf_len;
    size_t   prefix_len;             /* encoded (u8 len, bytes) per part */
    uint8_t  prefix[STO_BUF_MAX];
    /* Relational, never an adopted address: for a non-empty prefix both
     * allocations exist and every part's data addresses its own running offset
     * inside the owned buffer. */
    int      prefix_topology_ok;
    size_t   token_staged_count;
    const void *recv_buf;            /* preserved across reuse: identity matters */
    size_t   recv_cap;
    size_t   recv_len;
    uint8_t  recv[STO_BUF_MAX];
    int      invalid;
} sto_snap_t;

static void sto_read(const moq_session_t *s, int slot, sto_snap_t *v)
{
    memset(v, 0, sizeof(*v));
    if (slot < 0 || (size_t)slot >= s->track_sub_cap) { v->invalid = 1; return; }
    const moq_track_sub_entry_t *e = &s->track_subs[slot];
    v->present = (e->state != MOQ_TRACK_SUB_FREE);
    v->state = (int)e->state;
    v->role = (int)e->role;
    v->generation = e->generation;
    v->handle = e->handle._opaque;
    v->request_id = e->request_id;
    v->ep_kind = (int)e->request_ep.kind;
    v->ep_slot = e->request_ep.slot;
    v->ep_has_request_id = e->request_ep.has_request_id ? 1 : 0;
    v->ep_request_id = e->request_ep.request_id;
    v->ep_has_stream_ref = e->request_ep.has_stream_ref ? 1 : 0;
    v->ep_stream_ref = e->request_ep.stream_ref._v;
    v->stream_ref = e->request_stream_ref._v;
    v->forward = e->forward ? 1 : 0;
    v->req_recv_fin = e->req_recv_fin ? 1 : 0;
    v->handoff_fin = e->handoff_fin_pending ? 1 : 0;
    v->goaway_sent = e->goaway_sent ? 1 : 0;
    v->auth_committed = e->auth_committed ? 1 : 0;
    v->auth_reject_code = e->auth_reject_code;
    v->token_count = e->token_count;
    v->prefix_count = e->prefix_count;
    v->prefix_buf_len = e->prefix_buf_len;
    /* Refuse BEFORE dereferencing: a count past the wire maximum, a count with
     * no array, or a non-empty part with no data would each be an out-of-range
     * read inside the harness meant to diagnose it. */
    if (e->prefix_count > STO_MAX_PARTS) { v->invalid = 1; return; }
    if (e->prefix_count > 0 && !e->prefix_parts) { v->invalid = 1; return; }
    if (e->token_count > MOQ_DECODED_MAX_TOKENS) { v->invalid = 1; return; }
    for (size_t i = 0; i < e->token_count; i++)
        if (e->token_staged[i]) v->token_staged_count++;
    /* Topology fails CLOSED: every check below returns an incomparable record
     * BEFORE forming a pointer outside the declared allocation and before any
     * byte is copied. A corrupted part pointer must make this harness refuse,
     * never dereference. `prefix_topology_ok` therefore reads 1 on every
     * comparable record -- the discrimination lives in the refusal, and the
     * field remains as a declared fact rather than the detector. */
    v->prefix_topology_ok = 1;
    if (e->prefix_count > 0 && (!e->prefix_buf || !e->prefix_parts)) {
        v->invalid = 1;
        return;
    }
    size_t off = 0;
    for (size_t i = 0; i < e->prefix_count; i++) {
        size_t l = e->prefix_parts[i].len;
        if (l > 0 && !e->prefix_parts[i].data) { v->invalid = 1; return; }
        if (l > 255 || v->prefix_len + 1 + l > STO_BUF_MAX) {
            v->invalid = 1;
            return;
        }
        /* Subtraction-safe: prove this part lies inside the owned buffer
         * BEFORE `prefix_buf + off` is formed. */
        if (off > e->prefix_buf_len || l > e->prefix_buf_len - off) {
            v->invalid = 1;
            return;
        }
        if (e->prefix_parts[i].data != (const uint8_t *)e->prefix_buf + off) {
            v->invalid = 1;
            return;
        }
        off += l;
        v->prefix[v->prefix_len++] = (uint8_t)l;
        if (l) memcpy(v->prefix + v->prefix_len, e->prefix_parts[i].data, l);
        v->prefix_len += l;
    }
    if (off != e->prefix_buf_len) { v->invalid = 1; return; }
    v->recv_buf = (const void *)e->req_recv_buf;
    v->recv_cap = e->req_recv_cap;
    v->recv_len = e->req_recv_len;
    if (e->req_recv_len > STO_BUF_MAX ||
        (e->req_recv_len && !e->req_recv_buf)) {
        v->invalid = 1;
        return;
    }
    if (e->req_recv_len) memcpy(v->recv, e->req_recv_buf, e->req_recv_len);
}

static int sto_diff(const sto_snap_t *have, const sto_snap_t *want,
                    const char *what)
{
    if (have->invalid || want->invalid) {
        TXS_DIAG("TXN %s: incomparable track-sub record\n", what);
        return 1;
    }
    int bad = 0;
#define STO_EQ(field) do { \
        if ((uint64_t)have->field != (uint64_t)want->field) { \
            TXS_DIAG("TXN %s: st owner " #field " %llu, expected %llu\n", what, \
                     (unsigned long long)have->field, \
                     (unsigned long long)want->field); \
            bad++; \
        } \
    } while (0)
    STO_EQ(present); STO_EQ(state); STO_EQ(role); STO_EQ(generation);
    STO_EQ(handle); STO_EQ(request_id);
    STO_EQ(ep_kind); STO_EQ(ep_slot); STO_EQ(ep_has_request_id);
    STO_EQ(ep_request_id); STO_EQ(ep_has_stream_ref); STO_EQ(ep_stream_ref);
    STO_EQ(stream_ref); STO_EQ(forward); STO_EQ(req_recv_fin);
    STO_EQ(handoff_fin); STO_EQ(goaway_sent); STO_EQ(auth_committed);
    STO_EQ(auth_reject_code); STO_EQ(token_count); STO_EQ(prefix_count);
    STO_EQ(prefix_buf_len); STO_EQ(prefix_len); STO_EQ(recv_cap);
    STO_EQ(recv_len); STO_EQ(prefix_topology_ok); STO_EQ(token_staged_count);
#undef STO_EQ
    if (have->recv_buf != want->recv_buf) {
        TXS_DIAG("TXN %s: st owner receive buffer identity changed\n", what);
        bad++;
    }
    if (have->prefix_len == want->prefix_len && have->prefix_len &&
        memcmp(have->prefix, want->prefix, have->prefix_len) != 0) {
        TXS_DIAG("TXN %s: st owner prefix bytes differ\n", what);
        bad++;
    }
    if (have->recv_len == want->recv_len && have->recv_len &&
        memcmp(have->recv, want->recv, have->recv_len) != 0) {
        TXS_DIAG("TXN %s: st owner retained bytes differ\n", what);
        bad++;
    }
    return bad;
}

/* The record a committed publisher-role SUBSCRIBE_TRACKS owner must hold,
 * built from values the fixture declared before ingress plus the free slot's
 * preserved receive buffer -- never read back from the call under test. */
static void sto_declare(sto_snap_t *v, int slot, uint32_t gen, uint64_t handle,
                        uint64_t request_id, uint64_t stream_ref,
                        int handoff_fin, const void *recv_buf, size_t recv_cap)
{
    (void)slot;
    memset(v, 0, sizeof(*v));
    v->present = 1;
    v->state = (int)MOQ_TRACK_SUB_PENDING_PUBLISHER;
    v->role = (int)MOQ_TRACK_SUB_ROLE_PUBLISHER;
    v->generation = gen;
    v->handle = handle;
    v->request_id = request_id;
    /* The entry's own `request_ep` stays ZEROED on this family: the commit
     * builds the endpoint on the decoded request and installs it in the
     * stream-ref registry, and only the namespace-sub family mirrors one onto
     * its entry. Declared as unset so a future write to it is a diagnostic. */
    v->ep_kind = 0;
    v->ep_slot = 0;
    v->ep_has_request_id = 0;
    v->ep_request_id = 0;
    v->ep_has_stream_ref = 0;
    v->ep_stream_ref = 0;
    v->stream_ref = stream_ref;
    v->forward = 1;                      /* st_feed sets Forward = 1 */
    v->req_recv_fin = 0;
    v->handoff_fin = handoff_fin;
    v->goaway_sent = 0;
    /* The inbound publisher-side commit never writes the entry's auth staging:
     * the transaction is committed on the DECODED request, and resolved tokens
     * belong to the locally-issued subscriber path. */
    v->auth_committed = 0;
    v->auth_reject_code = 0;
    v->token_count = 0;
    v->prefix_count = 2;
    /* st_feed's prefix is ("live", "eu"); the stored buffer holds the bytes. */
    v->prefix_buf_len = 6;
    v->prefix_len = 0;
    v->prefix[v->prefix_len++] = 4;
    memcpy(v->prefix + v->prefix_len, "live", 4); v->prefix_len += 4;
    v->prefix[v->prefix_len++] = 2;
    memcpy(v->prefix + v->prefix_len, "eu", 2);   v->prefix_len += 2;
    v->prefix_topology_ok = 1;
    v->token_staged_count = 0;
    v->recv_buf = recv_buf;
    v->recv_cap = recv_cap;
    v->recv_len = 0;                     /* the request rode the staging entry */
}

/* A live publisher-role track-sub owner in these rows holds exactly one
 * stream-ref edge and no by-id or namespace-index edge. */
static int st_check_owner_edges(const og_graph_t *g, int slot,
                                moq_stream_ref_t ref, const char *what)
{
    const og_edge_spec_t want[] = { { OG_DOM_REQ_STREAMREF, ref._v } };
    int bad = og_check_owner_edges(g, (int)MOQ_REQ_SUBSCRIBE_TRACKS, slot,
                                   want, 1, what);
    bad += og_check_no_edge(g, OG_DOM_NS_REF, ref._v, what);
    return bad;
}

/* The declared session delta a committed SUBSCRIBE_TRACKS handoff may produce,
 * measured against a capture taken BEFORE the feed. Exactly two changes are
 * permitted: the stream's owner becomes the declared track-sub entry, and the
 * event-scratch cursor is ADOPTED as an explicit exclusion -- the surfaced
 * request image borrowed it, and draining events does not reclaim the arena
 * (the next advancing call's preparation does), so it cannot be declared
 * independently. Everything else -- session state, both queue depths, payload
 * accounting and drain membership -- must be unchanged. */
static int st_check_handoff_delta(moq_session_t *s, moq_stream_ref_t ref,
                                  const txs_snapshot_t *pre, int slot,
                                  uint32_t gen, const char *what)
{
    txs_snapshot_t observed;
    txs_capture(s, &ref, 1, &observed);
    txs_snapshot_t want = *pre;
    want.event_scratch_len = observed.event_scratch_len;   /* adopted */
    want.owners[0].kind = (int)MOQ_REQ_SUBSCRIBE_TRACKS;
    want.owners[0].slot = slot;
    want.owners[0].state = (int)MOQ_TRACK_SUB_PENDING_PUBLISHER;
    want.owners[0].generation = gen;
    want.owners[0].invalid = 0;
    return txs_check_eq(s, &ref, 1, &want, what);
}

static int st_declare_free_slot(const moq_session_t *s)
{
    for (size_t i = 0; i < s->track_sub_cap; i++)
        if (s->track_subs[i].state == MOQ_TRACK_SUB_FREE) return (int)i;
    return -1;
}

static const moq_track_sub_entry_t *st_by_ref(const moq_session_t *s,
                                              moq_stream_ref_t ref)
{
    for (size_t i = 0; i < s->track_sub_cap; i++) {
        const moq_track_sub_entry_t *e = &s->track_subs[i];
        if (e->state != MOQ_TRACK_SUB_FREE && e->request_stream_ref._v == ref._v)
            return e;
    }
    return NULL;
}

static moq_result_t st_feed(moq_session_t *s, void *ctx, moq_stream_ref_t ref,
                            bool fin)
{
    st_ctx_t *c = (st_ctx_t *)ctx;
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live"),
                            MOQ_BYTES_LITERAL("eu") };
    moq_namespace_t pfx = { parts, 2 };
    moq_d18_msg_params_t mp;
    memset(&mp, 0, sizeof(mp));
    mp.has_forward = true; mp.forward = 1;
    mp.auth_token_count = 1;
    mp.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_USE_VALUE;
    mp.auth_tokens[0].token_type = 6;
    mp.auth_tokens[0].token_value = MOQ_BYTES_LITERAL("sttok");
    uint8_t m[192];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, m, sizeof(m));
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_d18_encode_subscribe_tracks(&w, c->request_id, &pfx, &mp),
        (int)MOQ_OK);
    return moq_session_on_bidi_stream_bytes(s, ref, m,
                                            moq_buf_writer_offset(&w), fin, 1);
}

static moq_result_t st_app_reject(moq_session_t *s, void *ctx, uint64_t handle)
{
    (void)ctx;
    moq_track_sub_handle_t h;
    h._opaque = handle;
    moq_reject_subscribe_tracks_cfg_t rj;
    moq_reject_subscribe_tracks_cfg_init(&rj);
    rj.error_code = MOQ_REQUEST_ERROR_NOT_SUPPORTED;
    return moq_session_reject_subscribe_tracks(s, h, &rj, 1);
}

static void st_hook_capture(const moq_session_t *s, void *vctx, void *vst)
{
    st_ctx_t *c = (st_ctx_t *)vctx;
    st_hook_state_t *st = (st_hook_state_t *)vst;
    memset(st, 0, sizeof(*st));
    st->busy = st_busy_count(s);
    const moq_track_sub_entry_t *e = st_by_ref(s, c->ref);
    if (!e) return;
    st->present = 1;
    st->handle = e->handle._opaque;
    st->req_stream_ref = e->request_stream_ref._v;
    st->req_recv_fin = e->req_recv_fin;
    st->handoff_fin = e->handoff_fin_pending;
    st->state = (int)e->state;
    st->role = (int)e->role;
}

static int st_hook_check(const moq_session_t *s, void *vctx, const void *vst)
{
    st_hook_state_t now;
    st_hook_capture(s, vctx, &now);
    const st_hook_state_t *want = (const st_hook_state_t *)vst;
    int bad = 0;
    if (now.present != want->present) bad++;
    if (now.handle != want->handle) bad++;
    if (now.req_stream_ref != want->req_stream_ref) bad++;
    if (now.req_recv_fin != want->req_recv_fin) bad++;
    if (now.handoff_fin != want->handoff_fin) bad++;
    if (now.state != want->state) bad++;
    if (now.role != want->role) bad++;
    if (now.busy != want->busy) bad++;
    if (bad)
        TXS_DIAG("TXN st hooks: present %d handle %llu ref %llu fin %d "
                 "handoff %d state %d role %d busy %d, expected "
                 "%d/%llu/%llu/%d/%d/%d/%d/%d\n",
                 now.present, (unsigned long long)now.handle,
                 (unsigned long long)now.req_stream_ref, now.req_recv_fin,
                 now.handoff_fin, now.state, now.role, now.busy, want->present,
                 (unsigned long long)want->handle,
                 (unsigned long long)want->req_stream_ref, want->req_recv_fin,
                 want->handoff_fin, want->state, want->role, want->busy);
    return bad;
}

static bool st_norm_event(const moq_event_t *ev, void *vctx,
                          txs_norm_vec_t *out)
{
    st_ctx_t *c = (st_ctx_t *)vctx;
    txs_img_t im;
    txs_img_init(&im);
    switch (ev->kind) {
    case MOQ_EVENT_SUBSCRIBE_TRACKS_REQUEST: {
        const moq_subscribe_tracks_request_event_t *rq =
            &ev->u.subscribe_tracks_request;
        c->last_handle = rq->handle._opaque;
        txs_img_u64(&im, rq->handle._opaque);
        txs_img_u64(&im, rq->forward ? 1u : 0u);
        if (!txs_img_parts(&im, "subscribe_tracks prefix", rq->track_namespace_prefix.parts,
                           rq->track_namespace_prefix.count,
                           MOQ_DECODED_MAX_NAMESPACE_PARTS))
            return false;
        if (!txs_img_tokens(&im, "subscribe_tracks tokens", rq->tokens,
                            rq->token_count, MOQ_DECODED_MAX_TOKENS))
            return false;
        break;
    }
    case MOQ_EVENT_SUBSCRIBE_TRACKS_CANCELLED:
        txs_img_u64(&im, ev->u.subscribe_tracks_cancelled.handle._opaque);
        break;
    default:
        TXS_DIAG("TXN st: unnormalized event kind %u\n", (unsigned)ev->kind);
        return false;
    }
    return txs_norm_append_img(out, ev->kind, &im);
}

static void want_st_request(txs_norm_vec_t *v, uint64_t handle)
{
    txs_img_t im;
    txs_img_init(&im);
    txs_img_u64(&im, handle);
    txs_img_u64(&im, 1);                    /* forward */
    txs_img_u64(&im, 2);                    /* prefix parts */
    txs_img_bytes(&im, (const uint8_t *)"live", 4);
    txs_img_bytes(&im, (const uint8_t *)"eu", 2);
    txs_img_u64(&im, 1);                    /* token count */
    txs_img_u64(&im, 6);                    /* token type */
    txs_img_bytes(&im, (const uint8_t *)"sttok", 5);
    MOQ_TEST_CHECK(
        txs_norm_append_img(v, MOQ_EVENT_SUBSCRIBE_TRACKS_REQUEST, &im));
}

static void want_st_cancelled(txs_norm_vec_t *v, uint64_t handle)
{
    txs_img_t im;
    txs_img_init(&im);
    txs_img_u64(&im, handle);
    MOQ_TEST_CHECK(
        txs_norm_append_img(v, MOQ_EVENT_SUBSCRIBE_TRACKS_CANCELLED, &im));
}

static st_ctx_t st_ctx = { 0, { 1 }, 0 };

static fin_owner_case_t st_family(void)
{
    fin_owner_case_t f;
    memset(&f, 0, sizeof(f));
    f.name = "subscribe-tracks";
    f.ctx = &st_ctx;
    f.caps = FIN_TERM_APP_REJECT;
    f.feed = st_feed;
    f.owner_kind = MOQ_REQ_SUBSCRIBE_TRACKS;
    f.owner_role = (int)MOQ_TRACK_SUB_ROLE_PUBLISHER;
    f.owner_state = (int)MOQ_TRACK_SUB_PENDING_PUBLISHER;
    f.app_reject = st_app_reject;
    f.hooks.ctx = &st_ctx;
    f.hooks.capture = st_hook_capture;
    f.hooks.check = st_hook_check;
    f.hooks.normalize_event = st_norm_event;
    f.hooks.normalize_action = pub_norm_action;
    return f;
}

static int st_owner_present_problems(const moq_session_t *s,
                                     moq_stream_ref_t ref,
                                     moq_request_kind_t want_kind,
                                     int want_role, int want_state)
{
    int bad = 0;
    const moq_track_sub_entry_t *e = st_by_ref(s, ref);
    if (!e) return 1;
    if ((int)e->role != want_role) bad++;
    if ((int)e->state != want_state) bad++;
    if (st_busy_count(s) != 1) bad++;
    moq_request_endpoint_t ep = request_registry_find_by_streamref(s, ref);
    if ((int)ep.kind != (int)want_kind) bad++;
    else if (ep.slot != (int)(e - s->track_subs)) bad++;
    else if (!ep.has_stream_ref || ep.stream_ref._v != ref._v) bad++;
    if (bad)
        TXS_DIAG("TXN st owner: kind %d role %d state %d, expected "
                 "kind %d role %d state %d\n", (int)ep.kind, (int)e->role,
                 (int)e->state, (int)want_kind, want_role, want_state);
    return bad;
}

/* Retirement proven on the RAW graph as well as through the production lookup:
 * a hidden or duplicate edge pointing at the freed slot is invisible to every
 * real lookup while a table scan still sees it. The slot is the one DECLARED
 * before ingress -- never rediscovered through the index being audited. */
static void check_st_retired_slot(moq_session_t *s, moq_stream_ref_t ref,
                                  int slot, const char *what)
{
    MOQ_TEST_CHECK_EQ_INT(st_busy_count(s), 0);
    check_unregistered(s, ref);
    og_graph_t g;
    og_capture(s, &g);
    failures += og_check_integrity(&g, what);
    failures += og_check_no_edge(&g, OG_DOM_REQ_STREAMREF, ref._v, what);
    failures += og_check_no_edge(&g, OG_DOM_NS_REF, ref._v, what);
    failures += og_check_owner_unreferenced(&g, (int)MOQ_REQ_SUBSCRIBE_TRACKS,
                                            slot, what);
}

static void run_st_case(const fin_owner_case_t *f, uint32_t max_events,
                        bool fin_in_request, bool fill_ring, bool reject_now)
{
    moq_session_t *s = make_session_caps(MOQ_PERSPECTIVE_SERVER, max_events);
    st_ctx_t *c = (st_ctx_t *)f->ctx;
    moq_stream_ref_t ref = c->ref;
    c->last_handle = 0;

    /* The identity the request must land on is derived from pool state before
     * ingress, so the surfaced handle is checked against a declaration rather
     * than read back from whatever admission produced. */
    int slot = st_declare_free_slot(s);
    MOQ_TEST_CHECK(slot >= 0);
    if (slot < 0) { moq_session_destroy(s); return; }
    uint32_t gen = s->track_subs[slot].generation | 1u;
    uint64_t handle = moq_handle_pack(MOQ_HANDLE_POOL_SUBSCRIBE_TRACKS,
                                      s->session_tag, gen, (uint32_t)slot);
    /* The co-allocated receive buffer survives entry reuse, so its identity is
     * part of the declaration and is taken from the FREE slot. */
    const void *slot_recv_buf = (const void *)s->track_subs[slot].req_recv_buf;
    size_t slot_recv_cap = s->track_subs[slot].req_recv_cap;

    /* The refused handoff is measured against a capture taken BEFORE it, so the
     * unrelated session facts cannot be defined by the call under test. */
    txs_snapshot_t pre_feed;
    txs_capture(s, &ref, 1, &pre_feed);

    /* With the FIN in the request chunk and room for both events the
     * cancellation runs immediately; with room for only the request event it
     * defers and the feed reports backpressure. */
    bool defer = fin_in_request && max_events == 1;
    MOQ_TEST_CHECK_EQ_INT((int)f->feed(s, f->ctx, ref, fin_in_request),
                          (int)(defer ? MOQ_ERR_WOULD_BLOCK : MOQ_OK));

    txs_norm_vec_t got, want;
    txs_norm_init(&got);
    txs_norm_init(&want);
    collect_events(s, &f->hooks, &got);
    want_st_request(&want, c->last_handle);
    if (fin_in_request && !defer)
        want_st_cancelled(&want, c->last_handle);
    failures += txs_norm_equals(&got, &want, f->name);
    txs_norm_free(&got);
    txs_norm_free(&want);
    MOQ_TEST_CHECK_EQ_U64(c->last_handle, handle);

    if (fin_in_request && !defer) {
        check_st_retired_slot(s, ref, slot, "st same-call FIN retired");
        MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, 0);
        check_no_output(s, &f->hooks);
        moq_session_destroy(s);
        return;
    }

    failures += st_owner_present_problems(s, ref, f->owner_kind, f->owner_role,
                                          f->owner_state);
    st_hook_state_t owner_before;
    f->hooks.capture(s, f->ctx, &owner_before);
    MOQ_TEST_CHECK_EQ_U64(owner_before.handle, handle);
    MOQ_TEST_CHECK_EQ_U64(owner_before.req_stream_ref, ref._v);
    /* Resolve once and guard: a mutant that loses the owner must still reach a
     * normal exit with diagnostics, never a NULL dereference. */
    const moq_track_sub_entry_t *live = st_by_ref(s, ref);
    MOQ_TEST_CHECK(live != NULL);
    if (live) {
        MOQ_TEST_CHECK_EQ_INT((int)(live - s->track_subs), slot);
        MOQ_TEST_CHECK_EQ_U64(live->generation, gen);
    }
    /* The two FIN facts are distinct: a wire FIN whose cancellation was
     * refused on event capacity is held as transitional ownership, not as the
     * durable latch, and a request without FIN holds neither. */
    MOQ_TEST_CHECK_EQ_INT(owner_before.req_recv_fin, 0);
    MOQ_TEST_CHECK_EQ_INT(owner_before.handoff_fin, defer ? 1 : 0);

    /* The declared track-sub owner inventory, built before ingress. */
    sto_snap_t sto_want, sto_have;
    sto_declare(&sto_want, slot, gen, handle, c->request_id, ref._v,
                defer ? 1 : 0, slot_recv_buf, slot_recv_cap);
    sto_read(s, slot, &sto_have);
    failures += sto_diff(&sto_have, &sto_want, "st committed owner");

    /* The handoff's declared session delta (see st_check_handoff_delta). */
    failures += st_check_handoff_delta(s, ref, &pre_feed, slot, gen,
                                       "st handoff delta");

    /* The live owner's ABSOLUTE edge set, validated BEFORE any graph derived
     * from it is used as a conservation baseline. */
    og_graph_t live_graph;
    og_capture(s, &live_graph);
    failures += og_check_integrity(&live_graph, "st live");
    failures += st_check_owner_edges(&live_graph, slot, ref, "st live");

    if (fill_ring) fill_drain_ring(s);
    drain_snap_t ring_before;
    drain_snap(s, &ring_before);

    if (!reject_now) {
        /* The deferred cancellation completes exactly once on retry, without
         * a drain reference and without wire output. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, false, 1),
            (int)MOQ_OK);
        txs_norm_vec_t got2, want2;
        txs_norm_init(&got2);
        txs_norm_init(&want2);
        collect_events(s, &f->hooks, &got2);
        want_st_cancelled(&want2, c->last_handle);
        failures += txs_norm_equals(&got2, &want2, f->name);
        txs_norm_free(&got2);
        txs_norm_free(&want2);
        check_st_retired_slot(s, ref, slot, "st retry retired");
        drain_snap_t now;
        drain_snap(s, &now);
        failures += drain_multiset_equals(&now, &ring_before, "st retry");
        check_no_output(s, &f->hooks);
        moq_session_destroy(s);
        return;
    }

    bool expect_drain = !fin_in_request;
    moq_result_t want_rc = (expect_drain && fill_ring) ? MOQ_ERR_WOULD_BLOCK
                                                       : MOQ_OK;
    txs_snapshot_t before;
    txs_capture(s, &ref, 1, &before);
    expect_after_call_prepare(&before);
    MOQ_TEST_CHECK_EQ_INT((int)f->app_reject(s, f->ctx, c->last_handle),
                          (int)want_rc);

    if (want_rc == MOQ_ERR_WOULD_BLOCK) {
        failures += txs_check_eq(s, &ref, 1, &before, f->name);
        failures += f->hooks.check(s, f->ctx, &owner_before);
        failures += st_owner_present_problems(s, ref, f->owner_kind,
                                              f->owner_role, f->owner_state);
        /* The refusal must preserve the DECLARED record and the owner's exact
         * edge set, and conserve the whole topology validated before it -- the
         * narrow hook alone cannot see a field it never modelled or an edge
         * inserted after `live_graph` was taken. */
        sto_read(s, slot, &sto_have);
        failures += sto_diff(&sto_have, &sto_want, "st blocked owner");
        og_graph_t g;
        og_capture(s, &g);
        failures += og_check_integrity(&g, "st blocked");
        failures += st_check_owner_edges(&g, slot, ref, "st blocked");
        failures += og_check_same_topology(&g, &live_graph, "st blocked");
        drain_snap_t now;
        drain_snap(s, &now);
        failures += drain_multiset_equals(&now, &ring_before, "st blocked");
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
    failures += drain_multiset_equals(&now, &want_ring, "st granted");
    check_st_retired_slot(s, ref, slot, "st granted retired");
    check_reject_only_output(s, &f->hooks, ref, f->name);

    if (expect_drain) {
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
            (int)MOQ_OK);
        drain_snap_t after;
        drain_snap(s, &after);
        failures += drain_multiset_equals(&after, &ring_before, "st released");
        check_st_retired_slot(s, ref, slot, "st released retired");
        check_no_output(s, &f->hooks);
    }
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
    moq_session_destroy(s);
}

/* Clear-on-reuse for the subscribe-tracks pool. This family's consumer can
 * block, so the marker is genuinely live across a refusal: the case drives a
 * FIN-carrying request into a one-slot event queue, completes the deferred
 * cancellation from the empty re-feed, and then proves the observed close did
 * not follow the physical slot to its next owner. Both generations and both
 * packed handles are derived before ingress. */
static void run_st_slot_reuse_case(const fin_owner_case_t *f)
{
    moq_session_t *s = make_session_caps(MOQ_PERSPECTIVE_SERVER, 1);
    st_ctx_t *c = (st_ctx_t *)f->ctx;

    int slot = st_declare_free_slot(s);
    MOQ_TEST_CHECK(slot >= 0);
    if (slot < 0) { moq_session_destroy(s); return; }
    uint32_t gen_first = s->track_subs[slot].generation | 1u;
    uint32_t gen_reuse = (gen_first + 1u) | 1u;   /* the free advances once */
    uint64_t handle_first = moq_handle_pack(MOQ_HANDLE_POOL_SUBSCRIBE_TRACKS,
                                            s->session_tag, gen_first,
                                            (uint32_t)slot);
    uint64_t handle_reuse = moq_handle_pack(MOQ_HANDLE_POOL_SUBSCRIBE_TRACKS,
                                            s->session_tag, gen_reuse,
                                            (uint32_t)slot);
    MOQ_TEST_CHECK(handle_first != handle_reuse);
    const void *slot_recv_buf = (const void *)s->track_subs[slot].req_recv_buf;
    size_t slot_recv_cap = s->track_subs[slot].req_recv_cap;

    /* 1. SUBSCRIBE_TRACKS + FIN into a one-slot queue: the request event takes
     *    the slot, so the cancellation is refused and the marker survives it. */
    moq_stream_ref_t first = c->ref;
    c->last_handle = 0;
    txs_snapshot_t pre_first;
    txs_capture(s, &first, 1, &pre_first);
    MOQ_TEST_CHECK_EQ_INT((int)f->feed(s, f->ctx, first, true),
                          (int)MOQ_ERR_WOULD_BLOCK);
    txs_norm_vec_t got, want;
    txs_norm_init(&got);
    txs_norm_init(&want);
    collect_events(s, &f->hooks, &got);
    want_st_request(&want, c->last_handle);
    failures += txs_norm_equals(&got, &want, "st reuse first");
    txs_norm_free(&got);
    txs_norm_free(&want);
    MOQ_TEST_CHECK_EQ_U64(c->last_handle, handle_first);
    failures += st_owner_present_problems(s, first, f->owner_kind,
                                          f->owner_role, f->owner_state);
    {
        const moq_track_sub_entry_t *e = st_by_ref(s, first);
        MOQ_TEST_CHECK(e != NULL);
        if (e) {
            MOQ_TEST_CHECK_EQ_INT((int)(e - s->track_subs), slot);
            MOQ_TEST_CHECK_EQ_U64(e->generation, gen_first);
            MOQ_TEST_CHECK_EQ_U64(e->handle._opaque, handle_first);
            /* Held as transitional ownership, never as the durable latch. */
            MOQ_TEST_CHECK_EQ_INT(e->req_recv_fin, 0);
            MOQ_TEST_CHECK_EQ_INT(e->handoff_fin_pending, 1);
        }
    }
    {   /* The declared owner inventory, and the owner's exact edge set validated
         * before any graph derived from it becomes a conservation baseline. */
        sto_snap_t w, h;
        sto_declare(&w, slot, gen_first, handle_first, c->request_id, first._v,
                    1, slot_recv_buf, slot_recv_cap);
        sto_read(s, slot, &h);
        failures += sto_diff(&h, &w, "st reuse first owner");
        og_graph_t g;
        og_capture(s, &g);
        failures += og_check_integrity(&g, "st reuse first");
        failures += st_check_owner_edges(&g, slot, first, "st reuse first");
    }
    /* The retention feed's own declared session delta: without it an unrelated
     * mutation is invisible in exactly the proof that exists to catch one. */
    failures += st_check_handoff_delta(s, first, &pre_first, slot, gen_first,
                                       "st reuse handoff delta");

    /* 2. The empty re-feed completes the cancellation exactly once, drainlessly,
     *    and retires the owner. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, first, NULL, 0, false, 1),
        (int)MOQ_OK);
    txs_norm_init(&got);
    txs_norm_init(&want);
    collect_events(s, &f->hooks, &got);
    want_st_cancelled(&want, handle_first);
    failures += txs_norm_equals(&got, &want, "st reuse retry");
    txs_norm_free(&got);
    txs_norm_free(&want);
    MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, 0);
    check_no_output(s, &f->hooks);

    /* 3. The freed record: generation advanced, BOTH FIN facts clear, and no
     *    edge anywhere still points at the retired owner. */
    MOQ_TEST_CHECK_EQ_INT((int)s->track_subs[slot].state,
                          (int)MOQ_TRACK_SUB_FREE);
    MOQ_TEST_CHECK_EQ_U64(s->track_subs[slot].generation, gen_reuse - 1u);
    MOQ_TEST_CHECK_EQ_INT(s->track_subs[slot].req_recv_fin, 0);
    MOQ_TEST_CHECK_EQ_INT(s->track_subs[slot].handoff_fin_pending, 0);
    check_st_retired_slot(s, first, slot, "st reuse retired");

    /* 4. A distinct no-FIN SUBSCRIBE_TRACKS reuses that physical slot and
     *    holds neither FIN fact. */
    moq_stream_ref_t second = moq_stream_ref_from_u64(c->ref._v + 4);
    moq_stream_ref_t saved_ref = c->ref;
    uint64_t saved_id = c->request_id;
    c->ref = second;
    c->request_id = saved_id + 2;
    c->last_handle = 0;
    MOQ_TEST_CHECK_EQ_INT((int)f->feed(s, f->ctx, second, false), (int)MOQ_OK);
    txs_norm_init(&got);
    txs_norm_init(&want);
    collect_events(s, &f->hooks, &got);
    want_st_request(&want, c->last_handle);
    failures += txs_norm_equals(&got, &want, "st reuse second");
    txs_norm_free(&got);
    txs_norm_free(&want);
    failures += st_owner_present_problems(s, second, f->owner_kind,
                                          f->owner_role, f->owner_state);
    {
        const moq_track_sub_entry_t *e = st_by_ref(s, second);
        MOQ_TEST_CHECK(e != NULL);
        if (e) {
            MOQ_TEST_CHECK_EQ_INT((int)(e - s->track_subs), slot);
            MOQ_TEST_CHECK_EQ_U64(e->generation, gen_reuse);
            MOQ_TEST_CHECK_EQ_U64(e->handle._opaque, handle_reuse);
            MOQ_TEST_CHECK_EQ_INT(e->req_recv_fin, 0);
            MOQ_TEST_CHECK_EQ_INT(e->handoff_fin_pending, 0);
        }
        MOQ_TEST_CHECK_EQ_U64(c->last_handle, handle_reuse);
        check_registered(s, second, MOQ_REQ_SUBSCRIBE_TRACKS, slot);
        sto_snap_t w, h;
        sto_declare(&w, slot, gen_reuse, handle_reuse, c->request_id, second._v,
                    0, slot_recv_buf, slot_recv_cap);
        sto_read(s, slot, &h);
        failures += sto_diff(&h, &w, "st reuse second owner");
    }

    /* 5. Its peer half is genuinely open, so its rejection owes a drain
     *    reference and an exhausted ring refuses it transactionally. */
    fill_drain_ring(s);
    size_t filled = s->drain_ref_count;
    og_graph_t graph_before;
    og_capture(s, &graph_before);
    failures += og_check_integrity(&graph_before, "st reuse live");
    /* Validate the reused owner's ABSOLUTE edge set before this graph is used
     * as the conservation baseline -- an undeclared or duplicate edge must not
     * be able to become the thing conservation preserves. */
    failures += st_check_owner_edges(&graph_before, slot, second,
                                     "st reuse live");
    st_hook_state_t owner;
    f->hooks.capture(s, f->ctx, &owner);
    txs_snapshot_t before;
    txs_capture(s, &second, 1, &before);
    expect_after_call_prepare(&before);
    MOQ_TEST_CHECK_EQ_INT((int)f->app_reject(s, f->ctx, handle_reuse),
                          (int)MOQ_ERR_WOULD_BLOCK);
    failures += txs_check_eq(s, &second, 1, &before, "st reuse blocked");
    failures += f->hooks.check(s, f->ctx, &owner);
    MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, filled);
    {
        sto_snap_t w, h;
        sto_declare(&w, slot, gen_reuse, handle_reuse, c->request_id, second._v,
                    0, slot_recv_buf, slot_recv_cap);
        sto_read(s, slot, &h);
        failures += sto_diff(&h, &w, "st reuse blocked owner");
        og_graph_t g;
        og_capture(s, &g);
        failures += og_check_integrity(&g, "st reuse blocked");
        failures += st_check_owner_edges(&g, slot, second, "st reuse blocked");
        failures += og_check_same_topology(&g, &graph_before,
                                           "st reuse blocked");
    }
    check_no_output(s, &f->hooks);

    /* 6. The retired first-generation handle stays stale after the reuse. */
    txs_snapshot_t before_stale;
    txs_capture(s, &second, 1, &before_stale);
    expect_after_call_prepare(&before_stale);
    MOQ_TEST_CHECK_EQ_INT((int)f->app_reject(s, f->ctx, handle_first),
                          (int)MOQ_ERR_STALE_HANDLE);
    failures += txs_check_eq(s, &second, 1, &before_stale,
                             "st stale after reuse");
    failures += f->hooks.check(s, f->ctx, &owner);
    MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, filled);
    check_no_output(s, &f->hooks);

    c->ref = saved_ref;
    c->request_id = saved_id;
    moq_session_destroy(s);
}

/* Permanent self-checks for the inventory's refusal paths. Each perturbs one
 * field of a live owner, requires the record to come back INCOMPARABLE (so the
 * comparison FAILS rather than reading out of range), and restores the field
 * before the fixture is destroyed. Quiet: a passing run prints nothing. */
static void st_selfcheck_invalid(moq_session_t *s, int slot,
                                 const sto_snap_t *want, const char *what)
{
    sto_snap_t got;
    sto_read(s, slot, &got);
    if (!got.invalid) {
        fprintf(stderr, "FAIL: %s:%d: sto_read accepted %s\n",
                __FILE__, __LINE__, what);
        failures++;
        return;
    }
    txs_quiet = 1;
    int diff = sto_diff(&got, want, what);
    txs_quiet = 0;
    if (diff == 0) {
        fprintf(stderr, "FAIL: %s:%d: sto_diff compared equal on %s\n",
                __FILE__, __LINE__, what);
        failures++;
    }
}

static void st_inventory_selfchecks(const fin_owner_case_t *f)
{
    moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
    st_ctx_t *c = (st_ctx_t *)f->ctx;
    int slot = st_declare_free_slot(s);
    MOQ_TEST_CHECK(slot >= 0);
    if (slot < 0) { moq_session_destroy(s); return; }

    /* A live committed owner to perturb. */
    sto_snap_t valid;
    c->last_handle = 0;
    MOQ_TEST_CHECK_EQ_INT((int)f->feed(s, f->ctx, c->ref, false), (int)MOQ_OK);
    {   /* drain the request event */
        txs_norm_vec_t d;
        txs_norm_init(&d);
        collect_events(s, &f->hooks, &d);
        txs_norm_free(&d);
    }
    sto_read(s, slot, &valid);
    MOQ_TEST_CHECK(valid.invalid == 0);
    moq_track_sub_entry_t *e = &s->track_subs[slot];

    /* Out-of-range slots are refused without touching the pool, and their
     * records must also FAIL to compare -- the comparator end, not only the
     * capture end. */
    st_selfcheck_invalid(s, (int)s->track_sub_cap, &valid, "slot past the pool");
    st_selfcheck_invalid(s, -1, &valid, "negative slot");

    size_t       save_pc   = e->prefix_count;
    moq_bytes_t *save_pp   = e->prefix_parts;
    size_t       save_tc   = e->token_count;
    size_t       save_rl   = e->req_recv_len;
    uint8_t     *save_rb   = e->req_recv_buf;
    const uint8_t *save_d0 = e->prefix_parts ? e->prefix_parts[0].data : NULL;

    e->prefix_count = STO_MAX_PARTS + 1;
    st_selfcheck_invalid(s, slot, &valid, "excessive prefix count");
    e->prefix_count = save_pc;

    e->prefix_parts = NULL;
    st_selfcheck_invalid(s, slot, &valid, "NULL parts array with nonzero count");
    e->prefix_parts = save_pp;

    if (save_pp && save_pc > 0) {
        e->prefix_parts[0].data = NULL;      /* part 0 is non-empty ("live") */
        st_selfcheck_invalid(s, slot, &valid, "NULL nonempty part data");
        e->prefix_parts[0].data = save_d0;
    }

    e->token_count = MOQ_DECODED_MAX_TOKENS + 1;
    st_selfcheck_invalid(s, slot, &valid, "excessive token count");
    e->token_count = save_tc;

    e->req_recv_len = STO_BUF_MAX + 1;
    st_selfcheck_invalid(s, slot, &valid, "oversized retained length");
    e->req_recv_len = save_rl;

    e->req_recv_len = 4;
    e->req_recv_buf = NULL;
    st_selfcheck_invalid(s, slot, &valid, "retained length with NULL buffer");
    e->req_recv_buf = save_rb;
    e->req_recv_len = save_rl;

    /* Prefix allocation topology, all four branches. Every perturbation is
     * itself safe -- a nulled pointer, an offset that stays INSIDE the owned
     * buffer, and lengths that only shrink or grow the declared extent -- so a
     * refusal is proven without the probe ever risking an out-of-range read. */
    uint8_t     *save_pb   = e->prefix_buf;
    size_t       save_pbl  = e->prefix_buf_len;
    const uint8_t *save_d1 = (save_pp && save_pc > 1) ? save_pp[1].data : NULL;

    e->prefix_buf = NULL;
    st_selfcheck_invalid(s, slot, &valid, "NULL prefix buffer, nonempty prefix");
    e->prefix_buf = save_pb;

    if (save_pp && save_pc > 1 && save_pb) {
        e->prefix_parts[1].data = save_pb;      /* offset 0, still in-bounds */
        st_selfcheck_invalid(s, slot, &valid, "part addressing the wrong offset");
        e->prefix_parts[1].data = save_d1;
    }

    e->prefix_buf_len = 1;                      /* shorter than part 0 */
    st_selfcheck_invalid(s, slot, &valid, "prefix buffer length too short");
    e->prefix_buf_len = save_pbl;

    e->prefix_buf_len = save_pbl + 1;           /* parts fit, total disagrees */
    st_selfcheck_invalid(s, slot, &valid, "prefix extent length mismatch");
    e->prefix_buf_len = save_pbl;

    /* Every perturbation restored: the record compares equal again, so the
     * fixture is destroyed in exactly the state the session owns. */
    sto_snap_t back;
    sto_read(s, slot, &back);
    failures += sto_diff(&back, &valid, "st inventory restored");
    moq_session_destroy(s);
}

static void test_subscribe_tracks(void)
{
    fin_owner_case_t f = st_family();
    MOQ_TEST_CHECK_EQ_INT(fin_owner_problems(&f), 0);
    st_inventory_selfchecks(&f);
    run_st_case(&f, 0, false, false, true);   /* control: drain, then released */
    run_st_case(&f, 0, true,  false, true);   /* same-call FIN: cancels at once */
    run_st_case(&f, 1, true,  false, false);  /* deferred, completed by retry */
    run_st_case(&f, 1, true,  false, true);   /* answered in the window */
    run_st_case(&f, 1, true,  true,  true);   /* exhausted ring, FIN observed */
    run_st_case(&f, 1, false, true,  true);   /* exhausted ring, no FIN */
    /* The observed close does not follow the slot to its next owner. */
    run_st_slot_reuse_case(&f);
}


/* -- the declared namespace-sub owner inventory ---------------------- */

/* The DECLARED mutation inventory of one namespace-sub entry: its identity
 * (state, stream kind, generation, handle, request id and ref, the whole
 * request endpoint), its request shape (interest, forward), its lifecycle and
 * auth flags, the exact prefix it stored, and the request bytes it retained.
 * Deliberately NOT covered, because nothing on these routes touches them: the
 * announced-suffix set and its receive-budget charge, the resolved-token
 * storage itself (only its count), and the auth transaction's overlay. Every
 * bound is checked BEFORE a dereference, so a corrupt entry becomes an
 * incomparable record rather than an out-of-range read. */
#define NSO_BUF_MAX 256
/* draft-18 caps a Track Namespace at 32 fields. */
#define NSO_MAX_PARTS 32

typedef struct nso_snap {
    int      present;
    int      state;
    int      stream_kind;
    uint32_t generation;
    uint64_t handle;
    uint64_t request_id;
    uint64_t stream_ref;
    int      ep_kind;
    int      ep_slot;
    int      ep_has_request_id;
    uint64_t ep_request_id;
    int      ep_has_stream_ref;
    uint64_t ep_stream_ref;
    int      interest;
    int      forward;
    int      pending_fin;
    int      parse_complete;
    int      got_response;
    int      handoff_fin;
    int      closing_remote_error;
    int      goaway_sent;
    int      auth_processed;
    int      auth_committed;
    uint64_t auth_reject_code;
    size_t   token_count;
    int      prefix_valid;
    size_t   prefix_count;
    size_t   prefix_buf_len; /* the owned extent the parts must sum to */
    size_t   prefix_len;      /* encoded (u8 len, bytes) per part */
    uint8_t  prefix[NSO_BUF_MAX];
    const void *recv_buf;     /* preserved across reuse: identity matters */
    size_t   recv_cap;
    size_t   recv_len;
    uint8_t  recv[NSO_BUF_MAX];
    int      invalid;
} nso_snap_t;

static void nso_read(const moq_session_t *s, int slot, nso_snap_t *v)
{
    memset(v, 0, sizeof(*v));
    if (slot < 0 || (size_t)slot >= s->ns_sub_cap) { v->invalid = 1; return; }
    const moq_ns_sub_entry_t *e = &s->ns_subs[slot];
    v->present = (e->state != MOQ_NS_SUB_FREE);
    v->state = (int)e->state;
    v->stream_kind = (int)e->stream_kind;
    v->generation = e->generation;
    v->handle = e->handle._opaque;
    v->request_id = e->request_id;
    v->stream_ref = e->stream_ref._v;
    v->ep_kind = (int)e->request_ep.kind;
    v->ep_slot = e->request_ep.slot;
    v->ep_has_request_id = e->request_ep.has_request_id ? 1 : 0;
    v->ep_request_id = e->request_ep.request_id;
    v->ep_has_stream_ref = e->request_ep.has_stream_ref ? 1 : 0;
    v->ep_stream_ref = e->request_ep.stream_ref._v;
    v->interest = (int)e->namespace_interest;
    v->forward = e->forward ? 1 : 0;
    v->pending_fin = e->pending_fin ? 1 : 0;
    v->parse_complete = e->parse_complete ? 1 : 0;
    v->got_response = e->got_response ? 1 : 0;
    v->handoff_fin = e->handoff_fin_pending ? 1 : 0;
    v->closing_remote_error = e->closing_remote_error ? 1 : 0;
    v->goaway_sent = e->goaway_sent ? 1 : 0;
    v->auth_processed = e->auth_processed ? 1 : 0;
    v->auth_committed = e->auth_committed ? 1 : 0;
    v->auth_reject_code = e->auth_reject_code;
    v->token_count = e->token_count;
    v->prefix_valid = e->prefix_valid ? 1 : 0;
    v->prefix_count = e->prefix_count;
    /* Refuse BEFORE dereferencing: a count past the wire maximum, a count with
     * no array, or a non-empty part with no data would each be an out-of-range
     * read inside the harness meant to diagnose it. */
    if (e->prefix_count > NSO_MAX_PARTS) { v->invalid = 1; return; }
    if (e->prefix_count > 0 && !e->prefix_parts) { v->invalid = 1; return; }
    /* Allocation topology fails CLOSED, to the same discipline as the track-sub
     * inventory: for a stored prefix both allocations must exist, every part
     * must lie inside the owned buffer by SUBTRACTION-ONLY bounds proven BEFORE
     * `prefix_buf + off` is formed, its pointer must equal that declared offset,
     * and the offsets must sum to the owned extent. Bytes are copied only after
     * every check for that part passes, so a corrupt readable pointer is
     * refused rather than compared, and an unreadable one is never touched. */
    v->prefix_buf_len = e->prefix_buf_len;
    if (e->prefix_valid && e->prefix_count > 0 && !e->prefix_buf) {
        v->invalid = 1;
        return;
    }
    {
        size_t off = 0;
        for (size_t i = 0; i < e->prefix_count; i++) {
            size_t l = e->prefix_parts[i].len;
            if (l > 0 && !e->prefix_parts[i].data) { v->invalid = 1; return; }
            if (l > 255 || v->prefix_len + 1 + l > NSO_BUF_MAX) {
                v->invalid = 1;
                return;
            }
            if (!e->prefix_valid) continue;   /* counted, not encoded */
            if (off > e->prefix_buf_len || l > e->prefix_buf_len - off) {
                v->invalid = 1;
                return;
            }
            if (e->prefix_parts[i].data != (const uint8_t *)e->prefix_buf + off) {
                v->invalid = 1;
                return;
            }
            off += l;
            v->prefix[v->prefix_len++] = (uint8_t)l;
            if (l) memcpy(v->prefix + v->prefix_len, e->prefix_parts[i].data, l);
            v->prefix_len += l;
        }
        if (e->prefix_valid && off != e->prefix_buf_len) {
            v->invalid = 1;
            return;
        }
    }
    v->recv_buf = (const void *)e->recv_buf;
    v->recv_cap = e->recv_cap;
    v->recv_len = e->recv_len;
    if (e->recv_len > NSO_BUF_MAX || (e->recv_len && !e->recv_buf)) {
        v->invalid = 1;
        return;
    }
    if (e->recv_len) memcpy(v->recv, e->recv_buf, e->recv_len);
}

static int nso_diff(const nso_snap_t *have, const nso_snap_t *want,
                    const char *what)
{
    if (have->invalid || want->invalid) {
        TXS_DIAG("TXN %s: incomparable namespace-sub record\n", what);
        return 1;
    }
    int bad = 0;
#define NSO_EQ(field) do { \
        if ((uint64_t)have->field != (uint64_t)want->field) { \
            TXS_DIAG("TXN %s: ns owner " #field " %llu, expected %llu\n", what, \
                     (unsigned long long)have->field, \
                     (unsigned long long)want->field); \
            bad++; \
        } \
    } while (0)
    NSO_EQ(present); NSO_EQ(state); NSO_EQ(stream_kind); NSO_EQ(generation);
    NSO_EQ(handle); NSO_EQ(request_id); NSO_EQ(stream_ref);
    NSO_EQ(ep_kind); NSO_EQ(ep_slot); NSO_EQ(ep_has_request_id);
    NSO_EQ(ep_request_id); NSO_EQ(ep_has_stream_ref); NSO_EQ(ep_stream_ref);
    NSO_EQ(interest); NSO_EQ(forward); NSO_EQ(pending_fin);
    NSO_EQ(parse_complete); NSO_EQ(got_response); NSO_EQ(handoff_fin);
    NSO_EQ(closing_remote_error);
    NSO_EQ(goaway_sent); NSO_EQ(auth_processed); NSO_EQ(auth_committed);
    NSO_EQ(auth_reject_code); NSO_EQ(token_count); NSO_EQ(prefix_valid);
    NSO_EQ(prefix_count); NSO_EQ(prefix_len); NSO_EQ(recv_len);
    /* Storage facts are ALWAYS compared. Every namespace-sub receive buffer is
     * carved out of the session allocation at create (`session.c`, the
     * `ns_subs[i].recv_buf = mem + off_ns_recv + …` loop), so a free slot
     * exposes its buffer identity and capacity before any ingress -- there is
     * no slot for which they are unknowable. */
    NSO_EQ(prefix_buf_len); NSO_EQ(recv_cap);
    if (have->recv_buf != want->recv_buf) {
        TXS_DIAG("TXN %s: ns owner receive buffer identity changed\n", what);
        bad++;
    }
#undef NSO_EQ
    if (have->prefix_len == want->prefix_len && have->prefix_len &&
        memcmp(have->prefix, want->prefix, have->prefix_len) != 0) {
        TXS_DIAG("TXN %s: ns owner prefix bytes differ\n", what);
        bad++;
    }
    if (have->recv_len == want->recv_len && have->recv_len &&
        memcmp(have->recv, want->recv, have->recv_len) != 0) {
        TXS_DIAG("TXN %s: ns owner retained request bytes differ\n", what);
        bad++;
    }
    return bad;
}

/* -- SUBSCRIBE_NAMESPACE (P6) ---------------------------------------- */

/* This family's owner lives in the namespace index rather than the request
 * registry, and its durable FIN state is `pending_fin`. Its owner-present FIN
 * outcome is a reciprocal close: the peer's graceful cancel is answered by
 * closing our own send half, then the entry is reclaimed. That close needs an
 * action slot, so an action-blocked call must preserve both the owner and the
 * observed close for the retry. */

static txs_op_hooks_t nss_hooks_for_actions;   /* normalize_action only */

typedef struct nss_ctx {
    uint64_t         request_id;
    moq_stream_ref_t ref;
    uint64_t         last_handle;
    uint8_t          last_req[192];
    size_t           last_req_len;
} nss_ctx_t;

typedef struct nss_hook_state {
    int      present;
    uint64_t handle;
    uint64_t stream_ref;
    int      pending_fin;
    int      handoff_fin;
    int      state;
    uint32_t generation;
    int      busy;
} nss_hook_state_t;

static int ns_busy_count(const moq_session_t *s)
{
    int n = 0;
    for (size_t i = 0; i < s->ns_sub_cap; i++)
        if (s->ns_subs[i].state != MOQ_NS_SUB_FREE) n++;
    return n;
}

static const moq_ns_sub_entry_t *ns_by_ref(const moq_session_t *s,
                                           moq_stream_ref_t ref)
{
    int32_t slot = moq_index_find(s->idx_ns_by_ref, s->idx_ns_mask, ref._v);
    if (slot < 0 || (size_t)slot >= s->ns_sub_cap) return NULL;
    return &s->ns_subs[slot];
}

/* Allocation-identity CONSERVATION, and nothing more. A stored prefix's
 * addresses cannot be predicted before first ingress, so they are never used to
 * authorize the first call's extent, layout or bytes -- the declared inventory
 * does that. Once that absolute check has passed, these identities must not
 * change while the owner stays live, and this snapshot is what proves it. */
typedef struct nsc_snap {
    const void *prefix_buf;
    const void *prefix_parts;
    const void *part0_data;
    size_t      count;
    int         invalid;
} nsc_snap_t;

static void nsc_read(const moq_session_t *s, int slot, nsc_snap_t *v)
{
    memset(v, 0, sizeof(*v));
    if (slot < 0 || (size_t)slot >= s->ns_sub_cap) { v->invalid = 1; return; }
    const moq_ns_sub_entry_t *e = &s->ns_subs[slot];
    v->prefix_buf = (const void *)e->prefix_buf;
    v->prefix_parts = (const void *)e->prefix_parts;
    v->count = e->prefix_count;
    if (e->prefix_count > 0) {
        if (!e->prefix_parts) { v->invalid = 1; return; }
        v->part0_data = (const void *)e->prefix_parts[0].data;
    }
}

static int nsc_same(const nsc_snap_t *a, const nsc_snap_t *b, const char *what)
{
    if (a->invalid || b->invalid) {
        TXS_DIAG("TXN %s: incomparable allocation snapshot\n", what);
        return 1;
    }
    int bad = 0;
    if (a->prefix_buf != b->prefix_buf) bad++;
    if (a->prefix_parts != b->prefix_parts) bad++;
    if (a->part0_data != b->part0_data) bad++;
    if (a->count != b->count) bad++;
    if (bad)
        TXS_DIAG("TXN %s: prefix allocation identity changed\n", what);
    return bad;
}

static int nss_declare_free_slot(const moq_session_t *s)
{
    for (size_t i = 0; i < s->ns_sub_cap; i++)
        if (s->ns_subs[i].state == MOQ_NS_SUB_FREE) return (int)i;
    return -1;
}

/* A committed publisher-side namespace-sub owner in these rows holds its own
 * index edge AND its pending-phase request-id edge, and no request-stream-ref
 * edge -- the staging slot's key is reclaimed when that slot is freed. */
static int nss_check_owner_edges(const og_graph_t *g, int slot,
                                 moq_stream_ref_t ref, uint64_t rid,
                                 const char *what)
{
    const og_edge_spec_t want[] = {
        { OG_DOM_NS_REF,  ref._v },
        { OG_DOM_REQ_RID, rid    },
    };
    int bad = og_check_owner_edges(g, (int)MOQ_REQ_NAMESPACE_SUB, slot,
                                   want, 2, what);
    bad += og_check_no_edge(g, OG_DOM_REQ_STREAMREF, ref._v, what);
    return bad;
}

/* The record a committed publisher-side owner must hold, built from values the
 * fixture declared before ingress -- never read back from the call under test.
 * `recv` is the exact SUBSCRIBE_NAMESPACE the fixture encoded. */
static void nss_declare(nso_snap_t *v, int slot, uint32_t gen, uint64_t handle,
                        uint64_t rid, uint64_t ref, int handoff_fin,
                        const uint8_t *req, size_t req_len,
                        const void *recv_buf, size_t recv_cap)
{
    (void)slot;
    memset(v, 0, sizeof(*v));
    v->present = 1;
    v->state = (int)MOQ_NS_SUB_PENDING_PUBLISHER;
    v->stream_kind = (int)MOQ_STREAM_KIND_NAMESPACE_SUB;
    v->generation = gen;
    v->handle = handle;
    v->request_id = rid;
    v->stream_ref = ref;
    v->ep_kind = (int)MOQ_REQ_NAMESPACE_SUB;
    v->ep_slot = slot;
    v->ep_has_request_id = 1;
    v->ep_request_id = rid;
    v->ep_has_stream_ref = 1;
    v->ep_stream_ref = ref;
    v->interest = (int)MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
    v->forward = 1;                   /* the wire default when unset */
    v->pending_fin = 0;
    v->parse_complete = 1;
    v->got_response = 0;
    v->handoff_fin = handoff_fin;
    v->closing_remote_error = 0;
    v->goaway_sent = 0;
    v->auth_processed = 1;
    v->auth_committed = 1;
    v->auth_reject_code = 0;
    v->token_count = 1;               /* nss_feed carries one USE_VALUE token */
    v->prefix_valid = 1;
    v->prefix_count = 2;              /* ("live", "eu") */
    v->prefix[v->prefix_len++] = 4;
    memcpy(v->prefix + v->prefix_len, "live", 4); v->prefix_len += 4;
    v->prefix[v->prefix_len++] = 2;
    memcpy(v->prefix + v->prefix_len, "eu", 2);   v->prefix_len += 2;
    /* The stored extent is independently known from the fixture's own prefix
     * ("live" + "eu"), not read back from the entry. */
    v->prefix_buf_len = 6;
    v->recv_buf = recv_buf;
    v->recv_cap = recv_cap;
    v->recv_len = req_len;
    if (req_len && req_len <= NSO_BUF_MAX) memcpy(v->recv, req, req_len);
}

static moq_result_t nss_feed(moq_session_t *s, void *ctx, moq_stream_ref_t ref,
                             bool fin)
{
    nss_ctx_t *c = (nss_ctx_t *)ctx;
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live"),
                            MOQ_BYTES_LITERAL("eu") };
    moq_namespace_t ns = { parts, 2 };
    moq_d18_msg_params_t mp;
    memset(&mp, 0, sizeof(mp));
    mp.auth_token_count = 1;
    mp.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_USE_VALUE;
    mp.auth_tokens[0].token_type = 7;
    mp.auth_tokens[0].token_value = MOQ_BYTES_LITERAL("nsstok");
    uint8_t m[192];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, m, sizeof(m));
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_d18_encode_subscribe_namespace(&w, c->request_id, &ns, &mp),
        (int)MOQ_OK);
    /* Retain exactly what went on the wire so the declared record owns the
     * bytes the entry must hold, rather than reading them back from it. */
    c->last_req_len = moq_buf_writer_offset(&w);
    memcpy(c->last_req, m, c->last_req_len);
    return moq_session_on_bidi_stream_bytes(s, ref, m,
                                            c->last_req_len, fin, 1);
}

static moq_result_t nss_app_reject(moq_session_t *s, void *ctx, uint64_t handle)
{
    (void)ctx;
    moq_ns_sub_handle_t h;
    h._opaque = handle;
    moq_reject_ns_sub_cfg_t rj;
    moq_reject_ns_sub_cfg_init(&rj);
    rj.error_code = MOQ_REQUEST_ERROR_NOT_SUPPORTED;
    return moq_session_reject_ns_sub(s, h, &rj, 1);
}

static void nss_hook_capture(const moq_session_t *s, void *vctx, void *vst)
{
    nss_ctx_t *c = (nss_ctx_t *)vctx;
    nss_hook_state_t *st = (nss_hook_state_t *)vst;
    memset(st, 0, sizeof(*st));
    st->busy = ns_busy_count(s);
    const moq_ns_sub_entry_t *e = ns_by_ref(s, c->ref);
    if (!e) return;
    st->present = 1;
    st->handle = e->handle._opaque;
    st->stream_ref = e->stream_ref._v;
    st->pending_fin = e->pending_fin;
    st->handoff_fin = e->handoff_fin_pending;
    st->state = (int)e->state;
    st->generation = e->generation;
}

static int nss_hook_check(const moq_session_t *s, void *vctx, const void *vst)
{
    nss_hook_state_t now;
    nss_hook_capture(s, vctx, &now);
    const nss_hook_state_t *want = (const nss_hook_state_t *)vst;
    int bad = 0;
    if (now.present != want->present) bad++;
    if (now.handle != want->handle) bad++;
    if (now.stream_ref != want->stream_ref) bad++;
    if (now.pending_fin != want->pending_fin) bad++;
    if (now.handoff_fin != want->handoff_fin) bad++;
    if (now.state != want->state) bad++;
    if (now.generation != want->generation) bad++;
    if (now.busy != want->busy) bad++;
    if (bad)
        TXS_DIAG("TXN nss hooks: present %d handle %llu ref %llu fin %d "
                 "handoff %d state %d gen %u busy %d, expected "
                 "%d/%llu/%llu/%d/%d/%d/%u/%d\n",
                 now.present, (unsigned long long)now.handle,
                 (unsigned long long)now.stream_ref, now.pending_fin,
                 now.handoff_fin, now.state, now.generation, now.busy,
                 want->present, (unsigned long long)want->handle,
                 (unsigned long long)want->stream_ref, want->pending_fin,
                 want->handoff_fin, want->state, want->generation, want->busy);
    return bad;
}

static bool nss_norm_event(const moq_event_t *ev, void *vctx,
                           txs_norm_vec_t *out)
{
    nss_ctx_t *c = (nss_ctx_t *)vctx;
    txs_img_t im;
    txs_img_init(&im);
    switch (ev->kind) {
    case MOQ_EVENT_NS_SUB_REQUEST: {
        const moq_ns_sub_request_event_t *rq = &ev->u.ns_sub_request;
        c->last_handle = rq->handle._opaque;
        txs_img_u64(&im, rq->handle._opaque);
        txs_img_u64(&im, (uint64_t)rq->namespace_interest);
        txs_img_u64(&im, rq->forward ? 1u : 0u);
        if (!txs_img_parts(&im, "ns_sub prefix", rq->track_namespace_prefix.parts,
                           rq->track_namespace_prefix.count,
                           MOQ_DECODED_MAX_NAMESPACE_PARTS))
            return false;
        if (!txs_img_tokens(&im, "ns_sub tokens", rq->tokens,
                            rq->token_count, MOQ_DECODED_MAX_TOKENS))
            return false;
        break;
    }
    default:
        TXS_DIAG("TXN nss: unnormalized event kind %u\n", (unsigned)ev->kind);
        return false;
    }
    return txs_norm_append_img(out, ev->kind, &im);
}

/* The reciprocal close is an action of its own kind, so the family's action
 * normalizer must describe it as well as the bidi sends. */
static bool nss_norm_action(const moq_action_t *a, void *vctx,
                            txs_norm_vec_t *out)
{
    if (a->kind == MOQ_ACTION_CLOSE_BIDI_STREAM) {
        txs_img_t im;
        txs_img_init(&im);
        txs_img_u64(&im, a->u.close_bidi_stream.stream_ref._v);
        return txs_norm_append_img(out, a->kind, &im);
    }
    return pub_norm_action(a, vctx, out);
}

static void want_nss_request(txs_norm_vec_t *v, uint64_t handle)
{
    txs_img_t im;
    txs_img_init(&im);
    txs_img_u64(&im, handle);
    /* Draft-18 supplies both as fixed values. */
    txs_img_u64(&im, (uint64_t)MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE);
    txs_img_u64(&im, 1);                    /* forward */
    txs_img_u64(&im, 2);
    txs_img_bytes(&im, (const uint8_t *)"live", 4);
    txs_img_bytes(&im, (const uint8_t *)"eu", 2);
    txs_img_u64(&im, 1);
    txs_img_u64(&im, 7);
    txs_img_bytes(&im, (const uint8_t *)"nsstok", 6);
    MOQ_TEST_CHECK(txs_norm_append_img(v, MOQ_EVENT_NS_SUB_REQUEST, &im));
}

static void want_close_bidi(txs_norm_vec_t *v, moq_stream_ref_t ref)
{
    txs_img_t im;
    txs_img_init(&im);
    txs_img_u64(&im, ref._v);
    MOQ_TEST_CHECK(txs_norm_append_img(v, MOQ_ACTION_CLOSE_BIDI_STREAM, &im));
}

static nss_ctx_t nss_ctx = { 0, { 1 }, 0, { 0 }, 0 };

static fin_owner_case_t nss_family(void)
{
    fin_owner_case_t f;
    memset(&f, 0, sizeof(f));
    f.name = "subscribe-namespace";
    f.ctx = &nss_ctx;
    f.caps = FIN_TERM_APP_REJECT;
    f.feed = nss_feed;
    /* The owner is namespace-indexed, which the shared capture reports as
     * TXS_OWNER_NS; the descriptor's kind names the request family it came
     * from. */
    f.owner_kind = MOQ_REQ_NAMESPACE_SUB;
    f.owner_role = FIN_OWNER_ROLE_NA;
    f.owner_state = (int)MOQ_NS_SUB_PENDING_PUBLISHER;
    f.app_reject = nss_app_reject;
    f.hooks.ctx = &nss_ctx;
    f.hooks.capture = nss_hook_capture;
    f.hooks.check = nss_hook_check;
    f.hooks.normalize_event = nss_norm_event;
    f.hooks.normalize_action = nss_norm_action;
    /* The id-0 burn compares its rejection as a full envelope; it needs only
     * the action normalizer, and this is the one place it is available. */
    nss_hooks_for_actions.ctx = f.ctx;
    nss_hooks_for_actions.normalize_action = nss_norm_action;
    return f;
}

/* The owner, in the namespace index rather than the request registry. */
static int nss_owner_present_problems(const moq_session_t *s,
                                      moq_stream_ref_t ref,
                                      moq_request_kind_t want_kind,
                                      int want_role, int want_state)
{
    int bad = 0;
    /* The family is role-less and namespace-indexed: both facts are declared
     * by the descriptor and checked here rather than assumed. */
    if (want_kind != MOQ_REQ_NAMESPACE_SUB) bad++;
    if (want_role != FIN_OWNER_ROLE_NA) bad++;
    const moq_ns_sub_entry_t *e = ns_by_ref(s, ref);
    if (!e) return bad + 1;
    if ((int)e->state != want_state) bad++;
    if (e->stream_ref._v != ref._v) bad++;
    if (ns_busy_count(s) != 1) bad++;
    txs_owner_t o = txs_owner_capture(s, ref);
    if (o.kind != TXS_OWNER_NS) bad++;
    else if (o.slot != (int)(e - s->ns_subs)) bad++;
    if (bad)
        TXS_DIAG("TXN nss owner: state %d ref %llu busy %d owner-kind %d, "
                 "expected state %d ref %llu busy 1 owner-kind %d\n",
                 (int)e->state, (unsigned long long)e->stream_ref._v,
                 ns_busy_count(s), o.kind, want_state,
                 (unsigned long long)ref._v, TXS_OWNER_NS);
    return bad;
}

static void check_nss_retired(moq_session_t *s, moq_stream_ref_t ref)
{
    MOQ_TEST_CHECK_EQ_INT(ns_busy_count(s), 0);
    MOQ_TEST_CHECK(ns_by_ref(s, ref) == NULL);
    txs_owner_t o = txs_owner_capture(s, ref);
    MOQ_TEST_CHECK_EQ_INT(o.kind, TXS_OWNER_NONE);
}

/* Retire inbound request id 0 through this family's own public sequence -- a
 * complete SUBSCRIBE_NAMESPACE, its application rejection and the peer's FIN --
 * so every measured P6 owner carries a NONZERO request id. Without it the
 * ordinal-zero key of BACKLOG #252 sits in the graph and masks the topology
 * these rows assert. It encodes its own message rather than borrowing the
 * family feed, so the shared fixture context is never mutated, and nothing
 * about the burned exchange is measured. */
/* `rid` is the inbound ordinal to retire and `busy_after` the number of owners
 * expected to remain (nonzero when an unrelated owner is deliberately live).
 * The slot it frees also RETAINS its packed handle -- the free advances the
 * generation but does not clear the handle -- and the event-blocked row reads
 * that residue to build an equivalent one on its own target slot. */
static void nss_burn_request(moq_session_t *s, moq_stream_ref_t burn_ref,
                             uint64_t rid, int busy_after)
{
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("burn") };
    moq_namespace_t ns = { parts, 1 };
    moq_d18_msg_params_t mp;
    memset(&mp, 0, sizeof(mp));
    uint8_t m[192];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, m, sizeof(m));
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_d18_encode_subscribe_namespace(&w, rid, &ns, &mp), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, burn_ref, m,
                                              moq_buf_writer_offset(&w),
                                              false, 1),
        (int)MOQ_OK);
    uint64_t burn_handle = 0;
    {   /* EXACTLY one NS_SUB_REQUEST and nothing else. */
        int reqs = 0, other = 0;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_NS_SUB_REQUEST) {
                reqs++;
                burn_handle = ev.u.ns_sub_request.handle._opaque;
            } else other++;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(reqs, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
    }
    MOQ_TEST_CHECK(burn_handle != 0);
    {
        moq_ns_sub_handle_t h;
        h._opaque = burn_handle;
        moq_reject_ns_sub_cfg_t rj;
        moq_reject_ns_sub_cfg_init(&rj);
        rj.error_code = MOQ_REQUEST_ERROR_NOT_SUPPORTED;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_reject_ns_sub(s, h, &rj, 1),
                              (int)MOQ_OK);
    }
    {   /* Zero events at the rejection point, counted explicitly rather than
         * through a normalizer that would have to classify an unexpected kind. */
        int evs = 0;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            evs++;
            fprintf(stderr, "FAIL: %s:%d: unexpected burn event kind %u\n",
                    __FILE__, __LINE__, (unsigned)ev.kind);
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(evs, 0);
    }
    {   /* EXACTLY one REQUEST_ERROR(NOT_SUPPORTED, retry 0, empty reason) with
         * FIN on the burn ref, compared as a FULL ENVELOPE -- kind and ref
         * alone would accept any payload -- and nothing else queued. */
        txs_norm_vec_t acts, wacts;
        txs_norm_init(&acts);
        txs_norm_init(&wacts);
        collect_actions(s, &nss_hooks_for_actions, &acts);
        want_reject_error_code(&wacts, burn_ref,
                               (uint64_t)MOQ_REQUEST_ERROR_NOT_SUPPORTED);
        failures += txs_norm_equals(&acts, &wacts, "nss burn rejection");
        txs_norm_free(&acts);
        txs_norm_free(&wacts);
    }
    /* One drain reference is owed while the peer half is open. */
    MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, (size_t)1);
    /* The late FIN releases exactly that reference and produces no output. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, burn_ref, NULL, 0, true, 1),
        (int)MOQ_OK);
    {
        int any = 0;
        moq_event_t ev;
        moq_action_t a;
        while (moq_session_poll_events(s, &ev, 1) > 0) { any++; moq_event_cleanup(&ev); }
        while (moq_session_poll_actions(s, &a, 1) > 0) { any++; moq_action_cleanup(&a); }
        MOQ_TEST_CHECK_EQ_INT(any, 0);
    }
    MOQ_TEST_CHECK_EQ_INT(ns_busy_count(s), busy_after);
    MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, 0);
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
    /* No edge of the burned request survives to be measured against. */
    {
        og_graph_t g;
        og_capture(s, &g);
        failures += og_check_integrity(&g, "nss burn retired");
        failures += og_check_no_edge(&g, OG_DOM_REQ_RID, rid, "nss burn retired");
        failures += og_check_no_edge(&g, OG_DOM_NS_REF, burn_ref._v,
                                     "nss burn retired");
        failures += og_check_no_edge(&g, OG_DOM_REQ_STREAMREF, burn_ref._v,
                                     "nss burn retired");
    }
}

/* Permanent quiet self-checks for the namespace-sub inventory's refusal
 * branches. Each perturbs one field of a live owner, requires an INCOMPARABLE
 * capture AND an unequal comparison, and restores the field. Every perturbation
 * is itself safe -- nulled pointers, an offset that stays INSIDE the owned
 * buffer, and lengths that only shrink or grow the declared extent. */
static void nso_selfcheck_invalid(moq_session_t *s, int slot,
                                  const nso_snap_t *valid, const char *what)
{
    nso_snap_t got;
    nso_read(s, slot, &got);
    if (!got.invalid) {
        fprintf(stderr, "FAIL: %s:%d: nso_read accepted %s\n",
                __FILE__, __LINE__, what);
        failures++;
        return;
    }
    txs_quiet = 1;
    int diff = nso_diff(&got, valid, what);
    txs_quiet = 0;
    if (diff == 0) {
        fprintf(stderr, "FAIL: %s:%d: nso_diff compared equal on %s\n",
                __FILE__, __LINE__, what);
        failures++;
    }
}

static void nss_inventory_selfchecks(const fin_owner_case_t *f)
{
    moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
    nss_ctx_t *c = (nss_ctx_t *)f->ctx;
    uint64_t saved_id = c->request_id;
    nss_burn_request(s, moq_stream_ref_from_u64(c->ref._v + 8), 0, 0);
    c->request_id = 2;
    c->last_handle = 0;
    int slot = nss_declare_free_slot(s);
    MOQ_TEST_CHECK(slot >= 0);
    if (slot < 0) { c->request_id = saved_id; moq_session_destroy(s); return; }
    MOQ_TEST_CHECK_EQ_INT((int)f->feed(s, f->ctx, c->ref, false), (int)MOQ_OK);
    {
        txs_norm_vec_t d;
        txs_norm_init(&d);
        collect_events(s, &f->hooks, &d);
        txs_norm_free(&d);
    }
    nso_snap_t valid;
    nso_read(s, slot, &valid);
    MOQ_TEST_CHECK(valid.invalid == 0);
    moq_ns_sub_entry_t *e = &s->ns_subs[slot];

    /* Both out-of-range directions, through the comparator as well. */
    nso_selfcheck_invalid(s, (int)s->ns_sub_cap, &valid, "ns slot past the pool");
    nso_selfcheck_invalid(s, -1, &valid, "ns negative slot");

    size_t         save_pc  = e->prefix_count;
    moq_bytes_t   *save_pp  = e->prefix_parts;
    uint8_t       *save_pb  = e->prefix_buf;
    size_t         save_pbl = e->prefix_buf_len;
    const uint8_t *save_d0  = save_pp ? save_pp[0].data : NULL;
    const uint8_t *save_d1  = (save_pp && save_pc > 1) ? save_pp[1].data : NULL;
    size_t         save_rl  = e->recv_len;
    uint8_t       *save_rb  = e->recv_buf;

    e->prefix_count = NSO_MAX_PARTS + 1;
    nso_selfcheck_invalid(s, slot, &valid, "ns excessive prefix count");
    e->prefix_count = save_pc;

    e->prefix_parts = NULL;
    nso_selfcheck_invalid(s, slot, &valid, "ns NULL parts with nonzero count");
    e->prefix_parts = save_pp;

    if (save_pp && save_pc > 0) {
        e->prefix_parts[0].data = NULL;
        nso_selfcheck_invalid(s, slot, &valid, "ns NULL nonempty part data");
        e->prefix_parts[0].data = save_d0;
    }

    e->prefix_buf = NULL;
    nso_selfcheck_invalid(s, slot, &valid, "ns NULL prefix buffer");
    e->prefix_buf = save_pb;

    if (save_pp && save_pc > 1 && save_pb) {
        e->prefix_parts[1].data = save_pb;      /* offset 0, still in-bounds */
        nso_selfcheck_invalid(s, slot, &valid, "ns wrong in-bounds offset");
        e->prefix_parts[1].data = save_d1;
    }

    e->prefix_buf_len = 1;
    nso_selfcheck_invalid(s, slot, &valid, "ns prefix extent too short");
    e->prefix_buf_len = save_pbl + 1;
    nso_selfcheck_invalid(s, slot, &valid, "ns prefix extent too long");
    e->prefix_buf_len = save_pbl;

    e->recv_len = NSO_BUF_MAX + 1;
    nso_selfcheck_invalid(s, slot, &valid, "ns oversized retained length");
    e->recv_len = save_rl;

    e->recv_len = 4;
    e->recv_buf = NULL;
    nso_selfcheck_invalid(s, slot, &valid, "ns retained length, NULL buffer");
    e->recv_buf = save_rb;
    e->recv_len = save_rl;

    /* Every perturbation restored. */
    nso_snap_t back;
    nso_read(s, slot, &back);
    failures += nso_diff(&back, &valid, "ns inventory restored");
    c->request_id = saved_id;
    moq_session_destroy(s);
}

/* The complete retired postcondition for a P6 owner: production lookups, every
 * graph domain, no owner-targeting edge, and no output. Used after each
 * recovery branch AND after its second empty retry, so exactly-once is proven
 * rather than once-observed. */
static void nss_check_fully_retired(moq_session_t *s, const fin_owner_case_t *f,
                                    moq_stream_ref_t ref, int slot,
                                    uint64_t rid, const drain_snap_t *ring,
                                    const char *what)
{
    check_nss_retired(s, ref);
    og_graph_t g;
    og_capture(s, &g);
    failures += og_check_integrity(&g, what);
    failures += og_check_no_edge(&g, OG_DOM_NS_REF, ref._v, what);
    failures += og_check_no_edge(&g, OG_DOM_REQ_RID, rid, what);
    failures += og_check_no_edge(&g, OG_DOM_REQ_STREAMREF, ref._v, what);
    failures += og_check_owner_unreferenced(&g, (int)MOQ_REQ_NAMESPACE_SUB,
                                            slot, what);
    drain_snap_t now;
    drain_snap(s, &now);
    failures += drain_multiset_equals(&now, ring, what);
    check_no_output(s, &f->hooks);
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
}

/* PROFILE BOUNDARY. The marker is installed by the draft-18 request-stream
 * handoff ONLY. `ns_sub_process_recving_publisher()` is shared: the draft-16
 * bidi router reaches the same commit by creating the entry directly and
 * driving it through `handle_bidi_stream_bytes()`, which is NOT a handoff and
 * must never leave transitional FIN ownership behind.
 *
 * A full draft-16 session is not needed to discriminate that, and building one
 * here would test the setup rather than the boundary: driving the same shared
 * commit through the DIRECT creation route with a wire FIN observes the marker
 * at the one point it is visible. The action queue is filled first so the
 * reciprocal close refuses and the committed entry survives to be read. */
static void run_nss_direct_route_control(const fin_owner_case_t *f)
{
    moq_session_t *s = make_session_full(MOQ_PERSPECTIVE_SERVER, 0, 1);
    nss_ctx_t *c = (nss_ctx_t *)f->ctx;
    uint64_t saved_id = c->request_id;
    moq_stream_ref_t ref = moq_stream_ref_from_u64(c->ref._v + 24);
    moq_stream_ref_t blocker = moq_stream_ref_from_u64(0x99);

    /* Encode the request, then create the owner through the DIRECT route --
     * never through the draft-18 request-stream handoff. */
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("direct") };
    moq_namespace_t ns = { parts, 1 };
    moq_d18_msg_params_t mp;
    memset(&mp, 0, sizeof(mp));
    uint8_t m[192];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, m, sizeof(m));
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_d18_encode_subscribe_namespace(&w, 0, &ns, &mp), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(
        (int)ns_sub_on_new_bidi(s, ref, m, moq_buf_writer_offset(&w)),
        (int)MOQ_OK);
    int slot = (int)moq_index_find(s->idx_ns_by_ref, s->idx_ns_mask, ref._v);
    MOQ_TEST_CHECK(slot >= 0);
    if (slot < 0) { c->request_id = saved_id; moq_session_destroy(s); return; }
    /* Nothing is owed yet, and nothing was installed by the creation. */
    MOQ_TEST_CHECK_EQ_INT(s->ns_subs[slot].handoff_fin_pending, 0);

    MOQ_TEST_CHECK_EQ_INT((int)queue_close_bidi(s, blocker), (int)MOQ_OK);
    MOQ_TEST_CHECK(action_queue_full(s));

    /* The wire FIN drives the shared commit through the direct route. The
     * reciprocal close refuses on action capacity, so the committed entry is
     * still readable -- and it must carry NO transitional FIN ownership: this
     * call's FIN is the wire's, not a handed-over one. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)handle_bidi_stream_bytes(s, ref, NULL, 0, true),
        (int)MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK_EQ_INT((int)s->ns_subs[slot].state,
                          (int)MOQ_NS_SUB_PENDING_PUBLISHER);
    MOQ_TEST_CHECK_EQ_INT(s->ns_subs[slot].handoff_fin_pending, 0);
    MOQ_TEST_CHECK_EQ_INT(s->ns_subs[slot].pending_fin, 0);
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);

    c->request_id = saved_id;
    moq_session_destroy(s);
}

static void run_nss_case(const fin_owner_case_t *f, bool fin_in_request,
                         bool fill_ring, bool reject_now)
{
    moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
    nss_ctx_t *c = (nss_ctx_t *)f->ctx;
    moq_stream_ref_t ref = c->ref;
    uint64_t saved_id = c->request_id;
    nss_burn_request(s, moq_stream_ref_from_u64(ref._v + 8), 0, 0);
    c->request_id = 2;               /* the next legal inbound ordinal */
    uint64_t rid = c->request_id;
    c->last_handle = 0;

    /* Identity declared from pool state BEFORE ingress. */
    int slot = nss_declare_free_slot(s);
    MOQ_TEST_CHECK(slot >= 0);
    if (slot < 0) { c->request_id = saved_id; moq_session_destroy(s); return; }
    uint32_t gen = s->ns_subs[slot].generation | 1u;
    uint64_t handle = moq_handle_pack(MOQ_HANDLE_POOL_NAMESPACE_SUB,
                                      s->session_tag, gen, (uint32_t)slot);
    /* Co-allocated and preserved across reuse: declared from the FREE slot. */
    const void *slot_recv_buf = (const void *)s->ns_subs[slot].recv_buf;
    size_t slot_recv_cap = s->ns_subs[slot].recv_cap;
    txs_snapshot_t pre_feed;
    txs_capture(s, &ref, 1, &pre_feed);

    MOQ_TEST_CHECK_EQ_INT((int)f->feed(s, f->ctx, ref, fin_in_request),
                          (int)MOQ_OK);
    txs_norm_vec_t got, want;
    txs_norm_init(&got);
    txs_norm_init(&want);
    collect_events(s, &f->hooks, &got);
    want_nss_request(&want, c->last_handle);
    failures += txs_norm_equals(&got, &want, f->name);
    txs_norm_free(&got);
    txs_norm_free(&want);

    if (fin_in_request) {
        /* The owner is retired inside the feed, so the surfaced handle is
         * checked against the PRE-DERIVED one -- `c->last_handle` must not
         * authorize its own expectation. */
        MOQ_TEST_CHECK_EQ_U64(c->last_handle, handle);
        /* The peer's graceful cancel is answered by closing our send half,
         * and the entry is reclaimed. */
        txs_norm_vec_t acts, wacts;
        txs_norm_init(&acts);
        txs_norm_init(&wacts);
        collect_actions(s, &f->hooks, &acts);
        want_close_bidi(&wacts, ref);
        failures += txs_norm_equals(&acts, &wacts, f->name);
        txs_norm_free(&acts);
        txs_norm_free(&wacts);
        check_nss_retired(s, ref);
        {   /* Nothing of the retired owner survives in any domain. */
            og_graph_t g;
            og_capture(s, &g);
            failures += og_check_integrity(&g, "nss immediate retired");
            failures += og_check_no_edge(&g, OG_DOM_NS_REF, ref._v,
                                         "nss immediate retired");
            failures += og_check_no_edge(&g, OG_DOM_REQ_RID, rid,
                                         "nss immediate retired");
            failures += og_check_no_edge(&g, OG_DOM_REQ_STREAMREF, ref._v,
                                         "nss immediate retired");
            failures += og_check_owner_unreferenced(&g,
                (int)MOQ_REQ_NAMESPACE_SUB, slot, "nss immediate retired");
        }
        MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, 0);
        check_no_output(s, &f->hooks);
        c->request_id = saved_id;
        moq_session_destroy(s);
        return;
    }

    failures += nss_owner_present_problems(s, ref, f->owner_kind, f->owner_role, f->owner_state);
    nss_hook_state_t owner_before;
    f->hooks.capture(s, f->ctx, &owner_before);
    MOQ_TEST_CHECK_EQ_U64(owner_before.handle, handle);
    MOQ_TEST_CHECK_EQ_U64(c->last_handle, handle);
    MOQ_TEST_CHECK_EQ_INT(owner_before.pending_fin, 0);
    MOQ_TEST_CHECK_EQ_INT(owner_before.handoff_fin, 0);
    check_no_output(s, &f->hooks);

    /* The whole declared owner inventory, the session delta of the handoff, and
     * the owner's ABSOLUTE edge set -- validated before any graph derived from
     * it becomes a conservation baseline. */
    nso_snap_t nso_want, nso_have;
    nss_declare(&nso_want, slot, gen, handle, rid, ref._v, 0,
                c->last_req, c->last_req_len, slot_recv_buf, slot_recv_cap);
    nso_read(s, slot, &nso_have);
    failures += nso_diff(&nso_have, &nso_want, "nss committed owner");
    {
        txs_snapshot_t observed, want_feed = pre_feed;
        txs_capture(s, &ref, 1, &observed);
        want_feed.event_scratch_len = observed.event_scratch_len;  /* adopted */
        want_feed.owners[0].kind = TXS_OWNER_NS;
        want_feed.owners[0].slot = slot;
        want_feed.owners[0].state = (int)MOQ_NS_SUB_PENDING_PUBLISHER;
        want_feed.owners[0].generation = gen;
        want_feed.owners[0].invalid = 0;
        failures += txs_check_eq(s, &ref, 1, &want_feed, "nss handoff delta");
    }
    og_graph_t live_graph;
    og_capture(s, &live_graph);
    failures += og_check_integrity(&live_graph, "nss live");
    failures += nss_check_owner_edges(&live_graph, slot, ref, rid, "nss live");

    if (fill_ring) fill_drain_ring(s);
    drain_snap_t ring_before;
    drain_snap(s, &ring_before);
    if (!reject_now) {
        c->request_id = saved_id;
        moq_session_destroy(s);
        return;
    }

    moq_result_t want_rc = fill_ring ? MOQ_ERR_WOULD_BLOCK : MOQ_OK;
    txs_snapshot_t before;
    txs_capture(s, &ref, 1, &before);
    expect_after_call_prepare(&before);
    MOQ_TEST_CHECK_EQ_INT((int)f->app_reject(s, f->ctx, c->last_handle),
                          (int)want_rc);

    if (want_rc == MOQ_ERR_WOULD_BLOCK) {
        failures += txs_check_eq(s, &ref, 1, &before, f->name);
        failures += f->hooks.check(s, f->ctx, &owner_before);
        failures += nss_owner_present_problems(
            s, ref, f->owner_kind, f->owner_role, f->owner_state);
        drain_snap_t now;
        drain_snap(s, &now);
        failures += drain_multiset_equals(&now, &ring_before, "nss blocked");
        nso_read(s, slot, &nso_have);
        failures += nso_diff(&nso_have, &nso_want, "nss blocked owner");
        og_graph_t g;
        og_capture(s, &g);
        failures += og_check_integrity(&g, "nss blocked");
        failures += nss_check_owner_edges(&g, slot, ref, rid, "nss blocked");
        failures += og_check_same_topology(&g, &live_graph, "nss blocked");
        check_no_output(s, &f->hooks);
        c->request_id = saved_id;
        moq_session_destroy(s);
        return;
    }

    drain_snap_t now, want_ring;
    drain_snap(s, &now);
    drain_snap_plus(&ring_before, ref, MOQ_DRAIN_NORMAL, &want_ring);
    failures += drain_multiset_equals(&now, &want_ring, "nss granted");
    check_nss_retired(s, ref);
    check_reject_only_output(s, &f->hooks, ref, f->name);

    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
        (int)MOQ_OK);
    check_no_output(s, &f->hooks);
    drain_snap_t after;
    drain_snap(s, &after);
    failures += drain_multiset_equals(&after, &ring_before, "nss released");
    check_nss_retired(s, ref);
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
    c->request_id = saved_id;
    moq_session_destroy(s);
}

/* Same-call FIN with no action slot for the reciprocal close. The pressure is
 * applied directly -- a benign filler close on an unrelated stream -- so the
 * fixture creates no second protocol owner and cannot terminate the session.
 * `then_reject` answers inside the blocked window instead of retrying. */
static void run_nss_action_blocked(const fin_owner_case_t *f, bool then_reject)
{
    moq_session_t *s = make_session_full(MOQ_PERSPECTIVE_SERVER, 0, 1);
    nss_ctx_t *c = (nss_ctx_t *)f->ctx;
    moq_stream_ref_t ref = c->ref;
    moq_stream_ref_t blocker = moq_stream_ref_from_u64(0x77);
    uint64_t saved_id = c->request_id;

    /* #252 would destroy the pending request-id edge at ordinal zero, so the
     * measured owner uses a nonzero id and its topology asserts absolutely. */
    nss_burn_request(s, moq_stream_ref_from_u64(ref._v + 8), 0, 0);
    c->request_id = 2;
    uint64_t rid = c->request_id;
    c->last_handle = 0;

    int slot = nss_declare_free_slot(s);
    MOQ_TEST_CHECK(slot >= 0);
    if (slot < 0) { c->request_id = saved_id; moq_session_destroy(s); return; }
    uint32_t gen = s->ns_subs[slot].generation | 1u;
    uint64_t handle = moq_handle_pack(MOQ_HANDLE_POOL_NAMESPACE_SUB,
                                      s->session_tag, gen, (uint32_t)slot);
    const void *slot_recv_buf = (const void *)s->ns_subs[slot].recv_buf;
    size_t slot_recv_cap = s->ns_subs[slot].recv_cap;

    MOQ_TEST_CHECK_EQ_INT((int)queue_close_bidi(s, blocker), (int)MOQ_OK);
    MOQ_TEST_CHECK(action_queue_full(s));
    txs_snapshot_t pre_feed;
    txs_capture(s, &ref, 1, &pre_feed);

    /* The ring as it stands BEFORE the feed: a blocked call that wrongly took
     * a reference must not be able to install itself as the baseline. */
    drain_snap_t ring_pre_feed;
    drain_snap(s, &ring_pre_feed);

    /* No room for the reciprocal close. */
    MOQ_TEST_CHECK_EQ_INT((int)f->feed(s, f->ctx, ref, true),
                          (int)MOQ_ERR_WOULD_BLOCK);
    txs_norm_vec_t got, want;
    txs_norm_init(&got);
    txs_norm_init(&want);
    collect_events(s, &f->hooks, &got);
    want_nss_request(&want, c->last_handle);
    failures += txs_norm_equals(&got, &want, f->name);
    txs_norm_free(&got);
    txs_norm_free(&want);

    /* Nothing was drained by the blocked call itself. */
    {
        drain_snap_t after_feed;
        drain_snap(s, &after_feed);
        failures += drain_multiset_equals(&after_feed, &ring_pre_feed,
                                          "nss blocked feed");
    }

    /* The owner survives, and the observed close is held by the new marker --
     * which is distinct from pending_fin, so that field stays clear. */
    failures += nss_owner_present_problems(s, ref, f->owner_kind, f->owner_role,
                                           f->owner_state);
    nss_hook_state_t owner_before;
    f->hooks.capture(s, f->ctx, &owner_before);
    MOQ_TEST_CHECK_EQ_U64(owner_before.handle, handle);
    MOQ_TEST_CHECK_EQ_U64(c->last_handle, handle);
    MOQ_TEST_CHECK_EQ_INT(owner_before.pending_fin, 0);
    /* The observed close is transitional ownership, NOT the durable latch. */
    MOQ_TEST_CHECK_EQ_INT(owner_before.handoff_fin, 1);

    /* The declared owner inventory, the declared session delta of the refused
     * handoff, and the owner's ABSOLUTE edge set -- validated before any graph
     * derived from it becomes a conservation baseline. */
    nso_snap_t nso_want, nso_have;
    nss_declare(&nso_want, slot, gen, handle, rid, ref._v, 1,
                c->last_req, c->last_req_len, slot_recv_buf, slot_recv_cap);
    nso_read(s, slot, &nso_have);
    failures += nso_diff(&nso_have, &nso_want, "nss action-blocked owner");
    {
        txs_snapshot_t observed, want_feed = pre_feed;
        txs_capture(s, &ref, 1, &observed);
        want_feed.event_scratch_len = observed.event_scratch_len;  /* adopted */
        /* `pre_feed` was captured AFTER the blocker was queued, so the unchanged
         * action depth is already DECLARED by that snapshot -- adopting the
         * observed value would have made the one thing this row cares about
         * unfalsifiable. */
        want_feed.owners[0].kind = TXS_OWNER_NS;
        want_feed.owners[0].slot = slot;
        want_feed.owners[0].state = (int)MOQ_NS_SUB_PENDING_PUBLISHER;
        want_feed.owners[0].generation = gen;
        want_feed.owners[0].invalid = 0;
        failures += txs_check_eq(s, &ref, 1, &want_feed,
                                 "nss action-blocked delta");
    }
    nsc_snap_t ab_alloc;
    nsc_read(s, slot, &ab_alloc);           /* after the absolute check above */
    og_graph_t live_graph;
    og_capture(s, &live_graph);
    failures += og_check_integrity(&live_graph, "nss action-blocked live");
    failures += nss_check_owner_edges(&live_graph, slot, ref, rid,
                                      "nss action-blocked live");

    /* Exactly the filler, on the unrelated stream: polling it frees the slot,
     * and the ref proves which close was queued. */
    txs_norm_vec_t acts, wacts;
    txs_norm_init(&acts);
    txs_norm_init(&wacts);
    collect_actions(s, &f->hooks, &acts);
    want_close_bidi(&wacts, blocker);
    failures += txs_norm_equals(&acts, &wacts, f->name);
    txs_norm_free(&acts);
    txs_norm_free(&wacts);

    /* After the filler release, before the retry: the owner and its exact
     * topology are unchanged -- the release must not have disturbed them. */
    nso_read(s, slot, &nso_have);
    failures += nso_diff(&nso_have, &nso_want, "nss action-blocked released");
    {
        nsc_snap_t now_alloc;
        nsc_read(s, slot, &now_alloc);
        failures += nsc_same(&now_alloc, &ab_alloc,
                             "nss action-blocked alloc");
    }
    {
        og_graph_t g;
        og_capture(s, &g);
        failures += og_check_integrity(&g, "nss action-blocked released");
        failures += nss_check_owner_edges(&g, slot, ref, rid,
                                          "nss action-blocked released");
        failures += og_check_same_topology(&g, &live_graph,
                                           "nss action-blocked released");
    }

    if (!then_reject) {
        /* An empty refeed completes the deferred close exactly once, for the
         * request's own stream. The expectation is the PRE-FEED ring. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, false, 1),
            (int)MOQ_OK);
        txs_norm_vec_t a2, w2;
        txs_norm_init(&a2);
        txs_norm_init(&w2);
        collect_actions(s, &f->hooks, &a2);
        want_close_bidi(&w2, ref);
        failures += txs_norm_equals(&a2, &w2, f->name);
        txs_norm_free(&a2);
        txs_norm_free(&w2);
        nss_check_fully_retired(s, f, ref, slot, rid, &ring_pre_feed,
                                "nss retry retired");
        /* A second empty retry resurrects nothing and queues no duplicate. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, false, 1),
            (int)MOQ_OK);
        nss_check_fully_retired(s, f, ref, slot, rid, &ring_pre_feed,
                                "nss retry idempotent");
        c->request_id = saved_id;
        moq_session_destroy(s);
        return;
    }

    /* The application answers inside the blocked window, against an exhausted
     * ring: the observed close means no drain reference is needed. */
    fill_drain_ring(s);
    drain_snap_t ring_before;
    drain_snap(s, &ring_before);
    MOQ_TEST_CHECK_EQ_INT((int)f->app_reject(s, f->ctx, c->last_handle),
                          (int)MOQ_OK);
    drain_snap_t now;
    drain_snap(s, &now);
    failures += drain_multiset_equals(&now, &ring_before, "nss window reject");
    check_reject_only_output(s, &f->hooks, ref, f->name);
    /* The exact rejection output is collected above; retirement is then proven
     * in every domain, and repeated after a second empty retry. */
    nss_check_fully_retired(s, f, ref, slot, rid, &ring_before,
                            "nss window reject retired");
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, false, 1),
        (int)MOQ_OK);
    nss_check_fully_retired(s, f, ref, slot, rid, &ring_before,
                            "nss window reject idempotent");
    c->request_id = saved_id;
    moq_session_destroy(s);
}

/* Same-call FIN against a full drain ring -- the paired half of the no-FIN
 * discriminator. The reciprocal close needs no drain reference, so an
 * exhausted ring cannot refuse it. */
static void run_nss_fin_full_ring(const fin_owner_case_t *f)
{
    moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
    nss_ctx_t *c = (nss_ctx_t *)f->ctx;
    moq_stream_ref_t ref = c->ref;
    uint64_t saved_id = c->request_id;
    nss_burn_request(s, moq_stream_ref_from_u64(ref._v + 8), 0, 0);
    c->request_id = 2;
    uint64_t rid = c->request_id;
    c->last_handle = 0;
    int slot = nss_declare_free_slot(s);
    MOQ_TEST_CHECK(slot >= 0);
    if (slot < 0) { c->request_id = saved_id; moq_session_destroy(s); return; }
    uint32_t gen = s->ns_subs[slot].generation | 1u;
    uint64_t handle = moq_handle_pack(MOQ_HANDLE_POOL_NAMESPACE_SUB,
                                      s->session_tag, gen, (uint32_t)slot);
    fill_drain_ring(s);
    drain_snap_t ring_before;
    drain_snap(s, &ring_before);

    MOQ_TEST_CHECK_EQ_INT((int)f->feed(s, f->ctx, ref, true), (int)MOQ_OK);
    txs_norm_vec_t got, want;
    txs_norm_init(&got);
    txs_norm_init(&want);
    collect_events(s, &f->hooks, &got);
    want_nss_request(&want, c->last_handle);
    failures += txs_norm_equals(&got, &want, f->name);
    txs_norm_free(&got);
    txs_norm_free(&want);

    txs_norm_vec_t acts, wacts;
    txs_norm_init(&acts);
    txs_norm_init(&wacts);
    collect_actions(s, &f->hooks, &acts);
    want_close_bidi(&wacts, ref);
    failures += txs_norm_equals(&acts, &wacts, f->name);
    txs_norm_free(&acts);
    txs_norm_free(&wacts);

    drain_snap_t now;
    drain_snap(s, &now);
    failures += drain_multiset_equals(&now, &ring_before, "nss fin full ring");
    /* The owner is retired inside the feed, so the surfaced handle is checked
     * against the PRE-DERIVED one, and nothing of it survives in any domain. */
    MOQ_TEST_CHECK_EQ_U64(c->last_handle, handle);
    check_nss_retired(s, ref);
    {
        og_graph_t g;
        og_capture(s, &g);
        failures += og_check_integrity(&g, "nss full-ring retired");
        failures += og_check_no_edge(&g, OG_DOM_NS_REF, ref._v,
                                     "nss full-ring retired");
        failures += og_check_no_edge(&g, OG_DOM_REQ_RID, rid,
                                     "nss full-ring retired");
        failures += og_check_no_edge(&g, OG_DOM_REQ_STREAMREF, ref._v,
                                     "nss full-ring retired");
        failures += og_check_owner_unreferenced(&g, (int)MOQ_REQ_NAMESPACE_SUB,
                                                slot, "nss full-ring retired");
    }
    check_no_output(s, &f->hooks);
    c->request_id = saved_id;
    moq_session_destroy(s);
}

/* The PRE-COMMIT carrier: the event queue is full when the request arrives, so
 * `ns_sub_process_recving_publisher()` refuses BEFORE its commit -- yet the
 * draft-18 handoff still retires the generic staging subscription, because the
 * namespace-sub owner already owns the bidi and the request bytes. The FIN must
 * therefore reach that owner at the OWNERSHIP TRANSITION, not at the commit;
 * otherwise the empty re-feed commits with no FIN fact, queues no reciprocal
 * close and leaves the owner alive.
 *
 * The filler is a declared second SUBSCRIBE_NAMESPACE on its own ref: it is a
 * real, classified owner, so every count below names both. */
static void run_nss_event_blocked(const fin_owner_case_t *f)
{
    moq_session_t *s = make_session_caps(MOQ_PERSPECTIVE_SERVER, 1);
    nss_ctx_t *c = (nss_ctx_t *)f->ctx;
    uint64_t saved_id = c->request_id;
    moq_stream_ref_t saved_ref = c->ref;
    moq_stream_ref_t target = c->ref;
    moq_stream_ref_t filler = moq_stream_ref_from_u64(target._v + 16);

    /* The burn will take the first free slot. Everything the residue oracle
     * compares is derived from that slot's state BEFORE the burn, so the
     * product's own post-burn fields cannot define both sides of their
     * expectation -- in particular the claim that the free ADVANCED the
     * generation is only meaningful against a pre-derived value. */
    int burn_slot = nss_declare_free_slot(s);
    MOQ_TEST_CHECK(burn_slot >= 0);
    if (burn_slot < 0) { c->request_id = saved_id; moq_session_destroy(s);
                         return; }
    MOQ_TEST_CHECK_EQ_INT((int)s->ns_subs[burn_slot].state,
                          (int)MOQ_NS_SUB_FREE);
    uint32_t burn_pre_gen = s->ns_subs[burn_slot].generation;
    uint32_t burn_stale_gen = burn_pre_gen | 1u;        /* its live generation */
    uint32_t burn_free_gen = burn_stale_gen + 1u;       /* after the free */
    uint64_t burn_stale_handle =
        moq_handle_pack(MOQ_HANDLE_POOL_NAMESPACE_SUB, s->session_tag,
                        burn_stale_gen, (uint32_t)burn_slot);
    MOQ_TEST_CHECK(burn_stale_handle != 0);

    nss_burn_request(s, moq_stream_ref_from_u64(target._v + 8), 0, 0);

    /* The freed slot's residue, compared against those PRE-DERIVED values.
     * `ns_sub_free_entry()` advances the generation but does NOT clear
     * `handle`, so the slot is FREE while still carrying the packed handle of
     * the owner that has just gone. */
    MOQ_TEST_CHECK_EQ_INT((int)s->ns_subs[burn_slot].state,
                          (int)MOQ_NS_SUB_FREE);
    MOQ_TEST_CHECK_EQ_U64(s->ns_subs[burn_slot].generation, burn_free_gen);
    MOQ_TEST_CHECK_EQ_U64(s->ns_subs[burn_slot].handle._opaque,
                          burn_stale_handle);
    MOQ_TEST_CHECK_EQ_INT(ns_busy_count(s), 0);
    {
        og_graph_t g;
        og_capture(s, &g);
        failures += og_check_integrity(&g, "nss residue source");
        failures += og_check_owner_unreferenced(&g, (int)MOQ_REQ_NAMESPACE_SUB,
                                                burn_slot,
                                                "nss residue source");
    }

    /* The declared filler occupies the single event slot and is left unpolled,
     * so its handle is DERIVED before ingress rather than read from an event
     * this row must not drain. */
    int filler_slot = nss_declare_free_slot(s);
    MOQ_TEST_CHECK(filler_slot >= 0);
    if (filler_slot < 0) { c->request_id = saved_id; moq_session_destroy(s);
                           return; }
    uint32_t filler_gen = s->ns_subs[filler_slot].generation | 1u;
    uint64_t filler_handle = moq_handle_pack(
        MOQ_HANDLE_POOL_NAMESPACE_SUB, s->session_tag, filler_gen,
        (uint32_t)filler_slot);
    const void *filler_pre_buf = (const void *)s->ns_subs[filler_slot].recv_buf;
    size_t filler_pre_cap = s->ns_subs[filler_slot].recv_cap;
    uint8_t filler_req[192];
    size_t filler_req_len = 0;
    {   /* A DISJOINT prefix: the family feed's ("live","eu") would overlap the
         * target and be rejected under the independent-overlap rule instead of
         * blocking on event capacity, which is not what this row measures. */
        moq_bytes_t fp[] = { MOQ_BYTES_LITERAL("fill") };
        moq_namespace_t fns = { fp, 1 };
        moq_d18_msg_params_t fmp;
        memset(&fmp, 0, sizeof(fmp));
        uint8_t fm[192];
        moq_buf_writer_t fw;
        moq_buf_writer_init(&fw, fm, sizeof(fm));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_encode_subscribe_namespace(&fw, 2, &fns, &fmp),
            (int)MOQ_OK);
        filler_req_len = moq_buf_writer_offset(&fw);
        memcpy(filler_req, fm, filler_req_len);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, filler, fm,
                                                  filler_req_len, false, 1),
            (int)MOQ_OK);
    }
    MOQ_TEST_CHECK(event_queue_full(s));

    /* The target lands on the OTHER, virgin slot -- first-free had to leave the
     * filler where it is. Reproduce on it exactly the residue the burn proved
     * a real free leaves behind: the same free generation, and the stale handle
     * of the previous generation repacked for this slot. Fixture setup only --
     * the relationship was established through the real public lifecycle above,
     * and nothing about the target's own ingress, refusal, retry, output or
     * retirement is bypassed. */
    int slot = nss_declare_free_slot(s);
    MOQ_TEST_CHECK(slot >= 0);
    if (slot < 0) { c->ref = saved_ref; c->request_id = saved_id;
                    moq_session_destroy(s); return; }
    /* The allocation assumptions this construction rests on, asserted rather
     * than described: the filler DID reuse the burned slot, and the target is a
     * different, untouched slot still at its initial generation with no
     * handle. */
    MOQ_TEST_CHECK_EQ_INT(filler_slot, burn_slot);
    MOQ_TEST_CHECK(slot != filler_slot);
    MOQ_TEST_CHECK_EQ_INT((int)s->ns_subs[slot].state, (int)MOQ_NS_SUB_FREE);
    MOQ_TEST_CHECK_EQ_U64(s->ns_subs[slot].generation, 0);
    MOQ_TEST_CHECK_EQ_U64(s->ns_subs[slot].handle._opaque, 0);
    {
        int busy_before = ns_busy_count(s);
        size_t ev_before = s->event_tail - s->event_head;
        og_graph_t before_seed;
        og_capture(s, &before_seed);
        failures += og_check_integrity(&before_seed, "nss residue pre-seed");

        /* Exactly two fields, both PRE-DERIVED above. */
        s->ns_subs[slot].generation = burn_free_gen;
        s->ns_subs[slot].handle._opaque =
            moq_handle_pack(MOQ_HANDLE_POOL_NAMESPACE_SUB, s->session_tag,
                            burn_stale_gen, (uint32_t)slot);

        /* The seeding itself is inert: still FREE, no owner, no queue movement,
         * the whole topology conserved, and literally no edge to the target's
         * slot, ref or id in ANY of the three domains. */
        MOQ_TEST_CHECK_EQ_INT((int)s->ns_subs[slot].state,
                              (int)MOQ_NS_SUB_FREE);
        MOQ_TEST_CHECK_EQ_INT(ns_busy_count(s), busy_before);
        MOQ_TEST_CHECK_EQ_SIZE(s->event_tail - s->event_head, ev_before);
        og_graph_t g;
        og_capture(s, &g);
        failures += og_check_integrity(&g, "nss residue seeded");
        failures += og_check_same_topology(&g, &before_seed,
                                           "nss residue seeded");
        failures += og_check_owner_unreferenced(&g, (int)MOQ_REQ_NAMESPACE_SUB,
                                                slot, "nss residue seeded");
        failures += og_check_no_edge(&g, OG_DOM_NS_REF, target._v,
                                     "nss residue seeded");
        failures += og_check_no_edge(&g, OG_DOM_REQ_RID, 4,
                                     "nss residue seeded");
        failures += og_check_no_edge(&g, OG_DOM_REQ_STREAMREF, target._v,
                                     "nss residue seeded");
        failures += nss_check_owner_edges(&g, filler_slot, filler, 2,
                                          "nss residue seeded");
    }

    /* Pre-commit: `ns_sub_on_new_bidi()` mints nothing, so the refused
     * RECVING_PUBLISHER owner must still carry the slot's own STALE handle. */
    uint64_t target_pre_handle = s->ns_subs[slot].handle._opaque;
    uint32_t gen = s->ns_subs[slot].generation | 1u;
    /* Commit: the handle the commit must mint and surface. */
    uint64_t handle = moq_handle_pack(MOQ_HANDLE_POOL_NAMESPACE_SUB,
                                      s->session_tag, gen, (uint32_t)slot);
    MOQ_TEST_CHECK(target_pre_handle != 0);
    MOQ_TEST_CHECK_EQ_U64(target_pre_handle,
                          moq_handle_pack(MOQ_HANDLE_POOL_NAMESPACE_SUB,
                                          s->session_tag, burn_stale_gen,
                                          (uint32_t)slot));
    MOQ_TEST_CHECK(target_pre_handle != handle);
    /* The filler is a real committed owner, and its expected record is BUILT
     * from the fixture -- request id 2, its own ref, the one-part "fill" prefix
     * of extent 4, no tokens -- never read back from the setup that produced
     * it. */
    nso_snap_t filler_want, filler_have;
    memset(&filler_want, 0, sizeof(filler_want));
    filler_want.present = 1;
    filler_want.state = (int)MOQ_NS_SUB_PENDING_PUBLISHER;
    filler_want.stream_kind = (int)MOQ_STREAM_KIND_NAMESPACE_SUB;
    filler_want.generation = filler_gen;
    filler_want.handle = filler_handle;
    filler_want.request_id = 2;
    filler_want.stream_ref = filler._v;
    filler_want.ep_kind = (int)MOQ_REQ_NAMESPACE_SUB;
    filler_want.ep_slot = filler_slot;
    filler_want.ep_has_request_id = 1;
    filler_want.ep_request_id = 2;
    filler_want.ep_has_stream_ref = 1;
    filler_want.ep_stream_ref = filler._v;
    filler_want.interest = (int)MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
    filler_want.forward = 1;
    filler_want.parse_complete = 1;
    filler_want.auth_processed = 1;
    filler_want.auth_committed = 1;
    filler_want.prefix_valid = 1;
    filler_want.prefix_count = 1;
    filler_want.prefix_buf_len = 4;
    filler_want.prefix[filler_want.prefix_len++] = 4;
    memcpy(filler_want.prefix + filler_want.prefix_len, "fill", 4);
    filler_want.prefix_len += 4;
    filler_want.recv_buf = filler_pre_buf;
    filler_want.recv_cap = filler_pre_cap;
    filler_want.recv_len = filler_req_len;
    memcpy(filler_want.recv, filler_req, filler_req_len);
    nso_read(s, filler_slot, &filler_have);
    failures += nso_diff(&filler_have, &filler_want, "nss filler committed");
    /* Allocation identity, conserved from here on. */
    nsc_snap_t filler_alloc, tgt_alloc, alloc_now;
    nsc_read(s, filler_slot, &filler_alloc);
    og_graph_t filler_graph;
    og_capture(s, &filler_graph);
    failures += og_check_integrity(&filler_graph, "nss filler committed");
    failures += nss_check_owner_edges(&filler_graph, filler_slot, filler, 2,
                                      "nss filler committed");

    c->ref = target;
    c->request_id = 4;
    c->last_handle = 0;
    const void *slot_recv_buf = (const void *)s->ns_subs[slot].recv_buf;
    size_t slot_recv_cap = s->ns_subs[slot].recv_cap;
    drain_snap_t ring_before;
    drain_snap(s, &ring_before);
    moq_stream_ref_t both[2] = { target, filler };
    txs_snapshot_t pre_feed;
    txs_capture(s, both, 2, &pre_feed);

    /* Refused solely because the one event slot is taken. */
    MOQ_TEST_CHECK_EQ_INT((int)f->feed(s, f->ctx, target, true),
                          (int)MOQ_ERR_WOULD_BLOCK);

    /* The staging subscription and its stream-ref key are gone; exactly one
     * RECVING_PUBLISHER owner holds the bytes, its real namespace-ref edge and
     * the transitional FIN -- with `pending_fin` still clear. */
    {   /* the staging subscription is retired: no live sub entry at all */
        int live_subs = 0;
        for (size_t i = 0; i < s->sub_cap; i++)
            if (s->subs[i].state != MOQ_SUB_FREE) live_subs++;
        MOQ_TEST_CHECK_EQ_INT(live_subs, 0);
    }
    MOQ_TEST_CHECK_EQ_INT(ns_busy_count(s), 2);   /* filler + target */
    /* The DECLARED pre-commit inventory. The request is buffered, parsed and
     * request-id-validated but NOT committed: the validation stores the request
     * id and endpoint on the entry for the retry, so those ARE present, while
     * the auth commitment waits for the commit. The handle is the NONZERO STALE
     * one declared on the free slot -- `ns_sub_on_new_bidi()` mints nothing, so
     * the refused owner still carries it; the commit later replaces it with the
     * independently packed live handle. Standing omissions are those of
     * `nso_snap_t` itself -- resolved-token storage beyond its count, and the
     * auth transaction's overlay. */
    nso_snap_t tgt_want, tgt_have;
    memset(&tgt_want, 0, sizeof(tgt_want));
    tgt_want.present = 1;
    tgt_want.state = (int)MOQ_NS_SUB_RECVING_PUBLISHER;
    tgt_want.stream_kind = (int)MOQ_STREAM_KIND_NAMESPACE_SUB;
    tgt_want.generation = gen;
    tgt_want.handle = target_pre_handle;   /* pre-commit: the slot's own value */
    tgt_want.stream_ref = target._v;
    /* The request-id validation runs BEFORE the event refusal and stores the
     * endpoint on the entry for the retry, so these ARE part of the pre-commit
     * record; only the handle and the auth commitment wait for the commit. */
    tgt_want.request_id = 4;
    tgt_want.ep_kind = (int)MOQ_REQ_NAMESPACE_SUB;
    tgt_want.ep_slot = slot;
    tgt_want.ep_has_request_id = 1;
    tgt_want.ep_request_id = 4;
    tgt_want.ep_has_stream_ref = 1;
    tgt_want.ep_stream_ref = target._v;
    tgt_want.interest = (int)MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
    tgt_want.forward = 1;
    tgt_want.parse_complete = 1;
    tgt_want.auth_processed = 1;
    tgt_want.token_count = 1;
    tgt_want.prefix_valid = 1;
    tgt_want.prefix_count = 2;
    tgt_want.prefix_buf_len = 6;
    tgt_want.prefix[tgt_want.prefix_len++] = 4;
    memcpy(tgt_want.prefix + tgt_want.prefix_len, "live", 4);
    tgt_want.prefix_len += 4;
    tgt_want.prefix[tgt_want.prefix_len++] = 2;
    memcpy(tgt_want.prefix + tgt_want.prefix_len, "eu", 2);
    tgt_want.prefix_len += 2;
    tgt_want.handoff_fin = 1;
    tgt_want.recv_buf = slot_recv_buf;
    tgt_want.recv_cap = slot_recv_cap;
    tgt_want.recv_len = c->last_req_len;
    memcpy(tgt_want.recv, c->last_req, c->last_req_len);
    nso_read(s, slot, &tgt_have);
    failures += nso_diff(&tgt_have, &tgt_want, "nss event-blocked target");

    {   /* The target's EXACT pre-commit edge set: its namespace-ref edge only,
         * with no request-id key yet (the commit installs that) and no generic
         * stream-ref key (staging is already retired). */
        const og_edge_spec_t want_e[] = { { OG_DOM_NS_REF, target._v } };
        og_graph_t g;
        og_capture(s, &g);
        failures += og_check_integrity(&g, "nss event blocked");
        failures += og_check_owner_edges(&g, (int)MOQ_REQ_NAMESPACE_SUB, slot,
                                         want_e, 1, "nss event blocked");
        failures += og_check_no_edge(&g, OG_DOM_REQ_STREAMREF, target._v,
                                     "nss event blocked");
        failures += og_check_no_edge(&g, OG_DOM_REQ_RID, 4,
                                     "nss event blocked");
        /* The filler owner and its edges are untouched by the refusal. */
        failures += nss_check_owner_edges(&g, filler_slot, filler, 2,
                                          "nss event blocked filler edges");
    }
    nso_read(s, filler_slot, &filler_have);
    failures += nso_diff(&filler_have, &filler_want,
                         "nss event blocked filler owner");
    nsc_read(s, slot, &tgt_alloc);          /* after the absolute check above */
    nsc_read(s, filler_slot, &alloc_now);
    failures += nsc_same(&alloc_now, &filler_alloc,
                         "nss event blocked filler alloc");
    {   /* The declared whole-session delta over BOTH refs: the target's stream
         * acquires its namespace-sub owner and NOTHING else changes -- including
         * the event-scratch cursor. `event_queue_full()` returns before the
         * scratch-copy code, so a refused target cannot legitimately move it,
         * and the pre-feed value is compared unchanged. This is specific to the
         * event-capacity refusal; rows that successfully surface and poll an
         * event keep their documented scratch delta. */
        txs_snapshot_t want_feed = pre_feed;
        want_feed.owners[0].kind = TXS_OWNER_NS;
        want_feed.owners[0].slot = slot;
        want_feed.owners[0].state = (int)MOQ_NS_SUB_RECVING_PUBLISHER;
        want_feed.owners[0].generation = gen;
        want_feed.owners[0].invalid = 0;
        failures += txs_check_eq(s, both, 2, &want_feed,
                                 "nss event blocked delta");
    }
    {
        drain_snap_t now;
        drain_snap(s, &now);
        failures += drain_multiset_equals(&now, &ring_before,
                                          "nss event blocked");
    }
    /* Exactly the declared filler is queued -- the refused request emitted
     * nothing. */
    {
        txs_norm_vec_t got, want;
        txs_norm_init(&got);
        txs_norm_init(&want);
        collect_events(s, &f->hooks, &got);
        {   /* the filler's own declared image: one part, no tokens */
            txs_img_t im;
            txs_img_init(&im);
            txs_img_u64(&im, filler_handle);
            txs_img_u64(&im, (uint64_t)MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE);
            txs_img_u64(&im, 1);
            txs_img_u64(&im, 1);
            txs_img_bytes(&im, (const uint8_t *)"fill", 4);
            txs_img_u64(&im, 0);
            MOQ_TEST_CHECK(txs_norm_append_img(&want,
                MOQ_EVENT_NS_SUB_REQUEST, &im));
        }
        failures += txs_norm_equals(&got, &want, "nss event blocked filler");
        txs_norm_free(&got);
        txs_norm_free(&want);
    }

    check_no_output(s, &f->hooks);

    /* Polling the filler is a legitimate queue transition; both owner records
     * and the exact topology must survive it unchanged. */
    nso_read(s, slot, &tgt_have);
    failures += nso_diff(&tgt_have, &tgt_want, "nss event blocked drained");
    nso_read(s, filler_slot, &filler_have);
    failures += nso_diff(&filler_have, &filler_want,
                         "nss event blocked filler drained");
    nsc_read(s, slot, &alloc_now);
    failures += nsc_same(&alloc_now, &tgt_alloc,
                         "nss event blocked target alloc");
    nsc_read(s, filler_slot, &alloc_now);
    failures += nsc_same(&alloc_now, &filler_alloc,
                         "nss event blocked filler alloc drained");
    {
        const og_edge_spec_t want_e[] = { { OG_DOM_NS_REF, target._v } };
        og_graph_t g;
        og_capture(s, &g);
        failures += og_check_integrity(&g, "nss event blocked drained");
        failures += og_check_owner_edges(&g, (int)MOQ_REQ_NAMESPACE_SUB, slot,
                                         want_e, 1, "nss event blocked drained");
        failures += nss_check_owner_edges(&g, filler_slot, filler, 2,
                                          "nss event blocked filler drained");
    }

    /* The empty re-feed commits the request AND closes reciprocally. */
    c->last_handle = 0;
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, target, NULL, 0, false, 1),
        (int)MOQ_OK);
    {
        txs_norm_vec_t got, want;
        txs_norm_init(&got);
        txs_norm_init(&want);
        collect_events(s, &f->hooks, &got);
        want_nss_request(&want, handle);
        failures += txs_norm_equals(&got, &want, "nss event blocked retry");
        txs_norm_free(&got);
        txs_norm_free(&want);
    }
    MOQ_TEST_CHECK_EQ_U64(c->last_handle, handle);
    {
        txs_norm_vec_t acts, wacts;
        txs_norm_init(&acts);
        txs_norm_init(&wacts);
        collect_actions(s, &f->hooks, &acts);
        want_close_bidi(&wacts, target);
        failures += txs_norm_equals(&acts, &wacts, "nss event blocked close");
        txs_norm_free(&acts);
        txs_norm_free(&wacts);
    }
    /* The target is retired in every domain; the filler is byte-for-byte and
     * edge-for-edge unchanged. Repeated after a second empty re-feed, so the
     * completion is exactly-once rather than once-observed. */
    for (int pass = 0; pass < 2; pass++) {
        if (pass == 1)
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_session_on_bidi_stream_bytes(s, target, NULL, 0,
                                                      false, 1),
                (int)MOQ_OK);
        const char *what = pass == 0 ? "nss event blocked retired"
                                     : "nss event blocked idempotent";
        MOQ_TEST_CHECK(ns_by_ref(s, target) == NULL);
        MOQ_TEST_CHECK_EQ_INT(ns_busy_count(s), 1);
        MOQ_TEST_CHECK(ns_by_ref(s, filler) != NULL);
        nso_read(s, filler_slot, &filler_have);
        failures += nso_diff(&filler_have, &filler_want, what);
        nsc_read(s, filler_slot, &alloc_now);
        failures += nsc_same(&alloc_now, &filler_alloc, what);
        og_graph_t g;
        og_capture(s, &g);
        failures += og_check_integrity(&g, what);
        failures += og_check_no_edge(&g, OG_DOM_NS_REF, target._v, what);
        failures += og_check_no_edge(&g, OG_DOM_REQ_RID, 4, what);
        failures += og_check_no_edge(&g, OG_DOM_REQ_STREAMREF, target._v, what);
        failures += og_check_owner_unreferenced(&g, (int)MOQ_REQ_NAMESPACE_SUB,
                                                slot, what);
        failures += nss_check_owner_edges(&g, filler_slot, filler, 2, what);
        /* The declared topology TRANSITION closes here: filler-only before the
         * target arrived, filler+target while it lived, and filler-only again
         * once it retired -- so `filler_graph` is the baseline, not a capture
         * taken for its own sake. */
        failures += og_check_same_topology(&g, &filler_graph, what);
        drain_snap_t now;
        drain_snap(s, &now);
        failures += drain_multiset_equals(&now, &ring_before, what);
        check_no_output(s, &f->hooks);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
    }

    c->ref = saved_ref;
    c->request_id = saved_id;
    moq_session_destroy(s);
}

/* Clear-on-reuse for the namespace-sub pool. Its free is SELECTIVE, so the
 * marker's clearing is an explicit obligation rather than a memset by-product.
 * The blocked window makes the marker genuinely live at the boundary: the
 * reciprocal close is refused, the empty re-feed completes it, and the freed
 * record must carry neither FIN fact into its next owner. Both generations and
 * both packed handles are derived before ingress. */
static void run_nss_slot_reuse_case(const fin_owner_case_t *f)
{
    moq_session_t *s = make_session_full(MOQ_PERSPECTIVE_SERVER, 0, 1);
    nss_ctx_t *c = (nss_ctx_t *)f->ctx;
    uint64_t saved_id = c->request_id;
    moq_stream_ref_t saved_ref = c->ref;
    moq_stream_ref_t first = c->ref;
    moq_stream_ref_t blocker = moq_stream_ref_from_u64(0x77);

    nss_burn_request(s, moq_stream_ref_from_u64(first._v + 8), 0, 0);
    c->request_id = 2;
    c->last_handle = 0;

    int slot = nss_declare_free_slot(s);
    MOQ_TEST_CHECK(slot >= 0);
    if (slot < 0) { c->request_id = saved_id; moq_session_destroy(s); return; }
    uint32_t gen_first = s->ns_subs[slot].generation | 1u;
    uint32_t gen_reuse = (gen_first + 1u) | 1u;   /* the free advances once */
    uint64_t handle_first = moq_handle_pack(MOQ_HANDLE_POOL_NAMESPACE_SUB,
                                            s->session_tag, gen_first,
                                            (uint32_t)slot);
    uint64_t handle_reuse = moq_handle_pack(MOQ_HANDLE_POOL_NAMESPACE_SUB,
                                            s->session_tag, gen_reuse,
                                            (uint32_t)slot);
    MOQ_TEST_CHECK(handle_first != handle_reuse);
    const void *slot_recv_buf = (const void *)s->ns_subs[slot].recv_buf;
    size_t slot_recv_cap = s->ns_subs[slot].recv_cap;

    /* 1. SUBSCRIBE_NAMESPACE + FIN with no action slot: the reciprocal close is
     *    refused and the marker survives it. */
    MOQ_TEST_CHECK_EQ_INT((int)queue_close_bidi(s, blocker), (int)MOQ_OK);
    MOQ_TEST_CHECK(action_queue_full(s));
    txs_snapshot_t pre_first;
    txs_capture(s, &first, 1, &pre_first);
    MOQ_TEST_CHECK_EQ_INT((int)f->feed(s, f->ctx, first, true),
                          (int)MOQ_ERR_WOULD_BLOCK);
    {
        txs_norm_vec_t got, want;
        txs_norm_init(&got);
        txs_norm_init(&want);
        collect_events(s, &f->hooks, &got);
        want_nss_request(&want, c->last_handle);
        failures += txs_norm_equals(&got, &want, "nss reuse first");
        txs_norm_free(&got);
        txs_norm_free(&want);
    }
    MOQ_TEST_CHECK_EQ_U64(c->last_handle, handle_first);
    {   /* The retention feed's own declared session delta. `pre_first` was
         * captured for exactly this: leaving it uncompared would make an
         * unrelated mutation invisible in the proof built to catch one. */
        txs_snapshot_t observed, want_feed = pre_first;
        txs_capture(s, &first, 1, &observed);
        want_feed.event_scratch_len = observed.event_scratch_len;  /* adopted */
        want_feed.action_depth = observed.action_depth;            /* filler */
        want_feed.owners[0].kind = TXS_OWNER_NS;
        want_feed.owners[0].slot = slot;
        want_feed.owners[0].state = (int)MOQ_NS_SUB_PENDING_PUBLISHER;
        want_feed.owners[0].generation = gen_first;
        want_feed.owners[0].invalid = 0;
        failures += txs_check_eq(s, &first, 1, &want_feed,
                                 "nss reuse handoff delta");
    }
    {
        nso_snap_t w, h;
        nss_declare(&w, slot, gen_first, handle_first, 2, first._v, 1,
                    c->last_req, c->last_req_len, slot_recv_buf, slot_recv_cap);
        nso_read(s, slot, &h);
        failures += nso_diff(&h, &w, "nss reuse first owner");
        og_graph_t g;
        og_capture(s, &g);
        failures += og_check_integrity(&g, "nss reuse first");
        failures += nss_check_owner_edges(&g, slot, first, 2, "nss reuse first");
    }
    /* Drain the filler so the retry has a slot. */
    {
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
    }

    /* 2. The empty re-feed completes the close exactly once and retires it. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, first, NULL, 0, false, 1),
        (int)MOQ_OK);
    {
        txs_norm_vec_t acts, wacts;
        txs_norm_init(&acts);
        txs_norm_init(&wacts);
        collect_actions(s, &f->hooks, &acts);
        want_close_bidi(&wacts, first);
        failures += txs_norm_equals(&acts, &wacts, "nss reuse retry");
        txs_norm_free(&acts);
        txs_norm_free(&wacts);
    }
    MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, 0);

    /* 3. The freed record: generation advanced, BOTH FIN facts clear, and no
     *    edge anywhere still points at the retired owner. */
    MOQ_TEST_CHECK_EQ_INT((int)s->ns_subs[slot].state, (int)MOQ_NS_SUB_FREE);
    MOQ_TEST_CHECK_EQ_U64(s->ns_subs[slot].generation, gen_reuse - 1u);
    MOQ_TEST_CHECK_EQ_INT(s->ns_subs[slot].pending_fin, 0);
    MOQ_TEST_CHECK_EQ_INT(s->ns_subs[slot].handoff_fin_pending, 0);
    check_nss_retired(s, first);
    /* A second empty retry is silent, resurrects nothing and queues no
     * duplicate close -- exactly-once, not merely once-observed. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, first, NULL, 0, false, 1),
        (int)MOQ_OK);
    check_no_output(s, &f->hooks);
    MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, 0);
    MOQ_TEST_CHECK_EQ_INT((int)s->ns_subs[slot].state, (int)MOQ_NS_SUB_FREE);
    MOQ_TEST_CHECK_EQ_INT(s->ns_subs[slot].handoff_fin_pending, 0);
    check_nss_retired(s, first);
    {
        og_graph_t g;
        og_capture(s, &g);
        failures += og_check_integrity(&g, "nss reuse retired");
        failures += og_check_no_edge(&g, OG_DOM_NS_REF, first._v,
                                     "nss reuse retired");
        failures += og_check_no_edge(&g, OG_DOM_REQ_RID, 2,
                                     "nss reuse retired");
        failures += og_check_owner_unreferenced(&g, (int)MOQ_REQ_NAMESPACE_SUB,
                                                slot, "nss reuse retired");
    }

    /* 4. A distinct no-FIN request reuses that physical slot and holds neither
     *    FIN fact. */
    moq_stream_ref_t second = moq_stream_ref_from_u64(first._v + 4);
    c->ref = second;
    c->request_id = 4;
    c->last_handle = 0;
    MOQ_TEST_CHECK_EQ_INT((int)f->feed(s, f->ctx, second, false), (int)MOQ_OK);
    {
        txs_norm_vec_t got, want;
        txs_norm_init(&got);
        txs_norm_init(&want);
        collect_events(s, &f->hooks, &got);
        want_nss_request(&want, c->last_handle);
        failures += txs_norm_equals(&got, &want, "nss reuse second");
        txs_norm_free(&got);
        txs_norm_free(&want);
    }
    MOQ_TEST_CHECK_EQ_U64(c->last_handle, handle_reuse);
    og_graph_t graph_before;
    {
        nso_snap_t w, h;
        nss_declare(&w, slot, gen_reuse, handle_reuse, 4, second._v, 0,
                    c->last_req, c->last_req_len, slot_recv_buf, slot_recv_cap);
        nso_read(s, slot, &h);
        failures += nso_diff(&h, &w, "nss reuse second owner");
        og_capture(s, &graph_before);
        failures += og_check_integrity(&graph_before, "nss reuse live");
        failures += nss_check_owner_edges(&graph_before, slot, second, 4,
                                          "nss reuse live");
    }

    /* 5. Its peer half is genuinely open, so its rejection owes a drain
     *    reference and an exhausted ring refuses it transactionally. */
    fill_drain_ring(s);
    size_t filled = s->drain_ref_count;
    nss_hook_state_t owner;
    f->hooks.capture(s, f->ctx, &owner);
    txs_snapshot_t before;
    txs_capture(s, &second, 1, &before);
    expect_after_call_prepare(&before);
    MOQ_TEST_CHECK_EQ_INT((int)f->app_reject(s, f->ctx, handle_reuse),
                          (int)MOQ_ERR_WOULD_BLOCK);
    failures += txs_check_eq(s, &second, 1, &before, "nss reuse blocked");
    failures += f->hooks.check(s, f->ctx, &owner);
    MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, filled);
    {
        nso_snap_t w, h;
        nss_declare(&w, slot, gen_reuse, handle_reuse, 4, second._v, 0,
                    c->last_req, c->last_req_len, slot_recv_buf, slot_recv_cap);
        nso_read(s, slot, &h);
        failures += nso_diff(&h, &w, "nss reuse blocked owner");
        og_graph_t g;
        og_capture(s, &g);
        failures += og_check_integrity(&g, "nss reuse blocked");
        failures += nss_check_owner_edges(&g, slot, second, 4,
                                          "nss reuse blocked");
        failures += og_check_same_topology(&g, &graph_before,
                                           "nss reuse blocked");
    }
    check_no_output(s, &f->hooks);

    /* 6. The retired first-generation handle stays stale after the reuse. */
    txs_snapshot_t before_stale;
    txs_capture(s, &second, 1, &before_stale);
    expect_after_call_prepare(&before_stale);
    MOQ_TEST_CHECK_EQ_INT((int)f->app_reject(s, f->ctx, handle_first),
                          (int)MOQ_ERR_STALE_HANDLE);
    failures += txs_check_eq(s, &second, 1, &before_stale,
                             "nss stale after reuse");
    failures += f->hooks.check(s, f->ctx, &owner);
    MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, filled);
    check_no_output(s, &f->hooks);

    c->ref = saved_ref;
    c->request_id = saved_id;
    moq_session_destroy(s);
}

static void test_subscribe_namespace(void)
{
    fin_owner_case_t f = nss_family();
    MOQ_TEST_CHECK_EQ_INT(fin_owner_problems(&f), 0);
    run_nss_case(&f, false, false, true);   /* control: drain, then released */
    run_nss_case(&f, true,  false, false);  /* same-call FIN: reciprocal close */
    run_nss_case(&f, false, true,  true);   /* exhausted ring, no FIN */
    run_nss_fin_full_ring(&f);              /* its paired FIN half */
    run_nss_action_blocked(&f, false);      /* deferred close, then retry */
    run_nss_action_blocked(&f, true);       /* answered in the blocked window */
    nss_inventory_selfchecks(&f);
    run_nss_direct_route_control(&f);       /* the profile boundary */
    run_nss_event_blocked(&f);              /* the PRE-COMMIT carrier */
    /* The observed close does not follow the slot to its next owner. */
    run_nss_slot_reuse_case(&f);

    /* The role-less family must declare the sentinel, and only it. */
    {
        fin_owner_case_t bad = f;
        bad.owner_role = (int)MOQ_SUB_ROLE_PUBLISHER;
        MOQ_TEST_CHECK(fin_owner_problems(&bad) > 0);
        fin_owner_case_t bad2 = pub_family();
        bad2.owner_role = FIN_OWNER_ROLE_NA;
        MOQ_TEST_CHECK(fin_owner_problems(&bad2) > 0);
    }
}


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
    int      req_recv_fin;
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
        if (e->state != MOQ_TS_FREE && e->request_stream_ref._v == ref._v)
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
    st->req_stream_ref = e->request_stream_ref._v;
    st->req_recv_fin = e->req_recv_fin;
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
    if (now.req_recv_fin != want->req_recv_fin) bad++;
    if (now.handoff_fin != want->handoff_fin) bad++;
    if (now.state != want->state) bad++;
    if (now.role != want->role) bad++;
    if (now.busy != want->busy) bad++;
    if (bad)
        TXS_DIAG("TXN ts hooks: present %d handle %llu ref %llu fin %d "
                 "state %d busy %d, expected %d/%llu/%llu/%d/%d/%d\n",
                 now.present, (unsigned long long)now.handle,
                 (unsigned long long)now.req_stream_ref, now.req_recv_fin,
                 now.state, now.busy, want->present,
                 (unsigned long long)want->handle,
                 (unsigned long long)want->req_stream_ref, want->req_recv_fin,
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
    MOQ_TEST_CHECK_EQ_INT(owner_before.req_recv_fin, fin_in_request ? 1 : 0);
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

static void test_track_status(void)
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

/* -- Negative controls ------------------------------------------------ */

/* Control rows surface no further events after their request, so they reuse
 * the shared bidi-action normalizer with an event normalizer that refuses
 * everything: an unexpected event is a failure, not a silent pass. */
static bool no_event_expected(const moq_event_t *ev, void *ctx,
                              txs_norm_vec_t *out)
{
    (void)ctx; (void)out;
    TXS_DIAG("TXN control: unexpected event kind %u\n", (unsigned)ev->kind);
    return false;
}

static txs_op_hooks_t pub_hooks_for_actions = {
    NULL, NULL, NULL, NULL, no_event_expected, pub_norm_action
};

/* SUBSCRIBE is the same-entry control: the staging slot IS the subscription
 * slot, so there is no rekey and no marker to install. Its FIN is latched at
 * ingress today, and its terminal already consults that latch -- behaviour the
 * conversion must leave alone. */
static void test_subscribe_control(void)
{
    for (int fin = 0; fin < 2; fin++) {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_namespace_t ns = { parts, 1 };
        moq_d18_msg_params_t mp;
        memset(&mp, 0, sizeof(mp));
        mp.has_filter = true;
        mp.filter_type = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        mp.has_forward = true; mp.forward = 1;
        uint8_t m[160];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, m, sizeof(m));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_encode_subscribe(&w, 0, &ns, MOQ_BYTES_LITERAL("v"),
                                          &mp), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, m,
                                                  moq_buf_writer_offset(&w),
                                                  fin != 0, 1), (int)MOQ_OK);
        moq_subscription_t sub = MOQ_SUBSCRIPTION_INVALID;
        moq_event_t ev;
        int n_req = 0, n_other = 0;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                n_req++;
                sub = ev.u.subscribe_request.sub;
            } else {
                n_other++;
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(n_req, 1);
        MOQ_TEST_CHECK_EQ_INT(n_other, 0);

        /* Same entry, and the close is on it already -- no rekey happened.
         * The full identity is asserted because "same entry" is the claim. */
        sub_owner_state_t so;
        sub_owner_capture(s, ref, &so);
        MOQ_TEST_CHECK(so.present);
        MOQ_TEST_CHECK_EQ_INT(so.role, (int)MOQ_SUB_ROLE_PUBLISHER);
        MOQ_TEST_CHECK_EQ_INT(so.state, (int)MOQ_SUB_PENDING_PUBLISHER);
        MOQ_TEST_CHECK_EQ_INT(so.req_recv_fin, fin);
        MOQ_TEST_CHECK_EQ_U64(so.req_stream_ref, ref._v);
        MOQ_TEST_CHECK_EQ_U64(so.handle, sub._opaque);
        MOQ_TEST_CHECK(so.slot >= 0);
        MOQ_TEST_CHECK(so.has_stream_ref);
        MOQ_TEST_CHECK_EQ_U64(so.registry_ref, ref._v);
        MOQ_TEST_CHECK_EQ_INT(so.busy, 1);

        drain_snap_t ring_before;
        drain_snap(s, &ring_before);
        moq_reject_subscribe_cfg_t rj;
        moq_reject_subscribe_cfg_init(&rj);
        rj.error_code = MOQ_REQUEST_ERROR_NOT_SUPPORTED;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_reject_subscribe(s, sub, &rj, 1),
                              (int)MOQ_OK);
        drain_snap_t now, want_ring;
        drain_snap(s, &now);
        if (fin) want_ring = ring_before;
        else     drain_snap_plus(&ring_before, ref, MOQ_DRAIN_NORMAL,
                                 &want_ring);
        failures += drain_multiset_equals(&now, &want_ring, "subscribe control");
        MOQ_TEST_CHECK_EQ_INT(sub_busy_count(s), 0);
        check_unregistered(s, ref);
        check_reject_only_output(s, &pub_hooks_for_actions, ref,
                                 "subscribe control");

        if (!fin) {
            /* The reference the rejection took is released by the peer's
             * later FIN, restoring the ring exactly and producing nothing. */
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
                (int)MOQ_OK);
            drain_snap_t after;
            drain_snap(s, &after);
            failures += drain_multiset_equals(&after, &ring_before,
                                              "subscribe control released");
            MOQ_TEST_CHECK_EQ_INT(sub_busy_count(s), 0);
            check_unregistered(s, ref);
            check_no_output(s, &pub_hooks_for_actions);
        }
        moq_session_destroy(s);
    }
}

/* Route-exclusion negatives: a terminal the producer's owner cannot reach
 * must refuse it, which is why those sites are NOT marker consumers for that
 * producer. Note what each actually proves -- PUBLISH and PUBLISH_NAMESPACE
 * meet their STATE guard before their role guard is ever consulted, so only
 * FETCH demonstrates the role guard directly. */
static void test_route_exclusions(void)
{
    /* Inbound PUBLISH commits SUBSCRIBER role in PENDING_SUBSCRIBER; the
     * finish terminal requires an established PUBLISHER-role publication and
     * refuses on state. */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        MOQ_TEST_CHECK_EQ_INT((int)feed_publish(s, ref, 0, 7, false),
                              (int)MOQ_OK);
        moq_publication_t pub = MOQ_PUBLICATION_INVALID;
        int n_want = 0, n_other = 0;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_PUBLISH_REQUEST) {
                n_want++;
                pub = ev.u.publish_request.pub;
            } else { n_other++; }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(n_want, 1);
        MOQ_TEST_CHECK_EQ_INT(n_other, 0);
        const moq_pub_entry_t *e = pub_by_ref(s, ref);
        MOQ_TEST_CHECK(e != NULL);
        if (e) {
            MOQ_TEST_CHECK_EQ_U64(e->handle._opaque, pub._opaque);
            MOQ_TEST_CHECK_EQ_INT((int)e->role, (int)MOQ_PUB_ROLE_SUBSCRIBER);
            MOQ_TEST_CHECK_EQ_INT((int)e->state,
                                  (int)MOQ_PUB_PENDING_SUBSCRIBER);
        }
        txs_snapshot_t before;
        txs_capture(s, &ref, 1, &before);
        expect_after_call_prepare(&before);
        moq_finish_publish_cfg_t fc;
        moq_finish_publish_cfg_init(&fc);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_finish_publish(s, pub, &fc, 1),
                              (int)MOQ_ERR_WRONG_STATE);
        failures += txs_check_eq(s, &ref, 1, &before, "publish route exclusion");
        failures += pub_owner_present_problems(s, ref, MOQ_REQ_PUBLISH,
                                               (int)MOQ_PUB_ROLE_SUBSCRIBER,
                                               (int)MOQ_PUB_PENDING_SUBSCRIBER);
        check_no_output(s, &pub_hooks_for_actions);
        moq_session_destroy(s);
    }

    /* Inbound FETCH commits PUBLISHER role; the cancel terminal is
     * FETCHER-only, so this one does exercise the ROLE guard. */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        fetch_ctx_t fc = { 0, ref, 0 };
        MOQ_TEST_CHECK_EQ_INT((int)fetch_feed(s, &fc, ref, false), (int)MOQ_OK);
        moq_fetch_t fh = { 0 };
        int n_want = 0, n_other = 0;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_REQUEST) {
                n_want++;
                fh = ev.u.fetch_request.fetch;
            } else { n_other++; }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(n_want, 1);
        MOQ_TEST_CHECK_EQ_INT(n_other, 0);
        const moq_fetch_entry_t *e = fetch_by_ref(s, ref);
        MOQ_TEST_CHECK(e != NULL);
        if (e) {
            MOQ_TEST_CHECK_EQ_U64(e->handle._opaque, fh._opaque);
            MOQ_TEST_CHECK_EQ_INT((int)e->role,
                                  (int)MOQ_FETCH_ROLE_PUBLISHER);
            MOQ_TEST_CHECK_EQ_INT((int)e->state,
                                  (int)MOQ_FETCH_PENDING_PUBLISHER);
        }
        txs_snapshot_t before;
        txs_capture(s, &ref, 1, &before);
        expect_after_call_prepare(&before);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch_cancel(s, fh, 1),
                              (int)MOQ_ERR_WRONG_STATE);
        failures += txs_check_eq(s, &ref, 1, &before, "fetch route exclusion");
        failures += fetch_owner_present_problems(s, ref, MOQ_REQ_FETCH,
                                                 (int)MOQ_FETCH_ROLE_PUBLISHER,
                                                 (int)MOQ_FETCH_PENDING_PUBLISHER);
        check_no_output(s, &pub_hooks_for_actions);
        moq_session_destroy(s);
    }

    /* Inbound PUBLISH_NAMESPACE commits RECEIVER role in PENDING_RECEIVER;
     * the done terminal requires an established ANNOUNCER-role announcement
     * and refuses on state. */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        pn_ctx_t pc = { 0, ref, 0 };
        MOQ_TEST_CHECK_EQ_INT((int)pn_feed(s, &pc, ref, false), (int)MOQ_OK);
        moq_announcement_t ah = { 0 };
        int n_want = 0, n_other = 0;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED) {
                n_want++;
                ah = ev.u.namespace_published.ann;
            } else { n_other++; }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(n_want, 1);
        MOQ_TEST_CHECK_EQ_INT(n_other, 0);
        const moq_ann_entry_t *e = ann_by_ref(s, ref);
        MOQ_TEST_CHECK(e != NULL);
        if (e) {
            MOQ_TEST_CHECK_EQ_U64(e->handle._opaque, ah._opaque);
            MOQ_TEST_CHECK_EQ_INT((int)e->role, (int)MOQ_ANN_ROLE_RECEIVER);
            MOQ_TEST_CHECK_EQ_INT((int)e->state,
                                  (int)MOQ_ANN_PENDING_RECEIVER);
        }
        txs_snapshot_t before;
        txs_capture(s, &ref, 1, &before);
        expect_after_call_prepare(&before);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_publish_namespace_done(s, ah, 1),
            (int)MOQ_ERR_WRONG_STATE);
        failures += txs_check_eq(s, &ref, 1, &before, "namespace route exclusion");
        failures += pn_owner_present_problems(s, ref, MOQ_REQ_ANNOUNCEMENT,
                                              (int)MOQ_ANN_ROLE_RECEIVER,
                                              (int)MOQ_ANN_PENDING_RECEIVER);
        check_no_output(s, &pub_hooks_for_actions);
        moq_session_destroy(s);
    }
}


/* -- Pre-commit reject branches (Axis 3) ------------------------------ */

/* These branches answer before any destination owner exists, so they install no
 * handoff marker. The FIN they consult belongs to the generic staging owner
 * that carried the request: `handle_request_stream_bytes()` latches an arriving
 * FIN into `e->req_recv_fin`, retains it across a refused call, and hands that
 * latch -- not the raw call flag -- to the profile, so an empty re-feed still
 * knows the peer's send half is closed. Every PUBLISH branch shares one
 * observed-FIN decision, `reject_drain = d->endpoint.has_stream_ref &&
 * !request_fin_observed` (`session_core_on_publish`), and both
 * PUBLISH_NAMESPACE branches share its counterpart beside `req_stream`
 * (`session_core_on_publish_namespace`), so an observed FIN spends no drain
 * reference while wire output and staging retirement still follow stream
 * correlation. */

static void srv_cfg_init(moq_session_cfg_t *cfg)
{
    moq_session_cfg_init_sized(cfg, sizeof(*cfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_SERVER);
    cfg->version = MOQ_VERSION_DRAFT_18;
}

/* A draft-18 server established through the ordinary SETUP exchange with the
 * caller's caps. Establishment is asserted, so a fixture cannot silently drive
 * a session that never reached ESTABLISHED. */
static moq_session_t *make_session_srv(const moq_session_cfg_t *cfg)
{
    moq_session_t *s = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_create(cfg, 0, &s), (int)MOQ_OK);
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

static moq_session_t *make_session_default(void)
{
    moq_session_cfg_t cfg;
    srv_cfg_init(&cfg);
    return make_session_srv(&cfg);
}

static moq_session_t *make_session_pub_cap_1(void)
{
    moq_session_cfg_t cfg;
    srv_cfg_init(&cfg);
    cfg.max_publishes = 1;
    return make_session_srv(&cfg);
}

static moq_session_t *make_session_hist_cap_1(void)
{
    moq_session_cfg_t cfg;
    srv_cfg_init(&cfg);
    cfg.max_track_history_records = 1;
    return make_session_srv(&cfg);
}

/* One event slot, so a single queued sentinel refuses the next handler that
 * needs to surface anything. */
static moq_session_t *make_session_events_1(void)
{
    moq_session_cfg_t cfg;
    srv_cfg_init(&cfg);
    cfg.max_events = 1;
    return make_session_srv(&cfg);
}

static moq_session_t *make_session_ann_cap_1(void)
{
    moq_session_cfg_t cfg;
    srv_cfg_init(&cfg);
    cfg.max_announcements = 1;
    return make_session_srv(&cfg);
}

/* -- Request encoders ------------------------------------------------ */

static size_t enc_publish_full(uint8_t *buf, size_t cap, uint64_t rid,
                               uint64_t alias, const char *name,
                               const moq_d18_msg_params_t *params)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    moq_d18_publish_t p = { 0 };
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    p.request_id = rid;
    p.track_namespace = (moq_namespace_t){ parts, 1 };
    p.track_name = (moq_bytes_t){ (const uint8_t *)name, strlen(name) };
    p.track_alias = alias;
    if (params) p.params = *params;
    MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_publish(&w, &p), (int)MOQ_OK);
    return moq_buf_writer_offset(&w);
}

static size_t enc_publish_plain(uint8_t *buf, size_t cap, uint64_t rid,
                                uint64_t alias, const char *name)
{
    return enc_publish_full(buf, cap, rid, alias, name, NULL);
}

/* One unknown USE_ALIAS, which the token walk rejects with 0x17 and no reason
 * phrase (session_auth.c:395). */
static size_t enc_publish_unknown_alias(uint8_t *buf, size_t cap)
{
    moq_d18_msg_params_t params = { 0 };
    params.auth_token_count = 1;
    params.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_USE_ALIAS;
    params.auth_tokens[0].alias = 99;
    return enc_publish_full(buf, cap, 0, 7, "v", &params);
}

/* The encoder refuses a mandatory Track Property, so a valid KVP (type 0x4000,
 * value 0) is appended to an encoded PUBLISH payload and the message reframed.
 * The envelope is decoded rather than assuming a header width, and the type is
 * re-emitted from the decoded value. An ODD property type would be malformed
 * rather than mandatory-unknown, and would reach a different terminal. */
static size_t enc_publish_mandatory_prop(uint8_t *buf, size_t cap)
{
    uint8_t base[192];
    size_t base_len = enc_publish_full(base, sizeof(base), 0, 7, "v", NULL);
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, base, base_len);
    moq_control_envelope_t env;
    MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_envelope(&r, &env), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(env.msg_type, (uint64_t)MOQ_D18_PUBLISH);
    MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&r), (size_t)0);

    uint8_t kvp[8];
    moq_buf_writer_t kw;
    moq_buf_writer_init(&kw, kvp, sizeof(kvp));
    MOQ_TEST_CHECK_EQ_INT((int)moq_buf_write_vi64(&kw, 0x4000), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_buf_write_vi64(&kw, 0), (int)MOQ_OK);
    size_t kvp_len = moq_buf_writer_offset(&kw);

    uint8_t pl[224];
    MOQ_TEST_CHECK(env.payload_len + kvp_len <= sizeof(pl));
    if (env.payload_len + kvp_len > sizeof(pl)) return 0;
    memcpy(pl, env.payload, env.payload_len);
    memcpy(pl + env.payload_len, kvp, kvp_len);
    return enc_typed_envelope(buf, cap, env.msg_type, pl,
                              env.payload_len + kvp_len);
}

static size_t enc_publish_hist_target(uint8_t *buf, size_t cap)
{
    return enc_publish_plain(buf, cap, 0, 7, "target");
}

static size_t enc_publish_second(uint8_t *buf, size_t cap)
{
    return enc_publish_plain(buf, cap, 2, 8, "b");
}

static size_t enc_pns_full(uint8_t *buf, size_t cap, uint64_t rid,
                           const char *part0,
                           const moq_d18_msg_params_t *params)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    moq_bytes_t parts[] = { { (const uint8_t *)part0, strlen(part0) } };
    moq_namespace_t ns = { parts, 1 };
    moq_d18_msg_params_t empty = { 0 };
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_d18_encode_publish_namespace(&w, rid, &ns,
                                              params ? params : &empty),
        (int)MOQ_OK);
    return moq_buf_writer_offset(&w);
}

/* The same unknown-USE_ALIAS shape, on the namespace request. */
static size_t enc_pns_unknown_alias(uint8_t *buf, size_t cap)
{
    moq_d18_msg_params_t params = { 0 };
    params.auth_token_count = 1;
    params.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_USE_ALIAS;
    params.auth_tokens[0].alias = 99;
    return enc_pns_full(buf, cap, 0, "live", &params);
}

/* A namespace distinct from the one the fixture already retained. */
static size_t enc_pns_second(uint8_t *buf, size_t cap)
{
    return enc_pns_full(buf, cap, 2, "other", NULL);
}

/* -- Fixture preconditions ------------------------------------------- */

/* Every event and action a seed produces is counted by kind and required to be
 * exactly what the seed owes. Draining whatever appears would let a fixture
 * that reached a different path -- or that left a stray action behind -- seed
 * the case anyway. */
static void seed_output_exact(moq_session_t *s, uint32_t want_event_kind,
                              int want_events, const char *what)
{
    int matched = 0, other_events = 0, actions = 0;
    moq_event_t e;
    while (moq_session_poll_events(s, &e, 1) > 0) {
        if (e.kind == want_event_kind) matched++;
        else {
            other_events++;
            TXS_DIAG("TXN %s seed: unexpected event kind %u\n", what,
                     (unsigned)e.kind);
        }
        moq_event_cleanup(&e);
    }
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) {
        actions++;
        TXS_DIAG("TXN %s seed: unexpected action kind %u\n", what,
                 (unsigned)a.kind);
        moq_action_cleanup(&a);
    }
    MOQ_TEST_CHECK_EQ_INT(matched, want_events);
    MOQ_TEST_CHECK_EQ_INT(other_events, 0);
    MOQ_TEST_CHECK_EQ_INT(actions, 0);
}

/* Occupy the only publication slot with a surfaced, unanswered request. */
static void seed_publication(moq_session_t *s, moq_stream_ref_t seed_ref)
{
    uint8_t m[192];
    size_t n = enc_publish_plain(m, sizeof(m), 0, 7, "a");
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, seed_ref, m, n, false, 1),
        (int)MOQ_OK);
    seed_output_exact(s, MOQ_EVENT_PUBLISH_REQUEST, 1, "publication");
    MOQ_TEST_CHECK_EQ_INT(pub_busy_count(s), 1);
}

/* Occupy the only announcement slot with a surfaced, unanswered request. */
static void seed_announcement(moq_session_t *s, moq_stream_ref_t seed_ref)
{
    uint8_t m[192];
    size_t n = enc_pns_full(m, sizeof(m), 0, "seeded", NULL);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, seed_ref, m, n, false, 1),
        (int)MOQ_OK);
    seed_output_exact(s, MOQ_EVENT_NAMESPACE_PUBLISHED, 1, "announcement");
    MOQ_TEST_CHECK_EQ_INT(ann_busy_count(s), 1);
}

static moq_namespace_t hist_ns(moq_bytes_t *store)
{
    store[0] = MOQ_BYTES_LITERAL("live");
    return (moq_namespace_t){ store, 1 };
}

/* Pin the single history record on a DIFFERENT track identity, so the target
 * request finds the registry full rather than an existing record. */
static void seed_track_history(moq_session_t *s, moq_stream_ref_t seed_ref)
{
    (void)seed_ref;
    moq_bytes_t part;
    moq_namespace_t ns = hist_ns(&part);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_note_object_published(s, &ns,
                                               MOQ_BYTES_LITERAL("pinned"),
                                               1, 0),
        (int)MOQ_OK);
    /* Recording history is not application-visible: it owes no output. */
    seed_output_exact(s, MOQ_EVENT_SESSION_CLOSED, 0, "track history");
}

/* The selection the reject branch will make, asserted before the request is
 * fed: a fixture whose seed left a free slot would exercise the success path
 * and never reach the branch under test. */
static void expect_history_full(moq_session_t *s)
{
    moq_bytes_t part;
    moq_namespace_t ns = hist_ns(&part);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_track_hist_select(s, &ns, MOQ_BYTES_LITERAL("target")),
        (int)MOQ_TH_SEL_FULL);
}

/* -- Destination-owner oracles --------------------------------------- */

static void owners_publish_none(const moq_session_t *s, moq_stream_ref_t ref,
                                moq_stream_ref_t seed_ref)
{
    (void)seed_ref;
    MOQ_TEST_CHECK_EQ_INT(pub_busy_count(s), 0);
    MOQ_TEST_CHECK(pub_by_ref(s, ref) == NULL);
}

static void owners_publish_seeded(const moq_session_t *s, moq_stream_ref_t ref,
                                  moq_stream_ref_t seed_ref)
{
    MOQ_TEST_CHECK_EQ_INT(pub_busy_count(s), 1);
    MOQ_TEST_CHECK(pub_by_ref(s, seed_ref) != NULL);
    MOQ_TEST_CHECK(pub_by_ref(s, ref) == NULL);
}

static void owners_ann_none(const moq_session_t *s, moq_stream_ref_t ref,
                            moq_stream_ref_t seed_ref)
{
    (void)seed_ref;
    MOQ_TEST_CHECK_EQ_INT(ann_busy_count(s), 0);
    MOQ_TEST_CHECK(ann_by_ref(s, ref) == NULL);
}

static void owners_ann_seeded(const moq_session_t *s, moq_stream_ref_t ref,
                              moq_stream_ref_t seed_ref)
{
    MOQ_TEST_CHECK_EQ_INT(ann_busy_count(s), 1);
    MOQ_TEST_CHECK(ann_by_ref(s, seed_ref) != NULL);
    MOQ_TEST_CHECK(ann_by_ref(s, ref) == NULL);
}

/* -- Seeded-owner conservation --------------------------------------- */

/* A seeded owner must be preserved WHOLE, not merely present: occupancy and a
 * lookup by stream ref would miss a changed role, state, generation, handle,
 * request identity, FIN latch or registry linkage. Each family captures every
 * durable field it owns and the whole capture is re-taken and compared.
 *
 * The storage is a plain union rather than three parameters. Nothing about the
 * union prevents a mismatched pair: that is enforced by seed_ops_t, which owns
 * a family's seed, capture and check together, so a case names one bundle or
 * none. */

#define REQ_SEED_BUF_MAX 256

typedef struct req_seed_snap {
    int      busy;          /* occupied entries in the family's pool */
    int      found;         /* an entry keyed by the seeded stream ref */
    int      state;
    int      role;
    uint32_t generation;
    uint64_t handle;
    uint64_t request_id;
    uint64_t stream_ref;
    int      fin;           /* the seeded owner's own durable FIN latch */
    size_t   recv_len;      /* buffered response bytes on its request bidi */
    /* Registry linkage, an independent key: the whole endpoint the lookup
     * returns, not only its kind and slot. */
    int      reg_kind;
    int      reg_slot;
    int      reg_has_stream_ref;
    uint64_t reg_stream_ref;
    /* The by-REQUEST-ID registry is an independent key: a leaked entry there
     * would survive a retirement that only cleared the stream-ref index. */
    int      id_kind;
    int      id_slot;
    /* The by-ID lookup reconstructs the request-id half of the endpoint and
     * leaves the stream-ref half zero, so those are the fields worth pinning. */
    int      id_has_request_id;
    uint64_t id_request_id;
    /* Namespace-subscription lifecycle scalars; other families leave these
     * zero, so only the owner that sets them can diverge on them. */
    int      ns_got_response;
    int      ns_parse_complete;
    int      ns_closing_remote_error;
    /* The durable local-teardown obligation marker (#245c). Distinct from the
     * `fin` latch above: `fin` records that the peer's send half closed, while
     * this records that an extra-bytes/re-fed teardown is owed on this bidi. A
     * capacity-refused teardown must retain BOTH before the empty re-feed. */
    int      ns_local_teardown_pending;
    /* The retained request bytes themselves. A length alone would let an
     * altered but still-decodable message replay to the same terminal. */
    int      buf_invalid;   /* longer than the snapshot, or absent while
                             * claiming a length: nothing is copied and the
                             * snapshot can never compare equal */
    uint8_t  buf[REQ_SEED_BUF_MAX];
} req_seed_snap_t;

#define HIST_SEED_KEY_MAX 128

typedef struct hist_seed_snap {
    int      occupied;      /* in-use records in the registry */
    int      found;
    /* The key could not be captured -- longer than the snapshot, or absent
     * while claiming a length. No bytes are copied, and a snapshot carrying it
     * can never compare equal, so an over-long length can never index the
     * fixed array. */
    int      invalid;
    int      in_use;
    int      has_largest;
    uint32_t refs;
    uint64_t largest_group;
    uint64_t largest_object;
    size_t   key_len;
    uint8_t  key[HIST_SEED_KEY_MAX];
} hist_seed_snap_t;

typedef union seed_snap {
    req_seed_snap_t  req;
    hist_seed_snap_t hist;
} seed_snap_t;

#define SEED_DIFF_U64(what, label, have, want)                                \
    do {                                                                      \
        if ((uint64_t)(have) != (uint64_t)(want)) {                           \
            TXS_DIAG("TXN %s: seeded %s %llu, expected %llu\n", (what),        \
                     (label), (unsigned long long)(have),                     \
                     (unsigned long long)(want));                             \
            failures++;                                                       \
        }                                                                     \
    } while (0)

static void req_seed_diff(const req_seed_snap_t *have,
                          const req_seed_snap_t *want, const char *what)
{
    if (have->buf_invalid || want->buf_invalid) {
        TXS_DIAG("TXN %s: retained bytes uncapturable (have %d, want %d)\n",
                 what, have->buf_invalid, want->buf_invalid);
        failures++;
        return;
    }
    SEED_DIFF_U64(what, "pool occupancy", have->busy, want->busy);
    SEED_DIFF_U64(what, "presence", have->found, want->found);
    SEED_DIFF_U64(what, "state", have->state, want->state);
    SEED_DIFF_U64(what, "role", have->role, want->role);
    SEED_DIFF_U64(what, "generation", have->generation, want->generation);
    SEED_DIFF_U64(what, "handle", have->handle, want->handle);
    SEED_DIFF_U64(what, "request id", have->request_id, want->request_id);
    SEED_DIFF_U64(what, "request stream ref", have->stream_ref,
                  want->stream_ref);
    SEED_DIFF_U64(what, "FIN latch", have->fin, want->fin);
    SEED_DIFF_U64(what, "buffered length", have->recv_len, want->recv_len);
    SEED_DIFF_U64(what, "registry kind", have->reg_kind, want->reg_kind);
    SEED_DIFF_U64(what, "registry slot", have->reg_slot, want->reg_slot);
    SEED_DIFF_U64(what, "registry has_stream_ref", have->reg_has_stream_ref,
                  want->reg_has_stream_ref);
    SEED_DIFF_U64(what, "registry stream ref", have->reg_stream_ref,
                  want->reg_stream_ref);
    SEED_DIFF_U64(what, "by-id kind", have->id_kind, want->id_kind);
    SEED_DIFF_U64(what, "by-id slot", have->id_slot, want->id_slot);
    SEED_DIFF_U64(what, "by-id has_request_id", have->id_has_request_id,
                  want->id_has_request_id);
    SEED_DIFF_U64(what, "by-id request id", have->id_request_id,
                  want->id_request_id);
    SEED_DIFF_U64(what, "got_response", have->ns_got_response,
                  want->ns_got_response);
    SEED_DIFF_U64(what, "parse_complete", have->ns_parse_complete,
                  want->ns_parse_complete);
    SEED_DIFF_U64(what, "closing_remote_error", have->ns_closing_remote_error,
                  want->ns_closing_remote_error);
    SEED_DIFF_U64(what, "local_teardown_pending",
                  have->ns_local_teardown_pending,
                  want->ns_local_teardown_pending);
    if (have->recv_len == want->recv_len && want->recv_len > 0 &&
        memcmp(have->buf, want->buf, want->recv_len) != 0) {
        TXS_DIAG("TXN %s: retained request bytes changed\n", what);
        failures++;
    }
}

/* Both registry keys for one owner, captured together so neither can be
 * retired without the other being noticed. */
static void req_seed_capture_registry(req_seed_snap_t *r,
                                      const moq_session_t *s,
                                      moq_stream_ref_t ref, uint64_t request_id)
{
    moq_request_endpoint_t ep = request_registry_find_by_streamref(s, ref);
    r->reg_kind = (int)ep.kind;
    r->reg_slot = ep.slot;
    r->reg_has_stream_ref = ep.has_stream_ref ? 1 : 0;
    r->reg_stream_ref = ep.stream_ref._v;
    moq_request_endpoint_t ip = request_registry_find_by_id(s, request_id);
    r->id_kind = (int)ip.kind;
    r->id_slot = ip.slot;
    r->id_has_request_id = ip.has_request_id ? 1 : 0;
    r->id_request_id = ip.request_id;
}

/* Append `tail` to a captured snapshot's retained bytes, refusing every
 * arithmetic that could leave the fixed buffer. An invalid base stays invalid,
 * and on any refusal nothing is added and nothing is copied -- a corrupt or
 * oversized captured length can never become a destination offset. */
static void req_seed_append(req_seed_snap_t *r, const uint8_t *tail,
                            size_t tail_len)
{
    if (r->buf_invalid || r->recv_len > REQ_SEED_BUF_MAX ||
        tail_len > REQ_SEED_BUF_MAX - r->recv_len) {
        r->buf_invalid = 1;
        return;
    }
    if (tail_len > 0) memcpy(r->buf + r->recv_len, tail, tail_len);
    r->recv_len += tail_len;
}

/* Copy the owner's retained request bytes, or mark the snapshot uncapturable.
 * Shared by every request-owner family. */
static void req_seed_copy_buf(req_seed_snap_t *r, const uint8_t *buf,
                              size_t len)
{
    if (len > REQ_SEED_BUF_MAX || (!buf && len > 0)) {
        r->buf_invalid = 1;
        return;
    }
    if (len > 0) memcpy(r->buf, buf, len);
}

static void pub_seed_capture(const moq_session_t *s, moq_stream_ref_t seed_ref,
                             seed_snap_t *o)
{
    req_seed_snap_t *r = &o->req;
    memset(r, 0, sizeof(*r));
    r->busy = pub_busy_count(s);
    const moq_pub_entry_t *e = pub_by_ref(s, seed_ref);
    r->found = e != NULL;
    if (e) {
        r->state      = (int)e->state;
        r->role       = (int)e->role;
        r->generation = e->generation;
        r->handle     = e->handle._opaque;
        r->request_id = e->request_id;
        r->stream_ref = e->request_stream_ref._v;
        r->fin        = e->req_recv_fin ? 1 : 0;
        r->recv_len   = e->req_recv_len;
        req_seed_copy_buf(r, e->req_recv_buf, e->req_recv_len);
    }
    /* Both keys: a request-stream owner is stream-ref keyed and, once its
     * request id has been committed and consumed, absent from the by-ID index. */
    req_seed_capture_registry(r, s, seed_ref, r->request_id);
}

static void pub_seed_check(const moq_session_t *s, moq_stream_ref_t seed_ref,
                           const seed_snap_t *want, const char *what)
{
    seed_snap_t now;
    pub_seed_capture(s, seed_ref, &now);
    req_seed_diff(&now.req, &want->req, what);
}

static void ann_seed_capture(const moq_session_t *s, moq_stream_ref_t seed_ref,
                             seed_snap_t *o)
{
    req_seed_snap_t *r = &o->req;
    memset(r, 0, sizeof(*r));
    r->busy = ann_busy_count(s);
    const moq_ann_entry_t *e = ann_by_ref(s, seed_ref);
    r->found = e != NULL;
    if (e) {
        r->state      = (int)e->state;
        r->role       = (int)e->role;
        r->generation = e->generation;
        r->handle     = e->handle._opaque;
        r->request_id = e->request_id;
        r->stream_ref = e->request_stream_ref._v;
        r->fin        = e->req_recv_fin ? 1 : 0;
        r->recv_len   = e->req_recv_len;
        req_seed_copy_buf(r, e->req_recv_buf, e->req_recv_len);
    }
    /* Both keys: a request-stream owner is stream-ref keyed and, once its
     * request id has been committed and consumed, absent from the by-ID index. */
    req_seed_capture_registry(r, s, seed_ref, r->request_id);
}

static void ann_seed_check(const moq_session_t *s, moq_stream_ref_t seed_ref,
                           const seed_snap_t *want, const char *what)
{
    seed_snap_t now;
    ann_seed_capture(s, seed_ref, &now);
    req_seed_diff(&now.req, &want->req, what);
}

/* The single pinned history record, identified by its own key bytes rather
 * than by slot index, so a record replaced in place cannot pass. */
static void hist_seed_capture(const moq_session_t *s, moq_stream_ref_t seed_ref,
                              seed_snap_t *o)
{
    (void)seed_ref;
    hist_seed_snap_t *h = &o->hist;
    memset(h, 0, sizeof(*h));
    for (size_t i = 0; i < s->th_cap; i++) {
        const moq_track_hist_t *r = &s->track_hist[i];
        if (!r->in_use) continue;
        h->occupied++;
        if (h->found) continue;
        h->found          = 1;
        h->in_use         = 1;
        h->has_largest    = r->has_largest ? 1 : 0;
        h->refs           = r->refs;
        h->largest_group  = r->largest_group;
        h->largest_object = r->largest_object;
        if (r->key_len > HIST_SEED_KEY_MAX || (!r->key && r->key_len > 0)) {
            h->invalid = 1;          /* copy nothing; key_len stays 0 */
        } else {
            h->key_len = r->key_len;
            if (r->key_len > 0) memcpy(h->key, r->key, r->key_len);
        }
    }
}

static void hist_seed_check(const moq_session_t *s, moq_stream_ref_t seed_ref,
                            const seed_snap_t *want, const char *what)
{
    seed_snap_t now;
    hist_seed_capture(s, seed_ref, &now);
    const hist_seed_snap_t *h = &now.hist, *w = &want->hist;
    /* An uncapturable key is never comparable: fail before any byte compare so
     * a length that outran the snapshot cannot index it. */
    if (h->invalid || w->invalid) {
        TXS_DIAG("TXN %s: seeded history key uncapturable (have %d, want %d)\n",
                 what, h->invalid, w->invalid);
        failures++;
        return;
    }
    SEED_DIFF_U64(what, "history occupancy", h->occupied, w->occupied);
    SEED_DIFF_U64(what, "history presence", h->found, w->found);
    SEED_DIFF_U64(what, "history in_use", h->in_use, w->in_use);
    SEED_DIFF_U64(what, "history has_largest", h->has_largest, w->has_largest);
    SEED_DIFF_U64(what, "history refs", h->refs, w->refs);
    SEED_DIFF_U64(what, "history largest group", h->largest_group,
                  w->largest_group);
    SEED_DIFF_U64(what, "history largest object", h->largest_object,
                  w->largest_object);
    SEED_DIFF_U64(what, "history key length", h->key_len, w->key_len);
    if (h->key_len == w->key_len && w->key_len > 0 &&
        memcmp(h->key, w->key, w->key_len) != 0) {
        TXS_DIAG("TXN %s: seeded history key bytes changed\n", what);
        failures++;
    }
}

/* Seeding and conservation are ONE family-owned object, so a case cannot pair
 * one family's capture with another's check: it names the bundle or nothing. A
 * case that establishes no prior state carries no bundle at all. */
typedef struct seed_ops {
    void (*seed)(moq_session_t *s, moq_stream_ref_t seed_ref);
    void (*capture)(const moq_session_t *s, moq_stream_ref_t seed_ref,
                    seed_snap_t *out);
    void (*check)(const moq_session_t *s, moq_stream_ref_t seed_ref,
                  const seed_snap_t *want, const char *what);
} seed_ops_t;

static const seed_ops_t pub_seed_ops = {
    seed_publication, pub_seed_capture, pub_seed_check
};
static const seed_ops_t ann_seed_ops = {
    seed_announcement, ann_seed_capture, ann_seed_check
};
static const seed_ops_t hist_seed_ops = {
    seed_track_history, hist_seed_capture, hist_seed_check
};

/* -- The staging owner ------------------------------------------------ */

/* The receiving staging entry that carries an inbound request before its
 * destination owner exists. On a retryable refusal it and its buffered bytes
 * must survive for replay; after a successful rejection its registry key is
 * gone. */
static void check_staging_retained(const moq_session_t *s,
                                   moq_stream_ref_t ref,
                                   const uint8_t *want_buf, size_t want_len,
                                   bool want_fin)
{
    moq_request_endpoint_t ep = request_registry_find_by_streamref(s, ref);
    MOQ_TEST_CHECK_EQ_INT((int)ep.kind, (int)MOQ_REQ_SUBSCRIPTION);
    if (ep.kind != MOQ_REQ_SUBSCRIPTION) return;
    MOQ_TEST_CHECK(ep.slot >= 0 && (size_t)ep.slot < s->sub_cap);
    if (ep.slot < 0 || (size_t)ep.slot >= s->sub_cap) return;
    const moq_sub_entry_t *e = &s->subs[ep.slot];
    MOQ_TEST_CHECK_EQ_INT((int)e->state, (int)MOQ_SUB_RECVING_REQUEST);
    MOQ_TEST_CHECK_EQ_U64(e->request_stream_ref._v, ref._v);
    MOQ_TEST_CHECK_EQ_INT(e->req_recv_fin, want_fin ? 1 : 0);
    /* The buffered request must be replayable byte for byte. */
    failures += txs_check_span_bytes(e->req_recv_buf, e->req_recv_len,
                                     want_buf, want_len,
                                     "staging retained request bytes");
}

/* Retirement is both index removal and slot release: an unindexed staging
 * slot left behind would leak. */
static void check_staging_retired(const moq_session_t *s, moq_stream_ref_t ref)
{
    check_unregistered(s, ref);
    MOQ_TEST_CHECK_EQ_INT(sub_busy_count(s), 0);
}

/* -- The branch table ------------------------------------------------- */

/* One pre-commit reject branch. The expected REQUEST_ERROR payload -- code and
 * reason phrase, or the documented absence of a reason -- is what proves the
 * intended branch answered rather than a neighbour, so it is declared here and
 * encoded independently rather than read back from the session. */
typedef struct precommit_case {
    const char *name;
    moq_session_t *(*make)(void);
    /* Establishes the exhaustion the branch reports and conserves it; NULL
     * when the branch needs no prior state. */
    const seed_ops_t *seed_ops;
    /* Asserts the branch will be selected, before the request is fed. */
    void        (*precondition)(moq_session_t *s);
    size_t      (*encode)(uint8_t *buf, size_t cap);
    uint64_t      err_code;
    const char   *err_reason;    /* NULL: the branch carries no reason */
    /* No destination owner exists for the rejected stream, and any owner the
     * seed created still does. */
    void        (*check_owners)(const moq_session_t *s, moq_stream_ref_t ref,
                                moq_stream_ref_t seed_ref);
} precommit_case_t;

/* A bundle must be complete in all three parts. Returns a problem count so the
 * check is self-checkable. */
static int precommit_problems(const precommit_case_t *c)
{
    int bad = 0;
    if (!c->name || !c->make || !c->encode || !c->check_owners) bad++;
    if (c->seed_ops &&
        (!c->seed_ops->seed || !c->seed_ops->capture || !c->seed_ops->check))
        bad++;
    return bad;
}

static void want_precommit_error(txs_norm_vec_t *v, const precommit_case_t *c,
                                 moq_stream_ref_t ref)
{
    if (c->err_reason)
        want_reject_error_reason(v, ref, c->err_code, c->err_reason);
    else
        want_reject_error_code(v, ref, c->err_code);
}

/* Exactly the branch's REQUEST_ERROR + FIN and nothing else -- no event, no
 * second action. */
static void check_precommit_output(moq_session_t *s, const precommit_case_t *c,
                                   moq_stream_ref_t ref, const char *what)
{
    txs_norm_vec_t got, want;
    txs_norm_init(&got);
    txs_norm_init(&want);
    collect_events(s, &pub_hooks_for_actions, &got);
    MOQ_TEST_CHECK_EQ_SIZE(got.count, (size_t)0);   /* no event, before actions */
    collect_actions(s, &pub_hooks_for_actions, &got);
    want_precommit_error(&want, c, ref);
    failures += txs_norm_equals(&got, &want, what);
    txs_norm_free(&got);
    txs_norm_free(&want);
}

/* One branch under one (FIN, drain-ring) state. The drain expectations are
 * DECLARED from the ring captured before the call, never read back from the
 * ring the call produced. */
static void run_precommit(const precommit_case_t *c, bool fin_in_request,
                          bool fill_ring)
{
    char what[128];
    moq_session_t *s = c->make();
    if (!s) return;
    moq_stream_ref_t seed_ref = moq_stream_ref_from_u64(3);
    moq_stream_ref_t ref = moq_stream_ref_from_u64(1);

    MOQ_TEST_CHECK_EQ_INT(precommit_problems(c), 0);
    if (c->seed_ops) c->seed_ops->seed(s, seed_ref);
    if (c->precondition) c->precondition(s);
    seed_snap_t seed0;
    memset(&seed0, 0, sizeof(seed0));
    if (c->seed_ops) c->seed_ops->capture(s, seed_ref, &seed0);

    if (fill_ring) fill_drain_ring(s);
    drain_snap_t ring_before;
    drain_snap(s, &ring_before);

    uint8_t m[256];
    size_t n = c->encode(m, sizeof(m));
    MOQ_TEST_CHECK(n > 0);
    moq_result_t want_rc = (fill_ring && !fin_in_request) ? MOQ_ERR_WOULD_BLOCK
                                                          : MOQ_OK;
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, m, n, fin_in_request, 1),
        (int)want_rc);

    /* No destination owner was created on either outcome. The request-staging
     * entry that keys this stream is a different owner, asserted separately
     * per outcome below. */
    c->check_owners(s, ref, seed_ref);
    if (c->seed_ops) {
        snprintf(what, sizeof(what), "%s first call", c->name);
        c->seed_ops->check(s, seed_ref, &seed0, what);
    }
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);

    if (want_rc == MOQ_ERR_WOULD_BLOCK) {
        /* Retryable: the staging entry and its buffered request survive. */
        check_staging_retained(s, ref, m, n, fin_in_request);
        drain_snap_t now;
        drain_snap(s, &now);
        snprintf(what, sizeof(what), "%s blocked", c->name);
        failures += drain_multiset_equals(&now, &ring_before, what);
        check_no_output(s, &pub_hooks_for_actions);

        /* Recovery: release one known filler reference, then replay with no
         * new bytes. The rejection now completes from the buffered request. */
        moq_stream_ref_t filler = moq_stream_ref_from_u64(0x4000);
        MOQ_TEST_CHECK(drain_ref_contains(s, filler));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, filler, NULL, 0, true, 1),
            (int)MOQ_OK);
        drain_snap_t ring_freed, ring_now;
        drain_snap_minus(&ring_before, filler, MOQ_DRAIN_NORMAL, &ring_freed);
        drain_snap(s, &ring_now);
        snprintf(what, sizeof(what), "%s filler released", c->name);
        failures += drain_multiset_equals(&ring_now, &ring_freed, what);

        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, false, 1),
            (int)MOQ_OK);
        check_staging_retired(s, ref);
        c->check_owners(s, ref, seed_ref);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        if (c->seed_ops) {
            snprintf(what, sizeof(what), "%s replay", c->name);
            c->seed_ops->check(s, seed_ref, &seed0, what);
        }
        snprintf(what, sizeof(what), "%s replayed", c->name);
        check_precommit_output(s, c, ref, what);

        drain_snap_t after_replay, want_replay;
        drain_snap(s, &after_replay);
        drain_snap_plus(&ring_freed, ref, MOQ_DRAIN_NORMAL, &want_replay);
        snprintf(what, sizeof(what), "%s replay granted", c->name);
        failures += drain_multiset_equals(&after_replay, &want_replay, what);

        /* The peer's later FIN releases it, restoring the freed ring. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
            (int)MOQ_OK);
        drain_snap_t after;
        drain_snap(s, &after);
        snprintf(what, sizeof(what), "%s replay released", c->name);
        failures += drain_multiset_equals(&after, &ring_freed, what);
        check_staging_retired(s, ref);
        c->check_owners(s, ref, seed_ref);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        if (c->seed_ops) {
            snprintf(what, sizeof(what), "%s replay late FIN", c->name);
            c->seed_ops->check(s, seed_ref, &seed0, what);
        }
        check_no_output(s, &pub_hooks_for_actions);
        moq_session_destroy(s);
        return;
    }

    /* Committed: the staging key is gone. */
    check_staging_retired(s, ref);
    snprintf(what, sizeof(what), "%s output", c->name);
    check_precommit_output(s, c, ref, what);

    drain_snap_t now, want_ring;
    drain_snap(s, &now);
    if (fin_in_request) want_ring = ring_before;
    else drain_snap_plus(&ring_before, ref, MOQ_DRAIN_NORMAL, &want_ring);
    snprintf(what, sizeof(what), "%s granted", c->name);
    failures += drain_multiset_equals(&now, &want_ring, what);

    if (!fin_in_request) {
        /* The reference is released by the peer's later FIN, restoring the
         * ring exactly and producing nothing. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
            (int)MOQ_OK);
        drain_snap_t after;
        drain_snap(s, &after);
        snprintf(what, sizeof(what), "%s released", c->name);
        failures += drain_multiset_equals(&after, &ring_before, what);
        check_staging_retired(s, ref);
        c->check_owners(s, ref, seed_ref);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        if (c->seed_ops) {
            snprintf(what, sizeof(what), "%s late FIN", c->name);
            c->seed_ops->check(s, seed_ref, &seed0, what);
        }
        check_no_output(s, &pub_hooks_for_actions);
    }
    moq_session_destroy(s);
}

/* Every branch runs the same four states: the ordinary pair, and the pair
 * whose ring is already exhausted. Only the no-FIN exhausted row may refuse. */
static void run_precommit_matrix(const precommit_case_t *c)
{
    run_precommit(c, false, false);   /* no FIN: one drain reference */
    run_precommit(c, true,  false);   /* same-call FIN: none needed */
    run_precommit(c, true,  true);    /* exhausted ring, FIN observed */
    run_precommit(c, false, true);    /* exhausted ring, no FIN */
}

/* -- The retained request-FIN carrier --------------------------------- */

/* The FIN a pre-commit rejection consults is the request bidi's OBSERVED FIN,
 * not the flag of whichever transport call happens to reach the handler. An
 * earlier FIN-bearing call refused for event capacity leaves the generic
 * staging owner holding `req_recv_fin`, and `handle_request_stream_bytes()`
 * passes that latch -- never the raw call FIN -- to the profile, so the empty
 * re-feed that completes the rejection still knows the peer's send half is
 * closed. The four-state matrix only ever completes in the SAME call, so it
 * cannot tell the two apart. This row drives the other shape: refuse first,
 * then complete from an empty re-feed whose own `fin` is false, with the drain
 * ring full throughout -- which the rejection may only survive by taking no
 * reference at all. */

/* The sentinel that occupies the single event slot. It is declared in full and
 * compared field by field -- envelope metadata included -- both while it sits
 * in the queue across the refused call and again when it is handed out, so a
 * refusal cannot corrupt or lose it silently. This fixture owns the comparison
 * rather than widening the signed PUBLISH normalizer, which models only the
 * payload fields its own rows produce.
 *
 * The reason phrase is static storage, not event scratch: the sentinel then
 * outlives the poll without borrowing from the arena, so the refusal's scratch
 * cursor stays comparable while the byte comparison still has something to
 * compare. */
#define RFIN_SENTINEL_HANDLE 0x5E17E1u
#define RFIN_SENTINEL_STATUS 0x3u
#define RFIN_SENTINEL_COUNT  0x9u
static const char k_rfin_sentinel_reason[] = "capacity sentinel";

typedef struct rfin_sentinel {
    uint32_t detail_size;
    uint64_t epoch;        /* borrow_epoch stamped into the QUEUED record */
    uint64_t handle;
    uint64_t status;
    uint64_t count;
    const uint8_t *reason;
    size_t   reason_len;
} rfin_sentinel_t;

static void rfin_push_sentinel(moq_session_t *s, rfin_sentinel_t *d)
{
    memset(d, 0, sizeof(*d));
    d->detail_size = (uint32_t)sizeof(moq_publish_finished_event_t);
    d->epoch       = s->borrow_epoch;
    d->handle      = RFIN_SENTINEL_HANDLE;
    d->status      = RFIN_SENTINEL_STATUS;
    d->count       = RFIN_SENTINEL_COUNT;
    d->reason      = (const uint8_t *)k_rfin_sentinel_reason;
    d->reason_len  = sizeof(k_rfin_sentinel_reason) - 1;

    MOQ_TEST_CHECK_EQ_SIZE(s->event_tail - s->event_head, (size_t)0);
    moq_event_t ev;
    memset(&ev, 0, sizeof(ev));
    ev.kind = MOQ_EVENT_PUBLISH_FINISHED;
    ev.detail_size = d->detail_size;
    ev.borrow_epoch = d->epoch;
    ev.u.publish_finished.pub._opaque = d->handle;
    ev.u.publish_finished.status_code = d->status;
    ev.u.publish_finished.stream_count = d->count;
    ev.u.publish_finished.reason.data = d->reason;
    ev.u.publish_finished.reason.len = d->reason_len;
    MOQ_TEST_CHECK_EQ_INT((int)push_event(s, &ev), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(s->event_tail - s->event_head, (size_t)1);
    MOQ_TEST_CHECK(event_queue_full(s));
}

/* Every declared field, against an independently supplied borrow epoch: the
 * queued record must still carry the epoch it was pushed with, while a polled
 * record carries the epoch of its TRANSFER, which the session restamps on the
 * way out (session.c, poll_events_ex). Comparing a delivered record against the
 * pre-push value would pin a fiction, so the two are asserted separately. */
static void rfin_check_sentinel(const moq_event_t *ev, const rfin_sentinel_t *d,
                                uint64_t want_epoch, const char *what)
{
    char label[160];
    MOQ_TEST_CHECK_EQ_INT((int)ev->kind, (int)MOQ_EVENT_PUBLISH_FINISHED);
    if (ev->kind != MOQ_EVENT_PUBLISH_FINISHED) return;
    MOQ_TEST_CHECK_EQ_U64((uint64_t)ev->detail_size, (uint64_t)d->detail_size);
    MOQ_TEST_CHECK_EQ_U64(ev->borrow_epoch, want_epoch);
    MOQ_TEST_CHECK_EQ_U64(ev->u.publish_finished.pub._opaque, d->handle);
    MOQ_TEST_CHECK_EQ_U64(ev->u.publish_finished.status_code, d->status);
    MOQ_TEST_CHECK_EQ_U64(ev->u.publish_finished.stream_count, d->count);
    /* A declared length with a NULL pointer fails here rather than in a copy:
     * the comparison must survive a malformed borrowed span, not crash on it. */
    snprintf(label, sizeof(label), "%s sentinel reason", what);
    failures += txs_check_span_bytes(ev->u.publish_finished.reason.data,
                                     ev->u.publish_finished.reason.len,
                                     d->reason, d->reason_len, label);
}

/* The sentinel as the queue still holds it: metadata a refused call must not
 * have touched. */
static void rfin_check_queued_sentinel(const moq_session_t *s,
                                       const rfin_sentinel_t *d,
                                       const char *what)
{
    MOQ_TEST_CHECK_EQ_SIZE(s->event_tail - s->event_head, (size_t)1);
    if (s->event_tail == s->event_head) return;
    rfin_check_sentinel(&s->events[s->event_head % s->event_cap], d, d->epoch,
                        what);
}

/* Exactly one event comes out, it is the sentinel, and the queue is then
 * empty. */
static void rfin_poll_sentinel(moq_session_t *s, const rfin_sentinel_t *d,
                               const char *what)
{
    uint64_t transfer_epoch = s->borrow_epoch;
    moq_event_t ev;
    memset(&ev, 0, sizeof(ev));
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_poll_events(s, &ev, 1), 1);
    rfin_check_sentinel(&ev, d, transfer_epoch, what);
    moq_event_cleanup(&ev);
    moq_event_t extra;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_poll_events(s, &extra, 1), 0);
    MOQ_TEST_CHECK_EQ_SIZE(s->event_tail - s->event_head, (size_t)0);
}

/* A topology declared as the pre-call graph plus exactly one edge. Building the
 * expectation this way is what stops the blocked graph from being its own
 * baseline. */
static void rfin_topology_plus_edge(const og_graph_t *base, og_graph_t *out,
                                    og_domain_t domain, uint64_t key,
                                    int kind, int slot)
{
    *out = *base;
    MOQ_TEST_CHECK(out->edge_count < OG_MAX_EDGES);
    if (out->edge_count >= OG_MAX_EDGES) {
        out->overflow = 1;   /* incomparable, and loudly so */
        return;
    }
    og_edge_t *e = &out->edges[out->edge_count++];
    memset(e, 0, sizeof(*e));
    e->domain = domain;
    e->key = key;
    e->kind = kind;
    e->slot = slot;
    e->decodable = 1;
}

/* The staging slot the refusal must occupy, declared from the pool BEFORE
 * ingress: first free slot, its generation raised to the next live one. */
static int rfin_declare_staging_slot(const moq_session_t *s, uint32_t *out_gen)
{
    for (size_t i = 0; i < s->sub_cap; i++) {
        if (s->subs[i].state != MOQ_SUB_FREE) continue;
        *out_gen = s->subs[i].generation | 1u;
        return (int)i;
    }
    MOQ_TEST_CHECK(0);   /* no free staging slot: the fixture cannot run */
    return -1;
}

/* The DECLARED staging owner inventory -- the fields listed below, against
 * values declared before the call. Deliberately not the whole
 * `moq_sub_entry_t`: unrelated subscription coverage is not this slice's. */
static void rfin_check_staging(const moq_session_t *s, moq_stream_ref_t ref,
                               int slot, uint32_t gen,
                               const uint8_t *want_buf, size_t want_len)
{
    MOQ_TEST_CHECK_EQ_INT(sub_busy_count(s), 1);
    MOQ_TEST_CHECK(slot >= 0 && (size_t)slot < s->sub_cap);
    if (slot < 0 || (size_t)slot >= s->sub_cap) return;
    const moq_sub_entry_t *e = &s->subs[slot];
    MOQ_TEST_CHECK_EQ_INT((int)e->state, (int)MOQ_SUB_RECVING_REQUEST);
    MOQ_TEST_CHECK_EQ_INT((int)e->role, (int)MOQ_SUB_ROLE_PUBLISHER);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)e->generation, (uint64_t)gen);
    MOQ_TEST_CHECK_EQ_U64(e->request_id, 0u);   /* no by-id key while receiving */
    MOQ_TEST_CHECK_EQ_U64(e->request_stream_ref._v, ref._v);
    MOQ_TEST_CHECK_EQ_INT(e->req_recv_fin, 1);  /* the carrier under test */
    failures += txs_check_span_bytes(e->req_recv_buf, e->req_recv_len,
                                     want_buf, want_len,
                                     "retained-FIN staging request bytes");
    /* Also reachable through the registry the profile actually uses. */
    check_staging_retained(s, ref, want_buf, want_len, true);
}

static void run_precommit_retained_fin(const precommit_case_t *c)
{
    char what[128];
    moq_session_t *s = make_session_events_1();
    if (!s) return;
    moq_stream_ref_t ref = moq_stream_ref_from_u64(1);

    MOQ_TEST_CHECK_EQ_INT(precommit_problems(c), 0);
    /* These two rows carry no seeded owner, so the whole-session oracle below
     * needs no seed conservation of its own. */
    MOQ_TEST_CHECK(c->seed_ops == NULL);
    if (c->precondition) c->precondition(s);

    uint32_t stage_gen = 0;
    int stage_slot = rfin_declare_staging_slot(s, &stage_gen);
    if (stage_slot < 0) { moq_session_destroy(s); return; }

    rfin_sentinel_t sentinel;
    rfin_push_sentinel(s, &sentinel);
    fill_drain_ring(s);
    drain_snap_t ring_full;
    drain_snap(s, &ring_full);

    txs_snapshot_t before;
    txs_capture(s, &ref, 1, &before);
    MOQ_TEST_CHECK_EQ_INT(before.owners[0].kind, TXS_OWNER_NONE);
    og_graph_t g, g_pre;
    og_capture(s, &g_pre);
    snprintf(what, sizeof(what), "%s retained-FIN pre", c->name);
    failures += og_check_integrity(&g_pre, what);
    failures += og_check_no_edge(&g_pre, OG_DOM_REQ_STREAMREF, ref._v, what);
    failures += og_check_no_edge(&g_pre, OG_DOM_NS_REF, ref._v, what);

    uint8_t m[256];
    size_t n = c->encode(m, sizeof(m));
    MOQ_TEST_CHECK(n > 0);

    /* The complete request arrives WITH the peer's FIN, and is refused for
     * event capacity -- not for the ring, which the rejection would not have
     * needed anyway. */
    MOQ_TEST_CHECK(event_queue_full(s));
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, m, n, true, 1),
        (int)MOQ_ERR_WOULD_BLOCK);

    /* Transactional: the only declared change is the staging owner this call
     * created, built from the pre-call free slot rather than read back. */
    txs_snapshot_t want_blocked = before;
    want_blocked.owners[0].kind = (int)MOQ_REQ_SUBSCRIPTION;
    want_blocked.owners[0].slot = stage_slot;
    want_blocked.owners[0].state = (int)MOQ_SUB_RECVING_REQUEST;
    want_blocked.owners[0].generation = stage_gen;
    snprintf(what, sizeof(what), "%s retained-FIN blocked", c->name);
    failures += txs_check_eq(s, &ref, 1, &want_blocked, what);

    rfin_check_staging(s, ref, stage_slot, stage_gen, m, n);
    c->check_owners(s, ref, moq_stream_ref_from_u64(0));
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
    {
        drain_snap_t now;
        drain_snap(s, &now);
        failures += drain_multiset_equals(&now, &ring_full, what);
    }
    rfin_check_queued_sentinel(s, &sentinel, what);
    og_graph_t g_blocked;
    og_capture(s, &g_blocked);
    failures += og_check_integrity(&g_blocked, what);
    {
        /* The whole blocked topology is the pre-call one plus exactly the
         * staging edge -- not merely that owner's own closure. */
        og_graph_t want_topo;
        rfin_topology_plus_edge(&g_pre, &want_topo, OG_DOM_REQ_STREAMREF,
                                ref._v, (int)MOQ_REQ_SUBSCRIPTION, stage_slot);
        failures += og_check_same_topology(&g_blocked, &want_topo, what);
        og_edge_spec_t want_edges[1] = {
            { OG_DOM_REQ_STREAMREF, ref._v }
        };
        failures += og_check_owner_edges(&g_blocked, (int)MOQ_REQ_SUBSCRIPTION,
                                         stage_slot, want_edges, 1, what);
        failures += og_check_edge(&g_blocked, OG_DOM_REQ_STREAMREF, ref._v,
                                  (int)MOQ_REQ_SUBSCRIPTION, stage_slot, what);
        failures += og_check_no_edge(&g_blocked, OG_DOM_NS_REF, ref._v, what);
    }

    /* Exactly the sentinel, and nothing else, comes out. No peer bytes are
     * fed to free it. */
    snprintf(what, sizeof(what), "%s retained-FIN sentinel", c->name);
    rfin_poll_sentinel(s, &sentinel, what);

    /* The empty re-feed carries fin=false. It completes only because staging
     * retained the observed FIN -- and it must take no drain reference, which
     * the still-full ring is what proves. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, false, 1),
        (int)MOQ_OK);
    snprintf(what, sizeof(what), "%s retained-FIN completed", c->name);
    check_precommit_output(s, c, ref, what);
    check_staging_retired(s, ref);
    c->check_owners(s, ref, moq_stream_ref_from_u64(0));
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
    {
        drain_snap_t now;
        drain_snap(s, &now);
        failures += drain_multiset_equals(&now, &ring_full, what);
    }
    txs_snapshot_t want_done = before;
    want_done.event_depth = 0;          /* the sentinel was polled */
    failures += txs_check_eq(s, &ref, 1, &want_done, what);
    og_capture(s, &g);
    failures += og_check_integrity(&g, what);
    failures += og_check_same_topology(&g, &g_pre, what);
    failures += og_check_no_edge(&g, OG_DOM_REQ_STREAMREF, ref._v, what);
    failures += og_check_no_edge(&g, OG_DOM_NS_REF, ref._v, what);
    failures += og_check_owner_unreferenced(&g, (int)MOQ_REQ_SUBSCRIPTION,
                                            stage_slot, what);

    /* A second empty re-feed is silent and changes nothing -- including in the
     * domains a production lookup cannot see, which is what makes the claim
     * cover hidden and unreachable edges. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, false, 1),
        (int)MOQ_OK);
    snprintf(what, sizeof(what), "%s retained-FIN idempotent", c->name);
    check_no_output(s, &pub_hooks_for_actions);
    check_staging_retired(s, ref);
    c->check_owners(s, ref, moq_stream_ref_from_u64(0));
    failures += txs_check_eq(s, &ref, 1, &want_done, what);
    {
        drain_snap_t now;
        drain_snap(s, &now);
        failures += drain_multiset_equals(&now, &ring_full, what);
    }
    og_capture(s, &g);
    failures += og_check_integrity(&g, what);
    failures += og_check_same_topology(&g, &g_pre, what);
    failures += og_check_no_edge(&g, OG_DOM_REQ_STREAMREF, ref._v, what);
    failures += og_check_no_edge(&g, OG_DOM_NS_REF, ref._v, what);
    failures += og_check_owner_unreferenced(&g, (int)MOQ_REQ_SUBSCRIPTION,
                                            stage_slot, what);
    moq_session_destroy(s);
}

static void test_precommit_rejects(void)
{
    /* PUBLISH, session_publish.c:389 -- message-level auth reject. */
    static const precommit_case_t publish_auth = {
        .name = "publish auth", .make = make_session_default,
        .encode = enc_publish_unknown_alias,
        .err_code = 0x17u, .err_reason = NULL,
        .check_owners = owners_publish_none
    };
    /* PUBLISH, session_publish.c:420 -- unknown mandatory Track Property. */
    static const precommit_case_t publish_prop = {
        .name = "publish mandatory property", .make = make_session_default,
        .encode = enc_publish_mandatory_prop,
        .err_code = MOQ_REQUEST_ERROR_UNSUPPORTED_EXTENSION,
        .err_reason = "unsupported mandatory track property",
        .check_owners = owners_publish_none
    };
    /* PUBLISH, session_publish.c:451 -- publication pool exhausted. */
    static const precommit_case_t publish_pool = {
        .name = "publish pool full", .make = make_session_pub_cap_1,
        .seed_ops = &pub_seed_ops, .encode = enc_publish_second,
        .err_code = MOQ_REQUEST_ERROR_INTERNAL_ERROR,
        .err_reason = "publish pool full",
        .check_owners = owners_publish_seeded,
    };
    /* PUBLISH, session_publish.c:500 -- track-history registry exhausted. */
    static const precommit_case_t publish_hist = {
        .name = "publish track history full", .make = make_session_hist_cap_1,
        .seed_ops = &hist_seed_ops, .precondition = expect_history_full,
        .encode = enc_publish_hist_target,
        .err_code = MOQ_REQUEST_ERROR_INTERNAL_ERROR,
        .err_reason = "track history full",
        .check_owners = owners_publish_none,
    };
    /* PUBLISH_NAMESPACE, session_namespace.c:278 -- auth reject. */
    static const precommit_case_t pns_auth = {
        .name = "publish_namespace auth", .make = make_session_default,
        .encode = enc_pns_unknown_alias,
        .err_code = 0x17u, .err_reason = NULL,
        .check_owners = owners_ann_none
    };
    /* PUBLISH_NAMESPACE, session_namespace.c:310 -- pool exhausted. */
    static const precommit_case_t pns_pool = {
        .name = "publish_namespace pool full", .make = make_session_ann_cap_1,
        .seed_ops = &ann_seed_ops, .encode = enc_pns_second,
        .err_code = MOQ_REQUEST_ERROR_INTERNAL_ERROR,
        .err_reason = "announcement pool full",
        .check_owners = owners_ann_seeded,
    };

    run_precommit_matrix(&publish_auth);
    run_precommit_matrix(&publish_prop);
    run_precommit_matrix(&publish_pool);
    run_precommit_matrix(&publish_hist);
    run_precommit_matrix(&pns_auth);
    run_precommit_matrix(&pns_pool);

    /* One representative per semantic handler: the two boundaries the carrier
     * has to cross are `session_core_on_publish()` and
     * `session_core_on_publish_namespace()`, not the six branches. */
    run_precommit_retained_fin(&publish_auth);
    run_precommit_retained_fin(&pns_auth);
}

/* -- Axis 3 current-call-FIN terminals -------------------------------- */

/* These terminals close an owner the peer's message just condemned. No handoff
 * marker can be present, so the FIN they consult is the one on the call that
 * carried the message. Each reserves and adds its drain reference only when the
 * peer half is genuinely open: a peer that closed its send half in the same
 * chunk spends none, and an exhausted ring cannot refuse a same-call-FIN
 * transition that needs no reference at all. */

static size_t enc_request_update(uint8_t *buf, size_t cap, uint64_t rid)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    moq_d18_msg_params_t p = { 0 };
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_d18_encode_request_update(&w, rid, &p), (int)MOQ_OK);
    return moq_buf_writer_offset(&w);
}

/* The armed owner consumed inbound request id 0, so the update is the next one
 * in sequence. */
static size_t enc_update_next(uint8_t *buf, size_t cap)
{
    return enc_request_update(buf, cap, 2);
}

/* Drain whatever arming produced, requiring the exact counts the arm owes: a
 * stray record here would otherwise be attributed to the terminal. */
static void arm_output_exact(moq_session_t *s, uint32_t want_event_kind,
                             int want_events, int want_actions,
                             const char *what, uint64_t *out_handle)
{
    int matched = 0, other_events = 0, actions = 0;
    moq_event_t e;
    while (moq_session_poll_events(s, &e, 1) > 0) {
        if (e.kind == want_event_kind) {
            matched++;
            if (out_handle) {
                if (e.kind == MOQ_EVENT_NAMESPACE_PUBLISHED)
                    *out_handle = e.u.namespace_published.ann._opaque;
                else if (e.kind == MOQ_EVENT_SUBSCRIBE_TRACKS_REQUEST)
                    *out_handle = e.u.subscribe_tracks_request.handle._opaque;
            }
        } else {
            other_events++;
            TXS_DIAG("TXN %s arm: unexpected event kind %u\n", what,
                     (unsigned)e.kind);
        }
        moq_event_cleanup(&e);
    }
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) {
        actions++;
        moq_action_cleanup(&a);
    }
    MOQ_TEST_CHECK_EQ_INT(matched, want_events);
    MOQ_TEST_CHECK_EQ_INT(other_events, 0);
    MOQ_TEST_CHECK_EQ_INT(actions, want_actions);
}

/* An inbound PUBLISH_NAMESPACE the application accepted: a receiver-role
 * announcement in ESTABLISHED, which is the only state its bidi accepts a
 * REQUEST_UPDATE in. */
static moq_stream_ref_t arm_established_announcement(moq_session_t *s)
{
    moq_stream_ref_t ref = moq_stream_ref_from_u64(1);   /* peer-opened */
    uint8_t m[192];
    size_t n = enc_pns_full(m, sizeof(m), 0, "live", NULL);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, m, n, false, 1),
        (int)MOQ_OK);
    uint64_t h = 0;
    arm_output_exact(s, MOQ_EVENT_NAMESPACE_PUBLISHED, 1, 0, "announcement", &h);
    moq_announcement_t ann;
    ann._opaque = h;
    moq_accept_namespace_cfg_t ac;
    moq_accept_namespace_cfg_init(&ac);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_namespace(s, ann, &ac, 1),
                          (int)MOQ_OK);
    arm_output_exact(s, MOQ_EVENT_SESSION_CLOSED, 0, 1, "announcement accept",
                     NULL);
    const moq_ann_entry_t *e = ann_by_ref(s, ref);
    MOQ_TEST_CHECK(e != NULL);
    if (e) {
        MOQ_TEST_CHECK_EQ_INT((int)e->state, (int)MOQ_ANN_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT((int)e->role, (int)MOQ_ANN_ROLE_RECEIVER);
    }
    check_registered(s, ref, MOQ_REQ_ANNOUNCEMENT,
                     e ? (int)(e - s->announcements) : -1);
    return ref;
}

/* The publisher-side mirror: an accepted SUBSCRIBE_TRACKS in ESTABLISHED. */
static moq_stream_ref_t arm_established_track_sub(moq_session_t *s)
{
    moq_stream_ref_t ref = moq_stream_ref_from_u64(1);   /* peer-opened */
    st_ctx_t c = st_ctx;
    c.request_id = 0;
    MOQ_TEST_CHECK_EQ_INT((int)st_feed(s, &c, ref, false), (int)MOQ_OK);
    uint64_t h = 0;
    arm_output_exact(s, MOQ_EVENT_SUBSCRIBE_TRACKS_REQUEST, 1, 0,
                     "subscribe-tracks", &h);
    moq_track_sub_handle_t handle;
    handle._opaque = h;
    moq_accept_subscribe_tracks_cfg_t ac;
    moq_accept_subscribe_tracks_cfg_init(&ac);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_accept_subscribe_tracks(s, handle, &ac, 1), (int)MOQ_OK);
    arm_output_exact(s, MOQ_EVENT_SESSION_CLOSED, 0, 1,
                     "subscribe-tracks accept", NULL);
    const moq_track_sub_entry_t *e = st_by_ref(s, ref);
    MOQ_TEST_CHECK(e != NULL);
    if (e) {
        MOQ_TEST_CHECK_EQ_INT((int)e->state, (int)MOQ_TRACK_SUB_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT((int)e->role, (int)MOQ_TRACK_SUB_ROLE_PUBLISHER);
    }
    check_registered(s, ref, MOQ_REQ_SUBSCRIBE_TRACKS,
                     e ? (int)(e - s->track_subs) : -1);
    return ref;
}

/* Both helpers answer with the same NOT_SUPPORTED REQUEST_ERROR + FIN, encoded
 * independently of the product. */
static void want_update_rejected(txs_norm_vec_t *v, moq_stream_ref_t ref)
{
    want_reject_error(v, ref);
}

static void check_ann_terminal_retired(const moq_session_t *s,
                                       moq_stream_ref_t ref)
{
    MOQ_TEST_CHECK_EQ_INT(ann_busy_count(s), 0);
    MOQ_TEST_CHECK(ann_by_ref(s, ref) == NULL);
    check_unregistered(s, ref);
}

static void check_st_terminal_retired(const moq_session_t *s,
                                      moq_stream_ref_t ref)
{
    MOQ_TEST_CHECK_EQ_INT(st_busy_count(s), 0);
    MOQ_TEST_CHECK(st_by_ref(s, ref) == NULL);
    check_unregistered(s, ref);
}

static void st_owner_capture(const moq_session_t *s, moq_stream_ref_t ref,
                             seed_snap_t *o)
{
    req_seed_snap_t *r = &o->req;
    memset(r, 0, sizeof(*r));
    r->busy = st_busy_count(s);
    const moq_track_sub_entry_t *e = st_by_ref(s, ref);
    r->found = e != NULL;
    if (e) {
        r->state      = (int)e->state;
        r->role       = (int)e->role;
        r->generation = e->generation;
        r->handle     = e->handle._opaque;
        r->request_id = e->request_id;
        r->stream_ref = e->request_stream_ref._v;
        r->fin        = e->req_recv_fin ? 1 : 0;
        r->recv_len   = e->req_recv_len;
        req_seed_copy_buf(r, e->req_recv_buf, e->req_recv_len);
    }
    req_seed_capture_registry(r, s, ref, r->request_id);
}

static void st_owner_check(const moq_session_t *s, moq_stream_ref_t ref,
                           const seed_snap_t *want, const char *what)
{
    seed_snap_t now;
    st_owner_capture(s, ref, &now);
    req_seed_diff(&now.req, &want->req, what);
}

/* The owner a terminal condemns, and everything that must survive a refusal. */
typedef struct owner_ops {
    void (*capture)(const moq_session_t *s, moq_stream_ref_t ref,
                    seed_snap_t *out);
    void (*check)(const moq_session_t *s, moq_stream_ref_t ref,
                  const seed_snap_t *want, const char *what);
    void (*check_retired)(const moq_session_t *s, moq_stream_ref_t ref);
} owner_ops_t;

static const owner_ops_t ann_owner_ops = {
    ann_seed_capture, ann_seed_check, check_ann_terminal_retired
};
static const owner_ops_t st_owner_ops = {
    st_owner_capture, st_owner_check, check_st_terminal_retired
};

/* Durable state outside the generic snapshot, captured once and compared in
 * both directions. */
typedef struct extra_ops {
    void (*capture)(const moq_session_t *s);
    int  (*check_blocked)(const moq_session_t *s, const char *what);
    int  (*check_committed)(const moq_session_t *s, const char *what);
} extra_ops_t;

/* One Axis 3 terminal reached by one inbound message. */
/* The owner's exact edge set plus the identity the runner needs to prove
 * COMPLETE retirement -- no edge anywhere still points at it -- without knowing
 * the family. `owner_slot` is a pointer because the arm declares the slot. */
typedef struct terminal_graph_ops {
    int (*owner_edges)(const og_graph_t *g, moq_stream_ref_t ref,
                       const char *what);
    int         owner_kind;
    const int  *owner_slot;
} terminal_graph_ops_t;

/* A recognized, non-empty request family. MOQ_REQ_NONE would make every
 * owner-target scan pass vacuously, so it is rejected outright. */
static int terminal_graph_kind_ok(int kind)
{
    switch (kind) {
    case MOQ_REQ_SUBSCRIPTION:
    case MOQ_REQ_SUBSCRIPTION_UPDATE:
    case MOQ_REQ_ANNOUNCEMENT:
    case MOQ_REQ_NAMESPACE_SUB:
    case MOQ_REQ_FETCH:
    case MOQ_REQ_PUBLISH:
    case MOQ_REQ_PUBLICATION_UPDATE:
    case MOQ_REQ_TRACK_STATUS:
    case MOQ_REQ_SUBSCRIBE_TRACKS:
        return 1;
    default:
        return 0;
    }
}

typedef struct terminal_case {
    const char *name;
    moq_session_t *(*make)(void);
    /* Establishes the committed owner in the state the terminal requires and
     * RETURNS its stream ref: a locally-opened request bidi gets its ref from
     * the transport action, so the runner must not assume one. */
    moq_stream_ref_t (*arm)(moq_session_t *s);
    /* The message whose arrival drives the terminal. */
    size_t      (*encode)(uint8_t *buf, size_t cap);
    /* The reason the retirement reserves, DECLARED rather than read back. */
    uint8_t       drain_reason;
    /* Everything the terminal owes, events first then actions. Some terminals
     * surface an event and some do not, so the runner cannot assume either. */
    void        (*want_output)(txs_norm_vec_t *v, moq_stream_ref_t ref);
    /* Normalizers for exactly the records this terminal can produce; an
     * unexpected kind fails rather than being counted. */
    const txs_op_hooks_t *hooks;
    const owner_ops_t *owner;
    /* Optional, and indivisible: durable state the generic snapshot does not
     * model -- data streams, their counters and deadlines, the send buffer.
     * A refusal must leave it untouched and a completion must leave it in the
     * committed shape, so the two comparisons ship together with the capture
     * that feeds them. Declaring any one alone is a descriptor error. */
    const extra_ops_t *extra;
    /* Graph conservation is an EXPLICIT, VALIDATED capability, and its three
     * dependent members live in ONE descriptor so none can be dropped while the
     * rest stay valid: a NULL `graph` disables the capability, a non-NULL one
     * must be complete and its kind supported. When present, the runner applies
     * the same integrity, exact-owner-edge and topology conservation as the
     * durable runner -- at arm, under blockage, after the filler release, and
     * after every retirement. Lookup-based registry fields prove a key
     * resolves, never that no duplicate, hidden or unreachable edge exists. */
    const terminal_graph_ops_t *graph;
} terminal_case_t;

static int terminal_problems(const terminal_case_t *c)
{
    int bad = 0;
    if (!c->name || !c->make || !c->arm || !c->encode || !c->want_output) bad++;
    /* Output-only use: the runner needs both normalizers and nothing else, so
     * the full transactional hook set is deliberately not required here. */
    if (!c->hooks || !c->hooks->normalize_event ||
        !c->hooks->normalize_action) bad++;
    if (!c->owner || !c->owner->capture || !c->owner->check ||
        !c->owner->check_retired) bad++;
    if (c->extra && (!c->extra->capture || !c->extra->check_blocked ||
                     !c->extra->check_committed)) bad++;
    if (c->drain_reason != MOQ_DRAIN_NORMAL &&
        c->drain_reason != MOQ_DRAIN_GOAWAY_STRICT) bad++;
    /* All-or-nothing: a declared capability must be COMPLETE, and its kind must
     * be a real family -- MOQ_REQ_NONE (the value a deleted initializer leaves
     * behind) would make the owner-target retirement scan pass vacuously. */
    if (c->graph) {
        if (!c->graph->owner_edges || !c->graph->owner_slot) bad++;
        if (!terminal_graph_kind_ok(c->graph->owner_kind)) bad++;
    }
    return bad;
}

/* No edge in ANY domain still keys on the retired stream, and no edge anywhere
 * still points at the owner slot it occupied. */
static int check_terminal_owner_retired_edges(const moq_session_t *s,
                                              const terminal_case_t *c,
                                              moq_stream_ref_t ref,
                                              const char *what)
{
    if (!c->graph) return 0;
    og_graph_t g;
    og_capture(s, &g);
    int bad = og_check_integrity(&g, what);
    bad += og_check_no_edge(&g, OG_DOM_REQ_STREAMREF, ref._v, what);
    bad += og_check_no_edge(&g, OG_DOM_NS_REF, ref._v, what);
    if (*c->graph->owner_slot >= 0)
        bad += og_check_owner_edges(&g, c->graph->owner_kind,
                                    *c->graph->owner_slot, NULL, 0, what);
    return bad;
}

static void check_terminal_output(moq_session_t *s, const terminal_case_t *c,
                                  moq_stream_ref_t ref, const char *what)
{
    txs_norm_vec_t got, want;
    txs_norm_init(&got);
    txs_norm_init(&want);
    collect_events(s, c->hooks, &got);
    collect_actions(s, c->hooks, &got);
    c->want_output(&want, ref);
    failures += txs_norm_equals(&got, &want, what);
    txs_norm_free(&got);
    txs_norm_free(&want);
}

/* One terminal under one (FIN, drain-ring) state. Drain expectations are
 * declared from the ring captured before the call. */
static void run_terminal(const terminal_case_t *c, bool fin_in_call,
                         bool fill_ring, size_t split)
{
    char what[128];
    moq_session_t *s = c->make();
    if (!s) return;
    MOQ_TEST_CHECK_EQ_INT(terminal_problems(c), 0);
    moq_stream_ref_t ref = c->arm(s);
    MOQ_TEST_CHECK(ref._v != 0);
    if (ref._v == 0) { moq_session_destroy(s); return; }

    uint8_t m[192];
    size_t n = c->encode(m, sizeof(m));
    MOQ_TEST_CHECK(n > 0);
    MOQ_TEST_CHECK(split < n);
    if (split >= n) { moq_session_destroy(s); return; }

    /* A fragmented message: the head is retained on the owner and no terminal
     * runs, so the snapshot taken next already carries a real prefix and the
     * failing call contributes only the suffix. */
    if (split > 0) {
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, m, split, false, 1),
            (int)MOQ_OK);
        check_no_output(s, c->hooks);
    }

    seed_snap_t owner0;
    memset(&owner0, 0, sizeof(owner0));
    c->owner->capture(s, ref, &owner0);
    /* The baseline must be the bytes the fixture actually fed. Adopting
     * whatever the head left would let corruption during the first feed become
     * the expectation the blocked path is then measured against. */
    MOQ_TEST_CHECK_EQ_SIZE(owner0.req.recv_len, split);
    MOQ_TEST_CHECK_EQ_INT(owner0.req.buf_invalid, 0);
    if (!owner0.req.buf_invalid && owner0.req.recv_len == split && split > 0)
        MOQ_TEST_CHECK(memcmp(owner0.req.buf, m, split) == 0);

    if (fill_ring) fill_drain_ring(s);
    drain_snap_t ring_before;
    drain_snap(s, &ring_before);
    if (c->extra) c->extra->capture(s);

    /* The generic transactional snapshot: a refused terminal must leave the
     * WHOLE session as it found it, not merely its own owner. */
    txs_snapshot_t before;
    txs_capture(s, &ref, 1, &before);
    expect_after_call_prepare(&before);

    og_graph_t graph_before;
    og_capture(s, &graph_before);
    if (c->graph) {
        snprintf(what, sizeof(what), "%s armed", c->name);
        failures += og_check_integrity(&graph_before, what);
        failures += c->graph->owner_edges(&graph_before, ref, what);
    }

    const uint8_t *tail = m + split;
    size_t tail_len = n - split;
    moq_result_t want_rc = (fill_ring && !fin_in_call) ? MOQ_ERR_WOULD_BLOCK
                                                       : MOQ_OK;
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, tail, tail_len,
                                              fin_in_call, 1),
        (int)want_rc);
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);

    if (want_rc == MOQ_ERR_WOULD_BLOCK) {
        /* Retryable: the condemned owner survives whole apart from ONE declared
         * delta -- the suffix was buffered onto that same owner before the
         * terminal ran, and the refusal deliberately keeps the complete message
         * for replay. The pre-commit tranche avoids the delta by pre-admitting
         * its request first, which works there because the bytes land on a
         * SEPARATE staging owner. Here they land on the very owner under test,
         * so no admission order removes them, whatever gate refuses -- some of
         * these terminals owe an event and some do not. The expectation is
         * therefore the captured prefix followed by this call's suffix, byte
         * for byte. */
        seed_snap_t owner_blocked = owner0;
        req_seed_append(&owner_blocked.req, tail, tail_len);
        snprintf(what, sizeof(what), "%s blocked", c->name);
        c->owner->check(s, ref, &owner_blocked, what);
        failures += txs_check_eq(s, &ref, 1, &before, what);
        if (c->extra) failures += c->extra->check_blocked(s, what);
        if (c->graph) {
            /* Not merely a valid graph: the SAME graph. An edge quietly
             * removed or repointed is still internally consistent. */
            og_graph_t g_now;
            og_capture(s, &g_now);
            failures += og_check_integrity(&g_now, what);
            failures += c->graph->owner_edges(&g_now, ref, what);
            failures += og_check_same_topology(&g_now, &graph_before, what);
        }
        drain_snap_t now;
        drain_snap(s, &now);
        failures += drain_multiset_equals(&now, &ring_before, what);
        check_no_output(s, c->hooks);

        /* Release one known filler, then replay with no new bytes. */
        moq_stream_ref_t filler = moq_stream_ref_from_u64(0x4000);
        MOQ_TEST_CHECK(drain_ref_contains(s, filler));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, filler, NULL, 0, true, 1),
            (int)MOQ_OK);
        drain_snap_t ring_freed, ring_now;
        drain_snap_minus(&ring_before, filler, MOQ_DRAIN_NORMAL, &ring_freed);
        drain_snap(s, &ring_now);
        snprintf(what, sizeof(what), "%s filler released", c->name);
        failures += drain_multiset_equals(&ring_now, &ring_freed, what);
        if (c->graph) {
            /* Releasing an unrelated filler must not have touched the target
             * owner or the graph the replay is about to consume. */
            c->owner->check(s, ref, &owner_blocked, what);
            og_graph_t g;
            og_capture(s, &g);
            failures += og_check_integrity(&g, what);
            failures += c->graph->owner_edges(&g, ref, what);
            failures += og_check_same_topology(&g, &graph_before, what);
        }

        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, false, 1),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        c->owner->check_retired(s, ref);
        snprintf(what, sizeof(what), "%s replayed", c->name);
        check_terminal_output(s, c, ref, what);
        if (c->extra) failures += c->extra->check_committed(s, what);
        failures += check_terminal_owner_retired_edges(s, c, ref, what);

        drain_snap_t after_replay, want_replay;
        drain_snap(s, &after_replay);
        drain_snap_plus(&ring_freed, ref, c->drain_reason, &want_replay);
        snprintf(what, sizeof(what), "%s replay granted", c->name);
        failures += drain_multiset_equals(&after_replay, &want_replay, what);
        check_drain_ref_reason(s, ref, c->drain_reason);

        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        drain_snap_t after;
        drain_snap(s, &after);
        snprintf(what, sizeof(what), "%s replay released", c->name);
        failures += drain_multiset_equals(&after, &ring_freed, what);
        c->owner->check_retired(s, ref);
        check_no_output(s, c->hooks);
        failures += check_terminal_owner_retired_edges(s, c, ref, what);
        c->owner->check_retired(s, ref);   /* no resurrection */
        moq_session_destroy(s);
        return;
    }

    c->owner->check_retired(s, ref);
    snprintf(what, sizeof(what), "%s output", c->name);
    check_terminal_output(s, c, ref, what);
    if (c->extra) failures += c->extra->check_committed(s, what);
    failures += check_terminal_owner_retired_edges(s, c, ref, what);

    drain_snap_t now, want_ring;
    drain_snap(s, &now);
    if (fin_in_call) {
        want_ring = ring_before;
    } else {
        drain_snap_plus(&ring_before, ref, c->drain_reason, &want_ring);
        check_drain_ref_reason(s, ref, c->drain_reason);
    }
    snprintf(what, sizeof(what), "%s granted", c->name);
    failures += drain_multiset_equals(&now, &want_ring, what);

    if (!fin_in_call) {
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        drain_snap_t after;
        drain_snap(s, &after);
        snprintf(what, sizeof(what), "%s released", c->name);
        failures += drain_multiset_equals(&after, &ring_before, what);
        c->owner->check_retired(s, ref);
        check_no_output(s, c->hooks);
    }
    moq_session_destroy(s);
}

/* The four (FIN, ring) states, delivered whole; then the no-FIN exhausted-ring
 * state again with the message split, so the retained-byte oracle compares a
 * genuine prefix-plus-suffix rather than one whole call's input. */
static void run_terminal_matrix(const terminal_case_t *c, size_t split)
{
    run_terminal(c, false, false, 0);   /* no FIN: one releasable reference */
    run_terminal(c, true,  false, 0);   /* same-call FIN: none needed */
    run_terminal(c, true,  true,  0);   /* exhausted ring, FIN observed */
    run_terminal(c, false, true,  0);   /* exhausted ring, no FIN */
    run_terminal(c, false, true,  split);            /* fragmented */
}

/* -- Request-stream GOAWAY (§10.4) ------------------------------------ */

/* A GOAWAY on a committed request bidi migrates that one request: it surfaces
 * REQUEST_GOAWAY, closes our send half where it is still open, frees the entry
 * and retires the ref with a GOAWAY-STRICT reference only when the peer half is
 * still open. The caller passes the observed peer-FIN fact through, so a GOAWAY
 * that rode the same-call FIN owes no drain. */

#define GOAWAY_TIMEOUT_MS 4200

static size_t enc_goaway_request(uint8_t *buf, size_t cap)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_d18_encode_goaway_request(&w, NULL, 0, GOAWAY_TIMEOUT_MS),
        (int)MOQ_OK);
    return moq_buf_writer_offset(&w);
}

/* The handle the arm surfaced, so the expected event can be built from what the
 * fixture knows rather than from the event under test. */
static uint64_t g_goaway_handle;
static moq_request_family_t g_goaway_family;

static bool goaway_norm_event(const moq_event_t *ev, void *vctx,
                              txs_norm_vec_t *out)
{
    (void)vctx;
    txs_img_t im;
    txs_img_init(&im);
    if (ev->kind != MOQ_EVENT_REQUEST_GOAWAY) {
        TXS_DIAG("TXN goaway: unnormalized event kind %u\n",
                 (unsigned)ev->kind);
        return false;
    }
    txs_img_u64(&im, (uint64_t)ev->u.request_goaway.family);
    txs_img_u64(&im, ev->u.request_goaway.handle.raw);
    txs_img_u64(&im, ev->u.request_goaway.timeout_ms);
    txs_img_bytes(&im, ev->u.request_goaway.new_session_uri.data,
                  ev->u.request_goaway.new_session_uri.len);
    return txs_norm_append_img(out, ev->kind, &im);
}

static txs_op_hooks_t goaway_hooks = {
    NULL, NULL, NULL, NULL, goaway_norm_event, nss_norm_action
};

/* The event, then the close of our own send half. */
static void want_goaway_output(txs_norm_vec_t *v, moq_stream_ref_t ref)
{
    txs_img_t im;
    txs_img_init(&im);
    txs_img_u64(&im, (uint64_t)g_goaway_family);
    txs_img_u64(&im, g_goaway_handle);
    txs_img_u64(&im, GOAWAY_TIMEOUT_MS);
    txs_img_bytes(&im, NULL, 0);          /* no New Session URI */
    MOQ_TEST_CHECK(txs_norm_append_img(v, MOQ_EVENT_REQUEST_GOAWAY, &im));
    want_close_bidi(v, ref);
}

/* An accepted inbound PUBLISH_NAMESPACE: a committed, peer-opened request bidi,
 * which is the route d18_process_request_stream takes. */
static moq_stream_ref_t arm_goaway_announcement(moq_session_t *s)
{
    moq_stream_ref_t ref = arm_established_announcement(s);
    const moq_ann_entry_t *e = ann_by_ref(s, ref);
    MOQ_TEST_CHECK(e != NULL);
    g_goaway_handle = e ? e->handle._opaque : 0;
    g_goaway_family = MOQ_REQUEST_FAMILY_ANNOUNCEMENT;
    return ref;
}

/* The response-route caller carries a New Session URI, so that field is
 * discriminated rather than always empty. A server must reject a non-zero URI
 * (§10.4), so this row runs on a CLIENT session. */
static const uint8_t k_goaway_uri[] = { 'm','o','q',':','/','/','a','l','t' };

static size_t enc_goaway_request_uri(uint8_t *buf, size_t cap)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_d18_encode_goaway_request(&w, k_goaway_uri,
                                           sizeof(k_goaway_uri),
                                           GOAWAY_TIMEOUT_MS),
        (int)MOQ_OK);
    return moq_buf_writer_offset(&w);
}

/* Built from the fixture's own constant, never from the event under test. */
static void want_goaway_output_uri(txs_norm_vec_t *v, moq_stream_ref_t ref)
{
    txs_img_t im;
    txs_img_init(&im);
    txs_img_u64(&im, (uint64_t)g_goaway_family);
    txs_img_u64(&im, g_goaway_handle);
    txs_img_u64(&im, GOAWAY_TIMEOUT_MS);
    txs_img_bytes(&im, k_goaway_uri, sizeof(k_goaway_uri));
    MOQ_TEST_CHECK(txs_norm_append_img(v, MOQ_EVENT_REQUEST_GOAWAY, &im));
    want_close_bidi(v, ref);
}

static moq_session_t *make_session_client(void)
{
    return make_session(MOQ_PERSPECTIVE_CLIENT);
}

static int sub_pool_busy(const moq_session_t *s)
{
    int n = 0;
    for (size_t i = 0; i < s->sub_cap; i++)
        if (s->subs[i].state != MOQ_SUB_FREE) n++;
    return n;
}

static const moq_sub_entry_t *sub_entry_by_ref(const moq_session_t *s,
                                               moq_stream_ref_t ref)
{
    moq_request_endpoint_t ep = request_registry_find_by_streamref(s, ref);
    if (ep.kind != MOQ_REQ_SUBSCRIPTION) return NULL;
    if (ep.slot < 0 || (size_t)ep.slot >= s->sub_cap) return NULL;
    return &s->subs[ep.slot];
}

static void sub_term_capture(const moq_session_t *s, moq_stream_ref_t ref,
                             seed_snap_t *o)
{
    req_seed_snap_t *r = &o->req;
    memset(r, 0, sizeof(*r));
    r->busy = sub_pool_busy(s);
    const moq_sub_entry_t *e = sub_entry_by_ref(s, ref);
    r->found = e != NULL;
    if (e) {
        r->state      = (int)e->state;
        r->role       = (int)e->role;
        r->generation = e->generation;
        r->handle     = e->handle._opaque;
        r->request_id = e->request_id;
        r->stream_ref = e->request_stream_ref._v;
        r->fin        = e->req_recv_fin ? 1 : 0;
        r->recv_len   = e->req_recv_len;
        req_seed_copy_buf(r, e->req_recv_buf, e->req_recv_len);
    }
    req_seed_capture_registry(r, s, ref, r->request_id);
}

static void sub_term_check(const moq_session_t *s, moq_stream_ref_t ref,
                           const seed_snap_t *want, const char *what)
{
    seed_snap_t now;
    sub_term_capture(s, ref, &now);
    req_seed_diff(&now.req, &want->req, what);
}

static void check_sub_terminal_retired(const moq_session_t *s,
                                       moq_stream_ref_t ref)
{
    MOQ_TEST_CHECK_EQ_INT(sub_pool_busy(s), 0);
    MOQ_TEST_CHECK(sub_entry_by_ref(s, ref) == NULL);
    check_unregistered(s, ref);
}

static const owner_ops_t sub_owner_ops = {
    sub_term_capture, sub_term_check, check_sub_terminal_retired
};

/* A locally-issued SUBSCRIBE: the request bidi is OURS, so its ref comes from
 * the transport action rather than from the fixture. Setup must leave no other
 * record behind before the operation window opens. */
static moq_stream_ref_t arm_goaway_local_subscribe(moq_session_t *s)
{
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ parts, 1 };
    sc.track_name = MOQ_BYTES_LITERAL("v0");
    moq_subscription_t h = MOQ_SUBSCRIPTION_INVALID;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscribe(s, &sc, 1, &h), (int)MOQ_OK);

    moq_stream_ref_t ref = moq_stream_ref_from_u64(0);
    int n_open = 0, n_other = 0;
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) {
        if (a.kind == MOQ_ACTION_OPEN_BIDI_STREAM) {
            n_open++;
            ref = a.u.open_bidi_stream.stream_ref;
            /* Our send half must still be OPEN, or the terminal's close of it
             * would not be owed at all. */
            MOQ_TEST_CHECK_EQ_INT(a.u.open_bidi_stream.fin ? 1 : 0, 0);
            /* And the stream must carry exactly the SUBSCRIBE, so the fixture
             * cannot silently open some other request. */
            moq_buf_reader_t rr;
            moq_buf_reader_init(&rr, a.u.open_bidi_stream.data,
                                a.u.open_bidi_stream.len);
            moq_control_envelope_t oenv;
            MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_envelope(&rr, &oenv),
                                  (int)MOQ_OK);
            MOQ_TEST_CHECK_EQ_U64(oenv.msg_type, (uint64_t)MOQ_D18_SUBSCRIBE);
            MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&rr), (size_t)0);
        } else {
            n_other++;
            TXS_DIAG("TXN local subscribe arm: unexpected action kind %u\n",
                     (unsigned)a.kind);
        }
        moq_action_cleanup(&a);
    }
    MOQ_TEST_CHECK_EQ_INT(n_open, 1);
    MOQ_TEST_CHECK_EQ_INT(n_other, 0);
    MOQ_TEST_CHECK(ref._v != 0);
    int n_ev = 0;
    moq_event_t e;
    while (moq_session_poll_events(s, &e, 1) > 0) { n_ev++; moq_event_cleanup(&e); }
    MOQ_TEST_CHECK_EQ_INT(n_ev, 0);

    /* The ABSOLUTE owner shape before the terminal runs -- not merely whatever
     * baseline the snapshot would otherwise preserve. */
    MOQ_TEST_CHECK_EQ_INT(sub_pool_busy(s), 1);
    const moq_sub_entry_t *se = sub_entry_by_ref(s, ref);
    MOQ_TEST_CHECK(se != NULL);
    if (se) {
        MOQ_TEST_CHECK_EQ_INT((int)se->state, (int)MOQ_SUB_PENDING_SUBSCRIBER);
        MOQ_TEST_CHECK_EQ_INT((int)se->role, (int)MOQ_SUB_ROLE_SUBSCRIBER);
        MOQ_TEST_CHECK_EQ_U64(se->handle._opaque, h._opaque);
        MOQ_TEST_CHECK_EQ_U64(se->request_stream_ref._v, ref._v);
        MOQ_TEST_CHECK_EQ_INT(se->req_recv_fin ? 1 : 0, 0);
        MOQ_TEST_CHECK_EQ_SIZE(se->req_recv_len, (size_t)0);
        check_registered(s, ref, MOQ_REQ_SUBSCRIPTION, (int)(se - s->subs));
    }
    g_goaway_handle = h._opaque;
    g_goaway_family = MOQ_REQUEST_FAMILY_SUBSCRIBE;
    return ref;
}

/* -- ns_sub GOAWAY: the durable-latch control ------------------------- */

/* The namespace-subscription bidi is NOT keyed in the stream-ref request
 * registry -- it is keyed by its own index AND by request id -- and its peer
 * FIN reaches the terminal as the
 * entry's durable `pending_fin` (session_namespace_sub.c:1057) rather than as a
 * call parameter. That makes this origin the control for the GOAWAY pair: the
 * fact IS available here, and the shared terminal consults it -- the ns_sub
 * origin passes `pending_fin` through, so a same-call FIN owes no drain. */

static int ns_sub_pool_busy(const moq_session_t *s)
{
    int n = 0;
    for (size_t i = 0; i < s->ns_sub_cap; i++)
        if (s->ns_subs[i].state != MOQ_NS_SUB_FREE) n++;
    return n;
}

static void nsg_owner_capture(const moq_session_t *s, moq_stream_ref_t ref,
                              seed_snap_t *o)
{
    req_seed_snap_t *r = &o->req;
    memset(r, 0, sizeof(*r));
    r->busy = ns_sub_pool_busy(s);
    const moq_ns_sub_entry_t *e = ns_by_ref(s, ref);
    r->found = e != NULL;
    if (e) {
        r->state      = (int)e->state;
        r->role       = FIN_OWNER_ROLE_NA;      /* ns_sub carries no role */
        r->generation = e->generation;
        r->handle     = e->handle._opaque;
        r->request_id = e->request_id;
        r->stream_ref = e->stream_ref._v;
        r->fin        = e->pending_fin ? 1 : 0; /* the DURABLE latch */
        r->recv_len   = e->recv_len;
        req_seed_copy_buf(r, e->recv_buf, e->recv_len);
        r->ns_got_response         = e->got_response ? 1 : 0;
        r->ns_parse_complete       = e->parse_complete ? 1 : 0;
        r->ns_closing_remote_error = e->closing_remote_error ? 1 : 0;
        r->ns_local_teardown_pending = e->local_teardown_pending ? 1 : 0;
    }
    /* The stream-ref request-registry key is deliberately empty for this
     * family, but is recorded so a stray one would be caught; the by-ID key is
     * the one this family really holds. */
    req_seed_capture_registry(r, s, ref, r->request_id);
}

static void nsg_owner_check(const moq_session_t *s, moq_stream_ref_t ref,
                            const seed_snap_t *want, const char *what)
{
    seed_snap_t now;
    nsg_owner_capture(s, ref, &now);
    req_seed_diff(&now.req, &want->req, what);
}

/* The request id the arm issued, so retirement can be checked against the
 * by-ID registry after the entry that held it is gone. */
static uint64_t g_ns_sub_request_id;

static void check_nsg_retired(const moq_session_t *s, moq_stream_ref_t ref)
{
    MOQ_TEST_CHECK_EQ_INT(ns_sub_pool_busy(s), 0);
    MOQ_TEST_CHECK(ns_by_ref(s, ref) == NULL);
    check_unregistered(s, ref);
    /* The by-ID key is the real one for this family; a leak there would
     * survive a retirement that only cleared the stream-ref index. */
    moq_request_endpoint_t ip =
        request_registry_find_by_id(s, g_ns_sub_request_id);
    MOQ_TEST_CHECK_EQ_INT((int)ip.kind, (int)MOQ_REQ_NONE);
}

static const owner_ops_t nsg_owner_ops = {
    nsg_owner_capture, nsg_owner_check, check_nsg_retired
};

/* A locally-issued SUBSCRIBE_NAMESPACE. Draft-18 rejects an empty prefix and
 * the BOTH interest, so the fixture uses a one-part prefix and NAMESPACE_STATE. */
static moq_stream_ref_t arm_goaway_local_ns_sub(moq_session_t *s)
{
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_subscribe_namespace_cfg_t nc;
    moq_subscribe_namespace_cfg_init(&nc);
    nc.track_namespace_prefix = (moq_namespace_t){ parts, 1 };
    nc.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
    moq_ns_sub_handle_t h;
    memset(&h, 0, sizeof(h));
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_subscribe_namespace(s, &nc, 1, &h), (int)MOQ_OK);

    moq_stream_ref_t ref = moq_stream_ref_from_u64(0);
    int n_open = 0, n_other = 0;
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) {
        if (a.kind == MOQ_ACTION_OPEN_BIDI_STREAM) {
            n_open++;
            ref = a.u.open_bidi_stream.stream_ref;
            MOQ_TEST_CHECK_EQ_INT(a.u.open_bidi_stream.fin ? 1 : 0, 0);
            moq_buf_reader_t rr;
            moq_buf_reader_init(&rr, a.u.open_bidi_stream.data,
                                a.u.open_bidi_stream.len);
            moq_control_envelope_t oenv;
            MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_envelope(&rr, &oenv),
                                  (int)MOQ_OK);
            MOQ_TEST_CHECK_EQ_U64(oenv.msg_type,
                                  (uint64_t)MOQ_D18_SUBSCRIBE_NAMESPACE);
            MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&rr), (size_t)0);
            /* And the request itself is the one the fixture asked for. */
            moq_bytes_t dparts[4];
            moq_d18_subscribe_namespace_t dsn;
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_d18_decode_subscribe_namespace(oenv.payload,
                    oenv.payload_len, dparts, 4, &dsn), (int)MOQ_OK);
            MOQ_TEST_CHECK_EQ_SIZE(dsn.track_namespace_prefix.count, (size_t)1);
            failures += txs_check_part_bytes(&dsn.track_namespace_prefix, 0,
                                             "live", 4,
                                             "local ns_sub decoded prefix");
            MOQ_TEST_CHECK_EQ_SIZE(dsn.params.auth_token_count, (size_t)0);
            g_ns_sub_request_id = dsn.request_id;
        } else {
            n_other++;
            TXS_DIAG("TXN local ns_sub arm: unexpected action kind %u\n",
                     (unsigned)a.kind);
        }
        moq_action_cleanup(&a);
    }
    MOQ_TEST_CHECK_EQ_INT(n_open, 1);
    MOQ_TEST_CHECK_EQ_INT(n_other, 0);
    MOQ_TEST_CHECK(ref._v != 0);
    int n_ev = 0;
    moq_event_t e;
    while (moq_session_poll_events(s, &e, 1) > 0) { n_ev++; moq_event_cleanup(&e); }
    MOQ_TEST_CHECK_EQ_INT(n_ev, 0);

    /* The absolute subscriber-side owner shape, and its index linkage. */
    MOQ_TEST_CHECK_EQ_INT(ns_sub_pool_busy(s), 1);
    const moq_ns_sub_entry_t *ne = ns_by_ref(s, ref);
    MOQ_TEST_CHECK(ne != NULL);
    if (ne) {
        MOQ_TEST_CHECK_EQ_INT((int)ne->state,
                              (int)MOQ_NS_SUB_PENDING_SUBSCRIBER);
        MOQ_TEST_CHECK_EQ_U64(ne->handle._opaque, h._opaque);
        MOQ_TEST_CHECK_EQ_U64(ne->stream_ref._v, ref._v);
        MOQ_TEST_CHECK_EQ_INT(ne->pending_fin ? 1 : 0, 0);
        MOQ_TEST_CHECK_EQ_SIZE(ne->recv_len, (size_t)0);
        MOQ_TEST_CHECK_EQ_INT(ne->got_response ? 1 : 0, 0);
        MOQ_TEST_CHECK_EQ_INT(ne->parse_complete ? 1 : 0, 0);
        MOQ_TEST_CHECK_EQ_INT(ne->closing_remote_error ? 1 : 0, 0);
        MOQ_TEST_CHECK_EQ_INT((int)ne->namespace_interest,
                              (int)MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE);
        /* The wire request id, the owner and the by-ID registry all agree. */
        MOQ_TEST_CHECK_EQ_U64(ne->request_id, g_ns_sub_request_id);
        moq_request_endpoint_t ip =
            request_registry_find_by_id(s, g_ns_sub_request_id);
        MOQ_TEST_CHECK_EQ_INT((int)ip.kind, (int)MOQ_REQ_NAMESPACE_SUB);
        MOQ_TEST_CHECK_EQ_INT(ip.slot, (int)(ne - s->ns_subs));
        MOQ_TEST_CHECK_EQ_INT(ip.has_request_id ? 1 : 0, 1);
        MOQ_TEST_CHECK_EQ_U64(ip.request_id, g_ns_sub_request_id);
    }
    /* Deliberately absent from the stream-ref request registry. */
    check_unregistered(s, ref);
    g_goaway_handle = h._opaque;
    g_goaway_family = MOQ_REQUEST_FAMILY_NS_SUB;
    return ref;
}

/* The durable-latch discriminator. FIN fixes the stream's final size, so peer
 * bytes may never follow it; the terminal is instead reached by an ACTION-
 * blocked retry, and the empty re-feed is an internal retry rather than
 * post-FIN peer data. A corrected implementation consumes the durable latch and
 * takes no drain reference at all, so the exhausted ring cannot refuse it. */
static void run_ns_goaway_latched(void)
{
    const char *what = "ns_sub goaway latched";
    moq_session_t *s = make_session_full(MOQ_PERSPECTIVE_CLIENT, 0, 1);
    if (!s) return;
    moq_stream_ref_t ref = arm_goaway_local_ns_sub(s);
    if (ref._v == 0) { moq_session_destroy(s); return; }

    /* One action slot, occupied by a benign close on an unrelated stream: the
     * terminal's own close cannot be queued, so it must refuse. */
    moq_stream_ref_t blocker = moq_stream_ref_from_u64(0x5000);
    MOQ_TEST_CHECK_EQ_INT((int)queue_close_bidi(s, blocker), (int)MOQ_OK);
    MOQ_TEST_CHECK(action_queue_full(s));

    seed_snap_t owner0;
    memset(&owner0, 0, sizeof(owner0));
    nsg_owner_capture(s, ref, &owner0);
    drain_snap_t ring_before;
    drain_snap(s, &ring_before);
    txs_snapshot_t before;
    txs_capture(s, &ref, 1, &before);
    expect_after_call_prepare(&before);

    uint8_t m[192];
    size_t n = enc_goaway_request_uri(m, sizeof(m));
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, m, n, true, 1),
        (int)MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);

    /* The FIN is now DURABLE on the owner, the GOAWAY is retained byte for
     * byte, and nothing else moved. */
    seed_snap_t owner_blocked = owner0;
    owner_blocked.req.fin = 1;
    req_seed_append(&owner_blocked.req, m, n);
    nsg_owner_check(s, ref, &owner_blocked, what);
    failures += txs_check_eq(s, &ref, 1, &before, what);
    {
        const moq_ns_sub_entry_t *ne = ns_by_ref(s, ref);
        MOQ_TEST_CHECK(ne != NULL && ne->pending_fin);
    }
    drain_snap_t now;
    drain_snap(s, &now);
    failures += drain_multiset_equals(&now, &ring_before, what);

    /* Only the blocker is queued, and no event surfaced. */
    {
        txs_norm_vec_t got, want;
        txs_norm_init(&got);
        txs_norm_init(&want);
        collect_events(s, &goaway_hooks, &got);
        MOQ_TEST_CHECK_EQ_SIZE(got.count, (size_t)0);
        collect_actions(s, &goaway_hooks, &got);
        want_close_bidi(&want, blocker);
        failures += txs_norm_equals(&got, &want, "ns_sub goaway blocked");
        txs_norm_free(&got);
        txs_norm_free(&want);
    }

    /* Capacity returns, and the ring is then exhausted: with the peer's close
     * already durable the retry needs no reference at all. */
    fill_drain_ring(s);
    drain_snap_t ring_full;
    drain_snap(s, &ring_full);

    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, false, 1),
        (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
    check_nsg_retired(s, ref);
    {
        txs_norm_vec_t got, want;
        txs_norm_init(&got);
        txs_norm_init(&want);
        collect_events(s, &goaway_hooks, &got);
        collect_actions(s, &goaway_hooks, &got);
        want_goaway_output_uri(&want, ref);
        failures += txs_norm_equals(&got, &want, "ns_sub goaway latched output");
        txs_norm_free(&got);
        txs_norm_free(&want);
    }
    drain_snap_t after;
    drain_snap(s, &after);
    failures += drain_multiset_equals(&after, &ring_full,
                                      "ns_sub goaway latched ring");
    moq_session_destroy(s);
}

/* -- ns_sub local teardown (session_namespace_sub.c:665) -------------- */

/* Draft-18 defines no post-request message on a publisher-side namespace-sub
 * bidi, so extra bytes terminate that BIDI rather than the session (§10.9.1).
 * Two landed obligations meet here and are kept apart:
 *
 *   #245(c) -- the refused call's obligation is RETAINED durably in owner state.
 *   The branch (`if (len > 0) return ns_sub_local_teardown(...)`) deliberately
 *   does NOT append those bytes to the entry; instead the refusal installs the
 *   `local_teardown_pending` marker, which carries the semantic retry, and the
 *   documented empty re-feed (len == 0) then COMPLETES the teardown through that
 *   marker. The oracle pins the marker, not raw bytes -- recv_len is not
 *   required to grow.
 *
 *   #249 -- the drain selector is FIN-aware: the cumulative peer FIN is latched
 *   into `pending_fin` BEFORE the capacity refusal, so a same-call FIN owes no
 *   drain and a genuinely open peer half owes exactly one. */

static bool nsl_norm_action(const moq_action_t *a, void *vctx,
                            txs_norm_vec_t *out)
{
    if (a->kind == MOQ_ACTION_ABORT_BIDI_STREAM) {
        txs_img_t im;
        txs_img_init(&im);
        txs_img_u64(&im, a->u.abort_bidi_stream.stream_ref._v);
        txs_img_u64(&im, a->u.abort_bidi_stream.error_code);
        return txs_norm_append_img(out, a->kind, &im);
    }
    return nss_norm_action(a, vctx, out);
}

static txs_op_hooks_t nsl_hooks = {
    NULL, NULL, NULL, NULL, no_event_expected, nsl_norm_action
};

/* One whole-stream abort carrying CANCELLED (§3.3.3), and no event. */
static void want_nsl_output(txs_norm_vec_t *v, moq_stream_ref_t ref)
{
    txs_img_t im;
    txs_img_init(&im);
    txs_img_u64(&im, ref._v);
    txs_img_u64(&im, 0x1);
    MOQ_TEST_CHECK(txs_norm_append_img(v, MOQ_ACTION_ABORT_BIDI_STREAM, &im));
}

static const uint8_t k_nsl_extra[2] = { 0xde, 0xad };

/* Exact graph conservation: the captured edge set survives unchanged (same
 * count, every prior edge present exactly once, no added edge). Used to bracket
 * the refused teardown so a stray edge created by the refused call is caught
 * before any later drain/fill can absorb it. */
static int nsl_graph_equal(const og_graph_t *g0, const moq_session_t *s,
                           const char *what)
{
    og_graph_t g1;
    og_capture(s, &g1);
    int bad = og_check_integrity(&g1, what);
    if (g1.edge_count != g0->edge_count) {
        fprintf(stderr, "NSL %s: %zu graph edges, expected %zu\n", what,
                g1.edge_count, g0->edge_count);
        bad++;
    }
    for (size_t i = 0; i < g0->edge_count; i++) {
        const og_edge_t *e = &g0->edges[i];
        size_t seen = 0;
        for (size_t j = 0; j < g1.edge_count; j++) {
            const og_edge_t *f = &g1.edges[j];
            if (f->domain == e->domain && f->key == e->key &&
                f->kind == e->kind && f->slot == e->slot)
                seen++;
        }
        if (seen != 1) {
            fprintf(stderr, "NSL %s: edge (%d key %llu) not conserved (%zu)\n",
                    what, (int)e->domain, (unsigned long long)e->key, seen);
            bad++;
        }
    }
    return bad;
}

/* An inbound SUBSCRIBE_NAMESPACE the application has not answered: a
 * publisher-side entry in PENDING_PUBLISHER, whose bidi treats further bytes as
 * a local teardown. Returns the entry's own buffered length so a later oracle
 * can require it UNCHANGED without assuming how the fix stores its obligation. */
static moq_stream_ref_t arm_pending_publisher_ns_sub_at(moq_session_t *s,
                                                        uint64_t ref_v,
                                                        uint64_t request_id,
                                                        size_t *out_recv_len)
{
    moq_stream_ref_t ref = moq_stream_ref_from_u64(ref_v);   /* peer-opened */
    nss_ctx_t c = nss_ctx;
    c.request_id = request_id;
    MOQ_TEST_CHECK_EQ_INT((int)nss_feed(s, &c, ref, false), (int)MOQ_OK);
    arm_output_exact(s, MOQ_EVENT_NS_SUB_REQUEST, 1, 0, "ns_sub request", NULL);
    MOQ_TEST_CHECK_EQ_INT(ns_sub_pool_busy(s), 1);
    const moq_ns_sub_entry_t *e = ns_by_ref(s, ref);
    MOQ_TEST_CHECK(e != NULL);
    if (out_recv_len) *out_recv_len = 0;
    if (e) {
        MOQ_TEST_CHECK_EQ_INT((int)e->state,
                              (int)MOQ_NS_SUB_PENDING_PUBLISHER);
        MOQ_TEST_CHECK_EQ_U64(e->stream_ref._v, ref._v);
        /* Absolute "false at arm" for BOTH carriers. */
        MOQ_TEST_CHECK_EQ_INT(e->pending_fin ? 1 : 0, 0);
        MOQ_TEST_CHECK_EQ_INT(e->local_teardown_pending ? 1 : 0, 0);
        g_ns_sub_request_id = e->request_id;
        if (out_recv_len) *out_recv_len = e->recv_len;
    }
    return ref;
}

static moq_stream_ref_t arm_pending_publisher_ns_sub(moq_session_t *s,
                                                     size_t *out_recv_len)
{
    return arm_pending_publisher_ns_sub_at(s, 1, 0, out_recv_len);
}

/* #245(c) -- the refused call's obligation is RETAINED, isolated with NO peer
 * FIN so the drain selector plays no part. The publisher-side branch
 * (`if (len > 0) return ns_sub_local_teardown(...)`) deliberately does NOT
 * append those bytes to the entry; the refusal installs the durable
 * `local_teardown_pending` marker, and the documented empty re-feed (len == 0)
 * COMPLETES the teardown through it. The marker carries the retry, so nothing
 * here requires recv_len to grow: only the bytes the entry ALREADY owned are
 * immutable. */
static void run_nsl_retry_obligation(void)
{
    const char *what = "ns_sub teardown retry";
    moq_session_t *s = make_session_full(MOQ_PERSPECTIVE_SERVER, 0, 1);
    if (!s) return;
    size_t armed_len = 0;
    moq_stream_ref_t ref = arm_pending_publisher_ns_sub(s, &armed_len);

    /* Fill the ONE action slot with a distinct blocker on an unrelated ref, so
     * the teardown's own abort cannot be queued. */
    moq_stream_ref_t blocker = moq_stream_ref_from_u64(0x5000);
    MOQ_TEST_CHECK_EQ_INT((int)queue_close_bidi(s, blocker), (int)MOQ_OK);
    MOQ_TEST_CHECK(action_queue_full(s));

    seed_snap_t owner0;
    memset(&owner0, 0, sizeof(owner0));
    nsg_owner_capture(s, ref, &owner0);
    drain_snap_t ring_before;
    drain_snap(s, &ring_before);
    txs_snapshot_t before;
    txs_capture(s, &ref, 1, &before);
    expect_after_call_prepare(&before);

    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, k_nsl_extra,
                                              sizeof(k_nsl_extra), false, 1),
        (int)MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);

    /* Everything about the owner is required unchanged EXCEPT the retained
     * teardown itself, whose representation the fix chooses. The bytes the
     * entry already held are compared byte for byte; only what lies beyond that
     * prefix is left free. */
    {
        seed_snap_t nowsnap;
        memset(&nowsnap, 0, sizeof(nowsnap));
        nsg_owner_capture(s, ref, &nowsnap);
        MOQ_TEST_CHECK_EQ_INT(nowsnap.req.buf_invalid, 0);
        MOQ_TEST_CHECK(nowsnap.req.recv_len >= armed_len);
        if (!nowsnap.req.buf_invalid && !owner0.req.buf_invalid &&
            nowsnap.req.recv_len >= armed_len && armed_len > 0)
            MOQ_TEST_CHECK(memcmp(nowsnap.req.buf, owner0.req.buf,
                                  armed_len) == 0);
        seed_snap_t expect = owner0;
        expect.req.recv_len = nowsnap.req.recv_len;   /* carrier: unconstrained */
        memcpy(expect.req.buf, nowsnap.req.buf, sizeof(expect.req.buf));
        /* The ONE intended mutation: the teardown obligation is now durable.
         * No FIN was fed, so the cumulative-FIN latch stays false. */
        expect.req.ns_local_teardown_pending = 1;
        expect.req.fin = 0;
        nsg_owner_check(s, ref, &expect, what);
        MOQ_TEST_CHECK_EQ_INT(ns_sub_pool_busy(s), 1);
    }
    failures += txs_check_eq(s, &ref, 1, &before, what);
    drain_snap_t now;
    drain_snap(s, &now);
    failures += drain_multiset_equals(&now, &ring_before, what);

    /* Only the blocker is queued; the teardown produced nothing yet. */
    {
        txs_norm_vec_t got, want;
        txs_norm_init(&got);
        txs_norm_init(&want);
        collect_events(s, &nsl_hooks, &got);
        MOQ_TEST_CHECK_EQ_SIZE(got.count, (size_t)0);
        collect_actions(s, &nsl_hooks, &got);
        want_close_bidi(&want, blocker);
        failures += txs_norm_equals(&got, &want, "ns_sub teardown blocked");
        txs_norm_free(&got);
        txs_norm_free(&want);
    }

    /* The documented recovery: retry with no new bytes. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, false, 1),
        (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
    check_nsg_retired(s, ref);
    {
        txs_norm_vec_t got, want;
        txs_norm_init(&got);
        txs_norm_init(&want);
        collect_events(s, &nsl_hooks, &got);
        collect_actions(s, &nsl_hooks, &got);
        want_nsl_output(&want, ref);
        failures += txs_norm_equals(&got, &want, "ns_sub teardown replayed");
        txs_norm_free(&got);
        txs_norm_free(&want);
    }
    drain_snap_t after, want_ring;
    drain_snap(s, &after);
    drain_snap_plus(&ring_before, ref, MOQ_DRAIN_NORMAL, &want_ring);
    failures += drain_multiset_equals(&after, &want_ring,
                                      "ns_sub teardown replay granted");
    check_drain_ref_reason(s, ref, MOQ_DRAIN_NORMAL);

    /* And it happens exactly once: a second empty re-feed produces nothing. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, false, 1),
        (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
    check_nsg_retired(s, ref);          /* no silent resurrection */
    check_no_output(s, &nsl_hooks);
    drain_snap_t after2;
    drain_snap(s, &after2);
    failures += drain_multiset_equals(&after2, &want_ring,
                                      "ns_sub teardown replay idempotent");
    moq_session_destroy(s);
}

/* #245(c) + #249 cumulative-FIN retention: an extra-bytes+FIN teardown that
 * blocks on its action slot must retain the FIN durably in session-owned state,
 * not just the teardown obligation. The peer's send half is already closed, so
 * the eventual abort owes NO drain -- but the empty re-feed carries fin=false,
 * so unless the FIN was latched the selector recomputes need_drain=true and, on
 * a full ring, stalls the teardown forever waiting for a terminal that already
 * arrived. This pins the durable cumulative FIN fact and the drainless
 * completion against an exhausted ring. */
static void run_nsl_fin_retry_obligation(void)
{
    const char *what = "ns_sub teardown FIN retry";
    moq_session_t *s = make_session_full(MOQ_PERSPECTIVE_SERVER, 0, 1);
    if (!s) return;
    size_t armed_len = 0;
    moq_stream_ref_t ref = arm_pending_publisher_ns_sub(s, &armed_len);

    /* Fill the ONE action slot so the teardown's own abort cannot be queued. */
    moq_stream_ref_t blocker = moq_stream_ref_from_u64(0x5000);
    MOQ_TEST_CHECK_EQ_INT((int)queue_close_bidi(s, blocker), (int)MOQ_OK);
    MOQ_TEST_CHECK(action_queue_full(s));

    /* Baselines captured BEFORE the refused call, so a wrong drain, a stray
     * graph edge, or a corrupted owned byte inserted BY that call is caught at
     * the refused point -- never absorbed into a later fill. */
    seed_snap_t owner0;
    memset(&owner0, 0, sizeof(owner0));
    nsg_owner_capture(s, ref, &owner0);
    drain_snap_t ring_before;
    drain_snap(s, &ring_before);
    txs_snapshot_t before;
    txs_capture(s, &ref, 1, &before);
    expect_after_call_prepare(&before);
    og_graph_t g0;
    og_capture(s, &g0);

    /* Extra bytes AND a peer FIN, action-blocked. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, k_nsl_extra,
                                              sizeof(k_nsl_extra), true, 1),
        (int)MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);

    /* PRE-FILL oracle: only the declared owner deltas moved, the previously
     * owned byte prefix is exact, the whole session / graph / drain multiset are
     * unchanged, and only the blocker action is queued with no event. */
    {
        seed_snap_t nowsnap;
        memset(&nowsnap, 0, sizeof(nowsnap));
        nsg_owner_capture(s, ref, &nowsnap);
        MOQ_TEST_CHECK_EQ_INT(nowsnap.req.buf_invalid, 0);
        MOQ_TEST_CHECK(nowsnap.req.recv_len >= armed_len);
        if (!nowsnap.req.buf_invalid && !owner0.req.buf_invalid &&
            nowsnap.req.recv_len >= armed_len && armed_len > 0)
            MOQ_TEST_CHECK(memcmp(nowsnap.req.buf, owner0.req.buf,
                                  armed_len) == 0);
        seed_snap_t expect = owner0;
        expect.req.recv_len = nowsnap.req.recv_len;   /* carrier: unconstrained */
        memcpy(expect.req.buf, nowsnap.req.buf, sizeof(expect.req.buf));
        expect.req.ns_local_teardown_pending = 1;
        expect.req.fin = 1;                      /* cumulative FIN, retained */
        nsg_owner_check(s, ref, &expect, "ns_sub FIN teardown blocked");
        MOQ_TEST_CHECK_EQ_INT(ns_sub_pool_busy(s), 1);
    }
    failures += txs_check_eq(s, &ref, 1, &before, "ns_sub FIN teardown blocked");
    failures += nsl_graph_equal(&g0, s, "ns_sub FIN teardown blocked");
    {
        drain_snap_t now;
        drain_snap(s, &now);
        failures += drain_multiset_equals(&now, &ring_before,
                                          "ns_sub FIN teardown blocked ring");
    }
    {
        txs_norm_vec_t got, want;
        txs_norm_init(&got);
        txs_norm_init(&want);
        collect_events(s, &nsl_hooks, &got);
        MOQ_TEST_CHECK_EQ_SIZE(got.count, (size_t)0);
        collect_actions(s, &nsl_hooks, &got);
        want_close_bidi(&want, blocker);
        failures += txs_norm_equals(&got, &want, "ns_sub FIN teardown blocked");
        txs_norm_free(&got);
        txs_norm_free(&want);
    }

    /* Only now EXHAUST the drain ring: a NORMAL drain could no longer be
     * reserved, but the retained FIN means none is owed. (The blocker action was
     * consumed by the classification above.) */
    fill_drain_ring(s);
    drain_snap_t ring_full;
    drain_snap(s, &ring_full);

    /* The documented recovery: no new bytes. It completes with exactly one abort
     * and NO drain, even against the full ring. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, false, 1),
        (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
    check_nsg_retired(s, ref);
    {
        txs_norm_vec_t got, want;
        txs_norm_init(&got);
        txs_norm_init(&want);
        collect_events(s, &nsl_hooks, &got);
        collect_actions(s, &nsl_hooks, &got);
        want_nsl_output(&want, ref);
        failures += txs_norm_equals(&got, &want, what);
        txs_norm_free(&got);
        txs_norm_free(&want);
    }
    drain_snap_t after;
    drain_snap(s, &after);
    failures += drain_multiset_equals(&after, &ring_full,
                                      "ns_sub FIN teardown no drain");

    /* Exactly once: a second empty re-feed changes nothing. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, false, 1),
        (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
    check_nsg_retired(s, ref);
    check_no_output(s, &nsl_hooks);
    drain_snap_t after2;
    drain_snap(s, &after2);
    failures += drain_multiset_equals(&after2, &ring_full,
                                      "ns_sub FIN teardown idempotent");
    moq_session_destroy(s);
}

/* The drain selector pair, run with capacity available so nothing else can
 * confound it. */
static void run_nsl_drain_selector(bool fin_in_call)
{
    char what[96];
    moq_session_t *s = make_session_default();
    if (!s) return;
    moq_stream_ref_t ref = arm_pending_publisher_ns_sub(s, NULL);

    if (fin_in_call) fill_drain_ring(s);
    drain_snap_t ring_before;
    drain_snap(s, &ring_before);

    /* Same-call FIN needs no reference, so an exhausted ring cannot refuse it;
     * without the FIN exactly one normal-drain slot must be taken. */
    moq_result_t want_rc = MOQ_OK;
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, k_nsl_extra,
                                              sizeof(k_nsl_extra),
                                              fin_in_call, 1),
        (int)want_rc);
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
    check_nsg_retired(s, ref);
    snprintf(what, sizeof(what), "ns_sub teardown selector fin=%d",
             fin_in_call ? 1 : 0);
    {
        txs_norm_vec_t got, want;
        txs_norm_init(&got);
        txs_norm_init(&want);
        collect_events(s, &nsl_hooks, &got);
        collect_actions(s, &nsl_hooks, &got);
        want_nsl_output(&want, ref);
        failures += txs_norm_equals(&got, &want, what);
        txs_norm_free(&got);
        txs_norm_free(&want);
    }
    drain_snap_t now, want_ring;
    drain_snap(s, &now);
    if (fin_in_call) want_ring = ring_before;
    else {
        drain_snap_plus(&ring_before, ref, MOQ_DRAIN_NORMAL, &want_ring);
        check_drain_ref_reason(s, ref, MOQ_DRAIN_NORMAL);
    }
    failures += drain_multiset_equals(&now, &want_ring, what);

    if (!fin_in_call) {
        /* The reference the terminal took is RELEASABLE: the peer's later FIN
         * restores the ring exactly, emits nothing, and leaves the owner gone. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        drain_snap_t after;
        drain_snap(s, &after);
        failures += drain_multiset_equals(&after, &ring_before,
                                          "ns_sub teardown released");
        check_nsg_retired(s, ref);
        check_no_output(s, &nsl_hooks);
    }
    moq_session_destroy(s);
}

/* Clear-exactly-once across slot reuse: owner A genuinely reaches the blocked
 * teardown state with BOTH local_teardown_pending=true and pending_fin=true,
 * then recovers and retires. The selective free must clear BOTH, and the two
 * facts are proven by DIFFERENT observations, stated honestly:
 *   - the freed slot is inspected DIRECTLY (before owner B arms), so both
 *     local_teardown_pending==false and pending_fin==false are proven by the
 *     free itself;
 *   - only local_teardown_pending drives the empty-refeed replay, so owner B's
 *     first empty feed being INERT (no replayed abort) is the independent proof
 *     that no stale teardown marker survived. A stale pending_fin alone would
 *     not tear owner B down; the direct field check above is what pins it. */
static void run_nsl_reuse_after_teardown(void)
{
    moq_session_t *s = make_session_full(MOQ_PERSPECTIVE_SERVER, 0, 1);
    if (!s) return;

    /* Owner A, armed then action-blocked on its FIN teardown so BOTH markers
     * become durable. */
    moq_stream_ref_t refA = arm_pending_publisher_ns_sub_at(s, 1, 0, NULL);
    int32_t slotA = moq_index_find(s->idx_ns_by_ref, s->idx_ns_mask, refA._v);
    MOQ_TEST_CHECK(slotA >= 0);

    moq_stream_ref_t blocker = moq_stream_ref_from_u64(0x5000);
    MOQ_TEST_CHECK_EQ_INT((int)queue_close_bidi(s, blocker), (int)MOQ_OK);
    MOQ_TEST_CHECK(action_queue_full(s));
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, refA, k_nsl_extra,
                                              sizeof(k_nsl_extra), true, 1),
        (int)MOQ_ERR_WOULD_BLOCK);
    {
        const moq_ns_sub_entry_t *eA = ns_by_ref(s, refA);
        MOQ_TEST_CHECK(eA != NULL);
        if (eA) {
            MOQ_TEST_CHECK_EQ_INT(eA->local_teardown_pending ? 1 : 0, 1);
            MOQ_TEST_CHECK_EQ_INT(eA->pending_fin ? 1 : 0, 1);
        }
    }

    /* Recover: drain the blocker, empty re-feed completes the teardown and
     * retires owner A; drain its abort so the next arm starts clean. */
    {
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
    }
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, refA, NULL, 0, false, 1),
        (int)MOQ_OK);
    check_nsg_retired(s, refA);

    /* The selective free itself cleared BOTH fields on the vacated slot, checked
     * directly BEFORE any fresh-bidi re-init could mask them: this is where the
     * free-entry marker-clear and pending-FIN-clear are each observable. */
    MOQ_TEST_CHECK_EQ_INT((int)s->ns_subs[slotA].state, (int)MOQ_NS_SUB_FREE);
    MOQ_TEST_CHECK_EQ_INT(s->ns_subs[slotA].local_teardown_pending ? 1 : 0, 0);
    MOQ_TEST_CHECK_EQ_INT(s->ns_subs[slotA].pending_fin ? 1 : 0, 0);

    {
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
    }

    /* Owner B reuses the freed slot (the peer's next request id is 2). The arm
     * helper re-asserts both carriers false at arm; the slot identity is the one
     * A vacated. A stale field cleared by the selective free would break either
     * assertion or the inert-feed behaviour below. */
    moq_stream_ref_t refB = arm_pending_publisher_ns_sub_at(s, 5, 2, NULL);
    int32_t slotB = moq_index_find(s->idx_ns_by_ref, s->idx_ns_mask, refB._v);
    MOQ_TEST_CHECK_EQ_INT(slotB, slotA);

    /* An empty feed on the fresh owner is inert: no teardown, no graceful close,
     * no output; the owner stays PENDING_PUBLISHER with both carriers clear. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, refB, NULL, 0, false, 1),
        (int)MOQ_OK);
    const moq_ns_sub_entry_t *eB = ns_by_ref(s, refB);
    MOQ_TEST_CHECK(eB != NULL);
    if (eB) {
        MOQ_TEST_CHECK_EQ_INT((int)eB->state,
                              (int)MOQ_NS_SUB_PENDING_PUBLISHER);
        MOQ_TEST_CHECK_EQ_INT(eB->local_teardown_pending ? 1 : 0, 0);
        MOQ_TEST_CHECK_EQ_INT(eB->pending_fin ? 1 : 0, 0);
    }
    MOQ_TEST_CHECK_EQ_INT(ns_sub_pool_busy(s), 1);
    check_no_output(s, &nsl_hooks);
    moq_session_destroy(s);
}

static void test_ns_sub_local_teardown(void)
{
    run_nsl_retry_obligation();
    run_nsl_fin_retry_obligation();
    run_nsl_drain_selector(false);
    run_nsl_drain_selector(true);
    run_nsl_reuse_after_teardown();
}

/* -- #245(b): the no-owner admission blockers ------------------------
 *
 * handle_request_stream_bytes()'s no-slot path returns MOQ_ERR_WOULD_BLOCK on
 * capacity shortfalls BEFORE any staging entry or registry owner exists.
 * Verified against the current source, the path has exactly FOUR such
 * origins (session_subscribe.c:1258-1318):
 *
 *   1. !fin with the drain ring full, at the no-slot preflight (:1271);
 *   2. pre-SETUP no-slot rejection with action_queue_avail < 2 (:1280);
 *   3. post-SETUP no-slot REQUEST_ERROR with the action queue full
 *      (queue_send_bidi -> action_queue_full);
 *   4. post-SETUP no-slot REQUEST_ERROR with action room but an insufficient
 *      remaining send-buffer tail (queue_send_bidi's retryable tail check).
 *
 * No other WOULD_BLOCK origin exists on this path: push_action after the
 * `avail >= 2` preflight cannot block, the REQUEST_ERROR encode writes a
 * 64-byte stack buffer, drain_ref_add follows its own preflight, and
 * request_goaway_already_sent cannot hold for a fresh peer ref.
 *
 * The completion contract is STORAGE-NEUTRAL: each blocked case accepts
 * exactly two outcomes -- immediate MOQ_OK with the whole terminal obligation
 * durably queued, or WOULD_BLOCK with a durable carrier that completes on the
 * documented EMPTY re-feed -- through one shared completion checker, so both
 * arms require the same exact final output, cleanup and exactly-once
 * postconditions. Any other return fails the permanent outcome validator.
 * The WOULD_BLOCK arm completes on the documented empty re-feed through its
 * durable no-slot carrier (#245b), and its cumulative FIN is retained in the
 * carrier so the completion owes no drain the peer already made unnecessary.
 *
 * Terminal obligations, exact:
 *   pre-SETUP:  STOP_BIDI_STREAM + RESET_BIDI_STREAM, both CANCELLED (0x1),
 *               no event;
 *   post-SETUP: REQUEST_ERROR(INTERNAL_ERROR 0x0, retry=0,
 *               "request pool full") + FIN on the request bidi, no event;
 *   no peer FIN: exactly one NORMAL drain ref, released by the later FIN;
 *   peer FIN:    no drain ref.
 *
 * The seed is a LEGALLY occupied one-slot subscription pool: a fragmented
 * inbound SUBSCRIBE on its own bidi, held in MOQ_SUB_RECVING_REQUEST. Its
 * slot and generation are DERIVED before ingress, its retained prefix is
 * compared byte-for-byte against the fixture's own input before it becomes a
 * baseline, and its whole declared record -- identity, role, registry
 * presence fields, forbidden by-ID/ns edges -- is conserved absolutely at
 * every stage. At the REFUSED point the target is NOT required to stay
 * ownerless (that would forbid the ownership-based repair); every allowed
 * delta is bounded instead, and complete target cleanup is required at
 * terminal completion.
 */

#define NOM_SEED_REF   1u     /* peer-opened bidi carrying the seed */
#define NOM_TARGET_REF 5u     /* peer-opened bidi carrying the target */
#define NOM_FILLER0    0x4000u /* fill_drain_ring's first declared filler */

/* Origin 4's live-announcement GOAWAY blocker identity, predeclared before it
 * becomes a baseline. `armed` is 0 for origins 1-3 (no blocker owner). */
typedef struct nom_blk {
    int      armed;
    int      slot;
    uint32_t gen;         /* live generation (| 1) after accept */
    uint64_t handle;
    uint64_t rid;         /* the committed inbound id (0) */
    uint64_t ref;         /* NOM_BLK_REF */
} nom_blk_t;

static const char *nom_reason = "request pool full";

/* A session with a ONE-slot subscription pool and per-origin capacities.
 * `establish` selects the pre-/post-SETUP arm. */
static moq_session_t *nom_make_session(bool establish, uint32_t max_actions,
                                       uint32_t send_buffer_size)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_SERVER);
    cfg.version = MOQ_VERSION_DRAFT_18;
    cfg.max_subscriptions = 1;
    if (max_actions) cfg.max_actions = max_actions;
    if (send_buffer_size) cfg.send_buffer_size = send_buffer_size;
    moq_session_t *s = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_create(&cfg, 0, &s), (int)MOQ_OK);
    if (!s) return NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(s, 0), (int)MOQ_OK);
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
    if (establish) {
        uint8_t setup[16];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, setup, sizeof(setup));
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_setup(&w), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_control_bytes(s, setup,
                                              moq_buf_writer_offset(&w), 0),
            (int)MOQ_OK);
        moq_event_t e;
        while (moq_session_poll_events(s, &e, 1) > 0) moq_event_cleanup(&e);
        while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
    }
    return s;
}

/* One complete, wire-valid d18 SUBSCRIBE at request id 0. */
static size_t nom_encode_subscribe_at(uint8_t *buf, size_t cap,
                                      uint64_t request_id)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("nom") };
    moq_namespace_t ns = { parts, 1 };
    moq_d18_msg_params_t p = { 0 };
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_d18_encode_subscribe(&w, request_id, &ns,
                                      MOQ_BYTES_LITERAL("t"), &p),
        (int)MOQ_OK);
    return moq_buf_writer_offset(&w);
}

static size_t nom_encode_subscribe(uint8_t *buf, size_t cap)
{
    return nom_encode_subscribe_at(buf, cap, 0);
}

/* The seed owner's DECLARED record. Identity (slot, generation) is derived
 * from the free pool BEFORE ingress; the retained prefix is compared against
 * the fixture's own input before this record becomes a baseline. An invalid
 * buffer (over-long, or non-empty with a NULL pointer) makes the record
 * INCOMPARABLE rather than equal-by-omission. */
typedef struct nom_seed {
    int      valid;
    int      slot;
    int      state, role;
    uint64_t request_id;
    uint32_t generation;
    uint64_t stream_ref;
    int      recv_fin;
    size_t   recv_len;
    uint8_t  recv[64];
    /* Registry presence, complete: the by-streamref endpoint's kind, slot,
     * presence flags and embedded linkage values, plus the by-ID-0 answer --
     * whose absence is an ARM/seed-only and completed-state rule; the
     * refused phase's global topology (which may hold an identity-qualified
     * target carrier at id 0) is owned by nom_graph_conserved. */
    int      ep_kind;
    int      ep_slot;
    int      ep_has_stream_ref;
    int      ep_has_request_id;
    uint64_t ep_stream_ref;       /* the endpoint's EMBEDDED linkage values */
    uint64_t ep_request_id;       /* declared zero while has_request_id==0 */
    int      byid0_kind;          /* find_by_id(0).kind: an ARM/seed-only
                                   * declaration. The REFUSED phase's global
                                   * topology is owned by nom_graph_conserved
                                   * (which admits an identity-qualified
                                   * target carrier at id 0); completion
                                   * restores absolute absence through
                                   * nom_graph_seed_only. */
} nom_seed_t;

static void nom_seed_capture(const moq_session_t *s, nom_seed_t *o)
{
    memset(o, 0, sizeof(*o));
    o->slot = -1;
    for (size_t i = 0; i < s->sub_cap; i++)
        if (s->subs[i].state != MOQ_SUB_FREE) { o->slot = (int)i; break; }
    if (o->slot < 0) return;
    const moq_sub_entry_t *e = &s->subs[o->slot];
    o->state = (int)e->state;
    o->role = (int)e->role;
    o->request_id = e->request_id;
    o->generation = e->generation;
    o->stream_ref = e->request_stream_ref._v;
    o->recv_fin = e->req_recv_fin ? 1 : 0;
    o->recv_len = e->req_recv_len;
    if (o->recv_len > sizeof(o->recv) ||
        (o->recv_len && !e->req_recv_buf))
        return;                              /* valid stays 0: incomparable */
    if (o->recv_len)
        memcpy(o->recv, e->req_recv_buf, o->recv_len);
    moq_request_endpoint_t ep = request_registry_find_by_streamref(
        s, e->request_stream_ref);
    o->ep_kind = (int)ep.kind;
    o->ep_slot = ep.slot;
    o->ep_has_stream_ref = ep.has_stream_ref ? 1 : 0;
    o->ep_has_request_id = ep.has_request_id ? 1 : 0;
    o->ep_stream_ref = ep.stream_ref._v;
    o->ep_request_id = ep.request_id;
    o->byid0_kind = (int)request_registry_find_by_id(s, 0).kind;
    o->valid = 1;
}

static int nom_seed_equal(const moq_session_t *s, const nom_seed_t *want,
                          const char *what)
{
    nom_seed_t now;
    nom_seed_capture(s, &now);
    if (!now.valid || !want->valid) {
        fprintf(stderr, "NOM %s: seed record incomparable\n", what);
        return 1;
    }
    int bad = 0;
    if (now.slot != want->slot || now.state != want->state ||
        now.role != want->role || now.request_id != want->request_id ||
        now.generation != want->generation ||
        now.stream_ref != want->stream_ref ||
        now.recv_fin != want->recv_fin ||
        now.recv_len != want->recv_len ||
        (now.recv_len && memcmp(now.recv, want->recv, now.recv_len) != 0)) {
        fprintf(stderr, "NOM %s: seed owner record changed\n", what);
        bad++;
    }
    /* by-ID-0 absence is deliberately NOT compared here: it is absolute at
     * arm and at completion (the seed-only graph forbids the rid-0 edge
     * there), while the REFUSED phase admits an identity-qualified target
     * carrier at decoded id 0 -- the seed helper must not classify that
     * target-owned edge as seed mutation. */
    if (now.ep_kind != want->ep_kind || now.ep_slot != want->ep_slot ||
        now.ep_has_stream_ref != want->ep_has_stream_ref ||
        now.ep_has_request_id != want->ep_has_request_id ||
        now.ep_stream_ref != want->ep_stream_ref ||
        now.ep_request_id != want->ep_request_id) {
        fprintf(stderr, "NOM %s: seed registry answers changed\n", what);
        bad++;
    }
    return bad;
}

/* Every field the contract claims ABSOLUTELY, asserted before the captured
 * record may become a baseline -- shared by the matrix runner and the four
 * capacity controls so neither seed contract is weaker. */
static void nom_seed_arm_assert(const nom_seed_t *seed, int want_slot)
{
    MOQ_TEST_CHECK_EQ_INT(seed->valid, 1);
    MOQ_TEST_CHECK_EQ_INT(seed->slot, want_slot);
    MOQ_TEST_CHECK_EQ_INT(seed->state, (int)MOQ_SUB_RECVING_REQUEST);
    MOQ_TEST_CHECK_EQ_INT(seed->role, (int)MOQ_SUB_ROLE_PUBLISHER);
    MOQ_TEST_CHECK_EQ_U64(seed->request_id, (uint64_t)0);
    MOQ_TEST_CHECK_EQ_U64(seed->stream_ref, (uint64_t)NOM_SEED_REF);
    MOQ_TEST_CHECK_EQ_INT(seed->recv_fin, 0);
    MOQ_TEST_CHECK_EQ_INT(seed->ep_kind, (int)MOQ_REQ_SUBSCRIPTION);
    MOQ_TEST_CHECK_EQ_INT(seed->ep_slot, want_slot);
    MOQ_TEST_CHECK_EQ_INT(seed->ep_has_stream_ref, 1);
    MOQ_TEST_CHECK_EQ_INT(seed->ep_has_request_id, 0);
    MOQ_TEST_CHECK_EQ_U64(seed->ep_stream_ref, (uint64_t)NOM_SEED_REF);
    MOQ_TEST_CHECK_EQ_U64(seed->ep_request_id, (uint64_t)0);
    MOQ_TEST_CHECK_EQ_INT(seed->byid0_kind, (int)MOQ_REQ_NONE);
}

/* The seed-only graph: exactly ONE edge in the whole session -- the seed's
 * stream-ref registry edge -- and no edge of any kind for the target. */
static int nom_graph_seed_only(const moq_session_t *s, uint64_t seed_ref,
                               int seed_slot, uint64_t target_ref,
                               const nom_blk_t *blk, const char *what)
{
    og_graph_t g;
    og_capture(s, &g);
    int bad = og_check_integrity(&g, what);
    bad += og_check_edge(&g, OG_DOM_REQ_STREAMREF, seed_ref,
                         MOQ_REQ_SUBSCRIPTION, seed_slot, what);
    const og_edge_spec_t w[] = { { OG_DOM_REQ_STREAMREF, seed_ref } };
    bad += og_check_owner_edges(&g, MOQ_REQ_SUBSCRIPTION, seed_slot, w, 1,
                                what);
    bad += og_check_no_edge(&g, OG_DOM_REQ_STREAMREF, target_ref, what);
    bad += og_check_no_edge(&g, OG_DOM_NS_REF, seed_ref, what);
    bad += og_check_no_edge(&g, OG_DOM_NS_REF, target_ref, what);
    /* The seed is the only SUBSCRIPTION owner. Origin 4 additionally holds a
     * LIVE announcement blocker keyed by its own stream ref (a distinct
     * pool/domain-qualified edge); the by-RID index carries neither owner. */
    size_t want_edges = 1;
    if (blk && blk->armed) {
        bad += og_check_edge(&g, OG_DOM_REQ_STREAMREF, blk->ref,
                             MOQ_REQ_ANNOUNCEMENT, blk->slot, what);
        const og_edge_spec_t bw[] = { { OG_DOM_REQ_STREAMREF, blk->ref } };
        bad += og_check_owner_edges(&g, MOQ_REQ_ANNOUNCEMENT, blk->slot,
                                    bw, 1, what);
        bad += og_check_no_edge(&g, OG_DOM_REQ_RID, blk->rid, what);
        want_edges = 2;
    } else {
        bad += og_check_no_edge(&g, OG_DOM_REQ_RID, 0, what);
    }
    if (g.edge_count != want_edges) {
        fprintf(stderr, "NOM %s: %zu graph edges, expected exactly %zu\n",
                what, g.edge_count, want_edges);
        bad++;
    }
    return bad;
}

/* Conservation across the refused call: every prior edge survives exactly
 * once, and any ADDED edge is admitted only under a domain-aware,
 * IDENTITY-QUALIFIED allowance -- the bounded storage-neutral window for an
 * ownership repair:
 *
 *   - a new stream-ref/ns-ref edge must key the saved target ref AND its own
 *     (kind, slot) must resolve to a live owner whose declared stream
 *     identity IS that target;
 *   - a new request-ID edge may key the DECLARED target request id (0 for
 *     origins 1-3, 2 for origin 4) but must resolve to that same target owner,
 *     never the seed or an unrelated slot;
 *   - duplicates, unsupported kinds, wrong slots, unrelated keys, and an
 *     edge merely repointed at the seed all fail by identity.
 *
 * The resolver is local and bounds-safe: it models the two owner families a
 * request-bidi carrier could live in without broadening the shared ownership
 * policy. */
static int nom_edge_owns_target(const moq_session_t *s, const og_edge_t *f,
                                uint64_t target_ref, const char *what)
{
    if (f->kind == MOQ_REQ_SUBSCRIPTION) {
        if (f->slot < 0 || (size_t)f->slot >= s->sub_cap) {
            if (!og_quiet) fprintf(stderr, "NOM %s: added edge slot out of range\n", what);
            return 0;
        }
        const moq_sub_entry_t *e = &s->subs[f->slot];
        if (e->state == MOQ_SUB_FREE ||
            e->request_stream_ref._v != target_ref) {
            if (!og_quiet) fprintf(stderr, "NOM %s: added edge's owner does not carry the"
                    " target identity\n", what);
            return 0;
        }
        return 1;
    }
    /* The no-slot path is the GENERIC request-staging path, whose owner
     * family in source is MOQ_REQ_SUBSCRIPTION; no current route stages such
     * a request in another pool, so no other kind is blessed here. A future
     * explicit private carrier kind arrives with the product and its test
     * model. */
    if (!og_quiet) fprintf(stderr, "NOM %s: added edge has unsupported kind %d\n", what,
            f->kind);
    return 0;
}

static int nom_graph_conserved(const moq_session_t *s, const og_graph_t *g0,
                               uint64_t target_ref, uint64_t target_rid,
                               const char *what)
{
    og_graph_t g1;
    og_capture(s, &g1);
    int bad = og_check_integrity(&g1, what);
    for (size_t i = 0; i < g0->edge_count; i++) {
        const og_edge_t *e = &g0->edges[i];
        size_t seen = 0;
        for (size_t j = 0; j < g1.edge_count; j++) {
            const og_edge_t *f = &g1.edges[j];
            if (f->domain == e->domain && f->key == e->key &&
                f->kind == e->kind && f->slot == e->slot)
                seen++;
        }
        if (seen != 1) {
            if (!og_quiet) fprintf(stderr, "NOM %s: pre-existing edge (%d key %llu) not"
                    " conserved (%zu matches)\n", what, (int)e->domain,
                    (unsigned long long)e->key, seen);
            bad++;
        }
    }
    for (size_t j = 0; j < g1.edge_count; j++) {
        const og_edge_t *f = &g1.edges[j];
        int preexisting = 0;
        for (size_t i = 0; i < g0->edge_count; i++) {
            const og_edge_t *e = &g0->edges[i];
            if (f->domain == e->domain && f->key == e->key &&
                f->kind == e->kind && f->slot == e->slot) {
                preexisting = 1;
                break;
            }
        }
        if (preexisting) continue;
        /* Duplicate added edges on one (domain, key) fail. */
        size_t dup = 0;
        for (size_t k = 0; k < g1.edge_count; k++)
            if (g1.edges[k].domain == f->domain && g1.edges[k].key == f->key)
                dup++;
        if (dup != 1) {
            if (!og_quiet) fprintf(stderr, "NOM %s: added edge (%d key %llu) duplicated\n",
                    what, (int)f->domain, (unsigned long long)f->key);
            bad++;
            continue;
        }
        if (f->domain == OG_DOM_REQ_STREAMREF || f->domain == OG_DOM_NS_REF) {
            if (f->key != target_ref) {
                if (!og_quiet) fprintf(stderr, "NOM %s: added edge (%d key %llu) does not"
                        " key the target\n", what, (int)f->domain,
                        (unsigned long long)f->key);
                bad++;
                continue;
            }
            if (!nom_edge_owns_target(s, f, target_ref, what)) bad++;
        } else if (f->domain == OG_DOM_REQ_RID) {
            if (f->key != target_rid) {
                if (!og_quiet) fprintf(stderr, "NOM %s: added request-ID edge keys %llu,"
                        " not the target's declared id %llu\n", what,
                        (unsigned long long)f->key,
                        (unsigned long long)target_rid);
                bad++;
                continue;
            }
            if (!nom_edge_owns_target(s, f, target_ref, what)) bad++;
        } else {
            if (!og_quiet) fprintf(stderr, "NOM %s: added edge in unsupported domain %d\n",
                    what, (int)f->domain);
            bad++;
        }
    }
    return bad;
}

/* Normalize every action this matrix can legally see; anything else fails. */
static bool nom_norm_action(const moq_action_t *a, void *vctx,
                            txs_norm_vec_t *out)
{
    (void)vctx;
    txs_img_t im;
    txs_img_init(&im);
    switch (a->kind) {
    case MOQ_ACTION_CLOSE_BIDI_STREAM:
        txs_img_u64(&im, a->u.close_bidi_stream.stream_ref._v);
        return txs_norm_append_img(out, a->kind, &im);
    case MOQ_ACTION_STOP_BIDI_STREAM:
        txs_img_u64(&im, a->u.stop_bidi_stream.stream_ref._v);
        txs_img_u64(&im, a->u.stop_bidi_stream.error_code);
        return txs_norm_append_img(out, a->kind, &im);
    case MOQ_ACTION_RESET_BIDI_STREAM:
        txs_img_u64(&im, a->u.reset_bidi_stream.stream_ref._v);
        txs_img_u64(&im, a->u.reset_bidi_stream.error_code);
        return txs_norm_append_img(out, a->kind, &im);
    case MOQ_ACTION_SEND_BIDI_STREAM: {
        const moq_send_bidi_stream_action_t *sb = &a->u.send_bidi_stream;
        if (sb->len && !sb->data) {
            fprintf(stderr, "NOM: SEND_BIDI with NULL data\n");
            return false;
        }
        txs_img_u64(&im, sb->stream_ref._v);
        txs_img_u64(&im, sb->fin ? 1 : 0);
        /* Decode, never count: envelope, then the complete REQUEST_ERROR. */
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, sb->data, sb->len);
        moq_control_envelope_t env;
        memset(&env, 0, sizeof(env));
        if (moq_d18_decode_envelope(&r, &env) != MOQ_OK) {
            fprintf(stderr, "NOM: undecodable SEND_BIDI envelope\n");
            return false;
        }
        if (moq_buf_reader_remaining(&r) != 0) {
            fprintf(stderr, "NOM: trailing bytes after envelope\n");
            return false;
        }
        txs_img_u64(&im, env.msg_type);
        moq_d18_request_error_t er;
        memset(&er, 0, sizeof(er));
        if (env.msg_type == (uint64_t)MOQ_D18_REQUEST_ERROR) {
            if (moq_d18_decode_request_error(env.payload, env.payload_len,
                                             &er) != MOQ_OK) {
                fprintf(stderr, "NOM: undecodable REQUEST_ERROR body\n");
                return false;
            }
            if (er.reason.len && !er.reason.data) {
                fprintf(stderr, "NOM: REQUEST_ERROR reason NULL bytes\n");
                return false;
            }
            txs_img_u64(&im, er.error_code);
            txs_img_u64(&im, er.retry_interval);
            txs_img_bytes(&im, er.reason.data, er.reason.len);
        }
        return txs_norm_append_img(out, a->kind, &im);
    }
    default:
        fprintf(stderr, "NOM: unnormalized action kind %u\n",
                (unsigned)a->kind);
        return false;
    }
}

static txs_op_hooks_t nom_hooks = {
    NULL, NULL, NULL, NULL, no_event_expected, nom_norm_action
};

/* The DECLARED terminal output for one target completion. */
static void nom_want_terminal(txs_norm_vec_t *v, bool pre_setup,
                              uint64_t target_ref)
{
    txs_img_t im;
    if (pre_setup) {
        txs_img_init(&im);
        txs_img_u64(&im, target_ref);
        txs_img_u64(&im, 0x1);                        /* CANCELLED */
        MOQ_TEST_CHECK(txs_norm_append_img(v, MOQ_ACTION_STOP_BIDI_STREAM,
                                           &im));
        txs_img_init(&im);
        txs_img_u64(&im, target_ref);
        txs_img_u64(&im, 0x1);
        MOQ_TEST_CHECK(txs_norm_append_img(v, MOQ_ACTION_RESET_BIDI_STREAM,
                                           &im));
    } else {
        txs_img_init(&im);
        txs_img_u64(&im, target_ref);
        txs_img_u64(&im, 1);                          /* FIN */
        txs_img_u64(&im, (uint64_t)MOQ_D18_REQUEST_ERROR);
        txs_img_u64(&im, MOQ_REQUEST_ERROR_INTERNAL_ERROR);
        txs_img_u64(&im, 0);                          /* retry interval */
        txs_img_bytes(&im, (const uint8_t *)nom_reason, strlen(nom_reason));
        MOQ_TEST_CHECK(txs_norm_append_img(v, MOQ_ACTION_SEND_BIDI_STREAM,
                                           &im));
    }
}

/* Permanent outcome validator: the ONLY recognized results are MOQ_OK and
 * MOQ_ERR_WOULD_BLOCK; everything else is an oracle failure. Quiet mode lets
 * the negative self-check below run without noise. */
static int nom_outcome_quiet;
static int nom_outcome_classify(moq_result_t rc, const char *what)
{
    if (rc == MOQ_OK) return 0;
    if (rc == MOQ_ERR_WOULD_BLOCK) return 1;
    if (!nom_outcome_quiet)
        fprintf(stderr, "NOM %s: unrecognized outcome %d\n", what, (int)rc);
    return -1;
}

static void nom_outcome_selfcheck(void)
{
    nom_outcome_quiet = 1;
    MOQ_TEST_CHECK_EQ_INT(nom_outcome_classify(MOQ_OK, "self"), 0);
    MOQ_TEST_CHECK_EQ_INT(nom_outcome_classify(MOQ_ERR_WOULD_BLOCK, "self"),
                          1);
    MOQ_TEST_CHECK_EQ_INT(nom_outcome_classify(MOQ_ERR_INTERNAL, "self"), -1);
    MOQ_TEST_CHECK_EQ_INT(nom_outcome_classify(MOQ_ERR_CLOSED, "self"), -1);
    nom_outcome_quiet = 0;
}

/* The complete retirement postcondition, shared by both outcome arms, the
 * second retry, and the post-late-FIN recheck: target carrier fully gone,
 * seed-only topology, exact pool occupancy, exact session state, exact drain
 * multiset. */
/* The blocker announcement's exact edge set: one stream-ref request-registry
 * edge keyed by NOM_BLK_REF resolving to its announcement slot. */
static int nom_blk_graph_edges(const moq_session_t *s, const nom_blk_t *b,
                               const char *what)
{
    if (!b || !b->armed) return 0;
    og_graph_t g;
    og_capture(s, &g);
    int bad = og_check_edge(&g, OG_DOM_REQ_STREAMREF, b->ref,
                            MOQ_REQ_ANNOUNCEMENT, b->slot, what);
    const og_edge_spec_t w[] = { { OG_DOM_REQ_STREAMREF, b->ref } };
    bad += og_check_owner_edges(&g, MOQ_REQ_ANNOUNCEMENT, b->slot, w, 1, what);
    return bad;
}

/* Conservation of the blocker owner across a phase: the announcement stays
 * ESTABLISHED/RECEIVER with its declared identity and goaway_sent, and its
 * stream-ref edge is intact. */
static int nom_blk_conserved(const moq_session_t *s, const nom_blk_t *b,
                             const char *what)
{
    if (!b || !b->armed) return 0;
    int bad = 0;
    if (b->slot < 0 || (size_t)b->slot >= s->ann_cap) {
        fprintf(stderr, "NOM %s: blocker slot out of range\n", what);
        return 1;
    }
    const moq_ann_entry_t *e = &s->announcements[b->slot];
    if ((int)e->state != (int)MOQ_ANN_ESTABLISHED ||
        (int)e->role != (int)MOQ_ANN_ROLE_RECEIVER ||
        e->generation != b->gen || e->handle._opaque != b->handle ||
        e->request_id != b->rid || e->request_stream_ref._v != b->ref ||
        !e->goaway_sent) {
        fprintf(stderr, "NOM %s: blocker owner record changed\n", what);
        bad++;
    }
    bad += nom_blk_graph_edges(s, b, what);
    return bad;
}

static int nom_check_complete(const moq_session_t *s, const nom_seed_t *seed,
                              uint64_t seed_ref, uint64_t target_ref,
                              int pre_state, const drain_snap_t *want_ring,
                              const txs_snapshot_t *base, const nom_blk_t *blk,
                              const char *what)
{
    int bad = 0;
    size_t want_recv_payload = base->recv_payload_bytes;
    /* The remaining generic scalars, DERIVED from the pre-target baseline
     * with the declared completion transitions: all output consumed (both
     * queues empty), the scratch arena back at its pre-target cursor, and
     * the drain count equal to the declared ring's. */
    if ((size_t)(s->event_tail - s->event_head) != 0 ||
        (size_t)(s->action_tail - s->action_head) != 0) {
        fprintf(stderr, "NOM %s: queues not drained at completion\n", what);
        bad++;
    }
    if (s->event_scratch_len != base->event_scratch_len) {
        fprintf(stderr, "NOM %s: event_scratch_len %zu, expected the"
                " pre-target %zu\n", what, s->event_scratch_len,
                base->event_scratch_len);
        bad++;
    }
    if (s->drain_ref_count != want_ring->count) {
        fprintf(stderr, "NOM %s: drain count %zu, expected the declared"
                " %zu\n", what, s->drain_ref_count, want_ring->count);
        bad++;
    }
    /* The target carrier's ACCOUNTING must be released, not only its edges:
     * a repair that retains target bytes forever would otherwise pass. */
    if (s->recv_payload_bytes != want_recv_payload) {
        fprintf(stderr, "NOM %s: recv_payload_bytes %zu, expected the"
                " pre-target %zu\n", what, s->recv_payload_bytes,
                want_recv_payload);
        bad++;
    }
    moq_stream_ref_t tr; tr._v = target_ref;
    if (request_registry_find_by_streamref(s, tr).kind != MOQ_REQ_NONE) {
        fprintf(stderr, "NOM %s: target still has a registry owner\n", what);
        bad++;
    }
    bad += nom_graph_seed_only(s, seed_ref, seed->slot, target_ref, blk, what);
    /* The live blocker owner survives the target's whole lifecycle. */
    bad += nom_blk_conserved(s, blk, what);
    bad += nom_seed_equal(s, seed, what);
    {
        int busy = 0;
        for (size_t i = 0; i < s->sub_cap; i++)
            if (s->subs[i].state != MOQ_SUB_FREE) busy++;
        if (busy != 1) {
            fprintf(stderr, "NOM %s: pool occupancy %d, expected 1\n", what,
                    busy);
            bad++;
        }
    }
    if ((int)s->state != pre_state) {
        fprintf(stderr, "NOM %s: session state %d, expected %d\n", what,
                (int)s->state, pre_state);
        bad++;
    }
    drain_snap_t now;
    drain_snap(s, &now);
    bad += drain_multiset_equals(&now, want_ring, what);
    return bad;
}

typedef struct nom_case {
    const char *name;
    int         origin;       /* 1..4 */
    bool        fin;          /* the target call carries a FIN */
    bool        pre_setup;
    /* The inbound target Request ID, DECLARED per row independently of the
     * encoded target bytes (draft-18 §10.1). Origin 4's live announcement
     * blocker commits inbound id 0, so its next request is id 2; origins 1-3
     * commit no preceding request and use id 0. The no-slot terminal refuses
     * the target BEFORE its id is validated, so this is a wire-legality
     * declaration the encoder honours, not a runtime-killable value. */
    uint64_t    target_rid;
} nom_case_t;

/* Descriptor self-check: origin 4 requires target id 2, every other origin
 * requires 0. A missing/defaulted or coherently changed member cannot silently
 * redefine the fixture. */
static int nom_case_target_rid_ok(const nom_case_t *c)
{
    uint64_t want = (c->origin == 4) ? 2u : 0u;
    if (c->target_rid != want) {
        fprintf(stderr, "FAIL: nom %s: declared target_rid %llu, origin %d"
                " requires %llu\n", c->name, (unsigned long long)c->target_rid,
                c->origin, (unsigned long long)want);
        return 1;
    }
    return 0;
}

/* Green-phase perturbation switches (test-local, default off): each forces a
 * deliberate divergence a load-bearing oracle must catch. Used only by the
 * temporary kill runs recorded in the task report; they are OFF in the
 * committed tree and asserted so by the matrix driver. */
static int nom_perturb;   /* 0=off, else the numbered perturbation */

/* -- Origin 4's LIVE draft-18 request blocker ------------------------
 *
 * Origin 4 fills the send buffer with a legal per-request GOAWAY on an
 * accepted inbound announcement. The GOAWAY is one non-FIN SEND_BIDI_STREAM
 * whose bytes occupy the send tail so the refused target's REQUEST_ERROR cannot
 * be encoded. The announcement owner is predeclared and required LIVE through
 * the whole arc. */
#define NOM_BLK_REF 3u
/* Sized so the encoded GOAWAY fills the 64-byte send buffer, leaving a tail too
 * small for the refused target's REQUEST_ERROR, so the target is retained. */
static const uint8_t nom_goaway_uri[44] = {
    'm','o','q',':','/','/','n','o','m','-','a','l','t','.','e','x','a',
    'm','p','l','e','/','g','o','a','w','a','y','/','n','o','m','/','0',
    '0','0','0','0','0','0','0','0','0','1'
};

/* Arm the blocker: feed an inbound PUBLISH_NAMESPACE (id 0), accept it, drain
 * the accept's REQUEST_OK so the GOAWAY starts from a clean send tail, then
 * issue the per-request GOAWAY. Must run BEFORE the seed occupies the sole
 * subscription staging slot -- the d18 request dispatch grabs one transiently. */
static void nom_arm_goaway_blocker(moq_session_t *s, nom_blk_t *out)
{
    memset(out, 0, sizeof(*out));
    int slot = -1;
    uint32_t gen = 0;
    for (size_t i = 0; i < s->ann_cap; i++)
        if (s->announcements[i].state == MOQ_ANN_FREE) {
            slot = (int)i;
            gen = s->announcements[i].generation | 1u;   /* live generation */
            break;
        }
    MOQ_TEST_CHECK(slot >= 0);

    /* The packed announcement handle is MINTED from the pre-ingress
     * slot/generation/session-tag, never adopted from the entry afterwards, so
     * the surfaced event handle and the live owner handle are each REQUIRED to
     * equal this derivation. */
    uint16_t tag = s->session_tag;
    uint64_t want_handle =
        moq_handle_pack(MOQ_HANDLE_POOL_ANNOUNCEMENT, tag, gen, (uint32_t)slot);

    moq_stream_ref_t ref = moq_stream_ref_from_u64(NOM_BLK_REF);
    uint8_t m[192];
    size_t n = enc_pns_full(m, sizeof(m), 0, "blk", NULL);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, m, n, false, 1),
        (int)MOQ_OK);
    uint64_t h = 0;
    arm_output_exact(s, MOQ_EVENT_NAMESPACE_PUBLISHED, 1, 0, "nom blk", &h);
    MOQ_TEST_CHECK_EQ_U64(h, want_handle);

    moq_announcement_t ann;
    ann._opaque = want_handle;
    moq_accept_namespace_cfg_t ac;
    moq_accept_namespace_cfg_init(&ac);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_accept_namespace(s, ann, &ac, 1), (int)MOQ_OK);
    /* Drain the accept's REQUEST_OK so the empty action queue lets the next
     * advancing call reset the send tail before the GOAWAY encodes. */
    {
        moq_event_t e;
        while (moq_session_poll_events(s, &e, 1) > 0) moq_event_cleanup(&e);
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
    }

    moq_request_goaway_cfg_t gc;
    moq_request_goaway_cfg_init(&gc);
    gc.new_session_uri.data = nom_goaway_uri;
    gc.new_session_uri.len = sizeof(nom_goaway_uri);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_request_goaway_namespace(s, ann, &gc, 1), (int)MOQ_OK);

    const moq_ann_entry_t *e = ann_by_ref(s, ref);
    MOQ_TEST_CHECK(e != NULL);
    if (e) {
        out->armed  = 1;
        out->slot   = slot;               /* the pre-ingress derivation */
        out->gen    = gen;
        out->handle = want_handle;        /* MINTED, not read back */
        out->rid    = e->request_id;
        out->ref    = ref._v;
        MOQ_TEST_CHECK_EQ_INT((int)e->state, (int)MOQ_ANN_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT((int)e->role, (int)MOQ_ANN_ROLE_RECEIVER);
        MOQ_TEST_CHECK(e->goaway_sent);
        MOQ_TEST_CHECK_EQ_U64(out->rid, (uint64_t)0);
        /* The live owner's own identity must match the minted derivation. */
        MOQ_TEST_CHECK_EQ_INT((int)(e - s->announcements), slot);
        MOQ_TEST_CHECK_EQ_U64((uint64_t)e->generation, (uint64_t)gen);
        MOQ_TEST_CHECK_EQ_U64(e->handle._opaque, want_handle);
    }
}


/* The no-slot carrier state for a ref: -1 absent, 0 present (no FIN), 1 present
 * (FIN). Proves the PRODUCT installs/retires the refused target's carrier with
 * the row's exact FIN value, through real refusal/recovery. */
static int nom_carrier_state(const moq_session_t *s, uint64_t ref_v)
{
    moq_stream_ref_t ref;
    ref._v = ref_v;
    int i = noslot_carrier_find(s, ref);
    if (i < 0) return -1;
    return s->noslot_carriers[i].fin ? 1 : 0;
}

#define NOM_UNREL_CARRIER 0x8888u   /* an unrelated seeded carrier, conserved */

/* At every retired phase: the target carrier is gone, the count is back at the
 * declared baseline, and the unrelated seeded carrier is ref/FIN exact. */
static int nom_carrier_retired(const moq_session_t *s, size_t base,
                               const char *what)
{
    int bad = 0;
    if (nom_carrier_state(s, NOM_TARGET_REF) != -1) {
        fprintf(stderr, "NOM %s: target carrier still present\n", what);
        bad++;
    }
    if (nom_carrier_state(s, NOM_UNREL_CARRIER) != 0) {
        fprintf(stderr, "NOM %s: unrelated carrier changed\n", what);
        bad++;
    }
    if (s->noslot_carrier_count != base) {
        fprintf(stderr, "NOM %s: carrier count %zu, expected the baseline %zu\n",
                what, s->noslot_carrier_count, base);
        bad++;
    }
    return bad;
}

/* The shared matrix runner. Blockers by origin:
 *   1: full drain ring (no action blocker);
 *   2: pre-setup, max_actions=2, one queued close leaves avail == 1 < 2;
 *   3: max_actions=1, one queued close fills the ring;
 *   4: send_buffer_size=64, a per-request GOAWAY fills the send tail.
 * Recovery drains ONLY the blocker (poll_actions), then the documented empty
 * re-feed. No target bytes are ever re-delivered. */
static void run_nom_case(const nom_case_t *c)
{
    char what[96];
    snprintf(what, sizeof(what), "nom %s", c->name);
    uint32_t max_actions = 0, send_size = 0;
    if (c->origin == 2) max_actions = 2;
    if (c->origin == 3) max_actions = 1;
    if (c->origin == 4) send_size = 64;
    moq_session_t *s = nom_make_session(!c->pre_setup, max_actions, send_size);
    if (!s) return;

    failures += nom_case_target_rid_ok(c);

    /* Origin 4's blocker is a LIVE announcement + per-request GOAWAY. It must be
     * armed BEFORE the seed occupies the sole subscription staging slot, since
     * the d18 request dispatch grabs one transiently on the announcement
     * handoff. It commits inbound id 0, so the target is declared at id 2. */
    nom_blk_t blk;
    memset(&blk, 0, sizeof(blk));
    if (c->origin == 4)
        nom_arm_goaway_blocker(s, &blk);

    /* Seed identity, DERIVED from the free pool AFTER the blocker's transient
     * staging use (which advances the slot's generation). */
    int seed_slot = -1;
    uint32_t seed_gen = 0;
    for (size_t i = 0; i < s->sub_cap; i++)
        if (s->subs[i].state == MOQ_SUB_FREE) {
            seed_slot = (int)i;
            seed_gen = s->subs[i].generation | 1u;
            break;
        }
    MOQ_TEST_CHECK_EQ_INT(seed_slot, 0);

    /* Seed: a fragmented SUBSCRIBE occupies the ONE pool slot legally. */
    uint8_t sub[128];
    size_t sub_len = nom_encode_subscribe(sub, sizeof(sub));
    MOQ_TEST_CHECK(sub_len > 8);
    moq_stream_ref_t seed_ref = moq_stream_ref_from_u64(NOM_SEED_REF);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, seed_ref, sub, sub_len - 4,
                                              false, 1),
        (int)MOQ_OK);
    /* The retained prefix must equal the bytes actually fed BEFORE the
     * captured record becomes a baseline. */
    {
        const moq_sub_entry_t *e = &s->subs[seed_slot];
        MOQ_TEST_CHECK_EQ_INT((int)e->state, (int)MOQ_SUB_RECVING_REQUEST);
        MOQ_TEST_CHECK_EQ_U64((uint64_t)e->generation, (uint64_t)seed_gen);
        MOQ_TEST_CHECK_EQ_SIZE(e->req_recv_len, sub_len - 4);
        MOQ_TEST_CHECK(e->req_recv_buf != NULL);
        if (e->req_recv_buf && e->req_recv_len == sub_len - 4)
            MOQ_TEST_CHECK(memcmp(e->req_recv_buf, sub, sub_len - 4) == 0);
    }
    nom_seed_t seed;
    nom_seed_capture(s, &seed);
    nom_seed_arm_assert(&seed, seed_slot);
    failures += nom_graph_seed_only(s, NOM_SEED_REF, seed_slot,
                                    NOM_TARGET_REF, &blk, "seeded");

    /* Blockers. Origin 4's blocker is the live announcement GOAWAY armed before
     * the seed; its ref is NOM_BLK_REF, the others' is an unmapped close ref. */
    size_t blockers = 0;
    moq_stream_ref_t blocker_ref =
        moq_stream_ref_from_u64(c->origin == 4 ? NOM_BLK_REF : 0x6000);
    if (c->origin == 1) {
        fill_drain_ring(s);
    } else if (c->origin == 2 || c->origin == 3) {
        MOQ_TEST_CHECK_EQ_INT((int)queue_close_bidi(s, blocker_ref),
                              (int)MOQ_OK);
        blockers = 1;
        if (c->origin == 3) MOQ_TEST_CHECK(action_queue_full(s));
        else MOQ_TEST_CHECK_EQ_SIZE(action_queue_avail(s), (size_t)1);
    } else {
        /* The GOAWAY's SEND_BIDI_STREAM already occupies the action slot and
         * fills the send tail; nothing new is queued. Its bytes leave too small
         * a tail for the refused target's REQUEST_ERROR. */
        blockers = 1;
        MOQ_TEST_CHECK(s->send_cap - s->send_len <= 16);
        MOQ_TEST_CHECK(action_queue_avail(s) >= 2);
    }
    drain_snap_t ring0;
    drain_snap(s, &ring0);

    /* An unrelated no-slot carrier, seeded directly, that every phase must leave
     * ref/FIN exact. The baseline count is captured with it present. */
    MOQ_TEST_CHECK(noslot_carrier_install(
        s, moq_stream_ref_from_u64(NOM_UNREL_CARRIER), false));
    size_t carrier_base = s->noslot_carrier_count;
    MOQ_TEST_CHECK_EQ_INT(nom_carrier_state(s, NOM_UNREL_CARRIER), 0);
    MOQ_TEST_CHECK_EQ_INT(nom_carrier_state(s, NOM_TARGET_REF), -1);

    /* Whole-session and graph baselines, captured BEFORE the target call. */
    txs_snapshot_t before;
    txs_capture(s, &seed_ref, 1, &before);
    expect_after_call_prepare(&before);
    og_graph_t g0;
    og_capture(s, &g0);

    /* The target request, complete and FIN'd per the row, at the DECLARED id
     * (2 for origin 4's post-announcement sequence, 0 otherwise). */
    uint8_t tgt[128];
    size_t tgt_len = nom_encode_subscribe_at(tgt, sizeof(tgt), c->target_rid);
    moq_stream_ref_t target = moq_stream_ref_from_u64(NOM_TARGET_REF);
    int pre_state = (int)s->state;
    MOQ_TEST_CHECK(pre_state != (int)MOQ_SESS_CLOSED);
    if (!c->pre_setup)
        MOQ_TEST_CHECK_EQ_INT(pre_state, (int)MOQ_SESS_ESTABLISHED);
    moq_result_t rc = moq_session_on_bidi_stream_bytes(s, target, tgt, tgt_len,
                                                       c->fin, 1);
    if (nom_perturb == 6)
        s->event_scratch_len += 8;   /* an UNRELATED whole-session scalar */
    MOQ_TEST_CHECK_EQ_INT((int)s->state, pre_state);

    int arm = nom_outcome_classify(rc, what);
    MOQ_TEST_CHECK(arm >= 0);
    if (arm < 0) { moq_session_destroy(s); return; }

    /* The declared completion drain multiset. Origin 1's want is DERIVED by
     * removing exactly the DECLARED filler from the pre-call ring, never from
     * an observed result. */
    drain_snap_t comp_want, late_want;
    if (c->origin == 1) {
        drain_snap_t less = ring0;
        size_t k = 0;
        int removed = 0;
        for (size_t i = 0; i < ring0.count; i++) {
            if (!removed && ring0.ref[i] == NOM_FILLER0) { removed = 1; continue; }
            less.ref[k] = ring0.ref[i];
            less.reason[k] = ring0.reason[i];
            k++;
        }
        less.count = k;
        MOQ_TEST_CHECK_EQ_INT(removed, 1);
        late_want = less;
        drain_snap_plus(&less, target, MOQ_DRAIN_NORMAL, &comp_want);
    } else if (c->fin) {
        comp_want = ring0;
        late_want = ring0;
    } else {
        drain_snap_plus(&ring0, target, MOQ_DRAIN_NORMAL, &comp_want);
        late_want = ring0;
    }

    if (arm == 0) {
        /* Arm A: immediate completion -- the WHOLE obligation queued NOW.
         * Consume exactly the leading blocker actions so both arms meet the
         * same convergent output check. Structurally shared; no current or
         * candidate product reaches it yet. */
        txs_norm_vec_t lead;
        txs_norm_init(&lead);
        size_t taken = 0;
        moq_action_t a;
        while (taken < blockers && moq_session_poll_actions(s, &a, 1) > 0) {
            if (!nom_norm_action(&a, NULL, &lead)) failures++;
            moq_action_cleanup(&a);
            taken++;
        }
        MOQ_TEST_CHECK_EQ_SIZE(taken, blockers);
        txs_norm_free(&lead);
    } else {
        /* Arm B: conservation at the refused point. The target-specific
         * carrier is deliberately UNBOUNDED in identity (an ownership repair
         * may create one) but every allowed delta is bounded:
         *   - whole-session scalars equal, except recv_payload_bytes which
         *     may grow by at most the fed target length;
         *   - graph topology conserved; any added edge keys the target and
         *     resolves to a live carrier;
         *   - the seed record, drain multiset, blocker action (kind, ref,
         *     and for the GOAWAY blocker its exact bytes) unchanged;
         *   - no event, no new action. */
        failures += nom_seed_equal(s, &seed, what);
        {
            txs_snapshot_t expect = before;
            const size_t grew = s->recv_payload_bytes;
            MOQ_TEST_CHECK(grew >= before.recv_payload_bytes);
            MOQ_TEST_CHECK(grew - before.recv_payload_bytes <= tgt_len);
            expect.recv_payload_bytes = grew;   /* bounded target carrier */
            failures += txs_check_eq(s, &seed_ref, 1, &expect, what);
        }
        failures += nom_graph_conserved(s, &g0, NOM_TARGET_REF,
                                        c->target_rid, what);
        /* Product-path carrier: the refused target is retained in EXACTLY one
         * carrier with its declared ref and the ROW's FIN value; the count is
         * baseline + 1; the unrelated carrier stays ref/FIN exact. */
        MOQ_TEST_CHECK_EQ_INT(nom_carrier_state(s, NOM_TARGET_REF),
                              c->fin ? 1 : 0);
        MOQ_TEST_CHECK_EQ_INT(nom_carrier_state(s, NOM_UNREL_CARRIER), 0);
        MOQ_TEST_CHECK_EQ_SIZE(s->noslot_carrier_count, carrier_base + 1);
        drain_snap_t now;
        drain_snap(s, &now);
        failures += drain_multiset_equals(&now, &ring0, what);
        {
            int evs = 0;
            moq_event_t ev;
            while (moq_session_poll_events(s, &ev, 1) > 0) {
                evs++; moq_event_cleanup(&ev);
            }
            MOQ_TEST_CHECK_EQ_INT(evs, 0);
        }
        MOQ_TEST_CHECK_EQ_SIZE(
            (size_t)(s->action_tail - s->action_head), blockers);
        /* The blocker is still the SAME action, byte for byte. */
        if (blockers == 1) {
            const moq_action_t *head =
                &s->actions[s->action_head % s->action_cap];
            if (c->origin == 4) {
                /* The blocker is the queued per-request GOAWAY, compared in
                 * full: a non-FIN SEND_BIDI_STREAM on the announcement's ref
                 * whose bytes are one complete REQUEST_GOAWAY envelope with no
                 * trailing bytes and the exact New Session URI. */
                MOQ_TEST_CHECK_EQ_INT((int)head->kind,
                                      (int)MOQ_ACTION_SEND_BIDI_STREAM);
                MOQ_TEST_CHECK_EQ_U64(head->u.send_bidi_stream.stream_ref._v,
                                      blocker_ref._v);
                MOQ_TEST_CHECK_EQ_INT(head->u.send_bidi_stream.fin ? 1 : 0, 0);
                MOQ_TEST_CHECK(head->u.send_bidi_stream.data != NULL);
                if (head->u.send_bidi_stream.data) {
                    moq_buf_reader_t gr;
                    moq_buf_reader_init(&gr, head->u.send_bidi_stream.data,
                                        head->u.send_bidi_stream.len);
                    moq_control_envelope_t genv;
                    memset(&genv, 0, sizeof(genv));
                    MOQ_TEST_CHECK_EQ_INT(
                        (int)moq_d18_decode_envelope(&gr, &genv), (int)MOQ_OK);
                    MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&gr),
                                           (size_t)0);
                    MOQ_TEST_CHECK_EQ_U64(genv.msg_type,
                                          (uint64_t)MOQ_D18_GOAWAY);
                    moq_d18_goaway_t gaw;
                    memset(&gaw, 0, sizeof(gaw));
                    MOQ_TEST_CHECK_EQ_INT(
                        (int)moq_d18_decode_goaway_request(genv.payload,
                            genv.payload_len, &gaw), (int)MOQ_OK);
                    MOQ_TEST_CHECK(gaw.uri.len == sizeof(nom_goaway_uri) &&
                                   gaw.uri.data &&
                                   memcmp(gaw.uri.data, nom_goaway_uri,
                                          sizeof(nom_goaway_uri)) == 0);
                }
            } else {
                MOQ_TEST_CHECK_EQ_INT((int)head->kind,
                                      (int)MOQ_ACTION_CLOSE_BIDI_STREAM);
                MOQ_TEST_CHECK_EQ_U64(head->u.close_bidi_stream.stream_ref._v,
                                      blocker_ref._v);
            }
        }

        /* Recovery: drain ONLY the blocker(s), then the documented empty
         * re-feed with the row's own FIN fact. */
        {
            txs_norm_vec_t got;
            txs_norm_init(&got);
            collect_actions(s, &nom_hooks, &got);
            MOQ_TEST_CHECK_EQ_SIZE(got.count, blockers);
            txs_norm_free(&got);
        }
        if (c->origin == 1) {
            /* Free ONE drain slot: the DECLARED filler's own legitimate FIN
             * releases it through the public drain-ring absorb path. */
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_session_on_bidi_stream_bytes(
                    s, moq_stream_ref_from_u64(
                        nom_perturb == 4 ? NOM_FILLER0 + 1 : NOM_FILLER0),
                    NULL, 0, true, 1),
                (int)MOQ_OK);
        }
        rc = moq_session_on_bidi_stream_bytes(s, target, NULL, 0, c->fin, 1);
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
    }

    /* Both arms converge: the EXACT terminal obligation, once, and the whole
     * retirement postcondition. */
    MOQ_TEST_CHECK_EQ_INT((int)s->state, pre_state);
    {
        txs_norm_vec_t got, want;
        txs_norm_init(&got);
        txs_norm_init(&want);
        collect_events(s, &nom_hooks, &got);
        MOQ_TEST_CHECK_EQ_SIZE(got.count, (size_t)0);
        collect_actions(s, &nom_hooks, &got);
        nom_want_terminal(&want, c->pre_setup, NOM_TARGET_REF);
        failures += txs_norm_equals(&got, &want, what);
        txs_norm_free(&got);
        txs_norm_free(&want);
    }
    if (!c->fin) check_drain_ref_reason(s, target, MOQ_DRAIN_NORMAL);
    failures += nom_check_complete(s, &seed, NOM_SEED_REF, NOM_TARGET_REF,
                                   pre_state, &comp_want, &before, &blk, what);
    failures += nom_carrier_retired(s, carrier_base, what);
    if (nom_perturb == 2) {
        /* Leave a phantom target owner behind: the completion oracle must
         * name it. */
        moq_request_endpoint_t pep;
        memset(&pep, 0, sizeof(pep));
        pep.kind = MOQ_REQ_SUBSCRIPTION;
        pep.slot = seed.slot;
        request_registry_insert_by_streamref(
            s, moq_stream_ref_from_u64(NOM_TARGET_REF), pep);
        failures += nom_check_complete(s, &seed, NOM_SEED_REF, NOM_TARGET_REF,
                                       pre_state, &comp_want, &before, &blk,
                                       "perturb-2");
        request_registry_remove_by_streamref(
            s, moq_stream_ref_from_u64(NOM_TARGET_REF));
    }

    /* Exactly once: a second empty re-feed emits nothing, resurrects
     * nothing, and reproduces the WHOLE postcondition. */
    rc = moq_session_on_bidi_stream_bytes(s, target, NULL, 0, false, 1);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
    check_no_output(s, &nom_hooks);
    failures += nom_check_complete(s, &seed, NOM_SEED_REF, NOM_TARGET_REF,
                                   pre_state, &comp_want, &before, &blk, what);
    failures += nom_carrier_retired(s, carrier_base, "replay");
    /* The send buffer's accounting end state: the ring is empty and this
     * advancing call has run, so every queued byte -- blocker and terminal
     * alike -- must be reclaimed. */
    MOQ_TEST_CHECK_EQ_SIZE(s->send_len, (size_t)0);

    /* Without a peer FIN the drain ref is RELEASABLE: the later FIN restores
     * the DECLARED ring (never an observed one), emits nothing, and the whole
     * retirement + seed postcondition holds again. */
    if (!c->fin) {
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, target, NULL, 0, true, 1),
            (int)MOQ_OK);
        check_no_output(s, &nom_hooks);
        failures += nom_check_complete(s, &seed, NOM_SEED_REF, NOM_TARGET_REF,
                                       pre_state, &late_want, &before, &blk,
                                       what);
        failures += nom_carrier_retired(s, carrier_base, "late-terminal");
        MOQ_TEST_CHECK_EQ_SIZE(s->send_len, (size_t)0);
    }
    moq_session_destroy(s);
}

/* Capacity-available controls: the SAME seed and target, no blocker; each
 * must complete IMMEDIATELY through the exact terminal branch. These pin the
 * branch the blocked rows defer; a completed target emits no event. */
static void run_nom_control(bool pre_setup, bool fin)
{
    nom_case_t c;
    memset(&c, 0, sizeof(c));
    c.name = pre_setup ? (fin ? "control-pre-fin" : "control-pre")
                       : (fin ? "control-post-fin" : "control-post");
    c.origin = 0;
    c.fin = fin;
    c.pre_setup = pre_setup;
    char what[96];
    snprintf(what, sizeof(what), "nom %s", c.name);
    moq_session_t *s = nom_make_session(!pre_setup, 0, 0);
    if (!s) return;
    MOQ_TEST_CHECK(noslot_carrier_install(
        s, moq_stream_ref_from_u64(NOM_UNREL_CARRIER), false));
    size_t carrier_base = s->noslot_carrier_count;
    int seed_slot = -1;
    for (size_t i = 0; i < s->sub_cap; i++)
        if (s->subs[i].state == MOQ_SUB_FREE) { seed_slot = (int)i; break; }
    uint8_t sub[128];
    size_t sub_len = nom_encode_subscribe(sub, sizeof(sub));
    moq_stream_ref_t seed_ref = moq_stream_ref_from_u64(NOM_SEED_REF);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, seed_ref, sub, sub_len - 4,
                                              false, 1),
        (int)MOQ_OK);
    nom_seed_t seed;
    nom_seed_capture(s, &seed);
    nom_seed_arm_assert(&seed, seed_slot);
    failures += nom_graph_seed_only(s, NOM_SEED_REF, seed_slot,
                                    NOM_TARGET_REF, NULL, "control-seeded");
    int pre_state = (int)s->state;
    txs_snapshot_t ctrl_base;
    txs_capture(s, &seed_ref, 1, &ctrl_base);
    expect_after_call_prepare(&ctrl_base);
    drain_snap_t none, wantr;
    memset(&none, 0, sizeof(none));
    uint8_t tgt[128];
    size_t tgt_len = nom_encode_subscribe(tgt, sizeof(tgt));
    moq_stream_ref_t target = moq_stream_ref_from_u64(NOM_TARGET_REF);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, target, tgt, tgt_len, fin, 1),
        (int)MOQ_OK);
    /* Immediate completion creates NO target carrier. */
    failures += nom_carrier_retired(s, carrier_base, "control-immediate");
    {
        txs_norm_vec_t got, want;
        txs_norm_init(&got);
        txs_norm_init(&want);
        collect_events(s, &nom_hooks, &got);
        MOQ_TEST_CHECK_EQ_SIZE(got.count, (size_t)0);
        collect_actions(s, &nom_hooks, &got);
        nom_want_terminal(&want, pre_setup, NOM_TARGET_REF);
        failures += txs_norm_equals(&got, &want, what);
        txs_norm_free(&got);
        txs_norm_free(&want);
    }
    if (fin) wantr = none;
    else drain_snap_plus(&none, target, MOQ_DRAIN_NORMAL, &wantr);
    (void)seed_slot;
    failures += nom_check_complete(s, &seed, NOM_SEED_REF, NOM_TARGET_REF,
                                   pre_state, &wantr, &ctrl_base, NULL, what);
    failures += nom_carrier_retired(s, carrier_base, "control-complete");
    moq_session_destroy(s);
}

/* One legal MONOTONIC product route on the carrier's FIN: refuse the complete
 * target with NO FIN (carrier false), then -- while the SAME blocker remains --
 * feed the target's legal empty FIN so the SAME carrier flips to true with the
 * count unchanged, then recover with an empty fin=false re-feed. Because the FIN
 * is already observed in the carrier, the completed terminal owes NO drain. */
static void run_nom_carrier_monotonic(void)
{
    const char *what = "nom carrier monotonic";
    moq_session_t *s = nom_make_session(true, 1, 0);   /* ONE action slot */
    if (!s) return;

    /* Seed the sole subscription slot with a fragment. */
    uint8_t sub[128];
    size_t sub_len = nom_encode_subscribe(sub, sizeof(sub));
    moq_stream_ref_t seed_ref = moq_stream_ref_from_u64(NOM_SEED_REF);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, seed_ref, sub, sub_len - 4,
                                              false, 1),
        (int)MOQ_OK);

    MOQ_TEST_CHECK(noslot_carrier_install(
        s, moq_stream_ref_from_u64(NOM_UNREL_CARRIER), false));
    size_t base = s->noslot_carrier_count;

    /* Action-block: fill the ONE slot so the no-slot terminal cannot send. */
    moq_stream_ref_t blocker = moq_stream_ref_from_u64(0x6000);
    MOQ_TEST_CHECK_EQ_INT((int)queue_close_bidi(s, blocker), (int)MOQ_OK);
    MOQ_TEST_CHECK(action_queue_full(s));

    uint8_t tgt[128];
    size_t tgt_len = nom_encode_subscribe(tgt, sizeof(tgt));
    moq_stream_ref_t target = moq_stream_ref_from_u64(NOM_TARGET_REF);

    /* 1. Complete target, NO FIN: refused, carrier installed with fin=false. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, target, tgt, tgt_len, false, 1),
        (int)MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK_EQ_INT(nom_carrier_state(s, NOM_TARGET_REF), 0);
    MOQ_TEST_CHECK_EQ_INT(nom_carrier_state(s, NOM_UNREL_CARRIER), 0);
    MOQ_TEST_CHECK_EQ_SIZE(s->noslot_carrier_count, base + 1);

    /* 2. Legal empty FIN while the blocker remains: the SAME carrier flips to
     * true, count unchanged. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, target, NULL, 0, true, 1),
        (int)MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK_EQ_INT(nom_carrier_state(s, NOM_TARGET_REF), 1);
    MOQ_TEST_CHECK_EQ_SIZE(s->noslot_carrier_count, base + 1);

    /* 3. Recover: drain the blocker, then an empty fin=false re-feed completes
     * the refusal, taking NO drain (the FIN is already carried). */
    {
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
    }
    drain_snap_t ring_before;
    drain_snap(s, &ring_before);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, target, NULL, 0, false, 1),
        (int)MOQ_OK);
    failures += nom_carrier_retired(s, base, what);
    drain_snap_t ring_after;
    drain_snap(s, &ring_after);
    failures += drain_multiset_equals(&ring_after, &ring_before,
                                      "monotonic no drain");
    /* The completion drained its own terminal; a repeat is inert. */
    {
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
    }
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, target, NULL, 0, false, 1),
        (int)MOQ_OK);
    failures += nom_carrier_retired(s, base, "monotonic-idempotent");
    moq_session_destroy(s);
}

/* Both sides of the identity-qualified RID allowance, load-bearing against the
 * REAL graph-conservation oracle at BOTH declared ids (0 for origins 1-3, 2 for
 * origin 4): a correctly linked target RID edge at the DECLARED id is ACCEPTED;
 * the same id repointed at the seed is REJECTED by identity; and a wrong-key
 * edge (id 0 while the row declares 2) is REJECTED by key. Quiet: only failures
 * print. */
static void nom_rid_allowance_selfcheck(void)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_SERVER);
    cfg.version = MOQ_VERSION_DRAFT_18;
    cfg.max_subscriptions = 2;
    moq_session_t *s = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_create(&cfg, 0, &s), (int)MOQ_OK);
    if (!s) return;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(s, 0), (int)MOQ_OK);
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
    uint8_t setup[16];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, setup, sizeof(setup));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_setup(&w), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_control_bytes(s, setup, moq_buf_writer_offset(&w),
                                          0),
        (int)MOQ_OK);
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) moq_event_cleanup(&ev);
    while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
    uint8_t sub[128];
    size_t sub_len = nom_encode_subscribe(sub, sizeof(sub));
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(
            s, moq_stream_ref_from_u64(NOM_SEED_REF), sub, sub_len - 4,
            false, 1),
        (int)MOQ_OK);
    og_graph_t g0;
    og_capture(s, &g0);
    /* A second staging entry whose declared identity IS the target. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(
            s, moq_stream_ref_from_u64(NOM_TARGET_REF), sub, sub_len - 4,
            false, 1),
        (int)MOQ_OK);
    moq_request_endpoint_t tep = request_registry_find_by_streamref(
        s, moq_stream_ref_from_u64(NOM_TARGET_REF));
    MOQ_TEST_CHECK_EQ_INT((int)tep.kind, (int)MOQ_REQ_SUBSCRIPTION);
    moq_request_endpoint_t sep = request_registry_find_by_streamref(
        s, moq_stream_ref_from_u64(NOM_SEED_REF));
    MOQ_TEST_CHECK_EQ_INT((int)sep.kind, (int)MOQ_REQ_SUBSCRIPTION);

    /* id 0 (origins 1-3). POSITIVE: the identity-linked rid-0 edge is accepted;
     * NEGATIVE: the same id repointed at the SEED owner is rejected. */
    request_registry_insert_by_id(s, 0, tep);
    MOQ_TEST_CHECK_EQ_INT(
        nom_graph_conserved(s, &g0, NOM_TARGET_REF, 0, "rid0-positive"), 0);
    request_registry_remove_by_id(s, 0);
    request_registry_insert_by_id(s, 0, sep);
    og_quiet = 1;
    MOQ_TEST_CHECK(
        nom_graph_conserved(s, &g0, NOM_TARGET_REF, 0, "rid0-identity") > 0);
    og_quiet = 0;
    request_registry_remove_by_id(s, 0);

    /* id 2 (origin 4). POSITIVE: the identity-linked rid-2 edge is accepted at
     * the declared id 2. */
    request_registry_insert_by_id(s, 2, tep);
    MOQ_TEST_CHECK_EQ_INT(
        nom_graph_conserved(s, &g0, NOM_TARGET_REF, 2, "rid2-positive"), 0);
    /* NEGATIVE (wrong key): a rid-2 edge is rejected when the row declares id 0,
     * and symmetrically a rid-0 edge is rejected when the row declares id 2. */
    og_quiet = 1;
    MOQ_TEST_CHECK(
        nom_graph_conserved(s, &g0, NOM_TARGET_REF, 0, "rid2-wrongkey") > 0);
    og_quiet = 0;
    request_registry_remove_by_id(s, 2);
    request_registry_insert_by_id(s, 0, tep);
    og_quiet = 1;
    MOQ_TEST_CHECK(
        nom_graph_conserved(s, &g0, NOM_TARGET_REF, 2, "rid0-wrongkey") > 0);
    og_quiet = 0;
    request_registry_remove_by_id(s, 0);
    /* NEGATIVE (identity): a rid-2 edge repointed at the SEED is rejected. */
    request_registry_insert_by_id(s, 2, sep);
    og_quiet = 1;
    MOQ_TEST_CHECK(
        nom_graph_conserved(s, &g0, NOM_TARGET_REF, 2, "rid2-identity") > 0);
    og_quiet = 0;
    request_registry_remove_by_id(s, 2);
    moq_session_destroy(s);
}

static void test_no_owner_admission_matrix(void)
{
    nom_outcome_selfcheck();
    nom_rid_allowance_selfcheck();
    MOQ_TEST_CHECK_EQ_INT(nom_perturb, 0);

    /* Controls first: they emit no event and pin the exact terminal branches. */
    run_nom_control(true, false);
    run_nom_control(true, true);
    run_nom_control(false, false);
    run_nom_control(false, true);

    /* Origin 1 is reachable only without a FIN (the preflight is !fin-gated). */
    nom_case_t c;
    memset(&c, 0, sizeof(c));
    c.origin = 1; c.pre_setup = false; c.fin = false; c.target_rid = 0;
    c.name = "o1-drain-nofin";       run_nom_case(&c);
    c.origin = 2; c.pre_setup = true;
    c.fin = false; c.name = "o2-presetup-nofin"; run_nom_case(&c);
    c.fin = true;  c.name = "o2-presetup-fin";   run_nom_case(&c);
    c.origin = 3; c.pre_setup = false;
    c.fin = false; c.name = "o3-action-nofin";   run_nom_case(&c);
    c.fin = true;  c.name = "o3-action-fin";     run_nom_case(&c);
    /* Origin 4's live announcement blocker commits inbound id 0, so its target
     * is the next id, 2. */
    c.origin = 4; c.pre_setup = false; c.target_rid = 2;
    c.fin = false; c.name = "o4-send-nofin";     run_nom_case(&c);
    c.fin = true;  c.name = "o4-send-fin";       run_nom_case(&c);

    run_nom_carrier_monotonic();
}


/* -- Failed subscription REQUEST_UPDATE (§10.9.1) --------------------- */

/*
 * A REQUEST_UPDATE the publisher cannot satisfy terminates the subscription.
 * draft-18 §10.9.1: "When a REQUEST_UPDATE is unsuccessful, the publisher MUST
 * also terminate the subscription by sending a PUBLISH_DONE with error code
 * UPDATE_FAILED." The source reaches that through the shared
 * sub_reset_subgroups, so the subscription's open data streams are cancelled
 * first and its subgroup slots released rather than left pinned behind a
 * terminated subscription.
 *
 * Like the rest of Axis 3, this terminal reserves its drain FIN-aware: on a
 * stream-correlated profile it takes one NORMAL reference only when the peer
 * half is still open (req_recv_fin false), so a genuinely open peer half can
 * absorb a late in-flight message, a same-call FIN owes none, and an exhausted
 * ring makes the terminal wait exactly when a reference is owed.
 */

#define FU_SUBGROUPS 2

/* Declared, non-default acceptance values. */
#define FU_TRACK_ALIAS    0x51
#define FU_LARGEST_GROUP  0x22
#define FU_LARGEST_OBJECT 0x07
#define FU_EXPIRES_MS     9000

/* Captured at arm time, in the POOL order sub_reset_subgroups scans, since
 * that is the order the cancellations are queued in. */
static moq_stream_ref_t g_fu_sg_refs[FU_SUBGROUPS];
static size_t g_fu_sg_count;

static size_t enc_subscribe_pub_role(uint8_t *buf, size_t cap, uint64_t rid)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_namespace_t ns = { parts, 1 };
    moq_d18_msg_params_t p = { 0 };
    /* A subscriber-declared subgroup DELIVERY_TIMEOUT, so every subgroup the
     * publisher opens gets a FINITE deadline and the terminal's disarm is
     * observable rather than vacuous. */
    p.has_subgroup_delivery_timeout = true;
    p.subgroup_delivery_timeout_ms = 5000;
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_d18_encode_subscribe(&w, rid, &ns, MOQ_BYTES_LITERAL("v0"), &p),
        (int)MOQ_OK);
    return moq_buf_writer_offset(&w);
}

/* A REQUEST_UPDATE that is well-formed on the wire and fails on its CONTENT:
 * one USE_ALIAS naming an alias the peer never registered. A malformed message
 * would close the session at the decoder and never reach this terminal. */
static size_t enc_update_unknown_alias(uint8_t *buf, size_t cap)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    moq_d18_msg_params_t p = { 0 };
    p.auth_token_count = 1;
    p.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_USE_ALIAS;
    p.auth_tokens[0].alias = 99;
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_d18_encode_request_update(&w, 2, &p), (int)MOQ_OK);
    return moq_buf_writer_offset(&w);
}

/* An ESTABLISHED publisher-role subscription with FU_SUBGROUPS open data
 * streams, all through the public API: the peer's SUBSCRIBE arrives as request
 * bytes, the application accepts it, and the application opens the subgroups. */
/* Per-subgroup identity the setup declares and the terminal must preserve. */
static uint64_t g_fu_sg_group[FU_SUBGROUPS];
static uint64_t g_fu_sg_payload[FU_SUBGROUPS];
static uint64_t g_fu_sg_written[FU_SUBGROUPS];
static uint8_t  g_fu_sg_priority[FU_SUBGROUPS];

/* An ESTABLISHED publisher-role subscription with FU_SUBGROUPS data streams,
 * all through the public API: the peer's SUBSCRIBE arrives as request bytes
 * (carrying a subgroup DELIVERY_TIMEOUT so the opened streams get finite
 * deadlines), the application accepts it, opens the subgroups at distinct
 * times so their deadlines differ, and leaves each one STREAMING with a
 * partially written object -- so the counters and deadlines the terminal must
 * clear are all non-default before it runs. */
static moq_stream_ref_t arm_failed_update_subscription(moq_session_t *s)
{
    moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
    uint8_t msg[192];
    size_t n = enc_subscribe_pub_role(msg, sizeof(msg), 0);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1),
        (int)MOQ_OK);

    moq_subscription_t sub = MOQ_SUBSCRIPTION_INVALID;
    {
        int req = 0, other = 0;
        moq_event_t e;
        while (moq_session_poll_events(s, &e, 1) > 0) {
            if (e.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                req++; sub = e.u.subscribe_request.sub;
            } else {
                other++;
                TXS_DIAG("TXN failed-update arm: unexpected event kind %u\n",
                         (unsigned)e.kind);
            }
            moq_event_cleanup(&e);
        }
        MOQ_TEST_CHECK_EQ_INT(req, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
    }

    /* Explicit non-default values, so the response is compared against what
     * the fixture ASKED for rather than against whatever the session chose. */
    moq_accept_subscribe_cfg_t acfg;
    moq_accept_subscribe_cfg_init(&acfg);
    acfg.has_track_alias = true;
    acfg.track_alias = FU_TRACK_ALIAS;
    acfg.has_largest = true;
    acfg.largest_group = FU_LARGEST_GROUP;
    acfg.largest_object = FU_LARGEST_OBJECT;
    acfg.has_expires = true;
    acfg.expires_ms = FU_EXPIRES_MS;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_subscribe(s, sub, &acfg, 1),
                          (int)MOQ_OK);

    /* The acceptance owes exactly one SUBSCRIBE_OK on the request bidi -- the
     * subscribe family answers with its own message, not the generic
     * REQUEST_OK -- with our send half left open, a decodable payload, and
     * nothing after the envelope. */
    {
        int oks = 0, other = 0;
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) {
            if (a.kind == MOQ_ACTION_SEND_BIDI_STREAM &&
                a.u.send_bidi_stream.stream_ref._v == ref._v) {
                oks++;
                MOQ_TEST_CHECK_EQ_INT(a.u.send_bidi_stream.fin ? 1 : 0, 0);
                moq_buf_reader_t rr;
                moq_buf_reader_init(&rr, a.u.send_bidi_stream.data,
                                    a.u.send_bidi_stream.len);
                moq_control_envelope_t env;
                MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_envelope(&rr, &env),
                                      (int)MOQ_OK);
                MOQ_TEST_CHECK_EQ_U64(env.msg_type,
                                      (uint64_t)MOQ_D18_SUBSCRIBE_OK);
                MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&rr),
                                       (size_t)0);
                moq_d18_subscribe_ok_t ok;
                MOQ_TEST_CHECK_EQ_INT(
                    (int)moq_d18_decode_subscribe_ok(env.payload,
                                                     env.payload_len, &ok),
                    (int)MOQ_OK);
                /* Its values against the DECLARED accept configuration, and
                 * the same values against the live entry. */
                MOQ_TEST_CHECK_EQ_U64(ok.track_alias, FU_TRACK_ALIAS);
                MOQ_TEST_CHECK_EQ_INT(ok.params.has_largest ? 1 : 0, 1);
                MOQ_TEST_CHECK_EQ_U64(ok.params.largest_group,
                                      FU_LARGEST_GROUP);
                MOQ_TEST_CHECK_EQ_U64(ok.params.largest_object,
                                      FU_LARGEST_OBJECT);
                MOQ_TEST_CHECK_EQ_INT(ok.params.has_expires ? 1 : 0, 1);
                MOQ_TEST_CHECK_EQ_U64(ok.params.expires_ms, FU_EXPIRES_MS);
                MOQ_TEST_CHECK_EQ_INT(ok.track_properties_unsupported ? 1 : 0,
                                      0);
                MOQ_TEST_CHECK_EQ_INT(ok.dynamic_groups ? 1 : 0, 0);
                MOQ_TEST_CHECK_EQ_SIZE(ok.track_properties.len, (size_t)0);
                MOQ_TEST_CHECK_EQ_SIZE(ok.params.auth_token_count, (size_t)0);
                {
                    const moq_sub_entry_t *le = sub_entry_by_ref(s, ref);
                    MOQ_TEST_CHECK(le != NULL);
                    if (le) MOQ_TEST_CHECK_EQ_U64(le->track_alias,
                                                  FU_TRACK_ALIAS);
                }
            } else {
                other++;
                TXS_DIAG("TXN failed-update arm: unexpected accept action %u\n",
                         (unsigned)a.kind);
            }
            moq_action_cleanup(&a);
        }
        MOQ_TEST_CHECK_EQ_INT(oks, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
    }

    /* Real subgroups, opened publicly at distinct times so the negotiated
     * delivery timeout yields a distinct deadline for each. */
    moq_subgroup_handle_t sgh[FU_SUBGROUPS];
    for (int i = 0; i < FU_SUBGROUPS; i++) {
        moq_subgroup_cfg_t sgc;
        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = (uint64_t)(10 + i);
        sgc.subgroup_id = 0;
        /* Distinct and NON-default, so a header that fell back to the
         * default-priority encoding cannot pass. */
        sgc.publisher_priority = (uint8_t)(17 + i);
        g_fu_sg_group[i] = sgc.group_id;
        g_fu_sg_priority[i] = sgc.publisher_priority;
        sgh[i] = MOQ_SUBGROUP_INVALID;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_open_subgroup(s, sub, &sgc, (uint64_t)(2 + i),
                                           &sgh[i]), (int)MOQ_OK);
        MOQ_TEST_CHECK(moq_subgroup_is_valid(sgh[i]));
    }

    /* The transport refs, in the pool order the reset loop scans -- which is
     * what fixes the order of the cancellations it queues. */
    g_fu_sg_count = 0;
    for (size_t i = 0; i < s->sg_cap; i++) {
        if (s->subgroups[i].state == MOQ_SG_FREE) continue;
        if (!moq_subscription_eq(s->subgroups[i].sub, sub)) continue;
        MOQ_TEST_CHECK(g_fu_sg_count < FU_SUBGROUPS);
        if (g_fu_sg_count < FU_SUBGROUPS)
            g_fu_sg_refs[g_fu_sg_count] = s->subgroups[i].stream_ref;
        g_fu_sg_count++;
    }
    MOQ_TEST_CHECK_EQ_SIZE(g_fu_sg_count, (size_t)FU_SUBGROUPS);

    /* The opens are a BIJECTION onto those refs: exactly one write per ref,
     * none missing, none repeated, each carrying that subgroup's own header
     * with our send half open. */
    {
        int seen[FU_SUBGROUPS] = { 0 };
        int other = 0;
        /* Every header must carry the DECLARED alias -- not whatever the live
         * entry ended up holding, which an open-path mutation could move in
         * step with the wire. The entry is reasserted against the same
         * constant separately. */
        const uint64_t alias = FU_TRACK_ALIAS;
        {
            const moq_sub_entry_t *ae = sub_entry_by_ref(s, ref);
            MOQ_TEST_CHECK(ae != NULL);
            if (ae) MOQ_TEST_CHECK_EQ_U64(ae->track_alias, FU_TRACK_ALIAS);
        }
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) {
            if (a.kind != MOQ_ACTION_SEND_DATA) {
                other++;
                moq_action_cleanup(&a);
                continue;
            }
            int idx = -1;
            for (size_t k = 0; k < g_fu_sg_count; k++)
                if (g_fu_sg_refs[k]._v == a.u.send_data.stream_ref._v)
                    idx = (int)k;
            MOQ_TEST_CHECK(idx >= 0);
            if (idx < 0) { other++; moq_action_cleanup(&a); continue; }
            seen[idx]++;
            MOQ_TEST_CHECK_EQ_INT(a.u.send_data.fin ? 1 : 0, 0);
            moq_buf_reader_t hr;
            moq_buf_reader_init(&hr, a.u.send_data.header,
                                a.u.send_data.header_len);
            moq_d18_subgroup_header_t hdr;
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_d18_decode_subgroup_header(&hr, &hdr), (int)MOQ_OK);
            MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&hr), (size_t)0);
            MOQ_TEST_CHECK(moq_d18_subgroup_type_valid(hdr.type));
            MOQ_TEST_CHECK_EQ_U64(hdr.group_id, g_fu_sg_group[idx]);
            MOQ_TEST_CHECK_EQ_U64(hdr.subgroup_id, 0);
            /* The publisher spells the subgroup id out on the wire (mode 2)
             * rather than relying on the zero-mode shorthand. */
            MOQ_TEST_CHECK_EQ_INT(hdr.subgroup_id_mode, 2);
            MOQ_TEST_CHECK_EQ_INT(hdr.has_properties ? 1 : 0, 0);
            MOQ_TEST_CHECK_EQ_INT(hdr.end_of_group ? 1 : 0, 0);
            MOQ_TEST_CHECK_EQ_INT(hdr.first_object ? 1 : 0, 0);
            /* Spelled out, and exactly the value asked for. */
            MOQ_TEST_CHECK_EQ_INT(hdr.default_priority ? 1 : 0, 0);
            MOQ_TEST_CHECK_EQ_INT((int)hdr.publisher_priority,
                                  (int)g_fu_sg_priority[idx]);
            /* The alias is the declared one, on every stream. */
            MOQ_TEST_CHECK_EQ_U64(hdr.track_alias, alias);
            /* A header-only write: no object bytes ride the open. */
            MOQ_TEST_CHECK(a.u.send_data.payload == NULL);
            moq_action_cleanup(&a);
        }
        MOQ_TEST_CHECK_EQ_INT(other, 0);
        for (size_t k = 0; k < g_fu_sg_count; k++)
            MOQ_TEST_CHECK_EQ_INT(seen[k], 1);
    }

    /* Leave each stream mid-object: a declared payload length with only part
     * of it written, so streaming_payload_len and streaming_bytes_written are
     * both non-zero and differ per stream. */
    for (int i = 0; i < FU_SUBGROUPS; i++) {
        static const uint8_t body[8] = { 1, 2, 3, 4, 5, 6, 7, 8 };
        g_fu_sg_payload[i] = (uint64_t)(16 + i);
        g_fu_sg_written[i] = (uint64_t)(4 + i);
        moq_begin_object_cfg_t bc;
        moq_begin_object_cfg_init(&bc);
        bc.object_id = 0;
        bc.payload_length = g_fu_sg_payload[i];
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_begin_object_ex(s, sgh[i], &bc, 3), (int)MOQ_OK);
        moq_rcbuf_t *chunk = NULL;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_rcbuf_create(moq_alloc_default(), body,
                                  (size_t)g_fu_sg_written[i], &chunk),
            (int)MOQ_OK);
        MOQ_TEST_CHECK(chunk != NULL);
        if (chunk) {
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_session_write_object_data(s, sgh[i], chunk, 3),
                (int)MOQ_OK);
            moq_rcbuf_decref(chunk);
        }
    }
    /* Exactly the object header and the one payload write per stream. */
    {
        static const uint8_t body[8] = { 1, 2, 3, 4, 5, 6, 7, 8 };
        int hdrs[FU_SUBGROUPS] = { 0 }, pays[FU_SUBGROUPS] = { 0 };
        int other = 0;
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) {
            int idx = -1;
            if (a.kind == MOQ_ACTION_SEND_DATA)
                for (size_t k = 0; k < g_fu_sg_count; k++)
                    if (g_fu_sg_refs[k]._v == a.u.send_data.stream_ref._v)
                        idx = (int)k;
            if (idx < 0) { other++; moq_action_cleanup(&a); continue; }
            MOQ_TEST_CHECK_EQ_INT(a.u.send_data.fin ? 1 : 0, 0);
            if (a.u.send_data.header_len > 0) {
                /* The object header: no payload rides with it, and its bytes
                 * are exactly the declared object -- id 0 with this stream's
                 * declared payload length. There is no public object-header
                 * decoder, so the comparison is against a re-encode through
                 * the same profile op the session used. */
                hdrs[idx]++;
                MOQ_TEST_CHECK(a.u.send_data.payload == NULL);
                uint8_t want[32];
                moq_buf_writer_t hw;
                moq_buf_writer_init(&hw, want, sizeof(want));
                moq_object_header_encode_args_t oa;
                memset(&oa, 0, sizeof(oa));
                oa.object_id = 0;
                oa.payload_len = g_fu_sg_payload[idx];
                MOQ_TEST_CHECK_EQ_INT(
                    (int)s->profile->encode_object_header(
                        (moq_session_t *)s, &hw, &oa), (int)MOQ_OK);
                failures += txs_check_span_bytes(
                    a.u.send_data.header, (size_t)a.u.send_data.header_len,
                    want, moq_buf_writer_offset(&hw),
                    "failed-update object header");
            } else {
                /* The payload write: exactly the bytes handed in. */
                pays[idx]++;
                MOQ_TEST_CHECK(a.u.send_data.payload != NULL);
                if (a.u.send_data.payload) {
                    failures += txs_check_span_bytes(
                        moq_rcbuf_data(a.u.send_data.payload),
                        moq_rcbuf_len(a.u.send_data.payload), body,
                        (size_t)g_fu_sg_written[idx],
                        "failed-update payload rcbuf");
                }
            }
            moq_action_cleanup(&a);
        }
        MOQ_TEST_CHECK_EQ_INT(other, 0);
        for (size_t k = 0; k < g_fu_sg_count; k++) {
            MOQ_TEST_CHECK_EQ_INT(hdrs[k], 1);
            MOQ_TEST_CHECK_EQ_INT(pays[k], 1);
        }
    }
    {
        int n_ev = 0;
        moq_event_t e;
        while (moq_session_poll_events(s, &e, 1) > 0) { n_ev++; moq_event_cleanup(&e); }
        MOQ_TEST_CHECK_EQ_INT(n_ev, 0);
    }

    /* The streams really are in the non-default shape the oracle needs. */
    {
        uint64_t seen_deadline[FU_SUBGROUPS];
        size_t k = 0;
        for (size_t i = 0; i < s->sg_cap && k < g_fu_sg_count; i++) {
            if (s->subgroups[i].state == MOQ_SG_FREE) continue;
            if (!moq_subscription_eq(s->subgroups[i].sub, sub)) continue;
            MOQ_TEST_CHECK_EQ_INT((int)s->subgroups[i].state,
                                  (int)MOQ_SG_STREAMING);
            MOQ_TEST_CHECK_EQ_U64(s->subgroups[i].streaming_payload_len,
                                  g_fu_sg_payload[k]);
            MOQ_TEST_CHECK_EQ_U64(s->subgroups[i].streaming_bytes_written,
                                  g_fu_sg_written[k]);
            MOQ_TEST_CHECK(s->subgroups[i].delivery_deadline_us != UINT64_MAX);
            seen_deadline[k] = s->subgroups[i].delivery_deadline_us;
            k++;
        }
        MOQ_TEST_CHECK_EQ_SIZE(k, g_fu_sg_count);
        /* Distinct, so a per-stream disarm cannot be confused with a shared
         * one. */
        for (size_t i = 0; i + 1 < k; i++)
            MOQ_TEST_CHECK(seen_deadline[i] != seen_deadline[i + 1]);
        MOQ_TEST_CHECK(s->subgroup_deadline_us != UINT64_MAX);
    }

    /* The ABSOLUTE owner shape the terminal will act on. */
    MOQ_TEST_CHECK_EQ_INT(sub_pool_busy(s), 1);
    const moq_sub_entry_t *se = sub_entry_by_ref(s, ref);
    MOQ_TEST_CHECK(se != NULL);
    if (se) {
        MOQ_TEST_CHECK_EQ_INT((int)se->state, (int)MOQ_SUB_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT((int)se->role, (int)MOQ_SUB_ROLE_PUBLISHER);
        MOQ_TEST_CHECK_EQ_U64(se->handle._opaque, sub._opaque);
        MOQ_TEST_CHECK_EQ_U64(se->request_id, 0);
        MOQ_TEST_CHECK_EQ_U64(se->request_stream_ref._v, ref._v);
        MOQ_TEST_CHECK_EQ_INT(se->req_recv_fin ? 1 : 0, 0);
        MOQ_TEST_CHECK_EQ_SIZE(se->req_recv_len, (size_t)0);
        check_registered(s, ref, MOQ_REQ_SUBSCRIPTION, (int)(se - s->subs));
    }
    return ref;
}

/* The failed-update terminal touches state the generic snapshot does not
 * model: the subscription's data streams (their state, generation, streaming
 * counters and delivery deadline), the aggregate subgroup deadline, and the
 * send buffer the two atomic messages would fill. A refusal must leave every
 * one of them exactly as it was, so they are captured and compared rather than
 * spot-checked. */
typedef struct fu_extra {
    size_t   count;
    uint64_t ref[FU_SUBGROUPS];
    int      state[FU_SUBGROUPS];
    uint32_t generation[FU_SUBGROUPS];
    uint64_t payload_len[FU_SUBGROUPS];
    uint64_t bytes_written[FU_SUBGROUPS];
    uint64_t deadline[FU_SUBGROUPS];
    int      overflow;
    uint64_t agg_deadline;
    size_t   send_len;
} fu_extra_t;

static fu_extra_t g_fu_extra;

/* The exact send-buffer cost of the terminal's two atomic messages, declared
 * from the same encoders the output oracle uses. */
static size_t g_fu_committed_bytes;

static void fu_declare_committed_bytes(void)
{
    uint8_t err[128], done[128];
    size_t n = enc_request_error(err, sizeof(err), 0x17, 0, NULL);
    moq_buf_writer_t dw;
    moq_buf_writer_init(&dw, done, sizeof(done));
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_d18_encode_publish_done(&dw, 0x8, 0, (moq_bytes_t){0}),
        (int)MOQ_OK);
    g_fu_committed_bytes = n + moq_buf_writer_offset(&dw);
}

static void fu_extra_read(const moq_session_t *s, fu_extra_t *v)
{
    memset(v, 0, sizeof(*v));
    for (size_t i = 0; i < s->sg_cap; i++) {
        if (s->subgroups[i].state == MOQ_SG_FREE) continue;
        if (v->count >= FU_SUBGROUPS) { v->overflow = 1; break; }
        v->ref[v->count]           = s->subgroups[i].stream_ref._v;
        v->state[v->count]         = (int)s->subgroups[i].state;
        v->generation[v->count]    = s->subgroups[i].generation;
        v->payload_len[v->count]   = s->subgroups[i].streaming_payload_len;
        v->bytes_written[v->count] = s->subgroups[i].streaming_bytes_written;
        v->deadline[v->count]      = s->subgroups[i].delivery_deadline_us;
        v->count++;
    }
    v->agg_deadline = s->subgroup_deadline_us;
    v->send_len = s->send_len;
}

static void fu_extra_capture(const moq_session_t *s)
{
    fu_declare_committed_bytes();
    fu_extra_read(s, &g_fu_extra);
    MOQ_TEST_CHECK_EQ_INT(g_fu_extra.overflow, 0);
    MOQ_TEST_CHECK_EQ_SIZE(g_fu_extra.count, (size_t)FU_SUBGROUPS);
}

#define FU_EQ(now_v, want_v, label) do { \
        if ((now_v) != (want_v)) { \
            TXS_DIAG("TXN %s: " label " %llu, expected %llu\n", what, \
                     (unsigned long long)(now_v), \
                     (unsigned long long)(want_v)); \
            bad++; \
        } \
    } while (0)

static int fu_extra_common(const moq_session_t *s, fu_extra_t *now,
                           const char *what)
{
    fu_extra_read(s, now);
    if (now->overflow || g_fu_extra.overflow) {
        TXS_DIAG("TXN %s: incomparable subgroup capture\n", what);
        return 1;
    }
    if (now->count != g_fu_extra.count) {
        TXS_DIAG("TXN %s: %zu live subgroups, expected %zu\n", what,
                 now->count, g_fu_extra.count);
        return 1;
    }
    return 0;
}

/* A refusal leaves every one of them exactly as it found them. */
static int fu_extra_check_blocked(const moq_session_t *s, const char *what)
{
    fu_extra_t now;
    int bad = fu_extra_common(s, &now, what);
    if (bad) return bad;
    for (size_t i = 0; i < now.count; i++) {
        FU_EQ(now.ref[i],           g_fu_extra.ref[i],           "subgroup ref");
        FU_EQ(now.state[i],         g_fu_extra.state[i],         "subgroup state");
        FU_EQ(now.generation[i],    g_fu_extra.generation[i],    "subgroup generation");
        FU_EQ(now.payload_len[i],   g_fu_extra.payload_len[i],   "streaming payload length");
        FU_EQ(now.bytes_written[i], g_fu_extra.bytes_written[i], "streaming bytes written");
        FU_EQ(now.deadline[i],      g_fu_extra.deadline[i],      "subgroup deadline");
    }
    FU_EQ(now.agg_deadline, g_fu_extra.agg_deadline, "aggregate subgroup deadline");
    FU_EQ(now.send_len,     g_fu_extra.send_len,     "send buffer bytes");
    return bad;
}

/* A completion leaves them in the committed shape sub_reset_subgroups defines
 * (session_subscribe.c:2006): each stream keeps its identity, moves to
 * RESETTING, has its streaming counters cleared and its deadline disarmed, and
 * the aggregate deadline is recomputed away. The SLOTS are not free yet -- a
 * RESETTING entry is reclaimed later, by the terminal reap in
 * sg_reap_terminal_resumable (session_subgroup.c:131). */
static int fu_extra_check_committed(const moq_session_t *s, const char *what)
{
    fu_extra_t now;
    int bad = fu_extra_common(s, &now, what);
    if (bad) return bad;
    for (size_t i = 0; i < now.count; i++) {
        FU_EQ(now.ref[i],        g_fu_extra.ref[i],        "subgroup ref");
        FU_EQ(now.generation[i], g_fu_extra.generation[i], "subgroup generation");
        FU_EQ(now.state[i],      (uint64_t)MOQ_SG_RESETTING, "subgroup state");
        FU_EQ(now.payload_len[i],   0, "streaming payload length");
        FU_EQ(now.bytes_written[i], 0, "streaming bytes written");
        FU_EQ(now.deadline[i], UINT64_MAX, "subgroup deadline");
    }
    FU_EQ(now.agg_deadline, UINT64_MAX, "aggregate subgroup deadline");
    /* The two atomic messages went out, so the send buffer holds exactly what
     * they cost -- not whatever was left over from an earlier attempt. */
    FU_EQ(now.send_len, g_fu_extra.send_len + g_fu_committed_bytes,
          "send buffer bytes");
    return bad;
}
#undef FU_EQ

static const extra_ops_t fu_extra_ops = {
    fu_extra_capture, fu_extra_check_blocked, fu_extra_check_committed
};

/* One image for a queued data-stream cancellation. */
static void want_reset_data(txs_norm_vec_t *v, moq_stream_ref_t ref,
                            uint64_t code)
{
    txs_img_t im;
    txs_img_init(&im);
    txs_img_u64(&im, ref._v);
    txs_img_u64(&im, code);
    MOQ_TEST_CHECK(txs_norm_append_img(v, MOQ_ACTION_RESET_DATA, &im));
}

/* One SEND_BIDI image over an already-encoded control message. */
static void want_bidi_msg(txs_norm_vec_t *v, moq_stream_ref_t ref, bool fin,
                          const uint8_t *msg, size_t len)
{
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, msg, len);
    moq_control_envelope_t env;
    MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_envelope(&r, &env), (int)MOQ_OK);
    txs_img_t im;
    txs_img_init(&im);
    txs_img_u64(&im, ref._v);
    txs_img_u64(&im, fin ? 1u : 0u);
    txs_img_u64(&im, env.msg_type);
    txs_img_bytes(&im, env.payload, env.payload_len);
    MOQ_TEST_CHECK(txs_norm_append_img(v, MOQ_ACTION_SEND_BIDI_STREAM, &im));
}

/* Exactly what §10.9.1 plus the shared teardown owe, in order: one
 * cancellation per bound subgroup, then the REQUEST_ERROR, then the terminal
 * PUBLISH_DONE closing our send half. No event at all. */
static void want_failed_update_output(txs_norm_vec_t *v, moq_stream_ref_t ref)
{
    for (size_t i = 0; i < g_fu_sg_count; i++)
        want_reset_data(v, g_fu_sg_refs[i], 0x1);   /* CANCELLED (§3.3.3) */

    uint8_t err[128];
    size_t err_len = enc_request_error(err, sizeof(err), 0x17, 0, NULL);
    want_bidi_msg(v, ref, false, err, err_len);

    uint8_t done[128];
    moq_buf_writer_t dw;
    moq_buf_writer_init(&dw, done, sizeof(done));
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_d18_encode_publish_done(&dw, 0x8, 0, (moq_bytes_t){0}),
        (int)MOQ_OK);
    want_bidi_msg(v, ref, true, done, moq_buf_writer_offset(&dw));
}

static void test_axis3_fin_terminals(void)
{
    /* session_namespace.c:158 -- announce REQUEST_UPDATE rejection. */
    static const terminal_case_t ann_update = {
        .name = "announce update rejected", .make = make_session_default,
        .arm = arm_established_announcement, .encode = enc_update_next,
        .drain_reason = MOQ_DRAIN_NORMAL,
        .want_output = want_update_rejected, .hooks = &pub_hooks_for_actions,
        .owner = &ann_owner_ops
    };
    /* session_subscribe_tracks.c:479 -- its publisher-side mirror. */
    static const terminal_case_t st_update = {
        .name = "track-sub update rejected", .make = make_session_default,
        .arm = arm_established_track_sub, .encode = enc_update_next,
        .drain_reason = MOQ_DRAIN_NORMAL,
        .want_output = want_update_rejected, .hooks = &pub_hooks_for_actions,
        .owner = &st_owner_ops
    };

    /* profile_d18.c:1092 -- GOAWAY on a committed, peer-opened request bidi. */
    static const terminal_case_t goaway_request_route = {
        .name = "goaway request route", .make = make_session_default,
        .arm = arm_goaway_announcement, .encode = enc_goaway_request,
        .drain_reason = MOQ_DRAIN_GOAWAY_STRICT,
        .want_output = want_goaway_output, .hooks = &goaway_hooks,
        .owner = &ann_owner_ops
    };

    run_terminal_matrix(&ann_update, 2);
    run_terminal_matrix(&st_update, 2);
    run_terminal_matrix(&goaway_request_route, 2);

    /* profile_d18.c:2143 -- GOAWAY on our own, locally-opened request bidi. */
    {
        static const terminal_case_t goaway_response_route = {
            .name = "goaway response route", .make = make_session_client,
            .arm = arm_goaway_local_subscribe, .encode = enc_goaway_request_uri,
            .drain_reason = MOQ_DRAIN_GOAWAY_STRICT,
            .want_output = want_goaway_output_uri, .hooks = &goaway_hooks,
            .owner = &sub_owner_ops
        };
        run_terminal_matrix(&goaway_response_route, 2);
    }

    /* session_namespace_sub.c:1441 -- the durable-latch control. */
    {
        static const terminal_case_t goaway_ns_sub = {
            .name = "goaway ns_sub", .make = make_session_client,
            .arm = arm_goaway_local_ns_sub, .encode = enc_goaway_request_uri,
            .drain_reason = MOQ_DRAIN_GOAWAY_STRICT,
            .want_output = want_goaway_output_uri, .hooks = &goaway_hooks,
            .owner = &nsg_owner_ops
        };
        run_terminal_matrix(&goaway_ns_sub, 2);
    }
    run_ns_goaway_latched();

    /* session_subscribe.c:2226 -- the failed REQUEST_UPDATE termination. It
     * follows the SAME unified rule as every other row: a genuinely open peer
     * half owes exactly one releasable normal reference, a same-call FIN owes
     * none, and an exhausted ring refuses rather than terminating without one. */
    {
        static const terminal_case_t failed_update = {
            .name = "failed subscription update",
            .make = make_session_default,
            .arm = arm_failed_update_subscription,
            .encode = enc_update_unknown_alias,
            .drain_reason = MOQ_DRAIN_NORMAL,
            .want_output = want_failed_update_output,
            .hooks = &pub_hooks_for_actions,
            .owner = &sub_owner_ops,
            .extra = &fu_extra_ops
        };
        run_terminal_matrix(&failed_update, 2);
    }
}


/* -- ns_sub_send_request_error: both reject callers ------------------- */

/*
 * A SUBSCRIBE_NAMESPACE the responder refuses before the request is ever
 * app-visible: the auth walk rejects an unknown USE_ALIAS
 * (`session_namespace_sub.c:761`), or the prefix overlaps a live namespace
 * subscription (`:774`). Both reach `ns_sub_send_request_error` (`:365`),
 * which queues REQUEST_ERROR with FIN, drains the request bidi and frees the
 * transient entry.
 *
 * The drain is reserved FIN-aware: `ns_sub_send_request_error` takes a reference
 * only when `uses_request_streams && stream_ref && !peer_fin_observed`
 * (`session_namespace_sub.c:386`), where `peer_fin_observed` folds the current
 * FIN, the durable `pending_fin` latch and the `handoff_fin_pending` marker. So
 * a same-call FIN owes no drain and those rows are drainless under the unified
 * rule.
 *
 * Retention here is a ONE-owner story. The handoff transfers ownership of the
 * bidi to the `MOQ_NS_SUB_RECVING_PUBLISHER` entry and frees the generic
 * staging owner even on `MOQ_ERR_WOULD_BLOCK` (`session_subscribe.c:1310`),
 * because retries route through `idx_ns_by_ref` and never back through the
 * staging slot. The inline comment at `profile_d18.c:1590` is about
 * `ns_sub_on_new_bidi()` refusing before handoff on ns-sub pool exhaustion and
 * is accurate.
 */

static size_t enc_sns(uint8_t *buf, size_t cap, uint64_t rid, const char *field,
                      bool unknown_alias)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    moq_bytes_t parts[] = { { (const uint8_t *)field, strlen(field) } };
    moq_namespace_t ns = { parts, 1 };
    moq_d18_msg_params_t p = { 0 };
    if (unknown_alias) {
        p.auth_token_count = 1;
        p.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_USE_ALIAS;
        p.auth_tokens[0].alias = 99;
    }
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_d18_encode_subscribe_namespace(&w, rid, &ns, &p), (int)MOQ_OK);
    return moq_buf_writer_offset(&w);
}

static size_t enc_sns_auth(uint8_t *buf, size_t cap, uint64_t rid)
{
    return enc_sns(buf, cap, rid, "auth", true);
}

/* The same prefix the seeded subscription holds: an exact overlap. */
static size_t enc_sns_overlap(uint8_t *buf, size_t cap, uint64_t rid)
{
    return enc_sns(buf, cap, rid, "live", false);
}

/* Consume and retire inbound request id 0 through a complete, exactly checked
 * exchange, so no fixture in this family carries the ordinal-zero key whose
 * cross-owner deletion is BACKLOG #252. */
static void nsr_burn_request_id_0(moq_session_t *s)
{
    moq_stream_ref_t ref = moq_stream_ref_from_u64(9);
    uint8_t msg[192];
    size_t n = enc_sns(msg, sizeof(msg), 0, "burn", false);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1),
        (int)MOQ_OK);
    moq_ns_sub_handle_t h = MOQ_NS_SUB_HANDLE_INVALID;
    {
        int req = 0, other = 0;
        moq_event_t e;
        while (moq_session_poll_events(s, &e, 1) > 0) {
            if (e.kind == MOQ_EVENT_NS_SUB_REQUEST) {
                req++; h = e.u.ns_sub_request.handle;
            } else other++;
            moq_event_cleanup(&e);
        }
        MOQ_TEST_CHECK_EQ_INT(req, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
    }
    moq_reject_ns_sub_cfg_t rc;
    moq_reject_ns_sub_cfg_init(&rc);
    rc.error_code = MOQ_REQUEST_ERROR_NOT_SUPPORTED;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_reject_ns_sub(s, h, &rc, 1),
                          (int)MOQ_OK);
    /* Exactly one REQUEST_ERROR, decoded: our declared code, an empty reason,
     * FIN closing our send half, and nothing after the envelope. */
    {
        int sends = 0, other = 0;
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) {
            if (a.kind == MOQ_ACTION_SEND_BIDI_STREAM &&
                a.u.send_bidi_stream.stream_ref._v == ref._v) {
                sends++;
                MOQ_TEST_CHECK_EQ_INT(a.u.send_bidi_stream.fin ? 1 : 0, 1);
                uint8_t want[128];
                size_t wn = enc_request_error(want, sizeof(want),
                    MOQ_REQUEST_ERROR_NOT_SUPPORTED, 0, NULL);
                failures += txs_check_span_bytes(a.u.send_bidi_stream.data,
                                                 a.u.send_bidi_stream.len,
                                                 want, wn,
                                                 "ns_sub rejection wire");
            } else other++;
            moq_action_cleanup(&a);
        }
        MOQ_TEST_CHECK_EQ_INT(sends, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
    }
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
    /* The peer FINs its half; the entry and its drain reference go with it. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
        (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(ns_sub_pool_busy(s), 0);
    MOQ_TEST_CHECK_EQ_INT(sub_pool_busy(s), 0);
    MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, (size_t)0);
    check_no_output(s, &pub_hooks_for_actions);
    /* And no edge of the burned request survives anywhere: a stale key would
     * otherwise sit in the graph every later assertion is measured against. */
    {
        og_graph_t g;
        og_capture(s, &g);
        failures += og_check_integrity(&g, "burn retired");
        failures += og_check_no_edge(&g, OG_DOM_REQ_RID, 0, "burn retired");
        failures += og_check_no_edge(&g, OG_DOM_NS_REF, ref._v, "burn retired");
        failures += og_check_no_edge(&g, OG_DOM_REQ_STREAMREF, ref._v,
                                     "burn retired");
    }
}


/* -- the retained overlap seed --------------------------------------- */

static moq_stream_ref_t g_nsr_seed_ref;
static uint64_t g_nsr_seed_rid;
static int      g_nsr_seed_slot;
static nso_snap_t g_nsr_seed_want;

/* The seeded owner -- whose expected record is DECLARED at arm time, not read
 * back -- and its exact edge closure must survive every call, replay and late
 * FIN in the overlap branch. */
static void nsr_seed_check(const moq_session_t *s, const char *what)
{
    if (g_nsr_seed_slot < 0) return;      /* branch has no seed */
    nso_snap_t now;
    nso_read(s, g_nsr_seed_slot, &now);
    failures += nso_diff(&now, &g_nsr_seed_want, what);

    og_graph_t g;
    og_capture(s, &g);
    failures += og_check_integrity(&g, what);
    {
        /* An ESTABLISHED namespace subscription holds its own index edge and
         * nothing else: the acceptance removes the pending-phase by-id key
         * (session_namespace_sub.c:1155) as it transitions. */
        const og_edge_spec_t want[] = {
            { OG_DOM_NS_REF, g_nsr_seed_ref._v },
        };
        failures += og_check_owner_edges(&g, MOQ_REQ_NAMESPACE_SUB,
                                         g_nsr_seed_slot, want, 1, what);
    }
    failures += og_check_no_edge(&g, OG_DOM_REQ_RID, g_nsr_seed_rid, what);
    failures += og_check_no_edge(&g, OG_DOM_REQ_STREAMREF, g_nsr_seed_ref._v,
                                 what);
}

/* -- the reject descriptor and its runner ---------------------------- */

typedef struct nsr_case {
    const char *name;
    /* Prepares the session and returns the request id the trigger carries. */
    uint64_t (*arm)(moq_session_t *s);
    size_t (*encode)(uint8_t *buf, size_t cap, uint64_t rid);
    uint64_t err_code;
    /* Applies this branch's own mutations to the expected transient record,
     * which the runner has already seeded from the PRE-CALL slot state. */
    void (*declare_transient)(nso_snap_t *want, const nso_snap_t *pre);
} nsr_case_t;

static int nsr_problems(const nsr_case_t *c)
{
    int bad = 0;
    if (!c->name || !c->arm || !c->encode || !c->declare_transient) bad++;
    if (c->err_code == 0) bad++;
    return bad;
}

/* The single REQUEST_ERROR the reject owes, with FIN closing our send half. */
static void want_nsr_output(txs_norm_vec_t *v, moq_stream_ref_t ref,
                            uint64_t code)
{
    uint8_t buf[128];
    size_t n = enc_request_error(buf, sizeof(buf), code, 0, NULL);
    want_bidi_msg(v, ref, true, buf, n);
}

static void check_nsr_output(moq_session_t *s, moq_stream_ref_t ref,
                             uint64_t code, const char *what)
{
    txs_norm_vec_t got, want;
    txs_norm_init(&got);
    txs_norm_init(&want);
    collect_events(s, &pub_hooks_for_actions, &got);
    collect_actions(s, &pub_hooks_for_actions, &got);
    want_nsr_output(&want, ref, code);
    failures += txs_norm_equals(&got, &want, what);
    txs_norm_free(&got);
    txs_norm_free(&want);
}

/* The one owner a refusal leaves behind, compared against a record built from
 * the PRE-CALL slot state plus this branch's declared mutations -- so nothing
 * the refused call did can define its own expectation. */
static void check_nsr_blocked_owner(const moq_session_t *s,
                                    moq_stream_ref_t ref, uint64_t rid,
                                    int want_slot, const nso_snap_t *want,
                                    const char *what)
{
    /* The generic staging owner is GONE, with its stream-ref edge. */
    MOQ_TEST_CHECK_EQ_INT(sub_pool_busy(s), 0);
    MOQ_TEST_CHECK_EQ_INT(
        (int)request_registry_find_by_streamref(s, ref).kind, (int)MOQ_REQ_NONE);

    int want_busy = (g_nsr_seed_slot >= 0) ? 2 : 1;
    MOQ_TEST_CHECK_EQ_INT(ns_sub_pool_busy(s), want_busy);
    int32_t slot = moq_index_find(s->idx_ns_by_ref, s->idx_ns_mask, ref._v);
    /* The slot the handoff was DECLARED to take, not whichever one it took. */
    MOQ_TEST_CHECK_EQ_INT((int)slot, want_slot);
    if (slot < 0) return;

    nso_snap_t now;
    nso_read(s, slot, &now);
    failures += nso_diff(&now, want, what);

    /* Its edges: the namespace-ref edge it owns and NOTHING else -- the
     * request is not app-visible, so no by-id key is committed. */
    og_graph_t g;
    og_capture(s, &g);
    failures += og_check_integrity(&g, what);
    {
        const og_edge_spec_t want_e[] = { { OG_DOM_NS_REF, ref._v } };
        failures += og_check_owner_edges(&g, MOQ_REQ_NAMESPACE_SUB, slot,
                                         want_e, 1, what);
    }
    failures += og_check_no_edge(&g, OG_DOM_REQ_RID, rid, what);
    failures += og_check_no_edge(&g, OG_DOM_REQ_STREAMREF, ref._v, what);
    nsr_seed_check(s, what);
}

static void check_nsr_retired(const moq_session_t *s, moq_stream_ref_t ref,
                              uint64_t rid, const char *what)
{
    int want_busy = (g_nsr_seed_slot >= 0) ? 1 : 0;
    MOQ_TEST_CHECK_EQ_INT(ns_sub_pool_busy(s), want_busy);
    MOQ_TEST_CHECK_EQ_INT(sub_pool_busy(s), 0);
    og_graph_t g;
    og_capture(s, &g);
    failures += og_check_integrity(&g, what);
    failures += og_check_no_edge(&g, OG_DOM_NS_REF, ref._v, what);
    failures += og_check_no_edge(&g, OG_DOM_REQ_RID, rid, what);
    failures += og_check_no_edge(&g, OG_DOM_REQ_STREAMREF, ref._v, what);
    nsr_seed_check(s, what);
}

/* One branch under one (FIN, drain-ring) state. The blocked rows compare an
 * EMPTY re-feed against a snapshot taken once the transient owner exists,
 * since the owner is created by the very call under test. */
static void run_nsr(const nsr_case_t *c, bool fin_in_call, bool fill_ring,
                    size_t split)
{
    char what[128];
    moq_session_t *s = make_session_default();
    if (!s) return;
    MOQ_TEST_CHECK_EQ_INT(nsr_problems(c), 0);
    uint64_t rid = c->arm(s);
    moq_stream_ref_t ref = moq_stream_ref_from_u64(21);
    MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, (size_t)0);

    uint8_t m[192];
    size_t n = c->encode(m, sizeof(m), rid);
    MOQ_TEST_CHECK(n > 0 && split < n);
    if (split >= n) { moq_session_destroy(s); return; }

    if (fill_ring) fill_drain_ring(s);
    drain_snap_t ring_before;
    drain_snap(s, &ring_before);
    /* Captured BEFORE the terminal feed, so a refusal is measured against the
     * session as it was, not against whatever that same refused call left
     * behind. The destination slot and the generation it will carry are
     * DERIVED here too: ns_sub_on_new_bidi takes the first free slot and sets
     * `generation |= 1` (session_namespace_sub.c:942-950), so both are known
     * before the call and are never read back from the mutated owner. */
    txs_snapshot_t pre_call;
    txs_capture(s, &ref, 1, &pre_call);
    expect_after_call_prepare(&pre_call);
    int want_slot = -1;
    uint32_t want_gen = 0;
    uint64_t want_handle = 0;
    for (size_t i = 0; i < s->ns_sub_cap; i++) {
        if (s->ns_subs[i].state == MOQ_NS_SUB_FREE) {
            want_slot = (int)i;
            want_gen = s->ns_subs[i].generation | 1u;
            /* The handoff mints NO handle -- the entry is not app-visible
             * until it commits -- so whatever the slot carried must survive
             * unchanged. */
            want_handle = s->ns_subs[i].handle._opaque;
            break;
        }
    }
    MOQ_TEST_CHECK(want_slot >= 0);
    if (want_slot < 0) { moq_session_destroy(s); return; }
    nso_snap_t pre_slot;
    nso_read(s, want_slot, &pre_slot);
    MOQ_TEST_CHECK_EQ_INT(pre_slot.invalid, 0);

    if (split > 0) {
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, m, split, false, 1),
            (int)MOQ_OK);
        check_no_output(s, &pub_hooks_for_actions);
    }

    /* The declared inventory a refusal must leave: the pre-call slot, plus
     * the universal handoff mutations, plus this branch's own. */
    nso_snap_t want_owner = pre_slot;
    want_owner.present = 1;
    want_owner.state = (int)MOQ_NS_SUB_RECVING_PUBLISHER;
    want_owner.stream_kind = (int)MOQ_STREAM_KIND_NAMESPACE_SUB;
    want_owner.generation = want_gen;
    want_owner.handle = want_handle;
    want_owner.stream_ref = ref._v;
    want_owner.request_id = rid;
    want_owner.ep_kind = (int)MOQ_REQ_NAMESPACE_SUB;
    want_owner.ep_slot = want_slot;
    want_owner.ep_has_request_id = 1;
    want_owner.ep_request_id = rid;
    want_owner.ep_has_stream_ref = 1;
    want_owner.ep_stream_ref = ref._v;
    want_owner.parse_complete = 0;
    want_owner.got_response = 0;
    want_owner.pending_fin = 0;
    want_owner.closing_remote_error = 0;
    want_owner.goaway_sent = 0;
    want_owner.auth_committed = 0;
    want_owner.prefix_valid = 0;
    want_owner.prefix_len = 0;
    want_owner.recv_len = n;
    MOQ_TEST_CHECK(n <= NSO_BUF_MAX);
    memcpy(want_owner.recv, m, n);
    c->declare_transient(&want_owner, &pre_slot);

    moq_result_t want_rc = (fill_ring && !fin_in_call) ? MOQ_ERR_WOULD_BLOCK
                                                       : MOQ_OK;
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, m + split, n - split,
                                              fin_in_call, 1),
        (int)want_rc);
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);

    if (want_rc == MOQ_ERR_WOULD_BLOCK) {
        snprintf(what, sizeof(what), "%s blocked", c->name);
        check_nsr_blocked_owner(s, ref, rid, want_slot, &want_owner, what);
        check_no_output(s, &pub_hooks_for_actions);
        {
            drain_snap_t now;
            drain_snap(s, &now);
            failures += drain_multiset_equals(&now, &ring_before, what);
        }
        /* The ONE declared change the refused call is allowed to make: the
         * bidi's owner moves from the generic staging subscription to the
         * namespace-sub entry. Every other session field must be as it was
         * before the feed. */
        {
            txs_snapshot_t expect = pre_call;
            if (expect.owner_count == 1) {
                expect.owners[0].kind = TXS_OWNER_NS;
                expect.owners[0].slot = want_slot;
                expect.owners[0].state = (int)MOQ_NS_SUB_RECVING_PUBLISHER;
                expect.owners[0].generation = want_gen;
                expect.owners[0].invalid = 0;
            }
            snprintf(what, sizeof(what), "%s handoff delta", c->name);
            failures += txs_check_eq(s, &ref, 1, &expect, what);
        }
        snprintf(what, sizeof(what), "%s blocked", c->name);

        /* The transient owner's inventory as the blocked state left it. Every
         * field is already declared absolutely above; this second, independent
         * comparison is what pins CONSERVATION across the re-feed, so a field
         * that drifts between the two calls is caught even though both were
         * measured against the same declared record. */
        nso_snap_t blocked0;
        {
            int32_t bslot = moq_index_find(s->idx_ns_by_ref, s->idx_ns_mask,
                                           ref._v);
            nso_read(s, bslot, &blocked0);
            MOQ_TEST_CHECK_EQ_INT(blocked0.invalid, 0);
        }

        /* An empty re-feed against a snapshot taken with the owner already
         * present: nothing at all may move while the ring stays full. */
        txs_snapshot_t before;
        txs_capture(s, &ref, 1, &before);
        expect_after_call_prepare(&before);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, false, 1),
            (int)MOQ_ERR_WOULD_BLOCK);
        snprintf(what, sizeof(what), "%s refeed blocked", c->name);
        check_nsr_blocked_owner(s, ref, rid, want_slot, &want_owner, what);
        {
            nso_snap_t now;
            int32_t bslot = moq_index_find(s->idx_ns_by_ref, s->idx_ns_mask,
                                           ref._v);
            nso_read(s, bslot, &now);
            failures += nso_diff(&now, &blocked0, what);
        }
        failures += txs_check_eq(s, &ref, 1, &before, what);
        check_no_output(s, &pub_hooks_for_actions);
        {
            drain_snap_t now;
            drain_snap(s, &now);
            failures += drain_multiset_equals(&now, &ring_before, what);
        }

        /* Release one declared filler, then replay with no new bytes. */
        moq_stream_ref_t filler = moq_stream_ref_from_u64(0x4000);
        MOQ_TEST_CHECK(drain_ref_contains(s, filler));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, filler, NULL, 0, true, 1),
            (int)MOQ_OK);
        drain_snap_t ring_freed;
        drain_snap_minus(&ring_before, filler, MOQ_DRAIN_NORMAL, &ring_freed);

        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, false, 1),
            (int)MOQ_OK);
        snprintf(what, sizeof(what), "%s replayed", c->name);
        check_nsr_output(s, ref, c->err_code, what);
        check_nsr_retired(s, ref, rid, what);
        drain_snap_t after, want_ring;
        drain_snap(s, &after);
        drain_snap_plus(&ring_freed, ref, MOQ_DRAIN_NORMAL, &want_ring);
        failures += drain_multiset_equals(&after, &want_ring, what);
        check_drain_ref_reason(s, ref, MOQ_DRAIN_NORMAL);

        /* The late FIN releases exactly that reference and owes nothing. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
            (int)MOQ_OK);
        drain_snap_t released;
        drain_snap(s, &released);
        snprintf(what, sizeof(what), "%s replay released", c->name);
        failures += drain_multiset_equals(&released, &ring_freed, what);
        check_nsr_retired(s, ref, rid, what);
        check_no_output(s, &pub_hooks_for_actions);
        moq_session_destroy(s);
        return;
    }

    snprintf(what, sizeof(what), "%s output", c->name);
    check_nsr_output(s, ref, c->err_code, what);
    check_nsr_retired(s, ref, rid, what);

    drain_snap_t now, want_ring;
    drain_snap(s, &now);
    if (fin_in_call) {
        want_ring = ring_before;      /* the peer half is already closed */
    } else {
        drain_snap_plus(&ring_before, ref, MOQ_DRAIN_NORMAL, &want_ring);
        check_drain_ref_reason(s, ref, MOQ_DRAIN_NORMAL);
    }
    snprintf(what, sizeof(what), "%s granted", c->name);
    failures += drain_multiset_equals(&now, &want_ring, what);

    if (!fin_in_call) {
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
            (int)MOQ_OK);
        drain_snap_t after;
        drain_snap(s, &after);
        snprintf(what, sizeof(what), "%s released", c->name);
        failures += drain_multiset_equals(&after, &ring_before, what);
        check_nsr_retired(s, ref, rid, what);
        check_no_output(s, &pub_hooks_for_actions);
    }
    moq_session_destroy(s);
}

static void run_nsr_matrix(const nsr_case_t *c, size_t split)
{
    run_nsr(c, false, false, 0);   /* no FIN: one releasable reference */
    run_nsr(c, true,  false, 0);   /* same-call FIN: none needed */
    run_nsr(c, true,  true,  0);   /* exhausted ring, FIN observed */
    run_nsr(c, false, true,  0);   /* exhausted ring, no FIN */
    run_nsr(c, false, true,  split);            /* fragmented */
}

/* -- the two branches ------------------------------------------------ */

/* The auth walk runs and its verdict is retained for the replay; the request
 * shape is never applied and the prefix stage is never reached, so interest,
 * forward and the prefix count all keep whatever the slot held before. */
static void nsr_auth_declare(nso_snap_t *want, const nso_snap_t *pre)
{
    want->auth_processed = 1;
    /* ZERO resolved tokens: an unknown USE_ALIAS sets the reject code and
     * returns before the token is appended (session_auth.c:381), so the walk
     * leaves nothing staged (`:486`). */
    want->auth_reject_code = 0x17;
    want->token_count = 0;
    want->interest = pre->interest;
    want->forward = pre->forward;
    want->prefix_count = pre->prefix_count;
}

/* Auth passes, so the decoded request shape IS applied, and the conflicting
 * prefix is stored and then released when the reject cannot be queued
 * (`session_namespace_sub.c:776`) -- which also zeroes its count. */
static void nsr_overlap_declare(nso_snap_t *want, const nso_snap_t *pre)
{
    (void)pre;
    want->auth_processed = 1;
    want->auth_reject_code = 0;
    want->token_count = 0;
    want->interest = (int)MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
    want->forward = 1;
    want->prefix_count = 0;
}

static uint64_t nsr_arm_auth(moq_session_t *s)
{
    g_nsr_seed_slot = -1;
    g_nsr_seed_ref = moq_stream_ref_from_u64(0);
    nsr_burn_request_id_0(s);
    return 2;
}

/* An ACCEPTED namespace subscription on prefix "live" at request id 2, which
 * the overlapping request at id 4 must leave completely untouched. */
static uint64_t nsr_arm_overlap(moq_session_t *s)
{
    g_nsr_seed_slot = -1;
    nsr_burn_request_id_0(s);

    g_nsr_seed_ref = moq_stream_ref_from_u64(11);
    g_nsr_seed_rid = 2;
    /* The slot the handoff will take, the generation it will carry and the
     * handle the commit will therefore mint, ALL derived before ingress --
     * so a consistently wrong slot or handle cannot define its own
     * expectation. */
    int seed_slot = -1;
    uint32_t seed_gen = 0;
    for (size_t i = 0; i < s->ns_sub_cap; i++)
        if (s->ns_subs[i].state == MOQ_NS_SUB_FREE) {
            seed_slot = (int)i;
            seed_gen = s->ns_subs[i].generation | 1u;
            break;
        }
    MOQ_TEST_CHECK(seed_slot >= 0);
    if (seed_slot < 0) return 4;
    /* Storage is knowable BEFORE ingress: the receive buffer is carved out of
     * the session allocation at create, so a free slot already exposes it. */
    const void *seed_pre_buf = seed_slot >= 0
        ? (const void *)s->ns_subs[seed_slot].recv_buf : NULL;
    size_t seed_pre_cap = seed_slot >= 0 ? s->ns_subs[seed_slot].recv_cap : 0;
    uint64_t seed_handle = moq_handle_pack(MOQ_HANDLE_POOL_NAMESPACE_SUB,
                                           s->session_tag, seed_gen,
                                           (uint32_t)seed_slot);
    uint8_t msg[192];
    size_t n = enc_sns(msg, sizeof(msg), g_nsr_seed_rid, "live", false);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, g_nsr_seed_ref, msg, n,
                                              false, 1), (int)MOQ_OK);
    moq_ns_sub_handle_t h = MOQ_NS_SUB_HANDLE_INVALID;
    {
        int req = 0, other = 0;
        moq_event_t e;
        while (moq_session_poll_events(s, &e, 1) > 0) {
            if (e.kind == MOQ_EVENT_NS_SUB_REQUEST) {
                req++; h = e.u.ns_sub_request.handle;
            } else other++;
            moq_event_cleanup(&e);
        }
        MOQ_TEST_CHECK_EQ_INT(req, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
        MOQ_TEST_CHECK_EQ_U64(h._opaque, seed_handle);
    }
    moq_accept_ns_sub_cfg_t ac;
    moq_accept_ns_sub_cfg_init(&ac);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_ns_sub(s, h, &ac, 1),
                          (int)MOQ_OK);
    /* Exactly one REQUEST_OK, decoded, with our send half left OPEN so the
     * namespace stream can carry later NAMESPACE messages. */
    {
        int sends = 0, other = 0;
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) {
            if (a.kind == MOQ_ACTION_SEND_BIDI_STREAM &&
                a.u.send_bidi_stream.stream_ref._v == g_nsr_seed_ref._v) {
                sends++;
                MOQ_TEST_CHECK_EQ_INT(a.u.send_bidi_stream.fin ? 1 : 0, 0);
                moq_buf_reader_t rr;
                moq_buf_reader_init(&rr, a.u.send_bidi_stream.data,
                                    a.u.send_bidi_stream.len);
                moq_control_envelope_t env;
                MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_envelope(&rr, &env),
                                      (int)MOQ_OK);
                MOQ_TEST_CHECK_EQ_U64(env.msg_type,
                                      (uint64_t)MOQ_D18_REQUEST_OK);
                MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&rr),
                                       (size_t)0);
                MOQ_TEST_CHECK_EQ_INT(
                    (int)moq_d18_decode_request_ok(env.payload,
                                                   env.payload_len),
                    (int)MOQ_OK);
            } else other++;
            moq_action_cleanup(&a);
        }
        MOQ_TEST_CHECK_EQ_INT(sends, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
    }
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_index_find(s->idx_ns_by_ref, s->idx_ns_mask,
                            g_nsr_seed_ref._v), seed_slot);
    g_nsr_seed_slot = seed_slot;
    MOQ_TEST_CHECK_EQ_INT(ns_sub_pool_busy(s), 1);
    MOQ_TEST_CHECK_EQ_INT(sub_pool_busy(s), 0);
    MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, (size_t)0);
    /* The seed's expected record, DECLARED field by field rather than read
     * back -- slot, generation and handle all come from the pool state
     * captured before the request was fed. */
    memset(&g_nsr_seed_want, 0, sizeof(g_nsr_seed_want));
    g_nsr_seed_want.present = 1;
    g_nsr_seed_want.state = (int)MOQ_NS_SUB_ESTABLISHED;
    g_nsr_seed_want.stream_kind = (int)MOQ_STREAM_KIND_NAMESPACE_SUB;
    g_nsr_seed_want.generation = seed_gen;
    g_nsr_seed_want.handle = seed_handle;
    g_nsr_seed_want.request_id = g_nsr_seed_rid;
    g_nsr_seed_want.stream_ref = g_nsr_seed_ref._v;
    g_nsr_seed_want.ep_kind = (int)MOQ_REQ_NAMESPACE_SUB;
    g_nsr_seed_want.ep_slot = g_nsr_seed_slot;
    g_nsr_seed_want.ep_has_request_id = 1;
    g_nsr_seed_want.ep_request_id = g_nsr_seed_rid;
    g_nsr_seed_want.ep_has_stream_ref = 1;
    g_nsr_seed_want.ep_stream_ref = g_nsr_seed_ref._v;
    g_nsr_seed_want.interest = (int)MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
    g_nsr_seed_want.forward = 1;
    g_nsr_seed_want.parse_complete = 1;
    g_nsr_seed_want.auth_processed = 1;
    g_nsr_seed_want.auth_committed = 1;
    g_nsr_seed_want.prefix_valid = 1;
    g_nsr_seed_want.prefix_count = 1;
    /* Its stored extent is the declared length of "live"; its receive storage
     * is the slot's own, carved out at session create and captured from the
     * FREE slot before this request was fed. */
    g_nsr_seed_want.prefix_buf_len = 4;
    g_nsr_seed_want.recv_buf = seed_pre_buf;
    g_nsr_seed_want.recv_cap = seed_pre_cap;
    {
        static const uint8_t k_live[] = { 4, 'l', 'i', 'v', 'e' };
        g_nsr_seed_want.prefix_len = sizeof(k_live);
        memcpy(g_nsr_seed_want.prefix, k_live, sizeof(k_live));
    }
    g_nsr_seed_want.recv_len = n;
    memcpy(g_nsr_seed_want.recv, msg, n);
    /* It really is that record before it becomes the conservation baseline. */
    {
        nso_snap_t got;
        nso_read(s, g_nsr_seed_slot, &got);
        failures += nso_diff(&got, &g_nsr_seed_want, "seed declared");
    }
    return 4;
}

static void test_ns_sub_request_error(void)
{
    static const nsr_case_t auth_reject = {
        .name = "ns_sub auth reject", .arm = nsr_arm_auth,
        .encode = enc_sns_auth, .err_code = 0x17,
        .declare_transient = nsr_auth_declare
    };
    static const nsr_case_t overlap_reject = {
        .name = "ns_sub prefix overlap", .arm = nsr_arm_overlap,
        .encode = enc_sns_overlap,
        .err_code = MOQ_REQUEST_ERROR_PREFIX_OVERLAP,
        .declare_transient = nsr_overlap_declare
    };
    run_nsr_matrix(&auth_reject, 2);
    run_nsr_matrix(&overlap_reject, 2);
}


/* -- Axis 3 durable-latch terminals ---------------------------------- */

/*
 * These terminals run LATER than the FIN that populates their owner's latch:
 * a legal earlier exchange leaves `req_recv_fin` set on a still-live,
 * role-valid owner, and the terminal is then driven through its own public or
 * internal route. That is why they cannot use the current-call runner, which
 * assumes the terminal and the FIN arrive in one operation.
 *
 * SCOPE: DIRECT API terminals only -- routes 2, 4, 5, 6 and 7. Its fire()
 * contract assumes a direct result and zero mutation on refusal, which is
 * false for the deferred publish finalize (route 1, which defers behind
 * MOQ_OK after legitimate data progress) and for the announcement error
 * (route 3, whose terminal message itself supplies bytes and the latch).
 * Those two get their own runners.
 *
 * The declared rule is the same one every Axis 3 row uses: a genuinely open
 * peer half owes exactly one releasable MOQ_DRAIN_NORMAL reference, an
 * observed FIN owes none, and an exhausted ring must refuse rather than
 * terminate without one.
 */

typedef struct durable_case {
    const char *name;
    moq_session_t *(*make)(void);
    /* Establishes the role-valid owner with the peer FIN in the requested
     * OBSERVED state and returns its request-bidi ref; the owner handle comes
     * back through `out_handle` so the terminal can be fired through its real
     * API.
     *
     * `fin_observed` is the unified rule's `peer_fin_observed` -- the FIN has
     * been seen on this owner's request bidi -- NOT specifically a durable
     * `req_recv_fin` latch. A route whose FIN fact is a handoff marker rather
     * than a latch is legal here, but every arm must ASSERT which of the two it
     * established, so the runner's rows can never silently claim a latch a
     * route does not hold. */
    moq_stream_ref_t (*arm)(moq_session_t *s, bool fin_observed,
                            uint64_t *out_handle);
    /* Fires the terminal through its own production route. */
    moq_result_t (*fire)(moq_session_t *s, uint64_t handle);
    void (*want_output)(txs_norm_vec_t *v, moq_stream_ref_t ref,
                        uint64_t handle);
    const txs_op_hooks_t *hooks;
    const owner_ops_t *owner;
    /* The owner's EXACT edge set, declared per family. */
    int (*owner_edges)(const og_graph_t *g, moq_stream_ref_t ref,
                       const char *what);
    /* Optional, indivisible: durable owner state the family snapshot does not
     * model. A refusal owes ZERO owner mutation, so a route may snapshot the
     * whole entry representation plus its owned bytes rather than enumerate
     * fields. */
    const extra_ops_t *extra;
    /* Set when the terminal runs inside a sweep that DISCARDS its result: the
     * refusal is then reported as MOQ_OK and is proven by the surviving owner,
     * the absent output and the unchanged ring alone. */
    int refusal_is_silent;
} durable_case_t;

static int durable_problems(const durable_case_t *c)
{
    int bad = 0;
    if (!c->name || !c->make || !c->arm || !c->fire || !c->want_output) bad++;
    if (!c->hooks || !c->hooks->normalize_event ||
        !c->hooks->normalize_action) bad++;
    if (!c->owner || !c->owner->capture || !c->owner->check ||
        !c->owner->check_retired) bad++;
    if (!c->owner_edges) bad++;
    if (c->extra && (!c->extra->capture || !c->extra->check_blocked ||
                     !c->extra->check_committed)) bad++;
    return bad;
}

static void check_durable_output(moq_session_t *s, const durable_case_t *c,
                                 moq_stream_ref_t ref, uint64_t handle,
                                 const char *what)
{
    txs_norm_vec_t got, want;
    txs_norm_init(&got);
    txs_norm_init(&want);
    collect_events(s, c->hooks, &got);
    collect_actions(s, c->hooks, &got);
    c->want_output(&want, ref, handle);
    failures += txs_norm_equals(&got, &want, what);
    txs_norm_free(&got);
    txs_norm_free(&want);
}

/* One terminal under one (FIN observed, ring) state. */
static void run_durable(const durable_case_t *c, bool fin_observed,
                        bool fill_ring)
{
    char what[128];
    moq_session_t *s = c->make();
    if (!s) return;
    MOQ_TEST_CHECK_EQ_INT(durable_problems(c), 0);
    uint64_t handle = 0;
    moq_stream_ref_t ref = c->arm(s, fin_observed, &handle);
    MOQ_TEST_CHECK(ref._v != 0);
    if (ref._v == 0) { moq_session_destroy(s); return; }
    check_no_output(s, c->hooks);

    if (fill_ring) fill_drain_ring(s);
    drain_snap_t ring_before;
    drain_snap(s, &ring_before);

    seed_snap_t owner0;
    memset(&owner0, 0, sizeof(owner0));
    c->owner->capture(s, ref, &owner0);
    txs_snapshot_t before;
    txs_capture(s, &ref, 1, &before);
    expect_after_call_prepare(&before);
    if (c->extra) c->extra->capture(s);
    og_graph_t graph_before;
    og_capture(s, &graph_before);
    snprintf(what, sizeof(what), "%s armed", c->name);
    failures += og_check_integrity(&graph_before, what);
    failures += c->owner_edges(&graph_before, ref, what);

    bool refused = (fill_ring && !fin_observed);
    moq_result_t want_rc = (refused && !c->refusal_is_silent)
                               ? MOQ_ERR_WOULD_BLOCK : MOQ_OK;
    MOQ_TEST_CHECK_EQ_INT((int)c->fire(s, handle), (int)want_rc);
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);

    if (refused) {
        /* An API terminal touches no input, so the WHOLE session must be as it
         * was -- owner, snapshot, graph, output and ring alike. */
        snprintf(what, sizeof(what), "%s blocked", c->name);
        c->owner->check(s, ref, &owner0, what);
        if (c->extra) failures += c->extra->check_blocked(s, what);
        failures += txs_check_eq(s, &ref, 1, &before, what);
        check_no_output(s, c->hooks);
        {
            drain_snap_t now;
            drain_snap(s, &now);
            failures += drain_multiset_equals(&now, &ring_before, what);
            /* Not merely a valid graph: the SAME graph. An edge quietly
             * removed or repointed is still internally consistent. */
            og_graph_t g;
            og_capture(s, &g);
            failures += og_check_integrity(&g, what);
            failures += og_check_same_topology(&g, &graph_before, what);
            failures += c->owner_edges(&g, ref, what);
        }

        /* Free one declared filler and retry through the SAME production
         * route -- no peer bytes are delivered at any point. */
        moq_stream_ref_t filler = moq_stream_ref_from_u64(0x4000);
        MOQ_TEST_CHECK(drain_ref_contains(s, filler));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, filler, NULL, 0, true, 1),
            (int)MOQ_OK);
        drain_snap_t ring_freed;
        drain_snap_minus(&ring_before, filler, MOQ_DRAIN_NORMAL, &ring_freed);
        {
            drain_snap_t now;
            drain_snap(s, &now);
            snprintf(what, sizeof(what), "%s filler released", c->name);
            failures += drain_multiset_equals(&now, &ring_freed, what);
            /* Releasing an unrelated filler must not have touched the target
             * owner or the graph the retry is about to consume. */
            c->owner->check(s, ref, &owner0, what);
            og_graph_t g;
            og_capture(s, &g);
            failures += og_check_integrity(&g, what);
            failures += og_check_same_topology(&g, &graph_before, what);
            failures += c->owner_edges(&g, ref, what);
        }

        MOQ_TEST_CHECK_EQ_INT((int)c->fire(s, handle), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        snprintf(what, sizeof(what), "%s retried", c->name);
        check_durable_output(s, c, ref, handle, what);
        c->owner->check_retired(s, ref);
        {
            og_graph_t g;
            og_capture(s, &g);
            failures += og_check_integrity(&g, what);
            failures += og_check_no_edge(&g, OG_DOM_REQ_STREAMREF, ref._v, what);
        }
        drain_snap_t after, want_ring;
        drain_snap(s, &after);
        drain_snap_plus(&ring_freed, ref, MOQ_DRAIN_NORMAL, &want_ring);
        failures += drain_multiset_equals(&after, &want_ring, what);
        check_drain_ref_reason(s, ref, MOQ_DRAIN_NORMAL);

        /* The reference the retry took is RELEASABLE: the late FIN removes
         * exactly it, owes nothing, and cannot resurrect the owner. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        snprintf(what, sizeof(what), "%s retry released", c->name);
        {
            drain_snap_t released;
            drain_snap(s, &released);
            failures += drain_multiset_equals(&released, &ring_freed, what);
        }
        c->owner->check_retired(s, ref);
        check_no_output(s, c->hooks);
        {
            og_graph_t g;
            og_capture(s, &g);
            failures += og_check_integrity(&g, what);
            failures += og_check_no_edge(&g, OG_DOM_REQ_STREAMREF, ref._v, what);
        }
        c->owner->check_retired(s, ref);   /* no resurrection */
        moq_session_destroy(s);
        return;
    }

    snprintf(what, sizeof(what), "%s fired", c->name);
    check_durable_output(s, c, ref, handle, what);
    c->owner->check_retired(s, ref);
    {
        og_graph_t g;
        og_capture(s, &g);
        failures += og_check_integrity(&g, what);
        failures += og_check_no_edge(&g, OG_DOM_REQ_STREAMREF, ref._v, what);
    }

    drain_snap_t now, want_ring;
    drain_snap(s, &now);
    if (fin_observed) {
        /* The peer half is already closed: nothing to absorb. */
        want_ring = ring_before;
    } else {
        drain_snap_plus(&ring_before, ref, MOQ_DRAIN_NORMAL, &want_ring);
        check_drain_ref_reason(s, ref, MOQ_DRAIN_NORMAL);
    }
    snprintf(what, sizeof(what), "%s granted", c->name);
    failures += drain_multiset_equals(&now, &want_ring, what);

    if (!fin_observed) {
        /* The late FIN releases exactly that reference and owes nothing. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
            (int)MOQ_OK);
        drain_snap_t released;
        drain_snap(s, &released);
        snprintf(what, sizeof(what), "%s released", c->name);
        failures += drain_multiset_equals(&released, &ring_before, what);
        c->owner->check_retired(s, ref);
        check_no_output(s, c->hooks);
        {
            og_graph_t g;
            og_capture(s, &g);
            failures += og_check_integrity(&g, what);
            failures += og_check_no_edge(&g, OG_DOM_REQ_STREAMREF, ref._v,
                                         what);
        }
        c->owner->check_retired(s, ref);   /* no resurrection */
    }
    moq_session_destroy(s);
}

static void run_durable_matrix(const durable_case_t *c)
{
    run_durable(c, false, false);   /* FIN not observed: one releasable ref */
    run_durable(c, false, true);    /* exhausted ring, FIN not observed: refuse */
    run_durable(c, true,  false);   /* FIN observed: none needed */
    run_durable(c, true,  true);    /* exhausted ring, FIN observed */
}

/* -- announcer-side withdrawal (session_namespace.c:792 -> :85) ------- */

/* The slot the arm DECLARED, so the edge oracle never learns its expected
 * target from the graph it is auditing. */
static int g_ann_want_slot = -1;

static const uint8_t k_ann_ns0[] = { 'e','x','.','c','o','m' };
static const uint8_t k_ann_ns1[] = { 'l','i','v','e' };
static const uint8_t k_ann_tok[] = { 'a','n','n','t','o','k' };
#define k_ann_tok_type 11

/* A locally-opened PUBLISH_NAMESPACE the peer accepted, optionally closing its
 * half in the same REQUEST_OK. The entry stays ANNOUNCER/ESTABLISHED either
 * way, so the latch is durable state the withdrawal can consult. */

/* The WHOLE announcement: its object representation plus the bytes it owns.
 * `ann_seed_capture` models the request-family fields common to every owner
 * kind and therefore omits this entry's own -- the canonical namespace key and
 * its length, the receive-buffer capacity, and `goaway_sent` -- so a refusal is
 * additionally compared verbatim here. Bounds-safe: an over-long or
 * unbacked buffer makes the record incomparable rather than being read. */
#define ANN_OWN_MAX 256
typedef struct ann_full {
    int      valid;
    moq_ann_entry_t raw;
    size_t   nsid_len;
    uint8_t  nsid[ANN_OWN_MAX];
    size_t   recv_len;
    uint8_t  recv[ANN_OWN_MAX];
} ann_full_t;

static int g_ann_full_slot = -1;
static ann_full_t g_ann_full;
/* Route 3's terminal is driven by peer BYTES, so its owner legitimately grows
 * its receive buffer by exactly the bytes the refused call delivered; a direct
 * API terminal (route 2) may append nothing. */
static size_t g_ann_full_allow_append;

static void ann_full_read(const moq_session_t *s, ann_full_t *v)
{
    memset(v, 0, sizeof(*v));
    if (g_ann_full_slot < 0 || (size_t)g_ann_full_slot >= s->ann_cap) return;
    const moq_ann_entry_t *e = &s->announcements[g_ann_full_slot];
    v->raw = *e;
    v->nsid_len = e->ns_id_len;
    if (e->ns_id_len > ANN_OWN_MAX || (e->ns_id_len && !e->ns_id_buf)) return;
    if (e->ns_id_len) memcpy(v->nsid, e->ns_id_buf, e->ns_id_len);
    v->recv_len = e->req_recv_len;
    if (e->req_recv_len > ANN_OWN_MAX || (e->req_recv_len && !e->req_recv_buf))
        return;
    if (e->req_recv_len) memcpy(v->recv, e->req_recv_buf, e->req_recv_len);
    v->valid = 1;
}

static void ann_full_capture(const moq_session_t *s)
{
    ann_full_read(s, &g_ann_full);
    MOQ_TEST_CHECK_EQ_INT(g_ann_full.valid, 1);
}

static int ann_full_check_blocked(const moq_session_t *s, const char *what)
{
    ann_full_t now;
    ann_full_read(s, &now);
    int bad = 0;
    if (!now.valid || !g_ann_full.valid) {
        TXS_DIAG("TXN %s: incomparable announcement record\n", what);
        return 1;
    }
    /* `req_recv_len` and the bytes themselves are pinned EXACTLY by the owner
     * seed snapshot (its captured prefix plus this call's suffix, byte for
     * byte), so this record deliberately masks the length rather than
     * re-deriving the append -- and covers instead what that snapshot omits.
     * A route whose terminal takes no peer bytes declares an append of zero and
     * the mask is then a no-op. */
    moq_ann_entry_t want = g_ann_full.raw;
    want.req_recv_len = now.raw.req_recv_len;
    if (memcmp(&now.raw, &want, sizeof(want)) != 0) {
        TXS_DIAG("TXN %s: announcement entry changed\n", what);
        bad++;
    }
    if (g_ann_full_allow_append == 0 &&
        now.raw.req_recv_len != g_ann_full.raw.req_recv_len) {
        TXS_DIAG("TXN %s: retained request bytes grew under a terminal that"
                 " takes none\n", what);
        bad++;
    }
    /* Whatever the length, the prefix the owner already held is intact. */
    if (now.recv_len < g_ann_full.recv_len ||
        (g_ann_full.recv_len &&
         memcmp(now.recv, g_ann_full.recv, g_ann_full.recv_len) != 0)) {
        TXS_DIAG("TXN %s: retained request bytes changed\n", what);
        bad++;
    }
    if (now.nsid_len != g_ann_full.nsid_len ||
        (now.nsid_len &&
         memcmp(now.nsid, g_ann_full.nsid, now.nsid_len) != 0)) {
        TXS_DIAG("TXN %s: canonical namespace key changed\n", what);
        bad++;
    }
    return bad;
}

static int ann_full_check_committed(const moq_session_t *s, const char *what)
{
    if (g_ann_full_slot < 0 || (size_t)g_ann_full_slot >= s->ann_cap) return 0;
    if (s->announcements[g_ann_full_slot].state != MOQ_ANN_FREE) {
        TXS_DIAG("TXN %s: retired announcement slot is still live\n", what);
        return 1;
    }
    return 0;
}

static const extra_ops_t ann_full_ops = {
    ann_full_capture, ann_full_check_blocked, ann_full_check_committed
};

static moq_stream_ref_t arm_announcer_established(moq_session_t *s,
                                                  bool fin_observed,
                                                  uint64_t *out_handle)
{
    moq_bytes_t parts[] = { { k_ann_ns0, sizeof(k_ann_ns0) },
                            { k_ann_ns1, sizeof(k_ann_ns1) } };
    /* The public token carries type and value only; the wire encoding is
     * USE_VALUE, which is what the decoded body must show. */
    moq_auth_token_t tok;
    memset(&tok, 0, sizeof(tok));
    tok.token_type = k_ann_tok_type;
    tok.token_value = (moq_bytes_t){ k_ann_tok, sizeof(k_ann_tok) };
    moq_publish_namespace_cfg_t pc;
    moq_publish_namespace_cfg_init(&pc);
    pc.track_namespace = (moq_namespace_t){ parts, 2 };
    pc.auth_tokens = &tok;
    pc.auth_token_count = 1;
    /* The slot the request will take and the generation it will carry, both
     * derived from pool state BEFORE the call (ann_find_free takes the first
     * free slot; the commit sets `generation |= 1`), so nothing downstream can
     * discover its own expectation. */
    int want_slot = -1;
    uint32_t want_gen = 0;
    for (size_t i = 0; i < s->ann_cap; i++)
        if (s->announcements[i].state == MOQ_ANN_FREE) {
            want_slot = (int)i;
            want_gen = s->announcements[i].generation | 1u;
            break;
        }
    MOQ_TEST_CHECK(want_slot >= 0);
    if (want_slot < 0) return moq_stream_ref_from_u64(0);
    g_ann_want_slot = want_slot;
    g_ann_full_slot = want_slot;
    g_ann_full_allow_append = 0;   /* a direct API terminal appends nothing */

    moq_announcement_t ann = { 0 };
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_publish_namespace(s, &pc, 1, &ann), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(ann._opaque,
                          moq_handle_pack(MOQ_HANDLE_POOL_ANNOUNCEMENT,
                                          s->session_tag, want_gen,
                                          (uint32_t)want_slot));

    moq_stream_ref_t ref = moq_stream_ref_from_u64(0);
    {
        int opens = 0, other = 0;
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) {
            if (a.kind == MOQ_ACTION_OPEN_BIDI_STREAM) {
                opens++;
                ref = a.u.open_bidi_stream.stream_ref;
                MOQ_TEST_CHECK_EQ_INT(a.u.open_bidi_stream.fin ? 1 : 0, 0);
                moq_buf_reader_t rr;
                moq_buf_reader_init(&rr, a.u.open_bidi_stream.data,
                                    a.u.open_bidi_stream.len);
                moq_control_envelope_t env;
                memset(&env, 0, sizeof(env));
                MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_envelope(&rr, &env),
                                      (int)MOQ_OK);
                MOQ_TEST_CHECK_EQ_U64(env.msg_type,
                                      (uint64_t)MOQ_D18_PUBLISH_NAMESPACE);
                MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&rr),
                                       (size_t)0);
                /* The body against the fixture's DECLARED request, not
                 * against anything read out of the message itself. */
                moq_bytes_t dp[8];
                moq_d18_publish_namespace_t pn;
                memset(dp, 0, sizeof(dp));
                memset(&pn, 0, sizeof(pn));
                int dec_ok = (moq_d18_decode_publish_namespace(env.payload,
                                  env.payload_len, dp, 8, &pn) == MOQ_OK);
                MOQ_TEST_CHECK(dec_ok);
                if (dec_ok) {
                MOQ_TEST_CHECK_EQ_U64(pn.request_id, 0);
                MOQ_TEST_CHECK_EQ_SIZE(pn.track_namespace.count, (size_t)2);
                if (pn.track_namespace.count == 2) {
                    /* A declared part count REQUIRES the array, and a
                     * non-empty part REQUIRES its bytes -- asserted, then
                     * still guarded, so a null never reaches memcmp. */
                    MOQ_TEST_CHECK(pn.track_namespace.parts != NULL);
                }
                if (pn.track_namespace.count == 2 &&
                    pn.track_namespace.parts != NULL) {
                    const moq_bytes_t *p0 = &pn.track_namespace.parts[0];
                    const moq_bytes_t *p1 = &pn.track_namespace.parts[1];
                    MOQ_TEST_CHECK_EQ_SIZE(p0->len, sizeof(k_ann_ns0));
                    MOQ_TEST_CHECK(p0->len == 0 || p0->data != NULL);
                    if (p0->len == sizeof(k_ann_ns0) && p0->data)
                        MOQ_TEST_CHECK(memcmp(p0->data, k_ann_ns0,
                                              sizeof(k_ann_ns0)) == 0);
                    MOQ_TEST_CHECK_EQ_SIZE(p1->len, sizeof(k_ann_ns1));
                    MOQ_TEST_CHECK(p1->len == 0 || p1->data != NULL);
                    if (p1->len == sizeof(k_ann_ns1) && p1->data)
                        MOQ_TEST_CHECK(memcmp(p1->data, k_ann_ns1,
                                              sizeof(k_ann_ns1)) == 0);
                }
                MOQ_TEST_CHECK_EQ_SIZE(pn.params.auth_token_count, (size_t)1);
                /* `auth_tokens` is a fixed array inside the decoded params,
                 * not a pointer, so there is nothing to null-check there; only
                 * each token's borrowed VALUE can be absent. */
                if (pn.params.auth_token_count == 1) {
                    MOQ_TEST_CHECK_EQ_INT(
                        (int)pn.params.auth_tokens[0].alias_type,
                        (int)MOQ_AUTH_TOKEN_USE_VALUE);
                    MOQ_TEST_CHECK_EQ_U64(pn.params.auth_tokens[0].token_type,
                                          k_ann_tok_type);
                    const moq_bytes_t *tv =
                        &pn.params.auth_tokens[0].token_value;
                    MOQ_TEST_CHECK_EQ_SIZE(tv->len, sizeof(k_ann_tok));
                    MOQ_TEST_CHECK(tv->len == 0 || tv->data != NULL);
                    if (tv->len == sizeof(k_ann_tok) && tv->data)
                        MOQ_TEST_CHECK(memcmp(tv->data, k_ann_tok,
                                              sizeof(k_ann_tok)) == 0);
                }
                }
            } else other++;
            moq_action_cleanup(&a);
        }
        MOQ_TEST_CHECK_EQ_INT(opens, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
    }
    MOQ_TEST_CHECK(ref._v != 0);
    if (ref._v == 0) return ref;

    uint8_t ok[32];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, ok, sizeof(ok));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_request_ok(&w), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, ok,
                                              moq_buf_writer_offset(&w),
                                              fin_observed, 1), (int)MOQ_OK);
    {
        int accepted = 0, other = 0;
        moq_event_t e;
        while (moq_session_poll_events(s, &e, 1) > 0) {
            if (e.kind == MOQ_EVENT_NAMESPACE_ACCEPTED) {
                accepted++;
                MOQ_TEST_CHECK_EQ_U64(e.u.namespace_accepted.ann._opaque,
                                      ann._opaque);
            } else other++;
            moq_event_cleanup(&e);
        }
        MOQ_TEST_CHECK_EQ_INT(accepted, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
    }

    /* The ABSOLUTE owner shape the terminal will act on. */
    MOQ_TEST_CHECK_EQ_INT(ann_busy_count(s), 1);
    {
        /* The owner physically occupies the DECLARED slot. */
        const moq_ann_entry_t *e = &s->announcements[want_slot];
        MOQ_TEST_CHECK_EQ_INT((int)e->state, (int)MOQ_ANN_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT((int)e->role, (int)MOQ_ANN_ROLE_ANNOUNCER);
        MOQ_TEST_CHECK_EQ_U64(e->generation, want_gen);
        MOQ_TEST_CHECK_EQ_U64(e->handle._opaque, ann._opaque);
        MOQ_TEST_CHECK_EQ_U64(e->request_id, 0);
        MOQ_TEST_CHECK_EQ_U64(e->request_stream_ref._v, ref._v);
        MOQ_TEST_CHECK_EQ_INT(e->req_recv_fin ? 1 : 0, fin_observed ? 1 : 0);
        /* And the registry edge targets that declared slot. */
        check_registered(s, ref, MOQ_REQ_ANNOUNCEMENT, want_slot);
    }
    if (out_handle) *out_handle = ann._opaque;
    return ref;
}

/* An announcement owns exactly its request-stream-ref edge: the family keys
 * its bidi there and nowhere else. */
static int ann_owner_edges(const og_graph_t *g, moq_stream_ref_t ref,
                           const char *what)
{
    int bad = 0;
    if (g_ann_want_slot < 0) {
        OG_DIAG("OG %s: no declared announcement slot\n", what);
        return 1;
    }
    bad += og_check_edge(g, OG_DOM_REQ_STREAMREF, ref._v,
                         MOQ_REQ_ANNOUNCEMENT, g_ann_want_slot, what);
    {
        const og_edge_spec_t want[] = { { OG_DOM_REQ_STREAMREF, ref._v } };
        bad += og_check_owner_edges(g, MOQ_REQ_ANNOUNCEMENT, g_ann_want_slot,
                                    want, 1, what);
    }
    bad += og_check_no_edge(g, OG_DOM_NS_REF, ref._v, what);
    return bad;
}

static moq_result_t fire_publish_namespace_done(moq_session_t *s,
                                                uint64_t handle)
{
    moq_announcement_t ann;
    ann._opaque = handle;
    return moq_session_publish_namespace_done(s, ann, 1);
}

static void want_announcer_teardown(txs_norm_vec_t *v, moq_stream_ref_t ref,
                                    uint64_t handle)
{
    (void)handle;
    want_nsl_output(v, ref);   /* one ABORT_BIDI_STREAM(ref, CANCELLED) */
}

static txs_op_hooks_t durable_abort_hooks = {
    NULL, NULL, NULL, NULL, no_event_expected, nsl_norm_action
};


/* -- subscriber-side unsubscribe (session_subscribe.c:3012) ----------- */

/*
 * A locally-issued SUBSCRIBE the peer accepted, optionally closing its half in
 * the same SUBSCRIBE_OK. `subscribe_request_bidi_cancel` reserves its drain
 * FIN-aware: it takes a NORMAL reference only when the peer half is still open
 * (req_recv_fin false), so a same-call FIN owes none and the latch rows are
 * drainless under the unified rule.
 */

#define R6_TRACK_ALIAS   0x63
#define R6_LARGEST_GROUP 0x41
#define R6_LARGEST_OBJ   0x09
#define R6_EXPIRES_MS    7000
static const uint8_t k_r6_name[] = { 'v','0' };

static int g_r6_want_slot = -1;

static int sub_owner_edges(const og_graph_t *g, moq_stream_ref_t ref,
                           const char *what)
{
    int bad = 0;
    if (g_r6_want_slot < 0) {
        OG_DIAG("OG %s: no declared subscription slot\n", what);
        return 1;
    }
    bad += og_check_edge(g, OG_DOM_REQ_STREAMREF, ref._v,
                         MOQ_REQ_SUBSCRIPTION, g_r6_want_slot, what);
    {
        const og_edge_spec_t want[] = { { OG_DOM_REQ_STREAMREF, ref._v } };
        bad += og_check_owner_edges(g, MOQ_REQ_SUBSCRIPTION, g_r6_want_slot,
                                    want, 1, what);
    }
    bad += og_check_no_edge(g, OG_DOM_NS_REF, ref._v, what);
    return bad;
}

static moq_stream_ref_t arm_subscriber_established(moq_session_t *s,
                                                   bool fin_observed,
                                                   uint64_t *out_handle)
{
    /* The slot and generation the request will take, derived from pool state
     * BEFORE the call (sub_find_free takes the first free slot; the request
     * sets `generation |= 1`). */
    int want_slot = -1;
    uint32_t want_gen = 0;
    for (size_t i = 0; i < s->sub_cap; i++)
        if (s->subs[i].state == MOQ_SUB_FREE) {
            want_slot = (int)i;
            want_gen = s->subs[i].generation | 1u;
            break;
        }
    MOQ_TEST_CHECK(want_slot >= 0);
    if (want_slot < 0) return moq_stream_ref_from_u64(0);
    g_r6_want_slot = want_slot;

    moq_bytes_t parts[] = { { k_ann_ns0, sizeof(k_ann_ns0) },
                            { k_ann_ns1, sizeof(k_ann_ns1) } };
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ parts, 2 };
    sc.track_name = MOQ_BYTES_LITERAL("v0");
    moq_subscription_t sub = MOQ_SUBSCRIPTION_INVALID;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscribe(s, &sc, 1, &sub),
                          (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(sub._opaque,
                          moq_handle_pack(MOQ_HANDLE_POOL_SUBSCRIPTION,
                                          s->session_tag, want_gen,
                                          (uint32_t)want_slot));

    moq_stream_ref_t ref = moq_stream_ref_from_u64(0);
    {
        int opens = 0, other = 0;
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) {
            if (a.kind == MOQ_ACTION_OPEN_BIDI_STREAM) {
                opens++;
                ref = a.u.open_bidi_stream.stream_ref;
                MOQ_TEST_CHECK_EQ_INT(a.u.open_bidi_stream.fin ? 1 : 0, 0);
                moq_buf_reader_t rr;
                moq_buf_reader_init(&rr, a.u.open_bidi_stream.data,
                                    a.u.open_bidi_stream.len);
                moq_control_envelope_t env;
                memset(&env, 0, sizeof(env));
                MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_envelope(&rr, &env),
                                      (int)MOQ_OK);
                MOQ_TEST_CHECK_EQ_U64(env.msg_type,
                                      (uint64_t)MOQ_D18_SUBSCRIBE);
                MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&rr),
                                       (size_t)0);
                /* The body against the fixture's DECLARED request. */
                moq_bytes_t dp[8];
                moq_d18_subscribe_t sb;
                memset(dp, 0, sizeof(dp));
                memset(&sb, 0, sizeof(sb));
                int dec_ok = (moq_d18_decode_subscribe(env.payload,
                                  env.payload_len, dp, 8, &sb) == MOQ_OK);
                MOQ_TEST_CHECK(dec_ok);
                if (dec_ok) {
                    MOQ_TEST_CHECK_EQ_U64(sb.request_id, 0);
                    MOQ_TEST_CHECK_EQ_SIZE(sb.track_namespace.count, (size_t)2);
                    if (sb.track_namespace.count == 2)
                        MOQ_TEST_CHECK(sb.track_namespace.parts != NULL);
                    if (sb.track_namespace.count == 2 &&
                        sb.track_namespace.parts) {
                        const moq_bytes_t *q0 = &sb.track_namespace.parts[0];
                        const moq_bytes_t *q1 = &sb.track_namespace.parts[1];
                        MOQ_TEST_CHECK_EQ_SIZE(q0->len, sizeof(k_ann_ns0));
                        MOQ_TEST_CHECK(q0->len == 0 || q0->data != NULL);
                        if (q0->len == sizeof(k_ann_ns0) && q0->data)
                            MOQ_TEST_CHECK(memcmp(q0->data, k_ann_ns0,
                                                  sizeof(k_ann_ns0)) == 0);
                        MOQ_TEST_CHECK_EQ_SIZE(q1->len, sizeof(k_ann_ns1));
                        MOQ_TEST_CHECK(q1->len == 0 || q1->data != NULL);
                        if (q1->len == sizeof(k_ann_ns1) && q1->data)
                            MOQ_TEST_CHECK(memcmp(q1->data, k_ann_ns1,
                                                  sizeof(k_ann_ns1)) == 0);
                    }
                    MOQ_TEST_CHECK_EQ_SIZE(sb.track_name.len,
                                           sizeof(k_r6_name));
                    MOQ_TEST_CHECK(sb.track_name.len == 0 ||
                                   sb.track_name.data != NULL);
                    if (sb.track_name.len == sizeof(k_r6_name) &&
                        sb.track_name.data)
                        MOQ_TEST_CHECK(memcmp(sb.track_name.data, k_r6_name,
                                              sizeof(k_r6_name)) == 0);
                    /* The default parameter set the fixture configured. */
                    MOQ_TEST_CHECK_EQ_SIZE(sb.params.auth_token_count,
                                           (size_t)0);
                    MOQ_TEST_CHECK_EQ_INT(
                        sb.params.has_subgroup_delivery_timeout ? 1 : 0, 0);
                    MOQ_TEST_CHECK_EQ_INT(
                        sb.params.has_object_delivery_timeout ? 1 : 0, 0);
                }
            } else other++;
            moq_action_cleanup(&a);
        }
        MOQ_TEST_CHECK_EQ_INT(opens, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
    }
    MOQ_TEST_CHECK(ref._v != 0);
    if (ref._v == 0) return ref;

    /* The peer accepts with a declared non-default alias, optionally FINning. */
    uint8_t ok[128];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, ok, sizeof(ok));
    {
        moq_d18_msg_params_t p = { 0 };
        p.has_largest = true;
        p.largest_group = R6_LARGEST_GROUP;
        p.largest_object = R6_LARGEST_OBJ;
        p.has_expires = true;
        p.expires_ms = R6_EXPIRES_MS;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_encode_subscribe_ok(&w, R6_TRACK_ALIAS, &p,
                                             (moq_bytes_t){0}), (int)MOQ_OK);
    }
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, ok,
                                              moq_buf_writer_offset(&w),
                                              fin_observed, 1), (int)MOQ_OK);
    {
        int oks = 0, other = 0;
        moq_event_t e;
        while (moq_session_poll_events(s, &e, 1) > 0) {
            if (e.kind == MOQ_EVENT_SUBSCRIBE_OK) {
                oks++;
                const moq_subscribe_ok_event_t *ok_e = &e.u.subscribe_ok;
                MOQ_TEST_CHECK_EQ_U64(ok_e->sub._opaque, sub._opaque);
                MOQ_TEST_CHECK_EQ_U64(ok_e->track_alias, R6_TRACK_ALIAS);
                MOQ_TEST_CHECK_EQ_INT(ok_e->has_largest ? 1 : 0, 1);
                MOQ_TEST_CHECK_EQ_U64(ok_e->largest_group, R6_LARGEST_GROUP);
                MOQ_TEST_CHECK_EQ_U64(ok_e->largest_object, R6_LARGEST_OBJ);
                MOQ_TEST_CHECK_EQ_INT(ok_e->has_expires ? 1 : 0, 1);
                MOQ_TEST_CHECK_EQ_U64(ok_e->expires_ms, R6_EXPIRES_MS);
                MOQ_TEST_CHECK_EQ_SIZE(ok_e->track_properties.len, (size_t)0);
                MOQ_TEST_CHECK_EQ_INT(ok_e->dynamic_groups ? 1 : 0, 0);
            } else other++;
            moq_event_cleanup(&e);
        }
        MOQ_TEST_CHECK_EQ_INT(oks, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
    }

    {
        const moq_sub_entry_t *e = &s->subs[want_slot];
        MOQ_TEST_CHECK_EQ_INT(sub_pool_busy(s), 1);
        MOQ_TEST_CHECK_EQ_INT((int)e->state, (int)MOQ_SUB_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT((int)e->role, (int)MOQ_SUB_ROLE_SUBSCRIBER);
        MOQ_TEST_CHECK_EQ_U64(e->generation, want_gen);
        MOQ_TEST_CHECK_EQ_U64(e->handle._opaque, sub._opaque);
        MOQ_TEST_CHECK_EQ_U64(e->request_id, 0);
        MOQ_TEST_CHECK_EQ_U64(e->track_alias, R6_TRACK_ALIAS);
        MOQ_TEST_CHECK_EQ_U64(e->request_stream_ref._v, ref._v);
        MOQ_TEST_CHECK_EQ_INT(e->req_recv_fin ? 1 : 0, fin_observed ? 1 : 0);
        check_registered(s, ref, MOQ_REQ_SUBSCRIPTION, want_slot);
    }
    if (out_handle) *out_handle = sub._opaque;
    return ref;
}


/* The WHOLE established subscription: its object representation plus the bytes
 * it owns. A refused terminal owes zero owner mutation, so this is compared
 * verbatim rather than field by field -- nothing in the entry may move,
 * including the fields req_seed_snap_t does not model (track alias, resolved
 * window, filter/forward state, dynamic groups, timeouts, history identity). */
#define R6_OWN_MAX 256
typedef struct r6_full {
    int      valid;
    moq_sub_entry_t raw;
    size_t   req_len;
    uint8_t  req[R6_OWN_MAX];
    size_t   tid_len;
    uint8_t  tid[R6_OWN_MAX];
} r6_full_t;

static r6_full_t g_r6_full;

static void r6_full_read(const moq_session_t *s, r6_full_t *v)
{
    memset(v, 0, sizeof(*v));
    if (g_r6_want_slot < 0 || (size_t)g_r6_want_slot >= s->sub_cap) return;
    const moq_sub_entry_t *e = &s->subs[g_r6_want_slot];
    v->raw = *e;
    v->req_len = e->req_recv_len;
    if (e->req_recv_len > R6_OWN_MAX || (e->req_recv_len && !e->req_recv_buf))
        return;
    if (e->req_recv_len) memcpy(v->req, e->req_recv_buf, e->req_recv_len);
    v->tid_len = e->track_id_len;
    if (e->track_id_len > R6_OWN_MAX || (e->track_id_len && !e->track_id_buf))
        return;
    if (e->track_id_len) memcpy(v->tid, e->track_id_buf, e->track_id_len);
    v->valid = 1;
}

static void r6_full_capture(const moq_session_t *s)
{
    r6_full_read(s, &g_r6_full);
    MOQ_TEST_CHECK_EQ_INT(g_r6_full.valid, 1);
}

static int r6_full_check_blocked(const moq_session_t *s, const char *what)
{
    r6_full_t now;
    r6_full_read(s, &now);
    int bad = 0;
    if (!now.valid || !g_r6_full.valid) {
        TXS_DIAG("TXN %s: incomparable subscription record\n", what);
        return 1;
    }
    if (memcmp(&now.raw, &g_r6_full.raw, sizeof(now.raw)) != 0) {
        TXS_DIAG("TXN %s: established subscription entry changed\n", what);
        bad++;
    }
    if (now.req_len != g_r6_full.req_len ||
        (now.req_len && memcmp(now.req, g_r6_full.req, now.req_len) != 0)) {
        TXS_DIAG("TXN %s: retained request bytes changed\n", what);
        bad++;
    }
    if (now.tid_len != g_r6_full.tid_len ||
        (now.tid_len && memcmp(now.tid, g_r6_full.tid, now.tid_len) != 0)) {
        TXS_DIAG("TXN %s: retained track identity changed\n", what);
        bad++;
    }
    return bad;
}

/* A completed cancel retires the slot, so the only committed requirement is
 * that it is no longer live; retirement itself is asserted by the runner. */
static int r6_full_check_committed(const moq_session_t *s, const char *what)
{
    if (g_r6_want_slot < 0 || (size_t)g_r6_want_slot >= s->sub_cap) return 0;
    if (s->subs[g_r6_want_slot].state != MOQ_SUB_FREE) {
        TXS_DIAG("TXN %s: cancelled subscription slot is still live\n", what);
        return 1;
    }
    return 0;
}

static const extra_ops_t r6_full_ops = {
    r6_full_capture, r6_full_check_blocked, r6_full_check_committed
};

static moq_result_t fire_unsubscribe(moq_session_t *s, uint64_t handle)
{
    moq_subscription_t sub;
    sub._opaque = handle;
    return moq_session_unsubscribe(s, sub, 1);
}

static void want_bidi_cancel(txs_norm_vec_t *v, moq_stream_ref_t ref,
                             uint64_t handle)
{
    (void)handle;
    want_nsl_output(v, ref);   /* one ABORT_BIDI_STREAM(ref, CANCELLED) */
}


/* -- Route: fetch cancel (session_fetch.c:1485, via moq_session_fetch_cancel)
 *
 * The latch IS reachable, and the arm below drives it: a FETCH_OK is latched
 * FIN-first (session_subscribe.c:750), and accepting it sets
 * `control_response_seen`/`control_ok` WITHOUT leaving MOQ_FETCH_PENDING_FETCHER
 * (session_fetch.c:975) -- which is the only state the public cancel guards on
 * (:1534). So a valid FETCH_OK carrying FIN leaves a cancellable owner whose
 * durable latch the terminal consults: `fetch_request_bidi_cancel` reserves a
 * drain only when the peer half is still open, so a same-call FIN owes none. */
#define R5_END_OF_TRACK 1
/* Non-default, and INSIDE the requested range: draft-18 §10.13 permits the
 * response end to equal the requested end or shrink when published data ends
 * first, never to exceed it. */
#define R5_OK_END_GROUP 0x52
#define R5_OK_END_OBJ   0x08
#define R5_START_GROUP 0x31
#define R5_START_OBJ   0x02
#define R5_END_GROUP   0x60
#define R5_END_OBJ     0x0c
#define R5_PRIORITY    23
static const uint8_t k_r5_name[] = { 'f','0' };

static int g_r5_want_slot = -1;

static int fetch_owner_edges(const og_graph_t *g, moq_stream_ref_t ref,
                             const char *what)
{
    int bad = 0;
    if (g_r5_want_slot < 0) {
        OG_DIAG("OG %s: no declared fetch slot\n", what);
        return 1;
    }
    /* A locally-issued fetch registers BOTH keys by design: the response
     * correlates by the request bidi, while the response data uni carries the
     * Request ID (session_fetch.c:1360). */
    bad += og_check_edge(g, OG_DOM_REQ_STREAMREF, ref._v,
                         MOQ_REQ_FETCH, g_r5_want_slot, what);
    bad += og_check_edge(g, OG_DOM_REQ_RID, 0,
                         MOQ_REQ_FETCH, g_r5_want_slot, what);
    {
        const og_edge_spec_t want[] = { { OG_DOM_REQ_STREAMREF, ref._v },
                                        { OG_DOM_REQ_RID, 0 } };
        bad += og_check_owner_edges(g, MOQ_REQ_FETCH, g_r5_want_slot,
                                    want, 2, what);
    }
    bad += og_check_no_edge(g, OG_DOM_NS_REF, ref._v, what);
    return bad;
}

static void fetch_term_capture(const moq_session_t *s, moq_stream_ref_t ref,
                               seed_snap_t *o)
{
    req_seed_snap_t *r = &o->req;
    memset(r, 0, sizeof(*r));
    r->busy = fetch_busy_count(s);
    const moq_fetch_entry_t *e = fetch_by_ref(s, ref);
    r->found = e != NULL;
    if (e) {
        r->state      = (int)e->state;
        r->role       = (int)e->role;
        r->generation = e->generation;
        r->handle     = e->handle._opaque;
        r->request_id = e->request_id;
        r->stream_ref = e->request_stream_ref._v;
        r->fin        = e->req_recv_fin ? 1 : 0;
        r->recv_len   = e->req_recv_len;
        req_seed_copy_buf(r, e->req_recv_buf, e->req_recv_len);
    }
    req_seed_capture_registry(r, s, ref, r->request_id);
}

static void fetch_term_check(const moq_session_t *s, moq_stream_ref_t ref,
                             const seed_snap_t *want, const char *what)
{
    seed_snap_t now;
    fetch_term_capture(s, ref, &now);
    req_seed_diff(&now.req, &want->req, what);
}

static void check_fetch_terminal_retired(const moq_session_t *s,
                                         moq_stream_ref_t ref)
{
    MOQ_TEST_CHECK_EQ_INT(fetch_busy_count(s), 0);
    MOQ_TEST_CHECK(fetch_by_ref(s, ref) == NULL);
    check_unregistered(s, ref);
}

static const owner_ops_t fetch_owner_ops = {
    fetch_term_capture, fetch_term_check, check_fetch_terminal_retired
};

/* The whole pending fetch, compared verbatim: a refusal owes zero mutation,
 * including the fields req_seed_snap_t does not model (the requested range,
 * priority and group order, the data-stream identity and its started/FIN
 * flags, and the response buffer's capacity). */
typedef struct r5_full {
    int      valid;
    moq_fetch_entry_t raw;
    size_t   req_len;
    uint8_t  req[256];
} r5_full_t;

static r5_full_t g_r5_full;

static void r5_full_read(const moq_session_t *s, r5_full_t *v)
{
    memset(v, 0, sizeof(*v));
    if (g_r5_want_slot < 0 || (size_t)g_r5_want_slot >= s->fetch_cap) return;
    const moq_fetch_entry_t *e = &s->fetches[g_r5_want_slot];
    v->raw = *e;
    v->req_len = e->req_recv_len;
    if (e->req_recv_len > sizeof(v->req) ||
        (e->req_recv_len && !e->req_recv_buf)) return;
    if (e->req_recv_len) memcpy(v->req, e->req_recv_buf, e->req_recv_len);
    v->valid = 1;
}

static void r5_full_capture(const moq_session_t *s)
{
    r5_full_read(s, &g_r5_full);
    MOQ_TEST_CHECK_EQ_INT(g_r5_full.valid, 1);
}

static int r5_full_check_blocked(const moq_session_t *s, const char *what)
{
    r5_full_t now;
    r5_full_read(s, &now);
    int bad = 0;
    if (!now.valid || !g_r5_full.valid) {
        TXS_DIAG("TXN %s: incomparable fetch record\n", what);
        return 1;
    }
    if (memcmp(&now.raw, &g_r5_full.raw, sizeof(now.raw)) != 0) {
        TXS_DIAG("TXN %s: pending fetch entry changed\n", what);
        bad++;
    }
    if (now.req_len != g_r5_full.req_len ||
        (now.req_len && memcmp(now.req, g_r5_full.req, now.req_len) != 0)) {
        TXS_DIAG("TXN %s: retained response bytes changed\n", what);
        bad++;
    }
    return bad;
}

static int r5_full_check_committed(const moq_session_t *s, const char *what)
{
    if (g_r5_want_slot < 0 || (size_t)g_r5_want_slot >= s->fetch_cap) return 0;
    if (s->fetches[g_r5_want_slot].state != MOQ_FETCH_FREE) {
        TXS_DIAG("TXN %s: cancelled fetch slot is still live\n", what);
        return 1;
    }
    return 0;
}

static const extra_ops_t r5_full_ops = {
    r5_full_capture, r5_full_check_blocked, r5_full_check_committed
};

static moq_stream_ref_t arm_fetch_pending(moq_session_t *s, bool fin_observed,
                                          uint64_t *out_handle)
{
    int want_slot = -1;
    uint32_t want_gen = 0;
    for (size_t i = 0; i < s->fetch_cap; i++)
        if (s->fetches[i].state == MOQ_FETCH_FREE) {
            want_slot = (int)i;
            want_gen = s->fetches[i].generation | 1u;
            break;
        }
    MOQ_TEST_CHECK(want_slot >= 0);
    if (want_slot < 0) return moq_stream_ref_from_u64(0);
    g_r5_want_slot = want_slot;

    moq_bytes_t parts[] = { { k_ann_ns0, sizeof(k_ann_ns0) },
                            { k_ann_ns1, sizeof(k_ann_ns1) } };
    moq_fetch_cfg_t fc;
    moq_fetch_cfg_init(&fc);
    fc.track_namespace = (moq_namespace_t){ parts, 2 };
    fc.track_name = MOQ_BYTES_LITERAL("f0");
    fc.start_group = R5_START_GROUP;
    fc.start_object = R5_START_OBJ;
    fc.end_group = R5_END_GROUP;
    fc.end_object = R5_END_OBJ;
    fc.group_order = MOQ_GROUP_ORDER_ASCENDING;
    fc.has_subscriber_priority = true;
    fc.subscriber_priority = R5_PRIORITY;
    moq_fetch_t fh = MOQ_FETCH_INVALID;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(s, &fc, 1, &fh), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(fh._opaque,
                          moq_handle_pack(MOQ_HANDLE_POOL_FETCH,
                                          s->session_tag, want_gen,
                                          (uint32_t)want_slot));

    moq_stream_ref_t ref = moq_stream_ref_from_u64(0);
    {
        int opens = 0, other = 0;
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) {
            if (a.kind == MOQ_ACTION_OPEN_BIDI_STREAM) {
                opens++;
                ref = a.u.open_bidi_stream.stream_ref;
                MOQ_TEST_CHECK_EQ_INT(a.u.open_bidi_stream.fin ? 1 : 0, 0);
                moq_buf_reader_t rr;
                moq_buf_reader_init(&rr, a.u.open_bidi_stream.data,
                                    a.u.open_bidi_stream.len);
                moq_control_envelope_t env;
                memset(&env, 0, sizeof(env));
                MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_envelope(&rr, &env),
                                      (int)MOQ_OK);
                MOQ_TEST_CHECK_EQ_U64(env.msg_type, (uint64_t)MOQ_D18_FETCH);
                MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&rr),
                                       (size_t)0);
                moq_bytes_t dp[8];
                moq_d18_fetch_t fb;
                memset(dp, 0, sizeof(dp));
                memset(&fb, 0, sizeof(fb));
                int dec_ok = (moq_d18_decode_fetch(env.payload, env.payload_len,
                                                   dp, 8, &fb) == MOQ_OK);
                MOQ_TEST_CHECK(dec_ok);
                if (dec_ok) {
                    MOQ_TEST_CHECK_EQ_U64(fb.request_id, 0);
                    MOQ_TEST_CHECK_EQ_U64(fb.fetch_type, 1);
                    MOQ_TEST_CHECK_EQ_SIZE(fb.track_namespace.count, (size_t)2);
                    if (fb.track_namespace.count == 2)
                        MOQ_TEST_CHECK(fb.track_namespace.parts != NULL);
                    if (fb.track_namespace.count == 2 &&
                        fb.track_namespace.parts) {
                        const moq_bytes_t *q0 = &fb.track_namespace.parts[0];
                        const moq_bytes_t *q1 = &fb.track_namespace.parts[1];
                        MOQ_TEST_CHECK_EQ_SIZE(q0->len, sizeof(k_ann_ns0));
                        MOQ_TEST_CHECK(q0->len == 0 || q0->data != NULL);
                        if (q0->len == sizeof(k_ann_ns0) && q0->data)
                            MOQ_TEST_CHECK(memcmp(q0->data, k_ann_ns0,
                                                  sizeof(k_ann_ns0)) == 0);
                        MOQ_TEST_CHECK_EQ_SIZE(q1->len, sizeof(k_ann_ns1));
                        MOQ_TEST_CHECK(q1->len == 0 || q1->data != NULL);
                        if (q1->len == sizeof(k_ann_ns1) && q1->data)
                            MOQ_TEST_CHECK(memcmp(q1->data, k_ann_ns1,
                                                  sizeof(k_ann_ns1)) == 0);
                    }
                    MOQ_TEST_CHECK_EQ_SIZE(fb.track_name.len,
                                           sizeof(k_r5_name));
                    MOQ_TEST_CHECK(fb.track_name.len == 0 ||
                                   fb.track_name.data != NULL);
                    if (fb.track_name.len == sizeof(k_r5_name) &&
                        fb.track_name.data)
                        MOQ_TEST_CHECK(memcmp(fb.track_name.data, k_r5_name,
                                              sizeof(k_r5_name)) == 0);
                    MOQ_TEST_CHECK_EQ_U64(fb.start.group, R5_START_GROUP);
                    MOQ_TEST_CHECK_EQ_U64(fb.start.object, R5_START_OBJ);
                    MOQ_TEST_CHECK_EQ_U64(fb.end.group, R5_END_GROUP);
                    MOQ_TEST_CHECK_EQ_U64(fb.end.object, R5_END_OBJ);
                    MOQ_TEST_CHECK_EQ_INT(fb.params.has_group_order ? 1 : 0, 1);
                    MOQ_TEST_CHECK_EQ_INT((int)fb.params.group_order, 1);
                    MOQ_TEST_CHECK_EQ_INT(fb.params.has_subscriber_priority
                                              ? 1 : 0, 1);
                    MOQ_TEST_CHECK_EQ_U64(fb.params.subscriber_priority,
                                          R5_PRIORITY);
                    MOQ_TEST_CHECK_EQ_SIZE(fb.params.auth_token_count,
                                           (size_t)0);
                }
            } else other++;
            moq_action_cleanup(&a);
        }
        MOQ_TEST_CHECK_EQ_INT(opens, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
    }
    MOQ_TEST_CHECK(ref._v != 0);
    if (ref._v == 0) return ref;

    /* A COMPLETE, valid, non-default FETCH_OK -- the two rows differ only in
     * whether this same call carries the FIN. */
    {
        uint8_t ok[128];
        moq_buf_writer_t ow;
        moq_buf_writer_init(&ow, ok, sizeof(ok));
        /* start <= response end <= request end, pinned permanently so a later
         * edit cannot drift the fixture off the protocol. */
        MOQ_TEST_CHECK(R5_START_GROUP < R5_OK_END_GROUP ||
                       (R5_START_GROUP == R5_OK_END_GROUP &&
                        R5_START_OBJ <= R5_OK_END_OBJ));
        MOQ_TEST_CHECK(R5_OK_END_GROUP < R5_END_GROUP ||
                       (R5_OK_END_GROUP == R5_END_GROUP &&
                        R5_OK_END_OBJ <= R5_END_OBJ));
        moq_d18_location_t end = { R5_OK_END_GROUP, R5_OK_END_OBJ };
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_encode_fetch_ok(&ow, R5_END_OF_TRACK != 0, end,
                                         (moq_bytes_t){0}), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, ok,
                                                  moq_buf_writer_offset(&ow),
                                                  fin_observed, 1), (int)MOQ_OK);
    }
    {
        int oks = 0, other = 0;
        moq_event_t e;
        while (moq_session_poll_events(s, &e, 1) > 0) {
            if (e.kind == MOQ_EVENT_FETCH_OK) {
                oks++;
                const moq_fetch_ok_event_t *k = &e.u.fetch_ok;
                MOQ_TEST_CHECK_EQ_U64(k->fetch._opaque, fh._opaque);
                MOQ_TEST_CHECK_EQ_INT(k->end_of_track ? 1 : 0, R5_END_OF_TRACK);
                MOQ_TEST_CHECK_EQ_U64(k->end_group, R5_OK_END_GROUP);
                MOQ_TEST_CHECK_EQ_U64(k->end_object, R5_OK_END_OBJ);
                MOQ_TEST_CHECK_EQ_SIZE(k->track_properties.len, (size_t)0);
            } else other++;
            moq_event_cleanup(&e);
        }
        MOQ_TEST_CHECK_EQ_INT(oks, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
    }

    /* The acceptance leaves a still-CANCELLABLE owner: the state the public
     * cancel guards on is unchanged, and the latch now carries the row's FIN. */
    {
        const moq_fetch_entry_t *e = &s->fetches[want_slot];
        MOQ_TEST_CHECK_EQ_INT(fetch_busy_count(s), 1);
        MOQ_TEST_CHECK_EQ_INT((int)e->state, (int)MOQ_FETCH_PENDING_FETCHER);
        MOQ_TEST_CHECK_EQ_INT((int)e->role, (int)MOQ_FETCH_ROLE_FETCHER);
        MOQ_TEST_CHECK_EQ_INT(e->control_response_seen ? 1 : 0, 1);
        MOQ_TEST_CHECK_EQ_INT(e->control_ok ? 1 : 0, 1);
        MOQ_TEST_CHECK_EQ_U64(e->generation, want_gen);
        MOQ_TEST_CHECK_EQ_U64(e->handle._opaque, fh._opaque);
        MOQ_TEST_CHECK_EQ_U64(e->request_id, 0);
        MOQ_TEST_CHECK_EQ_U64(e->request_stream_ref._v, ref._v);
        MOQ_TEST_CHECK_EQ_INT(e->req_recv_fin ? 1 : 0, fin_observed ? 1 : 0);
        /* No data stream was ever opened, so the cancel owes no STOP_DATA. */
        MOQ_TEST_CHECK_EQ_INT(e->data_stream_started ? 1 : 0, 0);
        MOQ_TEST_CHECK_EQ_INT(e->data_stream_fin ? 1 : 0, 0);
        MOQ_TEST_CHECK_EQ_U64(e->data_stream_ref._v, 0);
        MOQ_TEST_CHECK_EQ_SIZE(e->req_recv_len, (size_t)0);
        check_registered(s, ref, MOQ_REQ_FETCH, want_slot);
    }
    if (out_handle) *out_handle = fh._opaque;
    return ref;
}

static moq_result_t fire_fetch_cancel(moq_session_t *s, uint64_t handle)
{
    moq_fetch_t f;
    f._opaque = handle;
    return moq_session_fetch_cancel(s, f, 1);
}

/* No data uni was ever opened, so the cancel owes exactly the request-bidi
 * abort -- no STOP_DATA. */
static void want_fetch_cancel(txs_norm_vec_t *v, moq_stream_ref_t ref,
                              uint64_t handle)
{
    (void)handle;
    want_nsl_output(v, ref);
}



/* -- Route: done SUBSCRIBE (session_subscribe.c:3250, moq_session_done_subscribe)
 *
 * The terminal reserves its drain FIN-aware: it consults `e->req_recv_fin`, so
 * it takes a reference only when the peer half is still open and a same-call FIN
 * owes none. */
#define R7_STATUS   0x2
#define R7_STREAMS  0x9
#define R7_TRACK_ALIAS 0x74
static const char k_r7_reason[] = "publisher done";
static const uint8_t k_r7_ns[] = { 'l','i','v','e' };
static const uint8_t k_r7_name[] = { 'v','0' };

static int g_r7_want_slot = -1;

static int r7_owner_edges(const og_graph_t *g, moq_stream_ref_t ref,
                          const char *what)
{
    int bad = 0;
    if (g_r7_want_slot < 0) {
        OG_DIAG("OG %s: no declared subscription slot\n", what);
        return 1;
    }
    bad += og_check_edge(g, OG_DOM_REQ_STREAMREF, ref._v,
                         MOQ_REQ_SUBSCRIPTION, g_r7_want_slot, what);
    {
        const og_edge_spec_t want[] = { { OG_DOM_REQ_STREAMREF, ref._v } };
        bad += og_check_owner_edges(g, MOQ_REQ_SUBSCRIPTION, g_r7_want_slot,
                                    want, 1, what);
    }
    bad += og_check_no_edge(g, OG_DOM_NS_REF, ref._v, what);
    return bad;
}

/* The whole accepted publisher-role subscription, compared verbatim. */
typedef struct r7_full {
    int      valid;
    moq_sub_entry_t raw;
    size_t   req_len;
    uint8_t  req[R6_OWN_MAX];
    size_t   tid_len;
    uint8_t  tid[R6_OWN_MAX];
} r7_full_t;

static r7_full_t g_r7_full;

static void r7_full_read(const moq_session_t *s, r7_full_t *v)
{
    memset(v, 0, sizeof(*v));
    if (g_r7_want_slot < 0 || (size_t)g_r7_want_slot >= s->sub_cap) return;
    const moq_sub_entry_t *e = &s->subs[g_r7_want_slot];
    v->raw = *e;
    v->req_len = e->req_recv_len;
    if (e->req_recv_len > R6_OWN_MAX || (e->req_recv_len && !e->req_recv_buf))
        return;
    if (e->req_recv_len) memcpy(v->req, e->req_recv_buf, e->req_recv_len);
    v->tid_len = e->track_id_len;
    if (e->track_id_len > R6_OWN_MAX || (e->track_id_len && !e->track_id_buf))
        return;
    if (e->track_id_len) memcpy(v->tid, e->track_id_buf, e->track_id_len);
    v->valid = 1;
}

static void r7_full_capture(const moq_session_t *s)
{
    r7_full_read(s, &g_r7_full);
    MOQ_TEST_CHECK_EQ_INT(g_r7_full.valid, 1);
}

static int r7_full_check_blocked(const moq_session_t *s, const char *what)
{
    r7_full_t now;
    r7_full_read(s, &now);
    int bad = 0;
    if (!now.valid || !g_r7_full.valid) {
        TXS_DIAG("TXN %s: incomparable subscription record\n", what);
        return 1;
    }
    if (memcmp(&now.raw, &g_r7_full.raw, sizeof(now.raw)) != 0) {
        TXS_DIAG("TXN %s: accepted subscription entry changed\n", what);
        bad++;
    }
    if (now.req_len != g_r7_full.req_len ||
        (now.req_len && memcmp(now.req, g_r7_full.req, now.req_len) != 0)) {
        TXS_DIAG("TXN %s: retained request bytes changed\n", what);
        bad++;
    }
    if (now.tid_len != g_r7_full.tid_len ||
        (now.tid_len && memcmp(now.tid, g_r7_full.tid, now.tid_len) != 0)) {
        TXS_DIAG("TXN %s: retained track identity changed\n", what);
        bad++;
    }
    return bad;
}

static int r7_full_check_committed(const moq_session_t *s, const char *what)
{
    if (g_r7_want_slot < 0 || (size_t)g_r7_want_slot >= s->sub_cap) return 0;
    if (s->subs[g_r7_want_slot].state != MOQ_SUB_FREE) {
        TXS_DIAG("TXN %s: finished subscription slot is still live\n", what);
        return 1;
    }
    return 0;
}

static const extra_ops_t r7_full_ops = {
    r7_full_capture, r7_full_check_blocked, r7_full_check_committed
};

static moq_stream_ref_t arm_accepted_subscription(moq_session_t *s,
                                                  bool fin_observed,
                                                  uint64_t *out_handle)
{
    /* Slot, generation and the handle they pack into, all derived from pool
     * state BEFORE the request is fed. */
    int want_slot = -1;
    uint32_t want_gen = 0;
    for (size_t i = 0; i < s->sub_cap; i++)
        if (s->subs[i].state == MOQ_SUB_FREE) {
            want_slot = (int)i;
            want_gen = s->subs[i].generation | 1u;
            break;
        }
    MOQ_TEST_CHECK(want_slot >= 0);
    if (want_slot < 0) return moq_stream_ref_from_u64(0);
    g_r7_want_slot = want_slot;
    uint64_t want_handle = moq_handle_pack(MOQ_HANDLE_POOL_SUBSCRIPTION,
                                           s->session_tag, want_gen,
                                           (uint32_t)want_slot);

    moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
    uint8_t msg[192];
    size_t n = enc_subscribe_pub_role(msg, sizeof(msg), 0);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, msg, n, fin_observed, 1),
        (int)MOQ_OK);

    moq_subscription_t sub = MOQ_SUBSCRIPTION_INVALID;
    {
        int req = 0, other = 0;
        moq_event_t e;
        while (moq_session_poll_events(s, &e, 1) > 0) {
            if (e.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                req++;
                sub = e.u.subscribe_request.sub;
                /* The complete request, against the fixture's own constants
                 * and the handle derived before ingress. */
                const moq_subscribe_request_event_t *rq = &e.u.subscribe_request;
                MOQ_TEST_CHECK_EQ_U64(rq->sub._opaque, want_handle);
                MOQ_TEST_CHECK_EQ_SIZE(rq->track_namespace.count, (size_t)1);
                if (rq->track_namespace.count == 1 &&
                    rq->track_namespace.parts) {
                    const moq_bytes_t *q = &rq->track_namespace.parts[0];
                    failures += txs_check_span_bytes(q->data, q->len, k_r7_ns,
                                                     sizeof(k_r7_ns),
                                                     "route7 request namespace");
                } else MOQ_TEST_CHECK(0);
                failures += txs_check_span_bytes(rq->track_name.data,
                                                 rq->track_name.len, k_r7_name,
                                                 sizeof(k_r7_name),
                                                 "route7 request track name");
                /* The subscriber-declared subgroup timeout the encoder set. */
                MOQ_TEST_CHECK_EQ_U64(rq->delivery_timeout_us,
                                      5000ull * 1000ull);
                MOQ_TEST_CHECK_EQ_SIZE(rq->token_count, (size_t)0);
                MOQ_TEST_CHECK_EQ_INT(rq->has_new_group_request ? 1 : 0, 0);
            } else other++;
            moq_event_cleanup(&e);
        }
        MOQ_TEST_CHECK_EQ_INT(req, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
    }

    moq_accept_subscribe_cfg_t acfg;
    moq_accept_subscribe_cfg_init(&acfg);
    acfg.has_track_alias = true;
    acfg.track_alias = R7_TRACK_ALIAS;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_subscribe(s, sub, &acfg, 1),
                          (int)MOQ_OK);

    /* The acceptance owes exactly one SUBSCRIBE_OK carrying the declared alias,
     * with our send half left open and nothing after the envelope. */
    {
        int oks = 0, other = 0;
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) {
            if (a.kind == MOQ_ACTION_SEND_BIDI_STREAM &&
                a.u.send_bidi_stream.stream_ref._v == ref._v) {
                oks++;
                MOQ_TEST_CHECK_EQ_INT(a.u.send_bidi_stream.fin ? 1 : 0, 0);
                moq_buf_reader_t rr;
                moq_buf_reader_init(&rr, a.u.send_bidi_stream.data,
                                    a.u.send_bidi_stream.len);
                moq_control_envelope_t env;
                memset(&env, 0, sizeof(env));
                MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_envelope(&rr, &env),
                                      (int)MOQ_OK);
                MOQ_TEST_CHECK_EQ_U64(env.msg_type,
                                      (uint64_t)MOQ_D18_SUBSCRIBE_OK);
                MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&rr),
                                       (size_t)0);
                moq_d18_subscribe_ok_t ok;
                memset(&ok, 0, sizeof(ok));
                if (moq_d18_decode_subscribe_ok(env.payload, env.payload_len,
                                                &ok) == MOQ_OK)
                    MOQ_TEST_CHECK_EQ_U64(ok.track_alias, R7_TRACK_ALIAS);
                else
                    MOQ_TEST_CHECK(0);
            } else other++;
            moq_action_cleanup(&a);
        }
        MOQ_TEST_CHECK_EQ_INT(oks, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
    }

    {
        const moq_sub_entry_t *e = &s->subs[want_slot];
        MOQ_TEST_CHECK_EQ_INT(sub_pool_busy(s), 1);
        MOQ_TEST_CHECK_EQ_INT((int)e->state, (int)MOQ_SUB_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT((int)e->role, (int)MOQ_SUB_ROLE_PUBLISHER);
        MOQ_TEST_CHECK_EQ_U64(e->handle._opaque, want_handle);
        MOQ_TEST_CHECK_EQ_U64(e->generation, want_gen);
        MOQ_TEST_CHECK_EQ_U64(e->request_id, 0);
        MOQ_TEST_CHECK_EQ_U64(e->track_alias, R7_TRACK_ALIAS);
        MOQ_TEST_CHECK_EQ_U64(e->request_stream_ref._v, ref._v);
        MOQ_TEST_CHECK_EQ_INT(e->req_recv_fin ? 1 : 0, fin_observed ? 1 : 0);
        MOQ_TEST_CHECK_EQ_INT(e->goaway_sent ? 1 : 0, 0);
        check_registered(s, ref, MOQ_REQ_SUBSCRIPTION, want_slot);
    }
    if (out_handle) *out_handle = sub._opaque;
    return ref;
}

/* The terminal's whole output is one control message on the request bidi. */
static txs_op_hooks_t r7_hooks = {
    NULL, NULL, NULL, NULL, no_event_expected, pub_norm_action
};

static moq_result_t fire_done_subscribe(moq_session_t *s, uint64_t handle)
{
    moq_subscription_t sub;
    sub._opaque = handle;
    moq_done_subscribe_cfg_t c;
    memset(&c, 0, sizeof(c));
    c.struct_size = (uint32_t)sizeof(c);
    c.status_code = R7_STATUS;
    c.stream_count = R7_STREAMS;
    c.reason = (moq_bytes_t){ (const uint8_t *)k_r7_reason,
                              sizeof(k_r7_reason) - 1 };
    return moq_session_done_subscribe(s, sub, &c, 1);
}

/* The declared terminal: one PUBLISH_DONE on the request bidi, FINning our
 * send half, encoded independently of the product. */
static void want_done_subscribe(txs_norm_vec_t *v, moq_stream_ref_t ref,
                                uint64_t handle)
{
    (void)handle;
    uint8_t buf[128];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_d18_encode_publish_done(
            &w, R7_STATUS, R7_STREAMS,
            (moq_bytes_t){ (const uint8_t *)k_r7_reason,
                           sizeof(k_r7_reason) - 1 }), (int)MOQ_OK);
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


/* -- Route: finish PUBLISH (session_publish.c:1847, moq_session_finish_publish)
 *
 * The POSITIVE CONTROL of this tranche: unlike its five siblings the terminal
 * already spells the unified rule, `need_drain = req_stream && !req_recv_fin`
 * (:1885), so the latch rows must COMPLETE with no drain taken even against an
 * exhausted ring. A selector mutant below proves the rows are discriminating
 * rather than passing by construction. */
#define R4_TRACK_ALIAS 0x41
#define R4_STATUS      0x3
#define R4_STREAMS     0x6
#define R4_OK_PRIORITY 37
#define R4_OK_TIMEOUT  6000
#define R4_OK_EXPIRES  8000
static const char k_r4_reason[] = "publication done";
static const uint8_t k_r4_name[] = { 'v','0' };

static int g_r4_want_slot = -1;

static int r4_owner_edges(const og_graph_t *g, moq_stream_ref_t ref,
                          const char *what)
{
    int bad = 0;
    if (g_r4_want_slot < 0) {
        OG_DIAG("OG %s: no declared publication slot\n", what);
        return 1;
    }
    bad += og_check_edge(g, OG_DOM_REQ_STREAMREF, ref._v,
                         MOQ_REQ_PUBLISH, g_r4_want_slot, what);
    {
        const og_edge_spec_t want[] = { { OG_DOM_REQ_STREAMREF, ref._v } };
        bad += og_check_owner_edges(g, MOQ_REQ_PUBLISH, g_r4_want_slot,
                                    want, 1, what);
    }
    bad += og_check_no_edge(g, OG_DOM_NS_REF, ref._v, what);
    return bad;
}

static const moq_pub_entry_t *r4_entry_by_ref(const moq_session_t *s,
                                              moq_stream_ref_t ref)
{
    moq_request_endpoint_t ep = request_registry_find_by_streamref(s, ref);
    if (ep.kind != MOQ_REQ_PUBLISH) return NULL;
    if (ep.slot < 0 || (size_t)ep.slot >= s->pub_cap) return NULL;
    return &s->publishes[ep.slot];
}

static void r4_owner_capture(const moq_session_t *s, moq_stream_ref_t ref,
                             seed_snap_t *o)
{
    req_seed_snap_t *r = &o->req;
    memset(r, 0, sizeof(*r));
    r->busy = pub_busy_count(s);
    const moq_pub_entry_t *e = r4_entry_by_ref(s, ref);
    r->found = e != NULL;
    if (e) {
        r->state      = (int)e->state;
        r->role       = (int)e->role;
        r->generation = e->generation;
        r->handle     = e->handle._opaque;
        r->request_id = e->request_id;
        r->stream_ref = e->request_stream_ref._v;
        r->fin        = e->req_recv_fin ? 1 : 0;
        r->recv_len   = e->req_recv_len;
        req_seed_copy_buf(r, e->req_recv_buf, e->req_recv_len);
    }
    req_seed_capture_registry(r, s, ref, r->request_id);
}

static void r4_owner_check(const moq_session_t *s, moq_stream_ref_t ref,
                           const seed_snap_t *want, const char *what)
{
    seed_snap_t now;
    r4_owner_capture(s, ref, &now);
    req_seed_diff(&now.req, &want->req, what);
}

static void r4_check_retired(const moq_session_t *s, moq_stream_ref_t ref)
{
    MOQ_TEST_CHECK_EQ_INT(pub_busy_count(s), 0);
    MOQ_TEST_CHECK(r4_entry_by_ref(s, ref) == NULL);
    check_unregistered(s, ref);
}

static const owner_ops_t r4_owner_ops = {
    r4_owner_capture, r4_owner_check, r4_check_retired
};

/* The whole established publication, compared verbatim. */
typedef struct r4_full {
    int      valid;
    moq_pub_entry_t raw;
    size_t   req_len;
    uint8_t  req[R6_OWN_MAX];
} r4_full_t;

static r4_full_t g_r4_full;

static void r4_full_read(const moq_session_t *s, r4_full_t *v)
{
    memset(v, 0, sizeof(*v));
    if (g_r4_want_slot < 0 || (size_t)g_r4_want_slot >= s->pub_cap) return;
    const moq_pub_entry_t *e = &s->publishes[g_r4_want_slot];
    v->raw = *e;
    v->req_len = e->req_recv_len;
    if (e->req_recv_len > R6_OWN_MAX || (e->req_recv_len && !e->req_recv_buf))
        return;
    if (e->req_recv_len) memcpy(v->req, e->req_recv_buf, e->req_recv_len);
    v->valid = 1;
}

static void r4_full_capture(const moq_session_t *s)
{
    r4_full_read(s, &g_r4_full);
    MOQ_TEST_CHECK_EQ_INT(g_r4_full.valid, 1);
}

static int r4_full_check_blocked(const moq_session_t *s, const char *what)
{
    r4_full_t now;
    r4_full_read(s, &now);
    int bad = 0;
    if (!now.valid || !g_r4_full.valid) {
        TXS_DIAG("TXN %s: incomparable publication record\n", what);
        return 1;
    }
    if (memcmp(&now.raw, &g_r4_full.raw, sizeof(now.raw)) != 0) {
        TXS_DIAG("TXN %s: established publication entry changed\n", what);
        bad++;
    }
    if (now.req_len != g_r4_full.req_len ||
        (now.req_len && memcmp(now.req, g_r4_full.req, now.req_len) != 0)) {
        TXS_DIAG("TXN %s: retained response bytes changed\n", what);
        bad++;
    }
    return bad;
}

static int r4_full_check_committed(const moq_session_t *s, const char *what)
{
    if (g_r4_want_slot < 0 || (size_t)g_r4_want_slot >= s->pub_cap) return 0;
    if (s->publishes[g_r4_want_slot].state != MOQ_PUB_FREE) {
        TXS_DIAG("TXN %s: finished publication slot is still live\n", what);
        return 1;
    }
    return 0;
}

static const extra_ops_t r4_full_ops = {
    r4_full_capture, r4_full_check_blocked, r4_full_check_committed
};

static moq_stream_ref_t arm_publisher_established(moq_session_t *s,
                                                  bool fin_observed,
                                                  uint64_t *out_handle)
{
    int want_slot = -1;
    uint32_t want_gen = 0;
    for (size_t i = 0; i < s->pub_cap; i++)
        if (s->publishes[i].state == MOQ_PUB_FREE) {
            want_slot = (int)i;
            want_gen = s->publishes[i].generation | 1u;
            break;
        }
    MOQ_TEST_CHECK(want_slot >= 0);
    if (want_slot < 0) return moq_stream_ref_from_u64(0);
    g_r4_want_slot = want_slot;

    moq_bytes_t parts[] = { { k_ann_ns0, sizeof(k_ann_ns0) },
                            { k_ann_ns1, sizeof(k_ann_ns1) } };
    moq_publish_cfg_t pc;
    moq_publish_cfg_init(&pc);
    pc.track_namespace = (moq_namespace_t){ parts, 2 };
    pc.track_name = MOQ_BYTES_LITERAL("v0");
    pc.has_track_alias = true;
    pc.track_alias = R4_TRACK_ALIAS;
    moq_publication_t ph = MOQ_PUBLICATION_INVALID;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_publish(s, &pc, 1, &ph),
                          (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(ph._opaque,
                          moq_handle_pack(MOQ_HANDLE_POOL_PUBLICATION,
                                          s->session_tag, want_gen,
                                          (uint32_t)want_slot));

    moq_stream_ref_t ref = moq_stream_ref_from_u64(0);
    {
        int opens = 0, other = 0;
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) {
            if (a.kind == MOQ_ACTION_OPEN_BIDI_STREAM) {
                opens++;
                ref = a.u.open_bidi_stream.stream_ref;
                MOQ_TEST_CHECK_EQ_INT(a.u.open_bidi_stream.fin ? 1 : 0, 0);
                moq_buf_reader_t rr;
                moq_buf_reader_init(&rr, a.u.open_bidi_stream.data,
                                    a.u.open_bidi_stream.len);
                moq_control_envelope_t env;
                memset(&env, 0, sizeof(env));
                MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_envelope(&rr, &env),
                                      (int)MOQ_OK);
                MOQ_TEST_CHECK_EQ_U64(env.msg_type, (uint64_t)MOQ_D18_PUBLISH);
                MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&rr),
                                       (size_t)0);
                /* The body against the fixture's DECLARED request. */
                moq_bytes_t dp[8];
                moq_d18_publish_t pb;
                memset(dp, 0, sizeof(dp));
                memset(&pb, 0, sizeof(pb));
                int dec_ok = (moq_d18_decode_publish(env.payload,
                                  env.payload_len, dp, 8, &pb) == MOQ_OK);
                MOQ_TEST_CHECK(dec_ok);
                if (dec_ok) {
                    MOQ_TEST_CHECK_EQ_U64(pb.request_id, 0);
                    MOQ_TEST_CHECK_EQ_U64(pb.track_alias, R4_TRACK_ALIAS);
                    MOQ_TEST_CHECK_EQ_SIZE(pb.track_namespace.count, (size_t)2);
                    if (pb.track_namespace.count == 2)
                        MOQ_TEST_CHECK(pb.track_namespace.parts != NULL);
                    if (pb.track_namespace.count == 2 &&
                        pb.track_namespace.parts) {
                        const moq_bytes_t *q0 = &pb.track_namespace.parts[0];
                        const moq_bytes_t *q1 = &pb.track_namespace.parts[1];
                        MOQ_TEST_CHECK_EQ_SIZE(q0->len, sizeof(k_ann_ns0));
                        MOQ_TEST_CHECK(q0->len == 0 || q0->data != NULL);
                        if (q0->len == sizeof(k_ann_ns0) && q0->data)
                            MOQ_TEST_CHECK(memcmp(q0->data, k_ann_ns0,
                                                  sizeof(k_ann_ns0)) == 0);
                        MOQ_TEST_CHECK_EQ_SIZE(q1->len, sizeof(k_ann_ns1));
                        MOQ_TEST_CHECK(q1->len == 0 || q1->data != NULL);
                        if (q1->len == sizeof(k_ann_ns1) && q1->data)
                            MOQ_TEST_CHECK(memcmp(q1->data, k_ann_ns1,
                                                  sizeof(k_ann_ns1)) == 0);
                    }
                    MOQ_TEST_CHECK_EQ_SIZE(pb.track_name.len,
                                           sizeof(k_r4_name));
                    MOQ_TEST_CHECK(pb.track_name.len == 0 ||
                                   pb.track_name.data != NULL);
                    if (pb.track_name.len == sizeof(k_r4_name) &&
                        pb.track_name.data)
                        MOQ_TEST_CHECK(memcmp(pb.track_name.data, k_r4_name,
                                              sizeof(k_r4_name)) == 0);
                    MOQ_TEST_CHECK_EQ_SIZE(pb.track_properties.len, (size_t)0);
                    MOQ_TEST_CHECK_EQ_INT(pb.dynamic_groups ? 1 : 0, 0);
                    MOQ_TEST_CHECK_EQ_SIZE(pb.params.auth_token_count,
                                           (size_t)0);
                }
            } else other++;
            moq_action_cleanup(&a);
        }
        MOQ_TEST_CHECK_EQ_INT(opens, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
    }
    MOQ_TEST_CHECK(ref._v != 0);
    if (ref._v == 0) return ref;

    /* The peer accepts, optionally closing its half in the same chunk. */
    uint8_t ok[128];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, ok, sizeof(ok));
    {
        /* Non-default throughout, so the event and the committed owner are
         * discriminating rather than matching whatever the defaults are. */
        moq_d18_msg_params_t p = { 0 };
        p.has_subscriber_priority = true;
        p.subscriber_priority = R4_OK_PRIORITY;
        p.has_group_order = true;
        p.group_order = 2;                    /* descending */
        p.has_forward = true;
        p.forward = true;
        p.has_object_delivery_timeout = true;
        p.object_delivery_timeout_ms = R4_OK_TIMEOUT;
        p.has_expires = true;
        p.expires_ms = R4_OK_EXPIRES;
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_publish_ok(&w, &p),
                              (int)MOQ_OK);
    }
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, ok,
                                              moq_buf_writer_offset(&w),
                                              fin_observed, 1), (int)MOQ_OK);
    {
        int oks = 0, other = 0;
        moq_event_t e;
        while (moq_session_poll_events(s, &e, 1) > 0) {
            if (e.kind == MOQ_EVENT_PUBLISH_OK) {
                oks++;
                const moq_publish_ok_event_t *k = &e.u.publish_ok;
                MOQ_TEST_CHECK_EQ_U64(k->pub._opaque, ph._opaque);
                MOQ_TEST_CHECK_EQ_INT(k->send_allowed ? 1 : 0, 1);
                MOQ_TEST_CHECK_EQ_U64(k->subscriber_priority, R4_OK_PRIORITY);
                MOQ_TEST_CHECK_EQ_INT((int)k->group_order,
                                      (int)MOQ_GROUP_ORDER_DESCENDING);
                MOQ_TEST_CHECK_EQ_INT(k->has_delivery_timeout ? 1 : 0, 1);
                MOQ_TEST_CHECK_EQ_U64(k->delivery_timeout_ms, R4_OK_TIMEOUT);
                MOQ_TEST_CHECK_EQ_INT(k->has_expires ? 1 : 0, 1);
                MOQ_TEST_CHECK_EQ_U64(k->expires_ms, R4_OK_EXPIRES);
            } else other++;
            moq_event_cleanup(&e);
        }
        MOQ_TEST_CHECK_EQ_INT(oks, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
    }

    {
        const moq_pub_entry_t *e = &s->publishes[want_slot];
        MOQ_TEST_CHECK_EQ_INT(pub_busy_count(s), 1);
        MOQ_TEST_CHECK_EQ_INT((int)e->state, (int)MOQ_PUB_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT((int)e->role, (int)MOQ_PUB_ROLE_PUBLISHER);
        MOQ_TEST_CHECK_EQ_U64(e->generation, want_gen);
        MOQ_TEST_CHECK_EQ_U64(e->handle._opaque, ph._opaque);
        MOQ_TEST_CHECK_EQ_U64(e->request_id, 0);
        MOQ_TEST_CHECK_EQ_U64(e->track_alias, R4_TRACK_ALIAS);
        MOQ_TEST_CHECK_EQ_U64(e->request_stream_ref._v, ref._v);
        MOQ_TEST_CHECK_EQ_INT(e->req_recv_fin ? 1 : 0, fin_observed ? 1 : 0);
        MOQ_TEST_CHECK_EQ_INT(e->goaway_sent ? 1 : 0, 0);
        /* The acceptance's values committed onto the owner, independently of
         * the event that surfaced them. */
        MOQ_TEST_CHECK_EQ_INT(e->send_allowed ? 1 : 0, 1);
        MOQ_TEST_CHECK_EQ_U64(e->subscriber_priority, R4_OK_PRIORITY);
        MOQ_TEST_CHECK_EQ_INT((int)e->group_order,
                              (int)MOQ_GROUP_ORDER_DESCENDING);
        check_registered(s, ref, MOQ_REQ_PUBLISH, want_slot);
    }
    if (out_handle) *out_handle = ph._opaque;
    return ref;
}

static moq_result_t fire_finish_publish(moq_session_t *s, uint64_t handle)
{
    moq_publication_t pub;
    pub._opaque = handle;
    moq_finish_publish_cfg_t c;
    memset(&c, 0, sizeof(c));
    c.struct_size = (uint32_t)sizeof(c);
    c.status_code = R4_STATUS;
    c.stream_count = R4_STREAMS;
    c.reason = (moq_bytes_t){ (const uint8_t *)k_r4_reason,
                              sizeof(k_r4_reason) - 1 };
    return moq_session_finish_publish(s, pub, &c, 1);
}

static void want_finish_publish(txs_norm_vec_t *v, moq_stream_ref_t ref,
                                uint64_t handle)
{
    (void)handle;
    uint8_t buf[128];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_d18_encode_publish_done(
            &w, R4_STATUS, R4_STREAMS,
            (moq_bytes_t){ (const uint8_t *)k_r4_reason,
                           sizeof(k_r4_reason) - 1 }), (int)MOQ_OK);
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


/* -- Route: announcement error (session_namespace.c:582)
 *
 * The announcer's own PUBLISH_NAMESPACE is answered with a terminal
 * REQUEST_ERROR. The retirement reserves its drain FIN-aware
 * (`req_stream && !ann_peer_fin_observed`, :594), so a same-call FIN owes none
 * -- the ordinary Axis 3 shape on the announcer side. */
#define R3_ERROR_CODE  MOQ_REQUEST_ERROR_NOT_SUPPORTED
#define R3_RETRY_MS    4500
static const char k_r3_reason[] = "namespace refused";

static uint64_t g_r3_handle;
static int g_r3_want_slot = -1;

static size_t enc_announcement_error(uint8_t *buf, size_t cap);

static int r3_owner_edges(const og_graph_t *g, moq_stream_ref_t ref,
                          const char *what)
{
    int bad = 0;
    if (g_r3_want_slot < 0) {
        OG_DIAG("OG %s: no declared announcement slot\n", what);
        return 1;
    }
    bad += og_check_edge(g, OG_DOM_REQ_STREAMREF, ref._v,
                         MOQ_REQ_ANNOUNCEMENT, g_r3_want_slot, what);
    {
        const og_edge_spec_t want[] = { { OG_DOM_REQ_STREAMREF, ref._v } };
        bad += og_check_owner_edges(g, MOQ_REQ_ANNOUNCEMENT, g_r3_want_slot,
                                    want, 1, what);
    }
    bad += og_check_no_edge(g, OG_DOM_NS_REF, ref._v, what);
    return bad;
}

/* A locally-issued PUBLISH_NAMESPACE left PENDING: the terminal REQUEST_ERROR
 * is legal only before the announcement is answered (the d18 dispatcher closes
 * on a non-PENDING_ANNOUNCER target), so this arm deliberately stops short of
 * the acceptance route 2 feeds. */
static moq_stream_ref_t arm_announcement_pending(moq_session_t *s)
{
    int want_slot = -1;
    uint32_t want_gen = 0;
    for (size_t i = 0; i < s->ann_cap; i++)
        if (s->announcements[i].state == MOQ_ANN_FREE) {
            want_slot = (int)i;
            want_gen = s->announcements[i].generation | 1u;
            break;
        }
    MOQ_TEST_CHECK(want_slot >= 0);
    if (want_slot < 0) return moq_stream_ref_from_u64(0);
    g_r3_want_slot = want_slot;
    g_ann_full_slot = want_slot;
    /* Peer BYTES drive this terminal, so the refused call legitimately appends
     * onto this same owner; the owner seed snapshot pins that append exactly. */
    g_ann_full_allow_append = 1;

    moq_bytes_t parts[] = { { k_ann_ns0, sizeof(k_ann_ns0) },
                            { k_ann_ns1, sizeof(k_ann_ns1) } };
    moq_publish_namespace_cfg_t pc;
    moq_publish_namespace_cfg_init(&pc);
    pc.track_namespace = (moq_namespace_t){ parts, 2 };
    moq_announcement_t ann = MOQ_ANNOUNCEMENT_INVALID;
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_publish_namespace(s, &pc, 1, &ann), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(ann._opaque,
                          moq_handle_pack(MOQ_HANDLE_POOL_ANNOUNCEMENT,
                                          s->session_tag, want_gen,
                                          (uint32_t)want_slot));
    g_r3_handle = ann._opaque;

    moq_stream_ref_t ref = moq_stream_ref_from_u64(0);
    {
        int opens = 0, other = 0;
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) {
            if (a.kind == MOQ_ACTION_OPEN_BIDI_STREAM) {
                opens++;
                ref = a.u.open_bidi_stream.stream_ref;
                MOQ_TEST_CHECK_EQ_INT(a.u.open_bidi_stream.fin ? 1 : 0, 0);
                moq_buf_reader_t rr;
                moq_buf_reader_init(&rr, a.u.open_bidi_stream.data,
                                    a.u.open_bidi_stream.len);
                moq_control_envelope_t env;
                memset(&env, 0, sizeof(env));
                MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_envelope(&rr, &env),
                                      (int)MOQ_OK);
                MOQ_TEST_CHECK_EQ_U64(env.msg_type,
                                      (uint64_t)MOQ_D18_PUBLISH_NAMESPACE);
                MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&rr),
                                       (size_t)0);
                /* The body against the fixture's DECLARED request. */
                moq_bytes_t dp[8];
                moq_d18_publish_namespace_t pn;
                memset(dp, 0, sizeof(dp));
                memset(&pn, 0, sizeof(pn));
                int dec_ok = (moq_d18_decode_publish_namespace(
                                  env.payload, env.payload_len, dp, 8, &pn)
                              == MOQ_OK);
                MOQ_TEST_CHECK(dec_ok);
                if (dec_ok) {
                    MOQ_TEST_CHECK_EQ_U64(pn.request_id, 0);
                    MOQ_TEST_CHECK_EQ_SIZE(pn.track_namespace.count, (size_t)2);
                    if (pn.track_namespace.count == 2)
                        MOQ_TEST_CHECK(pn.track_namespace.parts != NULL);
                    if (pn.track_namespace.count == 2 &&
                        pn.track_namespace.parts) {
                        const moq_bytes_t *q0 = &pn.track_namespace.parts[0];
                        const moq_bytes_t *q1 = &pn.track_namespace.parts[1];
                        MOQ_TEST_CHECK_EQ_SIZE(q0->len, sizeof(k_ann_ns0));
                        MOQ_TEST_CHECK(q0->len == 0 || q0->data != NULL);
                        if (q0->len == sizeof(k_ann_ns0) && q0->data)
                            MOQ_TEST_CHECK(memcmp(q0->data, k_ann_ns0,
                                                  sizeof(k_ann_ns0)) == 0);
                        MOQ_TEST_CHECK_EQ_SIZE(q1->len, sizeof(k_ann_ns1));
                        MOQ_TEST_CHECK(q1->len == 0 || q1->data != NULL);
                        if (q1->len == sizeof(k_ann_ns1) && q1->data)
                            MOQ_TEST_CHECK(memcmp(q1->data, k_ann_ns1,
                                                  sizeof(k_ann_ns1)) == 0);
                    }
                    /* The default parameter set this arm configured. */
                    MOQ_TEST_CHECK_EQ_SIZE(pn.params.auth_token_count,
                                           (size_t)0);
                }
            } else other++;
            moq_action_cleanup(&a);
        }
        MOQ_TEST_CHECK_EQ_INT(opens, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
    }
    MOQ_TEST_CHECK(ref._v != 0);
    if (ref._v == 0) return ref;

    {
        const moq_ann_entry_t *e = &s->announcements[want_slot];
        MOQ_TEST_CHECK_EQ_INT(ann_busy_count(s), 1);
        MOQ_TEST_CHECK_EQ_INT((int)e->state, (int)MOQ_ANN_PENDING_ANNOUNCER);
        MOQ_TEST_CHECK_EQ_INT((int)e->role, (int)MOQ_ANN_ROLE_ANNOUNCER);
        MOQ_TEST_CHECK_EQ_U64(e->generation, want_gen);
        MOQ_TEST_CHECK_EQ_U64(e->handle._opaque, ann._opaque);
        MOQ_TEST_CHECK_EQ_U64(e->request_id, 0);
        MOQ_TEST_CHECK_EQ_U64(e->request_stream_ref._v, ref._v);
        MOQ_TEST_CHECK_EQ_INT(e->req_recv_fin ? 1 : 0, 0);
        check_registered(s, ref, MOQ_REQ_ANNOUNCEMENT, want_slot);
    }
    return ref;
}

/* With buf == NULL this answers only the encoded length, so the fixture can
 * DECLARE the append the refused call will make instead of measuring it. */
static size_t enc_announcement_error(uint8_t *buf, size_t cap)
{
    uint8_t scratch[128];
    if (!buf) { buf = scratch; cap = sizeof(scratch); }
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_d18_encode_request_error(
            &w, R3_ERROR_CODE, R3_RETRY_MS,
            (moq_bytes_t){ (const uint8_t *)k_r3_reason,
                           sizeof(k_r3_reason) - 1 }), (int)MOQ_OK);
    return moq_buf_writer_offset(&w);
}

/* The declared terminal: one NAMESPACE_REJECTED event carrying the announcement
 * the arm surfaced, then our send half closed. */
static void want_announcement_error(txs_norm_vec_t *v, moq_stream_ref_t ref)
{
    txs_img_t im;
    txs_img_init(&im);
    txs_img_u64(&im, g_r3_handle);
    txs_img_u64(&im, (uint64_t)R3_ERROR_CODE);
    txs_img_u64(&im, 1);            /* can_retry, from the nonzero interval */
    txs_img_u64(&im, R3_RETRY_MS);
    txs_img_bytes(&im, (const uint8_t *)k_r3_reason, sizeof(k_r3_reason) - 1);
    MOQ_TEST_CHECK(txs_norm_append_img(v, MOQ_EVENT_NAMESPACE_REJECTED, &im));

    txs_img_init(&im);
    txs_img_u64(&im, ref._v);
    MOQ_TEST_CHECK(txs_norm_append_img(v, MOQ_ACTION_CLOSE_BIDI_STREAM, &im));
}

static bool r3_norm_event(const moq_event_t *ev, void *ctx,
                          txs_norm_vec_t *out)
{
    (void)ctx;
    if (ev->kind != MOQ_EVENT_NAMESPACE_REJECTED) {
        TXS_DIAG("TXN announcement error: unnormalized event kind %u\n",
                 (unsigned)ev->kind);
        return false;
    }
    txs_img_t im;
    txs_img_init(&im);
    txs_img_u64(&im, ev->u.namespace_rejected.ann._opaque);
    txs_img_u64(&im, (uint64_t)ev->u.namespace_rejected.error_code);
    txs_img_u64(&im, ev->u.namespace_rejected.can_retry ? 1u : 0u);
    txs_img_u64(&im, ev->u.namespace_rejected.retry_after_ms);
    txs_img_bytes(&im, ev->u.namespace_rejected.reason.data,
                  ev->u.namespace_rejected.reason.len);
    return txs_norm_append_img(out, ev->kind, &im);
}

static bool r3_norm_action(const moq_action_t *a, void *ctx,
                           txs_norm_vec_t *out)
{
    (void)ctx;
    if (a->kind != MOQ_ACTION_CLOSE_BIDI_STREAM) {
        TXS_DIAG("TXN announcement error: unnormalized action kind %u\n",
                 (unsigned)a->kind);
        return false;
    }
    txs_img_t im;
    txs_img_init(&im);
    txs_img_u64(&im, a->u.close_bidi_stream.stream_ref._v);
    return txs_norm_append_img(out, a->kind, &im);
}

/* Designated: a positional form would shift the slot pointer into the family
 * if a member were dropped, turning a real deletion into a build error rather
 * than the MOQ_REQ_NONE the validator exists to catch. */
static const terminal_graph_ops_t r3_graph_ops = {
    .owner_edges = r3_owner_edges,
    .owner_kind  = MOQ_REQ_ANNOUNCEMENT,
    .owner_slot  = &g_r3_want_slot
};

static txs_op_hooks_t r3_hooks = {
    NULL, NULL, NULL, NULL, r3_norm_event, r3_norm_action
};


/* -- Route: deferred publish finalize (session_publish.c:144, pub_finalize_done)
 *
 * The second POSITIVE CONTROL: the finalize already spells the unified rule,
 * `need_drain = request_stream_ref && !req_recv_fin` (:150), so all four rows
 * complete. It is reached only through the public deferred sweep -- a
 * PUBLISH_DONE whose Stream Count the subscriber has not yet satisfied defers,
 * and `moq_session_tick` past the stamped deadline finalizes it -- never by
 * calling the internal finalize directly. */
#define R1_ALIAS        0x11
#define R1_STATUS       0x4
#define R1_STREAMS      3
#define R1_DEADLINE_US  (31ull * 1000ull * 1000ull)
static const char k_r1_reason[] = "publisher finished";

static int g_r1_want_slot = -1;

static int r1_owner_edges(const og_graph_t *g, moq_stream_ref_t ref,
                          const char *what)
{
    int bad = 0;
    if (g_r1_want_slot < 0) {
        OG_DIAG("OG %s: no declared publication slot\n", what);
        return 1;
    }
    bad += og_check_edge(g, OG_DOM_REQ_STREAMREF, ref._v,
                         MOQ_REQ_PUBLISH, g_r1_want_slot, what);
    {
        const og_edge_spec_t want[] = { { OG_DOM_REQ_STREAMREF, ref._v } };
        bad += og_check_owner_edges(g, MOQ_REQ_PUBLISH, g_r1_want_slot,
                                    want, 1, what);
    }
    bad += og_check_no_edge(g, OG_DOM_NS_REF, ref._v, what);
    return bad;
}

/* The whole deferred publication, compared verbatim -- including the deferral
 * state the generic snapshot does not model (done_pending, its status, stream
 * count, stamped deadline and expiry flag). */
typedef struct r1_full {
    int      valid;
    moq_pub_entry_t raw;
    size_t   reason_len;
    uint8_t  reason[R6_OWN_MAX];
} r1_full_t;

static r1_full_t g_r1_full;

static void r1_full_read(const moq_session_t *s, r1_full_t *v)
{
    memset(v, 0, sizeof(*v));
    if (g_r1_want_slot < 0 || (size_t)g_r1_want_slot >= s->pub_cap) return;
    const moq_pub_entry_t *e = &s->publishes[g_r1_want_slot];
    v->raw = *e;
    /* The entry only holds a POINTER to the deferred reason, so the bytes are
     * copied out: a mutation in place would otherwise compare equal. */
    v->reason_len = e->done_reason_len;
    if (e->done_reason_len > R6_OWN_MAX ||
        (e->done_reason_len && !e->done_reason_buf)) return;
    if (e->done_reason_len)
        memcpy(v->reason, e->done_reason_buf, e->done_reason_len);
    v->valid = 1;
}

static void r1_full_capture(const moq_session_t *s)
{
    r1_full_read(s, &g_r1_full);
    MOQ_TEST_CHECK_EQ_INT(g_r1_full.valid, 1);
    /* The due-mark must be PROVEN by the tick, so it has to start false. */
    MOQ_TEST_CHECK_EQ_INT(g_r1_full.raw.done_expired ? 1 : 0, 0);
}

static int r1_full_check_blocked(const moq_session_t *s, const char *what)
{
    r1_full_t now;
    r1_full_read(s, &now);
    if (!now.valid || !g_r1_full.valid) {
        TXS_DIAG("TXN %s: incomparable publication record\n", what);
        return 1;
    }
    /* A refused sweep owes zero mutation EXCEPT its own due-mark: the sweep
     * charges `done_expired` before attempting the finalize
     * (session_publish.c:265), so that one flip is declared and everything
     * else must be verbatim. */
    if (!now.raw.done_expired) {
        TXS_DIAG("TXN %s: refused sweep never marked the deferral due\n", what);
        return 1;
    }
    moq_pub_entry_t want = g_r1_full.raw;
    want.done_expired = true;
    if (memcmp(&now.raw, &want, sizeof(want)) != 0) {
        TXS_DIAG("TXN %s: deferred publication entry changed beyond its"
                 " due-mark\n", what);
        return 1;
    }
    if (now.reason_len != g_r1_full.reason_len ||
        (now.reason_len &&
         memcmp(now.reason, g_r1_full.reason, now.reason_len) != 0)) {
        TXS_DIAG("TXN %s: retained terminal reason changed\n", what);
        return 1;
    }
    if (!now.raw.done_pending) {
        TXS_DIAG("TXN %s: refused sweep dropped the deferral\n", what);
        return 1;
    }
    return 0;
}

static int r1_full_check_committed(const moq_session_t *s, const char *what)
{
    if (g_r1_want_slot < 0 || (size_t)g_r1_want_slot >= s->pub_cap) return 0;
    if (s->publishes[g_r1_want_slot].state != MOQ_PUB_FREE) {
        TXS_DIAG("TXN %s: finalized publication slot is still live\n", what);
        return 1;
    }
    return 0;
}

static const extra_ops_t r1_full_ops = {
    r1_full_capture, r1_full_check_blocked, r1_full_check_committed
};

static uint64_t g_r1_handle;

static moq_stream_ref_t arm_deferred_publish(moq_session_t *s, bool fin_observed,
                                             uint64_t *out_handle)
{
    /* Slot, generation and the handle they pack into, all derived from pool
     * state BEFORE the inbound PUBLISH is fed. */
    int want_slot = -1;
    uint32_t want_gen = 0;
    for (size_t i = 0; i < s->pub_cap; i++)
        if (s->publishes[i].state == MOQ_PUB_FREE) {
            want_slot = (int)i;
            want_gen = s->publishes[i].generation | 1u;
            break;
        }
    MOQ_TEST_CHECK(want_slot >= 0);
    if (want_slot < 0) return moq_stream_ref_from_u64(0);
    g_r1_want_slot = want_slot;
    uint64_t want_handle = moq_handle_pack(MOQ_HANDLE_POOL_PUBLICATION,
                                           s->session_tag, want_gen,
                                           (uint32_t)want_slot);

    moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
    MOQ_TEST_CHECK_EQ_INT((int)feed_publish(s, ref, 0, R1_ALIAS, false),
                          (int)MOQ_OK);

    moq_publication_t pub = MOQ_PUBLICATION_INVALID;
    {
        int req = 0, other = 0;
        moq_event_t e;
        while (moq_session_poll_events(s, &e, 1) > 0) {
            if (e.kind == MOQ_EVENT_PUBLISH_REQUEST) {
                req++;
                pub = e.u.publish_request.pub;
                /* Against the handle derived before ingress, not the reverse. */
                MOQ_TEST_CHECK_EQ_U64(pub._opaque, want_handle);
            } else other++;
            moq_event_cleanup(&e);
        }
        MOQ_TEST_CHECK_EQ_INT(req, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
    }
    g_r1_handle = want_handle;

    moq_accept_publish_cfg_t ac;
    moq_accept_publish_cfg_init(&ac);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_publish(s, pub, &ac, 1),
                          (int)MOQ_OK);
    {
        int oks = 0, other = 0;
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) {
            if (a.kind == MOQ_ACTION_SEND_BIDI_STREAM &&
                a.u.send_bidi_stream.stream_ref._v == ref._v) oks++;
            else other++;
            moq_action_cleanup(&a);
        }
        MOQ_TEST_CHECK_EQ_INT(oks, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
    }

    /* A PUBLISH_DONE whose Stream Count this subscriber has not satisfied:
     * the terminal defers behind the stamped deadline instead of firing now. */
    {
        uint8_t msg[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_encode_publish_done(
                &w, R1_STATUS, R1_STREAMS,
                (moq_bytes_t){ (const uint8_t *)k_r1_reason,
                               sizeof(k_r1_reason) - 1 }), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, msg,
                                                  moq_buf_writer_offset(&w),
                                                  fin_observed, 1), (int)MOQ_OK);
    }
    /* The deferral itself owes no event; a close-half action may be queued. */
    {
        int events = 0;
        moq_event_t e;
        while (moq_session_poll_events(s, &e, 1) > 0) {
            events++;
            moq_event_cleanup(&e);
        }
        MOQ_TEST_CHECK_EQ_INT(events, 0);
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
    }

    {
        const moq_pub_entry_t *e = &s->publishes[want_slot];
        MOQ_TEST_CHECK_EQ_INT(pub_busy_count(s), 1);
        MOQ_TEST_CHECK_EQ_INT((int)e->state, (int)MOQ_PUB_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT((int)e->role, (int)MOQ_PUB_ROLE_SUBSCRIBER);
        MOQ_TEST_CHECK_EQ_U64(e->handle._opaque, want_handle);
        MOQ_TEST_CHECK_EQ_U64(e->generation, want_gen);
        MOQ_TEST_CHECK_EQ_U64(e->request_stream_ref._v, ref._v);
        MOQ_TEST_CHECK_EQ_INT(e->done_pending ? 1 : 0, 1);
        MOQ_TEST_CHECK_EQ_INT(e->done_expired ? 1 : 0, 0);
        failures += txs_check_span_bytes(e->done_reason_buf,
                                         e->done_reason_len, k_r1_reason,
                                         sizeof(k_r1_reason) - 1,
                                         "route1 done reason");
        MOQ_TEST_CHECK_EQ_U64(e->done_status_code, R1_STATUS);
        MOQ_TEST_CHECK_EQ_U64(e->done_stream_count, R1_STREAMS);
        MOQ_TEST_CHECK_EQ_INT(e->req_recv_fin ? 1 : 0, fin_observed ? 1 : 0);
        /* The deadline is stamped and still in the future, so the sweep the
         * terminal runs on is genuinely deferred rather than already due. */
        MOQ_TEST_CHECK(e->done_deadline_us > 1);
        MOQ_TEST_CHECK(e->done_deadline_us < R1_DEADLINE_US);
        check_registered(s, ref, MOQ_REQ_PUBLISH, want_slot);
    }
    if (out_handle) *out_handle = want_handle;
    return ref;
}

/* The public deferred sweep, never the internal finalize. */
static moq_result_t fire_deferred_sweep(moq_session_t *s, uint64_t handle)
{
    (void)handle;
    return moq_session_tick(s, R1_DEADLINE_US);
}

static void want_deferred_finalize(txs_norm_vec_t *v, moq_stream_ref_t ref,
                                   uint64_t handle)
{
    (void)ref;
    txs_img_t im;
    txs_img_init(&im);
    txs_img_u64(&im, handle);
    txs_img_u64(&im, R1_STATUS);
    txs_img_u64(&im, R1_STREAMS);
    txs_img_bytes(&im, (const uint8_t *)k_r1_reason, sizeof(k_r1_reason) - 1);
    MOQ_TEST_CHECK(txs_norm_append_img(v, MOQ_EVENT_PUBLISH_FINISHED, &im));
}

static txs_op_hooks_t r1_hooks = {
    NULL, NULL, NULL, NULL, pub_norm_event, pub_norm_action
};



/* -- T5: ann_local_teardown via its RECEIVER-side caller
 *      (session_namespace.c:85, reached only through
 *       moq_session_cancel_namespace at :955-983)
 *
 * The P4 marker consumer, and a DIFFERENT route from the announcer-side caller
 * already signed off: same helper, opposite role, reached by revoking a
 * namespace WE accepted rather than withdrawing one we announced. Under the
 * unified FIN-aware rule the helper reserves a NORMAL drain only when the peer
 * half is genuinely open; a same-call FIN owes none.
 *
 * Two arms, and they differ in WHERE the peer FIN comes from -- which the
 * fixture asserts rather than assumes, because the two are not the same fact.
 * Both are GREEN: whichever way the FIN arrives, it ends up in the DURABLE
 * latch, and the FIN rows are drainless under the unified rule.
 *
 *   T5-MARKER      the FIN rides the inbound PUBLISH_NAMESPACE that CREATES the
 *                  entry. The commit hands it to the announcement owner as
 *                  transitional ownership (the P4 handoff marker), and the
 *                  family's own handler consumes it into the durable latch -- so
 *                  a FIN-fed arm holds `req_recv_fin` TRUE and no marker.
 *   T5-DISCRIM     the FIN arrives as a legal empty FIN AFTER acceptance, which
 *                  `handle_announcement_stream_bytes` latches on the entry
 *                  (:121) -- a durable latch reached by a different route.
 *
 * Neither arm forges the latch from a same-call FIN by hand: the landed handoff
 * consumes the transitional marker into the latch, and both arms assert
 * `handoff_fin_pending` is cleared to prove that consumption is synchronous. */
#define T5_TOK_TYPE 11
static const uint8_t k_t5_tok[] = { 'r','c','v','t','o','k' };
static const uint8_t k_t5_ns0[] = { 'e','x','.','c','o','m' };
static const uint8_t k_t5_ns1[] = { 'r','e','c','v' };

static int g_t5_want_slot = -1;

static int t5_owner_edges(const og_graph_t *g, moq_stream_ref_t ref,
                          const char *what)
{
    int bad = 0;
    if (g_t5_want_slot < 0) {
        OG_DIAG("OG %s: no declared announcement slot\n", what);
        return 1;
    }
    bad += og_check_edge(g, OG_DOM_REQ_STREAMREF, ref._v,
                         MOQ_REQ_ANNOUNCEMENT, g_t5_want_slot, what);
    {
        const og_edge_spec_t want[] = { { OG_DOM_REQ_STREAMREF, ref._v } };
        bad += og_check_owner_edges(g, MOQ_REQ_ANNOUNCEMENT, g_t5_want_slot,
                                    want, 1, what);
    }
    bad += og_check_no_edge(g, OG_DOM_NS_REF, ref._v, what);
    return bad;
}

/* The inbound PUBLISH_NAMESPACE, with a non-default two-part namespace and one
 * USE_VALUE token, so the surfaced event is discriminating. */
static size_t enc_t5_publish_namespace(uint8_t *buf, size_t cap, uint64_t rid)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    moq_bytes_t parts[] = { { k_t5_ns0, sizeof(k_t5_ns0) },
                            { k_t5_ns1, sizeof(k_t5_ns1) } };
    moq_namespace_t ns = { parts, 2 };
    moq_d18_msg_params_t p = { 0 };
    p.auth_token_count = 1;
    p.auth_tokens[0].alias_type = 3;            /* USE_VALUE */
    p.auth_tokens[0].token_type = T5_TOK_TYPE;
    p.auth_tokens[0].token_value =
        (moq_bytes_t){ k_t5_tok, sizeof(k_t5_tok) };
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_d18_encode_publish_namespace(&w, rid, &ns, &p), (int)MOQ_OK);
    return moq_buf_writer_offset(&w);
}

/* Admit + accept a peer PUBLISH_NAMESPACE, leaving an ESTABLISHED
 * RECEIVER-role announcement. `fin_on_request` selects whether the creating
 * call carries the FIN; `want_latched` is what the entry must then hold. */
/* The peer stream ref and the clock are DECLARED by the caller: two peer
 * requests in one session are distinct transport streams, and a fixture that
 * reused one identity (or let time regress between rows) would be asserting
 * against a topology the transport cannot produce. */
static moq_stream_ref_t t5_admit_and_accept_at(moq_session_t *s,
                                               uint64_t rid,
                                               uint64_t peer_ref_v,
                                               uint64_t now_us,
                                               bool fin_on_request,
                                               bool want_latched,
                                               uint64_t *out_handle)
{
    /* Slot, generation and the handle they pack into, all derived from pool
     * state BEFORE the request is fed. */
    int want_slot = -1;
    uint32_t want_gen = 0;
    for (size_t i = 0; i < s->ann_cap; i++)
        if (s->announcements[i].state == MOQ_ANN_FREE) {
            want_slot = (int)i;
            want_gen = s->announcements[i].generation | 1u;
            break;
        }
    MOQ_TEST_CHECK(want_slot >= 0);
    if (want_slot < 0) return moq_stream_ref_from_u64(0);
    g_t5_want_slot = want_slot;
    g_ann_full_slot = want_slot;
    g_ann_full_allow_append = 0;   /* a direct API terminal appends nothing */
    uint64_t want_handle = moq_handle_pack(MOQ_HANDLE_POOL_ANNOUNCEMENT,
                                           s->session_tag, want_gen,
                                           (uint32_t)want_slot);
    /* The co-allocated receive buffer persists across entry reuse, so its
     * pointer and capacity are properties of the FREE slot: captured BEFORE
     * ingress rather than adopted from whatever admission leaves behind. */
    const uint8_t *want_recv_buf = s->announcements[want_slot].req_recv_buf;
    size_t want_recv_cap = s->announcements[want_slot].req_recv_cap;

    moq_stream_ref_t ref = moq_stream_ref_from_u64(peer_ref_v); /* peer-opened */
    uint8_t m[256];
    size_t n = enc_t5_publish_namespace(m, sizeof(m), rid);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, m, n, fin_on_request,
                                              now_us),
        (int)MOQ_OK);

    moq_announcement_t ann = MOQ_ANNOUNCEMENT_INVALID;
    {
        int pub = 0, other = 0;
        moq_event_t e;
        while (moq_session_poll_events(s, &e, 1) > 0) {
            if (e.kind == MOQ_EVENT_NAMESPACE_PUBLISHED) {
                pub++;
                const moq_namespace_published_event_t *ev =
                    &e.u.namespace_published;
                ann = ev->ann;
                /* Against the handle derived before ingress, and against the
                 * fixture's own declared namespace and token. */
                MOQ_TEST_CHECK_EQ_U64(ev->ann._opaque, want_handle);
                MOQ_TEST_CHECK_EQ_SIZE(ev->track_namespace.count, (size_t)2);
                MOQ_TEST_CHECK(ev->track_namespace.parts != NULL);
                if (ev->track_namespace.count == 2 &&
                    ev->track_namespace.parts) {
                    const moq_bytes_t *q0 = &ev->track_namespace.parts[0];
                    const moq_bytes_t *q1 = &ev->track_namespace.parts[1];
                    /* A non-empty field with NULL bytes is INVALID, not a
                     * comparison to skip. */
                    MOQ_TEST_CHECK_EQ_SIZE(q0->len, sizeof(k_t5_ns0));
                    MOQ_TEST_CHECK(q0->data != NULL);
                    if (q0->len == sizeof(k_t5_ns0) && q0->data)
                        MOQ_TEST_CHECK(memcmp(q0->data, k_t5_ns0,
                                              sizeof(k_t5_ns0)) == 0);
                    MOQ_TEST_CHECK_EQ_SIZE(q1->len, sizeof(k_t5_ns1));
                    MOQ_TEST_CHECK(q1->data != NULL);
                    if (q1->len == sizeof(k_t5_ns1) && q1->data)
                        MOQ_TEST_CHECK(memcmp(q1->data, k_t5_ns1,
                                              sizeof(k_t5_ns1)) == 0);
                } else MOQ_TEST_CHECK(0);
                MOQ_TEST_CHECK_EQ_SIZE(ev->token_count, (size_t)1);
                MOQ_TEST_CHECK(ev->tokens != NULL);
                if (ev->token_count == 1 && ev->tokens) {
                    const moq_bytes_t *tv = &ev->tokens[0].token_value;
                    MOQ_TEST_CHECK_EQ_U64(ev->tokens[0].token_type,
                                          T5_TOK_TYPE);
                    MOQ_TEST_CHECK_EQ_SIZE(tv->len, sizeof(k_t5_tok));
                    MOQ_TEST_CHECK(tv->data != NULL);
                    if (tv->len == sizeof(k_t5_tok) && tv->data)
                        MOQ_TEST_CHECK(memcmp(tv->data, k_t5_tok,
                                              sizeof(k_t5_tok)) == 0);
                } else MOQ_TEST_CHECK(0);
            } else other++;
            moq_event_cleanup(&e);
        }
        MOQ_TEST_CHECK_EQ_INT(pub, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
    }

    moq_accept_namespace_cfg_t ac;
    moq_accept_namespace_cfg_init(&ac);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_namespace(s, ann, &ac, now_us),
                          (int)MOQ_OK);
    /* The acceptance owes exactly one REQUEST_OK on the same ref, with our send
     * half left open and nothing after the envelope. */
    {
        int oks = 0, other = 0;
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) {
            if (a.kind == MOQ_ACTION_SEND_BIDI_STREAM &&
                a.u.send_bidi_stream.stream_ref._v == ref._v) {
                oks++;
                MOQ_TEST_CHECK_EQ_INT(a.u.send_bidi_stream.fin ? 1 : 0, 0);
                moq_buf_reader_t rr;
                moq_buf_reader_init(&rr, a.u.send_bidi_stream.data,
                                    a.u.send_bidi_stream.len);
                moq_control_envelope_t env;
                memset(&env, 0, sizeof(env));
                MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_envelope(&rr, &env),
                                      (int)MOQ_OK);
                MOQ_TEST_CHECK_EQ_U64(env.msg_type,
                                      (uint64_t)MOQ_D18_REQUEST_OK);
                MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&rr),
                                       (size_t)0);
                /* And the BODY. The envelope check sees only bytes AFTER the
                 * envelope; a payload carrying parameters or junk INSIDE it
                 * passes that and is caught only here. */
                MOQ_TEST_CHECK_EQ_INT(
                    (int)moq_d18_decode_request_ok(env.payload,
                                                   env.payload_len),
                    (int)MOQ_OK);
            } else other++;
            moq_action_cleanup(&a);
        }
        MOQ_TEST_CHECK_EQ_INT(oks, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
    }

    {
        const moq_ann_entry_t *e = &s->announcements[want_slot];
        MOQ_TEST_CHECK_EQ_INT(ann_busy_count(s), 1);
        MOQ_TEST_CHECK_EQ_INT((int)e->state, (int)MOQ_ANN_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT((int)e->role, (int)MOQ_ANN_ROLE_RECEIVER);
        MOQ_TEST_CHECK_EQ_U64(e->generation, want_gen);
        MOQ_TEST_CHECK_EQ_U64(e->handle._opaque, want_handle);
        MOQ_TEST_CHECK_EQ_U64(e->request_id, rid);
        MOQ_TEST_CHECK_EQ_U64(e->request_stream_ref._v, ref._v);
        MOQ_TEST_CHECK_EQ_INT(e->goaway_sent ? 1 : 0, 0);
        MOQ_TEST_CHECK_EQ_SIZE(e->req_recv_len, (size_t)0);
        /* Both derived pre-ingress, so admission cannot define them. */
        MOQ_TEST_CHECK(e->req_recv_buf == want_recv_buf);
        MOQ_TEST_CHECK_EQ_SIZE(e->req_recv_cap, want_recv_cap);
        /* The canonical namespace key, built INDEPENDENTLY from the fixture's
         * own parts rather than read back from the entry. */
        {
            uint8_t want_nsid[64];
            size_t n_ns = 0;
            want_nsid[n_ns++] = 2;                      /* part count */
            want_nsid[n_ns++] = (uint8_t)(sizeof(k_t5_ns0) >> 8);
            want_nsid[n_ns++] = (uint8_t)(sizeof(k_t5_ns0) & 0xff);
            memcpy(want_nsid + n_ns, k_t5_ns0, sizeof(k_t5_ns0));
            n_ns += sizeof(k_t5_ns0);
            want_nsid[n_ns++] = (uint8_t)(sizeof(k_t5_ns1) >> 8);
            want_nsid[n_ns++] = (uint8_t)(sizeof(k_t5_ns1) & 0xff);
            memcpy(want_nsid + n_ns, k_t5_ns1, sizeof(k_t5_ns1));
            n_ns += sizeof(k_t5_ns1);
            MOQ_TEST_CHECK_EQ_SIZE(e->ns_id_len, n_ns);
            MOQ_TEST_CHECK(e->ns_id_buf != NULL);
            if (e->ns_id_len == n_ns && e->ns_id_buf)
                MOQ_TEST_CHECK(memcmp(e->ns_id_buf, want_nsid, n_ns) == 0);
        }
        /* The FIN FACT this arm claims, asserted rather than assumed -- and
         * that it lives in the DURABLE latch, never left as transitional
         * ownership: this family's consumption is synchronous. */
        MOQ_TEST_CHECK_EQ_INT(e->req_recv_fin ? 1 : 0, want_latched ? 1 : 0);
        MOQ_TEST_CHECK_EQ_INT(e->handoff_fin_pending ? 1 : 0, 0);
        check_registered(s, ref, MOQ_REQ_ANNOUNCEMENT, want_slot);
    }
    if (out_handle) *out_handle = want_handle;
    return ref;
}

/* T5-MARKER: the FIN rides the creating request. The commit hands it to the
 * announcement owner as transitional ownership and the family's own handler
 * consumes it into the durable latch, so an arm fed with the FIN holds the
 * latch and no marker -- which is what makes the FIN rows drainless under the
 * unified rule. */
static moq_stream_ref_t t5_admit_and_accept(moq_session_t *s,
                                            bool fin_on_request,
                                            bool want_latched,
                                            uint64_t *out_handle)
{
    return t5_admit_and_accept_at(s, 0, /*peer_ref_v=*/1, /*now_us=*/1,
                                  fin_on_request, want_latched, out_handle);
}

static moq_stream_ref_t arm_t5_marker(moq_session_t *s, bool fin_observed,
                                      uint64_t *out_handle)
{
    /* The FIN rides the creating PUBLISH_NAMESPACE, so the accepted receiver
     * owner carries the latch exactly when this arm was fed one. */
    return t5_admit_and_accept(s, fin_observed, fin_observed, out_handle);
}

/* T5-DISCRIM: the FIN arrives as a legal empty FIN AFTER acceptance, which the
 * announce request-stream handler latches on the entry. No later peer bytes
 * follow, and the RECEIVER owner must stay established. */
static moq_stream_ref_t arm_t5_post_accept_fin(moq_session_t *s,
                                               bool fin_observed,
                                               uint64_t *out_handle)
{
    moq_stream_ref_t ref = t5_admit_and_accept(s, false, false, out_handle);
    if (ref._v == 0 || !fin_observed) return ref;

    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
        (int)MOQ_OK);
    check_no_output(s, &durable_abort_hooks);
    {
        const moq_ann_entry_t *e = &s->announcements[g_t5_want_slot];
        /* Latched, and the owner survives it whole. */
        MOQ_TEST_CHECK_EQ_INT(e->req_recv_fin ? 1 : 0, 1);
        MOQ_TEST_CHECK_EQ_INT((int)e->state, (int)MOQ_ANN_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT((int)e->role, (int)MOQ_ANN_ROLE_RECEIVER);
        MOQ_TEST_CHECK_EQ_SIZE(e->req_recv_len, (size_t)0);
        check_registered(s, ref, MOQ_REQ_ANNOUNCEMENT, g_t5_want_slot);
    }
    return ref;
}

static moq_result_t fire_cancel_namespace(moq_session_t *s, uint64_t handle)
{
    moq_announcement_t ann;
    ann._opaque = handle;
    moq_cancel_namespace_cfg_t c;
    moq_cancel_namespace_cfg_init(&c);
    /* A RESET/STOP carries only a numeric code, so the reason must stay empty;
     * a non-empty one is rejected outright (session_namespace.c:975). */
    return moq_session_cancel_namespace(s, ann, &c, 1);
}

static void want_t5_cancel(txs_norm_vec_t *v, moq_stream_ref_t ref,
                           uint64_t handle)
{
    (void)handle;
    want_nsl_output(v, ref);   /* one ABORT_BIDI_STREAM(ref, CANCELLED) */
}


/* T5 is NOT an Axis 3 durable-latch row: one of its two arms carries a
 * marker-sourced FIN rather than a latch, so it runs in its own function
 * instead of inside the durable-latch tranche. */
static void test_t5_receiver_teardown(void)
    {
        static const durable_case_t t5_marker = {
            .name = "cancel namespace (P4 marker)",
            .make = make_session_default,
            .arm = arm_t5_marker, .fire = fire_cancel_namespace,
            .want_output = want_t5_cancel,
            .hooks = &durable_abort_hooks, .owner = &ann_owner_ops,
            .owner_edges = t5_owner_edges, .extra = &ann_full_ops
        };
        run_durable_matrix(&t5_marker);

        static const durable_case_t t5_discrim = {
            .name = "cancel namespace (post-accept FIN)",
            .make = make_session_default,
            .arm = arm_t5_post_accept_fin, .fire = fire_cancel_namespace,
            .want_output = want_t5_cancel,
            .hooks = &durable_abort_hooks, .owner = &ann_owner_ops,
            .owner_edges = t5_owner_edges, .extra = &ann_full_ops
        };
        run_durable_matrix(&t5_discrim);
    }



/* A WHOLE live-announcement snapshot. `ann_seed_capture` deliberately models
 * only the fields common to every request family, so it sees neither the
 * canonical namespace key, nor `req_recv_cap`, nor `goaway_sent` -- an in-place
 * key edit or a lifecycle-flag flip would survive it. This one owns the entry's
 * bytes as well as its record, and is parameterized by ref (no single-slot
 * global). An oversized or unbacked record is INCOMPARABLE rather than read. */
#define ANN_WHOLE_MAX 256
typedef struct ann_whole_snap {
    int             valid;
    moq_ann_entry_t raw;
    size_t          ns_len;
    uint8_t         ns[ANN_WHOLE_MAX];
    size_t          rx_len;
    uint8_t         rx[ANN_WHOLE_MAX];
} ann_whole_snap_t;

static void ann_whole_capture(const moq_session_t *s, moq_stream_ref_t ref,
                              ann_whole_snap_t *o)
{
    memset(o, 0, sizeof(*o));
    const moq_ann_entry_t *e = ann_by_ref(s, ref);
    if (!e) return;
    if (e->ns_id_len > ANN_WHOLE_MAX || (e->ns_id_len && !e->ns_id_buf)) return;
    if (e->req_recv_len > ANN_WHOLE_MAX || (e->req_recv_len && !e->req_recv_buf))
        return;
    o->raw = *e;
    o->ns_len = e->ns_id_len;
    if (o->ns_len) memcpy(o->ns, e->ns_id_buf, o->ns_len);
    o->rx_len = e->req_recv_len;
    if (o->rx_len) memcpy(o->rx, e->req_recv_buf, o->rx_len);
    o->valid = 1;
}

static int ann_whole_check(const moq_session_t *s, moq_stream_ref_t ref,
                           const ann_whole_snap_t *want, const char *what)
{
    int bad = 0;
    ann_whole_snap_t now;
    ann_whole_capture(s, ref, &now);
    if (!want->valid || !now.valid) {
        fprintf(stderr, "AXIS4 %s: whole-owner record incomparable\n", what);
        return 1;
    }
#define ANN_W_EQ(field, a, b) do { \
    if ((uint64_t)(a) != (uint64_t)(b)) { \
        fprintf(stderr, "AXIS4 %s: whole owner %s = %llu, expected %llu\n", \
                what, field, (unsigned long long)(a), (unsigned long long)(b)); \
        bad++; \
    } \
} while (0)
    ANN_W_EQ("state", (int)now.raw.state, (int)want->raw.state);
    ANN_W_EQ("role", (int)now.raw.role, (int)want->raw.role);
    ANN_W_EQ("generation", now.raw.generation, want->raw.generation);
    ANN_W_EQ("handle", now.raw.handle._opaque, want->raw.handle._opaque);
    ANN_W_EQ("request_id", now.raw.request_id, want->raw.request_id);
    ANN_W_EQ("request_stream_ref", now.raw.request_stream_ref._v,
             want->raw.request_stream_ref._v);
    ANN_W_EQ("ns_id_len", now.raw.ns_id_len, want->raw.ns_id_len);
    ANN_W_EQ("req_recv_cap", now.raw.req_recv_cap, want->raw.req_recv_cap);
    ANN_W_EQ("req_recv_len", now.raw.req_recv_len, want->raw.req_recv_len);
    ANN_W_EQ("req_recv_fin", now.raw.req_recv_fin ? 1 : 0,
             want->raw.req_recv_fin ? 1 : 0);
    ANN_W_EQ("goaway_sent", now.raw.goaway_sent ? 1 : 0,
             want->raw.goaway_sent ? 1 : 0);
#undef ANN_W_EQ
    if (now.raw.req_recv_buf != want->raw.req_recv_buf) {
        fprintf(stderr, "AXIS4 %s: whole owner req_recv_buf pointer\n", what);
        bad++;
    }
    /* Pointer identity as well as bytes: swapping the owned allocation for a
     * byte-identical one changes ownership and would otherwise pass. */
    if (now.raw.ns_id_buf != want->raw.ns_id_buf) {
        fprintf(stderr, "AXIS4 %s: whole owner ns_id_buf pointer\n", what);
        bad++;
    }
    /* The OWNED bytes, not just the pointer: an in-place edit moves nothing. */
    if (now.ns_len != want->ns_len ||
        (now.ns_len && memcmp(now.ns, want->ns, now.ns_len) != 0)) {
        fprintf(stderr, "AXIS4 %s: whole owner namespace key bytes\n", what);
        bad++;
    }
    if (now.rx_len != want->rx_len ||
        (now.rx_len && memcmp(now.rx, want->rx, now.rx_len) != 0)) {
        fprintf(stderr, "AXIS4 %s: whole owner retained request bytes\n", what);
        bad++;
    }
    return bad;
}

/* The topology a successful retirement must leave: the pre-call graph minus
 * EXACTLY the target's declared stream-ref edge. Absent or duplicated is a
 * failure, not a silent no-op, and an overflowed derivation is refused. */
static int ann_derive_retired_topology(const og_graph_t *base, uint64_t ref_v,
                                       int slot, og_graph_t *out,
                                       const char *what)
{
    *out = *base;
    out->edge_count = 0;
    size_t removed = 0;
    for (size_t i = 0; i < base->edge_count; i++) {
        const og_edge_t *e = &base->edges[i];
        if (e->domain == OG_DOM_REQ_STREAMREF && e->key == ref_v &&
            e->kind == (int)MOQ_REQ_ANNOUNCEMENT && e->slot == slot) {
            removed++;
            continue;
        }
        if (out->edge_count >= OG_MAX_EDGES) {
            fprintf(stderr, "AXIS4 %s: derived topology overflowed\n", what);
            out->overflow = 1;
            return 1;
        }
        out->edges[out->edge_count++] = *e;
    }
    if (removed != 1) {
        fprintf(stderr,
                "AXIS4 %s: target stream-ref edge appears %zu times, expected 1\n",
                what, removed);
        return 1;
    }
    return 0;
}

/* Axis 4 needs a declared clock; the signed durable-case callback keeps its
 * own fixed time, so this is a separate narrow entry rather than a change to
 * it. */
static moq_result_t fire_cancel_namespace_at(moq_session_t *s, uint64_t handle,
                                             uint64_t now_us)
{
    moq_announcement_t ann;
    ann._opaque = handle;
    moq_cancel_namespace_cfg_t c;
    moq_cancel_namespace_cfg_init(&c);
    /* A RESET/STOP carries only a numeric code, so the reason must stay empty;
     * a non-empty one is rejected outright (session_namespace.c:975). */
    return moq_session_cancel_namespace(s, ann, &c, now_us);
}

/* The DECLARED free record. `ann_free_entry` (session_namespace.c:44) is a
 * SELECTIVE free, not a memset, so the expectation is the live entry plus
 * exactly the transformation that function performs: everything else is
 * asserted as RETAINED rather than adopted from the post-free entry. */
typedef struct ann_free_expect {
    int             state;
    int             role;
    uint32_t        generation;
    uint64_t        handle;
    uint64_t        request_id;
    uint64_t        request_stream_ref;
    const uint8_t  *ns_id_buf;
    size_t          ns_id_len;
    const uint8_t  *req_recv_buf;
    size_t          req_recv_cap;
    size_t          req_recv_len;
    int             req_recv_fin;
    int             goaway_sent;
} ann_free_expect_t;

/* Built from the LIVE entry BEFORE the terminal runs, so no expectation can be
 * derived from the record it is meant to check. */
static ann_free_expect_t ann_declare_free(const moq_ann_entry_t *live)
{
    ann_free_expect_t w;
    w.state              = (int)MOQ_ANN_FREE;
    w.role               = (int)live->role;          /* retained */
    w.generation         = live->generation + 1u;    /* uint32_t increment */
    w.handle             = live->handle._opaque;     /* retained */
    w.request_id         = live->request_id;         /* retained */
    w.request_stream_ref = 0;                        /* cleared with its key */
    w.ns_id_buf          = NULL;                     /* owned key freed */
    w.ns_id_len          = 0;
    w.req_recv_buf       = live->req_recv_buf;       /* co-allocated, retained */
    w.req_recv_cap       = live->req_recv_cap;       /* retained */
    w.req_recv_len       = 0;
    w.req_recv_fin       = 0;
    w.goaway_sent        = 0;
    return w;
}

/* Every field of moq_ann_entry_t, named individually. */
static int ann_check_free_record(const moq_session_t *s, int slot,
                                 const ann_free_expect_t *w, const char *what)
{
    int bad = 0;
    if (slot < 0 || (size_t)slot >= s->ann_cap) {
        fprintf(stderr, "AXIS4 %s: slot out of range\n", what);
        return 1;
    }
    const moq_ann_entry_t *e = &s->announcements[slot];
#define ANN_FREE_EQ(field, got, exp) do { \
    if ((uint64_t)(got) != (uint64_t)(exp)) { \
        fprintf(stderr, "AXIS4 %s: free record %s = %llu, expected %llu\n", \
                what, field, (unsigned long long)(got), \
                (unsigned long long)(exp)); \
        bad++; \
    } \
} while (0)
    ANN_FREE_EQ("state", (int)e->state, w->state);
    ANN_FREE_EQ("role", (int)e->role, w->role);
    ANN_FREE_EQ("generation", e->generation, w->generation);
    ANN_FREE_EQ("handle", e->handle._opaque, w->handle);
    ANN_FREE_EQ("request_id", e->request_id, w->request_id);
    ANN_FREE_EQ("request_stream_ref", e->request_stream_ref._v,
                w->request_stream_ref);
    ANN_FREE_EQ("ns_id_len", e->ns_id_len, w->ns_id_len);
    ANN_FREE_EQ("req_recv_cap", e->req_recv_cap, w->req_recv_cap);
    ANN_FREE_EQ("req_recv_len", e->req_recv_len, w->req_recv_len);
    ANN_FREE_EQ("req_recv_fin", e->req_recv_fin ? 1 : 0, w->req_recv_fin);
    ANN_FREE_EQ("goaway_sent", e->goaway_sent ? 1 : 0, w->goaway_sent);
#undef ANN_FREE_EQ
    if (e->ns_id_buf != w->ns_id_buf) {
        fprintf(stderr, "AXIS4 %s: free record ns_id_buf pointer\n", what);
        bad++;
    }
    if (e->req_recv_buf != w->req_recv_buf) {
        fprintf(stderr, "AXIS4 %s: free record req_recv_buf pointer\n", what);
        bad++;
    }
    return bad;
}

/* Drain accounting as the exact declared MULTISET -- ref and reason, not just a
 * count: a replaced ref or reason that preserved the count would otherwise
 * pass. This route reserves none, so the ring must be exactly what it was. */
static int ann_check_drain_same(const moq_session_t *s,
                                const drain_snap_t *want, const char *what)
{
    drain_snap_t now;
    drain_snap(s, &now);
    return drain_multiset_equals(&now, want, what);
}

/* Zero event AND action output, counted rather than sampled. */
static int ann_check_silent(moq_session_t *s, const char *what)
{
    int bad = 0, evs = 0, acts = 0;
    moq_event_t e;
    while (moq_session_poll_events(s, &e, 1) > 0) { evs++; moq_event_cleanup(&e); }
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) { acts++; moq_action_cleanup(&a); }
    if (evs) { fprintf(stderr, "AXIS4 %s: %d event(s)\n", what, evs); bad++; }
    if (acts) { fprintf(stderr, "AXIS4 %s: %d action(s)\n", what, acts); bad++; }
    return bad;
}

/* Axis 4 -- the terminal-cleanup obligation of `session_core_on_announce_torn_down`
 * (session_namespace.c:187), reached through the announcement arm of the bidi
 * teardown (session_subscribe.c:1473) by a peer RESET on an ESTABLISHED
 * RECEIVER announcement. It reserves no drain, so what it owes is RETIREMENT:
 * exactly one terminal event, a completely freed owner, and a generation
 * advance that makes the retired handle unusable -- including after the slot is
 * reused. Nothing here names or assumes any future FIN handoff marker.
 *
 * The first owner takes a legal empty FIN while still established, so its
 * durable latch is TRUE at the terminal and `ann_free_entry`'s explicit
 * `req_recv_fin = false` is observable at reuse; the reused request carries no
 * FIN, so a free that left the latch set would surface there.
 */
static void test_axis4_announce_torn_down_retirement(void)
{
    moq_session_t *s = make_session_default();
    MOQ_TEST_CHECK(s != NULL);
    if (!s) return;
    /* No local `failures`: the MOQ_TEST_CHECK macros and the helper returns
     * below must both land in the file-scope counter, or this row's
     * diagnostics would never reach the suite total. */

    /* now_us is monotonically nondecreasing across every phase below. */
    uint64_t h0 = 0;
    moq_stream_ref_t ref = t5_admit_and_accept_at(s, 0, /*peer_ref_v=*/1,
                                                  /*now_us=*/1, false, false,
                                                  &h0);
    const int slot = g_t5_want_slot;
    MOQ_TEST_CHECK(slot >= 0);
    if (slot < 0) { moq_session_destroy(s); return; }
    const uint32_t gen0 = moq_handle_generation(h0);

    /* A legal EMPTY FIN on the established RECEIVER request bidi. It latches
     * the durable FIN fact (session_namespace.c:121) and leaves the entry
     * established -- the handler's own "incomplete or clean FIN; wait for
     * teardown" exit -- so the later RESET still reaches the teardown through
     * the intact stream-ref key. The latch is ASSERTED, never forged. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 2),
        (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)s->announcements[slot].state,
                          (int)MOQ_ANN_ESTABLISHED);
    MOQ_TEST_CHECK_EQ_INT(s->announcements[slot].req_recv_fin ? 1 : 0, 1);
    failures += ann_check_silent(s, "post-accept FIN");

    /* Declared BEFORE the terminal: the free record and the exact generation. */
    const ann_free_expect_t want_free =
        ann_declare_free(&s->announcements[slot]);
    MOQ_TEST_CHECK_EQ_U64(want_free.generation, (uint64_t)(gen0 + 1u));
    /* A declared sentinel: the expected ring is built from the base plus the
     * (ref, REASON) pair we declare, then the production helper adds it and the
     * live ring is compared to that. Snapshotting AFTER the add would let a
     * wrong-reason insertion become its own baseline. */
    const moq_stream_ref_t sentinel = moq_stream_ref_from_u64(0x7001);
    drain_snap_t drain_base, drain0;
    drain_snap(s, &drain_base);
    drain_snap_plus(&drain_base, sentinel, MOQ_DRAIN_NORMAL, &drain0);
    MOQ_TEST_CHECK(drain_ref_add(s, sentinel));
    failures += ann_check_drain_same(s, &drain0, "drain sentinel seeded");
    og_graph_t g_pre_terminal;
    og_capture(s, &g_pre_terminal);

    /* The peer resets the announcement bidi: one terminal event, and only that. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_reset(s, ref, 0x1, 3), (int)MOQ_OK);
    {
        int done = 0, other = 0;
        moq_event_t e;
        while (moq_session_poll_events(s, &e, 1) > 0) {
            if (e.kind == MOQ_EVENT_NAMESPACE_DONE) {
                done++;
                MOQ_TEST_CHECK_EQ_U64(e.u.namespace_done.ann._opaque, h0);
            } else other++;
            moq_event_cleanup(&e);
        }
        MOQ_TEST_CHECK_EQ_INT(done, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
    }
    /* This route owes no output of its own. */
    {
        int acts = 0;
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) { acts++; moq_action_cleanup(&a); }
        MOQ_TEST_CHECK_EQ_INT(acts, 0);
    }

    /* The owner is retired COMPLETELY, against the record declared above. */
    failures += ann_check_free_record(s, slot, &want_free, "first retirement");
    failures += ann_check_drain_same(s, &drain0, "first retirement");
    {
        og_graph_t g;
        og_capture(s, &g);
        MOQ_TEST_CHECK_EQ_INT(og_check_integrity(&g, "axis4 torn-down"), 0);
        /* The WHOLE topology, derived from the pre-terminal graph by removing
         * exactly the target's stream-ref edge -- an unrelated edge inserted,
         * removed or repointed by this call is caught here, which a
         * target-owner-only check cannot do. */
        {
            og_graph_t want_topo;
            if (ann_derive_retired_topology(&g_pre_terminal, ref._v, slot,
                                            &want_topo, "first retirement") == 0)
                MOQ_TEST_CHECK_EQ_INT(
                    og_check_same_topology(&g, &want_topo, "first retirement"), 0);
            else failures++;
        }
        MOQ_TEST_CHECK_EQ_INT(
            og_check_no_edge(&g, OG_DOM_REQ_STREAMREF, ref._v, "axis4 torn-down"), 0);
        MOQ_TEST_CHECK_EQ_INT(
            og_check_no_edge(&g, OG_DOM_NS_REF, ref._v, "axis4 torn-down"), 0);
        MOQ_TEST_CHECK_EQ_INT(
            og_check_owner_unreferenced(&g, MOQ_REQ_ANNOUNCEMENT, slot,
                                   "axis4 torn-down"), 0);
    }

    /* Idempotence BEFORE reuse: a second teardown signal on the retired stream
     * misses the lookup entirely. It must not mutate unrelated session state,
     * advance the generation again, change the graph, add a drain, or
     * resurrect the owner. */
    {
        txs_snapshot_t before;
        txs_capture(s, &ref, 1, &before);
        expect_after_call_prepare(&before);
        og_graph_t g0;
        og_capture(s, &g0);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_reset(s, ref, 0x1, 4), (int)MOQ_OK);
        failures += ann_check_silent(s, "repeat terminal");
        failures += txs_check_eq(s, &ref, 1, &before, "repeat terminal");
        /* The SAME declared record: no second generation increment. */
        failures += ann_check_free_record(s, slot, &want_free, "repeat terminal");
        failures += ann_check_drain_same(s, &drain0, "repeat terminal");
        {
            og_graph_t g1;
            og_capture(s, &g1);
            MOQ_TEST_CHECK_EQ_INT(og_check_integrity(&g1, "repeat terminal"), 0);
            /* The captured baseline is COMPARED, not merely taken. */
            MOQ_TEST_CHECK_EQ_INT(
                og_check_same_topology(&g1, &g0, "repeat terminal"), 0);
            MOQ_TEST_CHECK_EQ_INT(
                og_check_owner_unreferenced(&g1, MOQ_REQ_ANNOUNCEMENT, slot,
                                            "repeat terminal"), 0);
        }
    }

    /* Slot reuse: a DISTINCT peer stream ref and request id, no FIN, at a
     * later time. The identity is derived from the DECLARED freed generation
     * rather than read back from the now-free slot. */
    const uint32_t want_reuse_gen = want_free.generation | 1u;
    const uint64_t want_reuse_handle =
        moq_handle_pack(MOQ_HANDLE_POOL_ANNOUNCEMENT, s->session_tag,
                        want_reuse_gen, (uint32_t)slot);
    uint64_t h1 = 0;
    moq_stream_ref_t ref1 = t5_admit_and_accept_at(s, 2, /*peer_ref_v=*/2,
                                                   /*now_us=*/5, false, false,
                                                   &h1);
    MOQ_TEST_CHECK_EQ_INT(g_t5_want_slot, slot);
    MOQ_TEST_CHECK(ref1._v != ref._v);
    MOQ_TEST_CHECK_EQ_U64(h1, want_reuse_handle);
    MOQ_TEST_CHECK(h1 != h0);
    /* The reused owner carries NO FIN: this is what makes ann_free_entry's
     * explicit latch clear load-bearing. */
    MOQ_TEST_CHECK_EQ_INT(s->announcements[slot].req_recv_fin ? 1 : 0, 0);
    MOQ_TEST_CHECK_EQ_U64(s->announcements[slot].generation,
                          (uint64_t)want_reuse_gen);

    /* The retired handle is still refused, and the REPLACEMENT owner survives
     * that refusal WHOLE -- session snapshot, owner record with its owned
     * bytes, graph topology, drain multiset and output all conserved. */
    {
        seed_snap_t owner_before;
        ann_owner_ops.capture(s, ref1, &owner_before);
        ann_whole_snap_t whole_before;
        ann_whole_capture(s, ref1, &whole_before);
        MOQ_TEST_CHECK(whole_before.valid);
        txs_snapshot_t before;
        txs_capture(s, &ref1, 1, &before);
        expect_after_call_prepare(&before);
        drain_snap_t drain1;
        drain_snap(s, &drain1);
        og_graph_t g_before;
        og_capture(s, &g_before);

        /* At a LATER declared time than the reuse: the row's clock never
         * regresses (the signed durable-case helper keeps its own fixed one). */
        MOQ_TEST_CHECK_EQ_INT((int)fire_cancel_namespace_at(s, h0, 6),
                              (int)MOQ_ERR_STALE_HANDLE);

        failures += ann_check_silent(s, "stale after reuse");
        failures += txs_check_eq(s, &ref1, 1, &before, "stale after reuse");
        ann_owner_ops.check(s, ref1, &owner_before, "stale after reuse");
        failures += ann_whole_check(s, ref1, &whole_before, "stale after reuse");
        failures += ann_check_drain_same(s, &drain1, "stale after reuse");
        {
            og_graph_t g;
            og_capture(s, &g);
            MOQ_TEST_CHECK_EQ_INT(og_check_integrity(&g, "stale after reuse"), 0);
            MOQ_TEST_CHECK_EQ_INT(
                og_check_same_topology(&g, &g_before, "stale after reuse"), 0);
            MOQ_TEST_CHECK_EQ_INT(
                ann_owner_edges(&g, ref1, "stale after reuse"), 0);
            /* The retired stream keys nothing, even though its slot is live. */
            MOQ_TEST_CHECK_EQ_INT(
                og_check_no_edge(&g, OG_DOM_REQ_STREAMREF, ref._v,
                                 "stale after reuse"), 0);
        }
        MOQ_TEST_CHECK_EQ_INT((int)s->announcements[slot].state,
                              (int)MOQ_ANN_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_U64(s->announcements[slot].generation,
                              (uint64_t)want_reuse_gen);
    }

    moq_session_destroy(s);
}

/* Axis 4 -- the same terminal under BACKPRESSURE.
 * `session_core_on_announce_torn_down` returns MOQ_ERR_WOULD_BLOCK when the
 * event queue is full (session_namespace.c:189), and `request_stream_teardown`
 * promises the caller can re-drive it with the owner untouched. The capacity
 * row above only ever exercises the path where the event fits, so a refusal
 * that lost or mutated the owner would pass there.
 *
 * Direct session-level retry, no bridge and no timing: the retry re-delivers
 * the SAME production teardown signal, never peer bytes.
 */
static void test_axis4_announce_torn_down_blocked_retry(void)
{
    /* One event slot, so a single queued event is a full queue. */
    moq_session_t *s = make_session_caps(MOQ_PERSPECTIVE_SERVER, 1);
    MOQ_TEST_CHECK(s != NULL);
    if (!s) return;

    uint64_t h0 = 0;
    moq_stream_ref_t ref = t5_admit_and_accept_at(s, 0, /*peer_ref_v=*/1,
                                                  /*now_us=*/1, false, false,
                                                  &h0);
    const int slot = g_t5_want_slot;
    MOQ_TEST_CHECK(slot >= 0);
    if (slot < 0) { moq_session_destroy(s); return; }

    /* The blocker: a SECOND peer request, admitted through the real ingress and
     * deliberately left unpolled, so the sole event slot is occupied by one
     * known event with a declared handle. */
    int blocker_slot = -1;
    uint32_t blocker_gen = 0;
    for (size_t i = 0; i < s->ann_cap; i++)
        if (s->announcements[i].state == MOQ_ANN_FREE) {
            blocker_slot = (int)i;
            blocker_gen = s->announcements[i].generation | 1u;
            break;
        }
    MOQ_TEST_CHECK(blocker_slot >= 0);
    if (blocker_slot < 0) { moq_session_destroy(s); return; }
    const uint64_t blocker_handle =
        moq_handle_pack(MOQ_HANDLE_POOL_ANNOUNCEMENT, s->session_tag,
                        blocker_gen, (uint32_t)blocker_slot);
    {
        moq_stream_ref_t bref = moq_stream_ref_from_u64(2);
        uint8_t m[256];
        size_t n = enc_t5_publish_namespace(m, sizeof(m), 2);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, bref, m, n, false, 2),
            (int)MOQ_OK);
    }
    MOQ_TEST_CHECK_EQ_SIZE(s->event_tail - s->event_head, (size_t)1);

    /* Everything the refusal must conserve, declared before it runs. Both the
     * TARGET and the BLOCKER refs are in the session snapshot: one ref cannot
     * make the other owner's transition visible. */
    const moq_stream_ref_t bref = moq_stream_ref_from_u64(2);
    const moq_stream_ref_t both_refs[2] = { ref, bref };
    const ann_free_expect_t want_free =
        ann_declare_free(&s->announcements[slot]);
    seed_snap_t owner_before;
    ann_owner_ops.capture(s, ref, &owner_before);
    ann_whole_snap_t whole_before, blocker_before;
    ann_whole_capture(s, ref, &whole_before);
    ann_whole_capture(s, bref, &blocker_before);
    MOQ_TEST_CHECK(whole_before.valid);
    MOQ_TEST_CHECK(blocker_before.valid);
    /* A declared sentinel, built independently (base + the declared
     * (ref, REASON) pair) rather than read back after the insertion. */
    const moq_stream_ref_t sentinel = moq_stream_ref_from_u64(0x7002);
    drain_snap_t drain_base, drain0;
    drain_snap(s, &drain_base);
    drain_snap_plus(&drain_base, sentinel, MOQ_DRAIN_NORMAL, &drain0);
    MOQ_TEST_CHECK(drain_ref_add(s, sentinel));
    failures += ann_check_drain_same(s, &drain0, "drain sentinel seeded");
    txs_snapshot_t before;
    txs_capture(s, both_refs, 2, &before);
    expect_after_call_prepare(&before);
    og_graph_t g_before;
    og_capture(s, &g_before);

    /* 1. The teardown is REFUSED for capacity. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_reset(s, ref, 0x1, 3),
        (int)MOQ_ERR_WOULD_BLOCK);
    failures += txs_check_eq(s, both_refs, 2, &before, "ann teardown blocked");
    ann_owner_ops.check(s, ref, &owner_before, "ann teardown blocked");
    failures += ann_whole_check(s, ref, &whole_before, "ann teardown blocked");
    failures += ann_whole_check(s, bref, &blocker_before,
                                "ann blocker across blocked");
    failures += ann_check_drain_same(s, &drain0, "ann teardown blocked");
    /* The target owner is untouched: still established, still keyed. */
    MOQ_TEST_CHECK_EQ_INT((int)s->announcements[slot].state,
                          (int)MOQ_ANN_ESTABLISHED);
    {
        og_graph_t g;
        og_capture(s, &g);
        MOQ_TEST_CHECK_EQ_INT(og_check_integrity(&g, "ann teardown blocked"), 0);
        MOQ_TEST_CHECK_EQ_INT(
            og_check_same_topology(&g, &g_before, "ann teardown blocked"), 0);
        MOQ_TEST_CHECK_EQ_INT(
            ann_owner_edges(&g, ref, "ann teardown blocked"), 0);
    }
    /* Only the declared blocker is queued, and no action was produced. */
    MOQ_TEST_CHECK_EQ_SIZE(s->event_tail - s->event_head, (size_t)1);
    {
        int acts = 0;
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) { acts++; moq_action_cleanup(&a); }
        MOQ_TEST_CHECK_EQ_INT(acts, 0);
    }

    /* 2. Drain EXACTLY the blocker, identified by its declared handle. */
    {
        int pub = 0, other = 0;
        moq_event_t e;
        while (moq_session_poll_events(s, &e, 1) > 0) {
            if (e.kind == MOQ_EVENT_NAMESPACE_PUBLISHED) {
                pub++;
                /* The COMPLETE fixture-declared image: handle, both namespace
                 * parts, token count/type/value bytes. Expected bytes come from
                 * the fixture's own constants, never from the event. */
                const moq_namespace_published_event_t *pe =
                    &e.u.namespace_published;
                MOQ_TEST_CHECK_EQ_U64(pe->ann._opaque, blocker_handle);
                MOQ_TEST_CHECK_EQ_SIZE(pe->track_namespace.count, (size_t)2);
                MOQ_TEST_CHECK(pe->track_namespace.parts != NULL);
                if (pe->track_namespace.count == 2 &&
                    pe->track_namespace.parts) {
                    const moq_bytes_t *q0 = &pe->track_namespace.parts[0];
                    const moq_bytes_t *q1 = &pe->track_namespace.parts[1];
                    MOQ_TEST_CHECK_EQ_SIZE(q0->len, sizeof(k_t5_ns0));
                    MOQ_TEST_CHECK(q0->data != NULL);
                    if (q0->len == sizeof(k_t5_ns0) && q0->data)
                        MOQ_TEST_CHECK(memcmp(q0->data, k_t5_ns0,
                                              sizeof(k_t5_ns0)) == 0);
                    MOQ_TEST_CHECK_EQ_SIZE(q1->len, sizeof(k_t5_ns1));
                    MOQ_TEST_CHECK(q1->data != NULL);
                    if (q1->len == sizeof(k_t5_ns1) && q1->data)
                        MOQ_TEST_CHECK(memcmp(q1->data, k_t5_ns1,
                                              sizeof(k_t5_ns1)) == 0);
                } else MOQ_TEST_CHECK(0);
                MOQ_TEST_CHECK_EQ_SIZE(pe->token_count, (size_t)1);
                MOQ_TEST_CHECK(pe->tokens != NULL);
                if (pe->token_count == 1 && pe->tokens) {
                    const moq_bytes_t *tv = &pe->tokens[0].token_value;
                    MOQ_TEST_CHECK_EQ_U64(pe->tokens[0].token_type, T5_TOK_TYPE);
                    MOQ_TEST_CHECK_EQ_SIZE(tv->len, sizeof(k_t5_tok));
                    MOQ_TEST_CHECK(tv->data != NULL);
                    if (tv->len == sizeof(k_t5_tok) && tv->data)
                        MOQ_TEST_CHECK(memcmp(tv->data, k_t5_tok,
                                              sizeof(k_t5_tok)) == 0);
                } else MOQ_TEST_CHECK(0);
            } else other++;
            moq_event_cleanup(&e);
        }
        MOQ_TEST_CHECK_EQ_INT(pub, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
    }

    /* 3. Retry the SAME teardown signal -- no peer bytes are re-delivered. */
    og_graph_t g_pre_retry;
    og_capture(s, &g_pre_retry);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_reset(s, ref, 0x1, 4), (int)MOQ_OK);
    {
        int done = 0, other = 0;
        moq_event_t e;
        while (moq_session_poll_events(s, &e, 1) > 0) {
            if (e.kind == MOQ_EVENT_NAMESPACE_DONE) {
                done++;
                MOQ_TEST_CHECK_EQ_U64(e.u.namespace_done.ann._opaque, h0);
            } else other++;
            moq_event_cleanup(&e);
        }
        MOQ_TEST_CHECK_EQ_INT(done, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
    }
    failures += ann_check_free_record(s, slot, &want_free, "ann teardown retry");
    failures += ann_check_drain_same(s, &drain0, "ann teardown retry");
    /* The UNRELATED blocker owner is whole across the target's retirement. */
    failures += ann_whole_check(s, bref, &blocker_before,
                                "ann blocker across retry");
    {
        int acts = 0;
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) { acts++; moq_action_cleanup(&a); }
        MOQ_TEST_CHECK_EQ_INT(acts, 0);
    }
    {
        og_graph_t g;
        og_capture(s, &g);
        MOQ_TEST_CHECK_EQ_INT(og_check_integrity(&g, "ann teardown retry"), 0);
        /* Derived from the PRE-RETRY graph by removing exactly the target's
         * stream-ref edge, so the blocker's edges must survive untouched. */
        {
            og_graph_t want_topo;
            if (ann_derive_retired_topology(&g_pre_retry, ref._v, slot,
                                            &want_topo, "ann teardown retry") == 0)
                MOQ_TEST_CHECK_EQ_INT(
                    og_check_same_topology(&g, &want_topo,
                                           "ann teardown retry"), 0);
            else failures++;
        }
        MOQ_TEST_CHECK_EQ_INT(
            og_check_no_edge(&g, OG_DOM_REQ_STREAMREF, ref._v,
                             "ann teardown retry"), 0);
        MOQ_TEST_CHECK_EQ_INT(
            og_check_owner_unreferenced(&g, MOQ_REQ_ANNOUNCEMENT, slot,
                                        "ann teardown retry"), 0);
    }

    /* 4. Once more: silent and fully conserved. */
    {
        txs_snapshot_t after;
        txs_capture(s, both_refs, 2, &after);
        expect_after_call_prepare(&after);
        og_graph_t g_idem;
        og_capture(s, &g_idem);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_reset(s, ref, 0x1, 5), (int)MOQ_OK);
        failures += ann_check_silent(s, "ann teardown idempotent");
        failures += txs_check_eq(s, both_refs, 2, &after,
                                 "ann teardown idempotent");
        failures += ann_check_free_record(s, slot, &want_free,
                                          "ann teardown idempotent");
        failures += ann_check_drain_same(s, &drain0, "ann teardown idempotent");
        failures += ann_whole_check(s, bref, &blocker_before,
                                    "ann blocker across idempotent");
        {
            og_graph_t g;
            og_capture(s, &g);
            MOQ_TEST_CHECK_EQ_INT(
                og_check_integrity(&g, "ann teardown idempotent"), 0);
            MOQ_TEST_CHECK_EQ_INT(
                og_check_same_topology(&g, &g_idem, "ann teardown idempotent"), 0);
        }
    }

    moq_session_destroy(s);
}

static void test_axis3_durable_terminals(void)
{
    /* session_namespace.c:792 -> :85 -- the withdrawal reserves its drain
     * FIN-aware, consulting the latch: none when the peer half already closed. */
    static const durable_case_t announcer_teardown = {
        .name = "announcer teardown", .make = make_session_client,
        .arm = arm_announcer_established,
        .fire = fire_publish_namespace_done,
        .want_output = want_announcer_teardown,
        .hooks = &durable_abort_hooks, .owner = &ann_owner_ops,
        .owner_edges = ann_owner_edges, .extra = &ann_full_ops
    };
    run_durable_matrix(&announcer_teardown);

    /* session_subscribe.c:3012 -- the cancel reserves its drain FIN-aware,
     * consulting the latch: none when the peer half already closed. */
    {
        static const durable_case_t sub_cancel = {
            .name = "subscriber unsubscribe", .make = make_session_client,
            .arm = arm_subscriber_established, .fire = fire_unsubscribe,
            .want_output = want_bidi_cancel,
            .hooks = &durable_abort_hooks, .owner = &sub_owner_ops,
            .owner_edges = sub_owner_edges, .extra = &r6_full_ops
        };
        run_durable_matrix(&sub_cancel);
    }

    /* session_fetch.c:1485 -- the cancel reserves its drain FIN-aware,
     * consulting the latch: none when the peer half already closed. */
    {
        static const durable_case_t fetch_cancel = {
            .name = "fetch cancel", .make = make_session_client,
            .arm = arm_fetch_pending, .fire = fire_fetch_cancel,
            .want_output = want_fetch_cancel,
            .hooks = &durable_abort_hooks, .owner = &fetch_owner_ops,
            .owner_edges = fetch_owner_edges, .extra = &r5_full_ops
        };
        run_durable_matrix(&fetch_cancel);
    }

    /* session_subscribe.c:3250 -- the terminal reserves its drain FIN-aware,
     * consulting the entry's own latch: none when the peer half already closed. */
    {
        static const durable_case_t done_sub = {
            .name = "done subscribe", .make = make_session_default,
            .arm = arm_accepted_subscription, .fire = fire_done_subscribe,
            .want_output = want_done_subscribe,
            .hooks = &r7_hooks, .owner = &sub_owner_ops,
            .owner_edges = r7_owner_edges, .extra = &r7_full_ops
        };
        run_durable_matrix(&done_sub);
    }

    /* session_publish.c:1847 -- the positive control: this terminal already
     * consults the latch, so all four rows complete. */
    {
        static const durable_case_t finish_pub = {
            .name = "finish publish", .make = make_session_client,
            .arm = arm_publisher_established, .fire = fire_finish_publish,
            .want_output = want_finish_publish,
            .hooks = &r7_hooks, .owner = &r4_owner_ops,
            .owner_edges = r4_owner_edges, .extra = &r4_full_ops
        };
        run_durable_matrix(&finish_pub);
    }

    /* session_namespace.c:582 -- the announcer-side terminal REQUEST_ERROR.
     * Its FIN arrives with the message, so it runs on the current-call
     * runner rather than the durable one. */
    {
        static const terminal_case_t ann_error = {
            .name = "announcement error", .make = make_session_client,
            .arm = arm_announcement_pending,
            .encode = enc_announcement_error,
            .drain_reason = MOQ_DRAIN_NORMAL,
            .want_output = want_announcement_error, .hooks = &r3_hooks,
            .owner = &ann_owner_ops, .extra = &ann_full_ops,
            .graph = &r3_graph_ops
        };
        /* Permanently pinned, all three members: this route's coverage is the
         * reason the capability exists, so dropping or repointing any of them
         * fails here rather than silently reducing what the runner checks. */
        MOQ_TEST_CHECK(ann_error.graph == &r3_graph_ops);
        MOQ_TEST_CHECK(r3_graph_ops.owner_edges == r3_owner_edges);
        MOQ_TEST_CHECK_EQ_INT(r3_graph_ops.owner_kind,
                              (int)MOQ_REQ_ANNOUNCEMENT);
        MOQ_TEST_CHECK(r3_graph_ops.owner_slot == &g_r3_want_slot);
        run_terminal_matrix(&ann_error, 2);
    }

    /* session_publish.c:144 -- the second positive control, reached through the
     * public deferred sweep. */
    {
        static const durable_case_t deferred_finalize = {
            .name = "deferred publish finalize", .make = make_session_default,
            .arm = arm_deferred_publish, .fire = fire_deferred_sweep,
            .want_output = want_deferred_finalize,
            .hooks = &r1_hooks, .owner = &r4_owner_ops,
            .owner_edges = r1_owner_edges, .extra = &r1_full_ops,
            .refusal_is_silent = 1
        };
        run_durable_matrix(&deferred_finalize);
    }

}

/* -- Harness self-checks --------------------------------------------- */

static void test_harness(void)
{
    /* A complete pre-commit descriptor validates; a missing required member,
     * and each incomplete seed bundle, are rejected. */
    {
        precommit_case_t pc = {
            .name = "probe", .make = make_session_default,
            .encode = enc_publish_unknown_alias,
            .check_owners = owners_publish_none
        };
        seed_ops_t partial;
        MOQ_TEST_CHECK_EQ_INT(precommit_problems(&pc), 0);
        pc.seed_ops = &pub_seed_ops;
        MOQ_TEST_CHECK_EQ_INT(precommit_problems(&pc), 0);
        partial = pub_seed_ops; partial.seed = NULL;
        pc.seed_ops = &partial;
        MOQ_TEST_CHECK(precommit_problems(&pc) > 0);
        partial = pub_seed_ops; partial.capture = NULL;
        MOQ_TEST_CHECK(precommit_problems(&pc) > 0);
        partial = pub_seed_ops; partial.check = NULL;
        MOQ_TEST_CHECK(precommit_problems(&pc) > 0);
        pc.seed_ops = &pub_seed_ops;
        pc.check_owners = NULL;
        MOQ_TEST_CHECK(precommit_problems(&pc) > 0);
    }

    /* Each registry field the owner comparator carries is compared on its own.
     * `id_request_id` needs saying explicitly: this fixture's first request id
     * is 0 and a deleted key also reports 0, so a live perturbation can never
     * distinguish a hardcoded-zero capture from a real one. */
    {
        req_seed_snap_t a, b;
        int base, delta, quiet = txs_quiet;
        memset(&a, 0, sizeof(a));
        b = a;
        txs_quiet = 1;
        base = failures;
        req_seed_diff(&a, &b, "registry probe");      /* identical */
        delta = failures - base;
        failures = base;
        txs_quiet = quiet;
        MOQ_TEST_CHECK_EQ_INT(delta, 0);

        b = a;
        b.id_request_id = 7;                          /* differs ONLY here */
        txs_quiet = 1;
        base = failures;
        req_seed_diff(&a, &b, "registry probe");
        delta = failures - base;
        failures = base;
        txs_quiet = quiet;
        MOQ_TEST_CHECK_EQ_INT(delta, 1);

        b = a;
        b.id_has_request_id = 1;
        txs_quiet = 1;
        base = failures;
        req_seed_diff(&a, &b, "registry probe");
        delta = failures - base;
        failures = base;
        txs_quiet = quiet;
        MOQ_TEST_CHECK_EQ_INT(delta, 1);
    }

    /* And the CAPTURE of that field is exercised against a NONZERO request id,
     * which no live row can supply: every fixture's first request is id 0, so a
     * hardcoded-zero capture would agree with the registry everywhere. */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
        if (s) {
            moq_bytes_t p0[] = { MOQ_BYTES_LITERAL("a") };
            moq_bytes_t p1[] = { MOQ_BYTES_LITERAL("b") };
            moq_subscribe_namespace_cfg_t nc;
            moq_ns_sub_handle_t h0, h1;
            memset(&h0, 0, sizeof(h0));
            memset(&h1, 0, sizeof(h1));
            moq_subscribe_namespace_cfg_init(&nc);
            nc.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
            nc.track_namespace_prefix = (moq_namespace_t){ p0, 1 };
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_session_subscribe_namespace(s, &nc, 1, &h0), (int)MOQ_OK);
            nc.track_namespace_prefix = (moq_namespace_t){ p1, 1 };
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_session_subscribe_namespace(s, &nc, 1, &h1), (int)MOQ_OK);
            moq_action_t a;
            while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);

            /* The second request's id is nonzero, and the capture must report
             * exactly it. */
            uint64_t rid = 0;
            for (size_t i = 0; i < s->ns_sub_cap; i++)
                if (s->ns_subs[i].state != MOQ_NS_SUB_FREE &&
                    s->ns_subs[i].handle._opaque == h1._opaque)
                    rid = s->ns_subs[i].request_id;
            MOQ_TEST_CHECK(rid != 0);
            req_seed_snap_t r;
            memset(&r, 0, sizeof(r));
            req_seed_capture_registry(&r, s, moq_stream_ref_from_u64(0), rid);
            MOQ_TEST_CHECK_EQ_INT(r.id_kind, (int)MOQ_REQ_NAMESPACE_SUB);
            MOQ_TEST_CHECK_EQ_INT(r.id_has_request_id, 1);
            MOQ_TEST_CHECK_EQ_U64(r.id_request_id, rid);
            moq_session_destroy(s);
        }
    }

    /* The checked append refuses exactly at the buffer boundary, and refuses
     * an invalid or impossible base without touching it. */
    {
        req_seed_snap_t r;
        static const uint8_t tail[2] = { 0xa1, 0xa2 };
        memset(&r, 0, sizeof(r));
        r.recv_len = REQ_SEED_BUF_MAX - 2;
        req_seed_append(&r, tail, 2);                 /* exactly fits */
        MOQ_TEST_CHECK_EQ_INT(r.buf_invalid, 0);
        MOQ_TEST_CHECK_EQ_SIZE(r.recv_len, (size_t)REQ_SEED_BUF_MAX);
        MOQ_TEST_CHECK(memcmp(r.buf + REQ_SEED_BUF_MAX - 2, tail, 2) == 0);

        memset(&r, 0, sizeof(r));
        r.recv_len = REQ_SEED_BUF_MAX - 1;
        req_seed_append(&r, tail, 2);                 /* one byte too many */
        MOQ_TEST_CHECK_EQ_INT(r.buf_invalid, 1);
        MOQ_TEST_CHECK_EQ_SIZE(r.recv_len, (size_t)(REQ_SEED_BUF_MAX - 1));

        memset(&r, 0, sizeof(r));
        r.recv_len = REQ_SEED_BUF_MAX;
        req_seed_append(&r, tail, 0);                 /* full, empty tail */
        MOQ_TEST_CHECK_EQ_INT(r.buf_invalid, 0);
        MOQ_TEST_CHECK_EQ_SIZE(r.recv_len, (size_t)REQ_SEED_BUF_MAX);

        memset(&r, 0, sizeof(r));
        r.recv_len = (size_t)REQ_SEED_BUF_MAX + 64;   /* impossible capture */
        req_seed_append(&r, tail, 2);
        MOQ_TEST_CHECK_EQ_INT(r.buf_invalid, 1);
        MOQ_TEST_CHECK_EQ_SIZE(r.recv_len, (size_t)REQ_SEED_BUF_MAX + 64);

        /* The case that distinguishes a subtraction-only bound from a sum: a
         * length this large makes base + tail wrap, so a sum-based guard would
         * admit the append and copy at an impossible offset. */
        memset(&r, 0, sizeof(r));
        r.recv_len = (size_t)-1;
        req_seed_append(&r, tail, 2);
        MOQ_TEST_CHECK_EQ_INT(r.buf_invalid, 1);
        MOQ_TEST_CHECK_EQ_SIZE(r.recv_len, (size_t)-1);

        memset(&r, 0, sizeof(r));
        r.buf_invalid = 1;
        req_seed_append(&r, tail, 2);                 /* invalid base */
        MOQ_TEST_CHECK_EQ_INT(r.buf_invalid, 1);
        MOQ_TEST_CHECK_EQ_SIZE(r.recv_len, (size_t)0);
    }

    /* The same for an Axis 3 terminal descriptor: a complete owner bundle and a
     * declared drain reason are both required. */
    {
        terminal_case_t tc = {
            .name = "probe", .make = make_session_default,
            .arm = arm_established_announcement, .encode = enc_update_next,
            .drain_reason = MOQ_DRAIN_NORMAL,
            .want_output = want_update_rejected,
            .hooks = &pub_hooks_for_actions, .owner = &ann_owner_ops
        };
        owner_ops_t partial;
        MOQ_TEST_CHECK_EQ_INT(terminal_problems(&tc), 0);
        partial = ann_owner_ops; partial.capture = NULL;
        tc.owner = &partial;
        MOQ_TEST_CHECK(terminal_problems(&tc) > 0);
        partial = ann_owner_ops; partial.check = NULL;
        MOQ_TEST_CHECK(terminal_problems(&tc) > 0);
        partial = ann_owner_ops; partial.check_retired = NULL;
        MOQ_TEST_CHECK(terminal_problems(&tc) > 0);
        tc.owner = &ann_owner_ops;
        MOQ_TEST_CHECK_EQ_INT(terminal_problems(&tc), 0);
        tc.drain_reason = (uint8_t)0x7f;      /* no such reason */
        MOQ_TEST_CHECK(terminal_problems(&tc) > 0);
        tc.drain_reason = MOQ_DRAIN_NORMAL;
        tc.arm = NULL;
        MOQ_TEST_CHECK(terminal_problems(&tc) > 0);
        tc.arm = arm_established_announcement;

        /* The graph capability is all-or-nothing, and its KIND is part of the
         * contract: MOQ_REQ_NONE is exactly what a deleted initializer leaves
         * behind, and it would make the owner-target retirement scan pass
         * vacuously. */
        {
            static int probe_slot = 0;
            terminal_graph_ops_t g = {
                .owner_edges = r3_owner_edges,
                .owner_kind  = MOQ_REQ_ANNOUNCEMENT,
                .owner_slot  = &probe_slot
            };
            tc.graph = NULL;                       /* absent: legal */
            MOQ_TEST_CHECK_EQ_INT(terminal_problems(&tc), 0);
            tc.graph = &g;                         /* complete: legal */
            MOQ_TEST_CHECK_EQ_INT(terminal_problems(&tc), 0);
            g.owner_kind = MOQ_REQ_NONE;           /* the deleted-field value */
            MOQ_TEST_CHECK(terminal_problems(&tc) > 0);
            g.owner_kind = 0x7f;                   /* no such family */
            MOQ_TEST_CHECK(terminal_problems(&tc) > 0);
            g.owner_kind = MOQ_REQ_ANNOUNCEMENT;
            g.owner_edges = NULL;
            MOQ_TEST_CHECK(terminal_problems(&tc) > 0);
            g.owner_edges = r3_owner_edges;
            g.owner_slot = NULL;
            MOQ_TEST_CHECK(terminal_problems(&tc) > 0);
            g.owner_slot = &probe_slot;
            MOQ_TEST_CHECK_EQ_INT(terminal_problems(&tc), 0);
            tc.graph = NULL;
        }

        /* And the kind is LOAD-BEARING, not decoration: a live owner audited
         * under its own family reports its edges, while the SAME scan under any
         * other family finds nothing and would pass vacuously. That is the
         * hazard the validation above exists to prevent. */
        {
            moq_session_t *gs = make_session_client();
            uint64_t h = 0;
            moq_stream_ref_t gref = arm_announcer_established(gs, false, &h);
            if (gref._v != 0) {
                og_graph_t g;
                og_capture(gs, &g);
                MOQ_TEST_CHECK(og_check_integrity(&g, "kind probe") == 0);
                og_quiet++;
                /* its own family sees the live owner's edges */
                int own = og_check_owner_edges(&g, MOQ_REQ_ANNOUNCEMENT,
                                               g_ann_want_slot, NULL, 0,
                                               "kind probe");
                /* every other family is blind to it */
                int other = og_check_owner_edges(&g, MOQ_REQ_FETCH,
                                                 g_ann_want_slot, NULL, 0,
                                                 "kind probe");
                og_quiet--;
                MOQ_TEST_CHECK(own > 0);
                MOQ_TEST_CHECK_EQ_INT(other, 0);
            }
            moq_session_destroy(gs);
        }
        MOQ_TEST_CHECK_EQ_INT(terminal_problems(&tc), 0);

        /* The extra-state bundle is optional but INDIVISIBLE: absent is fine,
         * any one member missing is a descriptor error -- a capture with no
         * comparison, or a comparison in one direction only, proves nothing. */
        extra_ops_t xp;
        tc.extra = NULL;
        MOQ_TEST_CHECK_EQ_INT(terminal_problems(&tc), 0);
        tc.extra = &fu_extra_ops;
        MOQ_TEST_CHECK_EQ_INT(terminal_problems(&tc), 0);
        tc.extra = &xp;
        xp = fu_extra_ops; xp.capture = NULL;
        MOQ_TEST_CHECK(terminal_problems(&tc) > 0);
        xp = fu_extra_ops; xp.check_blocked = NULL;
        MOQ_TEST_CHECK(terminal_problems(&tc) > 0);
        xp = fu_extra_ops; xp.check_committed = NULL;
        MOQ_TEST_CHECK(terminal_problems(&tc) > 0);
        tc.extra = NULL;

        /* The namespace-owner snapshot refuses malformed state BEFORE it
         * dereferences anything: under a sanitizer each of these would abort
         * the run if the guard were removed. */
        {
            moq_session_t *ms = make_session_default();
            MOQ_TEST_CHECK(ms != NULL);
            if (ms && ms->ns_sub_cap > 0) {
                moq_ns_sub_entry_t *e = &ms->ns_subs[0];
                moq_bytes_t bad_part = { NULL, 4 };
                nso_snap_t v;
                e->state = MOQ_NS_SUB_RECVING_PUBLISHER;
                e->prefix_valid = true;

                e->prefix_count = NSO_MAX_PARTS + 1;
                e->prefix_parts = &bad_part;
                nso_read(ms, 0, &v);
                MOQ_TEST_CHECK_EQ_INT(v.invalid, 1);

                e->prefix_count = 2;
                e->prefix_parts = NULL;
                nso_read(ms, 0, &v);
                MOQ_TEST_CHECK_EQ_INT(v.invalid, 1);

                e->prefix_count = 1;
                e->prefix_parts = &bad_part;      /* len 4, data NULL */
                nso_read(ms, 0, &v);
                MOQ_TEST_CHECK_EQ_INT(v.invalid, 1);

                e->prefix_count = 0;
                e->prefix_parts = NULL;
                e->prefix_valid = false;
                {
                    size_t saved_len = e->recv_len;
                    uint8_t *saved_buf = e->recv_buf;
                    e->recv_len = NSO_BUF_MAX + 1;
                    nso_read(ms, 0, &v);
                    MOQ_TEST_CHECK_EQ_INT(v.invalid, 1);
                    /* A retained length with no buffer is refused too. */
                    e->recv_len = 4;
                    e->recv_buf = NULL;
                    nso_read(ms, 0, &v);
                    MOQ_TEST_CHECK_EQ_INT(v.invalid, 1);
                    e->recv_buf = saved_buf;
                    e->recv_len = saved_len;
                }
                /* An out-of-range slot is refused without touching the pool. */
                nso_read(ms, (int)ms->ns_sub_cap + 3, &v);
                MOQ_TEST_CHECK_EQ_INT(v.invalid, 1);
                /* Two incomparable records never compare equal. */
                {
                    nso_snap_t a = v, b = v;
                    txs_quiet = 1;
                    MOQ_TEST_CHECK(nso_diff(&a, &b, "nso invalid") > 0);
                    txs_quiet = 0;
                }
                /* Every field this block overwrote goes back, so destruction
                 * frees the entry the session actually owns. */
                e->prefix_count = 0;
                e->prefix_parts = NULL;
                e->prefix_valid = false;
                e->state = MOQ_NS_SUB_FREE;
            }
            if (ms) moq_session_destroy(ms);
        }

        /* The durable-terminal descriptor: complete validates, and each
         * required member -- top-level callback, normalizer or owner hook --
         * is rejected on its own. */
        {
            durable_case_t dc = {
                .name = "probe", .make = make_session_client,
                .arm = arm_announcer_established,
                .fire = fire_publish_namespace_done,
                .want_output = want_announcer_teardown,
                .hooks = &durable_abort_hooks, .owner = &ann_owner_ops,
                .owner_edges = ann_owner_edges
            };
            txs_op_hooks_t dhk;
            owner_ops_t dow;
            MOQ_TEST_CHECK_EQ_INT(durable_problems(&dc), 0);
            dc.name = NULL; MOQ_TEST_CHECK(durable_problems(&dc) > 0);
            dc.name = "probe";
            dc.make = NULL; MOQ_TEST_CHECK(durable_problems(&dc) > 0);
            dc.make = make_session_client;
            dc.arm = NULL;  MOQ_TEST_CHECK(durable_problems(&dc) > 0);
            dc.arm = arm_announcer_established;
            dc.fire = NULL; MOQ_TEST_CHECK(durable_problems(&dc) > 0);
            dc.fire = fire_publish_namespace_done;
            dc.want_output = NULL; MOQ_TEST_CHECK(durable_problems(&dc) > 0);
            dc.want_output = want_announcer_teardown;
            dc.owner_edges = NULL; MOQ_TEST_CHECK(durable_problems(&dc) > 0);
            dc.owner_edges = ann_owner_edges;
            MOQ_TEST_CHECK_EQ_INT(durable_problems(&dc), 0);
            dc.hooks = NULL; MOQ_TEST_CHECK(durable_problems(&dc) > 0);
            dhk = durable_abort_hooks; dhk.normalize_event = NULL;
            dc.hooks = &dhk; MOQ_TEST_CHECK(durable_problems(&dc) > 0);
            dhk = durable_abort_hooks; dhk.normalize_action = NULL;
            MOQ_TEST_CHECK(durable_problems(&dc) > 0);
            dc.hooks = &durable_abort_hooks;
            dc.owner = NULL; MOQ_TEST_CHECK(durable_problems(&dc) > 0);
            dow = ann_owner_ops; dow.capture = NULL;
            dc.owner = &dow; MOQ_TEST_CHECK(durable_problems(&dc) > 0);
            dow = ann_owner_ops; dow.check = NULL;
            MOQ_TEST_CHECK(durable_problems(&dc) > 0);
            dow = ann_owner_ops; dow.check_retired = NULL;
            MOQ_TEST_CHECK(durable_problems(&dc) > 0);
            dc.owner = &ann_owner_ops;
            MOQ_TEST_CHECK_EQ_INT(durable_problems(&dc), 0);
        }

        /* The reject descriptor: complete validates, each required member
         * missing is rejected on its own. */
        {
            nsr_case_t nc = {
                .name = "probe", .arm = nsr_arm_auth, .encode = enc_sns_auth,
                .err_code = 0x17,
                .declare_transient = nsr_auth_declare
            };
            MOQ_TEST_CHECK_EQ_INT(nsr_problems(&nc), 0);
            nc.name = NULL;  MOQ_TEST_CHECK(nsr_problems(&nc) > 0);
            nc.name = "probe";
            nc.arm = NULL;   MOQ_TEST_CHECK(nsr_problems(&nc) > 0);
            nc.arm = nsr_arm_auth;
            nc.encode = NULL; MOQ_TEST_CHECK(nsr_problems(&nc) > 0);
            nc.encode = enc_sns_auth;
            nc.declare_transient = NULL;
            MOQ_TEST_CHECK(nsr_problems(&nc) > 0);
            nc.declare_transient = nsr_auth_declare;
            nc.err_code = 0; MOQ_TEST_CHECK(nsr_problems(&nc) > 0);
            nc.err_code = 0x17;
            MOQ_TEST_CHECK_EQ_INT(nsr_problems(&nc), 0);
        }

        /* The output hooks, at each of their three boundaries. */
        txs_op_hooks_t hk;
        tc.hooks = NULL;
        MOQ_TEST_CHECK(terminal_problems(&tc) > 0);
        hk = pub_hooks_for_actions; hk.normalize_event = NULL;
        tc.hooks = &hk;
        MOQ_TEST_CHECK(terminal_problems(&tc) > 0);
        hk = pub_hooks_for_actions; hk.normalize_action = NULL;
        MOQ_TEST_CHECK(terminal_problems(&tc) > 0);
        tc.hooks = &pub_hooks_for_actions;
        MOQ_TEST_CHECK_EQ_INT(terminal_problems(&tc), 0);
    }

    /* A well-formed descriptor validates; each capability/callback
     * disagreement is rejected. */
    fin_owner_case_t f = pub_family();
    MOQ_TEST_CHECK_EQ_INT(fin_owner_problems(&f), 0);
    {
        fin_owner_case_t bad = f;
        bad.caps |= FIN_TERM_INTERNAL_REJECT;   /* bit without its callback */
        MOQ_TEST_CHECK(fin_owner_problems(&bad) > 0);
    }
    {
        fin_owner_case_t bad = f;
        bad.caps &= ~(unsigned)FIN_TERM_APP_REJECT;  /* callback without bit */
        MOQ_TEST_CHECK(fin_owner_problems(&bad) > 0);
    }
    {
        fin_owner_case_t bad = f;
        bad.caps |= 1u << 7;                    /* unknown capability bit */
        MOQ_TEST_CHECK(fin_owner_problems(&bad) > 0);
    }
    {
        fin_owner_case_t bad = f;
        bad.feed = NULL;
        MOQ_TEST_CHECK(fin_owner_problems(&bad) > 0);
    }
    {
        fin_owner_case_t bad = f;
        bad.owner_kind = MOQ_REQ_NONE;
        MOQ_TEST_CHECK(fin_owner_problems(&bad) > 0);
    }
    /* Each required hook is individually mandatory. */
    {
        fin_owner_case_t bad = f;
        bad.hooks.normalize_event = NULL;
        MOQ_TEST_CHECK(fin_owner_problems(&bad) > 0);
    }
    {
        fin_owner_case_t bad = f;
        bad.hooks.normalize_action = NULL;
        MOQ_TEST_CHECK(fin_owner_problems(&bad) > 0);
    }
    {
        fin_owner_case_t bad = f;
        bad.hooks.capture = NULL;
        MOQ_TEST_CHECK(fin_owner_problems(&bad) > 0);
    }
    {
        fin_owner_case_t bad = f;
        bad.hooks.check = NULL;
        MOQ_TEST_CHECK(fin_owner_problems(&bad) > 0);
    }
    {
        fin_owner_case_t bad = f;
        bad.hooks.ctx = NULL;
        MOQ_TEST_CHECK(fin_owner_problems(&bad) > 0);
    }
    /* An event kind the family does not describe fails normalization. */
    {
        txs_norm_vec_t v;
        txs_norm_init(&v);
        moq_event_t ev;
        memset(&ev, 0, sizeof(ev));
        ev.kind = MOQ_EVENT_GOAWAY;
        txs_quiet = 1;
        MOQ_TEST_CHECK(!pub_norm_event(&ev, &pub_ctx, &v));
        moq_action_t a;
        memset(&a, 0, sizeof(a));
        a.kind = MOQ_ACTION_CLOSE_SESSION;
        MOQ_TEST_CHECK(!pub_norm_action(&a, &pub_ctx, &v));
        txs_quiet = 0;
        MOQ_TEST_CHECK_EQ_SIZE(v.count, 0);
        txs_norm_free(&v);
    }
    /* The other two classes validate on their own terms; an empty descriptor
     * of either is rejected rather than silently runnable. */
    {
        fin_nondest_case_t nd;
        memset(&nd, 0, sizeof(nd));
        MOQ_TEST_CHECK(fin_nondest_problems(&nd) > 0);
        fin_bridge_case_t br;
        memset(&br, 0, sizeof(br));
        MOQ_TEST_CHECK(fin_bridge_problems(&br) > 0);
    }

    /* The scratch normalization applies only to a drained event queue, so a
     * real scratch mutation behind a queued event stays detectable. */
    {
        txs_snapshot_t snap;
        memset(&snap, 0, sizeof(snap));
        snap.event_depth = 0;
        snap.event_scratch_len = 21;
        expect_after_call_prepare(&snap);
        MOQ_TEST_CHECK_EQ_SIZE(snap.event_scratch_len, 0);
    }
    {
        txs_snapshot_t snap;
        memset(&snap, 0, sizeof(snap));
        snap.event_depth = 1;
        snap.event_scratch_len = 21;
        expect_after_call_prepare(&snap);
        MOQ_TEST_CHECK_EQ_SIZE(snap.event_scratch_len, 21);
    }

    /* An out-of-range registry slot yields a loud incomparable record rather
     * than an out-of-bounds read. */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0x900);
        moq_request_endpoint_t ep;
        memset(&ep, 0, sizeof(ep));
        ep.kind = MOQ_REQ_PUBLISH;
        ep.slot = (int32_t)s->pub_cap + 3;
        ep.has_stream_ref = true;
        ep.stream_ref = ref;
        request_registry_insert_by_streamref(s, ref, ep);
        txs_quiet = 1;
        txs_owner_t o = txs_owner_capture(s, ref);
        txs_quiet = 0;
        MOQ_TEST_CHECK(o.invalid);
        MOQ_TEST_CHECK(o.kind < 0);
        /* Two identical indeclared records must never compare equal. */
        txs_quiet = 1;
        MOQ_TEST_CHECK(txs_owner_equals(&o, &o, "self") != 0);
        txs_quiet = 0;
        request_registry_remove_by_streamref(s, ref);

        /* A recognized kind whose pool is empty must still be bounds-checked:
         * a zero capacity may never wave a slot through to an indexed read. */
        moq_stream_ref_t zref = moq_stream_ref_from_u64(0x901);
        moq_request_endpoint_t zep;
        memset(&zep, 0, sizeof(zep));
        zep.kind = MOQ_REQ_SUBSCRIBE_TRACKS;
        zep.slot = 0;
        zep.has_stream_ref = true;
        zep.stream_ref = zref;
        size_t saved_cap = s->track_sub_cap;
        s->track_sub_cap = 0;
        request_registry_insert_by_streamref(s, zref, zep);
        txs_quiet = 1;
        txs_owner_t zo = txs_owner_capture(s, zref);
        txs_quiet = 0;
        MOQ_TEST_CHECK(zo.invalid);
        request_registry_remove_by_streamref(s, zref);
        s->track_sub_cap = saved_cap;

        /* The namespace-subscription index is bounds-checked on the same
         * terms as the request registry. */
        moq_stream_ref_t nref = moq_stream_ref_from_u64(0x902);
        moq_index_insert(s->idx_ns_by_ref, s->idx_ns_mask, nref._v,
                         (int)s->ns_sub_cap + 2);
        txs_quiet = 1;
        txs_owner_t no = txs_owner_capture(s, nref);
        txs_quiet = 0;
        MOQ_TEST_CHECK(no.invalid);
        moq_index_remove(s->idx_ns_by_ref, s->idx_ns_mask, nref._v);

        moq_session_destroy(s);
    }
}

int main(void)
{
    test_harness();
    check_image_fields();
    check_owner_metadata();
    test_publish();
    test_fetch();
    test_pending_join();
    test_publish_namespace();
    test_subscribe_tracks();
    test_subscribe_namespace();
    test_track_status();
    test_subscribe_control();
    test_route_exclusions();
    test_precommit_rejects();
    test_axis3_fin_terminals();
    test_ns_sub_request_error();
    test_axis3_durable_terminals();
    test_t5_receiver_teardown();
    failures += txs_selfcheck_img_states();
    test_axis4_announce_torn_down_retirement();
    test_axis4_announce_torn_down_blocked_retry();
    test_ns_sub_local_teardown();
    test_no_owner_admission_matrix();
    if (failures) { printf("FAILURES: %d\n", failures); return 1; }
    MOQ_TEST_PASS("d18_fin_handoff");
    return 0;
}
