/*
 * Draft-18 Joining FETCH (§10.12.2) + Next Group Start filter (§5.1.2, 0x1).
 * The FETCH codec, public fetch cfg, inbound range-calc/event, and outbound
 * mapping were already built (and exercised by the draft-16 joining tests); this
 * slice unblocks them for draft-18 (the encode op + the inbound gate) and enforces
 * the §10.12.2 Forward-State-1 MUST. Covers the D18 joining wire codec, SimPair
 * end-to-end relative/absolute joining (computed Start/End per §10.12.2.1), the
 * Forward-State-0 → INVALID_RANGE reject, and Next Group Start surfacing.
 */
#include <moq/sim.h>
#include <moq/control_d18.h>
#include "test_support.h"
#include "test_session_support.h"
#include "../support/failpoint.h"
#include "../support/txn_snapshot.h"
#include "../../core/src/session/session_internal.h"

static int failures = 0;

static const moq_bytes_t k_live[1] = { { (const uint8_t *)"live", 4 } };
static const moq_namespace_t k_ns = { (moq_bytes_t *)k_live, 1 };

static moq_simpair_t *make_pair(void)
{
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = moq_alloc_default();
    cfg.version = MOQ_VERSION_DRAFT_18;
    cfg.client_send_request_capacity = true; cfg.client_initial_request_capacity = 10;
    cfg.server_send_request_capacity = true; cfg.server_initial_request_capacity = 10;
    moq_simpair_t *sp = NULL;
    if (moq_simpair_create(&cfg, &sp) < 0) return NULL;
    return sp;
}

/* Establish a LARGEST_OBJECT subscription; the server accepts communicating a
 * largest location (lg, lo). `forward` sets the subscriber's Forward State.
 * Returns the client + server subscription handles. */
static bool setup_largest_sub(moq_simpair_t *sp, bool forward,
                              uint64_t lg, uint64_t lo,
                              moq_subscription_t *out_client,
                              moq_subscription_t *out_server)
{
    moq_session_t *client = moq_simpair_client(sp);
    moq_session_t *server = moq_simpair_server(sp);
    if (moq_simpair_start(sp) < 0) return false;
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    if (client->state != MOQ_SESS_ESTABLISHED ||
        server->state != MOQ_SESS_ESTABLISHED)
        return false;

    moq_subscribe_cfg_t sub; moq_subscribe_cfg_init(&sub);
    sub.track_namespace = k_ns; sub.track_name = MOQ_BYTES_LITERAL("v");
    sub.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    sub.has_forward = true; sub.forward = forward;
    moq_subscription_t ch;
    if (moq_session_subscribe(client, &sub, 1, &ch) != MOQ_OK) return false;
    moq_simpair_run_until_quiescent(sp, 16, NULL);

    moq_subscription_t sh = MOQ_SUBSCRIPTION_INVALID; bool got = false;
    moq_event_t ev;
    while (moq_session_poll_events(server, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) { sh = ev.u.subscribe_request.sub; got = true; }
        moq_event_cleanup(&ev);
    }
    if (!got) return false;

    moq_accept_subscribe_cfg_t acc; moq_accept_subscribe_cfg_init(&acc);
    acc.has_track_alias = true; acc.track_alias = 7;
    acc.has_largest = true; acc.largest_group = lg; acc.largest_object = lo;
    if (moq_session_accept_subscribe(server, sh, &acc, moq_simpair_now_us(sp)) != MOQ_OK)
        return false;
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    bool ok = false;
    while (moq_session_poll_events(client, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_OK) ok = true;
        moq_event_cleanup(&ev);
    }
    if (!ok) return false;
    *out_client = ch; *out_server = sh;
    return true;
}

/* --- Server-side single-session harness for buffered pending joins -------- *
 * The public fetch API blocks sending a joining fetch from a not-yet-OK'd
 * subscription (it requires has_largest), so the pending-join path is exercised by
 * feeding a raw inbound SUBSCRIBE (left pending) then a raw inbound Joining FETCH. */
static moq_session_t *make_server_alloc(const moq_alloc_t *alloc)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), alloc, MOQ_PERSPECTIVE_SERVER);
    cfg.version = MOQ_VERSION_DRAFT_18;
    moq_session_t *s = NULL;
    if (moq_session_create(&cfg, 0, &s) < 0) return NULL;
    if (moq_session_start(s, 0) < 0) { moq_session_destroy(s); return NULL; }
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
    uint8_t setup[16]; moq_buf_writer_t w;
    moq_buf_writer_init(&w, setup, sizeof(setup));
    moq_d18_encode_setup(&w);
    moq_session_on_control_bytes(s, setup, moq_buf_writer_offset(&w), 0);
    moq_event_t e;
    while (moq_session_poll_events(s, &e, 1) > 0) moq_event_cleanup(&e);
    while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
    return s;
}

static moq_session_t *make_server(void)
{
    return make_server_alloc(moq_alloc_default());
}

/* Feed an inbound LARGEST_OBJECT SUBSCRIBE (forward as given), leave it pending,
 * and return the responder subscription handle. */
static moq_subscription_t feed_pending_subscribe(moq_session_t *s, moq_stream_ref_t ref,
                                         uint64_t req_id, bool forward)
{
    moq_d18_msg_params_t mp; memset(&mp, 0, sizeof(mp));
    mp.has_filter = true; mp.filter_type = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    mp.has_forward = true; mp.forward = forward ? 1 : 0;
    /* A unique per-request track name avoids duplicate-track dedup when a single
     * server harness receives several SUBSCRIBEs. */
    uint8_t nbuf[8]; nbuf[0] = 'v'; nbuf[1] = (uint8_t)('0' + (req_id & 7));
    moq_bytes_t name = { nbuf, 2 };
    uint8_t m[160]; moq_buf_writer_t w; moq_buf_writer_init(&w, m, sizeof(m));
    moq_d18_encode_subscribe(&w, req_id, &k_ns, name, &mp);
    moq_session_on_bidi_stream_bytes(s, ref, m, moq_buf_writer_offset(&w), false, 1);
    moq_subscription_t h = MOQ_SUBSCRIPTION_INVALID; moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) h = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
    }
    return h;
}

/* Encode a raw inbound Joining FETCH (optionally carrying one USE_VALUE token). */
static size_t make_join_fetch(uint8_t *buf, size_t cap, uint64_t req_id,
                              uint64_t join_req_id, uint64_t ft, uint64_t jstart,
                              bool with_token)
{
    moq_d18_fetch_t f; memset(&f, 0, sizeof(f));
    f.request_id = req_id; f.fetch_type = ft;
    f.joining_request_id = join_req_id; f.joining_start = jstart;
    if (with_token) {
        f.params.auth_token_count = 1;
        f.params.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_USE_VALUE;
        f.params.auth_tokens[0].token_type = 7;
        f.params.auth_tokens[0].token_value = MOQ_BYTES_LITERAL("jointok");
    }
    moq_buf_writer_t w; moq_buf_writer_init(&w, buf, cap);
    moq_d18_encode_fetch(&w, &f);
    return moq_buf_writer_offset(&w);
}

/* A raw inbound Joining FETCH carrying two USE_VALUE tokens of distinct
 * lengths, so a copy failure can be aimed at the second one. */
static size_t make_join_fetch_two_tokens(uint8_t *buf, size_t cap,
                                         uint64_t req_id, uint64_t join_req_id,
                                         uint64_t ft, uint64_t jstart)
{
    moq_d18_fetch_t f; memset(&f, 0, sizeof(f));
    f.request_id = req_id; f.fetch_type = ft;
    f.joining_request_id = join_req_id; f.joining_start = jstart;
    f.params.auth_token_count = 2;
    f.params.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_USE_VALUE;
    f.params.auth_tokens[0].token_type = 7;
    f.params.auth_tokens[0].token_value = MOQ_BYTES_LITERAL("jointok");
    f.params.auth_tokens[1].alias_type = MOQ_AUTH_TOKEN_USE_VALUE;
    f.params.auth_tokens[1].token_type = 9;
    f.params.auth_tokens[1].token_value = MOQ_BYTES_LITERAL("second-join-token");
    moq_buf_writer_t w; moq_buf_writer_init(&w, buf, cap);
    moq_d18_encode_fetch(&w, &f);
    return moq_buf_writer_offset(&w);
}

/* Count PENDING_JOIN fetch entries (white-box). */
static int count_pending_joins(moq_session_t *s)
{
    int n = 0;
    for (size_t i = 0; i < s->fetch_cap; i++)
        if (s->fetches[i].state == MOQ_FETCH_PENDING_JOIN) n++;
    return n;
}

/* Drain actions; true iff a REQUEST_ERROR(error_code) was queued on `ref`. */
static bool saw_request_error(moq_session_t *s, moq_stream_ref_t ref,
                              uint64_t error_code)
{
    bool got = false; moq_action_t act;
    while (moq_session_poll_actions(s, &act, 1) > 0) {
        if (act.kind == MOQ_ACTION_SEND_BIDI_STREAM &&
            act.u.send_bidi_stream.stream_ref._v == ref._v) {
            moq_buf_reader_t rr;
            moq_buf_reader_init(&rr, act.u.send_bidi_stream.data,
                                act.u.send_bidi_stream.len);
            moq_control_envelope_t env;
            if (moq_d18_decode_envelope(&rr, &env) == MOQ_OK &&
                env.msg_type == MOQ_D18_REQUEST_ERROR) {
                moq_d18_request_error_t er;
                if (moq_d18_decode_request_error(env.payload, env.payload_len,
                                                 &er) == MOQ_OK &&
                    er.error_code == error_code)
                    got = true;
            }
        }
        moq_action_cleanup(&act);
    }
    return got;
}

/* Occupy exactly one action-queue slot with a throwaway accepted subscription,
 * left unpolled so the slot stays held -- used to build action-pressure cases. */
static bool occupy_one_action(moq_session_t *s, moq_stream_ref_t ref, uint64_t req_id)
{
    moq_subscription_t h = feed_pending_subscribe(s, ref, req_id, true);
    if (h._opaque == MOQ_SUBSCRIPTION_INVALID._opaque) return false;
    moq_accept_subscribe_cfg_t ac; moq_accept_subscribe_cfg_init(&ac);
    ac.has_track_alias = true; ac.track_alias = req_id + 100;
    ac.has_largest = true; ac.largest_group = 1; ac.largest_object = 0;
    return moq_session_accept_subscribe(s, h, &ac, 1) == MOQ_OK;
}

/* -- Failpoint fixtures and hooks for the buffered-join operation --- */

/* Occupancy across all seven request/subscription pools; the request-id
 * sequence lives in profile-private state, so its non-advancement is
 * proven behaviorally by the same-id re-delivery admitting cleanly. */
static int jf_registry_busy(const moq_session_t *s)
{
    int n = 0;
    for (size_t i = 0; i < s->sub_cap; i++)
        if (s->subs[i].state != MOQ_SUB_FREE) n++;
    for (size_t i = 0; i < s->fetch_cap; i++)
        if (s->fetches[i].state != MOQ_FETCH_FREE) n++;
    for (size_t i = 0; i < s->ann_cap; i++)
        if (s->announcements[i].state != MOQ_ANN_FREE) n++;
    for (size_t i = 0; i < s->ts_cap; i++)
        if (s->track_statuses[i].state != MOQ_TS_FREE) n++;
    for (size_t i = 0; i < s->pub_cap; i++)
        if (s->publishes[i].state != MOQ_PUB_FREE) n++;
    for (size_t i = 0; i < s->ns_sub_cap; i++)
        if (s->ns_subs[i].state != MOQ_NS_SUB_FREE) n++;
    for (size_t i = 0; i < s->track_sub_cap; i++)
        if (s->track_subs[i].state != MOQ_TRACK_SUB_FREE) n++;
    return n;
}

/* Operation-specific snapshot per the mutation inventory: the pending-join
 * table depth, request-pool occupancy, the staging carrier's buffered
 * input and latched FIN (resolved by the operation's stream ref; zero and
 * false when no carrier owns it), and the outbound send-buffer cursor. */
typedef struct jf_hook_state {
    int    pending_joins;
    int    registry_busy;
    size_t stage_recv_len;
    bool   stage_recv_fin;
    size_t send_len;
} jf_hook_state_t;

typedef struct jf_hook_ctx {
    uint64_t sub_opaque;   /* the referenced subscription's handle, for
                            * run-portable normalization of joining events */
    moq_stream_ref_t fref; /* the request bidi this operation rides */
} jf_hook_ctx_t;

static void jf_hook_capture(const moq_session_t *s, void *vctx, void *vst)
{
    const jf_hook_ctx_t *ctx = (const jf_hook_ctx_t *)vctx;
    jf_hook_state_t *st = (jf_hook_state_t *)vst;
    memset(st, 0, sizeof(*st));
    st->pending_joins = count_pending_joins((moq_session_t *)(uintptr_t)s);
    st->registry_busy = jf_registry_busy(s);
    /* The buffered input and the latched FIN follow the CURRENT owner of
     * the operation's bidi: the staging SUBSCRIPTION before handoff, the
     * destination FETCH after it -- a successful buffering stores the FIN
     * on the fetch entry, and losing it there must be visible here. */
    moq_request_endpoint_t ep =
        request_registry_find_by_streamref(s, ctx->fref);
    if (ep.kind == MOQ_REQ_SUBSCRIPTION) {
        st->stage_recv_len = s->subs[ep.slot].req_recv_len;
        st->stage_recv_fin = s->subs[ep.slot].req_recv_fin;
    } else if (ep.kind == MOQ_REQ_FETCH) {
        st->stage_recv_len = s->fetches[ep.slot].req_recv_len;
        st->stage_recv_fin = s->fetches[ep.slot].req_recv_fin;
    }
    st->send_len = s->send_len;
}

static int jf_hook_check(const moq_session_t *s, void *vctx, const void *vst)
{
    const jf_hook_state_t *want = (const jf_hook_state_t *)vst;
    jf_hook_state_t now;
    jf_hook_capture(s, vctx, &now);
    int bad = 0;
#define JF_HF(f, fmt) do { \
        if (now.f != want->f) { \
            fprintf(stderr, "TXN jf-hook: " #f " " fmt ", expected " fmt \
                    "\n", now.f, want->f); \
            bad++; \
        } \
    } while (0)
    JF_HF(pending_joins, "%d");
    JF_HF(registry_busy, "%d");
    JF_HF(stage_recv_len, "%zu");
    JF_HF(stage_recv_fin, "%d");
    JF_HF(send_len, "%zu");
#undef JF_HF
    return bad;
}

/* Normalize one event: for FETCH_REQUEST, EVERY semantic field -- the
 * joining linkage (run-portable: "joins the fixture's subscription", since
 * raw handles carry a per-session tag), the resolved range, priority,
 * group order, and each token's type and value bytes. An event kind
 * without a normalizer is a test failure. */
static bool jf_norm_event(const moq_event_t *ev, void *vctx,
                          txs_norm_vec_t *out)
{
    const jf_hook_ctx_t *ctx = (const jf_hook_ctx_t *)vctx;
    txs_img_t img;
    txs_img_init(&img);
    switch (ev->kind) {
    case MOQ_EVENT_FETCH_REQUEST: {
        const moq_fetch_request_event_t *fr = &ev->u.fetch_request;
        /* The fetch handle's pool, slot and generation are run-portable;
         * only its session tag is not. */
        txs_img_u64(&img, moq_handle_pool_tag(fr->fetch._opaque));
        txs_img_u64(&img, moq_handle_slot(fr->fetch._opaque));
        txs_img_u64(&img, moq_handle_generation(fr->fetch._opaque));
        txs_img_u64(&img, fr->joining_sub._opaque == ctx->sub_opaque);
        txs_img_u64(&img, (uint64_t)fr->track_namespace.count);
        for (size_t t = 0; t < fr->track_namespace.count; t++)
            txs_img_bytes(&img, fr->track_namespace.parts[t].data,
                          fr->track_namespace.parts[t].len);
        txs_img_bytes(&img, fr->track_name.data, fr->track_name.len);
        txs_img_u64(&img, fr->start_group);
        txs_img_u64(&img, fr->start_object);
        txs_img_u64(&img, fr->end_group);
        txs_img_u64(&img, fr->end_object);
        txs_img_u64(&img, fr->subscriber_priority);
        txs_img_u64(&img, (uint64_t)fr->group_order);
        txs_img_u64(&img, (uint64_t)fr->token_count);
        for (size_t t = 0; t < fr->token_count; t++) {
            txs_img_u64(&img, fr->tokens[t].token_type);
            txs_img_bytes(&img, fr->tokens[t].token_value.data,
                          fr->tokens[t].token_value.len);
        }
        break;
    }
    default:
        fprintf(stderr, "TXN jf-norm: unnormalized event kind %d\n",
                (int)ev->kind);
        return false;
    }
    return txs_norm_append_img(out, (uint64_t)ev->kind, &img);
}

/* Normalize one action: kind, stream ref, payload bytes. */
static bool jf_norm_action(const moq_action_t *a, void *vctx,
                           txs_norm_vec_t *out)
{
    (void)vctx;
    txs_img_t img;
    txs_img_init(&img);
    switch (a->kind) {
    case MOQ_ACTION_SEND_BIDI_STREAM:
        txs_img_u64(&img, a->u.send_bidi_stream.stream_ref._v);
        txs_img_u64(&img, a->u.send_bidi_stream.fin);
        txs_img_bytes(&img, a->u.send_bidi_stream.data,
                      a->u.send_bidi_stream.len);
        break;
    case MOQ_ACTION_OPEN_BIDI_STREAM:
        txs_img_u64(&img, a->u.open_bidi_stream.stream_ref._v);
        txs_img_u64(&img, a->u.open_bidi_stream.fin);
        txs_img_bytes(&img, a->u.open_bidi_stream.data,
                      a->u.open_bidi_stream.len);
        break;
    case MOQ_ACTION_CLOSE_BIDI_STREAM:
        txs_img_u64(&img, a->u.close_bidi_stream.stream_ref._v);
        break;
    case MOQ_ACTION_SEND_CONTROL:
        txs_img_bytes(&img, a->u.send_control.data, a->u.send_control.len);
        break;
    default:
        fprintf(stderr, "TXN jf-norm: unnormalized action kind %d\n",
                (int)a->kind);
        return false;
    }
    return txs_norm_append_img(out, 0x1000u + (uint64_t)a->kind, &img);
}

/* The registered hook set for this operation. The hook state holds no
 * same-run pointer identities, so one comparator serves both within-run
 * and cross-run checks. */
static txs_op_hooks_t jf_make_hooks(jf_hook_ctx_t *hctx)
{
    txs_op_hooks_t h;
    memset(&h, 0, sizeof(h));
    h.ctx = hctx;
    h.capture = jf_hook_capture;
    h.check = jf_hook_check;
    h.check_values = jf_hook_check;
    h.normalize_event = jf_norm_event;
    h.normalize_action = jf_norm_action;
    return h;
}

/* Accept the referenced subscription and normalize everything produced,
 * events then actions, in poll order, through the registered
 * normalizers. */
static int jf_accept_and_collect(moq_session_t *s, moq_subscription_t ssub,
                                 const txs_op_hooks_t *h, txs_norm_vec_t *out,
                                 int *failures_out)
{
    int bad = 0;
    moq_accept_subscribe_cfg_t ac; moq_accept_subscribe_cfg_init(&ac);
    ac.has_track_alias = true; ac.track_alias = 7;
    ac.has_largest = true; ac.largest_group = 10; ac.largest_object = 5;
    if (moq_session_accept_subscribe(s, ssub, &ac, 1) != MOQ_OK) {
        fprintf(stderr, "TXN jf: accept_subscribe failed\n");
        (*failures_out)++;
    }
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        if (!h->normalize_event(&ev, h->ctx, out)) bad++;
        moq_event_cleanup(&ev);
    }
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) {
        if (!h->normalize_action(&a, h->ctx, out)) bad++;
        moq_action_cleanup(&a);
    }
    return bad;
}

/* Tear down and require the allocator's terminal facts. */
static int jf_fx_teardown(fp_alloc_state_t *fs, moq_session_t *s,
                          const char *op)
{
    int bad = 0;
    moq_event_t ev; moq_action_t a;
    while (moq_session_poll_events(s, &ev, 1) > 0) moq_event_cleanup(&ev);
    while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
    bad += fp_sticky_clean(fs, op);
    moq_session_destroy(s);
    if (fs->balance != 0 || fs->live_bytes != 0 || fs->table_len != 0) {
        fprintf(stderr, "FAILPOINT %s: destroy left balance %lld, live %lld, "
                "table %zu\n", op, (long long)fs->balance,
                (long long)fs->live_bytes, fs->table_len);
        bad++;
    }
    return bad;
}

int main(void)
{
    /* == Codec: D18 joining FETCH round-trip (types 2 + 3) ============= */
    {
        for (uint64_t ft = 2; ft <= 3; ft++) {
            moq_d18_fetch_t f; memset(&f, 0, sizeof(f));
            f.request_id = 4; f.fetch_type = ft;
            f.joining_request_id = 2; f.joining_start = 3;
            uint8_t buf[128]; moq_buf_writer_t w;
            moq_buf_writer_init(&w, buf, sizeof(buf));
            MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_fetch(&w, &f), (int)MOQ_OK);
            moq_buf_reader_t r;
            moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
            moq_control_envelope_t env;
            MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_envelope(&r, &env), (int)MOQ_OK);
            MOQ_TEST_CHECK_EQ_U64(env.msg_type, MOQ_D18_FETCH);
            moq_bytes_t parts[4]; moq_d18_fetch_t out;
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_d18_decode_fetch(env.payload, env.payload_len, parts, 4, &out),
                (int)MOQ_OK);
            MOQ_TEST_CHECK_EQ_U64(out.fetch_type, ft);
            MOQ_TEST_CHECK_EQ_U64(out.joining_request_id, 2);
            MOQ_TEST_CHECK_EQ_U64(out.joining_start, 3);
        }
    }

    /* == SimPair e2e: relative + absolute joining fetch =============== *
     *  largest = {10, 5} ⇒ End = {10, 6}. Relative joining_start 3 ⇒ Start {7,0};
     *  absolute joining_start 4 ⇒ Start {4,0} (§10.12.2.1). */
    struct { bool relative; uint64_t jstart; uint64_t want_start_group; } jc[] = {
        { true,  3, 7 },
        { false, 4, 4 },
    };
    for (size_t i = 0; i < 2; i++) {
        moq_simpair_t *sp = make_pair();
        MOQ_TEST_CHECK(sp != NULL);
        moq_session_t *client = moq_simpair_client(sp);
        moq_session_t *server = moq_simpair_server(sp);
        moq_subscription_t csub, ssub;
        MOQ_TEST_CHECK(setup_largest_sub(sp, true, 10, 5, &csub, &ssub));

        moq_fetch_cfg_t fc; moq_fetch_cfg_init(&fc);
        fc.is_joining = true; fc.joining_relative = jc[i].relative;
        fc.joining_sub = csub; fc.joining_start = jc[i].jstart;
        moq_fetch_t fh;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_fetch(client, &fc, moq_simpair_now_us(sp), &fh), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        bool got = false; moq_event_t ev;
        while (moq_session_poll_events(server, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_REQUEST) {
                moq_fetch_request_event_t *fr = &ev.u.fetch_request;
                got = fr->joining_sub._opaque == ssub._opaque &&
                      fr->start_group == jc[i].want_start_group &&
                      fr->start_object == 0 &&
                      fr->end_group == 10 && fr->end_object == 6;
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got);
        MOQ_TEST_CHECK_EQ_INT((int)server->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT((int)client->state, (int)MOQ_SESS_ESTABLISHED);
        moq_simpair_destroy(sp);
    }

    /* == Forward State 0 ⇒ INVALID_RANGE (§10.12.2) =================== */
    {
        moq_simpair_t *sp = make_pair();
        MOQ_TEST_CHECK(sp != NULL);
        moq_session_t *client = moq_simpair_client(sp);
        moq_session_t *server = moq_simpair_server(sp);
        moq_subscription_t csub, ssub;
        MOQ_TEST_CHECK(setup_largest_sub(sp, false /* forward=0 */, 10, 5, &csub, &ssub));

        moq_fetch_cfg_t fc; moq_fetch_cfg_init(&fc);
        fc.is_joining = true; fc.joining_relative = true;
        fc.joining_sub = csub; fc.joining_start = 1;
        moq_fetch_t fh;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_fetch(client, &fc, moq_simpair_now_us(sp), &fh), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        /* server must NOT surface a fetch request; client gets INVALID_RANGE */
        bool srv_req = false; moq_event_t ev;
        while (moq_session_poll_events(server, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_REQUEST) srv_req = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(!srv_req);
        bool got_err = false;
        while (moq_session_poll_events(client, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_ERROR)
                got_err = ev.u.fetch_error.error_code == MOQ_REQUEST_ERROR_INVALID_RANGE;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got_err);
        MOQ_TEST_CHECK_EQ_INT((int)server->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT((int)client->state, (int)MOQ_SESS_ESTABLISHED);
        moq_simpair_destroy(sp);
    }

    /* == Next Group Start (filter 0x1) surfaces to the publisher ====== */
    {
        moq_simpair_t *sp = make_pair();
        MOQ_TEST_CHECK(sp != NULL);
        moq_session_t *client = moq_simpair_client(sp);
        moq_session_t *server = moq_simpair_server(sp);
        moq_simpair_start(sp);
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        moq_subscribe_cfg_t sub; moq_subscribe_cfg_init(&sub);
        sub.track_namespace = k_ns; sub.track_name = MOQ_BYTES_LITERAL("v");
        sub.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t ch;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscribe(client, &sub, 1, &ch), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        bool got = false; moq_event_t ev;
        while (moq_session_poll_events(server, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST)
                got = ev.u.subscribe_request.filter == MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got);
        MOQ_TEST_CHECK_EQ_INT((int)server->state, (int)MOQ_SESS_ESTABLISHED);
        moq_simpair_destroy(sp);
    }

    /* == Pending-subscribe Joining FETCH: buffer, then release on accept === */
    {
        moq_session_t *s = make_server();
        MOQ_TEST_CHECK(s != NULL);
        moq_subscription_t ssub = feed_pending_subscribe(s, moq_stream_ref_from_u64(0x7001),
                                                 0, true);
        uint8_t fb[160];
        size_t fn = make_join_fetch(fb, sizeof(fb), 2, 0, 2 /*relative*/, 3, false);
        moq_stream_ref_t fref = moq_stream_ref_from_u64(0x7002);
        moq_session_on_bidi_stream_bytes(s, fref, fb, fn, false, 1);
        /* Buffered: no event, no error on the fetch bidi, one PENDING_JOIN entry. */
        moq_event_t ev; bool any_ev = false;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_REQUEST) any_ev = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(!any_ev);
        MOQ_TEST_CHECK_EQ_INT(count_pending_joins(s), 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_action_t a; while (moq_session_poll_actions(s, &a, 1) > 0)
            moq_action_cleanup(&a);   /* discard SUBSCRIBE_OK-less staging actions */
        /* Accept the subscription (largest {10,5}) -> release the join. */
        moq_accept_subscribe_cfg_t ac; moq_accept_subscribe_cfg_init(&ac);
        ac.has_track_alias = true; ac.track_alias = 7;
        ac.has_largest = true; ac.largest_group = 10; ac.largest_object = 5;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_subscribe(s, ssub, &ac, 1),
                              (int)MOQ_OK);
        bool got = false;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_REQUEST) {
                moq_fetch_request_event_t *fr = &ev.u.fetch_request;
                got = fr->joining_sub._opaque == ssub._opaque &&
                      fr->start_group == 7 && fr->end_group == 10 &&
                      fr->end_object == 6;
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got);
        MOQ_TEST_CHECK_EQ_INT(count_pending_joins(s), 0);
        moq_session_destroy(s);
    }

    /* == Accept without Largest -> the buffered join is rejected INVALID_RANGE == */
    {
        moq_session_t *s = make_server();
        moq_subscription_t ssub = feed_pending_subscribe(s, moq_stream_ref_from_u64(0x7011),
                                                 0, true);
        uint8_t fb[160];
        size_t fn = make_join_fetch(fb, sizeof(fb), 2, 0, 2, 3, false);
        moq_stream_ref_t fref = moq_stream_ref_from_u64(0x7012);
        moq_session_on_bidi_stream_bytes(s, fref, fb, fn, false, 1);
        moq_action_t a; while (moq_session_poll_actions(s, &a, 1) > 0)
            moq_action_cleanup(&a);
        moq_accept_subscribe_cfg_t ac; moq_accept_subscribe_cfg_init(&ac);
        ac.has_track_alias = true; ac.track_alias = 7;   /* no largest */
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_subscribe(s, ssub, &ac, 1),
                              (int)MOQ_OK);
        moq_event_t ev; bool any_ev = false;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_REQUEST) any_ev = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(!any_ev);
        MOQ_TEST_CHECK(saw_request_error(s, fref, MOQ_REQUEST_ERROR_INVALID_RANGE));
        MOQ_TEST_CHECK_EQ_INT(count_pending_joins(s), 0);
        moq_session_destroy(s);
    }

    /* == Reject the subscription -> the buffered join is cleaned up ===== */
    {
        moq_session_t *s = make_server();
        moq_subscription_t ssub = feed_pending_subscribe(s, moq_stream_ref_from_u64(0x7021),
                                                 0, true);
        uint8_t fb[160];
        size_t fn = make_join_fetch(fb, sizeof(fb), 2, 0, 2, 3, false);
        moq_stream_ref_t fref = moq_stream_ref_from_u64(0x7022);
        moq_session_on_bidi_stream_bytes(s, fref, fb, fn, false, 1);
        moq_action_t a; while (moq_session_poll_actions(s, &a, 1) > 0)
            moq_action_cleanup(&a);
        moq_reject_subscribe_cfg_t rc; moq_reject_subscribe_cfg_init(&rc);
        rc.error_code = (moq_request_error_t)MOQ_REQUEST_ERROR_NOT_SUPPORTED;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_reject_subscribe(s, ssub, &rc, 1),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK(saw_request_error(s, fref,
                       MOQ_REQUEST_ERROR_INVALID_JOINING_REQUEST_ID));
        MOQ_TEST_CHECK_EQ_INT(count_pending_joins(s), 0);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Backpressure: a full event queue makes accept WOULD_BLOCK without
     *    losing the buffered join; retry after draining releases it. === */
    {
        moq_session_cfg_t cfg;
        moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), MOQ_PERSPECTIVE_SERVER);
        cfg.version = MOQ_VERSION_DRAFT_18;
        cfg.max_events = 1;
        moq_session_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_create(&cfg, 0, &s), (int)MOQ_OK);
        moq_session_start(s, 0);
        { moq_action_t a; while (moq_session_poll_actions(s,&a,1)>0) moq_action_cleanup(&a); }
        uint8_t setup[16]; moq_buf_writer_t sw; moq_buf_writer_init(&sw, setup, sizeof(setup));
        moq_d18_encode_setup(&sw);
        moq_session_on_control_bytes(s, setup, moq_buf_writer_offset(&sw), 0);
        { moq_event_t e; while (moq_session_poll_events(s,&e,1)>0) moq_event_cleanup(&e); }
        { moq_action_t a; while (moq_session_poll_actions(s,&a,1)>0) moq_action_cleanup(&a); }

        moq_subscription_t ssub = feed_pending_subscribe(s, moq_stream_ref_from_u64(0x7031),
                                                 0, true);   /* req 0; drains event */
        uint8_t fb[160];
        size_t fn = make_join_fetch(fb, sizeof(fb), 2, 0, 2, 3, false); /* fetch req 2 */
        moq_session_on_bidi_stream_bytes(s, moq_stream_ref_from_u64(0x7032), fb, fn, false, 1);
        { moq_action_t a; while (moq_session_poll_actions(s,&a,1)>0) moq_action_cleanup(&a); }
        MOQ_TEST_CHECK_EQ_INT(count_pending_joins(s), 1);
        /* Fill the 1-slot event queue with one undrained SUBSCRIBE_REQUEST (req 4). */
        moq_d18_msg_params_t mp; memset(&mp,0,sizeof(mp));
        mp.has_filter = true; mp.filter_type = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        mp.has_forward = true; mp.forward = 1;
        uint8_t sm[160]; moq_buf_writer_t mw; moq_buf_writer_init(&mw, sm, sizeof(sm));
        moq_d18_encode_subscribe(&mw, 4, &k_ns, MOQ_BYTES_LITERAL("w"), &mp);
        moq_session_on_bidi_stream_bytes(s, moq_stream_ref_from_u64(0x7034),
                                         sm, moq_buf_writer_offset(&mw), false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        /* Accept ssub: the release FETCH_REQUEST cannot fit -> WOULD_BLOCK. */
        moq_accept_subscribe_cfg_t ac; moq_accept_subscribe_cfg_init(&ac);
        ac.has_track_alias = true; ac.track_alias = 7;
        ac.has_largest = true; ac.largest_group = 10; ac.largest_object = 5;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_subscribe(s, ssub, &ac, 1),
                              (int)MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK_EQ_INT(count_pending_joins(s), 1);   /* not lost */
        /* Drain events, retry accept -> released. */
        { moq_event_t e; while (moq_session_poll_events(s,&e,1)>0) moq_event_cleanup(&e); }
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_subscribe(s, ssub, &ac, 1),
                              (int)MOQ_OK);
        bool got = false; moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_REQUEST) got = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got);
        MOQ_TEST_CHECK_EQ_INT(count_pending_joins(s), 0);
        moq_session_destroy(s);
    }

    /* == A buffered join's auth token survives to the released FETCH_REQUEST = */
    {
        moq_session_t *s = make_server();
        moq_subscription_t ssub = feed_pending_subscribe(s, moq_stream_ref_from_u64(0x7041),
                                                 0, true);
        uint8_t fb[160];
        size_t fn = make_join_fetch(fb, sizeof(fb), 2, 0, 2, 3, true /*token*/);
        moq_session_on_bidi_stream_bytes(s, moq_stream_ref_from_u64(0x7042), fb, fn, false, 1);
        moq_action_t a; while (moq_session_poll_actions(s, &a, 1) > 0)
            moq_action_cleanup(&a);
        moq_accept_subscribe_cfg_t ac; moq_accept_subscribe_cfg_init(&ac);
        ac.has_track_alias = true; ac.track_alias = 7;
        ac.has_largest = true; ac.largest_group = 10; ac.largest_object = 5;
        moq_session_accept_subscribe(s, ssub, &ac, 1);
        bool tok_ok = false; moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_REQUEST) {
                moq_fetch_request_event_t *fr = &ev.u.fetch_request;
                tok_ok = fr->token_count == 1 && fr->tokens &&
                         fr->tokens[0].token_type == 7 &&
                         fr->tokens[0].token_value.len == 7 &&
                         memcmp(fr->tokens[0].token_value.data, "jointok", 7) == 0;
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(tok_ok);
        moq_session_destroy(s);
    }

    /* == A buffered USE_VALUE token survives reuse of the request receive buffer
     *    by later inbound requests before the subscription is accepted. ===== */
    {
        moq_session_t *s = make_server();
        moq_subscription_t ssub = feed_pending_subscribe(s, moq_stream_ref_from_u64(0x7051),
                                                 0, true);
        uint8_t fb[160];
        size_t fn = make_join_fetch(fb, sizeof(fb), 2, 0, 2, 3, true /* token */);
        moq_session_on_bidi_stream_bytes(s, moq_stream_ref_from_u64(0x7052), fb, fn, false, 1);
        MOQ_TEST_CHECK_EQ_INT(count_pending_joins(s), 1);
        /* Churn the staging receive buffer: feed several more inbound requests whose
         * bytes (and their own auth tokens) reuse the freed staging slot's buffer. A
         * borrowed (un-copied) token value would now read this churn data. */
        for (uint64_t r = 0; r < 3; r++) {
            moq_d18_msg_params_t mp; memset(&mp, 0, sizeof(mp));
            mp.has_filter = true; mp.filter_type = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
            mp.has_forward = true; mp.forward = 1;
            mp.auth_token_count = 1;
            mp.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_USE_VALUE;
            mp.auth_tokens[0].token_type = 9;
            mp.auth_tokens[0].token_value = MOQ_BYTES_LITERAL("ZZZZZZZ");
            uint8_t sm[200]; moq_buf_writer_t mw; moq_buf_writer_init(&mw, sm, sizeof(sm));
            moq_d18_encode_subscribe(&mw, 4 + r * 2, &k_ns, MOQ_BYTES_LITERAL("churn"), &mp);
            moq_session_on_bidi_stream_bytes(s, moq_stream_ref_from_u64(0x7060 + r),
                                             sm, moq_buf_writer_offset(&mw), false, 1);
            moq_event_t cev; while (moq_session_poll_events(s, &cev, 1) > 0)
                moq_event_cleanup(&cev);
        }
        moq_action_t a; while (moq_session_poll_actions(s, &a, 1) > 0)
            moq_action_cleanup(&a);
        moq_accept_subscribe_cfg_t ac; moq_accept_subscribe_cfg_init(&ac);
        ac.has_track_alias = true; ac.track_alias = 7;
        ac.has_largest = true; ac.largest_group = 10; ac.largest_object = 5;
        moq_session_accept_subscribe(s, ssub, &ac, 1);
        bool tok_ok = false; moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_REQUEST) {
                moq_fetch_request_event_t *fr = &ev.u.fetch_request;
                tok_ok = fr->token_count == 1 && fr->tokens &&
                         fr->tokens[0].token_type == 7 &&
                         fr->tokens[0].token_value.len == 7 &&
                         memcmp(fr->tokens[0].token_value.data, "jointok", 7) == 0;
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(tok_ok);
        moq_session_destroy(s);
    }

    /* == accept-without-largest under action-queue pressure: WOULD_BLOCK leaves the
     *    pending join intact; the retry after draining rejects it (INVALID_RANGE). */
    {
        moq_session_cfg_t cfg;
        moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), MOQ_PERSPECTIVE_SERVER);
        cfg.version = MOQ_VERSION_DRAFT_18;
        cfg.max_actions = 2;
        moq_session_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_create(&cfg, 0, &s), (int)MOQ_OK);
        moq_session_start(s, 0);
        { moq_action_t a; while (moq_session_poll_actions(s,&a,1)>0) moq_action_cleanup(&a); }
        uint8_t setup[16]; moq_buf_writer_t sw; moq_buf_writer_init(&sw, setup, sizeof(setup));
        moq_d18_encode_setup(&sw);
        moq_session_on_control_bytes(s, setup, moq_buf_writer_offset(&sw), 0);
        { moq_event_t e; while (moq_session_poll_events(s,&e,1)>0) moq_event_cleanup(&e); }
        { moq_action_t a; while (moq_session_poll_actions(s,&a,1)>0) moq_action_cleanup(&a); }

        moq_subscription_t ssub = feed_pending_subscribe(s, moq_stream_ref_from_u64(0x7071), 0, true);
        uint8_t fb[160];
        size_t fn = make_join_fetch(fb, sizeof(fb), 2, 0, 2, 3, false);
        moq_stream_ref_t fref = moq_stream_ref_from_u64(0x7072);
        moq_session_on_bidi_stream_bytes(s, fref, fb, fn, false, 1);
        MOQ_TEST_CHECK_EQ_INT(count_pending_joins(s), 1);
        /* Hold one of the two action slots, then accept without largest: the join
         * must reject (1 action) alongside SUBSCRIBE_OK (1 action) -> no room. */
        MOQ_TEST_CHECK(occupy_one_action(s, moq_stream_ref_from_u64(0x7073), 4));
        moq_accept_subscribe_cfg_t ac; moq_accept_subscribe_cfg_init(&ac);
        ac.has_track_alias = true; ac.track_alias = 7;   /* no largest */
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_subscribe(s, ssub, &ac, 1),
                              (int)MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK_EQ_INT(count_pending_joins(s), 1);   /* intact */
        MOQ_TEST_CHECK_EQ_INT((int)s->subs[sub_resolve_handle(s, ssub)].state,
                              (int)MOQ_SUB_PENDING_PUBLISHER);
        /* Drain actions, retry -> succeeds, join rejected. */
        { moq_action_t a; while (moq_session_poll_actions(s,&a,1)>0) moq_action_cleanup(&a); }
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_subscribe(s, ssub, &ac, 1),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK(saw_request_error(s, fref, MOQ_REQUEST_ERROR_INVALID_RANGE));
        MOQ_TEST_CHECK_EQ_INT(count_pending_joins(s), 0);
        moq_session_destroy(s);
    }

    /* == reject-subscribe under action-queue pressure: WOULD_BLOCK leaves BOTH the
     *    subscription and the pending join intact; retry rejects both. ======= */
    {
        moq_session_cfg_t cfg;
        moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), MOQ_PERSPECTIVE_SERVER);
        cfg.version = MOQ_VERSION_DRAFT_18;
        cfg.max_actions = 2;
        moq_session_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_create(&cfg, 0, &s), (int)MOQ_OK);
        moq_session_start(s, 0);
        { moq_action_t a; while (moq_session_poll_actions(s,&a,1)>0) moq_action_cleanup(&a); }
        uint8_t setup[16]; moq_buf_writer_t sw; moq_buf_writer_init(&sw, setup, sizeof(setup));
        moq_d18_encode_setup(&sw);
        moq_session_on_control_bytes(s, setup, moq_buf_writer_offset(&sw), 0);
        { moq_event_t e; while (moq_session_poll_events(s,&e,1)>0) moq_event_cleanup(&e); }
        { moq_action_t a; while (moq_session_poll_actions(s,&a,1)>0) moq_action_cleanup(&a); }

        moq_subscription_t ssub = feed_pending_subscribe(s, moq_stream_ref_from_u64(0x7081), 0, true);
        uint8_t fb[160];
        size_t fn = make_join_fetch(fb, sizeof(fb), 2, 0, 2, 3, false);
        moq_stream_ref_t fref = moq_stream_ref_from_u64(0x7082);
        moq_session_on_bidi_stream_bytes(s, fref, fb, fn, false, 1);
        MOQ_TEST_CHECK(occupy_one_action(s, moq_stream_ref_from_u64(0x7083), 4));
        moq_reject_subscribe_cfg_t rc; moq_reject_subscribe_cfg_init(&rc);
        rc.error_code = (moq_request_error_t)MOQ_REQUEST_ERROR_NOT_SUPPORTED;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_reject_subscribe(s, ssub, &rc, 1),
                              (int)MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK_EQ_INT(count_pending_joins(s), 1);   /* join intact */
        MOQ_TEST_CHECK_EQ_INT((int)s->subs[sub_resolve_handle(s, ssub)].state,
                              (int)MOQ_SUB_PENDING_PUBLISHER);   /* sub intact */
        { moq_action_t a; while (moq_session_poll_actions(s,&a,1)>0) moq_action_cleanup(&a); }
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_reject_subscribe(s, ssub, &rc, 1),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK(saw_request_error(s, fref,
                       MOQ_REQUEST_ERROR_INVALID_JOINING_REQUEST_ID));
        MOQ_TEST_CHECK_EQ_INT(count_pending_joins(s), 0);
        moq_session_destroy(s);
    }

    /* == Multiple pending joins released by one accept (exact preflight). === */
    {
        moq_session_t *s = make_server();
        moq_subscription_t ssub = feed_pending_subscribe(s, moq_stream_ref_from_u64(0x7091), 0, true);
        uint8_t fb[160];
        size_t n1 = make_join_fetch(fb, sizeof(fb), 2, 0, 2 /*rel*/, 3, false);
        moq_session_on_bidi_stream_bytes(s, moq_stream_ref_from_u64(0x7092), fb, n1, false, 1);
        uint8_t fb2[160];
        size_t n2 = make_join_fetch(fb2, sizeof(fb2), 4, 0, 3 /*abs*/, 4, false);
        moq_session_on_bidi_stream_bytes(s, moq_stream_ref_from_u64(0x7093), fb2, n2, false, 1);
        MOQ_TEST_CHECK_EQ_INT(count_pending_joins(s), 2);
        moq_action_t a; while (moq_session_poll_actions(s, &a, 1) > 0)
            moq_action_cleanup(&a);
        moq_accept_subscribe_cfg_t ac; moq_accept_subscribe_cfg_init(&ac);
        ac.has_track_alias = true; ac.track_alias = 7;
        ac.has_largest = true; ac.largest_group = 10; ac.largest_object = 5;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_subscribe(s, ssub, &ac, 1),
                              (int)MOQ_OK);
        int n_req = 0; bool saw_rel = false, saw_abs = false; moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_REQUEST) {
                n_req++;
                if (ev.u.fetch_request.start_group == 7) saw_rel = true;   /* 10-3 */
                if (ev.u.fetch_request.start_group == 4) saw_abs = true;   /* abs 4 */
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(n_req, 2);
        MOQ_TEST_CHECK(saw_rel && saw_abs);
        MOQ_TEST_CHECK_EQ_INT(count_pending_joins(s), 0);
        moq_session_destroy(s);
    }

    /* == Mixed accept: a relative join releases while an absolute join whose start
     *    is past Largest is rejected (INVALID_RANGE), in the same accept. ==== */
    {
        moq_session_t *s = make_server();
        moq_subscription_t ssub = feed_pending_subscribe(s, moq_stream_ref_from_u64(0x70A1), 0, true);
        uint8_t fb[160];
        size_t n1 = make_join_fetch(fb, sizeof(fb), 2, 0, 2 /*rel*/, 3, false);
        moq_session_on_bidi_stream_bytes(s, moq_stream_ref_from_u64(0x70A2), fb, n1, false, 1);
        uint8_t fb2[160];
        size_t n2 = make_join_fetch(fb2, sizeof(fb2), 4, 0, 3 /*abs*/, 20 /*>largest*/, false);
        moq_stream_ref_t aref = moq_stream_ref_from_u64(0x70A3);
        moq_session_on_bidi_stream_bytes(s, aref, fb2, n2, false, 1);
        moq_action_t a; while (moq_session_poll_actions(s, &a, 1) > 0)
            moq_action_cleanup(&a);
        moq_accept_subscribe_cfg_t ac; moq_accept_subscribe_cfg_init(&ac);
        ac.has_track_alias = true; ac.track_alias = 7;
        ac.has_largest = true; ac.largest_group = 10; ac.largest_object = 5;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_subscribe(s, ssub, &ac, 1),
                              (int)MOQ_OK);
        int n_req = 0; moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_REQUEST) {
                n_req++;
                MOQ_TEST_CHECK_EQ_U64(ev.u.fetch_request.start_group, 7);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(n_req, 1);   /* only the relative join released */
        MOQ_TEST_CHECK(saw_request_error(s, aref, MOQ_REQUEST_ERROR_INVALID_RANGE));
        MOQ_TEST_CHECK_EQ_INT(count_pending_joins(s), 0);
        moq_session_destroy(s);
    }

    /* == A released FETCH_REQUEST's tokens are scratch-copied (per the public
     *    contract), NOT borrowed from entry storage: after release the fetch entry
     *    no longer owns the token heap, so a peer RESET that frees the entry before
     *    the next poll cannot dangle the queued event. Catches the regression where
     *    the event borrowed entry-owned token heap freed by fetch_free_entry. === */
    {
        moq_session_t *s = make_server();
        moq_subscription_t ssub = feed_pending_subscribe(s, moq_stream_ref_from_u64(0x70B1),
                                                 0, true);
        uint8_t fb[160];
        size_t fn = make_join_fetch(fb, sizeof(fb), 2, 0, 2, 3, true /* token */);
        moq_stream_ref_t jref = moq_stream_ref_from_u64(0x70B2);
        moq_session_on_bidi_stream_bytes(s, jref, fb, fn, false, 1);
        moq_action_t a; while (moq_session_poll_actions(s, &a, 1) > 0)
            moq_action_cleanup(&a);
        moq_accept_subscribe_cfg_t ac; moq_accept_subscribe_cfg_init(&ac);
        ac.has_track_alias = true; ac.track_alias = 7;
        ac.has_largest = true; ac.largest_group = 10; ac.largest_object = 5;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_subscribe(s, ssub, &ac, 1),
                              (int)MOQ_OK);
        /* White-box: the released (now PENDING_PUBLISHER) fetch entry must hold no
         * entry-owned token storage -- the event borrows scratch, so a later
         * fetch_free_entry frees nothing and cannot dangle the queued event. */
        bool released_clean = false;
        for (size_t i = 0; i < s->fetch_cap; i++)
            if (s->fetches[i].state == MOQ_FETCH_PENDING_PUBLISHER &&
                s->fetches[i].join_token_count == 0)
                released_clean = true;
        MOQ_TEST_CHECK(released_clean);
        /* Poll the FETCH_REQUEST in-epoch (contract-valid scratch borrow): token
         * bytes are intact. */
        bool tok_ok = false; moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_REQUEST) {
                moq_fetch_request_event_t *fr = &ev.u.fetch_request;
                tok_ok = fr->token_count == 1 && fr->tokens &&
                         fr->tokens[0].token_type == 7 &&
                         fr->tokens[0].token_value.len == 7 &&
                         memcmp(fr->tokens[0].token_value.data, "jointok", 7) == 0;
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(tok_ok);
        /* Now the peer RESETs the fetch bidi: the entry is torn down and freed. With
         * the fix this frees no token heap (join_token_count == 0) -- no double free,
         * no dangling (ASAN). Events are consumed without dereferencing stale data. */
        moq_session_on_bidi_stream_reset(s, jref, 0x1, 1);
        MOQ_TEST_CHECK_EQ_INT(count_pending_joins(s), 0);
        moq_event_t cev; while (moq_session_poll_events(s, &cev, 1) > 0)
            moq_event_cleanup(&cev);
        moq_session_destroy(s);
    }
    /* == Buffered-join token copies are all-or-nothing =================
     * Buffering a joining FETCH copies each borrowed token value out of the
     * shared request receive buffer, and those copies are the operation's
     * ONLY allocations -- the declared signature below pins exactly that.
     * A failed copy reports NOMEM, releases every copy already taken, and
     * the request-stream handler frees the still-receiving staging slot:
     * the buffered bytes go with it, an empty re-feed has nothing to
     * resume, and recovery is RE-DELIVERY of the whole request -- clean
     * because no request id was committed (the same id re-admitting is the
     * behavioral proof; the sequence counter itself is profile-private).
     *
     * Every ordinal is swept on its OWN fixture with its OWN
     * recovery-equivalence cycle against the baseline: same ordered
     * normalized output (the released FETCH_REQUEST's every semantic field
     * and the produced actions), same final snapshot and hook values, same
     * allocation footprint. The outcome table is machine-checked against
     * the signature's attempt count.
     *
     * Mutation inventory: the staging subscription slot and, after a
     * successful buffering handoff, the destination FETCH entry (state,
     * generation, buffered bytes, latched FIN -- ownership and state via
     * the generic snapshot's owner records; the buffered-input and FIN
     * latches via the hook, which resolves WHICHEVER of the two currently
     * owns the bidi), its by-streamref registry key, the
     * pending-join table and its token heap (the allocator map), and
     * request-pool occupancy. The referenced subscription, the event and
     * action queues, scratch, and the budget must not move; the generic
     * snapshot and the jf hooks cover those. The carrier's buffer is
     * session-slab storage, so the declared retirement delta is semantic
     * ONLY: the allocator map must not move at all. */
    {
        static const fp_expect_t k_join_sig[2] = {
            { FP_ALLOC, FP_SIZE_EXACT, 7, 0 },   /* "jointok" */
            { FP_ALLOC, FP_SIZE_EXACT, 17, 0 },  /* "second-join-token" */
        };
        static const fp_outcome_row_t k_join_out[2] = {
            { FP_PHASE_PRE_COMMIT, FP_NOMEM_REDELIVER },
            { FP_PHASE_PRE_COMMIT, FP_NOMEM_REDELIVER },
        };

        fp_attempt_t   base_log[FP_LOG_CAP];
        size_t         base_n = 0;
        txs_norm_vec_t base_out;
        txs_snapshot_t base_final;
        jf_hook_state_t base_hook;
        fp_delta_t     base_delta;
        txs_norm_init(&base_out);

        /* -- Baseline: the un-failed two-token flow through release ----- */
        {
            fp_alloc_state_t fs = {0};
            moq_alloc_t al = fp_allocator(&fs);
            moq_session_t *s = make_server_alloc(&al);
            MOQ_TEST_CHECK(s != NULL);
            moq_subscription_t ssub = feed_pending_subscribe(s,
                moq_stream_ref_from_u64(0x7091), 0, true);
            MOQ_TEST_CHECK(ssub._opaque != MOQ_SUBSCRIPTION_INVALID._opaque);
            moq_action_t a;
            while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
            moq_stream_ref_t fref = moq_stream_ref_from_u64(0x7092);
            jf_hook_ctx_t hctx = { ssub._opaque, fref };
            txs_op_hooks_t hooks = jf_make_hooks(&hctx);

            uint8_t fb[224];
            size_t fn = make_join_fetch_two_tokens(fb, sizeof(fb), 2, 0, 2, 3);
            fp_map_snap_t pre_op;
            fp_map_capture(&fs, &pre_op);
            fs.log_from = fs.call_count;
            fs.log_len = 0;
            /* The baseline delivery CARRIES FIN -- the operation's canonical
             * form, and the form the fragmented recovery must equal. */
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_session_on_bidi_stream_bytes(s, fref, fb, fn,
                                                      true, 1),
                (int)MOQ_OK);
            failures += fp_check_signature(&fs, 0, k_join_sig, 2,
                                           "join-baseline");
            memcpy(base_log, fs.log, fs.log_len * sizeof(fp_attempt_t));
            base_n = fs.log_len;
            MOQ_TEST_CHECK_EQ_SIZE(base_n, 2u);
            MOQ_TEST_CHECK_EQ_SIZE(sizeof(k_join_out) / sizeof(k_join_out[0]),
                                   base_n);
            MOQ_TEST_CHECK_EQ_INT(count_pending_joins(s), 1);

            failures += jf_accept_and_collect(s, ssub, &hooks, &base_out,
                                              &failures);
            txs_capture(s, &fref, 1, &base_final);
            hooks.capture(s, hooks.ctx, &base_hook);
            /* ABSOLUTE expectations a recovery-equivalence compare cannot
             * carry (a defect present in both runs cancels out): the
             * delivery CARRIED FIN, so after the handoff the destination
             * FETCH entry owns the bidi and must hold the latched FIN. */
            MOQ_TEST_CHECK(request_registry_find_by_streamref(s, fref).kind
                           == MOQ_REQ_FETCH);
            MOQ_TEST_CHECK(base_hook.stage_recv_fin);
            fp_delta_compute(&fs, &pre_op, &base_delta);
            failures += jf_fx_teardown(&fs, s, "join-baseline");
        }

        /* -- Per-ordinal sweep: one FRESH fixture per origin, each with
         * its own no-op empty re-feed, re-delivery, and full equivalence
         * cycle -- a defect at the first copy cannot hide behind the
         * second. */
        for (uint64_t k = 1; k <= 2; k++) {
            const fp_outcome_row_t *row = &k_join_out[k - 1];
            MOQ_TEST_CHECK(row->phase == FP_PHASE_PRE_COMMIT);
            MOQ_TEST_CHECK(row->outcome == FP_NOMEM_REDELIVER);

            fp_alloc_state_t fs = {0};
            moq_alloc_t al = fp_allocator(&fs);
            moq_session_t *s = make_server_alloc(&al);
            MOQ_TEST_CHECK(s != NULL);
            moq_subscription_t ssub = feed_pending_subscribe(s,
                moq_stream_ref_from_u64(0x7091), 0, true);
            MOQ_TEST_CHECK(ssub._opaque != MOQ_SUBSCRIPTION_INVALID._opaque);
            moq_action_t a;
            while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
            int sub_slot = test_sub_resolve_handle(s, ssub);
            MOQ_TEST_CHECK(sub_slot >= 0);
            moq_stream_ref_t fref = moq_stream_ref_from_u64(0x7092);
            jf_hook_ctx_t hctx = { ssub._opaque, fref };
            txs_op_hooks_t hooks = jf_make_hooks(&hctx);

            uint8_t fb[224];
            size_t fn = make_join_fetch_two_tokens(fb, sizeof(fb), 2, 0, 2, 3);

            /* One no-op advancing call recycles the arena left by the
             * SUBSCRIBE_REQUEST delivery, so the snapshot starts from the
             * cursor every later call starts from. */
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_session_on_bidi_stream_bytes(s, fref, NULL, 0,
                                                      false, 1),
                (int)MOQ_OK);
            MOQ_TEST_CHECK_EQ_SIZE(s->event_scratch_len, 0u);

            fp_map_snap_t m0;
            fp_map_capture(&fs, &m0);
            txs_snapshot_t s0;
            txs_capture(s, &fref, 1, &s0);
            MOQ_TEST_CHECK_EQ_INT(s0.owners[0].kind, TXS_OWNER_NONE);
            jf_hook_state_t h0;
            hooks.capture(s, hooks.ctx, &h0);

            fs.log_from = fs.call_count;
            fs.log_len = 0;
            fs.fail_at = fs.call_count + k;
            moq_result_t rc = moq_session_on_bidi_stream_bytes(
                s, fref, fb, fn, true, 1);
            fp_context("join-sweep", k, 2, &fs);
            MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_ERR_NOMEM);
            MOQ_TEST_CHECK_EQ_U64(fs.call_count, fs.fail_at);
            fs.fail_at = 0;
            failures += fp_check_prefix(&fs, 0, base_log, (size_t)k,
                                        "join-sweep");
            /* The single-call carrier never survives its own failure:
             * created and retired inside the call, semantic delta nil,
             * allocator-map delta nil (slab-backed buffering). */
            failures += fp_map_equals(&fs, &m0, "join-sweep");
            failures += txs_check_eq(s, &fref, 1, &s0, "join-sweep");
            failures += hooks.check(s, hooks.ctx, &h0);
            MOQ_TEST_CHECK_EQ_INT((int)s->subs[sub_slot].state,
                                  (int)MOQ_SUB_PENDING_PUBLISHER);
            moq_event_t ev;
            MOQ_TEST_CHECK(moq_session_poll_events(s, &ev, 1) == 0);
            MOQ_TEST_CHECK(moq_session_poll_actions(s, &a, 1) == 0);

            /* This ordinal's own recovery: the empty re-feed is a NO-OP
             * (nothing was retained), then re-delivery of the whole
             * request admits the SAME id and the run must equal the
             * baseline everywhere. */
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_session_on_bidi_stream_bytes(s, fref, NULL, 0,
                                                      false, 1),
                (int)MOQ_OK);
            failures += hooks.check(s, hooks.ctx, &h0);
            MOQ_TEST_CHECK(moq_session_poll_events(s, &ev, 1) == 0);

            /* Re-delivery against a NEW window: the recovery's allocation
             * sequence -- not just its net footprint -- must agree with
             * the baseline's. */
            fs.log_from = fs.call_count;
            fs.log_len = 0;
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_session_on_bidi_stream_bytes(s, fref, fb, fn,
                                                      true, 1),
                (int)MOQ_OK);
            failures += fp_check_signature(&fs, 0, k_join_sig, 2,
                                           "join-recovery");
            failures += fp_check_prefix(&fs, 0, base_log, 2,
                                        "join-recovery");
            MOQ_TEST_CHECK_EQ_INT(count_pending_joins(s), 1);

            txs_norm_vec_t out;
            txs_norm_init(&out);
            failures += jf_accept_and_collect(s, ssub, &hooks, &out,
                                              &failures);
            failures += txs_norm_equals(&out, &base_out, "join-recovery");
            txs_norm_free(&out);
            failures += txs_check_eq(s, &fref, 1, &base_final, "join-final");
            failures += hooks.check_values(s, hooks.ctx, &base_hook);
            fp_delta_t delta;
            fp_delta_compute(&fs, &m0, &delta);
            failures += fp_delta_equals(&delta, &base_delta, "join-recovery");
            failures += jf_fx_teardown(&fs, s, "join-sweep");
        }

        /* -- A fragmented request: the failing FINAL feed retires a REAL
         * pre-existing carrier. The head feed registers the staging slot
         * and buffers the partial message; the final feed CARRIES FIN and
         * fails on the first token copy. The declared retirement delta --
         * not pre-call equality -- is the oracle: the slot is freed with
         * its generation advanced, the by-streamref key is gone, the
         * buffered bytes and the latched FIN go with it, and the allocator
         * map does not move at all. Full re-delivery WITH FIN then
         * completes exactly once, equal to the baseline. */
        {
            fp_alloc_state_t fs = {0};
            moq_alloc_t al = fp_allocator(&fs);
            moq_session_t *s = make_server_alloc(&al);
            MOQ_TEST_CHECK(s != NULL);
            moq_subscription_t ssub = feed_pending_subscribe(s,
                moq_stream_ref_from_u64(0x7091), 0, true);
            MOQ_TEST_CHECK(ssub._opaque != MOQ_SUBSCRIPTION_INVALID._opaque);
            moq_action_t a;
            while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
            moq_stream_ref_t fref = moq_stream_ref_from_u64(0x7092);
            jf_hook_ctx_t hctx = { ssub._opaque, fref };
            txs_op_hooks_t hooks = jf_make_hooks(&hctx);

            uint8_t fb[224];
            size_t fn = make_join_fetch_two_tokens(fb, sizeof(fb), 2, 0, 2, 3);
            size_t head = fn / 2;

            /* Hook state BEFORE the head: retirement must return here. */
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_session_on_bidi_stream_bytes(s, fref, NULL, 0,
                                                      false, 1),
                (int)MOQ_OK);
            jf_hook_state_t h_pre;
            hooks.capture(s, hooks.ctx, &h_pre);
            fp_map_snap_t pre_op;
            fp_map_capture(&fs, &pre_op);

            /* The head registers the carrier and buffers the partial
             * message; nothing completes. */
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_session_on_bidi_stream_bytes(s, fref, fb, head,
                                                      false, 1),
                (int)MOQ_OK);
            moq_request_endpoint_t ep =
                request_registry_find_by_streamref(s, fref);
            MOQ_TEST_CHECK(ep.kind == MOQ_REQ_SUBSCRIPTION);
            int stage_slot = (int)ep.slot;
            MOQ_TEST_CHECK_EQ_INT((int)s->subs[stage_slot].state,
                                  (int)MOQ_SUB_RECVING_REQUEST);
            uint32_t gen_before = s->subs[stage_slot].generation;
            MOQ_TEST_CHECK(s->subs[stage_slot].req_recv_len > 0);
            fp_map_snap_t m0;
            fp_map_capture(&fs, &m0);

            /* The failing final feed, FIN riding it. */
            fs.log_from = fs.call_count;
            fs.log_len = 0;
            fs.fail_at = fs.call_count + 1;
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_session_on_bidi_stream_bytes(s, fref, fb + head,
                                                      fn - head, true, 1),
                (int)MOQ_ERR_NOMEM);
            MOQ_TEST_CHECK_EQ_U64(fs.call_count, fs.fail_at);
            fs.fail_at = 0;

            /* The declared retirement, exactly -- and nothing else. */
            MOQ_TEST_CHECK(request_registry_find_by_streamref(s, fref).kind
                           == MOQ_REQ_NONE);
            MOQ_TEST_CHECK_EQ_INT((int)s->subs[stage_slot].state,
                                  (int)MOQ_SUB_FREE);
            MOQ_TEST_CHECK_EQ_U64(s->subs[stage_slot].generation,
                                  gen_before + 1);
            MOQ_TEST_CHECK_EQ_SIZE(s->subs[stage_slot].req_recv_len, 0u);
            MOQ_TEST_CHECK(!s->subs[stage_slot].req_recv_fin);
            failures += fp_map_equals(&fs, &m0, "join-frag");
            failures += hooks.check(s, hooks.ctx, &h_pre);
            moq_event_t ev;
            MOQ_TEST_CHECK(moq_session_poll_events(s, &ev, 1) == 0);
            MOQ_TEST_CHECK(moq_session_poll_actions(s, &a, 1) == 0);
            MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);

            /* Nothing to resume; then the whole request WITH FIN completes
             * exactly once and equals the baseline's release. */
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_session_on_bidi_stream_bytes(s, fref, NULL, 0,
                                                      false, 1),
                (int)MOQ_OK);
            failures += hooks.check(s, hooks.ctx, &h_pre);

            /* Full re-delivery WITH FIN, against a NEW window: the
             * recovery's allocation sequence must agree with the FIN-
             * bearing baseline's. */
            fs.log_from = fs.call_count;
            fs.log_len = 0;
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_session_on_bidi_stream_bytes(s, fref, fb, fn,
                                                      true, 1),
                (int)MOQ_OK);
            failures += fp_check_signature(&fs, 0, k_join_sig, 2,
                                           "join-frag-rec");
            failures += fp_check_prefix(&fs, 0, base_log, 2,
                                        "join-frag-rec");
            MOQ_TEST_CHECK_EQ_INT(count_pending_joins(s), 1);

            txs_norm_vec_t out;
            txs_norm_init(&out);
            failures += jf_accept_and_collect(s, ssub, &hooks, &out,
                                              &failures);
            failures += txs_norm_equals(&out, &base_out, "join-frag-rec");
            txs_norm_free(&out);
            /* FIN-aware final equivalence: owner state (incl. the latched
             * FIN and buffered input through the hook), the generic
             * snapshot, and the allocation footprint all equal the
             * FIN-bearing baseline's. */
            failures += txs_check_eq(s, &fref, 1, &base_final,
                                     "join-frag-final");
            failures += hooks.check_values(s, hooks.ctx, &base_hook);
            {
                fp_delta_t delta;
                fp_delta_compute(&fs, &pre_op, &delta);
                failures += fp_delta_equals(&delta, &base_delta,
                                            "join-frag-final");
            }
            failures += jf_fx_teardown(&fs, s, "join-frag");
        }

        txs_norm_free(&base_out);
    }

    MOQ_TEST_PASS("d18_joining");
    return failures != 0;
}
