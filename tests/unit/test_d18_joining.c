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
static moq_session_t *make_server(void)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), MOQ_PERSPECTIVE_SERVER);
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

/* Feed an inbound LARGEST_OBJECT SUBSCRIBE (forward as given), leave it pending,
 * and return the responder subscription handle. */
static moq_subscription_t feed_subscribe(moq_session_t *s, moq_stream_ref_t ref,
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
    moq_subscription_t h = feed_subscribe(s, ref, req_id, true);
    if (h._opaque == MOQ_SUBSCRIPTION_INVALID._opaque) return false;
    moq_accept_subscribe_cfg_t ac; moq_accept_subscribe_cfg_init(&ac);
    ac.has_track_alias = true; ac.track_alias = req_id + 100;
    ac.has_largest = true; ac.largest_group = 1; ac.largest_object = 0;
    return moq_session_accept_subscribe(s, h, &ac, 1) == MOQ_OK;
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
        moq_subscription_t ssub = feed_subscribe(s, moq_stream_ref_from_u64(0x7001),
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
        moq_subscription_t ssub = feed_subscribe(s, moq_stream_ref_from_u64(0x7011),
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
        moq_subscription_t ssub = feed_subscribe(s, moq_stream_ref_from_u64(0x7021),
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

        moq_subscription_t ssub = feed_subscribe(s, moq_stream_ref_from_u64(0x7031),
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
        moq_subscription_t ssub = feed_subscribe(s, moq_stream_ref_from_u64(0x7041),
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
        moq_subscription_t ssub = feed_subscribe(s, moq_stream_ref_from_u64(0x7051),
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

        moq_subscription_t ssub = feed_subscribe(s, moq_stream_ref_from_u64(0x7071), 0, true);
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

        moq_subscription_t ssub = feed_subscribe(s, moq_stream_ref_from_u64(0x7081), 0, true);
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
        moq_subscription_t ssub = feed_subscribe(s, moq_stream_ref_from_u64(0x7091), 0, true);
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
        moq_subscription_t ssub = feed_subscribe(s, moq_stream_ref_from_u64(0x70A1), 0, true);
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
        moq_subscription_t ssub = feed_subscribe(s, moq_stream_ref_from_u64(0x70B1),
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

    MOQ_TEST_PASS("d18_joining");
    return failures != 0;
}
