/*
 * Draft-18 inbound FETCH on a request bidi (publisher side): a new bidi whose
 * first message is FETCH is received into a staging subscription slot, decoded,
 * and handed off to a fetch-pool entry -- the stream-ref registry is re-keyed
 * from the staging slot to (MOQ_REQ_FETCH, fetch_slot) and the staging slot is
 * released without disturbing that mapping. Covers fragmented receipt, the
 * backpressure retry, and fail-closed admission with no control-channel error.
 */
#include <moq/moq.h>
#include <moq/control_d18.h>
#include "test_support.h"
#include "../../core/src/session/session_internal.h"

static moq_session_t *make_server(uint32_t max_events)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), MOQ_PERSPECTIVE_SERVER);
    cfg.version = MOQ_VERSION_DRAFT_18;
    if (max_events) cfg.max_events = max_events;
    moq_session_t *s = NULL;
    if (moq_session_create(&cfg, 0, &s) < 0) return NULL;
    if (moq_session_start(s, 0) < 0) { moq_session_destroy(s); return NULL; }
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
    uint8_t setup[16];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, setup, sizeof(setup));
    moq_d18_encode_setup(&w);
    moq_session_on_control_bytes(s, setup, moq_buf_writer_offset(&w), 0);
    moq_event_t e;
    while (moq_session_poll_events(s, &e, 1) > 0) moq_event_cleanup(&e);
    return s;
}
static moq_session_t *make_established_d18_server(void) { return make_server(0); }

static moq_session_t *make_established_d18_client(void)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), MOQ_PERSPECTIVE_CLIENT);
    cfg.version = MOQ_VERSION_DRAFT_18;
    moq_session_t *s = NULL;
    if (moq_session_create(&cfg, 0, &s) < 0) return NULL;
    if (moq_session_start(s, 0) < 0) { moq_session_destroy(s); return NULL; }
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
    uint8_t setup[16];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, setup, sizeof(setup));
    moq_d18_encode_setup(&w);
    moq_session_on_control_bytes(s, setup, moq_buf_writer_offset(&w), 0);
    moq_event_t e;
    while (moq_session_poll_events(s, &e, 1) > 0) moq_event_cleanup(&e);
    return s;
}

/* Outbound standalone FETCH for "live"/track; returns the request bidi ref. */
static moq_stream_ref_t do_fetch(moq_session_t *s, const char *track,
                                 moq_fetch_t *h, moq_action_kind_t *out_kind)
{
    moq_fetch_cfg_t cfg;
    moq_fetch_cfg_init(&cfg);
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    cfg.track_namespace = (moq_namespace_t){ parts, 1 };
    cfg.track_name = (moq_bytes_t){ (const uint8_t *)track, strlen(track) };
    cfg.start_group = 0; cfg.start_object = 0;
    cfg.end_group = 10; cfg.end_object = 0;
    (void)moq_session_fetch(s, &cfg, 1, h);
    moq_action_t act;
    moq_stream_ref_t ref = moq_stream_ref_from_u64(0);
    if (moq_session_poll_actions(s, &act, 1) == 1) {
        if (out_kind) *out_kind = (moq_action_kind_t)act.kind;
        if (act.kind == MOQ_ACTION_OPEN_BIDI_STREAM)
            ref = act.u.open_bidi_stream.stream_ref;
        moq_action_cleanup(&act);
    }
    return ref;
}

static int drain_kind(moq_session_t *s, uint32_t want)
{
    moq_event_t ev;
    int n = 0;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        if (ev.kind == want) n++;
        moq_event_cleanup(&ev);
    }
    return n;
}

/* Build a FETCH message for namespace "live"/track. */
static size_t make_fetch(uint8_t *buf, size_t cap, uint64_t request_id,
                         uint64_t fetch_type, const char *track)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_d18_fetch_t f;
    memset(&f, 0, sizeof(f));
    f.request_id = request_id;
    f.fetch_type = fetch_type;
    f.track_namespace = (moq_namespace_t){ parts, 1 };
    f.track_name = (moq_bytes_t){ (const uint8_t *)track, strlen(track) };
    f.start = (moq_d18_location_t){ 0, 0 };
    f.end = (moq_d18_location_t){ 10, 0 };
    f.joining_request_id = 0;
    f.joining_start = 0;
    moq_d18_encode_fetch(&w, &f);
    return moq_buf_writer_offset(&w);
}

static bool got_fetch_request(moq_session_t *s)
{
    moq_event_t ev;
    bool got = false;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_FETCH_REQUEST) got = true;
        moq_event_cleanup(&ev);
    }
    return got;
}

static bool any_send_control(moq_session_t *s)
{
    moq_action_t a;
    bool found = false;
    while (moq_session_poll_actions(s, &a, 1) > 0) {
        if (a.kind == MOQ_ACTION_SEND_CONTROL) found = true;
        moq_action_cleanup(&a);
    }
    return found;
}

int main(void)
{
    int failures = 0;

    /* == A. Inbound FETCH handoff: request bidi -> fetch entry ======== */
    {
        moq_session_t *s = make_established_d18_server();
        MOQ_TEST_CHECK(s != NULL);
        uint8_t msg[128];
        size_t n = make_fetch(msg, sizeof(msg), 0, 1, "video");
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0xF001);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1),
            (int)MOQ_OK);
        MOQ_TEST_CHECK(got_fetch_request(s));
        /* The stream-ref now resolves to a FETCH entry (handoff re-keyed it). */
        moq_request_endpoint_t byref = request_registry_find_by_streamref(s, ref);
        MOQ_TEST_CHECK_EQ_INT((int)byref.kind, (int)MOQ_REQ_FETCH);
        MOQ_TEST_CHECK_EQ_INT((int)s->fetches[byref.slot].state,
                              (int)MOQ_FETCH_PENDING_PUBLISHER);
        MOQ_TEST_CHECK_EQ_U64(s->fetches[byref.slot].request_stream_ref._v,
                              ref._v);
        moq_session_destroy(s);
    }

    /* == A2. Inbound FETCH after local GOAWAY is refused ============== */
    {
        /* After we send GOAWAY (§10.4) the peer must not open new requests.
         * A fresh FETCH on a request bidi must be a protocol violation, not a
         * committed fetch entry + app event. */
        moq_session_t *s = make_established_d18_server();
        MOQ_TEST_CHECK(s != NULL);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_goaway(s, NULL, 0, 0),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(s),
                              (int)MOQ_SESS_DRAINING);

        uint8_t msg[128];
        size_t n = make_fetch(msg, sizeof(msg), 0, 1, "video");
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0xF0A2);
        moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1);

        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(s), (int)MOQ_SESS_CLOSED);
        /* No FETCH_REQUEST surfaced; the close is a protocol violation (0x3). */
        moq_event_t cev;
        bool saw_fetch = false, saw_closed = false;
        while (moq_session_poll_events(s, &cev, 1) > 0) {
            if (cev.kind == MOQ_EVENT_FETCH_REQUEST) saw_fetch = true;
            if (cev.kind == MOQ_EVENT_SESSION_CLOSED) {
                saw_closed = true;
                MOQ_TEST_CHECK_EQ_U64(cev.u.closed.code, 0x3);
            }
            moq_event_cleanup(&cev);
        }
        MOQ_TEST_CHECK(!saw_fetch);
        MOQ_TEST_CHECK(saw_closed);
        /* No committed fetch entry was created for the refused request. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)request_registry_find_by_streamref(s, ref).kind,
            (int)MOQ_REQ_NONE);
        moq_session_destroy(s);
    }

    /* == B. Fragmented inbound FETCH (one byte at a time) ============= */
    {
        moq_session_t *s = make_established_d18_server();
        MOQ_TEST_CHECK(s != NULL);
        uint8_t msg[128];
        size_t n = make_fetch(msg, sizeof(msg), 0, 1, "video");
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0xF002);
        for (size_t i = 0; i < n; i++) {
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_session_on_bidi_stream_bytes(s, ref, &msg[i], 1,
                    false, 1), (int)MOQ_OK);
            MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        }
        MOQ_TEST_CHECK(got_fetch_request(s));
        MOQ_TEST_CHECK_EQ_INT(
            (int)request_registry_find_by_streamref(s, ref).kind,
            (int)MOQ_REQ_FETCH);
        moq_session_destroy(s);
    }

    /* == C. Backpressure: WOULD_BLOCK keeps staging; re-feed commits == */
    {
        moq_session_t *s = make_server(1);   /* event queue holds one entry */
        MOQ_TEST_CHECK(s != NULL);
        /* First inbound FETCH commits and fills the event queue. */
        uint8_t m1[128];
        size_t n1 = make_fetch(m1, sizeof(m1), 0, 1, "video");
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s,
                moq_stream_ref_from_u64(0xF010), m1, n1, false, 1), (int)MOQ_OK);

        /* Second inbound FETCH: event queue full -> WOULD_BLOCK, staging kept. */
        uint8_t m2[128];
        size_t n2 = make_fetch(m2, sizeof(m2), 2, 1, "audio");
        moq_stream_ref_t ref2 = moq_stream_ref_from_u64(0xF011);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref2, m2, n2, false, 1),
            (int)MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);

        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) moq_event_cleanup(&ev);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref2, NULL, 0, false, 1),
            (int)MOQ_OK);
        MOQ_TEST_CHECK(got_fetch_request(s));
        MOQ_TEST_CHECK_EQ_INT(
            (int)request_registry_find_by_streamref(s, ref2).kind,
            (int)MOQ_REQ_FETCH);
        moq_session_destroy(s);
    }

    /* == D. Joining FETCH for an unknown subscription → REQUEST_ERROR
     *       (INVALID_JOINING_REQUEST_ID) on the request bidi, not a session close
     *       and not on the control channel. == */
    {
        moq_session_t *s = make_established_d18_server();
        MOQ_TEST_CHECK(s != NULL);
        uint8_t msg[128];
        size_t n = make_fetch(msg, sizeof(msg), 0, 2 /* relative join */, "v");
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0xF020);
        (void)moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK(!got_fetch_request(s));   /* rejected internally, no event */
        bool saw_err = false, saw_control = false;
        moq_action_t act;
        while (moq_session_poll_actions(s, &act, 1) > 0) {
            if (act.kind == MOQ_ACTION_SEND_CONTROL) saw_control = true;
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
                        er.error_code == MOQ_REQUEST_ERROR_INVALID_JOINING_REQUEST_ID)
                        saw_err = true;
                }
            }
            moq_action_cleanup(&act);
        }
        MOQ_TEST_CHECK(saw_err);                  /* REQUEST_ERROR on the bidi */
        MOQ_TEST_CHECK(!saw_control);             /* not on the control channel */
        /* The reject drained the bidi (the fetcher's send half was still open), so
         * a late empty FIN is absorbed rather than closing the session. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == D2. A terminal joining reject whose FETCH already carried FIN must NOT
     *        leave a drain ref behind (no slot leak) — there is no late FIN to
     *        absorb. == */
    {
        moq_session_t *s = make_established_d18_server();
        MOQ_TEST_CHECK(s != NULL);
        size_t drain_before = s->drain_ref_count;
        uint8_t msg[128];
        size_t n = make_fetch(msg, sizeof(msg), 0, 2 /* relative join */, "v");
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0xF021);
        (void)moq_session_on_bidi_stream_bytes(s, ref, msg, n, true /* fin */, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK(!got_fetch_request(s));
        MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, drain_before);   /* no leak */
        moq_session_destroy(s);
    }

    /* == E. Many handoffs do not exhaust the staging sub pool ========= *
     * Each handoff must free its staging slot; otherwise repeated FETCHes would
     * run the sub pool dry. Drive more FETCHes than a small sub pool holds. */
    {
        moq_session_cfg_t cfg;
        moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), MOQ_PERSPECTIVE_SERVER);
        cfg.version = MOQ_VERSION_DRAFT_18;
        cfg.max_subscriptions = 2;
        cfg.max_fetches = 8;
        moq_session_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_create(&cfg, 0, &s), (int)MOQ_OK);
        moq_session_start(s, 0);
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
        uint8_t setup[16];
        moq_buf_writer_t sw;
        moq_buf_writer_init(&sw, setup, sizeof(setup));
        moq_d18_encode_setup(&sw);
        moq_session_on_control_bytes(s, setup, moq_buf_writer_offset(&sw), 0);
        moq_event_t e;
        while (moq_session_poll_events(s, &e, 1) > 0) moq_event_cleanup(&e);

        char track[8];
        for (int i = 0; i < 5; i++) {   /* > max_subscriptions=2 */
            uint8_t msg[128];
            snprintf(track, sizeof(track), "t%d", i);
            size_t n = make_fetch(msg, sizeof(msg), (uint64_t)(i * 2), 1, track);
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_session_on_bidi_stream_bytes(s,
                    moq_stream_ref_from_u64(0xF100 + (uint64_t)i), msg, n,
                    false, 1), (int)MOQ_OK);
            MOQ_TEST_CHECK(got_fetch_request(s));
            MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        }
        moq_session_destroy(s);
    }

    /* == F. Fetch pool full fails closed with no control-channel error  */
    {
        moq_session_cfg_t cfg;
        moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), MOQ_PERSPECTIVE_SERVER);
        cfg.version = MOQ_VERSION_DRAFT_18;
        cfg.max_fetches = 1;
        moq_session_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_create(&cfg, 0, &s), (int)MOQ_OK);
        moq_session_start(s, 0);
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
        uint8_t setup[16];
        moq_buf_writer_t sw;
        moq_buf_writer_init(&sw, setup, sizeof(setup));
        moq_d18_encode_setup(&sw);
        moq_session_on_control_bytes(s, setup, moq_buf_writer_offset(&sw), 0);
        moq_event_t e;
        while (moq_session_poll_events(s, &e, 1) > 0) moq_event_cleanup(&e);

        uint8_t m1[128];
        size_t n1 = make_fetch(m1, sizeof(m1), 0, 1, "a");
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s,
                moq_stream_ref_from_u64(0xF200), m1, n1, false, 1), (int)MOQ_OK);
        MOQ_TEST_CHECK(got_fetch_request(s));
        while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);

        /* Second FETCH cannot allocate a slot -> fail closed, no SEND_CONTROL. */
        uint8_t m2[128];
        size_t n2 = make_fetch(m2, sizeof(m2), 2, 1, "b");
        (void)moq_session_on_bidi_stream_bytes(s,
            moq_stream_ref_from_u64(0xF201), m2, n2, false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        MOQ_TEST_CHECK(!any_send_control(s));
        moq_session_destroy(s);
    }

    /* == G. Outbound FETCH opens a bidi, registered by stream_ref ===== */
    {
        moq_session_t *s = make_established_d18_client();
        MOQ_TEST_CHECK(s != NULL);
        moq_fetch_t h;
        moq_action_kind_t kind = (moq_action_kind_t)0;
        moq_stream_ref_t ref = do_fetch(s, "video", &h, &kind);
        MOQ_TEST_CHECK_EQ_U64(kind, MOQ_ACTION_OPEN_BIDI_STREAM);  /* not SEND_CONTROL */
        moq_request_endpoint_t byref = request_registry_find_by_streamref(s, ref);
        MOQ_TEST_CHECK_EQ_INT((int)byref.kind, (int)MOQ_REQ_FETCH);
        /* The response correlates by stream_ref; the data uni (FETCH_HEADER)
         * carries the Request ID, so the by-id key is also registered. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)request_registry_find_by_id(s, 0).kind, (int)MOQ_REQ_FETCH);
        moq_session_destroy(s);
    }

    /* == H. Fetcher receives FETCH_OK on its request bidi ============= */
    {
        moq_session_t *s = make_established_d18_client();
        MOQ_TEST_CHECK(s != NULL);
        moq_fetch_t h;
        moq_stream_ref_t ref = do_fetch(s, "video", &h, NULL);
        uint8_t ok[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, ok, sizeof(ok));
        moq_d18_encode_fetch_ok(&w, false, (moq_d18_location_t){ 10, 0 }, (moq_bytes_t){0});
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, ok,
                moq_buf_writer_offset(&w), false, 1), (int)MOQ_OK);
        MOQ_TEST_CHECK(drain_kind(s, MOQ_EVENT_FETCH_OK) == 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == I. Fetcher REQUEST_ERROR with split FIN drains ============== */
    {
        moq_session_t *s = make_established_d18_client();
        MOQ_TEST_CHECK(s != NULL);
        moq_fetch_t h;
        moq_stream_ref_t ref = do_fetch(s, "video", &h, NULL);
        uint8_t er[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, er, sizeof(er));
        moq_d18_encode_request_error(&w, 0x4, 0, MOQ_BYTES_LITERAL("no"));
        /* Bytes first (no FIN): error surfaced, entry drains. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, er,
                moq_buf_writer_offset(&w), false, 1), (int)MOQ_OK);
        MOQ_TEST_CHECK(drain_kind(s, MOQ_EVENT_FETCH_ERROR) == 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT(
            (int)request_registry_find_by_streamref(s, ref).kind,
            (int)MOQ_REQ_FETCH);   /* still draining, not freed yet */
        /* FIN: drains and frees without closing the session. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT(
            (int)request_registry_find_by_streamref(s, ref).kind,
            (int)MOQ_REQ_NONE);
        moq_session_destroy(s);
    }

    /* == J. Publisher accept_fetch emits FETCH_OK on the bidi ========= */
    {
        moq_session_t *s = make_established_d18_server();
        MOQ_TEST_CHECK(s != NULL);
        uint8_t msg[128];
        size_t n = make_fetch(msg, sizeof(msg), 0, 1, "video");
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0xF300);
        moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1);
        moq_fetch_t h = { 0 };
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_REQUEST) h = ev.u.fetch_request.fetch;
            moq_event_cleanup(&ev);
        }
        moq_accept_fetch_cfg_t acc;
        moq_accept_fetch_cfg_init(&acc);
        acc.end_group = 10; acc.end_object = 0;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_fetch(s, h, &acc, 1),
                              (int)MOQ_OK);
        moq_action_t act;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_actions(s, &act, 1), 1);
        MOQ_TEST_CHECK_EQ_U64(act.kind, MOQ_ACTION_SEND_BIDI_STREAM);
        MOQ_TEST_CHECK_EQ_U64(act.u.send_bidi_stream.stream_ref._v, ref._v);
        MOQ_TEST_CHECK(!act.u.send_bidi_stream.fin);
        moq_buf_reader_t rr;
        moq_buf_reader_init(&rr, act.u.send_bidi_stream.data,
                            act.u.send_bidi_stream.len);
        moq_control_envelope_t env;
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_envelope(&rr, &env),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(env.msg_type, MOQ_D18_FETCH_OK);
        moq_action_cleanup(&act);
        moq_session_destroy(s);
    }

    /* == K. Publisher reject_fetch emits REQUEST_ERROR (fin), no ctrl = */
    {
        moq_session_t *s = make_established_d18_server();
        MOQ_TEST_CHECK(s != NULL);
        uint8_t msg[128];
        size_t n = make_fetch(msg, sizeof(msg), 0, 1, "video");
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0xF301);
        moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1);
        moq_fetch_t h = { 0 };
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_REQUEST) h = ev.u.fetch_request.fetch;
            moq_event_cleanup(&ev);
        }
        moq_reject_fetch_cfg_t rej;
        moq_reject_fetch_cfg_init(&rej);
        rej.error_code = (moq_request_error_t)0x4;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_reject_fetch(s, h, &rej, 1),
                              (int)MOQ_OK);
        moq_action_t act;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_actions(s, &act, 1), 1);
        MOQ_TEST_CHECK_EQ_U64(act.kind, MOQ_ACTION_SEND_BIDI_STREAM);
        MOQ_TEST_CHECK(act.u.send_bidi_stream.fin);
        moq_buf_reader_t rr;
        moq_buf_reader_init(&rr, act.u.send_bidi_stream.data,
                            act.u.send_bidi_stream.len);
        moq_control_envelope_t env;
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_envelope(&rr, &env),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(env.msg_type, MOQ_D18_REQUEST_ERROR);
        moq_action_cleanup(&act);
        moq_session_destroy(s);
    }

    /* == L. First fetch object referencing prior subgroup -> close ==== */
    {
        moq_session_t *s = make_established_d18_client();
        MOQ_TEST_CHECK(s != NULL);
        moq_fetch_t h;
        (void)do_fetch(s, "video", &h, NULL);   /* outbound fetch, request id 0 */
        uint8_t data[32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, data, sizeof(data));
        moq_d18_encode_fetch_header(&w, 0);     /* binds the data uni */
        /* flags 0x1D: group+object+priority + subgroup mode 0x01 (prior
         * subgroup) -> a first object referencing a prior field. */
        moq_buf_write_vi64(&w, 0x1D);
        (void)moq_session_on_data_bytes(s, moq_stream_ref_from_u64(0xFA01),
            data, moq_buf_writer_offset(&w), false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);
    }

    /* == M. Subgroup prior+1 overflow is rejected ===================== */
    {
        moq_session_t *s = make_established_d18_client();
        MOQ_TEST_CHECK(s != NULL);
        moq_fetch_t h;
        (void)do_fetch(s, "video", &h, NULL);
        uint8_t data[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, data, sizeof(data));
        moq_d18_encode_fetch_header(&w, 0);
        uint8_t prio = 0;
        /* Object 0: subgroup present = UINT64_MAX (flags 0x1F). */
        moq_buf_write_vi64(&w, 0x1F);
        moq_buf_write_vi64(&w, 0);              /* group delta (absolute 0) */
        moq_buf_write_vi64(&w, UINT64_MAX);     /* subgroup */
        moq_buf_write_vi64(&w, 0);              /* object delta (absolute 0) */
        moq_buf_write_raw(&w, &prio, 1);
        moq_buf_write_vi64(&w, 0);              /* payload len */
        /* Object 1: subgroup mode 0x02 (prior+1) would wrap past UINT64_MAX. */
        moq_buf_write_vi64(&w, 0x12);           /* priority + sg mode 0x02 */
        moq_buf_write_raw(&w, &prio, 1);
        moq_buf_write_vi64(&w, 0);              /* payload len */
        (void)moq_session_on_data_bytes(s, moq_stream_ref_from_u64(0xFA02),
            data, moq_buf_writer_offset(&w), false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);
    }

    /* == N. Leading range marker + object referencing its location === *
     * The object references the prior Location (from the marker) but not prior
     * subgroup/priority, so it must be accepted (group 5, object 3). */
    {
        moq_session_t *s = make_established_d18_client();
        MOQ_TEST_CHECK(s != NULL);
        moq_fetch_t h;
        (void)do_fetch(s, "video", &h, NULL);
        uint8_t data[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, data, sizeof(data));
        moq_d18_encode_fetch_header(&w, 0);
        /* End-of-Non-Existent-Range marker at {group 5, object 2}. */
        moq_buf_write_vi64(&w, 0x8C);
        moq_buf_write_vi64(&w, 5);
        moq_buf_write_vi64(&w, 2);
        /* Object: priority + subgroup present (0x13); no group/object delta, so
         * group = prior (5), object = prior + 1 (3). */
        uint8_t prio = 0;
        moq_buf_write_vi64(&w, 0x13);
        moq_buf_write_vi64(&w, 7);              /* subgroup present */
        moq_buf_write_raw(&w, &prio, 1);
        moq_buf_write_vi64(&w, 0);              /* payload len */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_data_bytes(s, moq_stream_ref_from_u64(0xFA03),
                data, moq_buf_writer_offset(&w), false, 1), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        bool gap = false, obj = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_GAP) gap = true;
            if (ev.kind == MOQ_EVENT_FETCH_OBJECT) {
                obj = true;
                MOQ_TEST_CHECK_EQ_U64(ev.u.fetch_object.group_id, 5);
                MOQ_TEST_CHECK_EQ_U64(ev.u.fetch_object.object_id, 3);
                MOQ_TEST_CHECK_EQ_U64(ev.u.fetch_object.subgroup_id, 7);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(gap);
        MOQ_TEST_CHECK(obj);
        moq_session_destroy(s);
    }

    /* == P. Fetch object with the Datagram forwarding-preference bit (0x40) ==
     * Decoded (not fail-closed) and surfaced: datagram=true, no subgroup. */
    {
        moq_session_t *s = make_established_d18_client();
        MOQ_TEST_CHECK(s != NULL);
        moq_fetch_t h; (void)do_fetch(s, "video", &h, NULL);
        uint8_t data[64]; moq_buf_writer_t w;
        moq_buf_writer_init(&w, data, sizeof(data));
        moq_d18_encode_fetch_header(&w, 0);
        uint8_t prio = 4;
        /* flags 0x5C = Datagram | priority | group delta | object delta (LSB 0). */
        moq_buf_write_vi64(&w, 0x5C);
        moq_buf_write_vi64(&w, 9);              /* group delta -> absolute 9 */
        moq_buf_write_vi64(&w, 0);              /* object delta -> absolute 0 */
        moq_buf_write_raw(&w, &prio, 1);
        moq_buf_write_vi64(&w, 0);              /* payload len */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_data_bytes(s, moq_stream_ref_from_u64(0xFA10),
                data, moq_buf_writer_offset(&w), false, 1), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        bool obj = false; moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_OBJECT) {
                obj = true;
                MOQ_TEST_CHECK(ev.u.fetch_object.datagram);
                MOQ_TEST_CHECK_EQ_U64(ev.u.fetch_object.group_id, 9);
                MOQ_TEST_CHECK_EQ_U64(ev.u.fetch_object.object_id, 0);
                MOQ_TEST_CHECK_EQ_U64(ev.u.fetch_object.subgroup_id, 0);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(obj);
        moq_session_destroy(s);
    }

    /* == Q. A normal (subgroup) fetch object surfaces datagram=false ==== */
    {
        moq_session_t *s = make_established_d18_client();
        MOQ_TEST_CHECK(s != NULL);
        moq_fetch_t h; (void)do_fetch(s, "video", &h, NULL);
        uint8_t data[64]; moq_buf_writer_t w;
        moq_buf_writer_init(&w, data, sizeof(data));
        moq_d18_encode_fetch_header(&w, 0);
        uint8_t prio = 0;
        /* flags 0x1F = priority | group delta | object delta | subgroup present. */
        moq_buf_write_vi64(&w, 0x1F);
        moq_buf_write_vi64(&w, 9);              /* group delta -> absolute 9 */
        moq_buf_write_vi64(&w, 7);              /* subgroup present */
        moq_buf_write_vi64(&w, 0);              /* object delta -> absolute 0 */
        moq_buf_write_raw(&w, &prio, 1);
        moq_buf_write_vi64(&w, 0);              /* payload len */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_data_bytes(s, moq_stream_ref_from_u64(0xFA11),
                data, moq_buf_writer_offset(&w), false, 1), (int)MOQ_OK);
        bool obj = false; moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_OBJECT) {
                obj = true;
                MOQ_TEST_CHECK(!ev.u.fetch_object.datagram);
                MOQ_TEST_CHECK_EQ_U64(ev.u.fetch_object.subgroup_id, 7);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(obj);
        moq_session_destroy(s);
    }

    /* == R. Datagram object: the two subgroup LSBs are ignored (§11.4.4.1) ==
     * 0x5D sets LSB 0x01 (would reference the prior Subgroup on a non-datagram
     * first object -> PROTO). With 0x40 the LSBs are ignored, so it is accepted
     * with subgroup 0. */
    {
        moq_session_t *s = make_established_d18_client();
        MOQ_TEST_CHECK(s != NULL);
        moq_fetch_t h; (void)do_fetch(s, "video", &h, NULL);
        uint8_t data[64]; moq_buf_writer_t w;
        moq_buf_writer_init(&w, data, sizeof(data));
        moq_d18_encode_fetch_header(&w, 0);
        uint8_t prio = 0;
        moq_buf_write_vi64(&w, 0x5D);           /* Datagram | priority | deltas | LSB 0x01 */
        moq_buf_write_vi64(&w, 9);
        moq_buf_write_vi64(&w, 0);
        moq_buf_write_raw(&w, &prio, 1);
        moq_buf_write_vi64(&w, 0);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_data_bytes(s, moq_stream_ref_from_u64(0xFA12),
                data, moq_buf_writer_offset(&w), false, 1), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        bool obj = false; moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_OBJECT) {
                obj = true;
                MOQ_TEST_CHECK(ev.u.fetch_object.datagram);
                MOQ_TEST_CHECK_EQ_U64(ev.u.fetch_object.subgroup_id, 0);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(obj);
        moq_session_destroy(s);
    }

    /* == S. Datagram object carrying Object Properties (0x40 | 0x20) ===== *
     * Field ordering is unchanged by the Datagram bit; properties still
     * decode and surface alongside datagram=true. */
    {
        moq_session_t *s = make_established_d18_client();
        MOQ_TEST_CHECK(s != NULL);
        moq_fetch_t h; (void)do_fetch(s, "video", &h, NULL);
        uint8_t data[64]; moq_buf_writer_t w;
        moq_buf_writer_init(&w, data, sizeof(data));
        moq_d18_encode_fetch_header(&w, 0);
        uint8_t prio = 0;
        moq_buf_write_vi64(&w, 0x7C);           /* Datagram | properties | priority | deltas */
        moq_buf_write_vi64(&w, 9);
        moq_buf_write_vi64(&w, 0);
        moq_buf_write_raw(&w, &prio, 1);
        uint8_t props[] = { 0x10, 0x00 };       /* one non-mandatory KVP (type 0x10, value 0) */
        moq_buf_write_vi64(&w, sizeof(props));
        moq_buf_write_raw(&w, props, sizeof(props));
        moq_buf_write_vi64(&w, 0);              /* payload len */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_data_bytes(s, moq_stream_ref_from_u64(0xFA13),
                data, moq_buf_writer_offset(&w), false, 1), (int)MOQ_OK);
        bool obj = false; moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_OBJECT) {
                obj = true;
                MOQ_TEST_CHECK(ev.u.fetch_object.datagram);
                MOQ_TEST_CHECK(ev.u.fetch_object.properties != NULL);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(obj);
        moq_session_destroy(s);
    }

    /* == Peer fetch teardown defers when the action queue is full ===== *
     *  A peer reset of an accepted fetch's request bidi must RESET the open
     *  response data uni. If the action queue is full, the teardown defers
     *  (WOULD_BLOCK) and keeps the entry so the bridge retries; it must never
     *  silently skip the data reset and free the fetch. */
    {
        moq_session_t *s = make_established_d18_server();
        MOQ_TEST_CHECK(s != NULL);
        uint8_t msg[128];
        size_t n = make_fetch(msg, sizeof(msg), 0, 1, "video");
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0xF400);
        moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1);
        moq_fetch_t h = { 0 };
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_REQUEST) h = ev.u.fetch_request.fetch;
            moq_event_cleanup(&ev);
        }
        moq_accept_fetch_cfg_t acc;
        moq_accept_fetch_cfg_init(&acc);
        acc.end_group = 10; acc.end_object = 0;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_fetch(s, h, &acc, 1),
                              (int)MOQ_OK);
        /* Drain the FETCH_OK / FETCH_HEADER actions, then fill the queue. */
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
        size_t filled = 0;
        while (!action_queue_full(s)) {
            moq_action_t f;
            memset(&f, 0, sizeof(f));
            f.kind = MOQ_ACTION_STOP_DATA;
            f.detail_size = (uint32_t)sizeof(moq_stop_data_action_t);
            f.borrow_epoch = s->borrow_epoch;
            f.u.stop_data.stream_ref = moq_stream_ref_from_u64(0x9000 + filled);
            if (push_action(s, &f) < 0) break;
            filled++;
        }
        MOQ_TEST_CHECK(filled > 0);

        /* Peer resets the request bidi: data uni open + action queue full ->
         * defer, entry retained. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_reset(s, ref, 0x1, 1),
            (int)MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT(
            (int)request_registry_find_by_streamref(s, ref).kind,
            (int)MOQ_REQ_FETCH);   /* not freed */

        /* Free a slot, retry: the teardown now completes with a data reset. */
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_actions(s, &a, 1), 1);
        moq_action_cleanup(&a);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_reset(s, ref, 0x1, 1), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(
            (int)request_registry_find_by_streamref(s, ref).kind,
            (int)MOQ_REQ_NONE);   /* freed */
        bool reset_data = false, cancelled = false;
        size_t na;
        moq_action_t acts[8];
        while ((na = moq_session_poll_actions(s, acts, 8)) > 0)
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_RESET_DATA) reset_data = true;
                moq_action_cleanup(&acts[i]);
            }
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_CANCELLED) cancelled = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(reset_data);
        MOQ_TEST_CHECK(cancelled);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Local fetch cancel drains a late response on the bidi ======== *
     *  After we cancel a pending fetch, a FETCH_OK the publisher sent before
     *  seeing our STOP arrives on the response bidi. It must be discarded (the
     *  bidi is draining), not mistaken for a new inbound request. */
    {
        moq_session_t *s = make_established_d18_client();
        MOQ_TEST_CHECK(s != NULL);
        moq_fetch_t h;
        moq_stream_ref_t ref = do_fetch(s, "video", &h, NULL);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch_cancel(s, h, 1),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
        /* The response-bidi stream ref is no longer registered as a request. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)request_registry_find_by_streamref(s, ref).kind,
            (int)MOQ_REQ_NONE);

        uint8_t ok[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, ok, sizeof(ok));
        moq_d18_encode_fetch_ok(&w, false, (moq_d18_location_t){ 10, 0 }, (moq_bytes_t){0});
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, ok,
                moq_buf_writer_offset(&w), false, 1), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        /* FIN retires the drain ref. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    MOQ_TEST_PASS("d18_fetch");
    return failures != 0;
}
