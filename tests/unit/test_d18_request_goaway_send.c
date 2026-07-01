/*
 * Draft-18 per-request GOAWAY send (§10.4): emit a GOAWAY on an active request
 * bidi to migrate that single request. Covers the family × state eligibility
 * matrix across all 7 families (responder role, single session), both roles via
 * SimPair (client requester + server responder, peer surfaces
 * MOQ_EVENT_REQUEST_GOAWAY), duplicate-send / wrong-state / track-status
 * requester / client-URI / real action-queue backpressure, and draft-16
 * rejection. The GOAWAY is queued with fin=false so the request bidi and its
 * data streams stay alive (graceful migration). The queued payload is decoded
 * (URI + Timeout) on every send-side check.
 */
#include <moq/sim.h>
#include <moq/control_d18.h>
#include "test_support.h"
#include "../../core/src/session/session_internal.h"

static int failures = 0;

static const moq_bytes_t k_live[1] = { { (const uint8_t *)"live", 4 } };
static const moq_namespace_t k_ns = { (moq_bytes_t *)k_live, 1 };

/* --- single-session helpers (responder establishment, no real transport) --- */

static moq_session_t *make_session_caps(moq_perspective_t persp,
                                        uint32_t max_events, uint32_t max_actions)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), persp);
    cfg.version = MOQ_VERSION_DRAFT_18;
    if (max_events) cfg.max_events = max_events;
    if (max_actions) cfg.max_actions = max_actions;
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
    moq_action_t a2;
    while (moq_session_poll_actions(s, &a2, 1) > 0) moq_action_cleanup(&a2);
    return s;
}

static moq_session_t *make_session(moq_perspective_t persp)
{
    return make_session_caps(persp, 0, 0);
}

static void drain_actions(moq_session_t *s)
{
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
}

/* Drain all queued actions and count the per-request GOAWAYs on `ref`, decoding
 * each (URI length + Timeout) and asserting no FIN (the bidi stays alive). */
static int check_goaway_sent(moq_session_t *s, moq_stream_ref_t ref,
                             size_t want_uri_len, uint64_t want_timeout)
{
    int n = 0;
    moq_action_t act;
    while (moq_session_poll_actions(s, &act, 1) > 0) {
        if (act.kind == MOQ_ACTION_SEND_BIDI_STREAM &&
            act.u.send_bidi_stream.stream_ref._v == ref._v) {
            moq_buf_reader_t rr;
            moq_buf_reader_init(&rr, act.u.send_bidi_stream.data,
                                act.u.send_bidi_stream.len);
            moq_control_envelope_t env;
            if (moq_d18_decode_envelope(&rr, &env) == MOQ_OK &&
                env.msg_type == MOQ_D18_GOAWAY) {
                n++;
                MOQ_TEST_CHECK(!act.u.send_bidi_stream.fin);  /* keep alive */
                moq_d18_goaway_t ga;
                MOQ_TEST_CHECK_EQ_INT(
                    (int)moq_d18_decode_goaway_request(env.payload,
                        env.payload_len, &ga), (int)MOQ_OK);
                MOQ_TEST_CHECK_EQ_SIZE(ga.uri.len, want_uri_len);
                MOQ_TEST_CHECK_EQ_U64(ga.timeout_ms, want_timeout);
                MOQ_TEST_CHECK_EQ_U64(ga.request_id, 0);   /* omitted */
            }
        }
        moq_action_cleanup(&act);
    }
    return n;
}

/* A server's GOAWAY carries a real New Session URI; clients must send none. */
#define SRV_URI "https://relay2"          /* len 14 */
#define SRV_URI_LEN 14u
#define SRV_TIMEOUT 3000u

static void goaway_cfg_server(moq_request_goaway_cfg_t *gc)
{
    moq_request_goaway_cfg_init(gc);
    gc->new_session_uri = MOQ_BYTES_LITERAL(SRV_URI);
    gc->timeout_ms = SRV_TIMEOUT;
}

/* Establish a SUBSCRIBE responder (server is the publisher), accept it, emit a
 * GOAWAY, and return the (still live, goaway-marked) request bidi ref + handle. */
static moq_session_t *sub_after_goaway(moq_stream_ref_t *out_ref,
                                       moq_subscription_t *out_h)
{
    moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
    moq_d18_msg_params_t mp = {0};
    uint8_t m[160]; moq_buf_writer_t w;
    moq_buf_writer_init(&w, m, sizeof(m));
    moq_d18_encode_subscribe(&w, 0, &k_ns, MOQ_BYTES_LITERAL("v"), &mp);
    moq_stream_ref_t ref = moq_stream_ref_from_u64(0x6200);
    moq_session_on_bidi_stream_bytes(s, ref, m, moq_buf_writer_offset(&w), false, 1);
    moq_subscription_t h = MOQ_SUBSCRIPTION_INVALID;
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) h = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
    }
    moq_accept_subscribe_cfg_t ac; moq_accept_subscribe_cfg_init(&ac);
    moq_session_accept_subscribe(s, h, &ac, 1);
    drain_actions(s);
    moq_request_goaway_cfg_t gc; goaway_cfg_server(&gc);
    moq_session_request_goaway_subscribe(s, h, &gc, 1);
    drain_actions(s);
    *out_ref = ref;
    if (out_h) *out_h = h;
    return s;
}

/* Publisher-side NS_SUB (server accepts an inbound SUBSCRIBE_NAMESPACE) that has
 * sent a GOAWAY; the bidi is keyed on idx_ns_by_ref, not the stream-ref registry. */
static moq_session_t *ns_pub_after_goaway(moq_stream_ref_t *out_ref)
{
    moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
    moq_d18_msg_params_t mp = {0};
    uint8_t m[160]; moq_buf_writer_t w;
    moq_buf_writer_init(&w, m, sizeof(m));
    moq_d18_encode_subscribe_namespace(&w, 0, &k_ns, &mp);
    moq_stream_ref_t ref = moq_stream_ref_from_u64(0x6400);
    moq_session_on_bidi_stream_bytes(s, ref, m, moq_buf_writer_offset(&w), false, 1);
    moq_ns_sub_handle_t h = {0};
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_NS_SUB_REQUEST) h = ev.u.ns_sub_request.handle;
        moq_event_cleanup(&ev);
    }
    moq_accept_ns_sub_cfg_t ac; moq_accept_ns_sub_cfg_init(&ac);
    moq_session_accept_ns_sub(s, h, &ac, 1);
    drain_actions(s);
    moq_request_goaway_cfg_t gc; goaway_cfg_server(&gc);
    moq_session_request_goaway_ns_sub(s, h, &gc, 1);
    drain_actions(s);
    *out_ref = ref;
    return s;
}

/* Subscriber-side NS_SUB (we opened SUBSCRIBE_NAMESPACE, peer answered REQUEST_OK)
 * that has sent a GOAWAY (empty URI, since we are the client). */
static moq_session_t *ns_subscriber_after_goaway(moq_stream_ref_t *out_ref)
{
    moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
    moq_subscribe_namespace_cfg_t cfg; moq_subscribe_namespace_cfg_init(&cfg);
    cfg.track_namespace_prefix = k_ns;
    cfg.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
    moq_ns_sub_handle_t h;
    moq_session_subscribe_namespace(s, &cfg, 1, &h);
    moq_stream_ref_t ref = moq_stream_ref_from_u64(0);
    moq_action_t act;
    while (moq_session_poll_actions(s, &act, 1) > 0) {
        if (act.kind == MOQ_ACTION_OPEN_BIDI_STREAM)
            ref = act.u.open_bidi_stream.stream_ref;
        moq_action_cleanup(&act);
    }
    uint8_t ok[32]; moq_buf_writer_t w; moq_buf_writer_init(&w, ok, sizeof(ok));
    moq_d18_encode_request_ok(&w);
    moq_session_on_bidi_stream_bytes(s, ref, ok, moq_buf_writer_offset(&w), false, 1);
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) moq_event_cleanup(&ev);
    moq_request_goaway_cfg_t gc; moq_request_goaway_cfg_init(&gc);   /* client: empty URI */
    moq_session_request_goaway_ns_sub(s, h, &gc, 1);
    drain_actions(s);
    *out_ref = ref;
    return s;
}

/* --- SimPair helpers (real transport, both roles) ------------------------ */

static moq_simpair_t *make_pair(moq_version_t version)
{
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = moq_alloc_default();
    cfg.version = version;
    /* Grant request-id budget so a draft-16 client may subscribe (draft-18
     * has no MAX_REQUEST_ID and ignores these). */
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 10;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 10;
    moq_simpair_t *sp = NULL;
    if (moq_simpair_create(&cfg, &sp) < 0) return NULL;
    return sp;
}

/* Establish a subscription, returning both endpoints' subscription handles. */
static bool sim_setup_sub(moq_simpair_t *sp, moq_subscription_t *out_client,
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
    sub.track_namespace = k_ns;
    sub.track_name = MOQ_BYTES_LITERAL("v");
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
    if (moq_session_accept_subscribe(server, sh, &acc,
                                     moq_simpair_now_us(sp)) != MOQ_OK)
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

/* Poll `s` for a request-GOAWAY event; return true and fill uri_len + timeout. */
static bool poll_request_goaway(moq_session_t *s, moq_request_family_t fam,
                                size_t *uri_len, uint64_t *timeout)
{
    bool got = false;
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_REQUEST_GOAWAY &&
            ev.u.request_goaway.family == fam) {
            got = true;
            if (uri_len) *uri_len = ev.u.request_goaway.new_session_uri.len;
            if (timeout) *timeout = ev.u.request_goaway.timeout_ms;
        }
        moq_event_cleanup(&ev);
    }
    return got;
}

int main(void)
{
    moq_d18_msg_params_t mp = {0};
    uint8_t msg[160];
    moq_buf_writer_t w;
    moq_request_goaway_cfg_t gc;

    /* == Per-family responder send (all 7 families, server role) ======== *
     *  Feed an inbound request, reach the family's eligible state, emit a
     *  GOAWAY carrying a server URI, and decode it off the queued action. */

    /* SUBSCRIBE (ESTABLISHED) — also duplicate-send and wrong-state checks. */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_buf_writer_init(&w, msg, sizeof(msg));
        moq_d18_encode_subscribe(&w, 0, &k_ns, MOQ_BYTES_LITERAL("v"), &mp);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0x6001);
        moq_session_on_bidi_stream_bytes(s, ref, msg, moq_buf_writer_offset(&w),
                                         false, 1);
        moq_subscription_t h = MOQ_SUBSCRIPTION_INVALID;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) h = ev.u.subscribe_request.sub;
            moq_event_cleanup(&ev);
        }
        /* Wrong state: a still-pending request (not yet accepted) is ineligible. */
        goaway_cfg_server(&gc);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_request_goaway_subscribe(s, h, &gc, 1),
            (int)MOQ_ERR_WRONG_STATE);

        moq_accept_subscribe_cfg_t ac; moq_accept_subscribe_cfg_init(&ac);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_subscribe(s, h, &ac, 1),
                              (int)MOQ_OK);
        drain_actions(s);   /* discard the SUBSCRIBE_OK */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_request_goaway_subscribe(s, h, &gc, 1), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(check_goaway_sent(s, ref, SRV_URI_LEN, SRV_TIMEOUT), 1);
        /* §10.4: GOAWAY does not impact subscription state — the entry stays live so
         * the publisher keeps producing. A second send ⇒ WRONG_STATE (one GOAWAY
         * per request stream). */
        MOQ_TEST_CHECK(request_registry_find_by_streamref(s, ref).kind ==
                       MOQ_REQ_SUBSCRIPTION);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_request_goaway_subscribe(s, h, &gc, 1),
            (int)MOQ_ERR_WRONG_STATE);
        MOQ_TEST_CHECK_EQ_INT(check_goaway_sent(s, ref, SRV_URI_LEN, SRV_TIMEOUT), 0);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* FETCH (ACCEPTED). */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_buf_writer_init(&w, msg, sizeof(msg));
        moq_d18_fetch_t f; memset(&f, 0, sizeof(f));
        f.request_id = 0; f.fetch_type = MOQ_D18_FETCH_TYPE_STANDALONE;
        f.track_namespace = k_ns; f.track_name = MOQ_BYTES_LITERAL("v");
        f.start = (moq_d18_location_t){0, 0}; f.end = (moq_d18_location_t){10, 0};
        moq_d18_encode_fetch(&w, &f);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0x6002);
        moq_session_on_bidi_stream_bytes(s, ref, msg, moq_buf_writer_offset(&w),
                                         false, 1);
        moq_fetch_t h = {0};
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_REQUEST) h = ev.u.fetch_request.fetch;
            moq_event_cleanup(&ev);
        }
        moq_accept_fetch_cfg_t ac; moq_accept_fetch_cfg_init(&ac); ac.end_group = 10;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_fetch(s, h, &ac, 1), (int)MOQ_OK);
        drain_actions(s);
        goaway_cfg_server(&gc);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_request_goaway_fetch(s, h, &gc, 1), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(check_goaway_sent(s, ref, SRV_URI_LEN, SRV_TIMEOUT), 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* TRACK_STATUS (responder, PENDING_PUBLISHER — requester opened with FIN). */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_buf_writer_init(&w, msg, sizeof(msg));
        moq_d18_encode_track_status(&w, 0, &k_ns, MOQ_BYTES_LITERAL("v"), &mp);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0x6003);
        moq_session_on_bidi_stream_bytes(s, ref, msg, moq_buf_writer_offset(&w),
                                         true, 1);   /* requester FINs its half */
        moq_track_status_handle_t h = {0};
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_TRACK_STATUS_REQUEST) h = ev.u.track_status_request.handle;
            moq_event_cleanup(&ev);
        }
        goaway_cfg_server(&gc);   /* eligible before answering */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_request_goaway_track_status(s, h, &gc, 1), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(check_goaway_sent(s, ref, SRV_URI_LEN, SRV_TIMEOUT), 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* ANNOUNCEMENT (ESTABLISHED). */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_buf_writer_init(&w, msg, sizeof(msg));
        moq_d18_encode_publish_namespace(&w, 0, &k_ns, &mp);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0x6004);
        moq_session_on_bidi_stream_bytes(s, ref, msg, moq_buf_writer_offset(&w),
                                         false, 1);
        moq_announcement_t h = {0};
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED) h = ev.u.namespace_published.ann;
            moq_event_cleanup(&ev);
        }
        moq_accept_namespace_cfg_t ac; moq_accept_namespace_cfg_init(&ac);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_namespace(s, h, &ac, 1), (int)MOQ_OK);
        drain_actions(s);
        goaway_cfg_server(&gc);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_request_goaway_namespace(s, h, &gc, 1), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(check_goaway_sent(s, ref, SRV_URI_LEN, SRV_TIMEOUT), 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* NS_SUB (ESTABLISHED). */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_buf_writer_init(&w, msg, sizeof(msg));
        moq_d18_encode_subscribe_namespace(&w, 0, &k_ns, &mp);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0x6005);
        moq_session_on_bidi_stream_bytes(s, ref, msg, moq_buf_writer_offset(&w),
                                         false, 1);
        moq_ns_sub_handle_t h = {0};
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_NS_SUB_REQUEST) h = ev.u.ns_sub_request.handle;
            moq_event_cleanup(&ev);
        }
        moq_accept_ns_sub_cfg_t ac; moq_accept_ns_sub_cfg_init(&ac);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_ns_sub(s, h, &ac, 1), (int)MOQ_OK);
        drain_actions(s);
        goaway_cfg_server(&gc);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_request_goaway_ns_sub(s, h, &gc, 1), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(check_goaway_sent(s, ref, SRV_URI_LEN, SRV_TIMEOUT), 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* PUBLISH (ESTABLISHED) — server is the subscriber receiving a PUBLISH. */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_buf_writer_init(&w, msg, sizeof(msg));
        moq_d18_publish_t p; memset(&p, 0, sizeof(p));
        p.request_id = 0; p.track_namespace = k_ns;
        p.track_name = MOQ_BYTES_LITERAL("v"); p.track_alias = 7;
        moq_d18_encode_publish(&w, &p);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0x6006);
        moq_session_on_bidi_stream_bytes(s, ref, msg, moq_buf_writer_offset(&w),
                                         false, 1);
        moq_publication_t h = {0};
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_PUBLISH_REQUEST) h = ev.u.publish_request.pub;
            moq_event_cleanup(&ev);
        }
        moq_accept_publish_cfg_t ac; moq_accept_publish_cfg_init(&ac);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_publish(s, h, &ac, 1), (int)MOQ_OK);
        drain_actions(s);
        goaway_cfg_server(&gc);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_request_goaway_publish(s, h, &gc, 1), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(check_goaway_sent(s, ref, SRV_URI_LEN, SRV_TIMEOUT), 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* SUBSCRIBE_TRACKS (ESTABLISHED). */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_buf_writer_init(&w, msg, sizeof(msg));
        moq_d18_encode_subscribe_tracks(&w, 0, &k_ns, &mp);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0x6007);
        moq_session_on_bidi_stream_bytes(s, ref, msg, moq_buf_writer_offset(&w),
                                         false, 1);
        moq_track_sub_handle_t h = {0};
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_TRACKS_REQUEST) h = ev.u.subscribe_tracks_request.handle;
            moq_event_cleanup(&ev);
        }
        moq_accept_subscribe_tracks_cfg_t ac; moq_accept_subscribe_tracks_cfg_init(&ac);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_accept_subscribe_tracks(s, h, &ac, 1), (int)MOQ_OK);
        drain_actions(s);
        goaway_cfg_server(&gc);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_request_goaway_subscribe_tracks(s, h, &gc, 1), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(check_goaway_sent(s, ref, SRV_URI_LEN, SRV_TIMEOUT), 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == After send: the request stays live, then the peer's old-stream teardown == *
     *  §10.4 "does not impact subscription state": the publisher keeps producing on
     *  the old request after sending the GOAWAY. The recipient then closes the old
     *  request stream (FIN / reset / STOP); the sender absorbs that without closing
     *  the session or surfacing a cancellation/error event. A duplicate GOAWAY on
     *  the stream still closes 0x3. */
    {   /* the publisher can still open subgroups + write objects after GOAWAY */
        moq_stream_ref_t ref; moq_subscription_t h;
        moq_session_t *s = sub_after_goaway(&ref, &h);
        MOQ_TEST_CHECK(request_registry_find_by_streamref(s, ref).kind ==
                       MOQ_REQ_SUBSCRIPTION);   /* entry still live */
        moq_subgroup_cfg_t sc; moq_subgroup_cfg_init(&sc);
        sc.group_id = 1; sc.subgroup_id = 0;
        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_open_subgroup(s, h, &sc, 1, &sg), (int)MOQ_OK);
        const uint8_t payload[3] = { 1, 2, 3 };
        moq_rcbuf_t *obj = NULL;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_rcbuf_create(moq_alloc_default(), payload, sizeof(payload), &obj),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_write_object(s, sg, 0, obj, 1), (int)MOQ_OK);
        moq_rcbuf_decref(obj);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }
    {   /* NS_SUB: send_namespace (a NAMESPACE on the request bidi) is GATED after
         * GOAWAY — the peer treats GOAWAY as terminal, so no further request-bidi
         * message may be written. (NS_SUB has no separate data plane.) */
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_d18_msg_params_t mp2 = {0};
        uint8_t m[160]; moq_buf_writer_t w2;
        moq_buf_writer_init(&w2, m, sizeof(m));
        moq_d18_encode_subscribe_namespace(&w2, 0, &k_ns, &mp2);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0x6300);
        moq_session_on_bidi_stream_bytes(s, ref, m, moq_buf_writer_offset(&w2), false, 1);
        moq_ns_sub_handle_t h = {0};
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_NS_SUB_REQUEST) h = ev.u.ns_sub_request.handle;
            moq_event_cleanup(&ev);
        }
        moq_accept_ns_sub_cfg_t ac; moq_accept_ns_sub_cfg_init(&ac);
        moq_session_accept_ns_sub(s, h, &ac, 1);
        drain_actions(s);
        /* send_namespace BEFORE GOAWAY works; after GOAWAY it is gated. */
        moq_bytes_t sfx[] = { MOQ_BYTES_LITERAL("room=1") };
        moq_namespace_t suffix = { sfx, 1 };
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_send_namespace(s, h, &suffix, 1), (int)MOQ_OK);
        drain_actions(s);
        moq_request_goaway_cfg_t gc2; goaway_cfg_server(&gc2);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_request_goaway_ns_sub(s, h, &gc2, 1), (int)MOQ_OK);
        drain_actions(s);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_send_namespace(s, h, &suffix, 1), (int)MOQ_ERR_WRONG_STATE);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }
    {   /* PUBLISH publisher can still open_pub_subgroup after GOAWAY (SimPair) */
        moq_simpair_t *sp = make_pair(MOQ_VERSION_DRAFT_18);
        MOQ_TEST_CHECK(sp != NULL);
        moq_session_t *client = moq_simpair_client(sp);
        moq_session_t *server = moq_simpair_server(sp);
        MOQ_TEST_CHECK_EQ_INT((int)moq_simpair_start(sp), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        moq_publish_cfg_t pc; moq_publish_cfg_init(&pc);
        pc.track_namespace = k_ns; pc.track_name = MOQ_BYTES_LITERAL("v");
        moq_publication_t spub;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_publish(server, &pc, moq_simpair_now_us(sp), &spub),
            (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        moq_publication_t cpub = {0}; moq_event_t ev;
        while (moq_session_poll_events(client, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_PUBLISH_REQUEST) cpub = ev.u.publish_request.pub;
            moq_event_cleanup(&ev);
        }
        moq_accept_publish_cfg_t pac; moq_accept_publish_cfg_init(&pac);
        moq_session_accept_publish(client, cpub, &pac, moq_simpair_now_us(sp));
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        while (moq_session_poll_events(server, &ev, 1) > 0) moq_event_cleanup(&ev);
        /* migrate the publish, then keep producing on the old request */
        moq_request_goaway_cfg_t gc3; goaway_cfg_server(&gc3);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_request_goaway_publish(server, spub, &gc3,
                moq_simpair_now_us(sp)), (int)MOQ_OK);
        moq_subgroup_cfg_t sc; moq_subgroup_cfg_init(&sc);
        sc.group_id = 1; sc.subgroup_id = 0;
        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_open_pub_subgroup(server, spub, &sc,
                moq_simpair_now_us(sp), &sg), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)server->state, (int)MOQ_SESS_ESTABLISHED);
        moq_simpair_destroy(sp);
    }
    {   /* TRACK_STATUS: accept (TRACK_STATUS_OK on the request bidi) is GATED after
         * GOAWAY — the reviewer's concrete case. The entry stays PENDING_PUBLISHER
         * but no further request-bidi message may be written. */
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_d18_msg_params_t mp2 = {0};
        uint8_t m[160]; moq_buf_writer_t w2; moq_buf_writer_init(&w2, m, sizeof(m));
        moq_d18_encode_track_status(&w2, 0, &k_ns, MOQ_BYTES_LITERAL("v"), &mp2);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0x6500);
        moq_session_on_bidi_stream_bytes(s, ref, m, moq_buf_writer_offset(&w2), true, 1);
        moq_track_status_handle_t h = {0}; moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_TRACK_STATUS_REQUEST) h = ev.u.track_status_request.handle;
            moq_event_cleanup(&ev);
        }
        moq_request_goaway_cfg_t gc2; goaway_cfg_server(&gc2);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_request_goaway_track_status(s, h, &gc2, 1), (int)MOQ_OK);
        drain_actions(s);
        moq_accept_track_status_cfg_t tac; moq_accept_track_status_cfg_init(&tac);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_accept_track_status(s, h, &tac, 1), (int)MOQ_ERR_WRONG_STATE);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }
    {   /* SUBSCRIBE: done_subscribe (PUBLISH_DONE via queue_subscribe_response, a
         * direct SEND_BIDI site distinct from queue_send_bidi) is GATED after GOAWAY,
         * and queues nothing. */
        moq_stream_ref_t ref; moq_subscription_t h;
        moq_session_t *s = sub_after_goaway(&ref, &h);
        moq_done_subscribe_cfg_t dc; moq_done_subscribe_cfg_init(&dc);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_done_subscribe(s, h, &dc, 1), (int)MOQ_ERR_WRONG_STATE);
        /* nothing was queued on the request bidi */
        int sends = 0; moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) {
            if (a.kind == MOQ_ACTION_SEND_BIDI_STREAM &&
                a.u.send_bidi_stream.stream_ref._v == ref._v) sends++;
            moq_action_cleanup(&a);
        }
        MOQ_TEST_CHECK_EQ_INT(sends, 0);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }
    {   /* Contract: after GOAWAY the gate runs BEFORE capacity preflight, so a
         * post-GOAWAY done_subscribe is WRONG_STATE even with the action queue full
         * (not WOULD_BLOCK). 1-slot queue, GOAWAY action left undrained. */
        moq_session_t *s = make_session_caps(MOQ_PERSPECTIVE_SERVER, 0, 1);
        moq_d18_msg_params_t mp2 = {0};
        uint8_t m[160]; moq_buf_writer_t w2; moq_buf_writer_init(&w2, m, sizeof(m));
        moq_d18_encode_subscribe(&w2, 0, &k_ns, MOQ_BYTES_LITERAL("v"), &mp2);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0x6600);
        moq_session_on_bidi_stream_bytes(s, ref, m, moq_buf_writer_offset(&w2), false, 1);
        moq_subscription_t h = MOQ_SUBSCRIPTION_INVALID; moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) h = ev.u.subscribe_request.sub;
            moq_event_cleanup(&ev);
        }
        moq_accept_subscribe_cfg_t ac; moq_accept_subscribe_cfg_init(&ac);
        moq_session_accept_subscribe(s, h, &ac, 1);
        drain_actions(s);
        moq_request_goaway_cfg_t gc2; goaway_cfg_server(&gc2);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_request_goaway_subscribe(s, h, &gc2, 1), (int)MOQ_OK);
        /* leave the GOAWAY action undrained: the 1-slot action queue is now full */
        moq_done_subscribe_cfg_t dc; moq_done_subscribe_cfg_init(&dc);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_done_subscribe(s, h, &dc, 1), (int)MOQ_ERR_WRONG_STATE);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }
    {   /* empty FIN silently retires the migrated request */
        moq_stream_ref_t ref; moq_session_t *s = sub_after_goaway(&ref, NULL);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK(request_registry_find_by_streamref(s, ref).kind ==
                       MOQ_REQ_NONE);   /* now retired */
        moq_event_t ev; bool spurious = false;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_DONE ||
                ev.kind == MOQ_EVENT_SUBSCRIBE_ERROR) spurious = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(!spurious);   /* no normal cancellation/error event */
        moq_session_destroy(s);
    }
    {   /* peer RESET silently retires it */
        moq_stream_ref_t ref; moq_session_t *s = sub_after_goaway(&ref, NULL);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_reset(s, ref, 0, 1), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK(request_registry_find_by_streamref(s, ref).kind == MOQ_REQ_NONE);
        moq_session_destroy(s);
    }
    {   /* peer STOP_SENDING silently retires it */
        moq_stream_ref_t ref; moq_session_t *s = sub_after_goaway(&ref, NULL);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_stop(s, ref, 0, 1), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK(request_registry_find_by_streamref(s, ref).kind == MOQ_REQ_NONE);
        moq_session_destroy(s);
    }
    {   /* a second (inbound) GOAWAY on the migrated stream ⇒ 0x3 */
        moq_stream_ref_t ref; moq_session_t *s = sub_after_goaway(&ref, NULL);
        uint8_t g[64]; moq_buf_writer_t gw; moq_buf_writer_init(&gw, g, sizeof(g));
        moq_d18_encode_goaway_request(&gw, NULL, 0, 0);
        (void)moq_session_on_bidi_stream_bytes(s, ref, g,
            moq_buf_writer_offset(&gw), false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);
    }

    /* == NS_SUB teardown/duplicate — keyed on idx_ns_by_ref, both roles ==== *
     *  NS_SUB request bidis are not in the stream-ref registry, so the GOAWAY
     *  marker lookup must resolve them via idx_ns_by_ref. */
    {   /* publisher-side: empty FIN silently retires the migrated ns_sub */
        moq_stream_ref_t ref; moq_session_t *s = ns_pub_after_goaway(&ref);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }
    {   /* publisher-side: peer RESET silently retires it */
        moq_stream_ref_t ref; moq_session_t *s = ns_pub_after_goaway(&ref);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_reset(s, ref, 0, 1), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }
    {   /* publisher-side: a duplicate inbound GOAWAY ⇒ 0x3 */
        moq_stream_ref_t ref; moq_session_t *s = ns_pub_after_goaway(&ref);
        uint8_t g[64]; moq_buf_writer_t gw; moq_buf_writer_init(&gw, g, sizeof(g));
        moq_d18_encode_goaway_request(&gw, NULL, 0, 0);
        (void)moq_session_on_bidi_stream_bytes(s, ref, g,
            moq_buf_writer_offset(&gw), false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);
    }
    {   /* subscriber-side: empty FIN silently retires (no leak) */
        moq_stream_ref_t ref; moq_session_t *s = ns_subscriber_after_goaway(&ref);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }
    {   /* subscriber-side: a duplicate inbound GOAWAY ⇒ 0x3 */
        moq_stream_ref_t ref; moq_session_t *s = ns_subscriber_after_goaway(&ref);
        uint8_t g[64]; moq_buf_writer_t gw; moq_buf_writer_init(&gw, g, sizeof(g));
        moq_d18_encode_goaway_request(&gw, NULL, 0, 0);
        (void)moq_session_on_bidi_stream_bytes(s, ref, g,
            moq_buf_writer_offset(&gw), false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);
    }

    /* == Slot reuse: the goaway_sent marker must not survive a selective free == *
     *  After a migrated request is freed (peer FIN), a new request reusing the same
     *  pool slot must NOT be seen as already migrated. */
    {   /* SUBSCRIBE: a fresh request on the recycled slot accepts a new GOAWAY */
        moq_stream_ref_t ref; moq_subscription_t h0;
        moq_session_t *s = sub_after_goaway(&ref, &h0);
        moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1);   /* free slot */
        MOQ_TEST_CHECK(request_registry_find_by_streamref(s, ref).kind == MOQ_REQ_NONE);
        moq_d18_msg_params_t mp2 = {0};
        uint8_t m[160]; moq_buf_writer_t w2; moq_buf_writer_init(&w2, m, sizeof(m));
        moq_d18_encode_subscribe(&w2, 2, &k_ns, MOQ_BYTES_LITERAL("v2"), &mp2);
        moq_stream_ref_t ref2 = moq_stream_ref_from_u64(0x6210);
        moq_session_on_bidi_stream_bytes(s, ref2, m, moq_buf_writer_offset(&w2), false, 1);
        moq_subscription_t h2 = MOQ_SUBSCRIPTION_INVALID; moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) h2 = ev.u.subscribe_request.sub;
            moq_event_cleanup(&ev);
        }
        moq_accept_subscribe_cfg_t ac; moq_accept_subscribe_cfg_init(&ac);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_subscribe(s, h2, &ac, 1), (int)MOQ_OK);
        drain_actions(s);
        moq_request_goaway_cfg_t gc2; goaway_cfg_server(&gc2);
        MOQ_TEST_CHECK_EQ_INT(   /* WRONG_STATE here would mean a leaked marker */
            (int)moq_session_request_goaway_subscribe(s, h2, &gc2, 1), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(check_goaway_sent(s, ref2, SRV_URI_LEN, SRV_TIMEOUT), 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }
    {   /* NS_SUB: a fresh request on the recycled slot is not seen as migrated *
         *  (handle_bidi_stream_bytes branches on goaway_sent) */
        moq_stream_ref_t ref; moq_session_t *s = ns_pub_after_goaway(&ref);
        moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1);   /* free slot */
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_d18_msg_params_t mp2 = {0};
        uint8_t m[160]; moq_buf_writer_t w2; moq_buf_writer_init(&w2, m, sizeof(m));
        moq_bytes_t p2[] = { MOQ_BYTES_LITERAL("live2") };
        moq_namespace_t ns2 = { p2, 1 };
        moq_d18_encode_subscribe_namespace(&w2, 2, &ns2, &mp2);
        moq_stream_ref_t ref2 = moq_stream_ref_from_u64(0x6410);
        moq_session_on_bidi_stream_bytes(s, ref2, m, moq_buf_writer_offset(&w2), false, 1);
        moq_ns_sub_handle_t h2 = {0}; moq_event_t ev; bool got = false;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_NS_SUB_REQUEST) { h2 = ev.u.ns_sub_request.handle; got = true; }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got);
        moq_accept_ns_sub_cfg_t ac; moq_accept_ns_sub_cfg_init(&ac);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_ns_sub(s, h2, &ac, 1), (int)MOQ_OK);
        drain_actions(s);
        /* If goaway_sent leaked, this fresh GOAWAY would be WRONG_STATE. */
        moq_request_goaway_cfg_t gc2; goaway_cfg_server(&gc2);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_request_goaway_ns_sub(s, h2, &gc2, 1), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == TRACK_STATUS requester may not migrate (opened with FIN) ======= *
     *  The requester's send half is already closed, so its handle is ineligible. */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
        moq_track_status_cfg_t cfg; moq_track_status_cfg_init(&cfg);
        cfg.track_namespace = k_ns; cfg.track_name = MOQ_BYTES_LITERAL("v");
        moq_track_status_handle_t h;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_track_status(s, &cfg, 1, &h), (int)MOQ_OK);
        drain_actions(s);
        moq_request_goaway_cfg_init(&gc);   /* empty URI: reach the state check */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_request_goaway_track_status(s, h, &gc, 1),
            (int)MOQ_ERR_WRONG_STATE);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Real backpressure: action queue full ⇒ WOULD_BLOCK, then retry == *
     *  With a 1-slot action queue holding the (undrained) SUBSCRIBE_OK, the
     *  GOAWAY cannot reserve its action slot: WOULD_BLOCK, nothing mutated. After
     *  draining it retries and emits exactly one GOAWAY. */
    {
        moq_session_t *s = make_session_caps(MOQ_PERSPECTIVE_SERVER, 0, 1);
        moq_buf_writer_init(&w, msg, sizeof(msg));
        moq_d18_encode_subscribe(&w, 0, &k_ns, MOQ_BYTES_LITERAL("v"), &mp);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0x6101);
        moq_session_on_bidi_stream_bytes(s, ref, msg, moq_buf_writer_offset(&w),
                                         false, 1);
        moq_subscription_t h = MOQ_SUBSCRIPTION_INVALID;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) h = ev.u.subscribe_request.sub;
            moq_event_cleanup(&ev);
        }
        moq_accept_subscribe_cfg_t ac; moq_accept_subscribe_cfg_init(&ac);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_subscribe(s, h, &ac, 1),
                              (int)MOQ_OK);   /* queues SUBSCRIBE_OK; queue now full */
        goaway_cfg_server(&gc);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_request_goaway_subscribe(s, h, &gc, 1),
            (int)MOQ_ERR_WOULD_BLOCK);
        /* Nothing mutated: the entry is intact (still established, not retired). */
        MOQ_TEST_CHECK_EQ_INT((int)s->subs[0].state, (int)MOQ_SUB_ESTABLISHED);
        MOQ_TEST_CHECK(request_registry_find_by_streamref(s, ref).kind ==
                       MOQ_REQ_SUBSCRIPTION);
        drain_actions(s);   /* free the slot (discard SUBSCRIBE_OK) */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_request_goaway_subscribe(s, h, &gc, 1), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(check_goaway_sent(s, ref, SRV_URI_LEN, SRV_TIMEOUT), 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Both roles end-to-end (SimPair): full round-trip stays clean ===== *
     *  A real client→server (empty URI) and server→client (server URI) send,
     *  each delivered over the transport pair. The receiver surfaces
     *  MOQ_EVENT_REQUEST_GOAWAY and, per §10.4, closes the old request stream
     *  (FIN). The sender retires its request on send (strict drain), so that
     *  returning FIN is absorbed: BOTH endpoints stay ESTABLISHED. */

    /* Client requester role: empty URI succeeds, non-empty ⇒ INVAL. */
    {
        moq_simpair_t *sp = make_pair(MOQ_VERSION_DRAFT_18);
        MOQ_TEST_CHECK(sp != NULL);
        moq_session_t *client = moq_simpair_client(sp);
        moq_session_t *server = moq_simpair_server(sp);
        moq_subscription_t csub, ssub;
        MOQ_TEST_CHECK(sim_setup_sub(sp, &csub, &ssub));

        /* A client must leave the New Session URI empty (§10.4). */
        goaway_cfg_server(&gc);   /* non-empty URI */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_request_goaway_subscribe(client, csub, &gc,
                moq_simpair_now_us(sp)), (int)MOQ_ERR_INVAL);

        /* Empty URI from the client: accepted and delivered to the server. */
        moq_request_goaway_cfg_init(&gc);
        gc.timeout_ms = 5000;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_request_goaway_subscribe(client, csub, &gc,
                moq_simpair_now_us(sp)), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        size_t ul = 99; uint64_t to = 0;
        MOQ_TEST_CHECK(poll_request_goaway(server, MOQ_REQUEST_FAMILY_SUBSCRIBE, &ul, &to));
        MOQ_TEST_CHECK_EQ_SIZE(ul, 0);
        MOQ_TEST_CHECK_EQ_U64(to, 5000);
        MOQ_TEST_CHECK_EQ_INT((int)client->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT((int)server->state, (int)MOQ_SESS_ESTABLISHED);
        moq_simpair_destroy(sp);
    }

    /* Server responder role: a server URI reaches the client. */
    {
        moq_simpair_t *sp = make_pair(MOQ_VERSION_DRAFT_18);
        MOQ_TEST_CHECK(sp != NULL);
        moq_session_t *client = moq_simpair_client(sp);
        moq_session_t *server = moq_simpair_server(sp);
        moq_subscription_t csub, ssub;
        MOQ_TEST_CHECK(sim_setup_sub(sp, &csub, &ssub));

        goaway_cfg_server(&gc);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_request_goaway_subscribe(server, ssub, &gc,
                moq_simpair_now_us(sp)), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        size_t ul = 0; uint64_t to = 0;
        MOQ_TEST_CHECK(poll_request_goaway(client, MOQ_REQUEST_FAMILY_SUBSCRIBE, &ul, &to));
        MOQ_TEST_CHECK_EQ_SIZE(ul, SRV_URI_LEN);
        MOQ_TEST_CHECK_EQ_U64(to, SRV_TIMEOUT);
        MOQ_TEST_CHECK_EQ_INT((int)client->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT((int)server->state, (int)MOQ_SESS_ESTABLISHED);
        moq_simpair_destroy(sp);
    }

    /* == Draft-16 has no per-request GOAWAY ⇒ MOQ_ERR_INVAL ============= *
     *  A real D16 session with a valid subscription handle still reaches the
     *  profile gate (the encode op is NULL), so the wrapper returns INVAL. */
    {
        moq_simpair_t *sp = make_pair(MOQ_VERSION_DRAFT_16);
        MOQ_TEST_CHECK(sp != NULL);
        moq_session_t *client = moq_simpair_client(sp);
        MOQ_TEST_CHECK_EQ_INT((int)moq_simpair_start(sp), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        MOQ_TEST_CHECK_EQ_INT((int)client->state, (int)MOQ_SESS_ESTABLISHED);
        moq_subscribe_cfg_t sub; moq_subscribe_cfg_init(&sub);
        sub.track_namespace = k_ns; sub.track_name = MOQ_BYTES_LITERAL("v");
        moq_subscription_t csub;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscribe(client, &sub, 1, &csub),
                              (int)MOQ_OK);
        moq_request_goaway_cfg_init(&gc);   /* empty URI, valid handle */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_request_goaway_subscribe(client, csub, &gc,
                moq_simpair_now_us(sp)), (int)MOQ_ERR_INVAL);
        moq_simpair_destroy(sp);
    }

    MOQ_TEST_PASS("d18_request_goaway_send");
    return failures != 0;
}
