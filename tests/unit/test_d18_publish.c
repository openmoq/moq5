/*
 * Draft-18 PUBLISH (§10.10): a publisher-initiated subscription on its own
 * request bidi. PUBLISH is the inverse of SUBSCRIBE -- the publisher opens the
 * bidi and sends PUBLISH; the subscriber replies PUBLISH_OK (a REQUEST_OK with
 * delivery params) or REQUEST_ERROR; then objects flow on subgroup streams keyed
 * by the publisher-chosen Track Alias, the subscriber may REQUEST_UPDATE the
 * forward state, and the publisher ends it with PUBLISH_DONE + FIN. Covers the
 * wire codecs, inbound surfacing, the outbound open-a-bidi path, the
 * protocol-violation matrix, peer teardown, and SimPair end-to-end including
 * object delivery by alias.
 */
#include <moq/moq.h>
#include <moq/control_d18.h>
#include <moq/sim.h>
#include "test_support.h"
#include "../../core/src/session/session_internal.h"

/* -- Codec round-trips and malformed rejection --------------------- */

static int test_codec(void)
{
    int failures = 0;
    uint8_t buf[256];
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("example.com"),
                            MOQ_BYTES_LITERAL("live") };
    moq_namespace_t ns = { parts, 2 };
    moq_bytes_t name = MOQ_BYTES_LITERAL("video");

    /* PUBLISH round-trip: alias + FORWARD(0) + an auth token + properties tail. */
    {
        moq_d18_publish_t p = { 0 };
        p.request_id = 4;
        p.track_namespace = ns;
        p.track_name = name;
        p.track_alias = 9;
        p.params.has_forward = true; p.params.forward = 0;
        p.params.auth_token_count = 1;
        p.params.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_USE_VALUE;
        p.params.auth_tokens[0].token_type = 5;
        p.params.auth_tokens[0].token_value = MOQ_BYTES_LITERAL("t");
        uint8_t props[] = { 0x02, 0x00 };   /* one even property */
        p.track_properties = (moq_bytes_t){ props, sizeof(props) };
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_publish(&w, &p), (int)MOQ_OK);
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        moq_d18_decode_envelope(&r, &env);
        MOQ_TEST_CHECK_EQ_U64(env.msg_type, MOQ_D18_PUBLISH);
        moq_bytes_t dp[8];
        moq_d18_publish_t out;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_publish(env.payload, env.payload_len, dp, 8, &out),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(out.request_id, 4);
        MOQ_TEST_CHECK_EQ_U64(out.track_alias, 9);
        MOQ_TEST_CHECK_EQ_SIZE(out.track_namespace.count, 2);
        MOQ_TEST_CHECK(memcmp(out.track_name.data, "video", 5) == 0);
        MOQ_TEST_CHECK(out.params.has_forward && out.params.forward == 0);
        MOQ_TEST_CHECK_EQ_SIZE(out.params.auth_token_count, 1);
        MOQ_TEST_CHECK_EQ_SIZE(out.track_properties.len, 2);
    }

    /* A Track-delivery parameter (GROUP_ORDER) is rejected on a PUBLISH. */
    {
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        size_t len_off;
        moq_buf_write_vi64(&w, MOQ_D18_PUBLISH);
        moq_buf_reserve_uint16(&w, &len_off);
        size_t start = moq_buf_writer_offset(&w);
        moq_buf_write_vi64(&w, 0);             /* request id */
        moq_buf_write_vi64(&w, 1);             /* ns count */
        moq_buf_write_vi64(&w, 3);
        moq_buf_write_raw(&w, (const uint8_t *)"abc", 3);
        moq_buf_write_vi64(&w, 1);             /* track name len */
        moq_buf_write_raw(&w, (const uint8_t *)"v", 1);
        moq_buf_write_vi64(&w, 1);             /* track alias */
        moq_buf_write_vi64(&w, 1);             /* 1 param */
        moq_buf_write_vi64(&w, MOQ_D18_PARAM_GROUP_ORDER);
        uint8_t go = 1; moq_buf_write_raw(&w, &go, 1);
        moq_buf_patch_uint16(&w, len_off,
            (uint16_t)(moq_buf_writer_offset(&w) - start));
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        moq_d18_decode_envelope(&r, &env);
        moq_bytes_t dp[8];
        moq_d18_publish_t out;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_publish(env.payload, env.payload_len, dp, 8, &out),
            (int)MOQ_ERR_PROTO);
    }

    /* PUBLISH_OK round-trip: SUBSCRIBER_PRIORITY + GROUP_ORDER, empty props. */
    {
        moq_d18_msg_params_t p = { 0 };
        p.has_subscriber_priority = true; p.subscriber_priority = 7;
        p.has_group_order = true; p.group_order = MOQ_GROUP_ORDER_DESCENDING;
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_publish_ok(&w, &p), (int)MOQ_OK);
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        moq_d18_decode_envelope(&r, &env);
        MOQ_TEST_CHECK_EQ_U64(env.msg_type, MOQ_D18_REQUEST_OK);
        moq_d18_publish_ok_t ok;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_publish_ok(env.payload, env.payload_len, &ok),
            (int)MOQ_OK);
        MOQ_TEST_CHECK(ok.params.has_subscriber_priority);
        MOQ_TEST_CHECK_EQ_U64(ok.params.subscriber_priority, 7);
        MOQ_TEST_CHECK(ok.params.has_group_order);
        MOQ_TEST_CHECK_EQ_U64(ok.params.group_order, MOQ_GROUP_ORDER_DESCENDING);
    }

    /* PUBLISH_OK with a non-empty Track Properties tail is a violation. */
    {
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        size_t len_off;
        moq_buf_write_vi64(&w, MOQ_D18_REQUEST_OK);
        moq_buf_reserve_uint16(&w, &len_off);
        size_t start = moq_buf_writer_offset(&w);
        moq_buf_write_vi64(&w, 0);             /* 0 params */
        moq_buf_write_raw(&w, (const uint8_t *)"\x02\x00", 2);  /* stray props */
        moq_buf_patch_uint16(&w, len_off,
            (uint16_t)(moq_buf_writer_offset(&w) - start));
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        moq_d18_decode_envelope(&r, &env);
        moq_d18_publish_ok_t ok;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_publish_ok(env.payload, env.payload_len, &ok),
            (int)MOQ_ERR_PROTO);
    }

    return failures;
}

/* -- Session helpers ----------------------------------------------- */

static moq_session_t *make_session_caps(moq_perspective_t persp,
                                        uint32_t max_events,
                                        uint32_t send_buffer_size,
                                        uint32_t max_publishes)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), persp);
    cfg.version = MOQ_VERSION_DRAFT_18;
    if (max_events) cfg.max_events = max_events;
    if (send_buffer_size) cfg.send_buffer_size = send_buffer_size;
    if (max_publishes) cfg.max_publishes = max_publishes;
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
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), persp);
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
    moq_action_t a2;
    while (moq_session_poll_actions(s, &a2, 1) > 0) moq_action_cleanup(&a2);
    return s;
}

static size_t encode_publish_msg(uint8_t *buf, size_t cap, uint64_t request_id,
                                 const char *field, const char *name,
                                 uint64_t alias)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    moq_d18_publish_t p = { 0 };
    moq_bytes_t parts[] = { { (const uint8_t *)field, strlen(field) } };
    p.request_id = request_id;
    p.track_namespace = (moq_namespace_t){ parts, 1 };
    p.track_name = (moq_bytes_t){ (const uint8_t *)name, strlen(name) };
    p.track_alias = alias;
    moq_d18_encode_publish(&w, &p);
    return moq_buf_writer_offset(&w);
}

/* Feed an inbound PUBLISH (no FIN; the subscription lives on the bidi) and return
 * the surfaced publication handle. */
static moq_publication_t feed_publish(moq_session_t *s, moq_stream_ref_t ref,
                                      uint64_t request_id, const char *field,
                                      uint64_t alias)
{
    uint8_t msg[160];
    size_t n = encode_publish_msg(msg, sizeof(msg), request_id, field, "v", alias);
    moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1);
    moq_publication_t h = MOQ_PUBLICATION_INVALID;
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_PUBLISH_REQUEST)
            h = ev.u.publish_request.pub;
        moq_event_cleanup(&ev);
    }
    return h;
}

/* Feed an inbound PUBLISH with an explicit FORWARD state; return the handle. */
static moq_publication_t feed_publish_fwd(moq_session_t *s, moq_stream_ref_t ref,
                                          uint64_t request_id, const char *field,
                                          uint64_t alias, bool forward)
{
    uint8_t msg[160];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, msg, sizeof(msg));
    moq_d18_publish_t p = { 0 };
    moq_bytes_t parts[] = { { (const uint8_t *)field, strlen(field) } };
    p.request_id = request_id;
    p.track_namespace = (moq_namespace_t){ parts, 1 };
    p.track_name = (moq_bytes_t){ (const uint8_t *)"v", 1 };
    p.track_alias = alias;
    p.params.has_forward = true;
    p.params.forward = forward ? 1 : 0;
    moq_d18_encode_publish(&w, &p);
    moq_session_on_bidi_stream_bytes(s, ref, msg, moq_buf_writer_offset(&w), false, 1);
    moq_publication_t h = MOQ_PUBLICATION_INVALID;
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_PUBLISH_REQUEST) h = ev.u.publish_request.pub;
        moq_event_cleanup(&ev);
    }
    return h;
}

/* Feed one inbound OBJECT_DATAGRAM for `alias`; return the on_datagram result. */
static moq_result_t feed_datagram(moq_session_t *s, uint64_t alias,
                                  uint64_t group, uint64_t object, const char *pl)
{
    moq_d18_object_datagram_t dg;
    memset(&dg, 0, sizeof(dg));
    dg.track_alias = alias; dg.group_id = group; dg.object_id = object;
    dg.publisher_priority = 128;
    dg.payload = (const uint8_t *)pl; dg.payload_len = strlen(pl);
    uint8_t buf[64];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    if (moq_d18_encode_object_datagram(&w, &dg) < 0) return MOQ_ERR_INVAL;
    return moq_session_on_datagram(s, buf, moq_buf_writer_offset(&w), 1);
}

/* Feed one inbound subgroup-stream object for `alias` on `ref`. */
static void feed_subgroup_object(moq_session_t *s, moq_stream_ref_t ref,
                                 uint64_t alias, uint64_t group, const char *pl)
{
    uint8_t wire[96];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, wire, sizeof(wire));
    moq_d18_subgroup_header_t shdr;
    memset(&shdr, 0, sizeof(shdr));
    shdr.subgroup_id_mode = MOQ_SUBGROUP_ID_MODE_PRESENT;
    shdr.track_alias = alias; shdr.group_id = group; shdr.subgroup_id = 0;
    shdr.publisher_priority = 128;
    moq_d18_encode_subgroup_header(&w, &shdr);
    moq_buf_write_vi64(&w, 0);                            /* object id delta 0 */
    moq_buf_write_vi64(&w, (uint64_t)strlen(pl));         /* payload length */
    moq_buf_write_raw(&w, (const uint8_t *)pl, strlen(pl));
    moq_session_on_data_bytes(s, ref, wire, moq_buf_writer_offset(&w), false, 1);
}

/* Drain events; count OBJECT_RECEIVED. */
static int count_objects(moq_session_t *s)
{
    int n = 0;
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) n++;
        moq_event_cleanup(&ev);
    }
    return n;
}

/* Drain actions; true if a STOP_DATA was queued for `ref`. */
static bool saw_stop_data(moq_session_t *s, moq_stream_ref_t ref)
{
    bool got = false;
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) {
        if (a.kind == MOQ_ACTION_STOP_DATA &&
            a.u.stop_data.stream_ref._v == ref._v) got = true;
        moq_action_cleanup(&a);
    }
    return got;
}

static bool action_has_msg(moq_session_t *s, moq_stream_ref_t ref,
                           uint64_t msg_type, bool *out_fin)
{
    bool seen = false;
    moq_action_t act;
    while (moq_session_poll_actions(s, &act, 1) > 0) {
        if (act.kind == MOQ_ACTION_SEND_BIDI_STREAM &&
            act.u.send_bidi_stream.stream_ref._v == ref._v) {
            moq_buf_reader_t r;
            moq_buf_reader_init(&r, act.u.send_bidi_stream.data,
                                act.u.send_bidi_stream.len);
            moq_control_envelope_t env;
            if (moq_d18_decode_envelope(&r, &env) == MOQ_OK &&
                env.msg_type == msg_type) {
                seen = true;
                if (out_fin) *out_fin = act.u.send_bidi_stream.fin;
            }
        }
        moq_action_cleanup(&act);
    }
    return seen;
}

/* -- SimPair helpers ----------------------------------------------- */

static moq_simpair_t *make_pair(void)
{
    moq_simpair_cfg_t scfg = MOQ_SIMPAIR_CFG_INIT;
    scfg.alloc = moq_alloc_default();
    scfg.version = MOQ_VERSION_DRAFT_18;
    moq_simpair_t *sp = NULL;
    if (moq_simpair_create(&scfg, &sp) < 0) return NULL;
    if (moq_simpair_start(sp) < 0) { moq_simpair_destroy(sp); return NULL; }
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    return sp;
}

/* Run the client publish handshake to ESTABLISHED; return the client/server
 * publication handles. */
static bool establish_publish(moq_simpair_t *sp, moq_publication_t *cpub,
                              moq_publication_t *spub)
{
    moq_session_t *cl = moq_simpair_client(sp), *sv = moq_simpair_server(sp);
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_publish_cfg_t pc;
    moq_publish_cfg_init(&pc);
    pc.track_namespace = (moq_namespace_t){ parts, 1 };
    pc.track_name = MOQ_BYTES_LITERAL("v");
    if (moq_session_publish(cl, &pc, moq_simpair_now_us(sp), cpub) != MOQ_OK)
        return false;
    moq_simpair_run_until_quiescent(sp, 16, NULL);

    *spub = MOQ_PUBLICATION_INVALID;
    moq_event_t ev;
    while (moq_session_poll_events(sv, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_PUBLISH_REQUEST)
            *spub = ev.u.publish_request.pub;
        moq_event_cleanup(&ev);
    }
    if (spub->_opaque == MOQ_PUBLICATION_INVALID._opaque) return false;

    moq_accept_publish_cfg_t ac;
    moq_accept_publish_cfg_init(&ac);
    if (moq_session_accept_publish(sv, *spub, &ac, moq_simpair_now_us(sp)) != MOQ_OK)
        return false;
    moq_simpair_run_until_quiescent(sp, 16, NULL);

    bool ok = false;
    while (moq_session_poll_events(cl, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_PUBLISH_OK) ok = true;
        moq_event_cleanup(&ev);
    }
    return ok;
}

int main(void)
{
    int failures = 0;
    failures += test_codec();

    /* == Inbound PUBLISH surfaces namespace/name/alias/forward ======== */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        MOQ_TEST_CHECK(s != NULL);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        uint8_t msg[160];
        size_t n = encode_publish_msg(msg, sizeof(msg), 0, "live", "v", 42);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1),
            (int)MOQ_OK);
        bool got = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_PUBLISH_REQUEST) {
                got = true;
                MOQ_TEST_CHECK_EQ_SIZE(
                    ev.u.publish_request.track_namespace.count, 1);
                MOQ_TEST_CHECK_EQ_U64(ev.u.publish_request.track_alias, 42);
                MOQ_TEST_CHECK(ev.u.publish_request.forward);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got);
        MOQ_TEST_CHECK(request_registry_find_by_streamref(s, ref).kind ==
                       MOQ_REQ_PUBLISH);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Inbound PUBLISH fragmented byte-by-byte -> one event ========= */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        uint8_t msg[160];
        size_t n = encode_publish_msg(msg, sizeof(msg), 0, "live", "v", 1);
        for (size_t i = 0; i < n; i++) {
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_session_on_bidi_stream_bytes(s, ref, &msg[i], 1,
                                                      false, 1),
                (int)MOQ_OK);
            MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        }
        int reqs = 0;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_PUBLISH_REQUEST) reqs++;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(reqs, 1);
        moq_session_destroy(s);
    }

    /* == Outbound publish opens a request bidi (no FIN) =============== */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_publish_cfg_t pc;
        moq_publish_cfg_init(&pc);
        pc.track_namespace = (moq_namespace_t){ parts, 1 };
        pc.track_name = MOQ_BYTES_LITERAL("v");
        moq_publication_t ph;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_publish(s, &pc, 1, &ph), (int)MOQ_OK);
        bool opened = false, open_fin = true, send_control = false;
        moq_action_t act;
        while (moq_session_poll_actions(s, &act, 1) > 0) {
            if (act.kind == MOQ_ACTION_OPEN_BIDI_STREAM) {
                moq_buf_reader_t r;
                moq_buf_reader_init(&r, act.u.open_bidi_stream.data,
                                    act.u.open_bidi_stream.len);
                moq_control_envelope_t env;
                if (moq_d18_decode_envelope(&r, &env) == MOQ_OK &&
                    env.msg_type == MOQ_D18_PUBLISH)
                    opened = true;
                open_fin = act.u.open_bidi_stream.fin;
            } else if (act.kind == MOQ_ACTION_SEND_CONTROL) {
                send_control = true;
            }
            moq_action_cleanup(&act);
        }
        MOQ_TEST_CHECK(opened);
        MOQ_TEST_CHECK(!open_fin);       /* the subscription lives on the bidi */
        MOQ_TEST_CHECK(!send_control);   /* never on the shared control channel */
        moq_session_destroy(s);
    }

    /* == Accept -> PUBLISH_OK on the bidi (no FIN) ==================== */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        moq_publication_t h = feed_publish(s, ref, 0, "live", 7);
        MOQ_TEST_CHECK(h._opaque != MOQ_PUBLICATION_INVALID._opaque);
        moq_accept_publish_cfg_t ac;
        moq_accept_publish_cfg_init(&ac);
        ac.has_subscriber_priority = true; ac.subscriber_priority = 5;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_accept_publish(s, h, &ac, 1), (int)MOQ_OK);
        bool fin = true;
        MOQ_TEST_CHECK(action_has_msg(s, ref, MOQ_D18_REQUEST_OK, &fin));
        MOQ_TEST_CHECK(!fin);   /* established: no FIN after PUBLISH_OK */
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Reject -> REQUEST_ERROR + FIN, staging reclaimed ============= */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        moq_publication_t h = feed_publish(s, ref, 0, "live", 7);
        moq_reject_publish_cfg_t rj;
        moq_reject_publish_cfg_init(&rj);
        rj.error_code = MOQ_REQUEST_ERROR_NOT_SUPPORTED;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_reject_publish(s, h, &rj, 1), (int)MOQ_OK);
        bool fin = false;
        MOQ_TEST_CHECK(action_has_msg(s, ref, MOQ_D18_REQUEST_ERROR, &fin));
        MOQ_TEST_CHECK(fin);
        /* The request had no FIN, so a drain ref absorbs the peer's late FIN. */
        MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, 1);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, 0);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Duplicate track alias -> session close 0x5 ================== *
     *  The first PUBLISH is accepted (established with alias A); a second PUBLISH
     *  reusing alias A is a duplicate and closes the session. */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t r1 = moq_stream_ref_from_u64(1);
        moq_publication_t h = feed_publish(s, r1, 0, "live", 50);
        moq_accept_publish_cfg_t ac;
        moq_accept_publish_cfg_init(&ac);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_accept_publish(s, h, &ac, 1), (int)MOQ_OK);
        { moq_action_t a; while (moq_session_poll_actions(s, &a, 1) > 0)
              moq_action_cleanup(&a); }
        moq_stream_ref_t r2 = moq_stream_ref_from_u64(3);
        uint8_t msg[160];
        size_t n = encode_publish_msg(msg, sizeof(msg), 2, "vod", "v", 50);
        (void)moq_session_on_bidi_stream_bytes(s, r2, msg, n, false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);
    }

    /* == Publisher: a second PUBLISH_OK after establishment closes 0x3  */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_publish_cfg_t pc;
        moq_publish_cfg_init(&pc);
        pc.track_namespace = (moq_namespace_t){ parts, 1 };
        pc.track_name = MOQ_BYTES_LITERAL("v");
        moq_publication_t ph;
        moq_session_publish(s, &pc, 1, &ph);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0);
        moq_action_t act;
        while (moq_session_poll_actions(s, &act, 1) > 0) {
            if (act.kind == MOQ_ACTION_OPEN_BIDI_STREAM)
                ref = act.u.open_bidi_stream.stream_ref;
            moq_action_cleanup(&act);
        }
        uint8_t ok[32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, ok, sizeof(ok));
        moq_d18_msg_params_t p = { 0 };
        moq_d18_encode_publish_ok(&w, &p);
        size_t oklen = moq_buf_writer_offset(&w);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, ok, oklen, false, 1),
            (int)MOQ_OK);
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) moq_event_cleanup(&ev);
        /* A second PUBLISH_OK on the established publish bidi is a violation. */
        (void)moq_session_on_bidi_stream_bytes(s, ref, ok, oklen, false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);
    }

    /* == Subscriber: peer RESET tears the entry down, session up ====== */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        moq_publication_t h = feed_publish(s, ref, 0, "live", 3);
        moq_accept_publish_cfg_t ac;
        moq_accept_publish_cfg_init(&ac);
        moq_session_accept_publish(s, h, &ac, 1);
        { moq_action_t a; while (moq_session_poll_actions(s, &a, 1) > 0)
              moq_action_cleanup(&a); }
        MOQ_TEST_CHECK(request_registry_find_by_streamref(s, ref).kind ==
                       MOQ_REQ_PUBLISH);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_reset(s, ref, 0x1, 1), (int)MOQ_OK);
        bool finished = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_PUBLISH_FINISHED) finished = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(finished);
        MOQ_TEST_CHECK(request_registry_find_by_streamref(s, ref).kind ==
                       MOQ_REQ_NONE);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == SimPair end-to-end: publish -> accept -> object by alias ===== *
     *  Proves the data plane: a D18 publisher-initiated pub entry establishes,
     *  the publisher sends an object on a subgroup, and the subscriber receives
     *  it via pub_find_by_alias_subscriber. */
    {
        moq_simpair_t *sp = make_pair();
        MOQ_TEST_CHECK(sp != NULL);
        moq_session_t *cl = moq_simpair_client(sp), *sv = moq_simpair_server(sp);
        moq_publication_t cpub, spub;
        MOQ_TEST_CHECK(establish_publish(sp, &cpub, &spub));

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        sg_cfg.group_id = 0; sg_cfg.subgroup_id = 0; sg_cfg.publisher_priority = 128;
        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_open_pub_subgroup(cl, cpub, &sg_cfg,
                moq_simpair_now_us(sp), &sg), (int)MOQ_OK);
        moq_rcbuf_t *payload = NULL;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_rcbuf_create(moq_alloc_default(),
                (const uint8_t *)"hello", 5, &payload), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_write_object(cl, sg, 0, payload,
                moq_simpair_now_us(sp)), (int)MOQ_OK);
        moq_rcbuf_decref(payload);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_close_subgroup(cl, sg, moq_simpair_now_us(sp)),
            (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 32, NULL);

        bool got_obj = false;
        moq_event_t ev;
        while (moq_session_poll_events(sv, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED &&
                ev.u.object_received.payload) {
                got_obj = (moq_rcbuf_len(ev.u.object_received.payload) == 5 &&
                    memcmp(moq_rcbuf_data(ev.u.object_received.payload),
                           "hello", 5) == 0);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got_obj);

        /* Publisher finishes -> the subscriber sees PUBLISH_FINISHED. */
        moq_finish_publish_cfg_t fc;
        moq_finish_publish_cfg_init(&fc);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_finish_publish(cl, cpub, &fc,
                moq_simpair_now_us(sp)), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 32, NULL);
        bool finished = false;
        while (moq_session_poll_events(sv, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_PUBLISH_FINISHED) finished = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(finished);
        /* Both halves closed: neither side retains a publish entry or drain ref. */
        MOQ_TEST_CHECK_EQ_SIZE(cl->drain_ref_count, 0);
        MOQ_TEST_CHECK(request_registry_find_by_streamref(cl,
            moq_stream_ref_from_u64(0)).kind == MOQ_REQ_NONE);
        MOQ_TEST_CHECK_EQ_INT((int)cl->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT((int)sv->state, (int)MOQ_SESS_ESTABLISHED);
        moq_simpair_destroy(sp);
    }

    /* == SimPair: subscriber REQUEST_UPDATE -> PUBLISH_UPDATED ======== */
    {
        moq_simpair_t *sp = make_pair();
        MOQ_TEST_CHECK(sp != NULL);
        moq_session_t *cl = moq_simpair_client(sp), *sv = moq_simpair_server(sp);
        moq_publication_t cpub, spub;
        MOQ_TEST_CHECK(establish_publish(sp, &cpub, &spub));

        moq_publication_update_cfg_t uc;
        moq_publication_update_cfg_init(&uc);
        uc.has_forward = true; uc.forward = false;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_update_publication(sv, spub, &uc,
                moq_simpair_now_us(sp)), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 32, NULL);

        bool updated = false;
        moq_event_t ev;
        while (moq_session_poll_events(cl, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_PUBLISH_UPDATED) {
                updated = true;
                MOQ_TEST_CHECK(ev.u.publish_updated.has_forward);
                MOQ_TEST_CHECK(!ev.u.publish_updated.forward);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(updated);
        MOQ_TEST_CHECK_EQ_INT((int)cl->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT((int)sv->state, (int)MOQ_SESS_ESTABLISHED);
        moq_simpair_destroy(sp);
    }

    /* == SimPair: publication update token reaches PUBLISH_UPDATED ===== *
     *  End-to-end through the real D18 wire: the subscriber's update token
     *  resolves on the publisher and rides the event. */
    {
        moq_simpair_t *sp = make_pair();
        MOQ_TEST_CHECK(sp != NULL);
        moq_session_t *cl = moq_simpair_client(sp), *sv = moq_simpair_server(sp);
        moq_publication_t cpub, spub;
        MOQ_TEST_CHECK(establish_publish(sp, &cpub, &spub));

        moq_publication_update_cfg_t uc;
        moq_publication_update_cfg_init(&uc);
        moq_auth_token_t tok = {
            .token_type = 9,
            .token_value = MOQ_BYTES_LITERAL("renew"),
        };
        uc.auth_tokens = &tok;
        uc.auth_token_count = 1;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_update_publication(sv, spub, &uc,
                moq_simpair_now_us(sp)), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 32, NULL);

        bool updated = false;
        moq_event_t ev;
        while (moq_session_poll_events(cl, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_PUBLISH_UPDATED &&
                ev.u.publish_updated.token_count == 1 &&
                ev.u.publish_updated.tokens[0].token_type == 9 &&
                ev.u.publish_updated.tokens[0].token_value.len == 5 &&
                memcmp(ev.u.publish_updated.tokens[0].token_value.data,
                       "renew", 5) == 0)
                updated = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(updated);
        MOQ_TEST_CHECK_EQ_INT((int)cl->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT((int)sv->state, (int)MOQ_SESS_ESTABLISHED);
        moq_simpair_destroy(sp);
    }

    /* == SimPair: unknown alias on a publication update rejects ======== *
     *  USE_ALIAS of a never-registered alias: the publisher answers
     *  REQUEST_ERROR (UNKNOWN_AUTH_TOKEN_ALIAS), no PUBLISH_UPDATED, the
     *  publication and session stay up. */
    {
        moq_simpair_t *sp = make_pair();
        MOQ_TEST_CHECK(sp != NULL);
        moq_session_t *cl = moq_simpair_client(sp), *sv = moq_simpair_server(sp);
        moq_publication_t cpub, spub;
        MOQ_TEST_CHECK(establish_publish(sp, &cpub, &spub));

        moq_publication_update_cfg_t uc;
        moq_publication_update_cfg_init(&uc);
        uc.has_forward = true; uc.forward = false;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_update_publication(sv, spub, &uc,
                moq_simpair_now_us(sp)), (int)MOQ_OK);
        /* Tamper at the source is awkward through SimPair; instead feed the
         * publisher a hand-built REQUEST_UPDATE with USE_ALIAS(99) on the
         * publish bidi after quiescing the legitimate update. */
        moq_simpair_run_until_quiescent(sp, 32, NULL);
        { moq_event_t e2;
          while (moq_session_poll_events(cl, &e2, 1) > 0)
              moq_event_cleanup(&e2);
          while (moq_session_poll_events(sv, &e2, 1) > 0)
              moq_event_cleanup(&e2); }

        moq_d18_msg_params_t mp;
        memset(&mp, 0, sizeof(mp));
        mp.auth_token_count = 1;
        mp.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_USE_ALIAS;
        mp.auth_tokens[0].alias = 99;
        uint8_t upd[96];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, upd, sizeof(upd));
        /* Wire request id: the subscriber (server side) mints server-parity
         * ids; the legitimate update above consumed 1, so 3 is the next
         * valid one. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_encode_request_update(&w, 3, &mp), (int)MOQ_OK);
        moq_stream_ref_t pref = moq_stream_ref_from_u64(0x501);
        /* Deliver on the established publish bidi of the CLIENT publisher:
         * find its ref via the entry. */
        (void)pref;
        moq_stream_ref_t ref = cl->publishes[0].request_stream_ref;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(cl, ref, upd,
                moq_buf_writer_offset(&w), false,
                moq_simpair_now_us(sp)), (int)MOQ_OK);
        bool any_updated = false;
        moq_event_t ev;
        while (moq_session_poll_events(cl, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_PUBLISH_UPDATED) any_updated = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(!any_updated);
        /* The reject went out as REQUEST_ERROR(0x17) on the publish bidi. */
        bool saw_reject = false;
        moq_action_t act;
        while (moq_session_poll_actions(cl, &act, 1) > 0) {
            if (act.kind == MOQ_ACTION_SEND_BIDI_STREAM &&
                act.u.send_bidi_stream.stream_ref._v == ref._v) {
                moq_buf_reader_t r2;
                moq_buf_reader_init(&r2, act.u.send_bidi_stream.data,
                                    act.u.send_bidi_stream.len);
                moq_control_envelope_t env;
                moq_d18_request_error_t err;
                if (moq_d18_decode_envelope(&r2, &env) == MOQ_OK &&
                    env.msg_type == MOQ_D18_REQUEST_ERROR &&
                    moq_d18_decode_request_error(env.payload, env.payload_len,
                                                 &err) == MOQ_OK &&
                    err.error_code == 0x17)
                    saw_reject = true;
            }
            moq_action_cleanup(&act);
        }
        MOQ_TEST_CHECK(saw_reject);
        MOQ_TEST_CHECK_EQ_INT((int)cl->state, (int)MOQ_SESS_ESTABLISHED);
        moq_simpair_destroy(sp);
    }

    /* == SimPair end-to-end reject =================================== */
    {
        moq_simpair_t *sp = make_pair();
        MOQ_TEST_CHECK(sp != NULL);
        moq_session_t *cl = moq_simpair_client(sp), *sv = moq_simpair_server(sp);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_publish_cfg_t pc;
        moq_publish_cfg_init(&pc);
        pc.track_namespace = (moq_namespace_t){ parts, 1 };
        pc.track_name = MOQ_BYTES_LITERAL("v");
        moq_publication_t cpub;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_publish(cl, &pc, moq_simpair_now_us(sp), &cpub),
            (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        moq_publication_t spub = MOQ_PUBLICATION_INVALID;
        moq_event_t ev;
        while (moq_session_poll_events(sv, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_PUBLISH_REQUEST)
                spub = ev.u.publish_request.pub;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(spub._opaque != MOQ_PUBLICATION_INVALID._opaque);
        moq_reject_publish_cfg_t rj;
        moq_reject_publish_cfg_init(&rj);
        rj.error_code = MOQ_REQUEST_ERROR_DOES_NOT_EXIST;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_reject_publish(sv, spub, &rj,
                moq_simpair_now_us(sp)), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        bool rejected = false;
        while (moq_session_poll_events(cl, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_PUBLISH_ERROR) rejected = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(rejected);
        MOQ_TEST_CHECK_EQ_SIZE(sv->drain_ref_count, 0);
        MOQ_TEST_CHECK_EQ_INT((int)cl->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT((int)sv->state, (int)MOQ_SESS_ESTABLISHED);
        moq_simpair_destroy(sp);
    }

    /* == Trailing bytes after PUBLISH_DONE close the session (0x3) ==== */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        moq_publication_t h = feed_publish(s, ref, 0, "live", 7);
        moq_accept_publish_cfg_t ac;
        moq_accept_publish_cfg_init(&ac);
        moq_session_accept_publish(s, h, &ac, 1);
        { moq_action_t a; while (moq_session_poll_actions(s, &a, 1) > 0)
              moq_action_cleanup(&a); }
        /* PUBLISH_DONE envelope followed by a stray trailing byte (no FIN). */
        uint8_t done[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, done, sizeof(done));
        moq_d18_encode_publish_done(&w, 0, 0, (moq_bytes_t){ NULL, 0 });
        size_t dlen = moq_buf_writer_offset(&w);
        done[dlen] = 0xAB;   /* trailing garbage */
        (void)moq_session_on_bidi_stream_bytes(s, ref, done, dlen + 1, false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);
    }

    /* == DYNAMIC_GROUPS on PUBLISH gates the accept's new-group request = *
     *  Without the property the accept refuses (INVAL, before mutation);
     *  with it, PUBLISH_OK carries the request and publish_request
     *  surfaces dynamic_groups. */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        moq_publication_t h = feed_publish(s, ref, 0, "live", 7);
        moq_accept_publish_cfg_t ac;
        moq_accept_publish_cfg_init(&ac);
        ac.has_new_group_request = true;
        ac.new_group_request = 3;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_accept_publish(s, h, &ac, 1),
            (int)MOQ_ERR_INVAL);                /* no DYNAMIC_GROUPS */
        /* The publication is still pending: a plain accept works. */
        moq_accept_publish_cfg_t ac2;
        moq_accept_publish_cfg_init(&ac2);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_accept_publish(s, h, &ac2, 1), (int)MOQ_OK);
        moq_session_destroy(s);
    }
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        /* PUBLISH carrying DYNAMIC_GROUPS=1 ({0x30, 0x01} property tail). */
        uint8_t msg[160];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        moq_d18_publish_t p = { 0 };
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        p.request_id = 0;
        p.track_namespace = (moq_namespace_t){ parts, 1 };
        p.track_name = (moq_bytes_t){ (const uint8_t *)"v", 1 };
        p.track_alias = 7;
        static const uint8_t dyn[] = { 0x30, 0x01 };
        p.track_properties = (moq_bytes_t){ dyn, 2 };
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_publish(&w, &p),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, msg,
                moq_buf_writer_offset(&w), false, 1), (int)MOQ_OK);
        moq_publication_t h = MOQ_PUBLICATION_INVALID;
        bool dyn_seen = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_PUBLISH_REQUEST) {
                h = ev.u.publish_request.pub;
                dyn_seen = ev.u.publish_request.dynamic_groups;
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(dyn_seen);
        moq_accept_publish_cfg_t ac;
        moq_accept_publish_cfg_init(&ac);
        ac.has_new_group_request = true;
        ac.new_group_request = 0;             /* "no group info" is a value */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_accept_publish(s, h, &ac, 1), (int)MOQ_OK);
        bool ok_seen = false;
        moq_action_t act;
        while (moq_session_poll_actions(s, &act, 1) > 0) {
            if (act.kind == MOQ_ACTION_SEND_BIDI_STREAM &&
                act.u.send_bidi_stream.stream_ref._v == ref._v) {
                moq_buf_reader_t r2;
                moq_buf_reader_init(&r2, act.u.send_bidi_stream.data,
                                    act.u.send_bidi_stream.len);
                moq_control_envelope_t env;
                moq_d18_publish_ok_t ok;
                if (moq_d18_decode_envelope(&r2, &env) == MOQ_OK &&
                    env.msg_type == MOQ_D18_REQUEST_OK &&
                    moq_d18_decode_publish_ok(env.payload, env.payload_len,
                                              &ok) == MOQ_OK &&
                    ok.params.has_new_group_request &&
                    ok.params.new_group_request == 0)
                    ok_seen = true;
            }
            moq_action_cleanup(&act);
        }
        MOQ_TEST_CHECK(ok_seen);

        /* The publication update may carry it too (support latched). */
        moq_publication_update_cfg_t uc;
        moq_publication_update_cfg_init(&uc);
        uc.has_new_group_request = true;
        uc.new_group_request = 12;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_update_publication(s, h, &uc, 1), (int)MOQ_OK);
        moq_session_destroy(s);
    }

    /* == Inbound PUBLISH_OK / update NGR without our advertisement ====== *
     *  Our PUBLISH carried no DYNAMIC_GROUPS: a PUBLISH_OK (and, once
     *  established, a publication update) carrying a new-group request
     *  violates the MUST NOT -- close 0x3, never delivered. Hand-built
     *  responses: the local encoder refuses to emit the violation only on
     *  the sender's own gate, so the bytes are crafted directly. */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_publish_cfg_t pc;
        moq_publish_cfg_init(&pc);
        pc.track_namespace = (moq_namespace_t){ parts, 1 };
        pc.track_name = MOQ_BYTES_LITERAL("v");
        moq_publication_t h;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_publish(s, &pc, 1, &h),
                              (int)MOQ_OK);
        moq_action_t act;
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0);
        while (moq_session_poll_actions(s, &act, 1) > 0) {
            if (act.kind == MOQ_ACTION_OPEN_BIDI_STREAM)
                ref = act.u.open_bidi_stream.stream_ref;
            moq_action_cleanup(&act);
        }
        /* Peer's PUBLISH_OK carrying NEW_GROUP_REQUEST. */
        moq_d18_msg_params_t mp;
        memset(&mp, 0, sizeof(mp));
        mp.has_new_group_request = true;
        mp.new_group_request = 6;
        uint8_t ok[96];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, ok, sizeof(ok));
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_publish_ok(&w, &mp),
                              (int)MOQ_OK);
        (void)moq_session_on_bidi_stream_bytes(s, ref, ok,
            moq_buf_writer_offset(&w), false, 1);
        bool surfaced = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_PUBLISH_OK) surfaced = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(!surfaced);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);
    }
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_publish_cfg_t pc;
        moq_publish_cfg_init(&pc);
        pc.track_namespace = (moq_namespace_t){ parts, 1 };
        pc.track_name = MOQ_BYTES_LITERAL("v");
        moq_publication_t h;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_publish(s, &pc, 1, &h),
                              (int)MOQ_OK);
        moq_action_t act;
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0);
        while (moq_session_poll_actions(s, &act, 1) > 0) {
            if (act.kind == MOQ_ACTION_OPEN_BIDI_STREAM)
                ref = act.u.open_bidi_stream.stream_ref;
            moq_action_cleanup(&act);
        }
        /* Plain PUBLISH_OK establishes... */
        uint8_t ok[96];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, ok, sizeof(ok));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_encode_publish_ok(&w, &(moq_d18_msg_params_t){0}),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, ok,
                moq_buf_writer_offset(&w), false, 1), (int)MOQ_OK);
        { moq_event_t de;
          while (moq_session_poll_events(s, &de, 1) > 0)
              moq_event_cleanup(&de); }
        /* ...then the peer's update carrying NGR closes 0x3. */
        moq_d18_msg_params_t mp;
        memset(&mp, 0, sizeof(mp));
        mp.has_new_group_request = true;
        mp.new_group_request = 9;
        uint8_t upd[96];
        moq_buf_writer_t uw;
        moq_buf_writer_init(&uw, upd, sizeof(upd));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_encode_request_update(&uw, 1, &mp), (int)MOQ_OK);
        (void)moq_session_on_bidi_stream_bytes(s, ref, upd,
            moq_buf_writer_offset(&uw), false, 1);
        bool surfaced = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_PUBLISH_UPDATED) surfaced = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(!surfaced);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);
    }

    /* == SimPair: PUBLISH_OK new-group request reaches the publisher ==== */
    {
        moq_simpair_t *sp = make_pair();
        MOQ_TEST_CHECK(sp != NULL);
        moq_session_t *cl = moq_simpair_client(sp), *sv = moq_simpair_server(sp);

        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_publish_cfg_t pc;
        moq_publish_cfg_init(&pc);
        pc.track_namespace = (moq_namespace_t){ parts, 1 };
        pc.track_name = MOQ_BYTES_LITERAL("v");
        static const uint8_t dyn[] = { 0x30, 0x01 };
        pc.track_properties = (moq_bytes_t){ dyn, 2 };
        moq_publication_t cpub;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_publish(cl, &pc, moq_simpair_now_us(sp), &cpub),
            (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        moq_publication_t spub = MOQ_PUBLICATION_INVALID;
        bool dyn_seen = false;
        moq_event_t ev;
        while (moq_session_poll_events(sv, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_PUBLISH_REQUEST) {
                spub = ev.u.publish_request.pub;
                dyn_seen = ev.u.publish_request.dynamic_groups;
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(dyn_seen);

        moq_accept_publish_cfg_t ac;
        moq_accept_publish_cfg_init(&ac);
        ac.has_new_group_request = true;
        ac.new_group_request = 4;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_accept_publish(sv, spub, &ac,
                moq_simpair_now_us(sp)), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        bool ok_seen = false;
        while (moq_session_poll_events(cl, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_PUBLISH_OK &&
                ev.u.publish_ok.has_new_group_request &&
                ev.u.publish_ok.new_group_request == 4)
                ok_seen = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(ok_seen);

        /* A later publication update carries a fresh request end to end. */
        moq_publication_update_cfg_t uc;
        moq_publication_update_cfg_init(&uc);
        uc.has_new_group_request = true;
        uc.new_group_request = 8;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_update_publication(sv, spub, &uc,
                moq_simpair_now_us(sp)), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 32, NULL);
        bool upd_seen = false;
        while (moq_session_poll_events(cl, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_PUBLISH_UPDATED &&
                ev.u.publish_updated.has_new_group_request &&
                ev.u.publish_updated.new_group_request == 8)
                upd_seen = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(upd_seen);
        MOQ_TEST_CHECK_EQ_INT((int)cl->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT((int)sv->state, (int)MOQ_SESS_ESTABLISHED);
        moq_simpair_destroy(sp);
    }

    /* == Publication update carries an auth token ===================== *
     *  The accepting subscriber sends REQUEST_UPDATE with a USE_VALUE
     *  token on the publish bidi; the queued message decodes with the
     *  token among its parameters. */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        moq_publication_t h = feed_publish(s, ref, 0, "live", 7);
        moq_accept_publish_cfg_t ac;
        moq_accept_publish_cfg_init(&ac);
        moq_session_accept_publish(s, h, &ac, 1);
        { moq_action_t a; while (moq_session_poll_actions(s, &a, 1) > 0)
              moq_action_cleanup(&a); }
        moq_publication_update_cfg_t uc;
        moq_publication_update_cfg_init(&uc);
        moq_auth_token_t tok = {
            .token_type = 9,
            .token_value = MOQ_BYTES_LITERAL("renew"),
        };
        uc.auth_tokens = &tok;
        uc.auth_token_count = 1;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_update_publication(s, h, &uc, 1), (int)MOQ_OK);
        bool got_update = false;
        moq_action_t act;
        while (moq_session_poll_actions(s, &act, 1) > 0) {
            if (act.kind == MOQ_ACTION_SEND_BIDI_STREAM &&
                act.u.send_bidi_stream.stream_ref._v == ref._v) {
                moq_buf_reader_t r;
                moq_buf_reader_init(&r, act.u.send_bidi_stream.data,
                                    act.u.send_bidi_stream.len);
                moq_control_envelope_t env;
                moq_d18_request_update_t u;
                if (moq_d18_decode_envelope(&r, &env) == MOQ_OK &&
                    env.msg_type == MOQ_D18_REQUEST_UPDATE &&
                    moq_d18_decode_request_update(env.payload,
                        env.payload_len, &u) == MOQ_OK &&
                    u.params.auth_token_count == 1 &&
                    u.params.auth_tokens[0].alias_type ==
                        MOQ_AUTH_TOKEN_USE_VALUE &&
                    u.params.auth_tokens[0].token_type == 9 &&
                    u.params.auth_tokens[0].token_value.len == 5 &&
                    memcmp(u.params.auth_tokens[0].token_value.data,
                           "renew", 5) == 0)
                    got_update = true;
            }
            moq_action_cleanup(&act);
        }
        MOQ_TEST_CHECK(got_update);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Update failure: REQUEST_ERROR then PUBLISH_DONE(UPDATE_FAILED) = *
     *  The subscriber sends a publication update; the publisher rejects it with
     *  REQUEST_ERROR (no session close), then terminates with
     *  PUBLISH_DONE(UPDATE_FAILED). A further update is refused. */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        moq_publication_t h = feed_publish(s, ref, 0, "live", 7);
        moq_accept_publish_cfg_t ac;
        moq_accept_publish_cfg_init(&ac);
        moq_session_accept_publish(s, h, &ac, 1);
        { moq_action_t a; while (moq_session_poll_actions(s, &a, 1) > 0)
              moq_action_cleanup(&a); }
        moq_publication_update_cfg_t uc;
        moq_publication_update_cfg_init(&uc);
        uc.has_forward = true; uc.forward = false;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_update_publication(s, h, &uc, 1), (int)MOQ_OK);
        { moq_action_t a; while (moq_session_poll_actions(s, &a, 1) > 0)
              moq_action_cleanup(&a); }
        /* Publisher rejects the update with REQUEST_ERROR (no FIN). */
        uint8_t err[64];
        moq_buf_writer_t ew;
        moq_buf_writer_init(&ew, err, sizeof(err));
        moq_d18_encode_request_error(&ew, MOQ_REQUEST_ERROR_NOT_SUPPORTED, 0,
                                     (moq_bytes_t){ NULL, 0 });
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, err,
                moq_buf_writer_offset(&ew), false, 1), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        /* A further update is refused while awaiting the terminal PUBLISH_DONE. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_update_publication(s, h, &uc, 1),
            (int)MOQ_ERR_WRONG_STATE);
        /* PUBLISH_DONE(UPDATE_FAILED = 0x8) + FIN terminates cleanly. */
        uint8_t done[64];
        moq_buf_writer_t dw;
        moq_buf_writer_init(&dw, done, sizeof(done));
        moq_d18_encode_publish_done(&dw, 0x8, 0, (moq_bytes_t){ NULL, 0 });
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, done,
                moq_buf_writer_offset(&dw), true, 1), (int)MOQ_OK);
        bool finished = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_PUBLISH_FINISHED) finished = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(finished);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == PUBLISH_DONE + bidi FIN before data defers finish (Stream Count) = *
     *  Stream-correlated split-FIN: the publisher sends PUBLISH_DONE
     *  (Stream Count = 1) and FINs the request bidi before the data stream
     *  arrives. The subscriber must NOT finalize on the bidi FIN -- it keeps the
     *  publication live, delivers the late object, and finishes only once the
     *  data stream has been processed. */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        moq_publication_t h = feed_publish_fwd(s, ref, 0, "live", 7, true);
        moq_accept_publish_cfg_t ac;
        moq_accept_publish_cfg_init(&ac);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_accept_publish(s, h, &ac, 1), (int)MOQ_OK);
        { moq_action_t a; while (moq_session_poll_actions(s, &a, 1) > 0)
              moq_action_cleanup(&a); }

        /* PUBLISH_DONE(Stream Count = 1) WITHOUT FIN, then a separate empty FIN
         * on the request bidi (split-FIN), all before any data stream. */
        uint8_t done[64];
        moq_buf_writer_t dw;
        moq_buf_writer_init(&dw, done, sizeof(done));
        moq_d18_encode_publish_done(&dw, 0x0, 1, (moq_bytes_t){ NULL, 0 });
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, done,
                moq_buf_writer_offset(&dw), false, 1), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0,
                true /* FIN */, 1), (int)MOQ_OK);

        /* The bidi FIN must NOT finalize: PUBLISH_FINISHED is deferred and the
         * publication stays established. */
        { moq_event_t ev; bool fin = false;
          while (moq_session_poll_events(s, &ev, 1) > 0) {
              if (ev.kind == MOQ_EVENT_PUBLISH_FINISHED) fin = true;
              moq_event_cleanup(&ev);
          }
          MOQ_TEST_CHECK(!fin); }
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);

        /* The late data stream: its object is delivered under the live
         * publication; its FIN reaches Stream Count and finalizes. */
        moq_stream_ref_t dref = moq_stream_ref_from_u64(0x900);
        feed_subgroup_object(s, dref, 7, 0, "late");
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_data_bytes(s, dref, NULL, 0, true, 1), (int)MOQ_OK);

        bool got_obj = false, finished = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED &&
                ev.u.object_received.payload &&
                moq_rcbuf_len(ev.u.object_received.payload) == 4)
                got_obj = true;
            if (ev.kind == MOQ_EVENT_PUBLISH_FINISHED) finished = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got_obj);
        MOQ_TEST_CHECK(finished);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Update failure: a non-UPDATE_FAILED PUBLISH_DONE closes (0x3) = */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        moq_publication_t h = feed_publish(s, ref, 0, "live", 7);
        moq_accept_publish_cfg_t ac;
        moq_accept_publish_cfg_init(&ac);
        moq_session_accept_publish(s, h, &ac, 1);
        moq_publication_update_cfg_t uc;
        moq_publication_update_cfg_init(&uc);
        uc.has_forward = true; uc.forward = false;
        moq_session_update_publication(s, h, &uc, 1);
        { moq_action_t a; while (moq_session_poll_actions(s, &a, 1) > 0)
              moq_action_cleanup(&a); }
        uint8_t err[64];
        moq_buf_writer_t ew;
        moq_buf_writer_init(&ew, err, sizeof(err));
        moq_d18_encode_request_error(&ew, MOQ_REQUEST_ERROR_NOT_SUPPORTED, 0,
                                     (moq_bytes_t){ NULL, 0 });
        moq_session_on_bidi_stream_bytes(s, ref, err,
            moq_buf_writer_offset(&ew), false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        /* PUBLISH_DONE with a status other than UPDATE_FAILED is a violation. */
        uint8_t done[64];
        moq_buf_writer_t dw;
        moq_buf_writer_init(&dw, done, sizeof(done));
        moq_d18_encode_publish_done(&dw, 0x0, 0, (moq_bytes_t){ NULL, 0 });
        (void)moq_session_on_bidi_stream_bytes(s, ref, done,
            moq_buf_writer_offset(&dw), true, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);
    }

    /* == Send-buffer backpressure: accept is WOULD_BLOCK, not BUFFER === *
     *  With a small send buffer and many pending publishes accepted without
     *  draining actions, the send buffer (not the action queue) fills; accept
     *  must return a retryable WOULD_BLOCK and commit nothing, then succeed once
     *  the buffer drains. */
    {
        moq_session_t *s = make_session_caps(MOQ_PERSPECTIVE_SERVER, 0, 24, 16);
        moq_publication_t handles[10];
        for (int i = 0; i < 10; i++) {
            moq_stream_ref_t ref = moq_stream_ref_from_u64((uint64_t)(1 + 2 * i));
            handles[i] = feed_publish(s, ref, (uint64_t)(i * 2), "live",
                                      (uint64_t)(100 + i));
        }
        int blocked = -1;
        for (int i = 0; i < 10; i++) {
            moq_accept_publish_cfg_t ac;
            moq_accept_publish_cfg_init(&ac);
            moq_result_t rc = moq_session_accept_publish(s, handles[i], &ac, 1);
            if (rc == MOQ_ERR_WOULD_BLOCK) { blocked = i; break; }
            MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
        }
        MOQ_TEST_CHECK(blocked >= 0);   /* send buffer filled, retryable */
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
        moq_accept_publish_cfg_t ac;
        moq_accept_publish_cfg_init(&ac);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_accept_publish(s, handles[blocked], &ac, 1),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Objects before PUBLISH_OK (§9.4) ============================== *
     * A PUBLISH with FORWARD omitted/1 may transmit objects immediately,
     * possibly before our PUBLISH_OK. They must surface on the still-pending
     * publication, not be dropped. A FORWARD=0 PUBLISH won't send yet, so an
     * early object is not delivered (STOP_SENDING on a stream; dropped for a
     * datagram). */

    /* forward=1 (omitted): early subgroup-stream object surfaces. */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_publication_t h = feed_publish(s, moq_stream_ref_from_u64(1),
                                           0, "live", 7);
        MOQ_TEST_CHECK(h._opaque != MOQ_PUBLICATION_INVALID._opaque);
        MOQ_TEST_CHECK_EQ_INT((int)s->publishes[pub_resolve_handle(s, h)].state,
                              (int)MOQ_PUB_PENDING_SUBSCRIBER);
        feed_subgroup_object(s, moq_stream_ref_from_u64(0x900), 7, 0, "hello");
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT(count_objects(s), 1);
        moq_session_destroy(s);
    }

    /* forward=1 (omitted): early datagram object surfaces. */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_publication_t h = feed_publish(s, moq_stream_ref_from_u64(2),
                                           0, "live", 8);
        MOQ_TEST_CHECK(h._opaque != MOQ_PUBLICATION_INVALID._opaque);
        MOQ_TEST_CHECK_EQ_INT((int)feed_datagram(s, 8, 0, 0, "hi"), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(count_objects(s), 1);
        moq_session_destroy(s);
    }

    /* forward=0: an early subgroup object is not delivered; the stream is STOPPED. */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_publication_t h = feed_publish_fwd(s, moq_stream_ref_from_u64(3),
                                               0, "live", 9, false);
        MOQ_TEST_CHECK(h._opaque != MOQ_PUBLICATION_INVALID._opaque);
        moq_stream_ref_t dref = moq_stream_ref_from_u64(0x901);
        feed_subgroup_object(s, dref, 9, 0, "nope");
        MOQ_TEST_CHECK_EQ_INT(count_objects(s), 0);
        MOQ_TEST_CHECK(saw_stop_data(s, dref));
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* forward=0: an early datagram object is dropped (no stream to stop). */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_publication_t h = feed_publish_fwd(s, moq_stream_ref_from_u64(4),
                                               0, "live", 10, false);
        MOQ_TEST_CHECK(h._opaque != MOQ_PUBLICATION_INVALID._opaque);
        MOQ_TEST_CHECK_EQ_INT((int)feed_datagram(s, 10, 0, 0, "x"), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(count_objects(s), 0);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* accept after an early object: the accept still succeeds and later objects
     * keep surfacing. */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_publication_t h = feed_publish(s, moq_stream_ref_from_u64(5),
                                           0, "live", 11);
        MOQ_TEST_CHECK_EQ_INT((int)feed_datagram(s, 11, 0, 0, "early"), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(count_objects(s), 1);
        moq_accept_publish_cfg_t ac; moq_accept_publish_cfg_init(&ac);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_publish(s, h, &ac, 1),
                              (int)MOQ_OK);
        { moq_action_t a; while (moq_session_poll_actions(s,&a,1)>0) moq_action_cleanup(&a); }
        MOQ_TEST_CHECK_EQ_INT((int)feed_datagram(s, 11, 0, 1, "late"), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(count_objects(s), 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->publishes[pub_resolve_handle(s, h)].state,
                              (int)MOQ_PUB_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* reject after an early object: terminates cleanly; the freed publication no
     * longer accepts objects for the alias. */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_publication_t h = feed_publish(s, moq_stream_ref_from_u64(6),
                                           0, "live", 12);
        MOQ_TEST_CHECK_EQ_INT((int)feed_datagram(s, 12, 0, 0, "early"), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(count_objects(s), 1);
        moq_reject_publish_cfg_t rj; moq_reject_publish_cfg_init(&rj);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_reject_publish(s, h, &rj, 1),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        { moq_action_t a; while (moq_session_poll_actions(s,&a,1)>0) moq_action_cleanup(&a); }
        MOQ_TEST_CHECK_EQ_INT((int)feed_datagram(s, 12, 0, 1, "after"), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(count_objects(s), 0);
        moq_session_destroy(s);
    }

    /* reject after an early SUBGROUP object: the bound data stream must not keep
     * emitting against the freed publication handle. A further object on the same
     * stream is dropped (STOP_SENDING), not surfaced. */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_publication_t h = feed_publish(s, moq_stream_ref_from_u64(7),
                                           0, "live", 13);
        moq_stream_ref_t dref = moq_stream_ref_from_u64(0x910);
        feed_subgroup_object(s, dref, 13, 0, "early");   /* binds stream + surfaces */
        MOQ_TEST_CHECK_EQ_INT(count_objects(s), 1);
        { moq_action_t a; while (moq_session_poll_actions(s,&a,1)>0) moq_action_cleanup(&a); }
        moq_reject_publish_cfg_t rj; moq_reject_publish_cfg_init(&rj);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_reject_publish(s, h, &rj, 1),
                              (int)MOQ_OK);
        { moq_event_t e; while (moq_session_poll_events(s,&e,1)>0) moq_event_cleanup(&e); }
        { moq_action_t a; while (moq_session_poll_actions(s,&a,1)>0) moq_action_cleanup(&a); }
        /* A further object on the same (now stale-bound) subgroup stream. */
        uint8_t obj2[8];
        moq_buf_writer_t w; moq_buf_writer_init(&w, obj2, sizeof(obj2));
        moq_buf_write_vi64(&w, 1);                          /* object id delta */
        moq_buf_write_vi64(&w, 1);                          /* payload length */
        moq_buf_write_raw(&w, (const uint8_t *)"y", 1);
        moq_session_on_data_bytes(s, dref, obj2, moq_buf_writer_offset(&w), false, 1);
        MOQ_TEST_CHECK_EQ_INT(count_objects(s), 0);         /* not emitted */
        MOQ_TEST_CHECK(saw_stop_data(s, dref));             /* stream stopped */
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* Two pending inbound PUBLISHes cannot reserve the same track alias (a pending
     * alias already routes early data): the second closes the session (0x5). */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_publication_t h = feed_publish(s, moq_stream_ref_from_u64(8),
                                           0, "live", 14);
        MOQ_TEST_CHECK(h._opaque != MOQ_PUBLICATION_INVALID._opaque);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        uint8_t msg[160];
        size_t n = encode_publish_msg(msg, sizeof(msg), 2, "live2", "v", 14);
        moq_session_on_bidi_stream_bytes(s, moq_stream_ref_from_u64(9),
                                         msg, n, false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);
    }

    MOQ_TEST_PASS("d18_publish");
    return failures != 0;
}
