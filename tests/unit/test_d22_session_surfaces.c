/*
 * Session control surfaces on draft-22, with the draft-16/18 behaviour each
 * one keeps:
 *   - Range Filter parameters at MAX_FILTER_RANGES 0: a zero-length one is
 *     still a parameter (INVALID_FILTER); an omitted one changes nothing;
 *   - FILL_PARAMETERS is subscriber-originated (§3.4, §3.4.1), and its value
 *     carries its own Number of Parameters (§9.20.15);
 *   - an unknown token alias is refused per request (0x17), apart from the
 *     session-level token failures;
 *   - the AUTHORIZATION TOKEN Setup Option is sent;
 *   - the GOAWAY Timeout reaches the receiver, which never closes with the
 *     sender's GOAWAY_TIMEOUT;
 *   - the sender of a FETCH and of a PUBLISH update their own requests;
 *   - the resolved Location window is readable on both sides.
 */
#include <moq/moq.h>
#include <moq/sim.h>
#include "test_support.h"
#include "test_binding_support.h"
#include "../../core/src/session/session_internal.h"
#include "../../core/src/wire/messages_d22_internal.h"
#include "../../core/src/wire/control_d22_internal.h"
#include <string.h>

#include "test_raw_pair_d22.h"

static moq_bytes_t g_parts[] = { { (const uint8_t *)"live", 4 } };

/* A SUBSCRIBE from the client, accepted with Largest {5,2}. */
static bool subscribe(moq_session_t *c, moq_session_t *sv, moq_subscribe_filter_t filter,
                      moq_subscription_t *ch, moq_subscription_t *sh)
{
    moq_subscribe_cfg_t cfg;
    moq_subscribe_cfg_init(&cfg);
    cfg.track_namespace = (moq_namespace_t){ g_parts, 1 };
    cfg.track_name = MOQ_BYTES_LITERAL("v");
    cfg.filter = filter;
    if (moq_session_subscribe(c, &cfg, 2, ch) != MOQ_OK) return false;
    run(c, sv, 3);
    moq_event_t ev;
    if (!take(sv, MOQ_EVENT_SUBSCRIBE_REQUEST, &ev)) return false;
    *sh = ev.u.subscribe_request.sub;
    moq_event_cleanup(&ev);
    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    ac.has_largest = true; ac.largest_group = 5; ac.largest_object = 2;
    if (moq_session_accept_subscribe(sv, *sh, &ac, 4) != MOQ_OK) return false;
    run(c, sv, 5);
    return true;
}

/* A PUBLISH from the client, accepted by the server. */
static bool publish_filtered(moq_session_t *c, moq_session_t *sv, moq_publication_t *ch,
                             moq_publication_t *sh, const moq_location_filter_cfg_t *loc)
{
    moq_publish_cfg_t cfg;
    moq_publish_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.track_namespace = (moq_namespace_t){ g_parts, 1 };
    cfg.track_name = MOQ_BYTES_LITERAL("v");
    cfg.location = loc;
    if (moq_session_publish(c, &cfg, 2, ch) != MOQ_OK) return false;
    run(c, sv, 3);
    moq_event_t ev;
    if (!take(sv, MOQ_EVENT_PUBLISH_REQUEST, &ev)) return false;
    *sh = ev.u.publish_request.pub;
    moq_event_cleanup(&ev);
    moq_accept_publish_cfg_t ac;
    moq_accept_publish_cfg_init(&ac);
    if (moq_session_accept_publish(sv, *sh, &ac, 4) != MOQ_OK) return false;
    run(c, sv, 5);
    drain(c); drain(sv);
    return true;
}

static bool publish(moq_session_t *c, moq_session_t *sv, moq_publication_t *ch,
                    moq_publication_t *sh)
{
    return publish_filtered(c, sv, ch, sh, NULL);
}

/* The code of the first REQUEST_ERROR queued on `ref`, or UINT64_MAX. */
static uint64_t request_error_on(moq_session_t *s, moq_stream_ref_t ref, bool *saw_ok)
{
    uint64_t code = UINT64_MAX;
    if (saw_ok) *saw_ok = false;
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) {
        if (a.kind == MOQ_ACTION_SEND_BIDI_STREAM &&
            a.u.send_bidi_stream.stream_ref._v == ref._v) {
            moq_buf_reader_t r;
            moq_buf_reader_init(&r, a.u.send_bidi_stream.data, a.u.send_bidi_stream.len);
            moq_control_envelope_t env;
            while (moq_buf_reader_remaining(&r) > 0 &&
                   moq_d22_decode_envelope(&r, &env) == MOQ_OK) {
                if (env.msg_type == MOQ_D22_REQUEST_OK && saw_ok) *saw_ok = true;
                if (env.msg_type != MOQ_D22_REQUEST_ERROR || code != UINT64_MAX) continue;
                moq_d22_message_context_t ctx;
                memset(&ctx, 0, sizeof(ctx));
                ctx.request_type = MOQ_D22_SUBSCRIBE;
                moq_d22_message_t m;
                if (moq_d22_decode_message(MOQ_D22_REQUEST_ERROR, env.payload,
                                           env.payload_len, &ctx, &m) == MOQ_OK)
                    code = m.code;
            }
        }
        moq_action_cleanup(&a);
    }
    return code;
}

static uint64_t closed_code(moq_session_t *s)
{
    moq_event_t ev;
    uint64_t code = UINT64_MAX;
    if (take(s, MOQ_EVENT_SESSION_CLOSED, &ev)) {
        code = ev.u.closed.code;
        moq_event_cleanup(&ev);
    }
    return code;
}

static int sub_slot(moq_session_t *s, moq_subscription_t h)
{
    return sub_resolve_handle(s, h);
}

/* -- Range Filters at MAX_FILTER_RANGES 0 --------------------------------- */

/* §3.3.2: "Range Filters are only allowed if the setup option
 * MAX_FILTER_RANGES is non-zero"; this side advertises 0. A zero-length
 * parameter is still a parameter (it would remove a filter where the
 * facility exists), so it is INVALID_FILTER like any other; a REQUEST_UPDATE
 * that omits the parameter leaves the subscription as it was. */
static int test_range_parameters(void)
{
    int failures = 0;
    moq_session_t *c, *sv;
    MOQ_TEST_CHECK(pair(&c, &sv, MOQ_VERSION_DRAFT_22));

    /* SUBSCRIBE carrying an empty SUBGROUP_FILTER. */
    uint8_t ns_buf[16];
    moq_buf_writer_t nw;
    moq_buf_writer_init(&nw, ns_buf, sizeof(ns_buf));
    moq_namespace_t ns = { g_parts, 1 };
    MOQ_TEST_CHECK_EQ_INT((int)moq_d22_encode_namespace(&nw, &ns), MOQ_OK);
    moq_d22_message_t m;
    memset(&m, 0, sizeof(m));
    m.type = MOQ_D22_SUBSCRIBE;
    m.request_id = 0;
    m.ns.encoded.data = ns_buf; m.ns.encoded.len = moq_buf_writer_offset(&nw); m.ns.count = 1;
    m.track_name = MOQ_BYTES_LITERAL("v");
    moq_d22_param_t p;
    memset(&p, 0, sizeof(p));
    p.type = 0x25;
    p.value.ranges.clear = true;
    moq_stream_ref_t ref = moq_stream_ref_from_u64(0x7200);
    MOQ_TEST_CHECK_EQ_INT((int)feed(sv, ref, &m, &p, 1, 0, false, 5), MOQ_OK);
    moq_event_t ev;
    MOQ_TEST_CHECK(!take(sv, MOQ_EVENT_SUBSCRIBE_REQUEST, &ev));
    MOQ_TEST_CHECK_EQ_U64(request_error_on(sv, ref, NULL), MOQ_REQUEST_ERROR_INVALID_FILTER);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv), MOQ_SESS_ESTABLISHED);
    destroy(c, sv);

    /* The subscriber's REQUEST_UPDATE: empty range parameter refused, and
     * an update without one accepted with the filter unchanged. */
    MOQ_TEST_CHECK(pair(&c, &sv, MOQ_VERSION_DRAFT_22));
    moq_subscription_t ch, sh;
    MOQ_TEST_CHECK(subscribe(c, sv, MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT, &ch, &sh));
    drain(c); drain(sv);
    int ss = sub_slot(sv, sh);
    MOQ_TEST_CHECK(ss >= 0);
    if (ss < 0) { destroy(c, sv); return failures; }
    moq_stream_ref_t sref = sv->subs[ss].rs.ref;
    moq_location_filter_cfg_t before, after;
    moq_location_filter_cfg_init(&before);
    moq_location_filter_cfg_init(&after);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscription_filter(sv, sh, &before), MOQ_OK);

    memset(&m, 0, sizeof(m));
    m.type = MOQ_D22_REQUEST_UPDATE;
    m.request_id = 2;
    memset(&p, 0, sizeof(p));
    p.type = 0x20;              /* SUBSCRIBER_PRIORITY only */
    p.value.integer = 7;
    MOQ_TEST_CHECK_EQ_INT((int)feed(sv, sref, &m, &p, 1, MOQ_D22_SUBSCRIBE, true, 6), MOQ_OK);
    bool ok = false;
    MOQ_TEST_CHECK_EQ_U64(request_error_on(sv, sref, &ok), UINT64_MAX);
    MOQ_TEST_CHECK(ok);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscription_filter(sv, sh, &after), MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(after.kind, before.kind);
    MOQ_TEST_CHECK_EQ_U64(after.start_group, before.start_group);
    MOQ_TEST_CHECK_EQ_INT((int)sv->subs[ss].subscriber_priority, 7);
    drain(sv);

    m.request_id = 4;
    memset(&p, 0, sizeof(p));
    p.type = 0x25;
    p.value.ranges.clear = true;
    MOQ_TEST_CHECK_EQ_INT((int)feed(sv, sref, &m, &p, 1, MOQ_D22_SUBSCRIBE, true, 7), MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(request_error_on(sv, sref, NULL), MOQ_REQUEST_ERROR_INVALID_FILTER);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv), MOQ_SESS_ESTABLISHED);
    destroy(c, sv);
    return failures;
}

/* -- FILL_PARAMETERS is subscriber-originated ----------------------------- */

/* §3.4/§3.4.1: the publisher opens a fill when it processes the SUBSCRIBE or
 * REQUEST_UPDATE carrying FILL_PARAMETERS, and the subscriber learns the
 * range from the response; nothing makes a fill a publisher's request. The
 * publisher's update of its own PUBLISH carrying one is outside that
 * message's parameters: PROTOCOL_VIOLATION, as an unknown or out-of-scope
 * parameter is (§9.20). A publisher has no REQUEST_UPDATE on a SUBSCRIBE
 * (§9.5): PROTOCOL_VIOLATION as well. The subscriber's update, including
 * after PUBLISH_OK, still carries one; locally the publisher cannot. */
static int test_fill_direction(void)
{
    int failures = 0;
    moq_session_t *c, *sv;

    /* PUBLISH-initiated: the publisher's own REQUEST_UPDATE with a fill. */
    MOQ_TEST_CHECK(pair(&c, &sv, MOQ_VERSION_DRAFT_22));
    moq_publication_t cp, sp;
    MOQ_TEST_CHECK(publish(c, sv, &cp, &sp));
    int ps = pub_resolve_handle(sv, sp);
    MOQ_TEST_CHECK(ps >= 0);
    moq_d22_message_t m;
    memset(&m, 0, sizeof(m));
    m.type = MOQ_D22_REQUEST_UPDATE;
    m.request_id = 2;
    static const uint8_t no_settings[] = { 0 };   /* Number of Parameters 0 */
    moq_d22_param_t p;
    memset(&p, 0, sizeof(p));
    p.type = 0x23;
    p.value.fill = (moq_bytes_t){ no_settings, sizeof(no_settings) };
    if (ps >= 0) {
        moq_stream_ref_t sref = sv->publishes[ps].rs.ref;
        /* Encoded with the subscriber's scope: the bytes are the same, and
         * the publisher's scope has no FILL_PARAMETERS to encode. */
        moq_result_t frc = feed(sv, sref, &m, &p, 1, MOQ_D22_PUBLISH, true, 6);
        MOQ_TEST_CHECK(frc == MOQ_OK || frc == MOQ_ERR_CLOSED);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv), MOQ_SESS_CLOSED);
        MOQ_TEST_CHECK_EQ_U64(closed_code(sv), 0x3);
        MOQ_TEST_CHECK_EQ_SIZE(sv->fills.live, 0);
    }
    destroy(c, sv);

    /* The same update without the fill is accepted: the refusal is the
     * parameter, not the sender. */
    MOQ_TEST_CHECK(pair(&c, &sv, MOQ_VERSION_DRAFT_22));
    MOQ_TEST_CHECK(publish(c, sv, &cp, &sp));
    ps = pub_resolve_handle(sv, sp);
    if (ps >= 0) {
        moq_stream_ref_t sref = sv->publishes[ps].rs.ref;
        memset(&p, 0, sizeof(p));
        p.type = 0x10; p.value.integer = 0;     /* FORWARD 0 */
        MOQ_TEST_CHECK_EQ_INT((int)feed(sv, sref, &m, &p, 1, MOQ_D22_PUBLISH, false, 6), MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv), MOQ_SESS_ESTABLISHED);
    }
    destroy(c, sv);

    /* SUBSCRIBE-initiated: the publisher has no update to send. */
    MOQ_TEST_CHECK(pair(&c, &sv, MOQ_VERSION_DRAFT_22));
    moq_subscription_t ch, sh;
    MOQ_TEST_CHECK(subscribe(c, sv, MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT, &ch, &sh));
    drain(c); drain(sv);
    int cs = sub_slot(c, ch);
    MOQ_TEST_CHECK(cs >= 0);
    if (cs >= 0) {
        moq_stream_ref_t cref = c->subs[cs].rs.ref;
        memset(&p, 0, sizeof(p));
        p.type = 0x23;
        p.value.fill = (moq_bytes_t){ no_settings, sizeof(no_settings) };
        m.request_id = 1;
        moq_result_t frc = feed(c, cref, &m, &p, 1, MOQ_D22_SUBSCRIBE, true, 6);
        MOQ_TEST_CHECK(frc == MOQ_OK || frc == MOQ_ERR_CLOSED);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), MOQ_SESS_CLOSED);
        MOQ_TEST_CHECK_EQ_U64(closed_code(c), 0x3);
    }
    destroy(c, sv);

    /* Locally: the publisher's update refuses a fill; the subscriber's
     * update after PUBLISH_OK carries one and the publisher owes it. */
    MOQ_TEST_CHECK(pair(&c, &sv, MOQ_VERSION_DRAFT_22));
    MOQ_TEST_CHECK(publish(c, sv, &cp, &sp));
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    moq_publication_update_cfg_t u;
    moq_publication_update_cfg_init(&u);
    /* With a token the publisher's update is otherwise sendable. */
    uint8_t tv[] = { 0x01 };
    moq_auth_token_t tok = { 2, { tv, sizeof(tv) } };
    u.auth_tokens = &tok;
    u.auth_token_count = 1;
    u.fill = &fill;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_publication(c, cp, &u, 7), MOQ_ERR_INVAL);
    u.auth_token_count = 0;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_publication(sv, sp, &u, 7), MOQ_OK);
    run(c, sv, 8);
    moq_event_t ev;
    /* The publisher processed the fill: with nothing published there is no
     * Largest Object, so the range is empty and the fill ends at once
     * (§3.4: no fill fetch stream). */
    bool fill_seen = false;
    while (moq_session_poll_events(c, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_FILL_REQUEST || ev.kind == MOQ_EVENT_FILL_TERMINAL)
            fill_seen = true;
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK(fill_seen);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), MOQ_SESS_ESTABLISHED);
    destroy(c, sv);
    return failures;
}

/* -- FILL_PARAMETERS carries a parameter count ----------------------------- */

/* §9.20.15: the value holds Parameters "encoded as if they were Parameters
 * for a separate message", so it starts with a Number of Parameters (§9.20;
 * the selected reading, supported by the moq-wg/moq-transport#1991
 * clarification). These are a
 * peer's SUBSCRIBE parameters byte for byte: FORWARD 1, SUBSCRIBER_PRIORITY
 * 255, LOCATION_FILTER Next Object, GROUP_ORDER Descending, and
 * FILL_PARAMETERS {count 1, LOCATION_FILTER Relative Start, StartGroup 1}.
 * The request is admitted, and the fill resolves against Largest {5,2} to
 * {5,0}..{5,2}, inheriting the subscription's priority and order. The same
 * fill without its count closes the session. */
static moq_result_t raw_subscribe(moq_session_t *sv, const uint8_t *params, size_t len,
                                  uint64_t ref)
{
    uint8_t msg[96];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, msg, sizeof(msg));
    moq_namespace_t ns = { g_parts, 1 };
    moq_buf_write_vi64(&w, MOQ_D22_SUBSCRIBE);
    uint8_t len16[2] = { 0, 0 };
    moq_buf_write_raw(&w, len16, 2);
    moq_buf_write_vi64(&w, 0);          /* Request ID */
    moq_d22_encode_namespace(&w, &ns);
    moq_buf_write_vi64(&w, 1);
    moq_buf_write_raw(&w, (const uint8_t *)"v", 1);
    moq_buf_write_raw(&w, params, len);
    size_t total = moq_buf_writer_offset(&w);
    msg[1] = (uint8_t)((total - 3) >> 8);
    msg[2] = (uint8_t)(total - 3);
    return moq_session_on_bidi_stream_bytes(sv, moq_stream_ref_from_u64(ref), msg, total,
                                            false, 5);
}

static int test_fill_count(void)
{
    int failures = 0;
    static const uint8_t peer[] = { 0x05, 0x10, 0x01, 0x10, 0xff, 0x01, 0x05, 0x01, 0x02,
                                    0x01, 0x04, 0x01, 0x21, 0x01, 0x01 };
    moq_session_t *c, *sv;
    MOQ_TEST_CHECK(pair(&c, &sv, MOQ_VERSION_DRAFT_22));
    MOQ_TEST_CHECK_EQ_INT((int)raw_subscribe(sv, peer, sizeof(peer), 0x7400), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv), MOQ_SESS_ESTABLISHED);
    moq_event_t ev;
    bool got = take(sv, MOQ_EVENT_SUBSCRIBE_REQUEST, &ev);
    MOQ_TEST_CHECK(got);
    if (got) {
        const moq_subscribe_request_event_t *rq = &ev.u.subscribe_request;
        MOQ_TEST_CHECK(rq->forward);
        MOQ_TEST_CHECK_EQ_INT(rq->subscriber_priority, 255);
        MOQ_TEST_CHECK_EQ_INT((int)rq->group_order, (int)MOQ_GROUP_ORDER_DESCENDING);
        moq_subscription_t sh = rq->sub;
        moq_event_cleanup(&ev);
        moq_accept_subscribe_cfg_t ac;
        moq_accept_subscribe_cfg_init(&ac);
        ac.has_largest = true; ac.largest_group = 5; ac.largest_object = 2;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_subscribe(sv, sh, &ac, 6), MOQ_OK);
        bool fill = take(sv, MOQ_EVENT_FILL_REQUEST, &ev);
        MOQ_TEST_CHECK(fill);
        if (fill) {
            const moq_fill_request_event_t *f = &ev.u.fill_request;
            MOQ_TEST_CHECK(moq_subscription_eq(f->sub, sh));
            MOQ_TEST_CHECK(f->start_group == 5 && f->start_object == 0 &&
                           f->end_group == 5 && f->end_object == 2);
            MOQ_TEST_CHECK_EQ_INT(f->subscriber_priority, 255);
            MOQ_TEST_CHECK_EQ_INT((int)f->group_order, (int)MOQ_GROUP_ORDER_DESCENDING);
            MOQ_TEST_CHECK(!f->has_fill_timeout);
            moq_event_cleanup(&ev);
        }
    }
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv), MOQ_SESS_ESTABLISHED);
    destroy(c, sv);

    /* The same fill with the count left out: 0x21 reads as the count. */
    static const uint8_t uncounted[] = { 0x05, 0x10, 0x01, 0x10, 0xff, 0x01, 0x05, 0x01, 0x02,
                                         0x01, 0x03, 0x21, 0x01, 0x01 };
    MOQ_TEST_CHECK(pair(&c, &sv, MOQ_VERSION_DRAFT_22));
    moq_result_t rc = raw_subscribe(sv, uncounted, sizeof(uncounted), 0x7400);
    MOQ_TEST_CHECK(rc == MOQ_OK || rc == MOQ_ERR_CLOSED);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv), MOQ_SESS_CLOSED);
    MOQ_TEST_CHECK_EQ_U64(closed_code(sv), 0x3);
    destroy(c, sv);
    return failures;
}

/* The FILL_PARAMETERS value of the first `msg_type` message `s` queued on a
 * request stream (opened or continued), decoded with the given request
 * context. */
static bool emitted_fill(moq_session_t *s, uint64_t msg_type, uint64_t request_type,
                         uint8_t *val, size_t cap, size_t *len)
{
    bool found = false;
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) {
        const uint8_t *data = NULL;
        size_t dlen = 0;
        if (a.kind == MOQ_ACTION_SEND_BIDI_STREAM) {
            data = a.u.send_bidi_stream.data; dlen = a.u.send_bidi_stream.len;
        } else if (a.kind == MOQ_ACTION_OPEN_BIDI_STREAM) {
            data = a.u.open_bidi_stream.data; dlen = a.u.open_bidi_stream.len;
        }
        if (!found && data) {
            moq_buf_reader_t r;
            moq_buf_reader_init(&r, data, dlen);
            moq_control_envelope_t env;
            while (!found && moq_buf_reader_remaining(&r) > 0 &&
                   moq_d22_decode_envelope(&r, &env) == MOQ_OK) {
                if (env.msg_type != msg_type) continue;
                moq_d22_message_context_t ctx;
                memset(&ctx, 0, sizeof(ctx));
                ctx.request_type = request_type;
                ctx.update_from_subscriber = true;
                ctx.max_filter_ranges = 8;
                moq_d22_message_t m;
                if (moq_d22_decode_message(env.msg_type, env.payload, env.payload_len,
                                           &ctx, &m) != MOQ_OK) continue;
                moq_buf_reader_t pr = { m.params.encoded.data, m.params.encoded.len, 0 };
                uint64_t prev = 0;
                moq_d22_param_t p;
                while (moq_buf_reader_remaining(&pr) > 0 &&
                       moq_d22_param_next_scanned(&pr, &prev, &p) == MOQ_OK) {
                    if (p.type != 0x23 || p.value.fill.len > cap) continue;
                    memcpy(val, p.value.fill.data, p.value.fill.len);
                    *len = p.value.fill.len;
                    found = true;
                }
            }
        }
        moq_action_cleanup(&a);
    }
    return found;
}

#define CHECK_FILL_VALUE(got, got_len, ...) do { \
    static const uint8_t want_[] = { __VA_ARGS__ }; \
    MOQ_TEST_CHECK((got_len) == sizeof(want_) && memcmp((got), want_, sizeof(want_)) == 0); \
} while (0)

/* Every fill the library authors carries its count: a SUBSCRIBE with no
 * settings is `00`, one with settings counts them, and the REQUEST_UPDATE
 * of a subscription and the subscriber's REQUEST_UPDATE of a PUBLISH do the
 * same. The expected values are the wire bytes, not a round trip. */
static int test_fill_emission(void)
{
    int failures = 0;
    moq_session_t *c, *sv;
    uint8_t val[32];
    size_t len = 0;

    moq_fill_cfg_t empty;
    moq_fill_cfg_init(&empty);
    moq_subscribe_cfg_t cfg;
    moq_subscribe_cfg_init(&cfg);
    cfg.track_namespace = (moq_namespace_t){ g_parts, 1 };
    cfg.track_name = MOQ_BYTES_LITERAL("v");
    cfg.fill = &empty;
    moq_subscription_t ch, sh;
    MOQ_TEST_CHECK(pair(&c, &sv, MOQ_VERSION_DRAFT_22));
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscribe(c, &cfg, 2, &ch), MOQ_OK);
    MOQ_TEST_CHECK(emitted_fill(c, MOQ_D22_SUBSCRIBE, 0, val, sizeof(val), &len));
    CHECK_FILL_VALUE(val, len, 0x00);
    destroy(c, sv);

    /* FILL_TIMEOUT 250 then SUBSCRIBER_PRIORITY 9 (delta 0x16). */
    moq_fill_cfg_t set;
    moq_fill_cfg_init(&set);
    set.has_fill_timeout = true; set.fill_timeout_ms = 250;
    set.has_subscriber_priority = true; set.subscriber_priority = 9;
    cfg.fill = &set;
    MOQ_TEST_CHECK(pair(&c, &sv, MOQ_VERSION_DRAFT_22));
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscribe(c, &cfg, 2, &ch), MOQ_OK);
    MOQ_TEST_CHECK(emitted_fill(c, MOQ_D22_SUBSCRIBE, 0, val, sizeof(val), &len));
    CHECK_FILL_VALUE(val, len, 0x02, 0x0a, 0x80, 0xfa, 0x16, 0x09);
    destroy(c, sv);

    MOQ_TEST_CHECK(pair(&c, &sv, MOQ_VERSION_DRAFT_22));
    MOQ_TEST_CHECK(subscribe(c, sv, MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT, &ch, &sh));
    drain(c); drain(sv);
    moq_subscription_update_cfg_t su;
    moq_subscription_update_cfg_init_sized(&su, sizeof(su));
    su.fill = &empty;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_subscription(c, ch, &su, 6), MOQ_OK);
    MOQ_TEST_CHECK(emitted_fill(c, MOQ_D22_REQUEST_UPDATE, MOQ_D22_SUBSCRIBE, val,
                                sizeof(val), &len));
    CHECK_FILL_VALUE(val, len, 0x00);
    destroy(c, sv);

    /* The subscriber of a PUBLISH: GROUP_ORDER Descending alone. */
    moq_fill_cfg_t order;
    moq_fill_cfg_init(&order);
    order.has_group_order = true; order.group_order = MOQ_GROUP_ORDER_DESCENDING;
    moq_publication_t cp, sp;
    MOQ_TEST_CHECK(pair(&c, &sv, MOQ_VERSION_DRAFT_22));
    MOQ_TEST_CHECK(publish(c, sv, &cp, &sp));
    moq_publication_update_cfg_t pu;
    moq_publication_update_cfg_init(&pu);
    pu.fill = &order;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_publication(sv, sp, &pu, 7), MOQ_OK);
    MOQ_TEST_CHECK(emitted_fill(sv, MOQ_D22_REQUEST_UPDATE, MOQ_D22_PUBLISH, val,
                                sizeof(val), &len));
    CHECK_FILL_VALUE(val, len, 0x01, 0x22, 0x02);
    destroy(c, sv);
    return failures;
}

/* -- Token alias outcomes -------------------------------------------------- */

/* A SUBSCRIBE whose AUTHORIZATION_TOKEN parameter is the given raw Token
 * structure (§8.9), on a fresh client request stream. */
static moq_result_t subscribe_with_token(moq_session_t *sv, uint64_t request_id,
                                         const uint8_t *token, size_t token_len,
                                         uint64_t ref)
{
    uint8_t body[128];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, body, sizeof(body));
    moq_namespace_t ns = { g_parts, 1 };
    moq_buf_write_vi64(&w, request_id);
    moq_d22_encode_namespace(&w, &ns);
    moq_buf_write_vi64(&w, 1);
    moq_buf_write_raw(&w, (const uint8_t *)"v", 1);
    moq_buf_write_vi64(&w, 1);          /* one parameter */
    moq_buf_write_vi64(&w, 0x03);       /* AUTHORIZATION_TOKEN */
    moq_buf_write_vi64(&w, token_len);
    moq_buf_write_raw(&w, token, token_len);
    size_t blen = moq_buf_writer_offset(&w);
    uint8_t msg[160];
    moq_buf_writer_t mw;
    moq_buf_writer_init(&mw, msg, sizeof(msg));
    moq_buf_write_vi64(&mw, MOQ_D22_SUBSCRIBE);
    uint8_t len16[2] = { (uint8_t)(blen >> 8), (uint8_t)blen };
    moq_buf_write_raw(&mw, len16, 2);
    moq_buf_write_raw(&mw, body, blen);
    return moq_session_on_bidi_stream_bytes(sv, moq_stream_ref_from_u64(ref), msg,
                                            moq_buf_writer_offset(&mw), false, 5);
}

/* §8.9: "The receiver of a message referencing an Alias that is not
 * currently registered MUST reject the message with
 * UNKNOWN_AUTH_TOKEN_ALIAS" -- a per-request refusal (0x17, the code the
 * session registry gives that name), unlike the adjacent session-level
 * rules: an undecodable Token structure (KEY_VALUE_FORMATTING_ERROR 0x6), a
 * duplicate registration (DUPLICATE_AUTH_TOKEN_ALIAS 0x14) and a registration
 * past the cache (AUTH_TOKEN_CACHE_OVERFLOW 0x13, §9.1.3) close the session. */
static int test_token_alias_outcomes(void)
{
    int failures = 0;
    side_cfg_t cfg = { .version = MOQ_VERSION_DRAFT_22, .token_cache = 64 };
    moq_session_t *c, *sv;

    /* USE_ALIAS and DELETE of an alias never registered: refused, open. */
    MOQ_TEST_CHECK(pair_cfg(&c, &sv, &cfg, &cfg, true));
    static const uint8_t use_alias[] = { 0x02, 0x09 };
    MOQ_TEST_CHECK_EQ_INT((int)subscribe_with_token(sv, 0, use_alias, sizeof(use_alias), 0x7200), MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(request_error_on(sv, moq_stream_ref_from_u64(0x7200), NULL), 0x17);
    static const uint8_t del_alias[] = { 0x00, 0x09 };
    MOQ_TEST_CHECK_EQ_INT((int)subscribe_with_token(sv, 2, del_alias, sizeof(del_alias), 0x7300), MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(request_error_on(sv, moq_stream_ref_from_u64(0x7300), NULL), 0x17);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv), MOQ_SESS_ESTABLISHED);
    destroy(c, sv);

    /* An Alias Type the structure does not define: undecodable. */
    MOQ_TEST_CHECK(pair_cfg(&c, &sv, &cfg, &cfg, true));
    static const uint8_t bad[] = { 0x07, 0x01 };
    (void)subscribe_with_token(sv, 0, bad, sizeof(bad), 0x7200);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv), MOQ_SESS_CLOSED);
    MOQ_TEST_CHECK_EQ_U64(closed_code(sv), 0x6);
    destroy(c, sv);

    /* The same alias registered twice. */
    MOQ_TEST_CHECK(pair_cfg(&c, &sv, &cfg, &cfg, true));
    static const uint8_t reg[] = { 0x01, 0x01, 0x00, 0xaa };
    MOQ_TEST_CHECK_EQ_INT((int)subscribe_with_token(sv, 0, reg, sizeof(reg), 0x7200), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv), MOQ_SESS_ESTABLISHED);
    (void)subscribe_with_token(sv, 2, reg, sizeof(reg), 0x7300);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv), MOQ_SESS_CLOSED);
    MOQ_TEST_CHECK_EQ_U64(closed_code(sv), 0x14);
    destroy(c, sv);

    /* A registration larger than the advertised cache. */
    MOQ_TEST_CHECK(pair_cfg(&c, &sv, &cfg, &cfg, true));
    uint8_t big[4 + 64];
    memset(big, 0x55, sizeof(big));
    big[0] = 0x01; big[1] = 0x02; big[2] = 0x00;
    (void)subscribe_with_token(sv, 0, big, sizeof(big), 0x7200);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv), MOQ_SESS_CLOSED);
    MOQ_TEST_CHECK_EQ_U64(closed_code(sv), 0x13);
    destroy(c, sv);
    return failures;
}

/* -- SETUP authorization tokens -------------------------------------------- */

/* The AUTHORIZATION TOKEN Setup Option (draft-22 §9.1.4, draft-18
 * §10.3.1.4, draft-16 §9.3.1.5): the client's configured tokens reach the
 * server's SETUP_COMPLETE by value, on every profile; the cfg copies them. */
static int test_setup_tokens(moq_version_t v)
{
    int failures = 0;
    uint8_t v1[] = { 0x01, 0x02, 0x03 };
    uint8_t v2[] = { 0xff };
    moq_auth_token_t tokens[2] = {
        { 7, { v1, sizeof(v1) } },
        { 0, { v2, sizeof(v2) } },
    };
    side_cfg_t cc = { .version = v, .setup_tokens = tokens, .setup_token_count = 2 };
    side_cfg_t sc = { .version = v };
    moq_session_t *c, *sv;
    MOQ_TEST_CHECK(pair_cfg(&c, &sv, &cc, &sc, false));
    v1[0] = 0x99;            /* the session holds its own copy */
    run(c, sv, 2);
    moq_event_t ev;
    bool got = take(sv, MOQ_EVENT_SETUP_COMPLETE, &ev);
    MOQ_TEST_CHECK(got);
    if (got) {
        MOQ_TEST_CHECK_EQ_SIZE(ev.u.setup_complete.token_count, 2);
        if (ev.u.setup_complete.token_count == 2) {
            const moq_resolved_token_t *t = ev.u.setup_complete.tokens;
            MOQ_TEST_CHECK_EQ_U64(t[0].token_type, 7);
            MOQ_TEST_CHECK(t[0].token_value.len == 3 && t[0].token_value.data[0] == 0x01 &&
                           t[0].token_value.data[2] == 0x03);
            MOQ_TEST_CHECK_EQ_U64(t[1].token_type, 0);
            MOQ_TEST_CHECK(t[1].token_value.len == 1 && t[1].token_value.data[0] == 0xff);
        }
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv), MOQ_SESS_ESTABLISHED);
    destroy(c, sv);

    /* No tokens configured: none arrive. */
    side_cfg_t plain = { .version = v };
    MOQ_TEST_CHECK(pair_cfg(&c, &sv, &plain, &plain, false));
    run(c, sv, 2);
    got = take(sv, MOQ_EVENT_SETUP_COMPLETE, &ev);
    MOQ_TEST_CHECK(got);
    if (got) {
        MOQ_TEST_CHECK_EQ_SIZE(ev.u.setup_complete.token_count, 0);
        moq_event_cleanup(&ev);
    }
    destroy(c, sv);
    return failures;
}

/* The supported token count -- sixteen -- fits a SETUP together with every
 * other parameter this side sends (MAX_REQUEST_ID, the token-cache size), in
 * both directions, and reaches the peer by value. The tokens are copied when
 * the session is created: changing the caller's storage before start sends
 * the configured bytes. */
static int test_setup_token_cap(moq_version_t v)
{
    int failures = 0;
    size_t counts[] = { 2, 16 };
    for (size_t ci = 0; ci < 2; ci++) {
        for (int server_tokens = 0; server_tokens < 2; server_tokens++) {
            uint8_t values[16];
            moq_auth_token_t tokens[16];
            for (size_t i = 0; i < 16; i++) {
                values[i] = (uint8_t)i;
                tokens[i] = (moq_auth_token_t){ 7, { values + i, 1 } };
            }
            side_cfg_t cc = { .version = v, .token_cache = 64 };
            side_cfg_t sc = { .version = v, .token_cache = 64 };
            side_cfg_t *src = server_tokens ? &sc : &cc;
            src->setup_tokens = tokens;
            src->setup_token_count = counts[ci];
            moq_session_t *c, *sv;
            MOQ_TEST_CHECK(pair_cfg(&c, &sv, &cc, &sc, false));
            MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), MOQ_SESS_ESTABLISHED);
            MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv), MOQ_SESS_ESTABLISHED);
            moq_event_t ev;
            bool got = take(server_tokens ? c : sv, MOQ_EVENT_SETUP_COMPLETE, &ev);
            MOQ_TEST_CHECK(got);
            if (got) {
                MOQ_TEST_CHECK_EQ_SIZE(ev.u.setup_complete.token_count, counts[ci]);
                for (size_t i = 0; i < ev.u.setup_complete.token_count && i < counts[ci]; i++)
                    MOQ_TEST_CHECK(ev.u.setup_complete.tokens[i].token_value.len == 1 &&
                                   ev.u.setup_complete.tokens[i].token_value.data[0] == i);
                moq_event_cleanup(&ev);
            }
            destroy(c, sv);
        }
    }

    /* A token just inside the default 4096-byte arenas: the SETUP leaves and
     * arrives whole, in both directions. */
    for (int server_tokens = 0; server_tokens < 2; server_tokens++) {
        static uint8_t big[3800];
        for (size_t i = 0; i < sizeof(big); i++) big[i] = (uint8_t)(i * 37u + 5u);
        moq_auth_token_t tok = { 12, { big, sizeof(big) } };
        side_cfg_t cc = { .version = v, .token_cache = 64 };
        side_cfg_t sc = { .version = v, .token_cache = 64 };
        side_cfg_t *src = server_tokens ? &sc : &cc;
        src->setup_tokens = &tok;
        src->setup_token_count = 1;
        moq_session_t *c, *sv;
        MOQ_TEST_CHECK(pair_cfg(&c, &sv, &cc, &sc, false));
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv), MOQ_SESS_ESTABLISHED);
        moq_event_t ev;
        bool got = take(server_tokens ? c : sv, MOQ_EVENT_SETUP_COMPLETE, &ev);
        MOQ_TEST_CHECK(got);
        if (got) {
            MOQ_TEST_CHECK_EQ_SIZE(ev.u.setup_complete.token_count, 1);
            if (ev.u.setup_complete.token_count == 1)
                MOQ_TEST_CHECK(ev.u.setup_complete.tokens[0].token_value.len == sizeof(big) &&
                               memcmp(ev.u.setup_complete.tokens[0].token_value.data, big,
                                      sizeof(big)) == 0);
            moq_event_cleanup(&ev);
        }
        destroy(c, sv);
    }

    /* Draft-16: a CLIENT_SETUP with more parameters than a receiver holds
     * (unknown ones, legal on the wire) is this side's limit, closed as an
     * internal error, not a protocol violation; one parameter fewer is
     * accepted. */
    if (v == MOQ_VERSION_DRAFT_16) {
        for (size_t n = 32; n <= 33; n++) {
            moq_kvp_entry_t params[33];
            uint8_t one = 1;
            for (size_t i = 0; i < n; i++)
                params[i] = (moq_kvp_entry_t){ .type = 0x40 + 2 * i, .value = &one,
                                               .value_len = 1, .is_varint = true };
            uint8_t buf[512];
            moq_buf_writer_t w;
            moq_buf_writer_init(&w, buf, sizeof(buf));
            MOQ_TEST_CHECK_EQ_INT((int)moq_d16_encode_client_setup(&w, params, n), MOQ_OK);
            side_cfg_t sc = { .version = v };
            moq_session_t *srv = session(MOQ_PERSPECTIVE_SERVER, &sc);
            MOQ_TEST_CHECK(srv != NULL);
            if (!srv) continue;
            (void)moq_session_on_control_bytes(srv, buf, moq_buf_writer_offset(&w), 1);
            if (n == 32) {
                MOQ_TEST_CHECK(moq_session_state(srv) != MOQ_SESS_CLOSED);
            } else {
                MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(srv), MOQ_SESS_CLOSED);
                MOQ_TEST_CHECK_EQ_U64(closed_code(srv), 0x1);
            }
            moq_session_destroy(srv);
        }
    }

    /* Copied at create, before start. */
    uint8_t value[2] = { 0x11, 0x22 };
    moq_auth_token_t tok = { 3, { value, sizeof(value) } };
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), MOQ_PERSPECTIVE_CLIENT);
    cfg.version = v;
    cfg.setup_auth_tokens = &tok;
    cfg.setup_auth_token_count = 1;
    moq_session_t *c = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_create(&cfg, 0, &c), MOQ_OK);
    value[0] = 0x99;
    tok.token_type = 9;
    MOQ_TEST_CHECK_EQ_INT((int)test_bind_if_required(c), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(c, 0), MOQ_OK);
    side_cfg_t sc = { .version = v };
    moq_session_t *sv = session(MOQ_PERSPECTIVE_SERVER, &sc);
    MOQ_TEST_CHECK(sv != NULL);
    run(c, sv, 1);
    moq_event_t ev;
    bool got = take(sv, MOQ_EVENT_SETUP_COMPLETE, &ev);
    MOQ_TEST_CHECK(got);
    if (got) {
        MOQ_TEST_CHECK_EQ_SIZE(ev.u.setup_complete.token_count, 1);
        if (ev.u.setup_complete.token_count == 1) {
            MOQ_TEST_CHECK_EQ_U64(ev.u.setup_complete.tokens[0].token_type, 3);
            MOQ_TEST_CHECK(ev.u.setup_complete.tokens[0].token_value.len == 2 &&
                           ev.u.setup_complete.tokens[0].token_value.data[0] == 0x11);
        }
        moq_event_cleanup(&ev);
    }
    destroy(c, sv);
    return failures;
}

static int test_setup_token_cfg(void)
{
    int failures = 0;
    moq_session_cfg_t cfg;
    moq_session_t *s = NULL;
    moq_auth_token_t tokens[17];
    memset(tokens, 0, sizeof(tokens));
    /* More than 16, a NULL array, a NULL value with a length, a value past
     * 65535 bytes: refused at create. Token Type is vi64 (§8.9), so a type
     * past 2^62-1 is accepted. */
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), MOQ_PERSPECTIVE_CLIENT);
    cfg.version = MOQ_VERSION_DRAFT_22;
    cfg.setup_auth_tokens = tokens;
    cfg.setup_auth_token_count = 17;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_create(&cfg, 0, &s), MOQ_ERR_INVAL);
    cfg.setup_auth_tokens = NULL;
    cfg.setup_auth_token_count = 1;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_create(&cfg, 0, &s), MOQ_ERR_INVAL);
    cfg.setup_auth_tokens = tokens;
    tokens[0].token_value = (moq_bytes_t){ NULL, 3 };
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_create(&cfg, 0, &s), MOQ_ERR_INVAL);
    tokens[0].token_value = (moq_bytes_t){ NULL, 0 };
    cfg.setup_auth_token_count = 1;
    tokens[0].token_type = UINT64_C(1) << 62;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_create(&cfg, 0, &s), MOQ_OK);
    moq_session_destroy(s);
    s = NULL;
    tokens[0].token_type = UINT64_MAX;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_create(&cfg, 0, &s), MOQ_OK);
    moq_session_destroy(s);
    s = NULL;
    static uint8_t huge[65536];
    tokens[0].token_type = 1;
    tokens[0].token_value = (moq_bytes_t){ huge, sizeof(huge) };
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_create(&cfg, 0, &s), MOQ_ERR_INVAL);
    /* An empty value is a token. */
    tokens[0].token_value = (moq_bytes_t){ NULL, 0 };
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_create(&cfg, 0, &s), MOQ_OK);
    moq_session_destroy(s);
    /* A caller whose struct ends before the token block sends none. */
    moq_session_cfg_init_sized(&cfg, offsetof(moq_session_cfg_t, _reserved_setup_auth),
                               moq_alloc_default(), MOQ_PERSPECTIVE_CLIENT);
    cfg.setup_auth_tokens = NULL;
    cfg.setup_auth_token_count = 99;
    s = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_create(&cfg, 0, &s), MOQ_OK);
    if (s) MOQ_TEST_CHECK_EQ_SIZE(s->setup_token_count, 0);
    moq_session_destroy(s);
    return failures;
}

/* -- GOAWAY Timeout --------------------------------------------------------- */

/* draft-22 §9.2 / draft-18 §10.4: the Timeout is the sender's: "the sender
 * closes the session with GOAWAY_TIMEOUT after the indicated timeout". The
 * receiver learns it and never closes itself, whatever its own configured
 * drain timeout; the sender closes with GOAWAY_TIMEOUT. Draft-16's GOAWAY has
 * no Timeout field (§3.5: "The server closes the session with a
 * GOAWAY_TIMEOUT"). */
static int test_goaway_timeout(moq_version_t v)
{
    int failures = 0;
    side_cfg_t cc = { .version = v, .goaway_timeout_us = 1000 };
    side_cfg_t sc = { .version = v, .goaway_timeout_us = 5000000 };
    moq_session_t *c, *sv;
    MOQ_TEST_CHECK(pair_cfg(&c, &sv, &cc, &sc, true));
    static const uint8_t uri[] = "moqt://next.example/";
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_goaway(sv, uri, sizeof(uri) - 1, 10), MOQ_OK);
    run(c, sv, 11);
    moq_event_t ev;
    bool got = take(c, MOQ_EVENT_GOAWAY, &ev);
    MOQ_TEST_CHECK(got);
    if (got) {
        MOQ_TEST_CHECK(ev.detail_size >= offsetof(moq_goaway_event_t, timeout_ms) + 8);
        MOQ_TEST_CHECK(ev.u.goaway.new_session_uri.len == sizeof(uri) - 1);
        bool d16 = v == MOQ_VERSION_DRAFT_16;
        MOQ_TEST_CHECK(ev.u.goaway.has_timeout == !d16);
        MOQ_TEST_CHECK_EQ_U64(ev.u.goaway.timeout_ms, d16 ? 0 : 5000);
        moq_event_cleanup(&ev);
    }
    /* The receiver (drain timeout 1 ms of its own) stays DRAINING. */
    MOQ_TEST_CHECK_EQ_U64(moq_session_next_deadline_us(c), UINT64_MAX);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_tick(c, 10000000), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), MOQ_SESS_DRAINING);
    /* The sender closes with GOAWAY_TIMEOUT once its Timeout passes. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_tick(sv, 10 + 5000000), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv), MOQ_SESS_CLOSED);
    MOQ_TEST_CHECK_EQ_U64(closed_code(sv), MOQ_CLOSE_GOAWAY_TIMEOUT);
    destroy(c, sv);
    return failures;
}

/* -- The sender of a FETCH updates it ----------------------------------------- */

static bool fetch(moq_session_t *c, moq_session_t *sv, moq_fetch_t *ch, moq_fetch_t *sh,
                  bool accept)
{
    moq_fetch_cfg_t cfg;
    moq_fetch_cfg_init(&cfg);
    cfg.track_namespace = (moq_namespace_t){ g_parts, 1 };
    cfg.track_name = MOQ_BYTES_LITERAL("v");
    cfg.start_group = 0; cfg.start_object = 0; cfg.end_group = 10; cfg.end_object = 0;
    if (moq_session_fetch(c, &cfg, 2, ch) != MOQ_OK) return false;
    run(c, sv, 3);
    moq_event_t ev;
    if (!take(sv, MOQ_EVENT_FETCH_REQUEST, &ev)) return false;
    *sh = ev.u.fetch_request.fetch;
    moq_event_cleanup(&ev);
    if (accept) {
        moq_accept_fetch_cfg_t afc;
        moq_accept_fetch_cfg_init(&afc);
        afc.end_group = 10;
        if (moq_session_accept_fetch(sv, *sh, &afc, 4) != MOQ_OK) return false;
        run(c, sv, 5);
    }
    drain(c);
    return true;
}

static int fetch_slot(moq_session_t *s, moq_fetch_t h)
{
    for (size_t i = 0; i < s->fetch_cap; i++)
        if (s->fetches[i].state != MOQ_FETCH_FREE &&
            s->fetches[i].handle._opaque == h._opaque)
            return (int)i;
    return -1;
}

/* draft-22 §9.5: "The sender of a request (SUBSCRIBE, PUBLISH, FETCH, ...)
 * can later send a REQUEST_UPDATE"; a FETCH update carries
 * AUTHORIZATION_TOKEN and SUBSCRIBER_PRIORITY. */
static int test_fetch_update(void)
{
    int failures = 0;
    moq_session_t *c, *sv;
    MOQ_TEST_CHECK(pair(&c, &sv, MOQ_VERSION_DRAFT_22));
    moq_fetch_t cf, sf;
    MOQ_TEST_CHECK(fetch(c, sv, &cf, &sf, false));
    moq_fetch_update_cfg_t u;
    moq_fetch_update_cfg_init(&u);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_fetch(c, cf, &u, 6), MOQ_ERR_INVAL);
    u.has_subscriber_priority = true;
    u.subscriber_priority = 3;
    uint8_t tv[] = { 0x42 };
    moq_auth_token_t tok = { 5, { tv, sizeof(tv) } };
    u.auth_tokens = &tok;
    u.auth_token_count = 1;
    /* Before FETCH_OK: sent; the publisher applies it and answers after
     * FETCH_OK. One outstanding at a time. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_fetch(c, cf, &u, 6), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_fetch(c, cf, &u, 6), MOQ_ERR_WRONG_STATE);
    run(c, sv, 7);
    int fs = fetch_slot(sv, sf);
    MOQ_TEST_CHECK(fs >= 0);
    if (fs >= 0) MOQ_TEST_CHECK_EQ_INT((int)sv->fetches[fs].subscriber_priority, 3);
    moq_accept_fetch_cfg_t afc;
    moq_accept_fetch_cfg_init(&afc);
    afc.end_group = 10;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_fetch(sv, sf, &afc, 8), MOQ_OK);
    run(c, sv, 9);
    /* The acknowledgement arrived: another update may go. */
    u.auth_token_count = 0;
    u.subscriber_priority = 9;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_fetch(c, cf, &u, 10), MOQ_OK);
    run(c, sv, 11);
    if (fs >= 0) MOQ_TEST_CHECK_EQ_INT((int)sv->fetches[fs].subscriber_priority, 9);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv), MOQ_SESS_ESTABLISHED);
    /* The publisher's side cannot send one. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_fetch(sv, sf, &u, 12), MOQ_ERR_WRONG_STATE);
    destroy(c, sv);

    /* A refusal ends the fetch (§9.5.1): FETCH_ERROR with its code, the
     * handle retired, the session open. */
    MOQ_TEST_CHECK(pair(&c, &sv, MOQ_VERSION_DRAFT_22));
    MOQ_TEST_CHECK(fetch(c, sv, &cf, &sf, true));
    u.subscriber_priority = 1;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_fetch(c, cf, &u, 6), MOQ_OK);
    { moq_action_t a; while (moq_session_poll_actions(c, &a, 1) > 0) moq_action_cleanup(&a); }
    int cs = fetch_slot(c, cf);
    MOQ_TEST_CHECK(cs >= 0);
    if (cs >= 0) {
        moq_d22_message_t m;
        memset(&m, 0, sizeof(m));
        m.type = MOQ_D22_REQUEST_ERROR;
        m.request_id = 3;
        m.code = MOQ_REQUEST_ERROR_NOT_SUPPORTED;
        moq_stream_ref_t ref = c->fetches[cs].rs.ref;
        MOQ_TEST_CHECK_EQ_INT((int)feed(c, ref, &m, NULL, 0, MOQ_D22_FETCH, true, 7), MOQ_OK);
        moq_event_t ev;
        bool got = take(c, MOQ_EVENT_FETCH_ERROR, &ev);
        MOQ_TEST_CHECK(got);
        if (got) {
            MOQ_TEST_CHECK(ev.u.fetch_error.fetch._opaque == cf._opaque);
            MOQ_TEST_CHECK_EQ_U64(ev.u.fetch_error.error_code, MOQ_REQUEST_ERROR_NOT_SUPPORTED);
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_fetch(c, cf, &u, 8), MOQ_ERR_STALE_HANDLE);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), MOQ_SESS_ESTABLISHED);
    }
    destroy(c, sv);

    /* Draft-16 and draft-18 send no fetcher update. */
    moq_version_t legacy[] = { MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18 };
    for (size_t i = 0; i < 2; i++) {
        MOQ_TEST_CHECK(pair(&c, &sv, legacy[i]));
        MOQ_TEST_CHECK(fetch(c, sv, &cf, &sf, false));
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_fetch(c, cf, &u, 6), MOQ_ERR_UNSUPPORTED);
        destroy(c, sv);
    }
    return failures;
}

/* -- The sender of a PUBLISH updates it --------------------------------------- */

/* draft-22 §9.5: the sender of a PUBLISH may update its own request. It
 * refreshes authorization and states its own delivery timeouts; the
 * subscriber-controlled parameters and FILL_PARAMETERS stay the
 * subscriber's. */
static int test_publisher_update(void)
{
    int failures = 0;
    moq_session_t *c, *sv;
    MOQ_TEST_CHECK(pair(&c, &sv, MOQ_VERSION_DRAFT_22));
    moq_publication_t cp, sp;
    MOQ_TEST_CHECK(publish(c, sv, &cp, &sp));
    moq_publication_update_cfg_t u;
    moq_publication_update_cfg_init(&u);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_publication(c, cp, &u, 6), MOQ_ERR_INVAL);
    u.has_subscriber_priority = true; u.subscriber_priority = 1;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_publication(c, cp, &u, 6), MOQ_ERR_INVAL);
    moq_publication_update_cfg_init(&u);
    u.has_forward = true; u.forward = false;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_publication(c, cp, &u, 6), MOQ_ERR_INVAL);

    moq_publication_update_cfg_init(&u);
    uint8_t tv[] = { 0x10, 0x20 };
    moq_auth_token_t tok = { 4, { tv, sizeof(tv) } };
    u.auth_tokens = &tok;
    u.auth_token_count = 1;
    u.delivery_timeout_flags = MOQ_SUBSCRIPTION_UPDATE_OBJECT_DELIVERY_TIMEOUT;
    u.object_delivery_timeout_ms = 250;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_publication(c, cp, &u, 6), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_publication(c, cp, &u, 6), MOQ_ERR_WRONG_STATE);
    run(c, sv, 7);
    moq_event_t ev;
    bool got = take(sv, MOQ_EVENT_PUBLISH_UPDATED, &ev);
    MOQ_TEST_CHECK(got);
    if (got) {
        MOQ_TEST_CHECK(ev.u.publish_updated.pub._opaque == sp._opaque);
        MOQ_TEST_CHECK_EQ_SIZE(ev.u.publish_updated.token_count, 1);
        if (ev.u.publish_updated.token_count == 1)
            MOQ_TEST_CHECK(ev.u.publish_updated.tokens[0].token_type == 4 &&
                           ev.u.publish_updated.tokens[0].token_value.len == 2);
        moq_event_cleanup(&ev);
    }
    int sps = pub_resolve_handle(sv, sp);
    if (sps >= 0)
        MOQ_TEST_CHECK(sv->publishes[sps].dt_pub_has_object &&
                       sv->publishes[sps].dt_pub_object_ms == 250);
    run(c, sv, 8);
    got = take(c, MOQ_EVENT_PUBLICATION_UPDATE_OK, &ev);
    MOQ_TEST_CHECK(got);
    if (got) {
        MOQ_TEST_CHECK(ev.u.publication_update_ok.pub._opaque == cp._opaque);
        moq_event_cleanup(&ev);
    }
    int cps = pub_resolve_handle(c, cp);
    if (cps >= 0) {
        MOQ_TEST_CHECK(c->publishes[cps].dt_pub_has_object &&
                       c->publishes[cps].dt_pub_object_ms == 250);
        MOQ_TEST_CHECK(!c->publishes[cps].update_pending);
    }
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv), MOQ_SESS_ESTABLISHED);

    /* A refusal: PUBLISH_ERROR on the established publication; no further
     * update; the terminal must be UPDATE_FAILED (§9.5.1). */
    u.delivery_timeout_flags = 0;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_publication(c, cp, &u, 9), MOQ_OK);
    { moq_action_t a; while (moq_session_poll_actions(c, &a, 1) > 0) moq_action_cleanup(&a); }
    if (cps >= 0) {
        moq_d22_message_t m;
        memset(&m, 0, sizeof(m));
        m.type = MOQ_D22_REQUEST_ERROR;
        m.request_id = 5;
        m.code = MOQ_REQUEST_ERROR_NOT_SUPPORTED;
        MOQ_TEST_CHECK_EQ_INT((int)feed(c, c->publishes[cps].rs.ref, &m, NULL, 0,
                                        MOQ_D22_PUBLISH, false, 10), MOQ_OK);
        got = take(c, MOQ_EVENT_PUBLISH_ERROR, &ev);
        MOQ_TEST_CHECK(got);
        if (got) {
            MOQ_TEST_CHECK(ev.u.publish_error.pub._opaque == cp._opaque);
            MOQ_TEST_CHECK_EQ_U64(ev.u.publish_error.error_code, MOQ_REQUEST_ERROR_NOT_SUPPORTED);
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_publication(c, cp, &u, 11), MOQ_ERR_WRONG_STATE);
        moq_finish_publish_cfg_t fc;
        moq_finish_publish_cfg_init(&fc);
        fc.status_code = 0x0;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_finish_publish(c, cp, &fc, 12), MOQ_ERR_INVAL);
        fc.status_code = 0x8;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_finish_publish(c, cp, &fc, 12), MOQ_OK);
    }
    destroy(c, sv);

    /* Draft-18: the publisher's own update is not sent. */
    MOQ_TEST_CHECK(pair(&c, &sv, MOQ_VERSION_DRAFT_18));
    MOQ_TEST_CHECK(publish(c, sv, &cp, &sp));
    moq_publication_update_cfg_init(&u);
    u.auth_tokens = &tok;
    u.auth_token_count = 1;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_publication(c, cp, &u, 6), MOQ_ERR_UNSUPPORTED);
    destroy(c, sv);
    return failures;
}

/* The subscriber that refused a publisher's update of its PUBLISH (0x17,
 * an unknown token alias) holds the publisher to the terminal §9.5.1
 * obliges: PUBLISH_DONE with UPDATE_FAILED. Another status is the
 * failed-update violation an update this side sent would make it; the
 * required one ends the publication normally. */
static int test_refused_publisher_update_terminal(void)
{
    int failures = 0;
    for (int status_ok = 0; status_ok < 2; status_ok++) {
        moq_session_t *c, *sv;
        side_cfg_t sc = { .version = MOQ_VERSION_DRAFT_22, .token_cache = 64 };
        MOQ_TEST_CHECK(pair_cfg(&c, &sv, &sc, &sc, true));
        moq_publication_t ch, sh;
        MOQ_TEST_CHECK(publish(c, sv, &ch, &sh));
        int slot = pub_resolve_handle(sv, sh);
        MOQ_TEST_CHECK(slot >= 0);
        if (slot < 0) { destroy(c, sv); continue; }
        moq_stream_ref_t ref = sv->publishes[slot].rs.ref;
        /* REQUEST_UPDATE id 2, one AUTHORIZATION TOKEN: USE_ALIAS 9. */
        static const uint8_t update[] = { 0x02, 0x00, 0x06, 0x02, 0x01, 0x03, 0x02, 0x02, 0x09 };
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_bidi_stream_bytes(sv, ref, update,
                              sizeof(update), false, 6), MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(request_error_on(sv, ref, NULL), 0x17);
        moq_d22_message_t done;
        memset(&done, 0, sizeof(done));
        done.type = MOQ_D22_PUBLISH_DONE;
        done.code = status_ok ? 0x8 : 0x0;
        (void)feed(sv, ref, &done, NULL, 0, MOQ_D22_PUBLISH, false, 7);
        if (status_ok) {
            MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv), MOQ_SESS_ESTABLISHED);
            moq_event_t ev;
            bool got = take(sv, MOQ_EVENT_PUBLISH_FINISHED, &ev);
            MOQ_TEST_CHECK(got);
            if (got) {
                MOQ_TEST_CHECK_EQ_U64(ev.u.publish_finished.status_code, 0x8);
                moq_event_cleanup(&ev);
            }
        } else {
            MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv), MOQ_SESS_CLOSED);
            MOQ_TEST_CHECK_EQ_U64(closed_code(sv), 0x3);
        }
        destroy(c, sv);
    }
    return failures;
}

/* -- Refused own updates under event-arena pressure ---------------------------- */

/* A client with a small event arena against a default server. */
static bool small_pair(moq_session_t **c, moq_session_t **sv, uint32_t scratch)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), MOQ_PERSPECTIVE_CLIENT);
    cfg.version = MOQ_VERSION_DRAFT_22;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 64;
    cfg.output_scratch_size = scratch;
    *c = NULL;
    *sv = NULL;
    if (moq_session_create(&cfg, 0, c) != MOQ_OK) return false;
    if (test_bind_if_required(*c) != MOQ_OK || moq_session_start(*c, 0) != MOQ_OK) {
        moq_session_destroy(*c);
        return false;
    }
    side_cfg_t sc = { .version = MOQ_VERSION_DRAFT_22 };
    *sv = session(MOQ_PERSPECTIVE_SERVER, &sc);
    if (!*sv) { moq_session_destroy(*c); return false; }
    run(*c, *sv, 1);
    drain(*c); drain(*sv);
    return moq_session_state(*c) == MOQ_SESS_ESTABLISHED &&
           moq_session_state(*sv) == MOQ_SESS_ESTABLISHED;
}

/* Send this side's update of `pub` or `fetch`, drain the output, and name
 * its request stream. */
static bool send_own_update(moq_session_t *c, bool publication, moq_publication_t pub,
                            moq_fetch_t fetch, moq_stream_ref_t *ref)
{
    if (publication) {
        moq_publication_update_cfg_t u;
        moq_publication_update_cfg_init(&u);
        u.delivery_timeout_flags = MOQ_SUBSCRIPTION_UPDATE_OBJECT_DELIVERY_TIMEOUT;
        u.object_delivery_timeout_ms = 1;
        if (moq_session_update_publication(c, pub, &u, 6) != MOQ_OK) return false;
        *ref = c->publishes[pub_resolve_handle(c, pub)].rs.ref;
    } else {
        moq_fetch_update_cfg_t u;
        moq_fetch_update_cfg_init(&u);
        u.has_subscriber_priority = true;
        u.subscriber_priority = 1;
        if (moq_session_update_fetch(c, fetch, &u, 6) != MOQ_OK) return false;
        *ref = c->fetches[fetch_slot(c, fetch)].rs.ref;
    }
    moq_action_t a;
    while (moq_session_poll_actions(c, &a, 1) > 0) moq_action_cleanup(&a);
    return true;
}

static moq_result_t refuse_update(moq_session_t *c, moq_stream_ref_t ref, bool publication,
                                  size_t reason_len, uint8_t fill, uint64_t now)
{
    uint8_t reason[192];
    memset(reason, fill, sizeof(reason));
    moq_d22_message_t m;
    memset(&m, 0, sizeof(m));
    m.type = MOQ_D22_REQUEST_ERROR;
    m.code = MOQ_REQUEST_ERROR_NOT_SUPPORTED;
    m.reason = (moq_bytes_t){ reason, reason_len };
    return feed(c, ref, &m, NULL, 0, publication ? MOQ_D22_PUBLISH : MOQ_D22_FETCH,
                false, now);
}

/* Exactly one refusal outcome for the update on `c` -- no competing FETCH
 * terminal -- carrying its code and a reason of `reason_len` bytes of
 * `fill`, then the family's state after it. */
static int check_refusal(moq_session_t *c, bool publication, moq_publication_t pub,
                         moq_fetch_t fetch, size_t reason_len, uint8_t fill)
{
    int failures = 0;
    int mine = 0, competing = 0;
    moq_event_t ev;
    while (moq_session_poll_events(c, &ev, 1) > 0) {
        moq_bytes_t reason = { NULL, 0 };
        uint64_t code = UINT64_MAX;
        bool hit = false;
        if (publication && ev.kind == MOQ_EVENT_PUBLISH_ERROR) {
            hit = ev.u.publish_error.pub._opaque == pub._opaque;
            code = ev.u.publish_error.error_code;
            reason = ev.u.publish_error.reason;
        } else if (!publication && ev.kind == MOQ_EVENT_FETCH_ERROR) {
            hit = ev.u.fetch_error.fetch._opaque == fetch._opaque;
            code = ev.u.fetch_error.error_code;
            reason = ev.u.fetch_error.reason;
        } else if (ev.kind == MOQ_EVENT_FETCH_RESET || ev.kind == MOQ_EVENT_FETCH_CANCELLED ||
                   ev.kind == MOQ_EVENT_PUBLISH_FINISHED) {
            competing++;
        }
        if (hit) {
            mine++;
            MOQ_TEST_CHECK_EQ_U64(code, MOQ_REQUEST_ERROR_NOT_SUPPORTED);
            MOQ_TEST_CHECK_EQ_SIZE(reason.len, reason_len);
            for (size_t i = 0; i < reason.len && i < reason_len; i++)
                if (reason.data[i] != fill) { MOQ_TEST_CHECK(false); break; }
        }
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK_EQ_INT(mine, 1);
    MOQ_TEST_CHECK_EQ_INT(competing, 0);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), MOQ_SESS_ESTABLISHED);
    if (publication) {
        int slot = pub_resolve_handle(c, pub);
        MOQ_TEST_CHECK(slot >= 0);
        if (slot >= 0) MOQ_TEST_CHECK(c->publishes[slot].update_failed);
    } else {
        MOQ_TEST_CHECK_EQ_INT(fetch_slot(c, fetch), -1);
    }
    return failures;
}

/* §9.5.1: a refused update ends that request (FETCH) or obliges its
 * UPDATE_FAILED terminal (PUBLISH). The refusal is the request's outcome: a
 * reason phrase the configured event arena can never hold is not a session
 * failure. Its code is delivered with an empty reason; a reason the arena
 * holds only once earlier events are consumed waits for them, complete, also
 * across a later FIN or RESET of the request stream; other requests are
 * untouched. */
static int test_update_error_capacity(bool publication)
{
    int failures = 0;
    struct { size_t reason; size_t expect; } rows[] = { { 16, 16 }, { 120, 120 }, { 192, 0 } };
    for (size_t r = 0; r < sizeof(rows) / sizeof(rows[0]); r++) {
        moq_session_t *c, *sv;
        MOQ_TEST_CHECK(small_pair(&c, &sv, 128));
        moq_publication_t cp = { 0 }, sp;
        moq_fetch_t cf = { 0 }, sf;
        moq_subscription_t other_c, other_s;
        MOQ_TEST_CHECK(subscribe(c, sv, MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT, &other_c, &other_s));
        drain(c); drain(sv);
        if (publication) MOQ_TEST_CHECK(publish(c, sv, &cp, &sp));
        else MOQ_TEST_CHECK(fetch(c, sv, &cf, &sf, true));
        moq_stream_ref_t ref;
        MOQ_TEST_CHECK(send_own_update(c, publication, cp, cf, &ref));
        MOQ_TEST_CHECK_EQ_INT((int)refuse_update(c, ref, publication, rows[r].reason, 'x', 7),
                              MOQ_OK);
        failures += check_refusal(c, publication, cp, cf, rows[r].expect, 'x');
        MOQ_TEST_CHECK(sub_slot(c, other_c) >= 0);
        destroy(c, sv);
    }

    /* Temporarily occupied: an earlier event holds most of the arena. */
    for (int signal = 0; signal < 3; signal++) {
        moq_session_t *c, *sv;
        MOQ_TEST_CHECK(small_pair(&c, &sv, 128));
        moq_publication_t cp = { 0 }, sp;
        moq_fetch_t cf = { 0 }, sf;
        if (publication) MOQ_TEST_CHECK(publish(c, sv, &cp, &sp));
        else MOQ_TEST_CHECK(fetch(c, sv, &cf, &sf, true));
        /* The occupier: a refused TRACK_STATUS whose 100-byte reason is not
         * consumed yet. */
        moq_track_status_cfg_t tc;
        moq_track_status_cfg_init(&tc);
        tc.track_namespace = (moq_namespace_t){ g_parts, 1 };
        tc.track_name = MOQ_BYTES_LITERAL("w");
        moq_track_status_handle_t th;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_track_status(c, &tc, 6, &th), MOQ_OK);
        moq_stream_ref_t ts_ref = { 0 };
        {
            moq_action_t a;
            while (moq_session_poll_actions(c, &a, 1) > 0) {
                if (a.kind == MOQ_ACTION_OPEN_BIDI_STREAM) ts_ref = a.u.open_bidi_stream.stream_ref;
                moq_action_cleanup(&a);
            }
        }
        moq_stream_ref_t ref;
        MOQ_TEST_CHECK(send_own_update(c, publication, cp, cf, &ref));
        uint8_t occupier[100];
        memset(occupier, 'o', sizeof(occupier));
        moq_d22_message_t m;
        memset(&m, 0, sizeof(m));
        m.type = MOQ_D22_REQUEST_ERROR;
        m.code = MOQ_REQUEST_ERROR_NOT_SUPPORTED;
        m.reason = (moq_bytes_t){ occupier, sizeof(occupier) };
        MOQ_TEST_CHECK_EQ_INT((int)feed(c, ts_ref, &m, NULL, 0, MOQ_D22_TRACK_STATUS, false, 7),
                              MOQ_OK);
        /* Held complete, nothing changed yet (the input contract: retry
         * with no bytes once events are consumed). */
        MOQ_TEST_CHECK_EQ_INT((int)refuse_update(c, ref, publication, 100, 'y', 8),
                              MOQ_ERR_WOULD_BLOCK);
        if (publication) {
            int slot = pub_resolve_handle(c, cp);
            MOQ_TEST_CHECK(slot >= 0 && !c->publishes[slot].update_failed);
        } else {
            MOQ_TEST_CHECK(fetch_slot(c, cf) >= 0);
        }
        /* A peer RESET after the refusal waits with it. */
        if (signal == 1)
            (void)moq_session_on_bidi_stream_bytes(c, ref, NULL, 0, true, 9);
        else if (signal == 2)
            MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_bidi_stream_reset(c, ref, 0x5, 9),
                                  MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), MOQ_SESS_ESTABLISHED);
        moq_event_t ev;
        bool got = moq_session_poll_events(c, &ev, 1) > 0;
        MOQ_TEST_CHECK(got && ev.kind == MOQ_EVENT_TRACK_STATUS_ERROR);
        if (got) moq_event_cleanup(&ev);
        if (signal != 2)
            MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_bidi_stream_bytes(c, ref, NULL, 0,
                                  signal == 1, 10), MOQ_OK);
        else
            MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_bidi_stream_reset(c, ref, 0x5, 10),
                                  MOQ_OK);
        if (signal == 2 && publication) {
            /* The refusal, then the reset that ends the publication. */
            got = moq_session_poll_events(c, &ev, 1) > 0;
            MOQ_TEST_CHECK(got && ev.kind == MOQ_EVENT_PUBLISH_ERROR);
            if (got) {
                MOQ_TEST_CHECK_EQ_SIZE(ev.u.publish_error.reason.len, 100);
                MOQ_TEST_CHECK_EQ_U64(ev.u.publish_error.error_code,
                                      MOQ_REQUEST_ERROR_NOT_SUPPORTED);
                moq_event_cleanup(&ev);
            }
            got = moq_session_poll_events(c, &ev, 1) > 0;
            MOQ_TEST_CHECK(got && ev.kind == MOQ_EVENT_PUBLISH_UNSUBSCRIBED);
            if (got) {
                MOQ_TEST_CHECK_EQ_INT((int)ev.u.publish_unsubscribed.cause,
                                      (int)MOQ_REQUEST_TERMINAL_PEER_RESET_STREAM);
                moq_event_cleanup(&ev);
            }
            MOQ_TEST_CHECK_EQ_INT(pub_resolve_handle(c, cp), -1);
            MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), MOQ_SESS_ESTABLISHED);
        } else {
            (void)moq_session_tick(c, 11);
            failures += check_refusal(c, publication, cp, cf, 100, 'y');
        }
        destroy(c, sv);
    }
    return failures;
}

/* -- The initial window of a PUBLISH ------------------------------------------ */

/* The publication window both sides of one PUBLISH hold once it is
 * acknowledged. The publisher's track history may move between its PUBLISH
 * and the answer (`advance`: another subscription's or publication's object
 * at 7/3); the window still resolves against the Largest the PUBLISH carried
 * (draft-22 §9.8: delivery starts relative to "the Largest Object at the
 * time the publisher begins sending"), which is all the subscriber knows. */
static int publish_window_basis(moq_version_t v, moq_location_filter_kind_t kind,
                                moq_subscribe_filter_t legacy, bool advance,
                                uint64_t expect_group, uint64_t expect_object)
{
    int failures = 0;
    moq_session_t *c, *sv;
    MOQ_TEST_CHECK(pair(&c, &sv, v));
    moq_namespace_t ns = { g_parts, 1 };
    moq_bytes_t name = MOQ_BYTES_LITERAL("v");
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_note_object_published(c, &ns, name, 5, 2), MOQ_OK);
    moq_location_filter_cfg_t loc;
    moq_location_filter_cfg_init(&loc);
    loc.kind = kind;
    loc.relative_group_offset = 1;
    moq_publish_cfg_t pc;
    moq_publish_cfg_init_sized(&pc, sizeof(pc));
    pc.track_namespace = ns;
    pc.track_name = name;
    if (v == MOQ_VERSION_DRAFT_22) pc.location = &loc;
    moq_publication_t ch, sh;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_publish(c, &pc, 2, &ch), MOQ_OK);
    run(c, sv, 3);
    moq_event_t ev;
    bool got = take(sv, MOQ_EVENT_PUBLISH_REQUEST, &ev);
    MOQ_TEST_CHECK(got);
    if (!got) { destroy(c, sv); return failures; }
    sh = ev.u.publish_request.pub;
    moq_event_cleanup(&ev);
    if (advance)
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_note_object_published(c, &ns, name, 7, 3), MOQ_OK);
    moq_accept_publish_cfg_t ac;
    moq_accept_publish_cfg_init_sized(&ac, sizeof(ac));
    if (v != MOQ_VERSION_DRAFT_22) ac.filter = legacy;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_publish(sv, sh, &ac, 4), MOQ_OK);
    run(c, sv, 5);
    moq_filter_window_t cw, sw;
    memset(&cw, 0, sizeof(cw));
    memset(&sw, 0, sizeof(sw));
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_publication_window(c, ch, &cw, sizeof(cw)), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_publication_window(sv, sh, &sw, sizeof(sw)), MOQ_OK);
    MOQ_TEST_CHECK(cw.resolved && sw.resolved);
    MOQ_TEST_CHECK_EQ_U64(cw.start_group, expect_group);
    MOQ_TEST_CHECK_EQ_U64(cw.start_object, expect_object);
    MOQ_TEST_CHECK_EQ_U64(sw.start_group, expect_group);
    MOQ_TEST_CHECK_EQ_U64(sw.start_object, expect_object);
    destroy(c, sv);
    return failures;
}

static int test_publish_window_basis(void)
{
    int failures = 0;
    for (int advance = 0; advance < 2; advance++) {
        failures += publish_window_basis(MOQ_VERSION_DRAFT_22, MOQ_LOCATION_FILTER_NEXT_OBJECT,
                                         MOQ_SUBSCRIBE_FILTER_NONE, advance, 5, 3);
        failures += publish_window_basis(MOQ_VERSION_DRAFT_22, MOQ_LOCATION_FILTER_RELATIVE_GROUP,
                                         MOQ_SUBSCRIBE_FILTER_NONE, advance, 5, 0);
        failures += publish_window_basis(MOQ_VERSION_DRAFT_18, MOQ_LOCATION_FILTER_NONE,
                                         MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT, advance, 5, 3);
        failures += publish_window_basis(MOQ_VERSION_DRAFT_18, MOQ_LOCATION_FILTER_NONE,
                                         MOQ_SUBSCRIBE_FILTER_NEXT_GROUP, advance, 6, 0);
    }
    return failures;
}

/* -- The resolved Location window ----------------------------------------------- */

static int test_filter_window(moq_version_t v)
{
    int failures = 0;
    moq_session_t *c, *sv;
    MOQ_TEST_CHECK(pair(&c, &sv, v));
    moq_subscribe_cfg_t cfg;
    moq_subscribe_cfg_init(&cfg);
    cfg.track_namespace = (moq_namespace_t){ g_parts, 1 };
    cfg.track_name = MOQ_BYTES_LITERAL("v");
    cfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    moq_subscription_t ch, sh;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscribe(c, &cfg, 2, &ch), MOQ_OK);
    moq_filter_window_t w;
    memset(&w, 0xa5, sizeof(w));
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscription_window(c, ch, &w, sizeof(w)), MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(w.struct_size, sizeof(w));
    MOQ_TEST_CHECK(!w.resolved);            /* nothing answered yet */
    run(c, sv, 3);
    moq_event_t ev;
    MOQ_TEST_CHECK(take(sv, MOQ_EVENT_SUBSCRIBE_REQUEST, &ev));
    sh = ev.u.subscribe_request.sub;
    moq_event_cleanup(&ev);
    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    ac.has_largest = true; ac.largest_group = 5; ac.largest_object = 2;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_subscribe(sv, sh, &ac, 4), MOQ_OK);
    /* Largest Object {5,2}: the window starts at the next object. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscription_window(sv, sh, &w, sizeof(w)), MOQ_OK);
    MOQ_TEST_CHECK(w.resolved && !w.empty && !w.has_end);
    MOQ_TEST_CHECK(w.start_group == 5 && w.start_object == 3);
    run(c, sv, 5);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscription_window(c, ch, &w, sizeof(w)), MOQ_OK);
    MOQ_TEST_CHECK(w.resolved && w.start_group == 5 && w.start_object == 3 && !w.has_end);

    /* A narrowing update to a bounded window, as the subscriber's own. */
    moq_location_filter_cfg_t loc;
    moq_location_filter_cfg_init(&loc);
    loc.kind = MOQ_LOCATION_FILTER_GROUP_END;
    loc.start_group = 6; loc.start_object = 0; loc.end_group = 9;
    moq_subscription_update_cfg_t u;
    moq_subscription_update_cfg_init_sized(&u, sizeof(u));
    bool typed = v == MOQ_VERSION_DRAFT_22;
    if (typed) {
        u.location = &loc;
    } else {
        u.has_filter = true;
        u.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE;
        u.start_group = 6; u.start_object = 0; u.end_group = 9;
    }
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_subscription(c, ch, &u, 6), MOQ_OK);
    run(c, sv, 7);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscription_window(sv, sh, &w, sizeof(w)), MOQ_OK);
    MOQ_TEST_CHECK(w.resolved && w.start_group == 6 && w.start_object == 0 &&
                   w.has_end && w.end_group == 9 && !w.has_end_object);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscription_window(c, ch, &w, sizeof(w)), MOQ_OK);
    MOQ_TEST_CHECK(w.resolved && w.start_group == 6 && w.has_end && w.end_group == 9);

    /* Size-aware: a short buffer is refused untouched, a longer one gets the
     * library's size stamped. */
    uint8_t small[sizeof(w) - 1];
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscription_window(c, ch,
                          (moq_filter_window_t *)(void *)small, sizeof(small)), MOQ_ERR_INVAL);
    struct { moq_filter_window_t w; uint64_t extra; } big;
    memset(&big, 0xa5, sizeof(big));
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscription_window(c, ch, &big.w, sizeof(big)), MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(big.w.struct_size, sizeof(w));
    MOQ_TEST_CHECK_EQ_U64(big.extra, UINT64_C(0xa5a5a5a5a5a5a5a5));
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscription_window(c, (moq_subscription_t){ 0 }, &w,
                          sizeof(w)), MOQ_ERR_STALE_HANDLE);
    destroy(c, sv);

    /* A PUBLISH-initiated subscription: both sides read its window. */
    MOQ_TEST_CHECK(pair(&c, &sv, v));
    moq_publication_t cp, sp;
    MOQ_TEST_CHECK(publish(c, sv, &cp, &sp));
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_publication_window(c, cp, &w, sizeof(w)), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_publication_window(sv, sp, &w, sizeof(w)), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_publication_window(sv, (moq_publication_t){ 0 }, &w,
                          sizeof(w)), MOQ_ERR_STALE_HANDLE);
    destroy(c, sv);

    /* The Location Filter a PUBLISH carries is the subscription's initial
     * filter: the side that accepts it holds the same window as the sender. */
    if (typed) {
        MOQ_TEST_CHECK(pair(&c, &sv, v));
        moq_location_filter_cfg_t pl;
        moq_location_filter_cfg_init(&pl);
        pl.kind = MOQ_LOCATION_FILTER_GROUP_END;
        pl.start_group = 2; pl.start_object = 0; pl.end_group = 9;
        MOQ_TEST_CHECK(publish_filtered(c, sv, &cp, &sp, &pl));
        moq_session_t *sides[2] = { c, sv };
        moq_publication_t handles[2] = { cp, sp };
        for (int i = 0; i < 2; i++) {
            memset(&w, 0, sizeof(w));
            MOQ_TEST_CHECK_EQ_INT((int)moq_session_publication_window(sides[i], handles[i], &w,
                                  sizeof(w)), MOQ_OK);
            MOQ_TEST_CHECK(w.resolved && !w.empty && w.start_group == 2 && w.start_object == 0 &&
                           w.has_end && w.end_group == 9 && !w.has_end_object);
        }
        destroy(c, sv);
    }
    return failures;
}

int main(void)
{
    int failures = 0;
    failures += test_range_parameters();
    failures += test_fill_direction();
    failures += test_fill_count();
    failures += test_fill_emission();
    failures += test_token_alias_outcomes();
    failures += test_setup_tokens(MOQ_VERSION_DRAFT_16);
    failures += test_setup_tokens(MOQ_VERSION_DRAFT_18);
    failures += test_setup_tokens(MOQ_VERSION_DRAFT_22);
    failures += test_setup_token_cfg();
    failures += test_setup_token_cap(MOQ_VERSION_DRAFT_16);
    failures += test_setup_token_cap(MOQ_VERSION_DRAFT_18);
    failures += test_setup_token_cap(MOQ_VERSION_DRAFT_22);
    failures += test_goaway_timeout(MOQ_VERSION_DRAFT_16);
    failures += test_goaway_timeout(MOQ_VERSION_DRAFT_18);
    failures += test_goaway_timeout(MOQ_VERSION_DRAFT_22);
    failures += test_fetch_update();
    failures += test_publisher_update();
    failures += test_refused_publisher_update_terminal();
    failures += test_update_error_capacity(false);
    failures += test_update_error_capacity(true);
    failures += test_publish_window_basis();
    failures += test_filter_window(MOQ_VERSION_DRAFT_18);
    failures += test_filter_window(MOQ_VERSION_DRAFT_22);
    if (failures) { fprintf(stderr, "FAIL: test_d22_session_surfaces (%d)\n", failures); return 1; }
    printf("PASS: test_d22_session_surfaces\n");
    return 0;
}
