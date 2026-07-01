/*
 * Unit tests for SUBSCRIBE_NAMESPACE bidi-stream transport skeleton.
 *
 * Tests: outbound subscribe action, inbound split-byte parsing,
 * two-stage commit, accept/reject response, WB retry, request-ID
 * validation, control-stream rejection, pool full, allocator balance.
 */

#include <moq/session.h>
#include <moq/codec.h>
#include "test_support.h"
#include "test_session_support.h"
#include "../../core/src/session/session_internal.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* -- Encode a framed SUBSCRIBE_NAMESPACE --------------------------- */

static size_t encode_sub_ns(uint8_t *buf, size_t cap,
                             uint64_t request_id,
                             const char *ns_field,
                             uint64_t options)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    moq_bytes_t parts[] = { moq_bytes_cstr(ns_field) };
    moq_namespace_t prefix = { parts, 1 };
    moq_d16_encode_subscribe_namespace(&w, request_id, &prefix,
                                        options, NULL, 0);
    return moq_buf_writer_offset(&w);
}

/* -- Build raw malformed SUBSCRIBE_NAMESPACE bytes ----------------- */

static size_t encode_sub_ns_raw(uint8_t *buf, size_t cap,
                                 uint64_t request_id,
                                 const char *ns_field,
                                 uint64_t options)
{
    uint8_t payload[128];
    moq_buf_writer_t pw;
    moq_buf_writer_init(&pw, payload, sizeof(payload));
    moq_buf_write_varint(&pw, request_id);
    moq_buf_write_varint(&pw, 1);
    moq_buf_write_span(&pw, moq_bytes_cstr(ns_field));
    moq_buf_write_varint(&pw, options);
    moq_buf_write_varint(&pw, 0);
    size_t plen = moq_buf_writer_offset(&pw);

    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    moq_control_encode_envelope(&w, MOQ_D16_SUBSCRIBE_NAMESPACE,
                                 payload, (uint16_t)plen);
    return moq_buf_writer_offset(&w);
}

/* -- Encode response messages --------------------------------------- */

static size_t encode_request_ok(uint8_t *buf, size_t cap, uint64_t request_id)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    moq_d16_encode_request_ok(&w, request_id, NULL, 0);
    return moq_buf_writer_offset(&w);
}

static size_t encode_request_error(uint8_t *buf, size_t cap,
                                    uint64_t request_id,
                                    uint64_t error_code,
                                    const char *reason)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    moq_d16_encode_request_error(&w, request_id, error_code, 0,
                                  (const uint8_t *)reason,
                                  reason ? strlen(reason) : 0);
    return moq_buf_writer_offset(&w);
}

static size_t encode_namespace_msg(uint8_t *buf, size_t cap,
                                    uint64_t msg_type,
                                    const char *suffix_field)
{
    uint8_t payload[128];
    moq_buf_writer_t pw;
    moq_buf_writer_init(&pw, payload, sizeof(payload));
    if (suffix_field) {
        moq_bytes_t parts[] = { moq_bytes_cstr(suffix_field) };
        moq_namespace_t ns = { parts, 1 };
        moq_buf_write_namespace_prefix(&pw, &ns);
    } else {
        moq_namespace_t ns = { NULL, 0 };
        moq_buf_write_namespace_prefix(&pw, &ns);
    }
    size_t plen = moq_buf_writer_offset(&pw);

    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    moq_control_encode_envelope(&w, msg_type, payload, (uint16_t)plen);
    return moq_buf_writer_offset(&w);
}

/* Helper: subscribe on client, drain OPEN_BIDI_STREAM action,
 * return the stream_ref and request_id for feeding responses. */
static moq_result_t setup_subscriber(moq_session_t *c,
                                      moq_ns_sub_handle_t *out_handle,
                                      moq_stream_ref_t *out_ref,
                                      uint64_t *out_request_id)
{
    moq_subscribe_namespace_cfg_t cfg;
    moq_subscribe_namespace_cfg_init(&cfg);
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("test") };
    cfg.track_namespace_prefix.parts = parts;
    cfg.track_namespace_prefix.count = 1;

    moq_result_t rc = moq_session_subscribe_namespace(c, &cfg, 0, out_handle);
    if (rc != MOQ_OK) return rc;

    moq_action_t acts[4]; size_t na;
    na = moq_session_poll_actions(c, acts, 4);
    for (size_t i = 0; i < na; i++) {
        if (acts[i].kind == MOQ_ACTION_OPEN_BIDI_STREAM) {
            *out_ref = acts[i].u.open_bidi_stream.stream_ref;
            /* Decode the request_id from the encoded SUBSCRIBE_NAMESPACE. */
            moq_control_envelope_t env;
            moq_buf_reader_t r;
            moq_buf_reader_init(&r, acts[i].u.open_bidi_stream.data,
                                 acts[i].u.open_bidi_stream.len);
            moq_control_decode_envelope(&r, &env);
            moq_bytes_t dp[32];
            moq_kvp_entry_t dparams[4];
            moq_d16_subscribe_namespace_t sub = {
                .track_namespace_prefix = { dp, 0 },
                .params = dparams, .params_cap = 4,
            };
            moq_d16_decode_subscribe_namespace(
                env.payload, env.payload_len, dp, 32, &sub);
            *out_request_id = sub.request_id;
        }
        moq_action_cleanup(&acts[i]);
    }
    return MOQ_OK;
}

/* -- Drain helper -------------------------------------------------- */

#define DRAIN_BOTH(c, sv) do { \
    moq_event_t _d[8]; moq_action_t _a[8]; size_t _ne, _na; \
    while ((_ne = moq_session_poll_events(c, _d, 8)) > 0) \
        for (size_t _i = 0; _i < _ne; _i++) moq_event_cleanup(&_d[_i]); \
    while ((_ne = moq_session_poll_events(sv, _d, 8)) > 0) \
        for (size_t _i = 0; _i < _ne; _i++) moq_event_cleanup(&_d[_i]); \
    while ((_na = moq_session_poll_actions(c, _a, 8)) > 0) \
        for (size_t _i = 0; _i < _na; _i++) moq_action_cleanup(&_a[_i]); \
    while ((_na = moq_session_poll_actions(sv, _a, 8)) > 0) \
        for (size_t _i = 0; _i < _na; _i++) moq_action_cleanup(&_a[_i]); \
} while (0)

int main(void)
{
    int failures = 0;

    /* == 1. subscribe_namespace → OPEN_BIDI_STREAM action ============= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

        moq_subscribe_namespace_cfg_t cfg;
        moq_subscribe_namespace_cfg_init(&cfg);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("test") };
        cfg.track_namespace_prefix.parts = parts;
        cfg.track_namespace_prefix.count = 1;
        cfg.namespace_interest = 2;

        moq_ns_sub_handle_t h;
        MOQ_TEST_CHECK(moq_session_subscribe_namespace(c, &cfg, 0, &h) == MOQ_OK);
        MOQ_TEST_CHECK(moq_ns_sub_handle_is_valid(h));

        moq_action_t acts[4];
        size_t na = moq_session_poll_actions(c, acts, 4);
        bool found_bidi = false;
        for (size_t i = 0; i < na; i++) {
            if (acts[i].kind == MOQ_ACTION_OPEN_BIDI_STREAM) {
                found_bidi = true;
                MOQ_TEST_CHECK(acts[i].u.open_bidi_stream.len > 0);

                moq_control_envelope_t env;
                moq_buf_reader_t r;
                moq_buf_reader_init(&r, acts[i].u.open_bidi_stream.data,
                                     acts[i].u.open_bidi_stream.len);
                MOQ_TEST_CHECK(moq_control_decode_envelope(&r, &env) >= 0);
                MOQ_TEST_CHECK(env.msg_type == MOQ_D16_SUBSCRIBE_NAMESPACE);

                moq_bytes_t dp[32];
                moq_kvp_entry_t dparams[4];
                moq_d16_subscribe_namespace_t sub = {
                    .track_namespace_prefix = { dp, 0 },
                    .params = dparams, .params_cap = 4,
                };
                MOQ_TEST_CHECK(moq_d16_decode_subscribe_namespace(
                    env.payload, env.payload_len, dp, 32, &sub) >= 0);
                MOQ_TEST_CHECK(sub.subscribe_options == 2);
                MOQ_TEST_CHECK(sub.track_namespace_prefix.count == 1);
            }
            moq_action_cleanup(&acts[i]);
        }
        MOQ_TEST_CHECK(found_bidi);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 2. WB on open action → no committed state ==================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t tiny = MOQ_SESSION_CFG_INIT;
        tiny.alloc = &alloc;
        tiny.max_actions = 2;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, &tiny, NULL);

        /* Fill 2-slot action queue with harmless credit updates. */
        moq_session_update_max_request_id(c, 128, 0);
        moq_session_update_max_request_id(c, 256, 0);

        moq_subscribe_namespace_cfg_t cfg;
        moq_subscribe_namespace_cfg_init(&cfg);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("wb") };
        cfg.track_namespace_prefix.parts = parts;
        cfg.track_namespace_prefix.count = 1;

        moq_ns_sub_handle_t h = MOQ_NS_SUB_HANDLE_INVALID;
        MOQ_TEST_CHECK(moq_session_subscribe_namespace(c, &cfg, 0, &h) == MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK(!moq_ns_sub_handle_is_valid(h));

        /* Drain the credit updates. */
        moq_action_t acts[4]; size_t na;
        while ((na = moq_session_poll_actions(c, acts, 4)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);

        /* Retry succeeds. */
        MOQ_TEST_CHECK(moq_session_subscribe_namespace(c, &cfg, 0, &h) == MOQ_OK);
        MOQ_TEST_CHECK(moq_ns_sub_handle_is_valid(h));

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 3. Request-ID credit exhausted → REQUEST_BLOCKED ============= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 2, 2, &c, &sv, NULL, NULL);

        moq_subscribe_namespace_cfg_t cfg;
        moq_subscribe_namespace_cfg_init(&cfg);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("cr") };
        cfg.track_namespace_prefix.parts = parts;
        cfg.track_namespace_prefix.count = 1;
        moq_ns_sub_handle_t h;
        MOQ_TEST_CHECK(moq_session_subscribe_namespace(c, &cfg, 0, &h) == MOQ_OK);
        moq_ns_sub_handle_t h2;
        MOQ_TEST_CHECK(moq_session_subscribe_namespace(c, &cfg, 0, &h2) == MOQ_ERR_INVAL);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 4. Split-byte inbound SUBSCRIBE_NAMESPACE ==================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

        uint8_t msg[256];
        size_t msg_len = encode_sub_ns(msg, sizeof(msg), 0, "split", 0);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(100);

        for (size_t i = 0; i < msg_len; i++) {
            moq_result_t rc = moq_session_on_bidi_stream_bytes(
                sv, ref, msg + i, 1, false, 0);
            if (i < msg_len - 1)
                MOQ_TEST_CHECK(rc == MOQ_OK);
            else
                MOQ_TEST_CHECK(rc == MOQ_OK);
        }

        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NS_SUB_REQUEST);
        MOQ_TEST_CHECK(ev.u.ns_sub_request.namespace_interest == 0);
        MOQ_TEST_CHECK(ev.u.ns_sub_request.track_namespace_prefix.count == 1);
        MOQ_TEST_CHECK(moq_ns_sub_handle_is_valid(ev.u.ns_sub_request.handle));
        moq_event_cleanup(&ev);

        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 0);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 4b. Inbound SUBSCRIBE_NAMESPACE after local GOAWAY refused ==== */
    {
        /* After we send GOAWAY (§10.4) the peer must not open new requests --
         * including a SUBSCRIBE_NAMESPACE on a fresh bidi. */
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

        MOQ_TEST_CHECK(moq_session_goaway(sv, NULL, 0, 0) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_DRAINING);

        uint8_t msg[256];
        size_t msg_len = encode_sub_ns(msg, sizeof(msg), 0, "late", 0);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(150);
        moq_session_on_bidi_stream_bytes(sv, ref, msg, msg_len, false, 0);

        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);
        moq_event_t ev;
        bool saw_ns_sub = false, saw_closed = false;
        while (moq_session_poll_events(sv, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_NS_SUB_REQUEST) saw_ns_sub = true;
            if (ev.kind == MOQ_EVENT_SESSION_CLOSED) {
                saw_closed = true;
                MOQ_TEST_CHECK(ev.u.closed.code == 0x3);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(!saw_ns_sub);
        MOQ_TEST_CHECK(saw_closed);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 4a. Partial inbound NS stream must not poison outbound ======== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

        /* Peer opens a SUBSCRIBE_NAMESPACE bidi but sends only an incomplete
         * fragment (no FIN): the server holds a RECVING_PUBLISHER slot whose
         * prefix has not been decoded yet (prefix_count==0). */
        uint8_t msg[256];
        size_t msg_len = encode_sub_ns(msg, sizeof(msg), 0, "incomplete", 0);
        (void)msg_len;
        moq_stream_ref_t ref_a = moq_stream_ref_from_u64(100);
        MOQ_TEST_CHECK(moq_session_on_bidi_stream_bytes(
            sv, ref_a, msg, 1, false, 0) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 0);

        /* The server's own outbound namespace subscription with a concrete
         * prefix must succeed: the unparsed inbound slot is not a real
         * "match-all" empty prefix and must not be treated as overlapping. */
        moq_subscribe_namespace_cfg_t cfg;
        moq_subscribe_namespace_cfg_init(&cfg);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("video") };
        cfg.track_namespace_prefix.parts = parts;
        cfg.track_namespace_prefix.count = 1;
        moq_ns_sub_handle_t h;
        MOQ_TEST_CHECK(moq_session_subscribe_namespace(sv, &cfg, 0, &h)
            == MOQ_OK);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 4b. Partial inbound NS stream must not poison later inbound === */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

        /* Stream A: incomplete SUBSCRIBE_NAMESPACE fragment, no FIN. */
        uint8_t msg_a[256];
        (void)encode_sub_ns(msg_a, sizeof(msg_a), 0, "partial", 0);
        moq_stream_ref_t ref_a = moq_stream_ref_from_u64(100);
        MOQ_TEST_CHECK(moq_session_on_bidi_stream_bytes(
            sv, ref_a, msg_a, 1, false, 0) == MOQ_OK);

        /* Stream B: a complete, disjoint SUBSCRIBE_NAMESPACE. It must be
         * accepted (NS_SUB_REQUEST), not rejected with PREFIX_OVERLAP by the
         * stale unparsed slot from stream A. */
        /* request_id 0: stream A never completed, so the next expected inbound
         * peer request id is still 0. */
        uint8_t msg_b[256];
        size_t len_b = encode_sub_ns(msg_b, sizeof(msg_b), 0, "complete", 0);
        moq_stream_ref_t ref_b = moq_stream_ref_from_u64(200);
        MOQ_TEST_CHECK(moq_session_on_bidi_stream_bytes(
            sv, ref_b, msg_b, len_b, false, 0) == MOQ_OK);

        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NS_SUB_REQUEST);
        MOQ_TEST_CHECK(ev.u.ns_sub_request.track_namespace_prefix.count == 1);
        moq_event_cleanup(&ev);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 4c. A parsed empty prefix still conflicts (wildcard preserved) = */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

        /* A genuinely parsed empty prefix (count==0) means "match all" and
         * must still overlap any later concrete prefix. */
        moq_subscribe_namespace_cfg_t empty_cfg;
        moq_subscribe_namespace_cfg_init(&empty_cfg);
        empty_cfg.track_namespace_prefix.parts = NULL;
        empty_cfg.track_namespace_prefix.count = 0;
        moq_ns_sub_handle_t he;
        MOQ_TEST_CHECK(moq_session_subscribe_namespace(c, &empty_cfg, 0, &he)
            == MOQ_OK);

        moq_subscribe_namespace_cfg_t cfg;
        moq_subscribe_namespace_cfg_init(&cfg);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("video") };
        cfg.track_namespace_prefix.parts = parts;
        cfg.track_namespace_prefix.count = 1;
        moq_ns_sub_handle_t h;
        MOQ_TEST_CHECK(moq_session_subscribe_namespace(c, &cfg, 0, &h)
            == MOQ_ERR_INVAL);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 5. Event queue full → WB, retry emits event ================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t tiny = MOQ_SESSION_CFG_INIT;
        tiny.alloc = &alloc;
        tiny.max_events = 1;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, &tiny);

        /* Fill server event queue with a SUBSCRIBE_REQUEST (uses req ID 0). */
        feed_subscribe(sv, 0, "ns", "fill", NULL, 0);

        uint8_t msg[256];
        size_t msg_len = encode_sub_ns(msg, sizeof(msg), 2, "wb", 1);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(200);

        moq_result_t rc = moq_session_on_bidi_stream_bytes(
            sv, ref, msg, msg_len, false, 0);
        MOQ_TEST_CHECK(rc == MOQ_ERR_WOULD_BLOCK);

        /* Drain filler → exactly 1 SUBSCRIBE_REQUEST. */
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        moq_event_cleanup(&ev);

        /* Retry → emits NS_SUB_REQUEST. */
        rc = moq_session_on_bidi_stream_bytes(sv, ref, NULL, 0, false, 0);
        MOQ_TEST_CHECK(rc == MOQ_OK);

        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NS_SUB_REQUEST);
        MOQ_TEST_CHECK(ev.u.ns_sub_request.namespace_interest == 1);
        moq_event_cleanup(&ev);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 6. No duplicate event on repeated retry ====================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t tiny = MOQ_SESSION_CFG_INIT;
        tiny.alloc = &alloc;
        tiny.max_events = 1;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, &tiny);

        /* Fill server event queue. */
        feed_subscribe(sv, 0, "ns", "fill2", NULL, 0);

        uint8_t msg[256];
        size_t msg_len = encode_sub_ns(msg, sizeof(msg), 2, "dup", 2);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(300);

        moq_result_t rc = moq_session_on_bidi_stream_bytes(
            sv, ref, msg, msg_len, false, 0);
        MOQ_TEST_CHECK(rc == MOQ_ERR_WOULD_BLOCK);

        /* Drain filler. */
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        moq_event_cleanup(&ev);

        /* First retry → OK, emits event. */
        rc = moq_session_on_bidi_stream_bytes(sv, ref, NULL, 0, false, 0);
        MOQ_TEST_CHECK(rc == MOQ_OK);

        /* Second retry → OK, no duplicate. */
        rc = moq_session_on_bidi_stream_bytes(sv, ref, NULL, 0, false, 0);
        MOQ_TEST_CHECK(rc == MOQ_OK);

        /* Exactly one NS_SUB_REQUEST, no more. */
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NS_SUB_REQUEST);
        moq_event_cleanup(&ev);
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 0);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 7. Malformed namespace interest → close ======================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

        uint8_t msg[256];
        size_t msg_len = encode_sub_ns_raw(msg, sizeof(msg), 0, "bad", 3);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(400);

        moq_session_on_bidi_stream_bytes(sv, ref, msg, msg_len, false, 0);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 8. accept → SEND_BIDI_STREAM with REQUEST_OK ================= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

        uint8_t msg[256];
        size_t msg_len = encode_sub_ns(msg, sizeof(msg), 0, "acc", 0);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(500);
        moq_session_on_bidi_stream_bytes(sv, ref, msg, msg_len, false, 0);

        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NS_SUB_REQUEST);
        moq_ns_sub_handle_t h = ev.u.ns_sub_request.handle;
        moq_event_cleanup(&ev);

        moq_accept_ns_sub_cfg_t acc;
        moq_accept_ns_sub_cfg_init(&acc);
        MOQ_TEST_CHECK(moq_session_accept_ns_sub(sv, h, &acc, 0) == MOQ_OK);

        /* Accept again → WRONG_STATE (now ESTABLISHED). */
        MOQ_TEST_CHECK(moq_session_accept_ns_sub(sv, h, &acc, 0) == MOQ_ERR_WRONG_STATE);

        moq_action_t acts[4]; size_t na;
        na = moq_session_poll_actions(sv, acts, 4);
        bool found = false;
        for (size_t i = 0; i < na; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_BIDI_STREAM) {
                found = true;
                MOQ_TEST_CHECK(acts[i].u.send_bidi_stream.stream_ref._v == ref._v);
                MOQ_TEST_CHECK(acts[i].u.send_bidi_stream.fin == false);
                MOQ_TEST_CHECK(acts[i].u.send_bidi_stream.len > 0);

                /* Decode REQUEST_OK and verify request_id. */
                moq_control_envelope_t env;
                moq_buf_reader_t r;
                moq_buf_reader_init(&r, acts[i].u.send_bidi_stream.data,
                                     acts[i].u.send_bidi_stream.len);
                MOQ_TEST_CHECK(moq_control_decode_envelope(&r, &env) >= 0);
                MOQ_TEST_CHECK(env.msg_type == MOQ_D16_REQUEST_OK);
                moq_kvp_entry_t ok_params[4];
                moq_d16_request_ok_t ok = { .params = ok_params, .params_cap = 4 };
                MOQ_TEST_CHECK(moq_d16_decode_request_ok(
                    env.payload, env.payload_len, &ok) >= 0);
                MOQ_TEST_CHECK(ok.request_id == 0);
            }
            moq_action_cleanup(&acts[i]);
        }
        MOQ_TEST_CHECK(found);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 9. reject → SEND_BIDI_STREAM with REQUEST_ERROR, fin=true ==== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

        uint8_t msg[256];
        size_t msg_len = encode_sub_ns(msg, sizeof(msg), 0, "rej", 1);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(600);
        moq_session_on_bidi_stream_bytes(sv, ref, msg, msg_len, false, 0);

        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        moq_ns_sub_handle_t h = ev.u.ns_sub_request.handle;
        moq_event_cleanup(&ev);

        moq_reject_ns_sub_cfg_t rej;
        moq_reject_ns_sub_cfg_init(&rej);
        rej.error_code = 0x10;
        rej.reason = MOQ_BYTES_LITERAL("no");
        MOQ_TEST_CHECK(moq_session_reject_ns_sub(sv, h, &rej, 0) == MOQ_OK);

        moq_action_t acts[4]; size_t na;
        na = moq_session_poll_actions(sv, acts, 4);
        bool found = false;
        for (size_t i = 0; i < na; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_BIDI_STREAM) {
                found = true;
                MOQ_TEST_CHECK(acts[i].u.send_bidi_stream.fin == true);

                /* Decode REQUEST_ERROR. */
                moq_control_envelope_t env;
                moq_buf_reader_t r;
                moq_buf_reader_init(&r, acts[i].u.send_bidi_stream.data,
                                     acts[i].u.send_bidi_stream.len);
                MOQ_TEST_CHECK(moq_control_decode_envelope(&r, &env) >= 0);
                MOQ_TEST_CHECK(env.msg_type == MOQ_D16_REQUEST_ERROR);
                moq_d16_request_error_t err;
                MOQ_TEST_CHECK(moq_d16_decode_request_error(
                    env.payload, env.payload_len, &err) >= 0);
                MOQ_TEST_CHECK(err.request_id == 0);
                MOQ_TEST_CHECK(err.error_code == 0x10);
                MOQ_TEST_CHECK(err.reason_len == 2);
            }
            moq_action_cleanup(&acts[i]);
        }
        MOQ_TEST_CHECK(found);

        /* Reject again → stale handle (entry freed). */
        MOQ_TEST_CHECK(moq_session_reject_ns_sub(sv, h, &rej, 0) == MOQ_ERR_STALE_HANDLE);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 10. accept/reject on invalid handle → STALE_HANDLE =========== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

        moq_ns_sub_handle_t invalid = MOQ_NS_SUB_HANDLE_INVALID;
        moq_accept_ns_sub_cfg_t acc;
        moq_accept_ns_sub_cfg_init(&acc);
        MOQ_TEST_CHECK(moq_session_accept_ns_sub(sv, invalid, &acc, 0) == MOQ_ERR_STALE_HANDLE);

        moq_reject_ns_sub_cfg_t rej;
        moq_reject_ns_sub_cfg_init(&rej);
        rej.error_code = 1;
        MOQ_TEST_CHECK(moq_session_reject_ns_sub(sv, invalid, &rej, 0) == MOQ_ERR_STALE_HANDLE);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 11. Control-stream rejection still intact ===================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

        uint8_t msg[256];
        size_t msg_len = encode_sub_ns(msg, sizeof(msg), 0, "ctrl", 0);
        moq_session_on_control_bytes(sv, msg, msg_len, 0);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 12. Pool full → WOULD_BLOCK ================================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t small = MOQ_SESSION_CFG_INIT;
        small.alloc = &alloc;
        small.max_namespace_subscriptions = 1;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, &small);

        uint8_t msg1[256];
        size_t len1 = encode_sub_ns(msg1, sizeof(msg1), 0, "p1", 0);
        moq_stream_ref_t ref1 = moq_stream_ref_from_u64(800);
        MOQ_TEST_CHECK(moq_session_on_bidi_stream_bytes(sv, ref1, msg1, len1, false, 0) == MOQ_OK);

        uint8_t msg2[256];
        size_t len2 = encode_sub_ns(msg2, sizeof(msg2), 2, "p2", 0);
        moq_stream_ref_t ref2 = moq_stream_ref_from_u64(801);
        MOQ_TEST_CHECK(moq_session_on_bidi_stream_bytes(sv, ref2, msg2, len2, false, 0) == MOQ_ERR_WOULD_BLOCK);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 13. Unknown stream + empty retry → no-op ===================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

        moq_stream_ref_t ref = moq_stream_ref_from_u64(999);
        MOQ_TEST_CHECK(moq_session_on_bidi_stream_bytes(sv, ref, NULL, 0, false, 0) == MOQ_OK);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 14. Extra bytes after full parse pending → close ============== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t tiny = MOQ_SESSION_CFG_INIT;
        tiny.alloc = &alloc;
        tiny.max_events = 1;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, &tiny);

        feed_subscribe(sv, 0, "ns", "fill3", NULL, 0);

        uint8_t msg[256];
        size_t msg_len = encode_sub_ns(msg, sizeof(msg), 2, "ex", 0);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1000);

        moq_session_on_bidi_stream_bytes(sv, ref, msg, msg_len, false, 0);

        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_event_cleanup(&ev);

        uint8_t extra[] = { 0xFF };
        moq_session_on_bidi_stream_bytes(sv, ref, extra, 1, false, 0);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 15. Request-ID parity violation → close ====================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

        /* Server expects even request IDs from client; send odd. */
        uint8_t msg[256];
        size_t msg_len = encode_sub_ns_raw(msg, sizeof(msg), 1, "par", 0);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1100);

        moq_session_on_bidi_stream_bytes(sv, ref, msg, msg_len, false, 0);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 16. Empty FIN on unknown stream → close ======================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

        moq_stream_ref_t ref = moq_stream_ref_from_u64(1200);
        moq_session_on_bidi_stream_bytes(sv, ref, NULL, 0, true, 0);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 17. Bidi bytes before ESTABLISHED → WRONG_STATE ============== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t cfg = MOQ_SESSION_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.perspective = MOQ_PERSPECTIVE_SERVER;
        moq_session_t *sv = NULL;
        MOQ_TEST_CHECK(moq_session_create(&cfg, 0, &sv) == MOQ_OK);

        moq_stream_ref_t ref = moq_stream_ref_from_u64(1300);
        uint8_t msg[64];
        size_t msg_len = encode_sub_ns(msg, sizeof(msg), 0, "pre", 0);
        MOQ_TEST_CHECK(moq_session_on_bidi_stream_bytes(
            sv, ref, msg, msg_len, false, 0) == MOQ_ERR_WRONG_STATE);

        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 18. recv_buf overflow → BUFFER without recv_len mutation ====== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t tiny = MOQ_SESSION_CFG_INIT;
        tiny.alloc = &alloc;
        tiny.recv_buffer_size = 8;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, &tiny);

        uint8_t msg[256];
        size_t msg_len = encode_sub_ns(msg, sizeof(msg), 0, "overflow", 0);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1400);

        /* First few bytes fit, then overflow. */
        MOQ_TEST_CHECK(moq_session_on_bidi_stream_bytes(
            sv, ref, msg, 4, false, 0) == MOQ_OK);
        /* Remaining bytes exceed recv_cap. */
        moq_result_t rc = moq_session_on_bidi_stream_bytes(
            sv, ref, msg + 4, msg_len - 4, false, 0);
        MOQ_TEST_CHECK(rc == MOQ_ERR_BUFFER);
        /* Session stays open. */
        MOQ_TEST_CHECK(moq_session_state(sv) != MOQ_SESS_CLOSED);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 19. Request-ID out of sequence → close ======================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

        /* Server expects next peer request_id=0; send 2 (skips 0). */
        uint8_t msg[256];
        size_t msg_len = encode_sub_ns_raw(msg, sizeof(msg), 2, "seq", 0);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1500);

        moq_session_on_bidi_stream_bytes(sv, ref, msg, msg_len, false, 0);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 20. Request-ID exceeds local MAX_REQUEST_ID → close ========== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 2, 2, &c, &sv, NULL, NULL);

        /* Server advertised MAX_REQUEST_ID=2; request_id=2 >= 2 → close. */
        uint8_t msg[256];
        size_t msg_len = encode_sub_ns_raw(msg, sizeof(msg), 2, "cred", 0);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1600);

        moq_session_on_bidi_stream_bytes(sv, ref, msg, msg_len, false, 0);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 21. Tiny scratch → permanent close =========================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t tiny = MOQ_SESSION_CFG_INIT;
        tiny.alloc = &alloc;
        tiny.output_scratch_size = 1;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, &tiny);

        uint8_t msg[256];
        size_t msg_len = encode_sub_ns(msg, sizeof(msg), 0, "scratch", 0);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1700);

        moq_session_on_bidi_stream_bytes(sv, ref, msg, msg_len, false, 0);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 22. FIN before complete envelope → close ===================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

        uint8_t partial[] = { 0x11, 0x03 };
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1800);
        moq_session_on_bidi_stream_bytes(sv, ref, partial, 2, true, 0);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* ================================================================= */
    /* Subscriber-side response parsing + lifecycle                     */
    /* ================================================================= */

    /* == 23. REQUEST_OK → NS_SUB_OK event ============================= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

        moq_ns_sub_handle_t h; moq_stream_ref_t ref; uint64_t rid;
        MOQ_TEST_CHECK(setup_subscriber(c, &h, &ref, &rid) == MOQ_OK);

        uint8_t resp[128];
        size_t rlen = encode_request_ok(resp, sizeof(resp), rid);
        MOQ_TEST_CHECK(moq_session_on_bidi_stream_bytes(c, ref, resp, rlen, false, 0) == MOQ_OK);

        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NS_SUB_OK);
        MOQ_TEST_CHECK(moq_ns_sub_handle_eq(ev.u.ns_sub_ok.handle, h));
        moq_event_cleanup(&ev);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 24. REQUEST_ERROR → NS_SUB_ERROR event ======================= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

        moq_ns_sub_handle_t h; moq_stream_ref_t ref; uint64_t rid;
        setup_subscriber(c, &h, &ref, &rid);

        uint8_t resp[128];
        size_t rlen = encode_request_error(resp, sizeof(resp), rid, 0x10, "no");
        MOQ_TEST_CHECK(moq_session_on_bidi_stream_bytes(c, ref, resp, rlen, true, 0) == MOQ_OK);

        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NS_SUB_ERROR);
        MOQ_TEST_CHECK(ev.u.ns_sub_error.error_code == 0x10);
        MOQ_TEST_CHECK(ev.u.ns_sub_error.reason.len == 2);
        moq_event_cleanup(&ev);

        /* Handle should be stale now. */
        moq_accept_ns_sub_cfg_t acc;
        moq_accept_ns_sub_cfg_init(&acc);
        MOQ_TEST_CHECK(moq_session_accept_ns_sub(c, h, &acc, 0) == MOQ_ERR_STALE_HANDLE);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 25. Duplicate response → close =============================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

        moq_ns_sub_handle_t h; moq_stream_ref_t ref; uint64_t rid;
        setup_subscriber(c, &h, &ref, &rid);

        uint8_t resp[128];
        size_t rlen = encode_request_ok(resp, sizeof(resp), rid);
        moq_session_on_bidi_stream_bytes(c, ref, resp, rlen, false, 0);
        moq_event_t ev;
        moq_session_poll_events(c, &ev, 1);
        moq_event_cleanup(&ev);

        /* Second REQUEST_OK → close. */
        moq_session_on_bidi_stream_bytes(c, ref, resp, rlen, false, 0);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_CLOSED);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 26. Wrong request_id in REQUEST_OK → close =================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

        moq_ns_sub_handle_t h; moq_stream_ref_t ref; uint64_t rid;
        setup_subscriber(c, &h, &ref, &rid);

        uint8_t resp[128];
        size_t rlen = encode_request_ok(resp, sizeof(resp), rid + 2);
        moq_session_on_bidi_stream_bytes(c, ref, resp, rlen, false, 0);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_CLOSED);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 27. REQUEST_ERROR + trailing bytes → close =================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

        moq_ns_sub_handle_t h; moq_stream_ref_t ref; uint64_t rid;
        setup_subscriber(c, &h, &ref, &rid);

        uint8_t resp[256];
        size_t rlen = encode_request_error(resp, sizeof(resp), rid, 1, "no");
        size_t nlen = encode_namespace_msg(resp + rlen, sizeof(resp) - rlen,
                                            MOQ_D16_NAMESPACE, "trail");
        moq_session_on_bidi_stream_bytes(c, ref, resp, rlen + nlen, false, 0);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_CLOSED);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 28. NAMESPACE → NAMESPACE_FOUND event ======================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

        moq_ns_sub_handle_t h; moq_stream_ref_t ref; uint64_t rid;
        setup_subscriber(c, &h, &ref, &rid);

        uint8_t resp[256];
        size_t rlen = encode_request_ok(resp, sizeof(resp), rid);
        size_t nlen = encode_namespace_msg(resp + rlen, sizeof(resp) - rlen,
                                            MOQ_D16_NAMESPACE, "found");
        moq_session_on_bidi_stream_bytes(c, ref, resp, rlen + nlen, false, 0);

        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NS_SUB_OK);
        moq_event_cleanup(&ev);

        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NAMESPACE_FOUND);
        MOQ_TEST_CHECK(ev.u.namespace_found.track_namespace_suffix.count == 1);
        moq_event_cleanup(&ev);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 29. NAMESPACE_DONE → NAMESPACE_GONE event ==================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

        moq_ns_sub_handle_t h; moq_stream_ref_t ref; uint64_t rid;
        setup_subscriber(c, &h, &ref, &rid);

        uint8_t ok[128];
        size_t olen = encode_request_ok(ok, sizeof(ok), rid);
        moq_session_on_bidi_stream_bytes(c, ref, ok, olen, false, 0);
        moq_event_t ev;
        moq_session_poll_events(c, &ev, 1);
        moq_event_cleanup(&ev);

        uint8_t ns_msg[128];
        size_t nlen = encode_namespace_msg(ns_msg, sizeof(ns_msg),
                                            MOQ_D16_NAMESPACE, "gone");
        moq_session_on_bidi_stream_bytes(c, ref, ns_msg, nlen, false, 0);
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NAMESPACE_FOUND);
        moq_event_cleanup(&ev);

        uint8_t ns_done[128];
        size_t dlen = encode_namespace_msg(ns_done, sizeof(ns_done),
                                            MOQ_D16_NAMESPACE_DONE, "gone");
        moq_session_on_bidi_stream_bytes(c, ref, ns_done, dlen, false, 0);

        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NAMESPACE_GONE);
        moq_event_cleanup(&ev);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 30. NAMESPACE before REQUEST_OK → close ====================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

        moq_ns_sub_handle_t h; moq_stream_ref_t ref; uint64_t rid;
        setup_subscriber(c, &h, &ref, &rid);

        uint8_t ns[128];
        size_t nlen = encode_namespace_msg(ns, sizeof(ns),
                                            MOQ_D16_NAMESPACE, "early");
        moq_session_on_bidi_stream_bytes(c, ref, ns, nlen, false, 0);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_CLOSED);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 31. Empty suffix valid ======================================= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

        moq_ns_sub_handle_t h; moq_stream_ref_t ref; uint64_t rid;
        setup_subscriber(c, &h, &ref, &rid);

        uint8_t ok[128];
        size_t olen = encode_request_ok(ok, sizeof(ok), rid);
        moq_session_on_bidi_stream_bytes(c, ref, ok, olen, false, 0);
        moq_event_t ev;
        moq_session_poll_events(c, &ev, 1);
        moq_event_cleanup(&ev);

        uint8_t ns[128];
        size_t nlen = encode_namespace_msg(ns, sizeof(ns),
                                            MOQ_D16_NAMESPACE, NULL);
        moq_session_on_bidi_stream_bytes(c, ref, ns, nlen, false, 0);
        MOQ_TEST_CHECK(moq_session_state(c) != MOQ_SESS_CLOSED);

        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NAMESPACE_FOUND);
        MOQ_TEST_CHECK(ev.u.namespace_found.track_namespace_suffix.count == 0);
        moq_event_cleanup(&ev);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 32. Cancel PENDING_SUBSCRIBER → CLOSE_BIDI_STREAM ============ */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

        moq_ns_sub_handle_t h; moq_stream_ref_t ref; uint64_t rid;
        setup_subscriber(c, &h, &ref, &rid);
        (void)rid;

        MOQ_TEST_CHECK(moq_session_cancel_namespace_sub(c, h, 0) == MOQ_OK);

        moq_action_t acts[4]; size_t na;
        na = moq_session_poll_actions(c, acts, 4);
        bool found = false;
        for (size_t i = 0; i < na; i++) {
            if (acts[i].kind == MOQ_ACTION_CLOSE_BIDI_STREAM) {
                found = true;
                MOQ_TEST_CHECK(acts[i].u.close_bidi_stream.stream_ref._v == ref._v);
            }
            moq_action_cleanup(&acts[i]);
        }
        MOQ_TEST_CHECK(found);

        /* Cancel again → WRONG_STATE (now CLOSING). */
        MOQ_TEST_CHECK(moq_session_cancel_namespace_sub(c, h, 0) == MOQ_ERR_WRONG_STATE);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 33. Cancel ESTABLISHED → CLOSE_BIDI_STREAM =================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

        moq_ns_sub_handle_t h; moq_stream_ref_t ref; uint64_t rid;
        setup_subscriber(c, &h, &ref, &rid);

        uint8_t ok[128];
        size_t olen = encode_request_ok(ok, sizeof(ok), rid);
        moq_session_on_bidi_stream_bytes(c, ref, ok, olen, false, 0);
        moq_event_t ev;
        moq_session_poll_events(c, &ev, 1);
        moq_event_cleanup(&ev);

        MOQ_TEST_CHECK(moq_session_cancel_namespace_sub(c, h, 0) == MOQ_OK);

        moq_action_t acts[4]; size_t na;
        na = moq_session_poll_actions(c, acts, 4);
        bool found = false;
        for (size_t i = 0; i < na; i++) {
            if (acts[i].kind == MOQ_ACTION_CLOSE_BIDI_STREAM) found = true;
            moq_action_cleanup(&acts[i]);
        }
        MOQ_TEST_CHECK(found);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 34. Late bytes on CLOSING → silently consumed ================ */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

        moq_ns_sub_handle_t h; moq_stream_ref_t ref; uint64_t rid;
        setup_subscriber(c, &h, &ref, &rid);
        moq_session_cancel_namespace_sub(c, h, 0);
        moq_action_t acts[4]; size_t na;
        while ((na = moq_session_poll_actions(c, acts, 4)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);

        uint8_t resp[128];
        size_t rlen = encode_request_ok(resp, sizeof(resp), rid);
        MOQ_TEST_CHECK(moq_session_on_bidi_stream_bytes(c, ref, resp, rlen, false, 0) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(c) != MOQ_SESS_CLOSED);

        /* No event emitted. */
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 0);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 35. Late FIN on CLOSING → frees entry ======================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

        moq_ns_sub_handle_t h; moq_stream_ref_t ref; uint64_t rid;
        setup_subscriber(c, &h, &ref, &rid);
        moq_session_cancel_namespace_sub(c, h, 0);
        moq_action_t acts[4]; size_t na;
        while ((na = moq_session_poll_actions(c, acts, 4)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);

        MOQ_TEST_CHECK(moq_session_on_bidi_stream_bytes(c, ref, NULL, 0, true, 0) == MOQ_OK);

        /* Handle should be stale now. */
        MOQ_TEST_CHECK(moq_session_cancel_namespace_sub(c, h, 0) == MOQ_ERR_STALE_HANDLE);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 36. Reset → entry freed ====================================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

        moq_ns_sub_handle_t h; moq_stream_ref_t ref; uint64_t rid;
        setup_subscriber(c, &h, &ref, &rid);
        (void)rid;

        MOQ_TEST_CHECK(moq_session_on_bidi_stream_reset(c, ref, 0x1, 0) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_cancel_namespace_sub(c, h, 0) == MOQ_ERR_STALE_HANDLE);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 37. Cancel then reset → clean cleanup ======================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

        moq_ns_sub_handle_t h; moq_stream_ref_t ref; uint64_t rid;
        setup_subscriber(c, &h, &ref, &rid);
        (void)rid;

        moq_session_cancel_namespace_sub(c, h, 0);
        moq_action_t acts[4]; size_t na;
        while ((na = moq_session_poll_actions(c, acts, 4)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);

        MOQ_TEST_CHECK(moq_session_on_bidi_stream_reset(c, ref, 0x1, 0) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_cancel_namespace_sub(c, h, 0) == MOQ_ERR_STALE_HANDLE);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 38. Split-byte subscriber REQUEST_OK ========================= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

        moq_ns_sub_handle_t h; moq_stream_ref_t ref; uint64_t rid;
        setup_subscriber(c, &h, &ref, &rid);

        uint8_t resp[128];
        size_t rlen = encode_request_ok(resp, sizeof(resp), rid);
        for (size_t i = 0; i < rlen; i++) {
            moq_result_t rc = moq_session_on_bidi_stream_bytes(
                c, ref, resp + i, 1, false, 0);
            MOQ_TEST_CHECK(rc == MOQ_OK);
        }

        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NS_SUB_OK);
        moq_event_cleanup(&ev);
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 0);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 39. Subscriber event WB retry ================================ */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t tiny = MOQ_SESSION_CFG_INIT;
        tiny.alloc = &alloc;
        tiny.max_events = 1;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, &tiny, NULL);

        moq_ns_sub_handle_t h; moq_stream_ref_t ref; uint64_t rid;
        setup_subscriber(c, &h, &ref, &rid);

        /* Fill client event queue with a subscribe from server side. */
        feed_subscribe(c, 1, "ns", "fill", NULL, 0);

        uint8_t resp[128];
        size_t rlen = encode_request_ok(resp, sizeof(resp), rid);
        moq_result_t rc = moq_session_on_bidi_stream_bytes(
            c, ref, resp, rlen, false, 0);
        MOQ_TEST_CHECK(rc == MOQ_ERR_WOULD_BLOCK);

        /* Drain filler. */
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        moq_event_cleanup(&ev);

        /* Retry. */
        rc = moq_session_on_bidi_stream_bytes(c, ref, NULL, 0, false, 0);
        MOQ_TEST_CHECK(rc == MOQ_OK);

        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NS_SUB_OK);
        moq_event_cleanup(&ev);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 40. Cancel WB → no state mutation ============================= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t tiny = MOQ_SESSION_CFG_INIT;
        tiny.alloc = &alloc;
        tiny.max_actions = 2;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, &tiny, NULL);

        moq_ns_sub_handle_t h; moq_stream_ref_t ref; uint64_t rid;
        setup_subscriber(c, &h, &ref, &rid);
        (void)rid;

        /* Fill both action slots. */
        moq_session_update_max_request_id(c, 128, 0);
        moq_session_update_max_request_id(c, 256, 0);

        MOQ_TEST_CHECK(moq_session_cancel_namespace_sub(c, h, 0) == MOQ_ERR_WOULD_BLOCK);

        /* Handle still valid — state unchanged. */
        MOQ_TEST_CHECK(moq_ns_sub_handle_is_valid(h));

        /* Drain, retry. */
        moq_action_t acts[4]; size_t na;
        while ((na = moq_session_poll_actions(c, acts, 4)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);

        MOQ_TEST_CHECK(moq_session_cancel_namespace_sub(c, h, 0) == MOQ_OK);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 41. Suffix trailing bytes → close ============================ */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

        moq_ns_sub_handle_t h; moq_stream_ref_t ref; uint64_t rid;
        setup_subscriber(c, &h, &ref, &rid);

        uint8_t ok[128];
        size_t olen = encode_request_ok(ok, sizeof(ok), rid);
        moq_session_on_bidi_stream_bytes(c, ref, ok, olen, false, 0);
        moq_event_t ev;
        moq_session_poll_events(c, &ev, 1);
        moq_event_cleanup(&ev);

        /* Build NAMESPACE with trailing byte in payload. */
        uint8_t payload[64];
        moq_buf_writer_t pw;
        moq_buf_writer_init(&pw, payload, sizeof(payload));
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("ok") };
        moq_namespace_t ns = { parts, 1 };
        moq_buf_write_namespace_prefix(&pw, &ns);
        /* Extra trailing byte. */
        moq_buf_write_varint(&pw, 0xFF);
        size_t plen = moq_buf_writer_offset(&pw);

        uint8_t msg[128];
        moq_buf_writer_t mw;
        moq_buf_writer_init(&mw, msg, sizeof(msg));
        moq_control_encode_envelope(&mw, MOQ_D16_NAMESPACE,
                                     payload, (uint16_t)plen);
        size_t mlen = moq_buf_writer_offset(&mw);

        moq_session_on_bidi_stream_bytes(c, ref, msg, mlen, false, 0);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_CLOSED);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 42. REQUEST_ERROR split FIN → no protocol close ============== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

        moq_ns_sub_handle_t h; moq_stream_ref_t ref; uint64_t rid;
        setup_subscriber(c, &h, &ref, &rid);

        uint8_t resp[128];
        size_t rlen = encode_request_error(resp, sizeof(resp), rid, 1, "no");
        /* Send ERROR without FIN. */
        MOQ_TEST_CHECK(moq_session_on_bidi_stream_bytes(
            c, ref, resp, rlen, false, 0) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(c) != MOQ_SESS_CLOSED);

        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NS_SUB_ERROR);
        moq_event_cleanup(&ev);

        /* Send empty FIN separately — should not close session. */
        MOQ_TEST_CHECK(moq_session_on_bidi_stream_bytes(
            c, ref, NULL, 0, true, 0) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(c) != MOQ_SESS_CLOSED);

        /* Handle should now be stale (entry freed by FIN). */
        MOQ_TEST_CHECK(moq_session_cancel_namespace_sub(c, h, 0) == MOQ_ERR_STALE_HANDLE);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 43. REQUEST_ERROR fin=true + WB → retry frees entry ========== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t tiny = MOQ_SESSION_CFG_INIT;
        tiny.alloc = &alloc;
        tiny.max_events = 1;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, &tiny, NULL);

        moq_ns_sub_handle_t h; moq_stream_ref_t ref; uint64_t rid;
        setup_subscriber(c, &h, &ref, &rid);

        /* Fill event queue. */
        feed_subscribe(c, 1, "ns", "fill", NULL, 0);

        uint8_t resp[128];
        size_t rlen = encode_request_error(resp, sizeof(resp), rid, 1, "wb");
        moq_result_t rc = moq_session_on_bidi_stream_bytes(
            c, ref, resp, rlen, true, 0);
        MOQ_TEST_CHECK(rc == MOQ_ERR_WOULD_BLOCK);

        /* Drain filler. */
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        moq_event_cleanup(&ev);

        /* Retry → event emitted, entry freed (FIN was persisted). */
        rc = moq_session_on_bidi_stream_bytes(c, ref, NULL, 0, false, 0);
        MOQ_TEST_CHECK(rc == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(c) != MOQ_SESS_CLOSED);

        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NS_SUB_ERROR);
        moq_event_cleanup(&ev);

        /* Handle should be stale — entry freed by persisted FIN. */
        MOQ_TEST_CHECK(moq_session_cancel_namespace_sub(c, h, 0) == MOQ_ERR_STALE_HANDLE);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 44. Remote-error CLOSING rejects extra bytes ================= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

        moq_ns_sub_handle_t h; moq_stream_ref_t ref; uint64_t rid;
        setup_subscriber(c, &h, &ref, &rid);

        uint8_t resp[128];
        size_t rlen = encode_request_error(resp, sizeof(resp), rid, 1, "rej");
        /* ERROR without FIN → CLOSING with got_response=true. */
        MOQ_TEST_CHECK(moq_session_on_bidi_stream_bytes(
            c, ref, resp, rlen, false, 0) == MOQ_OK);

        moq_event_t ev;
        moq_session_poll_events(c, &ev, 1);
        moq_event_cleanup(&ev);

        /* Extra NAMESPACE bytes on remote-error CLOSING → close. */
        uint8_t ns[128];
        size_t nlen = encode_namespace_msg(ns, sizeof(ns),
                                            MOQ_D16_NAMESPACE, "bad");
        moq_session_on_bidi_stream_bytes(c, ref, ns, nlen, false, 0);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_CLOSED);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 45. Local cancel → late NAMESPACE bytes → no close ============ */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

        moq_ns_sub_handle_t h; moq_stream_ref_t ref; uint64_t rid;
        setup_subscriber(c, &h, &ref, &rid);

        uint8_t ok[128];
        size_t olen = encode_request_ok(ok, sizeof(ok), rid);
        moq_session_on_bidi_stream_bytes(c, ref, ok, olen, false, 0);
        moq_event_t ev;
        moq_session_poll_events(c, &ev, 1);
        moq_event_cleanup(&ev);

        moq_session_cancel_namespace_sub(c, h, 0);
        moq_action_t acts[4]; size_t na;
        while ((na = moq_session_poll_actions(c, acts, 4)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);

        /* Late NAMESPACE bytes on local-cancel CLOSING → silently consumed. */
        uint8_t ns[128];
        size_t nlen = encode_namespace_msg(ns, sizeof(ns),
                                            MOQ_D16_NAMESPACE, "late");
        MOQ_TEST_CHECK(moq_session_on_bidi_stream_bytes(
            c, ref, ns, nlen, false, 0) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(c) != MOQ_SESS_CLOSED);
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 0);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 46. Remote ERROR → handle stale before FIN =================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

        moq_ns_sub_handle_t h; moq_stream_ref_t ref; uint64_t rid;
        setup_subscriber(c, &h, &ref, &rid);

        uint8_t resp[128];
        size_t rlen = encode_request_error(resp, sizeof(resp), rid, 1, "rej");
        moq_session_on_bidi_stream_bytes(c, ref, resp, rlen, false, 0);

        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NS_SUB_ERROR);
        moq_event_cleanup(&ev);

        /* Handle is stale immediately (generation bumped). */
        MOQ_TEST_CHECK(moq_session_cancel_namespace_sub(c, h, 0) == MOQ_ERR_STALE_HANDLE);

        /* Empty FIN absorbed cleanly — no close. */
        MOQ_TEST_CHECK(moq_session_on_bidi_stream_bytes(
            c, ref, NULL, 0, true, 0) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(c) != MOQ_SESS_CLOSED);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* ================================================================= */
    /* Publisher send_namespace / send_namespace_done API               */
    /* ================================================================= */

    /* == 47. send_namespace after accept → NAMESPACE_FOUND ============ */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

        /* Publisher side: receive subscribe, accept. */
        uint8_t msg[256];
        size_t msg_len = encode_sub_ns(msg, sizeof(msg), 0, "snd", 0);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(4700);
        MOQ_TEST_CHECK(moq_session_on_bidi_stream_bytes(sv, ref, msg, msg_len, false, 0) == MOQ_OK);
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NS_SUB_REQUEST);
        moq_ns_sub_handle_t ph = ev.u.ns_sub_request.handle;
        moq_event_cleanup(&ev);

        moq_accept_ns_sub_cfg_t acc;
        moq_accept_ns_sub_cfg_init(&acc);
        MOQ_TEST_CHECK(moq_session_accept_ns_sub(sv, ph, &acc, 0) == MOQ_OK);

        /* Send NAMESPACE. */
        moq_bytes_t suffix_parts[] = { MOQ_BYTES_LITERAL("track1") };
        moq_namespace_t suffix = { suffix_parts, 1 };
        MOQ_TEST_CHECK(moq_session_send_namespace(sv, ph, &suffix, 0) == MOQ_OK);

        /* Verify SEND_BIDI_STREAM action with NAMESPACE envelope. */
        moq_action_t acts[8]; size_t na;
        na = moq_session_poll_actions(sv, acts, 8);
        bool found_ns = false;
        for (size_t i = 0; i < na; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_BIDI_STREAM &&
                acts[i].u.send_bidi_stream.fin == false) {
                moq_control_envelope_t env;
                moq_buf_reader_t r;
                moq_buf_reader_init(&r, acts[i].u.send_bidi_stream.data,
                                     acts[i].u.send_bidi_stream.len);
                if (moq_control_decode_envelope(&r, &env) >= 0 &&
                    env.msg_type == MOQ_D16_NAMESPACE)
                    found_ns = true;
            }
            moq_action_cleanup(&acts[i]);
        }
        MOQ_TEST_CHECK(found_ns);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 48. send_namespace_done → NAMESPACE_GONE ===================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

        uint8_t msg[256];
        size_t msg_len = encode_sub_ns(msg, sizeof(msg), 0, "snd2", 0);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(4800);
        MOQ_TEST_CHECK(moq_session_on_bidi_stream_bytes(sv, ref, msg, msg_len, false, 0) == MOQ_OK);
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NS_SUB_REQUEST);
        moq_ns_sub_handle_t ph = ev.u.ns_sub_request.handle;
        moq_event_cleanup(&ev);

        moq_accept_ns_sub_cfg_t acc;
        moq_accept_ns_sub_cfg_init(&acc);
        MOQ_TEST_CHECK(moq_session_accept_ns_sub(sv, ph, &acc, 0) == MOQ_OK);

        moq_bytes_t suffix_parts[] = { MOQ_BYTES_LITERAL("done1") };
        moq_namespace_t suffix = { suffix_parts, 1 };
        MOQ_TEST_CHECK(moq_session_send_namespace(sv, ph, &suffix, 0) == MOQ_OK);
        { moq_action_t da[8]; size_t dn;
          while ((dn = moq_session_poll_actions(sv, da, 8)) > 0)
              for (size_t j = 0; j < dn; j++) moq_action_cleanup(&da[j]); }
        MOQ_TEST_CHECK(moq_session_send_namespace_done(sv, ph, &suffix, 0) == MOQ_OK);

        moq_action_t acts[8]; size_t na;
        na = moq_session_poll_actions(sv, acts, 8);
        bool found_done = false;
        for (size_t i = 0; i < na; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_BIDI_STREAM) {
                moq_control_envelope_t env;
                moq_buf_reader_t r;
                moq_buf_reader_init(&r, acts[i].u.send_bidi_stream.data,
                                     acts[i].u.send_bidi_stream.len);
                if (moq_control_decode_envelope(&r, &env) >= 0 &&
                    env.msg_type == MOQ_D16_NAMESPACE_DONE)
                    found_done = true;
            }
            moq_action_cleanup(&acts[i]);
        }
        MOQ_TEST_CHECK(found_done);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 49. send_namespace before accept → WRONG_STATE =============== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

        uint8_t msg[256];
        size_t msg_len = encode_sub_ns(msg, sizeof(msg), 0, "pre", 0);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(4900);
        MOQ_TEST_CHECK(moq_session_on_bidi_stream_bytes(sv, ref, msg, msg_len, false, 0) == MOQ_OK);
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NS_SUB_REQUEST);
        moq_ns_sub_handle_t ph = ev.u.ns_sub_request.handle;
        moq_event_cleanup(&ev);

        moq_namespace_t suffix = { NULL, 0 };
        MOQ_TEST_CHECK(moq_session_send_namespace(sv, ph, &suffix, 0) == MOQ_ERR_WRONG_STATE);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 50. send_namespace on subscriber handle → WRONG_STATE ======== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

        moq_ns_sub_handle_t h; moq_stream_ref_t ref; uint64_t rid;
        setup_subscriber(c, &h, &ref, &rid);

        moq_namespace_t suffix = { NULL, 0 };
        MOQ_TEST_CHECK(moq_session_send_namespace(c, h, &suffix, 0) == MOQ_ERR_WRONG_STATE);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 51. send_namespace after reject → STALE_HANDLE =============== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

        uint8_t msg[256];
        size_t msg_len = encode_sub_ns(msg, sizeof(msg), 0, "rej", 0);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(5100);
        MOQ_TEST_CHECK(moq_session_on_bidi_stream_bytes(sv, ref, msg, msg_len, false, 0) == MOQ_OK);
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NS_SUB_REQUEST);
        moq_ns_sub_handle_t ph = ev.u.ns_sub_request.handle;
        moq_event_cleanup(&ev);

        moq_reject_ns_sub_cfg_t rej;
        moq_reject_ns_sub_cfg_init(&rej);
        rej.error_code = 1;
        MOQ_TEST_CHECK(moq_session_reject_ns_sub(sv, ph, &rej, 0) == MOQ_OK);

        moq_namespace_t suffix = { NULL, 0 };
        MOQ_TEST_CHECK(moq_session_send_namespace(sv, ph, &suffix, 0) == MOQ_ERR_STALE_HANDLE);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 52. send_namespace WB → no mutation, retry OK ================ */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t tiny = MOQ_SESSION_CFG_INIT;
        tiny.alloc = &alloc;
        tiny.max_actions = 2;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, &tiny);

        uint8_t msg[256];
        size_t msg_len = encode_sub_ns(msg, sizeof(msg), 0, "wb", 0);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(5200);
        MOQ_TEST_CHECK(moq_session_on_bidi_stream_bytes(sv, ref, msg, msg_len, false, 0) == MOQ_OK);
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NS_SUB_REQUEST);
        moq_ns_sub_handle_t ph = ev.u.ns_sub_request.handle;
        moq_event_cleanup(&ev);

        moq_accept_ns_sub_cfg_t acc;
        moq_accept_ns_sub_cfg_init(&acc);
        MOQ_TEST_CHECK(moq_session_accept_ns_sub(sv, ph, &acc, 0) == MOQ_OK);

        /* Fill action queue. */
        moq_session_update_max_request_id(sv, 128, 0);

        moq_namespace_t suffix = { NULL, 0 };
        MOQ_TEST_CHECK(moq_session_send_namespace(sv, ph, &suffix, 0) == MOQ_ERR_WOULD_BLOCK);

        /* Drain, retry. */
        moq_action_t acts[4]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 4)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);

        MOQ_TEST_CHECK(moq_session_send_namespace(sv, ph, &suffix, 0) == MOQ_OK);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 53. send_namespace with small send_buf → BUFFER =============== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t tiny = MOQ_SESSION_CFG_INIT;
        tiny.alloc = &alloc;
        tiny.send_buffer_size = 64;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, &tiny);

        uint8_t msg[256];
        size_t msg_len = encode_sub_ns(msg, sizeof(msg), 0, "buf", 0);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(5300);
        MOQ_TEST_CHECK(moq_session_on_bidi_stream_bytes(sv, ref, msg, msg_len, false, 0) == MOQ_OK);
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NS_SUB_REQUEST);
        moq_ns_sub_handle_t ph = ev.u.ns_sub_request.handle;
        moq_event_cleanup(&ev);

        moq_accept_ns_sub_cfg_t acc;
        moq_accept_ns_sub_cfg_init(&acc);
        MOQ_TEST_CHECK(moq_session_accept_ns_sub(sv, ph, &acc, 0) == MOQ_OK);

        /* Drain actions to free send_buf, but it's still tiny. */
        moq_action_t acts[8]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 8)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);

        /* Suffix that exceeds the tiny send_buf → BUFFER. */
        moq_bytes_t big_parts[] = {
            MOQ_BYTES_LITERAL("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            MOQ_BYTES_LITERAL("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
        };
        moq_namespace_t big_suffix = { big_parts, 2 };
        MOQ_TEST_CHECK(moq_session_send_namespace(sv, ph, &big_suffix, 0) == MOQ_ERR_BUFFER);

        /* Small suffix succeeds. */
        moq_namespace_t small_suffix = { NULL, 0 };
        MOQ_TEST_CHECK(moq_session_send_namespace(sv, ph, &small_suffix, 0) == MOQ_OK);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 54. send_namespace with near-limit suffix (no stack cap) ====== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t big = MOQ_SESSION_CFG_INIT;
        big.alloc = &alloc;
        big.send_buffer_size = 8192;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, &big);

        uint8_t msg[256];
        size_t msg_len = encode_sub_ns(msg, sizeof(msg), 0, "big", 0);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(5400);
        MOQ_TEST_CHECK(moq_session_on_bidi_stream_bytes(sv, ref, msg, msg_len, false, 0) == MOQ_OK);
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NS_SUB_REQUEST);
        moq_ns_sub_handle_t ph = ev.u.ns_sub_request.handle;
        moq_event_cleanup(&ev);

        moq_accept_ns_sub_cfg_t acc;
        moq_accept_ns_sub_cfg_init(&acc);
        MOQ_TEST_CHECK(moq_session_accept_ns_sub(sv, ph, &acc, 0) == MOQ_OK);

        moq_action_t acts[8]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 8)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);

        /* Build a suffix whose encoded payload exceeds 4096 bytes.
         * 16 fields × 255 bytes = 4080 field bytes (under the 4096
         * field-data cap), plus 16 × 2-byte length varints + 1-byte
         * count = 4113 encoded payload bytes. This would not fit in
         * the old uint8_t payload[4096] stack buffer. */
        uint8_t field_data[255];
        memset(field_data, 'x', sizeof(field_data));
        moq_bytes_t big_parts[16];
        for (int i = 0; i < 16; i++) {
            big_parts[i].data = field_data;
            big_parts[i].len = sizeof(field_data);
        }
        moq_namespace_t big_suffix = { big_parts, 16 };
        MOQ_TEST_CHECK(moq_session_send_namespace(sv, ph, &big_suffix, 0) == MOQ_OK);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 55. All three namespace interest values map correctly ========= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        static const moq_namespace_interest_t interests[] = {
            MOQ_NAMESPACE_INTEREST_PUBLISHER_STATE,
            MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE,
            MOQ_NAMESPACE_INTEREST_BOTH,
        };
        for (int vi = 0; vi < 3; vi++) {
            moq_session_t *c = NULL, *sv = NULL;
            establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

            moq_subscribe_namespace_cfg_t cfg;
            moq_subscribe_namespace_cfg_init(&cfg);
            moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("int") };
            cfg.track_namespace_prefix.parts = parts;
            cfg.track_namespace_prefix.count = 1;
            cfg.namespace_interest = interests[vi];

            moq_ns_sub_handle_t h;
            MOQ_TEST_CHECK(moq_session_subscribe_namespace(c, &cfg, 0, &h) == MOQ_OK);

            /* Verify the wire-encoded subscribe_options matches. */
            moq_action_t acts[4]; size_t na;
            na = moq_session_poll_actions(c, acts, 4);
            for (size_t ai = 0; ai < na; ai++) {
                if (acts[ai].kind == MOQ_ACTION_OPEN_BIDI_STREAM) {
                    moq_control_envelope_t env;
                    moq_buf_reader_t r;
                    moq_buf_reader_init(&r, acts[ai].u.open_bidi_stream.data,
                                         acts[ai].u.open_bidi_stream.len);
                    moq_control_decode_envelope(&r, &env);
                    moq_bytes_t dp[32]; moq_kvp_entry_t dparams[4];
                    moq_d16_subscribe_namespace_t sub = {
                        .track_namespace_prefix = { dp, 0 },
                        .params = dparams, .params_cap = 4,
                    };
                    moq_d16_decode_subscribe_namespace(
                        env.payload, env.payload_len, dp, 32, &sub);
                    MOQ_TEST_CHECK(sub.subscribe_options == interests[vi]);
                }
                moq_action_cleanup(&acts[ai]);
            }

            DRAIN_BOTH(c, sv);
            moq_session_destroy(c);
            moq_session_destroy(sv);
            MOQ_TEST_CHECK(as.balance == 0);
        }
    }

    /* == 56. Invalid namespace interest → INVAL, no action ============ */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);
        DRAIN_BOTH(c, sv);

        moq_subscribe_namespace_cfg_t cfg;
        moq_subscribe_namespace_cfg_init(&cfg);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("bad") };
        cfg.track_namespace_prefix.parts = parts;
        cfg.track_namespace_prefix.count = 1;
        cfg.namespace_interest = 3;

        moq_ns_sub_handle_t h = MOQ_NS_SUB_HANDLE_INVALID;
        MOQ_TEST_CHECK(moq_session_subscribe_namespace(c, &cfg, 0, &h) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(!moq_ns_sub_handle_is_valid(h));

        moq_action_t acts[4];
        MOQ_TEST_CHECK(moq_session_poll_actions(c, acts, 4) == 0);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* ============================================================== */
    /* Batch 6: SUBSCRIBE_NAMESPACE params/auth/forward              */
    /* ============================================================== */

    /* == SUBSCRIBE_NAMESPACE with FORWARD=0 emits forward=false ====== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        /* Encode SUBSCRIBE_NAMESPACE with FORWARD=0. */
        uint8_t buf[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_bytes_t parts[] = { moq_bytes_cstr("live") };
        moq_namespace_t prefix = { parts, 1 };
        uint8_t fwd_v[8];
        size_t fwd_len = moq_quic_varint_encode(0, fwd_v, sizeof(fwd_v));
        moq_kvp_entry_t params[1] = {{
            .type = MOQ_MSG_PARAM_FORWARD,
            .value = fwd_v, .value_len = fwd_len,
            .is_varint = true,
        }};
        moq_d16_encode_subscribe_namespace(&w, 0, &prefix, 2, params, 1);
        size_t wire_len = moq_buf_writer_offset(&w);

        moq_stream_ref_t bidi_ref = moq_stream_ref_from_u64(100);
        MOQ_TEST_CHECK(moq_session_on_bidi_stream_bytes(sv, bidi_ref,
            buf, wire_len, true, 0) == MOQ_OK);

        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NS_SUB_REQUEST);
        MOQ_TEST_CHECK(!ev.u.ns_sub_request.forward);
        moq_event_cleanup(&ev);

        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == SUBSCRIBE_NAMESPACE without FORWARD emits forward=true ====== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        uint8_t buf[128];
        size_t wire_len = encode_sub_ns(buf, sizeof(buf), 0, "live", 2);

        moq_stream_ref_t bidi_ref = moq_stream_ref_from_u64(100);
        MOQ_TEST_CHECK(moq_session_on_bidi_stream_bytes(sv, bidi_ref,
            buf, wire_len, true, 0) == MOQ_OK);

        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NS_SUB_REQUEST);
        MOQ_TEST_CHECK(ev.u.ns_sub_request.forward);
        moq_event_cleanup(&ev);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == SUBSCRIBE_NAMESPACE with unknown param closes =============== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        uint8_t buf[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_bytes_t parts[] = { moq_bytes_cstr("live") };
        moq_namespace_t prefix = { parts, 1 };
        uint8_t dummy_v[8];
        size_t dummy_len = moq_quic_varint_encode(1, dummy_v, sizeof(dummy_v));
        moq_kvp_entry_t params[1] = {{
            .type = 0xFE,
            .value = dummy_v, .value_len = dummy_len,
            .is_varint = true,
        }};
        moq_d16_encode_subscribe_namespace(&w, 0, &prefix, 2, params, 1);
        size_t wire_len = moq_buf_writer_offset(&w);

        moq_stream_ref_t bidi_ref = moq_stream_ref_from_u64(100);
        moq_session_on_bidi_stream_bytes(sv, bidi_ref, buf, wire_len, true, 0);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == SUBSCRIBE_NAMESPACE with invalid FORWARD=99 closes =========== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        uint8_t buf[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_bytes_t parts[] = { moq_bytes_cstr("live") };
        moq_namespace_t prefix = { parts, 1 };
        uint8_t fwd_v[8];
        size_t fwd_len = moq_quic_varint_encode(99, fwd_v, sizeof(fwd_v));
        moq_kvp_entry_t params[1] = {{
            .type = MOQ_MSG_PARAM_FORWARD,
            .value = fwd_v, .value_len = fwd_len,
            .is_varint = true,
        }};
        moq_d16_encode_subscribe_namespace(&w, 0, &prefix, 2, params, 1);
        size_t wire_len = moq_buf_writer_offset(&w);

        moq_stream_ref_t bidi_ref = moq_stream_ref_from_u64(100);
        moq_session_on_bidi_stream_bytes(sv, bidi_ref, buf, wire_len, true, 0);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == SUBSCRIBE_NAMESPACE with duplicate FORWARD closes ============ */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        uint8_t buf[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_bytes_t parts[] = { moq_bytes_cstr("live") };
        moq_namespace_t prefix = { parts, 1 };
        uint8_t fwd_v[8];
        size_t fwd_len = moq_quic_varint_encode(1, fwd_v, sizeof(fwd_v));
        moq_kvp_entry_t params[2] = {
            { .type = MOQ_MSG_PARAM_FORWARD, .value = fwd_v, .value_len = fwd_len,
              .is_varint = true },
            { .type = MOQ_MSG_PARAM_FORWARD, .value = fwd_v, .value_len = fwd_len,
              .is_varint = true },
        };
        moq_d16_encode_subscribe_namespace(&w, 0, &prefix, 2, params, 2);
        size_t wire_len = moq_buf_writer_offset(&w);

        moq_stream_ref_t bidi_ref = moq_stream_ref_from_u64(100);
        moq_session_on_bidi_stream_bytes(sv, bidi_ref, buf, wire_len, true, 0);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Auth REGISTER + event WB: no cache mutation, retry once ===== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t s_extra = MOQ_SESSION_CFG_INIT;
        s_extra.max_events = 1;
        s_extra.send_auth_token_cache_size = true;
        s_extra.auth_token_cache_size = 1024;

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &s_extra);

        /* Fill event queue: client publishes a namespace. */
        {
            moq_bytes_t fill_parts[] = { moq_bytes_cstr("fill") };
            moq_namespace_t fill_ns = { fill_parts, 1 };
            moq_publish_namespace_cfg_t ncfg;
            moq_publish_namespace_cfg_init(&ncfg);
            ncfg.track_namespace = fill_ns;
            moq_announcement_t ann;
            moq_session_publish_namespace(c, &ncfg, 0, &ann);
            pump_actions_to_peer(c, sv, 0);
        }
        /* Server event queue: NAMESPACE_PUBLISHED for "fill" → full (1/1). */

        /* Build SUBSCRIBE_NAMESPACE with AUTH_TOKEN REGISTER. */
        moq_d16_auth_token_t reg_tok = {
            .alias_type = MOQ_AUTH_TOKEN_REGISTER,
            .alias = 1,
            .token_type = 1,
            .token_value = (const uint8_t *)"secret",
            .token_value_len = 6,
        };
        uint8_t tb[64];
        moq_buf_writer_t tw;
        moq_buf_writer_init(&tw, tb, sizeof(tb));
        moq_d16_auth_token_encode(&tw, &reg_tok);

        moq_kvp_entry_t params[1] = {{
            .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
            .value = tb, .value_len = moq_buf_writer_offset(&tw),
            .is_varint = false, .raw = NULL, .raw_len = 0,
        }};

        uint8_t buf[256];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_bytes_t parts[] = { moq_bytes_cstr("auth") };
        moq_namespace_t prefix = { parts, 1 };
        moq_d16_encode_subscribe_namespace(&w, 2, &prefix, 2, params, 1);
        size_t wire_len = moq_buf_writer_offset(&w);

        /* Feed via bidi stream — event queue full → WOULD_BLOCK. */
        moq_stream_ref_t bidi_ref = moq_stream_ref_from_u64(200);
        moq_result_t brc = moq_session_on_bidi_stream_bytes(sv, bidi_ref,
            buf, wire_len, true, 1000);
        MOQ_TEST_CHECK(brc == MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        /* Auth alias must NOT be in cache yet (commit-last). */
        MOQ_TEST_CHECK(moq_token_cache_lookup(&sv->peer_token_cache, 1,
            NULL, NULL, NULL) == MOQ_TOKEN_ERR_UNKNOWN);

        /* Drain events, retry. */
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_event_cleanup(&ev);
        moq_session_on_bidi_stream_bytes(sv, bidi_ref, NULL, 0, false, 1000);

        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NS_SUB_REQUEST);
        MOQ_TEST_CHECK(ev.u.ns_sub_request.token_count == 1);
        moq_event_cleanup(&ev);

        /* Auth alias should be committed exactly once now. */
        MOQ_TEST_CHECK(moq_token_cache_lookup(&sv->peer_token_cache, 1,
            NULL, NULL, NULL) == MOQ_TOKEN_OK);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Auth reject + action WB: retry sends error without reprocess = */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t s_extra = MOQ_SESSION_CFG_INIT;
        s_extra.max_actions = 1;
        s_extra.send_auth_token_cache_size = true;
        s_extra.auth_token_cache_size = 1024;

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &s_extra);

        /* First: register alias=1 via a PUBLISH_NAMESPACE so it's in cache. */
        {
            moq_d16_auth_token_t reg = {
                .alias_type = MOQ_AUTH_TOKEN_REGISTER,
                .alias = 1, .token_type = 1,
                .token_value = (const uint8_t *)"tok", .token_value_len = 3,
            };
            uint8_t tb[64];
            moq_buf_writer_t tw;
            moq_buf_writer_init(&tw, tb, sizeof(tb));
            moq_d16_auth_token_encode(&tw, &reg);
            moq_kvp_entry_t p[1] = {{
                .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
                .value = tb, .value_len = moq_buf_writer_offset(&tw),
                .is_varint = false,
            }};
            uint8_t pn[256];
            moq_buf_writer_t pw;
            moq_buf_writer_init(&pw, pn, sizeof(pn));
            moq_bytes_t ns_parts[] = { moq_bytes_cstr("reg") };
            moq_namespace_t ns = { ns_parts, 1 };
            moq_d16_encode_publish_namespace(&pw, 0, &ns, p, 1);
            moq_session_on_control_bytes(sv, pn, moq_buf_writer_offset(&pw), 0);
            moq_event_t drain;
            moq_session_poll_events(sv, &drain, 1);
            moq_event_cleanup(&drain);
        }

        /* Send SUBSCRIBE_NAMESPACE with REGISTER alias=2 followed by
         * USE_ALIAS alias=99 (unknown). Auth stages the REGISTER, then
         * rejects on USE_ALIAS. Proves retry doesn't reprocess auth. */
        moq_d16_auth_token_t reg2 = {
            .alias_type = MOQ_AUTH_TOKEN_REGISTER,
            .alias = 2, .token_type = 3,
            .token_value = (const uint8_t *)"new", .token_value_len = 3,
        };
        moq_d16_auth_token_t bad_use = {
            .alias_type = MOQ_AUTH_TOKEN_USE_ALIAS,
            .alias = 99,
        };
        uint8_t tb_r[64], tb_u[64];
        moq_buf_writer_t tw_r, tw_u;
        moq_buf_writer_init(&tw_r, tb_r, sizeof(tb_r));
        moq_buf_writer_init(&tw_u, tb_u, sizeof(tb_u));
        moq_d16_auth_token_encode(&tw_r, &reg2);
        moq_d16_auth_token_encode(&tw_u, &bad_use);
        moq_kvp_entry_t params[2] = {
            { .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
              .value = tb_r, .value_len = moq_buf_writer_offset(&tw_r),
              .is_varint = false },
            { .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
              .value = tb_u, .value_len = moq_buf_writer_offset(&tw_u),
              .is_varint = false },
        };

        uint8_t buf[256];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_bytes_t parts[] = { moq_bytes_cstr("rej") };
        moq_namespace_t prefix = { parts, 1 };
        moq_d16_encode_subscribe_namespace(&w, 2, &prefix, 2, params, 2);
        size_t wire_len = moq_buf_writer_offset(&w);

        /* Fill the action queue so the auth-reject path WBs. */
        moq_action_t fill_acts[4];
        {
            moq_bytes_t fn[] = { moq_bytes_cstr("fill") };
            moq_namespace_t fns = { fn, 1 };
            moq_publish_namespace_cfg_t ncfg;
            moq_publish_namespace_cfg_init(&ncfg);
            ncfg.track_namespace = fns;
            moq_announcement_t ann;
            moq_session_publish_namespace(sv, &ncfg, 0, &ann);
            /* Action queue now has 1/1: PUBLISH_NAMESPACE send. */
        }

        moq_stream_ref_t bidi_ref = moq_stream_ref_from_u64(300);
        moq_result_t brc = moq_session_on_bidi_stream_bytes(sv, bidi_ref,
            buf, wire_len, true, 1000);
        MOQ_TEST_CHECK(brc == MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        /* Alias=2 staged but NOT committed yet (action WB before commit). */
        MOQ_TEST_CHECK(moq_token_cache_lookup(&sv->peer_token_cache, 2,
            NULL, NULL, NULL) == MOQ_TOKEN_ERR_UNKNOWN);

        /* Drain action queue, retry. */
        size_t na = moq_session_poll_actions(sv, fill_acts, 4);
        for (size_t i = 0; i < na; i++) moq_action_cleanup(&fill_acts[i]);

        moq_session_on_bidi_stream_bytes(sv, bidi_ref, NULL, 0, false, 1000);

        /* Should have queued SEND_BIDI_STREAM with REQUEST_ERROR. */
        na = moq_session_poll_actions(sv, fill_acts, 4);
        MOQ_TEST_CHECK(na >= 1);
        MOQ_TEST_CHECK(fill_acts[0].kind == MOQ_ACTION_SEND_BIDI_STREAM);
        for (size_t i = 0; i < na; i++) moq_action_cleanup(&fill_acts[i]);

        /* Alias=2 should be committed exactly once after retry. */
        MOQ_TEST_CHECK(moq_token_cache_lookup(&sv->peer_token_cache, 2,
            NULL, NULL, NULL) == MOQ_TOKEN_OK);

        /* Session still alive — auth reject is request-level, not session. */
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == SUBSCRIBE_NAMESPACE with wrong-scope param ignored =========== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        uint8_t buf[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_bytes_t parts[] = { moq_bytes_cstr("live") };
        moq_namespace_t prefix = { parts, 1 };
        uint8_t dt_v[8];
        size_t dt_len = moq_quic_varint_encode(5000, dt_v, sizeof(dt_v));
        moq_kvp_entry_t params[1] = {{
            .type = MOQ_MSG_PARAM_DELIVERY_TIMEOUT,
            .value = dt_v, .value_len = dt_len,
            .is_varint = true,
        }};
        moq_d16_encode_subscribe_namespace(&w, 0, &prefix, 2, params, 1);
        size_t wire_len = moq_buf_writer_offset(&w);

        moq_stream_ref_t bidi_ref = moq_stream_ref_from_u64(100);
        MOQ_TEST_CHECK(moq_session_on_bidi_stream_bytes(sv, bidi_ref,
            buf, wire_len, true, 0) == MOQ_OK);

        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NS_SUB_REQUEST);
        moq_event_cleanup(&ev);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == REQUEST_UPDATE for ns_sub → NOT_SUPPORTED, no close ========= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

        uint8_t msg[256];
        size_t msg_len = encode_sub_ns(msg, sizeof(msg), 0, "test", 0);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(100);
        moq_session_on_bidi_stream_bytes(sv, ref, msg, msg_len, false, 0);

        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NS_SUB_REQUEST);
        moq_event_cleanup(&ev);

        uint8_t wire[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, wire, sizeof(wire));
        moq_d16_encode_request_update(&w, 2, 0, NULL, 0);
        MOQ_TEST_CHECK(moq_session_on_control_bytes(sv, wire,
            moq_buf_writer_offset(&w), 1000) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        moq_action_t acts[4];
        size_t na = moq_session_poll_actions(sv, acts, 4);
        bool found_not_supported = false;
        for (size_t i = 0; i < na; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL) {
                moq_buf_reader_t cr;
                moq_buf_reader_init(&cr, acts[i].u.send_control.data,
                    acts[i].u.send_control.len);
                moq_control_envelope_t env;
                if (moq_control_decode_envelope(&cr, &env) == MOQ_OK &&
                    env.msg_type == MOQ_D16_REQUEST_ERROR) {
                    moq_d16_request_error_t err;
                    if (moq_d16_decode_request_error(env.payload,
                            env.payload_len, &err) == MOQ_OK &&
                        err.error_code == MOQ_REQUEST_ERROR_NOT_SUPPORTED)
                        found_not_supported = true;
                }
            }
            moq_action_cleanup(&acts[i]);
        }
        MOQ_TEST_CHECK(found_not_supported);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Multi-part prefix survives aligned scratch copy =============== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        /* Encode a 3-part SUBSCRIBE_NAMESPACE and feed via bidi stream. */
        uint8_t msg[256];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        moq_bytes_t ns_parts[] = {
            MOQ_BYTES_LITERAL("a"),
            MOQ_BYTES_LITERAL("bb"),
            MOQ_BYTES_LITERAL("ccc"),
        };
        moq_namespace_t prefix = { ns_parts, 3 };
        moq_d16_encode_subscribe_namespace(&w, 0, &prefix, 0, NULL, 0);
        size_t msg_len = moq_buf_writer_offset(&w);

        /* Burn one EVENT-scratch byte to force an unaligned offset (the prefix
         * copy uses event_scratch, not output_scratch -- the earlier version of
         * this test burned output_scratch and so never actually unaligned the
         * arena it was meant to exercise). Feed bidi bytes via the internal
         * handler (bypassing begin_advance, which would reset event_scratch_len). */
        moq_stream_ref_t ref = moq_stream_ref_from_u64(200);
        session_begin_advance(sv, 0);
        sv->event_scratch[0] = 0x42;
        sv->event_scratch_len = 1;
        MOQ_TEST_CHECK(handle_bidi_stream_bytes(sv, ref,
            msg, msg_len, false) == MOQ_OK);

        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NS_SUB_REQUEST);
        MOQ_TEST_CHECK(ev.u.ns_sub_request.track_namespace_prefix.count == 3);
        MOQ_TEST_CHECK(ev.u.ns_sub_request.track_namespace_prefix.parts[0].len == 1);
        MOQ_TEST_CHECK(memcmp(
            ev.u.ns_sub_request.track_namespace_prefix.parts[0].data,
            "a", 1) == 0);
        MOQ_TEST_CHECK(ev.u.ns_sub_request.track_namespace_prefix.parts[1].len == 2);
        MOQ_TEST_CHECK(memcmp(
            ev.u.ns_sub_request.track_namespace_prefix.parts[1].data,
            "bb", 2) == 0);
        MOQ_TEST_CHECK(ev.u.ns_sub_request.track_namespace_prefix.parts[2].len == 3);
        MOQ_TEST_CHECK(memcmp(
            ev.u.ns_sub_request.track_namespace_prefix.parts[2].data,
            "ccc", 3) == 0);
        moq_event_cleanup(&ev);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == RED: aligned prefix copy must not overrun event scratch ======= *
     * At an unaligned event_scratch offset, the prefix-parts array is allocated
     * aligned (consuming padding), then the parts bytes are memcpy'd onto the
     * tail. The cap is sized so the aligned array fits exactly but the tail does
     * not: the old padding-blind preflight (cur + array + tail <= cap) passes,
     * yet the real consumption (aligned + array + tail) overruns. The fix must
     * refuse the copy without advancing event_scratch_len past cap. */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        const size_t A = _Alignof(moq_bytes_t);
        const size_t E = sizeof(moq_bytes_t);
        /* One part of (A-1) bytes -> exact old-preflight fit at offset 1, but
         * aligned(1)=A then E array then (A-1) tail overruns cap = A + E. */
        uint8_t partbuf[64];
        memset(partbuf, 'z', A - 1);
        moq_bytes_t ns_parts[] = { { partbuf, A - 1 } };
        moq_namespace_t prefix = { ns_parts, 1 };
        uint8_t msg[256];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        moq_d16_encode_subscribe_namespace(&w, 0, &prefix, 0, NULL, 0);
        size_t msg_len = moq_buf_writer_offset(&w);

        moq_stream_ref_t ref = moq_stream_ref_from_u64(201);
        session_begin_advance(sv, 0);
        sv->event_scratch_len = 1;          /* unaligned start (padding = A-1) */
        sv->event_scratch_cap = A + E;      /* aligned array fits; +tail does not */
        moq_result_t hrc = handle_bidi_stream_bytes(sv, ref, msg, msg_len, false);

        /* Fixed: refused with WOULD_BLOCK before overflow -- length never exceeds
         * cap, and no NS_SUB_REQUEST event was queued. (Pre-fix: event_scratch_len
         * overruns cap and the event is queued.) */
        MOQ_TEST_CHECK(hrc == MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK(sv->event_scratch_len <= sv->event_scratch_cap);
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 0);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == RED: aligned suffix copy must not overrun event scratch ======= *
     * Same alignment hazard on the subscriber side: copy_suffix_to_event_scratch
     * allocates the suffix-parts array aligned then memcpy's the parts onto the
     * tail. With an unaligned offset and a cap that fits the aligned array but
     * not the tail, the copy must be refused without overrunning. */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

        moq_ns_sub_handle_t h; moq_stream_ref_t ref; uint64_t rid;
        setup_subscriber(c, &h, &ref, &rid);

        /* Accept the subscription (REQUEST_OK) and drain the NS_SUB_OK so the
         * event queue is empty before we unalign event scratch. */
        uint8_t okbuf[128];
        size_t oklen = encode_request_ok(okbuf, sizeof(okbuf), rid);
        moq_session_on_bidi_stream_bytes(c, ref, okbuf, oklen, false, 0);
        moq_event_t ev;
        while (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);

        const size_t A = _Alignof(moq_bytes_t);
        const size_t E = sizeof(moq_bytes_t);
        char sbuf[16];
        memset(sbuf, 'z', A - 1);
        sbuf[A - 1] = '\0';                 /* single suffix part of A-1 bytes */
        uint8_t ns_msg[256];
        size_t ns_len = encode_namespace_msg(ns_msg, sizeof(ns_msg),
                                             MOQ_D16_NAMESPACE, sbuf);

        session_begin_advance(c, 0);
        c->event_scratch_len = 1;           /* unaligned start */
        c->event_scratch_cap = A + E;       /* aligned array fits; +tail does not */
        moq_result_t hrc = handle_bidi_stream_bytes(c, ref, ns_msg, ns_len, false);

        /* Fixed: refused with WOULD_BLOCK before overflow; no NAMESPACE_FOUND. */
        MOQ_TEST_CHECK(hrc == MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK(c->event_scratch_len <= c->event_scratch_cap);
        bool saw_found = false;
        while (moq_session_poll_events(c, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_NAMESPACE_FOUND) saw_found = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(!saw_found);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == RED: aligned token copy must not overrun event scratch ======== *
     * The resolved-token array is allocated aligned then each token value is
     * memcpy'd onto the tail. Reuse a real client-encoded SUBSCRIBE_NAMESPACE
     * carrying one auth token (so the server resolves it), then feed those wire
     * bytes to the server at an unaligned event-scratch offset with a cap sized
     * so the 1-byte prefix copies but the token-value tail would overrun. */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

        const size_t A   = _Alignof(moq_bytes_t);
        const size_t E   = sizeof(moq_bytes_t);
        const size_t ART = _Alignof(moq_resolved_token_t);
        const size_t RT  = sizeof(moq_resolved_token_t);
        const size_t L0  = 1;
        size_t len1     = ((L0 + A - 1) & ~(A - 1)) + E + 1;  /* after 1B prefix */
        size_t aligned1 = (len1 + ART - 1) & ~(ART - 1);
        size_t cap      = aligned1 + RT;     /* token array fits; +value does not */
        size_t V        = ART - 1;           /* exact old-preflight fit */

        /* Client encodes a 1-byte-prefix SUBSCRIBE_NAMESPACE with one token. */
        uint8_t prefix_byte = 'p';
        moq_subscribe_namespace_cfg_t cfg;
        moq_subscribe_namespace_cfg_init(&cfg);
        moq_bytes_t parts[] = { { &prefix_byte, 1 } };
        cfg.track_namespace_prefix.parts = parts;
        cfg.track_namespace_prefix.count = 1;
        uint8_t tok_val[64];
        memset(tok_val, 0xAB, V);
        moq_auth_token_t tok = { .token_type = 33,
                                 .token_value = { tok_val, V } };
        cfg.auth_tokens = &tok;
        cfg.auth_token_count = 1;
        moq_ns_sub_handle_t hc;
        MOQ_TEST_CHECK_EQ_INT(moq_session_subscribe_namespace(c, &cfg, 0, &hc),
                              MOQ_OK);

        /* Capture the wire bytes from the OPEN_BIDI_STREAM action. */
        uint8_t wire[512]; size_t wire_len = 0;
        moq_stream_ref_t sref = moq_stream_ref_from_u64(0);
        moq_action_t acts[4];
        size_t na = moq_session_poll_actions(c, acts, 4);
        for (size_t i = 0; i < na; i++) {
            if (acts[i].kind == MOQ_ACTION_OPEN_BIDI_STREAM &&
                acts[i].u.open_bidi_stream.len <= sizeof(wire)) {
                wire_len = acts[i].u.open_bidi_stream.len;
                memcpy(wire, acts[i].u.open_bidi_stream.data, wire_len);
                sref = acts[i].u.open_bidi_stream.stream_ref;
            }
            moq_action_cleanup(&acts[i]);
        }
        MOQ_TEST_CHECK(wire_len > 0);

        /* Feed to the server at an unaligned offset with the tuned cap. */
        session_begin_advance(sv, 0);
        sv->event_scratch_len = L0;
        sv->event_scratch_cap = cap;
        moq_result_t hrc = handle_bidi_stream_bytes(sv, sref, wire, wire_len,
                                                    false);

        /* Fixed: token copy refused with WOULD_BLOCK before overflow; no
         * NS_SUB_REQUEST queued. */
        MOQ_TEST_CHECK(hrc == MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK(sv->event_scratch_len <= sv->event_scratch_cap);
        moq_event_t ev;
        bool saw_req = false;
        while (moq_session_poll_events(sv, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_NS_SUB_REQUEST) saw_req = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(!saw_req);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == subscribe_namespace with auth token reaches server event ====== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

        moq_subscribe_namespace_cfg_t cfg;
        moq_subscribe_namespace_cfg_init(&cfg);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("test") };
        cfg.track_namespace_prefix.parts = parts;
        cfg.track_namespace_prefix.count = 1;
        cfg.namespace_interest = 0;
        uint8_t tok_val[] = { 0xDE, 0xAD };
        moq_auth_token_t tok = {
            .token_type = 33,
            .token_value = { tok_val, sizeof(tok_val) },
        };
        cfg.auth_tokens = &tok;
        cfg.auth_token_count = 1;

        moq_ns_sub_handle_t h;
        MOQ_TEST_CHECK_EQ_INT(moq_session_subscribe_namespace(c, &cfg, 0, &h),
            MOQ_OK);

        moq_action_t acts[4];
        size_t na = moq_session_poll_actions(c, acts, 4);
        for (size_t i = 0; i < na; i++) {
            if (acts[i].kind == MOQ_ACTION_OPEN_BIDI_STREAM) {
                moq_session_on_bidi_stream_bytes(sv,
                    acts[i].u.open_bidi_stream.stream_ref,
                    acts[i].u.open_bidi_stream.data,
                    acts[i].u.open_bidi_stream.len,
                    false, 0);
            }
            moq_action_cleanup(&acts[i]);
        }

        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NS_SUB_REQUEST);
        MOQ_TEST_CHECK_EQ_SIZE(ev.u.ns_sub_request.token_count, 1);
        MOQ_TEST_CHECK_EQ_U64(ev.u.ns_sub_request.tokens[0].token_type, 33);
        MOQ_TEST_CHECK_EQ_SIZE(
            ev.u.ns_sub_request.tokens[0].token_value.len, 2);
        MOQ_TEST_CHECK(
            ev.u.ns_sub_request.tokens[0].token_value.data[0] == 0xDE);
        MOQ_TEST_CHECK(
            ev.u.ns_sub_request.tokens[0].token_value.data[1] == 0xAD);
        moq_event_cleanup(&ev);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* ============================================================== */
    /* PREFIX OVERLAP DETECTION                                       */
    /* ============================================================== */

    /* == Outbound: exact prefix overlap rejected ====================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

        moq_subscribe_namespace_cfg_t cfg;
        moq_subscribe_namespace_cfg_init(&cfg);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        cfg.track_namespace_prefix.parts = parts;
        cfg.track_namespace_prefix.count = 1;
        moq_ns_sub_handle_t h1;
        MOQ_TEST_CHECK_EQ_INT(moq_session_subscribe_namespace(c, &cfg,
            0, &h1), MOQ_OK);

        moq_ns_sub_handle_t h2;
        MOQ_TEST_CHECK_EQ_INT(moq_session_subscribe_namespace(c, &cfg,
            0, &h2), MOQ_ERR_INVAL);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Outbound: parent prefix overlaps child ======================= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

        moq_subscribe_namespace_cfg_t cfg1;
        moq_subscribe_namespace_cfg_init(&cfg1);
        moq_bytes_t parts1[] = { MOQ_BYTES_LITERAL("live") };
        cfg1.track_namespace_prefix.parts = parts1;
        cfg1.track_namespace_prefix.count = 1;
        moq_ns_sub_handle_t h1;
        MOQ_TEST_CHECK_EQ_INT(moq_session_subscribe_namespace(c, &cfg1,
            0, &h1), MOQ_OK);

        moq_subscribe_namespace_cfg_t cfg2;
        moq_subscribe_namespace_cfg_init(&cfg2);
        moq_bytes_t parts2[] = {
            MOQ_BYTES_LITERAL("live"),
            MOQ_BYTES_LITERAL("video"),
        };
        cfg2.track_namespace_prefix.parts = parts2;
        cfg2.track_namespace_prefix.count = 2;
        moq_ns_sub_handle_t h2;
        MOQ_TEST_CHECK_EQ_INT(moq_session_subscribe_namespace(c, &cfg2,
            0, &h2), MOQ_ERR_INVAL);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Outbound: disjoint prefixes succeed =========================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

        moq_subscribe_namespace_cfg_t cfg1;
        moq_subscribe_namespace_cfg_init(&cfg1);
        moq_bytes_t parts1[] = { MOQ_BYTES_LITERAL("live") };
        cfg1.track_namespace_prefix.parts = parts1;
        cfg1.track_namespace_prefix.count = 1;
        moq_ns_sub_handle_t h1;
        MOQ_TEST_CHECK_EQ_INT(moq_session_subscribe_namespace(c, &cfg1,
            0, &h1), MOQ_OK);

        moq_subscribe_namespace_cfg_t cfg2;
        moq_subscribe_namespace_cfg_init(&cfg2);
        moq_bytes_t parts2[] = { MOQ_BYTES_LITERAL("archive") };
        cfg2.track_namespace_prefix.parts = parts2;
        cfg2.track_namespace_prefix.count = 1;
        moq_ns_sub_handle_t h2;
        MOQ_TEST_CHECK_EQ_INT(moq_session_subscribe_namespace(c, &cfg2,
            0, &h2), MOQ_OK);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Inbound: overlapping prefix rejected with REQUEST_ERROR ======= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

        /* First subscribe — deliver via bidi stream to server. */
        {
            uint8_t msg[256];
            size_t msg_len = encode_sub_ns(msg, sizeof(msg), 0, "live", 0);
            moq_stream_ref_t ref = moq_stream_ref_from_u64(100);
            MOQ_TEST_CHECK_EQ_INT(moq_session_on_bidi_stream_bytes(sv,
                ref, msg, msg_len, false, 0), MOQ_OK);
        }
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        /* Second overlapping subscribe — should be rejected. */
        {
            uint8_t msg[256];
            size_t msg_len = encode_sub_ns(msg, sizeof(msg), 2, "live", 0);
            moq_stream_ref_t ref = moq_stream_ref_from_u64(200);
            MOQ_TEST_CHECK_EQ_INT(moq_session_on_bidi_stream_bytes(sv,
                ref, msg, msg_len, false, 0), MOQ_OK);
        }
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        /* Drain the NS_SUB_REQUEST event from the first subscribe
         * so the alloc balance stays clean. */
        { moq_event_t ev;
          while (moq_session_poll_events(sv, &ev, 1) == 1)
              moq_event_cleanup(&ev); }

        /* Verify REQUEST_ERROR action with PREFIX_OVERLAP code. */
        {
            moq_action_t acts[8];
            size_t na = moq_session_poll_actions(sv, acts, 8);
            bool found_err = false;
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_BIDI_STREAM &&
                    acts[i].u.send_bidi_stream.fin) {
                    moq_control_envelope_t env;
                    moq_buf_reader_t r;
                    moq_buf_reader_init(&r,
                        acts[i].u.send_bidi_stream.data,
                        acts[i].u.send_bidi_stream.len);
                    if (moq_control_decode_envelope(&r, &env) >= 0 &&
                        env.msg_type == MOQ_D16_REQUEST_ERROR) {
                        moq_d16_request_error_t err;
                        if (moq_d16_decode_request_error(env.payload,
                                env.payload_len, &err) >= 0) {
                            MOQ_TEST_CHECK_EQ_HEX(err.error_code,
                                (uint64_t)MOQ_REQUEST_ERROR_PREFIX_OVERLAP);
                            found_err = true;
                        }
                    }
                }
                moq_action_cleanup(&acts[i]);
            }
            MOQ_TEST_CHECK(found_err);
        }

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Outbound: oversized prefix total rejected ====================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

        uint8_t big[4097];
        memset(big, 'x', sizeof(big));
        moq_bytes_t parts[] = { { big, sizeof(big) } };
        moq_subscribe_namespace_cfg_t cfg;
        moq_subscribe_namespace_cfg_init(&cfg);
        cfg.track_namespace_prefix.parts = parts;
        cfg.track_namespace_prefix.count = 1;
        moq_ns_sub_handle_t h;
        MOQ_TEST_CHECK_EQ_INT(moq_session_subscribe_namespace(c, &cfg,
            0, &h), MOQ_ERR_INVAL);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* ============================================================== */
    /* NAMESPACE_DONE ORDERING                                        */
    /* ============================================================== */

    /* == Outbound: send_namespace_done before send_namespace → WRONG_STATE */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

        uint8_t msg[256];
        size_t msg_len = encode_sub_ns(msg, sizeof(msg), 0, "ord", 0);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(500);
        moq_session_on_bidi_stream_bytes(sv, ref, msg, msg_len, false, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_ns_sub_handle_t ph = ev.u.ns_sub_request.handle;
        moq_event_cleanup(&ev);
        moq_accept_ns_sub_cfg_t acc;
        moq_accept_ns_sub_cfg_init(&acc);
        moq_session_accept_ns_sub(sv, ph, &acc, 0);
        { moq_action_t da[8]; size_t dn;
          while ((dn = moq_session_poll_actions(sv, da, 8)) > 0)
              for (size_t i = 0; i < dn; i++) moq_action_cleanup(&da[i]); }

        moq_bytes_t sp2[] = { MOQ_BYTES_LITERAL("x") };
        moq_namespace_t suf = { sp2, 1 };
        MOQ_TEST_CHECK_EQ_INT(moq_session_send_namespace_done(sv, ph,
            &suf, 0), MOQ_ERR_WRONG_STATE);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Outbound: send_namespace then send_namespace_done succeeds ==== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

        uint8_t msg[256];
        size_t msg_len = encode_sub_ns(msg, sizeof(msg), 0, "ord2", 0);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(501);
        moq_session_on_bidi_stream_bytes(sv, ref, msg, msg_len, false, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_ns_sub_handle_t ph = ev.u.ns_sub_request.handle;
        moq_event_cleanup(&ev);
        moq_accept_ns_sub_cfg_t acc;
        moq_accept_ns_sub_cfg_init(&acc);
        moq_session_accept_ns_sub(sv, ph, &acc, 0);
        { moq_action_t da[8]; size_t dn;
          while ((dn = moq_session_poll_actions(sv, da, 8)) > 0)
              for (size_t i = 0; i < dn; i++) moq_action_cleanup(&da[i]); }

        moq_bytes_t sp2[] = { MOQ_BYTES_LITERAL("x") };
        moq_namespace_t suf = { sp2, 1 };
        MOQ_TEST_CHECK_EQ_INT(moq_session_send_namespace(sv, ph,
            &suf, 0), MOQ_OK);
        { moq_action_t da[8]; size_t dn;
          while ((dn = moq_session_poll_actions(sv, da, 8)) > 0)
              for (size_t i = 0; i < dn; i++) moq_action_cleanup(&da[i]); }
        MOQ_TEST_CHECK_EQ_INT(moq_session_send_namespace_done(sv, ph,
            &suf, 0), MOQ_OK);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Outbound: namespace → done → namespace again succeeds ======== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

        uint8_t msg[256];
        size_t msg_len = encode_sub_ns(msg, sizeof(msg), 0, "ord3", 0);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(502);
        moq_session_on_bidi_stream_bytes(sv, ref, msg, msg_len, false, 0);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_ns_sub_handle_t ph = ev.u.ns_sub_request.handle;
        moq_event_cleanup(&ev);
        moq_accept_ns_sub_cfg_t acc;
        moq_accept_ns_sub_cfg_init(&acc);
        moq_session_accept_ns_sub(sv, ph, &acc, 0);
        { moq_action_t da[8]; size_t dn;
          while ((dn = moq_session_poll_actions(sv, da, 8)) > 0)
              for (size_t i = 0; i < dn; i++) moq_action_cleanup(&da[i]); }

        moq_bytes_t sp2[] = { MOQ_BYTES_LITERAL("x") };
        moq_namespace_t suf = { sp2, 1 };
        MOQ_TEST_CHECK_EQ_INT(moq_session_send_namespace(sv, ph,
            &suf, 0), MOQ_OK);
        { moq_action_t da[8]; size_t dn;
          while ((dn = moq_session_poll_actions(sv, da, 8)) > 0)
              for (size_t i = 0; i < dn; i++) moq_action_cleanup(&da[i]); }
        MOQ_TEST_CHECK_EQ_INT(moq_session_send_namespace_done(sv, ph,
            &suf, 0), MOQ_OK);
        { moq_action_t da[8]; size_t dn;
          while ((dn = moq_session_poll_actions(sv, da, 8)) > 0)
              for (size_t i = 0; i < dn; i++) moq_action_cleanup(&da[i]); }
        MOQ_TEST_CHECK_EQ_INT(moq_session_send_namespace(sv, ph,
            &suf, 0), MOQ_OK);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Inbound: NAMESPACE_DONE before NAMESPACE closes session ======= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

        moq_ns_sub_handle_t h; moq_stream_ref_t ref; uint64_t rid;
        setup_subscriber(c, &h, &ref, &rid);
        uint8_t ok[128];
        size_t olen = encode_request_ok(ok, sizeof(ok), rid);
        moq_session_on_bidi_stream_bytes(c, ref, ok, olen, false, 0);
        { moq_event_t ev;
          moq_session_poll_events(c, &ev, 1);
          moq_event_cleanup(&ev); }

        uint8_t nd[128];
        size_t ndlen = encode_namespace_msg(nd, sizeof(nd),
            MOQ_D16_NAMESPACE_DONE, "never_announced");
        moq_session_on_bidi_stream_bytes(c, ref, nd, ndlen, false, 0);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_CLOSED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Inbound: NAMESPACE then NAMESPACE_DONE emits both events ===== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

        moq_ns_sub_handle_t h; moq_stream_ref_t ref; uint64_t rid;
        setup_subscriber(c, &h, &ref, &rid);
        uint8_t ok[128];
        size_t olen = encode_request_ok(ok, sizeof(ok), rid);
        moq_session_on_bidi_stream_bytes(c, ref, ok, olen, false, 0);
        { moq_event_t ev;
          moq_session_poll_events(c, &ev, 1);
          moq_event_cleanup(&ev); }

        uint8_t ns[128];
        size_t nlen = encode_namespace_msg(ns, sizeof(ns),
            MOQ_D16_NAMESPACE, "track");
        moq_session_on_bidi_stream_bytes(c, ref, ns, nlen, false, 0);

        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NAMESPACE_FOUND);
        moq_event_cleanup(&ev);

        uint8_t nd[128];
        size_t ndlen = encode_namespace_msg(nd, sizeof(nd),
            MOQ_D16_NAMESPACE_DONE, "track");
        moq_session_on_bidi_stream_bytes(c, ref, nd, ndlen, false, 0);

        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(c, &ev, 1), 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NAMESPACE_GONE);
        moq_event_cleanup(&ev);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Inbound: unbounded unique NAMESPACE suffixes close session === *
     * A peer that announces an endless stream of distinct namespaces would,
     * without a bound, retain unbounded suffix-key memory. Each suffix is now
     * charged against the (tiny) receive budget; once exceeded the session is
     * closed. Events are drained every iteration, so the event queue is never
     * the limiter -- the byte budget is. */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t cextra = MOQ_SESSION_CFG_INIT;
        cextra.max_receive_buffer_bytes = 256;   /* tiny suffix-tracking budget */
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, &cextra, NULL);

        moq_ns_sub_handle_t h; moq_stream_ref_t ref; uint64_t rid;
        setup_subscriber(c, &h, &ref, &rid);
        uint8_t ok[128];
        size_t olen = encode_request_ok(ok, sizeof(ok), rid);
        moq_session_on_bidi_stream_bytes(c, ref, ok, olen, false, 0);
        { moq_event_t ev;
          while (moq_session_poll_events(c, &ev, 1) == 1)
              moq_event_cleanup(&ev); }

        bool closed = false;
        for (int i = 0; i < 64 && !closed; i++) {
            char suffix[32];
            snprintf(suffix, sizeof(suffix), "unique-%d", i);
            uint8_t ns[256];
            size_t nlen = encode_namespace_msg(ns, sizeof(ns),
                MOQ_D16_NAMESPACE, suffix);
            moq_session_on_bidi_stream_bytes(c, ref, ns, nlen, false, 0);
            if (moq_session_state(c) == MOQ_SESS_CLOSED) { closed = true; break; }
            moq_event_t ev;
            while (moq_session_poll_events(c, &ev, 1) == 1)
                moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(closed);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Inbound: duplicate NAMESPACE suffixes do not grow the budget = *
     * Re-announcing the same suffix is a no-op for tracking: it must not
     * double-charge or grow the set, so the session stays open indefinitely. */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t cextra = MOQ_SESSION_CFG_INIT;
        cextra.max_receive_buffer_bytes = 256;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, &cextra, NULL);

        moq_ns_sub_handle_t h; moq_stream_ref_t ref; uint64_t rid;
        setup_subscriber(c, &h, &ref, &rid);
        uint8_t ok[128];
        size_t olen = encode_request_ok(ok, sizeof(ok), rid);
        moq_session_on_bidi_stream_bytes(c, ref, ok, olen, false, 0);
        { moq_event_t ev;
          while (moq_session_poll_events(c, &ev, 1) == 1)
              moq_event_cleanup(&ev); }

        for (int i = 0; i < 64; i++) {
            uint8_t ns[256];
            size_t nlen = encode_namespace_msg(ns, sizeof(ns),
                MOQ_D16_NAMESPACE, "same-suffix");
            moq_session_on_bidi_stream_bytes(c, ref, ns, nlen, false, 0);
            MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);
            moq_event_t ev;
            while (moq_session_poll_events(c, &ev, 1) == 1)
                moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Inbound: NAMESPACE_DONE releases tracked suffix bytes ========= *
     * Many announce/done cycles with distinct suffixes stay open under the same
     * tiny budget that the unique-flood test above closes within ~9 suffixes:
     * each DONE releases its key bytes before the next announce. */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t cextra = MOQ_SESSION_CFG_INIT;
        cextra.max_receive_buffer_bytes = 256;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, &cextra, NULL);

        moq_ns_sub_handle_t h; moq_stream_ref_t ref; uint64_t rid;
        setup_subscriber(c, &h, &ref, &rid);
        uint8_t ok[128];
        size_t olen = encode_request_ok(ok, sizeof(ok), rid);
        moq_session_on_bidi_stream_bytes(c, ref, ok, olen, false, 0);
        { moq_event_t ev;
          while (moq_session_poll_events(c, &ev, 1) == 1)
              moq_event_cleanup(&ev); }

        for (int i = 0; i < 64; i++) {
            char suffix[32];
            snprintf(suffix, sizeof(suffix), "cycle-%d", i);
            uint8_t ns[256];
            size_t nlen = encode_namespace_msg(ns, sizeof(ns),
                MOQ_D16_NAMESPACE, suffix);
            moq_session_on_bidi_stream_bytes(c, ref, ns, nlen, false, 0);
            MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);
            { moq_event_t ev;
              while (moq_session_poll_events(c, &ev, 1) == 1)
                  moq_event_cleanup(&ev); }

            uint8_t nd[256];
            size_t ndlen = encode_namespace_msg(nd, sizeof(nd),
                MOQ_D16_NAMESPACE_DONE, suffix);
            moq_session_on_bidi_stream_bytes(c, ref, nd, ndlen, false, 0);
            MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);
            { moq_event_t ev;
              while (moq_session_poll_events(c, &ev, 1) == 1)
                  moq_event_cleanup(&ev); }
        }
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Inbound: under a normal receive budget, draining keeps it open = *
     * With the default (large) budget, many unique NAMESPACE responses with
     * normal event draining do NOT close the session -- proving the limiter is
     * the receive-budget byte cap, not event-queue draining. */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);  /* default budget */

        moq_ns_sub_handle_t h; moq_stream_ref_t ref; uint64_t rid;
        setup_subscriber(c, &h, &ref, &rid);
        uint8_t ok[128];
        size_t olen = encode_request_ok(ok, sizeof(ok), rid);
        moq_session_on_bidi_stream_bytes(c, ref, ok, olen, false, 0);
        { moq_event_t ev;
          while (moq_session_poll_events(c, &ev, 1) == 1)
              moq_event_cleanup(&ev); }

        for (int i = 0; i < 128; i++) {
            char suffix[32];
            snprintf(suffix, sizeof(suffix), "normal-%d", i);
            uint8_t ns[256];
            size_t nlen = encode_namespace_msg(ns, sizeof(ns),
                MOQ_D16_NAMESPACE, suffix);
            moq_session_on_bidi_stream_bytes(c, ref, ns, nlen, false, 0);
            MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);
            moq_event_t ev;
            while (moq_session_poll_events(c, &ev, 1) == 1)
                moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* ============================================================== */
    /* BIDI STREAM DISPATCH                                           */
    /* ============================================================== */

    /* == Unknown bidi stream type closes session ======================= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

        /* Inject a bidi stream with a non-SUBSCRIBE_NAMESPACE type.
         * Use type 0xFF which is not a valid D16 bidi request. */
        uint8_t fake[16];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, fake, sizeof(fake));
        moq_buf_write_varint(&w, 0xFF);
        moq_buf_write_varint(&w, 0);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(999);
        moq_session_on_bidi_stream_bytes(sv, ref, fake,
            moq_buf_writer_offset(&w), false, 0);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Valid SUBSCRIBE_NAMESPACE still works through dispatcher ====== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

        uint8_t msg[256];
        size_t msg_len = encode_sub_ns(msg, sizeof(msg), 0, "disp", 0);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1000);
        MOQ_TEST_CHECK_EQ_INT(moq_session_on_bidi_stream_bytes(sv, ref,
            msg, msg_len, false, 0), MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NS_SUB_REQUEST);
        moq_event_cleanup(&ev);

        DRAIN_BOTH(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Incomplete varint on bidi stream with FIN closes session ===== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 64, 64, &c, &sv, NULL, NULL);

        uint8_t trunc = 0x40;
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1001);
        moq_session_on_bidi_stream_bytes(sv, ref, &trunc, 1, true, 0);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    MOQ_TEST_PASS("test_session_namespace_sub");
    return failures;
}
