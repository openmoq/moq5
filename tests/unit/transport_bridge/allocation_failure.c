#include "test_cases.h"

/*
 * BR-RETAIN: the draft-16 namespace-response route, suffix-tracker origin.
 * Session-side this origin is NOMEM_RETAIN; behind the bridge it is the
 * one fatal terminal.
 */
int test_bridge_nomem_ns_response(void)
{
    int failures = 0;

    /* The canonical key of a one-part 2-byte suffix ("aa") is
     * [count u8][u16 len][bytes] = 5 bytes; the stored copy repeats it. */
    static const fp_expect_t k_sig[4] = {
        { FP_ALLOC, FP_SIZE_EXACT, 5, 0 },      /* canonical key */
        { FP_ALLOC, FP_SIZE_ANY, 0, 0 },        /* tracker (private) */
        { FP_ALLOC, FP_SIZE_ANY, 0, 0 },        /* tree node (private) */
        { FP_ALLOC, FP_SIZE_SAME_AS, 0, 0 },    /* stored key copy (== key) */
    };
    fp_attempt_t base_log[FP_LOG_CAP];
    size_t base_n = 0;

    /* No-fault fixture: the operation's signature on this route. */
    {
        fp_alloc_state_t fs = {0};
        moq_alloc_t alloc = fp_allocator(&fs);
        test_pair_t tp;
        if (test_pair_init_client_alloc(&tp, &alloc) < 0) {
            failures++;
            return failures;
        }
        MOQ_TEST_CHECK(setup_handshake(&tp));
        uint64_t bidi = br_ns_establish(&tp, &failures);
        MOQ_TEST_CHECK(bidi != 0);

        uint8_t msg[128];
        size_t mlen = br_encode_d16_namespace(msg, sizeof(msg), "aa");
        fs.log_from = fs.call_count;
        fs.log_len = 0;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_transport_bridge_on_peer_bidi_bytes(
                tp.client_bridge, bidi, msg, mlen, false, 0),
            (int)MOQ_OK);
        failures += fp_check_signature(&fs, 0, k_sig, 4, "br-retain-base");
        memcpy(base_log, fs.log, fs.log_len * sizeof(fp_attempt_t));
        base_n = fs.log_len;
        MOQ_TEST_CHECK_EQ_SIZE(base_n, 4u);
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(tp.client, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NAMESPACE_FOUND);
        moq_event_cleanup(&ev);
        failures += fp_sticky_clean(&fs, "br-retain-base");
        test_pair_destroy(&tp);
        failures += br_alloc_zeroed(&fs, "br-retain-base");
    }

    /* Failing fixture: the tracker origin, and the durable postcondition. */
    {
        fp_alloc_state_t fs = {0};
        moq_alloc_t alloc = fp_allocator(&fs);
        test_pair_t tp;
        if (test_pair_init_client_alloc(&tp, &alloc) < 0) {
            failures++;
            return failures;
        }
        MOQ_TEST_CHECK(setup_handshake(&tp));
        uint64_t bidi = br_ns_establish(&tp, &failures);
        MOQ_TEST_CHECK(bidi != 0);

        uint8_t msg[128];
        size_t mlen = br_encode_d16_namespace(msg, sizeof(msg), "aa");
        /* Not full: the NOMEM is attributable to the armed allocation. */
        MOQ_TEST_CHECK(!event_queue_full(tp.client));
        fs.log_from = fs.call_count;
        fs.log_len = 0;
        fs.fail_at = fs.call_count + 2;     /* +1 = key, +2 = the tracker */
        moq_result_t rc = moq_transport_bridge_on_peer_bidi_bytes(
            tp.client_bridge, bidi, msg, mlen, false, 0);
        fp_context("br-retain", 2, 4, &fs);
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_ERR_NOMEM);
        MOQ_TEST_CHECK_EQ_U64(fs.call_count, fs.fail_at);
        fs.fail_at = 0;
        failures += fp_check_prefix(&fs, 0, base_log, 2, "br-retain");
        failures += br_fatal_postcondition(tp.client_bridge, bidi, &fs,
                                           "br-retain");
        test_pair_destroy(&tp);
        failures += br_alloc_zeroed(&fs, "br-retain");
    }

    return failures;
}


/* One draft-18 LARGEST_OBJECT SUBSCRIBE, left pending on the server. */
static size_t br_encode_d18_subscribe(uint8_t *buf, size_t cap,
                                      uint64_t req_id)
{
    moq_d18_msg_params_t mp;
    memset(&mp, 0, sizeof(mp));
    mp.has_filter = true;
    mp.filter_type = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    mp.has_forward = true;
    mp.forward = 1;
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_namespace_t ns = { ns_parts, 1 };
    moq_bytes_t name = MOQ_BYTES_LITERAL("v0");
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    moq_d18_encode_subscribe(&w, req_id, &ns, name, &mp);
    return moq_buf_writer_offset(&w);
}


/* One joining FETCH carrying two USE_VALUE tokens of distinct lengths. */
static size_t br_encode_d18_join_fetch2(uint8_t *buf, size_t cap)
{
    moq_d18_fetch_t f;
    memset(&f, 0, sizeof(f));
    f.request_id = 2;
    f.fetch_type = 2;              /* relative joining */
    f.joining_request_id = 0;
    f.joining_start = 3;
    f.params.auth_token_count = 2;
    f.params.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_USE_VALUE;
    f.params.auth_tokens[0].token_type = 7;
    f.params.auth_tokens[0].token_value = MOQ_BYTES_LITERAL("jointok");
    f.params.auth_tokens[1].alias_type = MOQ_AUTH_TOKEN_USE_VALUE;
    f.params.auth_tokens[1].token_type = 9;
    f.params.auth_tokens[1].token_value =
        MOQ_BYTES_LITERAL("second-join-token");
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    moq_d18_encode_fetch(&w, &f);
    return moq_buf_writer_offset(&w);
}


/* PENDING_JOIN fetch entries on the receiving session (white-box). */
static int count_busy_pending_joins(const moq_session_t *s)
{
    int n = 0;
    for (size_t i = 0; i < s->fetch_cap; i++)
        if (s->fetches[i].state == MOQ_FETCH_PENDING_JOIN) n++;
    return n;
}


/* Feed the pending SUBSCRIBE the joining FETCH references; the SERVER is
 * the receiving session. */
static void br_join_prepare(test_pair_t *tp, int *failures_out)
{
    uint8_t sub[160];
    size_t sn = br_encode_d18_subscribe(sub, sizeof(sub), 0);
    if (moq_transport_bridge_on_peer_bidi_bytes(
            tp->server_bridge, 5001, sub, sn, false, 0) != MOQ_OK)
        (*failures_out)++;
    bool got = false;
    moq_event_t ev;
    while (moq_session_poll_events(tp->server, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) got = true;
        moq_event_cleanup(&ev);
    }
    if (!got) (*failures_out)++;
    moq_action_t a;
    while (moq_session_poll_actions(tp->server, &a, 1) > 0)
        moq_action_cleanup(&a);
}


/*
 * BR-REDELIVER: the draft-18 joining-FETCH route, second token-copy
 * origin -- so the first copy's release still runs under the failure.
 * Session-side this origin is NOMEM_REDELIVER; behind the bridge it is
 * the SAME fatal terminal as BR-RETAIN, deliberately: both recovery modes
 * collapse there, and no session-level recovery is claimed behind it.
 */
int test_bridge_nomem_joining_fetch(void)
{
    int failures = 0;

    static const fp_expect_t k_sig[2] = {
        { FP_ALLOC, FP_SIZE_EXACT, 7, 0 },   /* "jointok" */
        { FP_ALLOC, FP_SIZE_EXACT, 17, 0 },  /* "second-join-token" */
    };
    fp_attempt_t base_log[FP_LOG_CAP];
    size_t base_n = 0;

    /* No-fault fixture: the operation's signature on this route. */
    {
        fp_alloc_state_t fs = {0};
        moq_alloc_t alloc = fp_allocator(&fs);
        test_pair_t tp;
        if (d18_pair_init_alloc(&tp, 0, moq_alloc_default(), &alloc) < 0) {
            failures++;
            return failures;
        }
        moq_session_start(tp.client, 0);
        moq_session_start(tp.server, 0);
        d18_shuttle_until_quiescent(&tp, 30, 0);
        moq_event_t ev;
        while (moq_session_poll_events(tp.client, &ev, 1) > 0)
            moq_event_cleanup(&ev);
        while (moq_session_poll_events(tp.server, &ev, 1) > 0)
            moq_event_cleanup(&ev);
        br_join_prepare(&tp, &failures);

        uint8_t fb[224];
        size_t fn = br_encode_d18_join_fetch2(fb, sizeof(fb));
        fs.log_from = fs.call_count;
        fs.log_len = 0;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_transport_bridge_on_peer_bidi_bytes(
                tp.server_bridge, 5003, fb, fn, false, 0),
            (int)MOQ_OK);
        failures += fp_check_signature(&fs, 0, k_sig, 2, "br-redeliver-base");
        memcpy(base_log, fs.log, fs.log_len * sizeof(fp_attempt_t));
        base_n = fs.log_len;
        MOQ_TEST_CHECK_EQ_SIZE(base_n, 2u);
        MOQ_TEST_CHECK_EQ_INT(count_busy_pending_joins(tp.server), 1);
        failures += fp_sticky_clean(&fs, "br-redeliver-base");
        test_pair_destroy(&tp);
        failures += br_alloc_zeroed(&fs, "br-redeliver-base");
    }

    /* Failing fixture: the second copy, and the durable postcondition. */
    {
        fp_alloc_state_t fs = {0};
        moq_alloc_t alloc = fp_allocator(&fs);
        test_pair_t tp;
        if (d18_pair_init_alloc(&tp, 0, moq_alloc_default(), &alloc) < 0) {
            failures++;
            return failures;
        }
        moq_session_start(tp.client, 0);
        moq_session_start(tp.server, 0);
        d18_shuttle_until_quiescent(&tp, 30, 0);
        moq_event_t ev;
        while (moq_session_poll_events(tp.client, &ev, 1) > 0)
            moq_event_cleanup(&ev);
        while (moq_session_poll_events(tp.server, &ev, 1) > 0)
            moq_event_cleanup(&ev);
        br_join_prepare(&tp, &failures);

        uint8_t fb[224];
        size_t fn = br_encode_d18_join_fetch2(fb, sizeof(fb));
        MOQ_TEST_CHECK(!event_queue_full(tp.server));
        fs.log_from = fs.call_count;
        fs.log_len = 0;
        fs.fail_at = fs.call_count + 2;     /* the second token copy */
        moq_result_t rc = moq_transport_bridge_on_peer_bidi_bytes(
            tp.server_bridge, 5003, fb, fn, false, 0);
        fp_context("br-redeliver", 2, 2, &fs);
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_ERR_NOMEM);
        MOQ_TEST_CHECK_EQ_U64(fs.call_count, fs.fail_at);
        fs.fail_at = 0;
        failures += fp_check_prefix(&fs, 0, base_log, 2, "br-redeliver");
        MOQ_TEST_CHECK_EQ_INT(count_busy_pending_joins(tp.server), 0);
        failures += br_fatal_postcondition(tp.server_bridge, 5003, &fs,
                                           "br-redeliver");
        test_pair_destroy(&tp);
        failures += br_alloc_zeroed(&fs, "br-redeliver");
    }

    return failures;
}
