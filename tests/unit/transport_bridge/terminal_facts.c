#include "test_cases.h"

/* == Main =========================================================== */


/* --- independent monotonic terminal facts -------------------------------- *
 * ENQUEUED and OBSERVED are separate. Availability is not observation: a
 * queued-but-unpolled MOQ_EVENT_SESSION_CLOSED must read enqueued=true,
 * observed=false, and only the poll that TRANSFERS it flips observed. */
int test_terminal_facts_enqueued_then_observed(void)
{
    int failures = 0;
    test_pair_t tp;
    if (test_pair_init(&tp) < 0) { failures++; return failures; }

    bool observed = true; /* poisoned: must be overwritten */
    MOQ_TEST_CHECK(!moq_transport_bridge_terminal_facts(tp.client_bridge,
                                                        &observed));
    MOQ_TEST_CHECK(!observed);          /* neither fact before terminal */

    /* peer-side terminal: the event is enqueued, nobody has polled it */
    held_bridge_close(tp.client_bridge, 0x42, 0);
    observed = true;
    MOQ_TEST_CHECK(moq_transport_bridge_terminal_facts(tp.client_bridge,
                                                       &observed));
    MOQ_TEST_CHECK(!observed);          /* queued is NOT observed */

    /* the poll that transfers it is what makes it observed */
    moq_event_t ev;
    size_t n = 0;
    MOQ_TEST_CHECK(moq_session_poll_events_ex(tp.client, &ev, 1,
                                              sizeof(ev), &n) == MOQ_OK);
    MOQ_TEST_CHECK(n == 1);
    MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SESSION_CLOSED);
    observed = false;
    MOQ_TEST_CHECK(moq_transport_bridge_terminal_facts(tp.client_bridge,
                                                       &observed));
    MOQ_TEST_CHECK(observed);

    /* monotonic + idempotent: repeating the terminal does not clear anything,
     * and a second poll (no event left) leaves both facts set */
    held_bridge_close(tp.client_bridge, 0x43, 0);
    n = 0;
    (void)moq_session_poll_events_ex(tp.client, &ev, 1, sizeof(ev), &n);
    observed = false;
    MOQ_TEST_CHECK(moq_transport_bridge_terminal_facts(tp.client_bridge,
                                                       &observed));
    MOQ_TEST_CHECK(observed);

    /* out_observed is optional */
    MOQ_TEST_CHECK(moq_transport_bridge_terminal_facts(tp.client_bridge, NULL));

    test_pair_destroy(&tp);
    return failures;
}


/* A non-terminal event must not set the observed fact, and the facts are not
 * derived from the bridge's own terminal latches. */
int test_terminal_facts_not_set_by_other_events(void)
{
    int failures = 0;
    test_pair_t tp;
    if (test_pair_init(&tp) < 0) { failures++; return failures; }
    if (!setup_handshake(&tp)) { failures++; test_pair_destroy(&tp); return failures; }

    /* the handshake surfaced SETUP_COMPLETE; draining it must not look terminal */
    moq_event_t ev;
    size_t n = 0;
    while (moq_session_poll_events_ex(tp.client, &ev, 1, sizeof(ev),
                                      &n) == MOQ_OK && n > 0)
        n = 0;

    bool observed = true;
    MOQ_TEST_CHECK(!moq_transport_bridge_terminal_facts(tp.client_bridge,
                                                        &observed));
    MOQ_TEST_CHECK(!observed);

    test_pair_destroy(&tp);
    return failures;
}


/* A bridge-local endpoint failure can latch fatal before the physical
 * transport reports its terminal. The bridge latch clears transport state but
 * does not itself close the session, so either transport-terminal flavor must
 * still deliver exactly one SESSION_CLOSED while preserving the first cause. */
static int run_already_fatal_transport_terminal(bool clean)
{
    int failures = 0;
    test_pair_t tp;
    if (test_pair_init(&tp) < 0) { failures++; return failures; }

    const uint64_t first_code = 0x51;
    const uint64_t later_code = 0x62;
    bridge_set_fatal(tp.client_bridge, first_code);

    bool observed = true;
    MOQ_TEST_CHECK(moq_transport_bridge_is_fatal(tp.client_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_closed(tp.client_bridge));
    MOQ_TEST_CHECK(moq_transport_bridge_fatal_code(tp.client_bridge) ==
                   first_code);
    MOQ_TEST_CHECK(moq_session_state(tp.client) != MOQ_SESS_CLOSED);
    MOQ_TEST_CHECK(!moq_transport_bridge_terminal_facts(tp.client_bridge,
                                                        &observed));
    MOQ_TEST_CHECK(!observed);

    moq_result_t rc = clean
        ? held_bridge_close(tp.client_bridge, later_code,
                                                  1000)
        : held_bridge_error(tp.client_bridge, later_code,
                                                  1000);
    MOQ_TEST_CHECK(rc == MOQ_OK);
    MOQ_TEST_CHECK(moq_transport_bridge_is_fatal(tp.client_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_closed(tp.client_bridge));
    MOQ_TEST_CHECK(moq_transport_bridge_fatal_code(tp.client_bridge) ==
                   first_code);
    MOQ_TEST_CHECK(moq_session_state(tp.client) == MOQ_SESS_CLOSED);

    moq_event_t ev;
    size_t n = 0;
    memset(&ev, 0, sizeof(ev));
    MOQ_TEST_CHECK(moq_session_poll_events_ex(tp.client, &ev, 1, sizeof(ev),
                                              &n) == MOQ_OK);
    MOQ_TEST_CHECK(n == 1);
    if (n == 1) {
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SESSION_CLOSED);
        MOQ_TEST_CHECK(ev.u.closed.code == first_code);
        moq_event_cleanup(&ev);
    }

    /* The alternate flavor and repeated original flavor are idempotent. */
    (void)held_bridge_error(tp.client_bridge, 0x73,
                                                  1001);
    (void)held_bridge_close(tp.client_bridge, 0x84,
                                                  1002);
    n = 0;
    MOQ_TEST_CHECK(moq_session_poll_events_ex(tp.client, &ev, 1, sizeof(ev),
                                              &n) == MOQ_OK);
    MOQ_TEST_CHECK(n == 0);
    observed = false;
    MOQ_TEST_CHECK(moq_transport_bridge_terminal_facts(tp.client_bridge,
                                                       &observed));
    MOQ_TEST_CHECK(observed);

    test_pair_destroy(&tp);
    return failures;
}


int test_already_fatal_transport_terminal(void)
{
    return run_already_fatal_transport_terminal(false) +
           run_already_fatal_transport_terminal(true);
}


/* A SETUP whose completion-event tokens can never fit the arena must reach the
 * normal close path so the bridge dispatches CLOSE_SESSION: a buffer error here
 * would instead be escalated to a connection fatal, losing close semantics. */
int test_setup_scratch_shortfall_closes_not_fatal(void)
{
    int failures = 0;

    moq_session_cfg_t scfg;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_SERVER);
    scfg.output_scratch_size = 32;
    moq_session_t *sv = NULL;
    MOQ_TEST_CHECK(moq_session_create(&scfg, 0, &sv) == MOQ_OK);
    if (!sv) return failures;

    fake_endpoint_t ep;
    held_endpoint_init(&ep, 3000, 4000);
    moq_transport_bridge_t *br = NULL;
    moq_transport_bridge_cfg_t bcfg;
    moq_transport_bridge_cfg_init(&bcfg, moq_alloc_default());
    HELD_REQUIRE(held_bridge_create(&bcfg, sv, &ep.vtable, &ep,
                                               &br) == MOQ_OK);
    if (!br) { moq_session_destroy(sv); return failures; }

    uint8_t val[40];
    for (size_t i = 0; i < sizeof(val); i++)
        val[i] = (uint8_t)(0x41 + (i % 26));
    uint8_t tokbuf[64];
    moq_buf_writer_t tw;
    moq_buf_writer_init(&tw, tokbuf, sizeof(tokbuf));
    moq_buf_write_vi64(&tw, MOQ_AUTH_TOKEN_USE_VALUE);
    moq_buf_write_vi64(&tw, 0);
    moq_buf_write_vi64(&tw, sizeof(val));
    moq_buf_write_raw(&tw, val, sizeof(val));

    moq_kvp_entry_t prm;
    memset(&prm, 0, sizeof(prm));
    prm.type = MOQ_SETUP_PARAM_AUTHORIZATION_TOKEN;
    prm.value = tokbuf;
    prm.value_len = moq_buf_writer_offset(&tw);

    uint8_t msg[256];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, msg, sizeof(msg));
    moq_d16_encode_client_setup(&w, &prm, 1);

    moq_result_t rc = moq_transport_bridge_on_peer_control_bytes(
        br, 0 /* peer control stream */, msg, moq_buf_writer_offset(&w),
        false, 0);
    MOQ_TEST_CHECK(rc == MOQ_OK);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(br));
    MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);

    MOQ_TEST_CHECK(held_bridge_service(br, 0) == MOQ_OK);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(br));

    size_t closes = 0;
    MOQ_TEST_CHECK(ep.count < FAKE_EP_MAX_OPS);
    for (size_t i = 0; i < ep.count; i++) {
        if (ep.ops[i].kind != FAKE_OP_CLOSE) continue;
        closes++;
        MOQ_TEST_CHECK(ep.ops[i].error_code == 0x1);
    }
    MOQ_TEST_CHECK(closes == 1);

    held_bridge_destroy(br);
    moq_session_destroy(sv);
    return failures;
}
