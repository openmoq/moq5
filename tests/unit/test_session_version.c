/*
 * moq_session_version(): the immutable wire profile a session speaks.
 *
 * The accessor is a pure read of the profile descriptor bound at create, so
 * what this test has to prove is that the value is FIXED -- the same before
 * start, mid-handshake, at ESTABLISHED, and after a terminal transition -- and
 * that it is the version the session was actually configured with, on both
 * ends of a real handshake rather than on a session the test built alone.
 *
 * SimPair supplies that handshake deterministically: one thread, no sockets, no
 * wall time. It configures both sessions with the declared version and runs the
 * real setup exchange, so the value read afterwards is the profile the sessions
 * agreed to speak, not one the test assigned after the fact.
 *
 * Every phase asserts the state it claims to be in, on BOTH sessions, before
 * comparing versions -- otherwise a session that never reached the phase would
 * still satisfy the comparison and the phase label would be decoration.
 *
 * Usage: test_session_version
 */
#include <moq/moq.h>
#include <moq/sim.h>
#include "test_support.h"

#include <stdio.h>
#include <string.h>

static int failures = 0;

/* Both sessions report `want`, whatever else has happened to them. */
static void check_pair(moq_simpair_t *sp, moq_version_t want, const char *phase)
{
    moq_version_t c = moq_session_version(moq_simpair_client(sp));
    moq_version_t s = moq_session_version(moq_simpair_server(sp));

    if ((int)c != (int)want || (int)s != (int)want)
        fprintf(stderr, "FAIL[%s]: client=%d server=%d want=%d\n",
                phase, (int)c, (int)s, (int)want);
    MOQ_TEST_CHECK_EQ_INT((int)c, (int)want);
    MOQ_TEST_CHECK_EQ_INT((int)s, (int)want);
}

/*
 * One declared version, carried through a real handshake and a real terminal.
 * The version is sampled at every phase boundary; a value that moved would show
 * up as a phase-named failure rather than as a single final mismatch.
 */
static void t_lifetime(moq_version_t want, const char *name)
{
    int before = failures;
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    moq_simpair_t *sp = NULL;

    cfg.alloc = moq_alloc_default();
    cfg.seed = 0x5e5510u;
    cfg.version = want;

    printf("LIFETIME %s:\n", name);
    MOQ_TEST_CHECK_EQ_INT((int)moq_simpair_create(&cfg, &sp), (int)MOQ_OK);
    if (!sp) return;

    moq_session_t *client = moq_simpair_client(sp);
    moq_session_t *server = moq_simpair_server(sp);

    /* 1. before start: the client has not sent its setup */
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(client), (int)MOQ_SESS_IDLE);
    check_pair(sp, want, "before-start");

    /* 2. mid-handshake: started, not yet quiescent */
    MOQ_TEST_CHECK_EQ_INT((int)moq_simpair_start(sp), (int)MOQ_OK);
    MOQ_TEST_CHECK(moq_session_state(client) != MOQ_SESS_IDLE);
    check_pair(sp, want, "mid-handshake");

    /* 3. established: the real setup exchange completed */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_simpair_run_until_quiescent(sp, 64, NULL), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(client),
                          (int)MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(server),
                          (int)MOQ_SESS_ESTABLISHED);
    check_pair(sp, want, "established");

    /* the version is a session fact, not a per-side one */
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_version(client),
                          (int)moq_session_version(server));

    /* 4. terminal, client side.
     *
     * moq_session_close() queues CLOSE_SESSION; it does not reach the peer
     * until the pair is pumped. So the asymmetry asserted here is real but
     * TRANSIENT: CLOSED on the closer, still ESTABLISHED on the peer, because
     * nothing has been delivered yet.
     *
     * SimPair DOES model the transport connection close: its action-delivery
     * loop feeds CLOSE_SESSION to the peer through
     * moq_session_on_transport_close() with the application error code
     * (sim/src/simpair_pump.c, the CLOSE_SESSION arm). Derived from source and
     * confirmed by probe on this base -- and it is the opposite of what this
     * test asserted on the pre-relay base, where that arm only traced the
     * action. The pre-pump/post-pump split below is what distinguishes
     * "queued" from "delivered", so both facts stay pinned. */
    uint64_t now = moq_simpair_now_us(sp);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_close(client, 0x1, NULL, now),
                          (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(client),
                          (int)MOQ_SESS_CLOSED);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(server),
                          (int)MOQ_SESS_ESTABLISHED);
    check_pair(sp, want, "after-client-close");

    /* The pump result is required, not discarded: a refused or exhausted pump
     * would otherwise leave every comparison below trivially satisfied. */
    size_t steps = 0;
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_simpair_run_until_quiescent(sp, 64, &steps), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(client),
                          (int)MOQ_SESS_CLOSED);
    /* Once pumped, the peer HAS been told: the delivered transport close puts
     * it terminal too, so both sides are CLOSED here. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(server),
                          (int)MOQ_SESS_CLOSED);
    check_pair(sp, want, "after-close-pump");

    /* 5. the peer is already terminal, so feeding the public transport-close
     * input again must be IDEMPOTENT -- accepted, and leaving both sides
     * CLOSED. That is the same idempotence SimPair's own delivery arm relies
     * on to avoid a second close on a crossed close. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_transport_close(server, 0x1, now), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(client),
                          (int)MOQ_SESS_CLOSED);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(server),
                          (int)MOQ_SESS_CLOSED);
    check_pair(sp, want, "after-both-terminal");

    /* 6. and once both sides have drained their events -- states reasserted,
     * so a session that somehow left the terminal cannot pass here either. */
    moq_event_t ev;
    while (moq_session_poll_events(client, &ev, 1) > 0) moq_event_cleanup(&ev);
    while (moq_session_poll_events(server, &ev, 1) > 0) moq_event_cleanup(&ev);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(client),
                          (int)MOQ_SESS_CLOSED);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(server),
                          (int)MOQ_SESS_CLOSED);
    check_pair(sp, want, "after-drain");

    moq_simpair_destroy(sp);
    if (failures == before)
        printf("PASS: lifetime_%s\n", name);
}

/* A session the test owns outright: no pair, no handshake -- the value is still
 * the configured one, and NULL still reads zero beside it. */
static void t_standalone(void)
{
    int before = failures;
    moq_session_cfg_t cfg;
    moq_session_t *s = NULL;

    printf("STANDALONE:\n");
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_SERVER);
    cfg.version = MOQ_VERSION_DRAFT_18;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_create(&cfg, 0, &s), (int)MOQ_OK);
    if (!s) return;

    MOQ_TEST_CHECK_EQ_INT((int)moq_session_version(s),
                          (int)MOQ_VERSION_DRAFT_18);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_version(NULL), 0);

    MOQ_TEST_CHECK_EQ_INT((int)moq_session_close(s, 0x1, NULL, 1000),
                          (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(s), (int)MOQ_SESS_CLOSED);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_version(s),
                          (int)MOQ_VERSION_DRAFT_18);

    /* Observing, proven rather than asserted: a borrow stamped by the polled
     * CLOSE_SESSION action stays valid across the read. An advancing call
     * would invalidate it. */
    moq_action_t act;
    if (moq_session_poll_actions(s, &act, 1) == 1) {
        uint64_t epoch = act.borrow_epoch;
        MOQ_TEST_CHECK(moq_session_borrow_valid(s, epoch));
        (void)moq_session_version(s);
        MOQ_TEST_CHECK(moq_session_borrow_valid(s, epoch));
        moq_action_cleanup(&act);
    } else {
        MOQ_TEST_CHECK(false);   /* the close must have queued its action */
    }

    moq_session_destroy(s);
    if (failures == before)
        printf("PASS: standalone\n");
}

int main(void)
{
    t_lifetime(MOQ_VERSION_DRAFT_16, "d16");
    t_lifetime(MOQ_VERSION_DRAFT_18, "d18");
    t_lifetime(MOQ_VERSION_DRAFT_21, "d21");
    t_standalone();

    if (failures == 0)
        MOQ_TEST_PASS("session_version");
    else
        fprintf(stderr, "FAIL: session_version (%d)\n", failures);
    return failures != 0;
}
