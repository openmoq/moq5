/*
 * Managed reaper scheduling: a child is not reclaimed while a pump is OWED.
 *
 * doorbell_reap() frees one child with the lane UNLOCKED (the blocking
 * ConnectionClose must not hold the lane), then relocks and looks for the next
 * victim. A worker batch landing in that window arms a coalesced pump AND can
 * mark another child reapable. Reclaiming that child in the same pass destroys
 * its moq_session_t with the armed pump still unrun, so a holder (a relay bind,
 * say) never gets the pump it was owed before the pointer went away.
 *
 * The reaper must therefore YIELD whenever lane work is armed in that window and
 * let the doorbell run the pump first; the next pass reaps.
 *
 * SCOPE: this pins the SCHEDULING rule — a pump OPPORTUNITY precedes further
 * batch reclamation, i.e. the ORDER of work within a reap pass. It is not the
 * lifetime contract: whether a child may be reclaimed at all is decided by the
 * application's acknowledgment, which this test's server pump gives greedily in
 * the same pump that consumed the terminal, and which
 * test_msquic_terminal_ack.c is what actually pins. The closed-count below is
 * fixture validation (the rig really did drive terminals).
 *
 * The race is made deterministic with the white-box reap-gap seam: every reap
 * window arms a pump exactly as a worker batch's guard_leave would, so the yield
 * is exercised on every reclamation. A second window firing while that arm is
 * still outstanding means a child was reclaimed with a pump owed — the defect.
 *
 * Real MsQuic loopback; the per-call waits are hang guards, never timing
 * assertions. Compiled with MOQ_MSQUIC_TESTING (seams never ship).
 *
 * Usage: test_msquic_reap <cert.pem> <key.pem>
 */
#if defined(__linux__) && !defined(_POSIX_C_SOURCE)
#define _POSIX_C_SOURCE 200809L
#endif

#include <moq/msquic_managed.h>

#include <msquic.h>

#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

static int failures;
#define CHECK(c)                                                            \
    do {                                                                    \
        if (!(c)) {                                                         \
            fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #c);    \
            failures++;                                                     \
        }                                                                   \
    } while (0)

enum { NCLIENTS = 3 };

/* --- white-box seams (MOQ_MSQUIC_TESTING build only) ---------------------- */
extern void (*moq_msq_test_reap_gap)(moq_msquic_managed_lane_t *lane);
extern void moq_msq_test_lane_arm_pump(moq_msquic_managed_lane_t *lane);

static atomic_int g_gap_fires;         /* reap windows observed              */
static atomic_bool g_arm_outstanding;  /* a pump is armed and has not run    */
static atomic_bool g_reap_while_armed; /* VIOLATION: reclaimed with pump owed */

static void reap_gap_hook(moq_msquic_managed_lane_t *lane)
{
    /* Reaching this window again while an arm is outstanding means the reaper
     * freed another child after a pump was armed and before it ran. */
    if (atomic_load(&g_arm_outstanding))
        atomic_store(&g_reap_while_armed, true);
    atomic_fetch_add(&g_gap_fires, 1);
    atomic_store(&g_arm_outstanding, true);
    moq_msq_test_lane_arm_pump(lane); /* what the worker batch would do */
}

/* --- server: count terminal observations ---------------------------------- */

struct srv_obs {
    atomic_int closed_seen;   /* MOQ_EVENT_SESSION_CLOSED polled by the app */
    atomic_int setup_seen;
};

static int server_pump(moq_msquic_managed_t *m,
                       moq_msquic_managed_lane_t *lane, uint64_t now,
                       void *user)
{
    struct srv_obs *sv = user;
    moq_msquic_managed_conn_t *c = NULL;

    (void)m;
    (void)now;
    /* a pump ran: the arm (if any) has been satisfied */
    atomic_store(&g_arm_outstanding, false);
    while ((c = moq_msquic_lane_next_conn(lane, c)) != NULL) {
        moq_session_t *s = moq_msquic_managed_conn_session(c);
        moq_event_t ev;
        bool closed = false;

        if (s == NULL)
            continue;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SETUP_COMPLETE)
                atomic_fetch_add(&sv->setup_seen, 1);
            else if (ev.kind == MOQ_EVENT_SESSION_CLOSED) {
                atomic_fetch_add(&sv->closed_seen, 1);
                closed = true;
            }
            moq_event_cleanup(&ev);
        }
        /* A greedy consumer: it has just taken this child's terminal, so it
         * releases the child in the same pump. Reclamation waits for this. */
        if (closed)
            (void)moq_msquic_managed_conn_ack_terminal(c);
    }
    return 0;
}

static int client_pump(moq_msquic_managed_t *m,
                       moq_msquic_managed_lane_t *lane, uint64_t now,
                       void *user)
{
    moq_session_t *s = moq_msquic_managed_session(m);
    moq_event_t ev;

    (void)lane;
    (void)now;
    (void)user;
    while (s != NULL && moq_session_poll_events(s, &ev, 1) > 0)
        moq_event_cleanup(&ev);
    return 0;
}

static void t_reap_yields_to_armed_pump(const char *cert, const char *key)
{
    int before = failures;
    struct srv_obs sv;

    memset(&sv, 0, sizeof(sv));
    atomic_store(&g_gap_fires, 0);
    atomic_store(&g_arm_outstanding, false);
    atomic_store(&g_reap_while_armed, false);

    /* ONE lane: every child shares a reap pass, so a pass can hold several
     * victims and the unlock window between them is the case under test. */
    moq_msquic_managed_cfg_t scfg;
    moq_msquic_managed_cfg_init_sized(&scfg, sizeof(scfg));
    scfg.alloc = moq_alloc_default();
    scfg.perspective = MOQ_PERSPECTIVE_SERVER;
    scfg.host = "127.0.0.1";
    scfg.port = 0;
    scfg.cert_path = cert;
    scfg.key_path = key;
    scfg.max_events = 64;
    scfg.max_connections = NCLIENTS + 1;
    scfg.lane_count = 1;
    scfg.on_lane_pump = server_pump;
    scfg.on_lane_pump_user = &sv;

    moq_msquic_managed_t *srv = NULL;
    CHECK(moq_msquic_managed_create(&scfg, &srv) == MOQ_OK);
    if (srv == NULL)
        return;
    uint16_t port = moq_msquic_managed_port(srv);
    CHECK(port != 0);

    moq_msquic_managed_t *cli[NCLIENTS];
    memset(cli, 0, sizeof(cli));
    for (int i = 0; i < NCLIENTS; i++) {
        moq_msquic_managed_cfg_t ccfg;
        moq_msquic_managed_cfg_init_sized(&ccfg, sizeof(ccfg));
        ccfg.alloc = moq_alloc_default();
        ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        ccfg.host = "127.0.0.1";
        ccfg.port = port;
        ccfg.insecure_skip_verify = true;
        ccfg.max_events = 64;
        ccfg.on_lane_pump = client_pump;
        CHECK(moq_msquic_managed_create(&ccfg, &cli[i]) == MOQ_OK);
    }

    /* all children established (each client's SETUP observed server-side) */
    bool up = false;
    for (int i = 0; i < 40000 && !up; i++) {
        (void)moq_msquic_managed_wake(srv);
        for (int j = 0; j < NCLIENTS; j++)
            if (cli[j] != NULL)
                (void)moq_msquic_managed_wake(cli[j]);
        (void)moq_msquic_managed_wait(srv, 2 * 1000);
        up = atomic_load(&sv.setup_seen) >= NCLIENTS;
    }
    CHECK(up);

    /* Arm the injection only now: the window under test is reclamation, and
     * every reap window from here arms a pump. */
    moq_msq_test_reap_gap = reap_gap_hook;

    /* Drop every client at once so the children go terminal together and a
     * single reap pass has more than one candidate. */
    for (int i = 0; i < NCLIENTS; i++) {
        if (cli[i] != NULL) {
            (void)moq_msquic_managed_stop(cli[i]);
            moq_msquic_managed_destroy(cli[i]);
            cli[i] = NULL;
        }
    }

    /* Let the server observe the terminals and reclaim. Bounded by iterations
     * (a hang guard), and the assertions below are on observations, not time. */
    bool reaped = false;
    for (int i = 0; i < 40000 && !reaped; i++) {
        (void)moq_msquic_managed_wake(srv);
        (void)moq_msquic_managed_wait(srv, 2 * 1000);
        reaped = moq_msquic_managed_conn_count(srv) == 0 &&
                 atomic_load(&sv.closed_seen) >= NCLIENTS;
    }

    bool srv_fatal = moq_msquic_managed_is_fatal(srv);
    (void)moq_msquic_managed_stop(srv);
    moq_msquic_managed_destroy(srv);
    moq_msq_test_reap_gap = NULL;

    CHECK(reaped);                                  /* all children reclaimed */
    /* Fixture validation: the rig really did drive every child to a terminal the
     * pump could see (not a claim about the general lifetime contract). */
    CHECK(atomic_load(&sv.closed_seen) == NCLIENTS);
    /* The injected window actually fired (the pin exercised reclamation) ... */
    CHECK(atomic_load(&g_gap_fires) > 0);
    /* ... and no child was reclaimed while a pump was owed. */
    CHECK(!atomic_load(&g_reap_while_armed));
    CHECK(!srv_fatal);

    printf("REAP: children=%d closes_polled=%d reap_windows=%d "
           "reclaimed_with_pump_owed=%d\n",
           NCLIENTS, atomic_load(&sv.closed_seen), atomic_load(&g_gap_fires),
           (int)atomic_load(&g_reap_while_armed));

    if (failures == before)
        printf("PASS: reap_yields_to_armed_pump\n");
}

int main(int argc, char **argv)
{
    if (argc < 3) {
        fprintf(stderr, "usage: %s <cert.pem> <key.pem>\n", argv[0]);
        return 2;
    }
    static const QUIC_API_TABLE *lib_pin;

    if (QUIC_FAILED(MsQuicOpen2(&lib_pin))) {
        fprintf(stderr, "MsQuicOpen2 failed\n");
        return 2;
    }

    t_reap_yields_to_armed_pump(argv[1], argv[2]);

    if (failures == 0)
        printf("PASS: msquic_reap\n");
    else
        fprintf(stderr, "FAIL: msquic_reap (%d)\n", failures);
    return failures;
}
