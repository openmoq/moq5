/*
 * The two SERVER terminal shapes of the raw threaded adapter, in one fixture.
 *
 * Both rows drive a real server and a real client over loopback, use bounded
 * progress conditions only (no sleeps, no timing assumptions), and inject no
 * synthetic callbacks. They differ ONLY in the shape of the peer terminal --
 * and, as measured, they differ in outcome too, which is the whole point: a
 * fix that discriminates on the callback KIND alone would be wrong.
 *
 * The row is selected by argv[1]:
 *
 *   peer_terminal  The client is stopped before it requests an application
 *                  close. This is a peer-terminal ownership row, not a
 *                  stop-close oracle; after #17 stop() attempts a bounded
 *                  CONNECTION_CLOSE, while the short idle timeout remains a
 *                  fallback if no close lands. picoquic reaches
 *                  picoquic_state_disconnected, fires picoquic_callback_close,
 *                  and -- because this is a SERVER context -- DELETES the
 *                  picoquic_cnx_t (sender.c picoquic_prepare_next_packet_ex ->
 *                  picoquic_delete_cnx; client contexts are deliberately
 *                  retained instead). LibMoQ prunes its own record afterwards
 *                  and the raw destructor writes the callback field through
 *                  the freed cnx.
 *
 *                  Under ASan this row aborts in server_prune ->
 *                  server_conn_free -> moq_pq_conn_destroy ->
 *                  picoquic_set_callback. Without a sanitizer the same
 *                  sequence completes and every assertion passes -- which is
 *                  why this needs a sanitizer lane to be visible at all.
 *
 *   app_close      CONTROL, and it PASSES today, ASan included. The client
 *                  closes its MoQ session with an application error code from
 *                  its OWN on_lane_pump; the bridge dispatches that to
 *                  picoquic_close(). The server is by then at
 *                  picoquic_state_ready -- the row waits for its MoQ session
 *                  to reach ESTABLISHED, which cannot happen before the QUIC
 *                  handshake does -- so frames.c takes the APPLICATION_CLOSE
 *                  arm: cnx_state becomes picoquic_state_closing_received (NOT
 *                  disconnected) and picoquic_callback_application_close is
 *                  delivered while the cnx is STILL LIVE. Only a LATER
 *                  picoquic_delete_cnx -> picoquic_connection_disconnect would
 *                  emit picoquic_callback_close at
 *                  picoquic_state_disconnected.
 *
 *                  Measured consequence: on this shape LibMoQ prunes and
 *                  unbinds while the cnx is still valid, so there is no
 *                  use-after-free. That makes the row a standing obligation on
 *                  the fix rather than a defect: relinquishing ownership on
 *                  picoquic_callback_application_close would leave picoquic
 *                  holding a callback into freed LibMoQ memory here, which
 *                  ASan would report as a use-after-free in the OTHER
 *                  direction (picoquic reading memory this adapter freed).
 *                  ASan cleanliness is therefore this row's real oracle; the
 *                  application-code assertion below is what proves the
 *                  APPLICATION_CLOSE arm actually ran, since a transport-level
 *                  terminal surfaces code 0 instead.
 */

#include <moq/picoquic_threaded.h>
#include <moq/session.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int failures = 0;

#define CHECK(expr)                                                     \
    do {                                                                \
        if (!(expr)) {                                                  \
            fprintf(stderr, "FAIL: %s:%d: %s\n",                        \
                    __FILE__, __LINE__, #expr);                         \
            failures++;                                                 \
        }                                                               \
    } while (0)

#ifndef MOQ_TEST_CERT_PATH
int main(int argc, char **argv)
{
    (void)argc; (void)argv;
    printf("SKIP: threaded_server_terminal_uaf (no test certificates)\n");
    return 0;
}
#else

/* The application error code the app_close row asks the client to send. It is
 * carried end to end: the server's MOQ_EVENT_SESSION_CLOSED must report it,
 * which is what proves the APPLICATION_CLOSE arm ran (a transport-level close
 * surfaces a different code). */
#define ROW_APP_CLOSE_CODE  0x4d6f5142u   /* 'MoQB' */

/* The build defines MOQ_TEST_CERT_PATH only when the file EXISTED AT CONFIGURE
 * TIME. These dependency trees live under /tmp, which is reaped, so a stale
 * configure can leave the macro defined and pointing at nothing -- and every
 * server row then fails on an opaque "create != MOQ_OK" with no hint why.
 * Say so instead. Returns 0 when both files are readable. */
static int check_test_certs(void)
{
    const char *paths[2] = { MOQ_TEST_CERT_PATH, MOQ_TEST_KEY_PATH };
    int missing = 0;
    for (int i = 0; i < 2; i++) {
        FILE *f = fopen(paths[i], "rb");
        if (f) { fclose(f); continue; }
        fprintf(stderr,
                "FAIL: test certificate not readable: %s\n"
                "      MOQ_TEST_CERT_PATH was resolved at CMake configure time;"
                " re-run the dependency setup and reconfigure.\n",
                paths[i]);
        missing++;
    }
    return missing;
}

/* Server ports: ASK THE KERNEL, never a fixed range.
 *
 * These rows previously derived a port from an UNSEEDED rand(), which is fully
 * deterministic -- every run of this binary, and every ctest row that runs it,
 * picked the SAME port. Sequential rows therefore contended for one port, and
 * two checkouts running their suites at once collided every time, producing
 * false startup failures that look like product signal.
 *
 * pick_free_udp_port() binds an ephemeral loopback UDP socket, reads back what
 * the kernel assigned, and releases it. There is a small window between that
 * release and the server's own bind, so create_server_on_free_port() retries on
 * a fresh port; a create failure is only reported after every attempt fails,
 * which keeps the assertion meaningful. */
static int pick_free_udp_port(void)
{
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0)
        return 0;
    struct sockaddr_in a;
    memset(&a, 0, sizeof(a));
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    a.sin_port = 0;
    int port = 0;
    if (bind(fd, (struct sockaddr *)&a, sizeof(a)) == 0) {
        socklen_t len = sizeof(a);
        if (getsockname(fd, (struct sockaddr *)&a, &len) == 0)
            port = (int)ntohs(a.sin_port);
    }
    close(fd);
    return port;
}

/* Create the server on a kernel-chosen free port, retrying a bounded number of
 * times so a lost race does not read as a product failure. Returns the create
 * result of the last attempt; *out_port carries the port actually bound. */
static moq_result_t create_server_on_free_port(moq_pq_threaded_cfg_t *cfg,
                                               moq_pq_threaded_t **out,
                                               int *out_port)
{
    moq_result_t rc = MOQ_ERR_INTERNAL;
    for (int attempt = 0; attempt < 16; attempt++) {
        int port = pick_free_udp_port();
        if (port == 0)
            continue;
        cfg->port = port;
        *out = NULL;
        rc = moq_pq_threaded_create(cfg, out);
        if (rc == MOQ_OK) {
            *out_port = port;
            return rc;
        }
    }
    return rc;
}

static int dummy_pump(moq_pq_threaded_t *t, moq_pq_threaded_lane_t *lane,
                      uint64_t now, void *ctx)
{
    (void)t; (void)lane; (void)now; (void)ctx;
    return 0;
}

/* Client pump for the app_close row. Once the app thread arms it (after the
 * server has accepted, so the QUIC handshake is complete and the server cnx is
 * past picoquic_state_client_ready_start), it closes the MoQ session ONCE with
 * an application code. The bridge turns that into a CLOSE_SESSION action, and
 * the moq_pq_service() call that immediately follows on_lane_pump dispatches it
 * to picoquic_close() -- a real APPLICATION_CLOSE on the wire. */
typedef struct {
    int armed;
    int fired;
} client_close_ctx_t;

static int client_close_pump(moq_pq_threaded_t *t,
                             moq_pq_threaded_lane_t *lane,
                             uint64_t now, void *vctx)
{
    (void)lane;
    client_close_ctx_t *cc = (client_close_ctx_t *)vctx;
    if (cc->fired)
        return 0;
    if (!__atomic_load_n(&cc->armed, __ATOMIC_ACQUIRE))
        return 0;
    moq_session_t *s = moq_pq_threaded_session(t);
    if (!s)
        return 0;
    if (moq_session_close(s, ROW_APP_CLOSE_CODE, NULL, now) == MOQ_OK)
        cc->fired = 1;
    return 0;
}

/* Tracks ONE accepted connection through the app's eyes: polls its session
 * only through live lane iteration (never a stale pointer), records whether
 * MOQ_EVENT_SESSION_CLOSED was observed before the conn vanished, and trips on
 * pump re-entrancy. */
typedef struct {
    moq_pq_threaded_conn_t *conn;           /* network-thread only */
    int accepted;
    int established;
    int saw_session_closed;
    int vanished_before_observed;
    int vanished_after_observed;
    int in_pump;
    int reentered;
    uint64_t closed_code;                   /* code of the first SESSION_CLOSED */
    int closed_code_valid;
} lifecycle_ctx_t;

static int lifecycle_pump(moq_pq_threaded_t *t, moq_pq_threaded_lane_t *lane,
                          uint64_t now, void *vctx)
{
    (void)t; (void)now;
    lifecycle_ctx_t *lc = (lifecycle_ctx_t *)vctx;
    if (__atomic_exchange_n(&lc->in_pump, 1, __ATOMIC_ACQ_REL))
        __atomic_store_n(&lc->reentered, 1, __ATOMIC_RELEASE);

    int present = 0;
    moq_pq_threaded_conn_t *c = NULL;
    while ((c = moq_pq_threaded_lane_next_conn(lane, c)) != NULL) {
        if (!lc->conn) {
            lc->conn = c;
            __atomic_store_n(&lc->accepted, 1, __ATOMIC_RELEASE);
        }
        if (c != lc->conn)
            continue;
        present = 1;
        moq_session_t *sess = moq_pq_threaded_conn_session(c);
        if (!sess)
            continue;
        if (moq_session_state(sess) == MOQ_SESS_ESTABLISHED)
            __atomic_store_n(&lc->established, 1, __ATOMIC_RELEASE);
        moq_event_t ev[8];
        size_t ne = 0;
        moq_session_poll_events_ex(sess, ev, 8, sizeof(moq_event_t), &ne);
        for (size_t i = 0; i < ne; i++) {
            if (ev[i].kind != MOQ_EVENT_SESSION_CLOSED)
                continue;
            if (!__atomic_load_n(&lc->closed_code_valid, __ATOMIC_ACQUIRE)) {
                lc->closed_code = ev[i].u.closed.code;
                __atomic_store_n(&lc->closed_code_valid, 1, __ATOMIC_RELEASE);
            }
            __atomic_store_n(&lc->saw_session_closed, 1, __ATOMIC_RELEASE);
        }
    }
    if (lc->conn && !present) {
        if (__atomic_load_n(&lc->saw_session_closed, __ATOMIC_ACQUIRE))
            __atomic_store_n(&lc->vanished_after_observed, 1, __ATOMIC_RELEASE);
        else
            __atomic_store_n(&lc->vanished_before_observed, 1,
                             __ATOMIC_RELEASE);
        lc->conn = NULL;
    }
    __atomic_store_n(&lc->in_pump, 0, __ATOMIC_RELEASE);
    return 0;
}

int main(int argc, char **argv)
{
    /* Row selection. Default keeps the original single-row invocation valid. */
    const char *row = (argc > 1) ? argv[1] : "peer_terminal";
    int app_close_row = (strcmp(row, "app_close") == 0);
    if (!app_close_row && strcmp(row, "peer_terminal") != 0) {
        fprintf(stderr, "usage: %s [peer_terminal|app_close]\n", argv[0]);
        return 2;
    }

    if (check_test_certs() != 0) {
        failures++;
        printf("FAILED: %d check(s) [threaded_server_terminal_uaf %s]\n",
               failures, row);
        return 1;
    }

    int port = 0;
    lifecycle_ctx_t lc;
    memset(&lc, 0, sizeof(lc));
    client_close_ctx_t cc;
    memset(&cc, 0, sizeof(cc));

    moq_pq_threaded_cfg_t srv_cfg;
    moq_pq_threaded_cfg_init_sized(&srv_cfg, sizeof(srv_cfg));
    srv_cfg.alloc = moq_alloc_default();
    srv_cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    srv_cfg.cert_path = MOQ_TEST_CERT_PATH;
    srv_cfg.key_path = MOQ_TEST_KEY_PATH;
    srv_cfg.insecure_skip_verify = true;
    srv_cfg.on_lane_pump = lifecycle_pump;
    srv_cfg.on_lane_pump_ctx = &lc;
    /* peer_terminal: the client is stopped without asking its MoQ session for
     * an application close. stop() now attempts a bounded clean transport
     * close; the idle-timeout knob remains a fallback if no close lands, well
     * inside the bounded waits below. app_close does not depend on it -- the
     * peer's CONNECTION_CLOSE arrives immediately -- but the same bound is
     * harmless there. */
    srv_cfg.idle_timeout_ms = 1000;

    moq_pq_threaded_t *srv = NULL;
    moq_result_t src = create_server_on_free_port(&srv_cfg, &srv, &port);
    CHECK(src == MOQ_OK);
    CHECK(port != 0);

    if (src == MOQ_OK) {
        moq_pq_threaded_cfg_t cli_cfg;
        moq_pq_threaded_cfg_init_sized(&cli_cfg, sizeof(cli_cfg));
        cli_cfg.alloc = moq_alloc_default();
        cli_cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        cli_cfg.host = "localhost";
        cli_cfg.port = port;
        cli_cfg.insecure_skip_verify = true;
        cli_cfg.on_lane_pump = app_close_row ? client_close_pump : dummy_pump;
        cli_cfg.on_lane_pump_ctx = app_close_row ? (void *)&cc : NULL;
        moq_pq_threaded_t *cli = NULL;
        moq_result_t crc = moq_pq_threaded_create(&cli_cfg, &cli);
        CHECK(crc == MOQ_OK);

        if (crc == MOQ_OK) {
            for (int tries = 0; tries < 400 &&
                 !__atomic_load_n(&lc.accepted, __ATOMIC_ACQUIRE); tries++)
                moq_pq_threaded_wait(srv, 25000);
            CHECK(__atomic_load_n(&lc.accepted, __ATOMIC_ACQUIRE) == 1);

            if (app_close_row) {
                /* The APPLICATION_CLOSE arm at frames.c is selected only when
                 * the receiving cnx is already at or past
                 * picoquic_state_client_ready_start. Waiting for the server's
                 * MoQ session to reach ESTABLISHED is the public way to know
                 * that: the SETUP exchange cannot complete before the QUIC
                 * handshake does. Arming at accept time is too early -- the
                 * server is then still in a handshake state, takes the
                 * "disconnected" arm, and reports a transport terminal
                 * (code 0) instead. */
                for (int tries = 0; tries < 400 &&
                     !__atomic_load_n(&lc.established, __ATOMIC_ACQUIRE);
                     tries++)
                    moq_pq_threaded_wait(srv, 25000);
                CHECK(__atomic_load_n(&lc.established,
                                      __ATOMIC_ACQUIRE) == 1);

                /* Real, peer-driven APPLICATION_CLOSE. The client's own
                 * network thread issues it; the app thread only arms. */
                __atomic_store_n(&cc.armed, 1, __ATOMIC_RELEASE);
                moq_pq_threaded_wake(cli);
            } else {
                /* Real peer terminal from stopping the client facade. */
                moq_pq_threaded_stop(cli);
                moq_pq_threaded_destroy(cli);
                cli = NULL;
            }

            /* Bounded wait for the prune. Under ASan the process aborts
             * inside this window, in server_prune -> server_conn_free ->
             * moq_pq_conn_destroy -> picoquic_set_callback. */
            for (int tries = 0; tries < 400 &&
                 moq_pq_threaded_conn_count(srv) > 0; tries++)
                moq_pq_threaded_wait(srv, 25000);
            CHECK(moq_pq_threaded_conn_count(srv) == 0);

            /* The already-signed ordering contract: SESSION_CLOSED was polled
             * from an on_lane_pump with the conn still iterable, and no pump
             * ever saw the conn vanish unobserved. */
            CHECK(__atomic_load_n(&lc.saw_session_closed,
                                  __ATOMIC_ACQUIRE) == 1);
            CHECK(__atomic_load_n(&lc.vanished_before_observed,
                                  __ATOMIC_ACQUIRE) == 0);
            CHECK(__atomic_load_n(&lc.reentered, __ATOMIC_ACQUIRE) == 0);

            if (app_close_row) {
                /* The terminal carries the PEER'S APPLICATION error code.
                 * That is what proves the server took the APPLICATION_CLOSE
                 * arm while ready -- a transport-level terminal (the
                 * peer_terminal row) reports a different code entirely. */
                CHECK(__atomic_load_n(&lc.closed_code_valid,
                                      __ATOMIC_ACQUIRE) == 1);
                CHECK(lc.closed_code == ROW_APP_CLOSE_CODE);
                CHECK(cc.fired == 1);
            }

            if (cli) {
                moq_pq_threaded_stop(cli);
                moq_pq_threaded_destroy(cli);
            }
        }
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    if (failures) {
        printf("FAILED: %d check(s) [threaded_server_terminal_uaf %s]\n",
               failures, row);
        return 1;
    }
    printf("PASS: threaded_server_terminal_uaf %s\n", row);
    return 0;
}
#endif /* MOQ_TEST_CERT_PATH */
