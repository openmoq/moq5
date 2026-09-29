/*
 * Terminal-ownership CONTROLS for the raw picoquic adapters.
 *
 * These rows pass today and must keep passing. They exist so the eventual fix
 * for the threaded SERVER terminal use-after-free cannot be obtained by
 * weakening behaviour that is currently correct. The row is selected by
 * argv[1]:
 *
 *   client_unbind   RAW facade, caller-owned LIVE client picoquic_cnx_t, no
 *                   network at all. The public destructor must still unbind
 *                   the adapter from a live cnx the CALLER owns. Proven by
 *                   deleting that cnx afterwards: picoquic_delete_cnx() calls
 *                   picoquic_connection_disconnect() while cnx_state is below
 *                   picoquic_state_disconnected, which invokes cnx->callback_fn
 *                   if one is still installed. With the unbind, no call is
 *                   made. WITHOUT it -- a blanket "never unbind" change --
 *                   picoquic calls into the moq_pq_conn_t this test already
 *                   freed, which ASan reports as a heap-use-after-free.
 *                   ASan is therefore this row's oracle; without a sanitizer
 *                   the sequence completes and the assertions below pass.
 *
 *   local_close     THREADED server, real client, LIVE cnx. The server
 *                   application closes one accepted connection from inside its
 *                   own on_lane_pump via moq_pq_threaded_conn_close(). Nothing
 *                   about that connection is dead: picoquic has not deleted it
 *                   and no peer terminal arrived. The close must therefore
 *                   still reach the wire exactly once and the record must be
 *                   pruned. Observable oracle: the CLIENT sees exactly one
 *                   MOQ_EVENT_SESSION_CLOSED carrying the code the server
 *                   asked for, and the server's connection count returns to 0.
 *
 *                   Limitation, stated rather than papered over: "exactly one
 *                   picoquic_close()" is not directly countable through public
 *                   API. What is countable is that the peer observed exactly
 *                   one terminal with the requested code -- a duplicate close
 *                   on a already-closing cnx emits nothing, so this oracle
 *                   proves at least one and at most one OBSERVED close, not
 *                   the literal call count. Counting the call itself needs an
 *                   adapter-private test seam; see the report.
 *
 *   stop_close      THREADED client + server over real loopback. After the
 *                   server accepts the client, the APP thread calls
 *                   moq_pq_threaded_stop(cli). The client must send a bounded
 *                   local QUIC CONNECTION_CLOSE before its packet loop exits,
 *                   so the server promptly observes exactly one
 *                   MOQ_EVENT_SESSION_CLOSED with code 0 and prunes the
 *                   connection. Without this, the server retains the ghost
 *                   connection until idle timeout.
 *
 *   pump_exit_close Same as stop_close, except the client leaves by returning
 *                   nonzero from on_lane_pump. This prevents a stop-only fix
 *                   from leaving callback-requested loop termination as an
 *                   abrupt ghost-session path.
 *
 *   server_pump_exit_close
 *                   A threaded server with two accepted clients leaves by
 *                   returning nonzero from on_lane_pump. Both clients must
 *                   observe one clean close, proving the server pump-exit path
 *                   closes every owned connection, not only the first.
 */

#include <moq/picoquic.h>
#include <moq/picoquic_threaded.h>
#include <moq/session.h>
#include <picoquic.h>
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

#define ROW_LOCAL_CLOSE_CODE  0x4d6f5143u   /* 'MoQC' */

/* ---- client_unbind ------------------------------------------------ */

static int row_client_unbind(void)
{
    /* A real client picoquic context and a real client connection. No socket
     * is bound and no packet is ever driven -- the row is about ownership of
     * the callback binding, not about traffic. */
    picoquic_quic_t *quic = picoquic_create(4, NULL, NULL, NULL,
                                            MOQ_PQ_ALPN_DEFAULT,
                                            NULL, NULL, NULL, NULL, NULL,
                                            0, NULL, NULL, NULL, 0);
    CHECK(quic != NULL);
    if (!quic)
        return failures;

    struct sockaddr_in peer;
    memset(&peer, 0, sizeof(peer));
    peer.sin_family = AF_INET;
    peer.sin_port = htons(4433);
    peer.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    picoquic_cnx_t *cnx = picoquic_create_client_cnx(
        quic, (struct sockaddr *)&peer, 0, 0, "localhost",
        MOQ_PQ_ALPN_DEFAULT, NULL, NULL);
    CHECK(cnx != NULL);
    if (!cnx) {
        picoquic_free(quic);
        return failures;
    }

    /* The cnx is LIVE: picoquic has not disconnected it, so a callback
     * installed on it would still be invoked. */
    CHECK(picoquic_get_cnx_state(cnx) < picoquic_state_disconnected);
    CHECK(picoquic_is_client(cnx));

    moq_session_cfg_t scfg;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_CLIENT);
    moq_session_t *sess = NULL;
    CHECK(moq_session_create(&scfg, 0, &sess) == MOQ_OK);

    moq_pq_conn_cfg_t ccfg;
    moq_pq_conn_cfg_init_sized(&ccfg, sizeof(ccfg));
    ccfg.session = sess;
    ccfg.cnx = cnx;
    ccfg.alloc = moq_alloc_default();

    moq_pq_conn_t *conn = NULL;
    CHECK(moq_pq_conn_create(&ccfg, &conn) == 0);
    CHECK(conn != NULL);

    /* The public raw destructor on a LIVE, caller-owned cnx. It must unbind. */
    if (conn)
        moq_pq_conn_destroy(conn);

    /* Now make picoquic exercise the binding. delete_cnx sees cnx_state below
     * picoquic_state_disconnected and therefore runs
     * picoquic_connection_disconnect(), which calls cnx->callback_fn when one
     * is installed. If the destructor skipped the unbind, this call lands in
     * the freed moq_pq_conn_t -- a heap-use-after-free under ASan. */
    picoquic_delete_cnx(cnx);

    moq_session_destroy(sess);
    picoquic_free(quic);
    return failures;
}

/* ---- local_close --------------------------------------------------- */

#ifdef MOQ_TEST_CERT_PATH

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

/* Server ports: ASK THE KERNEL, never a fixed range. An UNSEEDED rand() is
 * fully deterministic, so the previous form made every run of this binary bind
 * the same port -- colliding with its own sibling rows and with any concurrent
 * checkout, which surfaces as a false startup failure rather than as product
 * signal. See the same helper in test_threaded_server_terminal_uaf.c. */
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

typedef struct {
    int accepted;
    int armed;
    int close_requested;
    moq_result_t close_rc;
    moq_pq_threaded_conn_t *conn;
} srv_close_ctx_t;

static int srv_close_pump(moq_pq_threaded_t *t, moq_pq_threaded_lane_t *lane,
                          uint64_t now, void *vctx)
{
    (void)t; (void)now;
    srv_close_ctx_t *sc = (srv_close_ctx_t *)vctx;

    moq_pq_threaded_conn_t *c = NULL;
    while ((c = moq_pq_threaded_lane_next_conn(lane, c)) != NULL) {
        if (!sc->conn) {
            sc->conn = c;
            __atomic_store_n(&sc->accepted, 1, __ATOMIC_RELEASE);
        }
        if (c != sc->conn)
            continue;
        /* Drain the session so the pump behaves like a real application. */
        moq_session_t *sess = moq_pq_threaded_conn_session(c);
        if (sess) {
            moq_event_t ev[8];
            size_t ne = 0;
            moq_session_poll_events_ex(sess, ev, 8, sizeof(moq_event_t), &ne);
        }
        if (__atomic_load_n(&sc->armed, __ATOMIC_ACQUIRE) &&
            !sc->close_requested) {
            /* A LOCAL close of a connection whose cnx is live: no peer
             * terminal, no picoquic deletion. */
            sc->close_rc = moq_pq_threaded_conn_close(c, ROW_LOCAL_CLOSE_CODE);
            sc->close_requested = 1;
        }
    }
    return 0;
}

typedef struct {
    int closed_count;
    uint64_t closed_code;
    int closed_code_valid;
} cli_watch_ctx_t;

static int cli_watch_pump(moq_pq_threaded_t *t, moq_pq_threaded_lane_t *lane,
                          uint64_t now, void *vctx)
{
    (void)lane; (void)now;
    cli_watch_ctx_t *cw = (cli_watch_ctx_t *)vctx;
    moq_session_t *s = moq_pq_threaded_session(t);
    if (!s)
        return 0;
    moq_event_t ev[8];
    size_t ne = 0;
    moq_session_poll_events_ex(s, ev, 8, sizeof(moq_event_t), &ne);
    for (size_t i = 0; i < ne; i++) {
        if (ev[i].kind != MOQ_EVENT_SESSION_CLOSED)
            continue;
        if (!__atomic_load_n(&cw->closed_code_valid, __ATOMIC_ACQUIRE)) {
            cw->closed_code = ev[i].u.closed.code;
            __atomic_store_n(&cw->closed_code_valid, 1, __ATOMIC_RELEASE);
        }
        __atomic_add_fetch(&cw->closed_count, 1, __ATOMIC_ACQ_REL);
    }
    return 0;
}

static int row_local_close(void)
{
    if (check_test_certs() != 0) {
        failures++;
        return failures;
    }

    int port = 0;
    srv_close_ctx_t sc;
    memset(&sc, 0, sizeof(sc));
    cli_watch_ctx_t cw;
    memset(&cw, 0, sizeof(cw));

    moq_pq_threaded_cfg_t srv_cfg;
    moq_pq_threaded_cfg_init_sized(&srv_cfg, sizeof(srv_cfg));
    srv_cfg.alloc = moq_alloc_default();
    srv_cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    srv_cfg.cert_path = MOQ_TEST_CERT_PATH;
    srv_cfg.key_path = MOQ_TEST_KEY_PATH;
    srv_cfg.insecure_skip_verify = true;
    srv_cfg.on_lane_pump = srv_close_pump;
    srv_cfg.on_lane_pump_ctx = &sc;

    moq_pq_threaded_t *srv = NULL;
    CHECK(create_server_on_free_port(&srv_cfg, &srv, &port) == MOQ_OK);
    CHECK(port != 0);
    if (!srv)
        return failures;

    moq_pq_threaded_cfg_t cli_cfg;
    moq_pq_threaded_cfg_init_sized(&cli_cfg, sizeof(cli_cfg));
    cli_cfg.alloc = moq_alloc_default();
    cli_cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cli_cfg.host = "localhost";
    cli_cfg.port = port;
    cli_cfg.insecure_skip_verify = true;
    cli_cfg.on_lane_pump = cli_watch_pump;
    cli_cfg.on_lane_pump_ctx = &cw;

    moq_pq_threaded_t *cli = NULL;
    CHECK(moq_pq_threaded_create(&cli_cfg, &cli) == MOQ_OK);

    if (cli) {
        for (int tries = 0; tries < 400 &&
             !__atomic_load_n(&sc.accepted, __ATOMIC_ACQUIRE); tries++)
            moq_pq_threaded_wait(srv, 25000);
        CHECK(__atomic_load_n(&sc.accepted, __ATOMIC_ACQUIRE) == 1);

        __atomic_store_n(&sc.armed, 1, __ATOMIC_RELEASE);
        moq_pq_threaded_wake(srv);

        for (int tries = 0; tries < 400 &&
             moq_pq_threaded_conn_count(srv) > 0; tries++)
            moq_pq_threaded_wait(srv, 25000);
        CHECK(moq_pq_threaded_conn_count(srv) == 0);
        CHECK(sc.close_requested == 1);
        CHECK(sc.close_rc == MOQ_OK);

        /* The peer must observe the local close, exactly once, with the code
         * the server asked for. */
        for (int tries = 0; tries < 400 &&
             __atomic_load_n(&cw.closed_count, __ATOMIC_ACQUIRE) == 0; tries++)
            moq_pq_threaded_wait(cli, 25000);
        CHECK(__atomic_load_n(&cw.closed_count, __ATOMIC_ACQUIRE) == 1);
        CHECK(__atomic_load_n(&cw.closed_code_valid, __ATOMIC_ACQUIRE) == 1);
        CHECK(cw.closed_code == ROW_LOCAL_CLOSE_CODE);

        moq_pq_threaded_stop(cli);
        moq_pq_threaded_destroy(cli);
    }

    moq_pq_threaded_stop(srv);
    moq_pq_threaded_destroy(srv);
    return failures;
}

typedef struct {
    int accepted;
    int closed_count;
    uint64_t closed_code;
    int closed_code_valid;
} srv_watch_ctx_t;

static int srv_watch_pump(moq_pq_threaded_t *t, moq_pq_threaded_lane_t *lane,
                          uint64_t now, void *vctx)
{
    (void)t; (void)now;
    srv_watch_ctx_t *sw = (srv_watch_ctx_t *)vctx;
    moq_pq_threaded_conn_t *c = NULL;
    while ((c = moq_pq_threaded_lane_next_conn(lane, c)) != NULL) {
        __atomic_store_n(&sw->accepted, 1, __ATOMIC_RELEASE);
        moq_session_t *s = moq_pq_threaded_conn_session(c);
        if (!s)
            continue;
        moq_event_t ev[8];
        size_t ne = 0;
        moq_session_poll_events_ex(s, ev, 8, sizeof(moq_event_t), &ne);
        for (size_t i = 0; i < ne; i++) {
            if (ev[i].kind != MOQ_EVENT_SESSION_CLOSED)
                continue;
            if (!__atomic_load_n(&sw->closed_code_valid,
                                 __ATOMIC_ACQUIRE)) {
                sw->closed_code = ev[i].u.closed.code;
                __atomic_store_n(&sw->closed_code_valid, 1,
                                 __ATOMIC_RELEASE);
            }
            __atomic_add_fetch(&sw->closed_count, 1, __ATOMIC_ACQ_REL);
        }
    }
    return 0;
}

static int dummy_pump(moq_pq_threaded_t *t, moq_pq_threaded_lane_t *lane,
                      uint64_t now, void *ctx)
{
    (void)t; (void)lane; (void)now; (void)ctx;
    return 0;
}

typedef struct {
    int armed;
    int fired;
} cli_exit_ctx_t;

static int cli_exit_pump(moq_pq_threaded_t *t, moq_pq_threaded_lane_t *lane,
                         uint64_t now, void *vctx)
{
    (void)t; (void)lane; (void)now;
    cli_exit_ctx_t *ce = (cli_exit_ctx_t *)vctx;
    if (__atomic_load_n(&ce->armed, __ATOMIC_ACQUIRE)) {
        __atomic_store_n(&ce->fired, 1, __ATOMIC_RELEASE);
        return 1;
    }
    return 0;
}

static int row_stop_or_pump_exit_close(int pump_exit_row)
{
    if (check_test_certs() != 0) {
        failures++;
        return failures;
    }

    int port = 0;
    srv_watch_ctx_t sw;
    memset(&sw, 0, sizeof(sw));

    moq_pq_threaded_cfg_t srv_cfg;
    moq_pq_threaded_cfg_init_sized(&srv_cfg, sizeof(srv_cfg));
    srv_cfg.alloc = moq_alloc_default();
    srv_cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    srv_cfg.cert_path = MOQ_TEST_CERT_PATH;
    srv_cfg.key_path = MOQ_TEST_KEY_PATH;
    srv_cfg.insecure_skip_verify = true;
    srv_cfg.on_lane_pump = srv_watch_pump;
    srv_cfg.on_lane_pump_ctx = &sw;

    moq_pq_threaded_t *srv = NULL;
    CHECK(create_server_on_free_port(&srv_cfg, &srv, &port) == MOQ_OK);
    CHECK(port != 0);
    if (!srv)
        return failures;

    moq_pq_threaded_cfg_t cli_cfg;
    moq_pq_threaded_cfg_init_sized(&cli_cfg, sizeof(cli_cfg));
    cli_exit_ctx_t ce;
    memset(&ce, 0, sizeof(ce));
    cli_cfg.alloc = moq_alloc_default();
    cli_cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cli_cfg.host = "localhost";
    cli_cfg.port = port;
    cli_cfg.insecure_skip_verify = true;
    cli_cfg.on_lane_pump = pump_exit_row ? cli_exit_pump : dummy_pump;
    cli_cfg.on_lane_pump_ctx = pump_exit_row ? (void *)&ce : NULL;

    moq_pq_threaded_t *cli = NULL;
    CHECK(moq_pq_threaded_create(&cli_cfg, &cli) == MOQ_OK);

    if (cli) {
        for (int tries = 0; tries < 400 &&
             !__atomic_load_n(&sw.accepted, __ATOMIC_ACQUIRE); tries++)
            moq_pq_threaded_wait(srv, 25000);
        CHECK(__atomic_load_n(&sw.accepted, __ATOMIC_ACQUIRE) == 1);
        CHECK(moq_pq_threaded_conn_count(srv) == 1);

        if (pump_exit_row) {
            __atomic_store_n(&ce.armed, 1, __ATOMIC_RELEASE);
            CHECK(moq_pq_threaded_wake(cli) == MOQ_OK);
            for (int tries = 0; tries < 400 &&
                 !__atomic_load_n(&ce.fired, __ATOMIC_ACQUIRE); tries++)
                moq_pq_threaded_wait(cli, 25000);
            CHECK(__atomic_load_n(&ce.fired, __ATOMIC_ACQUIRE) == 1);
            CHECK(moq_pq_threaded_wait(cli, 0) == MOQ_ERR_CLOSED);
            CHECK(moq_pq_threaded_stop(cli) == MOQ_OK);
        } else {
            CHECK(moq_pq_threaded_stop(cli) == MOQ_OK);
        }
        moq_pq_threaded_destroy(cli);

        for (int tries = 0; tries < 400 &&
             (__atomic_load_n(&sw.closed_count, __ATOMIC_ACQUIRE) == 0 ||
              moq_pq_threaded_conn_count(srv) > 0); tries++)
            moq_pq_threaded_wait(srv, 25000);

        CHECK(__atomic_load_n(&sw.closed_count, __ATOMIC_ACQUIRE) == 1);
        CHECK(__atomic_load_n(&sw.closed_code_valid, __ATOMIC_ACQUIRE) == 1);
        CHECK(sw.closed_code == 0);
        CHECK(moq_pq_threaded_conn_count(srv) == 0);
        CHECK(!moq_pq_threaded_is_fatal(srv));
    }

    moq_pq_threaded_stop(srv);
    moq_pq_threaded_destroy(srv);
    return failures;
}

static int row_stop_close(void)
{
    return row_stop_or_pump_exit_close(0);
}

static int row_pump_exit_close(void)
{
    return row_stop_or_pump_exit_close(1);
}

typedef struct {
    int accepted_count;
    int armed;
    int fired;
} srv_exit_ctx_t;

static int srv_exit_pump(moq_pq_threaded_t *t, moq_pq_threaded_lane_t *lane,
                         uint64_t now, void *vctx)
{
    (void)t; (void)now;
    srv_exit_ctx_t *se = (srv_exit_ctx_t *)vctx;
    int count = 0;
    moq_pq_threaded_conn_t *c = NULL;
    while ((c = moq_pq_threaded_lane_next_conn(lane, c)) != NULL) {
        count++;
        moq_session_t *s = moq_pq_threaded_conn_session(c);
        if (s) {
            moq_event_t ev[8];
            size_t ne = 0;
            moq_session_poll_events_ex(s, ev, 8, sizeof(moq_event_t), &ne);
        }
    }
    int prev = __atomic_load_n(&se->accepted_count, __ATOMIC_ACQUIRE);
    while (count > prev &&
           !__atomic_compare_exchange_n(&se->accepted_count, &prev, count,
                                        false, __ATOMIC_ACQ_REL,
                                        __ATOMIC_ACQUIRE)) {
    }
    if (count >= 2 && __atomic_load_n(&se->armed, __ATOMIC_ACQUIRE)) {
        __atomic_store_n(&se->fired, 1, __ATOMIC_RELEASE);
        return 1;
    }
    return 0;
}

static int create_client_watch(int port, cli_watch_ctx_t *cw,
                               moq_pq_threaded_t **out)
{
    moq_pq_threaded_cfg_t cfg;
    moq_pq_threaded_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = "localhost";
    cfg.port = port;
    cfg.insecure_skip_verify = true;
    cfg.on_lane_pump = cli_watch_pump;
    cfg.on_lane_pump_ctx = cw;
    return moq_pq_threaded_create(&cfg, out);
}

static int row_server_pump_exit_close(void)
{
    if (check_test_certs() != 0) {
        failures++;
        return failures;
    }

    int port = 0;
    srv_exit_ctx_t se;
    memset(&se, 0, sizeof(se));
    cli_watch_ctx_t cwa, cwb;
    memset(&cwa, 0, sizeof(cwa));
    memset(&cwb, 0, sizeof(cwb));

    moq_pq_threaded_cfg_t srv_cfg;
    moq_pq_threaded_cfg_init_sized(&srv_cfg, sizeof(srv_cfg));
    srv_cfg.alloc = moq_alloc_default();
    srv_cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    srv_cfg.cert_path = MOQ_TEST_CERT_PATH;
    srv_cfg.key_path = MOQ_TEST_KEY_PATH;
    srv_cfg.insecure_skip_verify = true;
    srv_cfg.on_lane_pump = srv_exit_pump;
    srv_cfg.on_lane_pump_ctx = &se;
    srv_cfg.max_connections = 2;

    moq_pq_threaded_t *srv = NULL;
    CHECK(create_server_on_free_port(&srv_cfg, &srv, &port) == MOQ_OK);
    CHECK(port != 0);
    if (!srv)
        return failures;

    moq_pq_threaded_t *a = NULL, *b = NULL;
    CHECK(create_client_watch(port, &cwa, &a) == MOQ_OK);
    CHECK(create_client_watch(port, &cwb, &b) == MOQ_OK);

    if (a && b) {
        for (int tries = 0; tries < 400 &&
             __atomic_load_n(&se.accepted_count, __ATOMIC_ACQUIRE) < 2;
             tries++)
            moq_pq_threaded_wait(srv, 25000);
        CHECK(moq_pq_threaded_conn_count(srv) == 2);
        CHECK(__atomic_load_n(&se.accepted_count, __ATOMIC_ACQUIRE) == 2);

        __atomic_store_n(&se.armed, 1, __ATOMIC_RELEASE);
        CHECK(moq_pq_threaded_wake(srv) == MOQ_OK);
        for (int tries = 0; tries < 400 &&
             !__atomic_load_n(&se.fired, __ATOMIC_ACQUIRE); tries++)
            moq_pq_threaded_wait(srv, 25000);
        CHECK(__atomic_load_n(&se.fired, __ATOMIC_ACQUIRE) == 1);
        CHECK(moq_pq_threaded_wait(srv, 0) == MOQ_ERR_CLOSED);
        CHECK(moq_pq_threaded_stop(srv) == MOQ_OK);

        for (int tries = 0; tries < 400 &&
             (__atomic_load_n(&cwa.closed_count, __ATOMIC_ACQUIRE) == 0 ||
              __atomic_load_n(&cwb.closed_count, __ATOMIC_ACQUIRE) == 0);
             tries++) {
            moq_pq_threaded_wait(a, 25000);
            moq_pq_threaded_wait(b, 25000);
        }
        CHECK(__atomic_load_n(&cwa.closed_count, __ATOMIC_ACQUIRE) == 1);
        CHECK(__atomic_load_n(&cwb.closed_count, __ATOMIC_ACQUIRE) == 1);
        CHECK(__atomic_load_n(&cwa.closed_code_valid, __ATOMIC_ACQUIRE) == 1);
        CHECK(__atomic_load_n(&cwb.closed_code_valid, __ATOMIC_ACQUIRE) == 1);
        CHECK(cwa.closed_code == 0);
        CHECK(cwb.closed_code == 0);
    }

    if (a) { moq_pq_threaded_stop(a); moq_pq_threaded_destroy(a); }
    if (b) { moq_pq_threaded_stop(b); moq_pq_threaded_destroy(b); }
    moq_pq_threaded_stop(srv);
    moq_pq_threaded_destroy(srv);
    return failures;
}

#else  /* !MOQ_TEST_CERT_PATH */

static int row_local_close(void)
{
    printf("SKIP: threaded_terminal_controls local_close "
           "(no test certificates)\n");
    return failures;
}

static int row_stop_close(void)
{
    printf("SKIP: threaded_terminal_controls stop_close "
           "(no test certificates)\n");
    return failures;
}

static int row_pump_exit_close(void)
{
    printf("SKIP: threaded_terminal_controls pump_exit_close "
           "(no test certificates)\n");
    return failures;
}

static int row_server_pump_exit_close(void)
{
    printf("SKIP: threaded_terminal_controls server_pump_exit_close "
           "(no test certificates)\n");
    return failures;
}

#endif

int main(int argc, char **argv)
{
    const char *row = (argc > 1) ? argv[1] : "client_unbind";
    if (strcmp(row, "client_unbind") == 0)
        (void)row_client_unbind();
    else if (strcmp(row, "local_close") == 0)
        (void)row_local_close();
    else if (strcmp(row, "stop_close") == 0)
        (void)row_stop_close();
    else if (strcmp(row, "pump_exit_close") == 0)
        (void)row_pump_exit_close();
    else if (strcmp(row, "server_pump_exit_close") == 0)
        (void)row_server_pump_exit_close();
    else {
        fprintf(stderr, "usage: %s "
                "[client_unbind|local_close|stop_close|pump_exit_close|"
                "server_pump_exit_close]\n",
                argv[0]);
        return 2;
    }

    if (failures) {
        printf("FAILED: %d check(s) [threaded_terminal_controls %s]\n",
               failures, row);
        return 1;
    }
    printf("PASS: threaded_terminal_controls %s\n", row);
    return 0;
}
