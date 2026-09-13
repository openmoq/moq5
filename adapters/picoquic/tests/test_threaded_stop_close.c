/*
 * stop() closes an open connection on the wire (raw picoquic facade).
 *
 * A managed threaded SERVER and a managed threaded CLIENT talk over loopback.
 * Once the client session is ESTABLISHED and idle, the test stops the client.
 * The server must learn of the departure promptly -- from the client's
 * CONNECTION_CLOSE, not from its own idle timeout -- so its connection count
 * drops to zero within a couple of seconds. Before the fix stop() only joined
 * the network thread: nothing was sent, and a relay kept the ghost session
 * (and the namespace it announced) until the 30 s idle timeout, which is
 * exactly what red5-moq-relay logged for every libmoq publisher exit.
 */
#include <moq/picoquic_threaded.h>
#include <moq/types.h>

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

static int failures = 0;

#define CHECK(expr)                                                     \
    do {                                                                \
        if (!(expr)) {                                                  \
            fprintf(stderr, "FAIL: %s:%d: %s\n",                        \
                    __FILE__, __LINE__, #expr);                         \
            failures++;                                                 \
        }                                                               \
    } while (0)

#ifdef MOQ_TEST_CERT_PATH

static int server_pump(moq_pq_threaded_t *t, moq_pq_threaded_lane_t *lane,
                       uint64_t now, void *u)
{
    (void)t; (void)lane; (void)now; (void)u;
    return 0;
}

typedef struct {
    int established;   /* atomic: set when the client session reaches ESTABLISHED */
} cli_ctx_t;

static int client_pump(moq_pq_threaded_t *t, moq_pq_threaded_lane_t *lane,
                       uint64_t now, void *u)
{
    (void)lane; (void)now;
    cli_ctx_t *c = (cli_ctx_t *)u;
    moq_session_t *s = moq_pq_threaded_session(t);
    if (s && moq_session_state(s) == MOQ_SESS_ESTABLISHED)
        __atomic_store_n(&c->established, 1, __ATOMIC_RELEASE);
    return 0;   /* keep running: the test stops the facade itself */
}

static double elapsed_s(const struct timespec *t0)
{
    struct timespec t1;
    clock_gettime(CLOCK_MONOTONIC, &t1);
    return (double)(t1.tv_sec - t0->tv_sec) +
           (double)(t1.tv_nsec - t0->tv_nsec) / 1e9;
}

/* 1 = ran to a verdict, 0 = the client never established (port busy /
 * handshake stalled: retry on another port). */
static int run_once(int port)
{
    moq_pq_threaded_cfg_t srv_cfg;
    moq_pq_threaded_cfg_init_sized(&srv_cfg, sizeof(srv_cfg));
    srv_cfg.alloc = moq_alloc_default();
    srv_cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    srv_cfg.cert_path = MOQ_TEST_CERT_PATH;
    srv_cfg.key_path = MOQ_TEST_KEY_PATH;
    srv_cfg.port = port;
    srv_cfg.on_lane_pump = server_pump;
    moq_pq_threaded_t *srv = NULL;
    if (moq_pq_threaded_create(&srv_cfg, &srv) != MOQ_OK || !srv)
        return 0;
    for (int i = 0; i < 3; i++) moq_pq_threaded_wait(srv, 100000);

    cli_ctx_t cc; memset(&cc, 0, sizeof(cc));
    moq_pq_threaded_cfg_t cli_cfg;
    moq_pq_threaded_cfg_init_sized(&cli_cfg, sizeof(cli_cfg));
    cli_cfg.alloc = moq_alloc_default();
    cli_cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cli_cfg.host = "127.0.0.1";
    cli_cfg.port = port;
    cli_cfg.insecure_skip_verify = true;
    cli_cfg.on_lane_pump = client_pump;
    cli_cfg.on_lane_pump_ctx = &cc;
    moq_pq_threaded_t *cli = NULL;
    if (moq_pq_threaded_create(&cli_cfg, &cli) != MOQ_OK || !cli) {
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
        return 0;
    }

    int established = 0;
    for (int i = 0; i < 400 && !established; i++) {
        established = __atomic_load_n(&cc.established, __ATOMIC_ACQUIRE);
        if (!established) moq_pq_threaded_wait(cli, 25000);
    }
    if (!established) {
        moq_pq_threaded_stop(cli);
        moq_pq_threaded_destroy(cli);
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
        return 0;
    }
    /* Let the server see the established session as a live connection. */
    for (int i = 0; i < 20 && moq_pq_threaded_conn_count(srv) == 0; i++)
        moq_pq_threaded_wait(srv, 50000);
    CHECK(moq_pq_threaded_conn_count(srv) == 1);

    /* The connection is open and idle: stop() must close it on the wire. */
    struct timespec t0;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    moq_pq_threaded_stop(cli);
    double stop_s = elapsed_s(&t0);
    CHECK(stop_s < 2.0);   /* the close flush is bounded, not a full join stall */

    int gone = 0;
    while (elapsed_s(&t0) < 3.0) {
        if (moq_pq_threaded_conn_count(srv) == 0) { gone = 1; break; }
        moq_pq_threaded_wait(srv, 100000);
    }
    if (!gone)
        fprintf(stderr, "[stop_close] server still holds the connection %.1fs "
                        "after the client stopped (idle timeout instead of "
                        "CONNECTION_CLOSE?)\n", elapsed_s(&t0));
    CHECK(gone);
    CHECK(!moq_pq_threaded_is_fatal(srv));

    moq_pq_threaded_destroy(cli);
    moq_pq_threaded_stop(srv);
    moq_pq_threaded_destroy(srv);
    return 1;
}

int main(void)
{
    srand((unsigned)getpid());
    int decided = 0;
    for (int attempt = 0; attempt < 4 && !decided; attempt++) {
        int port = 16300 + (rand() % 300);
        decided = run_once(port);
    }
    if (!decided) {
        fprintf(stderr, "no attempt reached an established session; skipping\n");
        return 77;
    }
    if (failures) {
        fprintf(stderr, "%d failure(s)\n", failures);
        return 1;
    }
    puts("threaded stop close: OK");
    return 0;
}

#else
int main(void)
{
    fprintf(stderr, "MOQ_TEST_CERT_PATH not defined; skipping\n");
    return 77;
}
#endif
