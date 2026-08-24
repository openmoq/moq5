/*
 * Real-handshake controls for the raw threaded picoquic client's TLS policy
 * (finding 2), over a localhost loopback against the picoquic test server
 * certificate (CN=test.example.com, signed by "picotls test ca"):
 *
 *   1. default client (system trust)  -> REJECTED, terminal (untrusted CA)
 *   2. insecure_skip_verify = true     -> ACCEPTED
 *   3. custom CA (the test CA) + SNI   -> ACCEPTED
 *   4. custom CA + WRONG SNI           -> REJECTED, terminal (hostname mismatch)
 *
 * A rejection is a TERMINAL outcome, not a timeout: each run is driven until the
 * client either ESTABLISHES or latches fatal, and the two rejection rows require
 * the full terminal signature (handle created, loop decided, never established,
 * is_fatal, fatal_code == 0, wait(0) == MOQ_ERR_CLOSED, and a QUIC TLS crypto
 * terminal_error in 0x100..0x1ff). A stalled handshake that merely fails to
 * establish is NOT a rejection -- pinned by is_terminal_rejection() and its
 * permanent self-check. The matching-CA row is the live serving/connectivity
 * control for the wrong-SNI row. Links the test-internals build.
 */

#include <moq/picoquic_threaded.h>
#include <moq/picoquic_verify.h>
#include <moq/types.h>            /* MOQ_OK, MOQ_ERR_CLOSED */

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
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

/* Declared terminal outcome of one client run, snapshotted from the public
 * facade state before stop/destroy. */
typedef struct {
    int      created;         /* a client handle was created */
    int      established;     /* reached MOQ_SESS_ESTABLISHED */
    int      loop_expired;    /* bounded loop ended with neither terminal */
    int      fatal;           /* moq_pq_threaded_is_fatal() */
    uint64_t fatal_code;      /* moq_pq_threaded_fatal_code() */
    uint64_t terminal_error;  /* moq_pq_threaded_terminal_error() */
    int      wait0;           /* moq_pq_threaded_wait(cli, 0) */
} hs_result_t;

/* A QUIC TLS crypto terminal error is 0x100|alert (0x100..0x1ff); the specific
 * alert number is backend-specific and deliberately not pinned. */
static int is_tls_crypto_error(uint64_t e) { return e >= 0x100 && e <= 0x1ff; }

/* THE load-bearing classifier: an undecided / no-terminal result is NOT a
 * rejection. A rejection is a fully terminal, cert-level failure. */
static int is_terminal_rejection(const hs_result_t *r)
{
    return r->created && !r->loop_expired && !r->established &&
           r->fatal && r->fatal_code == 0 &&
           r->wait0 == MOQ_ERR_CLOSED &&
           is_tls_crypto_error(r->terminal_error);
}

/* Permanent self-check: proves is_terminal_rejection() rejects the shapes a
 * timeout-only oracle would have accepted, and accepts a real terminal reject.
 * Runs every invocation, independent of the network. */
static void classifier_selfcheck(void)
{
    hs_result_t undecided = { 1, 0, 1, 0, 0, 0, MOQ_ERR_CLOSED };
    CHECK(!is_terminal_rejection(&undecided));      /* stall != rejection */

    hs_result_t no_fatal = { 1, 0, 0, 0, 0, 0x130, MOQ_ERR_CLOSED };
    CHECK(!is_terminal_rejection(&no_fatal));        /* not fatal != rejection */

    hs_result_t not_tls = { 1, 0, 0, 1, 0, 0x1, MOQ_ERR_CLOSED };
    CHECK(!is_terminal_rejection(&not_tls));         /* non-TLS terminal error */

    hs_result_t not_created = { 0, 0, 0, 1, 0, 0x130, MOQ_ERR_CLOSED };
    CHECK(!is_terminal_rejection(&not_created));     /* create failure */

    hs_result_t real = { 1, 0, 0, 1, 0, 0x130, MOQ_ERR_CLOSED };
    CHECK(is_terminal_rejection(&real));             /* genuine terminal reject */
}

static void expect_reject(const hs_result_t *r)
{
    CHECK(r->created);
    CHECK(!r->loop_expired);
    CHECK(!r->established);
    CHECK(r->fatal);
    CHECK(r->fatal_code == 0);
    CHECK(r->wait0 == MOQ_ERR_CLOSED);
    CHECK(is_tls_crypto_error(r->terminal_error));
    CHECK(is_terminal_rejection(r));
}

static void expect_accept(const hs_result_t *r)
{
    CHECK(r->created);
    CHECK(r->established);
    CHECK(!r->loop_expired);
    CHECK(!r->fatal);
    CHECK(!is_terminal_rejection(r));
}

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
    return 0;
}

/* configure_quic hook that pins the test CA (custom-CA scenario). */
static int pin_test_ca(picoquic_quic_t *quic, void *ctx)
{
    (void)ctx;
    return moq_picoquic_set_cert_verifier(quic, MOQ_TEST_CA_PATH);
}

/* Drive the loopback until the client ESTABLISHES or latches fatal, then
 * snapshot the public terminal state before stop/destroy. */
static hs_result_t run_client(int port, int insecure, int custom_ca,
                              const char *sni)
{
    hs_result_t r; memset(&r, 0, sizeof(r));
    cli_ctx_t c; memset(&c, 0, sizeof(c));
    moq_pq_threaded_cfg_t cfg;
    moq_pq_threaded_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = "127.0.0.1";
    cfg.port = port;
    cfg.insecure_skip_verify = insecure ? true : false;
    cfg.sni = sni;                 /* verified name; NULL => host */
    cfg.on_lane_pump = client_pump;
    cfg.on_lane_pump_ctx = &c;
    if (custom_ca)
        cfg.configure_quic = pin_test_ca;

    moq_pq_threaded_t *cli = NULL;
    if (moq_pq_threaded_create(&cfg, &cli) != MOQ_OK || !cli)
        return r;                  /* created == 0 */
    r.created = 1;

    int decided = 0;
    for (int i = 0; i < 400; i++) {
        if (__atomic_load_n(&c.established, __ATOMIC_ACQUIRE)) {
            r.established = 1; decided = 1; break;
        }
        if (moq_pq_threaded_is_fatal(cli)) { decided = 1; break; }
        moq_pq_threaded_wait(cli, 25000);   /* 25ms */
    }
    r.loop_expired = !decided;

    /* Snapshot the public facade terminal state before teardown. */
    r.fatal          = moq_pq_threaded_is_fatal(cli) ? 1 : 0;
    r.fatal_code     = moq_pq_threaded_fatal_code(cli);
    r.terminal_error = moq_pq_threaded_terminal_error(cli);
    r.wait0          = (int)moq_pq_threaded_wait(cli, 0);

    moq_pq_threaded_stop(cli);
    moq_pq_threaded_destroy(cli);
    return r;
}

int main(void)
{
    classifier_selfcheck();

    srand((unsigned)getpid());
    int port = 15900 + (rand() % 300);

    moq_pq_threaded_cfg_t srv;
    moq_pq_threaded_cfg_init_sized(&srv, sizeof(srv));
    srv.alloc = moq_alloc_default();
    srv.perspective = MOQ_PERSPECTIVE_SERVER;
    srv.cert_path = MOQ_TEST_CERT_PATH;
    srv.key_path = MOQ_TEST_KEY_PATH;
    srv.port = port;
    srv.on_lane_pump = server_pump;
    moq_pq_threaded_t *server = NULL;
    CHECK(moq_pq_threaded_create(&srv, &server) == MOQ_OK);

    if (server) {
        /* 1. Default client: system trust does not contain the test CA, so the
         *    server cert is rejected -- a terminal, cert-level failure. */
        hs_result_t def = run_client(port, /*insecure=*/0, /*custom_ca=*/0,
                                     "test.example.com");
        expect_reject(&def);

        /* 2. Insecure client: null verifier accepts any cert. */
        hs_result_t ins = run_client(port, /*insecure=*/1, /*custom_ca=*/0, NULL);
        expect_accept(&ins);

        /* 3. Custom CA (the test CA) + matching SNI: accepted. This is the live
         *    serving/connectivity control for row 4. */
        hs_result_t ca = run_client(port, /*insecure=*/0, /*custom_ca=*/1,
                                    "test.example.com");
        expect_accept(&ca);

        /* 4. Custom CA (trusted) + WRONG SNI: rejected on the hostname mismatch.
         *    Same trusted CA and server cert as row 3, so the chain validates --
         *    only the verified name differs (cert is CN=test.example.com). Row 3
         *    accepts and row 1 rejects, so a terminal rejection here is
         *    attributable specifically to the hostname mismatch. */
        hs_result_t ca_bad = run_client(port, /*insecure=*/0, /*custom_ca=*/1,
                                        "wrong.example.com");
        expect_reject(&ca_bad);

        moq_pq_threaded_stop(server);
        moq_pq_threaded_destroy(server);
    }

    if (failures) {
        fprintf(stderr, "threaded_tls_handshake: %d failure(s)\n", failures);
        return 1;
    }
    printf("threaded_tls_handshake: reject/insecure/custom-CA/wrong-host "
           "terminal controls passed\n");
    return 0;
}
