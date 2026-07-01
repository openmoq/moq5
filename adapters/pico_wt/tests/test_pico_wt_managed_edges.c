/*
 * Managed pico WT edge cases: config validation, NULL safety, connect
 * failure, certificate-verification failure, stop/wake/wait lifecycle,
 * and stop-from-on_pump rejection.
 *
 * Args: --cert <file> --key <file>
 */

#include <moq/moq.h>
#include <moq/pico_wt_managed.h>
#include <moq/picoquic_verify.h>   /* the helper under test */

#include <picoquic.h>
#include <picotls.h>

#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int failures = 0;
#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

static int noop_pump(moq_pico_wt_managed_t *m, uint64_t now, void *ctx)
{ (void)m; (void)now; (void)ctx; return 0; }

/* configure_quic hook: short handshake timeout so a failed connect
 * surfaces in ~1s instead of the multi-second default. */
static int short_handshake(picoquic_quic_t *quic, void *ctx)
{
    (void)ctx;
    picoquic_set_default_handshake_timeout(quic, 1000000);  /* 1s */
    return 0;
}

static void base_client_cfg(moq_pico_wt_managed_cfg_t *cfg, int port)
{
    moq_pico_wt_managed_cfg_init(cfg);
    cfg->alloc = moq_alloc_default();
    cfg->perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg->host = "127.0.0.1";
    cfg->port = port;
    cfg->on_pump = noop_pump;
}

/* -- config validation (no network) -------------------------------- */

static void test_cfg_validation(void)
{
    moq_pico_wt_managed_t *m = (moq_pico_wt_managed_t *)1;
    moq_pico_wt_managed_cfg_t cfg;

    CHECK(moq_pico_wt_managed_create(NULL, &m) == MOQ_ERR_INVAL);
    CHECK(m == NULL);

    /* missing on_pump */
    moq_pico_wt_managed_cfg_init(&cfg);
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = "127.0.0.1"; cfg.port = 4433;
    CHECK(moq_pico_wt_managed_create(&cfg, &m) == MOQ_ERR_INVAL);

    /* client without host */
    base_client_cfg(&cfg, 4433); cfg.host = NULL;
    CHECK(moq_pico_wt_managed_create(&cfg, &m) == MOQ_ERR_INVAL);

    /* server without cert/key */
    moq_pico_wt_managed_cfg_init(&cfg);
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.port = 4433; cfg.on_pump = noop_pump;
    CHECK(moq_pico_wt_managed_create(&cfg, &m) == MOQ_ERR_INVAL);

    /* invalid perspective */
    base_client_cfg(&cfg, 4433); cfg.perspective = (moq_perspective_t)0;
    CHECK(moq_pico_wt_managed_create(&cfg, &m) == MOQ_ERR_INVAL);

    /* bad ports */
    base_client_cfg(&cfg, 0);
    CHECK(moq_pico_wt_managed_create(&cfg, &m) == MOQ_ERR_INVAL);
    base_client_cfg(&cfg, 70000);
    CHECK(moq_pico_wt_managed_create(&cfg, &m) == MOQ_ERR_INVAL);

    /* missing alloc */
    base_client_cfg(&cfg, 4433); cfg.alloc = NULL;
    CHECK(moq_pico_wt_managed_create(&cfg, &m) == MOQ_ERR_INVAL);
}

/* -- NULL safety --------------------------------------------------- */

static void test_null_safety(void)
{
    CHECK(moq_pico_wt_managed_stop(NULL) == MOQ_ERR_INVAL);
    moq_pico_wt_managed_destroy(NULL);  /* void; must not crash */
    CHECK(moq_pico_wt_managed_session(NULL) == NULL);
    CHECK(!moq_pico_wt_managed_is_fatal(NULL));
    CHECK(!moq_pico_wt_managed_is_closed(NULL));
    CHECK(moq_pico_wt_managed_fatal_code(NULL) == 0);
    CHECK(moq_pico_wt_managed_close_code(NULL) == 0);
    CHECK(moq_pico_wt_managed_local_port(NULL) == 0);
    CHECK(moq_pico_wt_managed_wake(NULL) == MOQ_ERR_INVAL);
    CHECK(moq_pico_wt_managed_wait(NULL, 0) == MOQ_ERR_INVAL);
}

/* -- connect failure (no server) ----------------------------------- */

static void test_connect_failure(void)
{
    /* Nothing listens on port 1: the handshake fails and the managed
     * client must surface a terminal (fatal) state, not hang. */
    moq_pico_wt_managed_cfg_t cfg;
    base_client_cfg(&cfg, 1);
    cfg.insecure_skip_verify = true;
    cfg.configure_quic = short_handshake;
    moq_pico_wt_managed_t *cli = NULL;
    CHECK(moq_pico_wt_managed_create(&cfg, &cli) == MOQ_OK);
    if (!cli) return;

    int fatal = 0;
    for (int i = 0; i < 40 && !fatal; i++) {   /* up to ~4s */
        moq_pico_wt_managed_wait(cli, 100000);
        fatal = moq_pico_wt_managed_is_fatal(cli);
    }
    CHECK(fatal);
    CHECK(moq_pico_wt_managed_fatal_code(cli) == 0);   /* transport-level */
    CHECK(moq_pico_wt_managed_session(cli) == NULL);  /* never came up */

    moq_pico_wt_managed_stop(cli);
    moq_pico_wt_managed_destroy(cli);
}

/* -- stop/wake/wait lifecycle -------------------------------------- */

static void test_lifecycle(void)
{
    /* Client to a dead port with the default (long) handshake timeout:
     * it stays connecting (not fatal) for the brief checks below. */
    moq_pico_wt_managed_cfg_t cfg;
    base_client_cfg(&cfg, 1);
    cfg.insecure_skip_verify = true;
    moq_pico_wt_managed_t *cli = NULL;
    CHECK(moq_pico_wt_managed_create(&cfg, &cli) == MOQ_OK);
    if (!cli) return;

    CHECK(moq_pico_wt_managed_wake(cli) == MOQ_OK);   /* thread running */
    moq_result_t w = moq_pico_wt_managed_wait(cli, 0);  /* non-blocking */
    CHECK(w == MOQ_OK || w == MOQ_DONE);

    CHECK(moq_pico_wt_managed_stop(cli) == MOQ_OK);
    CHECK(moq_pico_wt_managed_stop(cli) == MOQ_OK);    /* idempotent */
    CHECK(moq_pico_wt_managed_wake(cli) == MOQ_ERR_CLOSED);
    CHECK(moq_pico_wt_managed_wait(cli, 0) == MOQ_ERR_CLOSED);

    moq_pico_wt_managed_destroy(cli);
}

/* -- cert verification failure ------------------------------------- */

static moq_pico_wt_managed_t *make_test_server(const char *cert,
    const char *key, int port)
{
    moq_pico_wt_managed_cfg_t cfg;
    moq_pico_wt_managed_cfg_init(&cfg);
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.cert_path = cert; cfg.key_path = key;
    cfg.port = port;
    cfg.on_pump = noop_pump;
    moq_pico_wt_managed_t *srv = NULL;
    if (moq_pico_wt_managed_create(&cfg, &srv) != MOQ_OK) return NULL;
    for (int i = 0; i < 3; i++) moq_pico_wt_managed_wait(srv, 100000);
    return srv;
}

/* A verify-certificate callback that rejects every server cert. Used to
 * exercise the verification-failure path (picoquic's built-in default
 * has no CA store and accepts, so we install an explicit verifier — the
 * configure_quic hook is exactly the documented place for this). */
static atomic_int reject_called;

static int reject_cert_cb(struct st_ptls_verify_certificate_t *self,
    ptls_t *tls, const char *server_name,
    int (**verify_sign)(void *, uint16_t, ptls_iovec_t, ptls_iovec_t),
    void **verify_data, ptls_iovec_t *certs, size_t num_certs)
{
    (void)self; (void)tls; (void)server_name; (void)verify_sign;
    (void)verify_data; (void)certs; (void)num_certs;
    atomic_store(&reject_called, 1);   /* proves verification ran */
    return PTLS_ALERT_BAD_CERTIFICATE;
}
/* Advertise the common TLS 1.3 signature schemes so negotiation
 * reaches certificate verification (the test cert is RSA): rsa_pss_rsae
 * sha256/384/512, ecdsa_secp256r1_sha256, ed25519. */
static uint16_t reject_algos[] = {
    0x0804, 0x0805, 0x0806, 0x0403, 0x0807, UINT16_MAX
};
static ptls_verify_certificate_t reject_verifier =
    { reject_cert_cb, reject_algos };

static int install_reject_verifier(picoquic_quic_t *quic, void *ctx)
{
    (void)ctx;
    picoquic_set_default_handshake_timeout(quic, 1000000);
    picoquic_set_verify_certificate_callback(quic, &reject_verifier, NULL);
    return 0;
}

static void test_cert_failure(const char *cert, const char *key)
{
    /* Retry candidate ports so a busy/unbound server is not mistaken
     * for a cert rejection. "Proven" requires the verify callback to
     * have actually run; only then are fatal + no-session asserted. */
    const int ports[] = { 18483, 19483, 20483 };
    int proven = 0;
    for (size_t i = 0; i < sizeof(ports) / sizeof(ports[0]) && !proven;
         i++) {
        atomic_store(&reject_called, 0);
        moq_pico_wt_managed_t *srv =
            make_test_server(cert, key, ports[i]);
        if (!srv) continue;                          /* bind — retry */

        moq_pico_wt_managed_cfg_t cfg;
        base_client_cfg(&cfg, moq_pico_wt_managed_local_port(srv));
        cfg.insecure_skip_verify = false;            /* verify the cert */
        cfg.configure_quic = install_reject_verifier;
        moq_pico_wt_managed_t *cli = NULL;
        if (moq_pico_wt_managed_create(&cfg, &cli) == MOQ_OK && cli) {
            int fatal = 0;
            for (int k = 0; k < 40 && !fatal; k++) {
                moq_pico_wt_managed_wait(cli, 100000);
                fatal = moq_pico_wt_managed_is_fatal(cli);
            }
            int called = atomic_load(&reject_called);
            int has_session = moq_pico_wt_managed_session(cli) != NULL;
            uint64_t fcode = moq_pico_wt_managed_fatal_code(cli);
            moq_pico_wt_managed_stop(cli);
            moq_pico_wt_managed_destroy(cli);
            if (called) {
                CHECK(fatal);            /* rejection → terminal state */
                CHECK(fcode == 0);       /* transport/handshake-level */
                CHECK(!has_session);     /* no session established */
                proven = 1;
            }
            /* else: handshake never reached verification (server may
             * not have bound on this port) — try the next candidate. */
        }
        moq_pico_wt_managed_stop(srv);
        moq_pico_wt_managed_destroy(srv);
    }
    CHECK(proven);   /* the verify callback ran and rejected somewhere */
}

/* -- production helper: moq_picoquic_set_cert_verifier ------------- */

/* configure_quic that installs the libmoq production verifier (system
 * trust store). Also shortens the handshake timeout so a rejected
 * connection surfaces quickly. */
static int install_helper_verifier(picoquic_quic_t *quic, void *ctx)
{
    (void)ctx;
    picoquic_set_default_handshake_timeout(quic, 1000000);  /* 1s */
    return moq_picoquic_set_cert_verifier(quic, NULL);  /* NULL = system */
}

/* Poll a managed client until it goes fatal or exposes a session. */
static int wait_client(moq_pico_wt_managed_t *cli, int *out_has_session)
{
    int fatal = 0;
    for (int k = 0; k < 50 && !fatal; k++) {
        moq_pico_wt_managed_wait(cli, 100000);
        fatal = moq_pico_wt_managed_is_fatal(cli);
        if (moq_pico_wt_managed_session(cli) != NULL) break;
    }
    *out_has_session = moq_pico_wt_managed_session(cli) != NULL;
    return moq_pico_wt_managed_is_fatal(cli);
}

static void test_helper_cert_failure(const char *cert, const char *key)
{
    /* Prove moq_picoquic_set_cert_verifier installs a REAL verifier:
     * against the self-signed test server it rejects (client fatal, no
     * session). Self-contained per port: a rejected client fails at TLS
     * BEFORE WT CONNECT, so it never occupies the one-connection server —
     * an insecure control client then connects to the SAME server and
     * comes up, proving the server was serving and isolating the helper's
     * verifier as the cause of the first client's failure. */
    const int ports[] = { 18493, 19493, 20493 };
    int proven = 0;
    for (size_t i = 0; i < sizeof(ports) / sizeof(ports[0]) && !proven;
         i++) {
        moq_pico_wt_managed_t *srv = make_test_server(cert, key, ports[i]);
        if (!srv) continue;
        int port = moq_pico_wt_managed_local_port(srv);

        /* Client 1: helper (system trust) → must reject the self-signed. */
        moq_pico_wt_managed_cfg_t c1;
        base_client_cfg(&c1, port);
        c1.insecure_skip_verify = false;
        c1.configure_quic = install_helper_verifier;
        moq_pico_wt_managed_t *cli1 = NULL;
        int h_fatal = 0, h_session = 1;
        if (moq_pico_wt_managed_create(&c1, &cli1) == MOQ_OK && cli1)
            h_fatal = wait_client(cli1, &h_session);
        if (cli1) {
            moq_pico_wt_managed_stop(cli1);
            moq_pico_wt_managed_destroy(cli1);
        }

        /* Client 2: insecure control → must come up on the same server. */
        moq_pico_wt_managed_cfg_t c2;
        base_client_cfg(&c2, port);
        c2.insecure_skip_verify = true;
        moq_pico_wt_managed_t *cli2 = NULL;
        int ctrl_session = 0, ctrl_dummy = 0;
        if (moq_pico_wt_managed_create(&c2, &cli2) == MOQ_OK && cli2) {
            (void)wait_client(cli2, &ctrl_dummy);
            ctrl_session = moq_pico_wt_managed_session(cli2) != NULL;
            moq_pico_wt_managed_stop(cli2);
            moq_pico_wt_managed_destroy(cli2);
        }

        moq_pico_wt_managed_stop(srv);
        moq_pico_wt_managed_destroy(srv);

        /* Only assert once the control proves the server served on this
         * port (rules out a busy/unbound-port false positive). */
        if (ctrl_session) {
            CHECK(h_fatal);       /* helper rejected → terminal */
            CHECK(!h_session);    /* no session established */
            proven = 1;
        }
    }
    CHECK(proven);  /* helper's verifier rejected against a serving peer */
}

static void test_default_cert_failure(const char *cert, const char *key)
{
    /* The DEFAULT client config (insecure_skip_verify=false, no configure_quic)
     * must fail closed: against the self-signed test server it rejects (client
     * fatal, no session), proving the managed client installs a real verifier by
     * default. An insecure control client then connects to the SAME server,
     * isolating the default verifier as the cause of the first failure.
     *
     * Guards against a default that installs no verifier: client 1 would then
     * ACCEPT the self-signed cert and come up with a session (h_fatal=0,
     * h_session=1). */
    const int ports[] = { 18495, 19495, 20495 };
    int proven = 0;
    for (size_t i = 0; i < sizeof(ports) / sizeof(ports[0]) && !proven;
         i++) {
        moq_pico_wt_managed_t *srv = make_test_server(cert, key, ports[i]);
        if (!srv) continue;
        int port = moq_pico_wt_managed_local_port(srv);

        /* Client 1: default verifier (no configure_quic) → must reject. An
         * explicit sni is required to verify against a bare-IP host; the
         * self-signed cert still fails chain validation regardless of name. */
        moq_pico_wt_managed_cfg_t c1;
        base_client_cfg(&c1, port);
        c1.insecure_skip_verify = false;   /* default; verifier installed */
        c1.sni = "localhost";              /* expected name (IP host) */
        moq_pico_wt_managed_t *cli1 = NULL;
        int d_fatal = 0, d_session = 1;
        if (moq_pico_wt_managed_create(&c1, &cli1) == MOQ_OK && cli1)
            d_fatal = wait_client(cli1, &d_session);
        if (cli1) {
            moq_pico_wt_managed_stop(cli1);
            moq_pico_wt_managed_destroy(cli1);
        }

        /* Client 2: insecure control → must come up on the same server. */
        moq_pico_wt_managed_cfg_t c2;
        base_client_cfg(&c2, port);
        c2.insecure_skip_verify = true;
        moq_pico_wt_managed_t *cli2 = NULL;
        int ctrl_session = 0, ctrl_dummy = 0;
        if (moq_pico_wt_managed_create(&c2, &cli2) == MOQ_OK && cli2) {
            (void)wait_client(cli2, &ctrl_dummy);
            ctrl_session = moq_pico_wt_managed_session(cli2) != NULL;
            moq_pico_wt_managed_stop(cli2);
            moq_pico_wt_managed_destroy(cli2);
        }

        moq_pico_wt_managed_stop(srv);
        moq_pico_wt_managed_destroy(srv);

        if (ctrl_session) {
            CHECK(d_fatal);       /* default verifier rejected → terminal */
            CHECK(!d_session);    /* no session established */
            proven = 1;
        }
    }
    CHECK(proven);  /* default fails closed against a serving peer */
}

static void test_ip_host_sni_policy(void)
{
    /* IP-literal host SNI policy. Drives only _create (+ stop/destroy) to a
     * dead port — no server or successful handshake is needed, though _create
     * does start the network thread.
     *  - insecure + IP host + no sni: must still CREATE (regression: a bare IP
     *    is not a valid SNI, so we must not feed it to picoquic). Historically
     *    SNI defaulted to "localhost"; that path must keep working.
     *  - verified + IP host + no sni: must FAIL CLOSED (MOQ_ERR_INVAL) — we
     *    cannot do hostname verification against a bare IP without an expected
     *    name, and must not send an invalid IP SNI or verify a placeholder.
     *  - verified + IP host + explicit sni: must CREATE (name supplied). */
    moq_pico_wt_managed_cfg_t cfg;
    moq_pico_wt_managed_t *cli = NULL;

    base_client_cfg(&cfg, 1);             /* host = 127.0.0.1, dead port 1 */
    cfg.insecure_skip_verify = true;      /* no sni */
    CHECK(moq_pico_wt_managed_create(&cfg, &cli) == MOQ_OK);
    CHECK(cli != NULL);
    if (cli) { moq_pico_wt_managed_stop(cli); moq_pico_wt_managed_destroy(cli); }

    cli = NULL;
    base_client_cfg(&cfg, 1);
    cfg.insecure_skip_verify = false;     /* verified, no sni, IP host */
    CHECK(moq_pico_wt_managed_create(&cfg, &cli) == MOQ_ERR_INVAL);
    CHECK(cli == NULL);

    cli = NULL;
    base_client_cfg(&cfg, 1);
    cfg.insecure_skip_verify = false;
    cfg.sni = "example.test";             /* verified, explicit name, IP host */
    CHECK(moq_pico_wt_managed_create(&cfg, &cli) == MOQ_OK);
    CHECK(cli != NULL);
    if (cli) { moq_pico_wt_managed_stop(cli); moq_pico_wt_managed_destroy(cli); }
}

/* -- stop() from on_pump must be rejected -------------------------- */

typedef struct { atomic_int done; int stop_rc; } stopcheck_app_t;

static int stopcheck_pump(moq_pico_wt_managed_t *m, uint64_t now, void *ctx)
{
    (void)now;
    stopcheck_app_t *a = (stopcheck_app_t *)ctx;
    if (!atomic_load(&a->done)) {
        a->stop_rc = (int)moq_pico_wt_managed_stop(m);  /* network thread */
        atomic_store(&a->done, 1);
    }
    return 0;
}

static void test_stop_from_pump(const char *cert, const char *key)
{
    /* Needs the client to attach (so on_pump runs), which needs a bound
     * server — retry candidate ports; only assert once on_pump ran. */
    const int ports[] = { 18484, 19484, 20484 };
    int proven = 0;
    for (size_t i = 0; i < sizeof(ports) / sizeof(ports[0]) && !proven;
         i++) {
        moq_pico_wt_managed_t *srv =
            make_test_server(cert, key, ports[i]);
        if (!srv) continue;

        stopcheck_app_t app; memset(&app, 0, sizeof(app));
        atomic_init(&app.done, 0);
        app.stop_rc = 0;

        moq_pico_wt_managed_cfg_t cfg;
        base_client_cfg(&cfg, moq_pico_wt_managed_local_port(srv));
        cfg.insecure_skip_verify = true;
        cfg.on_pump = stopcheck_pump;
        cfg.on_pump_ctx = &app;
        moq_pico_wt_managed_t *cli = NULL;
        if (moq_pico_wt_managed_create(&cfg, &cli) == MOQ_OK && cli) {
            uint64_t waited = 0;
            while (!atomic_load(&app.done) && waited < 5000) {
                if (moq_pico_wt_managed_wait(cli, 200000) == MOQ_ERR_CLOSED)
                    break;
                waited += 200;
            }
            int done = atomic_load(&app.done);
            int rc = app.stop_rc;
            moq_pico_wt_managed_stop(cli);
            moq_pico_wt_managed_destroy(cli);
            if (done) {
                CHECK(rc == MOQ_ERR_WRONG_STATE);    /* stop() refused */
                proven = 1;
            }
            /* else: client never attached (server may not have bound) —
             * try the next candidate port. */
        }
        moq_pico_wt_managed_stop(srv);
        moq_pico_wt_managed_destroy(srv);
    }
    CHECK(proven);   /* on_pump ran and stop() was rejected */
}

int main(int argc, char **argv)
{
    const char *cert = NULL, *key = NULL;
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--cert") && i + 1 < argc) cert = argv[++i];
        else if (!strcmp(argv[i], "--key") && i + 1 < argc) key = argv[++i];
    }
    if (!cert || !key) {
        fprintf(stderr, "usage: %s --cert <f> --key <f>\n", argv[0]);
        return 2;
    }

    test_cfg_validation();
    test_null_safety();
    test_ip_host_sni_policy();
    test_connect_failure();
    test_lifecycle();
    test_cert_failure(cert, key);
    test_helper_cert_failure(cert, key);
    test_default_cert_failure(cert, key);
    test_stop_from_pump(cert, key);

    if (failures == 0)
        printf("test_pico_wt_managed_edges: PASS\n");
    else
        fprintf(stderr, "test_pico_wt_managed_edges: %d failure(s)\n",
                failures);
    return failures ? 1 : 0;
}
