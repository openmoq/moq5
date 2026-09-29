/*
 * Deterministic proof that the pico WT managed facade arms QUIC keepalive from
 * moq_pico_wt_managed_cfg_t.keep_alive_interval_ms. The default arm uses a
 * no-relay client create to reach picoquic_start_client_cnx without wall-clock
 * idle timing, and the optional server arm proves accepted connections are
 * covered by a local loopback handshake. The MOQ_PICO_WT_TESTING seam captures
 * the keepalive call instead of invoking picoquic_enable_keep_alive.
 */
#include <moq/pico_wt_managed.h>

#include "pico_wt_test_seam.h"

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int failures;
#define CHECK(expr) do { if (!(expr)) { \
    fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
    failures++; } } while (0)

static void ka_noop(picoquic_cnx_t *cnx, uint64_t interval_us)
{
    (void)cnx;
    (void)interval_us;
}

static int pump_cb(moq_pico_wt_managed_t *m, uint64_t now_us, void *ctx)
{
    (void)m;
    (void)now_us;
    (void)ctx;
    return 0;
}

static void reset_seam(void)
{
    moq_pico_wt_managed_test_keep_alive = ka_noop;
    moq_pico_wt_managed_test_keep_alive_calls = 0;
    moq_pico_wt_managed_test_keep_alive_last_cnx = NULL;
    moq_pico_wt_managed_test_keep_alive_last_interval_us = 0;
}

static void client_cfg(moq_pico_wt_managed_cfg_t *cfg)
{
    moq_pico_wt_managed_cfg_init_sized(cfg, sizeof(*cfg));
    cfg->alloc = moq_alloc_default();
    cfg->perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg->host = "127.0.0.1";
    cfg->port = 34567;                 /* nothing listening; no WT setup */
    cfg->path = "/moq";
    cfg->insecure_skip_verify = true;  /* no cert needed for no-relay create */
    cfg->on_pump = pump_cb;
}

static void cleanup_facade(moq_pico_wt_managed_t *m)
{
    if (!m) return;
    (void)moq_pico_wt_managed_stop(m);
    moq_pico_wt_managed_destroy(m);
}

static void test_client_opt_in(void)
{
    reset_seam();
    moq_pico_wt_managed_cfg_t cfg;
    client_cfg(&cfg);
    cfg.keep_alive_interval_ms = 1234;

    moq_pico_wt_managed_t *m = NULL;
    moq_result_t rc = moq_pico_wt_managed_create(&cfg, &m);
    CHECK(rc == MOQ_OK);
    CHECK(m != NULL);
    CHECK(moq_pico_wt_managed_test_keep_alive_calls == 1);
    CHECK(moq_pico_wt_managed_test_keep_alive_last_cnx != NULL);
    CHECK(moq_pico_wt_managed_test_keep_alive_last_interval_us == 1234000ull);
    cleanup_facade(m);
}

static void test_default_disabled(void)
{
    reset_seam();
    moq_pico_wt_managed_cfg_t cfg;
    client_cfg(&cfg);

    moq_pico_wt_managed_t *m = NULL;
    moq_result_t rc = moq_pico_wt_managed_create(&cfg, &m);
    CHECK(rc == MOQ_OK);
    CHECK(m != NULL);
    CHECK(moq_pico_wt_managed_test_keep_alive_calls == 0);
    cleanup_facade(m);
}

static void test_prefix_before_field_zero_calls(void)
{
    reset_seam();
    moq_pico_wt_managed_cfg_t cfg;
    client_cfg(&cfg);
    cfg.keep_alive_interval_ms = 9999;  /* would-be bytes past old prefix */
    cfg.struct_size = (uint32_t)offsetof(moq_pico_wt_managed_cfg_t,
                                         keep_alive_interval_ms);

    moq_pico_wt_managed_t *m = NULL;
    moq_result_t rc = moq_pico_wt_managed_create(&cfg, &m);
    CHECK(rc == MOQ_OK);
    CHECK(m != NULL);
    CHECK(moq_pico_wt_managed_test_keep_alive_calls == 0);
    cleanup_facade(m);
}

static int wait_for_session(moq_pico_wt_managed_t *a,
                            moq_pico_wt_managed_t *b)
{
    for (int i = 0; i < 60; i++) {
        if (moq_pico_wt_managed_session(a) != NULL &&
            moq_pico_wt_managed_session(b) != NULL)
            return 1;
        (void)moq_pico_wt_managed_wait(a, 50000);
        (void)moq_pico_wt_managed_wait(b, 50000);
    }
    return 0;
}

static void test_server_accepted_connection(const char *cert,
                                            const char *key)
{
    for (int attempt = 0; attempt < 8; attempt++) {
        reset_seam();
        int port = 18880 + (int)(getpid() % 1000) + attempt;

        moq_pico_wt_managed_cfg_t scfg;
        moq_pico_wt_managed_cfg_init_sized(&scfg, sizeof(scfg));
        scfg.alloc = moq_alloc_default();
        scfg.perspective = MOQ_PERSPECTIVE_SERVER;
        scfg.cert_path = cert;
        scfg.key_path = key;
        scfg.port = port;
        scfg.on_pump = pump_cb;
        scfg.keep_alive_interval_ms = 4321;

        moq_pico_wt_managed_t *srv = NULL;
        if (moq_pico_wt_managed_create(&scfg, &srv) != MOQ_OK)
            continue;  /* busy port or local socket setup issue; retry */

        moq_pico_wt_managed_cfg_t ccfg;
        client_cfg(&ccfg);
        ccfg.port = moq_pico_wt_managed_local_port(srv);

        moq_pico_wt_managed_t *cli = NULL;
        moq_result_t crc = moq_pico_wt_managed_create(&ccfg, &cli);
        if (crc != MOQ_OK || !cli) {
            cleanup_facade(srv);
            continue;
        }

        int established = wait_for_session(srv, cli);
        cleanup_facade(cli);
        cleanup_facade(srv);

        if (!established)
            continue;

        CHECK(moq_pico_wt_managed_test_keep_alive_calls == 1);
        CHECK(moq_pico_wt_managed_test_keep_alive_last_cnx != NULL);
        CHECK(moq_pico_wt_managed_test_keep_alive_last_interval_us ==
              4321000ull);
        return;
    }
    CHECK(!"server accepted-connection keepalive arm was not proven");
}

int main(int argc, char **argv)
{
    if (argc != 1 && !(argc == 4 && strcmp(argv[1], "--server") == 0)) {
        fprintf(stderr, "usage: %s [--server CERT KEY]\n", argv[0]);
        return 2;
    }

    test_client_opt_in();
    test_default_disabled();
    test_prefix_before_field_zero_calls();
    if (argc == 4 && strcmp(argv[1], "--server") == 0)
        test_server_accepted_connection(argv[2], argv[3]);
    if (failures) {
        fprintf(stderr, "%d check(s) failed\n", failures);
        return 1;
    }
    printf("PASS: pico_wt_managed_keepalive\n");
    return 0;
}
