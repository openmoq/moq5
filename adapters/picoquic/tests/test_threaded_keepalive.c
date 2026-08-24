/*
 * Deterministic proof that the raw threaded picoquic adapter arms QUIC keepalive
 * from moq_pq_threaded_cfg_t.keep_alive_interval_ms (Vojtech idle-publisher
 * report). No live handshake, no real relay, no 30s wall clock: the adapter's
 * keepalive arming is routed through a swappable MOQ_PQ_THREADED_TESTING seam
 * (mirroring the cert-installer seam), so a client create is driven and the seam
 * proves the call count, the non-NULL connection, and the converted microsecond
 * interval. Links the test-internals build of the adapter (never shipped).
 */
#include <moq/picoquic_threaded.h>

#include <picoquic.h>

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* The MOQ_PQ_THREADED_TESTING keepalive seam (deliberately NOT public). */
extern void (*moq_pq_threaded_test_keep_alive)(picoquic_cnx_t *, uint64_t);
extern unsigned moq_pq_threaded_test_keep_alive_calls;
extern picoquic_cnx_t *moq_pq_threaded_test_keep_alive_last_cnx;
extern uint64_t moq_pq_threaded_test_keep_alive_last_interval_us;

static int failures = 0;
#define CHECK(expr) do { if (!(expr)) { \
    fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
    failures++; } } while (0)

/* Recorder hook: capture only (the seam globals record count/cnx/interval), so
 * the real picoquic_enable_keep_alive is not invoked on the test connection. */
static void ka_noop(picoquic_cnx_t *cnx, uint64_t interval_us)
{
    (void)cnx; (void)interval_us;
}

static int stub_pump(moq_pq_threaded_t *t, moq_pq_threaded_lane_t *lane,
                     uint64_t now, void *ctx)
{ (void)t; (void)lane; (void)now; (void)ctx; return 0; }

/* Build a minimal CLIENT config that creates a real client cnx without a relay:
 * 127.0.0.1 resolves offline; insecure_skip_verify avoids needing a cert; the
 * network thread never connects, and stop/destroy tears it down immediately. */
static void client_cfg(moq_pq_threaded_cfg_t *cfg)
{
    moq_pq_threaded_cfg_init_sized(cfg, sizeof(*cfg));
    cfg->alloc = moq_alloc_default();
    cfg->perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg->host = "127.0.0.1";
    cfg->port = 34567;                 /* nothing listening; we never connect */
    cfg->insecure_skip_verify = true;  /* client: no cert needed */
    cfg->on_lane_pump = stub_pump;
}

static void reset_seam(void)
{
    moq_pq_threaded_test_keep_alive = ka_noop;
    moq_pq_threaded_test_keep_alive_calls = 0;
    moq_pq_threaded_test_keep_alive_last_cnx = NULL;
    moq_pq_threaded_test_keep_alive_last_interval_us = 0;
}

/* keep_alive_interval_ms = 1234 -> exactly one call, non-NULL cnx, 1234000 us. */
static void test_client_opt_in(void)
{
    reset_seam();
    moq_pq_threaded_cfg_t cfg;
    client_cfg(&cfg);
    cfg.keep_alive_interval_ms = 1234;

    moq_pq_threaded_t *t = NULL;
    moq_result_t rc = moq_pq_threaded_create(&cfg, &t);
    CHECK(rc == MOQ_OK);
    CHECK(moq_pq_threaded_test_keep_alive_calls == 1);
    CHECK(moq_pq_threaded_test_keep_alive_last_cnx != NULL);
    CHECK(moq_pq_threaded_test_keep_alive_last_interval_us == 1234000ull);
    if (t) { moq_pq_threaded_stop(t); moq_pq_threaded_destroy(t); }
}

/* Default (zero) -> keepalive disabled, zero calls. */
static void test_client_default_disabled(void)
{
    reset_seam();
    moq_pq_threaded_cfg_t cfg;
    client_cfg(&cfg);
    /* keep_alive_interval_ms left 0 by sized init */

    moq_pq_threaded_t *t = NULL;
    moq_result_t rc = moq_pq_threaded_create(&cfg, &t);
    CHECK(rc == MOQ_OK);
    CHECK(moq_pq_threaded_test_keep_alive_calls == 0);
    if (t) { moq_pq_threaded_stop(t); moq_pq_threaded_destroy(t); }
}

/* A struct_size ending BEFORE keep_alive_interval_ms must not read the field
 * (no garbage, zero calls) even if the trailing bytes are nonzero. */
static void test_prefix_before_field_zero_calls(void)
{
    reset_seam();
    moq_pq_threaded_cfg_t cfg;
    client_cfg(&cfg);
    cfg.keep_alive_interval_ms = 9999;  /* would-be garbage past the prefix */
    /* Pretend the caller was built before the field existed. */
    cfg.struct_size = (uint32_t)offsetof(moq_pq_threaded_cfg_t,
                                         keep_alive_interval_ms);

    moq_pq_threaded_t *t = NULL;
    moq_result_t rc = moq_pq_threaded_create(&cfg, &t);
    CHECK(rc == MOQ_OK);                /* old prefix accepted */
    CHECK(moq_pq_threaded_test_keep_alive_calls == 0);  /* field not read */
    if (t) { moq_pq_threaded_stop(t); moq_pq_threaded_destroy(t); }
}

int main(void)
{
    test_client_opt_in();
    test_client_default_disabled();
    test_prefix_before_field_zero_calls();
    if (failures) { fprintf(stderr, "%d check(s) failed\n", failures); return 1; }
    printf("ok\n");
    return 0;
}
