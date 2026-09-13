/*
 * Deterministic proof that the managed pico WT facade arms QUIC keepalive from
 * moq_pico_wt_managed_cfg_t.keep_alive_interval_ms. Same shape as the raw
 * threaded adapter's test_threaded_keepalive: no live handshake, no relay, no
 * 30 s wall clock -- the arming call is routed through the MOQ_PICO_WT_TESTING
 * seam, a client create is driven, and the seam records the call count, the
 * connection and the converted microsecond interval.
 *
 * Why it matters: a WebTransport publisher waiting for its first subscriber
 * sends nothing, and red5-moq-relay / moxygen idle a silent session out after
 * 30 s; the raw picoquic facade already armed keepalive, the WT one did not.
 *
 * Recompiles the managed sources with MOQ_PICO_WT_TESTING (never shipped).
 */
#include <moq/moq.h>
#include <moq/pico_wt_managed.h>

#include <picoquic.h>

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* The MOQ_PICO_WT_TESTING keepalive seam (deliberately NOT public). */
extern void (*moq_pico_wt_test_keep_alive)(picoquic_cnx_t *, uint64_t);
extern unsigned moq_pico_wt_test_keep_alive_calls;
extern picoquic_cnx_t *moq_pico_wt_test_keep_alive_last_cnx;
extern uint64_t moq_pico_wt_test_keep_alive_last_interval_us;

static int failures = 0;
#define CHECK(expr) do { if (!(expr)) { \
    fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
    failures++; } } while (0)

/* Recorder hook: capture only, so the real picoquic_enable_keep_alive is not
 * invoked on the test connection. */
static void ka_noop(picoquic_cnx_t *cnx, uint64_t interval_us)
{
    (void)cnx; (void)interval_us;
}

static int stub_pump(moq_pico_wt_managed_t *m, uint64_t now, void *ctx)
{ (void)m; (void)now; (void)ctx; return 0; }

/* Minimal CLIENT config that creates a real client cnx without a relay:
 * 127.0.0.1 resolves offline, nothing listens on the port, the network thread
 * never connects, and stop/destroy tear it down. */
static void client_cfg(moq_pico_wt_managed_cfg_t *cfg, size_t size)
{
    moq_pico_wt_managed_cfg_init_sized(cfg, size);
    cfg->alloc = moq_alloc_default();
    cfg->perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg->host = "127.0.0.1";
    cfg->port = 34567;
    cfg->insecure_skip_verify = true;
    cfg->on_pump = stub_pump;
}

static void reset_seam(void)
{
    moq_pico_wt_test_keep_alive = ka_noop;
    moq_pico_wt_test_keep_alive_calls = 0;
    moq_pico_wt_test_keep_alive_last_cnx = NULL;
    moq_pico_wt_test_keep_alive_last_interval_us = 0;
}

/* keep_alive_interval_ms = 1234 -> exactly one call, non-NULL cnx, 1234000 us. */
static void test_client_opt_in(void)
{
    reset_seam();
    moq_pico_wt_managed_cfg_t cfg;
    client_cfg(&cfg, sizeof(cfg));
    cfg.keep_alive_interval_ms = 1234;

    moq_pico_wt_managed_t *m = NULL;
    moq_result_t rc = moq_pico_wt_managed_create(&cfg, &m);
    CHECK(rc == MOQ_OK);
    CHECK(moq_pico_wt_test_keep_alive_calls == 1);
    CHECK(moq_pico_wt_test_keep_alive_last_cnx != NULL);
    CHECK(moq_pico_wt_test_keep_alive_last_interval_us == 1234000ull);
    if (m) { moq_pico_wt_managed_stop(m); moq_pico_wt_managed_destroy(m); }
}

/* Default (0) -> keepalive is never armed. */
static void test_client_default_off(void)
{
    reset_seam();
    moq_pico_wt_managed_cfg_t cfg;
    client_cfg(&cfg, sizeof(cfg));

    moq_pico_wt_managed_t *m = NULL;
    moq_result_t rc = moq_pico_wt_managed_create(&cfg, &m);
    CHECK(rc == MOQ_OK);
    CHECK(moq_pico_wt_test_keep_alive_calls == 0);
    if (m) { moq_pico_wt_managed_stop(m); moq_pico_wt_managed_destroy(m); }
}

/* ABI: a caller whose struct_size predates the field never has it read, even
 * with a nonzero value sitting in the (to it, trailing) memory. */
static void test_short_struct_size_ignores_field(void)
{
    reset_seam();
    moq_pico_wt_managed_cfg_t cfg;
    client_cfg(&cfg, offsetof(moq_pico_wt_managed_cfg_t, keep_alive_interval_ms));
    cfg.keep_alive_interval_ms = 1234;   /* beyond the declared size */

    moq_pico_wt_managed_t *m = NULL;
    moq_result_t rc = moq_pico_wt_managed_create(&cfg, &m);
    CHECK(rc == MOQ_OK);
    CHECK(moq_pico_wt_test_keep_alive_calls == 0);
    if (m) { moq_pico_wt_managed_stop(m); moq_pico_wt_managed_destroy(m); }
}

int main(void)
{
    test_client_opt_in();
    test_client_default_off();
    test_short_struct_size_ignores_field();
    if (failures) {
        fprintf(stderr, "%d failure(s)\n", failures);
        return 1;
    }
    puts("pico_wt managed keepalive: OK");
    return 0;
}
