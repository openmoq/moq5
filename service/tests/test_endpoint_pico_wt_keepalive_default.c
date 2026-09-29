/*
 * Service WebTransport keepalive default: the pico_wt managed client the
 * endpoint builds must carry keep_alive_interval_ms = 15000 ms into
 * moq_pico_wt_managed_create(). Resolution/config tests cannot see this; this
 * test observes the value the adapter create actually receives.
 *
 * The endpoint object set this binary links is compiled with
 * moq_pico_wt_managed_create renamed to the recorder below. The recorder
 * captures the config and forwards to the real adapter create from this TU, so
 * the production connect path still runs and the shipping library never sees
 * the seam.
 */
#include "endpoint_internal.h"
#include "test_support.h"

#include <moq/pico_wt_managed.h>

#include <stdlib.h>
#include <string.h>

static int failures = 0;

#define DEAD_PEER_URL_WT "https://127.0.0.1:14455/moq"
#define SERVICE_KA_MS    15000u

static unsigned g_calls;
static uint32_t g_ka_ms;

moq_result_t moq_test_pico_wt_managed_create(
    const moq_pico_wt_managed_cfg_t *cfg, moq_pico_wt_managed_t **out);
moq_result_t moq_test_pico_wt_managed_create(
    const moq_pico_wt_managed_cfg_t *cfg, moq_pico_wt_managed_t **out)
{
    g_calls++;
    g_ka_ms = cfg->keep_alive_interval_ms;
    return moq_pico_wt_managed_create(cfg, out);
}

static moq_bytes_t B(const char *s)
{
    moq_bytes_t b = { (const uint8_t *)s, strlen(s) };
    return b;
}

int main(void)
{
    g_calls = 0;
    g_ka_ms = 0;

    moq_endpoint_cfg_t c;
    moq_endpoint_cfg_init_sized(&c, sizeof(c));
    c.url = B(DEAD_PEER_URL_WT);
    c.protocol = MOQ_TRANSPORT_PROTOCOL_WEBTRANSPORT;
    c.insecure_skip_verify = true;

    moq_endpoint_t *ep = NULL;
    moq_result_t rc = moq_endpoint_connect(&c, &ep);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
    MOQ_TEST_CHECK(ep != NULL);
    if (rc == MOQ_OK && ep != NULL) {
        MOQ_TEST_CHECK_EQ_U64((uint64_t)g_calls, 1);
        MOQ_TEST_CHECK_EQ_U64((uint64_t)g_ka_ms, (uint64_t)SERVICE_KA_MS);
        moq_endpoint_stop(ep);
        moq_endpoint_destroy(ep);
    }

    if (failures) return 1;
    printf("PASS: endpoint_pico_wt_keepalive_default\n");
    return 0;
}
