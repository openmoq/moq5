/*
 * Service picoquic keepalive default: the raw-threaded client the service
 * endpoint builds must carry keep_alive_interval_ms = 15000 ms into
 * moq_pq_threaded_create(). Resolution/config tests cannot see this -- deleting
 * the assignment in ep_create_pq() leaves everything else green -- so this test
 * observes the value the adapter create actually receives.
 *
 * How: the endpoint object set this binary links is compiled with
 * moq_pq_threaded_create renamed to the recorder below (a private, build-gated
 * seam -- nothing is exported from the shipping library, and the production
 * sources are unmodified). The recorder captures cfg->keep_alive_interval_ms and
 * forwards to the REAL moq_pq_threaded_create (this TU is NOT renamed), so the
 * whole real connect path still runs and the value asserted is the one the
 * adapter was handed.
 *
 * No peer: nothing listens on the port, so the connection never completes and is
 * torn down by destroy(). There is no elapsed-time assertion.
 */
#include "endpoint_internal.h"
#include "test_support.h"

#include <moq/picoquic_threaded.h>

#include <stdlib.h>
#include <string.h>

static int failures = 0;

/* Nothing listens here; create still runs its whole configuration path. */
#define DEAD_PEER_URL_QUIC "moqt://127.0.0.1:14453"
#define SERVICE_KA_MS      15000u

static unsigned g_calls;      /* how often create was reached */
static uint32_t g_ka_ms;      /* keep_alive_interval_ms it carried */

/* The symbol the endpoint object set calls in place of moq_pq_threaded_create. */
moq_result_t moq_test_pq_threaded_create(const moq_pq_threaded_cfg_t *cfg,
                                         moq_pq_threaded_t **out);
moq_result_t moq_test_pq_threaded_create(const moq_pq_threaded_cfg_t *cfg,
                                         moq_pq_threaded_t **out)
{
    g_calls++;
    g_ka_ms = cfg->keep_alive_interval_ms;
    /* This TU is not renamed, so this reaches the real adapter create. */
    return moq_pq_threaded_create(cfg, out);
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
    c.url = B(DEAD_PEER_URL_QUIC);
    c.protocol = MOQ_TRANSPORT_PROTOCOL_RAW_QUIC;
    c.insecure_skip_verify = true;   /* no peer, no cert: stay on the config path */

    moq_endpoint_t *ep = NULL;
    moq_result_t rc = moq_endpoint_connect(&c, &ep);
    /* A create failure here is a product/environment failure, not a pass. */
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
    MOQ_TEST_CHECK(ep != NULL);
    if (rc == MOQ_OK && ep != NULL) {
        /* Reached create exactly once, carrying the service default. */
        MOQ_TEST_CHECK_EQ_U64((uint64_t)g_calls, 1);
        MOQ_TEST_CHECK_EQ_U64((uint64_t)g_ka_ms, (uint64_t)SERVICE_KA_MS);
        moq_endpoint_stop(ep);
        moq_endpoint_destroy(ep);
    }

    if (failures) return 1;
    printf("PASS: endpoint_keepalive_default\n");
    return 0;
}
