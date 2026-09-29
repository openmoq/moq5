/*
 * Service MsQuic keepalive default: the explicit raw-QUIC MsQuic endpoint path
 * must carry keep_alive_interval_ms = 15000 ms into moq_msquic_managed_create().
 *
 * The endpoint object set this binary links is compiled with
 * moq_msquic_managed_create renamed to the recorder below. The recorder
 * captures the config and forwards to the real adapter create from this TU, so
 * the production connect path still runs and the shipping library never sees
 * the seam.
 */

#include "endpoint_internal.h"
#include "test_support.h"

#include <moq/msquic_managed.h>

#include <stdlib.h>
#include <string.h>

static int failures = 0;

#define DEAD_PEER_URL_MSQUIC "moqt://127.0.0.1:14454"
#define SERVICE_KA_MS        15000u

static unsigned g_calls;
static uint32_t g_ka_ms;

moq_result_t moq_test_msquic_managed_create(
    const moq_msquic_managed_cfg_t *cfg, moq_msquic_managed_t **out);
moq_result_t moq_test_msquic_managed_create(
    const moq_msquic_managed_cfg_t *cfg, moq_msquic_managed_t **out)
{
    g_calls++;
    g_ka_ms = cfg->keep_alive_interval_ms;
    return moq_msquic_managed_create(cfg, out);
}

static moq_bytes_t B(const char *s)
{
    moq_bytes_t b = { (const uint8_t *)s, strlen(s) };
    return b;
}

int main(void)
{
    static const moq_version_t v18 = MOQ_VERSION_DRAFT_18;

    g_calls = 0;
    g_ka_ms = 0;

    moq_endpoint_cfg_t c;
    moq_endpoint_cfg_init_sized(&c, sizeof(c));
    c.url = B(DEAD_PEER_URL_MSQUIC);
    c.protocol = MOQ_TRANSPORT_PROTOCOL_RAW_QUIC;
    c.backend = MOQ_TRANSPORT_BACKEND_MSQUIC;
    c.insecure_skip_verify = true;
    c.versions.struct_size = sizeof(c.versions);
    c.versions.policy = MOQ_VERSION_POLICY_EXACT;
    c.versions.versions = &v18;
    c.versions.version_count = 1;

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
    printf("PASS: endpoint_msquic_keepalive_default\n");
    return 0;
}
