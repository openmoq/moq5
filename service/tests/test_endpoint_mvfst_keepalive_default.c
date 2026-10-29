/*
 * Service mvfst keepalive default: the raw-QUIC mvfst endpoint path must carry
 * keep_alive_interval_ms = 15000 ms into moq_mvfst_managed_create().
 *
 * The endpoint object set this binary links is compiled with
 * moq_mvfst_managed_create renamed to the recorder below. The recorder
 * captures the config and refuses it, so no transport starts and the
 * production connect path runs up to the adapter; the shipping library never
 * sees the seam.
 */

#include "endpoint_internal.h"
#include "test_support.h"

#include <moq/mvfst.h>

#include <stdlib.h>
#include <string.h>

static int failures = 0;

#define DEAD_PEER_URL_MVFST "moqt://127.0.0.1:14456"
#define SERVICE_KA_MS       15000u

static unsigned g_calls;
static uint32_t g_ka_ms;
static uint32_t g_struct_size;

moq_result_t moq_test_mvfst_managed_create(
    const moq_mvfst_managed_cfg_t *cfg, moq_mvfst_managed_t **out);
moq_result_t moq_test_mvfst_managed_create(
    const moq_mvfst_managed_cfg_t *cfg, moq_mvfst_managed_t **out)
{
    g_calls++;
    g_ka_ms = cfg->keep_alive_interval_ms;
    g_struct_size = cfg->struct_size;
    *out = NULL;
    return MOQ_ERR_UNSUPPORTED;
}

static moq_bytes_t B(const char *s)
{
    moq_bytes_t b = { (const uint8_t *)s, strlen(s) };
    return b;
}

int main(void)
{
    static const moq_version_t v18 = MOQ_VERSION_DRAFT_18;

    moq_endpoint_cfg_t c;
    moq_endpoint_cfg_init_sized(&c, sizeof(c));
    c.url = B(DEAD_PEER_URL_MVFST);
    c.protocol = MOQ_TRANSPORT_PROTOCOL_RAW_QUIC;
    c.backend = MOQ_TRANSPORT_BACKEND_MVFST;
    c.insecure_skip_verify = true;
    c.versions.struct_size = sizeof(c.versions);
    c.versions.policy = MOQ_VERSION_POLICY_EXACT;
    c.versions.versions = &v18;
    c.versions.version_count = 1;

    moq_endpoint_t *ep = NULL;
    moq_result_t rc = moq_endpoint_connect(&c, &ep);
    MOQ_TEST_CHECK(rc != MOQ_OK);
    MOQ_TEST_CHECK(ep == NULL);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)g_calls, 1);
    MOQ_TEST_CHECK(g_struct_size >=
                   offsetof(moq_mvfst_managed_cfg_t, keep_alive_interval_ms) +
                   sizeof(uint32_t));
    MOQ_TEST_CHECK_EQ_U64((uint64_t)g_ka_ms, (uint64_t)SERVICE_KA_MS);
    if (ep != NULL)
        moq_endpoint_destroy(ep);

    if (failures) return 1;
    printf("PASS: endpoint_mvfst_keepalive_default\n");
    return 0;
}
