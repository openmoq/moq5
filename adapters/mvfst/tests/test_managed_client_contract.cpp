/*
 * Managed mvfst client contract.
 *
 * These are no-network checks through the adapter-local test-internals rail.
 * They pin two bugs reported from the service-tier mvfst publisher path:
 *
 *  - stop() must join the managed thread without freeing the client session;
 *    the service tier may still hold session-backed publisher state and destroy
 *    it after the endpoint has gone terminal.
 *  - the client facade must offer standard QUIC v1, not mvfst's default
 *    private version-first offer.
 */
#include <moq/mvfst.h>

#include "../src/mvfst_managed_testing.h"

#include <cstdint>
#include <cstdio>

static int g_fail = 0;

#define CHECK(expr) do { \
    if (!(expr)) { \
        std::fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        g_fail++; \
    } \
} while (0)

static int noop_pump(moq_mvfst_managed_t *, moq_mvfst_managed_lane_t *,
                     uint64_t, void *)
{
    return 0;
}

static moq_mvfst_managed_t *create_client(const char *host, int port)
{
    moq_mvfst_managed_cfg_t cfg;
    moq_mvfst_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = host;
    cfg.port = port;
    cfg.insecure_skip_verify = true;
    cfg.on_lane_pump = noop_pump;

    moq_mvfst_managed_t *m = nullptr;
    if (moq_mvfst_managed_create(&cfg, &m) != MOQ_OK) return nullptr;
    return m;
}

static void test_session_survives_stop()
{
    int before = g_fail;
    moq_mvfst_managed_t *m = create_client(nullptr, 0);
    CHECK(m != nullptr);
    if (!m) return;

    CHECK(moq_mvfst_managed_test_has_client_session(m));
    CHECK(moq_mvfst_managed_stop(m) == MOQ_OK);
    CHECK(moq_mvfst_managed_test_has_client_session(m));
    moq_mvfst_managed_destroy(m);

    if (g_fail == before)
        std::printf("PASS: mvfst_client_session_survives_stop\n");
}

static void test_client_offers_quic_v1_only()
{
    int before = g_fail;
    moq_mvfst_managed_t *m = create_client(nullptr, 0);
    CHECK(m != nullptr);
    if (!m) return;

    uint32_t versions[4] = {0, 0, 0, 0};
    size_t n = moq_mvfst_managed_test_client_quic_versions(
        m, versions, sizeof(versions) / sizeof(versions[0]));
    CHECK(n == 1);
    CHECK(versions[0] == 0x00000001u);

    moq_mvfst_managed_destroy(m);

    if (g_fail == before)
        std::printf("PASS: mvfst_client_quic_v1_only\n");
}

int main()
{
    test_session_survives_stop();
    test_client_offers_quic_v1_only();

    if (g_fail == 0) {
        std::printf("PASS: mvfst_managed_client_contract\n");
        return 0;
    }
    std::fprintf(stderr, "%d failure(s)\n", g_fail);
    return 1;
}
