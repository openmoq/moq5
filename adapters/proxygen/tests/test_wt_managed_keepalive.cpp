/* Deterministic production installation and caller-memory boundaries.
 * A numeric peer address is configured; the private probe stops before QUIC
 * start(), so these tests never dial, bind, or depend on packet/timer delivery.
 * Exact malloc extents also make prefix overreads detectable under ASan. */
#include <moq/proxygen_wt_managed.h>
#include "wt_managed_testing.h"
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>

static int failures;
#define TEST_CHECK(expr) do { if (!(expr)) { \
    std::fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #expr); \
    ++failures; } } while (0)
static int noop(moq_proxygen_wt_managed_t *, uint64_t, void *) { return 0; }
using cfg_t = moq_proxygen_wt_managed_cfg_t;
constexpr size_t field = offsetof(cfg_t, keep_alive_interval_ms);
constexpr size_t target_end = offsetof(cfg_t, target) + sizeof(cfg_t::target);
/* Historical sizeof: old last field rounded to the original struct alignment.
 * Unlike field's offset, this includes ONLY historical tail padding. */
constexpr size_t old_size = (target_end + alignof(cfg_t) - 1) & ~(alignof(cfg_t) - 1);
static_assert(field >= old_size);
static_assert(old_size == (sizeof(void *) == 8 ? 144 :
                          (alignof(uint64_t) == 8 ? 88 : 80)));

static void expect(const moq_proxygen_keepalive_probe &p, uint32_t interval)
{
    TEST_CHECK(p.observed);
    TEST_CHECK(p.enabled == (interval != 0));
    TEST_CHECK(p.interval_ms == interval);
    TEST_CHECK(p.bidi == 100);
    TEST_CHECK(p.uni == 100);
    TEST_CHECK(p.datagrams);
    TEST_CHECK(p.drop_old);
}

static void check(size_t extent, uint32_t interval, uint32_t wanted)
{
    std::printf("case extent=%zu request=%u expected=%u\n", extent, interval, wanted);
    cfg_t cfg;
    moq_proxygen_wt_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.host = "127.0.0.1";
    cfg.port = 443;
    cfg.on_pump = noop;
    cfg.keep_alive_interval_ms = interval;
    cfg.struct_size = (uint32_t)extent;
    /* Poison old tail padding AND reserved slots. Neither is readable input.
     * Full-field bytes retain interval; partial bytes deliberately stay live. */
    auto *bytes = static_cast<unsigned char *>(std::malloc(extent));
    TEST_CHECK(bytes != nullptr);
    if (!bytes) return;
    std::memset(bytes, 0xa5, extent);
    std::memcpy(bytes, &cfg, std::min(extent, sizeof(cfg)));
    if (extent > target_end)
        std::memset(bytes + target_end, 0xa5,
                    std::min(extent, field) - target_end);
    const std::vector<unsigned char> before(bytes, bytes + extent);
    moq_proxygen_keepalive_probe installed{};
    moq_proxygen_wt_test_set_keepalive_probe(&installed);
    moq_proxygen_wt_managed_t *m = nullptr;
    const auto rc = moq_proxygen_wt_managed_create(reinterpret_cast<const cfg_t *>(bytes), &m);
    TEST_CHECK(rc == MOQ_OK);
    TEST_CHECK(m != nullptr);
    if (m) {
        TEST_CHECK(moq_proxygen_wt_managed_stop(m) == MOQ_OK);
        expect(installed, wanted);
        moq_proxygen_wt_managed_destroy(m);
    }
    moq_proxygen_wt_test_set_keepalive_probe(nullptr);
    TEST_CHECK(std::memcmp(bytes, before.data(), extent) == 0);
    std::free(bytes);
}

static void initializer_canaries()
{
    /* Exact historic allocation plus separate canary runs: the former proves
     * the read bound under ASan; the latter detects writes in ordinary builds. */
    for (size_t size : {old_size, field, field + 1, field + 2, field + 3,
                        field + sizeof(uint32_t), sizeof(cfg_t)}) {
        auto *p = static_cast<unsigned char *>(std::malloc(size + 32));
        TEST_CHECK(p != nullptr);
        if (!p) continue;
        std::memset(p, 0xa5, size + 32);
        moq_proxygen_wt_managed_cfg_init_sized(reinterpret_cast<cfg_t *>(p + 16), size);
        uint32_t stamped = 0;
        std::memcpy(&stamped, p + 16, sizeof(stamped));
        TEST_CHECK(stamped == size);
        for (size_t i = 0; i != 16; ++i) {
            TEST_CHECK(p[i] == 0xa5);
            TEST_CHECK(p[16 + size + i] == 0xa5);
        }
        std::free(p);
    }
}

int main()
{
    initializer_canaries();
    check(old_size, UINT32_MAX, 0);   /* exact historical allocation */
    for (size_t n = 0; n != sizeof(uint32_t); ++n)
        check(field + n, UINT32_MAX, 0); /* EVERY incomplete field extent */
    check(field + sizeof(uint32_t), 15000, 15000); /* no final tail padding */
    check(sizeof(cfg_t), 0, 0);
    check(sizeof(cfg_t), 15000, 15000);
    check(sizeof(cfg_t), UINT32_MAX, UINT32_MAX);
    check(sizeof(cfg_t) + 32, 15000, 15000); /* genuinely oversized backing */
    if (failures) std::fprintf(stderr, "FAIL: %d failures\n", failures);
    return failures ? 1 : 0;
}
