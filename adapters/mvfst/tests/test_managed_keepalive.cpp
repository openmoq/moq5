/* Deterministic production installation and caller-memory boundaries.
 * A numeric peer address is configured; the private probe stops before QUIC
 * start(), so these tests never dial, bind, or depend on packet/timer delivery.
 * Exact malloc extents also make prefix overreads detectable under ASan. */
#include <moq/mvfst.h>
#include "../src/mvfst_managed_testing.h"
/* Reuse only the certificate generator; no loopback_pair is constructed. */
#include "support/mvfst_loopback_pair.hpp"
#include <unistd.h>
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
static int noop(moq_mvfst_managed_t *, moq_mvfst_managed_lane_t *, uint64_t, void *) { return 0; }
using cfg_t = moq_mvfst_managed_cfg_t;
constexpr size_t field = offsetof(cfg_t, keep_alive_interval_ms);
constexpr size_t target_end = offsetof(cfg_t, target) + sizeof(cfg_t::target);
/* Historical sizeof: old last field rounded to the original struct alignment.
 * Unlike field's offset, this includes ONLY historical tail padding. */
constexpr size_t old_size = (target_end + alignof(cfg_t) - 1) & ~(alignof(cfg_t) - 1);
static_assert(field >= old_size);
static_assert(old_size == (sizeof(void *) == 8 ? 208 :
                          (alignof(uint64_t) == 8 ? 136 : 128)));

static void expect(const moq_mvfst_keepalive_probe &p, uint32_t interval)
{
    TEST_CHECK(p.observed);
    TEST_CHECK(p.enabled == (interval != 0));
    TEST_CHECK(p.interval_ms == interval);
    TEST_CHECK(p.bidi == 100);
    TEST_CHECK(p.uni == 100);
    TEST_CHECK(p.datagrams);
    TEST_CHECK(p.drop_old);
    TEST_CHECK(p.ptos == 7);
    TEST_CHECK(p.rtt_us == 12345);
}

/* Existing mvfst fixtures generate a temporary P-256 certificate at runtime.
 * Only local PEM file IO is needed to enter the public server create path. */
struct cert_files {
    char cert[64] = "/tmp/moq_ka_cert_XXXXXX";
    char key[64] = "/tmp/moq_ka_key_XXXXXX";
    bool cert_created = false, key_created = false, ok = false;

    static bool save(int fd, const std::string &pem) {
        FILE *f = fdopen(fd, "wb");
        if (!f) { close(fd); return false; }
        bool written = std::fwrite(pem.data(), 1, pem.size(), f) == pem.size();
        return std::fclose(f) == 0 && written;
    }
    cert_files() {
        auto pem = moq::mvfst::test::generate_self_signed_cert();
        if (!pem.ok) return;
        int fd = mkstemp(cert);
        if (fd < 0) return;
        cert_created = true;
        if (!save(fd, pem.cert_pem)) return;
        fd = mkstemp(key);
        if (fd < 0) return;
        key_created = true;
        ok = save(fd, pem.key_pem);
    }
    ~cert_files() {
        if (key_created) unlink(key);
        if (cert_created) unlink(cert);
    }
};

static void check(size_t extent, uint32_t interval, uint32_t wanted,
                  const cert_files *server_files)
{
    std::printf("case role=%s extent=%zu request=%u expected=%u\n",
                server_files ? "server" : "client", extent, interval, wanted);
    cfg_t cfg;
    moq_mvfst_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.host = "127.0.0.1";
    cfg.port = server_files ? 0 : 443;
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    if (server_files) {
        cfg.perspective = MOQ_PERSPECTIVE_SERVER;
        cfg.cert_path = server_files->cert;
        cfg.key_path = server_files->key;
    }
    cfg.on_lane_pump = noop;
    cfg.max_num_ptos = 7;
    cfg.initial_rtt_us = 12345;
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
    moq_mvfst_keepalive_probe installed{};
    moq_mvfst_managed_test_set_keepalive_probe(&installed);
    moq_mvfst_managed_t *m = nullptr;
    const auto rc = moq_mvfst_managed_create(reinterpret_cast<const cfg_t *>(bytes), &m);
    TEST_CHECK(rc == MOQ_OK);
    TEST_CHECK(m != nullptr);
    if (m) {
        TEST_CHECK(moq_mvfst_managed_stop(m) == MOQ_OK);
        if (server_files) {
            /* Assert before handoff: it must not repair missing startup work. */
            TEST_CHECK(installed.server_registered);
            moq_mvfst_keepalive_probe factory{}, worker{};
            TEST_CHECK(moq_mvfst_managed_test_server_keepalive(m, &factory, &worker));
            expect(factory, wanted);
            expect(worker, wanted);
        } else {
            expect(installed, wanted);
        }
        moq_mvfst_managed_destroy(m);
    }
    moq_mvfst_managed_test_set_keepalive_probe(nullptr);
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
        moq_mvfst_managed_cfg_init_sized(reinterpret_cast<cfg_t *>(p + 16), size);
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
    cert_files files;
    TEST_CHECK(files.ok);
    if (!files.ok) return 1;
    auto check_both = [&files](size_t extent, uint32_t interval, uint32_t wanted) {
        check(extent, interval, wanted, nullptr);
        check(extent, interval, wanted, &files);
    };
    check_both(old_size, UINT32_MAX, 0);   /* exact historical allocation */
    for (size_t n = 0; n != sizeof(uint32_t); ++n)
        check_both(field + n, UINT32_MAX, 0); /* EVERY incomplete field extent */
    check_both(field + sizeof(uint32_t), 15000, 15000); /* no final tail padding */
    check_both(sizeof(cfg_t), 0, 0);
    check_both(sizeof(cfg_t), 15000, 15000);
    check_both(sizeof(cfg_t), UINT32_MAX, UINT32_MAX);
    check_both(sizeof(cfg_t) + 32, 15000, 15000); /* genuinely oversized backing */
    if (failures) std::fprintf(stderr, "FAIL: %d failures\n", failures);
    return failures ? 1 : 0;
}
