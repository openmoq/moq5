/*
 * Deterministic proof that the managed proxygen facade supplies periodic pumps
 * while otherwise idle: the shipped 5 ms schedule_pump_soon/pump_once cadence
 * drives a modeled time-based service deadline (the catalog-refresh analog) with
 * NO explicit wakes after the initial scheduling action.
 *
 * The facade runs its PRODUCTION pump loop over a fake WebTransport + real
 * libmoq session/Adapter (via the compile-guarded moq_proxygen_wt_test_start
 * seam — no dial, no live network). on_pump models a deadline: it counts a
 * generation only when now_us >= target, then advances the target one interval
 * out. Several generations with no wakes prove the periodic pump both fires and
 * re-arms; a callback that returns nonzero proves clean termination (no later
 * pump or re-arm). Removing the final periodic re-arm in pump_once collapses the
 * first test to a single generation.
 */
#include "wt_managed_testing.h"
#include "fake_webtransport.h"

#include <moq/proxygen_wt_managed.h>
#include <moq/moq.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <thread>

using moq::wt::testing::FakeWebTransport;

static int g_fail = 0;
#define WT_CHECK(e) do { \
    if (!(e)) { std::fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #e); \
                g_fail++; } } while (0)

static constexpr uint64_t INTERVAL_US = 40000;   /* 40 ms modeled deadline */
static std::atomic<uint64_t> g_target{0};         /* absolute deadline; 0 = due now */
static std::atomic<int>      g_gens{0};           /* generations serviced */
static std::atomic<int>      g_exit_after{-1};    /* -1 = never; else exit at this gen */

/* Models media_sender's periodic refresh: a generation is a pump AT/AFTER the
 * deadline; the deadline then advances one interval. */
static int pump_cb(moq_proxygen_wt_managed_t *, uint64_t now_us, void *)
{
    uint64_t t = g_target.load(std::memory_order_relaxed);
    if (now_us >= t) {
        int n = g_gens.fetch_add(1, std::memory_order_relaxed) + 1;
        g_target.store(now_us + INTERVAL_US, std::memory_order_relaxed);
        int ex = g_exit_after.load(std::memory_order_relaxed);
        if (ex >= 0 && n >= ex) return 1;   /* request clean exit */
    }
    return 0;
}

static void cfg_init(moq_proxygen_wt_managed_cfg_t *c)
{
    moq_proxygen_wt_managed_cfg_init(c);
    c->alloc = moq_alloc_default();
    c->host = "127.0.0.1";   /* unused: the seam never dials */
    c->port = 4433;
    c->on_pump = pump_cb;
    c->send_request_capacity = true;
    c->initial_request_capacity = 16;
}

static bool await_gens(int need, int window_ms)
{
    for (int i = 0; i < window_ms; i += 20) {
        if (g_gens.load() >= need) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return g_gens.load() >= need;
}

/* The idle facade supplies periodic pumps: several deadline generations accrue
 * with no explicit wakes after the initial scheduling action. */
static void test_idle_periodic_pump()
{
    int before = g_fail;
    g_target.store(0);
    g_gens.store(0);
    g_exit_after.store(-1);

    FakeWebTransport fake;
    moq_proxygen_wt_managed_cfg_t c;
    cfg_init(&c);
    moq_proxygen_wt_managed_t *m = moq_proxygen_wt_test_start(&c, &fake);
    WT_CHECK(m != nullptr);
    if (m) {
        /* No wakes: only the 5 ms periodic pump can advance the count. */
        WT_CHECK(await_gens(6, 3000));
        WT_CHECK(!moq_proxygen_wt_managed_is_fatal(m));
        moq_proxygen_wt_managed_stop(m);
        moq_proxygen_wt_managed_destroy(m);
    }
    if (g_fail == before) std::printf("PASS: idle_periodic_pump\n");
}

/* Clean termination: once on_pump requests exit, no later periodic pump runs or
 * re-arms — the generation count freezes. */
static void test_clean_termination()
{
    int before = g_fail;
    g_target.store(0);
    g_gens.store(0);
    g_exit_after.store(3);

    FakeWebTransport fake;
    moq_proxygen_wt_managed_cfg_t c;
    cfg_init(&c);
    moq_proxygen_wt_managed_t *m = moq_proxygen_wt_test_start(&c, &fake);
    WT_CHECK(m != nullptr);
    if (m) {
        /* Wait until the facade tears itself down after the exit request. */
        bool closed = false;
        for (int i = 0; i < 150; i++) {
            if (moq_proxygen_wt_managed_wait(m, 20000) == MOQ_ERR_CLOSED) { closed = true; break; }
        }
        WT_CHECK(closed);
        int gens_at_exit = g_gens.load();
        WT_CHECK(gens_at_exit == 3);            /* exited on the requested generation */
        /* No pump may run after the exit request: the count stays frozen. */
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        WT_CHECK(g_gens.load() == gens_at_exit);
        moq_proxygen_wt_managed_stop(m);
        moq_proxygen_wt_managed_destroy(m);
    }
    if (g_fail == before) std::printf("PASS: clean_termination\n");
}

int main()
{
    test_idle_periodic_pump();
    test_clean_termination();

    if (g_fail == 0) { std::printf("PASS: proxygen_wt_managed_scheduler\n"); return 0; }
    std::fprintf(stderr, "%d failure(s)\n", g_fail);
    return 1;
}
