/*
 * Application service-deadline fold for the managed mvfst facade.
 *
 * moq_mvfst_managed::compute_earliest_deadline() is the single source of truth
 * for both the client deadline_to and the server server_deadline_to reschedules:
 * it folds the application deadline (min with the session deadlines, only while
 * the pump is live) into the value the one-shot AsyncTimeout is armed from. These
 * tests drive it deterministically through the test-internals seam
 * (moq_mvfst_managed_test_earliest_deadline / _test_set_running), on an idle
 * lifecycle-only facade with no sessions — so the read reflects only the fold and
 * is safe off the network thread. This suite covers the fold value itself,
 * cancellation, rearm, the terminal guard, and the whole-block ABI gate; it does
 * NOT exercise the EventBase timers. That each distinct AsyncTimeout path (client
 * deadline_to and server server_deadline_to) actually fires from the folded value
 * and reschedules is covered separately by test_managed_app_deadline_timer.cpp.
 */
#include <moq/mvfst.h>

#include "../src/mvfst_managed_testing.h"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <thread>

static int g_fail = 0;
#define MVFST_CHECK(e) do { \
    if (!(e)) { std::fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #e); \
                g_fail++; } } while (0)

/* Controllable app deadline: a pure cached read (like media_sender's
 * refresh_wake_deadline_us), plus invocation + context observation. */
static std::atomic<uint64_t> g_target{UINT64_MAX};
static std::atomic<int>      g_calls{0};
static std::atomic<void *>   g_last_ctx{nullptr};
static uint64_t app_cb(void *ctx)
{
    g_calls.fetch_add(1, std::memory_order_relaxed);
    g_last_ctx.store(ctx, std::memory_order_relaxed);
    return g_target.load(std::memory_order_relaxed);
}

static int noop_pump(moq_mvfst_managed_t *, moq_mvfst_managed_lane_t *,
                     uint64_t, void *) { return 0; }

/* Idle lifecycle-only client (host == NULL: no transport, running == true) with
 * app_deadline wired at a caller-chosen struct_size (to drive the ABI gate). */
static moq_mvfst_managed_t *mk_idle(size_t ssz, void *dl_ctx)
{
    moq_mvfst_managed_cfg_t cfg;
    moq_mvfst_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = nullptr;                 /* lifecycle-only: no EventBase */
    cfg.on_lane_pump = noop_pump;
    cfg.app_deadline_us = app_cb;
    cfg.app_deadline_ctx = dl_ctx;
    cfg.struct_size = static_cast<uint32_t>(ssz);
    moq_mvfst_managed_t *m = nullptr;
    if (moq_mvfst_managed_create(&cfg, &m) != MOQ_OK) return nullptr;
    return m;
}

/* Fold + cancel + rearm (earlier/later) + full-size ctx delivery. */
static void test_fold_cancel_rearm()
{
    int ctx_marker = 0;
    g_target.store(UINT64_MAX);
    g_calls.store(0);
    moq_mvfst_managed_t *m = mk_idle(sizeof(moq_mvfst_managed_cfg_t), &ctx_marker);
    MVFST_CHECK(m != nullptr);
    if (!m) return;

    /* No app deadline, no session -> nothing pending. */
    g_target.store(UINT64_MAX);
    MVFST_CHECK(moq_mvfst_managed_test_earliest_deadline(m) == UINT64_MAX);

    /* A finite deadline is folded through verbatim (no session competes). */
    const uint64_t A = 5ull * 1000000;   /* absolute values; no session min */
    g_target.store(A);
    MVFST_CHECK(moq_mvfst_managed_test_earliest_deadline(m) == A);
    /* callback + context both arrived intact */
    MVFST_CHECK(g_last_ctx.load() == &ctx_marker);
    MVFST_CHECK(g_calls.load() > 0);

    /* Rearm to an EARLIER deadline, then a LATER one — each replaces. */
    const uint64_t EARLIER = 100ull * 1000;
    g_target.store(EARLIER);
    MVFST_CHECK(moq_mvfst_managed_test_earliest_deadline(m) == EARLIER);
    const uint64_t LATER = 9ull * 1000000;
    g_target.store(LATER);
    MVFST_CHECK(moq_mvfst_managed_test_earliest_deadline(m) == LATER);

    /* UINT64_MAX cancels: nothing to arm. */
    g_target.store(UINT64_MAX);
    MVFST_CHECK(moq_mvfst_managed_test_earliest_deadline(m) == UINT64_MAX);

    moq_mvfst_managed_stop(m);
    moq_mvfst_managed_destroy(m);
    if (g_fail == 0) std::printf("PASS: app_deadline_fold_cancel_rearm\n");
}

/* Terminal/stopping pump must not consult the callback or arm a timer. */
static void test_terminal_no_callback()
{
    int before = g_fail;
    int ctx_marker = 0;
    g_target.store(3ull * 1000000);      /* a finite deadline is armed... */
    moq_mvfst_managed_t *m = mk_idle(sizeof(moq_mvfst_managed_cfg_t), &ctx_marker);
    MVFST_CHECK(m != nullptr);
    if (!m) return;

    /* ...but once the facade latches stopping, the fold drops the app deadline
     * and never invokes the callback. Clearing running also exits the
     * lifecycle-only loop, so no background pump races the count. */
    moq_mvfst_managed_test_set_running(m, false);
    std::this_thread::sleep_for(std::chrono::milliseconds(30)); /* let the loop settle */
    int calls0 = g_calls.load();
    MVFST_CHECK(moq_mvfst_managed_test_earliest_deadline(m) == UINT64_MAX);
    MVFST_CHECK(g_calls.load() == calls0);   /* callback not consulted */

    moq_mvfst_managed_test_set_running(m, true);   /* restore for clean teardown */
    moq_mvfst_managed_stop(m);
    moq_mvfst_managed_destroy(m);
    if (g_fail == before) std::printf("PASS: app_deadline_terminal_no_callback\n");
}

/* Whole-block ABI gate. A prefix ending after app_deadline_us (one field short
 * of the block) must NOT read app_deadline_ctx: the callback is dropped, so the
 * fold ignores it and the callback is never consulted. Additionally causal under
 * ASan: a config allocated to exactly that prefix over-reads app_deadline_ctx if
 * the gate keys on app_deadline_us instead (heap-buffer-overflow). */
static void test_whole_block_gate()
{
    int before = g_fail;
    int ctx_marker = 0;

    /* (a) Logic: partial struct_size on a full cfg -> callback dropped. */
    g_target.store(7ull * 1000000);
    g_calls.store(0);
    size_t partial = offsetof(moq_mvfst_managed_cfg_t, app_deadline_ctx);
    moq_mvfst_managed_t *m = mk_idle(partial, &ctx_marker);
    MVFST_CHECK(m != nullptr);
    if (m) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        int c0 = g_calls.load();
        MVFST_CHECK(moq_mvfst_managed_test_earliest_deadline(m) == UINT64_MAX);
        MVFST_CHECK(g_calls.load() == c0);   /* callback dropped by the gate */
        moq_mvfst_managed_stop(m);
        moq_mvfst_managed_destroy(m);
    }

    /* (b) ASan-causal: drive create()'s config-import with a config allocated to
     * exactly the prefix ending at app_deadline_us. The correct gate never reads
     * app_deadline_ctx; a gate keyed on app_deadline_us reads it past the
     * allocation. */
    size_t prefix = offsetof(moq_mvfst_managed_cfg_t, app_deadline_us) +
                    sizeof(reinterpret_cast<moq_mvfst_managed_cfg_t *>(0)->app_deadline_us);
    unsigned char *raw = static_cast<unsigned char *>(std::malloc(prefix));
    MVFST_CHECK(raw != nullptr);
    if (raw) {
        std::memset(raw, 0, prefix);
        auto *cfg = reinterpret_cast<moq_mvfst_managed_cfg_t *>(raw);
        cfg->struct_size = static_cast<uint32_t>(prefix);
        cfg->alloc = moq_alloc_default();
        cfg->perspective = MOQ_PERSPECTIVE_CLIENT;
        cfg->host = nullptr;
        cfg->on_lane_pump = noop_pump;
        cfg->app_deadline_us = app_cb;
        /* app_deadline_ctx is beyond the allocation -- intentionally unset. */
        moq_mvfst_managed_t *pm = nullptr;
        moq_result_t rc = moq_mvfst_managed_create(cfg, &pm);
        if (rc == MOQ_OK && pm) {
            moq_mvfst_managed_stop(pm);
            moq_mvfst_managed_destroy(pm);
        }
        std::free(raw);
    }

    if (g_fail == before) std::printf("PASS: app_deadline_whole_block_gate\n");
}

int main()
{
    test_fold_cancel_rearm();
    test_terminal_no_callback();
    test_whole_block_gate();

    if (g_fail == 0) { std::printf("PASS: mvfst_managed_app_deadline\n"); return 0; }
    std::fprintf(stderr, "%d failure(s)\n", g_fail);
    return 1;
}
