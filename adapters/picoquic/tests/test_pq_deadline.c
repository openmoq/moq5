/* Deterministic check of the facade's application-deadline fold, without a live
 * loop: a served instant is dropped per candidate before the minimum (so a
 * served global never masks a still-pending lane deadline, in either order),
 * and the wait an absolute deadline yields is clamped so a far future never
 * wraps to a negative, busy-spinning value. White-box: links the threaded
 * adapter's test-internals archive for the two entrypoints and the facade
 * struct layout. */
#include "../pq_threaded_internal.h"

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

extern uint64_t moq_pq_threaded_test_app_deadline(moq_pq_threaded_t *t);
extern int64_t  moq_pq_threaded_test_deadline_until(uint64_t deadline_us,
                                                    uint64_t now_us);
extern int64_t  moq_pq_threaded_test_lane_deadline_delta(int64_t delta,
                                                         uint64_t deadline,
                                                         uint64_t served,
                                                         uint64_t now);

static int failures;
#define CHECK(c) do { if (!(c)) { \
    printf("CHECK failed: %s (%s:%d)\n", #c, __FILE__, __LINE__); failures++; } \
} while (0)

static uint64_t g_global, g_lane;
static uint64_t global_fn(void *ctx) { (void)ctx; return g_global; }
static uint64_t lane_fn(void *ctx, uint32_t lane)
{
    (void)ctx;
    return lane == 0 ? g_lane : UINT64_MAX;
}

int main(void)
{
    setvbuf(stdout, NULL, _IONBF, 0);

    moq_pq_threaded_t t;
    memset(&t, 0, sizeof(t));
    t.app_deadline_us = global_fn;
    t.app_lane_deadline_us = lane_fn;
    t.own_loop = false;            /* one-lane facade folds lane 0 */
    t.lane.index = 0;

    /* A served global must not mask a later lane deadline still pending. */
    t.deadline_served = 100;
    g_global = 100; g_lane = 200;
    uint64_t d = moq_pq_threaded_test_app_deadline(&t);
    printf("deadline: served-global -> %" PRIu64 "\n", d);
    CHECK(d == 200);

    /* The mirror: a served lane must not mask a pending global. */
    t.deadline_served = 100;
    g_global = 200; g_lane = 100;
    CHECK(moq_pq_threaded_test_app_deadline(&t) == 200);

    /* Both pending: the earlier wins. */
    t.deadline_served = 0;
    g_global = 300; g_lane = 150;
    CHECK(moq_pq_threaded_test_app_deadline(&t) == 150);

    /* Both already served: nothing to offer, no spin. */
    t.deadline_served = 300;
    g_global = 100; g_lane = 200;
    CHECK(moq_pq_threaded_test_app_deadline(&t) == UINT64_MAX);

    /* A group lane folds its lane deadline in the group loop, so the facade
     * query skips the lane and only the global applies. */
    t.own_loop = true;
    t.deadline_served = 0;
    g_global = 400; g_lane = 150;
    uint64_t dg = moq_pq_threaded_test_app_deadline(&t);
    printf("deadline: group-lane own_loop -> %" PRIu64 "\n", dg);
    CHECK(dg == 400);

    /* The wait clamps: past -> 0, future -> delta, unbounded -> INT64_MAX, so a
     * far deadline never wraps to a negative, busy-spinning wait. */
    CHECK(moq_pq_threaded_test_deadline_until(100, 200) == 0);
    CHECK(moq_pq_threaded_test_deadline_until(300, 100) == 200);
    CHECK(moq_pq_threaded_test_deadline_until(UINT64_MAX - 1, 0) == INT64_MAX);

    /* The group loop's per-lane deadline fold bounds the wait the same way: a
     * near deadline shortens it, an already-served one is skipped, a due one
     * yields no wait, and a far future stays bounded rather than wrapping
     * negative and busy-polling. */
    CHECK(moq_pq_threaded_test_lane_deadline_delta(5000, 200, 0, 100) == 100);
    CHECK(moq_pq_threaded_test_lane_deadline_delta(5000, 100, 100, 50) == 5000);
    CHECK(moq_pq_threaded_test_lane_deadline_delta(5000, 100, 0, 200) == 0);
    CHECK(moq_pq_threaded_test_lane_deadline_delta(5000, UINT64_MAX - 1, 0, 100) == 5000);

    if (failures) {
        printf("FAILED: %d check(s) [pq_deadline]\n", failures);
        return 1;
    }
    printf("PASS: pq_deadline\n");
    return 0;
}
