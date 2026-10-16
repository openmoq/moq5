/*
 * Relay-core time deadlines for the lane runtimes.
 *
 * Each lane's core has time-driven work (upstream linger, warm-track idle
 * retention) that only runs when the lane pumps. After every pump a lane
 * publishes its core's next deadline (moqr_core_next_deadline_us) as an
 * absolute time; the transport facade's app_deadline_us query returns the
 * earliest one among its lanes, and each facade wakes a lane and pumps once
 * that time has passed (msquic: query, sample now, pump when due; WebTransport:
 * fold at pump time, pump when the stored time passes). So an idle lane still
 * wakes, pumps and ticks on time.
 *
 * The floor is applied when a lane publishes, never when the facade asks: a
 * published deadline is at least MOQR_CLI_DEADLINE_REWAIT_US after the pump
 * that published it. That keeps a "tick owed now" (the core's 0) from making
 * the owning lane pump back to back, while every published value is an
 * absolute time that does fall due. The owner is late by at most one re-wait,
 * far below the retention bounds this serves.
 *
 * The query runs on the facade's lane threads, so it reads only these atomics
 * (never a core: cores are lane-confined), and it cannot tell which lane is
 * asking: every lane of a facade sees the facade-wide earliest deadline. When
 * that deadline is due, a sibling lane with nothing of its own due pumps too,
 * until the owning lane (due at the same instant, on its own thread) pumps and
 * republishes. Exact per-lane deadlines would need a lane index in the
 * adapters' app_deadline_us callback.
 */
#ifndef MOQR_CLI_DEADLINE_H
#define MOQR_CLI_DEADLINE_H

#include <stdatomic.h>
#include <stdint.h>

#define MOQR_CLI_DEADLINE_REWAIT_US 1000u

/* A facade's lanes: deadlines at[first .. first + count). */
typedef struct moqr_cli_deadline_range {
    _Atomic uint64_t *at;
    uint32_t          first;
    uint32_t          count;
} moqr_cli_deadline_range_t;

static inline void
moqr_cli_deadline_clear(_Atomic uint64_t *at, uint32_t n)
{
    for (uint32_t i = 0; i < n; i++) {
        atomic_store_explicit(&at[i], UINT64_MAX, memory_order_relaxed);
    }
}

/* A lane, right after the pump at `pump_now_us`, publishes its core's next
 * deadline `next_us` (UINT64_MAX = none), floored to one re-wait after the
 * pump. */
static inline void
moqr_cli_deadline_publish(_Atomic uint64_t *slot, uint64_t next_us,
                          uint64_t pump_now_us)
{
    uint64_t floor = pump_now_us + MOQR_CLI_DEADLINE_REWAIT_US;
    uint64_t v = next_us == UINT64_MAX ? UINT64_MAX
                 : next_us < floor     ? floor
                                       : next_us;
    atomic_store_explicit(slot, v, memory_order_relaxed);
}

/* The earliest published deadline of a facade's lanes; UINT64_MAX = none. */
static inline uint64_t
moqr_cli_deadline_fold(const moqr_cli_deadline_range_t *r)
{
    uint64_t dl = UINT64_MAX;
    for (uint32_t i = 0; i < r->count; i++) {
        uint64_t v = atomic_load_explicit(&r->at[r->first + i],
                                          memory_order_relaxed);
        if (v < dl) {
            dl = v;
        }
    }
    return dl;
}

/* The facades' app_deadline_us signature; ctx is a range. Values are in the
 * pump's now_us clock domain, which is the facades' own (CLOCK_MONOTONIC
 * microseconds). */
static inline uint64_t
moqr_cli_deadline_query(void *ctx)
{
    return moqr_cli_deadline_fold((const moqr_cli_deadline_range_t *)ctx);
}

#endif /* MOQR_CLI_DEADLINE_H */
