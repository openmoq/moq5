#ifndef MOQR_CLI_LANEDEADLINE_H
#define MOQR_CLI_LANEDEADLINE_H

/*
 * Relay fill deadlines, folded into the transport's wait.
 *
 * A binding holds a draft-22 fill at an evidenced gap until a deadline
 * (moqr_bind_next_deadline_us) and must end it then, not at whatever pump
 * happens to come next: FILL_TIMEOUT allows a relay to wait less than the
 * subscriber asked, never more. Both managed facades fold a per-lane
 * application deadline into each lane's next wait, asking for that lane by
 * index from the lane's own doorbell, concurrently with other lanes' pumps.
 * That query therefore must not read binding state.
 *
 * Instead each lane publishes its own binding's deadline into its shard's
 * slot here, at the end of its own pump, and the query answers the slot of
 * the shard the asking lane drives. A lane never sees another shard's
 * deadline, so it neither wakes for one nor waits on one it cannot advance.
 * No wake is needed when a slot changes: only its owning lane writes it,
 * from inside that lane's pump, and both facades recompute the owning lane's
 * wait after every pump.
 *
 * Header-only and free of transport types, so the rule is unit-tested on its
 * own and every relay build that compiles the CLI picks it up unchanged.
 */

#include <stdatomic.h>
#include <stdint.h>

/* Slot capacity: one per shard. main.c asserts it equals the CLI lane cap. */
#define MOQR_CLI_LANE_DEADLINE_SLOTS 64u

typedef struct moqr_cli_lane_deadlines {
    /* slots in use; set once before any facade can call the callback */
    uint32_t count;
    /* CLOCK_MONOTONIC microseconds, UINT64_MAX for none. Each value stands
     * alone -- nothing else is published with it -- so relaxed ordering is
     * sufficient on both sides. */
    _Atomic uint64_t at_us[MOQR_CLI_LANE_DEADLINE_SLOTS];
} moqr_cli_lane_deadlines_t;

/* Every slot empty; `count` beyond the capacity is clamped to it. Call before
 * the facade that reads these is created. */
static inline void
moqr_cli_lane_deadlines_init(moqr_cli_lane_deadlines_t *d, uint32_t count)
{
    d->count = count < MOQR_CLI_LANE_DEADLINE_SLOTS
                   ? count
                   : MOQR_CLI_LANE_DEADLINE_SLOTS;
    for (uint32_t i = 0; i < MOQR_CLI_LANE_DEADLINE_SLOTS; i++) {
        atomic_init(&d->at_us[i], UINT64_MAX);
    }
}

/*
 * A lane's pump publishes its binding's next deadline, measured against the
 * instant that pump ran at.
 *
 * A deadline at or before `now_us` publishes as none. The pump that just ran
 * was that deadline's chance: a fill still held past it is waiting on
 * transport credit, whose own events wake the lane. Publishing the past
 * instant instead would make the lane's doorbell find a due deadline on each pass
 * and pump without pause.
 *
 * A slot outside `count` is ignored. The table's capacity bounds the slot
 * too: `count` is a plain field, so the accessors do not rely on init having
 * clamped it.
 */
static inline void
moqr_cli_lane_deadlines_publish(moqr_cli_lane_deadlines_t *d, uint32_t slot,
                                uint64_t deadline_us, uint64_t now_us)
{
    if (slot >= d->count || slot >= MOQR_CLI_LANE_DEADLINE_SLOTS) {
        return;
    }
    atomic_store_explicit(&d->at_us[slot],
                          deadline_us > now_us ? deadline_us : UINT64_MAX,
                          memory_order_relaxed);
}

/* The deadline published in `slot`, UINT64_MAX when none is pending or the
 * slot is outside `count` (UINT32_MAX, the CLI plan's "no shard", included).
 * Pure: one atomic load, no lock, no facade call -- what a facade's per-lane
 * deadline query may do. */
static inline uint64_t
moqr_cli_lane_deadlines_at(moqr_cli_lane_deadlines_t *d, uint32_t slot)
{
    if (slot >= d->count || slot >= MOQR_CLI_LANE_DEADLINE_SLOTS) {
        return UINT64_MAX;
    }
    return atomic_load_explicit(&d->at_us[slot], memory_order_relaxed);
}

#endif /* MOQR_CLI_LANEDEADLINE_H */
