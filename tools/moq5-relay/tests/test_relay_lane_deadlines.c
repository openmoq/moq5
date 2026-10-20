/*
 * The table that carries each shard's fill deadline to the facade lane that
 * drives that shard.
 *
 * Pure: no transport, no clock. The end-to-end proof that a quiet track's
 * fill ends at its deadline is test_relay_fill_deadline; this pins the
 * arithmetic that test depends on.
 */
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "lanedeadline.h"

#include "../../../tests/unit/test_support.h"

/* -- nothing published reads as no deadline ------------------------------- */
static int
t_empty(void)
{
    int failures = 0;
    static moqr_cli_lane_deadlines_t d;

    /* storage that held stale values before init: init must clear every slot,
     * including those past `count` that a later init could bring into use */
    memset(&d, 0x5a, sizeof(d));
    moqr_cli_lane_deadlines_init(&d, 3);
    MOQ_TEST_CHECK_EQ_U64(d.count, 3);
    for (uint32_t i = 0; i < 3; i++) {
        MOQ_TEST_CHECK_EQ_U64(moqr_cli_lane_deadlines_at(&d, i), UINT64_MAX);
    }
    moqr_cli_lane_deadlines_init(&d, MOQR_CLI_LANE_DEADLINE_SLOTS);
    for (uint32_t i = 0; i < MOQR_CLI_LANE_DEADLINE_SLOTS; i++) {
        MOQ_TEST_CHECK_EQ_U64(moqr_cli_lane_deadlines_at(&d, i), UINT64_MAX);
    }

    /* zero slots in use: nothing is ever read or written */
    moqr_cli_lane_deadlines_init(&d, 0);
    moqr_cli_lane_deadlines_publish(&d, 0, 5000, 1000);
    MOQ_TEST_CHECK_EQ_U64(moqr_cli_lane_deadlines_at(&d, 0), UINT64_MAX);
    MOQ_TEST_CHECK_EQ_U64(atomic_load(&d.at_us[0]), UINT64_MAX);
    return failures;
}

/* -- a deadline the pump has already reached publishes as none ------------- */
static int
t_past_suppressed(void)
{
    int failures = 0;
    static moqr_cli_lane_deadlines_t d;

    moqr_cli_lane_deadlines_init(&d, 1);
    /* strictly in the future: published as itself */
    moqr_cli_lane_deadlines_publish(&d, 0, 1001, 1000);
    MOQ_TEST_CHECK_EQ_U64(moqr_cli_lane_deadlines_at(&d, 0), 1001);
    /* exactly the pump's own instant: that pump was the deadline's chance */
    moqr_cli_lane_deadlines_publish(&d, 0, 1000, 1000);
    MOQ_TEST_CHECK_EQ_U64(moqr_cli_lane_deadlines_at(&d, 0), UINT64_MAX);
    /* re-arm, then a deadline already behind the pump: cleared, not kept */
    moqr_cli_lane_deadlines_publish(&d, 0, 9000, 1000);
    MOQ_TEST_CHECK_EQ_U64(moqr_cli_lane_deadlines_at(&d, 0), 9000);
    moqr_cli_lane_deadlines_publish(&d, 0, 999, 1000);
    MOQ_TEST_CHECK_EQ_U64(moqr_cli_lane_deadlines_at(&d, 0), UINT64_MAX);
    /* the binding's own "none" stays none */
    moqr_cli_lane_deadlines_publish(&d, 0, UINT64_MAX, 1000);
    MOQ_TEST_CHECK_EQ_U64(moqr_cli_lane_deadlines_at(&d, 0), UINT64_MAX);
    /* a pump at instant 0 still publishes a later deadline */
    moqr_cli_lane_deadlines_publish(&d, 0, 1, 0);
    MOQ_TEST_CHECK_EQ_U64(moqr_cli_lane_deadlines_at(&d, 0), 1);
    return failures;
}

/* -- each slot answers for itself only ------------------------------------- */
static int
t_slots_independent(void)
{
    int failures = 0;
    static moqr_cli_lane_deadlines_t d;

    moqr_cli_lane_deadlines_init(&d, 4);
    moqr_cli_lane_deadlines_publish(&d, 0, 70000, 100);
    moqr_cli_lane_deadlines_publish(&d, 2, 40000, 100);
    moqr_cli_lane_deadlines_publish(&d, 3, 55000, 100);
    /* a lane is answered with its own shard's deadline, never an earlier one
     * that another shard published; a shard with none reads none */
    MOQ_TEST_CHECK_EQ_U64(moqr_cli_lane_deadlines_at(&d, 0), 70000);
    MOQ_TEST_CHECK_EQ_U64(moqr_cli_lane_deadlines_at(&d, 1), UINT64_MAX);
    MOQ_TEST_CHECK_EQ_U64(moqr_cli_lane_deadlines_at(&d, 2), 40000);
    MOQ_TEST_CHECK_EQ_U64(moqr_cli_lane_deadlines_at(&d, 3), 55000);

    /* one shard's fill ends: no other slot changes */
    moqr_cli_lane_deadlines_publish(&d, 2, UINT64_MAX, 40000);
    MOQ_TEST_CHECK_EQ_U64(moqr_cli_lane_deadlines_at(&d, 2), UINT64_MAX);
    MOQ_TEST_CHECK_EQ_U64(moqr_cli_lane_deadlines_at(&d, 0), 70000);
    MOQ_TEST_CHECK_EQ_U64(moqr_cli_lane_deadlines_at(&d, 3), 55000);
    /* each lane measures against its own pump instant: lane 0 publishing
     * after lane 3's deadline passed does not clear lane 3's slot */
    moqr_cli_lane_deadlines_publish(&d, 0, 80000, 60000);
    MOQ_TEST_CHECK_EQ_U64(moqr_cli_lane_deadlines_at(&d, 0), 80000);
    MOQ_TEST_CHECK_EQ_U64(moqr_cli_lane_deadlines_at(&d, 3), 55000);
    /* lane 3's own pump past its deadline clears it */
    moqr_cli_lane_deadlines_publish(&d, 3, 55000, 60000);
    MOQ_TEST_CHECK_EQ_U64(moqr_cli_lane_deadlines_at(&d, 3), UINT64_MAX);
    MOQ_TEST_CHECK_EQ_U64(moqr_cli_lane_deadlines_at(&d, 0), 80000);

    /* the last slot of a full table is its own */
    moqr_cli_lane_deadlines_init(&d, MOQR_CLI_LANE_DEADLINE_SLOTS);
    moqr_cli_lane_deadlines_publish(&d, 0, 900, 1);
    moqr_cli_lane_deadlines_publish(&d, MOQR_CLI_LANE_DEADLINE_SLOTS - 1u,
                                    800, 1);
    MOQ_TEST_CHECK_EQ_U64(moqr_cli_lane_deadlines_at(&d, 0), 900);
    MOQ_TEST_CHECK_EQ_U64(
        moqr_cli_lane_deadlines_at(&d, MOQR_CLI_LANE_DEADLINE_SLOTS - 1u), 800);
    return failures;
}

/* -- only the slots in use are read or written ------------------------------ */
static int
t_bounds(void)
{
    int failures = 0;
    static moqr_cli_lane_deadlines_t d;

    moqr_cli_lane_deadlines_init(&d, 2);
    /* a slot past `count` is ignored, never stored */
    moqr_cli_lane_deadlines_publish(&d, 2, 10, 1);
    moqr_cli_lane_deadlines_publish(&d, UINT32_MAX, 10, 1);
    MOQ_TEST_CHECK_EQ_U64(atomic_load(&d.at_us[2]), UINT64_MAX);
    /* and never read: storage past `count` holding a value still answers
     * none, as does the plan's "no shard" (UINT32_MAX) */
    atomic_store(&d.at_us[2], 10);
    MOQ_TEST_CHECK_EQ_U64(moqr_cli_lane_deadlines_at(&d, 2), UINT64_MAX);
    MOQ_TEST_CHECK_EQ_U64(moqr_cli_lane_deadlines_at(&d, UINT32_MAX),
                          UINT64_MAX);

    /* a count beyond the capacity is clamped, never overruns the table */
    moqr_cli_lane_deadlines_init(&d, MOQR_CLI_LANE_DEADLINE_SLOTS + 7u);
    MOQ_TEST_CHECK_EQ_U64(d.count, MOQR_CLI_LANE_DEADLINE_SLOTS);
    moqr_cli_lane_deadlines_publish(&d, MOQR_CLI_LANE_DEADLINE_SLOTS, 10, 1);
    MOQ_TEST_CHECK_EQ_U64(
        moqr_cli_lane_deadlines_at(&d, MOQR_CLI_LANE_DEADLINE_SLOTS),
        UINT64_MAX);

    /* `count` is a plain field: one set past the capacity without init still
     * never reaches beyond the table. The table is allocated at its exact
     * size, so an access past it is outside the allocation. */
    moqr_cli_lane_deadlines_t *h = malloc(sizeof(*h));
    MOQ_TEST_CHECK(h != NULL);
    if (h != NULL) {
        moqr_cli_lane_deadlines_init(h, MOQR_CLI_LANE_DEADLINE_SLOTS);
        h->count = MOQR_CLI_LANE_DEADLINE_SLOTS + 7u;
        moqr_cli_lane_deadlines_publish(h, MOQR_CLI_LANE_DEADLINE_SLOTS, 10, 1);
        MOQ_TEST_CHECK_EQ_U64(
            moqr_cli_lane_deadlines_at(h, MOQR_CLI_LANE_DEADLINE_SLOTS),
            UINT64_MAX);
        MOQ_TEST_CHECK_EQ_U64(
            moqr_cli_lane_deadlines_at(h, MOQR_CLI_LANE_DEADLINE_SLOTS - 1u),
            UINT64_MAX);
        free(h);
    }
    return failures;
}

int
main(void)
{
    int rc = 0;
    rc |= t_empty();
    rc |= t_past_suppressed();
    rc |= t_slots_independent();
    rc |= t_bounds();
    if (rc == 0) {
        printf("PASS: relay_lane_deadlines\n");
    }
    return rc == 0 ? 0 : 1;
}
