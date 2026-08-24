#ifndef MSQ_TEST_SEAMS_H
#define MSQ_TEST_SEAMS_H

/*
 * Test-only seams over the managed MsQuic facade, compiled solely under
 * MOQ_MSQUIC_TESTING and never installed. Everything here is either pure
 * observation (snapshots read under the lane lock and change nothing) or an
 * explicit driver over the SAME production step the doorbell thread runs, so
 * a single-threaded test drives production transitions rather than a model
 * of them.
 */

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include <msquic.h>

#include <moq/msquic_managed.h>

/*
 * Borrow the API table instead of calling MsQuicOpen2, and never MsQuicClose
 * it: a test can stand the whole facade on a fake table with no library, no
 * socket and no worker thread. Read once, at create.
 */
extern const QUIC_API_TABLE *moq_msq_test_api_override;

/*
 * Do not spawn doorbell threads at create; every doorbell iteration is then
 * driven explicitly through moq_msq_test_lane_step, and teardown must remain
 * valid with no worker present. Read once, at create.
 */
extern bool moq_msq_test_no_doorbell;

/* The outcome of one production doorbell iteration. */
typedef enum {
    MOQ_MSQ_TEST_STEP_PUMPED = 0,
    MOQ_MSQ_TEST_STEP_TICKED = 1,
    MOQ_MSQ_TEST_STEP_STOP = 2,
    MOQ_MSQ_TEST_STEP_IDLE = 3,
} moq_msq_test_step_t;

/*
 * An external snapshot is about to take this lane's mutex. Lets a test
 * rendezvous exactly at the lock boundary the snapshot serializes on, so a
 * concurrency proof can order a writer against it deterministically instead
 * of hoping the scheduler produces the interleaving.
 */
extern void (*moq_msq_test_snapshot_pre_lock)(
    moq_msquic_managed_lane_t *lane);

/* Run exactly one production doorbell iteration on the calling thread. */
moq_msq_test_step_t moq_msq_test_lane_step(moq_msquic_managed_lane_t *lane);

/*
 * Read-only, current-children-only snapshot taken under the lane lock. No
 * probe here mutates anything: terminal state is read through the bridge's
 * own facts, never through an acknowledgment call.
 */
typedef struct moq_msq_test_child_row {
    const void *id;            /* stable identity, for correlation only  */
    void       *user;          /* the application's conn_user tag        */
    bool        shutdown_complete;
    bool        reapable;
    bool        app_terminal_acked;
    bool        terminal_enqueued;  /* SESSION_CLOSED reached the queue  */
    bool        terminal_observed;  /* ... and a poll transferred it     */
    bool        has_events;         /* session events still queued       */
    bool        bridge_fatal;       /* the bridge latched a first fatal  */
    uint64_t    bridge_fatal_code;  /* that first cause (0 when none)    */
    uint32_t    close_feed_commits; /* feeds that crossed the pending/fed
                                     * guard -- exactly-once observable  */
} moq_msq_test_child_row_t;

typedef struct moq_msq_test_lane_row {
    bool     doorbell_running;
    bool     wake_pending;
    bool     pump_pending;
    bool     ext_wake;
    bool     pump_exit;
    bool     lane_stop;        /* this lane was asked to quiesce         */
    bool     facade_stop;      /* the facade-wide stop latch             */
    bool     in_sweep;
    bool     in_lane_pump;
    uint64_t bell_gen;
    size_t   conn_count;
    uint64_t pump_sweeps;      /* pre-suppressor count, NOT app callbacks */
    uint64_t service_passes;
    uint64_t deadline_sweeps;
    uint64_t idle_cap_wakes;
} moq_msq_test_lane_row_t;

/*
 * Fills *lane_row and up to `cap` current-child rows; returns the number of
 * children actually linked, which MAY EXCEED cap. A caller seeing that must
 * treat the picture as incomplete and fail closed rather than read a
 * silently truncated set.
 */
/* Append one LIVE, IDLE child through the production construction and
 * lane-membership path: no close, no terminal, no reapability latch, no pump
 * arm. The population the no-work deadline scan walks. */
bool moq_msq_test_lane_inject_idle_child(moq_msquic_managed_lane_t *lane);

size_t moq_msq_test_lane_snapshot(moq_msquic_managed_lane_t *lane,
                                  moq_msq_test_lane_row_t *lane_row,
                                  moq_msq_test_child_row_t *rows, size_t cap);

#endif /* MSQ_TEST_SEAMS_H */
