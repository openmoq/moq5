/*
 * The Origin gate's row driver.
 *
 * The driver owns the lifecycle: build the server, learn its bound port, open
 * the client environment, connect, wait for the row's terminal, tear down in
 * order, release the app reference after the completion barrier, and take the
 * verdict. It stops at the first failing row.
 *
 * Everything the driver touches goes through a CALLEE TABLE of opaque handles,
 * so this file needs no transport header. The native fixture fills the table
 * with thin wrappers over the real facade and client; the offline tests fill it
 * with a fake that can produce the orderings a real transport only produces by
 * accident -- a callback before connect returns, a reentrant callback, a late
 * contradictory event, a partial construction, an allocation failure, and a
 * call that blocks past the budget.
 *
 * The budget is monotonic and starts BEFORE synchronous setup, so a call that
 * blocks during construction is bounded by the same deadline as one that blocks
 * waiting for a terminal.
 */
#ifndef MOQR_ORIGIN_DRIVER_H
#define MOQR_ORIGIN_DRIVER_H

#include "origin_rows.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Milliseconds. One row budget covering setup and wait; one teardown grace. */
#define ORIGIN_ROW_BUDGET_MS   10000
#define ORIGIN_TEARDOWN_MS      5000
/* The owned ceiling a supervising parent applies to the whole process. */
#define ORIGIN_PROCESS_CEILING_MS 90000

typedef struct origin_driver origin_driver_t;

/*
 * The transport operations, as opaque handles.
 *
 * Every call returns 0 for success and nonzero otherwise, except where noted.
 * `ctx` is the table's own context. The driver never interprets a handle.
 */
typedef struct {
    void *ctx;

    /* Build the server for this row's policy. *out receives its handle. */
    int  (*server_create)(void *ctx, const origin_row_t *row, void **out);
    /* The bound port, after create. Zero means unavailable. */
    unsigned short (*server_port)(void *ctx, void *server);
    /*
     * Ordered teardown: request, then join, then destroy.
     *
     * The stop REQUEST is idempotent and cannot fail; a false from the facade
     * only means another caller began teardown first, which is not an error.
     * It is therefore void here rather than encoding a fake error domain.
     */
    void (*server_stop_begin)(void *ctx, void *server);
    int  (*server_join)(void *ctx, void *server);
    void (*server_destroy)(void *ctx, void *server);

    /* Open the client environment. */
    int  (*env_open)(void *ctx, void **out);
    /*
     * Connect with explicit trust. Callbacks for this session MAY run before
     * this returns; they record through `obs`, never through *out.
     * On success *out receives one app-owned reference.
     */
    int  (*connect)(void *ctx, void *env, const origin_row_t *row,
                    unsigned short port, origin_obs_t *obs, void **out);
    /* Wait for the row's terminal, bounded by the remaining budget. Returns
     * 0 when a terminal was observed, nonzero on timeout. */
    int  (*wait_terminal)(void *ctx, origin_obs_t *obs, int remaining_ms);
    /* The completion barrier: after this returns the backend will never touch
     * a session again. */
    void (*env_close)(void *ctx, void *env);
    /* Release the app-owned reference. Legal only after env_close. */
    void (*session_release)(void *ctx, void *session);

    /*
     * Called ONCE, immediately before unwind begins. This is the child's phase
     * transition: the supervising parent starts its teardown grace here rather
     * than guessing when work ended. Optional in a purely in-process run.
     */
    void (*on_enter_teardown)(void *ctx);

    /* Monotonic milliseconds. Injectable so offline tests need no sleep. */
    long long (*now_ms)(void *ctx);
} origin_callees_t;

/*
 * Run one row.
 *
 * Returns true when the row passed; `reason` explains a failure. When
 * `unsafe_out` is set true the row's completion barrier FAILED: the server was
 * not destroyed, callback-owned state may still be reachable by a live pump,
 * and the caller must NOT snapshot, free or report that state -- the process
 * should exit and leave the opaque cleanup to process teardown.
 */
bool origin_driver_run_row(const origin_callees_t *c, const origin_row_t *row,
                           origin_obs_t *obs, bool *unsafe_out,
                           char *reason, size_t reason_cap);

/* Run the frozen table, stopping at the first failure. `*executed` receives the
 * number of rows attempted; `*failed_index` the failing row, or the row count
 * when every row passed. */
bool origin_driver_run_table(const origin_callees_t *c, size_t *executed,
                             size_t *failed_index, char *reason,
                             size_t reason_cap);

#ifdef __cplusplus
}
#endif

#endif /* MOQR_ORIGIN_DRIVER_H */
