/*
 * One owned child per row, and the parent that judges it.
 *
 * Why a child at all: server_create, connect, server_join, env_close and
 * session_release are synchronous callee calls. A deadline checked after they
 * return cannot bound one that never returns, and unwinding a blocked call in
 * the parent would tear down callback state the backend may still be using. So
 * each row runs in its own process, and the parent stops it from outside.
 *
 * The parent trusts nothing the child claims. It recomputes the verdict from
 * the child's retained observation snapshot and compares that with the child's
 * own `passed` field; it binds exit status, signal and timeout to the report;
 * and it refuses to publish a pass when any of those disagree or when the
 * report is short, overlong, duplicated, for the wrong row, corrupt or followed
 * by trailing bytes.
 */
#ifndef MOQR_ORIGIN_SUPER_H
#define MOQR_ORIGIN_SUPER_H

#include "origin_driver.h"
#include "origin_rows.h"

#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#endif

/* How long a TERM is given before the KILL, event-driven throughout. */
#define ORIGIN_TERM_GRACE_MS 500

/* How a supervised row ended, from the parent's point of view. */
typedef enum {
    ORIGIN_CHILD_OK = 0,        /* one complete, consistent report */
    ORIGIN_CHILD_NO_REPORT,     /* exited without a complete report */
    ORIGIN_CHILD_BAD_REPORT,    /* short, overlong, corrupt or wrong row */
    ORIGIN_CHILD_EXTRA_DATA,    /* a second report or trailing bytes */
    ORIGIN_CHILD_CLAIM_MISMATCH,/* its `passed` disagreed with its snapshot */
    ORIGIN_CHILD_TIMEOUT,       /* the row budget elapsed; the child was stopped */
    ORIGIN_CHILD_SIGNAL,        /* died on a signal */
    ORIGIN_CHILD_EXIT_MISMATCH, /* exit status disagreed with the report */
    ORIGIN_CHILD_SPAWN_FAILED,
    ORIGIN_CHILD_BAD_PHASE,     /* missing, duplicate, reordered or corrupt */
    ORIGIN_CHILD_TABLE_CEILING  /* the whole-table ceiling preempted this row */
} origin_child_status_t;

typedef struct {
    origin_child_status_t status;
    bool     passed;            /* the PARENT's recomputed verdict */
    int      exit_code;
    int      signal;
    bool     reaped;            /* the owned child was waited for */
    pid_t    pid;
    char     detail[ORIGIN_REASON_CAP];
} origin_child_result_t;

/*
 * What the child does. It runs in the forked process and fills `out`; its
 * return value becomes the child's exit code (0 pass, 1 fail). Anything it
 * writes to the pipe is written by the supervisor, not by this callback.
 */
typedef int (*origin_child_fn)(void *ctx, size_t row_index,
                               origin_report_t *out);

/* Test seams: an injected budget, and a hook that lets a test corrupt exactly
 * what the child writes. Both default off. */
typedef struct {
    origin_child_fn child;
    /*
     * Test seams that make the parent's own timing attributable: the first
     * runs as the earliest setup operation (so its cost is spent from the row
     * budget), the second immediately after launch (so a parent that is slow
     * to read cannot be mistaken for a child that was slow to tear down).
     * Both are NULL in ordinary use.
     */
    void (*on_before_launch)(void *ctx);
    void (*on_after_launch)(void *ctx);
    /*
     * Models a parent descheduled before a wait, between a ready descriptor
     * and the read that wait authorizes. `got` is how much has already
     * arrived: observational only. It cannot identify what the NEXT read will
     * return -- the pipe may split or join any write -- so nothing may select
     * a read by it.
     */
    void (*on_before_wait)(void *ctx, size_t got);
    /*
     * Fired once every completed read, with that read's own result: `k` is 0
     * for EOF and positive for data. It is the only way a test can single out
     * the EOF read itself -- how much arrived earlier says nothing about what
     * the next read will return, because the pipe may split any write.
     */
    void (*on_after_read)(void *ctx, long long k);
    /*
     * Cap on a single read's capacity, or 0 for no cap. It is the only way to
     * put a DETERMINISTIC boundary between two of the child's writes: a timed
     * gap between them proves nothing, because the parent need not be
     * scheduled during it and one read may then take both writes and more.
     */
    size_t read_cap;
    /* Models a parent descheduled between the last read and the moment the
     * retained bytes are judged. */
    void (*on_before_score)(void *ctx);
    void           *ctx;
    int             row_budget_ms;      /* 0 = ORIGIN_ROW_BUDGET_MS */
    int             teardown_grace_ms;  /* 0 = ORIGIN_TEARDOWN_MS */
    int             table_ceiling_ms;   /* 0 = ORIGIN_PROCESS_CEILING_MS */
    /* Rewrite the bytes the child is about to write. Returns the length to
     * write. NULL means write the whole record unchanged. */
    size_t (*mangle)(void *ctx, void *buf, size_t len, size_t cap);
} origin_super_cfg_t;

/*
 * Called from inside a supervised child, from the driver's on_enter_teardown
 * callee, to emit the one phase transition the parent waits for.
 */
void origin_super_child_enter_teardown(void);

/*
 * Emit one arbitrary frame on the child's report channel. Tests only: it is
 * how a corrupt, wrong-kind or wrongly-timestamped transition is put on the
 * real wire, rather than only through a standalone codec call.
 */
void origin_super_child_emit_phase(const origin_phase_t *p);

/*
 * Run one row in an owned child and judge it.
 *
 * `table_deadline_ms` is an absolute monotonic instant that preempts either
 * phase; pass 0 for none. The parent waits on the MINIMUM of the active phase
 * deadline and this.
 */
void origin_super_run_row_until(const origin_super_cfg_t *cfg, size_t row_index,
                                long long table_deadline_ms,
                                origin_child_result_t *out);
void origin_super_run_row(const origin_super_cfg_t *cfg, size_t row_index,
                          origin_child_result_t *out);

/* Run the frozen table, one child per row, stopping at the first failure and
 * bounded by one whole-table ceiling. */
bool origin_super_run_table(const origin_super_cfg_t *cfg, size_t *executed,
                            size_t *failed_index, origin_child_result_t *last);

#ifdef __cplusplus
}
#endif

#endif /* MOQR_ORIGIN_SUPER_H */
