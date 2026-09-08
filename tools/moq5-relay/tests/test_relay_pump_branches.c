/*
 * Every production path by which the relay's on_lane_pump returns nonzero,
 * activated one at a time — through the REAL cli/main.c callbacks, not a
 * copy — over the sans-I/O facade. Each arm injects its initiating state
 * explicitly (a latched shard fail-stop, a lost binding, the real signal
 * handler) and reads the consequence from state: the callback's return via
 * pump_exit, the facade terminal via managed_wait, and the operator-visible
 * stderr line captured in-process. No sleeps, no network, no repetition.
 *
 * The distinctions pinned here: a benign non-work return is not a failure; a
 * hard failure is logged with its stage and exact status and closes the
 * facade rather than idling; an operator stop closes the facade the same way
 * but logs nothing — the two suppressions share a mechanism and differ by
 * the owner's own signal flag plus the presence of the named line.
 */

#include <moq/relay/moqr_bind.h>
#include "../cli/conn_reap.h"
#include "drain_wait.h"
#include <moq/relay/moqr_shards.h>
#if defined(MOQR_RELAY_INSPECT) || defined(MOQR_BIND_TESTING)
#include <moq/relay/moqr_shards_test.h>
#endif

#include <moq/msquic_managed.h>
#include <moq/session.h>

#include "support/fake_msq_managed.h"
#include "support/msq_test_seams.h"

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* the real production callbacks and their context builders (cli/main.c,
 * compiled into this test with main renamed away) */
extern moq_msquic_lane_pump_fn moqr_test_single_pump(void);
extern moq_msquic_lane_pump_fn moqr_test_lanes_pump(void);
extern void *moqr_test_mk_serve_ctx(moqr_bind_t *bind, moqr_core_t *core,
                                    moqr_trace_t *trace);
extern void *moqr_test_mk_lanes_ctx(moqr_shards_t *shards, uint32_t lanes);
extern void moqr_test_raise_stop(void);
extern void moqr_test_clear_stop(void);

static int g_failures;

#define T_CHECK(expr)                                                     \
    do {                                                                  \
        if (!(expr)) {                                                    \
            printf("FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr);       \
            g_failures++;                                                 \
        }                                                                 \
    } while (0)

/* capture stderr across one driven step, in-process */
static int  g_saved_stderr = -1;
static char g_errfile[] = "/tmp/relay_pump_branches_stderr_XXXXXX";

static void
stderr_capture_begin(void)
{
    fflush(stderr);
    g_saved_stderr = dup(2);
    int fd = mkstemp(g_errfile);
    dup2(fd, 2);
    close(fd);
}

static void
stderr_capture_end(char *buf, size_t cap)
{
    fflush(stderr);
    dup2(g_saved_stderr, 2);
    close(g_saved_stderr);
    FILE *f = fopen(g_errfile, "r");
    size_t n = f != NULL ? fread(buf, 1, cap - 1, f) : 0;
    buf[n] = '\0';
    if (f != NULL) {
        fclose(f);
    }
    unlink(g_errfile);
    memcpy(g_errfile, "/tmp/relay_pump_branches_stderr_XXXXXX",
           sizeof(g_errfile));
}


/* managed_wait consumes a pending activity notification before it reports the
 * facade terminal (activity is not a completion barrier); drain the pending
 * notifications — bounded — and require the terminal underneath. */
static bool
facade_closed(moq_msquic_managed_t *m)
{
    for (int i = 0; i < 4; i++) {
        moq_result_t rc = moq_msquic_managed_wait(m, 0);

        if (rc == MOQ_ERR_CLOSED) {
            return true;
        }
        if (rc != MOQ_OK) {
            return false; /* timeout with no terminal: not closed */
        }
    }
    return false;
}

typedef struct rig {
    fake_mgd_t            fake;
    moqr_core_t          *core;
    moqr_bind_t          *bind;
    moqr_trace_t         *trace;
    moqr_shards_t        *shards;
    void                 *ctx;   /* the production pump's own context */
    moq_msquic_managed_t *m;
} rig_t;

/* The caller memsets the rig and may already have populated core/bind/shards:
 * never wipe it here. */
static bool
rig_up(rig_t *r, uint32_t lanes, moq_msquic_lane_pump_fn pump, void *ctx)
{
    fake_mgd_init(&r->fake);
    moq_msq_test_api_override = fake_mgd_table(&r->fake);
    moq_msq_test_no_doorbell = true;

    moq_msquic_managed_cfg_t scfg;
    moq_msquic_managed_cfg_init_sized(&scfg, sizeof(scfg));
    scfg.alloc = moq_alloc_default();
    scfg.perspective = MOQ_PERSPECTIVE_SERVER;
    scfg.host = NULL;
    scfg.port = 0;
    scfg.cert_path = "unused-by-the-fake-cert.pem";
    scfg.key_path = "unused-by-the-fake-key.pem";
    scfg.version = MOQ_VERSION_DRAFT_18;
    scfg.lane_count = lanes;
    scfg.max_connections = 4;
    scfg.on_lane_pump = pump;
    scfg.on_lane_pump_user = ctx;
    return moq_msquic_managed_create(&scfg, &r->m) == MOQ_OK;
}

static void
rig_down(rig_t *r)
{
    if (r->m != NULL) {
        (void)moq_msquic_managed_stop(r->m);
        moq_msquic_managed_destroy(r->m);
    }
    free(r->ctx);
    if (r->shards != NULL) {
        moqr_shards_destroy(r->shards);
    }
    if (r->bind != NULL) {
        moqr_bind_destroy(r->bind);
    }
    if (r->core != NULL) {
        moqr_core_destroy(r->core);
    }
    if (r->trace != NULL) {
        moqr_trace_destroy(r->trace);
    }
    moq_msq_test_api_override = NULL;
    moq_msq_test_no_doorbell = false;
}

static void
wake_and_step(rig_t *r, uint32_t lane_idx)
{
    moq_msquic_managed_lane_t *lane = moq_msquic_managed_lane(r->m, lane_idx);

    (void)moq_msquic_lane_wake(lane);
    (void)moq_msq_test_lane_step(lane);
}

static bool
lane_pump_exit(rig_t *r, uint32_t lane_idx)
{
    moq_msq_test_lane_row_t lr;

    (void)moq_msq_test_lane_snapshot(moq_msquic_managed_lane(r->m, lane_idx),
                                     &lr, NULL, 0);
    return lr.pump_exit;
}

/* Benign non-work: a lane the shard runtime does not cover returns zero —
 * clamped lanes are inert, never a failure. */
static int
t_lanes_clamp_is_benign(void)
{
    int before = g_failures;
    rig_t r;
    moqr_shards_cfg_t shcfg;

    memset(&r, 0, sizeof(r));
    moqr_shards_cfg_init_sized(&shcfg, sizeof(shcfg), moq_alloc_default());
    shcfg.shards = 1;
    T_CHECK(moqr_shards_create(&shcfg, &r.shards) == MOQR_OK);
    void *ctx = moqr_test_mk_lanes_ctx(r.shards, 2);
    T_CHECK(rig_up(&r, 2, moqr_test_lanes_pump(), ctx));
    r.ctx = ctx;
    if (r.m == NULL) {
        rig_down(&r);
        return g_failures - before;
    }
    wake_and_step(&r, 1); /* lane 1 has no shard behind it */
    T_CHECK(!lane_pump_exit(&r, 1));
    T_CHECK(!facade_closed(r.m));

    rig_down(&r);
    if (g_failures == before) {
        printf("PASS: lanes_clamp_is_benign\n");
    }
    return g_failures - before;
}

/* Hard failure: the shard runtime's sticky manager fail-stop. The callback
 * must return nonzero, name the stage and exact status on stderr, close the
 * facade, and the fail-stop must not strand children past stop(). */
static int
t_step_failstop_is_loud_and_terminal(void)
{
    int before = g_failures;
    rig_t r;
    moqr_shards_cfg_t shcfg;
    char err[512];

    memset(&r, 0, sizeof(r));
    moqr_shards_cfg_init_sized(&shcfg, sizeof(shcfg), moq_alloc_default());
    shcfg.shards = 2;
    shcfg.admit_remote_demand = true; /* the production K>1 shape */
    T_CHECK(moqr_shards_create(&shcfg, &r.shards) == MOQR_OK);
    void *ctx = moqr_test_mk_lanes_ctx(r.shards, 2);
    T_CHECK(rig_up(&r, 2, moqr_test_lanes_pump(), ctx));
    r.ctx = ctx;
    if (r.m == NULL) {
        rig_down(&r);
        return g_failures - before;
    }

    /* a healthy pump first, with a child adopted on lane 0 */
    fake_mgd_conn_t *c = fake_mgd_accept(&r.fake, "moqt-18");
    T_CHECK(c != NULL);
    (void)moq_msq_test_lane_step(moq_msquic_managed_lane(r.m, 0));
    T_CHECK(!lane_pump_exit(&r, 0));
    T_CHECK(moq_msquic_managed_conn_count(r.m) == 1);

    /* latch the fail-stop, then drive the owed pump */
    moqr_shards_debug_fail_stop(r.shards, 0);
    stderr_capture_begin();
    wake_and_step(&r, 0);
    stderr_capture_end(err, sizeof(err));

    T_CHECK(lane_pump_exit(&r, 0));
    T_CHECK(strstr(err, "shard 0 step failed") != NULL);
    T_CHECK(strstr(err, "(-") != NULL); /* the exact status is printed */
    /* the failure is a facade terminal the owner can see, not an idle hold */
    T_CHECK(facade_closed(r.m));
    /* and the shared harness drain-wait NAMES it: a fail-stop mid-drain is
     * MOQR_DRAIN_CLOSED, never a timeout quietly burning its rounds */
    T_CHECK(moqr_drain_to_count(r.m, 0, 5) == MOQR_DRAIN_CLOSED);
    /* the sticky fail-stop cannot strand the child past stop() */
    T_CHECK(moq_msquic_managed_stop(r.m) == MOQ_OK);
    T_CHECK(moq_msquic_managed_conn_count(r.m) == 0);

    rig_down(&r);
    if (g_failures == before) {
        printf("PASS: step_failstop_is_loud_and_terminal\n");
    }
    return g_failures - before;
}

/* Defensive arm: the retirement pass refusing a missing binding. Not
 * reachable from any production composition (the serve paths construct the
 * binding before the facade); activated here to pin that IF it ever fires it
 * is loud and terminal, never a silent idle. */
static int
t_reap_failclosed_is_loud_and_terminal(void)
{
    int before = g_failures;
    rig_t r;
    char err[512];

    memset(&r, 0, sizeof(r));
    void *ctx = moqr_test_mk_serve_ctx(NULL, NULL, NULL); /* the lost binding */
    T_CHECK(rig_up(&r, 1, moqr_test_single_pump(), ctx));
    r.ctx = ctx;
    if (r.m == NULL) {
        rig_down(&r);
        return g_failures - before;
    }
    stderr_capture_begin();
    wake_and_step(&r, 0);
    stderr_capture_end(err, sizeof(err));

    T_CHECK(lane_pump_exit(&r, 0));
    T_CHECK(strstr(err, "connection retirement failed") != NULL);
    T_CHECK(facade_closed(r.m));

    rig_down(&r);
    if (g_failures == before) {
        printf("PASS: reap_failclosed_is_loud_and_terminal\n");
    }
    return g_failures - before;
}

/* Operator stop: the REAL signal handler. The same suppression mechanism as
 * a failure — pump_exit, facade CLOSED — but SILENT: no failure line. The
 * distinction the owner relies on is its own signal flag plus that silence. */
static int
t_stop_signal_is_silent_and_terminal(void)
{
    int before = g_failures;
    rig_t r;
    moqr_core_relay_cfg_t core_cfg;
    moqr_bind_cfg_t bcfg;
    char err[512];

    memset(&r, 0, sizeof(r));
    moqr_core_relay_cfg_init_sized(&core_cfg, sizeof(core_cfg),
                                   moq_alloc_default());
    T_CHECK(moqr_trace_create(moq_alloc_default(), 256, &r.trace) == MOQR_OK);
    core_cfg.trace = r.trace;
    T_CHECK(moqr_core_create(&core_cfg, &r.core) == MOQR_OK);
    moqr_bind_cfg_init_sized(&bcfg, sizeof(bcfg), moq_alloc_default());
    bcfg.core = r.core;
    T_CHECK(moqr_bind_create(&bcfg, &r.bind) == MOQR_OK);
    void *ctx = moqr_test_mk_serve_ctx(r.bind, r.core, r.trace);
    T_CHECK(rig_up(&r, 1, moqr_test_single_pump(), ctx));
    r.ctx = ctx;
    if (r.m == NULL) {
        rig_down(&r);
        return g_failures - before;
    }

    /* healthy first */
    wake_and_step(&r, 0);
    T_CHECK(!lane_pump_exit(&r, 0));

    /* the real handler, then the owed pump */
    moqr_test_raise_stop();
    stderr_capture_begin();
    wake_and_step(&r, 0);
    stderr_capture_end(err, sizeof(err));

    T_CHECK(lane_pump_exit(&r, 0));
    T_CHECK(err[0] == '\0'); /* silence IS the distinction from failure */
    T_CHECK(facade_closed(r.m));
    moqr_test_clear_stop();

    rig_down(&r);
    if (g_failures == before) {
        printf("PASS: stop_signal_is_silent_and_terminal\n");
    }
    return g_failures - before;
}

int
main(void)
{
    (void)t_lanes_clamp_is_benign();
    (void)t_step_failstop_is_loud_and_terminal();
    (void)t_reap_failclosed_is_loud_and_terminal();
    (void)t_stop_signal_is_silent_and_terminal();
    return g_failures;
}
