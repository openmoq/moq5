/*
 * Deterministic managed-lane seams: drive the REAL managed facade with no
 * MsQuic library, no socket and no worker thread.
 *
 * Four capabilities, each pinned by what it makes possible rather than by
 * its mere presence:
 *
 *   api override   -- the facade borrows a caller-owned API table instead of
 *                     calling MsQuicOpen2, and never closes it;
 *   no doorbell    -- create spawns no worker thread, and teardown is still
 *                     valid without one;
 *   lane step      -- exactly one PRODUCTION doorbell iteration runs on the
 *                     caller's thread; the seam calls the same decision
 *                     function doorbell_main does, so this drives production
 *                     transitions rather than a model of them;
 *   lane snapshot  -- observation only, taken under the lane lock, and a
 *                     snapshot that exceeds its row capacity reports the FULL
 *                     linked count so a caller fails closed instead of
 *                     silently reading a truncated picture.
 *
 * Threading scope: the deterministic seam cells suppress the doorbell worker
 * so each drives lane decisions by hand, and the first cell proves that
 * suppression as their precondition. The last two cells deliberately run a
 * REAL worker -- one to place the snapshot beside a live doorbell, one to fix
 * the exact lock ordering the sanitizer lane inspects.
 */
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>

#include "msquic_internal.h"
#include "support/fake_msq_table.h"
#include "support/msq_test_seams.h"

#include <moq/msquic_managed.h>

/* Pre-existing canonical seams, declared where each test already declares
 * them (they are test-only and deliberately unpublished). */
extern void moq_msq_test_bell_ring_raw(moq_msquic_managed_lane_t *lane);
extern uint64_t moq_msq_test_bell_generation(moq_msquic_managed_lane_t *lane);
extern void (*moq_msq_test_guard_pre_lock)(moq_msquic_managed_lane_t *lane);
extern void (*moq_msq_test_stats_pre_lock)(moq_msquic_managed_lane_t *lane);
extern moq_result_t moq_msq_test_lane_inject_live_child(
    moq_msquic_managed_lane_t *lane, HQUIC connection,
    moq_msquic_managed_conn_t **out);
extern QUIC_STATUS moq_msq_test_listener_accept(moq_msquic_managed_t *m,
                                                HQUIC connection);
extern moq_msquic_conn_t *moq_msq_test_managed_conn_adapter(
    moq_msquic_managed_conn_t *conn);
extern moq_result_t moq_msq_test_managed_create_lanes_only(
    const moq_msquic_managed_cfg_t *cfg, moq_msquic_managed_t **out);
extern moq_result_t moq_msq_test_managed_create_lanes_only_api(
    const moq_msquic_managed_cfg_t *cfg, const QUIC_API_TABLE *api,
    moq_msquic_managed_t **out);

static int failures = 0;

#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

static fake_msq_t g_table;
static fake_msq_t g_conn_fake;
static fake_msq_t g_accept_fake;

static int pump_calls;
static int pump_lane_calls;

static int counting_pump(moq_msquic_managed_t *m,
                         moq_msquic_managed_lane_t *lane, uint64_t now_us,
                         void *user)
{
    (void)m; (void)lane; (void)now_us; (void)user;
    pump_calls++;
    return 0;
}

static int lane_pump_noop(moq_msquic_managed_t *m,
                          moq_msquic_managed_lane_t *lane, uint64_t now_us,
                          void *user)
{
    (void)m; (void)lane; (void)now_us; (void)user;
    pump_lane_calls++;
    return 0;
}

/*
 * Stand the facade up deterministically: the lanes-only constructor is the
 * base's transport-free path, and `moq_msq_test_api_override` supplies the
 * table it would otherwise open with MsQuicOpen2. Nothing here creates a
 * listener, a socket or a worker thread.
 */
static moq_msquic_managed_t *stand_up_as(moq_perspective_t perspective,
                                         moq_msquic_lane_pump_fn pump,
                                         uint32_t lanes)
{
    moq_msquic_managed_cfg_t cfg;
    moq_msquic_managed_t *m = NULL;

    moq_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = perspective;
    cfg.host = "127.0.0.1";
    cfg.port = 4433;
    cfg.on_lane_pump = pump;
    cfg.max_connections = 4;
    cfg.lane_count = lanes;
    /* NULL table on purpose: the override is what must be consulted */
    if (moq_msq_test_managed_create_lanes_only(&cfg, &m) != MOQ_OK)
        return NULL;
    return m;
}

static moq_msquic_managed_t *stand_up(moq_msquic_lane_pump_fn pump,
                                      uint32_t lanes)
{
    return stand_up_as(MOQ_PERSPECTIVE_CLIENT, pump, lanes);
}


/* --- a socket-free table for the PUBLIC managed constructor ------------
 *
 * The public create path needs registration, configuration, credential,
 * listener and global-settings downcalls that the connection/stream fake does
 * not carry. This overlays exactly those onto a copy of that fake's table --
 * the minimum that lets moq_msquic_managed_create succeed with no MsQuic
 * library, no credential file, no listener and no socket. Every handle is a
 * distinct sentinel address, and every open/close is counted, so the cell can
 * assert that OUR table saw the whole lifecycle. */
typedef struct {
    QUIC_API_TABLE api;
    int reg_opens, reg_closes;
    int cfg_opens, cfg_closes, cred_loads;
    int listener_opens, listener_starts, listener_closes;
    int global_setparams;
} mgd_fake_t;

static mgd_fake_t g_mgd;
static int g_reg_obj, g_cfg_obj, g_lis_obj;

static QUIC_STATUS QUIC_API mf_registration_open(
    const QUIC_REGISTRATION_CONFIG *rc, HQUIC *out)
{
    (void)rc;
    g_mgd.reg_opens++;
    *out = (HQUIC)&g_reg_obj;
    return QUIC_STATUS_SUCCESS;
}

static void QUIC_API mf_registration_close(HQUIC h)
{
    if (h == (HQUIC)&g_reg_obj)
        g_mgd.reg_closes++;
}

static QUIC_STATUS QUIC_API mf_configuration_open(
    HQUIC reg, const QUIC_BUFFER *const alpns, uint32_t alpn_count,
    const QUIC_SETTINGS *settings, uint32_t settings_len, void *ctx,
    HQUIC *out)
{
    (void)reg; (void)alpns; (void)alpn_count; (void)settings;
    (void)settings_len; (void)ctx;
    g_mgd.cfg_opens++;
    *out = (HQUIC)&g_cfg_obj;
    return QUIC_STATUS_SUCCESS;
}

static void QUIC_API mf_configuration_close(HQUIC h)
{
    if (h == (HQUIC)&g_cfg_obj)
        g_mgd.cfg_closes++;
}

static QUIC_STATUS QUIC_API mf_configuration_load_credential(
    HQUIC cfg, const QUIC_CREDENTIAL_CONFIG *cred)
{
    (void)cfg; (void)cred;
    g_mgd.cred_loads++;
    return QUIC_STATUS_SUCCESS;
}

static QUIC_STATUS QUIC_API mf_listener_open(
    HQUIC reg, QUIC_LISTENER_CALLBACK_HANDLER handler, void *ctx, HQUIC *out)
{
    (void)reg; (void)handler; (void)ctx;
    g_mgd.listener_opens++;
    *out = (HQUIC)&g_lis_obj;
    return QUIC_STATUS_SUCCESS;
}

static QUIC_STATUS QUIC_API mf_listener_start(
    HQUIC lis, const QUIC_BUFFER *const alpns, uint32_t alpn_count,
    const QUIC_ADDR *addr)
{
    (void)lis; (void)alpns; (void)alpn_count; (void)addr;
    g_mgd.listener_starts++;
    return QUIC_STATUS_SUCCESS;
}

static void QUIC_API mf_listener_close(HQUIC h)
{
    if (h == (HQUIC)&g_lis_obj)
        g_mgd.listener_closes++;
}

static QUIC_STATUS QUIC_API mf_set_param(HQUIC h, uint32_t param,
                                         uint32_t len, const void *buf)
{
    (void)len; (void)buf;
    if (h == NULL && param == QUIC_PARAM_GLOBAL_SETTINGS)
        g_mgd.global_setparams++;
    return QUIC_STATUS_SUCCESS;
}

static QUIC_STATUS QUIC_API mf_get_param(HQUIC h, uint32_t param,
                                         uint32_t *len, void *buf)
{
    (void)h; (void)param; (void)len; (void)buf;
    /* the bound-port read is optional to the create path; refusing it keeps
     * the fake honest about having no socket */
    return QUIC_STATUS_NOT_SUPPORTED;
}

/* Build the managed table over the connection/stream fake's own entries, so
 * a child accepted later still routes through that fake. */
static const QUIC_API_TABLE *mgd_fake_table(mgd_fake_t *g, fake_msq_t *base)
{
    memset(g, 0, sizeof(*g));
    g->api = *fake_msq_table(base);
    g->api.RegistrationOpen = mf_registration_open;
    g->api.RegistrationClose = mf_registration_close;
    g->api.ConfigurationOpen = mf_configuration_open;
    g->api.ConfigurationClose = mf_configuration_close;
    g->api.ConfigurationLoadCredential = mf_configuration_load_credential;
    g->api.ListenerOpen = mf_listener_open;
    g->api.ListenerStart = mf_listener_start;
    g->api.ListenerClose = mf_listener_close;
    g->api.SetParam = mf_set_param;
    g->api.GetParam = mf_get_param;
    return &g->api;
}


/* --- 0. the PUBLIC constructor, socket-free -----------------------------
 *
 * This is the path every consumer of these seams actually takes: set the two
 * globals, then call moq_msquic_managed_create. The lanes-only constructor is
 * a different function, so a regression that removed the override or the
 * suppression from THIS path alone would leave every other cell green while
 * breaking every consumer. */
static void t_public_create_is_socket_free(void)
{
    int before = failures;
    moq_msquic_managed_cfg_t cfg;
    moq_msquic_managed_t *m = NULL;
    moq_msq_test_lane_row_t lr;

    fake_msq_init(&g_table, false);
    moq_msq_test_api_override = mgd_fake_table(&g_mgd, &g_table);
    moq_msq_test_no_doorbell = true;

    moq_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.host = NULL;                 /* no bind address: nothing listens */
    cfg.port = 0;
    cfg.cert_path = "unused-by-the-fake-cert.pem";
    cfg.key_path = "unused-by-the-fake-key.pem";
    cfg.version = MOQ_VERSION_DRAFT_18;
    cfg.lane_count = 3;              /* multi-lane: every lane must agree */
    cfg.max_connections = 4;
    cfg.on_lane_pump = counting_pump;

    CHECK(moq_msquic_managed_create(&cfg, &m) == MOQ_OK);
    CHECK(m != NULL);
    if (m == NULL)
        goto done;

    /*
     * The override table -- and only it -- ran the whole create lifecycle.
     * The listener open/start below is the FAKE listener path, deliberately
     * exercised: what this cell establishes is that no NATIVE listener or
     * socket, no credential file and no real MsQuic library were involved.
     */
    CHECK(g_mgd.global_setparams == 1);
    CHECK(g_mgd.reg_opens == 1);
    CHECK(g_mgd.cfg_opens == 1);
    CHECK(g_mgd.cred_loads == 1);
    CHECK(g_mgd.listener_opens == 1);
    CHECK(g_mgd.listener_starts == 1);
    /* ... and nothing was closed yet */
    CHECK(g_mgd.reg_closes == 0);
    CHECK(g_mgd.cfg_closes == 0);
    CHECK(g_mgd.listener_closes == 0);

    /* every lane of a multi-lane server agrees: no worker anywhere */
    for (uint32_t i = 0; i < 3; i++) {
        moq_msquic_managed_lane_t *lane = moq_msquic_managed_lane(m, i);

        CHECK(lane != NULL);
        if (lane == NULL)
            continue;
        memset(&lr, 0, sizeof(lr));
        (void)moq_msq_test_lane_snapshot(lane, &lr, NULL, 0);
        CHECK(!lr.doorbell_running);
        CHECK(!lr.lane_stop);
        CHECK(!lr.facade_stop);
    }

    /* stop/destroy stays valid with no worker, and the borrowed table sees
     * its own teardown */
    CHECK(moq_msquic_managed_stop(m) == MOQ_OK);
    moq_msquic_managed_destroy(m);
    CHECK(g_mgd.listener_closes == 1);
    CHECK(g_mgd.cfg_closes == 1);
    CHECK(g_mgd.reg_closes == 1);
    /* the table is still ours and still readable: destroy never handed a
     * BORROWED table to MsQuicClose (the sanitizer lane is what turns a
     * violation of that into a hard failure) */
    CHECK(g_mgd.api.RegistrationOpen == mf_registration_open);

done:
    moq_msq_test_api_override = NULL;
    moq_msq_test_no_doorbell = false;
    if (failures == before)
        printf("PASS: public_create_is_socket_free\n");
}

/* --- 1. the API table is BORROWED, not opened and not closed ---------- */

static void t_api_override_is_borrowed(void)
{
    int before = failures;
    moq_msquic_managed_t *m = NULL;

    fake_msq_init(&g_table, true);
    moq_msq_test_api_override = fake_msq_table(&g_table);
    moq_msq_test_no_doorbell = true;

    fake_msq_init(&g_conn_fake, true);
    m = stand_up(counting_pump, 1);
    CHECK(m != NULL);
    if (m == NULL)
        goto done;

    /*
     * The facade really used OUR table rather than opening its own: a child
     * injected on this lane is torn down through the borrowed table's
     * ConnectionShutdown, which only the fake can record. A facade that had
     * called MsQuicOpen2 would be driving a different table entirely.
     */
    moq_msquic_managed_lane_t *lane = moq_msquic_managed_lane(m, 0);
    moq_msquic_managed_conn_t *mc = NULL;

    CHECK(lane != NULL);
    if (lane != NULL)
        CHECK(moq_msq_test_lane_inject_live_child(
                  lane, fake_msq_conn_handle(&g_conn_fake), &mc) == MOQ_OK);

    CHECK(atomic_load(&g_conn_fake.conn_closes) == 0);
    moq_msquic_managed_stop(m);
    /* destroy must not hand a BORROWED table to MsQuicClose; the table is
     * still ours and still readable afterwards (the sanitizer lane is what
     * turns a violation of that into a hard failure) */
    moq_msquic_managed_destroy(m);
    /* the borrowed table -- and only it -- saw the child's ConnectionClose */
    CHECK(atomic_load(&g_conn_fake.conn_closes) == 1);
    /* destroy must not hand a BORROWED table to MsQuicClose; it is still
     * ours and still readable (the sanitizer lane is what turns a violation
     * of that into a hard failure) */
    CHECK(fake_msq_table(&g_table) != NULL);

done:
    moq_msq_test_api_override = NULL;
    moq_msq_test_no_doorbell = false;
    if (failures == before)
        printf("PASS: api_override_is_borrowed\n");
}

/* --- 2. no doorbell thread is created, and teardown is still valid ----- */

static void t_no_doorbell_spawns_no_thread(void)
{
    int before = failures;
    moq_msquic_managed_t *m = NULL;
    moq_msq_test_lane_row_t lr;
    moq_msq_test_child_row_t rows[4];

    fake_msq_init(&g_table, true);
    moq_msq_test_api_override = fake_msq_table(&g_table);
    moq_msq_test_no_doorbell = true;

    m = stand_up_as(MOQ_PERSPECTIVE_SERVER, counting_pump, 2);
    CHECK(m != NULL);
    if (m == NULL)
        goto done;

    for (uint32_t i = 0; i < 2; i++) {
        moq_msquic_managed_lane_t *lane = moq_msquic_managed_lane(m, i);

        CHECK(lane != NULL);
        if (lane == NULL)
            continue;
        memset(&lr, 0, sizeof(lr));
        (void)moq_msq_test_lane_snapshot(lane, &lr, rows, 4);
        CHECK(!lr.doorbell_running);   /* the discriminator */
    }
    /* teardown with no worker thread must still be valid */
    CHECK(moq_msquic_managed_stop(m) == MOQ_OK);
    moq_msquic_managed_destroy(m);

done:
    moq_msq_test_api_override = NULL;
    moq_msq_test_no_doorbell = false;
    if (failures == before)
        printf("PASS: no_doorbell_spawns_no_thread\n");
}

/* --- 3. one explicit PRODUCTION lane decision ------------------------- */

static void t_lane_step_runs_production_decision(void)
{
    int before = failures;
    moq_msquic_managed_t *m = NULL;
    moq_msquic_managed_lane_t *lane;
    moq_msq_test_lane_row_t lr;

    fake_msq_init(&g_table, true);
    moq_msq_test_api_override = fake_msq_table(&g_table);
    moq_msq_test_no_doorbell = true;
    pump_lane_calls = 0;

    m = stand_up(lane_pump_noop, 1);
    CHECK(m != NULL);
    if (m == NULL)
        goto done;
    lane = moq_msquic_managed_lane(m, 0);
    CHECK(lane != NULL);
    if (lane == NULL)
        goto teardown;

    /* quiet lane: the production decision is IDLE and nothing pumped */
    memset(&lr, 0, sizeof(lr));
    (void)moq_msq_test_lane_snapshot(lane, &lr, NULL, 0);
    uint64_t sweeps_before = lr.pump_sweeps;

    CHECK(moq_msq_test_lane_step(lane) == MOQ_MSQ_TEST_STEP_IDLE);
    memset(&lr, 0, sizeof(lr));
    (void)moq_msq_test_lane_snapshot(lane, &lr, NULL, 0);
    CHECK(lr.pump_sweeps == sweeps_before);

    /* arm an explicit wake: the SAME decision must consume it and pump */
    CHECK(moq_msquic_lane_wake(lane) == MOQ_OK);
    memset(&lr, 0, sizeof(lr));
    (void)moq_msq_test_lane_snapshot(lane, &lr, NULL, 0);
    CHECK(lr.wake_pending || lr.ext_wake);

    CHECK(moq_msq_test_lane_step(lane) == MOQ_MSQ_TEST_STEP_PUMPED);
    memset(&lr, 0, sizeof(lr));
    (void)moq_msq_test_lane_snapshot(lane, &lr, NULL, 0);
    /* the production counter moved -- the step ran the real sweep */
    CHECK(lr.pump_sweeps == sweeps_before + 1);
    /* and the wake was consumed by that same decision */
    CHECK(!lr.wake_pending && !lr.ext_wake);

    /* a drain request is the production STOP decision, not a pump */
    moq_msquic_managed_drain(m);
    CHECK(moq_msq_test_lane_step(lane) != MOQ_MSQ_TEST_STEP_PUMPED);

teardown:
    CHECK(moq_msquic_managed_stop(m) == MOQ_OK);
    /*
     * Both stop latches are OBSERVABLE through the row, and they are
     * distinct: stop asks each lane to quiesce (lane_stop) and publishes the
     * facade-wide latch (facade_stop). A step taken now is the production
     * STOP decision, not a pump.
     */
    if (lane != NULL) {
        memset(&lr, 0, sizeof(lr));
        (void)moq_msq_test_lane_snapshot(lane, &lr, NULL, 0);
        CHECK(lr.lane_stop);
        CHECK(lr.facade_stop);
        CHECK(moq_msq_test_lane_step(lane) == MOQ_MSQ_TEST_STEP_STOP);
    }
    moq_msquic_managed_destroy(m);
done:
    moq_msq_test_api_override = NULL;
    moq_msq_test_no_doorbell = false;
    if (failures == before)
        printf("PASS: lane_step_runs_production_decision\n");
}

/* --- 4. the snapshot observes, and fails closed past its capacity ----- */

/* The whole declared lane row, compared field by field so a diff names the
 * field. Nothing here is adopted from an earlier snapshot: the armed state is
 * asserted absolutely first (below), and these compare two readings of that
 * same state. */
static int lane_rows_differ(const moq_msq_test_lane_row_t *a,
                            const moq_msq_test_lane_row_t *b)
{
    int d = 0;

    if (a->doorbell_running != b->doorbell_running) d++;
    if (a->wake_pending != b->wake_pending) d++;
    if (a->pump_pending != b->pump_pending) d++;
    if (a->ext_wake != b->ext_wake) d++;
    if (a->pump_exit != b->pump_exit) d++;
    if (a->lane_stop != b->lane_stop) d++;
    if (a->facade_stop != b->facade_stop) d++;
    if (a->in_sweep != b->in_sweep) d++;
    if (a->in_lane_pump != b->in_lane_pump) d++;
    if (a->bell_gen != b->bell_gen) d++;
    if (a->conn_count != b->conn_count) d++;
    if (a->pump_sweeps != b->pump_sweeps) d++;
    if (a->service_passes != b->service_passes) d++;
    if (a->deadline_sweeps != b->deadline_sweeps) d++;
    if (a->idle_cap_wakes != b->idle_cap_wakes) d++;
    return d;
}

static int child_rows_differ(const moq_msq_test_child_row_t *a,
                             const moq_msq_test_child_row_t *b)
{
    int d = 0;

    if (a->id != b->id) d++;
    if (a->user != b->user) d++;
    if (a->shutdown_complete != b->shutdown_complete) d++;
    if (a->reapable != b->reapable) d++;
    if (a->app_terminal_acked != b->app_terminal_acked) d++;
    if (a->terminal_enqueued != b->terminal_enqueued) d++;
    if (a->terminal_observed != b->terminal_observed) d++;
    if (a->has_events != b->has_events) d++;
    if (a->bridge_fatal != b->bridge_fatal) d++;
    if (a->bridge_fatal_code != b->bridge_fatal_code) d++;
    if (a->close_feed_commits != b->close_feed_commits) d++;
    return d;
}

static void t_snapshot_observes_and_fails_closed(void)
{
    int before = failures;
    moq_msquic_managed_t *m = NULL;
    moq_msquic_managed_lane_t *lane;
    moq_msq_test_lane_row_t lr, lr2, lr3;
    moq_msq_test_child_row_t rows[2], rows2[2], rows3[2];
    moq_msquic_managed_conn_t *c0 = NULL, *c1 = NULL;

    fake_msq_init(&g_table, true);
    fake_msq_init(&g_conn_fake, true);
    moq_msq_test_api_override = fake_msq_table(&g_table);
    moq_msq_test_no_doorbell = true;

    m = stand_up(counting_pump, 1);
    CHECK(m != NULL);
    if (m == NULL)
        goto done;
    lane = moq_msquic_managed_lane(m, 0);
    CHECK(lane != NULL);
    if (lane == NULL)
        goto teardown;

    /* The bell generation BEFORE the injections, so the snapshot's report of
     * it is anchored to a declared result rather than to its own reading. */
    uint64_t bell_before = moq_msq_test_bell_generation(lane);

    /* two children on one lane, through the production injection seam */
    CHECK(moq_msq_test_lane_inject_live_child(
              lane, fake_msq_conn_handle(&g_conn_fake), &c0) == MOQ_OK);
    CHECK(moq_msq_test_lane_inject_live_child(
              lane, fake_msq_conn_handle(&g_conn_fake), &c1) == MOQ_OK);

    /* each injection rings the bell exactly once */
    uint64_t bell_expected = bell_before + 2;

    /* the independent observer agrees with the declared result */
    CHECK(moq_msq_test_bell_generation(lane) == bell_expected);

    /* capacity 2: complete picture */
    memset(&lr, 0, sizeof(lr));
    memset(rows, 0, sizeof(rows));
    size_t n = moq_msq_test_lane_snapshot(lane, &lr, rows, 2);

    CHECK(n == 2);

    /*
     * The ARMED state, asserted absolutely -- so the conservation comparison
     * below is anchored to facts this cell established, not to whatever the
     * first snapshot happened to report.
     */
    CHECK(!lr.doorbell_running);
    /* injecting a live child arms this lane's wake -- a production fact of
     * the injection seam, asserted rather than assumed away */
    CHECK(lr.wake_pending);
    CHECK(!lr.pump_pending);
    CHECK(!lr.ext_wake);
    CHECK(!lr.pump_exit);
    CHECK(!lr.lane_stop);
    CHECK(!lr.facade_stop);
    CHECK(!lr.in_sweep);
    CHECK(!lr.in_lane_pump);
    CHECK(lr.conn_count == 2);
    CHECK(lr.bell_gen == bell_expected);   /* declared, not adopted */
    CHECK(lr.pump_sweeps == 0);
    CHECK(lr.service_passes == 0);
    CHECK(lr.deadline_sweeps == 0);
    CHECK(lr.idle_cap_wakes == 0);

    CHECK(rows[0].id == c0);
    CHECK(rows[1].id == c1);
    CHECK(rows[0].id != rows[1].id);

    /*
     * has_events is read from the ARMED state through the adapter itself, so
     * the snapshot's report of it is checked against an independent observer
     * rather than against its own later readings.
     */
    moq_msquic_conn_t *a0 = moq_msq_test_managed_conn_adapter(c0);
    moq_msquic_conn_t *a1 = moq_msq_test_managed_conn_adapter(c1);

    CHECK(a0 != NULL && a1 != NULL);
    if (a0 != NULL && a1 != NULL) {
        bool armed0 = moq_msq_conn_has_events(a0);
        bool armed1 = moq_msq_conn_has_events(a1);

        /* a freshly injected child has queued nothing yet */
        CHECK(!armed0);
        CHECK(!armed1);
        CHECK(rows[0].has_events == armed0);
        CHECK(rows[1].has_events == armed1);
    }

    for (size_t i = 0; i < 2; i++) {
        CHECK(rows[i].user == NULL);
        CHECK(!rows[i].shutdown_complete);
        CHECK(!rows[i].reapable);
        CHECK(!rows[i].app_terminal_acked);
        CHECK(!rows[i].terminal_enqueued);
        CHECK(!rows[i].terminal_observed);
        CHECK(!rows[i].bridge_fatal);
        CHECK(rows[i].bridge_fatal_code == 0);
        CHECK(rows[i].close_feed_commits == 0);
    }

    /* A second complete reading must be identical in EVERY declared field --
     * lane row and both child rows -- because a snapshot observes and
     * nothing else. */
    memset(&lr2, 0, sizeof(lr2));
    memset(rows2, 0, sizeof(rows2));
    size_t n2 = moq_msq_test_lane_snapshot(lane, &lr2, rows2, 2);

    CHECK(n2 == 2);
    CHECK(lane_rows_differ(&lr, &lr2) == 0);
    CHECK(child_rows_differ(&rows[0], &rows2[0]) == 0);
    CHECK(child_rows_differ(&rows[1], &rows2[1]) == 0);

    /* capacity 1: the FULL linked count is still reported, so the caller
     * can tell an incomplete picture from a complete one */
    memset(&lr3, 0, sizeof(lr3));
    memset(rows3, 0, sizeof(rows3));
    size_t n1 = moq_msq_test_lane_snapshot(lane, &lr3, rows3, 1);

    CHECK(n1 == 2);              /* > cap: fail closed */
    /* and even the incomplete call conserved everything it observed */
    CHECK(lane_rows_differ(&lr, &lr3) == 0);
    CHECK(child_rows_differ(&rows[0], &rows3[0]) == 0);

    /* a third complete reading after the capacity-one call: still identical */
    memset(&lr2, 0, sizeof(lr2));
    memset(rows2, 0, sizeof(rows2));
    CHECK(moq_msq_test_lane_snapshot(lane, &lr2, rows2, 2) == 2);
    CHECK(lane_rows_differ(&lr, &lr2) == 0);
    CHECK(child_rows_differ(&rows[0], &rows2[0]) == 0);
    CHECK(child_rows_differ(&rows[1], &rows2[1]) == 0);

teardown:
    CHECK(moq_msquic_managed_stop(m) == MOQ_OK);
    moq_msquic_managed_destroy(m);
done:
    moq_msq_test_api_override = NULL;
    moq_msq_test_no_doorbell = false;
    if (failures == before)
        printf("PASS: snapshot_observes_and_fails_closed\n");
}

/* --- 5. the older seams still link and behave beside the new ones ------
 *
 * This is a COMPATIBILITY cell, not the canonical coverage of the seven older
 * seams. Each of them has its own canonical cell, and those are what prove
 * behavior after the doorbell extraction:
 *
 *   moq_msq_test_bell_ring_raw               msquic_lanes, msquic_no_spin,
 *                                            msquic_lane_stats
 *   moq_msq_test_guard_pre_lock              msquic_lanes
 *   moq_msq_test_stats_pre_lock              msquic_lanes
 *   moq_msq_test_lane_inject_live_child      msquic_facade_control,
 *                                            msquic_confinement,
 *                                            msquic_child_forwarding
 *   moq_msq_test_listener_accept             msquic_lanes, msquic_cfg_prefix,
 *                                            msquic_terminal_ack
 *   moq_msq_test_managed_conn_adapter        msquic_facade_control,
 *                                            msquic_confinement
 *   moq_msq_test_managed_create_lanes_only_api  all of the above
 *
 * What this cell adds is only that they still link and still behave WHEN THE
 * NEW SEAMS ARE ACTIVE on the same facade -- a borrowed table with no worker
 * thread -- which no canonical cell exercises.
 */

static int stats_hook_calls;

static void counting_stats_pre_lock(moq_msquic_managed_lane_t *lane)
{
    (void)lane;
    stats_hook_calls++;
}

static void t_preexisting_seams_still_work(void)
{
    int before = failures;
    moq_msquic_managed_t *m = NULL;
    moq_msquic_managed_lane_t *lane;
    moq_msquic_managed_conn_t *mc = NULL;
    moq_msquic_managed_cfg_t cfg;
    moq_msquic_managed_t *lanes_only = NULL;
    moq_msquic_lane_stats_t st;

    fake_msq_init(&g_table, true);
    fake_msq_init(&g_conn_fake, true);
    fake_msq_init(&g_accept_fake, true);
    moq_msq_test_api_override = fake_msq_table(&g_table);
    moq_msq_test_no_doorbell = true;

    /* a SERVER facade: moq_msq_test_listener_accept below drives the real
     * listener callback, which is a server-side path */
    m = stand_up_as(MOQ_PERSPECTIVE_SERVER, counting_pump, 1);
    CHECK(m != NULL);
    if (m == NULL)
        goto done;
    lane = moq_msquic_managed_lane(m, 0);
    CHECK(lane != NULL);
    if (lane == NULL)
        goto teardown;

    /* moq_msq_test_bell_ring_raw + the bell generation it moves */
    uint64_t gen = moq_msq_test_bell_generation(lane);

    moq_msq_test_bell_ring_raw(lane);
    CHECK(moq_msq_test_bell_generation(lane) == gen + 1);

    /* moq_msq_test_lane_inject_live_child + moq_msq_test_managed_conn_adapter */
    CHECK(moq_msq_test_lane_inject_live_child(
              lane, fake_msq_conn_handle(&g_conn_fake), &mc) == MOQ_OK);
    CHECK(mc != NULL);
    if (mc != NULL)
        CHECK(moq_msq_test_managed_conn_adapter(mc) != NULL);

    /*
     * moq_msq_test_stats_pre_lock is DRIVEN, not merely assigned: an external
     * stats read takes the lane lock through the arm this hook brackets.
     */
    stats_hook_calls = 0;
    moq_msq_test_stats_pre_lock = counting_stats_pre_lock;
    memset(&st, 0, sizeof(st));
    CHECK(moq_msquic_lane_get_stats(lane, &st, sizeof(st)) == MOQ_OK);
    CHECK(stats_hook_calls == 1);
    moq_msq_test_stats_pre_lock = NULL;

    /*
     * moq_msq_test_guard_pre_lock fires on a TRANSPORT WORKER inside the
     * connection guard, which this socket-free facade never reaches. It is
     * asserted here only to still link and to be settable and restorable;
     * msquic_lanes is the cell that drives it.
     */
    moq_msq_test_guard_pre_lock = counting_stats_pre_lock;
    CHECK(moq_msq_test_guard_pre_lock == counting_stats_pre_lock);
    moq_msq_test_guard_pre_lock = NULL;

    /* moq_msq_test_managed_create_lanes_only_api: a SECOND facade on the same
     * borrowed table, stopped and destroyed independently */
    moq_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = "127.0.0.1";
    cfg.port = 4433;
    cfg.on_lane_pump = counting_pump;
    CHECK(moq_msq_test_managed_create_lanes_only_api(
              &cfg, fake_msq_table(&g_table), &lanes_only) == MOQ_OK);
    if (lanes_only != NULL) {
        CHECK(moq_msquic_managed_stop(lanes_only) == MOQ_OK);
        moq_msquic_managed_destroy(lanes_only);
    }

    /* moq_msq_test_listener_accept runs the real listener callback on this
     * SERVER facade. Admission capacity is available (max_connections is 4
     * and one child is injected), so the exact expected outcome is a
     * successful placement -- not merely "not pending". */
    CHECK(moq_msq_test_listener_accept(m, fake_msq_conn_handle(&g_accept_fake))
          == QUIC_STATUS_SUCCESS);

teardown:
    CHECK(moq_msquic_managed_stop(m) == MOQ_OK);
    moq_msquic_managed_destroy(m);
done:
    moq_msq_test_api_override = NULL;
    moq_msq_test_no_doorbell = false;
    moq_msq_test_guard_pre_lock = NULL;
    moq_msq_test_stats_pre_lock = NULL;
    if (failures == before)
        printf("PASS: preexisting_seams_still_work\n");
}

/* --- liveness guards for the concurrency cells --------------------------
 *
 * These deadlines are FAILURE WATCHDOGS only. No successful path depends on
 * elapsed time, and no oracle is expressed in it: a guard that expires is a
 * named test failure, never a passing outcome. Their whole purpose is that a
 * broken rendezvous reports itself and terminates instead of parking a thread
 * forever and leaving CTest to kill the binary.
 */
#define SEAM_GUARD_US (5u * 1000u * 1000u)

static void seam_guard_deadline(struct timespec *abs)
{
    clock_gettime(CLOCK_REALTIME, abs);
    abs->tv_sec += (time_t)(SEAM_GUARD_US / 1000000u);
    abs->tv_nsec += (long)(SEAM_GUARD_US % 1000000u) * 1000;
    if (abs->tv_nsec >= 1000000000L) {
        abs->tv_sec += 1;
        abs->tv_nsec -= 1000000000L;
    }
}

static bool seam_guard_expired(const struct timespec *abs)
{
    struct timespec now;

    clock_gettime(CLOCK_REALTIME, &now);
    if (now.tv_sec != abs->tv_sec)
        return now.tv_sec > abs->tv_sec;
    return now.tv_nsec > abs->tv_nsec;
}

/* --- 6. lane_stop and facade_stop are INDEPENDENT ----------------------
 *
 * stop() publishes the facade latch and then, before it takes any lane lock
 * or sets a lane's own flag, calls the existing stop-latched hook. Blocking
 * there exposes the one intermediate state in which the two differ, which is
 * what proves the row reports two facts rather than one twice.
 *
 * Deterministic by construction: the hook itself is the rendezvous, and the
 * stop thread is joined. No sleeps, no timing windows.
 */
extern void (*moq_msq_test_stop_latched)(moq_msquic_managed_t *m);

static pthread_mutex_t sl_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t sl_cv = PTHREAD_COND_INITIALIZER;
static bool sl_reached;      /* the hook ran: the facade latch is published */
static bool sl_release;      /* the cell has taken its intermediate reading */
static moq_msquic_managed_t *sl_scope;

static void stop_latched_blocking(moq_msquic_managed_t *m)
{
    if (m != sl_scope)          /* facade-scoped: ignore any other stop */
        return;
    pthread_mutex_lock(&sl_mu);
    sl_reached = true;
    pthread_cond_broadcast(&sl_cv);
    while (!sl_release)
        pthread_cond_wait(&sl_cv, &sl_mu);
    pthread_mutex_unlock(&sl_mu);
}

struct stop_arg {
    moq_msquic_managed_t *m;
    moq_result_t rc;
};

static void *stop_thread_main(void *arg)
{
    struct stop_arg *sa = arg;

    sa->rc = moq_msquic_managed_stop(sa->m);
    return NULL;
}

static void t_lane_stop_is_distinct_from_facade_stop(void)
{
    int before = failures;
    moq_msquic_managed_t *m = NULL;
    moq_msquic_managed_lane_t *lane;
    moq_msq_test_lane_row_t lr;
    struct stop_arg sa;
    pthread_t th;
    bool joined = false;
    bool spawned = false;

    /* zeroed so an uncreated thread id is never even nominally read on a
     * failure path; `spawned` is what actually gates the join */
    memset(&th, 0, sizeof(th));

    fake_msq_init(&g_table, true);
    moq_msq_test_api_override = fake_msq_table(&g_table);
    moq_msq_test_no_doorbell = true;

    m = stand_up(counting_pump, 1);
    CHECK(m != NULL);
    if (m == NULL)
        goto done;
    lane = moq_msquic_managed_lane(m, 0);
    CHECK(lane != NULL);
    if (lane == NULL)
        goto teardown;

    /* before stop: neither latch */
    memset(&lr, 0, sizeof(lr));
    (void)moq_msq_test_lane_snapshot(lane, &lr, NULL, 0);
    CHECK(!lr.facade_stop);
    CHECK(!lr.lane_stop);

    sl_reached = false;
    sl_release = false;
    sl_scope = m;
    moq_msq_test_stop_latched = stop_latched_blocking;

    sa.m = m;
    sa.rc = MOQ_ERR_INTERNAL;
    /* the create result decides everything below: an uncreated thread must
     * never be waited on or joined */
    spawned = (pthread_create(&th, NULL, stop_thread_main, &sa) == 0);
    CHECK(spawned);
    if (!spawned)
        goto teardown;

    /* wait until the hook proves the facade latch is published, under a
     * failure watchdog -- expiry is a named failure, never an outcome */
    struct timespec abs;
    bool reached;

    seam_guard_deadline(&abs);
    pthread_mutex_lock(&sl_mu);
    while (!sl_reached)
        if (pthread_cond_timedwait(&sl_cv, &sl_mu, &abs) != 0)
            break;
    reached = sl_reached;
    pthread_mutex_unlock(&sl_mu);
    CHECK(reached);              /* guard expired: the hook never ran */
    if (!reached)
        goto teardown;

    /* THE INTERMEDIATE PHASE: facade latched, lane not yet asked */
    memset(&lr, 0, sizeof(lr));
    (void)moq_msq_test_lane_snapshot(lane, &lr, NULL, 0);
    CHECK(lr.facade_stop);
    CHECK(!lr.lane_stop);

    /* release the hook and let stop finish */
    pthread_mutex_lock(&sl_mu);
    sl_release = true;
    pthread_cond_broadcast(&sl_cv);
    pthread_mutex_unlock(&sl_mu);
    CHECK(pthread_join(th, NULL) == 0);
    joined = true;
    CHECK(sa.rc == MOQ_OK);

    /* after stop: both */
    memset(&lr, 0, sizeof(lr));
    (void)moq_msq_test_lane_snapshot(lane, &lr, NULL, 0);
    CHECK(lr.facade_stop);
    CHECK(lr.lane_stop);

teardown:
    /*
     * Release first, then join. The release is published unconditionally so a
     * blocked hook can always finish, and the join is gated on the CREATE
     * result alone -- never on whether the rendezvous was reached, since a
     * created thread must be joined before the hook pointer is cleared or the
     * facade destroyed.
     */
    pthread_mutex_lock(&sl_mu);
    sl_release = true;
    pthread_cond_broadcast(&sl_cv);
    pthread_mutex_unlock(&sl_mu);
    if (spawned && !joined)
        (void)pthread_join(th, NULL);
    moq_msq_test_stop_latched = NULL;
    sl_scope = NULL;
    (void)moq_msquic_managed_stop(m);
    moq_msquic_managed_destroy(m);
done:
    moq_msq_test_stop_latched = NULL;
    sl_scope = NULL;
    moq_msq_test_api_override = NULL;
    moq_msq_test_no_doorbell = false;
    if (failures == before)
        printf("PASS: lane_stop_is_distinct_from_facade_stop\n");
}

/* --- 7. the snapshot against a REAL doorbell worker --------------------
 *
 * Every cell above suppresses the worker, so on its own this file would be a
 * single-threaded program under any sanitizer. This cell deliberately runs
 * WITH a real doorbell thread and brackets the snapshot against it, so the
 * thread sanitizer has two threads actually contending on the lane lock the
 * seam reads under.
 *
 * Determinism comes from handshakes, not from timing: a fixed number of
 * wakes, each acknowledged by the pump through a condition variable before
 * the next is issued, and a join at the end. No sleeps, no wall-clock
 * windows, no localhost, no probabilistic iteration.
 */
enum { HANDSHAKE_ROUNDS = 32 };

static pthread_mutex_t hs_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t hs_cv = PTHREAD_COND_INITIALIZER;
static unsigned hs_pumps;

static int handshake_pump(moq_msquic_managed_t *m,
                          moq_msquic_managed_lane_t *lane, uint64_t now_us,
                          void *user)
{
    (void)m; (void)lane; (void)now_us; (void)user;
    pthread_mutex_lock(&hs_mu);
    hs_pumps++;
    pthread_cond_broadcast(&hs_cv);
    pthread_mutex_unlock(&hs_mu);
    return 0;
}

static void t_snapshot_against_live_doorbell(void)
{
    int before = failures;
    moq_msquic_managed_t *m = NULL;
    moq_msquic_managed_lane_t *lane;
    moq_msq_test_lane_row_t lr;
    moq_msq_test_child_row_t rows[2];

    fake_msq_init(&g_table, true);
    moq_msq_test_api_override = fake_msq_table(&g_table);
    moq_msq_test_no_doorbell = false;   /* a REAL worker, on purpose */
    hs_pumps = 0;

    m = stand_up(handshake_pump, 1);
    CHECK(m != NULL);
    if (m == NULL)
        goto done;
    lane = moq_msquic_managed_lane(m, 0);
    CHECK(lane != NULL);
    if (lane == NULL)
        goto teardown;

    /* the worker really exists -- otherwise this cell proves nothing */
    memset(&lr, 0, sizeof(lr));
    (void)moq_msq_test_lane_snapshot(lane, &lr, NULL, 0);
    CHECK(lr.doorbell_running);
    if (!lr.doorbell_running)
        goto teardown;

    for (unsigned i = 1; i <= HANDSHAKE_ROUNDS; i++) {
        struct timespec abs;
        bool acked;

        CHECK(moq_msquic_lane_wake(lane) == MOQ_OK);

        /* wait for THIS round's pump to be acknowledged before the next,
         * under a failure watchdog -- the deadline is not part of the
         * behavioral oracle, only a way for a broken round to report itself */
        seam_guard_deadline(&abs);
        pthread_mutex_lock(&hs_mu);
        while (hs_pumps < i)
            if (pthread_cond_timedwait(&hs_cv, &hs_mu, &abs) != 0)
                break;
        acked = hs_pumps >= i;
        pthread_mutex_unlock(&hs_mu);
        CHECK(acked);            /* guard expired: this round never pumped */
        if (!acked)
            goto teardown;

        /* the app-thread snapshot, taken while the worker is live */
        memset(&lr, 0, sizeof(lr));
        memset(rows, 0, sizeof(rows));
        size_t n = moq_msq_test_lane_snapshot(lane, &lr, rows, 2);

        CHECK(n == 0);                  /* no children on this lane */
        CHECK(lr.conn_count == 0);
        CHECK(lr.doorbell_running);
        CHECK(!lr.facade_stop);
    }

    /* every acknowledged round ran a production sweep */
    memset(&lr, 0, sizeof(lr));
    (void)moq_msq_test_lane_snapshot(lane, &lr, NULL, 0);
    CHECK(hs_pumps >= HANDSHAKE_ROUNDS);
    CHECK(lr.pump_sweeps >= HANDSHAKE_ROUNDS);

teardown:
    /* stop joins the worker; destroy then runs with no thread left */
    CHECK(moq_msquic_managed_stop(m) == MOQ_OK);
    moq_msquic_managed_destroy(m);
done:
    moq_msq_test_api_override = NULL;
    moq_msq_test_no_doorbell = false;
    if (failures == before)
        printf("PASS: snapshot_against_live_doorbell\n");
}

/* --- 8. the snapshot's lock ordering, proven exactly -------------------
 *
 * Cell 7 shows a real worker pumping beside the snapshot, but a scheduler is
 * free to run that worker all the way through its unlock before the app
 * thread resumes -- so on its own it does not deterministically place the
 * snapshot's read against a concurrent write.
 *
 * This cell fixes the ordering with no timing at all, and -- crucially --
 * with a handshake that establishes NO happens-before of its own. It is built
 * from RELAXED atomics only: no mutex, no condition variable, no release or
 * acquire. A handshake carrying its own ordering would synchronize the two
 * threads by itself and could never expose a missing lane lock, which is
 * exactly the trap an earlier shape of this cell fell into.
 *
 *   1. the doorbell enters on_lane_pump, holding lane->mu;
 *   2. it announces that, then spins on a relaxed flag;
 *   3. the app thread calls the snapshot; its PRE-LOCK hook fires before the
 *      mutex is taken and raises that flag;
 *   4. the still-holding pump performs a plain lane-field write --
 *      moq_msquic_lane_wake on its own lane arms wake_pending under the mutex
 *      it already holds -- and raises a second relaxed flag;
 *   5. the hook observes it and returns; the snapshot takes lane->mu, which
 *      it can only get once the doorbell releases it, and reads that field.
 *
 * With the production lock, the write and the read are ordered by lane->mu
 * and nothing else -- so there is no race. With ONLY the snapshot's lock
 * removed, the identical fixed ordering leaves that field with no
 * synchronization between the two threads at all, and the thread sanitizer
 * must report it here.
 */
static atomic_bool lk_pump_holding;     /* the pump holds lane->mu */
static atomic_bool lk_snapshot_at_lock; /* the snapshot reached the hook */
static atomic_bool lk_pump_wrote;       /* the pump did its guarded write */
static atomic_bool lk_arm;              /* only the armed round participates */
static atomic_bool lk_worker_guard_expired;
static atomic_bool lk_hook_guard_expired;
static moq_msquic_managed_lane_t *lk_lane;

/* Relaxed throughout: these carry a rendezvous, deliberately not an ordering. */
static void lk_set(atomic_bool *f)
{
    atomic_store_explicit(f, true, memory_order_relaxed);
}

static bool lk_get(atomic_bool *f)
{
    return atomic_load_explicit(f, memory_order_relaxed);
}

/*
 * Spin on a relaxed flag under a FAILURE WATCHDOG. The deadline is a clock
 * read only -- deliberately not a mutex, condition variable, or acquire /
 * release pair, any of which would synchronize the writer and the reader and
 * make the unlocked-snapshot discriminator vacuous. Returns false when the
 * guard expired, which every caller turns into a named failure.
 */
static bool lk_spin_until(atomic_bool *f)
{
    struct timespec abs;

    seam_guard_deadline(&abs);
    while (!lk_get(f))
        if (seam_guard_expired(&abs))
            return false;
    return true;
}

static int ordering_pump(moq_msquic_managed_t *m,
                         moq_msquic_managed_lane_t *lane, uint64_t now_us,
                         void *user)
{
    (void)m; (void)now_us; (void)user;
    if (!lk_get(&lk_arm) || lane != lk_lane)
        return 0;
    atomic_store_explicit(&lk_arm, false, memory_order_relaxed);

    lk_set(&lk_pump_holding);
    if (!lk_spin_until(&lk_snapshot_at_lock)) {
        /* the snapshot never reached the lock boundary: record it and leave,
         * rather than parking this worker inside the callback */
        lk_set(&lk_worker_guard_expired);
        lk_set(&lk_pump_wrote);   /* unblock any waiter on the other side */
        return 0;
    }

    /* the guarded plain write, made while this thread still holds lane->mu */
    (void)moq_msquic_lane_wake(lane);

    lk_set(&lk_pump_wrote);
    return 0;
}

static void snapshot_pre_lock_hook(moq_msquic_managed_lane_t *lane)
{
    if (lane != lk_lane || !lk_get(&lk_pump_holding))
        return;
    lk_set(&lk_snapshot_at_lock);
    if (!lk_spin_until(&lk_pump_wrote))
        lk_set(&lk_hook_guard_expired);
}

static void t_snapshot_lock_orders_against_a_guarded_write(void)
{
    int before = failures;
    moq_msquic_managed_t *m = NULL;
    moq_msquic_managed_lane_t *lane;
    moq_msq_test_lane_row_t lr;

    fake_msq_init(&g_table, true);
    moq_msq_test_api_override = fake_msq_table(&g_table);
    moq_msq_test_no_doorbell = false;   /* a REAL worker, on purpose */

    atomic_store(&lk_pump_holding, false);
    atomic_store(&lk_snapshot_at_lock, false);
    atomic_store(&lk_pump_wrote, false);
    atomic_store(&lk_arm, false);
    atomic_store(&lk_worker_guard_expired, false);
    atomic_store(&lk_hook_guard_expired, false);
    lk_lane = NULL;

    m = stand_up(ordering_pump, 1);
    CHECK(m != NULL);
    if (m == NULL)
        goto done;
    lane = moq_msquic_managed_lane(m, 0);
    CHECK(lane != NULL);
    if (lane == NULL)
        goto teardown;

    /* the worker precondition: without it there is no second thread and the
     * rendezvous below could never complete */
    memset(&lr, 0, sizeof(lr));
    (void)moq_msq_test_lane_snapshot(lane, &lr, NULL, 0);
    CHECK(lr.doorbell_running);
    if (!lr.doorbell_running)
        goto teardown;

    lk_lane = lane;
    moq_msq_test_snapshot_pre_lock = snapshot_pre_lock_hook;

    /* arm exactly one participating pump round, then drive it */
    lk_set(&lk_arm);
    CHECK(moq_msquic_lane_wake(lane) == MOQ_OK);

    /* wait for the pump to be inside the callback, holding lane->mu */
    if (!lk_spin_until(&lk_pump_holding)) {
        CHECK(!"guard expired waiting for the pump to hold lane->mu");
        goto teardown;
    }

    /* the read: its pre-lock hook completes the rendezvous, then it takes
     * lane->mu -- which the pump still holds -- and reads the field the pump
     * wrote under that same mutex */
    memset(&lr, 0, sizeof(lr));
    (void)moq_msq_test_lane_snapshot(lane, &lr, NULL, 0);

    /*
     * The ordering really happened. The read does not assert wake_pending's
     * VALUE: the doorbell legitimately consumes that wake and pumps again --
     * still under the same mutex, before the reader can acquire it -- which
     * is itself evidence the guarded write landed. What the field is here for
     * is the sanitizer: it is written plainly under lane->mu by the worker
     * and read plainly under lane->mu by the snapshot, so removing the
     * snapshot's lock leaves it unsynchronized across the two threads.
     */
    CHECK(!lk_get(&lk_worker_guard_expired));
    CHECK(!lk_get(&lk_hook_guard_expired));
    CHECK(lk_get(&lk_pump_wrote));
    CHECK(lr.pump_sweeps >= 2);  /* the armed round, then the wake it armed */

teardown:
    moq_msq_test_snapshot_pre_lock = NULL;
    /* never leave a pump spinning on this rendezvous, whatever failed */
    lk_set(&lk_snapshot_at_lock);
    lk_set(&lk_pump_wrote);
    atomic_store_explicit(&lk_arm, false, memory_order_relaxed);
    CHECK(moq_msquic_managed_stop(m) == MOQ_OK);
    moq_msquic_managed_destroy(m);
    lk_lane = NULL;
done:
    moq_msq_test_snapshot_pre_lock = NULL;
    lk_lane = NULL;
    moq_msq_test_api_override = NULL;
    moq_msq_test_no_doorbell = false;
    if (failures == before)
        printf("PASS: snapshot_lock_orders_against_a_guarded_write\n");
}

int main(void)
{
    setvbuf(stdout, NULL, _IOLBF, 0);
    /*
     * The no-doorbell cell is this file's PRECONDITION, not a peer of the
     * others: every cell below drives lane decisions by hand and would race
     * an accidentally spawned worker. If the precondition fails we stop here
     * and report it, rather than letting dependent cells hang against
     * threads that should not exist.
     */
    t_no_doorbell_spawns_no_thread();
    if (failures != 0) {
        fprintf(stderr,
                "FAIL: msq_seams (%d) -- doorbell suppression is a "
                "precondition; dependent cells not run\n", failures);
        return 1;
    }

    t_public_create_is_socket_free();
    t_api_override_is_borrowed();
    t_lane_step_runs_production_decision();
    t_snapshot_observes_and_fails_closed();
    t_preexisting_seams_still_work();
    t_lane_stop_is_distinct_from_facade_stop();
    /*
     * The two real-worker cells run LAST, in this order: one places the
     * snapshot beside a live doorbell, the other fixes the exact lock
     * ordering the sanitizer lane inspects. Nothing that depends on worker
     * suppression follows them.
     */
    t_snapshot_against_live_doorbell();
    t_snapshot_lock_orders_against_a_guarded_write();
    if (failures == 0)
        printf("PASS: msq_seams\n");
    else
        fprintf(stderr, "FAIL: msq_seams (%d)\n", failures);
    return failures != 0;
}
