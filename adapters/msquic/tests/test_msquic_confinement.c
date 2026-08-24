/*
 * Callback confinement of the managed facade's self-referential surface.
 *
 * moq_msquic_managed_session() is a CLIENT-only, pump-window-only accessor:
 * it must hand back the single connection's session inside lane 0's
 * on_lane_pump and NULL everywhere else -- from an application thread, from
 * on_activity (same thread, outside the pump window), and always on a
 * SERVER facade. And stop() from inside any managed callback must be
 * refused rather than deadlock on the lane mutex the caller already holds.
 *
 * Both facades come from the lanes-only testing constructor, so no
 * listener, socket, credential or certificate exists here; the CLIENT
 * facade's single child is injected through the production child path.
 */

#if defined(__linux__) && !defined(_POSIX_C_SOURCE)
#define _POSIX_C_SOURCE 200809L
#endif

#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#include "msquic_internal.h"

#include "support/fake_msq_table.h"

#include <moq/msquic_managed.h>

static int failures = 0;

#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

/* MOQ_MSQUIC_TESTING-only seams. */
extern moq_result_t moq_msq_test_managed_create_lanes_only_api(
    const moq_msquic_managed_cfg_t *cfg, const QUIC_API_TABLE *api,
    moq_msquic_managed_t **out);
extern moq_result_t moq_msq_test_lane_inject_live_child(
    moq_msquic_managed_lane_t *lane, HQUIC connection,
    moq_msquic_managed_conn_t **out);
extern moq_msquic_conn_t *moq_msq_test_managed_conn_adapter(
    moq_msquic_managed_conn_t *conn);

enum { GUARD_US = 5 * 1000 * 1000 };

/* Bounded waits below are fail-closed hang guards, never verdicts. */
static void guard_deadline(struct timespec *abs)
{
    clock_gettime(CLOCK_REALTIME, abs);
    abs->tv_sec += GUARD_US / 1000000;
}

/* --- observations, all recorded inside the callbacks ------------------- */

static struct {
    pthread_mutex_t mu;
    pthread_cond_t cv;

    /* on_lane_pump */
    bool pump_ran;
    int pumps;
    moq_session_t *pump_session;   /* what the accessor returned there */
    moq_session_t *pump_expected;  /* what the child's own accessor says */
    moq_result_t pump_stop_rc;
    bool pump_accessors_ran;       /* the four terminal accessors returned */

    /* on_activity */
    bool activity_ran;
    moq_session_t *activity_session;
    /* The facade bumps activity from several contexts. Only the DOORBELL's
     * own bump (msquic_managed.c:882, taken while the sweep markers are
     * still set) is the callback context whose stop() must be refused; the
     * bump the child-injection seam makes runs on THIS thread and is not a
     * managed callback at all. So the stop probe is armed explicitly, after
     * injection has settled, and fires on the next doorbell-originated
     * activity. */
    bool arm_stop;
    bool activity_stop_ran;
    moq_result_t activity_stop_rc;
} g_obs = {
    .mu = PTHREAD_MUTEX_INITIALIZER,
    .cv = PTHREAD_COND_INITIALIZER,
    .pump_stop_rc = MOQ_OK,
    .activity_stop_rc = MOQ_OK,
};

static void obs_reset(void)
{
    pthread_mutex_lock(&g_obs.mu);
    g_obs.pump_ran = false;
    g_obs.pumps = 0;
    g_obs.pump_session = NULL;
    g_obs.pump_expected = NULL;
    g_obs.pump_stop_rc = MOQ_OK;
    g_obs.pump_accessors_ran = false;
    g_obs.activity_ran = false;
    g_obs.activity_session = NULL;
    g_obs.arm_stop = false;
    g_obs.activity_stop_ran = false;
    g_obs.activity_stop_rc = MOQ_OK;
    pthread_mutex_unlock(&g_obs.mu);
}

static int client_pump(moq_msquic_managed_t *m,
                       moq_msquic_managed_lane_t *lane, uint64_t now_us,
                       void *user)
{
    moq_msquic_managed_conn_t *mc = moq_msquic_lane_next_conn(lane, NULL);
    /* the accessor under test, called from its one legal window */
    moq_session_t *s = moq_msquic_managed_session(m);

    (void)now_us;
    (void)user;
    pthread_mutex_lock(&g_obs.mu);
    if (!g_obs.pump_ran) {
        g_obs.pump_session = s;
        g_obs.pump_expected =
            mc != NULL ? moq_msquic_managed_conn_session(mc) : NULL;
        /* the documented contract: any facade accessor may be called from
         * here, and stop() is refused rather than deadlocked */
        (void)moq_msquic_managed_is_fatal(m);
        (void)moq_msquic_managed_is_closed(m);
        (void)moq_msquic_managed_fatal_code(m);
        (void)moq_msquic_managed_close_code(m);
        g_obs.pump_accessors_ran = true;
        g_obs.pump_stop_rc = moq_msquic_managed_stop(m);
        g_obs.pump_ran = true;
    }
    g_obs.pumps++;
    pthread_cond_broadcast(&g_obs.cv);
    pthread_mutex_unlock(&g_obs.mu);
    return 0;
}

static void activity_probe(moq_msquic_managed_t *m, void *ctx)
{
    (void)ctx;
    pthread_mutex_lock(&g_obs.mu);
    if (!g_obs.activity_ran) {
        /* same thread as the pump, but OUTSIDE its window */
        g_obs.activity_session = moq_msquic_managed_session(m);
        g_obs.activity_ran = true;
    }
    if (g_obs.arm_stop && !g_obs.activity_stop_ran) {
        g_obs.activity_stop_rc = moq_msquic_managed_stop(m);
        g_obs.activity_stop_ran = true;
    }
    pthread_cond_broadcast(&g_obs.cv);
    pthread_mutex_unlock(&g_obs.mu);
}

static bool obs_await_both(void)
{
    struct timespec abs;
    bool ok;

    guard_deadline(&abs);
    pthread_mutex_lock(&g_obs.mu);
    while (!(g_obs.pump_ran && g_obs.activity_ran))
        if (pthread_cond_timedwait(&g_obs.cv, &g_obs.mu, &abs) != 0)
            break;
    ok = g_obs.pump_ran && g_obs.activity_ran;
    pthread_mutex_unlock(&g_obs.mu);
    return ok;
}

/* Arm the on_activity stop probe, then drive one doorbell generation so the
 * probe fires from the doorbell's own activity bump. */
static bool obs_probe_activity_stop(moq_msquic_managed_lane_t *lane)
{
    struct timespec abs;
    bool ok;

    pthread_mutex_lock(&g_obs.mu);
    g_obs.arm_stop = true;
    pthread_mutex_unlock(&g_obs.mu);
    if (moq_msquic_lane_wake(lane) != MOQ_OK)
        return false;
    guard_deadline(&abs);
    pthread_mutex_lock(&g_obs.mu);
    while (!g_obs.activity_stop_ran)
        if (pthread_cond_timedwait(&g_obs.cv, &g_obs.mu, &abs) != 0)
            break;
    ok = g_obs.activity_stop_ran;
    pthread_mutex_unlock(&g_obs.mu);
    return ok;
}

/* --- rig --------------------------------------------------------------- */

static fake_msq_t g_table_fake;
static fake_msq_t g_conn_fake;

static void cfg_common(moq_msquic_managed_cfg_t *cfg,
                       moq_perspective_t persp)
{
    moq_msquic_managed_cfg_init_sized(cfg, sizeof(*cfg));
    cfg->alloc = moq_alloc_default();
    cfg->perspective = persp;
    cfg->host = "127.0.0.1";
    cfg->port = persp == MOQ_PERSPECTIVE_CLIENT ? 4433 : 0;
    cfg->on_lane_pump = client_pump;
    cfg->on_activity = activity_probe;
    cfg->send_request_capacity = true;
    cfg->initial_request_capacity = 16;
}

static void teardown(moq_msquic_managed_t *m, moq_msquic_conn_t *adapter)
{
    if (adapter != NULL) {
        QUIC_CONNECTION_EVENT ev;

        memset(&ev, 0, sizeof(ev));
        ev.Type = QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE;
        (void)moq_msquic_conn_callback()(fake_msq_conn_handle(&g_conn_fake),
                                         adapter, &ev);
    }
    (void)moq_msquic_managed_stop(m);
    moq_msquic_managed_destroy(m);
}

/* --- 1. the client accessor is pump-window-only ------------------------ */

static void t_client_session_accessor_window(void)
{
    int before = failures;
    moq_msquic_managed_cfg_t cfg;
    moq_msquic_managed_t *m = NULL;
    moq_msquic_managed_conn_t *mc = NULL;
    moq_msquic_conn_t *adapter = NULL;

    obs_reset();
    fake_msq_init(&g_table_fake, true);
    fake_msq_init(&g_conn_fake, true);
    cfg_common(&cfg, MOQ_PERSPECTIVE_CLIENT);
    CHECK(moq_msq_test_managed_create_lanes_only_api(
              &cfg, fake_msq_table(&g_table_fake), &m) == MOQ_OK);
    if (m == NULL)
        return;

    /* before any child exists, and from an application thread */
    CHECK(moq_msquic_managed_session(m) == NULL);

    moq_msquic_managed_lane_t *lane0 = moq_msquic_managed_lane(m, 0);

    CHECK(lane0 != NULL);
    CHECK(moq_msq_test_lane_inject_live_child(
              lane0, fake_msq_conn_handle(&g_conn_fake), &mc) == MOQ_OK);
    CHECK(mc != NULL);
    if (mc == NULL) {
        teardown(m, NULL);
        return;
    }
    adapter = moq_msq_test_managed_conn_adapter(mc);

    CHECK(obs_await_both());
    CHECK(obs_probe_activity_stop(lane0));

    pthread_mutex_lock(&g_obs.mu);
    /* inside lane 0's pump: the child's own session, not NULL */
    CHECK(g_obs.pump_session != NULL);
    CHECK(g_obs.pump_expected != NULL);
    CHECK(g_obs.pump_session == g_obs.pump_expected);
    /* from on_activity -- same thread, outside the window */
    CHECK(g_obs.activity_session == NULL);
    /* every facade accessor is callable from the pump ... */
    CHECK(g_obs.pump_accessors_ran);
    /* ... and stop() from either callback is refused, not deadlocked */
    CHECK(g_obs.pump_stop_rc == MOQ_ERR_WRONG_STATE);
    CHECK(g_obs.activity_stop_rc == MOQ_ERR_WRONG_STATE);
    pthread_mutex_unlock(&g_obs.mu);

    /* still NULL from the application thread with a live child present */
    CHECK(moq_msquic_managed_session(m) == NULL);
    CHECK(moq_msquic_managed_conn_count(m) == 1);
    /* the refused stops did not stop anything */
    CHECK(moq_msquic_managed_wake(m) == MOQ_OK);

    teardown(m, adapter);
    if (failures == before)
        printf("PASS: client_session_accessor_window\n");
}

/* --- 2. a SERVER facade never hands back a session -------------------- */

static void t_server_accessor_always_null(void)
{
    int before = failures;
    moq_msquic_managed_cfg_t cfg;
    moq_msquic_managed_t *m = NULL;
    moq_msquic_managed_conn_t *mc = NULL;
    moq_msquic_conn_t *adapter = NULL;

    obs_reset();
    fake_msq_init(&g_table_fake, false);
    fake_msq_init(&g_conn_fake, false);
    cfg_common(&cfg, MOQ_PERSPECTIVE_SERVER);
    CHECK(moq_msq_test_managed_create_lanes_only_api(
              &cfg, fake_msq_table(&g_table_fake), &m) == MOQ_OK);
    if (m == NULL)
        return;
    CHECK(moq_msquic_managed_session(m) == NULL);

    moq_msquic_managed_lane_t *lane0 = moq_msquic_managed_lane(m, 0);

    CHECK(lane0 != NULL);
    CHECK(moq_msq_test_lane_inject_live_child(
              lane0, fake_msq_conn_handle(&g_conn_fake), &mc) == MOQ_OK);
    CHECK(mc != NULL);
    if (mc == NULL) {
        teardown(m, NULL);
        return;
    }
    adapter = moq_msq_test_managed_conn_adapter(mc);

    CHECK(obs_await_both());
    CHECK(obs_probe_activity_stop(lane0));

    pthread_mutex_lock(&g_obs.mu);
    /* the child is iterable from the pump -- so a NULL accessor here is
     * the CLIENT-only rule, not an empty lane */
    CHECK(g_obs.pump_expected != NULL);
    CHECK(g_obs.pump_session == NULL);
    CHECK(g_obs.activity_session == NULL);
    CHECK(g_obs.pump_stop_rc == MOQ_ERR_WRONG_STATE);
    CHECK(g_obs.activity_stop_rc == MOQ_ERR_WRONG_STATE);
    pthread_mutex_unlock(&g_obs.mu);

    teardown(m, adapter);
    if (failures == before)
        printf("PASS: server_accessor_always_null\n");
}

int main(void)
{
    setvbuf(stdout, NULL, _IOLBF, 0);

    t_client_session_accessor_window();
    t_server_accessor_always_null();

    if (failures == 0)
        printf("PASS: msquic_confinement\n");
    else
        fprintf(stderr, "FAIL: msquic_confinement (%d)\n", failures);
    return failures;
}
