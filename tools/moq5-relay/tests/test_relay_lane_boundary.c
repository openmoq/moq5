/*
 * The one physical boundary the sans-I/O lifecycle proof cannot represent:
 * bell delivery across real threads. Everything else about the terminal
 * lifecycle — the arm decision, the pump, transfer, acknowledgment, reap —
 * is proven deterministically in test_relay_lane_lifecycle; what remains is
 * that a worker's guard-leave ring actually WAKES a doorbell that is asleep
 * (or about to sleep) on its bell, on a real pthread, once.
 *
 * The ordering is forced with barriers, not delay: the prewait hook reports
 * when the doorbell is entering its bell wait with no pending work, and the
 * test delivers the terminal batch only after that point. Completion is a
 * condition variable the application pump itself signals. The single timed
 * wait is a fail-closed hang guard, never the acceptance condition.
 */

#include <moq/relay/moqr_bind.h>
#include "../cli/conn_reap.h"

#include <moq/msquic_managed.h>
#include <moq/session.h>

#include "support/fake_msq_managed.h"
#include "support/msq_test_seams.h"

#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

/* gated adapter hook: fires as the doorbell is about to block on its bell */
extern void (*moq_msq_test_prewait)(moq_msquic_managed_lane_t *lane);

static int g_failures;

#define T_CHECK(expr)                                                     \
    do {                                                                  \
        if (!(expr)) {                                                    \
            printf("FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr);       \
            g_failures++;                                                 \
        }                                                                 \
    } while (0)

static pthread_mutex_t g_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  g_cv = PTHREAD_COND_INITIALIZER;
static int g_prewaits;   /* doorbell reached its bell wait N times          */
static int g_acked;      /* the pump's retirement pass acknowledged a child */

static void
on_prewait(moq_msquic_managed_lane_t *lane)
{
    (void)lane;
    pthread_mutex_lock(&g_mu);
    g_prewaits++;
    pthread_cond_broadcast(&g_cv);
    pthread_mutex_unlock(&g_mu);
}

typedef struct app_ctx {
    moqr_core_t  *core;
    moqr_bind_t  *bind;
    moqr_trace_t *trace;
} app_ctx_t;

static int
app_pump(moq_msquic_managed_t *m, moq_msquic_managed_lane_t *lane,
         uint64_t now_us, void *vctx)
{
    app_ctx_t *ctx = vctx;

    (void)m;
    for (moq_msquic_managed_conn_t *conn = moq_msquic_lane_next_conn(lane, NULL);
         conn != NULL; conn = moq_msquic_lane_next_conn(lane, conn)) {
        void *tag = moq_msquic_managed_conn_user(conn);
        moq_session_t *s = moq_msquic_managed_conn_session(conn);
        if (tag == MOQR_CONN_DEAD || s == NULL) {
            continue;
        }
        if (tag == NULL) {
            if (moqr_bind_conn_open(ctx->bind, s,
                                    moq_msquic_managed_conn_negotiated_version(conn)) == MOQR_OK) {
                moq_msquic_managed_conn_set_user(conn, MOQR_CONN_OPENED);
            } else {
                moq_msquic_managed_conn_set_user(conn, MOQR_CONN_DEAD);
                moq_msquic_managed_conn_close(conn, 0);
            }
        }
    }
    (void)moqr_bind_pump(ctx->bind, now_us);
    moqr_reap_stats_t st = { 0, 0 };
    if (!moqr_relay_reap_pass(ctx->bind, lane, &st)) {
        return 1;
    }
    if (st.acked > 0) {
        pthread_mutex_lock(&g_mu);
        g_acked += (int)st.acked;
        pthread_cond_broadcast(&g_cv);
        pthread_mutex_unlock(&g_mu);
    }
    return 0;
}

/* Wait on the completion condvar for `*flag >= want`. The deadline is a
 * fail-closed HANG GUARD only: on a healthy build the signal arrives at
 * bell-wake speed and the guard never participates in the verdict. */
static bool
await(int *flag, int want)
{
    struct timespec abs;
    bool ok;

    clock_gettime(CLOCK_REALTIME, &abs);
    abs.tv_sec += 5;
    pthread_mutex_lock(&g_mu);
    while (*flag < want) {
        if (pthread_cond_timedwait(&g_cv, &g_mu, &abs) != 0) {
            break;
        }
    }
    ok = *flag >= want;
    pthread_mutex_unlock(&g_mu);
    return ok;
}

int
main(void)
{
    fake_mgd_t fake;
    app_ctx_t app;

    memset(&app, 0, sizeof(app));
    fake_mgd_init(&fake);
    moq_msq_test_api_override = fake_mgd_table(&fake);
    moq_msq_test_prewait = on_prewait;

    moqr_core_relay_cfg_t core_cfg;
    moqr_core_relay_cfg_init_sized(&core_cfg, sizeof(core_cfg),
                                   moq_alloc_default());
    T_CHECK(moqr_trace_create(moq_alloc_default(), 256, &app.trace) == MOQR_OK);
    core_cfg.trace = app.trace;
    T_CHECK(moqr_core_create(&core_cfg, &app.core) == MOQR_OK);
    moqr_bind_cfg_t bcfg;
    moqr_bind_cfg_init_sized(&bcfg, sizeof(bcfg), moq_alloc_default());
    bcfg.core = app.core;
    bcfg.max_conns = 2;
    T_CHECK(moqr_bind_create(&bcfg, &app.bind) == MOQR_OK);

    moq_msquic_managed_cfg_t scfg;
    moq_msquic_managed_cfg_init_sized(&scfg, sizeof(scfg));
    scfg.alloc = moq_alloc_default();
    scfg.perspective = MOQ_PERSPECTIVE_SERVER;
    scfg.host = NULL;
    scfg.port = 0;
    scfg.cert_path = "unused-by-the-fake-cert.pem";
    scfg.key_path = "unused-by-the-fake-key.pem";
    scfg.version = MOQ_VERSION_DRAFT_18;
    scfg.lane_count = 1;
    scfg.max_connections = 2;
    scfg.on_lane_pump = app_pump;
    scfg.on_lane_pump_user = &app;
    moq_msquic_managed_t *m = NULL;
    T_CHECK(moq_msquic_managed_create(&scfg, &m) == MOQ_OK);
    if (m == NULL) {
        return 2;
    }

    /* barrier 1: the doorbell has gone idle at its bell at least once */
    T_CHECK(await(&g_prewaits, 1));

    /* accepted + adopted; then wait for the doorbell to be back at the bell
     * with the adoption consumed, so the terminal batch below races a lane
     * that is genuinely ASLEEP, not one still processing */
    fake_mgd_conn_t *c = fake_mgd_accept(&fake, "moqt-18");
    T_CHECK(c != NULL);
    int settled = g_prewaits + 1;
    T_CHECK(await(&g_prewaits, settled));

    /* the worker batch, from this (foreign) thread: terminal + queued
     * SESSION_CLOSED, guard-leave arm, bell ring — the wake under test */
    fake_mgd_deliver_peer_close(c, 0);
    fake_mgd_deliver_shutdown_complete(c);

    /* completion: the REAL doorbell thread must wake, pump, transfer,
     * acknowledge — no further stimulus from this thread */
    T_CHECK(await(&g_acked, 1));

    /* and the reap follows on the same thread's next idle iteration */
    T_CHECK(await(&g_prewaits, g_prewaits + 1));
    T_CHECK(moq_msquic_managed_conn_count(m) == 0);

    (void)moq_msquic_managed_stop(m);
    moq_msquic_managed_destroy(m);
    moqr_bind_destroy(app.bind);
    moqr_core_destroy(app.core);
    moqr_trace_destroy(app.trace);
    moq_msq_test_api_override = NULL;
    moq_msq_test_prewait = NULL;
    if (g_failures == 0) {
        printf("PASS: lane_boundary_bell_wake\n");
    }
    return g_failures;
}
