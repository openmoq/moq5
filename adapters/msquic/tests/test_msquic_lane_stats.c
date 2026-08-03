/*
 * Managed-lane doorbell statistics (moq_msquic_lane_get_stats): wake-arm
 * classification, the pending-cycle latency sample rule, idle-cap vs
 * explicit-wake causes, the caller-context rule (own-lane read, cross-lane
 * refusal, external snapshot), the sized-output discipline, and a
 * real-transport flush observation. Timing assertions are structural
 * (counter relations), never tight scheduler-latency thresholds.
 */

#include <moq/msquic_managed.h>

#include <msquic.h>

#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

static int failures = 0;

#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

static moq_result_t stats_of(moq_msquic_managed_lane_t *lane,
                             moq_msquic_lane_stats_t *st)
{
    memset(st, 0, sizeof(*st));
    return moq_msquic_lane_get_stats(lane, st, sizeof(*st));
}

/* --- in-callback probe: contexts + arm counting -------------------------- */

struct probe {
    moq_msquic_managed_t *m;
    _Atomic int armed;      /* main arms; the next lane-0 pump probes    */
    _Atomic int done;
    moq_result_t rc_own;    /* get_stats(own lane) from own pump         */
    moq_result_t rc_other;  /* get_stats(OTHER lane) from lane 0's pump  */
    moq_result_t rc_wake_own_1, rc_wake_own_2, rc_wake_other;
};

static int probe_pump(moq_msquic_managed_t *m,
                      moq_msquic_managed_lane_t *lane, uint64_t now_us,
                      void *user)
{
    struct probe *p = user;
    (void)now_us;
    if (moq_msquic_lane_index(lane) != 0 || !atomic_load(&p->armed) ||
        atomic_load(&p->done))
        return 0;
    moq_msquic_managed_lane_t *l0 = moq_msquic_managed_lane(m, 0);
    moq_msquic_managed_lane_t *l1 = moq_msquic_managed_lane(m, 1);
    moq_msquic_lane_stats_t st;
    /* own-lane read: must succeed WITHOUT re-locking (a deadlock here
     * hangs the test; the bounded CTest timeout is the backstop) */
    p->rc_own = stats_of(l0, &st);
    /* cross-lane read from a callback: deterministically refused */
    p->rc_other = stats_of(l1, &st);
    /* two same-lane wakes in one pump window: the first opens the next
     * pending cycle, the second finds it open (coalesced) */
    p->rc_wake_own_1 = moq_msquic_lane_wake(l0);
    p->rc_wake_own_2 = moq_msquic_lane_wake(l0);
    /* a cross-lane wake (this thread is lane 0's callback) */
    p->rc_wake_other = moq_msquic_lane_wake(l1);
    atomic_store(&p->done, 1);
    return 0;
}

static int noop_pump(moq_msquic_managed_t *m,
                     moq_msquic_managed_lane_t *lane, uint64_t now_us,
                     void *user)
{
    (void)m; (void)lane; (void)now_us; (void)user;
    return 0;
}

static moq_msquic_managed_t *mk_server(const char *cert, const char *key,
                                       int (*pump)(moq_msquic_managed_t *,
                                                   moq_msquic_managed_lane_t *,
                                                   uint64_t, void *),
                                       void *user)
{
    if (pump == NULL)
        pump = noop_pump;   /* the managed cfg requires a pump callback */
    moq_msquic_managed_cfg_t cfg;
    moq_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.host = "127.0.0.1";
    cfg.port = 0;
    cfg.cert_path = cert;
    cfg.key_path = key;
    cfg.max_connections = 4;
    cfg.lane_count = 2;
    cfg.on_lane_pump = pump;
    cfg.on_lane_pump_user = user;
    moq_msquic_managed_t *m = NULL;
    if (moq_msquic_managed_create(&cfg, &m) != MOQ_OK)
        return NULL;
    return m;
}

/* --- null / invalid arguments -------------------------------------------- */

static void t_invalid_args(const char *cert, const char *key)
{
    int before = failures;
    moq_msquic_managed_t *m = mk_server(cert, key, NULL, NULL);
    CHECK(m != NULL);
    if (m == NULL)
        return;
    moq_msquic_managed_lane_t *l0 = moq_msquic_managed_lane(m, 0);
    moq_msquic_lane_stats_t st;

    memset(&st, 0, sizeof(st));
    CHECK(moq_msquic_lane_get_stats(NULL, &st, sizeof(st)) == MOQ_ERR_INVAL);
    CHECK(moq_msquic_lane_get_stats(l0, NULL, sizeof(st)) == MOQ_ERR_INVAL);
    /* an out_size below the frozen v0 floor is refused */
    CHECK(moq_msquic_lane_get_stats(l0, &st,
                                    MOQ_MSQUIC_LANE_STATS_V0_SIZE - 1)
          == MOQ_ERR_INVAL);

    moq_msquic_managed_stop(m);
    moq_msquic_managed_destroy(m);
    if (failures == before)
        printf("PASS: invalid args refused\n");
}

/* --- sized-output discipline ---------------------------------------- */

static void t_prefix_discipline(const char *cert, const char *key)
{
    int before = failures;
    moq_msquic_managed_t *m = mk_server(cert, key, NULL, NULL);
    CHECK(m != NULL);
    if (m == NULL)
        return;
    moq_msquic_managed_lane_t *l0 = moq_msquic_managed_lane(m, 0);

    /* the exact v0 floor is accepted, and the stamped size is what the
     * library wrote */
    moq_msquic_lane_stats_t exact;
    CHECK(moq_msquic_lane_get_stats(l0, &exact,
                                    MOQ_MSQUIC_LANE_STATS_V0_SIZE) == MOQ_OK);
    CHECK(exact.struct_size == (uint32_t)MOQ_MSQUIC_LANE_STATS_V0_SIZE);

    /* a caller declaring MORE than this library knows (a future, larger
     * struct against this lib): the known fields are filled, the remainder
     * is zeroed (unknown appended fields read zero, never garbage), and the
     * stamped size is what this lib actually wrote */
    uint8_t buf[sizeof(moq_msquic_lane_stats_t) + 32];
    memset(buf, 0xA5, sizeof(buf));
    moq_msquic_lane_stats_t *st = (moq_msquic_lane_stats_t *)buf;
    CHECK(moq_msquic_lane_get_stats(l0, st, sizeof(buf)) == MOQ_OK);
    CHECK(st->struct_size == (uint32_t)sizeof(moq_msquic_lane_stats_t));
    for (size_t i = sizeof(moq_msquic_lane_stats_t); i < sizeof(buf); i++)
        if (buf[i] != 0) {
            CHECK(!"appended-unknown byte not zeroed");
            break;
        }

    moq_msquic_managed_stop(m);
    moq_msquic_managed_destroy(m);
    if (failures == before)
        printf("PASS: sized-output discipline\n");
}

/* --- idle-cap wakes are not explicit wakes -------------------------------- */

static void t_idle_cap_classification(const char *cert, const char *key)
{
    int before = failures;
    moq_msquic_managed_t *m = mk_server(cert, key, NULL, NULL);
    CHECK(m != NULL);
    if (m == NULL)
        return;
    moq_msquic_managed_lane_t *l0 = moq_msquic_managed_lane(m, 0);
    moq_msquic_lane_stats_t a, b;

    CHECK(stats_of(l0, &a) == MOQ_OK);
    /* no sessions, no wakes: the doorbell can only expire its idle cap
     * (200ms); half a second must show at least one, and no latency
     * samples or deadline sweeps may appear from idling alone */
    usleep(500 * 1000);
    CHECK(stats_of(l0, &b) == MOQ_OK);
    CHECK(b.idle_cap_wakes > a.idle_cap_wakes);
    CHECK(b.wake_to_pump_samples == a.wake_to_pump_samples);
    CHECK(b.deadline_sweeps == a.deadline_sweeps);

    moq_msquic_managed_stop(m);
    moq_msquic_managed_destroy(m);
    if (failures == before)
        printf("PASS: idle-cap wakes classified (no samples from idling)\n");
}

/* --- external wake: one cycle, one sample --------------------------------- */

static void t_external_wake_sample(const char *cert, const char *key)
{
    int before = failures;
    moq_msquic_managed_t *m = mk_server(cert, key, NULL, NULL);
    CHECK(m != NULL);
    if (m == NULL)
        return;
    moq_msquic_managed_lane_t *l0 = moq_msquic_managed_lane(m, 0);
    moq_msquic_lane_stats_t st;

    CHECK(moq_msquic_lane_wake(l0) == MOQ_OK);
    /* structural, not latency-bound: poll until the cycle is consumed */
    for (int i = 0; i < 400; i++) {
        CHECK(stats_of(l0, &st) == MOQ_OK);
        if (st.wake_to_pump_samples >= 1)
            break;
        usleep(10 * 1000);
    }
    CHECK(st.wakes_external == 1);
    CHECK(st.wakes_same_lane == 0);
    CHECK(st.wakes_cross_lane == 0);
    CHECK(st.wake_to_pump_samples == 1);   /* exactly one cycle consumed */
    CHECK(st.pump_sweeps >= 1);
    CHECK(st.wake_to_pump_max_us <= 300000); /* bounded by the idle cap +
                                              * slack; not a tight bound */

    moq_msquic_managed_stop(m);
    moq_msquic_managed_destroy(m);
    if (failures == before)
        printf("PASS: external wake -> one consumed cycle, one sample\n");
}

/* --- callback contexts: arms, coalescing, refusal -------------------------- */

static void t_callback_contexts(const char *cert, const char *key)
{
    int before = failures;
    struct probe p;
    memset(&p, 0, sizeof(p));
    moq_msquic_managed_t *m = mk_server(cert, key, probe_pump, &p);
    CHECK(m != NULL);
    if (m == NULL)
        return;
    p.m = m;
    moq_msquic_managed_lane_t *l0 = moq_msquic_managed_lane(m, 0);
    moq_msquic_managed_lane_t *l1 = moq_msquic_managed_lane(m, 1);

    /* baselines BEFORE any wake: every assertion below is an EXACT delta */
    moq_msquic_lane_stats_t b0, b1, s0, s1;
    CHECK(stats_of(l0, &b0) == MOQ_OK);
    CHECK(stats_of(l1, &b1) == MOQ_OK);

    atomic_store(&p.armed, 1);
    CHECK(moq_msquic_lane_wake(l0) == MOQ_OK);   /* external: triggers pump */
    for (int i = 0; i < 400 && !atomic_load(&p.done); i++)
        usleep(10 * 1000);
    CHECK(atomic_load(&p.done));

    /* in-callback contract: own-lane read OK (and did not deadlock);
     * OTHER-lane read deterministically refused; every wake accepted */
    CHECK(p.rc_own == MOQ_OK);
    CHECK(p.rc_other == MOQ_ERR_WRONG_STATE);
    CHECK(p.rc_wake_own_1 == MOQ_OK && p.rc_wake_own_2 == MOQ_OK);
    CHECK(p.rc_wake_other == MOQ_OK);

    /* settle: BOTH lanes must consume their armed cycles — the cross-lane
     * wake is proven SERVICED (lane 1 pumps and records its sample), not
     * merely counted at the arm site */
    for (int i = 0; i < 400; i++) {
        CHECK(stats_of(l0, &s0) == MOQ_OK);
        CHECK(stats_of(l1, &s1) == MOQ_OK);
        if (s0.wake_to_pump_samples - b0.wake_to_pump_samples >= 2 &&
            s1.wake_to_pump_samples - b1.wake_to_pump_samples >= 1)
            break;
        usleep(10 * 1000);
    }
    /* lane 0: 1 external (cycle 1) + 2 same-lane (ONE cycle: the second
     * found it open -> EXACTLY one coalesced) = exactly 2 consumed cycles */
    CHECK(s0.wakes_external - b0.wakes_external == 1);
    CHECK(s0.wakes_same_lane - b0.wakes_same_lane == 2);
    CHECK(s0.wakes_coalesced - b0.wakes_coalesced == 1);
    CHECK(s0.wake_to_pump_samples - b0.wake_to_pump_samples == 2);
    /* lane 1: exactly one cross-lane wake, CONSUMED: exactly one pump
     * sweep, exactly one latency sample, and a consistent total/max (one
     * new sample of delay d: total grows by d, max >= d, d bounded by the
     * idle cap + slack — a structural bound, not a tight latency gate) */
    CHECK(s1.wakes_cross_lane - b1.wakes_cross_lane == 1);
    CHECK(s1.wakes_same_lane == b1.wakes_same_lane);
    CHECK(s1.wakes_external == b1.wakes_external);
    CHECK(s1.wakes_coalesced == b1.wakes_coalesced);
    CHECK(s1.pump_sweeps - b1.pump_sweeps == 1);
    CHECK(s1.wake_to_pump_samples - b1.wake_to_pump_samples == 1);
    uint64_t d1 = s1.wake_to_pump_total_us - b1.wake_to_pump_total_us;
    CHECK(d1 <= 300000);
    CHECK(s1.wake_to_pump_max_us >= d1);

    moq_msquic_managed_stop(m);
    moq_msquic_managed_destroy(m);
    if (failures == before)
        printf("PASS: callback contexts (arms, coalesce, cross-lane "
               "refusal)\n");
}

/* --- concurrent polling + real-transport flush ----------------------------- */

struct poller {
    moq_msquic_managed_t *m;
    _Atomic int stop;
};

static void *poll_main(void *arg)
{
    struct poller *po = arg;
    moq_msquic_lane_stats_t st;
    while (!atomic_load(&po->stop)) {
        for (uint32_t i = 0; i < moq_msquic_managed_lane_count(po->m); i++)
            (void)stats_of(moq_msquic_managed_lane(po->m, i), &st);
        usleep(1000);
    }
    return NULL;
}

static void t_flush_and_polling(const char *cert, const char *key)
{
    int before = failures;
    moq_msquic_managed_t *srv = mk_server(cert, key, noop_pump, NULL);
    CHECK(srv != NULL);
    if (srv == NULL)
        return;
    uint16_t port = moq_msquic_managed_port(srv);
    CHECK(port != 0);

    /* hammer get_stats from an external thread the whole time (the TSan
     * lane proves this race-free; here it proves no deadlock/crash) */
    struct poller po;
    po.m = srv;
    atomic_store(&po.stop, 0);
    pthread_t pt;
    CHECK(pthread_create(&pt, NULL, poll_main, &po) == 0);

    /* one managed client: its MoQ SETUP exchange makes the server lane
     * write control bytes -> at least one ACCEPTED flush on some lane */
    moq_msquic_managed_cfg_t cfg;
    moq_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = "127.0.0.1";
    cfg.port = port;
    cfg.insecure_skip_verify = true;
    cfg.on_lane_pump = noop_pump;
    moq_msquic_managed_t *cli = NULL;
    CHECK(moq_msquic_managed_create(&cfg, &cli) == MOQ_OK);

    moq_msquic_lane_stats_t st;
    uint64_t sends = 0, bytes = 0, service = 0;
    for (int i = 0; i < 800; i++) {
        sends = bytes = service = 0;
        for (uint32_t l = 0; l < 2; l++) {
            CHECK(stats_of(moq_msquic_managed_lane(srv, l), &st) == MOQ_OK);
            sends += st.flush_sends;
            bytes += st.flush_bytes;
            service += st.service_passes;
        }
        if (sends >= 1)
            break;
        usleep(10 * 1000);
    }
    CHECK(sends >= 1);      /* an accepted control-stream flush happened */
    CHECK(bytes >= 1);      /* and it carried bytes */
    CHECK(service >= 1);    /* via at least one per-conn service pass */

    /* Flush totals are LANE-LIFETIME: close the client, wait until the
     * server has RECLAIMED the connection, and the lane must still report
     * at least the totals it had while the connection was live (the reap
     * latch — without it the reclaimed child's counters vanish and the
     * totals collapse below the pre-close snapshot). */
    uint64_t pre_close_sends = sends, pre_close_bytes = bytes;
    if (cli != NULL) {
        moq_msquic_managed_stop(cli);
        moq_msquic_managed_destroy(cli);
        cli = NULL;
    }
    bool reclaimed = false;
    for (int i = 0; i < 800; i++) {
        if (moq_msquic_managed_conn_count(srv) == 0) {
            reclaimed = true;
            break;
        }
        usleep(10 * 1000);
    }
    CHECK(reclaimed);
    sends = bytes = 0;
    for (uint32_t l = 0; l < 2; l++) {
        CHECK(stats_of(moq_msquic_managed_lane(srv, l), &st) == MOQ_OK);
        sends += st.flush_sends;
        bytes += st.flush_bytes;
    }
    CHECK(sends >= pre_close_sends);   /* survived reclamation (monotone) */
    CHECK(bytes >= pre_close_bytes);
    CHECK(sends >= 1);

    atomic_store(&po.stop, 1);
    pthread_join(pt, NULL);
    moq_msquic_managed_stop(srv);
    moq_msquic_managed_destroy(srv);
    if (failures == before)
        printf("PASS: real-transport flush observed under concurrent "
               "polling; totals survive connection reclamation\n");
}

/* --- positive deadline path ------------------------------------------------ */

/* A finite SESSION deadline (armed via the appended session_idle_timeout_us
 * knob) drives the doorbell's due-deadline branch: deadline_sweeps must go
 * POSITIVE, and a deadline sweep is a distinct cause — it consumes no armed
 * wake, so it may never mint a wake-to-pump latency sample. */
static void t_deadline_sweeps(const char *cert, const char *key)
{
    int before = failures;
    moq_msquic_managed_cfg_t cfg;
    moq_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.host = "127.0.0.1";
    cfg.port = 0;
    cfg.cert_path = cert;
    cfg.key_path = key;
    cfg.max_connections = 4;
    cfg.lane_count = 2;
    cfg.on_lane_pump = noop_pump;
    /* 400ms: long enough that the post-handshake QUIC chatter settles and
     * the lane goes QUIET before expiry — the doorbell's deadline wait must
     * be the one to observe it (a short timeout expires during the setup
     * burst, where an ordinary pump sweep services it first). */
    cfg.session_idle_timeout_us = 400 * 1000;
    moq_msquic_managed_t *srv = NULL;
    CHECK(moq_msquic_managed_create(&cfg, &srv) == MOQ_OK);
    if (srv == NULL)
        return;
    uint16_t port = moq_msquic_managed_port(srv);

    moq_msquic_lane_stats_t b[2], s[2];
    for (uint32_t l = 0; l < 2; l++)
        CHECK(stats_of(moq_msquic_managed_lane(srv, l), &b[l]) == MOQ_OK);

    /* one client connection: its server-side session carries the finite
     * idle deadline; NO explicit wakes are issued in this window */
    moq_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = "127.0.0.1";
    cfg.port = port;
    cfg.insecure_skip_verify = true;
    cfg.on_lane_pump = noop_pump;
    moq_msquic_managed_t *cli = NULL;
    CHECK(moq_msquic_managed_create(&cfg, &cli) == MOQ_OK);

    uint64_t ddelta = 0, sdelta = 0;
    for (int i = 0; i < 800; i++) {
        ddelta = sdelta = 0;
        for (uint32_t l = 0; l < 2; l++) {
            CHECK(stats_of(moq_msquic_managed_lane(srv, l), &s[l]) == MOQ_OK);
            ddelta += s[l].deadline_sweeps - b[l].deadline_sweeps;
            sdelta += s[l].wake_to_pump_samples - b[l].wake_to_pump_samples;
        }
        if (ddelta >= 1)
            break;
        usleep(10 * 1000);
    }
    CHECK(ddelta >= 1);   /* the due-deadline branch actually ran */
    CHECK(sdelta == 0);   /* and attributed NO explicit-wake sample */

    if (cli != NULL) {
        moq_msquic_managed_stop(cli);
        moq_msquic_managed_destroy(cli);
    }
    moq_msquic_managed_stop(srv);
    moq_msquic_managed_destroy(srv);
    if (failures == before)
        printf("PASS: deadline sweeps go positive (no wake sample "
               "attributed)\n");
}

/* --- flush totals survive stop() cleanup (not just ordinary reap) --------- */

/* The lifetime-total guarantee must hold across stop() too: a child that is
 * STILL LIVE when stop() runs is freed by the stop-cleanup loop, not the
 * doorbell reap, and that loop must fold its accepted flush totals into the
 * lane's lifetime accumulators. Without the stop-path latch the child's
 * counters vanish and the post-stop snapshot collapses below the pre-stop one. */
static void t_flush_survives_stop(const char *cert, const char *key)
{
    int before = failures;
    moq_msquic_managed_t *srv = mk_server(cert, key, noop_pump, NULL);
    CHECK(srv != NULL);
    if (srv == NULL)
        return;
    uint16_t port = moq_msquic_managed_port(srv);
    CHECK(port != 0);
    if (port == 0) {
        moq_msquic_managed_destroy(srv);
        return;
    }

    /* one managed client: its MoQ SETUP makes the server lane write control
     * bytes -> an ACCEPTED flush on some lane. Keep it connected so the
     * server-side child is still LIVE when we stop the server. */
    moq_msquic_managed_cfg_t cfg;
    moq_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = "127.0.0.1";
    cfg.port = port;
    cfg.insecure_skip_verify = true;
    cfg.on_lane_pump = noop_pump;
    moq_msquic_managed_t *cli = NULL;
    CHECK(moq_msquic_managed_create(&cfg, &cli) == MOQ_OK);

    moq_msquic_lane_stats_t st;
    uint64_t pre_sends = 0, pre_bytes = 0;
    for (int i = 0; i < 800; i++) {
        pre_sends = pre_bytes = 0;
        for (uint32_t l = 0; l < 2; l++) {
            CHECK(stats_of(moq_msquic_managed_lane(srv, l), &st) == MOQ_OK);
            pre_sends += st.flush_sends;
            pre_bytes += st.flush_bytes;
        }
        if (pre_sends >= 1 && moq_msquic_managed_conn_count(srv) >= 1)
            break;
        usleep(10 * 1000);
    }
    CHECK(pre_sends >= 1);                             /* an accepted flush */
    CHECK(pre_bytes >= 1);
    CHECK(moq_msquic_managed_conn_count(srv) >= 1);    /* child still LIVE */

    /* Stop the server WHILE the child is attached: doorbell_quiesce shuts it
     * down but leaves it linked, so the stop-cleanup loop frees it (not the
     * reap). Stats stay valid until destroy(). */
    moq_msquic_managed_stop(srv);

    uint64_t post_sends = 0, post_bytes = 0;
    for (uint32_t l = 0; l < 2; l++) {
        CHECK(stats_of(moq_msquic_managed_lane(srv, l), &st) == MOQ_OK);
        post_sends += st.flush_sends;
        post_bytes += st.flush_bytes;
    }
    /* lifetime totals must not regress across stop() and must still carry the
     * reclaimed child's accepted flushes */
    CHECK(post_sends >= pre_sends);
    CHECK(post_bytes >= pre_bytes);
    CHECK(post_sends >= 1);

    moq_msquic_managed_destroy(srv);
    if (cli != NULL) {
        moq_msquic_managed_stop(cli);
        moq_msquic_managed_destroy(cli);
    }
    if (failures == before)
        printf("PASS: flush totals survive stop() cleanup (not just reap)\n");
}

/* --- application service-deadline fold: idle scheduler + whole-block ABI --- */

#define APP_DL_INTERVAL_US 50000u

/* UINT64_MAX = disarmed. A FIXED absolute deadline (like media_sender's cached
 * refresh_wake_deadline_us), advanced only by the pump after each fire — NOT
 * recomputed on every query, which would keep it perpetually in the future. */
static _Atomic uint64_t g_app_target = UINT64_MAX;
static _Atomic int g_app_calls;              /* callback invocation count */

static uint64_t app_mono_now(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000u + (uint64_t)ts.tv_nsec / 1000u;
}

/* Pure cached read of the current deadline (the app_deadline_us contract). */
static uint64_t app_deadline_cb(void *ctx)
{
    (void)ctx;
    atomic_fetch_add(&g_app_calls, 1);
    return atomic_load(&g_app_target);
}

/* Stand-in for media_sender: after a fire (any pump) push the next deadline
 * one interval out, so the fold produces PERIODIC sweeps instead of a spin. */
static int advancing_pump(moq_msquic_managed_t *m,
                          moq_msquic_managed_lane_t *lane, uint64_t now_us,
                          void *user)
{
    (void)m; (void)lane; (void)now_us; (void)user;
    if (atomic_load(&g_app_target) != UINT64_MAX)
        atomic_store(&g_app_target, app_mono_now() + APP_DL_INTERVAL_US);
    return 0;
}

/* Idle server facade (no connections) with app_deadline wired at a
 * caller-chosen struct_size, so the whole-block gate can be exercised. */
static moq_msquic_managed_t *mk_app_deadline_server(const char *cert,
                                                    const char *key, size_t ssz)
{
    moq_msquic_managed_cfg_t cfg;
    moq_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.host = "127.0.0.1";
    cfg.port = 0;
    cfg.cert_path = cert;
    cfg.key_path = key;
    cfg.lane_count = 1;
    cfg.on_lane_pump = advancing_pump;
    cfg.app_deadline_us = app_deadline_cb;
    cfg.app_deadline_ctx = NULL;
    cfg.struct_size = (uint32_t)ssz;   /* override to drive the whole-block gate */
    moq_msquic_managed_t *m = NULL;
    if (moq_msquic_managed_create(&cfg, &m) != MOQ_OK)
        return NULL;
    return m;
}

/* Full struct_size: the app deadline is honored — the idle lane doorbell folds
 * it and the deadline-sweep path fires (deadline_sweeps climbs) while the
 * callback is consulted. */
static void t_app_deadline_fold(const char *cert, const char *key)
{
    int before = failures;
    atomic_store(&g_app_target, UINT64_MAX);
    atomic_store(&g_app_calls, 0);
    moq_msquic_managed_t *m =
        mk_app_deadline_server(cert, key, sizeof(moq_msquic_managed_cfg_t));
    CHECK(m != NULL);
    if (m == NULL)
        return;
    moq_msquic_managed_lane_t *l0 = moq_msquic_managed_lane(m, 0);
    moq_msquic_lane_stats_t a;
    CHECK(stats_of(l0, &a) == MOQ_OK);
    atomic_store(&g_app_target, app_mono_now() + APP_DL_INTERVAL_US);  /* arm */
    (void)moq_msquic_lane_wake(l0);
    usleep(1200 * 1000);
    atomic_store(&g_app_target, UINT64_MAX);       /* disarm before teardown */
    moq_msquic_lane_stats_t b;
    CHECK(stats_of(l0, &b) == MOQ_OK);
    CHECK(b.deadline_sweeps - a.deadline_sweeps >= 5);   /* idle wakes fired */
    CHECK(atomic_load(&g_app_calls) > 0);                /* callback consulted */
    (void)moq_msquic_managed_stop(m);
    moq_msquic_managed_destroy(m);
    if (failures == before)
        printf("PASS: app_deadline_fold\n");
}

/* Partial struct_size (through app_deadline_us only): the whole-block gate
 * drops the callback — it is NEVER consulted and no app-driven sweeps fire.
 * the callback must NOT run (count stays 0). */
static void t_app_deadline_partial_gate(const char *cert, const char *key)
{
    int before = failures;
    atomic_store(&g_app_target, app_mono_now() + APP_DL_INTERVAL_US); /* would fire if honored */
    atomic_store(&g_app_calls, 0);
    size_t partial = offsetof(moq_msquic_managed_cfg_t, app_deadline_ctx);
    moq_msquic_managed_t *m = mk_app_deadline_server(cert, key, partial);
    CHECK(m != NULL);
    if (m == NULL)
        return;
    moq_msquic_managed_lane_t *l0 = moq_msquic_managed_lane(m, 0);
    moq_msquic_lane_stats_t a;
    CHECK(stats_of(l0, &a) == MOQ_OK);
    (void)moq_msquic_lane_wake(l0);
    usleep(600 * 1000);
    atomic_store(&g_app_target, UINT64_MAX);
    moq_msquic_lane_stats_t b;
    CHECK(stats_of(l0, &b) == MOQ_OK);
    CHECK(atomic_load(&g_app_calls) == 0);               /* callback dropped */
    CHECK(b.deadline_sweeps == a.deadline_sweeps);       /* no app-driven sweep */
    (void)moq_msquic_managed_stop(m);
    moq_msquic_managed_destroy(m);
    if (failures == before)
        printf("PASS: app_deadline_partial_gate\n");
}

int main(int argc, char **argv)
{
    if (argc < 3) {
        fprintf(stderr, "usage: %s <cert.pem> <key.pem>\n", argv[0]);
        return 2;
    }
    const char *cert = argv[1], *key = argv[2];

    /* one library reference for the whole process (matches the other
     * managed suites: MsQuic's refcount must not bounce to zero between
     * sub-pins). Never released; reachable until exit. */
    static const QUIC_API_TABLE *lib_pin;
    if (QUIC_FAILED(MsQuicOpen2(&lib_pin))) {
        fprintf(stderr, "MsQuicOpen2 failed\n");
        return 2;
    }

    t_invalid_args(cert, key);
    t_prefix_discipline(cert, key);
    t_idle_cap_classification(cert, key);
    t_external_wake_sample(cert, key);
    t_callback_contexts(cert, key);
    t_deadline_sweeps(cert, key);
    t_app_deadline_fold(cert, key);
    t_app_deadline_partial_gate(cert, key);
    t_flush_and_polling(cert, key);
    t_flush_survives_stop(cert, key);

    if (failures == 0)
        printf("PASS: msquic_lane_stats\n");
    else
        fprintf(stderr, "FAIL: msquic_lane_stats (%d)\n", failures);
    return failures;
}
