/*
 * Real loopback smoke for the managed wtquic-MsQuic facade, both perspectives.
 *
 * CLIENT: stands up the EXISTING public wtquic-MsQuic listener and connects the
 * managed facade client to it, proving actual WT-Protocol negotiation and
 * MoQ-session/attach-adapter creation at the negotiated version — for draft-16
 * and draft-18 SEPARATELY. Establishment is observed through the facade's own
 * condition variable (broadcast on establishment).
 *
 * SERVER: stands up the managed facade server and connects a raw wtquic client,
 * proving the server admits AND establishes a child (draft-18 version + attach
 * adapter + retained ws, read under the child's lane guard) and then quiesces it
 * server-first at teardown (env_close must return). This case recompiles the
 * facade with MOQ_WTQ_MM_TESTING for the guarded child observation.
 *
 * Establishment only: no subscribe/object flow (that needs the pump slice), no
 * datagrams, no service integration. The per-call wait timeout and overall
 * deadline are pure hang guards, never timing assertions.
 *
 * Usage: test_wtquic_msquic_managed_loopback <cert.pem> <key.pem>
 */
#include <moq/wtquic_msquic_managed.h>
#include <moq/session.h>

#include "wtquic_msquic_managed_test_internal.h"

#include <wtquic/wtquic.h>
#include <wtquic/wtquic_msquic.h>

#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

static int g_fail;
#define CHECK(c)                                                            \
    do {                                                                    \
        if (!(c)) {                                                         \
            fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #c);    \
            g_fail++;                                                       \
        }                                                                   \
    } while (0)

#define WAIT_SECS 30

static const char *cert_path;
static const char *key_path;

static int dummy_pump(moq_wtquic_msquic_managed_t *m,
                      moq_wtquic_msquic_managed_lane_t *lane, uint64_t now_us,
                      void *user)
{
    (void)m;
    (void)lane;
    (void)now_us;
    (void)user;
    return 0;
}

/* Captures what the real CLIENT facade's own lane callback observed when it
 * tried to acknowledge the connection it was presented. */
static struct {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    int may_close;       /* set once establishment has been verified: the probe
                          * must not close the connection before then, or the
                          * facade's establishment wait would see a closed client */
    int seen;            /* pumps that observed the client's terminal */
    moq_result_t rc;     /* ack result for the client, AFTER observation */
    moq_result_t rc_null;
} g_cap;

/* The client facade's lane callback. It receives the single client connection
 * through the normal iteration API -- exactly as an application would -- and
 * acknowledgment of it must be refused because a client is not reaped
 * per-child. */
static int client_ack_probe_pump(moq_wtquic_msquic_managed_t *m,
                                 moq_wtquic_msquic_managed_lane_t *lane,
                                 uint64_t now_us, void *user)
{
    (void)m; (void)now_us; (void)user;
    for (moq_wtquic_msquic_managed_conn_t *c =
             moq_wtquic_msquic_lane_next_conn(lane, NULL);
         c != NULL; c = moq_wtquic_msquic_lane_next_conn(lane, c)) {
        moq_session_t *sess = moq_wtquic_msquic_managed_conn_session(c);
        if (sess == NULL)
            continue;
        /* Drain the client's real event queue. Only once its terminal has been
         * OBSERVED does the acknowledgment reach the client check -- before
         * that it would be refused for the unobserved reason instead, which
         * would not discriminate the guard under test. */
        moq_event_t ev;
        bool closed = false;
        while (moq_session_poll_events(sess, &ev, 1) == 1)
            if (ev.kind == MOQ_EVENT_SESSION_CLOSED)
                closed = true;
        if (!closed) {
            pthread_mutex_lock(&g_cap.mu);
            int go = g_cap.may_close;
            pthread_mutex_unlock(&g_cap.mu);
            /* drive this client's own connection to its terminal, through the
             * public API, so a later pump observes a real SESSION_CLOSED --
             * but only once the caller has verified establishment */
            if (go)
                moq_wtquic_msquic_managed_conn_close(c, 0);
            continue;
        }
        moq_result_t rc = moq_wtquic_msquic_managed_conn_ack_terminal(c);
        moq_result_t rn = moq_wtquic_msquic_managed_conn_ack_terminal(NULL);
        pthread_mutex_lock(&g_cap.mu);
        g_cap.rc = rc;
        g_cap.rc_null = rn;
        g_cap.seen++;
        pthread_cond_broadcast(&g_cap.cv);
        pthread_mutex_unlock(&g_cap.mu);
    }
    return 0;
}

/* One offer/version pair over a fresh listener + facade client. */
static void run_case(const char *token, moq_version_t expected)
{
    /* server: the public wtquic-MsQuic listener, supporting both MoQ
     * subprotocols. No-op events, no admission, no MoQ session — it only
     * accepts the WebTransport session so the client can establish. */
    wtq_msquic_env_cfg_t secfg = WTQ_MSQUIC_ENV_CFG_INIT;
    wtq_msquic_env_t *senv = NULL;
    CHECK(wtq_msquic_env_open(&secfg, &senv) == WTQ_OK);
    if (senv == NULL)
        return;

    static const char *const server_protos[] = { "moqt-18", "moqt-16" };
    wtq_serve_config_t serve = WTQ_SERVE_CONFIG_INIT;
    serve.path = "/moq";
    serve.subprotocols = server_protos;
    serve.subprotocol_count = 2;
    serve.require_subprotocol = true;

    wtq_session_events_t sev;
    wtq_session_events_init(&sev);
    wtq_msquic_listener_cfg_t lcfg = WTQ_MSQUIC_LISTENER_CFG_INIT;
    lcfg.bind_address = "127.0.0.1";
    lcfg.port = 0;
    lcfg.cert_file = cert_path;
    lcfg.key_file = key_path;
    lcfg.paths = &serve;
    lcfg.path_count = 1;
    lcfg.events = &sev;
    lcfg.user = NULL;
    wtq_msquic_listener_t *listener = NULL;
    CHECK(wtq_msquic_listener_start(senv, &lcfg, &listener) == WTQ_OK);
    if (listener == NULL) {
        wtq_msquic_env_close(senv);
        return;
    }
    uint16_t port = wtq_msquic_listener_port(listener);

    /* client: the managed facade, offering exactly `token` so the negotiated
     * version is unambiguous. */
    const char *offers[] = { token };
    moq_wtquic_msquic_managed_cfg_t cfg;
    moq_wtquic_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = "127.0.0.1";
    cfg.port = port;
    cfg.insecure_skip_verify = true;
    cfg.wt_path = "/moq";
    cfg.wt_protocols = offers;
    cfg.wt_protocol_count = 1;
    cfg.on_lane_pump = client_ack_probe_pump;

    memset(&g_cap, 0, sizeof(g_cap));
    CHECK(pthread_mutex_init(&g_cap.mu, NULL) == 0);
    CHECK(pthread_cond_init(&g_cap.cv, NULL) == 0);
    moq_wtquic_msquic_managed_t *m = NULL;
    CHECK(moq_wtquic_msquic_managed_create(&cfg, &m) == MOQ_OK);
    if (m != NULL) {
        /* one bounded wait: establishment latches activity -> MOQ_OK; a client
         * fatal/closed -> MOQ_ERR_CLOSED; the WAIT_SECS bound is only a hang
         * guard (it would surface as MOQ_DONE) */
        moq_result_t wr =
            moq_wtquic_msquic_managed_wait(m, (uint64_t)WAIT_SECS * 1000000u);
        CHECK(wr == MOQ_OK);
        /* exact negotiated-version publication (negotiated_version is set only
         * after the MoQ session + attach adapter are both created) */
        CHECK(moq_wtquic_msquic_managed_negotiated_version(m) == expected);
        CHECK(!moq_wtquic_msquic_managed_is_fatal(m));
        /* The CLIENT connection is not reaped per-child, so acknowledging it is
         * a state error EVEN ONCE ITS TERMINAL HAS BEEN OBSERVED. Proven from
         * the real client's own on_lane_pump, using the handle that callback
         * was presented -- no white-box seam. The callback drives its own
         * connection to terminal through the public close API. */
        pthread_mutex_lock(&g_cap.mu);
        g_cap.may_close = 1;
        pthread_mutex_unlock(&g_cap.mu);
        struct timespec dl;
        clock_gettime(CLOCK_REALTIME, &dl);
        dl.tv_sec += WAIT_SECS;
        pthread_mutex_lock(&g_cap.mu);
        int wrc = 0;
        while (g_cap.seen == 0 && wrc == 0) {
            pthread_mutex_unlock(&g_cap.mu);
            (void)moq_wtquic_msquic_lane_wake(
                moq_wtquic_msquic_managed_lane(m, 0));
            pthread_mutex_lock(&g_cap.mu);
            if (g_cap.seen != 0) break;
            wrc = pthread_cond_timedwait(&g_cap.cv, &g_cap.mu, &dl);
        }
        CHECK(g_cap.seen != 0);                       /* the pump saw the client */
        CHECK(g_cap.rc == MOQ_ERR_WRONG_STATE);       /* and refused the ack */
        CHECK(g_cap.rc_null == MOQ_ERR_INVAL);        /* NULL is an arg error */
        pthread_mutex_unlock(&g_cap.mu);
        /* clean client stop + teardown */
        CHECK(moq_wtquic_msquic_managed_stop(m) == MOQ_OK);
        moq_wtquic_msquic_managed_destroy(m);
    }

    pthread_cond_destroy(&g_cap.cv);
    pthread_mutex_destroy(&g_cap.mu);
    if (listener != NULL)
        wtq_msquic_listener_stop(listener);
    wtq_msquic_env_close(senv);
}

/*
 * Server: the managed listener admits + establishes a raw client, then tears
 * down cleanly. This is the diagnostic for the pre-barrier connection quiesce —
 * without it the server's env_close blocks on the established child's session.
 */
struct raw_cli {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    int established;
};
static void raw_established(wtq_session_t *s, wtq_str_t sub, void *user)
{
    (void)s;
    (void)sub;
    struct raw_cli *r = user;
    pthread_mutex_lock(&r->mu);
    r->established = 1;
    pthread_cond_broadcast(&r->cv);
    pthread_mutex_unlock(&r->mu);
}
static bool raw_wait_established(struct raw_cli *r)
{
    struct timespec dl;
    clock_gettime(CLOCK_REALTIME, &dl);
    dl.tv_sec += WAIT_SECS;
    pthread_mutex_lock(&r->mu);
    bool ok = true;
    while (r->established == 0 && ok)
        ok = pthread_cond_timedwait(&r->cv, &r->mu, &dl) == 0;
    bool set = r->established != 0;
    pthread_mutex_unlock(&r->mu);
    return set;
}

static void run_server_case(void)
{
    const char *offers[] = { "moqt-18", "moqt-16" };
    moq_wtquic_msquic_managed_cfg_t cfg;
    moq_wtquic_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.host = "127.0.0.1"; /* bind address */
    cfg.port = 0;           /* ephemeral */
    cfg.cert_path = cert_path;
    cfg.key_path = key_path;
    cfg.wt_path = "/moq";
    cfg.wt_protocols = offers;
    cfg.wt_protocol_count = 2;
    cfg.max_connections = 4;
    cfg.on_lane_pump = dummy_pump;

    moq_wtquic_msquic_managed_t *srv = NULL;
    CHECK(moq_wtquic_msquic_managed_create(&cfg, &srv) == MOQ_OK);
    if (srv == NULL)
        return;
    uint16_t port = moq_wtquic_msquic_managed_port(srv);
    CHECK(port != 0); /* bound-port reporting */

    struct raw_cli rc;
    memset(&rc, 0, sizeof(rc));
    CHECK(pthread_mutex_init(&rc.mu, NULL) == 0);
    CHECK(pthread_cond_init(&rc.cv, NULL) == 0);
    wtq_msquic_env_cfg_t ecfg = WTQ_MSQUIC_ENV_CFG_INIT;
    wtq_msquic_env_t *cenv = NULL;
    CHECK(wtq_msquic_env_open(&ecfg, &cenv) == WTQ_OK);
    /* the raw client's creator reference: released AFTER cenv is closed, per the
     * post-env_close session lifetime rule. Held at function scope so teardown
     * can outlive the connect block. */
    wtq_session_t *cs = NULL;
    if (cenv != NULL && port != 0) {
        static const char *const cprotos[] = { "moqt-18" };
        wtq_session_events_t cev;
        wtq_session_events_init(&cev);
        cev.on_established = raw_established;
        wtq_connect_config_t connect = WTQ_CONNECT_CONFIG_INIT;
        connect.authority = "localhost";
        connect.path = "/moq";
        connect.subprotocols = cprotos;
        connect.subprotocol_count = 1;
        wtq_msquic_client_cfg_t cli = WTQ_MSQUIC_CLIENT_CFG_INIT;
        cli.server_name = "127.0.0.1";
        cli.port = port;
        cli.insecure_skip_verify = true;
        cli.connect = &connect;
        cli.events = &cev;
        cli.user = &rc;
        CHECK(wtq_msquic_client_connect(cenv, &cli, &cs) == WTQ_OK);
        /* the client establishes only if the managed server admitted + accepted
         * the WT session; the server then holds exactly one live child */
        CHECK(raw_wait_established(&rc));
        CHECK(moq_wtquic_msquic_managed_conn_count(srv) == 1);
        /* server negotiation is per-connection: the facade accessor stays 0 */
        CHECK(moq_wtquic_msquic_managed_negotiated_version(srv) == 0);

        /* conn_count == 1 only proves ADMISSION. Prove the managed server also
         * completed ESTABLISHMENT: under the child's lane guard it holds the
         * negotiated draft-18 version, an attach adapter, and the retained ws. */
        moq_version_t child_ver = 0;
        bool child_has_adapter = false, child_has_ws = false;
        CHECK(moq_wtquic_msquic_managed_test_server_child(
            srv, &child_ver, &child_has_adapter, &child_has_ws));
        CHECK(child_ver == MOQ_VERSION_DRAFT_18);
        CHECK(child_has_adapter);
        CHECK(child_has_ws);
    }

    /* Prove SERVER-INITIATED quiescence: keep the client alive and stop the
     * server first, so the server's pre-barrier quiesce is what closes the
     * established child (if we closed the client first the peer could close the
     * child, hiding whether the server can). env_close must still return. */
    CHECK(moq_wtquic_msquic_managed_stop(srv) == MOQ_OK);
    moq_wtquic_msquic_managed_destroy(srv);

    /* now tear down the client: close the env, then release the creator ref */
    if (cenv != NULL)
        wtq_msquic_env_close(cenv);
    if (cs != NULL)
        wtq_session_release(cs);
    pthread_cond_destroy(&rc.cv);
    pthread_mutex_destroy(&rc.mu);
}


/* ---------------------------------------------------------------------------
 * Terminal conservation over a REAL session.
 *
 * Drives the whole chain with no injected facts: a real WebTransport child with
 * a real MoQ session reaches its transport terminal, the session enqueues
 * MOQ_EVENT_SESSION_CLOSED, the lane callback polls that event and acknowledges
 * that same connection, and only then may the child be reclaimed and its
 * capacity slot reused.
 *
 * Condition variables are the authority for every wait; the timeouts are hang
 * guards, never the thing being asserted.
 * ------------------------------------------------------------------------- */
#define ACC_TERMINAL_CONSUMED ((void *)0x7e)
struct acc_state {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    bool     draining;        /* the pump may poll the event queue */
    bool     may_ack;         /* the pump may acknowledge once it has polled */
    uint64_t pumps;           /* completed pump callbacks */
    uint64_t closed_polled;   /* SESSION_CLOSED events transferred to us */
    uint64_t acks_ok;         /* acknowledgments accepted */
    uint64_t acks_wrong_state;/* acknowledgments refused */
    bool     saw_terminal_unpolled; /* a pump saw the child while its terminal
                                     * was enqueued but not yet polled */
    uint64_t cycles;          /* completed managed pump CYCLES, signalled from
                               * on_activity after the reaper has run -- not
                               * from inside on_lane_pump */
};
static struct acc_state g_acc;

static void acc_init(void)
{
    memset(&g_acc, 0, sizeof(g_acc));
    CHECK(pthread_mutex_init(&g_acc.mu, NULL) == 0);
    CHECK(pthread_cond_init(&g_acc.cv, NULL) == 0);
}
static void acc_destroy(void)
{
    pthread_cond_destroy(&g_acc.cv);
    pthread_mutex_destroy(&g_acc.mu);
}

/* Wait until `pred` holds. Returns false only if the hang guard expires. */
static bool acc_wait(bool (*pred)(void))
{
    struct timespec dl;
    clock_gettime(CLOCK_REALTIME, &dl);
    dl.tv_sec += WAIT_SECS;
    pthread_mutex_lock(&g_acc.mu);
    int rc = 0;
    while (!pred() && rc == 0)
        rc = pthread_cond_timedwait(&g_acc.cv, &g_acc.mu, &dl);
    bool ok = pred();
    pthread_mutex_unlock(&g_acc.mu);
    return ok;
}
static uint64_t g_acc_cycle_target;
static bool acc_pred_cycle_target(void)   { return g_acc.cycles >= g_acc_cycle_target; }

/* Force one more COMPLETE managed cycle and wait for it. Waiting on the
 * post-cycle activity signal (not the in-callback pump counter) is what makes
 * a following reap assertion meaningful. */
static bool acc_force_cycle(moq_wtquic_msquic_managed_t *m)
{
    pthread_mutex_lock(&g_acc.mu);
    g_acc_cycle_target = g_acc.cycles + 1;
    pthread_mutex_unlock(&g_acc.mu);
    (void)moq_wtquic_msquic_lane_wake(moq_wtquic_msquic_managed_lane(m, 0));
    return acc_wait(acc_pred_cycle_target);
}
static bool acc_pred_terminal_unpolled(void) { return g_acc.saw_terminal_unpolled; }
static bool acc_pred_acked(void)          { return g_acc.acks_ok >= 1; }
static bool acc_pred_polled(void)         { return g_acc.closed_polled >= 1; }

static int acc_pump(moq_wtquic_msquic_managed_t *m,
                    moq_wtquic_msquic_managed_lane_t *lane, uint64_t now_us,
                    void *user)
{
    (void)now_us; (void)user;
    for (moq_wtquic_msquic_managed_conn_t *c =
             moq_wtquic_msquic_lane_next_conn(lane, NULL);
         c != NULL; c = moq_wtquic_msquic_lane_next_conn(lane, c)) {
        moq_session_t *s = moq_wtquic_msquic_managed_conn_session(c);
        if (s == NULL)
            continue;

        pthread_mutex_lock(&g_acc.mu);
        bool may_drain = g_acc.draining;
        pthread_mutex_unlock(&g_acc.mu);

        if (!may_drain) {
            /* Not polling yet. Once the facade has published this child's
             * session-backed transport terminal, we are being presented a
             * terminal child we never observed -- acknowledgment must be
             * refused in exactly that state. */
            uint64_t tterm = 0;
            moq_wtquic_msquic_managed_test_terminal_counters(
                m, &tterm, NULL, NULL, NULL, NULL);
            if (tterm >= 1) {
                moq_result_t rc = moq_wtquic_msquic_managed_conn_ack_terminal(c);
                pthread_mutex_lock(&g_acc.mu);
                if (rc == MOQ_ERR_WRONG_STATE) {
                    g_acc.acks_wrong_state++;
                    g_acc.saw_terminal_unpolled = true;
                }
                pthread_cond_broadcast(&g_acc.cv);
                pthread_mutex_unlock(&g_acc.mu);
            }
            continue;
        }

        /* Drain the real event queue; acknowledge only after the terminal event
         * has actually been transferred to us, from this same callback. */
        moq_event_t ev;
        bool closed = false;
        while (moq_session_poll_events(s, &ev, 1) == 1)
            if (ev.kind == MOQ_EVENT_SESSION_CLOSED)
                closed = true;
        /* Terminal consumption is remembered in the connection's OWN user slot.
         * The event is consumed by the poll, so a later callback relies on the
         * session's monotonic observed fact -- and a handle is never retained
         * or compared across callbacks, per the handle-confinement rule. */
        if (closed) {
            moq_wtquic_msquic_managed_conn_set_user(c, ACC_TERMINAL_CONSUMED);
            pthread_mutex_lock(&g_acc.mu);
            g_acc.closed_polled++;
            pthread_cond_broadcast(&g_acc.cv);
            pthread_mutex_unlock(&g_acc.mu);
        }

        pthread_mutex_lock(&g_acc.mu);
        bool ack_now = g_acc.may_ack;
        pthread_mutex_unlock(&g_acc.mu);
        if (ack_now &&
            moq_wtquic_msquic_managed_conn_user(c) == ACC_TERMINAL_CONSUMED) {
            moq_result_t rc = moq_wtquic_msquic_managed_conn_ack_terminal(c);
            /* a second acknowledgment in this same callback is harmless */
            moq_result_t rc2 = moq_wtquic_msquic_managed_conn_ack_terminal(c);
            pthread_mutex_lock(&g_acc.mu);
            if (rc == MOQ_OK) g_acc.acks_ok++;
            CHECK(rc2 == MOQ_OK);
            pthread_cond_broadcast(&g_acc.cv);
            pthread_mutex_unlock(&g_acc.mu);
        }
    }
    pthread_mutex_lock(&g_acc.mu);
    g_acc.pumps++;
    pthread_cond_broadcast(&g_acc.cv);
    pthread_mutex_unlock(&g_acc.mu);
    return 0;
}

/* Signal-only, and — unlike the pump counter — raised AFTER the managed cycle
 * completes, so a waiter that observes it knows the reaper has already run. */
static void acc_activity(moq_wtquic_msquic_managed_t *m, void *user)
{
    (void)m; (void)user;
    pthread_mutex_lock(&g_acc.mu);
    g_acc.cycles++;
    pthread_cond_broadcast(&g_acc.cv);
    pthread_mutex_unlock(&g_acc.mu);
}

/* Connect one raw client and wait for establishment. */
static bool acc_connect(wtq_msquic_env_t *env, uint16_t port,
                        struct raw_cli *rc, wtq_session_t **out)
{
    static const char *const cprotos[] = { "moqt-18" };
    memset(rc, 0, sizeof(*rc));
    CHECK(pthread_mutex_init(&rc->mu, NULL) == 0);
    CHECK(pthread_cond_init(&rc->cv, NULL) == 0);
    wtq_session_events_t cev;
    wtq_session_events_init(&cev);
    cev.on_established = raw_established;
    wtq_connect_config_t connect = WTQ_CONNECT_CONFIG_INIT;
    connect.authority = "localhost";
    connect.path = "/moq";
    connect.subprotocols = cprotos;
    connect.subprotocol_count = 1;
    wtq_msquic_client_cfg_t cli = WTQ_MSQUIC_CLIENT_CFG_INIT;
    cli.server_name = "127.0.0.1";
    cli.port = port;
    cli.insecure_skip_verify = true;
    cli.connect = &connect;
    cli.events = &cev;
    cli.user = rc;
    if (wtq_msquic_client_connect(env, &cli, out) != WTQ_OK)
        return false;
    return raw_wait_established(rc);
}

static void run_terminal_conservation_case(void)
{
    const char *offers[] = { "moqt-18", "moqt-16" };
    moq_wtquic_msquic_managed_cfg_t cfg;
    moq_wtquic_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.host = "127.0.0.1";
    cfg.port = 0;
    cfg.cert_path = cert_path;
    cfg.key_path = key_path;
    cfg.wt_path = "/moq";
    cfg.wt_protocols = offers;
    cfg.wt_protocol_count = 2;
    cfg.max_connections = 1;      /* one slot: the hold is observable */
    cfg.lane_count = 1;
    cfg.on_lane_pump = acc_pump;
    cfg.on_activity = acc_activity;

    acc_init();
    moq_wtquic_msquic_managed_t *srv = NULL;
    CHECK(moq_wtquic_msquic_managed_create(&cfg, &srv) == MOQ_OK);
    if (srv == NULL) { acc_destroy(); return; }
    uint16_t port = moq_wtquic_msquic_managed_port(srv);
    CHECK(port != 0);

    wtq_msquic_env_cfg_t ecfg = WTQ_MSQUIC_ENV_CFG_INIT;
    wtq_msquic_env_t *cenv = NULL;   /* env for the first client only */
    CHECK(wtq_msquic_env_open(&ecfg, &cenv) == WTQ_OK);
    wtq_msquic_env_t *cenv2 = NULL;  /* env for the later admission probe */
    CHECK(wtq_msquic_env_open(&ecfg, &cenv2) == WTQ_OK);

    struct raw_cli rc1;
    wtq_session_t *cs1 = NULL;
    if (cenv != NULL && port != 0 && acc_connect(cenv, port, &rc1, &cs1)) {
        uint64_t tterm = 0, ack = 0, reaped = 0, refused = 0;
        uint32_t held = 0;

        /* a real child with a real MoQ session is established */
        CHECK(moq_wtquic_msquic_managed_conn_count(srv) == 1);
        moq_version_t ver = 0;
        bool has_adapter = false, has_ws = false;
        CHECK(moq_wtquic_msquic_managed_test_server_child(srv, &ver,
                                                          &has_adapter, &has_ws));
        CHECK(ver == MOQ_VERSION_DRAFT_18);
        CHECK(has_adapter && has_ws);

        /* The peer goes away for real: close its env (which shuts the QUIC
         * connection down) and drop the creator reference. The server child
         * then reaches its transport terminal and its session enqueues
         * MOQ_EVENT_SESSION_CLOSED. */
        wtq_msquic_env_close(cenv);
        wtq_session_release(cs1);
        cs1 = NULL;
        cenv = NULL;

        /* Nobody polls yet. The pump is presented the child, its
         * acknowledgment is refused because the terminal was never observed,
         * and the child keeps its capacity slot. */
        CHECK(acc_wait(acc_pred_terminal_unpolled));
        pthread_mutex_lock(&g_acc.mu);
        CHECK(g_acc.acks_wrong_state >= 1);
        CHECK(g_acc.acks_ok == 0);
        pthread_mutex_unlock(&g_acc.mu);
        /* repeated pumps do not reclaim an unacknowledged terminal child */
        CHECK(acc_force_cycle(srv));
        CHECK(acc_force_cycle(srv));
        CHECK(moq_wtquic_msquic_managed_conn_count(srv) == 1); /* held */

        moq_wtquic_msquic_managed_test_terminal_counters(
            srv, &tterm, &ack, &reaped, &refused, &held);
        CHECK(tterm == 1);   /* session-backed transport terminal */
        CHECK(ack == 0 && reaped == 0);
        CHECK(held == 1);    /* the slot is held by contract */

        /* the reserve is full, and the refusal is attributed to that hold */
        struct raw_cli rc_blocked;
        wtq_session_t *cs_blocked = NULL;
        (void)acc_connect(cenv2, port, &rc_blocked, &cs_blocked);
        if (cs_blocked != NULL) { wtq_session_release(cs_blocked); cs_blocked = NULL; }
        pthread_cond_destroy(&rc_blocked.cv);
        pthread_mutex_destroy(&rc_blocked.mu);
        moq_wtquic_msquic_managed_test_terminal_counters(
            srv, &tterm, &ack, &reaped, &refused, &held);
        CHECK(refused >= 1);

        /* The application POLLS the real terminal event but does not
         * acknowledge: observation alone must not reclaim the child. */
        pthread_mutex_lock(&g_acc.mu);
        g_acc.draining = true;
        pthread_mutex_unlock(&g_acc.mu);
        (void)moq_wtquic_msquic_lane_wake(
            moq_wtquic_msquic_managed_lane(srv, 0));
        CHECK(acc_wait(acc_pred_polled));      /* a REAL SESSION_CLOSED */
        CHECK(acc_force_cycle(srv));
        CHECK(moq_wtquic_msquic_managed_conn_count(srv) == 1); /* still held */
        moq_wtquic_msquic_managed_test_terminal_counters(
            srv, &tterm, &ack, &reaped, &refused, &held);
        CHECK(ack == 0 && reaped == 0 && held == 1);

        /* The application acknowledges from the owning callback;
         * only now is the child reclaimed and its slot returned. */
        pthread_mutex_lock(&g_acc.mu);
        g_acc.may_ack = true;
        pthread_mutex_unlock(&g_acc.mu);
        (void)moq_wtquic_msquic_lane_wake(
            moq_wtquic_msquic_managed_lane(srv, 0));
        CHECK(acc_wait(acc_pred_acked));
        CHECK(acc_force_cycle(srv));   /* the cycle after the ack reclaims it */

        /* Reclamation actually happened: the child is gone, the slot is back,
         * and the reap counter moved. `held` alone would not prove this -- it
         * drops inside acknowledgment, before the reaper runs. */
        moq_wtquic_msquic_managed_test_terminal_counters(
            srv, &tterm, &ack, &reaped, &refused, &held);
        CHECK(ack == 1);
        CHECK(reaped == 1);
        CHECK(held == 0);
        CHECK(moq_wtquic_msquic_managed_conn_count(srv) == 0);

        /* Capacity is genuinely restored: a replacement client establishes a
         * real session on the freed slot. */
        struct raw_cli rc2;
        wtq_session_t *cs2 = NULL;
        CHECK(acc_connect(cenv2, port, &rc2, &cs2));
        CHECK(moq_wtquic_msquic_managed_conn_count(srv) == 1);
        moq_version_t ver2 = 0;
        bool has_adapter2 = false, has_ws2 = false;
        CHECK(moq_wtquic_msquic_managed_test_server_child(srv, &ver2,
                                                          &has_adapter2,
                                                          &has_ws2));
        CHECK(ver2 == MOQ_VERSION_DRAFT_18);   /* a real MoQ session, not just admission */
        CHECK(has_adapter2 && has_ws2);
        if (cs2 != NULL) wtq_session_release(cs2);
        pthread_cond_destroy(&rc2.cv);
        pthread_mutex_destroy(&rc2.mu);
    }

    CHECK(moq_wtquic_msquic_managed_stop(srv) == MOQ_OK);
    moq_wtquic_msquic_managed_destroy(srv);
    if (cenv != NULL)
        wtq_msquic_env_close(cenv);
    if (cenv2 != NULL)
        wtq_msquic_env_close(cenv2);
    if (cs1 != NULL)
        wtq_session_release(cs1);
    pthread_cond_destroy(&rc1.cv);
    pthread_mutex_destroy(&rc1.mu);
    acc_destroy();
}

int main(int argc, char **argv)
{
    if (argc < 3) {
        fprintf(stderr, "usage: %s <cert.pem> <key.pem>\n", argv[0]);
        return 2;
    }
    cert_path = argv[1];
    key_path = argv[2];

    run_case("moqt-16", MOQ_VERSION_DRAFT_16);
    run_case("moqt-18", MOQ_VERSION_DRAFT_18);
    run_server_case();
    run_terminal_conservation_case();

    if (g_fail != 0) {
        fprintf(stderr, "FAILED: test_wtquic_msquic_managed_loopback (%d)\n",
                g_fail);
        return 1;
    }
    printf("PASS: test_wtquic_msquic_managed_loopback\n");
    return 0;
}
