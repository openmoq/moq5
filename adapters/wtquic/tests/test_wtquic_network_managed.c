/*
 * Managed Network.framework MoQ client against a real wtquic MsQuic
 * server on localhost: the full vertical (moq_session -> transport
 * bridge -> wtquic adapter -> wtq_nw_conn -> Network.framework) driven
 * end-to-end through the public facade only.
 *
 * The server side is the attach shape from test_wtquic_loopback: a
 * pre-created MoQ server session + attach conn behind an MsQuic
 * listener. The client side is moq_wtquic_network_managed: creation dials,
 * negotiation and session construction happen on the wtq_nw_conn
 * domain, and the test's on_lane_pump drives the exchange through the
 * unified lane interface (lane 0's one connection view). The main
 * thread only calls off-domain-safe facade APIs (wait, wake,
 * snapshots, stop_begin/join/stop, destroy) — every pump structurally
 * re-checks the lane contract (iteration, view accessors, actor
 * escape-hatch agreement) via cli.lane_violations.
 *
 * Threading: everything MoQ runs on the Network.framework connection
 * domain; the main thread synchronizes on flags the pump sets.
 *
 * Earliest-callback coverage (composed proof, intentional): the create
 * path publishes the wtq_nw_conn output slot to wtquic under m->mu
 * before wtq_nw_conn_create returns, so a callback that fires during
 * create sees a usable conn (see wtquic_network_managed.c). That window
 * is NOT pinned in one executable here: the deterministic seam
 * (wtq_nw_test_on_earliest) lives only in wtquic's network-testing
 * component, which wtquic never installs or exports, so a libmoq test
 * cannot reach it without distorting the production package boundary.
 * The property is instead proven by composition: (1) wtquic's own
 * t_earliest_callback_publication proves the output slot is published
 * and usable for post()/stop_begin() before create returns; (2) this
 * facade passes that callback-visible slot directly as &m->conn under
 * the mutex; (3) this suite drives the full on_lane_pump -> pump_exit
 * -> stop_begin lifecycle against a real connection.
 */

#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <moq/moq.h>
#include <moq/wtquic.h>
#include <moq/wtquic_network_managed.h>

#include <wtquic/wtquic_msquic.h>

static int failures = 0;

#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

#define WAIT_SECS 30

static const char *cert_path;
static const char *key_path;

/* --- MsQuic server fixture (loopback attach shape) ------------------------- */

/* What the server publishes on SUBSCRIBE. */
enum srv_mode {
    SRV_PUBLISH_ONE = 0,   /* one object in one FIN-closed subgroup    */
    SRV_ABORT_THEN_ONE,    /* subgroup A: object + RESET; subgroup B:
                              object + FIN — the reset must never look
                              like a clean subgroup end downstream      */
};

struct srv {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    enum srv_mode mode;
    int accepted;
    int closed;
    uint64_t next_group;

    moq_session_t *ms;
    moq_wtquic_conn_t *conn;
    wtq_msquic_env_t *env;
    wtq_msquic_listener_t *listener;
    wtq_serve_config_t serve;
    uint16_t port;
};

static void srv_publish(struct srv *v, moq_session_t *s,
                        moq_subscription_t sub, const char *payload,
                        bool reset)
{
    moq_subgroup_cfg_t sgcfg;
    moq_subgroup_handle_t sg;
    moq_rcbuf_t *buf = NULL;
    moq_result_t rc;

    moq_subgroup_cfg_init(&sgcfg);
    sgcfg.group_id = v->next_group++;
    sgcfg.publisher_priority = 200;
    rc = moq_session_open_subgroup(s, sub, &sgcfg, 0, &sg);
    if (rc < 0) {
        fprintf(stderr, "srv open_subgroup(%s)=%d\n", payload, (int)rc);
        failures++;
        return;
    }
    if (moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)payload,
                         strlen(payload), &buf) < 0)
        return;
    rc = moq_session_write_object(s, sg, 0, buf, 0);
    moq_rcbuf_decref(buf);
    if (rc < 0) {
        fprintf(stderr, "srv write_object(%s)=%d\n", payload, (int)rc);
        failures++;
    }
    rc = reset ? moq_session_reset_subgroup(s, sg, 7, 0)
               : moq_session_close_subgroup(s, sg, 0);
    if (rc < 0) {
        fprintf(stderr, "srv %s(%s)=%d\n",
                reset ? "reset_subgroup" : "close_subgroup", payload,
                (int)rc);
        failures++;
    }
}

static void srv_hook(moq_wtquic_conn_t *conn, void *user)
{
    struct srv *v = user;
    moq_session_t *s = moq_wtquic_conn_session(conn);
    moq_event_t ev;

    while (moq_session_poll_events(s, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
            moq_subscription_t sub = ev.u.subscribe_request.sub;
            moq_accept_subscribe_cfg_t acfg;

            moq_accept_subscribe_cfg_init(&acfg);
            if (moq_session_accept_subscribe(s, sub, &acfg, 0) >= 0) {
                if (v->mode == SRV_ABORT_THEN_ONE) {
                    srv_publish(v, s, sub, "will-abort", true);
                    srv_publish(v, s, sub, "after-abort", false);
                } else {
                    srv_publish(v, s, sub, "hello-nw-managed", false);
                }
                pthread_mutex_lock(&v->mu);
                v->accepted = 1;
                pthread_cond_broadcast(&v->cv);
                pthread_mutex_unlock(&v->mu);
            }
        }
        moq_event_cleanup(&ev);
    }
    if (moq_wtquic_conn_is_closed(conn) || moq_wtquic_conn_is_fatal(conn)) {
        pthread_mutex_lock(&v->mu);
        v->closed = 1;
        pthread_cond_broadcast(&v->cv);
        pthread_mutex_unlock(&v->mu);
    }
}

static int srv_start(struct srv *v, const char *const *protos,
                     size_t proto_count, bool require_subprotocol,
                     moq_version_t version, enum srv_mode mode)
{
    memset(v, 0, sizeof(*v));
    pthread_mutex_init(&v->mu, NULL);
    pthread_cond_init(&v->cv, NULL);
    v->mode = mode;

    moq_session_cfg_t scfg;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_SERVER);
    scfg.send_request_capacity = 1;
    scfg.initial_request_capacity = 10;
    if (version != 0)
        scfg.version = version;
    if (moq_session_create(&scfg, 0, &v->ms) < 0)
        return -1;

    moq_wtquic_conn_cfg_t ccfg;
    moq_wtquic_conn_cfg_init_sized(&ccfg, sizeof(ccfg));
    ccfg.alloc = moq_alloc_default();
    ccfg.session = v->ms;
    ccfg.hook = srv_hook;
    ccfg.hook_user = v;
    if (moq_wtquic_conn_create(&ccfg, &v->conn) < 0)
        return -1;

    wtq_msquic_env_cfg_t ecfg = WTQ_MSQUIC_ENV_CFG_INIT;
    if (wtq_msquic_env_open(&ecfg, &v->env) != WTQ_OK)
        return -1;

    v->serve = (wtq_serve_config_t)WTQ_SERVE_CONFIG_INIT;
    v->serve.path = "/moq";
    v->serve.subprotocols = protos;
    v->serve.subprotocol_count = proto_count;
    v->serve.require_subprotocol = require_subprotocol;

    wtq_msquic_listener_cfg_t lcfg = WTQ_MSQUIC_LISTENER_CFG_INIT;
    lcfg.bind_address = "127.0.0.1";
    lcfg.port = 0;
    lcfg.cert_file = cert_path;
    lcfg.key_file = key_path;
    lcfg.paths = &v->serve;
    lcfg.path_count = 1;
    lcfg.events = moq_wtquic_conn_events();
    lcfg.user = v->conn;
    if (wtq_msquic_listener_start(v->env, &lcfg, &v->listener) != WTQ_OK)
        return -1;
    v->port = wtq_msquic_listener_port(v->listener);
    return 0;
}

/* Abruptly tear the transport out from under a live client. */
static void srv_kill_transport(struct srv *v)
{
    if (v->listener != NULL) {
        wtq_msquic_listener_stop(v->listener);
        v->listener = NULL;
    }
    if (v->env != NULL) {
        wtq_msquic_env_close(v->env);
        v->env = NULL;
    }
}

static void srv_stop(struct srv *v)
{
    if (v->listener != NULL)
        wtq_msquic_listener_stop(v->listener);
    if (v->env != NULL)
        wtq_msquic_env_close(v->env);
    if (v->conn != NULL)
        moq_wtquic_conn_destroy(v->conn);
    if (v->ms != NULL)
        moq_session_destroy(v->ms);
    pthread_mutex_destroy(&v->mu);
    pthread_cond_destroy(&v->cv);
}

static bool srv_wait(struct srv *v, const int *flag)
{
    struct timespec dl;
    bool ok = true, set;

    clock_gettime(CLOCK_REALTIME, &dl);
    dl.tv_sec += WAIT_SECS;
    pthread_mutex_lock(&v->mu);
    while (*flag == 0 && ok)
        ok = pthread_cond_timedwait(&v->cv, &v->mu, &dl) == 0;
    set = *flag != 0;
    pthread_mutex_unlock(&v->mu);
    return set;
}

/* --- managed client driver --------------------------------------------------- */

struct cli {
    pthread_mutex_t mu;
    pthread_cond_t cv;

    int setup_done;
    int subscribed;
    int sub_ok;
    int objects;             /* OBJECT_RECEIVED count                  */
    int finished;            /* SUBGROUP_FINISHED count                */
    int got_expected;        /* payload matched `expect`               */
    int pump_session_null;   /* in-window lane view/session was NULL   */
    int pumps;               /* on_lane_pump invocations (under mu)    */
    int lane_violations;     /* structural lane-contract breaks in-pump */
    int terminal_view_seen;  /* a pump observed the adapter terminal
                                (closed/fatal) with the conn view still
                                iterable — terminal stays visible       */
    int reentered;           /* pump re-entered (must stay 0)          */
    bool in_pump_flag;       /* reentry detector (domain-confined)     */

    const char *expect;      /* payload that sets got_expected         */

    /* the conn view stashed by the pump, read OFF-domain by tests to
     * prove strict accessors refuse outside the window */
    moq_wtquic_network_managed_conn_t *_Atomic seen_view;

    atomic_int stopped_seen; /* facade on_stopped delivered            */
    atomic_int pump_after_stopped; /* pump ran during/after on_stopped
                                      (must stay 0)                     */
    atomic_int please_close; /* wake-posted close job (actor shape)    */
    atomic_int exit_on_setup;/* pump returns 1 after SETUP_COMPLETE    */
    atomic_int subscribe_in_pump; /* pump issues one more SUBSCRIBE     */
    atomic_int wake_in_pump; /* pump issues one lane_wake on itself    */
    atomic_int wake_in_pump_rc;
    int stop_on_domain_rc;   /* rc of stop() called ON the domain      */

    /* App service-deadline (idle-wake discriminator): 0 = disarmed
     * (query returns UINT64_MAX); nonzero = a self-re-arming periodic
     * deadline `now + interval`, so an idle domain must keep waking. */
    _Atomic uint64_t deadline_interval_us;

    uint8_t payload[64];
    size_t payload_len;
};

/* The app service-deadline query wired into every managed cfg. Pure and
 * non-reentrant (reads one atomic + the monotonic clock), per the facade
 * contract. Disarmed by default so it is inert in every other case. */
static uint64_t cli_app_deadline(void *ctx)
{
    struct cli *c = ctx;
    uint64_t iv = atomic_load(&c->deadline_interval_us);
    if (iv == 0)
        return UINT64_MAX;
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    uint64_t now = (uint64_t)ts.tv_sec * 1000000u + (uint64_t)ts.tv_nsec / 1000u;
    return now + iv;
}

static void cli_init(struct cli *c, const char *expect)
{
    memset(c, 0, sizeof(*c));
    pthread_mutex_init(&c->mu, NULL);
    pthread_cond_init(&c->cv, NULL);
    c->expect = expect;
    c->stop_on_domain_rc = MOQ_OK; /* anything but WRONG_STATE */
}

static void cli_destroy(struct cli *c)
{
    pthread_mutex_destroy(&c->mu);
    pthread_cond_destroy(&c->cv);
}

static bool cli_wait(struct cli *c, const int *flag)
{
    struct timespec dl;
    bool ok = true, set;

    clock_gettime(CLOCK_REALTIME, &dl);
    dl.tv_sec += WAIT_SECS;
    pthread_mutex_lock(&c->mu);
    while (*flag == 0 && ok)
        ok = pthread_cond_timedwait(&c->cv, &c->mu, &dl) == 0;
    set = *flag != 0;
    pthread_mutex_unlock(&c->mu);
    return set;
}

static void cli_subscribe(moq_session_t *s)
{
    moq_subscribe_cfg_t sc;
    moq_subscription_t sub;
    static const moq_bytes_t ns[] = {
        { (const uint8_t *)"wtq", 3 },
        { (const uint8_t *)"nw", 2 },
    };

    moq_subscribe_cfg_init(&sc);
    sc.track_namespace.parts = (moq_bytes_t *)ns;
    sc.track_namespace.count = 2;
    sc.track_name = (moq_bytes_t){ (const uint8_t *)"video", 5 };
    sc.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
    (void)moq_session_subscribe(s, &sc, 0, &sub);
}

static int cli_pump(moq_wtquic_network_managed_t *m,
                    moq_wtquic_network_managed_lane_t *lane, uint64_t now_us,
                    void *ctx)
{
    struct cli *c = ctx;
    moq_event_t ev;
    int rc = 0;

    (void)now_us;
    /* reentry detector: a lane_wake from inside this pump must be
     * DEFERRED, never a recursive invocation */
    if (c->in_pump_flag)
        c->reentered++;
    c->in_pump_flag = true;
    /* on_stopped is the facade's FINAL block: a pump during/after it is
     * a contract violation the wake-vs-stop race test asserts against */
    if (atomic_load(&c->stopped_seen) != 0)
        atomic_fetch_add(&c->pump_after_stopped, 1);

    /* the strict lane window: iterate the one connection and pin the
     * whole unified contract structurally, every pass */
    moq_wtquic_network_managed_conn_t *view =
        moq_wtquic_network_lane_next_conn(lane, NULL);
    moq_session_t *s = moq_wtquic_network_managed_conn_session(view);
    moq_wtquic_conn_t *conn = moq_wtquic_network_managed_conn_adapter(view);

    if (view != NULL) {
        if (moq_wtquic_network_lane_next_conn(lane, view) != NULL)
            c->lane_violations++; /* one connection: must end */
        if (moq_wtquic_network_managed_conn_lane(view) != lane)
            c->lane_violations++;
        if (moq_wtquic_network_managed_lane(m, 0) != lane ||
            moq_wtquic_network_managed_lane_count(m) != 1 ||
            moq_wtquic_network_lane_index(lane) != 0)
            c->lane_violations++;
        /* the actor-domain escape hatch is legal here too and must
         * agree with the strict view accessors */
        if (moq_wtquic_network_managed_session(m) != s ||
            moq_wtquic_network_managed_adapter(m) != conn)
            c->lane_violations++;
        atomic_store(&c->seen_view, view);
        /* terminal visibility: the pump that observes the adapter
         * terminal must still have the connection view iterable (the
         * view survives until final teardown) */
        if (conn != NULL && (moq_wtquic_conn_is_fatal(conn) ||
                             moq_wtquic_conn_is_closed(conn))) {
            pthread_mutex_lock(&c->mu);
            c->terminal_view_seen = 1;
            pthread_cond_broadcast(&c->cv);
            pthread_mutex_unlock(&c->mu);
        }
    }

    if (atomic_exchange(&c->wake_in_pump, 0) != 0)
        atomic_store(&c->wake_in_pump_rc,
                     (int)moq_wtquic_network_lane_wake(lane));

    if (s == NULL || conn == NULL) {
        /* the window only opens once the negotiated session exists */
        pthread_mutex_lock(&c->mu);
        c->pump_session_null = 1;
        c->pumps++;
        pthread_cond_broadcast(&c->cv);
        pthread_mutex_unlock(&c->mu);
        c->in_pump_flag = false;
        return 0;
    }

    while (moq_session_poll_events(s, &ev, 1) > 0) {
        switch (ev.kind) {
        case MOQ_EVENT_SETUP_COMPLETE:
            pthread_mutex_lock(&c->mu);
            c->setup_done = 1;
            pthread_cond_broadcast(&c->cv);
            pthread_mutex_unlock(&c->mu);
            if (!c->subscribed) {
                c->subscribed = 1;
                cli_subscribe(s);
            }
            if (atomic_load(&c->exit_on_setup)) {
                /* the blocking stop() must refuse to run here */
                c->stop_on_domain_rc = moq_wtquic_network_managed_stop(m);
                rc = 1;
            }
            break;
        case MOQ_EVENT_SUBSCRIBE_OK:
            pthread_mutex_lock(&c->mu);
            c->sub_ok = 1;
            pthread_cond_broadcast(&c->cv);
            pthread_mutex_unlock(&c->mu);
            break;
        case MOQ_EVENT_OBJECT_RECEIVED:
            pthread_mutex_lock(&c->mu);
            c->objects++;
            if (ev.u.object_received.payload != NULL) {
                size_t l = moq_rcbuf_len(ev.u.object_received.payload);

                if (l <= sizeof(c->payload)) {
                    memcpy(c->payload,
                           moq_rcbuf_data(ev.u.object_received.payload),
                           l);
                    c->payload_len = l;
                }
                if (c->expect != NULL && l == strlen(c->expect) &&
                    memcmp(c->payload, c->expect, l) == 0)
                    c->got_expected = 1;
            }
            pthread_cond_broadcast(&c->cv);
            pthread_mutex_unlock(&c->mu);
            break;
        case MOQ_EVENT_SUBGROUP_FINISHED:
            pthread_mutex_lock(&c->mu);
            c->finished++;
            pthread_cond_broadcast(&c->cv);
            pthread_mutex_unlock(&c->mu);
            break;
        default:
            break;
        }
        moq_event_cleanup(&ev);
    }

    if (atomic_exchange(&c->subscribe_in_pump, 0) != 0) {
        /* a FRESH track, queued HERE in a possibly wake-initiated pump:
         * the facade must flush it (the wake path services the adapter)
         * or it never reaches the wire */
        moq_subscribe_cfg_t sc2;
        moq_subscription_t sub2;
        static const moq_bytes_t ns2[] = {
            { (const uint8_t *)"wtq", 3 },
            { (const uint8_t *)"nw", 2 },
        };
        moq_subscribe_cfg_init(&sc2);
        sc2.track_namespace.parts = (moq_bytes_t *)ns2;
        sc2.track_namespace.count = 2;
        sc2.track_name = (moq_bytes_t){ (const uint8_t *)"video2", 6 };
        sc2.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        int src2 = (int)moq_session_subscribe(s, &sc2, 0, &sub2);
        if (src2 < 0)
            c->lane_violations++;   /* the queue itself must succeed */
    }
    if (atomic_exchange(&c->please_close, 0) != 0)
        (void)wtq_session_close(moq_wtquic_conn_wtq_session(conn),
                                0, NULL, 0);
    pthread_mutex_lock(&c->mu);
    c->pumps++;
    pthread_cond_broadcast(&c->cv);
    pthread_mutex_unlock(&c->mu);
    c->in_pump_flag = false;
    return rc;
}

/* Wait until the pump count reaches at least `min` (bounded). */
static bool cli_wait_pumps(struct cli *c, int min)
{
    struct timespec dl;
    bool ok = true, met;

    clock_gettime(CLOCK_REALTIME, &dl);
    dl.tv_sec += WAIT_SECS;
    pthread_mutex_lock(&c->mu);
    while (c->pumps < min && ok)
        ok = pthread_cond_timedwait(&c->cv, &c->mu, &dl) == 0;
    met = c->pumps >= min;
    pthread_mutex_unlock(&c->mu);
    return met;
}

static void cli_cfg(moq_wtquic_network_managed_cfg_t *cfg, struct cli *c,
                    uint16_t port, const char *protos)
{
    moq_wtquic_network_managed_cfg_init(cfg);
    cfg->alloc = moq_alloc_default();
    cfg->host = "127.0.0.1";
    cfg->port = port;
    cfg->path = "/moq";
    cfg->authority = "localhost";
    cfg->wt_protocols = protos;
    cfg->insecure_skip_verify = true;
    cfg->on_lane_pump = cli_pump;
    cfg->on_lane_pump_user = c;
    cfg->send_request_capacity = true;
    cfg->initial_request_capacity = 10;
    cfg->app_deadline_us = cli_app_deadline;
    cfg->app_deadline_ctx = c;
}

/* Drain wait() until the facade reports terminal. */
static bool wait_terminal(moq_wtquic_network_managed_t *m)
{
    for (int i = 0; i < WAIT_SECS * 10; i++) {
        moq_result_t rc = moq_wtquic_network_managed_wait(m, 100 * 1000);

        if (rc == MOQ_ERR_CLOSED)
            return true;
        if (rc != MOQ_OK && rc != MOQ_DONE)
            return false;
    }
    return false;
}

/* --- sized-free-checking allocator --------------------------------------------- */

/* Records every live allocation's size and fails the run when a free
 * passes a size that does not match the original allocation (the core
 * sized-free contract), or when anything leaks. */
#define SF_SLOTS 512

struct sf_alloc {
    pthread_mutex_t mu;
    void *ptr[SF_SLOTS];
    size_t size[SF_SLOTS];
    int live;
    int mismatches;
    int overflow;
};

static void *sf_alloc_fn(size_t size, void *ctx)
{
    struct sf_alloc *a = ctx;
    void *p = malloc(size);

    if (p == NULL)
        return NULL;
    pthread_mutex_lock(&a->mu);
    int placed = 0;
    for (int i = 0; i < SF_SLOTS; i++)
        if (a->ptr[i] == NULL) {
            a->ptr[i] = p;
            a->size[i] = size;
            placed = 1;
            break;
        }
    if (!placed)
        a->overflow = 1;
    a->live++;
    pthread_mutex_unlock(&a->mu);
    return p;
}

static void *sf_realloc_fn(void *ptr, size_t old_size, size_t new_size,
                           void *ctx)
{
    struct sf_alloc *a = ctx;
    void *p;

    pthread_mutex_lock(&a->mu);
    for (int i = 0; i < SF_SLOTS; i++)
        if (ptr != NULL && a->ptr[i] == ptr) {
            if (a->size[i] != old_size)
                a->mismatches++;
            a->ptr[i] = NULL;
            break;
        }
    pthread_mutex_unlock(&a->mu);
    p = realloc(ptr, new_size);
    if (p != NULL) {
        pthread_mutex_lock(&a->mu);
        for (int i = 0; i < SF_SLOTS; i++)
            if (a->ptr[i] == NULL) {
                a->ptr[i] = p;
                a->size[i] = new_size;
                break;
            }
        if (ptr == NULL)
            a->live++;
        pthread_mutex_unlock(&a->mu);
    }
    return p;
}

static void sf_free_fn(void *ptr, size_t size, void *ctx)
{
    struct sf_alloc *a = ctx;

    if (ptr == NULL)
        return;
    pthread_mutex_lock(&a->mu);
    for (int i = 0; i < SF_SLOTS; i++)
        if (a->ptr[i] == ptr) {
            if (a->size[i] != size)
                a->mismatches++;
            a->ptr[i] = NULL;
            break;
        }
    a->live--;
    pthread_mutex_unlock(&a->mu);
    free(ptr);
}

/* --- cases -------------------------------------------------------------------- */

/* Full exchange at a given negotiated version, closed through the actor
 * shape: a wake-posted close job, terminal via wait(), stop_begin +
 * join, destroy. */
static void t_exchange(const char *name, const char *const *protos,
                       size_t proto_count, moq_version_t srv_version,
                       const char *offer, moq_version_t want_version)
{
    int before = failures;
    struct srv v;
    struct cli c;
    moq_wtquic_network_managed_t *m = NULL;
    moq_wtquic_network_managed_cfg_t cfg;

    cli_init(&c, "hello-nw-managed");
    CHECK(srv_start(&v, protos, proto_count, proto_count > 0,
                    srv_version, SRV_PUBLISH_ONE) == 0);
    cli_cfg(&cfg, &c, v.port, offer);
    CHECK(moq_wtquic_network_managed_create(&cfg, &m) == MOQ_OK);
    if (m == NULL)
        goto out;

    /* off-domain: the actor-domain accessors must refuse */
    CHECK(moq_wtquic_network_managed_session(m) == NULL);
    CHECK(moq_wtquic_network_managed_adapter(m) == NULL);
    /* off-domain: the strict lane window is closed */
    CHECK(moq_wtquic_network_lane_next_conn(
              moq_wtquic_network_managed_lane(m, 0), NULL) == NULL);

    CHECK(cli_wait(&c, &c.setup_done));
    CHECK(cli_wait(&c, &c.sub_ok));
    CHECK(srv_wait(&v, &v.accepted));
    CHECK(cli_wait(&c, &c.got_expected));
    CHECK(moq_wtquic_network_managed_negotiated_version(m) == want_version);
    CHECK(c.pump_session_null == 0);
    CHECK(c.lane_violations == 0);
    CHECK(c.reentered == 0);
    /* the pump's stashed conn view refuses OFF the window (off-domain) */
    moq_wtquic_network_managed_conn_t *view = atomic_load(&c.seen_view);
    CHECK(view != NULL);
    CHECK(moq_wtquic_network_managed_conn_session(view) == NULL);
    CHECK(moq_wtquic_network_managed_conn_adapter(view) == NULL);
    CHECK(moq_wtquic_network_managed_conn_lane(view) == NULL);

    /* actor shape: post the close job, await terminal, then stop */
    atomic_store(&c.please_close, 1);
    CHECK(moq_wtquic_network_managed_wake(m) == MOQ_OK);
    CHECK(wait_terminal(m));
    /* the pump pass that observed the terminal still had the conn view
     * iterable (bounded wait: the observing pump broadcasts the flag) */
    CHECK(cli_wait(&c, &c.terminal_view_seen));
    CHECK(moq_wtquic_network_managed_is_closed(m));
    CHECK(!moq_wtquic_network_managed_is_fatal(m));
    CHECK(moq_wtquic_network_managed_close_code(m) == 0);
    CHECK(srv_wait(&v, &v.closed));

    (void)moq_wtquic_network_managed_stop_begin(m);
    CHECK(moq_wtquic_network_managed_join(m) == MOQ_OK);
    /* post-stop: wake and wait must report terminal, not accept work */
    CHECK(moq_wtquic_network_managed_wake(m) == MOQ_ERR_CLOSED);
    CHECK(moq_wtquic_network_managed_wait(m, 0) == MOQ_ERR_CLOSED);
out:
    if (m != NULL)
        moq_wtquic_network_managed_destroy(m);
    srv_stop(&v);
    cli_destroy(&c);
    if (failures == before)
        printf("PASS: %s\n", name);
}

/* Idle-wake discriminator (the linchpin): once the exchange has quiesced,
 * a purely time-based app deadline must keep waking the domain with NO
 * transport event and NO explicit wake. A self-re-arming 100 ms deadline
 * drives periodic idle pumps — the facade folds it into wtquic's native
 * delayed doorbell after each service cycle. The pump delta over the idle
 * window is the discriminator: a facade that arms only once collapses it to
 * the single kick (p1 - p0 ~= 1) instead of the periodic wakes required. */
static void t_idle_deadline(void)
{
    int before = failures;
    struct srv v;
    struct cli c;
    moq_wtquic_network_managed_t *m = NULL;
    moq_wtquic_network_managed_cfg_t cfg;

    cli_init(&c, "hello-nw-managed");
    CHECK(srv_start(&v, NULL, 0, false, MOQ_VERSION_DRAFT_16,
                    SRV_PUBLISH_ONE) == 0);
    cli_cfg(&cfg, &c, v.port, "moqt-16");
    CHECK(moq_wtquic_network_managed_create(&cfg, &m) == MOQ_OK);
    if (m == NULL)
        goto out;

    /* establish + receive one object, then quiesce: no more subscribes,
     * no server data, no explicit wakes past this point. */
    CHECK(cli_wait(&c, &c.setup_done));
    CHECK(cli_wait(&c, &c.got_expected));

    pthread_mutex_lock(&c.mu);
    int p0 = c.pumps;
    pthread_mutex_unlock(&c.mu);

    /* arm a self-re-arming 100 ms deadline and kick ONE pump so the facade
     * picks it up and arms the delayed doorbell; nothing wakes it after. */
    atomic_store(&c.deadline_interval_us, 100 * 1000);
    CHECK(moq_wtquic_network_managed_wake(m) == MOQ_OK);

    /* idle ~1.2 s: only the native delayed doorbell can advance the pump
     * count (no wake(), no server traffic). */
    struct timespec idle = { 1, 200 * 1000 * 1000 };
    nanosleep(&idle, NULL);

    pthread_mutex_lock(&c.mu);
    int p1 = c.pumps;
    pthread_mutex_unlock(&c.mu);
    atomic_store(&c.deadline_interval_us, 0);  /* disarm before teardown */

    /* ~12 idle wakes expected at 100 ms over 1.2 s; require a clear
     * periodic signal well above any incidental transport pump. */
    CHECK(p1 - p0 >= 5);
    CHECK(c.reentered == 0);
    CHECK(c.lane_violations == 0);

    (void)moq_wtquic_network_managed_stop_begin(m);
    CHECK(moq_wtquic_network_managed_join(m) == MOQ_OK);
out:
    if (m != NULL)
        moq_wtquic_network_managed_destroy(m);
    srv_stop(&v);
    cli_destroy(&c);
    if (failures == before)
        printf("PASS: idle_deadline\n");
}

/* A tokenless server (no WT-Protocol echo): draft-16 legacy fallback
 * when moqt-16 was offered — the exchange must fully work. */
static void t_legacy_fallback(void)
{
    t_exchange("legacy_fallback_d16", NULL, 0, MOQ_VERSION_DRAFT_16,
               "moqt-16", MOQ_VERSION_DRAFT_16);
}

/* A tokenless server against an exact draft-18 offer: never silently
 * downgraded — terminal fatal, and no MoQ session is ever built. */
static void t_exact_d18_no_downgrade(void)
{
    int before = failures;
    struct srv v;
    struct cli c;
    moq_wtquic_network_managed_t *m = NULL;
    moq_wtquic_network_managed_cfg_t cfg;

    cli_init(&c, NULL);
    CHECK(srv_start(&v, NULL, 0, false, MOQ_VERSION_DRAFT_16,
                    SRV_PUBLISH_ONE) == 0);
    cli_cfg(&cfg, &c, v.port, "moqt-18");
    CHECK(moq_wtquic_network_managed_create(&cfg, &m) == MOQ_OK);
    if (m == NULL)
        goto out;

    CHECK(wait_terminal(m));
    CHECK(moq_wtquic_network_managed_is_fatal(m));
    CHECK(!moq_wtquic_network_managed_is_closed(m));
    CHECK(moq_wtquic_network_managed_negotiated_version(m) == 0);
    CHECK(c.setup_done == 0);
    CHECK(c.pump_session_null == 0); /* on_pump never ran: no session */

    CHECK(moq_wtquic_network_managed_stop(m) == MOQ_OK);
out:
    if (m != NULL)
        moq_wtquic_network_managed_destroy(m);
    srv_stop(&v);
    cli_destroy(&c);
    if (failures == before)
        printf("PASS: exact_d18_no_downgrade\n");
}

/* Whole-stream abort: the server RESETs one subgroup mid-flight and
 * FIN-closes another. The aborted stream must retire without ever
 * looking like a clean subgroup end, and teardown stays clean. */
static void t_abort_stream(void)
{
    int before = failures;
    struct srv v;
    struct cli c;
    moq_wtquic_network_managed_t *m = NULL;
    moq_wtquic_network_managed_cfg_t cfg;
    static const char *const protos[] = { "moqt-16" };

    cli_init(&c, "after-abort");
    CHECK(srv_start(&v, protos, 1, true, MOQ_VERSION_DRAFT_16,
                    SRV_ABORT_THEN_ONE) == 0);
    cli_cfg(&cfg, &c, v.port, "moqt-16");
    CHECK(moq_wtquic_network_managed_create(&cfg, &m) == MOQ_OK);
    if (m == NULL)
        goto out;

    CHECK(cli_wait(&c, &c.got_expected));
    CHECK(cli_wait(&c, &c.finished));

    atomic_store(&c.please_close, 1);
    CHECK(moq_wtquic_network_managed_wake(m) == MOQ_OK);
    CHECK(wait_terminal(m));
    CHECK(moq_wtquic_network_managed_is_closed(m));
    CHECK(!moq_wtquic_network_managed_is_fatal(m));
    /* exactly one clean subgroup end: the RESET stream produced none */
    pthread_mutex_lock(&c.mu);
    CHECK(c.finished == 1);
    pthread_mutex_unlock(&c.mu);

    CHECK(moq_wtquic_network_managed_stop(m) == MOQ_OK);
out:
    if (m != NULL)
        moq_wtquic_network_managed_destroy(m);
    srv_stop(&v);
    cli_destroy(&c);
    if (failures == before)
        printf("PASS: abort_stream\n");
}

/* post() racing stop(): every POST resolves exactly-once as accepted
 * (MOQ_OK — the closure RUNS, once) or refused-terminal
 * (MOQ_ERR_CLOSED — the closure NEVER runs); never lost, never
 * double-run, never a crash. wake() interleaves here only for churn —
 * its weaker contract (an MOQ_OK racing stop may be absorbed) is
 * pinned by t_wake_contract. */
#define HAMMERS 3

static _Atomic int hammer_executed;

static void hammer_job(void *ctx)
{
    (void)ctx;
    atomic_fetch_add(&hammer_executed, 1);
}

struct hammer {
    moq_wtquic_network_managed_t *m;
    int bad_rc;
    int post_accepted;
};

static void *hammer_main(void *arg)
{
    struct hammer *h = arg;

    for (int i = 0; i < 400; i++) {
        moq_result_t rc;

        if (i % 2 == 0) {
            rc = moq_wtquic_network_managed_post(h->m, hammer_job, NULL);
            if (rc == MOQ_OK)
                h->post_accepted++;
        } else {
            rc = moq_wtquic_network_managed_wake(h->m);
        }
        if (rc != MOQ_OK && rc != MOQ_ERR_CLOSED)
            h->bad_rc = (int)rc;
        if (rc == MOQ_ERR_CLOSED)
            break;
    }
    return NULL;
}

static void t_post_vs_stop(void)
{
    int before = failures;
    struct srv v;
    struct cli c;
    moq_wtquic_network_managed_t *m = NULL;
    moq_wtquic_network_managed_cfg_t cfg;
    static const char *const protos[] = { "moqt-16" };
    pthread_t th[HAMMERS];
    struct hammer h[HAMMERS];

    cli_init(&c, "hello-nw-managed");
    CHECK(srv_start(&v, protos, 1, true, MOQ_VERSION_DRAFT_16,
                    SRV_PUBLISH_ONE) == 0);
    cli_cfg(&cfg, &c, v.port, "moqt-16");
    CHECK(moq_wtquic_network_managed_create(&cfg, &m) == MOQ_OK);
    if (m == NULL)
        goto out;

    CHECK(cli_wait(&c, &c.setup_done));
    atomic_store(&hammer_executed, 0);
    for (int i = 0; i < HAMMERS; i++) {
        h[i].m = m;
        h[i].bad_rc = 0;
        h[i].post_accepted = 0;
        pthread_create(&th[i], NULL, hammer_main, &h[i]);
    }
    usleep(2000);
    CHECK(moq_wtquic_network_managed_stop(m) == MOQ_OK);
    int accepted = 0;
    for (int i = 0; i < HAMMERS; i++) {
        pthread_join(th[i], NULL);
        CHECK(h[i].bad_rc == 0);
        accepted += h[i].post_accepted;
    }
    /* exactly once: teardown completed (stop returned), so every
     * accepted closure has run — no more, no fewer */
    CHECK(atomic_load(&hammer_executed) == accepted);
    CHECK(moq_wtquic_network_managed_wake(m) == MOQ_ERR_CLOSED);
    CHECK(moq_wtquic_network_managed_post(m, hammer_job, NULL) ==
          MOQ_ERR_CLOSED);
    CHECK(atomic_load(&hammer_executed) == accepted);
out:
    if (m != NULL)
        moq_wtquic_network_managed_destroy(m);
    srv_stop(&v);
    cli_destroy(&c);
    if (failures == before)
        printf("PASS: post_vs_stop\n");
}

/* Stop from inside the domain: on_pump requests exit (pump_exit) and
 * proves the blocking stop() refuses to run on the domain; the facade
 * begins its own stop without blocking the domain. */
static void t_stop_from_domain(void)
{
    int before = failures;
    struct srv v;
    struct cli c;
    moq_wtquic_network_managed_t *m = NULL;
    moq_wtquic_network_managed_cfg_t cfg;
    static const char *const protos[] = { "moqt-16" };

    cli_init(&c, NULL);
    atomic_store(&c.exit_on_setup, 1);
    CHECK(srv_start(&v, protos, 1, true, MOQ_VERSION_DRAFT_16,
                    SRV_PUBLISH_ONE) == 0);
    cli_cfg(&cfg, &c, v.port, "moqt-16");
    CHECK(moq_wtquic_network_managed_create(&cfg, &m) == MOQ_OK);
    if (m == NULL)
        goto out;

    CHECK(cli_wait(&c, &c.setup_done));
    CHECK(moq_wtquic_network_managed_join(m) == MOQ_OK);
    CHECK(c.stop_on_domain_rc == MOQ_ERR_WRONG_STATE);
out:
    if (m != NULL)
        moq_wtquic_network_managed_destroy(m);
    srv_stop(&v);
    cli_destroy(&c);
    if (failures == before)
        printf("PASS: stop_from_domain\n");
}

/* Failure before establishment: the server refuses the extended
 * CONNECT (no such path) — terminal fatal with the refusal status, and
 * no MoQ session is ever constructed. (A pre-ready TLS/port failure is
 * NOT testable here: Network.framework emits no observable transition
 * for those — see the standing limitation in wtquic's NW backend — so
 * the CONNECT refusal is the deterministic pre-establishment failure
 * a public-surface test can drive.) */
static void t_refused_before_establishment(void)
{
    int before = failures;
    struct srv v;
    struct cli c;
    moq_wtquic_network_managed_t *m = NULL;
    moq_wtquic_network_managed_cfg_t cfg;
    static const char *const protos[] = { "moqt-16" };

    cli_init(&c, NULL);
    CHECK(srv_start(&v, protos, 1, true, MOQ_VERSION_DRAFT_16,
                    SRV_PUBLISH_ONE) == 0);
    cli_cfg(&cfg, &c, v.port, "moqt-16");
    cfg.path = "/nope";
    CHECK(moq_wtquic_network_managed_create(&cfg, &m) == MOQ_OK);
    if (m == NULL)
        goto out;

    CHECK(wait_terminal(m));
    CHECK(moq_wtquic_network_managed_is_fatal(m));
    CHECK(moq_wtquic_network_managed_fatal_code(m) == 404);
    CHECK(moq_wtquic_network_managed_negotiated_version(m) == 0);
    CHECK(c.setup_done == 0);
    CHECK(c.pump_session_null == 0); /* on_pump never ran: no session */

    CHECK(moq_wtquic_network_managed_stop(m) == MOQ_OK);
out:
    if (m != NULL)
        moq_wtquic_network_managed_destroy(m);
    srv_stop(&v);
    cli_destroy(&c);
    if (failures == before)
        printf("PASS: refused_before_establishment\n");
}

/* Transport failure on a live session: the server transport is torn
 * down abruptly under an established MoQ session — terminal fatal with
 * a transport-error record whose classification is stable across
 * off-domain reads, INCLUDING reads that race the failure itself (the
 * poller below overlaps the domain's snapshot publication; the TSan
 * lane checks that discipline). */
struct terr_poller {
    moq_wtquic_network_managed_t *m;
    _Atomic int stop;
};

static void *terr_poll_main(void *arg)
{
    struct terr_poller *tp = arg;

    while (!atomic_load(&tp->stop)) {
        wtq_transport_error_t rec;

        memset(&rec, 0, sizeof(rec));
        rec.struct_size = (uint32_t)sizeof(rec);
        (void)moq_wtquic_network_managed_transport_error(tp->m, &rec);
    }
    return NULL;
}

static void t_transport_failure_classified(void)
{
    int before = failures;
    struct srv v;
    struct cli c;
    moq_wtquic_network_managed_t *m = NULL;
    moq_wtquic_network_managed_cfg_t cfg;
    static const char *const protos[] = { "moqt-16" };

    cli_init(&c, "hello-nw-managed");
    CHECK(srv_start(&v, protos, 1, true, MOQ_VERSION_DRAFT_16,
                    SRV_PUBLISH_ONE) == 0);
    cli_cfg(&cfg, &c, v.port, "moqt-16");
    CHECK(moq_wtquic_network_managed_create(&cfg, &m) == MOQ_OK);
    if (m == NULL)
        goto out;

    CHECK(cli_wait(&c, &c.setup_done));
    struct terr_poller tp = { m, 0 };
    pthread_t poll_th;
    pthread_create(&poll_th, NULL, terr_poll_main, &tp);
    srv_kill_transport(&v);
    CHECK(wait_terminal(m));
    atomic_store(&tp.stop, 1);
    pthread_join(poll_th, NULL);
    CHECK(moq_wtquic_network_managed_is_fatal(m));

    /* the record must be present and BYTE-STABLE across off-domain
     * reads; its content depends on how the teardown surfaced on this
     * SDK (a local POSIX error when I/O was in flight, an empty record
     * when the close won the race), so only stability is asserted */
    wtq_transport_error_t e1, e2;
    memset(&e1, 0, sizeof(e1));
    memset(&e2, 0xff, sizeof(e2));
    e1.struct_size = e2.struct_size = (uint32_t)sizeof(e1);
    CHECK(moq_wtquic_network_managed_transport_error(m, &e1));
    CHECK(moq_wtquic_network_managed_transport_error(m, &e2));
    CHECK(memcmp(&e1, &e2, sizeof(e1)) == 0);
    printf("  transport error: kind=%u domain=%u native=%lld quic=%llu\n",
           (unsigned)e1.kind, (unsigned)e1.native_domain,
           (long long)e1.native_code, (unsigned long long)e1.quic_code);

    /* caller-sized copy-out: a SHORTER caller struct gets only the
     * bytes that fit — struct_size preserved, tail bytes untouched */
    union {
        wtq_transport_error_t rec;
        uint8_t raw[sizeof(wtq_transport_error_t)];
    } shortrec;
    memset(shortrec.raw, 0xAA, sizeof(shortrec.raw));
    const uint32_t short_size =
        (uint32_t)offsetof(wtq_transport_error_t, native_domain);
    shortrec.rec.struct_size = short_size;
    CHECK(moq_wtquic_network_managed_transport_error(m, &shortrec.rec));
    CHECK(shortrec.rec.struct_size == short_size);
    CHECK(shortrec.rec.kind == e1.kind);
    for (size_t i = short_size; i < sizeof(shortrec.raw); i++)
        if (shortrec.raw[i] != 0xAA) {
            fprintf(stderr, "FAIL: byte %zu past caller size written\n",
                    i);
            failures++;
            break;
        }
    /* undersized: too small to even carry `kind` is refused */
    wtq_transport_error_t tiny;
    memset(&tiny, 0, sizeof(tiny));
    tiny.struct_size = 2;
    CHECK(!moq_wtquic_network_managed_transport_error(m, &tiny));

    CHECK(moq_wtquic_network_managed_stop(m) == MOQ_OK);
out:
    if (m != NULL)
        moq_wtquic_network_managed_destroy(m);
    srv_stop(&v);
    cli_destroy(&c);
    if (failures == before)
        printf("PASS: transport_failure_classified\n");
}

/* Config validation and versioned prefixes: undersized rejected,
 * allocator without realloc rejected (the owned session's receive
 * buffering grows), oversized accepted, and a struct cut BETWEEN a
 * callback and its context yields the callback with a NULL context —
 * never a read past the caller's struct. */
static _Atomic intptr_t prefix_ctx_seen = -1;

static int prefix_pump(moq_wtquic_network_managed_t *m,
                       moq_wtquic_network_managed_lane_t *lane, uint64_t now_us,
                       void *ctx)
{
    (void)m;
    (void)lane;
    (void)now_us;
    atomic_store(&prefix_ctx_seen, (intptr_t)ctx);
    return 1; /* pump_exit: one observation is enough */
}

static void t_cfg_prefixes(void)
{
    int before = failures;
    struct srv v;
    moq_wtquic_network_managed_t *m = NULL;
    moq_wtquic_network_managed_cfg_t cfg;
    static const char *const protos[] = { "moqt-16" };

    CHECK(srv_start(&v, protos, 1, true, MOQ_VERSION_DRAFT_16,
                    SRV_PUBLISH_ONE) == 0);

    /* undersized v1 prefix: rejected before anything dials */
    moq_wtquic_network_managed_cfg_init(&cfg);
    cfg.alloc = moq_alloc_default();
    cfg.host = "127.0.0.1";
    cfg.port = v.port;
    cfg.struct_size = (uint32_t)offsetof(moq_wtquic_network_managed_cfg_t,
                                         insecure_skip_verify);
    CHECK(moq_wtquic_network_managed_create(&cfg, &m) == MOQ_ERR_INVAL);
    CHECK(m == NULL);

    /* allocator missing realloc: rejected at create, not at
     * establishment */
    moq_alloc_t norealloc = *moq_alloc_default();
    norealloc.realloc = NULL;
    moq_wtquic_network_managed_cfg_init(&cfg);
    cfg.alloc = &norealloc;
    cfg.host = "127.0.0.1";
    cfg.port = v.port;
    CHECK(moq_wtquic_network_managed_create(&cfg, &m) == MOQ_ERR_INVAL);

    /* struct cut after on_lane_pump but BEFORE on_lane_pump_user: the
     * callback is honoured, the context defaults to NULL — the
     * poisoned bytes after the cut are never read */
    atomic_store(&prefix_ctx_seen, -1);
    moq_wtquic_network_managed_cfg_init(&cfg);
    cfg.alloc = moq_alloc_default();
    cfg.host = "127.0.0.1";
    cfg.port = v.port;
    cfg.wt_protocols = "moqt-16";
    cfg.insecure_skip_verify = true;
    cfg.on_lane_pump = prefix_pump;
    cfg.on_lane_pump_user = (void *)(intptr_t)0x5A5A; /* poison past the
                                                         cut */
    cfg.struct_size = (uint32_t)(offsetof(moq_wtquic_network_managed_cfg_t,
                                          on_lane_pump) +
                                 sizeof(cfg.on_lane_pump));
    CHECK(moq_wtquic_network_managed_create(&cfg, &m) == MOQ_OK);
    if (m != NULL) {
        CHECK(wait_terminal(m)); /* pump_exit after the first pump */
        CHECK(moq_wtquic_network_managed_stop(m) == MOQ_OK);
        CHECK(atomic_load(&prefix_ctx_seen) == 0); /* NULL, not poison */
        moq_wtquic_network_managed_destroy(m);
        m = NULL;
    }

    /* oversized struct: accepted, unknown tail ignored */
    struct {
        moq_wtquic_network_managed_cfg_t base;
        uint64_t future_tail[4];
    } big;
    memset(&big, 0, sizeof(big));
    moq_wtquic_network_managed_cfg_init(&big.base);
    big.base.struct_size = (uint32_t)sizeof(big);
    big.base.alloc = moq_alloc_default();
    big.base.host = "127.0.0.1";
    big.base.port = v.port;
    big.base.wt_protocols = "moqt-16";
    big.base.insecure_skip_verify = true;
    big.future_tail[0] = 0xDEADBEEF;
    CHECK(moq_wtquic_network_managed_create(&big.base, &m) == MOQ_OK);
    if (m != NULL) {
        CHECK(moq_wtquic_network_managed_stop(m) == MOQ_OK);
        moq_wtquic_network_managed_destroy(m);
        m = NULL;
    }

    srv_stop(&v);
    if (failures == before)
        printf("PASS: cfg_prefixes\n");
}

/* Strict wt_protocols validation: malformed separators, empties,
 * duplicates, and unknown tokens are MOQ_ERR_INVAL before anything
 * dials; well-formed lists (spaces around commas included) are
 * accepted. */
static void t_protocol_validation(void)
{
    int before = failures;
    static const char *const bad[] = {
        "",                    /* empty list            */
        " ",                   /* only spaces           */
        ",",                   /* only separator        */
        "moqt-16,",            /* trailing comma        */
        ",moqt-16",            /* leading comma         */
        "moqt-16,,moqt-18",    /* empty token           */
        "moqt-16 moqt-18",     /* bare-space separator  */
        "moqt-99",             /* unrecognized token    */
        "h3",                  /* unrecognized token    */
        "moqt-16,moqt-16",     /* duplicate             */
        "moqt-16, moqt-18, moqt-16", /* duplicate, spaced */
    };
    static const char *const good[] = {
        "moqt-16",
        "moqt-18",
        "moqt-18,moqt-16",
        " moqt-18 , moqt-16 ",
    };
    moq_wtquic_network_managed_cfg_t cfg;
    moq_wtquic_network_managed_t *m = NULL;

    for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
        moq_wtquic_network_managed_cfg_init(&cfg);
        cfg.alloc = moq_alloc_default();
        cfg.host = "127.0.0.1";
        cfg.port = 9; /* never dialed: validation rejects first */
        cfg.wt_protocols = bad[i];
        m = NULL;
        if (moq_wtquic_network_managed_create(&cfg, &m) != MOQ_ERR_INVAL) {
            fprintf(stderr, "FAIL: bad list accepted: \"%s\"\n", bad[i]);
            failures++;
            if (m != NULL) {
                (void)moq_wtquic_network_managed_stop(m);
                moq_wtquic_network_managed_destroy(m);
            }
        }
    }
    for (size_t i = 0; i < sizeof(good) / sizeof(good[0]); i++) {
        moq_wtquic_network_managed_cfg_init(&cfg);
        cfg.alloc = moq_alloc_default();
        cfg.host = "127.0.0.1";
        cfg.port = 9; /* discard port: torn down before establishing */
        cfg.insecure_skip_verify = true;
        cfg.wt_protocols = good[i];
        m = NULL;
        if (moq_wtquic_network_managed_create(&cfg, &m) != MOQ_OK) {
            fprintf(stderr, "FAIL: good list rejected: \"%s\"\n",
                    good[i]);
            failures++;
        }
        if (m != NULL) {
            CHECK(moq_wtquic_network_managed_stop(m) == MOQ_OK);
            moq_wtquic_network_managed_destroy(m);
        }
    }
    if (failures == before)
        printf("PASS: protocol_validation\n");
}

/* Sized-free contract: every facade allocation is freed with its
 * ORIGINAL size (the NUL-split protocol buffer is the regression
 * magnet), and nothing leaks across create/stop/destroy. */
static void t_sized_free(void)
{
    int before = failures;
    struct sf_alloc sfa;
    moq_alloc_t alloc;
    moq_wtquic_network_managed_cfg_t cfg;
    moq_wtquic_network_managed_t *m = NULL;

    memset(&sfa, 0, sizeof(sfa));
    pthread_mutex_init(&sfa.mu, NULL);
    alloc = *moq_alloc_default();
    alloc.alloc = sf_alloc_fn;
    alloc.realloc = sf_realloc_fn;
    alloc.free = sf_free_fn;
    alloc.ctx = &sfa;

    moq_wtquic_network_managed_cfg_init(&cfg);
    cfg.alloc = &alloc;
    cfg.host = "127.0.0.1";
    cfg.port = 9; /* discard port: lifecycle only, no establishment */
    cfg.insecure_skip_verify = true;
    cfg.wt_protocols = "moqt-18, moqt-16";
    CHECK(moq_wtquic_network_managed_create(&cfg, &m) == MOQ_OK);
    if (m != NULL) {
        CHECK(moq_wtquic_network_managed_stop(m) == MOQ_OK);
        moq_wtquic_network_managed_destroy(m);
    }
    CHECK(sfa.overflow == 0);
    CHECK(sfa.mismatches == 0);
    CHECK(sfa.live == 0);
    pthread_mutex_destroy(&sfa.mu);
    if (failures == before)
        printf("PASS: sized_free\n");
}

static uint64_t noop_app_deadline(void *ctx) { (void)ctx; return UINT64_MAX; }

/* Create failure with the app-deadline block present leaks nothing: libmoq
 * owns NO timer thread or timer resource, so a failed create (here a bogus
 * WT-Protocol offer, rejected AFTER the app_deadline config-import) has
 * nothing to clean up. The sized-free allocator proves zero live bytes. */
static void t_create_fail_no_timer(void)
{
    int before = failures;
    struct sf_alloc sfa;
    moq_alloc_t alloc;
    moq_wtquic_network_managed_cfg_t cfg;
    moq_wtquic_network_managed_t *m = NULL;

    memset(&sfa, 0, sizeof(sfa));
    pthread_mutex_init(&sfa.mu, NULL);
    alloc = *moq_alloc_default();
    alloc.alloc = sf_alloc_fn;
    alloc.realloc = sf_realloc_fn;
    alloc.free = sf_free_fn;
    alloc.ctx = &sfa;

    moq_wtquic_network_managed_cfg_init(&cfg);
    cfg.alloc = &alloc;
    cfg.host = "127.0.0.1";
    cfg.port = 443;
    cfg.insecure_skip_verify = true;
    cfg.wt_protocols = "not-a-real-token";      /* rejected AFTER the block read */
    cfg.app_deadline_us = noop_app_deadline;    /* the app-deadline block IS set */
    cfg.app_deadline_ctx = NULL;
    CHECK(moq_wtquic_network_managed_create(&cfg, &m) == MOQ_ERR_INVAL);
    CHECK(m == NULL);
    CHECK(sfa.overflow == 0);
    CHECK(sfa.mismatches == 0);
    CHECK(sfa.live == 0);                        /* nothing armed, nothing leaked */
    pthread_mutex_destroy(&sfa.mu);
    if (failures == before)
        printf("PASS: create_fail_no_timer\n");
}

/* The post contract: submission order per thread, deferred-never-
 * inline on the domain, accepted runs exactly once, rejected never
 * runs — hammered against stop for the exactly-once accounting. */
#define POST_N 64

struct post_log {
    pthread_mutex_t mu;
    int seq[3 * POST_N + 4];
    int n;
    moq_wtquic_network_managed_t *m;
    _Atomic int inner_ran;
    int inner_deferred_ok;   /* inner post had NOT run inline */
    int inner_seq;           /* position the inner closure landed at */
};

static void post_log_append(struct post_log *lg, int tag)
{
    pthread_mutex_lock(&lg->mu);
    if (lg->n < (int)(sizeof(lg->seq) / sizeof(lg->seq[0])))
        lg->seq[lg->n++] = tag;
    pthread_mutex_unlock(&lg->mu);
}

static void post_tag_job(void *ctx)
{
    struct post_log *lg = (struct post_log *)(((intptr_t *)ctx)[0]);
    int tag = (int)(((intptr_t *)ctx)[1]);

    post_log_append(lg, tag);
    free(ctx);
}

static void post_inner_job(void *ctx)
{
    struct post_log *lg = ctx;

    pthread_mutex_lock(&lg->mu);
    lg->inner_seq = lg->n;
    lg->seq[lg->n++] = -2;
    pthread_mutex_unlock(&lg->mu);
    atomic_store(&lg->inner_ran, 1);
}

/* Posted from OFF-domain; posts the inner job from ON the domain and
 * verifies it did not run inline. */
static void post_outer_job(void *ctx)
{
    struct post_log *lg = ctx;

    post_log_append(lg, -1);
    if (moq_wtquic_network_managed_post(lg->m, post_inner_job, lg) == MOQ_OK &&
        atomic_load(&lg->inner_ran) == 0)
        lg->inner_deferred_ok = 1;
}

struct post_thread {
    struct post_log *lg;
    int base;         /* tag range [base, base+POST_N) */
    int accepted;
};

static void *post_thread_main(void *arg)
{
    struct post_thread *t = arg;

    for (int i = 0; i < POST_N; i++) {
        intptr_t *ctx = malloc(2 * sizeof(intptr_t));

        ctx[0] = (intptr_t)t->lg;
        ctx[1] = t->base + i;
        if (moq_wtquic_network_managed_post(t->lg->m, post_tag_job, ctx) ==
            MOQ_OK)
            t->accepted++;
        else
            free(ctx); /* rejected: never runs */
    }
    return NULL;
}

static void t_post_contract(void)
{
    int before = failures;
    struct post_log lg;
    moq_wtquic_network_managed_cfg_t cfg;
    moq_wtquic_network_managed_t *m = NULL;

    memset(&lg, 0, sizeof(lg));
    pthread_mutex_init(&lg.mu, NULL);

    moq_wtquic_network_managed_cfg_init(&cfg);
    cfg.alloc = moq_alloc_default();
    cfg.host = "127.0.0.1";
    cfg.port = 9; /* the domain runs regardless of establishment */
    cfg.insecure_skip_verify = true;
    CHECK(moq_wtquic_network_managed_create(&cfg, &m) == MOQ_OK);
    if (m == NULL)
        goto out;
    lg.m = m;

    /* deferred-never-inline + ordering across the domain boundary */
    CHECK(moq_wtquic_network_managed_post(m, post_outer_job, &lg) == MOQ_OK);

    /* two off-domain threads, disjoint tag ranges, racing posts */
    struct post_thread t1 = { &lg, 1000, 0 };
    struct post_thread t2 = { &lg, 2000, 0 };
    pthread_t th1, th2;
    pthread_create(&th1, NULL, post_thread_main, &t1);
    pthread_create(&th2, NULL, post_thread_main, &t2);
    pthread_join(th1, NULL);
    pthread_join(th2, NULL);

    /* drain: post a final marker and wait for it via polling the log */
    intptr_t *fin = malloc(2 * sizeof(intptr_t));
    fin[0] = (intptr_t)&lg;
    fin[1] = 9999;
    CHECK(moq_wtquic_network_managed_post(m, post_tag_job, fin) == MOQ_OK);
    for (int i = 0; i < WAIT_SECS * 100; i++) {
        pthread_mutex_lock(&lg.mu);
        int done = lg.n > 0 && lg.seq[lg.n - 1] == 9999;
        pthread_mutex_unlock(&lg.mu);
        if (done)
            break;
        usleep(10000);
    }

    pthread_mutex_lock(&lg.mu);
    /* exactly once: every accepted post appears exactly one time */
    CHECK(lg.n == 1 /*outer*/ + 1 /*inner*/ + t1.accepted + t2.accepted +
                      1 /*marker*/);
    /* per-thread submission order is preserved on the domain */
    int last1 = -1, last2 = -1, order_ok = 1;
    for (int i = 0; i < lg.n; i++) {
        int tag = lg.seq[i];
        if (tag >= 1000 && tag < 1000 + POST_N) {
            if (tag <= last1)
                order_ok = 0;
            last1 = tag;
        } else if (tag >= 2000 && tag < 2000 + POST_N) {
            if (tag <= last2)
                order_ok = 0;
            last2 = tag;
        }
    }
    CHECK(order_ok);
    /* the on-domain post was deferred, and ran after everything
     * submitted before it */
    CHECK(lg.inner_deferred_ok);
    CHECK(atomic_load(&lg.inner_ran) == 1);
    pthread_mutex_unlock(&lg.mu);

    CHECK(moq_wtquic_network_managed_stop(m) == MOQ_OK);
    /* rejected-never: post after stop is refused (nothing to observe
     * running — the log stays frozen) */
    int n_before;
    pthread_mutex_lock(&lg.mu);
    n_before = lg.n;
    pthread_mutex_unlock(&lg.mu);
    CHECK(moq_wtquic_network_managed_post(m, post_inner_job, &lg) ==
          MOQ_ERR_CLOSED);
    usleep(50000);
    pthread_mutex_lock(&lg.mu);
    CHECK(lg.n == n_before);
    pthread_mutex_unlock(&lg.mu);
out:
    if (m != NULL)
        moq_wtquic_network_managed_destroy(m);
    pthread_mutex_destroy(&lg.mu);
    if (failures == before)
        printf("PASS: post_contract\n");
}

/* The actor lifecycle, with NO join anywhere: an arbitrary posted
 * closure closes the MoQ transport, a second posted closure begins the
 * stop ON the domain, and the facade's on_stopped continuation —
 * running after the adapter and session were destroyed — resumes the
 * actor, which destroys the facade from inside it. The main thread
 * only ever blocks on the actor's own signal. */
struct actor {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    moq_wtquic_network_managed_t *m;
    int session_gone_in_cb; /* session()/adapter() NULL in on_stopped */
    int lane_closed_in_post;/* posted closure: escape hatch open, lane
                               window closed */
    int resumed;
};

static void actor_close_job(void *ctx)
{
    struct actor *a = ctx;
    /* the actor escape hatch: a POSTED closure is on the domain but
     * NOT in the lane window — the direct accessor works here... */
    moq_wtquic_conn_t *conn = moq_wtquic_network_managed_adapter(a->m);
    /* ...while the strict lane window stays closed */
    a->lane_closed_in_post =
        moq_wtquic_network_lane_next_conn(
            moq_wtquic_network_managed_lane(a->m, 0), NULL) == NULL &&
        moq_wtquic_network_managed_session(a->m) != NULL && conn != NULL;

    if (conn != NULL)
        (void)wtq_session_close(moq_wtquic_conn_wtq_session(conn),
                                0, NULL, 0);
}

static void actor_stop_job(void *ctx)
{
    struct actor *a = ctx;

    (void)moq_wtquic_network_managed_stop_begin(a->m); /* ON the domain */
}

static void actor_resume(void *ctx)
{
    struct actor *a = ctx;

    /* teardown already happened: the on-domain accessors are NULL */
    a->session_gone_in_cb =
        moq_wtquic_network_managed_session(a->m) == NULL &&
        moq_wtquic_network_managed_adapter(a->m) == NULL;
    /* the designed actor move: destroy from inside the continuation */
    moq_wtquic_network_managed_destroy(a->m);
    a->m = NULL;
    pthread_mutex_lock(&a->mu);
    a->resumed = 1;
    pthread_cond_broadcast(&a->cv);
    pthread_mutex_unlock(&a->mu);
}

static void t_actor_lifecycle(void)
{
    int before = failures;
    struct srv v;
    struct cli c;
    struct actor a;
    moq_wtquic_network_managed_cfg_t cfg;
    moq_wtquic_network_managed_t *m = NULL;
    static const char *const protos[] = { "moqt-16" };

    memset(&a, 0, sizeof(a));
    pthread_mutex_init(&a.mu, NULL);
    pthread_cond_init(&a.cv, NULL);
    cli_init(&c, "hello-nw-managed");
    CHECK(srv_start(&v, protos, 1, true, MOQ_VERSION_DRAFT_16,
                    SRV_PUBLISH_ONE) == 0);
    cli_cfg(&cfg, &c, v.port, "moqt-16");
    cfg.on_stopped = actor_resume;
    cfg.on_stopped_ctx = &a;
    CHECK(moq_wtquic_network_managed_create(&cfg, &m) == MOQ_OK);
    if (m == NULL)
        goto out;
    a.m = m;

    CHECK(cli_wait(&c, &c.got_expected));
    /* Arm a periodic app deadline so a native delayed doorbell is pending
     * ACROSS teardown: the on-domain stop must stay nonblocking (stop_begin
     * cancels the arm) and the on_stopped continuation must remain
     * immediate-destroy safe with an arm outstanding. Confirm the arm is
     * live by observing idle wakes before closing. */
    pthread_mutex_lock(&c.mu);
    int armed_base = c.pumps;
    pthread_mutex_unlock(&c.mu);
    atomic_store(&c.deadline_interval_us, 100 * 1000);
    CHECK(moq_wtquic_network_managed_wake(m) == MOQ_OK);
    CHECK(cli_wait_pumps(&c, armed_base + 3));   /* idle doorbell is firing */
    /* leave it ARMED (no disarm) through the teardown below */
    /* arbitrary posted MoQ close, then the on-domain stop */
    CHECK(moq_wtquic_network_managed_post(m, actor_close_job, &a) == MOQ_OK);
    CHECK(cli_wait(&c, &c.setup_done)); /* already set; keeps shape */
    CHECK(wait_terminal(m)); /* clean close latches */
    CHECK(moq_wtquic_network_managed_is_closed(m));
    CHECK(moq_wtquic_network_managed_post(m, actor_stop_job, &a) == MOQ_OK);

    /* NO join: block only on the actor's own resume signal */
    struct timespec dl;
    clock_gettime(CLOCK_REALTIME, &dl);
    dl.tv_sec += WAIT_SECS;
    pthread_mutex_lock(&a.mu);
    while (a.resumed == 0)
        if (pthread_cond_timedwait(&a.cv, &a.mu, &dl) == ETIMEDOUT)
            break;
    CHECK(a.resumed);
    pthread_mutex_unlock(&a.mu);
    CHECK(a.session_gone_in_cb);
    CHECK(a.lane_closed_in_post); /* the actor escape hatch worked in a
                                     posted closure while the strict
                                     lane window stayed closed */
    m = NULL; /* the continuation destroyed it */
out:
    if (m != NULL) {
        (void)moq_wtquic_network_managed_stop(m);
        moq_wtquic_network_managed_destroy(m);
    }
    srv_stop(&v);
    cli_destroy(&c);
    pthread_mutex_destroy(&a.mu);
    pthread_cond_destroy(&a.cv);
    if (failures == before)
        printf("PASS: actor_lifecycle\n");
}

/* wait() must not report stop COMPLETION merely because a stop was
 * REQUESTED: while a posted closure is still holding the domain, a
 * nonblocking wait after stop_begin reports MOQ_DONE, and only the
 * completed teardown reads as MOQ_ERR_CLOSED. */
static void wait_sema_sleeper(void *ctx)
{
    (void)ctx;
    usleep(300 * 1000); /* hold the domain so teardown cannot finish */
}

static void t_wait_not_premature(void)
{
    int before = failures;
    moq_wtquic_network_managed_cfg_t cfg;
    moq_wtquic_network_managed_t *m = NULL;

    moq_wtquic_network_managed_cfg_init(&cfg);
    cfg.alloc = moq_alloc_default();
    cfg.host = "127.0.0.1";
    cfg.port = 9; /* never establishes: no session outcome latches */
    cfg.insecure_skip_verify = true;
    CHECK(moq_wtquic_network_managed_create(&cfg, &m) == MOQ_OK);
    if (m == NULL)
        goto out;

    CHECK(moq_wtquic_network_managed_post(m, wait_sema_sleeper, NULL) ==
          MOQ_OK);
    CHECK(moq_wtquic_network_managed_stop_begin(m));
    /* teardown is pinned behind the sleeper: a requested-but-
     * incomplete stop must NOT read as terminal */
    CHECK(moq_wtquic_network_managed_wait(m, 0) == MOQ_DONE);
    CHECK(moq_wtquic_network_managed_wait(m, 50 * 1000) == MOQ_DONE);
    /* completion is what terminates the wait */
    for (int i = 0; i < WAIT_SECS * 10; i++) {
        moq_result_t rc = moq_wtquic_network_managed_wait(m, 100 * 1000);
        if (rc == MOQ_ERR_CLOSED)
            break;
        CHECK(rc == MOQ_OK || rc == MOQ_DONE);
    }
    CHECK(moq_wtquic_network_managed_wait(m, 0) == MOQ_ERR_CLOSED);
    CHECK(moq_wtquic_network_managed_join(m) == MOQ_OK); /* immediate */
out:
    if (m != NULL)
        moq_wtquic_network_managed_destroy(m);
    if (failures == before)
        printf("PASS: wait_not_premature\n");
}

/* wait() is LEVEL-RETAINED: pump activity that landed BEFORE the wait
 * (even long before) satisfies it immediately and is consumed exactly
 * once — the missed-wake regression behind the runtime proof's object
 * timeouts (an engine parked between two waits lost the only pump an
 * idle connection would ever deliver). */
static void t_wait_level_retained(void)
{
    int before = failures;
    struct srv v;
    struct cli c;
    moq_wtquic_network_managed_t *m = NULL;
    moq_wtquic_network_managed_cfg_t cfg;
    static const char *const protos[] = { "moqt-16" };

    cli_init(&c, "hello-nw-managed");
    CHECK(srv_start(&v, protos, 1, true, MOQ_VERSION_DRAFT_16,
                    SRV_PUBLISH_ONE) == 0);
    cli_cfg(&cfg, &c, v.port, "moqt-16");
    CHECK(moq_wtquic_network_managed_create(&cfg, &m) == MOQ_OK);
    if (m == NULL)
        goto out;

    /* establishment pumps ran before this thread waits. NO fixed
     * scheduler delay is assumed anywhere below: each retained-level
     * assertion polls NONBLOCKING waits under a wall deadline. With
     * level retention the first poll that runs after the pump
     * completed returns MOQ_OK; with edge semantics a zero-timeout
     * wait can essentially never observe a pump, so the deadline
     * expires and the check fails — the polling loop bounds
     * scheduling, it cannot mask the contract. */
    CHECK(cli_wait(&c, &c.setup_done));
    int seen = 0;
    for (int i = 0; i < WAIT_SECS * 1000 && !seen; i++) {
        if (moq_wtquic_network_managed_wait(m, 0) == MOQ_OK)
            seen = 1;
        else
            usleep(1000);
    }
    CHECK(seen); /* pre-wait pump activity was retained */

    /* consumed exactly once: drain to MOQ_DONE */
    int drains = 0;
    while (moq_wtquic_network_managed_wait(m, 0) == MOQ_OK && drains < 64)
        drains++;
    CHECK(moq_wtquic_network_managed_wait(m, 0) == MOQ_DONE);

    /* a fresh wake-driven pump re-arms the level exactly once more:
     * again observed via bounded nonblocking polling, never a fixed
     * sleep */
    CHECK(moq_wtquic_network_managed_wake(m) == MOQ_OK);
    seen = 0;
    for (int i = 0; i < WAIT_SECS * 1000 && !seen; i++) {
        if (moq_wtquic_network_managed_wait(m, 0) == MOQ_OK)
            seen = 1;
        else
            usleep(1000);
    }
    CHECK(seen); /* the wake-driven pump re-armed the level */

    CHECK(moq_wtquic_network_managed_stop(m) == MOQ_OK);
out:
    if (m != NULL)
        moq_wtquic_network_managed_destroy(m);
    srv_stop(&v);
    cli_destroy(&c);
    if (failures == before)
        printf("PASS: wait_level_retained\n");
}

/* Repeated connect / cancel / reconnect against one refusing listener
 * (no-overlap offer keeps the server's attach conn untouched, so every
 * iteration is a fresh pre-establishment lifecycle). Odd iterations
 * cancel immediately; even ones let the attempt make progress first. */
static void t_soak(void)
{
    int before = failures;
    struct srv v;
    static const char *const protos[] = { "moqt-16" };

    CHECK(srv_start(&v, protos, 1, true, MOQ_VERSION_DRAFT_16,
                    SRV_PUBLISH_ONE) == 0);
    for (int i = 0; i < 24 && failures == before; i++) {
        struct cli c;
        moq_wtquic_network_managed_t *m = NULL;
        moq_wtquic_network_managed_cfg_t cfg;

        cli_init(&c, NULL);
        cli_cfg(&cfg, &c, v.port, "moqt-18"); /* no overlap: refused */
        CHECK(moq_wtquic_network_managed_create(&cfg, &m) == MOQ_OK);
        if (m != NULL) {
            if (i % 2 == 0)
                (void)moq_wtquic_network_managed_wait(m, (i % 6) * 20000);
            CHECK(moq_wtquic_network_managed_stop(m) == MOQ_OK);
            CHECK(c.setup_done == 0);
            moq_wtquic_network_managed_destroy(m);
        }
        cli_destroy(&c);
    }
    srv_stop(&v);
    if (failures == before)
        printf("PASS: soak\n");
}

/* --- lane shape: counts, indexing, NULL-safety, off-domain refusal ----------- */

static void t_lane_shape(void)
{
    int before = failures;
    moq_wtquic_network_managed_cfg_t cfg;
    moq_wtquic_network_managed_t *m = NULL;

    /* NULL-safety across the whole lane surface */
    CHECK(moq_wtquic_network_managed_lane_count(NULL) == 0);
    CHECK(moq_wtquic_network_managed_lane(NULL, 0) == NULL);
    CHECK(moq_wtquic_network_lane_index(NULL) == 0);
    CHECK(moq_wtquic_network_lane_next_conn(NULL, NULL) == NULL);
    CHECK(moq_wtquic_network_lane_wake(NULL) == MOQ_ERR_INVAL);
    CHECK(moq_wtquic_network_managed_conn_session(NULL) == NULL);
    CHECK(moq_wtquic_network_managed_conn_adapter(NULL) == NULL);
    CHECK(moq_wtquic_network_managed_conn_lane(NULL) == NULL);

    /* a live facade (never establishes — port 9): one lane, index 0
     * only, stable handle, off-domain window closed */
    moq_wtquic_network_managed_cfg_init(&cfg);
    cfg.alloc = moq_alloc_default();
    cfg.host = "127.0.0.1";
    cfg.port = 9;
    cfg.insecure_skip_verify = true;
    CHECK(moq_wtquic_network_managed_create(&cfg, &m) == MOQ_OK);
    if (m == NULL)
        goto out;
    CHECK(moq_wtquic_network_managed_lane_count(m) == 1);
    moq_wtquic_network_managed_lane_t *lane = moq_wtquic_network_managed_lane(m, 0);
    CHECK(lane != NULL);
    CHECK(moq_wtquic_network_managed_lane(m, 1) == NULL);
    CHECK(moq_wtquic_network_managed_lane(m, 7) == NULL);
    CHECK(moq_wtquic_network_lane_index(lane) == 0);
    /* off-domain, and pre-establishment anyway: no connection */
    CHECK(moq_wtquic_network_lane_next_conn(lane, NULL) == NULL);
    CHECK(moq_wtquic_network_managed_stop(m) == MOQ_OK);
out:
    if (m != NULL)
        moq_wtquic_network_managed_destroy(m);
    if (failures == before)
        printf("PASS: lane_shape\n");
}

/* --- client-only, single-lane config validation ------------------------------ */

static void t_cfg_client_only(void)
{
    int before = failures;
    moq_wtquic_network_managed_cfg_t cfg;
    moq_wtquic_network_managed_t *m = NULL;

    /* server perspective: well-formed, unsupported by this facade */
    moq_wtquic_network_managed_cfg_init(&cfg);
    cfg.alloc = moq_alloc_default();
    cfg.host = "127.0.0.1";
    cfg.port = 9;
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    CHECK(moq_wtquic_network_managed_create(&cfg, &m) == MOQ_ERR_UNSUPPORTED);
    CHECK(m == NULL);

    /* malformed perspective value: invalid, not unsupported */
    moq_wtquic_network_managed_cfg_init(&cfg);
    cfg.alloc = moq_alloc_default();
    cfg.host = "127.0.0.1";
    cfg.port = 9;
    cfg.perspective = (moq_perspective_t)99;
    CHECK(moq_wtquic_network_managed_create(&cfg, &m) == MOQ_ERR_INVAL);
    CHECK(m == NULL);

    /* more than one lane: unsupported (single-lane facade) */
    moq_wtquic_network_managed_cfg_init(&cfg);
    cfg.alloc = moq_alloc_default();
    cfg.host = "127.0.0.1";
    cfg.port = 9;
    cfg.lane_count = 2;
    CHECK(moq_wtquic_network_managed_create(&cfg, &m) == MOQ_ERR_UNSUPPORTED);
    CHECK(m == NULL);

    /* the explicit spellings of the defaults are accepted */
    moq_wtquic_network_managed_cfg_init(&cfg);
    cfg.alloc = moq_alloc_default();
    cfg.host = "127.0.0.1";
    cfg.port = 9;
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.lane_count = 1;
    CHECK(moq_wtquic_network_managed_create(&cfg, &m) == MOQ_OK);
    if (m != NULL) {
        CHECK(moq_wtquic_network_managed_stop(m) == MOQ_OK);
        moq_wtquic_network_managed_destroy(m);
        m = NULL;
    }
    if (failures == before)
        printf("PASS: cfg_client_only\n");
}

/* --- wake behavior: deferred, never re-entrant, cross-thread ------------------ */

static void t_lane_wake(void)
{
    int before = failures;
    struct srv v;
    struct cli c;
    moq_wtquic_network_managed_t *m = NULL;
    moq_wtquic_network_managed_cfg_t cfg;
    static const char *const protos[] = { "moqt-16" };

    cli_init(&c, "hello-nw-managed");
    CHECK(srv_start(&v, protos, 1, true, MOQ_VERSION_DRAFT_16,
                    SRV_PUBLISH_ONE) == 0);
    cli_cfg(&cfg, &c, v.port, "moqt-16");
    CHECK(moq_wtquic_network_managed_create(&cfg, &m) == MOQ_OK);
    if (m == NULL)
        goto out;
    CHECK(cli_wait(&c, &c.got_expected));

    moq_wtquic_network_managed_lane_t *lane = moq_wtquic_network_managed_lane(m, 0);
    CHECK(lane != NULL);

    /* cross-thread lane wake: COALESCED delivery — every accepted wake
     * is followed by at least one pump pass; a burst may share one
     * (structural counter, no timing) */
    pthread_mutex_lock(&c.mu);
    int base = c.pumps;
    pthread_mutex_unlock(&c.mu);
    CHECK(moq_wtquic_network_lane_wake(lane) == MOQ_OK);
    CHECK(moq_wtquic_network_lane_wake(lane) == MOQ_OK);
    CHECK(moq_wtquic_network_lane_wake(lane) == MOQ_OK);
    CHECK(cli_wait_pumps(&c, base + 1));
    /* level-retention across bursts: a LATER wake delivers again */
    pthread_mutex_lock(&c.mu);
    base = c.pumps;
    pthread_mutex_unlock(&c.mu);
    CHECK(moq_wtquic_network_lane_wake(lane) == MOQ_OK);
    CHECK(cli_wait_pumps(&c, base + 1));

    /* same-domain wake: issued from INSIDE the pump — must be deferred
     * (no recursion) and still deliver a subsequent pass */
    pthread_mutex_lock(&c.mu);
    base = c.pumps;
    pthread_mutex_unlock(&c.mu);
    atomic_store(&c.wake_in_pump_rc, INT32_MIN);
    atomic_store(&c.wake_in_pump, 1);
    CHECK(moq_wtquic_network_lane_wake(lane) == MOQ_OK); /* pump #1: issues
                                                       the inner wake */
    CHECK(cli_wait_pumps(&c, base + 2)); /* #1 + the deferred inner */
    CHECK(atomic_load(&c.wake_in_pump_rc) == (int)MOQ_OK);
    CHECK(c.reentered == 0);
    CHECK(c.lane_violations == 0);

    /* terminal: wakes refuse */
    atomic_store(&c.please_close, 1);
    CHECK(moq_wtquic_network_managed_wake(m) == MOQ_OK);
    CHECK(wait_terminal(m));
    CHECK(moq_wtquic_network_lane_wake(lane) == MOQ_ERR_CLOSED);

    (void)moq_wtquic_network_managed_stop_begin(m);
    CHECK(moq_wtquic_network_managed_join(m) == MOQ_OK);
out:
    if (m != NULL)
        moq_wtquic_network_managed_destroy(m);
    srv_stop(&v);
    cli_destroy(&c);
    if (failures == before)
        printf("PASS: lane_wake\n");
}

/* --- wake-initiated pumps flush queued session actions ------------------------ */

/* Wait until the OBJECT count reaches at least `min` (bounded). */
static bool cli_wait_objects(struct cli *c, int min)
{
    struct timespec dl;
    bool ok = true, met;

    clock_gettime(CLOCK_REALTIME, &dl);
    dl.tv_sec += WAIT_SECS;
    pthread_mutex_lock(&c->mu);
    while (c->objects < min && ok)
        ok = pthread_cond_timedwait(&c->cv, &c->mu, &dl) == 0;
    met = c->objects >= min;
    pthread_mutex_unlock(&c->mu);
    return met;
}

/* A control message queued by a WAKE-initiated pump must reach the wire:
 * after the initial exchange the transport is quiet, so nothing else can
 * flush it -- the wake path itself must service the adapter. The server
 * publishes one object per accepted SUBSCRIBE, so the second SUBSCRIBE
 * (issued from the wake pump) proves itself by a second delivery. */
static void t_wake_flush(void)
{
    int before = failures;
    struct srv v;
    struct cli c;
    moq_wtquic_network_managed_t *m = NULL;
    moq_wtquic_network_managed_cfg_t cfg;
    static const char *const protos[] = { "moqt-16" };

    cli_init(&c, "hello-nw-managed");
    CHECK(srv_start(&v, protos, 1, true, MOQ_VERSION_DRAFT_16,
                    SRV_PUBLISH_ONE) == 0);
    cli_cfg(&cfg, &c, v.port, "moqt-16");
    CHECK(moq_wtquic_network_managed_create(&cfg, &m) == MOQ_OK);
    if (m == NULL)
        goto out;
    CHECK(cli_wait(&c, &c.got_expected));

    pthread_mutex_lock(&c.mu);
    int base = c.objects;
    pthread_mutex_unlock(&c.mu);
    /* the second SUBSCRIBE is queued inside the wake pump and must be
     * flushed by the wake path itself (the transport is idle) */
    atomic_store(&c.subscribe_in_pump, 1);
    CHECK(moq_wtquic_network_managed_wake(m) == MOQ_OK);
    CHECK(cli_wait_objects(&c, base + 1));
    CHECK(c.lane_violations == 0);

    (void)moq_wtquic_network_managed_stop_begin(m);
    CHECK(moq_wtquic_network_managed_join(m) == MOQ_OK);
out:
    if (m != NULL)
        moq_wtquic_network_managed_destroy(m);
    srv_stop(&v);
    cli_destroy(&c);
    if (failures == before)
        printf("PASS: wake_flush\n");
}

/* --- the truthful wake contract --------------------------------------------- */

static void wake_contract_on_stopped(void *ctx)
{
    struct cli *c = ctx;

    atomic_store(&c->stopped_seen, 1);
}

/* Wake-ONLY worker: rings wakes until it observes the first
 * MOQ_ERR_CLOSED, counting each outcome. Signals after its first
 * accepted wake so the test can order the stop AFTER at least one
 * acceptance — the transition then deterministically observes both an
 * accepted and a refused wake. */
struct wake_worker {
    moq_wtquic_network_managed_t *m;
    pthread_mutex_t mu;
    pthread_cond_t cv;
    int first_ok;      /* signaled on the first MOQ_OK */
    int wake_ok;       /* worker-thread-only counters */
    int wake_closed;
    int bad_rc;
};

static void *wake_worker_main(void *arg)
{
    struct wake_worker *w = arg;

    for (long i = 0; i < 50L * 1000 * 1000 && w->wake_closed == 0; i++) {
        moq_result_t rc = moq_wtquic_network_managed_wake(w->m);

        if (rc == MOQ_OK) {
            w->wake_ok++;
            if (!w->first_ok) {
                pthread_mutex_lock(&w->mu);
                w->first_ok = 1;
                pthread_cond_broadcast(&w->cv);
                pthread_mutex_unlock(&w->mu);
            }
        } else if (rc == MOQ_ERR_CLOSED) {
            w->wake_closed++;
        } else {
            w->bad_rc = (int)rc;
            break;
        }
    }
    return NULL;
}

static bool wake_worker_wait_first_ok(struct wake_worker *w)
{
    struct timespec dl;
    bool ok = true;

    clock_gettime(CLOCK_REALTIME, &dl);
    dl.tv_sec += WAIT_SECS;
    pthread_mutex_lock(&w->mu);
    while (w->first_ok == 0 && ok)
        ok = pthread_cond_timedwait(&w->cv, &w->mu, &dl) == 0;
    ok = w->first_ok != 0;
    pthread_mutex_unlock(&w->mu);
    return ok;
}

/* The three observation cases of the wake contract, deterministically:
 * no-race MOQ_OK delivers a pump; a wake AFTER stop latched is
 * MOQ_ERR_CLOSED and delivers nothing; wakes RACING stop_begin return
 * MOQ_OK or MOQ_ERR_CLOSED (an MOQ_OK may be absorbed by teardown) and
 * nothing ever pumps during or after on_stopped. */
static void t_wake_contract(void)
{
    int before = failures;
    struct srv v;
    struct cli c;
    moq_wtquic_network_managed_t *m = NULL;
    moq_wtquic_network_managed_cfg_t cfg;
    static const char *const protos[] = { "moqt-16" };

    cli_init(&c, "hello-nw-managed");
    CHECK(srv_start(&v, protos, 1, true, MOQ_VERSION_DRAFT_16,
                    SRV_PUBLISH_ONE) == 0);
    cli_cfg(&cfg, &c, v.port, "moqt-16");
    cfg.on_stopped = wake_contract_on_stopped;
    cfg.on_stopped_ctx = &c;
    CHECK(moq_wtquic_network_managed_create(&cfg, &m) == MOQ_OK);
    if (m == NULL)
        goto out;
    CHECK(cli_wait(&c, &c.got_expected));

    /* 1: no concurrent stop — MOQ_OK guarantees a deferred pump */
    pthread_mutex_lock(&c.mu);
    int base = c.pumps;
    pthread_mutex_unlock(&c.mu);
    CHECK(moq_wtquic_network_managed_wake(m) == MOQ_OK);
    CHECK(cli_wait_pumps(&c, base + 1));

    /* 2: wakes RACING stop_begin, via a dedicated wake-only worker.
     * The ordering gate makes the transition deterministic: the worker
     * signals after its FIRST accepted wake; the test banks that
     * wake's guaranteed no-race delivery (pump delta >= +1) BEFORE
     * initiating the stop, then the worker keeps ringing until it
     * observes the first refusal. So the run provably crosses the
     * boundary with at least one accepted AND one refused wake; any
     * MOQ_OK issued during the race may deliver or be absorbed
     * (coalescing permitted — no delivery claim is made for those),
     * and nothing pumps during/after on_stopped. */
    struct wake_worker w = { m, PTHREAD_MUTEX_INITIALIZER,
                             PTHREAD_COND_INITIALIZER, 0, 0, 0, 0 };
    pthread_t th;

    pthread_mutex_lock(&c.mu);
    int race_base = c.pumps;
    pthread_mutex_unlock(&c.mu);
    CHECK(pthread_create(&th, NULL, wake_worker_main, &w) == 0);
    CHECK(wake_worker_wait_first_ok(&w));
    /* the pre-stop accepted wake has NO concurrent stop: its deferred
     * pass is guaranteed — bank the pump delta before stopping */
    CHECK(cli_wait_pumps(&c, race_base + 1));
    (void)moq_wtquic_network_managed_stop_begin(m);
    pthread_join(th, NULL); /* exits on its first MOQ_ERR_CLOSED */
    CHECK(w.bad_rc == 0);       /* only MOQ_OK / MOQ_ERR_CLOSED */
    CHECK(w.wake_ok >= 1);      /* accepted side of the transition */
    CHECK(w.wake_closed >= 1);  /* refused side of the transition */
    CHECK(moq_wtquic_network_managed_join(m) == MOQ_OK);
    CHECK(atomic_load(&c.stopped_seen) == 1);
    pthread_mutex_destroy(&w.mu);
    pthread_cond_destroy(&w.cv);

    /* 3: stop latched and torn down — CLOSED, and provably no pump
     * (post-teardown nothing can run asynchronously) */
    pthread_mutex_lock(&c.mu);
    int final_pumps = c.pumps;
    pthread_mutex_unlock(&c.mu);
    CHECK(moq_wtquic_network_managed_wake(m) == MOQ_ERR_CLOSED);
    CHECK(moq_wtquic_network_lane_wake(
              moq_wtquic_network_managed_lane(m, 0)) == MOQ_ERR_CLOSED);
    pthread_mutex_lock(&c.mu);
    CHECK(c.pumps == final_pumps);
    pthread_mutex_unlock(&c.mu);
    CHECK(atomic_load(&c.pump_after_stopped) == 0);
out:
    if (m != NULL)
        moq_wtquic_network_managed_destroy(m);
    srv_stop(&v);
    cli_destroy(&c);
    if (failures == before)
        printf("PASS: wake_contract\n");
}

/* --- main --------------------------------------------------------------------- */

int main(int argc, char **argv)
{
    if (argc < 3) {
        fprintf(stderr, "usage: %s <cert.pem> <key.pem>\n", argv[0]);
        return 2;
    }
    cert_path = argv[1];
    key_path = argv[2];

    static const char *const d16[] = { "moqt-16" };
    static const char *const d18[] = { "moqt-18" };
    static const char *const both[] = { "moqt-18", "moqt-16" };

    t_exchange("exchange_d16", d16, 1, MOQ_VERSION_DRAFT_16,
               "moqt-16", MOQ_VERSION_DRAFT_16);
    t_exchange("exchange_d18", d18, 1, MOQ_VERSION_DRAFT_18,
               "moqt-18", MOQ_VERSION_DRAFT_18);
    t_exchange("multi_offer_selects_d18", both, 2, MOQ_VERSION_DRAFT_18,
               "moqt-18, moqt-16", MOQ_VERSION_DRAFT_18);
    t_legacy_fallback();
    t_idle_deadline();
    t_exact_d18_no_downgrade();
    t_abort_stream();
    t_post_vs_stop();
    t_stop_from_domain();
    t_refused_before_establishment();
    t_transport_failure_classified();
    t_cfg_prefixes();
    t_cfg_client_only();
    t_lane_shape();
    t_lane_wake();
    t_wake_flush();
    t_wake_contract();
    t_protocol_validation();
    t_sized_free();
    t_create_fail_no_timer();
    t_post_contract();
    t_actor_lifecycle();
    t_wait_not_premature();
    t_wait_level_retained();
    t_soak();

    if (failures != 0) {
        fprintf(stderr, "%d failure(s)\n", failures);
        return 1;
    }
    printf("all managed NW client tests passed\n");
    return 0;
}
