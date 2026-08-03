/*
 * MoQ over wtquic loopback: a real publish/subscribe exchange across a
 * real MsQuic connection on localhost — session setup over extended
 * CONNECT with the MoQ subprotocol, SUBSCRIBE and its acceptance over
 * the MoQ control stream, one object delivered over a WebTransport uni
 * stream, then a clean WebTransport close. Plus a refusal case: a
 * client requiring a MoQ version the server does not serve is refused
 * at the WebTransport layer and the MoQ session never sets up.
 *
 * Threading: wtquic delivers everything on its transport worker; all
 * MoQ session interaction happens inside the adapter hook on that
 * thread. The main thread only waits on flags the hook sets.
 */

#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#include <moq/moq.h>
#include <moq/wtquic.h>

#include <wtquic/wtquic_msquic.h>

static int failures = 0;

#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

#define WAIT_SECS 30

struct app_side {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    moq_wtquic_conn_t *conn;

    int setup_done;
    int subscribed;      /* client sent SUBSCRIBE */
    int accepted;        /* server accepted + published */
    int sub_ok;
    int got_object;
    int closed;          /* transport closed (bridge view) */

    uint8_t payload[64];
    size_t payload_len;
};

static void side_init(struct app_side *a)
{
    memset(a, 0, sizeof(*a));
    pthread_mutex_init(&a->mu, NULL);
    pthread_cond_init(&a->cv, NULL);
}

static void side_destroy(struct app_side *a)
{
    pthread_mutex_destroy(&a->mu);
    pthread_cond_destroy(&a->cv);
}

static bool side_wait(struct app_side *a, const int *flag)
{
    struct timespec deadline;
    bool ok = true;
    bool set;

    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += WAIT_SECS;
    pthread_mutex_lock(&a->mu);
    while (*flag == 0 && ok)
        ok = pthread_cond_timedwait(&a->cv, &a->mu, &deadline) == 0;
    set = *flag != 0;
    pthread_mutex_unlock(&a->mu);
    return set;
}

/* Server: accept the subscription and immediately publish one object
 * into a fresh subgroup, closing it with FIN. */
static void server_handle_subscribe(moq_wtquic_conn_t *conn,
                                    struct app_side *a,
                                    moq_subscription_t sub)
{
    moq_session_t *s = moq_wtquic_conn_session(conn);
    moq_accept_subscribe_cfg_t acfg;
    moq_subgroup_cfg_t sgcfg;
    moq_subgroup_handle_t sg;
    moq_rcbuf_t *buf = NULL;
    static const char payload[] = "hello-over-msquic";

    moq_accept_subscribe_cfg_init(&acfg);
    if (moq_session_accept_subscribe(s, sub, &acfg, 0) < 0)
        return;

    moq_subgroup_cfg_init(&sgcfg);
    sgcfg.group_id = 0;
    sgcfg.publisher_priority = 200;
    if (moq_session_open_subgroup(s, sub, &sgcfg, 0, &sg) < 0)
        return;
    if (moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)payload,
                         sizeof(payload) - 1, &buf) < 0)
        return;
    (void)moq_session_write_object(s, sg, 0, buf, 0);
    moq_rcbuf_decref(buf);
    (void)moq_session_close_subgroup(s, sg, 0);

    pthread_mutex_lock(&a->mu);
    a->accepted = 1;
    pthread_cond_broadcast(&a->cv);
    pthread_mutex_unlock(&a->mu);
}

/* Client: subscribe on setup; close the WebTransport session once the
 * object arrived. */
static void client_subscribe(moq_wtquic_conn_t *conn)
{
    moq_session_t *s = moq_wtquic_conn_session(conn);
    moq_subscribe_cfg_t sc;
    moq_subscription_t sub;
    static const moq_bytes_t ns[] = {
        { (const uint8_t *)"wtq", 3 },
        { (const uint8_t *)"smoke", 5 },
    };

    moq_subscribe_cfg_init(&sc);
    sc.track_namespace.parts = (moq_bytes_t *)ns;
    sc.track_namespace.count = 2;
    sc.track_name = (moq_bytes_t){ (const uint8_t *)"video", 5 };
    sc.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
    (void)moq_session_subscribe(s, &sc, 0, &sub);
}

static void app_hook(moq_wtquic_conn_t *conn, void *user)
{
    struct app_side *a = user;
    moq_session_t *s = moq_wtquic_conn_session(conn);
    bool client = moq_session_perspective(s) == MOQ_PERSPECTIVE_CLIENT;
    moq_event_t ev;

    while (moq_session_poll_events(s, &ev, 1) > 0) {
        switch (ev.kind) {
        case MOQ_EVENT_SETUP_COMPLETE:
            pthread_mutex_lock(&a->mu);
            a->setup_done = 1;
            pthread_cond_broadcast(&a->cv);
            pthread_mutex_unlock(&a->mu);
            if (client && !a->subscribed) {
                a->subscribed = 1;
                client_subscribe(conn);
            }
            break;
        case MOQ_EVENT_SUBSCRIBE_REQUEST:
            if (!client)
                server_handle_subscribe(conn, a,
                                        ev.u.subscribe_request.sub);
            break;
        case MOQ_EVENT_SUBSCRIBE_OK:
            pthread_mutex_lock(&a->mu);
            a->sub_ok = 1;
            pthread_cond_broadcast(&a->cv);
            pthread_mutex_unlock(&a->mu);
            break;
        case MOQ_EVENT_OBJECT_RECEIVED:
            pthread_mutex_lock(&a->mu);
            if (!a->got_object && ev.u.object_received.payload != NULL) {
                size_t l = moq_rcbuf_len(ev.u.object_received.payload);

                if (l <= sizeof(a->payload)) {
                    memcpy(a->payload,
                           moq_rcbuf_data(ev.u.object_received.payload),
                           l);
                    a->payload_len = l;
                }
                a->got_object = 1;
            }
            pthread_cond_broadcast(&a->cv);
            pthread_mutex_unlock(&a->mu);
            /* done: close the WebTransport session cleanly */
            if (client && a->got_object)
                (void)wtq_session_close(
                    moq_wtquic_conn_wtq_session(conn), 0, NULL, 0);
            break;
        default:
            break;
        }
        moq_event_cleanup(&ev);
    }

    if (moq_wtquic_conn_is_closed(conn) || moq_wtquic_conn_is_fatal(conn)) {
        pthread_mutex_lock(&a->mu);
        a->closed = 1;
        pthread_cond_broadcast(&a->cv);
        pthread_mutex_unlock(&a->mu);
    }
}

/* --- fixtures -------------------------------------------------------------- */

static const char *cert_path;
static const char *key_path;

static int make_session(moq_perspective_t persp, moq_session_t **out)
{
    moq_session_cfg_t cfg;

    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(),
                               persp);
    cfg.send_request_capacity = 1;
    cfg.initial_request_capacity = 10;
    return moq_session_create(&cfg, 0, out);
}

static int make_conn(moq_session_t *s, struct app_side *a,
                     moq_wtquic_conn_t **out)
{
    moq_wtquic_conn_cfg_t cfg;

    moq_wtquic_conn_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.session = s;
    cfg.hook = app_hook;
    cfg.hook_user = a;
    if (moq_wtquic_conn_create(&cfg, out) < 0)
        return -1;
    a->conn = *out;
    return 0;
}

/* --- the smoke -------------------------------------------------------------- */

static void test_pubsub_over_msquic(void)
{
    struct app_side sv, cl;
    moq_session_t *server_ms = NULL, *client_ms = NULL;
    moq_wtquic_conn_t *server_conn = NULL, *client_conn = NULL;
    wtq_msquic_env_cfg_t ecfg = WTQ_MSQUIC_ENV_CFG_INIT;
    wtq_msquic_env_t *env = NULL;
    wtq_msquic_listener_t *listener = NULL;
    wtq_session_t *cs = NULL;
    static const char *const protos[] = { "moqt-16" };

    side_init(&sv);
    side_init(&cl);
    CHECK(make_session(MOQ_PERSPECTIVE_SERVER, &server_ms) >= 0);
    CHECK(make_session(MOQ_PERSPECTIVE_CLIENT, &client_ms) >= 0);
    CHECK(make_conn(server_ms, &sv, &server_conn) == 0);
    CHECK(make_conn(client_ms, &cl, &client_conn) == 0);
    CHECK(wtq_msquic_env_open(&ecfg, &env) == WTQ_OK);
    if (server_conn == NULL || client_conn == NULL || env == NULL)
        goto out;

    wtq_serve_config_t serve = WTQ_SERVE_CONFIG_INIT;
    serve.path = "/moq";
    serve.subprotocols = protos;
    serve.subprotocol_count = 1;
    serve.require_subprotocol = true;

    wtq_msquic_listener_cfg_t lcfg = WTQ_MSQUIC_LISTENER_CFG_INIT;
    lcfg.bind_address = "127.0.0.1";
    lcfg.port = 0;
    lcfg.cert_file = cert_path;
    lcfg.key_file = key_path;
    lcfg.paths = &serve;
    lcfg.path_count = 1;
    lcfg.events = moq_wtquic_conn_events();
    lcfg.user = server_conn;
    CHECK(wtq_msquic_listener_start(env, &lcfg, &listener) == WTQ_OK);
    if (listener == NULL)
        goto out;

    wtq_connect_config_t ccfg = WTQ_CONNECT_CONFIG_INIT;
    ccfg.authority = "localhost";
    ccfg.path = "/moq";
    ccfg.subprotocols = protos;
    ccfg.subprotocol_count = 1;
    ccfg.require_subprotocol = true;

    wtq_msquic_client_cfg_t cli = WTQ_MSQUIC_CLIENT_CFG_INIT;
    cli.server_name = "127.0.0.1";
    cli.port = wtq_msquic_listener_port(listener);
    cli.insecure_skip_verify = true;
    cli.connect = &ccfg;
    cli.events = moq_wtquic_conn_events();
    cli.user = client_conn;
    CHECK(wtq_msquic_client_connect(env, &cli, &cs) == WTQ_OK);
    if (cs == NULL)
        goto out;

    /* the whole exchange runs on the transport thread; just wait */
    CHECK(side_wait(&cl, &cl.setup_done));
    CHECK(side_wait(&sv, &sv.setup_done));
    CHECK(side_wait(&cl, &cl.sub_ok));
    CHECK(side_wait(&sv, &sv.accepted));
    CHECK(side_wait(&cl, &cl.got_object));
    CHECK(side_wait(&cl, &cl.closed));
    CHECK(side_wait(&sv, &sv.closed));

out:
    if (listener != NULL)
        wtq_msquic_listener_stop(listener);
    if (env != NULL)
        wtq_msquic_env_close(env);
    if (cs != NULL)
        wtq_session_release(cs); /* legal after env close */

    CHECK(cl.got_object);
    if (cl.got_object) {
        CHECK(cl.payload_len == strlen("hello-over-msquic"));
        CHECK(memcmp(cl.payload, "hello-over-msquic",
                     cl.payload_len) == 0);
    }
    CHECK(!moq_wtquic_conn_is_fatal(client_conn));
    CHECK(!moq_wtquic_conn_is_fatal(server_conn));
    CHECK(moq_wtquic_conn_is_closed(client_conn));
    CHECK(moq_wtquic_conn_is_closed(server_conn));

    moq_wtquic_conn_destroy(client_conn);
    moq_wtquic_conn_destroy(server_conn);
    moq_session_destroy(client_ms);
    moq_session_destroy(server_ms);
    side_destroy(&sv);
    side_destroy(&cl);
    if (failures == 0)
        printf("PASS: pubsub_over_msquic\n");
}

/* A client requiring a MoQ version the server does not serve is
 * refused at the WebTransport layer; the MoQ session never sets up and
 * the adapter reports the failure. */
static void test_version_mismatch_refused(void)
{
    int before = failures;
    struct app_side sv, cl;
    moq_session_t *server_ms = NULL, *client_ms = NULL;
    moq_wtquic_conn_t *server_conn = NULL, *client_conn = NULL;
    wtq_msquic_env_cfg_t ecfg = WTQ_MSQUIC_ENV_CFG_INIT;
    wtq_msquic_env_t *env = NULL;
    wtq_msquic_listener_t *listener = NULL;
    wtq_session_t *cs = NULL;
    static const char *const serve_protos[] = { "moqt-16" };
    static const char *const offer_protos[] = { "moqt-18" };

    side_init(&sv);
    side_init(&cl);
    CHECK(make_session(MOQ_PERSPECTIVE_SERVER, &server_ms) >= 0);
    CHECK(make_session(MOQ_PERSPECTIVE_CLIENT, &client_ms) >= 0);
    CHECK(make_conn(server_ms, &sv, &server_conn) == 0);
    CHECK(make_conn(client_ms, &cl, &client_conn) == 0);
    CHECK(wtq_msquic_env_open(&ecfg, &env) == WTQ_OK);
    if (server_conn == NULL || client_conn == NULL || env == NULL)
        goto out;

    wtq_serve_config_t serve = WTQ_SERVE_CONFIG_INIT;
    serve.path = "/moq";
    serve.subprotocols = serve_protos;
    serve.subprotocol_count = 1;
    serve.require_subprotocol = true;

    wtq_msquic_listener_cfg_t lcfg = WTQ_MSQUIC_LISTENER_CFG_INIT;
    lcfg.bind_address = "127.0.0.1";
    lcfg.port = 0;
    lcfg.cert_file = cert_path;
    lcfg.key_file = key_path;
    lcfg.paths = &serve;
    lcfg.path_count = 1;
    lcfg.events = moq_wtquic_conn_events();
    lcfg.user = server_conn;
    CHECK(wtq_msquic_listener_start(env, &lcfg, &listener) == WTQ_OK);
    if (listener == NULL)
        goto out;

    wtq_connect_config_t ccfg = WTQ_CONNECT_CONFIG_INIT;
    ccfg.authority = "localhost";
    ccfg.path = "/moq";
    ccfg.subprotocols = offer_protos;
    ccfg.subprotocol_count = 1;
    ccfg.require_subprotocol = true;

    wtq_msquic_client_cfg_t cli = WTQ_MSQUIC_CLIENT_CFG_INIT;
    cli.server_name = "127.0.0.1";
    cli.port = wtq_msquic_listener_port(listener);
    cli.insecure_skip_verify = true;
    cli.connect = &ccfg;
    cli.events = moq_wtquic_conn_events();
    cli.user = client_conn;
    CHECK(wtq_msquic_client_connect(env, &cli, &cs) == WTQ_OK);
    if (cs == NULL)
        goto out;

    CHECK(side_wait(&cl, &cl.closed));

out:
    if (listener != NULL)
        wtq_msquic_listener_stop(listener);
    if (env != NULL)
        wtq_msquic_env_close(env);
    if (cs != NULL)
        wtq_session_release(cs);

    CHECK(moq_wtquic_conn_is_fatal(client_conn));
    CHECK(cl.setup_done == 0);
    CHECK(cl.got_object == 0);

    moq_wtquic_conn_destroy(client_conn);
    moq_wtquic_conn_destroy(server_conn);
    moq_session_destroy(client_ms);
    moq_session_destroy(server_ms);
    side_destroy(&sv);
    side_destroy(&cl);
    if (failures == before)
        printf("PASS: version_mismatch_refused\n");
}

/* A config missing any allocator function is rejected before the first
 * allocation. */
static void test_conn_cfg_validation(void)
{
    int before = failures;
    moq_session_t *ms = NULL;
    moq_wtquic_conn_t *conn = (moq_wtquic_conn_t *)&failures;
    moq_wtquic_conn_cfg_t cfg;
    moq_alloc_t partial;

    CHECK(make_session(MOQ_PERSPECTIVE_CLIENT, &ms) >= 0);
    if (ms == NULL)
        return;

    moq_wtquic_conn_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.session = ms;

    cfg.alloc = NULL;
    CHECK(moq_wtquic_conn_create(&cfg, &conn) == MOQ_ERR_INVAL);
    CHECK(conn == NULL);

    partial = *moq_alloc_default();
    partial.alloc = NULL;
    cfg.alloc = &partial;
    conn = (moq_wtquic_conn_t *)&failures;
    CHECK(moq_wtquic_conn_create(&cfg, &conn) == MOQ_ERR_INVAL);
    CHECK(conn == NULL);

    partial = *moq_alloc_default();
    partial.realloc = NULL;
    conn = (moq_wtquic_conn_t *)&failures;
    CHECK(moq_wtquic_conn_create(&cfg, &conn) == MOQ_ERR_INVAL);
    CHECK(conn == NULL);

    partial = *moq_alloc_default();
    partial.free = NULL;
    conn = (moq_wtquic_conn_t *)&failures;
    CHECK(moq_wtquic_conn_create(&cfg, &conn) == MOQ_ERR_INVAL);
    CHECK(conn == NULL);

    moq_session_destroy(ms);
    if (failures == before)
        printf("PASS: conn_cfg_validation\n");
}

/* --- raw (non-MoQ) wtquic peer, for hostile-byte injection ------------------ */

struct raw_side {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    int established;
    int closed;
};

static void raw_established(wtq_session_t *s, wtq_str_t sub, void *user)
{
    struct raw_side *r = user;
    wtq_stream_t *st = NULL;

    (void)sub;
    pthread_mutex_lock(&r->mu);
    r->established = 1;
    pthread_cond_broadcast(&r->cv);
    pthread_mutex_unlock(&r->mu);

    /* the server treats its first peer bidi as the MoQ control stream:
     * feed it garbage and FINISH it — a MoQ control stream must never
     * end, so the violation is unambiguous whatever the bytes parse as */
    if (wtq_session_open_bidi(s, &st) == WTQ_OK) {
        static const uint8_t garbage[16] = {
            0xde, 0xad, 0xbe, 0xef, 0xde, 0xad, 0xbe, 0xef,
            0xde, 0xad, 0xbe, 0xef, 0xde, 0xad, 0xbe, 0xef,
        };
        wtq_span_t span = { garbage, sizeof(garbage) };

        (void)wtq_stream_send(st, &span, 1, WTQ_SEND_FIN, NULL);
    }
}

static void raw_closed(wtq_session_t *s, uint32_t code,
                       const uint8_t *reason, size_t rlen, bool clean,
                       void *user)
{
    struct raw_side *r = user;

    (void)s;
    (void)code;
    (void)reason;
    (void)rlen;
    (void)clean;
    pthread_mutex_lock(&r->mu);
    r->closed = 1;
    pthread_cond_broadcast(&r->cv);
    pthread_mutex_unlock(&r->mu);
}

static bool raw_wait(struct raw_side *r, const int *flag)
{
    struct timespec deadline;
    bool ok = true;
    bool set;

    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += WAIT_SECS;
    pthread_mutex_lock(&r->mu);
    while (*flag == 0 && ok)
        ok = pthread_cond_timedwait(&r->cv, &r->mu, &deadline) == 0;
    set = *flag != 0;
    pthread_mutex_unlock(&r->mu);
    return set;
}

/* A peer that feeds garbage into the MoQ control stream makes the
 * bridge fatal — and a fatal bridge must not leave the WebTransport
 * session running: the adapter tears it down, observed here by the
 * hostile peer itself. */
static void test_garbage_control_torn_down(void)
{
    int before = failures;
    struct app_side sv;
    struct raw_side rc;
    moq_session_t *server_ms = NULL;
    moq_wtquic_conn_t *server_conn = NULL;
    wtq_msquic_env_cfg_t ecfg = WTQ_MSQUIC_ENV_CFG_INIT;
    wtq_msquic_env_t *env = NULL;
    wtq_msquic_listener_t *listener = NULL;
    wtq_session_t *cs = NULL;
    static const char *const protos[] = { "moqt-16" };

    side_init(&sv);
    memset(&rc, 0, sizeof(rc));
    pthread_mutex_init(&rc.mu, NULL);
    pthread_cond_init(&rc.cv, NULL);

    CHECK(make_session(MOQ_PERSPECTIVE_SERVER, &server_ms) >= 0);
    CHECK(make_conn(server_ms, &sv, &server_conn) == 0);
    CHECK(wtq_msquic_env_open(&ecfg, &env) == WTQ_OK);
    if (server_conn == NULL || env == NULL)
        goto out;

    wtq_serve_config_t serve = WTQ_SERVE_CONFIG_INIT;
    serve.path = "/moq";
    serve.subprotocols = protos;
    serve.subprotocol_count = 1;
    serve.require_subprotocol = true;

    wtq_msquic_listener_cfg_t lcfg = WTQ_MSQUIC_LISTENER_CFG_INIT;
    lcfg.bind_address = "127.0.0.1";
    lcfg.port = 0;
    lcfg.cert_file = cert_path;
    lcfg.key_file = key_path;
    lcfg.paths = &serve;
    lcfg.path_count = 1;
    lcfg.events = moq_wtquic_conn_events();
    lcfg.user = server_conn;
    CHECK(wtq_msquic_listener_start(env, &lcfg, &listener) == WTQ_OK);
    if (listener == NULL)
        goto out;

    wtq_session_events_t rev;
    wtq_session_events_init(&rev);
    rev.on_established = raw_established;
    rev.on_closed = raw_closed;

    wtq_connect_config_t ccfg = WTQ_CONNECT_CONFIG_INIT;
    ccfg.authority = "localhost";
    ccfg.path = "/moq";
    ccfg.subprotocols = protos;
    ccfg.subprotocol_count = 1;

    wtq_msquic_client_cfg_t cli = WTQ_MSQUIC_CLIENT_CFG_INIT;
    cli.server_name = "127.0.0.1";
    cli.port = wtq_msquic_listener_port(listener);
    cli.insecure_skip_verify = true;
    cli.connect = &ccfg;
    cli.events = &rev;
    cli.user = &rc;
    CHECK(wtq_msquic_client_connect(env, &cli, &cs) == WTQ_OK);
    if (cs == NULL)
        goto out;

    CHECK(raw_wait(&rc, &rc.established));
    /* the violation must reach the hostile peer as a close — either
     * the session's own protocol-error close through the bridge, or
     * the adapter's fatal-bridge backstop */
    CHECK(raw_wait(&rc, &rc.closed));

out:
    if (listener != NULL)
        wtq_msquic_listener_stop(listener);
    if (env != NULL)
        wtq_msquic_env_close(env);
    if (cs != NULL)
        wtq_session_release(cs);

    CHECK(moq_wtquic_conn_is_closed(server_conn) ||
          moq_wtquic_conn_is_fatal(server_conn));

    moq_wtquic_conn_destroy(server_conn);
    moq_session_destroy(server_ms);
    side_destroy(&sv);
    pthread_mutex_destroy(&rc.mu);
    pthread_cond_destroy(&rc.cv);
    if (failures == before)
        printf("PASS: garbage_control_torn_down\n");
}

int main(int argc, char **argv)
{
    if (argc < 3) {
        fprintf(stderr, "usage: %s <cert.pem> <key.pem>\n", argv[0]);
        return 2;
    }
    cert_path = argv[1];
    key_path = argv[2];

    test_conn_cfg_validation();
    test_pubsub_over_msquic();
    test_version_mismatch_refused();
    test_garbage_control_torn_down();

    if (failures == 0)
        printf("PASS: wtquic_loopback\n");
    else
        fprintf(stderr, "FAIL: wtquic_loopback (%d)\n", failures);
    return failures;
}
