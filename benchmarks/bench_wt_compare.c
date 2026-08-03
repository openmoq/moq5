/*
 * MoQ-over-WebTransport transport comparison: the identical workload —
 * one subscription, N deterministic objects published sequentially on
 * ONE subgroup stream — measured over both WebTransport paths:
 *
 *   wtquic          libmoq -> adapters/wtquic -> wtquic -> real MsQuic
 *                   loopback (real UDP, MsQuic worker threads)
 *   pico_wt_managed libmoq -> adapters/pico_wt managed facade ->
 *                   picoquic/h3zero real loopback (real UDP, picoquic
 *                   network threads)
 *
 * Both paths are real localhost transport with real TLS — the managed
 * pico facade (not the deterministic simulation) is the baseline
 * because it is the production shape of what a wtquic migration would
 * replace; simulated-clock numbers would measure simulator CPU, not
 * transport behavior. Localhost loopback still is not a network: the
 * numbers compare stack overhead, not congestion behavior. The single
 * hot stream is deliberate — the pico_wt path cannot pause reads and
 * tears its connection down under a handful of CONCURRENT subgroup
 * streams, so per-object stream churn is not comparable yet and is not
 * measured here.
 *
 * The subscriber counts exact objects and bytes and verifies payload
 * integrity (index prefix and length on every object, full content on
 * a deterministic sample). Warmup objects are excluded: the window
 * opens at the last warmup object's arrival (at SUBSCRIBE_OK when
 * warmup is 0) and closes at the final object's arrival.
 *
 * With --backend both, one JSON object per backend is printed per
 * line, followed by a ratio line (wtquic over pico_wt_managed).
 */

#if defined(__linux__) && !defined(_POSIX_C_SOURCE)
#define _POSIX_C_SOURCE 200809L
#endif

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <moq/moq.h>
#include <moq/pico_wt_managed.h>
#include <moq/wtquic.h>

#include <wtquic/wtquic_msquic.h>

#define BENCH_PATH "/moq"
#define BENCH_PROTO "moqt-16"
#define BENCH_BURST_DEFAULT 64  /* publishes attempted per pump/hook */
#define BENCH_TIMEOUT_SECS 60

static const char *const bench_protos[] = { BENCH_PROTO };

struct bench_opts {
    const char *backend;    /* "wtquic" | "pico_wt" | "both" */
    const char *cert;
    const char *key;
    uint64_t objects;       /* total published, warmup included */
    uint64_t object_size;   /* bytes per object (>= 8) */
    uint64_t warmup;        /* leading objects excluded from timing */
    uint64_t burst;         /* publishes attempted per pump/hook */
    int port;               /* pico_wt managed listen port */
    bool json;
};

struct bench_result {
    const char *backend;
    uint64_t measured;
    uint64_t bytes;
    uint64_t elapsed_us;
    uint64_t errors;
};

static uint64_t bench_now_us(void)
{
    struct timespec ts;

    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000u + (uint64_t)ts.tv_nsec / 1000u;
}

/* --- payload -------------------------------------------------------------- */

static void payload_fill(uint8_t *dst, uint64_t size, uint64_t idx)
{
    for (size_t i = 0; i < 8; i++)
        dst[i] = (uint8_t)(idx >> (8 * i));
    for (uint64_t j = 8; j < size; j++)
        dst[j] = (uint8_t)(idx * 131u + j * 7u + 47u);
}

/* --- publisher (identical on both backends) -------------------------------- */

struct bench_pub {
    const struct bench_opts *o;
    moq_subscription_t sub;
    moq_subgroup_handle_t sg;
    bool have_sub;
    bool sg_open;
    bool sg_closed;
    uint64_t published;
    uint64_t errors;
};

static void pub_handle_event(moq_session_t *s, struct bench_pub *p,
                             const moq_event_t *ev, uint64_t now)
{
    if (ev->kind == MOQ_EVENT_SUBSCRIBE_REQUEST && !p->have_sub) {
        moq_accept_subscribe_cfg_t acfg;

        moq_accept_subscribe_cfg_init(&acfg);
        if (moq_session_accept_subscribe(s, ev->u.subscribe_request.sub,
                                         &acfg, now) < 0) {
            p->errors++;
            return;
        }
        p->sub = ev->u.subscribe_request.sub;
        p->have_sub = true;
    }
}

/* Publish up to a burst of objects per pump, all on ONE subgroup (one
 * long-lived uni stream): a write that reports pressure is retried on
 * the next pump. Sequential objects on a hot stream is the workload
 * both transports handle; per-object stream churn is deliberately not
 * measured here (the pico_wt path tears down under a handful of
 * concurrent subgroup streams — see the caveats in the header). */
static void pub_drive(moq_session_t *s, struct bench_pub *p, uint64_t now)
{
    if (!p->have_sub)
        return;
    if (!p->sg_open) {
        moq_subgroup_cfg_t sgcfg;

        moq_subgroup_cfg_init(&sgcfg);
        sgcfg.group_id = 0;
        sgcfg.publisher_priority = 200;
        if (moq_session_open_subgroup(s, p->sub, &sgcfg, now,
                                      &p->sg) < 0)
            return; /* pressure: retry on a later pump */
        p->sg_open = true;
    }
    for (uint64_t burst = 0;
         burst < p->o->burst && p->published < p->o->objects; burst++) {
        moq_rcbuf_t *buf = NULL;

        uint8_t *bytes = malloc(p->o->object_size);
        if (bytes == NULL) {
            p->errors++;
            return;
        }
        payload_fill(bytes, p->o->object_size, p->published);
        int rc = moq_rcbuf_create(moq_alloc_default(), bytes,
                                  p->o->object_size, &buf);
        free(bytes);
        if (rc < 0) {
            p->errors++;
            return;
        }
        if (moq_session_write_object(s, p->sg, p->published, buf,
                                     now) < 0) {
            /* pressure (action/send capacity): retry on a later pump */
            moq_rcbuf_decref(buf);
            return;
        }
        moq_rcbuf_decref(buf);
        p->published++;
    }
    if (p->published == p->o->objects && !p->sg_closed) {
        if (moq_session_close_subgroup(s, p->sg, now) == 0)
            p->sg_closed = true;
    }
}

/* --- subscriber (identical on both backends) -------------------------------- */

struct bench_sub {
    const struct bench_opts *o;
    uint8_t *seen;          /* dedup bitmap, one bit per object */
    uint64_t received;
    uint64_t bytes;
    uint64_t errors;
    uint64_t t0;
    uint64_t t_end;
    bool sub_sent;
    bool done;
};

static void sub_subscribe(moq_session_t *s, uint64_t now)
{
    static const moq_bytes_t ns[] = {
        { (const uint8_t *)"bench", 5 },
        { (const uint8_t *)"wt", 2 },
    };
    moq_subscribe_cfg_t sc;
    moq_subscription_t sub;

    moq_subscribe_cfg_init(&sc);
    sc.track_namespace.parts = (moq_bytes_t *)ns;
    sc.track_namespace.count = 2;
    sc.track_name = (moq_bytes_t){ (const uint8_t *)"objects", 7 };
    sc.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
    (void)moq_session_subscribe(s, &sc, now, &sub);
}

static void sub_verify(struct bench_sub *b, const uint8_t *data,
                       size_t len)
{
    const struct bench_opts *o = b->o;

    if (len != o->object_size) {
        b->errors++;
        return;
    }
    uint64_t idx = 0;
    for (size_t i = 0; i < 8; i++)
        idx |= (uint64_t)data[i] << (8 * i);
    if (idx >= o->objects) {
        b->errors++;
        return;
    }
    if ((b->seen[idx / 8] >> (idx % 8)) & 1u) {
        b->errors++; /* duplicate delivery */
        return;
    }
    b->seen[idx / 8] |= (uint8_t)(1u << (idx % 8));

    /* full content check on a deterministic sample (every object at
     * smoke scale) */
    if (o->objects <= 64 || idx % 16 == 0) {
        for (uint64_t j = 8; j < len; j++)
            if (data[j] != (uint8_t)(idx * 131u + j * 7u + 47u)) {
                b->errors++;
                return;
            }
    }
}

static void sub_handle_event(struct bench_sub *b, moq_session_t *s,
                             const moq_event_t *ev, uint64_t now)
{
    switch (ev->kind) {
    case MOQ_EVENT_SETUP_COMPLETE:
        if (!b->sub_sent) {
            b->sub_sent = true;
            sub_subscribe(s, now);
        }
        break;
    case MOQ_EVENT_SUBSCRIBE_OK:
        if (b->o->warmup == 0 && b->t0 == 0)
            b->t0 = bench_now_us();
        break;
    case MOQ_EVENT_OBJECT_RECEIVED:
        if (ev->u.object_received.payload == NULL) {
            b->errors++;
            break;
        }
        sub_verify(b, moq_rcbuf_data(ev->u.object_received.payload),
                   moq_rcbuf_len(ev->u.object_received.payload));
        b->received++;
        if (b->received > b->o->warmup)
            b->bytes += moq_rcbuf_len(ev->u.object_received.payload);
        if (b->received == b->o->warmup)
            b->t0 = bench_now_us();
        if (b->received == b->o->objects) {
            b->t_end = bench_now_us();
            b->done = true;
        }
        break;
    default:
        break;
    }
}

/* --- wtquic backend ---------------------------------------------------------- */

struct wtq_bench_side {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    struct bench_pub pub;   /* server */
    struct bench_sub sub;   /* client */
    bool is_client;
    bool closed;
    bool close_sent;
};

static void wtq_bench_hook(moq_wtquic_conn_t *conn, void *user)
{
    struct wtq_bench_side *sd = user;
    moq_session_t *s = moq_wtquic_conn_session(conn);
    uint64_t now = bench_now_us();
    moq_event_t ev;

    while (moq_session_poll_events(s, &ev, 1) > 0) {
        if (sd->is_client)
            sub_handle_event(&sd->sub, s, &ev, now);
        else
            pub_handle_event(s, &sd->pub, &ev, now);
        moq_event_cleanup(&ev);
    }
    if (!sd->is_client)
        pub_drive(s, &sd->pub, now);

    if (sd->is_client && sd->sub.done && !sd->close_sent) {
        sd->close_sent = true;
        (void)wtq_session_close(moq_wtquic_conn_wtq_session(conn), 0,
                                NULL, 0);
    }
    /* the ONLY state the app thread reads before teardown: a terminal
     * flag, written and waited under the same mutex. Workload counters
     * stay worker-private until wtq_msquic_env_close has quiesced. */
    if (moq_wtquic_conn_is_closed(conn) || moq_wtquic_conn_is_fatal(conn)) {
        pthread_mutex_lock(&sd->mu);
        sd->closed = true;
        pthread_cond_broadcast(&sd->cv);
        pthread_mutex_unlock(&sd->mu);
    }
}

static bool wtq_wait_flag(struct wtq_bench_side *sd, const bool *flag)
{
    struct timespec deadline;
    bool ok = true;
    bool set;

    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += BENCH_TIMEOUT_SECS;
    pthread_mutex_lock(&sd->mu);
    while (!*flag && ok)
        ok = pthread_cond_timedwait(&sd->cv, &sd->mu, &deadline) == 0;
    set = *flag;
    pthread_mutex_unlock(&sd->mu);
    return set;
}

static int run_wtquic(const struct bench_opts *o, struct bench_result *r)
{
    int rc = -1;
    struct wtq_bench_side sv, cl;
    moq_session_t *server_ms = NULL, *client_ms = NULL;
    moq_wtquic_conn_t *server_conn = NULL, *client_conn = NULL;
    wtq_msquic_env_cfg_t ecfg = WTQ_MSQUIC_ENV_CFG_INIT;
    wtq_msquic_env_t *env = NULL;
    wtq_msquic_listener_t *listener = NULL;
    wtq_session_t *cs = NULL;
    moq_session_cfg_t scfg;
    moq_wtquic_conn_cfg_t ccfg;

    memset(&sv, 0, sizeof(sv));
    memset(&cl, 0, sizeof(cl));
    pthread_mutex_init(&sv.mu, NULL);
    pthread_cond_init(&sv.cv, NULL);
    pthread_mutex_init(&cl.mu, NULL);
    pthread_cond_init(&cl.cv, NULL);
    sv.pub.o = o;
    cl.sub.o = o;
    cl.is_client = true;
    cl.sub.seen = calloc((o->objects + 7) / 8, 1);
    if (cl.sub.seen == NULL)
        goto out;

    moq_session_cfg_init_sized(&scfg, sizeof(scfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_SERVER);
    scfg.send_request_capacity = 1;
    scfg.initial_request_capacity = 10;
    scfg.max_events = 512;
    if (moq_session_create(&scfg, 0, &server_ms) < 0)
        goto out;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_CLIENT);
    scfg.send_request_capacity = 1;
    scfg.initial_request_capacity = 10;
    scfg.max_events = 512;
    if (moq_session_create(&scfg, 0, &client_ms) < 0)
        goto out;

    moq_wtquic_conn_cfg_init_sized(&ccfg, sizeof(ccfg));
    ccfg.alloc = moq_alloc_default();
    ccfg.session = server_ms;
    ccfg.hook = wtq_bench_hook;
    ccfg.hook_user = &sv;
    if (moq_wtquic_conn_create(&ccfg, &server_conn) < 0)
        goto out;
    ccfg.session = client_ms;
    ccfg.hook_user = &cl;
    if (moq_wtquic_conn_create(&ccfg, &client_conn) < 0)
        goto out;

    if (wtq_msquic_env_open(&ecfg, &env) != WTQ_OK)
        goto out;

    wtq_serve_config_t serve = WTQ_SERVE_CONFIG_INIT;
    serve.path = BENCH_PATH;
    serve.subprotocols = bench_protos;
    serve.subprotocol_count = 1;
    serve.require_subprotocol = true;

    wtq_msquic_listener_cfg_t lcfg = WTQ_MSQUIC_LISTENER_CFG_INIT;
    lcfg.bind_address = "127.0.0.1";
    lcfg.port = 0;
    lcfg.cert_file = o->cert;
    lcfg.key_file = o->key;
    lcfg.paths = &serve;
    lcfg.path_count = 1;
    lcfg.events = moq_wtquic_conn_events();
    lcfg.user = server_conn;
    if (wtq_msquic_listener_start(env, &lcfg, &listener) != WTQ_OK)
        goto out;

    wtq_connect_config_t wcfg = WTQ_CONNECT_CONFIG_INIT;
    wcfg.authority = "localhost";
    wcfg.path = BENCH_PATH;
    wcfg.subprotocols = bench_protos;
    wcfg.subprotocol_count = 1;
    wcfg.require_subprotocol = true;

    wtq_msquic_client_cfg_t cli = WTQ_MSQUIC_CLIENT_CFG_INIT;
    cli.server_name = "127.0.0.1";
    cli.port = wtq_msquic_listener_port(listener);
    cli.insecure_skip_verify = true;
    cli.connect = &wcfg;
    cli.events = moq_wtquic_conn_events();
    cli.user = client_conn;
    if (wtq_msquic_client_connect(env, &cli, &cs) != WTQ_OK)
        goto out;

    if (!wtq_wait_flag(&cl, &cl.closed)) {
        fprintf(stderr, "wtquic: timed out waiting for the session "
                        "terminal\n");
        goto out;
    }
    rc = 0;

out:
    if (listener != NULL)
        wtq_msquic_listener_stop(listener);
    if (env != NULL)
        wtq_msquic_env_close(env);
    /* env close is the quiescence barrier: the workers are finished,
     * so workload counters and terminal states are safe to read */
    if (rc == 0 && (moq_wtquic_conn_is_fatal(client_conn) ||
                    moq_wtquic_conn_is_fatal(server_conn))) {
        fprintf(stderr, "wtquic: transport went fatal\n");
        rc = -1;
    }
    if (rc == 0 && !cl.sub.done) {
        fprintf(stderr, "wtquic: closed before completion "
                        "(%llu/%llu objects)\n",
                (unsigned long long)cl.sub.received,
                (unsigned long long)o->objects);
        rc = -1;
    }
    if (cs != NULL)
        wtq_session_release(cs);
    if (client_conn != NULL)
        moq_wtquic_conn_destroy(client_conn);
    if (server_conn != NULL)
        moq_wtquic_conn_destroy(server_conn);
    if (client_ms != NULL)
        moq_session_destroy(client_ms);
    if (server_ms != NULL)
        moq_session_destroy(server_ms);

    if (rc == 0) {
        r->backend = "wtquic";
        r->measured = cl.sub.received - o->warmup;
        r->bytes = cl.sub.bytes;
        r->elapsed_us = cl.sub.t_end - cl.sub.t0;
        r->errors = cl.sub.errors + sv.pub.errors;
    }
    free(cl.sub.seen);
    pthread_mutex_destroy(&sv.mu);
    pthread_cond_destroy(&sv.cv);
    pthread_mutex_destroy(&cl.mu);
    pthread_cond_destroy(&cl.cv);
    return rc;
}

/* --- pico_wt managed backend -------------------------------------------------- */

struct pico_bench_app {
    const struct bench_opts *o;
    struct bench_pub pub;   /* server pump state */
    struct bench_sub sub;   /* client pump state */
};

static int pico_server_pump(moq_pico_wt_managed_t *m, uint64_t now_us,
                            void *ctx)
{
    struct pico_bench_app *a = ctx;
    moq_session_t *s = moq_pico_wt_managed_session(m);
    moq_event_t ev;

    if (s == NULL)
        return 0;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        pub_handle_event(s, &a->pub, &ev, now_us);
        moq_event_cleanup(&ev);
    }
    pub_drive(s, &a->pub, now_us);
    return 0;
}

static int pico_client_pump(moq_pico_wt_managed_t *m, uint64_t now_us,
                            void *ctx)
{
    struct pico_bench_app *a = ctx;
    moq_session_t *s = moq_pico_wt_managed_session(m);
    moq_event_t ev;

    if (s == NULL)
        return 0;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        sub_handle_event(&a->sub, s, &ev, now_us);
        moq_event_cleanup(&ev);
    }
    return a->sub.done ? 1 : 0; /* nonzero: clean pump exit */
}

static int run_pico(const struct bench_opts *o, struct bench_result *r)
{
    int rc = -1;
    struct pico_bench_app app;
    moq_pico_wt_managed_t *srv = NULL, *cli = NULL;
    moq_pico_wt_managed_cfg_t cfg;
    uint64_t deadline = bench_now_us() +
                        (uint64_t)BENCH_TIMEOUT_SECS * 1000000u;

    memset(&app, 0, sizeof(app));
    app.o = o;
    app.pub.o = o;
    app.sub.o = o;
    app.sub.seen = calloc((o->objects + 7) / 8, 1);
    if (app.sub.seen == NULL)
        return -1;

    moq_pico_wt_managed_cfg_init(&cfg);
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.cert_path = o->cert;
    cfg.key_path = o->key;
    cfg.path = BENCH_PATH;
    cfg.port = o->port;
    cfg.send_request_capacity = true; /* grant the client request credit */
    cfg.initial_request_capacity = 16;
    cfg.wt_protocols = BENCH_PROTO;
    cfg.on_pump = pico_server_pump;
    cfg.on_pump_ctx = &app;
    if (moq_pico_wt_managed_create(&cfg, &srv) != MOQ_OK)
        goto out;
    /* let the listener come up before dialing it */
    for (int i = 0; i < 3; i++)
        (void)moq_pico_wt_managed_wait(srv, 100000);

    moq_pico_wt_managed_cfg_init(&cfg);
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = "127.0.0.1";
    cfg.path = BENCH_PATH;
    cfg.port = o->port;
    cfg.insecure_skip_verify = true;
    cfg.wt_protocols = BENCH_PROTO;
    cfg.on_pump = pico_client_pump;
    cfg.on_pump_ctx = &app;
    if (moq_pico_wt_managed_create(&cfg, &cli) != MOQ_OK)
        goto out;

    for (;;) {
        moq_result_t w = moq_pico_wt_managed_wait(cli, 200000);

        if (w == MOQ_ERR_CLOSED)
            break; /* pump exit (done), clean close, or fatal */
        (void)moq_pico_wt_managed_wake(srv);
        (void)moq_pico_wt_managed_wake(cli);
        if (bench_now_us() > deadline) {
            fprintf(stderr, "pico_wt: timed out (%llu/%llu objects)\n",
                    (unsigned long long)app.sub.received,
                    (unsigned long long)o->objects);
            goto out;
        }
    }
    /* stop joins the network threads; app.sub is safe to read after */
    moq_pico_wt_managed_stop(cli);
    moq_pico_wt_managed_stop(srv);
    if (moq_pico_wt_managed_is_fatal(cli) ||
        moq_pico_wt_managed_is_fatal(srv)) {
        fprintf(stderr, "pico_wt: transport went fatal "
                        "(client=%llu server=%llu)\n",
                (unsigned long long)moq_pico_wt_managed_fatal_code(cli),
                (unsigned long long)moq_pico_wt_managed_fatal_code(srv));
        goto out;
    }
    if (!app.sub.done) {
        fprintf(stderr,
                "pico_wt: terminated before completion "
                "(%llu/%llu objects; client fatal=%d code=%llu "
                "closed=%d; server fatal=%d code=%llu)\n",
                (unsigned long long)app.sub.received,
                (unsigned long long)o->objects,
                moq_pico_wt_managed_is_fatal(cli) ? 1 : 0,
                (unsigned long long)moq_pico_wt_managed_fatal_code(cli),
                moq_pico_wt_managed_is_closed(cli) ? 1 : 0,
                moq_pico_wt_managed_is_fatal(srv) ? 1 : 0,
                (unsigned long long)moq_pico_wt_managed_fatal_code(srv));
        goto out;
    }
    rc = 0;

out:
    if (cli != NULL) {
        moq_pico_wt_managed_stop(cli);
        moq_pico_wt_managed_destroy(cli);
    }
    if (srv != NULL) {
        moq_pico_wt_managed_stop(srv);
        moq_pico_wt_managed_destroy(srv);
    }
    if (rc == 0) {
        r->backend = "pico_wt_managed";
        r->measured = app.sub.received - o->warmup;
        r->bytes = app.sub.bytes;
        r->elapsed_us = app.sub.t_end - app.sub.t0;
        r->errors = app.sub.errors + app.pub.errors;
    }
    free(app.sub.seen);
    return rc;
}

/* --- output -------------------------------------------------------------------- */

static double result_ops(const struct bench_result *r)
{
    return r->elapsed_us > 0
               ? (double)r->measured * 1e6 / (double)r->elapsed_us
               : 0.0;
}

static double result_mbps(const struct bench_result *r)
{
    return r->elapsed_us > 0
               ? (double)r->bytes * 8.0 / (double)r->elapsed_us
               : 0.0;
}

static void result_print(const struct bench_opts *o,
                         const struct bench_result *r)
{
    if (o->json) {
        printf("{\"backend\":\"%s\",\"objects\":%llu,"
               "\"object_size\":%llu,\"warmup\":%llu,"
               "\"measured_objects\":%llu,\"bytes\":%llu,"
               "\"elapsed_us\":%llu,\"objects_per_sec\":%.1f,"
               "\"mbps\":%.1f,\"errors\":%llu}\n",
               r->backend, (unsigned long long)o->objects,
               (unsigned long long)o->object_size,
               (unsigned long long)o->warmup,
               (unsigned long long)r->measured,
               (unsigned long long)r->bytes,
               (unsigned long long)r->elapsed_us, result_ops(r),
               result_mbps(r), (unsigned long long)r->errors);
    } else {
        printf("%-16s %8llu obj x %5llu B  %10llu us  %12.1f obj/s"
               "  %9.1f Mbit/s  errors=%llu\n",
               r->backend, (unsigned long long)r->measured,
               (unsigned long long)o->object_size,
               (unsigned long long)r->elapsed_us, result_ops(r),
               result_mbps(r), (unsigned long long)r->errors);
    }
}

static void usage(const char *argv0)
{
    fprintf(stderr,
        "usage: %s --backend wtquic|pico_wt|both --cert <cert.pem> "
        "--key <key.pem>\n"
        "          [--objects N] [--object-size N] [--warmup N]\n"
        "          [--burst N] [--port N] [--json]\n"
        "\n"
        "Runs one MoQ subscription with N objects, published\n"
        "sequentially on one subgroup stream, over the selected\n"
        "WebTransport transport(s) on localhost.\n"
        "objects counts warmup; measured_objects = objects - warmup.\n"
        "pico_wt uses the managed real-loopback facade (labelled\n"
        "pico_wt_managed in output) and needs --port (default 14567).\n"
        "With --backend both, one JSON object per backend is printed\n"
        "per line, then a ratio line (wtquic over pico_wt_managed).\n",
        argv0);
}

int main(int argc, char **argv)
{
    struct bench_opts o = {
        .backend = "both",
        .objects = 1000,
        .object_size = 1200,
        .warmup = 50,
        .burst = BENCH_BURST_DEFAULT,
        .port = 14567,
    };

    for (int i = 1; i < argc; i++) {
        const char *a = argv[i];

        if (strcmp(a, "--backend") == 0 && i + 1 < argc)
            o.backend = argv[++i];
        else if (strcmp(a, "--cert") == 0 && i + 1 < argc)
            o.cert = argv[++i];
        else if (strcmp(a, "--key") == 0 && i + 1 < argc)
            o.key = argv[++i];
        else if (strcmp(a, "--objects") == 0 && i + 1 < argc)
            o.objects = strtoull(argv[++i], NULL, 10);
        else if (strcmp(a, "--object-size") == 0 && i + 1 < argc)
            o.object_size = strtoull(argv[++i], NULL, 10);
        else if (strcmp(a, "--warmup") == 0 && i + 1 < argc)
            o.warmup = strtoull(argv[++i], NULL, 10);
        else if (strcmp(a, "--burst") == 0 && i + 1 < argc)
            o.burst = strtoull(argv[++i], NULL, 10);
        else if (strcmp(a, "--port") == 0 && i + 1 < argc)
            o.port = atoi(argv[++i]);
        else if (strcmp(a, "--json") == 0)
            o.json = true;
        else {
            usage(argv[0]);
            return 2;
        }
    }

    bool want_wtq = strcmp(o.backend, "wtquic") == 0 ||
                    strcmp(o.backend, "both") == 0;
    bool want_pico = strcmp(o.backend, "pico_wt") == 0 ||
                     strcmp(o.backend, "both") == 0;
    if ((!want_wtq && !want_pico) || o.cert == NULL || o.key == NULL ||
        o.objects == 0 || o.object_size < 8 || o.burst == 0 ||
        o.warmup >= o.objects || o.port <= 0 || o.port > 65535) {
        usage(argv[0]);
        return 2;
    }

    struct bench_result rw = { 0 }, rp = { 0 };
    int failures = 0;

    if (want_wtq) {
        if (run_wtquic(&o, &rw) != 0 || rw.errors != 0) {
            fprintf(stderr, "wtquic run failed (errors=%llu)\n",
                    (unsigned long long)rw.errors);
            failures++;
        } else {
            result_print(&o, &rw);
        }
    }
    if (want_pico) {
        if (run_pico(&o, &rp) != 0 || rp.errors != 0) {
            fprintf(stderr, "pico_wt run failed (errors=%llu)\n",
                    (unsigned long long)rp.errors);
            failures++;
        } else {
            result_print(&o, &rp);
        }
    }
    if (failures == 0 && want_wtq && want_pico) {
        double ratio = result_ops(&rp) > 0.0
                           ? result_ops(&rw) / result_ops(&rp)
                           : 0.0;

        if (o.json)
            printf("{\"ratio\":{\"objects_per_sec_wtquic_over_pico\":"
                   "%.3f}}\n", ratio);
        else
            printf("ratio: wtquic/pico_wt_managed = %.3fx obj/s\n",
                   ratio);
    }
    return failures;
}
