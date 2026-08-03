/*
 * Raw MoQ-over-QUIC transport comparison: the identical workload — one
 * subscription, N deterministic objects published sequentially on ONE
 * subgroup stream — measured over both raw-QUIC managed paths:
 *
 *   msquic_managed  libmoq -> adapters/msquic managed lane facade ->
 *                   real MsQuic loopback (real UDP, MsQuic worker
 *                   threads + the lane mutex/doorbell)
 *   pq_threaded     libmoq -> adapters/picoquic threaded facade ->
 *                   real picoquic loopback (real UDP, one picoquic
 *                   network thread)
 *
 * Both paths are real localhost transport with real TLS at MoQ ALPN
 * moqt-16. Localhost loopback still is not a network: the numbers
 * compare stack overhead, not congestion behavior — and the two
 * threading models differ by design (MsQuic worker threads vs one
 * picoquic network thread), which is part of what is being compared.
 *
 * The subscriber counts exact objects and bytes and verifies payload
 * integrity (index prefix and length on every object, full content on
 * a deterministic sample). Warmup objects are excluded: the window
 * opens at the last warmup object's arrival (at SUBSCRIBE_OK when
 * warmup is 0) and closes at the final object's arrival. A run fails
 * nonzero on payload corruption, duplicate or out-of-range objects, a
 * fatal terminal, or termination before all objects arrived.
 *
 * With --backend both, one JSON object per backend is printed per
 * line, followed by a ratio line (msquic_managed over pq_threaded).
 * When built with mvfst support, --backend mvfst_managed adds the
 * third raw-QUIC managed path (libmoq -> adapters/mvfst managed
 * facade -> real mvfst loopback) and --backend all runs all three,
 * printing a ratio line per pair.
 */

#if defined(__linux__) && !defined(_POSIX_C_SOURCE)
#define _POSIX_C_SOURCE 200809L
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <moq/moq.h>
#include <moq/msquic_managed.h>
#include <moq/picoquic_threaded.h>
#ifdef MOQ_BENCH_HAVE_MVFST
#include <moq/mvfst.h>
#endif

#define BENCH_BURST_DEFAULT 64  /* publishes attempted per pump */
#define BENCH_TIMEOUT_SECS 60

struct bench_opts {
    const char *backend;    /* "msquic_managed" | "pq_threaded" |
                               "mvfst_managed" | "both" | "all" */
    const char *cert;
    const char *key;
    uint64_t objects;       /* total published, warmup included */
    uint64_t object_size;   /* bytes per object (>= 8) */
    uint64_t warmup;        /* leading objects excluded from timing */
    uint64_t burst;         /* publishes attempted per pump */
    int port;               /* pq_threaded listen port (msquic uses 0) */
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
 * the next pump. */
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
        { (const uint8_t *)"quic", 4 },
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

/* --- shared app state (one per backend run) ----------------------------------- */

struct bench_app {
    const struct bench_opts *o;
    struct bench_pub pub;   /* server pump state */
    struct bench_sub sub;   /* client pump state */
};

static int bench_app_init(struct bench_app *a, const struct bench_opts *o)
{
    memset(a, 0, sizeof(*a));
    a->o = o;
    a->pub.o = o;
    a->sub.o = o;
    a->sub.seen = calloc((o->objects + 7) / 8, 1);
    return a->sub.seen != NULL ? 0 : -1;
}

/* --- msquic managed backend ----------------------------------------------------- */

static int msq_server_pump(moq_msquic_managed_t *m,
                           moq_msquic_managed_lane_t *lane, uint64_t now_us,
                           void *ctx)
{
    struct bench_app *a = ctx;
    /* server: drive the single accepted session via lane iteration */
    moq_msquic_managed_conn_t *c = moq_msquic_lane_next_conn(lane, NULL);
    moq_session_t *s = c != NULL ? moq_msquic_managed_conn_session(c)
                                 : NULL;
    moq_event_t ev;

    (void)m;
    if (s == NULL)
        return 0;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        pub_handle_event(s, &a->pub, &ev, now_us);
        moq_event_cleanup(&ev);
    }
    pub_drive(s, &a->pub, now_us);
    return 0;
}

static int msq_client_pump(moq_msquic_managed_t *m,
                           moq_msquic_managed_lane_t *lane, uint64_t now_us,
                           void *ctx)
{
    struct bench_app *a = ctx;
    moq_session_t *s = moq_msquic_managed_session(m);
    moq_event_t ev;

    (void)lane;

    if (s == NULL)
        return 0;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        sub_handle_event(&a->sub, s, &ev, now_us);
        moq_event_cleanup(&ev);
    }
    return a->sub.done ? 1 : 0; /* nonzero: clean pump exit */
}

static int run_msquic(const struct bench_opts *o, struct bench_result *r)
{
    int rc = -1;
    struct bench_app app;
    moq_msquic_managed_t *srv = NULL, *cli = NULL;
    moq_msquic_managed_cfg_t cfg;
    uint64_t deadline = bench_now_us() +
                        (uint64_t)BENCH_TIMEOUT_SECS * 1000000u;

    if (bench_app_init(&app, o) != 0)
        return -1;

    moq_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.host = "127.0.0.1";
    cfg.port = 0; /* ephemeral */
    cfg.cert_path = o->cert;
    cfg.key_path = o->key;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 10;
    cfg.max_events = 512;
    cfg.on_lane_pump = msq_server_pump;
    cfg.on_lane_pump_user = &app;
    if (moq_msquic_managed_create(&cfg, &srv) != MOQ_OK)
        goto out;

    moq_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = "127.0.0.1";
    cfg.port = moq_msquic_managed_port(srv);
    cfg.insecure_skip_verify = true;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 10;
    cfg.max_events = 512;
    cfg.on_lane_pump = msq_client_pump;
    cfg.on_lane_pump_user = &app;
    if (moq_msquic_managed_create(&cfg, &cli) != MOQ_OK)
        goto out;

    bool timed_out = false;

    /* Watchdog pacing: waking on every activity return feeds back —
     * a wake-driven pump latches activity itself, so the loop spins
     * at condvar speed doing no-op pumps and steals throughput from
     * the transport workers. All steady-state progress here is
     * transport-event driven; the only thing an app wake provides is
     * a liveness tick for retry paths with no pending transport event
     * (large-object drains hit this). So: treat MOQ_OK as news, not a
     * reason to wake, and tick both facades only when a short wait
     * times out. */
    for (;;) {
        moq_result_t w = moq_msquic_managed_wait(cli, 5000);

        if (w == MOQ_ERR_CLOSED)
            break; /* pump exit (done), clean close, or fatal */
        if (w == MOQ_DONE) {
            (void)moq_msquic_managed_wake(srv);
            (void)moq_msquic_managed_wake(cli);
        }
        if (bench_now_us() > deadline) {
            timed_out = true;
            break;
        }
    }
    /* stop quiesces the transport FIRST; only then are the workload
     * counters safe to read — including for the timeout diagnostic
     * (the pumps may still be mutating them until here) */
    (void)moq_msquic_managed_stop(cli);
    (void)moq_msquic_managed_stop(srv);
    if (timed_out) {
        fprintf(stderr,
                "msquic_managed: timed out (%llu/%llu objects)\n",
                (unsigned long long)app.sub.received,
                (unsigned long long)o->objects);
        goto out;
    }
    if (moq_msquic_managed_is_fatal(cli) ||
        moq_msquic_managed_is_fatal(srv)) {
        fprintf(stderr,
                "msquic_managed: transport went fatal "
                "(client=%llu server=%llu)\n",
                (unsigned long long)moq_msquic_managed_fatal_code(cli),
                (unsigned long long)moq_msquic_managed_fatal_code(srv));
        goto out;
    }
    if (!app.sub.done) {
        fprintf(stderr,
                "msquic_managed: terminated before completion "
                "(%llu/%llu objects)\n",
                (unsigned long long)app.sub.received,
                (unsigned long long)o->objects);
        goto out;
    }
    rc = 0;

out:
    if (cli != NULL) {
        (void)moq_msquic_managed_stop(cli);
        moq_msquic_managed_destroy(cli);
    }
    if (srv != NULL) {
        (void)moq_msquic_managed_stop(srv);
        moq_msquic_managed_destroy(srv);
    }
    if (rc == 0) {
        r->backend = "msquic_managed";
        r->measured = app.sub.received - o->warmup;
        r->bytes = app.sub.bytes;
        r->elapsed_us = app.sub.t_end - app.sub.t0;
        r->errors = app.sub.errors + app.pub.errors;
    }
    free(app.sub.seen);
    return rc;
}

/* --- pq_threaded backend ---------------------------------------------------------- */

static int pq_server_pump(moq_pq_threaded_t *t, moq_pq_threaded_lane_t *lane,
                          uint64_t now_us, void *ctx)
{
    struct bench_app *a = ctx;
    moq_session_t *s = moq_pq_threaded_session(t);
    moq_event_t ev;

    (void)lane;
    if (s == NULL)
        return 0;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        pub_handle_event(s, &a->pub, &ev, now_us);
        moq_event_cleanup(&ev);
    }
    pub_drive(s, &a->pub, now_us);
    return 0;
}

static int pq_client_pump(moq_pq_threaded_t *t, moq_pq_threaded_lane_t *lane,
                          uint64_t now_us, void *ctx)
{
    struct bench_app *a = ctx;
    moq_session_t *s = moq_pq_threaded_session(t);
    moq_event_t ev;

    (void)lane;
    if (s == NULL)
        return 0;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        sub_handle_event(&a->sub, s, &ev, now_us);
        moq_event_cleanup(&ev);
    }
    return a->sub.done ? 1 : 0; /* nonzero: clean pump exit */
}

static int run_pq(const struct bench_opts *o, struct bench_result *r)
{
    int rc = -1;
    struct bench_app app;
    moq_pq_threaded_t *srv = NULL, *cli = NULL;
    moq_pq_threaded_cfg_t cfg;
    uint64_t deadline = bench_now_us() +
                        (uint64_t)BENCH_TIMEOUT_SECS * 1000000u;

    if (bench_app_init(&app, o) != 0)
        return -1;

    moq_pq_threaded_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.cert_path = o->cert;
    cfg.key_path = o->key;
    cfg.port = o->port;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 10;
    cfg.max_events = 512;
    cfg.on_lane_pump = pq_server_pump;
    cfg.on_lane_pump_ctx = &app;
    if (moq_pq_threaded_create(&cfg, &srv) != MOQ_OK)
        goto out;
    /* let the listener come up before dialing it */
    for (int i = 0; i < 3; i++)
        (void)moq_pq_threaded_wait(srv, 100000);

    moq_pq_threaded_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = "127.0.0.1";
    cfg.port = o->port;
    cfg.insecure_skip_verify = true;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 10;
    cfg.max_events = 512;
    cfg.on_lane_pump = pq_client_pump;
    cfg.on_lane_pump_ctx = &app;
    if (moq_pq_threaded_create(&cfg, &cli) != MOQ_OK)
        goto out;

    bool timed_out = false;

    for (;;) {
        moq_result_t w = moq_pq_threaded_wait(cli, 200000);

        if (w == MOQ_ERR_CLOSED)
            break; /* pump exit (done), clean close, or fatal */
        (void)moq_pq_threaded_wake(srv);
        (void)moq_pq_threaded_wake(cli);
        if (bench_now_us() > deadline) {
            timed_out = true;
            break;
        }
    }
    /* stop joins the network threads FIRST; only then are the workload
     * counters safe to read — including for the timeout diagnostic */
    (void)moq_pq_threaded_stop(cli);
    (void)moq_pq_threaded_stop(srv);
    if (timed_out) {
        fprintf(stderr,
                "pq_threaded: timed out (%llu/%llu objects)\n",
                (unsigned long long)app.sub.received,
                (unsigned long long)o->objects);
        goto out;
    }
    if (moq_pq_threaded_is_fatal(cli) || moq_pq_threaded_is_fatal(srv)) {
        fprintf(stderr,
                "pq_threaded: transport went fatal "
                "(client=%llu server=%llu)\n",
                (unsigned long long)moq_pq_threaded_fatal_code(cli),
                (unsigned long long)moq_pq_threaded_fatal_code(srv));
        goto out;
    }
    if (!app.sub.done) {
        fprintf(stderr,
                "pq_threaded: terminated before completion "
                "(%llu/%llu objects)\n",
                (unsigned long long)app.sub.received,
                (unsigned long long)o->objects);
        goto out;
    }
    rc = 0;

out:
    if (cli != NULL) {
        (void)moq_pq_threaded_stop(cli);
        moq_pq_threaded_destroy(cli);
    }
    if (srv != NULL) {
        (void)moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }
    if (rc == 0) {
        r->backend = "pq_threaded";
        r->measured = app.sub.received - o->warmup;
        r->bytes = app.sub.bytes;
        r->elapsed_us = app.sub.t_end - app.sub.t0;
        r->errors = app.sub.errors + app.pub.errors;
    }
    free(app.sub.seen);
    return rc;
}

#ifdef MOQ_BENCH_HAVE_MVFST
/* --- mvfst_managed backend --------------------------------------------------------- */

static int mvfst_server_pump(moq_mvfst_managed_t *m,
                             moq_mvfst_managed_lane_t *lane, uint64_t now_us,
                             void *ctx)
{
    struct bench_app *a = ctx;
    moq_event_t ev;

    (void)m;
    /* server sessions hang off accepted connections, iterated through the
     * lane; the client-only managed_session() accessor is NULL here */
    for (moq_mvfst_conn_t *c = moq_mvfst_lane_next_conn(lane, NULL);
         c != NULL; c = moq_mvfst_lane_next_conn(lane, c)) {
        moq_session_t *s = moq_mvfst_conn_session(c);

        if (s == NULL)
            continue;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            pub_handle_event(s, &a->pub, &ev, now_us);
            moq_event_cleanup(&ev);
        }
        pub_drive(s, &a->pub, now_us);
    }
    return 0;
}

static int mvfst_client_pump(moq_mvfst_managed_t *m,
                             moq_mvfst_managed_lane_t *lane, uint64_t now_us,
                             void *ctx)
{
    struct bench_app *a = ctx;
    moq_session_t *s = moq_mvfst_managed_session(m);
    moq_event_t ev;

    (void)lane;
    if (s == NULL)
        return 0;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        sub_handle_event(&a->sub, s, &ev, now_us);
        moq_event_cleanup(&ev);
    }
    return a->sub.done ? 1 : 0; /* nonzero: clean pump exit */
}

static int run_mvfst(const struct bench_opts *o, struct bench_result *r)
{
    int rc = -1;
    struct bench_app app;
    moq_mvfst_managed_t *srv = NULL, *cli = NULL;
    moq_mvfst_managed_cfg_t cfg;
    uint64_t deadline = bench_now_us() +
                        (uint64_t)BENCH_TIMEOUT_SECS * 1000000u;

    if (bench_app_init(&app, o) != 0)
        return -1;

    moq_mvfst_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.host = "127.0.0.1";
    cfg.port = 0; /* ephemeral */
    cfg.cert_path = o->cert;
    cfg.key_path = o->key;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 10;
    cfg.max_events = 512;
    cfg.on_lane_pump = mvfst_server_pump;
    cfg.user_ctx = &app;
    if (moq_mvfst_managed_create(&cfg, &srv) != MOQ_OK)
        goto out;

    moq_mvfst_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = "127.0.0.1";
    cfg.port = moq_mvfst_managed_local_port(srv);
    cfg.insecure_skip_verify = true;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 10;
    cfg.max_events = 512;
    cfg.on_lane_pump = mvfst_client_pump;
    cfg.user_ctx = &app;
    if (moq_mvfst_managed_create(&cfg, &cli) != MOQ_OK)
        goto out;

    bool timed_out = false;

    /* same watchdog pacing as the msquic loop: MOQ_OK is news, not a
     * reason to wake; tick both facades only on timeout */
    for (;;) {
        moq_result_t w = moq_mvfst_managed_wait(cli, 5000);

        if (w == MOQ_ERR_CLOSED)
            break; /* pump exit (done), clean close, or fatal */
        if (w == MOQ_DONE) {
            (void)moq_mvfst_managed_wake(srv);
            (void)moq_mvfst_managed_wake(cli);
        }
        if (bench_now_us() > deadline) {
            timed_out = true;
            break;
        }
    }
    /* stop joins the network threads FIRST; only then are the workload
     * counters safe to read — including for the timeout diagnostic */
    (void)moq_mvfst_managed_stop(cli);
    (void)moq_mvfst_managed_stop(srv);
    if (timed_out) {
        fprintf(stderr,
                "mvfst_managed: timed out (%llu/%llu objects)\n",
                (unsigned long long)app.sub.received,
                (unsigned long long)o->objects);
        goto out;
    }
    if (moq_mvfst_managed_is_fatal(cli) ||
        moq_mvfst_managed_is_fatal(srv)) {
        fprintf(stderr,
                "mvfst_managed: transport went fatal "
                "(client=%llu server=%llu)\n",
                (unsigned long long)moq_mvfst_managed_fatal_code(cli),
                (unsigned long long)moq_mvfst_managed_fatal_code(srv));
        goto out;
    }
    if (!app.sub.done) {
        fprintf(stderr,
                "mvfst_managed: terminated before completion "
                "(%llu/%llu objects)\n",
                (unsigned long long)app.sub.received,
                (unsigned long long)o->objects);
        goto out;
    }
    rc = 0;

out:
    if (cli != NULL) {
        (void)moq_mvfst_managed_stop(cli);
        moq_mvfst_managed_destroy(cli);
    }
    if (srv != NULL) {
        (void)moq_mvfst_managed_stop(srv);
        moq_mvfst_managed_destroy(srv);
    }
    if (rc == 0) {
        r->backend = "mvfst_managed";
        r->measured = app.sub.received - o->warmup;
        r->bytes = app.sub.bytes;
        r->elapsed_us = app.sub.t_end - app.sub.t0;
        r->errors = app.sub.errors + app.pub.errors;
    }
    free(app.sub.seen);
    return rc;
}
#endif /* MOQ_BENCH_HAVE_MVFST */

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

static void ratio_print(const struct bench_opts *o, const char *pair,
                        const struct bench_result *num,
                        const struct bench_result *den)
{
    double ratio = result_ops(den) > 0.0
                       ? result_ops(num) / result_ops(den)
                       : 0.0;

    if (o->json)
        printf("{\"ratio\":{\"objects_per_sec_%s\":%.3f}}\n", pair,
               ratio);
    else
        printf("ratio: %s = %.3fx obj/s\n", pair, ratio);
}

static void usage(const char *argv0)
{
    fprintf(stderr,
        "usage: %s --backend "
        "msquic_managed|pq_threaded|mvfst_managed|both|all\n"
        "          --cert <cert.pem> --key <key.pem>\n"
        "          [--objects N] [--object-size N] [--warmup N]\n"
        "          [--burst N] [--port N] [--json]\n"
        "\n"
        "Runs one MoQ subscription with N objects, published\n"
        "sequentially on one subgroup stream, over the selected raw\n"
        "MoQ-over-QUIC transport(s) on localhost (ALPN moqt-16).\n"
        "objects counts warmup; measured_objects = objects - warmup.\n"
        "msquic_managed listens on an ephemeral port; pq_threaded\n"
        "needs --port (default 14667).\n"
        "With --backend both (or all, when built with mvfst), one\n"
        "JSON object per backend is printed per line, then one ratio\n"
        "line per backend pair.\n",
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
        .port = 14667,
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

    bool want_all = strcmp(o.backend, "all") == 0;
    bool want_msq = strcmp(o.backend, "msquic_managed") == 0 ||
                    strcmp(o.backend, "both") == 0 || want_all;
    bool want_pq = strcmp(o.backend, "pq_threaded") == 0 ||
                   strcmp(o.backend, "both") == 0 || want_all;
    bool want_mvfst = strcmp(o.backend, "mvfst_managed") == 0 || want_all;
#ifndef MOQ_BENCH_HAVE_MVFST
    if (want_mvfst) {
        fprintf(stderr, "mvfst_managed not built into this binary "
                        "(configure with MOQ_BUILD_ADAPTER_MVFST=ON)\n");
        return 2;
    }
#endif
    if ((!want_msq && !want_pq && !want_mvfst) || o.cert == NULL ||
        o.key == NULL || o.objects == 0 || o.object_size < 8 ||
        o.burst == 0 || o.warmup >= o.objects || o.port <= 0 ||
        o.port > 65535) {
        usage(argv[0]);
        return 2;
    }

    struct bench_result rm = { 0 }, rp = { 0 }, rv = { 0 };
    bool ok_msq = false, ok_pq = false, ok_mvfst = false;
    int failures = 0;

    if (want_msq) {
        if (run_msquic(&o, &rm) != 0 || rm.errors != 0) {
            fprintf(stderr, "msquic_managed run failed (errors=%llu)\n",
                    (unsigned long long)rm.errors);
            failures++;
        } else {
            ok_msq = true;
            result_print(&o, &rm);
        }
    }
    if (want_pq) {
        if (run_pq(&o, &rp) != 0 || rp.errors != 0) {
            fprintf(stderr, "pq_threaded run failed (errors=%llu)\n",
                    (unsigned long long)rp.errors);
            failures++;
        } else {
            ok_pq = true;
            result_print(&o, &rp);
        }
    }
#ifdef MOQ_BENCH_HAVE_MVFST
    if (want_mvfst) {
        if (run_mvfst(&o, &rv) != 0 || rv.errors != 0) {
            fprintf(stderr, "mvfst_managed run failed (errors=%llu)\n",
                    (unsigned long long)rv.errors);
            failures++;
        } else {
            ok_mvfst = true;
            result_print(&o, &rv);
        }
    }
#endif
    if (ok_msq && ok_pq)
        ratio_print(&o, "msquic_managed_over_pq_threaded", &rm, &rp);
    if (ok_msq && ok_mvfst)
        ratio_print(&o, "msquic_managed_over_mvfst_managed", &rm, &rv);
    if (ok_mvfst && ok_pq)
        ratio_print(&o, "mvfst_managed_over_pq_threaded", &rv, &rp);
    return failures;
}
