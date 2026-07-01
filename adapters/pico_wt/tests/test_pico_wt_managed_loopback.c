/*
 * Managed pico WT loopback. A managed server and one or two managed
 * clients, each owning its own picoquic network thread, talk over real
 * loopback UDP.
 *
 *   --mode loopback (default): server + one client; the client
 *       subscribes, the server publishes one object, the client
 *       receives it; both are stopped and destroyed.
 *   --mode refuse: server + client1 (served) + client2; the second
 *       WT CONNECT is refused (one active connection). The server's path
 *       callback returns -1, h3zero answers HTTP 501, and the client maps
 *       the non-2xx status to connect_refused → fatal. So client2 reaches
 *       a terminal FATAL state in bounded time (wait()==MOQ_ERR_CLOSED),
 *       never reaches setup, gets no object, and exposes no session.
 *   --mode bigobj: server + one client; the server publishes a small
 *       control object and a 256 KiB object on one subgroup; the client
 *       verifies both arrive intact (exact length + byte pattern). The
 *       large transfer spans many packets/RTTs, so this mode waits by
 *       real elapsed time rather than an iteration count.
 *   --mode close: server + one client; after the object the client
 *       sends GOAWAY and (after the drain timer) reaches a clean
 *       terminal close — wait()/wake() return MOQ_ERR_CLOSED and the
 *       client is closed (not fatal) with MOQ_CLOSE_GOAWAY_TIMEOUT. The
 *       loop attempts a bounded best-effort flush of the CLOSE_SESSION
 *       capsule before stopping; the test also checks the server never
 *       goes fatal. (Prompt server-facade is_closed from the inbound
 *       capsule is timing-dependent over real threads; the capsule parse
 *       + close-code propagation are proven deterministically in
 *       test_pico_wt_loopback.)
 *
 * Each side's MoQ work runs on its own network thread inside on_pump;
 * the main thread only creates/waits/stops/destroys and reads atomic
 * flags published through the facades' wait() barriers.
 *
 * Args: --cert <file> --key <file> [--mode loopback|bigobj|refuse|close]
 */

#include <moq/moq.h>
#include <moq/pico_wt_managed.h>

#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>     /* clock_gettime for the large-object transfer wait */
#include <unistd.h>   /* getpid for per-process port spreading */

static int failures = 0;
#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

/* Per-attempt outcome, so a busy port (retryable) is not conflated
 * with a real handshake/protocol failure (fatal). */
typedef enum { RUN_OK = 1, RUN_RETRY = 0, RUN_HARD = -1 } run_status_t;

/* -- bigobj regression sizing --------------------------------------- */

/* Guards delivery of a large object over a WT subgroup stream: a small
 * control object followed by a 256 KiB object on the same subgroup, both
 * verified intact. A large object spans many packets and several RTTs, so
 * the test must wait by real elapsed time (see wait_realtime) — the
 * iteration-count loop used elsewhere assumes each wait() consumes its
 * full interval and under-budgets an active transfer. The control object
 * also proves the small path and ordering ahead of the big one. */
#define BIGOBJ_SMALL          "ctrl"
#define BIGOBJ_SMALL_LEN      4u
#define BIGOBJ_BIG_SIZE       (256u * 1024u)
/* Deterministic, position-dependent pattern so a reorder or truncation
 * is caught, not just a wrong length. */
static uint8_t bigobj_byte(size_t i) { return (uint8_t)((i * 131u + 7u) & 0xffu); }

/* -- server side ---------------------------------------------------- */

typedef struct {
    atomic_int published; /* set on the network thread, read by the app thread
                           * (bigobj_drain), so it must be atomic. */
    size_t big_size;   /* >0: publish [control, big] on one subgroup */
} server_app_t;

static void serve_subscribe(moq_session_t *s, moq_subscription_t sub,
                            uint64_t now, server_app_t *a)
{
    moq_accept_subscribe_cfg_t acfg;
    moq_accept_subscribe_cfg_init(&acfg);
    if (moq_session_accept_subscribe(s, sub, &acfg, now) < 0) return;

    moq_subgroup_cfg_t sgcfg;
    moq_subgroup_cfg_init(&sgcfg);
    sgcfg.group_id = 0;
    sgcfg.publisher_priority = 200;
    moq_subgroup_handle_t sg;
    if (moq_session_open_subgroup(s, sub, &sgcfg, now, &sg) < 0) return;

    if (a->big_size == 0) {
        moq_rcbuf_t *buf = NULL;
        if (moq_rcbuf_create(moq_alloc_default(),
                (const uint8_t *)"hello-pico-wt", 13, &buf) < 0) return;
        int wrc = moq_session_write_object(s, sg, 0, buf, now);
        moq_rcbuf_decref(buf);
        if (wrc < 0) { moq_session_close_subgroup(s, sg, now); return; }
        if (moq_session_close_subgroup(s, sg, now) < 0) return;
        atomic_store(&a->published, 1);
        return;
    }

    /* Object 0: small control. Object 1: large. Same subgroup stream, so
     * this exercises header/payload/header/payload ordering and proves
     * the large body delivers in full. */
    moq_rcbuf_t *small = NULL;
    if (moq_rcbuf_create(moq_alloc_default(),
            (const uint8_t *)BIGOBJ_SMALL, BIGOBJ_SMALL_LEN, &small) < 0)
        return;
    int wrc = moq_session_write_object(s, sg, 0, small, now);
    moq_rcbuf_decref(small);
    if (wrc < 0) { moq_session_close_subgroup(s, sg, now); return; }

    uint8_t *big = (uint8_t *)malloc(a->big_size);
    if (!big) { moq_session_close_subgroup(s, sg, now); return; }
    for (size_t i = 0; i < a->big_size; i++) big[i] = bigobj_byte(i);
    moq_rcbuf_t *bbuf = NULL;
    if (moq_rcbuf_create(moq_alloc_default(), big, a->big_size, &bbuf) < 0) {
        free(big);
        moq_session_close_subgroup(s, sg, now);
        return;
    }
    free(big);   /* rcbuf_create copies */
    wrc = moq_session_write_object(s, sg, 1, bbuf, now);
    moq_rcbuf_decref(bbuf);
    if (wrc < 0) { moq_session_close_subgroup(s, sg, now); return; }

    if (moq_session_close_subgroup(s, sg, now) < 0) return;
    atomic_store(&a->published, 1);
}

static int server_pump(moq_pico_wt_managed_t *m, uint64_t now, void *ctx)
{
    server_app_t *a = (server_app_t *)ctx;
    moq_session_t *s = moq_pico_wt_managed_session(m);
    if (!s) return 0;
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST && !atomic_load(&a->published))
            serve_subscribe(s, ev.u.subscribe_request.sub, now, a);
        moq_event_cleanup(&ev);
    }
    return 0;
}

/* -- client side ---------------------------------------------------- */

typedef struct {
    int        subscribed;
    atomic_int got_object;
    /* bigobj mode: verify a small control object and a large object both
     * arrive intact (exact length + position-dependent pattern). */
    size_t     expect_big;       /* >0 enables bigobj verification */
    atomic_int small_ok;
    atomic_int big_ok;
    atomic_int big_bad;          /* length/content mismatch seen */
    /* close mode: after the object, the client initiates GOAWAY and
     * keeps pumping so the drain timer fires and the facade reaches its
     * terminal clean-closed state (rather than pump-exiting). */
    int        initiate_goaway;
    int        goaway_sent;
    int        goaway_rc;   /* moq_session_goaway() result (close mode) */
} client_app_t;

static int client_pump(moq_pico_wt_managed_t *m, uint64_t now, void *ctx)
{
    client_app_t *a = (client_app_t *)ctx;
    moq_session_t *s = moq_pico_wt_managed_session(m);
    if (!s) return 0;
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SETUP_COMPLETE && !a->subscribed) {
            moq_subscribe_cfg_t sc;
            moq_subscribe_cfg_init(&sc);
            static const moq_bytes_t ns[] = {
                {(const uint8_t *)"pico", 4}, {(const uint8_t *)"wt", 2}
            };
            sc.track_namespace.parts = ns;
            sc.track_namespace.count = 2;
            sc.track_name = (moq_bytes_t){(const uint8_t *)"video", 5};
            sc.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
            moq_subscription_t sub;
            if (moq_session_subscribe(s, &sc, 0, &sub) >= 0)
                a->subscribed = 1;
        } else if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) {
            if (a->expect_big > 0) {
                /* Verify each object's length and pattern; only signal
                 * done once BOTH the control and the large object land. */
                const moq_object_received_event_t *o =
                    &ev.u.object_received;
                const uint8_t *p = o->payload ? moq_rcbuf_data(o->payload)
                                              : NULL;
                size_t plen = o->payload ? moq_rcbuf_len(o->payload) : 0;
                if (o->object_id == 0) {
                    if (plen == BIGOBJ_SMALL_LEN && p &&
                        memcmp(p, BIGOBJ_SMALL, BIGOBJ_SMALL_LEN) == 0)
                        atomic_store(&a->small_ok, 1);
                    else
                        atomic_store(&a->big_bad, 1);
                } else if (o->object_id == 1) {
                    int ok = (plen == a->expect_big) && p;
                    for (size_t i = 0; ok && i < plen; i++)
                        if (p[i] != bigobj_byte(i)) ok = 0;
                    if (ok) atomic_store(&a->big_ok, 1);
                    else    atomic_store(&a->big_bad, 1);
                }
                if (atomic_load(&a->small_ok) && atomic_load(&a->big_ok))
                    atomic_store(&a->got_object, 1);
                moq_event_cleanup(&ev);
                continue;
            }
            atomic_store(&a->got_object, 1);
            if (a->initiate_goaway && !a->goaway_sent) {
                /* Client GOAWAY (uri_len 0) → DRAINING → drain timer →
                 * CLOSE_SESSION → clean local close. */
                a->goaway_rc = (int)moq_session_goaway(s, NULL, 0, now);
                a->goaway_sent = 1;
            }
        }
        moq_event_cleanup(&ev);
    }
    if (a->initiate_goaway)
        return 0;  /* keep running to reach the terminal closed state */
    return atomic_load(&a->got_object) ? 1 : 0;  /* else pump-exit */
}

/* -- helpers -------------------------------------------------------- */

static moq_pico_wt_managed_t *make_server_ex(const char *cert, const char *key,
                                             int port, server_app_t *app,
                                             const char *wt_protocols)
{
    moq_pico_wt_managed_cfg_t cfg;
    moq_pico_wt_managed_cfg_init(&cfg);
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.cert_path = cert;
    cfg.key_path = key;
    cfg.port = port;
    cfg.send_request_capacity = true;   /* grant clients credit */
    cfg.initial_request_capacity = 16;
    cfg.on_pump = server_pump;
    cfg.on_pump_ctx = app;
    cfg.wt_protocols = wt_protocols;
    moq_pico_wt_managed_t *srv = NULL;
    if (moq_pico_wt_managed_create(&cfg, &srv) != MOQ_OK)
        return NULL;
    return srv;
}

static moq_pico_wt_managed_t *make_server(const char *cert, const char *key,
                                          int port, server_app_t *app)
{
    return make_server_ex(cert, key, port, app, NULL);
}

/* configure_quic hook: model a relay (e.g. moqx) that negotiates
 * WebTransport but does NOT advertise the reset_stream_at transport parameter.
 * picowt_set_default_transport_parameters has already enabled it on the default
 * TP; clear it so this server omits it on the wire. */
static int server_drop_reset_stream_at(picoquic_quic_t *quic, void *ctx)
{
    (void)ctx;
    picoquic_tp_t const *tp = picoquic_get_default_tp(quic);
    if (tp) ((picoquic_tp_t *)tp)->is_reset_stream_at_enabled = 0;
    return 0;
}

/* A configure_quic hook captures the advertised inbound per-uni-stream
 * flow-control window the adapter set on the default TPs (the hook runs after
 * the adapter applies its receive credit), so a test can assert the receiver
 * advertises enough inbound credit for large objects. Optional, installed
 * per-create via the pointer below. */
static uint64_t g_observed_uni_window;
static int (*g_client_configure_quic)(picoquic_quic_t *, void *);

static int capture_recv_window(picoquic_quic_t *quic, void *ctx)
{
    (void)ctx;
    picoquic_tp_t const *tp = picoquic_get_default_tp(quic);
    if (tp) g_observed_uni_window = tp->initial_max_stream_data_uni;
    return 0;
}

/* configure_quic hook: force the receiver's advertised inbound per-uni-stream
 * window back down to picoquic's ~64 KiB default, overriding the adapter's large
 * receive credit. A 256 KiB object then cannot be sent in one window: the
 * publisher must resume sending each time the peer extends credit with
 * MAX_STREAM_DATA. Used by the bigobj_smallwin mode to prove send resumption
 * works across many flow-control windows (independent of teardown timing). */
static int force_small_recv_window(picoquic_quic_t *quic, void *ctx)
{
    (void)ctx;
    picoquic_tp_t const *tp = picoquic_get_default_tp(quic);
    if (tp) {
        ((picoquic_tp_t *)tp)->initial_max_stream_data_uni = 65535;
        g_observed_uni_window = 65535;
    }
    return 0;
}

static moq_pico_wt_managed_t *make_server_no_rsa(const char *cert,
                                                 const char *key,
                                                 int port, server_app_t *app)
{
    moq_pico_wt_managed_cfg_t cfg;
    moq_pico_wt_managed_cfg_init(&cfg);
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.cert_path = cert;
    cfg.key_path = key;
    cfg.port = port;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 16;
    cfg.on_pump = server_pump;
    cfg.on_pump_ctx = app;
    cfg.configure_quic = server_drop_reset_stream_at;
    moq_pico_wt_managed_t *srv = NULL;
    if (moq_pico_wt_managed_create(&cfg, &srv) != MOQ_OK)
        return NULL;
    return srv;
}

static moq_pico_wt_managed_t *make_client_ex2(int port, client_app_t *app,
                                              uint64_t goaway_timeout_us,
                                              const char *wt_protocols)
{
    moq_pico_wt_managed_cfg_t cfg;
    moq_pico_wt_managed_cfg_init(&cfg);
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = "127.0.0.1";
    cfg.port = port;
    cfg.insecure_skip_verify = true;
    cfg.goaway_timeout_us = goaway_timeout_us;
    cfg.on_pump = client_pump;
    cfg.on_pump_ctx = app;
    cfg.wt_protocols = wt_protocols;
    if (g_client_configure_quic)
        cfg.configure_quic = g_client_configure_quic;
    moq_pico_wt_managed_t *cli = NULL;
    if (moq_pico_wt_managed_create(&cfg, &cli) != MOQ_OK)
        return NULL;
    return cli;
}

static moq_pico_wt_managed_t *make_client_ex(int port, client_app_t *app,
                                             uint64_t goaway_timeout_us)
{
    return make_client_ex2(port, app, goaway_timeout_us, NULL);
}

static moq_pico_wt_managed_t *make_client(int port, client_app_t *app)
{
    return make_client_ex(port, app, 0);
}

/* Wait up to budget for the client's object. */
static void wait_for_object(moq_pico_wt_managed_t *cli, client_app_t *app,
                            int timeout_sec)
{
    uint64_t waited = 0, budget = (uint64_t)timeout_sec * 1000;
    while (!atomic_load(&app->got_object) && waited < budget) {
        if (moq_pico_wt_managed_wait(cli, 200000) == MOQ_ERR_CLOSED)
            break;
        waited += 200;
    }
}

/*
 * Wait bounded by real wall-clock. Unlike wait_for_object (which counts
 * iterations assuming each wait() blocks its full timeout), a sustained
 * large-object transfer makes wait() return early many times, so an
 * iteration count under-budgets it and gives up mid-transfer. A transfer
 * that genuinely never completes still terminates at the deadline because
 * wait() blocks ~200ms per idle call.
 */
static void wait_realtime(moq_pico_wt_managed_t *cli, client_app_t *app,
                          int timeout_sec)
{
    struct timespec t0;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    while (!atomic_load(&app->got_object)) {
        if (moq_pico_wt_managed_wait(cli, 200000) == MOQ_ERR_CLOSED)
            break;
        struct timespec t1;
        clock_gettime(CLOCK_MONOTONIC, &t1);
        if ((t1.tv_sec - t0.tv_sec) >= timeout_sec)
            break;
    }
}

/* -- scenarios ------------------------------------------------------ */

static run_status_t run_loopback(const char *cert, const char *key,
                                 int port, int timeout_sec)
{
    server_app_t sapp; memset(&sapp, 0, sizeof(sapp));
    moq_pico_wt_managed_t *srv = make_server(cert, key, port, &sapp);
    if (!srv) return RUN_RETRY;            /* bind/create — retry */
    for (int i = 0; i < 3; i++) moq_pico_wt_managed_wait(srv, 100000);

    client_app_t capp; memset(&capp, 0, sizeof(capp));
    atomic_init(&capp.got_object, 0);
    moq_pico_wt_managed_t *cli =
        make_client(moq_pico_wt_managed_local_port(srv), &capp);
    if (!cli) {
        moq_pico_wt_managed_stop(srv);
        moq_pico_wt_managed_destroy(srv);
        return RUN_RETRY;
    }

    wait_for_object(cli, &capp, timeout_sec);

    int got = atomic_load(&capp.got_object);
    int fatal = moq_pico_wt_managed_is_fatal(cli) ||
                moq_pico_wt_managed_is_fatal(srv);
    int closed = moq_pico_wt_managed_is_closed(cli);

    if (got) {
        /* Pump-exit terminal contract: client on_pump returned nonzero
         * after the object, so wait() is CLOSED and the state is neither
         * fatal nor a clean session close (distinct from run_close). */
        CHECK(moq_pico_wt_managed_wait(cli, 1000000) == MOQ_ERR_CLOSED);
        CHECK(!moq_pico_wt_managed_is_fatal(cli));
        CHECK(!moq_pico_wt_managed_is_closed(cli));
    }

    moq_pico_wt_managed_stop(cli);     /* join → capp.subscribed safe */
    int reached_setup = capp.subscribed;
    moq_pico_wt_managed_destroy(cli);
    moq_pico_wt_managed_stop(srv);
    moq_pico_wt_managed_destroy(srv);

    if (got) return RUN_OK;
    if (reached_setup || fatal) {
        fprintf(stderr, "[loopback] port %d HARD FAIL: "
                "setup_reached=%d fatal=%d closed=%d got=0\n",
                port, reached_setup, fatal, closed);
        return RUN_HARD;                  /* real handshake/object failure */
    }
    fprintf(stderr, "[loopback] port %d: no setup reached "
            "(retryable; port may be busy)\n", port);
    return RUN_RETRY;
}

/* Large-object regression: the server publishes a small control object and
 * a 256 KiB object on one subgroup stream; the client must receive BOTH in
 * full (exact length + pattern). The transfer spans many packets/RTTs, so
 * it waits by real elapsed time (wait_realtime): the iteration-count wait
 * under-counts an active transfer (wait() returns early) and would give up
 * mid-transfer, falsely reporting non-delivery. */
static run_status_t run_bigobj(const char *cert, const char *key,
                               int port, int timeout_sec, int small_window)
{
    server_app_t sapp; memset(&sapp, 0, sizeof(sapp));
    sapp.big_size = BIGOBJ_BIG_SIZE;
    moq_pico_wt_managed_t *srv = make_server(cert, key, port, &sapp);
    if (!srv) return RUN_RETRY;
    for (int i = 0; i < 3; i++) moq_pico_wt_managed_wait(srv, 100000);

    client_app_t capp; memset(&capp, 0, sizeof(capp));
    atomic_init(&capp.got_object, 0);
    atomic_init(&capp.small_ok, 0);
    atomic_init(&capp.big_ok, 0);
    atomic_init(&capp.big_bad, 0);
    capp.expect_big = BIGOBJ_BIG_SIZE;
    /* Default mode: the receiver must advertise enough inbound per-uni-stream
     * credit to admit a large object in full, well above picoquic's ~64 KiB
     * default; the advertised credit is asserted directly via the configure_quic
     * hook. small_window mode instead forces the window back to ~64 KiB, so the
     * 256 KiB object spans many flow-control windows and only completes if the
     * publisher resumes sending after each peer MAX_STREAM_DATA. */
    g_observed_uni_window = 0;
    g_client_configure_quic =
        small_window ? force_small_recv_window : capture_recv_window;
    moq_pico_wt_managed_t *cli =
        make_client(moq_pico_wt_managed_local_port(srv), &capp);
    g_client_configure_quic = NULL;
    if (!cli) {
        moq_pico_wt_managed_stop(srv);
        moq_pico_wt_managed_destroy(srv);
        return RUN_RETRY;
    }
    if (small_window)
        CHECK(g_observed_uni_window == 65535);   /* window was actually shrunk */
    else
        CHECK(g_observed_uni_window >= (1u << 20));   /* large-object credit */

    wait_realtime(cli, &capp, timeout_sec);

    int got = atomic_load(&capp.got_object);
    int small_ok = atomic_load(&capp.small_ok);
    int big_ok = atomic_load(&capp.big_ok);
    int big_bad = atomic_load(&capp.big_bad);
    int fatal = moq_pico_wt_managed_is_fatal(cli) ||
                moq_pico_wt_managed_is_fatal(srv);

    moq_pico_wt_managed_stop(cli);
    int reached_setup = capp.subscribed;
    moq_pico_wt_managed_destroy(cli);
    moq_pico_wt_managed_stop(srv);
    moq_pico_wt_managed_destroy(srv);

    if (got) {
        CHECK(small_ok);    /* small control object intact */
        CHECK(big_ok);      /* full 256 KiB object intact, in order */
        CHECK(!big_bad);    /* no length/content mismatch */
        return RUN_OK;
    }
    if (reached_setup || fatal) {
        fprintf(stderr, "[bigobj] port %d HARD FAIL: setup=%d fatal=%d "
                "small_ok=%d big_ok=%d big_bad=%d got=0\n",
                port, reached_setup, fatal, small_ok, big_ok, big_bad);
        return RUN_HARD;
    }
    fprintf(stderr, "[bigobj] port %d: no setup reached (retryable)\n", port);
    return RUN_RETRY;
}

/* Graceful-drain (backs moq_pico_wt_managed_drain_state / moq_endpoint_drain):
 * publish a 256 KiB object under a forced ~64 KiB receive window so it spans
 * many flow-control windows, then -- from the app thread, mimicking a publisher
 * that has finished writing -- poll the drain predicate until the transport
 * reports its outbound flushed, and prove the object delivered in full at that
 * point. Without drain, a publisher that stopped right after queueing would
 * truncate this (see the hold ladder); here drain_state going 1 is the signal
 * that stopping is safe. */
static run_status_t run_bigobj_drain(const char *cert, const char *key,
                                     int port, int timeout_sec)
{
    server_app_t sapp; memset(&sapp, 0, sizeof(sapp));
    sapp.big_size = BIGOBJ_BIG_SIZE;
    moq_pico_wt_managed_t *srv = make_server(cert, key, port, &sapp);
    if (!srv) return RUN_RETRY;
    for (int i = 0; i < 3; i++) moq_pico_wt_managed_wait(srv, 100000);

    client_app_t capp; memset(&capp, 0, sizeof(capp));
    atomic_init(&capp.got_object, 0);
    atomic_init(&capp.small_ok, 0);
    atomic_init(&capp.big_ok, 0);
    atomic_init(&capp.big_bad, 0);
    capp.expect_big = BIGOBJ_BIG_SIZE;
    g_observed_uni_window = 0;
    g_client_configure_quic = force_small_recv_window;
    moq_pico_wt_managed_t *cli =
        make_client(moq_pico_wt_managed_local_port(srv), &capp);
    g_client_configure_quic = NULL;
    if (!cli) {
        moq_pico_wt_managed_stop(srv);
        moq_pico_wt_managed_destroy(srv);
        return RUN_RETRY;
    }
    CHECK(g_observed_uni_window == 65535);   /* window actually constrained */

    /* App-thread drain: once the object is queued, poll the predicate in real
     * time until the publisher's transport reports flushed. */
    struct timespec t0; clock_gettime(CLOCK_MONOTONIC, &t0);
    int drained = 0, published = 0;
    for (;;) {
        moq_pico_wt_managed_wait(cli, 20000);   /* advance the receiver ~20ms */
        if (!published) published = atomic_load(&sapp.published);   /* object queued (net thr) */
        if (published) {
            moq_pico_wt_managed_wake(srv);       /* kick the publisher loop to flush */
            int ds = moq_pico_wt_managed_drain_state(srv);
            CHECK(ds != -2);                    /* not on the network thread */
            if (ds == 1) { drained = 1; break; }
        }
        struct timespec tn; clock_gettime(CLOCK_MONOTONIC, &tn);
        double el = (double)(tn.tv_sec - t0.tv_sec) +
                    (double)(tn.tv_nsec - t0.tv_nsec) / 1e9;
        if (el > (double)timeout_sec) break;
    }

    /* drain_state==1 means the publisher's local stream backlog is flushed (all
     * reliable bytes + FIN handed to the transport); let the receiver's loop
     * surface the object event, then verify it arrived in full. */
    if (drained) wait_realtime(cli, &capp, 2);

    int got = atomic_load(&capp.got_object);
    int small_ok = atomic_load(&capp.small_ok);
    int big_ok = atomic_load(&capp.big_ok);
    int big_bad = atomic_load(&capp.big_bad);
    int reached_setup = capp.subscribed;
    int fatal = moq_pico_wt_managed_is_fatal(cli) ||
                moq_pico_wt_managed_is_fatal(srv);

    moq_pico_wt_managed_stop(cli);
    moq_pico_wt_managed_destroy(cli);
    moq_pico_wt_managed_stop(srv);
    moq_pico_wt_managed_destroy(srv);

    if (drained && got) {
        CHECK(small_ok);
        CHECK(big_ok);     /* full 256 KiB delivered once drain reported flushed */
        CHECK(!big_bad);
        return RUN_OK;
    }
    if (reached_setup || fatal) {
        fprintf(stderr, "[bigobj_drain] port %d HARD FAIL: setup=%d fatal=%d "
                "drained=%d got=%d small_ok=%d big_ok=%d big_bad=%d\n",
                port, reached_setup, fatal, drained, got, small_ok, big_ok,
                big_bad);
        return RUN_HARD;
    }
    fprintf(stderr, "[bigobj_drain] port %d: no setup reached (retryable)\n", port);
    return RUN_RETRY;
}

/* Regression: the server negotiates WebTransport but OMITS the
 * reset_stream_at transport parameter (modelling moqx). picowt gates sending the
 * WT CONNECT on the peer advertising reset_stream_at; the DEFAULT client (no
 * allow-missing knob) must tolerate the missing TP, so the WT session still
 * establishes and an object is delivered -- exactly as the plain loopback, with
 * NO opt-in flag. (Otherwise CONNECT is never sent and the connection
 * idle-closes with no MoQ traffic.) */
static run_status_t run_no_reset_stream_at(const char *cert, const char *key,
                                           int port, int timeout_sec)
{
    server_app_t sapp; memset(&sapp, 0, sizeof(sapp));
    moq_pico_wt_managed_t *srv = make_server_no_rsa(cert, key, port, &sapp);
    if (!srv) {
        /* server create / network-thread bind death — environment, retryable. */
        fprintf(stderr, "[no_rsa] port %d: server create/bind FAILED (retryable)\n", port);
        return RUN_RETRY;
    }
    for (int i = 0; i < 3; i++) moq_pico_wt_managed_wait(srv, 100000);

    client_app_t capp; memset(&capp, 0, sizeof(capp));
    atomic_init(&capp.got_object, 0);
    /* DEFAULT client — the reset_stream_at tolerance is always on (self-gating),
     * so no configuration is needed to interop with a peer that omits it. */
    moq_pico_wt_managed_t *cli =
        make_client(moq_pico_wt_managed_local_port(srv), &capp);
    if (!cli) {
        fprintf(stderr, "[no_rsa] port %d: client create FAILED (retryable)\n", port);
        moq_pico_wt_managed_stop(srv);
        moq_pico_wt_managed_destroy(srv);
        return RUN_RETRY;
    }

    wait_for_object(cli, &capp, timeout_sec);
    int got = atomic_load(&capp.got_object);
    int fatal = moq_pico_wt_managed_is_fatal(cli) ||
                moq_pico_wt_managed_is_fatal(srv);
    int closed = moq_pico_wt_managed_is_closed(cli);

    moq_pico_wt_managed_stop(cli);
    int reached_setup = capp.subscribed;
    moq_pico_wt_managed_destroy(cli);
    moq_pico_wt_managed_stop(srv);
    moq_pico_wt_managed_destroy(srv);

    /* Diagnostics distinguish the failure sub-state: a logic regression (the
     * default client failing to establish against a peer omitting
     * reset_stream_at) reaches setup or goes fatal -> HARD; an environment-only
     * failure (network-thread bind, busy port) never reaches setup -> RETRY. */
    if (got) return RUN_OK;            /* WT established despite missing TP */
    if (reached_setup || fatal) {
        fprintf(stderr, "[no_rsa] port %d HARD FAIL: setup_reached=%d fatal=%d "
                "closed=%d got=0\n", port, reached_setup, fatal, closed);
        return RUN_HARD;
    }
    fprintf(stderr, "[no_rsa] port %d: no setup reached "
            "(retryable; port may be busy / bind failed)\n", port);
    return RUN_RETRY;
}

/* Negotiated loopback: client offers {18, 16}; the d16-only server selects
 * moqt-16; the draft-16 session establishes and media flows as in the plain
 * loopback; both facades report negotiated version 16. */
static run_status_t run_nego(const char *cert, const char *key,
                             int port, int timeout_sec)
{
    server_app_t sapp; memset(&sapp, 0, sizeof(sapp));
    moq_pico_wt_managed_t *srv =
        make_server_ex(cert, key, port, &sapp, "moqt-16");
    if (!srv) return RUN_RETRY;
    for (int i = 0; i < 3; i++) moq_pico_wt_managed_wait(srv, 100000);

    client_app_t capp; memset(&capp, 0, sizeof(capp));
    atomic_init(&capp.got_object, 0);
    moq_pico_wt_managed_t *cli = make_client_ex2(
        moq_pico_wt_managed_local_port(srv), &capp, 0, "moqt-18, moqt-16");
    if (!cli) {
        moq_pico_wt_managed_stop(srv);
        moq_pico_wt_managed_destroy(srv);
        return RUN_RETRY;
    }

    wait_for_object(cli, &capp, timeout_sec);
    int got = atomic_load(&capp.got_object);
    CHECK(got);
    CHECK(!moq_pico_wt_managed_is_fatal(cli));
    CHECK(!moq_pico_wt_managed_is_fatal(srv));
    CHECK(moq_pico_wt_managed_negotiated_version(cli) == MOQ_VERSION_DRAFT_16);
    CHECK(moq_pico_wt_managed_negotiated_version(srv) == MOQ_VERSION_DRAFT_16);

    moq_pico_wt_managed_stop(cli);
    moq_pico_wt_managed_destroy(cli);
    moq_pico_wt_managed_stop(srv);
    moq_pico_wt_managed_destroy(srv);
    return got ? RUN_OK : RUN_HARD;
}

/* No-overlap negotiation: the client offers only moqt-18 against a
 * moqt-16-only server; the server refuses the CONNECT (explicit non-2xx) and
 * the client observes a deterministic terminal fatal -- never a silent
 * draft-16 session. */
static run_status_t run_nego_refuse(const char *cert, const char *key,
                                    int port, int timeout_sec)
{
    server_app_t sapp; memset(&sapp, 0, sizeof(sapp));
    moq_pico_wt_managed_t *srv =
        make_server_ex(cert, key, port, &sapp, "moqt-16");
    if (!srv) return RUN_RETRY;
    for (int i = 0; i < 3; i++) moq_pico_wt_managed_wait(srv, 100000);

    client_app_t capp; memset(&capp, 0, sizeof(capp));
    atomic_init(&capp.got_object, 0);
    moq_pico_wt_managed_t *cli = make_client_ex2(
        moq_pico_wt_managed_local_port(srv), &capp, 0, "moqt-18");
    if (!cli) {
        moq_pico_wt_managed_stop(srv);
        moq_pico_wt_managed_destroy(srv);
        return RUN_RETRY;
    }

    uint64_t waited = 0, budget = (uint64_t)timeout_sec * 1000;
    while (!moq_pico_wt_managed_is_fatal(cli) && waited < budget) {
        if (moq_pico_wt_managed_wait(cli, 200000) == MOQ_ERR_CLOSED)
            break;
        waited += 200;
    }
    int fatal = moq_pico_wt_managed_is_fatal(cli);
    CHECK(fatal);
    CHECK(!atomic_load(&capp.got_object));
    CHECK(moq_pico_wt_managed_negotiated_version(cli) == 0);

    moq_pico_wt_managed_stop(cli);
    moq_pico_wt_managed_destroy(cli);
    moq_pico_wt_managed_stop(srv);
    moq_pico_wt_managed_destroy(srv);
    return fatal ? RUN_OK : RUN_HARD;
}

/* Legacy no-subprotocol fallback: the server advertises no WT subprotocols,
 * so it accepts the WebTransport CONNECT but echoes NO WT-Protocol (the way
 * moxygen-based relays behave). The client offered {18, 16}; with no
 * transport-selected token it falls back to draft-16 (which it offered),
 * the session establishes, media flows, and both report version 16. */
static run_status_t run_nego_legacy(const char *cert, const char *key,
                                    int port, int timeout_sec)
{
    server_app_t sapp; memset(&sapp, 0, sizeof(sapp));
    moq_pico_wt_managed_t *srv =
        make_server_ex(cert, key, port, &sapp, NULL);   /* no WT subprotocols */
    if (!srv) return RUN_RETRY;
    for (int i = 0; i < 3; i++) moq_pico_wt_managed_wait(srv, 100000);

    client_app_t capp; memset(&capp, 0, sizeof(capp));
    atomic_init(&capp.got_object, 0);
    moq_pico_wt_managed_t *cli = make_client_ex2(
        moq_pico_wt_managed_local_port(srv), &capp, 0, "moqt-18, moqt-16");
    if (!cli) {
        moq_pico_wt_managed_stop(srv);
        moq_pico_wt_managed_destroy(srv);
        return RUN_RETRY;
    }

    wait_for_object(cli, &capp, timeout_sec);
    int got = atomic_load(&capp.got_object);
    CHECK(got);
    CHECK(!moq_pico_wt_managed_is_fatal(cli));
    CHECK(!moq_pico_wt_managed_is_fatal(srv));
    CHECK(moq_pico_wt_managed_negotiated_version(cli) == MOQ_VERSION_DRAFT_16);
    CHECK(moq_pico_wt_managed_negotiated_version(srv) == MOQ_VERSION_DRAFT_16);

    moq_pico_wt_managed_stop(cli);
    moq_pico_wt_managed_destroy(cli);
    moq_pico_wt_managed_stop(srv);
    moq_pico_wt_managed_destroy(srv);
    return got ? RUN_OK : RUN_HARD;
}

/* Legacy fallback guard: the server echoes no WT-Protocol, but the client
 * offered ONLY draft-18 (EXACT 18). With no transport-selected token AND no
 * draft-16 in the offer, the client must NOT silently downgrade to draft-16
 * -- it stays terminal fatal (the explicit-version contract is preserved). */
static run_status_t run_nego_legacy_18(const char *cert, const char *key,
                                       int port, int timeout_sec)
{
    server_app_t sapp; memset(&sapp, 0, sizeof(sapp));
    moq_pico_wt_managed_t *srv =
        make_server_ex(cert, key, port, &sapp, NULL);
    if (!srv) return RUN_RETRY;
    for (int i = 0; i < 3; i++) moq_pico_wt_managed_wait(srv, 100000);

    client_app_t capp; memset(&capp, 0, sizeof(capp));
    atomic_init(&capp.got_object, 0);
    moq_pico_wt_managed_t *cli = make_client_ex2(
        moq_pico_wt_managed_local_port(srv), &capp, 0, "moqt-18");
    if (!cli) {
        moq_pico_wt_managed_stop(srv);
        moq_pico_wt_managed_destroy(srv);
        return RUN_RETRY;
    }

    uint64_t waited = 0, budget = (uint64_t)timeout_sec * 1000;
    while (!moq_pico_wt_managed_is_fatal(cli) && waited < budget) {
        if (moq_pico_wt_managed_wait(cli, 200000) == MOQ_ERR_CLOSED)
            break;
        waited += 200;
    }
    int fatal = moq_pico_wt_managed_is_fatal(cli);
    CHECK(fatal);
    CHECK(!atomic_load(&capp.got_object));
    CHECK(moq_pico_wt_managed_negotiated_version(cli) == 0);

    moq_pico_wt_managed_stop(cli);
    moq_pico_wt_managed_destroy(cli);
    moq_pico_wt_managed_stop(srv);
    moq_pico_wt_managed_destroy(srv);
    return fatal ? RUN_OK : RUN_HARD;
}

static run_status_t run_refuse(const char *cert, const char *key,
                               int port, int timeout_sec)
{
    server_app_t sapp; memset(&sapp, 0, sizeof(sapp));
    moq_pico_wt_managed_t *srv = make_server(cert, key, port, &sapp);
    if (!srv) return RUN_RETRY;
    for (int i = 0; i < 3; i++) moq_pico_wt_managed_wait(srv, 100000);
    int p = moq_pico_wt_managed_local_port(srv);

    /* First client: should be served. */
    client_app_t c1; memset(&c1, 0, sizeof(c1));
    atomic_init(&c1.got_object, 0);
    moq_pico_wt_managed_t *cli1 = make_client(p, &c1);
    if (!cli1) {
        moq_pico_wt_managed_stop(srv); moq_pico_wt_managed_destroy(srv);
        return RUN_RETRY;
    }
    wait_for_object(cli1, &c1, timeout_sec);
    int g1 = atomic_load(&c1.got_object);

    /* First client must be served before the contract is even
     * exercisable. If it never reached setup, the port was likely busy
     * (retryable); if it reached setup or went fatal without an object,
     * that is a real failure. */
    if (!g1) {
        int c1_fatal = moq_pico_wt_managed_is_fatal(cli1);
        moq_pico_wt_managed_stop(cli1);
        int c1_setup = c1.subscribed;
        moq_pico_wt_managed_destroy(cli1);
        moq_pico_wt_managed_stop(srv);
        moq_pico_wt_managed_destroy(srv);
        if (c1_setup || c1_fatal) {
            fprintf(stderr, "[refuse] port %d HARD FAIL: first client "
                    "never received (setup=%d fatal=%d)\n",
                    port, c1_setup, c1_fatal);
            return RUN_HARD;
        }
        fprintf(stderr, "[refuse] port %d: first client no setup "
                "(retryable; port may be busy)\n", port);
        return RUN_RETRY;
    }

    /* Second client, now that the server holds one connection: its WT
     * CONNECT must be refused deterministically. The server returns -1
     * from its WT path callback, which h3zero answers with an HTTP 501;
     * the client maps the non-2xx status to connect_refused and the
     * managed facade latches fatal. So the second client must reach a
     * terminal FATAL state in bounded time, never reach setup, never
     * receive an object, and never expose a session. */
    client_app_t c2; memset(&c2, 0, sizeof(c2));
    atomic_init(&c2.got_object, 0);
    moq_pico_wt_managed_t *cli2 = make_client(p, &c2);
    int c2_created = (cli2 != NULL);
    int g2 = 0, c2_setup = 0, c2_fatal = 0, c2_session = 0;
    moq_result_t c2_wait = MOQ_OK;
    if (cli2) {
        /* Wait for the terminal (fatal), not for an object that will
         * never come. ~5s cap is generous; refusal lands sub-second. */
        for (int tries = 0; tries < 50; tries++) {
            c2_wait = moq_pico_wt_managed_wait(cli2, 100000);
            if (c2_wait == MOQ_ERR_CLOSED ||
                moq_pico_wt_managed_is_fatal(cli2))
                break;
        }
        g2 = atomic_load(&c2.got_object);
        c2_fatal = moq_pico_wt_managed_is_fatal(cli2);
        c2_session = (moq_pico_wt_managed_session(cli2) != NULL);
        moq_pico_wt_managed_stop(cli2);
        c2_setup = c2.subscribed;
        moq_pico_wt_managed_destroy(cli2);
    }

    moq_pico_wt_managed_stop(cli1);
    moq_pico_wt_managed_destroy(cli1);
    moq_pico_wt_managed_stop(srv);
    moq_pico_wt_managed_destroy(srv);

    /* Contract: first served; second created, refused, and terminal-fatal
     * with no setup, no object, and no session exposed. */
    if (!c2_created) {
        fprintf(stderr, "[refuse] port %d HARD FAIL: second client "
                "create failed after first was served\n", port);
        return RUN_HARD;
    }
    if (g2 || c2_setup) {
        fprintf(stderr, "[refuse] port %d HARD FAIL: second client not "
                "refused (got_object=%d setup=%d fatal=%d)\n",
                port, g2, c2_setup, c2_fatal);
        return RUN_HARD;
    }
    if (!c2_fatal || c2_wait != MOQ_ERR_CLOSED || c2_session) {
        fprintf(stderr, "[refuse] port %d HARD FAIL: refusal not terminal "
                "(fatal=%d wait=%d session=%d)\n",
                port, c2_fatal, (int)c2_wait, c2_session);
        return RUN_HARD;
    }
    return RUN_OK;
}

/* Clean-close: the client subscribes, receives the object, then sends
 * GOAWAY. After the drain timer fires the session emits CLOSE_SESSION
 * and the facade reaches its terminal clean-closed state. We assert on
 * the client's OWN local close (deterministic; independent of whether
 * the outbound CLOSE_SESSION reaches the server):
 *   wait() → MOQ_ERR_CLOSED, is_closed()==true, is_fatal()==false,
 *   close_code()==MOQ_CLOSE_GOAWAY_TIMEOUT, and wake() also CLOSED. */
static run_status_t run_close(const char *cert, const char *key,
                              int port, int timeout_sec)
{
    server_app_t sapp; memset(&sapp, 0, sizeof(sapp));
    moq_pico_wt_managed_t *srv = make_server(cert, key, port, &sapp);
    if (!srv) return RUN_RETRY;
    for (int i = 0; i < 3; i++) moq_pico_wt_managed_wait(srv, 100000);

    client_app_t capp; memset(&capp, 0, sizeof(capp));
    atomic_init(&capp.got_object, 0);
    capp.initiate_goaway = 1;
    /* 100ms drain: prompt, but exercises the real drain-timer path. */
    moq_pico_wt_managed_t *cli =
        make_client_ex(moq_pico_wt_managed_local_port(srv), &capp, 100000);
    if (!cli) {
        moq_pico_wt_managed_stop(srv);
        moq_pico_wt_managed_destroy(srv);
        return RUN_RETRY;
    }

    /* Reach the object path first (also triggers the client GOAWAY). */
    wait_for_object(cli, &capp, timeout_sec);
    if (!atomic_load(&capp.got_object)) {
        int fatal = moq_pico_wt_managed_is_fatal(cli);
        moq_pico_wt_managed_stop(cli);
        int setup = capp.subscribed;
        moq_pico_wt_managed_destroy(cli);
        moq_pico_wt_managed_stop(srv);
        moq_pico_wt_managed_destroy(srv);
        if (setup || fatal) {
            fprintf(stderr, "[close] port %d HARD FAIL: no object "
                    "(setup=%d fatal=%d)\n", port, setup, fatal);
            return RUN_HARD;
        }
        fprintf(stderr, "[close] port %d: no setup (retryable)\n", port);
        return RUN_RETRY;
    }

    /* (a) Client: the drain → clean close makes wait() terminal. */
    moq_result_t wr = MOQ_DONE;
    for (uint64_t waited = 0; waited < 5000; waited += 200) {
        wr = moq_pico_wt_managed_wait(cli, 200000);
        if (wr == MOQ_ERR_CLOSED) break;
    }
    int is_closed = moq_pico_wt_managed_is_closed(cli);
    int is_fatal  = moq_pico_wt_managed_is_fatal(cli);
    uint64_t code = moq_pico_wt_managed_close_code(cli);
    moq_result_t wk = moq_pico_wt_managed_wake(cli);  /* terminal too */

    /* The network thread performs a bounded best-effort flush of the
     * queued CLOSE_SESSION capsule before its loop exits (loop_callback).
     * That happens autonomously on the network thread — wait() returning
     * MOQ_ERR_CLOSED above is NOT proof the flush completed or that the
     * peer observed the close, and this test does not assert either.
     *
     * Server-side we require only that the clean close never looks like
     * an error (never fatal). The inbound-capsule parse →
     * on_transport_close path and close-code propagation are proven
     * deterministically at the adapter layer (test_pico_wt_loopback:
     * test_goaway_close_not_fatal asserts the receiver reaches is_closed,
     * not fatal, with the propagated MOQ_CLOSE_GOAWAY_TIMEOUT). */
    int srv_fatal = moq_pico_wt_managed_is_fatal(srv);

    moq_pico_wt_managed_stop(cli);
    int goaway_rc = capp.goaway_rc;          /* network thread joined */
    moq_pico_wt_managed_destroy(cli);
    moq_pico_wt_managed_stop(srv);
    moq_pico_wt_managed_destroy(srv);

    if (wr != MOQ_ERR_CLOSED || !is_closed || srv_fatal) {
        fprintf(stderr, "[close] port %d HARD FAIL: clean close not "
                "complete (wait=%d cli_closed=%d cli_fatal=%d code=0x%llx "
                "srv_fatal=%d goaway_rc=%d)\n",
                port, (int)wr, is_closed, is_fatal,
                (unsigned long long)code, srv_fatal, goaway_rc);
        return RUN_HARD;
    }
    /* Client: clean local terminal close with the GOAWAY-drain code. */
    CHECK(goaway_rc == MOQ_OK);
    CHECK(wr == MOQ_ERR_CLOSED);
    CHECK(is_closed);
    CHECK(!is_fatal);
    CHECK(code == MOQ_CLOSE_GOAWAY_TIMEOUT);
    CHECK(wk == MOQ_ERR_CLOSED);
    /* Server never mistook the clean close for an error/reset. */
    CHECK(!srv_fatal);
    return RUN_OK;
}

int main(int argc, char **argv)
{
    const char *cert = NULL, *key = NULL, *mode = "loopback";
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--cert") && i + 1 < argc) cert = argv[++i];
        else if (!strcmp(argv[i], "--key") && i + 1 < argc) key = argv[++i];
        else if (!strcmp(argv[i], "--mode") && i + 1 < argc) mode = argv[++i];
    }
    if (!cert || !key) {
        fprintf(stderr, "usage: %s --cert <f> --key <f> "
                        "[--mode loopback|bigobj|refuse|close]\n", argv[0]);
        return 2;
    }
    int refuse = !strcmp(mode, "refuse");
    int close_mode = !strcmp(mode, "close");
    int nego = !strcmp(mode, "nego");
    int nego_refuse = !strcmp(mode, "nego_refuse");
    int nego_legacy = !strcmp(mode, "nego_legacy");
    int nego_legacy_18 = !strcmp(mode, "nego_legacy_18");
    int no_rsa = !strcmp(mode, "no_reset_stream_at");
    int bigobj = !strcmp(mode, "bigobj");
    int bigobj_smallwin = !strcmp(mode, "bigobj_smallwin");
    int bigobj_drain = !strcmp(mode, "bigobj_drain");
    if (!refuse && !close_mode && !nego && !nego_refuse && !nego_legacy &&
        !nego_legacy_18 && !no_rsa && !bigobj && !bigobj_smallwin &&
        !bigobj_drain && strcmp(mode, "loopback") != 0) {
        fprintf(stderr, "unknown --mode '%s' (expected loopback|refuse|close|"
                "nego|nego_refuse|nego_legacy|nego_legacy_18|"
                "no_reset_stream_at|bigobj|bigobj_smallwin|bigobj_drain)\n", mode);
        return 2;
    }

    /* Per-process pseudo-random high ports, to avoid the fixed-port bind
     * sensitivity that fails on constrained/busy hosts (the managed server binds
     * dual-stack; a contended v6 port can kill the network-thread bind). True
     * ephemeral (port 0) is not usable here: picoquic's packet-loop port-update
     * callback reports the loop's wakeup address, not the server's LISTENING
     * port, so local_port() cannot recover an OS-assigned listen port. Spreading
     * across a wide range per PID makes a collision very unlikely; a retryable
     * (no-setup) failure advances to the next candidate, a hard handshake
     * failure stops immediately. */
    unsigned pbase = 20000u + (unsigned)((uintptr_t)getpid() * 2654435761u) % 30000u;
    const int ports[] = { (int)pbase, (int)(pbase + 311),
                          (int)(pbase + 743), (int)(pbase + 1373) };
    run_status_t st = RUN_RETRY;
    for (size_t i = 0; i < sizeof(ports) / sizeof(ports[0]); i++) {
        st = refuse         ? run_refuse(cert, key, ports[i], 6)
           : close_mode     ? run_close(cert, key, ports[i], 6)
           : nego           ? run_nego(cert, key, ports[i], 6)
           : nego_refuse    ? run_nego_refuse(cert, key, ports[i], 6)
           : nego_legacy    ? run_nego_legacy(cert, key, ports[i], 6)
           : nego_legacy_18 ? run_nego_legacy_18(cert, key, ports[i], 6)
           : no_rsa         ? run_no_reset_stream_at(cert, key, ports[i], 6)
           : bigobj         ? run_bigobj(cert, key, ports[i], 10, 0)
           : bigobj_smallwin ? run_bigobj(cert, key, ports[i], 10, 1)
           : bigobj_drain   ? run_bigobj_drain(cert, key, ports[i], 30)
                            : run_loopback(cert, key, ports[i], 6);
        if (st != RUN_RETRY) break;
    }

    CHECK(st == RUN_OK);
    if (failures == 0)
        printf("test_pico_wt_managed_loopback (%s): PASS\n", mode);
    else
        fprintf(stderr, "test_pico_wt_managed_loopback (%s): %d failure(s)\n",
                mode, failures);
    return failures ? 1 : 0;
}
