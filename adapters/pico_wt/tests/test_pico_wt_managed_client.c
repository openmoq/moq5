/*
 * Managed pico WT client test: drives moq_pico_wt_managed_t (client)
 * against a running WebTransport server, subscribes, and asserts the
 * first object arrives. Exits 0 on success.
 *
 * Accepts the same args as the example client (--host/--port/--path/
 * --timeout-sec) and prints "object received" + the payload, so it can
 * be driven by run_smoke.sh against the example server.
 *
 * All session work happens on the network thread inside on_pump; the
 * main thread only create/wait/stop/destroys and reads a shared flag
 * published through the facade's wait() barrier.
 *
 * --early-wake-control runs, with no server, the wait loop against waits
 * that return at once: the timeout is monotonic time, not a count of
 * waits.
 */

#include <moq/moq.h>
#include <moq/pico_wt_managed.h>

#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

typedef struct {
    int        subscribed;   /* network thread only */
    atomic_int got_object;   /* set on network thread, read on main */
} app_t;

static void do_subscribe(moq_session_t *s, app_t *a)
{
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    static const moq_bytes_t ns[] = {
        {(const uint8_t *)"pico", 4},
        {(const uint8_t *)"wt", 2}
    };
    sc.track_namespace.parts = ns;
    sc.track_namespace.count = 2;
    sc.track_name = (moq_bytes_t){(const uint8_t *)"video", 5};
    sc.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
    moq_subscription_t sub;
    if (moq_session_subscribe(s, &sc, 0, &sub) >= 0) {
        a->subscribed = 1;
        printf("[managed-client] subscribe sent\n");
    }
}

/* Network thread. Drives the MoQ flow: subscribe on setup, capture the
 * first object. Returns nonzero once done to exit the loop cleanly. */
static int on_pump(moq_pico_wt_managed_t *m, uint64_t now, void *ctx)
{
    (void)now;
    app_t *a = (app_t *)ctx;
    moq_session_t *s = moq_pico_wt_managed_session(m);
    if (!s) return 0;

    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SETUP_COMPLETE && !a->subscribed) {
            printf("[managed-client] MoQ setup complete\n");
            do_subscribe(s, a);
        } else if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) {
            if (ev.u.object_received.payload) {
                const uint8_t *d =
                    moq_rcbuf_data(ev.u.object_received.payload);
                size_t l = moq_rcbuf_len(ev.u.object_received.payload);
                printf("[managed-client] object received (%zu bytes): %.*s\n",
                       l, (int)l, (const char *)d);
            } else {
                printf("[managed-client] object received (empty)\n");
            }
            atomic_store(&a->got_object, 1);
        }
        moq_event_cleanup(&ev);
    }
    return atomic_load(&a->got_object) ? 1 : 0;
}

static uint64_t mono_us(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000u + (uint64_t)ts.tv_nsec / 1000u;
}

typedef moq_result_t (*wait_fn)(void *ctx, uint64_t timeout_us);
typedef bool (*done_fn)(void *ctx);

/* Wait until done() holds or `budget_us` of monotonic time has passed. A
 * wait may return early (every pump cycle wakes it); each return costs only
 * the time it took. True when done() held; false on timeout or a closed
 * facade. */
static bool wait_until(uint64_t budget_us, wait_fn wait, done_fn done,
                       void *ctx)
{
    const uint64_t deadline = mono_us() + budget_us;
    for (;;) {
        if (done(ctx)) return true;
        const uint64_t now = mono_us();
        if (now >= deadline) return false;
        const uint64_t slice = deadline - now < 200000u ? deadline - now
                                                         : 200000u;
        if (wait(ctx, slice) == MOQ_ERR_CLOSED) return done(ctx);
    }
}

typedef struct {
    moq_pico_wt_managed_t *m;
    app_t *app;
} real_wait_t;

static moq_result_t real_wait(void *ctx, uint64_t timeout_us)
{
    return moq_pico_wt_managed_wait(((real_wait_t *)ctx)->m, timeout_us);
}

static bool real_done(void *ctx)
{
    return atomic_load(&((real_wait_t *)ctx)->app->got_object) != 0;
}

typedef struct {
    unsigned calls;
    unsigned done_after;   /* done() holds after this many waits */
    bool sleep;            /* the wait sleeps its slice instead of returning */
} fake_wait_t;

static moq_result_t fake_wait(void *ctx, uint64_t timeout_us)
{
    fake_wait_t *f = (fake_wait_t *)ctx;
    f->calls++;
    if (f->sleep) {
        struct timespec ts = {
            .tv_sec = (time_t)(timeout_us / 1000000u),
            .tv_nsec = (long)(timeout_us % 1000000u) * 1000L };
        nanosleep(&ts, NULL);
    }
    return MOQ_OK;
}

static bool fake_done(void *ctx)
{
    const fake_wait_t *f = (const fake_wait_t *)ctx;
    return f->done_after && f->calls >= f->done_after;
}

/* A hundred waits returning at once, then the object: inside a five-second
 * budget, which a 200 ms charge per wait would spend after 25. Then a wait
 * that sleeps and an object that never comes: the budget ends it, on time. */
static int early_wake_control(void)
{
    int failures = 0;
    fake_wait_t f = { .done_after = 100 };
    if (!wait_until(5000000u, fake_wait, fake_done, &f) || f.calls != 100) {
        fprintf(stderr, "FAIL: early wakes: done=%d after %u waits\n",
                fake_done(&f), f.calls);
        failures++;
    }
    fake_wait_t g = { .sleep = true };
    const uint64_t t0 = mono_us();
    const bool got = wait_until(300000u, fake_wait, fake_done, &g);
    const uint64_t took = mono_us() - t0;
    if (got || took < 300000u || took > 2000000u) {
        fprintf(stderr, "FAIL: timeout: done=%d after %llu us\n", got,
                (unsigned long long)took);
        failures++;
    }
    if (failures) return 1;
    printf("PASS: early wakes cost only their time (100 waits within the "
           "budget; a 300 ms budget ended after %llu us)\n",
           (unsigned long long)took);
    return 0;
}

static void usage(const char *prog)
{
    fprintf(stderr,
        "usage: %s [--host H] [--port N] [--path /moq] [--timeout-sec N]\n"
        "       %s --early-wake-control\n",
        prog, prog);
}

int main(int argc, char **argv)
{
    const char *host = "127.0.0.1";
    int port = 4433;
    const char *path = "/moq";
    int timeout_sec = 5;

    if (argc == 2 && !strcmp(argv[1], "--early-wake-control"))
        return early_wake_control();
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--host") && i + 1 < argc) host = argv[++i];
        else if (!strcmp(argv[i], "--port") && i + 1 < argc)
            port = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--path") && i + 1 < argc)
            path = argv[++i];
        else if (!strcmp(argv[i], "--timeout-sec") && i + 1 < argc)
            timeout_sec = atoi(argv[++i]);
        else { usage(argv[0]); return 2; }
    }

    app_t app;
    memset(&app, 0, sizeof(app));
    atomic_init(&app.got_object, 0);

    moq_pico_wt_managed_cfg_t cfg;
    moq_pico_wt_managed_cfg_init(&cfg);
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = host;
    cfg.port = port;
    cfg.path = path;
    cfg.insecure_skip_verify = true;   /* test cert */
    cfg.on_pump = on_pump;
    cfg.on_pump_ctx = &app;

    moq_pico_wt_managed_t *m = NULL;
    if (moq_pico_wt_managed_create(&cfg, &m) != MOQ_OK) {
        fprintf(stderr, "[managed-client] create failed\n");
        return 1;
    }
    printf("[managed-client] connecting to %s:%d path %s\n",
           host, port, path);

    /* Wait for on_pump to report the object (or fatal / timeout). */
    real_wait_t rw = { .m = m, .app = &app };
    (void)wait_until((uint64_t)timeout_sec * 1000000u, real_wait, real_done,
                     &rw);

    bool fatal = moq_pico_wt_managed_is_fatal(m);
    moq_pico_wt_managed_stop(m);
    moq_pico_wt_managed_destroy(m);

    if (atomic_load(&app.got_object)) {
        printf("[managed-client] done: object received\n");
        return 0;
    }
    fprintf(stderr, "[managed-client] done: no object (fatal=%d)\n", fatal);
    return 1;
}
