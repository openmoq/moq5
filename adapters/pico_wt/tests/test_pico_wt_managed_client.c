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
 */

#include <moq/moq.h>
#include <moq/pico_wt_managed.h>

#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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

static void usage(const char *prog)
{
    fprintf(stderr,
        "usage: %s [--host H] [--port N] [--path /moq] [--timeout-sec N]\n",
        prog);
}

int main(int argc, char **argv)
{
    const char *host = "127.0.0.1";
    int port = 4433;
    const char *path = "/moq";
    int timeout_sec = 5;

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
    uint64_t waited_ms = 0;
    const uint64_t budget_ms = (uint64_t)timeout_sec * 1000;
    while (!atomic_load(&app.got_object) && waited_ms < budget_ms) {
        moq_result_t r = moq_pico_wt_managed_wait(m, 200000);  /* 200ms */
        if (r == MOQ_ERR_CLOSED)
            break;  /* pump_exit (done) or fatal */
        waited_ms += 200;
    }

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
