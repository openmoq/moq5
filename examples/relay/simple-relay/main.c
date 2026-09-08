/* One lane, one SDK runtime, no application-side forwarding algorithm. */
#define _POSIX_C_SOURCE 200809L
#define _DARWIN_C_SOURCE 1
#define _DEFAULT_SOURCE 1
#include <moq/msquic_managed.h>
#include <moq/relay/moqr_shards.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdio.h>
#include <string.h>

static volatile sig_atomic_t interrupted;
static char attached_tag, retired_tag;

typedef struct {
    moqr_shards_t *relay;
    atomic_bool failed;
} app_t;

static void interrupt_handler(int sig)
{
    (void)sig;
    interrupted = 1;
}

static int pump(moq_msquic_managed_t *transport,
                moq_msquic_managed_lane_t *lane, uint64_t now_us, void *user)
{
    app_t *app = user;
    moqr_bind_t *bind = moqr_shards_bind(app->relay, 0);
    (void)transport;
    if (moq_msquic_lane_index(lane) != 0 || bind == NULL) {
        goto failed;
    }
    for (moq_msquic_managed_conn_t *c = moq_msquic_lane_next_conn(lane, NULL);
         c != NULL; c = moq_msquic_lane_next_conn(lane, c)) {
        moq_session_t *s = moq_msquic_managed_conn_session(c);
        if (s == NULL) {
            goto failed;
        }
        if (moq_msquic_managed_conn_user(c) == NULL) {
            if (moqr_bind_conn_open(bind, s,
                    moq_msquic_managed_conn_negotiated_version(c)) == MOQR_OK) {
                moq_msquic_managed_conn_set_user(c, &attached_tag);
            } else {
                moq_msquic_managed_conn_set_user(c, &retired_tag);
                moq_msquic_managed_conn_close(c, 0);
            }
        }
    }
    uint64_t wakes = 0;
    if (moqr_shards_step_shard(app->relay, 0, now_us, &wakes) != MOQR_OK) {
        goto failed;
    }
    for (moq_msquic_managed_conn_t *c = moq_msquic_lane_next_conn(lane, NULL);
         c != NULL; c = moq_msquic_lane_next_conn(lane, c)) {
        moq_session_t *s = moq_msquic_managed_conn_session(c);
        if (moq_msquic_managed_conn_user(c) == &attached_tag &&
            !moqr_bind_conn_is_open(bind, s)) {
            moq_msquic_managed_conn_set_user(c, &retired_tag);
        }
        if (moq_msquic_managed_conn_user(c) != &retired_tag) {
            continue;
        }
        /* Only detached sessions are ours to poll. The terminal must be
         * consumed before acknowledging; an unready child stays iterable. */
        moq_event_t events[16];
        size_t n;
        while ((n = moq_session_poll_events(s, events, 16)) != 0) {
            for (size_t i = 0; i < n; ++i) {
                moq_event_cleanup(&events[i]);
            }
        }
        moq_result_t rc = moq_msquic_managed_conn_ack_terminal(c);
        if (rc != MOQ_OK && rc != MOQ_ERR_WRONG_STATE) {
            goto failed;
        }
    }
    /* This composition has exactly one shard. Honor its self-continuation;
     * never turn a finite work budget into a periodic polling dependency. */
    if ((wakes & ~UINT64_C(1)) != 0 ||
        (wakes != 0 && moq_msquic_lane_wake(lane) != MOQ_OK)) {
        goto failed;
    }
    return 0;
failed:
    atomic_store(&app->failed, true);
    return 1;
}

static int usage(FILE *out)
{
    fputs("Usage: moq_simple_relay CERT.pem KEY.pem [PORT]\n"
          "Loopback-only raw QUIC relay; drafts 18 and 16; default port 4433.\n"
          "PORT 0 selects an ephemeral port. Ctrl-C or SIGTERM stops it.\n", out);
    return out == stdout ? 0 : 2;
}

static bool parse_port(const char *text, uint16_t *port)
{
    unsigned value = 0;
    if (*text == '\0') {
        return false;
    }
    for (; *text != '\0'; ++text) {
        if (*text < '0' || *text > '9') {
            return false;
        }
        value = value * 10 + (unsigned)(*text - '0');
        if (value > 65535) {
            return false;
        }
    }
    *port = (uint16_t)value;
    return true;
}

int main(int argc, char **argv)
{
    uint16_t port = 4433;
    if (argc == 2 && (!strcmp(argv[1], "--help") || !strcmp(argv[1], "-h"))) {
        return usage(stdout);
    }
    if ((argc != 3 && argc != 4) || argv[1][0] == '\0' || argv[2][0] == '\0' ||
        (argc == 4 && !parse_port(argv[3], &port))) {
        return usage(stderr);
    }
    struct sigaction action;
    memset(&action, 0, sizeof(action));
    action.sa_handler = interrupt_handler;
    sigemptyset(&action.sa_mask);
    if (sigaction(SIGINT, &action, NULL) || sigaction(SIGTERM, &action, NULL)) {
        perror("sigaction");
        return 1;
    }
    app_t app = {0};
    atomic_init(&app.failed, false);
    moqr_shards_cfg_t relay_cfg;
    moqr_shards_cfg_init_sized(&relay_cfg, sizeof(relay_cfg), moq_alloc_default());
    relay_cfg.shards = 1;
    relay_cfg.live_visibility = true;
    relay_cfg.bind_cfg.max_conns = 16;
    if (moqr_shards_create(&relay_cfg, &app.relay) != MOQR_OK) {
        fputs("relay create failed\n", stderr);
        return 1;
    }
    const moq_version_t versions[] = {MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_16};
    moq_msquic_managed_cfg_t cfg;
    moq_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.host = "127.0.0.1";
    cfg.port = port;
    cfg.cert_path = argv[1];
    cfg.key_path = argv[2];
    cfg.version = 0;
    cfg.versions = versions;
    cfg.version_count = sizeof(versions) / sizeof(versions[0]);
    cfg.lane_count = 1;
    cfg.max_connections = 16;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 1024;
    cfg.on_lane_pump = pump;
    cfg.on_lane_pump_user = &app;
    moq_msquic_managed_t *transport = NULL;
    int result = 1;
    moq_result_t rc = moq_msquic_managed_create(&cfg, &transport);
    if (rc != MOQ_OK) {
        fprintf(stderr, "transport create failed: %d\n", (int)rc);
        goto done;
    }
    if (printf("Listening on 127.0.0.1:%u\n",
               (unsigned)moq_msquic_managed_port(transport)) < 0 || fflush(stdout)) {
        goto stop;
    }
    result = 0;
    while (!interrupted && !atomic_load(&app.failed)) {
        /* This timeout only bounds signal response. Lane work is event-driven. */
        rc = moq_msquic_managed_wait(transport, 100000);
        if (rc != MOQ_OK && rc != MOQ_DONE) {
            result = 1;
            break;
        }
    }
stop:
    /* stop joins the lane and destroys its sessions. Only then may the
     * memory-only runtime destructor release binding/core state. */
    if (moq_msquic_managed_stop(transport) != MOQ_OK || atomic_load(&app.failed)) {
        result = 1;
    }
    moq_msquic_managed_destroy(transport);
done:
    moqr_shards_destroy(app.relay);
    return result;
}
