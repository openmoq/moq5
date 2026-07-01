/*
 * Minimal MoQ subscriber over mvfst managed client.
 *
 * Connects to a MoQ relay, subscribes to a track, prints received
 * objects, and exits after the first object or a timeout.
 *
 * Usage:
 *   moq_mvfst_subscriber <host> <port> <namespace> <track>
 *
 * Example:
 *   moq_mvfst_subscriber --insecure 127.0.0.1 4433 live/cam1 video
 *   moq_mvfst_subscriber --ca relay-ca.pem relay.example.com 4433 live/cam1 video
 *
 * Flags:
 *   --insecure    Skip TLS certificate verification
 *   --ca FILE     PEM CA file for certificate verification
 *   --timeout N   Total elapsed timeout in seconds (default: 10)
 *
 * All session work happens inside on_pump on the managed thread.
 * The app thread only uses wake/wait for signaling.
 *
 * Thread safety: app_state fields are C11 atomics or constant
 * after init. The pump callback is the sole writer for protocol
 * state; the app thread reads only atomic done/error flags.
 */

#include <moq/mvfst.h>
#include <moq/rcbuf.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define MAX_NS_PARTS 8
#define MAX_NS_LEN   256

/* -- Application state ----------------------------------------------- */

typedef struct {
    /* Config (set before create, read-only in pump). */
    const char *ns_str;
    const char *track_str;

    /* Protocol state (written by pump thread, read by app via atomics). */
    atomic_int setup_done;
    atomic_int subscribed;
    atomic_int sub_ok;
    atomic_int sub_error;
    atomic_int objects_received;
    atomic_int done;
    atomic_int error;
} app_state_t;

/* -- Pump callback --------------------------------------------------- */

static int on_pump(moq_mvfst_managed_t *m, uint64_t now, void *ctx)
{
    (void)now;
    app_state_t *app = (app_state_t *)ctx;
    moq_session_t *s = moq_mvfst_managed_session(m);
    if (!s) return 0;

    /* Subscribe once after setup. */
    if (atomic_load(&app->setup_done) && !atomic_load(&app->subscribed)) {
        char ns_buf[MAX_NS_LEN];
        moq_bytes_t ns_parts[MAX_NS_PARTS];
        size_t ns_count = 0;

        size_t ns_len = strlen(app->ns_str);
        if (ns_len == 0 || ns_len >= MAX_NS_LEN) {
            fprintf(stderr, "namespace too long or empty\n");
            atomic_store(&app->error, 1);
            atomic_store(&app->done, 1);
            return 0;
        }
        memcpy(ns_buf, app->ns_str, ns_len + 1);

        /* Parse slash-separated namespace parts. */
        size_t start = 0;
        for (size_t i = 0; i <= ns_len; i++) {
            if (ns_buf[i] == '/' || ns_buf[i] == '\0') {
                if (i == start) {
                    fprintf(stderr, "empty namespace part\n");
                    atomic_store(&app->error, 1);
                    atomic_store(&app->done, 1);
                    return 0;
                }
                if (ns_count >= MAX_NS_PARTS) {
                    fprintf(stderr, "too many namespace parts (max %d)\n",
                            MAX_NS_PARTS);
                    atomic_store(&app->error, 1);
                    atomic_store(&app->done, 1);
                    return 0;
                }
                ns_buf[i] = '\0';
                ns_parts[ns_count].data = (const uint8_t *)&ns_buf[start];
                ns_parts[ns_count].len = i - start;
                ns_count++;
                start = i + 1;
            }
        }

        moq_subscribe_cfg_t sc;
        moq_subscribe_cfg_init(&sc);
        sc.track_namespace.parts = ns_parts;
        sc.track_namespace.count = ns_count;
        sc.track_name.data = (const uint8_t *)app->track_str;
        sc.track_name.len = strlen(app->track_str);
        sc.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;

        moq_subscription_t sub;
        moq_result_t rc = moq_session_subscribe(s, &sc, 0, &sub);
        if (rc < 0) {
            fprintf(stderr, "subscribe failed: %d\n", rc);
            atomic_store(&app->error, 1);
            atomic_store(&app->done, 1);
            return 0;
        }
        atomic_store(&app->subscribed, 1);
    }

    /* Drain events. */
    moq_event_t ev[16];
    size_t ne;
    moq_session_poll_events_ex(s, ev, 16, sizeof(moq_event_t), &ne);
    for (size_t i = 0; i < ne; i++) {
        switch (ev[i].kind) {
        case MOQ_EVENT_SETUP_COMPLETE:
            atomic_store(&app->setup_done, 1);
            fprintf(stderr, "setup complete\n");
            break;

        case MOQ_EVENT_SUBSCRIBE_OK:
            atomic_store(&app->sub_ok, 1);
            fprintf(stderr, "subscribe ok\n");
            break;

        case MOQ_EVENT_SUBSCRIBE_ERROR:
            atomic_store(&app->sub_error, 1);
            atomic_store(&app->done, 1);
            fprintf(stderr, "subscribe error\n");
            break;

        case MOQ_EVENT_OBJECT_RECEIVED: {
            moq_object_received_event_t *obj = &ev[i].u.object_received;
            size_t payload_len = 0;
            if (obj->payload)
                payload_len = moq_rcbuf_len(obj->payload);
            printf("object: group=%llu obj=%llu %s %zuB\n",
                   (unsigned long long)obj->group_id,
                   (unsigned long long)obj->object_id,
                   obj->datagram ? "datagram" : "stream",
                   payload_len);
            atomic_fetch_add(&app->objects_received, 1);
            atomic_store(&app->done, 1);
            break;
        }
        default:
            break;
        }
        moq_event_cleanup(&ev[i]);
    }

    return 0;
}

/* -- Arg parsing helpers --------------------------------------------- */

#include <errno.h>
#include <limits.h>

static int parse_int(const char *s, long *out)
{
    char *end = NULL;
    errno = 0;
    long v = strtol(s, &end, 10);
    if (end == s || *end != '\0') return -1;
    if (errno == ERANGE || v < INT_MIN || v > INT_MAX) return -1;
    *out = v;
    return 0;
}

/* -- Main ------------------------------------------------------------ */

int main(int argc, char **argv)
{
    const char *host = NULL;
    long port = 0;
    const char *ns = NULL;
    const char *track = NULL;
    int insecure = 0;
    const char *ca_file = NULL;
    long timeout_s = 10;

    int argi = 1;
    while (argi < argc) {
        if (strcmp(argv[argi], "--insecure") == 0) {
            insecure = 1;
            argi++;
        } else if (strcmp(argv[argi], "--ca") == 0) {
            argi++;
            if (argi >= argc || (argv[argi][0] == '-' && argv[argi][1] == '-')) {
                fprintf(stderr, "--ca requires a file path argument\n");
                return 1;
            }
            ca_file = argv[argi++];
        } else if (strcmp(argv[argi], "--timeout") == 0 && argi + 1 < argc) {
            argi++;
            if (parse_int(argv[argi], &timeout_s) < 0 || timeout_s <= 0) {
                fprintf(stderr, "invalid timeout: %s\n", argv[argi]);
                return 1;
            }
            argi++;
        } else if (!host) {
            host = argv[argi++];
        } else if (!port) {
            if (parse_int(argv[argi], &port) < 0 ||
                port < 1 || port > 65535) {
                fprintf(stderr, "invalid port: %s\n", argv[argi]);
                return 1;
            }
            argi++;
        } else if (!ns) {
            ns = argv[argi++];
        } else if (!track) {
            track = argv[argi++];
        } else {
            fprintf(stderr, "unexpected argument: %s\n", argv[argi]);
            return 1;
        }
    }

    if (timeout_s > 3600) {
        fprintf(stderr, "timeout too large (max 3600s)\n");
        return 1;
    }

    if (!host || !port || !ns || !track) {
        fprintf(stderr,
            "Usage: %s [--insecure] [--ca FILE] [--timeout N] <host> <port>"
            " <namespace> <track>\n"
            "  namespace parts separated by /\n"
            "  host may be a hostname or numeric IP address\n",
            argv[0] ? argv[0] : "moq_mvfst_subscriber");
        return 1;
    }

    if (insecure && ca_file) {
        fprintf(stderr, "--insecure and --ca are mutually exclusive\n");
        return 1;
    }

    if (strlen(track) == 0) {
        fprintf(stderr, "track name must not be empty\n");
        return 1;
    }

    app_state_t app;
    memset(&app, 0, sizeof(app));
    app.ns_str = ns;
    app.track_str = track;

    moq_mvfst_managed_cfg_t cfg;
    moq_mvfst_managed_cfg_init(&cfg);
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = host;
    cfg.port = (int)port;
    cfg.insecure_skip_verify = insecure ? true : false;
    cfg.cert_path = ca_file;
    cfg.on_pump = on_pump;
    cfg.user_ctx = &app;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 16;

    moq_mvfst_managed_t *m = NULL;
    moq_result_t rc = moq_mvfst_managed_create(&cfg, &m);
    if (rc != MOQ_OK) {
        fprintf(stderr, "create failed: %d\n", rc);
        return 1;
    }

    fprintf(stderr, "connecting to %s:%ld ...\n", host, port);

    /* Absolute monotonic deadline for total elapsed timeout. */
    struct timespec ts0;
    clock_gettime(CLOCK_MONOTONIC, &ts0);
    uint64_t deadline_us = (uint64_t)ts0.tv_sec * 1000000 +
                           (uint64_t)ts0.tv_nsec / 1000 +
                           (uint64_t)timeout_s * 1000000;

    while (!atomic_load(&app.done)) {
        moq_mvfst_managed_wake(m);

        struct timespec ts;
        clock_gettime(CLOCK_MONOTONIC, &ts);
        uint64_t now_us = (uint64_t)ts.tv_sec * 1000000 +
                          (uint64_t)ts.tv_nsec / 1000;
        if (now_us >= deadline_us) {
            fprintf(stderr, "timeout after %lds\n", timeout_s);
            break;
        }
        uint64_t remaining = deadline_us - now_us;
        if (remaining > 500000) remaining = 500000;

        moq_result_t wr = moq_mvfst_managed_wait(m, remaining);
        if (wr == MOQ_ERR_CLOSED) {
            if (moq_mvfst_managed_is_fatal(m))
                fprintf(stderr, "fatal: code=%llu\n",
                    (unsigned long long)moq_mvfst_managed_fatal_code(m));
            break;
        }
    }

    int received = atomic_load(&app.objects_received);
    if (received > 0)
        fprintf(stderr, "received %d object(s)\n", received);

    moq_mvfst_managed_destroy(m);
    return received > 0 ? 0 : 1;
}
