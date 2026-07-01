/*
 * Minimal MoQ server over mvfst managed server.
 *
 * Listens for MoQ connections, auto-accepts subscribes, and prints
 * received events (setup, subscribe, object).
 *
 * Usage:
 *   moq_mvfst_server --cert server.pem --key server-key.pem
 *
 * Example:
 *   moq_mvfst_server --cert server.pem --key server-key.pem --port 4433
 *   moq_mvfst_server --cert server.pem --key server-key.pem --port 0
 *
 * Flags:
 *   --host ADDR   Bind address (default: 0.0.0.0)
 *   --port PORT   Listen port, 0 = ephemeral (default: 4433)
 *   --cert FILE   PEM server certificate (required)
 *   --key FILE    PEM private key (required)
 *   --timeout N   Elapsed timeout in seconds (default: 30)
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

/* -- Application state ----------------------------------------------- */

typedef struct {
    /* Cross-thread state (atomics). */
    atomic_int done;
    atomic_int error;
    atomic_size_t last_conn_count;
} app_state_t;

/* -- Pump callback --------------------------------------------------- */

static int on_pump(moq_mvfst_managed_t *m, uint64_t now, void *ctx)
{
    (void)now;
    app_state_t *app = (app_state_t *)ctx;

    moq_mvfst_conn_t *conn = NULL;
    while ((conn = moq_mvfst_managed_next_conn(m, conn)) != NULL) {
        moq_session_t *s = moq_mvfst_conn_session(conn);
        if (!s) continue;

        /* Drain events. */
        moq_event_t ev[16];
        size_t ne;
        moq_session_poll_events_ex(s, ev, 16, sizeof(moq_event_t), &ne);
        for (size_t i = 0; i < ne; i++) {
            switch (ev[i].kind) {
            case MOQ_EVENT_SETUP_COMPLETE:
                fprintf(stderr, "SETUP_COMPLETE\n");
                break;

            case MOQ_EVENT_SUBSCRIBE_REQUEST: {
                moq_accept_subscribe_cfg_t acfg;
                moq_accept_subscribe_cfg_init(&acfg);
                moq_result_t rc = moq_session_accept_subscribe(
                    s, ev[i].u.subscribe_request.sub, &acfg, 0);
                if (rc < 0) {
                    fprintf(stderr,
                        "SUBSCRIBE_REQUEST: accept failed: %d\n", rc);
                } else {
                    fprintf(stderr, "SUBSCRIBE_REQUEST: accepted\n");
                }
                break;
            }

            case MOQ_EVENT_OBJECT_RECEIVED: {
                moq_object_received_event_t *obj =
                    &ev[i].u.object_received;
                size_t payload_len = 0;
                if (obj->payload)
                    payload_len = moq_rcbuf_len(obj->payload);
                printf("OBJECT_RECEIVED: group=%llu obj=%llu %s %zuB\n",
                       (unsigned long long)obj->group_id,
                       (unsigned long long)obj->object_id,
                       obj->datagram ? "datagram" : "stream",
                       payload_len);
                break;
            }
            default:
                break;
            }
            moq_event_cleanup(&ev[i]);
        }
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
    long port = 4433;
    const char *cert_file = NULL;
    const char *key_file = NULL;
    long timeout_s = 30;

    int argi = 1;
    while (argi < argc) {
        if (strcmp(argv[argi], "--host") == 0) {
            argi++;
            if (argi >= argc) {
                fprintf(stderr, "--host requires an argument\n");
                return 1;
            }
            host = argv[argi++];
        } else if (strcmp(argv[argi], "--port") == 0) {
            argi++;
            if (argi >= argc) {
                fprintf(stderr, "--port requires an argument\n");
                return 1;
            }
            if (parse_int(argv[argi], &port) < 0 || port < 0 || port > 65535) {
                fprintf(stderr, "invalid port: %s\n", argv[argi]);
                return 1;
            }
            argi++;
        } else if (strcmp(argv[argi], "--cert") == 0) {
            argi++;
            if (argi >= argc) {
                fprintf(stderr, "--cert requires a file argument\n");
                return 1;
            }
            cert_file = argv[argi++];
        } else if (strcmp(argv[argi], "--key") == 0) {
            argi++;
            if (argi >= argc) {
                fprintf(stderr, "--key requires a file argument\n");
                return 1;
            }
            key_file = argv[argi++];
        } else if (strcmp(argv[argi], "--timeout") == 0) {
            argi++;
            if (argi >= argc) {
                fprintf(stderr, "--timeout requires an argument\n");
                return 1;
            }
            if (parse_int(argv[argi], &timeout_s) < 0 || timeout_s <= 0) {
                fprintf(stderr, "invalid timeout: %s\n", argv[argi]);
                return 1;
            }
            argi++;
        } else {
            fprintf(stderr, "unexpected argument: %s\n", argv[argi]);
            return 1;
        }
    }

    if (!cert_file || !key_file) {
        fprintf(stderr,
            "Usage: %s --cert FILE --key FILE [--host ADDR] [--port PORT]"
            " [--timeout N]\n",
            argv[0] ? argv[0] : "moq_mvfst_server");
        return 1;
    }

    if (timeout_s > 3600) {
        fprintf(stderr, "timeout too large (max 3600s)\n");
        return 1;
    }

    app_state_t app;
    memset(&app, 0, sizeof(app));

    moq_mvfst_managed_cfg_t cfg;
    moq_mvfst_managed_cfg_init(&cfg);
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.host = host;
    cfg.port = (int)port;
    cfg.cert_path = cert_file;
    cfg.key_path = key_file;
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

    uint16_t local_port = moq_mvfst_managed_local_port(m);
    fprintf(stderr, "listening on %s:%u ...\n",
            host ? host : "0.0.0.0", (unsigned)local_port);

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

        /* Print conn_count changes from app thread. */
        size_t cc = moq_mvfst_managed_conn_count(m);
        size_t prev = atomic_load(&app.last_conn_count);
        if (cc != prev) {
            fprintf(stderr, "conn_count: %zu -> %zu\n", prev, cc);
            atomic_store(&app.last_conn_count, cc);
        }
    }

    if (atomic_load(&app.error))
        fprintf(stderr, "exiting with error\n");

    moq_mvfst_managed_destroy(m);
    return atomic_load(&app.error) ? 1 : 0;
}
