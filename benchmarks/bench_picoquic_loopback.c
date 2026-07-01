/*
 * moq_bench_picoquic_loopback — transport-level loopback benchmark.
 *
 * Single-process benchmark with one picoquic context acting as both
 * server (publisher) and client (subscriber) on localhost.  Measures
 * the full transport path: picoquic QUIC + TLS + adapter + session
 * encode/decode over kernel loopback.
 *
 * This is NOT a sans-I/O benchmark — it exercises real QUIC framing,
 * encryption, and UDP socket I/O on the loopback interface.
 *
 * Requires cert.pem and key.pem for TLS.
 */

#include <moq/picoquic.h>
#include <moq/publisher.h>
#include <moq/subscriber.h>
#include <moq/rcbuf.h>
#include <picoquic_packet_loop.h>

#include <inttypes.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <time.h>
#include <netinet/in.h>
#include <arpa/inet.h>

static volatile int running = 1;
static void sigint_handler(int sig) { (void)sig; running = 0; }

/* ------------------------------------------------------------------ */
/* Monotonic clock                                                     */
/* ------------------------------------------------------------------ */

static uint64_t now_us_mono(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000ULL + (uint64_t)ts.tv_nsec / 1000ULL;
}

/* ------------------------------------------------------------------ */
/* CLI                                                                 */
/* ------------------------------------------------------------------ */

typedef struct {
    const char *cert;
    const char *key;
    uint64_t    objects;
    uint64_t    object_size;
    uint64_t    warmup;
    int         port;
    bool        json;
} bench_cfg_t;

static bench_cfg_t parse_args(int argc, char **argv)
{
    bench_cfg_t cfg = {
        .cert        = NULL,
        .key         = NULL,
        .objects     = 1000,
        .object_size = 1200,
        .warmup      = 50,
        .port        = 14443,
        .json        = false,
    };

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--cert") == 0 && i + 1 < argc)
            cfg.cert = argv[++i];
        else if (strcmp(argv[i], "--key") == 0 && i + 1 < argc)
            cfg.key = argv[++i];
        else if (strcmp(argv[i], "--objects") == 0 && i + 1 < argc)
            cfg.objects = (uint64_t)strtoull(argv[++i], NULL, 10);
        else if (strcmp(argv[i], "--object-size") == 0 && i + 1 < argc)
            cfg.object_size = (uint64_t)strtoull(argv[++i], NULL, 10);
        else if (strcmp(argv[i], "--warmup") == 0 && i + 1 < argc)
            cfg.warmup = (uint64_t)strtoull(argv[++i], NULL, 10);
        else if (strcmp(argv[i], "--port") == 0 && i + 1 < argc)
            cfg.port = atoi(argv[++i]);
        else if (strcmp(argv[i], "--json") == 0)
            cfg.json = true;
        else if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-h") == 0) {
            fprintf(stderr,
                "Usage: moq_bench_picoquic_loopback --cert <cert.pem> --key <key.pem> [options]\n"
                "  --cert PATH           TLS certificate (required)\n"
                "  --key PATH            TLS private key (required)\n"
                "  --objects N           objects to publish (default 1000)\n"
                "  --object-size N       payload bytes per object (default 1200)\n"
                "  --warmup N            warmup objects, excluded from timing (default 50)\n"
                "  --port N              loopback port (default 14443)\n"
                "  --json                output results as JSON\n");
            exit(0);
        } else {
            fprintf(stderr, "unknown flag: %s (try --help)\n", argv[i]);
            exit(1);
        }
    }

    if (!cfg.cert || !cfg.key) {
        fprintf(stderr, "error: --cert and --key are required\n");
        exit(1);
    }
    if (cfg.object_size == 0 || cfg.object_size > SIZE_MAX) {
        fprintf(stderr, "error: --object-size must be 1..%zu\n", (size_t)SIZE_MAX);
        exit(1);
    }
    if (cfg.objects == 0) {
        fprintf(stderr, "error: --objects must be >= 1\n");
        exit(1);
    }
    if (cfg.warmup > cfg.objects) cfg.warmup = 0;

    return cfg;
}

/* ------------------------------------------------------------------ */
/* Application context (both publisher and subscriber)                 */
/* ------------------------------------------------------------------ */

typedef struct {
    /* Config */
    const bench_cfg_t *cfg;
    moq_alloc_t        alloc;

    /* Publisher (server) side */
    moq_pq_conn_t    *pub_adapter;
    moq_publisher_t  *pub;
    moq_pub_track_t  *pub_track;
    bool              pub_ready;

    /* Subscriber (client) side */
    moq_pq_conn_t    *sub_adapter;
    moq_subscriber_t *sub;
    moq_sub_track_t  *sub_track;
    moq_session_t    *sub_session;
    bool              sub_subscribed;

    /* Shared payload rcbuf (zero-copy, created once) */
    moq_rcbuf_t      *payload;
    uint8_t           *payload_data;

    /* Counters */
    uint64_t           objects_published;
    uint64_t           warmup_received;
    uint64_t           objects_received;
    uint64_t           payload_bytes_received;

    /* Timing */
    uint64_t           t_start;
    bool               warmup_done;
    bool               measuring;
    bool               done;
    int                exit_code;
} app_ctx_t;

/* ------------------------------------------------------------------ */
/* Subscriber callbacks                                                */
/* ------------------------------------------------------------------ */

static void on_subscribed(void *ctx, moq_sub_track_t *track) {
    app_ctx_t *app = (app_ctx_t *)ctx;
    app->sub_subscribed = true;
    (void)track;
}

static void on_sub_error(void *ctx, moq_sub_track_t *track,
                          moq_request_error_t code, moq_bytes_t reason) {
    app_ctx_t *app = (app_ctx_t *)ctx;
    (void)track; (void)reason;
    fprintf(stderr, "subscribe error: code=%u\n", code);
    app->done = true;
    app->exit_code = 1;
}

static void on_sub_closed(void *ctx, uint64_t code) {
    app_ctx_t *app = (app_ctx_t *)ctx;
    if (!app->done) {
        fprintf(stderr, "subscriber session closed: code=%" PRIu64 "\n", code);
        app->exit_code = 1;
    }
    app->done = true;
}

/* ------------------------------------------------------------------ */
/* Server (publisher) connection callback                              */
/* ------------------------------------------------------------------ */

static int server_callback(picoquic_cnx_t *cnx,
    uint64_t stream_id, uint8_t *bytes, size_t length,
    picoquic_call_back_event_t event, void *callback_ctx,
    void *stream_ctx)
{
    app_ctx_t *app = (app_ctx_t *)callback_ctx;
    uint64_t now = picoquic_get_quic_time(picoquic_get_quic_ctx(cnx));

    if (event == picoquic_callback_almost_ready ||
        event == picoquic_callback_ready) {
        if (app->pub_adapter) return 0;

        moq_session_cfg_t scfg;
        moq_session_cfg_init_sized(&scfg, sizeof(scfg), &app->alloc, MOQ_PERSPECTIVE_SERVER);
        scfg.send_request_capacity = true;
        scfg.initial_request_capacity = 64;

        moq_session_t *session = NULL;
        if (moq_session_create(&scfg, now, &session) != MOQ_OK) return -1;

        moq_pq_conn_cfg_t acfg;
        moq_pq_conn_cfg_init(&acfg);
        acfg.session = session;
        acfg.cnx = cnx;
        acfg.alloc = &app->alloc;
        if (moq_pq_conn_create(&acfg, &app->pub_adapter) != 0) {
            moq_session_destroy(session);
            return -1;
        }

        moq_pub_cfg_t pcfg;
        moq_pub_cfg_init_sized(&pcfg, sizeof(pcfg));
        pcfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
        if (moq_pub_create(session, &app->alloc, &pcfg, &app->pub) != MOQ_OK) {
            moq_pq_conn_destroy(app->pub_adapter);
            moq_session_destroy(session);
            app->pub_adapter = NULL;
            return -1;
        }

        moq_pub_track_cfg_t tcfg;
        moq_pub_track_cfg_init(&tcfg);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("bench") };
        tcfg.track_namespace.parts = ns_parts;
        tcfg.track_namespace.count = 1;
        tcfg.track_name = MOQ_BYTES_LITERAL("data");
        if (moq_pub_add_track(app->pub, &tcfg, now, &app->pub_track) != MOQ_OK) {
            moq_pub_destroy(app->pub);
            app->pub = NULL;
            moq_pq_conn_destroy(app->pub_adapter);
            moq_session_destroy(session);
            app->pub_adapter = NULL;
            return -1;
        }

        app->pub_ready = true;
        return 0;
    }

    if (app->pub_adapter)
        return moq_pq_callback(cnx, stream_id, bytes, length,
                                event, app->pub_adapter, stream_ctx);
    return 0;
}

/* ------------------------------------------------------------------ */
/* Client (subscriber) connection callback                             */
/* ------------------------------------------------------------------ */

static int client_callback(picoquic_cnx_t *cnx,
    uint64_t stream_id, uint8_t *bytes, size_t length,
    picoquic_call_back_event_t event, void *callback_ctx,
    void *stream_ctx)
{
    app_ctx_t *app = (app_ctx_t *)callback_ctx;
    if (!app->sub_adapter) return 0;
    return moq_pq_callback(cnx, stream_id, bytes, length,
                            event, app->sub_adapter, stream_ctx);
}

/* ------------------------------------------------------------------ */
/* Packet loop callback                                                */
/* ------------------------------------------------------------------ */

static int loop_callback(picoquic_quic_t *quic,
    picoquic_packet_loop_cb_enum cb_mode,
    void *callback_ctx, void *callback_arg)
{
    app_ctx_t *app = (app_ctx_t *)callback_ctx;
    (void)callback_arg;

    if (cb_mode != picoquic_packet_loop_after_receive &&
        cb_mode != picoquic_packet_loop_after_send)
        return 0;
    if (app->done || !running)
        return PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP;

    uint64_t now = picoquic_get_quic_time(quic);

    /* Service publisher adapter. */
    if (app->pub_adapter) {
        if (moq_pq_service(app->pub_adapter, now) < 0) {
            fprintf(stderr, "publisher adapter fatal\n");
            app->exit_code = 1; app->done = true;
            return PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP;
        }
        { moq_result_t trc = moq_pub_tick(app->pub, now);
          if (trc < 0 && trc != MOQ_ERR_WOULD_BLOCK) {
              fprintf(stderr, "pub_tick failed: %d\n", trc);
              app->exit_code = 1; app->done = true;
              return PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP;
          }
        }
        if (moq_pq_service(app->pub_adapter, now) < 0) {
            app->exit_code = 1; app->done = true;
            return PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP;
        }
    }

    /* Service subscriber adapter. */
    if (app->sub_adapter) {
        if (moq_pq_service(app->sub_adapter, now) < 0) {
            fprintf(stderr, "subscriber adapter fatal\n");
            app->exit_code = 1; app->done = true;
            return PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP;
        }
        { moq_result_t trc = moq_sub_tick(app->sub, now);
          if (trc < 0 && trc != MOQ_ERR_WOULD_BLOCK) {
              fprintf(stderr, "sub_tick failed: %d\n", trc);
              app->exit_code = 1; app->done = true;
              return PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP;
          }
        }
    }

    /* Subscribe once session is established. */
    if (app->sub_session && !app->sub_track &&
        moq_session_state(app->sub_session) == MOQ_SESS_ESTABLISHED) {
        moq_sub_track_cfg_t tcfg;
        moq_sub_track_cfg_init(&tcfg);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("bench") };
        tcfg.track_namespace.parts = ns_parts;
        tcfg.track_namespace.count = 1;
        tcfg.track_name = MOQ_BYTES_LITERAL("data");
        if (moq_sub_subscribe(app->sub, &tcfg, now, &app->sub_track) != MOQ_OK) {
            fprintf(stderr, "subscribe failed\n");
            app->exit_code = 1; app->done = true;
            return PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP;
        }
        if (moq_pq_service(app->sub_adapter, now) < 0) {
            app->exit_code = 1; app->done = true;
            return PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP;
        }
    }

    if (!app->pub_ready || !app->sub_subscribed ||
        !moq_pub_has_subscriber(app->pub, app->pub_track))
        return 0;

    /* Phase 1: warmup — publish and drain warmup objects. */
    if (!app->warmup_done) {
        if (app->objects_published < app->cfg->warmup) {
            moq_pub_object_cfg_t obj;
            moq_pub_object_cfg_init(&obj);
            obj.group_id = app->objects_published;
            obj.object_id = 0;
            obj.payload = app->payload;
            obj.end_of_group = true;
            moq_result_t rc = moq_pub_write_object_ex(
                app->pub, app->pub_track, &obj, now);
            if (rc == MOQ_OK) {
                app->objects_published++;
                if (moq_pq_service(app->pub_adapter, now) < 0) {
                    app->exit_code = 1; app->done = true;
                    return PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP;
                }
            } else if (rc != MOQ_ERR_WOULD_BLOCK) {
                fprintf(stderr, "warmup write failed: %d\n", rc);
                app->exit_code = 1; app->done = true;
                return PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP;
            }
        }

        moq_sub_object_t sobj;
        while (moq_sub_poll_object(app->sub, &sobj) == MOQ_OK) {
            app->warmup_received++;
            moq_sub_object_cleanup(&sobj);
        }

        if (app->warmup_received >= app->cfg->warmup) {
            app->warmup_done = true;
            app->t_start = now_us_mono();
        }
        if (app->sub_adapter && moq_pq_service(app->sub_adapter, now) < 0) {
            app->exit_code = 1; app->done = true;
            return PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP;
        }
        return 0;
    }

    /* Phase 2: measured — publish and count received objects. */
    if (app->objects_published < app->cfg->warmup + app->cfg->objects) {
        moq_pub_object_cfg_t obj;
        moq_pub_object_cfg_init(&obj);
        obj.group_id = app->objects_published;
        obj.object_id = 0;
        obj.payload = app->payload;
        obj.end_of_group = true;
        moq_result_t rc = moq_pub_write_object_ex(
            app->pub, app->pub_track, &obj, now);
        if (rc == MOQ_OK) {
            app->objects_published++;
            if (moq_pq_service(app->pub_adapter, now) < 0) {
                app->exit_code = 1; app->done = true;
                return PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP;
            }
        } else if (rc != MOQ_ERR_WOULD_BLOCK) {
            fprintf(stderr, "write_object_ex failed: %d\n", rc);
            app->exit_code = 1; app->done = true;
            return PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP;
        }
    }

    moq_sub_object_t sobj;
    while (moq_sub_poll_object(app->sub, &sobj) == MOQ_OK) {
        app->objects_received++;
        if (sobj.payload)
            app->payload_bytes_received += moq_rcbuf_len(sobj.payload);
        moq_sub_object_cleanup(&sobj);
    }

    if (app->sub_adapter && moq_pq_service(app->sub_adapter, now) < 0) {
        app->exit_code = 1; app->done = true;
        return PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP;
    }

    if (app->objects_received >= app->cfg->objects) {
        app->done = true;
        return PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP;
    }

    return 0;
}

/* ------------------------------------------------------------------ */
/* Main                                                                */
/* ------------------------------------------------------------------ */

int main(int argc, char *argv[])
{
    bench_cfg_t cfg = parse_args(argc, argv);
    signal(SIGINT, sigint_handler);

    app_ctx_t app;
    memset(&app, 0, sizeof(app));
    app.cfg = &cfg;
    app.alloc = *moq_alloc_default();

    /* Pre-allocate shared payload. */
    app.payload_data = (uint8_t *)malloc(cfg.object_size);
    if (!app.payload_data) {
        fprintf(stderr, "payload malloc failed\n");
        return 1;
    }
    memset(app.payload_data, 0xAB, cfg.object_size);
    if (moq_rcbuf_wrap(&app.alloc, app.payload_data, cfg.object_size,
                        NULL, NULL, &app.payload) != MOQ_OK) {
        fprintf(stderr, "rcbuf_wrap failed\n");
        free(app.payload_data);
        return 1;
    }

    uint64_t now = picoquic_current_time();

    /* Create picoquic context (server + client on same context). */
    picoquic_quic_t *quic = picoquic_create(
        8, cfg.cert, cfg.key, NULL, MOQ_PQ_ALPN_DEFAULT,
        server_callback, &app,
        NULL, NULL, NULL, now, NULL, NULL, NULL, 0);
    if (!quic) {
        fprintf(stderr, "picoquic_create failed\n");
        moq_rcbuf_decref(app.payload);
        free(app.payload_data);
        return 1;
    }
    picoquic_set_null_verifier(quic);

    /* Create client connection to localhost:port on the same context. */
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)cfg.port);
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    picoquic_cnx_t *cnx = picoquic_create_client_cnx(
        quic, (struct sockaddr *)&addr, now, 0,
        "localhost", MOQ_PQ_ALPN_DEFAULT, client_callback, &app);
    if (!cnx) {
        fprintf(stderr, "picoquic_create_client_cnx failed\n");
        picoquic_free(quic);
        moq_rcbuf_decref(app.payload);
        free(app.payload_data);
        return 1;
    }

    /* Create subscriber session + adapter + facade. */
    moq_session_cfg_t scfg;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), &app.alloc, MOQ_PERSPECTIVE_CLIENT);
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 64;
    if (moq_session_create(&scfg, now, &app.sub_session) != MOQ_OK) {
        fprintf(stderr, "client session_create failed\n");
        picoquic_free(quic);
        moq_rcbuf_decref(app.payload);
        free(app.payload_data);
        return 1;
    }

    moq_pq_conn_cfg_t acfg;
    moq_pq_conn_cfg_init(&acfg);
    acfg.session = app.sub_session;
    acfg.cnx = cnx;
    acfg.alloc = &app.alloc;
    if (moq_pq_conn_create(&acfg, &app.sub_adapter) != 0) {
        moq_session_destroy(app.sub_session);
        picoquic_free(quic);
        moq_rcbuf_decref(app.payload);
        free(app.payload_data);
        return 1;
    }

    moq_sub_cfg_t subcfg;
    moq_sub_cfg_init(&subcfg);
    subcfg.callbacks.ctx = &app;
    subcfg.callbacks.on_subscribed = on_subscribed;
    subcfg.callbacks.on_subscribe_error = on_sub_error;
    subcfg.callbacks.on_closed = on_sub_closed;
    if (moq_sub_create(app.sub_session, &app.alloc, &subcfg, &app.sub) != MOQ_OK) {
        moq_pq_conn_destroy(app.sub_adapter);
        moq_session_destroy(app.sub_session);
        picoquic_free(quic);
        moq_rcbuf_decref(app.payload);
        free(app.payload_data);
        return 1;
    }

    if (moq_session_start(app.sub_session, now) != MOQ_OK) {
        fprintf(stderr, "session_start failed\n");
        moq_sub_destroy(app.sub);
        moq_pq_conn_destroy(app.sub_adapter);
        moq_session_destroy(app.sub_session);
        picoquic_free(quic);
        moq_rcbuf_decref(app.payload);
        free(app.payload_data);
        return 1;
    }
    if (moq_pq_service(app.sub_adapter, now) < 0) {
        fprintf(stderr, "initial adapter service failed\n");
        moq_sub_destroy(app.sub);
        moq_pq_conn_destroy(app.sub_adapter);
        moq_session_destroy(app.sub_session);
        picoquic_free(quic);
        moq_rcbuf_decref(app.payload);
        free(app.payload_data);
        return 1;
    }
    /* picoquic_create_client_cnx already called picoquic_start_client_cnx
     * internally — do not call it again. */

    /* Run the event loop. */
    int rc = picoquic_packet_loop(quic, cfg.port, 0, 0, 0, 0,
                                   loop_callback, &app);

    uint64_t t_end = now_us_mono();
    uint64_t elapsed_us = (app.t_start > 0 && t_end > app.t_start)
        ? t_end - app.t_start : 1;

    double elapsed_ms = (double)elapsed_us / 1000.0;
    double elapsed_s  = (double)elapsed_us / 1000000.0;
    double obj_per_sec = elapsed_s > 0
        ? (double)app.objects_received / elapsed_s : 0;
    double mbps = elapsed_s > 0
        ? ((double)app.payload_bytes_received * 8.0) / (1048576.0 * elapsed_s) : 0;

    /* Cleanup. */
    if (app.sub) moq_sub_destroy(app.sub);
    if (app.pub) moq_pub_destroy(app.pub);
    if (app.sub_adapter) {
        moq_pq_conn_destroy(app.sub_adapter);
        moq_session_destroy(app.sub_session);
    }
    if (app.pub_adapter) {
        moq_session_t *ps = moq_pq_conn_session(app.pub_adapter);
        moq_pq_conn_destroy(app.pub_adapter);
        if (ps) moq_session_destroy(ps);
    }
    picoquic_free(quic);
    moq_rcbuf_decref(app.payload);
    free(app.payload_data);

    /* Report. */
    if (cfg.json) {
        printf("{\n");
        printf("  \"objects_published\": %" PRIu64 ",\n", cfg.objects);
        printf("  \"objects_received\": %" PRIu64 ",\n", app.objects_received);
        printf("  \"object_size\": %" PRIu64 ",\n", cfg.object_size);
        printf("  \"warmup\": %" PRIu64 ",\n", cfg.warmup);
        printf("  \"total_payload_bytes\": %" PRIu64 ",\n",
            app.payload_bytes_received);
        printf("  \"elapsed_ms\": %.2f,\n", elapsed_ms);
        printf("  \"objects_per_sec\": %.1f,\n", obj_per_sec);
        printf("  \"mbps\": %.2f,\n", mbps);
        printf("  \"port\": %d\n", cfg.port);
        printf("}\n");
    } else {
        printf("--- moq_bench_picoquic_loopback ---\n");
        printf("objects published : %" PRIu64 "\n", cfg.objects);
        printf("objects received  : %" PRIu64 "\n", app.objects_received);
        printf("object size       : %" PRIu64 " B\n", cfg.object_size);
        printf("warmup            : %" PRIu64 "\n", cfg.warmup);
        printf("total payload     : %" PRIu64 " B\n",
            app.payload_bytes_received);
        printf("elapsed           : %.2f ms\n", elapsed_ms);
        printf("throughput        : %.1f obj/s\n", obj_per_sec);
        printf("throughput        : %.2f Mbps\n", mbps);
        printf("port              : %d\n", cfg.port);
    }

    if (app.objects_received != cfg.objects) {
        fprintf(stderr, "FAIL: objects received %" PRIu64 " != expected %" PRIu64 "\n",
            app.objects_received, cfg.objects);
        return 1;
    }
    uint64_t expected_bytes = (cfg.objects <= UINT64_MAX / cfg.object_size)
        ? cfg.objects * cfg.object_size : UINT64_MAX;
    if (app.payload_bytes_received != expected_bytes) {
        fprintf(stderr, "FAIL: payload bytes %" PRIu64 " != expected %" PRIu64 "\n",
            app.payload_bytes_received, expected_bytes);
        return 1;
    }

    if (app.exit_code != 0 || rc != 0)
        return 1;

    return 0;
}
