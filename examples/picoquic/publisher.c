/*
 * MoQ Publisher over picoquic
 *
 * Listens for a QUIC connection with ALPN "moqt-16" (draft-16).
 * When a subscriber connects and subscribes to track ("example","counter"),
 * publishes a counter object every loop iteration using moq_rcbuf_wrap
 * for zero-copy payload ownership and per-object properties.
 *
 * Usage:
 *   moq_publisher_example <cert.pem> <key.pem> [port]
 *   (default port: 4433)
 */

#include <moq/picoquic.h>
#include <moq/publisher.h>
#include <moq/rcbuf.h>
#include <picoquic_packet_loop.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>

static volatile int running = 1;

static void sigint_handler(int sig) {
    (void)sig;
    running = 0;
}

typedef struct {
    moq_pq_conn_t   *adapter;
    moq_publisher_t  *pub;
    moq_pub_track_t  *track;
    moq_alloc_t       alloc;
    uint64_t          counter;
    bool              ready;
} app_ctx_t;

/* -- Release callback for wrapped payloads -------------------------- */

static void payload_release(void *ctx, const uint8_t *data, size_t len) {
    (void)ctx; (void)len;
    free((void *)data);
}

/* -- Callbacks ------------------------------------------------------ */

static void on_subscriber_joined(void *ctx, moq_pub_track_t *track) {
    (void)ctx; (void)track;
    fprintf(stderr, "  subscriber joined\n");
}

static void on_subscriber_left(void *ctx, moq_pub_track_t *track) {
    (void)ctx; (void)track;
    fprintf(stderr, "  subscriber left\n");
}

static void on_subscriber_updated(void *ctx, moq_pub_track_t *track,
    const moq_pub_subscribe_update_info_t *info) {
    (void)ctx; (void)track;
    if (info->has_subscriber_priority)
        fprintf(stderr, "  subscriber update: priority=%u\n",
            info->subscriber_priority);
    if (info->has_forward)
        fprintf(stderr, "  subscriber update: forward=%s\n",
            info->forward ? "true" : "false");
    if (info->has_delivery_timeout)
        fprintf(stderr, "  subscriber update: timeout=%llu us\n",
            (unsigned long long)info->delivery_timeout_us);
}

static void on_closed(void *ctx, uint64_t code) {
    (void)ctx;
    fprintf(stderr, "  session closed (code=%llu)\n",
        (unsigned long long)code);
    running = 0;
}

/* -- picoquic server callback --------------------------------------- */

static int server_callback(picoquic_cnx_t *cnx,
    uint64_t stream_id, uint8_t *bytes, size_t length,
    picoquic_call_back_event_t event, void *callback_ctx,
    void *stream_ctx)
{
    app_ctx_t *app = (app_ctx_t *)callback_ctx;
    uint64_t now = picoquic_get_quic_time(picoquic_get_quic_ctx(cnx));

    if (event == picoquic_callback_almost_ready ||
        event == picoquic_callback_ready) {
        if (app->adapter) return 0;

        moq_session_cfg_t scfg;
        moq_session_cfg_init_sized(&scfg, sizeof(scfg), &app->alloc, MOQ_PERSPECTIVE_SERVER);
        scfg.send_request_capacity = true;
        scfg.initial_request_capacity = 64;

        moq_session_t *session = NULL;
        if (moq_session_create(&scfg, now, &session) != MOQ_OK) {
            fprintf(stderr, "  failed to create session\n");
            return -1;
        }

        moq_pq_conn_cfg_t acfg;
        moq_pq_conn_cfg_init(&acfg);
        acfg.session = session;
        acfg.cnx = cnx;
        acfg.alloc = &app->alloc;
        if (moq_pq_conn_create(&acfg, &app->adapter) != 0) {
            moq_session_destroy(session);
            return -1;
        }

        moq_pub_cfg_t pcfg;
        moq_pub_cfg_init_sized(&pcfg, sizeof(pcfg));
        pcfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
        pcfg.callbacks.ctx = app;
        pcfg.callbacks.on_subscriber_joined = on_subscriber_joined;
        pcfg.callbacks.on_subscriber_left = on_subscriber_left;
        pcfg.callbacks.on_subscriber_updated = on_subscriber_updated;
        pcfg.callbacks.on_closed = on_closed;

        if (moq_pub_create(session, &app->alloc, &pcfg, &app->pub) != MOQ_OK) {
            moq_pq_conn_destroy(app->adapter);
            moq_session_destroy(session);
            app->adapter = NULL;
            return -1;
        }

        moq_pub_track_cfg_t tcfg;
        moq_pub_track_cfg_init(&tcfg);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("example") };
        tcfg.track_namespace.parts = ns_parts;
        tcfg.track_namespace.count = 1;
        tcfg.track_name = MOQ_BYTES_LITERAL("counter");

        if (moq_pub_add_track(app->pub, &tcfg, now, &app->track) != MOQ_OK)
            fprintf(stderr, "  failed to add track\n");

        app->ready = true;
        fprintf(stderr, "  connection ready, track added\n");
        return 0;
    }

    if (app->adapter)
        return moq_pq_callback(cnx, stream_id, bytes, length,
                                event, app->adapter, stream_ctx);
    return 0;
}

/* -- Packet loop callback ------------------------------------------- */

static int loop_callback(picoquic_quic_t *quic,
    picoquic_packet_loop_cb_enum cb_mode,
    void *callback_ctx, void *callback_arg)
{
    app_ctx_t *app = (app_ctx_t *)callback_ctx;
    (void)callback_arg;

    switch (cb_mode) {
    case picoquic_packet_loop_ready:
        fprintf(stderr, "  packet loop ready\n");
        return 0;
    case picoquic_packet_loop_after_receive:
    case picoquic_packet_loop_after_send:
        break;
    default:
        return 0;
    }

    if (!app->adapter || !app->ready) return 0;

    uint64_t now = picoquic_get_quic_time(quic);

    if (moq_pq_service(app->adapter, now) < 0) {
        fprintf(stderr, "  adapter service failed\n");
        running = 0;
        return PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP;
    }
    moq_pub_tick(app->pub, now);
    if (moq_pq_service(app->adapter, now) < 0) {
        running = 0;
        return PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP;
    }

    if (moq_pub_has_subscriber(app->pub, app->track)) {
        /* Build payload string on the heap. moq_rcbuf_wrap takes
         * ownership — payload_release frees it on final decref. */
        char *payload = (char *)malloc(64);
        if (!payload) return 0;
        int len = snprintf(payload, 64,
            "count=%llu", (unsigned long long)app->counter);

        moq_rcbuf_t *buf = NULL;
        if (moq_rcbuf_wrap(&app->alloc, (const uint8_t *)payload,
                (size_t)len, payload_release, NULL, &buf) != MOQ_OK) {
            free(payload);
            return 0;
        }

        /* Small per-object property: type=1 (odd=length-prefixed),
         * value = 4-byte big-endian counter. */
        uint8_t prop_bytes[6];
        prop_bytes[0] = 0x01; /* type */
        prop_bytes[1] = 0x04; /* value length */
        prop_bytes[2] = (uint8_t)(app->counter >> 24);
        prop_bytes[3] = (uint8_t)(app->counter >> 16);
        prop_bytes[4] = (uint8_t)(app->counter >> 8);
        prop_bytes[5] = (uint8_t)(app->counter);
        moq_rcbuf_t *props = NULL;
        if (moq_rcbuf_create(&app->alloc, prop_bytes, 6, &props) != MOQ_OK) {
            moq_rcbuf_decref(buf);
            return 0;
        }

        moq_pub_object_cfg_t obj;
        moq_pub_object_cfg_init(&obj);
        obj.group_id = app->counter;
        obj.object_id = 0;
        obj.payload = buf;
        obj.properties = props;

        moq_result_t rc = moq_pub_write_object_ex(
            app->pub, app->track, &obj, now);

        if (rc == MOQ_OK) {
            fprintf(stderr, "  published: g=%llu \"%.*s\" [props=%zu]\n",
                (unsigned long long)app->counter, len, payload,
                moq_rcbuf_len(props));
            app->counter++;
        }

        moq_rcbuf_decref(buf);
        moq_rcbuf_decref(props);

        if (moq_pq_service(app->adapter, now) < 0) {
            running = 0;
            return PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP;
        }
    }

    if (!running) return PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP;
    return 0;
}

/* -- Main ----------------------------------------------------------- */

int main(int argc, char *argv[])
{
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <cert.pem> <key.pem> [port]\n", argv[0]);
        return 1;
    }

    int port = argc > 3 ? atoi(argv[3]) : 4433;
    signal(SIGINT, sigint_handler);

    app_ctx_t app;
    memset(&app, 0, sizeof(app));
    app.alloc = *moq_alloc_default();

    uint64_t now = picoquic_current_time();

    picoquic_quic_t *quic = picoquic_create(
        8, argv[1], argv[2], NULL, MOQ_PQ_ALPN_DEFAULT,
        server_callback, &app,
        NULL, NULL, NULL, now, NULL, NULL, NULL, 0);
    if (!quic) {
        fprintf(stderr, "picoquic_create failed\n");
        return 1;
    }

    fprintf(stderr, "Listening on port %d...\n", port);

    int rc = picoquic_packet_loop(quic, port, 0, 0, 0, 0,
                                   loop_callback, &app);

    if (app.pub) moq_pub_destroy(app.pub);
    if (app.adapter) {
        moq_session_t *s = moq_pq_conn_session(app.adapter);
        moq_pq_conn_destroy(app.adapter);
        if (s) moq_session_destroy(s);
    }
    picoquic_free(quic);

    fprintf(stderr, "done (rc=%d)\n", rc);
    return rc;
}
