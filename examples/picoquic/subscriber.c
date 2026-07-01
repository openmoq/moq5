/*
 * MoQ Subscriber over picoquic
 *
 * Connects to a MoQ publisher with ALPN "moqt-16" (draft-16) and subscribes to
 * track ("example","counter"). Prints received objects including
 * per-object properties, and demonstrates a priority update after
 * a few objects arrive.
 *
 * Usage:
 *   moq_subscriber_example [host] [port]
 *   (defaults: localhost 4433)
 *
 * Application logic uses the moq_subscriber_t facade exclusively.
 * Transport/adapter setup is the only code that touches raw session
 * or picoquic APIs.
 */

#include <moq/picoquic.h>
#include <moq/subscriber.h>
#include <moq/rcbuf.h>
#include <picoquic_packet_loop.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <netinet/in.h>
#include <arpa/inet.h>

static volatile int running = 1;

static void sigint_handler(int sig) {
    (void)sig;
    running = 0;
}

typedef struct {
    moq_pq_conn_t    *adapter;
    moq_subscriber_t *sub;
    moq_sub_track_t  *track;
    moq_session_t    *session;
    moq_alloc_t       alloc;
    int               objects_received;
    bool              subscribed;
    bool              updated;
} sub_ctx_t;

/* -- Facade callbacks ----------------------------------------------- */

static void on_subscribed(void *ctx, moq_sub_track_t *track) {
    sub_ctx_t *app = (sub_ctx_t *)ctx;
    app->subscribed = true;
    (void)track;
    fprintf(stderr, "  subscribed - track active\n");
}

static void on_error(void *ctx, moq_sub_track_t *track,
                      moq_request_error_t code, moq_bytes_t reason) {
    (void)ctx; (void)track;
    fprintf(stderr, "  subscribe error: code=%u reason=\"%.*s\"\n",
        code, (int)reason.len, reason.data ? (const char *)reason.data : "");
}

static void on_draining(void *ctx) {
    (void)ctx;
    fprintf(stderr, "  server sent GOAWAY - draining\n");
}

static void on_closed(void *ctx, uint64_t code) {
    (void)ctx;
    fprintf(stderr, "  session closed (code=%llu)\n",
        (unsigned long long)code);
    running = 0;
}

/* -- picoquic client callback --------------------------------------- */

static int client_callback(picoquic_cnx_t *cnx,
    uint64_t stream_id, uint8_t *bytes, size_t length,
    picoquic_call_back_event_t event, void *callback_ctx,
    void *stream_ctx)
{
    sub_ctx_t *ctx = (sub_ctx_t *)callback_ctx;
    if (!ctx->adapter) return 0;
    return moq_pq_callback(cnx, stream_id, bytes, length,
                            event, ctx->adapter, stream_ctx);
}

/* -- Packet loop callback ------------------------------------------- */

static int loop_callback(picoquic_quic_t *quic,
    picoquic_packet_loop_cb_enum cb_mode,
    void *callback_ctx, void *callback_arg)
{
    sub_ctx_t *ctx = (sub_ctx_t *)callback_ctx;
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

    if (!ctx->adapter) return 0;

    uint64_t now = picoquic_get_quic_time(quic);

    if (moq_pq_service(ctx->adapter, now) < 0) {
        fprintf(stderr, "  adapter service failed\n");
        running = 0;
        return PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP;
    }
    moq_sub_tick(ctx->sub, now);

    /* Subscribe once the session handshake completes. */
    if (moq_session_state(ctx->session) == MOQ_SESS_ESTABLISHED &&
        !ctx->track) {
        moq_sub_track_cfg_t tcfg;
        moq_sub_track_cfg_init(&tcfg);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("example") };
        tcfg.track_namespace.parts = ns_parts;
        tcfg.track_namespace.count = 1;
        tcfg.track_name = MOQ_BYTES_LITERAL("counter");

        moq_result_t rc = moq_sub_subscribe(ctx->sub, &tcfg, now,
            &ctx->track);
        if (rc == MOQ_OK) {
            fprintf(stderr, "  subscribe sent for example/counter\n");
        } else {
            fprintf(stderr, "  subscribe failed: %d\n", rc);
            ctx->track = NULL;
        }
        if (moq_pq_service(ctx->adapter, now) < 0) {
            running = 0;
            return PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP;
        }
    }

    /* Poll received objects. */
    moq_sub_object_t obj;
    while (moq_sub_poll_object(ctx->sub, &obj) == MOQ_OK) {
        size_t props_len = obj.properties ? moq_rcbuf_len(obj.properties) : 0;
        if (obj.payload) {
            fprintf(stderr, "  object: g=%llu o=%llu \"%.*s\" [props=%zu]%s\n",
                (unsigned long long)obj.group_id,
                (unsigned long long)obj.object_id,
                (int)moq_rcbuf_len(obj.payload),
                (const char *)moq_rcbuf_data(obj.payload),
                props_len,
                obj.datagram ? " datagram" : "");
        } else {
            fprintf(stderr, "  object: g=%llu o=%llu status=%u\n",
                (unsigned long long)obj.group_id,
                (unsigned long long)obj.object_id,
                obj.status);
        }
        ctx->objects_received++;
        moq_sub_object_cleanup(&obj);
    }

    /* Update subscriber priority after 5 objects. */
    if (ctx->subscribed && !ctx->updated &&
        ctx->objects_received >= 5) {
        moq_sub_update_cfg_t ucfg;
        moq_sub_update_cfg_init(&ucfg);
        ucfg.has_subscriber_priority = true;
        ucfg.subscriber_priority = 200;
        moq_result_t rc = moq_sub_update_subscription(
            ctx->sub, ctx->track, &ucfg, now);
        if (rc == MOQ_OK) {
            ctx->updated = true;
            fprintf(stderr, "  sent priority update to 200\n");
        }
    }

    if (moq_pq_service(ctx->adapter, now) < 0) {
        running = 0;
        return PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP;
    }

    if (!running) return PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP;
    return 0;
}

/* -- Main ----------------------------------------------------------- */

int main(int argc, char *argv[])
{
    const char *host = argc > 1 ? argv[1] : "localhost";
    int port = argc > 2 ? atoi(argv[2]) : 4433;

    signal(SIGINT, sigint_handler);

    sub_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.alloc = *moq_alloc_default();

    uint64_t now = picoquic_current_time();

    picoquic_quic_t *quic = picoquic_create(
        1, NULL, NULL, NULL, MOQ_PQ_ALPN_DEFAULT,
        NULL, NULL, NULL, NULL, NULL, now, NULL, NULL, NULL, 0);
    if (!quic) {
        fprintf(stderr, "picoquic_create failed\n");
        return 1;
    }
    picoquic_set_null_verifier(quic);

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)port);
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    picoquic_cnx_t *cnx = picoquic_create_client_cnx(
        quic, (struct sockaddr *)&addr, now, 0,
        host, MOQ_PQ_ALPN_DEFAULT, client_callback, &ctx);
    if (!cnx) {
        fprintf(stderr, "picoquic_create_client_cnx failed\n");
        picoquic_free(quic);
        return 1;
    }

    moq_session_cfg_t scfg;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), &ctx.alloc, MOQ_PERSPECTIVE_CLIENT);
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 64;

    if (moq_session_create(&scfg, now, &ctx.session) != MOQ_OK) {
        fprintf(stderr, "moq_session_create failed\n");
        picoquic_free(quic);
        return 1;
    }

    moq_pq_conn_cfg_t acfg;
    moq_pq_conn_cfg_init(&acfg);
    acfg.session = ctx.session;
    acfg.cnx = cnx;
    acfg.alloc = &ctx.alloc;
    if (moq_pq_conn_create(&acfg, &ctx.adapter) != 0) {
        moq_session_destroy(ctx.session);
        picoquic_free(quic);
        return 1;
    }

    moq_sub_cfg_t subcfg;
    moq_sub_cfg_init(&subcfg);
    subcfg.callbacks.ctx = &ctx;
    subcfg.callbacks.on_subscribed = on_subscribed;
    subcfg.callbacks.on_subscribe_error = on_error;
    subcfg.callbacks.on_draining = on_draining;
    subcfg.callbacks.on_closed = on_closed;
    if (moq_sub_create(ctx.session, &ctx.alloc, &subcfg, &ctx.sub) != MOQ_OK) {
        fprintf(stderr, "moq_sub_create failed\n");
        moq_pq_conn_destroy(ctx.adapter);
        moq_session_destroy(ctx.session);
        picoquic_free(quic);
        return 1;
    }

    if (moq_session_start(ctx.session, now) != MOQ_OK) {
        fprintf(stderr, "moq_session_start failed\n");
        moq_sub_destroy(ctx.sub);
        moq_pq_conn_destroy(ctx.adapter);
        moq_session_destroy(ctx.session);
        picoquic_free(quic);
        return 1;
    }

    if (moq_pq_service(ctx.adapter, now) < 0) {
        fprintf(stderr, "initial adapter service failed\n");
        moq_sub_destroy(ctx.sub);
        moq_pq_conn_destroy(ctx.adapter);
        moq_session_destroy(ctx.session);
        picoquic_free(quic);
        return 1;
    }
    /* picoquic_create_client_cnx already started the TLS handshake. */

    fprintf(stderr, "Connecting to %s:%d...\n", host, port);

    int rc = picoquic_packet_loop(quic, 0, 0, 0, 0, 0,
                                   loop_callback, &ctx);

    fprintf(stderr, "Received %d objects\n", ctx.objects_received);

    if (ctx.sub) moq_sub_destroy(ctx.sub);
    if (ctx.adapter) moq_pq_conn_destroy(ctx.adapter);
    if (ctx.session) moq_session_destroy(ctx.session);
    picoquic_free(quic);

    fprintf(stderr, "done (rc=%d)\n", rc);
    return rc;
}
