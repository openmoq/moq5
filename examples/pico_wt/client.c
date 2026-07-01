/*
 * pico_wt_client — MoQ over WebTransport (picoquic) client example.
 *
 * Connects to a WebTransport server on /moq, attaches
 * moq_pico_wt_conn_t, runs the MoQ setup handshake, subscribes, and
 * prints the first received object. Single-threaded: the picoquic
 * packet loop runs on this thread, so every callback and the MoQ
 * session service run on one thread (session is confined).
 *
 * Pairs with pico_wt_server. Not a public API. Helpers are static.
 * Build-tree only; not installed.
 *
 * TLS certificate verification is ON by default. Pass --insecure-skip-verify
 * to install a null verifier (accept any cert) for a LOCAL/self-signed test
 * server only; real clients must keep verification on.
 */

#include <moq/moq.h>
#include <moq/pico_wt.h>

#include <picoquic.h>
#include <picoquic_packet_loop.h>
#include <picoquic_utils.h>
#include <picosocks.h>
#include <pico_webtransport.h>
#include <h3zero_common.h>

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define PICO_WT_ALPN "h3"

typedef struct {
    picoquic_quic_t    *quic;
    picoquic_cnx_t     *cnx;
    h3zero_callback_ctx_t *h3_ctx;
    h3zero_stream_ctx_t   *ctrl_ctx;

    moq_session_t      *session;
    moq_pico_wt_conn_t *conn;

    int      wt_accepted;
    int      attached;
    int      subscribed;
    int      got_object;
    uint64_t deadline_us;   /* give up after this sim/real time */
} client_ctx_t;

/* WT control-stream callback (client side). picowt_connect manages the
 * control stream + prefix; we only observe acceptance. */
static int client_wt_cb(picoquic_cnx_t *cnx, uint8_t *bytes, size_t length,
                        picohttp_call_back_event_t event,
                        h3zero_stream_ctx_t *stream_ctx, void *path_app_ctx)
{
    client_ctx_t *c = (client_ctx_t *)path_app_ctx;
    (void)cnx; (void)bytes; (void)length; (void)stream_ctx;
    if (event == picohttp_callback_connect_accepted) {
        c->wt_accepted = 1;
        printf("[client] WebTransport CONNECT accepted\n");
    }
    return 0;
}

/* Attach the adapter once WT CONNECT is accepted, then start the MoQ
 * session. The subscribe is sent later, after SETUP_COMPLETE (sending
 * it before setup would be rejected). Loop thread → confinement-safe. */
static int client_attach(client_ctx_t *c)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), MOQ_PERSPECTIVE_CLIENT);
    cfg.send_request_capacity = 1;
    cfg.initial_request_capacity = 16;
    if (moq_session_create(&cfg, 0, &c->session) != 0)
        return -1;

    moq_pico_wt_conn_cfg_t wc;
    moq_pico_wt_conn_cfg_init(&wc);
    wc.session = c->session;
    wc.cnx = c->cnx;
    wc.h3_ctx = c->h3_ctx;
    wc.ctrl_ctx = c->ctrl_ctx;
    wc.alloc = moq_alloc_default();
    if (moq_pico_wt_conn_create(&wc, &c->conn) != 0) {
        moq_session_destroy(c->session);
        c->session = NULL;
        return -1;
    }

    moq_session_start(c->session, picoquic_get_quic_time(c->quic));
    c->attached = 1;
    printf("[client] MoQ session started\n");
    return 0;
}

static void client_send_subscribe(client_ctx_t *c)
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
    if (moq_session_subscribe(c->session, &sc, 0, &sub) < 0) {
        fprintf(stderr, "[client] subscribe failed\n");
        return;
    }
    c->subscribed = 1;
    printf("[client] subscribe sent\n");
}

static void client_drain_events(client_ctx_t *c)
{
    moq_event_t ev;
    while (moq_session_poll_events(c->session, &ev, 1) > 0) {
        switch (ev.kind) {
        case MOQ_EVENT_SETUP_COMPLETE:
            printf("[client] MoQ setup complete\n");
            if (!c->subscribed)
                client_send_subscribe(c);
            break;
        case MOQ_EVENT_SUBSCRIBE_OK:
            printf("[client] SUBSCRIBE_OK\n");
            break;
        case MOQ_EVENT_OBJECT_RECEIVED:
            if (ev.u.object_received.payload) {
                const uint8_t *d =
                    moq_rcbuf_data(ev.u.object_received.payload);
                size_t l = moq_rcbuf_len(ev.u.object_received.payload);
                printf("[client] object received (%zu bytes): %.*s\n",
                       l, (int)l, (const char *)d);
            } else {
                printf("[client] object received (empty)\n");
            }
            c->got_object = 1;
            break;
        default:
            break;
        }
        moq_event_cleanup(&ev);
    }
}

/* Attach (once WT-accepted) and service the session. Driven from both
 * I/O callbacks and the periodic time-check, so progress happens even
 * when no packet just arrived (e.g. right after WT acceptance). */
static int client_progress(client_ctx_t *c)
{
    if (c->wt_accepted && !c->attached) {
        if (client_attach(c) != 0)
            return -1;
    }
    if (c->attached && c->conn) {
        moq_pico_wt_service(c->conn, picoquic_get_quic_time(c->quic));
        client_drain_events(c);
    }
    return 0;
}

static int client_loop_cb(picoquic_quic_t *quic,
                          picoquic_packet_loop_cb_enum cb_mode,
                          void *callback_ctx, void *callback_arg)
{
    client_ctx_t *c = (client_ctx_t *)callback_ctx;
    (void)quic;

    if (c->got_object)
        return PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP;

    switch (cb_mode) {
    case picoquic_packet_loop_ready:
        if (callback_arg)
            ((picoquic_packet_loop_options_t *)callback_arg)->do_time_check = 1;
        break;
    case picoquic_packet_loop_time_check: {
        packet_loop_time_check_arg_t *tc =
            (packet_loop_time_check_arg_t *)callback_arg;
        if (tc) {
            if (tc->current_time >= c->deadline_us)
                return PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP;
            if (tc->delta_t > 20000) tc->delta_t = 20000;
        }
        if (client_progress(c) != 0)
            return PICOQUIC_ERROR_UNEXPECTED_ERROR;
        break;
    }
    case picoquic_packet_loop_after_receive:
    case picoquic_packet_loop_after_send:
        if (client_progress(c) != 0)
            return PICOQUIC_ERROR_UNEXPECTED_ERROR;
        break;
    default:
        break;
    }
    if (c->got_object)
        return PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP;
    return 0;
}

static void usage(const char *prog)
{
    fprintf(stderr,
        "usage: %s [--host H] [--port N] [--sni S] [--path /moq] "
        "[--timeout-sec N] [--insecure-skip-verify]\n"
        "  --insecure-skip-verify  disable TLS certificate verification\n"
        "                          (LOCAL/self-signed testing ONLY)\n", prog);
}

int main(int argc, char **argv)
{
    const char *host = "127.0.0.1";
    int port = 4433;
    const char *sni = "test.example.com";
    const char *path = "/moq";
    int timeout_sec = 5;
    int insecure_skip_verify = 0;   /* TLS verification ON by default */

    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--host") && i + 1 < argc) host = argv[++i];
        else if (!strcmp(argv[i], "--port") && i + 1 < argc)
            port = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--sni") && i + 1 < argc) sni = argv[++i];
        else if (!strcmp(argv[i], "--path") && i + 1 < argc)
            path = argv[++i];
        else if (!strcmp(argv[i], "--timeout-sec") && i + 1 < argc)
            timeout_sec = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--insecure-skip-verify"))
            insecure_skip_verify = 1;
        else { usage(argv[0]); return 2; }
    }

    struct sockaddr_storage server_address;
    int is_name = 0;
    if (picoquic_get_server_address(host, port, &server_address,
                                    &is_name) != 0) {
        fprintf(stderr, "[client] cannot resolve %s:%d\n", host, port);
        return 1;
    }

    client_ctx_t c;
    memset(&c, 0, sizeof(c));

    uint64_t now = picoquic_current_time();
    c.deadline_us = now + (uint64_t)timeout_sec * 1000000ull;

    c.quic = picoquic_create(1, NULL, NULL, NULL, PICO_WT_ALPN,
        NULL, NULL, NULL, NULL, NULL, now, NULL, NULL, NULL, 0);
    if (!c.quic) {
        fprintf(stderr, "[client] picoquic_create failed\n");
        return 1;
    }
    /* TLS certificate verification stays on by default; install the null
     * verifier (accept any cert) only when the operator explicitly opts in via
     * --insecure-skip-verify, for a local/self-signed test server. */
    if (insecure_skip_verify)
        picoquic_set_null_verifier(c.quic);

    if (picowt_prepare_client_cnx(c.quic,
            (struct sockaddr *)&server_address,
            &c.cnx, &c.h3_ctx, &c.ctrl_ctx, now, sni) != 0) {
        fprintf(stderr, "[client] picowt_prepare_client_cnx failed\n");
        picoquic_free(c.quic);
        return 1;
    }
    picowt_set_transport_parameters(c.cnx);

    if (picowt_connect(c.cnx, c.h3_ctx, c.ctrl_ctx, sni, path,
                       client_wt_cb, &c, NULL) != 0) {
        fprintf(stderr, "[client] picowt_connect failed\n");
        picoquic_free(c.quic);
        return 1;
    }
    if (picoquic_start_client_cnx(c.cnx) != 0) {
        fprintf(stderr, "[client] picoquic_start_client_cnx failed\n");
        picoquic_free(c.quic);
        return 1;
    }

    printf("[client] connecting to %s:%d path %s\n", host, port, path);

    int ret = picoquic_packet_loop(c.quic, 0, 0, 0, 0, 0,
                                   client_loop_cb, &c);
    if (ret == PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP)
        ret = 0;

    if (c.conn) moq_pico_wt_conn_destroy(c.conn);
    if (c.session) moq_session_destroy(c.session);
    picoquic_free(c.quic);

    if (c.got_object) {
        printf("[client] done: object received\n");
        return 0;
    }
    fprintf(stderr, "[client] done: no object within %ds\n", timeout_sec);
    return 1;
}
