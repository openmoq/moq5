/*
 * pico_wt_server — MoQ over WebTransport (picoquic) server example.
 *
 * Demonstrates the attach-mode lifecycle from an application-shaped
 * entry point, using picoquic + h3zero + picowt directly and then
 * attaching moq_pico_wt_conn_t. Single-threaded: the picoquic packet
 * loop runs on this thread, so every picoquic/h3zero callback AND the
 * MoQ session service all run on one thread — no cross-thread
 * marshalling needed (the session is single-thread-confined).
 *
 * Flow: accept WT CONNECT on /moq, attach the adapter, complete the
 * MoQ setup, accept the first subscribe, and publish one object. Single
 * WT session only (an explicit one-session demo): a second CONNECT is
 * refused. A real server would track connections in a proper map.
 *
 * Not a public API. Helpers are static. Build-tree only; not installed.
 */

#include <moq/moq.h>
#include <moq/pico_wt.h>

#include <picoquic.h>
#include <picoquic_packet_loop.h>
#include <picoquic_utils.h>
#include <pico_webtransport.h>
#include <h3zero_common.h>
#include <demoserver.h>

#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* WT over HTTP/3 uses the "h3" ALPN. */
#define PICO_WT_ALPN "h3"

static volatile sig_atomic_t g_stop = 0;
static void on_sigint(int sig) { (void)sig; g_stop = 1; }

/* Single-connection server state for the skeleton. */
typedef struct {
    picohttp_server_path_item_t  path_table[1];
    picohttp_server_parameters_t server_param;
    const char                  *path;       /* e.g. "/moq" */
    moq_session_t               *session;    /* created on CONNECT */
    moq_pico_wt_conn_t          *conn;       /* attached on CONNECT */
    int                          attached;
    int                          published;  /* one-object demo guard */
} server_ctx_t;

/* Accept the subscribe and publish one demo object on it, so the
 * client example sees real end-to-end delivery. A real server would
 * publish a track's actual content; this just demonstrates the path. */
static void serve_subscribe(server_ctx_t *s, moq_subscription_t sub,
                            uint64_t now)
{
    moq_accept_subscribe_cfg_t acfg;
    moq_accept_subscribe_cfg_init(&acfg);
    if (moq_session_accept_subscribe(s->session, sub, &acfg, now) < 0)
        return;

    moq_subgroup_cfg_t sgcfg;
    moq_subgroup_cfg_init(&sgcfg);
    sgcfg.group_id = 0;
    sgcfg.publisher_priority = 200;
    moq_subgroup_handle_t sg;
    if (moq_session_open_subgroup(s->session, sub, &sgcfg, now, &sg) < 0)
        return;

    const char *payload = "hello-pico-wt";
    moq_rcbuf_t *buf = NULL;
    if (moq_rcbuf_create(moq_alloc_default(),
            (const uint8_t *)payload, strlen(payload), &buf) < 0)
        return;
    int wrc = moq_session_write_object(s->session, sg, 0, buf, now);
    moq_rcbuf_decref(buf);
    if (wrc < 0) {
        fprintf(stderr, "[server] write_object failed (%d)\n", wrc);
        moq_session_close_subgroup(s->session, sg, now);
        return;
    }
    if (moq_session_close_subgroup(s->session, sg, now) < 0) {
        fprintf(stderr, "[server] close_subgroup failed\n");
        return;
    }
    s->published = 1;
    printf("[server] subscribe accepted; published one object\n");
}

/* Drain MoQ events: report setup, and serve the first subscribe. */
static void drain_session_events(server_ctx_t *s, uint64_t now)
{
    moq_event_t ev;
    while (moq_session_poll_events(s->session, &ev, 1) > 0) {
        switch (ev.kind) {
        case MOQ_EVENT_SETUP_COMPLETE:
            printf("[server] MoQ setup complete\n");
            break;
        case MOQ_EVENT_SUBSCRIBE_REQUEST:
            if (!s->published)
                serve_subscribe(s, ev.u.subscribe_request.sub, now);
            break;
        default:
            break;
        }
        moq_event_cleanup(&ev);
    }
}

/*
 * WT control-stream callback. h3zero invokes this for the /moq path.
 * On CONNECT we declare the stream prefix and attach the adapter; the
 * adapter rebinds the prefix to its own callback, so WT data streams
 * and datagrams thereafter route through moq_pico_wt_conn_t. Returning
 * 0 from the connect event accepts the WT session.
 */
static int server_wt_cb(picoquic_cnx_t *cnx, uint8_t *bytes, size_t length,
                        picohttp_call_back_event_t event,
                        h3zero_stream_ctx_t *stream_ctx, void *path_app_ctx)
{
    server_ctx_t *s = (server_ctx_t *)path_app_ctx;
    (void)bytes; (void)length;

    if (event != picohttp_callback_connect)
        return 0;  /* data/fin/etc. route to the adapter post-attach */

    if (s->attached) {
        /* Explicit one-session demo: refuse a second WT CONNECT
         * (returning -1 rejects it) rather than accepting a session
         * we would not attach or service. */
        printf("[server] refusing extra CONNECT (one-session demo)\n");
        return -1;
    }

    h3zero_callback_ctx_t *h3 =
        (h3zero_callback_ctx_t *)picoquic_get_callback_context(cnx);
    if (h3zero_declare_stream_prefix(h3, stream_ctx->stream_id,
                                     server_wt_cb, s) != 0)
        return -1;

    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), MOQ_PERSPECTIVE_SERVER);
    cfg.send_request_capacity = 1;
    cfg.initial_request_capacity = 16;
    if (moq_session_create(&cfg, 0, &s->session) != 0)
        return -1;

    moq_pico_wt_conn_cfg_t wc;
    moq_pico_wt_conn_cfg_init(&wc);
    wc.session = s->session;
    wc.cnx = cnx;
    wc.h3_ctx = h3;
    wc.ctrl_ctx = stream_ctx;
    wc.alloc = moq_alloc_default();
    if (moq_pico_wt_conn_create(&wc, &s->conn) != 0) {
        moq_session_destroy(s->session);
        s->session = NULL;
        return -1;
    }

    s->attached = 1;
    printf("[server] WT CONNECT accepted on %s; MoQ adapter attached\n",
           s->path);
    return 0;
}

/*
 * Packet-loop callback. Runs on the loop thread after each receive/send.
 * This is where the skeleton services the MoQ session — the adapter has
 * no after-callback hook, so the application interleaves the pump here.
 * (A managed facade would own this loop and likely expose an
 * after_callback so callers don't have to wire servicing by hand.)
 */
static int server_loop_cb(picoquic_quic_t *quic,
                          picoquic_packet_loop_cb_enum cb_mode,
                          void *callback_ctx, void *callback_arg)
{
    server_ctx_t *s = (server_ctx_t *)callback_ctx;

    /* Checked on every loop event, including the periodic time-check
     * below — so a SIGINT during an idle recv still shuts down cleanly. */
    if (g_stop)
        return PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP;

    /* Ask the loop to poll us periodically (time-check) so we observe
     * g_stop and make MoQ progress even when no packets are flowing. */
    if (cb_mode == picoquic_packet_loop_ready && callback_arg) {
        ((picoquic_packet_loop_options_t *)callback_arg)->do_time_check = 1;
        return 0;
    }
    if (cb_mode == picoquic_packet_loop_time_check && callback_arg) {
        packet_loop_time_check_arg_t *tc =
            (packet_loop_time_check_arg_t *)callback_arg;
        if (tc->delta_t > 50000) tc->delta_t = 50000;  /* cap idle wait */
    }

    /* Service the session on every receive/send and time-check tick. */
    if ((cb_mode == picoquic_packet_loop_after_receive ||
         cb_mode == picoquic_packet_loop_after_send ||
         cb_mode == picoquic_packet_loop_time_check) &&
        s->attached && s->conn) {
        uint64_t now = picoquic_get_quic_time(quic);
        moq_pico_wt_service(s->conn, now);
        drain_session_events(s, now);
    }
    return 0;
}

static void usage(const char *prog)
{
    fprintf(stderr,
        "usage: %s --cert <file> --key <file> [--port N] [--path /moq]\n",
        prog);
}

int main(int argc, char **argv)
{
    const char *cert = NULL, *key = NULL;
    int port = 4433;
    const char *path = "/moq";

    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--cert") && i + 1 < argc) cert = argv[++i];
        else if (!strcmp(argv[i], "--key") && i + 1 < argc) key = argv[++i];
        else if (!strcmp(argv[i], "--port") && i + 1 < argc)
            port = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--path") && i + 1 < argc)
            path = argv[++i];
        else { usage(argv[0]); return 2; }
    }
    if (!cert || !key) { usage(argv[0]); return 2; }

    signal(SIGINT, on_sigint);

    server_ctx_t s;
    memset(&s, 0, sizeof(s));
    s.path = path;
    s.path_table[0] = (picohttp_server_path_item_t){
        .path = path, .path_length = strlen(path),
        .path_callback = server_wt_cb, .path_app_ctx = &s};
    s.server_param.path_table = s.path_table;
    s.server_param.path_table_nb = 1;

    /* Server QUIC context: default callback is h3zero_callback, which
     * dispatches WT CONNECT to our path table. */
    uint64_t now = picoquic_current_time();
    picoquic_quic_t *quic = picoquic_create(
        8, cert, key, NULL, PICO_WT_ALPN,
        h3zero_callback, &s.server_param,
        NULL, NULL, NULL, now, NULL, NULL, NULL, 0);
    if (!quic) {
        fprintf(stderr, "[server] picoquic_create failed "
                        "(check --cert/--key)\n");
        return 1;
    }

    picoquic_set_alpn_select_fn_v2(quic,
        picoquic_demo_server_callback_select_alpn);
    picowt_set_default_transport_parameters(quic);

    printf("[server] listening on UDP %d, WebTransport path %s\n",
           port, path);

    int ret = picoquic_packet_loop(quic, port, 0, 0, 0, 0,
                                   server_loop_cb, &s);
    /* A requested shutdown is success: either the loop terminated via
     * our callback, or a signal interrupted the idle socket wait
     * (EINTR → negative return) after g_stop was set. */
    if (g_stop || ret == PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP)
        ret = 0;

    /* Clean shutdown: detach the adapter (clears h3zero callbacks,
     * deregisters), then sessions, then the QUIC context. */
    if (s.conn) moq_pico_wt_conn_destroy(s.conn);
    if (s.session) moq_session_destroy(s.session);
    picoquic_free(quic);

    printf("[server] stopped (ret=%d)\n", ret);
    return ret == 0 ? 0 : 1;
}
