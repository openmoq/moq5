/*
 * Internal test/demo harness for the picoquic WebTransport adapter.
 * See pico_wt_harness.h.
 */

#include "pico_wt_harness.h"
#include <moq/moq.h>
#include <demoserver.h>
#include <picoquic_utils.h>

#include <stdbool.h>
#include <string.h>

#ifndef PICOQUIC_SOURCE_DIR
#define PICOQUIC_SOURCE_DIR "."
#endif

/* The harness's own control-stream contexts (the live harness's client and
 * server). Anything else reaching pico_wt_harness_wt_cb is an attached
 * adapter's context. */
static const void *g_harness_wt_ctxs[2];

static bool harness_owns_ctx(const void *ctx)
{
    return ctx != NULL &&
           (ctx == g_harness_wt_ctxs[0] || ctx == g_harness_wt_ctxs[1]);
}

int pico_wt_harness_wt_cb(picoquic_cnx_t *cnx, uint8_t *bytes,
    size_t length, picohttp_call_back_event_t event,
    h3zero_stream_ctx_t *stream_ctx, void *app_ctx)
{
    /* Body bytes on the server's CONNECT stream (close capsules, a capsule's
     * FIN) arrive here: h3zero's server body branch calls the path-table
     * callback, but with the stream's own context, which the attached adapter
     * rebound to itself. Forward them to the adapter, as the managed server
     * does. */
    if ((event == picohttp_callback_post_data ||
         event == picohttp_callback_post_fin) && !harness_owns_ctx(app_ctx))
        return moq_pico_wt_callback(cnx, bytes, length, event, stream_ctx,
                                    app_ctx);

    pico_wt_wt_cb_ctx_t *w = (pico_wt_wt_cb_ctx_t *)app_ctx;
    (void)bytes; (void)length; (void)cnx;
    switch (event) {
    case picohttp_callback_connect:
        if (!picowt_is_webtransport_connect(stream_ctx))
            return -1;
        w->h3_ctx = (h3zero_callback_ctx_t *)
            picoquic_get_callback_context(cnx);
        w->ctrl_ctx = stream_ctx;
        if (h3zero_declare_stream_prefix(w->h3_ctx,
                stream_ctx->stream_id, pico_wt_harness_wt_cb, w) != 0)
            return -1;
        w->connect_received = 1;
        break;
    case picohttp_callback_connect_accepted:
        w->connect_accepted = 1;
        break;
    default:
        break;
    }
    return 0;
}

static int harness_create_session(moq_perspective_t persp,
                                   uint32_t request_capacity,
                                   uint64_t goaway_timeout_us,
                                   uint32_t max_events,
                                   moq_version_t version,
                                   uint32_t max_fills,
                                   moq_session_t **out)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), persp);
    cfg.send_request_capacity = 1;
    cfg.initial_request_capacity = request_capacity;
    cfg.goaway_timeout_us = goaway_timeout_us;
    if (max_events) cfg.max_events = max_events;
    cfg.version = version;
    if (max_fills) cfg.max_fills = max_fills;
    return moq_session_create(&cfg, 0, out);
}

/* An attaching caller binds a session whose profile requires it (draft-22)
 * before starting it: inside a WebTransport session, handshake complete,
 * QUIC DATAGRAM from both sides' transport parameters with sending also
 * needing the peer's HTTP/3 datagram setting, and the authority/path the
 * CONNECT named. Profiles without requirements are left alone. */
static int harness_bind_session(moq_session_t *session, picoquic_cnx_t *cnx,
                                const h3zero_callback_ctx_t *h3)
{
    if (moq_session_binding_requirements(session) == 0) return 0;
    picoquic_tp_t const *local = picoquic_get_transport_parameters(cnx, 1);
    picoquic_tp_t const *remote = picoquic_get_transport_parameters(cnx, 0);
    uint32_t supported = MOQ_BINDING_HANDSHAKE_COMPLETE;
    bool local_dg = local && local->max_datagram_frame_size > 0;
    bool peer_dg = remote && remote->max_datagram_frame_size > 0;
    bool peer_h3_dg = h3 && h3->settings.settings_received && h3->settings.h3_datagram;
    if (local_dg) supported |= MOQ_BINDING_LOCAL_QUIC_DATAGRAM | MOQ_BINDING_DATAGRAM_RECEIVE;
    if (peer_dg) supported |= MOQ_BINDING_PEER_QUIC_DATAGRAM;
    if (peer_dg && peer_h3_dg) supported |= MOQ_BINDING_DATAGRAM_SEND;
    static const uint8_t authority[] = "test.example:443";
    static const uint8_t path[] = "/moq";
    moq_transport_binding_t b;
    moq_transport_binding_init(&b);
    b.kind = MOQ_BINDING_WEBTRANSPORT_H3;
    b.facts_known = MOQ_BINDING_FACTS_ALL;
    b.facts_supported = supported;
    b.target_present = MOQ_BINDING_TARGET_COMPLETE;
    b.authority.data = authority; b.authority.len = sizeof(authority) - 1;
    b.path_query.data = path; b.path_query.len = sizeof(path) - 1;
    return moq_session_bind_transport(session, &b, sizeof(b)) == MOQ_OK ? 0 : -1;
}

static pico_wt_harness_owner_t harness_owner(h3zero_callback_ctx_t *h3_ctx,
                                            const h3zero_stream_ctx_t *ctrl)
{
    pico_wt_harness_owner_t o = {0};
    h3zero_stream_prefix_t *pfx = h3zero_find_stream_prefix(h3_ctx,
                                                            ctrl->stream_id);
    if (pfx) { o.prefix_cb = pfx->function_call; o.prefix_ctx = pfx->function_ctx; }
    o.ctrl_cb = ctrl->path_callback;
    o.ctrl_ctx = ctrl->path_callback_ctx;
    return o;
}

static int harness_attach_adapter(moq_session_t *session,
                                   picoquic_cnx_t *cnx,
                                   h3zero_callback_ctx_t *h3_ctx,
                                   h3zero_stream_ctx_t *ctrl_ctx,
                                   moq_pico_wt_conn_t **out)
{
    moq_pico_wt_conn_cfg_t cfg;
    moq_pico_wt_conn_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.session = session;
    cfg.cnx = cnx;
    cfg.h3_ctx = h3_ctx;
    cfg.ctrl_ctx = ctrl_ctx;
    cfg.alloc = moq_alloc_default();
    return moq_pico_wt_conn_create(&cfg, out);
}

int pico_wt_harness_setup(pico_wt_harness_t *h,
                          const pico_wt_harness_cfg_t *cfg)
{
    memset(h, 0, sizeof(*h));
    picoquic_solution_dir = PICOQUIC_SOURCE_DIR;
    g_harness_wt_ctxs[0] = &h->client_wt;
    g_harness_wt_ctxs[1] = &h->server_wt;

    uint32_t cap = cfg->request_capacity ? cfg->request_capacity : 16;

    picoquic_connection_id_t cid = {
        {0x77, 0x74, 0xba, cfg->cid_byte, 0, 0, 0, 0}, 8};

    h->path_table[0] = (picohttp_server_path_item_t){
        .path = "/moq", .path_length = 4,
        .path_callback = pico_wt_harness_wt_cb, .path_app_ctx = &h->server_wt,
        .connect_protocol = H3ZERO_WEBTRANSPORT_H3_PROTOCOL,
        .connect_protocol_length = sizeof(H3ZERO_WEBTRANSPORT_H3_PROTOCOL) - 1};
    h->server_param.path_table = h->path_table;
    h->server_param.path_table_nb = 1;

    h->phase = PICO_WT_HARNESS_TRANSPORT;
    if (tls_api_init_ctx_ex(&h->test_ctx,
            PICOQUIC_INTERNAL_TEST_VERSION_1,
            PICOQUIC_TEST_SNI, "h3", &h->now,
            NULL, NULL, 0, 1, 0, &cid) != 0)
        return -1;

    picoquic_set_default_idle_timeout(h->test_ctx->qclient, 30000);
    picoquic_set_default_idle_timeout(h->test_ctx->qserver, 30000);
    picowt_set_default_transport_parameters(h->test_ctx->qserver);
    picowt_set_transport_parameters(h->test_ctx->cnx_client);

    if (picowt_prepare_client_cnx(h->test_ctx->qclient, NULL,
            &h->test_ctx->cnx_client, &h->client_h3_ctx,
            &h->client_ctrl_ctx, h->now, PICOQUIC_TEST_SNI) != 0)
        return -1;

    h->client_wt.h3_ctx = h->client_h3_ctx;
    h->client_wt.ctrl_ctx = h->client_ctrl_ctx;
    if (picowt_connect(h->test_ctx->cnx_client, h->client_h3_ctx,
            h->client_ctrl_ctx, PICOQUIC_TEST_SNI, "/moq",
            pico_wt_harness_wt_cb, &h->client_wt, NULL) != 0)
        return -1;

    picoquic_set_alpn_select_fn_v2(h->test_ctx->qserver,
        picoquic_demo_server_callback_select_alpn);
    picoquic_set_default_callback(h->test_ctx->qserver,
        h3zero_callback, &h->server_param);

    if (picoquic_start_client_cnx(h->test_ctx->cnx_client) != 0)
        return -1;

    h->loss_mask = 0;
    if (tls_api_connection_loop(h->test_ctx, &h->loss_mask, 0,
                                 &h->now) != 0)
        return -1;

    /* Drive the WT CONNECT to acceptance on both ends. */
    h->phase = PICO_WT_HARNESS_CONNECT;
    for (int i = 0; i < 2048; i++) {
        if (h->server_wt.connect_received &&
            h->client_wt.connect_accepted)
            break;
        int wa = 0;
        tls_api_one_sim_round(h->test_ctx, &h->now, h->now + 25000, &wa);
    }
    if (!h->server_wt.connect_received || !h->client_wt.connect_accepted)
        return -1;

    h->version = cfg->version;
    h->phase = PICO_WT_HARNESS_SESSIONS;
    if (harness_create_session(MOQ_PERSPECTIVE_CLIENT, cap, 0,
                               cfg->max_events, cfg->version, 0,
                               &h->client_session) != 0)
        return -1;
    if (harness_create_session(MOQ_PERSPECTIVE_SERVER, cap,
                               cfg->server_goaway_timeout_us,
                               cfg->max_events, cfg->version,
                               cfg->server_max_fills,
                               &h->server_session) != 0)
        return -1;

    h->phase = PICO_WT_HARNESS_CLIENT_ATTACH;
    h->pre_attach = harness_owner(h->client_h3_ctx, h->client_ctrl_ctx);
    if (harness_attach_adapter(h->client_session, h->test_ctx->cnx_client,
            h->client_h3_ctx, h->client_ctrl_ctx, &h->client_conn) != 0)
        return -1;
    h->phase = PICO_WT_HARNESS_SERVER_ATTACH;
    if (harness_attach_adapter(h->server_session, h->test_ctx->cnx_server,
            h->server_wt.h3_ctx, h->server_wt.ctrl_ctx,
            &h->server_conn) != 0)
        return -1;

    h->phase = PICO_WT_HARNESS_READY;
    return 0;
}

static bool harness_owner_eq(const pico_wt_harness_owner_t *a,
                             const pico_wt_harness_owner_t *b)
{
    return a->prefix_cb == b->prefix_cb && a->prefix_ctx == b->prefix_ctx &&
           a->ctrl_cb == b->ctrl_cb && a->ctrl_ctx == b->ctrl_ctx;
}

bool pico_wt_harness_refused_at_attach(const pico_wt_harness_t *h,
                                       int setup_rc)
{
    if (setup_rc != -1 || h->phase != PICO_WT_HARNESS_CLIENT_ATTACH)
        return false;
    if (!h->test_ctx || !h->client_wt.connect_accepted ||
        !h->server_wt.connect_received || !h->client_session ||
        !h->server_session || h->client_conn || h->server_conn)
        return false;
    pico_wt_harness_owner_t now = harness_owner(h->client_h3_ctx,
                                                h->client_ctrl_ctx);
    return now.prefix_cb != NULL && harness_owner_eq(&now, &h->pre_attach);
}

typedef struct {
    unsigned calls;   /* alloc + realloc requests made through this allocator */
} harness_count_alloc_t;

static void *harness_count_alloc(size_t n, void *ctx)
{
    ((harness_count_alloc_t *)ctx)->calls++;
    return moq_alloc_default()->alloc(n, moq_alloc_default()->ctx);
}
static void *harness_count_realloc(void *p, size_t old, size_t n, void *ctx)
{
    ((harness_count_alloc_t *)ctx)->calls++;
    return moq_alloc_default()->realloc(p, old, n, moq_alloc_default()->ctx);
}
static void harness_count_free(void *p, size_t n, void *ctx)
{
    (void)ctx;
    moq_alloc_default()->free(p, n, moq_alloc_default()->ctx);
}

const char *pico_wt_harness_check_attach_refusal(pico_wt_harness_t *h,
                                                 int setup_rc)
{
    if (setup_rc != -1) return "setup did not fail";
    if (h->phase != PICO_WT_HARNESS_CLIENT_ATTACH)
        return "setup failed before or after the client constructor";
    if (!pico_wt_harness_refused_at_attach(h, setup_rc))
        return "a prerequisite is missing or a callback was taken";

    harness_count_alloc_t counts = {0};
    const moq_alloc_t a = { .alloc = harness_count_alloc,
                            .realloc = harness_count_realloc,
                            .free = harness_count_free, .ctx = &counts };
    moq_pico_wt_conn_cfg_t cfg;
    moq_pico_wt_conn_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.session = h->client_session;
    cfg.cnx = h->test_ctx->cnx_client;
    cfg.h3_ctx = h->client_h3_ctx;
    cfg.ctrl_ctx = h->client_ctrl_ctx;
    cfg.alloc = &a;
    moq_pico_wt_conn_t *out = (moq_pico_wt_conn_t *)&counts; /* stale value */
    if (moq_pico_wt_conn_create(&cfg, &out) != -1) {
        if (out != NULL && out != (moq_pico_wt_conn_t *)&counts)
            moq_pico_wt_conn_destroy(out);
        return "constructor did not return -1";
    }
    if (out != NULL) return "refusal left *out set";
    if (counts.calls != 0) return "refusal allocated";
    pico_wt_harness_owner_t now = harness_owner(h->client_h3_ctx,
                                                h->client_ctrl_ctx);
    if (!harness_owner_eq(&now, &h->pre_attach))
        return "refusal took over a WT callback";

    /* The same inputs with a supported version attach. */
    moq_session_t *d16 = NULL;
    if (harness_create_session(MOQ_PERSPECTIVE_CLIENT, 16, 0, 0,
                               MOQ_VERSION_DRAFT_16, 0, &d16) != 0)
        return "draft-16 session refused";
    cfg.session = d16;
    int rc = moq_pico_wt_conn_create(&cfg, &out);
    bool took = false;
    if (rc == 0 && out != NULL) {
        now = harness_owner(h->client_h3_ctx, h->client_ctrl_ctx);
        took = now.prefix_ctx == (void *)out && now.ctrl_ctx == (void *)out;
        moq_pico_wt_conn_destroy(out);
    }
    moq_session_destroy(d16);
    if (rc != 0 || out == NULL)
        return "the same inputs refuse a supported version";
    return took ? NULL : "supported attach did not take the callbacks";
}

int pico_wt_harness_sim_round(pico_wt_harness_t *h,
                              uint64_t time_limit, int *was_active)
{
    if (h->client_conn) moq_pico_wt_service(h->client_conn, h->now);
    if (h->server_conn) moq_pico_wt_service(h->server_conn, h->now);

    int wa = 0;
    int rc = tls_api_one_sim_round(h->test_ctx, &h->now, time_limit, &wa);

    if (h->client_conn) moq_pico_wt_service(h->client_conn, h->now);
    if (h->server_conn) moq_pico_wt_service(h->server_conn, h->now);

    if (was_active) *was_active = wa;
    return rc;
}

int pico_wt_harness_pump(pico_wt_harness_t *h, uint64_t ms)
{
    uint64_t limit = h->now + ms * 1000;
    int inactive = 0;
    for (int i = 0; i < 100000; i++) {
        if (h->now > limit) return 0;
        int wa = 0;
        int rc = pico_wt_harness_sim_round(h, limit, &wa);
        if (rc != 0) return rc;
        if (!wa) {
            if (++inactive > 10) return 0;
        } else {
            inactive = 0;
        }
    }
    return 0;
}

int pico_wt_harness_pump_until_closed(pico_wt_harness_t *h, uint64_t ms,
                                      moq_pico_wt_conn_t *target)
{
    uint64_t limit = h->now + ms * 1000;
    for (int i = 0; i < 100000; i++) {
        if (h->now > limit) return 0;
        if (moq_pico_wt_conn_is_closed(target)) return 0;
        int wa = 0;
        int rc = pico_wt_harness_sim_round(h, limit, &wa);
        if (rc != 0) return rc;
    }
    return 0;
}

int pico_wt_harness_handshake(pico_wt_harness_t *h)
{
    /* Profiles that must be told how they are carried are bound first. */
    if (harness_bind_session(h->client_session, h->test_ctx->cnx_client,
                             h->client_wt.h3_ctx) != 0 ||
        harness_bind_session(h->server_session, h->test_ctx->cnx_server,
                             h->server_wt.h3_ctx) != 0)
        return -1;
    moq_session_start(h->client_session, h->now);
    /* Uni-control profiles (draft-18, draft-22) are a symmetric handshake
     * over a unidirectional control-channel pair: both endpoints start and
     * send SETUP. Draft-16 starts the client alone; the server is reactive. */
    if (h->version == MOQ_VERSION_DRAFT_18 || h->version == MOQ_VERSION_DRAFT_22)
        moq_session_start(h->server_session, h->now);
    moq_pico_wt_service(h->client_conn, h->now);

    int cd = 0, sd = 0;
    uint64_t tl = h->now + 5000000;
    for (int i = 0; i < 100000 && !(cd && sd); i++) {
        if (h->now > tl) break;
        int wa = 0;
        if (pico_wt_harness_sim_round(h, tl, &wa) != 0) break;
        moq_event_t ev;
        while (moq_session_poll_events(h->client_session, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SETUP_COMPLETE) cd = 1;
            moq_event_cleanup(&ev);
        }
        while (moq_session_poll_events(h->server_session, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SETUP_COMPLETE) sd = 1;
            moq_event_cleanup(&ev);
        }
    }
    if (!(cd && sd)) return -1;

    /* Drain any residual events on both ends. */
    moq_event_t ev;
    while (moq_session_poll_events(h->client_session, &ev, 1) > 0)
        moq_event_cleanup(&ev);
    while (moq_session_poll_events(h->server_session, &ev, 1) > 0)
        moq_event_cleanup(&ev);
    return 0;
}

void pico_wt_harness_cleanup(pico_wt_harness_t *h)
{
    /* Adapters first: each detaches from picoquic/h3zero (clears
     * callbacks, deregisters, deletes the stream prefix) so no stale
     * callback can reach freed state during the rest of teardown. */
    if (h->client_conn) { moq_pico_wt_conn_destroy(h->client_conn);
                          h->client_conn = NULL; }
    if (h->server_conn) { moq_pico_wt_conn_destroy(h->server_conn);
                          h->server_conn = NULL; }
    if (h->client_session) { moq_session_destroy(h->client_session);
                             h->client_session = NULL; }
    if (h->server_session) { moq_session_destroy(h->server_session);
                             h->server_session = NULL; }
    if (h->test_ctx) { tls_api_delete_ctx(h->test_ctx);
                       h->test_ctx = NULL; }
}
