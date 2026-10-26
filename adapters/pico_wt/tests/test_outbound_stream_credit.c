/* Outbound STREAM/FIN/reset frames stay within peer-advertised QUIC credit.
 * Real raw/WT adapters exchange packets in virtual time.
 *
 * Local ID reservation is recorded separately from packet output. Initial
 * limits are transport parameters advertised before handshake/adapter attach.
 * Permitted streams remain unfinished during the blocked phase. Completing
 * them through MoQ generates the provider's real replacement MAX_STREAMS;
 * no provider credit field is assigned and no frame is injected.
 *
 * This tests connection QUIC credit, not WT_MAX_STREAMS session capsules.
 * The WT bootstrap is the small-limit variant of pico_wt_harness_setup;
 * pumping, MoQ handshake and teardown reuse that existing harness.
 * Selectors: raw-uni, raw-bidi, wt-uni, wt-bidi, raw-reset, wt-reset.
 */
#include "pico_wt_harness.h"
#include <moq/moq.h>
#include <moq/picoquic.h>
#include <demoserver.h>
#include <picoquic_internal.h>
#include <picoquic_unified_log.h>
#include <picoquic_utils.h>
#include <inttypes.h>
#include <stdio.h>
#include <string.h>

enum { MAX_STREAMS = 16, WIRE_BYTES = 4096, MAX_ROUNDS = 4096 };
static unsigned failures, prerequisites;
static const char *row;
#define REQUIRE(x) do { if (!(x)) { \
    fprintf(stderr, "PREREQUISITE[%s]:%d: %s\n", row, __LINE__, #x); \
    prerequisites++; goto done; } } while (0)
#define CHECK(x) do { if (!(x)) { \
    fprintf(stderr, "FAIL[%s]:%d: %s\n", row, __LINE__, #x); \
    failures++; goto done; } } while (0)

typedef struct credit_pair {
    pico_wt_harness_t h;
    moq_pq_conn_t *raw_client, *raw_server;
    bool wt, bidi, reset, armed;
    int setup_error, bad;
    picoquic_cnx_t *sender, *receiver;
    uint64_t initial_credit, observed_credit, target_sid;
    unsigned grant_tx, grant_rx, packets, forbidden, frames, fins, resets;
    unsigned same_time_max;
    uint8_t wire[WIRE_BYTES], present[WIRE_BYTES];
    size_t wire_end;
    moq_subscription_t client_sub[MAX_STREAMS], server_sub[MAX_STREAMS];
    moq_subgroup_handle_t group[MAX_STREAMS];
    unsigned requests[MAX_STREAMS], oks[MAX_STREAMS], objects[MAX_STREAMS];
    unsigned setup_client, setup_server;
} credit_pair_t;

static credit_pair_t *active_pair; /* one sequential virtual-time pair */

/* Independent bounded QUIC varint reader for the packet observer. */
static const uint8_t *vi(const uint8_t *p, const uint8_t *end, uint64_t *v)
{
    if (p == end) return NULL;
    size_t n = (size_t)1 << (*p >> 6);
    if ((size_t)(end - p) < n) return NULL;
    *v = *p++ & 63u;
    for (size_t i = 1; i < n; i++) *v = (*v << 8) | *p++;
    return p;
}

static void observe_frames(credit_pair_t *p, picoquic_cnx_t *cnx,
                           bool incoming, const uint8_t *b, size_t len)
{
    if (!p->armed) return;
    const uint8_t *end = b + len;
    while (b < end) {
        const uint8_t *start = b;
        uint64_t type, sid = 0, off = 0, n = 0;
        b = vi(b, end, &type);
        if (!b) { p->bad = 1; return; }
        bool stream = type >= 8 && type <= 15;
        bool reset = type == 4 || type == 0x24;
        if (stream || reset) {
            b = vi(b, end, &sid);
            if (!b) { p->bad = 1; return; }
            if (!incoming && cnx == p->sender &&
                (sid & 3) == (p->target_sid & 3) &&
                sid / 4 + 1 > p->observed_credit) p->forbidden++;
            if (stream) {
                if (type & 4) b = vi(b, end, &off);
                if (b && (type & 2)) b = vi(b, end, &n);
                else if (b) n = (uint64_t)(end - b);
                if (!b || n > (uint64_t)(end - b)) { p->bad = 1; return; }
                if (!incoming && cnx == p->sender && sid == p->target_sid) {
                    p->frames++;
                    if (type & 1) p->fins++;
                    if (off > WIRE_BYTES || n > WIRE_BYTES - off) {
                        p->bad = 1; return;
                    }
                    for (size_t i = 0; i < (size_t)n; i++) {
                        size_t at = (size_t)off + i;
                        if (p->present[at] && p->wire[at] != b[i]) p->bad = 1;
                        p->present[at] = 1;
                        p->wire[at] = b[i];
                    }
                    if (off + n > p->wire_end) p->wire_end = (size_t)(off + n);
                }
            } else if (!incoming && cnx == p->sender && sid == p->target_sid) {
                p->resets++;
            }
        } else if (type == (p->bidi ? 0x12u : 0x13u)) {
            uint64_t grant;
            b = vi(b, end, &grant);
            if (!b) { p->bad = 1; return; }
            if (incoming && cnx == p->sender && grant > p->observed_credit) {
                p->observed_credit = grant;
                p->grant_rx++;
            }
            if (!incoming && cnx == p->receiver && grant > p->initial_credit)
                p->grant_tx++;
        }
        size_t consumed = 0;
        int pure_ack = 0;
        if (picoquic_skip_frame(start, (size_t)(end - start), &consumed, &pure_ack) != 0 ||
            consumed == 0 || consumed > (size_t)(end - start)) { p->bad = 1; return; }
        b = start + consumed;
    }
}

static void log_packet(picoquic_cnx_t *cnx, void *ctx, picoquic_path_t *path,
    int incoming, uint64_t now, picoquic_packet_header *ph,
    const uint8_t *bytes, size_t len)
{
    (void)path; (void)now;
    credit_pair_t *p = ctx;
    if (!incoming || ph->ptype != picoquic_packet_1rtt_protected) return;
    if (ph->offset > len || ph->payload_length > len - ph->offset) { p->bad = 1; return; }
    observe_frames(p, cnx, true, bytes + ph->offset, ph->payload_length);
}

/* Same packet/header boundary as the provider's binlog_outgoing_packet.
 * bytes is the formatted plaintext; send_buffer is the protected packet. */
static void log_out(picoquic_cnx_t *cnx, void *ctx, picoquic_path_t *path,
    uint8_t *bytes, uint64_t seq, size_t pn_len, size_t len,
    uint8_t *send_buffer, size_t send_len, uint64_t now)
{
    (void)path; (void)seq; (void)now;
    credit_pair_t *p = ctx;
    if (!p->armed) return;
    picoquic_packet_header ph;
    picoquic_cnx_t *found = cnx;
    memset(&ph, 0, sizeof(ph));
    if (picoquic_parse_packet_header(cnx->quic, send_buffer, send_len,
            (struct sockaddr *)&cnx->path[0]->first_tuple->local_addr,
            &ph, &found, 0) != 0) { p->bad = 1; return; }
    if (ph.ptype != picoquic_packet_1rtt_protected) return;
    if (ph.pn_offset > len || pn_len > len - ph.pn_offset) { p->bad = 1; return; }
    size_t offset = ph.pn_offset + pn_len;
    p->packets++;
    observe_frames(p, cnx, false, bytes + offset, len - offset);
}

/* The unified logging contract calls every connection callback when ctx is
 * non-NULL. Supply typed no-ops for the events this observer does not use. */
static void log_new(picoquic_cnx_t *c, void *p, void **ctx) { (void)c; *ctx = p; }
static void log_close(picoquic_cnx_t *c, void *p) { (void)c; (void)p; }
static void log_qclose(picoquic_quic_t *q, void *p) { (void)q; (void)p; }
static void log_msg(picoquic_cnx_t *c, void *p, const char *s, va_list a)
{ (void)c; (void)p; (void)s; (void)a; }
static void log_pdu(picoquic_cnx_t *c, void *p, int r, uint64_t t,
    const struct sockaddr *a, const struct sockaddr *b, size_t n, uint64_t id, unsigned char e)
{ (void)c; (void)p; (void)r; (void)t; (void)a; (void)b; (void)n; (void)id; (void)e; }
static void log_drop(picoquic_cnx_t *c, void *p, picoquic_path_t *x,
    picoquic_packet_header *h, size_t n, int e, uint64_t t)
{ (void)c; (void)p; (void)x; (void)h; (void)n; (void)e; (void)t; }
static void log_buffer(picoquic_cnx_t *c, void *p, picoquic_path_t *x,
    picoquic_packet_type_enum type, uint64_t t)
{ (void)c; (void)p; (void)x; (void)type; (void)t; }
static void log_lost(picoquic_cnx_t *c, void *p, picoquic_path_t *x,
    picoquic_packet_type_enum type, uint64_t seq, const char *s,
    picoquic_connection_id_t *id, size_t n, uint64_t t)
{ (void)c; (void)p; (void)x; (void)type; (void)seq; (void)s; (void)id; (void)n; (void)t; }
static void log_alpn(picoquic_cnx_t *c, void *p, int local, const uint8_t *s,
    size_t sn, const uint8_t *a, size_t an, const ptls_iovec_t *list, size_t count)
{ (void)c; (void)p; (void)local; (void)s; (void)sn; (void)a; (void)an; (void)list; (void)count; }
static void log_tp(picoquic_cnx_t *c, void *p, int local, size_t n, uint8_t *b)
{ (void)c; (void)p; (void)local; (void)n; (void)b; }
static void log_ticket(picoquic_cnx_t *c, void *p, uint8_t *b, uint16_t n)
{ (void)c; (void)p; (void)b; (void)n; }
static void log_cc(picoquic_cnx_t *c, void *p, picoquic_path_t *x, uint64_t t)
{ (void)c; (void)p; (void)x; (void)t; }
static picoquic_unified_logging_t observer = {
    .log_quic_close = log_qclose, .log_app_message = log_msg,
    .log_pdu = log_pdu, .log_packet = log_packet, .log_dropped_packet = log_drop,
    .log_buffered_packet = log_buffer, .log_outgoing_packet = log_out,
    .log_packet_lost = log_lost, .log_negotiated_alpn = log_alpn,
    .log_transport_extension = log_tp, .log_picotls_ticket = log_ticket,
    .log_new_connection = log_new, .log_close_connection = log_close,
    .log_cc_dump = log_cc, .log_flush = log_close
};

static int make_session(moq_perspective_t perspective, moq_session_t **out)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), perspective);
    cfg.version = MOQ_VERSION_DRAFT_18;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 32;
    cfg.max_events = 64;
    cfg.max_actions = 64;
    return moq_session_create(&cfg, 0, out);
}

static int raw_attach(moq_session_t *s, picoquic_cnx_t *cnx, moq_pq_conn_t **out)
{
    moq_pq_conn_cfg_t cfg;
    moq_pq_conn_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.session = s; cfg.cnx = cnx; cfg.alloc = moq_alloc_default();
    return moq_pq_conn_create(&cfg, out);
}

static int raw_server(picoquic_cnx_t *cnx, uint64_t sid, uint8_t *b,
    size_t n, picoquic_call_back_event_t e, void *ctx, void *stream)
{
    credit_pair_t *p = ctx;
    (void)sid; (void)b; (void)n; (void)stream;
    if (p->raw_server || (e != picoquic_callback_almost_ready && e != picoquic_callback_ready)) return 0;
    if (make_session(MOQ_PERSPECTIVE_SERVER, &p->h.server_session) != MOQ_OK ||
        raw_attach(p->h.server_session, cnx, &p->raw_server) != 0) {
        p->setup_error = 1; return -1;
    }
    return 0;
}

static int wt_connect_cb(picoquic_cnx_t *cnx, uint8_t *bytes, size_t length,
    picohttp_call_back_event_t event, h3zero_stream_ctx_t *stream, void *ctx)
{
    credit_pair_t *p = active_pair;
    if ((event == picohttp_callback_post_data || event == picohttp_callback_post_fin) &&
        ctx != &p->h.client_wt && ctx != &p->h.server_wt)
        return moq_pico_wt_callback(cnx, bytes, length, event, stream, ctx);
    pico_wt_wt_cb_ctx_t *w = ctx;
    if (event == picohttp_callback_connect) {
        if (!picowt_is_webtransport_connect(stream)) return -1;
        w->h3_ctx = picoquic_get_callback_context(cnx);
        w->ctrl_ctx = stream;
        if (h3zero_declare_stream_prefix(w->h3_ctx, stream->stream_id, wt_connect_cb, w) != 0) return -1;
        w->connect_received = 1;
    } else if (event == picohttp_callback_connect_accepted) w->connect_accepted = 1;
    return 0;
}

static int wt_attach(moq_session_t *s, picoquic_cnx_t *cnx,
    pico_wt_wt_cb_ctx_t *w, moq_pico_wt_conn_t **out)
{
    moq_pico_wt_conn_cfg_t cfg;
    moq_pico_wt_conn_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.session = s; cfg.cnx = cnx; cfg.h3_ctx = w->h3_ctx;
    cfg.ctrl_ctx = w->ctrl_ctx; cfg.alloc = moq_alloc_default();
    return moq_pico_wt_conn_create(&cfg, out);
}

static void service(credit_pair_t *p)
{
    if (p->wt) {
        if (p->h.client_conn) moq_pico_wt_service(p->h.client_conn, p->h.now);
        if (p->h.server_conn) moq_pico_wt_service(p->h.server_conn, p->h.now);
    } else {
        if (p->raw_client) moq_pq_service(p->raw_client, p->h.now);
        if (p->raw_server) moq_pq_service(p->raw_server, p->h.now);
    }
}

static bool healthy(const credit_pair_t *p)
{
    if (p->bad || p->setup_error) return false;
    if (p->wt ? (moq_pico_wt_conn_is_fatal(p->h.client_conn) || moq_pico_wt_conn_is_fatal(p->h.server_conn)) :
        (moq_pq_conn_is_fatal(p->raw_client) || moq_pq_conn_is_fatal(p->raw_server))) return false;
    picoquic_cnx_t *c = p->h.test_ctx->cnx_client, *s = p->h.test_ctx->cnx_server;
    return c && s && !c->local_error && !c->remote_error && !s->local_error && !s->remote_error &&
        moq_session_state(p->h.client_session) == MOQ_SESS_ESTABLISHED &&
        moq_session_state(p->h.server_session) == MOQ_SESS_ESTABLISHED;
}

static void collect(credit_pair_t *p)
{
    moq_event_t ev;
    while (moq_session_poll_events(p->h.server_session, &ev, 1)) {
        if (ev.kind == MOQ_EVENT_SETUP_COMPLETE) p->setup_server++;
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
            moq_bytes_t name = ev.u.subscribe_request.track_name;
            if (name.len != 2 || name.data[0] != 'c' || name.data[1] < 'A' ||
                name.data[1] >= 'A' + MAX_STREAMS) p->bad = 1;
            else {
                unsigned i = name.data[1] - 'A';
                p->requests[i]++;
                p->server_sub[i] = ev.u.subscribe_request.sub;
                moq_accept_subscribe_cfg_t ac;
                moq_accept_subscribe_cfg_init(&ac);
                if (moq_session_accept_subscribe(p->h.server_session, p->server_sub[i], &ac, p->h.now) != MOQ_OK)
                    p->bad = 1;
            }
        }
        moq_event_cleanup(&ev);
    }
    while (moq_session_poll_events(p->h.client_session, &ev, 1)) {
        if (ev.kind == MOQ_EVENT_SETUP_COMPLETE) p->setup_client++;
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_OK) {
            bool found = false;
            for (unsigned i = 0; i < MAX_STREAMS; i++)
                if (moq_subscription_eq(p->client_sub[i], ev.u.subscribe_ok.sub)) {
                    p->oks[i]++; found = true;
                }
            if (!found) p->bad = 1;
        }
        if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) {
            const moq_object_received_event_t *o = &ev.u.object_received;
            if (o->group_id >= MAX_STREAMS || o->object_id != 0 || !o->payload || moq_rcbuf_len(o->payload) != 5)
                p->bad = 1;
            else {
                uint8_t expected[] = {0xa5, 'c', (uint8_t)o->group_id, 0, 0xfe};
                if (memcmp(moq_rcbuf_data(o->payload), expected, sizeof(expected)) != 0) p->bad = 1;
                p->objects[o->group_id]++;
            }
        }
        moq_event_cleanup(&ev);
    }
}

/* Fixed virtual-time windows, with explicit iteration and same-time bounds.
 * This detects repeated immediate wakeups; it is not a CPU benchmark. */
static int pump(credit_pair_t *p, uint64_t duration)
{
    uint64_t limit = p->h.now + duration;
    unsigned same = 0;
    for (unsigned i = 0; i < MAX_ROUNDS && p->h.now < limit; i++) {
        uint64_t before = p->h.now;
        int activity = 0, rc;
        if (p->wt) rc = pico_wt_harness_sim_round(&p->h, limit, &activity);
        else {
            service(p);
            rc = tls_api_one_sim_round(p->h.test_ctx, &p->h.now, limit, &activity);
            service(p);
        }
        if (rc != 0) return -1;
        collect(p);
        if (p->bad || p->setup_error) return -1;
        same = p->h.now == before ? same + 1 : 0;
        if (same > p->same_time_max) p->same_time_max = same;
        if (same > 128) return -1;
    }
    return p->h.now >= limit ? 0 : -1;
}

static int setup(credit_pair_t *p, uint8_t discriminator)
{
    pico_wt_harness_t *h = &p->h;
    picoquic_solution_dir = PICOQUIC_SOURCE_DIR;
    active_pair = p;
    h->version = MOQ_VERSION_DRAFT_18;
    picoquic_connection_id_t cid = {.id = {0x45, 0x63, 0x72, discriminator}, .id_len = 8};
    if (tls_api_init_ctx_ex(&h->test_ctx, PICOQUIC_INTERNAL_TEST_VERSION_1,
            PICOQUIC_TEST_SNI, p->wt ? "h3" : PICOQUIC_TEST_ALPN, &h->now,
            NULL, NULL, 0, 1, 0, &cid) != 0) return -1;
    /* Keep packet observation active beyond the provider's short-log cap. */
    picoquic_set_log_level(h->test_ctx->qclient, 1);
    picoquic_set_log_level(h->test_ctx->qserver, 1);
    picoquic_set_default_idle_timeout(h->test_ctx->qclient, 30000);
    picoquic_set_default_idle_timeout(h->test_ctx->qserver, 30000);
    if (p->wt) {
        picowt_set_default_transport_parameters(h->test_ctx->qserver);
        picowt_set_transport_parameters(h->test_ctx->cnx_client);
    }
    /* Nine WT uni slots cover H3 control/QPACK + MoQ control plus enough
     * completable DATA to trigger replenishment without closing control.
     * Three WT bidis cover CONNECT and two held MoQ requests. */
    picoquic_tp_t tp = *picoquic_get_transport_parameters(h->test_ctx->cnx_client, 1);
    tp.initial_max_stream_id_unidir = p->wt ? 9 : 3;
    tp.is_reset_stream_at_enabled = 1;
    picoquic_set_transport_parameters(h->test_ctx->cnx_client, &tp);
    picoquic_set_default_tp_value(h->test_ctx->qserver,
        picoquic_tp_initial_max_streams_bidi, p->wt ? 3 : 1);
    picoquic_set_default_tp_value(h->test_ctx->qserver, picoquic_tp_reset_stream_at, 1);
    if (picoquic_register_log_functions(h->test_ctx->qclient, &observer, p) != 0 ||
        picoquic_register_log_functions(h->test_ctx->qserver, &observer, p) != 0) return -1;
    picoquic_log_new_connection(h->test_ctx->cnx_client);
    if (p->wt) {
        h->path_table[0] = (picohttp_server_path_item_t){
            .path = "/moq", .path_length = 4, .path_callback = wt_connect_cb,
            .path_app_ctx = &h->server_wt,
            .connect_protocol = H3ZERO_WEBTRANSPORT_H3_PROTOCOL,
            .connect_protocol_length = sizeof(H3ZERO_WEBTRANSPORT_H3_PROTOCOL) - 1};
        h->server_param.path_table = h->path_table;
        h->server_param.path_table_nb = 1;
        if (picowt_prepare_client_cnx(h->test_ctx->qclient, NULL,
                &h->test_ctx->cnx_client, &h->client_h3_ctx, &h->client_ctrl_ctx,
                h->now, PICOQUIC_TEST_SNI) != 0) return -1;
        /* prepare_client_cnx reapplies WT's >=63 defaults. Restore the test
         * TP before handshake or adapter attach, never after advertisement. */
        picoquic_set_transport_parameters(h->test_ctx->cnx_client, &tp);
        h->client_wt.h3_ctx = h->client_h3_ctx;
        h->client_wt.ctrl_ctx = h->client_ctrl_ctx;
        if (picowt_connect(h->test_ctx->cnx_client, h->client_h3_ctx, h->client_ctrl_ctx,
                PICOQUIC_TEST_SNI, "/moq", wt_connect_cb, &h->client_wt, NULL) != 0) return -1;
        picoquic_set_alpn_select_fn_v2(h->test_ctx->qserver, picoquic_demo_server_callback_select_alpn);
        picoquic_set_default_callback(h->test_ctx->qserver, h3zero_callback, &h->server_param);
    } else {
        if (make_session(MOQ_PERSPECTIVE_CLIENT, &h->client_session) != MOQ_OK ||
            raw_attach(h->client_session, h->test_ctx->cnx_client, &p->raw_client) != 0) return -1;
        picoquic_set_default_callback(h->test_ctx->qserver, raw_server, p);
    }
    if (picoquic_start_client_cnx(h->test_ctx->cnx_client) != 0 ||
        tls_api_connection_loop(h->test_ctx, &h->loss_mask, 0, &h->now) != 0) return -1;
    if (p->wt) {
        uint64_t limit = h->now + 2000000;
        for (unsigned i = 0; i < MAX_ROUNDS && h->now < limit &&
                !(h->client_wt.connect_accepted && h->server_wt.connect_received); i++) {
            int activity = 0;
            if (tls_api_one_sim_round(h->test_ctx, &h->now, limit, &activity) != 0) return -1;
        }
        if (!h->client_wt.connect_accepted || !h->server_wt.connect_received) return -1;
        if (make_session(MOQ_PERSPECTIVE_CLIENT, &h->client_session) != MOQ_OK ||
            make_session(MOQ_PERSPECTIVE_SERVER, &h->server_session) != MOQ_OK ||
            wt_attach(h->client_session, h->test_ctx->cnx_client, &h->client_wt, &h->client_conn) != 0 ||
            wt_attach(h->server_session, h->test_ctx->cnx_server, &h->server_wt, &h->server_conn) != 0)
            return -1;
        if (pico_wt_harness_handshake(h) != 0) return -1;
    } else {
        if (p->setup_error || !p->raw_server) return -1;
        if (moq_session_start(h->client_session, h->now) != MOQ_OK ||
            moq_session_start(h->server_session, h->now) != MOQ_OK || pump(p, 500000) != 0)
            return -1;
        if (p->setup_client != 1 || p->setup_server != 1) return -1;
    }
    return healthy(p) ? 0 : -1;
}

static int subscribe(credit_pair_t *p, unsigned i)
{
    uint8_t name[] = {'c', (uint8_t)('A' + i)};
    moq_bytes_t ns = MOQ_BYTES_LITERAL("issue45-credit");
    moq_subscribe_cfg_t cfg;
    moq_subscribe_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.track_namespace = (moq_namespace_t){&ns, 1};
    cfg.track_name = (moq_bytes_t){name, sizeof(name)};
    cfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    return moq_session_subscribe(p->h.client_session, &cfg, p->h.now, &p->client_sub[i]);
}

static int publish(credit_pair_t *p, unsigned i, bool data)
{
    moq_subgroup_cfg_t cfg;
    moq_subgroup_cfg_init(&cfg);
    cfg.group_id = i;
    int rc = moq_session_open_subgroup(p->h.server_session, p->server_sub[0], &cfg, p->h.now, &p->group[i]);
    if (rc != MOQ_OK || !data) return rc;
    const uint8_t bytes[] = {0xa5, 'c', (uint8_t)i, 0, 0xfe};
    moq_rcbuf_t *payload = NULL;
    rc = moq_rcbuf_create(moq_alloc_default(), bytes, sizeof(bytes), &payload);
    if (rc == MOQ_OK) rc = moq_session_write_object(p->h.server_session, p->group[i], 0, payload, p->h.now);
    moq_rcbuf_decref(payload);
    return rc;
}

static void run(bool wt, bool bidi, bool reset, uint8_t discriminator)
{
    credit_pair_t p;
    memset(&p, 0, sizeof(p));
    p.wt = wt; p.bidi = bidi; p.reset = reset;
    unsigned permitted = 0;
    uint64_t reserved_next = 0;
    REQUIRE(setup(&p, discriminator) == 0);
    p.sender = bidi ? p.h.test_ctx->cnx_client : p.h.test_ctx->cnx_server;
    p.receiver = bidi ? p.h.test_ctx->cnx_server : p.h.test_ctx->cnx_client;
    p.initial_credit = bidi ? p.sender->max_streams_bidir_remote : p.sender->max_streams_unidir_remote;
    REQUIRE(p.initial_credit == (uint64_t)(bidi ? (wt ? 3 : 1) : (wt ? 9 : 3)));
    if (!bidi) {
        REQUIRE(subscribe(&p, 0) == MOQ_OK && pump(&p, 200000) == 0);
        REQUIRE(p.requests[0] == 1 && p.oks[0] == 1);
    }
    uint64_t next = picoquic_get_next_local_stream_id(p.sender, !bidi);
    uint64_t overhead = next / 4;
    REQUIRE(overhead < p.initial_credit);
    REQUIRE(p.initial_credit - overhead < MAX_STREAMS);
    permitted = (unsigned)(p.initial_credit - overhead);
    /* Enough non-control streams to trigger this provider's own replenishment. */
    REQUIRE(permitted > p.initial_credit / 2);
    for (unsigned i = 0; i < permitted; i++) {
        REQUIRE((bidi ? subscribe(&p, i) : publish(&p, i, true)) == MOQ_OK);
        REQUIRE(pump(&p, 100000) == 0 && healthy(&p));
        REQUIRE(bidi ? p.requests[i] == 1 && p.oks[i] == 1 : p.objects[i] == 1);
    }
    p.target_sid = picoquic_get_next_local_stream_id(p.sender, !bidi);
    REQUIRE(p.target_sid / 4 + 1 == p.initial_credit + 1);
    REQUIRE((bidi ? p.sender->max_streams_bidir_remote : p.sender->max_streams_unidir_remote) == p.initial_credit);
    p.observed_credit = p.initial_credit;
    p.armed = true;
    CHECK((bidi ? subscribe(&p, permitted) : publish(&p, permitted, !reset)) == MOQ_OK);
    service(&p); /* offer the open before FIN/reset; do not send a packet yet */
    if (!bidi) CHECK((reset ? moq_session_reset_subgroup(p.h.server_session, p.group[permitted], 77, p.h.now) :
        moq_session_close_subgroup(p.h.server_session, p.group[permitted], p.h.now)) == MOQ_OK);
    service(&p);
    reserved_next = picoquic_get_next_local_stream_id(p.sender, !bidi);
    CHECK(reserved_next == p.target_sid || reserved_next == p.target_sid + 4);
    CHECK(pump(&p, 100000) == 0 && healthy(&p));
    CHECK(picoquic_get_next_local_stream_id(p.sender, !bidi) == reserved_next);
    CHECK(p.forbidden == 0 && p.frames == 0 && p.fins == 0 && p.resets == 0);
    CHECK(p.grant_tx == 0 && p.grant_rx == 0 && p.observed_credit == p.initial_credit);
    for (unsigned i = 0; i < permitted; i++) {
        uint64_t sid = next + 4 * i;
        const picoquic_stream_head_t *sent = picoquic_find_stream(p.sender, sid);
        const picoquic_stream_head_t *received = picoquic_find_stream(p.receiver, sid);
        CHECK(sent && !sent->fin_sent && !sent->reset_sent);
        CHECK(received && !received->fin_received && !received->reset_received);
    }
    CHECK(picoquic_find_stream(p.receiver, p.target_sid) == NULL);
    CHECK(bidi ? p.requests[permitted] == 0 && p.oks[permitted] == 0 : p.objects[permitted] == 0);
    printf("%s held: initial=%"PRIu64" overhead=%"PRIu64" permitted=%u target=%"PRIu64
           " reserved-next=%"PRIu64" frames=%u fin=%u reset=%u\n",
           row, p.initial_credit, overhead, permitted, p.target_sid, reserved_next, p.frames, p.fins, p.resets);

    /* This is the only credit release: real FIN/DONE via the adapters, then
     * the peer's real MAX_STREAMS packet. No permission field is changed. */
    for (unsigned i = 0; i < permitted; i++) {
        if (bidi) {
            moq_done_subscribe_cfg_t done;
            moq_done_subscribe_cfg_init(&done);
            CHECK(moq_session_done_subscribe(p.h.server_session, p.server_sub[i], &done, p.h.now) == MOQ_OK);
        } else CHECK(moq_session_close_subgroup(p.h.server_session, p.group[i], p.h.now) == MOQ_OK);
    }
    CHECK(pump(&p, 500000) == 0 && healthy(&p));
    CHECK(p.grant_tx > 0 && p.grant_rx > 0 && p.observed_credit > p.initial_credit);
    CHECK((bidi ? p.sender->max_streams_bidir_remote : p.sender->max_streams_unidir_remote) == p.observed_credit);
    CHECK(picoquic_get_next_local_stream_id(p.sender, !bidi) == p.target_sid + 4);
    CHECK(p.forbidden == 0 && p.frames > 0);
    CHECK(bidi ? p.requests[permitted] == 1 && p.oks[permitted] == 1 : p.objects[permitted] == (reset ? 0u : 1u));
    CHECK(reset ? p.resets > 0 && p.fins == 0 : p.resets == 0);
    if (!bidi && !reset) CHECK(p.fins > 0);
    CHECK(p.wire_end > 0);
    for (size_t i = 0; i < p.wire_end; i++) CHECK(p.present[i]);
    if (wt) {
        uint64_t type, session;
        const uint8_t *b = vi(p.wire, p.wire + p.wire_end, &type);
        CHECK(b != NULL);
        b = vi(b, p.wire + p.wire_end, &session);
        CHECK(b != NULL && type == (uint64_t)(bidi ? h3zero_frame_webtransport_stream : h3zero_stream_type_webtransport));
        CHECK(session == p.h.client_ctrl_ctx->stream_id);
        /* MoQ must follow the single WT preamble; another WT preamble would
         * not decode as this established MoQ request/subgroup at the peer. */
        CHECK(b < p.wire + p.wire_end);
    }
    CHECK(pump(&p, 100000) == 0 && healthy(&p));
    CHECK(bidi ? p.requests[permitted] == 1 && p.oks[permitted] == 1 : p.objects[permitted] == (reset ? 0u : 1u));
    CHECK(picoquic_get_next_local_stream_id(p.sender, !bidi) == p.target_sid + 4);
    CHECK(p.forbidden == 0);
    printf("%s released: grant=%"PRIu64" MAX_STREAMS tx/rx=%u/%u target frames/fin/reset=%u/%u/%u"
           " unique-bytes=%zu max-same-time-rounds=%u\n", row, p.observed_credit,
           p.grant_tx, p.grant_rx, p.frames, p.fins, p.resets, p.wire_end, p.same_time_max);
done:
    p.armed = false;
    if (p.raw_client) moq_pq_conn_destroy(p.raw_client);
    if (p.raw_server) moq_pq_conn_destroy(p.raw_server);
    pico_wt_harness_cleanup(&p.h);
    active_pair = NULL;
}

int main(int argc, char **argv)
{
    static const struct { const char *name; bool wt, bidi, reset; } rows[] = {
        {"raw-uni", false, false, false}, {"raw-bidi", false, true, false},
        {"wt-uni", true, false, false}, {"wt-bidi", true, true, false},
        {"raw-reset", false, false, true}, {"wt-reset", true, false, true}
    };
    if (argc > 2) return 2;
    unsigned selected = 0;
    printf("provider source: %s\n", PICOQUIC_SOURCE_DIR);
    for (unsigned i = 0; i < sizeof(rows) / sizeof(rows[0]); i++) {
        if (argc == 2 && strcmp(argv[1], rows[i].name) != 0) continue;
        row = rows[i].name;
        selected++;
        run(rows[i].wt, rows[i].bidi, rows[i].reset, (uint8_t)(0x90 + i));
    }
    printf("%u rows, %u prerequisites, %u failures\n", selected, prerequisites, failures);
    return !selected || prerequisites ? 2 : failures ? 1 : 0;
}
