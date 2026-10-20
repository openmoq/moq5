/*
 * Reliable resets over the REAL picoquic transport in the deterministic
 * simulator (tls_api): genuine QUIC packets and stream ids through the raw
 * adapter, observed from the receiving application.
 *
 * A data stream opened and reset in the same pass is the case the plain
 * reset loses: the stream header is still in the adapter's send queue when
 * the reset arrives, a plain RESET_STREAM abandons it (RFC 9000 §3.1), and
 * the receiver never learns which stream ended. With reset_stream_at
 * advertised by both endpoints the adapter sends RESET_STREAM_AT with the
 * header as its reliable size, so the header is delivered first.
 *
 * Two receiving-side observations, each run over several connection ids
 * (seeds) with and without the extension, the delivery rate printed:
 *
 *   done_count (draft-16 and draft-22): the publisher opens a subgroup,
 *     writes an object, resets it and ends the subscription with
 *     PUBLISH_DONE counting that one stream. The subscriber surfaces the
 *     done once it has accounted for the stream -- at once when the reset
 *     kept the header, only at its done-wait timeout otherwise.
 *   failed_fill (draft-22): a fill the publisher's pool cannot hold fails on
 *     the wire as a FETCH_HEADER followed by a reset (§3.4.1). The
 *     subscriber ends that fill only if the header arrived.
 *
 * Pinned: with the extension every run delivers; without it the adapter
 * falls back to a plain reset and the connection stays healthy.
 */

#include <moq/moq.h>
#include <moq/picoquic.h>

#include <picoquictest_internal.h>
#include <picoquic_utils.h>

#include "../../../tests/unit/test_binding_support.h"
#include "support/pq_attach_refusal.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int failures = 0;
static const char *scenario = "reliable_reset";
#define CHECK(cond) do { \
    if (!(cond)) { \
        fprintf(stderr, "FAIL[%s]: %s:%d: %s\n", scenario, __FILE__, \
                __LINE__, #cond); \
        failures++; \
    } } while (0)

#define RUNS 10

/* The setup step a sim pair last entered; a failed setup stops at it. */
typedef enum {
    SIM_TRANSPORT = 1,   /* test QUIC context and client connection */
    SIM_SESSION,         /* client MoQ session */
    SIM_ATTACH,          /* client adapter constructor */
    SIM_CONNECT,         /* QUIC handshake and server attach */
    SIM_READY
} sim_phase_t;

typedef struct {
    picoquic_test_tls_api_ctx_t *test_ctx;
    uint64_t now;
    uint64_t loss;
    moq_version_t version;
    sim_phase_t phase;
    picoquic_stream_data_cb_fn pre_attach_cb;   /* client callback owner */
    void *pre_attach_ctx;                       /* before the constructor */
    uint32_t server_fills;

    moq_session_t *client_session;
    moq_session_t *server_session;
    moq_pq_conn_t *client_conn;
    moq_pq_conn_t *server_conn;
    int            server_create_failed;
    unsigned       tracks;
} pq_sim_t;

static int sim_create_session(pq_sim_t *s, moq_perspective_t persp, moq_session_t **out)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), persp);
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 16;
    cfg.version = s->version;
    cfg.max_fills = persp == MOQ_PERSPECTIVE_SERVER ? s->server_fills : 4;
    /* Long enough that a done waiting on an unaccounted stream is plainly
     * distinguishable from one surfaced at once. */
    cfg.done_wait_timeout_us = 10000000;
    if (moq_session_create(&cfg, 0, out) != MOQ_OK) return -1;
    return test_bind_if_required(*out) == MOQ_OK ? 0 : -1;
}

static int sim_attach_conn(moq_session_t *session, picoquic_cnx_t *cnx, moq_pq_conn_t **out)
{
    moq_pq_conn_cfg_t cfg;
    moq_pq_conn_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.session = session;
    cfg.cnx = cnx;
    cfg.alloc = moq_alloc_default();
    return moq_pq_conn_create(&cfg, out);
}

static int sim_server_cb(picoquic_cnx_t *cnx, uint64_t stream_id, uint8_t *bytes,
                         size_t length, picoquic_call_back_event_t event,
                         void *callback_ctx, void *stream_ctx)
{
    pq_sim_t *s = (pq_sim_t *)callback_ctx;
    (void)stream_id; (void)bytes; (void)length; (void)stream_ctx;
    if (event != picoquic_callback_almost_ready && event != picoquic_callback_ready)
        return 0;
    if (s->server_conn) return 0;
    if (sim_create_session(s, MOQ_PERSPECTIVE_SERVER, &s->server_session) != 0 ||
        sim_attach_conn(s->server_session, cnx, &s->server_conn) != 0) {
        s->server_create_failed = 1;
        return -1;
    }
    return 0;
}

static int sim_setup(pq_sim_t *s, uint8_t cid_byte, moq_version_t version,
                     bool reliable, uint32_t server_fills)
{
    memset(s, 0, sizeof(*s));
    s->version = version;
    s->server_fills = server_fills;
    picoquic_solution_dir = PICOQUIC_SOURCE_DIR;
    picoquic_connection_id_t cid = {
        .id = {0x6d, 0x72, 0x72, cid_byte, 0, 0, 0, 0}, .id_len = 8
    };
    s->phase = SIM_TRANSPORT;
    if (tls_api_init_ctx_ex(&s->test_ctx, PICOQUIC_INTERNAL_TEST_VERSION_1,
                            PICOQUIC_TEST_SNI, PICOQUIC_TEST_ALPN, &s->now,
                            NULL, NULL, 0, 1, 0, &cid) != 0)
        return -1;
    picoquic_set_default_idle_timeout(s->test_ctx->qclient, 30000);
    picoquic_set_default_idle_timeout(s->test_ctx->qserver, 30000);
    if (reliable) {
        /* Both endpoints advertise reset_stream_at: the client cnx exists
         * already, so set its local parameters before the handshake; the
         * server cnx inherits the context default. */
        picoquic_tp_t tp = *picoquic_get_transport_parameters(s->test_ctx->cnx_client, 1);
        tp.is_reset_stream_at_enabled = 1;
        picoquic_set_transport_parameters(s->test_ctx->cnx_client, &tp);
        picoquic_set_default_tp_value(s->test_ctx->qserver, picoquic_tp_reset_stream_at, 1);
    }
    s->phase = SIM_SESSION;
    if (sim_create_session(s, MOQ_PERSPECTIVE_CLIENT, &s->client_session) != 0) return -1;
    s->phase = SIM_ATTACH;
    s->pre_attach_cb = picoquic_get_callback_function(s->test_ctx->cnx_client);
    s->pre_attach_ctx = picoquic_get_callback_context(s->test_ctx->cnx_client);
    if (sim_attach_conn(s->client_session, s->test_ctx->cnx_client, &s->client_conn) != 0)
        return -1;
    s->phase = SIM_CONNECT;
    picoquic_set_default_callback(s->test_ctx->qserver, sim_server_cb, s);
    if (picoquic_start_client_cnx(s->test_ctx->cnx_client) != 0) return -1;
    if (tls_api_connection_loop(s->test_ctx, &s->loss, 0, &s->now) != 0) return -1;
    if (s->server_create_failed || !s->server_conn) return -1;
    s->phase = SIM_READY;
    return 0;
}

static int sim_round(pq_sim_t *s, uint64_t time_limit, int *was_active)
{
    if (s->client_conn) moq_pq_service(s->client_conn, s->now);
    if (s->server_conn) moq_pq_service(s->server_conn, s->now);
    int wa = 0;
    int rc = tls_api_one_sim_round(s->test_ctx, &s->now, time_limit, &wa);
    if (s->client_conn) moq_pq_service(s->client_conn, s->now);
    if (s->server_conn) moq_pq_service(s->server_conn, s->now);
    if (was_active) *was_active = wa;
    return rc;
}

static void sim_pump(pq_sim_t *s, uint64_t ms)
{
    uint64_t limit = s->now + ms * 1000;
    int inactive = 0;
    for (int i = 0; i < 100000; i++) {
        if (s->now > limit) return;
        int wa = 0;
        if (sim_round(s, limit, &wa) != 0) return;
        if (!wa) { if (++inactive > 10) return; }
        else inactive = 0;
    }
}

static int sim_handshake(pq_sim_t *s)
{
    if (moq_session_start(s->client_session, s->now) < 0) return -1;
    if (s->version != MOQ_VERSION_DRAFT_16 &&
        moq_session_start(s->server_session, s->now) < 0) return -1;
    int cd = 0, sd = 0;
    uint64_t tl = s->now + 5000000;
    for (int i = 0; i < 100000 && !(cd && sd); i++) {
        if (s->now > tl) break;
        int wa = 0;
        if (sim_round(s, tl, &wa) != 0) break;
        moq_event_t ev;
        while (moq_session_poll_events(s->client_session, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SETUP_COMPLETE) cd = 1;
            moq_event_cleanup(&ev);
        }
        while (moq_session_poll_events(s->server_session, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SETUP_COMPLETE) sd = 1;
            moq_event_cleanup(&ev);
        }
    }
    return (cd && sd) ? 0 : -1;
}

static void sim_cleanup(pq_sim_t *s)
{
    if (s->client_conn) moq_pq_conn_destroy(s->client_conn);
    if (s->server_conn) moq_pq_conn_destroy(s->server_conn);
    if (s->client_session) moq_session_destroy(s->client_session);
    if (s->server_session) moq_session_destroy(s->server_session);
    if (s->test_ctx) tls_api_delete_ctx(s->test_ctx);
    memset(s, 0, sizeof(*s));
}

/* Subscribe (with an optional fill) and accept with Largest (5,2); returns
 * the server's handle, or an invalid one. */
static moq_subscription_t sim_subscribe(pq_sim_t *s, const moq_fill_cfg_t *fill)
{
    static const char names[] = "abcdefgh";
    moq_subscription_t none = {0};
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    moq_bytes_t ns[] = {{(const uint8_t *)"t", 1}};
    sc.track_namespace.parts = ns;
    sc.track_namespace.count = 1;
    sc.track_name = (moq_bytes_t){(const uint8_t *)&names[s->tracks++ % 8], 1};
    sc.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    sc.fill = fill;
    moq_subscription_t sub;
    if (moq_session_subscribe(s->client_session, &sc, s->now, &sub) < 0) return none;
    sim_pump(s, 200);
    moq_subscription_t ss = none;
    moq_event_t ev;
    while (moq_session_poll_events(s->server_session, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) ss = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
    }
    if (!moq_subscription_is_valid(ss)) return none;
    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    ac.has_largest = true; ac.largest_group = 5; ac.largest_object = 2;
    if (moq_session_accept_subscribe(s->server_session, ss, &ac, s->now) < 0) return none;
    return ss;
}

/* Poll the client's events; true if one of `kind` arrived. */
static bool client_saw(pq_sim_t *s, moq_event_kind_t kind)
{
    bool seen = false;
    moq_event_t ev;
    while (moq_session_poll_events(s->client_session, &ev, 1) > 0) {
        if (ev.kind == kind) seen = true;
        moq_event_cleanup(&ev);
    }
    return seen;
}

/* One run of done_count; returns 1 when the subscriber surfaced the done
 * within 300 ms of simulated time. */
static int run_done_count(uint8_t cid, moq_version_t v, bool reliable)
{
    pq_sim_t s;
    int got = 0;
    if (sim_setup(&s, cid, v, reliable, 0) != 0 || sim_handshake(&s) != 0) {
        CHECK(false); sim_cleanup(&s); return 0;
    }
    moq_subscription_t ss = sim_subscribe(&s, NULL);
    CHECK(moq_subscription_is_valid(ss));
    sim_pump(&s, 200);
    (void)client_saw(&s, MOQ_EVENT_SUBSCRIBE_OK);

    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = 6;
    sgc.publisher_priority = 128;
    moq_subgroup_handle_t sg;
    CHECK(moq_session_open_subgroup(s.server_session, ss, &sgc, s.now, &sg) == MOQ_OK);
    moq_rcbuf_t *b = NULL;
    moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)"object", 6, &b);
    CHECK(moq_session_write_object(s.server_session, sg, 0, b, s.now) == MOQ_OK);
    moq_rcbuf_decref(b);
    CHECK(moq_session_reset_subgroup(s.server_session, sg, 0x1, s.now) == MOQ_OK);
    moq_done_subscribe_cfg_t dc;
    moq_done_subscribe_cfg_init(&dc);
    dc.status_code = 0x2;
    dc.stream_count = 1;
    CHECK(moq_session_done_subscribe(s.server_session, ss, &dc, s.now) == MOQ_OK);
    sim_pump(&s, 300);
    got = client_saw(&s, MOQ_EVENT_SUBSCRIBE_DONE) ? 1 : 0;
    CHECK(!moq_pq_conn_is_fatal(s.client_conn));
    CHECK(!moq_pq_conn_is_fatal(s.server_conn));
    sim_cleanup(&s);
    return got;
}

/* One run of failed_fill; returns 1 when the subscriber ended the fill. */
static int run_failed_fill(uint8_t cid, bool reliable)
{
    pq_sim_t s;
    if (sim_setup(&s, cid, MOQ_VERSION_DRAFT_22, reliable, 1) != 0 || sim_handshake(&s) != 0) {
        CHECK(false); sim_cleanup(&s); return 0;
    }
    moq_location_filter_cfg_t loc;
    moq_location_filter_cfg_init(&loc);
    loc.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;
    loc.start_group = 3;
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.location = &loc;
    CHECK(moq_subscription_is_valid(sim_subscribe(&s, &fill)));   /* holds the slot */
    sim_pump(&s, 200);
    (void)client_saw(&s, MOQ_EVENT_FILL_TERMINAL);
    CHECK(moq_subscription_is_valid(sim_subscribe(&s, &fill)));   /* failed on the wire */
    sim_pump(&s, 300);
    int got = client_saw(&s, MOQ_EVENT_FILL_TERMINAL) ? 1 : 0;
    CHECK(!moq_pq_conn_is_fatal(s.client_conn));
    CHECK(!moq_pq_conn_is_fatal(s.server_conn));
    sim_cleanup(&s);
    return got;
}

/* The frame an accepted reset queues: a plain RESET_STREAM for an empty
 * reliable prefix, RESET_STREAM_AT otherwise. For this in-credit stream the
 * provider formats and queues it as soon as the reset is accepted, before any
 * of the prefix is acknowledged (reset_sent is that queue/format state, not
 * proof of transmission; a reset beyond the peer's stream credit is queued
 * later). The provider's ACK-boundary rows (empty and last-byte reliable
 * prefixes) are pinned by test_pq_reset_after_fin.c. */
static void check_reset_frame_type(uint64_t prefix, moq_version_t v)
{
    pq_sim_t s;
    scenario = prefix == 0 ? "prefix_zero" : "prefix_last_byte";
    if (sim_setup(&s, 0x70, v, true, 0) != 0 ||
        sim_handshake(&s) != 0) {
        CHECK(false); sim_cleanup(&s); return;
    }
    sim_pump(&s, 200);
    picoquic_cnx_t *cnx = s.test_ctx->cnx_client;
    uint64_t sid = picoquic_get_next_local_stream_id(cnx, 1);
    int rc = picoquic_mark_active_stream(cnx, sid, 0, NULL);
    CHECK(rc == 0);
    picoquic_stream_head_t *st = picoquic_find_stream(cnx, sid);
    if (rc != 0 || st == NULL) {
        CHECK(st != NULL); sim_cleanup(&s); return;
    }
    st->sent_offset = prefix;
    picoquic_misc_frame_header_t *before = cnx->last_misc_frame;
    CHECK(picoquic_reset_stream_at(cnx, sid, 1, prefix) == 0);
    CHECK(st->reset_requested && st->reset_sent);
    picoquic_misc_frame_header_t *queued = cnx->last_misc_frame;
    CHECK(queued != NULL && queued != before && queued->length > 0);
    if (queued != NULL && queued != before && queued->length > 0) {
        const uint8_t *frame = (const uint8_t *)(queued + 1);
        uint64_t type = 0;
        CHECK(picoquic_frames_varint_decode(frame, frame + queued->length, &type) != NULL);
        CHECK(type == (prefix == 0 ? picoquic_frame_type_reset_stream :
                                     picoquic_frame_type_reset_stream_at));
    }
    sim_cleanup(&s);
}

#if defined(PICOQUIC_STREAM_TERMINAL_CONTRACT_VERSION) && PICOQUIC_STREAM_TERMINAL_CONTRACT_VERSION >= 1
#define SIM_HAVE_D22 1
#else
#define SIM_HAVE_D22 0     /* draft-22 refuses to attach to this provider */
#endif

/* A setup that failed at the client adapter's constructor with every earlier
 * step in place, leaving the connection's callback with its owner. */
static bool sim_refused_at_attach(const pq_sim_t *s, int rc)
{
    return rc == -1 && s->phase == SIM_ATTACH && s->test_ctx != NULL &&
           s->client_session != NULL && s->client_conn == NULL &&
           s->server_session == NULL && s->server_conn == NULL &&
           picoquic_get_callback_function(s->test_ctx->cnx_client) == s->pre_attach_cb &&
           picoquic_get_callback_context(s->test_ctx->cnx_client) == s->pre_attach_ctx;
}

#if !SIM_HAVE_D22
/* Without the provider's stream terminal contract draft-22 refuses at the
 * constructor, and only there: the documented -1, *out cleared, nothing
 * allocated, no callback taken; the same connection accepts draft-16. */
static void check_d22_refused(void)
{
    pq_sim_t s;
    scenario = "d22_refused";
    int rc = sim_setup(&s, 0x71, MOQ_VERSION_DRAFT_22, true, 0);
    CHECK(sim_refused_at_attach(&s, rc));
    if (s.phase == SIM_ATTACH && s.client_session != NULL) {
        pq_sim_t d16;
        memset(&d16, 0, sizeof(d16));
        d16.version = MOQ_VERSION_DRAFT_16;
        moq_session_t *ok = NULL;
        CHECK(sim_create_session(&d16, MOQ_PERSPECTIVE_CLIENT, &ok) == 0);
        const char *why = pq_check_constructor_refusal(s.test_ctx->cnx_client,
                                                       s.client_session, ok);
        if (why) fprintf(stderr, "d22_refused: %s\n", why);
        CHECK(why == NULL);
        if (ok) moq_session_destroy(ok);
    }
    sim_cleanup(&s);
}
#endif

/* A setup failure before the constructor is not the capability refusal. */
static void check_unrelated_setup_failure(void)
{
    pq_sim_t s;
    scenario = "unrelated_setup_failure";
    int rc = sim_setup(&s, 0x72, (moq_version_t)17, true, 0);   /* no such profile */
    CHECK(rc == -1);
    CHECK(s.phase == SIM_SESSION);
    CHECK(!sim_refused_at_attach(&s, rc));
    sim_cleanup(&s);
}

int main(void)
{
    /* The frame type is the provider's; any session that attaches carries
     * the reset. */
    moq_version_t frame_v = SIM_HAVE_D22 ? MOQ_VERSION_DRAFT_22
                                         : MOQ_VERSION_DRAFT_16;
    check_reset_frame_type(0, frame_v);
    check_reset_frame_type(5, frame_v);
#if !SIM_HAVE_D22
    check_d22_refused();
#endif
    check_unrelated_setup_failure();
    static const struct { const char *name; moq_version_t v; } dc[] = {
        { "done_count_d16", MOQ_VERSION_DRAFT_16 },
        { "done_count_d22", MOQ_VERSION_DRAFT_22 },
    };
    for (size_t i = 0; i < sizeof(dc) / sizeof(dc[0]); i++) {
        if (dc[i].v == MOQ_VERSION_DRAFT_22 && !SIM_HAVE_D22) continue;
        for (int reliable = 1; reliable >= 0; reliable--) {
            scenario = dc[i].name;
            int got = 0;
            for (int r = 0; r < RUNS; r++)
                got += run_done_count((uint8_t)(0x10 + r), dc[i].v, reliable != 0);
            printf("%s %s: header delivered in %d/%d runs\n", dc[i].name,
                   reliable ? "RESET_STREAM_AT" : "plain reset", got, RUNS);
            if (reliable) CHECK(got == RUNS);
        }
    }
    for (int reliable = 1; reliable >= 0 && SIM_HAVE_D22; reliable--) {
        scenario = "failed_fill_d22";
        int got = 0;
        for (int r = 0; r < RUNS; r++)
            got += run_failed_fill((uint8_t)(0x30 + r), reliable != 0);
        printf("failed_fill_d22 %s: header delivered in %d/%d runs\n",
               reliable ? "RESET_STREAM_AT" : "plain reset", got, RUNS);
        if (reliable) CHECK(got == RUNS);
    }
    if (failures == 0)
        printf("PASS: pq_sim_reliable_reset\n");
    return failures != 0;
}
