/*
 * The end of a receive half this adapter stopped, over the REAL picoquic
 * transport in the deterministic simulator (tls_api).
 *
 * Once the application has asked for STOP_SENDING on a stream, picoquic
 * delivers none of that stream's later bytes and no FIN (frames.c gates the
 * data callback on !stop_sending_requested); a peer RESET is still signalled.
 * A peer that already sent its last byte and FIN when the STOP reaches it has
 * nothing left to reset (RFC 9000 section 3.5: a reset is deferred in "Data
 * Sent" and never owed once every byte is acknowledged), so the stream ends
 * with no callback at all unless the stream carries an application context:
 * then picoquic's stream release reports that it retired the stream. The
 * adapter watches every receive half it stops this way, keeps the watch
 * through the calls that clear picoquic's context (a reset, an active mark),
 * and on the release lets go of its own record and reports the stream's
 * whole terminal to the bridge -- a fact that names no cause and commits no
 * bytes. Which owners that terminal retires is the bridge's and the
 * session's to decide; an aborting request stream is one.
 *
 * Rows (draft-16/18/22; the bidi rows on 18/22, whose requests use their own
 * stream; draft-22 only where the provider carries the terminal contract):
 *   fin_after_stop    the FIN arrives after the local STOP: the release is
 *                     reported once, and the adapter's record goes;
 *   fin_before_stop   the FIN is seen first: nothing is watched or reported;
 *   peer_reset        a RESET after the STOP is the one terminal, and no
 *                     release follows it;
 *   duplicate_release a repeated or unknown release changes nothing the
 *                     adapter owns;
 *   teardown          an adapter destroyed with a stopped stream outstanding
 *                     leaves no context behind in picoquic;
 *   bidi              a request stream stopped and reset in either order
 *                     keeps the watch; with the publisher's reset, or its FIN
 *                     given at the transport, it ends once and its aborting
 *                     bridge entry is retired -- on a release, no earlier than
 *                     the acknowledgement of the client's own reset;
 *   growth            stopped streams survive receive-table growth, a reused
 *                     slot is never ended by an old stream's release, and a
 *                     release naming an unwatched live stream keeps it;
 *   setter_failure    a context the adapter cannot attach fails the STOP
 *                     rather than losing the stream's end;
 *   stop_after_reset  a STOP after a peer RESET is a terminal no-op;
 *   stop contract     repeat live/retired STOP, terminal receive halves,
 *                     unknown and send-only refusals, unrelated progress;
 *   paused            a release that comes while a stopped stream's bytes
 *                     are still owed waits for them: no terminal and no credit
 *                     until they reach the bridge, then exactly one;
 *   bidi rebind       the watch survives an active-mark write and a reliable
 *                     reset, or the plain reset that replaces a refused one.
 * growth, setter_failure, stop_after_reset and paused run on draft-16 only: the
 * receive table they exercise is common to every draft.
 */

#include <moq/moq.h>
#include <moq/picoquic.h>
#include <moq/transport_bridge.h>

#include <picoquictest_internal.h>
#include <picoquic_internal.h>
#include <picoquic_utils.h>

#include "pq_test_seam.h"
#include "pq_attach_refusal.h"
#include "../../../tests/unit/test_binding_support.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

size_t moq_pq_conn_active_stream_count(const moq_pq_conn_t *c);

static int failures = 0;
static const char *row = "";
static char row_buf[96];

static const char *row_name(const char *base, moq_version_t v)
{
    snprintf(row_buf, sizeof(row_buf), "%s d%u", base,
             v == MOQ_VERSION_DRAFT_16 ? 16u : v == MOQ_VERSION_DRAFT_18 ? 18u : 22u);
    return row_buf;
}
#define CHECK(cond) do { \
    if (!(cond)) { \
        fprintf(stderr, "FAIL[%s]: %s:%d: %s\n", row, __FILE__, __LINE__, \
                #cond); \
        failures++; \
    } } while (0)

typedef struct {
    picoquic_stream_data_cb_fn next;
    void *next_ctx;
    uint64_t sid;
    unsigned resets, stops, released, marked_released, commits;
    int fin_at_release, reset_at_release;
    const moq_transport_endpoint_ops_t *terminal_stop_ops;
    void *terminal_stop_ctx;
    unsigned terminal_stop_checks;
} spy_t;

/* The setup step a rig last entered; a failed setup stops at it. */
typedef enum {
    RIG_TRANSPORT = 1,   /* test QUIC context and client connection */
    RIG_SESSION,         /* client MoQ session */
    RIG_ATTACH,          /* client adapter constructor */
    RIG_CONNECT,         /* QUIC handshake and server attach */
    RIG_SETUP,           /* MoQ SETUP on both ends */
    RIG_READY
} rig_phase_t;

typedef struct {
    picoquic_test_tls_api_ctx_t *t;
    uint64_t now, loss;
    rig_phase_t phase;
    picoquic_stream_data_cb_fn pre_attach_cb;   /* client callback owner */
    void *pre_attach_ctx;                       /* before the constructor */
    moq_version_t version;
    uint32_t client_max_events;
    moq_session_t *cs, *ss;
    moq_pq_conn_t *cc, *sc;
    int server_failed;
    spy_t spy;
    unsigned tracks;
} rig_t;

static int spy_cb(picoquic_cnx_t *cnx, uint64_t sid, uint8_t *bytes, size_t len,
                  picoquic_call_back_event_t ev, void *ctx, void *sctx)
{
    spy_t *spy = ctx;
    if (sid == spy->sid) {
        if (ev == picoquic_callback_stream_reset) spy->resets++;
        if (ev == picoquic_callback_stop_sending) spy->stops++;
        if (spy->terminal_stop_ops != NULL &&
            (ev == picoquic_callback_stream_reset || ev == picoquic_callback_stream_fin)) {
            /* Probe while the real terminal receive state is still live,
             * before the adapter can drop it or hide it behind its FIN path.
             * A test-owned context makes clearing it observable; restore the
             * original before forwarding this one genuine callback. */
            picoquic_stream_head_t *st = picoquic_find_stream(cnx, sid);
            CHECK(st != NULL);
            if (st != NULL) {
                CHECK(st->reset_received ||
                      (st->fin_received && st->consumed_offset >= st->fin_offset));
                void *saved_ctx = st->app_stream_ctx;
                int stop_requested = st->stop_sending_requested;
                int reset_requested = st->reset_requested;
                int fin_requested = st->fin_requested;
                uint64_t cause = st->local_stop_error;
                picoquic_misc_frame_header_t *first = cnx->first_misc_frame;
                picoquic_misc_frame_header_t *last = cnx->last_misc_frame;
                uint64_t uni_credit = cnx->max_streams_unidir_local_computed;
                uint64_t bidi_credit = cnx->max_streams_bidir_local_computed;
                st->app_stream_ctx = spy;
                CHECK(spy->terminal_stop_ops->stop_sending(spy->terminal_stop_ctx,
                                                           sid, 0x51) == MOQ_TRANSPORT_OK);
                CHECK(spy->terminal_stop_ops->stop_sending(spy->terminal_stop_ctx,
                                                           sid, 0x52) == MOQ_TRANSPORT_OK);
                CHECK(st->app_stream_ctx == spy);
                CHECK(st->stop_sending_requested == stop_requested);
                CHECK(st->local_stop_error == cause);
                CHECK(st->reset_requested == reset_requested);
                CHECK(st->fin_requested == fin_requested);
                CHECK(cnx->first_misc_frame == first && cnx->last_misc_frame == last);
                CHECK(cnx->max_streams_unidir_local_computed == uni_credit);
                CHECK(cnx->max_streams_bidir_local_computed == bidi_credit);
                st->app_stream_ctx = saved_ctx;
                spy->terminal_stop_checks++;
            }
        }
        if (ev == picoquic_callback_stream_released) {
            spy->released++;
            if (sctx != NULL) spy->marked_released++;
            /* The receive half's state as picoquic retires the stream. */
            picoquic_stream_head_t *st = picoquic_find_stream(cnx, sid);
            spy->fin_at_release = st ? st->fin_received : -1;
            spy->reset_at_release = st ? st->reset_received : -1;
        }
#ifdef PICOQUIC_STREAM_TERMINAL_CONTRACT_VERSION
        if (ev == picoquic_callback_stream_send_committed) spy->commits++;
#endif
    }
    return spy->next(cnx, sid, bytes, len, ev, spy->next_ctx, sctx);
}

static void spy_on(rig_t *r, uint64_t sid)
{
    r->spy.sid = sid;
    r->spy.resets = r->spy.stops = r->spy.released = r->spy.marked_released = 0;
    r->spy.commits = 0;
    r->spy.fin_at_release = r->spy.reset_at_release = -1;
}

static int mk_session(rig_t *r, moq_perspective_t p, moq_session_t **out)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), p);
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 64;
    cfg.version = r->version;
    if (p == MOQ_PERSPECTIVE_CLIENT && r->client_max_events)
        cfg.max_events = r->client_max_events;
    if (moq_session_create(&cfg, 0, out) != MOQ_OK) return -1;
    return test_bind_if_required(*out) == MOQ_OK ? 0 : -1;
}

/* An allocator whose realloc always moves the block, so a table that grows
 * is relocated: the client adapter uses it when g_moving_alloc is set. */
static void *mv_alloc(size_t n, void *ctx)
{
    (void)ctx;
    return moq_alloc_default()->alloc(n, moq_alloc_default()->ctx);
}
static void mv_free(void *p, size_t n, void *ctx)
{
    (void)ctx;
    moq_alloc_default()->free(p, n, moq_alloc_default()->ctx);
}
static void *mv_realloc(void *p, size_t old, size_t n, void *ctx)
{
    (void)ctx;
    void *q = moq_alloc_default()->alloc(n, moq_alloc_default()->ctx);
    if (q == NULL) return NULL;
    if (p != NULL) {
        memcpy(q, p, old < n ? old : n);
        moq_alloc_default()->free(p, old, moq_alloc_default()->ctx);
    }
    return q;
}
static const moq_alloc_t g_mv = {
    .alloc = mv_alloc, .realloc = mv_realloc, .free = mv_free, .ctx = NULL };
static bool g_moving_alloc;   /* the next client attach relocates on growth */
static bool g_reliable;       /* the next setup negotiates reset_stream_at */

static int attach(moq_session_t *s, picoquic_cnx_t *cnx, moq_pq_conn_t **out)
{
    moq_pq_conn_cfg_t cfg;
    moq_pq_conn_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.session = s;
    cfg.cnx = cnx;
    cfg.alloc = moq_alloc_default();
    if (g_moving_alloc && picoquic_is_client(cnx)) cfg.alloc = &g_mv;
    return moq_pq_conn_create(&cfg, out);
}

static int server_cb(picoquic_cnx_t *cnx, uint64_t sid, uint8_t *b, size_t l,
                     picoquic_call_back_event_t ev, void *ctx, void *sctx)
{
    rig_t *r = ctx;
    (void)sid; (void)b; (void)l; (void)sctx;
    if (ev != picoquic_callback_almost_ready && ev != picoquic_callback_ready)
        return 0;
    if (r->sc) return 0;
    if (mk_session(r, MOQ_PERSPECTIVE_SERVER, &r->ss) != 0 ||
        attach(r->ss, cnx, &r->sc) != 0) {
        r->server_failed = 1;
        return -1;
    }
    return 0;
}

static void service(rig_t *r)
{
    if (r->cc) moq_pq_service(r->cc, r->now);
    if (r->sc) moq_pq_service(r->sc, r->now);
}

static int one_round(rig_t *r, uint64_t limit, int *active)
{
    service(r);
    int wa = 0;
    int rc = tls_api_one_sim_round(r->t, &r->now, limit, &wa);
    service(r);
    if (active) *active = wa;
    return rc;
}

static void pump(rig_t *r, uint64_t ms)
{
    uint64_t limit = r->now + ms * 1000;
    int idle = 0;
    for (int i = 0; i < 100000 && r->now <= limit; i++) {
        int wa = 0;
        if (one_round(r, limit, &wa) != 0) return;
        if (!wa) { if (++idle > 10) return; } else idle = 0;
    }
}

static int setup(rig_t *r, uint8_t cid_byte, moq_version_t v, uint32_t max_events)
{
    memset(r, 0, sizeof(*r));
    r->version = v;
    r->client_max_events = max_events;
    picoquic_solution_dir = PICOQUIC_SOURCE_DIR;
    picoquic_connection_id_t cid = {
        .id = {0x5e, 0x72, 0x11, cid_byte, 0, 0, 0, 0}, .id_len = 8
    };
    r->phase = RIG_TRANSPORT;
    if (tls_api_init_ctx_ex(&r->t, PICOQUIC_INTERNAL_TEST_VERSION_1,
                            PICOQUIC_TEST_SNI, PICOQUIC_TEST_ALPN, &r->now,
                            NULL, NULL, 0, 1, 0, &cid) != 0)
        return -1;
    picoquic_set_default_idle_timeout(r->t->qclient, 30000);
    picoquic_set_default_idle_timeout(r->t->qserver, 30000);
    if (g_reliable) {
        picoquic_tp_t tp = *picoquic_get_transport_parameters(r->t->cnx_client, 1);
        tp.is_reset_stream_at_enabled = 1;
        picoquic_set_transport_parameters(r->t->cnx_client, &tp);
        picoquic_set_default_tp_value(r->t->qserver, picoquic_tp_reset_stream_at, 1);
    }
    r->phase = RIG_SESSION;
    if (mk_session(r, MOQ_PERSPECTIVE_CLIENT, &r->cs) != 0) return -1;
    r->phase = RIG_ATTACH;
    r->pre_attach_cb = picoquic_get_callback_function(r->t->cnx_client);
    r->pre_attach_ctx = picoquic_get_callback_context(r->t->cnx_client);
    if (attach(r->cs, r->t->cnx_client, &r->cc) != 0) return -1;
    r->phase = RIG_CONNECT;
    r->spy.next = picoquic_get_callback_function(r->t->cnx_client);
    r->spy.next_ctx = picoquic_get_callback_context(r->t->cnx_client);
    r->spy.sid = UINT64_MAX;
    picoquic_set_callback(r->t->cnx_client, spy_cb, &r->spy);
    picoquic_set_default_callback(r->t->qserver, server_cb, r);
    if (picoquic_start_client_cnx(r->t->cnx_client) != 0) return -1;
    if (tls_api_connection_loop(r->t, &r->loss, 0, &r->now) != 0) return -1;
    if (r->server_failed || !r->sc) return -1;
    r->phase = RIG_SETUP;
    if (moq_session_start(r->cs, r->now) < 0) return -1;
    if (v != MOQ_VERSION_DRAFT_16 && moq_session_start(r->ss, r->now) < 0)
        return -1;
    int cd = 0, sd = 0;
    for (int i = 0; i < 100000 && !(cd && sd); i++) {
        if (one_round(r, r->now + 5000000, NULL) != 0) break;
        moq_event_t ev;
        while (moq_session_poll_events(r->cs, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SETUP_COMPLETE) cd = 1;
            moq_event_cleanup(&ev);
        }
        while (moq_session_poll_events(r->ss, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SETUP_COMPLETE) sd = 1;
            moq_event_cleanup(&ev);
        }
    }
    if (!(cd && sd)) return -1;
    r->phase = RIG_READY;
    return 0;
}

static void cleanup(rig_t *r)
{
    if (r->cc) moq_pq_conn_destroy(r->cc);
    if (r->sc) moq_pq_conn_destroy(r->sc);
    if (r->cs) moq_session_destroy(r->cs);
    if (r->ss) moq_session_destroy(r->ss);
    if (r->t) tls_api_delete_ctx(r->t);
    memset(r, 0, sizeof(*r));
}

typedef struct {
    unsigned objects, other, errors;
} seen_t;

/* Drain the client's events, counting objects of `sub`. */
static void drain(rig_t *r, moq_subscription_t sub, seen_t *o)
{
    moq_event_t ev;
    while (moq_session_poll_events(r->cs, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) {
            if (moq_subscription_eq(ev.u.object_received.sub, sub)) o->objects++;
            else o->other++;
        }
        if (ev.kind == MOQ_EVENT_SESSION_CLOSED) o->errors++;
        moq_event_cleanup(&ev);
    }
}

/* SUBSCRIBE from the client, accepted by the server. */
static moq_subscription_t subscribe(rig_t *r, moq_subscription_t *client_sub)
{
    static const char names[] = "abcdefgh";
    moq_subscription_t none = MOQ_SUBSCRIPTION_INVALID;
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    moq_bytes_t ns[] = {{(const uint8_t *)"stop", 4}};
    sc.track_namespace.parts = ns;
    sc.track_namespace.count = 1;
    sc.track_name = (moq_bytes_t){(const uint8_t *)&names[r->tracks++ % 8], 1};
    sc.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    if (moq_session_subscribe(r->cs, &sc, r->now, client_sub) < 0) return none;
    pump(r, 200);
    moq_subscription_t ss = none;
    moq_event_t ev;
    while (moq_session_poll_events(r->ss, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) ss = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
    }
    if (!moq_subscription_is_valid(ss)) return none;
    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    if (moq_session_accept_subscribe(r->ss, ss, &ac, r->now) < 0) return none;
    pump(r, 200);
    seen_t s = {0};
    drain(r, *client_sub, &s);
    return ss;
}

static moq_rcbuf_t *payload(uint8_t fill)
{
    uint8_t b[48];
    memset(b, fill, sizeof(b));
    moq_rcbuf_t *rc = NULL;
    (void)moq_rcbuf_create(moq_alloc_default(), b, sizeof(b), &rc);
    return rc;
}

/* An object of `len` bytes, each its id's low byte. */
static moq_result_t write_big(rig_t *r, moq_subgroup_handle_t h, uint64_t id,
                              size_t len)
{
    static uint8_t b[2048];
    memset(b, (int)id, len);
    moq_rcbuf_t *p = NULL;
    if (moq_rcbuf_create(moq_alloc_default(), b, len, &p) < 0)
        return MOQ_ERR_NOMEM;
    moq_result_t rc = moq_session_write_object(r->ss, h, id, p, r->now);
    moq_rcbuf_decref(p);
    return rc;
}

static moq_result_t write_obj(rig_t *r, moq_subgroup_handle_t h, uint64_t id)
{
    moq_rcbuf_t *p = payload((uint8_t)id);
    moq_result_t rc = moq_session_write_object(r->ss, h, id, p, r->now);
    moq_rcbuf_decref(p);
    return rc;
}

static moq_subgroup_handle_t open_sg(rig_t *r, moq_subscription_t ss, uint64_t g)
{
    moq_subgroup_cfg_t c;
    moq_subgroup_cfg_init(&c);
    c.group_id = g;
    moq_subgroup_handle_t h = {0};
    CHECK(moq_session_open_subgroup(r->ss, ss, &c, r->now, &h) == MOQ_OK);
    return h;
}

/* Server-initiated unidirectional streams the client adapter tracks, in
 * stream-id order (data streams; the draft-18/22 control stream excluded). */
static size_t tracked_server_uni(rig_t *r, uint64_t *out, size_t cap)
{
    size_t n = 0;
    moq_pq_test_rx_t st;
    for (uint64_t sid = 3; sid < 4000 && n < cap; sid += 4) {
        if (!moq_pq_test_rx_state(r->cc, sid, &st)) continue;
        if (r->version != MOQ_VERSION_DRAFT_16 && sid == 3) continue;
        out[n++] = sid;
    }
    return n;
}

static picoquic_stream_head_t *client_stream(rig_t *r, uint64_t sid)
{
    return picoquic_find_stream(r->t->cnx_client, sid);
}

static bool client_ok(rig_t *r)
{
    return moq_session_state(r->cs) == MOQ_SESS_ESTABLISHED &&
           !moq_pq_conn_is_fatal(r->cc);
}

/* -- rows ------------------------------------------------------------------ */

static void fin_after_stop(moq_version_t v, uint8_t cid)
{
    row = row_name("fin_after_stop", v);
    rig_t r;
    if (setup(&r, cid, v, 0) != 0) { CHECK(false); cleanup(&r); return; }
    moq_subscription_t csub;
    moq_subscription_t ss = subscribe(&r, &csub);
    CHECK(moq_subscription_is_valid(ss));
    size_t base = moq_pq_conn_active_stream_count(r.cc);
    moq_subgroup_handle_t h = open_sg(&r, ss, 0);
    CHECK(write_obj(&r, h, 0) == MOQ_OK);
    pump(&r, 100);
    seen_t seen = {0};
    drain(&r, csub, &seen);
    CHECK(seen.objects == 1);
    uint64_t sid[4];
    CHECK(tracked_server_uni(&r, sid, 4) == 1);
    spy_on(&r, sid[0]);
    CHECK(moq_pq_conn_active_stream_count(r.cc) == base + 1);

    /* The publisher's last object and FIN are on their way when the
     * subscriber cancels: its STOP is requested before they arrive. */
    CHECK(write_obj(&r, h, 1) == MOQ_OK);
    CHECK(moq_session_close_subgroup(r.ss, h, r.now) == MOQ_OK);
    moq_pq_service(r.sc, r.now);
    CHECK(moq_session_unsubscribe(r.cs, csub, r.now) == MOQ_OK);
    moq_pq_service(r.cc, r.now);
    picoquic_stream_head_t *st = client_stream(&r, sid[0]);
    CHECK(st != NULL && st->stop_sending_requested && !st->fin_received);

    pump(&r, 500);
    drain(&r, csub, &seen);
    /* picoquic received the FIN and retired the stream; the FIN itself was
     * never delivered, and no reset came. */
    CHECK(client_stream(&r, sid[0]) == NULL);
    CHECK(r.spy.resets == 0);
    CHECK(r.spy.fin_at_release == 1 && r.spy.reset_at_release == 0);
    /* The release reached the adapter, which let go of its own record and
     * reported the stream's whole terminal to the bridge, once. What that
     * settles in the bridge and session is theirs to decide. */
    CHECK(r.spy.marked_released == 1);
    CHECK(moq_pq_test_release_terminals(r.cc) == 1);
    CHECK(!moq_pq_test_rx_state(r.cc, sid[0], &(moq_pq_test_rx_t){0}));
    CHECK(seen.objects == 1);
    CHECK(client_ok(&r));
    printf("  %s: bridge streams %zu before the subgroup, %zu after its "
           "terminal\n", row, base, moq_pq_conn_active_stream_count(r.cc));

    /* A repeated release of that stream, and one for a stream that never
     * existed, change nothing the adapter owns: each is passed on as a
     * terminal for the bridge to ignore. */
    row = row_name("duplicate_release", v);
    picoquic_cnx_t *cnx = r.t->cnx_client;
    CHECK(moq_pq_callback(cnx, sid[0], NULL, 0, picoquic_callback_stream_released,
                          r.cc, r.cc) == 0);
    CHECK(moq_pq_callback(cnx, sid[0], NULL, 0, picoquic_callback_stream_released,
                          r.cc, r.cc) == 0);
    CHECK(moq_pq_callback(cnx, sid[0] + 4000, NULL, 0,
                          picoquic_callback_stream_released, r.cc, r.cc) == 0);
    size_t after = moq_pq_conn_active_stream_count(r.cc);
    pump(&r, 100);
    CHECK(moq_pq_test_release_terminals(r.cc) == 4);
    CHECK(!moq_pq_test_rx_state(r.cc, sid[0], &(moq_pq_test_rx_t){0}));
    CHECK(moq_pq_conn_active_stream_count(r.cc) == after);
    CHECK(client_ok(&r));
    cleanup(&r);
}

/* A subscription's own request stream: draft-16 requests ride the control
 * stream, later drafts open one bidirectional stream per request. */
static size_t request_streams(moq_version_t v)
{
    return v == MOQ_VERSION_DRAFT_16 ? 0 : 1;
}

static void fin_before_stop(moq_version_t v, uint8_t cid)
{
    row = row_name("fin_before_stop", v);
    rig_t r;
    if (setup(&r, cid, v, 0) != 0) { CHECK(false); cleanup(&r); return; }
    moq_subscription_t csub;
    moq_subscription_t ss = subscribe(&r, &csub);
    size_t base = moq_pq_conn_active_stream_count(r.cc);
    moq_subgroup_handle_t h = open_sg(&r, ss, 0);
    CHECK(write_obj(&r, h, 0) == MOQ_OK);
    pump(&r, 100);
    uint64_t sid[4];
    CHECK(tracked_server_uni(&r, sid, 4) == 1);
    spy_on(&r, sid[0]);
    CHECK(write_obj(&r, h, 1) == MOQ_OK);
    CHECK(moq_session_close_subgroup(r.ss, h, r.now) == MOQ_OK);
    pump(&r, 300);
    seen_t seen = {0};
    drain(&r, csub, &seen);
    CHECK(seen.objects == 2);
    CHECK(moq_pq_conn_active_stream_count(r.cc) == base);
    CHECK(moq_session_unsubscribe(r.cs, csub, r.now) == MOQ_OK);
    pump(&r, 300);
    CHECK(moq_pq_test_release_terminals(r.cc) == 0);
    CHECK(r.spy.marked_released == 0);
    /* The cancellation retires the request stream, if it has one. */
    CHECK(moq_pq_conn_active_stream_count(r.cc) + request_streams(v) == base);
    CHECK(client_ok(&r));
    cleanup(&r);
}

static void peer_reset(moq_version_t v, uint8_t cid)
{
    row = row_name("peer_reset", v);
    rig_t r;
    if (setup(&r, cid, v, 0) != 0) { CHECK(false); cleanup(&r); return; }
    moq_subscription_t csub;
    moq_subscription_t ss = subscribe(&r, &csub);
    size_t base = moq_pq_conn_active_stream_count(r.cc);
    moq_subgroup_handle_t h = open_sg(&r, ss, 0);
    CHECK(write_obj(&r, h, 0) == MOQ_OK);
    pump(&r, 100);
    uint64_t sid[4];
    CHECK(tracked_server_uni(&r, sid, 4) == 1);
    spy_on(&r, sid[0]);
    CHECK(moq_session_unsubscribe(r.cs, csub, r.now) == MOQ_OK);
    moq_pq_service(r.cc, r.now);
    CHECK(moq_session_reset_subgroup(r.ss, h, 0x7, r.now) == MOQ_OK);
    moq_pq_service(r.sc, r.now);
    pump(&r, 500);
    CHECK(r.spy.resets == 1);
    CHECK(moq_pq_test_release_terminals(r.cc) == 0);
    CHECK(!moq_pq_test_rx_state(r.cc, sid[0], &(moq_pq_test_rx_t){0}));
    CHECK(moq_pq_conn_active_stream_count(r.cc) + request_streams(v) == base);
    CHECK(client_ok(&r));
    cleanup(&r);
}

static int idle_cb(picoquic_cnx_t *cnx, uint64_t sid, uint8_t *b, size_t l,
                   picoquic_call_back_event_t ev, void *ctx, void *sctx)
{
    (void)cnx; (void)sid; (void)b; (void)l; (void)ev; (void)ctx; (void)sctx;
    return 0;
}

static void teardown(moq_version_t v, uint8_t cid)
{
    row = row_name("teardown", v);
    rig_t r;
    if (setup(&r, cid, v, 0) != 0) { CHECK(false); cleanup(&r); return; }
    moq_subscription_t csub;
    moq_subscription_t ss = subscribe(&r, &csub);
    moq_subgroup_handle_t h = open_sg(&r, ss, 0);
    CHECK(write_obj(&r, h, 0) == MOQ_OK);
    pump(&r, 100);
    uint64_t sid[4];
    CHECK(tracked_server_uni(&r, sid, 4) == 1);
    CHECK(write_obj(&r, h, 1) == MOQ_OK);
    CHECK(moq_session_close_subgroup(r.ss, h, r.now) == MOQ_OK);
    moq_pq_service(r.sc, r.now);
    CHECK(moq_session_unsubscribe(r.cs, csub, r.now) == MOQ_OK);
    moq_pq_service(r.cc, r.now);
    /* The adapter goes away with the stream still owed its end. */
    moq_pq_conn_destroy(r.cc);
    r.cc = NULL;
    picoquic_stream_head_t *st = client_stream(&r, sid[0]);
    CHECK(st != NULL && st->app_stream_ctx == NULL);
    /* The caller owns the cnx after the adapter is gone: keep it driven
     * with a callback of its own. */
    picoquic_set_callback(r.t->cnx_client, idle_cb, NULL);
    pump(&r, 300);
    CHECK(client_stream(&r, sid[0]) == NULL);
    cleanup(&r);
}

static uint64_t request_bidi(rig_t *r)
{
    moq_pq_test_rx_t rx;
    for (uint64_t s = 0; s < 64; s += 4)
        if (moq_pq_test_rx_state(r->cc, s, &rx)) return s;
    return UINT64_MAX;
}

/* The request stream of a subscription, stopped and reset by this
 * adapter before the subscriber's own cancellation is known to the peer:
 * the reset clears picoquic's context, and the stopped half keeps its watch.
 * The publisher answers the STOP with a reset, the stream's one end. */
static void bidi_stop_then_reset(moq_version_t v, uint8_t cid)
{
    row = row_name("bidi stop-then-reset", v);
    rig_t r;
    if (setup(&r, cid, v, 0) != 0) {
        CHECK(false); cleanup(&r); return;
    }
    moq_subscription_t csub, other_csub;
    moq_subscription_t ss = subscribe(&r, &csub);
    CHECK(moq_subscription_is_valid(ss));
    uint64_t bid = request_bidi(&r);
    CHECK(bid != UINT64_MAX);
    /* An unrelated subscription keeps flowing throughout. */
    moq_subscription_t other = subscribe(&r, &other_csub);
    CHECK(moq_subscription_is_valid(other));
    spy_on(&r, bid);
    const moq_transport_endpoint_ops_t *ops;
    void *ectx;
    moq_pq_test_endpoint(r.cc, &ops, &ectx);
    CHECK(ops->stop_sending(ectx, bid, 0x1) == MOQ_TRANSPORT_OK);
    CHECK(ops->reset_stream(ectx, bid, 0x1) == MOQ_TRANSPORT_OK);
    picoquic_stream_head_t *st = client_stream(&r, bid);
    CHECK(st != NULL && st->app_stream_ctx == (void *)r.cc);
    pump(&r, 500);
    CHECK(r.spy.resets == 1);
    CHECK(moq_pq_test_release_terminals(r.cc) == 0);
    CHECK(r.spy.commits == 0);
    CHECK(!moq_pq_test_rx_state(r.cc, bid, &(moq_pq_test_rx_t){0}));
    moq_subgroup_handle_t h = open_sg(&r, other, 0);
    CHECK(write_obj(&r, h, 0) == MOQ_OK);
    pump(&r, 200);
    seen_t seen = {0};
    drain(&r, other_csub, &seen);
    CHECK(seen.objects == 1);
    CHECK(client_ok(&r));
    cleanup(&r);
}

/* Adapter control for the subscriber's own cancellation of a request: the
 * bridge resets then stops the request stream. The publisher's end of its
 * half is injected at the server's transport here (a FIN, or a reset), not
 * produced by the server session, so the client adapter's handling of either
 * is pinned on its own: the stream retires once. A reset ends it at once; a
 * FIN, which picoquic withholds after the STOP, is reported only when
 * picoquic retires the stream on it -- after the client's reset is
 * acknowledged. */
static void bidi_cancel(moq_version_t v, bool peer_fin, uint8_t cid)
{
    row = row_name(peer_fin ? "bidi cancel control, injected peer FIN"
                            : "bidi cancel control, injected peer reset", v);
    rig_t r;
    if (setup(&r, cid, v, 0) != 0) {
        CHECK(false); cleanup(&r); return;
    }
    moq_subscription_t csub, other_csub;
    moq_subscription_t ss = subscribe(&r, &csub);
    CHECK(moq_subscription_is_valid(ss));
    uint64_t bid = request_bidi(&r);
    CHECK(bid != UINT64_MAX);
    moq_subscription_t other = subscribe(&r, &other_csub);
    CHECK(moq_subscription_is_valid(other));
    size_t base = moq_pq_conn_active_stream_count(r.cc);
    spy_on(&r, bid);
    CHECK(moq_session_unsubscribe(r.cs, csub, r.now) == MOQ_OK);
    moq_pq_service(r.cc, r.now);
    picoquic_stream_head_t *st = client_stream(&r, bid);
    CHECK(st != NULL && st->app_stream_ctx == (void *)r.cc);
    CHECK(st != NULL && st->reset_requested && st->stop_sending_requested);
    /* The publisher's end of its half is already on its way when the
     * client's reset and STOP leave. */
    picoquic_cnx_t *srv = r.t->cnx_server;
    CHECK(srv != NULL);
    if (srv != NULL) {
        if (peer_fin)
            CHECK(picoquic_add_to_stream(srv, bid, NULL, 0, 1) == 0);
        else
            CHECK(picoquic_reset_stream(srv, bid, 0x9) == 0);
    }
    int ack_round = -1, end_round = -1;
    uint64_t deadline = r.now + 2000000;
    for (int i = 0; i < 4000 && end_round < 0 && r.now < deadline; i++) {
        if (one_round(&r, deadline, NULL) != 0) break;
        st = client_stream(&r, bid);
        if (ack_round < 0 && (st == NULL || st->reset_acked)) ack_round = i;
        if (moq_pq_conn_active_stream_count(r.cc) + 1 == base) end_round = i;
    }
    CHECK(end_round >= 0);
    /* A release waits for the client's own reset to be acknowledged; the
     * peer's reset ends the request at once. */
    if (peer_fin) CHECK(ack_round >= 0 && end_round >= ack_round);
    pump(&r, 300);
    CHECK(moq_pq_conn_active_stream_count(r.cc) + 1 == base);
    CHECK(!moq_pq_test_rx_state(r.cc, bid, &(moq_pq_test_rx_t){0}));
    if (peer_fin) {
        /* picoquic retired the request stream on the publisher's FIN; the
         * terminal the adapter reported retired the bridge's aborting
         * entry. */
        CHECK(r.spy.resets == 0);
        CHECK(moq_pq_test_release_terminals(r.cc) == 1);
        CHECK(r.spy.fin_at_release == 1 && r.spy.reset_at_release == 0);
    } else {
        CHECK(r.spy.resets == 1);
        CHECK(moq_pq_test_release_terminals(r.cc) == 0);
    }
    CHECK(r.spy.commits == 0);
    moq_subgroup_handle_t h = open_sg(&r, other, 0);
    CHECK(write_obj(&r, h, 0) == MOQ_OK);
    pump(&r, 200);
    seen_t seen = {0};
    drain(&r, other_csub, &seen);
    CHECK(seen.objects == 1);
    CHECK(client_ok(&r));
    printf("  %s: client reset acknowledged at round %d, stream retired at "
           "round %d\n", row, ack_round, end_round);
    cleanup(&r);
}

/* The subscriber's cancellation of a request against a real libmoq publisher:
 * nothing is injected; the server session and its adapter answer over
 * picoquic. With `crossing_done` the publisher ends the subscription in the
 * same instant, so its PUBLISH_DONE and FIN cross the client's reset and STOP.
 * Either way the request stream retires exactly once on both ends, each end
 * through one terminal, neither connection turns fatal, and an unrelated
 * subscription still delivers. */
static void bidi_cancel_real_peer(moq_version_t v, bool crossing_done, uint8_t cid)
{
    row = row_name(crossing_done ? "bidi cancel, real peer, crossing done"
                                 : "bidi cancel, real peer", v);
    rig_t r;
    if (setup(&r, cid, v, 0) != 0) {
        CHECK(false); cleanup(&r); return;
    }
    moq_subscription_t csub, other_csub;
    moq_subscription_t ss = subscribe(&r, &csub);
    CHECK(moq_subscription_is_valid(ss));
    uint64_t bid = request_bidi(&r);
    CHECK(bid != UINT64_MAX);
    moq_subscription_t other = subscribe(&r, &other_csub);
    CHECK(moq_subscription_is_valid(other));
    size_t base = moq_pq_conn_active_stream_count(r.cc);
    size_t sbase = moq_pq_conn_active_stream_count(r.sc);
    spy_on(&r, bid);
    if (crossing_done) {
        moq_done_subscribe_cfg_t dc;
        moq_done_subscribe_cfg_init(&dc);
        CHECK(moq_session_done_subscribe(r.ss, ss, &dc, r.now) == MOQ_OK);
        moq_pq_service(r.sc, r.now);
    }
    CHECK(moq_session_unsubscribe(r.cs, csub, r.now) == MOQ_OK);
    moq_pq_service(r.cc, r.now);
    picoquic_stream_head_t *st = client_stream(&r, bid);
    CHECK(st != NULL && st->reset_requested && st->stop_sending_requested);
    int end_round = -1;
    uint64_t deadline = r.now + 2000000;
    for (int i = 0; i < 4000 && end_round < 0 && r.now < deadline; i++) {
        if (one_round(&r, deadline, NULL) != 0) break;
        if (moq_pq_conn_active_stream_count(r.cc) + 1 == base &&
            moq_pq_conn_active_stream_count(r.sc) + 1 == sbase)
            end_round = i;
    }
    CHECK(end_round >= 0);
    pump(&r, 300);
    CHECK(moq_pq_conn_active_stream_count(r.cc) + 1 == base);
    CHECK(moq_pq_conn_active_stream_count(r.sc) + 1 == sbase);
    CHECK(!moq_pq_test_rx_state(r.cc, bid, &(moq_pq_test_rx_t){0}));
    CHECK(!moq_pq_test_rx_state(r.sc, bid, &(moq_pq_test_rx_t){0}));
    uint64_t terminals = moq_pq_test_release_terminals(r.cc);
    /* The client's one terminal. A plain cancel is answered by the
     * publisher's reset. With the done crossing it, this ordering is
     * deterministic in the simulator: the publisher's FIN, which picoquic
     * withholds after the client's STOP, ends the stream through picoquic's
     * release. (Which end a real race takes is not a protocol requirement.) */
    CHECK(r.spy.resets + terminals == 1);
    if (crossing_done)
        CHECK(terminals == 1 && r.spy.resets == 0 && r.spy.fin_at_release == 1);
    else
        CHECK(r.spy.resets == 1 && terminals == 0);
    CHECK(r.spy.commits == 0);
    CHECK(!moq_pq_conn_is_fatal(r.sc));
    moq_subgroup_handle_t h = open_sg(&r, other, 0);
    CHECK(write_obj(&r, h, 0) == MOQ_OK);
    pump(&r, 200);
    seen_t seen = {0};
    drain(&r, other_csub, &seen);
    CHECK(seen.objects == 1);
    CHECK(client_ok(&r));
    printf("  %s: retired on both ends at round %d through the publisher's %s\n",
           row, end_round, terminals ? "FIN (stream released)" : "reset");
    cleanup(&r);
}

/* The publisher's endpoint reset contract, exercised beneath its bridge on
 * the server's own streams: reserved IDs, idempotence before and after the
 * provider retires a stream, refusals that create nothing, and the two
 * terminal-send cases. The connection and an unrelated subscription carry on
 * throughout. */
static void reset_contract(moq_version_t v, uint8_t cid)
{
    row = row_name("reset contract", v);
    rig_t r;
    if (setup(&r, cid, v, 0) != 0) {
        CHECK(false); cleanup(&r); return;
    }
    moq_subscription_t csub, other_csub;
    moq_subscription_t ss = subscribe(&r, &csub);
    moq_subscription_t other = subscribe(&r, &other_csub);
    CHECK(moq_subscription_is_valid(ss) && moq_subscription_is_valid(other));
    const moq_transport_endpoint_ops_t *ops;
    void *ectx;
    moq_pq_test_endpoint(r.sc, &ops, &ectx);
    picoquic_cnx_t *srv = r.t->cnx_server;
    picoquic_stream_head_t *st;

    /* Opened but unwritten: the ID is reserved at the provider, a second
     * open gets a fresh one, and a reset of either is taken. A repeat keeps
     * the first cause. */
    uint64_t a = UINT64_MAX, b = UINT64_MAX;
    CHECK(ops->open_uni(ectx, &a) == MOQ_TRANSPORT_OK);
    CHECK(ops->open_uni(ectx, &b) == MOQ_TRANSPORT_OK);
    CHECK(a != b);
    CHECK(picoquic_find_stream(srv, a) != NULL);
    CHECK(picoquic_find_stream(srv, b) != NULL);
    CHECK(ops->reset_stream(ectx, a, 0x2) == MOQ_TRANSPORT_OK);
    st = picoquic_find_stream(srv, a);
    CHECK(st != NULL && st->reset_requested && st->local_error == 0x2);
    CHECK(ops->reset_stream(ectx, a, 0x3) == MOQ_TRANSPORT_OK);
    st = picoquic_find_stream(srv, a);
    CHECK(st != NULL && st->local_error == 0x2);
    CHECK(ops->reset_stream(ectx, b, 0x2) == MOQ_TRANSPORT_OK);

    uint64_t ba = UINT64_MAX, bb = UINT64_MAX;
    CHECK(ops->open_bidi(ectx, &ba) == MOQ_TRANSPORT_OK);
    CHECK(ops->open_bidi(ectx, &bb) == MOQ_TRANSPORT_OK);
    CHECK(ba != bb);
    CHECK(picoquic_find_stream(srv, ba) != NULL);
    CHECK(picoquic_find_stream(srv, bb) != NULL);
    CHECK(ops->reset_stream(ectx, ba, 0x2) == MOQ_TRANSPORT_OK);
    CHECK(ops->reset_stream(ectx, bb, 0x2) == MOQ_TRANSPORT_OK);
    st = picoquic_find_stream(srv, ba);
    CHECK(st != NULL && st->reset_requested && st->local_error == 0x2);
    st = picoquic_find_stream(srv, bb);
    CHECK(st != NULL && st->reset_requested && st->local_error == 0x2);
    pump(&r, 300);

    /* After retirement: still a no-op, and nothing comes back to life. */
    CHECK(picoquic_find_stream(srv, a) == NULL);
    uint64_t next = picoquic_get_next_local_stream_id(srv, 1);
    CHECK(ops->reset_stream(ectx, a, 0x4) == MOQ_TRANSPORT_OK);
    CHECK(picoquic_find_stream(srv, a) == NULL);
    CHECK(picoquic_get_next_local_stream_id(srv, 1) == next);

    /* Never opened or receive-only: refused, nothing made. Out-of-range IDs
     * are tested only by the bounded provider stubs in test_pq_hold_input. */
    CHECK(ops->reset_stream(ectx, next + 8, 0x4) == MOQ_TRANSPORT_ERROR);
    CHECK(picoquic_find_stream(srv, next) == NULL);
    CHECK(picoquic_find_stream(srv, next + 8) == NULL);
    CHECK(picoquic_get_next_local_stream_id(srv, 1) == next);
    /* The client's first unidirectional stream: its control stream on the
     * uni-control drafts (live), absent on draft-16. */
    picoquic_stream_head_t *ctl = picoquic_find_stream(srv, 2);
    CHECK((ctl != NULL) == (v != MOQ_VERSION_DRAFT_16));
    CHECK(ops->reset_stream(ectx, 2, 0x4) == MOQ_TRANSPORT_ERROR);
    ctl = picoquic_find_stream(srv, 2);
    CHECK(ctl == NULL || !ctl->reset_requested);

    /* Every byte and the FIN acknowledged: terminal, so no reset is sent. */
    uint64_t done_sid = picoquic_get_next_local_stream_id(srv, 1);
    moq_subgroup_handle_t h = open_sg(&r, ss, 0);
    CHECK(write_obj(&r, h, 0) == MOQ_OK);
    CHECK(moq_session_close_subgroup(r.ss, h, r.now) == MOQ_OK);
    spy_on(&r, done_sid);
    pump(&r, 300);
    st = picoquic_find_stream(srv, done_sid);
    CHECK(st == NULL || (st->fin_sent &&
          picoquic_check_sack_list(&st->sack_list, 0, st->sent_offset) != 0));
    CHECK(ops->reset_stream(ectx, done_sid, 0x5) == MOQ_TRANSPORT_OK);
    st = picoquic_find_stream(srv, done_sid);
    CHECK(st == NULL || !st->reset_requested);
    pump(&r, 200);
    CHECK(r.spy.resets == 0);

    /* The FIN sent but not yet acknowledged: the reset is still taken. */
    uint64_t sent_sid = picoquic_get_next_local_stream_id(srv, 1);
    moq_subgroup_handle_t h2 = open_sg(&r, ss, 1);
    CHECK(write_obj(&r, h2, 0) == MOQ_OK);
    CHECK(moq_session_close_subgroup(r.ss, h2, r.now) == MOQ_OK);
    bool fin_unacked = false;
    for (int i = 0; i < 400 && !fin_unacked; i++) {
        if (one_round(&r, r.now + 1000000, NULL) != 0) break;
        st = picoquic_find_stream(srv, sent_sid);
        fin_unacked = st != NULL && st->fin_sent &&
            picoquic_check_sack_list(&st->sack_list, 0, st->sent_offset) == 0;
    }
    CHECK(fin_unacked);
    if (fin_unacked) {
        CHECK(ops->reset_stream(ectx, sent_sid, 0x6) == MOQ_TRANSPORT_OK);
        st = picoquic_find_stream(srv, sent_sid);
        CHECK(st != NULL && st->reset_requested && st->local_error == 0x6);
    }
    pump(&r, 300);

    /* Unrelated delivery is unchanged, and neither end is fatal. */
    moq_subgroup_handle_t ho = open_sg(&r, other, 0);
    CHECK(write_obj(&r, ho, 0) == MOQ_OK);
    pump(&r, 200);
    seen_t seen = {0};
    drain(&r, other_csub, &seen);
    CHECK(seen.objects == 1);
    CHECK(client_ok(&r));
    CHECK(!moq_pq_conn_is_fatal(r.sc));
    cleanup(&r);
}

/* mode 0: repeat a live STOP; 1: peer RESET first; 2: completed FIN first.
 * The server-side spy counts actual STOP deliveries, not endpoint attempts.
 * Terminal probes run inside the genuine receive callback, while the provider
 * still holds the stream, and are followed by the retired-stream no-op. */
static void stop_contract(moq_version_t v, unsigned mode, uint8_t cid)
{
    row = row_name(mode == 0 ? "stop contract live" :
                   mode == 1 ? "stop contract peer reset" : "stop contract FIN", v);
    rig_t r;
    if (setup(&r, cid, v, 0) != 0) {
        CHECK(false); cleanup(&r); return;
    }
    moq_subscription_t csub, other_csub;
    moq_subscription_t ss = subscribe(&r, &csub);
    moq_subscription_t other = subscribe(&r, &other_csub);
    CHECK(moq_subscription_is_valid(ss) && moq_subscription_is_valid(other));
    moq_subgroup_handle_t h = open_sg(&r, ss, 0);
    CHECK(write_obj(&r, h, 0) == MOQ_OK);
    pump(&r, 100);
    seen_t seen = {0};
    drain(&r, csub, &seen);
    CHECK(seen.objects == 1);
    uint64_t sid[4] = {UINT64_MAX, UINT64_MAX, UINT64_MAX, UINT64_MAX};
    CHECK(tracked_server_uni(&r, sid, 4) == 1);
    spy_on(&r, sid[0]);
    picoquic_cnx_t *cnx = r.t->cnx_client;
    picoquic_cnx_t *srv = r.t->cnx_server;
    spy_t peer = {
        .next = picoquic_get_callback_function(srv),
        .next_ctx = picoquic_get_callback_context(srv),
        .sid = sid[0]
    };
    picoquic_set_callback(srv, spy_cb, &peer);
    const moq_transport_endpoint_ops_t *ops;
    void *ectx;
    moq_pq_test_endpoint(r.cc, &ops, &ectx);

    if (mode == 0) {
        CHECK(ops->stop_sending(ectx, sid[0], 0x41) == MOQ_TRANSPORT_OK);
        picoquic_misc_frame_header_t *first = cnx->first_misc_frame;
        picoquic_misc_frame_header_t *last = cnx->last_misc_frame;
        CHECK(ops->stop_sending(ectx, sid[0], 0x42) == MOQ_TRANSPORT_OK);
        CHECK(cnx->first_misc_frame == first && cnx->last_misc_frame == last);
        picoquic_stream_head_t *st = client_stream(&r, sid[0]);
        CHECK(st != NULL && st->stop_sending_requested &&
              st->local_stop_error == 0x41 && st->app_stream_ctx == r.cc);
        moq_pq_test_rx_t rx;
        CHECK(moq_pq_test_rx_state(r.cc, sid[0], &rx) && rx.release_watch);
    } else {
        r.spy.terminal_stop_ops = ops;
        r.spy.terminal_stop_ctx = ectx;
        if (mode == 1)
            CHECK(moq_session_reset_subgroup(r.ss, h, 0x7, r.now) == MOQ_OK);
        else
            CHECK(moq_session_close_subgroup(r.ss, h, r.now) == MOQ_OK);
    }
    pump(&r, 500);
    CHECK(client_stream(&r, sid[0]) == NULL);
    CHECK(peer.stops == (mode == 0 ? 1u : 0u));
    CHECK(r.spy.resets == (mode == 2 ? 0u : 1u));
    CHECK(r.spy.terminal_stop_checks == (mode == 0 ? 0u : 1u));
    CHECK(r.spy.marked_released == 0 && r.spy.commits == 0);
    CHECK(r.spy.released == 0);
    CHECK(moq_pq_test_release_terminals(r.cc) == 0);
    CHECK(!moq_pq_test_rx_state(r.cc, sid[0], &(moq_pq_test_rx_t){0}));

    uint64_t next = cnx->next_stream_id[3];
    uint64_t credit = cnx->max_streams_unidir_local_computed;
    picoquic_misc_frame_header_t *first = cnx->first_misc_frame;
    picoquic_misc_frame_header_t *last = cnx->last_misc_frame;
    CHECK(ops->stop_sending(ectx, sid[0], 0x43) == MOQ_TRANSPORT_OK);
    CHECK(ops->stop_sending(ectx, sid[0], 0x44) == MOQ_TRANSPORT_OK);
    CHECK(client_stream(&r, sid[0]) == NULL);
    CHECK(cnx->next_stream_id[3] == next);
    CHECK(cnx->max_streams_unidir_local_computed == credit);
    CHECK(cnx->first_misc_frame == first && cnx->last_misc_frame == last);
    CHECK(!moq_pq_test_rx_state(r.cc, sid[0], &(moq_pq_test_rx_t){0}));

    /* Finite future ID and a local send-only uni: neither may become a
     * successful terminal no-op or acquire an adapter watch. */
    CHECK(ops->stop_sending(ectx, next + 8, 0x45) == MOQ_TRANSPORT_ERROR);
    CHECK(client_stream(&r, next + 8) == NULL);
    CHECK(cnx->next_stream_id[3] == next);
    picoquic_stream_head_t *ctl = client_stream(&r, 2);
    CHECK((ctl != NULL) == (v != MOQ_VERSION_DRAFT_16));
    void *ctl_ctx = ctl ? ctl->app_stream_ctx : NULL;
    CHECK(ops->stop_sending(ectx, 2, 0x46) == MOQ_TRANSPORT_ERROR);
    ctl = client_stream(&r, 2);
    CHECK(ctl == NULL || (!ctl->stop_sending_requested && ctl->app_stream_ctx == ctl_ctx));
    CHECK(!moq_pq_test_rx_state(r.cc, next + 8, &(moq_pq_test_rx_t){0}));

    pump(&r, 200);
    CHECK(peer.stops == (mode == 0 ? 1u : 0u));
    CHECK(r.spy.resets == (mode == 2 ? 0u : 1u));
    CHECK(r.spy.marked_released == 0 && r.spy.commits == 0);
    CHECK(r.spy.released == 0);
    CHECK(moq_pq_test_release_terminals(r.cc) == 0);
    CHECK(cnx->max_streams_unidir_local_computed == credit);
    moq_subgroup_handle_t ho = open_sg(&r, other, 0);
    CHECK(write_obj(&r, ho, 0) == MOQ_OK);
    pump(&r, 200);
    seen_t unrelated = {0};
    drain(&r, other_csub, &unrelated);
    CHECK(unrelated.objects == 1);
    CHECK(client_ok(&r));
    CHECK(!moq_pq_conn_is_fatal(r.sc));
    cleanup(&r);
}

/* Seven streams stopped by the adapter while the subscription lives on, then
 * four more streams grow the receive table before the seven end. */
static void growth(uint8_t cid)
{
    row = "growth d16";
    rig_t r;
    g_moving_alloc = true;
    int rc = setup(&r, cid, MOQ_VERSION_DRAFT_16, 0);
    g_moving_alloc = false;
    if (rc != 0) { CHECK(false); cleanup(&r); return; }
    moq_subscription_t csub;
    moq_subscription_t ss = subscribe(&r, &csub);
    moq_subgroup_handle_t h[12];
    /* A worse publisher priority than the later groups': the new streams'
     * data goes out before these streams' FINs, which would end them. */
    for (int g = 0; g < 7; g++) {
        moq_subgroup_cfg_t c;
        moq_subgroup_cfg_init(&c);
        c.group_id = (uint64_t)g;
        c.publisher_priority = 1;
        CHECK(moq_session_open_subgroup(r.ss, ss, &c, r.now, &h[g]) == MOQ_OK);
        CHECK(write_obj(&r, h[g], 0) == MOQ_OK);
    }
    pump(&r, 200);
    uint64_t sid[16];
    CHECK(tracked_server_uni(&r, sid, 16) == 7);
    const moq_transport_endpoint_ops_t *ops;
    void *ectx;
    moq_pq_test_endpoint(r.cc, &ops, &ectx);
    for (int g = 0; g < 7; g++) {
        CHECK(write_obj(&r, h[g], 1) == MOQ_OK);
        CHECK(moq_session_close_subgroup(r.ss, h[g], r.now) == MOQ_OK);
    }
    moq_pq_service(r.sc, r.now);
    for (int g = 0; g < 7; g++)
        CHECK(ops->stop_sending(ectx, sid[g], 0x1) == MOQ_TRANSPORT_OK);
    /* The new streams grow the receive table while the seven are watched;
     * this allocator moves it, so a context pointing into the old table
     * would now point at freed memory. */
    const void *base_before = moq_pq_test_rx_base(r.cc);
    for (int g = 7; g < 11; g++) {
        h[g] = open_sg(&r, ss, (uint64_t)g);
        CHECK(write_obj(&r, h[g], 0) == MOQ_OK);
    }
    pump(&r, 500);
    CHECK(moq_pq_test_rx_base(r.cc) != base_before);
    CHECK(moq_pq_test_release_terminals(r.cc) == 7);
    seen_t seen = {0};
    drain(&r, csub, &seen);
    for (int g = 0; g < 7; g++)
        CHECK(!moq_pq_test_rx_state(r.cc, sid[g], &(moq_pq_test_rx_t){0}));
    uint64_t live[16];
    size_t nlive = tracked_server_uni(&r, live, 16);
    CHECK(nlive == 4);
    /* A late release for an ended stream never ends a stream now in its
     * slot, and a release naming a live stream the adapter does not watch
     * leaves that stream's record as it is. */
    for (int g = 0; g < 7; g++)
        CHECK(moq_pq_callback(r.t->cnx_client, sid[g], NULL, 0,
                              picoquic_callback_stream_released, r.cc, r.cc) == 0);
    CHECK(moq_pq_callback(r.t->cnx_client, live[0], NULL, 0,
                          picoquic_callback_stream_released, r.cc, r.cc) == 0);
    CHECK(tracked_server_uni(&r, live, 16) == nlive);
    for (int g = 7; g < 11; g++)
        CHECK(write_obj(&r, h[g], 1) == MOQ_OK);
    pump(&r, 300);
    unsigned before_late = seen.objects;
    drain(&r, csub, &seen);
    CHECK(seen.objects == before_late + 4);
    CHECK(client_ok(&r));
    cleanup(&r);
}

static void setter_failure(uint8_t cid)
{
    row = "setter_failure d16";
    rig_t r;
    if (setup(&r, cid, MOQ_VERSION_DRAFT_16, 0) != 0) {
        CHECK(false); cleanup(&r); return;
    }
    moq_subscription_t csub;
    moq_subscription_t ss = subscribe(&r, &csub);
    moq_subgroup_handle_t h = open_sg(&r, ss, 0);
    CHECK(write_obj(&r, h, 0) == MOQ_OK);
    pump(&r, 100);
    uint64_t sid[4];
    CHECK(tracked_server_uni(&r, sid, 4) == 1);
    const moq_transport_endpoint_ops_t *ops;
    void *ectx;
    moq_pq_test_endpoint(r.cc, &ops, &ectx);
    moq_pq_test_fail_stream_ctx(1);
    CHECK(ops->stop_sending(ectx, sid[0], 0x1) == MOQ_TRANSPORT_ERROR);
    moq_pq_test_fail_stream_ctx(0);
    cleanup(&r);
}

static void stop_after_reset(uint8_t cid)
{
    row = "stop_after_reset d16";
    rig_t r;
    if (setup(&r, cid, MOQ_VERSION_DRAFT_16, 0) != 0) {
        CHECK(false); cleanup(&r); return;
    }
    moq_subscription_t csub;
    moq_subscription_t ss = subscribe(&r, &csub);
    moq_subgroup_handle_t h = open_sg(&r, ss, 0);
    CHECK(write_obj(&r, h, 0) == MOQ_OK);
    pump(&r, 100);
    uint64_t sid[4];
    CHECK(tracked_server_uni(&r, sid, 4) == 1);
    CHECK(moq_session_reset_subgroup(r.ss, h, 0x7, r.now) == MOQ_OK);
    pump(&r, 300);
    const moq_transport_endpoint_ops_t *ops;
    void *ectx;
    moq_pq_test_endpoint(r.cc, &ops, &ectx);
    CHECK(ops->stop_sending(ectx, sid[0], 0x1) == MOQ_TRANSPORT_OK);
    CHECK(ops->stop_sending(ectx, sid[0], 0x2) == MOQ_TRANSPORT_OK);
    CHECK(!moq_pq_test_rx_state(r.cc, sid[0], &(moq_pq_test_rx_t){0}));
    CHECK(moq_pq_test_release_terminals(r.cc) == 0);
    cleanup(&r);
}

/* At each terminal the adapter reports: its record of the stream is gone and
 * the bridge retains nothing for it -- every retained byte went first. */
static struct { uint64_t sid; int seen, record_left, bridge_pending; } g_term;

static void term_observer(moq_pq_conn_t *c, uint64_t sid)
{
    if (sid != g_term.sid) return;
    g_term.seen++;
    g_term.record_left += moq_pq_test_rx_state(c, sid, &(moq_pq_test_rx_t){0});
    g_term.bridge_pending += moq_pq_test_bridge_pending(c, sid);
}

/* A stream the session is still draining (its event queue is full) is
 * stopped by the adapter while its bytes are retained, so its watch is set
 * with work still owed. A release that comes then is latched -- no
 * terminal, no credit, the record kept -- and once the session drains, the
 * retained bytes reach the bridge and exactly one terminal follows. The
 * session receives those bytes (they arrived before the STOP) and no FIN.
 * The release is delivered here directly, with the stream's picoquic context
 * withdrawn so picoquic cannot deliver a second one. */
enum { PAUSED_OBJS = 24 };

static void paused(uint8_t cid)
{
    row = "paused d16";
    rig_t r;
    if (setup(&r, cid, MOQ_VERSION_DRAFT_16, 4) != 0) {
        CHECK(false); cleanup(&r); return;
    }
    moq_subscription_t csub;
    moq_subscription_t ss = subscribe(&r, &csub);
    moq_subgroup_handle_t h = open_sg(&r, ss, 0);
    /* Enough bytes, in enough packets, that once the session holds, later
     * deliveries are retained by the adapter itself. */
    for (uint64_t o = 0; o < PAUSED_OBJS; o++)
        CHECK(write_big(&r, h, o, 1200) == MOQ_OK);
    pump(&r, 200);
    uint64_t sid[4];
    CHECK(tracked_server_uni(&r, sid, 4) == 1);
    moq_pq_test_rx_t rx;
    CHECK(moq_pq_test_rx_state(r.cc, sid[0], &rx) && rx.paused && !rx.buf_fin);
    const moq_transport_endpoint_ops_t *ops;
    void *ectx;
    moq_pq_test_endpoint(r.cc, &ops, &ectx);
    CHECK(ops->stop_sending(ectx, sid[0], 0x1) == MOQ_TRANSPORT_OK);
    CHECK(moq_pq_test_rx_state(r.cc, sid[0], &rx) && rx.paused &&
          rx.release_watch && !rx.buf_fin);
    uint64_t granted = rx.granted;
    printf("  paused: at the STOP the adapter retains %zu bytes, the bridge "
           "%s\n", rx.buf_len,
           moq_pq_test_bridge_pending(r.cc, sid[0]) ? "a chunk" : "nothing");
    /* Bytes are retained in the adapter itself, not only in the bridge. */
    CHECK(rx.buf_len > 0);

    memset(&g_term, 0, sizeof(g_term));
    g_term.sid = sid[0];
    moq_pq_test_terminal_observer = term_observer;
    picoquic_unlink_app_stream_ctx(r.t->cnx_client, sid[0]);
    CHECK(moq_pq_callback(r.t->cnx_client, sid[0], NULL, 0,
                          picoquic_callback_stream_released, r.cc, r.cc) == 0);
    /* Latched, not reported. */
    CHECK(moq_pq_test_release_terminals(r.cc) == 0);
    CHECK(moq_pq_test_rx_state(r.cc, sid[0], &rx) && rx.paused &&
          rx.released && !rx.release_watch && rx.granted == granted &&
          !rx.buf_fin);

    /* The session drains; the retained bytes go first, then the terminal.
     * Only the client is serviced here, with no packets exchanged, so the
     * publisher's answer to the STOP cannot overtake the drain. */
    unsigned finished = 0, other = 0;
    unsigned per_id[PAUSED_OBJS] = {0};
    for (int i = 0; i < 200; i++) {
        moq_event_t ev;
        while (moq_session_poll_events(r.cs, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) {
                uint64_t id = ev.u.object_received.object_id;
                if (id < PAUSED_OBJS) per_id[id]++; else other++;
            }
            if (ev.kind == MOQ_EVENT_SUBGROUP_FINISHED) finished++;
            moq_event_cleanup(&ev);
        }
        moq_pq_service(r.cc, r.now);
    }
    moq_pq_test_terminal_observer = NULL;
    CHECK(g_term.seen == 1);
    CHECK(g_term.record_left == 0 && g_term.bridge_pending == 0);
    CHECK(moq_pq_test_release_terminals(r.cc) == 1);
    CHECK(!moq_pq_test_rx_state(r.cc, sid[0], &rx));
    /* Every object written before the STOP -- those the session had queued
     * and those replayed from the retained bytes -- surfaces exactly once. */
    unsigned delivered = 0;
    for (int id = 0; id < PAUSED_OBJS; id++) {
        CHECK(per_id[id] == 1);
        delivered += per_id[id];
    }
    CHECK(other == 0);
    CHECK(finished == 0);   /* no FIN is claimed for the stopped stream */
    /* Later traffic replays nothing more and ends nothing more. */
    pump(&r, 300);
    moq_event_t ev;
    unsigned late = 0;
    while (moq_session_poll_events(r.cs, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED ||
            ev.kind == MOQ_EVENT_SUBGROUP_FINISHED)
            late++;
        moq_event_cleanup(&ev);
    }
    CHECK(late == 0);
    CHECK(moq_pq_test_release_terminals(r.cc) == 1);
    CHECK(client_ok(&r));
    printf("  paused: objects 0-%d each delivered once (%u), then one "
           "terminal\n", PAUSED_OBJS - 1, delivered);
    cleanup(&r);
}

/* A request stream this adapter stopped keeps its watch through the other
 * calls that clear picoquic's context: an active-mark write (an empty write
 * marks the stream active and sends nothing) and a reliable reset -- the
 * RESET_STREAM_AT itself, or, when picoquic refuses the reliable size, the
 * plain reset that replaces it. */
static void bidi_rebind(moq_version_t v, bool fallback, uint8_t cid)
{
    row = row_name(fallback ? "bidi rebind, reset_stream_at refused"
                            : "bidi rebind, write + reset_stream_at", v);
    rig_t r;
    g_reliable = true;
    int rc = setup(&r, cid, v, 0);
    g_reliable = false;
    if (rc != 0) { CHECK(false); cleanup(&r); return; }
    moq_subscription_t csub;
    moq_subscription_t ss = subscribe(&r, &csub);
    CHECK(moq_subscription_is_valid(ss));
    uint64_t bid = request_bidi(&r);
    CHECK(bid != UINT64_MAX);
    const moq_transport_endpoint_ops_t *ops;
    void *ectx;
    moq_pq_test_endpoint(r.cc, &ops, &ectx);
    CHECK(ops->stop_sending(ectx, bid, 0x1) == MOQ_TRANSPORT_OK);
    picoquic_stream_head_t *st = client_stream(&r, bid);
    CHECK(st != NULL && st->app_stream_ctx == (void *)r.cc);
    CHECK(ops->write(ectx, bid, NULL, 0, false) == MOQ_TRANSPORT_OK);
    st = client_stream(&r, bid);
    CHECK(st != NULL && st->app_stream_ctx == (void *)r.cc);
    /* Nothing of this stream is queued, so the reliable reset is issued at
     * once; a reliable size past what was sent is refused by picoquic and
     * the adapter falls back to a plain reset. */
    uint64_t reliable = fallback ? 1u << 20 : 0;
    CHECK(ops->reset_stream_at(ectx, bid, 0x1, reliable) == MOQ_TRANSPORT_OK);
    st = client_stream(&r, bid);
    CHECK(st != NULL && st->reset_requested);
    CHECK(st != NULL && st->app_stream_ctx == (void *)r.cc);
    CHECK(!moq_pq_conn_is_fatal(r.cc));
    cleanup(&r);
}

/* A setup that failed at the client adapter's constructor with every earlier
 * step in place, leaving the connection's callback with its owner. */
static bool refused_at_attach(const rig_t *r, int rc)
{
    return rc == -1 && r->phase == RIG_ATTACH && r->t != NULL &&
           r->cs != NULL && r->cc == NULL && r->ss == NULL && r->sc == NULL &&
           picoquic_get_callback_function(r->t->cnx_client) == r->pre_attach_cb &&
           picoquic_get_callback_context(r->t->cnx_client) == r->pre_attach_ctx;
}

#if !defined(PICOQUIC_STREAM_TERMINAL_CONTRACT_VERSION) || PICOQUIC_STREAM_TERMINAL_CONTRACT_VERSION < 1
/* Without the provider's stream terminal contract draft-22 refuses at the
 * constructor, and only there: the documented -1, *out cleared, nothing
 * allocated, no callback taken; the same connection accepts draft-16. */
static void d22_refused(uint8_t cid)
{
    row = "d22_refused";
    rig_t r;
    int rc = setup(&r, cid, MOQ_VERSION_DRAFT_22, 0);
    CHECK(refused_at_attach(&r, rc));
    if (r.phase == RIG_ATTACH && r.cs != NULL) {
        rig_t d16;
        memset(&d16, 0, sizeof(d16));
        d16.version = MOQ_VERSION_DRAFT_16;
        moq_session_t *ok = NULL;
        CHECK(mk_session(&d16, MOQ_PERSPECTIVE_CLIENT, &ok) == 0);
        const char *why = pq_check_constructor_refusal(r.t->cnx_client, r.cs, ok);
        if (why) fprintf(stderr, "d22_refused: %s\n", why);
        CHECK(why == NULL);
        if (ok) moq_session_destroy(ok);
    }
    cleanup(&r);
}
#endif

/* A setup failure before the constructor is not the capability refusal. */
static void unrelated_setup_failure(uint8_t cid)
{
    row = "unrelated_setup_failure";
    rig_t r;
    int rc = setup(&r, cid, (moq_version_t)17, 0);   /* no such profile */
    CHECK(rc == -1);
    CHECK(r.phase == RIG_SESSION);
    CHECK(!refused_at_attach(&r, rc));
    cleanup(&r);
}

#define RUN(call) do { fprintf(stderr, "row %s\n", #call); call; } while (0)

#if defined(PICOQUIC_STREAM_TERMINAL_CONTRACT_VERSION) && PICOQUIC_STREAM_TERMINAL_CONTRACT_VERSION >= 1
#define HAVE_D22 1
#else
#define HAVE_D22 0     /* draft-22 refuses to attach to this provider */
#endif

int main(void)
{
    const moq_version_t all[] = {
        MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_22 };
    const size_t nall = HAVE_D22 ? 3 : 2;
    uint8_t cid = 1;
    for (size_t i = 0; i < nall; i++) {
        RUN(fin_after_stop(all[i], cid++));
        RUN(fin_before_stop(all[i], cid++));
        RUN(peer_reset(all[i], cid++));
        RUN(teardown(all[i], cid++));
    }
    for (size_t i = 1; i < nall; i++) {
        RUN(bidi_rebind(all[i], false, cid++));
        RUN(bidi_rebind(all[i], true, cid++));
        RUN(bidi_stop_then_reset(all[i], cid++));
        RUN(bidi_cancel(all[i], true, cid++));
        RUN(bidi_cancel(all[i], false, cid++));
        RUN(bidi_cancel_real_peer(all[i], false, cid++));
        RUN(bidi_cancel_real_peer(all[i], true, cid++));
    }
    for (size_t i = 0; i < nall; i++) {
        RUN(reset_contract(all[i], cid++));
        for (unsigned mode = 0; mode < 3; mode++)
            RUN(stop_contract(all[i], mode, cid++));
    }
    RUN(growth(cid++));
    RUN(setter_failure(cid++));
    RUN(stop_after_reset(cid++));
    RUN(paused(cid++));
#if !HAVE_D22
    RUN(d22_refused(cid++));
#endif
    RUN(unrelated_setup_failure(cid++));
    if (failures) {
        fprintf(stderr, "pq_stop_release: %d failure(s)\n", failures);
        return 1;
    }
    printf("pq_stop_release: all rows passed\n");
    return 0;
}
