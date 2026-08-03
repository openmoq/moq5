/*
 * Per-stream RECEIVE backpressure over the REAL picoquic transport in the
 * deterministic simulator (tls_api): genuine QUIC packets, real stream ids,
 * virtual time -- no sleeps, no localhost, no elapsed-time assertions.
 *
 * What this proves (and what the fake-pair / conformance suites cannot):
 * the raw picoquic adapter's application-controlled flow control actually
 * ARRESTS the peer's SENDER at the operator-configured window when the
 * receiving application stalls -- it does not merely delay delivery while
 * the sender keeps filling the pipe.
 *
 * The distinction is the whole point. "Delivery delay" would look like the
 * server transmitting the entire object (sent_offset -> total) and the bytes
 * piling up in the receiver's buffers, seen late by the app. "Arrest" means
 * the server physically CANNOT advance: its stream send offset is pinned at
 * the small window the constrained receiver advertised, far below the object
 * size, with data queued and blocked -- and only resumes once the app drains.
 *
 * We observe this white-box on the SERVER's picoquic stream state (as the
 * upstream flow_control_test.c does with picoquic_internal.h):
 *   - stream->sent_offset  : bytes the server has put on the wire
 *   - stream->maxdata_remote: the send-side flow-control ceiling == the
 *                             MAX_STREAM_DATA the constrained client advertised
 * On the sender, picoquic clamps sent_offset to maxdata_remote (frames.c:
 * allowed_space <= maxdata_remote - sent_offset). So maxdata_remote IS the
 * peer's advertised window, and sent_offset == maxdata_remote << object_size
 * means the sender is window-blocked with more to send: arrested, not delayed.
 *
 * Scenario (draft-16, so the control stream is bidi and the ONLY server-
 * initiated unidirectional stream is the subgroup data stream we measure):
 *   1. Client advertises a small per-stream receive window (initial_max_
 *      stream_data_uni), and a large connection window so the PER-STREAM
 *      limit is the binding constraint.
 *   2. Subscribe / accept; server publishes a large multi-window object set
 *      (many KB, several windows) on one subgroup uni stream.
 *   3. STALL: the receiving app never polls its events, so the session's
 *      event queue fills, the bridge WOULD_BLOCKs, the adapter withholds
 *      window credit, and the server's sender freezes at the bounded window.
 *      Assert sent_offset stops advancing, stays far below the object size,
 *      and the connection is healthy (arrest, not a stall-into-error).
 *   4. RESUME: the app drains its events, which drives moq_pq_service ->
 *      the adapter replenishes credit -> the sender resumes and the full
 *      object set transfers, in order, byte-exact.
 *
 * The credit anchor under test is the picoquic_set_app_flow_control() call in
 * moq_picoquic.c and rebuild -- picoquic then auto-advances the window as it
 * delivers bytes, the sender runs past the bounded window, and the
 * "sender is arrested below the object size" assertion FAILS.
 */

#include <moq/moq.h>
#include <moq/picoquic.h>

#include <picoquictest_internal.h>
#include <picoquic_internal.h>
#include <picoquic_utils.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int failures = 0;
static const char *scenario = "flow_control";
#define CHECK(cond) do { \
    if (!(cond)) { \
        fprintf(stderr, "FAIL[%s]: %s:%d: %s\n", scenario, __FILE__, \
                __LINE__, #cond); \
        failures++; \
    } } while (0)

/* -- Scenario parameters ------------------------------------------------ *
 * The per-stream window is small; the connection window is large so the
 * per-stream limit binds. The object set is many windows' worth so the
 * arrested sender is unambiguously stuck mid-transfer. */
#define FC_STREAM_WINDOW   8192u          /* initial_max_stream_data_uni */
#define FC_CONN_WINDOW     (1u << 20)     /* initial_max_data (large)     */
#define FC_OBJ_COUNT       192            /* objects in the subgroup      */
#define FC_OBJ_SIZE        1024u          /* bytes per object             */
#define FC_TOTAL_BYTES     ((uint64_t)FC_OBJ_COUNT * FC_OBJ_SIZE)  /* 196608 */

/* Upper bound on how far the arrested sender may have advanced while the app
 * is stalled. The frozen window settles at roughly (events buffered by the
 * stalled session) + (one advertised window): max_events(16) * FC_OBJ_SIZE +
 * FC_STREAM_WINDOW ~= 24 KiB. Allow generous slack but stay far below the
 * object size so a "delivery delay" (sent_offset -> FC_TOTAL_BYTES) or a
 * "runaway" unmistakably trips it. */
#define FC_ARREST_CEILING  49152u         /* 48 KiB, << 192 KiB total */

/* -- sim pair (mirrors test_pq_sim_establish.c) ------------------------- */

typedef struct {
    picoquic_test_tls_api_ctx_t *test_ctx;
    uint64_t now;
    uint64_t loss;
    moq_version_t version;

    moq_session_t *client_session;
    moq_session_t *server_session;
    moq_pq_conn_t *client_conn;
    moq_pq_conn_t *server_conn;
    int            server_create_failed;
} pq_sim_t;

/* The client (receiver) is the constrained side; the server is the sender.
 * Give the server a generous send buffer / action queue so the ONLY thing
 * pacing it is the wire flow control -- never its own local send limits.
 * Leave the client's event queue at the small default (16) so a non-polling
 * app fills it quickly and the bridge WOULD_BLOCKs. */
static int sim_create_session(moq_version_t version, moq_perspective_t persp,
                              moq_session_t **out)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), persp);
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 16;
    cfg.version = version;
    if (persp == MOQ_PERSPECTIVE_SERVER) {
        cfg.send_buffer_size = 1u << 20;   /* hold the whole object set */
        cfg.max_actions = 1024;
    }
    return moq_session_create(&cfg, 0, out) == MOQ_OK ? 0 : -1;
}

static int sim_attach_conn(moq_session_t *session, picoquic_cnx_t *cnx,
                           moq_pq_conn_t **out)
{
    moq_pq_conn_cfg_t cfg;
    moq_pq_conn_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.session = session;
    cfg.cnx = cnx;
    cfg.alloc = moq_alloc_default();
    return moq_pq_conn_create(&cfg, out);
}

static int sim_server_cb(picoquic_cnx_t *cnx,
    uint64_t stream_id, uint8_t *bytes, size_t length,
    picoquic_call_back_event_t event, void *callback_ctx,
    void *stream_ctx)
{
    pq_sim_t *s = (pq_sim_t *)callback_ctx;
    (void)stream_id; (void)bytes; (void)length; (void)stream_ctx;

    if (event != picoquic_callback_almost_ready &&
        event != picoquic_callback_ready)
        return 0;
    if (s->server_conn)
        return 0;

    if (sim_create_session(s->version, MOQ_PERSPECTIVE_SERVER,
                           &s->server_session) != 0 ||
        sim_attach_conn(s->server_session, cnx, &s->server_conn) != 0) {
        s->server_create_failed = 1;
        return -1;
    }
    return 0;
}

static int sim_setup(pq_sim_t *s, uint8_t cid_byte, moq_version_t version)
{
    memset(s, 0, sizeof(*s));
    s->version = version;
    picoquic_solution_dir = PICOQUIC_SOURCE_DIR;

    picoquic_connection_id_t cid = {
        {0x6d, 0x71, 0xfc, cid_byte, 0, 0, 0, 0}, 8};

    if (tls_api_init_ctx_ex(&s->test_ctx, PICOQUIC_INTERNAL_TEST_VERSION_1,
                            PICOQUIC_TEST_SNI, PICOQUIC_TEST_ALPN, &s->now,
                            NULL, NULL, 0, 1, 0, &cid) != 0)
        return -1;

    picoquic_set_default_idle_timeout(s->test_ctx->qclient, 30000);
    picoquic_set_default_idle_timeout(s->test_ctx->qserver, 30000);

    /* Constrain the client's advertised RECEIVE windows: a small per-stream
     * window (the operator's configured limit, which the adapter freezes) and
     * a large connection window so the per-stream limit is the binding
     * constraint on the server's sender. Read-modify-write the client's local
     * transport parameters exactly as the establish test injects the datagram
     * TP -- the cnx already exists, so set its local TP directly before the
     * handshake advertises it. */
    picoquic_tp_t tp =
        *picoquic_get_transport_parameters(s->test_ctx->cnx_client, 1);
    tp.initial_max_stream_data_uni = FC_STREAM_WINDOW;
    tp.initial_max_data = FC_CONN_WINDOW;
    picoquic_set_transport_parameters(s->test_ctx->cnx_client, &tp);

    if (sim_create_session(version, MOQ_PERSPECTIVE_CLIENT,
                           &s->client_session) != 0)
        return -1;
    if (sim_attach_conn(s->client_session, s->test_ctx->cnx_client,
                        &s->client_conn) != 0)
        return -1;

    picoquic_set_default_callback(s->test_ctx->qserver, sim_server_cb, s);

    if (picoquic_start_client_cnx(s->test_ctx->cnx_client) != 0)
        return -1;
    if (tls_api_connection_loop(s->test_ctx, &s->loss, 0, &s->now) != 0)
        return -1;
    if (s->server_create_failed || !s->server_conn)
        return -1;
    return 0;
}

/* One sim round: service both adapters, move packets, service again. */
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

/* Pump up to `ms` of sim time or until 10 consecutive idle rounds. Does NOT
 * poll client events -- used both during handshake/subscribe and to let the
 * transport quiesce while the receiving APP stays stalled. */
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
    if (s->version == MOQ_VERSION_DRAFT_18 &&
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
    if (s->client_conn) { moq_pq_conn_destroy(s->client_conn);
                          s->client_conn = NULL; }
    if (s->server_conn) { moq_pq_conn_destroy(s->server_conn);
                          s->server_conn = NULL; }
    if (s->client_session) { moq_session_destroy(s->client_session);
                             s->client_session = NULL; }
    if (s->server_session) { moq_session_destroy(s->server_session);
                             s->server_session = NULL; }
    if (s->test_ctx) { tls_api_delete_ctx(s->test_ctx);
                       s->test_ctx = NULL; }
}

/* Subscribe + accept (mirrors test_pq_sim_establish.c). */
static int sim_subscribe_accept(pq_sim_t *s, moq_subscription_t *ss_out)
{
    int lf = 0;
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    moq_bytes_t ns[] = {{(const uint8_t *)"t", 1}};
    sc.track_namespace.parts = ns;
    sc.track_namespace.count = 1;
    sc.track_name = (moq_bytes_t){(const uint8_t *)"v", 1};
    sc.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
    moq_subscription_t sub;
    if (moq_session_subscribe(s->client_session, &sc, s->now, &sub) < 0) lf++;
    sim_pump(s, 200);

    moq_subscription_t ss = {0};
    bool got_req = false;
    moq_event_t ev;
    while (moq_session_poll_events(s->server_session, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
            ss = ev.u.subscribe_request.sub; got_req = true;
        }
        moq_event_cleanup(&ev);
    }
    if (!got_req) { lf++; return lf; }

    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    if (moq_session_accept_subscribe(s->server_session, ss, &ac, s->now) < 0) lf++;
    sim_pump(s, 200);

    bool sub_ok = false;
    while (moq_session_poll_events(s->client_session, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_OK) sub_ok = true;
        moq_event_cleanup(&ev);
    }
    if (!sub_ok) lf++;
    *ss_out = ss;
    return lf;
}

/* -- white-box sender observation --------------------------------------- *
 * Find the server's subgroup data stream. In draft-16 the control stream is a
 * client-initiated BIDI stream, so the only server-initiated UNIDIRECTIONAL
 * streams (id % 4 == 3: bit0=1 server-initiated, bit1=1 unidirectional) are
 * subgroup data streams. We publish exactly one subgroup, so there is one such
 * stream; pick the one that has actually carried bytes. */
static picoquic_stream_head_t *find_server_data_stream(picoquic_cnx_t *cnx)
{
    picoquic_stream_head_t *best = NULL;
    for (uint64_t id = 3; id < 256; id += 4) {
        picoquic_stream_head_t *st = picoquic_find_stream(cnx, id);
        if (st == NULL) continue;
        if (best == NULL || st->sent_offset > best->sent_offset)
            best = st;
    }
    return best;
}

/* -- publishing --------------------------------------------------------- *
 * Write objects 0..N-1 into the subgroup, each filled with a per-object byte
 * pattern for byte-exact verification. Writes what the server session will
 * currently accept (WOULD_BLOCK when its send path is full because the wire is
 * frozen) and returns the next unwritten object id -- so the caller can keep
 * feeding as the wire drains. */
static int try_write_objects(pq_sim_t *s, moq_subgroup_handle_t sg,
                             int next, int count)
{
    static uint8_t payload[FC_OBJ_SIZE];
    while (next < count) {
        memset(payload, (uint8_t)(next & 0xFF), FC_OBJ_SIZE);
        moq_rcbuf_t *buf = NULL;
        if (moq_rcbuf_create(moq_alloc_default(), payload, FC_OBJ_SIZE,
                             &buf) < 0)
            break;
        moq_result_t rc = moq_session_write_object(
            s->server_session, sg, (uint64_t)next, buf, s->now);
        moq_rcbuf_decref(buf);
        if (rc == MOQ_ERR_WOULD_BLOCK) break;  /* send path full; retry later */
        if (rc < 0) { CHECK(rc >= 0); break; }
        next++;
    }
    return next;
}

/* -- the arrest proof --------------------------------------------------- */

static void run_flow_control_arrest(uint8_t cid_byte)
{
    scenario = "flow_control_arrest";
    pq_sim_t s;
    CHECK(sim_setup(&s, cid_byte, MOQ_VERSION_DRAFT_16) == 0);
    if (!(s.test_ctx && s.client_conn && s.server_conn)) {
        sim_cleanup(&s); return;
    }
    CHECK(sim_handshake(&s) == 0);

    moq_subscription_t ss = {0};
    failures += sim_subscribe_accept(&s, &ss);

    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = 0;
    sgc.publisher_priority = 200;
    moq_subgroup_handle_t sg;
    CHECK(moq_session_open_subgroup(s.server_session, ss, &sgc, s.now, &sg) >= 0);

    /* -- Phase 1: publish + STALL. Feed objects to the server, then pump the
     * transport WITHOUT ever polling the client's events. The receiving app is
     * stalled: its event queue fills, the bridge WOULD_BLOCKs, the adapter
     * withholds window credit, and the server's sender must freeze. */
    int next = try_write_objects(&s, sg, 0, FC_OBJ_COUNT);
    for (int r = 0; r < 40; r++) {
        sim_pump(&s, 50);
        next = try_write_objects(&s, sg, next, FC_OBJ_COUNT);
    }

    /* The connection must be HEALTHY: an application stall becomes bounded QUIC
     * backpressure, never an error. (This is also what separates arrest from
     * an adapter with unbounded retention would fault here.) */
    CHECK(!moq_pq_conn_is_fatal(s.client_conn));
    CHECK(!moq_pq_conn_is_fatal(s.server_conn));

    picoquic_stream_head_t *dstr = find_server_data_stream(s.test_ctx->cnx_server);
    CHECK(dstr != NULL);
    uint64_t sent_1 = dstr ? dstr->sent_offset : 0;
    /* Connection-level MAX_DATA as the SENDER sees it: must also freeze during
     * the stall (derivative arrest -- both the receiver's auto-advance and the
     * adapter's grants are anchored to delivery/progress, which has stopped). */
    uint64_t conn_maxd_1 = s.test_ctx->cnx_server->maxdata_remote;

    /* Pump many MORE rounds, still never polling. If this were mere delivery
     * delay the sender would keep advancing toward the full object size. */
    for (int r = 0; r < 60; r++) {
        sim_pump(&s, 50);
        next = try_write_objects(&s, sg, next, FC_OBJ_COUNT);
    }

    dstr = find_server_data_stream(s.test_ctx->cnx_server);
    uint64_t sent_2 = dstr ? dstr->sent_offset : 0;
    uint64_t maxd_2 = dstr ? dstr->maxdata_remote : 0;

    fprintf(stderr,
            "[arrest] sender frozen at sent_offset=%llu maxdata_remote=%llu"
            " (object set=%llu bytes, %d objects written)\n",
            (unsigned long long)sent_2, (unsigned long long)maxd_2,
            (unsigned long long)FC_TOTAL_BYTES, next);

    /* ARREST ASSERTIONS -- these are the load-bearing proofs.
     *
     * (a) The sender is pinned FAR below the object size. A delivery-delay
     *     implementation would have sent_offset climb toward FC_TOTAL_BYTES;
     *     genuine backpressure caps it at the advertised window. */
    CHECK(sent_2 < FC_TOTAL_BYTES);
    CHECK(sent_2 <= FC_ARREST_CEILING);

    /* (b) The sender STOPPED. Across 60 additional rounds with the app stalled
     *     the send offset did not advance -- the window is frozen, not slowly
     *     draining. (Equality, not just <=, is what distinguishes arrest from a
     *     merely slow trickle.) */
    CHECK(sent_2 == sent_1);

    /* (c) The sender is window-blocked exactly at the advertised ceiling: it
     *     has consumed all the credit it was granted (sent_offset ==
     *     maxdata_remote) yet has far more queued -- it WANTS to send and
     *     cannot. That is arrest, not idleness. */
    CHECK(dstr != NULL && sent_2 == maxd_2);

    /* (d) And that frozen ceiling is the small window the client advertised,
     *     not something picoquic auto-grew. */
    CHECK(maxd_2 <= FC_ARREST_CEILING);

    /* (e) Connection-level MAX_DATA also stopped: no MAX_DATA frames were
     *     emitted across the 60 stalled rounds. Stream arrest starves delivery,
     *     which arrests both picoquic's delivery-anchored auto-advance and the
     *     adapter's progress-anchored grants. */
    uint64_t conn_maxd_2 = s.test_ctx->cnx_server->maxdata_remote;
    CHECK(conn_maxd_2 == conn_maxd_1);

    /* -- Phase 2: RESUME. Now the app DRAINS -- poll events every round, which
     * drives moq_pq_service -> the adapter replenishes window credit -> the
     * sender unfreezes. Continue until the whole object set arrives. Verify
     * order and content byte-exact: nothing dropped, duplicated, or reordered. */
    int received = 0;
    bool order_ok = true;
    bool content_ok = true;
    bool len_ok = true;
    /* Emitted-credit invariants, checked EVERY drain step on the receiver's
     * white-box state (what it advertises is what the sender's maxdata_*
     * become):
     *   - MAX_STREAM_DATA never overgrants: the stream window stays anchored at
     *     consumed_offset + the advertised budget (the adapter's target is
     *     processed + budget <= consumed + budget).
     * The data stream on the CLIENT side shares the server stream's id. */
    bool stream_window_bounded = true;
    uint64_t data_sid = dstr ? dstr->stream_id : 0;
    uint64_t deadline = s.now + 30000000;  /* 30 s virtual budget */
    /* Each outer step: drain the app's events (frees event-queue capacity ->
     * moq_pq_service replenishes window credit), keep feeding the publisher,
     * then let the transport run until it quiesces. The window ratchets forward
     * one bounded grant at a time until the whole object set arrives. */
    for (int step = 0; step < 5000 && received < FC_OBJ_COUNT; step++) {
        if (s.now > deadline) break;

        moq_event_t ev;
        while (moq_session_poll_events(s.client_session, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) {
                moq_object_received_event_t *o = &ev.u.object_received;
                if ((int)o->object_id != received) order_ok = false;
                size_t l = o->payload ? moq_rcbuf_len(o->payload) : 0;
                if (l != FC_OBJ_SIZE) len_ok = false;
                else {
                    const uint8_t *d = moq_rcbuf_data(o->payload);
                    uint8_t want = (uint8_t)(received & 0xFF);
                    for (size_t k = 0; k < l; k++)
                        if (d[k] != want) { content_ok = false; break; }
                }
                received++;
            }
            moq_event_cleanup(&ev);
        }

        next = try_write_objects(&s, sg, next, FC_OBJ_COUNT);
        sim_pump(&s, 50);

        /* Per-step MAX_STREAM_DATA bound: the receiver never advertises more
         * than consumed + budget on the data stream. */
        picoquic_stream_head_t *cstr =
            picoquic_find_stream(s.test_ctx->cnx_client, data_sid);
        if (cstr != NULL &&
            cstr->maxdata_local > cstr->consumed_offset + FC_STREAM_WINDOW)
            stream_window_bounded = false;
    }
    /* Final drain: pick up anything delivered on the last pump. */
    {
        moq_event_t ev;
        while (moq_session_poll_events(s.client_session, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) {
                moq_object_received_event_t *o = &ev.u.object_received;
                if ((int)o->object_id != received) order_ok = false;
                size_t l = o->payload ? moq_rcbuf_len(o->payload) : 0;
                if (l != FC_OBJ_SIZE) len_ok = false;
                else {
                    const uint8_t *d = moq_rcbuf_data(o->payload);
                    uint8_t want = (uint8_t)(received & 0xFF);
                    for (size_t k = 0; k < l; k++)
                        if (d[k] != want) { content_ok = false; break; }
                }
                received++;
            }
            moq_event_cleanup(&ev);
        }
    }

    /* All objects delivered once the app drained: the sender RESUMED. */
    CHECK(received == FC_OBJ_COUNT);
    CHECK(order_ok);       /* in order */
    CHECK(content_ok);     /* byte-exact */
    CHECK(len_ok);         /* full length each */

    /* The server was able to finish: its send offset reached the full object
     * set once credit flowed again. */
    dstr = find_server_data_stream(s.test_ctx->cnx_server);
    CHECK(dstr != NULL && dstr->sent_offset >= FC_TOTAL_BYTES);

    /* Emitted-credit bounds over the WHOLE run:
     *   - MAX_STREAM_DATA stayed anchored (consumed + budget) at every step. */
    CHECK(stream_window_bounded);
    /*   - MAX_DATA advanced bounded-proportionally to application progress:
     *     each batched grant adds its stream headroom (<= budget) to the
     *     connection window at most once per budget/2 of progress, so the total
     *     connection advance is <= 2x the bytes actually delivered (+ slack for
     *     the control stream's grants and one in-flight grant). Unbatched
     *     per-callback grants would inflate this by the callback count and blow
     *     the bound by an order of magnitude. */
    {
        uint64_t progress = s.test_ctx->cnx_client->data_received;
        /* Accounting boundary: this bound covers ADAPTER-INDUCED MAX_DATA
         * increments only. picoquic's independent automatic conn-window growth
         * triggers at 2*offset_received > maxdata_local; the deliberately large
         * FC_CONN_WINDOW keeps it dormant for this workload -- assert that
         * explicitly so the bound below is known to measure the adapter alone. */
        CHECK(2 * progress < FC_CONN_WINDOW);
        uint64_t conn_advance =
            s.test_ctx->cnx_client->maxdata_local > FC_CONN_WINDOW
                ? s.test_ctx->cnx_client->maxdata_local - FC_CONN_WINDOW : 0;
        CHECK(conn_advance <= 2 * progress + 4 * FC_STREAM_WINDOW);
    }

    CHECK(!moq_pq_conn_is_fatal(s.client_conn));
    CHECK(!moq_pq_conn_is_fatal(s.server_conn));

    fprintf(stderr,
            "[resume] app drained: received=%d/%d objects in order, byte-exact;"
            " sender advanced to sent_offset=%llu\n",
            received, FC_OBJ_COUNT,
            (unsigned long long)(dstr ? dstr->sent_offset : 0));

    sim_cleanup(&s);
}

int main(void)
{
    run_flow_control_arrest(0x01);

    if (failures == 0)
        printf("PASS: pq_sim_flow_control\n");
    return failures != 0;
}
