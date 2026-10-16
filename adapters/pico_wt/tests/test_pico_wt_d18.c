/*
 * Draft-18 MoQ over the REAL WebTransport transport in the deterministic
 * simulator: genuine QUIC + H3 + WT CONNECT through the actual pico_wt
 * adapter, with real WT stream ids.
 *
 * Pins the uni-control-pair model end to end on this transport:
 *   - symmetric establish (both endpoints open their own unidirectional
 *     control channel and send SETUP);
 *   - subscribe + object delivery: the client's first request bidi is the
 *     first client-initiated WT bidi -- exactly the stream the draft-16
 *     model treats as the MoQ control stream, so this pins the adapter's
 *     mode-aware classification on both sides;
 *   - a draft-16 establish + subscribe/object run as the harness control.
 */

#include "pico_wt_harness.h"
#include "pico_wt_test_seam.h"
#include "picoquic_internal.h"
#include <picoquic_utils.h>
#include <moq/moq.h>

#include <stdio.h>
#include <string.h>

static int failures = 0;
static const char *scenario = "";
#define CHECK(cond) do { \
    if (!(cond)) { \
        fprintf(stderr, "FAIL[%s]: %s:%d: %s\n", scenario, __FILE__, \
                __LINE__, #cond); \
        failures++; \
    } } while (0)


/* -- Peer-terminal observation ---------------------------------------- *
 * The adapter (test build) reports every RESET/STOP_SENDING callback at
 * its boundary. At the ENTRY of a RESET callback the native stream is still
 * live, so picoquic's own record of the received code can be read there;
 * after the exchange picoquic may already have deleted a fully closed
 * stream, which makes a later getter read meaningless (it returns 0 for an
 * unknown stream). Records are keyed by adapter connection, so client and
 * server are told apart by identity. */
typedef struct {
    moq_pico_wt_conn_t *conn;
    int      event;
    int      phase;
    uint64_t sid;
    bool     stream_present;
    uint64_t raw_code;        /* picoquic_get_remote_stream_error while live */
    uint64_t mapped_code;     /* pico_wt_wt_err_to_moq(raw_code), when mapped */
    bool     has_mapped_code; /* raw_code is a WebTransport application code */
    bool     reset_received;
    uint64_t reset_offset;    /* reliable prefix the RESET_STREAM_AT carried */
} term_rec_t;
#define TERM_MAX 64
static term_rec_t g_term[TERM_MAX];
static int g_term_n;
static bool g_term_overflow;          /* sticky: the record is incomplete */
static pico_wt_harness_t *g_term_h;

static void term_observer(moq_pico_wt_conn_t *c, int event, uint64_t sid, int phase)
{
    if (event != (int)picohttp_callback_reset && event != (int)picohttp_callback_stop_sending) return;
    if (g_term_n >= TERM_MAX) { g_term_overflow = true; return; }
    picoquic_cnx_t *cnx = c == g_term_h->server_conn ? g_term_h->test_ctx->cnx_server
                        : c == g_term_h->client_conn ? g_term_h->test_ctx->cnx_client : NULL;
    term_rec_t *r = &g_term[g_term_n++];
    memset(r, 0, sizeof(*r));
    r->conn = c; r->event = event; r->phase = phase; r->sid = sid;
    if (cnx == NULL) return;
    picoquic_stream_head_t *st = picoquic_find_stream(cnx, sid);
    r->stream_present = st != NULL;
    r->raw_code = picoquic_get_remote_stream_error(cnx, sid);
    r->has_mapped_code = pico_wt_wt_err_to_moq(r->raw_code, &r->mapped_code);
    if (st) { r->reset_received = st->reset_received != 0; r->reset_offset = st->reset_offset; }
}
static void term_install(pico_wt_harness_t *h)
{
    g_term_h = h; g_term_n = 0; g_term_overflow = false;
    memset(g_term, 0, sizeof(g_term));
    pw_test_callback_observer = term_observer;
}
static void term_remove(void) { pw_test_callback_observer = NULL; g_term_h = NULL; }

/* Count RESET-entry records on `conn` for `sid`; the single match (if any)
 * is copied to *out. */
static int term_reset_entries(moq_pico_wt_conn_t *conn, uint64_t sid, term_rec_t *out)
{
    int n = 0;
    for (int i = 0; i < g_term_n; i++) {
        const term_rec_t *r = &g_term[i];
        if (r->conn == conn && r->event == (int)picohttp_callback_reset && r->phase == 0 && r->sid == sid) {
            if (n == 0 && out) *out = *r;
            n++;
        }
    }
    return n;
}

static int run_subscribe_object(pico_wt_harness_t *h, const char *tname)
{
    int local_failures = 0;
#define SCHECK(cond) do { \
    if (!(cond)) { \
        fprintf(stderr, "FAIL[%s]: %s:%d: %s\n", scenario, __FILE__, \
                __LINE__, #cond); \
        local_failures++; \
    } } while (0)

    /* Client subscribes (draft-18: opens a request bidi -- the first
     * client-initiated WT bidi inside the WT session). */
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    moq_bytes_t ns[] = {{(const uint8_t *)"t", 1}};
    sc.track_namespace.parts = ns;
    sc.track_namespace.count = 1;
    sc.track_name = (moq_bytes_t){(const uint8_t *)tname, strlen(tname)};
    sc.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
    moq_subscription_t sub;
    SCHECK(moq_session_subscribe(h->client_session, &sc, h->now, &sub) >= 0);

    pico_wt_harness_pump(h, 200);

    /* Server sees the request and accepts. */
    moq_subscription_t ss = {0};
    bool got_req = false;
    moq_event_t ev;
    while (moq_session_poll_events(h->server_session, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
            ss = ev.u.subscribe_request.sub;
            got_req = true;
        }
        moq_event_cleanup(&ev);
    }
    SCHECK(got_req);
    if (!got_req) return local_failures;

    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    SCHECK(moq_session_accept_subscribe(h->server_session, ss, &ac,
                                        h->now) >= 0);
    pico_wt_harness_pump(h, 200);

    bool sub_ok = false;
    while (moq_session_poll_events(h->client_session, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_OK) sub_ok = true;
        moq_event_cleanup(&ev);
    }
    SCHECK(sub_ok);
    if (!sub_ok) return local_failures;

    /* Server publishes one object in a subgroup (WT uni data stream --
     * in draft-18 it must classify as DATA alongside the uni control
     * pair). */
    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = 0;
    sgc.publisher_priority = 200;
    moq_subgroup_handle_t sg;
    SCHECK(moq_session_open_subgroup(h->server_session, ss, &sgc,
                                     h->now, &sg) >= 0);

    const uint8_t payload[] = {0xCA, 0xFE};
    moq_rcbuf_t *buf = NULL;
    SCHECK(moq_rcbuf_create(moq_alloc_default(), payload, 2, &buf) >= 0);
    SCHECK(moq_session_write_object(h->server_session, sg, 0, buf,
                                    h->now) >= 0);
    moq_rcbuf_decref(buf);
    SCHECK(moq_session_close_subgroup(h->server_session, sg, h->now) >= 0);

    pico_wt_harness_pump(h, 200);

    bool got_obj = false;
    while (moq_session_poll_events(h->client_session, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) {
            moq_object_received_event_t *o = &ev.u.object_received;
            if (o->payload && moq_rcbuf_len(o->payload) == 2) {
                const uint8_t *d = moq_rcbuf_data(o->payload);
                if (d[0] == 0xCA && d[1] == 0xFE) got_obj = true;
            }
        }
        moq_event_cleanup(&ev);
    }
    SCHECK(got_obj);

    SCHECK(!moq_pico_wt_conn_is_fatal(h->client_conn));
    SCHECK(!moq_pico_wt_conn_is_fatal(h->server_conn));
#undef SCHECK
    return local_failures;
}

/*
 * PUBLISH_NAMESPACE_DONE over the real WT transport. Draft-18 has no
 * DONE message: the announcer withdraws by aborting the announce request
 * bidi (RESET_STREAM + STOP_SENDING, §3.3.2/3.3.3). Over WebTransport the
 * abort error code must be mapped into the WT application-error space
 * (§4.4): a raw MoQ code is not a valid WebTransport error, so a spec
 * peer receives a malformed code. (The teardown that surfaced in interop
 * was a distinct problem: picowt_reset_stream requests a reliable_size
 * preamble, which picoquic refuses locally when reliable-reset was not
 * negotiated -- a fatal bridge error that closes the connection; covered by
 * the managed announce_done tests and the synthesized branch below.)
 *
 * This drives announce -> accept -> done and asserts the receiver still
 * sees NAMESPACE_DONE, both sessions stay ESTABLISHED, neither adapter is
 * fatal, and the WT connection is still usable afterwards. For draft-18 it
 * also reads picoquic's own record of the received reset code to pin the
 * on-wire discrimination: the abort lands on the announce request bidi
 * (never the WT session stream) and carries the exact WT-mapped code, and
 * (both peers negotiating reliable-reset) keeps the RESET_STREAM_AT form.
 */
static int run_publish_namespace_done(pico_wt_harness_t *h, bool is_d18)
{
    int local_failures = 0;
#define DCHECK(cond) do { \
    if (!(cond)) { \
        fprintf(stderr, "FAIL[%s]: %s:%d: %s\n", scenario, __FILE__, \
                __LINE__, #cond); \
        local_failures++; \
    } } while (0)

    /* Client announces a namespace; draft-18 opens a fresh request bidi. */
    size_t bidis_before = h->client_conn->opened_bidi_count;

    moq_publish_namespace_cfg_t pcfg;
    moq_publish_namespace_cfg_init(&pcfg);
    moq_bytes_t parts[] = {{(const uint8_t *)"live", 4}};
    pcfg.track_namespace = (moq_namespace_t){parts, 1};
    moq_announcement_t ann;
    DCHECK(moq_session_publish_namespace(h->client_session, &pcfg, h->now,
                                         &ann) == MOQ_OK);

    pico_wt_harness_pump(h, 200);

    /* Server accepts the published namespace. */
    moq_announcement_t sh = {0};
    bool got_pub = false;
    moq_event_t ev;
    while (moq_session_poll_events(h->server_session, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED) {
            got_pub = true;
            sh = ev.u.namespace_published.ann;
        }
        moq_event_cleanup(&ev);
    }
    DCHECK(got_pub);
    if (!got_pub) { local_failures++; goto done; }

    moq_accept_namespace_cfg_t acfg;
    moq_accept_namespace_cfg_init(&acfg);
    DCHECK(moq_session_accept_namespace(h->server_session, sh, &acfg,
                                        h->now) == MOQ_OK);
    pico_wt_harness_pump(h, 200);

    bool accepted = false;
    while (moq_session_poll_events(h->client_session, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_NAMESPACE_ACCEPTED) accepted = true;
        moq_event_cleanup(&ev);
    }
    DCHECK(accepted);

    /* The announce request bidi (draft-18): the bidi the announce opened,
     * distinct from both the WT session stream and the MoQ control stream. */
    uint64_t ann_bidi = h->client_conn->last_opened_bidi_id;
    if (is_d18) {
        DCHECK(h->client_conn->opened_bidi_count == bidis_before + 1);
        DCHECK(ann_bidi != h->client_conn->control_stream_id);
        DCHECK(ann_bidi != h->client_conn->moq_control_stream_id);
    }

    /* Withdraw: draft-18 aborts the request bidi; the receiver sees DONE. */
    term_install(h);
    DCHECK(moq_session_publish_namespace_done(h->client_session, ann,
                                              h->now) == MOQ_OK);
    pico_wt_harness_pump(h, 200);
    term_remove();

    bool got_done = false;
    while (moq_session_poll_events(h->server_session, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_NAMESPACE_DONE) got_done = true;
        moq_event_cleanup(&ev);
    }
    DCHECK(got_done);

    /* The connection must survive the withdrawal on both ends. */
    DCHECK(moq_session_state(h->client_session) == MOQ_SESS_ESTABLISHED);
    DCHECK(moq_session_state(h->server_session) == MOQ_SESS_ESTABLISHED);
    DCHECK(!moq_pico_wt_conn_is_fatal(h->client_conn));
    DCHECK(!moq_pico_wt_conn_is_fatal(h->server_conn));

    /* On-wire discrimination (draft-18 only): what the compliant peer saw,
     * captured at the server's RESET callback while the stream was live. */
    if (is_d18) {
        uint64_t want = 0;
        DCHECK(pico_wt_moq_err_to_wt(0x1, &want));
        DCHECK(!g_term_overflow);                       /* complete record */
        term_rec_t ann_reset;
        memset(&ann_reset, 0, sizeof(ann_reset));
        int n_ann = term_reset_entries(h->server_conn, ann_bidi, &ann_reset);
        /* Exactly one RESET landed on the announce request bidi... */
        DCHECK(n_ann == 1);
        if (n_ann == 1) {
            DCHECK(ann_reset.stream_present && ann_reset.reset_received);
            /* ...carrying the WT-mapped CANCELLED code, which maps back to 0x1... */
            DCHECK(ann_reset.raw_code == want);
            DCHECK(ann_reset.has_mapped_code && ann_reset.mapped_code == 0x1);
            /* ...as RESET_STREAM_AT with the WT preamble as its reliable prefix,
             * exactly the size the client's endpoint recorded. */
            DCHECK(ann_reset.reset_offset > 0 &&
                   ann_reset.reset_offset ==
                       h->client_conn->endpoint_ctx.last_reset_reliable_size);
        }
        /* ...and never touched the WT session (CONNECT) stream. */
        DCHECK(term_reset_entries(h->server_conn, h->server_conn->control_stream_id, NULL) == 0);

        /* Genuine reliable-reset negotiation: both peers advertise
         * reset_stream_at and the attach path never synthesizes it, so the
         * capability-aware ep_reset takes the RESET_STREAM_AT branch
         * (picowt_reset_stream, reliable WT preamble) rather than the plain
         * RESET_STREAM downgrade reserved for a faked-capability peer. */
        const picoquic_tp_t *rtp = picoquic_get_transport_parameters(
            h->test_ctx->cnx_client, 0);
        DCHECK(rtp && rtp->is_reset_stream_at_enabled);
        DCHECK(!h->client_conn->endpoint_ctx.reset_stream_at_synthesized);
        /* ...and the abort actually used the RESET_STREAM_AT preamble
         * (reliable_size > 0), not the plain downgrade. */
        DCHECK(h->client_conn->endpoint_ctx.last_reset_reliable_size > 0);

        /* Now pin the OTHER branch on the same live connection: force the
         * synthesized-capability decision and withdraw a second namespace. The
         * capability-aware reset must downgrade to a plain RESET_STREAM
         * (reliable_size == 0); the peer still sees NAMESPACE_DONE and neither
         * side goes fatal. (Both peers genuinely support reliable-reset here,
         * so a plain reset is equally accepted -- this isolates the branch, and
         * the managed no-rsa test covers the real faked-peer path.) */
        h->client_conn->endpoint_ctx.reset_stream_at_synthesized = true;
        moq_publish_namespace_cfg_t pcfg2;
        moq_publish_namespace_cfg_init(&pcfg2);
        moq_bytes_t parts2[] = {{(const uint8_t *)"live2", 5}};
        pcfg2.track_namespace = (moq_namespace_t){parts2, 1};
        moq_announcement_t ann2;
        DCHECK(moq_session_publish_namespace(h->client_session, &pcfg2,
                                             h->now, &ann2) == MOQ_OK);
        pico_wt_harness_pump(h, 200);
        moq_announcement_t sh2 = {0};
        bool got_pub2 = false;
        while (moq_session_poll_events(h->server_session, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED) {
                got_pub2 = true;
                sh2 = ev.u.namespace_published.ann;
            }
            moq_event_cleanup(&ev);
        }
        DCHECK(got_pub2);
        if (got_pub2) {
            moq_accept_namespace_cfg_t acfg2;
            moq_accept_namespace_cfg_init(&acfg2);
            DCHECK(moq_session_accept_namespace(h->server_session, sh2,
                                                &acfg2, h->now) == MOQ_OK);
            pico_wt_harness_pump(h, 200);
            /* drain the client's ACCEPTED */
            while (moq_session_poll_events(h->client_session, &ev, 1) > 0)
                moq_event_cleanup(&ev);
            uint64_t ann2_bidi = h->client_conn->last_opened_bidi_id;
            DCHECK(ann2_bidi != ann_bidi && ann2_bidi != h->client_conn->control_stream_id);
            term_install(h);
            DCHECK(moq_session_publish_namespace_done(
                h->client_session, ann2, h->now) == MOQ_OK);
            pico_wt_harness_pump(h, 200);
            term_remove();
            bool got_done2 = false;
            while (moq_session_poll_events(h->server_session, &ev, 1) > 0) {
                if (ev.kind == MOQ_EVENT_NAMESPACE_DONE) got_done2 = true;
                moq_event_cleanup(&ev);
            }
            DCHECK(got_done2);
            DCHECK(h->client_conn->endpoint_ctx.last_reset_reliable_size == 0);
            /* The downgrade is a plain RESET_STREAM on the second announce
             * bidi: same WT-mapped code, no reliable prefix, exactly one
             * terminal, nothing on the WT session stream. */
            DCHECK(!g_term_overflow);
            term_rec_t ann2_reset;
            memset(&ann2_reset, 0, sizeof(ann2_reset));
            int n_ann2 = term_reset_entries(h->server_conn, ann2_bidi, &ann2_reset);
            DCHECK(n_ann2 == 1);
            if (n_ann2 == 1) {
                DCHECK(ann2_reset.stream_present && ann2_reset.reset_received);
                DCHECK(ann2_reset.raw_code == want && ann2_reset.has_mapped_code &&
                       ann2_reset.mapped_code == 0x1);
                DCHECK(ann2_reset.reset_offset == 0);
            }
            DCHECK(term_reset_entries(h->server_conn, h->server_conn->control_stream_id, NULL) == 0);
            DCHECK(!moq_pico_wt_conn_is_fatal(h->client_conn));
            DCHECK(!moq_pico_wt_conn_is_fatal(h->server_conn));
        }
        h->client_conn->endpoint_ctx.reset_stream_at_synthesized = false;
    }

    /* The WT connection is still usable: run another full exchange on a
     * fresh track (the first subscription is still open). */
    local_failures += run_subscribe_object(h, "v2");

done:
    term_remove();
#undef DCHECK
    return local_failures;
}

/*
 * A reset requested while the stream's WebTransport preamble is still queued
 * in picoquic. picoquic takes a Reliable Size only up to the bytes it has
 * sent, and WebTransport needs RESET_STREAM_AT to cover the preamble
 * (draft-ietf-webtrans-http3 Section 4.4), so the endpoint accepts the reset,
 * holds it until the preamble is out, and then sends RESET_STREAM_AT with the
 * preamble as its reliable prefix. The peer associates the reset with the
 * session and neither side fails.
 */
static int run_reset_before_preamble(pico_wt_harness_t *h)
{
    int local_failures = 0;
#define RCHECK(cond) do { \
    if (!(cond)) { \
        fprintf(stderr, "FAIL[%s]: %s:%d: %s\n", scenario, __FILE__, \
                __LINE__, #cond); \
        local_failures++; \
    } } while (0)
    moq_pico_wt_conn_t *c = h->client_conn;
    pico_wt_endpoint_ctx_t *ep = &c->endpoint_ctx;
    uint64_t sid = 0;
    RCHECK(c->endpoint_ops.open_uni(ep, &sid) == MOQ_TRANSPORT_OK);
    picoquic_stream_head_t *st = picoquic_find_stream(h->test_ctx->cnx_client, sid);
    RCHECK(st != NULL && st->sent_offset == 0 && st->send_queue != NULL);

    term_install(h);
    RCHECK(c->endpoint_ops.reset_stream(ep, sid, 0x2) == MOQ_TRANSPORT_OK);
    RCHECK(ep->deferred_count == 1);
    pico_wt_harness_pump(h, 200);
    term_remove();
    RCHECK(ep->deferred_count == 0);

    size_t preamble = 2 + picoquic_frames_varint_encode_length(ep->control_stream_id);
    RCHECK(ep->last_reset_reliable_size == preamble);
    RCHECK(!g_term_overflow);
    term_rec_t r;
    memset(&r, 0, sizeof(r));
    int n = term_reset_entries(h->server_conn, sid, &r);
    RCHECK(n == 1);
    if (n == 1) {
        RCHECK(r.stream_present && r.reset_received);
        RCHECK(r.has_mapped_code && r.mapped_code == 0x2);
        RCHECK(r.reset_offset == preamble);
    }
    RCHECK(!moq_pico_wt_conn_is_fatal(h->client_conn));
    RCHECK(!moq_pico_wt_conn_is_fatal(h->server_conn));
#undef RCHECK
    return local_failures;
}

/* -- Deferred resets: failure, teardown and bound ----------------------- */

static int g_reset_hook_rc;
static int reset_hook(picoquic_cnx_t *cnx, uint64_t sid, uint64_t err, uint64_t rs)
{
    (void)cnx; (void)sid; (void)err; (void)rs;
    return g_reset_hook_rc;
}

static int deferred_setup(pico_wt_harness_t *h, uint8_t cid_byte)
{
    pico_wt_harness_cfg_t cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.cid_byte = cid_byte;
    cfg.version = MOQ_VERSION_DRAFT_18;
    if (pico_wt_harness_setup(h, &cfg) != 0) return -1;
    return pico_wt_harness_handshake(h);
}

/* Open a client uni stream and reset it before its preamble is sent. */
static uint64_t open_and_reset(pico_wt_harness_t *h, moq_transport_result_t *rr)
{
    moq_pico_wt_conn_t *c = h->client_conn;
    uint64_t sid = 0;
    CHECK(c->endpoint_ops.open_uni(&c->endpoint_ctx, &sid) == MOQ_TRANSPORT_OK);
    *rr = c->endpoint_ops.reset_stream(&c->endpoint_ctx, sid, 0x2);
    return sid;
}

/*
 * A deferred reset whose retry picoquic fails to issue (here: the reset
 * frame cannot be queued) is a transport failure, reported through whichever
 * entry point ran the retry -- service (via = 0) or an h3zero callback
 * (via = 1) -- with the bridge's failed-reset code. Controls on the same
 * connection, before the failure: a retry while the preamble is still unsent
 * keeps the reset deferred and fails nothing.
 */
static void run_deferred_reset_failure(int via)
{
    scenario = via ? "deferred-reset-failure-callback" : "deferred-reset-failure-service";
    pico_wt_harness_t h;
    CHECK(deferred_setup(&h, (uint8_t)(0x41 + via)) == 0);
    if (h.client_conn && h.server_conn) {
        moq_pico_wt_conn_t *c = h.client_conn;
        moq_transport_result_t rr;
        (void)open_and_reset(&h, &rr);
        CHECK(rr == MOQ_TRANSPORT_OK);
        CHECK(c->endpoint_ctx.deferred_count == 1);
        /* Control: still unsent, so the retry keeps it. */
        CHECK(moq_pico_wt_service(c, h.now) == 0);
        CHECK(c->endpoint_ctx.deferred_count == 1);
        CHECK(!moq_pico_wt_conn_is_fatal(c));
        /* The provider now fails to issue it. */
        g_reset_hook_rc = PICOQUIC_ERROR_MEMORY;
        pw_test_reset_stream_at = reset_hook;
        int rc = via ? moq_pico_wt_callback(h.test_ctx->cnx_client, NULL, 0,
                                            picohttp_callback_connecting, NULL, c)
                     : moq_pico_wt_service(c, h.now);
        pw_test_reset_stream_at = NULL;
        CHECK(rc == -1);
        CHECK(moq_pico_wt_conn_is_fatal(c));
        CHECK(moq_transport_bridge_fatal_code(c->bridge) == 0x1);
        CHECK(c->endpoint_ctx.deferred_count == 0);
    }
    pw_test_reset_stream_at = NULL;
    pico_wt_harness_cleanup(&h);
}

/*
 * A deferred reset whose stream has meanwhile ended and retired is settled
 * without error: the retry finds no stream and nothing is owed.
 */
static void run_deferred_reset_retired(void)
{
    scenario = "deferred-reset-retired";
    pico_wt_harness_t h;
    CHECK(deferred_setup(&h, 0x43) == 0);
    if (h.client_conn && h.server_conn) {
        moq_pico_wt_conn_t *c = h.client_conn;
        moq_transport_result_t rr;
        uint64_t sid = open_and_reset(&h, &rr);
        CHECK(rr == MOQ_TRANSPORT_OK && c->endpoint_ctx.deferred_count == 1);
        /* Keep the record while the stream ends some other way. */
        g_reset_hook_rc = PICOQUIC_ERROR_OFFSET_TOO_BIG;
        pw_test_reset_stream_at = reset_hook;
        CHECK(picoquic_reset_stream(h.test_ctx->cnx_client, sid, 0x9) == 0);
        for (int i = 0; i < 200 && picoquic_find_stream(h.test_ctx->cnx_client, sid); i++)
            pico_wt_harness_pump(&h, 20);
        pw_test_reset_stream_at = NULL;
        CHECK(picoquic_find_stream(h.test_ctx->cnx_client, sid) == NULL);
        CHECK(c->endpoint_ctx.deferred_count == 1);
        CHECK(moq_pico_wt_service(c, h.now) == 0);
        CHECK(c->endpoint_ctx.deferred_count == 0);
        CHECK(!moq_pico_wt_conn_is_fatal(c));
    }
    pw_test_reset_stream_at = NULL;
    pico_wt_harness_cleanup(&h);
}

/*
 * Detaching the session while a reset waits for its preamble. The QUIC
 * connection survives, and the stream's send side is still terminated:
 * session teardown resets it with WT_SESSION_GONE (WebTransport-H3 Section
 * 6), which needs its send side to be recorded as unfinished.
 */
static void run_deferred_reset_detach(void)
{
    scenario = "deferred-reset-detach";
    pico_wt_harness_t h;
    CHECK(deferred_setup(&h, 0x44) == 0);
    if (h.client_conn && h.server_conn) {
        moq_transport_result_t rr;
        uint64_t sid = open_and_reset(&h, &rr);
        CHECK(rr == MOQ_TRANSPORT_OK && h.client_conn->endpoint_ctx.deferred_count == 1);
        h3zero_stream_ctx_t *sc = h3zero_find_stream(h.client_h3_ctx, sid);
        CHECK(sc != NULL && !sc->ps.stream_state.is_fin_sent);
        moq_pico_wt_conn_destroy(h.client_conn);
        h.client_conn = NULL;
        picoquic_cnx_t *cnx = h.test_ctx->cnx_client;
        CHECK(picoquic_get_cnx_state(cnx) == picoquic_state_ready);
        picoquic_stream_head_t *st = picoquic_find_stream(cnx, sid);
        CHECK(st == NULL || (st->reset_requested &&
                             st->local_error == H3ZERO_WEBTRANSPORT_SESSION_GONE));
        pico_wt_harness_pump(&h, 300);
        CHECK(picoquic_get_cnx_state(cnx) == picoquic_state_ready);
        st = picoquic_find_stream(cnx, sid);
        CHECK(st == NULL || (st->reset_requested && st->reset_sent));
    }
    pico_wt_harness_cleanup(&h);
}

/*
 * Deferred resets are bounded. With the peer's stream credit withheld no
 * preamble can be sent, so every reset waits; once PICO_WT_DEFERRED_RESETS_MAX
 * wait, the next reset is refused with WOULD_BLOCK (the bridge keeps and
 * retries it) and changes nothing. Granting credit lets the preambles out, the
 * waiting resets are issued, and the refused one then goes through.
 */
static void run_deferred_reset_bound(void)
{
    scenario = "deferred-reset-bound";
    pico_wt_harness_t h;
    CHECK(deferred_setup(&h, 0x45) == 0);
    if (h.client_conn && h.server_conn) {
        moq_pico_wt_conn_t *c = h.client_conn;
        picoquic_cnx_t *cnx = h.test_ctx->cnx_client;
        uint64_t granted = cnx->max_streams_unidir_remote;
        uint64_t next = picoquic_get_next_local_stream_id(cnx, 1);
        cnx->max_streams_unidir_remote = STREAM_RANK_FROM_ID(next) - 1;
        moq_transport_result_t rr = MOQ_TRANSPORT_OK;
        for (unsigned i = 0; i < PICO_WT_DEFERRED_RESETS_MAX; i++) {
            (void)open_and_reset(&h, &rr);
            CHECK(rr == MOQ_TRANSPORT_OK);
        }
        CHECK(c->endpoint_ctx.deferred_count == PICO_WT_DEFERRED_RESETS_MAX);
        uint64_t extra = open_and_reset(&h, &rr);
        CHECK(rr == MOQ_TRANSPORT_WOULD_BLOCK);
        CHECK(c->endpoint_ctx.deferred_count == PICO_WT_DEFERRED_RESETS_MAX);
        h3zero_stream_ctx_t *sc = h3zero_find_stream(h.client_h3_ctx, extra);
        picoquic_stream_head_t *st = picoquic_find_stream(cnx, extra);
        CHECK(sc != NULL && !sc->ps.stream_state.is_fin_sent);
        CHECK(st != NULL && !st->reset_requested);
        pico_wt_harness_pump(&h, 200);
        CHECK(c->endpoint_ctx.deferred_count == PICO_WT_DEFERRED_RESETS_MAX);
        CHECK(!moq_pico_wt_conn_is_fatal(c));
        /* Release: the peer's credit arrives. */
        uint64_t withheld = cnx->max_streams_unidir_remote;
        cnx->max_streams_unidir_remote = granted;
        picoquic_add_output_streams(cnx, withheld, granted, 0);
        picoquic_reinsert_by_wake_time(cnx->quic, cnx, h.now);
        pico_wt_harness_pump(&h, 500);
        CHECK(c->endpoint_ctx.deferred_count == 0);
        CHECK(c->endpoint_ops.reset_stream(&c->endpoint_ctx, extra, 0x2) == MOQ_TRANSPORT_OK);
        CHECK(c->endpoint_ctx.deferred_count == 0);
        CHECK(!moq_pico_wt_conn_is_fatal(c));
    }
    pico_wt_harness_cleanup(&h);
}

static void run_version(uint8_t cid_byte, moq_version_t version)
{
    scenario = (version == MOQ_VERSION_DRAFT_18) ? "d18" : "d16";

    pico_wt_harness_t h;
    pico_wt_harness_cfg_t cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.cid_byte = cid_byte;
    cfg.version = version;

    CHECK(pico_wt_harness_setup(&h, &cfg) == 0);
    if (h.client_conn && h.server_conn) {
        CHECK(pico_wt_harness_handshake(&h) == 0);
        CHECK(!moq_pico_wt_conn_is_fatal(h.client_conn));
        CHECK(!moq_pico_wt_conn_is_fatal(h.server_conn));
        CHECK(moq_session_state(h.client_session) == MOQ_SESS_ESTABLISHED);
        CHECK(moq_session_state(h.server_session) == MOQ_SESS_ESTABLISHED);
        failures += run_subscribe_object(&h, "v");
        failures += run_publish_namespace_done(
            &h, version == MOQ_VERSION_DRAFT_18);
        failures += run_reset_before_preamble(&h);
    }
    pico_wt_harness_cleanup(&h);
}

/*
 * WebTransport §4.4 error-code mapping boundaries (pure helper coverage, no
 * transport). An additive base is only correct below the first reserved
 * GREASE point; the real mapping skips one code every 0x1e, bounds the space
 * to UINT32_MAX, and reports every non-WebTransport value as carrying no
 * application code. The adapter's encoder matches picoquic's
 * H3ZERO_WEBTRANSPORT_APPLICATION_ERROR.
 */
static void run_error_code_mapping(void)
{
    scenario = "errmap";
    const uint64_t FIRST = H3ZERO_WEBTRANSPORT_APPLICATION_ERROR(0);
    const uint64_t LAST =
        FIRST + (uint64_t)UINT32_MAX + (uint64_t)UINT32_MAX / 0x1e;
    CHECK(LAST == H3ZERO_WEBTRANSPORT_APPLICATION_ERROR_LAST);
    uint64_t out = 0;

    /* Exact encode + round-trip across the first GREASE skip and the edges. */
    struct { uint64_t n; uint64_t enc; } cases[] = {
        { 0,          FIRST + 0x00 },   /* base */
        { 1,          FIRST + 0x01 },
        { 0x1d,       FIRST + 0x1d },    /* 29: last before the first skip */
        { 0x1e,       FIRST + 0x1f },    /* 30: shifts +1 over the reserved point */
        { 0x1f,       FIRST + 0x20 },    /* 31 */
        { UINT32_MAX, LAST },            /* top of the WebTransport range */
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        CHECK(pico_wt_moq_err_to_wt(cases[i].n, &out));
        CHECK(out == cases[i].enc);
        /* The provider's encoder agrees. */
        CHECK(out == H3ZERO_WEBTRANSPORT_APPLICATION_ERROR(cases[i].n));
        uint64_t back = UINT64_MAX;
        CHECK(pico_wt_wt_err_to_moq(cases[i].enc, &back));
        CHECK(back == cases[i].n);
    }

    /* Outbound rejects codes beyond UINT32_MAX (not WebTransport codes). */
    CHECK(!pico_wt_moq_err_to_wt((uint64_t)UINT32_MAX + 1, &out));
    CHECK(!pico_wt_moq_err_to_wt(UINT64_MAX, &out));

    /* The first reserved HTTP/3 GREASE point (FIRST + 0x1e) is not a valid
     * WebTransport code: no application code, and the output is untouched. */
    out = 0xabc;
    CHECK(!pico_wt_wt_err_to_moq(FIRST + 0x1e, &out));
    CHECK(out == 0xabc);

    /* Values outside [FIRST, LAST] carry no application code (§4.4): a bare
     * small value a peer sent unmapped, a plain HTTP/3 error such as
     * WT_SESSION_GONE, or anything above the range. */
    CHECK(!pico_wt_wt_err_to_moq(0x1, &out));
    CHECK(!pico_wt_wt_err_to_moq(0x170d7b68, &out));
    CHECK(!pico_wt_wt_err_to_moq(FIRST - 1, &out));
    CHECK(!pico_wt_wt_err_to_moq(LAST + 1, &out));
    CHECK(out == 0xabc);
}

int main(void)
{
    /* Pure §4.4 mapping boundaries (no transport). */
    run_error_code_mapping();

    /* Harness control: the bidi-control profile. */
    run_version(0x16, MOQ_VERSION_DRAFT_16);

    /* Symmetric uni-control establish + request-bidi routing + subgroup
     * data delivery. */
    run_version(0x18, MOQ_VERSION_DRAFT_18);

    /* Resets that wait for their WebTransport preamble. */
    run_deferred_reset_failure(0);
    run_deferred_reset_failure(1);
    run_deferred_reset_retired();
    run_deferred_reset_detach();
    run_deferred_reset_bound();

    if (failures == 0)
        printf("PASS: pico_wt_d18\n");
    return failures != 0;
}
