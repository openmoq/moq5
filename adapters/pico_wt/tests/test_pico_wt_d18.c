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
    DCHECK(moq_session_publish_namespace_done(h->client_session, ann,
                                              h->now) == MOQ_OK);
    pico_wt_harness_pump(h, 200);

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

    /* On-wire discrimination (draft-18 only): what the compliant peer sees. */
    if (is_d18) {
        uint64_t want = 0;
        DCHECK(pico_wt_moq_err_to_wt(0x1, &want));
        uint64_t got_ann = picoquic_get_remote_stream_error(
            h->test_ctx->cnx_server, ann_bidi);
        uint64_t got_wtsession = picoquic_get_remote_stream_error(
            h->test_ctx->cnx_server, h->server_conn->control_stream_id);
        /* The abort lands on the request bidi with the WT-mapped code... */
        DCHECK(got_ann == want);
        /* ...and never touches the WT session (CONNECT) stream. */
        DCHECK(got_wtsession == 0);

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
            DCHECK(moq_session_publish_namespace_done(
                h->client_session, ann2, h->now) == MOQ_OK);
            pico_wt_harness_pump(h, 200);
            bool got_done2 = false;
            while (moq_session_poll_events(h->server_session, &ev, 1) > 0) {
                if (ev.kind == MOQ_EVENT_NAMESPACE_DONE) got_done2 = true;
                moq_event_cleanup(&ev);
            }
            DCHECK(got_done2);
            DCHECK(h->client_conn->endpoint_ctx.last_reset_reliable_size == 0);
            DCHECK(!moq_pico_wt_conn_is_fatal(h->client_conn));
            DCHECK(!moq_pico_wt_conn_is_fatal(h->server_conn));
        }
        h->client_conn->endpoint_ctx.reset_stream_at_synthesized = false;
    }

    /* The WT connection is still usable: run another full exchange on a
     * fresh track (the first subscription is still open). */
    local_failures += run_subscribe_object(h, "v2");

done:
#undef DCHECK
    return local_failures;
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
    }
    pico_wt_harness_cleanup(&h);
}

/*
 * WebTransport §4.4 error-code mapping boundaries (pure helper coverage, no
 * transport). The additive base is only correct below the first reserved
 * GREASE point; the real mapping skips one code every 0x1e, bounds the space
 * to UINT32_MAX, and preserves non-WebTransport values unchanged.
 */
static void run_error_code_mapping(void)
{
    scenario = "errmap";
    const uint64_t FIRST = H3ZERO_WEBTRANSPORT_APPLICATION_ERROR(0);
    const uint64_t LAST =
        FIRST + (uint64_t)UINT32_MAX + (uint64_t)UINT32_MAX / 0x1e;
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
        CHECK(pico_wt_wt_err_to_moq(cases[i].enc) == cases[i].n);
    }

    /* Outbound rejects codes beyond UINT32_MAX (not WebTransport codes). */
    CHECK(!pico_wt_moq_err_to_wt((uint64_t)UINT32_MAX + 1, &out));
    CHECK(!pico_wt_moq_err_to_wt(UINT64_MAX, &out));

    /* The first reserved HTTP/3 GREASE point (FIRST + 0x1e) is not a valid
     * WebTransport code and is preserved unchanged, never decoded. */
    CHECK(pico_wt_wt_err_to_moq(FIRST + 0x1e) == FIRST + 0x1e);

    /* Values outside [FIRST, LAST] pass through unchanged: a legacy peer's
     * raw MoQ code, a plain HTTP/3 error, or anything above the range. */
    CHECK(pico_wt_wt_err_to_moq(0x1) == 0x1);
    CHECK(pico_wt_wt_err_to_moq(FIRST - 1) == FIRST - 1);
    CHECK(pico_wt_wt_err_to_moq(LAST + 1) == LAST + 1);
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

    if (failures == 0)
        printf("PASS: pico_wt_d18\n");
    return failures != 0;
}
