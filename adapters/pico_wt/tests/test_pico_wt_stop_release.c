/*
 * The end of a WebTransport data stream this adapter stopped, over the REAL
 * picoquic + h3zero transport in the deterministic simulator.
 *
 * Once STOP_SENDING is requested on a stream, picoquic delivers no more of
 * its bytes and no FIN; a peer RESET is still signalled. A publisher that had
 * already sent its last byte and FIN ends the stream with that FIN (RFC 9000
 * section 3.5), and picoquic then retires it -- reporting the retirement
 * (picoquic_callback_stream_released, which h3zero forwards as
 * picohttp_callback_free and uses to delete its own stream context) only for
 * a stream that still carries an application context. picoquic_stop_sending
 * and an accepted reset clear that context, and so does marking the stream
 * inactive; the context is h3zero's own stream record.
 *
 * Rows:
 *   stop_then_fin      the publisher's FIN arrives after the subscriber's
 *                      STOP: picoquic retires the stream; h3zero's record of
 *                      it must go and the adapter must report the stream's
 *                      whole terminal to the bridge, once;
 *   fin_before_stop    control: the FIN is seen first and the stream retires
 *                      normally;
 *   bidi_cancel        adapter control, draft-22: the subscriber cancels its
 *                      request (the bridge resets, then stops, the request
 *                      stream) and the publisher's FIN for its half is given
 *                      directly at the transport, not by a libmoq publisher:
 *                      h3zero's record goes, and the bridge's aborting entry
 *                      is retired by the reported terminal.
 */
#include "pico_wt_harness.h"
#include "pico_wt_test_seam.h"
#include "../pico_wt_adapter.h"

#include "picoquic_internal.h"

#include <moq/pico_wt.h>
#include <moq/rcbuf.h>
#include <moq/session.h>
#include <moq/transport_bridge.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int g_failures;
static const char *g_row = "";
#define CHECK(cond)                                                          \
    do {                                                                     \
        if (!(cond)) {                                                       \
            fprintf(stderr, "FAIL[%s] %s:%d: %s\n", g_row, __FILE__,         \
                    __LINE__, #cond);                                        \
            g_failures++;                                                    \
        }                                                                    \
    } while (0)

typedef struct {
    pico_wt_harness_t h;
    moq_subscription_t client_sub, server_sub;
    bool ok;
} rig_t;

static void drain_client(rig_t *r, unsigned *objects)
{
    moq_event_t ev;
    while (moq_session_poll_events(r->h.client_session, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED && objects) (*objects)++;
        moq_event_cleanup(&ev);
    }
}

static void pump(rig_t *r, uint64_t ms)
{
    moq_pico_wt_service(r->h.client_conn, r->h.now);
    moq_pico_wt_service(r->h.server_conn, r->h.now);
    (void)pico_wt_harness_pump(&r->h, ms);
}

static bool rig_up(rig_t *r, uint8_t cid, moq_version_t v)
{
    memset(r, 0, sizeof(*r));
    pico_wt_harness_cfg_t cfg = { .cid_byte = cid, .request_capacity = 10,
                                  .version = v };
    if (pico_wt_harness_setup(&r->h, &cfg) != 0) return false;
    if (pico_wt_harness_handshake(&r->h) != 0) return false;
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    moq_bytes_t ns[] = { {(const uint8_t *)"pico", 4},
                         {(const uint8_t *)"wt", 2} };
    sc.track_namespace.parts = ns;
    sc.track_namespace.count = 2;
    sc.track_name = (moq_bytes_t){(const uint8_t *)"video", 5};
    sc.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    if (moq_session_subscribe(r->h.client_session, &sc, r->h.now,
                              &r->client_sub) < 0)
        return false;
    pump(r, 400);
    r->server_sub = MOQ_SUBSCRIPTION_INVALID;
    moq_event_t ev;
    while (moq_session_poll_events(r->h.server_session, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
            r->server_sub = ev.u.subscribe_request.sub;
            moq_accept_subscribe_cfg_t acfg;
            moq_accept_subscribe_cfg_init(&acfg);
            if (moq_session_accept_subscribe(r->h.server_session,
                                             r->server_sub, &acfg,
                                             r->h.now) < 0)
                r->server_sub = MOQ_SUBSCRIPTION_INVALID;
        }
        moq_event_cleanup(&ev);
    }
    if (!moq_subscription_is_valid(r->server_sub)) return false;
    pump(r, 400);
    drain_client(r, NULL);
    r->ok = true;
    return true;
}

static moq_result_t write_obj(rig_t *r, moq_subgroup_handle_t h, uint64_t id)
{
    uint8_t b[64];
    memset(b, (int)id, sizeof(b));
    moq_rcbuf_t *p = NULL;
    if (moq_rcbuf_create(moq_alloc_default(), b, sizeof(b), &p) < 0)
        return MOQ_ERR_NOMEM;
    moq_result_t rc = moq_session_write_object(r->h.server_session, h, id, p,
                                               r->h.now);
    moq_rcbuf_decref(p);
    return rc;
}

/* The one server-initiated unidirectional data stream the client has seen. */
static uint64_t data_stream(rig_t *r)
{
    uint64_t found = UINT64_MAX;
    for (picoquic_stream_head_t *s = picoquic_first_stream(r->h.test_ctx->cnx_client);
         s != NULL; s = picoquic_next_stream(s)) {
        if ((s->stream_id & 3) != 3) continue;
        if (h3zero_find_stream(r->h.client_h3_ctx, s->stream_id) == NULL)
            continue;
        if (s->stream_id == r->h.client_conn->moq_control_stream_id) continue;
        if (found == UINT64_MAX || s->stream_id > found) found = s->stream_id;
    }
    return found;
}

static size_t bridge_streams(rig_t *r)
{
    return moq_transport_bridge_stream_count(r->h.client_conn->bridge);
}

static void stop_then_fin(uint8_t cid, bool fin_first)
{
    g_row = fin_first ? "fin_before_stop d16" : "stop_then_fin d16";
    rig_t r;
    if (!rig_up(&r, cid, MOQ_VERSION_DRAFT_16)) {
        CHECK(false); pico_wt_harness_cleanup(&r.h); return;
    }
    size_t base = bridge_streams(&r);
    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = 0;
    moq_subgroup_handle_t sg;
    CHECK(moq_session_open_subgroup(r.h.server_session, r.server_sub, &sgc,
                                    r.h.now, &sg) == MOQ_OK);
    CHECK(write_obj(&r, sg, 0) == MOQ_OK);
    pump(&r, 200);
    unsigned objects = 0;
    drain_client(&r, &objects);
    CHECK(objects == 1);
    uint64_t sid = data_stream(&r);
    CHECK(sid != UINT64_MAX);
    size_t terminals = r.h.client_conn->stream_terminal_count;
    CHECK(write_obj(&r, sg, 1) == MOQ_OK);
    CHECK(moq_session_close_subgroup(r.h.server_session, sg, r.h.now) == MOQ_OK);
    moq_pico_wt_service(r.h.server_conn, r.h.now);
    if (fin_first) {
        pump(&r, 300);
        drain_client(&r, &objects);
        CHECK(objects == 2);
    }
    CHECK(moq_session_unsubscribe(r.h.client_session, r.client_sub,
                                  r.h.now) == MOQ_OK);
    moq_pico_wt_service(r.h.client_conn, r.h.now);
    if (!fin_first) {
        picoquic_stream_head_t *st =
            picoquic_find_stream(r.h.test_ctx->cnx_client, sid);
        CHECK(st != NULL && st->stop_sending_requested && !st->fin_received);
    }
    pump(&r, 600);
    /* picoquic retired the stream. */
    CHECK(picoquic_find_stream(r.h.test_ctx->cnx_client, sid) == NULL);
    /* h3zero's record of it went with it, and the adapter heard of it. */
    CHECK(h3zero_find_stream(r.h.client_h3_ctx, sid) == NULL);
    CHECK(r.h.client_conn->stream_terminal_count == terminals + 1);
    CHECK(!moq_pico_wt_conn_is_fatal(r.h.client_conn));
    printf("  %s: bridge streams %zu before the subgroup, %zu after; "
           "terminals %zu -> %zu\n", g_row, base, bridge_streams(&r), terminals,
           r.h.client_conn->stream_terminal_count);
    pico_wt_harness_cleanup(&r.h);
}

/* The subscriber's cancellation of a request against a real libmoq publisher:
 * nothing is injected; the server session and its adapter answer over
 * WebTransport. With `crossing_done` the publisher ends the subscription in
 * the same instant, so its PUBLISH_DONE and FIN cross the client's reset and
 * STOP. Either way the request stream retires exactly once on both ends,
 * each end reports exactly one terminal for it, and neither connection turns
 * fatal. */
static void bidi_cancel_real_peer(moq_version_t v, bool crossing_done,
                                  uint8_t cid)
{
    g_row = v == MOQ_VERSION_DRAFT_22
        ? (crossing_done ? "bidi_cancel real peer, crossing done d22"
                         : "bidi_cancel real peer d22")
        : (crossing_done ? "bidi_cancel real peer, crossing done d18"
                         : "bidi_cancel real peer d18");
    rig_t r;
    if (!rig_up(&r, cid, v)) {
        CHECK(false); pico_wt_harness_cleanup(&r.h); return;
    }
    uint64_t bid = r.h.client_conn->last_opened_bidi_id;
    CHECK(r.h.client_conn->opened_bidi_count >= 1);
    CHECK(h3zero_find_stream(r.h.client_h3_ctx, bid) != NULL);
    size_t base = bridge_streams(&r);
    size_t sbase = moq_transport_bridge_stream_count(r.h.server_conn->bridge);
    size_t terminals = r.h.client_conn->stream_terminal_count;
    size_t sterminals = r.h.server_conn->stream_terminal_count;
    if (crossing_done) {
        moq_done_subscribe_cfg_t dc;
        moq_done_subscribe_cfg_init(&dc);
        CHECK(moq_session_done_subscribe(r.h.server_session, r.server_sub,
                                         &dc, r.h.now) == MOQ_OK);
        moq_pico_wt_service(r.h.server_conn, r.h.now);
    }
    CHECK(moq_session_unsubscribe(r.h.client_session, r.client_sub,
                                  r.h.now) == MOQ_OK);
    moq_pico_wt_service(r.h.client_conn, r.h.now);
    picoquic_stream_head_t *st = picoquic_find_stream(r.h.test_ctx->cnx_client, bid);
    CHECK(st != NULL && st->reset_requested && st->stop_sending_requested);
    pump(&r, 1000);
    CHECK(picoquic_find_stream(r.h.test_ctx->cnx_client, bid) == NULL);
    CHECK(h3zero_find_stream(r.h.client_h3_ctx, bid) == NULL);
    CHECK(bridge_streams(&r) + 1 == base);
    CHECK(moq_transport_bridge_stream_count(r.h.server_conn->bridge) + 1 == sbase);
    /* Each end reports the request stream's terminal exactly once. */
    CHECK(r.h.client_conn->stream_terminal_count == terminals + 1);
    CHECK(r.h.server_conn->stream_terminal_count == sterminals + 1);
    CHECK(!moq_pico_wt_conn_is_fatal(r.h.client_conn));
    CHECK(!moq_pico_wt_conn_is_fatal(r.h.server_conn));
    printf("  %s: client terminals %zu -> %zu, server terminals %zu -> %zu\n",
           g_row, terminals, r.h.client_conn->stream_terminal_count,
           sterminals, r.h.server_conn->stream_terminal_count);
    pico_wt_harness_cleanup(&r.h);
}

#if defined(PICOQUIC_STREAM_TERMINAL_CONTRACT_VERSION) && PICOQUIC_STREAM_TERMINAL_CONTRACT_VERSION >= 1
/* Adapter control: the publisher's FIN for its half is injected at the
 * server's transport, not produced by the server session, so the client
 * adapter's handling of a FIN picoquic withholds after the STOP is pinned on
 * its own. draft-22 attaches only to a provider with the stream terminal
 * contract. */
static void bidi_cancel(uint8_t cid)
{
    g_row = "bidi_cancel control, injected peer FIN d22";
    rig_t r;
    if (!rig_up(&r, cid, MOQ_VERSION_DRAFT_22)) {
        CHECK(false); pico_wt_harness_cleanup(&r.h); return;
    }
    uint64_t bid = r.h.client_conn->last_opened_bidi_id;
    CHECK(r.h.client_conn->opened_bidi_count >= 1);
    CHECK(h3zero_find_stream(r.h.client_h3_ctx, bid) != NULL);
    size_t base = bridge_streams(&r);
    size_t terminals = r.h.client_conn->stream_terminal_count;
    CHECK(moq_session_unsubscribe(r.h.client_session, r.client_sub,
                                  r.h.now) == MOQ_OK);
    moq_pico_wt_service(r.h.client_conn, r.h.now);
    picoquic_stream_head_t *st = picoquic_find_stream(r.h.test_ctx->cnx_client, bid);
    CHECK(st != NULL && st->reset_requested && st->stop_sending_requested);
    /* The publisher's FIN for its half is already on its way. */
    picoquic_cnx_t *srv = r.h.test_ctx->cnx_server;
    CHECK(srv != NULL);
    if (srv) CHECK(picoquic_add_to_stream(srv, bid, NULL, 0, 1) == 0);
    pump(&r, 1000);
    CHECK(picoquic_find_stream(r.h.test_ctx->cnx_client, bid) == NULL);
    CHECK(h3zero_find_stream(r.h.client_h3_ctx, bid) == NULL);
    CHECK(r.h.client_conn->stream_terminal_count == terminals + 1);
    CHECK(bridge_streams(&r) + 1 == base);
    CHECK(!moq_pico_wt_conn_is_fatal(r.h.client_conn));
    pico_wt_harness_cleanup(&r.h);
}
#endif

int main(void)
{
    stop_then_fin(0x51, false);
    stop_then_fin(0x52, true);
    bidi_cancel_real_peer(MOQ_VERSION_DRAFT_18, false, 0x55);
    bidi_cancel_real_peer(MOQ_VERSION_DRAFT_18, true, 0x56);
#if defined(PICOQUIC_STREAM_TERMINAL_CONTRACT_VERSION) && PICOQUIC_STREAM_TERMINAL_CONTRACT_VERSION >= 1
    bidi_cancel(0x53);
    bidi_cancel_real_peer(MOQ_VERSION_DRAFT_22, false, 0x57);
    bidi_cancel_real_peer(MOQ_VERSION_DRAFT_22, true, 0x58);
#else
    {
        /* Without the provider's stream terminal contract draft-22 refuses
         * at the client adapter's constructor, and only there. */
        g_row = "d22_refused";
        pico_wt_harness_t h;
        pico_wt_harness_cfg_t cfg = { .cid_byte = 0x53, .request_capacity = 10,
                                      .version = MOQ_VERSION_DRAFT_22 };
        const char *why = pico_wt_harness_check_attach_refusal(
            &h, pico_wt_harness_setup(&h, &cfg));
        if (why) fprintf(stderr, "d22_refused: %s\n", why);
        CHECK(why == NULL);
        pico_wt_harness_cleanup(&h);
    }
#endif
    {
        /* A setup failure before the constructor is not that refusal. */
        g_row = "unrelated_setup_failure";
        pico_wt_harness_t h;
        pico_wt_harness_cfg_t cfg = { .cid_byte = 0x54, .request_capacity = 10,
                                      .version = (moq_version_t)17 };
        int rc = pico_wt_harness_setup(&h, &cfg);   /* no such profile */
        CHECK(rc == -1);
        CHECK(h.phase == PICO_WT_HARNESS_SESSIONS);
        CHECK(!pico_wt_harness_refused_at_attach(&h, rc));
        CHECK(pico_wt_harness_check_attach_refusal(&h, rc) != NULL);
        pico_wt_harness_cleanup(&h);
    }
    if (g_failures) {
        fprintf(stderr, "pico_wt_stop_release: %d failure(s)\n", g_failures);
        return 1;
    }
    printf("pico_wt_stop_release: all rows passed\n");
    return 0;
}
