/*
 * Reliable resets over the REAL WebTransport transport (picoquic + h3zero)
 * in the deterministic simulator, observed from the receiving application.
 *
 * A MoQ data stream opened and reset in the same pass still has its stream
 * header in the adapter's send queue when the reset comes. Where reliable
 * reset was genuinely negotiated the adapter sends RESET_STREAM_AT whose
 * reliable size is the WebTransport stream preamble PLUS the MoQ header, so
 * the subscriber can still identify the stream. Where it was not (modelled
 * here by the synthesized-peer flag the managed client sets for a peer that
 * omits the parameter) the bridge falls back to a plain reset, which
 * abandons the queued header.
 *
 *   done_count (draft-16, draft-22): subgroup opened, written, reset, then
 *     PUBLISH_DONE counting it -- the subscriber surfaces the done at once
 *     only when the header arrived.
 *   failed_fill (draft-22): a fill the publisher's pool cannot hold fails on
 *     the wire (FETCH_HEADER then reset); the subscriber ends that fill only
 *     when the header arrived.
 *
 * Each runs over several connection ids with and without reliable reset;
 * the delivery rates are printed and the reliable runs must all deliver,
 * with the last reliable size the endpoint used exceeding the preamble.
 */

#include "pico_wt_harness.h"
#include <moq/moq.h>
#include <picoquic_utils.h>
#include <stdio.h>
#include <string.h>

static int failures = 0;
static const char *scenario = "reliable_reset";
#define CHECK(c) do { if (!(c)) { \
    fprintf(stderr, "FAIL[%s]: %s:%d: %s\n", scenario, __FILE__, __LINE__, #c); \
    failures++; } } while (0)

#define RUNS 10

static unsigned g_tracks;

static bool client_saw(pico_wt_harness_t *h, moq_event_kind_t kind)
{
    bool seen = false;
    moq_event_t ev;
    while (moq_session_poll_events(h->client_session, &ev, 1) > 0) {
        if (ev.kind == kind) seen = true;
        moq_event_cleanup(&ev);
    }
    return seen;
}

static moq_subscription_t subscribe(pico_wt_harness_t *h, const moq_fill_cfg_t *fill)
{
    static const char names[] = "abcdefgh";
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    moq_bytes_t ns[] = {{(const uint8_t *)"r", 1}};
    sc.track_namespace.parts = ns;
    sc.track_namespace.count = 1;
    sc.track_name = (moq_bytes_t){(const uint8_t *)&names[g_tracks++ % 8], 1};
    sc.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    sc.fill = fill;
    moq_subscription_t csub, ssub = MOQ_SUBSCRIPTION_INVALID;
    if (moq_session_subscribe(h->client_session, &sc, h->now, &csub) < 0) return ssub;
    pico_wt_harness_pump(h, 200);
    moq_event_t ev;
    while (moq_session_poll_events(h->server_session, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) ssub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
    }
    if (!moq_subscription_is_valid(ssub)) return ssub;
    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    ac.has_largest = true; ac.largest_group = 5; ac.largest_object = 2;
    if (moq_session_accept_subscribe(h->server_session, ssub, &ac, h->now) < 0)
        return MOQ_SUBSCRIPTION_INVALID;
    return ssub;
}

static bool open_harness(pico_wt_harness_t *h, uint8_t cid, moq_version_t v,
                         bool reliable, uint32_t fills)
{
    pico_wt_harness_cfg_t cfg = { .cid_byte = cid, .request_capacity = 16,
                                  .version = v, .server_max_fills = fills };
    if (pico_wt_harness_setup(h, &cfg) != 0 || pico_wt_harness_handshake(h) != 0)
        return false;
    if (!reliable) h->server_conn->endpoint_ctx.reset_stream_at_synthesized = true;
    return true;
}

static size_t preamble_len(pico_wt_harness_t *h)
{
    return 2 + picoquic_frames_varint_encode_length(h->server_conn->endpoint_ctx.control_stream_id);
}

static int run_done_count(uint8_t cid, moq_version_t v, bool reliable)
{
    pico_wt_harness_t h;
    if (!open_harness(&h, cid, v, reliable, 0)) {
        CHECK(false); pico_wt_harness_cleanup(&h); return 0;
    }
    moq_subscription_t ss = subscribe(&h, NULL);
    CHECK(moq_subscription_is_valid(ss));
    pico_wt_harness_pump(&h, 200);
    (void)client_saw(&h, MOQ_EVENT_SUBSCRIBE_OK);
    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = 6;
    sgc.publisher_priority = 128;
    moq_subgroup_handle_t sg;
    CHECK(moq_session_open_subgroup(h.server_session, ss, &sgc, h.now, &sg) == MOQ_OK);
    moq_rcbuf_t *b = NULL;
    moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)"object", 6, &b);
    CHECK(moq_session_write_object(h.server_session, sg, 0, b, h.now) == MOQ_OK);
    moq_rcbuf_decref(b);
    CHECK(moq_session_reset_subgroup(h.server_session, sg, 0x1, h.now) == MOQ_OK);
    moq_done_subscribe_cfg_t dc;
    moq_done_subscribe_cfg_init(&dc);
    dc.status_code = 0x2;
    dc.stream_count = 1;
    CHECK(moq_session_done_subscribe(h.server_session, ss, &dc, h.now) == MOQ_OK);
    pico_wt_harness_pump(&h, 300);
    int got = client_saw(&h, MOQ_EVENT_SUBSCRIBE_DONE) ? 1 : 0;
    if (reliable) {
        CHECK(h.server_conn->endpoint_ctx.reliable_resets == 1);
        CHECK(h.server_conn->endpoint_ctx.last_reset_reliable_size > preamble_len(&h));
    } else {
        CHECK(h.server_conn->endpoint_ctx.reliable_resets == 0);
    }
    CHECK(!moq_pico_wt_conn_is_fatal(h.client_conn));
    CHECK(!moq_pico_wt_conn_is_fatal(h.server_conn));
    pico_wt_harness_cleanup(&h);
    return got;
}

static int run_failed_fill(uint8_t cid, bool reliable)
{
    pico_wt_harness_t h;
    if (!open_harness(&h, cid, MOQ_VERSION_DRAFT_22, reliable, 1)) {
        CHECK(false); pico_wt_harness_cleanup(&h); return 0;
    }
    moq_location_filter_cfg_t loc;
    moq_location_filter_cfg_init(&loc);
    loc.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;
    loc.start_group = 3;
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.location = &loc;
    CHECK(moq_subscription_is_valid(subscribe(&h, &fill)));   /* holds the slot */
    pico_wt_harness_pump(&h, 200);
    (void)client_saw(&h, MOQ_EVENT_FILL_TERMINAL);
    CHECK(moq_subscription_is_valid(subscribe(&h, &fill)));   /* failed on the wire */
    pico_wt_harness_pump(&h, 300);
    int got = client_saw(&h, MOQ_EVENT_FILL_TERMINAL) ? 1 : 0;
    CHECK(!moq_pico_wt_conn_is_fatal(h.client_conn));
    CHECK(!moq_pico_wt_conn_is_fatal(h.server_conn));
    pico_wt_harness_cleanup(&h);
    return got;
}

#if defined(PICOQUIC_STREAM_TERMINAL_CONTRACT_VERSION) && PICOQUIC_STREAM_TERMINAL_CONTRACT_VERSION >= 1
#define WT_HAVE_D22 1
#else
#define WT_HAVE_D22 0     /* draft-22 refuses to attach to this provider */
#endif

int main(void)
{
    if (!WT_HAVE_D22) {
        /* Without the provider's stream terminal contract draft-22 refuses
         * at the client adapter's constructor, and only there. */
        scenario = "d22_refused";
        pico_wt_harness_t h;
        pico_wt_harness_cfg_t cfg = { .cid_byte = 0x3f, .request_capacity = 16,
                                      .version = MOQ_VERSION_DRAFT_22 };
        const char *why = pico_wt_harness_check_attach_refusal(
            &h, pico_wt_harness_setup(&h, &cfg));
        if (why) fprintf(stderr, "d22_refused: %s\n", why);
        CHECK(why == NULL);
        pico_wt_harness_cleanup(&h);
    }
    {
        /* A setup failure before the constructor is not that refusal. */
        scenario = "unrelated_setup_failure";
        pico_wt_harness_t h;
        pico_wt_harness_cfg_t cfg = { .cid_byte = 0x3e, .request_capacity = 16,
                                      .version = (moq_version_t)17 };
        int rc = pico_wt_harness_setup(&h, &cfg);   /* no such profile */
        CHECK(rc == -1);
        CHECK(h.phase == PICO_WT_HARNESS_SESSIONS);
        CHECK(!pico_wt_harness_refused_at_attach(&h, rc));
        CHECK(pico_wt_harness_check_attach_refusal(&h, rc) != NULL);
        pico_wt_harness_cleanup(&h);
    }
    static const struct { const char *name; moq_version_t v; } dc[] = {
        { "done_count_d16", MOQ_VERSION_DRAFT_16 },
        { "done_count_d22", MOQ_VERSION_DRAFT_22 },
    };
    for (size_t i = 0; i < sizeof(dc) / sizeof(dc[0]); i++) {
        if (dc[i].v == MOQ_VERSION_DRAFT_22 && !WT_HAVE_D22) continue;
        for (int reliable = 1; reliable >= 0; reliable--) {
            scenario = dc[i].name;
            int got = 0;
            for (int r = 0; r < RUNS; r++)
                got += run_done_count((uint8_t)(0x40 + r), dc[i].v, reliable != 0);
            printf("%s %s: header delivered in %d/%d runs\n", dc[i].name,
                   reliable ? "RESET_STREAM_AT" : "plain reset", got, RUNS);
            if (reliable) CHECK(got == RUNS);
        }
    }
    for (int reliable = 1; reliable >= 0 && WT_HAVE_D22; reliable--) {
        scenario = "failed_fill_d22";
        int got = 0;
        for (int r = 0; r < RUNS; r++)
            got += run_failed_fill((uint8_t)(0x60 + r), reliable != 0);
        printf("failed_fill_d22 %s: header delivered in %d/%d runs\n",
               reliable ? "RESET_STREAM_AT" : "plain reset", got, RUNS);
        if (reliable) CHECK(got == RUNS);
    }
    if (failures == 0)
        printf("PASS: pico_wt_reliable_reset\n");
    return failures != 0;
}
