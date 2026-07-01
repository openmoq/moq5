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

static int run_subscribe_object(pico_wt_harness_t *h)
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
    sc.track_name = (moq_bytes_t){(const uint8_t *)"v", 1};
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
        failures += run_subscribe_object(&h);
    }
    pico_wt_harness_cleanup(&h);
}

int main(void)
{
    /* Harness control: the bidi-control profile. */
    run_version(0x16, MOQ_VERSION_DRAFT_16);

    /* Symmetric uni-control establish + request-bidi routing + subgroup
     * data delivery. */
    run_version(0x18, MOQ_VERSION_DRAFT_18);

    if (failures == 0)
        printf("PASS: pico_wt_d18\n");
    return failures != 0;
}
