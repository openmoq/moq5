/*
 * Draft-18 request-bidi teardown through the deterministic SimPair harness.
 * Stream-correlated profiles have no UNSUBSCRIBE / FETCH_CANCEL messages: a
 * subscriber or fetcher cancels by tearing down its request bidi at the
 * transport layer (STOP_SENDING, the required signal, plus a RESET of its own
 * send half), never by a control message. These tests drive a full D18 pair
 * and assert the teardown is observed by the peer, no control bytes are emitted
 * for the cancellation, and in-flight object/fetch data stays safe.
 */
#include <moq/moq.h>
#include <moq/sim.h>
#include "test_support.h"
#include "../../core/src/session/session_internal.h"

/* Counts SimPair input deliveries by kind once armed, so a test can assert what
 * crossed the wire during a specific phase (e.g. teardown). */
typedef struct {
    bool   armed;
    size_t control_bytes;
    size_t bidi_stop;
    size_t bidi_reset;
    size_t data_stop;
} wire_counts_t;

static void count_trace(void *ctx, const moq_sim_trace_record_t *r)
{
    wire_counts_t *c = (wire_counts_t *)ctx;
    if (!c->armed || r->kind != MOQ_SIM_TRACE_INPUT) return;
    switch (r->input_kind) {
    case MOQ_SIM_INPUT_CONTROL_BYTES: c->control_bytes++; break;
    case MOQ_SIM_INPUT_BIDI_STOP:     c->bidi_stop++;     break;
    case MOQ_SIM_INPUT_BIDI_RESET:    c->bidi_reset++;    break;
    case MOQ_SIM_INPUT_DATA_STOP:     c->data_stop++;     break;
    default: break;
    }
}

static moq_simpair_t *make_pair(wire_counts_t *ctr)
{
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = moq_alloc_default();
    cfg.version = MOQ_VERSION_DRAFT_18;
    cfg.trace_fn = count_trace;
    cfg.trace_ctx = ctr;
    moq_simpair_t *sp = NULL;
    if (moq_simpair_create(&cfg, &sp) < 0) return NULL;
    return sp;
}

/* Establish + SUBSCRIBE "live"/track + accept; returns the server-side sub. */
static bool setup_subscription(moq_simpair_t *sp, const char *track,
                               moq_subscription_t *out_server_sub,
                               moq_subscription_t *out_client_sub)
{
    moq_session_t *client = moq_simpair_client(sp);
    moq_session_t *server = moq_simpair_server(sp);

    if (moq_simpair_start(sp) < 0) return false;
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    if (client->state != MOQ_SESS_ESTABLISHED ||
        server->state != MOQ_SESS_ESTABLISHED)
        return false;

    moq_subscribe_cfg_t sub;
    moq_subscribe_cfg_init(&sub);
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_namespace_t ns = { parts, 1 };
    sub.track_namespace = ns;
    sub.track_name = (moq_bytes_t){ (const uint8_t *)track, strlen(track) };
    moq_subscription_t ch;
    if (moq_session_subscribe(client, &sub, 1, &ch) != MOQ_OK) return false;
    moq_simpair_run_until_quiescent(sp, 16, NULL);

    moq_subscription_t sh = {0};
    bool got = false;
    moq_event_t ev;
    while (moq_session_poll_events(server, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
            sh = ev.u.subscribe_request.sub;
            got = true;
        }
        moq_event_cleanup(&ev);
    }
    if (!got) return false;

    moq_accept_subscribe_cfg_t acc;
    moq_accept_subscribe_cfg_init(&acc);
    if (moq_session_accept_subscribe(server, sh, &acc,
                                     moq_simpair_now_us(sp)) != MOQ_OK)
        return false;
    moq_simpair_run_until_quiescent(sp, 16, NULL);

    bool ok = false;
    while (moq_session_poll_events(client, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_OK) ok = true;
        moq_event_cleanup(&ev);
    }
    if (!ok) return false;

    *out_server_sub = sh;
    *out_client_sub = ch;
    return true;
}

/* Issue a FETCH and return both handles; optionally accept it (opening the
 * response data uni). */
static bool setup_fetch(moq_simpair_t *sp, moq_fetch_t *out_client,
                        moq_fetch_t *out_server, bool accept)
{
    moq_session_t *client = moq_simpair_client(sp);
    moq_session_t *server = moq_simpair_server(sp);

    if (moq_simpair_start(sp) < 0) return false;
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    if (client->state != MOQ_SESS_ESTABLISHED) return false;

    moq_fetch_cfg_t fc;
    moq_fetch_cfg_init(&fc);
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    fc.track_namespace = (moq_namespace_t){ parts, 1 };
    fc.track_name = MOQ_BYTES_LITERAL("video");
    fc.end_group = 10;
    if (moq_session_fetch(client, &fc, moq_simpair_now_us(sp), out_client)
        != MOQ_OK)
        return false;
    moq_simpair_run_until_quiescent(sp, 16, NULL);

    moq_fetch_t sfh = {0};
    bool got = false;
    moq_event_t ev;
    while (moq_session_poll_events(server, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_FETCH_REQUEST) {
            sfh = ev.u.fetch_request.fetch; got = true;
        }
        moq_event_cleanup(&ev);
    }
    if (!got) return false;
    *out_server = sfh;

    if (accept) {
        moq_accept_fetch_cfg_t ac;
        moq_accept_fetch_cfg_init(&ac);
        ac.end_group = 10;
        if (moq_session_accept_fetch(server, sfh, &ac,
                                     moq_simpair_now_us(sp)) != MOQ_OK)
            return false;
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        /* Drain the client's FETCH_OK so later polls see only teardown. */
        while (moq_session_poll_events(client, &ev, 1) > 0)
            moq_event_cleanup(&ev);
    }
    return true;
}

int main(void)
{
    int failures = 0;
    const moq_alloc_t *alloc = moq_alloc_default();

    /* == A. D18 unsubscribe: transport teardown, no control message ==== *
     *  The subscriber unsubscribes; the publisher observes UNSUBSCRIBED and
     *  drops its subscription, no control bytes carry the cancellation, and the
     *  request bidi is STOP_SENDING'd. A late object data stream arriving after
     *  the subscription is gone is stopped, not fatal. */
    {
        wire_counts_t ctr = {0};
        moq_simpair_t *sp = make_pair(&ctr);
        MOQ_TEST_CHECK(sp != NULL);
        moq_session_t *client = moq_simpair_client(sp);
        moq_session_t *server = moq_simpair_server(sp);
        moq_subscription_t ssub, csub;
        MOQ_TEST_CHECK(setup_subscription(sp, "video", &ssub, &csub));
        (void)ssub;

        /* Subscriber tears down its request bidi. */
        ctr.armed = true;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_unsubscribe(client, csub, moq_simpair_now_us(sp)),
            (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 64, NULL);

        /* No control bytes carried the cancellation; STOP_SENDING was used. */
        MOQ_TEST_CHECK_EQ_SIZE(ctr.control_bytes, 0);
        MOQ_TEST_CHECK(ctr.bidi_stop >= 1);

        /* Publisher dropped the subscription and surfaced UNSUBSCRIBED. */
        bool unsub = false;
        moq_event_t ev;
        while (moq_session_poll_events(server, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_UNSUBSCRIBED) unsub = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(unsub);

        /* A late, well-formed subgroup for the now-unknown track alias must be
         * stopped (unknown alias), never close the session. Type 0x14 as the
         * 2-byte vi64 {0x80,0x14}; alias 112, group/subgroup/priority 0. */
        uint8_t late[] = { 0x80, 0x14, 0x70, 0x00, 0x00, 0x00 };
        (void)moq_session_on_data_bytes(client, moq_stream_ref_from_u64(0xDEAD),
                                        late, sizeof(late), false,
                                        moq_simpair_now_us(sp));

        MOQ_TEST_CHECK_EQ_INT((int)client->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT((int)server->state, (int)MOQ_SESS_ESTABLISHED);
        moq_simpair_destroy(sp);
    }

    /* == B. D18 fetch cancel before the response data uni opens ======== */
    {
        wire_counts_t ctr = {0};
        moq_simpair_t *sp = make_pair(&ctr);
        MOQ_TEST_CHECK(sp != NULL);
        moq_session_t *client = moq_simpair_client(sp);
        moq_session_t *server = moq_simpair_server(sp);
        moq_fetch_t cfh, sfh;
        MOQ_TEST_CHECK(setup_fetch(sp, &cfh, &sfh, false /* don't accept */));

        ctr.armed = true;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_fetch_cancel(client, cfh, moq_simpair_now_us(sp)),
            (int)MOQ_OK);
        /* The fetch slot is freed at cancel (reusable immediately) and the
         * request id is recorded in the cancel-tombstone cache so a late data
         * uni is absorbed without reoccupying the pool. */
        MOQ_TEST_CHECK(client->fetches[moq_handle_slot(cfh._opaque)].state
            == MOQ_FETCH_FREE);
        MOQ_TEST_CHECK(client->fetch_cancel_tomb_count >= 1);
        moq_simpair_run_until_quiescent(sp, 64, NULL);

        MOQ_TEST_CHECK_EQ_SIZE(ctr.control_bytes, 0);
        MOQ_TEST_CHECK(ctr.bidi_stop >= 1);
        /* No data uni was open, so no data-stream STOP is expected. */
        MOQ_TEST_CHECK_EQ_SIZE(ctr.data_stop, 0);

        bool cancelled = false;
        moq_event_t ev;
        while (moq_session_poll_events(server, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_CANCELLED) cancelled = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(cancelled);
        MOQ_TEST_CHECK_EQ_INT((int)client->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT((int)server->state, (int)MOQ_SESS_ESTABLISHED);
        moq_simpair_destroy(sp);
    }

    /* == C. D18 fetch cancel after the response data uni opens ========= *
     *  The publisher accepts (opening the data uni) and writes an object, then
     *  the fetcher cancels. The bidi STOP_SENDING tears down the request, the
     *  data uni is STOP_DATA'd, the publisher surfaces FETCH_CANCELLED, and any
     *  in-flight fetch object is absorbed without closing the fetcher. */
    {
        wire_counts_t ctr = {0};
        moq_simpair_t *sp = make_pair(&ctr);
        MOQ_TEST_CHECK(sp != NULL);
        moq_session_t *client = moq_simpair_client(sp);
        moq_session_t *server = moq_simpair_server(sp);
        moq_fetch_t cfh, sfh;
        MOQ_TEST_CHECK(setup_fetch(sp, &cfh, &sfh, true /* accept */));

        /* Publisher writes an object on the response data uni. */
        moq_rcbuf_t *p0 = NULL;
        moq_rcbuf_create(alloc, (const uint8_t *)"obj", 3, &p0);
        moq_fetch_object_cfg_t foc;
        moq_fetch_object_cfg_init(&foc);
        foc.group_id = 0; foc.subgroup_id = 0; foc.object_id = 0; foc.payload = p0;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_write_fetch_object(server, sfh, &foc,
                moq_simpair_now_us(sp)), (int)MOQ_OK);
        moq_rcbuf_decref(p0);

        ctr.armed = true;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_fetch_cancel(client, cfh, moq_simpair_now_us(sp)),
            (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 64, NULL);

        MOQ_TEST_CHECK_EQ_SIZE(ctr.control_bytes, 0);
        MOQ_TEST_CHECK(ctr.bidi_stop >= 1);
        /* The open response data uni was STOP_DATA'd by the fetcher. */
        MOQ_TEST_CHECK(ctr.data_stop >= 1);

        bool cancelled = false;
        moq_event_t ev;
        while (moq_session_poll_events(server, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_CANCELLED) cancelled = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(cancelled);
        MOQ_TEST_CHECK_EQ_INT((int)client->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT((int)server->state, (int)MOQ_SESS_ESTABLISHED);
        moq_simpair_destroy(sp);
    }

    /* == D. D16 regression: unsubscribe still uses a control message ==== *
     *  The draft-16 profile keeps the UNSUBSCRIBE control message and emits no
     *  bidi teardown, proving the new path is gated on the profile capability. */
    {
        wire_counts_t ctr = {0};
        moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
        cfg.alloc = alloc;
        cfg.version = MOQ_VERSION_DRAFT_16;
        cfg.trace_fn = count_trace;
        cfg.trace_ctx = &ctr;
        /* Draft-16 gates requests on granted capacity (MAX_REQUEST_ID). */
        cfg.server_send_request_capacity = true;
        cfg.server_initial_request_capacity = 16;
        moq_simpair_t *sp = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_simpair_create(&cfg, &sp), (int)MOQ_OK);
        moq_session_t *client = moq_simpair_client(sp);
        moq_session_t *server = moq_simpair_server(sp);
        moq_subscription_t ssub, csub;
        MOQ_TEST_CHECK(setup_subscription(sp, "video", &ssub, &csub));

        ctr.armed = true;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_unsubscribe(client, csub, moq_simpair_now_us(sp)),
            (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 32, NULL);

        /* D16 carries UNSUBSCRIBE as control bytes and never emits bidi stop. */
        MOQ_TEST_CHECK(ctr.control_bytes >= 1);
        MOQ_TEST_CHECK_EQ_SIZE(ctr.bidi_stop, 0);
        MOQ_TEST_CHECK_EQ_SIZE(ctr.bidi_reset, 0);
        MOQ_TEST_CHECK_EQ_INT((int)client->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT((int)server->state, (int)MOQ_SESS_ESTABLISHED);
        moq_simpair_destroy(sp);
    }

    /* == E. Delayed/reordered teardown still tears down ================ *
     *  With transport delay+reorder faults enabled, the STOP/RESET on the
     *  request bidi may mature out of order; the publisher must still observe
     *  the cancellation and neither side may close. Exercises the delayed
     *  bidi-stop delivery path. */
    {
        wire_counts_t ctr = {0};
        moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
        cfg.alloc = alloc;
        cfg.version = MOQ_VERSION_DRAFT_18;
        cfg.trace_fn = count_trace;
        cfg.trace_ctx = &ctr;
        cfg.fault_flags = MOQ_SIM_FAULT_DELAY | MOQ_SIM_FAULT_REORDER_ACTION;
        cfg.fault_per_mille = 1000;
        cfg.seed = 0x10A;
        moq_simpair_t *sp = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_simpair_create(&cfg, &sp), (int)MOQ_OK);
        moq_session_t *client = moq_simpair_client(sp);
        moq_session_t *server = moq_simpair_server(sp);
        moq_subscription_t ssub, csub;
        MOQ_TEST_CHECK(setup_subscription(sp, "video", &ssub, &csub));
        (void)ssub;

        moq_simpair_enable_faults(sp);   /* delay/reorder only after handshake */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_unsubscribe(client, csub, moq_simpair_now_us(sp)),
            (int)MOQ_OK);
        /* Drain by advancing virtual time to each pending deadline so delayed
         * teardown deliveries mature; don't poll events here so the assertion
         * below can observe them. */
        for (int i = 0; i < 64; i++) {
            moq_simpair_run_until_quiescent(sp, 32, NULL);
            uint64_t dl = moq_simpair_next_deadline_us(sp);
            if (dl == UINT64_MAX) break;
            moq_simpair_advance_to(sp, dl);
        }
        moq_simpair_run_until_quiescent(sp, 32, NULL);

        bool unsub = false;
        moq_event_t ev;
        while (moq_session_poll_events(server, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_UNSUBSCRIBED) unsub = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(unsub);
        MOQ_TEST_CHECK_EQ_INT((int)client->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT((int)server->state, (int)MOQ_SESS_ESTABLISHED);
        moq_simpair_destroy(sp);
    }

    MOQ_TEST_PASS("d18_teardown");
    return failures != 0;
}
