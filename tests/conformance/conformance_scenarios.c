#include "conformance_scenarios.h"
#include <moq/session.h>
#include <moq/rcbuf.h>
#include <stdio.h>
#include <string.h>

#define CFAIL(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "CONFORMANCE FAIL: %s:%d: %s\n", \
                __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

/* -- Helpers -------------------------------------------------------- */

static void drain_events(moq_session_t *s)
{
    moq_event_t ev[16]; size_t ne;
    moq_session_poll_events_ex(s, ev, 16, sizeof(moq_event_t), &ne);
    for (size_t i = 0; i < ne; i++)
        moq_event_cleanup(&ev[i]);
}

static bool has_event(moq_session_t *s, uint32_t kind)
{
    moq_event_t ev[16]; size_t ne;
    moq_session_poll_events_ex(s, ev, 16, sizeof(moq_event_t), &ne);
    bool found = false;
    for (size_t i = 0; i < ne; i++) {
        if (ev[i].kind == kind) found = true;
        moq_event_cleanup(&ev[i]);
    }
    return found;
}

/* ================================================================== */
/* 1. Setup handshake                                                  */
/* ================================================================== */

int conformance_setup_handshake(moq_adapter_pair_t *pair)
{
    int failures = 0;
    moq_session_t *client = moq_pair_client(pair);
    moq_session_t *server = moq_pair_server(pair);

    CFAIL(moq_session_start(client, pair->ops->now_us(pair->ctx)) >= 0);

    moq_adapter_pair_pump_result_t r = moq_pair_pump(pair);
    CFAIL(r == MOQ_ADAPTER_PAIR_QUIESCENT);

    bool client_setup = has_event(client, MOQ_EVENT_SETUP_COMPLETE);
    bool server_setup = has_event(server, MOQ_EVENT_SETUP_COMPLETE);
    CFAIL(client_setup);
    CFAIL(server_setup);

    CFAIL(!pair->ops->has_error(pair->ctx));
    return failures;
}

/* ================================================================== */
/* 2. Subscribe + object delivery                                      */
/* ================================================================== */

int conformance_subscribe_and_object(moq_adapter_pair_t *pair)
{
    int failures = 0;
    moq_session_t *client = moq_pair_client(pair);
    moq_session_t *server = moq_pair_server(pair);

    CFAIL(moq_session_start(client, pair->ops->now_us(pair->ctx)) >= 0);
    moq_pair_pump(pair);
    drain_events(client);
    drain_events(server);

    /* Client subscribes. */
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    moq_bytes_t ns[] = {{(const uint8_t *)"t", 1}};
    sc.track_namespace.parts = ns;
    sc.track_namespace.count = 1;
    sc.track_name = (moq_bytes_t){(const uint8_t *)"v", 1};
    sc.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
    moq_subscription_t sub;
    CFAIL(moq_session_subscribe(client, &sc, 0, &sub) >= 0);

    moq_pair_pump(pair);

    /* Server accepts. */
    moq_event_t sev[16]; size_t sne;
    moq_session_poll_events_ex(server, sev, 16, sizeof(moq_event_t), &sne);
    moq_subscription_t ss = {0};
    bool got_req = false;
    for (size_t i = 0; i < sne; i++) {
        if (sev[i].kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
            ss = sev[i].u.subscribe_request.sub;
            got_req = true;
        }
        moq_event_cleanup(&sev[i]);
    }
    CFAIL(got_req);
    if (!got_req) return failures;

    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    CFAIL(moq_session_accept_subscribe(server, ss, &ac, 0) >= 0);
    moq_pair_pump(pair);

    /* Client sees SUBSCRIBE_OK. */
    bool sub_ok = has_event(client, MOQ_EVENT_SUBSCRIBE_OK);
    CFAIL(sub_ok);
    if (!sub_ok) return failures;

    /* Server writes an object in a subgroup. */
    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = 0;
    sgc.publisher_priority = 200;
    moq_subgroup_handle_t sg;
    CFAIL(moq_session_open_subgroup(server, ss, &sgc, 0, &sg) >= 0);

    const uint8_t payload[] = {0xCA, 0xFE};
    moq_rcbuf_t *buf = NULL;
    CFAIL(moq_rcbuf_create(moq_alloc_default(), payload, 2, &buf) >= 0);
    CFAIL(moq_session_write_object(server, sg, 0, buf, 0) >= 0);
    moq_rcbuf_decref(buf);
    CFAIL(moq_session_close_subgroup(server, sg, 0) >= 0);

    moq_pair_pump(pair);

    /* Client receives the object. */
    moq_event_t cev[16]; size_t cne;
    moq_session_poll_events_ex(client, cev, 16, sizeof(moq_event_t), &cne);
    bool got_obj = false;
    for (size_t i = 0; i < cne; i++) {
        if (cev[i].kind == MOQ_EVENT_OBJECT_RECEIVED) {
            moq_object_received_event_t *o = &cev[i].u.object_received;
            if (o->payload) {
                const uint8_t *d = moq_rcbuf_data(o->payload);
                size_t l = moq_rcbuf_len(o->payload);
                if (l == 2 && d[0] == 0xCA && d[1] == 0xFE)
                    got_obj = true;
            }
        }
        moq_event_cleanup(&cev[i]);
    }
    CFAIL(got_obj);
    CFAIL(!pair->ops->has_error(pair->ctx));

    return failures;
}

/* ================================================================== */
/* 3. Datagram object                                                  */
/* ================================================================== */

int conformance_datagram_object(moq_adapter_pair_t *pair)
{
    int failures = 0;

    if (!moq_pair_has_cap(pair, MOQ_ADAPTER_PAIR_CAP_DATAGRAMS)) {
        fprintf(stderr, "SKIP: conformance_datagram_object "
                "(adapter lacks DATAGRAMS capability)\n");
        return 0;
    }

    moq_session_t *client = moq_pair_client(pair);
    moq_session_t *server = moq_pair_server(pair);

    CFAIL(moq_session_start(client, pair->ops->now_us(pair->ctx)) >= 0);
    moq_pair_pump(pair);
    drain_events(client);
    drain_events(server);

    /* Subscribe so datagram has a target. */
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    moq_bytes_t ns[] = {{(const uint8_t *)"d", 1}};
    sc.track_namespace.parts = ns;
    sc.track_namespace.count = 1;
    sc.track_name = (moq_bytes_t){(const uint8_t *)"g", 1};
    sc.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
    moq_subscription_t sub;
    CFAIL(moq_session_subscribe(client, &sc, 0, &sub) >= 0);
    moq_pair_pump(pair);

    moq_event_t sev[16]; size_t sne;
    moq_session_poll_events_ex(server, sev, 16, sizeof(moq_event_t), &sne);
    moq_subscription_t ss = {0};
    bool got_req = false;
    for (size_t i = 0; i < sne; i++) {
        if (sev[i].kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
            ss = sev[i].u.subscribe_request.sub;
            got_req = true;
        }
        moq_event_cleanup(&sev[i]);
    }
    CFAIL(got_req);
    if (!got_req) return failures;

    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    CFAIL(moq_session_accept_subscribe(server, ss, &ac, 0) >= 0);
    moq_pair_pump(pair);
    drain_events(client);

    /* Server sends datagram. */
    const uint8_t dgp[] = {0xDA, 0x7A};
    moq_rcbuf_t *dgbuf = NULL;
    CFAIL(moq_rcbuf_create(moq_alloc_default(), dgp, 2, &dgbuf) >= 0);
    CFAIL(moq_session_send_object_datagram(
        server, ss, 5, 3, 200, false, dgbuf, NULL, 0, 0) >= 0);
    moq_rcbuf_decref(dgbuf);

    moq_pair_pump(pair);

    /* Client receives datagram. */
    moq_event_t cev[16]; size_t cne;
    moq_session_poll_events_ex(client, cev, 16, sizeof(moq_event_t), &cne);
    bool got_dg = false;
    for (size_t i = 0; i < cne; i++) {
        if (cev[i].kind == MOQ_EVENT_OBJECT_RECEIVED &&
            cev[i].u.object_received.datagram)
            got_dg = true;
        moq_event_cleanup(&cev[i]);
    }
    CFAIL(got_dg);
    CFAIL(!pair->ops->has_error(pair->ctx));

    return failures;
}

/* ================================================================== */
/* 4. Single control stream                                            */
/* ================================================================== */

int conformance_single_control_stream(moq_adapter_pair_t *pair)
{
    int failures = 0;

    if (!moq_pair_has_cap(pair, MOQ_ADAPTER_PAIR_CAP_OPENED_BIDI_COUNT)) {
        fprintf(stderr, "SKIP: conformance_single_control_stream "
                "(adapter lacks OPENED_BIDI_COUNT capability)\n");
        return 0;
    }

    moq_session_t *client = moq_pair_client(pair);

    CFAIL(moq_session_start(client, pair->ops->now_us(pair->ctx)) >= 0);
    moq_pair_pump(pair);
    drain_events(client);
    drain_events(moq_pair_server(pair));

    /* After setup, the server must NOT have opened its own bidi
     * control stream. In draft-16, the server responds on the
     * client-opened control bidi. */
    size_t server_bidi = pair->ops->opened_bidi_count(
        pair->ctx, MOQ_ADAPTER_PAIR_SERVER);
    CFAIL(server_bidi == 0);

    /* Client opened exactly one bidi (the control stream). */
    size_t client_bidi = pair->ops->opened_bidi_count(
        pair->ctx, MOQ_ADAPTER_PAIR_CLIENT);
    CFAIL(client_bidi == 1);

    CFAIL(!pair->ops->has_error(pair->ctx));
    return failures;
}

/* ================================================================== */
/* 5. Clean close is not fatal                                         */
/* ================================================================== */

int conformance_close_not_fatal(moq_adapter_pair_t *pair)
{
    int failures = 0;

    if (!moq_pair_has_cap(pair, MOQ_ADAPTER_PAIR_CAP_VIRTUAL_TIME)) {
        fprintf(stderr, "SKIP: conformance_close_not_fatal "
                "(adapter lacks VIRTUAL_TIME capability)\n");
        return 0;
    }

    moq_session_t *client = moq_pair_client(pair);
    moq_session_t *server = moq_pair_server(pair);
    uint64_t now = pair->ops->now_us(pair->ctx);

    CFAIL(moq_session_start(client, now) >= 0);
    moq_pair_pump(pair);
    drain_events(client);
    drain_events(server);

    /* Server sends GOAWAY. The session has goaway_timeout_us = 0
     * by default, so CLOSE_SESSION fires on the next tick. */
    CFAIL(moq_session_goaway(server, NULL, 0, now) >= 0);
    moq_pair_pump(pair);

    /* Advance past any drain timeout. */
    uint64_t dl = pair->ops->next_deadline_us(pair->ctx);
    if (dl != UINT64_MAX) {
        pair->ops->advance_to(pair->ctx, dl + 1);
        moq_pair_pump(pair);
    }

    /* Server should be closed, not fatal. */
    CFAIL(pair->ops->is_closed(pair->ctx, MOQ_ADAPTER_PAIR_SERVER));
    CFAIL(!pair->ops->has_fatal(pair->ctx, MOQ_ADAPTER_PAIR_SERVER));

    return failures;
}

/* ================================================================== */
/* 6. Session timer drives GOAWAY drain                                */
/* ================================================================== */

int conformance_session_timer_goaway(moq_adapter_pair_t *pair)
{
    int failures = 0;

    if (!moq_pair_has_cap(pair, MOQ_ADAPTER_PAIR_CAP_VIRTUAL_TIME)) {
        fprintf(stderr, "SKIP: conformance_session_timer_goaway "
                "(adapter lacks VIRTUAL_TIME capability)\n");
        return 0;
    }

    moq_session_t *client = moq_pair_client(pair);
    moq_session_t *server = moq_pair_server(pair);
    uint64_t now = pair->ops->now_us(pair->ctx);

    CFAIL(moq_session_start(client, now) >= 0);
    moq_pair_pump(pair);
    drain_events(client);
    drain_events(server);

    /* Server sends GOAWAY. */
    CFAIL(moq_session_goaway(server, NULL, 0, now) >= 0);
    moq_pair_pump(pair);

    /* A deadline should exist for the GOAWAY drain. */
    uint64_t dl = pair->ops->next_deadline_us(pair->ctx);
    CFAIL(dl != UINT64_MAX);
    if (dl == UINT64_MAX) return failures;

    /* Advance time past the deadline and pump. The adapter's
     * service() should call moq_session_tick(), which fires
     * the drain timeout → CLOSE_SESSION. */
    pair->ops->advance_to(pair->ctx, dl + 1);
    moq_pair_pump(pair);

    /* Server should be closed via timer-driven tick. */
    CFAIL(pair->ops->is_closed(pair->ctx, MOQ_ADAPTER_PAIR_SERVER));

    return failures;
}

/* ================================================================== */
/* 7. Bidi half-close: accept after namespace subscribe                */
/* ================================================================== */

int conformance_bidi_halfclose_accept(moq_adapter_pair_t *pair)
{
    int failures = 0;

    if (!moq_pair_has_cap(pair, MOQ_ADAPTER_PAIR_CAP_BIDI_STREAMS) ||
        !moq_pair_has_cap(pair, MOQ_ADAPTER_PAIR_CAP_INJECT_BIDI_FIN))
        return 0;

    moq_session_t *client = moq_pair_client(pair);
    moq_session_t *server = moq_pair_server(pair);
    uint64_t now = pair->ops->now_us(pair->ctx);

    CFAIL(moq_session_start(client, now) >= 0);
    moq_pair_pump(pair);
    drain_events(client);
    drain_events(server);

    moq_subscribe_namespace_cfg_t nscfg;
    moq_subscribe_namespace_cfg_init(&nscfg);
    moq_bytes_t ns[] = {{(const uint8_t *)"hc", 2}};
    nscfg.track_namespace_prefix.parts = ns;
    nscfg.track_namespace_prefix.count = 1;
    moq_ns_sub_handle_t nsub;
    CFAIL(moq_session_subscribe_namespace(client, &nscfg, now, &nsub) >= 0);
    moq_pair_pump(pair);

    moq_event_t sev[16]; size_t sne;
    moq_session_poll_events_ex(server, sev, 16, sizeof(moq_event_t), &sne);
    moq_ns_sub_handle_t server_nsub = {0};
    bool got_req = false;
    for (size_t i = 0; i < sne; i++) {
        if (sev[i].kind == MOQ_EVENT_NS_SUB_REQUEST) {
            server_nsub = sev[i].u.ns_sub_request.handle;
            got_req = true;
        }
        moq_event_cleanup(&sev[i]);
    }
    CFAIL(got_req);
    if (!got_req) return failures;

    /* Inject client FIN before server accepts — this is the
     * half-close: peer send side closed, but server must still
     * be able to write the accept response. */
    if (moq_pair_has_cap(pair, MOQ_ADAPTER_PAIR_CAP_INJECT_BIDI_FIN)) {
        CFAIL(pair->ops->inject_bidi_fin(
            pair->ctx, MOQ_ADAPTER_PAIR_CLIENT) == 0);
    }

    moq_accept_ns_sub_cfg_t acfg;
    moq_accept_ns_sub_cfg_init(&acfg);
    CFAIL(moq_session_accept_ns_sub(server, server_nsub, &acfg, now) >= 0);
    moq_pair_pump(pair);

    bool got_ok = has_event(client, MOQ_EVENT_NS_SUB_OK);
    CFAIL(got_ok);
    CFAIL(!pair->ops->has_error(pair->ctx));

    return failures;
}

/* ================================================================== */
/* 8. Bidi half-close: reject + tombstone + late FIN                   */
/* ================================================================== */

int conformance_bidi_halfclose_reject_late_fin(moq_adapter_pair_t *pair)
{
    int failures = 0;

    if (!moq_pair_has_cap(pair, MOQ_ADAPTER_PAIR_CAP_BIDI_STREAMS) ||
        !moq_pair_has_cap(pair, MOQ_ADAPTER_PAIR_CAP_TOMBSTONES) ||
        !moq_pair_has_cap(pair, MOQ_ADAPTER_PAIR_CAP_INJECT_BIDI_FIN))
        return 0;

    moq_session_t *client = moq_pair_client(pair);
    moq_session_t *server = moq_pair_server(pair);
    uint64_t now = pair->ops->now_us(pair->ctx);

    CFAIL(moq_session_start(client, now) >= 0);
    moq_pair_pump(pair);
    drain_events(client);
    drain_events(server);

    moq_subscribe_namespace_cfg_t nscfg;
    moq_subscribe_namespace_cfg_init(&nscfg);
    moq_bytes_t ns[] = {{(const uint8_t *)"rj", 2}};
    nscfg.track_namespace_prefix.parts = ns;
    nscfg.track_namespace_prefix.count = 1;
    moq_ns_sub_handle_t nsub;
    CFAIL(moq_session_subscribe_namespace(client, &nscfg, now, &nsub) >= 0);
    moq_pair_pump(pair);

    moq_event_t sev[16]; size_t sne;
    moq_session_poll_events_ex(server, sev, 16, sizeof(moq_event_t), &sne);
    moq_ns_sub_handle_t server_nsub = {0};
    bool got_req = false;
    for (size_t i = 0; i < sne; i++) {
        if (sev[i].kind == MOQ_EVENT_NS_SUB_REQUEST) {
            server_nsub = sev[i].u.ns_sub_request.handle;
            got_req = true;
        }
        moq_event_cleanup(&sev[i]);
    }
    CFAIL(got_req);
    if (!got_req) return failures;

    moq_reject_ns_sub_cfg_t rcfg;
    moq_reject_ns_sub_cfg_init(&rcfg);
    rcfg.error_code = 0x42;
    CFAIL(moq_session_reject_ns_sub(server, server_nsub, &rcfg, now) >= 0);
    moq_pair_pump(pair);

    bool got_err = has_event(client, MOQ_EVENT_NS_SUB_ERROR);
    CFAIL(got_err);

    /* After reject+pump, the server should have tombstoned the
     * bidi stream (local FIN sent, peer FIN not yet received). */
    if (moq_pair_has_cap(pair, MOQ_ADAPTER_PAIR_CAP_TOMBSTONES)) {
        size_t tc = pair->ops->tombstone_count(
            pair->ctx, MOQ_ADAPTER_PAIR_SERVER);
        CFAIL(tc > 0);

        /* Inject late client FIN — tombstone should absorb it. */
        if (moq_pair_has_cap(pair, MOQ_ADAPTER_PAIR_CAP_INJECT_BIDI_FIN)) {
            CFAIL(pair->ops->inject_bidi_fin(
                pair->ctx, MOQ_ADAPTER_PAIR_CLIENT) == 0);
            size_t tc2 = pair->ops->tombstone_count(
                pair->ctx, MOQ_ADAPTER_PAIR_SERVER);
            CFAIL(tc2 < tc);
        }
    }

    CFAIL(!pair->ops->has_error(pair->ctx));

    return failures;
}

/* ================================================================== */
/* 9. Crossed cancel: client cancels while server responds             */
/* ================================================================== */

int conformance_crossed_cancel(moq_adapter_pair_t *pair)
{
    int failures = 0;

    if (!moq_pair_has_cap(pair, MOQ_ADAPTER_PAIR_CAP_BIDI_STREAMS))
        return 0;

    moq_session_t *client = moq_pair_client(pair);
    moq_session_t *server = moq_pair_server(pair);
    uint64_t now = pair->ops->now_us(pair->ctx);

    CFAIL(moq_session_start(client, now) >= 0);
    moq_pair_pump(pair);
    drain_events(client);
    drain_events(server);

    moq_subscribe_namespace_cfg_t nscfg;
    moq_subscribe_namespace_cfg_init(&nscfg);
    moq_bytes_t ns[] = {{(const uint8_t *)"cx", 2}};
    nscfg.track_namespace_prefix.parts = ns;
    nscfg.track_namespace_prefix.count = 1;
    moq_ns_sub_handle_t client_nsub;
    CFAIL(moq_session_subscribe_namespace(client, &nscfg, now, &client_nsub) >= 0);
    moq_pair_pump(pair);

    moq_event_t sev[16]; size_t sne;
    moq_session_poll_events_ex(server, sev, 16, sizeof(moq_event_t), &sne);
    moq_ns_sub_handle_t server_nsub = {0};
    bool got_req = false;
    for (size_t i = 0; i < sne; i++) {
        if (sev[i].kind == MOQ_EVENT_NS_SUB_REQUEST) {
            server_nsub = sev[i].u.ns_sub_request.handle;
            got_req = true;
        }
        moq_event_cleanup(&sev[i]);
    }
    CFAIL(got_req);
    if (!got_req) return failures;

    /* Client cancels before server responds. */
    CFAIL(moq_session_cancel_namespace_sub(client, client_nsub, now) >= 0);

    /* Server rejects (crossed response). */
    moq_reject_ns_sub_cfg_t rcfg;
    moq_reject_ns_sub_cfg_init(&rcfg);
    rcfg.error_code = 0x42;
    CFAIL(moq_session_reject_ns_sub(server, server_nsub, &rcfg, now) >= 0);

    /* Pump both sides — crossed messages should be absorbed. */
    moq_pair_pump(pair);
    drain_events(client);

    CFAIL(!pair->ops->has_fatal(pair->ctx, MOQ_ADAPTER_PAIR_CLIENT));
    CFAIL(!pair->ops->has_fatal(pair->ctx, MOQ_ADAPTER_PAIR_SERVER));

    return failures;
}

/* ================================================================== */
/* 10. Reset propagation                                               */
/* ================================================================== */

int conformance_reset_propagation(moq_adapter_pair_t *pair)
{
    int failures = 0;
    moq_session_t *client = moq_pair_client(pair);
    moq_session_t *server = moq_pair_server(pair);
    uint64_t now = pair->ops->now_us(pair->ctx);

    CFAIL(moq_session_start(client, now) >= 0);
    moq_pair_pump(pair);
    drain_events(client);
    drain_events(server);

    /* Subscribe + accept. */
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    moq_bytes_t ns[] = {{(const uint8_t *)"r", 1}};
    sc.track_namespace.parts = ns;
    sc.track_namespace.count = 1;
    sc.track_name = (moq_bytes_t){(const uint8_t *)"s", 1};
    sc.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
    moq_subscription_t sub;
    CFAIL(moq_session_subscribe(client, &sc, now, &sub) >= 0);
    moq_pair_pump(pair);

    moq_event_t sev[16]; size_t sne;
    moq_session_poll_events_ex(server, sev, 16, sizeof(moq_event_t), &sne);
    moq_subscription_t ss = {0};
    bool got_req = false;
    for (size_t i = 0; i < sne; i++) {
        if (sev[i].kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
            ss = sev[i].u.subscribe_request.sub;
            got_req = true;
        }
        moq_event_cleanup(&sev[i]);
    }
    CFAIL(got_req);
    if (!got_req) return failures;

    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    CFAIL(moq_session_accept_subscribe(server, ss, &ac, now) >= 0);
    moq_pair_pump(pair);
    drain_events(client);

    /* Open subgroup, write one object, then reset. */
    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = 0;
    sgc.publisher_priority = 200;
    moq_subgroup_handle_t sg;
    CFAIL(moq_session_open_subgroup(server, ss, &sgc, now, &sg) >= 0);
    uint8_t payload[] = {0x11};
    moq_rcbuf_t *buf = NULL;
    CFAIL(moq_rcbuf_create(moq_alloc_default(), payload, 1, &buf) >= 0);
    CFAIL(moq_session_write_object(server, sg, 0, buf, now) >= 0);
    moq_rcbuf_decref(buf);

    /* Service to open the uni stream and write the object. */
    moq_pair_pump(pair);

    /* Record server stream count before reset. The uni stream
     * should be active (object written, not yet closed). */
    size_t sc_before = 0;
    if (moq_pair_has_cap(pair, MOQ_ADAPTER_PAIR_CAP_STREAM_COUNTS))
        sc_before = pair->ops->stream_count(
            pair->ctx, MOQ_ADAPTER_PAIR_SERVER);

    /* Record client stream count before the reset is delivered. */
    size_t cc_before = 0;
    if (moq_pair_has_cap(pair, MOQ_ADAPTER_PAIR_CAP_STREAM_COUNTS))
        cc_before = pair->ops->stream_count(
            pair->ctx, MOQ_ADAPTER_PAIR_CLIENT);

    CFAIL(moq_session_reset_subgroup(server, sg, 0x77, now) >= 0);
    moq_pair_pump(pair);

    /* Server stream mapping deactivated by outbound reset. */
    if (moq_pair_has_cap(pair, MOQ_ADAPTER_PAIR_CAP_STREAM_COUNTS)) {
        size_t sc_after = pair->ops->stream_count(
            pair->ctx, MOQ_ADAPTER_PAIR_SERVER);
        CFAIL(sc_after < sc_before);
    }

    /* Client should see the object. */
    bool got_obj = has_event(client, MOQ_EVENT_OBJECT_RECEIVED);
    CFAIL(got_obj);

    /* Client-side stream mapping deactivated by inbound reset.
     * This proves the reset was actually propagated to the peer,
     * not just dropped. */
    if (moq_pair_has_cap(pair, MOQ_ADAPTER_PAIR_CAP_STREAM_COUNTS)) {
        size_t cc_after = pair->ops->stream_count(
            pair->ctx, MOQ_ADAPTER_PAIR_CLIENT);
        CFAIL(cc_after < cc_before);
    }

    CFAIL(!pair->ops->has_error(pair->ctx));

    return failures;
}

/* ================================================================== */
/* 11. Would-block ordering: first stream blocks, FIFO preserved      */
/* ================================================================== */

int conformance_would_block_ordering(moq_adapter_pair_t *pair)
{
    int failures = 0;

    if (!moq_pair_has_cap(pair, MOQ_ADAPTER_PAIR_CAP_BLOCK_WRITES))
        return 0;

    moq_session_t *client = moq_pair_client(pair);
    moq_session_t *server = moq_pair_server(pair);
    uint64_t now = pair->ops->now_us(pair->ctx);

    CFAIL(moq_session_start(client, now) >= 0);
    moq_pair_pump(pair);
    drain_events(client);
    drain_events(server);

    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    moq_bytes_t ns[] = {{(const uint8_t *)"w", 1}};
    sc.track_namespace.parts = ns;
    sc.track_namespace.count = 1;
    sc.track_name = (moq_bytes_t){(const uint8_t *)"b", 1};
    sc.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
    moq_subscription_t sub;
    CFAIL(moq_session_subscribe(client, &sc, now, &sub) >= 0);
    moq_pair_pump(pair);

    moq_event_t sev[16]; size_t sne;
    moq_session_poll_events_ex(server, sev, 16, sizeof(moq_event_t), &sne);
    moq_subscription_t ss = {0};
    bool got_req = false;
    for (size_t i = 0; i < sne; i++) {
        if (sev[i].kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
            ss = sev[i].u.subscribe_request.sub;
            got_req = true;
        }
        moq_event_cleanup(&sev[i]);
    }
    CFAIL(got_req);
    if (!got_req) return failures;

    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    CFAIL(moq_session_accept_subscribe(server, ss, &ac, now) >= 0);
    moq_pair_pump(pair);
    drain_events(client);

    /* Block server writes, then write two ordered objects. */
    pair->ops->block_writes(pair->ctx, MOQ_ADAPTER_PAIR_SERVER, true);

    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = 0;
    sgc.publisher_priority = 200;
    moq_subgroup_handle_t sg;
    CFAIL(moq_session_open_subgroup(server, ss, &sgc, now, &sg) >= 0);

    uint8_t p1[] = {0xAA};
    moq_rcbuf_t *b1 = NULL;
    CFAIL(moq_rcbuf_create(moq_alloc_default(), p1, 1, &b1) >= 0);
    CFAIL(moq_session_write_object(server, sg, 0, b1, now) >= 0);
    moq_rcbuf_decref(b1);

    uint8_t p2[] = {0xBB};
    moq_rcbuf_t *b2 = NULL;
    CFAIL(moq_rcbuf_create(moq_alloc_default(), p2, 1, &b2) >= 0);
    CFAIL(moq_session_write_object(server, sg, 1, b2, now) >= 0);
    moq_rcbuf_decref(b2);

    CFAIL(moq_session_close_subgroup(server, sg, now) >= 0);

    /* Service should not fatal under would-block. */
    moq_pair_pump(pair);
    CFAIL(!pair->ops->has_error(pair->ctx));

    /* Unblock and pump — both objects should arrive in order. */
    pair->ops->block_writes(pair->ctx, MOQ_ADAPTER_PAIR_SERVER, false);
    moq_pair_pump(pair);

    moq_event_t cev[16]; size_t cne;
    moq_session_poll_events_ex(client, cev, 16, sizeof(moq_event_t), &cne);
    int obj_count = 0;
    uint64_t prev_oid = 0;
    bool order_ok = true;
    for (size_t i = 0; i < cne; i++) {
        if (cev[i].kind == MOQ_EVENT_OBJECT_RECEIVED) {
            uint64_t oid = cev[i].u.object_received.object_id;
            if (obj_count > 0 && oid <= prev_oid)
                order_ok = false;
            prev_oid = oid;
            obj_count++;
        }
        moq_event_cleanup(&cev[i]);
    }
    CFAIL(obj_count == 2);
    CFAIL(order_ok);
    CFAIL(!pair->ops->has_error(pair->ctx));

    return failures;
}

/* ================================================================== */
/* 12. Dropped datagram does not stall stream FIFO                     */
/* ================================================================== */

int conformance_dropped_datagram_unblocks(moq_adapter_pair_t *pair)
{
    int failures = 0;

    if (!moq_pair_has_cap(pair, MOQ_ADAPTER_PAIR_CAP_DROP_DATAGRAMS) ||
        !moq_pair_has_cap(pair, MOQ_ADAPTER_PAIR_CAP_DATAGRAMS))
        return 0;

    moq_session_t *client = moq_pair_client(pair);
    moq_session_t *server = moq_pair_server(pair);
    uint64_t now = pair->ops->now_us(pair->ctx);

    CFAIL(moq_session_start(client, now) >= 0);
    moq_pair_pump(pair);
    drain_events(client);
    drain_events(server);

    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    moq_bytes_t ns[] = {{(const uint8_t *)"d", 1}};
    sc.track_namespace.parts = ns;
    sc.track_namespace.count = 1;
    sc.track_name = (moq_bytes_t){(const uint8_t *)"d", 1};
    sc.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
    moq_subscription_t sub;
    CFAIL(moq_session_subscribe(client, &sc, now, &sub) >= 0);
    moq_pair_pump(pair);

    moq_event_t sev[16]; size_t sne;
    moq_session_poll_events_ex(server, sev, 16, sizeof(moq_event_t), &sne);
    moq_subscription_t ss = {0};
    bool got_req = false;
    for (size_t i = 0; i < sne; i++) {
        if (sev[i].kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
            ss = sev[i].u.subscribe_request.sub;
            got_req = true;
        }
        moq_event_cleanup(&sev[i]);
    }
    CFAIL(got_req);
    if (!got_req) return failures;

    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    CFAIL(moq_session_accept_subscribe(server, ss, &ac, now) >= 0);
    moq_pair_pump(pair);
    drain_events(client);

    /* Drop datagrams, then send a datagram followed by a stream
     * object. The datagram is silently dropped. The stream object
     * must still arrive — the drop must not stall the FIFO. */
    pair->ops->drop_datagrams(pair->ctx, MOQ_ADAPTER_PAIR_SERVER, true);

    const uint8_t dgp[] = {0xDD};
    moq_rcbuf_t *dgbuf = NULL;
    CFAIL(moq_rcbuf_create(moq_alloc_default(), dgp, 1, &dgbuf) >= 0);
    CFAIL(moq_session_send_object_datagram(
        server, ss, 0, 0, 200, false, dgbuf, NULL, 0, now) >= 0);
    moq_rcbuf_decref(dgbuf);

    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = 1;
    sgc.publisher_priority = 200;
    moq_subgroup_handle_t sg;
    CFAIL(moq_session_open_subgroup(server, ss, &sgc, now, &sg) >= 0);
    uint8_t payload[] = {0xEE};
    moq_rcbuf_t *buf = NULL;
    CFAIL(moq_rcbuf_create(moq_alloc_default(), payload, 1, &buf) >= 0);
    CFAIL(moq_session_write_object(server, sg, 0, buf, now) >= 0);
    moq_rcbuf_decref(buf);
    CFAIL(moq_session_close_subgroup(server, sg, now) >= 0);

    moq_pair_pump(pair);

    /* Stream object should arrive with correct payload.
     * No datagram object should arrive (it was dropped). */
    bool got_stream_obj = false;
    bool got_datagram = false;
    moq_event_t cev[16]; size_t cne;
    moq_session_poll_events_ex(client, cev, 16, sizeof(moq_event_t), &cne);
    for (size_t i = 0; i < cne; i++) {
        if (cev[i].kind == MOQ_EVENT_OBJECT_RECEIVED) {
            if (cev[i].u.object_received.datagram) {
                got_datagram = true;
            } else if (cev[i].u.object_received.payload) {
                const uint8_t *d = moq_rcbuf_data(
                    cev[i].u.object_received.payload);
                size_t l = moq_rcbuf_len(
                    cev[i].u.object_received.payload);
                if (l == 1 && d[0] == 0xEE)
                    got_stream_obj = true;
            }
        }
        moq_event_cleanup(&cev[i]);
    }
    CFAIL(got_stream_obj);
    CFAIL(!got_datagram);
    CFAIL(!pair->ops->has_error(pair->ctx));

    return failures;
}

/* ================================================================== */
/* 13. Blocked namespace reject + tombstone absorption                 */
/* ================================================================== */

/*
 * A retried SEND_BIDI_STREAM with fin=true must mark local_send_closed and
 * tombstone the bidi stream: a blocked reject still produces a tombstone, and
 * a late client FIN is absorbed without fatal.
 */
int conformance_blocked_ns_reject_tombstone(moq_adapter_pair_t *pair)
{
    int failures = 0;

    if (!moq_pair_has_cap(pair, MOQ_ADAPTER_PAIR_CAP_BIDI_STREAMS) ||
        !moq_pair_has_cap(pair, MOQ_ADAPTER_PAIR_CAP_TOMBSTONES) ||
        !moq_pair_has_cap(pair, MOQ_ADAPTER_PAIR_CAP_BLOCK_WRITES) ||
        !moq_pair_has_cap(pair, MOQ_ADAPTER_PAIR_CAP_INJECT_BIDI_FIN))
        return 0;

    moq_session_t *client = moq_pair_client(pair);
    moq_session_t *server = moq_pair_server(pair);
    uint64_t now = pair->ops->now_us(pair->ctx);

    CFAIL(moq_session_start(client, now) >= 0);
    moq_pair_pump(pair);
    drain_events(client);
    drain_events(server);

    /* Client subscribes to a namespace. */
    moq_subscribe_namespace_cfg_t nscfg;
    moq_subscribe_namespace_cfg_init(&nscfg);
    moq_bytes_t ns[] = {{(const uint8_t *)"blk", 3}};
    nscfg.track_namespace_prefix.parts = ns;
    nscfg.track_namespace_prefix.count = 1;
    moq_ns_sub_handle_t nsub;
    CFAIL(moq_session_subscribe_namespace(client, &nscfg, now, &nsub) >= 0);
    moq_pair_pump(pair);

    /* Server receives the request. */
    moq_event_t sev[16]; size_t sne;
    moq_session_poll_events_ex(server, sev, 16, sizeof(moq_event_t), &sne);
    moq_ns_sub_handle_t server_nsub = {0};
    bool got_req = false;
    for (size_t i = 0; i < sne; i++) {
        if (sev[i].kind == MOQ_EVENT_NS_SUB_REQUEST) {
            server_nsub = sev[i].u.ns_sub_request.handle;
            got_req = true;
        }
        moq_event_cleanup(&sev[i]);
    }
    CFAIL(got_req);
    if (!got_req) return failures;

    /* Block server writes, then reject. The reject queues
     * SEND_BIDI_STREAM(fin=true) which goes to the pending queue. */
    pair->ops->block_writes(pair->ctx, MOQ_ADAPTER_PAIR_SERVER, true);

    moq_reject_ns_sub_cfg_t rcfg;
    moq_reject_ns_sub_cfg_init(&rcfg);
    rcfg.error_code = 0x42;
    CFAIL(moq_session_reject_ns_sub(server, server_nsub, &rcfg, now) >= 0);

    /* Pump while blocked — reject data is pending. */
    moq_pair_pump(pair);

    /* Unblock and pump — pending reject should be retried, FIN
     * delivered, stream tombstoned. */
    pair->ops->block_writes(pair->ctx, MOQ_ADAPTER_PAIR_SERVER, false);
    moq_pair_pump(pair);

    /* Server must have tombstoned the bidi stream. */
    size_t tc = pair->ops->tombstone_count(
        pair->ctx, MOQ_ADAPTER_PAIR_SERVER);
    CFAIL(tc > 0);

    /* Inject late client FIN — tombstone should absorb it. */
    CFAIL(pair->ops->inject_bidi_fin(
        pair->ctx, MOQ_ADAPTER_PAIR_CLIENT) == 0);

    /* No fatal error — tombstone absorbed the late FIN. */
    CFAIL(!pair->ops->has_error(pair->ctx));
    CFAIL(!pair->ops->has_fatal(pair->ctx, MOQ_ADAPTER_PAIR_SERVER));

    return failures;
}
