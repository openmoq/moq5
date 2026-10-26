/*
 * Track metadata and upstream retrieval through the production relay
 * binding over real sessions: draft-22 Track Properties reach every
 * subscriber as the origin stated them (also between draft-18 and draft-22
 * peers), replies the relay originates (TRACK_STATUS_OK, FETCH_OK) state
 * the track's properties, pushed PUBLISH metadata goes through the auth
 * gate, replies fit the downstream's send capacity, public-core publishers
 * share the core with the session binding, retrieval from a PUBLISH source
 * keeps its live frontier and its borrowed and refused bytes exact, and the
 * metadata a track carries is admitted and released within its budget in
 * plain and managed configurations. The properties modes run the parity,
 * mixed-draft and deferred-auth flows first as fixture controls.
 */
#include "relay_session_binding_harness.h"

/* Unknown nonmandatory Track Properties, including an Immutable Properties
 * container. Its inner type 0x7a/value 42 use nonminimal vi64 encodings; the
 * outer delta 0x6d then selects type 0x78/value 17. These are literal wire
 * expectations, independent of the encoder and the origin's mutable input. */
static const uint8_t d22_tp_expected[] = {
    0x0b, 0x04, 0x80, 0x7a, 0x80, 0x2a, 0x6d, 0x11
};
static const uint8_t d22_tp_object_properties[] = { 0x7c, 0x23 };
static const uint8_t d22_tp_payload[] = { 0x44, 0x00, 0xab, 0x19 };

typedef struct d22_tp_peer {
    bool origin;
    moq_subscription_t sub;
    moq_announcement_t ann;
    int setups, announcements, requests, acks, objects, unsubs;
    size_t properties_len;
    bool properties_match;
} d22_tp_peer_t;

/* Keep the narrow oracle separate from relay_bind_test_peer_drain's permissive event recorder:
 * a refused request, wrong identity or closed session is not a metadata
 * recovery failure. */
static void
d22_tp_drain(rig_t *r, conn_t *cn, d22_tp_peer_t *ps, bool with_properties,
             const char *label)
{
    moq_event_t ev;
    while (moq_session_poll_events(cn->peer, &ev, 1) != 0) {
        switch (ev.kind) {
        case MOQ_EVENT_SETUP_COMPLETE:
            ps->setups++;
            break;
        case MOQ_EVENT_REQUEST_READY:
            break;
        case MOQ_EVENT_NAMESPACE_ACCEPTED:
            R_CHECK(r, ps->origin &&
                           ev.u.namespace_accepted.ann._opaque == ps->ann._opaque);
            ps->announcements++;
            break;
        case MOQ_EVENT_SUBSCRIBE_REQUEST: {
            const moq_subscribe_request_event_t *q = &ev.u.subscribe_request;
            R_CHECK(r, ps->origin && ps->requests == 0 && q->sub._opaque != 0);
            R_CHECK(r, q->track_namespace.count == 2 &&
                           q->track_namespace.parts != NULL &&
                           q->track_namespace.parts[0].len == 5 &&
                           q->track_namespace.parts[0].data != NULL &&
                           memcmp(q->track_namespace.parts[0].data, "props", 5) == 0 &&
                           q->track_namespace.parts[1].len == 4 &&
                           q->track_namespace.parts[1].data != NULL &&
                           memcmp(q->track_namespace.parts[1].data, "live", 4) == 0);
            R_CHECK(r, q->track_name.len == 5 && q->track_name.data != NULL &&
                           memcmp(q->track_name.data, "video", 5) == 0);
            ps->sub = q->sub;
            ps->requests++;
            break;
        }
        case MOQ_EVENT_SUBSCRIBE_OK: {
            const moq_subscribe_ok_event_t *ok = &ev.u.subscribe_ok;
            R_CHECK(r, !ps->origin && ps->sub._opaque != 0 &&
                           ok->sub._opaque == ps->sub._opaque);
            R_CHECK(r, ok->has_largest && ok->largest_group == 0 &&
                           ok->largest_object == 0);
            ps->acks++;
            ps->properties_len = ok->track_properties.len;
            ps->properties_match = with_properties
                ? ok->track_properties.len == sizeof(d22_tp_expected) &&
                      ok->track_properties.data != NULL &&
                      memcmp(ok->track_properties.data, d22_tp_expected,
                             sizeof(d22_tp_expected)) == 0
                : ok->track_properties.len == 0;
            break;
        }
        case MOQ_EVENT_OBJECT_RECEIVED: {
            const moq_object_received_event_t *ob = &ev.u.object_received;
            R_CHECK(r, !ps->origin && ob->sub._opaque == ps->sub._opaque &&
                           ob->group_id == 1 && ob->object_id == 0 &&
                           ob->datagram && ob->publisher_priority == 90 &&
                           ob->status == MOQ_OBJECT_NORMAL);
            R_CHECK(r, ob->payload != NULL &&
                           moq_rcbuf_len(ob->payload) == sizeof(d22_tp_payload) &&
                           memcmp(moq_rcbuf_data(ob->payload), d22_tp_payload,
                                  sizeof(d22_tp_payload)) == 0);
            R_CHECK(r, ob->properties != NULL &&
                           moq_rcbuf_len(ob->properties) ==
                               sizeof(d22_tp_object_properties) &&
                           memcmp(moq_rcbuf_data(ob->properties),
                                  d22_tp_object_properties,
                                  sizeof(d22_tp_object_properties)) == 0);
            ps->objects++;
            break;
        }
        case MOQ_EVENT_UNSUBSCRIBED:
            R_CHECK(r, ps->origin &&
                           ev.u.unsubscribed.sub._opaque == ps->sub._opaque);
            ps->unsubs++;
            break;
        default:
            printf("FIXTURE: %s unexpected event %d\n", label, (int)ev.kind);
            r->failures++;
            break;
        }
        moq_event_cleanup(&ev);
    }
}

/* 0 = pass, 1 = metadata mismatch only, 2 = fixture/control failure. */
static int
track_properties_profiles(moq_version_t origin_version,
                           moq_version_t requester_version, bool with_properties)
{
    const char *label;
    if (origin_version == MOQ_VERSION_DRAFT_22 &&
        requester_version == MOQ_VERSION_DRAFT_22) {
        label = "draft22_track_properties";
    } else if (origin_version == MOQ_VERSION_DRAFT_18 &&
               requester_version == MOQ_VERSION_DRAFT_22) {
        label = "mixed_track_properties(origin18->requester22)";
    } else if (origin_version == MOQ_VERSION_DRAFT_22 &&
               requester_version == MOQ_VERSION_DRAFT_18) {
        label = "mixed_track_properties(origin22->requester18)";
    } else {
        printf("FIXTURE: track_properties_profiles unsupported draft pair\n");
        return 2;
    }
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    const char *row = with_properties ? "metadata" : "empty";
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        printf("FIXTURE: %s(%s) rig create\n", label, row);
        return 2;
    }
    conn_t *cn[3] = {0};   /* origin, first subscriber, later subscriber */
    d22_tp_peer_t ps[3] = {0};
    ps[0].origin = true;
    moq_rcbuf_t *payload = NULL;
#define TP_REQUIRE(expr)                                                      \
    do {                                                                      \
        if (!(expr)) {                                                        \
            printf("FIXTURE: %s(%s): %s\n", label, row, #expr);               \
            rig.failures++;                                                   \
            goto cleanup;                                                     \
        }                                                                     \
    } while (0)
    for (size_t i = 0; i < 3; i++) {
        cn[i] = relay_bind_test_rig_connect(&rig, i == 0 ? origin_version : requester_version);
        TP_REQUIRE(cn[i] != NULL);
    }
    relay_bind_test_rig_pump(&rig, 4);
    for (size_t i = 0; i < 3; i++) {
        d22_tp_drain(&rig, cn[i], &ps[i], with_properties, label);
        TP_REQUIRE(ps[i].setups == 1);
        TP_REQUIRE(moq_session_state(cn[i]->peer) == MOQ_SESS_ESTABLISHED &&
                   moq_session_state(cn[i]->rsess) == MOQ_SESS_ESTABLISHED);
        moq_version_t expected = i == 0 ? origin_version : requester_version;
        TP_REQUIRE(moq_session_version(cn[i]->peer) == expected &&
                   moq_session_version(cn[i]->rsess) == expected &&
                   moqr_bind_conn_version(rig.bind, cn[i]->rsess) == expected);
    }
    TP_REQUIRE(rig.failures == 0);

    moq_bytes_t ns[] = { B("props"), B("live") };
    moq_publish_namespace_cfg_t pcfg;
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = ns, .count = 2 };
    TP_REQUIRE(moq_session_publish_namespace(cn[0]->peer, &pcfg, rig.now,
                                             &ps[0].ann) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 4);
    d22_tp_drain(&rig, cn[0], &ps[0], with_properties, label);
    TP_REQUIRE(ps[0].announcements == 1 && rig.failures == 0);

    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = ns, .count = 2 };
    scfg.track_name = B("video");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    scfg.start_group = 0;
    scfg.start_object = 0;
    TP_REQUIRE(moq_session_subscribe(cn[1]->peer, &scfg, rig.now,
                                     &ps[1].sub) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 4);
    d22_tp_drain(&rig, cn[0], &ps[0], with_properties, label);
    d22_tp_drain(&rig, cn[1], &ps[1], with_properties, label);
    TP_REQUIRE(ps[0].requests == 1 && ps[1].acks == 0 && rig.failures == 0);

    uint8_t input_properties[] = {
        0x0b, 0x04, 0x80, 0x7a, 0x80, 0x2a, 0x6d, 0x11
    };
    moq_accept_subscribe_cfg_t acfg;
    moq_accept_subscribe_cfg_init(&acfg);
    acfg.has_largest = true;
    acfg.largest_group = 0;
    acfg.largest_object = 0;
    if (with_properties) {
        acfg.track_properties = (moq_bytes_t){
            .data = input_properties, .len = sizeof(input_properties)
        };
    }
    TP_REQUIRE(moq_session_accept_subscribe(cn[0]->peer, ps[0].sub,
                                            &acfg, rig.now) == MOQ_OK);
    memset(input_properties, 0xe6, sizeof(input_properties));

    /* Consume the origin's ACK but hold its downstream transport. A second,
     * unrelated namespace request on that same upstream session then reuses
     * event scratch. Its wire ACK is the barrier before observing either
     * downstream reply; no borrowed event pointer escapes this test's poll. */
    relay_bind_test_rig_pump_except(&rig, cn[1], 4);
    moq_bytes_t overwrite_ns[] = { B("scratch-reuse-after-track-properties") };
    pcfg.track_namespace = (moq_namespace_t){ .parts = overwrite_ns, .count = 1 };
    TP_REQUIRE(moq_session_publish_namespace(cn[0]->peer, &pcfg, rig.now,
                                             &ps[0].ann) == MOQ_OK);
    relay_bind_test_rig_pump_except(&rig, cn[1], 4);
    d22_tp_drain(&rig, cn[0], &ps[0], with_properties, label);
    d22_tp_drain(&rig, cn[1], &ps[1], with_properties, label);
    TP_REQUIRE(ps[0].announcements == 2 && ps[1].acks == 0 && rig.failures == 0);
    relay_bind_test_rig_pump(&rig, 4);
    d22_tp_drain(&rig, cn[1], &ps[1], with_properties, label);
    TP_REQUIRE(ps[1].acks == 1 && rig.failures == 0);

    /* First subscriber stays attached: the later ACK must come from the same
     * ACTIVE track, with no second upstream SUBSCRIBE to refresh metadata. */
    TP_REQUIRE(moq_session_subscribe(cn[2]->peer, &scfg, rig.now,
                                     &ps[2].sub) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 4);
    for (size_t i = 0; i < 3; i++) {
        d22_tp_drain(&rig, cn[i], &ps[i], with_properties, label);
    }
    TP_REQUIRE(ps[0].requests == 1 && ps[0].unsubs == 0 &&
               ps[1].acks == 1 && ps[2].acks == 1 && rig.failures == 0);

    const uint8_t body[] = { 0x44, 0x00, 0xab, 0x19 };
    const uint8_t object_properties[] = { 0x7c, 0x23 };
    TP_REQUIRE(moq_rcbuf_create(&a.vt, body, sizeof(body), &payload) == MOQ_OK);
    moq_result_t sent = moq_session_send_object_datagram(
        cn[0]->peer, ps[0].sub, 1, 0, 90, false, payload,
        object_properties, sizeof(object_properties), rig.now);
    moq_rcbuf_decref(payload);
    payload = NULL;
    TP_REQUIRE(sent == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 6);
    for (size_t i = 0; i < 3; i++) {
        d22_tp_drain(&rig, cn[i], &ps[i], with_properties, label);
    }
    TP_REQUIRE(ps[1].objects == 1 && ps[2].objects == 1 && rig.failures == 0);

    TP_REQUIRE(moq_session_unsubscribe(cn[1]->peer, ps[1].sub, rig.now) == MOQ_OK);
    TP_REQUIRE(moq_session_unsubscribe(cn[2]->peer, ps[2].sub, rig.now) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 6);
    for (size_t i = 0; i < 3; i++) {
        d22_tp_drain(&rig, cn[i], &ps[i], with_properties, label);
        TP_REQUIRE(ps[i].setups == 1);
        TP_REQUIRE(moq_session_state(cn[i]->peer) == MOQ_SESS_ESTABLISHED &&
                   moq_session_state(cn[i]->rsess) == MOQ_SESS_ESTABLISHED);
    }
    TP_REQUIRE(ps[0].requests == 1 && ps[0].unsubs == 1 &&
               ps[1].acks == 1 && ps[2].acks == 1 &&
               ps[1].objects == 1 && ps[2].objects == 1);
    moqr_bind_stats_t stats;
    moqr_bind_get_stats(rig.bind, &stats);
    TP_REQUIRE(stats.session_errors == 0 && stats.ingest_refusals == 0);

cleanup:
    moq_rcbuf_decref(payload);
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
#undef TP_REQUIRE
    if (rig.failures != 0) {
        printf("FIXTURE: %s(%s) failures=%d\n", label, row, rig.failures);
        return 2;
    }
    int metadata_failures = 0;
    for (size_t i = 1; i < 3; i++) {
        if (!ps[i].properties_match) {
            printf("FAIL: %s(%s) %s_ack_properties "
                   "expected_len=%zu actual_len=%zu\n", label, row,
                   i == 1 ? "initial" : "active",
                   with_properties ? sizeof(d22_tp_expected) : (size_t)0,
                   ps[i].properties_len);
            metadata_failures++;
        }
    }
    printf("%s: %s(%s) metadata_failures=%d fixture_failures=0\n",
           metadata_failures == 0 ? "PASS" : "FAIL", label, row, metadata_failures);
    return metadata_failures == 0 ? 0 : 1;
}

static int
draft22_track_properties(bool with_properties)
{
    return track_properties_profiles(MOQ_VERSION_DRAFT_22, MOQ_VERSION_DRAFT_22,
                                      with_properties);
}

/* Replies the relay originates state the track's properties: TRACK_STATUS_OK
 * and FETCH_OK (draft-22 §7.7 "A relay MUST include all Properties
 * associated with a Track when sending any PUBLISH, SUBSCRIBE_OK,
 * TRACK_STATUS_OK, or FETCH_OK"; draft-18 §9.6 and draft-16 §8.6 say the same).
 * The track is established by a subscriber on the origin's draft; a second
 * session then asks for the status and fetches the cached group. A set the
 * requester's draft cannot carry refuses both requests, as it refuses an
 * ACK. Draft-16's TRACK_STATUS answer has no Track Extensions field, so it
 * carries none and is never refused for them. INCLUDE_PROPERTIES = 0
 * (draft-22) empties both answers. */
static const uint8_t d16_tp_extensions[] = { 0x40, 0x78, 0x01 };
/* The same set as draft-18 and draft-22 write it: Type 120 = 1 in vi64. */
static const uint8_t d16_tp_as_vi64[] = { 0x78, 0x01 };

typedef struct answer_props_peer {
    int status_ok, status_err, fetch_ok, fetch_err, closed;
    uint64_t status_err_code, fetch_err_code;
    bool status_largest;
    uint64_t status_group, status_object;
    uint8_t status_props[16], fetch_props[16];
    size_t status_len, fetch_len;
    int sub_ok;
    uint8_t sub_props[16];
    size_t sub_len;
} answer_props_peer_t;

static void
answer_props_drain(rig_t *r, conn_t *cn, answer_props_peer_t *ps,
                   moq_subscription_t *origin_sub)
{
    (void)r;
    moq_event_t ev;
    while (moq_session_poll_events(cn->peer, &ev, 1) != 0) {
        switch (ev.kind) {
        case MOQ_EVENT_SUBSCRIBE_REQUEST:
            if (origin_sub != NULL) *origin_sub = ev.u.subscribe_request.sub;
            break;
        case MOQ_EVENT_TRACK_STATUS_OK: {
            const moq_track_status_ok_event_t *ok = &ev.u.track_status_ok;
            ps->status_ok++;
            ps->status_largest = ok->has_largest;
            ps->status_group = ok->largest_group;
            ps->status_object = ok->largest_object;
            ps->status_len = ok->track_properties.len;
            if (ps->status_len <= sizeof(ps->status_props) && ps->status_len != 0)
                memcpy(ps->status_props, ok->track_properties.data, ps->status_len);
            break;
        }
        case MOQ_EVENT_SUBSCRIBE_OK:
            ps->sub_ok++;
            ps->sub_len = ev.u.subscribe_ok.track_properties.len;
            if (ps->sub_len <= sizeof(ps->sub_props) && ps->sub_len != 0)
                memcpy(ps->sub_props, ev.u.subscribe_ok.track_properties.data, ps->sub_len);
            break;
        case MOQ_EVENT_TRACK_STATUS_ERROR:
            ps->status_err++;
            ps->status_err_code = ev.u.track_status_error.error_code;
            break;
        case MOQ_EVENT_FETCH_OK:
            ps->fetch_ok++;
            ps->fetch_len = ev.u.fetch_ok.track_properties.len;
            if (ps->fetch_len <= sizeof(ps->fetch_props) && ps->fetch_len != 0)
                memcpy(ps->fetch_props, ev.u.fetch_ok.track_properties.data,
                       ps->fetch_len);
            break;
        case MOQ_EVENT_FETCH_ERROR:
            ps->fetch_err++;
            ps->fetch_err_code = ev.u.fetch_error.error_code;
            break;
        case MOQ_EVENT_SESSION_CLOSED:
            ps->closed++;
            break;
        default:
            break;
        }
        moq_event_cleanup(&ev);
    }
}

/* AP_VI64_SET: a vi64 set without Immutable Properties (Type 120 = 1). */
typedef enum { AP_NONE, AP_D22_SET, AP_D16_SET, AP_VI64_SET } ap_props_t;

static int
relay_answer_properties(moq_version_t origin_version, moq_version_t requester_version,
                        ap_props_t kind, int include)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    char label[96];
    snprintf(label, sizeof(label), "relay_answer_properties(d%d->d%d,%s,include=%d)",
             (int)(origin_version & 0xff), (int)(requester_version & 0xff),
             kind == AP_NONE ? "none" : kind == AP_D22_SET ? "d22set"
                                     : kind == AP_D16_SET ? "d16set" : "vi64set",
             include);
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        printf("FIXTURE: %s rig create\n", label);
        return 1;
    }
    conn_t *origin = relay_bind_test_rig_connect(&rig, origin_version);
    conn_t *sub = relay_bind_test_rig_connect(&rig, origin_version);
    conn_t *req = relay_bind_test_rig_connect(&rig, requester_version);
    answer_props_peer_t po = {0}, psub = {0}, preq = {0};
    moq_subscription_t osub = MOQ_SUBSCRIPTION_INVALID;
    moq_rcbuf_t *payload = NULL;
#define AP_REQUIRE(expr)                                                      \
    do {                                                                      \
        if (!(expr)) {                                                        \
            printf("FIXTURE: %s: %s\n", label, #expr);                        \
            rig.failures++;                                                   \
            goto cleanup;                                                     \
        }                                                                     \
    } while (0)
    AP_REQUIRE(origin != NULL && sub != NULL && req != NULL);
    relay_bind_test_rig_pump(&rig, 4);
    answer_props_drain(&rig, origin, &po, NULL);
    answer_props_drain(&rig, sub, &psub, NULL);
    answer_props_drain(&rig, req, &preq, NULL);

    moq_bytes_t ns[] = { B("props"), B("live") };
    moq_publish_namespace_cfg_t pcfg;
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = ns, .count = 2 };
    moq_announcement_t ann;
    AP_REQUIRE(moq_session_publish_namespace(origin->peer, &pcfg, rig.now, &ann) ==
               MOQ_OK);
    relay_bind_test_rig_pump(&rig, 4);
    answer_props_drain(&rig, origin, &po, NULL);

    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = ns, .count = 2 };
    scfg.track_name = B("video");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    moq_subscription_t dsub;
    AP_REQUIRE(moq_session_subscribe(sub->peer, &scfg, rig.now, &dsub) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 4);
    answer_props_drain(&rig, origin, &po, &osub);
    AP_REQUIRE(osub._opaque != 0);
    moq_accept_subscribe_cfg_t acfg;
    moq_accept_subscribe_cfg_init(&acfg);
    acfg.has_largest = true;
    if (kind == AP_D22_SET)
        acfg.track_properties = (moq_bytes_t){ d22_tp_expected, sizeof(d22_tp_expected) };
    else if (kind == AP_D16_SET)
        acfg.track_properties = (moq_bytes_t){ d16_tp_extensions,
                                               sizeof(d16_tp_extensions) };
    else if (kind == AP_VI64_SET)
        acfg.track_properties = (moq_bytes_t){ d16_tp_as_vi64, sizeof(d16_tp_as_vi64) };
    AP_REQUIRE(moq_session_accept_subscribe(origin->peer, osub, &acfg, rig.now) ==
               MOQ_OK);
    relay_bind_test_rig_pump(&rig, 6);
    answer_props_drain(&rig, sub, &psub, NULL);

    /* One object in group 1, cached by the relay. */
    const uint8_t body[] = { 0x44, 0x00, 0xab, 0x19 };
    AP_REQUIRE(moq_rcbuf_create(&a.vt, body, sizeof(body), &payload) == MOQ_OK);
    moq_subgroup_cfg_t sg;
    moq_subgroup_cfg_init(&sg);
    sg.group_id = 1;
    sg.subgroup_id = 0;
    sg.publisher_priority = 90;
    moq_subgroup_handle_t sgh;
    AP_REQUIRE(moq_session_open_subgroup(origin->peer, osub, &sg, rig.now, &sgh) ==
               MOQ_OK);
    AP_REQUIRE(moq_session_write_object(origin->peer, sgh, 0, payload, rig.now) ==
               MOQ_OK);
    AP_REQUIRE(moq_session_close_subgroup(origin->peer, sgh, rig.now) == MOQ_OK);
    moq_rcbuf_decref(payload);
    payload = NULL;
    relay_bind_test_rig_pump(&rig, 6);
    answer_props_drain(&rig, sub, &psub, NULL);
    answer_props_drain(&rig, origin, &po, NULL);

    moq_track_status_cfg_t tcfg;
    moq_track_status_cfg_init(&tcfg);
    tcfg.track_namespace = (moq_namespace_t){ .parts = ns, .count = 2 };
    tcfg.track_name = B("video");
    if (include >= 0) {
        tcfg.has_include_properties = true;
        tcfg.include_properties = include != 0;
    }
    moq_track_status_handle_t th;
    AP_REQUIRE(moq_session_track_status(req->peer, &tcfg, rig.now, &th) == MOQ_OK);
    moq_fetch_cfg_t fc;
    moq_fetch_cfg_init_sized(&fc, sizeof(fc));
    fc.track_namespace = (moq_namespace_t){ .parts = ns, .count = 2 };
    fc.track_name = B("video");
    fc.start_group = 1;
    fc.start_object = 0;
    fc.end_group = 1;
    fc.end_object = 1;
    if (include >= 0) {
        fc.has_include_properties = true;
        fc.include_properties = include != 0;
    }
    moq_fetch_t fh;
    AP_REQUIRE(moq_session_fetch(req->peer, &fc, rig.now, &fh) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 8);
    answer_props_drain(&rig, req, &preq, NULL);
    AP_REQUIRE(preq.closed == 0 && preq.status_ok + preq.status_err == 1 &&
               preq.fetch_ok + preq.fetch_err == 1);

    const uint8_t *want = NULL;
    size_t want_len = 0;
    bool replayable = true;
    if (kind == AP_D22_SET) {
        /* Its Immutable Properties read differently as draft-16 integers,
         * and are never rewritten: not representable there. */
        want = d22_tp_expected;
        want_len = sizeof(d22_tp_expected);
        replayable = requester_version != MOQ_VERSION_DRAFT_16;
    } else if (kind == AP_D16_SET) {
        /* Carried to draft-18/22 in their integer encoding. */
        bool same = requester_version == MOQ_VERSION_DRAFT_16;
        want = same ? d16_tp_extensions : d16_tp_as_vi64;
        want_len = same ? sizeof(d16_tp_extensions) : sizeof(d16_tp_as_vi64);
    } else if (kind == AP_VI64_SET) {
        /* Carried to draft-16 in QUIC varints. */
        bool to16 = requester_version == MOQ_VERSION_DRAFT_16;
        want = to16 ? d16_tp_extensions : d16_tp_as_vi64;
        want_len = to16 ? sizeof(d16_tp_extensions) : sizeof(d16_tp_as_vi64);
    }
    if (include == 0) want_len = 0;
    if (!replayable) {
        /* Draft-16's TRACK_STATUS answer has no Track Properties field, so it
         * is answered whatever the set; its FETCH_OK has one. */
        if (requester_version == MOQ_VERSION_DRAFT_16) {
            R_CHECK(&rig, preq.status_ok == 1 && preq.status_len == 0);
        } else {
            R_CHECK(&rig, preq.status_err == 1 &&
                              preq.status_err_code == MOQ_REQUEST_ERROR_INTERNAL_ERROR);
        }
        R_CHECK(&rig, preq.fetch_err == 1 &&
                          preq.fetch_err_code == MOQ_REQUEST_ERROR_INTERNAL_ERROR);
    } else {
        size_t status_len = requester_version == MOQ_VERSION_DRAFT_16 ? 0 : want_len;
        R_CHECK(&rig, preq.status_ok == 1 && preq.status_len == status_len &&
                          (status_len == 0 ||
                           memcmp(preq.status_props, want, status_len) == 0));
        R_CHECK(&rig, preq.status_largest && preq.status_group == 1 &&
                          preq.status_object == 0);
        R_CHECK(&rig, preq.fetch_ok == 1 && preq.fetch_len == want_len &&
                          (want_len == 0 || memcmp(preq.fetch_props, want, want_len) == 0));
    }
    moqr_bind_stats_t stats;
    moqr_bind_get_stats(rig.bind, &stats);
    R_CHECK(&rig, stats.session_errors == 0);

cleanup:
    moq_rcbuf_decref(payload);
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
#undef AP_REQUIRE
    printf("%s: %s\n", rig.failures == 0 ? "PASS" : "FAIL", label);
    return rig.failures;
}

static int
relay_answer_properties_all(void)
{
    int f = 0;
    const moq_version_t v16 = MOQ_VERSION_DRAFT_16, v18 = MOQ_VERSION_DRAFT_18,
                        v22 = MOQ_VERSION_DRAFT_22;
    f += relay_answer_properties(v22, v22, AP_D22_SET, -1);
    f += relay_answer_properties(v22, v22, AP_D22_SET, 1);
    f += relay_answer_properties(v22, v22, AP_D22_SET, 0);
    f += relay_answer_properties(v22, v22, AP_NONE, -1);
    f += relay_answer_properties(v18, v18, AP_D22_SET, -1);
    f += relay_answer_properties(v18, v22, AP_D22_SET, -1);
    f += relay_answer_properties(v22, v18, AP_D22_SET, -1);
    f += relay_answer_properties(v16, v16, AP_D16_SET, -1);
    f += relay_answer_properties(v16, v16, AP_NONE, -1);
    f += relay_answer_properties(v16, v18, AP_D16_SET, -1);
    f += relay_answer_properties(v16, v22, AP_D16_SET, -1);
    f += relay_answer_properties(v18, v16, AP_VI64_SET, -1);
    f += relay_answer_properties(v22, v16, AP_VI64_SET, -1);
    f += relay_answer_properties(v18, v22, AP_VI64_SET, -1);
    f += relay_answer_properties(v18, v16, AP_D22_SET, -1);
    f += relay_answer_properties(v16, v22, AP_NONE, -1);
    return f;
}

/* A draft-18 SUBSCRIBE_OK that cannot be written at once states the Track
 * Properties revision its answer pinned, converted from the draft-16 set
 * the origin stated, even after a newer revision replaces the track's
 * latest: the conversion reads the pin, never the directory. The answer
 * waits in the binding's one ordered queue behind NAMESPACE updates for a
 * namespace subscriber whose transport is held; a SUBSCRIBE answered after
 * the replacement states the newer revision, converted the same way. */
static const uint8_t pinned_newer_d16[] = { 0x40, 0x7a, 0x02 };   /* Type 122 = 2 */
static moqr_result_t metadata_lifetime_track(rig_t *r, moqr_binding_t observer, moqr_ns_t ns,
                                             moq_bytes_t name, moqr_track_t *track,
                                             uint64_t *gen);

static int
relay_answer_pinned_converted(void)
{
    const char *label = "relay_answer_pinned_converted(d16->d18)";
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        printf("FIXTURE: %s rig create\n", label);
        return 1;
    }
    enum { NS_FILL = 6 };
    conn_t *origin = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_16);
    conn_t *sub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_16);
    conn_t *req = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *late = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *ann2 = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    relay_bind_test_max_actions = 3;
    conn_t *nsx = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    relay_bind_test_max_actions = 0;
    answer_props_peer_t po = {0}, psub = {0}, preq = {0}, plate = {0}, pjunk = {0};
    moq_subscription_t osub = MOQ_SUBSCRIPTION_INVALID;
    moqr_bind_meta_ref_t latest = {0};
    moqr_binding_t observer = MOQR_BINDING_INVALID;
    bool replaced = false;
#define PC_REQUIRE(expr)                                                      \
    do {                                                                      \
        if (!(expr)) {                                                        \
            printf("FIXTURE: %s: %s\n", label, #expr);                        \
            rig.failures++;                                                   \
            goto cleanup;                                                     \
        }                                                                     \
    } while (0)
    PC_REQUIRE(origin != NULL && sub != NULL && req != NULL && late != NULL &&
               ann2 != NULL && nsx != NULL);
    relay_bind_test_rig_pump(&rig, 4);
    answer_props_drain(&rig, origin, &po, NULL);
    answer_props_drain(&rig, sub, &psub, NULL);
    answer_props_drain(&rig, req, &preq, NULL);
    answer_props_drain(&rig, late, &plate, NULL);
    answer_props_drain(&rig, ann2, &pjunk, NULL);
    answer_props_drain(&rig, nsx, &pjunk, NULL);

    moq_bytes_t ns[] = { B("props"), B("pinned") };
    moq_publish_namespace_cfg_t pcfg;
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = ns, .count = 2 };
    moq_announcement_t ann;
    PC_REQUIRE(moq_session_publish_namespace(origin->peer, &pcfg, rig.now, &ann) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 4);
    answer_props_drain(&rig, origin, &po, NULL);

    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = ns, .count = 2 };
    scfg.track_name = B("video");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    moq_subscription_t dsub;
    PC_REQUIRE(moq_session_subscribe(sub->peer, &scfg, rig.now, &dsub) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 4);
    answer_props_drain(&rig, origin, &po, &osub);
    PC_REQUIRE(osub._opaque != 0);
    moq_accept_subscribe_cfg_t acfg;
    moq_accept_subscribe_cfg_init(&acfg);
    acfg.track_properties = (moq_bytes_t){ d16_tp_extensions, sizeof(d16_tp_extensions) };
    PC_REQUIRE(moq_session_accept_subscribe(origin->peer, osub, &acfg, rig.now) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 6);
    answer_props_drain(&rig, sub, &psub, NULL);
    PC_REQUIRE(psub.sub_ok == 1);

    /* A namespace subscriber with a three-action queue, then held: the
     * NAMESPACE updates for it fill its queue and the rest park in order. */
    moq_bytes_t xp[] = { B("disc") };
    moq_subscribe_namespace_cfg_t nscfg;
    moq_subscribe_namespace_cfg_init(&nscfg);
    nscfg.track_namespace_prefix = (moq_namespace_t){ .parts = xp, .count = 1 };
    nscfg.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
    moq_ns_sub_handle_t nsh;
    moq_result_t nrc = moq_session_subscribe_namespace(nsx->peer, &nscfg, rig.now, &nsh);
    PC_REQUIRE(nrc == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 6);
    answer_props_drain(&rig, nsx, &pjunk, NULL);
    nsx->frozen = true;
    char nsname[NS_FILL][8];
    for (int i = 0; i < NS_FILL; i++) {
        snprintf(nsname[i], sizeof(nsname[i]), "n%d", i);
        moq_bytes_t np[] = { B("disc"), B(nsname[i]) };
        moq_publish_namespace_cfg_t c2;
        moq_publish_namespace_cfg_init(&c2);
        c2.track_namespace = (moq_namespace_t){ .parts = np, .count = 2 };
        moq_announcement_t a2;
        PC_REQUIRE(moq_session_publish_namespace(ann2->peer, &c2, rig.now, &a2) == MOQ_OK);
        relay_bind_test_rig_pump(&rig, 2);
        answer_props_drain(&rig, ann2, &pjunk, NULL);
    }
    moqr_bind_stats_t stats;
    moq_subscription_t rsub;
    PC_REQUIRE(moq_session_subscribe(req->peer, &scfg, rig.now, &rsub) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 4);
    answer_props_drain(&rig, req, &preq, NULL);
    PC_REQUIRE(preq.sub_ok == 0);   /* parked behind the held updates */

    /* A newer revision replaces the track's latest while the answer waits. */
    PC_REQUIRE(moqr_core_binding_open(rig.core, 901, &observer) == MOQR_OK);
    moqr_track_t track = MOQR_TRACK_INVALID;
    uint64_t gen = 0;
    PC_REQUIRE(metadata_lifetime_track(&rig, observer, (moqr_ns_t){ .parts = ns, .count = 2 },
                                       B("video"), &track, &gen) == MOQR_OK);
    PC_REQUIRE(moqr_bind_meta_latest(rig.bind, track, &latest) == MOQR_OK);
    PC_REQUIRE(latest.state == MOQR_BIND_META_PRESENT &&
               latest.wire_version == MOQ_VERSION_DRAFT_16);
    moqr_bind_meta_ref_t newer = latest;
    newer.bytes = (moq_bytes_t){ pinned_newer_d16, sizeof(pinned_newer_d16) };
    uint64_t revision = 0;
    PC_REQUIRE(moqr_bind_meta_replace(rig.bind, track, gen, &newer, rig.now, &revision) ==
               MOQR_OK && revision > latest.revision);
    replaced = true;
    moqr_bind_meta_release(rig.bind, &latest);

    nsx->frozen = false;
    for (int i = 0; i < 32 && preq.sub_ok == 0; i++) {
        relay_bind_test_rig_pump(&rig, 1);
        answer_props_drain(&rig, req, &preq, NULL);
        answer_props_drain(&rig, nsx, &pjunk, NULL);
    }
    PC_REQUIRE(moq_session_subscribe(late->peer, &scfg, rig.now, &rsub) == MOQ_OK);
    for (int i = 0; i < 16 && plate.sub_ok == 0; i++) {
        relay_bind_test_rig_pump(&rig, 1);
        answer_props_drain(&rig, late, &plate, NULL);
    }
    moqr_bind_get_stats(rig.bind, &stats);
    printf("%s: pinned sub_ok=%d len=%zu %02x %02x | late sub_ok=%d len=%zu %02x %02x | "
           "session_errors=%llu\n",
           label, preq.sub_ok, preq.sub_len, preq.sub_props[0], preq.sub_props[1],
           plate.sub_ok, plate.sub_len, plate.sub_props[0], plate.sub_props[1],
           (unsigned long long)stats.session_errors);
    static const uint8_t newer_as_vi64[] = { 0x7a, 0x02 };
    R_CHECK(&rig, preq.closed == 0 && preq.sub_ok == 1 &&
                      preq.sub_len == sizeof(d16_tp_as_vi64) &&
                      memcmp(preq.sub_props, d16_tp_as_vi64, sizeof(d16_tp_as_vi64)) == 0);
    R_CHECK(&rig, plate.closed == 0 && plate.sub_ok == 1 &&
                      plate.sub_len == sizeof(newer_as_vi64) &&
                      memcmp(plate.sub_props, newer_as_vi64, sizeof(newer_as_vi64)) == 0);
    R_CHECK(&rig, stats.session_errors == 0);

cleanup:
    (void)replaced;
    moqr_bind_meta_release(rig.bind, &latest);
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
#undef PC_REQUIRE
    printf("%s: %s\n", rig.failures == 0 ? "PASS" : "FAIL", label);
    return rig.failures;
}

/* PUBLISH metadata uses the existing auth gate/ticket path, with identities
 * observed at the public callback rather than assumed from binding slots. */
typedef struct d22_push_metadata_auth {
    rig_t *rig;
    uint64_t setup_cookies[2];
    uint64_t ticket;
    int setups, publishes, subscribes, announcements;
} d22_push_metadata_auth_t;

static void
d22_push_metadata_auth(void *ctx, const moqr_auth_request_t *req,
                       moqr_auth_verdict_t *out)
{
    d22_push_metadata_auth_t *a = ctx;
    relay_bind_test_defer_hook(NULL, req, out);
    if (req->action == MOQR_AUTH_CLIENT_SETUP) {
        R_CHECK(a->rig, a->setups < 2 && req->binding_cookie != 0);
        if (a->setups < 2) {
            a->setup_cookies[a->setups] = req->binding_cookie;
        }
        a->setups++;
    } else if (req->action == MOQR_AUTH_PUBLISH ||
               req->action == MOQR_AUTH_SUBSCRIBE) {
        size_t peer = req->action == MOQR_AUTH_PUBLISH ? 0u : 1u;
        R_CHECK(a->rig, a->setups == 2 &&
                           req->binding_cookie == a->setup_cookies[peer]);
        R_CHECK(a->rig, req->ns.count == 2 && req->ns.parts != NULL &&
                           req->ns.parts[0].len == 5 && req->ns.parts[0].data != NULL &&
                           memcmp(req->ns.parts[0].data, "props", 5) == 0 &&
                           req->ns.parts[1].len == 4 && req->ns.parts[1].data != NULL &&
                           memcmp(req->ns.parts[1].data, "live", 4) == 0 &&
                           req->name.len == 5 && req->name.data != NULL &&
                           memcmp(req->name.data, "video", 5) == 0);
        if (req->action == MOQR_AUTH_PUBLISH) {
            a->publishes++;
            a->ticket = out->decision == MOQR_AUTH_DEFER ? out->ticket : 0;
        } else {
            a->subscribes++;
        }
    } else if (req->action == MOQR_AUTH_PUBLISH_NAMESPACE) {
        R_CHECK(a->rig, req->binding_cookie == a->setup_cookies[0]);
        a->announcements++;
    } else {
        R_CHECK(a->rig, false);
    }
}

typedef struct d22_push_metadata_peer {
    bool origin;
    moq_publication_t pub;
    moq_announcement_t ann;
    moq_subscription_t sub;
    int setups, publications, announcements, acks, objects;
    size_t properties_len;
    bool properties_match;
} d22_push_metadata_peer_t;

static void
d22_push_metadata_drain(rig_t *r, conn_t *cn, d22_push_metadata_peer_t *ps,
                        bool with_properties)
{
    moq_event_t ev;
    while (moq_session_poll_events(cn->peer, &ev, 1) != 0) {
        switch (ev.kind) {
        case MOQ_EVENT_SETUP_COMPLETE:
            ps->setups++;
            break;
        case MOQ_EVENT_REQUEST_READY:
            break;
        case MOQ_EVENT_PUBLISH_OK:
            R_CHECK(r, ps->origin && ps->pub._opaque != 0 &&
                           ev.u.publish_ok.pub._opaque == ps->pub._opaque &&
                           ev.u.publish_ok.send_allowed);
            ps->publications++;
            break;
        case MOQ_EVENT_NAMESPACE_ACCEPTED:
            R_CHECK(r, ps->origin &&
                           ev.u.namespace_accepted.ann._opaque == ps->ann._opaque);
            ps->announcements++;
            break;
        case MOQ_EVENT_SUBSCRIBE_OK:
            R_CHECK(r, !ps->origin && ps->sub._opaque != 0 &&
                           ev.u.subscribe_ok.sub._opaque == ps->sub._opaque);
            ps->acks++;
            ps->properties_len = ev.u.subscribe_ok.track_properties.len;
            ps->properties_match = with_properties
                ? ps->properties_len == sizeof(d22_tp_expected) &&
                      ev.u.subscribe_ok.track_properties.data != NULL &&
                      memcmp(ev.u.subscribe_ok.track_properties.data,
                             d22_tp_expected, sizeof(d22_tp_expected)) == 0
                : ps->properties_len == 0;
            break;
        case MOQ_EVENT_OBJECT_RECEIVED: {
            const moq_object_received_event_t *ob = &ev.u.object_received;
            R_CHECK(r, !ps->origin && ob->sub._opaque == ps->sub._opaque &&
                           ob->group_id == 1 && ob->object_id == 0 &&
                           ob->datagram && ob->publisher_priority == 90 &&
                           ob->status == MOQ_OBJECT_NORMAL);
            R_CHECK(r, ob->payload != NULL &&
                           moq_rcbuf_len(ob->payload) == sizeof(d22_tp_payload) &&
                           memcmp(moq_rcbuf_data(ob->payload), d22_tp_payload,
                                  sizeof(d22_tp_payload)) == 0);
            R_CHECK(r, ob->properties != NULL &&
                           moq_rcbuf_len(ob->properties) ==
                               sizeof(d22_tp_object_properties) &&
                           memcmp(moq_rcbuf_data(ob->properties),
                                  d22_tp_object_properties,
                                  sizeof(d22_tp_object_properties)) == 0);
            ps->objects++;
            break;
        }
        default:
            /* Includes an unexpected pull SUBSCRIBE, any refusal or terminal. */
            printf("FIXTURE: draft22_push_track_properties unexpected event %d\n",
                   (int)ev.kind);
            r->failures++;
            break;
        }
        moq_event_cleanup(&ev);
    }
}

/* 0 = pass, 1 = metadata mismatch only, 2 = fixture/control failure. */
static int
draft22_push_track_properties(bool deferred, bool with_properties)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    d22_push_metadata_auth_t auth = {0};
    auth.rig = &rig;
    relay_bind_test_authorize = d22_push_metadata_auth;
    relay_bind_test_authorize_ctx = &auth;
    relay_bind_test_defer_action = deferred ? MOQR_AUTH_PUBLISH : 0xFFFFFFFFu;
    relay_bind_test_defer_fixed = 0;
    relay_bind_test_defer_last = 0;
    const char *mode = deferred ? "deferred" : "immediate";
    const char *row = with_properties ? "metadata" : "empty";
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        relay_bind_test_authorize = NULL;
        relay_bind_test_authorize_ctx = NULL;
        relay_bind_test_defer_action = 0xFFFFFFFFu;
        printf("FIXTURE: draft22_push_track_properties(%s/%s) rig create\n", mode, row);
        return 2;
    }
    conn_t *cn[2] = {0};
    d22_push_metadata_peer_t ps[2] = {0};
    ps[0].origin = true;
    moq_rcbuf_t *payload = NULL;
#define PT_REQUIRE(expr)                                                       \
    do {                                                                      \
        if (!(expr)) {                                                        \
            printf("FIXTURE: draft22_push_track_properties(%s/%s): %s\n",     \
                   mode, row, #expr);                                         \
            rig.failures++;                                                   \
            goto cleanup;                                                     \
        }                                                                     \
    } while (0)
    /* Complete each setup separately so the callback's two cookies identify
     * the actual origin and downstream connections without slot assumptions. */
    for (size_t i = 0; i < 2; i++) {
        cn[i] = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_22);
        PT_REQUIRE(cn[i] != NULL);
        relay_bind_test_rig_pump(&rig, 4);
        d22_push_metadata_drain(&rig, cn[i], &ps[i], with_properties);
        PT_REQUIRE(ps[i].setups == 1 && auth.setups == (int)i + 1 && rig.failures == 0);
    }
    PT_REQUIRE(auth.setup_cookies[0] != auth.setup_cookies[1]);
    moqr_core_stats_t before, parked, accepted;
    moqr_core_get_stats(rig.core, &before);
    PT_REQUIRE(before.tracks == 0 && before.subs == 0);

    moq_bytes_t ns[] = { B("props"), B("live") };
    uint8_t input_properties[] = {
        0x0b, 0x04, 0x80, 0x7a, 0x80, 0x2a, 0x6d, 0x11
    };
    moq_publish_cfg_t pcfg;
    moq_publish_cfg_init_sized(&pcfg, sizeof(pcfg));
    pcfg.track_namespace = (moq_namespace_t){ .parts = ns, .count = 2 };
    pcfg.track_name = B("video");
    if (with_properties) {
        pcfg.track_properties = (moq_bytes_t){
            .data = input_properties, .len = sizeof(input_properties)
        };
    }
    PT_REQUIRE(moq_session_publish(cn[0]->peer, &pcfg, rig.now, &ps[0].pub) == MOQ_OK);
    memset(input_properties, 0xe6, sizeof(input_properties));
    relay_bind_test_rig_pump(&rig, 4);
    PT_REQUIRE(auth.publishes == 1 && auth.subscribes == 0 && rig.failures == 0);
    moqr_core_get_stats(rig.core, &parked);
    PT_REQUIRE(parked.tracks == (deferred ? 0u : 1u) && parked.subs == 0);
    if (deferred) {
        PT_REQUIRE(auth.ticket != 0 && auth.ticket == relay_bind_test_defer_last);
        PT_REQUIRE(parked.auth_decisions[MOQR_AUTH_PUBLISH][MOQR_AUTH_DEFER] == 1);
    } else {
        PT_REQUIRE(auth.ticket == 0 && relay_bind_test_defer_last == 0);
    }

    /* A real variable-length request overwrites the relay-side origin event
     * scratch before ticket resolution or observation of either reply. No
     * announcement for props/live exists to conceal a failed push as a pull. */
    moq_bytes_t overwrite_ns[] = { B("scratch-reuse-after-push-properties") };
    moq_publish_namespace_cfg_t ncfg;
    moq_publish_namespace_cfg_init(&ncfg);
    ncfg.track_namespace = (moq_namespace_t){ .parts = overwrite_ns, .count = 1 };
    PT_REQUIRE(moq_session_publish_namespace(cn[0]->peer, &ncfg, rig.now,
                                             &ps[0].ann) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 4);
    d22_push_metadata_drain(&rig, cn[0], &ps[0], with_properties);
    PT_REQUIRE(auth.announcements == 1 && ps[0].announcements == 1 &&
               ps[0].publications == (deferred ? 0 : 1) && rig.failures == 0);
    if (deferred) {
        moqr_core_get_stats(rig.core, &parked);
        PT_REQUIRE(parked.tracks == 0 && parked.subs == 0 && auth.ticket == relay_bind_test_defer_last);
        moqr_auth_verdict_t allow = { .decision = MOQR_AUTH_ALLOW };
        PT_REQUIRE(moqr_bind_auth_resolve(rig.bind, auth.ticket, &allow, rig.now) == MOQR_OK);
        relay_bind_test_rig_pump(&rig, 4);
        d22_push_metadata_drain(&rig, cn[0], &ps[0], with_properties);
    }
    moqr_core_get_stats(rig.core, &accepted);
    PT_REQUIRE(ps[0].publications == 1 && auth.publishes == 1 &&
               accepted.tracks == 1 && accepted.subs == 0 && rig.failures == 0);

    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = ns, .count = 2 };
    scfg.track_name = B("video");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    scfg.start_group = 0;
    scfg.start_object = 0;
    PT_REQUIRE(moq_session_subscribe(cn[1]->peer, &scfg, rig.now, &ps[1].sub) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 4);
    for (size_t i = 0; i < 2; i++) {
        d22_push_metadata_drain(&rig, cn[i], &ps[i], with_properties);
    }
    PT_REQUIRE(auth.subscribes == 1 && ps[1].acks == 1 && rig.failures == 0);

    const uint8_t body[] = { 0x44, 0x00, 0xab, 0x19 };
    const uint8_t object_properties[] = { 0x7c, 0x23 };
    PT_REQUIRE(moq_rcbuf_create(&a.vt, body, sizeof(body), &payload) == MOQ_OK);
    moq_result_t sent = moq_session_send_pub_object_datagram(
        cn[0]->peer, ps[0].pub, 1, 0, 90, false, payload,
        object_properties, sizeof(object_properties), rig.now);
    moq_rcbuf_decref(payload);
    payload = NULL;
    PT_REQUIRE(sent == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 6);
    for (size_t i = 0; i < 2; i++) {
        d22_push_metadata_drain(&rig, cn[i], &ps[i], with_properties);
    }
    PT_REQUIRE(ps[1].objects == 1 && rig.failures == 0);

    PT_REQUIRE(moq_session_unsubscribe(cn[1]->peer, ps[1].sub, rig.now) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 6);
    moq_finish_publish_cfg_t finish;
    moq_finish_publish_cfg_init(&finish);
    PT_REQUIRE(moq_session_finish_publish(cn[0]->peer, ps[0].pub, &finish, rig.now) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 6);
    for (size_t i = 0; i < 2; i++) {
        d22_push_metadata_drain(&rig, cn[i], &ps[i], with_properties);
        PT_REQUIRE(ps[i].setups == 1 &&
                   moq_session_state(cn[i]->peer) == MOQ_SESS_ESTABLISHED &&
                   moq_session_state(cn[i]->rsess) == MOQ_SESS_ESTABLISHED);
    }
    moqr_bind_stats_t stats;
    moqr_bind_get_stats(rig.bind, &stats);
    moqr_core_get_stats(rig.core, &accepted);
    PT_REQUIRE(ps[0].publications == 1 && ps[1].acks == 1 && ps[1].objects == 1 &&
               auth.publishes == 1 && auth.subscribes == 1 && accepted.subs == 0 &&
               stats.session_errors == 0 && stats.ingest_refusals == 0);

cleanup:
    moq_rcbuf_decref(payload);
    relay_bind_test_rig_destroy(&rig);
    relay_bind_test_authorize = NULL;
    relay_bind_test_authorize_ctx = NULL;
    relay_bind_test_defer_action = 0xFFFFFFFFu;
    relay_bind_test_defer_fixed = 0;
    relay_bind_test_defer_last = 0;
    R_CHECK(&rig, a.live == 0);
#undef PT_REQUIRE
    if (rig.failures != 0) {
        printf("FIXTURE: draft22_push_track_properties(%s/%s) failures=%d\n",
               mode, row, rig.failures);
        return 2;
    }
    if (!ps[1].properties_match) {
        printf("FAIL: draft22_push_track_properties(%s/%s) subscribe_ack_properties "
               "expected_len=%zu actual_len=%zu\n", mode, row,
               with_properties ? sizeof(d22_tp_expected) : (size_t)0,
               ps[1].properties_len);
    }
    printf("%s: draft22_push_track_properties(%s/%s) metadata_failures=%d fixture_failures=0\n",
           ps[1].properties_match ? "PASS" : "FAIL", mode, row,
           ps[1].properties_match ? 0 : 1);
    return ps[1].properties_match ? 0 : 1;
}

/* The existing SimPair has a fixed default send arena. Real queued
 * namespace output makes the downstream's AVAILABLE reply span smaller than
 * the origin's, without changing a session field or introducing a test seam. */
typedef struct tp_capacity_peer {
    bool origin;
    unsigned track;
    moq_subscription_t sub[2];   /* target, independent healthy track */
    moq_announcement_t ann;
    moq_announcement_t filler_ann[3];
    moq_bytes_t properties;
    const uint8_t *filler;
    int setups, announcements, requests[2], unsubs[2];
    int acks, errors, objects, fillers, filler_done;
} tp_capacity_peer_t;

static const char *const tp_capacity_names[2] = { "target", "healthy" };

static void
tp_capacity_drain(rig_t *r, conn_t *cn, tp_capacity_peer_t *p)
{
    moq_event_t ev;
    while (moq_session_poll_events(cn->peer, &ev, 1) != 0) {
        switch (ev.kind) {
        case MOQ_EVENT_SETUP_COMPLETE:
            p->setups++;
            break;
        case MOQ_EVENT_REQUEST_READY:
            break;
        case MOQ_EVENT_NAMESPACE_ACCEPTED:
            R_CHECK(r, p->origin &&
                           ev.u.namespace_accepted.ann._opaque == p->ann._opaque);
            p->announcements++;
            break;
        case MOQ_EVENT_SUBSCRIBE_REQUEST: {
            const moq_subscribe_request_event_t *q = &ev.u.subscribe_request;
            R_CHECK(r, p->origin && q->sub._opaque != 0);
            R_CHECK(r, q->track_namespace.count == 2 &&
                           q->track_namespace.parts != NULL &&
                           q->track_namespace.parts[0].len == 8 &&
                           q->track_namespace.parts[0].data != NULL &&
                           memcmp(q->track_namespace.parts[0].data, "capacity", 8) == 0 &&
                           q->track_namespace.parts[1].len == 8 &&
                           q->track_namespace.parts[1].data != NULL &&
                           memcmp(q->track_namespace.parts[1].data, "metadata", 8) == 0);
            unsigned t = q->track_name.len == 6 ? 0u : 1u;
            R_CHECK(r, q->track_name.len == strlen(tp_capacity_names[t]) &&
                           q->track_name.data != NULL &&
                           memcmp(q->track_name.data, tp_capacity_names[t],
                                  q->track_name.len) == 0 && p->requests[t] == 0);
            p->sub[t] = q->sub;
            p->requests[t]++;
            break;
        }
        case MOQ_EVENT_SUBSCRIBE_OK: {
            const moq_subscribe_ok_event_t *ok = &ev.u.subscribe_ok;
            R_CHECK(r, !p->origin &&
                           ok->sub._opaque == p->sub[p->track]._opaque &&
                           ok->has_largest && ok->largest_group == 0 &&
                           ok->largest_object == 0);
            R_CHECK(r, ok->track_properties.len == p->properties.len &&
                           ok->track_properties.data != NULL &&
                           memcmp(ok->track_properties.data, p->properties.data,
                                  p->properties.len) == 0);
            p->acks++;
            break;
        }
        case MOQ_EVENT_SUBSCRIBE_ERROR:
            R_CHECK(r, !p->origin && p->track == 0 &&
                           ev.u.subscribe_error.sub._opaque == p->sub[0]._opaque &&
                           ev.u.subscribe_error.error_code == MOQ_REQUEST_ERROR_INTERNAL_ERROR &&
                           !ev.u.subscribe_error.can_retry);
            p->errors++;
            break;
        case MOQ_EVENT_OBJECT_RECEIVED: {
            const moq_object_received_event_t *ob = &ev.u.object_received;
            R_CHECK(r, !p->origin && ob->sub._opaque == p->sub[p->track]._opaque &&
                           ob->group_id == (uint64_t)p->objects + 1 &&
                           ob->object_id == 0 && ob->datagram &&
                           ob->publisher_priority == 90 && ob->status == MOQ_OBJECT_NORMAL);
            R_CHECK(r, ob->payload != NULL &&
                           moq_rcbuf_len(ob->payload) == sizeof(d22_tp_payload) &&
                           memcmp(moq_rcbuf_data(ob->payload), d22_tp_payload,
                                  sizeof(d22_tp_payload)) == 0);
            p->objects++;
            break;
        }
        case MOQ_EVENT_UNSUBSCRIBED: {
            unsigned t = ev.u.unsubscribed.sub._opaque == p->sub[0]._opaque ? 0u : 1u;
            R_CHECK(r, p->origin &&
                           ev.u.unsubscribed.sub._opaque == p->sub[t]._opaque);
            p->unsubs[t]++;
            break;
        }
        case MOQ_EVENT_NAMESPACE_PUBLISHED: {
            const moq_namespace_published_event_t *q = &ev.u.namespace_published;
            size_t len = p->fillers == 0 ? 2400u : 1u;
            R_CHECK(r, !p->origin && p->track == 0 && p->fillers < 3 &&
                           q->track_namespace.count == 1 &&
                           q->track_namespace.parts != NULL &&
                           q->track_namespace.parts[0].len == len &&
                           q->track_namespace.parts[0].data != NULL && p->filler != NULL &&
                           memcmp(q->track_namespace.parts[0].data,
                                  p->filler + p->fillers, len) == 0);
            if (p->fillers < 3) p->filler_ann[p->fillers] = q->ann;
            p->fillers++;
            moq_accept_namespace_cfg_t cfg;
            moq_accept_namespace_cfg_init(&cfg);
            R_CHECK(r, moq_session_accept_namespace(cn->peer, q->ann, &cfg, r->now) == MOQ_OK);
            break;
        }
        case MOQ_EVENT_NAMESPACE_TERMINATED: {
            bool full = ev.detail_size >= sizeof(moq_namespace_terminated_event_t);
            R_CHECK(r, full);
            if (!full) {
                printf("FIXTURE: metadata_reply_capacity namespace terminal detail_size=%u "
                       "origin=%d track=%u\n",
                       (unsigned)ev.detail_size, (int)p->origin, p->track);
                break;
            }
            const moq_namespace_terminated_event_t *q = &ev.u.namespace_terminated;
            bool expected = !p->origin && p->track == 0 &&
                            q->local_role == MOQ_NAMESPACE_ROLE_RESPONDER &&
                            q->cause == MOQ_NAMESPACE_TERMINAL_PEER_RESET_STREAM &&
                            q->stream_error_code == 0x1;
            R_CHECK(r, expected);
            bool found = false;
            if (expected) {
                for (int i = 0; i < p->fillers && i < 3; i++) {
                    if (p->filler_ann[i]._opaque != 0 &&
                        p->filler_ann[i]._opaque == q->ann._opaque) {
                        p->filler_ann[i] = MOQ_ANNOUNCEMENT_INVALID;
                        found = true;
                        break;
                    }
                }
                R_CHECK(r, found);
            }
            if (!expected || !found) {
                printf("FIXTURE: metadata_reply_capacity namespace terminal role=%u "
                       "cause=%u error=%llu ann=%llu origin=%d track=%u matched=%d\n",
                       (unsigned)q->local_role, (unsigned)q->cause,
                       (unsigned long long)q->stream_error_code,
                       (unsigned long long)q->ann._opaque,
                       (int)p->origin, p->track, (int)found);
            }
            if (found) p->filler_done++;
            break;
        }
        default:
            printf("FIXTURE: metadata_reply_capacity unexpected event %d\n", (int)ev.kind);
            r->failures++;
            break;
        }
        moq_event_cleanup(&ev);
    }
}

/* 0 = pass, 1 = the named missing response only, 2 = setup/control failure. */
static int
metadata_reply_capacity(bool oversized, bool blocked)
{
    const char *row = !oversized ? "fitting" : blocked ? "blocked-refusal" : "immediate-refusal";
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    uint32_t saved_actions = relay_bind_test_max_actions;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        printf("FIXTURE: metadata_reply_capacity(%s) rig create\n", row);
        return 2;
    }
    conn_t *cn[3] = {0};   /* origin, target requester, healthy requester */
    tp_capacity_peer_t ps[3] = {0};
    ps[0].origin = true;
    ps[2].track = 1;
    moq_rcbuf_t *payload = NULL;
    int missing_response = 0;
    uint8_t large[2003], filler[2400];
    /* Unknown nonmandatory odd type 0x79, literal vi64 length 2000. */
    large[0] = 0x79; large[1] = 0x87; large[2] = 0xd0;
    memset(large + 3, 0x5a, sizeof(large) - 3);
    memset(filler, 'p', sizeof(filler));
    filler[1] = 'q'; filler[2] = 'r';
    ps[1].filler = filler;
    ps[1].properties = oversized
        ? (moq_bytes_t){ .data = large, .len = sizeof(large) }
        : (moq_bytes_t){ .data = d22_tp_expected, .len = sizeof(d22_tp_expected) };
    ps[2].properties = (moq_bytes_t){ .data = d22_tp_expected, .len = sizeof(d22_tp_expected) };
    moq_announcement_t filler_ann[3] = {0};
    int filler_count = blocked ? 3 : 1;
#define TC_REQUIRE(expr)                                                      \
    do {                                                                     \
        if (!(expr)) {                                                       \
            printf("FIXTURE: metadata_reply_capacity(%s): %s\n", row, #expr); \
            rig.failures++;                                                  \
            goto cleanup;                                                    \
        }                                                                    \
    } while (0)
    for (size_t i = 0; i < 3; i++) {
        relay_bind_test_max_actions = i == 1 && blocked ? 3u : 0u;
        cn[i] = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_22);
        TC_REQUIRE(cn[i] != NULL);
    }
    relay_bind_test_max_actions = saved_actions;
    relay_bind_test_rig_pump(&rig, 8);
    for (size_t i = 0; i < 3; i++) {
        tp_capacity_drain(&rig, cn[i], &ps[i]);
        TC_REQUIRE(ps[i].setups == 1 &&
                   moq_session_version(cn[i]->peer) == MOQ_VERSION_DRAFT_22 &&
                   moq_session_version(cn[i]->rsess) == MOQ_VERSION_DRAFT_22 &&
                   moqr_bind_conn_version(rig.bind, cn[i]->rsess) == MOQ_VERSION_DRAFT_22);
    }
    moq_bytes_t ns[] = { B("capacity"), B("metadata") };
    moq_publish_namespace_cfg_t pcfg;
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = ns, .count = 2 };
    TC_REQUIRE(moq_session_publish_namespace(cn[0]->peer, &pcfg, rig.now, &ps[0].ann) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 8);
    tp_capacity_drain(&rig, cn[0], &ps[0]);
    TC_REQUIRE(ps[0].announcements == 1 && rig.failures == 0);

    /* Establish the unrelated real-ingress track, then leave target pending. */
    for (int i = 2; i >= 1; i--) {
        unsigned t = ps[i].track;
        moq_subscribe_cfg_t scfg;
        moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = (moq_namespace_t){ .parts = ns, .count = 2 };
        scfg.track_name = B(tp_capacity_names[t]);
        scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
        TC_REQUIRE(moq_session_subscribe(cn[i]->peer, &scfg, rig.now, &ps[i].sub[t]) == MOQ_OK);
        relay_bind_test_rig_pump(&rig, 8);
        tp_capacity_drain(&rig, cn[0], &ps[0]);
        TC_REQUIRE(ps[0].requests[t] == 1 && rig.failures == 0);
        if (i == 2) {
            moq_accept_subscribe_cfg_t cfg;
            moq_accept_subscribe_cfg_init(&cfg);
            cfg.has_largest = true;
            cfg.track_properties = ps[2].properties;
            TC_REQUIRE(moq_session_accept_subscribe(cn[0]->peer, ps[0].sub[1], &cfg, rig.now) == MOQ_OK);
            relay_bind_test_rig_pump(&rig, 8);
            tp_capacity_drain(&rig, cn[2], &ps[2]);
            TC_REQUIRE(ps[2].acks == 1 && ps[2].errors == 0 && rig.failures == 0);
        }
    }
    TC_REQUIRE(moq_rcbuf_create(&a.vt, d22_tp_payload, sizeof(d22_tp_payload), &payload) == MOQ_OK);
    TC_REQUIRE(moq_session_send_object_datagram(cn[0]->peer, ps[0].sub[1],
                   1, 0, 90, false, payload, NULL, 0, rig.now) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 8);
    tp_capacity_drain(&rig, cn[2], &ps[2]);
    TC_REQUIRE(ps[2].objects == 1 && rig.failures == 0);

    moqr_bind_stats_t before;
    moqr_bind_get_stats(rig.bind, &before);
    TC_REQUIRE(before.session_errors == 0 && before.ingest_refusals == 0);
    /* No target transport after this point until the explicit release. The
     * 2400-byte namespace occupies bytes; two short ones fill a depth-3 queue. */
    cn[1]->frozen = true;
    size_t room = moq_session_action_capacity(cn[1]->rsess);
    TC_REQUIRE(blocked ? room == 3 : room > 3);
    for (int i = 0; i < filler_count; i++) {
        moq_bytes_t part = { .data = filler + i, .len = i == 0 ? sizeof(filler) : 1u };
        pcfg.track_namespace = (moq_namespace_t){ .parts = &part, .count = 1 };
        TC_REQUIRE(moq_session_publish_namespace(cn[1]->rsess, &pcfg, rig.now, &filler_ann[i]) == MOQ_OK);
    }
    TC_REQUIRE(moq_session_action_capacity(cn[1]->rsess) == room - (size_t)filler_count);
    moq_accept_subscribe_cfg_t acfg;
    moq_accept_subscribe_cfg_init(&acfg);
    acfg.has_largest = true;
    acfg.track_properties = ps[1].properties;
    TC_REQUIRE(moq_session_accept_subscribe(cn[0]->peer, ps[0].sub[0], &acfg, rig.now) == MOQ_OK);
    if (oversized) {
        TC_REQUIRE(moq_session_send_object_datagram(cn[0]->peer, ps[0].sub[0],
                       1, 0, 90, false, payload, NULL, 0, rig.now) == MOQ_OK);
    }
    relay_bind_test_rig_pump(&rig, 8);
    tp_capacity_drain(&rig, cn[1], &ps[1]);
    TC_REQUIRE(ps[1].acks == 0 && ps[1].errors == 0 && ps[1].objects == 0 && ps[1].fillers == 0);
    if (blocked) TC_REQUIRE(moq_session_action_capacity(cn[1]->rsess) == 0);
    moqr_bind_stats_t held;
    moqr_bind_get_stats(rig.bind, &held);

    /* Queue the healthy second object during the hold; a parked refusal may
     * defer ordinary delivery until the existing pending-intent gate clears. */
    TC_REQUIRE(moq_session_send_object_datagram(cn[0]->peer, ps[0].sub[1],
                   2, 0, 90, false, payload, NULL, 0, rig.now) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 8);
    tp_capacity_drain(&rig, cn[2], &ps[2]);
    TC_REQUIRE(ps[2].acks == 1 && ps[2].errors == 0 &&
               ps[2].objects >= 1 && ps[2].objects <= 2 && rig.failures == 0);
    TC_REQUIRE(moq_session_state(cn[2]->peer) == MOQ_SESS_ESTABLISHED &&
               moq_session_state(cn[2]->rsess) == MOQ_SESS_ESTABLISHED);

    cn[1]->frozen = false;
    for (int i = 0; i < 32; i++) {
        relay_bind_test_rig_cycle(&rig);
        for (size_t j = 0; j < 3; j++) tp_capacity_drain(&rig, cn[j], &ps[j]);
    }
    TC_REQUIRE(ps[1].fillers == filler_count && rig.failures == 0);
    TC_REQUIRE(ps[2].acks == 1 && ps[2].errors == 0 && ps[2].objects == 2 && rig.failures == 0);
    if (oversized) {
        TC_REQUIRE(ps[1].acks == 0 && ps[1].objects == 0 && ps[1].errors <= 1);
        if (ps[1].errors == 0) {
            printf("FAIL: metadata_reply_capacity(%s) missing_internal_error expected=1 actual=0\n", row);
            missing_response = 1;
            /* Clean up the still-pending request AFTER recording the missing
             * terminal; this cancellation cannot masquerade as its response. */
            TC_REQUIRE(moq_session_unsubscribe(cn[1]->peer, ps[1].sub[0], rig.now) == MOQ_OK);
        } else {
            TC_REQUIRE(moq_session_unsubscribe(cn[1]->peer, ps[1].sub[0], rig.now) == MOQ_ERR_STALE_HANDLE);
            if (blocked) TC_REQUIRE(held.pending_high_water > before.pending_high_water);
        }
    } else {
        TC_REQUIRE(ps[1].acks == 1 && ps[1].errors == 0);
        TC_REQUIRE(moq_session_send_object_datagram(cn[0]->peer, ps[0].sub[0],
                       1, 0, 90, false, payload, NULL, 0, rig.now) == MOQ_OK);
        relay_bind_test_rig_pump(&rig, 8);
        tp_capacity_drain(&rig, cn[1], &ps[1]);
        TC_REQUIRE(ps[1].objects == 1 && rig.failures == 0);
        TC_REQUIRE(moq_session_unsubscribe(cn[1]->peer, ps[1].sub[0], rig.now) == MOQ_OK);
    }
    TC_REQUIRE(moq_session_unsubscribe(cn[2]->peer, ps[2].sub[1], rig.now) == MOQ_OK);
    for (int i = 0; i < filler_count; i++) {
        TC_REQUIRE(moq_session_publish_namespace_done(cn[1]->rsess, filler_ann[i], rig.now) == MOQ_OK);
        relay_bind_test_rig_pump(&rig, 4);
        tp_capacity_drain(&rig, cn[1], &ps[1]);
    }
    for (int i = 0; i < 16; i++) {
        relay_bind_test_rig_cycle(&rig);
        for (size_t j = 0; j < 3; j++) tp_capacity_drain(&rig, cn[j], &ps[j]);
    }
    TC_REQUIRE(ps[0].requests[0] == 1 && ps[0].requests[1] == 1 &&
               ps[0].unsubs[0] == 1 && ps[0].unsubs[1] == 1 &&
               ps[1].filler_done == filler_count && ps[2].objects == 2 &&
               ps[1].errors == (oversized && !missing_response ? 1 : 0) &&
               ps[1].acks == (oversized ? 0 : 1) &&
               ps[1].objects == (oversized ? 0 : 1));
    for (size_t i = 0; i < 3; i++) {
        TC_REQUIRE(moq_session_state(cn[i]->peer) == MOQ_SESS_ESTABLISHED &&
                   moq_session_state(cn[i]->rsess) == MOQ_SESS_ESTABLISHED);
    }
    moqr_core_stats_t core_stats;
    moqr_core_get_stats(rig.core, &core_stats);
    TC_REQUIRE(core_stats.subs == 0);
    moqr_bind_stats_t after;
    moqr_bind_get_stats(rig.bind, &after);
    TC_REQUIRE(after.ingest_refusals == 0 && after.ordered_failed_closed == 0 &&
               after.pending_nonscalar_blocked == 0);
    printf("  metadata_reply_capacity(%s): property_bytes=%zu held_pending_high_water=%u "
           "session_errors=%llu errors=%d acks=%d objects=%d healthy_objects=%d\n",
           row, ps[1].properties.len, held.pending_high_water,
           (unsigned long long)after.session_errors, ps[1].errors, ps[1].acks,
           ps[1].objects, ps[2].objects);

cleanup:
    relay_bind_test_max_actions = saved_actions;
    moq_rcbuf_decref(payload);
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
#undef TC_REQUIRE
    printf("%s: metadata_reply_capacity(%s) response_failures=%d fixture_failures=%d\n",
           rig.failures == 0 && !missing_response ? "PASS" : "FAIL", row,
           missing_response, rig.failures);
    return rig.failures != 0 ? 2 : missing_response ? 1 : 0;
}

/* Legacy public-core publishers share a caller-owned core with the
 * session binding. The pre-existing row consumes its own publication intent
 * before any binding or attached session exists. */
typedef struct direct_core_peer {
    moq_subscription_t sub;
    unsigned setups, acks, errors, objects, finished, closed;
    uint64_t error_code;
} direct_core_peer_t;

static const uint8_t direct_core_payload[] = { 0x44, 0x49, 0x52, 0x00, 0xfe, 0x7a };

static void
direct_core_peer_drain(rig_t *r, conn_t *cn, direct_core_peer_t *ps)
{
    moq_event_t ev;
    while (moq_session_poll_events(cn->peer, &ev, 1) != 0) {
        switch (ev.kind) {
        case MOQ_EVENT_SETUP_COMPLETE:
            ps->setups++;
            R_CHECK(r, ps->setups == 1);
            break;
        case MOQ_EVENT_REQUEST_READY:
            break;
        case MOQ_EVENT_SUBSCRIBE_OK: {
            bool full = ev.detail_size >= sizeof(moq_subscribe_ok_event_t);
            R_CHECK(r, full);
            if (!full) break;
            const moq_subscribe_ok_event_t *ok = &ev.u.subscribe_ok;
            R_CHECK(r, ps->sub._opaque != 0 && ok->sub._opaque == ps->sub._opaque);
            R_CHECK(r, ok->track_properties.len == 0 && !ok->dynamic_groups &&
                           !ok->has_largest);
            ps->acks++;
            R_CHECK(r, ps->acks == 1);
            break;
        }
        case MOQ_EVENT_SUBSCRIBE_ERROR: {
            bool full = ev.detail_size >= sizeof(moq_subscribe_error_event_t);
            R_CHECK(r, full);
            if (!full) break;
            R_CHECK(r, ps->sub._opaque != 0 &&
                           ev.u.subscribe_error.sub._opaque == ps->sub._opaque);
            ps->errors++;
            ps->error_code = ev.u.subscribe_error.error_code;
            R_CHECK(r, ps->errors == 1);
            break;
        }
        case MOQ_EVENT_OBJECT_RECEIVED: {
            bool full = ev.detail_size >= sizeof(moq_object_received_event_t);
            R_CHECK(r, full);
            if (!full) break;
            const moq_object_received_event_t *ob = &ev.u.object_received;
            R_CHECK(r, ps->acks == 1 && ps->errors == 0 &&
                           ob->sub._opaque == ps->sub._opaque && ob->pub._opaque == 0 &&
                           ob->group_id == 7 && ob->subgroup_id == 3 && ob->object_id == 0 &&
                           ob->publisher_priority == 17 && ob->status == MOQ_OBJECT_NORMAL &&
                           !ob->datagram && !ob->end_of_group);
            R_CHECK(r, ob->payload != NULL &&
                           moq_rcbuf_len(ob->payload) == sizeof(direct_core_payload) &&
                           memcmp(moq_rcbuf_data(ob->payload), direct_core_payload,
                                  sizeof(direct_core_payload)) == 0);
            ps->objects++;
            R_CHECK(r, ps->objects == 1);
            break;
        }
        case MOQ_EVENT_SUBGROUP_FINISHED: {
            bool full = ev.detail_size >= sizeof(moq_subgroup_finished_event_t);
            R_CHECK(r, full);
            if (!full) break;
            const moq_subgroup_finished_event_t *sg = &ev.u.subgroup_finished;
            R_CHECK(r, ps->objects == 1 && sg->sub._opaque == ps->sub._opaque &&
                           sg->group_id == 7 && sg->subgroup_id == 3);
            ps->finished++;
            R_CHECK(r, ps->finished == 1);
            break;
        }
        case MOQ_EVENT_SESSION_CLOSED:
            ps->closed++;
            break;
        default:
            printf("FIXTURE: direct_core_publisher unexpected event=%d\n", (int)ev.kind);
            r->failures++;
            break;
        }
        moq_event_cleanup(&ev);
    }
}

/* 0 = compatible, 1 = missing/error admission, 2 = setup/data/cleanup failure. */
static int
direct_core_publisher(moq_version_t version, bool preexisting)
{
    const char *profile = version == MOQ_VERSION_DRAFT_16 ? "d16" : "d18";
    const char *order = preexisting ? "before-bind" : "after-bind";
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    memset(&rig, 0, sizeof(rig));
    rig.alloc = &a;
    rig.now = 1;
    conn_t *sub = NULL;
    moq_rcbuf_t *payload = NULL;
    direct_core_peer_t ps;
    memset(&ps, 0, sizeof(ps));
    int admission_failure = 0;

#define DP_REQUIRE(expr)                                                     \
    do {                                                                    \
        if (!(expr)) {                                                      \
            printf("FIXTURE: direct_core_publisher(%s/%s): %s\n",          \
                   profile, order, #expr);                                   \
            rig.failures++;                                                 \
            goto cleanup;                                                   \
        }                                                                   \
    } while (0)

    moqr_core_relay_cfg_t cfg;
    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a.vt);
    cfg.log_budget.max_groups = 8;
    cfg.log_budget.max_bytes = 1 << 20;
    cfg.linger_us = 500;
    DP_REQUIRE(moqr_core_create(&cfg, &rig.core) == MOQR_OK);
    moqr_bind_cfg_t bcfg;
    moqr_bind_cfg_init_sized(&bcfg, sizeof(bcfg), &a.vt);
    bcfg.core = rig.core;
    bcfg.max_conns = 1;
    if (!preexisting) {
        DP_REQUIRE(moqr_bind_create(&bcfg, &rig.bind) == MOQR_OK);
    }

    moqr_binding_t producer = MOQR_BINDING_INVALID;
    moqr_track_t track = MOQR_TRACK_INVALID;
    moq_bytes_t parts[1] = { B("direct") };
    moqr_ns_t ns = { .parts = parts, .count = 1 };
    DP_REQUIRE(moqr_core_binding_open(rig.core, 900, &producer) == MOQR_OK);
    DP_REQUIRE(moqr_core_announce(rig.core, producer, ns) == MOQR_OK);
    DP_REQUIRE(moqr_core_publish_open(rig.core, producer, ns, B("video"),
                                      901, &track) == MOQR_OK);
    if (preexisting) {
        moqr_intent_t it;
        DP_REQUIRE(rig.bind == NULL);
        DP_REQUIRE(moqr_core_poll_intents(rig.core, &it, 1) == 1);
        DP_REQUIRE(it.kind == MOQR_INTENT_ACCEPT_PUBLISH &&
                   it.binding_cookie == 900 && it.cookie == 901 &&
                   it.track._opaque == track._opaque && it.track_gen != 0);
        DP_REQUIRE(moqr_core_poll_intents(rig.core, &it, 1) == 0);
        DP_REQUIRE(moqr_bind_create(&bcfg, &rig.bind) == MOQR_OK);
    }

    sub = relay_bind_test_rig_connect(&rig, version);
    DP_REQUIRE(sub != NULL);
    relay_bind_test_rig_pump(&rig, 4);
    direct_core_peer_drain(&rig, sub, &ps);
    DP_REQUIRE(rig.failures == 0 && ps.setups == 1 && ps.closed == 0 &&
               moq_session_state(sub->peer) == MOQ_SESS_ESTABLISHED &&
               moq_session_state(sub->rsess) == MOQ_SESS_ESTABLISHED);

    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = parts, .count = 1 };
    scfg.track_name = B("video");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    scfg.start_group = 7;
    scfg.start_object = 0;
    DP_REQUIRE(moq_session_subscribe(sub->peer, &scfg, rig.now, &ps.sub) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 8);
    direct_core_peer_drain(&rig, sub, &ps);
    moqr_bind_stats_t stats;
    moqr_bind_get_stats(rig.bind, &stats);
    DP_REQUIRE(rig.failures == 0 && ps.closed == 0 && ps.objects == 0 &&
               stats.session_errors == 0 && stats.ingest_refusals == 0 &&
               moq_session_state(sub->peer) == MOQ_SESS_ESTABLISHED &&
               moq_session_state(sub->rsess) == MOQ_SESS_ESTABLISHED);
    if (ps.acks != 1 || ps.errors != 0) {
        printf("FAIL: direct_core_publisher(%s/%s) empty_subscribe_ok "
               "expected=1 actual=%u errors=%u code=%llu\n",
               profile, order, ps.acks, ps.errors, (unsigned long long)ps.error_code);
        admission_failure = 1;
        goto cleanup;
    }

    DP_REQUIRE(moq_rcbuf_create(&a.vt, direct_core_payload,
                                sizeof(direct_core_payload), &payload) == MOQ_OK);
    moqr_log_append_desc_t desc;
    moqr_log_append_desc_init(&desc);
    desc.group_id = 7;
    desc.subgroup_id = 3;
    desc.object_id = 0;
    desc.publisher_priority = 17;
    desc.payload = payload;
    desc.now_us = rig.now;
    DP_REQUIRE(moqr_core_ingest(rig.core, track, &desc) == MOQR_OK);
    payload = NULL;
    relay_bind_test_rig_pump(&rig, 8);
    direct_core_peer_drain(&rig, sub, &ps);
    DP_REQUIRE(rig.failures == 0 && ps.acks == 1 && ps.errors == 0 &&
               ps.closed == 0 && ps.objects == 1);
    DP_REQUIRE(moq_session_unsubscribe(sub->peer, ps.sub, rig.now) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 8);
    direct_core_peer_drain(&rig, sub, &ps);
    moqr_bind_get_stats(rig.bind, &stats);
    moqr_core_stats_t core_stats;
    moqr_core_get_stats(rig.core, &core_stats);
    DP_REQUIRE(rig.failures == 0 && ps.acks == 1 && ps.errors == 0 &&
               ps.closed == 0 && ps.objects == 1 && core_stats.subs == 0 &&
               stats.deliveries_written == 1 && stats.session_errors == 0 &&
               stats.ingest_refusals == 0 &&
               moq_session_state(sub->peer) == MOQ_SESS_ESTABLISHED &&
               moq_session_state(sub->rsess) == MOQ_SESS_ESTABLISHED);
    DP_REQUIRE(moqr_core_binding_close(rig.core, producer, rig.now) == MOQR_OK);

cleanup:
    moq_rcbuf_decref(payload);
    if (rig.bind != NULL) moqr_bind_destroy(rig.bind);
    if (rig.core != NULL) moqr_core_destroy(rig.core);
    if (sub != NULL) moq_simpair_destroy(sub->sp);
    R_CHECK(&rig, a.live == 0);
#undef DP_REQUIRE
    printf("%s: direct_core_publisher(%s/%s) admission_failures=%d fixture_failures=%d\n",
           admission_failure == 0 && rig.failures == 0 ? "PASS" : "FAIL",
           profile, order, admission_failure, rig.failures);
    return rig.failures != 0 ? 2 : admission_failure;
}

static int
direct_core_publisher_all(void)
{
    const moq_version_t versions[] = { MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18 };
    int failed = 0;
    for (size_t i = 0; i < sizeof(versions) / sizeof(versions[0]); i++) {
        for (unsigned order = 0; order < 2; order++) {
            int rc = direct_core_publisher(versions[i], order != 0);
            if (rc == 2) return 2;
            if (rc != 0) failed = 1;
        }
    }
    return failed;
}

typedef struct retrieval_push_peer {
    conn_t *cn;
    bool origin;
    uint8_t tag;
    uint8_t end_object; /* 0 preserves the original live island / FETCH end 2. */
    bool from_zero;    /* Opt in to a leading miss with only the end island. */
    moq_publication_t pub;
    moq_subscription_t sub;
    moq_fill_t fill;
    moq_fetch_t fetch;
    moq_rcbuf_t *retained, *properties;
    unsigned setups, publications, requests, cancelled, acks, created;
    unsigned objects, terminals, done, live;
    uint32_t terminal_reason, terminal_domain;
} retrieval_push_peer_t;

static bool
retrieval_push_bytes(const moq_rcbuf_t *buf, const uint8_t *want, size_t n)
{
    return buf != NULL && moq_rcbuf_len(buf) == n &&
           memcmp(moq_rcbuf_data(buf), want, n) == 0;
}

static void
retrieval_push_drain(rig_t *r, retrieval_push_peer_t *p)
{
    moq_event_t ev;
    while (moq_session_poll_events(p->cn->peer, &ev, 1) == 1) {
        switch (ev.kind) {
        case MOQ_EVENT_SETUP_COMPLETE:
            p->setups++;
            R_CHECK(r, p->setups == 1);
            break;
        case MOQ_EVENT_REQUEST_READY:
            break;
        case MOQ_EVENT_PUBLISH_OK:
            R_CHECK(r, p->origin && ev.u.publish_ok.pub._opaque == p->pub._opaque &&
                       ev.u.publish_ok.send_allowed);
            p->publications++;
            R_CHECK(r, p->publications == 1);
            break;
        case MOQ_EVENT_FETCH_REQUEST: {
            const moq_fetch_request_event_t *q = &ev.u.fetch_request;
            R_CHECK(r, p->origin && p->publications == 1 && q->fetch._opaque != 0 &&
                       q->joining_sub._opaque == 0 && q->joining_pub._opaque == 0 &&
                       q->track_namespace.count == 2 && q->track_namespace.parts != NULL);
            if (q->track_namespace.count == 2 && q->track_namespace.parts != NULL) {
                R_CHECK(r, q->track_namespace.parts[0].len == 8 &&
                           q->track_namespace.parts[0].data != NULL &&
                           memcmp(q->track_namespace.parts[0].data, "retained", 8) == 0 &&
                           q->track_namespace.parts[1].len == 4 &&
                           q->track_namespace.parts[1].data != NULL &&
                           memcmp(q->track_namespace.parts[1].data, "push", 4) == 0);
            }
            R_CHECK(r, q->track_name.len == 1 && q->track_name.data != NULL &&
                       q->track_name.data[0] == 'v' &&
                       q->start_group == 0 && q->start_object == (p->from_zero ? 0u : 1u) &&
                       q->end_group == 0 &&
                       q->end_object == (p->end_object != 0 ? p->end_object : 2u) &&
                       q->group_order == MOQ_GROUP_ORDER_ASCENDING &&
                       q->subscriber_priority == 61);
            p->requests++;
            R_CHECK(r, p->requests == 1);
            if (p->requests == 1) p->fetch = q->fetch;
            break;
        }
        case MOQ_EVENT_FETCH_CANCELLED:
            R_CHECK(r, p->origin && p->requests == 1 &&
                       ev.u.fetch_cancelled.fetch._opaque == p->fetch._opaque);
            p->cancelled++;
            R_CHECK(r, p->cancelled == 1);
            break;
        case MOQ_EVENT_SUBSCRIBE_OK:
            R_CHECK(r, !p->origin && ev.u.subscribe_ok.sub._opaque == p->sub._opaque &&
                       ev.u.subscribe_ok.track_properties.len == 0 &&
                       ev.u.subscribe_ok.has_largest &&
                       ev.u.subscribe_ok.largest_group == 0 &&
                       ev.u.subscribe_ok.largest_object == 2);
            p->acks++;
            R_CHECK(r, p->acks == 1);
            break;
        case MOQ_EVENT_FILL_CREATED:
            R_CHECK(r, !p->origin && ev.u.fill_created.sub._opaque == p->sub._opaque &&
                       ev.u.fill_created.pub._opaque == 0 && ev.u.fill_created.fill._opaque != 0);
            p->fill = ev.u.fill_created.fill;
            p->created++;
            R_CHECK(r, p->created == 1);
            break;
        case MOQ_EVENT_FILL_OBJECT: {
            const moq_fill_object_event_t *o = &ev.u.fill_object;
            uint8_t id = (uint8_t)(p->objects + 1u);
            uint8_t body[] = { 0x52, 0, id, id == 1 ? p->tag : 0xc3, 0, 0xfe };
            uint8_t properties[] = { 0x7c, id == 1 ? 0x31 : 0x23 };
            R_CHECK(r, !p->origin && p->created == 1 && p->objects < 2 &&
                       p->terminals == 0 && p->done == 0 &&
                       o->fill._opaque == p->fill._opaque && o->sub._opaque == p->sub._opaque &&
                       o->pub._opaque == 0 && o->group_id == 0 && o->object_id == id &&
                       o->subgroup_id == 0 && o->publisher_priority == 73 &&
                       o->datagram == (id == 2));
            R_CHECK(r, retrieval_push_bytes(o->payload, body, sizeof(body)) &&
                       retrieval_push_bytes(o->properties, properties, sizeof(properties)));
            p->objects++;
            break;
        }
        case MOQ_EVENT_FILL_TERMINAL:
            R_CHECK(r, !p->origin && ev.u.fill_terminal.fill._opaque == p->fill._opaque &&
                       ev.u.fill_terminal.sub._opaque == p->sub._opaque &&
                       ev.u.fill_terminal.pub._opaque == 0);
            p->terminals++;
            p->terminal_reason = ev.u.fill_terminal.reason;
            p->terminal_domain = ev.u.fill_terminal.error_domain;
            R_CHECK(r, p->terminals == 1);
            break;
        case MOQ_EVENT_SUBSCRIBE_DONE:
            R_CHECK(r, !p->origin && ev.u.subscribe_done.sub._opaque == p->sub._opaque);
            p->done++;
            R_CHECK(r, p->done == 1);
            break;
        case MOQ_EVENT_OBJECT_RECEIVED: {
            static const uint8_t body[] = { 0x52, 1, 0, 0xc3, 0, 0xfe };
            static const uint8_t properties[] = { 0x7c, 0x23 };
            const moq_object_received_event_t *o = &ev.u.object_received;
            R_CHECK(r, !p->origin && p->objects == 2 && p->terminals == 1 &&
                       p->done == 0 && o->sub._opaque == p->sub._opaque &&
                       o->pub._opaque == 0 && o->group_id == 1 && o->object_id == 0 &&
                       o->datagram && o->publisher_priority == 73 &&
                       o->status == MOQ_OBJECT_NORMAL);
            R_CHECK(r, retrieval_push_bytes(o->payload, body, sizeof(body)) &&
                       retrieval_push_bytes(o->properties, properties, sizeof(properties)));
            p->live++;
            R_CHECK(r, p->live == 1);
            break;
        }
        default:
            /* In particular, a pull SUBSCRIBE or namespace route is not a
             * substitute for retrieving from the accepted publication. */
            printf("FIXTURE: retrieval_publish_source unexpected event=%d\n", (int)ev.kind);
            r->failures++;
            break;
        }
        moq_event_cleanup(&ev);
    }
}

static void
retrieval_push_pump(rig_t *r, retrieval_push_peer_t peers[4], int cycles)
{
    for (int i = 0; i < cycles; i++) {
        relay_bind_test_rig_cycle(r);
        for (size_t j = 0; j < 4; j++) {
            if (peers[j].cn != NULL) retrieval_push_drain(r, &peers[j]);
        }
    }
}

static bool
retrieval_push_live(rig_t *r, retrieval_push_peer_t *p, uint8_t group, uint8_t id)
{
    uint8_t body[] = { 0x52, group, id, 0xc3, 0, 0xfe };
    static const uint8_t properties[] = { 0x7c, 0x23 };
    moq_rcbuf_t *buf = NULL;
    if (moq_rcbuf_create(&r->alloc->vt, body, sizeof(body), &buf) != MOQ_OK) return false;
    moq_result_t rc = moq_session_send_pub_object_datagram(p->cn->peer, p->pub,
        group, id, 73, false, buf, properties, sizeof(properties), r->now);
    moq_rcbuf_decref(buf);
    return rc == MOQ_OK;
}

static bool
retrieval_push_origin(rig_t *r, retrieval_push_peer_t peers[4], size_t index,
                      uint8_t tag)
{
    retrieval_push_peer_t *p = &peers[index];
    uint8_t body[] = { 0x52, 0, 1, tag, 0, 0xfe };
    static const uint8_t properties[] = { 0x7c, 0x31 };
    p->origin = true;
    p->tag = tag;
    if (moq_rcbuf_create(&r->alloc->vt, body, sizeof(body), &p->retained) != MOQ_OK ||
        moq_rcbuf_create(&r->alloc->vt, properties, sizeof(properties), &p->properties) != MOQ_OK)
        return false;
    p->cn = relay_bind_test_rig_connect(r, MOQ_VERSION_DRAFT_22);
    if (p->cn == NULL) return false;
    retrieval_push_pump(r, peers, 4);
    moq_bytes_t parts[] = { B("retained"), B("push") };
    moq_publish_cfg_t cfg;
    moq_publish_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.track_namespace = (moq_namespace_t){ .parts = parts, .count = 2 };
    cfg.track_name = B("v");
    if (moq_session_publish(p->cn->peer, &cfg, r->now, &p->pub) != MOQ_OK) return false;
    retrieval_push_pump(r, peers, 6);
    if (p->setups != 1 || p->publications != 1 || r->failures != 0) return false;
    /* Only live islands enter the relay; the intervening span stays at origin. */
    if (!p->from_zero) {
        if (!retrieval_push_live(r, p, 0, 0)) return false;
        retrieval_push_pump(r, peers, 4);
    }
    if (!retrieval_push_live(r, p, 0, p->end_object != 0 ? p->end_object : 2)) return false;
    retrieval_push_pump(r, peers, 4);
    return r->failures == 0;
}

static bool
retrieval_push_subscribe(rig_t *r, retrieval_push_peer_t peers[4], size_t index,
                         uint8_t tag)
{
    retrieval_push_peer_t *p = &peers[index];
    p->tag = tag;
    p->cn = relay_bind_test_rig_connect(r, MOQ_VERSION_DRAFT_22);
    if (p->cn == NULL) return false;
    retrieval_push_pump(r, peers, 4);
    moq_bytes_t parts[] = { B("retained"), B("push") };
    moq_location_filter_cfg_t loc, start;
    moq_location_filter_cfg_init(&loc);
    loc.kind = MOQ_LOCATION_FILTER_NEXT_OBJECT;
    moq_location_filter_cfg_init(&start);
    start.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;
    start.start_object = 1;
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.location = &start;
    fill.has_fill_timeout = true;
    fill.fill_timeout_ms = 100;
    moq_subscribe_cfg_t cfg;
    moq_subscribe_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.track_namespace = (moq_namespace_t){ .parts = parts, .count = 2 };
    cfg.track_name = B("v");
    cfg.location = &loc;
    cfg.fill = &fill;
    cfg.group_order = MOQ_GROUP_ORDER_ASCENDING;
    cfg.has_subscriber_priority = true;
    cfg.subscriber_priority = 61;
    return moq_session_subscribe(p->cn->peer, &cfg, r->now, &p->sub) == MOQ_OK;
}

static bool
retrieval_push_accept(rig_t *r, retrieval_push_peer_t *p)
{
    moq_accept_fetch_cfg_t cfg;
    moq_accept_fetch_cfg_init(&cfg);
    cfg.end_group = 0;
    cfg.end_object = p->end_object != 0 ? p->end_object : 2;
    return moq_session_accept_fetch(p->cn->peer, p->fetch, &cfg, r->now) == MOQ_OK;
}

static bool
retrieval_push_write(rig_t *r, retrieval_push_peer_t *p, bool fin)
{
    moq_fetch_object_cfg_t cfg;
    moq_fetch_object_cfg_init(&cfg);
    cfg.object_id = 1;
    cfg.publisher_priority = 73;
    cfg.payload = p->retained;
    cfg.properties = p->properties;
    return moq_session_write_fetch_object(p->cn->peer, p->fetch, &cfg, r->now) == MOQ_OK &&
           (!fin || moq_session_end_fetch(p->cn->peer, p->fetch, r->now) == MOQ_OK);
}

static int
retrieval_publish_source(bool replacement)
{
    const char *row = replacement ? "replacement" : "roundtrip";
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        printf("FIXTURE: retrieval_publish_source(%s) rig create\n", row);
        return 2;
    }
    retrieval_push_peer_t peers[4] = {0};
    int missing = 0;
    moqr_core_stats_t stats;
    moqr_bind_stats_t bs;
#define RP_REQUIRE(expr)                                                      \
    do {                                                                     \
        if (!(expr)) {                                                       \
            printf("FIXTURE: retrieval_publish_source(%s): %s\n", row, #expr); \
            rig.failures++;                                                  \
            goto cleanup;                                                    \
        }                                                                    \
    } while (0)
    RP_REQUIRE(retrieval_push_origin(&rig, peers, 0, 0xa1));
    RP_REQUIRE(retrieval_push_subscribe(&rig, peers, 1, 0xa1));
    uint64_t start = rig.now;
    retrieval_push_pump(&rig, peers, 20);
    uint64_t deadline = moqr_bind_next_deadline_us(rig.bind);
    RP_REQUIRE(rig.failures == 0 && peers[1].acks == 1 && peers[1].created == 1 &&
               peers[1].objects == 0 && peers[1].terminals == 0 && peers[1].live == 0 &&
               deadline >= start + 100000 && deadline <= start + 105000);
    moqr_core_get_stats(rig.core, &stats);
    RP_REQUIRE(stats.ingested_total == 2);
    if (peers[0].requests != 1) {
        printf("FAIL: retrieval_publish_source(%s) origin_requests expected=1 actual=%u\n",
               row, peers[0].requests);
        missing = 1;
        goto cleanup;
    }
    RP_REQUIRE(retrieval_push_accept(&rig, &peers[0]));
    retrieval_push_pump(&rig, peers, 4);
    RP_REQUIRE(rig.failures == 0 && peers[1].objects == 0 && peers[1].terminals == 0 &&
               moqr_bind_next_deadline_us(rig.bind) == deadline);
    size_t origin = 0, downstream = 1;
    if (replacement) {
        moq_finish_publish_cfg_t cfg;
        moq_finish_publish_cfg_init(&cfg);
        RP_REQUIRE(moq_session_finish_publish(peers[0].cn->peer, peers[0].pub,
                                               &cfg, rig.now) == MOQ_OK);
        /* Deliver the real publication terminal, then keep its queued FETCH
         * cancellation off the old peer until replacement demand is active. */
        relay_bind_test_rig_step_pair(&rig, peers[0].cn);
        peers[0].cn->frozen = true;
        relay_bind_test_rig_bind_pump_once(&rig);
        retrieval_push_pump(&rig, peers, 6);
        RP_REQUIRE(rig.failures == 0 && peers[1].done == 1 && peers[1].terminals == 1 &&
                   peers[1].objects == 0 && rig.now < deadline);
        RP_REQUIRE((peers[1].terminal_reason == MOQ_FILL_PARENT_TERMINATED &&
                     peers[1].terminal_domain == MOQ_FILL_ERROR_PARENT_DONE) ||
                   (peers[1].terminal_reason == MOQ_FILL_PEER_CANCELLED_OR_RESET &&
                     peers[1].terminal_domain == MOQ_FILL_ERROR_STREAM));
        RP_REQUIRE(retrieval_push_origin(&rig, peers, 2, 0xb2));
        RP_REQUIRE(retrieval_push_subscribe(&rig, peers, 3, 0xb2));
        start = rig.now;
        retrieval_push_pump(&rig, peers, 20);
        deadline = moqr_bind_next_deadline_us(rig.bind);
        RP_REQUIRE(rig.failures == 0 && peers[2].requests == 1 && peers[3].acks == 1 &&
                   peers[3].created == 1 && peers[3].objects == 0 &&
                   peers[3].terminals == 0 && deadline >= start + 100000 &&
                   deadline <= start + 105000);
        RP_REQUIRE(retrieval_push_write(&rig, &peers[0], false));
        peers[0].cn->frozen = false;
        retrieval_push_pump(&rig, peers, 8);
        RP_REQUIRE(rig.failures == 0 && peers[0].cancelled == 1 &&
                   peers[0].requests == 1 && peers[1].terminals == 1 &&
                   peers[1].objects == 0 && peers[3].objects == 0 &&
                   peers[3].terminals == 0 && moqr_bind_next_deadline_us(rig.bind) == deadline);
        RP_REQUIRE(moq_session_end_fetch(peers[0].cn->peer, peers[0].fetch, rig.now) ==
                   MOQ_ERR_STALE_HANDLE);
        RP_REQUIRE(retrieval_push_accept(&rig, &peers[2]));
        origin = 2;
        downstream = 3;
    }
    RP_REQUIRE(retrieval_push_write(&rig, &peers[origin], true));
    retrieval_push_pump(&rig, peers, 16);
    RP_REQUIRE(rig.failures == 0 && peers[downstream].objects == 2 &&
               peers[downstream].terminals == 1 && peers[downstream].done == 0 &&
               peers[downstream].terminal_reason == MOQ_FILL_COMPLETE &&
               peers[downstream].terminal_domain == MOQ_FILL_ERROR_NONE && rig.now < deadline &&
               peers[origin].requests == 1 && peers[origin].cancelled == 0);
    moqr_core_get_stats(rig.core, &stats);
    RP_REQUIRE(stats.ingested_total == (replacement ? 4u : 2u));
    RP_REQUIRE(retrieval_push_live(&rig, &peers[origin], 1, 0));
    retrieval_push_pump(&rig, peers, 8);
    RP_REQUIRE(rig.failures == 0 && peers[downstream].live == 1 &&
               peers[downstream].objects == 2 && peers[downstream].terminals == 1);
    RP_REQUIRE(moq_session_unsubscribe(peers[downstream].cn->peer,
                                         peers[downstream].sub, rig.now) == MOQ_OK);
    retrieval_push_pump(&rig, peers, 8);
cleanup:
    moqr_bind_get_stats(rig.bind, &bs);
    R_CHECK(&rig, bs.session_errors == 0 && bs.ingest_refusals == 0);
    for (size_t i = 0; i < 4; i++) {
        if (peers[i].cn != NULL) {
            R_CHECK(&rig, moq_session_state(peers[i].cn->peer) == MOQ_SESS_ESTABLISHED &&
                          moq_session_state(peers[i].cn->rsess) == MOQ_SESS_ESTABLISHED);
        }
    }
    for (size_t i = 0; i < 4; i++) {
        moq_rcbuf_decref(peers[i].retained);
        moq_rcbuf_decref(peers[i].properties);
    }
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
#undef RP_REQUIRE
    printf("%s: retrieval_publish_source(%s) retrieval_failures=%d fixture_failures=%d\n",
           missing || rig.failures ? "FAIL" : "PASS", row, missing, rig.failures);
    return rig.failures != 0 ? 2 : missing;
}

static int
retrieval_publish_source_all(void)
{
    int plain = retrieval_publish_source(false);
    if (plain == 2) return 2;
    int replacement = retrieval_publish_source(true);
    return replacement == 2 ? 2 : plain != 0 || replacement != 0;
}

static bool
retrieval_frontier_write(rig_t *r, retrieval_push_peer_t *p, uint8_t id,
                         uint8_t tag, uint8_t property)
{
    uint8_t body[] = { 0x52, 0, id, tag, 0, 0xfe };
    uint8_t properties[] = { 0x7c, property };
    moq_rcbuf_t *payload = NULL, *props = NULL;
    bool ok = moq_rcbuf_create(&r->alloc->vt, body, sizeof(body), &payload) == MOQ_OK &&
              moq_rcbuf_create(&r->alloc->vt, properties, sizeof(properties), &props) == MOQ_OK;
    if (ok) {
        moq_fetch_object_cfg_t cfg;
        moq_fetch_object_cfg_init(&cfg);
        cfg.object_id = id;
        cfg.publisher_priority = 73;
        cfg.payload = payload;
        cfg.properties = props;
        ok = moq_session_write_fetch_object(p->cn->peer, p->fetch, &cfg, r->now) == MOQ_OK;
    }
    moq_rcbuf_decref(payload);
    moq_rcbuf_decref(props);
    return ok;
}

static bool
retrieval_frontier_object(const moqr_retrieve_item_t *item, uint8_t id,
                          bool live, uint8_t tag, uint8_t property)
{
    uint8_t body[] = { 0x52, 0, id, tag, 0, 0xfe };
    uint8_t properties[] = { 0x7c, property };
    return item->kind == MOQR_RETRIEVE_OBJECT &&
           item->start.group == 0 && item->start.object == id &&
           item->end.group == 0 && item->end.object == id &&
           item->object.group_id == 0 && item->object.object_id == id &&
           item->object.subgroup_id == 0 && item->object.publisher_priority == 73 &&
           item->object.status == MOQR_OBJ_NORMAL &&
           item->object.obj_state == MOQR_OBJ_COMPLETE &&
           item->object.datagram_pref == live && !item->object.end_of_group &&
           retrieval_push_bytes(item->object.payload, body, sizeof(body)) &&
           retrieval_push_bytes(item->object.properties, properties, sizeof(properties));
}

static bool
retrieval_frontier_open(rig_t *r, uint8_t start, uint8_t end,
                        moqr_retrieve_cursor_t *cursor, uint64_t *deadline)
{
    /* The accepted pseudo parent is the private cursor's downstream owner;
     * ordinary forwarding stays paused so only explicit commits consume it. */
    moqr_binding_t consumer = MOQR_BINDING_INVALID;
    if (moqr_core_binding_open(r->core, 803, &consumer) != MOQR_OK) return false;
    moq_bytes_t parts[] = { B("retained"), B("push") };
    moqr_subscribe_req_t sr;
    moqr_subscribe_req_init(&sr);
    sr.ns = (moqr_ns_t){ .parts = parts, .count = 2 };
    sr.name = B("v");
    sr.cookie = 902;
    sr.group_order = MOQR_GROUP_ORDER_ASCENDING;
    sr.forward = false;
    moqr_sub_t parent = MOQR_SUB_INVALID;
    if (moqr_core_subscribe(r->core, consumer, &sr, &parent) != MOQR_OK) return false;
    moqr_intent_t intent;
    if (moqr_core_poll_intents(r->core, &intent, 1) != 1 ||
        intent.kind != MOQR_INTENT_ACCEPT_SUB ||
        intent.binding_cookie != 803 || intent.cookie != 902 ||
        intent.sub._opaque != parent._opaque) return false;
    moqr_retrieve_request_t request = {0};
    request.binding = consumer;
    request.parent = parent;
    if (moqr_core_sub_track(r->core, parent, &request.track,
                           &request.track_gen) != MOQR_OK) return false;
    request.source = 803;
    request.demand = parent._opaque;
    request.child = 903;
    request.start.object = start;
    request.end.object = end;
    request.group_order = MOQR_GROUP_ORDER_ASCENDING;
    request.subscriber_priority = 61;
    *deadline = r->now + 100000;
    request.deadline_us = *deadline;
    return moqr_bind_retrieve_init(r->bind, cursor, &request) == MOQR_OK;
}

static int
retrieval_live_frontier(bool gap)
{
    const char *row = gap ? "gap-tail" : "object-tail";
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        printf("FIXTURE: retrieval_live_frontier(%s) rig create\n", row);
        return 2;
    }
    retrieval_push_peer_t peers[4] = {0};
    moqr_retrieve_cursor_t cursor = {0};
    moqr_bind_fwd_debug_t fwd;
    moqr_bind_stats_t bs;
    moqr_core_stats_t stats;
    unsigned committed = 0;
#define RF_REQUIRE(expr)                                                      \
    do {                                                                     \
        if (!(expr)) {                                                       \
            printf("FAIL: retrieval_live_frontier(%s): %s\n", row, #expr);    \
            rig.failures++;                                                  \
            goto cleanup;                                                    \
        }                                                                    \
    } while (0)
    peers[0].end_object = 4;
    RF_REQUIRE(retrieval_push_origin(&rig, peers, 0, 0xa1));
    /* Like a shard owner, consume through an accepted core parent and the
     * private cursor. PUBLISH identity and all data still arrive over SimPair. */
    uint64_t deadline = 0;
    RF_REQUIRE(retrieval_frontier_open(&rig, 1, 4, &cursor, &deadline));
    moqr_retrieve_item_t item, held, again;
    RF_REQUIRE(moqr_bind_retrieve_peek(rig.bind, &cursor, rig.now, &item) ==
               MOQR_ERR_WOULD_BLOCK);
    retrieval_push_pump(&rig, peers, 6);
    RF_REQUIRE(rig.failures == 0 && peers[0].requests == 1 &&
               moqr_bind_next_deadline_us(rig.bind) == deadline);
    RF_REQUIRE(retrieval_push_accept(&rig, &peers[0]));
    retrieval_push_pump(&rig, peers, 4);
    moqr_bind_debug_fwd_stats(rig.bind, &fwd);
    RF_REQUIRE(fwd.admitted == 1 && fwd.live == 1 && fwd.completed == 0 &&
               peers[0].cancelled == 0);

    RF_REQUIRE(retrieval_push_live(&rig, &peers[0], 0, 1));
    retrieval_push_pump(&rig, peers, 4);
    RF_REQUIRE(moqr_bind_retrieve_peek(rig.bind, &cursor, rig.now, &item) == MOQR_OK &&
               retrieval_frontier_object(&item, 1, true, 0xc3, 0x23));
    RF_REQUIRE(moqr_bind_retrieve_commit(rig.bind, &cursor) == MOQR_OK);
    committed++;
    RF_REQUIRE(moqr_bind_retrieve_peek(rig.bind, &cursor, rig.now, &item) ==
               MOQR_ERR_WOULD_BLOCK);
    RF_REQUIRE(moqr_bind_next_deadline_us(rig.bind) == deadline);
    RF_REQUIRE(retrieval_push_live(&rig, &peers[0], 0, 2));
    retrieval_push_pump(&rig, peers, 4);
    RF_REQUIRE(moqr_bind_retrieve_peek(rig.bind, &cursor, rig.now, &held) == MOQR_OK &&
               retrieval_frontier_object(&held, 2, true, 0xc3, 0x23));
    const uint8_t *held_payload = moq_rcbuf_data(held.object.payload);
    const uint8_t *held_properties = moq_rcbuf_data(held.object.properties);

    /* Both copies arrive after object 1 committed, while cached object 2 is
     * borrowed. No FIN yet: neither the original timer nor the borrow moves. */
    RF_REQUIRE(retrieval_frontier_write(&rig, &peers[0], 1, 0xc3, 0x23));
    RF_REQUIRE(retrieval_frontier_write(&rig, &peers[0], 2, 0xc3, 0x23));
    if (gap) {
        RF_REQUIRE(moq_session_write_fetch_range(peers[0].cn->peer, peers[0].fetch,
                     MOQ_FETCH_RANGE_NON_EXISTENT, 0, 3, rig.now) == MOQ_OK);
    } else {
        RF_REQUIRE(retrieval_frontier_write(&rig, &peers[0], 3, 0xa1, 0x31));
    }
    retrieval_push_pump(&rig, peers, 4);
    moqr_bind_debug_fwd_stats(rig.bind, &fwd);
    RF_REQUIRE(rig.failures == 0 && fwd.admitted == 1 && fwd.live == 1 &&
               fwd.completed == 0 && fwd.bytes_used != 0 && peers[0].cancelled == 0 &&
               moqr_bind_next_deadline_us(rig.bind) == deadline);
    RF_REQUIRE(moqr_bind_retrieve_peek(rig.bind, &cursor, rig.now, &again) == MOQR_OK &&
               retrieval_frontier_object(&again, 2, true, 0xc3, 0x23) &&
               again.object.payload == held.object.payload &&
               again.object.properties == held.object.properties &&
               moq_rcbuf_data(again.object.payload) == held_payload &&
               moq_rcbuf_data(again.object.properties) == held_properties);
    RF_REQUIRE(moqr_bind_retrieve_commit(rig.bind, &cursor) == MOQR_OK);
    committed++;
    RF_REQUIRE(moqr_bind_retrieve_peek(rig.bind, &cursor, rig.now, &item) ==
               MOQR_ERR_WOULD_BLOCK);
    RF_REQUIRE(rig.now < deadline && moqr_bind_next_deadline_us(rig.bind) == deadline);
    RF_REQUIRE(moq_session_end_fetch(peers[0].cn->peer, peers[0].fetch, rig.now) == MOQ_OK);
    retrieval_push_pump(&rig, peers, 6);
    moqr_bind_debug_fwd_stats(rig.bind, &fwd);
    RF_REQUIRE(fwd.completed == 1 && fwd.live == 1 && peers[0].cancelled == 0);
    RF_REQUIRE(moqr_bind_retrieve_peek(rig.bind, &cursor, rig.now, &item) == MOQR_OK);
    if (gap) {
        RF_REQUIRE(item.kind == MOQR_RETRIEVE_GAP &&
                   item.start.group == 0 && item.start.object == 3 &&
                   item.end.group == 0 && item.end.object == 3 &&
                   item.gap_kind == MOQ_FETCH_RANGE_NON_EXISTENT);
    } else {
        RF_REQUIRE(retrieval_frontier_object(&item, 3, false, 0xa1, 0x31));
    }
    RF_REQUIRE(moqr_bind_retrieve_commit(rig.bind, &cursor) == MOQR_OK);
    committed++;
    moqr_bind_debug_fwd_stats(rig.bind, &fwd);
    RF_REQUIRE(fwd.live == 0 && fwd.bytes_used == 0);
    RF_REQUIRE(moqr_bind_retrieve_peek(rig.bind, &cursor, rig.now, &item) == MOQR_OK &&
               retrieval_frontier_object(&item, 4, true, 0xc3, 0x23));
    RF_REQUIRE(moqr_bind_retrieve_commit(rig.bind, &cursor) == MOQR_OK);
    committed++;
    RF_REQUIRE(moqr_bind_retrieve_peek(rig.bind, &cursor, rig.now, &item) == MOQR_OK &&
               item.kind == MOQR_RETRIEVE_END && item.start.group == 0 &&
               item.start.object == 1 && item.end.group == 0 && item.end.object == 4);
    RF_REQUIRE(committed == 4 && moqr_bind_retrieve_commit(rig.bind, &cursor) == MOQR_OK);
    moqr_bind_retrieve_close(rig.bind, &cursor);
    retrieval_push_pump(&rig, peers, 6);
    moqr_bind_debug_fwd_stats(rig.bind, &fwd);
    moqr_core_get_stats(rig.core, &stats);
    RF_REQUIRE(rig.failures == 0 && rig.now < deadline && stats.ingested_total == 4 &&
               peers[0].requests == 1 && peers[0].cancelled == 0 &&
               fwd.admitted == 1 && fwd.completed == 1 && fwd.live == 0 &&
               fwd.bytes_used == 0 && fwd.cancelled_up == 0 && fwd.cancelled_down == 0 &&
               fwd.errors == 0 && fwd.resets == 0 && fwd.lost == 0 &&
               fwd.overflow == 0 && fwd.mismatch == 0);
cleanup:
    moqr_bind_retrieve_close(rig.bind, &cursor);
    moqr_bind_get_stats(rig.bind, &bs);
    R_CHECK(&rig, bs.session_errors == 0 && bs.ingest_refusals == 0);
    if (peers[0].cn != NULL) {
        R_CHECK(&rig, moq_session_state(peers[0].cn->peer) == MOQ_SESS_ESTABLISHED &&
                      moq_session_state(peers[0].cn->rsess) == MOQ_SESS_ESTABLISHED);
    }
    moq_rcbuf_decref(peers[0].retained);
    moq_rcbuf_decref(peers[0].properties);
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
#undef RF_REQUIRE
    printf("%s: retrieval_live_frontier(%s) committed=%u failures=%d\n",
           rig.failures ? "FAIL" : "PASS", row, committed, rig.failures);
    return rig.failures != 0;
}

static int
retrieval_frontier_suffix(bool gap)
{
    const char *row = gap ? "gap-over-live" : "objects-after-live";
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        printf("FIXTURE: retrieval_frontier_suffix(%s) rig create\n", row);
        return 2;
    }
    retrieval_push_peer_t peers[4] = {0};
    moqr_retrieve_cursor_t cursor = {0};
    moqr_retrieve_item_t item = {0};
    moqr_bind_fwd_debug_t fwd;
    moqr_bind_stats_t bs;
    moqr_core_stats_t stats;
    unsigned committed = 0;
#define RS_REQUIRE(expr)                                                      \
    do {                                                                     \
        if (!(expr)) {                                                       \
            printf("FAIL: retrieval_frontier_suffix(%s): %s kind=%u local=%d\n", \
                   row, #expr, (unsigned)item.kind, (int)item.local_result); \
            rig.failures++;                                                  \
            goto cleanup;                                                    \
        }                                                                    \
    } while (0)
    const uint8_t last = gap ? 5 : 3;
    peers[0].end_object = last;
    peers[0].from_zero = true;
    RS_REQUIRE(retrieval_push_origin(&rig, peers, 0, 0xa1));
    uint64_t deadline = 0;
    RS_REQUIRE(retrieval_frontier_open(&rig, 0, last, &cursor, &deadline));
    RS_REQUIRE(moqr_bind_retrieve_peek(rig.bind, &cursor, rig.now, &item) ==
               MOQR_ERR_WOULD_BLOCK);
    retrieval_push_pump(&rig, peers, 6);
    RS_REQUIRE(rig.failures == 0 && peers[0].requests == 1 &&
               moqr_bind_next_deadline_us(rig.bind) == deadline);
    RS_REQUIRE(retrieval_push_accept(&rig, &peers[0]));
    retrieval_push_pump(&rig, peers, 4);
    RS_REQUIRE(retrieval_push_live(&rig, &peers[0], 0, gap ? 3 : 0));
    retrieval_push_pump(&rig, peers, 4);
    if (gap) {
        /* Reopen the cache onto MISSING 0..2, before the new live object 3. */
        RS_REQUIRE(moqr_bind_retrieve_peek(rig.bind, &cursor, rig.now, &item) ==
                   MOQR_ERR_WOULD_BLOCK);
    } else {
        RS_REQUIRE(moqr_bind_retrieve_peek(rig.bind, &cursor, rig.now, &item) == MOQR_OK &&
                   retrieval_frontier_object(&item, 0, true, 0xc3, 0x23));
        RS_REQUIRE(moqr_bind_retrieve_commit(rig.bind, &cursor) == MOQR_OK);
        committed++;
    }
    moqr_bind_debug_fwd_stats(rig.bind, &fwd);
    RS_REQUIRE(fwd.admitted == 1 && fwd.live == 1 && fwd.completed == 0 &&
               peers[0].cancelled == 0 && moqr_bind_next_deadline_us(rig.bind) == deadline);

    /* No intervening peek: the next owner turn starts only after FETCH FIN.
     * Keep a cached suffix beyond the collector, plus a shorter cached gap
     * inside the collector in the gap row. */
    if (gap) {
        RS_REQUIRE(moq_session_write_fetch_range(peers[0].cn->peer, peers[0].fetch,
                     MOQ_FETCH_RANGE_NON_EXISTENT, 0, 4, rig.now) == MOQ_OK);
    } else {
        RS_REQUIRE(retrieval_frontier_write(&rig, &peers[0], 1, 0xa1, 0x31));
        RS_REQUIRE(retrieval_frontier_write(&rig, &peers[0], 2, 0xa1, 0x31));
    }
    RS_REQUIRE(moq_session_end_fetch(peers[0].cn->peer, peers[0].fetch, rig.now) == MOQ_OK);
    retrieval_push_pump(&rig, peers, 6);
    moqr_bind_debug_fwd_stats(rig.bind, &fwd);
    RS_REQUIRE(fwd.completed == 1 && fwd.live == 1);
    if (gap) {
        RS_REQUIRE(moqr_bind_retrieve_peek(rig.bind, &cursor, rig.now, &item) == MOQR_OK &&
                   item.kind == MOQR_RETRIEVE_GAP && item.start.group == 0 &&
                   item.start.object == 0 && item.end.group == 0 && item.end.object == 2 &&
                   item.gap_kind == MOQ_FETCH_RANGE_NON_EXISTENT);
        RS_REQUIRE(moqr_bind_retrieve_commit(rig.bind, &cursor) == MOQR_OK);
        committed++;
        RS_REQUIRE(moqr_bind_retrieve_peek(rig.bind, &cursor, rig.now, &item) == MOQR_OK &&
                   retrieval_frontier_object(&item, 3, true, 0xc3, 0x23));
        RS_REQUIRE(moqr_bind_retrieve_commit(rig.bind, &cursor) == MOQR_OK);
        committed++;
        RS_REQUIRE(moqr_bind_retrieve_peek(rig.bind, &cursor, rig.now, &item) == MOQR_OK &&
                   item.kind == MOQR_RETRIEVE_GAP && item.start.group == 0 &&
                   item.start.object == 4 && item.end.group == 0 && item.end.object == 4 &&
                   item.gap_kind == MOQ_FETCH_RANGE_NON_EXISTENT);
        RS_REQUIRE(moqr_bind_retrieve_commit(rig.bind, &cursor) == MOQR_OK);
        committed++;
    } else {
        for (uint8_t id = 1; id <= 2; id++) {
            RS_REQUIRE(moqr_bind_retrieve_peek(rig.bind, &cursor, rig.now, &item) == MOQR_OK &&
                       retrieval_frontier_object(&item, id, false, 0xa1, 0x31));
            RS_REQUIRE(moqr_bind_retrieve_commit(rig.bind, &cursor) == MOQR_OK);
            committed++;
        }
    }
    RS_REQUIRE(moqr_bind_retrieve_peek(rig.bind, &cursor, rig.now, &item) == MOQR_OK &&
               retrieval_frontier_object(&item, last, true, 0xc3, 0x23));
    RS_REQUIRE(moqr_bind_retrieve_commit(rig.bind, &cursor) == MOQR_OK);
    committed++;
    RS_REQUIRE(moqr_bind_retrieve_peek(rig.bind, &cursor, rig.now, &item) == MOQR_OK &&
               item.kind == MOQR_RETRIEVE_END && item.start.group == 0 &&
               item.start.object == 0 && item.end.group == 0 && item.end.object == last);
    RS_REQUIRE(committed == 4 && moqr_bind_retrieve_commit(rig.bind, &cursor) == MOQR_OK);
    moqr_bind_retrieve_close(rig.bind, &cursor);
    retrieval_push_pump(&rig, peers, 6);
    moqr_bind_debug_fwd_stats(rig.bind, &fwd);
    moqr_core_get_stats(rig.core, &stats);
    RS_REQUIRE(rig.failures == 0 && rig.now < deadline && stats.ingested_total == 2 &&
               peers[0].requests == 1 && peers[0].cancelled == 0 &&
               fwd.admitted == 1 && fwd.completed == 1 && fwd.live == 0 &&
               fwd.bytes_used == 0 && fwd.cancelled_up == 0 && fwd.mismatch == 0 &&
               fwd.errors == 0 && fwd.resets == 0 && fwd.lost == 0 && fwd.overflow == 0);
cleanup:
    moqr_bind_retrieve_close(rig.bind, &cursor);
    moqr_bind_get_stats(rig.bind, &bs);
    R_CHECK(&rig, bs.session_errors == 0 && bs.ingest_refusals == 0);
    if (peers[0].cn != NULL) {
        R_CHECK(&rig, moq_session_state(peers[0].cn->peer) == MOQ_SESS_ESTABLISHED &&
                      moq_session_state(peers[0].cn->rsess) == MOQ_SESS_ESTABLISHED);
    }
    moq_rcbuf_decref(peers[0].retained);
    moq_rcbuf_decref(peers[0].properties);
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
#undef RS_REQUIRE
    printf("%s: retrieval_frontier_suffix(%s) committed=%u failures=%d\n",
           rig.failures ? "FAIL" : "PASS", row, committed, rig.failures);
    return rig.failures != 0;
}

static int
retrieval_frontier_suffix_all(void)
{
    int objects = retrieval_frontier_suffix(false);
    if (objects == 2) return 2;
    int gap = retrieval_frontier_suffix(true);
    return gap == 2 ? 2 : objects != 0 || gap != 0;
}

typedef struct retrieval_frontier_trace {
    bool watching;
    unsigned opens;
    moq_stream_ref_t request_ref;
} retrieval_frontier_trace_t;

static void
retrieval_frontier_trace(void *ctx, const moq_sim_trace_record_t *record)
{
    retrieval_frontier_trace_t *trace = ctx;
    if (trace->watching && record->kind == MOQ_SIM_TRACE_ACTION &&
        record->from == MOQ_PERSPECTIVE_SERVER &&
        record->action_kind == MOQ_ACTION_OPEN_BIDI_STREAM &&
        record->struct_size >= offsetof(moq_sim_trace_record_t, stream_ref) +
                               sizeof(record->stream_ref)) {
        trace->opens++;
        trace->request_ref = record->stream_ref;
    }
}

static int
retrieval_response_frontier(bool commit_live)
{
    const char *row = commit_live ? "live-committed" : "unconsumed-control";
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        printf("FIXTURE: retrieval_response_frontier(%s) rig create\n", row);
        return 2;
    }
    retrieval_push_peer_t peers[4] = {0};
    retrieval_frontier_trace_t trace = {0};
    moqr_retrieve_cursor_t cursor = {0};
    moqr_retrieve_item_t item = {0};
    moq_action_t response = {0};
    moq_rcbuf_t *held_response = NULL;
    moqr_bind_fwd_debug_t fwd;
    moqr_bind_stats_t bs;
    moqr_core_stats_t stats;
    unsigned committed = 0;
#define RE_REQUIRE(expr)                                                      \
    do {                                                                     \
        if (!(expr)) {                                                       \
            printf("FAIL: retrieval_response_frontier(%s): %s "              \
                   "kind=%u error=%u local=%d\n", row, #expr,                \
                   (unsigned)item.kind, (unsigned)item.error_kind,           \
                   (int)item.local_result);                                 \
            rig.failures++;                                                  \
            goto cleanup;                                                    \
        }                                                                    \
    } while (0)
    moq_sim_trace_fn old_trace = relay_bind_test_trace_fn;
    void *old_ctx = relay_bind_test_trace_ctx;
    relay_bind_test_trace_fn = retrieval_frontier_trace;
    relay_bind_test_trace_ctx = &trace;
    peers[0].end_object = 10;
    bool origin_ok = retrieval_push_origin(&rig, peers, 0, 0xa1);
    relay_bind_test_trace_fn = old_trace;
    relay_bind_test_trace_ctx = old_ctx;
    RE_REQUIRE(origin_ok);
    trace.watching = true;
    uint64_t deadline = 0;
    RE_REQUIRE(retrieval_frontier_open(&rig, 1, 9, &cursor, &deadline));
    RE_REQUIRE(moqr_bind_retrieve_peek(rig.bind, &cursor, rig.now, &item) ==
               MOQR_ERR_WOULD_BLOCK);
    retrieval_push_pump(&rig, peers, 6);
    trace.watching = false;
    RE_REQUIRE(rig.failures == 0 && peers[0].requests == 1 && trace.opens == 1 &&
               moqr_bind_next_deadline_us(rig.bind) == deadline);
    moqr_bind_debug_fwd_stats(rig.bind, &fwd);
    const uint64_t initial_bytes = fwd.bytes_used;
    RE_REQUIRE(fwd.admitted == 1 && fwd.live == 1 && fwd.completed == 0);

    /* Save the actual first response action, not invented control bytes.
     * Its raw end 5 means inclusive end 4. SimPair still transports the real
     * FETCH_HEADER and object 5 while this request-stream response is held. */
    moq_accept_fetch_cfg_t accept;
    moq_accept_fetch_cfg_init(&accept);
    accept.end_object = 5;
    RE_REQUIRE(moq_session_accept_fetch(peers[0].cn->peer, peers[0].fetch,
                                        &accept, rig.now) == MOQ_OK);
    size_t n = 0;
    RE_REQUIRE(moq_session_poll_actions_ex(peers[0].cn->peer, &response, 1,
                                          sizeof(response), &n) == MOQ_OK && n == 1 &&
               response.kind == MOQ_ACTION_SEND_BIDI_STREAM &&
               !response.u.send_bidi_stream.fin && response.u.send_bidi_stream.len > 0 &&
               response.u.send_bidi_stream.len <= 128);
    RE_REQUIRE(moq_rcbuf_create(&a.vt, response.u.send_bidi_stream.data,
                                 response.u.send_bidi_stream.len, &held_response) == MOQ_OK);
    moq_action_cleanup(&response);
    RE_REQUIRE(retrieval_frontier_write(&rig, &peers[0], 5, 0xc3, 0x23));
    retrieval_push_pump(&rig, peers, 4);
    moqr_bind_debug_fwd_stats(rig.bind, &fwd);
    RE_REQUIRE(rig.failures == 0 && fwd.bytes_used > initial_bytes &&
               fwd.completed == 0 && fwd.mismatch == 0 && peers[0].cancelled == 0);
    if (commit_live) {
        for (uint8_t id = 1; id <= 5; id++) {
            RE_REQUIRE(retrieval_push_live(&rig, &peers[0], 0, id));
            retrieval_push_pump(&rig, peers, 4);
            RE_REQUIRE(moqr_bind_retrieve_peek(rig.bind, &cursor, rig.now, &item) == MOQR_OK &&
                       retrieval_frontier_object(&item, id, true, 0xc3, 0x23));
            RE_REQUIRE(moqr_bind_retrieve_commit(rig.bind, &cursor) == MOQR_OK);
            committed++;
        }
        moqr_bind_debug_fwd_stats(rig.bind, &fwd);
        RE_REQUIRE(committed == 5 && fwd.bytes_used == initial_bytes);
    }
    RE_REQUIRE(rig.now < deadline && moqr_bind_next_deadline_us(rig.bind) == deadline);
    /* The trace recorded the opener's actual local ref; the responder ref
     * in its action is deliberately not assumed equal across SimPair. */
    RE_REQUIRE(moq_session_on_bidi_stream_bytes(peers[0].cn->rsess, trace.request_ref,
                 moq_rcbuf_data(held_response), moq_rcbuf_len(held_response),
                 false, rig.now) == MOQ_OK);
    relay_bind_test_rig_bind_pump_once(&rig);
    RE_REQUIRE(moqr_bind_retrieve_peek(rig.bind, &cursor, rig.now, &item) == MOQR_OK &&
               item.kind == MOQR_RETRIEVE_ERROR && item.error_kind == MOQR_RETRIEVE_MALFORMED &&
               item.local_result == MOQR_ERR_INVAL);
    RE_REQUIRE(moqr_bind_retrieve_commit(rig.bind, &cursor) == MOQR_ERR_WRONG_STATE);
    moqr_bind_retrieve_close(rig.bind, &cursor);
    retrieval_push_pump(&rig, peers, 6);
    moqr_bind_debug_fwd_stats(rig.bind, &fwd);
    moqr_core_get_stats(rig.core, &stats);
    RE_REQUIRE(rig.failures == 0 && rig.now < deadline && peers[0].requests == 1 &&
               peers[0].cancelled == 1 && fwd.admitted == 1 && fwd.mismatch == 1 &&
               fwd.cancelled_up == 1 && fwd.completed == 0 && fwd.live == 0 &&
               fwd.bytes_used == 0 && fwd.errors == 0 && fwd.resets == 0 &&
               fwd.lost == 0 && fwd.overflow == 0 &&
               stats.ingested_total == (commit_live ? 7u : 2u));
    RE_REQUIRE(moq_session_end_fetch(peers[0].cn->peer, peers[0].fetch, rig.now) ==
               MOQ_ERR_STALE_HANDLE);
cleanup:
    moq_action_cleanup(&response);
    moqr_bind_retrieve_close(rig.bind, &cursor);
    moqr_bind_get_stats(rig.bind, &bs);
    R_CHECK(&rig, bs.session_errors == 0 && bs.ingest_refusals == 0);
    if (peers[0].cn != NULL) {
        R_CHECK(&rig, moq_session_state(peers[0].cn->peer) == MOQ_SESS_ESTABLISHED &&
                      moq_session_state(peers[0].cn->rsess) == MOQ_SESS_ESTABLISHED);
    }
    moq_rcbuf_decref(held_response);
    moq_rcbuf_decref(peers[0].retained);
    moq_rcbuf_decref(peers[0].properties);
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
#undef RE_REQUIRE
    printf("%s: retrieval_response_frontier(%s) committed=%u failures=%d\n",
           rig.failures ? "FAIL" : "PASS", row, committed, rig.failures);
    return rig.failures != 0;
}

static int
retrieval_response_frontier_all(void)
{
    int control = retrieval_response_frontier(false);
    if (control == 2) return 2;
    int committed = retrieval_response_frontier(true);
    return committed == 2 ? 2 : control != 0 || committed != 0;
}

static int
retrieval_live_frontier_all(void)
{
    int objects = retrieval_live_frontier(false);
    if (objects == 2) return 2;
    int gap = retrieval_live_frontier(true);
    if (gap == 2) return 2;
    int suffix = retrieval_frontier_suffix_all();
    if (suffix == 2) return 2;
    int response = retrieval_response_frontier_all();
    return response == 2 ? 2 : objects != 0 || gap != 0 || suffix != 0 || response != 0;
}

/* Observe the allocations containing returned bytes, without dereferencing a
 * borrow after its allocation has been freed. The normal allocator still owns
 * every allocation; this observer neither retains nor releases an extra ref. */
typedef struct retrieval_borrow_alloc {
    ca_t count;
    uintptr_t bytes[2];
    unsigned released[2];
} retrieval_borrow_alloc_t;

static void
retrieval_borrow_freed(retrieval_borrow_alloc_t *a, uintptr_t base, size_t n)
{
    for (size_t i = 0; i < 2; i++) {
        if (a->bytes[i] != 0 && a->released[i] == 0 &&
            a->bytes[i] >= base && a->bytes[i] - base < n) {
            a->released[i] = 1;
        }
    }
}

static void *
retrieval_borrow_alloc(size_t n, void *ctx)
{
    retrieval_borrow_alloc_t *a = ctx;
    return relay_bind_test_ca_a(n, &a->count);
}

static void *
retrieval_borrow_realloc(void *p, size_t old, size_t n, void *ctx)
{
    retrieval_borrow_alloc_t *a = ctx;
    uintptr_t base = (uintptr_t)p;
    void *q = relay_bind_test_ca_r(p, old, n, &a->count);
    if (q != NULL && (uintptr_t)q != base) retrieval_borrow_freed(a, base, old);
    return q;
}

static void
retrieval_borrow_free(void *p, size_t n, void *ctx)
{
    retrieval_borrow_alloc_t *a = ctx;
    if (p != NULL) retrieval_borrow_freed(a, (uintptr_t)p, n);
    relay_bind_test_ca_f(p, n, &a->count);
}

/* Exercise only the private cached-borrow contract. The caller owns this core
 * and consumes its own scalar intents; no wire ACK or metadata is synthesized. */
static int
retrieval_cached_borrow(bool forget)
{
    const char *row = forget ? "destroy-forget" : "close";
    static const uint8_t body[] = { 0x42, 0x00, 0xe7, 0x19, 0x7f, 0xfe };
    static const uint8_t properties[] = { 0x7c, 0x35 };
    retrieval_borrow_alloc_t a;
    memset(&a, 0, sizeof(a));
    relay_bind_test_ca_init(&a.count);
    a.count.vt.ctx = &a;
    a.count.vt.alloc = retrieval_borrow_alloc;
    a.count.vt.realloc = retrieval_borrow_realloc;
    a.count.vt.free = retrieval_borrow_free;
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a.count) != MOQR_OK) {
        printf("FIXTURE: retrieval_cached_borrow(%s) rig create\n", row);
        return 2;
    }
    moqr_retrieve_cursor_t cursor = {0};
    moq_rcbuf_t *payload = NULL, *props = NULL;
    unsigned premature[2] = {0, 0};
    int release_failures = 0;
#define RB_REQUIRE(expr)                                                      \
    do {                                                                     \
        if (!(expr)) {                                                       \
            printf("FIXTURE: retrieval_cached_borrow(%s): %s\n", row, #expr); \
            rig.failures++;                                                  \
            goto cleanup;                                                    \
        }                                                                    \
    } while (0)
    moqr_binding_t origin = MOQR_BINDING_INVALID, next = MOQR_BINDING_INVALID;
    moqr_binding_t consumer = MOQR_BINDING_INVALID;
    RB_REQUIRE(moqr_core_binding_open(rig.core, 801, &origin) == MOQR_OK);
    RB_REQUIRE(moqr_core_binding_open(rig.core, 802, &next) == MOQR_OK);
    RB_REQUIRE(moqr_core_binding_open(rig.core, 803, &consumer) == MOQR_OK);
    moq_bytes_t parts[] = { B("held"), B("borrow") };
    moqr_ns_t ns = { .parts = parts, .count = 2 };
    moqr_track_t track = MOQR_TRACK_INVALID, fresh = MOQR_TRACK_INVALID;
    RB_REQUIRE(moqr_core_publish_open(rig.core, origin, ns, B("v"),
                                      901, &track) == MOQR_OK);
    moqr_intent_t intent;
    RB_REQUIRE(moqr_core_poll_intents(rig.core, &intent, 1) == 1 &&
               intent.kind == MOQR_INTENT_ACCEPT_PUBLISH &&
               intent.binding_cookie == 801 && intent.cookie == 901 &&
               moqr_track_eq(intent.track, track) && intent.track_gen != 0);
    uint64_t generation = intent.track_gen;
    RB_REQUIRE(moq_rcbuf_create(&a.count.vt, body, sizeof(body), &payload) == MOQ_OK);
    RB_REQUIRE(moq_rcbuf_create(&a.count.vt, properties, sizeof(properties),
                                &props) == MOQ_OK);
    moqr_log_append_desc_t desc;
    moqr_log_append_desc_init(&desc);
    desc.payload = payload;
    desc.properties = props;
    desc.publisher_priority = 73;
    desc.now_us = rig.now;
    RB_REQUIRE(moqr_core_ingest(rig.core, track, &desc) == MOQR_OK);
    payload = NULL;
    props = NULL;
    moqr_subscribe_req_t sr;
    moqr_subscribe_req_init(&sr);
    sr.ns = ns;
    sr.name = B("v");
    sr.cookie = 902;
    sr.group_order = MOQR_GROUP_ORDER_ASCENDING;
    moqr_sub_t parent = MOQR_SUB_INVALID;
    RB_REQUIRE(moqr_core_subscribe(rig.core, consumer, &sr, &parent) == MOQR_OK);
    RB_REQUIRE(moqr_core_poll_intents(rig.core, &intent, 1) == 1 &&
               intent.kind == MOQR_INTENT_ACCEPT_SUB &&
               intent.sub._opaque == parent._opaque && intent.cookie == 902);
    moqr_retrieve_request_t request = {0};
    request.binding = consumer;
    request.parent = parent;
    request.track = track;
    request.track_gen = generation;
    request.source = 803;
    request.demand = parent._opaque;
    request.child = 903;
    request.group_order = MOQR_GROUP_ORDER_ASCENDING;
    request.subscriber_priority = 61;
    request.deadline_us = rig.now + 100000;
    RB_REQUIRE(moqr_bind_retrieve_init(rig.bind, &cursor, &request) == MOQR_OK);
    moqr_retrieve_item_t held, again;
    RB_REQUIRE(moqr_bind_retrieve_peek(rig.bind, &cursor, rig.now, &held) == MOQR_OK &&
               held.kind == MOQR_RETRIEVE_OBJECT && held.object.group_id == 0 &&
               held.object.object_id == 0 && held.object.publisher_priority == 73 &&
               held.object.payload != NULL && held.object.properties != NULL);
    RB_REQUIRE(moq_rcbuf_len(held.object.payload) == sizeof(body) &&
               moq_rcbuf_len(held.object.properties) == sizeof(properties));
    const uint8_t *borrowed_payload = moq_rcbuf_data(held.object.payload);
    const uint8_t *borrowed_properties = moq_rcbuf_data(held.object.properties);
    a.bytes[0] = (uintptr_t)borrowed_payload;
    a.bytes[1] = (uintptr_t)borrowed_properties;
    RB_REQUIRE(memcmp(borrowed_payload, body, sizeof(body)) == 0 &&
               memcmp(borrowed_properties, properties, sizeof(properties)) == 0);
    RB_REQUIRE(moqr_bind_retrieve_peek(rig.bind, &cursor, rig.now + 1, &again) == MOQR_OK &&
               again.kind == MOQR_RETRIEVE_OBJECT &&
               again.object.payload == held.object.payload &&
               again.object.properties == held.object.properties);
    RB_REQUIRE(moqr_core_upstream_lost(rig.core, track, generation) == MOQR_OK);
    RB_REQUIRE(moqr_core_publish_open(rig.core, next, ns, B("v"),
                                      904, &fresh) == MOQR_OK &&
               !moqr_track_eq(fresh, track));
    RB_REQUIRE(moqr_bind_retrieve_peek(rig.bind, &cursor, rig.now + 2, &again) == MOQR_OK &&
               again.kind == MOQR_RETRIEVE_ERROR &&
               again.error_kind == MOQR_RETRIEVE_SOURCE_LOST &&
               again.local_result == MOQR_ERR_STALE_HANDLE);
    premature[0] = a.released[0];
    premature[1] = a.released[1];
    /* Never read the baseline's freed memory just to demonstrate the defect. */
    if (premature[0] == 0) RB_REQUIRE(memcmp(borrowed_payload, body, sizeof(body)) == 0);
    if (premature[1] == 0) RB_REQUIRE(memcmp(borrowed_properties, properties,
                                            sizeof(properties)) == 0);
    RB_REQUIRE(moqr_bind_retrieve_commit(rig.bind, &cursor) == MOQR_ERR_WRONG_STATE);
    if (forget) {
        /* Quiesced manager ordering: bind owns retained copies, then manager
         * forgets its cursor, then core destroys its remaining cursor pins. */
        moqr_bind_destroy(rig.bind);
        rig.bind = NULL;
        release_failures = a.released[0] != 1 || a.released[1] != 1;
        moqr_bind_retrieve_forget(&cursor);
    } else {
        moqr_bind_retrieve_close(rig.bind, &cursor);
        release_failures = a.released[0] != 1 || a.released[1] != 1;
    }
    RB_REQUIRE(!cursor.live);
cleanup:
    if (rig.bind != NULL) moqr_bind_retrieve_close(rig.bind, &cursor);
    else moqr_bind_retrieve_forget(&cursor);
    moq_rcbuf_decref(payload);
    moq_rcbuf_decref(props);
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.count.live == 0);
#undef RB_REQUIRE
    int failed = premature[0] != 0 || premature[1] != 0 || release_failures != 0;
    printf("%s: retrieval_cached_borrow(%s) premature_payload=%u "
           "premature_properties=%u release_failures=%d fixture_failures=%d\n",
           failed || rig.failures ? "FAIL" : "PASS", row,
           premature[0], premature[1], release_failures, rig.failures);
    return rig.failures != 0 ? 2 : failed;
}

static int
retrieval_cached_borrow_all(void)
{
    int normal = retrieval_cached_borrow(false);
    if (normal == 2) return 2;
    int quiesced = retrieval_cached_borrow(true);
    return quiesced == 2 ? 2 : normal != 0 || quiesced != 0;
}

/* Observe exact-sized clone allocations and their release without changing
 * the allocator's existing one-shot failure policy or retaining extra refs. */
typedef struct retrieval_refusal_alloc {
    ca_t count;
    bool watching;
    size_t sizes[2];
    moq_bytes_t expected[2];
    uintptr_t copies[2];
    unsigned attempts[2], releases[2];
    bool released_exact[2];
} retrieval_refusal_alloc_t;

static void *
retrieval_refusal_alloc(size_t n, void *ctx)
{
    retrieval_refusal_alloc_t *a = ctx;
    void *p = relay_bind_test_ca_a(n, &a->count);
    for (size_t i = 0; a->watching && i < 2; i++) {
        if (n == a->sizes[i]) {
            a->attempts[i]++;
            if (p != NULL) a->copies[i] = (uintptr_t)p;
        }
    }
    return p;
}

static void *
retrieval_refusal_realloc(void *p, size_t old, size_t n, void *ctx)
{
    retrieval_refusal_alloc_t *a = ctx;
    return relay_bind_test_ca_r(p, old, n, &a->count);
}

static void
retrieval_refusal_free(void *p, size_t n, void *ctx)
{
    retrieval_refusal_alloc_t *a = ctx;
    for (size_t i = 0; a->watching && p != NULL && i < 2; i++) {
        if ((uintptr_t)p == a->copies[i]) {
            a->releases[i]++;
            /* Root clone allocations end in the visible bytes. Compare
             * while allocated, never dereference a previously freed copy. */
            a->released_exact[i] = n == a->sizes[i] && n >= a->expected[i].len &&
                memcmp((const uint8_t *)p + n - a->expected[i].len,
                       a->expected[i].data, a->expected[i].len) == 0;
        }
    }
    relay_bind_test_ca_f(p, n, &a->count);
}

static void
retrieval_refusal_watch(retrieval_refusal_alloc_t *a)
{
    memset(a->copies, 0, sizeof(a->copies));
    memset(a->attempts, 0, sizeof(a->attempts));
    memset(a->releases, 0, sizeof(a->releases));
    memset(a->released_exact, 0, sizeof(a->released_exact));
    a->watching = true;
}

static int
retrieval_cached_refusal(unsigned mode)
{
    static const char *const rows[] = { "payload-oom", "properties-oom", "capacity" };
    static const uint8_t body[] = { 0x42, 0x00, 0xe7, 0x19, 0x7f, 0xfe, 1, 2, 3, 4, 5 };
    static const uint8_t properties[] = { 0x20, 0x40, 0x40 };
    const char *row = rows[mode];
    retrieval_refusal_alloc_t a;
    memset(&a, 0, sizeof(a));
    relay_bind_test_ca_init(&a.count);
    a.count.vt.ctx = &a;
    a.count.vt.alloc = retrieval_refusal_alloc;
    a.count.vt.realloc = retrieval_refusal_realloc;
    a.count.vt.free = retrieval_refusal_free;
    a.expected[0] = (moq_bytes_t){ body, sizeof(body) };
    a.expected[1] = (moq_bytes_t){ properties, sizeof(properties) };
    if (moq_rcbuf_allocation_size(sizeof(body), &a.sizes[0]) != MOQ_OK ||
        moq_rcbuf_allocation_size(sizeof(properties), &a.sizes[1]) != MOQ_OK) return 2;
    uint64_t bytes = (uint64_t)a.sizes[0] + (uint64_t)a.sizes[1];
    relay_bind_test_fetch_forward_bytes = bytes;
    relay_bind_test_max_fetches = mode == 2 ? 2u : 1u;
    rig_t rig;
    moqr_result_t created = relay_bind_test_rig_create(&rig, &a.count);
    relay_bind_test_fetch_forward_bytes = 0;
    relay_bind_test_max_fetches = 0;
    if (created != MOQR_OK) {
        printf("FIXTURE: retrieval_cached_refusal(%s) rig create\n", row);
        return 2;
    }
    moqr_retrieve_cursor_t cursor = {0}, holder = {0};
    moq_rcbuf_t *payload = NULL, *props = NULL;
    moqr_bind_fwd_debug_t stats;
#define RR_REQUIRE(expr)                                                      \
    do {                                                                     \
        if (!(expr)) {                                                       \
            printf("FAIL: retrieval_cached_refusal(%s): %s\n", row, #expr); \
            rig.failures++;                                                  \
            goto cleanup;                                                    \
        }                                                                    \
    } while (0)
    moqr_binding_t origin = MOQR_BINDING_INVALID, consumer = MOQR_BINDING_INVALID;
    RR_REQUIRE(moqr_core_binding_open(rig.core, 811, &origin) == MOQR_OK);
    RR_REQUIRE(moqr_core_binding_open(rig.core, 812, &consumer) == MOQR_OK);
    moq_bytes_t parts[] = { B("held"), B("refusal") };
    moqr_ns_t ns = { .parts = parts, .count = 2 };
    moqr_track_t track = MOQR_TRACK_INVALID;
    RR_REQUIRE(moqr_core_publish_open(rig.core, origin, ns, B("v"), 911, &track) == MOQR_OK);
    moqr_intent_t intent;
    RR_REQUIRE(moqr_core_poll_intents(rig.core, &intent, 1) == 1 &&
               intent.kind == MOQR_INTENT_ACCEPT_PUBLISH &&
               moqr_track_eq(intent.track, track) && intent.track_gen != 0);
    uint64_t generation = intent.track_gen;
    RR_REQUIRE(moq_rcbuf_create(&a.count.vt, body, sizeof(body), &payload) == MOQ_OK);
    RR_REQUIRE(moq_rcbuf_create(&a.count.vt, properties, sizeof(properties), &props) == MOQ_OK);
    const moq_rcbuf_t *retained_payload = payload, *retained_props = props;
    moqr_log_append_desc_t desc;
    moqr_log_append_desc_init(&desc);
    desc.payload = payload;
    desc.properties = props;
    desc.publisher_priority = 73;
    desc.now_us = rig.now;
    RR_REQUIRE(moqr_core_ingest(rig.core, track, &desc) == MOQR_OK);
    payload = NULL;
    props = NULL;
    moqr_subscribe_req_t sr;
    moqr_subscribe_req_init(&sr);
    sr.ns = ns;
    sr.name = B("v");
    sr.cookie = 912;
    sr.group_order = MOQR_GROUP_ORDER_ASCENDING;
    moqr_sub_t parent = MOQR_SUB_INVALID;
    RR_REQUIRE(moqr_core_subscribe(rig.core, consumer, &sr, &parent) == MOQR_OK);
    RR_REQUIRE(moqr_core_poll_intents(rig.core, &intent, 1) == 1 &&
               intent.kind == MOQR_INTENT_ACCEPT_SUB && intent.sub._opaque == parent._opaque);
    moqr_retrieve_request_t request = {0};
    request.binding = consumer;
    request.parent = parent;
    request.track = track;
    request.track_gen = generation;
    request.source = 812;
    request.demand = parent._opaque;
    request.child = 913;
    request.group_order = MOQR_GROUP_ORDER_ASCENDING;
    request.deadline_us = rig.now + 100000;
    retrieval_refusal_watch(&a);
    moqr_retrieve_item_t held = {0}, item = {0}, again = {0};
    if (mode == 2) {
        RR_REQUIRE(moqr_bind_retrieve_init(rig.bind, &holder, &request) == MOQR_OK);
        RR_REQUIRE(moqr_bind_retrieve_peek(rig.bind, &holder, rig.now, &held) == MOQR_OK &&
                   held.kind == MOQR_RETRIEVE_OBJECT &&
                   held.object.payload != retained_payload && held.object.properties != retained_props);
        request.child++;
    } else {
        /* Distinct odd data lengths select the rcbuf allocations, not the
         * core's fixed-size fetch plan. The property failure follows one
         * successful payload allocation, whose exact bytes/free are checked. */
        a.count.fail_size = a.sizes[mode];
    }
    RR_REQUIRE(moqr_bind_retrieve_init(rig.bind, &cursor, &request) == MOQR_OK);
    RR_REQUIRE(moqr_bind_retrieve_peek(rig.bind, &cursor, rig.now, &item) == MOQR_OK &&
               item.kind == MOQR_RETRIEVE_ERROR && item.error_kind == MOQR_RETRIEVE_LOCAL &&
               item.local_result == (mode == 2 ? MOQR_ERR_CAPACITY : MOQR_ERR_NOMEM) &&
               item.object.payload == NULL && item.object.properties == NULL);
    RR_REQUIRE(cursor.cache_peeked && cursor.cache_item.kind == MOQR_FETCH_ITEM_OBJECT &&
               cursor.cached_slot == 0 && cursor.cached_generation == 0);
    RR_REQUIRE(a.count.fail_size == 0 && a.attempts[0] == 1 &&
               a.attempts[1] == (mode == 0 ? 0u : 1u));
    RR_REQUIRE(a.releases[0] == (mode == 1 ? 1u : 0u) && a.releases[1] == 0);
    if (mode == 0) RR_REQUIRE(a.copies[0] == 0 && a.copies[1] == 0);
    if (mode == 1) RR_REQUIRE(a.copies[0] != 0 && a.copies[1] == 0 && a.released_exact[0]);
    moqr_bind_debug_fwd_stats(rig.bind, &stats);
    RR_REQUIRE(stats.bytes_cap == bytes && stats.bytes_used == (mode == 2 ? bytes : 0) &&
               stats.bytes_high == bytes && stats.live == 0);
    long attempts = a.count.attempts;
    RR_REQUIRE(moqr_bind_retrieve_peek(rig.bind, &cursor, rig.now + 1, &again) == MOQR_OK &&
               again.kind == item.kind && again.error_kind == item.error_kind &&
               again.local_result == item.local_result && a.count.attempts == attempts);
    RR_REQUIRE(moqr_bind_retrieve_commit(rig.bind, &cursor) == MOQR_ERR_WRONG_STATE);
    moqr_bind_retrieve_close(rig.bind, &cursor);
    if (mode == 2) {
        RR_REQUIRE(moqr_bind_retrieve_peek(rig.bind, &holder, rig.now + 1, &again) == MOQR_OK &&
                   again.object.payload == held.object.payload && again.object.properties == held.object.properties &&
                   moq_rcbuf_len(again.object.payload) == sizeof(body) &&
                   moq_rcbuf_len(again.object.properties) == sizeof(properties) &&
                   memcmp(moq_rcbuf_data(again.object.payload), body, sizeof(body)) == 0 &&
                   memcmp(moq_rcbuf_data(again.object.properties), properties, sizeof(properties)) == 0);
        RR_REQUIRE(moqr_bind_retrieve_commit(rig.bind, &holder) == MOQR_OK &&
                   a.releases[0] == 1 && a.releases[1] == 1 &&
                   a.released_exact[0] && a.released_exact[1]);
        moqr_bind_retrieve_close(rig.bind, &holder);
    }
    moqr_bind_debug_fwd_stats(rig.bind, &stats);
    RR_REQUIRE(stats.bytes_used == 0 && stats.live == 0);

    /* Reuse the full unchanged byte cap after refusal. This must return new
     * owned bytes, release them exactly once on commit, then reach END. */
    retrieval_refusal_watch(&a);
    request.child++;
    RR_REQUIRE(moqr_bind_retrieve_init(rig.bind, &cursor, &request) == MOQR_OK);
    RR_REQUIRE(moqr_bind_retrieve_peek(rig.bind, &cursor, rig.now + 2, &item) == MOQR_OK &&
               item.kind == MOQR_RETRIEVE_OBJECT && item.object.group_id == 0 &&
               item.object.object_id == 0 && item.object.publisher_priority == 73 &&
               item.object.payload != retained_payload && item.object.properties != retained_props &&
               moq_rcbuf_len(item.object.payload) == sizeof(body) &&
               moq_rcbuf_len(item.object.properties) == sizeof(properties) &&
               memcmp(moq_rcbuf_data(item.object.payload), body, sizeof(body)) == 0 &&
               memcmp(moq_rcbuf_data(item.object.properties), properties, sizeof(properties)) == 0);
    RR_REQUIRE((uintptr_t)item.object.payload == a.copies[0] &&
               (uintptr_t)item.object.properties == a.copies[1] &&
               a.attempts[0] == 1 && a.attempts[1] == 1 &&
               a.releases[0] == 0 && a.releases[1] == 0);
    moqr_bind_debug_fwd_stats(rig.bind, &stats);
    RR_REQUIRE(stats.bytes_used == bytes && stats.bytes_high == bytes && stats.live == 0);
    RR_REQUIRE(moqr_bind_retrieve_commit(rig.bind, &cursor) == MOQR_OK &&
               a.releases[0] == 1 && a.releases[1] == 1 &&
               a.released_exact[0] && a.released_exact[1]);
    moqr_bind_debug_fwd_stats(rig.bind, &stats);
    RR_REQUIRE(stats.bytes_used == 0);
    RR_REQUIRE(moqr_bind_retrieve_peek(rig.bind, &cursor, rig.now + 3, &again) == MOQR_OK &&
               again.kind == MOQR_RETRIEVE_END &&
               moqr_bind_retrieve_commit(rig.bind, &cursor) == MOQR_OK);
cleanup:
    a.count.fail_size = 0;
    moqr_bind_retrieve_close(rig.bind, &cursor);
    moqr_bind_retrieve_close(rig.bind, &holder);
    a.watching = false;
    moq_rcbuf_decref(payload);
    moq_rcbuf_decref(props);
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.count.live == 0);
#undef RR_REQUIRE
    printf("%s: retrieval_cached_refusal(%s) failures=%d\n",
           rig.failures ? "FAIL" : "PASS", row, rig.failures);
    return rig.failures != 0;
}

static int
retrieval_cached_refusal_all(void)
{
    for (unsigned mode = 0; mode < 3; mode++) {
        int rc = retrieval_cached_refusal(mode);
        if (rc != 0) return rc;
    }
    return 0;
}

/* Real wire admission under the unchanged metadata budget. Existing
 * managed-import operations fill the directory; no private layout is read. */
typedef struct metadata_lifetime_peer {
    moq_publication_t pending;
    unsigned setups, accepted, refused;
    uint64_t error;
} metadata_lifetime_peer_t;

static void
metadata_lifetime_drain(rig_t *r, conn_t *cn, metadata_lifetime_peer_t *p)
{
    moq_event_t e;
    while (moq_session_poll_events(cn->peer, &e, 1) != 0) {
        switch (e.kind) {
        case MOQ_EVENT_SETUP_COMPLETE:
            p->setups++;
            break;
        case MOQ_EVENT_REQUEST_READY:
            break;
        case MOQ_EVENT_PUBLISH_OK:
            R_CHECK(r, e.detail_size >= sizeof(moq_publish_ok_event_t) &&
                       e.u.publish_ok.pub._opaque == p->pending._opaque &&
                       e.u.publish_ok.send_allowed);
            p->accepted++;
            break;
        case MOQ_EVENT_PUBLISH_ERROR:
            R_CHECK(r, e.detail_size >= sizeof(moq_publish_error_event_t) &&
                       e.u.publish_error.pub._opaque == p->pending._opaque);
            p->refused++;
            p->error = e.u.publish_error.error_code;
            break;
        default:
            printf("FIXTURE: metadata_lifetime unexpected event=%d\n", (int)e.kind);
            r->failures++;
            break;
        }
        moq_event_cleanup(&e);
    }
}

/* A paused public-core subscriber observes the already admitted identity.
 * It neither manufactures a source nor inserts metadata. */
static moqr_result_t
metadata_lifetime_track(rig_t *r, moqr_binding_t observer, moqr_ns_t ns,
                        moq_bytes_t name, moqr_track_t *track, uint64_t *gen)
{
    moqr_subscribe_req_t req;
    moqr_subscribe_req_init(&req);
    req.ns = ns;
    req.name = name;
    req.forward = false;
    moqr_sub_t sub = MOQR_SUB_INVALID;
    moqr_result_t rc = moqr_core_subscribe(r->core, observer, &req, &sub);
    if (rc != MOQR_OK) return rc;
    rc = moqr_core_sub_track(r->core, sub, track, gen);
    moqr_result_t closed = moqr_core_unsubscribe(r->core, sub, r->now);
    relay_bind_test_rig_bind_pump_once(r);
    return rc == MOQR_OK ? closed : rc;
}

/* Modes: 0 dead directory; 1 WARM must remain charged; 2 manager pin must
 * remain charged after directory retirement, then release allows recovery.
 * 0=pass, 1=exact missing recovery, 2=fixture/control/cleanup failure. */
static int
metadata_lifetime(unsigned mode)
{
    enum { COUNT = 16, PROPERTY_BYTES = 65500 };
    const char *row = mode == 0 ? "dead" : mode == 1 ? "warm" : "pin";
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        printf("FIXTURE: metadata_lifetime(%s) create\n", row);
        return 2;
    }
    conn_t *pub = NULL;
    metadata_lifetime_peer_t peer = {0};
    moqr_binding_t observer = MOQR_BINDING_INVALID;
    moq_publication_t pubs[COUNT] = {0};
    moqr_track_t tracks[COUNT] = {0};
    uint64_t generations[COUNT] = {0};
    moqr_bind_meta_ref_t latest = {0}, held = {0};
    moq_rcbuf_t *payload = NULL;
    uint8_t *large = NULL;
    uint8_t probe[2003];
    char names[COUNT][16];
    unsigned attempted = 0;
    int recovery_failure = 0;
    moq_bytes_t parts[] = { B("lifetime") };
    moqr_ns_t ns = { .parts = parts, .count = 1 };
    moqr_core_stats_t cs;
    moqr_bind_stats_t bs;

#define ML_REQUIRE(expr)                                                   \
    do {                                                                   \
        if (!(expr)) {                                                     \
            printf("FIXTURE: metadata_lifetime(%s): %s\n", row, #expr);      \
            rig.failures++;                                                \
            goto cleanup;                                                  \
        }                                                                  \
    } while (0)

    ML_REQUIRE(mode <= 2);
    /* Even excluding allocation headers, another 2003-byte set cannot fit. */
    ML_REQUIRE((uint64_t)COUNT * PROPERTY_BYTES + sizeof(probe) >
               MOQR_BIND_META_BYTES_MAX);
    large = a.vt.alloc(PROPERTY_BYTES, a.vt.ctx);
    ML_REQUIRE(large != NULL);
    /* Unknown nonmandatory property 0x79; vi64 length 65496. */
    large[0] = 0x79; large[1] = 0xc0; large[2] = 0xff; large[3] = 0xd8;
    memset(large + 4, 0x5a, PROPERTY_BYTES - 4);
    probe[0] = 0x79; probe[1] = 0x87; probe[2] = 0xd0;
    memset(probe + 3, 0x6b, sizeof(probe) - 3);
    pub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_22);
    ML_REQUIRE(pub != NULL);
    relay_bind_test_rig_pump(&rig, 4);
    metadata_lifetime_drain(&rig, pub, &peer);
    ML_REQUIRE(peer.setups == 1 && rig.failures == 0);
    ML_REQUIRE(moqr_core_binding_open(rig.core, 900, &observer) == MOQR_OK);

    for (unsigned i = 0; i < COUNT; i++) {
        int n = snprintf(names[i], sizeof(names[i]), "track%u", i);
        ML_REQUIRE(n > 0 && (size_t)n < sizeof(names[i]));
        moq_publish_cfg_t pc;
        moq_publish_cfg_init(&pc);
        pc.track_namespace = (moq_namespace_t){ .parts = parts, .count = 1 };
        pc.track_name = B(names[i]);
        pc.track_properties = (moq_bytes_t){ .data = d22_tp_expected,
                                           .len = sizeof(d22_tp_expected) };
        ML_REQUIRE(moq_session_publish(pub->peer, &pc, rig.now, &pubs[i]) == MOQ_OK);
        peer.pending = pubs[i];
        relay_bind_test_rig_pump(&rig, 6);
        metadata_lifetime_drain(&rig, pub, &peer);
        ML_REQUIRE(peer.accepted == i + 1u && peer.refused == 0 && rig.failures == 0);
        ML_REQUIRE(metadata_lifetime_track(&rig, observer, ns, B(names[i]),
                   &tracks[i], &generations[i]) == MOQR_OK);
        ML_REQUIRE(moqr_bind_meta_latest(rig.bind, tracks[i], &latest) == MOQR_OK);
        ML_REQUIRE(latest.state == MOQR_BIND_META_PRESENT &&
                   latest.wire_version == MOQ_VERSION_DRAFT_22 &&
                   latest.source_binding._opaque != 0 &&
                   latest.source_generation == generations[i] &&
                   latest.bytes.len == sizeof(d22_tp_expected) &&
                   memcmp(latest.bytes.data, d22_tp_expected, latest.bytes.len) == 0);
        moqr_bind_meta_ref_t imported = latest;
        imported.bytes = (moq_bytes_t){ .data = large, .len = PROPERTY_BYTES };
        uint64_t revision = 0;
        ML_REQUIRE(moqr_bind_meta_replace(rig.bind, tracks[i], generations[i],
                   &imported, rig.now, &revision) == MOQR_OK);
        ML_REQUIRE(revision > latest.revision);
        moqr_bind_meta_release(rig.bind, &latest);
    }
    moqr_core_get_stats(rig.core, &cs);
    ML_REQUIRE(cs.tracks == COUNT && cs.subs == 0);
    ML_REQUIRE(moqr_bind_meta_latest(rig.bind, tracks[0], &latest) == MOQR_OK);
    ML_REQUIRE(latest.state == MOQR_BIND_META_PRESENT &&
               latest.bytes.len == PROPERTY_BYTES &&
               memcmp(latest.bytes.data, large, PROPERTY_BYTES) == 0);
    uint64_t old_revision = latest.revision;
    if (mode == 2)
        ML_REQUIRE(moqr_bind_meta_retain(rig.bind, &latest, &held) == MOQR_OK);
    moqr_bind_meta_release(rig.bind, &latest);

    if (mode == 1) {
        ML_REQUIRE(moq_rcbuf_create(&a.vt, direct_core_payload,
                   sizeof(direct_core_payload), &payload) == MOQ_OK);
        moq_result_t sent = moq_session_send_pub_object_datagram(
            pub->peer, pubs[0], 7, 0, 17, false, payload, NULL, 0, rig.now);
        moq_rcbuf_decref(payload);
        payload = NULL;
        ML_REQUIRE(sent == MOQ_OK);
        relay_bind_test_rig_pump(&rig, 6);
        metadata_lifetime_drain(&rig, pub, &peer);
        bool has = false;
        uint64_t group = 0, object = 0;
        ML_REQUIRE(moqr_core_track_largest(rig.core, tracks[0], generations[0],
                   &has, &group, &object) == MOQR_OK && has && group == 7 && object == 0);
    }
    moq_finish_publish_cfg_t done;
    moq_finish_publish_cfg_init(&done);
    ML_REQUIRE(moq_session_finish_publish(pub->peer, pubs[0], &done, rig.now) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 6);
    metadata_lifetime_drain(&rig, pub, &peer);
    moqr_core_get_stats(rig.core, &cs);
    ML_REQUIRE(cs.tracks == (mode == 1 ? COUNT : COUNT - 1u) && cs.subs == 0);
    ML_REQUIRE(peer.accepted == COUNT && peer.refused == 0 && rig.failures == 0);

    if (mode == 1) {
        moqr_track_t warm = MOQR_TRACK_INVALID;
        uint64_t gen = 0;
        ML_REQUIRE(metadata_lifetime_track(&rig, observer, ns, B(names[0]),
                   &warm, &gen) == MOQR_OK);
        ML_REQUIRE(moqr_track_eq(warm, tracks[0]) && gen != generations[0]);
        ML_REQUIRE(moqr_bind_meta_latest(rig.bind, warm, &latest) == MOQR_OK);
        ML_REQUIRE(latest.state == MOQR_BIND_META_PRESENT &&
                   latest.revision == old_revision &&
                   latest.source_generation == generations[0] &&
                   latest.bytes.len == PROPERTY_BYTES &&
                   memcmp(latest.bytes.data, large, PROPERTY_BYTES) == 0);
        moqr_bind_meta_release(rig.bind, &latest);
    }

    for (unsigned attempt = 0; attempt < (mode == 2 ? 2u : 1u); attempt++) {
        bool blocked = mode == 1 || (mode == 2 && attempt == 0);
        moq_publish_cfg_t pc;
        moq_publish_cfg_init(&pc);
        pc.track_namespace = (moq_namespace_t){ .parts = parts, .count = 1 };
        pc.track_name = B(attempt == 0 ? "replacement" : "after-pin");
        pc.track_properties = (moq_bytes_t){ .data = probe, .len = sizeof(probe) };
        unsigned accepted = peer.accepted, refused = peer.refused;
        ML_REQUIRE(moq_session_publish(pub->peer, &pc, rig.now, &peer.pending) == MOQ_OK);
        attempted++;
        relay_bind_test_rig_pump(&rig, 6);
        metadata_lifetime_drain(&rig, pub, &peer);
        moqr_bind_get_stats(rig.bind, &bs);
        ML_REQUIRE(rig.failures == 0 && bs.session_errors == 0 &&
                   bs.ingest_refusals == 0 &&
                   moq_session_state(pub->peer) == MOQ_SESS_ESTABLISHED &&
                   moq_session_state(pub->rsess) == MOQ_SESS_ESTABLISHED);
        if (blocked) {
            ML_REQUIRE(peer.accepted == accepted && peer.refused == refused + 1u &&
                       peer.error == MOQ_REQUEST_ERROR_INTERNAL_ERROR);
        } else if (peer.accepted != accepted + 1u || peer.refused != refused) {
            ML_REQUIRE(peer.accepted == accepted && peer.refused == refused + 1u &&
                       peer.error == MOQ_REQUEST_ERROR_INTERNAL_ERROR);
            printf("FAIL: metadata_lifetime(%s) recovery expected=1 actual=0\n", row);
            recovery_failure = 1;
        } else {
            moqr_track_t replacement = MOQR_TRACK_INVALID;
            uint64_t gen = 0;
            ML_REQUIRE(metadata_lifetime_track(&rig, observer, ns, pc.track_name,
                       &replacement, &gen) == MOQR_OK);
            ML_REQUIRE(!moqr_track_eq(replacement, tracks[0]) &&
                       moq_handle_slot(replacement._opaque) == moq_handle_slot(tracks[0]._opaque));
            ML_REQUIRE(moqr_bind_meta_latest(rig.bind, replacement, &latest) == MOQR_OK);
            ML_REQUIRE(latest.state == MOQR_BIND_META_PRESENT &&
                       latest.bytes.len == sizeof(probe) &&
                       memcmp(latest.bytes.data, probe, sizeof(probe)) == 0);
            moqr_bind_meta_release(rig.bind, &latest);
        }
        if (mode == 2 && attempt == 0) {
            ML_REQUIRE(held.state == MOQR_BIND_META_PRESENT &&
                       held.revision == old_revision &&
                       held.source_generation == generations[0] &&
                       held.bytes.len == PROPERTY_BYTES &&
                       memcmp(held.bytes.data, large, PROPERTY_BYTES) == 0);
            moqr_bind_meta_release(rig.bind, &held);
        }
    }

cleanup:
    moqr_bind_meta_release(rig.bind, &latest);
    moqr_bind_meta_release(rig.bind, &held);
    moq_rcbuf_decref(payload);
    if (large != NULL) a.vt.free(large, PROPERTY_BYTES, a.vt.ctx);
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
#undef ML_REQUIRE
    printf("%s: metadata_lifetime(%s) probes=%u recovery_failures=%d fixture_failures=%d\n",
           rig.failures || recovery_failure ? "FAIL" : "PASS", row, attempted,
           recovery_failure, rig.failures);
    return rig.failures ? 2 : recovery_failure;
}

static int
metadata_lifetime_all(void)
{
    int result = 0;
    for (unsigned mode = 0; mode < 3; mode++) {
        int rc = metadata_lifetime(mode);
        if (rc == 2) return 2;
        if (rc != 0) result = 1;
    }
    return result;
}

typedef struct metadata_managed_peer {
    moq_subscription_t sub;
    moq_bytes_t expected;
    const uint8_t *filler;
    unsigned setups, acks, errors, fillers, closed;
    uint64_t error;
} metadata_managed_peer_t;

static void
metadata_managed_drain(rig_t *r, conn_t *cn, metadata_managed_peer_t *p)
{
    moq_event_t e;
    while (moq_session_poll_events(cn->peer, &e, 1) != 0) {
        switch (e.kind) {
        case MOQ_EVENT_SETUP_COMPLETE:
            p->setups++;
            break;
        case MOQ_EVENT_REQUEST_READY:
            break;
        case MOQ_EVENT_SUBSCRIBE_OK: {
            bool full = e.detail_size >= sizeof(moq_subscribe_ok_event_t);
            R_CHECK(r, full);
            if (!full) break;
            const moq_subscribe_ok_event_t *ok = &e.u.subscribe_ok;
            R_CHECK(r, ok->sub._opaque == p->sub._opaque && !ok->has_largest &&
                       !ok->dynamic_groups && ok->track_properties.len == p->expected.len);
            if (ok->track_properties.len == p->expected.len && p->expected.len != 0) {
                R_CHECK(r, ok->track_properties.data != NULL &&
                           memcmp(ok->track_properties.data, p->expected.data, p->expected.len) == 0);
            }
            p->acks++;
            break;
        }
        case MOQ_EVENT_SUBSCRIBE_ERROR:
            R_CHECK(r, e.detail_size >= sizeof(moq_subscribe_error_event_t) &&
                       e.u.subscribe_error.sub._opaque == p->sub._opaque);
            p->errors++;
            p->error = e.u.subscribe_error.error_code;
            break;
        case MOQ_EVENT_NAMESPACE_PUBLISHED: {
            const moq_namespace_published_event_t *q = &e.u.namespace_published;
            size_t len = p->fillers == 0 ? 2400u : 1u;
            R_CHECK(r, e.detail_size >= sizeof(*q) && p->filler != NULL &&
                       p->fillers < 3 && q->track_namespace.count == 1 &&
                       q->track_namespace.parts != NULL);
            if (p->filler != NULL && p->fillers < 3 &&
                q->track_namespace.count == 1 && q->track_namespace.parts != NULL) {
                R_CHECK(r, q->track_namespace.parts[0].len == len &&
                           q->track_namespace.parts[0].data != NULL &&
                           memcmp(q->track_namespace.parts[0].data,
                                  p->filler + p->fillers, len) == 0);
            }
            p->fillers++;
            moq_accept_namespace_cfg_t cfg;
            moq_accept_namespace_cfg_init(&cfg);
            R_CHECK(r, moq_session_accept_namespace(cn->peer, q->ann, &cfg, r->now) == MOQ_OK);
            break;
        }
        case MOQ_EVENT_SESSION_CLOSED:
            p->closed++;
            break;
        default:
            printf("FIXTURE: metadata_managed unexpected event=%d\n", (int)e.kind);
            r->failures++;
            break;
        }
        moq_event_cleanup(&e);
    }
}

/* Missing and failed state never uses the property-less direct exception.
 * The blocked ACK row owns A only through the production pending reply pin. */
static int
metadata_managed(unsigned mode)
{
    static const char *const names[] = {
        "missing", "unavailable", "oom", "capacity", "explicit-empty",
        "direct-import-refused", "lost-directory", "old-ack",
        "direct-pending", "managed-pending"
    };
    if (mode >= sizeof(names) / sizeof(names[0])) return 2;
    const char *row = names[mode];
    bool pending = mode >= 8;
    bool managed = mode != 5 && mode != 8;
    bool accepts = mode == 4 || mode == 5 || mode == 7 || mode == 8;
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) return 2;
    uint32_t saved_actions = relay_bind_test_max_actions;
    conn_t *cn[2] = {0};
    metadata_managed_peer_t peer[2] = {0};
    moqr_bind_meta_ref_t latest = {0};
    uint8_t *oversized = NULL;
    uint8_t filler[2400];
    memset(filler, 'm', sizeof(filler));
    filler[1] = 'n'; filler[2] = 'o';
    moqr_binding_t producer = MOQR_BINDING_INVALID;
    moqr_track_t track = MOQR_TRACK_INVALID;
    uint64_t generation = 0, revision = 0;
    moq_bytes_t part = B("managed");
    moqr_ns_t ns = { .parts = &part, .count = 1 };
    moqr_bind_stats_t stats;
#define MM_REQUIRE(expr) do { if (!(expr)) { \
    printf("FIXTURE: metadata_managed(%s): %s\n", row, #expr); \
    rig.failures++; goto cleanup; \
} } while (0)
    moqr_binding_options_t options;
    moqr_binding_options_init_sized(&options, sizeof(options));
    options.flags = managed ? MOQR_BINDING_TRACK_METADATA_MANAGED : 0;
    MM_REQUIRE(moqr_core_binding_open_with_options(rig.core, 900, &options, &producer) == MOQR_OK);
    if (pending) {
        MM_REQUIRE(moqr_core_announce(rig.core, producer, ns) == MOQR_OK);
    } else {
        MM_REQUIRE(moqr_core_publish_open(rig.core, producer, ns, B("video"), 901, &track) == MOQR_OK);
        moqr_intent_t it;
        MM_REQUIRE(moqr_core_poll_intents(rig.core, &it, 1) == 1 &&
                   it.kind == MOQR_INTENT_ACCEPT_PUBLISH && moqr_track_eq(it.track, track));
        generation = it.track_gen;
        MM_REQUIRE(moqr_core_poll_intents(rig.core, &it, 1) == 0);
        bool actual = !managed;
        MM_REQUIRE(moqr_core_track_metadata_managed(rig.core, track, &actual) == MOQR_OK &&
                   actual == managed);
    }
    if (mode >= 1 && mode <= 7) {
        moqr_bind_meta_ref_t input = {0};
        input.state = mode == 1 ? MOQR_BIND_META_UNAVAILABLE :
                      mode == 4 ? MOQR_BIND_META_EMPTY : MOQR_BIND_META_PRESENT;
        input.wire_version = MOQ_VERSION_DRAFT_22;
        input.source_binding = producer;
        input.source_handle = 901;
        input.source_generation = generation;
        if (input.state == MOQR_BIND_META_PRESENT) {
            input.bytes = (moq_bytes_t){ .data = d22_tp_expected, .len = sizeof(d22_tp_expected) };
        }
        if (mode == 3) {
            size_t size = (size_t)MOQR_BIND_META_SET_MAX + 1u;
            oversized = a.vt.alloc(size, a.vt.ctx);
            MM_REQUIRE(oversized != NULL);
            oversized[0] = 0x79; oversized[1] = 0xc0;
            oversized[2] = 0xff; oversized[3] = 0xfd;
            memset(oversized + 4, 0x5a, size - 4);
            input.bytes = (moq_bytes_t){ .data = oversized, .len = size };
        }
        long attempts = a.attempts;
        if (mode == 2) a.fail_at = attempts + 1;
        moqr_result_t result = moqr_bind_meta_replace(rig.bind, track, generation,
                                                     &input, rig.now, &revision);
        a.fail_at = 0;
        moqr_result_t expected = mode == 2 ? MOQR_ERR_NOMEM :
                                mode == 3 ? MOQR_ERR_CAPACITY :
                                mode == 5 ? MOQR_ERR_WRONG_STATE : MOQR_OK;
        MM_REQUIRE(result == expected);
        if (mode == 2) MM_REQUIRE(a.attempts == attempts + 1);
        MM_REQUIRE(mode == 5 ? revision == 0 : revision != 0);
        bool actual = !managed;
        MM_REQUIRE(moqr_core_track_metadata_managed(rig.core, track, &actual) == MOQR_OK &&
                   actual == managed);
        MM_REQUIRE(moqr_bind_meta_latest(rig.bind, track, &latest) == MOQR_OK);
        moqr_bind_meta_state_t state = mode == 4 ? MOQR_BIND_META_EMPTY :
            mode == 6 || mode == 7 ? MOQR_BIND_META_PRESENT : MOQR_BIND_META_UNAVAILABLE;
        MM_REQUIRE(latest.state == state && latest.revision == revision);
        moqr_bind_meta_release(rig.bind, &latest);
        if (mode == 6) {
            moqr_bind_destroy(rig.bind);
            rig.bind = NULL;
            moqr_bind_cfg_t cfg;
            moqr_bind_cfg_init_sized(&cfg, sizeof(cfg), &a.vt);
            cfg.core = rig.core;
            MM_REQUIRE(moqr_bind_create(&cfg, &rig.bind) == MOQR_OK);
            MM_REQUIRE(moqr_core_track_metadata_managed(rig.core, track, &actual) == MOQR_OK && actual);
            MM_REQUIRE(moqr_bind_meta_latest(rig.bind, track, &latest) == MOQR_OK &&
                       latest.state == MOQR_BIND_META_UNAVAILABLE && latest.revision == 0);
            moqr_bind_meta_release(rig.bind, &latest);
        }
    }

    relay_bind_test_max_actions = mode == 7 ? 3u : 0u;
    cn[0] = relay_bind_test_rig_connect(&rig, mode == 4 ? MOQ_VERSION_DRAFT_16 : MOQ_VERSION_DRAFT_22);
    relay_bind_test_max_actions = saved_actions;
    MM_REQUIRE(cn[0] != NULL);
    relay_bind_test_rig_pump(&rig, 4);
    metadata_managed_drain(&rig, cn[0], &peer[0]);
    MM_REQUIRE(peer[0].setups == 1 && rig.failures == 0);
    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = &part, .count = 1 };
    scfg.track_name = B("video");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    scfg.start_group = 7;
    MM_REQUIRE(moq_session_subscribe(cn[0]->peer, &scfg, rig.now, &peer[0].sub) == MOQ_OK);

    if (mode == 7) {
        peer[0].expected = (moq_bytes_t){ .data = d22_tp_expected, .len = sizeof(d22_tp_expected) };
        peer[0].filler = filler;
        relay_bind_test_rig_step_pair(&rig, cn[0]);
        cn[0]->frozen = true;
        MM_REQUIRE(moq_session_action_capacity(cn[0]->rsess) == 3);
        moq_announcement_t anns[3];
        for (unsigned i = 0; i < 3; i++) {
            moq_publish_namespace_cfg_t cfg;
            moq_publish_namespace_cfg_init(&cfg);
            moq_bytes_t name = { .data = filler + i, .len = i == 0 ? sizeof(filler) : 1u };
            cfg.track_namespace = (moq_namespace_t){ .parts = &name, .count = 1 };
            MM_REQUIRE(moq_session_publish_namespace(cn[0]->rsess, &cfg, rig.now, &anns[i]) == MOQ_OK);
        }
        MM_REQUIRE(moq_session_action_capacity(cn[0]->rsess) == 0);
        moqr_bind_get_stats(rig.bind, &stats);
        MM_REQUIRE(stats.pending_high_water == 0);
        relay_bind_test_rig_bind_pump_once(&rig);
        metadata_managed_drain(&rig, cn[0], &peer[0]);
        moqr_bind_get_stats(rig.bind, &stats);
        MM_REQUIRE(stats.pending_high_water >= 1 && peer[0].acks == 0 &&
                   peer[0].errors == 0 && peer[0].fillers == 0 && rig.failures == 0);
        moqr_bind_meta_ref_t unavailable = {0};
        uint64_t newer = 0;
        MM_REQUIRE(moqr_bind_meta_replace(rig.bind, track, generation, &unavailable,
                                          rig.now, &newer) == MOQR_OK && newer > revision);
        MM_REQUIRE(moqr_bind_meta_latest(rig.bind, track, &latest) == MOQR_OK &&
                   latest.state == MOQR_BIND_META_UNAVAILABLE && latest.revision == newer);
        moqr_bind_meta_release(rig.bind, &latest);
        cn[0]->frozen = false;
    } else if (pending) {
        relay_bind_test_rig_pump(&rig, 4);
        metadata_managed_drain(&rig, cn[0], &peer[0]);
        MM_REQUIRE(peer[0].acks == 0 && peer[0].errors == 0 && rig.failures == 0);
        moqr_binding_t observer;
        MM_REQUIRE(moqr_core_binding_open(rig.core, 902, &observer) == MOQR_OK);
        moqr_subscribe_req_t request;
        moqr_subscribe_req_init(&request);
        request.ns = ns;
        request.name = B("video");
        request.forward = false;
        moqr_sub_t sub;
        MM_REQUIRE(moqr_core_subscribe(rig.core, observer, &request, &sub) == MOQR_OK);
        moqr_sub_state_t state;
        MM_REQUIRE(moqr_core_sub_state(rig.core, sub, &state) == MOQR_OK && state == MOQR_SUB_PARKED);
        MM_REQUIRE(moqr_core_sub_track(rig.core, sub, &track, &generation) == MOQR_OK);
        bool actual = !managed;
        MM_REQUIRE(moqr_core_track_metadata_managed(rig.core, track, &actual) == MOQR_OK &&
                   actual == managed);
        MM_REQUIRE(moqr_bind_meta_latest(rig.bind, track, &latest) == MOQR_OK &&
                   latest.state == MOQR_BIND_META_UNAVAILABLE && latest.revision == 0);
        moqr_bind_meta_release(rig.bind, &latest);
        MM_REQUIRE(moqr_core_unsubscribe(rig.core, sub, rig.now) == MOQR_OK);
        MM_REQUIRE(moqr_core_binding_close(rig.core, observer, rig.now) == MOQR_OK);
        relay_bind_test_rig_pump(&rig, 4);
        metadata_managed_drain(&rig, cn[0], &peer[0]);
        MM_REQUIRE(peer[0].acks == 0 && peer[0].errors == 0 && rig.failures == 0);
        MM_REQUIRE(moqr_core_upstream_ok(rig.core, track, generation, 903, false, 0, 0) == MOQR_OK);
    }

    for (unsigned i = 0; i < 8; i++) {
        relay_bind_test_rig_cycle(&rig);
        metadata_managed_drain(&rig, cn[0], &peer[0]);
    }
    MM_REQUIRE(peer[0].acks == (accepts ? 1u : 0u) &&
               peer[0].errors == (accepts ? 0u : 1u) && peer[0].closed == 0 &&
               peer[0].fillers == (mode == 7 ? 3u : 0u) && rig.failures == 0);
    if (!accepts) MM_REQUIRE(peer[0].error == MOQ_REQUEST_ERROR_INTERNAL_ERROR);
    if (mode == 7) {
        cn[1] = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_22);
        MM_REQUIRE(cn[1] != NULL);
        relay_bind_test_rig_pump(&rig, 4);
        metadata_managed_drain(&rig, cn[1], &peer[1]);
        MM_REQUIRE(peer[1].setups == 1);
        MM_REQUIRE(moq_session_subscribe(cn[1]->peer, &scfg, rig.now, &peer[1].sub) == MOQ_OK);
        relay_bind_test_rig_pump(&rig, 8);
        metadata_managed_drain(&rig, cn[0], &peer[0]);
        metadata_managed_drain(&rig, cn[1], &peer[1]);
        MM_REQUIRE(peer[0].acks == 1 && peer[0].errors == 0 &&
                   peer[1].acks == 0 && peer[1].errors == 1 &&
                   peer[1].error == MOQ_REQUEST_ERROR_INTERNAL_ERROR && rig.failures == 0);
    }
    moqr_bind_get_stats(rig.bind, &stats);
    MM_REQUIRE(stats.session_errors == 0 && stats.ingest_refusals == 0);
    for (unsigned i = 0; i < 2; i++) {
        if (cn[i] != NULL) {
            MM_REQUIRE(peer[i].closed == 0 &&
                       moq_session_state(cn[i]->peer) == MOQ_SESS_ESTABLISHED &&
                       moq_session_state(cn[i]->rsess) == MOQ_SESS_ESTABLISHED);
        }
    }
cleanup:
    relay_bind_test_max_actions = saved_actions;
    a.fail_at = 0;
    moqr_bind_meta_release(rig.bind, &latest);
    if (oversized != NULL) a.vt.free(oversized, (size_t)MOQR_BIND_META_SET_MAX + 1u, a.vt.ctx);
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
#undef MM_REQUIRE
    printf("%s: metadata_managed(%s) acks=%u errors=%u fixture_failures=%d\n",
           rig.failures ? "FAIL" : "PASS", row, peer[0].acks + peer[1].acks,
           peer[0].errors + peer[1].errors, rig.failures);
    return rig.failures ? 2 : 0;
}

static int
metadata_managed_all(void)
{
    for (unsigned mode = 0; mode < 10; mode++) {
        if (metadata_managed(mode) != 0) return 2;
    }
    return 0;
}

int
main(int argc, char **argv)
{
    if (argc == 2 && strcmp(argv[1], "--metadata-managed") == 0) {
        return metadata_managed_all();
    }
    if (argc == 2 && strcmp(argv[1], "--metadata-lifetime") == 0) {
        return metadata_lifetime_all();
    }
    if (argc == 2 && strcmp(argv[1], "--retrieval-publish-source") == 0) {
        return retrieval_publish_source_all();
    }
    if (argc == 2 && strcmp(argv[1], "--retrieval-live-frontier") == 0) {
        return retrieval_live_frontier_all();
    }
    if (argc == 2 && strcmp(argv[1], "--retrieval-live-suffix") == 0) {
        return retrieval_frontier_suffix_all();
    }
    if (argc == 2 && strcmp(argv[1], "--retrieval-response-frontier") == 0) {
        return retrieval_response_frontier_all();
    }
    if (argc == 2 && strcmp(argv[1], "--retrieval-cached-borrow") == 0) {
        return retrieval_cached_borrow_all();
    }
    if (argc == 2 && strcmp(argv[1], "--retrieval-cached-refusal") == 0) {
        return retrieval_cached_refusal_all();
    }
    if (argc > 1 && strcmp(argv[1], "--direct-core-publisher") == 0) {
        return direct_core_publisher_all();
    }
    if (argc > 1 && strcmp(argv[1], "--track-properties") == 0) {
        if (relay_bind_test_parity_flow(MOQ_VERSION_DRAFT_16) != 0 ||
            relay_bind_test_parity_flow(MOQ_VERSION_DRAFT_18) != 0 ||
            draft22_track_properties(false) != 0) {
            printf("FIXTURE: track-properties control failure\n");
            return 2;
        }
        return draft22_track_properties(true);
    }
    if (argc > 1 && strcmp(argv[1], "--mixed-track-properties") == 0) {
        if (relay_bind_test_parity_flow(MOQ_VERSION_DRAFT_16) != 0 ||
            relay_bind_test_parity_flow(MOQ_VERSION_DRAFT_18) != 0 ||
            draft22_track_properties(false) != 0 ||
            track_properties_profiles(MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_22,
                                       false) != 0 ||
            track_properties_profiles(MOQ_VERSION_DRAFT_22, MOQ_VERSION_DRAFT_18,
                                       false) != 0) {
            printf("FIXTURE: mixed-track-properties control failure\n");
            return 2;
        }
        int forward = track_properties_profiles(
            MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_22, true);
        if (forward == 2) return 2;
        int reverse = track_properties_profiles(
            MOQ_VERSION_DRAFT_22, MOQ_VERSION_DRAFT_18, true);
        if (reverse == 2) return 2;
        return forward == 0 && reverse == 0 ? 0 : 1;
    }
    if (argc > 1 && strcmp(argv[1], "--push-track-properties") == 0) {
        if (relay_bind_test_parity_flow(MOQ_VERSION_DRAFT_16) != 0 ||
            relay_bind_test_parity_flow(MOQ_VERSION_DRAFT_18) != 0 ||
            relay_bind_test_dataplane_mixed_draft_oracle(MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18, 0) != 0 ||
            relay_bind_test_dataplane_mixed_draft_oracle(MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_16, 0) != 0 ||
            relay_bind_test_run_defer_case(MOQR_AUTH_PUBLISH, MOQR_AUTH_ALLOW, "publish/allow") != 0 ||
            draft22_push_track_properties(false, false) != 0 ||
            draft22_push_track_properties(true, false) != 0) {
            printf("FIXTURE: push-track-properties control failure\n");
            return 2;
        }
        int immediate = draft22_push_track_properties(false, true);
        if (immediate == 2) {
            return 2;
        }
        int deferred = draft22_push_track_properties(true, true);
        if (deferred == 2) {
            return 2;
        }
        return immediate == 0 && deferred == 0 ? 0 : 1;
    }
    if (argc > 1 && strcmp(argv[1], "--metadata-reply-capacity") == 0) {
        if (metadata_reply_capacity(false, false) != 0) return 2;
        int immediate = metadata_reply_capacity(true, false);
        if (immediate == 2) return 2;
        int blocked = metadata_reply_capacity(true, true);
        if (blocked == 2) return 2;
        return immediate == 0 && blocked == 0 ? 0 : 1;
    }
    int failures = 0;
    failures += metadata_managed_all();
    failures += metadata_lifetime_all();
    failures += retrieval_publish_source_all();
    failures += retrieval_live_frontier_all();
    failures += retrieval_cached_borrow_all();
    failures += retrieval_cached_refusal_all();
    failures += direct_core_publisher_all();
    failures += metadata_reply_capacity(false, false);
    failures += metadata_reply_capacity(true, false);
    failures += metadata_reply_capacity(true, true);
    failures += draft22_track_properties(false);
    failures += draft22_track_properties(true);
    failures += track_properties_profiles(MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_22,
                                           false);
    failures += track_properties_profiles(MOQ_VERSION_DRAFT_22, MOQ_VERSION_DRAFT_18,
                                           false);
    failures += track_properties_profiles(MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_22,
                                           true);
    failures += track_properties_profiles(MOQ_VERSION_DRAFT_22, MOQ_VERSION_DRAFT_18,
                                           true);
    failures += relay_answer_properties_all();
    failures += relay_answer_pinned_converted();
    failures += draft22_push_track_properties(false, false);
    failures += draft22_push_track_properties(true, false);
    failures += draft22_push_track_properties(false, true);
    failures += draft22_push_track_properties(true, true);
    return failures == 0 ? 0 : 1; /* exit status truncates to 8 bits */
}
