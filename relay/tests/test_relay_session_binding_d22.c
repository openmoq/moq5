/*
 * Draft-22 Location Filters, range ends and fills through the production
 * relay binding over real sessions: filters map onto the relay's retained
 * state, range ends are honoured (draft-18 alongside as the control), a
 * fill serves what the relay holds and asks the origin for the rest, marks
 * a terminal gap or an evicted prefix explicitly, and its FILL_TIMEOUT
 * waits, expires, caps and defaults as configured. SUBSCRIBE_UPDATE filters
 * apply against the relay's own Largest Object, answers to held updates go
 * out before the subscription's terminal, and draft-16 and draft-18 update
 * behaviour is unchanged.
 */
#include "relay_session_binding_harness.h"

/* -- draft-22: Location Filters, range ends, fills --------------------------- *
 * A draft-22 peer's SUBSCRIBE carries a LOCATION_FILTER parameter and may
 * carry FILL_PARAMETERS. The binding maps the filter forms onto the relay
 * filter model, refuses the forms the retained log cannot serve with
 * REQUEST_ERROR NOT_SUPPORTED, answers a fill from the retained log on a fill
 * fetch stream, and fails a fill it cannot serve on that stream alone. */

static uint64_t
dlv_min_group(const peer_state_t *ps)
{
    uint64_t m = UINT64_MAX;
    for (int i = 0; i < ps->dlv_n; i++) {
        if (ps->dlv[i].group < m) {
            m = ps->dlv[i].group;
        }
    }
    return m;
}

/* A draft-22 connection attaches and names its draft. */
static int
draft22_conn_version(void)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: draft22_conn_version rig create\n");
        return 1;
    }
    conn_t *cn = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_22);
    R_CHECK(&rig, cn != NULL);
    if (cn != NULL) {
        R_CHECK(&rig, moqr_bind_conn_version(rig.bind, cn->rsess) ==
                          MOQ_VERSION_DRAFT_22);
        relay_bind_test_rig_pump(&rig, 4);
        R_CHECK(&rig, moq_session_state(cn->rsess) == MOQ_SESS_ESTABLISHED);
        R_CHECK(&rig, moq_session_state(cn->peer) == MOQ_SESS_ESTABLISHED);
    }
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (rig.failures == 0) {
        printf("PASS: draft22_conn_version\n");
    }
    return rig.failures;
}

/* Semantic Location Filters: the current group (relative
 * StartGroup=1) starts at {Largest Object.Group, 0}; a relative start further
 * back starts that many groups before the Next Group (StartGroup=2: one group
 * before the current one); Next Object starts past Largest Object. An
 * inclusive absolute object end admits exactly the requested objects. */
static int
draft22_filter_mapping(void)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: draft22_filter_mapping rig create\n");
        return 1;
    }
    d22_track_t t;
    relay_bind_test_d22_track_setup(&rig, &t, MOQ_VERSION_DRAFT_22, 2); /* largest {1,2} */
    R_CHECK(&rig, t.prime_ps.objects == 6);

    conn_t *cur = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_22);
    conn_t *rel2 = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_22);
    conn_t *objend = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_22);
    conn_t *nextobj = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_22);
    R_CHECK(&rig, cur && rel2 && objend && nextobj);
    relay_bind_test_rig_pump(&rig, 4);

    moq_location_filter_cfg_t loc;
    moq_subscription_t h;

    moq_location_filter_cfg_init(&loc);
    loc.kind = MOQ_LOCATION_FILTER_RELATIVE_GROUP;
    loc.relative_group_offset = 1;
    R_CHECK(&rig, relay_bind_test_d22_subscribe(&rig, cur, &t, &loc, NULL, &h) == MOQ_OK);

    moq_location_filter_cfg_init(&loc);
    loc.kind = MOQ_LOCATION_FILTER_RELATIVE_GROUP;
    loc.relative_group_offset = 2;
    R_CHECK(&rig, relay_bind_test_d22_subscribe(&rig, rel2, &t, &loc, NULL, &h) == MOQ_OK);

    moq_location_filter_cfg_init(&loc);
    loc.kind = MOQ_LOCATION_FILTER_OBJECT_END;
    loc.start_group = 0;
    loc.start_object = 0;
    loc.end_group = 1;
    loc.end_object = 0;
    R_CHECK(&rig, relay_bind_test_d22_subscribe(&rig, objend, &t, &loc, NULL, &h) == MOQ_OK);

    moq_location_filter_cfg_init(&loc);
    loc.kind = MOQ_LOCATION_FILTER_NEXT_OBJECT;
    R_CHECK(&rig, relay_bind_test_d22_subscribe(&rig, nextobj, &t, &loc, NULL, &h) == MOQ_OK);

    relay_bind_test_rig_pump(&rig, 10);
    peer_state_t cur_ps, rel2_ps, objend_ps, next_ps;
    memset(&cur_ps, 0, sizeof(cur_ps));
    memset(&rel2_ps, 0, sizeof(rel2_ps));
    memset(&objend_ps, 0, sizeof(objend_ps));
    memset(&next_ps, 0, sizeof(next_ps));
    relay_bind_test_peer_drain(&rig, cur, &cur_ps, false, false, 0, 0);
    relay_bind_test_peer_drain(&rig, rel2, &rel2_ps, false, false, 0, 0);
    relay_bind_test_peer_drain(&rig, objend, &objend_ps, false, false, 0, 0);
    relay_bind_test_peer_drain(&rig, nextobj, &next_ps, false, false, 0, 0);

    /* Current group: SUBSCRIBE_OK with Largest Object, then exactly the
     * retained group 1 (group 0 is below the start and never passes). */
    R_CHECK(&rig, cur_ps.subscribe_ok);
    R_CHECK(&rig, cur_ps.has_largest && cur_ps.largest_group == 1 &&
                      cur_ps.largest_object == 2);
    R_CHECK(&rig, cur_ps.objects == 3);
    R_CHECK(&rig, dlv_min_group(&cur_ps) == 1);

    /* StartGroup=2: from group 0, both retained groups. */
    R_CHECK(&rig, rel2_ps.subscribe_ok && rel2_ps.subscribe_errors == 0);
    R_CHECK(&rig, rel2_ps.has_largest && rel2_ps.largest_group == 1 &&
                      rel2_ps.largest_object == 2);
    R_CHECK(&rig, rel2_ps.objects == 6 && dlv_min_group(&rel2_ps) == 0);
    R_CHECK(&rig, objend_ps.subscribe_errors == 0 && objend_ps.subscribe_ok);
    R_CHECK(&rig, objend_ps.objects == 4 && objend_ps.dlv_n == 4);
    for (int i = 0; i < objend_ps.dlv_n && i < 4; i++) {
        R_CHECK(&rig, objend_ps.dlv[i].group == (i < 3 ? 0u : 1u));
        R_CHECK(&rig, objend_ps.dlv[i].subgroup == 0);
        R_CHECK(&rig, objend_ps.dlv[i].object_id == (i < 3 ? (uint64_t)i : 0));
    }
    /* No session closed over any of it. */
    R_CHECK(&rig, !rel2_ps.session_closed && !objend_ps.session_closed);

    /* Next Object: accepted, nothing retained passes; the next published
     * group does. */
    R_CHECK(&rig, next_ps.subscribe_ok && next_ps.objects == 0);
    relay_bind_test_pub_subgroup_n(&rig, t.pub, t.pub_ps.up_sub, 2, 0, 3, false);
    relay_bind_test_rig_pump(&rig, 8);
    relay_bind_test_peer_drain(&rig, nextobj, &next_ps, false, false, 0, 0);
    relay_bind_test_peer_drain(&rig, cur, &cur_ps, false, false, 0, 0);
    R_CHECK(&rig, next_ps.objects == 3 && dlv_min_group(&next_ps) == 2);
    R_CHECK(&rig, cur_ps.objects == 6);   /* open-ended: the live edge too */
    relay_bind_test_peer_drain(&rig, rel2, &rel2_ps, false, false, 0, 0);
    R_CHECK(&rig, rel2_ps.objects == 9);
    relay_bind_test_peer_drain(&rig, objend, &objend_ps, false, false, 0, 0);
    R_CHECK(&rig, objend_ps.objects == 4 && !objend_ps.done_seen &&
                      !objend_ps.session_closed);

    int f = rig.failures;
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (f == 0 && rig.failures == 0) {
        printf("PASS: draft22_filter_mapping\n");
    }
    return f + rig.failures;
}

/* A bounded Location Filter whose end is passed by Largest Object: under
 * draft-18 the relay retires the subscription with PUBLISH_DONE
 * SUBSCRIPTION_ENDED (0x3); under draft-22 the subscription stays established
 * with nothing further delivered, because a draft-22 publisher does not end a
 * subscription for that reason and the status no longer exists. */
static int
draft_range_end(moq_version_t version)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: draft_range_end rig create\n");
        return 1;
    }
    d22_track_t t;
    relay_bind_test_d22_track_setup(&rig, &t, version, 1); /* group 0 retained */
    conn_t *sub = relay_bind_test_rig_connect(&rig, version);
    R_CHECK(&rig, sub != NULL);
    relay_bind_test_rig_pump(&rig, 4);

    moq_subscription_t h;
    if (version == MOQ_VERSION_DRAFT_22) {
        moq_location_filter_cfg_t loc;
        moq_location_filter_cfg_init(&loc);
        loc.kind = MOQ_LOCATION_FILTER_GROUP_END;
        loc.start_group = 0;
        loc.start_object = 0;
        loc.end_group = 0;
        R_CHECK(&rig, relay_bind_test_d22_subscribe(&rig, sub, &t, &loc, NULL, &h) == MOQ_OK);
    } else {
        moq_subscribe_cfg_t scfg;
        moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = (moq_namespace_t){ .parts = t.nsp, .count = 2 };
        scfg.track_name = B("v");
        scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE;
        scfg.start_group = 0;
        scfg.start_object = 0;
        scfg.end_group = 0;
        R_CHECK(&rig, moq_session_subscribe(sub->peer, &scfg, rig.now, &h) ==
                          MOQ_OK);
    }
    relay_bind_test_rig_pump(&rig, 8);
    peer_state_t s_ps;
    memset(&s_ps, 0, sizeof(s_ps));
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.subscribe_ok && s_ps.objects == 3);
    R_CHECK(&rig, !s_ps.done_seen);   /* the range is not yet passed */

    /* Group 1 arrives: Largest Object passes the end of the filter. */
    relay_bind_test_pub_subgroup_n(&rig, t.pub, t.pub_ps.up_sub, 1, 0, 2, false);
    relay_bind_test_rig_pump(&rig, 10);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.objects == 3);   /* nothing outside the range */
    if (version == MOQ_VERSION_DRAFT_22) {
        R_CHECK(&rig, !s_ps.done_seen);
        /* Still established: a later group changes nothing, and the
         * subscriber ends it itself. */
        relay_bind_test_pub_subgroup_n(&rig, t.pub, t.pub_ps.up_sub, 2, 0, 1, false);
        relay_bind_test_rig_pump(&rig, 8);
        relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
        R_CHECK(&rig, s_ps.objects == 3 && !s_ps.done_seen);
        R_CHECK(&rig, moq_session_unsubscribe(sub->peer, h, rig.now) == MOQ_OK);
        relay_bind_test_rig_pump(&rig, 4);
    } else {
        R_CHECK(&rig, s_ps.done_seen && s_ps.done_count == 1);
        R_CHECK(&rig, s_ps.done_code == 0x3u);   /* SUBSCRIPTION_ENDED */
    }
    R_CHECK(&rig, !s_ps.session_closed);

    int f = rig.failures;
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (f == 0 && rig.failures == 0) {
        printf("PASS: draft_range_end(draft-%d)\n", (int)version);
    }
    return f + rig.failures;
}

/* Whether the fill's End of Range markers, in order, are exactly `want`. */
typedef struct d22_gap {
    uint32_t kind;
    uint64_t group, object;
} d22_gap_t;

static bool
d22_fill_gaps_are(const peer_state_t *ps, const d22_gap_t *want, int n)
{
    if (ps->fill_gaps != n) {
        return false;
    }
    for (int i = 0; i < n; i++) {
        if (ps->fill_gap_log[i].kind != want[i].kind ||
            ps->fill_gap_log[i].group != want[i].group ||
            ps->fill_gap_log[i].object != want[i].object) {
            return false;
        }
    }
    return true;
}

/* Group 0's tail alone, marked Unknown after its origin resolves the gap. */
static bool
d22_fill_group0_tail_unknown(const peer_state_t *ps)
{
    const d22_gap_t want[1] = { { MOQ_FETCH_RANGE_UNKNOWN, 0, UINT64_MAX } };
    return d22_fill_gaps_are(ps, want, 1);
}

/* A fill whose range the retained log holds is answered on a fill fetch
 * stream: every retained object in the range, then a FIN. The subscription
 * itself (Next Object) delivers nothing retained and keeps serving the live
 * edge afterwards. */
static int
draft22_fill_served(void)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: draft22_fill_served rig create\n");
        return 1;
    }
    d22_track_t t;
    relay_bind_test_d22_track_setup(&rig, &t, MOQ_VERSION_DRAFT_22, 2); /* largest {1,2} */
    conn_t *sub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_22);
    R_CHECK(&rig, sub != NULL);
    relay_bind_test_rig_pump(&rig, 4);

    moq_location_filter_cfg_t loc, floc;
    moq_location_filter_cfg_init(&loc);
    loc.kind = MOQ_LOCATION_FILTER_NEXT_OBJECT;
    moq_location_filter_cfg_init(&floc);
    floc.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;   /* {0,0} .. Largest */
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.location = &floc;
    moq_subscription_t h;
    R_CHECK(&rig, relay_bind_test_d22_subscribe(&rig, sub, &t, &loc, &fill, &h) == MOQ_OK);
    int origin_requests = 0;
    for (int i = 0; i < 12; i++) {
        relay_bind_test_rig_cycle(&rig);
        origin_requests += relay_bind_test_d22_fill_origin_tail(&rig, &t, true);
    }
    R_CHECK(&rig, origin_requests == 1);
    peer_state_t s_ps;
    memset(&s_ps, 0, sizeof(s_ps));
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.subscribe_ok);
    R_CHECK(&rig, s_ps.has_largest && s_ps.largest_group == 1 &&
                      s_ps.largest_object == 2);
    R_CHECK(&rig, s_ps.objects == 0);         /* nothing retained passes */
    R_CHECK(&rig, s_ps.fill_objects == 6);    /* the whole retained range */
    R_CHECK(&rig, s_ps.fill_min_group == 0 && s_ps.fill_max_group == 1);
    R_CHECK(&rig, d22_fill_group0_tail_unknown(&s_ps));
    R_CHECK(&rig, s_ps.fill_terminals == 1);
    R_CHECK(&rig, s_ps.fill_term_reason == MOQ_FILL_COMPLETE);
    R_CHECK(&rig, !s_ps.done_seen);

    /* The subscription is untouched by the fill: the live edge still flows. */
    relay_bind_test_pub_subgroup_n(&rig, t.pub, t.pub_ps.up_sub, 2, 0, 3, false);
    relay_bind_test_rig_pump(&rig, 8);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.objects == 3 && dlv_min_group(&s_ps) == 2);
    R_CHECK(&rig, s_ps.fill_terminals == 1);
    R_CHECK(&rig, !s_ps.session_closed);

    /* Relay-side bookkeeping is released: the binding reports no error. */
    moqr_bind_stats_t bs;
    moqr_bind_get_stats(rig.bind, &bs);
    R_CHECK(&rig, bs.session_errors == 0);

    int f = rig.failures;
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (f == 0 && rig.failures == 0) {
        printf("PASS: draft22_fill_served\n");
    }
    return f + rig.failures;
}

/* The origin advertises a catalog published before the relay subscribed:
 * Largest is {2,0}, but the relay has never ingested an Object. A relative
 * fill must retrieve that exact Object, not wait for it on the live feed. */
static int
draft22_fill_before_upstream_subscription(bool refuse_fetch, bool descending)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) return 1;
    d22_track_t t;
    memset(&t, 0, sizeof(t));
    t.nsp[0] = B("late");
    t.nsp[1] = B("catalog");
    t.pub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_22);
    conn_t *sub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_22);
    R_CHECK(&rig, t.pub != NULL && sub != NULL);
    if (t.pub == NULL || sub == NULL) goto done;
    relay_bind_test_rig_pump(&rig, 4);
    moq_publish_namespace_cfg_t pcfg;
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = t.nsp, .count = 2 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(t.pub->peer, &pcfg,
                                                rig.now, &ann) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 4);
    moq_location_filter_cfg_t loc, floc;
    moq_location_filter_cfg_init(&loc);
    loc.kind = MOQ_LOCATION_FILTER_NEXT_OBJECT;
    moq_location_filter_cfg_init(&floc);
    floc.kind = MOQ_LOCATION_FILTER_RELATIVE_GROUP;
    floc.relative_group_offset = 1;
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.location = &floc;
    fill.has_fill_timeout = true;
    fill.fill_timeout_ms = 500;
    moq_subscription_t h;
    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = t.nsp, .count = 2 };
    scfg.track_name = B("v");
    scfg.location = &loc;
    scfg.fill = &fill;
    scfg.group_order = descending ? MOQ_GROUP_ORDER_DESCENDING
                                  : MOQ_GROUP_ORDER_ASCENDING;
    R_CHECK(&rig, moq_session_subscribe(sub->peer, &scfg, rig.now, &h) == MOQ_OK);
    static const uint8_t body[] = "retained catalog";
    unsigned requests = 0, objects = 0, terminals = 0, acks = 0;
    moq_subscription_t upstream = {0};
    for (unsigned cycle = 0; cycle < 40; cycle++) {
        relay_bind_test_rig_cycle(&rig);
        moq_event_t ev;
        while (moq_session_poll_events(t.pub->peer, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                upstream = ev.u.subscribe_request.sub;
                moq_accept_subscribe_cfg_t ac;
                moq_accept_subscribe_cfg_init(&ac);
                ac.has_largest = true;
                ac.largest_group = 2;
                R_CHECK(&rig, moq_session_accept_subscribe(t.pub->peer,
                    upstream, &ac, rig.now) == MOQ_OK);
            } else if (ev.kind == MOQ_EVENT_FETCH_REQUEST) {
                const moq_fetch_request_event_t *q = &ev.u.fetch_request;
                moq_fetch_t fh = q->fetch;
                requests++;
                R_CHECK(&rig, requests == 1 && q->start_group == 2 &&
                    q->start_object == 0 && q->end_group == 2 &&
                    q->end_object == 1 && q->track_name.len == 1 &&
                    q->track_name.data[0] == 'v');
                /* A single-group origin request uses Object order; the
                 * fill's descending order chooses which group comes next. */
                R_CHECK(&rig, q->group_order == MOQ_GROUP_ORDER_ASCENDING);
                R_CHECK(&rig, q->options != NULL &&
                    (q->options->flags & MOQ_FETCH_REQUEST_HAS_FILL_TIMEOUT) == 0);
                if (refuse_fetch) {
                    moq_reject_fetch_cfg_t reject;
                    moq_reject_fetch_cfg_init(&reject);
                    reject.error_code = MOQ_REQUEST_ERROR_NOT_SUPPORTED;
                    reject.reason = B("FETCH not supported");
                    R_CHECK(&rig, moq_session_reject_fetch(t.pub->peer, fh,
                        &reject, rig.now) == MOQ_OK);
                    moq_event_cleanup(&ev);
                    continue;
                }
                moq_accept_fetch_cfg_t ac;
                moq_accept_fetch_cfg_init(&ac);
                ac.end_group = 2;
                ac.end_object = 1;
                R_CHECK(&rig, moq_session_accept_fetch(t.pub->peer, fh,
                                                        &ac, rig.now) == MOQ_OK);
                moq_fetch_object_cfg_t oc;
                moq_fetch_object_cfg_init(&oc);
                oc.group_id = 2;
                R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body),
                                                 &oc.payload) == MOQ_OK);
                moq_result_t rc = moq_session_write_fetch_object(t.pub->peer,
                    fh, &oc, rig.now);
                moq_rcbuf_decref(oc.payload);
                R_CHECK(&rig, rc == MOQ_OK);
                R_CHECK(&rig, moq_session_end_fetch(t.pub->peer, fh,
                                                     rig.now) == MOQ_OK);
            }
            moq_event_cleanup(&ev);
        }
        while (moq_session_poll_events(sub->peer, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_OK) {
                acks++;
                R_CHECK(&rig, ev.u.subscribe_ok.has_largest &&
                    ev.u.subscribe_ok.largest_group == 2 &&
                    ev.u.subscribe_ok.largest_object == 0);
            } else if (ev.kind == MOQ_EVENT_FILL_OBJECT) {
                objects++;
                R_CHECK(&rig, ev.u.fill_object.group_id == 2 &&
                    ev.u.fill_object.object_id == 0 &&
                    moq_rcbuf_len(ev.u.fill_object.payload) == sizeof(body) &&
                    memcmp(moq_rcbuf_data(ev.u.fill_object.payload), body,
                           sizeof(body)) == 0);
            } else if (ev.kind == MOQ_EVENT_FILL_TERMINAL) {
                terminals++;
                R_CHECK(&rig, ev.u.fill_terminal.reason ==
                    (refuse_fetch ? MOQ_FILL_PEER_CANCELLED_OR_RESET
                                    : MOQ_FILL_COMPLETE));
                R_CHECK(&rig, ev.u.fill_terminal.error_domain ==
                    (refuse_fetch ? MOQ_FILL_ERROR_STREAM : MOQ_FILL_ERROR_NONE));
                R_CHECK(&rig, ev.u.fill_terminal.error_code ==
                    (refuse_fetch ? MOQ_STREAM_ERROR_UNKNOWN_OBJECT_STATUS : 0));
            } else {
                R_CHECK(&rig, ev.kind != MOQ_EVENT_OBJECT_RECEIVED &&
                    ev.kind != MOQ_EVENT_FILL_GAP &&
                    ev.kind != MOQ_EVENT_SUBSCRIBE_ERROR &&
                    ev.kind != MOQ_EVENT_SESSION_CLOSED);
            }
            moq_event_cleanup(&ev);
        }
    }
    R_CHECK(&rig, requests == 1 && objects == (refuse_fetch ? 0u : 1u) &&
        terminals == 1 && acks == 1);
    moqr_bind_fwd_debug_t st;
    moqr_bind_debug_fwd_stats(rig.bind, &st);
    R_CHECK(&rig, st.live == 0 && st.bytes_used == 0);
    R_CHECK(&rig, moq_session_state(sub->peer) == MOQ_SESS_ESTABLISHED &&
        moq_session_state(t.pub->peer) == MOQ_SESS_ESTABLISHED);
    relay_bind_test_pub_subgroup_n(&rig, t.pub, upstream, 3, 0, 1, false);
    relay_bind_test_rig_pump(&rig, 8);
    peer_state_t ps = {0};
    relay_bind_test_peer_drain(&rig, sub, &ps, false, false, 0, 0);
    R_CHECK(&rig, ps.objects == 1 && ps.dlv_n == 1 && ps.dlv[0].group == 3);
done:
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (rig.failures == 0)
        printf("PASS: draft22_fill_before_upstream_subscription(%s,%s)\n",
               refuse_fetch ? "origin-refuses-fetch" : "exact-object",
               descending ? "descending" : "ascending");
    return rig.failures;
}

/* Withhold that same origin response: at the original observation times,
 * the fill must still owe group 0's tail, not invent Unknown or complete. */
static int
draft22_fill_origin_unanswered(void)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: draft22_fill_origin_unanswered rig create\n");
        return 1;
    }
    d22_track_t t;
    relay_bind_test_d22_track_setup(&rig, &t, MOQ_VERSION_DRAFT_22, 2);
    conn_t *sub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_22);
    R_CHECK(&rig, sub != NULL);
    relay_bind_test_rig_pump(&rig, 4);
    moq_location_filter_cfg_t loc, floc;
    moq_location_filter_cfg_init(&loc);
    loc.kind = MOQ_LOCATION_FILTER_NEXT_OBJECT;
    moq_location_filter_cfg_init(&floc);
    floc.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.location = &floc;
    moq_subscription_t h;
    R_CHECK(&rig, relay_bind_test_d22_subscribe(&rig, sub, &t, &loc, &fill, &h) == MOQ_OK);
    uint64_t start = rig.now;
    int origin_requests = 0;
    for (int i = 0; i < 12; i++) {
        relay_bind_test_rig_cycle(&rig);
        origin_requests += relay_bind_test_d22_fill_origin_tail(&rig, &t, false);
    }
    R_CHECK(&rig, origin_requests == 1 && rig.now == start + 12000u);
    peer_state_t s_ps;
    memset(&s_ps, 0, sizeof(s_ps));
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.subscribe_ok && s_ps.has_largest &&
                      s_ps.largest_group == 1 && s_ps.largest_object == 2);
    R_CHECK(&rig, s_ps.objects == 0 && s_ps.fill_objects == 3);
    R_CHECK(&rig, s_ps.fill_min_group == 0 && s_ps.fill_max_group == 0);
    R_CHECK(&rig, s_ps.fill_gaps == 0 && s_ps.fill_terminals == 0);
    uint64_t deadline = moqr_bind_next_deadline_us(rig.bind);
    R_CHECK(&rig, deadline != UINT64_MAX && deadline > rig.now);

    relay_bind_test_pub_subgroup_n(&rig, t.pub, t.pub_ps.up_sub, 2, 0, 3, false);
    relay_bind_test_rig_pump(&rig, 8);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, rig.now == start + 20000u && rig.now < deadline);
    R_CHECK(&rig, moqr_bind_next_deadline_us(rig.bind) == deadline);
    R_CHECK(&rig, s_ps.objects == 3 && dlv_min_group(&s_ps) == 2);
    R_CHECK(&rig, s_ps.fill_objects == 3 && s_ps.fill_gaps == 0 &&
                      s_ps.fill_terminals == 0);
    R_CHECK(&rig, !s_ps.done_seen && !s_ps.session_closed);
    moqr_bind_stats_t bs;
    moqr_bind_get_stats(rig.bind, &bs);
    R_CHECK(&rig, bs.session_errors == 0);
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (rig.failures == 0) {
        printf("PASS: draft22_fill_origin_unanswered\n");
    }
    return rig.failures;
}

typedef struct d22_fill_origin_range {
    uint64_t start_group, start_object, end_group, end_object;
    bool answer_unknown;
    unsigned requests;
} d22_fill_origin_range_t;

static unsigned
d22_fill_origin_ranges(rig_t *rig, d22_track_t *t,
                       d22_fill_origin_range_t *ranges, size_t count)
{
    unsigned events = 0;
    for (; events < 16; events++) {
        moq_event_t ev;
        if (moq_session_poll_events(t->pub->peer, &ev, 1) == 0) {
            break;
        }
        if (ev.kind == MOQ_EVENT_FETCH_REQUEST) {
            const moq_fetch_request_event_t *q = &ev.u.fetch_request;
            moq_fetch_t fetch = q->fetch;
            d22_fill_origin_range_t *range = NULL;
            for (size_t i = 0; i < count; i++) {
                if (q->start_group == ranges[i].start_group &&
                    q->start_object == ranges[i].start_object &&
                    q->end_group == ranges[i].end_group &&
                    q->end_object == ranges[i].end_object) {
                    range = &ranges[i];
                    break;
                }
            }
            R_CHECK(rig, range != NULL);
            R_CHECK(rig, q->joining_sub._opaque == 0 && q->joining_pub._opaque == 0);
            if (range != NULL) {
                range->requests++;
                R_CHECK(rig, range->requests == 1);
                if (range->answer_unknown) {
                    moq_accept_fetch_cfg_t accept;
                    moq_accept_fetch_cfg_init(&accept);
                    accept.end_group = range->end_group;
                    accept.end_object = range->end_object;
                    R_CHECK(rig, moq_session_accept_fetch(t->pub->peer, fetch,
                                                          &accept, rig->now) == MOQ_OK);
                    R_CHECK(rig, moq_session_write_fetch_range(
                                     t->pub->peer, fetch, MOQ_FETCH_RANGE_UNKNOWN,
                                     range->end_group,
                                     range->end_object == 0 ? UINT64_MAX
                                                           : range->end_object - 1,
                                     rig->now) == MOQ_OK);
                    R_CHECK(rig, moq_session_end_fetch(t->pub->peer, fetch,
                                                       rig->now) == MOQ_OK);
                }
            }
        }
        moq_event_cleanup(&ev);
    }
    R_CHECK(rig, events < 16);
    return events;
}

/* Service only origin transport work and the binding's public self-wake at
 * the current clock. Do not drain the subscriber or extend a timeout window. */
static void
d22_fill_origin_pump(rig_t *rig, d22_track_t *t,
                     d22_fill_origin_range_t *ranges, size_t count, int cycles)
{
    uint64_t start = rig->now;
    for (int cycle = 0; cycle < cycles; cycle++) {
        relay_bind_test_rig_cycle(rig);
        bool settled = false;
        for (unsigned wake = 0; wake < 16; wake++) {
            size_t work = 0;
            R_CHECK(rig, moq_simpair_step(t->pub->sp, &work) == MOQ_OK);
            unsigned events = d22_fill_origin_ranges(rig, t, ranges, count);
            if (work == 0 && events == 0 &&
                !moqr_bind_pump_budget_pending(rig->bind)) {
                settled = true;
                break;
            }
            R_CHECK(rig, moqr_bind_pump(rig->bind, rig->now) == MOQR_OK);
        }
        R_CHECK(rig, settled);
    }
    R_CHECK(rig, rig->now == start + (uint64_t)cycles * 1000u);
}

/* The fill over relay_bind_test_d22_hole_setup's track: an origin UNKNOWN response resolves
 * group 0's plain-FIN tail, but {1,1} stays unanswered until its deadline.
 * Timeout zero requests neither range and marks both Timed-Out. */
static bool
d22_fill_timed_out_at(const peer_state_t *ps, uint32_t tail, uint64_t group,
                      uint64_t object)
{
    const d22_gap_t want[2] = {
        { tail, 0, UINT64_MAX },
        { MOQ_FETCH_RANGE_TIMED_OUT, group, object },
    };
    return d22_fill_gaps_are(ps, want, 2);
}


/* FILL_TIMEOUT of zero over a partially retained range: the relay serves what
 * it holds, marks the Object it does not hold as one End of Timed-Out Range
 * at that Location, and ends the stream normally. The subscription is
 * untouched. */
static int
draft22_fill_timeout_zero_partial(void)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: draft22_fill_timeout_zero_partial rig create\n");
        return 1;
    }
    d22_track_t t;
    relay_bind_test_d22_hole_setup(&rig, &t);
    R_CHECK(&rig, t.prime_ps.objects == 5);
    moqr_bind_fwd_debug_t before, after;
    moqr_bind_debug_fwd_stats(rig.bind, &before);
    conn_t *sub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_22);
    R_CHECK(&rig, sub != NULL);
    relay_bind_test_rig_pump(&rig, 4);

    moq_subscription_t h;
    R_CHECK(&rig, relay_bind_test_d22_subscribe_fill(&rig, sub, &t, true, 0, &h) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 12);
    peer_state_t s_ps;
    memset(&s_ps, 0, sizeof(s_ps));
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.subscribe_ok);
    R_CHECK(&rig, s_ps.has_largest && s_ps.largest_group == 1 &&
                      s_ps.largest_object == 2);
    R_CHECK(&rig, s_ps.objects == 0);
    R_CHECK(&rig, s_ps.fill_objects == 5);
    R_CHECK(&rig, s_ps.fill_min_group == 0 && s_ps.fill_max_group == 1);
    R_CHECK(&rig, d22_fill_timed_out_at(&s_ps, MOQ_FETCH_RANGE_TIMED_OUT, 1, 1));
    R_CHECK(&rig, s_ps.fill_terminals == 1);
    R_CHECK(&rig, s_ps.fill_term_reason == MOQ_FILL_COMPLETE);
    R_CHECK(&rig, !s_ps.done_seen && !s_ps.session_closed);
    R_CHECK(&rig, moqr_bind_next_deadline_us(rig.bind) == UINT64_MAX);
    moqr_bind_debug_fwd_stats(rig.bind, &after);
    R_CHECK(&rig, after.admitted == before.admitted && after.live == 0);

    /* The subscription lives on at the live edge. */
    relay_bind_test_pub_subgroup_n(&rig, t.pub, t.pub_ps.up_sub, 2, 0, 3, false);
    relay_bind_test_rig_pump(&rig, 8);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.objects == 3 && dlv_min_group(&s_ps) == 2);
    R_CHECK(&rig, s_ps.fill_terminals == 1 && s_ps.fill_objects == 5);
    moqr_bind_stats_t bs;
    moqr_bind_get_stats(rig.bind, &bs);
    R_CHECK(&rig, bs.session_errors == 0);

    int f = rig.failures;
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (f == 0 && rig.failures == 0) {
        printf("PASS: draft22_fill_timeout_zero_partial\n");
    }
    return f + rig.failures;
}

/* A nonzero FILL_TIMEOUT holds the fill at the Object it does not hold -- the
 * Objects before it are already written -- and upstream delivering it before
 * the deadline closes the run: every Object, no marker, a clean end. The
 * pending deadline is what the binding reports as its next wake. */
static int
draft22_fill_timeout_waits_for_upstream(void)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: draft22_fill_timeout_waits_for_upstream rig create\n");
        return 1;
    }
    d22_track_t t;
    relay_bind_test_d22_hole_setup(&rig, &t);
    conn_t *sub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_22);
    R_CHECK(&rig, sub != NULL);
    relay_bind_test_rig_pump(&rig, 4);

    moq_subscription_t h;
    R_CHECK(&rig, relay_bind_test_d22_subscribe_fill(&rig, sub, &t, true, 20, &h) == MOQ_OK);
    d22_fill_origin_range_t origin[] = {
        { .start_group = 0, .start_object = 3,
          .end_group = 0, .end_object = 0, .answer_unknown = true },
        { .start_group = 1, .start_object = 1,
          .end_group = 1, .end_object = 2, .answer_unknown = false }
    };
    d22_fill_origin_pump(&rig, &t, origin, 2, 4);
    R_CHECK(&rig, origin[0].requests == 1 && origin[1].requests == 1);
    peer_state_t s_ps;
    memset(&s_ps, 0, sizeof(s_ps));
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.subscribe_ok);
    R_CHECK(&rig, s_ps.fill_objects == 4);    /* {0,0..2} and {1,0} */
    R_CHECK(&rig, d22_fill_group0_tail_unknown(&s_ps));
    R_CHECK(&rig, s_ps.fill_terminals == 0);
    uint64_t deadline = moqr_bind_next_deadline_us(rig.bind);
    R_CHECK(&rig, deadline != UINT64_MAX && deadline > rig.now);
    R_CHECK(&rig, deadline <= rig.now + 20000u);

    /* Upstream delivers {1,1} on another subgroup: the run closes. */
    static const uint64_t late[1] = { 1 };
    relay_bind_test_d22_pub_ids(&rig, &t, 1, 1, late, 1);
    relay_bind_test_rig_pump(&rig, 4);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.fill_objects == 6);
    R_CHECK(&rig, d22_fill_group0_tail_unknown(&s_ps));
    R_CHECK(&rig, s_ps.fill_terminals == 1 &&
                      s_ps.fill_term_reason == MOQ_FILL_COMPLETE);
    R_CHECK(&rig, moqr_bind_next_deadline_us(rig.bind) == UINT64_MAX);
    R_CHECK(&rig, !s_ps.done_seen && !s_ps.session_closed);
    moqr_bind_stats_t bs;
    moqr_bind_get_stats(rig.bind, &bs);
    R_CHECK(&rig, bs.session_errors == 0);

    int f = rig.failures;
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (f == 0 && rig.failures == 0) {
        printf("PASS: draft22_fill_timeout_waits_for_upstream\n");
    }
    return f + rig.failures;
}

/* The deadline passes with the Object still missing: the run is marked
 * Timed-Out at its Location, the rest of the range follows and the stream
 * ends normally. The Object arriving afterwards changes nothing on the fill. */
static int
draft22_fill_timeout_expires(void)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: draft22_fill_timeout_expires rig create\n");
        return 1;
    }
    d22_track_t t;
    relay_bind_test_d22_hole_setup(&rig, &t);
    conn_t *sub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_22);
    R_CHECK(&rig, sub != NULL);
    relay_bind_test_rig_pump(&rig, 4);

    moq_subscription_t h;
    R_CHECK(&rig, relay_bind_test_d22_subscribe_fill(&rig, sub, &t, true, 10, &h) == MOQ_OK);
    d22_fill_origin_range_t origin[] = {
        { .start_group = 0, .start_object = 3,
          .end_group = 0, .end_object = 0, .answer_unknown = true },
        { .start_group = 1, .start_object = 1,
          .end_group = 1, .end_object = 2, .answer_unknown = false }
    };
    d22_fill_origin_pump(&rig, &t, origin, 2, 4);
    R_CHECK(&rig, origin[0].requests == 1 && origin[1].requests == 1);
    peer_state_t s_ps;
    memset(&s_ps, 0, sizeof(s_ps));
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.fill_objects == 4 && s_ps.fill_terminals == 0);
    uint64_t deadline = moqr_bind_next_deadline_us(rig.bind);
    R_CHECK(&rig, deadline != UINT64_MAX && deadline > rig.now);

    relay_bind_test_rig_pump(&rig, 12);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, rig.now >= deadline);
    R_CHECK(&rig, s_ps.fill_objects == 5);
    R_CHECK(&rig, d22_fill_timed_out_at(&s_ps, MOQ_FETCH_RANGE_UNKNOWN, 1, 1));
    R_CHECK(&rig, s_ps.fill_terminals == 1 &&
                      s_ps.fill_term_reason == MOQ_FILL_COMPLETE);
    R_CHECK(&rig, moqr_bind_next_deadline_us(rig.bind) == UINT64_MAX);

    static const uint64_t late[1] = { 1 };
    relay_bind_test_d22_pub_ids(&rig, &t, 1, 1, late, 1);
    relay_bind_test_rig_pump(&rig, 4);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.fill_objects == 5 && s_ps.fill_terminals == 1);
    R_CHECK(&rig, s_ps.objects == 0);   /* Next Object: below its start */
    R_CHECK(&rig, !s_ps.done_seen && !s_ps.session_closed);
    moqr_bind_stats_t bs;
    moqr_bind_get_stats(rig.bind, &bs);
    R_CHECK(&rig, bs.session_errors == 0);

    int f = rig.failures;
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (f == 0 && rig.failures == 0) {
        printf("PASS: draft22_fill_timeout_expires\n");
    }
    return f + rig.failures;
}

/* The bind cfg ends at fetch_forward_bytes and never grows (the shards
 * config embeds it), so a caller built against that layout passes the
 * current sizeof, and nothing past it is read -- here an exact heap block of
 * that size. A binding whose creator never sets the fill wait, as no such caller
 * can, waits the named defaults: a fill without FILL_TIMEOUT waits
 * MOQR_BIND_DEF_FILL_TIMEOUT_MS and a longer one is capped at
 * MOQR_BIND_DEF_FILL_TIMEOUT_MAX_MS. */
static int
draft22_fill_timeout_old_cfg(void)
{
    const size_t end = offsetof(moqr_bind_cfg_t, fetch_forward_bytes) +
                       sizeof(uint64_t);
    const size_t align = _Alignof(moqr_bind_cfg_t);
    const size_t old = (end + align - 1) / align * align;
    int failures = 0;
    MOQ_TEST_CHECK(sizeof(moqr_bind_cfg_t) == old);
    for (int exact = 0; exact < 2; exact++) {
        for (int timed = 0; timed < 2; timed++) {
            ca_t a;
            relay_bind_test_ca_init(&a);
            relay_bind_test_bind_cfg_size = old;
            relay_bind_test_bind_cfg_exact = exact != 0;
            rig_t rig;
            moqr_result_t rc = relay_bind_test_rig_create(&rig, &a);
            relay_bind_test_bind_cfg_size = 0;
            relay_bind_test_bind_cfg_exact = false;
            if (rc != MOQR_OK) {
                printf("FAIL: draft22_fill_timeout_old_cfg rig create\n");
                failures++;
                continue;
            }
            d22_track_t t;
            relay_bind_test_d22_hole_setup(&rig, &t);
            conn_t *sub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_22);
            R_CHECK(&rig, sub != NULL);
            relay_bind_test_rig_pump(&rig, 4);

            moq_subscription_t h;
            R_CHECK(&rig, relay_bind_test_d22_subscribe_fill(&rig, sub, &t, timed != 0, 1000000,
                                                             &h) == MOQ_OK);
            d22_fill_origin_range_t origin[] = {
                { .start_group = 0, .start_object = 3,
                  .end_group = 0, .end_object = 0, .answer_unknown = true },
                { .start_group = 1, .start_object = 1,
                  .end_group = 1, .end_object = 2, .answer_unknown = false }
            };
            d22_fill_origin_pump(&rig, &t, origin, 2, 3);
            R_CHECK(&rig, origin[0].requests == 1 && origin[1].requests == 1);
            peer_state_t s_ps;
            memset(&s_ps, 0, sizeof(s_ps));
            relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
            R_CHECK(&rig, s_ps.fill_objects == 4 && s_ps.fill_terminals == 0);
            const uint64_t wait_us = (timed ? MOQR_BIND_DEF_FILL_TIMEOUT_MAX_MS
                                            : MOQR_BIND_DEF_FILL_TIMEOUT_MS) * 1000u;
            uint64_t deadline = moqr_bind_next_deadline_us(rig.bind);
            R_CHECK(&rig, deadline != UINT64_MAX);
            /* processed within the last few pump cycles of 1 ms each */
            R_CHECK(&rig, deadline <= rig.now + wait_us);
            R_CHECK(&rig, deadline + 10000u > rig.now + wait_us);

            int f = rig.failures;
            relay_bind_test_rig_destroy(&rig);
            R_CHECK(&rig, a.live == 0);
            failures += f + rig.failures;
        }
    }
    if (failures == 0) {
        printf("PASS: draft22_fill_timeout_old_cfg\n");
    }
    return failures;
}

/* A requested FILL_TIMEOUT above the relay's maximum is shortened to it
 * without telling the subscriber: the fill ends at the relay's deadline. */
static int
draft22_fill_timeout_capped(void)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    relay_bind_test_fill_timeout_max_ms = 5;
    relay_bind_test_fill_timeout_default_ms = 5;
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        relay_bind_test_fill_timeout_max_ms = 0;
        relay_bind_test_fill_timeout_default_ms = 0;
        printf("FAIL: draft22_fill_timeout_capped rig create\n");
        return 1;
    }
    d22_track_t t;
    relay_bind_test_d22_hole_setup(&rig, &t);
    conn_t *sub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_22);
    R_CHECK(&rig, sub != NULL);
    relay_bind_test_rig_pump(&rig, 4);

    /* refused pairs leave the applied one in place */
    R_CHECK(&rig, moqr_bind_set_fill_timeouts(rig.bind, 20, 10) ==
                      MOQR_ERR_INVAL);
    R_CHECK(&rig, moqr_bind_set_fill_timeouts(rig.bind, 0, UINT64_MAX) ==
                      MOQR_ERR_INVAL);

    moq_subscription_t h;
    R_CHECK(&rig, relay_bind_test_d22_subscribe_fill(&rig, sub, &t, true, 1000000, &h) ==
                      MOQ_OK);
    d22_fill_origin_range_t origin[] = {
        { .start_group = 0, .start_object = 3,
          .end_group = 0, .end_object = 0, .answer_unknown = true },
        { .start_group = 1, .start_object = 1,
          .end_group = 1, .end_object = 2, .answer_unknown = false }
    };
    d22_fill_origin_pump(&rig, &t, origin, 2, 3);
    R_CHECK(&rig, origin[0].requests == 1 && origin[1].requests == 1);
    peer_state_t s_ps;
    memset(&s_ps, 0, sizeof(s_ps));
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.fill_objects == 4 && s_ps.fill_terminals == 0);
    uint64_t deadline = moqr_bind_next_deadline_us(rig.bind);
    R_CHECK(&rig, deadline != UINT64_MAX);
    R_CHECK(&rig, deadline <= rig.now + 5000u);   /* the cap, not 1000 s */

    relay_bind_test_rig_pump(&rig, 10);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.fill_objects == 5);
    R_CHECK(&rig, d22_fill_timed_out_at(&s_ps, MOQ_FETCH_RANGE_UNKNOWN, 1, 1));
    R_CHECK(&rig, s_ps.fill_terminals == 1 &&
                      s_ps.fill_term_reason == MOQ_FILL_COMPLETE);
    R_CHECK(&rig, !s_ps.done_seen && !s_ps.session_closed);

    relay_bind_test_fill_timeout_max_ms = 0;
    relay_bind_test_fill_timeout_default_ms = 0;
    int f = rig.failures;
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (f == 0 && rig.failures == 0) {
        printf("PASS: draft22_fill_timeout_capped\n");
    }
    return f + rig.failures;
}

/* A fill without FILL_TIMEOUT waits the relay's own configured duration
 * before marking a run Timed-Out. */
static int
draft22_fill_absent_timeout_default(void)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    relay_bind_test_fill_timeout_default_ms = 4;
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        relay_bind_test_fill_timeout_default_ms = 0;
        printf("FAIL: draft22_fill_absent_timeout_default rig create\n");
        return 1;
    }
    d22_track_t t;
    relay_bind_test_d22_hole_setup(&rig, &t);
    conn_t *sub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_22);
    R_CHECK(&rig, sub != NULL);
    relay_bind_test_rig_pump(&rig, 4);

    moq_subscription_t h;
    R_CHECK(&rig, relay_bind_test_d22_subscribe_fill(&rig, sub, &t, false, 0, &h) == MOQ_OK);
    d22_fill_origin_range_t origin[] = {
        { .start_group = 0, .start_object = 3,
          .end_group = 0, .end_object = 0, .answer_unknown = true },
        { .start_group = 1, .start_object = 1,
          .end_group = 1, .end_object = 2, .answer_unknown = false }
    };
    d22_fill_origin_pump(&rig, &t, origin, 2, 3);
    R_CHECK(&rig, origin[0].requests == 1 && origin[1].requests == 1);
    peer_state_t s_ps;
    memset(&s_ps, 0, sizeof(s_ps));
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.fill_objects == 4 && s_ps.fill_terminals == 0);
    uint64_t deadline = moqr_bind_next_deadline_us(rig.bind);
    R_CHECK(&rig, deadline != UINT64_MAX && deadline <= rig.now + 4000u);

    relay_bind_test_rig_pump(&rig, 10);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.fill_objects == 5);
    R_CHECK(&rig, d22_fill_timed_out_at(&s_ps, MOQ_FETCH_RANGE_UNKNOWN, 1, 1));
    R_CHECK(&rig, s_ps.fill_terminals == 1 &&
                      s_ps.fill_term_reason == MOQ_FILL_COMPLETE);
    R_CHECK(&rig, !s_ps.done_seen && !s_ps.session_closed);

    relay_bind_test_fill_timeout_default_ms = 0;
    int f = rig.failures;
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (f == 0 && rig.failures == 0) {
        printf("PASS: draft22_fill_absent_timeout_default\n");
    }
    return f + rig.failures;
}

/* The fill wait pair is validated by its setter: a default above the
 * maximum is refused, whether set explicitly or left at its named default,
 * as is a maximum too large to express in microseconds or a NULL binding. A
 * binding no one set it on is created with the named pair. */
static int
bind_fill_wait_cfg(void)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: bind_fill_wait_cfg rig create\n");
        return 1;
    }
    R_CHECK(&rig, moqr_bind_set_fill_timeouts(NULL, 0, 0) == MOQR_ERR_INVAL);
    R_CHECK(&rig, moqr_bind_set_fill_timeouts(rig.bind, 20, 10) ==
                      MOQR_ERR_INVAL);
    /* the named default, 1000 ms, above an explicit 500 ms maximum */
    R_CHECK(&rig, moqr_bind_set_fill_timeouts(rig.bind, 0, 500) ==
                      MOQR_ERR_INVAL);
    R_CHECK(&rig, moqr_bind_set_fill_timeouts(rig.bind, 1, UINT64_MAX) ==
                      MOQR_ERR_INVAL);
    R_CHECK(&rig, moqr_bind_set_fill_timeouts(rig.bind, 500, 500) == MOQR_OK);
    /* an explicit default under the named maximum */
    R_CHECK(&rig, moqr_bind_set_fill_timeouts(rig.bind, 500, 0) == MOQR_OK);
    /* back to the named pair */
    R_CHECK(&rig, moqr_bind_set_fill_timeouts(rig.bind, 0, 0) == MOQR_OK);

    int f = rig.failures;
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (f == 0 && rig.failures == 0) {
        printf("PASS: bind_fill_wait_cfg\n");
    }
    return f + rig.failures;
}

/* A fill the relay cannot serve at all -- here the second of two fills with
 * one retained-fetch cursor to share -- fails on the wire: the fill fetch
 * stream opens with its FETCH_HEADER and is reset immediately. Neither
 * subscription is affected, and the other fill is served whole. */
static int
draft22_fill_failed_on_wire(void)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    relay_bind_test_max_fetches = 1;
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        relay_bind_test_max_fetches = 0;
        printf("FAIL: draft22_fill_failed_on_wire rig create\n");
        return 1;
    }
    d22_track_t t;
    relay_bind_test_d22_track_setup(&rig, &t, MOQ_VERSION_DRAFT_22, 2);
    conn_t *s1 = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_22);
    conn_t *s2 = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_22);
    R_CHECK(&rig, s1 != NULL && s2 != NULL);
    relay_bind_test_rig_pump(&rig, 4);

    moq_subscription_t h1, h2;
    R_CHECK(&rig, relay_bind_test_d22_subscribe_fill(&rig, s1, &t, false, 0, &h1) == MOQ_OK);
    R_CHECK(&rig, relay_bind_test_d22_subscribe_fill(&rig, s2, &t, false, 0, &h2) == MOQ_OK);
    d22_fill_origin_range_t origin[] = {
        { .start_group = 0, .start_object = 3,
          .end_group = 0, .end_object = 0, .answer_unknown = true }
    };
    d22_fill_origin_pump(&rig, &t, origin, 1, 12);
    R_CHECK(&rig, origin[0].requests == 1);
    peer_state_t p1, p2;
    memset(&p1, 0, sizeof(p1));
    memset(&p2, 0, sizeof(p2));
    relay_bind_test_peer_drain(&rig, s1, &p1, false, false, 0, 0);
    relay_bind_test_peer_drain(&rig, s2, &p2, false, false, 0, 0);
    R_CHECK(&rig, p1.subscribe_ok && p2.subscribe_ok);
    R_CHECK(&rig, p1.fill_terminals == 1 && p2.fill_terminals == 1);
    const peer_state_t *served = p1.fill_objects == 6 ? &p1 : &p2;
    const peer_state_t *failed = served == &p1 ? &p2 : &p1;
    R_CHECK(&rig, served->fill_objects == 6 &&
                      d22_fill_group0_tail_unknown(served));
    R_CHECK(&rig, served->fill_term_reason == MOQ_FILL_COMPLETE);
    R_CHECK(&rig, failed->fill_objects == 0 && failed->fill_gaps == 0);
    R_CHECK(&rig, failed->fill_term_reason == MOQ_FILL_PEER_CANCELLED_OR_RESET);
    R_CHECK(&rig, failed->fill_term_domain == MOQ_FILL_ERROR_STREAM);
    R_CHECK(&rig, failed->fill_term_code == 0x0u);   /* reset INTERNAL_ERROR */
    R_CHECK(&rig, !p1.done_seen && !p1.session_closed);
    R_CHECK(&rig, !p2.done_seen && !p2.session_closed);

    relay_bind_test_pub_subgroup_n(&rig, t.pub, t.pub_ps.up_sub, 2, 0, 3, false);
    relay_bind_test_rig_pump(&rig, 8);
    relay_bind_test_peer_drain(&rig, s1, &p1, false, false, 0, 0);
    relay_bind_test_peer_drain(&rig, s2, &p2, false, false, 0, 0);
    R_CHECK(&rig, p1.objects == 3 && p2.objects == 3);   /* both live on */
    R_CHECK(&rig, p1.fill_terminals == 1 && p2.fill_terminals == 1);
    moqr_bind_stats_t bs;
    moqr_bind_get_stats(rig.bind, &bs);
    R_CHECK(&rig, bs.session_errors == 0);

    relay_bind_test_max_fetches = 0;
    int f = rig.failures;
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (f == 0 && rig.failures == 0) {
        printf("PASS: draft22_fill_failed_on_wire\n");
    }
    return f + rig.failures;
}

/* A draft-22 subscriber replaces its Location Filter on an update: the relay
 * forwards from the new window (a later absolute start skips the groups
 * before it); lowering the start afterwards re-sends nothing the cursor has
 * passed and the live edge keeps flowing. */
static int
draft22_update_filter_applied(bool typed, bool automatic)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: draft22_update_filter_applied rig create\n");
        return 1;
    }
    d22_track_t t;
    relay_bind_test_d22_track_setup(&rig, &t, MOQ_VERSION_DRAFT_22, 2); /* largest {1,2} */
    conn_t *sub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_22);
    R_CHECK(&rig, sub != NULL);
    relay_bind_test_rig_pump(&rig, 4);

    moq_location_filter_cfg_t loc;
    moq_location_filter_cfg_init(&loc);
    loc.kind = MOQ_LOCATION_FILTER_NEXT_OBJECT;
    moq_subscription_t h;
    R_CHECK(&rig, relay_bind_test_d22_subscribe(&rig, sub, &t, &loc, NULL, &h) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 8);
    peer_state_t s_ps;
    memset(&s_ps, 0, sizeof(s_ps));
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.subscribe_ok && s_ps.objects == 0);

    if (automatic) {
        R_CHECK(&rig, moq_session_set_defer_subscription_updates(sub->rsess,
                                                                  false) == MOQ_OK);
    }
    /* Start at group 3: group 2 is outside the window, group 3 is served. */
    moq_subscription_update_cfg_t ucfg;
    moq_subscription_update_cfg_init_sized(&ucfg, sizeof(ucfg));
    if (typed) {
        loc.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;
        loc.start_group = 3;
        ucfg.location = &loc;
    } else {
        /* Accepted legacy input remains a compatibility control on D22. */
        ucfg.has_filter = true;
        ucfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
        ucfg.start_group = 3;
        ucfg.start_object = 0;
    }
    R_CHECK(&rig, moq_session_update_subscription(sub->peer, h, &ucfg,
                                                  rig.now) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 6);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.update_oks == 1);
    if (typed) {
        /* An empty typed-options record must not clear the selected filter. */
        moq_subscription_update_cfg_init_sized(&ucfg, sizeof(ucfg));
        R_CHECK(&rig, moq_session_update_subscription(sub->peer, h, &ucfg,
                                                      rig.now) == MOQ_OK);
        relay_bind_test_rig_pump(&rig, 6);
        relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
        R_CHECK(&rig, s_ps.update_oks == 2);
    }
    relay_bind_test_pub_subgroup_n(&rig, t.pub, t.pub_ps.up_sub, 2, 0, 3, false);
    relay_bind_test_rig_pump(&rig, 8);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.objects == 0);
    relay_bind_test_pub_subgroup_n(&rig, t.pub, t.pub_ps.up_sub, 3, 0, 3, false);
    relay_bind_test_rig_pump(&rig, 8);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.objects == 3 && dlv_min_group(&s_ps) == 3);
    R_CHECK(&rig, !s_ps.done_seen && !s_ps.session_closed);

    /* Lowering the start to {0,0}: nothing passed is re-sent; the live edge
     * still flows. */
    moq_subscription_update_cfg_init_sized(&ucfg, sizeof(ucfg));
    if (typed) {
        loc.start_group = 0;
        ucfg.location = &loc;
    } else {
        ucfg.has_filter = true;
        ucfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
        ucfg.start_group = 0;
        ucfg.start_object = 0;
    }
    R_CHECK(&rig, moq_session_update_subscription(sub->peer, h, &ucfg,
                                                  rig.now) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 8);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.objects == 3);
    relay_bind_test_pub_subgroup_n(&rig, t.pub, t.pub_ps.up_sub, 4, 0, 2, false);
    relay_bind_test_rig_pump(&rig, 8);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.objects == 5);
    if (typed) {
        R_CHECK(&rig, s_ps.update_oks == 3);
        loc.start_group = 8;
        R_CHECK(&rig, moq_session_update_subscription(sub->peer, h, &ucfg,
                                                      rig.now) == MOQ_OK);
        relay_bind_test_rig_pump(&rig, 6);
        relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
        R_CHECK(&rig, s_ps.update_oks == 4);
        relay_bind_test_pub_subgroup_n(&rig, t.pub, t.pub_ps.up_sub, 5, 0, 2, false);
        relay_bind_test_rig_pump(&rig, 8);
        relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
        R_CHECK(&rig, s_ps.objects == 5);

        /* Unlike omission above, an explicit NONE removes the start bound. */
        moq_location_filter_cfg_init(&loc);
        loc.kind = MOQ_LOCATION_FILTER_NONE;
        R_CHECK(&rig, moq_session_update_subscription(sub->peer, h, &ucfg,
                                                      rig.now) == MOQ_OK);
        relay_bind_test_rig_pump(&rig, 6);
        relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
        R_CHECK(&rig, s_ps.update_oks == 5 && s_ps.objects == 5);
        relay_bind_test_pub_subgroup_n(&rig, t.pub, t.pub_ps.up_sub, 6, 0, 2, false);
        relay_bind_test_rig_pump(&rig, 8);
        relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
        R_CHECK(&rig, s_ps.objects == 7 && s_ps.dlv_n == 7);
        if (s_ps.dlv_n == 7) {
            R_CHECK(&rig, s_ps.dlv[5].group == 6 &&
                              s_ps.dlv[5].object_id == 0);
            R_CHECK(&rig, s_ps.dlv[6].group == 6 &&
                              s_ps.dlv[6].object_id == 1);
        }
    }
    R_CHECK(&rig, !s_ps.done_seen && !s_ps.session_closed);
    moqr_bind_stats_t bs;
    moqr_bind_get_stats(rig.bind, &bs);
    R_CHECK(&rig, bs.session_errors == 0);

    int f = rig.failures;
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (f == 0 && rig.failures == 0) {
        printf("PASS: draft22_update_filter_applied(typed %d, automatic %d)\n",
               typed, automatic);
    }
    return f + rig.failures;
}

static void
d22_pub_one(rig_t *rig, const d22_track_t *t, uint64_t group,
            uint64_t subgroup, uint64_t object)
{
    moq_subgroup_cfg_t sc;
    moq_subgroup_cfg_init(&sc);
    sc.group_id = group;
    sc.subgroup_id = subgroup;
    moq_subgroup_handle_t sg;
    R_CHECK(rig, moq_session_open_subgroup(t->pub->peer, t->pub_ps.up_sub,
                                          &sc, rig->now, &sg) == MOQ_OK);
    uint8_t bytes[4] = { (uint8_t)group, (uint8_t)object, 0xC3, 0x3C };
    moq_rcbuf_t *pl = NULL;
    R_CHECK(rig, moq_rcbuf_create(&rig->alloc->vt, bytes, sizeof(bytes), &pl) == MOQ_OK);
    R_CHECK(rig, moq_session_write_object(t->pub->peer, sg, object, pl,
                                          rig->now) == MOQ_OK);
    moq_rcbuf_decref(pl);
    R_CHECK(rig, moq_session_close_subgroup(t->pub->peer, sg, rig->now) == MOQ_OK);
}

static int
draft22_update_none_next_late(bool automatic)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) return 1;
    d22_track_t t;
    relay_bind_test_d22_track_setup(&rig, &t, MOQ_VERSION_DRAFT_22, 2);
    conn_t *sub[3];
    peer_state_t ps[3];
    moq_subscription_t h[3];
    memset(ps, 0, sizeof(ps));
    moq_location_filter_cfg_t loc;
    moq_location_filter_cfg_init(&loc);
    loc.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;
    loc.start_group = 8;
    for (int i = 0; i < 3; i++) {
        sub[i] = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_22);
        R_CHECK(&rig, sub[i] != NULL);
    }
    relay_bind_test_rig_pump(&rig, 4);
    for (int i = 0; i < 3; i++) {
        R_CHECK(&rig, relay_bind_test_d22_subscribe(&rig, sub[i], &t, &loc, NULL, &h[i]) == MOQ_OK);
    }
    relay_bind_test_rig_pump(&rig, 8);
    for (int i = 0; i < 3; i++) {
        relay_bind_test_peer_drain(&rig, sub[i], &ps[i], false, false, 0, 0);
        R_CHECK(&rig, ps[i].subscribe_ok && ps[i].objects == 0);
        if (automatic) {
            R_CHECK(&rig, moq_session_set_defer_subscription_updates(sub[i]->rsess,
                                                                      false) == MOQ_OK);
        }
    }
    relay_bind_test_pub_subgroup_n(&rig, t.pub, t.pub_ps.up_sub, 2, 0, 2, false);
    relay_bind_test_rig_pump(&rig, 8);
    for (int i = 0; i < 3; i++) {
        moq_subscription_update_cfg_t u;
        moq_subscription_update_cfg_init_sized(&u, sizeof(u));
        loc.kind = i == 0 ? MOQ_LOCATION_FILTER_NONE : MOQ_LOCATION_FILTER_NEXT_OBJECT;
        u.location = i == 2 ? NULL : &loc;
        R_CHECK(&rig, moq_session_update_subscription(sub[i]->peer, h[i], &u,
                                                      rig.now) == MOQ_OK);
    }
    relay_bind_test_rig_pump(&rig, 8);
    for (int i = 0; i < 3; i++) {
        relay_bind_test_peer_drain(&rig, sub[i], &ps[i], false, false, 0, 0);
        R_CHECK(&rig, ps[i].update_oks == 1 && ps[i].objects == 0);
    }
    d22_pub_one(&rig, &t, 1, 1, 3);   /* new arrival below Largest {2,1} */
    relay_bind_test_rig_pump(&rig, 8);
    relay_bind_test_pub_subgroup_n(&rig, t.pub, t.pub_ps.up_sub, 3, 0, 2, false);
    relay_bind_test_rig_pump(&rig, 8);
    for (int i = 0; i < 3; i++) {
        relay_bind_test_peer_drain(&rig, sub[i], &ps[i], false, false, 0, 0);
        int expected = i == 0 ? 3 : (i == 1 ? 2 : 0);
        R_CHECK(&rig, ps[i].objects == expected && ps[i].dlv_n == expected);
        for (int j = 0; j < ps[i].dlv_n && j < expected; j++) {
            bool late = i == 0 && j == 0;
            R_CHECK(&rig, ps[i].dlv[j].group == (late ? 1u : 3u));
            R_CHECK(&rig, ps[i].dlv[j].subgroup == (late ? 1u : 0u));
            R_CHECK(&rig, ps[i].dlv[j].object_id ==
                          (late ? 3u : (uint64_t)(j - (i == 0 ? 1 : 0))));
        }
        R_CHECK(&rig, !ps[i].done_seen && !ps[i].session_closed);
    }
    moqr_bind_stats_t bs;
    moqr_bind_get_stats(rig.bind, &bs);
    R_CHECK(&rig, bs.session_errors == 0);
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (rig.failures == 0) printf("PASS: draft22_update_none_next_late(automatic %d)\n", automatic);
    return rig.failures;
}

/* The native response snapshot is {1,2}. Holding just its answer must not
 * rebase a relative/Next Object filter after group 3 arrives. The delayed-poll
 * variant separately queues the update before the bind ingests group 3. */
static int
draft22_update_relative_snapshot(uint32_t kind, bool delayed_poll, bool automatic)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) return 1;
    d22_track_t t;
    relay_bind_test_d22_track_setup(&rig, &t, MOQ_VERSION_DRAFT_22, 2);
    conn_t *sub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_22);
    R_CHECK(&rig, sub != NULL);
    relay_bind_test_rig_pump(&rig, 4);
    moq_location_filter_cfg_t loc;
    moq_location_filter_cfg_init(&loc);
    loc.kind = MOQ_LOCATION_FILTER_NEXT_OBJECT;
    moq_subscription_t h;
    R_CHECK(&rig, relay_bind_test_d22_subscribe(&rig, sub, &t, &loc, NULL, &h) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 8);
    peer_state_t ps;
    memset(&ps, 0, sizeof(ps));
    relay_bind_test_peer_drain(&rig, sub, &ps, false, false, 0, 0);
    R_CHECK(&rig, ps.subscribe_ok && ps.objects == 0);
    if (automatic) {
        R_CHECK(&rig, moq_session_set_defer_subscription_updates(sub->rsess,
                                                                  false) == MOQ_OK);
    }
    moq_subscription_update_cfg_t u;
    moq_subscription_update_cfg_init_sized(&u, sizeof(u));
    loc.kind = kind;
    loc.relative_group_offset = 0;
    u.location = &loc;
    if (!automatic) moqr_bind_debug_block_update_answers(1000);
    R_CHECK(&rig, moq_session_update_subscription(sub->peer, h, &u, rig.now) == MOQ_OK);
    if (delayed_poll) relay_bind_test_rig_step_pair(&rig, sub);
    else relay_bind_test_rig_pump(&rig, 6);
    d22_pub_one(&rig, &t, 3, 0, 0);
    if (delayed_poll) relay_bind_test_rig_step_pair(&rig, t.pub);
    relay_bind_test_rig_pump(&rig, 8);
    relay_bind_test_peer_drain(&rig, sub, &ps, false, false, 0, 0);
    R_CHECK(&rig, ps.update_oks == (automatic ? 1 : 0));
    moqr_bind_debug_block_update_answers(0);
    relay_bind_test_rig_pump(&rig, 8);
    relay_bind_test_peer_drain(&rig, sub, &ps, false, false, 0, 0);
    R_CHECK(&rig, ps.update_oks == 1 && ps.uok_has_largest);
    R_CHECK(&rig, ps.uok_largest_group == 1 && ps.uok_largest_object == 2);
    d22_pub_one(&rig, &t, 2, 0, 0);
    relay_bind_test_rig_pump(&rig, 8);
    relay_bind_test_peer_drain(&rig, sub, &ps, false, false, 0, 0);
    printf("ORACLE relative_snapshot kind=%u delayed_poll=%d automatic=%d objects=%d identities=%d ack={%llu,%llu}\n",
           (unsigned)kind, delayed_poll, automatic, ps.objects, ps.dlv_n,
           (unsigned long long)ps.uok_largest_group,
           (unsigned long long)ps.uok_largest_object);
    R_CHECK(&rig, ps.objects == 2 && ps.dlv_n == 2);
    if (ps.dlv_n == 2) {
        R_CHECK(&rig, ps.dlv[0].group == 3 && ps.dlv[0].object_id == 0);
        R_CHECK(&rig, ps.dlv[1].group == 2 && ps.dlv[1].object_id == 0);
    }
    R_CHECK(&rig, !ps.session_closed && !ps.done_seen);
    moqr_bind_stats_t bs;
    moqr_bind_get_stats(rig.bind, &bs);
    R_CHECK(&rig, bs.session_errors == 0);
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (rig.failures == 0) printf("PASS: draft22_update_relative_snapshot(kind %u, delayed_poll %d, automatic %d)\n",
                                  (unsigned)kind, delayed_poll, automatic);
    return rig.failures;
}

/* The relay answers an update with its own Largest Object: REQUEST_UPDATE_OK
 * carries it (9.20.17), and a fill carried on the update is bounded by it
 * (3.4: "the fill range never extends beyond Largest Object"). The
 * subscription starts at group 3, so nothing of group 2 is written to the
 * subscriber's session while the relay ingests it: the binding must note the
 * relay's Largest {2,3} (group 2's END_OF_GROUP) before UPDATE processing,
 * without relying on data delivery. The update asks for a fill over
 * {0,0}..Largest. Holding the answer while later objects arrive must not
 * change that snapshot. */
static int
draft22_update_largest_is_relay(bool held, bool streaming)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: draft22_update_largest_is_relay rig create\n");
        return 1;
    }
    d22_track_t t;
    relay_bind_test_server_streaming_objects = streaming;
    relay_bind_test_d22_track_setup(&rig, &t, MOQ_VERSION_DRAFT_22, 2); /* largest {1,2} */
    relay_bind_test_server_streaming_objects = false;
    conn_t *sub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_22);
    R_CHECK(&rig, sub != NULL);
    relay_bind_test_rig_pump(&rig, 4);

    moq_location_filter_cfg_t loc;
    moq_location_filter_cfg_init(&loc);
    loc.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;
    loc.start_group = held ? 5 : 3;
    loc.start_object = 0;
    moq_subscription_t h;
    R_CHECK(&rig, relay_bind_test_d22_subscribe(&rig, sub, &t, &loc, NULL, &h) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 8);
    peer_state_t s_ps;
    memset(&s_ps, 0, sizeof(s_ps));
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.subscribe_ok && s_ps.has_largest &&
                      s_ps.largest_group == 1 && s_ps.largest_object == 2);

    relay_bind_test_pub_subgroup_n(&rig, t.pub, t.pub_ps.up_sub, 2, 0, 3, true);
    relay_bind_test_rig_pump(&rig, 8);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.objects == 0);   /* before the subscription's start */

    moq_location_filter_cfg_t floc;
    moq_location_filter_cfg_init(&floc);
    floc.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;   /* {0,0} .. Largest */
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.location = &floc;
    moq_subscription_update_cfg_t ucfg;
    moq_subscription_update_cfg_init_sized(&ucfg, sizeof(ucfg));
    ucfg.fill = &fill;
    if (held) {
        moqr_bind_debug_block_update_answers(1000);
    }
    R_CHECK(&rig, moq_session_update_subscription(sub->peer, h, &ucfg,
                                                  rig.now) == MOQ_OK);
    if (held) {
        relay_bind_test_rig_pump(&rig, 8);
        relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
        R_CHECK(&rig, s_ps.update_oks == 0 && s_ps.fill_objects == 0);
        /* Later ingest cannot move the UPDATE's message-processing snapshot. */
        relay_bind_test_pub_subgroup_n(&rig, t.pub, t.pub_ps.up_sub, 3, 0, 3, true);
        relay_bind_test_rig_pump(&rig, 8);
        relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
        R_CHECK(&rig, s_ps.update_oks == 0 && s_ps.objects == 0);
        moqr_bind_debug_block_update_answers(0);
    }
    d22_fill_origin_range_t origin[] = {
        { .start_group = 0, .start_object = 3,
          .end_group = 0, .end_object = 0, .answer_unknown = true },
        { .start_group = 1, .start_object = 3,
          .end_group = 1, .end_object = 0, .answer_unknown = true }
    };
    d22_fill_origin_pump(&rig, &t, origin, 2, 16);
    R_CHECK(&rig, origin[0].requests == 1 && origin[1].requests == 1);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.update_oks == 1);
    R_CHECK(&rig, s_ps.uok_has_largest && s_ps.uok_largest_group == 2 &&
                      s_ps.uok_largest_object == 3);
    /* The fill runs to the relay's Largest: groups 0..2, the tails of the
     * two groups without END_OF_GROUP evidence marked Unknown, nothing past
     * group 2's end. */
    R_CHECK(&rig, s_ps.fill_objects == 9);
    R_CHECK(&rig, s_ps.fill_min_group == 0 && s_ps.fill_max_group == 2);
    const d22_gap_t want[2] = {
        { MOQ_FETCH_RANGE_UNKNOWN, 0, UINT64_MAX },
        { MOQ_FETCH_RANGE_UNKNOWN, 1, UINT64_MAX },
    };
    R_CHECK(&rig, d22_fill_gaps_are(&s_ps, want, 2));
    R_CHECK(&rig, s_ps.fill_terminals == 1 &&
                      s_ps.fill_term_reason == MOQ_FILL_COMPLETE);
    R_CHECK(&rig, s_ps.objects == 0);
    R_CHECK(&rig, !s_ps.done_seen && !s_ps.session_closed);
    moqr_bind_stats_t bs;
    moqr_bind_get_stats(rig.bind, &bs);
    R_CHECK(&rig, bs.session_errors == 0);

    int f = rig.failures;
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (f == 0 && rig.failures == 0) {
        printf("PASS: draft22_update_largest_is_relay(held %d, streaming %d)\n",
               held, streaming);
    }
    return f + rig.failures;
}

/* A fill that overlaps live delivery of the same group. The subscription is
 * paused (Forward 0) while the relay ingests group 2; the update resumes it
 * and asks for a fill over {0,0}..Largest. Group 2 then reaches the
 * subscriber twice: on the subscription's subgroup stream and on the fill.
 * The subscriber drains only between pump batches, so its session refuses
 * deliveries with WOULD_BLOCK while both streams are in flight. Backpressure
 * stalls them and the drains resume them: every object of both paths
 * surfaces and the fill ends with its terminal (§3.4, §11.3.2). */
static int
draft22_update_fill_overlaps_live(void)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: draft22_update_fill_overlaps_live rig create\n");
        return 1;
    }
    d22_track_t t;
    relay_bind_test_d22_track_setup(&rig, &t, MOQ_VERSION_DRAFT_22, 2); /* largest {1,2} */
    conn_t *sub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_22);
    R_CHECK(&rig, sub != NULL);
    relay_bind_test_rig_pump(&rig, 4);

    moq_location_filter_cfg_t loc;
    moq_location_filter_cfg_init(&loc);
    loc.kind = MOQ_LOCATION_FILTER_NEXT_OBJECT;
    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = t.nsp, .count = 2 };
    scfg.track_name = B("v");
    scfg.location = &loc;
    scfg.has_forward = true;
    scfg.forward = false;
    moq_subscription_t h;
    R_CHECK(&rig, moq_session_subscribe(sub->peer, &scfg, rig.now, &h) ==
                      MOQ_OK);
    relay_bind_test_rig_pump(&rig, 8);
    peer_state_t s_ps;
    memset(&s_ps, 0, sizeof(s_ps));
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.subscribe_ok && s_ps.has_largest &&
                      s_ps.largest_group == 1 && s_ps.largest_object == 2);

    relay_bind_test_pub_subgroup_n(&rig, t.pub, t.pub_ps.up_sub, 2, 0, 3, true);
    relay_bind_test_rig_pump(&rig, 8);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.objects == 0);   /* paused */

    moq_location_filter_cfg_t floc;
    moq_location_filter_cfg_init(&floc);
    floc.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;   /* {0,0} .. Largest */
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.location = &floc;
    moq_subscription_update_cfg_t ucfg;
    moq_subscription_update_cfg_init_sized(&ucfg, sizeof(ucfg));
    ucfg.has_forward = true;
    ucfg.forward = true;
    ucfg.fill = &fill;
    R_CHECK(&rig, moq_session_update_subscription(sub->peer, h, &ucfg,
                                                  rig.now) == MOQ_OK);
    d22_fill_origin_range_t origin[] = {
        { .start_group = 0, .start_object = 3,
          .end_group = 0, .end_object = 0, .answer_unknown = true },
        { .start_group = 1, .start_object = 3,
          .end_group = 1, .end_object = 0, .answer_unknown = true }
    };
    d22_fill_origin_pump(&rig, &t, origin, 2, 16);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.update_oks == 1);
    R_CHECK(&rig, s_ps.uok_has_largest && s_ps.uok_largest_group == 2 &&
                      s_ps.uok_largest_object == 3);
    /* The first batch stalled on the subscriber's full queue. */
    int first_pass = s_ps.fill_objects + s_ps.objects;
    R_CHECK(&rig, first_pass < 9 + 4);
    for (int k = 0; k < 4; k++) {
        d22_fill_origin_pump(&rig, &t, origin, 2, 16);
        relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    }
    R_CHECK(&rig, origin[0].requests == 1 && origin[1].requests == 1);
    /* Both paths complete: the fill's 9 objects with the Unknown tails of
     * groups 0 and 1 and its terminal, and group 2 live (three objects and
     * its END_OF_GROUP). */
    R_CHECK(&rig, s_ps.fill_objects == 9);
    R_CHECK(&rig, s_ps.fill_min_group == 0 && s_ps.fill_max_group == 2);
    const d22_gap_t want[2] = {
        { MOQ_FETCH_RANGE_UNKNOWN, 0, UINT64_MAX },
        { MOQ_FETCH_RANGE_UNKNOWN, 1, UINT64_MAX },
    };
    R_CHECK(&rig, d22_fill_gaps_are(&s_ps, want, 2));
    R_CHECK(&rig, s_ps.fill_terminals == 1 &&
                      s_ps.fill_term_reason == MOQ_FILL_COMPLETE);
    R_CHECK(&rig, s_ps.objects == 4);
    R_CHECK(&rig, !s_ps.done_seen && !s_ps.session_closed);
    moqr_bind_stats_t bs;
    moqr_bind_get_stats(rig.bind, &bs);
    R_CHECK(&rig, bs.session_errors == 0);

    int f = rig.failures;
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (f == 0 && rig.failures == 0) {
        printf("PASS: draft22_update_fill_overlaps_live\n");
    }
    return f + rig.failures;
}

/* What the relay's session wrote on its request streams to one draft-22
 * subscriber, message by message: the type and, for REQUEST_ERROR and
 * PUBLISH_DONE, the code. Installed as the SimPair trace hook of that
 * subscriber's connection only. */
typedef struct {
    uint64_t type;
    uint64_t code;
    bool     fin;
} rwire_msg_t;

typedef struct {
    rwire_msg_t m[64];
    size_t      n;
} rwire_log_t;

#define RWIRE_REQUEST_OK    0x07u
#define RWIRE_REQUEST_ERROR 0x05u
#define RWIRE_PUBLISH_DONE  0x0Bu

static rwire_log_t g_rwire;

/* One draft-22 vi64 at `p`: the count of leading 1-bits in the first byte
 * plus one is the length (all nine for 0xFF); the value is the first byte's
 * remaining bits followed by the next bytes, big-endian. Returns the length,
 * or 0 when it runs past `len`. */
static size_t
rwire_vi64(const uint8_t *p, size_t len, uint64_t *out)
{
    if (len == 0) {
        return 0;
    }
    size_t n = 1;
    while (n < 9 && (p[0] & (0x80u >> (n - 1))) != 0) {
        n++;
    }
    if (n > len) {
        return 0;
    }
    uint64_t v = n == 9 ? 0 : (uint64_t)(p[0] & (0xFFu >> n));
    for (size_t i = 1; i < n; i++) {
        v = (v << 8) | p[i];
    }
    *out = v;
    return n;
}

static void
rwire_trace(void *ctx, const moq_sim_trace_record_t *r)
{
    rwire_log_t *l = (rwire_log_t *)ctx;
    if (r->kind != MOQ_SIM_TRACE_ACTION || r->from != MOQ_PERSPECTIVE_SERVER ||
        r->action_kind != MOQ_ACTION_SEND_BIDI_STREAM) {
        return;
    }
    const uint8_t *p = r->detail_bytes.data;
    size_t left = r->detail_bytes.len;
    while (left > 0 && l->n < 64) {
        uint64_t type = 0;
        size_t tl = rwire_vi64(p, left, &type);
        if (tl == 0 || left - tl < 2) {
            return;
        }
        size_t plen = ((size_t)p[tl] << 8) | p[tl + 1];
        size_t hdr = tl + 2;
        if (plen > left - hdr) {
            return;
        }
        rwire_msg_t *m = &l->m[l->n++];
        memset(m, 0, sizeof(*m));
        m->type = type;
        if (type == RWIRE_REQUEST_ERROR || type == RWIRE_PUBLISH_DONE) {
            (void)rwire_vi64(p + hdr, plen, &m->code);
        }
        p += hdr + plen;
        left -= hdr + plen;
        m->fin = r->fin && left == 0;
    }
}

static size_t
rwire_count(uint64_t type)
{
    size_t n = 0;
    for (size_t i = 0; i < g_rwire.n; i++) {
        if (g_rwire.m[i].type == type) {
            n++;
        }
    }
    return n;
}

/* Index of the first message of `type`, or 64 when there is none. */
static size_t
rwire_first(uint64_t type)
{
    for (size_t i = 0; i < g_rwire.n; i++) {
        if (g_rwire.m[i].type == type) {
            return i;
        }
    }
    return 64;
}

static void
rwire_reset(void)
{
    memset(&g_rwire, 0, sizeof(g_rwire));
}

/* A draft-22 subscriber connection whose relay-side writes land in g_rwire. */
static conn_t *
rwire_connect(rig_t *rig)
{
    rwire_reset();
    relay_bind_test_trace_fn = rwire_trace;
    relay_bind_test_trace_ctx = &g_rwire;
    conn_t *cn = relay_bind_test_rig_connect(rig, MOQ_VERSION_DRAFT_22);
    relay_bind_test_trace_fn = NULL;
    relay_bind_test_trace_ctx = NULL;
    return cn;
}

/* A real inclusive object-end update on an open subgroup is accepted. A
 * separately injected invalid kind is refused as the update's answer:
 * REQUEST_UPDATE_ERROR INTERNAL_ERROR -- the code the same form gets at
 * subscribe time -- then PUBLISH_DONE
 * UPDATE_FAILED with FIN on the request stream, and never an
 * acknowledgement. The subscriber ends the subscription with that status;
 * the open data stream is reset with it and its relay slot released,
 * nothing further is delivered and the session stays up. */
static int
draft22_update_object_end_or_invalid(bool typed)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: draft22_update_object_end_or_invalid rig create\n");
        return 1;
    }
    d22_track_t t;
    relay_bind_test_d22_track_setup(&rig, &t, MOQ_VERSION_DRAFT_22, 1);
    conn_t *sub = rwire_connect(&rig);
    R_CHECK(&rig, sub != NULL);
    relay_bind_test_rig_pump(&rig, 4);

    moq_location_filter_cfg_t loc;
    moq_location_filter_cfg_init(&loc);
    loc.kind = MOQ_LOCATION_FILTER_NEXT_OBJECT;
    moq_subscription_t h;
    R_CHECK(&rig, relay_bind_test_d22_subscribe(&rig, sub, &t, &loc, NULL, &h) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 8);
    peer_state_t s_ps;
    memset(&s_ps, 0, sizeof(s_ps));
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.subscribe_ok && s_ps.objects == 0);

    /* A downstream data stream is open when the update arrives. */
    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = 1;
    moq_subgroup_handle_t sgh;
    R_CHECK(&rig, moq_session_open_subgroup(t.pub->peer, t.pub_ps.up_sub, &sgc,
                                            rig.now, &sgh) == MOQ_OK);
    for (uint64_t o = 0; o < 2; o++) {
        uint8_t body[4] = { 1, (uint8_t)o, 0xC3, 0x3C };
        moq_rcbuf_t *pl = NULL;
        R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) == MOQ_OK);
        R_CHECK(&rig, moq_session_write_object(t.pub->peer, sgh, o, pl,
                                               rig.now) == MOQ_OK);
        moq_rcbuf_decref(pl);
    }
    relay_bind_test_rig_pump(&rig, 8);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.objects == 2);
    R_CHECK(&rig, moqr_bind_debug_conn_open_sgs(rig.bind, 2) == 1);

    rwire_reset();
    moq_subscription_update_cfg_t ucfg;
    moq_subscription_update_cfg_init_sized(&ucfg, sizeof(ucfg));
    if (typed) {
        loc.kind = MOQ_LOCATION_FILTER_OBJECT_END;
        loc.start_group = 1;
        loc.end_group = 1;
        loc.end_object = 2;
        ucfg.location = &loc;
    } else {
        moqr_bind_debug_update_filter_kind(UINT32_MAX);
        ucfg.has_filter = true;
        ucfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
        ucfg.start_group = 1;
        ucfg.start_object = 0;
    }
    R_CHECK(&rig, moq_session_update_subscription(sub->peer, h, &ucfg,
                                                  rig.now) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 8);
    if (typed) {
        R_CHECK(&rig, rwire_count(RWIRE_REQUEST_OK) == 1);
        R_CHECK(&rig, rwire_count(RWIRE_REQUEST_ERROR) == 0);
        R_CHECK(&rig, rwire_count(RWIRE_PUBLISH_DONE) == 0);
        for (uint64_t o = 2; o < 4; o++) {
            uint8_t body[4] = { 1, (uint8_t)o, 0xC3, 0x3C };
            moq_rcbuf_t *pl = NULL;
            R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) == MOQ_OK);
            R_CHECK(&rig, moq_session_write_object(t.pub->peer, sgh, o, pl,
                                                   rig.now) == MOQ_OK);
            moq_rcbuf_decref(pl);
        }
        relay_bind_test_rig_pump(&rig, 8);
        relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
        R_CHECK(&rig, s_ps.update_oks == 1 && !s_ps.done_seen);
        R_CHECK(&rig, s_ps.objects == 3 && s_ps.dlv_n == 3);
        for (int i = 0; i < s_ps.dlv_n && i < 3; i++) {
            R_CHECK(&rig, s_ps.dlv[i].group == 1 && s_ps.dlv[i].subgroup == 0);
            R_CHECK(&rig, s_ps.dlv[i].object_id == (uint64_t)i);
        }
        R_CHECK(&rig, moqr_bind_debug_conn_open_sgs(rig.bind, 2) == 1);
    } else {
        R_CHECK(&rig, rwire_count(RWIRE_REQUEST_OK) == 0);
        R_CHECK(&rig, rwire_count(RWIRE_REQUEST_ERROR) == 1);
        R_CHECK(&rig, rwire_count(RWIRE_PUBLISH_DONE) == 1);
        size_t ie = rwire_first(RWIRE_REQUEST_ERROR);
        size_t id = rwire_first(RWIRE_PUBLISH_DONE);
        R_CHECK(&rig, ie < id && id < 64);
        if (id < 64) {
            R_CHECK(&rig, g_rwire.m[ie].code == 0x0u);   /* INTERNAL_ERROR */
            R_CHECK(&rig, g_rwire.m[id].code == 0x8u);   /* UPDATE_FAILED */
            R_CHECK(&rig, g_rwire.m[id].fin);
        }
        relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
        R_CHECK(&rig, s_ps.update_oks == 0);
        R_CHECK(&rig, s_ps.done_seen && s_ps.done_count == 1);
        R_CHECK(&rig, s_ps.done_code == 0x8u);   /* UPDATE_FAILED */
        R_CHECK(&rig, moqr_bind_debug_conn_open_sgs(rig.bind, 2) == 0);
    }
    R_CHECK(&rig, !s_ps.session_closed);

    /* Neither the finite window nor the refused subscription admits group 2. */
    R_CHECK(&rig, moq_session_close_subgroup(t.pub->peer, sgh, rig.now) ==
                      MOQ_OK);
    relay_bind_test_pub_subgroup_n(&rig, t.pub, t.pub_ps.up_sub, 2, 0, 3, false);
    relay_bind_test_rig_pump(&rig, 8);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.objects == (typed ? 3 : 2) && !s_ps.session_closed);
    R_CHECK(&rig, moqr_bind_debug_conn_open_sgs(rig.bind, 2) == 0);
    moqr_bind_stats_t bs;
    moqr_bind_get_stats(rig.bind, &bs);
    R_CHECK(&rig, bs.session_errors == 0);

    int f = rig.failures;
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (f == 0 && rig.failures == 0) {
        printf("PASS: draft22_update_object_end_or_invalid(typed %d)\n", typed);
    }
    return f + rig.failures;
}

/* The relay applies a replacement filter before the update is acknowledged:
 * with the acceptance held (the session cannot take it yet), the relay
 * already forwards under the new window while the subscriber has no
 * REQUEST_UPDATE_OK. Another
 * subscriber on the track is served throughout. The held acceptance is
 * retried every pump and goes out once the session takes it. */
static int
draft22_update_answer_held_accept(void)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: draft22_update_answer_held_accept rig create\n");
        return 1;
    }
    d22_track_t t;
    relay_bind_test_d22_track_setup(&rig, &t, MOQ_VERSION_DRAFT_22, 2); /* largest {1,2} */
    conn_t *sub = rwire_connect(&rig);
    conn_t *other = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_22);
    R_CHECK(&rig, sub != NULL && other != NULL);
    relay_bind_test_rig_pump(&rig, 4);

    moq_location_filter_cfg_t loc;
    moq_location_filter_cfg_init(&loc);
    loc.kind = MOQ_LOCATION_FILTER_NEXT_OBJECT;
    moq_subscription_t h, h2;
    R_CHECK(&rig, relay_bind_test_d22_subscribe(&rig, sub, &t, &loc, NULL, &h) == MOQ_OK);
    R_CHECK(&rig, relay_bind_test_d22_subscribe(&rig, other, &t, &loc, NULL, &h2) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 8);
    peer_state_t s_ps, o_ps;
    memset(&s_ps, 0, sizeof(s_ps));
    memset(&o_ps, 0, sizeof(o_ps));
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    relay_bind_test_peer_drain(&rig, other, &o_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.subscribe_ok && o_ps.subscribe_ok);

    rwire_reset();
    moqr_bind_debug_block_update_answers(1000);
    moq_subscription_update_cfg_t ucfg;
    moq_subscription_update_cfg_init_sized(&ucfg, sizeof(ucfg));
    ucfg.has_filter = true;
    ucfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    ucfg.start_group = 3;
    ucfg.start_object = 0;
    R_CHECK(&rig, moq_session_update_subscription(sub->peer, h, &ucfg,
                                                  rig.now) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 6);
    /* Held: no acknowledgement. */
    R_CHECK(&rig, rwire_count(RWIRE_REQUEST_OK) == 0);
    /* ...but the relay already forwards under the new window: group 2 is
     * outside it, group 3 is served. The other subscriber gets both. */
    relay_bind_test_pub_subgroup_n(&rig, t.pub, t.pub_ps.up_sub, 2, 0, 3, false);
    relay_bind_test_rig_pump(&rig, 8);
    relay_bind_test_pub_subgroup_n(&rig, t.pub, t.pub_ps.up_sub, 3, 0, 3, false);
    relay_bind_test_rig_pump(&rig, 8);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    relay_bind_test_peer_drain(&rig, other, &o_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.objects == 3 && dlv_min_group(&s_ps) == 3);
    R_CHECK(&rig, s_ps.update_oks == 0 && !s_ps.done_seen);
    R_CHECK(&rig, o_ps.objects == 6);
    R_CHECK(&rig, rwire_count(RWIRE_REQUEST_OK) == 0);

    /* The session takes it: one acknowledgement, the filter latched. */
    moqr_bind_debug_block_update_answers(0);
    relay_bind_test_rig_pump(&rig, 6);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, rwire_count(RWIRE_REQUEST_OK) == 1);
    R_CHECK(&rig, rwire_count(RWIRE_REQUEST_ERROR) == 0);
    R_CHECK(&rig, s_ps.update_oks == 1);
    R_CHECK(&rig, !s_ps.done_seen && !s_ps.session_closed);
    moqr_bind_stats_t bs;
    moqr_bind_get_stats(rig.bind, &bs);
    R_CHECK(&rig, bs.session_errors == 0);

    int f = rig.failures;
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (f == 0 && rig.failures == 0) {
        printf("PASS: draft22_update_answer_held_accept\n");
    }
    return f + rig.failures;
}

/* A replacement relative Location Filter further back than the current
 * group (StartGroup=2) is a form the relay serves: the update is
 * acknowledged, the subscription stays established, and -- the delivery
 * frontier never moving back -- it keeps delivering the live edge without
 * re-sending what it passed. */
static int
draft22_update_filter_relative_deep(bool typed)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: draft22_update_filter_relative_deep rig create\n");
        return 1;
    }
    d22_track_t t;
    relay_bind_test_d22_track_setup(&rig, &t, MOQ_VERSION_DRAFT_22, 2); /* largest {1,2} */
    conn_t *sub = rwire_connect(&rig);
    R_CHECK(&rig, sub != NULL);
    relay_bind_test_rig_pump(&rig, 4);
    moq_location_filter_cfg_t loc;
    moq_location_filter_cfg_init(&loc);
    loc.kind = MOQ_LOCATION_FILTER_NEXT_OBJECT;
    moq_subscription_t h;
    R_CHECK(&rig, relay_bind_test_d22_subscribe(&rig, sub, &t, &loc, NULL, &h) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 8);
    peer_state_t s_ps;
    memset(&s_ps, 0, sizeof(s_ps));
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.subscribe_ok && s_ps.objects == 0);

    rwire_reset();
    moq_subscription_update_cfg_t ucfg;
    moq_subscription_update_cfg_init_sized(&ucfg, sizeof(ucfg));
    if (typed) {
        loc.kind = MOQ_LOCATION_FILTER_RELATIVE_GROUP;
        loc.relative_group_offset = 2;
        ucfg.location = &loc;
    } else {
        moqr_bind_debug_update_filter_kind(MOQ_LOCATION_FILTER_RELATIVE_GROUP);
        ucfg.has_filter = true;
        ucfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
        ucfg.start_group = 1;
        ucfg.start_object = 0;
    }
    R_CHECK(&rig, moq_session_update_subscription(sub->peer, h, &ucfg,
                                                  rig.now) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 8);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, rwire_count(RWIRE_REQUEST_OK) == 1);
    R_CHECK(&rig, rwire_count(RWIRE_REQUEST_ERROR) == 0);
    R_CHECK(&rig, rwire_count(RWIRE_PUBLISH_DONE) == 0);
    R_CHECK(&rig, s_ps.update_oks == 1 && !s_ps.done_seen);
    R_CHECK(&rig, s_ps.objects == 0);   /* nothing passed is re-sent */
    relay_bind_test_pub_subgroup_n(&rig, t.pub, t.pub_ps.up_sub, 2, 0, 3, false);
    relay_bind_test_rig_pump(&rig, 8);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.objects == 3 && dlv_min_group(&s_ps) == 2);
    R_CHECK(&rig, !s_ps.done_seen && !s_ps.session_closed);
    moqr_bind_stats_t bs;
    moqr_bind_get_stats(rig.bind, &bs);
    R_CHECK(&rig, bs.session_errors == 0);

    int f = rig.failures;
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (f == 0 && rig.failures == 0) {
        printf("PASS: draft22_update_filter_relative_deep(typed %d)\n", typed);
    }
    return f + rig.failures;
}

/* A refusal the session cannot take yet is held and retried, never dropped:
 * nothing is answered while it is held, and once the session takes it the
 * subscriber sees REQUEST_UPDATE_ERROR INTERNAL_ERROR then PUBLISH_DONE
 * UPDATE_FAILED. */
static int
draft22_update_answer_held_refusal(void)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: draft22_update_answer_held_refusal rig create\n");
        return 1;
    }
    d22_track_t t;
    relay_bind_test_d22_track_setup(&rig, &t, MOQ_VERSION_DRAFT_22, 1);
    conn_t *sub = rwire_connect(&rig);
    R_CHECK(&rig, sub != NULL);
    relay_bind_test_rig_pump(&rig, 4);
    moq_location_filter_cfg_t loc;
    moq_location_filter_cfg_init(&loc);
    loc.kind = MOQ_LOCATION_FILTER_NEXT_OBJECT;
    moq_subscription_t h;
    R_CHECK(&rig, relay_bind_test_d22_subscribe(&rig, sub, &t, &loc, NULL, &h) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 8);
    peer_state_t s_ps;
    memset(&s_ps, 0, sizeof(s_ps));
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.subscribe_ok);

    rwire_reset();
    moqr_bind_debug_block_update_answers(1000);
    moqr_bind_debug_update_filter_kind(UINT32_MAX);
    moq_subscription_update_cfg_t ucfg;
    moq_subscription_update_cfg_init_sized(&ucfg, sizeof(ucfg));
    ucfg.has_filter = true;
    ucfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    ucfg.start_group = 1;
    ucfg.start_object = 0;
    R_CHECK(&rig, moq_session_update_subscription(sub->peer, h, &ucfg,
                                                  rig.now) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 8);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, g_rwire.n == 0);
    R_CHECK(&rig, !s_ps.done_seen && s_ps.update_oks == 0);

    moqr_bind_debug_block_update_answers(0);
    relay_bind_test_rig_pump(&rig, 6);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, rwire_count(RWIRE_REQUEST_OK) == 0);
    size_t ie = rwire_first(RWIRE_REQUEST_ERROR);
    size_t id = rwire_first(RWIRE_PUBLISH_DONE);
    R_CHECK(&rig, ie < id && id < 64);
    if (id < 64) {
        R_CHECK(&rig, g_rwire.m[ie].code == 0x0u);   /* INTERNAL_ERROR */
        R_CHECK(&rig, g_rwire.m[id].code == 0x8u);
    }
    R_CHECK(&rig, s_ps.done_seen && s_ps.done_count == 1 &&
                      s_ps.done_code == 0x8u);
    relay_bind_test_pub_subgroup_n(&rig, t.pub, t.pub_ps.up_sub, 1, 0, 3, false);
    relay_bind_test_rig_pump(&rig, 8);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.objects == 0 && !s_ps.session_closed);
    moqr_bind_stats_t bs;
    moqr_bind_get_stats(rig.bind, &bs);
    R_CHECK(&rig, bs.session_errors == 0);

    int f = rig.failures;
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (f == 0 && rig.failures == 0) {
        printf("PASS: draft22_update_answer_held_refusal\n");
    }
    return f + rig.failures;
}

/* The relay ends a downstream subscription (its source finished) while an
 * update answer is held: the answer goes out first, then the terminal. A
 * held acceptance is followed by the source's PUBLISH_DONE; a held refusal
 * is itself the terminal (UPDATE_FAILED) and no second PUBLISH_DONE
 * follows. */
static int
draft22_update_answer_before_sub_done(bool refuse)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: draft22_update_answer_before_sub_done rig create\n");
        return 1;
    }
    d22_track_t t;
    relay_bind_test_d22_track_setup(&rig, &t, MOQ_VERSION_DRAFT_22, 1);
    conn_t *sub = rwire_connect(&rig);
    R_CHECK(&rig, sub != NULL);
    relay_bind_test_rig_pump(&rig, 4);
    moq_location_filter_cfg_t loc;
    moq_location_filter_cfg_init(&loc);
    loc.kind = MOQ_LOCATION_FILTER_NEXT_OBJECT;
    moq_subscription_t h;
    R_CHECK(&rig, relay_bind_test_d22_subscribe(&rig, sub, &t, &loc, NULL, &h) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 8);
    peer_state_t s_ps;
    memset(&s_ps, 0, sizeof(s_ps));
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.subscribe_ok);

    rwire_reset();
    moqr_bind_debug_block_update_answers(1000);
    moq_subscription_update_cfg_t ucfg;
    moq_subscription_update_cfg_init_sized(&ucfg, sizeof(ucfg));
    if (refuse) {
        moqr_bind_debug_update_filter_kind(UINT32_MAX);
        ucfg.has_filter = true;
        ucfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
        ucfg.start_group = 1;
    } else {
        ucfg.has_subscriber_priority = true;
        ucfg.subscriber_priority = 9;
    }
    R_CHECK(&rig, moq_session_update_subscription(sub->peer, h, &ucfg,
                                                  rig.now) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 6);
    R_CHECK(&rig, g_rwire.n == 0);

    /* The origin finishes the track: the relay ends its downstream
     * subscription, which waits behind the held answer. */
    moq_done_subscribe_cfg_t dcfg;
    moq_done_subscribe_cfg_init(&dcfg);
    dcfg.status_code = 0x2;
    R_CHECK(&rig, moq_session_done_subscribe(t.pub->peer, t.pub_ps.up_sub,
                                             &dcfg, rig.now) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 6);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, g_rwire.n == 0);
    R_CHECK(&rig, !s_ps.done_seen);

    moqr_bind_debug_block_update_answers(0);
    relay_bind_test_rig_pump(&rig, 8);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, rwire_count(RWIRE_PUBLISH_DONE) == 1);
    R_CHECK(&rig, s_ps.done_seen && s_ps.done_count == 1);
    size_t id = rwire_first(RWIRE_PUBLISH_DONE);
    if (refuse) {
        size_t ie = rwire_first(RWIRE_REQUEST_ERROR);
        R_CHECK(&rig, rwire_count(RWIRE_REQUEST_OK) == 0);
        R_CHECK(&rig, ie < id && id < 64);
        R_CHECK(&rig, s_ps.done_code == 0x8u && s_ps.update_oks == 0);
    } else {
        size_t io = rwire_first(RWIRE_REQUEST_OK);
        R_CHECK(&rig, rwire_count(RWIRE_REQUEST_ERROR) == 0);
        R_CHECK(&rig, io < id && id < 64);
        R_CHECK(&rig, s_ps.done_code != 0x8u && s_ps.update_oks == 1);
    }
    R_CHECK(&rig, !s_ps.session_closed);
    moqr_bind_stats_t bs;
    moqr_bind_get_stats(rig.bind, &bs);
    R_CHECK(&rig, bs.session_errors == 0);

    int f = rig.failures;
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (f == 0 && rig.failures == 0) {
        printf("PASS: draft22_update_answer_before_sub_done(%s)\n",
               refuse ? "refusal" : "acceptance");
    }
    return f + rig.failures;
}

/* A draft-16/18 SUBSCRIBE_UPDATE carrying a new window is acknowledged at
 * once and leaves relay forwarding as it was: the subscription keeps
 * delivering under the filter it was accepted with. */
static int
draft_update_filter_legacy_unchanged(moq_version_t version)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: draft_update_filter_legacy_unchanged rig create\n");
        return 1;
    }
    d22_track_t t;
    relay_bind_test_d22_track_setup(&rig, &t, version, 1); /* group 0 retained */
    conn_t *sub = relay_bind_test_rig_connect(&rig, version);
    R_CHECK(&rig, sub != NULL);
    relay_bind_test_rig_pump(&rig, 4);

    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = t.nsp, .count = 2 };
    scfg.track_name = B("v");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    moq_subscription_t h;
    R_CHECK(&rig, moq_session_subscribe(sub->peer, &scfg, rig.now, &h) ==
                      MOQ_OK);
    relay_bind_test_rig_pump(&rig, 8);
    peer_state_t s_ps;
    memset(&s_ps, 0, sizeof(s_ps));
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.subscribe_ok && s_ps.objects == 3);

    moq_subscription_update_cfg_t ucfg;
    moq_subscription_update_cfg_init_sized(&ucfg, sizeof(ucfg));
    ucfg.has_filter = true;
    ucfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    ucfg.start_group = 5;
    ucfg.start_object = 0;
    R_CHECK(&rig, moq_session_update_subscription(sub->peer, h, &ucfg,
                                                  rig.now) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 6);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.update_oks == 1);   /* accepted at once */
    relay_bind_test_pub_subgroup_n(&rig, t.pub, t.pub_ps.up_sub, 1, 0, 3, false);
    relay_bind_test_rig_pump(&rig, 8);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.objects == 6);   /* group 1 still forwarded */
    R_CHECK(&rig, !s_ps.done_seen && !s_ps.session_closed);
    moqr_bind_stats_t bs;
    moqr_bind_get_stats(rig.bind, &bs);
    R_CHECK(&rig, bs.session_errors == 0);

    int f = rig.failures;
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (f == 0 && rig.failures == 0) {
        printf("PASS: draft_update_filter_legacy_unchanged(draft-%d)\n",
               (int)version);
    }
    return f + rig.failures;
}

/* A fill range starting below the retention horizon is served from the oldest
 * retained group, led by one end-of-range marker covering the evicted prefix
 * -- the same shape a FETCH over that range gets. The origin explicitly
 * reports Unknown for the prefix and the retained group's unresolved tail.
 * FILL_TIMEOUT 0 instead requests neither range and marks both Timed-Out
 * (9.20.5), without spending time on retrieval. */
static int
draft22_fill_evicted_prefix(bool timeout_zero)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    relay_bind_test_log_max_groups = 2;
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        relay_bind_test_log_max_groups = 0;
        printf("FAIL: draft22_fill_evicted_prefix rig create\n");
        return 1;
    }
    d22_track_t t;
    relay_bind_test_d22_track_setup(&rig, &t, MOQ_VERSION_DRAFT_22, 4); /* retained: 2, 3 */
    conn_t *sub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_22);
    R_CHECK(&rig, sub != NULL);
    relay_bind_test_rig_pump(&rig, 4);

    moq_location_filter_cfg_t loc, floc;
    moq_location_filter_cfg_init(&loc);
    loc.kind = MOQ_LOCATION_FILTER_NEXT_OBJECT;
    moq_location_filter_cfg_init(&floc);
    floc.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;   /* {0,0}: evicted */
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.location = &floc;
    fill.has_fill_timeout = timeout_zero;
    fill.fill_timeout_ms = 0;
    moq_subscription_t h;
    R_CHECK(&rig, relay_bind_test_d22_subscribe(&rig, sub, &t, &loc, &fill, &h) == MOQ_OK);
    d22_fill_origin_range_t origin[] = {
        { .start_group = 0, .start_object = 0,
          .end_group = 1, .end_object = 0, .answer_unknown = true },
        { .start_group = 2, .start_object = 3,
          .end_group = 2, .end_object = 0, .answer_unknown = true }
    };
    if (timeout_zero) {
        relay_bind_test_rig_pump(&rig, 12);
    } else {
        d22_fill_origin_pump(&rig, &t, origin, 2, 12);
        R_CHECK(&rig, origin[0].requests == 1 && origin[1].requests == 1);
    }
    peer_state_t s_ps;
    memset(&s_ps, 0, sizeof(s_ps));
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.subscribe_ok);
    /* The evicted prefix through the last evicted group, then group 2's
     * tail (no END_OF_GROUP evidence); group 3 holds Largest. */
    const uint32_t kind = timeout_zero ? MOQ_FETCH_RANGE_TIMED_OUT
                                       : MOQ_FETCH_RANGE_UNKNOWN;
    const d22_gap_t want[2] = {
        { kind, 1, UINT64_MAX },
        { kind, 2, UINT64_MAX },
    };
    R_CHECK(&rig, d22_fill_gaps_are(&s_ps, want, 2));
    R_CHECK(&rig, s_ps.fill_objects == 6);
    R_CHECK(&rig, s_ps.fill_min_group == 2 && s_ps.fill_max_group == 3);
    R_CHECK(&rig, s_ps.fill_terminals == 1 &&
                      s_ps.fill_term_reason == MOQ_FILL_COMPLETE);
    R_CHECK(&rig, !s_ps.done_seen && !s_ps.session_closed);

    relay_bind_test_log_max_groups = 0;
    int f = rig.failures;
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (f == 0 && rig.failures == 0) {
        printf("PASS: draft22_fill_evicted_prefix%s\n",
               timeout_zero ? " (FILL_TIMEOUT 0)" : "");
    }
    return f + rig.failures;
}

typedef enum d22_gap_policy_mode {
    D22_GAP_TERMINAL_NI,
    D22_GAP_FIN_BLOCKED,
    D22_GAP_NONTERMINAL_NI,
    D22_GAP_TERMINAL_UNKNOWN,
    D22_GAP_TERMINAL_TIMED_OUT,
    D22_GAP_CAPPED_CLAIM,
    D22_GAP_COMMIT_ERROR,
    D22_GAP_SOURCE_ERROR,
    D22_GAP_BAD_END,
    D22_GAP_POLICY_MODES
} d22_gap_policy_mode_t;

typedef struct d22_gap_source {
    rig_t *rig;
    d22_gap_policy_mode_t mode;
    moqr_bind_fill_query_t query;
    moqr_bind_fill_claim_t claim;
    moqr_retrieve_item_t item;
    unsigned opens, commits, closes, terminal_peeks, step;
    bool live, pending, allow_end, pending_on_close;
} d22_gap_source_t;

static moqr_result_t
d22_gap_source_open(void *ctx, const moqr_bind_fill_query_t *q,
                     moqr_bind_fill_claim_t *claim)
{
    d22_gap_source_t *s = ctx;
    R_CHECK(s->rig, !s->live && s->opens == 0);
    R_CHECK(s->rig, q->start_group == 0 && q->start_object == 0 &&
                       q->end_group == 0 && q->end_object == 2);
    R_CHECK(s->rig, q->group_order == MOQR_GROUP_ORDER_ASCENDING);
    s->opens++;
    s->live = true;
    s->query = *q;
    s->claim.handle = UINT64_C(4877);
    s->claim.end_group = q->end_group;
    s->claim.end_object = s->mode == D22_GAP_CAPPED_CLAIM ? 0 : q->end_object;
    *claim = s->claim;
    return MOQR_OK;
}

static moqr_result_t
d22_gap_source_public_peek(void *ctx, uint64_t handle, uint64_t now_us,
                            moqr_fetch_item_t *out)
{
    d22_gap_source_t *s = ctx;
    (void)handle;
    (void)now_us;
    (void)out;
    R_CHECK(s->rig, false);   /* these rows must use the real typed writer */
    return MOQR_ERR_INTERNAL;
}

static moqr_result_t
d22_gap_source_peek(void *ctx, uint64_t handle, uint64_t now_us,
                     moqr_retrieve_item_t *out)
{
    d22_gap_source_t *s = ctx;
    (void)now_us;
    R_CHECK(s->rig, s->live && handle == s->claim.handle);
    if (s->pending) {
        *out = s->item;
        return MOQR_OK;
    }
    memset(out, 0, sizeof(*out));
    if (s->step == 0 || (s->mode == D22_GAP_NONTERMINAL_NI && s->step == 1)) {
        out->kind = MOQR_RETRIEVE_GAP;
        out->start.group = s->query.start_group;
        out->start.object = s->step == 0 ? s->query.start_object : 1;
        out->end.group = s->claim.end_group;
        out->end.object = s->mode == D22_GAP_NONTERMINAL_NI && s->step == 0
                              ? 0 : s->claim.end_object;
        out->gap_kind = s->mode == D22_GAP_TERMINAL_UNKNOWN || s->step == 1
                            ? MOQ_FETCH_RANGE_UNKNOWN
                            : s->mode == D22_GAP_TERMINAL_TIMED_OUT
                                  ? MOQ_FETCH_RANGE_TIMED_OUT
                                  : MOQ_FETCH_RANGE_NON_EXISTENT;
        s->item = *out;
        s->pending = true;
        return MOQR_OK;
    }
    if (!s->allow_end) return MOQR_ERR_WOULD_BLOCK;
    s->terminal_peeks++;
    out->kind = s->mode == D22_GAP_SOURCE_ERROR ? MOQR_RETRIEVE_ERROR
                                               : MOQR_RETRIEVE_END;
    out->error_kind = MOQR_RETRIEVE_SOURCE_LOST;
    out->local_result = MOQR_ERR_STALE_HANDLE;
    out->start.group = s->query.start_group;
    out->start.object = s->query.start_object;
    out->end.group = s->claim.end_group;
    out->end.object = s->mode == D22_GAP_BAD_END ? 1 : s->claim.end_object;
    return MOQR_OK;
}

static moqr_result_t
d22_gap_source_commit(void *ctx, uint64_t handle)
{
    d22_gap_source_t *s = ctx;
    R_CHECK(s->rig, s->live && handle == s->claim.handle && s->pending);
    s->commits++;
    if (s->mode == D22_GAP_COMMIT_ERROR) return MOQR_ERR_INTERNAL;
    s->pending = false;
    s->step++;
    return MOQR_OK;
}

static void
d22_gap_source_close(void *ctx, uint64_t handle)
{
    d22_gap_source_t *s = ctx;
    R_CHECK(s->rig, s->live && handle == s->claim.handle);
    s->closes++;
    s->pending_on_close = s->pending;
    s->pending = false;
    s->live = false;
}

/* Real fill admission and wire delivery with controlled typed source items.
 * Holding END after the commit proves that consuming a terminal gap does not
 * bypass END validation or retire the source early. */
static int
draft22_fill_terminal_gap(d22_gap_policy_mode_t mode)
{
    static const char *const names[D22_GAP_POLICY_MODES] = {
        "terminal-ni", "fin-blocked", "nonterminal-ni", "terminal-unknown",
        "terminal-timed-out", "capped-claim", "commit-error", "source-error",
        "bad-end"
    };
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) return 1;
    d22_track_t t;
    relay_bind_test_d22_track_setup(&rig, &t, MOQ_VERSION_DRAFT_22, 1);
    relay_bind_test_max_actions = mode == D22_GAP_FIN_BLOCKED ? 4 : 0;
    conn_t *sub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_22);
    relay_bind_test_max_actions = 0;
    R_CHECK(&rig, sub != NULL);
    if (sub == NULL) {
        relay_bind_test_rig_destroy(&rig);
        return 1;
    }
    relay_bind_test_rig_pump(&rig, 4);
    d22_gap_source_t source;
    memset(&source, 0, sizeof(source));
    source.rig = &rig;
    source.mode = mode;
    moqr_bind_fill_source_t callbacks = {
        .struct_size = sizeof(callbacks), .ctx = &source,
        .open = d22_gap_source_open, .peek = d22_gap_source_public_peek,
        .commit = d22_gap_source_commit, .close = d22_gap_source_close
    };
    R_CHECK(&rig, moqr_bind_set_fill_source(rig.bind, &callbacks) == MOQR_OK);
    R_CHECK(&rig, moqr_bind_retrieve_source_set(rig.bind,
                                               d22_gap_source_peek) == MOQR_OK);
    moq_subscription_t h;
    R_CHECK(&rig, relay_bind_test_d22_subscribe_fill(&rig, sub, &t, true, 0, &h) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 8);
    peer_state_t ps;
    memset(&ps, 0, sizeof(ps));
    relay_bind_test_peer_drain(&rig, sub, &ps, false, false, 0, 0);
    unsigned commits = mode == D22_GAP_NONTERMINAL_NI ? 2 : 1;
    R_CHECK(&rig, source.opens == 1 && source.commits == commits &&
                      source.terminal_peeks == 0);
    R_CHECK(&rig, ps.subscribe_ok && ps.has_largest && ps.largest_group == 0 &&
                      ps.largest_object == 2 && ps.fill_objects == 0);
    if (mode != D22_GAP_COMMIT_ERROR) {
        R_CHECK(&rig, source.live && !source.pending && source.closes == 0);
        R_CHECK(&rig, ps.fill_terminals == 0);
        R_CHECK(&rig, moqr_bind_retrieve_source_set(rig.bind,
                         d22_gap_source_peek) == MOQR_ERR_WRONG_STATE);
        if (mode == D22_GAP_FIN_BLOCKED) {
            /* Saturate actual session actions, not a bind counter or clock.
             * END may close the source, but FIN must remain owed. */
            moq_bytes_t part = B("gap-policy-pressure");
            moq_track_status_cfg_t cfg;
            moq_track_status_cfg_init(&cfg);
            cfg.track_namespace = (moq_namespace_t){ .parts = &part, .count = 1 };
            moq_result_t rc = MOQ_OK;
            unsigned queued = 0;
            for (; queued < 16; queued++) {
                char name[16];
                (void)snprintf(name, sizeof(name), "pressure-%u", queued);
                cfg.track_name = B(name);
                moq_track_status_handle_t status;
                rc = moq_session_track_status(sub->rsess, &cfg, rig.now, &status);
                if (rc != MOQ_OK) break;
            }
            R_CHECK(&rig, queued == 4 && rc == MOQ_ERR_WOULD_BLOCK);
        }
        source.allow_end = true;
        R_CHECK(&rig, moqr_bind_pump(rig.bind, rig.now) == MOQR_OK);
        if (mode == D22_GAP_FIN_BLOCKED) {
            R_CHECK(&rig, source.closes == 1 && source.terminal_peeks == 1);
            R_CHECK(&rig, moqr_bind_retrieve_source_set(rig.bind,
                             d22_gap_source_peek) == MOQR_ERR_WRONG_STATE);
            R_CHECK(&rig, moqr_bind_pump(rig.bind, rig.now) == MOQR_OK);
            R_CHECK(&rig, source.commits == commits && source.closes == 1 &&
                              source.terminal_peeks == 1);
            relay_bind_test_peer_drain(&rig, sub, &ps, false, false, 0, 0);
            R_CHECK(&rig, ps.fill_terminals == 0);
        }
    }
    relay_bind_test_rig_pump(&rig, 8);
    relay_bind_test_peer_drain(&rig, sub, &ps, false, false, 0, 0);
    d22_gap_t want[2];
    int gaps = 0;
    /* Only the marked Unknown run needs a preceding explicit Non-Existent
     * split (§3.2.2). In capped-claim, the following Object proves the gap
     * implicitly even though the two readers supplied different pieces. */
    if (mode == D22_GAP_NONTERMINAL_NI) {
        want[gaps++] = (d22_gap_t){ MOQ_FETCH_RANGE_NON_EXISTENT, 0, 0 };
    }
    if (mode == D22_GAP_NONTERMINAL_NI || mode == D22_GAP_TERMINAL_UNKNOWN) {
        want[gaps++] = (d22_gap_t){ MOQ_FETCH_RANGE_UNKNOWN, 0, 2 };
    }
    if (mode == D22_GAP_TERMINAL_TIMED_OUT) {
        want[gaps++] = (d22_gap_t){ MOQ_FETCH_RANGE_TIMED_OUT, 0, 2 };
    }
    bool reset = mode == D22_GAP_COMMIT_ERROR || mode == D22_GAP_SOURCE_ERROR ||
                 mode == D22_GAP_BAD_END;
    R_CHECK(&rig, d22_fill_gaps_are(&ps, want, gaps));
    R_CHECK(&rig, ps.fill_objects == (mode == D22_GAP_CAPPED_CLAIM ? 2 : 0));
    R_CHECK(&rig, ps.fill_terminals == 1 && ps.fill_term_reason ==
                      (reset ? MOQ_FILL_PEER_CANCELLED_OR_RESET : MOQ_FILL_COMPLETE));
    R_CHECK(&rig, source.opens == 1 && source.commits == commits &&
                      source.closes == 1 && !source.live && !source.pending);
    R_CHECK(&rig, source.terminal_peeks == (mode == D22_GAP_COMMIT_ERROR ? 0u : 1u));
    R_CHECK(&rig, source.pending_on_close == (mode == D22_GAP_COMMIT_ERROR));
    R_CHECK(&rig, moqr_bind_retrieve_source_set(rig.bind,
                                               d22_gap_source_peek) == MOQR_OK);
    relay_bind_test_rig_pump(&rig, 4);
    relay_bind_test_peer_drain(&rig, sub, &ps, false, false, 0, 0);
    R_CHECK(&rig, ps.fill_terminals == 1 && source.closes == 1);
    R_CHECK(&rig, !ps.done_seen && !ps.session_closed && ps.objects == 0);
    moqr_bind_stats_t stats;
    moqr_bind_get_stats(rig.bind, &stats);
    R_CHECK(&rig, stats.session_errors == 0);
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0 && source.closes == 1);
    printf("%s: draft22_fill_terminal_gap(%s)\n",
           rig.failures == 0 ? "PASS" : "FAIL", names[mode]);
    return rig.failures;
}

static int
draft22_fill_terminal_gap_all(void)
{
    int failures = 0;
    for (int mode = 0; mode < D22_GAP_POLICY_MODES; mode++) {
        failures += draft22_fill_terminal_gap((d22_gap_policy_mode_t)mode);
    }
    return failures;
}

/* -- Downstream terminal refused ------------------------------------------ */

/* What one downstream peer saw: accepted publications, subscription
 * acceptance, terminals of either kind, and whether its session ended. */
typedef struct term_obs {
    int  pubs_accepted;
    bool sub_ok;
    int  sub_dones;
    int  pub_finished;
    bool closed;
} term_obs_t;

static void
term_drain(rig_t *r, conn_t *c, term_obs_t *o)
{
    moq_event_t evs[16];
    for (int guard = 0; guard < 64; guard++) {
        size_t n = moq_session_poll_events(c->peer, evs, 16);
        if (n == 0) {
            break;
        }
        for (size_t i = 0; i < n; i++) {
            moq_event_t *ev = &evs[i];
            switch (ev->kind) {
            case MOQ_EVENT_PUBLISH_REQUEST: {
                moq_accept_publish_cfg_t a;
                moq_accept_publish_cfg_init(&a);
                R_CHECK(r, moq_session_accept_publish(
                               c->peer, ev->u.publish_request.pub, &a,
                               r->now) == MOQ_OK);
                o->pubs_accepted++;
                break;
            }
            case MOQ_EVENT_SUBSCRIBE_OK:
                o->sub_ok = true;
                break;
            case MOQ_EVENT_SUBSCRIBE_DONE:
                o->sub_dones++;
                break;
            case MOQ_EVENT_PUBLISH_FINISHED:
                o->pub_finished++;
                break;
            case MOQ_EVENT_SESSION_CLOSED:
                o->closed = true;
                break;
            default:
                break;
            }
            moq_event_cleanup(ev);
        }
    }
}

/* A draft-22 origin publishes a/t1; one downstream peer takes it as a
 * discovered PUBLISH (SUBSCRIBE_TRACKS, Forward=1) and a second takes it
 * with an ordinary SUBSCRIBE. The origin then finishes the track, so both
 * downstream requests are owed a terminal.
 *   kind 1: the SUBSCRIBE's terminal write is refused hard (nothing queued);
 *   kind 2: the PUBLISH's terminal write is refused hard;
 *   kind 3: control -- the PUBLISH's terminal write first meets WOULD_BLOCK.
 * A refused terminal can never be delivered, so the binding must not release
 * the request as if it were sent: that peer's session is closed, so the
 * request is not left hanging. The other peer gets its one terminal and keeps
 * its session. With a transient block nobody is closed and each gets exactly
 * one terminal after the retry. */
static int
draft22_terminal_refused(int kind)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: terminal_refused rig create\n");
        return 1;
    }
    conn_t *pub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_22);
    conn_t *ps = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_22);
    conn_t *ss = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_22);
    R_CHECK(&rig, pub && ps && ss);
    relay_bind_test_rig_pump(&rig, 4);

    moq_bytes_t part = B("a");
    moq_publish_cfg_t pc;
    moq_publish_cfg_init_sized(&pc, sizeof(pc));
    pc.track_namespace = (moq_namespace_t){ .parts = &part, .count = 1 };
    pc.track_name = B("t1");
    moq_publication_t pubh = { 0 };
    R_CHECK(&rig, moq_session_publish(pub->peer, &pc, rig.now, &pubh) ==
                      MOQ_OK);
    relay_bind_test_rig_pump(&rig, 6);
    peer_state_t pub_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    relay_bind_test_peer_drain(&rig, pub, &pub_ps, false, false, 0, 0);
    R_CHECK(&rig, pub_ps.publish_ok);

    moq_subscribe_tracks_cfg_t tc;
    moq_subscribe_tracks_cfg_init(&tc);
    tc.track_namespace_prefix = (moq_namespace_t){ .parts = &part, .count = 1 };
    tc.has_forward = true;
    tc.forward = true;
    moq_track_sub_handle_t th;
    R_CHECK(&rig, moq_session_subscribe_tracks(ps->peer, &tc, rig.now, &th) ==
                      MOQ_OK);
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ .parts = &part, .count = 1 };
    sc.track_name = B("t1");
    moq_subscription_t sh;
    R_CHECK(&rig, moq_session_subscribe(ss->peer, &sc, rig.now, &sh) ==
                      MOQ_OK);
    term_obs_t po, so;
    memset(&po, 0, sizeof(po));
    memset(&so, 0, sizeof(so));
    for (int k = 0; k < 12; k++) {
        relay_bind_test_rig_pump(&rig, 1);
        term_drain(&rig, ps, &po);
        term_drain(&rig, ss, &so);
    }
    R_CHECK(&rig, po.pubs_accepted == 1);
    R_CHECK(&rig, so.sub_ok);

    if (kind == 1 || kind == 2) {
        moqr_bind_debug_fail_sub_done(1, kind);
    } else {
        moqr_bind_debug_block_sub_done(1);
    }
    moqr_bind_stats_t bs;
    moqr_bind_get_stats(rig.bind, &bs);
    uint64_t errs0 = bs.session_errors;
    moq_finish_publish_cfg_t fc;
    moq_finish_publish_cfg_init(&fc);
    fc.status_code = 0x2;   /* TRACK_ENDED */
    fc.stream_count = 0;
    DP_RETRY(&rig, moq_session_finish_publish(pub->peer, pubh, &fc, rig.now));
    for (int k = 0; k < 24; k++) {
        relay_bind_test_rig_pump(&rig, 1);
        term_drain(&rig, ps, &po);
        term_drain(&rig, ss, &so);
    }
    moqr_bind_debug_fail_sub_done(0, 0);
    moqr_bind_debug_block_sub_done(0);

    if (kind == 1) {
        R_CHECK(&rig, so.closed && so.sub_dones == 0);
        R_CHECK(&rig, !po.closed && po.pub_finished == 1);
    } else if (kind == 2) {
        R_CHECK(&rig, po.closed && po.pub_finished == 0);
        R_CHECK(&rig, !so.closed && so.sub_dones == 1);
    } else {
        R_CHECK(&rig, !po.closed && po.pub_finished == 1);
        R_CHECK(&rig, !so.closed && so.sub_dones == 1);
    }
    moqr_bind_get_stats(rig.bind, &bs);
    R_CHECK(&rig, bs.session_errors == errs0 + (kind == 3 ? 0u : 1u));
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    printf("%s: draft22_terminal_refused(%s)\n", f == 0 ? "PASS" : "FAIL",
           kind == 1 ? "subscription" : kind == 2 ? "publication"
                                                  : "blocked-retry control");
    return f;
}

/* -- Downstream terminal under a short send arena --------------------------- */

/* Control bytes the relay's downstream session put on the wire to the watched
 * peer (server -> client), in order. */
typedef struct arena_trace {
    size_t len[64];
    int    n;
} arena_trace_t;

static void
arena_trace_fn(void *ctx, const moq_sim_trace_record_t *r)
{
    arena_trace_t *t = ctx;
    if (r->kind != MOQ_SIM_TRACE_ACTION || r->from != MOQ_PERSPECTIVE_SERVER) {
        return;
    }
    size_t len = 0;
    if (r->action_kind == MOQ_ACTION_SEND_CONTROL) {
        len = r->bytes.len;
    } else if ((r->action_kind == MOQ_ACTION_SEND_BIDI_STREAM ||
                r->action_kind == MOQ_ACTION_OPEN_BIDI_STREAM) &&
               r->struct_size >= offsetof(moq_sim_trace_record_t,
                                          detail_bytes) +
                                     sizeof(r->detail_bytes)) {
        len = r->detail_bytes.len;
    } else {
        return;
    }
    if (len > 0 && t->n < 64) {
        t->len[t->n++] = len;
    }
}

#define ARENA_CAP 4096u   /* MOQ_DEFAULT_SEND_BUF: the session's send arena */

typedef struct arena_result {
    size_t ns_len[2];     /* the two namespace notifications, as sent */
    size_t done_len;      /* the subscription's terminal, as sent (0: none) */
    bool   watch_closed;
    int    watch_dones;
    bool   sib_closed;
    int    sib_dones;
    bool   ns_seen;
    uint64_t session_errors;   /* the binding's, accumulated by this run */
} arena_result_t;

/* A watching peer subscribes to the track fill/v and to the namespace prefix
 * fill; a sibling peer subscribes to fill/v too. With `frozen`, the watching
 * peer's transport is then stopped, so whatever the relay sends it stays
 * queued in its session's send arena. The origin announces two namespaces
 * under fill whose suffixes are `l1` and `l2` bytes long -- their
 * notifications are queued toward the watching peer -- and then ends
 * fill/v, so its terminal is written into what is left of the arena. Finally
 * the watching peer's transport resumes. */
static void
arena_run(moq_version_t v, size_t l1, size_t l2, bool frozen, bool finish,
          arena_result_t *res, int *failures)
{
    memset(res, 0, sizeof(*res));
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        (*failures)++;
        return;
    }
    conn_t *pub = relay_bind_test_rig_connect(&rig, v);
    arena_trace_t tr;
    memset(&tr, 0, sizeof(tr));
    relay_bind_test_trace_fn = arena_trace_fn;
    relay_bind_test_trace_ctx = &tr;
    conn_t *watch = relay_bind_test_rig_connect(&rig, v);
    relay_bind_test_trace_fn = NULL;
    relay_bind_test_trace_ctx = NULL;
    conn_t *sib = relay_bind_test_rig_connect(&rig, v);
    R_CHECK(&rig, pub && watch && sib);
    relay_bind_test_rig_pump(&rig, 4);

    moq_bytes_t root = B("fill");
    moq_publish_cfg_t pc;
    moq_publish_cfg_init_sized(&pc, sizeof(pc));
    pc.track_namespace = (moq_namespace_t){ .parts = &root, .count = 1 };
    pc.track_name = B("v");
    moq_publication_t pubh = { 0 };
    R_CHECK(&rig, moq_session_publish(pub->peer, &pc, rig.now, &pubh) ==
                      MOQ_OK);
    relay_bind_test_rig_pump(&rig, 6);

    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ .parts = &root, .count = 1 };
    sc.track_name = B("v");
    moq_subscription_t sh;
    R_CHECK(&rig, moq_session_subscribe(watch->peer, &sc, rig.now, &sh) ==
                      MOQ_OK);
    R_CHECK(&rig, moq_session_subscribe(sib->peer, &sc, rig.now, &sh) ==
                      MOQ_OK);
    moq_subscribe_namespace_cfg_t nsc;
    moq_subscribe_namespace_cfg_init(&nsc);
    nsc.track_namespace_prefix = (moq_namespace_t){ .parts = &root, .count = 1 };
    nsc.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
    moq_ns_sub_handle_t nsh;
    R_CHECK(&rig, moq_session_subscribe_namespace(watch->peer, &nsc, rig.now,
                                                  &nsh) == MOQ_OK);
    term_obs_t wo, so;
    memset(&wo, 0, sizeof(wo));
    memset(&so, 0, sizeof(so));
    peer_state_t wps;
    memset(&wps, 0, sizeof(wps));
    for (int k = 0; k < 12; k++) {
        relay_bind_test_rig_pump(&rig, 1);
        term_drain(&rig, sib, &so);
        relay_bind_test_peer_drain(&rig, watch, &wps, false, false, 0, 0);
    }
    R_CHECK(&rig, so.sub_ok && wps.subscribe_ok);
    int before = tr.n;

    if (frozen) {
        watch->frozen = true;
    }
    static uint8_t big[2][ARENA_CAP];
    size_t lens[2] = { l1, l2 };
    for (int i = 0; i < 2; i++) {
        R_CHECK(&rig, lens[i] <= sizeof(big[i]));
        if (lens[i] > sizeof(big[i])) {
            lens[i] = sizeof(big[i]);
        }
        memset(big[i], 'a' + i, sizeof(big[i]));
        moq_bytes_t parts[2] = { root, { big[i], lens[i] } };
        moq_publish_namespace_cfg_t nc;
        moq_publish_namespace_cfg_init(&nc);
        nc.track_namespace = (moq_namespace_t){ .parts = parts, .count = 2 };
        moq_announcement_t ann;
        R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &nc, rig.now,
                                                    &ann) == MOQ_OK);
        relay_bind_test_rig_pump(&rig, 4);
    }
    if (finish) {
        moq_finish_publish_cfg_t fc;
        moq_finish_publish_cfg_init(&fc);
        fc.status_code = 0x2;   /* TRACK_ENDED */
        fc.stream_count = 0;
        DP_RETRY(&rig, moq_session_finish_publish(pub->peer, pubh, &fc,
                                                  rig.now));
    }
    relay_bind_test_rig_pump(&rig, 8);
    term_drain(&rig, sib, &so);
    watch->frozen = false;
    for (int k = 0; k < 24; k++) {
        relay_bind_test_rig_pump(&rig, 1);
        term_drain(&rig, sib, &so);
        relay_bind_test_peer_drain(&rig, watch, &wps, false, false, 0, 0);
    }
    int sent = tr.n - before;
    res->ns_len[0] = sent > 0 ? tr.len[before] : 0;
    res->ns_len[1] = sent > 1 ? tr.len[before + 1] : 0;
    res->done_len = sent > 2 ? tr.len[before + 2] : 0;
    res->watch_closed = wps.session_closed;
    res->watch_dones = wps.done_count;
    res->sib_closed = so.closed;
    res->sib_dones = so.sub_dones;
    res->ns_seen = wps.ns_found == 2;
    moqr_bind_stats_t bs;
    moqr_bind_get_stats(rig.bind, &bs);
    res->session_errors = bs.session_errors;
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    *failures += rig.failures;
}

/* A subscription's terminal that does not fit the occupied tail of its
 * session's send arena, though it fits the arena once the queued output has
 * drained, is only delayed: the relay sends it after that output, exactly
 * once, without closing the session, and the sibling subscriber is
 * unaffected. Three runs of the same sequence: a calibration run with
 * everything flowing measures the two namespace notifications and the
 * terminal on the wire; a fill control (frozen, no terminal) proves the
 * notifications occupy all but terminal - 1 bytes of the arena; the real run
 * then ends the track at that fill. */
static int
arena_short_tail_terminal(moq_version_t v)
{
    int failures = 0;
    arena_result_t cal, run;
    const size_t l1 = 1500, l2_probe = 1500;
    arena_run(v, l1, l2_probe, false, true, &cal, &failures);
    if (cal.ns_len[0] == 0 || cal.ns_len[1] == 0 || cal.done_len == 0 ||
        cal.watch_dones != 1) {
        printf("FAIL: arena_short_tail_terminal(%u) calibration: ns %zu %zu "
               "done %zu dones %d\n", (unsigned)v, cal.ns_len[0],
               cal.ns_len[1], cal.done_len, cal.watch_dones);
        return failures + 1;
    }
    /* Leave exactly done_len - 1 bytes after the two notifications. */
    size_t target = ARENA_CAP - (cal.done_len - 1);
    size_t over2 = cal.ns_len[1] - l2_probe;
    size_t l2 = target - cal.ns_len[0] - over2;
    arena_result_t fill;
    arena_run(v, l1, l2, true, false, &fill, &failures);
    bool exact = fill.ns_len[0] == cal.ns_len[0] &&
                 fill.ns_len[0] + fill.ns_len[1] == target && fill.ns_seen &&
                 !fill.watch_closed && fill.session_errors == 0;
    arena_run(v, l1, l2, true, true, &run, &failures);
    if (!exact) {
        printf("FAIL: arena_short_tail_terminal(%u) fill control: ns %zu + "
               "%zu, want %zu (closed=%d ns_seen=%d errors=%llu)\n",
               (unsigned)v, fill.ns_len[0], fill.ns_len[1], target,
               (int)fill.watch_closed, (int)fill.ns_seen,
               (unsigned long long)fill.session_errors);
        return failures + 1;
    }
    int f = failures;
    if (run.watch_closed || run.watch_dones != 1 || run.done_len != cal.done_len)
        f++;
    if (run.sib_closed || run.sib_dones != 1 || run.session_errors != 0)
        f++;
    printf("%s: arena_short_tail_terminal(draft %u): fill exact, tail %zu < "
           "terminal %zu; watch closed=%d dones=%d; sibling closed=%d "
           "dones=%d; binding session errors %llu\n",
           f == 0 ? "PASS" : "FAIL", (unsigned)(v & 0xff),
           ARENA_CAP - target, cal.done_len, (int)run.watch_closed,
           run.watch_dones, (int)run.sib_closed, run.sib_dones,
           (unsigned long long)run.session_errors);
    return f;
}

int
main(int argc, char **argv)
{
    if (argc == 2 && strcmp(argv[1], "--late-subscriber") == 0) {
        int failures = draft22_fill_before_upstream_subscription(false, false);
        failures += draft22_fill_before_upstream_subscription(true, false);
        failures += draft22_fill_before_upstream_subscription(false, true);
        failures += draft22_fill_before_upstream_subscription(true, true);
        return failures == 0 ? 0 : 1;
    }
    if (argc == 2 && strcmp(argv[1], "--fill-terminal-gap") == 0) {
        return draft22_fill_terminal_gap_all() == 0 ? 0 : 1;
    }
    if (argc == 2 && strcmp(argv[1], "--fill-terminal-causal") == 0) {
        int failures = 0;
        for (unsigned held = 0; held < 2; held++) {
            for (unsigned streaming = 0; streaming < 2; streaming++) {
                failures += draft22_update_largest_is_relay(held != 0, streaming != 0);
            }
        }
        failures += draft22_update_fill_overlaps_live();
        return failures == 0 ? 0 : 1;
    }
    if (argc == 2 && strcmp(argv[1], "--fill-origin-response") == 0) {
        return draft22_fill_served() == 0 ? 0 : 1;
    }
    if (argc == 2 && strcmp(argv[1], "--fill-origin-unanswered") == 0) {
        return draft22_fill_origin_unanswered() == 0 ? 0 : 1;
    }
    int failures = 0;
    failures += draft22_conn_version();
    failures += draft22_filter_mapping();
    failures += draft_range_end(MOQ_VERSION_DRAFT_18);
    failures += draft_range_end(MOQ_VERSION_DRAFT_22);
    failures += draft22_fill_served();
    failures += draft22_fill_before_upstream_subscription(false, false);
    failures += draft22_fill_before_upstream_subscription(true, false);
    failures += draft22_fill_before_upstream_subscription(false, true);
    failures += draft22_fill_before_upstream_subscription(true, true);
    failures += draft22_fill_origin_unanswered();
    failures += draft22_fill_terminal_gap_all();
    failures += draft22_fill_failed_on_wire();
    failures += draft22_fill_evicted_prefix(false);
    failures += draft22_fill_evicted_prefix(true);
    failures += draft22_fill_timeout_zero_partial();
    failures += draft22_fill_timeout_waits_for_upstream();
    failures += draft22_fill_timeout_expires();
    failures += draft22_fill_timeout_capped();
    failures += draft22_fill_timeout_old_cfg();
    failures += draft22_fill_absent_timeout_default();
    failures += bind_fill_wait_cfg();
    failures += draft22_update_filter_applied(false, false);
    failures += draft22_update_filter_applied(true, false);
    failures += draft22_update_filter_applied(true, true);
    failures += draft22_update_none_next_late(false);
    failures += draft22_update_none_next_late(true);
    failures += draft22_update_relative_snapshot(MOQ_LOCATION_FILTER_NEXT_OBJECT, false, false);
    failures += draft22_update_relative_snapshot(MOQ_LOCATION_FILTER_RELATIVE_GROUP, false, false);
    failures += draft22_update_relative_snapshot(MOQ_LOCATION_FILTER_NEXT_OBJECT, true, false);
    failures += draft22_update_relative_snapshot(MOQ_LOCATION_FILTER_RELATIVE_GROUP, true, false);
    failures += draft22_update_relative_snapshot(MOQ_LOCATION_FILTER_NEXT_OBJECT, false, true);
    failures += draft22_update_relative_snapshot(MOQ_LOCATION_FILTER_RELATIVE_GROUP, false, true);
    failures += draft22_update_relative_snapshot(MOQ_LOCATION_FILTER_NEXT_OBJECT, true, true);
    failures += draft22_update_relative_snapshot(MOQ_LOCATION_FILTER_RELATIVE_GROUP, true, true);
    failures += draft22_update_largest_is_relay(false, false);
    failures += draft22_update_largest_is_relay(true, false);
    failures += draft22_update_largest_is_relay(false, true);
    failures += draft22_update_largest_is_relay(true, true);
    failures += draft22_update_fill_overlaps_live();
    failures += draft22_update_object_end_or_invalid(true);
    failures += draft22_update_filter_relative_deep(true);
    failures += draft22_update_object_end_or_invalid(false);
    failures += draft22_update_filter_relative_deep(false);
    failures += draft22_update_answer_held_accept();
    failures += draft22_update_answer_held_refusal();
    failures += draft22_update_answer_before_sub_done(false);
    failures += draft22_update_answer_before_sub_done(true);
    failures += draft_update_filter_legacy_unchanged(MOQ_VERSION_DRAFT_16);
    failures += draft_update_filter_legacy_unchanged(MOQ_VERSION_DRAFT_18);
    for (int kind = 1; kind <= 3; kind++) {
        failures += draft22_terminal_refused(kind);
    }
    failures += arena_short_tail_terminal(MOQ_VERSION_DRAFT_16);
    failures += arena_short_tail_terminal(MOQ_VERSION_DRAFT_18);
    failures += arena_short_tail_terminal(MOQ_VERSION_DRAFT_22);
    return failures == 0 ? 0 : 1; /* exit status truncates to 8 bits */
}
