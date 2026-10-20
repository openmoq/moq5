/*
 * FETCH through the production relay binding over real sessions: retained
 * FETCH with exact payloads, evicted-prefix and unvouched-range markers at
 * each draft's maximum object id, datagram preference across drafts,
 * chunked records served with whole-object parity, backpressure, mid-stream
 * close, errors and coalesce OOM failing closed, and the draft-22
 * relay-resolved FETCH with its fill timeout and in-progress responses.
 *
 * The forwarded Joining FETCH: every row asserts the exact downstream
 * ledger, the origin's observed requests and cancellations, and the
 * binding's forwarding gauges back to zero, across upstream error, reset
 * and source loss, downstream cancel and abort, queue blocking, auth, slot
 * and OOM refusal, the config ABI and request lifetime. A fetcher's session
 * that has cancelled absorbs the relay's late response stream and stays
 * established — a core-contract check run only by mode, never in the
 * ordinary suite.
 */
#include "relay_session_binding_harness.h"

/* Ingest a NORMAL object flagged with datagram preference (a d18 fetcher keeps
 * the bit; a d16 fetcher clears it). Otherwise identical to relay_bind_test_fetch_ing. */
static void
fetch_ing_dg(rig_t *r, moqr_track_t tk, uint64_t g, uint64_t sg, uint64_t o,
             uint8_t fp)
{
    uint8_t body[8];
    memset(body, fp, sizeof(body));
    moq_rcbuf_t *pl = NULL;
    R_CHECK(r, moq_rcbuf_create(&r->alloc->vt, body, sizeof(body), &pl) == 0);
    moqr_log_append_desc_t d;
    moqr_log_append_desc_init(&d);
    d.group_id = g;
    d.subgroup_id = sg;
    d.object_id = o;
    d.publisher_priority = 1;
    d.datagram_pref = true;
    d.payload = pl;
    d.now_us = r->now;
    if (moqr_core_ingest(r->core, tk, &d) != MOQR_OK) {
        moq_rcbuf_decref(pl);
        R_CHECK(r, 0);
    }
}

/* Exact retained replay + multi-subgroup merge + INVALID_RANGE, per draft. */
static int
fetch_flow(moq_version_t version)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != 0) {
        printf("FAIL: fetch rig create (v%d)\n", (int)version);
        return 1;
    }
    conn_t *sub = relay_bind_test_rig_connect(&rig, version);
    R_CHECK(&rig, sub != NULL);
    relay_bind_test_rig_pump(&rig, 4);

    moqr_binding_t pb = relay_bind_test_fetch_pub_open(&rig);
    moqr_track_t t1 = relay_bind_test_fetch_pub_track(&rig, pb, "v1");
    moqr_track_t t2 = relay_bind_test_fetch_pub_track(&rig, pb, "v2");

    /* T1: exact retained replay over one subgroup -> ids + payload bytes. */
    for (uint64_t o = 0; o < 3; o++) {
        relay_bind_test_fetch_ing(&rig, t1, 0, 0, o, (uint8_t)(0x10 + o));
    }
    relay_bind_test_rig_pump(&rig, 2);
    moq_fetch_t fh1;
    relay_bind_test_fetch_issue(&rig, sub, "v1", 0, 0, 0, 3, &fh1); /* [0:0 .. 0:3) */
    relay_bind_test_rig_pump(&rig, 10);
    peer_state_t ps;
    memset(&ps, 0, sizeof(ps));
    relay_bind_test_peer_drain(&rig, sub, &ps, false, false, 0, 0);
    R_CHECK(&rig, ps.fetch_ok);
    R_CHECK(&rig, ps.fok_end_group == 0 && ps.fok_end_object == 3);
    R_CHECK(&rig, !ps.fok_eot);
    R_CHECK(&rig, ps.fetch_complete == 1);
    R_CHECK(&rig, ps.fetch_error == 0);
    R_CHECK(&rig, ps.fdlv_n == 3);
    for (int i = 0; i < 3 && i < ps.fdlv_n; i++) {
        R_CHECK(&rig, ps.fdlv[i].group == 0);
        R_CHECK(&rig, ps.fdlv[i].object == (uint64_t)i);
        R_CHECK(&rig, ps.fdlv[i].fp == (uint8_t)(0x10 + i));
    }

    /* T3: multi-subgroup merge -> strict object-id order 0..5. */
    relay_bind_test_fetch_ing(&rig, t2, 5, 0, 0, 0xA0);
    relay_bind_test_fetch_ing(&rig, t2, 5, 0, 2, 0xA2);
    relay_bind_test_fetch_ing(&rig, t2, 5, 0, 4, 0xA4);
    relay_bind_test_fetch_ing(&rig, t2, 5, 1, 1, 0xA1);
    relay_bind_test_fetch_ing(&rig, t2, 5, 1, 3, 0xA3);
    relay_bind_test_fetch_ing(&rig, t2, 5, 1, 5, 0xA5);
    relay_bind_test_rig_pump(&rig, 2);
    moq_fetch_t fh2;
    relay_bind_test_fetch_issue(&rig, sub, "v2", 5, 0, 5, 0, &fh2); /* whole group 5 */
    relay_bind_test_rig_pump(&rig, 10);
    peer_state_t ps2;
    memset(&ps2, 0, sizeof(ps2));
    relay_bind_test_peer_drain(&rig, sub, &ps2, false, false, 0, 0);
    R_CHECK(&rig, ps2.fetch_ok && ps2.fetch_complete == 1);
    R_CHECK(&rig, ps2.fdlv_n == 6);
    for (int i = 0; i < 6 && i < ps2.fdlv_n; i++) {
        R_CHECK(&rig, ps2.fdlv[i].object == (uint64_t)i);
    }

    /* T6: INVALID_RANGE — start past the largest object of the track. */
    moq_fetch_t fh3;
    relay_bind_test_fetch_issue(&rig, sub, "v1", 9, 0, 9, 1, &fh3);
    relay_bind_test_rig_pump(&rig, 8);
    peer_state_t ps3;
    memset(&ps3, 0, sizeof(ps3));
    relay_bind_test_peer_drain(&rig, sub, &ps3, false, false, 0, 0);
    R_CHECK(&rig, ps3.fetch_error == 1);
    R_CHECK(&rig, ps3.fetch_error_code == MOQ_REQUEST_ERROR_INVALID_RANGE);
    R_CHECK(&rig, !ps3.fetch_ok);

    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (rig.failures == 0) {
        printf("PASS: fetch_flow (v%d)\n", (int)version);
    }
    return rig.failures;
}

/* Evicted-prefix fetch. The Start falls below the retention horizon, so
 * the relay serves the retained suffix led by one UNKNOWN marker before the
 * first retained group -- via the profile-owned seam. Proves the profile max
 * crosses the wire per draft (asserted here as literals, since the relay
 * boundary forbids the wire headers; the session test pins them symbolically:
 * d16 = MOQ_QUIC_VARINT_MAX = 2^62-1, d18 = MOQ_VI64_MAX = UINT64_MAX). With
 * max_actions != 0 the marker write WOULD_BLOCKs first and must re-peek/retry --
 * never skip. */
static int
fetch_evicted_prefix(moq_version_t version, uint64_t expect_max,
                     uint32_t max_actions)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    relay_bind_test_log_max_groups = 3; /* retain 3 whole groups -> prefix evicts */
    rig_t rig;
    int rc = relay_bind_test_rig_create(&rig, &a);
    relay_bind_test_log_max_groups = 0;
    if (rc != 0) {
        printf("FAIL: evicted-prefix rig create (v%d)\n", (int)version);
        return 1;
    }
    relay_bind_test_max_actions = max_actions; /* 0 = default; >0 forces WOULD_BLOCK */
    conn_t *sub = relay_bind_test_rig_connect(&rig, version);
    relay_bind_test_max_actions = 0;
    R_CHECK(&rig, sub != NULL);
    relay_bind_test_rig_pump(&rig, 8);

    moqr_binding_t pb = relay_bind_test_fetch_pub_open(&rig);
    moqr_track_t tk = relay_bind_test_fetch_pub_track(&rig, pb, "v");
    /* Groups 0..5, one object each. max_groups=3 evicts 0,1,2 -> oldest == 3. */
    for (uint64_t g = 0; g <= 5; g++) {
        relay_bind_test_fetch_ing(&rig, tk, g, 0, 0, (uint8_t)(0x30 + g));
    }
    relay_bind_test_rig_pump(&rig, 4);

    moq_fetch_t fh;
    relay_bind_test_fetch_issue(&rig, sub, "v", 0, 0, 5, 1, &fh); /* Start below the horizon */
    relay_bind_test_rig_pump(&rig, 60); /* enough for any WOULD_BLOCK write to drain */
    peer_state_t ps;
    memset(&ps, 0, sizeof(ps));
    relay_bind_test_peer_drain(&rig, sub, &ps, false, false, 0, 0);

    R_CHECK(&rig, ps.fetch_ok);
    R_CHECK(&rig, ps.fok_end_group == 5 && ps.fok_end_object == 1);
    R_CHECK(&rig, ps.fetch_error == 0);
    /* One UNKNOWN marker at { oldest-1 = 2, profile_max } for the evicted
     * prefix. Groups 3 and 4 end with no END_OF_GROUP evidence, so the rest
     * of each is of unknown state and is marked through the same seam, at
     * { 3, profile_max } and { 4, profile_max }; group 5 holds Largest. */
    R_CHECK(&rig, ps.fgap_n == 3);
    for (int i = 0; i < 3 && i < ps.fgap_n; i++) {
        R_CHECK(&rig, ps.fgap[i].kind == MOQ_FETCH_RANGE_UNKNOWN);
        R_CHECK(&rig, ps.fgap[i].group == (uint64_t)(2 + i));
        R_CHECK(&rig, ps.fgap[i].object == expect_max);
    }
    /* Retained objects from group 3 onward (3,4,5), object 0 each. The marker
     * precedes them: d18 is ascending-only, so a marker at group 2 arriving
     * after an object at group 3 would fault -- a clean completion is the
     * ordering proof. */
    R_CHECK(&rig, ps.fdlv_n == 3);
    for (int i = 0; i < 3 && i < ps.fdlv_n; i++) {
        R_CHECK(&rig, ps.fdlv[i].group == (uint64_t)(3 + i));
        R_CHECK(&rig, ps.fdlv[i].object == 0);
        R_CHECK(&rig, ps.fdlv[i].fp == (uint8_t)(0x30 + 3 + i));
    }
    R_CHECK(&rig, ps.fetch_complete == 1);

    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (rig.failures == 0) {
        printf("PASS: fetch_evicted_prefix (v%d%s)\n", (int)version,
               max_actions ? ", backpressure" : "");
    }
    return rig.failures;
}

/* Every Location of a FETCH response is served, marked, or known not to
 * exist: an unmarked gap tells the fetcher the skipped Objects do not exist
 * (in every draft), so the relay marks the Locations it cannot vouch for.
 * Group 0 holds {0, 2} with no END_OF_GROUP evidence, group 1 was never seen,
 * group 2 holds {0} and an END_OF_GROUP at 1, group 3 holds Largest {3,0}.
 * The response is {0,0}, a marker at the hole {0,1}, {0,2}, one marker
 * through the end of group 1 (group 0's tail and the unseen group), {2,0},
 * nothing past group 2's end, {3,0}. A FETCH does not wait, and the relay
 * cannot determine those Objects' state, so every marker is Unknown. A run
 * that ends with its group carries the draft's last object id. */
static int
fetch_unvouched_marked(moq_version_t version, uint32_t expect_kind,
                       uint64_t expect_max)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != 0) {
        printf("FAIL: fetch_unvouched_marked rig create (v%d)\n", (int)version);
        return 1;
    }
    conn_t *sub = relay_bind_test_rig_connect(&rig, version);
    R_CHECK(&rig, sub != NULL);
    relay_bind_test_rig_pump(&rig, 4);
    moqr_binding_t pb = relay_bind_test_fetch_pub_open(&rig);
    moqr_track_t tk = relay_bind_test_fetch_pub_track(&rig, pb, "v");
    relay_bind_test_fetch_ing(&rig, tk, 0, 0, 0, 0x00);
    relay_bind_test_fetch_ing(&rig, tk, 0, 0, 2, 0x02);
    relay_bind_test_fetch_ing(&rig, tk, 2, 0, 0, 0x20);
    moqr_log_append_desc_t d;
    moqr_log_append_desc_init(&d);
    d.group_id = 2;
    d.object_id = 1;
    d.publisher_priority = 1;
    d.status = MOQR_OBJ_END_OF_GROUP;
    d.now_us = rig.now;
    R_CHECK(&rig, moqr_core_ingest(rig.core, tk, &d) == MOQR_OK);
    relay_bind_test_fetch_ing(&rig, tk, 3, 0, 0, 0x30);
    relay_bind_test_rig_pump(&rig, 2);

    moq_fetch_t fh;
    relay_bind_test_fetch_issue(&rig, sub, "v", 0, 0, 3, 1, &fh); /* {0,0} .. {3,0} */
    relay_bind_test_rig_pump(&rig, 20);
    peer_state_t ps;
    memset(&ps, 0, sizeof(ps));
    relay_bind_test_peer_drain(&rig, sub, &ps, false, false, 0, 0);
    R_CHECK(&rig, ps.fetch_ok && ps.fetch_error == 0);
    R_CHECK(&rig, ps.fok_end_group == 3 && ps.fok_end_object == 1);
    static const struct { uint64_t g, o; uint8_t fp; } objs[4] = {
        { 0, 0, 0x00 }, { 0, 2, 0x02 }, { 2, 0, 0x20 }, { 3, 0, 0x30 },
    };
    R_CHECK(&rig, ps.fdlv_n == 4);
    for (int i = 0; i < 4 && i < ps.fdlv_n; i++) {
        R_CHECK(&rig, ps.fdlv[i].group == objs[i].g &&
                          ps.fdlv[i].object == objs[i].o &&
                          ps.fdlv[i].fp == objs[i].fp);
    }
    R_CHECK(&rig, ps.fgap_n == 2);
    if (ps.fgap_n == 2) {
        R_CHECK(&rig, ps.fgap[0].kind == expect_kind &&
                          ps.fgap[0].group == 0 && ps.fgap[0].object == 1);
        R_CHECK(&rig, ps.fgap[1].kind == expect_kind &&
                          ps.fgap[1].group == 1 &&
                          ps.fgap[1].object == expect_max);
    }
    R_CHECK(&rig, ps.fetch_complete == 1 && !ps.session_closed);

    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (rig.failures == 0) {
        printf("PASS: fetch_unvouched_marked (v%d)\n", (int)version);
    }
    return rig.failures;
}

/* A draft-22 FETCH of demo/<name> in the given semantic form (`kind`
 * MOQ_LOCATION_FILTER_*, or UINT32_MAX for no filter at all) with an
 * optional FILL_TIMEOUT. */
static void
d22_fetch_loc(rig_t *r, conn_t *cn, const char *name, uint32_t kind,
              uint64_t rel, uint64_t sg, uint64_t so, uint64_t eg,
              bool has_ft, uint64_t ft_ms, moq_fetch_t *out)
{
    moq_bytes_t nsb[1] = { B("demo") };
    moq_location_filter_cfg_t loc;
    moq_location_filter_cfg_init(&loc);
    moq_fetch_cfg_t fc;
    moq_fetch_cfg_init_sized(&fc, sizeof(fc));
    fc.track_namespace = (moq_namespace_t){ .parts = nsb, .count = 1 };
    fc.track_name = B(name);
    if (kind == UINT32_MAX) {
        loc.kind = MOQ_LOCATION_FILTER_NONE;
    } else {
        loc.kind = kind;
        loc.relative_group_offset = rel;
        loc.start_group = sg;
        loc.start_object = so;
        loc.end_group = eg;
    }
    fc.location = &loc;
    fc.has_fill_timeout = has_ft;
    fc.fill_timeout_ms = ft_ms;
    R_CHECK(r, moq_session_fetch(cn->peer, &fc, r->now, out) == MOQ_OK);
}

/* One fetch's answer, from a clean state. */
static void
d22_fetch_result(rig_t *r, conn_t *cn, peer_state_t *ps, int cycles)
{
    relay_bind_test_rig_pump(r, cycles);
    memset(ps, 0, sizeof(*ps));
    relay_bind_test_peer_drain(r, cn, ps, false, false, 0, 0);
}

/* A draft-22 FETCH whose range is stated relative to the Largest Object is
 * resolved by the relay against its own log, even on a connection that never
 * carried the track: the session leaves it unresolved, the relay resolves it
 * with the plan, and the session holds the FETCH_OK End to the request's form.
 * Track demo/v holds groups 0..3, objects {g,0} and {g,1}, with END_OF_GROUP
 * at {g,2} for groups 0..2: Largest Object {3,1}. */
static int
draft22_fetch_relay_resolved(void)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != 0) {
        printf("FAIL: draft22_fetch_relay_resolved rig create\n");
        return 1;
    }
    conn_t *sub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_22);
    R_CHECK(&rig, sub != NULL);
    relay_bind_test_rig_pump(&rig, 4);
    moqr_binding_t pb = relay_bind_test_fetch_pub_open(&rig);
    moqr_track_t tk = relay_bind_test_fetch_pub_track(&rig, pb, "v");
    (void)relay_bind_test_fetch_pub_track(&rig, pb, "empty");
    for (uint64_t g = 0; g <= 3; g++) {
        relay_bind_test_fetch_ing(&rig, tk, g, 0, 0, (uint8_t)(0x10 * g));
        relay_bind_test_fetch_ing(&rig, tk, g, 0, 1, (uint8_t)(0x10 * g + 1));
        if (g < 3) {
            moqr_log_append_desc_t d;
            moqr_log_append_desc_init(&d);
            d.group_id = g;
            d.object_id = 2;
            d.publisher_priority = 1;
            d.status = MOQR_OBJ_END_OF_GROUP;
            d.now_us = rig.now;
            R_CHECK(&rig, moqr_core_ingest(rig.core, tk, &d) == MOQR_OK);
        }
    }
    relay_bind_test_rig_pump(&rig, 2);
    moq_fetch_t fh;
    peer_state_t ps;

    /* Two groups back from the next group: groups 2 and 3, through {3,1}. */
    d22_fetch_loc(&rig, sub, "v", MOQ_LOCATION_FILTER_RELATIVE_GROUP, 2, 0, 0, 0,
                  false, 0, &fh);
    d22_fetch_result(&rig, sub, &ps, 12);
    R_CHECK(&rig, ps.fetch_ok && ps.fetch_error == 0);
    R_CHECK(&rig, ps.fok_end_group == 3 && ps.fok_end_object == 2);
    R_CHECK(&rig, ps.fdlv_n == 4 && ps.fgap_n == 0 && ps.fetch_complete == 1);
    if (ps.fdlv_n == 4) {
        R_CHECK(&rig, ps.fdlv[0].group == 2 && ps.fdlv[0].object == 0 &&
                          ps.fdlv[0].fp == 0x20);
        R_CHECK(&rig, ps.fdlv[3].group == 3 && ps.fdlv[3].object == 1 &&
                          ps.fdlv[3].fp == 0x31);
    }

    /* No filter: the whole track through the Largest Object. */
    d22_fetch_loc(&rig, sub, "v", UINT32_MAX, 0, 0, 0, 0, false, 0, &fh);
    d22_fetch_result(&rig, sub, &ps, 12);
    R_CHECK(&rig, ps.fetch_ok && ps.fok_end_group == 3 && ps.fok_end_object == 2);
    R_CHECK(&rig, ps.fdlv_n == 8 && ps.fgap_n == 0 && ps.fetch_complete == 1);

    /* An absolute start with an open end. */
    d22_fetch_loc(&rig, sub, "v", MOQ_LOCATION_FILTER_ABSOLUTE_START, 0, 3, 1, 0,
                  false, 0, &fh);
    d22_fetch_result(&rig, sub, &ps, 12);
    R_CHECK(&rig, ps.fetch_ok && ps.fok_end_group == 3 && ps.fok_end_object == 2);
    R_CHECK(&rig, ps.fdlv_n == 1 && ps.fetch_complete == 1);

    /* A bounded end past the Largest Object stops at it. */
    d22_fetch_loc(&rig, sub, "v", MOQ_LOCATION_FILTER_GROUP_END, 0, 1, 0, 9,
                  false, 0, &fh);
    d22_fetch_result(&rig, sub, &ps, 12);
    R_CHECK(&rig, ps.fetch_ok && ps.fok_end_group == 3 && ps.fok_end_object == 2);
    R_CHECK(&rig, ps.fdlv_n == 6 && ps.fetch_complete == 1);

    /* Past the Largest Object (the next group, the Next Object, a start
     * after it), and a track with no Objects: INVALID_RANGE. An unknown
     * track: DOES_NOT_EXIST. */
    static const struct { uint32_t kind; uint64_t rel, sg, so; const char *n;
                          uint64_t code; } bad[] = {
        { MOQ_LOCATION_FILTER_RELATIVE_GROUP, 0, 0, 0, "v",
          MOQ_REQUEST_ERROR_INVALID_RANGE },
        { MOQ_LOCATION_FILTER_NEXT_OBJECT, 0, 0, 0, "v",
          MOQ_REQUEST_ERROR_INVALID_RANGE },
        { MOQ_LOCATION_FILTER_ABSOLUTE_START, 0, 3, 2, "v",
          MOQ_REQUEST_ERROR_INVALID_RANGE },
        { UINT32_MAX, 0, 0, 0, "empty", MOQ_REQUEST_ERROR_INVALID_RANGE },
        { UINT32_MAX, 0, 0, 0, "none", MOQ_REQUEST_ERROR_DOES_NOT_EXIST },
    };
    for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
        d22_fetch_loc(&rig, sub, bad[i].n, bad[i].kind, bad[i].rel, bad[i].sg,
                      bad[i].so, 0, false, 0, &fh);
        d22_fetch_result(&rig, sub, &ps, 8);
        R_CHECK(&rig, !ps.fetch_ok && ps.fetch_error == 1);
        R_CHECK(&rig, ps.fetch_error_code == bad[i].code);
    }
    R_CHECK(&rig, !ps.session_closed);
    moqr_bind_stats_t bs;
    moqr_bind_get_stats(rig.bind, &bs);
    R_CHECK(&rig, bs.session_errors == 0);

    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (rig.failures == 0) {
        printf("PASS: draft22_fetch_relay_resolved\n");
    }
    return rig.failures;
}

/* A draft-22 FETCH marks what it does not serve by its FILL_TIMEOUT: the
 * relay waits for nothing it does not hold, so with a FILL_TIMEOUT of 0 every
 * unavailable Location is Timed-Out (9.20.5), and with a budget left (none
 * given, or a long one) it is Unknown. The track is fetch_unvouched_marked's:
 * a hole at {0,1}, then group 0's tail and the unseen group 1. */
static int
draft22_fetch_fill_timeout_kinds(int mode)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != 0) {
        printf("FAIL: draft22_fetch_fill_timeout_kinds rig create\n");
        return 1;
    }
    conn_t *sub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_22);
    R_CHECK(&rig, sub != NULL);
    relay_bind_test_rig_pump(&rig, 4);
    moqr_binding_t pb = relay_bind_test_fetch_pub_open(&rig);
    moqr_track_t tk = relay_bind_test_fetch_pub_track(&rig, pb, "v");
    relay_bind_test_fetch_ing(&rig, tk, 0, 0, 0, 0x00);
    relay_bind_test_fetch_ing(&rig, tk, 0, 0, 2, 0x02);
    relay_bind_test_fetch_ing(&rig, tk, 2, 0, 0, 0x20);
    relay_bind_test_rig_pump(&rig, 2);
    moq_fetch_t fh;
    d22_fetch_loc(&rig, sub, "v", MOQ_LOCATION_FILTER_ABSOLUTE_START, 0, 0, 0, 0,
                  mode != 0, mode == 1 ? 0 : 60000, &fh);
    peer_state_t ps;
    d22_fetch_result(&rig, sub, &ps, 20);
    R_CHECK(&rig, ps.fetch_ok && ps.fok_end_group == 2 && ps.fok_end_object == 1);
    R_CHECK(&rig, ps.fdlv_n == 3 && ps.fetch_complete == 1);
    const uint32_t kind = mode == 1 ? MOQ_FETCH_RANGE_TIMED_OUT
                                    : MOQ_FETCH_RANGE_UNKNOWN;
    R_CHECK(&rig, ps.fgap_n == 2);
    if (ps.fgap_n == 2) {
        R_CHECK(&rig, ps.fgap[0].kind == kind && ps.fgap[0].group == 0 &&
                          ps.fgap[0].object == 1);
        R_CHECK(&rig, ps.fgap[1].kind == kind && ps.fgap[1].group == 1 &&
                          ps.fgap[1].object == UINT64_MAX);
    }
    R_CHECK(&rig, !ps.session_closed);

    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (rig.failures == 0) {
        printf("PASS: draft22_fetch_fill_timeout_kinds(%s)\n",
               mode == 0 ? "absent" : mode == 1 ? "zero" : "long");
    }
    return rig.failures;
}

/* Open an Object at {g, 0, o} on the relay's log with its first chunk. */
static void
fetch_ing_open(rig_t *r, moqr_track_t tk, uint64_t g, uint64_t o)
{
    moqr_log_append_desc_t d;
    moqr_log_append_desc_init(&d);
    d.group_id = g;
    d.subgroup_id = 0;
    d.object_id = o;
    d.publisher_priority = 1;
    d.obj_state = MOQR_OBJ_OPEN;
    d.declared_len = 16;
    d.now_us = r->now;
    R_CHECK(r, moqr_core_ingest(r->core, tk, &d) == MOQR_OK);
    uint8_t body[8];
    memset(body, 0xE0, sizeof(body));
    moq_rcbuf_t *ch = NULL;
    R_CHECK(r, moq_rcbuf_create(&r->alloc->vt, body, sizeof(body), &ch) == 0);
    R_CHECK(r, moqr_core_append_chunk(r->core, tk, g, 0, o, ch) == MOQR_OK);
    moq_rcbuf_decref(ch);
}

/* The Largest Object can be an Object still arriving (3.1.4), and a FETCH
 * whose range includes it includes it in full, the rest delivered as it
 * becomes available (3.2). The relay's Largest names it from its first
 * byte: the FETCH_OK End covers it, the FETCH serves what precedes it and
 * then waits for it (the binding reports the wait's deadline), and serves it
 * whole once it completes. At the deadline it gives up on it instead,
 * marking its Location Timed-Out on draft-22 and Unknown on draft-18, whose
 * fetch stream has no Timed-Out marker; a FILL_TIMEOUT of 0 gives up at
 * once. Track demo/v: {0,0} whole, {0,1} in progress. */
enum { FIP_COMPLETES, FIP_DEADLINE, FIP_TIMEOUT_ZERO };
static int
fetch_in_progress(moq_version_t version, int mode)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != 0) {
        printf("FAIL: fetch_in_progress rig create\n");
        return 1;
    }
    R_CHECK(&rig, moqr_bind_set_fill_timeouts(rig.bind, 20, 0) == MOQR_OK);
    conn_t *sub = relay_bind_test_rig_connect(&rig, version);
    R_CHECK(&rig, sub != NULL);
    relay_bind_test_rig_pump(&rig, 4);
    moqr_binding_t pb = relay_bind_test_fetch_pub_open(&rig);
    moqr_track_t tk = relay_bind_test_fetch_pub_track(&rig, pb, "v");
    relay_bind_test_fetch_ing(&rig, tk, 0, 0, 0, 0x00);
    fetch_ing_open(&rig, tk, 0, 1);
    bool has = false;
    uint64_t lg = 0, lo = 0;
    R_CHECK(&rig, moqr_core_track_largest(rig.core, tk, 1, &has, &lg, &lo) ==
                      MOQR_OK);
    R_CHECK(&rig, has && lg == 0 && lo == 1);
    relay_bind_test_rig_pump(&rig, 2);

    moq_fetch_t fh;
    if (version == MOQ_VERSION_DRAFT_22) {
        d22_fetch_loc(&rig, sub, "v", UINT32_MAX, 0, 0, 0, 0,
                      mode == FIP_TIMEOUT_ZERO, 0, &fh);
    } else {
        relay_bind_test_fetch_issue(&rig, sub, "v", 0, 0, 1, 0, &fh); /* through group 0 */
    }
    peer_state_t ps;
    d22_fetch_result(&rig, sub, &ps, 6);
    R_CHECK(&rig, ps.fetch_ok && ps.fok_end_group == 0 && ps.fok_end_object == 2);
    R_CHECK(&rig, ps.fdlv_n >= 1 && ps.fdlv[0].object == 0);
    const uint32_t gave_up = version == MOQ_VERSION_DRAFT_22
                                 ? MOQ_FETCH_RANGE_TIMED_OUT
                                 : MOQ_FETCH_RANGE_UNKNOWN;
    if (mode == FIP_TIMEOUT_ZERO) {
        R_CHECK(&rig, ps.fdlv_n == 1 && ps.fgap_n == 1 && ps.fetch_complete == 1);
        R_CHECK(&rig, ps.fgap_n == 1 && ps.fgap[0].kind == gave_up &&
                          ps.fgap[0].group == 0 && ps.fgap[0].object == 1);
    } else {
        /* Waiting: nothing more yet, and a deadline to wake for. */
        R_CHECK(&rig, ps.fdlv_n == 1 && ps.fgap_n == 0 && ps.fetch_complete == 0);
        uint64_t deadline = moqr_bind_next_deadline_us(rig.bind);
        R_CHECK(&rig, deadline != UINT64_MAX && deadline > rig.now);
        if (mode == FIP_COMPLETES) {
            uint8_t body[8];
            memset(body, 0xE1, sizeof(body));
            moq_rcbuf_t *ch = NULL;
            R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &ch) == 0);
            R_CHECK(&rig, moqr_core_append_chunk(rig.core, tk, 0, 0, 1, ch) ==
                              MOQR_OK);
            moq_rcbuf_decref(ch);
            R_CHECK(&rig, moqr_core_complete_record(rig.core, tk, 0, 0, 1) ==
                              MOQR_OK);
            relay_bind_test_rig_pump(&rig, 4);
            relay_bind_test_peer_drain(&rig, sub, &ps, false, false, 0, 0);
            R_CHECK(&rig, ps.fdlv_n == 2 && ps.fgap_n == 0 &&
                              ps.fetch_complete == 1);
            R_CHECK(&rig, ps.fdlv_n == 2 && ps.fdlv[1].object == 1 &&
                              ps.fdlv[1].len == 16 && ps.fdlv[1].fp == 0xE0);
        } else {
            relay_bind_test_rig_pump(&rig, 30);
            relay_bind_test_peer_drain(&rig, sub, &ps, false, false, 0, 0);
            R_CHECK(&rig, rig.now >= deadline);
            R_CHECK(&rig, ps.fdlv_n == 1 && ps.fgap_n == 1 &&
                              ps.fetch_complete == 1);
            R_CHECK(&rig, ps.fgap_n == 1 && ps.fgap[0].kind == gave_up &&
                              ps.fgap[0].group == 0 && ps.fgap[0].object == 1);
        }
    }
    R_CHECK(&rig, moqr_bind_next_deadline_us(rig.bind) == UINT64_MAX);
    R_CHECK(&rig, !ps.session_closed);
    moqr_bind_stats_t bs;
    moqr_bind_get_stats(rig.bind, &bs);
    R_CHECK(&rig, bs.session_errors == 0);

    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (rig.failures == 0) {
        printf("PASS: fetch_in_progress (v%d, %s)\n", (int)version,
               mode == FIP_COMPLETES ? "completes"
               : mode == FIP_DEADLINE ? "deadline" : "timeout zero");
    }
    return rig.failures;
}

/* A fill bounded by the Largest Object can end at an Object still arriving:
 * the fill serves what precedes it, waits for it as for any Object the relay
 * does not hold yet, and serves it whole once it completes. Track demo/v:
 * {0,0} whole, {0,1} in progress. */
static int
draft22_fill_in_progress(void)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != 0) {
        printf("FAIL: draft22_fill_in_progress rig create\n");
        return 1;
    }
    conn_t *sub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_22);
    R_CHECK(&rig, sub != NULL);
    relay_bind_test_rig_pump(&rig, 4);
    moqr_binding_t pb = relay_bind_test_fetch_pub_open(&rig);
    moqr_track_t tk = relay_bind_test_fetch_pub_track(&rig, pb, "v");
    relay_bind_test_fetch_ing(&rig, tk, 0, 0, 0, 0x00);
    fetch_ing_open(&rig, tk, 0, 1);
    relay_bind_test_rig_pump(&rig, 2);

    moq_bytes_t nsb[1] = { B("demo") };
    moq_location_filter_cfg_t loc, floc;
    moq_location_filter_cfg_init(&loc);
    loc.kind = MOQ_LOCATION_FILTER_NEXT_OBJECT;
    moq_location_filter_cfg_init(&floc);
    floc.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.location = &floc;
    fill.has_fill_timeout = true;
    fill.fill_timeout_ms = 500;
    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = nsb, .count = 1 };
    scfg.track_name = B("v");
    scfg.location = &loc;
    scfg.fill = &fill;
    moq_subscription_t h;
    R_CHECK(&rig, moq_session_subscribe(sub->peer, &scfg, rig.now, &h) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 8);
    peer_state_t s_ps;
    memset(&s_ps, 0, sizeof(s_ps));
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.subscribe_ok);
    R_CHECK(&rig, s_ps.fill_objects == 1 && s_ps.fill_gaps == 0 &&
                      s_ps.fill_terminals == 0);

    uint8_t body[8];
    memset(body, 0xE1, sizeof(body));
    moq_rcbuf_t *ch = NULL;
    R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &ch) == 0);
    R_CHECK(&rig, moqr_core_append_chunk(rig.core, tk, 0, 0, 1, ch) == MOQR_OK);
    moq_rcbuf_decref(ch);
    R_CHECK(&rig, moqr_core_complete_record(rig.core, tk, 0, 0, 1) == MOQR_OK);
    relay_bind_test_rig_pump(&rig, 6);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.fill_objects == 2 && s_ps.fill_gaps == 0);
    R_CHECK(&rig, s_ps.fill_terminals == 1 &&
                      s_ps.fill_term_reason == MOQ_FILL_COMPLETE);
    R_CHECK(&rig, !s_ps.done_seen && !s_ps.session_closed);

    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (rig.failures == 0) {
        printf("PASS: draft22_fill_in_progress\n");
    }
    return rig.failures;
}

/* A retained object carrying datagram preference, fetched per draft. The
 * d18 fetch header can encode the bit (expect_dg=true); the d16 header cannot,
 * so the relay clears it and serves the object normally (expect_dg=false) WITHOUT
 * a session close. Identity/payload are the same either way. */
static int
fetch_datagram_pref(moq_version_t version, bool expect_dg)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != 0) {
        printf("FAIL: dg-pref rig create (v%d)\n", (int)version);
        return 1;
    }
    conn_t *sub = relay_bind_test_rig_connect(&rig, version);
    R_CHECK(&rig, sub != NULL);
    relay_bind_test_rig_pump(&rig, 4);
    moqr_binding_t pb = relay_bind_test_fetch_pub_open(&rig);
    moqr_track_t tk = relay_bind_test_fetch_pub_track(&rig, pb, "vg");
    fetch_ing_dg(&rig, tk, 7, 0, 0, 0xD7);   /* a datagram-preference object */
    relay_bind_test_rig_pump(&rig, 2);
    moq_fetch_t fh;
    relay_bind_test_fetch_issue(&rig, sub, "vg", 7, 0, 7, 1, &fh); /* [7:0 .. 7:0] */
    relay_bind_test_rig_pump(&rig, 10);
    peer_state_t ps;
    memset(&ps, 0, sizeof(ps));
    relay_bind_test_peer_drain(&rig, sub, &ps, false, false, 0, 0);

    R_CHECK(&rig, !ps.session_closed);       /* d16 must NOT hard-close */
    R_CHECK(&rig, ps.fetch_ok);
    R_CHECK(&rig, ps.fetch_error == 0);
    R_CHECK(&rig, ps.fdlv_n == 1);
    R_CHECK(&rig, ps.fdlv[0].group == 7 && ps.fdlv[0].object == 0);
    R_CHECK(&rig, ps.fdlv[0].fp == 0xD7);
    R_CHECK(&rig, ps.fdlv[0].datagram == expect_dg);
    R_CHECK(&rig, ps.fetch_complete == 1);

    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (rig.failures == 0) {
        printf("PASS: fetch_datagram_pref_%s\n",
               version == MOQ_VERSION_DRAFT_18 ? "d18" : "d16_clear");
    }
    return rig.failures;
}

/* Cross-draft parity. The SAME retained matrix (mixed datagram preference)
 * fetched by a d16 and a d18 fetcher delivers identical group/object/payload in
 * identical order; the only difference is the datagram bit -- preserved on the
 * pref object for d18, cleared for d16. */
static int
fetch_datagram_parity(void)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != 0) {
        printf("FAIL: dg-parity rig create\n");
        return 1;
    }
    conn_t *s16 = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_16);
    conn_t *s18 = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, s16 != NULL && s18 != NULL);
    relay_bind_test_rig_pump(&rig, 4);
    moqr_binding_t pb = relay_bind_test_fetch_pub_open(&rig);
    moqr_track_t tk = relay_bind_test_fetch_pub_track(&rig, pb, "vp");
    relay_bind_test_fetch_ing(&rig, tk, 8, 0, 0, 0x80); /* pref = false */
    fetch_ing_dg(&rig, tk, 8, 0, 1, 0x81);     /* pref = true  */
    relay_bind_test_fetch_ing(&rig, tk, 8, 0, 2, 0x82); /* pref = false */
    relay_bind_test_rig_pump(&rig, 2);
    moq_fetch_t f16, f18;
    relay_bind_test_fetch_issue(&rig, s16, "vp", 8, 0, 8, 0, &f16); /* whole group 8 */
    relay_bind_test_fetch_issue(&rig, s18, "vp", 8, 0, 8, 0, &f18);
    relay_bind_test_rig_pump(&rig, 14);
    peer_state_t p16, p18;
    memset(&p16, 0, sizeof(p16));
    memset(&p18, 0, sizeof(p18));
    relay_bind_test_peer_drain(&rig, s16, &p16, false, false, 0, 0);
    relay_bind_test_peer_drain(&rig, s18, &p18, false, false, 0, 0);

    R_CHECK(&rig, !p16.session_closed && !p18.session_closed);
    R_CHECK(&rig, p16.fetch_complete == 1 && p18.fetch_complete == 1);
    R_CHECK(&rig, p16.fdlv_n == 3 && p18.fdlv_n == 3);
    for (int i = 0; i < 3 && i < p16.fdlv_n && i < p18.fdlv_n; i++) {
        /* Identity + order + payload identical across drafts. */
        R_CHECK(&rig, p16.fdlv[i].group == 8 && p18.fdlv[i].group == 8);
        R_CHECK(&rig, p16.fdlv[i].object == (uint64_t)i);
        R_CHECK(&rig, p18.fdlv[i].object == (uint64_t)i);
        R_CHECK(&rig, p16.fdlv[i].fp == (uint8_t)(0x80 + i));
        R_CHECK(&rig, p18.fdlv[i].fp == (uint8_t)(0x80 + i));
    }
    /* Only the datagram bit differs: d18 preserves it on the pref object (1);
     * d16 clears all three. */
    R_CHECK(&rig, !p16.fdlv[0].datagram && !p16.fdlv[1].datagram &&
                      !p16.fdlv[2].datagram);
    R_CHECK(&rig, !p18.fdlv[0].datagram && p18.fdlv[1].datagram &&
                      !p18.fdlv[2].datagram);

    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (rig.failures == 0) {
        printf("PASS: fetch_datagram_parity\n");
    }
    return rig.failures;
}

/* WOULD_BLOCK backpressure: a tiny session action queue forces the fetch writes
 * to back up; every object is still delivered exactly once, in order, across
 * pumps (the peek-holds-on-WOULD_BLOCK invariant end-to-end). */
static int
fetch_backpressure(void)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    relay_bind_test_max_actions = 2; /* constrain both SimPair sessions' queues */
    rig_t rig;
    int cr = relay_bind_test_rig_create(&rig, &a);
    if (cr != 0) {
        relay_bind_test_max_actions = 0;
        printf("FAIL: fetch bp rig create\n");
        return 1;
    }
    conn_t *sub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18); /* reads the knob */
    relay_bind_test_max_actions = 0; /* restore for later tests */
    R_CHECK(&rig, sub != NULL);
    relay_bind_test_rig_pump(&rig, 8);
    moqr_binding_t pb = relay_bind_test_fetch_pub_open(&rig);
    moqr_track_t tk = relay_bind_test_fetch_pub_track(&rig, pb, "vb");
    for (uint64_t o = 0; o < 8; o++) {
        relay_bind_test_fetch_ing(&rig, tk, 0, 0, o, (uint8_t)(0x20 + o));
    }
    relay_bind_test_rig_pump(&rig, 4);
    moq_fetch_t fh;
    relay_bind_test_fetch_issue(&rig, sub, "vb", 0, 0, 0, 8, &fh);
    relay_bind_test_rig_pump(&rig, 60); /* plenty of pumps for the backed-up writes to drain */
    peer_state_t ps;
    memset(&ps, 0, sizeof(ps));
    relay_bind_test_peer_drain(&rig, sub, &ps, false, false, 0, 0);
    R_CHECK(&rig, ps.fetch_ok && ps.fetch_complete == 1);
    R_CHECK(&rig, ps.fdlv_n == 8);   /* exactly once each */
    for (int i = 0; i < 8 && i < ps.fdlv_n; i++) {
        R_CHECK(&rig, ps.fdlv[i].object == (uint64_t)i);          /* in order */
        R_CHECK(&rig, ps.fdlv[i].fp == (uint8_t)(0x20 + i));      /* payload   */
    }
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (rig.failures == 0) {
        printf("PASS: fetch_backpressure\n");
    }
    return rig.failures;
}

/* No stale state after close: tear the rig down with a fetch in flight (a tiny
 * action queue guarantees a pinned object is still held) -> conn_detach ->
 * binding_close retires the core fetch (pins released), the bind table frees.
 * No crash, allocator balance clean. */
static int
fetch_close_midstream(void)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    relay_bind_test_max_actions = 1;
    rig_t rig;
    int cr = relay_bind_test_rig_create(&rig, &a);
    if (cr != 0) {
        relay_bind_test_max_actions = 0;
        printf("FAIL: fetch close rig create\n");
        return 1;
    }
    conn_t *sub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18); /* reads the knob */
    relay_bind_test_max_actions = 0;
    R_CHECK(&rig, sub != NULL);
    relay_bind_test_rig_pump(&rig, 8);
    moqr_binding_t pb = relay_bind_test_fetch_pub_open(&rig);
    moqr_track_t tk = relay_bind_test_fetch_pub_track(&rig, pb, "vc");
    for (uint64_t o = 0; o < 8; o++) {
        relay_bind_test_fetch_ing(&rig, tk, 0, 0, o, (uint8_t)(0x30 + o));
    }
    relay_bind_test_rig_pump(&rig, 4);
    moq_fetch_t fh;
    relay_bind_test_fetch_issue(&rig, sub, "vc", 0, 0, 0, 8, &fh);
    relay_bind_test_rig_pump(&rig, 3); /* accept + a few objects, then still in flight */
    relay_bind_test_rig_destroy(&rig); /* teardown mid-fetch */
    R_CHECK(&rig, a.live == 0);
    if (rig.failures == 0) {
        printf("PASS: fetch_close_midstream\n");
    }
    return rig.failures;
}

/* Terminal liveness: an ACCEPTED fetch that hits an unrecoverable write error
 * must give the peer a wire terminal (a hard session close), never a silent
 * hang or a false FETCH_COMPLETE. Forced deterministically with a
 * accepted fetch whose retained range is evicted (capacity eviction from new
 * ingest on the active track) before it drains can no longer be served fully:
 * peek returns TOO_OLD and the binding hard-closes the session (INTERNAL_ERROR)
 * rather than FIN a truncated range or hang. A tiny action queue holds the fetch
 * mid-stream so the eviction lands while it is still open. (Earlier this was
 * driven by the d16 datagram-preference INVAL; now that case is clear-and-
 * serve, so mid-fetch eviction is now the post-admission terminal under test.) */
static int
fetch_error_terminates(void)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    relay_bind_test_log_max_groups = 2; /* retain 2 groups -> new ingest evicts fast */
    relay_bind_test_max_actions = 3; /* FETCH_OK flushes, but the drain backs up */
    rig_t rig;
    int cr = relay_bind_test_rig_create(&rig, &a);
    if (cr != 0) {
        relay_bind_test_log_max_groups = 0;
        relay_bind_test_max_actions = 0;
        printf("FAIL: fetch terminate rig create\n");
        return 1;
    }
    conn_t *sub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18); /* reads the knobs */
    relay_bind_test_max_actions = 0;
    relay_bind_test_log_max_groups = 0;
    R_CHECK(&rig, sub != NULL);
    relay_bind_test_rig_pump(&rig, 8);
    moqr_binding_t pb = relay_bind_test_fetch_pub_open(&rig);
    moqr_track_t tk = relay_bind_test_fetch_pub_track(&rig, pb, "vd");
    for (uint64_t o = 0; o < 24; o++) {               /* too big to drain in a few */
        relay_bind_test_fetch_ing(&rig, tk, 5, 0, o, (uint8_t)(0x50 + o)); /* pumps before evict  */
    }
    relay_bind_test_rig_pump(&rig, 4);
    moq_fetch_t fh;
    relay_bind_test_fetch_issue(&rig, sub, "vd", 5, 0, 5, 0, &fh); /* whole group 5 */
    relay_bind_test_rig_pump(&rig, 3); /* fetch_open + accept + a few objects; then it backs up */
    /* New groups evict group 5 (max_groups=2) underneath the open fetch. */
    relay_bind_test_fetch_ing(&rig, tk, 6, 0, 0, 0x60);
    relay_bind_test_fetch_ing(&rig, tk, 7, 0, 0, 0x70);
    relay_bind_test_rig_pump(&rig, 20); /* peek now sees cur_group(5) < oldest(6) -> TOO_OLD */
    peer_state_t ps;
    memset(&ps, 0, sizeof(ps));
    relay_bind_test_peer_drain(&rig, sub, &ps, false, false, 0, 0);
    /* Terminal: session close (INTERNAL_ERROR), never a truncated FETCH_COMPLETE
     * or a false FETCH_ERROR over a range that couldn't be fully served. (The
     * hard close preempts the buffered FETCH_OK/objects in this sequence, which
     * is fine: the session close is itself the terminal.) */
    R_CHECK(&rig, ps.session_closed);
    R_CHECK(&rig, ps.close_code == 0x1u);      /* session INTERNAL_ERROR        */
    R_CHECK(&rig, ps.fetch_complete == 0);
    R_CHECK(&rig, ps.fetch_error == 0);
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (rig.failures == 0) {
        printf("PASS: fetch_error_terminates\n");
    }
    return rig.failures;
}

/* ---- Retained FETCH representation parity ---------------------------------- *
 * With production streaming ingest on, a warmed NORMAL object is
 * stored as a chunked COMPLETE record (obj_state COMPLETE, chunk_count > 0,
 * payload NULL, bytes in the log chunk list). Retained standalone FETCH must
 * serve that representation exactly like a whole-object record. This oracle warms
 * the SAME object both ways — flipping only relay_bind_test_server_streaming_objects, with
 * identical streamed publish code — and proves an identical FETCH result (End
 * Location, object identity/order, payload length + bytes, completion, no false
 * error/close). The failure this pins is a chunked run yielding FETCH_OK + zero
 * objects + FETCH_COMPLETE: parity depends on fetch_scan_next serving records
 * whose payload lives in the chunk list (chunk_count > 0), not skipping them. */

typedef struct {
    bool     fetch_ok, fok_eot, session_closed;
    uint64_t fok_end_group, fok_end_object;
    int      fetch_complete, fetch_error, n;
    uint64_t fetch_error_code;
    struct {
        uint64_t group, subgroup, object, len;
        uint32_t fp_all;
        bool     datagram;
    } obj[16];
} fetch_res_t;

static int
fetch_res_eq(const fetch_res_t *x, const fetch_res_t *y)
{
    if (x->fetch_ok != y->fetch_ok || x->fok_eot != y->fok_eot ||
        x->fok_end_group != y->fok_end_group ||
        x->fok_end_object != y->fok_end_object ||
        x->fetch_complete != y->fetch_complete ||
        x->fetch_error != y->fetch_error ||
        x->fetch_error_code != y->fetch_error_code ||
        x->session_closed != y->session_closed || x->n != y->n) {
        return 0;
    }
    for (int i = 0; i < x->n && i < 16; i++) {
        if (x->obj[i].group != y->obj[i].group ||
            x->obj[i].subgroup != y->obj[i].subgroup ||
            x->obj[i].object != y->obj[i].object ||
            x->obj[i].len != y->obj[i].len ||
            x->obj[i].fp_all != y->obj[i].fp_all ||
            x->obj[i].datagram != y->obj[i].datagram) {
            return 0;
        }
    }
    return 1;
}

/* Warm one track over the production SimPair binding and capture a standalone
 * FETCH's wire result. `streaming` selects whole-object (false) vs chunked
 * COMPLETE (true) storage; `max_actions` (0 = default) constrains ONLY the
 * fetcher's session, driving fetch-egress WOULD_BLOCK so a coalesced object is
 * held + replayed across pumps without drop/dup. */
static int
retained_fetch_parity_run(moq_version_t version, bool streaming,
                          uint32_t max_actions, fetch_res_t *out)
{
    memset(out, 0, sizeof(*out));
    ca_t a;
    relay_bind_test_ca_init(&a);
    relay_bind_test_server_streaming_objects = streaming;
    relay_bind_test_client_streaming_objects = streaming;
    rig_t rig;
    int cr = relay_bind_test_rig_create(&rig, &a);
    if (cr != 0) {
        relay_bind_test_server_streaming_objects = false;
        relay_bind_test_client_streaming_objects = false;
        printf("FAIL: parity rig create (stream=%d v%d)\n", (int)streaming,
               (int)version);
        return 1;
    }
    /* Only the publisher's relay-side session ingests, so streaming is scoped to
     * its connect; the warmer and fetcher are plain sessions. */
    conn_t *pub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    relay_bind_test_server_streaming_objects = false;
    relay_bind_test_client_streaming_objects = false;
    conn_t *warmer = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    relay_bind_test_max_actions = max_actions; /* constrain only the fetcher */
    conn_t *fetcher = relay_bind_test_rig_connect(&rig, version);
    relay_bind_test_max_actions = 0;
    R_CHECK(&rig, pub != NULL && warmer != NULL && fetcher != NULL);
    relay_bind_test_rig_pump(&rig, 4);

    moq_bytes_t nsp[1] = { B("demo") };
    moq_publish_namespace_cfg_t pcfg;
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 1 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg, rig.now,
                                                &ann) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 4);

    /* Warmer subscribes so the relay opens an upstream SUBSCRIBE to pull the
     * publisher's objects into the retained log. */
    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 1 };
    scfg.track_name = B("v");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    moq_subscription_t s1;
    R_CHECK(&rig, moq_session_subscribe(warmer->peer, &scfg, rig.now, &s1) ==
                      MOQ_OK);
    relay_bind_test_rig_pump(&rig, 6);
    peer_state_t pub_ps, warm_ps, fetch_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&warm_ps, 0, sizeof(warm_ps));
    memset(&fetch_ps, 0, sizeof(fetch_ps));
    relay_bind_test_peer_drain(&rig, pub, &pub_ps, true, false, 0, 0); /* accept upstream */
    R_CHECK(&rig, pub_ps.up_seen);
    relay_bind_test_rig_pump(&rig, 4);
    relay_bind_test_peer_drain(&rig, warmer, &warm_ps, false, false, 0, 0);

    /* Same streamed publish for BOTH representations: group 0 / subgroup 0 holds
     * obj0 (one 32-byte chunk) and obj1 (three DISTINCT 32-byte chunks, so a drop
     * or reorder changes the coalesced length/fingerprint). */
    moq_subgroup_cfg_t sca;
    moq_subgroup_cfg_init(&sca);
    sca.group_id = 0;
    sca.subgroup_id = 0;
    moq_subgroup_handle_t sg;
    R_CHECK(&rig, moq_session_open_subgroup(pub->peer, pub_ps.up_sub, &sca,
                                            rig.now, &sg) == MOQ_OK);
    R_CHECK(&rig, moq_session_begin_object(pub->peer, sg, 0, 32, rig.now) ==
                      MOQ_OK);
    relay_bind_test_parity_write_chunk(&rig, pub, sg, 0xA0, 32);
    R_CHECK(&rig, moq_session_end_object(pub->peer, sg, rig.now) == MOQ_OK);
    R_CHECK(&rig, moq_session_begin_object(pub->peer, sg, 1, 96, rig.now) ==
                      MOQ_OK);
    relay_bind_test_parity_write_chunk(&rig, pub, sg, 0xB1, 32);
    relay_bind_test_parity_write_chunk(&rig, pub, sg, 0xB2, 32);
    relay_bind_test_parity_write_chunk(&rig, pub, sg, 0xB3, 32);
    R_CHECK(&rig, moq_session_end_object(pub->peer, sg, rig.now) == MOQ_OK);
    R_CHECK(&rig, moq_session_close_subgroup(pub->peer, sg, rig.now) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 8);
    relay_bind_test_peer_drain(&rig, warmer, &warm_ps, false, false, 0, 0);

    /* Fetcher issues an identical whole-group-0 standalone FETCH. */
    moq_fetch_t fh;
    relay_bind_test_fetch_issue(&rig, fetcher, "v", 0, 0, 0, 0, &fh);
    for (int p = 0; p < 32; p++) {
        relay_bind_test_rig_pump(&rig, 2);
        relay_bind_test_peer_drain(&rig, warmer, &warm_ps, false, false, 0, 0);
        relay_bind_test_peer_drain(&rig, fetcher, &fetch_ps, false, false, 0, 0);
        if (fetch_ps.fetch_complete || fetch_ps.fetch_error ||
            fetch_ps.session_closed) {
            break;
        }
    }

    out->fetch_ok = fetch_ps.fetch_ok;
    out->fok_eot = fetch_ps.fok_eot;
    out->fok_end_group = fetch_ps.fok_end_group;
    out->fok_end_object = fetch_ps.fok_end_object;
    out->fetch_complete = fetch_ps.fetch_complete;
    out->fetch_error = fetch_ps.fetch_error;
    out->fetch_error_code = fetch_ps.fetch_error_code;
    out->session_closed = fetch_ps.session_closed;
    out->n = fetch_ps.fdlv_n;
    for (int i = 0; i < fetch_ps.fdlv_n && i < 16; i++) {
        out->obj[i].group = fetch_ps.fdlv[i].group;
        out->obj[i].subgroup = fetch_ps.fdlv[i].subgroup;
        out->obj[i].object = fetch_ps.fdlv[i].object;
        out->obj[i].len = fetch_ps.fdlv[i].len;
        out->obj[i].fp_all = fetch_ps.fdlv[i].fp_all;
        out->obj[i].datagram = fetch_ps.fdlv[i].datagram;
    }

    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    return rig.failures;
}

static int
retained_fetch_representation_parity_oracle(moq_version_t version,
                                            uint32_t max_actions)
{
    fetch_res_t whole, chunked;
    int f = 0;
    f += retained_fetch_parity_run(version, false, max_actions, &whole);
    f += retained_fetch_parity_run(version, true, max_actions, &chunked);

    int bad = 0;
    /* Absolute sanity on BOTH representations. */
    if (!whole.fetch_ok || !chunked.fetch_ok) bad = 1;
    if (whole.n != 2 || chunked.n != 2) bad = 1;
    if (whole.fetch_complete != 1 || chunked.fetch_complete != 1) bad = 1;
    if (whole.fetch_error != 0 || chunked.fetch_error != 0) bad = 1;
    if (whole.session_closed || chunked.session_closed) bad = 1;
    /* The multi-chunk object carries all 96 bytes in BOTH representations. */
    if (whole.n == 2 && (whole.obj[1].len != 96 || chunked.obj[1].len != 96)) {
        bad = 1;
    }
    /* Representation parity: byte-identical wire result. */
    if (!fetch_res_eq(&whole, &chunked)) bad = 1;
    f += bad;

    if (f == 0) {
        printf("PASS: retained_fetch_representation_parity_oracle(v%d%s)\n",
               (int)version, max_actions ? ", aq" : "");
    } else {
        printf("FAIL: retained_fetch_representation_parity_oracle(v%d%s) "
               "whole{ok=%d n=%d comp=%d err=%d o1len=%llu} "
               "chunked{ok=%d n=%d comp=%d err=%d o1len=%llu}\n",
               (int)version, max_actions ? ", aq" : "",
               (int)whole.fetch_ok, whole.n, whole.fetch_complete,
               whole.fetch_error,
               (unsigned long long)(whole.n == 2 ? whole.obj[1].len : 0),
               (int)chunked.fetch_ok, chunked.n, chunked.fetch_complete,
               chunked.fetch_error,
               (unsigned long long)(chunked.n == 2 ? chunked.obj[1].len : 0));
    }
    return f;
}

/* Binding-level fail-closed under OOM. A chunked COMPLETE track is
 * warmed; a fetcher's standalone FETCH is admitted (FETCH_OK), then the coalesce
 * buffer allocation is forced to fail (a distinctively sized alloc — 3x111=333 B).
 * The relay must terminate the fetcher VISIBLY (session close via
 * bind_fetch_terminate) and NEVER emit a false FETCH_COMPLETE or a truncated
 * object; teardown must leave the allocator balanced (no leaked coalesce buffer). */
static int
chunked_fetch_oom_terminates(void)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    relay_bind_test_server_streaming_objects = true;
    relay_bind_test_client_streaming_objects = true;
    rig_t rig;
    int cr = relay_bind_test_rig_create(&rig, &a);
    if (cr != 0) {
        relay_bind_test_server_streaming_objects = false;
        relay_bind_test_client_streaming_objects = false;
        printf("FAIL: chunked_fetch_oom rig create\n");
        return 1;
    }
    conn_t *pub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    relay_bind_test_server_streaming_objects = false;
    relay_bind_test_client_streaming_objects = false;
    conn_t *warmer = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *fetcher = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub != NULL && warmer != NULL && fetcher != NULL);
    relay_bind_test_rig_pump(&rig, 4);

    moq_bytes_t nsp[1] = { B("demo") };
    moq_publish_namespace_cfg_t pcfg;
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 1 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg, rig.now,
                                                &ann) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 4);
    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 1 };
    scfg.track_name = B("v");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    moq_subscription_t s1;
    R_CHECK(&rig, moq_session_subscribe(warmer->peer, &scfg, rig.now, &s1) ==
                      MOQ_OK);
    relay_bind_test_rig_pump(&rig, 6);
    peer_state_t pub_ps, warm_ps, fetch_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&warm_ps, 0, sizeof(warm_ps));
    memset(&fetch_ps, 0, sizeof(fetch_ps));
    relay_bind_test_peer_drain(&rig, pub, &pub_ps, true, false, 0, 0);
    R_CHECK(&rig, pub_ps.up_seen);
    relay_bind_test_rig_pump(&rig, 4);
    relay_bind_test_peer_drain(&rig, warmer, &warm_ps, false, false, 0, 0);

    /* One chunked object, distinctively sized (3 x 111 = 333 bytes) so the
     * coalesce buffer alloc is unambiguous to target by size. */
    moq_subgroup_cfg_t sca;
    moq_subgroup_cfg_init(&sca);
    sca.group_id = 0;
    sca.subgroup_id = 0;
    moq_subgroup_handle_t sg;
    R_CHECK(&rig, moq_session_open_subgroup(pub->peer, pub_ps.up_sub, &sca,
                                            rig.now, &sg) == MOQ_OK);
    R_CHECK(&rig, moq_session_begin_object(pub->peer, sg, 0, 333, rig.now) ==
                      MOQ_OK);
    relay_bind_test_parity_write_chunk(&rig, pub, sg, 0xC1, 111);
    relay_bind_test_parity_write_chunk(&rig, pub, sg, 0xC2, 111);
    relay_bind_test_parity_write_chunk(&rig, pub, sg, 0xC3, 111);
    R_CHECK(&rig, moq_session_end_object(pub->peer, sg, rig.now) == MOQ_OK);
    R_CHECK(&rig, moq_session_close_subgroup(pub->peer, sg, rig.now) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 8);
    relay_bind_test_peer_drain(&rig, warmer, &warm_ps, false, false, 0, 0);

    /* Issue the FETCH, then arm the coalesce-buffer OOM (exactly 333 bytes). The
     * fetch is admitted (FETCH_OK) before the coalesce runs. */
    moq_fetch_t fh;
    relay_bind_test_fetch_issue(&rig, fetcher, "v", 0, 0, 0, 0, &fh);
    a.fail_size = 333;
    for (int p = 0; p < 32; p++) {
        relay_bind_test_rig_pump(&rig, 2);
        relay_bind_test_peer_drain(&rig, warmer, &warm_ps, false, false, 0, 0);
        relay_bind_test_peer_drain(&rig, fetcher, &fetch_ps, false, false, 0, 0);
        if (fetch_ps.session_closed || fetch_ps.fetch_complete) {
            break;
        }
    }
    /* The one-shot cleared fail_size => the 333-byte coalesce buffer alloc was
     * actually hit (not a vacuous pass). Combined with the assertions below, only
     * the coalesce failing can produce this state. */
    R_CHECK(&rig, a.fail_size == 0);
    a.fail_size = 0;

    /* Fail-closed: a VISIBLE terminal (session close), never a false completion
     * or a truncated object. */
    R_CHECK(&rig, fetch_ps.session_closed);        /* bind_fetch_terminate close */
    R_CHECK(&rig, fetch_ps.fetch_complete == 0);   /* no false FETCH_COMPLETE    */
    R_CHECK(&rig, fetch_ps.fdlv_n == 0);           /* no (truncated) object      */

    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);                    /* coalesce buffer not leaked */
    if (rig.failures == 0) {
        printf("PASS: chunked_fetch_oom_terminates\n");
    }
    return rig.failures;
}

static void
jf_reset_obs(jf_down_t *d)
{
    conn_t *cn = d->cn;
    moq_subscription_t sh = d->sh, lsh = d->live_sh;
    bool sub_ok = d->sub_ok, live_ok = d->live_ok;
    memset(d, 0, sizeof(*d));
    d->cn = cn;
    d->sh = sh;
    d->live_sh = lsh;
    d->sub_ok = sub_ok;
    d->live_ok = live_ok;
}

/* The retained generation: (0,0) base bytes, (0,1) delta bytes (+ exact
 * per-object properties on the delta when asked). */
static void
jf_origin_write_generation(jf_t *j, int k, bool with_props)
{
    R_CHECK(&j->rig, relay_bind_test_jf_origin_write_raw(j, k, 0, 0, 0xB0, BOOT_BASE_LEN, false) == MOQ_OK);
    R_CHECK(&j->rig, relay_bind_test_jf_origin_write_raw(j, k, 0, 1, 0xD0, BOOT_DELTA_LEN, with_props) == MOQ_OK);
}

static void
jf_origin_answer_exact(jf_t *j, int k, bool with_props)
{
    relay_bind_test_jf_origin_accept(j, k, 0, 2, false, false);
    jf_origin_write_generation(j, k, with_props);
    relay_bind_test_jf_origin_end(j, k);
}

static void
jf_origin_reject(jf_t *j, int k, uint64_t code, const char *reason, bool can_retry,
                 uint64_t retry_after_ms)
{
    moq_reject_fetch_cfg_t rj;
    moq_reject_fetch_cfg_init(&rj);
    rj.error_code = code;
    rj.reason = B(reason);
    rj.can_retry = can_retry;
    rj.retry_after_ms = retry_after_ms;
    moq_result_t rc;
    JF_ORIGIN_RETRY(j, moq_session_reject_fetch(j->o.cn->peer, j->o.req[k], &rj, j->rig.now), rc);
    R_CHECK(&j->rig, rc == MOQ_OK);
}

static void
jf_wait_terminal(jf_t *j, jf_down_t *d)
{
    for (int i = 0; i < 16 && d->complete + d->error + d->reset == 0; i++) relay_bind_test_jf_cycle(j);
}

/* The exact bootstrap contract on one downstream: OK{eot=false, 0/2} ->
 * (0,0) base -> (0,1) delta -> COMPLETE, nothing else, no foreign item. */
static int
jf_check_exact(jf_t *j, const jf_down_t *d, bool with_props, const char *label)
{
    int bad = 0;
#define JF_EXPECT(cond)                                                   \
    do {                                                                  \
        if (!(cond)) {                                                    \
            printf("FAIL: %s:%d: %s %s: %s\n", __FILE__, __LINE__, label,  \
                   j->lbl, #cond);                                         \
            bad++;                                                        \
        }                                                                 \
    } while (0)
    JF_EXPECT(d->foreign == 0 && !d->overflow);
    JF_EXPECT(d->ledger_n == 4);
    JF_EXPECT(d->ledger_n >= 1 && d->ledger[0] == (int)MOQ_EVENT_FETCH_OK);
    JF_EXPECT(d->ledger_n >= 2 && d->ledger[1] == (int)MOQ_EVENT_FETCH_OBJECT);
    JF_EXPECT(d->ledger_n >= 3 && d->ledger[2] == (int)MOQ_EVENT_FETCH_OBJECT);
    JF_EXPECT(d->ledger_n >= 4 && d->ledger[3] == (int)MOQ_EVENT_FETCH_COMPLETE);
    JF_EXPECT(d->ok == 1 && d->error == 0 && d->reset == 0 && d->gaps == 0);
    JF_EXPECT(!d->ok_eot && d->ok_eg == 0 && d->ok_eo == 2);
    JF_EXPECT(d->objects == 2 && d->complete == 1);
    JF_EXPECT(d->objects >= 1 && d->obj_g[0] == 0 && d->obj_o[0] == 0 &&
              d->obj_len[0] == BOOT_BASE_LEN && d->obj_exact[0] && d->props_len[0] == 0);
    JF_EXPECT(d->objects >= 2 && d->obj_g[1] == 0 && d->obj_o[1] == 1 &&
              d->obj_len[1] == BOOT_DELTA_LEN && d->obj_exact[1]);
    if (with_props) {
        JF_EXPECT(d->objects >= 2 && d->props_len[1] == JF_PROPS_LEN && d->props_exact[1]);
    } else {
        JF_EXPECT(d->objects >= 2 && d->props_len[1] == 0);
    }
#undef JF_EXPECT
    return bad;
}

/* A rejection before any success: exactly one FETCH_ERROR carrying `code`,
 * nothing else ever polled for the request. */
static int
jf_check_rejected(jf_t *j, const jf_down_t *d, uint64_t code, const char *label)
{
    int bad = 0;
    if (!(d->foreign == 0 && d->ledger_n == 1 && d->ledger[0] == (int)MOQ_EVENT_FETCH_ERROR &&
          d->ok == 0 && d->objects == 0 && d->complete == 0 && d->reset == 0 &&
          d->error == 1 && d->error_code == code)) {
        printf("FAIL: %s %s: expected one FETCH_ERROR(%llu); got ledger_n=%d ok=%d objects=%d "
               "complete=%d error=%d code=%llu reset=%d foreign=%d\n",
               label, j->lbl, (unsigned long long)code, d->ledger_n, d->ok, d->objects,
               d->complete, d->error, (unsigned long long)d->error_code, d->reset,
               d->foreign);
        bad++;
    }
    return bad;
}

/* -- rows ---------------------------------------------------------------- */

typedef struct jf_singleton_trace {
    bool armed;
    int requests;
    moq_stream_ref_t request_ref;
} jf_singleton_trace_t;

static void
jf_singleton_trace(void *ctx, const moq_sim_trace_record_t *r)
{
    jf_singleton_trace_t *t = ctx;
    if (t->armed && r->kind == MOQ_SIM_TRACE_ACTION &&
        r->from == MOQ_PERSPECTIVE_SERVER &&
        r->action_kind == MOQ_ACTION_OPEN_BIDI_STREAM &&
        r->detail_bytes.len > 0 && r->detail_bytes.data[0] == 0x16) {
        t->request_ref = r->stream_ref;
        t->requests++;
    }
}

/* Issue #51: Largest {2,0}, no live catalog tail, Joining FETCH(0) forwarded
 * as standalone [2:0,2:1). Literal peers bypass the local object encoder and
 * independently finish the response half before or after the data stream.
 * The response is still collected until both FETCH_OK and data FIN exist. */
static int
jf_exact_group2_singleton(moq_version_t version, int wire_order)
{
    jf_singleton_trace_t tr = {0};
    relay_bind_test_trace_fn = jf_singleton_trace;
    relay_bind_test_trace_ctx = &tr;
    jf_t j;
    bool ready = relay_bind_test_jf_setup_at(&j, version, 1, true, 2, 0, false);
    relay_bind_test_trace_fn = NULL;
    relay_bind_test_trace_ctx = NULL;
    if (!ready) return 1 + j.rig.failures;
    tr.armed = true;
    relay_bind_test_jf_join(&j, &j.d[0]);
    relay_bind_test_jf_wait_requests(&j, 1);
    tr.armed = false;
    R_CHECK(&j.rig, j.o.req_sg[0] == 2 && j.o.req_so[0] == 0 &&
                    j.o.req_eg[0] == 2 && j.o.req_eo[0] == 1);

    if (wire_order == 0) {
        relay_bind_test_jf_origin_accept(&j, 0, 2, 1, false, false);
        R_CHECK(&j.rig, relay_bind_test_jf_origin_write_raw(
            &j, 0, 2, 0, 0xB0, BOOT_BASE_LEN, false) == MOQ_OK);
        for (int i = 0; i < 3; i++) relay_bind_test_jf_cycle(&j);
        R_CHECK(&j.rig, j.d[0].ledger_n == 0);
        relay_bind_test_jf_live_control(&j);
        relay_bind_test_jf_origin_end(&j, 0);
    } else {
        /* Accept only to obtain the allocated request ID from FETCH_HEADER;
         * consume, cleanup and replace both actions rather than delivering a
         * second response through SimPair. All wire bytes below are literal. */
        relay_bind_test_jf_origin_accept(&j, 0, 2, 1, false, false);
        moq_action_t actions[8];
        size_t n = moq_session_poll_actions(j.o.cn->peer, actions, 8);
        int headers = 0, responses = 0;
        uint8_t request_id = 0;
        for (size_t i = 0; i < n; i++) {
            const moq_action_t *a = &actions[i];
            if (a->kind == MOQ_ACTION_SEND_DATA) {
                headers++;
                R_CHECK(&j.rig, a->u.send_data.header_len == 2 &&
                                a->u.send_data.header[0] == 0x05 &&
                                a->u.send_data.header[1] < 64 &&
                                a->u.send_data.payload == NULL && !a->u.send_data.fin);
                request_id = a->u.send_data.header[1];
            } else if (a->kind == MOQ_ACTION_SEND_CONTROL ||
                       a->kind == MOQ_ACTION_SEND_BIDI_STREAM) {
                responses++;
            } else {
                R_CHECK(&j.rig, 0);
            }
            moq_action_cleanup(&actions[i]);
        }
        R_CHECK(&j.rig, n == 2 && headers == 1 && responses == 1);
        uint8_t ok16[] = { 0x18, 0x00, 0x05, request_id, 0x00, 0x02, 0x01, 0x00 };
        static const uint8_t ok18[] = { 0x18, 0x00, 0x04, 0x00, 0x02, 0x01, 0x00 };
        uint8_t data[2 + 5 + BOOT_BASE_LEN] = {
            0x05, request_id, 0x1c, 0x02, 0x00, 0x00, BOOT_BASE_LEN
        };
        relay_bind_test_boot_fill(data + 7, BOOT_BASE_LEN, 0xB0);
        const moq_stream_ref_t data_ref = moq_stream_ref_from_u64(UINT64_C(0x51000051));
        if (version == MOQ_VERSION_DRAFT_18)
            R_CHECK(&j.rig, tr.requests == 1 && tr.request_ref._v != 0);

        if (wire_order == 2) {
            R_CHECK(&j.rig, moq_session_on_data_bytes(j.o.cn->rsess,
                data_ref, data, sizeof(data), true, j.rig.now) == MOQ_OK);
            for (int i = 0; i < 3; i++) relay_bind_test_jf_cycle(&j);
            R_CHECK(&j.rig, j.d[0].ledger_n == 0);
        }
        if (version == MOQ_VERSION_DRAFT_16) {
            R_CHECK(&j.rig, moq_session_on_control_bytes(j.o.cn->rsess,
                ok16, sizeof(ok16), j.rig.now) == MOQ_OK);
        } else {
            R_CHECK(&j.rig, moq_session_on_bidi_stream_bytes(j.o.cn->rsess,
                tr.request_ref, ok18, sizeof(ok18), true, j.rig.now) == MOQ_OK);
        }
        if (wire_order == 1) {
            /* Preserve the real writer's separate header/object/length writes,
             * and prove that no terminal is inferred from a complete payload. */
            R_CHECK(&j.rig, moq_session_on_data_bytes(j.o.cn->rsess,
                data_ref, data, 2, false, j.rig.now) == MOQ_OK);
            R_CHECK(&j.rig, moq_session_on_data_bytes(j.o.cn->rsess,
                data_ref, data + 2, 4, false, j.rig.now) == MOQ_OK);
            R_CHECK(&j.rig, moq_session_on_data_bytes(j.o.cn->rsess,
                data_ref, data + 6, sizeof(data) - 6, false, j.rig.now) == MOQ_OK);
            for (int i = 0; i < 3; i++) relay_bind_test_jf_cycle(&j);
            R_CHECK(&j.rig, j.d[0].ledger_n == 0);
            relay_bind_test_jf_live_control(&j);
            R_CHECK(&j.rig, moq_session_on_data_bytes(j.o.cn->rsess,
                data_ref, NULL, 0, true, j.rig.now) == MOQ_OK);
        }
    }

    jf_wait_terminal(&j, &j.d[0]);
    for (int i = 0; i < 3; i++) relay_bind_test_jf_cycle(&j);
    jf_down_t *d = &j.d[0];
    R_CHECK(&j.rig, d->ledger_n == 3 && d->ledger[0] == MOQ_EVENT_FETCH_OK &&
                    d->ledger[1] == MOQ_EVENT_FETCH_OBJECT &&
                    d->ledger[2] == MOQ_EVENT_FETCH_COMPLETE);
    R_CHECK(&j.rig, d->ok == 1 && d->objects == 1 && d->complete == 1 &&
                    d->gaps == 0 && d->error == 0 && d->reset == 0 &&
                    d->foreign == 0 && !d->overflow && d->closed == 0);
    R_CHECK(&j.rig, !d->ok_eot && d->ok_eg == 2 && d->ok_eo == 1 &&
                    d->obj_g[0] == 2 && d->obj_o[0] == 0 &&
                    d->obj_len[0] == BOOT_BASE_LEN && d->obj_exact[0] &&
                    d->props_len[0] == 0);
    moqr_bind_fwd_debug_t st;
    relay_bind_test_jf_stats(&j, &st);
    R_CHECK(&j.rig, st.admitted == 1 && st.completed == 1 && st.delivered == 1 &&
                    st.live == 0 && st.bytes_used == 0 && st.cancelled_up == 0 &&
                    st.errors == 0 && st.rejected_down == 0 && st.aborted == 0);
    R_CHECK(&j.rig, j.o.fetch_requests == 1 && j.o.cancelled == 0);
    if (wire_order == 2) relay_bind_test_jf_live_control(&j);
    return relay_bind_test_jf_teardown(&j, wire_order == 0 ? "fwd_group2_singleton"
        : wire_order == 1 ? "fwd_group2_literal_response_fin_first"
                          : "fwd_group2_literal_data_fin_first");
}

/* Exact single receiver, with per-object properties on the delta: one
 * upstream request carrying the normalized range [0:0, 0:2), the response
 * relayed byte-exact in order, then the pool and transactions back to zero
 * and the live path intact. */
static int
jf_exact_single(moq_version_t version, bool with_props)
{
    jf_t j;
    if (!relay_bind_test_jf_setup(&j, version, 1, true, 1)) return 1 + j.rig.failures;
    relay_bind_test_jf_join(&j, &j.d[0]);
    relay_bind_test_jf_wait_requests(&j, 1);
    R_CHECK(&j.rig, j.o.req_sg[0] == 0 && j.o.req_so[0] == 0 &&
                    j.o.req_eg[0] == 0 && j.o.req_eo[0] == 2);
    moqr_bind_fwd_debug_t st;
    relay_bind_test_jf_stats(&j, &st);
    R_CHECK(&j.rig, st.admitted == 1 && st.live == 1 && st.bytes_used == JF_KEY_LEN);
    jf_origin_answer_exact(&j, 0, with_props);
    jf_wait_terminal(&j, &j.d[0]);
    j.rig.failures += jf_check_exact(&j, &j.d[0], with_props, "exact single");
    relay_bind_test_jf_stats(&j, &st);
    R_CHECK(&j.rig, st.completed == 1 && st.delivered == 1 && st.live == 0 &&
                    st.bytes_used == 0 && st.cancelled_up == 0 && st.errors == 0 &&
                    st.rejected_down == 0 && st.aborted == 0);
    /* Peak pool use is exactly the owned key + the collected bytes. */
    R_CHECK(&j.rig, st.bytes_high == JF_KEY_LEN + BOOT_BASE_LEN + BOOT_DELTA_LEN +
                                         (with_props ? JF_PROPS_LEN : 0));
    R_CHECK(&j.rig, j.o.cancelled == 0 && j.o.fetch_requests == 1);
    /* The forwarded objects never entered the live log. */
    {
        char dump[4096];
        size_t dn = 0;
        if (moqr_core_route_dump_text(j.rig.core, dump, sizeof(dump), &dn) == MOQR_OK &&
            dn < sizeof(dump)) {
            dump[dn] = '\0';
            const char *cat = strstr(dump, "\"catalog\"");
            const char *log = cat ? strstr(cat, "log:") : NULL;
            R_CHECK(&j.rig, log != NULL && strstr(log, "records=0") != NULL);
        } else {
            R_CHECK(&j.rig, 0);
        }
    }
    relay_bind_test_jf_live_control(&j);
    return relay_bind_test_jf_teardown(&j, with_props ? "fwd_exact_single_props" : "fwd_exact_single");
}

/* Two receivers in the same cycle: two upstream requests, each answered and
 * relayed exactly to its own requester, no cross-talk, no duplicate. */
static int
jf_exact_two(moq_version_t version)
{
    jf_t j;
    if (!relay_bind_test_jf_setup(&j, version, 2, true, 1)) return 1 + j.rig.failures;
    relay_bind_test_jf_join(&j, &j.d[0]);
    relay_bind_test_jf_join(&j, &j.d[1]);
    relay_bind_test_jf_wait_requests(&j, 2);
    moqr_bind_fwd_debug_t st;
    relay_bind_test_jf_stats(&j, &st);
    R_CHECK(&j.rig, st.admitted == 2 && st.live == 2 && st.bytes_used == 2 * JF_KEY_LEN);
    jf_origin_answer_exact(&j, 0, false);
    jf_origin_answer_exact(&j, 1, false);
    jf_wait_terminal(&j, &j.d[0]);
    jf_wait_terminal(&j, &j.d[1]);
    j.rig.failures += jf_check_exact(&j, &j.d[0], false, "exact two (a)");
    j.rig.failures += jf_check_exact(&j, &j.d[1], false, "exact two (b)");
    relay_bind_test_jf_stats(&j, &st);
    R_CHECK(&j.rig, st.completed == 2 && st.delivered == 2 && st.live == 0 &&
                    st.bytes_used == 0 && j.o.cancelled == 0 && j.o.fetch_requests == 2);
    relay_bind_test_jf_live_control(&j);
    return relay_bind_test_jf_teardown(&j, "fwd_exact_two_receivers");
}

/* Two concurrent collections share ONE byte pool: sized for both keys and
 * one full response, so the second response overflows mid-collection. The
 * overflowed request is rejected before any OK, its upstream request is
 * cancelled, the other is exact, the pool returns to zero and the live path
 * is intact. */
static int
jf_shared_bound(moq_version_t version)
{
    relay_bind_test_fetch_forward_bytes = 2 * JF_KEY_LEN + BOOT_BASE_LEN + BOOT_DELTA_LEN + 4;
    jf_t j;
    if (!relay_bind_test_jf_setup(&j, version, 2, true, 1)) return 1 + j.rig.failures;
    moqr_bind_fwd_debug_t st;
    relay_bind_test_jf_stats(&j, &st);
    R_CHECK(&j.rig, st.bytes_cap == relay_bind_test_fetch_forward_bytes);
    relay_bind_test_jf_join(&j, &j.d[0]);
    relay_bind_test_jf_join(&j, &j.d[1]);
    relay_bind_test_jf_wait_requests(&j, 2);
    /* Interleave the answers so the second collection meets the shared
     * ceiling with the first still charged: both accepted, first written in
     * full, then the second. */
    relay_bind_test_jf_origin_accept(&j, 0, 0, 2, false, false);
    relay_bind_test_jf_origin_accept(&j, 1, 0, 2, false, false);
    jf_origin_write_generation(&j, 0, false);   /* first: fully collected, still open */
    relay_bind_test_jf_cycle(&j);
    relay_bind_test_jf_cycle(&j);
    relay_bind_test_jf_stats(&j, &st);
    R_CHECK(&j.rig, st.live == 2 && st.bytes_used == 2 * JF_KEY_LEN + BOOT_BASE_LEN + BOOT_DELTA_LEN);
    jf_origin_write_generation(&j, 1, false);   /* second: the delta cannot be charged */
    relay_bind_test_jf_cycle(&j);
    relay_bind_test_jf_cycle(&j);
    relay_bind_test_jf_stats(&j, &st);
    R_CHECK(&j.rig, st.overflow == 1);
    relay_bind_test_jf_origin_end(&j, 0);
    jf_wait_terminal(&j, &j.d[0]);
    jf_wait_terminal(&j, &j.d[1]);
    j.rig.failures += jf_check_exact(&j, &j.d[0], false, "shared bound (first)");
    j.rig.failures += jf_check_rejected(&j, &j.d[1], MOQ_REQUEST_ERROR_INTERNAL_ERROR,
                                        "shared bound (second)");
    for (int i = 0; i < 6 && j.o.cancelled == 0; i++) relay_bind_test_jf_cycle(&j);
    R_CHECK(&j.rig, j.o.cancelled == 1);
    relay_bind_test_jf_stats(&j, &st);
    R_CHECK(&j.rig, st.overflow == 1 && st.cancelled_up == 1 && st.rejected_down == 1 &&
                    st.delivered == 1 && st.live == 0 && st.bytes_used == 0 &&
                    st.bytes_high <= st.bytes_cap);
    relay_bind_test_jf_live_control(&j);
    return relay_bind_test_jf_teardown(&j, "fwd_shared_byte_bound");
}

/* Count bound: a response of more than 64 objects (inside a wide range) is
 * abandoned at the 65th, rejected before OK, cancelled upstream. */
static int
jf_count_overflow(moq_version_t version)
{
    jf_t j;
    if (!relay_bind_test_jf_setup(&j, version, 1, true, 80)) return 1 + j.rig.failures;
    relay_bind_test_jf_join(&j, &j.d[0]);
    relay_bind_test_jf_wait_requests(&j, 1);
    R_CHECK(&j.rig, j.o.req_eg[0] == 0 && j.o.req_eo[0] == 81);
    relay_bind_test_jf_origin_accept(&j, 0, 0, 81, false, false);
    int written = 0;
    for (uint64_t o = 0; o < 65; o++) {
        if (relay_bind_test_jf_origin_write_raw(&j, 0, 0, o, (uint8_t)o, 8, false) == MOQ_OK) {
            written++;
        }
        if ((o % 8) == 7) relay_bind_test_jf_cycle(&j);
    }
    R_CHECK(&j.rig, written == 65);
    jf_wait_terminal(&j, &j.d[0]);
    j.rig.failures += jf_check_rejected(&j, &j.d[0], MOQ_REQUEST_ERROR_INTERNAL_ERROR,
                                        "count overflow");
    for (int i = 0; i < 6 && j.o.cancelled == 0; i++) relay_bind_test_jf_cycle(&j);
    R_CHECK(&j.rig, j.o.cancelled == 1);
    moqr_bind_fwd_debug_t st;
    relay_bind_test_jf_stats(&j, &st);
    R_CHECK(&j.rig, st.overflow == 1 && st.cancelled_up == 1 && st.live == 0 &&
                    st.bytes_used == 0 && st.delivered == 0);
    relay_bind_test_jf_live_control(&j);
    return relay_bind_test_jf_teardown(&j, "fwd_count_overflow");
}

/* Upstream REQUEST_ERROR: the code, can_retry and reason text reach the
 * downstream verbatim; nothing else is ever sent for the request. */
static int
jf_upstream_error(moq_version_t version)
{
    jf_t j;
    if (!relay_bind_test_jf_setup(&j, version, 1, true, 1)) return 1 + j.rig.failures;
    relay_bind_test_jf_join(&j, &j.d[0]);
    relay_bind_test_jf_wait_requests(&j, 1);
    jf_origin_reject(&j, 0, MOQ_REQUEST_ERROR_UNAUTHORIZED, "origin says no", true, 250);
    jf_wait_terminal(&j, &j.d[0]);
    j.rig.failures += jf_check_rejected(&j, &j.d[0], MOQ_REQUEST_ERROR_UNAUTHORIZED,
                                        "upstream error");
    R_CHECK(&j.rig, j.d[0].reason_len == 14 &&
                    memcmp(j.d[0].reason, "origin says no", 14) == 0);
    moqr_bind_fwd_debug_t st;
    relay_bind_test_jf_stats(&j, &st);
    R_CHECK(&j.rig, st.errors == 1 && st.rejected_down == 1 && st.cancelled_up == 0 &&
                    st.live == 0 && st.bytes_used == 0 && j.o.cancelled == 0);
    relay_bind_test_jf_live_control(&j);
    return relay_bind_test_jf_teardown(&j, "fwd_upstream_error_reason");
}

/* Range mismatch: an upstream object outside the pinned range (or an OK End
 * Location past it) is never relayed as a success. */
static int
jf_range_mismatch(moq_version_t version, bool at_ok)
{
    jf_t j;
    if (!relay_bind_test_jf_setup(&j, version, 1, true, 1)) return 1 + j.rig.failures;
    relay_bind_test_jf_join(&j, &j.d[0]);
    relay_bind_test_jf_wait_requests(&j, 1);
    if (at_ok) {
        relay_bind_test_jf_origin_accept(&j, 0, 0, 9, false, false); /* End past [0:0, 0:2) */
    } else {
        relay_bind_test_jf_origin_accept(&j, 0, 0, 2, false, false);
        R_CHECK(&j.rig, relay_bind_test_jf_origin_write_raw(&j, 0, 0, 0, 0xB0, BOOT_BASE_LEN, false) == MOQ_OK);
        R_CHECK(&j.rig, relay_bind_test_jf_origin_write_raw(&j, 0, 0, 5, 0x55, 8, false) == MOQ_OK);
    }
    jf_wait_terminal(&j, &j.d[0]);
    j.rig.failures += jf_check_rejected(&j, &j.d[0], MOQ_REQUEST_ERROR_INTERNAL_ERROR,
                                        at_ok ? "range mismatch (ok)" : "range mismatch (object)");
    for (int i = 0; i < 6 && j.o.cancelled == 0; i++) relay_bind_test_jf_cycle(&j);
    R_CHECK(&j.rig, j.o.cancelled == 1);
    moqr_bind_fwd_debug_t st;
    relay_bind_test_jf_stats(&j, &st);
    R_CHECK(&j.rig, st.mismatch == 1 && st.cancelled_up == 1 && st.live == 0 &&
                    st.bytes_used == 0 && st.delivered == 0);
    relay_bind_test_jf_live_control(&j);
    return relay_bind_test_jf_teardown(&j, at_ok ? "fwd_range_mismatch_ok" : "fwd_range_mismatch_object");
}

/* Upstream data-stream RESET after OK + one object: rejected before any
 * downstream OK (never a truncated success). */
static int
jf_upstream_reset(moq_version_t version)
{
    jf_t j;
    if (!relay_bind_test_jf_setup(&j, version, 1, true, 1)) return 1 + j.rig.failures;
    relay_bind_test_jf_join(&j, &j.d[0]);
    relay_bind_test_jf_wait_requests(&j, 1);
    relay_bind_test_jf_origin_accept(&j, 0, 0, 2, false, false);
    R_CHECK(&j.rig, relay_bind_test_jf_origin_write_raw(&j, 0, 0, 0, 0xB0, BOOT_BASE_LEN, false) == MOQ_OK);
    relay_bind_test_jf_cycle(&j);
    moq_result_t rc;
    JF_ORIGIN_RETRY(&j, moq_session_abort_fetch(j.o.cn->peer, j.o.req[0], 0x5u, j.rig.now), rc);
    R_CHECK(&j.rig, rc == MOQ_OK);
    jf_wait_terminal(&j, &j.d[0]);
    j.rig.failures += jf_check_rejected(&j, &j.d[0], MOQ_REQUEST_ERROR_INTERNAL_ERROR,
                                        "upstream reset");
    moqr_bind_fwd_debug_t st;
    relay_bind_test_jf_stats(&j, &st);
    R_CHECK(&j.rig, st.resets == 1 && st.rejected_down == 1 && st.live == 0 &&
                    st.bytes_used == 0 && st.delivered == 0);
    relay_bind_test_jf_live_control(&j);
    return relay_bind_test_jf_teardown(&j, "fwd_upstream_reset");
}

/* Source loss with the request pending upstream, then a replacement origin:
 * the pending transaction is rejected (never re-aimed at the newcomer), a
 * fresh subscription + join against the replacement is served exactly, and
 * the replacement sees only that one request. */
static int
jf_source_loss_replacement(moq_version_t version)
{
    jf_t j;
    if (!relay_bind_test_jf_setup(&j, version, 2, false, 1)) return 1 + j.rig.failures;
    relay_bind_test_jf_join(&j, &j.d[0]);
    relay_bind_test_jf_wait_requests(&j, 1);
    moqr_bind_fwd_debug_t st;
    relay_bind_test_jf_stats(&j, &st);
    R_CHECK(&j.rig, st.live == 1);
    /* The origin goes away at the binding before answering. */
    R_CHECK(&j.rig, moqr_bind_conn_close(j.rig.bind, j.o.cn->rsess) == MOQR_OK);
    j.o.gone = true;
    jf_wait_terminal(&j, &j.d[0]);
    j.rig.failures += jf_check_rejected(&j, &j.d[0], MOQ_REQUEST_ERROR_INTERNAL_ERROR,
                                        "source loss");
    relay_bind_test_jf_stats(&j, &st);
    R_CHECK(&j.rig, st.lost == 1 && st.rejected_down == 1 && st.live == 0 && st.bytes_used == 0);
    /* Replacement origin on a new connection: announce, then the second
     * downstream (whose subscription was retired with the source) subscribes
     * afresh and joins. */
    jf_origin_t old = j.o;
    memset(&j.o, 0, sizeof(j.o));
    j.o.cn = relay_bind_test_rig_connect(&j.rig, version);
    j.o.largest_object = 1;
    R_CHECK(&j.rig, j.o.cn != NULL);
    relay_bind_test_rig_pump(&j.rig, 4);
    moq_publish_namespace_cfg_t pcfg;
    memset(&pcfg, 0, sizeof(pcfg));
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = (moq_bytes_t *)relay_bind_test_JF_NS, .count = 2 };
    moq_announcement_t ann;
    R_CHECK(&j.rig, moq_session_publish_namespace(j.o.cn->peer, &pcfg, j.rig.now, &ann) == MOQ_OK);
    for (int i = 0; i < 8 && j.o.ns_accepted == 0; i++) relay_bind_test_jf_cycle(&j);
    R_CHECK(&j.rig, j.o.ns_accepted == 1);
    for (int i = 0; i < 8 && j.d[1].done == 0; i++) relay_bind_test_jf_cycle(&j);
    jf_reset_obs(&j.d[1]);
    j.d[1].sub_ok = false;
    relay_bind_test_jf_subscribe_catalog(&j, &j.d[1]);
    for (int i = 0; i < 12 && !j.d[1].sub_ok; i++) relay_bind_test_jf_cycle(&j);
    R_CHECK(&j.rig, j.d[1].sub_ok && j.o.cat_seen);
    relay_bind_test_jf_join(&j, &j.d[1]);
    relay_bind_test_jf_wait_requests(&j, 1);
    jf_origin_answer_exact(&j, 0, false);
    jf_wait_terminal(&j, &j.d[1]);
    j.rig.failures += jf_check_exact(&j, &j.d[1], false, "replacement");
    R_CHECK(&j.rig, j.o.fetch_requests == 1 && old.fetch_requests == 1);
    relay_bind_test_jf_stats(&j, &st);
    R_CHECK(&j.rig, st.admitted == 2 && st.delivered == 1 && st.live == 0 && st.bytes_used == 0);
    return relay_bind_test_jf_teardown(&j, "fwd_source_loss_replacement");
}

/* Downstream cancel (a) before the origin answered, (b) during collection
 * (OK + one object held open): the upstream request is cancelled, the
 * transaction and its bytes are released, and nothing is ever written down. */
static int
jf_down_cancel(moq_version_t version, bool during_collection)
{
    jf_t j;
    if (!relay_bind_test_jf_setup(&j, version, 1, true, 1)) return 1 + j.rig.failures;
    relay_bind_test_jf_join(&j, &j.d[0]);
    relay_bind_test_jf_wait_requests(&j, 1);
    if (during_collection) {
        relay_bind_test_jf_origin_accept(&j, 0, 0, 2, false, false);
        R_CHECK(&j.rig, relay_bind_test_jf_origin_write_raw(&j, 0, 0, 0, 0xB0, BOOT_BASE_LEN, false) == MOQ_OK);
        relay_bind_test_jf_cycle(&j);
        moqr_bind_fwd_debug_t mid;
        relay_bind_test_jf_stats(&j, &mid);
        R_CHECK(&j.rig, mid.live == 1 && mid.bytes_used == JF_KEY_LEN + BOOT_BASE_LEN);
    }
    R_CHECK(&j.rig, moq_session_fetch_cancel(j.d[0].cn->peer, j.d[0].fh, j.rig.now) == MOQ_OK);
    for (int i = 0; i < 8 && j.o.cancelled == 0; i++) relay_bind_test_jf_cycle(&j);
    R_CHECK(&j.rig, j.o.cancelled == 1);
    for (int i = 0; i < 4; i++) relay_bind_test_jf_cycle(&j);
    R_CHECK(&j.rig, j.d[0].ledger_n == 0 && j.d[0].foreign == 0);
    moqr_bind_fwd_debug_t st;
    relay_bind_test_jf_stats(&j, &st);
    R_CHECK(&j.rig, st.cancelled_down == 1 && st.cancelled_up == 1 && st.live == 0 &&
                    st.bytes_used == 0 && st.delivered == 0 && st.rejected_down == 0);
    relay_bind_test_jf_live_control(&j);
    return relay_bind_test_jf_teardown(&j, during_collection ? "fwd_down_cancel_during_collection"
                                             : "fwd_down_cancel_before_answer");
}

/* Diagnostic trace ring: SimPair inputs refused by a session and stream
 * resets/stops, newest last. */
typedef struct jf_trace {
    bool armed;
    bool verbose;   /* keep every input/action record, not only refusals/stops */
    int  n;
    struct {
        int      kind, input_kind, action_kind, from, to;
        int      result;
        uint64_t stream, step, code;
        uint8_t  first[6];
        size_t   len;
    } rec[64];
    int refused_total;
} jf_trace_t;

static void
jf_trace_fn(void *ctx, const moq_sim_trace_record_t *r)
{
    jf_trace_t *t = (jf_trace_t *)ctx;
    if (!t->armed) return;
    bool keep = t->verbose && (r->kind == MOQ_SIM_TRACE_INPUT || r->kind == MOQ_SIM_TRACE_ACTION);
    if (r->kind == MOQ_SIM_TRACE_INPUT && r->result < 0) { keep = true; t->refused_total++; }
    if (r->kind == MOQ_SIM_TRACE_INPUT &&
        (r->input_kind == MOQ_SIM_INPUT_DATA_RESET || r->input_kind == MOQ_SIM_INPUT_DATA_STOP ||
         r->input_kind == MOQ_SIM_INPUT_BIDI_RESET || r->input_kind == MOQ_SIM_INPUT_BIDI_STOP)) keep = true;
    if (r->kind == MOQ_SIM_TRACE_ACTION &&
        (r->action_kind == MOQ_ACTION_RESET_DATA || r->action_kind == MOQ_ACTION_STOP_DATA ||
         r->action_kind == MOQ_ACTION_RESET_BIDI_STREAM || r->action_kind == MOQ_ACTION_STOP_BIDI_STREAM ||
         r->action_kind == MOQ_ACTION_ABORT_BIDI_STREAM)) keep = true;
    if (r->kind >= MOQ_SIM_TRACE_FAULT_DROP && r->kind <= MOQ_SIM_TRACE_DELAY_STALE) keep = true;
    if (!keep) return;
    int i = t->n < 64 ? t->n++ : (memmove(&t->rec[0], &t->rec[1], sizeof(t->rec[0]) * 63), 63);
    memset(&t->rec[i], 0, sizeof(t->rec[i]));
    t->rec[i].kind = (int)r->kind;
    t->rec[i].input_kind = (int)r->input_kind;
    t->rec[i].action_kind = (int)r->action_kind;
    t->rec[i].from = (int)r->from;
    t->rec[i].to = (int)r->to;
    t->rec[i].result = (int)r->result;
    t->rec[i].step = r->step;
    t->rec[i].code = r->code;
    if (r->struct_size >= offsetof(moq_sim_trace_record_t, stream_ref) + sizeof(r->stream_ref)) {
        t->rec[i].stream = r->stream_ref._v;
    }
    t->rec[i].len = r->bytes.len;
    size_t c = r->bytes.len < 6 ? r->bytes.len : 6;
    if (c > 0 && r->bytes.data) memcpy(t->rec[i].first, r->bytes.data, c);
}

static void
jf_trace_dump(const jf_trace_t *t, const char *label)
{
    printf("  trace %s: refused_total=%d kept=%d\n", label, t->refused_total, t->n);
    for (int i = 0; i < t->n; i++) {
        printf("    [%d] kind=%d in=%d act=%d %d->%d rc=%d stream=%llu step=%llu code=%llu len=%zu "
               "first=%02x%02x%02x%02x%02x%02x\n", i, t->rec[i].kind, t->rec[i].input_kind,
               t->rec[i].action_kind, t->rec[i].from, t->rec[i].to, t->rec[i].result,
               (unsigned long long)t->rec[i].stream, (unsigned long long)t->rec[i].step,
               (unsigned long long)t->rec[i].code, t->rec[i].len, t->rec[i].first[0],
               t->rec[i].first[1], t->rec[i].first[2], t->rec[i].first[3], t->rec[i].first[4],
               t->rec[i].first[5]);
    }
}

/* Downstream cancel after acceptance: a 60-object response against a
 * 32-deep relay action queue, so the relay's FETCH_OK and first objects are
 * queued with the rest still held (WOULD_BLOCK, retried). The downstream
 * pair is then stepped WITHOUT a bind pump: the queued OK and objects reach
 * the peer, which cancels; the next transport step delivers the cancel to
 * an idle relay queue, and the following pump observes FETCH_CANCELLED with
 * items still unwritten, releases them without a write, and leaves the
 * origin untouched. */
static int
jf_down_cancel_after_ok(moq_version_t version)
{
    static jf_trace_t tr;
    memset(&tr, 0, sizeof(tr));
    relay_bind_test_trace_fn = jf_trace_fn;
    relay_bind_test_trace_ctx = &tr;
    jf_t j;
    relay_bind_test_max_actions = 32;
    if (!relay_bind_test_jf_setup(&j, version, 1, false, 60)) return 1 + j.rig.failures;
    relay_bind_test_trace_fn = NULL;
    tr.armed = true;
    relay_bind_test_jf_join(&j, &j.d[0]);
    relay_bind_test_jf_wait_requests(&j, 1);
    relay_bind_test_jf_origin_accept(&j, 0, 0, 61, false, false);
    for (uint64_t o = 0; o < 60; o++) {
        R_CHECK(&j.rig, relay_bind_test_jf_origin_write_raw(&j, 0, 0, o, (uint8_t)o, 8, false) == MOQ_OK);
        if ((o % 8) == 7) relay_bind_test_jf_cycle(&j);
    }
    relay_bind_test_jf_origin_end(&j, 0);
    moqr_bind_fwd_debug_t st;
    for (int i = 0; i < 30; i++) {
        relay_bind_test_jf_cycle(&j);
        relay_bind_test_jf_stats(&j, &st);
        if (st.completed == 1) break;
    }
    /* The collecting pump also accepted and started writing: the relay's
     * queue is full, the rest of the items are held. */
    R_CHECK(&j.rig, st.completed == 1 && st.live == 1 && st.delivered == 0 && st.retried_write > 0);
    uint64_t holds = st.retried_write;
    relay_bind_test_rig_step_pair(&j.rig, j.d[0].cn); /* transport only: OK + first objects land */
    relay_bind_test_jf_down_drain(&j.rig, &j.d[0]);
    R_CHECK(&j.rig, j.d[0].ok == 1 && j.d[0].objects > 0 && j.d[0].objects < 60);
    int seen = j.d[0].objects;
    R_CHECK(&j.rig, moq_session_fetch_cancel(j.d[0].cn->peer, j.d[0].fh, j.rig.now) == MOQ_OK);
    relay_bind_test_rig_step_pair(&j.rig, j.d[0].cn); /* the cancel reaches an idle relay queue */
    relay_bind_test_rig_bind_pump_once(&j.rig);
    relay_bind_test_jf_stats(&j, &st);
    R_CHECK(&j.rig, st.cancelled_down == 1 && st.live == 0 && st.bytes_used == 0 &&
                    st.delivered == 0 && st.aborted == 0);
    for (int i = 0; i < 8; i++) relay_bind_test_jf_cycle(&j);
    relay_bind_test_jf_stats(&j, &st);
    R_CHECK(&j.rig, st.cancelled_down == 1 && st.live == 0 && st.bytes_used == 0 &&
                    st.delivered == 0 && st.aborted == 0 && st.retried_write == holds);
    R_CHECK(&j.rig, j.d[0].foreign == 0 && j.d[0].error == 0 && j.d[0].complete == 0 &&
                    j.d[0].closed == 0 && j.o.cancelled == 0);
    R_CHECK(&j.rig, j.d[0].objects == seen);   /* nothing written after the cancel */
    R_CHECK(&j.rig, moq_session_state(j.d[0].cn->rsess) == MOQ_SESS_ESTABLISHED);
    printf("  cancel-after-ok %s: holds=%llu objects_seen_at_cancel=%d cancelled_down=%llu "
           "peer_closed=%d\n", j.lbl, (unsigned long long)holds, seen,
           (unsigned long long)st.cancelled_down, j.d[0].closed);
    if (j.d[0].closed) jf_trace_dump(&tr, "cancel-after-ok");
    return relay_bind_test_jf_teardown(&j, "fwd_down_cancel_after_ok");
}

/* Named core-contract check, run only through its own CTest entry (the
 * binary's `--core-cancel-after-stop` argument), never in the ordinary suite:
 * the downstream pair's sessions hold a 3-deep action queue and the pair is
 * held unstepped while the relay accepts and starts writing the forwarded
 * response, so FETCH_OK and the first objects sit in the relay's queue; the
 * pair is released in the same cycle the downstream peer cancels. The
 * contract under test is the PEER's session: having cancelled, it must
 * absorb the late response stream (STOP at its header, discard what was
 * already on the wire behind it) and stay established. The relay-side
 * invariants (no write after the cancel, no relay-side close, cleanup to
 * zero) are asserted as well. The exact SimPair action/input trace of the
 * crossing is printed. */
static int
core_cancel_after_stop(moq_version_t version)
{
    static jf_trace_t tr;
    memset(&tr, 0, sizeof(tr));
    tr.verbose = true;
    relay_bind_test_trace_fn = jf_trace_fn;
    relay_bind_test_trace_ctx = &tr;
    jf_t j;
    relay_bind_test_max_actions = 3;
    if (!relay_bind_test_jf_setup(&j, version, 1, false, 1)) return 1 + j.rig.failures;
    relay_bind_test_trace_fn = NULL;
    relay_bind_test_jf_join(&j, &j.d[0]);
    relay_bind_test_jf_wait_requests(&j, 1);
    jf_origin_answer_exact(&j, 0, false);
    j.d[0].cn->frozen = true;
    moqr_bind_fwd_debug_t st;
    int held = 0;
    for (int i = 0; i < 12; i++) {
        relay_bind_test_jf_cycle(&j);
        relay_bind_test_jf_stats(&j, &st);
        if (st.completed == 1) held++;
        if (st.retried_accept + st.retried_write + st.retried_fin > 0 && held >= 3) break;
    }
    relay_bind_test_jf_stats(&j, &st);
    R_CHECK(&j.rig, st.completed == 1 && st.live == 1 && st.delivered == 0);
    tr.armed = true;   /* trace the crossing only */
    j.d[0].cn->frozen = false;
    R_CHECK(&j.rig, moq_session_fetch_cancel(j.d[0].cn->peer, j.d[0].fh, j.rig.now) == MOQ_OK);
    for (int i = 0; i < 8; i++) relay_bind_test_jf_cycle(&j);
    tr.armed = false;
    relay_bind_test_jf_stats(&j, &st);
    printf("  core cancel-after-stop %s: peer_closed=%d code=0x%llx reason=\"%s\" peer saw "
           "ok=%d objects=%d reset=%d | relay cancelled_down=%llu down_gone=%llu down_lost=%llu "
           "delivered=%llu aborted=%llu live=%llu relay_session_state=%d\n",
           j.lbl, j.d[0].closed, (unsigned long long)j.d[0].close_code, j.d[0].close_reason,
           j.d[0].ok, j.d[0].objects, j.d[0].reset, (unsigned long long)st.cancelled_down,
           (unsigned long long)st.down_gone, (unsigned long long)st.down_lost,
           (unsigned long long)st.delivered, (unsigned long long)st.aborted,
           (unsigned long long)st.live, (int)moq_session_state(j.d[0].cn->rsess));
    jf_trace_dump(&tr, "core cancel-after-stop");
    /* Relay-side invariants. */
    R_CHECK(&j.rig, st.live == 0 && st.bytes_used == 0 && st.aborted == 0);
    R_CHECK(&j.rig, j.o.cancelled == 0 && j.d[0].foreign == 0);
    /* The core contract under test: the cancelling peer stays established. */
    R_CHECK(&j.rig, j.d[0].closed == 0 && moq_session_state(j.d[0].cn->peer) == MOQ_SESS_ESTABLISHED);
    return relay_bind_test_jf_teardown(&j, "core_cancel_after_stop");
}

/* Queued upstream tail across pumps + immediate request-slot reuse: the
 * origin paces a 24-object response at 4 objects per cycle, and the
 * downstream peer's events are processed BEFORE the origin's in every pump.
 * Two cycles in, the downstream cancels and immediately re-joins (its
 * replacement takes the freed request slot on both sessions). The objects
 * that arrive in the cancel's own pump still name the retired handle and are
 * drained without matching anything; the objects the origin keeps writing
 * afterwards reach a released transaction and are ignored. The replacement
 * is served exactly (24 objects with the SECOND response's bytes, none of
 * the first) and nothing leaks. */
static int
jf_queued_tail_reuse(moq_version_t version)
{
    static jf_trace_t tr;
    memset(&tr, 0, sizeof(tr));
    relay_bind_test_trace_fn = jf_trace_fn;
    relay_bind_test_trace_ctx = &tr;
    jf_t j;
    relay_bind_test_max_events = 64; /* the downstream peer is drained once per cycle */
    if (!relay_bind_test_jf_setup_ex(&j, version, 1, false, 24, true)) return 1 + j.rig.failures;
    relay_bind_test_trace_fn = NULL;
    tr.armed = true;
    relay_bind_test_jf_join(&j, &j.d[0]);
    relay_bind_test_jf_wait_requests(&j, 1);
    relay_bind_test_jf_origin_accept(&j, 0, 0, 25, false, false);
    uint64_t next = 0;
    moqr_bind_fwd_debug_t st;
    uint64_t old_handle = j.d[0].fh._opaque;
    bool cancelled = false;
    for (int cyc = 0; cyc < 12; cyc++) {
        for (int k = 0; k < 4 && next < 24; k++, next++) {
            moq_result_t rc = relay_bind_test_jf_origin_write_raw(&j, 0, 0, next, (uint8_t)(0x30 + next), 8, false);
            /* After the cancel the origin's writes may be refused: its
             * request was retired by the relay's FETCH_CANCEL. */
            R_CHECK(&j.rig, rc == MOQ_OK || cancelled);
        }
        if (cyc == 1) {
            relay_bind_test_jf_stats(&j, &st);
            R_CHECK(&j.rig, st.live == 1 && st.completed == 0 && st.bytes_used > JF_KEY_LEN);
            R_CHECK(&j.rig, moq_session_fetch_cancel(j.d[0].cn->peer, j.d[0].fh, j.rig.now) == MOQ_OK);
            jf_reset_obs(&j.d[0]);
            relay_bind_test_jf_join(&j, &j.d[0]);
            R_CHECK(&j.rig, j.d[0].fh._opaque != old_handle);
            cancelled = true;
        }
        relay_bind_test_jf_cycle(&j);
    }
    relay_bind_test_jf_wait_requests(&j, 2);
    for (int i = 0; i < 4; i++) relay_bind_test_jf_cycle(&j);
    relay_bind_test_jf_stats(&j, &st);
    R_CHECK(&j.rig, st.cancelled_down == 1 && st.cancelled_up == 1 && st.admitted == 2 &&
                    st.live == 1 && st.delivered == 0);
    R_CHECK(&j.rig, j.d[0].ledger_n == 0 && j.d[0].foreign == 0);
    /* The replacement is answered with different bytes, paced the same way
     * (a session queues at most 16 events between pumps). */
    relay_bind_test_jf_origin_accept(&j, 1, 0, 25, false, false);
    for (uint64_t o = 0; o < 24; o++) {
        R_CHECK(&j.rig, relay_bind_test_jf_origin_write_raw(&j, 1, 0, o, (uint8_t)(0x50 + o), 8, false) == MOQ_OK);
        if ((o % 4) == 3) relay_bind_test_jf_cycle(&j);
    }
    relay_bind_test_jf_origin_end(&j, 1);
    for (int i = 0; i < 24 && j.d[0].complete + j.d[0].error + j.d[0].reset == 0; i++) relay_bind_test_jf_cycle(&j);
    for (int i = 0; i < 4; i++) relay_bind_test_jf_cycle(&j);
    R_CHECK(&j.rig, j.d[0].ok == 1 && j.d[0].objects == 24 && j.d[0].complete == 1 &&
                    j.d[0].error == 0 && j.d[0].reset == 0 && j.d[0].foreign == 0);
    if (j.rig.failures) {
        relay_bind_test_jf_stats(&j, &st);
        printf("  queued-tail diag: ok=%d objects=%d complete=%d error=%d code=%llu reset=%d "
               "foreign=%d ledger_n=%d | admitted=%llu completed=%llu errors=%llu overflow=%llu "
               "mismatch=%llu delivered=%llu aborted=%llu live=%llu bytes=%llu requests=%d "
               "origin_cancelled=%d states d=%d o=%d\n",
               j.d[0].ok, j.d[0].objects, j.d[0].complete, j.d[0].error,
               (unsigned long long)j.d[0].error_code, j.d[0].reset, j.d[0].foreign,
               j.d[0].ledger_n, (unsigned long long)st.admitted, (unsigned long long)st.completed,
               (unsigned long long)st.errors, (unsigned long long)st.overflow,
               (unsigned long long)st.mismatch, (unsigned long long)st.delivered,
               (unsigned long long)st.aborted, (unsigned long long)st.live,
               (unsigned long long)st.bytes_used, j.o.fetch_requests, j.o.cancelled,
               (int)moq_session_state(j.d[0].cn->peer), (int)moq_session_state(j.o.cn->peer));
        printf("  queued-tail diag: relay sessions d=%d o=%d lost=%llu stale=%llu drained=%llu "
               "retried_send=%llu retried_cancel=%llu peer_closed d=%d(0x%llx \"%s\")\n",
               (int)moq_session_state(j.d[0].cn->rsess), (int)moq_session_state(j.o.cn->rsess),
               (unsigned long long)st.lost, (unsigned long long)st.stale_events_ignored,
               (unsigned long long)st.drained_events, (unsigned long long)st.retried_send,
               (unsigned long long)st.retried_cancel, j.d[0].closed,
               (unsigned long long)j.d[0].close_code, j.d[0].close_reason);
        jf_trace_dump(&tr, "queued-tail");
    }
    /* obj_* keeps the first 8: ids in order with the SECOND response's bytes. */
    for (int k = 0; k < 8; k++) {
        R_CHECK(&j.rig, j.d[0].obj_g[k] == 0 && j.d[0].obj_o[k] == (uint64_t)k &&
                        j.d[0].obj_len[k] == 8 && j.d[0].obj_first[k] == (uint8_t)(0x50 + k));
    }
    relay_bind_test_jf_stats(&j, &st);
    R_CHECK(&j.rig, st.drained_events > 0);
    R_CHECK(&j.rig, st.delivered == 1 && st.live == 0 && st.bytes_used == 0);
    printf("  queued-tail %s: drained_events=%llu stale_events_ignored=%llu origin_cancelled=%d\n",
           j.lbl, (unsigned long long)st.drained_events,
           (unsigned long long)st.stale_events_ignored, j.o.cancelled);
    return relay_bind_test_jf_teardown(&j, "fwd_queued_tail_slot_reuse");
}

/* WOULD_BLOCK at the upstream request and at the upstream cancel: the relay's
 * session toward the origin has a 3-deep action queue, filled by three
 * upstream SUBSCRIBEs while the origin pair is held; each held pump retries
 * exactly once (one HOLD per pump), and the step commits as soon as the pair
 * moves. */
static int
jf_block_upstream(moq_version_t version, bool cancel)
{
    jf_t j;
    relay_bind_test_max_actions = 3;
    if (!relay_bind_test_jf_setup(&j, version, 1, false, 1)) return 1 + j.rig.failures;
    const int K = 4;
    moqr_bind_fwd_debug_t st;
    if (!cancel) {
        /* Hold the origin pair and fill the relay->origin queue. */
        j.o.cn->frozen = true;
        for (int t = 0; t < 3; t++) {
            moq_subscribe_cfg_t sc;
            memset(&sc, 0, sizeof(sc));
            moq_subscribe_cfg_init(&sc);
            sc.track_namespace = (moq_namespace_t){ .parts = (moq_bytes_t *)relay_bind_test_JF_NS, .count = 2 };
            char nm[8];
            (void)snprintf(nm, sizeof(nm), "f%d", t);
            sc.track_name = (moq_bytes_t){ (const uint8_t *)nm, strlen(nm) };
            sc.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
            moq_subscription_t sh;
            R_CHECK(&j.rig, moq_session_subscribe(j.d[0].cn->peer, &sc, j.rig.now, &sh) == MOQ_OK);
            relay_bind_test_jf_cycle(&j);
        }
        relay_bind_test_jf_join(&j, &j.d[0]);
        for (int i = 0; i < K; i++) relay_bind_test_jf_cycle(&j);
        relay_bind_test_jf_stats(&j, &st);
        R_CHECK(&j.rig, st.admitted == 1 && st.live == 1 && j.o.fetch_requests == 0);
        R_CHECK(&j.rig, st.retried_send == (uint64_t)K);
        j.o.cn->frozen = false;
        relay_bind_test_jf_wait_requests(&j, 1);
        relay_bind_test_jf_stats(&j, &st);
        R_CHECK(&j.rig, st.retried_send == (uint64_t)K);   /* no further holds */
        jf_origin_answer_exact(&j, 0, false);
        jf_wait_terminal(&j, &j.d[0]);
        j.rig.failures += jf_check_exact(&j, &j.d[0], false, "block upstream send");
    } else {
        relay_bind_test_jf_join(&j, &j.d[0]);
        relay_bind_test_jf_wait_requests(&j, 1);
        j.o.cn->frozen = true;
        for (int t = 0; t < 3; t++) {
            moq_subscribe_cfg_t sc;
            memset(&sc, 0, sizeof(sc));
            moq_subscribe_cfg_init(&sc);
            sc.track_namespace = (moq_namespace_t){ .parts = (moq_bytes_t *)relay_bind_test_JF_NS, .count = 2 };
            char nm[8];
            (void)snprintf(nm, sizeof(nm), "g%d", t);
            sc.track_name = (moq_bytes_t){ (const uint8_t *)nm, strlen(nm) };
            sc.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
            moq_subscription_t sh;
            R_CHECK(&j.rig, moq_session_subscribe(j.d[0].cn->peer, &sc, j.rig.now, &sh) == MOQ_OK);
            relay_bind_test_jf_cycle(&j);
        }
        R_CHECK(&j.rig, moq_session_fetch_cancel(j.d[0].cn->peer, j.d[0].fh, j.rig.now) == MOQ_OK);
        for (int i = 0; i < K; i++) relay_bind_test_jf_cycle(&j);
        relay_bind_test_jf_stats(&j, &st);
        R_CHECK(&j.rig, st.cancelled_down == 1 && st.cancelled_up == 0 && st.live == 1);
        R_CHECK(&j.rig, st.retried_cancel == (uint64_t)K);
        j.o.cn->frozen = false;
        for (int i = 0; i < 8 && j.o.cancelled == 0; i++) relay_bind_test_jf_cycle(&j);
        R_CHECK(&j.rig, j.o.cancelled == 1);
        relay_bind_test_jf_stats(&j, &st);
        R_CHECK(&j.rig, st.retried_cancel == (uint64_t)K && st.cancelled_up == 1);
    }
    relay_bind_test_jf_stats(&j, &st);
    R_CHECK(&j.rig, st.live == 0 && st.bytes_used == 0);
    return relay_bind_test_jf_teardown(&j, cancel ? "fwd_block_upstream_cancel" : "fwd_block_upstream_send");
}

/* WOULD_BLOCK on the downstream side (accept / object / FIN / reject / abort
 * steps): the downstream pair's 3-deep action queue is held unstepped. Each
 * held pump retries exactly one step once (holds == pumps), and after the
 * pair moves the outcome is the exact contract (or the exact rejection /
 * the request-local abort). A permanent refusal is observed for 40 pumps
 * with the transaction retained and no further progress. */
static int
jf_block_downstream(moq_version_t version, int mode)
{
    /* mode 0 = success path, 1 = reject path, 2 = abort path, 3 = permanent,
     * 4 = the relay's own INTERNAL_ERROR when the upstream reason could not
     *     be retained */
    static const char *const names[5] = { "fwd_block_downstream_deliver",
                                          "fwd_block_downstream_reject",
                                          "fwd_block_downstream_abort",
                                          "fwd_block_downstream_permanent",
                                          "fwd_block_downstream_reject_local" };
    jf_t j;
    relay_bind_test_max_actions = 3;
    if (!relay_bind_test_jf_setup(&j, version, 1, false, 1)) return 1 + j.rig.failures;
    if (mode == 1 || mode == 2 || mode == 4) {
        /* A one-action step (reject / abort) only blocks against a queue
         * already full: the downstream also follows the live track, whose
         * objects the relay queues toward it while the pair is held. */
        relay_bind_test_jf_subscribe_live(&j, &j.d[0]);
        for (int i = 0; i < 12 && !(j.d[0].live_ok && j.o.live_seen); i++) relay_bind_test_jf_cycle(&j);
        R_CHECK(&j.rig, j.d[0].live_ok && j.o.live_seen);
    }
    relay_bind_test_jf_join(&j, &j.d[0]);
    relay_bind_test_jf_wait_requests(&j, 1);
    j.d[0].cn->frozen = true;
    if (mode == 1 || mode == 2 || mode == 4) {
        for (uint64_t g = 1; g <= 4; g++) {
            moq_subgroup_cfg_t sgc;
            moq_subgroup_cfg_init(&sgc);
            sgc.group_id = g;
            sgc.subgroup_id = 0;
            sgc.publisher_priority = 100;
            moq_subgroup_handle_t sgh;
            R_CHECK(&j.rig, moq_session_open_subgroup(j.o.cn->peer, j.o.live_sub, &sgc,
                                                      j.rig.now, &sgh) == MOQ_OK);
            uint8_t body[8];
            memset(body, (int)g, sizeof(body));
            moq_rcbuf_t *pl = NULL;
            R_CHECK(&j.rig, moq_rcbuf_create(&j.a.vt, body, sizeof(body), &pl) == MOQ_OK);
            R_CHECK(&j.rig, moq_session_write_object(j.o.cn->peer, sgh, 0, pl, j.rig.now) == MOQ_OK);
            moq_rcbuf_decref(pl);
            R_CHECK(&j.rig, moq_session_close_subgroup(j.o.cn->peer, sgh, j.rig.now) == MOQ_OK);
            relay_bind_test_jf_cycle(&j);
        }
    }
    if (mode == 2) {
        moqr_bind_debug_fail_fwd_write(1);
    }
    if (mode == 1) {
        jf_origin_reject(&j, 0, MOQ_REQUEST_ERROR_DOES_NOT_EXIST, "gone", false, 0);
    } else if (mode == 4) {
        j.a.fail_size = 9;   /* "forbidden": the reason copy is refused */
        jf_origin_reject(&j, 0, MOQ_REQUEST_ERROR_UNAUTHORIZED, "forbidden", true, 250);
    } else {
        jf_origin_answer_exact(&j, 0, false);
    }
    moqr_bind_fwd_debug_t st;
    /* Let the upstream side settle, counting the pumps the downstream step
     * is held. The first held pump is the one where the upstream terminal
     * arrived; from then on each pump holds exactly once. */
    const int K = mode == 3 ? 40 : 5;
    int pumps_after_terminal = 0;
    for (int i = 0; i < 12 + K; i++) {
        relay_bind_test_jf_cycle(&j);
        relay_bind_test_jf_stats(&j, &st);
        if (st.completed + st.errors == 1) {
            pumps_after_terminal++;
        }
        if (pumps_after_terminal == K) break;
    }
    relay_bind_test_jf_stats(&j, &st);
    uint64_t holds = st.retried_accept + st.retried_write + st.retried_fin +
                     st.retried_reject + st.retried_abort;
    R_CHECK(&j.rig, pumps_after_terminal == K);
    R_CHECK(&j.rig, st.live == 1 && st.delivered == 0 && st.rejected_down == 0 &&
                    st.aborted == 0);
    R_CHECK(&j.rig, holds >= (uint64_t)K - 1 && holds <= (uint64_t)K);
    if (mode == 1 || mode == 4) {
        R_CHECK(&j.rig, st.retried_reject == holds);
    }
    j.a.fail_size = 0;
    if (mode == 3) {
        /* Permanent: the downstream goes away still blocked; cleanup to zero
         * without any write and without touching the origin. */
        R_CHECK(&j.rig, moqr_bind_conn_close(j.rig.bind, j.d[0].cn->rsess) == MOQR_OK);
        for (int i = 0; i < 4; i++) relay_bind_test_jf_cycle(&j);
        relay_bind_test_jf_stats(&j, &st);
        R_CHECK(&j.rig, st.live == 0 && st.bytes_used == 0 && st.delivered == 0 &&
                        j.o.cancelled == 0);
        printf("  block permanent %s: holds=%llu over %d pumps\n", j.lbl,
               (unsigned long long)holds, K);
        moqr_bind_debug_fail_fwd_write(0);
        return relay_bind_test_jf_teardown(&j, names[mode]);
    }
    j.d[0].cn->frozen = false;
    jf_wait_terminal(&j, &j.d[0]);
    for (int i = 0; i < 4; i++) relay_bind_test_jf_cycle(&j);
    relay_bind_test_jf_stats(&j, &st);
    uint64_t holds_after = st.retried_accept + st.retried_write + st.retried_fin +
                           st.retried_reject + st.retried_abort;
    if (mode == 0) {
        j.rig.failures += jf_check_exact(&j, &j.d[0], false, "block downstream deliver");
        R_CHECK(&j.rig, st.delivered == 1);
    } else if (mode == 1) {
        j.rig.failures += jf_check_rejected(&j, &j.d[0], MOQ_REQUEST_ERROR_DOES_NOT_EXIST,
                                            "block downstream reject");
        R_CHECK(&j.rig, st.rejected_down == 1 && j.d[0].reason_len == 4 &&
                        memcmp(j.d[0].reason, "gone", 4) == 0);
    } else if (mode == 4) {
        j.rig.failures += jf_check_rejected(&j, &j.d[0], MOQ_REQUEST_ERROR_INTERNAL_ERROR,
                                            "block downstream reject local");
        R_CHECK(&j.rig, st.rejected_down == 1 && st.reason_unretained == 1 &&
                        j.d[0].reason_len == 0 && !j.d[0].can_retry);
    } else {
        /* OK then the request-local abort: the downstream sees FETCH_OK and
         * a RESET terminal carrying INTERNAL_ERROR (0x0), no object. */
        R_CHECK(&j.rig, st.aborted == 1 && st.delivered == 0);
        R_CHECK(&j.rig, j.d[0].ok == 1 && j.d[0].reset == 1 && j.d[0].reset_code == 0 &&
                        j.d[0].objects == 0 && j.d[0].complete == 0 && j.d[0].error == 0 &&
                        j.d[0].foreign == 0);
        R_CHECK(&j.rig, st.retried_abort + st.retried_accept == holds_after);
    }
    /* Once the pair moves, at most the holds already counted plus the pumps
     * before the step committed; no busy continuation. */
    R_CHECK(&j.rig, holds_after - holds <= 2);
    R_CHECK(&j.rig, st.live == 0 && st.bytes_used == 0);
    printf("  block downstream %s mode=%d: holds=%llu (accept=%llu write=%llu fin=%llu "
           "reject=%llu abort=%llu) after=%llu\n", j.lbl, mode, (unsigned long long)holds,
           (unsigned long long)st.retried_accept, (unsigned long long)st.retried_write,
           (unsigned long long)st.retried_fin, (unsigned long long)st.retried_reject,
           (unsigned long long)st.retried_abort, (unsigned long long)holds_after);
    moqr_bind_debug_fail_fwd_write(0);
    return relay_bind_test_jf_teardown(&j, names[mode]);
}

/* Request-local abort without any refusal: the first forwarded write fails
 * locally after OK; the downstream sees OK then RESET(0x0); the origin is
 * untouched; no connection close. */
static int
jf_abort_after_ok(moq_version_t version)
{
    jf_t j;
    if (!relay_bind_test_jf_setup(&j, version, 1, true, 1)) return 1 + j.rig.failures;
    relay_bind_test_jf_join(&j, &j.d[0]);
    relay_bind_test_jf_wait_requests(&j, 1);
    moqr_bind_debug_fail_fwd_write(1);
    jf_origin_answer_exact(&j, 0, false);
    jf_wait_terminal(&j, &j.d[0]);
    for (int i = 0; i < 4; i++) relay_bind_test_jf_cycle(&j);
    moqr_bind_debug_fail_fwd_write(0);
    moqr_bind_fwd_debug_t st;
    relay_bind_test_jf_stats(&j, &st);
    R_CHECK(&j.rig, st.aborted == 1 && st.delivered == 0 && st.live == 0 && st.bytes_used == 0);
    R_CHECK(&j.rig, j.d[0].ok == 1 && j.d[0].reset == 1 && j.d[0].reset_code == 0 &&
                    j.d[0].objects == 0 && j.d[0].complete == 0 && j.d[0].error == 0);
    R_CHECK(&j.rig, j.o.cancelled == 0);
    R_CHECK(&j.rig, moq_session_state(j.d[0].cn->peer) == MOQ_SESS_ESTABLISHED);
    relay_bind_test_jf_live_control(&j);
    return relay_bind_test_jf_teardown(&j, "fwd_abort_after_ok");
}

/* Authorization on the RESOLVED identity, fail-closed: a joining request is
 * first authorized on its own empty identity (which admits nothing) and then,
 * once resolved, on the joined track's namespace/name. The hook allows the
 * former and denies the latter: the request is rejected with the hook's code
 * and never reaches the origin. */
typedef struct jf_auth {
    int    fetch_calls;       /* MOQR_AUTH_FETCH evaluations               */
    int    empty_calls;       /* ... on the empty identity (pre-resolution) */
    int    resolved_calls;    /* ... on the resolved boot/cat + catalog     */
    size_t ns_count, name_len;
} jf_auth_t;

static void
jf_auth_hook(void *ctx, const moqr_auth_request_t *req, moqr_auth_verdict_t *out)
{
    jf_auth_t *a = (jf_auth_t *)ctx;
    out->decision = MOQR_AUTH_ALLOW;
    out->reason = MOQR_AUTH_REASON_OK;
    if (req->action != MOQR_AUTH_FETCH) {
        return;
    }
    a->fetch_calls++;
    a->ns_count = req->ns.count;
    a->name_len = req->name.len;
    if (req->ns.count == 0 && req->name.len == 0) {
        a->empty_calls++;     /* the request's own (empty) joining identity */
        return;               /* allowed: it admits nothing by itself */
    }
    bool ns_exact = req->ns.count == 2 && req->ns.parts[0].len == 4 &&
                    memcmp(req->ns.parts[0].data, "boot", 4) == 0 &&
                    req->ns.parts[1].len == 3 && memcmp(req->ns.parts[1].data, "cat", 3) == 0;
    bool name_exact = req->name.len == 7 && memcmp(req->name.data, "catalog", 7) == 0;
    if (ns_exact && name_exact) {
        a->resolved_calls++;
    }
    out->decision = MOQR_AUTH_DENY;
    out->reason = MOQR_AUTH_REASON_UNSCOPED;
    out->error_code = MOQ_REQUEST_ERROR_UNAUTHORIZED;
}

static int
jf_auth_fail_closed(moq_version_t version)
{
    jf_auth_t au;
    memset(&au, 0, sizeof(au));
    relay_bind_test_authorize = jf_auth_hook;
    relay_bind_test_authorize_ctx = &au;
    jf_t j;
    if (!relay_bind_test_jf_setup(&j, version, 1, true, 1)) return 1 + j.rig.failures;
    relay_bind_test_jf_join(&j, &j.d[0]);
    jf_wait_terminal(&j, &j.d[0]);
    j.rig.failures += jf_check_rejected(&j, &j.d[0], MOQ_REQUEST_ERROR_UNAUTHORIZED,
                                        "auth fail-closed");
    /* Two evaluations: the empty pre-resolution identity (allowed) and the
     * resolved track identity (denied) -- the denial is what the downstream
     * sees, and the resolved call carried exactly boot/cat + catalog. */
    R_CHECK(&j.rig, au.fetch_calls == 2 && au.empty_calls == 1 && au.resolved_calls == 1);
    if (!(au.fetch_calls == 2 && au.empty_calls == 1 && au.resolved_calls == 1)) {
        printf("  auth hook saw: calls=%d empty=%d resolved=%d last ns_count=%zu name_len=%zu\n",
               au.fetch_calls, au.empty_calls, au.resolved_calls, au.ns_count, au.name_len);
    }
    R_CHECK(&j.rig, j.o.fetch_requests == 0);
    moqr_bind_fwd_debug_t st;
    relay_bind_test_jf_stats(&j, &st);
    R_CHECK(&j.rig, st.admitted == 0 && st.live == 0 && st.bytes_used == 0);
    relay_bind_test_jf_live_control(&j);
    return relay_bind_test_jf_teardown(&j, "fwd_auth_fail_closed");
}

/* Transaction-count exhaustion: the pool follows the core's max_fetches (2);
 * a third concurrent join is rejected before OK while the origin still holds
 * the first two, which then complete exactly; live traffic unaffected. */
static int
jf_slot_exhaustion(moq_version_t version)
{
    relay_bind_test_max_fetches = 2;
    jf_t j;
    if (!relay_bind_test_jf_setup(&j, version, 3, true, 1)) return 1 + j.rig.failures;
    for (int i = 0; i < 3; i++) relay_bind_test_jf_join(&j, &j.d[i]);
    relay_bind_test_jf_wait_requests(&j, 2);
    for (int i = 0; i < 4; i++) relay_bind_test_jf_cycle(&j);
    moqr_bind_fwd_debug_t st;
    relay_bind_test_jf_stats(&j, &st);
    R_CHECK(&j.rig, st.admitted == 2 && st.rejected_no_slot == 1 && st.live == 2);
    int rejected = -1;
    for (int i = 0; i < 3; i++) {
        if (j.d[i].error == 1) rejected = i;
    }
    R_CHECK(&j.rig, rejected >= 0);
    if (rejected >= 0) {
        /* Capacity refusal: d18 EXCESSIVE_LOAD, d16 INTERNAL_ERROR. */
        j.rig.failures += jf_check_rejected(&j, &j.d[rejected],
                                            version == MOQ_VERSION_DRAFT_18
                                                ? MOQ_REQUEST_ERROR_EXCESSIVE_LOAD
                                                : MOQ_REQUEST_ERROR_INTERNAL_ERROR,
                                            "slot exhaustion");
    }
    jf_origin_answer_exact(&j, 0, false);
    jf_origin_answer_exact(&j, 1, false);
    for (int i = 0; i < 3; i++) {
        if (i != rejected) {
            jf_wait_terminal(&j, &j.d[i]);
            j.rig.failures += jf_check_exact(&j, &j.d[i], false, "slot exhaustion (served)");
        }
    }
    relay_bind_test_jf_stats(&j, &st);
    R_CHECK(&j.rig, st.delivered == 2 && st.live == 0 && st.bytes_used == 0 &&
                    j.o.fetch_requests == 2);
    relay_bind_test_jf_live_control(&j);
    return relay_bind_test_jf_teardown(&j, "fwd_slot_exhaustion");
}

/* Unsupported shape stays explicitly rejected: a DESCENDING joining fetch is
 * answered NOT_SUPPORTED without admission or an upstream request. */
static int
jf_descending_rejected(moq_version_t version)
{
    jf_t j;
    if (!relay_bind_test_jf_setup(&j, version, 1, false, 1)) return 1 + j.rig.failures;
    moq_result_t rc = relay_bind_test_jf_join_ex(&j, &j.d[0], MOQ_GROUP_ORDER_DESCENDING);
    if (version == MOQ_VERSION_DRAFT_18) {
        /* draft-18's fetch data plane is ascending-only: the requesting
         * session refuses up front, nothing reaches the relay. */
        R_CHECK(&j.rig, rc == MOQ_ERR_INVAL);
    } else {
        R_CHECK(&j.rig, rc == MOQ_OK);
        jf_wait_terminal(&j, &j.d[0]);
        j.rig.failures += jf_check_rejected(&j, &j.d[0], MOQ_REQUEST_ERROR_NOT_SUPPORTED,
                                            "descending");
    }
    moqr_bind_fwd_debug_t st;
    relay_bind_test_jf_stats(&j, &st);
    R_CHECK(&j.rig, st.admitted == 0 && j.o.fetch_requests == 0);
    return relay_bind_test_jf_teardown(&j, "fwd_descending_not_supported");
}

/* OOM at every allocation attempt the exact single-receiver flow makes from
 * the join to the terminal (the counting allocator serves the relay binding,
 * the core AND every session, so each index fails one of them). The
 * admissible outcomes, judged against what the origin actually managed to
 * send: the response relayed exactly as sent (OK + every object the origin
 * wrote + COMPLETE); a rejection before OK; no answer because the origin
 * never completed its response (then a downstream cancel must clean the
 * transaction up); or a session that failed under its own allocation. Never
 * an OK followed by fewer objects than were sent while both sessions are
 * still established. Transactions, pool bytes and the allocator return to
 * zero every time. */
static int
jf_oom_sweep(moq_version_t version)
{
    int failures = 0;
    long base = 0, span = 0;
    {
        jf_t j;
        if (!relay_bind_test_jf_setup(&j, version, 1, false, 1)) return 1 + j.rig.failures;
        base = j.a.attempts;
        relay_bind_test_jf_join(&j, &j.d[0]);
        relay_bind_test_jf_wait_requests(&j, 1);
        jf_origin_answer_exact(&j, 0, true);
        jf_wait_terminal(&j, &j.d[0]);
        for (int i = 0; i < 4; i++) relay_bind_test_jf_cycle(&j);
        span = j.a.attempts - base;
        failures += jf_check_exact(&j, &j.d[0], true, "oom sweep baseline");
        failures += relay_bind_test_jf_teardown(&j, "fwd_oom_sweep_baseline");
    }
    int exact = 0, rejected = 0, unanswered = 0, closed = 0, bind_oom = 0;
    int stalled = 0, delivered_lost = 0;
    for (long k = 1; k <= span; k++) {
        jf_t j;
        if (!relay_bind_test_jf_setup(&j, version, 1, false, 1)) { failures++; break; }
        if (j.a.attempts != base) {
            printf("FAIL: oom sweep %s: setup attempts %ld != %ld\n", j.lbl, j.a.attempts, base);
            failures++;
        }
        j.a.fail_at = base + k;
        moq_result_t jrc = relay_bind_test_jf_join_ex(&j, &j.d[0], MOQ_GROUP_ORDER_ASCENDING);
        moq_result_t acc = MOQ_ERR_INVAL, w0 = MOQ_ERR_INVAL, w1 = MOQ_ERR_INVAL,
                     fin = MOQ_ERR_INVAL;
        if (jrc == MOQ_OK) {
            for (int i = 0; i < 12 && j.o.fetch_requests < 1; i++) relay_bind_test_jf_cycle(&j);
            if (j.o.fetch_requests >= 1) {
                moq_accept_fetch_cfg_t ac;
                moq_accept_fetch_cfg_init(&ac);
                ac.end_group = 0;
                ac.end_object = 2;
                JF_ORIGIN_RETRY(&j, moq_session_accept_fetch(j.o.cn->peer, j.o.req[0], &ac, j.rig.now), acc);
                if (acc == MOQ_OK) {
                    w0 = relay_bind_test_jf_origin_write_raw(&j, 0, 0, 0, 0xB0, BOOT_BASE_LEN, false);
                    w1 = relay_bind_test_jf_origin_write_raw(&j, 0, 0, 1, 0xD0, BOOT_DELTA_LEN, true);
                    JF_ORIGIN_RETRY(&j, moq_session_end_fetch(j.o.cn->peer, j.o.req[0], j.rig.now), fin);
                }
            }
            jf_wait_terminal(&j, &j.d[0]);
        }
        for (int i = 0; i < 8; i++) relay_bind_test_jf_cycle(&j);
        j.a.fail_at = 0;
        const jf_down_t *d = &j.d[0];
        moqr_bind_fwd_debug_t st;
        relay_bind_test_jf_stats(&j, &st);
        bool sessions_up = moq_session_state(d->cn->peer) == MOQ_SESS_ESTABLISHED &&
                           moq_session_state(j.o.cn->peer) == MOQ_SESS_ESTABLISHED &&
                           d->done == 0;
        int sent = (w0 == MOQ_OK ? 1 : 0) + (w1 == MOQ_OK ? 1 : 0);
        bool origin_done = acc == MOQ_OK && fin == MOQ_OK;
        bool is_exact = origin_done && d->ok == 1 && d->objects == sent && d->complete == 1 &&
                        d->error == 0 && d->reset == 0 &&
                        (sent < 1 || d->obj_exact[0]) &&
                        (sent < 2 || (d->obj_exact[1] && d->props_exact[1]));
        bool is_rejected = d->ok == 0 && d->objects == 0 && d->complete == 0 && d->error == 1;
        bool is_unanswered = !origin_done && d->ledger_n == 0 && st.live == 1;
        /* The simulated link drops a segment whose delay entry could not be
         * allocated: the response never completes upstream (open
         * transaction, still cancellable) or, once the binding wrote and
         * finished it (delivered), never completes downstream. */
        bool is_stalled = origin_done && sessions_up && d->ledger_n == 0 && st.live == 1 &&
                          st.completed == 0 && st.errors + st.resets + st.lost == 0;
        bool is_delivered_lost = origin_done && sessions_up && st.delivered == 1 &&
                                 st.aborted == 0 && d->ok == 1 && d->complete == 0 &&
                                 d->error == 0 && d->reset == 0;
        if (st.rejected_oom + st.overflow > 0) bind_oom++;
        if (is_exact) {
            exact++;
        } else if (is_rejected) {
            rejected++;
        } else if (is_unanswered || is_stalled) {
            /* The origin never finished (or its response never arrived):
             * the transaction is rightly still open. A downstream cancel
             * must release it completely. */
            if (is_unanswered) unanswered++; else stalled++;
            (void)moq_session_fetch_cancel(d->cn->peer, d->fh, j.rig.now);
            for (int i = 0; i < 8; i++) relay_bind_test_jf_cycle(&j);
            relay_bind_test_jf_stats(&j, &st);
        } else if (is_delivered_lost) {
            delivered_lost++;
        } else if (!sessions_up || jrc != MOQ_OK) {
            closed++;
        } else {
            printf("FAIL: oom sweep %s fail_at=%ld: ok=%d objects=%d complete=%d error=%d reset=%d "
                   "ledger_n=%d | origin acc=%d w0=%d w1=%d fin=%d | admitted=%llu completed=%llu "
                   "errors=%llu lost=%llu overflow=%llu delivered=%llu aborted=%llu live=%llu\n",
                   j.lbl, k, d->ok, d->objects, d->complete, d->error, d->reset, d->ledger_n,
                   (int)acc, (int)w0, (int)w1, (int)fin, (unsigned long long)st.admitted,
                   (unsigned long long)st.completed, (unsigned long long)st.errors,
                   (unsigned long long)st.lost, (unsigned long long)st.overflow,
                   (unsigned long long)st.delivered, (unsigned long long)st.aborted,
                   (unsigned long long)st.live);
            failures++;
        }
        if (st.live != 0 || st.bytes_used != 0) {
            printf("FAIL: oom sweep %s fail_at=%ld: live=%llu bytes_used=%llu\n", j.lbl, k,
                   (unsigned long long)st.live, (unsigned long long)st.bytes_used);
            failures++;
        }
        j.rig.failures = 0;   /* per-step R_CHECKs are reported through the classes */
        relay_bind_test_rig_destroy(&j.rig);
        if (j.a.live != 0) {
            printf("FAIL: oom sweep %s fail_at=%ld leaked %ld bytes\n", j.lbl, k, j.a.live);
            failures++;
        }
    }
    relay_bind_test_fetch_forward_bytes = 0;
    printf("  oom sweep %s: span=%ld exact=%d rejected=%d unanswered=%d stalled=%d "
           "delivered_lost=%d closed=%d (binding-side refusals observed in %d)\n",
           version == MOQ_VERSION_DRAFT_16 ? "v16" : "v18", span, exact, rejected, unanswered,
           stalled, delivered_lost, closed, bind_oom);
    if (failures == 0) {
        printf("PASS: fwd_oom_sweep %s\n", version == MOQ_VERSION_DRAFT_16 ? "v16" : "v18");
    }
    return failures;
}

/* The binding's OWN copies, each failed exactly once by size (one-shot):
 * the owned key at admission, the upstream Track Properties at FETCH_OK,
 * the error reason at FETCH_ERROR, and the upstream-subscription key copy
 * at SUBSCRIBE time. Each is a rejection before OK (or, for the reason, a
 * preserved error without its text), with cleanup to zero. */
static int
jf_oom_owned_copies(moq_version_t version)
{
    int failures = 0;
    /* (a) key copy at admission: no transaction, no upstream request. */
    {
        jf_t j;
        if (!relay_bind_test_jf_setup(&j, version, 1, true, 1)) return 1 + j.rig.failures;
        j.a.fail_size = JF_KEY_LEN;
        relay_bind_test_jf_join(&j, &j.d[0]);
        jf_wait_terminal(&j, &j.d[0]);
        j.a.fail_size = 0;
        j.rig.failures += jf_check_rejected(&j, &j.d[0], MOQ_REQUEST_ERROR_INTERNAL_ERROR,
                                            "oom key copy");
        moqr_bind_fwd_debug_t st;
        relay_bind_test_jf_stats(&j, &st);
        R_CHECK(&j.rig, st.rejected_oom == 1 && st.admitted == 0 && st.live == 0 &&
                        st.bytes_used == 0 && j.o.fetch_requests == 0);
        relay_bind_test_jf_live_control(&j);
        failures += relay_bind_test_jf_teardown(&j, "fwd_oom_key_copy");
    }
    /* (b) Track Properties copy at FETCH_OK (draft-18 carries them): the
     *     collection is abandoned before OK and cancelled upstream. */
    if (version == MOQ_VERSION_DRAFT_18) {
        jf_t j;
        if (!relay_bind_test_jf_setup(&j, version, 1, true, 1)) return 1 + j.rig.failures;
        relay_bind_test_jf_join(&j, &j.d[0]);
        relay_bind_test_jf_wait_requests(&j, 1);
        j.a.fail_size = 10;   /* the 10-byte Track Properties block */
        relay_bind_test_jf_origin_accept(&j, 0, 0, 2, false, true);
        jf_origin_write_generation(&j, 0, false);
        relay_bind_test_jf_origin_end(&j, 0);
        jf_wait_terminal(&j, &j.d[0]);
        j.a.fail_size = 0;
        j.rig.failures += jf_check_rejected(&j, &j.d[0], MOQ_REQUEST_ERROR_INTERNAL_ERROR,
                                            "oom props copy");
        for (int i = 0; i < 6 && j.o.cancelled == 0; i++) relay_bind_test_jf_cycle(&j);
        moqr_bind_fwd_debug_t st;
        relay_bind_test_jf_stats(&j, &st);
        R_CHECK(&j.rig, st.overflow == 1 && st.cancelled_up + st.completed == 1 &&
                        st.live == 0 && st.bytes_used == 0 && st.delivered == 0);
        relay_bind_test_jf_live_control(&j);
        failures += relay_bind_test_jf_teardown(&j, "fwd_oom_props_copy");
    }
    /* (c) reason copy at FETCH_ERROR refused by the allocator, and (c')
     *     refused by the aggregate budget: the complete upstream error
     *     cannot be retained, so the downstream gets the relay's OWN
     *     INTERNAL_ERROR with no retry metadata and no text -- never the
     *     upstream code with its text silently removed. The upstream ERROR
     *     already ended the request: no cancel is attempted. */
    for (int variant = 0; variant < 2; variant++) {
        if (variant == 1) {
            relay_bind_test_fetch_forward_bytes = JF_KEY_LEN + 4; /* key fits, reason does not */
        }
        jf_t j;
        if (!relay_bind_test_jf_setup(&j, version, 1, true, 1)) return 1 + j.rig.failures;
        relay_bind_test_jf_join(&j, &j.d[0]);
        relay_bind_test_jf_wait_requests(&j, 1);
        if (variant == 0) {
            j.a.fail_size = 9;   /* "forbidden" */
        }
        jf_origin_reject(&j, 0, MOQ_REQUEST_ERROR_UNAUTHORIZED, "forbidden", true, 250);
        jf_wait_terminal(&j, &j.d[0]);
        j.a.fail_size = 0;
        j.rig.failures += jf_check_rejected(&j, &j.d[0], MOQ_REQUEST_ERROR_INTERNAL_ERROR,
                                            variant == 0 ? "reason copy refused (allocator)"
                                                         : "reason copy refused (budget)");
        R_CHECK(&j.rig, j.d[0].reason_len == 0 && !j.d[0].can_retry);
        moqr_bind_fwd_debug_t st;
        relay_bind_test_jf_stats(&j, &st);
        R_CHECK(&j.rig, st.errors == 1 && st.reason_unretained == 1 && st.rejected_down == 1 &&
                        st.cancelled_up == 0 && st.live == 0 && st.bytes_used == 0);
        R_CHECK(&j.rig, j.o.cancelled == 0);
        relay_bind_test_jf_live_control(&j);
        failures += relay_bind_test_jf_teardown(&j, variant == 0 ? "fwd_reason_unretained_allocator"
                                                 : "fwd_reason_unretained_budget");
    }
    /* (d) the upstream-subscription key copy at SUBSCRIBE time: the
     *     subscription still serves live delivery, but a join through it
     *     finds no forwarding source (DOES_NOT_EXIST), no upstream request. */
    {
        jf_t j;
        memset(&j, 0, sizeof(j));
        relay_bind_test_ca_init(&j.a);
        j.version = version;
        j.lbl = version == MOQ_VERSION_DRAFT_16 ? "v16" : "v18";
        if (relay_bind_test_rig_create(&j.rig, &j.a) != MOQR_OK) return failures + 1;
        j.o.cn = relay_bind_test_rig_connect(&j.rig, version);
        j.o.largest_object = 1;
        j.d[0].cn = relay_bind_test_rig_connect(&j.rig, version);
        j.n_down = 1;
        R_CHECK(&j.rig, j.o.cn != NULL && j.d[0].cn != NULL);
        relay_bind_test_rig_pump(&j.rig, 4);
        moq_publish_namespace_cfg_t pcfg;
        memset(&pcfg, 0, sizeof(pcfg));
        moq_publish_namespace_cfg_init(&pcfg);
        pcfg.track_namespace = (moq_namespace_t){ .parts = (moq_bytes_t *)relay_bind_test_JF_NS, .count = 2 };
        moq_announcement_t ann;
        R_CHECK(&j.rig, moq_session_publish_namespace(j.o.cn->peer, &pcfg, j.rig.now, &ann) == MOQ_OK);
        for (int i = 0; i < 8 && j.o.ns_accepted == 0; i++) relay_bind_test_jf_cycle(&j);
        j.a.fail_size = JF_KEY_LEN;   /* the usub key copy is the first 14-byte alloc */
        relay_bind_test_jf_subscribe_catalog(&j, &j.d[0]);
        for (int i = 0; i < 16 && !j.d[0].sub_ok; i++) relay_bind_test_jf_cycle(&j);
        j.a.fail_size = 0;
        R_CHECK(&j.rig, j.d[0].sub_ok && j.o.cat_seen);
        relay_bind_test_jf_join(&j, &j.d[0]);
        jf_wait_terminal(&j, &j.d[0]);
        j.rig.failures += jf_check_rejected(&j, &j.d[0], MOQ_REQUEST_ERROR_DOES_NOT_EXIST,
                                            "oom usub key");
        moqr_bind_fwd_debug_t st;
        relay_bind_test_jf_stats(&j, &st);
        R_CHECK(&j.rig, st.rejected_no_upstream == 1 && st.admitted == 0 && st.live == 0 &&
                        j.o.fetch_requests == 0);
        failures += relay_bind_test_jf_teardown(&j, "fwd_oom_usub_key");
    }
    return failures;
}

/* Admission refusals under downstream backpressure. The downstream pair's
 * sessions hold a 3-deep action queue. The peer's three PUBLISH_NAMESPACEs
 * and then its joining fetch are delivered with transport-only steps (no
 * bind pump), so the ONE pump that processes them fills the relay's queue
 * with the three acceptances before it reaches the FETCH_REQUEST: the
 * refusal's moq_session_reject_fetch returns WOULD_BLOCK (observed through
 * the refusal gauge) and the request stays pending in the session. The
 * binding must own that refusal: after the queue drains exactly one correct
 * rejection reaches the peer, no upstream FETCH is sent, nothing is stranded,
 * no session closes, and unrelated traffic still flows; a cancel or a
 * connection teardown while the refusal is held releases it without a
 * rejection. Refusal kinds: authorization denial, unsupported shape
 * (descending, draft-16), no forwarding source, transaction-pool exhaustion,
 * key-copy allocation failure, unresolved join (source retired at the
 * binding while the peer's subscription was still live). */
enum { JR_AUTH, JR_DESCENDING, JR_NO_SOURCE, JR_POOL, JR_KEY_OOM, JR_UNRESOLVED, JR_COUNT };
enum { JR_RELEASE, JR_CANCEL, JR_TEARDOWN };

static const char *const jr_names[JR_COUNT] = {
    "fwd_refusal_held_auth", "fwd_refusal_held_descending", "fwd_refusal_held_no_source",
    "fwd_refusal_held_pool", "fwd_refusal_held_key_oom", "fwd_refusal_held_unresolved"
};

static int
jf_refusal_held(moq_version_t version, int mode, int after)
{
    if (mode == JR_DESCENDING && version == MOQ_VERSION_DRAFT_18) {
        return 0;   /* the requesting session refuses descending locally */
    }
    jf_auth_t au;
    memset(&au, 0, sizeof(au));
    if (mode == JR_AUTH) {
        relay_bind_test_authorize = jf_auth_hook;
        relay_bind_test_authorize_ctx = &au;
    }
    if (mode == JR_POOL) {
        relay_bind_test_max_fetches = 2;
    }
    if (mode == JR_NO_SOURCE) {
        relay_bind_test_jf_setup_usub_key_oom = true;
    }
    relay_bind_test_max_actions = 3;
    jf_t j;
    int n_down = mode == JR_POOL ? 3 : 1;
    if (!relay_bind_test_jf_setup(&j, version, n_down, true, 1)) return 1 + j.rig.failures;
    jf_down_t *d = &j.d[n_down - 1];
    char label[96];
    (void)snprintf(label, sizeof(label), "%s/%s", jr_names[mode],
                   after == JR_RELEASE ? "release" : after == JR_CANCEL ? "cancel" : "teardown");
    if (mode == JR_POOL) {
        relay_bind_test_jf_join(&j, &j.d[0]);
        relay_bind_test_jf_join(&j, &j.d[1]);
        relay_bind_test_jf_wait_requests(&j, 2); /* the origin holds both */
    }
    /* Three announces from the peer, delivered without a pump. */
    static const moq_bytes_t JR_H = { (const uint8_t *)"h", 1 };
    const char *tails[3] = { "t0", "t1", "t2" };
    for (int t = 0; t < 3; t++) {
        moq_bytes_t parts[2] = { JR_H, { (const uint8_t *)tails[t], 2 } };
        moq_publish_namespace_cfg_t pcfg;
        memset(&pcfg, 0, sizeof(pcfg));
        moq_publish_namespace_cfg_init(&pcfg);
        pcfg.track_namespace = (moq_namespace_t){ .parts = parts, .count = 2 };
        moq_announcement_t ann;
        R_CHECK(&j.rig, moq_session_publish_namespace(d->cn->peer, &pcfg, j.rig.now, &ann) == MOQ_OK);
    }
    relay_bind_test_rig_step_pair(&j.rig, d->cn);
    /* The join, delivered without a pump; mode-specific pre-pump state. */
    if (mode == JR_DESCENDING) {
        R_CHECK(&j.rig, relay_bind_test_jf_join_ex(&j, d, MOQ_GROUP_ORDER_DESCENDING) == MOQ_OK);
    } else {
        relay_bind_test_jf_join(&j, d);
    }
    relay_bind_test_rig_step_pair(&j.rig, d->cn);
    if (mode == JR_UNRESOLVED) {
        R_CHECK(&j.rig, moqr_bind_conn_close(j.rig.bind, j.o.cn->rsess) == MOQR_OK);
        j.o.gone = true;
    }
    if (mode == JR_KEY_OOM) {
        j.a.fail_size = JF_KEY_LEN;
    }
    relay_bind_test_rig_bind_pump_once(&j.rig);
    j.a.fail_size = 0;
    moqr_bind_fwd_debug_t st;
    relay_bind_test_jf_stats(&j, &st);
    relay_bind_test_jf_down_drain(&j.rig, d);
    /* The refusal met WOULD_BLOCK and is held; nothing reached the peer. */
    R_CHECK(&j.rig, st.refusals_blocked == 1 && st.refusals_sent == 0);
    R_CHECK(&j.rig, d->ledger_n == 0 && d->foreign == 0);
    R_CHECK(&j.rig, st.admitted == (mode == JR_POOL ? 2u : 0u));
    /* Transaction-pool exhaustion is a capacity refusal: d18 EXCESSIVE_LOAD,
     * d16 INTERNAL_ERROR (its registry has no load code). */
    uint64_t pool_code = version == MOQ_VERSION_DRAFT_18
                             ? MOQ_REQUEST_ERROR_EXCESSIVE_LOAD
                             : MOQ_REQUEST_ERROR_INTERNAL_ERROR;
    uint64_t code = mode == JR_AUTH ? MOQ_REQUEST_ERROR_UNAUTHORIZED
                  : mode == JR_DESCENDING ? MOQ_REQUEST_ERROR_NOT_SUPPORTED
                  : mode == JR_NO_SOURCE ? MOQ_REQUEST_ERROR_DOES_NOT_EXIST
                  : mode == JR_UNRESOLVED ? MOQ_REQUEST_ERROR_INVALID_JOINING_REQUEST_ID
                  : mode == JR_POOL ? pool_code
                  : MOQ_REQUEST_ERROR_INTERNAL_ERROR;
    if (after == JR_CANCEL) {
        R_CHECK(&j.rig, moq_session_fetch_cancel(d->cn->peer, d->fh, j.rig.now) == MOQ_OK);
    } else if (after == JR_TEARDOWN) {
        R_CHECK(&j.rig, moqr_bind_conn_close(j.rig.bind, d->cn->rsess) == MOQR_OK);
    }
    for (int i = 0; i < 8; i++) relay_bind_test_jf_cycle(&j);
    relay_bind_test_jf_stats(&j, &st);
    if (after == JR_RELEASE) {
        j.rig.failures += jf_check_rejected(&j, d, code, label);
        R_CHECK(&j.rig, st.refusals_sent == 1 && st.refusals_pending == 0 &&
                        st.refusals_dropped == 0);
        R_CHECK(&j.rig, moq_session_state(d->cn->peer) == MOQ_SESS_ESTABLISHED &&
                        moq_session_state(d->cn->rsess) == MOQ_SESS_ESTABLISHED);
    } else {
        R_CHECK(&j.rig, d->ledger_n == 0 && d->foreign == 0);
        R_CHECK(&j.rig, st.refusals_sent == 0 && st.refusals_pending == 0 &&
                        st.refusals_dropped == 1);
    }
    R_CHECK(&j.rig, st.refusals_unparked == 0);
    if (mode == JR_POOL) {
        /* The two held transactions are unaffected; afterwards the pool slot
         * is reusable: a fresh join on the refused peer is served exactly. */
        R_CHECK(&j.rig, j.o.fetch_requests == 2 && st.live == 2);
        jf_origin_answer_exact(&j, 0, false);
        jf_origin_answer_exact(&j, 1, false);
        jf_wait_terminal(&j, &j.d[0]);
        jf_wait_terminal(&j, &j.d[1]);
        j.rig.failures += jf_check_exact(&j, &j.d[0], false, "refusal pool (held a)");
        j.rig.failures += jf_check_exact(&j, &j.d[1], false, "refusal pool (held b)");
        if (after == JR_RELEASE) {
            jf_reset_obs(d);
            relay_bind_test_jf_join(&j, d);
            relay_bind_test_jf_wait_requests(&j, 3);
            jf_origin_answer_exact(&j, 2, false);
            jf_wait_terminal(&j, d);
            j.rig.failures += jf_check_exact(&j, d, false, "refusal pool (after)");
        }
    } else if (mode != JR_UNRESOLVED) {
        R_CHECK(&j.rig, j.o.fetch_requests == 0);
    }
    if (after != JR_TEARDOWN && mode != JR_UNRESOLVED) {
        relay_bind_test_jf_live_control(&j);
    }
    relay_bind_test_jf_stats(&j, &st);
    R_CHECK(&j.rig, st.refusals_pending == 0);
    return relay_bind_test_jf_teardown(&j, label);
}

/* Held-refusal capacity: the relay is declared for ONE parked authorization
 * (max_parked = 1, so two entries: one poll and one parkable authorization)
 * while the peer's session may hold 16 pending requests. Four joins arrive
 * under the full 3-deep downstream queue (announce trick as above): two
 * refusals are held, which leaves no entry for the third and fourth -- their
 * events stay queued in the session (the connection's polling pauses)
 * instead of being consumed without an owner. While they are held,
 * the peer cancels the first, issues a fifth join that reuses that request
 * slot, and announces one more
 * namespace -- all queued behind. Required outcome: every request receives
 * exactly its refusal or is retired by its cancel, no request is abandoned,
 * no session closes, the other connection keeps flowing meanwhile, and
 * nothing is left held. */
static int
jf_refusal_capacity(moq_version_t version)
{
    jf_auth_t au;
    memset(&au, 0, sizeof(au));
    relay_bind_test_authorize = jf_auth_hook;
    relay_bind_test_authorize_ctx = &au;
    relay_bind_test_max_fetches = 1;
    relay_bind_test_max_parked = 1;
    relay_bind_test_max_actions = 3;
    static jf_trace_t tr;
    memset(&tr, 0, sizeof(tr));
    tr.verbose = true;
    relay_bind_test_trace_fn = jf_trace_fn;
    relay_bind_test_trace_ctx = &tr;
    jf_t j;
    if (!relay_bind_test_jf_setup(&j, version, 1, true, 1)) return 1 + j.rig.failures;
    relay_bind_test_trace_fn = NULL;
    tr.armed = true;
    jf_down_t *d = &j.d[0];
    static const moq_bytes_t JR_H = { (const uint8_t *)"h", 1 };
    const char *tails[4] = { "t0", "t1", "t2", "t3" };
    for (int t = 0; t < 3; t++) {
        moq_bytes_t parts[2] = { JR_H, { (const uint8_t *)tails[t], 2 } };
        moq_publish_namespace_cfg_t pcfg;
        memset(&pcfg, 0, sizeof(pcfg));
        moq_publish_namespace_cfg_init(&pcfg);
        pcfg.track_namespace = (moq_namespace_t){ .parts = parts, .count = 2 };
        moq_announcement_t ann;
        R_CHECK(&j.rig, moq_session_publish_namespace(d->cn->peer, &pcfg, j.rig.now, &ann) == MOQ_OK);
    }
    moqr_bind_stats_t bs0;
    moqr_bind_get_stats(j.rig.bind, &bs0);
    relay_bind_test_rig_step_pair(&j.rig, d->cn);
    /* Four joins (A = d->fh, B..D = req_fh[0..2]). */
    relay_bind_test_jf_join(&j, d);
    moq_fetch_t fa = d->fh;
    moq_fetch_cfg_t fc;
    memset(&fc, 0, sizeof(fc));
    moq_fetch_cfg_init(&fc);
    fc.is_joining = true;
    fc.joining_relative = true;
    fc.joining_sub = d->sh;
    R_CHECK(&j.rig, moq_session_fetch(d->cn->peer, &fc, j.rig.now, &d->req_fh[0]) == MOQ_OK);
    R_CHECK(&j.rig, moq_session_fetch(d->cn->peer, &fc, j.rig.now, &d->req_fh[1]) == MOQ_OK);
    relay_bind_test_rig_step_pair(&j.rig, d->cn); /* the peer's own 3-deep queue */
    R_CHECK(&j.rig, moq_session_fetch(d->cn->peer, &fc, j.rig.now, &d->req_fh[2]) == MOQ_OK);
    d->req_n = 3;
    relay_bind_test_rig_step_pair(&j.rig, d->cn);
    relay_bind_test_rig_bind_pump_once(&j.rig);
    moqr_bind_fwd_debug_t st;
    relay_bind_test_jf_stats(&j, &st);
    relay_bind_test_jf_down_drain(&j.rig, d);
    /* A and B refused and held (every entry); C's and D's requests stay
     * queued in the session: the pump paused this connection rather than
     * consume a request it could not own. */
    R_CHECK(&j.rig, st.refusals_blocked == 2 && st.refusals_sent == 0 &&
                    st.refusals_unparked == 0 && st.refusals_pending == 2 && st.polls_paused >= 1);
    R_CHECK(&j.rig, d->ledger_n == 0 && d->req_err[0] == 0 && d->req_err[1] == 0 &&
                    d->req_err[2] == 0 && d->ns_accepted == 0);
    /* Held: cancel A, issue E on the freed request slot, announce t3. */
    R_CHECK(&j.rig, moq_session_fetch_cancel(d->cn->peer, fa, j.rig.now) == MOQ_OK);
    R_CHECK(&j.rig, moq_session_fetch(d->cn->peer, &fc, j.rig.now, &d->req_fh[3]) == MOQ_OK);
    d->req_n = 4;
    {
        moq_bytes_t parts[2] = { JR_H, { (const uint8_t *)tails[3], 2 } };
        moq_publish_namespace_cfg_t pcfg;
        memset(&pcfg, 0, sizeof(pcfg));
        moq_publish_namespace_cfg_init(&pcfg);
        pcfg.track_namespace = (moq_namespace_t){ .parts = parts, .count = 2 };
        moq_announcement_t ann;
        R_CHECK(&j.rig, moq_session_publish_namespace(d->cn->peer, &pcfg, j.rig.now, &ann) == MOQ_OK);
    }
    relay_bind_test_rig_step_pair(&j.rig, d->cn); /* delivered; the relay's queue is still full */
    relay_bind_test_rig_bind_pump_once(&j.rig);
    relay_bind_test_jf_stats(&j, &st);
    relay_bind_test_jf_down_drain(&j.rig, d);
    /* With every refusal entry held the connection's event consumption was
     * paused in that pump: the cancel, D, E and t3 stayed
     * queued (t3 not accepted; the three earlier acceptances, drained by the
     * step, are what the peer sees), and nothing was abandoned. The held
     * refusals themselves commit in the same pump once the step emptied the
     * queue. */
    {
        moqr_bind_stats_t bs1;
        moqr_bind_get_stats(j.rig.bind, &bs1);
        printf("  refusal capacity %s at pause: paused=%llu pending=%llu sent=%llu ns_accepted=%d ledger=%d "
               "errB=%d errC=%d session_errors %llu->%llu\n", j.lbl, (unsigned long long)st.polls_paused,
               (unsigned long long)st.refusals_pending, (unsigned long long)st.refusals_sent,
               d->ns_accepted, d->ledger_n, d->req_err[0], d->req_err[1],
               (unsigned long long)bs0.session_errors, (unsigned long long)bs1.session_errors);
        printf("  refusal capacity %s at pause: ns_rejected=%d code=%llu reason=\"%s\"\n", j.lbl,
               d->ns_rejected, (unsigned long long)d->ns_rejected_code, d->ns_rejected_reason);
        if (d->ns_accepted != 3) jf_trace_dump(&tr, "refusal capacity");
    }
    tr.armed = false;
    R_CHECK(&j.rig, st.polls_paused >= 2 && st.refusals_unparked == 0 && st.refusals_pending <= 2);
    R_CHECK(&j.rig, d->ns_accepted == 3 && d->ledger_n == 0 && d->req_err[0] == 0 &&
                    d->req_err[1] == 0 && d->req_err[2] == 0 && d->req_err[3] == 0);
    /* The other connection keeps flowing meanwhile (its own pumps drain the
     * downstream queue too, so the refusals commit along the way). */
    relay_bind_test_jf_live_control(&j);
    for (int i = 0; i < 8; i++) relay_bind_test_jf_cycle(&j);
    relay_bind_test_jf_stats(&j, &st);
    /* A: retired by its cancel (the held refusal found the request gone);
     * B, C, D and E: exactly one refusal each; t0..t3 accepted; nothing
     * held. */
    R_CHECK(&j.rig, d->ledger_n == 0);                       /* A: no error */
    R_CHECK(&j.rig, d->req_err[0] == 1 && d->req_err[1] == 1 && d->req_err[2] == 1 &&
                    d->req_err[3] == 1 && d->foreign == 0);
    R_CHECK(&j.rig, d->ns_accepted == 4);
    R_CHECK(&j.rig, st.refusals_sent == 4 && st.refusals_gone + st.refusals_dropped == 1 &&
                    st.refusals_pending == 0 && st.refusals_unparked == 0);
    R_CHECK(&j.rig, moq_session_state(d->cn->peer) == MOQ_SESS_ESTABLISHED &&
                    moq_session_state(d->cn->rsess) == MOQ_SESS_ESTABLISHED);
    R_CHECK(&j.rig, j.o.fetch_requests == 0 && st.admitted == 0);
    printf("  refusal capacity %s: blocked=%llu parked=%llu paused=%llu sent=%llu gone=%llu dropped=%llu\n",
           j.lbl, (unsigned long long)st.refusals_blocked, (unsigned long long)st.refusals_parked,
           (unsigned long long)st.polls_paused, (unsigned long long)st.refusals_sent,
           (unsigned long long)st.refusals_gone, (unsigned long long)st.refusals_dropped);
    return relay_bind_test_jf_teardown(&j, "fwd_refusal_capacity");
}

static int
jf_refusals_all(moq_version_t v)
{
    int f = 0;
    for (int mode = 0; mode < JR_COUNT; mode++) {
        f += jf_refusal_held(v, mode, JR_RELEASE);
        if (mode != JR_POOL) {
            f += jf_refusal_held(v, mode, JR_CANCEL);
            f += jf_refusal_held(v, mode, JR_TEARDOWN);
        }
    }
    f += jf_refusal_capacity(v);
    return f;
}

/* Config ABI of the appended fetch_forward_bytes: old-size storage (a buffer
 * of exactly the pre-append size, so any read past it is out of bounds),
 * poisoned tail bytes, partial-field presence, full-size zero/custom values,
 * 32-bit representability (refused above UINT32_MAX, never truncated), and
 * the estimator's total_bytes carrying the two forwarding ceilings and
 * the separate fixed metadata reservation. */
static int
jf_cfg_abi(void)
{
    int failures = 0;
    ca_t a;
    relay_bind_test_ca_init(&a);
    moqr_core_relay_cfg_t ccfg;
    moqr_core_relay_cfg_init_sized(&ccfg, sizeof(ccfg), &a.vt);
    ccfg.log_budget.max_groups = 4;
    ccfg.log_budget.max_bytes = 1 << 20;
    moqr_core_t *core = NULL;
    MOQ_TEST_CHECK(moqr_core_create(&ccfg, &core) == MOQR_OK);
    moqr_core_limits_t lim;
    MOQ_TEST_CHECK(moqr_core_limits_resolve(&ccfg, &lim) == MOQR_OK);
    const size_t old_size = offsetof(moqr_bind_cfg_t, fetch_forward_bytes);
    const size_t full = sizeof(moqr_bind_cfg_t);
#define JF_CAP_OF(cfgp, out)                                                  \
    do {                                                                      \
        moqr_bind_t *b_ = NULL;                                               \
        moqr_result_t rc_ = moqr_bind_create((cfgp), &b_);                    \
        if (rc_ == MOQR_OK) {                                                 \
            moqr_bind_fwd_debug_t st_;                                        \
            moqr_bind_debug_fwd_stats(b_, &st_);                              \
            (out) = st_.bytes_cap;                                            \
            moqr_bind_destroy(b_);                                            \
        } else {                                                              \
            (out) = UINT64_MAX;                                               \
        }                                                                     \
    } while (0)
    uint64_t cap;
    /* 1. Old-size storage: the caller's struct occupies exactly old_size
     *    bytes (a heap block of that size), so any read past it is out of
     *    bounds; the full-size local only prepares the prefix bytes. */
    {
        uint8_t *buf = malloc(old_size);
        MOQ_TEST_CHECK(buf != NULL);
        moqr_bind_cfg_t prep;
        moqr_bind_cfg_init_sized(&prep, old_size, &a.vt);
        MOQ_TEST_CHECK(prep.struct_size == (uint32_t)old_size);
        prep.core = core;
        prep.max_conns = 2;
        memcpy(buf, &prep, old_size);
        const moqr_bind_cfg_t *cfg = (const moqr_bind_cfg_t *)(const void *)buf;
        JF_CAP_OF(cfg, cap);
        MOQ_TEST_CHECK_EQ_U64(cap, MOQR_BIND_DEF_FETCH_FORWARD_BYTES);
        moqr_bind_capacity_t cp;
        MOQ_TEST_CHECK(moqr_bind_capacity_describe(cfg, &lim, &cp) == MOQR_OK);
        moqr_bind_limits_t bl;
        MOQ_TEST_CHECK(moqr_bind_cfg_resolve(cfg, &lim, &bl) == MOQR_OK);
        MOQ_TEST_CHECK_EQ_U64(cp.total_bytes - cp.structure_bytes - cp.announce_bytes,
                              MOQR_BIND_DEF_FETCH_FORWARD_BYTES +
                                  (uint64_t)bl.max_conns * bl.n_usubs * 4096u +
                                  MOQR_BIND_META_BYTES_MAX);
        free(buf);
    }
    /* 2. Poisoned tail: a full-size struct whose bytes past the old size are
     *    garbage, with struct_size = old size. */
    {
        moqr_bind_cfg_t cfg;
        memset(&cfg, 0xA5, sizeof(cfg));
        moqr_bind_cfg_init_sized(&cfg, old_size, &a.vt);
        MOQ_TEST_CHECK(cfg.struct_size == (uint32_t)old_size);
        MOQ_TEST_CHECK(cfg.fetch_forward_bytes == 0xA5A5A5A5A5A5A5A5ull);
        cfg.core = core;
        cfg.max_conns = 2;
        JF_CAP_OF(&cfg, cap);
        MOQ_TEST_CHECK_EQ_U64(cap, MOQR_BIND_DEF_FETCH_FORWARD_BYTES);
    }
    /* 3. Partial-field presence: the struct reaches only part of the field. */
    {
        moqr_bind_cfg_t cfg;
        moqr_bind_cfg_init_sized(&cfg, full, &a.vt);
        cfg.struct_size = (uint32_t)(old_size + 4);
        cfg.fetch_forward_bytes = 0xFFFFFFFFFFFFFFFFull;   /* would be refused if read */
        cfg.core = core;
        cfg.max_conns = 2;
        JF_CAP_OF(&cfg, cap);
        MOQ_TEST_CHECK_EQ_U64(cap, MOQR_BIND_DEF_FETCH_FORWARD_BYTES);
    }
    /* 4. Full size: zero = default; custom honored; UINT32_MAX honored;
     *    UINT32_MAX + 1 refused by create, describe and resolve alike. */
    {
        moqr_bind_cfg_t cfg;
        moqr_bind_cfg_init_sized(&cfg, full, &a.vt);
        cfg.core = core;
        cfg.max_conns = 2;
        JF_CAP_OF(&cfg, cap);
        MOQ_TEST_CHECK_EQ_U64(cap, MOQR_BIND_DEF_FETCH_FORWARD_BYTES);
        cfg.fetch_forward_bytes = 65536;
        JF_CAP_OF(&cfg, cap);
        MOQ_TEST_CHECK_EQ_U64(cap, 65536);
        moqr_bind_capacity_t cp;
        MOQ_TEST_CHECK(moqr_bind_capacity_describe(&cfg, &lim, &cp) == MOQR_OK);
        moqr_bind_limits_t bl;
        MOQ_TEST_CHECK(moqr_bind_cfg_resolve(&cfg, &lim, &bl) == MOQR_OK);
        MOQ_TEST_CHECK_EQ_U64(cp.total_bytes - cp.structure_bytes - cp.announce_bytes,
                              65536u + (uint64_t)bl.max_conns * bl.n_usubs * 4096u +
                                  MOQR_BIND_META_BYTES_MAX);
        cfg.fetch_forward_bytes = UINT32_MAX;
        JF_CAP_OF(&cfg, cap);
        MOQ_TEST_CHECK_EQ_U64(cap, UINT32_MAX);
        cfg.fetch_forward_bytes = (uint64_t)UINT32_MAX + 1u;
        moqr_bind_t *b = (moqr_bind_t *)(void *)&cfg;   /* must be reset to NULL */
        MOQ_TEST_CHECK(moqr_bind_create(&cfg, &b) == MOQR_ERR_INVAL);
        MOQ_TEST_CHECK(b == NULL);
        MOQ_TEST_CHECK(moqr_bind_capacity_describe(&cfg, &lim, &cp) == MOQR_ERR_INVAL);
        MOQ_TEST_CHECK(cp.total_bytes == 0 && cp.structure_bytes == 0);
        MOQ_TEST_CHECK(moqr_bind_cfg_resolve(&cfg, &lim, &bl) == MOQR_ERR_INVAL);
        MOQ_TEST_CHECK(bl.max_conns == 0 && bl.n_fetches == 0);
    }
#undef JF_CAP_OF
    moqr_core_destroy(core);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    if (failures == 0) {
        MOQ_TEST_PASS("fwd_cfg_abi");
    }
    return failures;
}

static int
jf_all(moq_version_t v)
{
    int f = 0;
    f += jf_exact_group2_singleton(v, 0);
    f += jf_exact_group2_singleton(v, 1);
    f += jf_exact_group2_singleton(v, 2);
    f += jf_exact_single(v, false);
    f += jf_exact_single(v, true);
    f += jf_exact_two(v);
    f += jf_shared_bound(v);
    f += jf_count_overflow(v);
    f += jf_upstream_error(v);
    f += jf_range_mismatch(v, false);
    f += jf_range_mismatch(v, true);
    f += jf_upstream_reset(v);
    f += jf_source_loss_replacement(v);
    f += jf_down_cancel(v, false);
    f += jf_down_cancel(v, true);
    f += jf_down_cancel_after_ok(v);
    f += jf_queued_tail_reuse(v);
    f += jf_block_upstream(v, false);
    f += jf_block_upstream(v, true);
    f += jf_block_downstream(v, 0);
    f += jf_block_downstream(v, 1);
    f += jf_block_downstream(v, 2);
    f += jf_block_downstream(v, 3);
    f += jf_block_downstream(v, 4);
    f += jf_abort_after_ok(v);
    f += jf_auth_fail_closed(v);
    f += jf_slot_exhaustion(v);
    f += jf_descending_rejected(v);
    f += jf_oom_owned_copies(v);
    f += jf_oom_sweep(v);
    f += jf_refusals_all(v);
    return f;
}

/* ---- Fetcher-side FETCH lifetime probes (direct SimPair, raw sessions) ----- *
 * What a relay that forwards an upstream FETCH must know, measured on the
 * public session API on both drafts:
 *   P1 cancel after FETCH_OK + one object with the response still open:
 *      exact moq_session_fetch_cancel result, the publisher's observed
 *      cancellation (FETCH_CANCELLED event; a later write on its handle),
 *      delivery of objects that were ALREADY queued at the fetcher before the
 *      cancel (old handle), their release through moq_event_cleanup, and
 *      immediate request-slot reuse (fresh generation).
 *   P2 cancel refused by a full action queue (WOULD_BLOCK) is retryable.
 *   P3 error terminal (publisher rejects): slot reuse after FETCH_ERROR on
 *      both drafts (draft-18 keeps the request bidi draining until FIN).
 *   P4 completion terminal: OK -> object -> COMPLETE, then slot reuse; a
 *      publisher write on the completed handle is STALE_HANDLE.
 * The counting allocator proves reference reclamation (live == 0 at exit). */
typedef struct probe_ev {
    int      ok, obj, gap, complete, err, cancelled, request;
    uint64_t err_code;
    uint64_t last_obj_fetch;   /* handle carried by the last FETCH_OBJECT */
    int      obj_old_handle;   /* objects that still carried a given handle */
    moq_fetch_t last_req;      /* publisher: last FETCH_REQUEST handle      */
    moq_fetch_t last_cancelled;
} probe_ev_t;

static void
probe_drain(moq_session_t *sess, probe_ev_t *o, uint64_t watch_handle)
{
    moq_event_t evs[8];
    size_t n;
    while ((n = moq_session_poll_events(sess, evs, 8)) > 0) {
        for (size_t i = 0; i < n; i++) {
            moq_event_t *ev = &evs[i];
            switch (ev->kind) {
            case MOQ_EVENT_FETCH_REQUEST:
                o->request++;
                o->last_req = ev->u.fetch_request.fetch;
                break;
            case MOQ_EVENT_FETCH_OK:        o->ok++; break;
            case MOQ_EVENT_FETCH_OBJECT:
                o->obj++;
                o->last_obj_fetch = ev->u.fetch_object.fetch._opaque;
                if (ev->u.fetch_object.fetch._opaque == watch_handle) {
                    o->obj_old_handle++;
                }
                break;
            case MOQ_EVENT_FETCH_GAP:       o->gap++; break;
            case MOQ_EVENT_FETCH_COMPLETE:  o->complete++; break;
            case MOQ_EVENT_FETCH_ERROR:
                o->err++;
                o->err_code = ev->u.fetch_error.error_code;
                break;
            case MOQ_EVENT_FETCH_CANCELLED:
                o->cancelled++;
                o->last_cancelled = ev->u.fetch_cancelled.fetch;
                break;
            default: break;
            }
            moq_event_cleanup(ev);   /* releases FETCH_OBJECT refs we did not take */
        }
    }
}

static moq_result_t
probe_pub_write(ca_t *a, moq_session_t *pub, moq_fetch_t fh, uint64_t oid,
                uint64_t now)
{
    uint8_t body[16];
    memset(body, (int)(0x70 + oid), sizeof(body));
    moq_rcbuf_t *pl = NULL;
    if (moq_rcbuf_create(&a->vt, body, sizeof(body), &pl) != MOQ_OK) {
        return MOQ_ERR_NOMEM;
    }
    moq_fetch_object_cfg_t oc;
    moq_fetch_object_cfg_init(&oc);
    oc.group_id = 0;
    oc.subgroup_id = 0;
    oc.object_id = oid;
    oc.publisher_priority = 100;
    oc.payload = pl;
    moq_result_t rc = moq_session_write_fetch_object(pub, fh, &oc, now);
    moq_rcbuf_decref(pl);
    return rc;
}

static int
fetch_lifetime_probe(moq_version_t version, bool tiny_action_queue)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    int failures = 0;
    const char *lbl = version == MOQ_VERSION_DRAFT_16 ? "v16" : "v18";
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = &a.vt;
    cfg.seed = 0x9F0BEu;
    cfg.version = version;
    /* Fill variant: a 3-deep action queue on BOTH sessions; the publisher's
     * calls below retry across cycles, the fetcher fills its queue before the
     * cancel so the cancel meets a genuinely full queue. */
    cfg.max_actions = tiny_action_queue ? 3 : 0;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 1024;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 1024;
    moq_simpair_t *sp = NULL;
    if (moq_simpair_create(&cfg, &sp) != MOQ_OK || moq_simpair_start(sp) != MOQ_OK) {
        printf("FAIL: fetch_lifetime_probe simpair\n");
        return 1;
    }
    moq_session_t *fetcher = moq_simpair_client(sp);
    moq_session_t *pub = moq_simpair_server(sp);
    uint64_t now = 1;
#define PROBE_CYCLE()                                                     \
    do {                                                                  \
        now += 1000;                                                      \
        (void)moq_simpair_advance_to(sp, now);                            \
        size_t steps_ = 0;                                                \
        (void)moq_simpair_run_until_quiescent(sp, 64, &steps_);           \
    } while (0)
    for (int i = 0; i < 8; i++) PROBE_CYCLE();
    moq_bytes_t nsp[1] = { B("probe") };
    moq_fetch_cfg_t fc;
    memset(&fc, 0, sizeof(fc));
    moq_fetch_cfg_init(&fc);
    fc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 1 };
    fc.track_name = B("t");
    fc.start_group = 0; fc.start_object = 0; fc.end_group = 0; fc.end_object = 3;
    probe_ev_t fo, po;
    memset(&fo, 0, sizeof(fo));
    memset(&po, 0, sizeof(po));

    /* P1: OK + one object delivered, response left open, then cancel. */
    moq_fetch_t f1;
    if (moq_session_fetch(fetcher, &fc, now, &f1) != MOQ_OK) {
        printf("FAIL: probe %s: initial fetch not placed\n", lbl);
        moq_simpair_destroy(sp);
        return 1;
    }
    for (int i = 0; i < 6 && po.request == 0; i++) { PROBE_CYCLE(); probe_drain(pub, &po, 0); }
    if (po.request != 1) { printf("FAIL: probe %s: publisher saw %d requests\n", lbl, po.request); failures++; }
    moq_accept_fetch_cfg_t ac;
    moq_accept_fetch_cfg_init(&ac);
    ac.end_of_track = false; ac.end_group = 0; ac.end_object = 3;
#define PROBE_RETRY(expr, var)                                            \
    do {                                                                  \
        var = (expr);                                                     \
        for (int r_ = 0; r_ < 8 && var == MOQ_ERR_WOULD_BLOCK; r_++) {    \
            PROBE_CYCLE();                                                \
            var = (expr);                                                 \
        }                                                                 \
    } while (0)
    moq_result_t arc, w0;
    PROBE_RETRY(moq_session_accept_fetch(pub, po.last_req, &ac, now), arc);
    PROBE_RETRY(probe_pub_write(&a, pub, po.last_req, 0, now), w0);
    for (int i = 0; i < 6 && !(fo.ok == 1 && fo.obj == 1); i++) { PROBE_CYCLE(); probe_drain(fetcher, &fo, f1._opaque); }
    printf("PROBE P1 %s: accept rc=%d write0 rc=%d fetcher ok=%d obj=%d complete=%d (response open)\n",
           lbl, (int)arc, (int)w0, fo.ok, fo.obj, fo.complete);
    if (!(arc == MOQ_OK && w0 == MOQ_OK && fo.ok == 1 && fo.obj == 1 && fo.complete == 0)) {
        printf("FAIL: probe %s: P1 setup\n", lbl); failures++;
    }
    /* Two more objects: transported so they are QUEUED at the fetcher, NOT polled. */
    moq_result_t w1, w2;
    PROBE_RETRY(probe_pub_write(&a, pub, po.last_req, 1, now), w1);
    PROBE_RETRY(probe_pub_write(&a, pub, po.last_req, 2, now), w2);
    PROBE_CYCLE(); PROBE_CYCLE();
    /* P2 (fill variant): fill the FETCHER's action queue with control requests
     * (no transport step) until one is refused WOULD_BLOCK, so the cancel that
     * follows meets a genuinely full queue; one cycle drains it, then retry. */
    moq_result_t pre = MOQ_OK;
    int filled = 0;
    if (tiny_action_queue) {
        /* Distinct TRACK_STATUS requests: one control action each, no
         * dedup, no fetch-slot consumption. */
        for (int i = 0; i < 4096; i++) {
            char nm[8];
            (void)snprintf(nm, sizeof(nm), "s%04d", i);
            moq_track_status_cfg_t tsc;
            memset(&tsc, 0, sizeof(tsc));
            moq_track_status_cfg_init(&tsc);
            tsc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 1 };
            tsc.track_name = (moq_bytes_t){ (const uint8_t *)nm, strlen(nm) };
            moq_track_status_handle_t tsh;
            pre = moq_session_track_status(fetcher, &tsc, now, &tsh);
            if (pre != MOQ_OK) break;
            filled++;
        }
    }
    moq_result_t c1 = moq_session_fetch_cancel(fetcher, f1, now);
    moq_result_t c1b = MOQ_OK;
    if (tiny_action_queue) {
        PROBE_CYCLE();                      /* drain the action queue */
        c1b = moq_session_fetch_cancel(fetcher, f1, now);
    }
    /* Immediate slot reuse: a new fetch right after the cancel. */
    moq_fetch_t f2;
    moq_result_t r2 = moq_session_fetch(fetcher, &fc, now, &f2);
    /* Now poll: the two already-queued objects still carry the OLD handle. */
    int obj_before = fo.obj;
    probe_drain(fetcher, &fo, f1._opaque);
    int queued_old = fo.obj - obj_before;
    for (int i = 0; i < 6; i++) { PROBE_CYCLE(); probe_drain(pub, &po, 0); probe_drain(fetcher, &fo, f1._opaque); }
    /* The publisher's handle for the first request (its own handle space). */
    moq_result_t w_after = probe_pub_write(&a, pub, po.last_cancelled, 3, now);
    moq_result_t c_again = moq_session_fetch_cancel(fetcher, f1, now);
    printf("PROBE P1/P2 %s: fill=%d last_fill_rc=%d cancel rc=%d%s%d reuse rc=%d handle_changed=%d; queued old-handle objects "
           "polled after cancel=%d (old-handle total %d, w1=%d w2=%d); publisher FETCH_CANCELLED=%d "
           "(handle matches=%d) write_after=%d; second cancel=%d; publisher saw requests=%d\n",
           lbl, filled, (int)pre, (int)c1, tiny_action_queue ? " retry rc=" : "", tiny_action_queue ? (int)c1b : 0,
           (int)r2, (int)(r2 == MOQ_OK && f2._opaque != f1._opaque), queued_old, fo.obj_old_handle,
           (int)w1, (int)w2, po.cancelled, (int)(po.cancelled == 1) /* its own handle space */,
           (int)w_after, (int)c_again, po.request);
    if (tiny_action_queue) {
        if (!(pre == MOQ_ERR_WOULD_BLOCK && c1 == MOQ_ERR_WOULD_BLOCK && c1b == MOQ_OK)) {
            printf("FAIL: probe %s: P2 expected fill WOULD_BLOCK, cancel WOULD_BLOCK, retry OK\n", lbl); failures++;
        }
    } else if (c1 != MOQ_OK) {
        printf("FAIL: probe %s: P1 cancel after OK+object rc=%d\n", lbl, (int)c1); failures++;
    }
    if (!(r2 == MOQ_OK && f2._opaque != f1._opaque)) { printf("FAIL: probe %s: slot reuse\n", lbl); failures++; }
    if (c_again != MOQ_ERR_STALE_HANDLE) { printf("FAIL: probe %s: second cancel rc=%d\n", lbl, (int)c_again); failures++; }
    if (po.cancelled != 1) { printf("FAIL: probe %s: publisher cancelled events=%d\n", lbl, po.cancelled); failures++; }
    if (w_after != MOQ_ERR_STALE_HANDLE && w_after != MOQ_ERR_WRONG_STATE) {
        printf("FAIL: probe %s: publisher write after cancel rc=%d\n", lbl, (int)w_after); failures++;
    }
    /* P3: the publisher rejects the second request -> FETCH_ERROR terminal; reuse again. */
    moq_reject_fetch_cfg_t rj;
    moq_reject_fetch_cfg_init(&rj);
    rj.error_code = MOQ_REQUEST_ERROR_NOT_SUPPORTED;
    moq_result_t rj_rc = MOQ_ERR_INVAL;
    for (int i = 0; i < 6 && po.request < 2; i++) { PROBE_CYCLE(); probe_drain(pub, &po, 0); }
    if (po.request == 2) PROBE_RETRY(moq_session_reject_fetch(pub, po.last_req, &rj, now), rj_rc);
    for (int i = 0; i < 6 && fo.err == 0; i++) { PROBE_CYCLE(); probe_drain(fetcher, &fo, f1._opaque); }
    moq_fetch_t f3;
    moq_result_t r3 = moq_session_fetch(fetcher, &fc, now, &f3);
    printf("PROBE P3 %s: reject rc=%d fetcher err=%d code=%llu; reuse after error rc=%d handle_changed=%d\n",
           lbl, (int)rj_rc, fo.err, (unsigned long long)fo.err_code, (int)r3,
           (int)(r3 == MOQ_OK && f3._opaque != f2._opaque));
    if (!(rj_rc == MOQ_OK && fo.err == 1 && fo.err_code == MOQ_REQUEST_ERROR_NOT_SUPPORTED && r3 == MOQ_OK)) {
        printf("FAIL: probe %s: P3\n", lbl); failures++;
    }
    /* P4: completion terminal, then reuse; publisher write on the completed handle. */
    for (int i = 0; i < 6 && po.request < 3; i++) { PROBE_CYCLE(); probe_drain(pub, &po, 0); }
    moq_result_t a3 = MOQ_ERR_INVAL, w3 = MOQ_ERR_INVAL, e3 = MOQ_ERR_INVAL;
    if (po.request == 3) {
        PROBE_RETRY(moq_session_accept_fetch(pub, po.last_req, &ac, now), a3);
        PROBE_RETRY(probe_pub_write(&a, pub, po.last_req, 0, now), w3);
        PROBE_RETRY(moq_session_end_fetch(pub, po.last_req, now), e3);
    }
    int ok_before = fo.ok, obj_b4 = fo.obj, cmp_b4 = fo.complete;
    for (int i = 0; i < 8 && fo.complete == cmp_b4; i++) { PROBE_CYCLE(); probe_drain(fetcher, &fo, f1._opaque); }
    moq_result_t w_done = po.request == 3 ? probe_pub_write(&a, pub, po.last_req, 1, now) : MOQ_ERR_INVAL;
    moq_fetch_t f4;
    moq_result_t r4 = moq_session_fetch(fetcher, &fc, now, &f4);
    printf("PROBE P4 %s: accept=%d write=%d end=%d fetcher ok+%d obj+%d complete+%d; publisher write after FIN=%d; reuse rc=%d handle_changed=%d\n",
           lbl, (int)a3, (int)w3, (int)e3, fo.ok - ok_before, fo.obj - obj_b4, fo.complete - cmp_b4,
           (int)w_done, (int)r4, (int)(r4 == MOQ_OK && f4._opaque != f3._opaque));
    if (!(a3 == MOQ_OK && w3 == MOQ_OK && e3 == MOQ_OK && fo.ok - ok_before == 1 &&
          fo.obj - obj_b4 == 1 && fo.complete - cmp_b4 == 1 && w_done == MOQ_ERR_STALE_HANDLE && r4 == MOQ_OK)) {
        printf("FAIL: probe %s: P4\n", lbl); failures++;
    }
    /* leave f4 pending; teardown must reclaim everything */
    for (int i = 0; i < 4; i++) { PROBE_CYCLE(); probe_drain(pub, &po, 0); probe_drain(fetcher, &fo, f1._opaque); }
#undef PROBE_RETRY
#undef PROBE_CYCLE
    moq_simpair_destroy(sp);
    if (a.live != 0) { printf("FAIL: probe %s: live allocations after teardown=%zu\n", lbl, a.live); failures++; }
    if (failures == 0) {
        printf("PASS: fetch_lifetime_probe %s%s\n", lbl, tiny_action_queue ? " (full action queue at cancel)" : "");
    }
    return failures;
}

int
main(int argc, char **argv)
{
    if (argc == 2 && strcmp(argv[1], "--late-subscriber") == 0) {
        int f = 0;
        for (int order = 0; order < 3; order++) {
            f += jf_exact_group2_singleton(MOQ_VERSION_DRAFT_16, order);
            f += jf_exact_group2_singleton(MOQ_VERSION_DRAFT_18, order);
        }
        return f == 0 ? 0 : 1;
    }
    if (argc == 2 && strcmp(argv[1], "--fetch-capacity-abi") == 0) {
        return jf_cfg_abi() == 0 ? 0 : 1;
    }
    if (argc > 1 && strcmp(argv[1], "--core-cancel-after-stop") == 0) {
        int f = core_cancel_after_stop(MOQ_VERSION_DRAFT_16);
        f += core_cancel_after_stop(MOQ_VERSION_DRAFT_18);
        return f == 0 ? 0 : 1;
    }
    int failures = 0;
    failures += draft22_fetch_relay_resolved();
    failures += draft22_fetch_fill_timeout_kinds(0);
    failures += draft22_fetch_fill_timeout_kinds(1);
    failures += draft22_fetch_fill_timeout_kinds(2);
    failures += fetch_in_progress(MOQ_VERSION_DRAFT_22, FIP_COMPLETES);
    failures += fetch_in_progress(MOQ_VERSION_DRAFT_22, FIP_DEADLINE);
    failures += fetch_in_progress(MOQ_VERSION_DRAFT_22, FIP_TIMEOUT_ZERO);
    failures += fetch_in_progress(MOQ_VERSION_DRAFT_18, FIP_COMPLETES);
    failures += fetch_in_progress(MOQ_VERSION_DRAFT_18, FIP_DEADLINE);
    failures += draft22_fill_in_progress();
    failures += fetch_flow(MOQ_VERSION_DRAFT_16);
    failures += fetch_flow(MOQ_VERSION_DRAFT_18);
    /* Evicted-prefix marker, both drafts + a d18 backpressure variant. The
     * marker object id is the draft max (literals; see fetch_evicted_prefix):
     * d16 = 2^62-1 (MOQ_QUIC_VARINT_MAX), d18 = UINT64_MAX (MOQ_VI64_MAX). */
    failures += fetch_evicted_prefix(MOQ_VERSION_DRAFT_16, 0x3FFFFFFFFFFFFFFFull, 0);
    failures += fetch_evicted_prefix(MOQ_VERSION_DRAFT_18, UINT64_MAX, 0);
    failures += fetch_evicted_prefix(MOQ_VERSION_DRAFT_18, UINT64_MAX, 2);
    failures += fetch_unvouched_marked(MOQ_VERSION_DRAFT_16,
                                       MOQ_FETCH_RANGE_UNKNOWN,
                                       0x3FFFFFFFFFFFFFFFull);
    failures += fetch_unvouched_marked(MOQ_VERSION_DRAFT_18,
                                       MOQ_FETCH_RANGE_UNKNOWN, UINT64_MAX);
    failures += fetch_unvouched_marked(MOQ_VERSION_DRAFT_22,
                                       MOQ_FETCH_RANGE_UNKNOWN, UINT64_MAX);
    /* Fetch datagram-preference cross-draft (d18 preserves, d16 clears). */
    failures += fetch_datagram_pref(MOQ_VERSION_DRAFT_18, true);
    failures += fetch_datagram_pref(MOQ_VERSION_DRAFT_16, false);
    failures += fetch_datagram_parity();
    /* Retained FETCH over chunked COMPLETE records —
     * parity with whole-object storage across d16/d18 + a backpressure variant. */
    failures += retained_fetch_representation_parity_oracle(MOQ_VERSION_DRAFT_18, 0);
    failures += retained_fetch_representation_parity_oracle(MOQ_VERSION_DRAFT_16, 0);
    failures += retained_fetch_representation_parity_oracle(MOQ_VERSION_DRAFT_18, 3);
    failures += chunked_fetch_oom_terminates();   /* coalesce OOM fails closed */
    failures += fetch_backpressure();
    failures += fetch_close_midstream();
    failures += fetch_error_terminates();
    failures += jf_all(MOQ_VERSION_DRAFT_16);
    failures += jf_all(MOQ_VERSION_DRAFT_18);
    failures += jf_cfg_abi();
    failures += fetch_lifetime_probe(MOQ_VERSION_DRAFT_16, false);
    failures += fetch_lifetime_probe(MOQ_VERSION_DRAFT_18, false);
    failures += fetch_lifetime_probe(MOQ_VERSION_DRAFT_16, true);
    failures += fetch_lifetime_probe(MOQ_VERSION_DRAFT_18, true);
    return failures == 0 ? 0 : 1; /* exit status truncates to 8 bits */
}
