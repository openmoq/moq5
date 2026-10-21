/*
 * The relay data plane over real sessions, proven from wire-observable
 * events alone: every in-range object is delivered exactly once, in
 * per-(group, subgroup) order and byte-faithful, with nothing out of range
 * and nothing after a subscription terminal — clean and under action-queue
 * backpressure, across datagrams and object status, retained replay, late
 * FIN and End-of-Group, first-object forwarding on every draft, eviction,
 * source end, push subscriptions and mixed-draft peers. Chunked ingest
 * delivers COMPLETE objects and resets an abandoned live edge.
 *
 * The ready-set scheduler drives delivery without idle probes, parks at the
 * action cap, rearms after blocking and never lets a hot track starve the
 * rest; and downstream Forward state pauses and resumes a subscription
 * exactly once without terminating it.
 */
#include "relay_session_binding_harness.h"

/* ---- Data-plane delivery oracle ------------------------------------------ *
 * Drive a KNOWN object matrix over the production SimPair binding and prove,
 * from wire-observable events ALONE (no relay internals), that the relay's data
 * plane is correct: every in-range object delivered exactly once, in
 * per-(group, subgroup) object-id order, byte-faithful, nothing out of range,
 * nothing after the subscription terminal, and every in-range subgroup sealed.
 * Runs clean and under a small session action queue — WOULD_BLOCK churn must
 * delay delivery, never corrupt or duplicate the stream. */
#define DP_G  2u   /* in-range groups 0..DP_G-1 (group DP_G is out of range) */

static int
dataplane_delivery_oracle(uint32_t max_actions)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    relay_bind_test_max_actions = max_actions;
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        relay_bind_test_max_actions = 0;
        printf("FAIL: dataplane_delivery_oracle rig create (aq=%u)\n",
               max_actions);
        return 1;
    }
    int pumps = max_actions == 0 ? 40 : 200;   /* backpressure needs more turns */
    conn_t *pub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub && sub);
    relay_bind_test_rig_pump(&rig, 4);

    moq_bytes_t nsp[2] = { B("live"), B("cam") };
    moq_publish_namespace_cfg_t pcfg;
    memset(&pcfg, 0, sizeof(pcfg));
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg, rig.now,
                                                &ann) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 4);

    /* Bounded range [0, DP_G-1]; the out-of-range group DP_G completes it. */
    moq_subscribe_cfg_t scfg;
    memset(&scfg, 0, sizeof(scfg));
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    scfg.track_name = B("v");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE;
    scfg.start_group = 0;
    scfg.start_object = 0;
    scfg.end_group = DP_G - 1;
    moq_subscription_t sh;
    R_CHECK(&rig, moq_session_subscribe(sub->peer, &scfg, rig.now, &sh) ==
                      MOQ_OK);
    relay_bind_test_rig_pump(&rig, 6);
    peer_state_t pub_ps, s_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&s_ps, 0, sizeof(s_ps));
    relay_bind_test_peer_drain(&rig, pub, &pub_ps, true, false, 0, 0); /* accept upstream */
    R_CHECK(&rig, pub_ps.up_seen);
    relay_bind_test_rig_pump(&rig, 6);

    /* Publish the in-range matrix, then one out-of-range group to move the live
     * edge past the range end so the subscription terminates cleanly. */
    for (uint64_t g = 0; g < DP_G; g++) {
        for (uint64_t sg = 0; sg < DP_SG; sg++) {
            relay_bind_test_dp_send_subgroup(&rig, pub, pub_ps.up_sub, g, sg);
            relay_bind_test_rig_pump(&rig, 4);                  /* flush live  */
            relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0); /* no pile-up  */
        }
    }
    /* Out-of-range group moves the live edge past the range end -> terminate. */
    relay_bind_test_dp_send_subgroup(&rig, pub, pub_ps.up_sub, DP_G, 0);
    for (int k = 0; k < pumps; k++) {
        relay_bind_test_rig_pump(&rig, 1);
        relay_bind_test_peer_drain(&rig, pub, &pub_ps, false, false, 0, 0); /* keep origin live */
        relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
        if (s_ps.done_seen) {
            break;
        }
    }
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0); /* sweep any tail */

    /* ---- oracle (wire-observable only) ---- */
    const uint32_t expect = DP_G * DP_SG * DP_O;
    R_CHECK(&rig, s_ps.done_seen);                     /* range terminated    */
    R_CHECK(&rig, s_ps.dlv_overflow == 0);
    R_CHECK(&rig, (uint32_t)s_ps.dlv_n == expect);     /* exact count         */

    int seen[DP_G][DP_OPG];
    memset(seen, 0, sizeof(seen));
    for (int i = 0; i < s_ps.dlv_n; i++) {
        dlv_rec_t *d = &s_ps.dlv[i];
        R_CHECK(&rig, !d->after_done);            /* nothing after terminal   */
        R_CHECK(&rig, d->sub == sh._opaque);      /* attributed to this sub   */
        R_CHECK(&rig, d->group < DP_G);           /* in range (no group DP_G) */
        R_CHECK(&rig, d->object_id < DP_OPG);
        R_CHECK(&rig, d->subgroup == d->object_id / DP_O);  /* right stream   */
        R_CHECK(&rig, !d->payload_null);
        R_CHECK(&rig, d->fp == relay_bind_test_dp_tag(d->group, d->object_id));
        if (d->group < DP_G && d->object_id < DP_OPG) {
            seen[d->group][d->object_id]++;
        }
    }
    for (uint64_t g = 0; g < DP_G; g++) {
        for (uint64_t oid = 0; oid < DP_OPG; oid++) {
            R_CHECK(&rig, seen[g][oid] == 1);         /* exactly once         */
        }
        for (uint64_t sg = 0; sg < DP_SG; sg++) {
            int64_t last = -1;                        /* ascending per stream  */
            for (int i = 0; i < s_ps.dlv_n; i++) {
                dlv_rec_t *d = &s_ps.dlv[i];
                if (d->group == g && d->subgroup == sg) {
                    R_CHECK(&rig, (int64_t)d->object_id > last);
                    last = (int64_t)d->object_id;
                }
            }
        }
    }
    /* Exactly the in-range subgroups sealed — each once, none extra. A weaker
     * >= would let an out-of-range group's FIN (or a duplicate) inflate the
     * total unnoticed; assert the recorded (group, subgroup) FIN set precisely. */
    R_CHECK(&rig, s_ps.subgroup_finished == (int)(DP_G * DP_SG));
    for (uint64_t g = 0; g < DP_G; g++) {
        for (uint64_t sg = 0; sg < DP_SG; sg++) {
            int fins = 0;
            for (int fi = 0; fi < s_ps.finished_count; fi++) {
                if (s_ps.finished_gsg[fi][0] == g &&
                    s_ps.finished_gsg[fi][1] == sg) {
                    fins++;
                }
            }
            R_CHECK(&rig, fins == 1);   /* this in-range subgroup, sealed once */
        }
    }

    relay_bind_test_max_actions = 0;
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: dataplane_delivery_oracle (aq=%u)\n", max_actions);
    }
    return f;
}

/* ---- Datagram + status-object delivery oracle ---------------------------- *
 * Extends the delivery-oracle framework to the object shapes it left open:
 * datagram-preference objects, datagram status objects, and stream status
 * objects.
 * Proves the relay forwards each shape with the right datagram flag, payload
 * presence, and status code, and that out-of-range shapes are filtered. */
static int
dataplane_datagram_status_oracle(uint32_t max_actions)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    relay_bind_test_max_actions = max_actions;
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        relay_bind_test_max_actions = 0;
        printf("FAIL: dataplane_datagram_status_oracle rig create (aq=%u)\n",
               max_actions);
        return 1;
    }
    int pumps = max_actions == 0 ? 40 : 200;
    conn_t *pub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub && sub);
    relay_bind_test_rig_pump(&rig, 4);

    moq_bytes_t nsp[2] = { B("live"), B("cam") };
    moq_publish_namespace_cfg_t pcfg;
    memset(&pcfg, 0, sizeof(pcfg));
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg, rig.now,
                                                &ann) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 4);

    moq_subscribe_cfg_t scfg;
    memset(&scfg, 0, sizeof(scfg));
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    scfg.track_name = B("v");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE;
    scfg.start_group = 0;
    scfg.start_object = 0;
    scfg.end_group = 1;   /* in-range groups 0,1; group 2 is the terminal */
    moq_subscription_t sh;
    R_CHECK(&rig, moq_session_subscribe(sub->peer, &scfg, rig.now, &sh) ==
                      MOQ_OK);
    relay_bind_test_rig_pump(&rig, 6);
    peer_state_t pub_ps, s_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&s_ps, 0, sizeof(s_ps));
    relay_bind_test_peer_drain(&rig, pub, &pub_ps, true, false, 0, 0);
    R_CHECK(&rig, pub_ps.up_seen);
    relay_bind_test_rig_pump(&rig, 6);

    /* group 0: a datagram object (o0), then a datagram status EOG (o1). */
    {
        uint8_t body[4] = { 0xA0, 0, 0, 0 };
        moq_rcbuf_t *pl = NULL;
        R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                          MOQ_OK);
        DP_RETRY(&rig, moq_session_send_object_datagram(
                           pub->peer, pub_ps.up_sub, 0, 0, 7, false, pl, NULL,
                           0, rig.now));
        moq_rcbuf_decref(pl);
    }
    DP_RETRY(&rig, moq_session_send_status_datagram(
                       pub->peer, pub_ps.up_sub, 0, 1, 7,
                       MOQ_OBJECT_END_OF_GROUP, rig.now));
    relay_bind_test_rig_pump(&rig, 6);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);

    /* group 1: a stream subgroup with a normal object (o0), then a stream
     * status EOG object (o1), then close. */
    {
        moq_subgroup_cfg_t sgc;
        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = 1;
        sgc.subgroup_id = 0;
        sgc.publisher_priority = 0;
        moq_subgroup_handle_t sgh;
        DP_RETRY(&rig, moq_session_open_subgroup(pub->peer, pub_ps.up_sub,
                                                 &sgc, rig.now, &sgh));
        uint8_t body[4] = { 0xB0, 1, 0, 0 };
        moq_rcbuf_t *pl = NULL;
        R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                          MOQ_OK);
        DP_RETRY(&rig, moq_session_write_object(pub->peer, sgh, 0, pl,
                                                rig.now));
        moq_rcbuf_decref(pl);
        DP_RETRY(&rig, moq_session_write_status_object(
                           pub->peer, sgh, 1, MOQ_OBJECT_END_OF_GROUP,
                           rig.now));
        DP_RETRY(&rig, moq_session_close_subgroup(pub->peer, sgh, rig.now));
    }
    relay_bind_test_rig_pump(&rig, 6);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);

    /* group 2 (out of range): a datagram object, moving the live edge past the
     * range end so the subscription terminates. Must NOT be delivered. */
    {
        uint8_t body[4] = { 0xC0, 2, 0, 0 };
        moq_rcbuf_t *pl = NULL;
        R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                          MOQ_OK);
        DP_RETRY(&rig, moq_session_send_object_datagram(
                           pub->peer, pub_ps.up_sub, 2, 0, 7, false, pl, NULL,
                           0, rig.now));
        moq_rcbuf_decref(pl);
    }
    for (int k = 0; k < pumps; k++) {
        relay_bind_test_rig_pump(&rig, 1);
        relay_bind_test_peer_drain(&rig, pub, &pub_ps, false, false, 0, 0);
        relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
        if (s_ps.done_seen) {
            break;
        }
    }
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);

    /* ---- oracle (wire-observable only) ---- *
     * The terminal is the range completion (the group-2 datagram moves the live
     * edge past end_group=1); the EOG statuses end their group/subgroup, not the
     * subscription. Each in-range shape is delivered exactly once with the right
     * datagram flag, payload presence, status code, and payload fingerprint. EOG
     * is carried by the STATUS field (st == END_OF_GROUP), NOT the end_of_group
     * header bit, so every delivered record has eog == false. */
    static const struct {
        uint64_t g, o;
        bool     dg, pn;
        uint8_t  st, fp;
    } want[4] = {
        { 0, 0, true,  false, MOQ_OBJECT_NORMAL,       0xA0 },  /* datagram obj  */
        { 0, 1, true,  true,  MOQ_OBJECT_END_OF_GROUP, 0x00 },  /* datagram stat */
        { 1, 0, false, false, MOQ_OBJECT_NORMAL,       0xB0 },  /* stream obj    */
        { 1, 1, false, true,  MOQ_OBJECT_END_OF_GROUP, 0x00 },  /* stream stat   */
    };
    R_CHECK(&rig, s_ps.done_seen);
    R_CHECK(&rig, s_ps.dlv_overflow == 0);
    R_CHECK(&rig, s_ps.dlv_n == 4);
    int seen[4] = { 0, 0, 0, 0 };
    for (int i = 0; i < s_ps.dlv_n; i++) {
        dlv_rec_t *d = &s_ps.dlv[i];
        R_CHECK(&rig, !d->after_done);       /* nothing after terminal   */
        R_CHECK(&rig, d->sub == sh._opaque); /* attributed to this sub   */
        R_CHECK(&rig, d->group < 2);         /* in range: group 2 filtered */
        R_CHECK(&rig, !d->eog);              /* EOG via status, not the bit */
        int m = -1;
        for (int w = 0; w < 4; w++) {
            if (want[w].g == d->group && want[w].o == d->object_id) {
                m = w;
                break;
            }
        }
        R_CHECK(&rig, m >= 0);               /* matched an expected shape */
        if (m >= 0) {
            seen[m]++;
            R_CHECK(&rig, d->datagram == want[m].dg);
            R_CHECK(&rig, d->payload_null == want[m].pn);
            R_CHECK(&rig, d->status == want[m].st);
            R_CHECK(&rig, d->fp == want[m].fp);
        }
    }
    for (int w = 0; w < 4; w++) {
        R_CHECK(&rig, seen[w] == 1);         /* each shape exactly once  */
    }
    /* The one and only subgroup FIN is exactly (group 1, subgroup 0) — the
     * group-1 stream subgroup. Group-0 datagram/status shapes carry no subgroup
     * and the out-of-range group 2 is filtered, so neither may FIN. A total-only
     * check would pass on a bogus FIN (e.g. for the datagram group) masking a
     * missing real one; assert the recorded FIN identity precisely. */
    R_CHECK(&rig, s_ps.subgroup_finished == 1);
    int fin_1_0 = 0;
    for (int fi = 0; fi < s_ps.finished_count; fi++) {
        if (s_ps.finished_gsg[fi][0] == 1 && s_ps.finished_gsg[fi][1] == 0) {
            fin_1_0++;
        }
    }
    R_CHECK(&rig, fin_1_0 == 1);   /* the FIN is (group 1, subgroup 0), once */

    relay_bind_test_max_actions = 0;
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: dataplane_datagram_status_oracle (aq=%u)\n", max_actions);
    }
    return f;
}

/* ---- Retained pre-subscribe replay oracle -------------------------------- *
 * A subscriber that joins a WARM track (after the original subscriber left and
 * the relay released upstream) must be replayed the retained in-range history
 * exactly once, then the live tail — no duplicate, no gap, no out-of-filter
 * leak, and exactly one warm-track upstream rejoin. */
static uint8_t
re_tag(uint64_t g, uint64_t oid)
{
    return (uint8_t)(0x50u + g * 16u + oid);   /* distinct, nonzero */
}

/* A subgroup of `n` tagged objects with ids [oid_base, oid_base+n), plain FIN. */
static void
re_send_subgroup(rig_t *r, conn_t *pub, moq_subscription_t up_sub,
                 uint64_t group, uint64_t subgroup, uint64_t oid_base,
                 uint64_t n)
{
    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = group;
    sgc.subgroup_id = subgroup;
    sgc.publisher_priority = 0;
    moq_subgroup_handle_t sgh;
    DP_RETRY(r, moq_session_open_subgroup(pub->peer, up_sub, &sgc, r->now,
                                          &sgh));
    for (uint64_t i = 0; i < n; i++) {
        uint64_t oid = oid_base + i;
        uint8_t body[4] = { re_tag(group, oid), (uint8_t)group,
                            (uint8_t)subgroup, (uint8_t)oid };
        moq_rcbuf_t *pl = NULL;
        R_CHECK(r, moq_rcbuf_create(&r->alloc->vt, body, sizeof(body), &pl) ==
                       MOQ_OK);
        DP_RETRY(r, moq_session_write_object(pub->peer, sgh, oid, pl, r->now));
        moq_rcbuf_decref(pl);
    }
    DP_RETRY(r, moq_session_close_subgroup(pub->peer, sgh, r->now));
}

static int
dataplane_retained_replay_oracle(uint32_t max_actions)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    relay_bind_test_max_actions = max_actions;
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        relay_bind_test_max_actions = 0;
        printf("FAIL: dataplane_retained_replay_oracle rig create (aq=%u)\n",
               max_actions);
        return 1;
    }
    int pumps = max_actions == 0 ? 40 : 200;
    conn_t *pub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *subA = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *subB = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub && subA && subB);
    relay_bind_test_rig_pump(&rig, 4);

    moq_bytes_t nsp[2] = { B("live"), B("cam") };
    moq_publish_namespace_cfg_t pcfg;
    memset(&pcfg, 0, sizeof(pcfg));
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg, rig.now,
                                                &ann) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 4);

    /* Sub A pulls the retained history into the relay log. */
    moq_subscribe_cfg_t scfgA;
    memset(&scfgA, 0, sizeof(scfgA));
    moq_subscribe_cfg_init(&scfgA);
    scfgA.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    scfgA.track_name = B("v");
    scfgA.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    scfgA.start_group = 0;
    scfgA.start_object = 0;
    moq_subscription_t shA;
    R_CHECK(&rig, moq_session_subscribe(subA->peer, &scfgA, rig.now, &shA) ==
                      MOQ_OK);
    relay_bind_test_rig_pump(&rig, 6);
    peer_state_t pub_ps, sA_ps, sB_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&sA_ps, 0, sizeof(sA_ps));
    memset(&sB_ps, 0, sizeof(sB_ps));
    relay_bind_test_peer_drain(&rig, pub, &pub_ps, true, false, 0, 0); /* accept upstream */
    R_CHECK(&rig, pub_ps.up_seen);
    relay_bind_test_rig_pump(&rig, 6);

    /* Retained matrix: group 0 (sg0 o0-2), group 1 (sg0 o0-2, sg1 o3-5). */
    re_send_subgroup(&rig, pub, pub_ps.up_sub, 0, 0, 0, 3);
    relay_bind_test_rig_pump(&rig, 4);
    relay_bind_test_peer_drain(&rig, subA, &sA_ps, false, false, 0, 0);
    re_send_subgroup(&rig, pub, pub_ps.up_sub, 1, 0, 0, 3);
    relay_bind_test_rig_pump(&rig, 4);
    relay_bind_test_peer_drain(&rig, subA, &sA_ps, false, false, 0, 0);
    re_send_subgroup(&rig, pub, pub_ps.up_sub, 1, 1, 3, 3);
    relay_bind_test_rig_pump(&rig, 4);
    relay_bind_test_peer_drain(&rig, subA, &sA_ps, false, false, 0, 0);

    /* Sub A leaves; the track lingers then warms (relay releases upstream). */
    R_CHECK(&rig, moq_session_unsubscribe(subA->peer, shA, rig.now) == MOQ_OK);
    for (int k = 0; k < pumps; k++) {
        relay_bind_test_rig_pump(&rig, 1);
        relay_bind_test_peer_drain(&rig, pub, &pub_ps, false, false, 0, 0);
        if (pub_ps.unsubscribed >= 1) {
            break;
        }
    }
    R_CHECK(&rig, pub_ps.unsubscribed == 1);   /* warm: upstream released */
    int up_subs_before = pub_ps.up_subs;

    /* Sub B joins the WARM track with a subset range [1,2]: it must replay
     * retained group 1 (NOT the retained group 0, before its start), then the
     * live tail (group 2). */
    moq_subscribe_cfg_t scfgB;
    memset(&scfgB, 0, sizeof(scfgB));
    moq_subscribe_cfg_init(&scfgB);
    scfgB.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    scfgB.track_name = B("v");
    scfgB.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE;
    scfgB.start_group = 1;
    scfgB.start_object = 0;
    scfgB.end_group = 2;
    moq_subscription_t shB;
    R_CHECK(&rig, moq_session_subscribe(subB->peer, &scfgB, rig.now, &shB) ==
                      MOQ_OK);
    relay_bind_test_rig_pump(&rig, 4);
    relay_bind_test_peer_drain(&rig, pub, &pub_ps, true, true, 1, 5); /* warm rejoin */
    R_CHECK(&rig, pub_ps.up_subs == up_subs_before + 1);   /* exactly one */
    relay_bind_test_rig_pump(&rig, 6);
    relay_bind_test_peer_drain(&rig, subB, &sB_ps, false, false, 0, 0);

    /* Live tail (group 2), then out-of-range group 3 completes the range. */
    re_send_subgroup(&rig, pub, pub_ps.up_sub, 2, 0, 0, 3);
    relay_bind_test_rig_pump(&rig, 4);
    relay_bind_test_peer_drain(&rig, subB, &sB_ps, false, false, 0, 0);
    re_send_subgroup(&rig, pub, pub_ps.up_sub, 3, 0, 0, 3);
    for (int k = 0; k < pumps; k++) {
        relay_bind_test_rig_pump(&rig, 1);
        relay_bind_test_peer_drain(&rig, pub, &pub_ps, false, false, 0, 0);
        relay_bind_test_peer_drain(&rig, subB, &sB_ps, false, false, 0, 0);
        if (sB_ps.done_seen) {
            break;
        }
    }
    relay_bind_test_peer_drain(&rig, subB, &sB_ps, false, false, 0, 0);

    /* ---- oracle (wire-observable only) ---- *
     * Sub B's delivered set is exactly the retained group 1 (both subgroups)
     * plus the live group-2 tail: 9 objects. The retained group 0 (before B's
     * start) and the out-of-range group 3 (which completes the range) are both
     * absent. Exactly one warm-track upstream rejoin happened (A's initial
     * SUBSCRIBE + B's rejoin = 2), i.e. no resubscribe storm. */
    static const struct { uint64_t g, o; } want[9] = {
        { 1, 0 }, { 1, 1 }, { 1, 2 },   /* retained group 1, subgroup 0 */
        { 1, 3 }, { 1, 4 }, { 1, 5 },   /* retained group 1, subgroup 1 */
        { 2, 0 }, { 2, 1 }, { 2, 2 },   /* live tail  group 2, subgroup 0 */
    };
    R_CHECK(&rig, sB_ps.done_seen);
    R_CHECK(&rig, sB_ps.dlv_overflow == 0);
    R_CHECK(&rig, sB_ps.dlv_n == 9);
    R_CHECK(&rig, pub_ps.up_subs == 2);   /* one warm rejoin, no storm */
    int seen[9] = { 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    for (int i = 0; i < sB_ps.dlv_n; i++) {
        dlv_rec_t *d = &sB_ps.dlv[i];
        R_CHECK(&rig, !d->after_done);           /* nothing after terminal   */
        R_CHECK(&rig, d->sub == shB._opaque);    /* attributed to sub B      */
        R_CHECK(&rig, d->group == 1 || d->group == 2); /* in range, not 0/3  */
        R_CHECK(&rig, !d->payload_null);
        R_CHECK(&rig, !d->datagram);
        R_CHECK(&rig, d->status == MOQ_OBJECT_NORMAL);
        R_CHECK(&rig, d->subgroup == d->object_id / 3);  /* stream placement */
        R_CHECK(&rig, d->fp == re_tag(d->group, d->object_id));
        int m = -1;
        for (int w = 0; w < 9; w++) {
            if (want[w].g == d->group && want[w].o == d->object_id) {
                m = w;
                break;
            }
        }
        R_CHECK(&rig, m >= 0);
        if (m >= 0) {
            seen[m]++;
        }
    }
    for (int w = 0; w < 9; w++) {
        R_CHECK(&rig, seen[w] == 1);   /* each expected object exactly once  */
    }
    /* Per-stream object order + exact subgroup FIN identity. The three replayed/
     * live streams each seal exactly once; no extra or missing FIN. */
    static const struct { uint64_t g, sg; } streams[3] = {
        { 1, 0 }, { 1, 1 }, { 2, 0 },
    };
    R_CHECK(&rig, sB_ps.subgroup_finished == 3);
    for (int s = 0; s < 3; s++) {
        int64_t last = -1;
        for (int i = 0; i < sB_ps.dlv_n; i++) {
            dlv_rec_t *d = &sB_ps.dlv[i];
            if (d->group == streams[s].g && d->subgroup == streams[s].sg) {
                R_CHECK(&rig, (int64_t)d->object_id > last);
                last = (int64_t)d->object_id;
            }
        }
        int fins = 0;
        for (int fi = 0; fi < sB_ps.finished_count; fi++) {
            if (sB_ps.finished_gsg[fi][0] == streams[s].g &&
                sB_ps.finished_gsg[fi][1] == streams[s].sg) {
                fins++;
            }
        }
        R_CHECK(&rig, fins == 1);   /* this stream sealed exactly once */
    }

    relay_bind_test_max_actions = 0;
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: dataplane_retained_replay_oracle (aq=%u)\n", max_actions);
    }
    return f;
}

/* ---- Late-FIN seal oracle -------------------------------------------------- *
 * A subgroup FIN that reaches the relay only AFTER the subscriber already
 * received the subgroup's last object must still FIN the downstream stream.
 * The seal lands with nothing left to deliver, so no record delivery will
 * ever carry the final-record flag — the FIN rides the acknowledged SEAL
 * notice instead. Before that notice existed, this stream never closed. */
static int
dataplane_late_fin_oracle(uint32_t max_actions)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    relay_bind_test_max_actions = max_actions;
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        relay_bind_test_max_actions = 0;
        printf("FAIL: dataplane_late_fin_oracle rig create (aq=%u)\n",
               max_actions);
        return 1;
    }
    int pumps = max_actions == 0 ? 40 : 200;
    conn_t *pub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub && sub);
    relay_bind_test_rig_pump(&rig, 4);

    moq_bytes_t nsp[2] = { B("live"), B("cam") };
    moq_publish_namespace_cfg_t pcfg;
    memset(&pcfg, 0, sizeof(pcfg));
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg, rig.now,
                                                &ann) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 4);

    moq_subscribe_cfg_t scfg;
    memset(&scfg, 0, sizeof(scfg));
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    scfg.track_name = B("v");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    scfg.start_group = 0;
    scfg.start_object = 0;
    moq_subscription_t sh;
    R_CHECK(&rig, moq_session_subscribe(sub->peer, &scfg, rig.now, &sh) ==
                      MOQ_OK);
    relay_bind_test_rig_pump(&rig, 6);
    peer_state_t pub_ps, s_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&s_ps, 0, sizeof(s_ps));
    relay_bind_test_peer_drain(&rig, pub, &pub_ps, true, false, 0, 0); /* accept upstream */
    R_CHECK(&rig, pub_ps.up_seen);
    relay_bind_test_rig_pump(&rig, 6);

    /* One object on (group 0, subgroup 0); the upstream stream stays OPEN. */
    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = 0;
    sgc.subgroup_id = 0;
    sgc.publisher_priority = 0;
    moq_subgroup_handle_t sgh;
    DP_RETRY(&rig, moq_session_open_subgroup(pub->peer, pub_ps.up_sub, &sgc,
                                             rig.now, &sgh));
    {
        uint8_t body[4] = { 0x77, 0, 0, 0 };
        moq_rcbuf_t *pl = NULL;
        R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                          MOQ_OK);
        DP_RETRY(&rig, moq_session_write_object(pub->peer, sgh, 0, pl,
                                                rig.now));
        moq_rcbuf_decref(pl);
    }
    /* The subscriber receives the object while the subgroup is unsealed —
     * this delivery cannot carry the final-record flag. */
    for (int k = 0; k < pumps; k++) {
        relay_bind_test_rig_pump(&rig, 1);
        relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
        if (s_ps.dlv_n >= 1) {
            break;
        }
    }
    R_CHECK(&rig, s_ps.dlv_n == 1);
    R_CHECK(&rig, s_ps.dlv[0].fp == 0x77);
    relay_bind_test_rig_pump(&rig, 4);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.subgroup_finished == 0);   /* genuinely late FIN */

    /* The late FIN: sealed with nothing left to deliver. */
    DP_RETRY(&rig, moq_session_close_subgroup(pub->peer, sgh, rig.now));
    for (int k = 0; k < pumps; k++) {
        relay_bind_test_rig_pump(&rig, 1);
        relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
        if (s_ps.subgroup_finished >= 1) {
            break;
        }
    }
    R_CHECK(&rig, s_ps.subgroup_finished == 1);
    int fin_0_0 = 0;
    for (int fi = 0; fi < s_ps.finished_count; fi++) {
        if (s_ps.finished_gsg[fi][0] == 0 && s_ps.finished_gsg[fi][1] == 0) {
            fin_0_0++;
        }
    }
    R_CHECK(&rig, fin_0_0 == 1);       /* exactly (group 0, subgroup 0)   */
    R_CHECK(&rig, s_ps.dlv_n == 1);    /* the notice delivered no record  */
    relay_bind_test_rig_pump(&rig, 4);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.subgroup_finished == 1);   /* acknowledged once   */

    relay_bind_test_max_actions = 0;
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: dataplane_late_fin_oracle (aq=%u)\n", max_actions);
    }
    return f;
}

/* ---- FIN-under-backpressure oracle ---------------------------------------- *
 * The final object's write fits the pump's action quota but the subgroup
 * close would not: with a quota of ONE action per pump, the close can never
 * share a pump with the delivery. The FIN must still arrive — it rides the
 * acknowledged SEAL notice, whose close retries on WOULD_BLOCK — and the
 * object must not be re-delivered. (A close piggybacked on the record
 * delivery and swallowed on WOULD_BLOCK would lose the FIN forever.) */
static int
dataplane_fin_backpressure_oracle(void)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    relay_bind_test_max_actions = 1; /* one session action per pump */
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        relay_bind_test_max_actions = 0;
        printf("FAIL: dataplane_fin_backpressure_oracle rig create\n");
        return 1;
    }
    int pumps = 300;
    conn_t *pub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub && sub);
    relay_bind_test_rig_pump(&rig, 8);

    moq_bytes_t nsp[2] = { B("live"), B("cam") };
    moq_publish_namespace_cfg_t pcfg;
    memset(&pcfg, 0, sizeof(pcfg));
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg, rig.now,
                                                &ann) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 8);

    moq_subscribe_cfg_t scfg;
    memset(&scfg, 0, sizeof(scfg));
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    scfg.track_name = B("v");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    scfg.start_group = 0;
    scfg.start_object = 0;
    moq_subscription_t sh;
    R_CHECK(&rig, moq_session_subscribe(sub->peer, &scfg, rig.now, &sh) ==
                      MOQ_OK);
    peer_state_t pub_ps, s_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&s_ps, 0, sizeof(s_ps));
    for (int k = 0; k < pumps && !pub_ps.up_seen; k++) {
        relay_bind_test_rig_pump(&rig, 1);
        relay_bind_test_peer_drain(&rig, pub, &pub_ps, true, false, 0, 0);
    }
    R_CHECK(&rig, pub_ps.up_seen);

    /* One object, then the upstream FIN right behind it — the seal reaches
     * the relay while the record is still owed downstream. */
    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = 0;
    sgc.subgroup_id = 0;
    sgc.publisher_priority = 0;
    moq_subgroup_handle_t sgh;
    DP_RETRY(&rig, moq_session_open_subgroup(pub->peer, pub_ps.up_sub, &sgc,
                                             rig.now, &sgh));
    {
        uint8_t body[4] = { 0x66, 0, 0, 0 };
        moq_rcbuf_t *pl = NULL;
        R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                          MOQ_OK);
        DP_RETRY(&rig, moq_session_write_object(pub->peer, sgh, 0, pl,
                                                rig.now));
        moq_rcbuf_decref(pl);
    }
    DP_RETRY(&rig, moq_session_close_subgroup(pub->peer, sgh, rig.now));

    /* Under the 1-action quota the delivery and the close land on different
     * pumps; both must eventually arrive, each exactly once. */
    for (int k = 0; k < pumps; k++) {
        relay_bind_test_rig_pump(&rig, 1);
        relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
        if (s_ps.dlv_n >= 1 && s_ps.subgroup_finished >= 1) {
            break;
        }
    }
    R_CHECK(&rig, s_ps.dlv_n == 1);
    R_CHECK(&rig, s_ps.dlv[0].fp == 0x66);
    R_CHECK(&rig, s_ps.subgroup_finished == 1);
    int fin_0_0 = 0;
    for (int fi = 0; fi < s_ps.finished_count; fi++) {
        if (s_ps.finished_gsg[fi][0] == 0 && s_ps.finished_gsg[fi][1] == 0) {
            fin_0_0++;
        }
    }
    R_CHECK(&rig, fin_0_0 == 1);
    relay_bind_test_rig_pump(&rig, 6);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.dlv_n == 1);              /* no repeated object   */
    R_CHECK(&rig, s_ps.subgroup_finished == 1);  /* exactly one FIN      */

    relay_bind_test_max_actions = 0;
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: dataplane_fin_backpressure_oracle\n");
    }
    return f;
}

/* ---- Header-EOG cadence oracle ---------------------------------------------- *
 * end_of_group is subgroup-header METADATA: opening an EOG subgroup does not
 * make its first object a terminal, and objects keep flowing until the actual
 * upstream FIN. Under a one-action-per-pump quota: object 0 arrives with the
 * header bit preserved and NO FIN; object 1 follows; only the explicit
 * upstream close produces the (notice-driven, retry-safe) downstream FIN —
 * exactly one, with no duplicate object afterwards. */
static int
dataplane_eog_backpressure_oracle(bool streaming, moq_version_t ver)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    relay_bind_test_max_actions = 1; /* one session action per pump */
    /* streaming = the production relay ingest representation (chunk-through
     * OPEN records); the header-EOG metadata must survive it identically. */
    relay_bind_test_server_streaming_objects = streaming;
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        relay_bind_test_max_actions = 0;
        relay_bind_test_server_streaming_objects = false;
        printf("FAIL: dataplane_eog_backpressure_oracle rig create\n");
        return 1;
    }
    int pumps = 300;
    conn_t *pub = relay_bind_test_rig_connect(&rig, ver);
    conn_t *sub = relay_bind_test_rig_connect(&rig, ver);
    relay_bind_test_server_streaming_objects = false;
    R_CHECK(&rig, pub && sub);
    relay_bind_test_rig_pump(&rig, 8);

    moq_bytes_t nsp[2] = { B("live"), B("cam") };
    moq_publish_namespace_cfg_t pcfg;
    memset(&pcfg, 0, sizeof(pcfg));
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg, rig.now,
                                                &ann) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 8);

    moq_subscribe_cfg_t scfg;
    memset(&scfg, 0, sizeof(scfg));
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    scfg.track_name = B("v");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    scfg.start_group = 0;
    scfg.start_object = 0;
    moq_subscription_t sh;
    R_CHECK(&rig, moq_session_subscribe(sub->peer, &scfg, rig.now, &sh) ==
                      MOQ_OK);
    peer_state_t pub_ps, s_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&s_ps, 0, sizeof(s_ps));
    for (int k = 0; k < pumps && !pub_ps.up_seen; k++) {
        relay_bind_test_rig_pump(&rig, 1);
        relay_bind_test_peer_drain(&rig, pub, &pub_ps, true, false, 0, 0);
    }
    R_CHECK(&rig, pub_ps.up_seen);

    /* An EOG subgroup: the header bit rides every record; nothing about it
     * terminates the stream. */
    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = 0;
    sgc.subgroup_id = 0;
    sgc.publisher_priority = 0;
    sgc.end_of_group = true;
    moq_subgroup_handle_t sgh;
    DP_RETRY(&rig, moq_session_open_subgroup(pub->peer, pub_ps.up_sub, &sgc,
                                             rig.now, &sgh));
    {
        uint8_t body[4] = { 0x55, 0, 0, 0 };
        moq_rcbuf_t *pl = NULL;
        R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                          MOQ_OK);
        DP_RETRY(&rig, moq_session_write_object(pub->peer, sgh, 0, pl,
                                                rig.now));
        moq_rcbuf_decref(pl);
    }
    /* Object 0 arrives with the header bit — and NO FIN: an EOG header must
     * never be treated as a per-record terminal. */
    for (int k = 0; k < pumps && s_ps.dlv_n < 1; k++) {
        relay_bind_test_rig_pump(&rig, 1);
        relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    }
    R_CHECK(&rig, s_ps.dlv_n == 1);
    R_CHECK(&rig, s_ps.dlv[0].fp == 0x55);
    R_CHECK(&rig, s_ps.dlv[0].eog);              /* header bit preserved */
    relay_bind_test_rig_pump(&rig, 8);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.subgroup_finished == 0);  /* stream still open    */

    /* Object 1, then the actual graceful terminal. */
    {
        uint8_t body[4] = { 0x56, 0, 0, 1 };
        moq_rcbuf_t *pl = NULL;
        R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                          MOQ_OK);
        DP_RETRY(&rig, moq_session_write_object(pub->peer, sgh, 1, pl,
                                                rig.now));
        moq_rcbuf_decref(pl);
    }
    DP_RETRY(&rig, moq_session_close_subgroup(pub->peer, sgh, rig.now));

    /* Under the quota the delivery and the close land on different pumps;
     * both objects exactly once, then exactly one FIN. */
    for (int k = 0; k < pumps; k++) {
        relay_bind_test_rig_pump(&rig, 1);
        relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
        if (s_ps.dlv_n >= 2 && s_ps.subgroup_finished >= 1) {
            break;
        }
    }
    R_CHECK(&rig, s_ps.dlv_n == 2);
    R_CHECK(&rig, s_ps.dlv[1].fp == 0x56);
    R_CHECK(&rig, s_ps.dlv[1].eog);
    R_CHECK(&rig, s_ps.subgroup_finished == 1);
    int fin_0_0 = 0;
    for (int fi = 0; fi < s_ps.finished_count; fi++) {
        if (s_ps.finished_gsg[fi][0] == 0 && s_ps.finished_gsg[fi][1] == 0) {
            fin_0_0++;
        }
    }
    R_CHECK(&rig, fin_0_0 == 1);
    relay_bind_test_rig_pump(&rig, 6);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.dlv_n == 2);              /* no repeated object   */
    R_CHECK(&rig, s_ps.subgroup_finished == 1);  /* exactly one FIN      */

    relay_bind_test_max_actions = 0;
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: dataplane_eog_backpressure_oracle (streaming=%d v=%d)\n",
               streaming ? 1 : 0, (int)ver);
    }
    return f;
}

/* ---- FIRST_OBJECT forwarding ----------------------------------------------- *
 * Draft-18 and draft-22 §2.2: "A relay forwarding a subgroup that begins with
 * the first object ever published in that subgroup MUST set the FIRST_OBJECT
 * bit." The origin opens subgroup (0,0) with the bit and subgroup (1,0)
 * without it. The live subscriber's stream for (0,0) begins with that first
 * object and carries the bit on object 0 only; (1,0) carries none. A second
 * subscriber served from the cache starting at (0,1) gets a stream that does
 * not begin with the first object: no bit. Draft-16 has no such bit. Both the
 * whole-object and the chunk-through ingest paths carry the mark. */
static int
dataplane_first_object_forward(bool streaming, moq_version_t ver)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    relay_bind_test_server_streaming_objects = streaming;
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        relay_bind_test_server_streaming_objects = false;
        printf("FAIL: dataplane_first_object_forward rig create\n");
        return 1;
    }
    const bool has_bit = ver != MOQ_VERSION_DRAFT_16;
    int pumps = 300;
    conn_t *pub = relay_bind_test_rig_connect(&rig, ver);
    conn_t *sub = relay_bind_test_rig_connect(&rig, ver);
    conn_t *late = relay_bind_test_rig_connect(&rig, ver);
    relay_bind_test_server_streaming_objects = false;
    R_CHECK(&rig, pub && sub && late);
    relay_bind_test_rig_pump(&rig, 8);

    moq_bytes_t nsp[2] = { B("live"), B("cam") };
    moq_publish_namespace_cfg_t pcfg;
    memset(&pcfg, 0, sizeof(pcfg));
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg, rig.now,
                                                &ann) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 8);

    moq_subscribe_cfg_t scfg;
    memset(&scfg, 0, sizeof(scfg));
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    scfg.track_name = B("v");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    scfg.start_group = 0;
    scfg.start_object = 0;
    moq_subscription_t sh;
    R_CHECK(&rig, moq_session_subscribe(sub->peer, &scfg, rig.now, &sh) ==
                      MOQ_OK);
    peer_state_t pub_ps, s_ps, l_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&s_ps, 0, sizeof(s_ps));
    memset(&l_ps, 0, sizeof(l_ps));
    for (int k = 0; k < pumps && !pub_ps.up_seen; k++) {
        relay_bind_test_rig_pump(&rig, 1);
        relay_bind_test_peer_drain(&rig, pub, &pub_ps, true, false, 0, 0);
    }
    R_CHECK(&rig, pub_ps.up_seen);

    for (uint64_t g = 0; g < 2; g++) {
        moq_subgroup_cfg_t sgc;
        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = g;
        sgc.subgroup_id = 0;
        sgc.first_object = g == 0;
        moq_subgroup_handle_t sgh;
        DP_RETRY(&rig, moq_session_open_subgroup(pub->peer, pub_ps.up_sub,
                                                 &sgc, rig.now, &sgh));
        for (uint64_t o = 0; o < 2; o++) {
            uint8_t body[2] = { (uint8_t)(0x60 + 2 * g + o), 0 };
            moq_rcbuf_t *pl = NULL;
            R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                              MOQ_OK);
            DP_RETRY(&rig, moq_session_write_object(pub->peer, sgh, o, pl,
                                                    rig.now));
            moq_rcbuf_decref(pl);
        }
        DP_RETRY(&rig, moq_session_close_subgroup(pub->peer, sgh, rig.now));
    }
    for (int k = 0; k < pumps && s_ps.dlv_n < 4; k++) {
        relay_bind_test_rig_pump(&rig, 1);
        relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    }
    R_CHECK(&rig, s_ps.dlv_n == 4);
    for (int i = 0; i < s_ps.dlv_n && i < 4; i++) {
        const dlv_rec_t *dr = &s_ps.dlv[i];
        bool want = has_bit && dr->group == 0 && dr->object_id == 0;
        R_CHECK(&rig, dr->first == want);
    }

    /* Served from the cache starting at (0,1): not the subgroup's start. */
    scfg.start_group = 0;
    scfg.start_object = 1;
    moq_subscription_t lh;
    R_CHECK(&rig, moq_session_subscribe(late->peer, &scfg, rig.now, &lh) ==
                      MOQ_OK);
    for (int k = 0; k < pumps && l_ps.dlv_n < 3; k++) {
        relay_bind_test_rig_pump(&rig, 1);
        relay_bind_test_peer_drain(&rig, late, &l_ps, false, false, 0, 0);
    }
    R_CHECK(&rig, l_ps.dlv_n == 3);
    for (int i = 0; i < l_ps.dlv_n && i < 3; i++) {
        R_CHECK(&rig, !(l_ps.dlv[i].group == 0 && l_ps.dlv[i].object_id == 0));
        R_CHECK(&rig, !l_ps.dlv[i].first);
    }

    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: dataplane_first_object_forward (streaming=%d v=%d)\n",
               streaming ? 1 : 0, (int)ver);
    }
    return f;
}

static void ev_write_obj(rig_t *r, conn_t *pub, moq_subgroup_handle_t h,
                         uint64_t group, uint64_t object_id);

/* ---- Header-EOG contradictory-final oracle ---------------------------------- *
 * An EOG subgroup reaches object 20 while ANOTHER subgroup admits object 21
 * before the EOG FIN. The FIN makes 20 the Group's final Object, below
 * object 21 already received: a Malformed Track (draft-18 §2.4.2 condition 4).
 * The relay's upstream session cancels the subscription ("MUST cancel any
 * corresponding subscription"), the relay ends the downstream subscription
 * with PUBLISH_DONE MALFORMED_TRACK ("MUST immediately terminate downstream
 * subscriptions with PUBLISH_DONE"), the cache never declares a final below
 * retained content, and neither session closes. */
static int
dataplane_eog_conflict_fail_closed(void)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: dataplane_eog_conflict_fail_closed rig create\n");
        return 1;
    }
    int pumps = 120;
    conn_t *pub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub && sub);
    relay_bind_test_rig_pump(&rig, 4);

    moq_bytes_t nsp[2] = { B("live"), B("cam") };
    moq_publish_namespace_cfg_t pcfg;
    memset(&pcfg, 0, sizeof(pcfg));
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg, rig.now,
                                                &ann) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 4);
    moq_subscribe_cfg_t scfg;
    memset(&scfg, 0, sizeof(scfg));
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    scfg.track_name = B("v");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    moq_subscription_t sh;
    R_CHECK(&rig, moq_session_subscribe(sub->peer, &scfg, rig.now, &sh) ==
                      MOQ_OK);
    relay_bind_test_rig_pump(&rig, 6);
    peer_state_t pub_ps, s_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&s_ps, 0, sizeof(s_ps));
    relay_bind_test_peer_drain(&rig, pub, &pub_ps, true, false, 0, 0);
    R_CHECK(&rig, pub_ps.up_seen);
    relay_bind_test_rig_pump(&rig, 6);

    /* EOG subgroup 0 with object 20 (left open)... */
    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = 0;
    sgc.subgroup_id = 0;
    sgc.publisher_priority = 0;
    sgc.end_of_group = true;
    moq_subgroup_handle_t sg0;
    DP_RETRY(&rig, moq_session_open_subgroup(pub->peer, pub_ps.up_sub, &sgc,
                                             rig.now, &sg0));
    ev_write_obj(&rig, pub, sg0, 0, 20);
    relay_bind_test_rig_pump(&rig, 6);
    /* ...another subgroup admits object 21 BEFORE the EOG FIN... */
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = 0;
    sgc.subgroup_id = 1;
    sgc.publisher_priority = 0;
    moq_subgroup_handle_t sg1;
    DP_RETRY(&rig, moq_session_open_subgroup(pub->peer, pub_ps.up_sub, &sgc,
                                             rig.now, &sg1));
    ev_write_obj(&rig, pub, sg1, 0, 21);
    relay_bind_test_rig_pump(&rig, 6);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);

    /* ...the FIN's final (20) contradicts received 21: a Malformed Track. */
    DP_RETRY(&rig, moq_session_close_subgroup(pub->peer, sg0, rig.now));
    for (int k = 0; k < pumps; k++) {
        relay_bind_test_rig_pump(&rig, 1);
        relay_bind_test_peer_drain(&rig, pub, &pub_ps, false, false, 0, 0);
        relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
        if (pub_ps.unsubscribed && s_ps.done_count) {
            break;
        }
    }
    R_CHECK(&rig, pub_ps.unsubscribed == 1);
    R_CHECK(&rig, s_ps.done_count == 1 && s_ps.done_code == 0x12);
    R_CHECK(&rig, !pub_ps.session_closed && !s_ps.session_closed);

    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: dataplane_eog_conflict_fail_closed\n");
    }
    return f;
}

/* ---- Eviction / skip correctness oracle ---------------------------------- *
 * When retention evicts data under a live subscriber, the relay must surface the
 * skip (observability), deliver only the coherent set (no dup / no gap / no
 * stale group), and NOT close a still-retained lower group's downstream stream.
 * Seeded from subgroup_eviction_spares_live_lower_group. */
static uint8_t
ev_tag(uint64_t g, uint64_t o)
{
    return (uint8_t)(0x90u + g * 8u + o);   /* distinct per (group, object) */
}

static void
ev_write_obj(rig_t *r, conn_t *pub, moq_subgroup_handle_t h, uint64_t group,
             uint64_t object_id)
{
    uint8_t body[4] = { ev_tag(group, object_id), (uint8_t)group,
                        (uint8_t)object_id, 0x33 };
    moq_rcbuf_t *pl = NULL;
    R_CHECK(r, moq_rcbuf_create(&r->alloc->vt, body, sizeof(body), &pl) ==
                   MOQ_OK);
    R_CHECK(r, moq_session_write_object(pub->peer, h, object_id, pl, r->now) ==
                   MOQ_OK);
    moq_rcbuf_decref(pl);
}

/* Count MOQR_TRACE_CURSOR_SKIP records in the relay's attached trace ring. The
 * production delivery path (the gpos scheduler in moqr_core_next_delivery) emits
 * this trace when it jumps a subscriber past evicted content, so a skip is
 * observable both here and via the downstream eviction-close. */
static int
ev_skip_count(rig_t *r)
{
    moqr_trace_rec_t recs[512];
    size_t n = moqr_trace_read(r->trace, recs, 512);
    int skips = 0;
    for (size_t i = 0; i < n; i++) {
        if (recs[i].kind == MOQR_TRACE_CURSOR_SKIP) {
            skips++;
        }
    }
    return skips;
}

static int
dataplane_eviction_skip_oracle(void)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    relay_bind_test_log_max_groups = 2; /* group 0 evicts when group 2 is ingested */
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        relay_bind_test_log_max_groups = 0;
        printf("FAIL: dataplane_eviction_skip_oracle rig create\n");
        return 1;
    }
    conn_t *pub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub && sub);
    relay_bind_test_rig_pump(&rig, 4);

    moq_bytes_t nsp[2] = { B("live"), B("cam") };
    moq_publish_namespace_cfg_t pcfg;
    memset(&pcfg, 0, sizeof(pcfg));
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg, rig.now,
                                                &ann) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 4);

    moq_subscribe_cfg_t scfg;
    memset(&scfg, 0, sizeof(scfg));
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    scfg.track_name = B("v");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;   /* open-ended */
    scfg.start_group = 0;
    scfg.start_object = 0;
    moq_subscription_t sh;
    R_CHECK(&rig, moq_session_subscribe(sub->peer, &scfg, rig.now, &sh) ==
                      MOQ_OK);
    relay_bind_test_rig_pump(&rig, 4);
    peer_state_t pub_ps, s_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&s_ps, 0, sizeof(s_ps));
    relay_bind_test_peer_drain(&rig, pub, &pub_ps, true, false, 0, 0);
    R_CHECK(&rig, pub_ps.up_seen);
    relay_bind_test_rig_pump(&rig, 4);

    /* Deliver g0o0 AND g1o0 first: both downstream subgroups open, both cursors
     * advanced (held OPEN, no FIN). */
    moq_subgroup_handle_t h0, h1, h2;
    moq_subgroup_cfg_t sc;
    moq_subgroup_cfg_init(&sc);
    sc.group_id = 0;
    sc.subgroup_id = 0;
    R_CHECK(&rig, moq_session_open_subgroup(pub->peer, pub_ps.up_sub, &sc,
                                            rig.now, &h0) == MOQ_OK);
    ev_write_obj(&rig, pub, h0, 0, 0);
    sc.group_id = 1;
    R_CHECK(&rig, moq_session_open_subgroup(pub->peer, pub_ps.up_sub, &sc,
                                            rig.now, &h1) == MOQ_OK);
    ev_write_obj(&rig, pub, h1, 1, 0);
    relay_bind_test_rig_pump(&rig, 8); /* deliver g0o0, g1o0; both cursors advanced */
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);

    int skips_before = ev_skip_count(&rig);

    /* Bundle a NEW group-0 object (g0o1, which the g0 cursor now wants) with
     * group 2, ingested in ONE bind pump: group 2's arrival evicts group 0,
     * dropping g0o1 undelivered, and the acknowledged EVICT_WATERMARK notice
     * precedes group 2's record while group 1 stays RETAINED and open — the
     * exact shape that exposed the watermark bug (a `group < delivered`
     * cleanup would wrongly close group 1). */
    ev_write_obj(&rig, pub, h0, 0, 1);   /* g0o1: evicted undelivered */
    sc.group_id = 2;
    R_CHECK(&rig, moq_session_open_subgroup(pub->peer, pub_ps.up_sub, &sc,
                                            rig.now, &h2) == MOQ_OK);
    ev_write_obj(&rig, pub, h2, 2, 0);
    relay_bind_test_rig_pump(&rig, 8); /* g0 evicts (g0o1 dropped); skip lands on group 2 */
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);

    int skips_after = ev_skip_count(&rig);

    /* More data on the still-RETAINED group 1 (must stay on its open stream). */
    ev_write_obj(&rig, pub, h1, 1, 1);
    relay_bind_test_rig_pump(&rig, 8);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);

    /* ---- oracle (wire-observable only) ---- *
     * Coherent delivered set: the evicted-undelivered g0 o1 is ABSENT, so the set
     * is exactly {g0o0, g1o0, g2o0, g1o1} — no duplicate, no stale/out-of-window
     * object, fingerprints exact. The skip is REPORTED, not silently swallowed:
     * the relay reset the evicted group 0's downstream subgroup (h0 stays open
     * at the origin). A FIN would claim the subgroup complete with g0o1
     * missing: "If a sender closes the stream before delivering all such
     * objects to the QUIC stream, it MUST reset the stream" (draft-18 §11.4.3;
     * draft-22 §11.3.2), and a relay "MUST NOT reorder or drop objects
     * received on a multi-object stream" (draft-18 §9.4; draft-22 §7.4). The
     * still-retained lower group 1 keeps its OPEN stream: NOT closed/reopened
     * (the watermark invariant).
     *
     * The skip is ALSO surfaced in the relay's trace ring: the production gpos
     * scheduler emits exactly one MOQR_TRACE_CURSOR_SKIP for the jump past evicted
     * group 0 (skips_after == skips_before + 1). */
    static const struct { uint64_t g, o; } want[4] = {
        { 0, 0 }, { 1, 0 }, { 2, 0 }, { 1, 1 },
    };
    R_CHECK(&rig, s_ps.dlv_overflow == 0);
    R_CHECK(&rig, s_ps.dlv_n == 4);
    R_CHECK(&rig, !s_ps.done_seen);             /* open-ended: no early terminal */
    R_CHECK(&rig, s_ps.subscribe_errors == 0);
    R_CHECK(&rig, !s_ps.session_closed);
    R_CHECK(&rig, !s_ps.reopen_after_finish);   /* retained g1 stream not reopened */
    R_CHECK(&rig, skips_after == skips_before + 1);  /* one skip, traced */
    int seen[4] = { 0, 0, 0, 0 };
    for (int i = 0; i < s_ps.dlv_n; i++) {
        dlv_rec_t *d = &s_ps.dlv[i];
        R_CHECK(&rig, d->sub == sh._opaque);
        R_CHECK(&rig, !d->payload_null);
        R_CHECK(&rig, !d->datagram);
        R_CHECK(&rig, d->subgroup == 0);
        R_CHECK(&rig, d->fp == ev_tag(d->group, d->object_id));
        int m = -1;
        for (int w = 0; w < 4; w++) {
            if (want[w].g == d->group && want[w].o == d->object_id) {
                m = w;
                break;
            }
        }
        R_CHECK(&rig, m >= 0);
        if (m >= 0) {
            seen[m]++;
        }
    }
    for (int w = 0; w < 4; w++) {
        R_CHECK(&rig, seen[w] == 1);   /* each object exactly once, no dup/dropped */
    }
    int64_t last = -1;   /* group 1 subgroup 0 objects ascending in delivery */
    for (int i = 0; i < s_ps.dlv_n; i++) {
        dlv_rec_t *d = &s_ps.dlv[i];
        if (d->group == 1 && d->subgroup == 0) {
            R_CHECK(&rig, (int64_t)d->object_id > last);
            last = (int64_t)d->object_id;
        }
    }
    /* The skip's wire signal: exactly the evicted group 0's subgroup was reset
     * by the relay (not the origin), and only that one; nothing was FINed. */
    R_CHECK(&rig, s_ps.subgroup_finished == 0);
    R_CHECK(&rig, s_ps.subgroup_resets == 1);
    R_CHECK(&rig, s_ps.reset_gsg[0][0] == 0 && s_ps.reset_gsg[0][1] == 0);

    (void)moq_session_close_subgroup(pub->peer, h1, rig.now);
    (void)moq_session_close_subgroup(pub->peer, h2, rig.now);
    relay_bind_test_rig_pump(&rig, 8);

    relay_bind_test_log_max_groups = 0;
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: dataplane_eviction_skip_oracle\n");
    }
    return f;
}

static int
dataplane_source_end_oracle(bool push, uint32_t max_actions)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    relay_bind_test_max_actions = max_actions;
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        relay_bind_test_max_actions = 0;
        printf("FAIL: dataplane_source_end_oracle rig create (push=%d aq=%u)\n",
               (int)push, max_actions);
        return 1;
    }
    int pumps = max_actions == 0 ? 40 : 200;
    conn_t *pub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub && sub);
    relay_bind_test_rig_pump(&rig, 4);

    moq_bytes_t nsp[2] = { B("live"), B("cam") };
    peer_state_t pub_ps, s_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&s_ps, 0, sizeof(s_ps));
    moq_subscription_t up_sub = { 0 };
    moq_publication_t pubh = { 0 };

    if (push) {
        /* PUSH: the origin publishes the track; the relay accepts it. */
        moq_publish_cfg_t pubc;
        memset(&pubc, 0, sizeof(pubc));
        moq_publish_cfg_init(&pubc);
        pubc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
        pubc.track_name = B("v");
        R_CHECK(&rig, moq_session_publish(pub->peer, &pubc, rig.now, &pubh) ==
                          MOQ_OK);
        relay_bind_test_rig_pump(&rig, 6);
        relay_bind_test_peer_drain(&rig, pub, &pub_ps, false, false, 0, 0);
        R_CHECK(&rig, pub_ps.publish_ok);
    } else {
        /* PULL: the origin announces; the relay subscribes upstream. */
        moq_publish_namespace_cfg_t pcfg;
        memset(&pcfg, 0, sizeof(pcfg));
        moq_publish_namespace_cfg_init(&pcfg);
        pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
        moq_announcement_t ann;
        R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg, rig.now,
                                                    &ann) == MOQ_OK);
        relay_bind_test_rig_pump(&rig, 4);
    }

    /* Open-ended downstream subscriber. */
    moq_subscribe_cfg_t scfg;
    memset(&scfg, 0, sizeof(scfg));
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    scfg.track_name = B("v");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    scfg.start_group = 0;
    scfg.start_object = 0;
    moq_subscription_t sh;
    R_CHECK(&rig, moq_session_subscribe(sub->peer, &scfg, rig.now, &sh) ==
                      MOQ_OK);
    relay_bind_test_rig_pump(&rig, 6);
    if (!push) {
        relay_bind_test_peer_drain(&rig, pub, &pub_ps, true, false, 0, 0); /* accept upstream */
        R_CHECK(&rig, pub_ps.up_seen);
        up_sub = pub_ps.up_sub;
        relay_bind_test_rig_pump(&rig, 6);
    }
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);

    /* Deterministic matrix: group 0 and group 1, each subgroup 0 with objects
     * 0,1, plain FIN. */
    relay_bind_test_se_send_group(&rig, pub, push, up_sub, pubh, 0, 2);
    relay_bind_test_rig_pump(&rig, 6);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    relay_bind_test_se_send_group(&rig, pub, push, up_sub, pubh, 1, 2);
    relay_bind_test_rig_pump(&rig, 6);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);

    /* Source end with a nonzero status. */
    if (push) {
        moq_finish_publish_cfg_t fcfg;
        moq_finish_publish_cfg_init(&fcfg);
        fcfg.status_code = 0x4;
        R_CHECK(&rig, moq_session_finish_publish(pub->peer, pubh, &fcfg,
                                                 rig.now) == MOQ_OK);
    } else {
        moq_done_subscribe_cfg_t dcfg;
        moq_done_subscribe_cfg_init(&dcfg);
        dcfg.status_code = 0x4;
        R_CHECK(&rig, moq_session_done_subscribe(pub->peer, up_sub, &dcfg,
                                                 rig.now) == MOQ_OK);
    }
    for (int k = 0; k < pumps; k++) {
        relay_bind_test_rig_pump(&rig, 1);
        relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
        if (s_ps.done_seen) {
            break;
        }
    }
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);

    /* ---- oracle (wire-observable only) ---- *
     * Every pre-end object was delivered exactly once (correct fingerprint, per-
     * stream order), then exactly one wire SUBSCRIBE_DONE carrying the forwarded
     * source status (0x4), with nothing delivered after it, and both plain-FIN
     * subgroups sealed by identity. Identical for the pull (SUBSCRIBE_DONE) and
     * push (PUBLISH_FINISHED) source kinds. */
    static const struct { uint64_t g, o; } want[4] = {
        { 0, 0 }, { 0, 1 }, { 1, 0 }, { 1, 1 },
    };
    R_CHECK(&rig, s_ps.dlv_overflow == 0);
    R_CHECK(&rig, s_ps.dlv_n == 4);
    R_CHECK(&rig, s_ps.done_seen);
    R_CHECK(&rig, s_ps.done_count == 1);    /* exactly one SUBSCRIBE_DONE     */
    R_CHECK(&rig, s_ps.done_code == 0x4);   /* source status forwarded        */
    int seen[4] = { 0, 0, 0, 0 };
    for (int i = 0; i < s_ps.dlv_n; i++) {
        dlv_rec_t *d = &s_ps.dlv[i];
        R_CHECK(&rig, !d->after_done);       /* nothing after terminal        */
        R_CHECK(&rig, d->sub == sh._opaque);
        R_CHECK(&rig, !d->payload_null);
        R_CHECK(&rig, !d->datagram);
        R_CHECK(&rig, d->status == MOQ_OBJECT_NORMAL);
        R_CHECK(&rig, d->subgroup == 0);
        R_CHECK(&rig, d->fp == relay_bind_test_se_tag(d->group, d->object_id));
        int m = -1;
        for (int w = 0; w < 4; w++) {
            if (want[w].g == d->group && want[w].o == d->object_id) {
                m = w;
                break;
            }
        }
        R_CHECK(&rig, m >= 0);
        if (m >= 0) {
            seen[m]++;
        }
    }
    for (int w = 0; w < 4; w++) {
        R_CHECK(&rig, seen[w] == 1);         /* each object exactly once      */
    }
    for (uint64_t g = 0; g < 2; g++) {
        int64_t last = -1;                   /* per-stream ascending order    */
        for (int i = 0; i < s_ps.dlv_n; i++) {
            dlv_rec_t *d = &s_ps.dlv[i];
            if (d->group == g && d->subgroup == 0) {
                R_CHECK(&rig, (int64_t)d->object_id > last);
                last = (int64_t)d->object_id;
            }
        }
    }
    /* Exact FIN identity: (0,0) and (1,0), each sealed once, before source end. */
    R_CHECK(&rig, s_ps.subgroup_finished == 2);
    for (uint64_t g = 0; g < 2; g++) {
        int fins = 0;
        for (int fi = 0; fi < s_ps.finished_count; fi++) {
            if (s_ps.finished_gsg[fi][0] == g &&
                s_ps.finished_gsg[fi][1] == 0) {
                fins++;
            }
        }
        R_CHECK(&rig, fins == 1);
    }

    relay_bind_test_max_actions = 0;
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: dataplane_source_end_oracle (push=%d aq=%u)\n", (int)push,
               max_actions);
    }
    return f;
}

/* A lawful source end that reaches the relay before its downstream has
 * drained: the origin FIN-closes every subgroup and then ends the
 * subscription (draft-18 section 10.11 / draft-22 section 9.9: PUBLISH_DONE
 * only after every stream it will open is closed). Every object must still
 * reach the subscriber, every subgroup must end with its FIN (never a reset),
 * and the one SUBSCRIBE_DONE -- carrying the forwarded status -- must come
 * after them. Modes:
 *   SE_BURST   the objects and the source end are sent back to back, so the
 *              relay ingests both in one pass, before writing any of it;
 *   SE_FROZEN  the subscriber's connection is not stepped while the relay
 *              ingests everything and the source end: the relay holds what it
 *              owes until that subscriber drains;
 *   SE_STUCK   the source end advertises one stream more than was sent: the
 *              relay keeps forwarding meanwhile, and the terminal comes only
 *              when the session's bounded wait for that stream expires;
 *   SE_UNSEALED the origin closes the last group's subgroup legally, but
 *              that one FIN is lost on the wire (the pair drops it), so the
 *              relay never sees it end; the terminal comes when the session's
 *              bounded wait for that stream expires. The relay cannot know the
 *              subgroup ended, so it delivers its objects and then resets it
 *              with CANCELLED -- never a FIN (draft-18 section 11.4.3,
 *              draft-22 section 11.3.2) -- before the one terminal; the sealed
 *              subgroups still end with FINs;
 *   SE_UNSEALED_BLOCKED as SE_UNSEALED, with that reset first refused
 *              (WOULD_BLOCK before the session is called): the terminal waits
 *              and the retry resets the stream before it;
 *   SE_UNSEALED_FAIL as SE_UNSEALED with two subscribers and the first such
 *              reset failing hard: that subscriber's session is closed (its
 *              stream may never be left without a terminal) and the other
 *              is unaffected;
 *   SE_UNSEALED_SESSION_RESET as SE_UNSEALED, but before the terminal the
 *              relay's own downstream session resets that stream itself
 *              (DELIVERY_TIMEOUT, as its delivery timer would), which the
 *              binding is not told of. The session owns that termination: the
 *              binding's reset is answered STALE_HANDLE and tolerated, the
 *              subscriber sees exactly the session's one reset, and the
 *              terminal follows with the session open. */
enum { SE_BURST, SE_FROZEN, SE_STUCK, SE_UNSEALED, SE_UNSEALED_BLOCKED,
       SE_UNSEALED_FAIL, SE_UNSEALED_SESSION_RESET };

/* Wire records the origin's pair dropped: how many, and how many were a
 * payload-free subgroup FIN. */
typedef struct se_drops {
    int dropped;
    int fins;
} se_drops_t;

static void
se_drop_trace(void *ctx, const moq_sim_trace_record_t *rec)
{
    se_drops_t *d = ctx;
    if (rec->kind != MOQ_SIM_TRACE_FAULT_DROP) {
        return;
    }
    d->dropped++;
    if (rec->struct_size >= offsetof(moq_sim_trace_record_t, fin) +
                               sizeof(rec->fin) &&
        rec->action_kind == MOQ_ACTION_SEND_DATA && rec->fin &&
        rec->count == 0) {
        d->fins++;
    }
}

/* Drain the subscriber, and the second one when the mode has it. */
static void
se_drain(rig_t *rig, conn_t *sub, peer_state_t *ps, conn_t *sub2,
         peer_state_t *ps2)
{
    relay_bind_test_peer_drain(rig, sub, ps, false, false, 0, 0);
    if (sub2 != NULL) {
        relay_bind_test_peer_drain(rig, sub2, ps2, false, false, 0, 0);
    }
}

static int
dataplane_source_end_before_drain(bool push, uint32_t max_actions, int mode)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    relay_bind_test_max_actions = max_actions;
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        relay_bind_test_max_actions = 0;
        printf("FAIL: dataplane_source_end_before_drain rig create (push=%d aq=%u)\n",
               (int)push, max_actions);
        return 1;
    }
    int pumps = max_actions == 0 ? 40 : 200;
    const bool unsealed = mode >= SE_UNSEALED;
    se_drops_t drops = { 0, 0 };
    if (unsealed) {
        /* Configured on the origin's pair only, and suppressed until the
         * FIN that must be lost is the only data left to send. */
        relay_bind_test_fault_flags = MOQ_SIM_FAULT_DROP_DATA;
        relay_bind_test_fault_per_mille = 1000;
        relay_bind_test_trace_fn = se_drop_trace;
        relay_bind_test_trace_ctx = &drops;
    }
    conn_t *pub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    relay_bind_test_fault_flags = 0;
    relay_bind_test_fault_per_mille = 0;
    relay_bind_test_trace_fn = NULL;
    relay_bind_test_trace_ctx = NULL;
    conn_t *sub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub && sub);
    /* A second, healthy subscriber where one subscriber's reset is made to
     * fail: only the failing one may be ended. */
    conn_t *sub2 = NULL;
    if (mode == SE_UNSEALED_FAIL) {
        sub2 = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
        R_CHECK(&rig, sub2 != NULL);
    }
    relay_bind_test_rig_pump(&rig, 4);

    moq_bytes_t nsp[2] = { B("live"), B("cam") };
    peer_state_t pub_ps, s_ps, s2_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&s_ps, 0, sizeof(s_ps));
    memset(&s2_ps, 0, sizeof(s2_ps));
    moq_subscription_t up_sub = { 0 };
    moq_publication_t pubh = { 0 };

    if (push) {
        /* PUSH: the origin publishes the track; the relay accepts it. */
        moq_publish_cfg_t pubc;
        memset(&pubc, 0, sizeof(pubc));
        moq_publish_cfg_init(&pubc);
        pubc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
        pubc.track_name = B("v");
        R_CHECK(&rig, moq_session_publish(pub->peer, &pubc, rig.now, &pubh) ==
                          MOQ_OK);
        relay_bind_test_rig_pump(&rig, 6);
        relay_bind_test_peer_drain(&rig, pub, &pub_ps, false, false, 0, 0);
        R_CHECK(&rig, pub_ps.publish_ok);
    } else {
        /* PULL: the origin announces; the relay subscribes upstream. */
        moq_publish_namespace_cfg_t pcfg;
        memset(&pcfg, 0, sizeof(pcfg));
        moq_publish_namespace_cfg_init(&pcfg);
        pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
        moq_announcement_t ann;
        R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg, rig.now,
                                                    &ann) == MOQ_OK);
        relay_bind_test_rig_pump(&rig, 4);
    }

    /* Open-ended downstream subscriber. */
    moq_subscribe_cfg_t scfg;
    memset(&scfg, 0, sizeof(scfg));
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    scfg.track_name = B("v");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    scfg.start_group = 0;
    scfg.start_object = 0;
    moq_subscription_t sh;
    R_CHECK(&rig, moq_session_subscribe(sub->peer, &scfg, rig.now, &sh) ==
                      MOQ_OK);
    if (sub2 != NULL) {
        moq_subscription_t sh2;
        R_CHECK(&rig, moq_session_subscribe(sub2->peer, &scfg, rig.now,
                                            &sh2) == MOQ_OK);
    }
    relay_bind_test_rig_pump(&rig, 6);
    if (!push) {
        relay_bind_test_peer_drain(&rig, pub, &pub_ps, true, false, 0, 0); /* accept upstream */
        R_CHECK(&rig, pub_ps.up_seen);
        up_sub = pub_ps.up_sub;
        relay_bind_test_rig_pump(&rig, 6);
    }
    se_drain(&rig, sub, &s_ps, sub2, &s2_ps);

    /* Three groups, each subgroup 0 with four objects and a FIN, then the
     * source end -- no pump in between, so nothing has drained yet. */
    enum { GROUPS = 3, OBJS = 4 };
    if (mode == SE_FROZEN) {
        sub->frozen = true;
    }
    moq_subgroup_handle_t last_open;
    memset(&last_open, 0, sizeof(last_open));
    for (uint64_t g = 0; g < GROUPS; g++) {
        relay_bind_test_se_send_group_fin(&rig, pub, push, up_sub, pubh, g,
                                          OBJS, !unsealed || g + 1 < GROUPS,
                                          &last_open);
    }
    if (unsealed) {
        /* Everything sent so far reaches the relay and its subscriber; the
         * last subgroup is still open upstream. */
        for (int k = 0; k < pumps; k++) {
            relay_bind_test_rig_pump(&rig, 1);
            se_drain(&rig, sub, &s_ps, sub2, &s2_ps);
        }
        R_CHECK(&rig, s_ps.dlv_n == GROUPS * OBJS);
        R_CHECK(&rig, s_ps.subgroup_finished == GROUPS - 1);
        R_CHECK(&rig, drops.dropped == 0);
        /* The origin closes it legally; the pair loses exactly that FIN. */
        moq_simpair_enable_faults(pub->sp);
        DP_RETRY(&rig, moq_session_close_subgroup(pub->peer, last_open,
                                                  rig.now));
        relay_bind_test_rig_pump(&rig, 4);
        R_CHECK(&rig, drops.dropped == 1 && drops.fins == 1);
    }
    if (push) {
        moq_finish_publish_cfg_t fcfg;
        moq_finish_publish_cfg_init(&fcfg);
        fcfg.status_code = 0x2;   /* TRACK_ENDED */
        fcfg.stream_count = GROUPS + (mode == SE_STUCK ? 1u : 0u);
        DP_RETRY(&rig, moq_session_finish_publish(pub->peer, pubh, &fcfg,
                                                  rig.now));
    } else {
        moq_done_subscribe_cfg_t dcfg;
        moq_done_subscribe_cfg_init(&dcfg);
        dcfg.status_code = 0x2;   /* TRACK_ENDED */
        dcfg.stream_count = GROUPS + (mode == SE_STUCK ? 1u : 0u);
        DP_RETRY(&rig, moq_session_done_subscribe(pub->peer, up_sub, &dcfg,
                                                  rig.now));
    }
    if (mode == SE_FROZEN) {
        /* The relay ingests everything and the source end while this
         * subscriber is not stepped; nothing may have reached it yet. */
        relay_bind_test_rig_pump(&rig, 20);
        se_drain(&rig, sub, &s_ps, sub2, &s2_ps);
        R_CHECK(&rig, s_ps.dlv_n == 0 && !s_ps.done_seen);
        sub->frozen = false;
    }
    if (mode == SE_STUCK || unsealed) {
        /* Every sent object and FIN is forwarded while the upstream terminal
         * still waits for the advertised fourth stream, or for the one whose
         * FIN was lost. */
        for (int k = 0; k < pumps; k++) {
            relay_bind_test_rig_pump(&rig, 1);
            se_drain(&rig, sub, &s_ps, sub2, &s2_ps);
        }
        R_CHECK(&rig, s_ps.dlv_n == GROUPS * OBJS);
        R_CHECK(&rig, s_ps.subgroup_finished == GROUPS - (unsealed ? 1 : 0));
        R_CHECK(&rig, !s_ps.done_seen);
        /* Past the session's done wait (30 s default): the wait expires and
         * the terminal follows. */
        if (mode == SE_UNSEALED_BLOCKED) {
            moqr_bind_debug_block_sg_reset(1);   /* the first reset refused */
        } else if (mode == SE_UNSEALED_FAIL) {
            moqr_bind_debug_fail_sg_reset(1);    /* the first reset fails */
        } else if (mode == SE_UNSEALED_SESSION_RESET) {
            moq_subgroup_handle_t dh;
            R_CHECK(&rig, moqr_bind_debug_sg_handle(rig.bind, sub->rsess,
                                                    GROUPS - 1, 0, &dh));
            R_CHECK(&rig, moq_session_reset_subgroup(sub->rsess, dh, 0x2,
                                                     rig.now) == MOQ_OK);
        }
        rig.now += 31000000u;
    }
    for (int k = 0; k < pumps &&
                    !(s_ps.done_seen || s_ps.session_closed); k++) {
        relay_bind_test_rig_pump(&rig, 1);
        se_drain(&rig, sub, &s_ps, sub2, &s2_ps);
    }
    for (int k = 0; k < pumps && sub2 != NULL &&
                    !(s2_ps.done_seen || s2_ps.session_closed); k++) {
        relay_bind_test_rig_pump(&rig, 1);
        se_drain(&rig, sub, &s_ps, sub2, &s2_ps);
    }
    se_drain(&rig, sub, &s_ps, sub2, &s2_ps);
    if (mode == SE_UNSEALED_BLOCKED) {
        R_CHECK(&rig, moqr_bind_debug_sg_reset_blocks_left() == 0);
    }
    moqr_bind_debug_block_sg_reset(0);
    moqr_bind_debug_fail_sg_reset(0);
    if (mode == SE_UNSEALED_FAIL) {
        /* Exactly one subscriber's reset failed: that session is closed
         * rather than left with an unterminated stream -- no FIN for the
         * lost subgroup, no reset, no terminal -- and the other subscriber
         * gets its reset and its one terminal. */
        R_CHECK(&rig, drops.dropped == 1 && drops.fins == 1);
        R_CHECK(&rig, s_ps.session_closed != s2_ps.session_closed);
        peer_state_t *bad = s_ps.session_closed ? &s_ps : &s2_ps;
        peer_state_t *good = s_ps.session_closed ? &s2_ps : &s_ps;
        R_CHECK(&rig, bad->done_count == 0);
        R_CHECK(&rig, bad->subgroup_resets == 0);
        R_CHECK(&rig, bad->subgroup_finished == GROUPS - 1);
        R_CHECK(&rig, !good->session_closed);
        R_CHECK(&rig, good->done_count == 1 && good->done_code == 0x2);
        R_CHECK(&rig, good->subgroup_resets == 1);
        R_CHECK(&rig, good->reset_codes[0] == 0x1);   /* CANCELLED */
        R_CHECK(&rig, good->resets_after_done == 0);
        R_CHECK(&rig, good->subgroup_finished == GROUPS - 1);
        R_CHECK(&rig, good->dlv_n == GROUPS * OBJS);
        relay_bind_test_max_actions = 0;
        relay_bind_test_rig_destroy(&rig);
        R_CHECK(&rig, a.live == 0);
        int ff = rig.failures;
        printf("%s: dataplane_source_end_before_drain (push=%d aq=%u mode=%d)\n",
               ff == 0 ? "PASS" : "FAIL", (int)push, max_actions, mode);
        return ff;
    }

    R_CHECK(&rig, s_ps.dlv_overflow == 0);
    R_CHECK(&rig, s_ps.dlv_n == GROUPS * OBJS);
    R_CHECK(&rig, s_ps.done_seen);
    R_CHECK(&rig, s_ps.done_count == 1);
    R_CHECK(&rig, s_ps.done_code == 0x2);
    if (unsealed) {
        /* Nothing but that one FIN was lost. Exactly that subgroup is reset,
         * with CANCELLED, before the terminal; the sealed ones FIN. */
        R_CHECK(&rig, drops.dropped == 1 && drops.fins == 1);
        R_CHECK(&rig, s_ps.subgroup_resets == 1);
        R_CHECK(&rig, s_ps.reset_gsg[0][0] == GROUPS - 1 &&
                          s_ps.reset_gsg[0][1] == 0);
        /* CANCELLED from the binding; DELIVERY_TIMEOUT where the session
         * had already reset the stream itself. */
        R_CHECK(&rig, s_ps.reset_codes[0] ==
                          (mode == SE_UNSEALED_SESSION_RESET ? 0x2 : 0x1));
        R_CHECK(&rig, s_ps.resets_after_done == 0);
        R_CHECK(&rig, s_ps.subgroup_finished == GROUPS - 1);
        R_CHECK(&rig, !s_ps.session_closed);
    } else {
        R_CHECK(&rig, s_ps.subgroup_resets == 0);     /* no truncation reset */
        R_CHECK(&rig, s_ps.subgroup_finished == GROUPS);
    }
    int seen[GROUPS][OBJS];
    memset(seen, 0, sizeof(seen));
    for (int i = 0; i < s_ps.dlv_n; i++) {
        dlv_rec_t *d = &s_ps.dlv[i];
        R_CHECK(&rig, !d->after_done);
        R_CHECK(&rig, d->subgroup == 0);
        R_CHECK(&rig, d->fp == relay_bind_test_se_tag(d->group, d->object_id));
        if (d->group < GROUPS && d->object_id < OBJS) {
            seen[d->group][d->object_id]++;
        }
    }
    for (int g = 0; g < GROUPS; g++) {
        for (int o = 0; o < OBJS; o++) {
            R_CHECK(&rig, seen[g][o] == 1);
        }
    }

    relay_bind_test_max_actions = 0;
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: dataplane_source_end_before_drain (push=%d aq=%u mode=%d)\n",
               (int)push, max_actions, mode);
    } else {
        printf("FAIL: dataplane_source_end_before_drain (push=%d aq=%u mode=%d): "
               "delivered %d of %d, resets %d, fins %d, done %d\n",
               (int)push, max_actions, mode, s_ps.dlv_n, GROUPS * OBJS,
               s_ps.subgroup_resets, s_ps.subgroup_finished, (int)s_ps.done_seen);
    }
    return f;
}

/* A PUBLISH-created source is not a pull subscription.  The last downstream
 * subscriber leaving must therefore not run the pull-source linger teardown:
 * there is no upstream SUBSCRIBE to cancel, and warming the core track while
 * the accepted PUBLISH remains live turns every later pushed object into an
 * ingest refusal.  This is the exact lifecycle observed with the physical
 * MoQCam publisher and moq5-relay on moqbox.
 *
 * Keep the policy neutral: this oracle does not require a demand-gated
 * PUBLISH_UPDATE.  It requires only the current accepted-PUBLISH contract to
 * stay coherent -- while that publication remains permitted to send, its
 * backing relay track remains ACTIVE and accepts data.  Two downstream churn
 * cycles pin the accumulation class seen on the physical Camera relay without
 * making this deterministic unit test depend on live deployment. */
static int
dataplane_push_survives_downstream_linger(void)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: push-survives-linger rig create\n");
        return 1;
    }

    conn_t *pub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub != NULL && sub != NULL);
    peer_state_t pub_ps, sub_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&sub_ps, 0, sizeof(sub_ps));
    relay_bind_test_rig_pump(&rig, 4);

    moq_bytes_t nsp[2] = { B("push"), B("linger") };
    /* Match media_sender's real topology: the catalog track advertises the
     * namespace, while the media track is also opened with PUBLISH.  This is
     * what makes a stale WARM transition especially destructive: a later
     * viewer can manufacture a pull SUBSCRIBE alongside the live PUBLISH. */
    moq_publish_namespace_cfg_t ncfg;
    moq_publish_namespace_cfg_init(&ncfg);
    ncfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &ncfg, rig.now,
                                                 &ann) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 4);

    moq_publish_cfg_t pc;
    moq_publish_cfg_init(&pc);
    pc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    pc.track_name = B("video");
    moq_publication_t pubh;
    R_CHECK(&rig, moq_session_publish(pub->peer, &pc, rig.now, &pubh) ==
                      MOQ_OK);
    relay_bind_test_rig_pump(&rig, 6);
    relay_bind_test_peer_drain(&rig, pub, &pub_ps, false, false, 0, 0);
    R_CHECK(&rig, pub_ps.publish_ok && !pub_ps.publish_error);

    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    sc.track_name = B("video");
    sc.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    moq_subscription_t sh;
    R_CHECK(&rig, moq_session_subscribe(sub->peer, &sc, rig.now, &sh) ==
                      MOQ_OK);
    relay_bind_test_rig_pump(&rig, 6);
    relay_bind_test_peer_drain(&rig, sub, &sub_ps, false, false, 0, 0);
    R_CHECK(&rig, sub_ps.subscribe_ok);
    R_CHECK(&rig, pub_ps.up_subs == 0); /* PUBLISH already owns the source. */

    relay_bind_test_se_send_group(&rig, pub, true, MOQ_SUBSCRIPTION_INVALID, pubh, 0, 1);
    relay_bind_test_rig_pump(&rig, 6);
    relay_bind_test_peer_drain(&rig, sub, &sub_ps, false, false, 0, 0);
    R_CHECK(&rig, sub_ps.objects == 1 && sub_ps.last_group == 0);

    moqr_bind_stats_t before;
    moqr_bind_get_stats(rig.bind, &before);

    /* Retire the only downstream and cross the configured 500us linger. */
    R_CHECK(&rig, moq_session_unsubscribe(sub->peer, sh, rig.now) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 6);
    relay_bind_test_peer_drain(&rig, pub, &pub_ps, false, false, 0, 0);

    /* The accepted PUBLISH remains the source, hence the track must not have
     * been turned WARM by a pull-only teardown. */
    char route[4096];
    size_t route_len = 0;
    R_CHECK(&rig, moqr_core_route_dump_json(rig.core, route, sizeof(route),
                                             &route_len) == MOQR_OK);
    R_CHECK(&rig, route_len < sizeof(route));
    R_CHECK(&rig, strstr(route,
                         "\"name\":\"video\",\"state\":\"active\"") != NULL);

    /* Data still permitted by the publication must remain acceptable.  The
     * buggy implementation increments ingest_refusals here because the core
     * track is WARM even though the wire publication is still established. */
    relay_bind_test_se_send_group(&rig, pub, true, MOQ_SUBSCRIPTION_INVALID, pubh, 1, 1);
    relay_bind_test_rig_pump(&rig, 6);
    moqr_bind_stats_t after;
    moqr_bind_get_stats(rig.bind, &after);
    R_CHECK(&rig, after.ingest_refusals == before.ingest_refusals);

    /* A new downstream binds directly to the still-live push source.  It must
     * not manufacture a pull SUBSCRIBE alongside the existing PUBLISH. */
    conn_t *sub2 = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, sub2 != NULL);
    peer_state_t sub2_ps;
    memset(&sub2_ps, 0, sizeof(sub2_ps));
    relay_bind_test_rig_pump(&rig, 4);
    moq_subscription_t sh2;
    R_CHECK(&rig, moq_session_subscribe(sub2->peer, &sc, rig.now, &sh2) ==
                      MOQ_OK);
    relay_bind_test_rig_pump(&rig, 6);
    relay_bind_test_peer_drain(&rig, pub, &pub_ps, false, false, 0, 0);
    relay_bind_test_peer_drain(&rig, sub2, &sub2_ps, false, false, 0, 0);
    R_CHECK(&rig, pub_ps.up_subs == 0);
    R_CHECK(&rig, sub2_ps.subscribe_ok);

    int sub2_objects = sub2_ps.objects;
    relay_bind_test_se_send_group(&rig, pub, true, MOQ_SUBSCRIPTION_INVALID, pubh, 2, 1);
    relay_bind_test_rig_pump(&rig, 6);
    relay_bind_test_peer_drain(&rig, sub2, &sub2_ps, false, false, 0, 0);
    moqr_bind_get_stats(rig.bind, &after);
    R_CHECK(&rig, after.ingest_refusals == before.ingest_refusals);
    R_CHECK(&rig, sub2_ps.objects == sub2_objects + 1 &&
                  sub2_ps.last_group == 2);

    /* Repeat the churn once more.  The deployed unfixed relay failed only
     * after accumulated viewer sessions, so keep the deterministic regression
     * from becoming a single-transition special case. */
    R_CHECK(&rig, moq_session_unsubscribe(sub2->peer, sh2, rig.now) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 6);
    relay_bind_test_peer_drain(&rig, pub, &pub_ps, false, false, 0, 0);

    route_len = 0;
    R_CHECK(&rig, moqr_core_route_dump_json(rig.core, route, sizeof(route),
                                             &route_len) == MOQR_OK);
    R_CHECK(&rig, route_len < sizeof(route));
    R_CHECK(&rig, strstr(route,
                         "\"name\":\"video\",\"state\":\"active\"") != NULL);

    relay_bind_test_se_send_group(&rig, pub, true, MOQ_SUBSCRIPTION_INVALID, pubh, 3, 1);
    relay_bind_test_rig_pump(&rig, 6);
    moqr_bind_get_stats(rig.bind, &after);
    R_CHECK(&rig, after.ingest_refusals == before.ingest_refusals);

    conn_t *sub3 = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, sub3 != NULL);
    peer_state_t sub3_ps;
    memset(&sub3_ps, 0, sizeof(sub3_ps));
    relay_bind_test_rig_pump(&rig, 4);
    moq_subscription_t sh3;
    R_CHECK(&rig, moq_session_subscribe(sub3->peer, &sc, rig.now, &sh3) ==
                      MOQ_OK);
    relay_bind_test_rig_pump(&rig, 6);
    relay_bind_test_peer_drain(&rig, pub, &pub_ps, false, false, 0, 0);
    relay_bind_test_peer_drain(&rig, sub3, &sub3_ps, false, false, 0, 0);
    R_CHECK(&rig, pub_ps.up_subs == 0);
    R_CHECK(&rig, sub3_ps.subscribe_ok);

    int sub3_objects = sub3_ps.objects;
    relay_bind_test_se_send_group(&rig, pub, true, MOQ_SUBSCRIPTION_INVALID, pubh, 4, 1);
    relay_bind_test_rig_pump(&rig, 6);
    relay_bind_test_peer_drain(&rig, sub3, &sub3_ps, false, false, 0, 0);
    moqr_bind_get_stats(rig.bind, &after);
    R_CHECK(&rig, after.ingest_refusals == before.ingest_refusals);
    R_CHECK(&rig, sub3_ps.objects == sub3_objects + 1 &&
                  sub3_ps.last_group == 4);

    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: dataplane_push_survives_downstream_linger\n");
    }
    return f;
}

/* Chunked COMPLETE delivery. With the relay-side session in streaming-
 * receive mode, an inbound object surfaces as OBJECT_CHUNK and the bind records
 * it into an OPEN record: a normal object completes (chunked COMPLETE, bytes
 * retained) and is DELIVERED whole on the subscribe path, while a mid-object
 * reset abandons (chunks released) and is never delivered. (OPEN live-edge and
 * RESET/STOP propagation are exercised separately, and retained FETCH still
 * skips chunked records.) */
static int
chunk_ingest_flow(void)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    relay_bind_test_server_streaming_objects = true; /* read in relay_bind_test_rig_connect, below */
    rig_t rig;
    int cr = relay_bind_test_rig_create(&rig, &a);
    if (cr != 0) {
        relay_bind_test_server_streaming_objects = false;
        printf("FAIL: chunk ingest rig create\n");
        return 1;
    }
    conn_t *pub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    relay_bind_test_server_streaming_objects = false; /* restore after both connects */
    R_CHECK(&rig, pub != NULL && sub != NULL);
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
    scfg.start_group = 0;
    scfg.start_object = 0;
    moq_subscription_t s1;
    R_CHECK(&rig, moq_session_subscribe(sub->peer, &scfg, rig.now, &s1) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 4);
    peer_state_t pub_ps, sub_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&sub_ps, 0, sizeof(sub_ps));
    relay_bind_test_peer_drain(&rig, pub, &pub_ps, true, false, 0, 0); /* accept upstream sub */
    R_CHECK(&rig, pub_ps.up_seen);
    relay_bind_test_rig_pump(&rig, 4);

    /* Group 1, subgroup 0 carries TWO streamed objects the relay ingests as
     * chunked COMPLETE records and delivers whole via
     * begin/write_object_data/end, reassembled by the subscriber. Two objects
     * on ONE subgroup prove the relay ends object 0 downstream before beginning
     * object 1 (so end_object is load-bearing, not just bookkeeping).
     *   object 0: three 32-byte chunks (96 bytes, 0xC5)
     *   object 1: two   32-byte chunks (64 bytes, 0xD6) */
    moq_subgroup_cfg_t sca;
    moq_subgroup_cfg_init(&sca);
    sca.group_id = 1;
    sca.subgroup_id = 0;
    moq_subgroup_handle_t sga;
    R_CHECK(&rig, moq_session_open_subgroup(pub->peer, pub_ps.up_sub, &sca,
                                            rig.now, &sga) == MOQ_OK);
    R_CHECK(&rig, moq_session_begin_object(pub->peer, sga, 0, 96, rig.now) ==
                      MOQ_OK);
    for (int ci = 0; ci < 3; ci++) {
        uint8_t body[32];
        memset(body, 0xC5, sizeof(body));
        moq_rcbuf_t *pl = NULL;
        R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                          MOQ_OK);
        R_CHECK(&rig, moq_session_write_object_data(pub->peer, sga, pl,
                                                    rig.now) == MOQ_OK);
        moq_rcbuf_decref(pl);
    }
    R_CHECK(&rig, moq_session_end_object(pub->peer, sga, rig.now) == MOQ_OK);
    R_CHECK(&rig, moq_session_begin_object(pub->peer, sga, 1, 64, rig.now) ==
                      MOQ_OK);
    for (int ci = 0; ci < 2; ci++) {
        uint8_t body[32];
        memset(body, 0xD6, sizeof(body));
        moq_rcbuf_t *pl = NULL;
        R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                          MOQ_OK);
        R_CHECK(&rig, moq_session_write_object_data(pub->peer, sga, pl,
                                                    rig.now) == MOQ_OK);
        moq_rcbuf_decref(pl);
    }
    R_CHECK(&rig, moq_session_end_object(pub->peer, sga, rig.now) == MOQ_OK);
    R_CHECK(&rig, moq_session_close_subgroup(pub->peer, sga, rig.now) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 8);

    /* Object B (group 2): begin, partial data, then reset mid-object -> the
     * relay records ABANDONED (partial chunks released), never COMPLETE. */
    moq_subgroup_cfg_t scb;
    moq_subgroup_cfg_init(&scb);
    scb.group_id = 2;
    scb.subgroup_id = 0;
    moq_subgroup_handle_t sgb;
    R_CHECK(&rig, moq_session_open_subgroup(pub->peer, pub_ps.up_sub, &scb,
                                            rig.now, &sgb) == MOQ_OK);
    R_CHECK(&rig, moq_session_begin_object(pub->peer, sgb, 0, 64, rig.now) == MOQ_OK);
    uint8_t half[32];
    memset(half, 0xB7, sizeof(half));
    moq_rcbuf_t *hp = NULL;
    R_CHECK(&rig, moq_rcbuf_create(&a.vt, half, sizeof(half), &hp) == MOQ_OK);
    R_CHECK(&rig, moq_session_write_object_data(pub->peer, sgb, hp, rig.now) == MOQ_OK);
    moq_rcbuf_decref(hp);
    R_CHECK(&rig, moq_session_reset_subgroup(pub->peer, sgb, 0, rig.now) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 8);

    /* Relay log state: objects 0+1 retained/counted; B's partial bytes released. */
    moqr_core_stats_t st;
    moqr_core_get_stats(rig.core, &st);
    R_CHECK(&rig, st.retained_bytes == 96 + 64);   /* obj0 + obj1; B released */
    R_CHECK(&rig, st.ingested_total == 2);          /* obj0 + obj1 completed */

    /* Both chunked COMPLETE records are delivered whole to the subscriber
     * (streamed downstream chunk-by-chunk on one subgroup, reassembled); the
     * ABANDONED record B is never delivered. The last object is 1 (0xD6, 64B). */
    relay_bind_test_peer_drain(&rig, sub, &sub_ps, false, false, 0, 0);
    R_CHECK(&rig, sub_ps.objects == 2);
    R_CHECK(&rig, sub_ps.last_group == 1 && sub_ps.last_object == 1);
    R_CHECK(&rig, !sub_ps.last_payload_null && sub_ps.last_byte == 0xD6);
    R_CHECK(&rig, sub_ps.last_len == 64);
    R_CHECK(&rig, !sub_ps.session_closed);

    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (rig.failures == 0) {
        printf("PASS: chunk_ingest_flow\n");
    }
    return rig.failures;
}

/* Backpressure: with a constrained action queue the relay's downstream
 * write_object_data WOULD_BLOCKs partway through a chunked object; the delivery
 * holds and resumes the SAME chunk (no duplicate, no skip), so the subscriber
 * still reassembles the exact object. Deterministic under SimPair. */
static int
chunk_delivery_backpressure(void)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    relay_bind_test_server_streaming_objects = true;
    relay_bind_test_max_actions = 3; /* forces WOULD_BLOCK during the chunked delivery */
    rig_t rig;
    int cr = relay_bind_test_rig_create(&rig, &a);
    if (cr != 0) {
        relay_bind_test_server_streaming_objects = false;
        relay_bind_test_max_actions = 0;
        printf("FAIL: chunk backpressure rig create\n");
        return 1;
    }
    conn_t *pub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    relay_bind_test_server_streaming_objects = false;
    relay_bind_test_max_actions = 0; /* restore after both connects read the knobs */
    R_CHECK(&rig, pub != NULL && sub != NULL);
    relay_bind_test_rig_pump(&rig, 8);

    moq_bytes_t nsp[1] = { B("demo") };
    moq_publish_namespace_cfg_t pcfg;
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 1 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg, rig.now,
                                                &ann) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 8);

    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 1 };
    scfg.track_name = B("v");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    scfg.start_group = 0;
    scfg.start_object = 0;
    moq_subscription_t s1;
    R_CHECK(&rig,
            moq_session_subscribe(sub->peer, &scfg, rig.now, &s1) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 8);
    peer_state_t pub_ps, sub_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&sub_ps, 0, sizeof(sub_ps));
    relay_bind_test_peer_drain(&rig, pub, &pub_ps, true, false, 0, 0);
    R_CHECK(&rig, pub_ps.up_seen);
    relay_bind_test_rig_pump(&rig, 8);

    /* Stream a 3-chunk (96-byte) object, pumping between writes so the PUBLISHER
     * never blocks on the constrained queue; the relay accumulates an OPEN
     * record and only delivers once it completes. */
    moq_subgroup_cfg_t sca;
    moq_subgroup_cfg_init(&sca);
    sca.group_id = 1;
    sca.subgroup_id = 0;
    moq_subgroup_handle_t sga;
    R_CHECK(&rig, moq_session_open_subgroup(pub->peer, pub_ps.up_sub, &sca,
                                            rig.now, &sga) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 4);
    R_CHECK(&rig, moq_session_begin_object(pub->peer, sga, 0, 96, rig.now) ==
                      MOQ_OK);
    relay_bind_test_rig_pump(&rig, 4);
    for (int ci = 0; ci < 3; ci++) {
        uint8_t body[32];
        memset(body, 0xC5, sizeof(body));
        moq_rcbuf_t *pl = NULL;
        R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                          MOQ_OK);
        R_CHECK(&rig, moq_session_write_object_data(pub->peer, sga, pl,
                                                    rig.now) == MOQ_OK);
        moq_rcbuf_decref(pl);
        relay_bind_test_rig_pump(&rig, 4);
    }
    R_CHECK(&rig, moq_session_end_object(pub->peer, sga, rig.now) == MOQ_OK);
    R_CHECK(&rig, moq_session_close_subgroup(pub->peer, sga, rig.now) == MOQ_OK);
    /* Generous, bounded drain: the constrained relay delivers the object across
     * several WOULD_BLOCK / resume cycles. */
    relay_bind_test_rig_pump(&rig, 40);

    /* Reassembled exactly: one object, 96 bytes, correct identity/bytes — proof
     * the held chunk cursor never duplicated or skipped a chunk. */
    relay_bind_test_peer_drain(&rig, sub, &sub_ps, false, false, 0, 0);
    R_CHECK(&rig, sub_ps.objects == 1);
    R_CHECK(&rig, sub_ps.last_group == 1 && sub_ps.last_object == 0);
    R_CHECK(&rig, !sub_ps.last_payload_null && sub_ps.last_byte == 0xC5);
    R_CHECK(&rig, sub_ps.last_len == 96);
    R_CHECK(&rig, !sub_ps.session_closed);

    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (rig.failures == 0) {
        printf("PASS: chunk_delivery_backpressure\n");
    }
    return rig.failures;
}

/* Live edge + abandon over SimPair with a streaming-receive subscriber.
 * `abort_mid` == false: stream a 3-chunk object incrementally; the subscriber
 * observes the first downstream chunk BEFORE the object completes, then the
 * object completes cleanly. `abort_mid` == true: after the first chunk the
 * publisher resets the object; the subscriber observes a downstream RESET (the
 * relay's minimal abandon propagation), never a clean completion. */
static int
chunk_liveedge_case(bool abort_mid)
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
        printf("FAIL: chunk liveedge rig create\n");
        return 1;
    }
    conn_t *pub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    relay_bind_test_server_streaming_objects = false;
    relay_bind_test_client_streaming_objects = false;
    R_CHECK(&rig, pub != NULL && sub != NULL);
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
    R_CHECK(&rig,
            moq_session_subscribe(sub->peer, &scfg, rig.now, &s1) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 4);
    peer_state_t pub_ps, sub_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&sub_ps, 0, sizeof(sub_ps));
    relay_bind_test_peer_drain(&rig, pub, &pub_ps, true, false, 0, 0);
    R_CHECK(&rig, pub_ps.up_seen);
    relay_bind_test_rig_pump(&rig, 4);

    /* Begin the object and stream only the FIRST chunk. */
    moq_subgroup_cfg_t sca;
    moq_subgroup_cfg_init(&sca);
    sca.group_id = 1;
    sca.subgroup_id = 0;
    moq_subgroup_handle_t sga;
    R_CHECK(&rig, moq_session_open_subgroup(pub->peer, pub_ps.up_sub, &sca,
                                            rig.now, &sga) == MOQ_OK);
    R_CHECK(&rig, moq_session_begin_object(pub->peer, sga, 0, 96, rig.now) ==
                      MOQ_OK);
    {
        uint8_t body[32];
        memset(body, 0xC5, sizeof(body));
        moq_rcbuf_t *pl = NULL;
        R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                          MOQ_OK);
        R_CHECK(&rig,
                moq_session_write_object_data(pub->peer, sga, pl, rig.now) ==
                    MOQ_OK);
        moq_rcbuf_decref(pl);
    }
    relay_bind_test_rig_pump(&rig, 8);

    /* Live edge: the subscriber has the first chunk BEFORE the object is done. */
    relay_bind_test_peer_drain(&rig, sub, &sub_ps, false, false, 0, 0);
    R_CHECK(&rig, sub_ps.chunks >= 1);
    R_CHECK(&rig, sub_ps.last_chunk_byte == 0xC5);
    R_CHECK(&rig, !sub_ps.obj_complete);
    R_CHECK(&rig, !sub_ps.saw_reset);

    if (abort_mid) {
        /* Reset the object mid-stream: the relay abandons the record and resets
         * the already-begun downstream subgroup. */
        R_CHECK(&rig,
                moq_session_reset_subgroup(pub->peer, sga, 0x99, rig.now) ==
                    MOQ_OK);
        relay_bind_test_rig_pump(&rig, 8);
        relay_bind_test_peer_drain(&rig, sub, &sub_ps, false, false, 0, 0);
        R_CHECK(&rig, sub_ps.saw_reset);        /* wire-visible downstream reset */
        R_CHECK(&rig, sub_ps.last_reset_code == 0x99); /* real code propagated   */
        R_CHECK(&rig, !sub_ps.obj_complete);    /* never a clean completion      */
    } else {
        /* Finish the object: two more chunks + end. */
        for (int ci = 0; ci < 2; ci++) {
            uint8_t body[32];
            memset(body, 0xC5, sizeof(body));
            moq_rcbuf_t *pl = NULL;
            R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                              MOQ_OK);
            R_CHECK(&rig, moq_session_write_object_data(pub->peer, sga, pl,
                                                        rig.now) == MOQ_OK);
            moq_rcbuf_decref(pl);
        }
        R_CHECK(&rig, moq_session_end_object(pub->peer, sga, rig.now) == MOQ_OK);
        R_CHECK(&rig,
                moq_session_close_subgroup(pub->peer, sga, rig.now) == MOQ_OK);
        relay_bind_test_rig_pump(&rig, 8);
        relay_bind_test_peer_drain(&rig, sub, &sub_ps, false, false, 0, 0);
        R_CHECK(&rig, sub_ps.obj_complete);     /* completes cleanly */
        R_CHECK(&rig, !sub_ps.saw_reset);
    }

    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (rig.failures == 0) {
        printf("PASS: chunk_liveedge_%s\n", abort_mid ? "abort" : "complete");
    }
    return rig.failures;
}

/* Eviction after live-edge, end-to-end: a streaming subscriber gets the
 * first chunk of an OPEN object, then that object's group is capacity-evicted
 * before it completes — the relay must reset the begun downstream subgroup (the
 * bind's evict-reset path), so the subscriber observes a RESET rather than a
 * silently-stuck object. */
static int
chunk_liveedge_evict(void)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    relay_bind_test_server_streaming_objects = true;
    relay_bind_test_client_streaming_objects = true;
    relay_bind_test_log_max_groups = 2; /* group 1 evicts once group 3 is ingested */
    rig_t rig;
    int cr = relay_bind_test_rig_create(&rig, &a);
    if (cr != 0) {
        relay_bind_test_server_streaming_objects = false;
        relay_bind_test_client_streaming_objects = false;
        relay_bind_test_log_max_groups = 0;
        printf("FAIL: chunk liveedge evict rig create\n");
        return 1;
    }
    conn_t *pub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    relay_bind_test_server_streaming_objects = false;
    relay_bind_test_client_streaming_objects = false;
    relay_bind_test_log_max_groups = 0;
    R_CHECK(&rig, pub != NULL && sub != NULL);
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
    R_CHECK(&rig,
            moq_session_subscribe(sub->peer, &scfg, rig.now, &s1) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 4);
    peer_state_t pub_ps, sub_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&sub_ps, 0, sizeof(sub_ps));
    relay_bind_test_peer_drain(&rig, pub, &pub_ps, true, false, 0, 0);
    R_CHECK(&rig, pub_ps.up_seen);
    relay_bind_test_rig_pump(&rig, 4);

    /* Group 1: begin + one chunk (stays OPEN); the relay live-delivers it. */
    moq_subgroup_cfg_t sca;
    moq_subgroup_cfg_init(&sca);
    sca.group_id = 1;
    sca.subgroup_id = 0;
    moq_subgroup_handle_t sga;
    R_CHECK(&rig, moq_session_open_subgroup(pub->peer, pub_ps.up_sub, &sca,
                                            rig.now, &sga) == MOQ_OK);
    R_CHECK(&rig, moq_session_begin_object(pub->peer, sga, 0, 96, rig.now) ==
                      MOQ_OK);
    {
        uint8_t body[32];
        memset(body, 0xC5, sizeof(body));
        moq_rcbuf_t *pl = NULL;
        R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                          MOQ_OK);
        R_CHECK(&rig,
                moq_session_write_object_data(pub->peer, sga, pl, rig.now) ==
                    MOQ_OK);
        moq_rcbuf_decref(pl);
    }
    relay_bind_test_rig_pump(&rig, 8);
    relay_bind_test_peer_drain(&rig, sub, &sub_ps, false, false, 0, 0);
    R_CHECK(&rig, sub_ps.chunks >= 1 && !sub_ps.obj_complete);

    /* Groups 2 and 3: whole objects on fresh subgroups. Ingesting group 3 evicts
     * group 1 (max_groups == 2), truncating the begun OPEN object. */
    for (uint64_t g = 2; g <= 3; g++) {
        moq_subgroup_cfg_t sc;
        moq_subgroup_cfg_init(&sc);
        sc.group_id = g;
        sc.subgroup_id = 0;
        moq_subgroup_handle_t sgh;
        R_CHECK(&rig, moq_session_open_subgroup(pub->peer, pub_ps.up_sub, &sc,
                                                rig.now, &sgh) == MOQ_OK);
        uint8_t body[16];
        memset(body, (uint8_t)(0xE0 + g), sizeof(body));
        moq_rcbuf_t *pl = NULL;
        R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                          MOQ_OK);
        R_CHECK(&rig,
                moq_session_write_object(pub->peer, sgh, 0, pl, rig.now) ==
                    MOQ_OK);
        moq_rcbuf_decref(pl);
        R_CHECK(&rig,
                moq_session_close_subgroup(pub->peer, sgh, rig.now) == MOQ_OK);
        relay_bind_test_rig_pump(&rig, 8);
    }

    /* The begun OPEN object's group was evicted → the relay reset its downstream
     * subgroup, so the subscriber saw a RESET (not a silent hang). */
    relay_bind_test_peer_drain(&rig, sub, &sub_ps, false, false, 0, 0);
    R_CHECK(&rig, sub_ps.saw_reset);
    R_CHECK(&rig, !sub_ps.session_closed);

    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (rig.failures == 0) {
        printf("PASS: chunk_liveedge_evict\n");
    }
    return rig.failures;
}

/* A subscription terminated while a downstream subgroup is mid-object: the relay
 * must RESET the begun subgroup (the streaming-safe terminal) so the peer sees a
 * clean reset before the SUBSCRIBE_DONE — never a begun object left with no
 * terminal. A SUB_DONE handler that only closed subgroups would fail this:
 * close returns WRONG_STATE on a streaming subgroup, so the open object would
 * be dropped with no downstream reset. */
static int
sub_done_resets_begun_subgroup(void)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    relay_bind_test_rv_lease_sub = 3000; /* a subscribe grant with a live revalidation lease */
    relay_bind_test_authorize = relay_bind_test_rv_hook;
    relay_bind_test_server_streaming_objects = true;
    relay_bind_test_client_streaming_objects = true;
    rig_t rig;
    int cr = relay_bind_test_rig_create(&rig, &a);
    if (cr != 0) {
        relay_bind_test_server_streaming_objects = false;
        relay_bind_test_client_streaming_objects = false;
        relay_bind_test_rv_reset();
        printf("FAIL: sub_done_reset rig create\n");
        return 1;
    }
    conn_t *pub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    relay_bind_test_server_streaming_objects = false;
    relay_bind_test_client_streaming_objects = false;
    R_CHECK(&rig, pub != NULL && sub != NULL);
    peer_state_t pub_ps, sub_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&sub_ps, 0, sizeof(sub_ps));
    relay_bind_test_rig_pump(&rig, 4);

    /* Announce + subscribe under a subscribe grant with a revalidation lease. */
    moq_bytes_t nsp[2] = { B("live"), B("cam1") };
    relay_bind_test_reval_make_subscribe_grant(&rig, pub, sub, &pub_ps, &sub_ps, nsp);
    R_CHECK(&rig, !sub_ps.done_seen);

    /* Begin an object, ship only the first chunk → the downstream subgroup is open
     * and streaming (obj_begun) at the subscriber. */
    moq_subgroup_cfg_t sca;
    moq_subgroup_cfg_init(&sca);
    sca.group_id = 1;
    sca.subgroup_id = 0;
    moq_subgroup_handle_t sga;
    R_CHECK(&rig, moq_session_open_subgroup(pub->peer, pub_ps.up_sub, &sca,
                                            rig.now, &sga) == MOQ_OK);
    R_CHECK(&rig, moq_session_begin_object(pub->peer, sga, 0, 96, rig.now) ==
                      MOQ_OK);
    {
        uint8_t body[32];
        memset(body, 0xC5, sizeof(body));
        moq_rcbuf_t *pl = NULL;
        R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                          MOQ_OK);
        R_CHECK(&rig,
                moq_session_write_object_data(pub->peer, sga, pl, rig.now) ==
                    MOQ_OK);
        moq_rcbuf_decref(pl);
    }
    relay_bind_test_rig_pump(&rig, 8);
    relay_bind_test_peer_drain(&rig, sub, &sub_ps, false, false, 0, 0);
    R_CHECK(&rig, sub_ps.chunks >= 1);
    R_CHECK(&rig, !sub_ps.obj_complete);
    R_CHECK(&rig, !sub_ps.saw_reset);

    /* Revoke the subscription (auth DENY on revalidation) WHILE the downstream
     * subgroup is mid-object → the relay fans a downstream SUB_DONE. The begun
     * subgroup must be RESET before the SUBSCRIBE_DONE — with the reset
     * registry's CANCELLED (0x1): the detailed cause rides the PUBLISH_DONE
     * status, never the reset code (distinct registries). */
    relay_bind_test_rv_decision = MOQR_AUTH_DENY;
    relay_bind_test_rv_deny_code = 0x7;
    relay_bind_test_rig_pump(&rig, 8);
    relay_bind_test_peer_drain(&rig, sub, &sub_ps, false, false, 0, 0);
    R_CHECK(&rig, sub_ps.saw_reset);               /* begun object RESET, not stranded */
    R_CHECK(&rig, sub_ps.last_reset_code == 0x1);  /* reset registry: CANCELLED         */
    R_CHECK(&rig, !sub_ps.obj_complete);           /* never a clean FIN                 */
    R_CHECK(&rig, sub_ps.done_seen);               /* SUBSCRIBE_DONE still delivered    */
    R_CHECK(&rig, sub_ps.done_code == 0x7);        /* the revoke code, as DONE status   */

    relay_bind_test_rig_destroy(&rig);
    relay_bind_test_rv_reset();
    R_CHECK(&rig, a.live == 0);
    if (rig.failures == 0) {
        printf("PASS: sub_done_resets_begun_subgroup\n");
    }
    return rig.failures;
}

/* ==== ready-delivery scheduler (Phase F consumes the core ready set) ======= */

/* Retry a peer-session call that can hit its own action-queue limit while a
 * fixture holds the transport still: pace it with pump cycles (bounded). */
#define RDY_RETRY(r, expr)                                                   \
    do {                                                                     \
        moq_result_t rrc_ = (expr);                                          \
        for (int t_ = 0; rrc_ == MOQ_ERR_WOULD_BLOCK && t_ < 64; t_++) {     \
            relay_bind_test_rig_cycle(r);                                    \
            rrc_ = (expr);                                                   \
        }                                                                    \
        R_CHECK(r, rrc_ == MOQ_OK);                                          \
    } while (0)

/* Shared mini-fixture: optionally announce ns {n0,n1} from `pub`, subscribe
 * `name` from `sub`, auto-accept upstream, and quiesce. Fills pub_ps. */
static void
rdy_wire_track(rig_t *r, conn_t *pub, conn_t *sub, const char *n0,
               const char *n1, const char *name, bool announce,
               peer_state_t *pub_ps, moq_subscription_t *sh)
{
    moq_bytes_t nsp[2] = { B(n0), B(n1) };
    if (announce) {
        moq_publish_namespace_cfg_t pcfg;
        memset(&pcfg, 0, sizeof(pcfg));
        moq_publish_namespace_cfg_init(&pcfg);
        pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
        moq_announcement_t ann;
        RDY_RETRY(r, moq_session_publish_namespace(pub->peer, &pcfg, r->now,
                                                   &ann));
        relay_bind_test_rig_pump(r, 6);
    }
    moq_subscribe_cfg_t scfg;
    memset(&scfg, 0, sizeof(scfg));
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    scfg.track_name = B(name);
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    RDY_RETRY(r, moq_session_subscribe(sub->peer, &scfg, r->now, sh));
    relay_bind_test_rig_pump(r, 6);
    relay_bind_test_peer_drain(r, pub, pub_ps, true, false, 0, 0);
    relay_bind_test_rig_pump(r, 6);
}

/* Retry a publisher-session call by advancing ONLY that connection's
 * transport (plus the bind pump): the other connections' queues stay
 * exactly as the fixture left them. */
#define RDY_RETRY_ONE(r, cn, expr)                                           \
    do {                                                                     \
        moq_result_t rrc_ = (expr);                                          \
        for (int t_ = 0; rrc_ == MOQ_ERR_WOULD_BLOCK && t_ < 64; t_++) {     \
            (r)->now += 1000;                                                \
            (void)moq_simpair_advance_to((cn)->sp, (r)->now);                \
            size_t st_ = 0;                                                  \
            (void)moq_simpair_run_until_quiescent((cn)->sp, 64, &st_);       \
            (void)moqr_bind_pump((r)->bind, (r)->now);                       \
            rrc_ = (expr);                                                   \
        }                                                                    \
        R_CHECK(r, rrc_ == MOQ_OK);                                          \
    } while (0)

/* One pump cycle with a DEEP transport budget: bulk fixtures move hundreds
 * of objects per stream, far past relay_bind_test_rig_cycle's 64-step quiescence cap. */
static void
rdy_deep_cycle(rig_t *r)
{
    r->now += 1000;
    for (int i = 0; i < MAX_CONNS; i++) {
        conn_t *cn = &r->conns[i];
        if (!cn->used) {
            continue;
        }
        (void)moq_simpair_advance_to(cn->sp, r->now);
        size_t steps = 0;
        (void)moq_simpair_run_until_quiescent(cn->sp, 65536, &steps);
    }
    (void)moqr_bind_pump(r->bind, r->now);
}

/* Publish one whole object into an open-and-FINished subgroup, paced (each
 * session call retries through pump cycles when the peer queue is tiny). */
static void
rdy_pub_one(rig_t *r, conn_t *pub, moq_subscription_t up_sub, uint64_t group)
{
    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = group;
    sgc.subgroup_id = 0;
    sgc.publisher_priority = 0;
    moq_subgroup_handle_t h;
    RDY_RETRY(r, moq_session_open_subgroup(pub->peer, up_sub, &sgc, r->now,
                                           &h));
    uint8_t body[4] = { (uint8_t)group, 1, 2, 3 };
    moq_rcbuf_t *pl = NULL;
    R_CHECK(r, moq_rcbuf_create(&r->alloc->vt, body, sizeof(body), &pl) ==
                   MOQ_OK);
    RDY_RETRY(r, moq_session_write_object(pub->peer, h, 0, pl, r->now));
    moq_rcbuf_decref(pl);
    RDY_RETRY(r, moq_session_close_subgroup(pub->peer, h, r->now));
}

/* Idle ACTIVE bindings cost ZERO delivery probes: after one full delivery
 * quiesces, repeated pumps must not touch the delivery half of ANY conn —
 * including a second, never-trafficked subscriber on an unrelated track.
 * A fresh publish then resumes within one pump (the mark is the wake). */
static int
ready_idle_zero_probes(void)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: ready_idle_zero_probes rig create\n");
        return 1;
    }
    conn_t *pub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *idle = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub && sub && idle);
    relay_bind_test_rig_pump(&rig, 4);
    peer_state_t pub_ps, s_ps, i_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&s_ps, 0, sizeof(s_ps));
    memset(&i_ps, 0, sizeof(i_ps));
    moq_subscription_t sh, ih;
    rdy_wire_track(&rig, pub, sub, "live", "cam", "v", true, &pub_ps,
                   &sh);
    R_CHECK(&rig, pub_ps.up_seen);
    /* The unrelated subscriber: its own track, never any traffic. */
    peer_state_t pub_ps2;
    memset(&pub_ps2, 0, sizeof(pub_ps2));
    rdy_wire_track(&rig, pub, idle, "live", "cam", "u", false,
                   &pub_ps2, &ih);

    relay_bind_test_pub_subgroup_plain_fin(&rig, pub, pub_ps.up_sub, 0, 0, 0);
    relay_bind_test_rig_pump(&rig, 12);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.objects == 1);

    /* Steady idle: N pumps, zero probes. */
    uint64_t p0 = moqr_bind_debug_delivery_probes();
    relay_bind_test_rig_pump(&rig, 20);
    R_CHECK(&rig, moqr_bind_debug_delivery_probes() == p0);

    /* A fresh publish wakes exactly through the mark. */
    relay_bind_test_pub_subgroup_plain_fin(&rig, pub, pub_ps.up_sub, 1, 0, 0);
    relay_bind_test_rig_pump(&rig, 12);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.objects == 2);
    R_CHECK(&rig, moqr_bind_debug_delivery_probes() > p0);
    relay_bind_test_peer_drain(&rig, idle, &i_ps, false, false, 0, 0);
    R_CHECK(&rig, i_ps.objects == 0);

    /* And idle again: the wake did not leave a standing poll behind. */
    uint64_t p1 = moqr_bind_debug_delivery_probes();
    relay_bind_test_rig_pump(&rig, 20);
    R_CHECK(&rig, moqr_bind_debug_delivery_probes() == p1);

    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: ready_idle_zero_probes\n");
    }
    return f;
}

/* ACTION_CAP: a delivery blocked on the session action queue parks with a
 * capacity floor. While the transport does not drain (capacity at the
 * floor), pumps cost ZERO probes; the first pump after the drain retries
 * once and completes. */
static int
ready_action_cap_park(void)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    relay_bind_test_max_actions = 1;
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        relay_bind_test_max_actions = 0;
        printf("FAIL: ready_action_cap_park rig create\n");
        return 1;
    }
    conn_t *pub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub && sub);
    relay_bind_test_rig_pump(&rig, 8);
    peer_state_t pub_ps, s_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&s_ps, 0, sizeof(s_ps));
    moq_subscription_t sh;
    rdy_wire_track(&rig, pub, sub, "live", "cam", "v", true, &pub_ps,
                   &sh);
    relay_bind_test_rig_pump(&rig, 8); /* setup spreads over more pumps at max_actions 1 */
    R_CHECK(&rig, pub_ps.up_seen);

    /* Publish paced through ONLY the publisher's transport: the object
     * ingests, the delivery attempt fills the subscriber session's single
     * action slot, and the binding parks — the subscriber's transport never
     * drains during this window. */
    {
        moq_subgroup_cfg_t sgc;
        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = 0;
        sgc.subgroup_id = 0;
        sgc.publisher_priority = 0;
        moq_subgroup_handle_t h;
        RDY_RETRY_ONE(&rig, pub, moq_session_open_subgroup(
                                     pub->peer, pub_ps.up_sub, &sgc, rig.now,
                                     &h));
        uint8_t body[4] = { 0xAC, 1, 2, 3 };
        moq_rcbuf_t *pl = NULL;
        R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                          MOQ_OK);
        RDY_RETRY_ONE(&rig, pub,
                      moq_session_write_object(pub->peer, h, 0, pl, rig.now));
        moq_rcbuf_decref(pl);
        RDY_RETRY_ONE(&rig, pub,
                      moq_session_close_subgroup(pub->peer, h, rig.now));
    }
    for (int i = 0; i < 3; i++) {
        rig.now += 1000;
        (void)moq_simpair_advance_to(pub->sp, rig.now);
        size_t steps = 0;
        (void)moq_simpair_run_until_quiescent(pub->sp, 64, &steps);
        (void)moqr_bind_pump(rig.bind, rig.now);
    }
    /* Parked: with the queue full and no drain, pumps are probe-free —
     * and the blocked conn (rig slot 1) shows EXACTLY ONE action-cap park
     * with no further park entries across the frozen window. */
    uint64_t bc0[3];
    moqr_bind_debug_conn_blocked_counts(rig.bind, 1, bc0);
    R_CHECK(&rig, bc0[0] == 1 && bc0[1] == 0 && bc0[2] == 0);
    /* The LIVE aggregate reports this as the CURRENT exceptional set: two
     * live conns, exactly one reached action-cap and is parked on it right
     * now; no session-sg park anywhere. This pins parked_* against a real
     * park (seeding-only tests cannot exercise the park bits). */
    {
        moqr_bind_blocked_agg_t agg;
        R_CHECK(&rig, moqr_bind_debug_blocked_aggregate(rig.bind, &agg) ==
                          MOQR_OK);
        R_CHECK(&rig, agg.live_conns == 2);
        R_CHECK(&rig, agg.conns_action_cap == 1 && agg.action_cap_total == 1);
        R_CHECK(&rig, agg.parked_action_cap == 1);
        R_CHECK(&rig, agg.conns_session_sg == 0 && agg.session_sg_total == 0);
        R_CHECK(&rig, agg.conns_bind_sg == 0 && agg.parked_session_sg == 0);
    }
    uint64_t p0 = moqr_bind_debug_delivery_probes();
    for (int i = 0; i < 6; i++) {
        rig.now += 1000;
        (void)moqr_bind_pump(rig.bind, rig.now);
    }
    R_CHECK(&rig, moqr_bind_debug_delivery_probes() == p0);
    {
        uint64_t bc1[3];
        moqr_bind_debug_conn_blocked_counts(rig.bind, 1, bc1);
        R_CHECK(&rig, bc1[0] == bc0[0]);   /* zero new parks while frozen */
    }

    /* Drain the subscriber transport once: capacity rises above the floor;
     * the next pump makes exactly one recovery pass. */
    rig.now += 1000;
    (void)moq_simpair_advance_to(sub->sp, rig.now);
    size_t steps = 0;
    (void)moq_simpair_run_until_quiescent(sub->sp, 64, &steps);
    uint64_t p1 = moqr_bind_debug_delivery_probes();
    (void)moqr_bind_pump(rig.bind, rig.now);
    uint64_t p2 = moqr_bind_debug_delivery_probes();
    R_CHECK(&rig, p2 > p1);
    R_CHECK(&rig, p2 - p1 <= 4);   /* one bounded pass, not a poll storm */

    relay_bind_test_rig_pump(&rig, 12);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.objects == 1);

    relay_bind_test_max_actions = 0;
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: ready_action_cap_park\n");
    }
    return f;
}

/* The LIVE blocked aggregate: seeded per-conn counters drive the summation,
 * LIVE-only exclusion of a detached slot that keeps stale counters, clean
 * reuse of a recycled slot, the fail-closed NULL guards, and the checked-add
 * overflow refusal. (Park populations are pinned separately against a REAL
 * park in ready_action_cap_park — seeding sets only the history counters.) */
static int
ready_blocked_aggregate(void)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: ready_blocked_aggregate rig create\n");
        return 1;
    }
    conn_t *c0 = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *c1 = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *c2 = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, c0 && c1 && c2);
    relay_bind_test_rig_pump(&rig, 4);

    /* Seed each live slot with a distinct reason's history. */
    const uint64_t s0[3] = { 5, 0, 0 };   /* action-cap */
    const uint64_t s1[3] = { 0, 3, 0 };   /* session-sg */
    const uint64_t s2[3] = { 0, 0, 7 };   /* bind-sg     */
    R_CHECK(&rig, moqr_bind_debug_seed_blocked(rig.bind, 0, s0));
    R_CHECK(&rig, moqr_bind_debug_seed_blocked(rig.bind, 1, s1));
    R_CHECK(&rig, moqr_bind_debug_seed_blocked(rig.bind, 2, s2));

    {
        moqr_bind_blocked_agg_t agg;
        R_CHECK(&rig, moqr_bind_debug_blocked_aggregate(rig.bind, &agg) ==
                          MOQR_OK);
        R_CHECK(&rig, agg.live_conns == 3);
        R_CHECK(&rig, agg.conns_action_cap == 1 && agg.action_cap_total == 5);
        R_CHECK(&rig, agg.conns_session_sg == 1 && agg.session_sg_total == 3);
        R_CHECK(&rig, agg.conns_bind_sg == 1 && agg.bind_sg_total == 7);
        R_CHECK(&rig, agg.parked_action_cap == 0 &&
                          agg.parked_session_sg == 0);
    }

    /* Seeding a non-live or out-of-range slot is refused. */
    R_CHECK(&rig, !moqr_bind_debug_seed_blocked(rig.bind, 99, s0));
    R_CHECK(&rig, !moqr_bind_debug_seed_blocked(NULL, 0, s0));

    /* NULL guards fail closed with a zeroed output. */
    {
        moqr_bind_blocked_agg_t agg;
        memset(&agg, 0xAB, sizeof(agg));
        R_CHECK(&rig, moqr_bind_debug_blocked_aggregate(NULL, &agg) ==
                          MOQR_ERR_INVAL);
        R_CHECK(&rig, agg.live_conns == 0 && agg.action_cap_total == 0);
        R_CHECK(&rig, moqr_bind_debug_blocked_aggregate(rig.bind, NULL) ==
                          MOQR_ERR_INVAL);
    }

    /* Overflow: two live slots each at UINT64_MAX for the same reason make
     * the total's checked add refuse; the output is zeroed (no partial sum). */
    {
        const uint64_t big[3] = { UINT64_MAX, 0, 0 };
        R_CHECK(&rig, moqr_bind_debug_seed_blocked(rig.bind, 0, big));
        R_CHECK(&rig, moqr_bind_debug_seed_blocked(rig.bind, 2, big));
        moqr_bind_blocked_agg_t agg;
        memset(&agg, 0xAB, sizeof(agg));
        R_CHECK(&rig, moqr_bind_debug_blocked_aggregate(rig.bind, &agg) ==
                          MOQR_ERR_INTERNAL);
        R_CHECK(&rig, agg.live_conns == 0 && agg.action_cap_total == 0 &&
                          agg.conns_action_cap == 0);
        /* restore sane seeds */
        R_CHECK(&rig, moqr_bind_debug_seed_blocked(rig.bind, 0, s0));
        R_CHECK(&rig, moqr_bind_debug_seed_blocked(rig.bind, 2, s2));
    }

    /* Detach slot 1: its session-sg counter stays on the slot (stale), but a
     * LIVE aggregate must exclude it. Prove the counter is still physically
     * present via the raw per-slot getter, yet absent from the aggregate. */
    R_CHECK(&rig, moqr_bind_conn_close(rig.bind, c1->rsess) == MOQR_OK);
    relay_bind_test_rig_pump(&rig, 4); /* let any WOULD_BLOCK detach settle */
    {
        uint64_t raw[3];
        moqr_bind_debug_conn_blocked_counts(rig.bind, 1, raw);
        R_CHECK(&rig, raw[1] == 3);   /* counter retained on the dead slot */
        moqr_bind_blocked_agg_t agg;
        R_CHECK(&rig, moqr_bind_debug_blocked_aggregate(rig.bind, &agg) ==
                          MOQR_OK);
        R_CHECK(&rig, agg.live_conns == 2);
        R_CHECK(&rig, agg.conns_session_sg == 0 && agg.session_sg_total == 0);
        R_CHECK(&rig, agg.conns_action_cap == 1 && agg.action_cap_total == 5);
        R_CHECK(&rig, agg.conns_bind_sg == 1 && agg.bind_sg_total == 7);
    }

    /* Reuse: a new conn takes the lowest free bind slot (slot 1), whose
     * counters were memset clean on open — the stale history is gone. */
    conn_t *c3 = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, c3 != NULL);
    relay_bind_test_rig_pump(&rig, 4);
    {
        uint64_t raw[3];
        moqr_bind_debug_conn_blocked_counts(rig.bind, 1, raw);
        R_CHECK(&rig, raw[0] == 0 && raw[1] == 0 && raw[2] == 0);
        moqr_bind_blocked_agg_t agg;
        R_CHECK(&rig, moqr_bind_debug_blocked_aggregate(rig.bind, &agg) ==
                          MOQR_OK);
        R_CHECK(&rig, agg.live_conns == 3);
        R_CHECK(&rig, agg.conns_session_sg == 0 && agg.session_sg_total == 0);
    }

    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: ready_blocked_aggregate\n");
    }
    return f;
}

/* BIND_SG: with a one-slot downstream subgroup pool, a second concurrent
 * subgroup blocks WITHOUT parking or polling (zero probes while the slot is
 * held) and resumes from the slot-release re-arm when the first subgroup
 * seals — no new object is published between block and resume. */
static int
ready_bind_sg_rearm(void)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    relay_bind_test_max_open_subgroups = 1;
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        relay_bind_test_max_open_subgroups = 0;
        printf("FAIL: ready_bind_sg_rearm rig create\n");
        return 1;
    }
    conn_t *pub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub && sub);
    relay_bind_test_rig_pump(&rig, 4);
    peer_state_t pub_ps, s_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&s_ps, 0, sizeof(s_ps));
    moq_subscription_t sh;
    rdy_wire_track(&rig, pub, sub, "live", "cam", "v", true, &pub_ps,
                   &sh);
    R_CHECK(&rig, pub_ps.up_seen);

    /* Subgroup A: one object, stream left OPEN (holds the only slot). */
    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = 0;
    sgc.subgroup_id = 0;
    sgc.publisher_priority = 0;
    moq_subgroup_handle_t sga;
    R_CHECK(&rig, moq_session_open_subgroup(pub->peer, pub_ps.up_sub, &sgc,
                                            rig.now, &sga) == MOQ_OK);
    {
        uint8_t body[4] = { 0xA0, 1, 2, 3 };
        moq_rcbuf_t *pl = NULL;
        R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                          MOQ_OK);
        R_CHECK(&rig, moq_session_write_object(pub->peer, sga, 0, pl,
                                               rig.now) == MOQ_OK);
        moq_rcbuf_decref(pl);
    }
    relay_bind_test_rig_pump(&rig, 8);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.objects == 1);

    /* Subgroup B (group 1): blocked on the bind slot pool. */
    relay_bind_test_pub_subgroup_plain_fin(&rig, pub, pub_ps.up_sub, 1, 0, 0);
    relay_bind_test_rig_pump(&rig, 6);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.objects == 1);   /* still held out */

    /* No park, no poll: zero probes while the slot stays held — and the
     * blocked conn (rig slot 1) shows EXACTLY ONE bind-sg refusal, with no
     * growth across the quiet window. */
    uint64_t bc0[3];
    moqr_bind_debug_conn_blocked_counts(rig.bind, 1, bc0);
    R_CHECK(&rig, bc0[2] == 1 && bc0[0] == 0 && bc0[1] == 0);
    uint64_t p0 = moqr_bind_debug_delivery_probes();
    relay_bind_test_rig_pump(&rig, 10);
    R_CHECK(&rig, moqr_bind_debug_delivery_probes() == p0);
    {
        uint64_t bc1[3];
        moqr_bind_debug_conn_blocked_counts(rig.bind, 1, bc1);
        R_CHECK(&rig, bc1[2] == bc0[2]);
    }

    /* FIN A upstream: the SEAL closes the downstream stream, the freed slot
     * re-arms the binding, and B flows — with no new ingest. */
    R_CHECK(&rig, moq_session_close_subgroup(pub->peer, sga, rig.now) ==
                      MOQ_OK);
    relay_bind_test_rig_pump(&rig, 12);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.objects == 2);

    relay_bind_test_max_open_subgroups = 0;
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: ready_bind_sg_rearm\n");
    }
    return f;
}

/* A hot binding with >256 queued deliveries cannot starve an intermittent
 * one: the guard ends the hot pass, the hot binding self-rearms (BUDGET),
 * and the intermittent binding is served in the same pump. */
static int
ready_hot_not_starving(void)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    relay_bind_test_max_events = 512; /* consumers must outlive a 256-burst pass */
    relay_bind_test_max_actions = 1024; /* the guard, not the queue, must bind      */
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        relay_bind_test_max_events = 0;
        relay_bind_test_max_actions = 0;
        printf("FAIL: ready_hot_not_starving rig create\n");
        return 1;
    }
    conn_t *pub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *starter = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *hot = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *cold = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub && starter && hot && cold);
    relay_bind_test_rig_pump(&rig, 4);
    peer_state_t pub_ps, st_ps, h_ps, c_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&st_ps, 0, sizeof(st_ps));
    memset(&h_ps, 0, sizeof(h_ps));
    memset(&c_ps, 0, sizeof(c_ps));
    moq_subscription_t sth, hh, ch;
    rdy_wire_track(&rig, pub, starter, "live", "cam", "hotv", true, &pub_ps,
                   &sth);
    R_CHECK(&rig, pub_ps.up_seen);
    peer_state_t pub_ps2;
    memset(&pub_ps2, 0, sizeof(pub_ps2));
    rdy_wire_track(&rig, pub, cold, "live", "cam", "coldv", false,
                   &pub_ps2, &ch);
    R_CHECK(&rig, pub_ps2.up_seen);

    /* Retain 300 objects in ONE subgroup (paced; the starter consumes them
     * live), then LATE-JOIN the hot subscriber: its accept mark schedules a
     * 300-object catch-up — deeper than the 256-per-pass guard. */
    {
        moq_subgroup_cfg_t sgc;
        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = 0;
        sgc.subgroup_id = 0;
        sgc.publisher_priority = 0;
        moq_subgroup_handle_t h;
        RDY_RETRY(&rig, moq_session_open_subgroup(pub->peer, pub_ps.up_sub,
                                                  &sgc, rig.now, &h));
        for (uint64_t o = 0; o < 300; o++) {
            uint8_t body[4] = { (uint8_t)o, 1, 2, 3 };
            moq_rcbuf_t *pl = NULL;
            R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                              MOQ_OK);
            RDY_RETRY(&rig, moq_session_write_object(pub->peer, h, o, pl,
                                                     rig.now));
            moq_rcbuf_decref(pl);
            if ((o & 15u) == 15u) {
                relay_bind_test_rig_cycle(&rig); /* pace the wire burst */
            }
        }
        /* The subgroup stays OPEN: a FIN's SEAL notice would mark the
         * binding and mask a lost BUDGET self-rearm. */
    }
    for (int w = 0; w < 200 && st_ps.objects < 300; w++) {
        rdy_deep_cycle(&rig);
        relay_bind_test_peer_drain(&rig, starter, &st_ps, false, false, 0, 0);
    }
    R_CHECK(&rig, st_ps.objects == 300);

    /* Late-join with the transports held still around the catch-up: the
     * first delivery pass must write EXACTLY the 256-delivery guard and the
     * SECOND pass (same pump cadence, no transport help, no new mark) must
     * make progress — that progress exists ONLY through the BUDGET
     * self-rearm. */
    {
        moq_bytes_t nsp[2] = { B("live"), B("cam") };
        moq_subscribe_cfg_t scfg;
        memset(&scfg, 0, sizeof(scfg));
        moq_subscribe_cfg_init(&scfg);
        moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
        scfg.track_name = B("hotv");
        scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
        R_CHECK(&rig, moq_session_subscribe(hot->peer, &scfg, rig.now,
                                            &hh) == MOQ_OK);
    }
    rig.now += 1000;
    (void)moq_simpair_advance_to(hot->sp, rig.now);
    {
        size_t steps = 0;
        (void)moq_simpair_run_until_quiescent(hot->sp, 4096, &steps);
    }
    {
        moqr_bind_stats_t bs;
        moqr_bind_get_stats(rig.bind, &bs);
        uint64_t w0 = bs.deliveries_written;
        (void)moqr_bind_pump(rig.bind, rig.now);   /* accept + pass 1 */
        moqr_bind_get_stats(rig.bind, &bs);
        R_CHECK(&rig, bs.deliveries_written - w0 == 256);   /* the guard */
        uint64_t w1 = bs.deliveries_written;
        rig.now += 1000;
        (void)moqr_bind_pump(rig.bind, rig.now);   /* pass 2: self-rearm */
        moqr_bind_get_stats(rig.bind, &bs);
        R_CHECK(&rig, bs.deliveries_written > w1);
    }
    rdy_pub_one(&rig, pub, pub_ps2.up_sub, 0);
    relay_bind_test_rig_pump(&rig, 3);
    relay_bind_test_peer_drain(&rig, cold, &c_ps, false, false, 0, 0);
    relay_bind_test_peer_drain(&rig, hot, &h_ps, false, false, 0, 0);
    R_CHECK(&rig, c_ps.objects == 1);   /* served despite hot's backlog */
    for (int w = 0; w < 200 && h_ps.objects < 300; w++) {
        rdy_deep_cycle(&rig);
        relay_bind_test_peer_drain(&rig, hot, &h_ps, false, false, 0, 0);
    }
    R_CHECK(&rig, h_ps.objects == 300);   /* BUDGET self-rearm completed it */

    relay_bind_test_max_events = 0;
    relay_bind_test_max_actions = 0;
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: ready_hot_not_starving\n");
    }
    return f;
}

/* SESSION_SG: with the relay session's outgoing subgroup pool at 2 and the
 * bind slot pool roomy, a third concurrent subgroup blocks on the SESSION
 * pool (WOULD_BLOCK with action-queue room). The parked binding gets
 * EXACTLY ONE bounded attempt per pump — no more — and recovers through the
 * reap performed by a later attempt's own open prologue once an upstream
 * FIN closes a downstream stream; the action-queue capacity round-trips to
 * the same value across that cycle, so no capacity gate could see it. */
static int
ready_session_sg_poll(void)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    relay_bind_test_max_open_subgroups = 8; /* bind slots: NOT the blocker */
    relay_bind_test_server_max_open_subgroups = 2; /* session pool: the blocker   */
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        relay_bind_test_max_open_subgroups = 0;
        relay_bind_test_server_max_open_subgroups = 0;
        printf("FAIL: ready_session_sg_poll rig create\n");
        return 1;
    }
    conn_t *pub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub && sub);
    relay_bind_test_rig_pump(&rig, 4);
    peer_state_t pub_ps, s_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&s_ps, 0, sizeof(s_ps));
    moq_subscription_t sh;
    rdy_wire_track(&rig, pub, sub, "live", "cam", "v", true, &pub_ps, &sh);
    R_CHECK(&rig, pub_ps.up_seen);

    /* Three concurrent upstream subgroups (groups 0..2), one object each,
     * streams left OPEN: two occupy the session pool, the third blocks. */
    moq_subgroup_handle_t sg[3];
    for (uint64_t g = 0; g < 3; g++) {
        moq_subgroup_cfg_t sgc;
        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = g;
        sgc.subgroup_id = 0;
        sgc.publisher_priority = 0;
        RDY_RETRY(&rig, moq_session_open_subgroup(pub->peer, pub_ps.up_sub,
                                                  &sgc, rig.now, &sg[g]));
        uint8_t body[4] = { (uint8_t)g, 5, 6, 7 };
        moq_rcbuf_t *pl = NULL;
        R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                          MOQ_OK);
        RDY_RETRY(&rig,
                  moq_session_write_object(pub->peer, sg[g], 0, pl, rig.now));
        moq_rcbuf_decref(pl);
    }
    relay_bind_test_rig_pump(&rig, 10);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.objects == 2);   /* the third is pool-blocked */

    /* EXACTLY one SESSION_SG re-attempt per blocked binding per pump (the
     * reason-specific gauge counts pump-granted attempts), and the
     * exceptional set holds exactly this one binding. */
    {
        uint64_t bc0[3];
        moqr_bind_debug_conn_blocked_counts(rig.bind, 1, bc0);
        for (int i = 0; i < 4; i++) {
            uint64_t g0 = moqr_bind_debug_sg_attempts();
            relay_bind_test_rig_cycle(&rig);
            R_CHECK(&rig, moqr_bind_debug_sg_attempts() - g0 == 1);
            R_CHECK(&rig, moqr_bind_debug_sg_park_count(rig.bind) == 1);
        }
        uint64_t bc1[3];
        moqr_bind_debug_conn_blocked_counts(rig.bind, 1, bc1);
        /* every granted attempt re-blocked in the session pool: exact */
        R_CHECK(&rig, bc1[1] - bc0[1] == 4);
    }

    /* FIN subgroup 0: the SEAL closes one downstream stream (CLOSING); the
     * next attempt's own prologue reaps it and the third subgroup opens —
     * the action queue ends where it started (same-value cycle). */
    RDY_RETRY(&rig, moq_session_close_subgroup(pub->peer, sg[0], rig.now));
    relay_bind_test_rig_pump(&rig, 10);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.objects == 3);

    relay_bind_test_max_open_subgroups = 0;
    relay_bind_test_server_max_open_subgroups = 0;
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: ready_session_sg_poll\n");
    }
    return f;
}

/* The control-path SUB_DONE re-arm (the one recovery edge with NO core mark
 * guarantee): subscription X holds the ONLY bind subgroup slot mid-object;
 * sibling Y (same connection, other track) is BIND_SG-blocked. X's track is
 * force-withdrawn while the subscriber's transport is held still, so the
 * SUB_DONE intent DEFERS (action queue full) past the pump that consumed
 * X's retirement mark. When the deferred intent finally executes, its slot
 * release must re-arm the binding — nothing else ever will. */
static int
ready_subdone_rearm(void)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    relay_bind_test_max_open_subgroups = 1;
    relay_bind_test_max_actions = 1;
    relay_bind_test_server_streaming_objects = true; /* X stays mid-object (begun) */
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        relay_bind_test_max_open_subgroups = 0;
        relay_bind_test_max_actions = 0;
        relay_bind_test_server_streaming_objects = false;
        printf("FAIL: ready_subdone_rearm rig create\n");
        return 1;
    }
    conn_t *pub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub && sub);
    relay_bind_test_rig_pump(&rig, 8);
    peer_state_t px, py, s_ps;
    memset(&px, 0, sizeof(px));
    memset(&py, 0, sizeof(py));
    memset(&s_ps, 0, sizeof(s_ps));
    moq_subscription_t xh, yh;
    /* X on namespace {wa,x}; Y on {wb,y} — force_withdraw hits X only. */
    rdy_wire_track(&rig, pub, sub, "wa", "x", "tx", true, &px, &xh);
    relay_bind_test_rig_pump(&rig, 8);
    R_CHECK(&rig, px.up_seen);
    rdy_wire_track(&rig, pub, sub, "wb", "y", "ty", true, &py, &yh);
    relay_bind_test_rig_pump(&rig, 8);
    R_CHECK(&rig, py.up_seen);

    /* X: open subgroup + one object, stream open — X holds the only bind
     * slot with a begun object downstream. */
    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = 0;
    sgc.subgroup_id = 0;
    sgc.publisher_priority = 0;
    moq_subgroup_handle_t sgx;
    RDY_RETRY_ONE(&rig, pub, moq_session_open_subgroup(
                                 pub->peer, px.up_sub, &sgc, rig.now, &sgx));
    {
        uint8_t body[4] = { 0x58, 1, 2, 3 };
        moq_rcbuf_t *pl = NULL;
        R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                          MOQ_OK);
        RDY_RETRY_ONE(&rig, pub, moq_session_write_object(pub->peer, sgx, 0,
                                                          pl, rig.now));
        moq_rcbuf_decref(pl);
    }
    relay_bind_test_rig_pump(&rig, 10);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.objects >= 1 || s_ps.chunks >= 1);

    /* Y: one whole object — blocked on the bind slot X holds. */
    {
        moq_subgroup_cfg_t syc;
        moq_subgroup_cfg_init(&syc);
        syc.group_id = 0;
        syc.subgroup_id = 0;
        syc.publisher_priority = 0;
        moq_subgroup_handle_t sgy;
        RDY_RETRY_ONE(&rig, pub, moq_session_open_subgroup(
                                     pub->peer, py.up_sub, &syc, rig.now,
                                     &sgy));
        uint8_t body[4] = { 0x59, 1, 2, 3 };
        moq_rcbuf_t *pl = NULL;
        R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                          MOQ_OK);
        RDY_RETRY_ONE(&rig, pub, moq_session_write_object(pub->peer, sgy, 0,
                                                          pl, rig.now));
        moq_rcbuf_decref(pl);
        /* Y's subgroup stays OPEN: a FIN's SEAL notice would mark the
         * binding and mask a lost slot-release re-arm. */
    }
    for (int i = 0; i < 4; i++) {
        rig.now += 1000;
        (void)moq_simpair_advance_to(pub->sp, rig.now);
        size_t steps = 0;
        (void)moq_simpair_run_until_quiescent(pub->sp, 64, &steps);
        (void)moqr_bind_pump(rig.bind, rig.now);
    }

    /* Withdraw X's namespace with the SUBSCRIBER transport held still: the
     * SUB_DONE reset needs the single action slot, so the intent defers
     * while the retirement mark is consumed by a still-blocked attempt. */
    {
        moq_bytes_t nsp[2] = { B("wa"), B("x") };
        moqr_result_t wrc = moqr_core_force_withdraw(
            rig.core, (moqr_ns_t){ nsp, 2 }, 0x10, rig.now);
        R_CHECK(&rig, wrc == MOQR_OK || wrc == MOQR_ERR_WOULD_BLOCK);
        for (int i = 0; wrc == MOQR_ERR_WOULD_BLOCK && i < 100; i++) {
            rig.now += 1000;
            (void)moqr_bind_pump(rig.bind, rig.now);
            wrc = moqr_core_force_withdraw(rig.core, (moqr_ns_t){ nsp, 2 },
                                           0x10, rig.now);
        }
        R_CHECK(&rig, wrc == MOQR_OK);
    }
    for (int i = 0; i < 3; i++) {
        rig.now += 1000;
        (void)moqr_bind_pump(rig.bind, rig.now);   /* mark consumed here */
    }
    /* Release ONLY the subscriber's transport: the deferred SUB_DONE
     * executes over the next pumps (its reset consumes the single action
     * slot, so the DONE itself may re-defer once more, holding Phase F
     * off), and Y's held delivery resumes purely through the slot-release
     * re-arm — no other transport moves, so no mark can rescue a lost one. */
    {
        moqr_bind_stats_t bs;
        moqr_bind_get_stats(rig.bind, &bs);
        uint64_t w0 = bs.deliveries_written;
        bool resumed = false;
        for (int i = 0; i < 6 && !resumed; i++) {
            rig.now += 1000;
            (void)moq_simpair_advance_to(sub->sp, rig.now);
            size_t steps = 0;
            (void)moq_simpair_run_until_quiescent(sub->sp, 4096, &steps);
            (void)moqr_bind_pump(rig.bind, rig.now);
            moqr_bind_get_stats(rig.bind, &bs);
            resumed = bs.deliveries_written > w0;
        }
        R_CHECK(&rig, resumed);   /* Y resumed with the sub transport only */
    }
    relay_bind_test_rig_pump(&rig, 12);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.objects >= 1);     /* Y's object flowed after */
    R_CHECK(&rig, s_ps.last_byte == 0x59);

    relay_bind_test_max_open_subgroups = 0;
    relay_bind_test_max_actions = 0;
    relay_bind_test_server_streaming_objects = false;
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: ready_subdone_rearm\n");
    }
    return f;
}

/* A failed delivery confirmation is a consistency failure: the connection
 * must FAIL CLOSED (session closed, conn detached) — never survive as a
 * live, unschedulable zombie. The slot must be fully reusable after. */
static int
ready_confirm_failure_closes(void)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: ready_confirm_failure_closes rig create\n");
        return 1;
    }
    conn_t *pub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub && sub);
    relay_bind_test_rig_pump(&rig, 4);
    peer_state_t pub_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    moq_subscription_t sh;
    rdy_wire_track(&rig, pub, sub, "live", "cam", "v", true, &pub_ps, &sh);
    R_CHECK(&rig, pub_ps.up_seen);

    /* Inject: the next confirmation misfires mid-delivery. */
    moqr_bind_debug_fail_confirm(1);
    relay_bind_test_pub_subgroup_plain_fin(&rig, pub, pub_ps.up_sub, 0, 0, 0);
    relay_bind_test_rig_pump(&rig, 8);
    moqr_bind_debug_fail_confirm(0);
    R_CHECK(&rig, !moqr_bind_conn_is_open(rig.bind, sub->rsess));
    {
        moqr_bind_stats_t bs;
        moqr_bind_get_stats(rig.bind, &bs);
        R_CHECK(&rig, bs.session_errors >= 1);
    }

    /* The slot is reusable and probe-clean from its first pump. */
    moq_simpair_destroy(sub->sp);
    sub->used = false;
    conn_t *re = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, re != NULL);
    uint64_t p0 = moqr_bind_debug_delivery_probes();
    relay_bind_test_rig_pump(&rig, 8);
    R_CHECK(&rig, moqr_bind_debug_delivery_probes() == p0);
    peer_state_t r_ps, pub_ps2;
    memset(&r_ps, 0, sizeof(r_ps));
    memset(&pub_ps2, 0, sizeof(pub_ps2));
    moq_subscription_t rh;
    rdy_wire_track(&rig, pub, re, "live", "cam", "w", false, &pub_ps2, &rh);
    R_CHECK(&rig, pub_ps2.up_seen);
    rdy_pub_one(&rig, pub, pub_ps2.up_sub, 0);
    relay_bind_test_rig_pump(&rig, 10);
    relay_bind_test_peer_drain(&rig, re, &r_ps, false, false, 0, 0);
    R_CHECK(&rig, r_ps.objects == 1);

    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: ready_confirm_failure_closes\n");
    }
    return f;
}

/* The probe-error classification is CAPACITY-only: an injected CAPACITY
 * result self-rearms and the delivery completes on the next pump; every
 * other injected error (NOMEM included — the signed state machine does not
 * allow it as transient) fails the connection closed. */
static int
ready_probe_error_policy(void)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: ready_probe_error_policy rig create\n");
        return 1;
    }
    conn_t *pub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub && sub);
    relay_bind_test_rig_pump(&rig, 4);
    peer_state_t pub_ps, s_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&s_ps, 0, sizeof(s_ps));
    moq_subscription_t sh;
    rdy_wire_track(&rig, pub, sub, "live", "cam", "v", true, &pub_ps, &sh);
    R_CHECK(&rig, pub_ps.up_seen);

    /* CAPACITY: transient — the object still arrives. */
    moqr_bind_debug_fail_probe(1, MOQR_ERR_CAPACITY);
    relay_bind_test_pub_subgroup_plain_fin(&rig, pub, pub_ps.up_sub, 0, 0, 0);
    relay_bind_test_rig_pump(&rig, 10);
    moqr_bind_debug_fail_probe(0, MOQR_OK);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.objects == 1);
    R_CHECK(&rig, moqr_bind_conn_is_open(rig.bind, sub->rsess));

    /* NOMEM: NOT transient — the connection fails closed. */
    moqr_bind_debug_fail_probe(1, MOQR_ERR_NOMEM);
    relay_bind_test_pub_subgroup_plain_fin(&rig, pub, pub_ps.up_sub, 1, 0, 0);
    relay_bind_test_rig_pump(&rig, 10);
    moqr_bind_debug_fail_probe(0, MOQR_OK);
    R_CHECK(&rig, !moqr_bind_conn_is_open(rig.bind, sub->rsess));
    {
        moqr_bind_stats_t bs;
        moqr_bind_get_stats(rig.bind, &bs);
        R_CHECK(&rig, bs.session_errors >= 1);
    }

    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: ready_probe_error_policy\n");
    }
    return f;
}

/* A property-bearing object write needs TWO action slots, so it refuses
 * with capacity 1: the park must record THAT floor and make ZERO retries
 * until capacity exceeds it — a merely-nonzero gate would retry every
 * pump. */
static int
ready_property_write_floor(void)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    relay_bind_test_max_actions = 2; /* open takes one slot; props need two */
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        relay_bind_test_max_actions = 0;
        printf("FAIL: ready_property_write_floor rig create\n");
        return 1;
    }
    conn_t *pub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub && sub);
    relay_bind_test_rig_pump(&rig, 8);
    peer_state_t pub_ps, s_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&s_ps, 0, sizeof(s_ps));
    moq_subscription_t sh;
    rdy_wire_track(&rig, pub, sub, "live", "cam", "v", true, &pub_ps, &sh);
    relay_bind_test_rig_pump(&rig, 8);
    R_CHECK(&rig, pub_ps.up_seen);

    /* One object WITH properties, paced through the publisher only: the
     * downstream open consumes one of two slots, the property write then
     * sees capacity 1 < 2 and refuses. */
    {
        moq_subgroup_cfg_t sgc;
        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = 0;
        sgc.subgroup_id = 0;
        sgc.publisher_priority = 0;
        sgc.object_properties = true;
        moq_subgroup_handle_t h;
        RDY_RETRY_ONE(&rig, pub, moq_session_open_subgroup(
                                     pub->peer, pub_ps.up_sub, &sgc, rig.now,
                                     &h));
        uint8_t body[4] = { 0xF1, 1, 2, 3 };
        /* One valid property entry (type 2, varint value 9) — the same
         * wire form both drafts accept. */
        uint8_t props[2] = { 0x02, 0x09 };
        moq_rcbuf_t *pl = NULL;
        R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                          MOQ_OK);
        moq_object_cfg_t ocfg;
        moq_object_cfg_init(&ocfg);
        ocfg.object_id = 0;
        ocfg.payload = pl;
        moq_rcbuf_t *pr = NULL;
        R_CHECK(&rig, moq_rcbuf_create(&a.vt, props, sizeof(props), &pr) ==
                          MOQ_OK);
        ocfg.properties = pr;
        RDY_RETRY_ONE(&rig, pub, moq_session_write_object_ex(pub->peer, h,
                                                             &ocfg, rig.now));
        moq_rcbuf_decref(pl);
        moq_rcbuf_decref(pr);
    }
    for (int i = 0; i < 3; i++) {
        rig.now += 1000;
        (void)moq_simpair_advance_to(pub->sp, rig.now);
        size_t steps = 0;
        (void)moq_simpair_run_until_quiescent(pub->sp, 64, &steps);
        (void)moqr_bind_pump(rig.bind, rig.now);
    }
    /* Parked with the OBSERVED floor: with the subscriber transport held,
     * capacity stays AT the floor and pumps make zero probes/attempts. */
    uint64_t p0 = moqr_bind_debug_delivery_probes();
    for (int i = 0; i < 6; i++) {
        rig.now += 1000;
        (void)moqr_bind_pump(rig.bind, rig.now);
    }
    R_CHECK(&rig, moqr_bind_debug_delivery_probes() == p0);

    /* Drain once: capacity rises past the floor; the delivery completes. */
    rig.now += 1000;
    (void)moq_simpair_advance_to(sub->sp, rig.now);
    {
        size_t steps = 0;
        (void)moq_simpair_run_until_quiescent(sub->sp, 4096, &steps);
    }
    (void)moqr_bind_pump(rig.bind, rig.now);
    relay_bind_test_rig_pump(&rig, 10);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.objects == 1);
    R_CHECK(&rig, s_ps.last_props_len == 2);

    relay_bind_test_max_actions = 0;
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: ready_property_write_floor\n");
    }
    return f;
}

/* force_withdraw against an IDLE system must wake the subscriber's binding:
 * the terminal reaches the wire although no delivery mark preceded it. */
static int
ready_withdraw_idle_wake(void)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: ready_withdraw_idle_wake rig create\n");
        return 1;
    }
    conn_t *pub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub && sub);
    relay_bind_test_rig_pump(&rig, 4);
    peer_state_t pub_ps, s_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&s_ps, 0, sizeof(s_ps));
    moq_subscription_t sh;
    rdy_wire_track(&rig, pub, sub, "live", "cam", "v", true, &pub_ps,
                   &sh);
    R_CHECK(&rig, pub_ps.up_seen);
    relay_bind_test_pub_subgroup_plain_fin(&rig, pub, pub_ps.up_sub, 0, 0, 0);
    relay_bind_test_rig_pump(&rig, 10);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.objects == 1);

    /* Idle... then a relay-topology withdrawal mid-idle. */
    uint64_t p0 = moqr_bind_debug_delivery_probes();
    relay_bind_test_rig_pump(&rig, 10);
    R_CHECK(&rig, moqr_bind_debug_delivery_probes() == p0);
    moq_bytes_t nsp[2] = { B("live"), B("cam") };
    R_CHECK(&rig, moqr_core_force_withdraw(rig.core,
                                           (moqr_ns_t){ nsp, 2 }, 0x10,
                                           rig.now) == MOQR_OK);
    relay_bind_test_rig_pump(&rig, 8);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.done_seen);   /* the DONE reached the wire */

    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: ready_withdraw_idle_wake\n");
    }
    return f;
}

/* Detach with standing ready/parked state must not leak into the slot's next
 * claimant: a replacement connection starts probe-free and fully usable. */
static int
ready_detach_replacement_clean(void)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    relay_bind_test_max_actions = 1;
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        relay_bind_test_max_actions = 0;
        printf("FAIL: ready_detach_replacement_clean rig create\n");
        return 1;
    }
    conn_t *pub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub && sub);
    relay_bind_test_rig_pump(&rig, 8);
    peer_state_t pub_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    moq_subscription_t sh;
    rdy_wire_track(&rig, pub, sub, "live", "cam", "v", true, &pub_ps,
                   &sh);
    relay_bind_test_rig_pump(&rig, 8);
    R_CHECK(&rig, pub_ps.up_seen);

    /* Park the subscriber's binding (blocked delivery, undrained queue). */
    {
        moq_subgroup_cfg_t sgc;
        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = 0;
        sgc.subgroup_id = 0;
        sgc.publisher_priority = 0;
        moq_subgroup_handle_t h;
        RDY_RETRY_ONE(&rig, pub, moq_session_open_subgroup(
                                     pub->peer, pub_ps.up_sub, &sgc, rig.now,
                                     &h));
        uint8_t body[4] = { 0xDC, 1, 2, 3 };
        moq_rcbuf_t *pl = NULL;
        R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                          MOQ_OK);
        RDY_RETRY_ONE(&rig, pub,
                      moq_session_write_object(pub->peer, h, 0, pl, rig.now));
        moq_rcbuf_decref(pl);
        RDY_RETRY_ONE(&rig, pub,
                      moq_session_close_subgroup(pub->peer, h, rig.now));
    }
    for (int i = 0; i < 3; i++) {
        rig.now += 1000;
        (void)moq_simpair_advance_to(pub->sp, rig.now);
        size_t steps = 0;
        (void)moq_simpair_run_until_quiescent(pub->sp, 64, &steps);
        (void)moqr_bind_pump(rig.bind, rig.now);
    }
    /* Detach mid-park; the conn slot is reclaimed by a fresh connection. */
    R_CHECK(&rig, moqr_bind_conn_close(rig.bind, sub->rsess) == MOQR_OK);
    relay_bind_test_rig_pump(&rig, 4);
    moq_simpair_destroy(sub->sp);
    sub->used = false;
    conn_t *re = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, re != NULL);

    /* The replacement inherits nothing — asserted from its very first
     * pump: even a single stray probe means state leaked across reuse. */
    uint64_t p0 = moqr_bind_debug_delivery_probes();
    relay_bind_test_rig_pump(&rig, 10);
    R_CHECK(&rig, moqr_bind_debug_delivery_probes() == p0);

    /* And it is fully usable end-to-end. */
    peer_state_t r_ps;
    memset(&r_ps, 0, sizeof(r_ps));
    moq_subscription_t rh;
    peer_state_t pub_ps2;
    memset(&pub_ps2, 0, sizeof(pub_ps2));
    rdy_wire_track(&rig, pub, re, "live", "cam", "w", false, &pub_ps2,
                   &rh);
    relay_bind_test_rig_pump(&rig, 8);
    R_CHECK(&rig, pub_ps2.up_seen);
    rdy_pub_one(&rig, pub, pub_ps2.up_sub, 0);
    relay_bind_test_rig_pump(&rig, 12);
    relay_bind_test_peer_drain(&rig, re, &r_ps, false, false, 0, 0);
    R_CHECK(&rig, r_ps.objects == 1);

    relay_bind_test_max_actions = 0;
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: ready_detach_replacement_clean\n");
    }
    return f;
}

/* ========================= DOWNSTREAM FORWARD ===============================
 * Downstream Forward-state (SUBSCRIBE_UPDATE §10.2.12): Forward=0 PAUSES
 * object delivery for one subscription without terminating it; Forward=1
 * resumes. A paused sub's next write must not draw MOQ_ERR_WRONG_STATE, which
 * the bind would map to STREAM_ERROR and the core would retire the sub. These
 * scenarios drive real SUBSCRIBE_UPDATE bytes over SimPair and assert the
 * subscription survives, emits nothing while paused, and resumes exact-once.
 * Backlog policy (stated, not spec-required): an object published DURING the
 * pause is retained and delivered on resume (the cursor never advances past
 * it); a FRESH post-resume object is the unambiguous exact-once anchor.
 * Env-gated disable hooks let each moving part be switched off on its own to
 * show that it has a dedicated failure. */

static const moq_bytes_t FWD_NS[2] = { { (const uint8_t *)"live", 4 },
                                       { (const uint8_t *)"cam1", 4 } };

static void
fwd_announce(rig_t *r, conn_t *pub, peer_state_t *pub_ps)
{
    moq_publish_namespace_cfg_t pcfg;
    memset(&pcfg, 0, sizeof(pcfg));
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace =
        (moq_namespace_t){ .parts = (moq_bytes_t *)FWD_NS, .count = 2 };
    moq_announcement_t ann;
    R_CHECK(r, moq_session_publish_namespace(pub->peer, &pcfg, r->now,
                                             &ann) == MOQ_OK);
    relay_bind_test_rig_pump(r, 4);
    (void)pub_ps;
}

/* Subscribe `sub` to live/cam1 video; optionally set the initial Forward bit.
 * Accepts the relay's upstream SUBSCRIBE at the publisher (largest {0,0}).
 * Returns the client subscription handle for later updates. */
static void
fwd_subscribe_named(rig_t *r, conn_t *pub, peer_state_t *pub_ps, conn_t *sub,
                    peer_state_t *sub_ps, moq_subscription_t *sh,
                    moq_bytes_t name, bool set_forward, bool forward)
{
    moq_subscribe_cfg_t scfg;
    memset(&scfg, 0, sizeof(scfg));
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace =
        (moq_namespace_t){ .parts = (moq_bytes_t *)FWD_NS, .count = 2 };
    scfg.track_name = name;
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    scfg.start_group = 0;
    scfg.start_object = 0;
    scfg.has_forward = set_forward;
    scfg.forward = forward;
    R_CHECK(r, moq_session_subscribe(sub->peer, &scfg, r->now, sh) == MOQ_OK);
    relay_bind_test_rig_pump(r, 4);
    relay_bind_test_peer_drain(r, pub, pub_ps, true, true, 0, 0); /* accept upstream */
    relay_bind_test_rig_pump(r, 4);
    relay_bind_test_peer_drain(r, sub, sub_ps, false, false, 0, 0);
    R_CHECK(r, sub_ps->subscribe_ok);
}

/* Video-track shorthand for the single-subscription oracle. */
static void
fwd_subscribe(rig_t *r, conn_t *pub, peer_state_t *pub_ps, conn_t *sub,
              peer_state_t *sub_ps, moq_subscription_t *sh, bool set_forward,
              bool forward)
{
    fwd_subscribe_named(r, pub, pub_ps, sub, sub_ps, sh, B("video"),
                        set_forward, forward);
}

/* Publish one whole object (its own subgroup) on the relay's upstream sub. */
static void
fwd_pub_whole(rig_t *r, ca_t *a, conn_t *pub, peer_state_t *pub_ps,
              uint64_t group, uint8_t byte)
{
    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = group;
    sgc.subgroup_id = 0;
    sgc.publisher_priority = 100;
    moq_subgroup_handle_t sgh;
    R_CHECK(r, moq_session_open_subgroup(pub->peer, pub_ps->up_sub, &sgc,
                                         r->now, &sgh) == MOQ_OK);
    uint8_t body[32];
    memset(body, byte, sizeof(body));
    moq_rcbuf_t *pl = NULL;
    R_CHECK(r, moq_rcbuf_create(&a->vt, body, sizeof(body), &pl) == MOQ_OK);
    R_CHECK(r, moq_session_write_object(pub->peer, sgh, 0, pl, r->now) ==
                   MOQ_OK);
    moq_rcbuf_decref(pl);
    R_CHECK(r, moq_session_close_subgroup(pub->peer, sgh, r->now) == MOQ_OK);
    relay_bind_test_rig_pump(r, 6);
}

static void
fwd_update_forward(rig_t *r, conn_t *sub, moq_subscription_t sh, bool forward)
{
    moq_subscription_update_cfg_t ucfg;
    moq_subscription_update_cfg_init(&ucfg);
    ucfg.has_forward = true;
    ucfg.forward = forward;
    R_CHECK(r, moq_session_update_subscription(sub->peer, sh, &ucfg,
                                               r->now) == MOQ_OK);
    relay_bind_test_rig_pump(r, 6);
}

/* A SUBSCRIBE asking for Forward State 0 that the authorization hook DEFERs
 * resumes paused once allowed: nothing is forwarded while it is paused, and
 * an update turning Forward on delivers what arrived meanwhile, once. */
static int
auth_defer_forward_zero(moq_version_t version)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    relay_bind_test_defer_action = MOQR_AUTH_SUBSCRIBE;
    relay_bind_test_authorize = relay_bind_test_defer_hook;
    relay_bind_test_authorize_ctx = NULL;
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        relay_bind_test_authorize = NULL;
        relay_bind_test_defer_action = 0xFFFFFFFFu;
        printf("FAIL: defer forward rig create (v%d)\n", (int)version);
        return 1;
    }
    conn_t *pub = relay_bind_test_rig_connect(&rig, version);
    conn_t *sub = relay_bind_test_rig_connect(&rig, version);
    R_CHECK(&rig, pub && sub);
    peer_state_t pub_ps, sps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&sps, 0, sizeof(sps));
    relay_bind_test_rig_pump(&rig, 4);
    fwd_announce(&rig, pub, &pub_ps);

    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace =
        (moq_namespace_t){ .parts = (moq_bytes_t *)FWD_NS, .count = 2 };
    scfg.track_name = B("video");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    scfg.has_forward = true;
    scfg.forward = false;
    moq_subscription_t sh;
    R_CHECK(&rig, moq_session_subscribe(sub->peer, &scfg, rig.now, &sh) ==
                      MOQ_OK);
    relay_bind_test_defer_last = 0;
    relay_bind_test_rig_pump(&rig, 6);
    relay_bind_test_peer_drain(&rig, sub, &sps, false, false, 0, 0);
    R_CHECK(&rig, !sps.subscribe_ok && sps.subscribe_errors == 0);   /* parked */
    uint64_t tkt = relay_bind_test_defer_last;
    R_CHECK(&rig, tkt != 0);
    R_CHECK(&rig, moqr_bind_auth_resolve(
                      rig.bind, tkt,
                      &(moqr_auth_verdict_t){ .decision = MOQR_AUTH_ALLOW },
                      rig.now) == MOQR_OK);
    relay_bind_test_rig_pump(&rig, 4);
    relay_bind_test_peer_drain(&rig, pub, &pub_ps, true, true, 0, 0); /* accept upstream */
    relay_bind_test_rig_pump(&rig, 4);
    relay_bind_test_peer_drain(&rig, sub, &sps, false, false, 0, 0);
    R_CHECK(&rig, sps.subscribe_ok);

    fwd_pub_whole(&rig, &a, pub, &pub_ps, 0, 0xB0);   /* data while paused */
    relay_bind_test_peer_drain(&rig, sub, &sps, false, false, 0, 0);
    R_CHECK(&rig, sps.objects == 0);
    R_CHECK(&rig, !sps.done_seen && !sps.session_closed);
    fwd_update_forward(&rig, sub, sh, true);
    relay_bind_test_peer_drain(&rig, sub, &sps, false, false, 0, 0);
    R_CHECK(&rig, sps.objects == 1 && sps.last_byte == 0xB0);
    R_CHECK(&rig, !sps.done_seen && !sps.session_closed);

    relay_bind_test_rig_destroy(&rig);
    relay_bind_test_authorize = NULL;
    relay_bind_test_authorize_ctx = NULL;
    relay_bind_test_defer_action = 0xFFFFFFFFu;
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: auth_defer_forward_zero (v%d)\n", (int)version);
    }
    return f;
}

/* (1) Initial Forward=0: established, no emission, no retirement; Forward=1
 *     resumes. (2) 1->0->1: pause with data pending, nothing emitted while
 *     paused, sub+conn live, backlog + a fresh object resume exact-once.
 *     (5) idempotence: 0->0, 1->1, and an omit-FORWARD update are no-ops. */
static int
forward_pause_resume_oracle(moq_version_t version)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: fwd rig create (v%d)\n", (int)version);
        return 1;
    }
    int failures = 0;
    conn_t *pub = relay_bind_test_rig_connect(&rig, version);
    conn_t *sub = relay_bind_test_rig_connect(&rig, version);
    R_CHECK(&rig, pub && sub);
    peer_state_t pub_ps, sps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&sps, 0, sizeof(sps));
    relay_bind_test_rig_pump(&rig, 4);
    fwd_announce(&rig, pub, &pub_ps);

    /* --- initial Forward=0: established but paused --- */
    (void)failures;
    moq_subscription_t sh;
    fwd_subscribe(&rig, pub, &pub_ps, sub, &sps, &sh, true, false);
    fwd_pub_whole(&rig, &a, pub, &pub_ps, 0, 0xA0);   /* data while paused */
    relay_bind_test_peer_drain(&rig, sub, &sps, false, false, 0, 0);
    R_CHECK(&rig, sps.objects == 0);       /* no emission while paused */
    R_CHECK(&rig, !sps.done_seen);         /* NOT retired */
    R_CHECK(&rig, !sps.session_closed);    /* NOT disconnected */
    /* resume -> the backlog object delivers exactly once */
    fwd_update_forward(&rig, sub, sh, true);
    relay_bind_test_peer_drain(&rig, sub, &sps, false, false, 0, 0);
    R_CHECK(&rig, sps.objects == 1 && sps.last_byte == 0xA0);
    R_CHECK(&rig, !sps.done_seen);
    /* idempotent 1->1 and omit-FORWARD: no effect, no retire, no dup */
    fwd_update_forward(&rig, sub, sh, true);   /* 1->1 */
    {
        moq_subscription_update_cfg_t ucfg;
        moq_subscription_update_cfg_init(&ucfg);
        ucfg.has_subscriber_priority = true;   /* update WITHOUT forward */
        ucfg.subscriber_priority = 7;
        R_CHECK(&rig, moq_session_update_subscription(sub->peer, sh, &ucfg,
                                                      rig.now) == MOQ_OK);
        relay_bind_test_rig_pump(&rig, 4);
    }
    relay_bind_test_peer_drain(&rig, sub, &sps, false, false, 0, 0);
    R_CHECK(&rig, sps.objects == 1 && !sps.done_seen);

    /* --- 1->0->1 on the live subscription --- */
    fwd_pub_whole(&rig, &a, pub, &pub_ps, 1, 0xB1);   /* delivered live */
    relay_bind_test_peer_drain(&rig, sub, &sps, false, false, 0, 0);
    R_CHECK(&rig, sps.objects == 2 && sps.last_byte == 0xB1);
    fwd_update_forward(&rig, sub, sh, false);   /* PAUSE */
    fwd_update_forward(&rig, sub, sh, false);   /* 0->0 idempotent */
    /* several earned pumps with data pending: nothing emitted, still live */
    fwd_pub_whole(&rig, &a, pub, &pub_ps, 2, 0xC2);   /* during pause */
    fwd_pub_whole(&rig, &a, pub, &pub_ps, 3, 0xC3);
    relay_bind_test_peer_drain(&rig, sub, &sps, false, false, 0, 0);
    R_CHECK(&rig, sps.objects == 2);        /* frozen while paused */
    R_CHECK(&rig, !sps.done_seen && !sps.session_closed);
    /* RESUME: backlog (g2,g3) replays, then a fresh object -> exact once */
    fwd_update_forward(&rig, sub, sh, true);
    relay_bind_test_peer_drain(&rig, sub, &sps, false, false, 0, 0);
    R_CHECK(&rig, sps.objects == 4);        /* 2 backlog delivered once each */
    fwd_pub_whole(&rig, &a, pub, &pub_ps, 4, 0xD4);   /* fresh post-resume */
    relay_bind_test_peer_drain(&rig, sub, &sps, false, false, 0, 0);
    R_CHECK(&rig, sps.objects == 5 && sps.last_byte == 0xD4);   /* exact-once */
    R_CHECK(&rig, !sps.done_seen);

    R_CHECK(&rig, moq_session_unsubscribe(sub->peer, sh, rig.now) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 4);
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (rig.failures == 0) {
        printf("PASS: forward_pause_resume_oracle v%d\n", (int)version);
    }
    return rig.failures;
}

/* (3) Sibling isolation: two subs on one binding; pausing one must not stop
 *     the other from delivering, and neither is retired. */
static int
forward_sibling_oracle(moq_version_t version)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: fwd sibling rig create (v%d)\n", (int)version);
        return 1;
    }
    conn_t *pub = relay_bind_test_rig_connect(&rig, version);
    conn_t *s = relay_bind_test_rig_connect(&rig, version); /* ONE downstream session/binding */
    R_CHECK(&rig, pub && s);
    /* Two upstream subs (one per track) need distinct pub-side state to hold
     * each track's relay subscription handle. */
    peer_state_t pub_v, pub_au, sps;
    memset(&pub_v, 0, sizeof(pub_v));
    memset(&pub_au, 0, sizeof(pub_au));
    memset(&sps, 0, sizeof(sps));
    relay_bind_test_rig_pump(&rig, 4);
    fwd_announce(&rig, pub, &pub_v);
    /* Two subscriptions to DIFFERENT tracks on the SAME session => two subs on
     * one relay binding, so pausing one exercises binding-wide HOL. */
    moq_subscription_t hv, hau;
    fwd_subscribe_named(&rig, pub, &pub_v, s, &sps, &hv, B("video"), false, true);
    fwd_subscribe_named(&rig, pub, &pub_au, s, &sps, &hau, B("audio"), false,
                        true);
    /* both tracks deliver a first object over the shared session */
    fwd_pub_whole(&rig, &a, pub, &pub_v, 0, 0x50);    /* video */
    fwd_pub_whole(&rig, &a, pub, &pub_au, 0, 0xA0);   /* audio */
    relay_bind_test_peer_drain(&rig, s, &sps, false, false, 0, 0);
    R_CHECK(&rig, sps.objects == 2);
    /* pause the video sub; publish on BOTH tracks. The audio sibling must keep
     * flowing through the shared binding; video is frozen; neither retired. */
    fwd_update_forward(&rig, s, hv, false);
    fwd_pub_whole(&rig, &a, pub, &pub_v, 1, 0x51);    /* video, during pause */
    fwd_pub_whole(&rig, &a, pub, &pub_au, 1, 0xA1);   /* audio sibling */
    relay_bind_test_peer_drain(&rig, s, &sps, false, false, 0, 0);
    R_CHECK(&rig, sps.objects == 3 && sps.last_byte == 0xA1);   /* audio flowed */
    R_CHECK(&rig, !sps.done_seen && !sps.session_closed);
    /* resume video: it catches up to its paused object exactly once */
    fwd_update_forward(&rig, s, hv, true);
    relay_bind_test_peer_drain(&rig, s, &sps, false, false, 0, 0);
    R_CHECK(&rig, sps.objects == 4 && sps.last_byte == 0x51 && !sps.done_seen);
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (rig.failures == 0) {
        printf("PASS: forward_sibling_oracle v%d\n", (int)version);
    }
    return rig.failures;
}

/* Cross-layer check that makes the bind's begun-checkpoint call load-bearing, end
 * to end over SimPair through the real bind. A multi-chunk object is delivered
 * downstream under an action-constrained relay session (max_actions=2), so the
 * relay writes begin + some chunks and then WOULD_BLOCKs — and the bind drives
 * moqr_core_delivery_note_emitted, advancing the core's note_emitted_total.
 *
 * LOAD-BEARING assertion: note_emitted_total must advance. Without the bind's
 * production note_emitted call it stays flat and this test fails. (NB: SimPair
 * cannot make a *behavioral* failure here — the bind writes
 * chunks ahead into the session and reaches STALLED before a Forward=0 pause can
 * propagate, and STALLED marks begun independently, so the downstream RESET
 * survives deleting the call. The note_emitted_total counter is therefore the
 * load-bearing signal; the RESET below is integration coverage.)
 *
 * The object is left OPEN and then abandoned (reset_subgroup) while paused: the
 * paused subscriber still observes a downstream RESET of the begun-but-unfinished
 * object and stays live. */
static int
forward_begin_zero_reset(moq_version_t version)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    relay_bind_test_server_streaming_objects = true;
    relay_bind_test_client_streaming_objects = true;
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        relay_bind_test_server_streaming_objects = false;
        relay_bind_test_client_streaming_objects = false;
        printf("FAIL: fwd begin-zero rig create (v%d)\n", (int)version);
        return 1;
    }
    conn_t *pub = relay_bind_test_rig_connect(&rig, version);
    relay_bind_test_max_actions = 2; /* constrain ONLY the downstream (sub) session */
    conn_t *sub = relay_bind_test_rig_connect(&rig, version);
    relay_bind_test_max_actions = 0;
    relay_bind_test_server_streaming_objects = false;
    relay_bind_test_client_streaming_objects = false;
    R_CHECK(&rig, pub && sub);
    relay_bind_test_rig_pump(&rig, 8);

    moq_bytes_t nsp[1] = { B("demo") };
    moq_publish_namespace_cfg_t pcfg;
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 1 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg, rig.now,
                                                &ann) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 8);

    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 1 };
    scfg.track_name = B("v");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    scfg.start_group = 0;
    scfg.start_object = 0;
    moq_subscription_t s1;
    R_CHECK(&rig,
            moq_session_subscribe(sub->peer, &scfg, rig.now, &s1) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 8);
    peer_state_t pub_ps, sub_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&sub_ps, 0, sizeof(sub_ps));
    relay_bind_test_peer_drain(&rig, pub, &pub_ps, true, false, 0, 0);
    R_CHECK(&rig, pub_ps.up_seen);
    relay_bind_test_rig_pump(&rig, 8);

    /* Group 1: an OPEN object whose 40 chunks are all available (the publisher
     * is not action-limited, so they land at the relay); the relay delivers it
     * downstream across several WOULD_BLOCK/resume cycles because the sub session
     * is action-limited. */
    moq_subgroup_cfg_t sca;
    moq_subgroup_cfg_init(&sca);
    sca.group_id = 1;
    sca.subgroup_id = 0;
    moq_subgroup_handle_t sga;
    R_CHECK(&rig, moq_session_open_subgroup(pub->peer, pub_ps.up_sub, &sca,
                                            rig.now, &sga) == MOQ_OK);
    R_CHECK(&rig, moq_session_begin_object(pub->peer, sga, 0, 1312, rig.now) ==
                      MOQ_OK);
    /* 40 chunks of the object's 41 are AVAILABLE and the object is left OPEN
     * (its last 32 bytes are never written): so the relay WOULD_BLOCKs per
     * cycle as it delivers (recording begun via note_emitted) and the object
     * never completes, at the relay or downstream. */
    for (int ci = 0; ci < 40; ci++) {
        uint8_t body[32];
        memset(body, 0xC5, sizeof(body));
        moq_rcbuf_t *pl = NULL;
        R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                          MOQ_OK);
        R_CHECK(&rig, moq_session_write_object_data(pub->peer, sga, pl,
                                                    rig.now) == MOQ_OK);
        moq_rcbuf_decref(pl);
    }

    moqr_core_stats_t st0;
    moqr_core_get_stats(rig.core, &st0);

    /* A couple cycles: the relay begins the object downstream and delivers some
     * chunks, WOULD_BLOCKing on the rest — begun downstream, not complete. */
    relay_bind_test_rig_pump(&rig, 2);
    relay_bind_test_peer_drain(&rig, sub, &sub_ps, false, false, 0, 0);
    R_CHECK(&rig, !sub_ps.obj_complete && !sub_ps.saw_reset);
    /* Load-bearing cross-layer assertion: the bind hit a downstream WOULD_BLOCK
     * and drove moqr_core_delivery_note_emitted, so the CORE's begun-checkpoint
     * counter advanced. Deleting the bind's note_emitted call leaves this at
     * zero (the counter is incremented only inside the core function). */
    moqr_core_stats_t st1;
    moqr_core_get_stats(rig.core, &st1);
    R_CHECK(&rig, st1.note_emitted_total > st0.note_emitted_total);

    /* PAUSE releases the held delivery while the source object remains open,
     * leaving it begun but unfinished downstream (incomplete, and reset below). */
    {
        moq_subscription_update_cfg_t ucfg;
        moq_subscription_update_cfg_init(&ucfg);
        ucfg.has_forward = true;
        ucfg.forward = false;
        R_CHECK(&rig, moq_session_update_subscription(sub->peer, s1, &ucfg,
                                                      rig.now) == MOQ_OK);
    }
    relay_bind_test_rig_pump(&rig, 30); /* let the pause settle; delivery freezes partway */
    relay_bind_test_peer_drain(&rig, sub, &sub_ps, false, false, 0, 0);
    R_CHECK(&rig, !sub_ps.obj_complete && !sub_ps.saw_reset);   /* still open */

    /* ABANDON the begun-but-unfinished object upstream (one action — no fresh
     * subgroups, so the publisher never overflows). */
    R_CHECK(&rig, moq_session_reset_subgroup(pub->peer, sga, 0x99, rig.now) ==
                      MOQ_OK);
    relay_bind_test_rig_pump(&rig, 20);
    relay_bind_test_peer_drain(&rig, sub, &sub_ps, false, false, 0, 0);
    /* The begun-but-unfinished downstream object is RESET, never silently left
     * hanging, and it never completed. */
    R_CHECK(&rig, sub_ps.saw_reset);
    R_CHECK(&rig, sub_ps.last_reset_code == 0x99);
    R_CHECK(&rig, !sub_ps.obj_complete);
    R_CHECK(&rig, !sub_ps.done_seen && !sub_ps.session_closed);   /* sub live */

    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (rig.failures == 0) {
        printf("PASS: forward_begin_zero_reset v%d\n", (int)version);
    }
    return rig.failures;
}

/* Object Properties across drafts. A Key-Value-Pair's Delta Type, Length
 * and even-type value are integers in the draft's own encoding: draft-16
 * (i), the QUIC variable-length integer; draft-18 (vi64) (draft-16 1.4.2,
 * draft-18 1.4.1/1.4.3). Values below 64 encode alike, so a value of 1000 is
 * used: 0x43E8 on draft-16, 0x83E8 on draft-18. The subscriber receives the
 * publisher's property -- type 0x10, value 1000 -- in its own draft's
 * encoding, and the Track stays well-formed. */
static int
dataplane_mixed_draft_properties(moq_version_t pub_ver, moq_version_t sub_ver)
{
    static const uint8_t d16_props[3] = { 0x10, 0x43, 0xE8 };
    static const uint8_t d18_props[3] = { 0x10, 0x83, 0xE8 };
    const uint8_t *sent = pub_ver == MOQ_VERSION_DRAFT_16 ? d16_props : d18_props;
    const uint8_t *want = sub_ver == MOQ_VERSION_DRAFT_16 ? d16_props : d18_props;
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: dataplane_mixed_draft_properties rig create\n");
        return 1;
    }
    conn_t *pub = relay_bind_test_rig_connect(&rig, pub_ver);
    conn_t *sub = relay_bind_test_rig_connect(&rig, sub_ver);
    R_CHECK(&rig, pub && sub);
    relay_bind_test_rig_pump(&rig, 4);
    moq_bytes_t nsp[2] = { B("live"), B("cam") };
    moq_publish_namespace_cfg_t pcfg;
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg, rig.now, &ann) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 4);
    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    scfg.track_name = B("v");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    moq_subscription_t sh;
    R_CHECK(&rig, moq_session_subscribe(sub->peer, &scfg, rig.now, &sh) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 6);
    peer_state_t pub_ps, s_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&s_ps, 0, sizeof(s_ps));
    relay_bind_test_peer_drain(&rig, pub, &pub_ps, true, false, 0, 0);
    R_CHECK(&rig, pub_ps.up_seen);
    relay_bind_test_rig_pump(&rig, 6);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);

    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.object_properties = true;
    moq_subgroup_handle_t sgh;
    R_CHECK(&rig, moq_session_open_subgroup(pub->peer, pub_ps.up_sub, &sgc, rig.now, &sgh) ==
                      MOQ_OK);
    uint8_t body[4] = { 0xA0, 0x0B, 0x1E, 0xC7 };
    moq_rcbuf_t *pl = NULL, *pr = NULL;
    R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) == MOQ_OK);
    R_CHECK(&rig, moq_rcbuf_create(&a.vt, sent, 3, &pr) == MOQ_OK);
    moq_object_cfg_t oc;
    moq_object_cfg_init(&oc);
    oc.object_id = 0;
    oc.payload = pl;
    oc.properties = pr;
    R_CHECK(&rig, moq_session_write_object_ex(pub->peer, sgh, &oc, rig.now) == MOQ_OK);
    moq_rcbuf_decref(pl);
    moq_rcbuf_decref(pr);
    R_CHECK(&rig, moq_session_close_subgroup(pub->peer, sgh, rig.now) == MOQ_OK);

    int objects = 0, exact = 0, unsubscribed = 0, done = 0;
    size_t got_len = 0;
    uint8_t got[8] = { 0 };
    for (int k = 0; k < 24; k++) {
        relay_bind_test_rig_pump(&rig, 1);
        moq_event_t ev;
        while (moq_session_poll_events(sub->peer, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) {
                const moq_object_received_event_t *o = &ev.u.object_received;
                objects++;
                got_len = o->properties != NULL ? moq_rcbuf_len(o->properties) : 0;
                if (got_len <= sizeof(got) && got_len != 0)
                    memcpy(got, moq_rcbuf_data(o->properties), got_len);
                exact += o->payload != NULL && moq_rcbuf_len(o->payload) == sizeof(body) &&
                         memcmp(moq_rcbuf_data(o->payload), body, sizeof(body)) == 0 &&
                         got_len == 3 && memcmp(got, want, 3) == 0;
            } else if (ev.kind == MOQ_EVENT_UNSUBSCRIBED) {
                unsubscribed++;
            } else if (ev.kind == MOQ_EVENT_SUBSCRIBE_DONE) {
                done++;
            }
            moq_event_cleanup(&ev);
        }
    }
    printf("mixed_draft_properties pub=%d sub=%d: objects=%d exact=%d props=%zu[%02x %02x %02x] "
           "unsubscribed=%d done=%d\n", (int)pub_ver, (int)sub_ver, objects, exact, got_len,
           got[0], got[1], got[2], unsubscribed, done);
    R_CHECK(&rig, objects == 1 && exact == 1 && unsubscribed == 0 && done == 0);
    R_CHECK(&rig, moq_session_state(sub->peer) == MOQ_SESS_ESTABLISHED);
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (rig.failures == 0) {
        printf("PASS: dataplane_mixed_draft_properties (pub=%d sub=%d)\n", (int)pub_ver,
               (int)sub_ver);
    }
    return rig.failures;
}

int
main(void)
{
    int failures = 0;
    failures += forward_pause_resume_oracle(MOQ_VERSION_DRAFT_16);
    failures += forward_pause_resume_oracle(MOQ_VERSION_DRAFT_18);
    failures += auth_defer_forward_zero(MOQ_VERSION_DRAFT_18);
    failures += auth_defer_forward_zero(MOQ_VERSION_DRAFT_22);
    failures += forward_sibling_oracle(MOQ_VERSION_DRAFT_16);
    failures += forward_sibling_oracle(MOQ_VERSION_DRAFT_18);
    failures += forward_begin_zero_reset(MOQ_VERSION_DRAFT_16);
    failures += forward_begin_zero_reset(MOQ_VERSION_DRAFT_18);
    failures += ready_idle_zero_probes();
    failures += ready_action_cap_park();
    failures += ready_blocked_aggregate();
    failures += ready_bind_sg_rearm();
    failures += ready_hot_not_starving();
    failures += ready_session_sg_poll();
    failures += ready_subdone_rearm();
    failures += ready_confirm_failure_closes();
    failures += ready_probe_error_policy();
    failures += ready_property_write_floor();
    failures += ready_withdraw_idle_wake();
    failures += ready_detach_replacement_clean();
    failures += dataplane_delivery_oracle(0);   /* clean               */
    failures += dataplane_delivery_oracle(3);   /* under backpressure  */
    failures += dataplane_datagram_status_oracle(0);
    failures += dataplane_datagram_status_oracle(3);
    failures += dataplane_retained_replay_oracle(0);
    failures += dataplane_retained_replay_oracle(3);
    failures += dataplane_late_fin_oracle(0);
    failures += dataplane_late_fin_oracle(3);
    failures += dataplane_fin_backpressure_oracle();
    failures += dataplane_eog_backpressure_oracle(false, MOQ_VERSION_DRAFT_18);
    failures += dataplane_eog_backpressure_oracle(true, MOQ_VERSION_DRAFT_18);
    failures += dataplane_eog_backpressure_oracle(false, MOQ_VERSION_DRAFT_16);
    failures += dataplane_eog_backpressure_oracle(true, MOQ_VERSION_DRAFT_16);
    {
        const moq_version_t fo_versions[] = { MOQ_VERSION_DRAFT_16,
                                              MOQ_VERSION_DRAFT_18,
                                              MOQ_VERSION_DRAFT_22 };
        for (size_t i = 0; i < sizeof(fo_versions) / sizeof(fo_versions[0]); i++) {
            failures += dataplane_first_object_forward(false, fo_versions[i]);
            failures += dataplane_first_object_forward(true, fo_versions[i]);
        }
    }
    failures += dataplane_eog_conflict_fail_closed();
    failures += dataplane_eviction_skip_oracle();
    failures += dataplane_source_end_oracle(false, 0);   /* pull            */
    failures += dataplane_source_end_oracle(false, 3);   /* pull + backpressure */
    failures += dataplane_source_end_oracle(true, 0);    /* push            */
    failures += dataplane_source_end_oracle(true, 3);    /* push + backpressure */
    for (int mode = SE_BURST; mode <= SE_UNSEALED_SESSION_RESET; mode++) {
        failures += dataplane_source_end_before_drain(false, 0, mode);
        failures += dataplane_source_end_before_drain(false, 3, mode);
        failures += dataplane_source_end_before_drain(true, 0, mode);
        failures += dataplane_source_end_before_drain(true, 3, mode);
    }
    failures += dataplane_push_survives_downstream_linger();
    failures += relay_bind_test_dataplane_mixed_draft_oracle(MOQ_VERSION_DRAFT_16,
                                                             MOQ_VERSION_DRAFT_18, 0);
    failures += relay_bind_test_dataplane_mixed_draft_oracle(MOQ_VERSION_DRAFT_18,
                                                             MOQ_VERSION_DRAFT_16, 0);
    failures += relay_bind_test_dataplane_mixed_draft_oracle(MOQ_VERSION_DRAFT_16,
                                                             MOQ_VERSION_DRAFT_18, 3);
    failures += relay_bind_test_dataplane_mixed_draft_oracle(MOQ_VERSION_DRAFT_18,
                                                             MOQ_VERSION_DRAFT_16, 3);
    failures += dataplane_mixed_draft_properties(MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_16);
    failures += dataplane_mixed_draft_properties(MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18);
    failures += dataplane_mixed_draft_properties(MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_18);
    failures += dataplane_mixed_draft_properties(MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_16);
    failures += chunk_ingest_flow();   /* chunked COMPLETE delivery + abandon-skip */
    failures += chunk_delivery_backpressure();   /* WOULD_BLOCK resume */
    failures += chunk_liveedge_case(false);   /* live edge + clean complete */
    failures += chunk_liveedge_case(true);    /* abandon → downstream reset */
    failures += chunk_liveedge_evict();       /* eviction-after-stall → reset */
    failures += sub_done_resets_begun_subgroup(); /* SUB_DONE mid-object → reset */
    return failures == 0 ? 0 : 1; /* exit status truncates to 8 bits */
}
