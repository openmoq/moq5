/*
 * Production-lifecycle proof for the pools' intrusive OCCUPANCY lists.
 *
 * Those lists are what every advancing-preamble scan walks, so "allocated iff
 * linked" is a real invariant with a real consequence: an owner the list cannot
 * reach is an owner no bounded scan can see.
 *
 * The PRODUCTION-ADOPTION cases -- every allocation and retirement site of all
 * five pools -- use no white-box arming at all, which is what makes them
 * witnesses for those sites. The sweep-cursor proofs at the end are
 * deliberately different: they are white-box SWEEP-POSITION discriminators,
 * built with sweep_arm.h so a bounded sweep can be parked exactly where the
 * cursor rule has to be observed, and they are not offered as production
 * adoption evidence.
 *
 * NOT installed; requires moq-core-test-internals.
 */
#include <moq/moq.h>
#include "test_support.h"
#include "test_session_support.h"
#include "../../core/src/session/session_internal.h"
#include "../support/occupancy_audit.h"
#include "../support/sweep_arm.h"
#include <stdio.h>
#include <string.h>

static int failures = 0;

/* Report and assert; `where` names the phase so a failure is attributable. */
static int occ_audit(const moq_session_t *s, const char *where)
{
    const char *pool = "";
    size_t slot = 0;
    occ_fault_t f = occ_check_all(s, &pool, &slot);
    if (f != OCC_OK) {
        printf("  OCCUPANCY FAULT (%s): pool=%s slot=%zu -- %s\n",
               where, pool, slot, occ_fault_name(f));
        MOQ_TEST_CHECK(f == OCC_OK);
    }
    return 0;
}

/* Membership of one slot, by walking the list rather than reading the flag:
 * the flag is what a broken link would keep telling us. */
static bool occ_pub_reachable(const moq_session_t *s, size_t slot)
{
    size_t steps = 0;
    for (int32_t i = s->pub_occ_head; i >= 0 && (size_t)i < s->pub_cap;
         i = s->publishes[i].occ_next)
        if ((size_t)i == slot || ++steps > s->pub_cap) return (size_t)i == slot;
    return false;
}

static bool occ_sub_reachable(const moq_session_t *s, size_t slot)
{
    size_t steps = 0;
    for (int32_t i = s->sub_occ_head; i >= 0 && (size_t)i < s->sub_cap;
         i = s->subs[i].occ_next)
        if ((size_t)i == slot || ++steps > s->sub_cap) return (size_t)i == slot;
    return false;
}

/* ================= auditor self-check ============================= */

/*
 * Hand-built corruptions on a real session. Each must be DETECTED and named,
 * and none may read out of range -- the checker range-checks every edge before
 * it subscripts, which is what keeps a corrupt list a report rather than a
 * crash under ASan/UBSan. Expected failures are quiet: the checker returns a
 * code and touches no test macro.
 */
static int test_auditor_selfcheck(void)
{
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    moq_session_cfg_t cfg = MOQ_SESSION_CFG_INIT;
    cfg.alloc = &alloc;
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    moq_session_t *s = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_create(&cfg, 0, &s), (int)MOQ_OK);
    if (!s) return failures;

    size_t slot = 0;
    /*
     * FAIL CLOSED ON NULL, before anything else: a runner may audit a session
     * whose creation failed, and an oracle advertised as bounds-safe must
     * report that rather than dereference it. Proven for a per-pool checker and
     * for the aggregate, both with and without out-parameters.
     */
    MOQ_TEST_CHECK_EQ_INT((int)occ_check_pub(NULL, &slot),
                          (int)OCC_NULL_SESSION);
    MOQ_TEST_CHECK_EQ_INT((int)occ_check_pub(NULL, NULL),
                          (int)OCC_NULL_SESSION);
    MOQ_TEST_CHECK_EQ_INT((int)occ_check_rx(NULL, &slot),
                          (int)OCC_NULL_SESSION);
    {
        const char *pool = "x";
        size_t nslot = 0;
        MOQ_TEST_CHECK_EQ_INT((int)occ_check_all(NULL, &pool, &nslot),
                              (int)OCC_NULL_SESSION);
        MOQ_TEST_CHECK_EQ_INT((int)occ_check_all(NULL, NULL, NULL),
                              (int)OCC_NULL_SESSION);
    }

    /* An empty pool is clean. */
    MOQ_TEST_CHECK_EQ_INT((int)occ_check_pub(s, &slot), (int)OCC_OK);

    struct { const char *name; occ_fault_t want; } probes[] = {
        { "head lost",           OCC_COUNT_MISMATCH },
        { "allocated unlinked",  OCC_ALLOC_UNLINKED },
        { "free linked",         OCC_FREE_LINKED },
        { "unreachable linked",  OCC_COUNT_MISMATCH },
        { "wrong prev",          OCC_NO_RECIPROCITY },
        { "wrong next order",    OCC_NOT_ASCENDING },
        { "out-of-range edge",   OCC_BAD_NEXT },
        { "self-loop cycle",     OCC_NO_RECIPROCITY },
        { "head out of range",   OCC_BAD_HEAD },
        { "free residue",        OCC_FREE_RESIDUE },
    };
    size_t n = sizeof(probes) / sizeof(probes[0]);

    for (size_t p = 0; p < n; p++) {
        /* Rebuild a clean two-member list each time. */
        for (size_t i = 0; i < s->pub_cap; i++) {
            s->publishes[i].state = MOQ_PUB_FREE;
            s->publishes[i].occ_next = -1;
            s->publishes[i].occ_prev = -1;
            s->publishes[i].occ_linked = false;
        }
        s->pub_occ_head = -1;
        s->publishes[0].state = MOQ_PUB_ESTABLISHED;
        pub_occ_link(s, 0);
        s->publishes[2].state = MOQ_PUB_ESTABLISHED;
        pub_occ_link(s, 2);
        MOQ_TEST_CHECK_EQ_INT((int)occ_check_pub(s, &slot), (int)OCC_OK);

        switch (p) {
        case 0: s->pub_occ_head = -1; break;                  /* head lost */
        case 1: s->publishes[1].state = MOQ_PUB_ESTABLISHED; break;
        case 2: s->publishes[1].occ_linked = true; break;
        case 3: s->publishes[3].state = MOQ_PUB_ESTABLISHED;  /* linked but */
                s->publishes[3].occ_linked = true; break;     /* unreachable */
        case 4: s->publishes[2].occ_prev = 1; break;
        case 5: s->publishes[0].occ_next = 0;                 /* descending */
                s->publishes[0].occ_prev = -1;
                s->pub_occ_head = 2;
                s->publishes[2].occ_prev = -1;
                s->publishes[2].occ_next = 0;
                s->publishes[0].occ_prev = 2; break;
        case 6: s->publishes[0].occ_next = (int32_t)s->pub_cap; break;
        case 7: s->publishes[2].occ_next = 2; break;          /* 2 -> 2 */
        case 8: s->pub_occ_head = (int32_t)s->pub_cap; break;
        case 9: s->publishes[1].occ_next = 5; break;          /* free residue */
        }

        occ_fault_t got = occ_check_pub(s, &slot);
        if (got != probes[p].want) {
            printf("  AUDITOR SELF-CHECK FAILURE (%s): expected '%s', got '%s'\n",
                   probes[p].name, occ_fault_name(probes[p].want),
                   occ_fault_name(got));
            MOQ_TEST_CHECK(got == probes[p].want);
        }
    }
    printf("AUDITOR SELF-CHECK: %zu corruptions, each detected and named\n", n);
    /*
     * OCC_TOO_LONG is the walk's UNCONDITIONAL step bound rather than a
     * separately reachable verdict: strict ascending order already forbids
     * revisiting a slot, so every cycle or duplicate trips reciprocity or
     * ordering first. The bound is what guarantees the auditor terminates on
     * an arbitrarily corrupt list instead of hanging, which is why it stays.
     */

    /* Leave the pool clean so destroy does not trip over the probes. */
    for (size_t i = 0; i < s->pub_cap; i++) {
        s->publishes[i].state = MOQ_PUB_FREE;
        s->publishes[i].occ_next = -1;
        s->publishes[i].occ_prev = -1;
        s->publishes[i].occ_linked = false;
    }
    s->pub_occ_head = -1;
    moq_session_destroy(s);
    return failures;
}

/* ================= local publish lifecycle ======================== */

/*
 * The LOCAL allocation path: moq_session_publish() takes a slot of its own.
 *
 * The rule is the container's, not any one scan's: EVERY allocated owner must
 * be linked, so that every present and future bounded scan sees the declared
 * live set. An owner whose commit does not link it is invisible to all of them
 * for its whole life. What this test proves is exactly that -- production
 * adoption at the allocation commit, and correct retirement and slot reuse --
 * not that some particular sweep would have finalized this publisher.
 */
static int test_local_publish_occupancy(void)
{
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    moq_session_t *c = NULL, *sv = NULL;
    establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);
    MOQ_TEST_CHECK(c != NULL && sv != NULL);
    if (!c || !sv) return failures;
    occ_audit(c, "publish.established");

    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_namespace_t ns = { parts, 1 };
    moq_publish_cfg_t pcfg;
    moq_publish_cfg_init(&pcfg);
    pcfg.track_namespace = ns;
    pcfg.track_name = MOQ_BYTES_LITERAL("video");

    moq_publication_t h;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_publish(c, &pcfg, 1000, &h),
                          (int)MOQ_OK);
    MOQ_TEST_CHECK(moq_publication_is_valid(h));
    size_t slot = moq_handle_slot(h._opaque);
    MOQ_TEST_CHECK(slot < c->pub_cap);
    if (slot >= c->pub_cap) { moq_session_destroy(c); moq_session_destroy(sv);
                              return failures; }

    /* The allocation itself must have joined the list. */
    MOQ_TEST_CHECK_EQ_INT((int)c->publishes[slot].state,
                          (int)MOQ_PUB_PENDING_PUBLISHER);
    MOQ_TEST_CHECK(occ_pub_reachable(c, slot));
    occ_audit(c, "publish.pending_publisher");

    /* Acceptance changes state; it must not change membership. */
    pump_actions_to_peer(c, sv, 1000);
    moq_event_t ev;
    MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), (size_t)1);
    MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_PUBLISH_REQUEST);
    moq_accept_publish_cfg_t acfg;
    moq_accept_publish_cfg_init(&acfg);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_publish(
        sv, ev.u.publish_request.pub, &acfg, 2000), (int)MOQ_OK);
    moq_event_cleanup(&ev);
    occ_audit(sv, "publish.peer_accepted");
    pump_actions_to_peer(sv, c, 2000);
    { moq_event_t e; while (moq_session_poll_events(c, &e, 1) == 1)
          moq_event_cleanup(&e); }
    MOQ_TEST_CHECK_EQ_INT((int)c->publishes[slot].state,
                          (int)MOQ_PUB_ESTABLISHED);
    MOQ_TEST_CHECK(occ_pub_reachable(c, slot));
    occ_audit(c, "publish.established_local");

    /* Retirement unlinks, and the freed slot carries the sentinel image. */
    moq_finish_publish_cfg_t fin;
    moq_finish_publish_cfg_init(&fin);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_finish_publish(c, h, &fin, 3000),
                          (int)MOQ_OK);
    { moq_action_t a; while (moq_session_poll_actions(c, &a, 1) == 1)
          moq_action_cleanup(&a); }
    MOQ_TEST_CHECK_EQ_INT((int)c->publishes[slot].state, (int)MOQ_PUB_FREE);
    MOQ_TEST_CHECK(!occ_pub_reachable(c, slot));
    MOQ_TEST_CHECK(!c->publishes[slot].occ_linked);
    occ_audit(c, "publish.retired");

    /* Slot reuse re-links exactly once. */
    moq_publication_t h2;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_publish(c, &pcfg, 4000, &h2),
                          (int)MOQ_OK);
    MOQ_TEST_CHECK(occ_pub_reachable(c, moq_handle_slot(h2._opaque)));
    occ_audit(c, "publish.reused");

    moq_session_destroy(c);
    moq_session_destroy(sv);
    MOQ_TEST_CHECK_EQ_INT(as.balance, 0);
    return failures;
}

/*
 * The PEER-origin path, audited while the owner is still PENDING_SUBSCRIBER --
 * before acceptance can mask a missing creation link.
 */
static int test_peer_publish_occupancy(void)
{
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    moq_session_t *c = NULL, *sv = NULL;
    establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);
    MOQ_TEST_CHECK(c != NULL && sv != NULL);
    if (!c || !sv) return failures;

    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_namespace_t ns = { parts, 1 };
    moq_publish_cfg_t pcfg;
    moq_publish_cfg_init(&pcfg);
    pcfg.track_namespace = ns;
    pcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_publication_t h;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_publish(c, &pcfg, 1000, &h),
                          (int)MOQ_OK);
    pump_actions_to_peer(c, sv, 1000);

    moq_event_t ev;
    MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), (size_t)1);
    MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_PUBLISH_REQUEST);
    size_t pslot = moq_handle_slot(ev.u.publish_request.pub._opaque);
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK(pslot < sv->pub_cap);
    if (pslot >= sv->pub_cap) { moq_session_destroy(c); moq_session_destroy(sv);
                                return failures; }

    /* STILL PENDING: the creation link is the only thing that can have put it
     * on the list, because nothing has accepted it yet. */
    MOQ_TEST_CHECK_EQ_INT((int)sv->publishes[pslot].state,
                          (int)MOQ_PUB_PENDING_SUBSCRIBER);
    MOQ_TEST_CHECK(occ_pub_reachable(sv, pslot));
    occ_audit(sv, "peer_publish.pending_subscriber");

    /* Rejecting it retires the slot without acceptance ever running. */
    moq_reject_publish_cfg_t rej;
    moq_reject_publish_cfg_init(&rej);
    rej.error_code = 0x1;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_reject_publish(
        sv, (moq_publication_t){ moq_handle_pack(MOQ_HANDLE_POOL_PUBLICATION,
              sv->session_tag, sv->publishes[pslot].generation,
              (uint32_t)pslot) }, &rej, 2000), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)sv->publishes[pslot].state, (int)MOQ_PUB_FREE);
    MOQ_TEST_CHECK(!occ_pub_reachable(sv, pslot));
    occ_audit(sv, "peer_publish.rejected");

    moq_session_destroy(c);
    moq_session_destroy(sv);
    MOQ_TEST_CHECK_EQ_INT(as.balance, 0);
    return failures;
}

/* ================= the other four pools, through production ======= */

static int test_other_pools_occupancy(void)
{
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    moq_session_t *c = NULL, *sv = NULL;
    establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);
    MOQ_TEST_CHECK(c != NULL && sv != NULL);
    if (!c || !sv) return failures;

    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_namespace_t ns = { parts, 1 };

    /* --- subscription: local allocation, peer allocation, retirement --- */
    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = ns;
    scfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_subscription_t sub;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscribe(c, &scfg, 1000, &sub),
                          (int)MOQ_OK);
    size_t cslot = moq_handle_slot(sub._opaque);
    MOQ_TEST_CHECK(cslot < c->sub_cap);
    MOQ_TEST_CHECK(occ_sub_reachable(c, cslot));
    occ_audit(c, "subscribe.local");

    pump_actions_to_peer(c, sv, 1000);
    moq_event_t ev;
    MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), (size_t)1);
    MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_SUBSCRIBE_REQUEST);
    moq_subscription_t peer_sub = ev.u.subscribe_request.sub;
    size_t sslot = moq_handle_slot(peer_sub._opaque);
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK(sslot < sv->sub_cap);
    MOQ_TEST_CHECK(occ_sub_reachable(sv, sslot));
    occ_audit(sv, "subscribe.peer");

    moq_accept_subscribe_cfg_t acfg;
    moq_accept_subscribe_cfg_init(&acfg);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_subscribe(sv, peer_sub, &acfg,
                                                            2000), (int)MOQ_OK);
    occ_audit(sv, "subscribe.accepted");
    pump_actions_to_peer(sv, c, 2000);
    { moq_event_t e; while (moq_session_poll_events(c, &e, 1) == 1)
          moq_event_cleanup(&e); }
    occ_audit(c, "subscribe.established");

    /* --- subgroup: a real open on the publisher side, then close+reap --- */
    moq_subgroup_cfg_t gcfg;
    moq_subgroup_cfg_init(&gcfg);
    gcfg.group_id = 7;
    gcfg.subgroup_id = 1;
    moq_subgroup_handle_t sg;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_subgroup(sv, peer_sub, &gcfg,
                                                         2100, &sg), (int)MOQ_OK);
    size_t gslot = moq_handle_slot(sg._opaque);
    MOQ_TEST_CHECK(gslot < sv->sg_cap);
    MOQ_TEST_CHECK(sv->subgroups[gslot].occ_linked);
    occ_audit(sv, "subgroup.open");

    MOQ_TEST_CHECK_EQ_INT((int)moq_session_close_subgroup(sv, sg, 2200),
                          (int)MOQ_OK);
    occ_audit(sv, "subgroup.closing");
    { moq_action_t a; while (moq_session_poll_actions(sv, &a, 1) == 1)
          moq_action_cleanup(&a); }
    moq_session_tick(sv, 2300);                 /* reaps the terminal subgroup */
    MOQ_TEST_CHECK_EQ_INT((int)sv->subgroups[gslot].state, (int)MOQ_SG_FREE);
    MOQ_TEST_CHECK(!sv->subgroups[gslot].occ_linked);
    occ_audit(sv, "subgroup.reaped");

    /* --- rx: a real inbound data stream on the subscriber side --- */
    {
        moq_action_t acts[8];
        size_t na = moq_session_poll_actions(sv, acts, 8);
        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
    }
    moq_subgroup_handle_t sg2;
    gcfg.group_id = 9;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_subgroup(sv, peer_sub, &gcfg,
                                                         2400, &sg2), (int)MOQ_OK);
    {
        moq_action_t acts[8];
        size_t na = moq_session_poll_actions(sv, acts, 8);
        for (size_t i = 0; i < na; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_DATA) {
                moq_stream_ref_t dref = moq_stream_ref_from_u64(
                    acts[i].u.send_data.stream_ref._v + 50000);
                moq_session_on_data_bytes(c, dref, acts[i].u.send_data.header,
                                          acts[i].u.send_data.header_len,
                                          false, 2500);
            }
            moq_action_cleanup(&acts[i]);
        }
    }
    MOQ_TEST_CHECK(c->rx_occ_head >= 0);        /* a real rx entry exists */
    occ_audit(c, "rx.header_received");

    /* --- fetch: a real local FETCH, then its cancellation --- */
    moq_fetch_cfg_t fcfg;
    moq_fetch_cfg_init(&fcfg);
    fcfg.track_namespace = ns;
    fcfg.track_name = MOQ_BYTES_LITERAL("video");
    fcfg.end_group = 4;
    moq_fetch_t fh;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(c, &fcfg, 2600, &fh),
                          (int)MOQ_OK);
    size_t fslot = moq_handle_slot(fh._opaque);
    MOQ_TEST_CHECK(fslot < c->fetch_cap);
    MOQ_TEST_CHECK(c->fetches[fslot].occ_linked);
    occ_audit(c, "fetch.pending");
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch_cancel(c, fh, 2700),
                          (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)c->fetches[fslot].state, (int)MOQ_FETCH_FREE);
    MOQ_TEST_CHECK(!c->fetches[fslot].occ_linked);
    occ_audit(c, "fetch.cancelled");

    /*
     * --- close ---
     * The close frees the SUBGROUP and RX pools (close_with_error's subgroup
     * invalidation and free_rx_stream_bufs); it does not free request owners,
     * which survive to destruction. So what is checked here is not "every pool
     * empties": it is that whatever the close freed left the lists consistent,
     * and that every owner still allocated retains valid topology.
     */
    moq_session_close(c, 0x0, NULL, 3000);
    occ_audit(c, "session.closed");

    moq_session_destroy(c);
    moq_session_destroy(sv);
    return failures;
}

/* ================= sweep-cursor conservation ====================== */

/*
 * The RX unlink advances a sweep cursor parked on the freed slot. That fixup
 * must be conditioned on an ACTIVE sweep actually parked there -- a retired
 * session's cursor is a plain reset 0, and freeing RX slot 0 outside any sweep
 * must not move it, or the retired-cursor image becomes a function of which
 * stream happened to be freed last.
 */
static int test_idle_rx_cursor_unmoved(void)
{
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    moq_session_t *c = NULL, *sv = NULL;
    establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);
    MOQ_TEST_CHECK(c != NULL && sv != NULL);
    if (!c || !sv) return failures;

    /* Retire any sweep so the cursor is at its reset value. */
    moq_session_tick(c, 1000);
    MOQ_TEST_CHECK(!c->sweep_active);
    MOQ_TEST_CHECK_EQ_SIZE(c->sweep_rx_pos, (size_t)0);

    /* A real inbound data stream, which takes RX slot 0 on a fresh pool. */
    /*
     * The LEADING BYTE of a real FETCH_HEADER, built by the session's own
     * profile encoder: the stream type is complete but the request id is not,
     * so the entry is created and parked in AWAITING_HEADER rather than being
     * classified and torn down. No magic constant is fed.
     */
    uint8_t hdr[32];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, hdr, sizeof(hdr));
    MOQ_TEST_CHECK_EQ_INT(
        (int)c->profile->encode_fetch_header(c, &w, 0x777ull), (int)MOQ_OK);
    MOQ_TEST_CHECK(moq_buf_writer_offset(&w) > 1);
    moq_stream_ref_t ref = moq_stream_ref_from_u64(0x4242ull);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_data_bytes(c, ref, hdr, 1, false, 1100),
        (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c),
                          (int)MOQ_SESS_ESTABLISHED);
    int32_t slot = moq_index_find(c->idx_rx_by_ref, c->idx_rx_mask, ref._v);
    MOQ_TEST_CHECK_EQ_INT((int)slot, 0);
    occ_audit(c, "idle_cursor.rx_created");

    /* Freeing it happens OUTSIDE any sweep. */
    MOQ_TEST_CHECK(!c->sweep_active);
    (void)moq_session_on_data_reset(c, ref, 0x0, 1200);
    MOQ_TEST_CHECK(!c->rx_streams[0].active);
    MOQ_TEST_CHECK(!c->rx_streams[0].occ_linked);
    /* THE POINT: the idle cursor is untouched. */
    MOQ_TEST_CHECK_EQ_SIZE(c->sweep_rx_pos, (size_t)0);
    occ_audit(c, "idle_cursor.rx_freed");

    moq_session_destroy(c);
    moq_session_destroy(sv);
    return failures;
}

/*
 * The other half, re-proved: a BOUNDED sweep parked on an RX entry that the
 * stop then frees must resume at the successor captured before the free, and
 * each bound stream must be stopped EXACTLY once across the suspension.
 */
static int test_parked_rx_cursor_advances(void)
{
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    moq_session_cfg_t cfg = MOQ_SESSION_CFG_INIT;
    cfg.alloc = &alloc;
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    moq_session_t *s = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_create(&cfg, 0, &s), (int)MOQ_OK);
    if (!s) return failures;

    /* One expired publication with TWO bound streams, at non-adjacent slots so
     * "advanced to the captured successor" is distinguishable from "+1". */
    s->publishes[0].state = MOQ_PUB_ESTABLISHED;
    pub_occ_link(s, 0);
    sweep_arm_expired_pub(s, 0);
    sweep_bind_rx(s, 1, s->publishes[0].handle);
    sweep_bind_rx(s, 4, s->publishes[0].handle);
    occ_audit(s, "parked.armed");

    /* Budget 1: the first STOP is charged, then the sweep suspends. */
    session_budget_enter(s, 1);
    moq_result_t rc = session_begin_advance_budgeted(s, 100);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_SESSION_SUSPENDED);
    session_budget_leave(s);
    /* Slot 1 was stopped and freed; the cursor sits on its captured successor. */
    MOQ_TEST_CHECK(!s->rx_streams[1].active);
    MOQ_TEST_CHECK(s->rx_streams[4].active);
    MOQ_TEST_CHECK_EQ_SIZE(s->sweep_rx_pos, (size_t)4);
    occ_audit(s, "parked.suspended");

    /* Continue unlimited: the second stream is stopped, and neither is stopped
     * twice -- exactly two STOP_DATA actions in total. */
    (void)moq_session_tick(s, 100);
    MOQ_TEST_CHECK(!s->rx_streams[4].active);
    occ_audit(s, "parked.completed");
    size_t stops = 0;
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) == 1) {
        if (a.kind == MOQ_ACTION_STOP_DATA) stops++;
        moq_action_cleanup(&a);
    }
    MOQ_TEST_CHECK_EQ_SIZE(stops, (size_t)2);

    moq_session_destroy(s);
    return failures;
}

/* ================= the three remaining production sites =========== */

/*
 * Three allocation sites are not reachable from the flows above, and each one
 * needs its own witness or its link is unproven:
 *
 *  - the PUBLICATION-side subgroup open (a different entry point from the
 *    subscription-side one);
 *  - the PEER-origin FETCH, which creates a PENDING_PUBLISHER entry;
 *  - the draft-18 request-STAGING slot, which exists only while a request bidi
 *    is still incomplete.
 */
static int test_pub_subgroup_occupancy(void)
{
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    moq_session_t *c = NULL, *sv = NULL;
    establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);
    MOQ_TEST_CHECK(c != NULL && sv != NULL);
    if (!c || !sv) return failures;

    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_namespace_t ns = { parts, 1 };
    moq_publish_cfg_t pcfg;
    moq_publish_cfg_init(&pcfg);
    pcfg.track_namespace = ns;
    pcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_publication_t h;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_publish(c, &pcfg, 1000, &h),
                          (int)MOQ_OK);
    pump_actions_to_peer(c, sv, 1000);
    moq_event_t ev;
    MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), (size_t)1);
    moq_accept_publish_cfg_t acfg;
    moq_accept_publish_cfg_init(&acfg);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_publish(
        sv, ev.u.publish_request.pub, &acfg, 2000), (int)MOQ_OK);
    moq_event_cleanup(&ev);
    pump_actions_to_peer(sv, c, 2000);
    { moq_event_t e; while (moq_session_poll_events(c, &e, 1) == 1)
          moq_event_cleanup(&e); }
    MOQ_TEST_CHECK_EQ_INT((int)c->publishes[moq_handle_slot(h._opaque)].state,
                          (int)MOQ_PUB_ESTABLISHED);

    /* THE SITE: a subgroup opened on a PUBLICATION, not on a subscription. */
    moq_subgroup_cfg_t gcfg;
    moq_subgroup_cfg_init(&gcfg);
    gcfg.group_id = 3;
    gcfg.subgroup_id = 0;
    moq_subgroup_handle_t sg;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_pub_subgroup(c, h, &gcfg, 2100,
                                                             &sg), (int)MOQ_OK);
    size_t gslot = moq_handle_slot(sg._opaque);
    MOQ_TEST_CHECK(gslot < c->sg_cap);
    MOQ_TEST_CHECK(c->subgroups[gslot].occ_linked);
    occ_audit(c, "pub_subgroup.open");

    MOQ_TEST_CHECK_EQ_INT((int)moq_session_close_subgroup(c, sg, 2200),
                          (int)MOQ_OK);
    { moq_action_t a; while (moq_session_poll_actions(c, &a, 1) == 1)
          moq_action_cleanup(&a); }
    moq_session_tick(c, 2300);
    MOQ_TEST_CHECK_EQ_INT((int)c->subgroups[gslot].state, (int)MOQ_SG_FREE);
    MOQ_TEST_CHECK(!c->subgroups[gslot].occ_linked);
    occ_audit(c, "pub_subgroup.reaped");

    moq_session_destroy(c);
    moq_session_destroy(sv);
    return failures;
}

static int test_peer_fetch_occupancy(void)
{
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    moq_session_t *c = NULL, *sv = NULL;
    establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);
    MOQ_TEST_CHECK(c != NULL && sv != NULL);
    if (!c || !sv) return failures;

    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_namespace_t ns = { parts, 1 };
    moq_fetch_cfg_t fcfg;
    moq_fetch_cfg_init(&fcfg);
    fcfg.track_namespace = ns;
    fcfg.track_name = MOQ_BYTES_LITERAL("video");
    fcfg.end_group = 4;
    moq_fetch_t fh;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(c, &fcfg, 1000, &fh),
                          (int)MOQ_OK);
    pump_actions_to_peer(c, sv, 1000);

    /* THE SITE: the peer's FETCH created a PENDING_PUBLISHER entry here. */
    moq_event_t ev;
    MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, &ev, 1), (size_t)1);
    MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_FETCH_REQUEST);
    size_t fslot = moq_handle_slot(ev.u.fetch_request.fetch._opaque);
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK(fslot < sv->fetch_cap);
    if (fslot >= sv->fetch_cap) { moq_session_destroy(c); moq_session_destroy(sv);
                                  return failures; }
    MOQ_TEST_CHECK_EQ_INT((int)sv->fetches[fslot].state,
                          (int)MOQ_FETCH_PENDING_PUBLISHER);
    MOQ_TEST_CHECK(sv->fetches[fslot].occ_linked);
    occ_audit(sv, "peer_fetch.pending_publisher");

    moq_session_destroy(c);
    moq_session_destroy(sv);
    return failures;
}

/*
 * The draft-18 request-STAGING slot. A request bidi whose message has not
 * arrived in full holds a real subscription slot in MOQ_SUB_RECVING_REQUEST;
 * that slot is allocated, so it must be linked, and it is the only shape in
 * which the staging site can be observed.
 */
static int test_d18_staging_occupancy(void)
{
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    moq_session_cfg_t ccfg = MOQ_SESSION_CFG_INIT, scfg = MOQ_SESSION_CFG_INIT;
    ccfg.alloc = &alloc; ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    scfg.alloc = &alloc; scfg.perspective = MOQ_PERSPECTIVE_SERVER;
    ccfg.version = scfg.version = MOQ_VERSION_DRAFT_18;
    moq_session_t *c = NULL, *sv = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_create(&ccfg, 0, &c), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_create(&scfg, 0, &sv), (int)MOQ_OK);
    if (!c || !sv) return failures;

    /* Draft-18 opens a uni control channel per side, so BOTH are started and
     * each side's setup rides its own OPEN_UNI_CONTROL. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(sv, 0), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(c, 0), (int)MOQ_OK);
    moq_action_t a;
    uint8_t sv_setup[512]; size_t sv_len = 0;
    while (moq_session_poll_actions(sv, &a, 1) == 1) {
        if (a.kind == MOQ_ACTION_OPEN_UNI_CONTROL &&
            a.u.open_uni_control.len <= sizeof(sv_setup)) {
            memcpy(sv_setup, a.u.open_uni_control.data,
                   a.u.open_uni_control.len);
            sv_len = a.u.open_uni_control.len;
        }
        moq_action_cleanup(&a);
    }
    while (moq_session_poll_actions(c, &a, 1) == 1) {
        if (a.kind == MOQ_ACTION_OPEN_UNI_CONTROL)
            MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_control_bytes(
                sv, a.u.open_uni_control.data, a.u.open_uni_control.len, 0),
                (int)MOQ_OK);
        moq_action_cleanup(&a);
    }
    MOQ_TEST_CHECK(sv_len > 0);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_control_bytes(c, sv_setup, sv_len, 0), (int)MOQ_OK);
    { moq_event_t e; while (moq_session_poll_events(c, &e, 1) == 1)
          moq_event_cleanup(&e);
      while (moq_session_poll_events(sv, &e, 1) == 1) moq_event_cleanup(&e); }
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv),
                          (int)MOQ_SESS_ESTABLISHED);
    occ_audit(sv, "d18_staging.established");

    /* A real SUBSCRIBE, delivered as a PREFIX so the request stays incomplete
     * and its staging slot persists. */
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_namespace_t ns = { parts, 1 };
    moq_subscribe_cfg_t subcfg;
    moq_subscribe_cfg_init(&subcfg);
    subcfg.track_namespace = ns;
    subcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_subscription_t sub;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscribe(c, &subcfg, 1000, &sub),
                          (int)MOQ_OK);
    uint8_t req[512]; size_t req_len = 0;
    moq_stream_ref_t bidi = moq_stream_ref_from_u64(0);
    while (moq_session_poll_actions(c, &a, 1) == 1) {
        if (a.kind == MOQ_ACTION_OPEN_BIDI_STREAM) {
            bidi = a.u.open_bidi_stream.stream_ref;
            if (a.u.open_bidi_stream.len <= sizeof(req)) {
                memcpy(req, a.u.open_bidi_stream.data, a.u.open_bidi_stream.len);
                req_len = a.u.open_bidi_stream.len;
            }
        }
        moq_action_cleanup(&a);
    }
    MOQ_TEST_CHECK(req_len > 4);
    if (req_len > 4) {
        /* Half the request: enough to create the staging slot, not enough to
         * complete the message. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_bidi_stream_bytes(
            sv, moq_stream_ref_from_u64(0x5150ull), req, req_len - 2, false,
            1100), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv),
                              (int)MOQ_SESS_ESTABLISHED);
        /* THE SITE: a staging slot exists and must be a list member. */
        bool staged = false;
        for (size_t i = 0; i < sv->sub_cap; i++)
            if (sv->subs[i].state == MOQ_SUB_RECVING_REQUEST) {
                staged = true;
                MOQ_TEST_CHECK(sv->subs[i].occ_linked);
            }
        MOQ_TEST_CHECK(staged);
        occ_audit(sv, "d18_staging.recving_request");

        /* The tail completes it; the same slot becomes the subscription. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_bidi_stream_bytes(
            sv, moq_stream_ref_from_u64(0x5150ull), req + req_len - 2, 2, false,
            1200), (int)MOQ_OK);
        occ_audit(sv, "d18_staging.completed");
    }
    (void)bidi;

    moq_session_destroy(c);
    moq_session_destroy(sv);
    return failures;
}

int main(void)
{
    failures += test_auditor_selfcheck();
    failures += test_local_publish_occupancy();
    failures += test_peer_publish_occupancy();
    failures += test_other_pools_occupancy();
    failures += test_idle_rx_cursor_unmoved();
    failures += test_parked_rx_cursor_advances();
    failures += test_pub_subgroup_occupancy();
    failures += test_peer_fetch_occupancy();
    failures += test_d18_staging_occupancy();
    if (failures == 0) MOQ_TEST_PASS("session_occupancy");
    printf("FAILURES: %d\n", failures);
    return failures ? 1 : 0;
}
