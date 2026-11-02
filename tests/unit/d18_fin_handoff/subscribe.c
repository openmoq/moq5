#include "test_cases.h"

/* -- Negative controls ------------------------------------------------ */

/* Control rows surface no further events after their request, so they reuse
 * the shared bidi-action normalizer with an event normalizer that refuses
 * everything: an unexpected event is a failure, not a silent pass. */
bool no_event_expected(const moq_event_t *ev, void *ctx,
                       txs_norm_vec_t *out)
{
    (void)ctx; (void)out;
    TXS_DIAG("TXN control: unexpected event kind %u\n", (unsigned)ev->kind);
    return false;
}


txs_op_hooks_t pub_hooks_for_actions = {
    NULL, NULL, NULL, NULL, no_event_expected, pub_norm_action
};


/* SUBSCRIBE is the same-entry control: the staging slot IS the subscription
 * slot, so there is no rekey and no marker to install. Its FIN is latched at
 * ingress today, and its terminal already consults that latch -- behaviour the
 * conversion must leave alone. */
void test_subscribe_control(void)
{
    for (int fin = 0; fin < 2; fin++) {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_namespace_t ns = { parts, 1 };
        moq_d18_msg_params_t mp;
        memset(&mp, 0, sizeof(mp));
        mp.has_filter = true;
        mp.filter_type = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        mp.has_forward = true; mp.forward = 1;
        uint8_t m[160];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, m, sizeof(m));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_encode_subscribe(&w, 0, &ns, MOQ_BYTES_LITERAL("v"),
                                          &mp), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, m,
                                                  moq_buf_writer_offset(&w),
                                                  fin != 0, 1), (int)MOQ_OK);
        moq_subscription_t sub = MOQ_SUBSCRIPTION_INVALID;
        moq_event_t ev;
        int n_req = 0, n_other = 0;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                n_req++;
                sub = ev.u.subscribe_request.sub;
            } else {
                n_other++;
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(n_req, 1);
        MOQ_TEST_CHECK_EQ_INT(n_other, 0);

        /* Same entry, and the close is on it already -- no rekey happened.
         * The full identity is asserted because "same entry" is the claim. */
        sub_owner_state_t so;
        sub_owner_capture(s, ref, &so);
        MOQ_TEST_CHECK(so.present);
        MOQ_TEST_CHECK_EQ_INT(so.role, (int)MOQ_SUB_ROLE_PUBLISHER);
        MOQ_TEST_CHECK_EQ_INT(so.state, (int)MOQ_SUB_PENDING_PUBLISHER);
        MOQ_TEST_CHECK_EQ_INT(so.rs.recv_fin, fin);
        MOQ_TEST_CHECK_EQ_U64(so.req_stream_ref, ref._v);
        MOQ_TEST_CHECK_EQ_U64(so.handle, sub._opaque);
        MOQ_TEST_CHECK(so.slot >= 0);
        MOQ_TEST_CHECK(so.has_stream_ref);
        MOQ_TEST_CHECK_EQ_U64(so.registry_ref, ref._v);
        MOQ_TEST_CHECK_EQ_INT(so.busy, 1);

        drain_snap_t ring_before;
        drain_snap(s, &ring_before);
        moq_reject_subscribe_cfg_t rj;
        moq_reject_subscribe_cfg_init(&rj);
        rj.error_code = MOQ_REQUEST_ERROR_NOT_SUPPORTED;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_reject_subscribe(s, sub, &rj, 1),
                              (int)MOQ_OK);
        drain_snap_t now, want_ring;
        drain_snap(s, &now);
        if (fin) want_ring = ring_before;
        else     drain_snap_plus(&ring_before, ref, MOQ_DRAIN_NORMAL,
                                 &want_ring);
        failures += drain_multiset_equals(&now, &want_ring, "subscribe control");
        MOQ_TEST_CHECK_EQ_INT(sub_busy_count(s), 0);
        check_unregistered(s, ref);
        check_reject_only_output(s, &pub_hooks_for_actions, ref,
                                 "subscribe control");

        if (!fin) {
            /* The reference the rejection took is released by the peer's
             * later FIN, restoring the ring exactly and producing nothing. */
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
                (int)MOQ_OK);
            drain_snap_t after;
            drain_snap(s, &after);
            failures += drain_multiset_equals(&after, &ring_before,
                                              "subscribe control released");
            MOQ_TEST_CHECK_EQ_INT(sub_busy_count(s), 0);
            check_unregistered(s, ref);
            check_no_output(s, &pub_hooks_for_actions);
        }
        moq_session_destroy(s);
    }
}


/* Route-exclusion negatives: a terminal the producer's owner cannot reach
 * must refuse it, which is why those sites are NOT marker consumers for that
 * producer. Note what each actually proves -- PUBLISH and PUBLISH_NAMESPACE
 * meet their STATE guard before their role guard is ever consulted, so only
 * FETCH demonstrates the role guard directly. */
void test_route_exclusions(void)
{
    /* Inbound PUBLISH commits SUBSCRIBER role in PENDING_SUBSCRIBER; the
     * finish terminal requires an established PUBLISHER-role publication and
     * refuses on state. */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        MOQ_TEST_CHECK_EQ_INT((int)feed_publish(s, ref, 0, 7, false),
                              (int)MOQ_OK);
        moq_publication_t pub = MOQ_PUBLICATION_INVALID;
        int n_want = 0, n_other = 0;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_PUBLISH_REQUEST) {
                n_want++;
                pub = ev.u.publish_request.pub;
            } else { n_other++; }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(n_want, 1);
        MOQ_TEST_CHECK_EQ_INT(n_other, 0);
        const moq_pub_entry_t *e = pub_by_ref(s, ref);
        MOQ_TEST_CHECK(e != NULL);
        if (e) {
            MOQ_TEST_CHECK_EQ_U64(e->handle._opaque, pub._opaque);
            MOQ_TEST_CHECK_EQ_INT((int)e->role, (int)MOQ_PUB_ROLE_SUBSCRIBER);
            MOQ_TEST_CHECK_EQ_INT((int)e->state,
                                  (int)MOQ_PUB_PENDING_SUBSCRIBER);
        }
        txs_snapshot_t before;
        txs_capture(s, &ref, 1, &before);
        expect_after_call_prepare(&before);
        moq_finish_publish_cfg_t fc;
        moq_finish_publish_cfg_init(&fc);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_finish_publish(s, pub, &fc, 1),
                              (int)MOQ_ERR_WRONG_STATE);
        failures += txs_check_eq(s, &ref, 1, &before, "publish route exclusion");
        failures += pub_owner_present_problems(s, ref, MOQ_REQ_PUBLISH,
                                               (int)MOQ_PUB_ROLE_SUBSCRIBER,
                                               (int)MOQ_PUB_PENDING_SUBSCRIBER);
        check_no_output(s, &pub_hooks_for_actions);
        moq_session_destroy(s);
    }

    /* Inbound FETCH commits PUBLISHER role; the cancel terminal is
     * FETCHER-only, so this one does exercise the ROLE guard. */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        fetch_ctx_t fc = { 0, ref, 0 };
        MOQ_TEST_CHECK_EQ_INT((int)fetch_feed(s, &fc, ref, false), (int)MOQ_OK);
        moq_fetch_t fh = { 0 };
        int n_want = 0, n_other = 0;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_REQUEST) {
                n_want++;
                fh = ev.u.fetch_request.fetch;
            } else { n_other++; }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(n_want, 1);
        MOQ_TEST_CHECK_EQ_INT(n_other, 0);
        const moq_fetch_entry_t *e = fetch_by_ref(s, ref);
        MOQ_TEST_CHECK(e != NULL);
        if (e) {
            MOQ_TEST_CHECK_EQ_U64(e->handle._opaque, fh._opaque);
            MOQ_TEST_CHECK_EQ_INT((int)e->role,
                                  (int)MOQ_FETCH_ROLE_PUBLISHER);
            MOQ_TEST_CHECK_EQ_INT((int)e->state,
                                  (int)MOQ_FETCH_PENDING_PUBLISHER);
        }
        txs_snapshot_t before;
        txs_capture(s, &ref, 1, &before);
        expect_after_call_prepare(&before);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch_cancel(s, fh, 1),
                              (int)MOQ_ERR_WRONG_STATE);
        failures += txs_check_eq(s, &ref, 1, &before, "fetch route exclusion");
        failures += fetch_owner_present_problems(s, ref, MOQ_REQ_FETCH,
                                                 (int)MOQ_FETCH_ROLE_PUBLISHER,
                                                 (int)MOQ_FETCH_PENDING_PUBLISHER);
        check_no_output(s, &pub_hooks_for_actions);
        moq_session_destroy(s);
    }

    /* Inbound PUBLISH_NAMESPACE commits RECEIVER role in PENDING_RECEIVER;
     * the done terminal requires an established ANNOUNCER-role announcement
     * and refuses on state. */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        pn_ctx_t pc = { 0, ref, 0 };
        MOQ_TEST_CHECK_EQ_INT((int)pn_feed(s, &pc, ref, false), (int)MOQ_OK);
        moq_announcement_t ah = { 0 };
        int n_want = 0, n_other = 0;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED) {
                n_want++;
                ah = ev.u.namespace_published.ann;
            } else { n_other++; }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(n_want, 1);
        MOQ_TEST_CHECK_EQ_INT(n_other, 0);
        const moq_ann_entry_t *e = ann_by_ref(s, ref);
        MOQ_TEST_CHECK(e != NULL);
        if (e) {
            MOQ_TEST_CHECK_EQ_U64(e->handle._opaque, ah._opaque);
            MOQ_TEST_CHECK_EQ_INT((int)e->role, (int)MOQ_ANN_ROLE_RECEIVER);
            MOQ_TEST_CHECK_EQ_INT((int)e->state,
                                  (int)MOQ_ANN_PENDING_RECEIVER);
        }
        txs_snapshot_t before;
        txs_capture(s, &ref, 1, &before);
        expect_after_call_prepare(&before);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_publish_namespace_done(s, ah, 1),
            (int)MOQ_ERR_WRONG_STATE);
        failures += txs_check_eq(s, &ref, 1, &before, "namespace route exclusion");
        failures += pn_owner_present_problems(s, ref, MOQ_REQ_ANNOUNCEMENT,
                                              (int)MOQ_ANN_ROLE_RECEIVER,
                                              (int)MOQ_ANN_PENDING_RECEIVER);
        check_no_output(s, &pub_hooks_for_actions);
        moq_session_destroy(s);
    }
}



/* -- subscriber-side unsubscribe (session_subscribe.c:3012) ----------- */

/*
 * A locally-issued SUBSCRIBE the peer accepted, optionally closing its half in
 * the same SUBSCRIBE_OK. `subscribe_request_bidi_cancel` reserves its drain
 * FIN-aware: it takes a NORMAL reference only when the peer half is still open
 * (req_recv_fin false), so a same-call FIN owes none and the latch rows are
 * drainless under the unified rule.
 */

#define R6_TRACK_ALIAS   0x63

#define R6_LARGEST_GROUP 0x41

#define R6_LARGEST_OBJ   0x09

#define R6_EXPIRES_MS    7000

static const uint8_t k_r6_name[] = { 'v','0' };


static int g_r6_want_slot = -1;


int sub_owner_edges(const og_graph_t *g, moq_stream_ref_t ref,
                    const char *what)
{
    int bad = 0;
    if (g_r6_want_slot < 0) {
        OG_DIAG("OG %s: no declared subscription slot\n", what);
        return 1;
    }
    bad += og_check_edge(g, OG_DOM_REQ_STREAMREF, ref._v,
                         MOQ_REQ_SUBSCRIPTION, g_r6_want_slot, what);
    {
        const og_edge_spec_t want[] = { { OG_DOM_REQ_STREAMREF, ref._v } };
        bad += og_check_owner_edges(g, MOQ_REQ_SUBSCRIPTION, g_r6_want_slot,
                                    want, 1, what);
    }
    bad += og_check_no_edge(g, OG_DOM_NS_REF, ref._v, what);
    return bad;
}


moq_stream_ref_t arm_subscriber_established(moq_session_t *s,
                                            bool fin_observed,
                                            uint64_t *out_handle)
{
    /* The slot and generation the request will take, derived from pool state
     * BEFORE the call (sub_find_free takes the first free slot; the request
     * sets `generation |= 1`). */
    int want_slot = -1;
    uint32_t want_gen = 0;
    for (size_t i = 0; i < s->sub_cap; i++)
        if (s->subs[i].state == MOQ_SUB_FREE) {
            want_slot = (int)i;
            want_gen = s->subs[i].generation | 1u;
            break;
        }
    MOQ_TEST_CHECK(want_slot >= 0);
    if (want_slot < 0) return moq_stream_ref_from_u64(0);
    g_r6_want_slot = want_slot;

    moq_bytes_t parts[] = { { k_ann_ns0, sizeof(k_ann_ns0) },
                            { k_ann_ns1, sizeof(k_ann_ns1) } };
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ parts, 2 };
    sc.track_name = MOQ_BYTES_LITERAL("v0");
    moq_subscription_t sub = MOQ_SUBSCRIPTION_INVALID;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscribe(s, &sc, 1, &sub),
                          (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(sub._opaque,
                          moq_handle_pack(MOQ_HANDLE_POOL_SUBSCRIPTION,
                                          s->session_tag, want_gen,
                                          (uint32_t)want_slot));

    moq_stream_ref_t ref = moq_stream_ref_from_u64(0);
    {
        int opens = 0, other = 0;
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) {
            if (a.kind == MOQ_ACTION_OPEN_BIDI_STREAM) {
                opens++;
                ref = a.u.open_bidi_stream.stream_ref;
                MOQ_TEST_CHECK_EQ_INT(a.u.open_bidi_stream.fin ? 1 : 0, 0);
                moq_buf_reader_t rr;
                moq_buf_reader_init(&rr, a.u.open_bidi_stream.data,
                                    a.u.open_bidi_stream.len);
                moq_control_envelope_t env;
                memset(&env, 0, sizeof(env));
                MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_envelope(&rr, &env),
                                      (int)MOQ_OK);
                MOQ_TEST_CHECK_EQ_U64(env.msg_type,
                                      (uint64_t)MOQ_D18_SUBSCRIBE);
                MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&rr),
                                       (size_t)0);
                /* The body against the fixture's DECLARED request. */
                moq_bytes_t dp[8];
                moq_d18_subscribe_t sb;
                memset(dp, 0, sizeof(dp));
                memset(&sb, 0, sizeof(sb));
                int dec_ok = (moq_d18_decode_subscribe(env.payload,
                                  env.payload_len, dp, 8, &sb) == MOQ_OK);
                MOQ_TEST_CHECK(dec_ok);
                if (dec_ok) {
                    MOQ_TEST_CHECK_EQ_U64(sb.request_id, 0);
                    MOQ_TEST_CHECK_EQ_SIZE(sb.track_namespace.count, (size_t)2);
                    if (sb.track_namespace.count == 2)
                        MOQ_TEST_CHECK(sb.track_namespace.parts != NULL);
                    if (sb.track_namespace.count == 2 &&
                        sb.track_namespace.parts) {
                        const moq_bytes_t *q0 = &sb.track_namespace.parts[0];
                        const moq_bytes_t *q1 = &sb.track_namespace.parts[1];
                        MOQ_TEST_CHECK_EQ_SIZE(q0->len, sizeof(k_ann_ns0));
                        MOQ_TEST_CHECK(q0->len == 0 || q0->data != NULL);
                        if (q0->len == sizeof(k_ann_ns0) && q0->data)
                            MOQ_TEST_CHECK(memcmp(q0->data, k_ann_ns0,
                                                  sizeof(k_ann_ns0)) == 0);
                        MOQ_TEST_CHECK_EQ_SIZE(q1->len, sizeof(k_ann_ns1));
                        MOQ_TEST_CHECK(q1->len == 0 || q1->data != NULL);
                        if (q1->len == sizeof(k_ann_ns1) && q1->data)
                            MOQ_TEST_CHECK(memcmp(q1->data, k_ann_ns1,
                                                  sizeof(k_ann_ns1)) == 0);
                    }
                    MOQ_TEST_CHECK_EQ_SIZE(sb.track_name.len,
                                           sizeof(k_r6_name));
                    MOQ_TEST_CHECK(sb.track_name.len == 0 ||
                                   sb.track_name.data != NULL);
                    if (sb.track_name.len == sizeof(k_r6_name) &&
                        sb.track_name.data)
                        MOQ_TEST_CHECK(memcmp(sb.track_name.data, k_r6_name,
                                              sizeof(k_r6_name)) == 0);
                    /* The default parameter set the fixture configured. */
                    MOQ_TEST_CHECK_EQ_SIZE(sb.params.auth_token_count,
                                           (size_t)0);
                    MOQ_TEST_CHECK_EQ_INT(
                        sb.params.has_subgroup_delivery_timeout ? 1 : 0, 0);
                    MOQ_TEST_CHECK_EQ_INT(
                        sb.params.has_object_delivery_timeout ? 1 : 0, 0);
                }
            } else other++;
            moq_action_cleanup(&a);
        }
        MOQ_TEST_CHECK_EQ_INT(opens, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
    }
    MOQ_TEST_CHECK(ref._v != 0);
    if (ref._v == 0) return ref;

    /* The peer accepts with a declared non-default alias, optionally FINning. */
    uint8_t ok[128];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, ok, sizeof(ok));
    {
        moq_d18_msg_params_t p = { 0 };
        p.has_largest = true;
        p.largest_group = R6_LARGEST_GROUP;
        p.largest_object = R6_LARGEST_OBJ;
        p.has_expires = true;
        p.expires_ms = R6_EXPIRES_MS;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_encode_subscribe_ok(&w, R6_TRACK_ALIAS, &p,
                                             (moq_bytes_t){0}), (int)MOQ_OK);
    }
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, ok,
                                              moq_buf_writer_offset(&w),
                                              fin_observed, 1), (int)MOQ_OK);
    {
        int oks = 0, other = 0;
        moq_event_t e;
        while (moq_session_poll_events(s, &e, 1) > 0) {
            if (e.kind == MOQ_EVENT_SUBSCRIBE_OK) {
                oks++;
                const moq_subscribe_ok_event_t *ok_e = &e.u.subscribe_ok;
                MOQ_TEST_CHECK_EQ_U64(ok_e->sub._opaque, sub._opaque);
                MOQ_TEST_CHECK_EQ_U64(ok_e->track_alias, R6_TRACK_ALIAS);
                MOQ_TEST_CHECK_EQ_INT(ok_e->has_largest ? 1 : 0, 1);
                MOQ_TEST_CHECK_EQ_U64(ok_e->largest_group, R6_LARGEST_GROUP);
                MOQ_TEST_CHECK_EQ_U64(ok_e->largest_object, R6_LARGEST_OBJ);
                MOQ_TEST_CHECK_EQ_INT(ok_e->has_expires ? 1 : 0, 1);
                MOQ_TEST_CHECK_EQ_U64(ok_e->expires_ms, R6_EXPIRES_MS);
                MOQ_TEST_CHECK_EQ_SIZE(ok_e->track_properties.len, (size_t)0);
                MOQ_TEST_CHECK_EQ_INT(ok_e->dynamic_groups ? 1 : 0, 0);
            } else other++;
            moq_event_cleanup(&e);
        }
        MOQ_TEST_CHECK_EQ_INT(oks, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
    }

    {
        const moq_sub_entry_t *e = &s->subs[want_slot];
        MOQ_TEST_CHECK_EQ_INT(sub_pool_busy(s), 1);
        MOQ_TEST_CHECK_EQ_INT((int)e->state, (int)MOQ_SUB_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT((int)e->role, (int)MOQ_SUB_ROLE_SUBSCRIBER);
        MOQ_TEST_CHECK_EQ_U64(e->generation, want_gen);
        MOQ_TEST_CHECK_EQ_U64(e->handle._opaque, sub._opaque);
        MOQ_TEST_CHECK_EQ_U64(e->request_id, 0);
        MOQ_TEST_CHECK_EQ_U64(e->track_alias, R6_TRACK_ALIAS);
        MOQ_TEST_CHECK_EQ_U64(e->rs.ref._v, ref._v);
        MOQ_TEST_CHECK_EQ_INT(e->rs.recv_fin ? 1 : 0, fin_observed ? 1 : 0);
        check_registered(s, ref, MOQ_REQ_SUBSCRIPTION, want_slot);
    }
    if (out_handle) *out_handle = sub._opaque;
    return ref;
}
typedef struct r6_full {
    int      valid;
    moq_sub_entry_t raw;
    size_t   req_len;
    uint8_t  req[R6_OWN_MAX];
    size_t   tid_len;
    uint8_t  tid[R6_OWN_MAX];
} r6_full_t;


static r6_full_t g_r6_full;


static void r6_full_read(const moq_session_t *s, r6_full_t *v)
{
    memset(v, 0, sizeof(*v));
    if (g_r6_want_slot < 0 || (size_t)g_r6_want_slot >= s->sub_cap) return;
    const moq_sub_entry_t *e = &s->subs[g_r6_want_slot];
    v->raw = *e;
    v->req_len = e->rs.recv_len;
    if (e->rs.recv_len > R6_OWN_MAX || (e->rs.recv_len && !e->rs.recv_buf))
        return;
    if (e->rs.recv_len) memcpy(v->req, e->rs.recv_buf, e->rs.recv_len);
    v->tid_len = e->track_id_len;
    if (e->track_id_len > R6_OWN_MAX || (e->track_id_len && !e->track_id_buf))
        return;
    if (e->track_id_len) memcpy(v->tid, e->track_id_buf, e->track_id_len);
    v->valid = 1;
}


static void r6_full_capture(const moq_session_t *s)
{
    r6_full_read(s, &g_r6_full);
    MOQ_TEST_CHECK_EQ_INT(g_r6_full.valid, 1);
}


static int r6_full_check_blocked(const moq_session_t *s, const char *what)
{
    r6_full_t now;
    r6_full_read(s, &now);
    int bad = 0;
    if (!now.valid || !g_r6_full.valid) {
        TXS_DIAG("TXN %s: incomparable subscription record\n", what);
        return 1;
    }
    if (memcmp(&now.raw, &g_r6_full.raw, sizeof(now.raw)) != 0) {
        TXS_DIAG("TXN %s: established subscription entry changed\n", what);
        bad++;
    }
    if (now.req_len != g_r6_full.req_len ||
        (now.req_len && memcmp(now.req, g_r6_full.req, now.req_len) != 0)) {
        TXS_DIAG("TXN %s: retained request bytes changed\n", what);
        bad++;
    }
    if (now.tid_len != g_r6_full.tid_len ||
        (now.tid_len && memcmp(now.tid, g_r6_full.tid, now.tid_len) != 0)) {
        TXS_DIAG("TXN %s: retained track identity changed\n", what);
        bad++;
    }
    return bad;
}


/* A completed cancel retires the slot, so the only committed requirement is
 * that it is no longer live; retirement itself is asserted by the runner. */
static int r6_full_check_committed(const moq_session_t *s, const char *what)
{
    if (g_r6_want_slot < 0 || (size_t)g_r6_want_slot >= s->sub_cap) return 0;
    if (s->subs[g_r6_want_slot].state != MOQ_SUB_FREE) {
        TXS_DIAG("TXN %s: cancelled subscription slot is still live\n", what);
        return 1;
    }
    return 0;
}


const extra_ops_t r6_full_ops = {
    r6_full_capture, r6_full_check_blocked, r6_full_check_committed
};


moq_result_t fire_unsubscribe(moq_session_t *s, uint64_t handle)
{
    moq_subscription_t sub;
    sub._opaque = handle;
    return moq_session_unsubscribe(s, sub, 1);
}


void want_bidi_cancel(txs_norm_vec_t *v, moq_stream_ref_t ref,
                      uint64_t handle)
{
    (void)handle;
    want_nsl_output(v, ref);   /* one ABORT_BIDI_STREAM(ref, CANCELLED) */
}




/* -- Route: done SUBSCRIBE (session_subscribe.c:3250, moq_session_done_subscribe)
 *
 * The terminal reserves its drain FIN-aware: it consults `e->rs.recv_fin`, so
 * it takes a reference only when the peer half is still open and a same-call FIN
 * owes none. */
#define R7_STATUS   0x2

#define R7_STREAMS  0x9

#define R7_TRACK_ALIAS 0x74

static const char k_r7_reason[] = "publisher done";

static const uint8_t k_r7_ns[] = { 'l','i','v','e' };

static const uint8_t k_r7_name[] = { 'v','0' };


static int g_r7_want_slot = -1;


int r7_owner_edges(const og_graph_t *g, moq_stream_ref_t ref,
                   const char *what)
{
    int bad = 0;
    if (g_r7_want_slot < 0) {
        OG_DIAG("OG %s: no declared subscription slot\n", what);
        return 1;
    }
    bad += og_check_edge(g, OG_DOM_REQ_STREAMREF, ref._v,
                         MOQ_REQ_SUBSCRIPTION, g_r7_want_slot, what);
    {
        const og_edge_spec_t want[] = { { OG_DOM_REQ_STREAMREF, ref._v } };
        bad += og_check_owner_edges(g, MOQ_REQ_SUBSCRIPTION, g_r7_want_slot,
                                    want, 1, what);
    }
    bad += og_check_no_edge(g, OG_DOM_NS_REF, ref._v, what);
    return bad;
}


/* The whole accepted publisher-role subscription, compared verbatim. */
typedef struct r7_full {
    int      valid;
    moq_sub_entry_t raw;
    size_t   req_len;
    uint8_t  req[R6_OWN_MAX];
    size_t   tid_len;
    uint8_t  tid[R6_OWN_MAX];
} r7_full_t;


static r7_full_t g_r7_full;


static void r7_full_read(const moq_session_t *s, r7_full_t *v)
{
    memset(v, 0, sizeof(*v));
    if (g_r7_want_slot < 0 || (size_t)g_r7_want_slot >= s->sub_cap) return;
    const moq_sub_entry_t *e = &s->subs[g_r7_want_slot];
    v->raw = *e;
    v->req_len = e->rs.recv_len;
    if (e->rs.recv_len > R6_OWN_MAX || (e->rs.recv_len && !e->rs.recv_buf))
        return;
    if (e->rs.recv_len) memcpy(v->req, e->rs.recv_buf, e->rs.recv_len);
    v->tid_len = e->track_id_len;
    if (e->track_id_len > R6_OWN_MAX || (e->track_id_len && !e->track_id_buf))
        return;
    if (e->track_id_len) memcpy(v->tid, e->track_id_buf, e->track_id_len);
    v->valid = 1;
}


static void r7_full_capture(const moq_session_t *s)
{
    r7_full_read(s, &g_r7_full);
    MOQ_TEST_CHECK_EQ_INT(g_r7_full.valid, 1);
}


static int r7_full_check_blocked(const moq_session_t *s, const char *what)
{
    r7_full_t now;
    r7_full_read(s, &now);
    int bad = 0;
    if (!now.valid || !g_r7_full.valid) {
        TXS_DIAG("TXN %s: incomparable subscription record\n", what);
        return 1;
    }
    if (memcmp(&now.raw, &g_r7_full.raw, sizeof(now.raw)) != 0) {
        TXS_DIAG("TXN %s: accepted subscription entry changed\n", what);
        bad++;
    }
    if (now.req_len != g_r7_full.req_len ||
        (now.req_len && memcmp(now.req, g_r7_full.req, now.req_len) != 0)) {
        TXS_DIAG("TXN %s: retained request bytes changed\n", what);
        bad++;
    }
    if (now.tid_len != g_r7_full.tid_len ||
        (now.tid_len && memcmp(now.tid, g_r7_full.tid, now.tid_len) != 0)) {
        TXS_DIAG("TXN %s: retained track identity changed\n", what);
        bad++;
    }
    return bad;
}


static int r7_full_check_committed(const moq_session_t *s, const char *what)
{
    if (g_r7_want_slot < 0 || (size_t)g_r7_want_slot >= s->sub_cap) return 0;
    if (s->subs[g_r7_want_slot].state != MOQ_SUB_FREE) {
        TXS_DIAG("TXN %s: finished subscription slot is still live\n", what);
        return 1;
    }
    return 0;
}


const extra_ops_t r7_full_ops = {
    r7_full_capture, r7_full_check_blocked, r7_full_check_committed
};


moq_stream_ref_t arm_accepted_subscription(moq_session_t *s,
                                           bool fin_observed,
                                           uint64_t *out_handle)
{
    /* Slot, generation and the handle they pack into, all derived from pool
     * state BEFORE the request is fed. */
    int want_slot = -1;
    uint32_t want_gen = 0;
    for (size_t i = 0; i < s->sub_cap; i++)
        if (s->subs[i].state == MOQ_SUB_FREE) {
            want_slot = (int)i;
            want_gen = s->subs[i].generation | 1u;
            break;
        }
    MOQ_TEST_CHECK(want_slot >= 0);
    if (want_slot < 0) return moq_stream_ref_from_u64(0);
    g_r7_want_slot = want_slot;
    uint64_t want_handle = moq_handle_pack(MOQ_HANDLE_POOL_SUBSCRIPTION,
                                           s->session_tag, want_gen,
                                           (uint32_t)want_slot);

    moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
    uint8_t msg[192];
    size_t n = enc_subscribe_pub_role(msg, sizeof(msg), 0);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, msg, n, fin_observed, 1),
        (int)MOQ_OK);

    moq_subscription_t sub = MOQ_SUBSCRIPTION_INVALID;
    {
        int req = 0, other = 0;
        moq_event_t e;
        while (moq_session_poll_events(s, &e, 1) > 0) {
            if (e.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                req++;
                sub = e.u.subscribe_request.sub;
                /* The complete request, against the fixture's own constants
                 * and the handle derived before ingress. */
                const moq_subscribe_request_event_t *rq = &e.u.subscribe_request;
                MOQ_TEST_CHECK_EQ_U64(rq->sub._opaque, want_handle);
                MOQ_TEST_CHECK_EQ_SIZE(rq->track_namespace.count, (size_t)1);
                if (rq->track_namespace.count == 1 &&
                    rq->track_namespace.parts) {
                    const moq_bytes_t *q = &rq->track_namespace.parts[0];
                    failures += txs_check_span_bytes(q->data, q->len, k_r7_ns,
                                                     sizeof(k_r7_ns),
                                                     "route7 request namespace");
                } else MOQ_TEST_CHECK(0);
                failures += txs_check_span_bytes(rq->track_name.data,
                                                 rq->track_name.len, k_r7_name,
                                                 sizeof(k_r7_name),
                                                 "route7 request track name");
                /* The subscriber-declared subgroup timeout the encoder set. */
                MOQ_TEST_CHECK_EQ_U64(rq->delivery_timeout_us,
                                      5000ull * 1000ull);
                MOQ_TEST_CHECK_EQ_SIZE(rq->token_count, (size_t)0);
                MOQ_TEST_CHECK_EQ_INT(rq->has_new_group_request ? 1 : 0, 0);
            } else other++;
            moq_event_cleanup(&e);
        }
        MOQ_TEST_CHECK_EQ_INT(req, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
    }

    moq_accept_subscribe_cfg_t acfg;
    moq_accept_subscribe_cfg_init(&acfg);
    acfg.has_track_alias = true;
    acfg.track_alias = R7_TRACK_ALIAS;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_subscribe(s, sub, &acfg, 1),
                          (int)MOQ_OK);

    /* The acceptance owes exactly one SUBSCRIBE_OK carrying the declared alias,
     * with our send half left open and nothing after the envelope. */
    {
        int oks = 0, other = 0;
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) {
            if (a.kind == MOQ_ACTION_SEND_BIDI_STREAM &&
                a.u.send_bidi_stream.stream_ref._v == ref._v) {
                oks++;
                MOQ_TEST_CHECK_EQ_INT(a.u.send_bidi_stream.fin ? 1 : 0, 0);
                moq_buf_reader_t rr;
                moq_buf_reader_init(&rr, a.u.send_bidi_stream.data,
                                    a.u.send_bidi_stream.len);
                moq_control_envelope_t env;
                memset(&env, 0, sizeof(env));
                MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_envelope(&rr, &env),
                                      (int)MOQ_OK);
                MOQ_TEST_CHECK_EQ_U64(env.msg_type,
                                      (uint64_t)MOQ_D18_SUBSCRIBE_OK);
                MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&rr),
                                       (size_t)0);
                moq_d18_subscribe_ok_t ok;
                memset(&ok, 0, sizeof(ok));
                if (moq_d18_decode_subscribe_ok(env.payload, env.payload_len,
                                                &ok) == MOQ_OK)
                    MOQ_TEST_CHECK_EQ_U64(ok.track_alias, R7_TRACK_ALIAS);
                else
                    MOQ_TEST_CHECK(0);
            } else other++;
            moq_action_cleanup(&a);
        }
        MOQ_TEST_CHECK_EQ_INT(oks, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
    }

    {
        const moq_sub_entry_t *e = &s->subs[want_slot];
        MOQ_TEST_CHECK_EQ_INT(sub_pool_busy(s), 1);
        MOQ_TEST_CHECK_EQ_INT((int)e->state, (int)MOQ_SUB_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT((int)e->role, (int)MOQ_SUB_ROLE_PUBLISHER);
        MOQ_TEST_CHECK_EQ_U64(e->handle._opaque, want_handle);
        MOQ_TEST_CHECK_EQ_U64(e->generation, want_gen);
        MOQ_TEST_CHECK_EQ_U64(e->request_id, 0);
        MOQ_TEST_CHECK_EQ_U64(e->track_alias, R7_TRACK_ALIAS);
        MOQ_TEST_CHECK_EQ_U64(e->rs.ref._v, ref._v);
        MOQ_TEST_CHECK_EQ_INT(e->rs.recv_fin ? 1 : 0, fin_observed ? 1 : 0);
        MOQ_TEST_CHECK_EQ_INT(e->goaway_sent ? 1 : 0, 0);
        check_registered(s, ref, MOQ_REQ_SUBSCRIPTION, want_slot);
    }
    if (out_handle) *out_handle = sub._opaque;
    return ref;
}


/* The terminal's whole output is one control message on the request bidi. */
txs_op_hooks_t r7_hooks = {
    NULL, NULL, NULL, NULL, no_event_expected, pub_norm_action
};


moq_result_t fire_done_subscribe(moq_session_t *s, uint64_t handle)
{
    moq_subscription_t sub;
    sub._opaque = handle;
    moq_done_subscribe_cfg_t c;
    memset(&c, 0, sizeof(c));
    c.struct_size = (uint32_t)sizeof(c);
    c.status_code = R7_STATUS;
    c.stream_count = R7_STREAMS;
    c.reason = (moq_bytes_t){ (const uint8_t *)k_r7_reason,
                              sizeof(k_r7_reason) - 1 };
    return moq_session_done_subscribe(s, sub, &c, 1);
}


/* The declared terminal: one PUBLISH_DONE on the request bidi, FINning our
 * send half, encoded independently of the product. */
void want_done_subscribe(txs_norm_vec_t *v, moq_stream_ref_t ref,
                         uint64_t handle)
{
    (void)handle;
    uint8_t buf[128];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_d18_encode_publish_done(
            &w, R7_STATUS, R7_STREAMS,
            (moq_bytes_t){ (const uint8_t *)k_r7_reason,
                           sizeof(k_r7_reason) - 1 }), (int)MOQ_OK);
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
    moq_control_envelope_t env;
    MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_envelope(&r, &env), (int)MOQ_OK);
    txs_img_t im;
    txs_img_init(&im);
    txs_img_u64(&im, ref._v);
    txs_img_u64(&im, 1);            /* FIN closes our send half */
    txs_img_u64(&im, env.msg_type);
    txs_img_bytes(&im, env.payload, env.payload_len);
    MOQ_TEST_CHECK(txs_norm_append_img(v, MOQ_ACTION_SEND_BIDI_STREAM, &im));
}
