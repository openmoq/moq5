#include "test_cases.h"

/* -- Request-stream GOAWAY (§10.4) ------------------------------------ */

/* A GOAWAY on a committed request bidi migrates that one request: it surfaces
 * REQUEST_GOAWAY, closes our send half where it is still open, frees the entry
 * and retires the ref with a GOAWAY-STRICT reference only when the peer half is
 * still open. The caller passes the observed peer-FIN fact through, so a GOAWAY
 * that rode the same-call FIN owes no drain. */

#define GOAWAY_TIMEOUT_MS 4200


size_t enc_goaway_request(uint8_t *buf, size_t cap)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_d18_encode_goaway_request(&w, NULL, 0, GOAWAY_TIMEOUT_MS),
        (int)MOQ_OK);
    return moq_buf_writer_offset(&w);
}


/* The handle the arm surfaced, so the expected event can be built from what the
 * fixture knows rather than from the event under test. */
static uint64_t g_goaway_handle;

static moq_request_family_t g_goaway_family;


static bool goaway_norm_event(const moq_event_t *ev, void *vctx,
                              txs_norm_vec_t *out)
{
    (void)vctx;
    txs_img_t im;
    txs_img_init(&im);
    if (ev->kind != MOQ_EVENT_REQUEST_GOAWAY) {
        TXS_DIAG("TXN goaway: unnormalized event kind %u\n",
                 (unsigned)ev->kind);
        return false;
    }
    txs_img_u64(&im, (uint64_t)ev->u.request_goaway.family);
    txs_img_u64(&im, ev->u.request_goaway.handle.raw);
    txs_img_u64(&im, ev->u.request_goaway.timeout_ms);
    txs_img_bytes(&im, ev->u.request_goaway.new_session_uri.data,
                  ev->u.request_goaway.new_session_uri.len);
    return txs_norm_append_img(out, ev->kind, &im);
}


txs_op_hooks_t goaway_hooks = {
    NULL, NULL, NULL, NULL, goaway_norm_event, nss_norm_action
};


/* The event, then the close of our own send half. */
void want_goaway_output(txs_norm_vec_t *v, moq_stream_ref_t ref)
{
    txs_img_t im;
    txs_img_init(&im);
    txs_img_u64(&im, (uint64_t)g_goaway_family);
    txs_img_u64(&im, g_goaway_handle);
    txs_img_u64(&im, GOAWAY_TIMEOUT_MS);
    txs_img_bytes(&im, NULL, 0);          /* no New Session URI */
    MOQ_TEST_CHECK(txs_norm_append_img(v, MOQ_EVENT_REQUEST_GOAWAY, &im));
    want_close_bidi(v, ref);
}


/* An accepted inbound PUBLISH_NAMESPACE: a committed, peer-opened request bidi,
 * which is the route d18_process_request_stream takes. */
moq_stream_ref_t arm_goaway_announcement(moq_session_t *s)
{
    moq_stream_ref_t ref = arm_established_announcement(s);
    const moq_ann_entry_t *e = ann_by_ref(s, ref);
    MOQ_TEST_CHECK(e != NULL);
    g_goaway_handle = e ? e->handle._opaque : 0;
    g_goaway_family = MOQ_REQUEST_FAMILY_ANNOUNCEMENT;
    return ref;
}


/* The response-route caller carries a New Session URI, so that field is
 * discriminated rather than always empty. A server must reject a non-zero URI
 * (§10.4), so this row runs on a CLIENT session. */
static const uint8_t k_goaway_uri[] = { 'm','o','q',':','/','/','a','l','t' };


size_t enc_goaway_request_uri(uint8_t *buf, size_t cap)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_d18_encode_goaway_request(&w, k_goaway_uri,
                                           sizeof(k_goaway_uri),
                                           GOAWAY_TIMEOUT_MS),
        (int)MOQ_OK);
    return moq_buf_writer_offset(&w);
}


/* Built from the fixture's own constant, never from the event under test. */
void want_goaway_output_uri(txs_norm_vec_t *v, moq_stream_ref_t ref)
{
    txs_img_t im;
    txs_img_init(&im);
    txs_img_u64(&im, (uint64_t)g_goaway_family);
    txs_img_u64(&im, g_goaway_handle);
    txs_img_u64(&im, GOAWAY_TIMEOUT_MS);
    txs_img_bytes(&im, k_goaway_uri, sizeof(k_goaway_uri));
    MOQ_TEST_CHECK(txs_norm_append_img(v, MOQ_EVENT_REQUEST_GOAWAY, &im));
    want_close_bidi(v, ref);
}


moq_session_t *make_session_client(void)
{
    return make_session(MOQ_PERSPECTIVE_CLIENT);
}


int sub_pool_busy(const moq_session_t *s)
{
    int n = 0;
    for (size_t i = 0; i < s->sub_cap; i++)
        if (s->subs[i].state != MOQ_SUB_FREE) n++;
    return n;
}


const moq_sub_entry_t *sub_entry_by_ref(const moq_session_t *s,
                                        moq_stream_ref_t ref)
{
    moq_request_endpoint_t ep = request_registry_find_by_streamref(s, ref);
    if (ep.kind != MOQ_REQ_SUBSCRIPTION) return NULL;
    if (ep.slot < 0 || (size_t)ep.slot >= s->sub_cap) return NULL;
    return &s->subs[ep.slot];
}


static void sub_term_capture(const moq_session_t *s, moq_stream_ref_t ref,
                             seed_snap_t *o)
{
    req_seed_snap_t *r = &o->req;
    memset(r, 0, sizeof(*r));
    r->busy = sub_pool_busy(s);
    const moq_sub_entry_t *e = sub_entry_by_ref(s, ref);
    r->found = e != NULL;
    if (e) {
        r->state      = (int)e->state;
        r->role       = (int)e->role;
        r->generation = e->generation;
        r->handle     = e->handle._opaque;
        r->request_id = e->request_id;
        r->stream_ref = e->rs.ref._v;
        r->fin        = e->rs.recv_fin ? 1 : 0;
        r->recv_len   = e->rs.recv_len;
        req_seed_copy_buf(r, e->rs.recv_buf, e->rs.recv_len);
    }
    req_seed_capture_registry(r, s, ref, r->request_id);
}


static void sub_term_check(const moq_session_t *s, moq_stream_ref_t ref,
                           const seed_snap_t *want, const char *what)
{
    seed_snap_t now;
    sub_term_capture(s, ref, &now);
    req_seed_diff(&now.req, &want->req, what);
}


void check_sub_terminal_retired(const moq_session_t *s,
                                moq_stream_ref_t ref)
{
    MOQ_TEST_CHECK_EQ_INT(sub_pool_busy(s), 0);
    MOQ_TEST_CHECK(sub_entry_by_ref(s, ref) == NULL);
    check_unregistered(s, ref);
}


const owner_ops_t sub_owner_ops = {
    sub_term_capture, sub_term_check, check_sub_terminal_retired
};


/* A locally-issued SUBSCRIBE: the request bidi is OURS, so its ref comes from
 * the transport action rather than from the fixture. Setup must leave no other
 * record behind before the operation window opens. */
moq_stream_ref_t arm_goaway_local_subscribe(moq_session_t *s)
{
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ parts, 1 };
    sc.track_name = MOQ_BYTES_LITERAL("v0");
    moq_subscription_t h = MOQ_SUBSCRIPTION_INVALID;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscribe(s, &sc, 1, &h), (int)MOQ_OK);

    moq_stream_ref_t ref = moq_stream_ref_from_u64(0);
    int n_open = 0, n_other = 0;
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) {
        if (a.kind == MOQ_ACTION_OPEN_BIDI_STREAM) {
            n_open++;
            ref = a.u.open_bidi_stream.stream_ref;
            /* Our send half must still be OPEN, or the terminal's close of it
             * would not be owed at all. */
            MOQ_TEST_CHECK_EQ_INT(a.u.open_bidi_stream.fin ? 1 : 0, 0);
            /* And the stream must carry exactly the SUBSCRIBE, so the fixture
             * cannot silently open some other request. */
            moq_buf_reader_t rr;
            moq_buf_reader_init(&rr, a.u.open_bidi_stream.data,
                                a.u.open_bidi_stream.len);
            moq_control_envelope_t oenv;
            MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_envelope(&rr, &oenv),
                                  (int)MOQ_OK);
            MOQ_TEST_CHECK_EQ_U64(oenv.msg_type, (uint64_t)MOQ_D18_SUBSCRIBE);
            MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&rr), (size_t)0);
        } else {
            n_other++;
            TXS_DIAG("TXN local subscribe arm: unexpected action kind %u\n",
                     (unsigned)a.kind);
        }
        moq_action_cleanup(&a);
    }
    MOQ_TEST_CHECK_EQ_INT(n_open, 1);
    MOQ_TEST_CHECK_EQ_INT(n_other, 0);
    MOQ_TEST_CHECK(ref._v != 0);
    int n_ev = 0;
    moq_event_t e;
    while (moq_session_poll_events(s, &e, 1) > 0) { n_ev++; moq_event_cleanup(&e); }
    MOQ_TEST_CHECK_EQ_INT(n_ev, 0);

    /* The ABSOLUTE owner shape before the terminal runs -- not merely whatever
     * baseline the snapshot would otherwise preserve. */
    MOQ_TEST_CHECK_EQ_INT(sub_pool_busy(s), 1);
    const moq_sub_entry_t *se = sub_entry_by_ref(s, ref);
    MOQ_TEST_CHECK(se != NULL);
    if (se) {
        MOQ_TEST_CHECK_EQ_INT((int)se->state, (int)MOQ_SUB_PENDING_SUBSCRIBER);
        MOQ_TEST_CHECK_EQ_INT((int)se->role, (int)MOQ_SUB_ROLE_SUBSCRIBER);
        MOQ_TEST_CHECK_EQ_U64(se->handle._opaque, h._opaque);
        MOQ_TEST_CHECK_EQ_U64(se->rs.ref._v, ref._v);
        MOQ_TEST_CHECK_EQ_INT(se->rs.recv_fin ? 1 : 0, 0);
        MOQ_TEST_CHECK_EQ_SIZE(se->rs.recv_len, (size_t)0);
        check_registered(s, ref, MOQ_REQ_SUBSCRIPTION, (int)(se - s->subs));
    }
    g_goaway_handle = h._opaque;
    g_goaway_family = MOQ_REQUEST_FAMILY_SUBSCRIBE;
    return ref;
}


/* -- ns_sub GOAWAY: the durable-latch control ------------------------- */

/* The namespace-subscription bidi is NOT keyed in the stream-ref request
 * registry -- it is keyed by its own index AND by request id -- and its peer
 * FIN reaches the terminal as the
 * entry's durable `pending_fin` (session_namespace_sub.c:1057) rather than as a
 * call parameter. That makes this origin the control for the GOAWAY pair: the
 * fact IS available here, and the shared terminal consults it -- the ns_sub
 * origin passes `pending_fin` through, so a same-call FIN owes no drain. */

int ns_sub_pool_busy(const moq_session_t *s)
{
    int n = 0;
    for (size_t i = 0; i < s->ns_sub_cap; i++)
        if (s->ns_subs[i].state != MOQ_NS_SUB_FREE) n++;
    return n;
}


void nsg_owner_capture(const moq_session_t *s, moq_stream_ref_t ref,
                       seed_snap_t *o)
{
    req_seed_snap_t *r = &o->req;
    memset(r, 0, sizeof(*r));
    r->busy = ns_sub_pool_busy(s);
    const moq_ns_sub_entry_t *e = ns_by_ref(s, ref);
    r->found = e != NULL;
    if (e) {
        r->state      = (int)e->state;
        r->role       = FIN_OWNER_ROLE_NA;      /* ns_sub carries no role */
        r->generation = e->generation;
        r->handle     = e->handle._opaque;
        r->request_id = e->request_id;
        r->stream_ref = e->stream_ref._v;
        r->fin        = e->pending_fin ? 1 : 0; /* the DURABLE latch */
        r->recv_len   = e->recv_len;
        req_seed_copy_buf(r, e->recv_buf, e->recv_len);
        r->ns_got_response         = e->got_response ? 1 : 0;
        r->ns_parse_complete       = e->parse_complete ? 1 : 0;
        r->ns_closing_remote_error = e->closing_remote_error ? 1 : 0;
        r->ns_local_teardown_pending = e->local_teardown_pending ? 1 : 0;
    }
    /* The stream-ref request-registry key is deliberately empty for this
     * family, but is recorded so a stray one would be caught; the by-ID key is
     * the one this family really holds. */
    req_seed_capture_registry(r, s, ref, r->request_id);
}


void nsg_owner_check(const moq_session_t *s, moq_stream_ref_t ref,
                     const seed_snap_t *want, const char *what)
{
    seed_snap_t now;
    nsg_owner_capture(s, ref, &now);
    req_seed_diff(&now.req, &want->req, what);
}


/* The request id the arm issued, so retirement can be checked against the
 * by-ID registry after the entry that held it is gone. */
uint64_t g_ns_sub_request_id;


void check_nsg_retired(const moq_session_t *s, moq_stream_ref_t ref)
{
    MOQ_TEST_CHECK_EQ_INT(ns_sub_pool_busy(s), 0);
    MOQ_TEST_CHECK(ns_by_ref(s, ref) == NULL);
    check_unregistered(s, ref);
    /* The by-ID key is the real one for this family; a leak there would
     * survive a retirement that only cleared the stream-ref index. */
    moq_request_endpoint_t ip =
        request_registry_find_by_id(s, g_ns_sub_request_id);
    MOQ_TEST_CHECK_EQ_INT((int)ip.kind, (int)MOQ_REQ_NONE);
}


const owner_ops_t nsg_owner_ops = {
    nsg_owner_capture, nsg_owner_check, check_nsg_retired
};


/* A locally-issued SUBSCRIBE_NAMESPACE. Draft-18 rejects an empty prefix and
 * the BOTH interest, so the fixture uses a one-part prefix and NAMESPACE_STATE. */
moq_stream_ref_t arm_goaway_local_ns_sub(moq_session_t *s)
{
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_subscribe_namespace_cfg_t nc;
    moq_subscribe_namespace_cfg_init(&nc);
    nc.track_namespace_prefix = (moq_namespace_t){ parts, 1 };
    nc.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
    moq_ns_sub_handle_t h;
    memset(&h, 0, sizeof(h));
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_subscribe_namespace(s, &nc, 1, &h), (int)MOQ_OK);

    moq_stream_ref_t ref = moq_stream_ref_from_u64(0);
    int n_open = 0, n_other = 0;
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) {
        if (a.kind == MOQ_ACTION_OPEN_BIDI_STREAM) {
            n_open++;
            ref = a.u.open_bidi_stream.stream_ref;
            MOQ_TEST_CHECK_EQ_INT(a.u.open_bidi_stream.fin ? 1 : 0, 0);
            moq_buf_reader_t rr;
            moq_buf_reader_init(&rr, a.u.open_bidi_stream.data,
                                a.u.open_bidi_stream.len);
            moq_control_envelope_t oenv;
            MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_envelope(&rr, &oenv),
                                  (int)MOQ_OK);
            MOQ_TEST_CHECK_EQ_U64(oenv.msg_type,
                                  (uint64_t)MOQ_D18_SUBSCRIBE_NAMESPACE);
            MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&rr), (size_t)0);
            /* And the request itself is the one the fixture asked for. */
            moq_bytes_t dparts[4];
            moq_d18_subscribe_namespace_t dsn;
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_d18_decode_subscribe_namespace(oenv.payload,
                    oenv.payload_len, dparts, 4, &dsn), (int)MOQ_OK);
            MOQ_TEST_CHECK_EQ_SIZE(dsn.track_namespace_prefix.count, (size_t)1);
            failures += txs_check_part_bytes(&dsn.track_namespace_prefix, 0,
                                             "live", 4,
                                             "local ns_sub decoded prefix");
            MOQ_TEST_CHECK_EQ_SIZE(dsn.params.auth_token_count, (size_t)0);
            g_ns_sub_request_id = dsn.request_id;
        } else {
            n_other++;
            TXS_DIAG("TXN local ns_sub arm: unexpected action kind %u\n",
                     (unsigned)a.kind);
        }
        moq_action_cleanup(&a);
    }
    MOQ_TEST_CHECK_EQ_INT(n_open, 1);
    MOQ_TEST_CHECK_EQ_INT(n_other, 0);
    MOQ_TEST_CHECK(ref._v != 0);
    int n_ev = 0;
    moq_event_t e;
    while (moq_session_poll_events(s, &e, 1) > 0) { n_ev++; moq_event_cleanup(&e); }
    MOQ_TEST_CHECK_EQ_INT(n_ev, 0);

    /* The absolute subscriber-side owner shape, and its index linkage. */
    MOQ_TEST_CHECK_EQ_INT(ns_sub_pool_busy(s), 1);
    const moq_ns_sub_entry_t *ne = ns_by_ref(s, ref);
    MOQ_TEST_CHECK(ne != NULL);
    if (ne) {
        MOQ_TEST_CHECK_EQ_INT((int)ne->state,
                              (int)MOQ_NS_SUB_PENDING_SUBSCRIBER);
        MOQ_TEST_CHECK_EQ_U64(ne->handle._opaque, h._opaque);
        MOQ_TEST_CHECK_EQ_U64(ne->stream_ref._v, ref._v);
        MOQ_TEST_CHECK_EQ_INT(ne->pending_fin ? 1 : 0, 0);
        MOQ_TEST_CHECK_EQ_SIZE(ne->recv_len, (size_t)0);
        MOQ_TEST_CHECK_EQ_INT(ne->got_response ? 1 : 0, 0);
        MOQ_TEST_CHECK_EQ_INT(ne->parse_complete ? 1 : 0, 0);
        MOQ_TEST_CHECK_EQ_INT(ne->closing_remote_error ? 1 : 0, 0);
        MOQ_TEST_CHECK_EQ_INT((int)ne->namespace_interest,
                              (int)MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE);
        /* The wire request id, the owner and the by-ID registry all agree. */
        MOQ_TEST_CHECK_EQ_U64(ne->request_id, g_ns_sub_request_id);
        moq_request_endpoint_t ip =
            request_registry_find_by_id(s, g_ns_sub_request_id);
        MOQ_TEST_CHECK_EQ_INT((int)ip.kind, (int)MOQ_REQ_NAMESPACE_SUB);
        MOQ_TEST_CHECK_EQ_INT(ip.slot, (int)(ne - s->ns_subs));
        MOQ_TEST_CHECK_EQ_INT(ip.has_request_id ? 1 : 0, 1);
        MOQ_TEST_CHECK_EQ_U64(ip.request_id, g_ns_sub_request_id);
    }
    /* Deliberately absent from the stream-ref request registry. */
    check_unregistered(s, ref);
    g_goaway_handle = h._opaque;
    g_goaway_family = MOQ_REQUEST_FAMILY_NS_SUB;
    return ref;
}


/* The durable-latch discriminator. FIN fixes the stream's final size, so peer
 * bytes may never follow it; the terminal is instead reached by an ACTION-
 * blocked retry, and the empty re-feed is an internal retry rather than
 * post-FIN peer data. A corrected implementation consumes the durable latch and
 * takes no drain reference at all, so the exhausted ring cannot refuse it. */
void run_ns_goaway_latched(void)
{
    const char *what = "ns_sub goaway latched";
    moq_session_t *s = make_session_full(MOQ_PERSPECTIVE_CLIENT, 0, 1);
    if (!s) return;
    moq_stream_ref_t ref = arm_goaway_local_ns_sub(s);
    if (ref._v == 0) { moq_session_destroy(s); return; }

    /* One action slot, occupied by a benign close on an unrelated stream: the
     * terminal's own close cannot be queued, so it must refuse. */
    moq_stream_ref_t blocker = moq_stream_ref_from_u64(0x5000);
    MOQ_TEST_CHECK_EQ_INT((int)queue_close_bidi(s, blocker), (int)MOQ_OK);
    MOQ_TEST_CHECK(action_queue_full(s));

    seed_snap_t owner0;
    memset(&owner0, 0, sizeof(owner0));
    nsg_owner_capture(s, ref, &owner0);
    drain_snap_t ring_before;
    drain_snap(s, &ring_before);
    txs_snapshot_t before;
    txs_capture(s, &ref, 1, &before);
    expect_after_call_prepare(&before);

    uint8_t m[192];
    size_t n = enc_goaway_request_uri(m, sizeof(m));
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, m, n, true, 1),
        (int)MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);

    /* The FIN is now DURABLE on the owner, the GOAWAY is retained byte for
     * byte, and nothing else moved. */
    seed_snap_t owner_blocked = owner0;
    owner_blocked.req.fin = 1;
    req_seed_append(&owner_blocked.req, m, n);
    nsg_owner_check(s, ref, &owner_blocked, what);
    failures += txs_check_eq(s, &ref, 1, &before, what);
    {
        const moq_ns_sub_entry_t *ne = ns_by_ref(s, ref);
        MOQ_TEST_CHECK(ne != NULL && ne->pending_fin);
    }
    drain_snap_t now;
    drain_snap(s, &now);
    failures += drain_multiset_equals(&now, &ring_before, what);

    /* Only the blocker is queued, and no event surfaced. */
    {
        txs_norm_vec_t got, want;
        txs_norm_init(&got);
        txs_norm_init(&want);
        collect_events(s, &goaway_hooks, &got);
        MOQ_TEST_CHECK_EQ_SIZE(got.count, (size_t)0);
        collect_actions(s, &goaway_hooks, &got);
        want_close_bidi(&want, blocker);
        failures += txs_norm_equals(&got, &want, "ns_sub goaway blocked");
        txs_norm_free(&got);
        txs_norm_free(&want);
    }

    /* Capacity returns, and the ring is then exhausted: with the peer's close
     * already durable the retry needs no reference at all. */
    fill_drain_ring(s);
    drain_snap_t ring_full;
    drain_snap(s, &ring_full);

    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, false, 1),
        (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
    check_nsg_retired(s, ref);
    {
        txs_norm_vec_t got, want;
        txs_norm_init(&got);
        txs_norm_init(&want);
        collect_events(s, &goaway_hooks, &got);
        collect_actions(s, &goaway_hooks, &got);
        want_goaway_output_uri(&want, ref);
        failures += txs_norm_equals(&got, &want, "ns_sub goaway latched output");
        txs_norm_free(&got);
        txs_norm_free(&want);
    }
    drain_snap_t after;
    drain_snap(s, &after);
    failures += drain_multiset_equals(&after, &ring_full,
                                      "ns_sub goaway latched ring");
    moq_session_destroy(s);
}
