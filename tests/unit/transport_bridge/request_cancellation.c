#include "test_cases.h"

/* -- Peer STOP_SENDING on a request bidi (draft-18 §3.3.2) -------------
 *
 * STOP_SENDING targets one direction: our SENDING part of the stream, for
 * which RFC 9000 §3.5 asks us to send RESET_STREAM. It does not terminate the
 * peer's sending part of a bidi. §3.3.2 additionally says an endpoint that
 * rejects a request without application processing SHOULD send a
 * REQUEST_ERROR and FIN the stream -- which can only arrive if we kept the
 * receive half of the request bidi alive after its STOP.
 *
 * So a peer STOP on a LOCALLY-opened request bidi must close our send half and
 * nothing else. A peer STOP on a PEER-opened request bidi is the requester
 * cancelling the request it made, which is delivered to the session.
 *
 * Every one of these decisions is keyed on the STOP fact itself, never on the
 * ordinary FIN state: the no-STOP control below pins that an ordinary FIN
 * keeps its existing mapping, tombstone and output behaviour.
 */

/* The COMPLETE request shape the fixture issues, declared here and compared
 * against the wire rather than adopted from it. Designated initializers keep a
 * new field from silently shifting an existing fixture's values. */
typedef struct {
    uint64_t    request_id;
    const char *ns;
    const char *track;
} req_shape_t;


/* One draft-18 SUBSCRIBE, decoded and compared across EVERY surfaced field:
 * the request id, the namespace, the track name, the one filter parameter this
 * fixture asks for (with its irrelevant position scalars at their normalized
 * zeros), and the absence of every other representable message parameter --
 * including FORWARD, whose omission means the protocol default applies. Every
 * span is bounds- and NULL-guarded before it is read. */
static int check_subscribe_bytes(const uint8_t *data, size_t len,
                                 const req_shape_t *want, const char *what)
{
    int bad = 0;
    if (!data || len == 0) {
        fprintf(stderr, "FAIL: %s: no request bytes\n", what);
        return 1;
    }
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, data, len);
    moq_control_envelope_t env;
    if (moq_d18_decode_envelope(&r, &env) != MOQ_OK) {
        fprintf(stderr, "FAIL: %s: request is not a decodable envelope\n", what);
        return 1;
    }
    if (env.msg_type != MOQ_D18_SUBSCRIBE) {
        fprintf(stderr, "FAIL: %s: msg type 0x%llx, expected SUBSCRIBE\n", what,
                (unsigned long long)env.msg_type);
        bad++;
    }
    if (moq_buf_reader_remaining(&r) != 0) {
        fprintf(stderr, "FAIL: %s: %zu trailing bytes after the request\n",
                what, moq_buf_reader_remaining(&r));
        bad++;
    }
    moq_bytes_t parts[MOQ_DECODED_MAX_NAMESPACE_PARTS];
    moq_d18_subscribe_t sub;
    if (moq_d18_decode_subscribe(env.payload, env.payload_len, parts,
                                 MOQ_DECODED_MAX_NAMESPACE_PARTS,
                                 &sub) != MOQ_OK) {
        fprintf(stderr, "FAIL: %s: SUBSCRIBE body did not decode\n", what);
        return bad + 1;
    }
    if (sub.track_namespace.count != 1 || sub.track_namespace.parts == NULL) {
        fprintf(stderr, "FAIL: %s: namespace has %zu parts, expected 1\n", what,
                sub.track_namespace.count);
        return bad + 1;
    }
    size_t ns_len = strlen(want->ns);
    if (sub.track_namespace.parts[0].len != ns_len ||
        sub.track_namespace.parts[0].data == NULL ||
        memcmp(sub.track_namespace.parts[0].data, want->ns, ns_len) != 0) {
        fprintf(stderr, "FAIL: %s: namespace part differs from '%s'\n", what,
                want->ns);
        bad++;
    }
    size_t tn_len = strlen(want->track);
    if (sub.track_name.len != tn_len || sub.track_name.data == NULL ||
        memcmp(sub.track_name.data, want->track, tn_len) != 0) {
        fprintf(stderr, "FAIL: %s: track name differs from '%s'\n", what,
                want->track);
        bad++;
    }
    if (sub.request_id != want->request_id) {
        fprintf(stderr, "FAIL: %s: request id %llu, expected %llu\n", what,
                (unsigned long long)sub.request_id,
                (unsigned long long)want->request_id);
        bad++;
    }
    /* §5.1.2 filter type 2 is LARGEST_OBJECT, the shape the fixture asks for;
     * the absolute-range scalars belong to types 3/4 and must be normalized. */
    if (!sub.params.has_filter || sub.params.filter_type != 2u) {
        fprintf(stderr, "FAIL: %s: filter type %u (present=%d), expected 2\n",
                what, (unsigned)sub.params.filter_type,
                (int)sub.params.has_filter);
        bad++;
    }
    if (sub.params.filter_start_group != 0 ||
        sub.params.filter_start_object != 0 ||
        sub.params.filter_end_group != 0) {
        fprintf(stderr, "FAIL: %s: filter position scalars are not zero\n",
                what);
        bad++;
    }
    /* Every other representable parameter is absent. */
    if (sub.params.has_forward) {
        fprintf(stderr, "FAIL: %s: FORWARD present (%u); the fixture relies on"
                " the protocol default\n", what,
                (unsigned)sub.params.forward);
        bad++;
    }
    if (sub.params.has_subscriber_priority) {
        fprintf(stderr, "FAIL: %s: SUBSCRIBER_PRIORITY present\n", what);
        bad++;
    }
    if (sub.params.has_group_order) {
        fprintf(stderr, "FAIL: %s: GROUP_ORDER present\n", what);
        bad++;
    }
    if (sub.params.has_expires) {
        fprintf(stderr, "FAIL: %s: EXPIRES present\n", what);
        bad++;
    }
    if (sub.params.has_largest) {
        fprintf(stderr, "FAIL: %s: LARGEST_OBJECT response param present\n",
                what);
        bad++;
    }
    if (sub.params.has_object_delivery_timeout) {
        fprintf(stderr, "FAIL: %s: OBJECT_DELIVERY_TIMEOUT present\n", what);
        bad++;
    }
    if (sub.params.has_subgroup_delivery_timeout) {
        fprintf(stderr, "FAIL: %s: SUBGROUP_DELIVERY_TIMEOUT present\n", what);
        bad++;
    }
    if (sub.params.auth_token_count != 0) {
        fprintf(stderr, "FAIL: %s: %zu authorization tokens, expected 0\n",
                what, sub.params.auth_token_count);
        bad++;
    }
    if (sub.params.has_new_group_request) {
        fprintf(stderr, "FAIL: %s: NEW_GROUP_REQUEST present\n", what);
        bad++;
    }
    return bad;
}


/* An unblocked local request arm: exactly OPEN_BIDI then WRITE on the same
 * transport id, the write carrying exactly the declared request with no FIN. */
static int classify_local_request(fake_endpoint_t *ep, const req_shape_t *want,
                                  uint64_t *out_sid, const char *what)
{
    int bad = 0;
    if (ep->count != 2) {
        fprintf(stderr, "FAIL: %s: %zu setup ops, expected 2\n", what,
                ep->count);
        for (size_t i = 0; i < ep->count; i++)
            fprintf(stderr, "      [%zu] kind=%d sid=%llu\n", i,
                    (int)ep->ops[i].kind,
                    (unsigned long long)ep->ops[i].stream_id);
        fake_endpoint_clear_ops(ep);
        return bad + 1;
    }
    if (ep->ops[0].kind != FAKE_OP_OPEN_BIDI ||
        ep->ops[1].kind != FAKE_OP_WRITE) {
        fprintf(stderr, "FAIL: %s: setup ops are kind %d,%d, expected"
                " OPEN_BIDI,WRITE\n", what, (int)ep->ops[0].kind,
                (int)ep->ops[1].kind);
        bad++;
    }
    if (ep->ops[0].stream_id != ep->ops[1].stream_id) {
        fprintf(stderr, "FAIL: %s: open on %llu but write on %llu\n", what,
                (unsigned long long)ep->ops[0].stream_id,
                (unsigned long long)ep->ops[1].stream_id);
        bad++;
    }
    if (ep->ops[1].fin) {
        fprintf(stderr, "FAIL: %s: the request write carried FIN\n", what);
        bad++;
    }
    bad += check_subscribe_bytes(ep->ops[1].data, ep->ops[1].data_len, want,
                                 what);
    *out_sid = ep->ops[0].stream_id;
    fake_endpoint_clear_ops(ep);
    return bad;
}


typedef struct {
    test_pair_t        tp;
    uint64_t           bidi;    /* transport id of the local request bidi */
    moq_stream_ref_t   ref;
    moq_subscription_t sub;
} local_req_fixture_t;


static int local_subscribe(test_pair_t *tp, const char *ns, const char *track,
                           moq_subscription_t *out)
{
    moq_bytes_t ns_parts[1];
    ns_parts[0].data = (const uint8_t *)ns;
    ns_parts[0].len = strlen(ns);
    moq_namespace_t nsp = { ns_parts, 1 };
    moq_subscribe_cfg_t cfg;
    moq_subscribe_cfg_init(&cfg);
    cfg.track_namespace = nsp;
    cfg.track_name.data = (const uint8_t *)track;
    cfg.track_name.len = strlen(track);
    cfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    return moq_session_subscribe(tp->client, &cfg, 0, out) == MOQ_OK ? 0 : -1;
}


/* A locally issued draft-18 SUBSCRIBE whose request bidi the client bridge has
 * opened and written, with the server deliberately never answering: the peer's
 * send half is still open, so the response is the test's to deliver. */
static int local_request_bidi_fixture_ex(local_req_fixture_t *f,
                                         bool block_write)
{
    memset(f, 0, sizeof(*f));
    if (d18_pair_init(&f->tp, 0) < 0) return -1;
    moq_session_start(f->tp.client, 0);
    moq_session_start(f->tp.server, 0);
    d18_shuttle_until_quiescent(&f->tp, 30, 0);

    moq_event_t ev;
    while (moq_session_poll_events(f->tp.client, &ev, 1) > 0)
        moq_event_cleanup(&ev);
    while (moq_session_poll_events(f->tp.server, &ev, 1) > 0)
        moq_event_cleanup(&ev);

    if (local_subscribe(&f->tp, "live", "video", &f->sub) < 0) goto fail;

    fake_endpoint_clear_ops(&f->tp.client_ep);
    /* Blocking the write leaves the request itself queued on the bidi the open
     * already created -- the "request write still pending" state. */
    f->tp.client_ep.block_write = block_write;
    if (held_bridge_service(f->tp.client_bridge, 0) != MOQ_OK)
        goto fail;

    /* A fresh draft-18 CLIENT starts at request id 0 (profile_d18.c:35-38) and
     * commits by two (:410,:419), so the single-request fixtures expect 0. */
    static const req_shape_t k_shape = {
        .request_id = 0, .ns = "live", .track = "video" };
    if (block_write) {
        /* The endpoint saw exactly the open; the request itself is the one
         * pending copied write, tied to the same id and ref. Its retained
         * bytes are decoded, not adopted. */
        if (f->tp.client_ep.count != 1 ||
            f->tp.client_ep.ops[0].kind != FAKE_OP_OPEN_BIDI) {
            fprintf(stderr, "NF setup: blocked arm produced %zu ops\n",
                    f->tp.client_ep.count);
            goto fail;
        }
        f->bidi = f->tp.client_ep.ops[0].stream_id;
        fake_endpoint_clear_ops(&f->tp.client_ep);
        if (f->tp.client_bridge->pending_count != 1 ||
            f->tp.client_bridge->pending[0].kind != PENDING_COPIED_WRITE ||
            f->tp.client_bridge->pending[0].stream_id != f->bidi)
            goto fail;
        if (check_subscribe_bytes(f->tp.client_bridge->pending[0].data,
                                  f->tp.client_bridge->pending[0].data_len,
                                  &k_shape, "NF setup blocked request") != 0)
            goto fail;
    } else {
        if (classify_local_request(&f->tp.client_ep, &k_shape, &f->bidi,
                                   "NF setup request") != 0)
            goto fail;
    }
    if (!f->bidi) goto fail;

    bridge_stream_entry_t *e = bridge_find_by_id(f->tp.client_bridge, f->bidi);
    if (!e || e->kind != BRIDGE_STREAM_BIDI ||
        e->origin != BRIDGE_ORIGIN_LOCAL || !e->active) goto fail;
    f->ref = e->ref;
    if (block_write &&
        f->tp.client_bridge->pending[0].stream_ref._v != f->ref._v) goto fail;
    return 0;

fail:
    test_pair_destroy(&f->tp);
    return -1;
}


static int local_request_bidi_fixture(local_req_fixture_t *f)
{
    return local_request_bidi_fixture_ex(f, false);
}


/* 1. STOP then REQUEST_ERROR+FIN on the same stream completes the request. */
int test_local_bidi_stop_keeps_response_half(void)
{
    int failures = 0;
    local_req_fixture_t f;
    if (local_request_bidi_fixture(&f) < 0) { failures++; return failures; }

    MOQ_TEST_CHECK(request_registry_find_by_streamref(f.tp.client, f.ref).kind
                   == MOQ_REQ_SUBSCRIPTION);

    MOQ_TEST_CHECK(moq_transport_bridge_on_peer_stop_sending(
                       f.tp.client_bridge, f.bidi, 0x1, 0) == MOQ_OK);

    /* The callback itself touches the transport not at all: the reset is
     * queued, never issued re-entrantly. */
    failures += ep_expect_none(&f.tp.client_ep, "case 1 callback");

    bridge_stream_entry_t *e = bridge_find_by_id(f.tp.client_bridge, f.bidi);
    MOQ_TEST_CHECK(e != NULL && e->active);
    if (e) {
        MOQ_TEST_CHECK(e->peer_stop_received);
        MOQ_TEST_CHECK(e->local_send_closed);
        MOQ_TEST_CHECK(!e->peer_send_closed);
    }
    MOQ_TEST_CHECK(request_registry_find_by_streamref(f.tp.client, f.ref).kind
                   == MOQ_REQ_SUBSCRIPTION);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(f.tp.client_bridge));
    MOQ_TEST_CHECK(moq_session_state(f.tp.client) == MOQ_SESS_ESTABLISHED);

    /* First service: exactly the declared RESET and nothing else. */
    MOQ_TEST_CHECK(held_bridge_service(f.tp.client_bridge, 0) ==
                   MOQ_OK);
    {
        ep_rec_t want[1] = { ep_reset(f.bidi, 0x1) };
        failures += ep_expect(&f.tp.client_ep, want, 1, "case 1 service");
    }

    /* The peer rejects the request on its still-open send half (§3.3.2). */
    uint8_t msg[128];
    size_t n = encode_request_error(msg, sizeof(msg));
    MOQ_TEST_CHECK(n > 0);
    MOQ_TEST_CHECK(moq_transport_bridge_on_peer_bidi_bytes(
                       f.tp.client_bridge, f.bidi, msg, n, true, 0) == MOQ_OK);

    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(f.tp.client_bridge));
    MOQ_TEST_CHECK(moq_session_state(f.tp.client) == MOQ_SESS_ESTABLISHED);

    moq_event_t ev;
    int errors = 0, other = 0;
    while (moq_session_poll_events(f.tp.client, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_ERROR) {
            errors++;
            MOQ_TEST_CHECK(ev.u.subscribe_error.sub._opaque ==
                           f.sub._opaque);
            MOQ_TEST_CHECK((uint64_t)ev.u.subscribe_error.error_code ==
                           LSTOP_ERR_CODE);
            MOQ_TEST_CHECK(ev.u.subscribe_error.retry_after_ms ==
                           LSTOP_ERR_RETRY);
            MOQ_TEST_CHECK_EQ_SIZE(ev.u.subscribe_error.reason.len,
                                   strlen(LSTOP_ERR_REASON));
            MOQ_TEST_CHECK(ev.u.subscribe_error.reason.data != NULL);
            if (ev.u.subscribe_error.reason.data != NULL &&
                ev.u.subscribe_error.reason.len == strlen(LSTOP_ERR_REASON))
                MOQ_TEST_CHECK(memcmp(ev.u.subscribe_error.reason.data,
                                      LSTOP_ERR_REASON,
                                      strlen(LSTOP_ERR_REASON)) == 0);
        } else {
            other++;
        }
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK(errors == 1);
    MOQ_TEST_CHECK(other == 0);

    /* The response terminal produces no endpoint output of its own, and the
     * mapping and owner retire. */
    failures += ep_expect_none(&f.tp.client_ep, "case 1 response terminal");
    MOQ_TEST_CHECK(held_bridge_service(f.tp.client_bridge, 0) ==
                   MOQ_OK);
    failures += ep_expect_none(&f.tp.client_ep, "case 1 service after terminal");
    MOQ_TEST_CHECK(request_registry_find_by_streamref(f.tp.client, f.ref).kind
                   == MOQ_REQ_NONE);
    failures += check_mapping_gone(f.tp.client_bridge, f.bidi, f.ref,
                                   "case 1 terminal");

    /* Repeated service stays inert, and the mapping stays gone by BOTH
     * identities. */
    MOQ_TEST_CHECK(held_bridge_service(f.tp.client_bridge, 0) ==
                   MOQ_OK);
    failures += ep_expect_none(&f.tp.client_ep, "case 1 repeated service");
    failures += check_mapping_gone(f.tp.client_bridge, f.bidi, f.ref,
                                   "case 1 repeated service");

    test_pair_destroy(&f.tp);
    return failures;
}


/* 2. Duplicate STOP is idempotent: still one RESET, carrying the FIRST
 *    STOP's code (the reset is already owed; a repeat cannot re-arm it). */
int test_local_bidi_stop_duplicate_is_idempotent(void)
{
    int failures = 0;
    local_req_fixture_t f;
    if (local_request_bidi_fixture(&f) < 0) { failures++; return failures; }

    MOQ_TEST_CHECK(moq_transport_bridge_on_peer_stop_sending(
                       f.tp.client_bridge, f.bidi, 0x1, 0) == MOQ_OK);
    MOQ_TEST_CHECK(moq_transport_bridge_on_peer_stop_sending(
                       f.tp.client_bridge, f.bidi, 0x4, 0) == MOQ_OK);
    failures += ep_expect_none(&f.tp.client_ep, "case 2 callbacks");

    MOQ_TEST_CHECK(held_bridge_service(f.tp.client_bridge, 0) ==
                   MOQ_OK);
    {
        ep_rec_t want[1] = { ep_reset(f.bidi, 0x1) };
        failures += ep_expect(&f.tp.client_ep, want, 1, "case 2 service");
    }
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(f.tp.client_bridge));

    /* A third STOP after the reset was sent adds nothing. */
    MOQ_TEST_CHECK(moq_transport_bridge_on_peer_stop_sending(
                       f.tp.client_bridge, f.bidi, 0x5, 0) == MOQ_OK);
    MOQ_TEST_CHECK(held_bridge_service(f.tp.client_bridge, 0) ==
                   MOQ_OK);
    failures += ep_expect_none(&f.tp.client_ep, "case 2 third stop");

    bridge_stream_entry_t *e = bridge_find_by_id(f.tp.client_bridge, f.bidi);
    MOQ_TEST_CHECK(e != NULL && e->active);
    MOQ_TEST_CHECK(request_registry_find_by_streamref(f.tp.client, f.ref).kind
                   == MOQ_REQ_SUBSCRIPTION);

    test_pair_destroy(&f.tp);
    return failures;
}


/* 3. Blocked RESET stays retryable while the targeted stream's queued output
 *    is discarded -- freeing exactly its own buffer -- and unrelated records
 *    keep their order, their contents and their ownership.
 *
 * The FIFO cannot hold two items through the drain loop: bridge_drain_actions
 * stops as soon as one is pending, and a later pass re-breaks at the retry. So
 * the unrelated records are constructed directly, each against a LIVE bridge
 * mapping created by a real subscribe, and one of them owns bytes taken from
 * the same accounting allocator the bridge itself uses -- which is what lets
 * the compaction be measured rather than assumed.
 */
int test_local_bidi_stop_blocked_reset_retries(void)
{
    int failures = 0;
    fp_alloc_state_t fs = {0};
    moq_alloc_t balloc = fp_allocator(&fs);

    test_pair_t tp;
    memset(&tp, 0, sizeof(tp));

    moq_session_cfg_t ccfg, scfg;
    moq_session_cfg_init_sized(&ccfg, sizeof(ccfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_CLIENT);
    ccfg.version = MOQ_VERSION_DRAFT_18;
    ccfg.send_request_capacity = true;
    ccfg.initial_request_capacity = 10;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_SERVER);
    scfg.version = MOQ_VERSION_DRAFT_18;
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 10;
    if (moq_session_create(&ccfg, 0, &tp.client) < 0) { failures++; return failures; }
    if (moq_session_create(&scfg, 0, &tp.server) < 0) {
        moq_session_destroy(tp.client); failures++; return failures;
    }
    held_endpoint_init(&tp.client_ep, 1000, 2000);
    held_endpoint_init(&tp.server_ep, 3000, 4000);

    moq_transport_bridge_cfg_t cbcfg, sbcfg;
    moq_transport_bridge_cfg_init(&cbcfg, &balloc);
    moq_transport_bridge_cfg_init(&sbcfg, moq_alloc_default());
    if (held_bridge_create(&cbcfg, tp.client, &tp.client_ep.vtable,
                                    &tp.client_ep, &tp.client_bridge) < 0 ||
        held_bridge_create(&sbcfg, tp.server, &tp.server_ep.vtable,
                                    &tp.server_ep, &tp.server_bridge) < 0) {
        moq_session_destroy(tp.server); moq_session_destroy(tp.client);
        failures++; return failures;
    }

    moq_session_start(tp.client, 0);
    moq_session_start(tp.server, 0);
    d18_shuttle_until_quiescent(&tp, 30, 0);
    {
        moq_event_t ev;
        while (moq_session_poll_events(tp.client, &ev, 1) > 0)
            moq_event_cleanup(&ev);
        while (moq_session_poll_events(tp.server, &ev, 1) > 0)
            moq_event_cleanup(&ev);
    }

    /* Two live unrelated request bidis, written and flushed. */
    moq_subscription_t sub_b, sub_c, sub_a;
    /* Three requests in issue order on one client: 0, 2, 4. */
    static const req_shape_t k_b = {
        .request_id = 0, .ns = "live", .track = "audio" };
    static const req_shape_t k_c = {
        .request_id = 2, .ns = "live", .track = "text" };
    static const req_shape_t k_a = {
        .request_id = 4, .ns = "live", .track = "video" };
    uint64_t sid_b = 0, sid_c = 0;
    /* Each request is serviced and classified on its own, so a mapping is
     * bound to its own OPEN/WRITE pair rather than to a scan of the queue. */
    MOQ_TEST_CHECK(local_subscribe(&tp, k_b.ns, k_b.track, &sub_b) == 0);
    MOQ_TEST_CHECK(held_bridge_service(tp.client_bridge, 0) == MOQ_OK);
    failures += classify_local_request(&tp.client_ep, &k_b, &sid_b, "case 3 arm B");
    MOQ_TEST_CHECK(local_subscribe(&tp, k_c.ns, k_c.track, &sub_c) == 0);
    MOQ_TEST_CHECK(held_bridge_service(tp.client_bridge, 0) == MOQ_OK);
    failures += classify_local_request(&tp.client_ep, &k_c, &sid_c, "case 3 arm C");
    MOQ_TEST_CHECK(sid_b != 0 && sid_c != 0 && sid_b != sid_c);
    bridge_stream_entry_t *eb = bridge_find_by_id(tp.client_bridge, sid_b);
    bridge_stream_entry_t *ec = bridge_find_by_id(tp.client_bridge, sid_c);
    MOQ_TEST_CHECK(eb != NULL && eb->active);
    MOQ_TEST_CHECK(ec != NULL && ec->active);

    /* The targeted request: its own write blocks and stays pending. */
    tp.client_ep.block_write = true;
    MOQ_TEST_CHECK(local_subscribe(&tp, k_a.ns, k_a.track, &sub_a) == 0);
    MOQ_TEST_CHECK(held_bridge_service(tp.client_bridge, 0) == MOQ_OK);
    uint64_t sid_a = 0;
    {
        ep_rec_t want[1];
        memset(want, 0, sizeof(want));
        want[0].kind = FAKE_OP_OPEN_BIDI;
        want[0].stream_id = tp.client_ep.count ? tp.client_ep.ops[0].stream_id
                                               : 0;
        sid_a = want[0].stream_id;
        failures += ep_expect(&tp.client_ep, want, 1, "case 3 arm A open");
    }
    MOQ_TEST_CHECK(sid_a != 0 && sid_a != sid_b && sid_a != sid_c);
    bridge_stream_entry_t *ea = bridge_find_by_id(tp.client_bridge, sid_a);
    MOQ_TEST_CHECK(ea != NULL && ea->active);
    MOQ_TEST_CHECK(tp.client_bridge->pending_count == 1);
    MOQ_TEST_CHECK(tp.client_bridge->pending[0].kind == PENDING_COPIED_WRITE);
    MOQ_TEST_CHECK(tp.client_bridge->pending[0].stream_id == sid_a);
    if (ea) MOQ_TEST_CHECK(tp.client_bridge->pending[0].stream_ref._v ==
                           ea->ref._v);
    failures += check_subscribe_bytes(tp.client_bridge->pending[0].data,
                                      tp.client_bridge->pending[0].data_len,
                                      &k_a, "case 3 arm A request");

    /* Two unrelated records, one owning bytes from the bridge's allocator. */
    static const uint8_t k_unrelated[] = { 0xA1, 0xB2, 0xC3, 0xD4, 0xE5 };
    uint8_t *owned = (uint8_t *)balloc.alloc(sizeof(k_unrelated), balloc.ctx);
    MOQ_TEST_CHECK(owned != NULL);
    if (!owned) { test_pair_destroy(&tp); failures++; return failures; }
    memcpy(owned, k_unrelated, sizeof(k_unrelated));

    {
        bridge_pending_item_t *q = tp.client_bridge->pending;
        q[1] = q[0];                        /* targeted write moves to index 1 */
        memset(&q[0], 0, sizeof(q[0]));
        q[0].kind = PENDING_COPIED_WRITE;
        q[0].stream_id = sid_b;
        q[0].stream_ref = eb->ref;
        q[0].data = owned;
        q[0].data_len = sizeof(k_unrelated);
        memset(&q[2], 0, sizeof(q[2]));
        q[2].kind = PENDING_CLOSE_BIDI_FIN;
        q[2].stream_id = sid_c;
        q[2].stream_ref = ec->ref;
        tp.client_bridge->pending_count = 3;
    }
    const int64_t balance_before = fs.balance;
    const uint8_t *targeted_bytes = tp.client_bridge->pending[1].data;
    MOQ_TEST_CHECK(targeted_bytes != NULL);

    tp.client_ep.block_reset = true;
    MOQ_TEST_CHECK(moq_transport_bridge_on_peer_stop_sending(
                       tp.client_bridge, sid_a, 0x1, 0) == MOQ_OK);
    failures += ep_expect_none(&tp.client_ep, "case 3 callback");

    /* Exactly the targeted item left; the unrelated records kept their order,
     * their complete contents and their buffer; the RESET took the tail. */
    MOQ_TEST_CHECK(tp.client_bridge->pending_count == 3);
    if (tp.client_bridge->pending_count == 3) {
        bridge_pending_item_t *q = tp.client_bridge->pending;
        MOQ_TEST_CHECK(q[0].kind == PENDING_COPIED_WRITE);
        MOQ_TEST_CHECK(q[0].stream_id == sid_b);
        MOQ_TEST_CHECK(q[0].stream_ref._v == eb->ref._v);
        MOQ_TEST_CHECK(q[0].data == owned);
        MOQ_TEST_CHECK_EQ_SIZE(q[0].data_len, sizeof(k_unrelated));
        if (q[0].data)
            MOQ_TEST_CHECK(memcmp(q[0].data, k_unrelated,
                                  sizeof(k_unrelated)) == 0);
        MOQ_TEST_CHECK(q[1].kind == PENDING_CLOSE_BIDI_FIN);
        MOQ_TEST_CHECK(q[1].stream_id == sid_c);
        MOQ_TEST_CHECK(q[1].stream_ref._v == ec->ref._v);
        MOQ_TEST_CHECK(q[1].data == NULL);
        MOQ_TEST_CHECK(q[2].kind == PENDING_RESET_STREAM);
        MOQ_TEST_CHECK(q[2].stream_id == sid_a);
        MOQ_TEST_CHECK(q[2].error_code == 0x1);
    }
    /* Exactly one block freed: the targeted write's own buffer. */
    MOQ_TEST_CHECK(fs.balance == balance_before - 1);
    MOQ_TEST_CHECK(fp_sticky_clean(&fs, "case 3 compaction") == 0);

    /* Reset still blocked: the unrelated work goes out, the target does not. */
    tp.client_ep.block_write = false;
    MOQ_TEST_CHECK(held_bridge_service(tp.client_bridge, 0) == MOQ_OK);
    {
        ep_rec_t want[2];
        memset(want, 0, sizeof(want));
        want[0].kind = FAKE_OP_WRITE;
        want[0].stream_id = sid_b;
        want[0].data = k_unrelated;
        want[0].data_len = sizeof(k_unrelated);
        want[0].check_fin = true;
        want[0].fin = false;
        want[1].kind = FAKE_OP_WRITE;
        want[1].stream_id = sid_c;
        want[1].check_fin = true;
        want[1].fin = true;
        failures += ep_expect(&tp.client_ep, want, 2, "case 3 blocked service");
    }
    MOQ_TEST_CHECK(tp.client_bridge->pending_count == 1);
    MOQ_TEST_CHECK(tp.client_bridge->pending[0].kind == PENDING_RESET_STREAM);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.client_bridge));

    /* The response mapping survived the blocked reset. */
    ea = bridge_find_by_id(tp.client_bridge, sid_a);
    MOQ_TEST_CHECK(ea != NULL && ea->active);
    MOQ_TEST_CHECK(request_registry_find_by_streamref(tp.client, ea->ref).kind
                   == MOQ_REQ_SUBSCRIPTION);

    tp.client_ep.block_reset = false;
    MOQ_TEST_CHECK(held_bridge_service(tp.client_bridge, 0) == MOQ_OK);
    {
        ep_rec_t want[1] = { ep_reset(sid_a, 0x1) };
        failures += ep_expect(&tp.client_ep, want, 1, "case 3 reset service");
    }
    MOQ_TEST_CHECK(tp.client_bridge->pending_count == 0);

    test_pair_destroy(&tp);
    MOQ_TEST_CHECK(br_alloc_zeroed(&fs, "case 3 teardown") == 0);
    return failures;
}


/* 4. Control: a PEER-opened request bidi keeps requester-cancellation, and its
 *    own physical terminal -- RESET or FIN -- then retires the stream exactly
 *    once. The STOP retires the request owner, while the session keeps the
 *    peer's still-open sending direction as a drain reference, so its later
 *    legal empty FIN is absorbed rather than read as a new request. */
static int peer_bidi_stop_then_terminal(bool use_fin)
{
    int failures = 0;
    test_pair_t tp;
    uint64_t bidi = 0;
    if (d18_request_bidi_fixture(&tp, &bidi) < 0) { failures++; return failures; }

    bridge_stream_entry_t *e = bridge_find_by_id(tp.client_bridge, bidi);
    MOQ_TEST_CHECK(e != NULL && e->kind == BRIDGE_STREAM_BIDI);
    if (e) MOQ_TEST_CHECK(e->origin == BRIDGE_ORIGIN_PEER);
    moq_stream_ref_t bref = e ? e->ref : moq_stream_ref_from_u64(0);

    moq_event_t ev;
    moq_subscription_t want_sub = MOQ_SUBSCRIPTION_INVALID;
    while (moq_session_poll_events(tp.client, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST)
            want_sub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK(moq_subscription_is_valid(want_sub));
    fake_endpoint_clear_ops(&tp.client_ep);

    MOQ_TEST_CHECK(moq_transport_bridge_on_peer_stop_sending(
                       tp.client_bridge, bidi, 0x1, 0) == MOQ_OK);
    failures += ep_expect_none(&tp.client_ep, "case 4 callback");
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.client_bridge));
    MOQ_TEST_CHECK(moq_session_state(tp.client) == MOQ_SESS_ESTABLISHED);

    /* The cancellation reaches the application on the same subscription and
     * retires the request owner -- the peer-origin behaviour is unchanged. */
    int cancels = 0;
    while (moq_session_poll_events(tp.client, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_UNSUBSCRIBED) {
            cancels++;
            MOQ_TEST_CHECK(ev.u.unsubscribed.sub._opaque ==
                           want_sub._opaque);
        }
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK(cancels == 1);
    MOQ_TEST_CHECK(request_registry_find_by_streamref(tp.client, bref).kind ==
                   MOQ_REQ_NONE);

    /* Our response send half is reset exactly once (RFC 9000 §3.5). */
    MOQ_TEST_CHECK(held_bridge_service(tp.client_bridge, 0) == MOQ_OK);
    {
        ep_rec_t want[1] = { ep_reset(bidi, 0x1) };
        failures += ep_expect(&tp.client_ep, want, 1, "case 4 service");
    }

    /* The peer's own remaining direction then terminates. */
    if (use_fin)
        MOQ_TEST_CHECK(moq_transport_bridge_on_peer_bidi_bytes(
                           tp.client_bridge, bidi, NULL, 0, true, 0) == MOQ_OK);
    else
        MOQ_TEST_CHECK(held_bridge_reset(
                           tp.client_bridge, bidi, 0x1, 0) == MOQ_OK);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.client_bridge));
    MOQ_TEST_CHECK(moq_session_state(tp.client) == MOQ_SESS_ESTABLISHED);
    {
        moq_event_t tev; int extra = 0;
        while (moq_session_poll_events(tp.client, &tev, 1) > 0) {
            extra++;
            moq_event_cleanup(&tev);
        }
        MOQ_TEST_CHECK(extra == 0);
    }
    failures += ep_expect_none(&tp.client_ep, "case 4 terminal");

    failures += check_mapping_gone(tp.client_bridge, bidi, bref, "case 4 terminal");

    /* Repeated terminal and repeated service are both inert. */
    MOQ_TEST_CHECK(held_bridge_reset(
                       tp.client_bridge, bidi, 0x1, 0) == MOQ_OK);
    MOQ_TEST_CHECK(held_bridge_service(tp.client_bridge, 0) == MOQ_OK);
    failures += ep_expect_none(&tp.client_ep, "case 4 repeat");
    failures += check_mapping_gone(tp.client_bridge, bidi, bref, "case 4 repeat");
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.client_bridge));
    MOQ_TEST_CHECK(moq_session_state(tp.client) == MOQ_SESS_ESTABLISHED);

    test_pair_destroy(&tp);
    return failures;
}


int test_peer_bidi_stop_is_request_cancellation(void)
{
    return peer_bidi_stop_then_terminal(false) +
           peer_bidi_stop_then_terminal(true);
}


/* 5. Peer FIN and peer RESET each retire a preserved mapping exactly once. */
int test_local_bidi_stop_then_peer_terminal_retires_once(void)
{
    int failures = 0;

    for (int use_reset = 0; use_reset < 2; use_reset++) {
        local_req_fixture_t f;
        if (local_request_bidi_fixture(&f) < 0) { failures++; return failures; }

        MOQ_TEST_CHECK(moq_transport_bridge_on_peer_stop_sending(
                           f.tp.client_bridge, f.bidi, 0x1, 0) == MOQ_OK);
        MOQ_TEST_CHECK(held_bridge_service(f.tp.client_bridge, 0) ==
                       MOQ_OK);
        {
            ep_rec_t want[1] = { ep_reset(f.bidi, 0x1) };
            failures += ep_expect(&f.tp.client_ep, want, 1, "case 5 service");
        }
        MOQ_TEST_CHECK(bridge_find_by_id(f.tp.client_bridge, f.bidi) != NULL);

        if (use_reset) {
            MOQ_TEST_CHECK(held_bridge_reset(
                               f.tp.client_bridge, f.bidi, 0x1, 0) == MOQ_OK);
        } else {
            /* The peer's own terminal: a complete response, then FIN. The
             * preserved mapping must carry these bytes to the session -- a
             * tombstone here would turn a valid response into a protocol
             * error. (A BARE FIN with no response at all is a truncated
             * request stream and closes 0x3; that is pre-existing session
             * behaviour and is not what this row is about.) */
            uint8_t msg[128];
            size_t n = encode_request_error(msg, sizeof(msg));
            MOQ_TEST_CHECK(n > 0);
            MOQ_TEST_CHECK(moq_transport_bridge_on_peer_bidi_bytes(
                               f.tp.client_bridge, f.bidi, msg, n, true, 0)
                           == MOQ_OK);
        }
        {
            moq_event_t ev;
            while (moq_session_poll_events(f.tp.client, &ev, 1) > 0)
                moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(f.tp.client_bridge));
        MOQ_TEST_CHECK(moq_session_state(f.tp.client) == MOQ_SESS_ESTABLISHED);
        failures += ep_expect_none(&f.tp.client_ep, "case 5 terminal");

        failures += check_mapping_gone(f.tp.client_bridge, f.bidi, f.ref,
                                       "case 5 terminal");
        MOQ_TEST_CHECK(request_registry_find_by_streamref(f.tp.client, f.ref)
                       .kind == MOQ_REQ_NONE);

        /* A second terminal and a second service pass are both inert. */
        MOQ_TEST_CHECK(held_bridge_reset(
                           f.tp.client_bridge, f.bidi, 0x1, 0) == MOQ_OK);
        MOQ_TEST_CHECK(held_bridge_service(f.tp.client_bridge, 0) ==
                       MOQ_OK);
        failures += ep_expect_none(&f.tp.client_ep, "case 5 repeat");
        failures += check_mapping_gone(f.tp.client_bridge, f.bidi, f.ref,
                                       "case 5 repeat");
        MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(f.tp.client_bridge));

        test_pair_destroy(&f.tp);
    }
    return failures;
}


/* 6. STOP after our OWN FIN, on a local-origin request bidi.
 *
 * RFC 9000 §3.5 requires the RESET_STREAM in the Ready and Send states and
 * permits it to be DEFERRED in Data Sent until the outstanding data is
 * acknowledged or declared lost. Sending nothing at all, forever, is not that
 * deferral, so this row does not claim the current outcome is conformant --
 * it pins what the product does.
 *
 * The cause: moq_session_has_transport_stream() does not keep a local-origin
 * request bidi's mapping alive after its local FIN, so an ordinary FIN retires
 * and tombstones the mapping through the ordinary half-close path, and by the
 * time the STOP arrives there is no stream left to reset.
 * Retaining every local-FIN mapping instead would broaden retention to every
 * local-origin stream, so this row pins the current mapping lifetime.
 */
int test_local_bidi_stop_after_local_fin(void)
{
    int failures = 0;
    local_req_fixture_t f;
    if (local_request_bidi_fixture(&f) < 0) { failures++; return failures; }

    MOQ_TEST_CHECK(queue_close_bidi(f.tp.client, f.ref) == MOQ_OK);
    MOQ_TEST_CHECK(held_bridge_service(f.tp.client_bridge, 0) ==
                   MOQ_OK);
    {
        ep_rec_t want[1];
        memset(want, 0, sizeof(want));
        want[0].kind = FAKE_OP_WRITE;
        want[0].stream_id = f.bidi;
        want[0].check_fin = true;
        want[0].fin = true;
        want[0].data_len = 0;
        want[0].data = (const uint8_t *)"";   /* empty FIN, payload compared */
        failures += ep_expect(&f.tp.client_ep, want, 1, "case 6 fin");
    }

    /* The ordinary FIN retired the mapping: pre-existing tombstone behaviour,
     * which the no-STOP control below pins in its own right. */
    MOQ_TEST_CHECK(bridge_find_by_id(f.tp.client_bridge, f.bidi) == NULL);

    MOQ_TEST_CHECK(moq_transport_bridge_on_peer_stop_sending(
                       f.tp.client_bridge, f.bidi, 0x1, 0) == MOQ_OK);
    MOQ_TEST_CHECK(held_bridge_service(f.tp.client_bridge, 0) ==
                   MOQ_OK);
    failures += ep_expect_none(&f.tp.client_ep, "case 6 stop after fin");
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(f.tp.client_bridge));
    MOQ_TEST_CHECK(moq_session_state(f.tp.client) == MOQ_SESS_ESTABLISHED);

    test_pair_destroy(&f.tp);
    return failures;
}


/* 7. Control: with NO peer STOP anywhere, an ordinary local FIN keeps its
 *    existing mapping/tombstone behaviour and later session output for that
 *    stream is still suppressed by the pre-existing path -- none of which the
 *    STOP correction may change. */
int test_local_bidi_normal_fin_unchanged(void)
{
    int failures = 0;
    local_req_fixture_t f;
    if (local_request_bidi_fixture(&f) < 0) { failures++; return failures; }

    bridge_stream_entry_t *e = bridge_find_by_id(f.tp.client_bridge, f.bidi);
    MOQ_TEST_CHECK(e != NULL && e->active);
    if (e) MOQ_TEST_CHECK(!e->peer_stop_received);

    MOQ_TEST_CHECK(queue_close_bidi(f.tp.client, f.ref) == MOQ_OK);
    MOQ_TEST_CHECK(held_bridge_service(f.tp.client_bridge, 0) ==
                   MOQ_OK);
    {
        ep_rec_t want[1];
        memset(want, 0, sizeof(want));
        want[0].kind = FAKE_OP_WRITE;
        want[0].stream_id = f.bidi;
        want[0].check_fin = true;
        want[0].fin = true;
        failures += ep_expect(&f.tp.client_ep, want, 1, "case 7 fin");
    }

    /* Retired and tombstoned through the ordinary local-FIN path, by both identities. */
    failures += check_mapping_gone(f.tp.client_bridge, f.bidi, f.ref, "case 7 fin");
    MOQ_TEST_CHECK(bridge_is_tombstoned(f.tp.client_bridge, f.bidi));

    /* A second FIN for the retired ref produces nothing. */
    MOQ_TEST_CHECK(queue_close_bidi(f.tp.client, f.ref) == MOQ_OK);
    MOQ_TEST_CHECK(held_bridge_service(f.tp.client_bridge, 0) ==
                   MOQ_OK);
    failures += ep_expect_none(&f.tp.client_ep, "case 7 second fin");
    failures += check_mapping_gone(f.tp.client_bridge, f.bidi, f.ref,
                                   "case 7 second fin");
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(f.tp.client_bridge));
    MOQ_TEST_CHECK(moq_session_state(f.tp.client) == MOQ_SESS_ESTABLISHED);

    test_pair_destroy(&f.tp);
    return failures;
}
