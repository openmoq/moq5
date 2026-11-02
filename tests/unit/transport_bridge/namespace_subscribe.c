#include "test_cases.h"

/* -- SUBSCRIBE_NAMESPACE response-stream termination, over the real
 * bridge (draft-18 §10.18).
 *
 * The route is indexed by idx_ns_by_ref, so request-registry ownership
 * lookup is not on its path -- these rows PROVE that with the very
 * predicate the bridge consults, and then by taking a genuine retained
 * WOULD_BLOCK rather than a fatal. Recovery is SERVICE-ONLY: the fixture never
 * re-delivers peer bytes, a FIN, or a second reset.
 *
 * The owner inventory, event images, drain multiset and action classification
 * come from tests/support/ns_owner_inventory.h, so this suite and the
 * direct-session suite hold ONE field contract. */

typedef struct nsfin_arm {
    test_pair_t      tp;
    uint64_t         bidi;      /* transport id, DECLARED before the request */
    moq_stream_ref_t ref;       /* session ref, DECLARED before the request */
    int              slot;
    uint32_t         generation;
    uint64_t         handle;
    int              srv_slot;        /* all three DERIVED before delivery */
    uint32_t         srv_generation;
    uint64_t         srv_handle;
    size_t           budget0;      /* client receive budget before any suffix */
    size_t           budget_active; /* DECLARED: budget0 + array + suffix key */
    nf_inv_t         want_live;     /* DECLARED established owner, not observed */
    const char      *sfx;
} nsfin_arm_t;


#define NSFIN_PREFIX  "live"

#define NSFIN_PREFIX2 "v2"

#define NSFIN_RID     0u


/* The exact bridge-entry class for this LOCAL-ORIGIN request bidi. (The
 * client opened it -- transport_bridge.c:1800 -- even though the peer's
 * response bytes later arrive on it. The earlier "peer-origin" wording was
 * wrong.) FIN and RESET supply only their own flag/code deltas. */
typedef struct nsfin_entry_want {
    int      active;
    int      pending_retry, pending_fin, fin_retained;
    int      pending_reset;  uint64_t pending_reset_code;
    int      pending_stop;   uint64_t pending_stop_code;
    int      peer_send_closed, local_send_closed, aborting;
    int      stream_pending;      /* moq_transport_bridge_stream_has_pending */
    int      bridge_pending;      /* moq_transport_bridge_has_pending */
    /* Inbound-uni classification storage. A BIDI/LOCAL entry never classifies,
     * so its declared value is the zero/PENDING state in EVERY live phase. */
    uint8_t  uni_disp;
    uint8_t  classify_len;
    uint8_t  classify_buf[9];
} nsfin_entry_want_t;


static nsfin_entry_want_t nsfin_entry_live(void)
{
    nsfin_entry_want_t w;
    memset(&w, 0, sizeof(w));
    w.active = 1;
    return w;
}


static int nsfin_check_entry(nsfin_arm_t *a, const nsfin_entry_want_t *w,
                             const char *what)
{
    int failures = 0;
    bridge_stream_entry_t *by_id = bridge_find_by_id(a->tp.client_bridge,
                                                     a->bidi);
    bridge_stream_entry_t *by_ref = bridge_find_by_ref(a->tp.client_bridge,
                                                       a->ref);
    if (!by_id || by_id != by_ref) {
        fprintf(stderr, "NSFIN %s: the entry is not reachable by BOTH the"
                " declared transport id and the declared internal ref\n", what);
        return failures + 1;
    }
#define NSE(f, fmt, expr) do { \
        if ((expr) != (w->f)) { \
            fprintf(stderr, "NSFIN %s: entry " #f " " fmt ", expected " fmt \
                    "\n", what, (expr), (w->f)); \
            failures++; \
        } \
    } while (0)
    /* identity first: a coherently wrong mapping must not pass */
    if (by_id->ref._v != a->ref._v) {
        fprintf(stderr, "NSFIN %s: entry ref %llu, declared %llu\n", what,
                (unsigned long long)by_id->ref._v,
                (unsigned long long)a->ref._v);
        failures++;
    }
    if (by_id->transport_id != a->bidi) {
        fprintf(stderr, "NSFIN %s: entry transport id %llu, declared %llu\n",
                what, (unsigned long long)by_id->transport_id,
                (unsigned long long)a->bidi);
        failures++;
    }
    if (by_id->kind != BRIDGE_STREAM_BIDI) {
        fprintf(stderr, "NSFIN %s: entry kind %d, expected BIDI\n", what,
                (int)by_id->kind);
        failures++;
    }
    if (by_id->origin != BRIDGE_ORIGIN_LOCAL) {
        fprintf(stderr, "NSFIN %s: entry origin %d, expected LOCAL\n", what,
                (int)by_id->origin);
        failures++;
    }
    NSE(active, "%d", (int)by_id->active);
    NSE(pending_retry, "%d", (int)by_id->pending_retry);
    NSE(pending_fin, "%d", (int)by_id->pending_fin);
    NSE(fin_retained, "%d", (int)by_id->fin_retained);
    NSE(pending_reset, "%d", (int)by_id->pending_reset);
    NSE(pending_stop, "%d", (int)by_id->pending_stop);
    NSE(peer_send_closed, "%d", (int)by_id->peer_send_closed);
    NSE(local_send_closed, "%d", (int)by_id->local_send_closed);
    NSE(aborting, "%d", (int)by_id->aborting);
    NSE(uni_disp, "%d", (int)by_id->uni_disp);
    NSE(classify_len, "%d", (int)by_id->classify_len);
#undef NSE
    if (memcmp(by_id->classify_buf, w->classify_buf,
               sizeof(w->classify_buf)) != 0) {
        fprintf(stderr, "NSFIN %s: entry classify_buf differs from the"
                " declared zero state\n", what);
        failures++;
    }
    if (by_id->pending_reset_code != w->pending_reset_code) {
        fprintf(stderr, "NSFIN %s: entry pending_reset_code %llu, expected"
                " %llu\n", what,
                (unsigned long long)by_id->pending_reset_code,
                (unsigned long long)w->pending_reset_code);
        failures++;
    }
    if (by_id->pending_stop_code != w->pending_stop_code) {
        fprintf(stderr, "NSFIN %s: entry pending_stop_code %llu, expected"
                " %llu\n", what,
                (unsigned long long)by_id->pending_stop_code,
                (unsigned long long)w->pending_stop_code);
        failures++;
    }
    if ((int)moq_transport_bridge_stream_has_pending(a->tp.client_bridge,
                                                     a->bidi)
        != w->stream_pending) {
        fprintf(stderr, "NSFIN %s: stream_has_pending %d, expected %d\n", what,
                (int)moq_transport_bridge_stream_has_pending(
                    a->tp.client_bridge, a->bidi), w->stream_pending);
        failures++;
    }
    if ((int)moq_transport_bridge_has_pending(a->tp.client_bridge)
        != w->bridge_pending) {
        fprintf(stderr, "NSFIN %s: has_pending %d, expected %d\n", what,
                (int)moq_transport_bridge_has_pending(a->tp.client_bridge),
                w->bridge_pending);
        failures++;
    }
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(a->tp.client_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_closed(a->tp.client_bridge));
    return failures;
}


/* Exactly this endpoint operation set on `ep`, and nothing else. */
static int nsfin_ops_exact(fake_endpoint_t *ep, int want_open_bidi,
                           int want_write, uint64_t want_id, const char *what)
{
    int failures = 0;
    int opens = 0, writes = 0, other = 0, wrong_id = 0;
    MOQ_TEST_CHECK(ep->count < FAKE_EP_MAX_OPS);
    for (size_t i = 0; i < ep->count; i++) {
        if (want_id && ep->ops[i].stream_id != want_id) wrong_id++;
        switch (ep->ops[i].kind) {
        case FAKE_OP_OPEN_BIDI: opens++;  break;
        case FAKE_OP_WRITE:     writes++; break;
        default:                other++;  break;
        }
    }
    if (wrong_id) {
        fprintf(stderr, "NSFIN %s: %d ops on a stream id other than the"
                " declared %llu\n", what, wrong_id,
                (unsigned long long)want_id);
        failures++;
    }
    if (opens != want_open_bidi || writes != want_write || other != 0) {
        fprintf(stderr, "NSFIN %s: ops open-bidi %d write %d other %d,"
                " expected %d/%d/0\n", what, opens, writes, other,
                want_open_bidi, want_write);
        failures++;
    }
    return failures;
}


/* Deliver every WRITE on `from` to `to`, checking each ingress result, and
 * report how many were delivered. Unlike d18_feed nothing is discarded. */
static int nsfin_deliver(moq_transport_bridge_t *to, fake_endpoint_t *from,
                         size_t *delivered, const char *what)
{
    int failures = 0;
    *delivered = 0;
    MOQ_TEST_CHECK(from->count < FAKE_EP_MAX_OPS);
    for (size_t i = 0; i < from->count; i++) {
        fake_op_t *o = &from->ops[i];
        if (o->kind != FAKE_OP_WRITE) {
            if (o->kind != FAKE_OP_OPEN_BIDI && o->kind != FAKE_OP_OPEN_UNI) {
                fprintf(stderr, "NSFIN %s: unexpected endpoint op kind %d\n",
                        what, (int)o->kind);
                failures++;
            }
            continue;
        }
        moq_result_t rc = (o->stream_id >= 2000 && o->stream_id < 3000) ||
                          (o->stream_id >= 4000)
            ? moq_transport_bridge_on_peer_bidi_bytes(
                  to, o->stream_id, o->data, o->data_len, o->fin, 0)
            : held_bridge_uni_bytes(
                  to, o->stream_id, o->data, o->data_len, o->fin, 0);
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
        (*delivered)++;
    }
    fake_endpoint_clear_ops(from);
    return failures;
}


/* Phases 1-4: one established client-side namespace subscription with exactly
 * one active suffix, every result checked and every record classified. */
static int nsfin_arm_build(nsfin_arm_t *a, const char *suffix_field)
{
    int failures = 0;
    memset(a, 0, sizeof(*a));
    a->sfx = suffix_field;
    if (d18_pair_init(&a->tp, 1) < 0) return 1;

    /* (1) checked starts, strict shuttle, exactly one SETUP_COMPLETE a side. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(a->tp.client, 0), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(a->tp.server, 0), (int)MOQ_OK);
    failures += d18_strict_shuttle(&a->tp, 30, 0, "nsfin setup");
    {
        moq_event_t ev;
        int c_setup = 0, s_setup = 0, other = 0;
        while (moq_session_poll_events(a->tp.client, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SETUP_COMPLETE) c_setup++; else other++;
            moq_event_cleanup(&ev);
        }
        while (moq_session_poll_events(a->tp.server, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SETUP_COMPLETE) s_setup++; else other++;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(c_setup, 1);
        MOQ_TEST_CHECK_EQ_INT(s_setup, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
    }
    MOQ_TEST_CHECK_EQ_SIZE(a->tp.client_ep.count, (size_t)0);
    MOQ_TEST_CHECK_EQ_SIZE(a->tp.server_ep.count, (size_t)0);

    /* (2) derive EVERY identity BEFORE the request exists: the session ref
     * from the session's own counter, the transport id from the fake
     * endpoint's next bidi id, and the owner slot/generation/handle from the
     * free pool. Nothing here is adopted from what the call produces. */
    a->ref = moq_stream_ref_from_u64(a->tp.client->next_stream_ref);
    a->bidi = a->tp.client_ep.next_bidi_id;
    MOQ_TEST_CHECK(a->ref._v != 0);
    MOQ_TEST_CHECK(a->bidi != 0);

    int want_slot = -1;
    for (size_t i = 0; i < a->tp.client->ns_sub_cap; i++)
        if (a->tp.client->ns_subs[i].state == MOQ_NS_SUB_FREE) {
            want_slot = (int)i; break;
        }
    MOQ_TEST_CHECK(want_slot >= 0);
    if (want_slot < 0) return failures + 1;
    a->slot = want_slot;
    a->generation = a->tp.client->ns_subs[want_slot].generation | 1u;
    a->handle = moq_handle_pack(MOQ_HANDLE_POOL_NAMESPACE_SUB,
                                a->tp.client->session_tag, a->generation,
                                (uint32_t)want_slot);
    MOQ_TEST_CHECK(a->handle != 0);

    nf_inv_t free_rec;
    nf_inv_read(a->tp.client, want_slot, &free_rec);
    MOQ_TEST_CHECK(free_rec.valid);

    moq_bytes_t pfx_parts[] = { MOQ_BYTES_LITERAL(NSFIN_PREFIX),
                                MOQ_BYTES_LITERAL(NSFIN_PREFIX2) };
    moq_namespace_t pfx = { pfx_parts, 2 };
    moq_subscribe_namespace_cfg_t nc;
    moq_subscribe_namespace_cfg_init(&nc);
    nc.track_namespace_prefix = pfx;
    nc.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
    moq_ns_sub_handle_t nh;
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_subscribe_namespace(a->tp.client, &nc, 0, &nh),
        (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(nh._opaque, a->handle);

    /* (3) exactly one local OPEN + one WRITE, decoded as SUBSCRIBE_NAMESPACE. */
    fake_endpoint_clear_ops(&a->tp.client_ep);
    MOQ_TEST_CHECK_EQ_INT(
        (int)held_bridge_service(a->tp.client_bridge, 0), (int)MOQ_OK);
    failures += nsfin_ops_exact(&a->tp.client_ep, 1, 1, a->bidi,
                                "arm local open");
    for (size_t i = 0; i < a->tp.client_ep.count; i++) {
        fake_op_t *o = &a->tp.client_ep.ops[i];
        if (o->kind != FAKE_OP_WRITE) continue;
        MOQ_TEST_CHECK_EQ_U64(o->stream_id, a->bidi);
        MOQ_TEST_CHECK(!o->fin);
        moq_control_envelope_t env;
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, o->data, o->data_len);
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_envelope(&r, &env),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(env.msg_type, MOQ_D18_SUBSCRIBE_NAMESPACE);
        MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&r), (size_t)0);
        moq_bytes_t dp[MOQ_DECODED_MAX_NAMESPACE_PARTS];
        moq_d18_subscribe_namespace_t sn;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_subscribe_namespace(
                env.payload, env.payload_len, dp,
                MOQ_DECODED_MAX_NAMESPACE_PARTS, &sn), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(sn.request_id, NSFIN_RID);
        MOQ_TEST_CHECK_EQ_SIZE(sn.track_namespace_prefix.count, (size_t)2);
        failures += txs_check_part_bytes(&sn.track_namespace_prefix, 0,
                                         NSFIN_PREFIX, strlen(NSFIN_PREFIX),
                                         "arm request prefix 0");
        failures += txs_check_part_bytes(&sn.track_namespace_prefix, 1,
                                         NSFIN_PREFIX2, strlen(NSFIN_PREFIX2),
                                         "arm request prefix 1");
        /* no unexpected parameters may ride the request */
        MOQ_TEST_CHECK(sn.params.auth_token_count == 0 &&
                       !sn.params.has_forward &&
                       !sn.params.has_subscriber_priority &&
                       !sn.params.has_filter &&
                       !sn.params.has_group_order &&
                       !sn.params.has_new_group_request);
    }
    MOQ_TEST_CHECK(moq_index_find(a->tp.client->idx_ns_by_ref,
                                  a->tp.client->idx_ns_mask,
                                  a->ref._v) == a->slot);
    /* Independence from the request registry, PROVEN with the predicate the
     * bridge consults. */
    MOQ_TEST_CHECK(moq_session_has_transport_stream(a->tp.client, a->ref));

    /* Derive the SERVER owner BEFORE the request is delivered -- afterwards
     * the slot is already occupied and a scan would name the NEXT free one. */
    a->srv_slot = -1;
    for (size_t i = 0; i < a->tp.server->ns_sub_cap; i++)
        if (a->tp.server->ns_subs[i].state == MOQ_NS_SUB_FREE) {
            a->srv_slot = (int)i; break;
        }
    MOQ_TEST_CHECK(a->srv_slot >= 0);
    if (a->srv_slot < 0) return failures + 1;
    a->srv_generation = a->tp.server->ns_subs[a->srv_slot].generation | 1u;
    a->srv_handle = moq_handle_pack(MOQ_HANDLE_POOL_NAMESPACE_SUB,
                                    a->tp.server->session_tag,
                                    a->srv_generation,
                                    (uint32_t)a->srv_slot);
    MOQ_TEST_CHECK(a->srv_handle != 0);

    /* The DECLARED pending-subscriber owner, from the free record plus the
     * fixture's own inputs -- never re-read from the session. */
    {
        static const char *const kParts[2] = { NSFIN_PREFIX, NSFIN_PREFIX2 };
        a->want_live = nf_local_pending_want(&free_rec, a->generation,
                                             a->handle, NSFIN_RID, a->ref._v,
                                             kParts, 2,
                                             MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE);
        int adopt = nf_adopt_prefix_addrs(a->tp.client, a->slot,
                                          &a->want_live, "arm client owner");
        failures += adopt;
        if (adopt == 0)
            failures += nf_inv_check(a->tp.client, a->slot, &a->want_live,
                                     "arm client owner");
    }

    size_t moved = 0;
    failures += nsfin_deliver(a->tp.server_bridge, &a->tp.client_ep, &moved,
                              "arm request");
    MOQ_TEST_CHECK_EQ_SIZE(moved, (size_t)1);

    /* (4) exactly one NS_SUB_REQUEST with the declared image. */
    moq_ns_sub_handle_t sh = MOQ_NS_SUB_HANDLE_INVALID;
    {
        moq_event_t ev;
        int reqs = 0, other = 0;
        while (moq_session_poll_events(a->tp.server, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_NS_SUB_REQUEST) {
                reqs++;
                sh = ev.u.ns_sub_request.handle;
                MOQ_TEST_CHECK_EQ_U64(sh._opaque, a->srv_handle);
                MOQ_TEST_CHECK(ev.u.ns_sub_request.forward);
                MOQ_TEST_CHECK_EQ_SIZE(
                    ev.u.ns_sub_request.track_namespace_prefix.count,
                    (size_t)2);
                failures += txs_check_part_bytes(
                    &ev.u.ns_sub_request.track_namespace_prefix, 0,
                    NSFIN_PREFIX, strlen(NSFIN_PREFIX),
                    "arm ns_sub_request 0");
                failures += txs_check_part_bytes(
                    &ev.u.ns_sub_request.track_namespace_prefix, 1,
                    NSFIN_PREFIX2, strlen(NSFIN_PREFIX2),
                    "arm ns_sub_request 1");
                MOQ_TEST_CHECK_EQ_U64(
                    ev.u.ns_sub_request.namespace_interest,
                    MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE);
                MOQ_TEST_CHECK_EQ_SIZE(ev.u.ns_sub_request.token_count,
                                       (size_t)0);
            } else {
                other++;
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(reqs, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
    }
    /* the physical server owner matches the derivation too */
    MOQ_TEST_CHECK(a->tp.server->ns_subs[a->srv_slot].state !=
                   MOQ_NS_SUB_FREE);
    MOQ_TEST_CHECK_EQ_U64(a->tp.server->ns_subs[a->srv_slot].generation,
                          a->srv_generation);
    MOQ_TEST_CHECK_EQ_U64(a->tp.server->ns_subs[a->srv_slot].handle._opaque,
                          a->srv_handle);

    moq_accept_ns_sub_cfg_t ac;
    moq_accept_ns_sub_cfg_init(&ac);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_accept_ns_sub(a->tp.server, sh, &ac, 0), (int)MOQ_OK);
    fake_endpoint_clear_ops(&a->tp.server_ep);
    MOQ_TEST_CHECK_EQ_INT(
        (int)held_bridge_service(a->tp.server_bridge, 0), (int)MOQ_OK);
    /* The acceptance rides the client's own bidi: one WRITE, no local open. */
    failures += nsfin_ops_exact(&a->tp.server_ep, 0, 1, a->bidi,
                                "arm acceptance");
    {
        /* Decode the REQUEST_OK before delivering it. */
        for (size_t i = 0; i < a->tp.server_ep.count; i++) {
            fake_op_t *o = &a->tp.server_ep.ops[i];
            if (o->kind != FAKE_OP_WRITE) continue;
            MOQ_TEST_CHECK_EQ_U64(o->stream_id, a->bidi);
            MOQ_TEST_CHECK(!o->fin);
            moq_control_envelope_t env;
            moq_buf_reader_t r;
            moq_buf_reader_init(&r, o->data, o->data_len);
            MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_envelope(&r, &env),
                                  (int)MOQ_OK);
            MOQ_TEST_CHECK_EQ_U64(env.msg_type, MOQ_D18_REQUEST_OK);
            MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&r), (size_t)0);
            /* the BODY too: junk inside the envelope must not pass */
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_d18_decode_request_ok(env.payload, env.payload_len),
                (int)MOQ_OK);
            /* The body check is load-bearing: junk inside the envelope must
             * be rejected, so an envelope-type-only assertion would not be
             * equivalent. */
            if (env.payload_len + 1 <= 64) {
                /* A VALID body plus one trailing byte, still inside the
                 * envelope, must be rejected -- otherwise the body check
                 * would only be proving that a malformed count fails. */
                uint8_t junk[64];
                memcpy(junk, env.payload, env.payload_len);
                junk[env.payload_len] = 0x5A;
                MOQ_TEST_CHECK(moq_d18_decode_request_ok(
                    junk, env.payload_len + 1) != MOQ_OK);
            }
        }
    }
    failures += nsfin_deliver(a->tp.client_bridge, &a->tp.server_ep, &moved,
                              "arm acceptance");
    MOQ_TEST_CHECK_EQ_SIZE(moved, (size_t)1);
    {
        nf_ev_t got[NF_EV_MAX]; size_t k = 0;
        failures += nf_collect(a->tp.client, a->handle, got, NF_EV_MAX, &k,
                               "arm ns_sub_ok");
        MOQ_TEST_CHECK_EQ_SIZE(k, (size_t)1);
        if (k == 1) {
            nf_ev_t want = nf_ev_want(MOQ_EVENT_NS_SUB_OK, NULL, NULL);
            failures += nf_ev_equals(&got[0], &want, "arm ns_sub_ok");
        }
    }

    /* The REQUEST_OK transition, applied EXPLICITLY rather than re-read. */
    a->want_live.state = MOQ_NS_SUB_ESTABLISHED;
    a->want_live.got_response = 1;
    failures += nf_inv_check(a->tp.client, a->slot, &a->want_live,
                             "arm established owner");
    a->budget0 = a->tp.client->recv_payload_bytes;

    /* One NAMESPACE: decoded on the wire, then surfaced as one exact
     * NAMESPACE_FOUND that is deliberately LEFT QUEUED as the blocker. */
    {
        moq_bytes_t sp[2];
        sp[0] = (moq_bytes_t){ (const uint8_t *)"room", 4 };
        sp[1] = (moq_bytes_t){ (const uint8_t *)suffix_field,
                               strlen(suffix_field) };
        moq_namespace_t sfx = { sp, 2 };
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_send_namespace(a->tp.server, sh, &sfx, 0),
            (int)MOQ_OK);
    }
    MOQ_TEST_CHECK_EQ_INT(
        (int)held_bridge_service(a->tp.server_bridge, 0), (int)MOQ_OK);
    failures += nsfin_ops_exact(&a->tp.server_ep, 0, 1, a->bidi,
                                "arm namespace");
    for (size_t i = 0; i < a->tp.server_ep.count; i++) {
        fake_op_t *o = &a->tp.server_ep.ops[i];
        if (o->kind != FAKE_OP_WRITE) continue;
        MOQ_TEST_CHECK_EQ_U64(o->stream_id, a->bidi);
        moq_control_envelope_t env;
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, o->data, o->data_len);
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_envelope(&r, &env),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(env.msg_type, MOQ_D18_NAMESPACE);
        MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&r), (size_t)0);
        MOQ_TEST_CHECK(!o->fin);
        moq_buf_reader_t pr;
        moq_buf_reader_init(&pr, env.payload, env.payload_len);
        moq_bytes_t sp[MOQ_DECODED_MAX_NAMESPACE_PARTS];
        moq_namespace_t got_ns;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_buf_read_namespace_prefix(&pr, sp,
                MOQ_DECODED_MAX_NAMESPACE_PARTS, &got_ns), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_SIZE(got_ns.count, (size_t)2);
        failures += txs_check_part_bytes(&got_ns, 0, "room", 4,
                                         "arm namespace suffix 0");
        failures += txs_check_part_bytes(&got_ns, 1, suffix_field,
                                         strlen(suffix_field),
                                         "arm namespace suffix 1");
        /* the whole payload, not just a decodable prefix of it */
        MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&pr), (size_t)0);
        /* and that check is load-bearing: the same body plus one trailing
         * byte inside the envelope must leave the reader unconsumed. */
        if (env.payload_len + 1 <= 128) {
            uint8_t tail[128];
            memcpy(tail, env.payload, env.payload_len);
            tail[env.payload_len] = 0x5A;
            moq_buf_reader_t tr;
            moq_buf_reader_init(&tr, tail, env.payload_len + 1);
            moq_bytes_t tp2[MOQ_DECODED_MAX_NAMESPACE_PARTS];
            moq_namespace_t tns;
            MOQ_TEST_CHECK(moq_buf_read_namespace_prefix(
                &tr, tp2, MOQ_DECODED_MAX_NAMESPACE_PARTS, &tns) == MOQ_OK);
            MOQ_TEST_CHECK(moq_buf_reader_remaining(&tr) != 0);
        }
    }
    failures += nsfin_deliver(a->tp.client_bridge, &a->tp.server_ep, &moved,
                              "arm namespace");
    MOQ_TEST_CHECK_EQ_SIZE(moved, (size_t)1);
    MOQ_TEST_CHECK(event_queue_full(a->tp.client));
    MOQ_TEST_CHECK_EQ_SIZE(a->tp.client_ep.count, (size_t)0);

    /* The NAMESPACE transition, applied EXPLICITLY: one inbound tracker is
     * present (presence only -- its address cannot authorize itself) and the
     * active budget is an absolute declared value with checked addition. */
    {
        const size_t charge = nf_suffix_charge(suffix_field);
        MOQ_TEST_CHECK(charge <= SIZE_MAX - a->budget0);
        a->budget_active = a->budget0 + charge;
        nf_inv_t want = a->want_live;
        want.cmp_suffix_ptr = 0;
        want.suffixes = (const void *)1;
        want.suffixes_inbound = 1;
        nf_inv_t now;
        nf_inv_read(a->tp.client, a->slot, &now);
        now.cmp_suffix_ptr = 0;
        failures += nf_inv_equals(&now, &want, "arm namespace owner");
        MOQ_TEST_CHECK(now.suffixes != NULL);
        MOQ_TEST_CHECK_EQ_SIZE(a->tp.client->recv_payload_bytes,
                               a->budget_active);
        /* Only now is the tracker address adopted for later conservation. */
        a->want_live.suffixes =
            a->tp.client->ns_subs[a->slot].announced_suffixes;
        a->want_live.suffixes_inbound = 1;
    }
    return failures;
}


/* The complete pre-terminal picture: owner inventory, sole ns edge, empty
 * drain set, and ONE bridge record reachable by both identities. */
static int nsfin_arm_precheck(nsfin_arm_t *a, nf_inv_t *live, nf_drain_t *d0,
                              og_graph_t *g0, const char *what)
{
    /* The DECLARED owner the arm built, not a fresh read of the session. */
    *live = a->want_live;
    int failures = nf_inv_check(a->tp.client, a->slot, live, what);

    MOQ_TEST_CHECK(nf_drain_snap(a->tp.client, d0) == 0);
    MOQ_TEST_CHECK_EQ_SIZE(d0->count, (size_t)0);

    og_capture(a->tp.client, g0);
    failures += og_check_integrity(g0, what);
    const og_edge_spec_t edges[] = { { OG_DOM_NS_REF, a->ref._v } };
    failures += og_check_owner_edges(g0, MOQ_REQ_NAMESPACE_SUB, a->slot,
                                     edges, 1, what);

    nsfin_entry_want_t ew = nsfin_entry_live();
    failures += nsfin_check_entry(a, &ew, what);
    /* No session output is owed at the arm point. */
    MOQ_TEST_CHECK_EQ_SIZE(
        a->tp.client->action_tail - a->tp.client->action_head, (size_t)0);
    return failures;
}


/* One conservation checker for the blocked and post-blocker windows: the
 * complete owner, exact graph topology, exact drain set, exact receive
 * budget, and no session action output. */
static int nsfin_conserved(nsfin_arm_t *a, const nf_inv_t *want,
                           const og_graph_t *g0, const nf_drain_t *d0,
                           size_t budget, const char *what)
{
    int failures = nf_inv_check(a->tp.client, a->slot, want, what);
    og_graph_t g;
    og_capture(a->tp.client, &g);
    failures += og_check_same_topology(g0, &g, what);
    nf_drain_t d;
    MOQ_TEST_CHECK(nf_drain_snap(a->tp.client, &d) == 0);
    failures += nf_drain_equals(&d, d0, what);
    if (a->tp.client->recv_payload_bytes != budget) {
        fprintf(stderr, "NSFIN %s: receive budget %zu, expected %zu\n", what,
                a->tp.client->recv_payload_bytes, budget);
        failures++;
    }
    MOQ_TEST_CHECK_EQ_SIZE(
        a->tp.client->action_tail - a->tp.client->action_head, (size_t)0);
    return failures;
}


/* Phases 8-9: nothing of this owner survives, in EITHER identity. */
static int nsfin_check_retired(nsfin_arm_t *a, const nf_inv_t *live,
                               const nf_drain_t *d0, size_t budget0,
                               const char *what)
{
    int failures = 0;
    if (a->tp.client->recv_payload_bytes != budget0) {
        fprintf(stderr, "NSFIN %s: receive budget %zu, expected %zu\n", what,
                a->tp.client->recv_payload_bytes, budget0);
        failures++;
    }
    if (moq_transport_bridge_stream_has_pending(a->tp.client_bridge, a->bidi)) {
        fprintf(stderr, "NSFIN %s: the stream still reports pending work\n",
                what);
        failures++;
    }
    nf_inv_t want = *live;
    nf_inv_apply_free(&want);
    failures += nf_inv_check(a->tp.client, a->slot, &want, what);

    og_graph_t g;
    og_capture(a->tp.client, &g);
    failures += og_check_integrity(&g, what);
    failures += og_check_no_edge(&g, OG_DOM_NS_REF, a->ref._v, what);
    failures += og_check_owner_unreferenced(&g, MOQ_REQ_NAMESPACE_SUB, a->slot,
                                            what);

    nf_drain_t d1;
    MOQ_TEST_CHECK(nf_drain_snap(a->tp.client, &d1) == 0);
    failures += nf_drain_equals(&d1, d0, what);

    if (bridge_find_by_id(a->tp.client_bridge, a->bidi) != NULL) {
        fprintf(stderr, "NSFIN %s: the transport-id mapping survived\n", what);
        failures++;
    }
    if (bridge_find_by_ref(a->tp.client_bridge, a->ref) != NULL) {
        fprintf(stderr, "NSFIN %s: the internal-ref mapping survived\n", what);
        failures++;
    }
    MOQ_TEST_CHECK(!moq_transport_bridge_has_pending(a->tp.client_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(a->tp.client_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_closed(a->tp.client_bridge));
    return failures;
}


/* Exactly one empty-FIN WRITE on the original transport id, or none. */
static int nsfin_expect_ops(nsfin_arm_t *a, size_t want_fin_writes,
                            const char *what)
{
    int failures = 0;
    size_t fins = 0, other = 0;
    MOQ_TEST_CHECK(a->tp.client_ep.count < FAKE_EP_MAX_OPS);
    for (size_t i = 0; i < a->tp.client_ep.count; i++) {
        fake_op_t *o = &a->tp.client_ep.ops[i];
        if (o->kind == FAKE_OP_WRITE && o->stream_id == a->bidi &&
            o->data_len == 0 && o->fin)
            fins++;
        else
            other++;
    }
    if (fins != want_fin_writes || other != 0) {
        fprintf(stderr, "NSFIN %s: %zu empty-FIN writes and %zu other ops,"
                " expected %zu/0\n", what, fins, other, want_fin_writes);
        failures++;
    }
    fake_endpoint_clear_ops(&a->tp.client_ep);
    return failures;
}


/* The client's endpoint did exactly one thing: reset our half of the
 * request bidi with CANCELLED. */
static int nsfin_expect_cancel(nsfin_arm_t *a, const char *what)
{
    int failures = 0;
    size_t resets = 0, other = 0;
    MOQ_TEST_CHECK(a->tp.client_ep.count < FAKE_EP_MAX_OPS);
    for (size_t i = 0; i < a->tp.client_ep.count; i++) {
        fake_op_t *o = &a->tp.client_ep.ops[i];
        if (o->kind == FAKE_OP_RESET && o->stream_id == a->bidi &&
            o->error_code == MOQ_STREAM_ERROR_CANCELLED)
            resets++;
        else
            other++;
    }
    if (resets != 1 || other != 0) {
        fprintf(stderr, "NSFIN %s: %zu CANCELLED resets and %zu other ops,"
                " expected 1/0\n", what, resets, other);
        failures++;
    }
    fake_endpoint_clear_ops(&a->tp.client_ep);
    return failures;
}


/* Release ONLY the blocker: exactly the declared NAMESPACE_FOUND. */
static int nsfin_release_blocker(nsfin_arm_t *a)
{
    nf_ev_t got[NF_EV_MAX]; size_t k = 0;
    int failures = nf_collect(a->tp.client, a->handle, got, NF_EV_MAX, &k,
                              "blocker");
    MOQ_TEST_CHECK_EQ_SIZE(k, (size_t)1);
    if (k == 1) {
        nf_ev_t want = nf_ev_want(MOQ_EVENT_NAMESPACE_FOUND, "room", a->sfx);
        failures += nf_ev_equals(&got[0], &want, "blocker");
    }
    return failures;
}


static int nsfin_expect_gone(nsfin_arm_t *a, size_t n, const char *what)
{
    nf_ev_t got[NF_EV_MAX]; size_t k = 0;
    int failures = nf_collect(a->tp.client, a->handle, got, NF_EV_MAX, &k,
                              what);
    nf_ev_t want[1] = { nf_ev_want(MOQ_EVENT_NAMESPACE_GONE, "room", a->sfx) };
    if (n == 0) {
        MOQ_TEST_CHECK_EQ_SIZE(k, (size_t)0);
        return failures;
    }
    failures += nf_multiset(got, k, want, 1, what);
    return failures;
}


int test_ns_response_fin_bridge(void)
{
    int failures = 0;
    nsfin_arm_t a;
    failures += nsfin_arm_build(&a, "alpha");

    nf_inv_t live; nf_drain_t d0; og_graph_t g0;
    failures += nsfin_arm_precheck(&a, &live, &d0, &g0, "fin arm");

    /* (5) the peer FINs its response half while the client cannot emit. */
    moq_result_t rc = moq_transport_bridge_on_peer_bidi_bytes(
        a.tp.client_bridge, a.bidi, NULL, 0, true, 0);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(a.tp.client_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_closed(a.tp.client_bridge));
    {
        nsfin_entry_want_t ew = nsfin_entry_live();
        ew.pending_retry = 1;
        ew.fin_retained = 1;
        ew.stream_pending = 1;
        ew.bridge_pending = 1;
        failures += nsfin_check_entry(&a, &ew, "fin blocked");
    }
    nf_inv_t want_blocked = live;
    want_blocked.pending_fin = 1;
    const size_t budget_live = a.budget_active;   /* DECLARED by the arm */
    failures += nsfin_conserved(&a, &want_blocked, &g0, &d0, budget_live,
                                "fin blocked");
    failures += nsfin_expect_ops(&a, 0, "fin blocked");

    /* (7) release only the blocker, reassert, then SERVICE -- no
     * re-delivery of bytes, FIN or reset at any point. */
    failures += nsfin_release_blocker(&a);
    {
        nsfin_entry_want_t ew = nsfin_entry_live();
        ew.pending_retry = 1;
        ew.fin_retained = 1;
        ew.stream_pending = 1;
        ew.bridge_pending = 1;
        failures += nsfin_check_entry(&a, &ew, "fin released");
    }
    failures += nsfin_conserved(&a, &want_blocked, &g0, &d0, budget_live,
                                "fin released");
    MOQ_TEST_CHECK_EQ_INT(
        (int)held_bridge_service(a.tp.client_bridge, 0), (int)MOQ_OK);
    /* (8) exact completion */
    failures += nsfin_expect_gone(&a, 1, "fin complete");
    failures += nsfin_expect_ops(&a, 1, "fin complete");
    failures += nsfin_check_retired(&a, &live, &d0, a.budget0, "fin complete");

    /* (9) a second service emits nothing and repeats the postcondition. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)held_bridge_service(a.tp.client_bridge, 0), (int)MOQ_OK);
    failures += nsfin_expect_gone(&a, 0, "fin idempotent");
    failures += nsfin_expect_ops(&a, 0, "fin idempotent");
    failures += nsfin_check_retired(&a, &live, &d0, a.budget0, "fin idempotent");

    test_pair_destroy(&a.tp);
    return failures;
}


int test_ns_response_reset_bridge(void)
{
    int failures = 0;
    nsfin_arm_t a;
    failures += nsfin_arm_build(&a, "beta");

    nf_inv_t live; nf_drain_t d0; og_graph_t g0;
    failures += nsfin_arm_precheck(&a, &live, &d0, &g0, "reset arm");

    /* (6) the peer resets while the client cannot emit. */
    moq_result_t rc = held_bridge_reset(
        a.tp.client_bridge, a.bidi, 0x2B, 0);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(a.tp.client_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_closed(a.tp.client_bridge));
    {
        nsfin_entry_want_t ew = nsfin_entry_live();
        ew.pending_reset = 1;
        ew.pending_reset_code = 0x2Bu;
        ew.stream_pending = 1;
        ew.bridge_pending = 1;
        failures += nsfin_check_entry(&a, &ew, "reset blocked");
    }
    const size_t budget_live = a.budget_active;   /* DECLARED by the arm */
    failures += nsfin_conserved(&a, &live, &g0, &d0, budget_live,
                                "reset blocked");
    failures += nsfin_expect_ops(&a, 0, "reset blocked");

    failures += nsfin_release_blocker(&a);
    {
        nsfin_entry_want_t ew = nsfin_entry_live();
        ew.pending_reset = 1;
        ew.pending_reset_code = 0x2Bu;
        ew.stream_pending = 1;
        ew.bridge_pending = 1;
        failures += nsfin_check_entry(&a, &ew, "reset released");
    }
    failures += nsfin_conserved(&a, &live, &g0, &d0, budget_live,
                                "reset released");
    MOQ_TEST_CHECK_EQ_INT(
        (int)held_bridge_service(a.tp.client_bridge, 0), (int)MOQ_OK);
    failures += nsfin_expect_gone(&a, 1, "reset complete");
    /* The peer's reset ends the request: the session queues no close, and
     * our still-open half is cancelled once, with CANCELLED. */
    failures += nsfin_expect_cancel(&a, "reset complete");
    failures += nsfin_check_retired(&a, &live, &d0, a.budget0, "reset complete");

    MOQ_TEST_CHECK_EQ_INT(
        (int)held_bridge_service(a.tp.client_bridge, 0), (int)MOQ_OK);
    failures += nsfin_expect_gone(&a, 0, "reset idempotent");
    failures += nsfin_expect_ops(&a, 0, "reset idempotent");
    failures += nsfin_check_retired(&a, &live, &d0, a.budget0, "reset idempotent");

    test_pair_destroy(&a.tp);
    return failures;
}
