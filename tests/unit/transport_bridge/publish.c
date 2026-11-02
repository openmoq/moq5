#include "test_cases.h"

/* -- P3 PUBLISH: the RETAINED ingress class -------------------------
 *
 * The counterpart to P7. Here the destination cannot take the FIN in the same
 * call -- the request event fills the ONE-slot queue, so the teardown defers --
 * and the obligation is RETAINED on the bridge instead of transferred to the
 * owner. That is the whole difference, and it is DECLARED as FIN_BR_RETAINED
 * rather than inferred: the ingress owes MOQ_ERR_WOULD_BLOCK, the bridge holds
 * pending_retry + fin_retained, and `peer_send_closed` is NOT yet set because
 * the retained path returns before the FIN is recorded.
 *
 * The semantic owner is a committed publication keyed ONLY in the request
 * stream registry (idx_req_by_streamref). moq_session_has_transport_stream()
 * consults that registry, so the bridge recognises the live owner and RETAINS
 * the WOULD_BLOCK teardown instead of fatalizing the connection. This case
 * pins that behaviour.
 *
 * The application then rejects the surfaced owner inside that window, and the
 * bridge's own service -- with NO peer bytes redelivered -- completes the
 * retained obligation and retires the physical mapping exactly once.
 *
 * The fixture asserts the bridge's retention flags and the owner's own state.
 * It does NOT name, inspect or prescribe any storage for the FIN handoff
 * carrier; the FIN-aware drain selection owns that, storage-agnostic here.
 */
#define P3_ALIAS 0x21

static const uint8_t k_p3_ns0[]  = { 'l','i','v','e' };

static const uint8_t k_p3_ns1[]  = { 'c','a','m' };

static const uint8_t k_p3_name[] = { 'v' };


/* Every surfaced field carries a value distinguishable from its default, so an
 * omitted field cannot pass by coinciding with the zero image. */
#define P3_TOK_TYPE     0x0b

static const uint8_t k_p3_tok[]  = { 'p','t','o','k' };

/* Track Properties: DYNAMIC_GROUPS (0x30, an even type, so varint-valued) = 1,
 * delta-encoded from zero. */
static const uint8_t k_p3_props[] = { 0x30, 0x01 };

#define P3_LARGEST_GROUP 0x51

#define P3_LARGEST_OBJ   0x07

#define P3_EXPIRES_MS    7250

/* The publisher's initial forward intent, sent EXPLICITLY as 0 -- the opposite
 * of the omitted-parameter default. */
#define P3_FORWARD       0


/* The rejection's payload: a nonzero retry interval and a nonempty reason, so
 * neither can be dropped without changing the decoded image. */
#define P3_RETRY_MS      4500

static const uint8_t k_p3_reason[] = { 'n','o','t',' ','h','e','r','e' };


static const moq_pub_entry_t *p3_owner(const moq_session_t *s,
                                       moq_stream_ref_t ref)
{
    moq_request_endpoint_t ep = request_registry_find_by_streamref(s, ref);
    if (ep.kind != MOQ_REQ_PUBLISH) return NULL;
    if (ep.slot < 0 || (size_t)ep.slot >= s->pub_cap) return NULL;
    return &s->publishes[ep.slot];
}


static int p3_pool_busy(const moq_session_t *s)
{
    int n = 0;
    for (size_t i = 0; i < s->pub_cap; i++)
        if (s->publishes[i].state != MOQ_PUB_FREE) n++;
    return n;
}


static int p3_derive_slot(const moq_session_t *s, uint32_t *out_gen)
{
    for (size_t i = 0; i < s->pub_cap; i++)
        if (s->publishes[i].state == MOQ_PUB_FREE) {
            *out_gen = s->publishes[i].generation | 1u;
            return (int)i;
        }
    return -1;
}


static int p3_check_live(const moq_session_t *s, moq_stream_ref_t ref,
                         int want_slot, uint32_t want_gen, uint64_t want_handle,
                         const char *what)
{
    int bad = 0;
    if (want_slot < 0 || (size_t)want_slot >= s->pub_cap) {
        fprintf(stderr, "FINBR %s: declared slot out of range\n", what);
        return 1;
    }
    const moq_pub_entry_t *e = p3_owner(s, ref);
    if (!e) { fprintf(stderr, "FINBR %s: owner absent\n", what); return 1; }
    if (e != &s->publishes[want_slot]) {
        fprintf(stderr, "FINBR %s: owner in an undeclared slot\n", what); bad++;
    }
    if ((int)e->state != (int)MOQ_PUB_PENDING_SUBSCRIBER) {
        fprintf(stderr, "FINBR %s: state %d\n", what, (int)e->state); bad++;
    }
    if ((int)e->role != (int)MOQ_PUB_ROLE_SUBSCRIBER) {
        fprintf(stderr, "FINBR %s: role %d\n", what, (int)e->role); bad++;
    }
    if (e->generation != want_gen) {
        fprintf(stderr, "FINBR %s: generation\n", what); bad++;
    }
    if (e->handle._opaque != want_handle) {
        fprintf(stderr, "FINBR %s: handle\n", what); bad++;
    }
    if (e->request_id != 0) {
        fprintf(stderr, "FINBR %s: request id\n", what); bad++;
    }
    if (e->rs.ref._v != ref._v) {
        fprintf(stderr, "FINBR %s: owner ref\n", what); bad++;
    }
    /* RETAINED class: the FIN is held by the BRIDGE, so the owner's own latch
     * must still be clear. This says nothing about where a future marker
     * lives -- only that the durable latch has not been set behind our back. */
    if (e->rs.recv_fin) {
        fprintf(stderr, "FINBR %s: owner latch set on a retained ingress\n",
                what);
        bad++;
    }
    if (p3_pool_busy(s) != 1) {
        fprintf(stderr, "FINBR %s: pool occupancy %d, expected 1\n", what,
                p3_pool_busy(s));
        bad++;
    }
    return bad;
}


static int p3_check_retired(const moq_session_t *s, moq_stream_ref_t ref,
                            int want_slot, const char *what)
{
    int bad = 0;
    if (p3_owner(s, ref) != NULL) {
        fprintf(stderr, "FINBR %s: registry edge survives\n", what); bad++;
    }
    if (want_slot >= 0 && (size_t)want_slot < s->pub_cap &&
        s->publishes[want_slot].state != MOQ_PUB_FREE) {
        fprintf(stderr, "FINBR %s: pool slot leaked\n", what); bad++;
    }
    if (p3_pool_busy(s) != 0) {
        fprintf(stderr, "FINBR %s: pool occupancy %d, expected 0\n", what,
                p3_pool_busy(s));
        bad++;
    }
    return bad;
}


static int p3_check_edges(const og_graph_t *g, moq_stream_ref_t ref,
                          int want_slot, int live, const char *what)
{
    int bad = og_check_integrity(g, what);
    if (live) {
        bad += og_check_edge(g, OG_DOM_REQ_STREAMREF, ref._v,
                             MOQ_REQ_PUBLISH, want_slot, what);
        const og_edge_spec_t w[] = { { OG_DOM_REQ_STREAMREF, ref._v } };
        bad += og_check_owner_edges(g, MOQ_REQ_PUBLISH, want_slot, w, 1, what);
    } else {
        bad += og_check_no_edge(g, OG_DOM_REQ_STREAMREF, ref._v, what);
        bad += og_check_owner_edges(g, MOQ_REQ_PUBLISH, want_slot, NULL, 0,
                                    what);
    }
    bad += og_check_no_edge(g, OG_DOM_NS_REF, ref._v, what);
    return bad;
}


/* The peer's close is observed before the rejection commits, so this route
 * owes NO drain either -- the declared multiset is empty throughout. */
static int p3_check_drain(const moq_session_t *s, const char *what)
{
    return check_drain_membership(s, NULL, 0, what);
}


static void p3_capture(const moq_session_t *s, void *vctx, void *state,
                       size_t cap)
{
    fin_bridge_run_t *r = (fin_bridge_run_t *)vctx;
    p3_snap_t *o = (p3_snap_t *)state;
    if (cap < sizeof(*o)) {
        fprintf(stderr, "FINBR: snapshot storage too small\n"); return;
    }
    memset(o, 0, sizeof(*o));
    if (r->want_slot < 0 || (size_t)r->want_slot >= s->pub_cap) return;
    const moq_pub_entry_t *e = &s->publishes[r->want_slot];
    o->raw = *e;
    /* The publication's OWNED bytes are its deferred terminal reason
     * (`done_reason_buf`); `req_recv_buf` is co-allocated in the session block
     * and deliberately not copied. */
    o->tid_len = e->done_reason_len;
    if (e->done_reason_len > P3_OWN_MAX ||
        (e->done_reason_len && !e->done_reason_buf))
        return;
    if (e->done_reason_len)
        memcpy(o->tid, e->done_reason_buf, e->done_reason_len);
    o->valid = 1;
}


static int p3_check(const moq_session_t *s, void *vctx, const void *state,
                    size_t cap, const char *what)
{
    const p3_snap_t *want = (const p3_snap_t *)state;
    p3_snap_t now;
    if (cap < sizeof(now)) {
        fprintf(stderr, "FINBR %s: snapshot storage too small\n", what);
        return 1;
    }
    p3_capture(s, vctx, &now, sizeof(now));
    if (!now.valid || !want->valid) {
        fprintf(stderr, "FINBR %s: incomparable owner record\n", what);
        return 1;
    }
    if (memcmp(&now.raw, &want->raw, sizeof(now.raw)) != 0) {
        fprintf(stderr, "FINBR %s: owner record changed\n", what); return 1;
    }
    if (now.tid_len != want->tid_len ||
        (now.tid_len && memcmp(now.tid, want->tid, now.tid_len) != 0)) {
        fprintf(stderr, "FINBR %s: retained terminal reason changed\n", what);
        return 1;
    }
    return 0;
}


static int p3_want_request(txs_norm_vec_t *v, uint64_t handle)
{
    txs_img_t im;
    txs_img_init(&im);
    txs_img_u64(&im, handle);
    txs_img_u64(&im, 2);                              /* ns part count */
    txs_img_bytes(&im, k_p3_ns0, sizeof(k_p3_ns0));
    txs_img_bytes(&im, k_p3_ns1, sizeof(k_p3_ns1));
    txs_img_bytes(&im, k_p3_name, sizeof(k_p3_name));
    txs_img_u64(&im, P3_ALIAS);
    txs_img_u64(&im, P3_FORWARD);
    txs_img_u64(&im, 1);                              /* token count */
    txs_img_u64(&im, P3_TOK_TYPE);
    txs_img_bytes(&im, k_p3_tok, sizeof(k_p3_tok));
    txs_img_bytes(&im, k_p3_props, sizeof(k_p3_props));
    txs_img_u64(&im, 1);                              /* dynamic_groups */
    txs_img_u64(&im, 1);                              /* has_largest */
    txs_img_u64(&im, P3_LARGEST_GROUP);
    txs_img_u64(&im, P3_LARGEST_OBJ);
    txs_img_u64(&im, 1);                              /* has_expires */
    txs_img_u64(&im, P3_EXPIRES_MS);
    if (!txs_norm_append_img(v, MOQ_EVENT_PUBLISH_REQUEST, &im)) {
        fprintf(stderr, "FINBR: could not build the declared request image\n");
        return 1;
    }
    return 0;
}


static bool p3_norm_event(const moq_event_t *ev, void *ctx, txs_norm_vec_t *out)
{
    (void)ctx;
    if (ev->kind != MOQ_EVENT_PUBLISH_REQUEST) {
        fprintf(stderr, "FINBR: unnormalized event kind %u\n",
                (unsigned)ev->kind);
        return false;
    }
    const moq_publish_request_event_t *q = &ev->u.publish_request;
    if (q->track_namespace.count > 32 || q->token_count > 16) {
        fprintf(stderr, "FINBR: implausible request counts\n"); return false;
    }
    if (q->track_namespace.count && !q->track_namespace.parts) {
        fprintf(stderr, "FINBR: namespace count with NULL parts\n");
        return false;
    }
    if (q->token_count && !q->tokens) {
        fprintf(stderr, "FINBR: token count with NULL tokens\n");
        return false;
    }
    txs_img_t im;
    txs_img_init(&im);
    txs_img_u64(&im, q->pub._opaque);
    txs_img_u64(&im, (uint64_t)q->track_namespace.count);
    for (size_t i = 0; i < q->track_namespace.count; i++) {
        const moq_bytes_t *b = &q->track_namespace.parts[i];
        if (b->len && !b->data) {
            fprintf(stderr, "FINBR: namespace part %zu has NULL bytes\n", i);
            return false;
        }
        txs_img_bytes(&im, b->data, b->len);
    }
    if (q->track_name.len && !q->track_name.data) {
        fprintf(stderr, "FINBR: track name has NULL bytes\n"); return false;
    }
    txs_img_bytes(&im, q->track_name.data, q->track_name.len);
    txs_img_u64(&im, q->track_alias);
    txs_img_u64(&im, q->forward ? 1 : 0);
    txs_img_u64(&im, (uint64_t)q->token_count);
    for (size_t i = 0; i < q->token_count; i++) {
        if (q->tokens[i].token_value.len && !q->tokens[i].token_value.data) {
            fprintf(stderr, "FINBR: token %zu has NULL bytes\n", i);
            return false;
        }
        txs_img_u64(&im, q->tokens[i].token_type);
        txs_img_bytes(&im, q->tokens[i].token_value.data,
                      q->tokens[i].token_value.len);
    }
    if (q->track_properties.len && !q->track_properties.data) {
        fprintf(stderr, "FINBR: track properties have NULL bytes\n");
        return false;
    }
    txs_img_bytes(&im, q->track_properties.data, q->track_properties.len);
    txs_img_u64(&im, q->dynamic_groups ? 1 : 0);
    txs_img_u64(&im, q->has_largest ? 1 : 0);
    txs_img_u64(&im, q->largest_group);
    txs_img_u64(&im, q->largest_object);
    txs_img_u64(&im, q->has_expires ? 1 : 0);
    txs_img_u64(&im, q->expires_ms);
    return txs_norm_append_img(out, ev->kind, &im);
}


static bool p3_norm_action(const moq_action_t *a, void *ctx, txs_norm_vec_t *out)
{
    (void)ctx; (void)out;
    fprintf(stderr, "FINBR: action kind %u left queued after service\n",
            (unsigned)a->kind);
    return false;
}


/* The rejection this family puts on the wire: one REQUEST_ERROR on the request
 * bidi, FIN'd, decoded rather than counted. */
static int p3_check_terminal_wire(void *vctx, const char *what)
{
    fin_bridge_run_t *r = (fin_bridge_run_t *)vctx;
    int bad = 0;
    if (r->tp.server_ep.count != 1) {
        fprintf(stderr, "FINBR %s: %zu endpoint ops, expected 1\n", what,
                r->tp.server_ep.count);
        return 1;
    }
    fake_op_t *o = &r->tp.server_ep.ops[0];
    if (o->kind != FAKE_OP_WRITE) {
        fprintf(stderr, "FINBR %s: op kind %d, expected WRITE\n", what,
                (int)o->kind);
        return 1;
    }
    if (o->stream_id != r->transport_id) {
        fprintf(stderr, "FINBR %s: wrong transport stream\n", what); bad++;
    }
    if (!o->fin) {
        fprintf(stderr, "FINBR %s: local half not FIN'd\n", what); bad++;
    }
    moq_buf_reader_t rr;
    moq_buf_reader_init(&rr, o->data, o->data_len);
    moq_control_envelope_t env;
    memset(&env, 0, sizeof(env));
    if (moq_d18_decode_envelope(&rr, &env) != MOQ_OK) {
        fprintf(stderr, "FINBR %s: undecodable envelope\n", what);
        return bad + 1;
    }
    if (env.msg_type != (uint64_t)MOQ_D18_REQUEST_ERROR) {
        fprintf(stderr, "FINBR %s: msg type %llu\n", what,
                (unsigned long long)env.msg_type);
        bad++;
    }
    if (moq_buf_reader_remaining(&rr) != 0) {
        fprintf(stderr, "FINBR %s: trailing bytes after the envelope\n", what);
        bad++;
    }
    moq_d18_request_error_t er;
    memset(&er, 0, sizeof(er));
    if (moq_d18_decode_request_error(env.payload, env.payload_len,
                                     &er) != MOQ_OK) {
        fprintf(stderr, "FINBR %s: undecodable REQUEST_ERROR body\n", what);
        return bad + 1;
    }
    /* The COMPLETE decoded payload, not the code alone: a dropped retry
     * interval or reason would otherwise leave the rejection looking correct. */
    if (er.error_code != (uint64_t)MOQ_REQUEST_ERROR_NOT_SUPPORTED) {
        fprintf(stderr, "FINBR %s: error code %llu\n", what,
                (unsigned long long)er.error_code);
        bad++;
    }
    /* The wire Retry Interval is the interval plus one (d18 §10.6). */
    if (er.retry_interval != (uint64_t)P3_RETRY_MS + 1u) {
        fprintf(stderr, "FINBR %s: retry interval %llu, expected %d + 1\n",
                what, (unsigned long long)er.retry_interval, P3_RETRY_MS);
        bad++;
    }
    if (er.reason.len != sizeof(k_p3_reason) ||
        (er.reason.len && !er.reason.data) ||
        (er.reason.len &&
         memcmp(er.reason.data, k_p3_reason, sizeof(k_p3_reason)) != 0)) {
        fprintf(stderr, "FINBR %s: reason phrase differs\n", what);
        bad++;
    }
    return bad;
}


static moq_result_t p3_feed(moq_transport_bridge_t *b, void *vctx,
                            uint64_t transport_id, bool fin)
{
    (void)vctx;
    uint8_t msg[224];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, msg, sizeof(msg));
    moq_d18_publish_t p = { 0 };
    moq_bytes_t parts[] = { { k_p3_ns0, sizeof(k_p3_ns0) },
                            { k_p3_ns1, sizeof(k_p3_ns1) } };
    p.request_id = 0;
    p.track_namespace = (moq_namespace_t){ parts, 2 };
    p.track_name = (moq_bytes_t){ k_p3_name, sizeof(k_p3_name) };
    p.track_alias = P3_ALIAS;
    p.params.has_forward = true;
    p.params.forward = P3_FORWARD;
    p.params.has_largest = true;
    p.params.largest_group = P3_LARGEST_GROUP;
    p.params.largest_object = P3_LARGEST_OBJ;
    p.params.has_expires = true;
    p.params.expires_ms = P3_EXPIRES_MS;
    p.params.auth_token_count = 1;
    p.params.auth_tokens[0].alias_type = 3;           /* USE_VALUE */
    p.params.auth_tokens[0].token_type = P3_TOK_TYPE;
    p.params.auth_tokens[0].token_value =
        (moq_bytes_t){ k_p3_tok, sizeof(k_p3_tok) };
    p.track_properties = (moq_bytes_t){ k_p3_props, sizeof(k_p3_props) };
    p.dynamic_groups = true;
    if (moq_d18_encode_publish(&w, &p) != MOQ_OK) return MOQ_ERR_INTERNAL;
    return moq_transport_bridge_on_peer_bidi_bytes(
        b, transport_id, msg, moq_buf_writer_offset(&w), fin, 0);
}


static moq_result_t p3_terminal(moq_transport_bridge_t *b, void *vctx,
                                uint64_t transport_id)
{
    fin_bridge_run_t *r = (fin_bridge_run_t *)vctx;
    (void)b; (void)transport_id;
    moq_publication_t pub;
    pub._opaque = r->want_handle;
    moq_reject_publish_cfg_t c;
    moq_reject_publish_cfg_init(&c);
    c.error_code = MOQ_REQUEST_ERROR_NOT_SUPPORTED;
    c.can_retry = true;
    c.retry_after_ms = P3_RETRY_MS;
    c.reason = (moq_bytes_t){ k_p3_reason, sizeof(k_p3_reason) };
    return moq_session_reject_publish(r->tp.server, pub, &c, 0);
}


static const fin_bridge_family_t p3_family = {
    .owner_kind    = MOQ_REQ_PUBLISH,
    .pool_tag      = MOQ_HANDLE_POOL_PUBLICATION,
    .snap_size     = sizeof(p3_snap_t),
    .capture       = p3_capture,
    .check         = p3_check,
    .normalize_event  = p3_norm_event,
    .normalize_action = p3_norm_action,
    .want_request  = p3_want_request,
    .derive_slot   = p3_derive_slot,
    .check_live    = p3_check_live,
    .check_retired = p3_check_retired,
    .check_edges   = p3_check_edges,
    .check_drain   = p3_check_drain,
    .check_terminal_wire = p3_check_terminal_wire,
};


int test_p3_bridge_fin_retirement(void)
{
    int failures = 0;
    fin_bridge_run_t r;
    memset(&r, 0, sizeof(r));
    /* ONE event slot on the destination: the request event fits, so its FIN
     * teardown must defer and the bridge must retain the obligation. */
    if (d18_pair_init_caps(&r.tp, 0, moq_alloc_default(),
                           moq_alloc_default(), 0, false, 1) < 0)
        return 1;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(r.tp.client, 0), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(r.tp.server, 0), (int)MOQ_OK);
    failures += d18_strict_shuttle(&r.tp, 30, 0, "p3 setup");
    {
        int cs = 0, co = 0, ss = 0, so = 0;
        moq_event_t ev;
        while (moq_session_poll_events(r.tp.client, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SETUP_COMPLETE) cs++; else co++;
            moq_event_cleanup(&ev);
        }
        while (moq_session_poll_events(r.tp.server, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SETUP_COMPLETE) ss++; else so++;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(cs, 1);
        MOQ_TEST_CHECK_EQ_INT(co, 0);
        MOQ_TEST_CHECK_EQ_INT(ss, 1);
        MOQ_TEST_CHECK_EQ_INT(so, 0);
    }
    fake_endpoint_clear_ops(&r.tp.server_ep);
    fake_endpoint_clear_ops(&r.tp.client_ep);
    r.transport_id = 400;

    fin_bridge_case_t f;
    memset(&f, 0, sizeof(f));
    f.name = "p3 publish";
    f.ctx = &r;
    f.ingress = FIN_BR_RETAINED;
    f.family = &p3_family;
    f.feed = p3_feed;
    f.terminal = p3_terminal;

    failures += run_fin_bridge(&f, &r);
    test_pair_destroy(&r.tp);
    return failures;
}
