#include "test_cases.h"

/* -- P7 TRACK_STATUS family ----------------------------------------- */

#define P7_TOK_TYPE 7

static const uint8_t k_p7_tok[]  = { 't','s','t','o','k' };

static const uint8_t k_p7_ns0[]  = { 'e','x','.','c','o','m' };

static const uint8_t k_p7_ns1[]  = { 's','t','a','t' };

static const uint8_t k_p7_name[] = { 't','0' };

#define P7_LARGEST_GROUP 0x33

#define P7_LARGEST_OBJ   0x04

#define P7_EXPIRES_MS    5500


static const moq_ts_entry_t *p7_owner(const moq_session_t *s,
                                      moq_stream_ref_t ref)
{
    moq_request_endpoint_t ep = request_registry_find_by_streamref(s, ref);
    if (ep.kind != MOQ_REQ_TRACK_STATUS) return NULL;
    if (ep.slot < 0 || (size_t)ep.slot >= s->ts_cap) return NULL;
    return &s->track_statuses[ep.slot];
}


static int p7_pool_busy(const moq_session_t *s)
{
    int n = 0;
    for (size_t i = 0; i < s->ts_cap; i++)
        if (s->track_statuses[i].state != MOQ_TS_FREE) n++;
    return n;
}


static int p7_derive_slot(const moq_session_t *s, uint32_t *out_gen)
{
    for (size_t i = 0; i < s->ts_cap; i++)
        if (s->track_statuses[i].state == MOQ_TS_FREE) {
            *out_gen = s->track_statuses[i].generation | 1u;
            return (int)i;
        }
    return -1;
}


/* The peer's request id for the request under check. Peer-opened request ids
 * advance by two, so a second request on the same session legitimately carries
 * a different one; the expectation is declared by the caller rather than being
 * read back from the entry. */
static uint64_t p7_want_request_id;


/* The FIN fact the owner under check must carry. The first request arrives WITH
 * a FIN, so its owner latches one; a reused slot admitted from a request with
 * NO FIN must show a CLEARED latch. Declared by the caller rather than assumed
 * true, or a free that left a stale `true` behind would pass at reuse. */
static int p7_want_fin = 1;


static int p7_check_live(const moq_session_t *s, moq_stream_ref_t ref,
                         int want_slot, uint32_t want_gen, uint64_t want_handle,
                         const char *what)
{
    int bad = 0;
    if (want_slot < 0 || (size_t)want_slot >= s->ts_cap) {
        fprintf(stderr, "FINBR %s: declared slot out of range\n", what);
        return 1;
    }
    const moq_ts_entry_t *e = p7_owner(s, ref);
    if (!e) { fprintf(stderr, "FINBR %s: owner absent\n", what); return 1; }
    if (e != &s->track_statuses[want_slot]) {
        fprintf(stderr, "FINBR %s: owner in an undeclared slot\n", what);
        bad++;
    }
    if ((int)e->state != (int)MOQ_TS_PENDING_PUBLISHER) {
        fprintf(stderr, "FINBR %s: state %d\n", what, (int)e->state);
        bad++;
    }
    if ((int)e->role != (int)MOQ_TS_ROLE_PUBLISHER) {
        fprintf(stderr, "FINBR %s: role %d\n", what, (int)e->role);
        bad++;
    }
    if (e->generation != want_gen) {
        fprintf(stderr, "FINBR %s: generation\n", what); bad++;
    }
    if (e->handle._opaque != want_handle) {
        fprintf(stderr, "FINBR %s: handle\n", what); bad++;
    }
    if (e->request_id != p7_want_request_id) {
        fprintf(stderr, "FINBR %s: request id\n", what); bad++;
    }
    if (e->rs.ref._v != ref._v) {
        fprintf(stderr, "FINBR %s: owner ref\n", what); bad++;
    }
    /* The transferred FIN -- this family's carrier is the durable latch. */
    if ((e->rs.recv_fin ? 1 : 0) != p7_want_fin) {
        fprintf(stderr, "FINBR %s: FIN latch %d, expected %d\n", what,
                e->rs.recv_fin ? 1 : 0, p7_want_fin);
        bad++;
    }
    if (p7_pool_busy(s) != 1) {
        fprintf(stderr, "FINBR %s: pool occupancy %d, expected 1\n", what,
                p7_pool_busy(s));
        bad++;
    }
    return bad;
}


static int p7_check_retired(const moq_session_t *s, moq_stream_ref_t ref,
                            int want_slot, const char *what)
{
    int bad = 0;
    if (p7_owner(s, ref) != NULL) {
        fprintf(stderr, "FINBR %s: registry edge survives\n", what); bad++;
    }
    /* The POOL too: removing the edge while leaking the slot must not pass. */
    if (want_slot >= 0 && (size_t)want_slot < s->ts_cap &&
        s->track_statuses[want_slot].state != MOQ_TS_FREE) {
        fprintf(stderr, "FINBR %s: pool slot leaked\n", what); bad++;
    }
    if (p7_pool_busy(s) != 0) {
        fprintf(stderr, "FINBR %s: pool occupancy %d, expected 0\n", what,
                p7_pool_busy(s));
        bad++;
    }
    return bad;
}


/* The request the producer must surface, built from the DECLARED handle. */
static int p7_want_request(txs_norm_vec_t *v, uint64_t handle)
{
    txs_img_t im;
    txs_img_init(&im);
    txs_img_u64(&im, handle);
    txs_img_u64(&im, 2);                                /* ns part count */
    txs_img_bytes(&im, k_p7_ns0, sizeof(k_p7_ns0));
    txs_img_bytes(&im, k_p7_ns1, sizeof(k_p7_ns1));
    txs_img_bytes(&im, k_p7_name, sizeof(k_p7_name));
    txs_img_u64(&im, 1);                                /* token count */
    txs_img_u64(&im, P7_TOK_TYPE);
    txs_img_bytes(&im, k_p7_tok, sizeof(k_p7_tok));
    if (!txs_norm_append_img(v, MOQ_EVENT_TRACK_STATUS_REQUEST, &im)) {
        fprintf(stderr, "FINBR: could not build the declared request image\n");
        return 1;
    }
    return 0;
}


static int p7_check_terminal_wire(void *vctx, const char *what);


/* The family's EXACT topology: one stream-ref edge while live, none at all
 * once retired. */
static int p7_check_edges(const og_graph_t *g, moq_stream_ref_t ref,
                          int want_slot, int live, const char *what)
{
    int bad = og_check_integrity(g, what);
    if (live) {
        bad += og_check_edge(g, OG_DOM_REQ_STREAMREF, ref._v,
                             MOQ_REQ_TRACK_STATUS, want_slot, what);
        const og_edge_spec_t w[] = { { OG_DOM_REQ_STREAMREF, ref._v } };
        bad += og_check_owner_edges(g, MOQ_REQ_TRACK_STATUS, want_slot, w, 1,
                                    what);
    } else {
        bad += og_check_no_edge(g, OG_DOM_REQ_STREAMREF, ref._v, what);
        bad += og_check_owner_edges(g, MOQ_REQ_TRACK_STATUS, want_slot, NULL,
                                    0, what);
    }
    bad += og_check_no_edge(g, OG_DOM_NS_REF, ref._v, what);
    return bad;
}


/* This route owes NO drain at any phase: the FIN is observed before the
 * terminal runs, so the declared multiset is EMPTY throughout. */
static int p7_check_drain(const moq_session_t *s, const char *what)
{
    return check_drain_membership(s, NULL, 0, what);
}



/* -- the descriptor's hooks, all four genuinely consumed -------------- */


static void p7_capture(const moq_session_t *s, void *vctx, void *state,
                       size_t cap)
{
    fin_bridge_run_t *r = (fin_bridge_run_t *)vctx;
    p7_snap_t *o = (p7_snap_t *)state;
    /* The declared size is the contract; refuse rather than overwrite. */
    if (cap < sizeof(*o)) { fprintf(stderr, "FINBR: snapshot storage too small\n"); return; }
    memset(o, 0, sizeof(*o));
    if (r->want_slot < 0 || (size_t)r->want_slot >= s->ts_cap) return;
    const moq_ts_entry_t *e = &s->track_statuses[r->want_slot];
    o->raw = *e;
    o->tid_len = e->track_id_len;
    if (e->track_id_len > P7_OWN_MAX || (e->track_id_len && !e->track_id_buf))
        return;
    if (e->track_id_len) memcpy(o->tid, e->track_id_buf, e->track_id_len);
    o->valid = 1;
}


static int p7_check(const moq_session_t *s, void *vctx, const void *state,
                    size_t cap, const char *what)
{
    const p7_snap_t *want = (const p7_snap_t *)state;
    p7_snap_t now;
    if (cap < sizeof(now)) {
        fprintf(stderr, "FINBR %s: snapshot storage too small\n", what);
        return 1;
    }
    p7_capture(s, vctx, &now, sizeof(now));
    if (!now.valid || !want->valid) {
        fprintf(stderr, "FINBR %s: incomparable owner record\n", what);
        return 1;
    }
    if (memcmp(&now.raw, &want->raw, sizeof(now.raw)) != 0) {
        fprintf(stderr, "FINBR %s: owner record changed\n", what);
        return 1;
    }
    if (now.tid_len != want->tid_len ||
        (now.tid_len && memcmp(now.tid, want->tid, now.tid_len) != 0)) {
        fprintf(stderr, "FINBR %s: retained track identity changed\n", what);
        return 1;
    }
    return 0;
}


static bool p7_norm_event(const moq_event_t *ev, void *ctx, txs_norm_vec_t *out)
{
    (void)ctx;
    if (ev->kind != MOQ_EVENT_TRACK_STATUS_REQUEST) {
        fprintf(stderr, "FINBR: unnormalized event kind %u\n",
                (unsigned)ev->kind);
        return false;
    }
    const moq_track_status_request_event_t *q = &ev->u.track_status_request;
    txs_img_t im;
    txs_img_init(&im);
    txs_img_u64(&im, q->handle._opaque);
    if (q->track_namespace.count > 32 || q->token_count > 16) {
        fprintf(stderr, "FINBR: implausible request counts\n");
        return false;
    }
    if (q->track_namespace.count && !q->track_namespace.parts) {
        fprintf(stderr, "FINBR: namespace count with NULL parts\n");
        return false;
    }
    if (q->token_count && !q->tokens) {
        fprintf(stderr, "FINBR: token count with NULL tokens\n");
        return false;
    }
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
        fprintf(stderr, "FINBR: track name has NULL bytes\n");
        return false;
    }
    txs_img_bytes(&im, q->track_name.data, q->track_name.len);
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
    return txs_norm_append_img(out, ev->kind, &im);
}


/* The bridge drains the session's actions itself, so anything still queued
 * afterwards is unexpected by definition. */
static bool p7_norm_action(const moq_action_t *a, void *ctx, txs_norm_vec_t *out)
{
    (void)ctx; (void)out;
    fprintf(stderr, "FINBR: action kind %u left queued after service\n",
            (unsigned)a->kind);
    return false;
}


static moq_result_t p7_feed_id(moq_transport_bridge_t *b, uint64_t request_id,
                               uint64_t transport_id, bool fin)
{
    uint8_t msg[192];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, msg, sizeof(msg));
    moq_bytes_t parts[] = { { k_p7_ns0, sizeof(k_p7_ns0) },
                            { k_p7_ns1, sizeof(k_p7_ns1) } };
    moq_namespace_t ns = { parts, 2 };
    moq_d18_msg_params_t p = { 0 };
    p.auth_token_count = 1;
    p.auth_tokens[0].alias_type = 3;              /* USE_VALUE */
    p.auth_tokens[0].token_type = P7_TOK_TYPE;
    p.auth_tokens[0].token_value = (moq_bytes_t){ k_p7_tok, sizeof(k_p7_tok) };
    if (moq_d18_encode_track_status(&w, request_id, &ns,
            (moq_bytes_t){ k_p7_name, sizeof(k_p7_name) }, &p) != MOQ_OK)
        return MOQ_ERR_INTERNAL;
    return moq_transport_bridge_on_peer_bidi_bytes(
        b, transport_id, msg, moq_buf_writer_offset(&w), fin, 0);
}


moq_result_t p7_feed(moq_transport_bridge_t *b, void *vctx,
                     uint64_t transport_id, bool fin)
{
    (void)vctx;
    return p7_feed_id(b, 0, transport_id, fin);
}


moq_result_t p7_terminal(moq_transport_bridge_t *b, void *vctx,
                         uint64_t transport_id)
{
    fin_bridge_run_t *r = (fin_bridge_run_t *)vctx;
    (void)b; (void)transport_id;
    moq_track_status_handle_t h;
    h._opaque = r->want_handle;
    moq_accept_track_status_cfg_t c;
    moq_accept_track_status_cfg_init(&c);
    c.has_largest = true;
    c.largest_group = P7_LARGEST_GROUP;
    c.largest_object = P7_LARGEST_OBJ;
    c.has_expires = true;
    c.expires_ms = P7_EXPIRES_MS;
    return moq_session_accept_track_status(r->tp.server, h, &c, 0);
}


const fin_bridge_family_t p7_family = {
    .owner_kind    = MOQ_REQ_TRACK_STATUS,
    .pool_tag      = MOQ_HANDLE_POOL_TRACK_STATUS,
    .snap_size     = sizeof(p7_snap_t),
    .capture       = p7_capture,
    .check         = p7_check,
    .normalize_event  = p7_norm_event,
    .normalize_action = p7_norm_action,
    .want_request  = p7_want_request,
    .derive_slot   = p7_derive_slot,
    .check_live    = p7_check_live,
    .check_retired = p7_check_retired,
    .check_edges   = p7_check_edges,
    .check_drain   = p7_check_drain,
    .check_terminal_wire = p7_check_terminal_wire,
};


/* The EXACT terminal the service must put on the wire, decoded rather than
 * counted: one write, on this transport stream, FIN'd, one REQUEST_OK envelope
 * with nothing after it, and a body carrying the declared status. */
static int p7_check_terminal_wire(void *vctx, const char *what)
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
        fprintf(stderr, "FINBR %s: undecodable envelope\n", what); return bad + 1;
    }
    if (env.msg_type != (uint64_t)MOQ_D18_REQUEST_OK) {
        fprintf(stderr, "FINBR %s: msg type %llu\n", what,
                (unsigned long long)env.msg_type);
        bad++;
    }
    if (moq_buf_reader_remaining(&rr) != 0) {
        fprintf(stderr, "FINBR %s: trailing bytes after the envelope\n", what);
        bad++;
    }
    moq_d18_track_status_ok_t ok;
    memset(&ok, 0, sizeof(ok));
    if (moq_d18_decode_track_status_ok(env.payload, env.payload_len,
                                       &ok) != MOQ_OK) {
        fprintf(stderr, "FINBR %s: undecodable TRACK_STATUS_OK body\n", what);
        return bad + 1;
    }
    if (!ok.params.has_largest || ok.params.largest_group != P7_LARGEST_GROUP ||
        ok.params.largest_object != P7_LARGEST_OBJ) {
        fprintf(stderr, "FINBR %s: largest mismatch\n", what); bad++;
    }
    if (!ok.params.has_expires || ok.params.expires_ms != P7_EXPIRES_MS) {
        fprintf(stderr, "FINBR %s: expires mismatch\n", what); bad++;
    }
    if (ok.track_properties.len != 0) {
        fprintf(stderr, "FINBR %s: non-empty track properties\n", what); bad++;
    }
    return bad;
}


static int p7_fixture_setup(fin_bridge_run_t *r, const char *what)
{
    int failures = 0;
    memset(r, 0, sizeof(*r));
    if (d18_pair_init(&r->tp, 0) < 0) return -1;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(r->tp.client, 0), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(r->tp.server, 0), (int)MOQ_OK);
    failures += d18_strict_shuttle(&r->tp, 30, 0, what);
    /* Setup is classified, not discarded: exactly one SETUP_COMPLETE per side
     * and nothing else. */
    {
        int cs = 0, co = 0, ss = 0, so = 0;
        moq_event_t ev;
        while (moq_session_poll_events(r->tp.client, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SETUP_COMPLETE) cs++; else co++;
            moq_event_cleanup(&ev);
        }
        while (moq_session_poll_events(r->tp.server, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SETUP_COMPLETE) ss++; else so++;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(cs, 1);
        MOQ_TEST_CHECK_EQ_INT(co, 0);
        MOQ_TEST_CHECK_EQ_INT(ss, 1);
        MOQ_TEST_CHECK_EQ_INT(so, 0);
    }
    fake_endpoint_clear_ops(&r->tp.server_ep);
    fake_endpoint_clear_ops(&r->tp.client_ep);
    r->transport_id = 400;                /* peer-opened client-initiated bidi */
    return failures;
}


static void p7_case_init(fin_bridge_case_t *f, fin_bridge_run_t *r)
{
    memset(f, 0, sizeof(*f));
    f->name = "p7 track-status";
    f->ctx = r;
    f->ingress = FIN_BR_CONSUMED;
    f->family = &p7_family;
    f->feed = p7_feed;
    f->terminal = p7_terminal;
}


int test_p7_bridge_fin_retirement(void)
{
    int failures = 0;
    fin_bridge_run_t r;
    int rc = p7_fixture_setup(&r, "p7 setup");
    if (rc < 0) return 1;
    failures += rc;

    fin_bridge_case_t f;
    p7_case_init(&f, &r);

    failures += run_fin_bridge(&f, &r);
    test_pair_destroy(&r.tp);
    return failures;
}


/* Axis 4, on the family whose whole lifecycle is reachable today: once the
 * owner has been retired, retirement must be COMPLETE and hold under repetition
 * and reuse. Three obligations, each non-vacuous against the current source:
 *
 *   - a repeated APPLICATION terminal on the retired handle is refused and
 *     changes nothing -- no wire byte, no event, no action, no resurrected
 *     owner, no drain, no edge;
 *   - a fresh request REUSES the slot with an ADVANCED generation, so the two
 *     owners are distinguishable by handle;
 *   - the RETIRED handle stays refused after that reuse, which is the
 *     clear-exactly-once fact: a stale handle must not address the new owner.
 *
 * It deliberately asserts nothing about where a future FIN handoff marker
 * lives, and does not carry P3's retained-ingress assumption.
 */
static int p7_accept(fin_bridge_run_t *r, uint64_t handle)
{
    moq_track_status_handle_t h;
    h._opaque = handle;
    moq_accept_track_status_cfg_t c;
    moq_accept_track_status_cfg_init(&c);
    c.has_largest = true;
    c.largest_group = P7_LARGEST_GROUP;
    c.largest_object = P7_LARGEST_OBJ;
    c.has_expires = true;
    c.expires_ms = P7_EXPIRES_MS;
    return (int)moq_session_accept_track_status(r->tp.server, h, &c, 0);
}


/* No output of ANY kind: no endpoint op, no event, no action. */
static int p7_check_silent(fin_bridge_run_t *r, const char *what)
{
    int bad = 0;
    if (r->tp.server_ep.count != 0) {
        fprintf(stderr, "AXIS4 %s: %zu endpoint ops, expected 0\n", what,
                r->tp.server_ep.count);
        bad++;
    }
    moq_event_t ev;
    while (moq_session_poll_events(r->tp.server, &ev, 1) > 0) {
        fprintf(stderr, "AXIS4 %s: unexpected event kind %u\n", what,
                (unsigned)ev.kind);
        moq_event_cleanup(&ev);
        bad++;
    }
    moq_action_t a;
    while (moq_session_poll_actions(r->tp.server, &a, 1) > 0) {
        fprintf(stderr, "AXIS4 %s: unexpected action kind %u\n", what,
                (unsigned)a.kind);
        moq_action_cleanup(&a);
        bad++;
    }
    return bad;
}


/* The generic snapshot records the scratch cursor as it stands, but the next
 * advancing call reclaims it once the event queue has drained
 * (session_call_prepare). Normalizing that -- and only that, and only when the
 * captured queue was empty -- keeps real scratch mutation detectable. Same rule
 * the session-side FIN suite applies. */
static void p7_expect_after_call_prepare(txs_snapshot_t *snap)
{
    if (snap->event_depth == 0) snap->event_scratch_len = 0;
}


/* The DECLARED free record for `ts_free_entry` (session_track_status.c:27).
 * That free MEMSETS the entry and then restores exactly three things: the FREE
 * state, the next generation, and the co-allocated receive buffer. So the
 * expectation is "zeroed except those", with the owned track key required GONE
 * -- a narrowed cleanup that left any field behind is caught here rather than
 * by an absence check that a stale value also satisfies. */
typedef struct p7_free_expect {
    uint32_t        generation;
    struct {
        const uint8_t  * recv_buf;
        size_t recv_cap;
    } rs;
} p7_free_expect_t;


static int p7_check_free_record(const moq_session_t *s, int slot,
                                const p7_free_expect_t *w, const char *what)
{
    int bad = 0;
    if (slot < 0 || (size_t)slot >= s->ts_cap) {
        fprintf(stderr, "AXIS4 %s: slot out of range\n", what);
        return 1;
    }
    const moq_ts_entry_t *e = &s->track_statuses[slot];
#define P7_FREE_EQ(field, got, exp) do { \
    if ((uint64_t)(got) != (uint64_t)(exp)) { \
        fprintf(stderr, "AXIS4 %s: free record %s = %llu, expected %llu\n", \
                what, field, (unsigned long long)(got), \
                (unsigned long long)(exp)); \
        bad++; \
    } \
} while (0)
    P7_FREE_EQ("state", (int)e->state, (int)MOQ_TS_FREE);
    P7_FREE_EQ("generation", e->generation, w->generation);
    P7_FREE_EQ("req_recv_cap", e->rs.recv_cap, w->rs.recv_cap);
    /* Everything the memset zeroes. */
    P7_FREE_EQ("role", (int)e->role, 0);
    P7_FREE_EQ("handle", e->handle._opaque, 0);
    P7_FREE_EQ("request_id", e->request_id, 0);
    P7_FREE_EQ("request_stream_ref", e->rs.ref._v, 0);
    P7_FREE_EQ("req_recv_len", e->rs.recv_len, 0);
    P7_FREE_EQ("req_recv_fin", e->rs.recv_fin ? 1 : 0, 0);
    P7_FREE_EQ("goaway_sent", e->goaway_sent ? 1 : 0, 0);
    P7_FREE_EQ("track_id_len", e->track_id_len, 0);
#undef P7_FREE_EQ
    if (e->rs.recv_buf != w->rs.recv_buf) {
        fprintf(stderr, "AXIS4 %s: free record req_recv_buf pointer\n", what);
        bad++;
    }
    if (e->track_id_buf != NULL) {
        fprintf(stderr, "AXIS4 %s: owned track key survives the free\n", what);
        bad++;
    }
    if (e->hist != NULL) {
        fprintf(stderr, "AXIS4 %s: reserved history record survives\n", what);
        bad++;
    }
    return bad;
}


/* The retired physical mapping is gone under BOTH identities -- a stale entry
 * that lost only its transport-id key would still be reachable by ref -- and
 * the bridge itself is healthy with nothing owed. */
static int p7_check_mapping_absent(fin_bridge_run_t *r, uint64_t transport_id,
                                   moq_stream_ref_t old_ref, const char *what)
{
    int bad = 0;
    if (bridge_find_by_id(r->tp.server_bridge, transport_id) != NULL) {
        fprintf(stderr, "AXIS4 %s: retired transport id still maps\n", what);
        bad++;
    }
    if (bridge_find_by_ref(r->tp.server_bridge, old_ref) != NULL) {
        fprintf(stderr, "AXIS4 %s: retired internal ref still maps\n", what);
        bad++;
    }
    if (moq_transport_bridge_is_fatal(r->tp.server_bridge)) {
        fprintf(stderr, "AXIS4 %s: bridge fatal\n", what); bad++;
    }
    if (moq_transport_bridge_is_closed(r->tp.server_bridge)) {
        fprintf(stderr, "AXIS4 %s: bridge closed\n", what); bad++;
    }
    if (moq_transport_bridge_has_pending(r->tp.server_bridge)) {
        fprintf(stderr, "AXIS4 %s: bridge has pending work\n", what); bad++;
    }
    return bad;
}


/* The reused peer bidi's EXACT bridge record. This is an OPEN-peer-bidi oracle,
 * deliberately not the FIN-consumed phase oracle: that phase expects
 * peer_send_closed, which is wrong for an entry admitted from a request with no
 * FIN. Both lookups must land on the SAME record -- a reverse-lookup repoint
 * that kept the transport id would satisfy a "some mapping remains" check --
 * and every flag is declared, so an otherwise-unobserved one cannot drift. */
static int p7_check_open_peer_bidi(fin_bridge_run_t *r, uint64_t transport_id,
                                   moq_stream_ref_t want_ref, const char *what)
{
    int bad = 0;
    bridge_stream_entry_t *by_id = bridge_find_by_id(r->tp.server_bridge,
                                                     transport_id);
    bridge_stream_entry_t *by_ref = bridge_find_by_ref(r->tp.server_bridge,
                                                       want_ref);
    if (!by_id) {
        fprintf(stderr, "AXIS4 %s: no bridge entry for transport id\n", what);
        return 1;
    }
    if (!by_ref) {
        fprintf(stderr, "AXIS4 %s: no bridge entry for internal ref\n", what);
        return 1;
    }
    if (by_id != by_ref) {
        fprintf(stderr, "AXIS4 %s: the two lookups reach DIFFERENT records\n",
                what);
        bad++;
    }
#define P7_BR_EQ(field, got, exp) do { \
    if ((uint64_t)(got) != (uint64_t)(exp)) { \
        fprintf(stderr, "AXIS4 %s: bridge entry %s = %llu, expected %llu\n", \
                what, field, (unsigned long long)(got), \
                (unsigned long long)(exp)); \
        bad++; \
    } \
} while (0)
    P7_BR_EQ("transport_id", by_id->transport_id, transport_id);
    P7_BR_EQ("ref", by_id->ref._v, want_ref._v);
    P7_BR_EQ("kind", (int)by_id->kind, (int)BRIDGE_STREAM_BIDI);
    P7_BR_EQ("origin", (int)by_id->origin, (int)BRIDGE_ORIGIN_PEER);
    P7_BR_EQ("active", by_id->active ? 1 : 0, 1);
    /* An OPEN peer bidi with no FIN and no teardown: every one of these is
     * false, and each is named so a single drifting flag is attributable. */
    P7_BR_EQ("peer_send_closed", by_id->peer_send_closed ? 1 : 0, 0);
    P7_BR_EQ("local_send_closed", by_id->local_send_closed ? 1 : 0, 0);
    P7_BR_EQ("aborting", by_id->aborting ? 1 : 0, 0);
    P7_BR_EQ("pending_retry", by_id->pending_retry ? 1 : 0, 0);
    P7_BR_EQ("pending_fin", by_id->pending_fin ? 1 : 0, 0);
    P7_BR_EQ("fin_retained", by_id->fin_retained ? 1 : 0, 0);
    P7_BR_EQ("pending_reset", by_id->pending_reset ? 1 : 0, 0);
    P7_BR_EQ("pending_stop", by_id->pending_stop ? 1 : 0, 0);
    P7_BR_EQ("pending_reset_code", by_id->pending_reset_code, 0);
    P7_BR_EQ("pending_stop_code", by_id->pending_stop_code, 0);
    /* Uni-only classification residue stays in its initialized zero state on a
     * bidi entry. */
    P7_BR_EQ("uni_disp", by_id->uni_disp, (uint8_t)BRIDGE_UNI_DISP_PENDING);
    P7_BR_EQ("classify_len", by_id->classify_len, 0);
#undef P7_BR_EQ
    /* Length zero does not imply the buffer is clean: every retained
     * classification byte must be in its initialized zero state. */
    for (size_t ci = 0; ci < sizeof(by_id->classify_buf); ci++) {
        if (by_id->classify_buf[ci] != 0) {
            fprintf(stderr,
                    "AXIS4 %s: bridge entry classify_buf[%zu] = %u, expected 0\n",
                    what, ci, (unsigned)by_id->classify_buf[ci]);
            bad++;
        }
    }
    /* And the bridge itself is still healthy with nothing owed. */
    if (moq_transport_bridge_is_fatal(r->tp.server_bridge)) {
        fprintf(stderr, "AXIS4 %s: bridge fatal\n", what); bad++;
    }
    if (moq_transport_bridge_is_closed(r->tp.server_bridge)) {
        fprintf(stderr, "AXIS4 %s: bridge closed\n", what); bad++;
    }
    if (moq_transport_bridge_has_pending(r->tp.server_bridge)) {
        fprintf(stderr, "AXIS4 %s: bridge has pending work\n", what); bad++;
    }
    return bad;
}


int test_p7_retirement_idempotence_and_reuse(void)
{
    int failures = 0;
    fin_bridge_run_t r;
    int rc = p7_fixture_setup(&r, "axis4 setup");
    if (rc < 0) return 1;
    failures += rc;

    fin_bridge_case_t f;
    p7_case_init(&f, &r);

    /* The co-allocated receive buffer persists across entry reuse, so its
     * pointer and capacity are properties of the FREE slot: captured BEFORE
     * ingress rather than adopted from the record the free produces. */
    const int slot_pre = r.want_slot;
    MOQ_TEST_CHECK(slot_pre >= 0 && (size_t)slot_pre < r.tp.server->ts_cap);
    const uint8_t *pre_recv_buf = r.tp.server->track_statuses[slot_pre].rs.recv_buf;
    const size_t   pre_recv_cap = r.tp.server->track_statuses[slot_pre].rs.recv_cap;

    failures += run_fin_bridge(&f, &r);

    const uint64_t retired_handle = r.want_handle;
    const int      slot           = r.want_slot;
    const moq_stream_ref_t old_ref = r.ref;

    /* The declared free record: the generation the LIVE owner carried (taken
     * from its declared handle, which the fixture derived pre-ingress) plus
     * one, and the buffer identity captured above. Nothing is read back from
     * the freed entry. */
    p7_free_expect_t want_free;
    want_free.generation   = moq_handle_generation(retired_handle) + 1u;
    want_free.rs.recv_buf = pre_recv_buf;
    want_free.rs.recv_cap = pre_recv_cap;
    failures += p7_check_free_record(r.tp.server, slot, &want_free,
                                     "first retirement");

    /* 1. A repeated application terminal on the retired handle. It must not
     *    mutate unrelated session state, advance the generation a second time,
     *    change the graph, add a drain, or recreate the retired owner. */
    fake_endpoint_clear_ops(&r.tp.server_ep);
    og_graph_t g_pre_repeat;
    og_capture(r.tp.server, &g_pre_repeat);
    {
        txs_snapshot_t before;
        txs_capture(r.tp.server, &old_ref, 1, &before);
        p7_expect_after_call_prepare(&before);
        MOQ_TEST_CHECK_EQ_INT(p7_accept(&r, retired_handle),
                              (int)MOQ_ERR_STALE_HANDLE);
        failures += p7_check_silent(&r, "repeat terminal");
        failures += txs_check_eq(r.tp.server, &old_ref, 1, &before,
                                 "repeat terminal");
    }
    failures += p7_family.check_retired(r.tp.server, old_ref, slot,
                                        "repeat terminal");
    failures += p7_family.check_drain(r.tp.server, "repeat terminal");
    /* The SAME declared record: no second generation increment. */
    failures += p7_check_free_record(r.tp.server, slot, &want_free,
                                     "repeat terminal");
    {
        og_graph_t g;
        og_capture(r.tp.server, &g);
        failures += og_check_integrity(&g, "repeat terminal");
        /* The WHOLE topology, not just the target's edges: an unrelated edge
         * inserted, removed or repointed by this refused call is caught here. */
        failures += og_check_same_topology(&g, &g_pre_repeat, "repeat terminal");
        failures += p7_family.check_edges(&g, old_ref, slot, 0,
                                          "repeat terminal");
    }
    failures += p7_check_mapping_absent(&r, r.transport_id, old_ref,
                                        "repeat terminal");

    /* 2. A fresh request reuses the slot with an advanced generation. The new
     *    identity is DERIVED from pool state before ingress, never read back. */
    /* Derived from the DECLARED freed generation, not from a fresh read of the
     *    now-free slot -- a free that advanced the generation twice, or not at
     *    all, must not be able to define the expectation it is checked against. */
    const uint32_t new_gen = want_free.generation | 1u;
    const int new_slot = slot;
    uint64_t new_handle = moq_handle_pack(p7_family.pool_tag,
                                          r.tp.server->session_tag, new_gen,
                                          (uint32_t)new_slot);
    MOQ_TEST_CHECK(new_handle != retired_handle);
    {   /* The pool really does hand back the same slot. */
        uint32_t probe_gen = 0;
        MOQ_TEST_CHECK_EQ_INT(p7_family.derive_slot(r.tp.server, &probe_gen),
                              slot);
        MOQ_TEST_CHECK_EQ_U64(probe_gen, (uint64_t)new_gen);
    }

    const uint64_t reuse_id = 404;                  /* a second peer-opened bidi */
    p7_want_request_id = 2;                         /* peer ids advance by two */
    /* NO FIN this time: the reused owner must show a CLEARED latch, which is
     * what makes ts_free_entry's cleanup of that field observable. */
    p7_want_fin = 0;
    fake_endpoint_clear_ops(&r.tp.server_ep);
    MOQ_TEST_CHECK_EQ_INT(
        (int)p7_feed_id(r.tp.server_bridge, 2, reuse_id, false), (int)MOQ_OK);
    {
        bridge_stream_entry_t *e = bridge_find_by_id(r.tp.server_bridge, reuse_id);
        MOQ_TEST_CHECK(e != NULL);
        if (!e) return failures;             /* the check above counted it */
        moq_stream_ref_t new_ref = e->ref;
        MOQ_TEST_CHECK(new_ref._v != old_ref._v);
        failures += p7_family.check_live(r.tp.server, new_ref, new_slot, new_gen,
                                         new_handle, "reused");
    }
    {   /* The request surfaces against the DERIVED handle, and only it. */
        txs_norm_vec_t got, want;
        txs_norm_init(&got); txs_norm_init(&want);
        moq_event_t ev;
        while (moq_session_poll_events(r.tp.server, &ev, 1) > 0) {
            if (!p7_family.normalize_event(&ev, &r, &got)) failures++;
            moq_event_cleanup(&ev);
        }
        failures += p7_family.want_request(&want, new_handle);
        failures += txs_norm_equals(&got, &want, "reused request");
        txs_norm_free(&got); txs_norm_free(&want);
    }

    /* 3. The retired handle is STILL refused, although its slot is occupied
     *    again -- a stale handle must never address the new owner -- and the
     *    REPLACEMENT owner survives that refusal WHOLE. */
    fake_endpoint_clear_ops(&r.tp.server_ep);
    {
        bridge_stream_entry_t *be = bridge_find_by_id(r.tp.server_bridge,
                                                      reuse_id);
        MOQ_TEST_CHECK(be != NULL);
        if (be) {
            moq_stream_ref_t new_ref = be->ref;
            fin_bridge_snap_t owner_before;
            p7_family.capture(r.tp.server, &r, owner_before.bytes,
                              p7_family.snap_size);
            txs_snapshot_t before;
            txs_capture(r.tp.server, &new_ref, 1, &before);
            p7_expect_after_call_prepare(&before);
            og_graph_t g_pre_stale;
            og_capture(r.tp.server, &g_pre_stale);

            MOQ_TEST_CHECK_EQ_INT(p7_accept(&r, retired_handle),
                                  (int)MOQ_ERR_STALE_HANDLE);

            failures += p7_check_silent(&r, "stale after reuse");
            failures += txs_check_eq(r.tp.server, &new_ref, 1, &before,
                                     "stale after reuse");
            failures += p7_family.check(r.tp.server, &r, owner_before.bytes,
                                        p7_family.snap_size,
                                        "stale after reuse");
            failures += p7_family.check_live(r.tp.server, new_ref, new_slot,
                                             new_gen, new_handle,
                                             "stale after reuse");
            failures += p7_family.check_drain(r.tp.server, "stale after reuse");
            {
                og_graph_t g;
                og_capture(r.tp.server, &g);
                failures += og_check_integrity(&g, "stale after reuse");
                failures += og_check_same_topology(&g, &g_pre_stale,
                                                   "stale after reuse");
                failures += p7_family.check_edges(&g, new_ref, new_slot, 1,
                                                  "stale after reuse");
                /* The retired stream keys nothing, though its slot is live. */
                failures += og_check_no_edge(&g, OG_DOM_REQ_STREAMREF,
                                             old_ref._v, "stale after reuse");
            }
            /* Both identities, with the flags this phase requires. */
            failures += p7_check_open_peer_bidi(&r, reuse_id, new_ref,
                                                "stale after reuse");
            failures += p7_check_mapping_absent(&r, r.transport_id, old_ref,
                                                "stale after reuse");
        }
    }

    p7_want_request_id = 0;
    p7_want_fin = 1;
    test_pair_destroy(&r.tp);
    return failures;
}
