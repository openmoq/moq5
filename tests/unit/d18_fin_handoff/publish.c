#include "test_cases.h"

static moq_result_t pub_feed(moq_session_t *s, void *ctx, moq_stream_ref_t ref,
                             bool fin)
{
    pub_ctx_t *c = (pub_ctx_t *)ctx;
    return feed_publish(s, ref, c->request_id, c->alias, fin);
}


static moq_result_t pub_app_reject(moq_session_t *s, void *ctx, uint64_t handle)
{
    (void)ctx;
    moq_publication_t pub;
    pub._opaque = handle;
    moq_reject_publish_cfg_t rj;
    moq_reject_publish_cfg_init(&rj);
    rj.error_code = MOQ_REQUEST_ERROR_NOT_SUPPORTED;
    return moq_session_reject_publish(s, pub, &rj, 1);
}


/* PUBLISH offers only the application terminal: there is no rejection path
 * reachable before the request is surfaced, so the internal capability bit is
 * deliberately clear (pending-join FETCH will set it). */
pub_ctx_t pub_ctx = { 0, 7, { 1 }, 0 };


fin_owner_case_t pub_family(void)
{
    fin_owner_case_t f;
    memset(&f, 0, sizeof(f));
    f.name = "publish";
    f.ctx = &pub_ctx;
    f.caps = FIN_TERM_APP_REJECT;
    f.feed = pub_feed;
    f.owner_kind = MOQ_REQ_PUBLISH;
    f.owner_role = (int)MOQ_PUB_ROLE_SUBSCRIBER;
    f.owner_state = (int)MOQ_PUB_PENDING_SUBSCRIBER;
    f.app_reject = pub_app_reject;
    f.hooks.ctx = &pub_ctx;
    f.hooks.capture = pub_hook_capture;
    f.hooks.check = pub_hook_check;
    f.hooks.normalize_event = pub_norm_event;
    f.hooks.normalize_action = pub_norm_action;
    return f;
}


static void run_pub_case(const fin_owner_case_t *f, uint32_t max_events,
                         bool fin_in_request, bool fill_ring,
                         bool reject_in_window)
{
    moq_session_t *s = make_session_caps(MOQ_PERSPECTIVE_SERVER, max_events);
    pub_ctx_t *c = (pub_ctx_t *)f->ctx;
    moq_stream_ref_t ref = c->ref;
    c->last_pub = 0;

    /* Feeding the request: with the FIN in the same chunk and room for both
     * events the teardown runs immediately; with room for only the request
     * event it defers and reports backpressure. */
    bool defer = fin_in_request && max_events == 1;
    MOQ_TEST_CHECK_EQ_INT((int)f->feed(s, f->ctx, ref, fin_in_request),
                          (int)(defer ? MOQ_ERR_WOULD_BLOCK : MOQ_OK));

    txs_norm_vec_t got, want;
    txs_norm_init(&got);
    txs_norm_init(&want);
    collect_events(s, &f->hooks, &got);
    want_publish_request(&want, c->last_pub);
    if (fin_in_request && !defer)
        want_publish_finished(&want, c->last_pub);
    failures += txs_norm_equals(&got, &want, f->name);
    txs_norm_free(&got);
    txs_norm_free(&want);

    if (fin_in_request && !defer) {
        /* Consumed in the same call: nothing left to own or to answer. */
        check_pub_owner_retired(s, ref);
        MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, 0);
        check_no_output(s, &f->hooks);
        moq_session_destroy(s);
        return;
    }

    failures += pub_owner_present_problems(s, ref, f->owner_kind,
                                           f->owner_role, f->owner_state);
    pub_hook_state_t owner_before;
    f->hooks.capture(s, f->ctx, &owner_before);
    /* The handle the request surfaced is the live owner's handle. */
    MOQ_TEST_CHECK_EQ_U64(owner_before.handle, c->last_pub);
    /* Which FIN fact the owner holds, and that it holds only one. A deferred
     * teardown keeps the peer's close as transitional ownership -- the durable
     * latch is NOT written on the way in, so a terminal that reads the latch
     * alone cannot mistake the deferral for a completed close. */
    MOQ_TEST_CHECK_EQ_INT(owner_before.handoff_fin, defer ? 1 : 0);
    MOQ_TEST_CHECK_EQ_INT(owner_before.rs.recv_fin, 0);

    size_t filled = 0;
    if (fill_ring) {
        fill_drain_ring(s);
        filled = s->drain_ref_count;
    }

    if (!reject_in_window) {
        /* Retrying with no new bytes completes the deferred teardown exactly
         * once, without a drain reference and without wire output. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, false, 1),
            (int)MOQ_OK);
        txs_norm_init(&got);
        txs_norm_init(&want);
        collect_events(s, &f->hooks, &got);
        want_publish_finished(&want, c->last_pub);
        failures += txs_norm_equals(&got, &want, f->name);
        txs_norm_free(&got);
        txs_norm_free(&want);
        check_pub_owner_retired(s, ref);
        MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, 0);
        check_no_output(s, &f->hooks);
        moq_session_destroy(s);
        return;
    }

    /* The application answers. With the peer's close already observed the
     * rejection needs no drain reference, so an exhausted ring cannot refuse
     * it; without the close it must reserve one, and a full ring refuses. */
    bool expect_drain = !fin_in_request;
    moq_result_t want_rc = (expect_drain && fill_ring) ? MOQ_ERR_WOULD_BLOCK
                                                       : MOQ_OK;
    txs_snapshot_t before;
    txs_capture(s, &ref, 1, &before);
    expect_after_call_prepare(&before);
    MOQ_TEST_CHECK_EQ_INT((int)f->app_reject(s, f->ctx, c->last_pub),
                          (int)want_rc);

    if (want_rc == MOQ_ERR_WOULD_BLOCK) {
        /* Transactional blockage: the whole session, and the family's own
         * mutation inventory, are exactly as the refused call found them. */
        failures += txs_check_eq(s, &ref, 1, &before, f->name);
        failures += f->hooks.check(s, f->ctx, &owner_before);
        MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, filled);
        failures += pub_owner_present_problems(s, ref, f->owner_kind,
                                               f->owner_role, f->owner_state);
        check_no_output(s, &f->hooks);
        moq_session_destroy(s);
        return;
    }

    MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count,
                           expect_drain ? filled + 1 : filled);
    check_pub_owner_retired(s, ref);
    check_reject_only_output(s, &f->hooks, ref, f->name);

    if (expect_drain) {
        /* The peer's late FIN releases the reference and produces nothing. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, filled);
        check_pub_owner_retired(s, ref);
        check_no_output(s, &f->hooks);
    }
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
    moq_session_destroy(s);
}


/* Clear-on-reuse: a same-call FIN consumed by one publication must not follow
 * its pool slot to the next one. The second request carries no FIN, so the
 * reused owner must hold neither FIN fact -- and its terminal must still
 * reserve a drain reference for the peer's send half, which an exhausted ring
 * refuses. A marker that survived the free would make that rejection succeed
 * with no reference, silently abandoning a live peer half. */
static void run_pub_slot_reuse_case(const fin_owner_case_t *f)
{
    moq_session_t *s = make_session_caps(MOQ_PERSPECTIVE_SERVER, 0);
    pub_ctx_t *c = (pub_ctx_t *)f->ctx;

    /* The physical slot both requests must occupy, and the generations it
     * carries, derived from pool state BEFORE any ingress. Deriving them here
     * is what makes this a same-slot reuse case rather than a case that merely
     * happens to reuse whatever the allocator picked. */
    int slot = -1;
    uint32_t gen_first = 0;
    for (size_t i = 0; i < s->pub_cap; i++)
        if (s->publishes[i].state == MOQ_PUB_FREE) {
            slot = (int)i;
            gen_first = s->publishes[i].generation | 1u;
            break;
        }
    MOQ_TEST_CHECK(slot >= 0);
    if (slot < 0) { moq_session_destroy(s); return; }
    /* pub_free_entry advances the generation once; the reused owner's live
     * generation is that value re-oddified. */
    uint32_t gen_reuse = (gen_first + 1) | 1u;
    uint64_t handle_first = moq_handle_pack(MOQ_HANDLE_POOL_PUBLICATION,
                                            s->session_tag, gen_first,
                                            (uint32_t)slot);
    uint64_t handle_reuse = moq_handle_pack(MOQ_HANDLE_POOL_PUBLICATION,
                                            s->session_tag, gen_reuse,
                                            (uint32_t)slot);
    MOQ_TEST_CHECK(handle_first != handle_reuse);

    /* First request: same-call FIN, torn down immediately, slot released. */
    moq_stream_ref_t first = c->ref;
    MOQ_TEST_CHECK_EQ_INT((int)f->feed(s, f->ctx, first, true), (int)MOQ_OK);
    txs_norm_vec_t got, want;
    txs_norm_init(&got);
    txs_norm_init(&want);
    collect_events(s, &f->hooks, &got);
    want_publish_request(&want, c->last_pub);
    want_publish_finished(&want, c->last_pub);
    failures += txs_norm_equals(&got, &want, "publish reuse first");
    txs_norm_free(&got);
    txs_norm_free(&want);
    /* The handle the first request surfaced is the one the derived slot and
     * generation pack into -- the fixture's topology, not a read-back. */
    MOQ_TEST_CHECK_EQ_U64(c->last_pub, handle_first);
    check_pub_owner_retired(s, first);
    /* That exact slot is free, its generation advanced as declared, and the
     * transitional marker went with it. */
    MOQ_TEST_CHECK_EQ_INT((int)s->publishes[slot].state, (int)MOQ_PUB_FREE);
    MOQ_TEST_CHECK_EQ_U64(s->publishes[slot].generation, gen_reuse - 1u);
    MOQ_TEST_CHECK_EQ_INT(s->publishes[slot].handoff_fin_pending, 0);
    MOQ_TEST_CHECK_EQ_INT(s->publishes[slot].rs.recv_fin, 0);

    /* Second request on a fresh bidi, no FIN: it lands in the released slot. */
    moq_stream_ref_t second = moq_stream_ref_from_u64(c->ref._v + 4);
    moq_stream_ref_t saved = c->ref;
    c->ref = second;
    MOQ_TEST_CHECK_EQ_INT(
        (int)feed_publish(s, second, c->request_id + 2, c->alias + 1, false),
        (int)MOQ_OK);
    /* Draining the request event both surfaces the new owner's handle and
     * proves the reused slot produced no second teardown. */
    txs_norm_init(&got);
    txs_norm_init(&want);
    collect_events(s, &f->hooks, &got);
    want_publish_request(&want, c->last_pub);
    failures += txs_norm_equals(&got, &want, "publish reuse second");
    txs_norm_free(&got);
    txs_norm_free(&want);
    failures += pub_owner_present_problems(s, second, f->owner_kind,
                                           f->owner_role, f->owner_state);
    /* The reuse landed in THAT physical slot, carrying the pre-derived
     * generation and handle, and the stream-ref registry edge points at it. */
    {
        const moq_pub_entry_t *e = pub_by_ref(s, second);
        MOQ_TEST_CHECK(e != NULL);
        if (e) {
            MOQ_TEST_CHECK_EQ_INT((int)(e - s->publishes), slot);
            MOQ_TEST_CHECK_EQ_U64(e->generation, gen_reuse);
            MOQ_TEST_CHECK_EQ_U64(e->handle._opaque, handle_reuse);
        }
        MOQ_TEST_CHECK_EQ_U64(c->last_pub, handle_reuse);
        moq_request_endpoint_t ep =
            request_registry_find_by_streamref(s, second);
        MOQ_TEST_CHECK_EQ_INT((int)ep.kind, (int)MOQ_REQ_PUBLISH);
        MOQ_TEST_CHECK_EQ_INT(ep.slot, slot);
    }
    pub_hook_state_t owner;
    f->hooks.capture(s, f->ctx, &owner);
    /* Neither FIN fact is set before the terminal check: the consumed FIN did
     * not follow the slot to its next owner. */
    MOQ_TEST_CHECK_EQ_INT(owner.handoff_fin, 0);
    MOQ_TEST_CHECK_EQ_INT(owner.rs.recv_fin, 0);

    /* The peer's send half is still open, so the terminal owes a drain
     * reference; with the ring exhausted it must refuse transactionally. */
    fill_drain_ring(s);
    size_t filled = s->drain_ref_count;
    txs_snapshot_t before;
    txs_capture(s, &second, 1, &before);
    expect_after_call_prepare(&before);
    MOQ_TEST_CHECK_EQ_INT((int)f->app_reject(s, f->ctx, c->last_pub),
                          (int)MOQ_ERR_WOULD_BLOCK);
    failures += txs_check_eq(s, &second, 1, &before, "publish reuse blocked");
    failures += f->hooks.check(s, f->ctx, &owner);
    MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, filled);
    check_no_output(s, &f->hooks);

    c->ref = saved;
    moq_session_destroy(s);
}


void test_publish(void)
{
    fin_owner_case_t f = pub_family();
    MOQ_TEST_CHECK_EQ_INT(fin_owner_problems(&f), 0);

    /* Control: no FIN in the request chunk -- one drain reference, released
     * by the peer's later FIN. */
    run_pub_case(&f, 0, false, false, true);
    /* Same-call FIN, room for both events: consumed immediately. */
    run_pub_case(&f, 0, true, false, false);
    /* Same-call FIN, room for one: deferred, then completed by the retry. */
    run_pub_case(&f, 1, true, false, false);
    /* Same-call FIN, answered inside the deferral window: no drain. */
    run_pub_case(&f, 1, true, false, true);
    /* The selector against one and the same exhausted ring. */
    run_pub_case(&f, 1, true, true, true);
    run_pub_case(&f, 1, false, true, true);
    /* The consumed FIN does not follow the slot to its next owner. */
    run_pub_slot_reuse_case(&f);
}



/* -- Route: finish PUBLISH (session_publish.c:1847, moq_session_finish_publish)
 *
 * The POSITIVE CONTROL of this tranche: unlike its five siblings the terminal
 * already spells the unified rule, `need_drain = req_stream && !req_recv_fin`
 * (:1885), so the latch rows must COMPLETE with no drain taken even against an
 * exhausted ring. A selector mutant below proves the rows are discriminating
 * rather than passing by construction. */
#define R4_TRACK_ALIAS 0x41

#define R4_STATUS      0x3

#define R4_STREAMS     0x6

#define R4_OK_PRIORITY 37

#define R4_OK_TIMEOUT  6000

#define R4_OK_EXPIRES  8000

static const char k_r4_reason[] = "publication done";

static const uint8_t k_r4_name[] = { 'v','0' };


static int g_r4_want_slot = -1;


int r4_owner_edges(const og_graph_t *g, moq_stream_ref_t ref,
                   const char *what)
{
    int bad = 0;
    if (g_r4_want_slot < 0) {
        OG_DIAG("OG %s: no declared publication slot\n", what);
        return 1;
    }
    bad += og_check_edge(g, OG_DOM_REQ_STREAMREF, ref._v,
                         MOQ_REQ_PUBLISH, g_r4_want_slot, what);
    {
        const og_edge_spec_t want[] = { { OG_DOM_REQ_STREAMREF, ref._v } };
        bad += og_check_owner_edges(g, MOQ_REQ_PUBLISH, g_r4_want_slot,
                                    want, 1, what);
    }
    bad += og_check_no_edge(g, OG_DOM_NS_REF, ref._v, what);
    return bad;
}


static const moq_pub_entry_t *r4_entry_by_ref(const moq_session_t *s,
                                              moq_stream_ref_t ref)
{
    moq_request_endpoint_t ep = request_registry_find_by_streamref(s, ref);
    if (ep.kind != MOQ_REQ_PUBLISH) return NULL;
    if (ep.slot < 0 || (size_t)ep.slot >= s->pub_cap) return NULL;
    return &s->publishes[ep.slot];
}


static void r4_owner_capture(const moq_session_t *s, moq_stream_ref_t ref,
                             seed_snap_t *o)
{
    req_seed_snap_t *r = &o->req;
    memset(r, 0, sizeof(*r));
    r->busy = pub_busy_count(s);
    const moq_pub_entry_t *e = r4_entry_by_ref(s, ref);
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


static void r4_owner_check(const moq_session_t *s, moq_stream_ref_t ref,
                           const seed_snap_t *want, const char *what)
{
    seed_snap_t now;
    r4_owner_capture(s, ref, &now);
    req_seed_diff(&now.req, &want->req, what);
}


static void r4_check_retired(const moq_session_t *s, moq_stream_ref_t ref)
{
    MOQ_TEST_CHECK_EQ_INT(pub_busy_count(s), 0);
    MOQ_TEST_CHECK(r4_entry_by_ref(s, ref) == NULL);
    check_unregistered(s, ref);
}


const owner_ops_t r4_owner_ops = {
    r4_owner_capture, r4_owner_check, r4_check_retired
};


/* The whole established publication, compared verbatim. */
typedef struct r4_full {
    int      valid;
    moq_pub_entry_t raw;
    size_t   req_len;
    uint8_t  req[R6_OWN_MAX];
} r4_full_t;


static r4_full_t g_r4_full;


static void r4_full_read(const moq_session_t *s, r4_full_t *v)
{
    memset(v, 0, sizeof(*v));
    if (g_r4_want_slot < 0 || (size_t)g_r4_want_slot >= s->pub_cap) return;
    const moq_pub_entry_t *e = &s->publishes[g_r4_want_slot];
    v->raw = *e;
    v->req_len = e->rs.recv_len;
    if (e->rs.recv_len > R6_OWN_MAX || (e->rs.recv_len && !e->rs.recv_buf))
        return;
    if (e->rs.recv_len) memcpy(v->req, e->rs.recv_buf, e->rs.recv_len);
    v->valid = 1;
}


static void r4_full_capture(const moq_session_t *s)
{
    r4_full_read(s, &g_r4_full);
    MOQ_TEST_CHECK_EQ_INT(g_r4_full.valid, 1);
}


static int r4_full_check_blocked(const moq_session_t *s, const char *what)
{
    r4_full_t now;
    r4_full_read(s, &now);
    int bad = 0;
    if (!now.valid || !g_r4_full.valid) {
        TXS_DIAG("TXN %s: incomparable publication record\n", what);
        return 1;
    }
    if (memcmp(&now.raw, &g_r4_full.raw, sizeof(now.raw)) != 0) {
        TXS_DIAG("TXN %s: established publication entry changed\n", what);
        bad++;
    }
    if (now.req_len != g_r4_full.req_len ||
        (now.req_len && memcmp(now.req, g_r4_full.req, now.req_len) != 0)) {
        TXS_DIAG("TXN %s: retained response bytes changed\n", what);
        bad++;
    }
    return bad;
}


static int r4_full_check_committed(const moq_session_t *s, const char *what)
{
    if (g_r4_want_slot < 0 || (size_t)g_r4_want_slot >= s->pub_cap) return 0;
    if (s->publishes[g_r4_want_slot].state != MOQ_PUB_FREE) {
        TXS_DIAG("TXN %s: finished publication slot is still live\n", what);
        return 1;
    }
    return 0;
}


const extra_ops_t r4_full_ops = {
    r4_full_capture, r4_full_check_blocked, r4_full_check_committed
};


moq_stream_ref_t arm_publisher_established(moq_session_t *s,
                                           bool fin_observed,
                                           uint64_t *out_handle)
{
    int want_slot = -1;
    uint32_t want_gen = 0;
    for (size_t i = 0; i < s->pub_cap; i++)
        if (s->publishes[i].state == MOQ_PUB_FREE) {
            want_slot = (int)i;
            want_gen = s->publishes[i].generation | 1u;
            break;
        }
    MOQ_TEST_CHECK(want_slot >= 0);
    if (want_slot < 0) return moq_stream_ref_from_u64(0);
    g_r4_want_slot = want_slot;

    moq_bytes_t parts[] = { { k_ann_ns0, sizeof(k_ann_ns0) },
                            { k_ann_ns1, sizeof(k_ann_ns1) } };
    moq_publish_cfg_t pc;
    moq_publish_cfg_init(&pc);
    pc.track_namespace = (moq_namespace_t){ parts, 2 };
    pc.track_name = MOQ_BYTES_LITERAL("v0");
    pc.has_track_alias = true;
    pc.track_alias = R4_TRACK_ALIAS;
    moq_publication_t ph = MOQ_PUBLICATION_INVALID;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_publish(s, &pc, 1, &ph),
                          (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(ph._opaque,
                          moq_handle_pack(MOQ_HANDLE_POOL_PUBLICATION,
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
                MOQ_TEST_CHECK_EQ_U64(env.msg_type, (uint64_t)MOQ_D18_PUBLISH);
                MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&rr),
                                       (size_t)0);
                /* The body against the fixture's DECLARED request. */
                moq_bytes_t dp[8];
                moq_d18_publish_t pb;
                memset(dp, 0, sizeof(dp));
                memset(&pb, 0, sizeof(pb));
                int dec_ok = (moq_d18_decode_publish(env.payload,
                                  env.payload_len, dp, 8, &pb) == MOQ_OK);
                MOQ_TEST_CHECK(dec_ok);
                if (dec_ok) {
                    MOQ_TEST_CHECK_EQ_U64(pb.request_id, 0);
                    MOQ_TEST_CHECK_EQ_U64(pb.track_alias, R4_TRACK_ALIAS);
                    MOQ_TEST_CHECK_EQ_SIZE(pb.track_namespace.count, (size_t)2);
                    if (pb.track_namespace.count == 2)
                        MOQ_TEST_CHECK(pb.track_namespace.parts != NULL);
                    if (pb.track_namespace.count == 2 &&
                        pb.track_namespace.parts) {
                        const moq_bytes_t *q0 = &pb.track_namespace.parts[0];
                        const moq_bytes_t *q1 = &pb.track_namespace.parts[1];
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
                    MOQ_TEST_CHECK_EQ_SIZE(pb.track_name.len,
                                           sizeof(k_r4_name));
                    MOQ_TEST_CHECK(pb.track_name.len == 0 ||
                                   pb.track_name.data != NULL);
                    if (pb.track_name.len == sizeof(k_r4_name) &&
                        pb.track_name.data)
                        MOQ_TEST_CHECK(memcmp(pb.track_name.data, k_r4_name,
                                              sizeof(k_r4_name)) == 0);
                    MOQ_TEST_CHECK_EQ_SIZE(pb.track_properties.len, (size_t)0);
                    MOQ_TEST_CHECK_EQ_INT(pb.dynamic_groups ? 1 : 0, 0);
                    MOQ_TEST_CHECK_EQ_SIZE(pb.params.auth_token_count,
                                           (size_t)0);
                }
            } else other++;
            moq_action_cleanup(&a);
        }
        MOQ_TEST_CHECK_EQ_INT(opens, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
    }
    MOQ_TEST_CHECK(ref._v != 0);
    if (ref._v == 0) return ref;

    /* The peer accepts, optionally closing its half in the same chunk. */
    uint8_t ok[128];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, ok, sizeof(ok));
    {
        /* Non-default throughout, so the event and the committed owner are
         * discriminating rather than matching whatever the defaults are. */
        moq_d18_msg_params_t p = { 0 };
        p.has_subscriber_priority = true;
        p.subscriber_priority = R4_OK_PRIORITY;
        p.has_group_order = true;
        p.group_order = 2;                    /* descending */
        p.has_forward = true;
        p.forward = true;
        p.has_object_delivery_timeout = true;
        p.object_delivery_timeout_ms = R4_OK_TIMEOUT;
        p.has_expires = true;
        p.expires_ms = R4_OK_EXPIRES;
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_publish_ok(&w, &p),
                              (int)MOQ_OK);
    }
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, ok,
                                              moq_buf_writer_offset(&w),
                                              fin_observed, 1), (int)MOQ_OK);
    {
        int oks = 0, other = 0;
        moq_event_t e;
        while (moq_session_poll_events(s, &e, 1) > 0) {
            if (e.kind == MOQ_EVENT_PUBLISH_OK) {
                oks++;
                const moq_publish_ok_event_t *k = &e.u.publish_ok;
                MOQ_TEST_CHECK_EQ_U64(k->pub._opaque, ph._opaque);
                MOQ_TEST_CHECK_EQ_INT(k->send_allowed ? 1 : 0, 1);
                MOQ_TEST_CHECK_EQ_U64(k->subscriber_priority, R4_OK_PRIORITY);
                MOQ_TEST_CHECK_EQ_INT((int)k->group_order,
                                      (int)MOQ_GROUP_ORDER_DESCENDING);
                MOQ_TEST_CHECK_EQ_INT(k->has_delivery_timeout ? 1 : 0, 1);
                MOQ_TEST_CHECK_EQ_U64(k->delivery_timeout_ms, R4_OK_TIMEOUT);
                MOQ_TEST_CHECK_EQ_INT(k->has_expires ? 1 : 0, 1);
                MOQ_TEST_CHECK_EQ_U64(k->expires_ms, R4_OK_EXPIRES);
            } else other++;
            moq_event_cleanup(&e);
        }
        MOQ_TEST_CHECK_EQ_INT(oks, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
    }

    {
        const moq_pub_entry_t *e = &s->publishes[want_slot];
        MOQ_TEST_CHECK_EQ_INT(pub_busy_count(s), 1);
        MOQ_TEST_CHECK_EQ_INT((int)e->state, (int)MOQ_PUB_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT((int)e->role, (int)MOQ_PUB_ROLE_PUBLISHER);
        MOQ_TEST_CHECK_EQ_U64(e->generation, want_gen);
        MOQ_TEST_CHECK_EQ_U64(e->handle._opaque, ph._opaque);
        MOQ_TEST_CHECK_EQ_U64(e->request_id, 0);
        MOQ_TEST_CHECK_EQ_U64(e->track_alias, R4_TRACK_ALIAS);
        MOQ_TEST_CHECK_EQ_U64(e->rs.ref._v, ref._v);
        MOQ_TEST_CHECK_EQ_INT(e->rs.recv_fin ? 1 : 0, fin_observed ? 1 : 0);
        MOQ_TEST_CHECK_EQ_INT(e->goaway_sent ? 1 : 0, 0);
        /* The acceptance's values committed onto the owner, independently of
         * the event that surfaced them. */
        MOQ_TEST_CHECK_EQ_INT(e->send_allowed ? 1 : 0, 1);
        MOQ_TEST_CHECK_EQ_U64(e->subscriber_priority, R4_OK_PRIORITY);
        MOQ_TEST_CHECK_EQ_INT((int)e->group_order,
                              (int)MOQ_GROUP_ORDER_DESCENDING);
        check_registered(s, ref, MOQ_REQ_PUBLISH, want_slot);
    }
    if (out_handle) *out_handle = ph._opaque;
    return ref;
}


moq_result_t fire_finish_publish(moq_session_t *s, uint64_t handle)
{
    moq_publication_t pub;
    pub._opaque = handle;
    moq_finish_publish_cfg_t c;
    memset(&c, 0, sizeof(c));
    c.struct_size = (uint32_t)sizeof(c);
    c.status_code = R4_STATUS;
    c.stream_count = R4_STREAMS;
    c.reason = (moq_bytes_t){ (const uint8_t *)k_r4_reason,
                              sizeof(k_r4_reason) - 1 };
    return moq_session_finish_publish(s, pub, &c, 1);
}


void want_finish_publish(txs_norm_vec_t *v, moq_stream_ref_t ref,
                         uint64_t handle)
{
    (void)handle;
    uint8_t buf[128];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_d18_encode_publish_done(
            &w, R4_STATUS, R4_STREAMS,
            (moq_bytes_t){ (const uint8_t *)k_r4_reason,
                           sizeof(k_r4_reason) - 1 }), (int)MOQ_OK);
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



/* -- Route: deferred publish finalize (session_publish.c:144, pub_finalize_done)
 *
 * The second POSITIVE CONTROL: the finalize already spells the unified rule,
 * `need_drain = request_stream_ref && !req_recv_fin` (:150), so all four rows
 * complete. It is reached only through the public deferred sweep -- a
 * PUBLISH_DONE whose Stream Count the subscriber has not yet satisfied defers,
 * and `moq_session_tick` past the stamped deadline finalizes it -- never by
 * calling the internal finalize directly. */
#define R1_ALIAS        0x11

#define R1_STATUS       0x4

#define R1_STREAMS      3

#define R1_DEADLINE_US  (31ull * 1000ull * 1000ull)

static const char k_r1_reason[] = "publisher finished";


static int g_r1_want_slot = -1;


int r1_owner_edges(const og_graph_t *g, moq_stream_ref_t ref,
                   const char *what)
{
    int bad = 0;
    if (g_r1_want_slot < 0) {
        OG_DIAG("OG %s: no declared publication slot\n", what);
        return 1;
    }
    bad += og_check_edge(g, OG_DOM_REQ_STREAMREF, ref._v,
                         MOQ_REQ_PUBLISH, g_r1_want_slot, what);
    {
        const og_edge_spec_t want[] = { { OG_DOM_REQ_STREAMREF, ref._v } };
        bad += og_check_owner_edges(g, MOQ_REQ_PUBLISH, g_r1_want_slot,
                                    want, 1, what);
    }
    bad += og_check_no_edge(g, OG_DOM_NS_REF, ref._v, what);
    return bad;
}


/* The whole deferred publication, compared verbatim -- including the deferral
 * state the generic snapshot does not model (done_pending, its status, stream
 * count, stamped deadline and expiry flag). */
typedef struct r1_full {
    int      valid;
    moq_pub_entry_t raw;
    size_t   reason_len;
    uint8_t  reason[R6_OWN_MAX];
} r1_full_t;


static r1_full_t g_r1_full;


static void r1_full_read(const moq_session_t *s, r1_full_t *v)
{
    memset(v, 0, sizeof(*v));
    if (g_r1_want_slot < 0 || (size_t)g_r1_want_slot >= s->pub_cap) return;
    const moq_pub_entry_t *e = &s->publishes[g_r1_want_slot];
    v->raw = *e;
    /* The entry only holds a POINTER to the deferred reason, so the bytes are
     * copied out: a mutation in place would otherwise compare equal. */
    v->reason_len = e->done_reason_len;
    if (e->done_reason_len > R6_OWN_MAX ||
        (e->done_reason_len && !e->done_reason_buf)) return;
    if (e->done_reason_len)
        memcpy(v->reason, e->done_reason_buf, e->done_reason_len);
    v->valid = 1;
}


static void r1_full_capture(const moq_session_t *s)
{
    r1_full_read(s, &g_r1_full);
    MOQ_TEST_CHECK_EQ_INT(g_r1_full.valid, 1);
    /* The due-mark must be PROVEN by the tick, so it has to start false. */
    MOQ_TEST_CHECK_EQ_INT(g_r1_full.raw.done_expired ? 1 : 0, 0);
}


static int r1_full_check_blocked(const moq_session_t *s, const char *what)
{
    r1_full_t now;
    r1_full_read(s, &now);
    if (!now.valid || !g_r1_full.valid) {
        TXS_DIAG("TXN %s: incomparable publication record\n", what);
        return 1;
    }
    /* A refused sweep owes zero mutation EXCEPT its own due-mark: the sweep
     * charges `done_expired` before attempting the finalize
     * (session_publish.c:265), so that one flip is declared and everything
     * else must be verbatim. */
    if (!now.raw.done_expired) {
        TXS_DIAG("TXN %s: refused sweep never marked the deferral due\n", what);
        return 1;
    }
    moq_pub_entry_t want = g_r1_full.raw;
    want.done_expired = true;
    if (memcmp(&now.raw, &want, sizeof(want)) != 0) {
        TXS_DIAG("TXN %s: deferred publication entry changed beyond its"
                 " due-mark\n", what);
        return 1;
    }
    if (now.reason_len != g_r1_full.reason_len ||
        (now.reason_len &&
         memcmp(now.reason, g_r1_full.reason, now.reason_len) != 0)) {
        TXS_DIAG("TXN %s: retained terminal reason changed\n", what);
        return 1;
    }
    if (!now.raw.done_pending) {
        TXS_DIAG("TXN %s: refused sweep dropped the deferral\n", what);
        return 1;
    }
    return 0;
}


static int r1_full_check_committed(const moq_session_t *s, const char *what)
{
    if (g_r1_want_slot < 0 || (size_t)g_r1_want_slot >= s->pub_cap) return 0;
    if (s->publishes[g_r1_want_slot].state != MOQ_PUB_FREE) {
        TXS_DIAG("TXN %s: finalized publication slot is still live\n", what);
        return 1;
    }
    return 0;
}


const extra_ops_t r1_full_ops = {
    r1_full_capture, r1_full_check_blocked, r1_full_check_committed
};


static uint64_t g_r1_handle;


moq_stream_ref_t arm_deferred_publish(moq_session_t *s, bool fin_observed,
                                      uint64_t *out_handle)
{
    /* Slot, generation and the handle they pack into, all derived from pool
     * state BEFORE the inbound PUBLISH is fed. */
    int want_slot = -1;
    uint32_t want_gen = 0;
    for (size_t i = 0; i < s->pub_cap; i++)
        if (s->publishes[i].state == MOQ_PUB_FREE) {
            want_slot = (int)i;
            want_gen = s->publishes[i].generation | 1u;
            break;
        }
    MOQ_TEST_CHECK(want_slot >= 0);
    if (want_slot < 0) return moq_stream_ref_from_u64(0);
    g_r1_want_slot = want_slot;
    uint64_t want_handle = moq_handle_pack(MOQ_HANDLE_POOL_PUBLICATION,
                                           s->session_tag, want_gen,
                                           (uint32_t)want_slot);

    moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
    MOQ_TEST_CHECK_EQ_INT((int)feed_publish(s, ref, 0, R1_ALIAS, false),
                          (int)MOQ_OK);

    moq_publication_t pub = MOQ_PUBLICATION_INVALID;
    {
        int req = 0, other = 0;
        moq_event_t e;
        while (moq_session_poll_events(s, &e, 1) > 0) {
            if (e.kind == MOQ_EVENT_PUBLISH_REQUEST) {
                req++;
                pub = e.u.publish_request.pub;
                /* Against the handle derived before ingress, not the reverse. */
                MOQ_TEST_CHECK_EQ_U64(pub._opaque, want_handle);
            } else other++;
            moq_event_cleanup(&e);
        }
        MOQ_TEST_CHECK_EQ_INT(req, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
    }
    g_r1_handle = want_handle;

    moq_accept_publish_cfg_t ac;
    moq_accept_publish_cfg_init(&ac);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_publish(s, pub, &ac, 1),
                          (int)MOQ_OK);
    {
        int oks = 0, other = 0;
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) {
            if (a.kind == MOQ_ACTION_SEND_BIDI_STREAM &&
                a.u.send_bidi_stream.stream_ref._v == ref._v) oks++;
            else other++;
            moq_action_cleanup(&a);
        }
        MOQ_TEST_CHECK_EQ_INT(oks, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
    }

    /* A PUBLISH_DONE whose Stream Count this subscriber has not satisfied:
     * the terminal defers behind the stamped deadline instead of firing now. */
    {
        uint8_t msg[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_encode_publish_done(
                &w, R1_STATUS, R1_STREAMS,
                (moq_bytes_t){ (const uint8_t *)k_r1_reason,
                               sizeof(k_r1_reason) - 1 }), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, msg,
                                                  moq_buf_writer_offset(&w),
                                                  fin_observed, 1), (int)MOQ_OK);
    }
    /* The deferral itself owes no event; a close-half action may be queued. */
    {
        int events = 0;
        moq_event_t e;
        while (moq_session_poll_events(s, &e, 1) > 0) {
            events++;
            moq_event_cleanup(&e);
        }
        MOQ_TEST_CHECK_EQ_INT(events, 0);
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
    }

    {
        const moq_pub_entry_t *e = &s->publishes[want_slot];
        MOQ_TEST_CHECK_EQ_INT(pub_busy_count(s), 1);
        MOQ_TEST_CHECK_EQ_INT((int)e->state, (int)MOQ_PUB_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT((int)e->role, (int)MOQ_PUB_ROLE_SUBSCRIBER);
        MOQ_TEST_CHECK_EQ_U64(e->handle._opaque, want_handle);
        MOQ_TEST_CHECK_EQ_U64(e->generation, want_gen);
        MOQ_TEST_CHECK_EQ_U64(e->rs.ref._v, ref._v);
        MOQ_TEST_CHECK_EQ_INT(e->done_pending ? 1 : 0, 1);
        MOQ_TEST_CHECK_EQ_INT(e->done_expired ? 1 : 0, 0);
        failures += txs_check_span_bytes(e->done_reason_buf,
                                         e->done_reason_len, k_r1_reason,
                                         sizeof(k_r1_reason) - 1,
                                         "route1 done reason");
        MOQ_TEST_CHECK_EQ_U64(e->done_status_code, R1_STATUS);
        MOQ_TEST_CHECK_EQ_U64(e->done_stream_count, R1_STREAMS);
        MOQ_TEST_CHECK_EQ_INT(e->rs.recv_fin ? 1 : 0, fin_observed ? 1 : 0);
        /* The deadline is stamped and still in the future, so the sweep the
         * terminal runs on is genuinely deferred rather than already due. */
        MOQ_TEST_CHECK(e->done_deadline_us > 1);
        MOQ_TEST_CHECK(e->done_deadline_us < R1_DEADLINE_US);
        check_registered(s, ref, MOQ_REQ_PUBLISH, want_slot);
    }
    if (out_handle) *out_handle = want_handle;
    return ref;
}


/* The public deferred sweep, never the internal finalize. */
moq_result_t fire_deferred_sweep(moq_session_t *s, uint64_t handle)
{
    (void)handle;
    return moq_session_tick(s, R1_DEADLINE_US);
}


void want_deferred_finalize(txs_norm_vec_t *v, moq_stream_ref_t ref,
                            uint64_t handle)
{
    (void)ref;
    txs_img_t im;
    txs_img_init(&im);
    txs_img_u64(&im, handle);
    txs_img_u64(&im, R1_STATUS);
    txs_img_u64(&im, R1_STREAMS);
    txs_img_bytes(&im, (const uint8_t *)k_r1_reason, sizeof(k_r1_reason) - 1);
    MOQ_TEST_CHECK(txs_norm_append_img(v, MOQ_EVENT_PUBLISH_FINISHED, &im));
}


txs_op_hooks_t r1_hooks = {
    NULL, NULL, NULL, NULL, pub_norm_event, pub_norm_action
};
