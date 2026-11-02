#include "test_cases.h"

/* -- Anti-vacuity self-checks ---------------------------------------- */

/* Every field the normalized images carry must actually discriminate, and the
 * proof has to run through the PRODUCTION normalizers: comparing two
 * hand-built expected images would only show that txs_norm_equals() works,
 * and a normalizer hardcoding a field would survive it. So each pair below is
 * two RAW records differing in exactly one field, normalized by
 * pub_norm_event / pub_norm_action, which must then compare unequal. */

static moq_event_t mk_finish_event(uint64_t handle, uint64_t status,
                                   uint64_t streams, const char *reason)
{
    moq_event_t ev;
    memset(&ev, 0, sizeof(ev));
    ev.kind = MOQ_EVENT_PUBLISH_FINISHED;
    ev.u.publish_finished.pub._opaque = handle;
    ev.u.publish_finished.status_code = status;
    ev.u.publish_finished.stream_count = streams;
    if (reason) {
        ev.u.publish_finished.reason.data = (const uint8_t *)reason;
        ev.u.publish_finished.reason.len = strlen(reason);
    }
    return ev;
}


static moq_event_t mk_request_event(uint64_t handle)
{
    moq_event_t ev;
    memset(&ev, 0, sizeof(ev));
    ev.kind = MOQ_EVENT_PUBLISH_REQUEST;
    ev.u.publish_request.pub._opaque = handle;
    return ev;
}


/* Normalize two raw event lists through the production normalizer and require
 * the results to differ. */
static void expect_events_differ(const moq_event_t *a, size_t na,
                                 const moq_event_t *b, size_t nb,
                                 const char *what)
{
    txs_norm_vec_t va, vb;
    txs_norm_init(&va); txs_norm_init(&vb);
    for (size_t i = 0; i < na; i++)
        MOQ_TEST_CHECK(pub_norm_event(&a[i], &pub_ctx, &va));
    for (size_t i = 0; i < nb; i++)
        MOQ_TEST_CHECK(pub_norm_event(&b[i], &pub_ctx, &vb));
    txs_quiet = 1;
    int diff = txs_norm_equals(&va, &vb, what);
    txs_quiet = 0;
    if (diff == 0)
        fprintf(stderr, "FAIL: %s:%d: normalizer ignores %s\n",
                __FILE__, __LINE__, what);
    MOQ_TEST_CHECK(diff != 0);
    txs_norm_free(&va); txs_norm_free(&vb);
}


static moq_action_t mk_bidi_action(moq_stream_ref_t ref, bool fin,
                                   const uint8_t *data, size_t len)
{
    moq_action_t a;
    memset(&a, 0, sizeof(a));
    a.kind = MOQ_ACTION_SEND_BIDI_STREAM;
    a.u.send_bidi_stream.stream_ref = ref;
    a.u.send_bidi_stream.fin = fin;
    a.u.send_bidi_stream.data = data;
    a.u.send_bidi_stream.len = len;
    return a;
}


static moq_action_t mk_reset_data_action(moq_stream_ref_t ref, uint64_t code)
{
    moq_action_t a;
    memset(&a, 0, sizeof(a));
    a.kind = MOQ_ACTION_RESET_DATA;
    a.u.reset_data.stream_ref = ref;
    a.u.reset_data.error_code = code;
    return a;
}


static void expect_actions_differ(const moq_action_t *a, const moq_action_t *b,
                                  const char *what)
{
    txs_norm_vec_t va, vb;
    txs_norm_init(&va); txs_norm_init(&vb);
    MOQ_TEST_CHECK(pub_norm_action(a, &pub_ctx, &va));
    MOQ_TEST_CHECK(pub_norm_action(b, &pub_ctx, &vb));
    txs_quiet = 1;
    int diff = txs_norm_equals(&va, &vb, what);
    txs_quiet = 0;
    if (diff == 0)
        fprintf(stderr, "FAIL: %s:%d: normalizer ignores %s\n",
                __FILE__, __LINE__, what);
    MOQ_TEST_CHECK(diff != 0);
    txs_norm_free(&va); txs_norm_free(&vb);
}


void check_image_fields(void)
{
    const uint64_t H = 0x1234;

    /* Event order: the SAME two raw records, normalized in opposite order. */
    {
        moq_event_t fwd[2] = { mk_request_event(H),
                               mk_finish_event(H, 0, 0, NULL) };
        moq_event_t rev[2] = { mk_finish_event(H, 0, 0, NULL),
                               mk_request_event(H) };
        expect_events_differ(fwd, 2, rev, 2, "event order");
    }
    /* One event field at a time. */
    {
        moq_event_t a = mk_finish_event(H, 0, 0, NULL);
        moq_event_t b = mk_finish_event(H + 1, 0, 0, NULL);
        expect_events_differ(&a, 1, &b, 1, "finish handle");
    }
    {
        moq_event_t a = mk_finish_event(H, 0, 0, NULL);
        moq_event_t b = mk_finish_event(H, 7, 0, NULL);
        expect_events_differ(&a, 1, &b, 1, "finish status");
    }
    {
        moq_event_t a = mk_finish_event(H, 0, 0, NULL);
        moq_event_t b = mk_finish_event(H, 0, 3, NULL);
        expect_events_differ(&a, 1, &b, 1, "finish stream count");
    }
    {
        moq_event_t a = mk_finish_event(H, 0, 0, NULL);
        moq_event_t b = mk_finish_event(H, 0, 0, "gone");
        expect_events_differ(&a, 1, &b, 1, "finish reason");
    }
    {
        moq_event_t a = mk_request_event(H);
        moq_event_t b = mk_request_event(H + 1);
        expect_events_differ(&a, 1, &b, 1, "request handle");
    }

    /* One action field at a time. */
    uint8_t base[160], other_code[160], with_reason[160];
    size_t base_len = enc_request_error(base, sizeof(base),
                                        MOQ_REQUEST_ERROR_NOT_SUPPORTED, 0,
                                        NULL);
    size_t other_len = enc_request_error(other_code, sizeof(other_code),
                                         MOQ_REQUEST_ERROR_UNAUTHORIZED, 0,
                                         NULL);
    size_t reason_len = enc_request_error(with_reason, sizeof(with_reason),
                                          MOQ_REQUEST_ERROR_NOT_SUPPORTED, 0,
                                          "no");
    moq_stream_ref_t r1 = moq_stream_ref_from_u64(1);
    moq_stream_ref_t r2 = moq_stream_ref_from_u64(2);
    {
        moq_action_t a = mk_bidi_action(r1, true, base, base_len);
        moq_action_t b = mk_bidi_action(r2, true, base, base_len);
        expect_actions_differ(&a, &b, "action stream ref");
    }
    {
        moq_action_t a = mk_bidi_action(r1, true, base, base_len);
        moq_action_t b = mk_bidi_action(r1, false, base, base_len);
        expect_actions_differ(&a, &b, "action FIN");
    }
    {
        /* Same message type, different payload bytes. */
        moq_action_t a = mk_bidi_action(r1, true, base, base_len);
        moq_action_t b = mk_bidi_action(r1, true, other_code, other_len);
        expect_actions_differ(&a, &b, "action payload");
    }
    {
        moq_action_t a = mk_bidi_action(r1, true, base, base_len);
        moq_action_t b = mk_bidi_action(r1, true, with_reason, reason_len);
        expect_actions_differ(&a, &b, "action payload length");
    }
    {
        /* Message type ALONE: two envelopes whose payload bytes and length are
         * identical, differing only in the type field. Comparing a REQUEST_OK
         * against a REQUEST_ERROR would not isolate it -- their payloads
         * differ too, so an ignored type would still show unequal images. */
        uint8_t t1[64], t2[64];
        static const uint8_t body[] = { 0x01, 0x02, 0x03, 0x04 };
        size_t n1 = enc_typed_envelope(t1, sizeof(t1), MOQ_D18_REQUEST_ERROR,
                                       body, sizeof(body));
        size_t n2 = enc_typed_envelope(t2, sizeof(t2), MOQ_D18_REQUEST_OK,
                                       body, sizeof(body));
        moq_action_t a = mk_bidi_action(r1, true, t1, n1);
        moq_action_t b = mk_bidi_action(r1, true, t2, n2);
        expect_actions_differ(&a, &b, "action message type");
    }
    /* The data-cancellation record's own two fields, one at a time. */
    {
        moq_action_t a = mk_reset_data_action(r1, 0x1);
        moq_action_t b = mk_reset_data_action(r2, 0x1);
        expect_actions_differ(&a, &b, "reset stream ref");
    }
    {
        moq_action_t a = mk_reset_data_action(r1, 0x1);
        moq_action_t b = mk_reset_data_action(r1, 0x2);
        expect_actions_differ(&a, &b, "reset error code");
    }
    {
        /* Kind ALONE. The two action shapes cannot be made to produce the same
         * image through their builders -- their field lists differ -- so the
         * record kind is isolated at the normalization layer instead: one
         * identical image appended under two kinds must not compare equal. */
        txs_img_t im;
        txs_img_init(&im);
        txs_img_u64(&im, r1._v);
        txs_img_u64(&im, 0x1);
        txs_norm_vec_t va, vb;
        txs_norm_init(&va); txs_norm_init(&vb);
        MOQ_TEST_CHECK(txs_norm_append_img(&va, MOQ_ACTION_RESET_DATA, &im));
        MOQ_TEST_CHECK(txs_norm_append_img(&vb, MOQ_ACTION_STOP_DATA, &im));
        txs_quiet = 1;
        MOQ_TEST_CHECK(txs_norm_equals(&va, &vb, "action kind") != 0);
        txs_quiet = 0;
        txs_norm_free(&va); txs_norm_free(&vb);
    }

    /* Trailing bytes after the envelope are refused outright. */
    {
        uint8_t buf[160];
        size_t n = enc_request_error(buf, sizeof(buf) - 1,
                                     MOQ_REQUEST_ERROR_NOT_SUPPORTED, 0, NULL);
        buf[n] = 0x00;
        moq_action_t a = mk_bidi_action(r1, true, buf, n + 1);
        txs_norm_vec_t v;
        txs_norm_init(&v);
        txs_quiet = 1;
        MOQ_TEST_CHECK(!pub_norm_action(&a, &pub_ctx, &v));
        txs_quiet = 0;
        MOQ_TEST_CHECK_EQ_SIZE(v.count, 0);
        txs_norm_free(&v);
    }
}


/* The declared owner kind, role and state are compared, not merely stored:
 * perturbing any one of them must fail the comparison. */
void check_owner_metadata(void)
{
    fin_owner_case_t f = pub_family();
    pub_ctx_t *c = (pub_ctx_t *)f.ctx;
    moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
    MOQ_TEST_CHECK_EQ_INT((int)f.feed(s, f.ctx, c->ref, false), (int)MOQ_OK);
    txs_norm_vec_t got;
    txs_norm_init(&got);
    collect_events(s, &f.hooks, &got);
    txs_norm_free(&got);

    MOQ_TEST_CHECK_EQ_INT(
        pub_owner_present_problems(s, c->ref, f.owner_kind, f.owner_role,
                                   f.owner_state), 0);
    txs_quiet = 1;
    MOQ_TEST_CHECK(pub_owner_present_problems(s, c->ref, MOQ_REQ_FETCH,
                                              f.owner_role, f.owner_state) > 0);
    MOQ_TEST_CHECK(pub_owner_present_problems(s, c->ref, f.owner_kind,
                                              (int)MOQ_PUB_ROLE_PUBLISHER,
                                              f.owner_state) > 0);
    MOQ_TEST_CHECK(pub_owner_present_problems(s, c->ref, f.owner_kind,
                                              f.owner_role,
                                              (int)MOQ_PUB_ESTABLISHED) > 0);
    txs_quiet = 0;
    moq_session_destroy(s);
}


/* -- Harness self-checks --------------------------------------------- */

void test_harness(void)
{
    /* A complete pre-commit descriptor validates; a missing required member,
     * and each incomplete seed bundle, are rejected. */
    {
        precommit_case_t pc = {
            .name = "probe", .make = make_session_default,
            .encode = enc_publish_unknown_alias,
            .check_owners = owners_publish_none
        };
        seed_ops_t partial;
        MOQ_TEST_CHECK_EQ_INT(precommit_problems(&pc), 0);
        pc.seed_ops = &pub_seed_ops;
        MOQ_TEST_CHECK_EQ_INT(precommit_problems(&pc), 0);
        partial = pub_seed_ops; partial.seed = NULL;
        pc.seed_ops = &partial;
        MOQ_TEST_CHECK(precommit_problems(&pc) > 0);
        partial = pub_seed_ops; partial.capture = NULL;
        MOQ_TEST_CHECK(precommit_problems(&pc) > 0);
        partial = pub_seed_ops; partial.check = NULL;
        MOQ_TEST_CHECK(precommit_problems(&pc) > 0);
        pc.seed_ops = &pub_seed_ops;
        pc.check_owners = NULL;
        MOQ_TEST_CHECK(precommit_problems(&pc) > 0);
    }

    /* Each registry field the owner comparator carries is compared on its own.
     * `id_request_id` needs saying explicitly: this fixture's first request id
     * is 0 and a deleted key also reports 0, so a live perturbation can never
     * distinguish a hardcoded-zero capture from a real one. */
    {
        req_seed_snap_t a, b;
        int base, delta, quiet = txs_quiet;
        memset(&a, 0, sizeof(a));
        b = a;
        txs_quiet = 1;
        base = failures;
        req_seed_diff(&a, &b, "registry probe");      /* identical */
        delta = failures - base;
        failures = base;
        txs_quiet = quiet;
        MOQ_TEST_CHECK_EQ_INT(delta, 0);

        b = a;
        b.id_request_id = 7;                          /* differs ONLY here */
        txs_quiet = 1;
        base = failures;
        req_seed_diff(&a, &b, "registry probe");
        delta = failures - base;
        failures = base;
        txs_quiet = quiet;
        MOQ_TEST_CHECK_EQ_INT(delta, 1);

        b = a;
        b.id_has_request_id = 1;
        txs_quiet = 1;
        base = failures;
        req_seed_diff(&a, &b, "registry probe");
        delta = failures - base;
        failures = base;
        txs_quiet = quiet;
        MOQ_TEST_CHECK_EQ_INT(delta, 1);
    }

    /* And the CAPTURE of that field is exercised against a NONZERO request id,
     * which no live row can supply: every fixture's first request is id 0, so a
     * hardcoded-zero capture would agree with the registry everywhere. */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
        if (s) {
            moq_bytes_t p0[] = { MOQ_BYTES_LITERAL("a") };
            moq_bytes_t p1[] = { MOQ_BYTES_LITERAL("b") };
            moq_subscribe_namespace_cfg_t nc;
            moq_ns_sub_handle_t h0, h1;
            memset(&h0, 0, sizeof(h0));
            memset(&h1, 0, sizeof(h1));
            moq_subscribe_namespace_cfg_init(&nc);
            nc.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
            nc.track_namespace_prefix = (moq_namespace_t){ p0, 1 };
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_session_subscribe_namespace(s, &nc, 1, &h0), (int)MOQ_OK);
            nc.track_namespace_prefix = (moq_namespace_t){ p1, 1 };
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_session_subscribe_namespace(s, &nc, 1, &h1), (int)MOQ_OK);
            moq_action_t a;
            while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);

            /* The second request's id is nonzero, and the capture must report
             * exactly it. */
            uint64_t rid = 0;
            for (size_t i = 0; i < s->ns_sub_cap; i++)
                if (s->ns_subs[i].state != MOQ_NS_SUB_FREE &&
                    s->ns_subs[i].handle._opaque == h1._opaque)
                    rid = s->ns_subs[i].request_id;
            MOQ_TEST_CHECK(rid != 0);
            req_seed_snap_t r;
            memset(&r, 0, sizeof(r));
            req_seed_capture_registry(&r, s, moq_stream_ref_from_u64(0), rid);
            MOQ_TEST_CHECK_EQ_INT(r.id_kind, (int)MOQ_REQ_NAMESPACE_SUB);
            MOQ_TEST_CHECK_EQ_INT(r.id_has_request_id, 1);
            MOQ_TEST_CHECK_EQ_U64(r.id_request_id, rid);
            moq_session_destroy(s);
        }
    }

    /* The checked append refuses exactly at the buffer boundary, and refuses
     * an invalid or impossible base without touching it. */
    {
        req_seed_snap_t r;
        static const uint8_t tail[2] = { 0xa1, 0xa2 };
        memset(&r, 0, sizeof(r));
        r.recv_len = REQ_SEED_BUF_MAX - 2;
        req_seed_append(&r, tail, 2);                 /* exactly fits */
        MOQ_TEST_CHECK_EQ_INT(r.buf_invalid, 0);
        MOQ_TEST_CHECK_EQ_SIZE(r.recv_len, (size_t)REQ_SEED_BUF_MAX);
        MOQ_TEST_CHECK(memcmp(r.buf + REQ_SEED_BUF_MAX - 2, tail, 2) == 0);

        memset(&r, 0, sizeof(r));
        r.recv_len = REQ_SEED_BUF_MAX - 1;
        req_seed_append(&r, tail, 2);                 /* one byte too many */
        MOQ_TEST_CHECK_EQ_INT(r.buf_invalid, 1);
        MOQ_TEST_CHECK_EQ_SIZE(r.recv_len, (size_t)(REQ_SEED_BUF_MAX - 1));

        memset(&r, 0, sizeof(r));
        r.recv_len = REQ_SEED_BUF_MAX;
        req_seed_append(&r, tail, 0);                 /* full, empty tail */
        MOQ_TEST_CHECK_EQ_INT(r.buf_invalid, 0);
        MOQ_TEST_CHECK_EQ_SIZE(r.recv_len, (size_t)REQ_SEED_BUF_MAX);

        memset(&r, 0, sizeof(r));
        r.recv_len = (size_t)REQ_SEED_BUF_MAX + 64;   /* impossible capture */
        req_seed_append(&r, tail, 2);
        MOQ_TEST_CHECK_EQ_INT(r.buf_invalid, 1);
        MOQ_TEST_CHECK_EQ_SIZE(r.recv_len, (size_t)REQ_SEED_BUF_MAX + 64);

        /* The case that distinguishes a subtraction-only bound from a sum: a
         * length this large makes base + tail wrap, so a sum-based guard would
         * admit the append and copy at an impossible offset. */
        memset(&r, 0, sizeof(r));
        r.recv_len = (size_t)-1;
        req_seed_append(&r, tail, 2);
        MOQ_TEST_CHECK_EQ_INT(r.buf_invalid, 1);
        MOQ_TEST_CHECK_EQ_SIZE(r.recv_len, (size_t)-1);

        memset(&r, 0, sizeof(r));
        r.buf_invalid = 1;
        req_seed_append(&r, tail, 2);                 /* invalid base */
        MOQ_TEST_CHECK_EQ_INT(r.buf_invalid, 1);
        MOQ_TEST_CHECK_EQ_SIZE(r.recv_len, (size_t)0);
    }

    /* The same for an Axis 3 terminal descriptor: a complete owner bundle and a
     * declared drain reason are both required. */
    {
        terminal_case_t tc = {
            .name = "probe", .make = make_session_default,
            .arm = arm_established_announcement, .encode = enc_update_next,
            .drain_reason = MOQ_DRAIN_NORMAL,
            .want_output = want_update_rejected,
            .hooks = &pub_hooks_for_actions, .owner = &ann_owner_ops
        };
        owner_ops_t partial;
        MOQ_TEST_CHECK_EQ_INT(terminal_problems(&tc), 0);
        partial = ann_owner_ops; partial.capture = NULL;
        tc.owner = &partial;
        MOQ_TEST_CHECK(terminal_problems(&tc) > 0);
        partial = ann_owner_ops; partial.check = NULL;
        MOQ_TEST_CHECK(terminal_problems(&tc) > 0);
        partial = ann_owner_ops; partial.check_retired = NULL;
        MOQ_TEST_CHECK(terminal_problems(&tc) > 0);
        tc.owner = &ann_owner_ops;
        MOQ_TEST_CHECK_EQ_INT(terminal_problems(&tc), 0);
        tc.drain_reason = (uint8_t)0x7f;      /* no such reason */
        MOQ_TEST_CHECK(terminal_problems(&tc) > 0);
        tc.drain_reason = MOQ_DRAIN_NORMAL;
        tc.arm = NULL;
        MOQ_TEST_CHECK(terminal_problems(&tc) > 0);
        tc.arm = arm_established_announcement;

        /* The graph capability is all-or-nothing, and its KIND is part of the
         * contract: MOQ_REQ_NONE is exactly what a deleted initializer leaves
         * behind, and it would make the owner-target retirement scan pass
         * vacuously. */
        {
            static int probe_slot = 0;
            terminal_graph_ops_t g = {
                .owner_edges = r3_owner_edges,
                .owner_kind  = MOQ_REQ_ANNOUNCEMENT,
                .owner_slot  = &probe_slot
            };
            tc.graph = NULL;                       /* absent: legal */
            MOQ_TEST_CHECK_EQ_INT(terminal_problems(&tc), 0);
            tc.graph = &g;                         /* complete: legal */
            MOQ_TEST_CHECK_EQ_INT(terminal_problems(&tc), 0);
            g.owner_kind = MOQ_REQ_NONE;           /* the deleted-field value */
            MOQ_TEST_CHECK(terminal_problems(&tc) > 0);
            g.owner_kind = 0x7f;                   /* no such family */
            MOQ_TEST_CHECK(terminal_problems(&tc) > 0);
            g.owner_kind = MOQ_REQ_ANNOUNCEMENT;
            g.owner_edges = NULL;
            MOQ_TEST_CHECK(terminal_problems(&tc) > 0);
            g.owner_edges = r3_owner_edges;
            g.owner_slot = NULL;
            MOQ_TEST_CHECK(terminal_problems(&tc) > 0);
            g.owner_slot = &probe_slot;
            MOQ_TEST_CHECK_EQ_INT(terminal_problems(&tc), 0);
            tc.graph = NULL;
        }

        /* And the kind is LOAD-BEARING, not decoration: a live owner audited
         * under its own family reports its edges, while the SAME scan under any
         * other family finds nothing and would pass vacuously. That is the
         * hazard the validation above exists to prevent. */
        {
            moq_session_t *gs = make_session_client();
            uint64_t h = 0;
            moq_stream_ref_t gref = arm_announcer_established(gs, false, &h);
            if (gref._v != 0) {
                og_graph_t g;
                og_capture(gs, &g);
                MOQ_TEST_CHECK(og_check_integrity(&g, "kind probe") == 0);
                og_quiet++;
                /* its own family sees the live owner's edges */
                int own = og_check_owner_edges(&g, MOQ_REQ_ANNOUNCEMENT,
                                               g_ann_want_slot, NULL, 0,
                                               "kind probe");
                /* every other family is blind to it */
                int other = og_check_owner_edges(&g, MOQ_REQ_FETCH,
                                                 g_ann_want_slot, NULL, 0,
                                                 "kind probe");
                og_quiet--;
                MOQ_TEST_CHECK(own > 0);
                MOQ_TEST_CHECK_EQ_INT(other, 0);
            }
            moq_session_destroy(gs);
        }
        MOQ_TEST_CHECK_EQ_INT(terminal_problems(&tc), 0);

        /* The extra-state bundle is optional but INDIVISIBLE: absent is fine,
         * any one member missing is a descriptor error -- a capture with no
         * comparison, or a comparison in one direction only, proves nothing. */
        extra_ops_t xp;
        tc.extra = NULL;
        MOQ_TEST_CHECK_EQ_INT(terminal_problems(&tc), 0);
        tc.extra = &fu_extra_ops;
        MOQ_TEST_CHECK_EQ_INT(terminal_problems(&tc), 0);
        tc.extra = &xp;
        xp = fu_extra_ops; xp.capture = NULL;
        MOQ_TEST_CHECK(terminal_problems(&tc) > 0);
        xp = fu_extra_ops; xp.check_blocked = NULL;
        MOQ_TEST_CHECK(terminal_problems(&tc) > 0);
        xp = fu_extra_ops; xp.check_committed = NULL;
        MOQ_TEST_CHECK(terminal_problems(&tc) > 0);
        tc.extra = NULL;

        /* The namespace-owner snapshot refuses malformed state BEFORE it
         * dereferences anything: under a sanitizer each of these would abort
         * the run if the guard were removed. */
        {
            moq_session_t *ms = make_session_default();
            MOQ_TEST_CHECK(ms != NULL);
            if (ms && ms->ns_sub_cap > 0) {
                moq_ns_sub_entry_t *e = &ms->ns_subs[0];
                moq_bytes_t bad_part = { NULL, 4 };
                nso_snap_t v;
                e->state = MOQ_NS_SUB_RECVING_PUBLISHER;
                e->prefix_valid = true;

                e->prefix_count = NSO_MAX_PARTS + 1;
                e->prefix_parts = &bad_part;
                nso_read(ms, 0, &v);
                MOQ_TEST_CHECK_EQ_INT(v.invalid, 1);

                e->prefix_count = 2;
                e->prefix_parts = NULL;
                nso_read(ms, 0, &v);
                MOQ_TEST_CHECK_EQ_INT(v.invalid, 1);

                e->prefix_count = 1;
                e->prefix_parts = &bad_part;      /* len 4, data NULL */
                nso_read(ms, 0, &v);
                MOQ_TEST_CHECK_EQ_INT(v.invalid, 1);

                e->prefix_count = 0;
                e->prefix_parts = NULL;
                e->prefix_valid = false;
                {
                    size_t saved_len = e->recv_len;
                    uint8_t *saved_buf = e->recv_buf;
                    e->recv_len = NSO_BUF_MAX + 1;
                    nso_read(ms, 0, &v);
                    MOQ_TEST_CHECK_EQ_INT(v.invalid, 1);
                    /* A retained length with no buffer is refused too. */
                    e->recv_len = 4;
                    e->recv_buf = NULL;
                    nso_read(ms, 0, &v);
                    MOQ_TEST_CHECK_EQ_INT(v.invalid, 1);
                    e->recv_buf = saved_buf;
                    e->recv_len = saved_len;
                }
                /* An out-of-range slot is refused without touching the pool. */
                nso_read(ms, (int)ms->ns_sub_cap + 3, &v);
                MOQ_TEST_CHECK_EQ_INT(v.invalid, 1);
                /* Two incomparable records never compare equal. */
                {
                    nso_snap_t a = v, b = v;
                    txs_quiet = 1;
                    MOQ_TEST_CHECK(nso_diff(&a, &b, "nso invalid") > 0);
                    txs_quiet = 0;
                }
                /* Every field this block overwrote goes back, so destruction
                 * frees the entry the session actually owns. */
                e->prefix_count = 0;
                e->prefix_parts = NULL;
                e->prefix_valid = false;
                e->state = MOQ_NS_SUB_FREE;
            }
            if (ms) moq_session_destroy(ms);
        }

        /* The durable-terminal descriptor: complete validates, and each
         * required member -- top-level callback, normalizer or owner hook --
         * is rejected on its own. */
        {
            durable_case_t dc = {
                .name = "probe", .make = make_session_client,
                .arm = arm_announcer_established,
                .fire = fire_publish_namespace_done,
                .want_output = want_announcer_teardown,
                .hooks = &durable_abort_hooks, .owner = &ann_owner_ops,
                .owner_edges = ann_owner_edges
            };
            txs_op_hooks_t dhk;
            owner_ops_t dow;
            MOQ_TEST_CHECK_EQ_INT(durable_problems(&dc), 0);
            dc.name = NULL; MOQ_TEST_CHECK(durable_problems(&dc) > 0);
            dc.name = "probe";
            dc.make = NULL; MOQ_TEST_CHECK(durable_problems(&dc) > 0);
            dc.make = make_session_client;
            dc.arm = NULL;  MOQ_TEST_CHECK(durable_problems(&dc) > 0);
            dc.arm = arm_announcer_established;
            dc.fire = NULL; MOQ_TEST_CHECK(durable_problems(&dc) > 0);
            dc.fire = fire_publish_namespace_done;
            dc.want_output = NULL; MOQ_TEST_CHECK(durable_problems(&dc) > 0);
            dc.want_output = want_announcer_teardown;
            dc.owner_edges = NULL; MOQ_TEST_CHECK(durable_problems(&dc) > 0);
            dc.owner_edges = ann_owner_edges;
            MOQ_TEST_CHECK_EQ_INT(durable_problems(&dc), 0);
            dc.hooks = NULL; MOQ_TEST_CHECK(durable_problems(&dc) > 0);
            dhk = durable_abort_hooks; dhk.normalize_event = NULL;
            dc.hooks = &dhk; MOQ_TEST_CHECK(durable_problems(&dc) > 0);
            dhk = durable_abort_hooks; dhk.normalize_action = NULL;
            MOQ_TEST_CHECK(durable_problems(&dc) > 0);
            dc.hooks = &durable_abort_hooks;
            dc.owner = NULL; MOQ_TEST_CHECK(durable_problems(&dc) > 0);
            dow = ann_owner_ops; dow.capture = NULL;
            dc.owner = &dow; MOQ_TEST_CHECK(durable_problems(&dc) > 0);
            dow = ann_owner_ops; dow.check = NULL;
            MOQ_TEST_CHECK(durable_problems(&dc) > 0);
            dow = ann_owner_ops; dow.check_retired = NULL;
            MOQ_TEST_CHECK(durable_problems(&dc) > 0);
            dc.owner = &ann_owner_ops;
            MOQ_TEST_CHECK_EQ_INT(durable_problems(&dc), 0);
        }

        /* The reject descriptor: complete validates, each required member
         * missing is rejected on its own. */
        {
            nsr_case_t nc = {
                .name = "probe", .arm = nsr_arm_auth, .encode = enc_sns_auth,
                .err_code = 0x17,
                .declare_transient = nsr_auth_declare
            };
            MOQ_TEST_CHECK_EQ_INT(nsr_problems(&nc), 0);
            nc.name = NULL;  MOQ_TEST_CHECK(nsr_problems(&nc) > 0);
            nc.name = "probe";
            nc.arm = NULL;   MOQ_TEST_CHECK(nsr_problems(&nc) > 0);
            nc.arm = nsr_arm_auth;
            nc.encode = NULL; MOQ_TEST_CHECK(nsr_problems(&nc) > 0);
            nc.encode = enc_sns_auth;
            nc.declare_transient = NULL;
            MOQ_TEST_CHECK(nsr_problems(&nc) > 0);
            nc.declare_transient = nsr_auth_declare;
            nc.err_code = 0; MOQ_TEST_CHECK(nsr_problems(&nc) > 0);
            nc.err_code = 0x17;
            MOQ_TEST_CHECK_EQ_INT(nsr_problems(&nc), 0);
        }

        /* The output hooks, at each of their three boundaries. */
        txs_op_hooks_t hk;
        tc.hooks = NULL;
        MOQ_TEST_CHECK(terminal_problems(&tc) > 0);
        hk = pub_hooks_for_actions; hk.normalize_event = NULL;
        tc.hooks = &hk;
        MOQ_TEST_CHECK(terminal_problems(&tc) > 0);
        hk = pub_hooks_for_actions; hk.normalize_action = NULL;
        MOQ_TEST_CHECK(terminal_problems(&tc) > 0);
        tc.hooks = &pub_hooks_for_actions;
        MOQ_TEST_CHECK_EQ_INT(terminal_problems(&tc), 0);
    }

    /* A well-formed descriptor validates; each capability/callback
     * disagreement is rejected. */
    fin_owner_case_t f = pub_family();
    MOQ_TEST_CHECK_EQ_INT(fin_owner_problems(&f), 0);
    {
        fin_owner_case_t bad = f;
        bad.caps |= FIN_TERM_INTERNAL_REJECT;   /* bit without its callback */
        MOQ_TEST_CHECK(fin_owner_problems(&bad) > 0);
    }
    {
        fin_owner_case_t bad = f;
        bad.caps &= ~(unsigned)FIN_TERM_APP_REJECT;  /* callback without bit */
        MOQ_TEST_CHECK(fin_owner_problems(&bad) > 0);
    }
    {
        fin_owner_case_t bad = f;
        bad.caps |= 1u << 7;                    /* unknown capability bit */
        MOQ_TEST_CHECK(fin_owner_problems(&bad) > 0);
    }
    {
        fin_owner_case_t bad = f;
        bad.feed = NULL;
        MOQ_TEST_CHECK(fin_owner_problems(&bad) > 0);
    }
    {
        fin_owner_case_t bad = f;
        bad.owner_kind = MOQ_REQ_NONE;
        MOQ_TEST_CHECK(fin_owner_problems(&bad) > 0);
    }
    /* Each required hook is individually mandatory. */
    {
        fin_owner_case_t bad = f;
        bad.hooks.normalize_event = NULL;
        MOQ_TEST_CHECK(fin_owner_problems(&bad) > 0);
    }
    {
        fin_owner_case_t bad = f;
        bad.hooks.normalize_action = NULL;
        MOQ_TEST_CHECK(fin_owner_problems(&bad) > 0);
    }
    {
        fin_owner_case_t bad = f;
        bad.hooks.capture = NULL;
        MOQ_TEST_CHECK(fin_owner_problems(&bad) > 0);
    }
    {
        fin_owner_case_t bad = f;
        bad.hooks.check = NULL;
        MOQ_TEST_CHECK(fin_owner_problems(&bad) > 0);
    }
    {
        fin_owner_case_t bad = f;
        bad.hooks.ctx = NULL;
        MOQ_TEST_CHECK(fin_owner_problems(&bad) > 0);
    }
    /* An event kind the family does not describe fails normalization. */
    {
        txs_norm_vec_t v;
        txs_norm_init(&v);
        moq_event_t ev;
        memset(&ev, 0, sizeof(ev));
        ev.kind = MOQ_EVENT_GOAWAY;
        txs_quiet = 1;
        MOQ_TEST_CHECK(!pub_norm_event(&ev, &pub_ctx, &v));
        moq_action_t a;
        memset(&a, 0, sizeof(a));
        a.kind = MOQ_ACTION_CLOSE_SESSION;
        MOQ_TEST_CHECK(!pub_norm_action(&a, &pub_ctx, &v));
        txs_quiet = 0;
        MOQ_TEST_CHECK_EQ_SIZE(v.count, 0);
        txs_norm_free(&v);
    }
    /* The other two classes validate on their own terms; an empty descriptor
     * of either is rejected rather than silently runnable. */
    {
        fin_nondest_case_t nd;
        memset(&nd, 0, sizeof(nd));
        MOQ_TEST_CHECK(fin_nondest_problems(&nd) > 0);
        fin_bridge_case_t br;
        memset(&br, 0, sizeof(br));
        MOQ_TEST_CHECK(fin_bridge_problems(&br) > 0);
    }

    /* The scratch normalization applies only to a drained event queue, so a
     * real scratch mutation behind a queued event stays detectable. */
    {
        txs_snapshot_t snap;
        memset(&snap, 0, sizeof(snap));
        snap.event_depth = 0;
        snap.event_scratch_len = 21;
        expect_after_call_prepare(&snap);
        MOQ_TEST_CHECK_EQ_SIZE(snap.event_scratch_len, 0);
    }
    {
        txs_snapshot_t snap;
        memset(&snap, 0, sizeof(snap));
        snap.event_depth = 1;
        snap.event_scratch_len = 21;
        expect_after_call_prepare(&snap);
        MOQ_TEST_CHECK_EQ_SIZE(snap.event_scratch_len, 21);
    }

    /* An out-of-range registry slot yields a loud incomparable record rather
     * than an out-of-bounds read. */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0x900);
        moq_request_endpoint_t ep;
        memset(&ep, 0, sizeof(ep));
        ep.kind = MOQ_REQ_PUBLISH;
        ep.slot = (int32_t)s->pub_cap + 3;
        ep.has_stream_ref = true;
        ep.stream_ref = ref;
        request_registry_insert_by_streamref(s, ref, ep);
        txs_quiet = 1;
        txs_owner_t o = txs_owner_capture(s, ref);
        txs_quiet = 0;
        MOQ_TEST_CHECK(o.invalid);
        MOQ_TEST_CHECK(o.kind < 0);
        /* Two identical indeclared records must never compare equal. */
        txs_quiet = 1;
        MOQ_TEST_CHECK(txs_owner_equals(&o, &o, "self") != 0);
        txs_quiet = 0;
        request_registry_remove_by_streamref(s, ref);

        /* A recognized kind whose pool is empty must still be bounds-checked:
         * a zero capacity may never wave a slot through to an indexed read. */
        moq_stream_ref_t zref = moq_stream_ref_from_u64(0x901);
        moq_request_endpoint_t zep;
        memset(&zep, 0, sizeof(zep));
        zep.kind = MOQ_REQ_SUBSCRIBE_TRACKS;
        zep.slot = 0;
        zep.has_stream_ref = true;
        zep.stream_ref = zref;
        size_t saved_cap = s->track_sub_cap;
        s->track_sub_cap = 0;
        request_registry_insert_by_streamref(s, zref, zep);
        txs_quiet = 1;
        txs_owner_t zo = txs_owner_capture(s, zref);
        txs_quiet = 0;
        MOQ_TEST_CHECK(zo.invalid);
        request_registry_remove_by_streamref(s, zref);
        s->track_sub_cap = saved_cap;

        /* The namespace-subscription index is bounds-checked on the same
         * terms as the request registry. */
        moq_stream_ref_t nref = moq_stream_ref_from_u64(0x902);
        moq_index_insert(s->idx_ns_by_ref, s->idx_ns_mask, nref._v,
                         (int)s->ns_sub_cap + 2);
        txs_quiet = 1;
        txs_owner_t no = txs_owner_capture(s, nref);
        txs_quiet = 0;
        MOQ_TEST_CHECK(no.invalid);
        moq_index_remove(s->idx_ns_by_ref, s->idx_ns_mask, nref._v);

        moq_session_destroy(s);
    }
}


/* A retired subscription does not retire its FIN-started delivery timers. */
void test_failed_update_closing_timer(void)
{
    const int setup_before = failures;
    moq_session_t *s = make_session_default();
    MOQ_TEST_CHECK(s != NULL);
    if (!s) return;
    if (failures != setup_before) goto closing_timer_out;
    moq_stream_ref_t ref = arm_failed_update_completed_subscription(s);
    bool armed = failures == setup_before && ref._v != 0 && g_fu_sg_count == FU_SUBGROUPS &&
        g_fu_sg_refs[0]._v != 0 && g_fu_sg_refs[1]._v != 0 &&
        g_fu_sg_refs[0]._v != g_fu_sg_refs[1]._v;
    MOQ_TEST_CHECK(armed);
    if (!armed) goto closing_timer_out;
    int slot = sg_find_by_stream_ref(s, g_fu_sg_refs[0]);
    MOQ_TEST_CHECK(slot >= 0 && (size_t)slot < s->sg_cap);
    if (slot < 0 || (size_t)slot >= s->sg_cap) goto closing_timer_out;
    const moq_sg_entry_t original = s->subgroups[slot];
    const moq_subgroup_handle_t subgroup = sg_make_handle(s, (size_t)slot);
    bool identity_ready = original.state == MOQ_SG_CLOSING &&
        moq_subgroup_is_valid(subgroup) && moq_subscription_is_valid(original.sub) &&
        original.pub._opaque == 0 && original.header_len > 0 &&
        original.group_id == g_fu_sg_group[0] && original.subgroup_id == 0;
    MOQ_TEST_CHECK(identity_ready);
    if (!identity_ready) goto closing_timer_out;
    fu_extra_capture(s);
    if (failures != setup_before) goto closing_timer_out;
    uint8_t msg[192];
    size_t len = enc_update_unknown_alias(msg, sizeof(msg));
    MOQ_TEST_CHECK(len > 0);
    if (len == 0 || failures != setup_before) goto closing_timer_out;
    moq_result_t rc = moq_session_on_bidi_stream_bytes(s, ref, msg, len, true, 10);
    MOQ_TEST_CHECK(rc == MOQ_OK);
    if (rc != MOQ_OK) goto closing_timer_out;
    check_sub_terminal_retired(s, ref);
    const terminal_case_t terminal = {
        .want_output = want_failed_update_output,
        .hooks = &pub_hooks_for_actions
    };
    check_terminal_output(s, &terminal, ref, "closing stream terminal");
    failures += fu_extra_check_committed(s, "closing stream terminal");
    MOQ_TEST_CHECK_EQ_U64(s->subgroup_deadline_us, 5000004);

    rc = moq_session_on_data_committed(s, g_fu_sg_refs[1], 11);
    MOQ_TEST_CHECK(rc == MOQ_OK);
    if (rc != MOQ_OK) goto closing_timer_out;
    check_no_output(s, &pub_hooks_for_actions);
    MOQ_TEST_CHECK_EQ_U64(s->subgroup_deadline_us, 5000004);
    MOQ_TEST_CHECK(moq_session_tick(s, 5000003) == MOQ_OK);
    check_no_output(s, &pub_hooks_for_actions);
    MOQ_TEST_CHECK(moq_session_tick(s, 5000004) == MOQ_OK);
    moq_event_t event = {0};
    size_t events = 0;
    for (;;) {
        size_t n = 0;
        rc = moq_session_poll_events_ex(s, &event, 1, sizeof(event), &n);
        MOQ_TEST_CHECK(rc == MOQ_OK);
        if (rc != MOQ_OK || n == 0) break;
        ++events;
        MOQ_TEST_CHECK(event.kind == MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED);
        MOQ_TEST_CHECK(event.detail_size == MOQ_OUTGOING_SUBGROUP_TERMINATED_EVENT_V0_SIZE);
        if (event.kind == MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED &&
            event.detail_size >= MOQ_OUTGOING_SUBGROUP_TERMINATED_EVENT_V0_SIZE) {
            const moq_outgoing_subgroup_terminated_event_t *t = &event.u.outgoing_subgroup_terminated;
            MOQ_TEST_CHECK(moq_subgroup_eq(t->subgroup, subgroup));
            MOQ_TEST_CHECK(moq_subscription_eq(t->sub, original.sub));
            MOQ_TEST_CHECK(t->pub._opaque == 0);
            MOQ_TEST_CHECK_EQ_U64(t->stream_ref._v, g_fu_sg_refs[0]._v);
            MOQ_TEST_CHECK_EQ_U64(t->group_id, original.group_id);
            MOQ_TEST_CHECK_EQ_U64(t->subgroup_id, original.subgroup_id);
            MOQ_TEST_CHECK(t->cause == MOQ_OUTGOING_SUBGROUP_TERMINAL_DELIVERY_TIMEOUT);
            MOQ_TEST_CHECK_EQ_U64(t->error_code, MOQ_RESET_DELIVERY_TIMEOUT);
        }
        moq_event_cleanup(&event);
    }
    MOQ_TEST_CHECK_EQ_SIZE(events, 1);
    moq_action_t action = {0};
    size_t actions = 0;
    for (;;) {
        size_t n = 0;
        rc = moq_session_poll_actions_ex(s, &action, 1, sizeof(action), &n);
        MOQ_TEST_CHECK(rc == MOQ_OK);
        if (rc != MOQ_OK || n == 0) break;
        ++actions;
        MOQ_TEST_CHECK(action.kind == MOQ_ACTION_RESET_DATA);
        if (action.kind == MOQ_ACTION_RESET_DATA) {
            MOQ_TEST_CHECK_EQ_U64(action.u.reset_data.stream_ref._v, g_fu_sg_refs[0]._v);
            MOQ_TEST_CHECK_EQ_U64(action.u.reset_data.error_code, 0x2);
            MOQ_TEST_CHECK_EQ_U64(action.u.reset_data.reliable_size, original.header_len);
        }
        moq_action_cleanup(&action);
    }
    MOQ_TEST_CHECK_EQ_SIZE(actions, 1);
    MOQ_TEST_CHECK_EQ_U64(s->subgroup_deadline_us, UINT64_MAX);
    MOQ_TEST_CHECK(moq_session_tick(s, 5000005) == MOQ_OK);
    check_no_output(s, &pub_hooks_for_actions);
    check_sub_terminal_retired(s, ref);
closing_timer_out:
    moq_session_destroy(s);
}
