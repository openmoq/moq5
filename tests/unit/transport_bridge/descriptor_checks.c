#include "test_cases.h"

/*
 * The EXACT bridge state for a class at a phase -- every flag constrained, not
 * just the interesting ones, so an unrelated flag turning on is caught.
 *
 * A retained ingress returns at transport_bridge.c:2662, BEFORE
 * peer_send_closed is set at :2677: the obligation lives in the retry flags
 * instead, which is why the two classes need different expectations here.
 */
static int fin_br_check_bridge(moq_transport_bridge_t *b, uint64_t transport_id,
                               moq_stream_ref_t want_ref,
                               fin_bridge_ingress_t ingress,
                               fin_br_phase_t phase, const char *what)
{
    int bad = 0;
    bridge_stream_entry_t *e = bridge_find_by_id(b, transport_id);

    if (phase == FIN_BR_AFTER_SERVICE || phase == FIN_BR_AFTER_RESERVICE) {
        if (e && e->active) {
            fprintf(stderr, "FINBR %s: mapping still active\n", what);
            bad++;
        }
        {   /* Absent by BOTH identities: an entry repointed to another
             * transport id while keeping the internal ref must not pass. */
            bridge_stream_entry_t *by_ref = bridge_find_by_ref(b, want_ref);
            if (by_ref && by_ref->active) {
                fprintf(stderr, "FINBR %s: mapping still active by ref\n",
                        what);
                bad++;
            }
        }
        if (moq_transport_bridge_has_pending(b)) {
            fprintf(stderr, "FINBR %s: bridge reports pending work\n", what);
            bad++;
        }
    } else {
        if (!e) {
            fprintf(stderr, "FINBR %s: mapping absent\n", what);
            return bad + 1;
        }
        if (!e->active) {
            fprintf(stderr, "FINBR %s: mapping inactive\n", what); bad++;
        }
        if (e->transport_id != transport_id) {
            fprintf(stderr, "FINBR %s: transport id\n", what); bad++;
        }
        /* BOTH identities, so a repointed entry cannot masquerade. */
        if (e->ref._v != want_ref._v) {
            fprintf(stderr, "FINBR %s: internal ref\n", what); bad++;
        }
        if (bridge_find_by_ref(b, want_ref) != e) {
            fprintf(stderr, "FINBR %s: ref lookup resolves elsewhere\n", what);
            bad++;
        }
        if (e->kind != BRIDGE_STREAM_BIDI) {
            fprintf(stderr, "FINBR %s: stream kind %d\n", what, (int)e->kind);
            bad++;
        }
        if (e->origin != BRIDGE_ORIGIN_PEER) {
            fprintf(stderr, "FINBR %s: stream origin %d\n", what,
                    (int)e->origin);
            bad++;
        }
        if (e->aborting) {
            fprintf(stderr, "FINBR %s: entry aborting\n", what); bad++;
        }
        if (e->pending_reset || e->pending_stop) {
            fprintf(stderr, "FINBR %s: reset/stop pending\n", what); bad++;
        }
        int want_peer_closed = (ingress == FIN_BR_CONSUMED);
        if (!!e->peer_send_closed != want_peer_closed) {
            fprintf(stderr, "FINBR %s: peer_send_closed %d, expected %d\n",
                    what, (int)e->peer_send_closed, want_peer_closed);
            bad++;
        }
        if (e->local_send_closed) {
            fprintf(stderr, "FINBR %s: local half already closed\n", what);
            bad++;
        }
        if (ingress == FIN_BR_CONSUMED) {
            if (e->pending_retry || e->pending_fin || e->fin_retained) {
                fprintf(stderr, "FINBR %s: FIN state retained after a "
                        "consumed ingress\n", what);
                bad++;
            }
        } else {
            if (!e->pending_retry || !e->fin_retained) {
                fprintf(stderr, "FINBR %s: retained obligation missing\n",
                        what);
                bad++;
            }
            /* `pending_fin` is a LATER FIN arriving while already suspended;
             * initial same-call retention owes it false. */
            if (e->pending_fin) {
                fprintf(stderr, "FINBR %s: unexpected pending_fin on initial "
                        "retention\n", what);
                bad++;
            }
        }
    }
    if (moq_transport_bridge_is_fatal(b)) {
        fprintf(stderr, "FINBR %s: bridge went fatal\n", what); bad++;
    }
    if (moq_transport_bridge_is_closed(b)) {
        fprintf(stderr, "FINBR %s: bridge closed\n", what); bad++;
    }
    return bad;
}


/* Set while a self-check drives the runner down a deliberately REFUSED path,
 * so an expected refusal cannot be mistaken for a failure in focused output. */
static int fin_br_quiet;


int run_fin_bridge(const fin_bridge_case_t *f, fin_bridge_run_t *r)
{
    int failures = 0;
    char what[160];
    /* An invalid descriptor must not reach ANY family hook: an oversized
     * snapshot would otherwise be written into bounded storage. */
    if (fin_bridge_problems(f) != 0) {
        if (!fin_br_quiet)
            fprintf(stderr, "FINBR %s: invalid descriptor; no hook invoked\n",
                    f->name ? f->name : "(unnamed)");
        return failures + 1;
    }
    if (f->family->snap_size > sizeof(((fin_bridge_snap_t *)0)->bytes)) {
        if (!fin_br_quiet)
            fprintf(stderr, "FINBR %s: snapshot larger than storage\n",
                    f->name);
        return failures + 1;
    }

    /* Slot, generation and the handle they pack into, all from pool state
     * BEFORE ingress. */
    r->want_slot = f->family->derive_slot(r->tp.server, &r->want_gen);
    MOQ_TEST_CHECK(r->want_slot >= 0);
    if (r->want_slot < 0) return failures;   /* the check above counted it */
    r->want_handle = moq_handle_pack(f->family->pool_tag,
                                     r->tp.server->session_tag, r->want_gen,
                                     (uint32_t)r->want_slot);
    /* This route owes NO drain at any point: the FIN is observed before the
     * terminal runs, so the declared multiset is EMPTY throughout. */
    failures += f->family->check_drain(r->tp.server, "pre-ingress");

    /* 1. Request + FIN in ONE chunk, through real bridge ingress. */
    moq_result_t want_rc = (f->ingress == FIN_BR_CONSUMED)
                               ? MOQ_OK : MOQ_ERR_WOULD_BLOCK;
    MOQ_TEST_CHECK_EQ_INT((int)f->feed(r->tp.server_bridge, f->ctx,
                                       r->transport_id, true), (int)want_rc);

    /* The SESSION ref, captured while the entry exists; re-deriving it later
     * would return NULL once retired and make every owner check vacuous. */
    {
        bridge_stream_entry_t *e =
            bridge_find_by_id(r->tp.server_bridge, r->transport_id);
        MOQ_TEST_CHECK(e != NULL);
        if (!e) return failures;             /* the check above counted it */
        r->ref = e->ref;
        MOQ_TEST_CHECK(r->ref._v != 0);
        /* The two identities are genuinely different values. */
        MOQ_TEST_CHECK(r->ref._v != r->transport_id);
        MOQ_TEST_CHECK(e->active);
    }
    snprintf(what, sizeof(what), "%s ingress", f->name);
    failures += fin_br_check_bridge(r->tp.server_bridge, r->transport_id,
                                    r->ref, f->ingress, FIN_BR_AFTER_INGRESS, what);

    /* 2. The owner, against the DECLARED identity, plus its exact edge set. */
    snprintf(what, sizeof(what), "%s admitted", f->name);
    failures += f->family->check_live(r->tp.server, r->ref, r->want_slot,
                                     r->want_gen, r->want_handle, what);
    {
        og_graph_t g;
        og_capture(r->tp.server, &g);
        failures += f->family->check_edges(&g, r->ref, r->want_slot, 1, what);
    }

    /* 3. Snapshot the owner BEFORE polling, so a mutation caused BY the poll
     *    cannot become the baseline it is measured against. */
    fin_bridge_snap_t snap;
    MOQ_TEST_CHECK(f->family->snap_size <= sizeof(snap.bytes));
    f->family->capture(r->tp.server, f->ctx, snap.bytes,
                       f->family->snap_size);

    /* Exactly the declared request event, compared field for field. */
    {
        txs_norm_vec_t got, want;
        txs_norm_init(&got);
        txs_norm_init(&want);
        moq_event_t ev;
        while (moq_session_poll_events(r->tp.server, &ev, 1) > 0) {
            if (!f->family->normalize_event(&ev, f->ctx, &got)) failures++;
            moq_event_cleanup(&ev);
        }
        failures += f->family->want_request(&want, r->want_handle);
        failures += txs_norm_equals(&got, &want, f->name);
        txs_norm_free(&got);
        txs_norm_free(&want);
    }

    /* Nothing the poll touched may have moved: the owner record, its identity,
     * its exact live edge topology and the drain multiset are ALL reasserted
     * here, so a poll-time mutation the terminal later clears cannot pass. */
    snprintf(what, sizeof(what), "%s post-poll", f->name);
    failures += f->family->check(r->tp.server, f->ctx, snap.bytes,
                                 f->family->snap_size, what);
    failures += f->family->check_live(r->tp.server, r->ref, r->want_slot,
                                      r->want_gen, r->want_handle, what);
    {
        og_graph_t g;
        og_capture(r->tp.server, &g);
        failures += f->family->check_edges(&g, r->ref, r->want_slot, 1, what);
    }
    failures += f->family->check_drain(r->tp.server, what);
    failures += fin_br_check_bridge(r->tp.server_bridge, r->transport_id,
                                    r->ref, f->ingress, FIN_BR_AFTER_INGRESS, what);

    /* 4. The application answers; the FIN having been observed, NO drain. */
    MOQ_TEST_CHECK_EQ_INT((int)f->terminal(r->tp.server_bridge, f->ctx,
                                           r->transport_id), (int)MOQ_OK);
    snprintf(what, sizeof(what), "%s terminal", f->name);
    failures += f->family->check_retired(r->tp.server, r->ref, r->want_slot,
                                        what);
    failures += f->family->check_drain(r->tp.server, what);
    {   /* Semantic retirement is proven in the RAW graph too, before any
         * physical service runs. */
        og_graph_t g;
        og_capture(r->tp.server, &g);
        failures += f->family->check_edges(&g, r->ref, r->want_slot, 0, what);
    }
    /* The PHYSICAL mapping is untouched by the semantic terminal: still active,
     * still carrying its class's ingress state, our half still open. */
    failures += fin_br_check_bridge(r->tp.server_bridge, r->transport_id,
                                    r->ref, f->ingress, FIN_BR_AFTER_TERMINAL, what);

    /* 5. Bridge service alone emits the EXACT terminal and retires the map. */
    fake_endpoint_clear_ops(&r->tp.server_ep);
    MOQ_TEST_CHECK_EQ_INT(
        (int)held_bridge_service(r->tp.server_bridge, 0), (int)MOQ_OK);
    snprintf(what, sizeof(what), "%s serviced", f->name);
    failures += f->family->check_terminal_wire(f->ctx, what);
    failures += f->family->check_retired(r->tp.server, r->ref, r->want_slot,
                                        what);
    failures += fin_br_check_bridge(r->tp.server_bridge, r->transport_id,
                                    r->ref, f->ingress, FIN_BR_AFTER_SERVICE, what);
    {
        og_graph_t g;
        og_capture(r->tp.server, &g);
        failures += f->family->check_edges(&g, r->ref, r->want_slot, 0, what);
    }
    failures += f->family->check_drain(r->tp.server, what);

    /* 6. Idempotence: a second service adds EXACTLY ZERO operations, leaves
     *    nothing queued, recreates no owner, and installs no drain. */
    fake_endpoint_clear_ops(&r->tp.server_ep);
    MOQ_TEST_CHECK_EQ_INT(
        (int)held_bridge_service(r->tp.server_bridge, 0), (int)MOQ_OK);
    snprintf(what, sizeof(what), "%s reserviced", f->name);
    MOQ_TEST_CHECK_EQ_SIZE(r->tp.server_ep.count, (size_t)0);
    {
        txs_norm_vec_t leftover;
        txs_norm_init(&leftover);
        moq_event_t ev;
        while (moq_session_poll_events(r->tp.server, &ev, 1) > 0) {
            if (!f->family->normalize_event(&ev, f->ctx, &leftover))
                failures++;
            moq_event_cleanup(&ev);
        }
        moq_action_t a;
        while (moq_session_poll_actions(r->tp.server, &a, 1) > 0) {
            if (!f->family->normalize_action(&a, f->ctx, &leftover))
                failures++;
            moq_action_cleanup(&a);
        }
        MOQ_TEST_CHECK_EQ_SIZE(leftover.count, (size_t)0);
        txs_norm_free(&leftover);
    }
    failures += f->family->check_retired(r->tp.server, r->ref, r->want_slot,
                                        what);
    failures += f->family->check_drain(r->tp.server, what);
    {
        og_graph_t g;
        og_capture(r->tp.server, &g);
        failures += f->family->check_edges(&g, r->ref, r->want_slot, 0, what);
    }
    /* The COMPLETE physical postcondition again: a mapping resurrected only by
     * the second service must not pass. */
    failures += fin_br_check_bridge(r->tp.server_bridge, r->transport_id,
                                    r->ref, f->ingress, FIN_BR_AFTER_RESERVICE, what);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(r->tp.server),
                          (int)MOQ_SESS_ESTABLISHED);
    return failures;
}


/* Permanent descriptor self-checks: every required member, the ingress class,
 * and the (kind, pool) pairing. Deleting a member from a real descriptor would
 * only fail the BUILD via an unused static, so validity is probed here on
 * local copies instead. */
static int probe_capture_calls;

static void probe_capture_forbidden(const moq_session_t *s, void *ctx,
                                    void *state, size_t cap)
{
    (void)s; (void)ctx; (void)state; (void)cap;
    probe_capture_calls++;
}


int test_fin_bridge_descriptor_validation(void)
{
    int failures = 0;
    fin_bridge_run_t r;
    memset(&r, 0, sizeof(r));

    fin_bridge_family_t o = p7_family;
    fin_bridge_case_t f;
    memset(&f, 0, sizeof(f));
    f.name = "probe";
    f.ctx = &r;
    f.ingress = FIN_BR_CONSUMED;
    f.family = &o;
    f.feed = p7_feed;
    f.terminal = p7_terminal;
    MOQ_TEST_CHECK_EQ_INT(fin_bridge_problems(&f), 0);

    /* An oversized snapshot declaration must be refused BEFORE any family hook
     * runs -- the runner writes into bounded storage, so a hook invoked under an
     * invalid descriptor would overflow it. */
    {
        fin_bridge_family_t big = p7_family;
        big.snap_size = FIN_BR_SNAP_MAX + 1;
        big.capture = probe_capture_forbidden;
        fin_bridge_case_t bad = f;
        bad.family = &big;
        probe_capture_calls = 0;
        MOQ_TEST_CHECK(fin_bridge_problems(&bad) > 0);
        fin_br_quiet = 1;
        MOQ_TEST_CHECK(run_fin_bridge(&bad, &r) > 0);
        fin_br_quiet = 0;
        MOQ_TEST_CHECK_EQ_INT(probe_capture_calls, 0);
    }

    /* ingress class */
    f.ingress = 0;              MOQ_TEST_CHECK(fin_bridge_problems(&f) > 0);
    f.ingress = 99;             MOQ_TEST_CHECK(fin_bridge_problems(&f) > 0);
    f.ingress = FIN_BR_RETAINED; MOQ_TEST_CHECK_EQ_INT(fin_bridge_problems(&f), 0);
    f.ingress = FIN_BR_CONSUMED;

    /* the bundle itself */
    f.family = NULL;             MOQ_TEST_CHECK(fin_bridge_problems(&f) > 0);
    f.family = &o;

    /* every required member */
    o = p7_family; o.snap_size = 0;
    MOQ_TEST_CHECK(fin_bridge_problems(&f) > 0);
    o = p7_family; o.snap_size = FIN_BR_SNAP_MAX + 1;
    MOQ_TEST_CHECK(fin_bridge_problems(&f) > 0);
    /* A nonzero but UNDERSIZED declaration validates structurally -- the
     * descriptor cannot know the family's real state size -- so the runner
     * passes the DECLARED size and the family refuses to write. That refusal
     * is what the undersized mutant below proves. */
    o = p7_family; o.snap_size = 1;
    MOQ_TEST_CHECK_EQ_INT(fin_bridge_problems(&f), 0);
    o = p7_family; o.derive_slot = NULL;
    MOQ_TEST_CHECK(fin_bridge_problems(&f) > 0);
    o = p7_family; o.check_live = NULL;
    MOQ_TEST_CHECK(fin_bridge_problems(&f) > 0);
    o = p7_family; o.check_retired = NULL;
    MOQ_TEST_CHECK(fin_bridge_problems(&f) > 0);
    o = p7_family; o.check_edges = NULL;
    MOQ_TEST_CHECK(fin_bridge_problems(&f) > 0);
    o = p7_family; o.check_drain = NULL;
    MOQ_TEST_CHECK(fin_bridge_problems(&f) > 0);
    o = p7_family; o.check_terminal_wire = NULL;
    MOQ_TEST_CHECK(fin_bridge_problems(&f) > 0);
    o = p7_family; o.want_request = NULL;
    MOQ_TEST_CHECK(fin_bridge_problems(&f) > 0);

    /* family identity: unknown kind, and a tag that belongs to another family */
    o = p7_family; o.owner_kind = MOQ_REQ_NONE;
    MOQ_TEST_CHECK(fin_bridge_problems(&f) > 0);
    o = p7_family; o.owner_kind = 0x7f;
    MOQ_TEST_CHECK(fin_bridge_problems(&f) > 0);
    o = p7_family; o.pool_tag = MOQ_HANDLE_POOL_FETCH;
    MOQ_TEST_CHECK(fin_bridge_problems(&f) > 0);
    o = p7_family; o.owner_kind = MOQ_REQ_FETCH;   /* tag still TRACK_STATUS */
    MOQ_TEST_CHECK(fin_bridge_problems(&f) > 0);
    o = p7_family;
    MOQ_TEST_CHECK_EQ_INT(fin_bridge_problems(&f), 0);

    /* the output members, now family-owned */
    o = p7_family; o.capture = NULL;
    MOQ_TEST_CHECK(fin_bridge_problems(&f) > 0);
    o = p7_family; o.check = NULL;
    MOQ_TEST_CHECK(fin_bridge_problems(&f) > 0);
    o = p7_family; o.normalize_event = NULL;
    MOQ_TEST_CHECK(fin_bridge_problems(&f) > 0);
    o = p7_family; o.normalize_action = NULL;
    MOQ_TEST_CHECK(fin_bridge_problems(&f) > 0);
    o = p7_family;
    MOQ_TEST_CHECK_EQ_INT(fin_bridge_problems(&f), 0);

    /* the case's single context */
    f.ctx = NULL;               MOQ_TEST_CHECK(fin_bridge_problems(&f) > 0);
    f.ctx = &r;
    MOQ_TEST_CHECK_EQ_INT(fin_bridge_problems(&f), 0);
    return failures;
}
