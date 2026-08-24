/*
 * SUBSCRIBE_NAMESPACE response-stream termination (draft-18 §10.18).
 *
 * §10.18, lines 4829-4832 of draft-ietf-moq-transport-18:
 *
 *   "When a subscriber receives a stream reset or FIN on a SUBSCRIBE_NAMESPACE
 *    response stream, it SHOULD treat this as though each active namespace
 *    received a NAMESPACE_DONE."
 *
 * One sentence, two ingress signals. The per-suffix outcome is derived from the
 * explicit NAMESPACE_DONE message arm; the owner-retirement shape is derived
 * from the REQUEST_ERROR terminal, the only established terminal on this route.
 * FIN additionally closes our own send half, because after the peer's half is
 * gone nothing else completes the bidi's physical lifecycle; RESET does not,
 * because the reset ingress owns physical teardown. STOP_SENDING is a distinct
 * signal and is held to its current behaviour by a negative control.
 *
 * The active-suffix set is the durable cursor: per-suffix outcomes may commit
 * across event-capacity retries, while the final outcome, the close-half action
 * and retirement must all be capacity-safe.
 */
#include <moq/moq.h>
#include <moq/control_d18.h>
#include "test_support.h"
#include "../support/ownership_graph.h"
#include "../support/txn_snapshot.h"
#include "../../core/src/session/session_internal.h"
#include "../support/ns_owner_inventory.h"

/* -- fixture constants, declared once and never read back ------------ */

#define NF_PREFIX      "live"
#define NF_PREFIX2     "v2"   /* two parts: boundaries are observable */
#define NF_REQUEST_ID  0u
#define NF_INTEREST    MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE
#define NF_RESET_CODE  0x2Bu

/* Two-part suffixes keep both namespace fields non-default. */
static const char *const kA0 = "room",  *const kA1 = "alpha";
static const char *const kB0 = "room",  *const kB1 = "beta";
/* R7 needs charges whose subset sums are unique, so removing the WRONG
 * suffix cannot satisfy the budget oracle: lengths 1, 2 and 4. */
static const char *const kP0 = "room", *const kP1 = "p";
static const char *const kQ0 = "room", *const kQ1 = "qq";
static const char *const kS0 = "room", *const kS1 = "ssss";
/* Same byte length as "alpha", different bytes: a substitution mutant that a
 * length-only oracle would miss. */
static const char *const kX1 = "alpZa";

/* The EVENT half of the per-round contract: every newly collected event must
 * match a DECLARED outcome, none may repeat, and `seen`/`spent` advance only
 * for accepted ones. Factored so the live probe exercises the same path the
 * row does. */
static int nf_round_classify(const nf_ev_t *got, size_t got_n,
                             const nf_ev_t *want, size_t want_n,
                             const size_t *charge, int *seen, size_t *spent,
                             size_t *accepted, const char *what)
{
    int failures = 0;
    for (size_t i = 0; i < got_n; i++) {
        size_t j;
        for (j = 0; j < want_n; j++)
            if (nf_ev_same(&got[i], &want[j])) break;
        if (j == want_n) {
            NF_DIAG("NF %s: undeclared outcome (kind %d)\n", what,
                    got[i].kind);
            failures++;
            continue;
        }
        if (seen[j]) {
            NF_DIAG("NF %s: declared outcome %zu surfaced twice\n", what, j);
            failures++;
            continue;
        }
        seen[j] = 1;
        *spent += charge[j];
        (*accepted)++;
    }
    return failures;
}

/* The per-round contract is TWO members and the wrapper below is the only
 * way a row runs a partial round: `nf_partial_round` classifies the batch
 * (advancing `seen`/`spent`) and then checks the state half against that
 * updated `spent`. `nf_partial_round_ok` is the state member and is never
 * called directly by a row.
 *
 * The FINAL successful round deliberately uses the classifier alone: the
 * owner is expected to RETIRE there, so the non-retirement and
 * owner-conservation checks below would be wrong. */
/* The state half of the per-round contract of a partially blocked terminal, in one
 * place so the live probe exercises the same code the row does: the owner is
 * still live with its FIN carrier latched and every other field unchanged,
 * the graph topology and the whole drain multiset are conserved, no close
 * action has been queued, retirement has not happened, and the receive budget
 * is reduced by exactly the canonical charges of the outcomes emitted so far. */
static int nf_partial_round_ok(moq_session_t *c, int slot,
                               const nf_inv_t *live, const og_graph_t *g0,
                               const nf_drain_t *d0, const nf_acts_t *acts,
                               size_t budget_active, size_t spent,
                               const char *what)
{
    nf_inv_t want = *live;
    want.pending_fin = 1;
    int failures = nf_inv_check(c, slot, &want, what);
    og_graph_t gi;
    og_capture(c, &gi);
    failures += og_check_same_topology(g0, &gi, what);
    nf_drain_t di;
    if (nf_drain_snap(c, &di) != 0) return failures + 1;
    failures += nf_drain_equals(&di, d0, what);
    failures += nf_acts_equals(acts, 0, what);
    if (c->ns_subs[slot].state == MOQ_NS_SUB_FREE) {
        NF_DIAG("NF %s: the owner was retired before the final outcome\n",
                what);
        failures++;
    }
    if (c->recv_payload_bytes != budget_active - spent) {
        NF_DIAG("NF %s: budget %zu, expected %zu after the outcomes so far\n",
                what, c->recv_payload_bytes, budget_active - spent);
        failures++;
    }
    return failures;
}

/* The bundled partial-round contract: classification and state, in that
 * order, with the state half measured against the freshly updated `spent`.
 * Rows call THIS, never either member alone. */
static int nf_partial_round(moq_session_t *c, int slot, uint64_t handle,
                            const nf_ev_t *want, size_t want_n,
                            const size_t *charge, int *seen, size_t *spent,
                            size_t *accepted, nf_acts_t *acts, uint64_t ref,
                            const nf_inv_t *live, const og_graph_t *g0,
                            const nf_drain_t *d0, size_t budget_active,
                            const char *what)
{
    nf_ev_t got[NF_EV_MAX];
    size_t k = 0;
    int failures = nf_collect(c, handle, got, NF_EV_MAX, &k, what);
    failures += nf_round_classify(got, k, want, want_n, charge, seen, spent,
                                  accepted, what);
    nf_take_actions(c, ref, acts);
    failures += nf_partial_round_ok(c, slot, live, g0, d0, acts,
                                    budget_active, *spent, what);
    return failures;
}

/* -- the strict fixture ------------------------------------------------
 *
 * Every start/feed result is checked, every setup event and action is
 * classified (extras are failures), and the owner's identity is DERIVED from
 * pool state captured before the request is issued -- never adopted from what
 * the call produced.
 */
typedef struct nf_fx {
    moq_session_t      *c;
    moq_ns_sub_handle_t h;
    moq_stream_ref_t    ref;
    int                 slot;
    uint32_t            generation;   /* derived, then confirmed */
    uint64_t            handle;       /* derived, then confirmed */
} nf_fx_t;

/* Everything up to and including the validated public request: the session,
 * the handshake, the derived-then-confirmed owner and its two edges, with the
 * subscription left PENDING_SUBSCRIBER. `nf_fx_setup` is this plus the
 * acceptance, so every signed row's setup is unchanged. */
static int nf_fx_setup_pending(nf_fx_t *f, uint32_t max_events,
                               uint32_t max_ns_subs, uint32_t max_actions)
{
    int bad = 0;
    memset(f, 0, sizeof(*f));
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_CLIENT);
    cfg.version = MOQ_VERSION_DRAFT_18;
    if (max_events)   cfg.max_events = max_events;
    if (max_ns_subs)  cfg.max_namespace_subscriptions = max_ns_subs;
    if (max_actions)  cfg.max_actions = max_actions;
    if (moq_session_create(&cfg, 0, &f->c) != MOQ_OK || !f->c) {
        fprintf(stderr, "NF setup: session_create failed\n");
        return 1;
    }
    if (moq_session_start(f->c, 0) != MOQ_OK) {
        fprintf(stderr, "NF setup: session_start failed\n");
        return 1;
    }
    /* The client's own SETUP is the only action so far, and there is no
     * event yet: classify rather than drain. */
    {
        moq_action_t a;
        int setups = 0, other = 0;
        while (moq_session_poll_actions(f->c, &a, 1) > 0) {
            if (a.kind == MOQ_ACTION_SEND_CONTROL ||
                a.kind == MOQ_ACTION_OPEN_UNI_CONTROL)
                setups++;
            else
                other++;
            moq_action_cleanup(&a);
        }
        if (other) {
            fprintf(stderr, "NF setup: %d unexpected pre-SETUP actions\n",
                    other);
            bad++;
        }
        if (setups == 0) {
            fprintf(stderr, "NF setup: the client queued no SETUP output\n");
            bad++;
        }
    }
    bad += nf_expect_no_events(f->c, "setup pre-handshake");

    uint8_t setup[16];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, setup, sizeof(setup));
    if (moq_d18_encode_setup(&w) != MOQ_OK) {
        fprintf(stderr, "NF setup: encode_setup failed\n");
        return 1;
    }
    if (moq_session_on_control_bytes(f->c, setup, moq_buf_writer_offset(&w),
                                     0) != MOQ_OK) {
        fprintf(stderr, "NF setup: peer SETUP was not accepted\n");
        return 1;
    }
    {
        moq_event_t ev;
        int complete = 0;
        while (moq_session_poll_events(f->c, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SETUP_COMPLETE) complete++;
            else {
                fprintf(stderr, "NF setup: unexpected event kind %d after"
                        " SETUP\n", (int)ev.kind);
                bad++;
            }
            moq_event_cleanup(&ev);
        }
        if (complete != 1) {
            fprintf(stderr, "NF setup: %d SETUP_COMPLETE events, expected 1\n",
                    complete);
            bad++;
        }
    }
    bad += nf_expect_no_actions(f->c, "setup post-handshake");

    /* Derive the destination BEFORE the request exists. */
    int want_slot = -1;
    for (size_t i = 0; i < f->c->ns_sub_cap; i++) {
        if (f->c->ns_subs[i].state == MOQ_NS_SUB_FREE) {
            want_slot = (int)i;
            break;
        }
    }
    if (want_slot < 0) {
        fprintf(stderr, "NF setup: no free ns_sub slot\n");
        return 1;
    }
    nf_inv_t free_rec;
    nf_inv_read(f->c, want_slot, &free_rec);
    const uint64_t want_ref = f->c->next_stream_ref;
    uint32_t want_gen = (f->c->ns_subs[want_slot].generation | 1u);
    uint64_t want_handle = moq_handle_pack(MOQ_HANDLE_POOL_NAMESPACE_SUB,
                                           f->c->session_tag, want_gen,
                                           (uint32_t)want_slot);
    if (want_handle == 0) {
        fprintf(stderr, "NF setup: derived handle is structurally invalid\n");
        return 1;
    }

    moq_subscribe_namespace_cfg_t nc;
    moq_subscribe_namespace_cfg_init(&nc);
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL(NF_PREFIX),
                            MOQ_BYTES_LITERAL(NF_PREFIX2) };
    nc.track_namespace_prefix = (moq_namespace_t){ parts, 2 };
    nc.namespace_interest = NF_INTEREST;
    if (moq_session_subscribe_namespace(f->c, &nc, 0, &f->h) != MOQ_OK) {
        fprintf(stderr, "NF setup: subscribe_namespace failed\n");
        return 1;
    }

    /* Exactly one OPEN_BIDI_STREAM, fin false, carrying exactly one
     * SUBSCRIBE_NAMESPACE with no trailing bytes. */
    {
        moq_action_t a;
        int opens = 0, other = 0;
        while (moq_session_poll_actions(f->c, &a, 1) > 0) {
            if (a.kind == MOQ_ACTION_OPEN_BIDI_STREAM) {
                opens++;
                f->ref = a.u.open_bidi_stream.stream_ref;
                if (a.u.open_bidi_stream.fin) {
                    fprintf(stderr, "NF setup: request bidi opened with FIN\n");
                    bad++;
                }
                moq_control_envelope_t env;
                moq_buf_reader_t r;
                moq_buf_reader_init(&r, a.u.open_bidi_stream.data,
                                    a.u.open_bidi_stream.len);
                if (moq_d18_decode_envelope(&r, &env) != MOQ_OK) {
                    fprintf(stderr, "NF setup: request body is not a"
                            " decodable envelope\n");
                    bad++;
                } else if (env.msg_type != MOQ_D18_SUBSCRIBE_NAMESPACE) {
                    fprintf(stderr, "NF setup: request type 0x%llx, expected"
                            " SUBSCRIBE_NAMESPACE\n",
                            (unsigned long long)env.msg_type);
                    bad++;
                } else if (moq_buf_reader_remaining(&r) != 0) {
                    fprintf(stderr, "NF setup: %zu trailing bytes after the"
                            " request\n", moq_buf_reader_remaining(&r));
                    bad++;
                } else {
                    moq_bytes_t dparts[MOQ_DECODED_MAX_NAMESPACE_PARTS];
                    moq_d18_subscribe_namespace_t sn;
                    if (moq_d18_decode_subscribe_namespace(
                            env.payload, env.payload_len, dparts,
                            MOQ_DECODED_MAX_NAMESPACE_PARTS, &sn) != MOQ_OK) {
                        fprintf(stderr, "NF setup: SUBSCRIBE_NAMESPACE body"
                                " did not decode\n");
                        bad++;
                    } else {
                        if (sn.request_id != NF_REQUEST_ID) {
                            fprintf(stderr, "NF setup: request id %llu,"
                                    " expected %u\n",
                                    (unsigned long long)sn.request_id,
                                    NF_REQUEST_ID);
                            bad++;
                        }
                        if (sn.track_namespace_prefix.count != 2) {
                            fprintf(stderr, "NF setup: prefix has %zu parts,"
                                    " expected 2\n",
                                    sn.track_namespace_prefix.count);
                            bad++;
                        } else {
                            bad += txs_check_part_bytes(
                                &sn.track_namespace_prefix, 0, NF_PREFIX,
                                strlen(NF_PREFIX), "NF setup prefix 0");
                            bad += txs_check_part_bytes(
                                &sn.track_namespace_prefix, 1, NF_PREFIX2,
                                strlen(NF_PREFIX2), "NF setup prefix 1");
                        }
                        /* Draft-18 gives namespace interest no wire
                         * representation -- NAMESPACE_STATE is implied and any
                         * other value is refused before encoding
                         * (profile_d18.c:748). What the wire must carry here
                         * is therefore NO parameters at all. */
                        if (sn.params.auth_token_count != 0 ||
                            sn.params.has_forward ||
                            sn.params.has_subscriber_priority ||
                            sn.params.has_filter) {
                            fprintf(stderr, "NF setup: the request carries"
                                    " unexpected parameters\n");
                            bad++;
                        }
                    }
                }
            } else {
                other++;
            }
            moq_action_cleanup(&a);
        }
        if (opens != 1) {
            fprintf(stderr, "NF setup: %d OPEN_BIDI_STREAM actions,"
                    " expected 1\n", opens);
            bad++;
        }
        if (other) {
            fprintf(stderr, "NF setup: %d unexpected actions with the"
                    " request\n", other);
            bad++;
        }
    }
    bad += nf_expect_no_events(f->c, "setup after request");
    if (f->ref._v == 0) {
        fprintf(stderr, "NF setup: no request stream ref\n");
        return 1;
    }

    /* Confirm the API handle and the physical owner against the derivation. */
    f->slot = want_slot;
    f->generation = want_gen;
    f->handle = want_handle;
    if (f->h._opaque != want_handle) {
        fprintf(stderr, "NF setup: handle %llu, derived %llu\n",
                (unsigned long long)f->h._opaque,
                (unsigned long long)want_handle);
        bad++;
    }
    if (f->c->ns_subs[want_slot].generation != want_gen) {
        fprintf(stderr, "NF setup: generation %u, derived %u\n",
                f->c->ns_subs[want_slot].generation, want_gen);
        bad++;
    }
    if (f->c->ns_subs[want_slot].stream_ref._v != f->ref._v) {
        fprintf(stderr, "NF setup: owner stream ref mismatch\n");
        bad++;
    }
    if (f->ref._v != want_ref) {
        fprintf(stderr, "NF setup: stream ref %llu, derived %llu\n",
                (unsigned long long)f->ref._v, (unsigned long long)want_ref);
        bad++;
    }
    if (moq_index_find(f->c->idx_ns_by_ref, f->c->idx_ns_mask, f->ref._v) !=
        want_slot) {
        fprintf(stderr, "NF setup: idx_ns_by_ref does not name the derived"
                " slot\n");
        bad++;
    }

    /* The locally issued pending-subscriber owner, declared from the free
     * record and the fixture's own inputs. This is the same declaration R5
     * reuses for the reused slot, exercised here on the green first request
     * so a wrong contract fails immediately rather than only in the RED. */
    {
        static const char *const kParts[2] = { NF_PREFIX, NF_PREFIX2 };
        nf_inv_t want = nf_local_pending_want(&free_rec, want_gen,
                                              want_handle, NF_REQUEST_ID,
                                              want_ref, kParts, 2,
                                              NF_INTEREST);
        int adopt = nf_adopt_prefix_addrs(f->c, want_slot, &want,
                                          "NF setup owner");
        bad += adopt;
        /* A failed adoption leaves `want` holding no validated addresses, so
         * comparing against it would be unfounded (and would re-read a
         * pointer this helper just rejected). */
        if (adopt == 0)
            bad += nf_inv_check(f->c, want_slot, &want, "NF setup owner");
        og_graph_t g;
        og_capture(f->c, &g);
        bad += og_check_integrity(&g, "NF setup");
        const og_edge_spec_t edges[] = {
            { OG_DOM_NS_REF,  want_ref },
            { OG_DOM_REQ_RID, NF_REQUEST_ID },
        };
        bad += og_check_owner_edges(&g, MOQ_REQ_NAMESPACE_SUB, want_slot,
                                    edges, 2, "NF setup");
    }
    return bad;
}

static int nf_fx_setup(nf_fx_t *f, uint32_t max_events, uint32_t max_ns_subs,
                       uint32_t max_actions)
{
    int bad = nf_fx_setup_pending(f, max_events, max_ns_subs, max_actions);
    if (bad && f->ref._v == 0) return bad;

    /* Accept the request: exactly one REQUEST_OK in, exactly one NS_SUB_OK
     * out, no action, and the owner established. */
    uint8_t ok[32];
    moq_buf_writer_t ow;
    moq_buf_writer_init(&ow, ok, sizeof(ok));
    if (moq_d18_encode_request_ok(&ow) != MOQ_OK) {
        fprintf(stderr, "NF setup: encode_request_ok failed\n");
        return 1;
    }
    if (moq_session_on_bidi_stream_bytes(f->c, f->ref, ok,
                                         moq_buf_writer_offset(&ow), false,
                                         0) != MOQ_OK) {
        fprintf(stderr, "NF setup: REQUEST_OK was not accepted\n");
        return 1;
    }
    {
        nf_ev_t got[NF_EV_MAX];
        size_t n = 0;
        bad += nf_collect(f->c, f->handle, got, NF_EV_MAX, &n, "setup ok");
        if (n != 1) {
            fprintf(stderr, "NF setup: %zu events for REQUEST_OK, expected"
                    " 1\n", n);
            bad++;
        } else {
            nf_ev_t want = nf_ev_want(MOQ_EVENT_NS_SUB_OK, NULL, NULL);
            bad += nf_ev_equals(&got[0], &want, "setup ns_sub_ok");
        }
    }
    bad += nf_expect_no_actions(f->c, "setup after REQUEST_OK");
    if (f->c->ns_subs[f->slot].state != MOQ_NS_SUB_ESTABLISHED ||
        !f->c->ns_subs[f->slot].got_response) {
        fprintf(stderr, "NF setup: owner is not an established subscriber\n");
        bad++;
    }
    return bad;
}

static void nf_fx_free(nf_fx_t *f)
{
    moq_event_t ev; moq_action_t a;
    while (moq_session_poll_events(f->c, &ev, 1) > 0) moq_event_cleanup(&ev);
    while (moq_session_poll_actions(f->c, &a, 1) > 0) moq_action_cleanup(&a);
    moq_session_destroy(f->c);
}

/* Feed one NAMESPACE and require exactly its NAMESPACE_FOUND. */
static int nf_announce(nf_fx_t *f, const char *a, const char *b,
                       const char *what)
{
    int bad = 0;
    uint8_t msg[160];
    size_t n = nf_encode_ns(msg, sizeof(msg), MOQ_D18_NAMESPACE, a, b);
    if (n == 0) {
        fprintf(stderr, "NF %s: NAMESPACE encode failed\n", what);
        return bad + 1;
    }
    if (moq_session_on_bidi_stream_bytes(f->c, f->ref, msg, n, false, 0) !=
        MOQ_OK) {
        fprintf(stderr, "NF %s: NAMESPACE was not accepted\n", what);
        return bad + 1;
    }
    nf_ev_t got[NF_EV_MAX];
    size_t k = 0;
    bad += nf_collect(f->c, f->handle, got, NF_EV_MAX, &k, what);
    if (k != 1) {
        fprintf(stderr, "NF %s: %zu events for NAMESPACE, expected 1\n", what,
                k);
        return bad + 1;
    }
    nf_ev_t want = nf_ev_want(MOQ_EVENT_NAMESPACE_FOUND, a, b);
    bad += nf_ev_equals(&got[0], &want, what);
    bad += nf_expect_no_actions(f->c, what);
    return bad;
}

/* The complete retirement oracle: the declared free inventory, the exact
 * budget return, and an owner nothing can still reach. */
static int nf_check_retired(moq_session_t *c, int slot, uint64_t ref,
                            const nf_inv_t *live, size_t budget0,
                            nf_inv_t *out, const char *what)
{
    nf_inv_t want = *live;
    nf_inv_apply_free(&want);
    int bad = nf_inv_check(c, slot, &want, what);
    if (c->recv_payload_bytes != budget0) {
        NF_DIAG("NF %s: receive budget %zu, expected %zu\n", what,
                c->recv_payload_bytes, budget0);
        bad++;
    }
    og_graph_t g;
    og_capture(c, &g);
    bad += og_check_integrity(&g, what);
    bad += og_check_no_edge(&g, OG_DOM_NS_REF, ref, what);
    bad += og_check_owner_unreferenced(&g, MOQ_REQ_NAMESPACE_SUB, slot, what);
    if (out) nf_inv_read(c, slot, out);
    return bad;
}

/* -- harness self-checks ----------------------------------------------
 *
 * The multiset matcher probes candidates silently and speaks only when a
 * declared expectation has no match at all.
 */
static int nf_self_checks(void)
{
    int failures = 0;
    nf_ev_t a = nf_ev_want(MOQ_EVENT_NAMESPACE_GONE, kA0, kA1);
    nf_ev_t b = nf_ev_want(MOQ_EVENT_NAMESPACE_GONE, kB0, kB1);
    nf_ev_t x = nf_ev_want(MOQ_EVENT_NAMESPACE_GONE, kA0, kX1);

    nf_ev_t got_ordered[2]  = { a, b };
    nf_ev_t got_reversed[2] = { b, a };
    nf_ev_t want[2]         = { a, b };
    /* Reordered success must be SILENT: a false diagnostic here would train
     * the reader to ignore real ones. */
    MOQ_TEST_CHECK_EQ_INT(nf_multiset(got_ordered, 2, want, 2, "self"), 0);
    MOQ_TEST_CHECK_EQ_INT(nf_multiset(got_reversed, 2, want, 2, "self"), 0);

    /* Same length, different bytes: must fail. */
    nf_ev_t got_sub[2] = { x, b };
    nf_quiet = 1;
    MOQ_TEST_CHECK(nf_multiset(got_sub, 2, want, 2, "self-quiet") > 0);
    /* A duplicate cannot satisfy two distinct declarations. */
    nf_ev_t got_dup[2] = { a, a };
    MOQ_TEST_CHECK(nf_multiset(got_dup, 2, want, 2, "self-quiet") > 0);
    /* A wrong count is refused before any probing. */
    MOQ_TEST_CHECK(nf_multiset(got_ordered, 1, want, 2, "self-quiet") > 0);
    nf_quiet = 0;

    MOQ_TEST_CHECK(nf_ev_same(&a, &a));
    MOQ_TEST_CHECK(!nf_ev_same(&a, &x));

    /* The declared free record is EXACTLY one free: a double increment must
     * not pass, which "generation advanced" alone would. */
    {
        nf_inv_t live;
        memset(&live, 0, sizeof(live));
        live.valid = 1;
        live.cmp_suffix_ptr = 1;
        /* Seed every PRESERVED field with a non-default value, so surviving
         * the free is a real assertion rather than 0 == 0. */
        live.handle     = 0xABCD1234u;
        live.request_id = 42u;
        live.stream_ref = 0x99u;
        live.interest   = 3;
        live.recv_buf   = (const void *)0x5150;
        live.recv_cap   = 4096u;
        /* ...and representative CLEARED fields likewise non-default. */
        live.state = 4; live.stream_kind = 2; live.generation = 5;
        live.prefix_valid = 1; live.prefix_buf = (const void *)0x1;
        live.prefix_buf_len = 4; live.prefix_count = 1;
        live.part_len[0] = 4; live.part_ptr[0] = (const void *)0x2;
        live.got_response = 1; live.pending_fin = 1; live.forward = 1;
        live.auth_processed = 1; live.auth_reject_code = 7;
        live.token_count = 2; live.goaway_sent = 1;
        live.ep_kind = 3; live.ep_slot = 1; live.ep_has_ref = 1;
        live.recv_len = 9; live.recv_bytes_len = 9;
        live.suffixes = (const void *)0x1; live.suffixes_inbound = 1;

        nf_inv_t freed = live;
        nf_inv_apply_free(&freed);
        MOQ_TEST_CHECK_EQ_INT(nf_inv_equals(&freed, &freed, "self"), 0);
        /* PRESERVED survived. */
        MOQ_TEST_CHECK_EQ_U64(freed.handle, 0xABCD1234u);
        MOQ_TEST_CHECK_EQ_U64(freed.request_id, 42u);
        MOQ_TEST_CHECK_EQ_U64(freed.stream_ref, 0x99u);
        MOQ_TEST_CHECK_EQ_INT(freed.interest, 3);
        MOQ_TEST_CHECK(freed.recv_buf == (const void *)0x5150);
        MOQ_TEST_CHECK_EQ_SIZE(freed.recv_cap, 4096u);
        /* CLEARED reached their declared post-free values. */
        MOQ_TEST_CHECK_EQ_U64(freed.generation, 6u);
        MOQ_TEST_CHECK_EQ_INT(freed.state, (int)MOQ_NS_SUB_FREE);
        MOQ_TEST_CHECK_EQ_INT(freed.stream_kind,
                              (int)MOQ_STREAM_KIND_UNKNOWN);
        MOQ_TEST_CHECK(!freed.prefix_valid && !freed.prefix_buf &&
                       !freed.prefix_parts && freed.prefix_count == 0 &&
                       freed.prefix_buf_len == 0);
        MOQ_TEST_CHECK(freed.part_len[0] == 0 && freed.part_ptr[0] == NULL);
        MOQ_TEST_CHECK(!freed.got_response && !freed.pending_fin &&
                       !freed.forward && !freed.auth_processed &&
                       freed.auth_reject_code == 0 && freed.token_count == 0 &&
                       !freed.goaway_sent);
        MOQ_TEST_CHECK(freed.ep_kind == 0 && freed.ep_slot == 0 &&
                       !freed.ep_has_ref);
        MOQ_TEST_CHECK(freed.recv_len == 0 && freed.recv_bytes_len == 0);
        MOQ_TEST_CHECK(freed.suffixes == NULL && !freed.suffixes_inbound);
        nf_quiet = 1;
        nf_inv_t m = freed;
        m.generation = 7;            /* freed twice */
        MOQ_TEST_CHECK(nf_inv_equals(&m, &freed, "self-quiet") > 0);
        m = freed;
        m.suffixes = (const void *)0x1;   /* tracker left behind */
        MOQ_TEST_CHECK(nf_inv_equals(&m, &freed, "self-quiet") > 0);
        m = freed;
        m.prefix_valid = 1;          /* a field the free must clear */
        MOQ_TEST_CHECK(nf_inv_equals(&m, &freed, "self-quiet") > 0);
        m = freed;
        m.handle = 0x9999;           /* a field the free must PRESERVE */
        MOQ_TEST_CHECK(nf_inv_equals(&m, &freed, "self-quiet") > 0);
        nf_quiet = 0;
    }

    /* The owner inventory is field-by-field, including byte contents: an
     * unrelated scalar and a same-length byte substitution must both fail. */
    {
        nf_inv_t base;
        memset(&base, 0, sizeof(base));
        base.valid = 1;
        base.state = 4;
        base.pfx_bytes_len = 5;
        memcpy(base.pfx_bytes, "alpha", 5);
        MOQ_TEST_CHECK_EQ_INT(nf_inv_equals(&base, &base, "self"), 0);
        nf_quiet = 1;
        nf_inv_t m = base;
        m.goaway_sent = 1;                       /* unrelated field */
        MOQ_TEST_CHECK(nf_inv_equals(&m, &base, "self-quiet") > 0);
        m = base;
        memcpy(m.pfx_bytes, "alpZa", 5);         /* same length, other bytes */
        MOQ_TEST_CHECK(nf_inv_equals(&m, &base, "self-quiet") > 0);
        m = base;
        m.valid = 0;                             /* incomparable is a failure */
        MOQ_TEST_CHECK(nf_inv_equals(&m, &base, "self-quiet") > 0);
        /* Same count, same concatenation, different boundaries. */
        nf_inv_t p1 = base, p2 = base;
        p1.prefix_count = 2; p1.part_len[0] = 2; p1.part_len[1] = 1;
        p2.prefix_count = 2; p2.part_len[0] = 1; p2.part_len[1] = 2;
        p1.parts_bytes_len = p2.parts_bytes_len = 3;
        memcpy(p1.parts_bytes, "abc", 3);
        memcpy(p2.parts_bytes, "abc", 3);
        MOQ_TEST_CHECK(nf_inv_equals(&p1, &p2, "self-quiet") > 0);
        nf_quiet = 0;
    }

    /* An overflowed drain snapshot is INCOMPARABLE, never a weaker
     * comparison -- and it never appears on a live row. */
    {
        nf_drain_t a1, b1;
        memset(&a1, 0, sizeof(a1));
        memset(&b1, 0, sizeof(b1));
        a1.overflow = 1;
        nf_quiet = 1;
        MOQ_TEST_CHECK(nf_drain_equals(&a1, &b1, "self-quiet") > 0);
        MOQ_TEST_CHECK(nf_drain_equals(&b1, &a1, "self-quiet") > 0);
        nf_quiet = 0;
    }

    /* Declaration-only constructor probes: the bound is enforced BEFORE any
     * byte is copied, so an oversized declaration comes back explicitly
     * incomparable rather than as a partial or out-of-bounds write. No live
     * owner state is involved, so nothing needs restoring. */
    {
        nf_inv_t base_rec;
        memset(&base_rec, 0, sizeof(base_rec));
        base_rec.valid = 1;
        base_rec.cmp_suffix_ptr = 1;

        /* exact fit: NF_INV_BYTES split across two parts */
        static char big_a[NF_INV_BYTES / 2 + 1];
        static char big_b[NF_INV_BYTES / 2 + 1];
        memset(big_a, 'a', sizeof(big_a) - 1);
        memset(big_b, 'b', sizeof(big_b) - 1);
        big_a[sizeof(big_a) - 1] = '\0';
        big_b[sizeof(big_b) - 1] = '\0';
        const char *fit[2] = { big_a, big_b };
        nf_inv_t w = nf_local_pending_want(&base_rec, 1u, 1u, 0u, 1u, fit, 2,
                                           NF_INTEREST);
        MOQ_TEST_CHECK(w.valid);
        MOQ_TEST_CHECK_EQ_SIZE(w.pfx_bytes_len, (size_t)NF_INV_BYTES);
        MOQ_TEST_CHECK_EQ_SIZE(w.parts_bytes_len, (size_t)NF_INV_BYTES);

        nf_quiet = 1;
        /* one byte over: incomparable, and NOTHING copied */
        static char over[NF_INV_BYTES + 2];
        memset(over, 'c', sizeof(over) - 1);
        over[sizeof(over) - 1] = '\0';
        const char *one_over[1] = { over };
        nf_inv_t w2 = nf_local_pending_want(&base_rec, 1u, 1u, 0u, 1u,
                                            one_over, 1, NF_INTEREST);
        MOQ_TEST_CHECK(!w2.valid);
        MOQ_TEST_CHECK_EQ_SIZE(w2.pfx_bytes_len, (size_t)0);
        MOQ_TEST_CHECK_EQ_SIZE(w2.parts_bytes_len, (size_t)0);

        /* too many parts */
        const char *many[MOQ_DECODED_MAX_NAMESPACE_PARTS + 1];
        for (size_t i = 0; i < MOQ_DECODED_MAX_NAMESPACE_PARTS + 1; i++)
            many[i] = "x";
        nf_inv_t w3 = nf_local_pending_want(
            &base_rec, 1u, 1u, 0u, 1u, many,
            MOQ_DECODED_MAX_NAMESPACE_PARTS + 1, NF_INTEREST);
        MOQ_TEST_CHECK(!w3.valid);

        /* NULL parts array with a nonzero count */
        nf_inv_t w4 = nf_local_pending_want(&base_rec, 1u, 1u, 0u, 1u, NULL, 2,
                                            NF_INTEREST);
        MOQ_TEST_CHECK(!w4.valid);

        /* a NULL element inside a valid array */
        const char *hole[2] = { "ok", NULL };
        nf_inv_t w5 = nf_local_pending_want(&base_rec, 1u, 1u, 0u, 1u, hole, 2,
                                            NF_INTEREST);
        MOQ_TEST_CHECK(!w5.valid);
        MOQ_TEST_CHECK_EQ_SIZE(w5.pfx_bytes_len, (size_t)0);

        /* An incomparable declaration is refused AS SUCH. The declaration
         * used here MATCHES the live owner completely -- same parts, same
         * bytes -- and differs only in `valid`, so nothing but the validity
         * guard can reject it. */
        {
            nf_fx_t f;
            if (nf_fx_setup(&f, 0, 0, 0) == 0) {
                static const char *const kLive[2] = { NF_PREFIX, NF_PREFIX2 };
                nf_inv_t free_rec;
                memset(&free_rec, 0, sizeof(free_rec));
                free_rec.valid = 1;
                free_rec.cmp_suffix_ptr = 1;
                nf_inv_t ok_decl = nf_local_pending_want(
                    &free_rec, 1u, 1u, NF_REQUEST_ID, f.ref._v, kLive, 2,
                    NF_INTEREST);
                MOQ_TEST_CHECK(ok_decl.valid);
                nf_inv_t match = ok_decl;
                /* it really does match: adoption succeeds while valid */
                MOQ_TEST_CHECK_EQ_INT(
                    nf_adopt_prefix_addrs(f.c, f.slot, &match, "P-match"), 0);
                nf_inv_t invalid = ok_decl;      /* same topology and bytes */
                invalid.valid = 0;               /* the ONLY difference */
                nf_inv_t before_call = invalid;
                MOQ_TEST_CHECK(nf_adopt_prefix_addrs(f.c, f.slot, &invalid,
                                                     "self-quiet") > 0);
                /* and the refused declaration is left untouched */
                MOQ_TEST_CHECK(memcmp(&invalid, &before_call,
                                      sizeof(invalid)) == 0);
                MOQ_TEST_CHECK(nf_adopt_prefix_addrs(f.c, f.slot, NULL,
                                                     "self-quiet") > 0);
                MOQ_TEST_CHECK(nf_adopt_prefix_addrs(f.c, f.slot, &w2,
                                                     "self-quiet") > 0);
                /* declared lengths that disagree are refused too */
                nf_inv_t w6 = nf_local_pending_want(&base_rec, 1u, 1u, 0u, 1u,
                                                    fit, 2, NF_INTEREST);
                w6.prefix_buf_len = w6.pfx_bytes_len + 1;
                MOQ_TEST_CHECK(nf_adopt_prefix_addrs(f.c, f.slot, &w6,
                                                     "self-quiet") > 0);
                nf_fx_free(&f);
            } else {
                failures++;
            }
        }
        nf_quiet = 0;
    }

    /* The R7 per-round checker on a LIVE owner. Current product behaviour
     * never takes a partial refusal on this route (the terminal does not
     * exist yet), so the SAME helper the row calls is exercised directly:
     * a correct partial state passes, and each component -- an unrelated
     * owner field, a drain insertion, a queued action, an early retirement
     * and a wrong budget -- is NAMED on its own. */
    {
        nf_fx_t f;
        if (nf_fx_setup(&f, 0, 0, 0) == 0) {
            size_t budget_active0 = f.c->recv_payload_bytes;
            failures += nf_announce(&f, kA0, kA1, "P1 announce");
            size_t budget_active = f.c->recv_payload_bytes;
            nf_inv_t live;
            nf_inv_read(f.c, f.slot, &live);
            og_graph_t g0;
            og_capture(f.c, &g0);
            nf_drain_t d0;
            MOQ_TEST_CHECK(nf_drain_snap(f.c, &d0) == 0);
            nf_acts_t none = { 0, 0 };
            moq_ns_sub_entry_t *e = &f.c->ns_subs[f.slot];
            bool sv_fin = e->pending_fin, sv_fwd = e->forward;
            e->pending_fin = true;

            {
                /* the BUNDLED wrapper on a green partial round */
                int seen0[1] = { 0 };
                size_t sp0 = 0, acc0 = 0;
                const nf_ev_t decl0[1] = {
                    nf_ev_want(MOQ_EVENT_NAMESPACE_GONE, kA0, kA1) };
                const size_t ch0[1] = { nf_suffix_charge(kA1) };
                nf_acts_t acc_acts = { 0, 0 };
                MOQ_TEST_CHECK_EQ_INT(
                    nf_partial_round(f.c, f.slot, f.handle, decl0, 1, ch0,
                                     seen0, &sp0, &acc0, &acc_acts, f.ref._v,
                                     &live, &g0, &d0, budget_active,
                                     "P1 bundled"), 0);
                MOQ_TEST_CHECK_EQ_SIZE(acc0, 0u);
            }
            MOQ_TEST_CHECK_EQ_INT(
                nf_partial_round_ok(f.c, f.slot, &live, &g0, &d0, &none,
                                    budget_active, 0, "P1 ok"), 0);
            nf_quiet = 1;
            og_quiet = 1;   /* the graph comparator has its own gate */
            e->forward = !sv_fwd;                     /* unrelated field */
            MOQ_TEST_CHECK(nf_partial_round_ok(f.c, f.slot, &live, &g0, &d0,
                                               &none, budget_active, 0,
                                               "P1-quiet") > 0);
            e->forward = sv_fwd;
            nf_acts_t one = { 1, 0 };                 /* a close was queued */
            MOQ_TEST_CHECK(nf_partial_round_ok(f.c, f.slot, &live, &g0, &d0,
                                               &one, budget_active, 0,
                                               "P1-quiet") > 0);
            /* a wrong budget delta */
            MOQ_TEST_CHECK(nf_partial_round_ok(f.c, f.slot, &live, &g0, &d0,
                                               &none, budget_active, 1,
                                               "P1-quiet") > 0);
            /* an inserted drain ref */
            MOQ_TEST_CHECK(drain_ref_add(f.c, moq_stream_ref_from_u64(0x4242)));
            MOQ_TEST_CHECK(nf_partial_round_ok(f.c, f.slot, &live, &g0, &d0,
                                               &none, budget_active, 0,
                                               "P1-quiet") > 0);
            MOQ_TEST_CHECK(drain_ref_remove(f.c,
                                            moq_stream_ref_from_u64(0x4242)));
            /* an early retirement */
            e->pending_fin = sv_fin;
            ns_sub_free_entry(f.c, (size_t)f.slot);
            MOQ_TEST_CHECK(nf_partial_round_ok(f.c, f.slot, &live, &g0, &d0,
                                               &none, budget_active, 0,
                                               "P1-quiet") > 0);
            /* The event half of the same contract: a declared outcome is
             * accepted once, a DUPLICATE is rejected, and an undeclared
             * same-length outcome is rejected. */
            {
                const nf_ev_t decl[2] = {
                    nf_ev_want(MOQ_EVENT_NAMESPACE_GONE, kA0, kA1),
                    nf_ev_want(MOQ_EVENT_NAMESPACE_GONE, kB0, kB1),
                };
                const size_t ch[2] = { nf_suffix_charge(kA1),
                                       nf_suffix_charge(kB1) };
                int seen2[2] = { 0, 0 };
                size_t sp = 0, acc = 0;
                nf_ev_t one[1] = { decl[0] };
                MOQ_TEST_CHECK_EQ_INT(
                    nf_round_classify(one, 1, decl, 2, ch, seen2, &sp, &acc,
                                      "P1 classify"), 0);
                MOQ_TEST_CHECK_EQ_SIZE(acc, 1u);
                MOQ_TEST_CHECK_EQ_SIZE(sp, ch[0]);
                MOQ_TEST_CHECK(nf_round_classify(one, 1, decl, 2, ch, seen2,
                                                 &sp, &acc, "P1-quiet") > 0);
                nf_ev_t alien[1] = {
                    nf_ev_want(MOQ_EVENT_NAMESPACE_GONE, kA0, kX1) };
                MOQ_TEST_CHECK(nf_round_classify(alien, 1, decl, 2, ch, seen2,
                                                 &sp, &acc, "P1-quiet") > 0);
                MOQ_TEST_CHECK_EQ_SIZE(acc, 1u);
            }
            nf_quiet = 0;
            og_quiet = 0;
            MOQ_TEST_CHECK(f.c->recv_payload_bytes == budget_active0);
            nf_fx_free(&f);
        } else {
            failures++;
        }
    }

    /* Live-entry bounds probes: an out-of-range slot must be refused BEFORE
     * any pool dereference, and a corrupt owner must be incomparable without
     * reading out of bounds. Every corrupted field is restored before the
     * session is destroyed. */
    {
        nf_fx_t f;
        if (nf_fx_setup(&f, 0, 0, 0) == 0) {
            nf_inv_t probe;
            nf_quiet = 1;
            nf_inv_read(f.c, -1, &probe);
            MOQ_TEST_CHECK(!probe.valid);
            nf_inv_read(f.c, (int)f.c->ns_sub_cap, &probe);
            MOQ_TEST_CHECK(!probe.valid);
            nf_inv_read(f.c, (int)f.c->ns_sub_cap + 7, &probe);
            MOQ_TEST_CHECK(!probe.valid);

            moq_ns_sub_entry_t *e = &f.c->ns_subs[f.slot];
            size_t   sv_count = e->prefix_count;
            moq_bytes_t *sv_parts = e->prefix_parts;
            uint8_t *sv_buf = e->prefix_buf;
            size_t   sv_buf_len = e->prefix_buf_len;
            size_t   sv_recv_len = e->recv_len;
            const uint8_t *sv_p0 = sv_parts ? sv_parts[0].data : NULL;
            size_t   sv_p0_len = sv_parts ? sv_parts[0].len : 0;

            e->prefix_count = MOQ_DECODED_MAX_NAMESPACE_PARTS + 1;
            nf_inv_read(f.c, f.slot, &probe);
            MOQ_TEST_CHECK(!probe.valid);
            e->prefix_count = sv_count;

            e->prefix_parts = NULL;          /* nonzero count, NULL array */
            nf_inv_read(f.c, f.slot, &probe);
            MOQ_TEST_CHECK(!probe.valid);
            e->prefix_parts = sv_parts;

            if (sv_parts) {
                e->prefix_parts[0].data = NULL;   /* non-empty, NULL data */
                nf_inv_read(f.c, f.slot, &probe);
                MOQ_TEST_CHECK(!probe.valid);
                e->prefix_parts[0].data = sv_p0;
                e->prefix_parts[0].len = NF_INV_BYTES + 1;   /* oversized part */
                nf_inv_read(f.c, f.slot, &probe);
                MOQ_TEST_CHECK(!probe.valid);
                e->prefix_parts[0].len = sv_p0_len;
            }

            /* A non-empty part whose pointer lies OUTSIDE prefix_buf: the
             * record must be incomparable and NOTHING may be read through
             * that pointer -- proven clean under ASan, not a crash. */
            if (sv_parts && sv_p0_len > 0) {
                static uint8_t k_far[8] = { 0xAA, 0xAA, 0xAA, 0xAA,
                                            0xAA, 0xAA, 0xAA, 0xAA };
                e->prefix_parts[0].data = k_far;
                nf_inv_read(f.c, f.slot, &probe);
                MOQ_TEST_CHECK(!probe.valid);
                nf_inv_t w_far;
                nf_inv_read(f.c, f.slot, &w_far);
                MOQ_TEST_CHECK(nf_adopt_prefix_addrs(f.c, f.slot, &w_far,
                                                     "self-quiet") > 0);
                /* one-past-the-end of the real buffer is out of range too */
                e->prefix_parts[0].data = sv_buf + sv_buf_len;
                nf_inv_read(f.c, f.slot, &probe);
                MOQ_TEST_CHECK(!probe.valid);
                e->prefix_parts[0].data = sv_p0;
            }

            e->prefix_buf_len = NF_INV_BYTES + 1;            /* oversized buf */
            nf_inv_read(f.c, f.slot, &probe);
            MOQ_TEST_CHECK(!probe.valid);
            e->prefix_buf_len = sv_buf_len;

            e->recv_len = e->recv_cap + 1;      /* length past its capacity */
            nf_inv_read(f.c, f.slot, &probe);
            MOQ_TEST_CHECK(!probe.valid);
            e->recv_len = sv_recv_len;
            nf_quiet = 0;

            /* Fully restored: the record is comparable again. */
            nf_inv_read(f.c, f.slot, &probe);
            MOQ_TEST_CHECK(probe.valid);
            MOQ_TEST_CHECK(e->prefix_buf == sv_buf &&
                           e->prefix_parts == sv_parts &&
                           e->prefix_count == sv_count);
            nf_fx_free(&f);
        } else {
            failures++;
        }
    }

    /* Bounds probes: a corrupt length must make the record incomparable
     * without reading out of bounds. */
    {
        uint8_t dst[8];
        size_t used = 0;
        static const uint8_t src[4] = { 1, 2, 3, 4 };
        nf_quiet = 1;
        MOQ_TEST_CHECK(nf_inv_copy(dst, sizeof(dst), &used, src, SIZE_MAX,
                                   "self-quiet") == 0);
        used = sizeof(dst) + 1;                       /* accumulator past cap */
        MOQ_TEST_CHECK(nf_inv_copy(dst, sizeof(dst), &used, src, 1,
                                   "self-quiet") == 0);
        used = 0;
        MOQ_TEST_CHECK(nf_inv_copy(dst, sizeof(dst), &used, NULL, 1,
                                   "self-quiet") == 0);
        nf_quiet = 0;
        used = 0;
        MOQ_TEST_CHECK(nf_inv_copy(dst, sizeof(dst), &used, src, 4, "self")
                       == 1);
        MOQ_TEST_CHECK_EQ_SIZE(used, 4u);
    }
    return failures;
}

/* -- rows -------------------------------------------------------------- */

static int nf_rows(void)
{
    int failures = 0;

    /* R1 -- no-FIN control: a successful NAMESPACE leaves the suffix active
     * and the owner untouched apart from its tracker. */
    {
        nf_fx_t f;
        MOQ_TEST_CHECK_EQ_INT(nf_fx_setup(&f, 0, 0, 0), 0);
        size_t budget0 = f.c->recv_payload_bytes;
        nf_inv_t before;
        nf_inv_read(f.c, f.slot, &before);
        MOQ_TEST_CHECK(before.suffixes == NULL);   /* no tracker yet */

        /* The whole expectation is built HERE, before the operation. The
         * tracker's address is unknowable, so the transition is expressed as
         * presence; every other field is exact. */
        nf_inv_t want = before;
        want.cmp_suffix_ptr = 0;
        want.suffixes = (const void *)1;
        want.suffixes_inbound = 1;
        const size_t want_budget =
            budget0 + nf_suffix_charge(kA1);

        failures += nf_announce(&f, kA0, kA1, "R1 announce");

        nf_inv_t now;
        nf_inv_read(f.c, f.slot, &now);
        now.cmp_suffix_ptr = 0;
        failures += nf_inv_equals(&now, &want, "R1 owner");
        MOQ_TEST_CHECK(now.suffixes != NULL);
        MOQ_TEST_CHECK_EQ_SIZE(f.c->recv_payload_bytes, want_budget);
        og_graph_t g;
        og_capture(f.c, &g);
        failures += og_check_integrity(&g, "R1");
        const og_edge_spec_t edges[] = { { OG_DOM_NS_REF, f.ref._v } };
        failures += og_check_owner_edges(&g, MOQ_REQ_NAMESPACE_SUB, f.slot,
                                         edges, 1, "R1");
        nf_fx_free(&f);
    }

    /* R2 -- explicit NAMESPACE_DONE control: the derivation source for the
     * per-suffix outcome. One GONE, suffix deactivated, owner ALIVE, no
     * action, no drain. */
    {
        nf_fx_t f;
        MOQ_TEST_CHECK_EQ_INT(nf_fx_setup(&f, 0, 0, 0), 0);
        size_t budget0 = f.c->recv_payload_bytes;
        failures += nf_announce(&f, kA0, kA1, "R2 announce");
        size_t budget_active = f.c->recv_payload_bytes;
        nf_inv_t before;
        nf_inv_read(f.c, f.slot, &before);
        nf_drain_t d0;
        MOQ_TEST_CHECK(nf_drain_snap(f.c, &d0) == 0);

        uint8_t msg[160];
        size_t n = nf_encode_ns(msg, sizeof(msg), MOQ_D18_NAMESPACE_DONE,
                                kA0, kA1);
        MOQ_TEST_CHECK(n > 0);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(f.c, f.ref, msg, n, false, 0),
            (int)MOQ_OK);
        nf_ev_t got[NF_EV_MAX]; size_t k = 0;
        failures += nf_collect(f.c, f.handle, got, NF_EV_MAX, &k, "R2");
        MOQ_TEST_CHECK_EQ_SIZE(k, 1u);
        if (k == 1) {
            nf_ev_t want = nf_ev_want(MOQ_EVENT_NAMESPACE_GONE, kA0, kA1);
            failures += nf_ev_equals(&got[0], &want, "R2 gone");
        }
        nf_acts_t acts = { 0, 0 };
        nf_take_actions(f.c, f.ref._v, &acts);
        failures += nf_acts_equals(&acts, 0, "R2");
        failures += nf_inv_check(f.c, f.slot, &before, "R2 owner");
        nf_drain_t d1;
        MOQ_TEST_CHECK(nf_drain_snap(f.c, &d1) == 0);
        failures += nf_drain_equals(&d1, &d0, "R2");
        /* Deactivating a suffix returns exactly its charged key bytes. The
         * ordered-tree membership charges only key bytes (its per-node overhead
         * is bounded by the suffix cap, not the byte budget), so removing the
         * only tracked suffix returns the budget fully to its pre-announce
         * value. */
        MOQ_TEST_CHECK(f.c->recv_payload_bytes < budget_active);
        MOQ_TEST_CHECK_EQ_SIZE(f.c->recv_payload_bytes, budget0);

        /* Membership probe in the negative direction: the suffix is gone, so
         * a repeat NAMESPACE_DONE is the §10.18 ordering violation. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(f.c, f.ref, msg, n, false, 0),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(f.c),
                              (int)MOQ_SESS_CLOSED);
        nf_fx_free(&f);
    }

    /* R3 -- same-call FIN with two active namespaces. The message result is
     * ordered before the terminal; the per-suffix outcomes are a multiset. */
    {
        nf_fx_t f;
        MOQ_TEST_CHECK_EQ_INT(nf_fx_setup(&f, 0, 0, 0), 0);
        size_t budget0 = f.c->recv_payload_bytes;
        failures += nf_announce(&f, kA0, kA1, "R3 announce A");
        nf_drain_t d0;
        MOQ_TEST_CHECK(nf_drain_snap(f.c, &d0) == 0);
        MOQ_TEST_CHECK(f.c->ns_subs[f.slot].state == MOQ_NS_SUB_ESTABLISHED);
        MOQ_TEST_CHECK(f.c->ns_subs[f.slot].got_response);
        MOQ_TEST_CHECK(moq_index_find(f.c->idx_ns_by_ref, f.c->idx_ns_mask,
                                      f.ref._v) == f.slot);

        uint8_t msg[160];
        size_t n = nf_encode_ns(msg, sizeof(msg), MOQ_D18_NAMESPACE, kB0, kB1);
        MOQ_TEST_CHECK(n > 0);
        nf_inv_t live;
        nf_inv_read(f.c, f.slot, &live);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(f.c, f.ref, msg, n, true, 0),
            (int)MOQ_OK);
        nf_ev_t got[NF_EV_MAX]; size_t k = 0;
        failures += nf_collect(f.c, f.handle, got, NF_EV_MAX, &k, "R3");
        /* The message commits before the terminal runs, so the declared free
         * record derives from the state the terminal actually saw: the same
         * owner with the tracker the second NAMESPACE established. */
        live.suffixes = f.c->ns_subs[f.slot].announced_suffixes;
        MOQ_TEST_CHECK_EQ_SIZE(k, 3u);
        if (k == 3) {
            nf_ev_t found = nf_ev_want(MOQ_EVENT_NAMESPACE_FOUND, kB0, kB1);
            failures += nf_ev_equals(&got[0], &found, "R3 found");
            nf_ev_t want[2] = {
                nf_ev_want(MOQ_EVENT_NAMESPACE_GONE, kA0, kA1),
                nf_ev_want(MOQ_EVENT_NAMESPACE_GONE, kB0, kB1),
            };
            failures += nf_multiset(&got[1], 2, want, 2, "R3 terminal");
        }
        nf_acts_t acts = { 0, 0 };
        nf_take_actions(f.c, f.ref._v, &acts);
        failures += nf_acts_equals(&acts, 1, "R3");
        nf_drain_t d1;
        MOQ_TEST_CHECK(nf_drain_snap(f.c, &d1) == 0);
        failures += nf_drain_equals(&d1, &d0, "R3");
        failures += nf_check_retired(f.c, f.slot, f.ref._v, &live, budget0,
                                     NULL, "R3");
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(f.c),
                              (int)MOQ_SESS_ESTABLISHED);
        nf_fx_free(&f);
    }

    /* R4 -- split empty FIN, then idempotence against the FIRST retired
     * record. */
    {
        nf_fx_t f;
        MOQ_TEST_CHECK_EQ_INT(nf_fx_setup(&f, 0, 0, 0), 0);
        size_t budget0 = f.c->recv_payload_bytes;
        failures += nf_announce(&f, kA0, kA1, "R4 announce");
        MOQ_TEST_CHECK_EQ_SIZE(f.c->ns_subs[f.slot].recv_len, 0u);
        nf_drain_t d0;
        MOQ_TEST_CHECK(nf_drain_snap(f.c, &d0) == 0);
        MOQ_TEST_CHECK(f.c->ns_subs[f.slot].state == MOQ_NS_SUB_ESTABLISHED);
        MOQ_TEST_CHECK(moq_index_find(f.c->idx_ns_by_ref, f.c->idx_ns_mask,
                                      f.ref._v) == f.slot);

        nf_inv_t live;
        nf_inv_read(f.c, f.slot, &live);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(f.c, f.ref, NULL, 0, true, 0),
            (int)MOQ_OK);
        nf_ev_t got[NF_EV_MAX]; size_t k = 0;
        failures += nf_collect(f.c, f.handle, got, NF_EV_MAX, &k, "R4");
        nf_ev_t want[1] = { nf_ev_want(MOQ_EVENT_NAMESPACE_GONE, kA0, kA1) };
        failures += nf_multiset(got, k, want, 1, "R4 terminal");
        nf_acts_t acts = { 0, 0 };
        nf_take_actions(f.c, f.ref._v, &acts);
        failures += nf_acts_equals(&acts, 1, "R4");
        nf_drain_t d1;
        MOQ_TEST_CHECK(nf_drain_snap(f.c, &d1) == 0);
        failures += nf_drain_equals(&d1, &d0, "R4");
        nf_inv_t first;
        failures += nf_check_retired(f.c, f.slot, f.ref._v, &live, budget0,
                                     &first, "R4");

        /* A repeated empty non-FIN re-feed on the retired ref is a no-op.
         * A repeated FIN is not a legal input: the bridge never re-delivers
         * one, and on this profile an unknown ref with an empty FIN is an
         * incomplete request. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(f.c, f.ref, NULL, 0, false,
                                                  0), (int)MOQ_OK);
        k = 0;
        failures += nf_collect(f.c, f.handle, got, NF_EV_MAX, &k,
                               "R4 idempotent");
        MOQ_TEST_CHECK_EQ_SIZE(k, 0u);
        nf_acts_t acts2 = { 0, 0 };
        nf_take_actions(f.c, f.ref._v, &acts2);
        failures += nf_acts_equals(&acts2, 0, "R4 idempotent");
        failures += nf_inv_check(f.c, f.slot, &first, "R4 idempotent");
        /* the complete retirement, not just the owner record */
        failures += nf_check_retired(f.c, f.slot, f.ref._v, &live, budget0,
                                     NULL, "R4 idempotent");
        nf_drain_t d2;
        MOQ_TEST_CHECK(nf_drain_snap(f.c, &d2) == 0);
        failures += nf_drain_equals(&d2, &d0, "R4 idempotent");
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(f.c),
                              (int)MOQ_SESS_ESTABLISHED);
        nf_fx_free(&f);
    }

    /* R5 -- event-full PURE refusal, then retry. With no slot at all the
     * refusal cannot make partial progress, which is what makes whole-session
     * conservation the right oracle. The blocker is this route's own
     * unpolled NAMESPACE_FOUND: no second owner, no session-state change. */
    {
        nf_fx_t f;
        MOQ_TEST_CHECK_EQ_INT(nf_fx_setup(&f, 1, 1, 0), 0);
        size_t budget0 = f.c->recv_payload_bytes;
        uint8_t msg[160];
        size_t n = nf_encode_ns(msg, sizeof(msg), MOQ_D18_NAMESPACE, kA0, kA1);
        MOQ_TEST_CHECK(n > 0);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(f.c, f.ref, msg, n, false, 0),
            (int)MOQ_OK);
        MOQ_TEST_CHECK(event_queue_full(f.c));

        nf_inv_t before;
        nf_inv_read(f.c, f.slot, &before);
        txs_snapshot_t s0;
        txs_capture(f.c, &f.ref, 1, &s0);
        nf_drain_t d0;
        MOQ_TEST_CHECK(nf_drain_snap(f.c, &d0) == 0);
        og_graph_t g0;
        og_capture(f.c, &g0);
        failures += og_check_integrity(&g0, "R5 arm");

        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(f.c, f.ref, NULL, 0, true, 0),
            (int)MOQ_ERR_WOULD_BLOCK);

        nf_inv_t live = before;
        nf_inv_t want_blocked = before;
        want_blocked.pending_fin = 1;   /* the one declared carrier mutation */
        failures += nf_inv_check(f.c, f.slot, &want_blocked, "R5 blocked");
        failures += txs_check_eq(f.c, &f.ref, 1, &s0, "R5 blocked");
        nf_drain_t d1;
        MOQ_TEST_CHECK(nf_drain_snap(f.c, &d1) == 0);
        failures += nf_drain_equals(&d1, &d0, "R5 blocked");
        og_graph_t g1;
        og_capture(f.c, &g1);
        failures += og_check_same_topology(&g0, &g1, "R5 blocked");
        nf_acts_t noacts = { 0, 0 };
        nf_take_actions(f.c, f.ref._v, &noacts);
        failures += nf_acts_equals(&noacts, 0, "R5 blocked");

        /* Release ONLY the blocker. */
        nf_ev_t got[NF_EV_MAX]; size_t k = 0;
        failures += nf_collect(f.c, f.handle, got, NF_EV_MAX, &k, "R5 blocker");
        MOQ_TEST_CHECK_EQ_SIZE(k, 1u);
        if (k == 1) {
            nf_ev_t want = nf_ev_want(MOQ_EVENT_NAMESPACE_FOUND, kA0, kA1);
            failures += nf_ev_equals(&got[0], &want, "R5 blocker");
        }
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(f.c, f.ref, NULL, 0, false,
                                                  0), (int)MOQ_OK);
        k = 0;
        failures += nf_collect(f.c, f.handle, got, NF_EV_MAX, &k, "R5 retry");
        nf_ev_t want1[1] = { nf_ev_want(MOQ_EVENT_NAMESPACE_GONE, kA0, kA1) };
        failures += nf_multiset(got, k, want1, 1, "R5 retry");
        nf_acts_t acts = { 0, 0 };
        nf_take_actions(f.c, f.ref._v, &acts);
        failures += nf_acts_equals(&acts, 1, "R5 retry");
        nf_drain_t d2;
        MOQ_TEST_CHECK(nf_drain_snap(f.c, &d2) == 0);
        failures += nf_drain_equals(&d2, &d0, "R5 retry");
        nf_inv_t first;
        failures += nf_check_retired(f.c, f.slot, f.ref._v, &live, budget0,
                                     &first, "R5 retry");

        /* Slot reuse on a one-slot pool: only a real retirement frees it.
         * Everything is DERIVED before the call -- the same free-slot record
         * the retirement produced, the next generation and packed handle, the
         * next stream ref from the session's own counter, and the next
         * request id as a declared constant (draft-18 client ids are even and
         * advance by two, and the first was asserted to be NF_REQUEST_ID).
         * The reused owner is compared with the SAME declaration the green
         * first request already validates in nf_fx_setup. */
        static const char *const kPfx2Parts[1] = { "live2" };
        const uint32_t want_gen2 = first.generation | 1u;
        const uint64_t want_h2 = moq_handle_pack(MOQ_HANDLE_POOL_NAMESPACE_SUB,
                                                 f.c->session_tag, want_gen2,
                                                 (uint32_t)f.slot);
        const uint64_t want_rid2 = NF_REQUEST_ID + 2u;
        const uint64_t want_ref2 = f.c->next_stream_ref;
        const size_t   send_len_before = f.c->send_len;
        MOQ_TEST_CHECK(want_h2 != 0);
        MOQ_TEST_CHECK(want_h2 != f.handle);
        MOQ_TEST_CHECK(want_ref2 != f.ref._v);

        nf_inv_t want2 = nf_local_pending_want(&first, want_gen2, want_h2,
                                               want_rid2, want_ref2,
                                               kPfx2Parts, 1, NF_INTEREST);

        moq_subscribe_namespace_cfg_t nc;
        moq_subscribe_namespace_cfg_init(&nc);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live2") };
        nc.track_namespace_prefix = (moq_namespace_t){ parts, 1 };
        nc.namespace_interest = NF_INTEREST;
        moq_ns_sub_handle_t h2 = MOQ_NS_SUB_HANDLE_INVALID;   /* never read
                                                               * uninitialized
                                                               * after a
                                                               * failing call */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_subscribe_namespace(f.c, &nc, 0, &h2),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(h2._opaque, want_h2);

        /* Exactly one OPEN_BIDI_STREAM, on the PREDECLARED ref, decoded. */
        size_t open_len = 0;
        {
            moq_action_t a;
            int opens = 0, other = 0;
            while (moq_session_poll_actions(f.c, &a, 1) > 0) {
                if (a.kind == MOQ_ACTION_OPEN_BIDI_STREAM) {
                    opens++;
                    MOQ_TEST_CHECK_EQ_U64(
                        a.u.open_bidi_stream.stream_ref._v, want_ref2);
                    open_len = a.u.open_bidi_stream.len;
                    MOQ_TEST_CHECK(!a.u.open_bidi_stream.fin);
                    moq_control_envelope_t env;
                    moq_buf_reader_t r;
                    moq_buf_reader_init(&r, a.u.open_bidi_stream.data,
                                        a.u.open_bidi_stream.len);
                    MOQ_TEST_CHECK(moq_d18_decode_envelope(&r, &env) == MOQ_OK);
                    MOQ_TEST_CHECK_EQ_U64(env.msg_type,
                                          MOQ_D18_SUBSCRIBE_NAMESPACE);
                    MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&r), 0u);
                    moq_bytes_t dp[MOQ_DECODED_MAX_NAMESPACE_PARTS];
                    moq_d18_subscribe_namespace_t sn;
                    MOQ_TEST_CHECK(moq_d18_decode_subscribe_namespace(
                        env.payload, env.payload_len, dp,
                        MOQ_DECODED_MAX_NAMESPACE_PARTS, &sn) == MOQ_OK);
                    MOQ_TEST_CHECK_EQ_U64(sn.request_id, want_rid2);
                    MOQ_TEST_CHECK_EQ_SIZE(
                        sn.track_namespace_prefix.count, 1u);
                    failures += txs_check_part_bytes(
                        &sn.track_namespace_prefix, 0, "live2", 5,
                        "R5 reuse prefix");
                    MOQ_TEST_CHECK(sn.params.auth_token_count == 0 &&
                                   !sn.params.has_forward &&
                                   !sn.params.has_subscriber_priority &&
                                   !sn.params.has_filter);
                } else {
                    other++;
                }
                moq_action_cleanup(&a);
            }
            MOQ_TEST_CHECK_EQ_INT(opens, 1);
            MOQ_TEST_CHECK_EQ_INT(other, 0);
        }
        MOQ_TEST_CHECK(nf_expect_no_events(f.c, "R5 reuse") == 0);
        MOQ_TEST_CHECK_EQ_SIZE(f.c->send_len, send_len_before + open_len);

        int adopt2 = nf_adopt_prefix_addrs(f.c, f.slot, &want2,
                                           "R5 reuse owner");
        failures += adopt2;
        if (adopt2 == 0)
            failures += nf_inv_check(f.c, f.slot, &want2, "R5 reuse owner");

        /* Both declared edges: by-request-id (session_namespace_sub.c:663)
         * and namespace-ref (:664), with the retired ref absent. */
        og_graph_t g_reuse;
        og_capture(f.c, &g_reuse);
        failures += og_check_integrity(&g_reuse, "R5 reuse");
        {
            const og_edge_spec_t e2[] = {
                { OG_DOM_NS_REF,  want_ref2 },
                { OG_DOM_REQ_RID, want_rid2 },
            };
            failures += og_check_owner_edges(&g_reuse, MOQ_REQ_NAMESPACE_SUB,
                                             f.slot, e2, 2, "R5 reuse");
            failures += og_check_no_edge(&g_reuse, OG_DOM_NS_REF, f.ref._v,
                                         "R5 reuse");
        }
        nf_drain_t d_reuse;
        MOQ_TEST_CHECK(nf_drain_snap(f.c, &d_reuse) == 0);
        const size_t budget_reuse = f.c->recv_payload_bytes;
        /* The row classified every action above, so the ring is empty and the
         * bytes still in the send buffer belong to an already-polled action.
         * Making that premise explicit is what licenses the send-cursor oracle
         * below. */
        MOQ_TEST_CHECK(f.c->action_head == f.c->action_tail);

        /* The retired handle is refused AND touches nothing: the whole reused
         * owner, its edges, the drain set, the send cursor, the budget and
         * the session state are all unchanged across the refusal. */
        {
            moq_request_goaway_cfg_t gc;
            moq_request_goaway_cfg_init(&gc);
            moq_ns_sub_handle_t stale = { f.handle };
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_session_request_goaway_ns_sub(f.c, stale, &gc, 0),
                (int)MOQ_ERR_STALE_HANDLE);
            failures += nf_inv_check(f.c, f.slot, &want2, "R5 stale handle");
            og_graph_t g_stale;
            og_capture(f.c, &g_stale);
            failures += og_check_same_topology(&g_reuse, &g_stale,
                                               "R5 stale handle");
            nf_drain_t d_stale;
            MOQ_TEST_CHECK(nf_drain_snap(f.c, &d_stale) == 0);
            failures += nf_drain_equals(&d_stale, &d_reuse, "R5 stale handle");
            /* Scratch reclamation is mandatory per PHYSICAL call, not a
             * property of this route: with the action ring empty,
             * session_call_prepare() (session.c:95-110) must release the
             * polled action's residue, so the only honest expectation here is
             * zero. Zero together with this block's no-action oracle is what
             * proves the refused call appended no output of its own. */
            MOQ_TEST_CHECK_EQ_SIZE(f.c->send_len, 0u);
            MOQ_TEST_CHECK_EQ_SIZE(f.c->recv_payload_bytes, budget_reuse);
            MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(f.c),
                                  (int)MOQ_SESS_ESTABLISHED);
            MOQ_TEST_CHECK(nf_expect_no_events(f.c, "R5 stale handle") == 0);
            MOQ_TEST_CHECK(nf_expect_no_actions(f.c, "R5 stale handle") == 0);
        }
        nf_fx_free(&f);
    }

    /* R6 -- same-call message+FIN with ONE event slot. The message result may
     * enqueue and fill the queue; the terminal must then refuse with the
     * bytes consumed, the new suffix tracked and the FIN retained. Declared
     * partial-progress delta: recv_len 0 (bytes consumed), a tracker present,
     * pending_fin set -- and NOTHING else. */
    {
        nf_fx_t f;
        MOQ_TEST_CHECK_EQ_INT(nf_fx_setup(&f, 1, 1, 0), 0);
        size_t budget0 = f.c->recv_payload_bytes;
        nf_inv_t before;
        nf_inv_read(f.c, f.slot, &before);
        nf_drain_t d0;
        MOQ_TEST_CHECK(nf_drain_snap(f.c, &d0) == 0);
        MOQ_TEST_CHECK(!event_queue_full(f.c));

        uint8_t msg[160];
        size_t n = nf_encode_ns(msg, sizeof(msg), MOQ_D18_NAMESPACE, kA0, kA1);
        MOQ_TEST_CHECK(n > 0);

        /* The ENTIRE expectation is built here, BEFORE ingress: the FIN
         * carrier is latched, the bytes are consumed, one non-NULL inbound
         * tracker exists (presence only -- its address cannot authorize
         * itself), and nothing else moves. The budget is an absolute value,
         * with checked addition, not a subtraction from the observation. */
        nf_inv_t want = before;
        want.pending_fin = 1;
        want.cmp_suffix_ptr = 0;
        want.suffixes = (const void *)1;
        want.suffixes_inbound = 1;
        want.recv_len = 0;
        want.recv_bytes_len = 0;
        const size_t charge = nf_suffix_charge(kA1);
        MOQ_TEST_CHECK(charge <= SIZE_MAX - budget0);
        const size_t budget_want = budget0 + charge;

        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(f.c, f.ref, msg, n, true, 0),
            (int)MOQ_ERR_WOULD_BLOCK);

        nf_inv_t now;
        nf_inv_read(f.c, f.slot, &now);
        now.cmp_suffix_ptr = 0;
        failures += nf_inv_equals(&now, &want, "R6 blocked");
        MOQ_TEST_CHECK(now.suffixes != NULL);
        MOQ_TEST_CHECK_EQ_SIZE(f.c->recv_payload_bytes, budget_want);
        /* Only now, both absolute checks having passed, is the tracker
         * identity adopted as the retry-conservation baseline. */
        nf_inv_t live = now;
        live.cmp_suffix_ptr = 1;
        live.suffixes = f.c->ns_subs[f.slot].announced_suffixes;
        nf_acts_t noacts = { 0, 0 };
        nf_take_actions(f.c, f.ref._v, &noacts);
        failures += nf_acts_equals(&noacts, 0, "R6 blocked");
        nf_drain_t d1;
        MOQ_TEST_CHECK(nf_drain_snap(f.c, &d1) == 0);
        failures += nf_drain_equals(&d1, &d0, "R6 blocked");
        MOQ_TEST_CHECK(f.c->ns_subs[f.slot].state == MOQ_NS_SUB_ESTABLISHED);

        /* Poll ONLY the message result, then empty-refeed. */
        nf_ev_t got[NF_EV_MAX]; size_t k = 0;
        failures += nf_collect(f.c, f.handle, got, NF_EV_MAX, &k, "R6 blocker");
        MOQ_TEST_CHECK_EQ_SIZE(k, 1u);
        if (k == 1) {
            nf_ev_t w = nf_ev_want(MOQ_EVENT_NAMESPACE_FOUND, kA0, kA1);
            failures += nf_ev_equals(&got[0], &w, "R6 blocker");
        }
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(f.c, f.ref, NULL, 0, false,
                                                  0), (int)MOQ_OK);
        k = 0;
        failures += nf_collect(f.c, f.handle, got, NF_EV_MAX, &k, "R6 retry");
        nf_ev_t want1[1] = { nf_ev_want(MOQ_EVENT_NAMESPACE_GONE, kA0, kA1) };
        failures += nf_multiset(got, k, want1, 1, "R6 retry");
        nf_acts_t acts = { 0, 0 };
        nf_take_actions(f.c, f.ref._v, &acts);
        failures += nf_acts_equals(&acts, 1, "R6 retry");
        failures += nf_check_retired(f.c, f.slot, f.ref._v, &live, budget0,
                                     NULL, "R6 retry");
        nf_fx_free(&f);
    }

    /* R7 -- multi-suffix cursor: three active namespaces, two event slots.
     * Per-suffix progress may commit, so the oracle is per ROUND, not only at
     * the end: after every partial refusal the owner must still be live with
     * its FIN carrier latched and its validated tracker identity, the graph
     * and the whole drain multiset unchanged, no close action and no
     * retirement yet, and the receive budget reduced by EXACTLY the canonical
     * key lengths of the outcomes emitted so far. */
    {
        nf_fx_t f;
        MOQ_TEST_CHECK_EQ_INT(nf_fx_setup(&f, 2, 1, 0), 0);
        size_t budget0 = f.c->recv_payload_bytes;
        failures += nf_announce(&f, kP0, kP1, "R7 announce A");
        failures += nf_announce(&f, kQ0, kQ1, "R7 announce B");
        failures += nf_announce(&f, kS0, kS1, "R7 announce C");
        nf_drain_t d0;
        MOQ_TEST_CHECK(nf_drain_snap(f.c, &d0) == 0);
        og_graph_t g0;
        og_capture(f.c, &g0);
        failures += og_check_integrity(&g0, "R7 arm");
        nf_inv_t live;
        nf_inv_read(f.c, f.slot, &live);
        size_t budget_active = f.c->recv_payload_bytes;

        const nf_ev_t want3[3] = {
            nf_ev_want(MOQ_EVENT_NAMESPACE_GONE, kP0, kP1),
            nf_ev_want(MOQ_EVENT_NAMESPACE_GONE, kQ0, kQ1),
            nf_ev_want(MOQ_EVENT_NAMESPACE_GONE, kS0, kS1),
        };
        const size_t charge[3] = { nf_suffix_charge(kP1),
                                   nf_suffix_charge(kQ1),
                                   nf_suffix_charge(kS1) };
        int seen[3] = { 0, 0, 0 };
        size_t spent = 0;
        size_t total_seen = 0;
        nf_acts_t acts = { 0, 0 };
        size_t rounds = 0;

        moq_result_t rc = moq_session_on_bidi_stream_bytes(f.c, f.ref, NULL, 0,
                                                           true, 0);
        for (;;) {
            if (rc == MOQ_ERR_WOULD_BLOCK) {
                /* Every blocked round goes through the ONE bundled contract:
                 * classification and state together, never one alone. */
                failures += nf_partial_round(f.c, f.slot, f.handle, want3, 3,
                                             charge, seen, &spent, &total_seen,
                                             &acts, f.ref._v, &live, &g0, &d0,
                                             budget_active, "R7 partial");
                if (++rounds > 4) {
                    fprintf(stderr, "NF R7: the terminal did not converge in"
                            " %zu retries\n", rounds);
                    failures++;
                    break;
                }
                rc = moq_session_on_bidi_stream_bytes(f.c, f.ref, NULL, 0,
                                                      false, 0);
                continue;
            }
            /* The final round: the owner is expected to RETIRE, so only the
             * classification member applies here. */
            {
                nf_ev_t got[NF_EV_MAX]; size_t k = 0;
                failures += nf_collect(f.c, f.handle, got, NF_EV_MAX, &k,
                                       "R7 final");
                failures += nf_round_classify(got, k, want3, 3, charge, seen,
                                              &spent, &total_seen, "R7 final");
                nf_take_actions(f.c, f.ref._v, &acts);
            }
            break;
        }
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_SIZE(total_seen, 3u);
        MOQ_TEST_CHECK(seen[0] && seen[1] && seen[2]);
        failures += nf_acts_equals(&acts, 1, "R7");
        nf_drain_t d1;
        MOQ_TEST_CHECK(nf_drain_snap(f.c, &d1) == 0);
        failures += nf_drain_equals(&d1, &d0, "R7");
        failures += nf_check_retired(f.c, f.slot, f.ref._v, &live, budget0,
                                     NULL, "R7");
        nf_fx_free(&f);
    }

    /* R8 -- action-full final step. DIRECT queue_close_bidi pressure on a
     * distinct blocker ref fills the only action slot, so the terminal's own
     * close-half cannot be queued. No second owner and no extra graph state is
     * introduced: the blocker is one action, nothing more. The FIN must refuse
     * before the final outcome and before retirement -- the physical bidi may
     * never close while outcomes are still owed. */
    {
        nf_fx_t f;
        MOQ_TEST_CHECK_EQ_INT(nf_fx_setup(&f, 0, 0, 1), 0);
        size_t budget0 = f.c->recv_payload_bytes;
        failures += nf_announce(&f, kA0, kA1, "R8 announce");

        const moq_stream_ref_t blocker = moq_stream_ref_from_u64(0x7777);
        MOQ_TEST_CHECK(blocker._v != f.ref._v);
        MOQ_TEST_CHECK(queue_close_bidi(f.c, blocker) == MOQ_OK);
        MOQ_TEST_CHECK(action_queue_full(f.c));
        /* The queued action is exactly the blocker's close, and the identical
         * action must still be there after the refused FIN. */
        MOQ_TEST_CHECK(nf_head_action_is_close(f.c, blocker._v));

        nf_inv_t before;
        nf_inv_read(f.c, f.slot, &before);
        txs_snapshot_t s0;
        txs_capture(f.c, &f.ref, 1, &s0);
        nf_expect_after_call_prepare(&s0);
        nf_drain_t d0;
        MOQ_TEST_CHECK(nf_drain_snap(f.c, &d0) == 0);
        og_graph_t g0;
        og_capture(f.c, &g0);
        failures += og_check_integrity(&g0, "R8 arm");

        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(f.c, f.ref, NULL, 0, true, 0),
            (int)MOQ_ERR_WOULD_BLOCK);
        nf_inv_t want = before;
        want.pending_fin = 1;
        nf_inv_t live = want;
        failures += nf_inv_check(f.c, f.slot, &want, "R8 blocked");
        failures += txs_check_eq(f.c, &f.ref, 1, &s0, "R8 blocked");
        nf_drain_t d1;
        MOQ_TEST_CHECK(nf_drain_snap(f.c, &d1) == 0);
        failures += nf_drain_equals(&d1, &d0, "R8 blocked");
        og_graph_t g1;
        og_capture(f.c, &g1);
        failures += og_check_same_topology(&g0, &g1, "R8 blocked");
        MOQ_TEST_CHECK(nf_expect_no_events(f.c, "R8 blocked") == 0);
        /* The identical blocker action is still queued, unconsumed. */
        MOQ_TEST_CHECK(nf_head_action_is_close(f.c, blocker._v));

        /* Drain ONLY the blocker, then reassert conservation before retrying. */
        {
            moq_action_t a;
            size_t drained = 0;
            while (moq_session_poll_actions(f.c, &a, 1) > 0) {
                if (a.kind == MOQ_ACTION_CLOSE_BIDI_STREAM &&
                    a.u.close_bidi_stream.stream_ref._v == blocker._v)
                    drained++;
                else
                    failures++;
                moq_action_cleanup(&a);
            }
            MOQ_TEST_CHECK_EQ_SIZE(drained, 1u);
        }
        failures += nf_inv_check(f.c, f.slot, &want, "R8 released");
        og_graph_t g2;
        og_capture(f.c, &g2);
        failures += og_check_same_topology(&g0, &g2, "R8 released");
        nf_drain_t d2;
        MOQ_TEST_CHECK(nf_drain_snap(f.c, &d2) == 0);
        failures += nf_drain_equals(&d2, &d0, "R8 released");

        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(f.c, f.ref, NULL, 0, false,
                                                  0), (int)MOQ_OK);
        nf_ev_t got[NF_EV_MAX]; size_t k = 0;
        failures += nf_collect(f.c, f.handle, got, NF_EV_MAX, &k, "R8 retry");
        nf_ev_t want1[1] = { nf_ev_want(MOQ_EVENT_NAMESPACE_GONE, kA0, kA1) };
        failures += nf_multiset(got, k, want1, 1, "R8 retry");
        /* The target close is exactly this request bidi's, not the blocker's. */
        nf_acts_t acts = { 0, 0 };
        nf_take_actions(f.c, f.ref._v, &acts);
        failures += nf_acts_equals(&acts, 1, "R8 retry");
        nf_drain_t d3;
        MOQ_TEST_CHECK(nf_drain_snap(f.c, &d3) == 0);
        failures += nf_drain_equals(&d3, &d0, "R8 retry");
        failures += nf_check_retired(f.c, f.slot, f.ref._v, &live, budget0,
                                     NULL, "R8 retry");
        nf_fx_free(&f);
    }

    /* R9 -- full drain ring. FIN completion owes no drain, so an exhausted
     * ring must not block it. The ring is filled with synthetic refs that do
     * not touch this owner. */
    {
        nf_fx_t f;
        MOQ_TEST_CHECK_EQ_INT(nf_fx_setup(&f, 0, 0, 0), 0);
        size_t budget0 = f.c->recv_payload_bytes;
        failures += nf_announce(&f, kA0, kA1, "R9 announce");
        size_t filled = 0;
        for (uint64_t r = 0x9000; f.c->drain_ref_count < f.c->drain_ref_cap;
             r++) {
            if (!drain_ref_add(f.c, moq_stream_ref_from_u64(r))) break;
            filled++;
        }
        MOQ_TEST_CHECK(filled > 0);
        MOQ_TEST_CHECK(f.c->drain_ref_count == f.c->drain_ref_cap);
        nf_drain_t d0;
        MOQ_TEST_CHECK(nf_drain_snap(f.c, &d0) == 0);
        MOQ_TEST_CHECK(d0.count == f.c->drain_ref_cap);
        nf_inv_t live;
        nf_inv_read(f.c, f.slot, &live);

        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(f.c, f.ref, NULL, 0, true, 0),
            (int)MOQ_OK);
        nf_ev_t got[NF_EV_MAX]; size_t k = 0;
        failures += nf_collect(f.c, f.handle, got, NF_EV_MAX, &k, "R9");
        nf_ev_t want1[1] = { nf_ev_want(MOQ_EVENT_NAMESPACE_GONE, kA0, kA1) };
        failures += nf_multiset(got, k, want1, 1, "R9 terminal");
        nf_acts_t acts = { 0, 0 };
        nf_take_actions(f.c, f.ref._v, &acts);
        failures += nf_acts_equals(&acts, 1, "R9");
        /* The WHOLE ring, entry for entry and reason for reason. */
        nf_drain_t d1;
        MOQ_TEST_CHECK(nf_drain_snap(f.c, &d1) == 0);
        failures += nf_drain_equals(&d1, &d0, "R9");
        MOQ_TEST_CHECK(!drain_ref_contains(f.c, f.ref));
        failures += nf_check_retired(f.c, f.slot, f.ref._v, &live, budget0,
                                     NULL, "R9");
        nf_fx_free(&f);
    }

    /* R10 -- RESET with capacity available and two active namespaces: the
     * same per-suffix outcomes and the same retirement, but NO local
     * close-half action (the reset ingress owns physical teardown) and no
     * drain. */
    {
        nf_fx_t f;
        MOQ_TEST_CHECK_EQ_INT(nf_fx_setup(&f, 0, 0, 0), 0);
        size_t budget0 = f.c->recv_payload_bytes;
        failures += nf_announce(&f, kA0, kA1, "R10 announce A");
        failures += nf_announce(&f, kB0, kB1, "R10 announce B");
        nf_drain_t d0;
        MOQ_TEST_CHECK(nf_drain_snap(f.c, &d0) == 0);

        nf_inv_t live;
        nf_inv_read(f.c, f.slot, &live);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_reset(f.c, f.ref, NF_RESET_CODE, 0),
            (int)MOQ_OK);
        nf_ev_t got[NF_EV_MAX]; size_t k = 0;
        failures += nf_collect(f.c, f.handle, got, NF_EV_MAX, &k, "R10");
        nf_ev_t want2[2] = {
            nf_ev_want(MOQ_EVENT_NAMESPACE_GONE, kA0, kA1),
            nf_ev_want(MOQ_EVENT_NAMESPACE_GONE, kB0, kB1),
        };
        failures += nf_multiset(got, k, want2, 2, "R10 terminal");
        nf_acts_t acts = { 0, 0 };
        nf_take_actions(f.c, f.ref._v, &acts);
        failures += nf_acts_equals(&acts, 0, "R10");
        nf_drain_t d1;
        MOQ_TEST_CHECK(nf_drain_snap(f.c, &d1) == 0);
        failures += nf_drain_equals(&d1, &d0, "R10");
        failures += nf_check_retired(f.c, f.slot, f.ref._v, &live, budget0,
                                     NULL, "R10");
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(f.c),
                              (int)MOQ_SESS_ESTABLISHED);
        nf_fx_free(&f);
    }

    /* R11 -- RESET blocked on event capacity, retried through the SAME reset
     * call (a reset has no empty-re-feed carrier of its own). */
    {
        nf_fx_t f;
        MOQ_TEST_CHECK_EQ_INT(nf_fx_setup(&f, 1, 1, 0), 0);
        size_t budget0 = f.c->recv_payload_bytes;
        uint8_t msg[160];
        size_t n = nf_encode_ns(msg, sizeof(msg), MOQ_D18_NAMESPACE, kA0, kA1);
        MOQ_TEST_CHECK(n > 0);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(f.c, f.ref, msg, n, false, 0),
            (int)MOQ_OK);
        MOQ_TEST_CHECK(event_queue_full(f.c));

        nf_inv_t before;
        nf_inv_read(f.c, f.slot, &before);
        txs_snapshot_t s0;
        txs_capture(f.c, &f.ref, 1, &s0);
        nf_drain_t d0;
        MOQ_TEST_CHECK(nf_drain_snap(f.c, &d0) == 0);
        og_graph_t g0;
        og_capture(f.c, &g0);
        failures += og_check_integrity(&g0, "R11 arm");

        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_reset(f.c, f.ref, NF_RESET_CODE, 0),
            (int)MOQ_ERR_WOULD_BLOCK);
        /* A reset carries no per-owner latch of its own, so nothing at all
         * may change on a refusal before progress. */
        nf_inv_t live = before;
        failures += nf_inv_check(f.c, f.slot, &before, "R11 blocked");
        failures += txs_check_eq(f.c, &f.ref, 1, &s0, "R11 blocked");
        og_graph_t g1;
        og_capture(f.c, &g1);
        failures += og_check_same_topology(&g0, &g1, "R11 blocked");
        nf_drain_t d1;
        MOQ_TEST_CHECK(nf_drain_snap(f.c, &d1) == 0);
        failures += nf_drain_equals(&d1, &d0, "R11 blocked");
        nf_acts_t noacts = { 0, 0 };
        nf_take_actions(f.c, f.ref._v, &noacts);
        failures += nf_acts_equals(&noacts, 0, "R11 blocked");

        nf_ev_t got[NF_EV_MAX]; size_t k = 0;
        failures += nf_collect(f.c, f.handle, got, NF_EV_MAX, &k,
                               "R11 blocker");
        MOQ_TEST_CHECK_EQ_SIZE(k, 1u);
        if (k == 1) {
            nf_ev_t w = nf_ev_want(MOQ_EVENT_NAMESPACE_FOUND, kA0, kA1);
            failures += nf_ev_equals(&got[0], &w, "R11 blocker");
        }
        /* Only the blocker moved: reassert before repeating the reset. */
        failures += nf_inv_check(f.c, f.slot, &before, "R11 released");
        og_graph_t g2;
        og_capture(f.c, &g2);
        failures += og_check_same_topology(&g0, &g2, "R11 released");
        nf_drain_t d2;
        MOQ_TEST_CHECK(nf_drain_snap(f.c, &d2) == 0);
        failures += nf_drain_equals(&d2, &d0, "R11 released");
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_reset(f.c, f.ref, NF_RESET_CODE, 0),
            (int)MOQ_OK);
        k = 0;
        failures += nf_collect(f.c, f.handle, got, NF_EV_MAX, &k, "R11 retry");
        nf_ev_t want1[1] = { nf_ev_want(MOQ_EVENT_NAMESPACE_GONE, kA0, kA1) };
        failures += nf_multiset(got, k, want1, 1, "R11 retry");
        nf_acts_t acts = { 0, 0 };
        nf_take_actions(f.c, f.ref._v, &acts);
        failures += nf_acts_equals(&acts, 0, "R11 retry");
        nf_drain_t d3;
        MOQ_TEST_CHECK(nf_drain_snap(f.c, &d3) == 0);
        failures += nf_drain_equals(&d3, &d0, "R11 retry");
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(f.c),
                              (int)MOQ_SESS_ESTABLISHED);
        failures += nf_check_retired(f.c, f.slot, f.ref._v, &live, budget0,
                                     NULL, "R11 retry");
        nf_fx_free(&f);
    }

    /* R12 -- STOP_SENDING negative control. It is a DISTINCT signal and must
     * NOT be widened into the §10.18 synthesis: it retires the owner today
     * and must keep doing exactly that, with no per-suffix outcome. This row
     * is green now and must stay green after the correction. */
    {
        nf_fx_t f;
        MOQ_TEST_CHECK_EQ_INT(nf_fx_setup(&f, 0, 0, 0), 0);
        size_t budget0 = f.c->recv_payload_bytes;
        failures += nf_announce(&f, kA0, kA1, "R12 announce");
        nf_drain_t d0;
        MOQ_TEST_CHECK(nf_drain_snap(f.c, &d0) == 0);

        nf_inv_t live;
        nf_inv_read(f.c, f.slot, &live);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_stop(f.c, f.ref, NF_RESET_CODE, 0),
            (int)MOQ_OK);
        MOQ_TEST_CHECK(nf_expect_no_events(f.c, "R12") == 0);
        nf_acts_t acts = { 0, 0 };
        nf_take_actions(f.c, f.ref._v, &acts);
        failures += nf_acts_equals(&acts, 0, "R12");
        nf_drain_t d1;
        MOQ_TEST_CHECK(nf_drain_snap(f.c, &d1) == 0);
        failures += nf_drain_equals(&d1, &d0, "R12");
        failures += nf_check_retired(f.c, f.slot, f.ref._v, &live, budget0,
                                     NULL, "R12");
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(f.c),
                              (int)MOQ_SESS_ESTABLISHED);
        nf_fx_free(&f);
    }

    /* R13 -- FIN before any response. §10.18 applies to the response stream,
     * and §11.4.1 makes termination of the request bidi terminate the
     * SUBSCRIBE_NAMESPACE request, so a FIN on a still-pending subscription is
     * the zero-active-suffix case of the same terminal: no outcome exists to
     * synthesize, and no wire error was received, so there is nothing truthful
     * to report -- exactly one close and exactly one retirement. */
    {
        nf_fx_t f;
        MOQ_TEST_CHECK_EQ_INT(nf_fx_setup_pending(&f, 0, 0, 0), 0);
        size_t budget0 = f.c->recv_payload_bytes;
        nf_drain_t d0;
        MOQ_TEST_CHECK(nf_drain_snap(f.c, &d0) == 0);
        MOQ_TEST_CHECK(f.c->ns_subs[f.slot].state ==
                       MOQ_NS_SUB_PENDING_SUBSCRIBER);
        MOQ_TEST_CHECK(!f.c->ns_subs[f.slot].got_response);
        MOQ_TEST_CHECK(f.c->ns_subs[f.slot].announced_suffixes == NULL);

        nf_inv_t live;
        nf_inv_read(f.c, f.slot, &live);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(f.c, f.ref, NULL, 0, true, 0),
            (int)MOQ_OK);
        MOQ_TEST_CHECK(nf_expect_no_events(f.c, "R13") == 0);
        nf_acts_t acts = { 0, 0 };
        nf_take_actions(f.c, f.ref._v, &acts);
        failures += nf_acts_equals(&acts, 1, "R13");
        nf_drain_t d1;
        MOQ_TEST_CHECK(nf_drain_snap(f.c, &d1) == 0);
        failures += nf_drain_equals(&d1, &d0, "R13");
        nf_inv_t first;
        failures += nf_check_retired(f.c, f.slot, f.ref._v, &live, budget0,
                                     &first, "R13");
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(f.c),
                              (int)MOQ_SESS_ESTABLISHED);
        /* The retirement invalidated the public handle the request produced. */
        {
            moq_request_goaway_cfg_t gc;
            moq_request_goaway_cfg_init(&gc);
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_session_request_goaway_ns_sub(f.c, f.h, &gc, 0),
                (int)MOQ_ERR_STALE_HANDLE);
        }

        /* An empty non-FIN re-feed on the retired ref is inert. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(f.c, f.ref, NULL, 0, false,
                                                  0), (int)MOQ_OK);
        MOQ_TEST_CHECK(nf_expect_no_events(f.c, "R13 idempotent") == 0);
        nf_acts_t acts2 = { 0, 0 };
        nf_take_actions(f.c, f.ref._v, &acts2);
        failures += nf_acts_equals(&acts2, 0, "R13 idempotent");
        failures += nf_inv_check(f.c, f.slot, &first, "R13 idempotent");
        failures += nf_check_retired(f.c, f.slot, f.ref._v, &live, budget0,
                                     NULL, "R13 idempotent");
        nf_drain_t d2;
        MOQ_TEST_CHECK(nf_drain_snap(f.c, &d2) == 0);
        failures += nf_drain_equals(&d2, &d0, "R13 idempotent");
        nf_fx_free(&f);
    }

    return failures;
}

int main(void)
{
    int failures = 0;
    failures += nf_self_checks();
    failures += nf_rows();
    MOQ_TEST_PASS("d18_ns_response_fin");
    return failures != 0;
}
