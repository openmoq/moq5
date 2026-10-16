/*
 * Contract coverage: a Joining FETCH associated with a PUBLISH-initiated
 * subscription, on both endpoints and both draft profiles.
 *
 * SPEC BASIS (draft-ietf-moq-transport-18):
 *
 *   5.1 -- "A subscription can be initiated and moved to the Pending state by
 *   either a publisher or a subscriber. A publisher initiates a subscription to
 *   a track by sending the PUBLISH message." Both origins converge on ONE
 *   Established state in the same state machine.
 *
 *   5.1 -- "A publisher MUST save the Largest Location communicated in
 *   SUBSCRIBE_OK, PUBLISH or REQUEST_UPDATE_OK that changes the Forward State
 *   from 0 to 1. This value is called the Joining Location and can be used in a
 *   Joining FETCH (see Section 10.12.2) while the subscription is in the
 *   Established state." PUBLISH is named explicitly, and the saved value is a
 *   LATCH -- not a live read of track history.
 *
 *   10.12.2 -- "A Joining Fetch is associated with a Subscribe request by
 *   specifying the Request ID of a subscription in the Established or Pending
 *   (subscriber) state." The normative object is "a subscription"; 5.1 makes a
 *   PUBLISH-initiated one exactly that once PUBLISH_OK arrives. Pending
 *   (Publisher) is deliberately NOT in that list.
 *
 *   10.12.2.1 -- End Location is {Joining Location.Group,
 *   Joining Location.Object + 1}; a Relative Joining Fetch sets Start to
 *   {Joining Location.Group - Joining Start, 0}.
 *
 * PROFILE BOUNDARY. The two drafts differ, and the rows keep them apart:
 *
 *   draft-18 gates a join on Forward State 1 (10.12.2, INVALID_RANGE
 *   otherwise) and supplies a NEW Joining Location on a REQUEST_UPDATE_OK
 *   that raises Forward 0 -> 1.
 *
 *   draft-16 5.1 saves the Largest communicated in PUBLISH or SUBSCRIBE_OK
 *   WHEN ESTABLISHING the subscription, and 9.16.2 makes a non-LARGEST_OBJECT
 *   filter a protocol violation rather than a request error. It states no
 *   update-driven transition rule, so a draft-16 Joining Location must NOT
 *   move when a later update or history advance occurs.
 *
 * MSF-01 5 requires catalog subscribers to use SUBSCRIBE with a Joining FETCH
 * (offset = 0). A PUBLISH-initiated join does NOT satisfy that MUST -- it is a
 * different initiator. It is covered here because draft-18 requires the
 * capability on its own terms, not as an MSF substitute.
 *
 * What each row freezes is stated at the row. Nothing casts or puns an opaque
 * handle: the publication is named as a publication.
 */
#include <moq/sim.h>
#include <moq/publisher.h>
#include <moq/control_d18.h>
#include <moq/control.h>
#include <moq/wire.h>
#include "test_support.h"
#include "../../core/src/session/session_internal.h"

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <stddef.h>

static int failures = 0;

/* MOQ_TEST_PASS is gated on the GLOBAL failure count, which any failing row
 * trips -- so each row reports its OWN delta. That is what keeps a passing
 * control visible next to a failing row. */
#define ROW_BEGIN(name) const char *row__ = (name); int mark__ = failures
#define ROW_END() \
    printf("%s: %s (%d diagnostic%s)\n", \
           failures == mark__ ? "ROW OK  " : "ROW FAIL", row__, \
           failures - mark__, (failures - mark__) == 1 ? "" : "s")

/* ---- helpers -------------------------------------------------------- */

static moq_simpair_t *make_pair(moq_version_t ver)
{
    moq_simpair_cfg_t scfg = MOQ_SIMPAIR_CFG_INIT;
    scfg.alloc = moq_alloc_default();
    scfg.version = ver;
    moq_simpair_t *sp = NULL;
    if (moq_simpair_create(&scfg, &sp) < 0) return NULL;
    if (moq_simpair_start(sp) < 0) { moq_simpair_destroy(sp); return NULL; }
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    return sp;
}

/* Publisher-side request id of the sole PUBLISHER-role publication. */
static bool publish_request_id(moq_session_t *s, uint64_t *out)
{
    int found = 0;
    for (size_t i = 0; i < s->pub_cap; i++) {
        const moq_pub_entry_t *e = &s->publishes[i];
        if (e->state != MOQ_PUB_FREE && e->role == MOQ_PUB_ROLE_PUBLISHER) {
            *out = e->request_id;
            found++;
        }
    }
    return found == 1;
}

/* Count subscription-pool entries in any non-free state. */
static int live_subs(moq_session_t *s)
{
    int n = 0;
    for (size_t i = 0; i < s->sub_cap; i++)
        if (s->subs[i].state != MOQ_SUB_FREE) n++;
    return n;
}

static size_t encode_join_fetch(uint8_t *buf, size_t cap, uint64_t req_id,
                                uint64_t join_req_id, uint64_t ft,
                                uint64_t jstart)
{
    moq_d18_fetch_t f;
    memset(&f, 0, sizeof(f));
    f.request_id = req_id;
    f.fetch_type = ft;
    f.joining_request_id = join_req_id;
    f.joining_start = jstart;
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    if (moq_d18_encode_fetch(&w, &f) != MOQ_OK) return 0;
    return moq_buf_writer_offset(&w);
}

/* True iff a REQUEST_ERROR with `code` was queued on `ref`. */
/* ---- one complete wire inventory per feed ---------------------------- *
 * A "did I see code X" probe that drains the queue cannot be asked a second
 * question: the follow-up sees an empty queue and passes vacuously. Worse, it
 * says nothing about foreign or duplicate output. So every request-stream row
 * takes ONE capture and classifies EVERY action in it, then asserts the whole
 * declared set against that single pass. */
#define WI_MAX_CODES 8
typedef struct {
    int      actions;            /* every action, whatever its kind */
    int      on_ref;             /* SEND_BIDI_STREAM on the row's ref */
    int      foreign;            /* any action that is neither of those */
    int      fin_count;          /* sends on the ref carrying FIN */
    int      req_errors;         /* fully decoded REQUEST_ERROR bodies */
    int      undecodable;        /* on-ref sends that did not fully decode */
    int      trailing;           /* decoded, but with bytes left over */
    int      other_msg;          /* on-ref control message of another type */
    int      code_count;
    uint64_t codes[WI_MAX_CODES];
    int      code_hits[WI_MAX_CODES];
} wire_inv_t;

static void wi_note_code(wire_inv_t *wi, uint64_t code)
{
    for (int i = 0; i < wi->code_count; i++) {
        if (wi->codes[i] == code) { wi->code_hits[i]++; return; }
    }
    if (wi->code_count < WI_MAX_CODES) {
        wi->codes[wi->code_count] = code;
        wi->code_hits[wi->code_count] = 1;
        wi->code_count++;
    }
}

static int wi_hits(const wire_inv_t *wi, uint64_t code)
{
    for (int i = 0; i < wi->code_count; i++)
        if (wi->codes[i] == code) return wi->code_hits[i];
    return 0;
}

/* Drain the action queue ONCE, classifying everything. */
static void wire_inventory(moq_session_t *s, moq_stream_ref_t ref,
                           wire_inv_t *wi)
{
    memset(wi, 0, sizeof(*wi));
    moq_action_t act;
    while (moq_session_poll_actions(s, &act, 1) > 0) {
        wi->actions++;
        if (act.kind == MOQ_ACTION_SEND_BIDI_STREAM &&
            act.u.send_bidi_stream.stream_ref._v == ref._v) {
            wi->on_ref++;
            if (act.u.send_bidi_stream.fin) wi->fin_count++;
            moq_buf_reader_t rr;
            moq_buf_reader_init(&rr, act.u.send_bidi_stream.data,
                                act.u.send_bidi_stream.len);
            moq_control_envelope_t env;
            if (moq_d18_decode_envelope(&rr, &env) != MOQ_OK) {
                wi->undecodable++;
            } else if (env.msg_type != MOQ_D18_REQUEST_ERROR) {
                wi->other_msg++;
            } else {
                moq_d18_request_error_t er;
                if (moq_d18_decode_request_error(env.payload, env.payload_len,
                                                 &er) != MOQ_OK) {
                    wi->undecodable++;
                } else {
                    wi->req_errors++;
                    wi_note_code(wi, er.error_code);
                    /* The envelope must account for the whole send: a
                     * same-length body with a junk tail would otherwise pass
                     * the code check. */
                    if (moq_buf_reader_remaining(&rr) != 0) wi->trailing++;
                }
            }
        } else {
            wi->foreign++;
        }
        moq_action_cleanup(&act);
    }
}

/* draft-16 answers on the CONTROL stream, not a request bidi, so its declared
 * set is a different shape: exactly one SEND_CONTROL carrying one cleanly
 * decoded REQUEST_ERROR with `want` and no occurrence of `forbid`. Same
 * one-capture discipline. */
static void expect_sole_control_request_error(moq_session_t *s, uint64_t want,
                                              uint64_t forbid,
                                              const char *what)
{
    int actions = 0, on_ctrl = 0, foreign = 0, errs = 0;
    int undecodable = 0, trailing = 0, other_msg = 0;
    int want_hits = 0, forbid_hits = 0;
    uint64_t seen[WI_MAX_CODES]; int seen_n = 0;
    moq_action_t act;
    while (moq_session_poll_actions(s, &act, 1) > 0) {
        actions++;
        if (act.kind == MOQ_ACTION_SEND_CONTROL) {
            on_ctrl++;
            moq_buf_reader_t rr;
            moq_buf_reader_init(&rr, act.u.send_control.data,
                                act.u.send_control.len);
            moq_control_envelope_t env;
            if (moq_control_decode_envelope(&rr, &env) != MOQ_OK) {
                undecodable++;
            } else if (env.msg_type != MOQ_D16_REQUEST_ERROR) {
                other_msg++;
            } else {
                moq_d16_request_error_t er;
                if (moq_d16_decode_request_error(env.payload, env.payload_len,
                                                 &er) != MOQ_OK) {
                    undecodable++;
                } else {
                    errs++;
                    if (er.error_code == want) want_hits++;
                    if (er.error_code == forbid) forbid_hits++;
                    if (seen_n < WI_MAX_CODES) seen[seen_n++] = er.error_code;
                    if (moq_buf_reader_remaining(&rr) != 0) trailing++;
                }
            }
        } else {
            foreign++;
        }
        moq_action_cleanup(&act);
    }
    if (actions != 1 || on_ctrl != 1 || foreign != 0 || errs != 1 ||
        undecodable != 0 || trailing != 0 || other_msg != 0 ||
        want_hits != 1 || forbid_hits != 0) {
        printf("FAIL: %s: actions=%d on_ctrl=%d foreign=%d errs=%d "
               "undecodable=%d trailing=%d other_msg=%d want(0x%llx)=%d "
               "forbid(0x%llx)=%d\n",
               what, actions, on_ctrl, foreign, errs, undecodable, trailing,
               other_msg, (unsigned long long)want, want_hits,
               (unsigned long long)forbid, forbid_hits);
        for (int i = 0; i < seen_n; i++)
            printf("      saw code 0x%llx\n", (unsigned long long)seen[i]);
        failures++;
    }
}

/* The declared set for an ACCEPTED request-stream feed: the session surfaces
 * the request to the application and answers nothing on the wire yet, so the
 * whole capture must be empty. Asserting "no REQUEST_ERROR" alone would let a
 * stray or duplicate action through. */
static void expect_no_wire_output(moq_session_t *s, moq_stream_ref_t ref,
                                  const char *what)
{
    wire_inv_t wi;
    wire_inventory(s, ref, &wi);
    if (wi.actions != 0) {
        printf("FAIL: %s: expected no wire output, got actions=%d on_ref=%d "
               "foreign=%d req_errors=%d\n",
               what, wi.actions, wi.on_ref, wi.foreign, wi.req_errors);
        for (int i = 0; i < wi.code_count; i++)
            printf("      saw code 0x%llx x%d\n",
                   (unsigned long long)wi.codes[i], wi.code_hits[i]);
        failures++;
    }
}

/* The declared set for a refused request-stream feed: exactly one action, a
 * send on the row's own ref, one cleanly decoded REQUEST_ERROR carrying
 * `want`, nothing trailing, nothing foreign, and NO occurrence of `forbid`
 * -- all judged against the SAME capture. */
static void expect_sole_request_error(moq_session_t *s, moq_stream_ref_t ref,
                                      uint64_t want, uint64_t forbid,
                                      const char *what)
{
    wire_inv_t wi;
    wire_inventory(s, ref, &wi);
    if (wi.actions != 1 || wi.on_ref != 1 || wi.foreign != 0 ||
        wi.req_errors != 1 || wi.undecodable != 0 || wi.trailing != 0 ||
        wi.other_msg != 0 || wi_hits(&wi, want) != 1 ||
        wi_hits(&wi, forbid) != 0) {
        printf("FAIL: %s: actions=%d on_ref=%d foreign=%d req_errors=%d "
               "undecodable=%d trailing=%d other_msg=%d want(0x%llx)=%d "
               "forbid(0x%llx)=%d\n",
               what, wi.actions, wi.on_ref, wi.foreign, wi.req_errors,
               wi.undecodable, wi.trailing, wi.other_msg,
               (unsigned long long)want, wi_hits(&wi, want),
               (unsigned long long)forbid, wi_hits(&wi, forbid));
        for (int i = 0; i < wi.code_count; i++)
            printf("      saw code 0x%llx x%d\n",
                   (unsigned long long)wi.codes[i], wi.code_hits[i]);
        failures++;
    }
}

/* ---- fixture: an ESTABLISHED PUBLISH-initiated subscription --------- *
 * Publisher on the server (facade, so the retained group merges into the
 * track history and the PUBLISH carries LARGEST_OBJECT); consumer on the
 * client. Mirrors the push-mode catalog bootstrap. */
typedef struct {
    moq_simpair_t     *sp;
    moq_publisher_t   *pub;
    moq_pub_track_t   *track;
    moq_publication_t  cons_pub;    /* consumer-side handle */
    bool               cons_has_lg;
    uint64_t           cons_lg_group, cons_lg_object;
} fix_t;

/* opts: the effective accept state the consumer answers PUBLISH_OK with. */
typedef struct {
    bool                    forward;
    bool                    set_filter;
    moq_subscribe_filter_t  filter;
} accept_opts_t;

static bool fixture_up_opt(fix_t *f, moq_version_t ver, accept_opts_t ao);

static bool fixture_up(fix_t *f, moq_version_t ver)
{
    accept_opts_t ao = { .forward = true, .set_filter = false };
    return fixture_up_opt(f, ver, ao);
}

static bool fixture_up_opt(fix_t *f, moq_version_t ver, accept_opts_t ao)
{
    memset(f, 0, sizeof(*f));
    f->sp = make_pair(ver);
    if (!f->sp) return false;
    moq_session_t *sv = moq_simpair_server(f->sp);
    moq_session_t *cl = moq_simpair_client(f->sp);

    /* The publisher lives on the server, so the CLIENT must grant it request
     * capacity before it can send PUBLISH. draft-16 starts at zero, so without
     * this the publish is REQUEST_BLOCKED (-10) and the fixture would look
     * like a missing profile capability rather than an ungranted budget. */
    (void)moq_session_grant_request_capacity(cl, 32, moq_simpair_now_us(f->sp));
    /* And the server grants the client, so the consumer can issue its own
     * requests (the Joining FETCH). draft-16 starts both directions at zero. */
    (void)moq_session_grant_request_capacity(sv, 32, moq_simpair_now_us(f->sp));
    moq_simpair_run_until_quiescent(f->sp, 16, NULL);

    moq_pub_cfg_t pc;
    moq_pub_cfg_init_sized(&pc, sizeof(pc));
    pc.accept_mode = MOQ_PUB_ACCEPT_ALL;
    if (moq_pub_create(sv, moq_alloc_default(), &pc, &f->pub) != MOQ_OK)
        return false;

    moq_pub_track_cfg_t tc;
    moq_pub_track_cfg_init(&tc);
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("live") };
    tc.track_namespace = (moq_namespace_t){ ns, 1 };
    tc.track_name = MOQ_BYTES_LITERAL("catalog");
    if (moq_pub_add_track(f->pub, &tc, moq_simpair_now_us(f->sp), &f->track)
        != MOQ_OK)
        return false;

    /* Retained group 0 object 0: the catalog a Joining FETCH(offset 0) must
     * return, and the Largest the PUBLISH must advertise. */
    uint8_t body[] = { 'c', 'a', 't' };
    moq_rcbuf_t *pay = NULL;
    if (moq_rcbuf_create(moq_alloc_default(), body, sizeof(body), &pay)
        != MOQ_OK)
        return false;
    moq_pub_retained_object_t ro;
    memset(&ro, 0, sizeof(ro));
    ro.object_id = 0;
    ro.payload = pay;
    moq_pub_retained_group_cfg_t rg;
    moq_pub_retained_group_cfg_init(&rg);
    rg.group_id = 0;
    rg.objects = &ro;
    rg.object_count = 1;
    moq_result_t rrc = moq_pub_set_retained_group(f->pub, f->track, &rg);
    moq_rcbuf_decref(pay);
    if (rrc != MOQ_OK) return false;

    moq_pub_publish_cfg_t pp;
    moq_pub_publish_cfg_init(&pp);
    if (moq_pub_publish_track(f->pub, f->track, &pp,
                              moq_simpair_now_us(f->sp)) != MOQ_OK)
        return false;
    moq_simpair_run_until_quiescent(f->sp, 16, NULL);

    /* Consumer accepts, capturing the advertised Largest first. */
    bool got = false;
    moq_event_t ev;
    while (moq_session_poll_events(cl, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_PUBLISH_REQUEST) {
            f->cons_pub = ev.u.publish_request.pub;
            f->cons_has_lg = ev.u.publish_request.has_largest;
            f->cons_lg_group = ev.u.publish_request.largest_group;
            f->cons_lg_object = ev.u.publish_request.largest_object;
            got = true;
        }
        moq_event_cleanup(&ev);
    }
    if (!got) return false;

    moq_accept_publish_cfg_t ac;
    moq_accept_publish_cfg_init_sized(&ac, sizeof(ac));
    ac.has_forward = true;
    ac.forward = ao.forward;
    if (ao.set_filter) {
        ac.filter = ao.filter;              /* draft-16 9.2.2.5 */
        ac.start_group = 0;
        ac.start_object = 0;
        ac.end_group = 0;
    }
    if (moq_session_accept_publish(cl, f->cons_pub, &ac,
                                   moq_simpair_now_us(f->sp)) != MOQ_OK)
        return false;
    moq_simpair_run_until_quiescent(f->sp, 16, NULL);
    (void)moq_pub_tick(f->pub, moq_simpair_now_us(f->sp));
    return true;
}

static void fixture_down(fix_t *f)
{
    if (f->pub) moq_pub_destroy(f->pub);
    if (f->sp) {
        moq_simpair_run_until_quiescent(f->sp, 8, NULL);
        moq_simpair_destroy(f->sp);
    }
}

/* Find the sole entry of a role in a session's publication pool. */
static moq_pub_entry_t *sole_pub_entry(moq_session_t *s, int role)
{
    moq_pub_entry_t *found = NULL;
    for (size_t i = 0; i < s->pub_cap; i++) {
        moq_pub_entry_t *pe = &s->publishes[i];
        if (pe->state == MOQ_PUB_FREE) continue;
        if ((int)pe->role != role) continue;
        if (found) return NULL;          /* ambiguous: the caller must know */
        found = pe;
    }
    return found;
}

/* Send a publication REQUEST_UPDATE from the consumer and settle it. */
static bool update_publication_forward(fix_t *f, bool forward)
{
    moq_publication_update_cfg_t uc;
    moq_publication_update_cfg_init(&uc);
    uc.has_forward = true;
    uc.forward = forward;
    if (moq_session_update_publication(moq_simpair_client(f->sp), f->cons_pub,
                                       &uc, moq_simpair_now_us(f->sp)) != MOQ_OK)
        return false;
    moq_simpair_run_until_quiescent(f->sp, 16, NULL);
    (void)moq_pub_tick(f->pub, moq_simpair_now_us(f->sp));
    moq_simpair_run_until_quiescent(f->sp, 16, NULL);
    return true;
}

/* Drain the consumer's events, returning the ACK's Largest. */
static int take_update_ok(fix_t *f, bool *has_lg, uint64_t *g, uint64_t *o)
{
    int n = 0;
    moq_event_t ev;
    while (moq_session_poll_events(moq_simpair_client(f->sp), &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_PUBLICATION_UPDATE_OK) {
            n++;
            *has_lg = ev.u.publication_update_ok.has_largest;
            *g = ev.u.publication_update_ok.largest_group;
            *o = ev.u.publication_update_ok.largest_object;
        }
        moq_event_cleanup(&ev);
    }
    return n;
}


/* ==================================================================== *
 * Consumer side: the peer that accepted a PUBLISH names that publication
 * directly (joining_pub) and receives the retained catalog.
 * ==================================================================== */
static void consumer_joins_publication(void)
{
    ROW_BEGIN("consumer originates a PUBLISH-joined FETCH and gets the catalog");
    fix_t f;
    if (!fixture_up(&f, MOQ_VERSION_DRAFT_18)) {
        MOQ_TEST_CHECK(false);
        fixture_down(&f);
        ROW_END();
        return;
    }
    moq_session_t *cl = moq_simpair_client(f.sp);

    /* Preconditions -- the spec-level subscription really is there. */
    MOQ_TEST_CHECK(f.cons_has_lg);                       /* 10.2.11 */
    MOQ_TEST_CHECK_EQ_U64(f.cons_lg_group, 0);           /* Joining Location */
    MOQ_TEST_CHECK_EQ_U64(f.cons_lg_object, 0);
    MOQ_TEST_CHECK(moq_publication_is_valid(f.cons_pub));

    /* The subscription 5.1 says PUBLISH created is modelled as a publication,
     * not a moq_subscription_t: the consumer's subscription pool is empty. It
     * is named with joining_pub, no handle cast anywhere. */
    MOQ_TEST_CHECK_EQ_INT(live_subs(cl), 0);

    moq_fetch_cfg_t fc;
    moq_fetch_cfg_init_sized(&fc, sizeof(fc));   /* required to reach joining_pub */
    fc.is_joining = true;
    fc.joining_relative = true;
    fc.joining_start = 0;                        /* offset 0 */
    fc.joining_pub = f.cons_pub;
    moq_fetch_t fh = MOQ_FETCH_INVALID;
    moq_result_t rc =
        moq_session_fetch(cl, &fc, moq_simpair_now_us(f.sp), &fh);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
    MOQ_TEST_CHECK(moq_fetch_is_valid(fh));

    /* Discriminator rules, each checked before anything is mutated. */
    {
        moq_fetch_cfg_t bad;
        moq_fetch_t bh = MOQ_FETCH_INVALID;
        /* (the BOTH case needs two genuinely valid handles and is checked in
         * the control row, which holds a real subscription as well as the
         * publication; a malformed handle is not a name and correctly reads as
         * absent here) */
        /* neither */
        moq_fetch_cfg_init_sized(&bad, sizeof(bad));
        bad.is_joining = true; bad.joining_relative = true;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_fetch(cl, &bad, moq_simpair_now_us(f.sp), &bh),
            (int)MOQ_ERR_INVAL);
        /* a standalone fetch may name no owner */
        moq_fetch_cfg_init_sized(&bad, sizeof(bad));
        bad.joining_pub = f.cons_pub;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_fetch(cl, &bad, moq_simpair_now_us(f.sp), &bh),
            (int)MOQ_ERR_INVAL);
        /* A malformed joining_pub is NAMED, not absent: the resolver answers
         * STALE_HANDLE, matching the long-standing joining_sub behaviour. */
        moq_fetch_cfg_init_sized(&bad, sizeof(bad));
        bad.is_joining = true; bad.joining_relative = true;
        bad.joining_pub = (moq_publication_t){ 0xDEADBEEF };
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_fetch(cl, &bad, moq_simpair_now_us(f.sp), &bh),
            (int)MOQ_ERR_STALE_HANDLE);
        /* the pointer-only init cannot reach joining_pub: its struct_size
         * stops at the frozen v0 floor, so the field reads as absent and the
         * call is "neither". */
        moq_fetch_cfg_t oldcfg;
        moq_fetch_cfg_init(&oldcfg);
        MOQ_TEST_CHECK_EQ_U64(oldcfg.struct_size, MOQ_FETCH_CFG_V0_SIZE);
        oldcfg.is_joining = true; oldcfg.joining_relative = true;
        oldcfg.joining_pub = f.cons_pub;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_fetch(cl, &oldcfg, moq_simpair_now_us(f.sp), &bh),
            (int)MOQ_ERR_INVAL);
    }

    /* Drive it to completion: the publisher facade must locate the retained
     * group through the PUBLICATION owner and serve objects 0..N. */
    moq_simpair_run_until_quiescent(f.sp, 16, NULL);
    (void)moq_pub_tick(f.pub, moq_simpair_now_us(f.sp));
    moq_simpair_run_until_quiescent(f.sp, 16, NULL);

    int n_ok = 0, n_obj = 0, n_done = 0, n_err = 0;
    uint64_t ok_eg = 0, ok_eo = 0, og = 0, oo = 0;
    uint8_t body[64]; size_t body_len = 0;
    moq_event_t ev;
    while (moq_session_poll_events(cl, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_FETCH_OK &&
            moq_fetch_eq(ev.u.fetch_ok.fetch, fh)) {
            n_ok++; ok_eg = ev.u.fetch_ok.end_group; ok_eo = ev.u.fetch_ok.end_object;
        } else if (ev.kind == MOQ_EVENT_FETCH_OBJECT &&
                   moq_fetch_eq(ev.u.fetch_object.fetch, fh)) {
            n_obj++; og = ev.u.fetch_object.group_id; oo = ev.u.fetch_object.object_id;
            if (ev.u.fetch_object.payload) {
                size_t n = moq_rcbuf_len(ev.u.fetch_object.payload);
                if (n && n <= sizeof(body)) {
                    memcpy(body, moq_rcbuf_data(ev.u.fetch_object.payload), n);
                    body_len = n;
                }
            }
        } else if (ev.kind == MOQ_EVENT_FETCH_COMPLETE &&
                   moq_fetch_eq(ev.u.fetch_complete.fetch, fh)) {
            n_done++;
        } else if (ev.kind == MOQ_EVENT_FETCH_ERROR &&
                   moq_fetch_eq(ev.u.fetch_error.fetch, fh)) {
            n_err++;
        }
        moq_event_cleanup(&ev);
    }
    /* Exact correlation and NO duplicates. */
    MOQ_TEST_CHECK_EQ_INT(n_err, 0);
    MOQ_TEST_CHECK_EQ_INT(n_ok, 1);
    MOQ_TEST_CHECK_EQ_INT(n_obj, 1);
    MOQ_TEST_CHECK_EQ_INT(n_done, 1);
    /* 10.12.2.1: End is {Joining Location.Group, Joining Location.Object + 1}. */
    MOQ_TEST_CHECK_EQ_U64(ok_eg, f.cons_lg_group);
    MOQ_TEST_CHECK_EQ_U64(ok_eo, f.cons_lg_object + 1);
    /* Retained object identity and payload, byte for byte. */
    MOQ_TEST_CHECK_EQ_U64(og, 0);
    MOQ_TEST_CHECK_EQ_U64(oo, 0);
    MOQ_TEST_CHECK_EQ_SIZE(body_len, 3);
    MOQ_TEST_CHECK(body_len == 3 && memcmp(body, "cat", 3) == 0);

    /* Retirement: the fetch handle is retired once complete, so a second
     * cancel finds nothing to cancel and produces no further output. */
    (void)moq_session_fetch_cancel(cl, fh, moq_simpair_now_us(f.sp));
    moq_simpair_run_until_quiescent(f.sp, 8, NULL);
    int extra = 0;
    while (moq_session_poll_events(cl, &ev, 1) > 0) {
        if ((ev.kind == MOQ_EVENT_FETCH_OBJECT &&
             moq_fetch_eq(ev.u.fetch_object.fetch, fh)) ||
            (ev.kind == MOQ_EVENT_FETCH_COMPLETE &&
             moq_fetch_eq(ev.u.fetch_complete.fetch, fh)))
            extra++;
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK_EQ_INT(extra, 0);

    fixture_down(&f);
    ROW_END();
}

/* ==================================================================== *
 * Publisher side: a conformant Joining FETCH whose Joining Request ID is a
 * PUBLISH request id resolves against the publication pool.
 * ==================================================================== */
static void publisher_resolves_publication_join(void)
{
    ROW_BEGIN("publisher resolves a PUBLISH Joining Request ID");
    fix_t f;
    if (!fixture_up(&f, MOQ_VERSION_DRAFT_18)) {
        MOQ_TEST_CHECK(false);
        fixture_down(&f);
        ROW_END();
        return;
    }
    moq_session_t *sv = moq_simpair_server(f.sp);   /* the publisher */

    /* The publisher holds exactly one PUBLISHER-role publication; its Request
     * ID is what 10.12.2 says a Joining Fetch names. */
    uint64_t pub_req_id = 0;
    MOQ_TEST_CHECK(publish_request_id(sv, &pub_req_id));

    /* ...and it is NOT in the subscription pool, which is where inbound
     * joining resolution used to look exclusively. */
    MOQ_TEST_CHECK_EQ_INT(live_subs(sv), 0);

    /* MOVE THE TRACK HISTORY well past the Joining Location. 5.1 latches the
     * Joining Location at the transition; it is not a live read, so objects
     * published afterwards must NOT move the range a join resolves to. */
    {
        uint8_t d2[] = { 'x' };
        moq_rcbuf_t *p2 = NULL;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_rcbuf_create(moq_alloc_default(), d2, sizeof(d2), &p2),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_pub_write_object(f.pub, f.track, 5, 7, p2,
                                      moq_simpair_now_us(f.sp)),
            (int)MOQ_OK);
        moq_rcbuf_decref(p2);
        moq_simpair_run_until_quiescent(f.sp, 16, NULL);
        /* The registry really did advance -- otherwise the check below would
         * pass vacuously. */
        for (size_t i = 0; i < sv->pub_cap; i++) {
            const moq_pub_entry_t *pe = &sv->publishes[i];
            if (pe->state == MOQ_PUB_FREE || !pe->hist) continue;
            MOQ_TEST_CHECK(pe->hist->has_largest);
            MOQ_TEST_CHECK_EQ_U64(pe->hist->largest_group, 5);
            MOQ_TEST_CHECK_EQ_U64(pe->hist->largest_object, 7);
            /* ...while the latched Joining Location stayed put. */
            MOQ_TEST_CHECK(pe->has_joining_loc);
            MOQ_TEST_CHECK_EQ_U64(pe->joining_group, 0);
            MOQ_TEST_CHECK_EQ_U64(pe->joining_object, 0);
        }
    }

    /* Feed a wire-valid Relative Joining FETCH, offset 0, naming it. */
    (void)moq_session_poll_actions(sv, NULL, 0);
    moq_stream_ref_t ref = moq_stream_ref_from_u64(0x4242);
    uint8_t msg[128];
    /* First request the consumer originates in this session: id 0 keeps the
     * inbound parity/sequence rule satisfied so the feed reaches the joining
     * resolution rather than a request-id protocol close. */
    size_t n = encode_join_fetch(msg, sizeof(msg), /*req_id*/ 0, pub_req_id,
                                 MOQ_D18_FETCH_TYPE_RELATIVE, /*jstart*/ 0);
    MOQ_TEST_CHECK(n > 0);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(sv, ref, msg, n, false,
                                              moq_simpair_now_us(f.sp)),
        (int)MOQ_OK);

    /* The Joining Request ID resolves to the PUBLISH-initiated subscription in
     * the publication pool, so the feed is ACCEPTED: nothing is answered on
     * the wire here, and the whole capture must be empty -- not merely free of
     * one forbidden code. */
    expect_no_wire_output(sv, ref, "publisher resolves publication join");

    /* CONTRACT: the Joining Request ID resolves to the Established
     * PUBLISH-initiated subscription, so no INVALID_JOINING_REQUEST_ID is
     * emitted and a FETCH_REQUEST surfaces with the range 10.12.2.1 computes
     * from the Joining Location (0,0): start {0,0}, end {0,1}. */
    bool saw_fetch_req = false;
    uint64_t sg = 0, so = 0, eg = 0, eo = 0;
    moq_event_t ev;
    while (moq_session_poll_events(sv, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_FETCH_REQUEST) {
            saw_fetch_req = true;
            sg = ev.u.fetch_request.start_group;
            so = ev.u.fetch_request.start_object;
            eg = ev.u.fetch_request.end_group;
            eo = ev.u.fetch_request.end_object;
        }
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK(saw_fetch_req);
    MOQ_TEST_CHECK_EQ_U64(sg, 0);   /* {Joining Location.Group - 0, 0} */
    MOQ_TEST_CHECK_EQ_U64(so, 0);
    MOQ_TEST_CHECK_EQ_U64(eg, 0);   /* {Joining Location.Group,           */
    MOQ_TEST_CHECK_EQ_U64(eo, 1);   /*  Joining Location.Object + 1}      */

    fixture_down(&f);
    ROW_END();
}

/* ==================================================================== *
 * CONTROL -- the same feed against a SUBSCRIBE-established subscription
 * resolves today. Isolates the defect to the PUBLISH origin rather than
 * to the harness, the encoding, or the joining machinery.
 * ==================================================================== */
static void control_subscribe_origin_resolves(void)
{
    ROW_BEGIN("CONTROL subscribe-origin joining FETCH resolves");
    fix_t f;
    if (!fixture_up(&f, MOQ_VERSION_DRAFT_18)) {
        MOQ_TEST_CHECK(false);
        fixture_down(&f);
        ROW_END();
        return;
    }
    moq_session_t *sv = moq_simpair_server(f.sp);
    moq_session_t *cl = moq_simpair_client(f.sp);

    /* A REAL subscriber-initiated subscription on the same track. */
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("live") };
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ ns, 1 };
    sc.track_name = MOQ_BYTES_LITERAL("catalog");
    sc.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;   /* 9.16.2 / 10.12.2 */
    moq_subscription_t sub = MOQ_SUBSCRIPTION_INVALID;
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_subscribe(cl, &sc, moq_simpair_now_us(f.sp), &sub),
        (int)MOQ_OK);
    moq_simpair_run_until_quiescent(f.sp, 16, NULL);
    (void)moq_pub_tick(f.pub, moq_simpair_now_us(f.sp));
    moq_simpair_run_until_quiescent(f.sp, 16, NULL);

    /* The publisher now has a subscription-pool entry -- the thing inbound
     * joining resolution can find. */
    MOQ_TEST_CHECK(live_subs(sv) >= 1);

    uint64_t sub_req_id = 0;
    bool found = false;
    for (size_t i = 0; i < sv->sub_cap; i++) {
        if (sv->subs[i].state == MOQ_SUB_ESTABLISHED &&
            sv->subs[i].role == MOQ_SUB_ROLE_PUBLISHER) {
            sub_req_id = sv->subs[i].request_id;
            found = true;
        }
    }
    MOQ_TEST_CHECK(found);

    (void)moq_session_poll_actions(sv, NULL, 0);
    moq_stream_ref_t ref = moq_stream_ref_from_u64(0x5151);
    uint8_t msg[128];
    /* The SUBSCRIBE above consumed id 0, so the next inbound id is 2. */
    size_t n = encode_join_fetch(msg, sizeof(msg), /*req_id*/ 2, sub_req_id,
                                 MOQ_D18_FETCH_TYPE_RELATIVE, 0);
    MOQ_TEST_CHECK(n > 0);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(sv, ref, msg, n, false,
                                              moq_simpair_now_us(f.sp)),
        (int)MOQ_OK);

    /* BOTH discriminators, each genuinely valid: refused before any mutation. */
    {
        moq_fetch_cfg_t bad;
        moq_fetch_cfg_init_sized(&bad, sizeof(bad));
        bad.is_joining = true;
        bad.joining_relative = true;
        bad.joining_sub = sub;          /* real, established */
        bad.joining_pub = f.cons_pub;   /* real, established */
        moq_fetch_t bh = MOQ_FETCH_INVALID;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_fetch(cl, &bad, moq_simpair_now_us(f.sp), &bh),
            (int)MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(!moq_fetch_is_valid(bh));
    }

    /* Accepted, so the whole capture is empty -- the subscribe-origin path is
     * unchanged by the publication-pool fallback. */
    expect_no_wire_output(sv, ref, "subscribe-origin join");

    fixture_down(&f);
    ROW_END();
}

/* ==================================================================== *
 * DRAFT-16 -- implementable, with its OWN eligibility rule.
 *
 * d16 is not generalized from d18; the rules differ in both directions and
 * both differences are quoted here:
 *
 *   d16 5.1 (line 1382) -- "A publisher MUST save the Largest Location
 *   communicated in PUBLISH or SUBSCRIBE_OK when establishing a subscription.
 *   This value can be used in a Joining FETCH ... at any time while the
 *   subscription is active." PUBLISH is named, with NO Forward-change
 *   qualifier -- the location is fixed AT ESTABLISHMENT and no update moves
 *   it, which the d16 update row below pins.
 *
 *   d16 9.16.2 -- "A Joining Fetch is only permitted when the associated
 *   Subscribe has the Filter Type Largest Object; any other value results in
 *   closing the session with a PROTOCOL_VIOLATION." A publication negotiates
 *   its filter on PUBLISH_OK (9.2.2.5), so that gate DOES have an answer for a
 *   PUBLISH-initiated subscription: permit the join when the established
 *   publication negotiated LARGEST_OBJECT, and follow 9.16.2's protocol-error
 *   rule otherwise. d18 replaced this gate with Forward State 1 (10.12.2).
 *
 * Both halves are asserted: the filtered join resolves, and a non-Largest
 * filter closes the session rather than answering a request error.
 * ==================================================================== */
static void d16_filtered_join(void)
{
    ROW_BEGIN("draft-16 filtered publication join resolves");
    fix_t f;
    /* draft-16 9.16.2 gates on the associated subscription having Filter Type
     * Largest Object; a publication negotiates its filter on PUBLISH_OK
     * (9.2.2.5). draft-18 has no such gate -- it uses Forward State 1 -- so
     * this eligibility rule is deliberately NOT shared between the profiles. */
    accept_opts_t ao = { .forward = true, .set_filter = true,
                         .filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT };
    if (!fixture_up_opt(&f, MOQ_VERSION_DRAFT_16, ao)) {
        MOQ_TEST_CHECK(false);
        fixture_down(&f);
        ROW_END();
        return;
    }
    moq_session_t *sv = moq_simpair_server(f.sp);

    uint64_t pub_req_id = 0;
    MOQ_TEST_CHECK(publish_request_id(sv, &pub_req_id));
    MOQ_TEST_CHECK_EQ_INT(live_subs(sv), 0);   /* modelled in s->publishes */
    (void)moq_session_poll_actions(sv, NULL, 0);
    uint8_t wire[128];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, wire, sizeof(wire));
    moq_d16_fetch_t fe;
    memset(&fe, 0, sizeof(fe));
    fe.request_id = 0;
    fe.fetch_type = MOQ_D16_FETCH_TYPE_RELATIVE_JOIN;
    fe.joining_request_id = pub_req_id;
    fe.joining_start = 0;
    MOQ_TEST_CHECK_EQ_INT((int)moq_d16_encode_fetch(&w, &fe, NULL, 0),
                          (int)MOQ_OK);
    /* draft-16 carries requests on the CONTROL stream, not a request bidi. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_control_bytes(sv, wire, moq_buf_writer_offset(&w),
                                          moq_simpair_now_us(f.sp)),
        (int)MOQ_OK);

    bool saw_fetch_req = false;
    uint64_t sg = 0, so = 0, eg = 0, eo = 0;
    moq_event_t ev;
    while (moq_session_poll_events(sv, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_FETCH_REQUEST) {
            saw_fetch_req = true;
            sg = ev.u.fetch_request.start_group;
            so = ev.u.fetch_request.start_object;
            eg = ev.u.fetch_request.end_group;
            eo = ev.u.fetch_request.end_object;
            /* The joined owner is surfaced as the publication, not a
             * subscription. */
            MOQ_TEST_CHECK(moq_publication_is_valid(ev.u.fetch_request.joining_pub));
            MOQ_TEST_CHECK(!moq_subscription_is_valid(ev.u.fetch_request.joining_sub));
        }
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK(saw_fetch_req);
    MOQ_TEST_CHECK_EQ_U64(sg, 0);
    MOQ_TEST_CHECK_EQ_U64(so, 0);
    MOQ_TEST_CHECK_EQ_U64(eg, 0);
    MOQ_TEST_CHECK_EQ_U64(eo, 1);

    fixture_down(&f);
    ROW_END();
}

/* draft-16 9.16.2: "any other value results in closing the session with a
 * PROTOCOL_VIOLATION" -- a request error would be wrong here. */
static void d16_non_largest_filter_is_protocol_error(void)
{
    ROW_BEGIN("draft-16 non-Largest filter closes the session");
    fix_t f;
    accept_opts_t ao = { .forward = true, .set_filter = true,
                         .filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP };
    if (!fixture_up_opt(&f, MOQ_VERSION_DRAFT_16, ao)) {
        MOQ_TEST_CHECK(false);
        fixture_down(&f);
        ROW_END();
        return;
    }
    moq_session_t *sv = moq_simpair_server(f.sp);
    uint64_t pub_req_id = 0;
    MOQ_TEST_CHECK(publish_request_id(sv, &pub_req_id));

    uint8_t wire[128];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, wire, sizeof(wire));
    moq_d16_fetch_t fe;
    memset(&fe, 0, sizeof(fe));
    fe.request_id = 0;
    fe.fetch_type = MOQ_D16_FETCH_TYPE_RELATIVE_JOIN;
    fe.joining_request_id = pub_req_id;
    fe.joining_start = 0;
    MOQ_TEST_CHECK_EQ_INT((int)moq_d16_encode_fetch(&w, &fe, NULL, 0),
                          (int)MOQ_OK);
    (void)moq_session_on_control_bytes(sv, wire, moq_buf_writer_offset(&w),
                                       moq_simpair_now_us(f.sp));
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv), (int)MOQ_SESS_CLOSED);

    fixture_down(&f);
    ROW_END();
}


/* ==================================================================== *
 * Gate rows -- the two states 10.12.2 excludes, each with its own code.
 * ==================================================================== */
static void gate_pending_publisher_is_invalid_joining_id(void)
{
    ROW_BEGIN("Pending (Publisher) join is INVALID_JOINING_REQUEST_ID");
    /* 10.12.2's state list is "Established or Pending (subscriber)". Pending
     * (Publisher) is absent, so a join before PUBLISH_OK is refused -- and
     * deliberately NOT buffered the way a pending SUBSCRIBE-origin join is. */
    fix_t f;
    memset(&f, 0, sizeof(f));
    f.sp = make_pair(MOQ_VERSION_DRAFT_18);
    MOQ_TEST_CHECK(f.sp != NULL);
    if (!f.sp) { ROW_END(); return; }
    moq_session_t *sv = moq_simpair_server(f.sp);
    moq_session_t *cl = moq_simpair_client(f.sp);
    (void)moq_session_grant_request_capacity(cl, 32, moq_simpair_now_us(f.sp));
    (void)moq_session_grant_request_capacity(sv, 32, moq_simpair_now_us(f.sp));
    moq_simpair_run_until_quiescent(f.sp, 16, NULL);

    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("live") };
    moq_publish_cfg_t pc;
    moq_publish_cfg_init(&pc);
    pc.track_namespace = (moq_namespace_t){ ns, 1 };
    pc.track_name = MOQ_BYTES_LITERAL("catalog");
    moq_publication_t ph = MOQ_PUBLICATION_INVALID;
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_publish(sv, &pc, moq_simpair_now_us(f.sp), &ph),
        (int)MOQ_OK);
    moq_simpair_run_until_quiescent(f.sp, 16, NULL);
    /* Deliberately NOT accepted: still Pending (Publisher). */

    uint64_t pub_req_id = 0;
    MOQ_TEST_CHECK(publish_request_id(sv, &pub_req_id));

    (void)moq_session_poll_actions(sv, NULL, 0);
    moq_stream_ref_t ref = moq_stream_ref_from_u64(0x6161);
    uint8_t msg[128];
    size_t n = encode_join_fetch(msg, sizeof(msg), 0, pub_req_id,
                                 MOQ_D18_FETCH_TYPE_RELATIVE, 0);
    MOQ_TEST_CHECK(n > 0);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(sv, ref, msg, n, false,
                                              moq_simpair_now_us(f.sp)),
        (int)MOQ_OK);
    expect_sole_request_error(sv, ref,
                              MOQ_REQUEST_ERROR_INVALID_JOINING_REQUEST_ID,
                              MOQ_REQUEST_ERROR_INVALID_RANGE,
                              "pending-publisher join");
    /* Not buffered: no FETCH_REQUEST now, and none later either. */
    bool any = false;
    moq_event_t ev;
    while (moq_session_poll_events(sv, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_FETCH_REQUEST) any = true;
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK(!any);

    moq_simpair_run_until_quiescent(f.sp, 8, NULL);
    moq_simpair_destroy(f.sp);
    f.sp = NULL;
    ROW_END();
}

static void gate_forward_zero_is_invalid_range(void)
{
    ROW_BEGIN("Forward 1 -> 0 keeps the location and answers INVALID_RANGE");
    /* 10.12.2: "A Joining Fetch is only permitted when the associated
     * subscription has Forward State 1; otherwise the publisher MUST respond
     * with a REQUEST_ERROR with error code INVALID_RANGE."
     *
     * Establishing AT Forward 0 would not isolate that rule: such an entry
     * also has no latched Joining Location, so deleting the Forward check
     * still lands on the !has_joining_loc arm and answers INVALID_RANGE for
     * the wrong reason. The row therefore establishes at Forward 1, latches a
     * real location, and then DROPS Forward to 0 -- leaving a state where the
     * location is present and only the Forward gate can refuse. */
    fix_t f;
    accept_opts_t ao = { .forward = true, .set_filter = false };
    if (!fixture_up_opt(&f, MOQ_VERSION_DRAFT_18, ao)) {
        MOQ_TEST_CHECK(false);
        fixture_down(&f);
        ROW_END();
        return;
    }
    moq_session_t *sv = moq_simpair_server(f.sp);

    /* 1. Established at Forward 1 with a real latched location. */
    moq_pub_entry_t *pe = sole_pub_entry(sv, MOQ_PUB_ROLE_PUBLISHER);
    MOQ_TEST_CHECK(pe != NULL);
    if (!pe) { fixture_down(&f); ROW_END(); return; }
    MOQ_TEST_CHECK(pe->send_allowed);
    MOQ_TEST_CHECK(pe->has_joining_loc);
    const uint64_t g0 = pe->joining_group, o0 = pe->joining_object;

    /* 2. A real REQUEST_UPDATE dropping Forward to 0, acknowledged. */
    { bool hl=false; uint64_t g=0,o=0; (void)take_update_ok(&f,&hl,&g,&o); }
    MOQ_TEST_CHECK(update_publication_forward(&f, false));
    { bool hl=false; uint64_t g=0,o=0;
      MOQ_TEST_CHECK_EQ_INT(take_update_ok(&f,&hl,&g,&o), 1); }
    MOQ_TEST_CHECK(!pe->send_allowed);       /* the drop really happened */

    /* 3. The location survives the drop, unchanged. 5.1 makes it a saved
     *    value, not a live one: nothing about lowering Forward erases it. */
    MOQ_TEST_CHECK(pe->has_joining_loc);
    MOQ_TEST_CHECK_EQ_U64(pe->joining_group, g0);
    MOQ_TEST_CHECK_EQ_U64(pe->joining_object, o0);

    /* 4. The join is refused by the Forward gate ALONE. */
    uint64_t pub_req_id = 0;
    MOQ_TEST_CHECK(publish_request_id(sv, &pub_req_id));
    /* Drain BOTH rings for real before the feed: the update exchange above
     * left traffic in them, and a full action ring would make the refusal
     * unqueueable -- the row would then fail for a capacity reason instead of
     * proving the gate. (poll_actions with cap 0 drains nothing.) */
    { moq_action_t a;
      while (moq_session_poll_actions(sv, &a, 1) > 0) moq_action_cleanup(&a); }
    { moq_event_t d; while (moq_session_poll_events(sv, &d, 1) > 0)
          moq_event_cleanup(&d); }
    moq_stream_ref_t ref = moq_stream_ref_from_u64(0x6262);
    uint8_t msg[128];
    /* Request id 2, not 0: draft-18 requires inbound ids strictly in sequence,
     * and the REQUEST_UPDATE above was the client's FIRST request, consuming
     * id 0. Feeding 0 here would be a sequence violation and the session would
     * close instead of answering -- which is exactly what the closed-session
     * assertion below catches if this ever drifts. */
    size_t n = encode_join_fetch(msg, sizeof(msg), 2, pub_req_id,
                                 MOQ_D18_FETCH_TYPE_RELATIVE, 0);
    MOQ_TEST_CHECK(n > 0);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(sv, ref, msg, n, false,
                                              moq_simpair_now_us(f.sp)),
        (int)MOQ_OK);
    /* The refusal is a REQUEST_ERROR on a LIVE session, not a protocol close. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv), (int)MOQ_SESS_ESTABLISHED);
    /* ONE capture, whole declared set: exactly one INVALID_RANGE on our own
     * ref and NO unknown-id code. Both halves are judged against the same
     * drain, so the forbidden half cannot pass on an emptied queue. */
    expect_sole_request_error(sv, ref, MOQ_REQUEST_ERROR_INVALID_RANGE,
                              MOQ_REQUEST_ERROR_INVALID_JOINING_REQUEST_ID,
                              "forward-0 join");
    /* A refused join surfaces NO request to the application. */
    int n_req = 0;
    moq_event_t ev;
    while (moq_session_poll_events(sv, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_FETCH_REQUEST) n_req++;
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK_EQ_INT(n_req, 0);

    fixture_down(&f);
    ROW_END();
}

static void reuse_leaves_no_stale_joining_location(void)
{
    ROW_BEGIN("slot reuse carries no stale Joining Location");
    fix_t f;
    if (!fixture_up(&f, MOQ_VERSION_DRAFT_18)) {
        MOQ_TEST_CHECK(false);
        fixture_down(&f);
        ROW_END();
        return;
    }
    moq_session_t *sv = moq_simpair_server(f.sp);

    /* The first publication latched (0,0). */
    uint64_t first_req = 0;
    MOQ_TEST_CHECK(publish_request_id(sv, &first_req));

    /* Retire it. */
    moq_finish_publish_cfg_t fin;
    memset(&fin, 0, sizeof(fin));
    fin.struct_size = sizeof(fin);
    fin.status_code = 0;
    fin.stream_count = 0;
    for (size_t i = 0; i < sv->pub_cap; i++) {
        if (sv->publishes[i].state != MOQ_PUB_FREE &&
            sv->publishes[i].role == MOQ_PUB_ROLE_PUBLISHER) {
            (void)moq_session_finish_publish(sv, sv->publishes[i].handle, &fin,
                                             moq_simpair_now_us(f.sp));
            break;
        }
    }
    moq_simpair_run_until_quiescent(f.sp, 16, NULL);
    (void)moq_pub_tick(f.pub, moq_simpair_now_us(f.sp));

    /* Every freed slot must be scrubbed -- no Joining Location may survive
     * into the next occupant. pub_free_entry's memset is what guarantees it;
     * this row is what makes that load-bearing. */
    int live = 0;
    for (size_t i = 0; i < sv->pub_cap; i++) {
        const moq_pub_entry_t *pe = &sv->publishes[i];
        if (pe->state == MOQ_PUB_FREE) {
            MOQ_TEST_CHECK(!pe->has_joining_loc);
            MOQ_TEST_CHECK(!pe->publish_has_largest);
            MOQ_TEST_CHECK_EQ_U64(pe->joining_group, 0);
            MOQ_TEST_CHECK_EQ_U64(pe->joining_object, 0);
        } else {
            live++;
        }
    }
    MOQ_TEST_CHECK_EQ_INT(live, 0);

    fixture_down(&f);
    ROW_END();
}

/* The frozen v0 layout is the pointer-only init floor: a caller that
 * allocated exactly the old struct must not have its allocation overrun by a
 * newer library, and moq_fetch_cfg_init_sized() must clamp into
 * [v0 floor, sizeof(cfg)]. */
/* draft-18 10.13: "The End Location MUST be the same as or larger than the
 * Start Location" for an Absolute Joining Fetch, and a Start Group beyond the
 * Largest MUST be answered with REQUEST_ERROR(INVALID_RANGE).
 *
 * An Absolute join takes its Start from the request and its End from the
 * Joining Location, so the two can disagree -- unlike a Relative join, whose
 * Start is derived from the same location and is clamped at 0. The
 * SUBSCRIBE-origin branch has always checked this; the PUBLISH-origin branch
 * must too, or an inverted range reaches the application.
 *
 * Runs on BOTH profiles: the branch is shared, but draft-16 reaches it under a
 * different eligibility rule (the LARGEST_OBJECT filter, 9.16.2) and its
 * coverage is not inferred from draft-18. */
static void absolute_join_start_past_location_is_invalid_range(
    moq_version_t ver, const char *label)
{
    char title[96];
    snprintf(title, sizeof(title),
             "%s absolute join past the location is INVALID_RANGE", label);
    ROW_BEGIN(title);
    fix_t f;
    /* draft-16 eligibility is the negotiated filter; draft-18's is Forward 1.
     * Each profile is established the way ITS OWN rule requires. */
    accept_opts_t ao = { .forward = true, .set_filter = false };
    if (ver == MOQ_VERSION_DRAFT_16) {
        ao.set_filter = true;
        ao.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    }
    if (!fixture_up_opt(&f, ver, ao)) {
        MOQ_TEST_CHECK(false);
        fixture_down(&f);
        ROW_END();
        return;
    }
    moq_session_t *sv = moq_simpair_server(f.sp);

    moq_pub_entry_t *pe = sole_pub_entry(sv, MOQ_PUB_ROLE_PUBLISHER);
    MOQ_TEST_CHECK(pe != NULL);
    if (!pe) { fixture_down(&f); ROW_END(); return; }
    /* Eligible, with a real latched location -- so nothing EARLIER in the
     * cascade can refuse, and the range check is the only thing left. */
    MOQ_TEST_CHECK(pe->has_joining_loc);
    const uint64_t loc_g = pe->joining_group;
    const uint64_t loc_o = pe->joining_object;

    /* Snapshot the state a refusal must not disturb. */
    const uint64_t st_before = (uint64_t)pe->state;
    const uint32_t gen_before = pe->generation;
    const bool     fwd_before = pe->send_allowed;

    uint64_t pub_req_id = 0;
    MOQ_TEST_CHECK(publish_request_id(sv, &pub_req_id));
    { moq_action_t a;
      while (moq_session_poll_actions(sv, &a, 1) > 0) moq_action_cleanup(&a); }
    { moq_event_t d; while (moq_session_poll_events(sv, &d, 1) > 0)
          moq_event_cleanup(&d); }

    /* Start ONE GROUP BEYOND the latched location: End would be
     * {loc_g, loc_o+1}, so Start > End -- exactly the inversion 10.13 bars.
     *
     * The two profiles are fed through their OWN request carriers and judged
     * by their own declared output shape: draft-18 uses a request bidi,
     * draft-16 the control stream. Nothing is inferred across them. */
    moq_stream_ref_t ref = moq_stream_ref_from_u64(0x7373);
    if (ver == MOQ_VERSION_DRAFT_18) {
        uint8_t msg[128];
        size_t n = encode_join_fetch(msg, sizeof(msg), 0, pub_req_id,
                                     MOQ_D18_FETCH_TYPE_ABSOLUTE, loc_g + 1);
        MOQ_TEST_CHECK(n > 0);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(sv, ref, msg, n, false,
                                                  moq_simpair_now_us(f.sp)),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv),
                              (int)MOQ_SESS_ESTABLISHED);
        expect_sole_request_error(sv, ref, MOQ_REQUEST_ERROR_INVALID_RANGE,
                                  MOQ_REQUEST_ERROR_INVALID_JOINING_REQUEST_ID,
                                  title);
    } else {
        uint8_t wire[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, wire, sizeof(wire));
        moq_d16_fetch_t fe;
        memset(&fe, 0, sizeof(fe));
        fe.request_id = 0;
        fe.fetch_type = MOQ_D16_FETCH_TYPE_ABSOLUTE_JOIN;
        fe.joining_request_id = pub_req_id;
        fe.joining_start = loc_g + 1;
        MOQ_TEST_CHECK_EQ_INT((int)moq_d16_encode_fetch(&w, &fe, NULL, 0),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_control_bytes(sv, wire,
                                              moq_buf_writer_offset(&w),
                                              moq_simpair_now_us(f.sp)),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv),
                              (int)MOQ_SESS_ESTABLISHED);
        expect_sole_control_request_error(
            sv, MOQ_REQUEST_ERROR_INVALID_RANGE,
            MOQ_REQUEST_ERROR_INVALID_JOINING_REQUEST_ID, title);
    }
    /* No request reaches the application. */
    int n_req = 0;
    moq_event_t ev;
    while (moq_session_poll_events(sv, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_FETCH_REQUEST) n_req++;
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK_EQ_INT(n_req, 0);
    /* And the publication is untouched: a refused join mutates no state. */
    MOQ_TEST_CHECK_EQ_U64((uint64_t)pe->state, st_before);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)pe->generation, (uint64_t)gen_before);
    MOQ_TEST_CHECK_EQ_U64(pe->send_allowed ? 1 : 0, fwd_before ? 1 : 0);
    MOQ_TEST_CHECK(pe->has_joining_loc);
    MOQ_TEST_CHECK_EQ_U64(pe->joining_group, loc_g);
    MOQ_TEST_CHECK_EQ_U64(pe->joining_object, loc_o);

    fixture_down(&f);
    ROW_END();
}

static void d18_absolute_join_past_location(void)
{
    absolute_join_start_past_location_is_invalid_range(MOQ_VERSION_DRAFT_18,
                                                       "draft-18");
}

static void d16_absolute_join_past_location(void)
{
    absolute_join_start_past_location_is_invalid_range(MOQ_VERSION_DRAFT_16,
                                                       "draft-16");
}

/* ---- one complete FETCH_REQUEST event inventory ---------------------- *
 * A row that polls the queue and looks only at MOQ_EVENT_FETCH_REQUEST cannot
 * see a foreign or duplicate event, and one that compares a few fields cannot
 * see a wrong owner, priority, order or token. The boundary rows therefore
 * declare the WHOLE expected event up front -- never normalized from the event
 * under test -- and compare it field by field against a single capture that
 * classifies every event in the queue. */
#define FRI_MAX_TOKENS 4
typedef struct {
    moq_fetch_t        fetch;          /* exact minted handle */
    bool               join_pub;       /* true: expect joining_pub, else _sub */
    moq_publication_t  joining_pub;
    moq_subscription_t joining_sub;
    uint64_t start_group, start_object, end_group, end_object;
    uint8_t            subscriber_priority;
    moq_group_order_t  group_order;
    size_t             token_count;
    uint64_t           token_type[FRI_MAX_TOKENS];
    const uint8_t     *token_value[FRI_MAX_TOKENS];
    size_t             token_len[FRI_MAX_TOKENS];
} fetch_req_expect_t;

/* Compare the complete event queue against `x`. `what` names the row. */
static void expect_sole_fetch_request(moq_session_t *s,
                                      const fetch_req_expect_t *x,
                                      const char *what)
{
    int events = 0, reqs = 0, foreign = 0;
    moq_fetch_request_event_t got = {0};
    bool have = false;
    /* Token values borrow output scratch and are valid only until the next
     * poll, so they are compared inside the loop; everything else is copied. */
    bool tok_ok = true;
    size_t tok_seen = 0;
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        events++;
        if (ev.kind == MOQ_EVENT_FETCH_REQUEST) {
            reqs++;
            got = ev.u.fetch_request;
            have = true;
            tok_seen = got.token_count;
            if (tok_seen != x->token_count) {
                tok_ok = false;
            } else if (tok_seen > 0) {
                if (!got.tokens) {
                    tok_ok = false;      /* nonempty but NULL: never deref */
                } else {
                    for (size_t i = 0; i < tok_seen; i++) {
                        if (got.tokens[i].token_type != x->token_type[i] ||
                            got.tokens[i].token_value.len != x->token_len[i]) {
                            tok_ok = false; break;
                        }
                        if (x->token_len[i] > 0) {
                            if (!got.tokens[i].token_value.data) {
                                tok_ok = false; break;
                            }
                            if (memcmp(got.tokens[i].token_value.data,
                                       x->token_value[i], x->token_len[i]) != 0) {
                                tok_ok = false; break;
                            }
                        }
                    }
                }
            }
        } else {
            foreign++;
        }
        moq_event_cleanup(&ev);
    }

    if (events != 1 || reqs != 1 || foreign != 0) {
        printf("FAIL: %s: events=%d fetch_requests=%d foreign=%d\n",
               what, events, reqs, foreign);
        failures++;
        return;
    }
    if (!have) return;   /* unreachable given the counts above */

    /* Exact minted handle -- not merely "nonzero". */
    if (!moq_fetch_eq(got.fetch, x->fetch)) {
        printf("FAIL: %s: fetch handle is not the one minted for this request\n",
               what);
        failures++;
    }
    /* A joining fetch names no track: 10.12.2 resolves it from the owner. */
    if (got.track_namespace.count != 0) {
        printf("FAIL: %s: namespace has %zu parts, expected 0\n",
               what, got.track_namespace.count);
        failures++;
    }
    if (got.track_name.len != 0) {
        printf("FAIL: %s: track name has %zu bytes, expected 0\n",
               what, got.track_name.len);
        failures++;
    }
    /* EXACTLY ONE joined owner, and it is the declared one. */
    if (x->join_pub) {
        if (!moq_publication_eq(got.joining_pub, x->joining_pub)) {
            printf("FAIL: %s: joining_pub is not the declared publication\n",
                   what);
            failures++;
        }
        if (moq_subscription_is_valid(got.joining_sub)) {
            printf("FAIL: %s: joining_sub is set on a publication join\n", what);
            failures++;
        }
    } else {
        if (!moq_subscription_eq(got.joining_sub, x->joining_sub)) {
            printf("FAIL: %s: joining_sub is not the declared subscription\n",
                   what);
            failures++;
        }
        if (moq_publication_is_valid(got.joining_pub)) {
            printf("FAIL: %s: joining_pub is set on a subscription join\n", what);
            failures++;
        }
    }
    if (got.start_group != x->start_group || got.start_object != x->start_object ||
        got.end_group != x->end_group || got.end_object != x->end_object) {
        printf("FAIL: %s: range {%llu,%llu}..{%llu,%llu}, expected "
               "{%llu,%llu}..{%llu,%llu}\n", what,
               (unsigned long long)got.start_group,
               (unsigned long long)got.start_object,
               (unsigned long long)got.end_group,
               (unsigned long long)got.end_object,
               (unsigned long long)x->start_group,
               (unsigned long long)x->start_object,
               (unsigned long long)x->end_group,
               (unsigned long long)x->end_object);
        failures++;
    }
    if (got.subscriber_priority != x->subscriber_priority) {
        printf("FAIL: %s: subscriber_priority %u, expected %u\n", what,
               (unsigned)got.subscriber_priority,
               (unsigned)x->subscriber_priority);
        failures++;
    }
    if (got.group_order != x->group_order) {
        printf("FAIL: %s: group_order %u, expected %u\n", what,
               (unsigned)got.group_order, (unsigned)x->group_order);
        failures++;
    }
    if (!tok_ok) {
        printf("FAIL: %s: token inventory differs (count %zu, expected %zu)\n",
               what, tok_seen, x->token_count);
        failures++;
    }
}

/* ---- independent mint preflight ------------------------------------- *
 * Reading the handle back out of the pool after ingress is exact equality but
 * NOT an oracle: a defect that writes the same malformed handle into both the
 * owner and the event compares equal to itself. So the expectation is DERIVED
 * BEFORE the feed, from pool state the allocator has not yet touched, and
 * without calling the allocator's own free-slot scan or mint. */
typedef struct {
    bool     valid;           /* false = no usable prediction; never index */
    int      slot;            /* the slot the allocator must take */
    uint32_t want_gen;        /* generation | 1 */
    moq_fetch_t want_handle;  /* independently packed */
} mint_pre_t;

/* Always leaves *out in a defined state: on any failure it is a STABLE INVALID
 * descriptor (valid == false, slot == -1), which every consumer diagnoses and
 * stops on rather than indexing. */
static bool mint_preflight(moq_session_t *s, mint_pre_t *out)
{
    memset(out, 0, sizeof(*out));
    out->slot = -1;
    /* Independently identify the FIRST free slot. Deliberately open-coded
     * rather than calling fetch_find_free(), which is part of what is under
     * test. */
    int slot = -1;
    for (size_t i = 0; i < s->fetch_cap; i++) {
        if (s->fetches[i].state == MOQ_FETCH_FREE) { slot = (int)i; break; }
    }
    if (slot < 0) return false;                        /* stays invalid */
    out->want_gen = s->fetches[slot].generation | 1u;  /* live = odd */
    moq_fetch_t h = { moq_handle_pack(MOQ_HANDLE_POOL_FETCH, s->session_tag,
                                      out->want_gen, (uint32_t)slot) };
    if (h._opaque == 0) return false;                  /* stays invalid */
    out->slot = slot;
    out->want_handle = h;
    out->valid = true;
    return true;
}

/* After ingress/release: the declared slot must hold exactly the owner the
 * preflight predicted.
 *
 * `want_ref` is compared EXACTLY on every call, zero included: for draft-16
 * zero is the expected value (that profile has no request bidi), so treating
 * it as "unchecked" would let a stale nonzero ref survive unnoticed. If an
 * optional comparison is ever needed it gets its own boolean rather than
 * overloading a valid expected value. */
static void expect_minted_owner(moq_session_t *s, const mint_pre_t *pre,
                                uint64_t want_req_id, moq_stream_ref_t want_ref,
                                int want_state, int want_role, const char *what)
{
    if (!pre->valid || pre->slot < 0 || (size_t)pre->slot >= s->fetch_cap) {
        printf("FAIL: %s: mint preflight produced no usable slot (valid=%d "
               "slot=%d cap=%zu); not indexing\n",
               what, (int)pre->valid, pre->slot, s->fetch_cap);
        failures++;
        return;
    }
    const moq_fetch_entry_t *fe = &s->fetches[pre->slot];
    if ((int)fe->state != want_state || (int)fe->role != want_role ||
        fe->generation != pre->want_gen ||
        !moq_fetch_eq(fe->handle, pre->want_handle) ||
        fe->request_id != want_req_id ||
        fe->request_stream_ref._v != want_ref._v) {
        printf("FAIL: %s: minted owner at slot %d differs: state=%d(want %d) "
               "role=%d(want %d) gen=%u(want %u) handle=%llu(want %llu) "
               "req_id=%llu(want %llu) ref=%llu(want %llu)\n",
               what, pre->slot, (int)fe->state, want_state, (int)fe->role,
               want_role, (unsigned)fe->generation, (unsigned)pre->want_gen,
               (unsigned long long)fe->handle._opaque,
               (unsigned long long)pre->want_handle._opaque,
               (unsigned long long)fe->request_id,
               (unsigned long long)want_req_id,
               (unsigned long long)fe->request_stream_ref._v,
               (unsigned long long)want_ref._v);
        failures++;
    }
}

/* No event of ANY kind -- used where a feed must surface nothing yet. */
static void expect_no_events(moq_session_t *s, const char *what)
{
    int events = 0;
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        events++;
        printf("FAIL: %s: unexpected event kind %d\n", what, (int)ev.kind);
        moq_event_cleanup(&ev);
    }
    if (events != 0) failures++;
}

/* The complete action queue must be empty -- every kind, every ref. */
static void expect_no_actions(moq_session_t *s, const char *what)
{
    int actions = 0;
    moq_action_t act;
    while (moq_session_poll_actions(s, &act, 1) > 0) {
        actions++;
        printf("FAIL: %s: unexpected action kind %d\n", what, (int)act.kind);
        moq_action_cleanup(&act);
    }
    if (actions != 0) failures++;
}

/* ==================================================================== *
 * FETCH End Location at the profile's Location ceiling.
 *
 * draft-16 9.16.1 / draft-18 10.12.1: a FETCH End Location is "the end
 * Location, plus 1", and an End Location.Object of 0 means the ENTIRE group is
 * requested. So a Joining Location sitting AT the profile ceiling yields the
 * exact representable End {same group, 0} -- which covers the whole end group
 * and therefore still includes the Joining Location, as 9.16.2.1 / 10.12.2.1
 * require.
 *
 * Two things must hold and are asserted separately:
 *   - at the ceiling: end_object == 0 AND the end GROUP is unmoved (a carry to
 *     {group + 1, 0} -- what moq_loc_successor does for filter windows -- is
 *     the wrong answer here);
 *   - at ceiling - 1: end_object == ceiling, so the sentinel cannot be
 *     selected one step early.
 *
 * The ceiling differs per profile: 2^62-1 on draft-16 (where +1 leaves the
 * encoding entirely) and UINT64_MAX on draft-18 (where unsigned wrap happens to
 * coincide with the right value -- correct by accident, which is exactly why
 * the arithmetic is checked rather than inherited).
 * ==================================================================== */
static uint64_t profile_loc_ceiling(moq_version_t ver)
{
    return ver == MOQ_VERSION_DRAFT_16 ? MOQ_QUIC_VARINT_MAX : UINT64_MAX;
}

/* Site 3: the established PUBLISH-origin join. */
static void pub_join_end_at(moq_version_t ver, const char *label,
                            bool at_ceiling)
{
    const uint64_t ceil_o = profile_loc_ceiling(ver);
    const uint64_t loc_o = at_ceiling ? ceil_o : ceil_o - 1;
    const uint64_t want_eo = at_ceiling ? 0 : ceil_o;
    char title[128];
    snprintf(title, sizeof(title), "%s publication join end at %s",
             label, at_ceiling ? "the ceiling" : "ceiling - 1");
    ROW_BEGIN(title);

    fix_t f;
    accept_opts_t ao = { .forward = true, .set_filter = false };
    if (ver == MOQ_VERSION_DRAFT_16) {
        ao.set_filter = true;
        ao.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    }
    if (!fixture_up_opt(&f, ver, ao)) {
        MOQ_TEST_CHECK(false); fixture_down(&f); ROW_END(); return;
    }
    moq_session_t *sv = moq_simpair_server(f.sp);
    moq_pub_entry_t *pe = sole_pub_entry(sv, MOQ_PUB_ROLE_PUBLISHER);
    MOQ_TEST_CHECK(pe != NULL);
    if (!pe) { fixture_down(&f); ROW_END(); return; }
    MOQ_TEST_CHECK(pe->has_joining_loc);

    /* NARROW ARITHMETIC/CALL-SITE FIXTURE. The boundary value is INJECTED
     * directly into private owner state -- it did NOT traverse the peer wire
     * or the public facade path, and this row does not claim it did. That is
     * valid here because no facade history can reach the profile ceiling (the
     * retained-group contract caps object ids at 63), and because the real
     * latch carriers are proven separately by the establishment and
     * Forward-rise rows. What this row tests is the End conversion and its
     * call-site wiring, nothing more. Site 1 does carry its boundary through
     * the real public acceptance call. */
    const uint64_t loc_g = 4;
    pe->joining_group = loc_g;
    pe->joining_object = loc_o;

    uint64_t pub_req_id = 0;
    MOQ_TEST_CHECK(publish_request_id(sv, &pub_req_id));
    { moq_action_t a;
      while (moq_session_poll_actions(sv, &a, 1) > 0) moq_action_cleanup(&a); }
    { moq_event_t d; while (moq_session_poll_events(sv, &d, 1) > 0)
          moq_event_cleanup(&d); }

    /* Non-default priority and order, and one nonempty authorization token,
     * declared HERE as wire inputs and compared against the surfaced event --
     * never normalized from the event under test. Both profiles carry all
     * three, so no field falls back to a protocol default. */
    const uint8_t tok_bytes[] = { 0xC0, 0xFF, 0xEE };
    const uint64_t tok_type = 7;
    const uint8_t want_prio = 33;          /* default is 128 */
    const moq_group_order_t want_order = MOQ_GROUP_ORDER_DESCENDING;

    moq_stream_ref_t ref = moq_stream_ref_from_u64(0x8484);
    const uint64_t wire_req_id = 0;
    /* Derived BEFORE the feed, from untouched pool state. */
    mint_pre_t pre;
    if (!mint_preflight(sv, &pre)) {
        MOQ_TEST_CHECK(false);           /* no usable prediction: stop here */
        fixture_down(&f);
        ROW_END();
        return;
    }
    if (ver == MOQ_VERSION_DRAFT_18) {
        moq_d18_fetch_t fe;
        memset(&fe, 0, sizeof(fe));
        fe.request_id = wire_req_id;
        fe.fetch_type = MOQ_D18_FETCH_TYPE_RELATIVE;
        fe.joining_request_id = pub_req_id;
        fe.joining_start = 0;
        fe.params.has_subscriber_priority = true;
        fe.params.subscriber_priority = want_prio;
        fe.params.has_group_order = true;
        fe.params.group_order = 2;                 /* descending */
        fe.params.auth_token_count = 1;
        fe.params.auth_tokens[0].alias_type = 3;   /* USE_VALUE */
        fe.params.auth_tokens[0].token_type = tok_type;
        fe.params.auth_tokens[0].token_value =
            (moq_bytes_t){ tok_bytes, sizeof(tok_bytes) };
        uint8_t msg[192];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_fetch(&w, &fe), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(sv, ref, msg,
                                                  moq_buf_writer_offset(&w),
                                                  false,
                                                  moq_simpair_now_us(f.sp)),
            (int)MOQ_OK);
    } else {
        /* draft-16 carries the same three as KVP message parameters. */
        uint8_t tokbuf[64];
        moq_buf_writer_t tw;
        moq_buf_writer_init(&tw, tokbuf, sizeof(tokbuf));
        moq_d16_auth_token_t d16tok;
        memset(&d16tok, 0, sizeof(d16tok));
        d16tok.alias_type = 3;                     /* USE_VALUE */
        d16tok.token_type = tok_type;
        d16tok.token_value = tok_bytes;
        d16tok.token_value_len = sizeof(tok_bytes);
        MOQ_TEST_CHECK_EQ_INT((int)moq_d16_auth_token_encode(&tw, &d16tok),
                              (int)MOQ_OK);
        /* Even (varint) KVP types carry the value as ENCODED varint bytes,
         * not as a raw integer in value_len. */
        uint8_t prio_v[8], order_v[8];
        size_t prio_n = moq_quic_varint_encode(want_prio, prio_v,
                                               sizeof(prio_v));
        size_t order_n = moq_quic_varint_encode(2, order_v, sizeof(order_v));
        MOQ_TEST_CHECK(prio_n > 0 && order_n > 0);
        /* KVP entries encode as deltas and must be ASCENDING by type:
         * AUTHORIZATION_TOKEN (0x03) precedes SUBSCRIBER_PRIORITY (0x20) and
         * GROUP_ORDER (0x22). */
        moq_kvp_entry_t params[3];
        memset(params, 0, sizeof(params));
        params[0].type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN;
        params[0].is_varint = false;
        params[0].value = tokbuf;
        params[0].value_len = moq_buf_writer_offset(&tw);
        params[1].type = MOQ_MSG_PARAM_SUBSCRIBER_PRIORITY;
        params[1].is_varint = true;
        params[1].value = prio_v; params[1].value_len = prio_n;
        params[2].type = MOQ_MSG_PARAM_GROUP_ORDER;
        params[2].is_varint = true;
        params[2].value = order_v; params[2].value_len = order_n;
        uint8_t wire[512];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, wire, sizeof(wire));
        moq_d16_fetch_t fe;
        memset(&fe, 0, sizeof(fe));
        fe.request_id = wire_req_id;
        fe.fetch_type = MOQ_D16_FETCH_TYPE_RELATIVE_JOIN;
        fe.joining_request_id = pub_req_id;
        fe.joining_start = 0;
        /* draft-16 params ride the ENCODER's arguments, not the struct's
         * decode-side fields. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_d16_encode_fetch(&w, &fe, params, 3),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_control_bytes(sv, wire,
                                              moq_buf_writer_offset(&w),
                                              moq_simpair_now_us(f.sp)),
            (int)MOQ_OK);
    }
    /* Accepted, not refused, and the session stays open. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv), (int)MOQ_SESS_ESTABLISHED);

    /* The COMPLETE declared event, against a capture of the whole queue. */
    /* The allocator took the predicted slot and minted the predicted handle. */
    expect_minted_owner(sv, &pre, wire_req_id,
                        ver == MOQ_VERSION_DRAFT_18 ? ref
                                                    : moq_stream_ref_from_u64(0),
                        MOQ_FETCH_PENDING_PUBLISHER, MOQ_FETCH_ROLE_PUBLISHER,
                        title);
    fetch_req_expect_t x;
    memset(&x, 0, sizeof(x));
    x.fetch = pre.want_handle;      /* pre-derived, never read back */
    x.join_pub = true;
    x.joining_pub = pe->handle;
    x.start_group = loc_g;          /* relative offset 0 */
    x.start_object = 0;
    x.end_group = loc_g;            /* no carry to group + 1 */
    x.end_object = want_eo;
    x.subscriber_priority = want_prio;
    x.group_order = want_order;
    x.token_count = 1;
    x.token_type[0] = tok_type;
    x.token_value[0] = tok_bytes;
    x.token_len[0] = sizeof(tok_bytes);
    expect_sole_fetch_request(sv, &x, title);

    /* The latched location is untouched by the conversion -- PRESENCE as well
     * as coordinates: clearing the bit while leaving the numbers would be
     * invisible to a coordinate-only check. */
    MOQ_TEST_CHECK(pe->has_joining_loc);
    MOQ_TEST_CHECK_EQ_U64(pe->joining_group, loc_g);
    MOQ_TEST_CHECK_EQ_U64(pe->joining_object, loc_o);
    /* An accepted feed owes NOTHING on the wire yet -- any kind, any ref,
     * both profiles. */
    expect_no_actions(sv, title);

    fixture_down(&f);
    ROW_END();
}

/* Site 2: the established SUBSCRIBE-origin join, at the same boundaries.
 * The publication branch and the subscription branch are separate code paths
 * that must agree, so this is not inferred from site 3. */
static void sub_join_end_at(moq_version_t ver, const char *label,
                            bool at_ceiling)
{
    const uint64_t ceil_o = profile_loc_ceiling(ver);
    const uint64_t loc_o = at_ceiling ? ceil_o : ceil_o - 1;
    const uint64_t want_eo = at_ceiling ? 0 : ceil_o;
    char title[128];
    snprintf(title, sizeof(title), "%s subscription join end at %s",
             label, at_ceiling ? "the ceiling" : "ceiling - 1");
    ROW_BEGIN(title);

    fix_t f;
    if (!fixture_up(&f, ver)) {
        MOQ_TEST_CHECK(false); fixture_down(&f); ROW_END(); return;
    }
    moq_session_t *sv = moq_simpair_server(f.sp);
    moq_session_t *cl = moq_simpair_client(f.sp);

    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("live") };
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ ns, 1 };
    sc.track_name = MOQ_BYTES_LITERAL("catalog");
    sc.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    moq_subscription_t sub = MOQ_SUBSCRIPTION_INVALID;
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_subscribe(cl, &sc, moq_simpair_now_us(f.sp), &sub),
        (int)MOQ_OK);
    moq_simpair_run_until_quiescent(f.sp, 16, NULL);
    (void)moq_pub_tick(f.pub, moq_simpair_now_us(f.sp));
    moq_simpair_run_until_quiescent(f.sp, 16, NULL);

    /* The publisher-side subscription entry: the owner inbound resolution
     * finds, and whose stored largest supplies the End. */
    moq_sub_entry_t *se = NULL;
    uint64_t sub_req_id = 0;
    int live = 0;
    for (size_t i = 0; i < sv->sub_cap; i++) {
        if (sv->subs[i].state == MOQ_SUB_ESTABLISHED &&
            sv->subs[i].role == MOQ_SUB_ROLE_PUBLISHER) {
            se = &sv->subs[i]; sub_req_id = sv->subs[i].request_id; live++;
        }
    }
    MOQ_TEST_CHECK_EQ_INT(live, 1);
    if (!se) { fixture_down(&f); ROW_END(); return; }
    /* Injected, exactly as at site 3 -- a narrow arithmetic/call-site fixture,
     * not a claim that this value crossed the wire. See the note there. */
    const uint64_t loc_g = 4;
    se->has_largest = true;
    se->largest_group = loc_g;
    se->largest_object = loc_o;

    { moq_action_t a;
      while (moq_session_poll_actions(sv, &a, 1) > 0) moq_action_cleanup(&a); }
    { moq_event_t d; while (moq_session_poll_events(sv, &d, 1) > 0)
          moq_event_cleanup(&d); }

    /* Same declared wire inputs as site 3, so the two branches are compared
     * against the same non-default values rather than each other. */
    const uint8_t tok_bytes[] = { 0xC0, 0xFF, 0xEE };
    const uint64_t tok_type = 7;
    const uint8_t want_prio = 33;
    const moq_group_order_t want_order = MOQ_GROUP_ORDER_DESCENDING;

    moq_stream_ref_t ref = moq_stream_ref_from_u64(0x8585);
    /* The SUBSCRIBE above consumed inbound id 0. */
    const uint64_t wire_req_id = 2;
    mint_pre_t pre;
    if (!mint_preflight(sv, &pre)) {
        MOQ_TEST_CHECK(false);           /* no usable prediction: stop here */
        fixture_down(&f);
        ROW_END();
        return;
    }
    if (ver == MOQ_VERSION_DRAFT_18) {
        moq_d18_fetch_t fe;
        memset(&fe, 0, sizeof(fe));
        fe.request_id = wire_req_id;
        fe.fetch_type = MOQ_D18_FETCH_TYPE_RELATIVE;
        fe.joining_request_id = sub_req_id;
        fe.joining_start = 0;
        fe.params.has_subscriber_priority = true;
        fe.params.subscriber_priority = want_prio;
        fe.params.has_group_order = true;
        fe.params.group_order = 2;
        fe.params.auth_token_count = 1;
        fe.params.auth_tokens[0].alias_type = 3;   /* USE_VALUE */
        fe.params.auth_tokens[0].token_type = tok_type;
        fe.params.auth_tokens[0].token_value =
            (moq_bytes_t){ tok_bytes, sizeof(tok_bytes) };
        uint8_t msg[192];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_fetch(&w, &fe), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(sv, ref, msg,
                                                  moq_buf_writer_offset(&w),
                                                  false,
                                                  moq_simpair_now_us(f.sp)),
            (int)MOQ_OK);
    } else {
        uint8_t tokbuf[64];
        moq_buf_writer_t tw;
        moq_buf_writer_init(&tw, tokbuf, sizeof(tokbuf));
        moq_d16_auth_token_t d16tok;
        memset(&d16tok, 0, sizeof(d16tok));
        d16tok.alias_type = 3;
        d16tok.token_type = tok_type;
        d16tok.token_value = tok_bytes;
        d16tok.token_value_len = sizeof(tok_bytes);
        MOQ_TEST_CHECK_EQ_INT((int)moq_d16_auth_token_encode(&tw, &d16tok),
                              (int)MOQ_OK);
        uint8_t prio_v[8], order_v[8];
        size_t prio_n = moq_quic_varint_encode(want_prio, prio_v,
                                               sizeof(prio_v));
        size_t order_n = moq_quic_varint_encode(2, order_v, sizeof(order_v));
        MOQ_TEST_CHECK(prio_n > 0 && order_n > 0);
        moq_kvp_entry_t params[3];      /* ascending by type */
        memset(params, 0, sizeof(params));
        params[0].type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN;
        params[0].is_varint = false;
        params[0].value = tokbuf;
        params[0].value_len = moq_buf_writer_offset(&tw);
        params[1].type = MOQ_MSG_PARAM_SUBSCRIBER_PRIORITY;
        params[1].is_varint = true;
        params[1].value = prio_v; params[1].value_len = prio_n;
        params[2].type = MOQ_MSG_PARAM_GROUP_ORDER;
        params[2].is_varint = true;
        params[2].value = order_v; params[2].value_len = order_n;
        uint8_t wire[512];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, wire, sizeof(wire));
        moq_d16_fetch_t fe;
        memset(&fe, 0, sizeof(fe));
        fe.request_id = wire_req_id;
        fe.fetch_type = MOQ_D16_FETCH_TYPE_RELATIVE_JOIN;
        fe.joining_request_id = sub_req_id;
        fe.joining_start = 0;
        MOQ_TEST_CHECK_EQ_INT((int)moq_d16_encode_fetch(&w, &fe, params, 3),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_control_bytes(sv, wire,
                                              moq_buf_writer_offset(&w),
                                              moq_simpair_now_us(f.sp)),
            (int)MOQ_OK);
    }
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv), (int)MOQ_SESS_ESTABLISHED);

    expect_minted_owner(sv, &pre, wire_req_id,
                        ver == MOQ_VERSION_DRAFT_18 ? ref
                                                    : moq_stream_ref_from_u64(0),
                        MOQ_FETCH_PENDING_PUBLISHER, MOQ_FETCH_ROLE_PUBLISHER,
                        title);
    fetch_req_expect_t x;
    memset(&x, 0, sizeof(x));
    x.fetch = pre.want_handle;
    x.join_pub = false;                  /* subscribe-origin: exact joining_sub */
    x.joining_sub = se->handle;
    x.start_group = loc_g;
    x.start_object = 0;
    x.end_group = loc_g;                 /* no carry to group + 1 */
    x.end_object = want_eo;
    x.subscriber_priority = want_prio;
    x.group_order = want_order;
    x.token_count = 1;
    x.token_type[0] = tok_type;
    x.token_value[0] = tok_bytes;
    x.token_len[0] = sizeof(tok_bytes);
    expect_sole_fetch_request(sv, &x, title);

    /* The stored largest is untouched -- presence as well as coordinates. */
    MOQ_TEST_CHECK(se->has_largest);
    MOQ_TEST_CHECK_EQ_U64(se->largest_group, loc_g);
    MOQ_TEST_CHECK_EQ_U64(se->largest_object, loc_o);
    /* An accepted feed owes nothing on the wire yet -- both profiles. */
    expect_no_actions(sv, title);

    fixture_down(&f);
    ROW_END();
}

/* Site 1: the PENDING-subscription join release. The Joining FETCH arrives
 * while the subscription is still Pending (Publisher), so it is BUFFERED on
 * its request bidi (10.12.2, request-stream profiles only) and its range is
 * computed later, when acceptance releases it -- a third, separate End
 * computation. draft-18 only: draft-16 has no request bidi to defer on and
 * keeps its eager reject, so there is nothing to release there. */
static void pending_join_release_end_at(bool at_ceiling)
{
    const uint64_t ceil_o = profile_loc_ceiling(MOQ_VERSION_DRAFT_18);
    const uint64_t loc_o = at_ceiling ? ceil_o : ceil_o - 1;
    const uint64_t want_eo = at_ceiling ? 0 : ceil_o;
    char title[128];
    snprintf(title, sizeof(title),
             "draft-18 pending-join release end at %s",
             at_ceiling ? "the ceiling" : "ceiling - 1");
    ROW_BEGIN(title);

    moq_simpair_t *sp = make_pair(MOQ_VERSION_DRAFT_18);
    if (!sp) { MOQ_TEST_CHECK(false); ROW_END(); return; }
    moq_session_t *sv = moq_simpair_server(sp);
    moq_session_t *cl = moq_simpair_client(sp);
    (void)moq_session_grant_request_capacity(cl, 32, moq_simpair_now_us(sp));
    (void)moq_session_grant_request_capacity(sv, 32, moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 16, NULL);

    /* A peer SUBSCRIBE the application has NOT answered yet: the subscription
     * sits Pending (Publisher) on the server. */
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("live") };
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ ns, 1 };
    sc.track_name = MOQ_BYTES_LITERAL("catalog");
    sc.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    moq_subscription_t sub = MOQ_SUBSCRIPTION_INVALID;
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_subscribe(cl, &sc, moq_simpair_now_us(sp), &sub),
        (int)MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 16, NULL);

    moq_sub_entry_t *se = NULL;
    uint64_t sub_req_id = 0;
    for (size_t i = 0; i < sv->sub_cap; i++) {
        if (sv->subs[i].state == MOQ_SUB_PENDING_PUBLISHER &&
            sv->subs[i].role == MOQ_SUB_ROLE_PUBLISHER) {
            se = &sv->subs[i]; sub_req_id = sv->subs[i].request_id;
        }
    }
    MOQ_TEST_CHECK(se != NULL);
    if (!se) { moq_simpair_destroy(sp); ROW_END(); return; }

    /* Drain the establishment exchange so the inventories below describe only
     * what the join itself produces. */
    { moq_event_t d; while (moq_session_poll_events(sv, &d, 1) > 0)
          moq_event_cleanup(&d); }
    { moq_action_t a;
      while (moq_session_poll_actions(sv, &a, 1) > 0) moq_action_cleanup(&a); }

    /* The join arrives BEFORE acceptance: buffered, not answered. Same
     * declared non-default wire inputs as sites 2 and 3, so the released event
     * is compared against values this row states rather than reads back. */
    const uint8_t tok_bytes[] = { 0xC0, 0xFF, 0xEE };
    const uint64_t tok_type = 7;
    const uint8_t want_prio = 33;
    const moq_group_order_t want_order = MOQ_GROUP_ORDER_DESCENDING;

    moq_stream_ref_t ref = moq_stream_ref_from_u64(0x8686);
    const uint64_t wire_req_id = 2;   /* the SUBSCRIBE consumed inbound id 0 */
    /* The BUFFERING feed is what allocates the fetch owner here, so the
     * preflight is taken before it -- not before the later release. */
    mint_pre_t pre;
    if (!mint_preflight(sv, &pre)) {
        MOQ_TEST_CHECK(false);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        moq_simpair_destroy(sp);
        ROW_END();
        return;
    }
    {
        moq_d18_fetch_t fe;
        memset(&fe, 0, sizeof(fe));
        fe.request_id = wire_req_id;
        fe.fetch_type = MOQ_D18_FETCH_TYPE_RELATIVE;
        fe.joining_request_id = sub_req_id;
        fe.joining_start = 0;
        fe.params.has_subscriber_priority = true;
        fe.params.subscriber_priority = want_prio;
        fe.params.has_group_order = true;
        fe.params.group_order = 2;
        fe.params.auth_token_count = 1;
        fe.params.auth_tokens[0].alias_type = 3;   /* USE_VALUE */
        fe.params.auth_tokens[0].token_type = tok_type;
        fe.params.auth_tokens[0].token_value =
            (moq_bytes_t){ tok_bytes, sizeof(tok_bytes) };
        uint8_t msg[192];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_fetch(&w, &fe), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(sv, ref, msg,
                                                  moq_buf_writer_offset(&w),
                                                  false,
                                                  moq_simpair_now_us(sp)),
            (int)MOQ_OK);
    }
    /* Buffered, not surfaced -- and NOTHING else surfaced either. Counting
     * only FETCH_REQUEST here would miss a foreign event the buffering path
     * should not have produced. */
    expect_no_events(sv, "pending-join buffered: no event yet");
    /* The buffering allocation took the predicted slot and minted the
     * predicted handle, while the join waits in PENDING_JOIN. */
    expect_minted_owner(sv, &pre, wire_req_id, ref,
                        MOQ_FETCH_PENDING_JOIN, MOQ_FETCH_ROLE_PUBLISHER,
                        title);

    /* Acceptance carries the Largest at the boundary under test, and RELEASES
     * the buffered join -- the third End computation. Unlike sites 2 and 3,
     * this boundary value travels through the REAL public acceptance call. */
    const uint64_t loc_g = 4;
    const uint64_t want_alias = 77;          /* non-default */
    const uint64_t want_expires = 4321;      /* non-default, declared here */
    /* One valid, non-mandatory Track Property, declared as bytes so the body
     * oracle compares content rather than mere presence. The KVP type is even
     * (varint) and outside the mandatory range. */
    uint8_t propbuf[16];
    size_t prop_n = 0;
    {
        moq_kvp_entry_t pe2;
        memset(&pe2, 0, sizeof(pe2));
        uint8_t v[8];
        size_t vn = moq_quic_varint_encode(1, v, sizeof(v));
        MOQ_TEST_CHECK(vn > 0);
        pe2.type = 0x30;                     /* DYNAMIC_GROUPS (§12.6) */
        pe2.is_varint = true;
        pe2.value = v; pe2.value_len = vn;
        prop_n = moq_kvp_encode_entry(0, &pe2, propbuf, sizeof(propbuf));
        MOQ_TEST_CHECK(prop_n > 0);
    }
    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    ac.has_track_alias = true;
    ac.track_alias = want_alias;
    ac.has_largest = true;
    ac.largest_group = loc_g;
    ac.largest_object = loc_o;
    ac.has_expires = true;
    ac.expires_ms = want_expires;
    ac.track_properties = (moq_bytes_t){ propbuf, prop_n };
    moq_subscription_t svsub = se->handle;
    /* The subscription's OWN request bidi -- captured BEFORE the call, so the
     * action inventory below can tell it from the held FETCH ref. */
    const moq_stream_ref_t sub_ref = se->request_stream_ref;
    MOQ_TEST_CHECK(sub_ref._v != ref._v);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_accept_subscribe(sv, svsub, &ac,
                                          moq_simpair_now_us(sp)),
        (int)MOQ_OK);

    /* The COMPLETE released event. */
    fetch_req_expect_t x;
    memset(&x, 0, sizeof(x));
    x.fetch = pre.want_handle;
    x.join_pub = false;
    x.joining_sub = svsub;
    x.start_group = loc_g;
    x.start_object = 0;
    x.end_group = loc_g;                 /* no carry to group + 1 */
    x.end_object = want_eo;
    x.subscriber_priority = want_prio;
    x.group_order = want_order;
    x.token_count = 1;
    x.token_type[0] = tok_type;
    x.token_value[0] = tok_bytes;
    x.token_len[0] = sizeof(tok_bytes);
    expect_sole_fetch_request(sv, &x, title);
    /* The released owner keeps that same identity -- the release advances its
     * state, it does not re-mint. */
    expect_minted_owner(sv, &pre, wire_req_id, ref,
                        MOQ_FETCH_PENDING_PUBLISHER, MOQ_FETCH_ROLE_PUBLISHER,
                        title);

    /* The COMPLETE action inventory. Unlike sites 2 and 3, acceptance here
     * legitimately owes ONE SUBSCRIBE_OK on the SUBSCRIPTION's request bidi --
     * and nothing at all on the held FETCH ref. The decoded body is compared
     * for EVERY semantic field the SUBSCRIBE_OK mask can surface, against
     * values declared above; absent fields are pinned to their exact absent
     * values rather than skipped. */
    {
        int actions = 0, on_sub = 0, on_fetch_ref = 0, foreign = 0;
        int oks = 0, undecodable = 0, trailing = 0, other_msg = 0, finned = 0;
        int null_payload = 0, body_bad = 0;
        moq_action_t act;
        while (moq_session_poll_actions(sv, &act, 1) > 0) {
            actions++;
            if (act.kind == MOQ_ACTION_SEND_BIDI_STREAM &&
                act.u.send_bidi_stream.stream_ref._v == sub_ref._v) {
                on_sub++;
                if (act.u.send_bidi_stream.fin) finned++;
                /* A nonempty action payload must be non-NULL BEFORE decoding,
                 * so a bad fixture output is a named diagnostic rather than a
                 * reliance on decoder defensiveness. */
                if (act.u.send_bidi_stream.len > 0 &&
                    !act.u.send_bidi_stream.data) {
                    null_payload++;
                    moq_action_cleanup(&act);
                    continue;
                }
                moq_buf_reader_t rr;
                moq_buf_reader_init(&rr, act.u.send_bidi_stream.data,
                                    act.u.send_bidi_stream.len);
                moq_control_envelope_t env;
                if (moq_d18_decode_envelope(&rr, &env) != MOQ_OK) {
                    undecodable++;
                } else if (env.msg_type != MOQ_D18_SUBSCRIBE_OK) {
                    other_msg++;
                } else {
                    moq_d18_subscribe_ok_t ok;
                    if (moq_d18_decode_subscribe_ok(env.payload,
                                                    env.payload_len,
                                                    &ok) != MOQ_OK) {
                        undecodable++;
                    } else {
                        oks++;
                        if (moq_buf_reader_remaining(&rr) != 0) trailing++;
                        /* --- the complete declared body --- */
                        #define BODY_CHK(cond, name) \
                            do { if (!(cond)) { \
                                printf("FAIL: %s: SUBSCRIBE_OK body field %s\n", \
                                       title, (name)); body_bad++; } } while (0)
                        BODY_CHK(ok.track_alias == want_alias, "track_alias");
                        BODY_CHK(ok.params.has_largest, "has_largest");
                        BODY_CHK(ok.params.largest_group == loc_g,
                                 "largest_group");
                        BODY_CHK(ok.params.largest_object == loc_o,
                                 "largest_object");
                        BODY_CHK(ok.params.has_expires, "has_expires");
                        BODY_CHK(ok.params.expires_ms == want_expires,
                                 "expires_ms");
                        /* Track Properties: exact length AND bytes, guarded. */
                        BODY_CHK(ok.track_properties.len == prop_n,
                                 "track_properties length");
                        if (ok.track_properties.len == prop_n && prop_n > 0) {
                            BODY_CHK(ok.track_properties.data != NULL,
                                     "track_properties data NULL");
                            if (ok.track_properties.data)
                                BODY_CHK(memcmp(ok.track_properties.data,
                                                propbuf, prop_n) == 0,
                                         "track_properties bytes");
                        }
                        /* The declared property IS DYNAMIC_GROUPS=1, and it is
                         * NOT in the mandatory range. */
                        BODY_CHK(ok.dynamic_groups, "dynamic_groups");
                        BODY_CHK(!ok.track_properties_unsupported,
                                 "track_properties_unsupported");
                        /* Every other surfaced parameter must be ABSENT, with
                         * its value pinned to zero. */
                        BODY_CHK(!ok.params.has_forward, "has_forward");
                        BODY_CHK(ok.params.forward == 0, "forward");
                        BODY_CHK(!ok.params.has_subscriber_priority,
                                 "has_subscriber_priority");
                        BODY_CHK(ok.params.subscriber_priority == 0,
                                 "subscriber_priority");
                        BODY_CHK(!ok.params.has_group_order, "has_group_order");
                        BODY_CHK(ok.params.group_order == 0, "group_order");
                        BODY_CHK(!ok.params.has_filter, "has_filter");
                        BODY_CHK(ok.params.filter_type == 0, "filter_type");
                        BODY_CHK(ok.params.filter_start_group == 0,
                                 "filter_start_group");
                        BODY_CHK(ok.params.filter_start_object == 0,
                                 "filter_start_object");
                        BODY_CHK(ok.params.filter_end_group == 0,
                                 "filter_end_group");
                        BODY_CHK(!ok.params.has_object_delivery_timeout,
                                 "has_object_delivery_timeout");
                        BODY_CHK(ok.params.object_delivery_timeout_ms == 0,
                                 "object_delivery_timeout_ms");
                        BODY_CHK(!ok.params.has_subgroup_delivery_timeout,
                                 "has_subgroup_delivery_timeout");
                        BODY_CHK(ok.params.subgroup_delivery_timeout_ms == 0,
                                 "subgroup_delivery_timeout_ms");
                        BODY_CHK(ok.params.auth_token_count == 0,
                                 "auth_token_count");
                        BODY_CHK(!ok.params.has_new_group_request,
                                 "has_new_group_request");
                        BODY_CHK(ok.params.new_group_request == 0,
                                 "new_group_request");
                        #undef BODY_CHK
                    }
                }
            } else if (act.kind == MOQ_ACTION_SEND_BIDI_STREAM &&
                       act.u.send_bidi_stream.stream_ref._v == ref._v) {
                on_fetch_ref++;
            } else {
                foreign++;
            }
            moq_action_cleanup(&act);
        }
        if (actions != 1 || on_sub != 1 || on_fetch_ref != 0 || foreign != 0 ||
            oks != 1 || undecodable != 0 || trailing != 0 || other_msg != 0 ||
            finned != 0 || null_payload != 0) {
            printf("FAIL: %s: actions=%d on_sub=%d on_fetch_ref=%d foreign=%d "
                   "oks=%d undecodable=%d trailing=%d other_msg=%d fin=%d "
                   "null_payload=%d\n",
                   title, actions, on_sub, on_fetch_ref, foreign, oks,
                   undecodable, trailing, other_msg, finned, null_payload);
            failures++;
        }
        if (body_bad) failures++;
    }

    /* The accepted subscription's stored Joining Location is unchanged by the
     * conversion. */
    MOQ_TEST_CHECK(se->has_largest);
    MOQ_TEST_CHECK_EQ_U64(se->largest_group, loc_g);
    MOQ_TEST_CHECK_EQ_U64(se->largest_object, loc_o);

    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_simpair_destroy(sp);
    ROW_END();
}

/* draft-18 5.1: "A publisher MUST save the Largest Location communicated in
 * SUBSCRIBE_OK, PUBLISH or REQUEST_UPDATE_OK that changes the Forward State
 * from 0 to 1."
 *
 * Both endpoints must obey it. The subscriber-role entry learns the location
 * from the ACK it RECEIVES; the publisher-role entry -- the one a later
 * publication-keyed Joining FETCH actually resolves on -- must latch the same
 * snapshot it ENCODED into that ACK. The row drives real messages end to end
 * and then proves the derived range, so a latch that disagreed with the wire
 * would show up as a wrong range rather than only as a wrong field. */
static void d18_forward_rise_latches_on_both_endpoints(void)
{
    ROW_BEGIN("draft-18 Forward 0->1 update latches on both endpoints");
    fix_t f;
    accept_opts_t ao = { .forward = false, .set_filter = false };
    if (!fixture_up_opt(&f, MOQ_VERSION_DRAFT_18, ao)) {
        MOQ_TEST_CHECK(false);
        fixture_down(&f);
        ROW_END();
        return;
    }
    moq_session_t *sv = moq_simpair_server(f.sp);
    moq_session_t *cl = moq_simpair_client(f.sp);

    /* 1. Established at Forward 0 with a Largest on the PUBLISH -- and with NO
     *    Joining Location, because 5.1 supplies one only on a rise to 1. */
    MOQ_TEST_CHECK(f.cons_has_lg);
    moq_pub_entry_t *pe_pub = sole_pub_entry(sv, MOQ_PUB_ROLE_PUBLISHER);
    moq_pub_entry_t *pe_sub = sole_pub_entry(cl, MOQ_PUB_ROLE_SUBSCRIBER);
    MOQ_TEST_CHECK(pe_pub != NULL);
    MOQ_TEST_CHECK(pe_sub != NULL);
    if (!pe_pub || !pe_sub) { fixture_down(&f); ROW_END(); return; }
    MOQ_TEST_CHECK(!pe_pub->has_joining_loc);
    MOQ_TEST_CHECK(!pe_sub->has_joining_loc);
    /* The establishment snapshot the PUBLISH carried, which no update may
     * rewrite. Captured now so step 4 can prove it survived. */
    const bool     est_has = pe_pub->publish_has_largest;
    const uint64_t est_g = pe_pub->publish_largest_group;
    const uint64_t est_o = pe_pub->publish_largest_object;
    MOQ_TEST_CHECK(est_has);
    /* The subscriber-role entry keeps its OWN establishment record, and the
     * relatch on that side must not rewrite it either. */
    const bool     sest_has = pe_sub->publish_has_largest;
    const uint64_t sest_g = pe_sub->publish_largest_group;
    const uint64_t sest_o = pe_sub->publish_largest_object;
    MOQ_TEST_CHECK(sest_has);

    /* Advance the publisher's history PAST the establishment snapshot, so a
     * latch that re-read moving history instead of using the ACK snapshot
     * would be visibly wrong -- and so would one that reused the PUBLISH
     * snapshot. All three candidate values are now distinct.
     *
     * Deliberately WITHIN the retained group: a relative offset of 0 derives
     * a start of {Joining Location.Group - 0, 0}, so advancing into a later
     * GROUP would put the retained group outside the derived range and the
     * facade would decline for a reason that has nothing to do with the
     * latch. Advancing the OBJECT keeps step 5 a test of the range. */
    {
        uint8_t body[] = { 'u', 'p', 'd' };
        moq_rcbuf_t *pay = NULL;
        if (moq_rcbuf_create(moq_alloc_default(), body, sizeof(body), &pay)
            == MOQ_OK) {
            moq_pub_object_cfg_t po;
            moq_pub_object_cfg_init_sized(&po, sizeof(po));
            po.group_id = 0;
            po.object_id = 9;
            po.payload = pay;
            (void)moq_pub_write_object_ex(f.pub, f.track, &po,
                                          moq_simpair_now_us(f.sp));
            moq_rcbuf_decref(pay);
        }
        moq_simpair_run_until_quiescent(f.sp, 16, NULL);
        (void)moq_pub_tick(f.pub, moq_simpair_now_us(f.sp));
        moq_simpair_run_until_quiescent(f.sp, 16, NULL);
    }
    /* Drain anything queued so far; the ACK is counted from a clean slate. */
    { bool hl=false; uint64_t g=0,o=0; (void)take_update_ok(&f,&hl,&g,&o); }

    /* 2 + 3. A real REQUEST_UPDATE changing Forward to 1, and the real ACK. */
    MOQ_TEST_CHECK(update_publication_forward(&f, true));
    bool ack_has = false; uint64_t ack_g = 0, ack_o = 0;
    MOQ_TEST_CHECK_EQ_INT(take_update_ok(&f, &ack_has, &ack_g, &ack_o), 1);
    MOQ_TEST_CHECK(ack_has);

    /* 4. BOTH entries latched EXACTLY what the ACK carried -- and neither
     *    rewrote the PUBLISH establishment record to do it. */
    MOQ_TEST_CHECK(pe_pub->has_joining_loc);
    MOQ_TEST_CHECK_EQ_U64(pe_pub->joining_group, ack_g);
    MOQ_TEST_CHECK_EQ_U64(pe_pub->joining_object, ack_o);
    MOQ_TEST_CHECK(pe_sub->has_joining_loc);
    MOQ_TEST_CHECK_EQ_U64(pe_sub->joining_group, ack_g);
    MOQ_TEST_CHECK_EQ_U64(pe_sub->joining_object, ack_o);
    MOQ_TEST_CHECK_EQ_U64(pe_pub->publish_has_largest ? 1 : 0, est_has ? 1 : 0);
    MOQ_TEST_CHECK_EQ_U64(pe_pub->publish_largest_group, est_g);
    MOQ_TEST_CHECK_EQ_U64(pe_pub->publish_largest_object, est_o);
    MOQ_TEST_CHECK_EQ_U64(pe_sub->publish_has_largest ? 1 : 0, sest_has ? 1 : 0);
    MOQ_TEST_CHECK_EQ_U64(pe_sub->publish_largest_group, sest_g);
    MOQ_TEST_CHECK_EQ_U64(pe_sub->publish_largest_object, sest_o);
    /* The ACK carried the CURRENT largest, which the write above moved past
     * the establishment snapshot -- so "latched the ACK" and "reused the
     * PUBLISH snapshot" are genuinely different answers here. */
    MOQ_TEST_CHECK(ack_g != est_g || ack_o != est_o);

    /* 5. The join resolves on the publisher-role entry and derives its range
     *    from that latched location.
     *
     *    The range is read from the FETCH_REQUEST the PUBLISHER received,
     *    polled from the session before the facade's tick consumes it. That is
     *    the derived range EXACTLY. A FETCH_OK's end would not be: draft-18
     *    10.13 lets the response end shrink to where published data actually
     *    ends, so asserting on it would test the retained cache's extent, not
     *    the derivation under test. (The served path is row 1's job.) */
    moq_fetch_cfg_t fc;
    moq_fetch_cfg_init_sized(&fc, sizeof(fc));
    fc.is_joining = true;
    fc.joining_relative = true;
    fc.joining_start = 0;
    fc.joining_pub = f.cons_pub;
    moq_fetch_t fh = MOQ_FETCH_INVALID;
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_fetch(cl, &fc, moq_simpair_now_us(f.sp), &fh),
        (int)MOQ_OK);
    moq_simpair_run_until_quiescent(f.sp, 16, NULL);

    int n_req = 0;
    uint64_t rq_sg = 0, rq_so = 0, rq_eg = 0, rq_eo = 0;
    bool rq_named_pub = false;
    moq_event_t ev;
    while (moq_session_poll_events(sv, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_FETCH_REQUEST) {
            n_req++;
            rq_sg = ev.u.fetch_request.start_group;
            rq_so = ev.u.fetch_request.start_object;
            rq_eg = ev.u.fetch_request.end_group;
            rq_eo = ev.u.fetch_request.end_object;
            rq_named_pub =
                moq_publication_is_valid(ev.u.fetch_request.joining_pub);
        }
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK_EQ_INT(n_req, 1);
    /* 10.12.2.1: a Relative start is {Joining Location.Group - offset, 0} and
     * End is {Joining Location.Group, Joining Location.Object + 1}. */
    MOQ_TEST_CHECK_EQ_U64(rq_sg, ack_g);
    MOQ_TEST_CHECK_EQ_U64(rq_so, 0);
    MOQ_TEST_CHECK_EQ_U64(rq_eg, ack_g);
    MOQ_TEST_CHECK_EQ_U64(rq_eo, ack_o + 1);
    /* The event surfaces the publication that was joined, not a subscription. */
    MOQ_TEST_CHECK(rq_named_pub);

    fixture_down(&f);
    ROW_END();
}

/* draft-16 5.1 saves the Largest communicated in PUBLISH or SUBSCRIBE_OK WHEN
 * ESTABLISHING the subscription. It states no REQUEST_UPDATE_OK
 * Forward-transition rule, so a later update -- or a later history advance --
 * must NOT move a draft-16 Joining Location. The row exists because the
 * update-driven relatch is generic code shared by both profiles; without the
 * profile gate it would silently move this one. */
static void d16_update_does_not_move_joining_location(void)
{
    ROW_BEGIN("draft-16 update leaves the establishment location alone");
    fix_t f;
    /* Forward 0 at establishment, so the update below is a genuine 0 -> 1
     * RISE -- the exact transition draft-18 acts on and draft-16 does not.
     * Under draft-16 eligibility is the LARGEST_OBJECT filter, not Forward,
     * so the location is latched at establishment regardless. Establishing at
     * Forward 1 would leave nothing to rise and the row would pass whether or
     * not the profile gate existed. */
    accept_opts_t ao = { .forward = false, .set_filter = true,
                         .filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT };
    if (!fixture_up_opt(&f, MOQ_VERSION_DRAFT_16, ao)) {
        MOQ_TEST_CHECK(false);
        fixture_down(&f);
        ROW_END();
        return;
    }
    moq_session_t *sv = moq_simpair_server(f.sp);
    moq_session_t *cl = moq_simpair_client(f.sp);
    moq_pub_entry_t *pe_pub = sole_pub_entry(sv, MOQ_PUB_ROLE_PUBLISHER);
    moq_pub_entry_t *pe_sub = sole_pub_entry(cl, MOQ_PUB_ROLE_SUBSCRIBER);
    MOQ_TEST_CHECK(pe_pub != NULL);
    MOQ_TEST_CHECK(pe_sub != NULL);
    if (!pe_pub || !pe_sub) { fixture_down(&f); ROW_END(); return; }

    /* Established under LARGEST_OBJECT: the establishment location is latched. */
    MOQ_TEST_CHECK(pe_pub->has_joining_loc);
    MOQ_TEST_CHECK(pe_sub->has_joining_loc);
    const uint64_t g0 = pe_pub->joining_group, o0 = pe_pub->joining_object;
    const uint64_t sg0 = pe_sub->joining_group, so0 = pe_sub->joining_object;

    /* Advance history well past it. */
    {
        uint8_t body[] = { 'm', 'v' };
        moq_rcbuf_t *pay = NULL;
        if (moq_rcbuf_create(moq_alloc_default(), body, sizeof(body), &pay)
            == MOQ_OK) {
            moq_pub_object_cfg_t po;
            moq_pub_object_cfg_init_sized(&po, sizeof(po));
            po.group_id = 7;
            po.object_id = 3;
            po.payload = pay;
            (void)moq_pub_write_object_ex(f.pub, f.track, &po,
                                          moq_simpair_now_us(f.sp));
            moq_rcbuf_decref(pay);
        }
        moq_simpair_run_until_quiescent(f.sp, 16, NULL);
        (void)moq_pub_tick(f.pub, moq_simpair_now_us(f.sp));
        moq_simpair_run_until_quiescent(f.sp, 16, NULL);
    }
    MOQ_TEST_CHECK(pe_pub->hist != NULL);
    if (pe_pub->hist) {
        /* The history really did move -- otherwise the assertions below would
         * hold for the wrong reason. */
        MOQ_TEST_CHECK(pe_pub->hist->has_largest);
        MOQ_TEST_CHECK(pe_pub->hist->largest_group != g0 ||
                       pe_pub->hist->largest_object != o0);
    }

    /* A real publication update raising Forward 0 -> 1. Draft-18 would relatch
     * here; draft-16 has no such rule, so the location must not move on either
     * endpoint. */
    MOQ_TEST_CHECK(!pe_pub->send_allowed);
    MOQ_TEST_CHECK(update_publication_forward(&f, true));
    MOQ_TEST_CHECK(pe_pub->send_allowed);   /* the rise really happened */
    { bool hl=false; uint64_t g=0,o=0; (void)take_update_ok(&f,&hl,&g,&o); }

    MOQ_TEST_CHECK(pe_pub->has_joining_loc);
    MOQ_TEST_CHECK_EQ_U64(pe_pub->joining_group, g0);
    MOQ_TEST_CHECK_EQ_U64(pe_pub->joining_object, o0);
    MOQ_TEST_CHECK(pe_sub->has_joining_loc);
    MOQ_TEST_CHECK_EQ_U64(pe_sub->joining_group, sg0);
    MOQ_TEST_CHECK_EQ_U64(pe_sub->joining_object, so0);

    fixture_down(&f);
    ROW_END();
}

/* A sized initializer that writes past what the caller supplied is the one
 * failure it exists to prevent -- so the proof must be able to OBSERVE such a
 * write rather than be destroyed by it.
 *
 * Backing storage is therefore always at least
 *   max(offer, sizeof(moq_fetch_cfg_t)) + guard
 * while `offer` is the LOGICAL caller size handed to the initializer. A
 * correct implementation writes at most min(offer, sizeof(cfg)) bytes; an
 * upward-clamping one writes up to the v0 floor. Both stay inside memory this
 * function owns, so the mutant fails on a named sentinel byte instead of
 * corrupting the heap -- a non-crashing run of undefined behaviour is not a
 * clean kill. The row also runs under ASan, which would catch a write past
 * even this larger allocation. */
static void sized_init_case(const char *what, size_t offer, size_t expect_stamp,
                            bool expect_stamped)
{
    const size_t guard = 16;
    const size_t logical = offer;
    const size_t owned = (offer > sizeof(moq_fetch_cfg_t)
                              ? offer : sizeof(moq_fetch_cfg_t));
    const size_t alloc = owned + guard;
    uint8_t *raw = malloc(alloc);
    if (!raw) { MOQ_TEST_CHECK(false); return; }
    memset(raw, 0xAA, alloc);

    moq_fetch_cfg_init_sized((moq_fetch_cfg_t *)raw, logical);

    /* Everything the initializer is permitted to touch. */
    const size_t writable = logical < sizeof(moq_fetch_cfg_t)
                                ? logical : sizeof(moq_fetch_cfg_t);
    if (expect_stamped) {
        uint32_t ssz = 0;
        memcpy(&ssz, raw + offsetof(moq_fetch_cfg_t, struct_size), sizeof(ssz));
        if (ssz != (uint32_t)expect_stamp) {
            printf("FAIL: %s: struct_size %u, expected %zu\n",
                   what, (unsigned)ssz, expect_stamp);
            failures++;
        }
    } else {
        /* Too small even to stamp: NOTHING may be written, including the
         * bytes the caller did supply. */
        for (size_t i = 0; i < logical; i++) {
            if (raw[i] != 0xAA) {
                printf("FAIL: %s: byte %zu of caller storage was written\n",
                       what, i);
                failures++;
                break;
            }
        }
    }
    /* Every byte past the logical writable prefix must survive -- including
     * the region an upward clamp would reach into. */
    for (size_t i = writable; i < alloc; i++) {
        if (raw[i] != 0xAA) {
            printf("FAIL: %s: byte %zu past the writable prefix was written "
                   "(offer=%zu writable=%zu owned=%zu alloc=%zu)\n",
                   what, i, logical, writable, owned, alloc);
            failures++;
            break;
        }
    }
    free(raw);
}

/* The frozen v0 layout is the pointer-only init floor, and the sized form must
 * respect the caller's allocation exactly. */
/* Drive a standalone FETCH through the real public call with the given cfg and
 * classify the OUTCOME, so an MOQ_OK produced by an empty fixture cannot pass
 * for acceptance. Returns the result; on success the caller checks what the
 * session actually did with the appended fields. */
static moq_result_t abi_try_fetch(fix_t *f, moq_fetch_cfg_t *cfg,
                                  moq_fetch_t *out)
{
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("live") };
    cfg->track_namespace = (moq_namespace_t){ ns, 1 };
    cfg->track_name = MOQ_BYTES_LITERAL("catalog");
    cfg->start_group = 0;
    cfg->start_object = 0;
    cfg->end_group = 0;
    cfg->end_object = 1;
    *out = MOQ_FETCH_INVALID;
    return moq_session_fetch(moq_simpair_client(f->sp), cfg,
                             moq_simpair_now_us(f->sp), out);
}

/* ---- complete peer-queue classification for the ABI rows -------------- *
 * Counting tokens on "the last matching request" adopts post-call state, is
 * blind to foreign events, and accepts duplicates. These rows instead declare
 * what the peer must receive and classify the WHOLE queue once. */
typedef struct {
    int  events;        /* every event, any kind */
    int  reqs;          /* MOQ_EVENT_FETCH_REQUEST */
    int  foreign;       /* anything else */
    /* Captured from the sole request, under a kind guard, without any
     * dereference unless the count/pointer preconditions hold. */
    int  token_count;   /* -1 = no request seen */
    uint64_t tok_type;
    size_t   tok_len;
    bool     tok_null;      /* nonempty token with a NULL data pointer */
    bool     tok_bytes_ok;
    bool     names_pub;
    bool     names_sub;
    moq_publication_t got_pub;
} abi_peer_obs_t;

static void abi_observe_peer(fix_t *f, abi_peer_obs_t *o)
{
    memset(o, 0, sizeof(*o));
    o->token_count = -1;
    moq_simpair_run_until_quiescent(f->sp, 16, NULL);
    for (;;) {
        moq_event_t ev;
        memset(&ev, 0, sizeof(ev));
        if (moq_session_poll_events(moq_simpair_server(f->sp), &ev, 1) != 1)
            break;
        o->events++;
        if (ev.kind == MOQ_EVENT_FETCH_REQUEST) {
            o->reqs++;
            o->token_count = (int)ev.u.fetch_request.token_count;
            o->names_pub =
                moq_publication_is_valid(ev.u.fetch_request.joining_pub);
            o->names_sub =
                moq_subscription_is_valid(ev.u.fetch_request.joining_sub);
            o->got_pub = ev.u.fetch_request.joining_pub;
            if (o->token_count == 1) {
                if (!ev.u.fetch_request.tokens) {
                    o->tok_null = true;
                } else {
                    o->tok_type = ev.u.fetch_request.tokens[0].token_type;
                    o->tok_len = ev.u.fetch_request.tokens[0].token_value.len;
                    if (o->tok_len > 0 &&
                        !ev.u.fetch_request.tokens[0].token_value.data)
                        o->tok_null = true;
                }
            }
        } else {
            o->foreign++;
        }
        moq_event_cleanup(&ev);
    }
}

/* Same, additionally comparing the sole token's bytes against `want`. Kept
 * separate so the byte compare happens while the borrow is still live. */
static void abi_observe_peer_tok(fix_t *f, abi_peer_obs_t *o,
                                 const uint8_t *want, size_t want_len)
{
    memset(o, 0, sizeof(*o));
    o->token_count = -1;
    moq_simpair_run_until_quiescent(f->sp, 16, NULL);
    for (;;) {
        moq_event_t ev;
        memset(&ev, 0, sizeof(ev));
        if (moq_session_poll_events(moq_simpair_server(f->sp), &ev, 1) != 1)
            break;
        o->events++;
        if (ev.kind == MOQ_EVENT_FETCH_REQUEST) {
            o->reqs++;
            o->token_count = (int)ev.u.fetch_request.token_count;
            o->names_pub =
                moq_publication_is_valid(ev.u.fetch_request.joining_pub);
            o->names_sub =
                moq_subscription_is_valid(ev.u.fetch_request.joining_sub);
            o->got_pub = ev.u.fetch_request.joining_pub;
            if (o->token_count == 1) {
                if (!ev.u.fetch_request.tokens) {
                    o->tok_null = true;
                } else {
                    o->tok_type = ev.u.fetch_request.tokens[0].token_type;
                    o->tok_len = ev.u.fetch_request.tokens[0].token_value.len;
                    if (o->tok_len != want_len) {
                        /* length mismatch: nothing to compare */
                    } else if (!ev.u.fetch_request.tokens[0].token_value.data) {
                        o->tok_null = true;
                    } else {
                        o->tok_bytes_ok = memcmp(
                            ev.u.fetch_request.tokens[0].token_value.data,
                            want, want_len) == 0;
                    }
                }
            }
        } else {
            o->foreign++;
        }
        moq_event_cleanup(&ev);
    }
}

/* Every ABI acceptance row owes exactly one request and nothing else. */
static void abi_check_sole_request(const abi_peer_obs_t *o, const char *what)
{
    if (o->events != 1 || o->reqs != 1 || o->foreign != 0) {
        printf("FAIL: %s: peer events=%d fetch_requests=%d foreign=%d, "
               "expected exactly one request\n",
               what, o->events, o->reqs, o->foreign);
        failures++;
    }
}

/* The PUBLISHER-side publication the peer must name, declared from fixture
 * state BEFORE the FETCH. Exact multiplicity: ambiguity is a failure, not a
 * first match. */
static bool abi_declare_peer_pub(moq_session_t *sv, moq_publication_t *out,
                                 const char *what)
{
    moq_pub_entry_t *pe = sole_pub_entry(sv, MOQ_PUB_ROLE_PUBLISHER);
    if (!pe) {
        printf("FAIL: %s: no single publisher-role publication to declare\n",
               what);
        failures++;
        return false;
    }
    *out = pe->handle;
    return moq_publication_is_valid(*out);
}

/* The layout boundary immediately BEFORE joining_pub. Deliberately NOT called
 * "v0": v0 is the ORIGINAL layout, which ends before auth_tokens. This is a
 * private test constant naming the pre-joining-publication size, used only to
 * offer a size that reaches the auth-token fields but not joining_pub. */
#define TEST_FETCH_CFG_PRE_JOINING_PUB_SIZE \
    offsetof(moq_fetch_cfg_t, joining_pub)

static void abi_floor_init_bounds(void)
{
    ROW_BEGIN("fetch cfg init respects the caller's allocation");

    /* The frozen v0 prefix ends at the FIRST appended field, auth_tokens --
     * not at joining_pub. auth_tokens/auth_token_count were themselves
     * appended after the original layout (they are labelled so on the base
     * commit, and the pre-existing consumer minimum was offsetof(auth_tokens)),
     * and joining_pub after them. Also asserted at compile time in
     * session_fetch.c; stated here because the rows below depend on it. */
    MOQ_TEST_CHECK_EQ_U64((uint64_t)MOQ_FETCH_CFG_V0_SIZE,
                          (uint64_t)offsetof(moq_fetch_cfg_t, auth_tokens));
    MOQ_TEST_CHECK(MOQ_FETCH_CFG_V0_SIZE <
                   TEST_FETCH_CFG_PRE_JOINING_PUB_SIZE);
    MOQ_TEST_CHECK(TEST_FETCH_CFG_PRE_JOINING_PUB_SIZE <
                   sizeof(moq_fetch_cfg_t));

    /* (1) Pointer-only init on an allocation of exactly the ORIGINAL layout,
     *     with a guard immediately after it.
     *
     *     The allocation is sized from the LAYOUT FACT, offsetof(auth_tokens),
     *     NOT from MOQ_FETCH_CFG_V0_SIZE. That is deliberate: sizing it from
     *     the macro under test would grow the buffer in step with an enlarged
     *     floor and the guard could never fire. Anchored on the layout, an
     *     initializer that writes past the original prefix -- exactly what an
     *     enlarged floor causes -- lands in the sentinel and is named.
     *
     *     Backing storage is the full struct plus guard so such a write stays
     *     inside memory this test owns (a non-crashing overrun is still
     *     undefined behaviour); `orig` is the LOGICAL caller allocation. */
    const size_t orig = offsetof(moq_fetch_cfg_t, auth_tokens);
    const size_t guard = 16;
    const size_t owned = sizeof(moq_fetch_cfg_t) + guard;
    uint8_t *raw = malloc(owned);
    if (!raw) { ROW_END(); return; }
    memset(raw, 0xAA, owned);
    moq_fetch_cfg_init((moq_fetch_cfg_t *)raw);
    uint32_t ssz = 0;
    memcpy(&ssz, raw + offsetof(moq_fetch_cfg_t, struct_size), sizeof(ssz));
    MOQ_TEST_CHECK_EQ_U64((uint64_t)ssz, (uint64_t)orig);
    for (size_t i = orig; i < owned; i++) {
        if (raw[i] != 0xAA) {
            printf("FAIL: pointer-only init wrote byte %zu past the original "
                   "prefix (orig=%zu owned=%zu)\n", i, orig, owned);
            failures++;
            break;
        }
    }
    free(raw);

    /* (6) Too-small-to-stamp and oversized offers, plus the sentinel-backed
     *     bounds proof across the whole range. */
    const size_t stamp = sizeof(((moq_fetch_cfg_t *)0)->struct_size);
    sized_init_case("offer 0",         0,         0,     false);
    sized_init_case("offer 1",         1,         0,     false);
    sized_init_case("offer stamp-1",   stamp - 1, 0,     false);
    sized_init_case("offer stamp",     stamp,     stamp, true);
    sized_init_case("offer v0-1",      MOQ_FETCH_CFG_V0_SIZE - 1,
                                       MOQ_FETCH_CFG_V0_SIZE - 1, true);
    sized_init_case("offer v0",        MOQ_FETCH_CFG_V0_SIZE,
                                       MOQ_FETCH_CFG_V0_SIZE, true);
    sized_init_case("offer pre-joining_pub",
                                       TEST_FETCH_CFG_PRE_JOINING_PUB_SIZE,
                                       TEST_FETCH_CFG_PRE_JOINING_PUB_SIZE,
                                       true);
    sized_init_case("offer full",      sizeof(moq_fetch_cfg_t),
                                       sizeof(moq_fetch_cfg_t), true);
    sized_init_case("offer oversized", sizeof(moq_fetch_cfg_t) * 4,
                                       sizeof(moq_fetch_cfg_t), true);

    /* Now the CONSUMING behaviour at each boundary, through the real public
     * call. Each fixture is fresh so one row's request cannot colour another. */
    uint8_t tok_val[] = { 0xBE, 0xEF };
    moq_auth_token_t tok = { .token_type = 7,
                             .token_value = { tok_val, sizeof(tok_val) } };

    /* (2) V0 - 1: the initializer stays bounds-safe (proven above), and the
     *     consuming call REJECTS -- such a stamp cannot describe even the
     *     original layout. */
    {
        fix_t f; memset(&f, 0, sizeof(f));
        if (fixture_up(&f, MOQ_VERSION_DRAFT_18)) {
            moq_fetch_cfg_t c; moq_fetch_t h;
            moq_fetch_cfg_init_sized(&c, MOQ_FETCH_CFG_V0_SIZE - 1);
            MOQ_TEST_CHECK_EQ_INT((int)abi_try_fetch(&f, &c, &h),
                                  (int)MOQ_ERR_INVAL);
            MOQ_TEST_CHECK(!moq_fetch_is_valid(h));
            fixture_down(&f);
        }
    }
    /* Too small even to stamp: still rejected by the consuming call. */
    {
        fix_t f; memset(&f, 0, sizeof(f));
        if (fixture_up(&f, MOQ_VERSION_DRAFT_18)) {
            moq_fetch_cfg_t c; moq_fetch_t h;
            memset(&c, 0, sizeof(c));
            moq_fetch_cfg_init_sized(&c, stamp);
            MOQ_TEST_CHECK_EQ_U64(c.struct_size, (uint64_t)stamp);
            MOQ_TEST_CHECK_EQ_INT((int)abi_try_fetch(&f, &c, &h),
                                  (int)MOQ_ERR_INVAL);
            fixture_down(&f);
        }
    }

    /* (3) Exactly V0: a valid prefix-only FETCH is ACCEPTED, and the peer
     *     really receives it -- classified by output, so an empty fixture
     *     cannot masquerade as acceptance. */
    {
        fix_t f; memset(&f, 0, sizeof(f));
        if (fixture_up(&f, MOQ_VERSION_DRAFT_18)) {
            moq_fetch_cfg_t c; moq_fetch_t h;
            moq_fetch_cfg_init_sized(&c, MOQ_FETCH_CFG_V0_SIZE);
            MOQ_TEST_CHECK_EQ_INT((int)abi_try_fetch(&f, &c, &h), (int)MOQ_OK);
            MOQ_TEST_CHECK(moq_fetch_is_valid(h));
            abi_peer_obs_t o;
            abi_observe_peer(&f, &o);
            abi_check_sole_request(&o, "abi exact-v0");
            MOQ_TEST_CHECK_EQ_INT(o.token_count, 0);   /* exactly zero */
            MOQ_TEST_CHECK(!o.names_pub);              /* standalone fetch */
            MOQ_TEST_CHECK(!o.names_sub);
            fixture_down(&f);
        }
    }

    /* (4) Through auth_token_count: the appended tokens are REACHABLE and
     *     arrive, while joining_pub stays absent -- setting it is ignored,
     *     so a joining fetch naming only it is "neither". */
    {
        fix_t f; memset(&f, 0, sizeof(f));
        if (fixture_up(&f, MOQ_VERSION_DRAFT_18)) {
            moq_fetch_cfg_t c; moq_fetch_t h;
            moq_fetch_cfg_init_sized(&c,
                TEST_FETCH_CFG_PRE_JOINING_PUB_SIZE);
            c.auth_tokens = &tok;
            c.auth_token_count = 1;
            MOQ_TEST_CHECK_EQ_INT((int)abi_try_fetch(&f, &c, &h), (int)MOQ_OK);
            MOQ_TEST_CHECK(moq_fetch_is_valid(h));
            abi_peer_obs_t o;
            abi_observe_peer_tok(&f, &o, tok_val, sizeof(tok_val));
            abi_check_sole_request(&o, "abi pre-joining_pub");
            /* Exactly one token, with the INDEPENDENTLY DECLARED type, length,
             * non-NULL value pointer and bytes. */
            MOQ_TEST_CHECK_EQ_INT(o.token_count, 1);
            MOQ_TEST_CHECK(!o.tok_null);
            MOQ_TEST_CHECK_EQ_U64(o.tok_type, 7);
            MOQ_TEST_CHECK_EQ_SIZE(o.tok_len, sizeof(tok_val));
            MOQ_TEST_CHECK(o.tok_bytes_ok);
            fixture_down(&f);
        }
        /* joining_pub is out of reach at this size. */
        memset(&f, 0, sizeof(f));
        if (fixture_up(&f, MOQ_VERSION_DRAFT_18)) {
            moq_fetch_cfg_t c; moq_fetch_t h;
            moq_fetch_cfg_init_sized(&c,
                TEST_FETCH_CFG_PRE_JOINING_PUB_SIZE);
            c.is_joining = true; c.joining_relative = true;
            c.joining_pub = f.cons_pub;
            MOQ_TEST_CHECK_EQ_INT((int)abi_try_fetch(&f, &c, &h),
                                  (int)MOQ_ERR_INVAL);   /* neither named */
            fixture_down(&f);
        }
    }

    /* (5) Full size: joining_pub is reachable and a publication join works. */
    {
        fix_t f; memset(&f, 0, sizeof(f));
        if (fixture_up(&f, MOQ_VERSION_DRAFT_18)) {
            moq_fetch_cfg_t c; moq_fetch_t h;
            moq_fetch_cfg_init_sized(&c, sizeof(c));
            MOQ_TEST_CHECK_EQ_U64(c.struct_size, (uint64_t)sizeof(c));
            MOQ_TEST_CHECK(!moq_publication_is_valid(c.joining_pub));
            MOQ_TEST_CHECK(!moq_subscription_is_valid(c.joining_sub));
            c.is_joining = true; c.joining_relative = true;
            c.joining_start = 0;
            c.joining_pub = f.cons_pub;
            /* Declared BEFORE the call, from the peer's own pool state. */
            moq_publication_t want_pub = MOQ_PUBLICATION_INVALID;
            const bool have_pub = abi_declare_peer_pub(
                moq_simpair_server(f.sp), &want_pub, "abi full-size");
            MOQ_TEST_CHECK_EQ_INT((int)abi_try_fetch(&f, &c, &h), (int)MOQ_OK);
            MOQ_TEST_CHECK(moq_fetch_is_valid(h));
            abi_peer_obs_t o;
            abi_observe_peer(&f, &o);
            abi_check_sole_request(&o, "abi full-size");
            /* MOQ_OK plus a valid LOCAL handle proves nothing about the
             * appended discriminator: the peer must name the exact expected
             * PUBLICATION, and no subscription. */
            MOQ_TEST_CHECK(o.names_pub);
            MOQ_TEST_CHECK(!o.names_sub);
            if (have_pub)
                MOQ_TEST_CHECK(moq_publication_eq(o.got_pub, want_pub));
            fixture_down(&f);
        }
    }

    /* NULL is tolerated by both forms. */
    moq_fetch_cfg_init(NULL);
    moq_fetch_cfg_init_sized(NULL, sizeof(moq_fetch_cfg_t));

    ROW_END();
}

int main(void)
{
    consumer_joins_publication();
    publisher_resolves_publication_join();
    control_subscribe_origin_resolves();
    d16_filtered_join();
    d16_non_largest_filter_is_protocol_error();
    gate_pending_publisher_is_invalid_joining_id();
    gate_forward_zero_is_invalid_range();
    reuse_leaves_no_stale_joining_location();
    d18_forward_rise_latches_on_both_endpoints();
    d16_update_does_not_move_joining_location();
    pub_join_end_at(MOQ_VERSION_DRAFT_18, "draft-18", true);
    pub_join_end_at(MOQ_VERSION_DRAFT_18, "draft-18", false);
    pub_join_end_at(MOQ_VERSION_DRAFT_16, "draft-16", true);
    pub_join_end_at(MOQ_VERSION_DRAFT_16, "draft-16", false);
    sub_join_end_at(MOQ_VERSION_DRAFT_18, "draft-18", true);
    sub_join_end_at(MOQ_VERSION_DRAFT_18, "draft-18", false);
    sub_join_end_at(MOQ_VERSION_DRAFT_16, "draft-16", true);
    sub_join_end_at(MOQ_VERSION_DRAFT_16, "draft-16", false);
    pending_join_release_end_at(true);
    pending_join_release_end_at(false);
    d18_absolute_join_past_location();
    d16_absolute_join_past_location();
    abi_floor_init_bounds();
    printf("%s: %d failures\n", failures ? "FAIL" : "PASS", failures);
    return failures ? 1 : 0;
}
