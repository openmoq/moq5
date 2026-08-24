/*
 * Draft-18 SUBSCRIBE_NAMESPACE (§10.18) reusing the namespace-sub machinery via
 * the generic request-bidi staging + one-way handoff. Covers: wire codec
 * round-trips and rejection (SUBSCRIBE_NAMESPACE / NAMESPACE / NAMESPACE_DONE),
 * inbound surfacing (single-read and byte-by-byte fragmented), the handoff
 * registry invariant (idx_ns_by_ref owns the bidi, the stale staging key is
 * reclaimed), the outbound subscriber response path (REQUEST_OK then
 * NAMESPACE / NAMESPACE_DONE), accept/reject, PREFIX_OVERLAP, the session-close
 * protocol guards vs the bidi-only REQUEST_UPDATE guard, auth reject routing,
 * and end-to-end accept + delivery + reject over SimPair.
 */
#include <moq/moq.h>
#include <moq/control_d18.h>
#include <moq/sim.h>
#include "test_support.h"
#include "../support/failpoint.h"
#include "../support/txn_snapshot.h"
#include "../../core/src/session/session_internal.h"

/* Non-exported membership-work probe (session_namespace_sub.c, gated by
 * MOQ_SESSION_SWEEP_TESTING; resolved from moq-core-test-internals). Shared by
 * the inbound and outbound suffix sets; these tests read its DELTA around a
 * controlled inbound workload. */
extern uint64_t session_ns_suffix_probe_count;

static moq_session_t *make_session(moq_perspective_t persp);   /* defined below */

static uint64_t ns_ceil_log2(uint64_t v)
{
    uint64_t lg = 0;
    while (((uint64_t)1 << lg) < v) lg++;
    return lg;
}

/* Exact match of one polled event against (kind, handle, single-part suffix). */
static int ns_event_is(const moq_event_t *ev, unsigned kind,
                       moq_ns_sub_handle_t h, const uint8_t *sb, size_t sblen)
{
    /* Reject an unsupported expected kind before touching a union member. */
    if (kind != MOQ_EVENT_NAMESPACE_FOUND && kind != MOQ_EVENT_NAMESPACE_GONE)
        return 0;
    if (ev->kind != kind) return 0;
    moq_ns_sub_handle_t eh;
    moq_namespace_t sfx;
    if (kind == MOQ_EVENT_NAMESPACE_GONE) {
        eh = ev->u.namespace_gone.handle;
        sfx = ev->u.namespace_gone.track_namespace_suffix;
    } else {
        eh = ev->u.namespace_found.handle;
        sfx = ev->u.namespace_found.track_namespace_suffix;
    }
    if (eh._opaque != h._opaque) return 0;
    if (sfx.count != 1) return 0;
    if (sfx.parts == NULL) return 0;                 /* NULL-safe before deref */
    if (sfx.parts[0].len != sblen) return 0;
    if (sblen != 0 && sfx.parts[0].data == NULL) return 0;
    return sblen == 0 || memcmp(sfx.parts[0].data, sb, sblen) == 0;
}

/* Feed one NAMESPACE / NAMESPACE_DONE for a 4-byte single-part suffix and
 * require exactly one matching event, no unrelated output, session ESTABLISHED.
 * Returns failure count. */
static int ns_feed_one(moq_session_t *s, moq_stream_ref_t ref,
                       moq_ns_sub_handle_t h, const uint8_t *sb, int is_done,
                       unsigned expect_kind)
{
    int failures = 0;
    moq_bytes_t sp = { sb, 4 };
    moq_namespace_t suffix = { &sp, 1 };
    uint8_t nm[64];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, nm, sizeof(nm));
    moq_result_t erc = moq_d18_encode_namespace_msg(&w, &suffix, is_done != 0);
    MOQ_TEST_CHECK_EQ_INT((int)erc, (int)MOQ_OK);
    if (erc != MOQ_OK) return failures;            /* never feed invalid bytes */
    moq_result_t frc = moq_session_on_bidi_stream_bytes(s, ref, nm,
        moq_buf_writer_offset(&w), false, 1);
    MOQ_TEST_CHECK_EQ_INT((int)frc, (int)MOQ_OK);
    if (frc != MOQ_OK) return failures;            /* never poll after a bad feed */

    /* This route emits no action: exactly one event, zero actions. Stop before
     * the event assertions if an unexpected action appears (clean it first). */
    moq_action_t act;
    int na = moq_session_poll_actions(s, &act, 1);
    MOQ_TEST_CHECK_EQ_INT(na, 0);
    if (na > 0) { moq_action_cleanup(&act); return failures; }

    moq_event_t ev;
    int got = moq_session_poll_events(s, &ev, 1);
    MOQ_TEST_CHECK_EQ_INT(got, 1);
    if (got == 1) {                                /* inspect only a real event */
        MOQ_TEST_CHECK(ns_event_is(&ev, expect_kind, h, sb, 4));
        moq_event_cleanup(&ev);
        int extra = moq_session_poll_events(s, &ev, 1);
        MOQ_TEST_CHECK_EQ_INT(extra, 0);           /* no more */
        if (extra > 0) moq_event_cleanup(&ev);     /* clean the unexpected one */
    }
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
    return failures;
}

/* Establish a d18 subscriber on an already-created session `s` to ESTABLISHED,
 * capturing exactly one bidi ref. Every step is gated on all prior requirements:
 * subscribe MOQ_OK; exactly one OPEN_BIDI with a nonzero ref and fin==false and
 * no unrelated action; REQUEST_OK encode+feed; zero actions; exactly one
 * NS_SUB_OK for the handle; no extra event; exactly ESTABLISHED. On any failure
 * it cleans everything actually polled, records the failure into *acc and
 * returns false so a caller never issues NAMESPACE bytes on an invalid
 * ref/owner. Does NOT own `s`. */
static bool ns_establish_on(moq_session_t *s, moq_ns_sub_handle_t *out_h,
                            moq_stream_ref_t *out_ref, int *acc)
{
    int failures = 0;
    moq_subscribe_namespace_cfg_t cfg;
    moq_subscribe_namespace_cfg_init(&cfg);
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    cfg.track_namespace_prefix = (moq_namespace_t){ parts, 1 };
    cfg.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
    moq_result_t src = moq_session_subscribe_namespace(s, &cfg, 1, out_h);
    MOQ_TEST_CHECK_EQ_INT((int)src, (int)MOQ_OK);
    if (src != MOQ_OK) goto fail;

    moq_stream_ref_t ref = moq_stream_ref_from_u64(0);
    int opens = 0, other = 0;
    bool open_fin = true;
    moq_action_t act;
    while (moq_session_poll_actions(s, &act, 1) > 0) {
        if (act.kind == MOQ_ACTION_OPEN_BIDI_STREAM) {
            opens++;
            ref = act.u.open_bidi_stream.stream_ref;
            open_fin = act.u.open_bidi_stream.fin;
        } else {
            other++;
        }
        moq_action_cleanup(&act);
    }
    MOQ_TEST_CHECK_EQ_INT(opens, 1);           /* exactly one bidi opened     */
    MOQ_TEST_CHECK_EQ_INT(other, 0);           /* no unrelated setup action   */
    MOQ_TEST_CHECK(ref._v != 0);               /* a real, nonzero stream ref  */
    MOQ_TEST_CHECK(open_fin == false);         /* send half stays open for the
                                                  response -- not a one-shot  */
    if (opens != 1 || other != 0 || ref._v == 0 || open_fin) goto fail;

    uint8_t ok[32];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, ok, sizeof(ok));
    moq_result_t erc = moq_d18_encode_request_ok(&w);
    MOQ_TEST_CHECK_EQ_INT((int)erc, (int)MOQ_OK);
    if (erc != MOQ_OK) goto fail;
    moq_result_t frc = moq_session_on_bidi_stream_bytes(s, ref, ok,
        moq_buf_writer_offset(&w), false, 1);
    MOQ_TEST_CHECK_EQ_INT((int)frc, (int)MOQ_OK);
    if (frc != MOQ_OK) goto fail;

    /* REQUEST_OK emits exactly one NS_SUB_OK and no action. */
    moq_action_t sa;
    int sna = moq_session_poll_actions(s, &sa, 1);
    MOQ_TEST_CHECK_EQ_INT(sna, 0);
    if (sna > 0) { moq_action_cleanup(&sa); goto fail; }
    moq_event_t ev;
    int got = moq_session_poll_events(s, &ev, 1);
    MOQ_TEST_CHECK_EQ_INT(got, 1);
    if (got != 1) goto fail;
    bool ev_ok = ((int)ev.kind == (int)MOQ_EVENT_NS_SUB_OK) &&
                 (ev.u.ns_sub_ok.handle._opaque == out_h->_opaque);
    MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_NS_SUB_OK);
    MOQ_TEST_CHECK(ev.u.ns_sub_ok.handle._opaque == out_h->_opaque);
    moq_event_cleanup(&ev);
    if (!ev_ok) goto fail;
    int extra = moq_session_poll_events(s, &ev, 1);
    MOQ_TEST_CHECK_EQ_INT(extra, 0);           /* no more */
    if (extra > 0) { moq_event_cleanup(&ev); goto fail; }
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
    if (s->state != MOQ_SESS_ESTABLISHED) goto fail;

    *out_ref = ref;
    if (acc) *acc += failures;
    return true;

fail:
    if (acc) *acc += failures;
    return false;
}

/* Create a fresh default-cap session and establish a subscriber on it. Owns the
 * session on success; destroys it and returns NULL on any establishment
 * failure. */
static moq_session_t *ns_establish_sub(moq_ns_sub_handle_t *out_h,
                                       moq_stream_ref_t *out_ref, int *acc)
{
    int failures = 0;
    moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
    MOQ_TEST_CHECK(s != NULL);
    if (!s) { if (acc) *acc += failures; return NULL; }
    if (acc) *acc += failures;
    if (!ns_establish_on(s, out_h, out_ref, acc)) {
        moq_session_destroy(s);
        return NULL;
    }
    return s;
}

/* Permanent probe-counter self-check: two distinct inbound suffixes drive at
 * least one key comparison (the 2nd insert scans the 1st). Deleting the probe
 * increment makes this fail locally by name, independent of the 1000-message
 * RED. Representation-neutral (a tree also probes >= 1 key here). */
static int test_ns_probe_selfcheck(void)
{
    int failures = 0;
    moq_ns_sub_handle_t h;
    moq_stream_ref_t ref;
    moq_session_t *s = ns_establish_sub(&h, &ref, &failures);
    if (!s) return failures;
    uint64_t p0 = session_ns_suffix_probe_count;
    uint8_t a[4] = { 0, 0, 0, 1 }, b[4] = { 0, 0, 0, 2 };
    failures += ns_feed_one(s, ref, h, a, 0, MOQ_EVENT_NAMESPACE_FOUND);
    failures += ns_feed_one(s, ref, h, b, 0, MOQ_EVENT_NAMESPACE_FOUND);
    /* No unsigned subtraction unless monotonicity is proven by control flow:
     * compute the delta only in the true branch. */
    if (session_ns_suffix_probe_count >= p0) {
        uint64_t probe = session_ns_suffix_probe_count - p0;
        MOQ_TEST_CHECK(probe >= 1u);         /* named: probe increment live */
    } else {
        MOQ_TEST_CHECK(session_ns_suffix_probe_count >= p0);  /* monotonicity */
    }
    moq_session_destroy(s);
    if (!failures) MOQ_TEST_PASS("ns_probe_selfcheck");
    return failures;
}

/* -- MOQ-NAMESPACE-QUADRATIC-SET (security report finding 3): peer-controlled
 *    namespace enumeration has superlinear duplicate-tracking cost.
 *
 * Drives N distinct inbound NAMESPACE responses through the PUBLIC d18 session
 * route. The permanent assertions are REPRESENTATION-NEUTRAL security contracts
 * (a future ordered-tree membership must satisfy them); the exact current
 * measurements (probes, bytes) are reported as characterization, not asserted.
 * Wall time is not used. */
static int test_ns_quadratic_red(void)
{
    int failures = 0;
    const int N = 1000;

    moq_ns_sub_handle_t h;
    moq_stream_ref_t ref;
    moq_session_t *s = ns_establish_sub(&h, &ref, &failures);
    if (!s) return failures;

    uint64_t probe0 = session_ns_suffix_probe_count;
    size_t   bytes0 = s->recv_payload_bytes;

    for (int i = 0; i < N; i++) {
        uint8_t sb[4] = { (uint8_t)(i >> 24), (uint8_t)(i >> 16),
                          (uint8_t)(i >> 8), (uint8_t)i };
        failures += ns_feed_one(s, ref, h, sb, 0, MOQ_EVENT_NAMESPACE_FOUND);
    }

    /* No unsigned subtraction unless BOTH accumulators are proven monotonic by
     * control flow; the delta assertions and the characterization print run only
     * on the proven-monotonic path, so a corrupt wrapped value can never produce
     * a small delta or a misleading huge CHAR value. */
    bool mono = (session_ns_suffix_probe_count >= probe0) &&
                (s->recv_payload_bytes >= bytes0);
    MOQ_TEST_CHECK(session_ns_suffix_probe_count >= probe0);  /* named monotonicity */
    MOQ_TEST_CHECK(s->recv_payload_bytes >= bytes0);          /* named monotonicity */
    if (mono) {
        uint64_t probe = session_ns_suffix_probe_count - probe0;
        size_t   bytes = s->recv_payload_bytes - bytes0;

        /* Characterization for the report (NOT asserted -- both would be
         * repair-hostile): exact probes and charged bytes on the current
         * flat-array implementation. */
        fprintf(stderr, "CHAR ns_quadratic: probes=%llu bytes=%llu (N=%d)\n",
                (unsigned long long)probe, (unsigned long long)bytes, N);

        /* Positive lower bound: a removed probe increment makes this fail. */
        MOQ_TEST_CHECK(probe >= (uint64_t)(N - 1));

        /* Conservative worst-case O(N log N) ceiling: RED now (superlinear),
         * green for a balanced ordered tree. Bound = 4*N*ceil_log2(N+1). */
        uint64_t ceiling = 4u * (uint64_t)N * ns_ceil_log2((uint64_t)N + 1u);
        MOQ_TEST_CHECK(probe <= ceiling);

        /* Retained bytes are positive and far below the receive budget for this
         * workload -- without asserting any array layout. Expressed with division
         * (never `bytes * 100`, which could wrap and make a corrupt large value
         * look small): retained stays strictly under 1/100th of the budget. On
         * the current ~23 KiB / 16 MiB this leaves ~680x headroom, so the strict
         * /100 form has ample margin. */
        MOQ_TEST_CHECK(bytes > 0);
        MOQ_TEST_CHECK(bytes < s->max_recv_buf / 100u);
    }
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);

    /* Duplicate control: a repeated identical NAMESPACE changes neither
     * topology nor charged bytes and emits exactly one duplicate FOUND. */
    {
        uint8_t sb[4] = { 0, 0, 0, 0 };   /* suffix i==0, already live */
        size_t bb = s->recv_payload_bytes;
        failures += ns_feed_one(s, ref, h, sb, 0, MOQ_EVENT_NAMESPACE_FOUND);
        MOQ_TEST_CHECK_EQ_U64((uint64_t)s->recv_payload_bytes, (uint64_t)bb);
    }

    /* Removal control: NAMESPACE_DONE emits exactly one GONE and releases a
     * positive live charge. */
    {
        uint8_t sb[4] = { 0, 0, 0, 0 };
        size_t bb = s->recv_payload_bytes;
        failures += ns_feed_one(s, ref, h, sb, 1, MOQ_EVENT_NAMESPACE_GONE);
        MOQ_TEST_CHECK(s->recv_payload_bytes < bb);        /* released charge */
    }

    /* Remove/re-add/remove cycle returns to the same residual charge and reuses
     * the same semantic state -- without assuming retained array capacity. */
    {
        uint8_t sb[4] = { 0, 0, 0, 1 };   /* suffix i==1, currently live */
        failures += ns_feed_one(s, ref, h, sb, 1, MOQ_EVENT_NAMESPACE_GONE);
        size_t residual = s->recv_payload_bytes;
        failures += ns_feed_one(s, ref, h, sb, 0, MOQ_EVENT_NAMESPACE_FOUND);
        failures += ns_feed_one(s, ref, h, sb, 1, MOQ_EVENT_NAMESPACE_GONE);
        MOQ_TEST_CHECK_EQ_U64((uint64_t)s->recv_payload_bytes,
                              (uint64_t)residual);
    }

    moq_session_destroy(s);
    if (!failures) MOQ_TEST_PASS("ns_quadratic_red");
    return failures;
}

/* -- Fix C: per-subscription active-suffix cap --------------------- *
 * A d18 client session with a chosen suffix cap and event budget. Setup is
 * checked (encode, feed, exactly one SETUP_COMPLETE) rather than discarded, so
 * the cap rows cannot silently run on a bad session. */
static moq_session_t *make_session_scap_ex(uint32_t suffix_cap,
                                           uint32_t max_events,
                                           uint32_t max_actions, int *acc)
{
    int failures = 0;
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_CLIENT);
    cfg.version = MOQ_VERSION_DRAFT_18;
    cfg.max_namespace_suffixes_per_subscription = suffix_cap;
    if (max_events) cfg.max_events = max_events;
    if (max_actions) cfg.max_actions = max_actions;
    moq_session_t *s = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_create(&cfg, 0, &s), (int)MOQ_OK);
    if (!s) { if (acc) *acc += failures; return NULL; }
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(s, 0), (int)MOQ_OK);
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
    uint8_t setup[16];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, setup, sizeof(setup));
    moq_result_t erc = moq_d18_encode_setup(&w);
    MOQ_TEST_CHECK_EQ_INT((int)erc, (int)MOQ_OK);
    if (erc == MOQ_OK) {
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_control_bytes(s, setup,
                moq_buf_writer_offset(&w), 0), (int)MOQ_OK);
        moq_event_t ev;
        int got = moq_session_poll_events(s, &ev, 1);
        MOQ_TEST_CHECK_EQ_INT(got, 1);
        if (got == 1) {
            MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_SETUP_COMPLETE);
            moq_event_cleanup(&ev);
            MOQ_TEST_CHECK_EQ_INT(moq_session_poll_events(s, &ev, 1), 0);
        }
    }
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(s), (int)MOQ_SESS_ESTABLISHED);
    if (acc) *acc += failures;
    return s;
}

static moq_session_t *make_session_scap(uint32_t suffix_cap, uint32_t max_events,
                                        int *acc)
{
    return make_session_scap_ex(suffix_cap, max_events, 0, acc);
}

/* Feed one NAMESPACE for a 4-byte suffix with a chosen FIN flag and return the
 * session result; does NOT poll (the caller inspects events itself). */
static moq_result_t ns_feed_namespace_fin(moq_session_t *s, moq_stream_ref_t ref,
                                          const uint8_t *sb, bool fin)
{
    moq_bytes_t sp = { sb, 4 };
    moq_namespace_t suffix = { &sp, 1 };
    uint8_t nm[64];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, nm, sizeof(nm));
    if (moq_d18_encode_namespace_msg(&w, &suffix, false) != MOQ_OK)
        return MOQ_ERR_INTERNAL;
    return moq_session_on_bidi_stream_bytes(s, ref, nm,
        moq_buf_writer_offset(&w), fin, 1);
}
static moq_result_t ns_feed_namespace_raw(moq_session_t *s, moq_stream_ref_t ref,
                                          const uint8_t *sb)
{
    return ns_feed_namespace_fin(s, ref, sb, false);
}

/* Exact action oracle: exactly one CLOSE_BIDI_STREAM for `ref`, nothing else. */
static int ns_expect_one_close(moq_session_t *s, moq_stream_ref_t ref)
{
    int failures = 0;
    int closes = 0, other = 0;
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) {
        if (a.kind == MOQ_ACTION_CLOSE_BIDI_STREAM &&
            a.u.close_bidi_stream.stream_ref._v == ref._v)
            closes++;
        else
            other++;
        moq_action_cleanup(&a);
    }
    MOQ_TEST_CHECK_EQ_INT(closes, 1);
    MOQ_TEST_CHECK_EQ_INT(other, 0);
    return failures;
}

/* Exact action oracle: zero actions. */
static int ns_expect_no_actions(moq_session_t *s)
{
    int failures = 0;
    int n = 0;
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) { n++; moq_action_cleanup(&a); }
    MOQ_TEST_CHECK_EQ_INT(n, 0);
    return failures;
}

/* Count live (non-FREE) ns_sub entries -- proves absorbed late bytes create no
 * new owner. */
static size_t ns_live_count(const moq_session_t *s)
{
    size_t n = 0;
    for (size_t i = 0; i < s->ns_sub_cap; i++)
        if (s->ns_subs[i].state != MOQ_NS_SUB_FREE) n++;
    return n;
}

/* Count busy generic request-staging owners (subscription pool). Defined later
 * in the file; forward-declared for the cap rows' late-byte oracle. */
static int count_busy_subs(moq_session_t *s);

static int test_ns_suffix_cap(void)
{
    int failures = 0;
    static const uint8_t A[4] = { 0, 0, 0, 'a' };
    static const uint8_t B[4] = { 0, 0, 0, 'b' };
    static const uint8_t C[4] = { 0, 0, 0, 'c' };

    static const uint8_t D[4] = { 0, 0, 0, 'd' };

    /* == Row 1: no-FIN completion installs a drain carrier ============
     * a and b surface FOUND; the over-cap c (no FIN) surfaces no FOUND(c).
     * Instead NAMESPACE_GONE fires for a then b (the tree cursor's ascending
     * order), the owner retires (index gone, entry FREE), exactly ONE
     * CLOSE_BIDI_STREAM(ref) is queued, the session stays ESTABLISHED, and a
     * normal drain carrier holds the still-live peer half. Late non-empty peer
     * bytes are absorbed with no new owner/event/action; a late FIN releases the
     * carrier. */
    {
        moq_session_t *s = make_session_scap(2, 0, &failures);
        MOQ_TEST_CHECK(s != NULL);
        if (!s) return failures + 1;
        MOQ_TEST_CHECK_EQ_U64((uint64_t)s->max_ns_suffixes, 2u);
        moq_ns_sub_handle_t h; moq_stream_ref_t ref;
        if (!ns_establish_on(s, &h, &ref, &failures)) {
            moq_session_destroy(s); return failures;
        }
        int slot = moq_index_find(s->idx_ns_by_ref, s->idx_ns_mask, ref._v);
        MOQ_TEST_CHECK(slot >= 0);

        failures += ns_feed_one(s, ref, h, A, 0, MOQ_EVENT_NAMESPACE_FOUND);
        failures += ns_feed_one(s, ref, h, B, 0, MOQ_EVENT_NAMESPACE_FOUND);

        /* Over-cap third suffix, no FIN. */
        MOQ_TEST_CHECK_EQ_INT((int)ns_feed_namespace_raw(s, ref, C), (int)MOQ_OK);
        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_INT(moq_session_poll_events(s, &ev, 1), 1);
        MOQ_TEST_CHECK(ns_event_is(&ev, MOQ_EVENT_NAMESPACE_GONE, h, A, 4));
        moq_event_cleanup(&ev);
        MOQ_TEST_CHECK_EQ_INT(moq_session_poll_events(s, &ev, 1), 1);
        MOQ_TEST_CHECK(ns_event_is(&ev, MOQ_EVENT_NAMESPACE_GONE, h, B, 4));
        moq_event_cleanup(&ev);
        MOQ_TEST_CHECK_EQ_INT(moq_session_poll_events(s, &ev, 1), 0);  /* no FOUND(c) */

        failures += ns_expect_one_close(s, ref);   /* exactly one close for ref */
        if (slot >= 0)
            MOQ_TEST_CHECK_EQ_INT((int)s->ns_subs[slot].state,
                                  (int)MOQ_NS_SUB_FREE);
        MOQ_TEST_CHECK(moq_index_find(s->idx_ns_by_ref, s->idx_ns_mask,
                                      ref._v) < 0);
        MOQ_TEST_CHECK(drain_ref_contains(s, ref));           /* carrier present */
        MOQ_TEST_CHECK_EQ_INT((int)drain_ref_reason(s, ref), (int)MOQ_DRAIN_NORMAL);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);

        /* Late non-empty peer bytes on the same ref: absorbed by the carrier,
         * no new owner/event/action, session still established, carrier valid. */
        size_t ns_live_before = ns_live_count(s);
        MOQ_TEST_CHECK_EQ_INT((int)ns_feed_namespace_raw(s, ref, D), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(moq_session_poll_events(s, &ev, 1), 0);
        failures += ns_expect_no_actions(s);
        MOQ_TEST_CHECK_EQ_SIZE(ns_live_count(s), ns_live_before);
        /* The original bug class routed the freed ref through the generic
         * request path, minting a fresh staging owner. The carrier must absorb
         * the bytes with NO generic owner and NO request-registry edge. */
        MOQ_TEST_CHECK_EQ_INT(count_busy_subs(s), 0);
        MOQ_TEST_CHECK_EQ_INT(
            (int)request_registry_find_by_streamref(s, ref).kind,
            (int)MOQ_REQ_NONE);
        MOQ_TEST_CHECK(drain_ref_contains(s, ref));
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);

        /* Late FIN releases the carrier. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
            (int)MOQ_OK);
        MOQ_TEST_CHECK(!drain_ref_contains(s, ref));
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        failures += ns_expect_no_actions(s);
        moq_session_destroy(s);
    }

    /* == Row 2: FIN completion owes no carrier ========================
     * The over-cap message itself carries FIN, so the peer send half is already
     * gone: exact GONE(a), GONE(b), one CLOSE_BIDI_STREAM(ref), no drain carrier
     * owed after completion. */
    {
        moq_session_t *s = make_session_scap(2, 0, &failures);
        MOQ_TEST_CHECK(s != NULL);
        if (!s) return failures + 1;
        moq_ns_sub_handle_t h; moq_stream_ref_t ref;
        if (!ns_establish_on(s, &h, &ref, &failures)) {
            moq_session_destroy(s); return failures;
        }
        int slot = moq_index_find(s->idx_ns_by_ref, s->idx_ns_mask, ref._v);
        MOQ_TEST_CHECK(slot >= 0);

        failures += ns_feed_one(s, ref, h, A, 0, MOQ_EVENT_NAMESPACE_FOUND);
        failures += ns_feed_one(s, ref, h, B, 0, MOQ_EVENT_NAMESPACE_FOUND);

        /* Over-cap third suffix WITH FIN. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)ns_feed_namespace_fin(s, ref, C, true), (int)MOQ_OK);
        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_INT(moq_session_poll_events(s, &ev, 1), 1);
        MOQ_TEST_CHECK(ns_event_is(&ev, MOQ_EVENT_NAMESPACE_GONE, h, A, 4));
        moq_event_cleanup(&ev);
        MOQ_TEST_CHECK_EQ_INT(moq_session_poll_events(s, &ev, 1), 1);
        MOQ_TEST_CHECK(ns_event_is(&ev, MOQ_EVENT_NAMESPACE_GONE, h, B, 4));
        moq_event_cleanup(&ev);
        MOQ_TEST_CHECK_EQ_INT(moq_session_poll_events(s, &ev, 1), 0);

        failures += ns_expect_one_close(s, ref);
        if (slot >= 0)
            MOQ_TEST_CHECK_EQ_INT((int)s->ns_subs[slot].state,
                                  (int)MOQ_NS_SUB_FREE);
        MOQ_TEST_CHECK(moq_index_find(s->idx_ns_by_ref, s->idx_ns_mask,
                                      ref._v) < 0);
        MOQ_TEST_CHECK(!drain_ref_contains(s, ref));   /* no carrier owed */
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Row 3: blocked cap terminal, durable retry ===================
     * cap 2, event queue 2. a and b are queued but NOT polled, filling the
     * queue; the over-cap c (no FIN) triggers the cap terminal, which blocks on
     * event capacity. On refusal NO close action and NO drain carrier escaped;
     * owner/index/tree/budget/marker are retained. Draining the FOUNDs and
     * re-feeding empty completes exactly once with the same output, one close,
     * and the no-FIN carrier; a second empty re-feed is a no-op. */
    {
        moq_session_t *s = make_session_scap(2, 2, &failures);
        MOQ_TEST_CHECK(s != NULL);
        if (!s) return failures + 1;
        moq_ns_sub_handle_t h; moq_stream_ref_t ref;
        if (!ns_establish_on(s, &h, &ref, &failures)) {
            moq_session_destroy(s); return failures;
        }
        int slot = moq_index_find(s->idx_ns_by_ref, s->idx_ns_mask, ref._v);
        MOQ_TEST_CHECK(slot >= 0);

        MOQ_TEST_CHECK_EQ_INT((int)ns_feed_namespace_raw(s, ref, A), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)ns_feed_namespace_raw(s, ref, B), (int)MOQ_OK);
        size_t budget_at_block = s->recv_payload_bytes;
        size_t drain_at_block = s->drain_ref_count;

        MOQ_TEST_CHECK_EQ_INT(
            (int)ns_feed_namespace_raw(s, ref, C), (int)MOQ_ERR_WOULD_BLOCK);
        /* No side effect escaped: no close action, no drain carrier installed. */
        failures += ns_expect_no_actions(s);
        MOQ_TEST_CHECK_EQ_U64((uint64_t)s->drain_ref_count,
                              (uint64_t)drain_at_block);
        MOQ_TEST_CHECK(!drain_ref_contains(s, ref));
        MOQ_TEST_CHECK(slot >= 0 && s->ns_subs[slot].suffix_cap_terminating);
        MOQ_TEST_CHECK(s->ns_subs[slot].announced_suffixes != NULL);
        MOQ_TEST_CHECK_EQ_U64((uint64_t)s->recv_payload_bytes,
                              (uint64_t)budget_at_block);
        MOQ_TEST_CHECK(moq_index_find(s->idx_ns_by_ref, s->idx_ns_mask,
                                      ref._v) >= 0);

        moq_event_t ev;
        for (int i = 0; i < 2; i++) {
            MOQ_TEST_CHECK_EQ_INT(moq_session_poll_events(s, &ev, 1), 1);
            MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NAMESPACE_FOUND);
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, false, 0),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(moq_session_poll_events(s, &ev, 1), 1);
        MOQ_TEST_CHECK(ns_event_is(&ev, MOQ_EVENT_NAMESPACE_GONE, h, A, 4));
        moq_event_cleanup(&ev);
        MOQ_TEST_CHECK_EQ_INT(moq_session_poll_events(s, &ev, 1), 1);
        MOQ_TEST_CHECK(ns_event_is(&ev, MOQ_EVENT_NAMESPACE_GONE, h, B, 4));
        moq_event_cleanup(&ev);
        MOQ_TEST_CHECK_EQ_INT(moq_session_poll_events(s, &ev, 1), 0);

        failures += ns_expect_one_close(s, ref);
        MOQ_TEST_CHECK(drain_ref_contains(s, ref));           /* no-FIN carrier */
        if (slot >= 0)
            MOQ_TEST_CHECK_EQ_INT((int)s->ns_subs[slot].state,
                                  (int)MOQ_NS_SUB_FREE);
        MOQ_TEST_CHECK(moq_index_find(s->idx_ns_by_ref, s->idx_ns_mask,
                                      ref._v) < 0);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);

        /* Exactly-once: a second empty re-feed emits nothing and no action. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, false, 0),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(moq_session_poll_events(s, &ev, 1), 0);
        failures += ns_expect_no_actions(s);
        moq_session_destroy(s);
    }

    /* == Row 4: no-FIN full drain ring blocks before any output =======
     * With the drain ring full, the no-FIN cap terminal preflights the drain
     * slot and returns WOULD_BLOCK BEFORE any GONE or close; owner, tree, index
     * and output are preserved. Releasing one filler ref lets the empty re-feed
     * complete and install exactly (ref, MOQ_DRAIN_NORMAL). */
    {
        moq_session_t *s = make_session_scap(2, 0, &failures);
        MOQ_TEST_CHECK(s != NULL);
        if (!s) return failures + 1;
        moq_ns_sub_handle_t h; moq_stream_ref_t ref;
        if (!ns_establish_on(s, &h, &ref, &failures)) {
            moq_session_destroy(s); return failures;
        }
        int slot = moq_index_find(s->idx_ns_by_ref, s->idx_ns_mask, ref._v);
        MOQ_TEST_CHECK(slot >= 0);

        failures += ns_feed_one(s, ref, h, A, 0, MOQ_EVENT_NAMESPACE_FOUND);
        failures += ns_feed_one(s, ref, h, B, 0, MOQ_EVENT_NAMESPACE_FOUND);

        /* Fill the drain ring with distinct filler refs (none equal to ref). */
        moq_stream_ref_t filler0 = moq_stream_ref_from_u64(1000001);
        uint64_t fv = 1000001;
        MOQ_TEST_CHECK(s->drain_ref_count < s->drain_ref_cap);
        while (s->drain_ref_count < s->drain_ref_cap) {
            MOQ_TEST_CHECK(drain_ref_add(s, moq_stream_ref_from_u64(fv)));
            fv++;
        }
        MOQ_TEST_CHECK_EQ_U64((uint64_t)s->drain_ref_count,
                              (uint64_t)s->drain_ref_cap);

        /* Over-cap c, no FIN: drain preflight fails before any output. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)ns_feed_namespace_raw(s, ref, C), (int)MOQ_ERR_WOULD_BLOCK);
        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_INT(moq_session_poll_events(s, &ev, 1), 0);  /* no GONE */
        failures += ns_expect_no_actions(s);
        MOQ_TEST_CHECK(slot >= 0 && s->ns_subs[slot].suffix_cap_terminating);
        MOQ_TEST_CHECK(s->ns_subs[slot].announced_suffixes != NULL);
        MOQ_TEST_CHECK(moq_index_find(s->idx_ns_by_ref, s->idx_ns_mask,
                                      ref._v) >= 0);

        /* Release one filler; the empty re-feed completes and installs the ref. */
        MOQ_TEST_CHECK(drain_ref_remove(s, filler0));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, false, 0),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(moq_session_poll_events(s, &ev, 1), 1);
        MOQ_TEST_CHECK(ns_event_is(&ev, MOQ_EVENT_NAMESPACE_GONE, h, A, 4));
        moq_event_cleanup(&ev);
        MOQ_TEST_CHECK_EQ_INT(moq_session_poll_events(s, &ev, 1), 1);
        MOQ_TEST_CHECK(ns_event_is(&ev, MOQ_EVENT_NAMESPACE_GONE, h, B, 4));
        moq_event_cleanup(&ev);
        MOQ_TEST_CHECK_EQ_INT(moq_session_poll_events(s, &ev, 1), 0);
        failures += ns_expect_one_close(s, ref);
        MOQ_TEST_CHECK(drain_ref_contains(s, ref));
        MOQ_TEST_CHECK_EQ_INT((int)drain_ref_reason(s, ref), (int)MOQ_DRAIN_NORMAL);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Row 5: slot reuse leaves no stale cap state ==================
     * After a cap terminal frees the owner, a fresh namespace subscription
     * reuses the same ns_sub slot: no stale cap marker, no stale tree, and it
     * tracks suffixes normally. */
    {
        moq_session_t *s = make_session_scap(2, 0, &failures);
        MOQ_TEST_CHECK(s != NULL);
        if (!s) return failures + 1;
        moq_ns_sub_handle_t h; moq_stream_ref_t ref;
        if (!ns_establish_on(s, &h, &ref, &failures)) {
            moq_session_destroy(s); return failures;
        }
        int slot = moq_index_find(s->idx_ns_by_ref, s->idx_ns_mask, ref._v);
        MOQ_TEST_CHECK(slot >= 0);

        failures += ns_feed_one(s, ref, h, A, 0, MOQ_EVENT_NAMESPACE_FOUND);
        failures += ns_feed_one(s, ref, h, B, 0, MOQ_EVENT_NAMESPACE_FOUND);
        /* Over-cap WITH FIN so no carrier survives to confuse the reuse. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)ns_feed_namespace_fin(s, ref, C, true), (int)MOQ_OK);
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) moq_event_cleanup(&ev);
        failures += ns_expect_one_close(s, ref);
        MOQ_TEST_CHECK(slot >= 0 &&
                       s->ns_subs[slot].state == MOQ_NS_SUB_FREE);
        MOQ_TEST_CHECK(!drain_ref_contains(s, ref));

        moq_ns_sub_handle_t h2; moq_stream_ref_t ref2;
        if (!ns_establish_on(s, &h2, &ref2, &failures)) {
            moq_session_destroy(s); return failures;
        }
        int slot2 = moq_index_find(s->idx_ns_by_ref, s->idx_ns_mask, ref2._v);
        MOQ_TEST_CHECK(slot2 >= 0);
        /* The freed slot is reused, so "no stale state follows the slot" is a
         * fact about THIS slot, not merely some fresh one. */
        MOQ_TEST_CHECK_EQ_INT(slot2, slot);
        if (slot2 >= 0) {
            MOQ_TEST_CHECK(!s->ns_subs[slot2].suffix_cap_terminating);
            MOQ_TEST_CHECK(s->ns_subs[slot2].announced_suffixes == NULL);
        }
        failures += ns_feed_one(s, ref2, h2, A, 0, MOQ_EVENT_NAMESPACE_FOUND);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Row 6: action-full cap terminal preflights the close =========
     * The terminal depends on the close-action preflight (want_close &&
     * action_queue_full), because the close queue after the GONE loop ignores
     * queue_close_bidi's return. With max_actions=1 and one blocker close
     * already queued on a DISTINCT ref, the no-FIN cap terminal returns
     * WOULD_BLOCK BEFORE any GONE, close, or drain carrier: zero events, only
     * the blocker action queued, and owner/index/tree/budget/marker retained.
     * Draining the blocker and re-feeding empty completes normally: GONE(a),
     * GONE(b), exactly one CLOSE_BIDI_STREAM(ref), and (ref, MOQ_DRAIN_NORMAL)
     * installed. Deleting the action preflight makes this row fail by name. */
    {
        moq_session_t *s = make_session_scap_ex(2, 0, 1, &failures);
        MOQ_TEST_CHECK(s != NULL);
        if (!s) return failures + 1;
        moq_ns_sub_handle_t h; moq_stream_ref_t ref;
        if (!ns_establish_on(s, &h, &ref, &failures)) {
            moq_session_destroy(s); return failures;
        }
        int slot = moq_index_find(s->idx_ns_by_ref, s->idx_ns_mask, ref._v);
        MOQ_TEST_CHECK(slot >= 0);

        failures += ns_feed_one(s, ref, h, A, 0, MOQ_EVENT_NAMESPACE_FOUND);
        failures += ns_feed_one(s, ref, h, B, 0, MOQ_EVENT_NAMESPACE_FOUND);

        /* Fill the single-slot action queue with a blocker close on a DISTINCT
         * ref, so the terminal's close preflight -- not event capacity -- is
         * what refuses. */
        moq_stream_ref_t blocker = moq_stream_ref_from_u64(2000002);
        MOQ_TEST_CHECK(blocker._v != ref._v);
        MOQ_TEST_CHECK_EQ_INT((int)queue_close_bidi(s, blocker), (int)MOQ_OK);
        MOQ_TEST_CHECK(action_queue_full(s));

        size_t budget_at_block = s->recv_payload_bytes;
        size_t drain_at_block = s->drain_ref_count;

        /* Over-cap c, no FIN: the close preflight fails before any output. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)ns_feed_namespace_raw(s, ref, C), (int)MOQ_ERR_WOULD_BLOCK);
        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_INT(moq_session_poll_events(s, &ev, 1), 0);  /* no GONE */
        MOQ_TEST_CHECK(!drain_ref_contains(s, ref));   /* no drain carrier */
        MOQ_TEST_CHECK_EQ_U64((uint64_t)s->drain_ref_count,
                              (uint64_t)drain_at_block);
        MOQ_TEST_CHECK(slot >= 0 && s->ns_subs[slot].suffix_cap_terminating);
        MOQ_TEST_CHECK(s->ns_subs[slot].announced_suffixes != NULL);
        MOQ_TEST_CHECK_EQ_U64((uint64_t)s->recv_payload_bytes,
                              (uint64_t)budget_at_block);
        MOQ_TEST_CHECK(moq_index_find(s->idx_ns_by_ref, s->idx_ns_mask,
                                      ref._v) >= 0);
        /* The only queued action is the blocker on the distinct ref (no target
         * close escaped). Draining it also frees the slot for the retry. */
        failures += ns_expect_one_close(s, blocker);

        /* Empty re-feed now completes: exact GONEs, one close, no-FIN carrier. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, false, 0),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(moq_session_poll_events(s, &ev, 1), 1);
        MOQ_TEST_CHECK(ns_event_is(&ev, MOQ_EVENT_NAMESPACE_GONE, h, A, 4));
        moq_event_cleanup(&ev);
        MOQ_TEST_CHECK_EQ_INT(moq_session_poll_events(s, &ev, 1), 1);
        MOQ_TEST_CHECK(ns_event_is(&ev, MOQ_EVENT_NAMESPACE_GONE, h, B, 4));
        moq_event_cleanup(&ev);
        MOQ_TEST_CHECK_EQ_INT(moq_session_poll_events(s, &ev, 1), 0);
        failures += ns_expect_one_close(s, ref);
        MOQ_TEST_CHECK(drain_ref_contains(s, ref));
        MOQ_TEST_CHECK_EQ_INT((int)drain_ref_reason(s, ref),
                              (int)MOQ_DRAIN_NORMAL);
        if (slot >= 0)
            MOQ_TEST_CHECK_EQ_INT((int)s->ns_subs[slot].state,
                                  (int)MOQ_NS_SUB_FREE);
        MOQ_TEST_CHECK(moq_index_find(s->idx_ns_by_ref, s->idx_ns_mask,
                                      ref._v) < 0);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    if (!failures) MOQ_TEST_PASS("ns_suffix_cap");
    return failures;
}

/* ABI-additive contract for the suffix cap: an old caller whose struct_size
 * stops before the field gets the library default; a new caller with the full
 * struct_size can set a small cap. */
static int test_ns_suffix_cap_abi(void)
{
    int failures = 0;

    /* Old caller: struct_size ends before max_namespace_suffixes_per_subscription. */
    {
        moq_session_cfg_t cfg;
        size_t old_size = offsetof(moq_session_cfg_t,
                                   max_namespace_suffixes_per_subscription);
        moq_session_cfg_init_sized(&cfg, old_size, moq_alloc_default(),
                                   MOQ_PERSPECTIVE_CLIENT);
        cfg.version = MOQ_VERSION_DRAFT_18;
        moq_session_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_create(&cfg, 0, &s), (int)MOQ_OK);
        if (s) {
            MOQ_TEST_CHECK_EQ_U64((uint64_t)s->max_ns_suffixes,
                                  (uint64_t)MOQ_DEFAULT_MAX_NS_SUFFIXES);
            moq_session_destroy(s);
        }
    }

    /* New caller: full struct_size, explicit small cap honored. */
    {
        moq_session_cfg_t cfg;
        moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(),
                                   MOQ_PERSPECTIVE_CLIENT);
        cfg.version = MOQ_VERSION_DRAFT_18;
        cfg.max_namespace_suffixes_per_subscription = 2;
        moq_session_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_create(&cfg, 0, &s), (int)MOQ_OK);
        if (s) {
            MOQ_TEST_CHECK_EQ_U64((uint64_t)s->max_ns_suffixes, 2u);
            moq_session_destroy(s);
        }
    }

    /* New caller, field 0: selects the default. */
    {
        moq_session_cfg_t cfg;
        moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(),
                                   MOQ_PERSPECTIVE_CLIENT);
        cfg.version = MOQ_VERSION_DRAFT_18;
        cfg.max_namespace_suffixes_per_subscription = 0;
        moq_session_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_create(&cfg, 0, &s), (int)MOQ_OK);
        if (s) {
            MOQ_TEST_CHECK_EQ_U64((uint64_t)s->max_ns_suffixes,
                                  (uint64_t)MOQ_DEFAULT_MAX_NS_SUFFIXES);
            moq_session_destroy(s);
        }
    }

    if (!failures) MOQ_TEST_PASS("ns_suffix_cap_abi");
    return failures;
}

/* -- Codec round-trips and malformed rejection --------------------- */

static int test_codec(void)
{
    int failures = 0;
    uint8_t buf[256];
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("example.com"),
                            MOQ_BYTES_LITERAL("meeting=1") };
    moq_namespace_t ns = { parts, 2 };

    /* SUBSCRIBE_NAMESPACE round-trip with an auth token. */
    {
        moq_d18_msg_params_t p = { 0 };
        p.auth_token_count = 1;
        p.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_USE_VALUE;
        p.auth_tokens[0].token_type = 7;
        p.auth_tokens[0].token_value = MOQ_BYTES_LITERAL("tok");
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_encode_subscribe_namespace(&w, 4, &ns, &p), (int)MOQ_OK);
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        moq_d18_decode_envelope(&r, &env);
        MOQ_TEST_CHECK_EQ_U64(env.msg_type, MOQ_D18_SUBSCRIBE_NAMESPACE);
        moq_bytes_t dp[8];
        moq_d18_subscribe_namespace_t sn;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_subscribe_namespace(env.payload, env.payload_len,
                                                    dp, 8, &sn), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(sn.request_id, 4);
        MOQ_TEST_CHECK_EQ_SIZE(sn.track_namespace_prefix.count, 2);
        failures += txs_check_part_bytes(&sn.track_namespace_prefix, 0,
                                         "example.com", 11,
                                         "decoded subscribe_namespace prefix");
        MOQ_TEST_CHECK_EQ_SIZE(sn.params.auth_token_count, 1);
        MOQ_TEST_CHECK_EQ_U64(sn.params.auth_tokens[0].token_type, 7);
    }

    /* Zero-field prefix round-trips (draft-18 allows 0..32). */
    {
        moq_namespace_t empty = { NULL, 0 };
        moq_d18_msg_params_t p = { 0 };
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_encode_subscribe_namespace(&w, 0, &empty, &p), (int)MOQ_OK);
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        moq_d18_decode_envelope(&r, &env);
        moq_bytes_t dp[8];
        moq_d18_subscribe_namespace_t sn;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_subscribe_namespace(env.payload, env.payload_len,
                                                    dp, 8, &sn), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_SIZE(sn.track_namespace_prefix.count, 0);
    }

    /* NAMESPACE / NAMESPACE_DONE suffix round-trip. */
    {
        for (int done = 0; done < 2; done++) {
            moq_buf_writer_t w;
            moq_buf_writer_init(&w, buf, sizeof(buf));
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_d18_encode_namespace_msg(&w, &ns, done != 0), (int)MOQ_OK);
            moq_buf_reader_t r;
            moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
            moq_control_envelope_t env;
            moq_d18_decode_envelope(&r, &env);
            MOQ_TEST_CHECK_EQ_U64(env.msg_type,
                done ? MOQ_D18_NAMESPACE_DONE : MOQ_D18_NAMESPACE);
            moq_bytes_t dp[8];
            moq_namespace_t suffix;
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_d18_decode_namespace_msg(env.payload, env.payload_len,
                                                  dp, 8, &suffix), (int)MOQ_OK);
            MOQ_TEST_CHECK_EQ_SIZE(suffix.count, 2);
            failures += txs_check_part_bytes(&suffix, 1, "meeting=1", 9,
                                             "decoded namespace suffix");
        }
    }

    /* A non-AUTHORIZATION_TOKEN parameter (FORWARD) is rejected. */
    {
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        size_t len_off;
        moq_buf_write_vi64(&w, MOQ_D18_SUBSCRIBE_NAMESPACE);
        moq_buf_reserve_uint16(&w, &len_off);
        size_t start = moq_buf_writer_offset(&w);
        moq_buf_write_vi64(&w, 0);                       /* request id */
        moq_buf_write_vi64(&w, 1);                       /* ns count */
        moq_buf_write_vi64(&w, 3);
        moq_buf_write_raw(&w, (const uint8_t *)"abc", 3);
        moq_buf_write_vi64(&w, 1);                       /* 1 parameter */
        moq_buf_write_vi64(&w, MOQ_D18_PARAM_FORWARD);
        uint8_t one = 1; moq_buf_write_raw(&w, &one, 1);
        moq_buf_patch_uint16(&w, len_off,
            (uint16_t)(moq_buf_writer_offset(&w) - start));
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        moq_d18_decode_envelope(&r, &env);
        moq_bytes_t dp[8];
        moq_d18_subscribe_namespace_t sn;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_subscribe_namespace(env.payload, env.payload_len,
                                                    dp, 8, &sn), (int)MOQ_ERR_PROTO);
    }

    /* A malformed auth Token structure -> MOQ_D18_ERR_KVP_FORMAT. */
    {
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        size_t len_off;
        moq_buf_write_vi64(&w, MOQ_D18_SUBSCRIBE_NAMESPACE);
        moq_buf_reserve_uint16(&w, &len_off);
        size_t start = moq_buf_writer_offset(&w);
        moq_buf_write_vi64(&w, 0);
        moq_buf_write_vi64(&w, 1);
        moq_buf_write_vi64(&w, 3);
        moq_buf_write_raw(&w, (const uint8_t *)"abc", 3);
        moq_buf_write_vi64(&w, 1);
        moq_buf_write_vi64(&w, MOQ_D18_PARAM_AUTHORIZATION_TOKEN);
        moq_buf_write_vi64(&w, 1);                       /* span len */
        moq_buf_write_vi64(&w, 9);                       /* invalid alias type */
        moq_buf_patch_uint16(&w, len_off,
            (uint16_t)(moq_buf_writer_offset(&w) - start));
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        moq_d18_decode_envelope(&r, &env);
        moq_bytes_t dp[8];
        moq_d18_subscribe_namespace_t sn;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_subscribe_namespace(env.payload, env.payload_len,
                                                    dp, 8, &sn),
            (int)MOQ_D18_ERR_KVP_FORMAT);
    }

    return failures;
}

/* -- Session helpers ----------------------------------------------- */

static moq_session_t *make_session(moq_perspective_t persp)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), persp);
    cfg.version = MOQ_VERSION_DRAFT_18;
    moq_session_t *s = NULL;
    if (moq_session_create(&cfg, 0, &s) < 0) return NULL;
    if (moq_session_start(s, 0) < 0) { moq_session_destroy(s); return NULL; }
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
    uint8_t setup[16];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, setup, sizeof(setup));
    moq_d18_encode_setup(&w);
    moq_session_on_control_bytes(s, setup, moq_buf_writer_offset(&w), 0);
    moq_event_t e;
    while (moq_session_poll_events(s, &e, 1) > 0) moq_event_cleanup(&e);
    return s;
}

/* Session with explicit capacity knobs (for backpressure / pool-exhaustion). */
static moq_session_t *make_session_caps(moq_perspective_t persp,
                                        uint32_t max_events,
                                        uint32_t send_buffer_size,
                                        uint32_t max_ns_subs)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), persp);
    cfg.version = MOQ_VERSION_DRAFT_18;
    if (max_events) cfg.max_events = max_events;
    if (send_buffer_size) cfg.send_buffer_size = send_buffer_size;
    if (max_ns_subs) cfg.max_namespace_subscriptions = max_ns_subs;
    moq_session_t *s = NULL;
    if (moq_session_create(&cfg, 0, &s) < 0) return NULL;
    if (moq_session_start(s, 0) < 0) { moq_session_destroy(s); return NULL; }
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
    uint8_t setup[16];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, setup, sizeof(setup));
    moq_d18_encode_setup(&w);
    moq_session_on_control_bytes(s, setup, moq_buf_writer_offset(&w), 0);
    moq_event_t e;
    while (moq_session_poll_events(s, &e, 1) > 0) moq_event_cleanup(&e);
    moq_action_t a2;
    while (moq_session_poll_actions(s, &a2, 1) > 0) moq_action_cleanup(&a2);
    return s;
}

/* Count non-free generic subscription (staging) slots. */
static int count_busy_subs(moq_session_t *s)
{
    int n = 0;
    for (size_t i = 0; i < s->sub_cap; i++)
        if (s->subs[i].state != MOQ_SUB_FREE) n++;
    return n;
}

/* Encode a SUBSCRIBE_NAMESPACE with a single-field prefix. */
static size_t encode_sns(uint8_t *buf, size_t cap, uint64_t request_id,
                         const char *field, const moq_d18_msg_params_t *p)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    moq_bytes_t parts[] = { { (const uint8_t *)field, strlen(field) } };
    moq_namespace_t ns = { parts, 1 };
    moq_d18_encode_subscribe_namespace(&w, request_id, &ns, p);
    return moq_buf_writer_offset(&w);
}

/* Feed an inbound SUBSCRIBE_NAMESPACE on `ref`, return the surfaced handle. */
static moq_ns_sub_handle_t feed_sns(moq_session_t *s, moq_stream_ref_t ref,
                                    uint64_t request_id, const char *field,
                                    const moq_d18_msg_params_t *p)
{
    uint8_t msg[128];
    size_t n = encode_sns(msg, sizeof(msg), request_id, field, p);
    moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1);
    moq_ns_sub_handle_t h = MOQ_NS_SUB_HANDLE_INVALID;
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_NS_SUB_REQUEST)
            h = ev.u.ns_sub_request.handle;
        moq_event_cleanup(&ev);
    }
    return h;
}

/* Scan queued actions for a message of `msg_type` on `ref`. */
static bool action_has_msg(moq_session_t *s, moq_stream_ref_t ref,
                           uint64_t msg_type, bool *out_fin)
{
    bool seen = false;
    moq_action_t act;
    while (moq_session_poll_actions(s, &act, 1) > 0) {
        if (act.kind == MOQ_ACTION_SEND_BIDI_STREAM &&
            act.u.send_bidi_stream.stream_ref._v == ref._v) {
            moq_buf_reader_t r;
            moq_buf_reader_init(&r, act.u.send_bidi_stream.data,
                                act.u.send_bidi_stream.len);
            moq_control_envelope_t env;
            if (moq_d18_decode_envelope(&r, &env) == MOQ_OK &&
                env.msg_type == msg_type) {
                seen = true;
                if (out_fin) *out_fin = act.u.send_bidi_stream.fin;
            }
        }
        moq_action_cleanup(&act);
    }
    return seen;
}

/* The profiles must no longer emit the legacy half-action kinds. */
static int g_legacy_half_actions;

/* Count queued teardown actions on `ref`: cancellation is ONE whole-stream
 * abort (counted into BOTH out-params so existing expectations of "reset
 * and stop happened" keep meaning "the request stream was torn down");
 * the legacy half-action kinds must no longer be emitted at all. */
static void count_teardown(moq_session_t *s, moq_stream_ref_t ref,
                           int *resets, int *stops)
{
    *resets = 0; *stops = 0;
    moq_action_t act;
    while (moq_session_poll_actions(s, &act, 1) > 0) {
        if (act.kind == MOQ_ACTION_ABORT_BIDI_STREAM &&
            act.u.abort_bidi_stream.stream_ref._v == ref._v) {
            (*resets)++;
            (*stops)++;
        }
        /* the profiles emit no legacy half-actions anymore */
        if (act.kind == MOQ_ACTION_RESET_BIDI_STREAM ||
            act.kind == MOQ_ACTION_STOP_BIDI_STREAM)
            g_legacy_half_actions++;
        moq_action_cleanup(&act);
    }
}

/* -- Failpoint fixtures and hooks: the NAMESPACE response op on the
 * draft-18 request-stream route. The oracle mirrors the draft-16 suite's
 * (the handler is shared); only the wire route differs. */

/* Occupancy across all seven request/subscription pools; request-id
 * sequence counters are profile-private, and this operation consumes no
 * request id at all. */
static int p18ns_registry_busy(const moq_session_t *s)
{
    int n = 0;
    for (size_t i = 0; i < s->sub_cap; i++)
        if (s->subs[i].state != MOQ_SUB_FREE) n++;
    for (size_t i = 0; i < s->fetch_cap; i++)
        if (s->fetches[i].state != MOQ_FETCH_FREE) n++;
    for (size_t i = 0; i < s->ann_cap; i++)
        if (s->announcements[i].state != MOQ_ANN_FREE) n++;
    for (size_t i = 0; i < s->ts_cap; i++)
        if (s->track_statuses[i].state != MOQ_TS_FREE) n++;
    for (size_t i = 0; i < s->pub_cap; i++)
        if (s->publishes[i].state != MOQ_PUB_FREE) n++;
    for (size_t i = 0; i < s->ns_sub_cap; i++)
        if (s->ns_subs[i].state != MOQ_NS_SUB_FREE) n++;
    for (size_t i = 0; i < s->track_sub_cap; i++)
        if (s->track_subs[i].state != MOQ_TRACK_SUB_FREE) n++;
    return n;
}

/* Mutation-inventory hook: tracker presence/identity/flag, buffered
 * response bytes, send cursor, pool occupancy. The tracked set's interior
 * scalars are a private type, covered by the receive budget (generic
 * snapshot) without layout assumptions. */
typedef struct p18ns_hook_state {
    bool        tracker_present;
    const void *tracker;          /* same-run identity; not cross-run */
    bool        tracker_inbound;
    size_t      recv_len;
    size_t      send_len;
    int         registry_busy;
} p18ns_hook_state_t;

typedef struct p18ns_hook_ctx {
    int      slot;
    uint64_t h_opaque;   /* run-portable handle normalization */
} p18ns_hook_ctx_t;

static void p18ns_hook_capture(const moq_session_t *s, void *vctx, void *vst)
{
    const p18ns_hook_ctx_t *ctx = (const p18ns_hook_ctx_t *)vctx;
    p18ns_hook_state_t *st = (p18ns_hook_state_t *)vst;
    memset(st, 0, sizeof(*st));
    const void *set = s->ns_subs[ctx->slot].announced_suffixes;
    st->tracker_present = set != NULL;
    st->tracker = set;
    st->tracker_inbound = s->ns_subs[ctx->slot].announced_suffixes_inbound;
    st->recv_len = s->ns_subs[ctx->slot].recv_len;
    st->send_len = s->send_len;
    st->registry_busy = p18ns_registry_busy(s);
}

static int p18ns_hook_check(const moq_session_t *s, void *vctx,
                            const void *vst)
{
    const p18ns_hook_state_t *want = (const p18ns_hook_state_t *)vst;
    p18ns_hook_state_t now;
    p18ns_hook_capture(s, vctx, &now);
    int bad = 0;
#define P18_HF(f, fmt) do { \
        if (now.f != want->f) { \
            fprintf(stderr, "TXN p18ns-hook: " #f " " fmt ", expected " fmt \
                    "\n", now.f, want->f); \
            bad++; \
        } \
    } while (0)
    P18_HF(tracker_present, "%d");
    P18_HF(tracker, "%p");
    P18_HF(tracker_inbound, "%d");
    P18_HF(recv_len, "%zu");
    P18_HF(send_len, "%zu");
    P18_HF(registry_busy, "%d");
#undef P18_HF
    return bad;
}

/* Cross-run: values only, pointer identity differs between runs. */
static int p18ns_hook_check_values(const moq_session_t *s, void *vctx,
                                   const void *vst)
{
    const p18ns_hook_state_t *want = (const p18ns_hook_state_t *)vst;
    p18ns_hook_state_t now;
    p18ns_hook_capture(s, vctx, &now);
    int bad = 0;
#define P18_HF(f, fmt) do { \
        if (now.f != want->f) { \
            fprintf(stderr, "TXN p18ns-final: " #f " " fmt ", expected " \
                    fmt "\n", now.f, want->f); \
            bad++; \
        } \
    } while (0)
    P18_HF(tracker_present, "%d");
    P18_HF(tracker_inbound, "%d");
    P18_HF(recv_len, "%zu");
    P18_HF(send_len, "%zu");
    P18_HF(registry_busy, "%d");
#undef P18_HF
    return bad;
}

/* Full-semantics event normalizer: handle linkage plus the complete
 * suffix structure. An unnormalized kind is a test failure. */
static bool p18ns_norm_event(const moq_event_t *ev, void *vctx,
                             txs_norm_vec_t *out)
{
    const p18ns_hook_ctx_t *ctx = (const p18ns_hook_ctx_t *)vctx;
    txs_img_t img;
    txs_img_init(&img);
    switch (ev->kind) {
    case MOQ_EVENT_NAMESPACE_FOUND:
    case MOQ_EVENT_NAMESPACE_GONE: {
        const moq_namespace_t *ns =
            ev->kind == MOQ_EVENT_NAMESPACE_FOUND
                ? &ev->u.namespace_found.track_namespace_suffix
                : &ev->u.namespace_gone.track_namespace_suffix;
        uint64_t handle = ev->kind == MOQ_EVENT_NAMESPACE_FOUND
                ? ev->u.namespace_found.handle._opaque
                : ev->u.namespace_gone.handle._opaque;
        txs_img_u64(&img, handle == ctx->h_opaque);
        if (!txs_img_parts(&img, "namespace found/gone suffix", ns->parts,
                           ns->count, MOQ_DECODED_MAX_NAMESPACE_PARTS))
            return false;
        break;
    }
    case MOQ_EVENT_NS_SUB_OK:
        txs_img_u64(&img, ev->u.ns_sub_ok.handle._opaque == ctx->h_opaque);
        break;
    default:
        fprintf(stderr, "TXN p18ns-norm: unnormalized event kind %d\n",
                (int)ev->kind);
        return false;
    }
    return txs_norm_append_img(out, (uint64_t)ev->kind, &img);
}

static bool p18ns_norm_action(const moq_action_t *a, void *vctx,
                              txs_norm_vec_t *out)
{
    (void)vctx;
    txs_img_t img;
    txs_img_init(&img);
    switch (a->kind) {
    case MOQ_ACTION_OPEN_BIDI_STREAM:
        txs_img_u64(&img, a->u.open_bidi_stream.stream_ref._v);
        txs_img_u64(&img, a->u.open_bidi_stream.fin);
        txs_img_bytes(&img, a->u.open_bidi_stream.data,
                      a->u.open_bidi_stream.len);
        break;
    case MOQ_ACTION_SEND_BIDI_STREAM:
        txs_img_u64(&img, a->u.send_bidi_stream.stream_ref._v);
        txs_img_u64(&img, a->u.send_bidi_stream.fin);
        txs_img_bytes(&img, a->u.send_bidi_stream.data,
                      a->u.send_bidi_stream.len);
        break;
    case MOQ_ACTION_CLOSE_BIDI_STREAM:
        txs_img_u64(&img, a->u.close_bidi_stream.stream_ref._v);
        break;
    default:
        fprintf(stderr, "TXN p18ns-norm: unnormalized action kind %d\n",
                (int)a->kind);
        return false;
    }
    return txs_norm_append_img(out, 0x1000u + (uint64_t)a->kind, &img);
}

static txs_op_hooks_t p18ns_make_hooks(p18ns_hook_ctx_t *hctx)
{
    txs_op_hooks_t h;
    memset(&h, 0, sizeof(h));
    h.ctx = hctx;
    h.capture = p18ns_hook_capture;
    h.check = p18ns_hook_check;
    h.check_values = p18ns_hook_check_values;
    h.normalize_event = p18ns_norm_event;
    h.normalize_action = p18ns_norm_action;
    return h;
}

static int p18ns_collect_output(moq_session_t *c, const txs_op_hooks_t *h,
                                txs_norm_vec_t *out)
{
    int bad = 0;
    moq_event_t ev;
    while (moq_session_poll_events(c, &ev, 1) > 0) {
        if (!h->normalize_event(&ev, h->ctx, out)) bad++;
        moq_event_cleanup(&ev);
    }
    moq_action_t a;
    while (moq_session_poll_actions(c, &a, 1) > 0) {
        if (!h->normalize_action(&a, h->ctx, out)) bad++;
        moq_action_cleanup(&a);
    }
    return bad;
}

/* One draft-18 client with an established namespace subscription on the
 * failpoint allocator. tiny_events keeps the scratch-free NS_SUB_OK queued
 * in a one-slot queue (the pre-admission state); otherwise it is drained. */
typedef struct p18ns_fx {
    fp_alloc_state_t fs;
    moq_session_t *c;
    moq_ns_sub_handle_t h;
    moq_stream_ref_t ref;
    int slot;
} p18ns_fx_t;

static int p18ns_fx_setup(p18ns_fx_t *f, bool tiny_events)
{
    memset(f, 0, sizeof(*f));
    moq_alloc_t alloc = fp_allocator(&f->fs);
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), &alloc,
                               MOQ_PERSPECTIVE_CLIENT);
    cfg.version = MOQ_VERSION_DRAFT_18;
    if (tiny_events) cfg.max_events = 1;
    if (moq_session_create(&cfg, 0, &f->c) < 0) return 1;
    if (moq_session_start(f->c, 0) < 0) return 1;
    moq_action_t a;
    while (moq_session_poll_actions(f->c, &a, 1) > 0) moq_action_cleanup(&a);
    uint8_t setup[16];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, setup, sizeof(setup));
    moq_d18_encode_setup(&w);
    moq_session_on_control_bytes(f->c, setup, moq_buf_writer_offset(&w), 0);
    moq_event_t ev;
    while (moq_session_poll_events(f->c, &ev, 1) > 0) moq_event_cleanup(&ev);

    moq_subscribe_namespace_cfg_t nc;
    moq_subscribe_namespace_cfg_init(&nc);
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    nc.track_namespace_prefix = (moq_namespace_t){ parts, 1 };
    nc.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
    if (moq_session_subscribe_namespace(f->c, &nc, 0, &f->h) != MOQ_OK)
        return 1;
    f->ref = (moq_stream_ref_t){ 0 };
    while (moq_session_poll_actions(f->c, &a, 1) > 0) {
        if (a.kind == MOQ_ACTION_OPEN_BIDI_STREAM)
            f->ref = a.u.open_bidi_stream.stream_ref;
        moq_action_cleanup(&a);
    }
    if (f->ref._v == 0) return 1;

    uint8_t ok[32];
    moq_buf_writer_t ow;
    moq_buf_writer_init(&ow, ok, sizeof(ok));
    if (moq_d18_encode_request_ok(&ow) != MOQ_OK) return 1;
    if (moq_session_on_bidi_stream_bytes(f->c, f->ref, ok,
            moq_buf_writer_offset(&ow), false, 0) != MOQ_OK)
        return 1;
    if (!tiny_events) {
        while (moq_session_poll_events(f->c, &ev, 1) > 0)
            moq_event_cleanup(&ev);
    }
    f->slot = moq_index_find(f->c->idx_ns_by_ref, f->c->idx_ns_mask,
                             f->ref._v);
    return f->slot < 0;
}

static size_t p18ns_encode_namespace(uint8_t *buf, size_t cap,
                                     const char *field)
{
    uint8_t payload[64];
    moq_buf_writer_t pw;
    moq_buf_writer_init(&pw, payload, sizeof(payload));
    moq_bytes_t parts[] = { { (const uint8_t *)field, strlen(field) } };
    moq_namespace_t ns = { parts, 1 };
    moq_buf_write_namespace_prefix(&pw, &ns);
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    moq_control_encode_envelope(&w, MOQ_D18_NAMESPACE, payload,
                                (uint16_t)moq_buf_writer_offset(&pw));
    return moq_buf_writer_offset(&w);
}

static int p18ns_fx_teardown(p18ns_fx_t *f, const char *op)
{
    int bad = 0;
    moq_event_t ev; moq_action_t a;
    while (moq_session_poll_events(f->c, &ev, 1) > 0) moq_event_cleanup(&ev);
    while (moq_session_poll_actions(f->c, &a, 1) > 0) moq_action_cleanup(&a);
    bad += fp_sticky_clean(&f->fs, op);
    moq_session_destroy(f->c);
    if (f->fs.balance != 0 || f->fs.live_bytes != 0 || f->fs.table_len != 0) {
        fprintf(stderr, "FAILPOINT %s: destroy left balance %lld, live %lld, "
                "table %zu\n", op, (long long)f->fs.balance,
                (long long)f->fs.live_bytes, f->fs.table_len);
        bad++;
    }
    return bad;
}

int main(void)
{
    int failures = 0;
    failures += test_codec();
    failures += test_ns_probe_selfcheck();
    failures += test_ns_quadratic_red();
    failures += test_ns_suffix_cap();
    failures += test_ns_suffix_cap_abi();

    /* == Inbound SUBSCRIBE_NAMESPACE surfaces the prefix + token ====== */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        MOQ_TEST_CHECK(s != NULL);
        moq_d18_msg_params_t p = { 0 };
        p.auth_token_count = 1;
        p.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_USE_VALUE;
        p.auth_tokens[0].token_type = 9;
        p.auth_tokens[0].token_value = MOQ_BYTES_LITERAL("pk");
        uint8_t msg[128];
        size_t n = encode_sns(msg, sizeof(msg), 0, "live", &p);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1),
            (int)MOQ_OK);
        bool got = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_NS_SUB_REQUEST) {
                got = true;
                MOQ_TEST_CHECK_EQ_SIZE(
                    ev.u.ns_sub_request.track_namespace_prefix.count, 1);
                failures += txs_check_part_bytes(
                    &ev.u.ns_sub_request.track_namespace_prefix, 0, "live", 4,
                    "surfaced ns_sub_request prefix");
                /* The token array is borrowed too: bound and pointer BEFORE
                 * the index below. */
                MOQ_TEST_CHECK(ev.u.ns_sub_request.token_count <=
                               MOQ_DECODED_MAX_TOKENS);
                MOQ_TEST_CHECK(ev.u.ns_sub_request.token_count == 0 ||
                               ev.u.ns_sub_request.tokens != NULL);
                MOQ_TEST_CHECK_EQ_U64(ev.u.ns_sub_request.namespace_interest,
                                      MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE);
                MOQ_TEST_CHECK_EQ_SIZE(ev.u.ns_sub_request.token_count, 1);
                if (ev.u.ns_sub_request.token_count == 1 &&
                    ev.u.ns_sub_request.tokens)
                    MOQ_TEST_CHECK_EQ_U64(
                        ev.u.ns_sub_request.tokens[0].token_type, 9);
                else MOQ_TEST_CHECK(0);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);

        /* Handoff registry invariant: idx_ns_by_ref owns the bidi and the stale
         * request-registry streamref key was reclaimed. */
        MOQ_TEST_CHECK(moq_index_find(s->idx_ns_by_ref, s->idx_ns_mask,
                                      ref._v) >= 0);
        MOQ_TEST_CHECK_EQ_INT(
            (int)request_registry_find_by_streamref(s, ref).kind,
            (int)MOQ_REQ_NONE);
        moq_session_destroy(s);
    }

    /* == Inbound fragmented byte-by-byte -> same event ================ */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        moq_d18_msg_params_t p = { 0 };
        uint8_t msg[128];
        size_t n = encode_sns(msg, sizeof(msg), 0, "live", &p);
        for (size_t i = 0; i < n; i++) {
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_session_on_bidi_stream_bytes(s, ref, &msg[i], 1,
                    false, 1), (int)MOQ_OK);
            MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        }
        int reqs = 0;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_NS_SUB_REQUEST) reqs++;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(reqs, 1);
        MOQ_TEST_CHECK(moq_index_find(s->idx_ns_by_ref, s->idx_ns_mask,
                                      ref._v) >= 0);
        moq_session_destroy(s);
    }

    /* == Outbound subscribe_namespace opens a request bidi ============ */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
        moq_subscribe_namespace_cfg_t cfg;
        moq_subscribe_namespace_cfg_init(&cfg);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        cfg.track_namespace_prefix = (moq_namespace_t){ parts, 1 };
        cfg.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
        moq_ns_sub_handle_t h;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_subscribe_namespace(s, &cfg, 1, &h), (int)MOQ_OK);
        bool opened = false;
        moq_action_t act;
        while (moq_session_poll_actions(s, &act, 1) > 0) {
            if (act.kind == MOQ_ACTION_OPEN_BIDI_STREAM) {
                moq_buf_reader_t r;
                moq_buf_reader_init(&r, act.u.open_bidi_stream.data,
                                    act.u.open_bidi_stream.len);
                moq_control_envelope_t env;
                if (moq_d18_decode_envelope(&r, &env) == MOQ_OK &&
                    env.msg_type == MOQ_D18_SUBSCRIBE_NAMESPACE)
                    opened = true;
            }
            moq_action_cleanup(&act);
        }
        MOQ_TEST_CHECK(opened);
        moq_session_destroy(s);
    }

    /* == Inbound accept -> REQUEST_OK (fin=false), then NAMESPACE ====== */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        moq_d18_msg_params_t p = { 0 };
        moq_ns_sub_handle_t h = feed_sns(s, ref, 0, "live", &p);
        moq_accept_ns_sub_cfg_t ac;
        moq_accept_ns_sub_cfg_init(&ac);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_accept_ns_sub(s, h, &ac, 1), (int)MOQ_OK);
        bool fin = true;
        MOQ_TEST_CHECK(action_has_msg(s, ref, MOQ_D18_REQUEST_OK, &fin));
        MOQ_TEST_CHECK(!fin);
        /* send_namespace -> NAMESPACE on the bidi. */
        moq_bytes_t sp[] = { MOQ_BYTES_LITERAL("room=1") };
        moq_namespace_t suffix = { sp, 1 };
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_send_namespace(s, h, &suffix, 1), (int)MOQ_OK);
        MOQ_TEST_CHECK(action_has_msg(s, ref, MOQ_D18_NAMESPACE, NULL));
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Inbound reject -> REQUEST_ERROR + FIN; trailing FIN absorbed == */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        moq_d18_msg_params_t p = { 0 };
        moq_ns_sub_handle_t h = feed_sns(s, ref, 0, "live", &p);
        moq_reject_ns_sub_cfg_t rc;
        moq_reject_ns_sub_cfg_init(&rc);
        rc.error_code = MOQ_REQUEST_ERROR_NOT_SUPPORTED;
        rc.reason = MOQ_BYTES_LITERAL("nope");
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_reject_ns_sub(s, h, &rc, 1), (int)MOQ_OK);
        bool fin = false;
        MOQ_TEST_CHECK(action_has_msg(s, ref, MOQ_D18_REQUEST_ERROR, &fin));
        MOQ_TEST_CHECK(fin);
        /* The peer FINs its request half after the error; absorbed via drain. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == PREFIX_OVERLAP: a second overlapping request is rejected ===== */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t r1 = moq_stream_ref_from_u64(1);
        moq_d18_msg_params_t p = { 0 };
        moq_ns_sub_handle_t h = feed_sns(s, r1, 0, "live", &p);
        moq_accept_ns_sub_cfg_t ac;
        moq_accept_ns_sub_cfg_init(&ac);
        moq_session_accept_ns_sub(s, h, &ac, 1);
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
        /* Same prefix on a new bidi (request id 2): PREFIX_OVERLAP, no event. */
        moq_stream_ref_t r2 = moq_stream_ref_from_u64(2);
        uint8_t msg[128];
        size_t n = encode_sns(msg, sizeof(msg), 2, "live", &p);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, r2, msg, n, false, 1),
            (int)MOQ_OK);
        int reqs = 0;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_NS_SUB_REQUEST) reqs++;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(reqs, 0);
        bool fin = false;
        MOQ_TEST_CHECK(action_has_msg(s, r2, MOQ_D18_REQUEST_ERROR, &fin));
        MOQ_TEST_CHECK(fin);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Auth reject: unknown USE_ALIAS -> REQUEST_ERROR + FIN, no event */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        moq_d18_msg_params_t p = { 0 };
        p.auth_token_count = 1;
        p.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_USE_ALIAS;
        p.auth_tokens[0].alias = 99;
        uint8_t msg[128];
        size_t n = encode_sns(msg, sizeof(msg), 0, "live", &p);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1),
            (int)MOQ_OK);
        bool any = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_NS_SUB_REQUEST) any = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(!any);
        bool fin = false;
        MOQ_TEST_CHECK(action_has_msg(s, ref, MOQ_D18_REQUEST_ERROR, &fin));
        MOQ_TEST_CHECK(fin);
        /* The rejected bidi is drained: a trailing FIN is absorbed. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Malformed auth token closes the session with 0x6 ============= */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        uint8_t msg[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        size_t len_off;
        moq_buf_write_vi64(&w, MOQ_D18_SUBSCRIBE_NAMESPACE);
        moq_buf_reserve_uint16(&w, &len_off);
        size_t start = moq_buf_writer_offset(&w);
        moq_buf_write_vi64(&w, 0);
        moq_buf_write_vi64(&w, 1);
        moq_buf_write_vi64(&w, 4);
        moq_buf_write_raw(&w, (const uint8_t *)"live", 4);
        moq_buf_write_vi64(&w, 1);
        moq_buf_write_vi64(&w, MOQ_D18_PARAM_AUTHORIZATION_TOKEN);
        moq_buf_write_vi64(&w, 1);
        moq_buf_write_vi64(&w, 9);                       /* invalid alias type */
        moq_buf_patch_uint16(&w, len_off,
            (uint16_t)(moq_buf_writer_offset(&w) - start));
        (void)moq_session_on_bidi_stream_bytes(s, ref, msg,
            moq_buf_writer_offset(&w), false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);
    }

    /* == A prefix with >32 fields closes the session ================== */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        uint8_t msg[256];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        size_t len_off;
        moq_buf_write_vi64(&w, MOQ_D18_SUBSCRIBE_NAMESPACE);
        moq_buf_reserve_uint16(&w, &len_off);
        size_t start = moq_buf_writer_offset(&w);
        moq_buf_write_vi64(&w, 0);
        moq_buf_write_vi64(&w, 33);                      /* 33 > 32 fields */
        for (int i = 0; i < 33; i++) {
            moq_buf_write_vi64(&w, 1);
            uint8_t c = 'a'; moq_buf_write_raw(&w, &c, 1);
        }
        moq_buf_write_vi64(&w, 0);                       /* 0 params */
        moq_buf_patch_uint16(&w, len_off,
            (uint16_t)(moq_buf_writer_offset(&w) - start));
        (void)moq_session_on_bidi_stream_bytes(s, ref, msg,
            moq_buf_writer_offset(&w), false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);
    }

    /* == REQUEST_UPDATE guard: bidi-only close, session stays up ======= *
     *  An inbound REQUEST_UPDATE on an established publisher-side ns_sub bidi is
     *  unmodelled; §10.9.1 closes only the bidi (RESET+STOP), not the session,
     *  and a second namespace subscription still works. */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        moq_d18_msg_params_t p = { 0 };
        moq_ns_sub_handle_t h = feed_sns(s, ref, 0, "live", &p);
        moq_accept_ns_sub_cfg_t ac;
        moq_accept_ns_sub_cfg_init(&ac);
        moq_session_accept_ns_sub(s, h, &ac, 1);
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);

        moq_d18_msg_params_t up = { 0 };
        uint8_t msg[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        moq_d18_encode_request_update(&w, 2, &up);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, msg,
                moq_buf_writer_offset(&w), false, 1), (int)MOQ_OK);
        int resets = 0, stops = 0;
        count_teardown(s, ref, &resets, &stops);
        MOQ_TEST_CHECK_EQ_INT(resets, 1);
        MOQ_TEST_CHECK_EQ_INT(stops, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        /* The bidi is gone; a late in-flight byte is drained, not reopened. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
            (int)MOQ_OK);
        /* A second, fresh ns_sub on a new bidi still works. */
        moq_stream_ref_t r2 = moq_stream_ref_from_u64(3);
        moq_ns_sub_handle_t h2 = feed_sns(s, r2, 2, "vod", &p);
        MOQ_TEST_CHECK(h2._opaque != MOQ_NS_SUB_HANDLE_INVALID._opaque);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Subscriber response path (white-box): REQUEST_OK then NAMESPACE */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
        moq_subscribe_namespace_cfg_t cfg;
        moq_subscribe_namespace_cfg_init(&cfg);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        cfg.track_namespace_prefix = (moq_namespace_t){ parts, 1 };
        cfg.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
        moq_ns_sub_handle_t h;
        moq_session_subscribe_namespace(s, &cfg, 1, &h);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0);
        moq_action_t act;
        while (moq_session_poll_actions(s, &act, 1) > 0) {
            if (act.kind == MOQ_ACTION_OPEN_BIDI_STREAM)
                ref = act.u.open_bidi_stream.stream_ref;
            moq_action_cleanup(&act);
        }
        /* REQUEST_OK -> NS_SUB_OK. */
        uint8_t ok[32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, ok, sizeof(ok));
        moq_d18_encode_request_ok(&w);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, ok,
                moq_buf_writer_offset(&w), false, 1), (int)MOQ_OK);
        bool ok_ev = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_NS_SUB_OK) ok_ev = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(ok_ev);
        /* NAMESPACE (suffix) -> NAMESPACE_FOUND; then NAMESPACE_DONE -> GONE. */
        moq_bytes_t sp[] = { MOQ_BYTES_LITERAL("room=1") };
        moq_namespace_t suffix = { sp, 1 };
        uint8_t nm[64];
        moq_buf_writer_init(&w, nm, sizeof(nm));
        moq_d18_encode_namespace_msg(&w, &suffix, false);
        moq_session_on_bidi_stream_bytes(s, ref, nm,
            moq_buf_writer_offset(&w), false, 1);
        bool found = false;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_NAMESPACE_FOUND) found = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(found);
        moq_buf_writer_init(&w, nm, sizeof(nm));
        moq_d18_encode_namespace_msg(&w, &suffix, true);
        moq_session_on_bidi_stream_bytes(s, ref, nm,
            moq_buf_writer_offset(&w), false, 1);
        bool gone = false;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_NAMESPACE_GONE) gone = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(gone);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Subscriber: NAMESPACE_DONE before NAMESPACE closes the session  */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
        moq_subscribe_namespace_cfg_t cfg;
        moq_subscribe_namespace_cfg_init(&cfg);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        cfg.track_namespace_prefix = (moq_namespace_t){ parts, 1 };
        cfg.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
        moq_ns_sub_handle_t h;
        moq_session_subscribe_namespace(s, &cfg, 1, &h);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0);
        moq_action_t act;
        while (moq_session_poll_actions(s, &act, 1) > 0) {
            if (act.kind == MOQ_ACTION_OPEN_BIDI_STREAM)
                ref = act.u.open_bidi_stream.stream_ref;
            moq_action_cleanup(&act);
        }
        uint8_t ok[32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, ok, sizeof(ok));
        moq_d18_encode_request_ok(&w);
        moq_session_on_bidi_stream_bytes(s, ref, ok,
            moq_buf_writer_offset(&w), false, 1);
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) moq_event_cleanup(&ev);
        /* NAMESPACE_DONE for a suffix never announced -> PROTOCOL_VIOLATION. */
        moq_bytes_t sp[] = { MOQ_BYTES_LITERAL("room=1") };
        moq_namespace_t suffix = { sp, 1 };
        uint8_t nm[64];
        moq_buf_writer_init(&w, nm, sizeof(nm));
        moq_d18_encode_namespace_msg(&w, &suffix, true);
        (void)moq_session_on_bidi_stream_bytes(s, ref, nm,
            moq_buf_writer_offset(&w), false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);
    }

    /* == Subscriber: a non-OK/ERROR first response closes the session == */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
        moq_subscribe_namespace_cfg_t cfg;
        moq_subscribe_namespace_cfg_init(&cfg);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        cfg.track_namespace_prefix = (moq_namespace_t){ parts, 1 };
        cfg.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
        moq_ns_sub_handle_t h;
        moq_session_subscribe_namespace(s, &cfg, 1, &h);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0);
        moq_action_t act;
        while (moq_session_poll_actions(s, &act, 1) > 0) {
            if (act.kind == MOQ_ACTION_OPEN_BIDI_STREAM)
                ref = act.u.open_bidi_stream.stream_ref;
            moq_action_cleanup(&act);
        }
        /* NAMESPACE as the first response message is a PROTOCOL_VIOLATION. */
        moq_bytes_t sp[] = { MOQ_BYTES_LITERAL("room=1") };
        moq_namespace_t suffix = { sp, 1 };
        uint8_t nm[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, nm, sizeof(nm));
        moq_d18_encode_namespace_msg(&w, &suffix, false);
        (void)moq_session_on_bidi_stream_bytes(s, ref, nm,
            moq_buf_writer_offset(&w), false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);
    }

    /* == Subscriber cancel -> CLOSE_BIDI (FIN), draft-18 §2249 ========= */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
        moq_subscribe_namespace_cfg_t cfg;
        moq_subscribe_namespace_cfg_init(&cfg);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        cfg.track_namespace_prefix = (moq_namespace_t){ parts, 1 };
        cfg.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
        moq_ns_sub_handle_t h;
        moq_session_subscribe_namespace(s, &cfg, 1, &h);
        moq_action_t act;
        while (moq_session_poll_actions(s, &act, 1) > 0) moq_action_cleanup(&act);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_cancel_namespace_sub(s, h, 1), (int)MOQ_OK);
        bool closed = false;
        while (moq_session_poll_actions(s, &act, 1) > 0) {
            if (act.kind == MOQ_ACTION_CLOSE_BIDI_STREAM) closed = true;
            moq_action_cleanup(&act);
        }
        MOQ_TEST_CHECK(closed);
        moq_session_destroy(s);
    }

    /* == End-to-end accept + delivery over SimPair ==================== */
    {
        moq_simpair_cfg_t scfg = MOQ_SIMPAIR_CFG_INIT;
        scfg.alloc = moq_alloc_default();
        scfg.version = MOQ_VERSION_DRAFT_18;
        moq_simpair_t *sp = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_simpair_create(&scfg, &sp), (int)MOQ_OK);
        moq_session_t *client = moq_simpair_client(sp);
        moq_session_t *server = moq_simpair_server(sp);
        MOQ_TEST_CHECK_EQ_INT((int)moq_simpair_start(sp), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        moq_subscribe_namespace_cfg_t cfg;
        moq_subscribe_namespace_cfg_init(&cfg);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        cfg.track_namespace_prefix = (moq_namespace_t){ parts, 1 };
        cfg.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
        moq_ns_sub_handle_t ch;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_subscribe_namespace(client, &cfg, 1, &ch), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        moq_ns_sub_handle_t shdl = MOQ_NS_SUB_HANDLE_INVALID;
        moq_event_t ev;
        while (moq_session_poll_events(server, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_NS_SUB_REQUEST)
                shdl = ev.u.ns_sub_request.handle;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(shdl._opaque != MOQ_NS_SUB_HANDLE_INVALID._opaque);
        moq_accept_ns_sub_cfg_t ac;
        moq_accept_ns_sub_cfg_init(&ac);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_accept_ns_sub(server, shdl, &ac,
                moq_simpair_now_us(sp)), (int)MOQ_OK);
        moq_bytes_t sp2[] = { MOQ_BYTES_LITERAL("room=1") };
        moq_namespace_t suffix = { sp2, 1 };
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_send_namespace(server, shdl, &suffix,
                moq_simpair_now_us(sp)), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        bool ok = false, found = false;
        while (moq_session_poll_events(client, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_NS_SUB_OK) ok = true;
            if (ev.kind == MOQ_EVENT_NAMESPACE_FOUND) {
                found = true;
                MOQ_TEST_CHECK_EQ_SIZE(
                    ev.u.namespace_found.track_namespace_suffix.count, 1);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(ok);
        MOQ_TEST_CHECK(found);
        MOQ_TEST_CHECK_EQ_INT((int)client->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT((int)server->state, (int)MOQ_SESS_ESTABLISHED);
        moq_simpair_destroy(sp);
    }

    /* == End-to-end reject over SimPair =============================== */
    {
        moq_simpair_cfg_t scfg = MOQ_SIMPAIR_CFG_INIT;
        scfg.alloc = moq_alloc_default();
        scfg.version = MOQ_VERSION_DRAFT_18;
        moq_simpair_t *sp = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_simpair_create(&scfg, &sp), (int)MOQ_OK);
        moq_session_t *client = moq_simpair_client(sp);
        moq_session_t *server = moq_simpair_server(sp);
        MOQ_TEST_CHECK_EQ_INT((int)moq_simpair_start(sp), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        moq_subscribe_namespace_cfg_t cfg;
        moq_subscribe_namespace_cfg_init(&cfg);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        cfg.track_namespace_prefix = (moq_namespace_t){ parts, 1 };
        cfg.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
        moq_ns_sub_handle_t ch;
        moq_session_subscribe_namespace(client, &cfg, 1, &ch);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        moq_ns_sub_handle_t shdl = MOQ_NS_SUB_HANDLE_INVALID;
        moq_event_t ev;
        while (moq_session_poll_events(server, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_NS_SUB_REQUEST)
                shdl = ev.u.ns_sub_request.handle;
            moq_event_cleanup(&ev);
        }
        moq_reject_ns_sub_cfg_t rc;
        moq_reject_ns_sub_cfg_init(&rc);
        rc.error_code = MOQ_REQUEST_ERROR_NOT_SUPPORTED;
        rc.reason = MOQ_BYTES_LITERAL("no");
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_reject_ns_sub(server, shdl, &rc,
                moq_simpair_now_us(sp)), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        bool rejected = false;
        while (moq_session_poll_events(client, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_NS_SUB_ERROR) rejected = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(rejected);
        MOQ_TEST_CHECK_EQ_INT((int)client->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT((int)server->state, (int)MOQ_SESS_ESTABLISHED);
        moq_simpair_destroy(sp);
    }

    /* == Unrepresentable namespace_interest returns INVAL (no silent drop) === */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_ns_sub_handle_t h;
        /* Default cfg interest is PUBLISHER_STATE(0); BOTH(2) is also track-bearing.
         * Both need SUBSCRIBE_TRACKS (deferred), so D18 rejects them. */
        moq_subscribe_namespace_cfg_t cfg;
        moq_subscribe_namespace_cfg_init(&cfg);
        cfg.track_namespace_prefix = (moq_namespace_t){ parts, 1 };
        cfg.namespace_interest = MOQ_NAMESPACE_INTEREST_PUBLISHER_STATE;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_subscribe_namespace(s, &cfg, 1, &h), (int)MOQ_ERR_INVAL);
        cfg.namespace_interest = MOQ_NAMESPACE_INTEREST_BOTH;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_subscribe_namespace(s, &cfg, 1, &h), (int)MOQ_ERR_INVAL);
        /* NAMESPACE_STATE is representable. */
        cfg.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_subscribe_namespace(s, &cfg, 1, &h), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Direct FIN on a publisher-side ns_sub tears the entry down ===== *
     *  A real bridge CLOSE_BIDI_STREAM delivers a FIN inline (not a reset);
     *  §2249 makes that a cancellation. The entry frees, session stays up. */
    {
        for (int established = 0; established < 2; established++) {
            moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
            moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
            moq_d18_msg_params_t p = { 0 };
            moq_ns_sub_handle_t h = feed_sns(s, ref, 0, "live", &p);
            if (established) {
                moq_accept_ns_sub_cfg_t ac;
                moq_accept_ns_sub_cfg_init(&ac);
                moq_session_accept_ns_sub(s, h, &ac, 1);
            }
            moq_action_t a;
            while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
            MOQ_TEST_CHECK(moq_index_find(s->idx_ns_by_ref, s->idx_ns_mask,
                                          ref._v) >= 0);
            /* FIN (len=0) directly on the publisher-side bidi. */
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
                (int)MOQ_OK);
            MOQ_TEST_CHECK(moq_index_find(s->idx_ns_by_ref, s->idx_ns_mask,
                                          ref._v) < 0);
            MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
            moq_session_destroy(s);
        }
    }

    /* == WOULD_BLOCK at the handoff event push: no orphaned staging slot = *
     *  max_events=1: the first request fills the event queue, so the second
     *  request's handoff event push WOULD_BLOCKs. The staging slot must be freed
     *  (no stuck sub slot, no stale registry key) and the empty re-feed must
     *  complete via the ns_sub path. */
    {
        moq_session_t *s = make_session_caps(MOQ_PERSPECTIVE_SERVER, 1, 0, 0);
        moq_stream_ref_t r1 = moq_stream_ref_from_u64(1);
        moq_stream_ref_t r2 = moq_stream_ref_from_u64(3);
        moq_d18_msg_params_t p = { 0 };
        uint8_t m1[128], m2[128];
        size_t n1 = encode_sns(m1, sizeof(m1), 0, "live", &p);
        size_t n2 = encode_sns(m2, sizeof(m2), 2, "vod", &p);
        /* First request: emits NS_SUB_REQUEST, fills the 1-slot event queue. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, r1, m1, n1, false, 1),
            (int)MOQ_OK);
        /* Second request: handoff created the ns_sub entry, event push blocks. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, r2, m2, n2, false, 1),
            (int)MOQ_ERR_WOULD_BLOCK);
        /* The staging slot was freed despite WOULD_BLOCK: no stuck slot, and the
         * request registry's stale streamref key was reclaimed. */
        MOQ_TEST_CHECK_EQ_INT(count_busy_subs(s), 0);
        MOQ_TEST_CHECK_EQ_INT(
            (int)request_registry_find_by_streamref(s, r2).kind, (int)MOQ_REQ_NONE);
        MOQ_TEST_CHECK(moq_index_find(s->idx_ns_by_ref, s->idx_ns_mask,
                                      r2._v) >= 0);
        /* Drain one event, then retry empty -> the second request completes. */
        moq_event_t ev; int reqs = 0;
        if (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_NS_SUB_REQUEST) reqs++;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, r2, NULL, 0, false, 1),
            (int)MOQ_OK);
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_NS_SUB_REQUEST) reqs++;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(reqs, 2);
        MOQ_TEST_CHECK_EQ_INT(count_busy_subs(s), 0);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Trailing bytes after the request are rejected on the blocked path = *
     *  Full event queue + (SUBSCRIBE_NAMESPACE followed by an extra byte in the
     *  same feed): the handoff reports the request consumed even though the event
     *  push blocks, so the trailing byte is a protocol violation -- it must close,
     *  not be silently dropped with the freed staging slot and then commit on the
     *  empty re-feed. */
    {
        moq_session_t *s = make_session_caps(MOQ_PERSPECTIVE_SERVER, 1, 0, 0);
        moq_stream_ref_t r1 = moq_stream_ref_from_u64(1);
        moq_stream_ref_t r2 = moq_stream_ref_from_u64(3);
        moq_d18_msg_params_t p = { 0 };
        uint8_t m1[128], m2[129];
        size_t n1 = encode_sns(m1, sizeof(m1), 0, "live", &p);
        size_t n2 = encode_sns(m2, sizeof(m2), 2, "vod", &p);
        m2[n2] = 0x00;                 /* one trailing byte after the envelope */
        /* Fill the 1-slot event queue. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, r1, m1, n1, false, 1),
            (int)MOQ_OK);
        /* Request + trailing byte while the event queue is full: closes. */
        (void)moq_session_on_bidi_stream_bytes(s, r2, m2, n2 + 1, false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);
    }

    /* == Send-buffer backpressure on accept is WOULD_BLOCK, not BUFFER === *
     *  A temporary remaining-buffer shortfall must be retryable (queue_send_bidi),
     *  not collapse to a hard BUFFER that would lose the response. */
    {
        moq_session_t *s = make_session_caps(MOQ_PERSPECTIVE_SERVER, 0, 16, 8);
        moq_d18_msg_params_t p = { 0 };
        const char *fields = "abcdefgh";
        moq_ns_sub_handle_t handles[8];
        for (int i = 0; i < 8; i++) {
            moq_stream_ref_t ref = moq_stream_ref_from_u64((uint64_t)(1 + i));
            char f[2] = { fields[i], 0 };
            handles[i] = feed_sns(s, ref, (uint64_t)(i * 2), f, &p);
        }
        /* Accept (REQUEST_OK) without polling actions until the send buffer is
         * exhausted: the blocking accept must be WOULD_BLOCK (retryable). */
        int blocked = -1;
        for (int i = 0; i < 8; i++) {
            moq_accept_ns_sub_cfg_t ac;
            moq_accept_ns_sub_cfg_init(&ac);
            moq_result_t rc = moq_session_accept_ns_sub(s, handles[i], &ac, 1);
            if (rc == MOQ_ERR_WOULD_BLOCK) { blocked = i; break; }
            MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
        }
        MOQ_TEST_CHECK(blocked >= 0);   /* WOULD_BLOCK occurred (not BUFFER) */
        /* Drain actions (reclaims the send buffer), retry -> succeeds. */
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
        moq_accept_ns_sub_cfg_t ac;
        moq_accept_ns_sub_cfg_init(&ac);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_accept_ns_sub(s, handles[blocked], &ac, 1),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == ns_sub pool full -> WOULD_BLOCK, retryable, no orphaned staging == */
    {
        moq_session_t *s = make_session_caps(MOQ_PERSPECTIVE_SERVER, 0, 0, 1);
        moq_d18_msg_params_t p = { 0 };
        moq_stream_ref_t r1 = moq_stream_ref_from_u64(1);
        moq_ns_sub_handle_t h = feed_sns(s, r1, 0, "live", &p);
        MOQ_TEST_CHECK(h._opaque != MOQ_NS_SUB_HANDLE_INVALID._opaque);
        /* Pool (cap 1) is full; a second request WOULD_BLOCKs without orphaning
         * the staging slot (it is retained for the re-feed). */
        moq_stream_ref_t r2 = moq_stream_ref_from_u64(3);
        uint8_t m2[128];
        size_t n2 = encode_sns(m2, sizeof(m2), 2, "vod", &p);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, r2, m2, n2, false, 1),
            (int)MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        /* Free the first (subscriber cancels via FIN), then retry the second. */
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
        moq_accept_ns_sub_cfg_t ac;
        moq_accept_ns_sub_cfg_init(&ac);
        moq_session_accept_ns_sub(s, h, &ac, 1);
        moq_session_on_bidi_stream_bytes(s, r1, NULL, 0, true, 1);  /* FIN cancel */
        while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, r2, NULL, 0, false, 1),
            (int)MOQ_OK);
        bool got = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_NS_SUB_REQUEST) got = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got);
        MOQ_TEST_CHECK_EQ_INT(count_busy_subs(s), 0);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Suffix-tracker allocation failure is transactional (parity) ==
     * The transactional NOMEM contract for inbound NAMESPACE handling is
     * swept per-origin on draft-16; the handler is shared, so ONE origin
     * here -- the tracker, the first allocation behind the event gate --
     * runs the SAME oracle through the draft-18 request-stream route:
     * declared signature and outcome row, pre-admitted empty-re-feed
     * equality (map, generic snapshot, hooks), reached-ordinal and prefix
     * agreement, then a recovery whose allocation sequence, ordered
     * normalized output, final snapshot, hook values and footprint all
     * equal a no-fault draft-18 baseline.
     *
     * Mutation inventory: identical to the draft-16 suite's -- tracker
     * pointer + inbound flag, buffered response bytes, receive budget,
     * event queue + scratch; entry state/generation, actions, occupancy
     * and request-id consumption must never move. */
    {
        static const fp_expect_t k_first_sig[4] = {
            { FP_ALLOC, FP_SIZE_EXACT, 8, 0 },      /* canonical key */
            { FP_ALLOC, FP_SIZE_ANY, 0, 0 },        /* tracker (private) */
            { FP_ALLOC, FP_SIZE_ANY, 0, 0 },        /* tree node (private) */
            { FP_ALLOC, FP_SIZE_SAME_AS, 0, 0 },    /* stored key copy (== key) */
        };
        static const fp_outcome_row_t k_first_out[4] = {
            { FP_PHASE_PRE_COMMIT, FP_NOMEM_RETAIN },
            { FP_PHASE_PRE_COMMIT, FP_NOMEM_RETAIN },
            { FP_PHASE_PRE_COMMIT, FP_NOMEM_RETAIN },
            { FP_PHASE_PRE_COMMIT, FP_NOMEM_RETAIN },
        };
        const uint64_t k = 2;   /* the tracker ordinal */
        MOQ_TEST_CHECK(k_first_out[k - 1].phase == FP_PHASE_PRE_COMMIT);
        MOQ_TEST_CHECK(k_first_out[k - 1].outcome == FP_NOMEM_RETAIN);

        fp_attempt_t   base_log[FP_LOG_CAP];
        txs_norm_vec_t base_out;
        txs_snapshot_t base_final;
        p18ns_hook_state_t base_hook;
        fp_delta_t     base_delta;
        size_t         base_budget_delta = 0;
        txs_norm_init(&base_out);

        /* Baseline: the un-failed operation on this route. */
        {
            p18ns_fx_t f;
            MOQ_TEST_CHECK(p18ns_fx_setup(&f, false) == 0);
            p18ns_hook_ctx_t hctx = { f.slot, f.h._opaque };
            txs_op_hooks_t hooks = p18ns_make_hooks(&hctx);

            fp_map_snap_t pre_op;
            fp_map_capture(&f.fs, &pre_op);
            size_t budget0 = f.c->recv_payload_bytes;
            uint8_t msg[128];
            size_t mlen = p18ns_encode_namespace(msg, sizeof(msg), "found");
            f.fs.log_from = f.fs.call_count;
            f.fs.log_len = 0;
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_session_on_bidi_stream_bytes(f.c, f.ref, msg, mlen,
                                                      false, 0),
                (int)MOQ_OK);
            failures += fp_check_signature(&f.fs, 0, k_first_sig, 4,
                                           "p18ns-baseline");
            memcpy(base_log, f.fs.log, f.fs.log_len * sizeof(fp_attempt_t));
            MOQ_TEST_CHECK_EQ_SIZE(
                sizeof(k_first_out) / sizeof(k_first_out[0]), f.fs.log_len);

            failures += p18ns_collect_output(f.c, &hooks, &base_out);
            txs_capture(f.c, &f.ref, 1, &base_final);
            hooks.capture(f.c, hooks.ctx, &base_hook);
            fp_delta_compute(&f.fs, &pre_op, &base_delta);
            base_budget_delta = f.c->recv_payload_bytes - budget0;
            MOQ_TEST_CHECK(base_budget_delta > 0);
            failures += p18ns_fx_teardown(&f, "p18ns-baseline");
        }

        /* The swept origin, pre-admitted, with its own recovery cycle. */
        {
            p18ns_fx_t f;
            MOQ_TEST_CHECK(p18ns_fx_setup(&f, true) == 0);
            p18ns_hook_ctx_t hctx = { f.slot, f.h._opaque };
            txs_op_hooks_t hooks = p18ns_make_hooks(&hctx);
            MOQ_TEST_CHECK(event_queue_full(f.c));

            fp_map_snap_t pre_feed;
            fp_map_capture(&f.fs, &pre_feed);
            uint8_t msg[128];
            size_t mlen = p18ns_encode_namespace(msg, sizeof(msg), "found");
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_session_on_bidi_stream_bytes(f.c, f.ref, msg, mlen,
                                                      false, 0),
                (int)MOQ_ERR_WOULD_BLOCK);
            failures += fp_map_equals(&f.fs, &pre_feed, "p18ns-preadmit");
            MOQ_TEST_CHECK(f.c->ns_subs[f.slot].recv_len > 0);

            moq_event_t ev;
            MOQ_TEST_CHECK(moq_session_poll_events(f.c, &ev, 1) == 1);
            MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NS_SUB_OK);
            moq_event_cleanup(&ev);
            MOQ_TEST_CHECK_EQ_SIZE(f.c->event_scratch_len, 0u);

            fp_map_snap_t m0;
            fp_map_capture(&f.fs, &m0);
            txs_snapshot_t s0;
            txs_capture(f.c, &f.ref, 1, &s0);
            p18ns_hook_state_t h0;
            hooks.capture(f.c, hooks.ctx, &h0);
            size_t budget0 = f.c->recv_payload_bytes;

            f.fs.log_from = f.fs.call_count;
            f.fs.log_len = 0;
            f.fs.fail_at = f.fs.call_count + k;
            moq_result_t rc = moq_session_on_bidi_stream_bytes(
                f.c, f.ref, NULL, 0, false, 0);
            fp_context("p18ns-sweep", k, 4, &f.fs);
            MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_ERR_NOMEM);
            MOQ_TEST_CHECK_EQ_U64(f.fs.call_count, f.fs.fail_at);
            f.fs.fail_at = 0;
            failures += fp_check_prefix(&f.fs, 0, base_log, (size_t)k,
                                        "p18ns-sweep");
            failures += fp_map_equals(&f.fs, &m0, "p18ns-sweep");
            failures += txs_check_eq(f.c, &f.ref, 1, &s0, "p18ns-sweep");
            failures += hooks.check(f.c, hooks.ctx, &h0);
            MOQ_TEST_CHECK(f.c->ns_subs[f.slot].announced_suffixes == NULL);
            MOQ_TEST_CHECK(moq_session_poll_events(f.c, &ev, 1) == 0);
            moq_action_t no_act;
            MOQ_TEST_CHECK(moq_session_poll_actions(f.c, &no_act, 1) == 0);

            /* Recovery against a NEW window: the sequence, not just the
             * footprint, must equal the baseline's. */
            f.fs.log_from = f.fs.call_count;
            f.fs.log_len = 0;
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_session_on_bidi_stream_bytes(f.c, f.ref, NULL, 0,
                                                      false, 0),
                (int)MOQ_OK);
            failures += fp_check_signature(&f.fs, 0, k_first_sig, 4,
                                           "p18ns-recovery");
            failures += fp_check_prefix(&f.fs, 0, base_log, 4,
                                        "p18ns-recovery");
            txs_norm_vec_t out;
            txs_norm_init(&out);
            failures += p18ns_collect_output(f.c, &hooks, &out);
            failures += txs_norm_equals(&out, &base_out, "p18ns-recovery");
            txs_norm_free(&out);

            txs_snapshot_t expect = base_final;
            expect.recv_payload_bytes = budget0 + base_budget_delta;
            failures += txs_check_eq(f.c, &f.ref, 1, &expect, "p18ns-final");
            failures += hooks.check_values(f.c, hooks.ctx, &base_hook);
            fp_delta_t delta;
            fp_delta_compute(&f.fs, &m0, &delta);
            failures += fp_delta_equals(&delta, &base_delta,
                                        "p18ns-recovery");
            MOQ_TEST_CHECK(f.c->ns_subs[f.slot].announced_suffixes != NULL);
            failures += p18ns_fx_teardown(&f, "p18ns-sweep");
        }

        txs_norm_free(&base_out);
    }

    MOQ_TEST_PASS("d18_subscribe_namespace");
    return failures != 0;
}
