/*
 * Layer C: session-level Largest Object negotiation, resolved-window
 * storage, update acknowledgment surface, entry-history lifecycle, automatic
 * write/receive feed, and inbound cap-exhaustion rejection (design §§1, 2, 2b,
 * 4). Driven end to end over moq-sim on BOTH draft-16 and draft-18, with
 * white-box registry / window inspection via moq-core-test-internals.
 */
#include <moq/codec.h>
#include <moq/control_d18.h>
#include <moq/sim.h>
#include "test_support.h"
#include "../../core/src/session/session_internal.h"
#include "../../core/src/wire/control_d18_internal.h"
#include <moq/wire.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

/* File-scope failure counter (MOQ_TEST_CHECK increments `failures`). */
static int failures = 0;

/* -- Allocator (balance-checked) ----------------------------------- */
typedef struct { int64_t balance; } lc_alloc_state_t;
static void *lc_alloc(size_t n, void *ctx)
{ lc_alloc_state_t *s = ctx; void *p = malloc(n); if (p) s->balance++; return p; }
static void *lc_realloc(void *p, size_t o, size_t n, void *ctx)
{ (void)o; (void)ctx; return realloc(p, n); }
static void lc_free(void *p, size_t n, void *ctx)
{ lc_alloc_state_t *s = ctx; (void)n; if (p) s->balance--; free(p); }
static moq_alloc_t lc_allocator(lc_alloc_state_t *s)
{ moq_alloc_t a = { s, lc_alloc, lc_realloc, lc_free }; return a; }

static moq_namespace_t ns1(moq_bytes_t *part, const char *s)
{ part->data = (const uint8_t *)s; part->len = strlen(s); moq_namespace_t ns = { part, 1 }; return ns; }

/* Find the active receive-stream ref carrying a subscription's subgroup data. */
static moq_stream_ref_t rx_ref_for_sub(moq_session_t *s, moq_subscription_t sub)
{
    for (size_t i = 0; i < s->rx_cap; i++) {
        moq_rx_stream_t *rx = &s->rx_streams[i];
        if (rx->active && moq_subscription_eq(rx->sub, sub))
            return rx->stream_ref;
    }
    return moq_stream_ref_from_u64(0);
}

static moq_track_hist_t *rec_of(moq_session_t *s, const char *nsstr,
                                const char *name)
{
    moq_bytes_t p; moq_namespace_t ns = ns1(&p, nsstr);
    moq_bytes_t nm = { (const uint8_t *)name, strlen(name) };
    size_t kl = 0; uint8_t *k = moq_build_track_key(s, &ns, nm, &kl);
    if (!k && kl > 0) return NULL;
    moq_track_hist_t *r = track_hist_find(s, k, kl);
    if (k) s->alloc.free(k, kl, s->alloc.ctx);
    return r;
}

/* Poll a session for the first event of `kind`; copies detail out, cleans up
 * everything else. Returns true if found (into *out). */
static bool poll_for(moq_session_t *s, moq_event_kind_t kind, moq_event_t *out)
{
    bool got = false;
    moq_event_t e;
    while (moq_session_poll_events(s, &e, 1) == 1) {
        if (!got && e.kind == kind) { *out = e; got = true; continue; }
        moq_event_cleanup(&e);
    }
    return got;
}

static void drain_events(moq_session_t *s)
{ moq_event_t e; while (moq_session_poll_events(s, &e, 1) == 1) moq_event_cleanup(&e); }

/* Client subscribes for ns/"track" with `filter`; server accepts with the given
 * seed largest. Returns the client-side SUBSCRIBE_OK largest via out params and
 * the server publisher-role subscription handle (for window inspection). */
static void subscribe_accept(moq_simpair_t *sp, moq_subscribe_filter_t filter,
    uint64_t f_sg, uint64_t f_so, uint64_t f_eg,
    bool seed_has, uint64_t seed_g, uint64_t seed_o,
    bool *ok_has, uint64_t *ok_g, uint64_t *ok_o,
    moq_subscription_t *server_sub_out)
{
    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);
    uint64_t now = moq_simpair_now_us(sp);
    moq_bytes_t p; moq_namespace_t ns = ns1(&p, "live");

    moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = ns;
    scfg.track_name = MOQ_BYTES_LITERAL("track");
    scfg.filter = filter;
    scfg.start_group = f_sg; scfg.start_object = f_so; scfg.end_group = f_eg;
    moq_subscription_t cl_sub;
    moq_session_subscribe(cl, &scfg, now, &cl_sub);
    moq_simpair_run_until_quiescent(sp, 16, NULL);

    moq_event_t req;
    bool got_req = poll_for(sv, MOQ_EVENT_SUBSCRIBE_REQUEST, &req);
    MOQ_TEST_CHECK(got_req);
    moq_subscription_t srv_sub = got_req ? req.u.subscribe_request.sub
                                         : (moq_subscription_t){0};
    if (got_req) moq_event_cleanup(&req);
    if (server_sub_out) *server_sub_out = srv_sub;

    moq_accept_subscribe_cfg_t acfg; moq_accept_subscribe_cfg_init(&acfg);
    acfg.has_largest = seed_has;
    acfg.largest_group = seed_g;
    acfg.largest_object = seed_o;
    MOQ_TEST_CHECK(moq_session_accept_subscribe(sv, srv_sub, &acfg, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 16, NULL);

    moq_event_t ok;
    bool got_ok = poll_for(cl, MOQ_EVENT_SUBSCRIBE_OK, &ok);
    MOQ_TEST_CHECK(got_ok);
    if (got_ok) {
        *ok_has = ok.u.subscribe_ok.has_largest;
        *ok_g = ok.u.subscribe_ok.largest_group;
        *ok_o = ok.u.subscribe_ok.largest_object;
        moq_event_cleanup(&ok);
    }
}

static moq_simpair_t *make_pair(moq_alloc_t *alloc, moq_version_t ver,
                                uint32_t th_cap)
{
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = alloc;
    cfg.seed = 1; cfg.initial_now_us = 1000;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 32;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 32;
    cfg.version = ver;
    cfg.max_track_history_records = th_cap;
    moq_simpair_t *sp = NULL;
    if (moq_simpair_create(&cfg, &sp) != MOQ_OK) return NULL;
    moq_simpair_start(sp);
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    return sp;
}

/* ===================================================================== */

/* Build a standalone draft-18 SERVER session with optional registry cap,
 * subscription-pool cap, and SETUP. Used to feed raw SUBSCRIBE bytes with
 * controlled FIN timing / pool pressure. */
static moq_session_t *d18_server_ex(moq_alloc_t *alloc, uint32_t th_cap,
                                    uint32_t max_subs, bool do_setup)
{
    moq_session_cfg_t cfg = MOQ_SESSION_CFG_INIT;
    cfg.alloc = alloc;
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.version = MOQ_VERSION_DRAFT_18;
    if (th_cap) cfg.max_track_history_records = th_cap;
    if (max_subs) cfg.max_subscriptions = max_subs;
    moq_session_t *s = NULL;
    if (moq_session_create(&cfg, 0, &s) < 0) return NULL;
    moq_session_start(s, 0);
    moq_action_t a; while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
    if (do_setup) {
        uint8_t setup[32]; moq_buf_writer_t w;
        moq_buf_writer_init(&w, setup, sizeof(setup));
        moq_d18_encode_setup(&w);
        moq_session_on_control_bytes(s, setup, moq_buf_writer_offset(&w), 0);
        moq_event_t e; while (moq_session_poll_events(s, &e, 1) > 0) moq_event_cleanup(&e);
    }
    return s;
}

static moq_session_t *d18_server_established(moq_alloc_t *alloc, uint32_t th_cap)
{ return d18_server_ex(alloc, th_cap, 0, true); }

/* Feed a raw draft-18 SUBSCRIBE for "live"/track on a fresh request bidi, with
 * the peer's FIN either riding the request (same-call) or absent (trailing).
 * Returns the session-input result so callers can assert it. */
static moq_result_t feed_d18_subscribe(moq_session_t *s, moq_stream_ref_t ref,
                                       const char *track, bool fin)
{
    uint8_t buf[128]; moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_namespace_t ns = { parts, 1 };
    moq_bytes_t tn = { (const uint8_t *)track, strlen(track) };
    moq_d18_msg_params_t mp = {0};
    /* request_id 0: even parity + matches peer_next_request_id for a server's
     * first inbound client request (draft-18 §10.3 request-id rules). Only the
     * first (dispatched) SUBSCRIBE has its id validated; pool-exhausted staging
     * rejects before dispatch, so a shared id 0 is fine there. */
    moq_result_t erc = moq_d18_encode_subscribe(&w, 0 /* request_id */, &ns, tn, &mp);
    if (erc < 0) return erc;
    return moq_session_on_bidi_stream_bytes(s, ref, buf,
                                            moq_buf_writer_offset(&w), fin, 0);
}

/* Decode the next SEND_BIDI_STREAM action on `ref` and return its d18 msg type
 * (or UINT64_MAX if none). Cleans up all polled actions. */
static uint64_t poll_bidi_msg_type(moq_session_t *s, moq_stream_ref_t ref,
                                   bool *out_fin)
{
    uint64_t mt = UINT64_MAX;
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) == 1) {
        if (mt == UINT64_MAX && a.kind == MOQ_ACTION_SEND_BIDI_STREAM &&
            a.u.send_bidi_stream.stream_ref._v == ref._v) {
            moq_buf_reader_t rr;
            moq_buf_reader_init(&rr, a.u.send_bidi_stream.data,
                                a.u.send_bidi_stream.len);
            moq_control_envelope_t env;
            if (moq_d18_decode_envelope(&rr, &env) == MOQ_OK) mt = env.msg_type;
            if (out_fin) *out_fin = a.u.send_bidi_stream.fin;
        }
        moq_action_cleanup(&a);
    }
    return mt;
}

/* Direct: a rejected draft-18 SUBSCRIBE drains ONLY when its FIN
 * has not already arrived. Registry cap 1 (pre-seeded) forces a history-cap
 * reject through sub_reject_terminal, driven at the bidi level so request_fin is
 * exercised directly. */
static void test_d18_subscribe_reject_fin_accounting(void)
{
    /* SAME-CALL FIN: the SUBSCRIBE arrives with its FIN -> no drain ref retained,
     * session survives, no SUBSCRIBE_REQUEST surfaced. */
    {
        lc_alloc_state_t as = {0};
        moq_alloc_t alloc = lc_allocator(&as);
        moq_session_t *s = d18_server_established(&alloc, 1);
        MOQ_TEST_CHECK(s != NULL);
        if (!s) return;
        moq_bytes_t p; moq_namespace_t ns = ns1(&p, "live");
        MOQ_TEST_CHECK(moq_session_note_object_published(
            s, &ns, MOQ_BYTES_LITERAL("seed"), 1, 0) == MOQ_OK);   /* registry full */
        MOQ_TEST_CHECK(feed_d18_subscribe(
            s, moq_stream_ref_from_u64(4), "x", /*fin*/true) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(s) == MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, (size_t)0);     /* no drain retained */
        int nreq = 0; moq_event_t e;
        while (moq_session_poll_events(s, &e, 1) == 1) {
            if (e.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) nreq++;
            moq_event_cleanup(&e);
        }
        MOQ_TEST_CHECK_EQ_INT(nreq, 0);
        moq_session_destroy(s);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* TRAILING FIN: the SUBSCRIBE arrives without FIN -> a drain ref is retained
     * (count 1) so the peer's later empty FIN is absorbed (1 -> 0) and the
     * session survives -- never a PROTOCOL_VIOLATION close. */
    {
        lc_alloc_state_t as = {0};
        moq_alloc_t alloc = lc_allocator(&as);
        moq_session_t *s = d18_server_established(&alloc, 1);
        MOQ_TEST_CHECK(s != NULL);
        if (!s) return;
        moq_bytes_t p; moq_namespace_t ns = ns1(&p, "live");
        MOQ_TEST_CHECK(moq_session_note_object_published(
            s, &ns, MOQ_BYTES_LITERAL("seed"), 1, 0) == MOQ_OK);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(4);
        MOQ_TEST_CHECK(feed_d18_subscribe(s, ref, "x", /*fin*/false) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(s) == MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, (size_t)1);     /* drain retained */
        /* The peer's trailing empty FIN is absorbed by the drain ring: 1 -> 0. */
        moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 0);
        MOQ_TEST_CHECK(moq_session_state(s) == MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, (size_t)0);     /* 1 -> 0 */
        moq_session_destroy(s);
        MOQ_TEST_CHECK(as.balance == 0);
    }
}

/* Direct: GENUINE subscription-pool exhaustion (sub_find_free()
 * == -1, max_subscriptions filled) is local resource pressure, never a
 * PROTOCOL_VIOLATION. Post-setup -> graceful REQUEST_ERROR + FIN on the bidi
 * (drain only when the peer's FIN has not yet arrived). Pre-setup -> the bidi is
 * RESET (STOP_SENDING + RESET_STREAM) and the session is preserved (draft-18
 * §3.3: an early request bidi may be reset when it cannot be buffered). */
static void test_d18_subscribe_pool_exhaustion(void)
{
    /* POST-SETUP, SAME-CALL FIN: pool of 1 held by a pending subscribe; the
     * second SUBSCRIBE (with FIN) is rejected with REQUEST_ERROR, no drain. */
    {
        lc_alloc_state_t as = {0};
        moq_alloc_t alloc = lc_allocator(&as);
        moq_session_t *s = d18_server_ex(&alloc, 0, /*max_subs*/1, /*setup*/true);
        MOQ_TEST_CHECK(s != NULL);
        if (!s) return;
        /* #1 fills the one slot (dispatched, pending accept -- not freed). */
        MOQ_TEST_CHECK(feed_d18_subscribe(
            s, moq_stream_ref_from_u64(4), "a", /*fin*/true) == MOQ_OK);
        int nreq = 0; moq_event_t e;
        while (moq_session_poll_events(s, &e, 1) == 1) {
            if (e.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) nreq++;
            moq_event_cleanup(&e);
        }
        MOQ_TEST_CHECK_EQ_INT(nreq, 1);
        { moq_action_t a; while (moq_session_poll_actions(s, &a, 1) == 1) moq_action_cleanup(&a); }
        /* #2 hits sub_find_free()==-1 -> graceful reject, session survives. */
        moq_stream_ref_t r2 = moq_stream_ref_from_u64(8);
        MOQ_TEST_CHECK(feed_d18_subscribe(s, r2, "b", /*fin*/true) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(s) == MOQ_SESS_ESTABLISHED);
        bool efin = false;
        MOQ_TEST_CHECK_EQ_U64(poll_bidi_msg_type(s, r2, &efin), MOQ_D18_REQUEST_ERROR);
        MOQ_TEST_CHECK(efin);                                  /* terminal FIN */
        MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, (size_t)0); /* same-call FIN */
        moq_session_destroy(s);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* POST-SETUP, TRAILING FIN: reject retains one drain ref; the peer's later
     * empty FIN is absorbed (1 -> 0). */
    {
        lc_alloc_state_t as = {0};
        moq_alloc_t alloc = lc_allocator(&as);
        moq_session_t *s = d18_server_ex(&alloc, 0, /*max_subs*/1, /*setup*/true);
        MOQ_TEST_CHECK(s != NULL);
        if (!s) return;
        MOQ_TEST_CHECK(feed_d18_subscribe(
            s, moq_stream_ref_from_u64(4), "a", /*fin*/true) == MOQ_OK);
        { moq_event_t e; while (moq_session_poll_events(s, &e, 1) == 1) moq_event_cleanup(&e); }
        { moq_action_t a; while (moq_session_poll_actions(s, &a, 1) == 1) moq_action_cleanup(&a); }
        moq_stream_ref_t r2 = moq_stream_ref_from_u64(8);
        MOQ_TEST_CHECK(feed_d18_subscribe(s, r2, "b", /*fin*/false) == MOQ_OK);
        bool efin = false;
        MOQ_TEST_CHECK_EQ_U64(poll_bidi_msg_type(s, r2, &efin), MOQ_D18_REQUEST_ERROR);
        MOQ_TEST_CHECK(efin);
        MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, (size_t)1); /* drain retained */
        moq_session_on_bidi_stream_bytes(s, r2, NULL, 0, true, 0);   /* trailing FIN */
        MOQ_TEST_CHECK(moq_session_state(s) == MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, (size_t)0); /* 1 -> 0 */
        moq_session_destroy(s);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* PRE-SETUP exhaustion: an early SUBSCRIBE fills the one staging slot; the
     * second early SUBSCRIBE cannot be buffered, so its bidi is RESET
     * (STOP_SENDING + RESET_STREAM) and the SESSION is preserved -- NOT a
     * PROTOCOL_VIOLATION close (the shipped behavior before this fix). */
    {
        lc_alloc_state_t as = {0};
        moq_alloc_t alloc = lc_allocator(&as);
        moq_session_t *s = d18_server_ex(&alloc, 0, /*max_subs*/1, /*setup*/false);
        MOQ_TEST_CHECK(s != NULL);
        if (!s) return;
        MOQ_TEST_CHECK(moq_session_state(s) != MOQ_SESS_ESTABLISHED); /* pre-setup */
        /* #1 reserves the one staging slot (buffered, not dispatched). */
        MOQ_TEST_CHECK(feed_d18_subscribe(
            s, moq_stream_ref_from_u64(4), "a", /*fin*/false) == MOQ_OK);
        { moq_action_t a; while (moq_session_poll_actions(s, &a, 1) == 1) moq_action_cleanup(&a); }
        /* #2 cannot get a slot -> reset that stream only, session survives. */
        moq_stream_ref_t r2 = moq_stream_ref_from_u64(8);
        MOQ_TEST_CHECK(feed_d18_subscribe(s, r2, "b", /*fin*/false) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(s) != MOQ_SESS_CLOSED);
        MOQ_TEST_CHECK(moq_session_state(s) != MOQ_SESS_DRAINING);
        int nstop = 0, nreset = 0;
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) == 1) {
            if (a.kind == MOQ_ACTION_STOP_BIDI_STREAM &&
                a.u.stop_bidi_stream.stream_ref._v == r2._v) nstop++;
            if (a.kind == MOQ_ACTION_RESET_BIDI_STREAM &&
                a.u.reset_bidi_stream.stream_ref._v == r2._v) nreset++;
            moq_action_cleanup(&a);
        }
        MOQ_TEST_CHECK_EQ_INT(nstop, 1);
        MOQ_TEST_CHECK_EQ_INT(nreset, 1);
        MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, (size_t)1); /* trailing, no FIN */
        moq_session_destroy(s);
        MOQ_TEST_CHECK(as.balance == 0);
    }
}

static void test_varint_boundary(void)
{
    /* moq_loc_successor + moq_resolve_filter_window carry against a
     * PROFILE-SPECIFIC ceiling (design §4): draft-16 QUIC varint (2^62-1),
     * draft-18 vi64 (UINT64_MAX). Carry, never saturate-to-empty. */
    const uint64_t D16 = MOQ_QUIC_VARINT_MAX;   /* draft-16 ceiling */
    const uint64_t D18 = UINT64_MAX;            /* draft-18 ceiling (vi64) */
    uint64_t g = 0, o = 0;
    /* Normal object++ (ceiling-independent). */
    MOQ_TEST_CHECK(!moq_loc_successor(3, 7, D16, &g, &o) && g == 3 && o == 8);
    /* d16: object at 2^62-1 carries to {group+1, 0}. */
    MOQ_TEST_CHECK(!moq_loc_successor(3, D16, D16, &g, &o) && g == 4 && o == 0);
    /* d16: both at 2^62-1 -> no successor. */
    MOQ_TEST_CHECK(moq_loc_successor(D16, D16, D16, &g, &o));
    /* d18: 2^62-1 is NOT the ceiling -- object++ stays in the same group. */
    MOQ_TEST_CHECK(!moq_loc_successor(3, D16, D18, &g, &o) &&
                   g == 3 && o == D16 + 1);
    /* d18: object at UINT64_MAX carries to {group+1, 0}. */
    MOQ_TEST_CHECK(!moq_loc_successor(3, D18, D18, &g, &o) && g == 4 && o == 0);
    /* d18: both at UINT64_MAX -> no successor. */
    MOQ_TEST_CHECK(moq_loc_successor(D18, D18, D18, &g, &o));

    moq_resolved_window_t w;
    /* d16 LargestObject with LO at ceiling -> start {LG+1,0}, NOT empty. */
    moq_resolve_filter_window(MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT, 0, 0, 0,
                              true, 5, D16, D16, &w);
    MOQ_TEST_CHECK(w.has_window && !w.unsatisfiable &&
                   w.start_group == 6 && w.start_object == 0);
    /* d16 LargestObject at double ceiling -> unsatisfiable. */
    moq_resolve_filter_window(MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT, 0, 0, 0,
                              true, D16, D16, D16, &w);
    MOQ_TEST_CHECK(w.has_window && w.unsatisfiable);
    /* d18: the SAME {D16,D16} snapshot is far below the vi64 ceiling -> a plain
     * object++ within the group, satisfiable. */
    moq_resolve_filter_window(MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT, 0, 0, 0,
                              true, D16, D16, D18, &w);
    MOQ_TEST_CHECK(w.has_window && !w.unsatisfiable &&
                   w.start_group == D16 && w.start_object == D16 + 1);
    /* d18 LargestObject at the vi64 double ceiling -> unsatisfiable. */
    moq_resolve_filter_window(MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT, 0, 0, 0,
                              true, D18, D18, D18, &w);
    MOQ_TEST_CHECK(w.has_window && w.unsatisfiable);
    /* NextGroup -> {LG+1,0}; empty only at the profile ceiling group. */
    moq_resolve_filter_window(MOQ_SUBSCRIBE_FILTER_NEXT_GROUP, 0, 0, 0,
                              true, 9, 3, D16, &w);
    MOQ_TEST_CHECK(w.has_window && !w.unsatisfiable &&
                   w.start_group == 10 && w.start_object == 0);
    moq_resolve_filter_window(MOQ_SUBSCRIBE_FILTER_NEXT_GROUP, 0, 0, 0,
                              true, D16, 0, D16, &w);
    MOQ_TEST_CHECK(w.has_window && w.unsatisfiable);            /* d16 ceiling */
    moq_resolve_filter_window(MOQ_SUBSCRIBE_FILTER_NEXT_GROUP, 0, 0, 0,
                              true, D16, 0, D18, &w);
    MOQ_TEST_CHECK(w.has_window && !w.unsatisfiable &&
                   w.start_group == D16 + 1);                   /* d18: not ceiling */
    /* AbsoluteRange stores a finite end. */
    moq_resolve_filter_window(MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE, 2, 1, 8,
                              false, 0, 0, D16, &w);
    MOQ_TEST_CHECK(w.has_window && w.has_end && w.start_group == 2 &&
                   w.start_object == 1 && w.end_group == 8);
    /* Relative with no snapshot resolves to an open window from origin. */
    moq_resolve_filter_window(MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT, 0, 0, 0,
                              false, 0, 0, D16, &w);
    MOQ_TEST_CHECK(w.has_window && !w.unsatisfiable &&
                   w.start_group == 0 && w.start_object == 0);
}

static void test_accept_snapshot(moq_version_t ver)
{
    lc_alloc_state_t as = {0};
    moq_alloc_t alloc = lc_allocator(&as);
    moq_simpair_t *sp = make_pair(&alloc, ver, 0);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *sv = moq_simpair_server(sp);

    /* Seed the server registry for the track (a prior observation): (7,2). */
    moq_bytes_t p; moq_namespace_t ns = ns1(&p, "live");
    MOQ_TEST_CHECK(moq_session_note_object_published(
        sv, &ns, MOQ_BYTES_LITERAL("track"), 7, 2) == MOQ_OK);

    /* Accept with a LOWER seed (3,0): SUBSCRIBE_OK must carry max = (7,2). */
    bool ok_has = false; uint64_t ok_g = 0, ok_o = 0;
    moq_subscription_t srv_sub;
    subscribe_accept(sp, MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT, 0, 0, 0,
                     true, 3, 0, &ok_has, &ok_g, &ok_o, &srv_sub);
    MOQ_TEST_CHECK(ok_has);
    MOQ_TEST_CHECK_EQ_INT((int)ok_g, 7);
    MOQ_TEST_CHECK_EQ_INT((int)ok_o, 2);

    /* The stored window used the SAME snapshot: LargestObject start = (7,3). */
    const moq_resolved_window_t *w = moq_session_sub_resolved_window(sv, srv_sub);
    MOQ_TEST_CHECK(w != NULL);
    MOQ_TEST_CHECK(w && w->filter == MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT);
    MOQ_TEST_CHECK(w && w->start_group == 7 && w->start_object == 3);

    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* full-width draft-18 locations (above the 2^62-1 QUIC-varint bound)
 * are accepted on the negotiation path -- the public note API AND the accept
 * snapshot / SUBSCRIBE_OK -- under draft-18 (vi64), and rejected under
 * draft-16. */
static void test_d18_wide_locations(void)
{
    const uint64_t WIDE = MOQ_QUIC_VARINT_MAX + 100;   /* > 2^62-1, < 2^64-1 */

    /* draft-18: the note API accepts a full-width location and records it. */
    {
        lc_alloc_state_t as = {0};
        moq_alloc_t alloc = lc_allocator(&as);
        moq_simpair_t *sp = make_pair(&alloc, MOQ_VERSION_DRAFT_18, 0);
        MOQ_TEST_CHECK(sp != NULL);
        if (!sp) return;
        moq_session_t *sv = moq_simpair_server(sp);
        moq_bytes_t p; moq_namespace_t ns = ns1(&p, "live");
        MOQ_TEST_CHECK(moq_session_note_object_published(
            sv, &ns, MOQ_BYTES_LITERAL("track"), WIDE, 7) == MOQ_OK);
        moq_track_hist_t *r = rec_of(sv, "live", "track");
        MOQ_TEST_CHECK(r && r->has_largest &&
                       r->largest_group == WIDE && r->largest_object == 7);

        /* Accept snapshot carries the full-width largest in SUBSCRIBE_OK. */
        bool ok_has = false; uint64_t ok_g = 0, ok_o = 0;
        moq_subscription_t srv_sub;
        subscribe_accept(sp, MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT, 0, 0, 0,
                         true, WIDE, 7, &ok_has, &ok_g, &ok_o, &srv_sub);
        MOQ_TEST_CHECK(ok_has);
        MOQ_TEST_CHECK(ok_g == WIDE && ok_o == 7);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* draft-16: the same full-width location is above the QUIC-varint ceiling
     * and is rejected (no record). */
    {
        lc_alloc_state_t as = {0};
        moq_alloc_t alloc = lc_allocator(&as);
        moq_simpair_t *sp = make_pair(&alloc, MOQ_VERSION_DRAFT_16, 0);
        MOQ_TEST_CHECK(sp != NULL);
        if (!sp) return;
        moq_session_t *sv = moq_simpair_server(sp);
        moq_bytes_t p; moq_namespace_t ns = ns1(&p, "live");
        MOQ_TEST_CHECK(moq_session_note_object_published(
            sv, &ns, MOQ_BYTES_LITERAL("track"), WIDE, 0) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(rec_of(sv, "live", "track") == NULL);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
    }
}

/* under draft-16 the QUIC-varint ceiling also
 * governs the negotiation-supplied largest. A moq_session_accept_subscribe whose
 * accept cfg carries a largest above 2^62-1 is rejected with MOQ_ERR_INVAL
 * BEFORE any mutation (no resolved window stored, subscription still acceptable);
 * a valid retry then succeeds and delivers SUBSCRIBE_OK. */
static void test_d16_accept_subscribe_ceiling_reject(void)
{
    const uint64_t WIDE = MOQ_QUIC_VARINT_MAX + 100;   /* > 2^62-1 (d16 ceiling) */
    lc_alloc_state_t as = {0};
    moq_alloc_t alloc = lc_allocator(&as);
    moq_simpair_t *sp = make_pair(&alloc, MOQ_VERSION_DRAFT_16, 0);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);
    uint64_t now = moq_simpair_now_us(sp);
    moq_bytes_t p; moq_namespace_t ns = ns1(&p, "live");

    moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = ns; scfg.track_name = MOQ_BYTES_LITERAL("track");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    moq_subscription_t cl_sub;
    moq_session_subscribe(cl, &scfg, now, &cl_sub);
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    moq_event_t req; MOQ_TEST_CHECK(poll_for(sv, MOQ_EVENT_SUBSCRIBE_REQUEST, &req));
    moq_subscription_t srv_sub = req.u.subscribe_request.sub; moq_event_cleanup(&req);

    /* Accept carrying an over-ceiling largest: rejected, nothing mutated. */
    moq_accept_subscribe_cfg_t bad; moq_accept_subscribe_cfg_init(&bad);
    bad.has_largest = true; bad.largest_group = WIDE; bad.largest_object = 0;
    MOQ_TEST_CHECK(moq_session_accept_subscribe(sv, srv_sub, &bad, now) == MOQ_ERR_INVAL);
    MOQ_TEST_CHECK(moq_session_sub_resolved_window(sv, srv_sub) == NULL);
    /* No SUBSCRIBE_OK escaped to the client. */
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    { moq_event_t e; while (moq_session_poll_events(cl, &e, 1) == 1) {
        MOQ_TEST_CHECK(e.kind != MOQ_EVENT_SUBSCRIBE_OK); moq_event_cleanup(&e); } }

    /* Valid retry (within-ceiling largest) succeeds: window stored, SUBSCRIBE_OK. */
    moq_accept_subscribe_cfg_t good; moq_accept_subscribe_cfg_init(&good);
    good.has_largest = true; good.largest_group = 9; good.largest_object = 2;
    MOQ_TEST_CHECK(moq_session_accept_subscribe(sv, srv_sub, &good, now) == MOQ_OK);
    const moq_resolved_window_t *w = moq_session_sub_resolved_window(sv, srv_sub);
    MOQ_TEST_CHECK(w != NULL);
    MOQ_TEST_CHECK(w && w->start_group == 9 && w->start_object == 3);
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    moq_event_t ok; MOQ_TEST_CHECK(poll_for(cl, MOQ_EVENT_SUBSCRIBE_OK, &ok));
    MOQ_TEST_CHECK(ok.u.subscribe_ok.has_largest);
    MOQ_TEST_CHECK_EQ_INT((int)ok.u.subscribe_ok.largest_group, 9);
    MOQ_TEST_CHECK_EQ_INT((int)ok.u.subscribe_ok.largest_object, 2);
    moq_event_cleanup(&ok);

    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* the internal REQUEST_UPDATE_OK codec round-trips distinct nonzero
 * LARGEST_OBJECT and EXPIRES response parameters, and rejects disallowed
 * parameters (both encode- and decode-side) and trailing Track Properties. */
static void test_d18_request_update_ok_codec(void)
{
    /* Round-trip: distinct nonzero Largest (g,o) AND a nonzero EXPIRES. */
    {
        moq_d18_msg_params_t p = {0};
        p.has_largest = true; p.largest_group = 12345; p.largest_object = 678;
        p.has_expires = true; p.expires_ms = 999;
        uint8_t buf[64]; moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_d18_encode_request_update_ok(&w, &p) == MOQ_OK);

        moq_buf_reader_t rr;
        moq_buf_reader_init(&rr, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        MOQ_TEST_CHECK(moq_d18_decode_envelope(&rr, &env) == MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(env.msg_type, MOQ_D18_REQUEST_OK);
        moq_d18_msg_params_t out;
        MOQ_TEST_CHECK(moq_d18_decode_request_update_ok(
            env.payload, env.payload_len, &out) == MOQ_OK);
        MOQ_TEST_CHECK(out.has_largest);
        MOQ_TEST_CHECK_EQ_U64(out.largest_group, 12345);
        MOQ_TEST_CHECK_EQ_U64(out.largest_object, 678);
        MOQ_TEST_CHECK(out.has_expires);
        MOQ_TEST_CHECK_EQ_U64(out.expires_ms, 999);
    }

    /* Encode rejects a parameter outside the LARGEST_OBJECT|EXPIRES mask. */
    {
        moq_d18_msg_params_t p = {0};
        p.has_forward = true; p.forward = 1;          /* FORWARD not permitted */
        uint8_t buf[64]; moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_d18_encode_request_update_ok(&w, &p) == MOQ_ERR_INVAL);
    }

    /* Decode rejects a disallowed parameter: hand-build [count=1][FORWARD]. */
    {
        moq_d18_msg_params_t forward = {0};
        forward.has_forward = true; forward.forward = 1;
        uint8_t body[32]; moq_buf_writer_t bw;
        moq_buf_writer_init(&bw, body, sizeof(body));
        MOQ_TEST_CHECK(moq_d18_encode_msg_params(&bw, &forward) == MOQ_OK);
        moq_d18_msg_params_t out;
        MOQ_TEST_CHECK(moq_d18_decode_request_update_ok(
            body, moq_buf_writer_offset(&bw), &out) == MOQ_ERR_PROTO);
    }

    /* Decode rejects trailing Track-Property bytes after the params (§10.5). */
    {
        moq_d18_msg_params_t p = {0};
        p.has_largest = true; p.largest_group = 3; p.largest_object = 4;
        uint8_t body[32]; moq_buf_writer_t bw;
        moq_buf_writer_init(&bw, body, sizeof(body));
        MOQ_TEST_CHECK(moq_d18_encode_msg_params(&bw, &p) == MOQ_OK);
        size_t n = moq_buf_writer_offset(&bw);
        MOQ_TEST_CHECK(n < sizeof(body));
        body[n] = 0x00;                               /* one trailing byte */
        moq_d18_msg_params_t out;
        MOQ_TEST_CHECK(moq_d18_decode_request_update_ok(
            body, n + 1, &out) == MOQ_ERR_PROTO);
    }
}

static void test_update_ok_event(moq_version_t ver)
{
    lc_alloc_state_t as = {0};
    moq_alloc_t alloc = lc_allocator(&as);
    moq_simpair_t *sp = make_pair(&alloc, ver, 0);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);

    bool ok_has; uint64_t ok_g, ok_o;
    moq_subscription_t srv_sub, cl_sub;
    /* Establish; capture the client subscription handle too. */
    {
        uint64_t now = moq_simpair_now_us(sp);
        moq_bytes_t p; moq_namespace_t ns = ns1(&p, "live");
        moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = ns; scfg.track_name = MOQ_BYTES_LITERAL("track");
        scfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        moq_session_subscribe(cl, &scfg, now, &cl_sub);
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        moq_event_t req; MOQ_TEST_CHECK(poll_for(sv, MOQ_EVENT_SUBSCRIBE_REQUEST, &req));
        srv_sub = req.u.subscribe_request.sub; moq_event_cleanup(&req);
        moq_accept_subscribe_cfg_t acfg; moq_accept_subscribe_cfg_init(&acfg);
        MOQ_TEST_CHECK(moq_session_accept_subscribe(sv, srv_sub, &acfg, now) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        drain_events(cl);
        (void)ok_has; (void)ok_g; (void)ok_o;
    }

    /* Server observes an object on the track (registry advances to (4,1)). */
    MOQ_TEST_CHECK(moq_session_note_object_published(
        sv, &(moq_namespace_t){ &(moq_bytes_t){(const uint8_t*)"live",4}, 1 },
        MOQ_BYTES_LITERAL("track"), 4, 1) == MOQ_OK);

    /* Client updates its subscription; the ack carries the server's largest and
     * fires MOQ_EVENT_SUBSCRIPTION_UPDATE_OK exactly once. */
    uint64_t now = moq_simpair_now_us(sp);
    moq_subscription_update_cfg_t ucfg; moq_subscription_update_cfg_init(&ucfg);
    ucfg.has_forward = true; ucfg.forward = true;
    MOQ_TEST_CHECK(moq_session_update_subscription(cl, cl_sub, &ucfg, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 16, NULL);

    /* Exactly one UPDATE_OK, carrying (4,1). */
    int n_ok = 0; bool ev_has = false; uint64_t ev_g = 0, ev_o = 0;
    moq_event_t e;
    while (moq_session_poll_events(cl, &e, 1) == 1) {
        if (e.kind == MOQ_EVENT_SUBSCRIPTION_UPDATE_OK) {
            n_ok++;
            ev_has = e.u.subscription_update_ok.has_largest;
            ev_g = e.u.subscription_update_ok.largest_group;
            ev_o = e.u.subscription_update_ok.largest_object;
        }
        moq_event_cleanup(&e);
    }
    MOQ_TEST_CHECK_EQ_INT(n_ok, 1);
    MOQ_TEST_CHECK(ev_has);
    MOQ_TEST_CHECK_EQ_INT((int)ev_g, 4);
    MOQ_TEST_CHECK_EQ_INT((int)ev_o, 1);

    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* MOQ_EVENT_PUBLICATION_UPDATE_OK: a subscriber that accepted a PUBLISH updates
 * it; the publisher's REQUEST_OK carries the publisher's Largest Object, which
 * fires PUBLICATION_UPDATE_OK exactly once on the updater (subscriber) side with
 * the correct scalars. */
/* Access the client sub entry's update_pending (white-box) for the invariant. */
static bool sub_update_pending(moq_session_t *s, moq_subscription_t h)
{
    int slot = sub_resolve_handle(s, h);
    return slot >= 0 && s->subs[slot].update_pending;
}
static bool pub_update_pending(moq_session_t *s, moq_publication_t h)
{
    int slot = pub_resolve_handle(s, h);
    return slot >= 0 && s->publishes[slot].update_pending;
}
static moq_stream_ref_t sub_req_ref(moq_session_t *s, moq_subscription_t h)
{
    int slot = sub_resolve_handle(s, h);
    return slot >= 0 ? s->subs[slot].request_stream_ref
                     : moq_stream_ref_from_u64(0);
}
static moq_stream_ref_t pub_req_ref(moq_session_t *s, moq_publication_t h)
{
    int slot = pub_resolve_handle(s, h);
    return slot >= 0 ? s->publishes[slot].request_stream_ref
                     : moq_stream_ref_from_u64(0);
}
/* Re-drive a session's deferred control/response processing DIRECTLY (no bridge
 * pump), after event-queue space was freed: draft-16 via process_pending (shared
 * control stream), draft-18 via an empty bidi input on the request stream ref
 * (the bridge's retry primitive). Returns the retry's result so the caller can
 * require MOQ_OK and attribute the emit to this direct call, not to a later
 * independent bridge drive. */
static moq_result_t redrive(moq_simpair_t *sp, moq_session_t *s,
                            moq_version_t ver, moq_stream_ref_t bidi_ref)
{
    if (ver == MOQ_VERSION_DRAFT_18)
        return moq_session_on_bidi_stream_bytes(s, bidi_ref, NULL, 0, false,
                                                moq_simpair_now_us(sp));
    return moq_session_process_pending(s, moq_simpair_now_us(sp));
}

/* the UPDATE_OK emission pre-checks event_queue_full and returns a
 * retryable WOULD_BLOCK BEFORE clearing update_pending, so a full client event
 * queue DEFERS -- never drops, never clears-before-emit -- the acknowledgment.
 * Deterministic invariant: with the client event queue full when the ack
 * arrives, NO UPDATE_OK is emitted and update_pending stays SET (preserved for
 * retry). A clear-before-emit regression would clear update_pending here with no
 * event -- a lost ack -- which this test catches. Resumption is then proven
 * deterministically below: once the queue drains, ONE DIRECT re-drive (redrive()
 * == MOQ_OK: process_pending for d16 / empty bidi input for d18) emits the ack
 * exactly once, with NO intervening bridge pump -- so the emit is attributable to
 * the retry, not a bridge false-pass. */
static void test_update_ok_queue_backpressure(moq_version_t ver)
{
    lc_alloc_state_t as = {0};
    moq_alloc_t alloc = lc_allocator(&as);
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = &alloc; cfg.seed = 1; cfg.initial_now_us = 1000;
    cfg.client_send_request_capacity = true; cfg.client_initial_request_capacity = 32;
    cfg.server_send_request_capacity = true; cfg.server_initial_request_capacity = 32;
    cfg.version = ver;
    cfg.max_events = 2;   /* tiny client+server event queues */
    moq_simpair_t *sp = NULL;
    MOQ_TEST_CHECK(moq_simpair_create(&cfg, &sp) == MOQ_OK);
    if (!sp) return;
    moq_simpair_start(sp);
    moq_simpair_run_until_quiescent(sp, 32, NULL);
    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);
    uint64_t now = moq_simpair_now_us(sp);
    moq_bytes_t p; moq_namespace_t ns = ns1(&p, "live");

    moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = ns; scfg.track_name = MOQ_BYTES_LITERAL("track");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    moq_subscription_t cl_sub;
    moq_session_subscribe(cl, &scfg, now, &cl_sub);
    moq_simpair_run_until_quiescent(sp, 32, NULL);
    moq_event_t req; MOQ_TEST_CHECK(poll_for(sv, MOQ_EVENT_SUBSCRIBE_REQUEST, &req));
    moq_subscription_t srv_sub = req.u.subscribe_request.sub; moq_event_cleanup(&req);
    moq_accept_subscribe_cfg_t acfg; moq_accept_subscribe_cfg_init(&acfg);
    MOQ_TEST_CHECK(moq_session_accept_subscribe(sv, srv_sub, &acfg, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 32, NULL);
    drain_events(sv);
    /* Deliberately DO NOT drain the client: its event queue (cap 2) is left full
     * (e.g. SETUP_COMPLETE + SUBSCRIBE_OK), so the update ack cannot emit yet. */

    MOQ_TEST_CHECK(moq_session_note_object_published(
        sv, &ns, MOQ_BYTES_LITERAL("track"), 4, 1) == MOQ_OK);
    moq_subscription_update_cfg_t ucfg; moq_subscription_update_cfg_init(&ucfg);
    ucfg.has_forward = true; ucfg.forward = true;
    MOQ_TEST_CHECK(moq_session_update_subscription(cl, cl_sub, &ucfg, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 32, NULL);

    /* The ack arrived while the client event queue was full: DEFERRED, not
     * delivered, update_pending PRESERVED (WOULD_BLOCK before the clear -- a
     * clear-before-emit bug fails here). Draining counts UPDATE_OK (none yet). */
    moq_stream_ref_t bref = sub_req_ref(cl, cl_sub);
    int n_ok = 0;
    {
        moq_event_t e;
        while (moq_session_poll_events(cl, &e, 1) == 1) {
            if (e.kind == MOQ_EVENT_SUBSCRIPTION_UPDATE_OK) n_ok++;
            moq_event_cleanup(&e);
        }
    }
    MOQ_TEST_CHECK_EQ_INT(n_ok, 0);                     /* not emitted while full */
    MOQ_TEST_CHECK(sub_update_pending(cl, cl_sub));     /* preserved, not cleared */

    /* Queue drained -> ONE DIRECT re-drive (process_pending d16 / empty bidi
     * input d18) must return MOQ_OK and, WITHOUT any bridge drive, deliver the
     * ack EXACTLY ONCE with the right scalars; pending clears. Attributing the
     * emit to the direct retry (no run_until_quiescent) rules out a bridge
     * false-pass. */
    MOQ_TEST_CHECK(redrive(sp, cl, ver, bref) == MOQ_OK);
    bool ev_has = false; uint64_t ev_g = 0, ev_o = 0;
    {
        moq_event_t e;
        while (moq_session_poll_events(cl, &e, 1) == 1) {
            if (e.kind == MOQ_EVENT_SUBSCRIPTION_UPDATE_OK) {
                n_ok++;
                ev_has = e.u.subscription_update_ok.has_largest;
                ev_g = e.u.subscription_update_ok.largest_group;
                ev_o = e.u.subscription_update_ok.largest_object;
            }
            moq_event_cleanup(&e);
        }
    }
    MOQ_TEST_CHECK_EQ_INT(n_ok, 1);                     /* exactly once on resume */
    MOQ_TEST_CHECK(ev_has);
    MOQ_TEST_CHECK_EQ_INT((int)ev_g, 4);
    MOQ_TEST_CHECK_EQ_INT((int)ev_o, 1);
    MOQ_TEST_CHECK(!sub_update_pending(cl, cl_sub));    /* cleared after emit */

    /* A further direct re-drive produces NO duplicate. */
    MOQ_TEST_CHECK(redrive(sp, cl, ver, bref) == MOQ_OK);
    {
        moq_event_t e;
        while (moq_session_poll_events(cl, &e, 1) == 1) {
            if (e.kind == MOQ_EVENT_SUBSCRIPTION_UPDATE_OK) n_ok++;
            moq_event_cleanup(&e);
        }
    }
    MOQ_TEST_CHECK_EQ_INT(n_ok, 1);                     /* no duplicate */

    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* Publication-ack counterpart of the backpressure invariant: the updater
 * (server, subscriber role) receives the REQUEST_OK while ITS event queue is
 * full (pre-filled with received objects). PUBLICATION_UPDATE_OK is deferred and
 * pub update_pending preserved; a re-drive then fires it EXACTLY ONCE with the
 * correct scalars, clears pending, and a further re-drive adds no duplicate. */
static void test_publication_update_ok_backpressure(moq_version_t ver)
{
    lc_alloc_state_t as = {0};
    moq_alloc_t alloc = lc_allocator(&as);
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = &alloc; cfg.seed = 1; cfg.initial_now_us = 1000;
    cfg.client_send_request_capacity = true; cfg.client_initial_request_capacity = 32;
    cfg.server_send_request_capacity = true; cfg.server_initial_request_capacity = 32;
    cfg.version = ver;
    cfg.max_events = 2;
    moq_simpair_t *sp = NULL;
    MOQ_TEST_CHECK(moq_simpair_create(&cfg, &sp) == MOQ_OK);
    if (!sp) return;
    moq_simpair_start(sp);
    moq_simpair_run_until_quiescent(sp, 32, NULL);
    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);
    uint64_t now = moq_simpair_now_us(sp);
    moq_bytes_t p; moq_namespace_t ns = ns1(&p, "live");

    /* Client publishes "pub"; server accepts with forwarding on. */
    moq_publish_cfg_t pc; moq_publish_cfg_init(&pc);
    pc.track_namespace = ns; pc.track_name = MOQ_BYTES_LITERAL("pub");
    pc.has_forward = true; pc.forward = true;
    moq_publication_t cl_pub;
    MOQ_TEST_CHECK(moq_session_publish(cl, &pc, now, &cl_pub) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 32, NULL);
    moq_event_t req; MOQ_TEST_CHECK(poll_for(sv, MOQ_EVENT_PUBLISH_REQUEST, &req));
    moq_publication_t srv_pub = req.u.publish_request.pub; moq_event_cleanup(&req);
    moq_accept_publish_cfg_t apc; moq_accept_publish_cfg_init(&apc);
    MOQ_TEST_CHECK(moq_session_accept_publish(sv, srv_pub, &apc, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 32, NULL);
    drain_events(cl); drain_events(sv);

    /* Publisher observes (6,2). */
    MOQ_TEST_CHECK(moq_session_note_object_published(
        cl, &ns, MOQ_BYTES_LITERAL("pub"), 6, 2) == MOQ_OK);

    /* Fill the SERVER event queue (cap 2) with received objects so the ack it is
     * about to get cannot emit immediately. Send them BELOW the noted largest
     * (group 5 < 6) so the fill does not advance the publisher's registry and
     * the asserted ack largest stays (6,2). */
    for (int i = 0; i < 2; i++) {
        uint8_t d[] = { (uint8_t)i };
        moq_rcbuf_t *pay = NULL; moq_rcbuf_create(&alloc, d, sizeof(d), &pay);
        MOQ_TEST_CHECK(moq_session_send_pub_object_datagram(
            cl, cl_pub, 5, (uint64_t)i, 128, false, pay, NULL, 0,
            moq_simpair_now_us(sp)) == MOQ_OK);
        moq_rcbuf_decref(pay);
    }
    /* Server sends the update; the publisher auto-acks; the server's ack is
     * deferred (its queue is full of the received objects). */
    moq_publication_update_cfg_t ucfg; moq_publication_update_cfg_init(&ucfg);
    ucfg.has_forward = true; ucfg.forward = true;
    MOQ_TEST_CHECK(moq_session_update_publication(sv, srv_pub, &ucfg,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 32, NULL);

    moq_stream_ref_t bref = pub_req_ref(sv, srv_pub);
    int n_ok = 0;
    {   /* Drain the server queue (received objects); count UPDATE_OK (none yet). */
        moq_event_t e;
        while (moq_session_poll_events(sv, &e, 1) == 1) {
            if (e.kind == MOQ_EVENT_PUBLICATION_UPDATE_OK) n_ok++;
            moq_event_cleanup(&e);
        }
    }
    MOQ_TEST_CHECK_EQ_INT(n_ok, 0);                     /* deferred while full */
    MOQ_TEST_CHECK(pub_update_pending(sv, srv_pub));    /* preserved, not cleared */

    /* ONE DIRECT re-drive (no bridge pump) must return MOQ_OK and deliver the
     * ack exactly once with the right scalars; pending clears. */
    MOQ_TEST_CHECK(redrive(sp, sv, ver, bref) == MOQ_OK);
    bool ev_has = false; uint64_t ev_g = 0, ev_o = 0;
    {
        moq_event_t e;
        while (moq_session_poll_events(sv, &e, 1) == 1) {
            if (e.kind == MOQ_EVENT_PUBLICATION_UPDATE_OK) {
                n_ok++;
                ev_has = e.u.publication_update_ok.has_largest;
                ev_g = e.u.publication_update_ok.largest_group;
                ev_o = e.u.publication_update_ok.largest_object;
            }
            moq_event_cleanup(&e);
        }
    }
    MOQ_TEST_CHECK_EQ_INT(n_ok, 1);
    MOQ_TEST_CHECK(ev_has);
    MOQ_TEST_CHECK_EQ_INT((int)ev_g, 6);
    MOQ_TEST_CHECK_EQ_INT((int)ev_o, 2);
    MOQ_TEST_CHECK(!pub_update_pending(sv, srv_pub));

    /* A further direct re-drive produces NO duplicate. */
    MOQ_TEST_CHECK(redrive(sp, sv, ver, bref) == MOQ_OK);
    {
        moq_event_t e;
        while (moq_session_poll_events(sv, &e, 1) == 1) {
            if (e.kind == MOQ_EVENT_PUBLICATION_UPDATE_OK) n_ok++;
            moq_event_cleanup(&e);
        }
    }
    MOQ_TEST_CHECK_EQ_INT(n_ok, 1);                     /* no duplicate */

    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

static void test_publication_update_ok_event(moq_version_t ver)
{
    lc_alloc_state_t as = {0};
    moq_alloc_t alloc = lc_allocator(&as);
    moq_simpair_t *sp = make_pair(&alloc, ver, 0);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);
    uint64_t now = moq_simpair_now_us(sp);
    moq_bytes_t p; moq_namespace_t ns = ns1(&p, "live");

    /* Client publishes "pub"; server (subscriber role) accepts. */
    moq_publish_cfg_t pc; moq_publish_cfg_init(&pc);
    pc.track_namespace = ns; pc.track_name = MOQ_BYTES_LITERAL("pub");
    moq_publication_t cl_pub;
    MOQ_TEST_CHECK(moq_session_publish(cl, &pc, now, &cl_pub) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    moq_event_t req; MOQ_TEST_CHECK(poll_for(sv, MOQ_EVENT_PUBLISH_REQUEST, &req));
    moq_publication_t srv_pub = req.u.publish_request.pub; moq_event_cleanup(&req);
    moq_accept_publish_cfg_t apc; moq_accept_publish_cfg_init(&apc);
    MOQ_TEST_CHECK(moq_session_accept_publish(sv, srv_pub, &apc, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    drain_events(cl); drain_events(sv);

    /* The publisher (client) observes an object on "pub" -> its registry (6,2). */
    MOQ_TEST_CHECK(moq_session_note_object_published(
        cl, &ns, MOQ_BYTES_LITERAL("pub"), 6, 2) == MOQ_OK);

    /* The subscriber (server) updates the publication; the publisher's ack
     * carries (6,2) and fires PUBLICATION_UPDATE_OK on the server exactly once. */
    moq_publication_update_cfg_t ucfg; moq_publication_update_cfg_init(&ucfg);
    ucfg.has_forward = true; ucfg.forward = true;
    MOQ_TEST_CHECK(moq_session_update_publication(sv, srv_pub, &ucfg, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 16, NULL);

    int n_ok = 0; bool ev_has = false; uint64_t ev_g = 0, ev_o = 0;
    moq_event_t e;
    while (moq_session_poll_events(sv, &e, 1) == 1) {
        if (e.kind == MOQ_EVENT_PUBLICATION_UPDATE_OK) {
            n_ok++;
            ev_has = e.u.publication_update_ok.has_largest;
            ev_g = e.u.publication_update_ok.largest_group;
            ev_o = e.u.publication_update_ok.largest_object;
        }
        moq_event_cleanup(&e);
    }
    MOQ_TEST_CHECK_EQ_INT(n_ok, 1);
    MOQ_TEST_CHECK(ev_has);
    MOQ_TEST_CHECK_EQ_INT((int)ev_g, 6);
    MOQ_TEST_CHECK_EQ_INT((int)ev_o, 2);

    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

static void test_entry_reserve_release(moq_version_t ver)
{
    /* A subscription that never observes an object holds an EMPTY reservation
     * that is reclaimed at teardown; a subscription whose registry record was
     * seeded (observed) leaves a PINNED record that persists. */
    lc_alloc_state_t as = {0};
    moq_alloc_t alloc = lc_allocator(&as);
    moq_simpair_t *sp = make_pair(&alloc, ver, 0);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);

    /* Establish + immediately unsubscribe an UNOBSERVED track. */
    uint64_t now = moq_simpair_now_us(sp);
    moq_bytes_t p; moq_namespace_t ns = ns1(&p, "live");
    moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = ns; scfg.track_name = MOQ_BYTES_LITERAL("ghost");
    moq_subscription_t cl_sub;
    moq_session_subscribe(cl, &scfg, now, &cl_sub);
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    moq_event_t req; MOQ_TEST_CHECK(poll_for(sv, MOQ_EVENT_SUBSCRIBE_REQUEST, &req));
    moq_subscription_t srv_sub = req.u.subscribe_request.sub; moq_event_cleanup(&req);
    moq_accept_subscribe_cfg_t acfg; moq_accept_subscribe_cfg_init(&acfg);
    MOQ_TEST_CHECK(moq_session_accept_subscribe(sv, srv_sub, &acfg, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    /* An empty reservation exists on the server for "ghost". */
    MOQ_TEST_CHECK(rec_of(sv, "live", "ghost") != NULL);
    MOQ_TEST_CHECK(!rec_of(sv, "live", "ghost")->has_largest);

    drain_events(cl); drain_events(sv);
    MOQ_TEST_CHECK(moq_session_unsubscribe(cl, cl_sub, moq_simpair_now_us(sp)) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    /* Empty reservation reclaimed on both sides after teardown. */
    MOQ_TEST_CHECK(rec_of(sv, "live", "ghost") == NULL);
    MOQ_TEST_CHECK(rec_of(cl, "live", "ghost") == NULL);

    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

static void test_write_receive_feed(moq_version_t ver)
{
    /* An object written on an accepted (publisher-role) subscription advances
     * BOTH the server's publisher-role record and, on receipt, the client's
     * subscriber-role record. */
    lc_alloc_state_t as = {0};
    moq_alloc_t alloc = lc_allocator(&as);
    moq_simpair_t *sp = make_pair(&alloc, ver, 0);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);

    uint64_t now = moq_simpair_now_us(sp);
    moq_bytes_t p; moq_namespace_t ns = ns1(&p, "live");
    moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = ns; scfg.track_name = MOQ_BYTES_LITERAL("track");
    moq_subscription_t cl_sub;
    moq_session_subscribe(cl, &scfg, now, &cl_sub);
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    moq_event_t req; MOQ_TEST_CHECK(poll_for(sv, MOQ_EVENT_SUBSCRIBE_REQUEST, &req));
    moq_subscription_t srv_sub = req.u.subscribe_request.sub; moq_event_cleanup(&req);
    moq_accept_subscribe_cfg_t acfg; moq_accept_subscribe_cfg_init(&acfg);
    MOQ_TEST_CHECK(moq_session_accept_subscribe(sv, srv_sub, &acfg, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    drain_events(cl);

    /* Datagram object at (5,4) written on the server publisher-role sub. */
    uint8_t d[] = { 0xAA };
    moq_rcbuf_t *pay = NULL; moq_rcbuf_create(&alloc, d, sizeof(d), &pay);
    MOQ_TEST_CHECK(moq_session_send_object_datagram(
        sv, srv_sub, 5, 4, 128, false, pay, NULL, 0,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_rcbuf_decref(pay);
    moq_simpair_run_until_quiescent(sp, 16, NULL);

    /* Server publisher-role record advanced by the raw write. */
    moq_track_hist_t *srec = rec_of(sv, "live", "track");
    MOQ_TEST_CHECK(srec && srec->has_largest &&
                   srec->largest_group == 5 && srec->largest_object == 4);
    /* Client subscriber-role record advanced by the receive path. */
    moq_track_hist_t *crec = rec_of(cl, "live", "track");
    MOQ_TEST_CHECK(crec && crec->has_largest &&
                   crec->largest_group == 5 && crec->largest_object == 4);

    drain_events(cl);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* Run one TRACK_STATUS request/response for "track"; server responds with the
 * given largest. The requester (client) merges the response's Largest Object
 * into its reserved registry record. */
static void ts_query(moq_simpair_t *sp, bool has_largest, uint64_t lg, uint64_t lo)
{
    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);
    uint64_t now = moq_simpair_now_us(sp);
    moq_bytes_t p; moq_namespace_t ns = ns1(&p, "live");

    moq_track_status_cfg_t tcfg; moq_track_status_cfg_init(&tcfg);
    tcfg.track_namespace = ns; tcfg.track_name = MOQ_BYTES_LITERAL("track");
    moq_track_status_handle_t th;
    MOQ_TEST_CHECK(moq_session_track_status(cl, &tcfg, now, &th) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 16, NULL);

    moq_event_t req;
    MOQ_TEST_CHECK(poll_for(sv, MOQ_EVENT_TRACK_STATUS_REQUEST, &req));
    moq_track_status_handle_t srv_h = req.u.track_status_request.handle;
    moq_event_cleanup(&req);

    moq_accept_track_status_cfg_t acfg; moq_accept_track_status_cfg_init(&acfg);
    acfg.has_largest = has_largest; acfg.largest_group = lg; acfg.largest_object = lo;
    MOQ_TEST_CHECK(moq_session_accept_track_status(sv, srv_h, &acfg, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    drain_events(cl); drain_events(sv);
}

static void test_track_status_ok_merge(moq_version_t ver)
{
    lc_alloc_state_t as = {0};
    moq_alloc_t alloc = lc_allocator(&as);
    moq_simpair_t *sp = make_pair(&alloc, ver, 0);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *cl = moq_simpair_client(sp);

    /* First response (5,5) is attributed to "track" on the requester side. */
    ts_query(sp, true, 5, 5);
    moq_track_hist_t *r = rec_of(cl, "live", "track");
    MOQ_TEST_CHECK(r && r->has_largest &&
                   r->largest_group == 5 && r->largest_object == 5);

    /* A subsequent LOWER response (2,0) must NOT regress the record (monotonic
     * max-merge). */
    ts_query(sp, true, 2, 0);
    r = rec_of(cl, "live", "track");
    MOQ_TEST_CHECK(r && r->largest_group == 5 && r->largest_object == 5);

    /* A subsequent HIGHER response (8,1) advances it. */
    ts_query(sp, true, 8, 1);
    r = rec_of(cl, "live", "track");
    MOQ_TEST_CHECK(r && r->largest_group == 8 && r->largest_object == 1);

    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* A publish whose explicit alias collides must NOT leak its
 * history reservation. With a small registry, repeated alias collisions would
 * exhaust the cap if each leaked a record; a later valid publish proves they
 * did not. */
static void test_publish_alias_rollback(moq_version_t ver)
{
    lc_alloc_state_t as = {0};
    moq_alloc_t alloc = lc_allocator(&as);
    moq_simpair_t *sp = make_pair(&alloc, ver, 2);   /* registry cap 2 */
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);
    moq_bytes_t p; moq_namespace_t ns = ns1(&p, "live");

    /* First publish claims alias 100 and one registry slot. */
    moq_publish_cfg_t pc1; moq_publish_cfg_init(&pc1);
    pc1.track_namespace = ns; pc1.track_name = MOQ_BYTES_LITERAL("p1");
    pc1.has_track_alias = true; pc1.track_alias = 100;
    moq_publication_t ph1;
    MOQ_TEST_CHECK(moq_session_publish(cl, &pc1, now, &ph1) == MOQ_OK);

    /* Five distinct-name publishes all colliding on alias 100 -> INVAL. If any
     * leaked its reservation the registry (cap 2) would exhaust. */
    for (int i = 0; i < 5; i++) {
        char nm[4] = { 'c', (char)('0' + i), 0, 0 };
        moq_publish_cfg_t pc; moq_publish_cfg_init(&pc);
        pc.track_namespace = ns;
        pc.track_name = (moq_bytes_t){ (const uint8_t *)nm, 2 };
        pc.has_track_alias = true; pc.track_alias = 100;   /* collision */
        moq_publication_t ph;
        MOQ_TEST_CHECK(moq_session_publish(cl, &pc, now, &ph) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(rec_of(cl, "live", nm) == NULL);    /* no record left */
    }

    /* A valid publish now still gets the SECOND slot -- proves the cap was not
     * silently consumed by the failed-alias attempts. */
    moq_publish_cfg_t pc2; moq_publish_cfg_init(&pc2);
    pc2.track_namespace = ns; pc2.track_name = MOQ_BYTES_LITERAL("p2");
    pc2.has_track_alias = true; pc2.track_alias = 101;
    moq_publication_t ph2;
    MOQ_TEST_CHECK(moq_session_publish(cl, &pc2, now, &ph2) == MOQ_OK);

    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* Streaming (chunked) receive delivery must advance history at
 * object admission -- the streaming path emits OBJECT_CHUNK, not a single
 * OBJECT_RECEIVED, and must still feed the registry. */
static void test_streaming_receive_history(moq_version_t ver)
{
    lc_alloc_state_t as = {0};
    moq_alloc_t alloc = lc_allocator(&as);
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = &alloc; cfg.seed = 1; cfg.initial_now_us = 1000;
    cfg.client_send_request_capacity = true; cfg.client_initial_request_capacity = 32;
    cfg.server_send_request_capacity = true; cfg.server_initial_request_capacity = 32;
    cfg.version = ver;
    cfg.client_streaming_objects = true;   /* client receives via the chunk path */
    moq_simpair_t *sp = NULL;
    MOQ_TEST_CHECK(moq_simpair_create(&cfg, &sp) == MOQ_OK);
    if (!sp) return;
    moq_simpair_start(sp);
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);
    uint64_t now = moq_simpair_now_us(sp);
    moq_bytes_t p; moq_namespace_t ns = ns1(&p, "live");

    moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = ns; scfg.track_name = MOQ_BYTES_LITERAL("track");
    moq_subscription_t cl_sub;
    moq_session_subscribe(cl, &scfg, now, &cl_sub);
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    moq_event_t req; MOQ_TEST_CHECK(poll_for(sv, MOQ_EVENT_SUBSCRIBE_REQUEST, &req));
    moq_subscription_t srv_sub = req.u.subscribe_request.sub; moq_event_cleanup(&req);
    moq_accept_subscribe_cfg_t acfg; moq_accept_subscribe_cfg_init(&acfg);
    MOQ_TEST_CHECK(moq_session_accept_subscribe(sv, srv_sub, &acfg, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    drain_events(cl);

    /* Server writes an object over a subgroup STREAM (not a datagram) at (6,2). */
    moq_subgroup_cfg_t sgc; moq_subgroup_cfg_init(&sgc);
    sgc.group_id = 6; sgc.subgroup_id = 0;
    moq_subgroup_handle_t sg;
    MOQ_TEST_CHECK(moq_session_open_subgroup(sv, srv_sub, &sgc,
        moq_simpair_now_us(sp), &sg) == MOQ_OK);
    uint8_t d[] = { 0xC0, 0xDE };
    moq_rcbuf_t *pay = NULL; moq_rcbuf_create(&alloc, d, sizeof(d), &pay);
    MOQ_TEST_CHECK(moq_session_write_object(sv, sg, 2, pay,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_rcbuf_decref(pay);
    moq_simpair_run_until_quiescent(sp, 16, NULL);

    /* Client received it via the streaming chunk path; its record advanced. */
    moq_track_hist_t *crec = rec_of(cl, "live", "track");
    MOQ_TEST_CHECK(crec && crec->has_largest &&
                   crec->largest_group == 6 && crec->largest_object == 2);

    drain_events(cl);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* inbound PUBLISH whose track needs a record but the registry is
 * full -> pre-commit REQUEST_ERROR (client PUBLISH_ERROR), no server record,
 * session stays open (PUBLISH reject already drains). */
static void test_publish_cap_exhaustion(moq_version_t ver)
{
    lc_alloc_state_t as = {0};
    moq_alloc_t alloc = lc_allocator(&as);
    moq_simpair_t *sp = make_pair(&alloc, ver, 1);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);
    uint64_t now = moq_simpair_now_us(sp);
    moq_bytes_t p; moq_namespace_t ns = ns1(&p, "live");

    /* Pin the server's single slot. */
    MOQ_TEST_CHECK(moq_session_note_object_published(
        sv, &ns, MOQ_BYTES_LITERAL("seed"), 1, 0) == MOQ_OK);

    moq_publish_cfg_t pc; moq_publish_cfg_init(&pc);
    pc.track_namespace = ns; pc.track_name = MOQ_BYTES_LITERAL("pub");
    moq_publication_t ph;
    MOQ_TEST_CHECK(moq_session_publish(cl, &pc, now, &ph) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 16, NULL);

    moq_event_t rq;
    MOQ_TEST_CHECK(!poll_for(sv, MOQ_EVENT_PUBLISH_REQUEST, &rq));   /* no event */
    MOQ_TEST_CHECK(rec_of(sv, "live", "pub") == NULL);              /* no record */
    int n_err = 0, n_closed = 0; moq_event_t ce;
    while (moq_session_poll_events(cl, &ce, 1) == 1) {
        if (ce.kind == MOQ_EVENT_PUBLISH_ERROR) n_err++;
        if (ce.kind == MOQ_EVENT_SESSION_CLOSED) n_closed++;
        moq_event_cleanup(&ce);
    }
    MOQ_TEST_CHECK(n_err == 1);
    MOQ_TEST_CHECK(n_closed == 0);
    MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* inbound TRACK_STATUS whose track needs a record but the registry
 * is full -> pre-commit REQUEST_ERROR (client TRACK_STATUS_ERROR), no server
 * record, session stays open. */
static void test_track_status_cap_exhaustion(moq_version_t ver)
{
    lc_alloc_state_t as = {0};
    moq_alloc_t alloc = lc_allocator(&as);
    moq_simpair_t *sp = make_pair(&alloc, ver, 1);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);
    uint64_t now = moq_simpair_now_us(sp);
    moq_bytes_t p; moq_namespace_t ns = ns1(&p, "live");

    MOQ_TEST_CHECK(moq_session_note_object_published(
        sv, &ns, MOQ_BYTES_LITERAL("seed"), 1, 0) == MOQ_OK);

    moq_track_status_cfg_t tcfg; moq_track_status_cfg_init(&tcfg);
    tcfg.track_namespace = ns; tcfg.track_name = MOQ_BYTES_LITERAL("ts");
    moq_track_status_handle_t th;
    MOQ_TEST_CHECK(moq_session_track_status(cl, &tcfg, now, &th) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 16, NULL);

    moq_event_t rq;
    MOQ_TEST_CHECK(!poll_for(sv, MOQ_EVENT_TRACK_STATUS_REQUEST, &rq)); /* no event */
    MOQ_TEST_CHECK(rec_of(sv, "live", "ts") == NULL);                  /* no record */
    int n_err = 0, n_closed = 0; moq_event_t ce;
    while (moq_session_poll_events(cl, &ce, 1) == 1) {
        if (ce.kind == MOQ_EVENT_TRACK_STATUS_ERROR) n_err++;
        if (ce.kind == MOQ_EVENT_SESSION_CLOSED) n_closed++;
        moq_event_cleanup(&ce);
    }
    MOQ_TEST_CHECK(n_err == 1);
    MOQ_TEST_CHECK(n_closed == 0);
    MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);
    /* SAME-CALL-FIN case: TRACK_STATUS is first-and-only, so the request arrived
     * WITH its FIN (request_fin true) -> the reject consumes no drain slot. */
    if (ver == MOQ_VERSION_DRAFT_18)
        MOQ_TEST_CHECK_EQ_SIZE(sv->drain_ref_count, (size_t)0);

    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* history advances at object ADMISSION, before application delivery.
 * With the client event queue full, a received subgroup object cannot be
 * DELIVERED (its OBJECT_RECEIVED is deferred), yet its Location has ALREADY
 * advanced the registry -- proving admission-time recording (a record-after-
 * delivery bug would show no history here). A re-drive then delivers it exactly
 * once. Also: an over-limit object is dropped and does NOT advance history. */
static void test_receive_history_admission_timing(moq_version_t ver)
{
    lc_alloc_state_t as = {0};
    moq_alloc_t alloc = lc_allocator(&as);
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = &alloc; cfg.seed = 1; cfg.initial_now_us = 1000;
    cfg.client_send_request_capacity = true; cfg.client_initial_request_capacity = 32;
    cfg.server_send_request_capacity = true; cfg.server_initial_request_capacity = 32;
    cfg.version = ver;
    cfg.max_events = 2;                 /* tiny client queue -> delivery backpressure */
    cfg.max_object_payload_size = 8;    /* small so an over-limit object is cheap */
    moq_simpair_t *sp = NULL;
    MOQ_TEST_CHECK(moq_simpair_create(&cfg, &sp) == MOQ_OK);
    if (!sp) return;
    moq_simpair_start(sp);
    moq_simpair_run_until_quiescent(sp, 32, NULL);
    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);
    uint64_t now = moq_simpair_now_us(sp);
    moq_bytes_t p; moq_namespace_t ns = ns1(&p, "live");

    moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = ns; scfg.track_name = MOQ_BYTES_LITERAL("track");
    moq_subscription_t cl_sub;
    moq_session_subscribe(cl, &scfg, now, &cl_sub);
    moq_simpair_run_until_quiescent(sp, 32, NULL);
    moq_event_t req; MOQ_TEST_CHECK(poll_for(sv, MOQ_EVENT_SUBSCRIBE_REQUEST, &req));
    moq_subscription_t srv_sub = req.u.subscribe_request.sub; moq_event_cleanup(&req);
    moq_accept_subscribe_cfg_t acfg; moq_accept_subscribe_cfg_init(&acfg);
    MOQ_TEST_CHECK(moq_session_accept_subscribe(sv, srv_sub, &acfg, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 32, NULL);
    /* Do NOT drain the client: its event queue (cap 2) is left full so a received
     * object cannot be delivered. */

    /* Server writes an object over a subgroup STREAM at (7,3). */
    moq_subgroup_cfg_t sgc; moq_subgroup_cfg_init(&sgc);
    sgc.group_id = 7; sgc.subgroup_id = 0;
    moq_subgroup_handle_t sg;
    MOQ_TEST_CHECK(moq_session_open_subgroup(sv, srv_sub, &sgc,
        moq_simpair_now_us(sp), &sg) == MOQ_OK);
    uint8_t d[] = { 0x01, 0x02 };
    moq_rcbuf_t *pay = NULL; moq_rcbuf_create(&alloc, d, sizeof(d), &pay);
    MOQ_TEST_CHECK(moq_session_write_object(sv, sg, 3, pay,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_rcbuf_decref(pay);
    moq_simpair_run_until_quiescent(sp, 32, NULL);

    /* ADMISSION discriminator: history advanced to (7,3) even though the object
     * event has NOT been delivered yet (the full queue holds only the
     * establishment events -- no OBJECT_RECEIVED among them). */
    moq_track_hist_t *crec = rec_of(cl, "live", "track");
    MOQ_TEST_CHECK(crec && crec->has_largest &&
                   crec->largest_group == 7 && crec->largest_object == 3);
    int n_obj_before = 0;
    {   /* Drain the (full) establishment queue; count any OBJECT_RECEIVED. */
        moq_event_t e;
        while (moq_session_poll_events(cl, &e, 1) == 1) {
            if (e.kind == MOQ_EVENT_OBJECT_RECEIVED) n_obj_before++;
            moq_event_cleanup(&e);
        }
    }
    MOQ_TEST_CHECK_EQ_INT(n_obj_before, 0);   /* not delivered while queue full */

    /* Queue drained -> re-drive the deferred receive stream (empty data input on
     * its ref, the bridge's retry primitive): the object now delivers EXACTLY
     * ONCE. */
    moq_stream_ref_t rxref = rx_ref_for_sub(cl, cl_sub);
    int n_obj = 0;
    for (int round = 0; round < 6 && n_obj == 0; round++) {
        moq_session_on_data_bytes(cl, rxref, NULL, 0, false,
                                  moq_simpair_now_us(sp));
        moq_simpair_run_until_quiescent(sp, 32, NULL);
        moq_event_t e;
        while (moq_session_poll_events(cl, &e, 1) == 1) {
            if (e.kind == MOQ_EVENT_OBJECT_RECEIVED) n_obj++;
            moq_event_cleanup(&e);
        }
    }
    MOQ_TEST_CHECK_EQ_INT(n_obj, 1);          /* delivered exactly once on resume */

    /* Over-limit object: a datagram whose payload exceeds max_object_payload_size
     * is dropped at admission and must NOT advance history past (7,3). */
    uint8_t big[16] = {0};   /* 16 > max_object_payload_size (8) */
    moq_rcbuf_t *bp = NULL; moq_rcbuf_create(&alloc, big, sizeof(big), &bp);
    MOQ_TEST_CHECK(moq_session_send_object_datagram(
        sv, srv_sub, 9, 9, 128, false, bp, NULL, 0,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_rcbuf_decref(bp);
    moq_simpair_run_until_quiescent(sp, 32, NULL);
    drain_events(cl);
    crec = rec_of(cl, "live", "track");
    MOQ_TEST_CHECK(crec && crec->largest_group == 7 && crec->largest_object == 3);

    drain_events(cl);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* the PUBLISH-established receive branch (objects arriving on a
 * publication WE accepted) also advances history on the subscriber-role pub
 * entry. */
static void test_publish_established_receive_history(moq_version_t ver)
{
    lc_alloc_state_t as = {0};
    moq_alloc_t alloc = lc_allocator(&as);
    moq_simpair_t *sp = make_pair(&alloc, ver, 0);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);
    uint64_t now = moq_simpair_now_us(sp);
    moq_bytes_t p; moq_namespace_t ns = ns1(&p, "live");

    /* Server publishes "pub" to the client; the client accepts (subscriber role)
     * with forwarding on. */
    moq_publish_cfg_t pc; moq_publish_cfg_init(&pc);
    pc.track_namespace = ns; pc.track_name = MOQ_BYTES_LITERAL("pub");
    pc.has_forward = true; pc.forward = true;
    moq_publication_t sv_pub;
    MOQ_TEST_CHECK(moq_session_publish(sv, &pc, now, &sv_pub) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    moq_event_t req; MOQ_TEST_CHECK(poll_for(cl, MOQ_EVENT_PUBLISH_REQUEST, &req));
    moq_publication_t cl_pub = req.u.publish_request.pub; moq_event_cleanup(&req);
    moq_accept_publish_cfg_t apc; moq_accept_publish_cfg_init(&apc);
    MOQ_TEST_CHECK(moq_session_accept_publish(cl, cl_pub, &apc, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    drain_events(cl); drain_events(sv);

    /* Server sends an object on the publication at (4,5); the client receives it
     * via the PUBLISH-established branch and advances its record. */
    uint8_t d[] = { 0xEE };
    moq_rcbuf_t *pay = NULL; moq_rcbuf_create(&alloc, d, sizeof(d), &pay);
    MOQ_TEST_CHECK(moq_session_send_pub_object_datagram(
        sv, sv_pub, 4, 5, 128, false, pay, NULL, 0,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_rcbuf_decref(pay);
    moq_simpair_run_until_quiescent(sp, 16, NULL);

    moq_track_hist_t *crec = rec_of(cl, "live", "pub");
    MOQ_TEST_CHECK(crec && crec->has_largest &&
                   crec->largest_group == 4 && crec->largest_object == 5);

    drain_events(cl);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* Subscription-POOL exhaustion (distinct from the history cap) is
 * a graceful stream-correlated rejection, not a session close. The server sub
 * pool is 1; an accepted subscription fills it, so a second inbound SUBSCRIBE
 * cannot stage a slot -> REQUEST_ERROR (client SUBSCRIBE_ERROR), NO SESSION_CLOSED,
 * session stays ESTABLISHED on BOTH drafts. On draft-18 this exercises the
 * pre-parser staging path (was a PROTOCOL_VIOLATION close before the fix). */
static void test_subscribe_pool_exhaustion_graceful(moq_version_t ver)
{
    lc_alloc_state_t as = {0};
    moq_alloc_t alloc = lc_allocator(&as);
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = &alloc; cfg.seed = 1; cfg.initial_now_us = 1000;
    cfg.client_send_request_capacity = true; cfg.client_initial_request_capacity = 32;
    cfg.server_send_request_capacity = true; cfg.server_initial_request_capacity = 32;
    cfg.version = ver;
    cfg.server_max_subscriptions = 1;   /* one server subscription slot */
    cfg.client_max_subscriptions = 8;
    moq_simpair_t *sp = NULL;
    MOQ_TEST_CHECK(moq_simpair_create(&cfg, &sp) == MOQ_OK);
    if (!sp) return;
    moq_simpair_start(sp);
    moq_simpair_run_until_quiescent(sp, 32, NULL);
    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);
    uint64_t now = moq_simpair_now_us(sp);
    moq_bytes_t p; moq_namespace_t ns = ns1(&p, "live");

    /* Accept "a" -> the single server sub slot is now occupied. */
    moq_subscribe_cfg_t s1; moq_subscribe_cfg_init(&s1);
    s1.track_namespace = ns; s1.track_name = MOQ_BYTES_LITERAL("a");
    moq_subscription_t h1; MOQ_TEST_CHECK(moq_session_subscribe(cl, &s1, now, &h1) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 32, NULL);
    moq_event_t req; MOQ_TEST_CHECK(poll_for(sv, MOQ_EVENT_SUBSCRIBE_REQUEST, &req));
    moq_subscription_t srv1 = req.u.subscribe_request.sub; moq_event_cleanup(&req);
    moq_accept_subscribe_cfg_t ac; moq_accept_subscribe_cfg_init(&ac);
    MOQ_TEST_CHECK(moq_session_accept_subscribe(sv, srv1, &ac, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 32, NULL);
    drain_events(cl); drain_events(sv);

    /* Subscribe "b": the server sub pool is full -> graceful reject. */
    moq_subscribe_cfg_t s2; moq_subscribe_cfg_init(&s2);
    s2.track_namespace = ns; s2.track_name = MOQ_BYTES_LITERAL("b");
    moq_subscription_t h2; MOQ_TEST_CHECK(moq_session_subscribe(cl, &s2, now, &h2) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 32, NULL);

    moq_event_t rq2;
    MOQ_TEST_CHECK(!poll_for(sv, MOQ_EVENT_SUBSCRIBE_REQUEST, &rq2)); /* no event */
    int n_err = 0, n_closed = 0; moq_event_t ce;
    while (moq_session_poll_events(cl, &ce, 1) == 1) {
        if (ce.kind == MOQ_EVENT_SUBSCRIBE_ERROR) n_err++;
        if (ce.kind == MOQ_EVENT_SESSION_CLOSED) n_closed++;
        moq_event_cleanup(&ce);
    }
    MOQ_TEST_CHECK(n_err == 1);
    MOQ_TEST_CHECK(n_closed == 0);
    MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);   /* not closed */

    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

static void test_inbound_cap_exhaustion(moq_version_t ver)
{
    /* The server registry (cap 1) is pre-filled by a prior observation, so an
     * inbound SUBSCRIBE for a NEW track cannot reserve a record. The server must
     * REJECT it via the pre-commit pattern: NO SUBSCRIBE_REQUEST event, NO server
     * record/entry for the track, a REQUEST_ERROR the client surfaces as
     * SUBSCRIBE_ERROR (a graceful reject, never a delivered session-fatal close).
     * The client does a single subscribe, so the client's own (shared cap-1)
     * registry is not the limiter -- this isolates the SERVER reject path.
     *
     * On BOTH drafts the SESSION STAYS OPEN and handles a subsequent request: the
     * draft-18 reject rides the request bidi and installs a drain reference, so
     * the rejected subscriber's trailing FIN is absorbed (drain ring) rather than
     * closing the session (design §1; the PUBLISH/TRACK_STATUS reject pattern). */
    lc_alloc_state_t as = {0};
    moq_alloc_t alloc = lc_allocator(&as);
    moq_simpair_t *sp = make_pair(&alloc, ver, 1);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);
    MOQ_TEST_CHECK_EQ_SIZE(sv->th_cap, (size_t)1);

    uint64_t now = moq_simpair_now_us(sp);
    moq_bytes_t p; moq_namespace_t ns = ns1(&p, "live");

    /* Pin the server's single registry slot with an observed record. */
    MOQ_TEST_CHECK(moq_session_note_object_published(
        sv, &ns, MOQ_BYTES_LITERAL("seed"), 1, 0) == MOQ_OK);
    MOQ_TEST_CHECK(rec_of(sv, "live", "seed") != NULL);

    /* Client's single subscribe for a new track "b". */
    moq_subscribe_cfg_t s2; moq_subscribe_cfg_init(&s2);
    s2.track_namespace = ns; s2.track_name = MOQ_BYTES_LITERAL("b");
    moq_subscription_t h2;
    MOQ_TEST_CHECK(moq_session_subscribe(cl, &s2, now, &h2) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 16, NULL);

    moq_event_t rq2;
    MOQ_TEST_CHECK(!poll_for(sv, MOQ_EVENT_SUBSCRIBE_REQUEST, &rq2)); /* no event */
    MOQ_TEST_CHECK(rec_of(sv, "live", "b") == NULL);                 /* no record */
    /* The client sees a graceful REQUEST_ERROR (SUBSCRIBE_ERROR), NOT a delivered
     * session-fatal close. */
    int n_err = 0, n_closed = 0;
    moq_event_t ce;
    while (moq_session_poll_events(cl, &ce, 1) == 1) {
        if (ce.kind == MOQ_EVENT_SUBSCRIBE_ERROR) n_err++;
        if (ce.kind == MOQ_EVENT_SESSION_CLOSED) n_closed++;
        moq_event_cleanup(&ce);
    }
    MOQ_TEST_CHECK(n_err == 1);
    MOQ_TEST_CHECK(n_closed == 0);

    /* The session stays OPEN on both drafts and still handles requests: a
     * subsequent observation merges (the trailing FIN was absorbed, not fatal). */
    MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK(moq_session_note_object_published(
        sv, &ns, MOQ_BYTES_LITERAL("seed"), 2, 0) == MOQ_OK);
    /* And a fresh inbound subscribe is still processed end to end (rejected
     * again, registry still full -- but a graceful REQUEST_ERROR, proving the
     * request path is alive after the first reject). */
    moq_subscribe_cfg_t s3; moq_subscribe_cfg_init(&s3);
    s3.track_namespace = ns; s3.track_name = MOQ_BYTES_LITERAL("c");
    moq_subscription_t h3;
    MOQ_TEST_CHECK(moq_session_subscribe(cl, &s3, moq_simpair_now_us(sp), &h3) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);
    int n_err2 = 0;
    while (moq_session_poll_events(cl, &ce, 1) == 1) {
        if (ce.kind == MOQ_EVENT_SUBSCRIBE_ERROR) n_err2++;
        moq_event_cleanup(&ce);
    }
    MOQ_TEST_CHECK(n_err2 == 1);

    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* -- Track-history tri-state reservation --------------------------- */

/* Fail the Nth allocation (1-indexed), pass everything else. */
typedef struct { int64_t balance; int count; int fail_at; } th_alloc_state_t;

static void *th_alloc(size_t n, void *ctx)
{
    th_alloc_state_t *s = (th_alloc_state_t *)ctx;
    s->count++;
    if (s->fail_at && s->count == s->fail_at) return NULL;
    void *p = malloc(n);
    if (p) s->balance++;
    return p;
}
static void *th_realloc(void *p, size_t o, size_t n, void *ctx)
{
    th_alloc_state_t *s = (th_alloc_state_t *)ctx;
    (void)o;
    if (n == 0) { if (p) { s->balance--; free(p); } return NULL; }
    s->count++;
    if (s->fail_at && s->count == s->fail_at) return NULL;
    void *q = realloc(p, n);
    if (q && !p) s->balance++;
    return q;
}
static void th_free(void *p, size_t n, void *ctx)
{
    th_alloc_state_t *s = (th_alloc_state_t *)ctx;
    (void)n;
    if (p) { s->balance--; free(p); }
}
static moq_alloc_t th_allocator(th_alloc_state_t *s)
{ moq_alloc_t a = { s, th_alloc, th_realloc, th_free }; return a; }

/* Count SEND_BIDI_STREAM actions on `ref` carrying a d18 REQUEST_ERROR,
 * plus the TOTAL number of actions drained -- so "exactly one
 * REQUEST_ERROR and nothing else" is actually measured. */
static void th_count_request_errors(moq_session_t *s, moq_stream_ref_t ref,
                                    int *out_errs, int *out_total)
{
    int errs = 0, total = 0;
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) == 1) {
        total++;
        if (a.kind == MOQ_ACTION_SEND_BIDI_STREAM &&
            a.u.send_bidi_stream.stream_ref._v == ref._v) {
            moq_buf_reader_t rr;
            moq_buf_reader_init(&rr, a.u.send_bidi_stream.data,
                                a.u.send_bidi_stream.len);
            moq_control_envelope_t env;
            if (moq_d18_decode_envelope(&rr, &env) == MOQ_OK &&
                env.msg_type == MOQ_D18_REQUEST_ERROR)
                errs++;
        }
        moq_action_cleanup(&a);
    }
    if (out_errs) *out_errs = errs;
    if (out_total) *out_total = total;
}

/* After a hard failure on a draft-18 request-stream family, the request
 * owner must be fully retired: the stream ref resolves to nothing and no
 * subscription / publication / track-status slot is left occupied.
 * Retirement is enforced by the request-stream handler, which frees a
 * still-receiving staging slot on ANY hard failure
 * (session_subscribe.c's `if (rc < 0) { if (receiving) sub_free_entry }`),
 * so this pins the invariant across the whole inbound path rather than a
 * single core handler's cleanup label. Returns the failed-check count. */
static int th_owner_retired(moq_session_t *s, moq_stream_ref_t ref)
{
    int failures = 0;
    moq_request_endpoint_t ep = request_registry_find_by_streamref(s, ref);
    MOQ_TEST_CHECK_EQ_INT((int)ep.kind, (int)MOQ_REQ_NONE);
    for (size_t i = 0; i < s->sub_cap; i++)
        MOQ_TEST_CHECK(s->subs[i].state == MOQ_SUB_FREE);
    for (size_t i = 0; i < s->pub_cap; i++)
        MOQ_TEST_CHECK(s->publishes[i].state == MOQ_PUB_FREE);
    for (size_t i = 0; i < s->ts_cap; i++)
        MOQ_TEST_CHECK(s->track_statuses[i].state == MOQ_TS_FREE);
    return failures;
}

/* A FRAGMENTED draft-18 SUBSCRIBE: the head arrives without FIN (creating
 * a real staging owner registered by stream ref), then the tail completes
 * it. Returns the tail feed's result. */
static moq_result_t feed_d18_subscribe_split(moq_session_t *s,
                                             moq_stream_ref_t ref,
                                             const char *track,
                                             th_alloc_state_t *as,
                                             int fail_ordinal_after_head)
{
    uint8_t buf[128]; moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_namespace_t ns = { parts, 1 };
    moq_bytes_t tn = { (const uint8_t *)track, strlen(track) };
    moq_d18_msg_params_t mp = {0};
    if (moq_d18_encode_subscribe(&w, 0, &ns, tn, &mp) < 0)
        return MOQ_ERR_INTERNAL;
    size_t total = moq_buf_writer_offset(&w);
    size_t head = total / 2;
    moq_result_t rc = moq_session_on_bidi_stream_bytes(s, ref, buf, head,
                                                       false, 0);
    if (rc < 0) return rc;

    /* The head must have created a REAL staging owner, otherwise the
     * post-failure retirement check would pass without any owner
     * transition having occurred: the stream ref resolves to a
     * SUBSCRIPTION endpoint, its slot is valid and RECVING_REQUEST, and
     * the head bytes are retained on that entry. */
    {
        moq_request_endpoint_t ep =
            request_registry_find_by_streamref(s, ref);
        MOQ_TEST_CHECK_EQ_INT((int)ep.kind, (int)MOQ_REQ_SUBSCRIPTION);
        MOQ_TEST_CHECK(ep.slot >= 0 && (size_t)ep.slot < s->sub_cap);
        if (ep.kind == MOQ_REQ_SUBSCRIPTION && ep.slot >= 0 &&
            (size_t)ep.slot < s->sub_cap) {
            moq_sub_entry_t *e = &s->subs[ep.slot];
            MOQ_TEST_CHECK_EQ_INT((int)e->state,
                                  (int)MOQ_SUB_RECVING_REQUEST);
            MOQ_TEST_CHECK_EQ_SIZE(e->req_recv_len, head);
            MOQ_TEST_CHECK(e->req_recv_buf != NULL &&
                           memcmp(e->req_recv_buf, buf, head) == 0);
        }
    }

    if (as && fail_ordinal_after_head > 0)
        as->fail_at = as->count + fail_ordinal_after_head;
    rc = moq_session_on_bidi_stream_bytes(s, ref, buf + head, total - head,
                                          true, 0);
    if (as) as->fail_at = 0;
    return rc;
}

/* Count occupied registry slots. */
static size_t th_used(moq_session_t *s)
{
    size_t n = 0;
    if (!s->track_hist) return 0;
    for (size_t i = 0; i < s->th_cap; i++)
        if (s->track_hist[i].in_use) n++;
    return n;
}

/* Total refs held across the registry. */
static uint32_t th_refs(moq_session_t *s)
{
    uint32_t r = 0;
    if (!s->track_hist) return 0;
    for (size_t i = 0; i < s->th_cap; i++)
        if (s->track_hist[i].in_use) r += s->track_hist[i].refs;
    return r;
}

/* Count queued REQUEST_ERROR responses (bidi or control) and any events. */
static void th_drain_counts(moq_session_t *s, int *out_actions, int *out_events)
{
    int na = 0, ne = 0;
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) == 1) { na++; moq_action_cleanup(&a); }
    moq_event_t e;
    while (moq_session_poll_events(s, &e, 1) == 1) { ne++; moq_event_cleanup(&e); }
    if (out_actions) *out_actions = na;
    if (out_events) *out_events = ne;
}

/* The pure comparator agrees with the built-key path, validates bounds, and
 * never over-reads a truncated or malformed stored key. */
static void test_track_key_pure_comparator(void)
{
    th_alloc_state_t as = {0};
    moq_alloc_t alloc = th_allocator(&as);
    moq_session_t *s = d18_server_ex(&alloc, 4, 0, true);
    MOQ_TEST_CHECK(s != NULL);
    if (!s) return;

    /* Multi-part namespace with an EMPTY component and an empty name. */
    moq_bytes_t parts[3] = {
        MOQ_BYTES_LITERAL("live"),
        { NULL, 0 },
        MOQ_BYTES_LITERAL("cam2"),
    };
    moq_namespace_t ns = { parts, 3 };
    moq_bytes_t name = MOQ_BYTES_LITERAL("hi");

    size_t klen = 0;
    uint8_t *key = moq_build_track_key(s, &ns, name, &klen);
    MOQ_TEST_CHECK(key != NULL);
    if (!key) { moq_session_destroy(s); return; }

    /* Agreement with the built-key representation. */
    MOQ_TEST_CHECK(moq_track_key_matches(key, klen, &ns, name));

    /* Every single-field perturbation fails to match. */
    moq_bytes_t other_name = MOQ_BYTES_LITERAL("h");
    MOQ_TEST_CHECK(!moq_track_key_matches(key, klen, &ns, other_name));
    moq_namespace_t short_ns = { parts, 2 };
    MOQ_TEST_CHECK(!moq_track_key_matches(key, klen, &short_ns, name));
    moq_bytes_t alt_parts[3] = { MOQ_BYTES_LITERAL("live"),
                                 { NULL, 0 },
                                 MOQ_BYTES_LITERAL("cam3") };
    moq_namespace_t alt_ns = { alt_parts, 3 };
    MOQ_TEST_CHECK(!moq_track_key_matches(key, klen, &alt_ns, name));

    /* TRUNCATION at every length: never matches, never over-reads (ASan
     * would trap a read past the shortened copy). */
    for (size_t cut = 0; cut < klen; cut++) {
        uint8_t *frag = (uint8_t *)malloc(cut ? cut : 1);
        MOQ_TEST_CHECK(frag != NULL);
        if (!frag) break;
        if (cut) memcpy(frag, key, cut);
        MOQ_TEST_CHECK(!moq_track_key_matches(frag, cut, &ns, name));
        free(frag);
    }
    /* A trailing byte past the exact encoding never matches either. */
    {
        uint8_t *longer = (uint8_t *)malloc(klen + 1);
        MOQ_TEST_CHECK(longer != NULL);
        if (longer) {
            memcpy(longer, key, klen);
            longer[klen] = 0x00;
            MOQ_TEST_CHECK(!moq_track_key_matches(longer, klen + 1, &ns, name));
            free(longer);
        }
    }
    /* A malformed stored key whose declared part length exceeds the blob. */
    {
        uint8_t bad[8] = { 1, 0xFF, 0xFF, 'a', 'b', 0, 0, 0 };
        moq_bytes_t bp = MOQ_BYTES_LITERAL("ab");
        moq_namespace_t bns = { &bp, 1 };
        MOQ_TEST_CHECK(!moq_track_key_matches(bad, sizeof(bad), &bns,
                                              MOQ_BYTES_LITERAL("")));
    }
    /* The empty identity matches only its CANONICAL encoding -- three
     * bytes: [count 0][u16-be name length 0]. A NULL/zero-length blob is
     * not that encoding and must never match. */
    {
        moq_namespace_t empty_ns = { NULL, 0 };
        moq_bytes_t empty_name = { NULL, 0 };
        size_t eklen = 0;
        uint8_t *ekey = moq_build_track_key(s, &empty_ns, empty_name, &eklen);
        MOQ_TEST_CHECK(ekey != NULL);
        MOQ_TEST_CHECK_EQ_SIZE(eklen, (size_t)3);
        if (ekey) {
            MOQ_TEST_CHECK(moq_track_key_matches(ekey, eklen, &empty_ns,
                                                 empty_name));
            s->alloc.free(ekey, eklen, s->alloc.ctx);
        }
        MOQ_TEST_CHECK(!moq_track_key_matches(NULL, 0, &empty_ns, empty_name));
        MOQ_TEST_CHECK(!moq_track_key_matches(key, klen, &empty_ns,
                                              empty_name));
    }

    /* Selector agreement with moq_build_track_key + track_hist_find for a
     * live record. */
    MOQ_TEST_CHECK(moq_track_hist_select(s, &ns, name) ==
                   MOQ_TH_SEL_FREE_SLOT);
    MOQ_TEST_CHECK(track_hist_find(s, key, klen) == NULL);
    moq_track_hist_t *rec = NULL;
    MOQ_TEST_CHECK(track_hist_reserve_selected(s, &ns, name, &rec) == MOQ_OK);
    MOQ_TEST_CHECK(rec != NULL);
    MOQ_TEST_CHECK(track_hist_find(s, key, klen) == rec);
    MOQ_TEST_CHECK(track_hist_find_id(s, &ns, name) == rec);
    MOQ_TEST_CHECK(moq_track_hist_select(s, &ns, name) ==
                   MOQ_TH_SEL_EXISTING);
    /* An existing record's reservation takes a ref rather than a slot. */
    size_t used_before = th_used(s);
    moq_track_hist_t *again = NULL;
    MOQ_TEST_CHECK(track_hist_reserve_selected(s, &ns, name, &again) == MOQ_OK);
    MOQ_TEST_CHECK(again == rec);
    MOQ_TEST_CHECK_EQ_SIZE(th_used(s), used_before);
    MOQ_TEST_CHECK_EQ_INT((int)rec->refs, 2);
    track_hist_release(s, rec);
    track_hist_release(s, rec);

    s->alloc.free(key, klen, s->alloc.ctx);
    moq_session_destroy(s);
    MOQ_TEST_CHECK(as.balance == 0);
}

/*
 * The registry-full rejection and an allocation failure are DISTINCT on all
 * three inbound paths: a full registry emits exactly one REQUEST_ERROR and
 * keeps the session open; a failed record key copy returns exact
 * MOQ_ERR_NOMEM with no response queued and nothing mutated.
 */
static void test_track_hist_tristate_subscribe(void)
{
    /* Full registry: one REQUEST_ERROR, session open, no history change. */
    {
        th_alloc_state_t as = {0};
        moq_alloc_t alloc = th_allocator(&as);
        moq_session_t *s = d18_server_established(&alloc, 1);
        MOQ_TEST_CHECK(s != NULL);
        if (!s) return;
        moq_bytes_t p; moq_namespace_t ns = ns1(&p, "live");
        MOQ_TEST_CHECK(moq_session_note_object_published(
            s, &ns, MOQ_BYTES_LITERAL("seed"), 1, 0) == MOQ_OK);
        size_t used = th_used(s);
        uint32_t refs = th_refs(s);
        th_drain_counts(s, NULL, NULL);

        MOQ_TEST_CHECK(feed_d18_subscribe(
            s, moq_stream_ref_from_u64(4), "x", /*fin*/true) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(s) == MOQ_SESS_ESTABLISHED);
        int errs = 0, total = 0;
        th_count_request_errors(s, moq_stream_ref_from_u64(4), &errs, &total);
        MOQ_TEST_CHECK_EQ_INT(errs, 1);      /* exactly one REQUEST_ERROR */
        MOQ_TEST_CHECK_EQ_INT(total, 1);     /* and no other action */
        { int ne = 0; moq_event_t e;
          while (moq_session_poll_events(s, &e, 1) == 1) { ne++;
              moq_event_cleanup(&e); }
          MOQ_TEST_CHECK_EQ_INT(ne, 0); }
        MOQ_TEST_CHECK_EQ_SIZE(th_used(s), used);
        MOQ_TEST_CHECK_EQ_INT((int)th_refs(s), (int)refs);
        moq_session_destroy(s);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* Funded ordinal 1 -- the entry's RETAINED identity key -- fails:
     * exact NOMEM, no response, nothing mutated, owner retired. */
    {
        th_alloc_state_t as = {0};
        moq_alloc_t alloc = th_allocator(&as);
        moq_session_t *s = d18_server_established(&alloc, 4);
        MOQ_TEST_CHECK(s != NULL);
        if (!s) return;
        th_drain_counts(s, NULL, NULL);
        size_t used = th_used(s);
        uint32_t refs = th_refs(s);
        size_t scratch = s->event_scratch_len;
        int64_t bal = as.balance;

        as.fail_at = as.count + 1;
        MOQ_TEST_CHECK_EQ_INT(
            (int)feed_d18_subscribe(s, moq_stream_ref_from_u64(4), "x",
                                    /*fin*/true),
            (int)MOQ_ERR_NOMEM);
        as.fail_at = 0;
        MOQ_TEST_CHECK(moq_session_state(s) == MOQ_SESS_ESTABLISHED);
        int na1 = 0, ne1 = 0;
        th_drain_counts(s, &na1, &ne1);
        MOQ_TEST_CHECK_EQ_INT(na1, 0);
        MOQ_TEST_CHECK_EQ_INT(ne1, 0);
        MOQ_TEST_CHECK_EQ_SIZE(th_used(s), used);   /* no record created */
        MOQ_TEST_CHECK_EQ_INT((int)th_refs(s), (int)refs);
        MOQ_TEST_CHECK_EQ_SIZE(s->event_scratch_len, scratch);
        MOQ_TEST_CHECK(as.balance == bal);
        MOQ_TEST_CHECK(th_owner_retired(s, moq_stream_ref_from_u64(4)) == 0);
        moq_session_destroy(s);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* FRAGMENTED feed: the head creates a real staging owner registered by
     * stream ref; the tail's funded allocation then fails. The owner must
     * be fully retired -- no registry key, no staging slot left behind. */
    {
        th_alloc_state_t as = {0};
        moq_alloc_t alloc = th_allocator(&as);
        moq_session_t *s = d18_server_established(&alloc, 4);
        MOQ_TEST_CHECK(s != NULL);
        if (!s) return;
        th_drain_counts(s, NULL, NULL);
        size_t used = th_used(s);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(4);

        MOQ_TEST_CHECK_EQ_INT(
            (int)feed_d18_subscribe_split(s, ref, "x", &as, 1),
            (int)MOQ_ERR_NOMEM);
        MOQ_TEST_CHECK(moq_session_state(s) == MOQ_SESS_ESTABLISHED);
        int nas = 0, nes = 0;
        th_drain_counts(s, &nas, &nes);
        MOQ_TEST_CHECK_EQ_INT(nas, 0);
        MOQ_TEST_CHECK_EQ_INT(nes, 0);
        MOQ_TEST_CHECK_EQ_SIZE(th_used(s), used);
        MOQ_TEST_CHECK(th_owner_retired(s, ref) == 0);
        moq_session_destroy(s);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* Record key failure (ordinal 2): exact NOMEM, no response, nothing
     * mutated. */
    {
        th_alloc_state_t as = {0};
        moq_alloc_t alloc = th_allocator(&as);
        moq_session_t *s = d18_server_established(&alloc, 4);
        MOQ_TEST_CHECK(s != NULL);
        if (!s) return;
        th_drain_counts(s, NULL, NULL);
        size_t used = th_used(s);
        uint32_t refs = th_refs(s);
        size_t scratch = s->event_scratch_len;
        int64_t bal = as.balance;

        /* Funded execution allocates exactly twice: ordinal 1 is the
         * entry's RETAINED identity key, ordinal 2 is the REGISTRY
         * RECORD's key -- the allocation that used to be reported as
         * "track history full". Arm ordinal 2. */
        as.fail_at = as.count + 2;
        MOQ_TEST_CHECK_EQ_INT(
            (int)feed_d18_subscribe(s, moq_stream_ref_from_u64(4), "x",
                                    /*fin*/true),
            (int)MOQ_ERR_NOMEM);
        as.fail_at = 0;
        MOQ_TEST_CHECK(moq_session_state(s) == MOQ_SESS_ESTABLISHED);
        int na = 0, ne = 0;
        th_drain_counts(s, &na, &ne);
        MOQ_TEST_CHECK_EQ_INT(na, 0);              /* no response queued */
        MOQ_TEST_CHECK_EQ_INT(ne, 0);
        MOQ_TEST_CHECK_EQ_SIZE(th_used(s), used);  /* registry unchanged */
        MOQ_TEST_CHECK_EQ_INT((int)th_refs(s), (int)refs);
        MOQ_TEST_CHECK_EQ_SIZE(s->event_scratch_len, scratch);
        MOQ_TEST_CHECK(as.balance == bal);
        MOQ_TEST_CHECK(th_owner_retired(s, moq_stream_ref_from_u64(4)) == 0);
        moq_session_destroy(s);
        MOQ_TEST_CHECK(as.balance == 0);
    }
}

/* Feed a raw draft-18 PUBLISH on a fresh request bidi. */
static moq_result_t feed_d18_publish_id(moq_session_t *s, moq_stream_ref_t ref,
                                        const char *track, uint64_t alias,
                                        uint64_t request_id, bool fin)
{
    uint8_t buf[160]; moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_d18_publish_t pm = {0};
    pm.request_id = request_id;
    pm.track_namespace = (moq_namespace_t){ parts, 1 };
    pm.track_name = (moq_bytes_t){ (const uint8_t *)track, strlen(track) };
    pm.track_alias = alias;
    if (moq_d18_encode_publish(&w, &pm) < 0) return MOQ_ERR_INTERNAL;
    return moq_session_on_bidi_stream_bytes(s, ref, buf,
                                            moq_buf_writer_offset(&w), fin, 0);
}

static moq_result_t feed_d18_publish(moq_session_t *s, moq_stream_ref_t ref,
                                     const char *track, uint64_t alias,
                                     bool fin)
{ return feed_d18_publish_id(s, ref, track, alias, 0, fin); }

/* Feed a raw draft-18 TRACK_STATUS request on a fresh request bidi. */
static moq_result_t feed_d18_track_status(moq_session_t *s,
                                          moq_stream_ref_t ref,
                                          const char *track, bool fin)
{
    uint8_t buf[160]; moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_namespace_t ns = { parts, 1 };
    moq_bytes_t tn = { (const uint8_t *)track, strlen(track) };
    moq_d18_msg_params_t mp = {0};
    if (moq_d18_encode_track_status(&w, 0, &ns, tn, &mp) < 0)
        return MOQ_ERR_INTERNAL;
    return moq_session_on_bidi_stream_bytes(s, ref, buf,
                                            moq_buf_writer_offset(&w), fin, 0);
}

/* Feed a draft-18 SUBSCRIBE with an explicit request id (client ids are
 * even and must arrive in sequence). */
static moq_result_t feed_d18_subscribe_id(moq_session_t *s,
                                          moq_stream_ref_t ref,
                                          const char *track,
                                          uint64_t request_id, bool fin)
{
    uint8_t buf[128]; moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_namespace_t ns = { parts, 1 };
    moq_bytes_t tn = { (const uint8_t *)track, strlen(track) };
    moq_d18_msg_params_t mp = {0};
    if (moq_d18_encode_subscribe(&w, request_id, &ns, tn, &mp) < 0)
        return MOQ_ERR_INTERNAL;
    return moq_session_on_bidi_stream_bytes(s, ref, buf,
                                            moq_buf_writer_offset(&w), fin, 0);
}

/* SUBSCRIBE selection is allocation-free: neither the duplicate rejection
 * nor the history-full rejection performs a single allocator call. (Both
 * are selected REJECTIONS, so each still commits its inbound request id
 * and auth transaction and queues one REQUEST_ERROR -- what they never do
 * is allocate.) */
static void test_track_hist_selection_allocation_free(void)
{
    /* Duplicate rejection: a live publisher-role subscription for the same
     * identity already exists, so the second SUBSCRIBE is refused -- with
     * zero allocations. */
    {
        th_alloc_state_t as = {0};
        moq_alloc_t alloc = th_allocator(&as);
        moq_session_t *s = d18_server_established(&alloc, 4);
        MOQ_TEST_CHECK(s != NULL);
        if (!s) return;
        MOQ_TEST_CHECK(feed_d18_subscribe(s, moq_stream_ref_from_u64(4), "x",
                                          /*fin*/true) == MOQ_OK);
        th_drain_counts(s, NULL, NULL);
        int calls = as.count;
        int64_t bal = as.balance;

        /* A second SUBSCRIBE for the SAME track (next client request id):
         * duplicate. */
        MOQ_TEST_CHECK(feed_d18_subscribe_id(s, moq_stream_ref_from_u64(8),
                                             "x", 2, /*fin*/true) == MOQ_OK);
        int errs = 0, total = 0;
        th_count_request_errors(s, moq_stream_ref_from_u64(8), &errs, &total);
        MOQ_TEST_CHECK_EQ_INT(errs, 1);
        MOQ_TEST_CHECK_EQ_INT(as.count, calls);   /* zero allocator calls */
        MOQ_TEST_CHECK(as.balance == bal);
        th_drain_counts(s, NULL, NULL);
        moq_session_destroy(s);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* History-full rejection: the registry is full, so the request is
     * refused before the identity key is ever built -- zero allocations. */
    {
        th_alloc_state_t as = {0};
        moq_alloc_t alloc = th_allocator(&as);
        moq_session_t *s = d18_server_established(&alloc, 1);
        MOQ_TEST_CHECK(s != NULL);
        if (!s) return;
        moq_bytes_t p; moq_namespace_t ns = ns1(&p, "live");
        MOQ_TEST_CHECK(moq_session_note_object_published(
            s, &ns, MOQ_BYTES_LITERAL("seed"), 1, 0) == MOQ_OK);
        th_drain_counts(s, NULL, NULL);
        int calls = as.count;
        int64_t bal = as.balance;

        MOQ_TEST_CHECK(feed_d18_subscribe(s, moq_stream_ref_from_u64(4), "x",
                                          /*fin*/true) == MOQ_OK);
        int errs = 0, total = 0;
        th_count_request_errors(s, moq_stream_ref_from_u64(4), &errs, &total);
        MOQ_TEST_CHECK_EQ_INT(errs, 1);
        MOQ_TEST_CHECK_EQ_INT(as.count, calls);   /* zero allocator calls */
        MOQ_TEST_CHECK(as.balance == bal);
        th_drain_counts(s, NULL, NULL);
        moq_session_destroy(s);
        MOQ_TEST_CHECK(as.balance == 0);
    }
}

/* PUBLISH: history-full rejects with exactly one REQUEST_ERROR and stays
 * AHEAD of the later duplicate-alias close; a record key-copy failure is
 * exact NOMEM with no response. */
static void test_track_hist_tristate_publish(void)
{
    /* Full registry AND a genuine duplicate track alias: the history-full
     * rejection must win, keeping the session open. A first PUBLISH takes
     * registry slot 1 and OWNS alias 7; a seeded record fills slot 2; the
     * second PUBLISH then collides on alias 7 with a full registry.
     * Moving the duplicate-alias close ahead of the history decision
     * closes the session and fails this. */
    {
        th_alloc_state_t as = {0};
        moq_alloc_t alloc = th_allocator(&as);
        moq_session_t *s = d18_server_established(&alloc, 2);
        MOQ_TEST_CHECK(s != NULL);
        if (!s) return;
        /* No FIN: the publication must stay live to own alias 7 and hold its
         * registry slot. A same-call FIN is the peer closing its half, which
         * tears the publication down and releases both. */
        MOQ_TEST_CHECK(feed_d18_publish(s, moq_stream_ref_from_u64(4), "a",
                                        7, /*fin*/false) == MOQ_OK);
        /* The first PUBLISH surfaced a request event and owns alias 7. */
        {   int npub = 0; moq_event_t e;
            while (moq_session_poll_events(s, &e, 1) == 1) {
                if (e.kind == MOQ_EVENT_PUBLISH_REQUEST) npub++;
                moq_event_cleanup(&e); }
            MOQ_TEST_CHECK_EQ_INT(npub, 1); }
        moq_bytes_t p; moq_namespace_t ns = ns1(&p, "live");
        MOQ_TEST_CHECK(moq_session_note_object_published(
            s, &ns, MOQ_BYTES_LITERAL("seed"), 1, 0) == MOQ_OK);
        th_drain_counts(s, NULL, NULL);
        size_t used = th_used(s);
        MOQ_TEST_CHECK_EQ_SIZE(used, (size_t)2);   /* registry now full */

        MOQ_TEST_CHECK(feed_d18_publish_id(s, moq_stream_ref_from_u64(8), "x",
                                           7, 2, /*fin*/true) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(s) == MOQ_SESS_ESTABLISHED);
        int errs = 0, total = 0;
        th_count_request_errors(s, moq_stream_ref_from_u64(8), &errs, &total);
        MOQ_TEST_CHECK_EQ_INT(errs, 1);      /* exactly one REQUEST_ERROR */
        MOQ_TEST_CHECK_EQ_INT(total, 1);     /* and no other action */
        { int ne = 0; moq_event_t e;
          while (moq_session_poll_events(s, &e, 1) == 1) { ne++;
              moq_event_cleanup(&e); }
          MOQ_TEST_CHECK_EQ_INT(ne, 0); }
        MOQ_TEST_CHECK_EQ_SIZE(th_used(s), used);
        moq_session_destroy(s);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* An EXISTING record's reservation performs NO allocation at all: with
     * the same track already in the registry, arming the next allocation
     * cannot turn the reservation into a failure. (The collapsed form built
     * a throwaway canonical key here, so this ordinal used to fail.) */
    {
        th_alloc_state_t as = {0};
        moq_alloc_t alloc = th_allocator(&as);
        moq_session_t *s = d18_server_established(&alloc, 4);
        MOQ_TEST_CHECK(s != NULL);
        if (!s) return;
        moq_bytes_t p; moq_namespace_t ns = ns1(&p, "live");
        MOQ_TEST_CHECK(moq_session_note_object_published(
            s, &ns, MOQ_BYTES_LITERAL("x"), 3, 1) == MOQ_OK);
        th_drain_counts(s, NULL, NULL);
        size_t used = th_used(s);
        uint32_t refs = th_refs(s);
        int64_t bal = as.balance;
        int calls = as.count;

        /* No FIN: the reservation's ref is only observable while the
         * publication it belongs to is still live. */
        moq_result_t rc = feed_d18_publish(s, moq_stream_ref_from_u64(4),
                                           "x", 7, /*fin*/false);
        MOQ_TEST_CHECK(rc != MOQ_ERR_NOMEM);
        MOQ_TEST_CHECK(moq_session_state(s) == MOQ_SESS_ESTABLISHED);
        /* EXISTING: exactly ZERO allocator attempts for the whole
         * reservation -- the record-owned key is built only when a slot
         * is actually filled. */
        MOQ_TEST_CHECK_EQ_INT(as.count, calls);
        MOQ_TEST_CHECK(as.balance == bal + 0);
        /* The existing record gained exactly one ref and no slot. */
        MOQ_TEST_CHECK_EQ_SIZE(th_used(s), used);
        MOQ_TEST_CHECK_EQ_INT((int)th_refs(s), (int)refs + 1);
        th_drain_counts(s, NULL, NULL);
        moq_session_destroy(s);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* Record key-copy failure -> exact NOMEM, no response, nothing mutated. */
    {
        th_alloc_state_t as = {0};
        moq_alloc_t alloc = th_allocator(&as);
        moq_session_t *s = d18_server_established(&alloc, 4);
        MOQ_TEST_CHECK(s != NULL);
        if (!s) return;
        th_drain_counts(s, NULL, NULL);
        size_t used = th_used(s);
        uint32_t refs = th_refs(s);
        size_t scratch = s->event_scratch_len;
        int64_t bal = as.balance;

        /* FREE_SLOT: the reservation performs exactly ONE allocator
         * attempt -- the record-owned canonical key, built directly (no
         * temporary-key-plus-copy pair). Arming it is therefore the
         * complete NOMEM case for this path. */
        int calls0 = as.count;
        as.fail_at = as.count + 1;
        MOQ_TEST_CHECK_EQ_INT(
            (int)feed_d18_publish(s, moq_stream_ref_from_u64(4), "x", 7,
                                  /*fin*/true),
            (int)MOQ_ERR_NOMEM);
        as.fail_at = 0;
        MOQ_TEST_CHECK(moq_session_state(s) == MOQ_SESS_ESTABLISHED);
        int na = 0, ne = 0;
        th_drain_counts(s, &na, &ne);
        MOQ_TEST_CHECK_EQ_INT(na, 0);
        MOQ_TEST_CHECK_EQ_INT(ne, 0);
        MOQ_TEST_CHECK_EQ_INT(as.count, calls0 + 1);   /* exactly one */
        MOQ_TEST_CHECK_EQ_SIZE(th_used(s), used);
        MOQ_TEST_CHECK_EQ_INT((int)th_refs(s), (int)refs);
        MOQ_TEST_CHECK_EQ_SIZE(s->event_scratch_len, scratch);
        MOQ_TEST_CHECK(as.balance == bal);
        MOQ_TEST_CHECK(th_owner_retired(s, moq_stream_ref_from_u64(4)) == 0);
        moq_session_destroy(s);
        MOQ_TEST_CHECK(as.balance == 0);
    }
}

/* TRACK_STATUS: history-full rejects with one REQUEST_ERROR; the retained
 * canonical key and the record ref are released exactly once when a later
 * event-scratch copy blocks. */
static void test_track_hist_tristate_track_status(void)
{
    /* Full registry -> REQUEST_ERROR, session open. */
    {
        th_alloc_state_t as = {0};
        moq_alloc_t alloc = th_allocator(&as);
        moq_session_t *s = d18_server_established(&alloc, 1);
        MOQ_TEST_CHECK(s != NULL);
        if (!s) return;
        moq_bytes_t p; moq_namespace_t ns = ns1(&p, "live");
        MOQ_TEST_CHECK(moq_session_note_object_published(
            s, &ns, MOQ_BYTES_LITERAL("seed"), 1, 0) == MOQ_OK);
        th_drain_counts(s, NULL, NULL);
        size_t used = th_used(s);

        MOQ_TEST_CHECK(feed_d18_track_status(s, moq_stream_ref_from_u64(4),
                                             "x", /*fin*/true) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(s) == MOQ_SESS_ESTABLISHED);
        int errs = 0, total = 0;
        th_count_request_errors(s, moq_stream_ref_from_u64(4), &errs, &total);
        MOQ_TEST_CHECK_EQ_INT(errs, 1);      /* exactly one REQUEST_ERROR */
        MOQ_TEST_CHECK_EQ_INT(total, 1);     /* and no other action */
        { int ne = 0; moq_event_t e;
          while (moq_session_poll_events(s, &e, 1) == 1) { ne++;
              moq_event_cleanup(&e); }
          MOQ_TEST_CHECK_EQ_INT(ne, 0); }
        MOQ_TEST_CHECK_EQ_SIZE(th_used(s), used);
        moq_session_destroy(s);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* Canonical-key allocation failure -> exact NOMEM, no response. */
    {
        th_alloc_state_t as = {0};
        moq_alloc_t alloc = th_allocator(&as);
        moq_session_t *s = d18_server_established(&alloc, 4);
        MOQ_TEST_CHECK(s != NULL);
        if (!s) return;
        th_drain_counts(s, NULL, NULL);
        size_t used = th_used(s);
        int64_t bal = as.balance;

        as.fail_at = as.count + 1;      /* the retained canonical key */
        MOQ_TEST_CHECK_EQ_INT(
            (int)feed_d18_track_status(s, moq_stream_ref_from_u64(4), "x",
                                       /*fin*/true),
            (int)MOQ_ERR_NOMEM);
        as.fail_at = 0;
        MOQ_TEST_CHECK(moq_session_state(s) == MOQ_SESS_ESTABLISHED);
        int na = 0, ne = 0;
        th_drain_counts(s, &na, &ne);
        MOQ_TEST_CHECK_EQ_INT(na, 0);
        MOQ_TEST_CHECK_EQ_INT(ne, 0);
        MOQ_TEST_CHECK_EQ_SIZE(th_used(s), used);
        MOQ_TEST_CHECK(as.balance == bal);
        MOQ_TEST_CHECK(th_owner_retired(s, moq_stream_ref_from_u64(4)) == 0);
        moq_session_destroy(s);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* Record key-copy failure (ordinal 2) -> exact NOMEM, no response,
     * and the retained canonical key from ordinal 1 is released exactly
     * once (allocator balance returns to its pre-call value). */
    {
        th_alloc_state_t as = {0};
        moq_alloc_t alloc = th_allocator(&as);
        moq_session_t *s = d18_server_established(&alloc, 4);
        MOQ_TEST_CHECK(s != NULL);
        if (!s) return;
        th_drain_counts(s, NULL, NULL);
        size_t used = th_used(s);
        uint32_t refs = th_refs(s);
        int64_t bal = as.balance;

        as.fail_at = as.count + 2;      /* the record's key copy */
        MOQ_TEST_CHECK_EQ_INT(
            (int)feed_d18_track_status(s, moq_stream_ref_from_u64(4), "x",
                                       /*fin*/true),
            (int)MOQ_ERR_NOMEM);
        as.fail_at = 0;
        MOQ_TEST_CHECK(moq_session_state(s) == MOQ_SESS_ESTABLISHED);
        int na = 0, ne = 0;
        th_drain_counts(s, &na, &ne);
        MOQ_TEST_CHECK_EQ_INT(na, 0);
        MOQ_TEST_CHECK_EQ_INT(ne, 0);
        MOQ_TEST_CHECK_EQ_SIZE(th_used(s), used);
        MOQ_TEST_CHECK_EQ_INT((int)th_refs(s), (int)refs);
        MOQ_TEST_CHECK(as.balance == bal);   /* key released exactly once */
        MOQ_TEST_CHECK(th_owner_retired(s, moq_stream_ref_from_u64(4)) == 0);
        moq_session_destroy(s);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* A later event-scratch block releases the ref AND the retained key
     * exactly once: a tiny scratch arena makes the namespace copy fail
     * after the reservation succeeded. */
    {
        th_alloc_state_t as = {0};
        moq_alloc_t alloc = th_allocator(&as);
        moq_session_cfg_t cfg = MOQ_SESSION_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.perspective = MOQ_PERSPECTIVE_SERVER;
        cfg.version = MOQ_VERSION_DRAFT_18;
        cfg.max_track_history_records = 4;
        cfg.output_scratch_size = 8;     /* too small for the event copies */
        moq_session_t *s = NULL;
        MOQ_TEST_CHECK(moq_session_create(&cfg, 0, &s) == MOQ_OK);
        if (!s) return;
        moq_session_start(s, 0);
        { moq_action_t a;
          while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a); }
        { uint8_t setup[32]; moq_buf_writer_t w;
          moq_buf_writer_init(&w, setup, sizeof(setup));
          moq_d18_encode_setup(&w);
          moq_session_on_control_bytes(s, setup, moq_buf_writer_offset(&w), 0); }
        th_drain_counts(s, NULL, NULL);
        size_t used = th_used(s);
        uint32_t refs = th_refs(s);
        int64_t bal = as.balance;

        int calls_before = as.count;
        moq_result_t rc = feed_d18_track_status(s, moq_stream_ref_from_u64(4),
                                                "trackname", /*fin*/true);
        /* BOTH key allocations ran before the scratch copy failed: the
         * retained canonical key and the registry record's key copy. */
        MOQ_TEST_CHECK_EQ_INT(as.count - calls_before, 2);
        /* The reservation succeeded, so the scratch shortfall is the
         * PERMANENT one against an empty arena: a funded terminal close,
         * reported as MOQ_OK, releasing both the ref and the key. */
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(s) == MOQ_SESS_CLOSED);
        MOQ_TEST_CHECK_EQ_SIZE(th_used(s), used);
        MOQ_TEST_CHECK_EQ_INT((int)th_refs(s), (int)refs);
        MOQ_TEST_CHECK(as.balance == bal);   /* both keys released once */
        th_drain_counts(s, NULL, NULL);
        moq_session_destroy(s);
        MOQ_TEST_CHECK(as.balance == 0);
    }
}

int main(void)
{
    /* Static ABI assertions for the two new public event details. */
    MOQ_TEST_CHECK(sizeof(moq_subscription_update_ok_event_t) <= MOQ_EVENT_DETAIL_MAX);
    MOQ_TEST_CHECK(sizeof(moq_publication_update_ok_event_t) <= MOQ_EVENT_DETAIL_MAX);

    test_track_key_pure_comparator();
    test_track_hist_tristate_subscribe();
    test_track_hist_selection_allocation_free();
    test_track_hist_tristate_publish();
    test_track_hist_tristate_track_status();
    test_varint_boundary();
    test_d18_subscribe_reject_fin_accounting();
    test_d18_subscribe_pool_exhaustion();
    test_accept_snapshot(MOQ_VERSION_DRAFT_16);
    test_accept_snapshot(MOQ_VERSION_DRAFT_18);
    test_d18_wide_locations();
    test_d16_accept_subscribe_ceiling_reject();
    test_d18_request_update_ok_codec();
    test_update_ok_event(MOQ_VERSION_DRAFT_16);
    test_update_ok_event(MOQ_VERSION_DRAFT_18);
    test_publication_update_ok_event(MOQ_VERSION_DRAFT_16);
    test_publication_update_ok_event(MOQ_VERSION_DRAFT_18);
    test_update_ok_queue_backpressure(MOQ_VERSION_DRAFT_16);
    test_update_ok_queue_backpressure(MOQ_VERSION_DRAFT_18);
    test_publication_update_ok_backpressure(MOQ_VERSION_DRAFT_16);
    test_publication_update_ok_backpressure(MOQ_VERSION_DRAFT_18);
    test_entry_reserve_release(MOQ_VERSION_DRAFT_16);
    test_entry_reserve_release(MOQ_VERSION_DRAFT_18);
    test_write_receive_feed(MOQ_VERSION_DRAFT_16);
    test_write_receive_feed(MOQ_VERSION_DRAFT_18);
    test_track_status_ok_merge(MOQ_VERSION_DRAFT_16);
    test_track_status_ok_merge(MOQ_VERSION_DRAFT_18);
    test_receive_history_admission_timing(MOQ_VERSION_DRAFT_16);
    test_receive_history_admission_timing(MOQ_VERSION_DRAFT_18);
    test_publish_established_receive_history(MOQ_VERSION_DRAFT_16);
    test_publish_established_receive_history(MOQ_VERSION_DRAFT_18);
    test_publish_alias_rollback(MOQ_VERSION_DRAFT_16);
    test_publish_alias_rollback(MOQ_VERSION_DRAFT_18);
    test_streaming_receive_history(MOQ_VERSION_DRAFT_16);
    test_streaming_receive_history(MOQ_VERSION_DRAFT_18);
    test_publish_cap_exhaustion(MOQ_VERSION_DRAFT_16);
    test_publish_cap_exhaustion(MOQ_VERSION_DRAFT_18);
    test_track_status_cap_exhaustion(MOQ_VERSION_DRAFT_16);
    test_track_status_cap_exhaustion(MOQ_VERSION_DRAFT_18);
    test_subscribe_pool_exhaustion_graceful(MOQ_VERSION_DRAFT_16);
    test_subscribe_pool_exhaustion_graceful(MOQ_VERSION_DRAFT_18);
    test_inbound_cap_exhaustion(MOQ_VERSION_DRAFT_16);
    test_inbound_cap_exhaustion(MOQ_VERSION_DRAFT_18);

    if (failures == 0) printf("PASS\n");
    else printf("FAIL: %d checks failed\n", failures);
    return failures ? 1 : 0;
}
