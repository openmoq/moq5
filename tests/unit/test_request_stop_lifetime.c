/* A peer's STOP_SENDING on a request stream (draft-18, draft-22) ends this
 * side's sending direction only: it cancels the request, but the peer's
 * direction stays open until its FIN or RESET (draft-22 §6.4.2.2-6.4.2.3,
 * draft-18 §3.3.2, RFC 9000 §3.5). Bytes or a FIN the peer sends after it are
 * the cancelled request's and are discarded, never read as a new request.
 *
 * Pinned, on draft-18 and draft-22 each:
 * - the cancelled request ends exactly once, the session survives, the
 *   session writes nothing more on the stream (the transport binding answers
 *   the STOP with the RESET of that half), nothing is surfaced again, and the
 *   receive side's identity is reclaimed once the peer's FIN or RESET
 *   arrives;
 * - a request only partly received when the STOP arrives is never
 *   dispatched, whatever arrives after it;
 * - a FIN before the STOP (draft-22, where it is no cancellation) and a
 *   repeated STOP change nothing more: a repeated STOP keeps the identity of
 *   a stream the peer stopped until the peer's own FIN or RESET, whole or
 *   fragmented late requests in between are discarded, and a full drain
 *   table before or between the STOPs changes nothing it should not;
 * - under event pressure the STOP is refused with nothing changed and takes
 *   effect exactly once when retried; with no room to keep the stream's
 *   identity it is refused as well;
 * - a slot reused many times never delivers to a stale owner and leaks no
 *   identity;
 * - an established request stream accepts only that request's messages: a
 *   second initial request on it closes the session and leaves the owner
 *   intact, while a REQUEST_UPDATE is still taken;
 * - controls: malformed input on a request that was not cancelled stays
 *   fatal, and a GOAWAY as the first message still closes the session.
 */
#include <moq/moq.h>
#include <moq/sim.h>
#include <moq/control_d18.h>
#include <moq/buf.h>
#include "test_support.h"
#include "../../core/src/session/session_internal.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int failures;

typedef struct { size_t blocks, bytes; } balance_t;

static void *b_alloc(size_t n, void *ctx)
{
    balance_t *b = ctx;
    void *p = malloc(n ? n : 1);
    if (p) { b->blocks++; b->bytes += n; }
    return p;
}

static void b_free(void *p, size_t n, void *ctx)
{
    balance_t *b = ctx;
    if (!p) return;
    b->blocks--; b->bytes -= n;
    free(p);
}

static void *b_realloc(void *p, size_t o, size_t n, void *ctx)
{
    balance_t *b = ctx;
    if (!p) return b_alloc(n, ctx);
    if (!n) { b_free(p, o, ctx); return NULL; }
    void *q = realloc(p, n);
    if (q) b->bytes = b->bytes - o + n;
    return q;
}

typedef struct rig {
    balance_t      bal;
    moq_alloc_t    alloc;
    moq_simpair_t *sp;
    moq_session_t *c;     /* produces request bytes */
    moq_session_t *sv;    /* the side under test: receives the requests */
    moq_version_t  v;
    uint64_t       next_ref;
    /* observed on sv */
    int            requests, cancels, closes;
    uint64_t       close_code;
    int            outputs_on_ref;
    uint64_t       watch_ref;
    moq_subscription_t last_request;
    int            updates;
    int            update_priority;
} rig_t;

static bool rig_open(rig_t *r, moq_version_t v, uint32_t max_events, uint32_t max_subs)
{
    memset(r, 0, sizeof(*r));
    r->alloc = (moq_alloc_t){ .ctx = &r->bal, .alloc = b_alloc, .realloc = b_realloc,
                              .free = b_free };
    moq_simpair_cfg_t c = MOQ_SIMPAIR_CFG_INIT;
    c.alloc = &r->alloc;
    c.version = v;
    c.max_events = max_events;
    c.server_max_subscriptions = max_subs;
    c.client_send_request_capacity = true;
    c.client_initial_request_capacity = 1024;
    c.server_send_request_capacity = true;
    c.server_initial_request_capacity = 1024;
    if (moq_simpair_create(&c, &r->sp) < 0) return false;
    r->c = moq_simpair_client(r->sp);
    r->sv = moq_simpair_server(r->sp);
    r->v = v;
    r->next_ref = 0x90000;
    if (moq_simpair_start(r->sp) < 0) return false;
    moq_simpair_run_until_quiescent(r->sp, 32, NULL);
    moq_event_t ev;
    while (moq_session_poll_events(r->sv, &ev, 1) > 0) moq_event_cleanup(&ev);
    while (moq_session_poll_events(r->c, &ev, 1) > 0) moq_event_cleanup(&ev);
    return moq_session_state(r->sv) == MOQ_SESS_ESTABLISHED;
}

static void rig_close(rig_t *r, const char *what)
{
    if (r->sp) moq_simpair_destroy(r->sp);
    if (r->bal.blocks != 0 || r->bal.bytes != 0) {
        fprintf(stderr, "FAIL %s: allocator balance %zu block(s), %zu byte(s)\n", what,
                r->bal.blocks, r->bal.bytes);
        failures++;
    }
}

static moq_stream_ref_t new_ref(rig_t *r)
{
    moq_stream_ref_t ref = moq_stream_ref_from_u64(r->next_ref);
    r->next_ref += 4;
    return ref;
}

/* The bytes a client's SUBSCRIBE for `name` opens its request stream with;
 * `filter` 0 keeps the default. */
static size_t request_bytes_filtered(rig_t *r, const char *name, uint8_t filter,
                                     uint8_t *out, size_t cap)
{
    static moq_bytes_t parts[] = { { (const uint8_t *)"live", 4 } };
    moq_subscribe_cfg_t cfg;
    moq_subscribe_cfg_init(&cfg);
    if (filter) cfg.filter = filter;
    cfg.track_namespace = (moq_namespace_t){ parts, 1 };
    cfg.track_name = (moq_bytes_t){ (const uint8_t *)name, strlen(name) };
    moq_subscription_t h;
    if (moq_session_subscribe(r->c, &cfg, 0, &h) != MOQ_OK) return 0;
    size_t n = 0;
    moq_action_t a;
    while (moq_session_poll_actions(r->c, &a, 1) > 0) {
        if (a.kind == MOQ_ACTION_OPEN_BIDI_STREAM && a.u.open_bidi_stream.len <= cap) {
            memcpy(out, a.u.open_bidi_stream.data, a.u.open_bidi_stream.len);
            n = a.u.open_bidi_stream.len;
        }
        moq_action_cleanup(&a);
    }
    return n;
}

static size_t request_bytes(rig_t *r, const char *name, uint8_t *out, size_t cap)
{
    return request_bytes_filtered(r, name, 0, out, cap);
}

/* Record what sv surfaces and asks for. */
static void observe(rig_t *r)
{
    moq_event_t ev;
    while (moq_session_poll_events(r->sv, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
            r->requests++;
            r->last_request = ev.u.subscribe_request.sub;
        }
        if (ev.kind == MOQ_EVENT_UNSUBSCRIBED) r->cancels++;
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_UPDATED) {
            r->updates++;
            r->update_priority = ev.u.subscribe_updated.has_subscriber_priority
                ? (int)ev.u.subscribe_updated.subscriber_priority : -1;
        }
        if (ev.kind == MOQ_EVENT_SESSION_CLOSED) r->closes++;
        moq_event_cleanup(&ev);
    }
    moq_action_t a;
    while (moq_session_poll_actions(r->sv, &a, 1) > 0) {
        if (a.kind == MOQ_ACTION_CLOSE_SESSION) r->close_code = a.u.close_session.code;
        if ((a.kind == MOQ_ACTION_RESET_BIDI_STREAM &&
             a.u.reset_bidi_stream.stream_ref._v == r->watch_ref) ||
            (a.kind == MOQ_ACTION_ABORT_BIDI_STREAM &&
             a.u.abort_bidi_stream.stream_ref._v == r->watch_ref) ||
            (a.kind == MOQ_ACTION_SEND_BIDI_STREAM &&
             a.u.send_bidi_stream.stream_ref._v == r->watch_ref) ||
            (a.kind == MOQ_ACTION_CLOSE_BIDI_STREAM &&
             a.u.close_bidi_stream.stream_ref._v == r->watch_ref))
            r->outputs_on_ref++;
        moq_action_cleanup(&a);
    }
}

static moq_result_t feed(rig_t *r, moq_stream_ref_t ref, const uint8_t *b, size_t n, bool fin)
{
    return moq_session_on_bidi_stream_bytes(r->sv, ref, b, n, fin, 0);
}

static bool identity_kept(rig_t *r, moq_stream_ref_t ref)
{
    return drain_ref_contains(r->sv, ref) ||
           request_registry_find_by_streamref(r->sv, ref).kind != MOQ_REQ_NONE;
}

/* A REQUEST_UPDATE with an empty body: framed, but not a valid update. Read
 * as anything on a live request, it would be a protocol error. */
static const uint8_t k_late[] = { 0x02, 0x00, 0x00 };

static void expect(bool ok, const char *what, const rig_t *r)
{
    if (ok) return;
    fprintf(stderr, "FAIL d%u %s: requests=%d cancels=%d closes=%d close=0x%llx outputs=%d "
            "state=%d\n", (unsigned)r->v, what, r->requests, r->cancels, r->closes,
            (unsigned long long)r->close_code, r->outputs_on_ref,
            (int)moq_session_state(r->sv));
    failures++;
}

/* Complete request, optionally accepted, then STOP, then late bytes and a
 * FIN (or a RESET). */
static void test_complete_then_stop(moq_version_t v, bool accept, int after)
{
    char what[96];
    snprintf(what, sizeof(what), "complete%s then STOP then %s", accept ? "+accepted" : "",
             after == 0 ? "bytes+FIN" : after == 1 ? "FIN" : "RESET");
    rig_t r;
    if (!rig_open(&r, v, 0, 0)) { failures++; rig_close(&r, what); return; }
    uint8_t req[512];
    size_t n = request_bytes(&r, "a", req, sizeof(req));
    moq_stream_ref_t ref = new_ref(&r);
    r.watch_ref = ref._v;
    MOQ_TEST_CHECK(n > 0 && feed(&r, ref, req, n, false) == MOQ_OK);
    observe(&r);
    expect(r.requests == 1, "request surfaced", &r);
    if (accept) {
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        MOQ_TEST_CHECK(moq_session_accept_subscribe(r.sv, r.last_request, &acc, 0) == MOQ_OK);
        observe(&r);
    }
    r.outputs_on_ref = 0;
    MOQ_TEST_CHECK(moq_session_on_bidi_stream_stop(r.sv, ref, 0x1, 0) == MOQ_OK);
    observe(&r);
    expect(r.cancels == 1, what, &r);
    expect(r.outputs_on_ref == 0, "nothing more written on the stream", &r);
    MOQ_TEST_CHECK(sub_resolve_handle(r.sv, r.last_request) < 0);
    expect(identity_kept(&r, ref), "identity kept until the peer's terminal", &r);
    if (after == 2) {
        MOQ_TEST_CHECK(moq_session_on_bidi_stream_reset(r.sv, ref, 0x1, 0) == MOQ_OK);
    } else {
        if (after == 0) (void)feed(&r, ref, k_late, sizeof(k_late), false);
        (void)feed(&r, ref, NULL, 0, true);
    }
    observe(&r);
    expect(r.closes == 0 && moq_session_state(r.sv) == MOQ_SESS_ESTABLISHED, what, &r);
    expect(r.requests == 1 && r.cancels == 1, "nothing surfaced again", &r);
    expect(!identity_kept(&r, ref), "identity reclaimed", &r);
    rig_close(&r, what);
}

/* Part of a request, then STOP, then the rest and a FIN, or a RESET. */
static void test_partial_then_stop(moq_version_t v, bool reset)
{
    const char *what = reset ? "partial then STOP then RESET" : "partial then STOP then tail+FIN";
    rig_t r;
    if (!rig_open(&r, v, 0, 0)) { failures++; rig_close(&r, what); return; }
    uint8_t req[512];
    size_t n = request_bytes(&r, "a", req, sizeof(req));
    moq_stream_ref_t ref = new_ref(&r);
    r.watch_ref = ref._v;
    size_t half = n / 2;
    MOQ_TEST_CHECK(n > 4 && feed(&r, ref, req, half, false) == MOQ_OK);
    observe(&r);
    expect(r.requests == 0, "nothing yet", &r);
    r.outputs_on_ref = 0;
    MOQ_TEST_CHECK(moq_session_on_bidi_stream_stop(r.sv, ref, 0x1, 0) == MOQ_OK);
    observe(&r);
    expect(r.outputs_on_ref == 0, "nothing more written on the stream", &r);
    expect(identity_kept(&r, ref), "identity kept until the peer's terminal", &r);
    if (reset) {
        MOQ_TEST_CHECK(moq_session_on_bidi_stream_reset(r.sv, ref, 0x1, 0) == MOQ_OK);
    } else {
        (void)feed(&r, ref, req + half, n - half, false);
        (void)feed(&r, ref, NULL, 0, true);
    }
    observe(&r);
    expect(r.requests == 0 && r.cancels == 0, "never dispatched", &r);
    expect(r.closes == 0 && moq_session_state(r.sv) == MOQ_SESS_ESTABLISHED, what, &r);
    expect(!identity_kept(&r, ref), "identity reclaimed", &r);
    rig_close(&r, what);
}

/* A STOP repeated, after a FIN where the FIN leaves the request standing
 * (draft-22: a requester's FIN is no cancellation). Draft-18 keeps its own
 * rule for a FIN on an established subscription's stream, so there the STOP
 * is repeated without one and the stream then ends with the peer's FIN. */
static void test_fin_then_stop_and_duplicate(moq_version_t v)
{
    const bool fin_first = false;   /* draft-18: a FIN is not the case here */
    const char *what = fin_first ? "FIN then STOP, STOP twice" : "STOP twice";
    rig_t r;
    if (!rig_open(&r, v, 0, 0)) { failures++; rig_close(&r, what); return; }
    uint8_t req[512];
    size_t n = request_bytes(&r, "a", req, sizeof(req));
    moq_stream_ref_t ref = new_ref(&r);
    r.watch_ref = ref._v;
    MOQ_TEST_CHECK(n > 0 && feed(&r, ref, req, n, false) == MOQ_OK);
    observe(&r);
    moq_accept_subscribe_cfg_t acc;
    moq_accept_subscribe_cfg_init(&acc);
    MOQ_TEST_CHECK(moq_session_accept_subscribe(r.sv, r.last_request, &acc, 0) == MOQ_OK);
    observe(&r);
    if (fin_first) (void)feed(&r, ref, NULL, 0, true);
    observe(&r);
    int cancels_after_fin = r.cancels;
    MOQ_TEST_CHECK(moq_session_on_bidi_stream_stop(r.sv, ref, 0x1, 0) == MOQ_OK);
    observe(&r);
    if (!fin_first) {
        expect(identity_kept(&r, ref), "identity kept after the STOP", &r);
        (void)feed(&r, ref, k_late, sizeof(k_late), false);
        observe(&r);
        expect(r.closes == 0 && r.requests == 1, "late bytes discarded", &r);
    }
    MOQ_TEST_CHECK(moq_session_on_bidi_stream_stop(r.sv, ref, 0x1, 0) == MOQ_OK);
    observe(&r);
    expect(r.cancels == 1 && cancels_after_fin <= 1, "ended once", &r);
    expect(r.closes == 0 && moq_session_state(r.sv) == MOQ_SESS_ESTABLISHED, what, &r);
    if (!fin_first) {
        expect(identity_kept(&r, ref), "identity kept across the repeated STOP", &r);
        (void)feed(&r, ref, NULL, 0, true);
        observe(&r);
    }
    expect(!identity_kept(&r, ref), "identity reclaimed", &r);
    rig_close(&r, what);
}

/* STOP, STOP again, then a whole or fragmented late request, then the peer's
 * FIN or RESET. `pressure` 1 fills the drain table before the first STOP (it
 * is refused with nothing changed, then retried); 2 fills it between the two
 * STOPs (the repeat needs no room). Once the stream ends, the next request
 * on a new stream takes the freed slot and the drain table is empty. */
static void test_repeated_stop(moq_version_t v, bool fragmented, bool reset, int pressure)
{
    char what[96];
    snprintf(what, sizeof(what), "STOP, STOP, %s late request, %s, pressure %d",
             fragmented ? "fragmented" : "whole", reset ? "RESET" : "FIN", pressure);
    rig_t r;
    if (!rig_open(&r, v, 0, 1)) { failures++; rig_close(&r, what); return; }
    uint8_t req[512], late[512];
    size_t n = request_bytes(&r, "a", req, sizeof(req));
    size_t ln = request_bytes(&r, "b", late, sizeof(late));
    moq_stream_ref_t ref = new_ref(&r);
    r.watch_ref = ref._v;
    MOQ_TEST_CHECK(n > 0 && ln > 4 && feed(&r, ref, req, n, false) == MOQ_OK);
    observe(&r);
    moq_accept_subscribe_cfg_t acc;
    moq_accept_subscribe_cfg_init(&acc);
    MOQ_TEST_CHECK(moq_session_accept_subscribe(r.sv, r.last_request, &acc, 0) == MOQ_OK);
    observe(&r);
    size_t saved_cap = r.sv->drain_ref_cap;
    if (pressure == 1) {
        r.sv->drain_ref_cap = r.sv->drain_ref_count;
        expect(moq_session_on_bidi_stream_stop(r.sv, ref, 0x1, 0) == MOQ_ERR_WOULD_BLOCK,
               "refused without room", &r);
        expect(sub_resolve_handle(r.sv, r.last_request) >= 0 &&
               !drain_ref_contains(r.sv, ref), "nothing changed at refusal", &r);
        r.sv->drain_ref_cap = saved_cap;
    }
    r.outputs_on_ref = 0;
    MOQ_TEST_CHECK(moq_session_on_bidi_stream_stop(r.sv, ref, 0x1, 0) == MOQ_OK);
    observe(&r);
    if (pressure == 2) r.sv->drain_ref_cap = r.sv->drain_ref_count;
    MOQ_TEST_CHECK(moq_session_on_bidi_stream_stop(r.sv, ref, 0x1, 0) == MOQ_OK);
    observe(&r);
    r.sv->drain_ref_cap = saved_cap;
    expect(identity_kept(&r, ref), "identity kept across the repeated STOP", &r);
    if (fragmented) {
        (void)feed(&r, ref, late, ln / 2, false);
        (void)feed(&r, ref, late + ln / 2, ln - ln / 2, false);
    } else {
        (void)feed(&r, ref, late, ln, false);
    }
    observe(&r);
    expect(identity_kept(&r, ref), "identity kept until the peer's terminal", &r);
    if (reset)
        MOQ_TEST_CHECK(moq_session_on_bidi_stream_reset(r.sv, ref, 0x1, 0) == MOQ_OK);
    else
        (void)feed(&r, ref, NULL, 0, true);
    observe(&r);
    expect(r.requests == 1 && r.cancels == 1 && r.closes == 0 &&
           moq_session_state(r.sv) == MOQ_SESS_ESTABLISHED, what, &r);
    expect(r.outputs_on_ref == 0, "nothing more written on the stream", &r);
    expect(!identity_kept(&r, ref) && r.sv->drain_ref_count == 0, "retired exactly", &r);
    /* The slot is reused by the next request. */
    uint8_t next[512];
    size_t nn = request_bytes(&r, "c", next, sizeof(next));
    moq_stream_ref_t ref2 = new_ref(&r);
    MOQ_TEST_CHECK(nn > 0 && feed(&r, ref2, next, nn, false) == MOQ_OK);
    observe(&r);
    expect(r.requests == 2 && sub_resolve_handle(r.sv, r.last_request) >= 0, "slot reused", &r);
    rig_close(&r, what);
}

/* A full event queue refuses the STOP with nothing changed; it takes effect
 * once when retried. A full drain table refuses it too, keeping the request. */
static void test_pressure(moq_version_t v, bool drain_full)
{
    const char *what = drain_full ? "STOP with no room for the identity"
                                  : "STOP under event pressure";
    rig_t r;
    if (!rig_open(&r, v, drain_full ? 0 : 2, 0)) { failures++; rig_close(&r, what); return; }
    uint8_t req[512];
    size_t n = request_bytes(&r, "a", req, sizeof(req));
    moq_stream_ref_t ref = new_ref(&r);
    r.watch_ref = ref._v;
    MOQ_TEST_CHECK(n > 0 && feed(&r, ref, req, n, false) == MOQ_OK);
    observe(&r);
    moq_accept_subscribe_cfg_t acc;
    moq_accept_subscribe_cfg_init(&acc);
    MOQ_TEST_CHECK(moq_session_accept_subscribe(r.sv, r.last_request, &acc, 0) == MOQ_OK);
    observe(&r);
    size_t saved_cap = r.sv->drain_ref_cap;
    if (drain_full) {
        r.sv->drain_ref_cap = r.sv->drain_ref_count;
    } else {
        /* Two events nobody polls fill the queue. */
        for (int i = 0; i < 2; i++) {
            moq_event_t fill;
            memset(&fill, 0, sizeof(fill));
            fill.kind = MOQ_EVENT_GOAWAY;
            fill.detail_size = (uint32_t)sizeof(moq_goaway_event_t);
            fill.borrow_epoch = r.sv->borrow_epoch;
            (void)push_event(r.sv, &fill);
        }
        MOQ_TEST_CHECK(event_queue_full(r.sv));
    }
    moq_result_t rc = moq_session_on_bidi_stream_stop(r.sv, ref, 0x1, 0);
    expect(rc == MOQ_ERR_WOULD_BLOCK, "refused under pressure", &r);
    expect(sub_resolve_handle(r.sv, r.last_request) >= 0, "nothing changed", &r);
    if (drain_full) {
        r.sv->drain_ref_cap = saved_cap;
    } else {
        moq_event_t ev;
        while (moq_session_poll_events(r.sv, &ev, 1) > 0) moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK(moq_session_on_bidi_stream_stop(r.sv, ref, 0x1, 0) == MOQ_OK);
    observe(&r);
    expect(r.cancels == 1, "takes effect once", &r);
    expect(identity_kept(&r, ref), "identity kept", &r);
    (void)feed(&r, ref, k_late, sizeof(k_late), true);
    observe(&r);
    expect(r.closes == 0 && r.cancels == 1, what, &r);
    expect(!identity_kept(&r, ref), "identity reclaimed", &r);
    rig_close(&r, what);
}

/* One subscription slot, reused across many STOPped requests whose late
 * bytes arrive after the next request took the slot. */
static void test_slot_reuse(moq_version_t v)
{
    const char *what = "slot reuse";
    rig_t r;
    if (!rig_open(&r, v, 0, 1)) { failures++; rig_close(&r, what); return; }
    moq_stream_ref_t prev = { 0 };
    for (int i = 0; i < 20; i++) {
        char name[16];
        snprintf(name, sizeof(name), "t%d", i);
        uint8_t req[512];
        size_t n = request_bytes(&r, name, req, sizeof(req));
        moq_stream_ref_t ref = new_ref(&r);
        r.watch_ref = ref._v;
        int before = r.requests;
        MOQ_TEST_CHECK(n > 0 && feed(&r, ref, req, n, false) == MOQ_OK);
        observe(&r);
        expect(r.requests == before + 1, "each request surfaced", &r);
        moq_subscription_t live = r.last_request;
        /* The previous request's late bytes and FIN land on the new owner's
         * slot generation: they must not reach it. */
        if (prev._v != 0) {
            (void)feed(&r, prev, k_late, sizeof(k_late), true);
            observe(&r);
            expect(sub_resolve_handle(r.sv, live) >= 0, "live owner untouched", &r);
            expect(!identity_kept(&r, prev), "old identity reclaimed", &r);
        }
        MOQ_TEST_CHECK(moq_session_on_bidi_stream_stop(r.sv, ref, 0x1, 0) == MOQ_OK);
        observe(&r);
        prev = ref;
    }
    (void)feed(&r, prev, NULL, 0, true);
    observe(&r);
    expect(r.requests == 20 && r.cancels == 20 && r.closes == 0, what, &r);
    expect(r.sv->drain_ref_count == 0, "no identity left", &r);
    rig_close(&r, what);
}

/* A pending SUBSCRIBE with one buffered Joining FETCH (§10.12.2), both peer
 * halves open, and room for exactly one more drain reference. A STOP on the
 * SUBSCRIBE needs two: the rejected join keeps its stream's identity, and so
 * does the cancelled SUBSCRIBE. The STOP is refused with nothing changed at the
 * session (owners, queued events and actions, drain references); once one
 * older reference is released it takes effect exactly once, keeping both
 * identities, and the peers' later FIN and RESET retire each exactly once.
 * With `reset`, a RESET instead of a STOP needs no reference for the
 * SUBSCRIBE and is not refused. */
static void push_raw_join(rig_t *r, moq_stream_ref_t ref, uint64_t req_id,
                          uint64_t join_req_id)
{
    moq_d18_fetch_t f;
    memset(&f, 0, sizeof(f));
    f.request_id = req_id;
    f.fetch_type = 2;                 /* relative joining */
    f.joining_request_id = join_req_id;
    f.joining_start = 0;
    uint8_t buf[256];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    MOQ_TEST_CHECK(moq_d18_encode_fetch(&w, &f) == MOQ_OK);
    MOQ_TEST_CHECK(feed(r, ref, buf, moq_buf_writer_offset(&w), false) == MOQ_OK);
}

static size_t pending_joins(const moq_session_t *s)
{
    size_t n = 0;
    for (size_t i = 0; i < s->fetch_cap; i++)
        if (s->fetches[i].state == MOQ_FETCH_PENDING_JOIN) n++;
    return n;
}

/* The join's rejection, read from the session's actions: every response on
 * the join's request stream (draft-18 correlates a response with its request
 * by the stream; REQUEST_ERROR carries no Request ID) is counted, and the
 * first is decoded. A rejection is exactly one REQUEST_ERROR with the expected
 * code, carrying the FIN. Other actions are discarded. */
typedef struct join_response {
    int      responses;      /* SEND_BIDI or CLOSE_BIDI on the join's stream */
    bool     is_request_error;
    uint64_t error_code;
    bool     fin;
} join_response_t;

static void collect_join_response(rig_t *r, moq_stream_ref_t jref, join_response_t *out)
{
    moq_action_t a;
    while (moq_session_poll_actions(r->sv, &a, 1) > 0) {
        if (a.kind == MOQ_ACTION_SEND_BIDI_STREAM &&
            a.u.send_bidi_stream.stream_ref._v == jref._v) {
            if (out->responses++ == 0) {
                moq_buf_reader_t rd;
                moq_buf_reader_init(&rd, a.u.send_bidi_stream.data,
                                    a.u.send_bidi_stream.len);
                moq_control_envelope_t env;
                moq_d18_request_error_t re;
                if (moq_d18_decode_envelope(&rd, &env) == MOQ_OK &&
                    env.msg_type == MOQ_D18_REQUEST_ERROR &&
                    moq_buf_reader_remaining(&rd) == 0 &&
                    moq_d18_decode_request_error(env.payload, env.payload_len,
                                                 &re) == MOQ_OK) {
                    out->is_request_error = true;
                    out->error_code = re.error_code;
                }
                out->fin = a.u.send_bidi_stream.fin;
            }
        } else if (a.kind == MOQ_ACTION_CLOSE_BIDI_STREAM &&
                   a.u.close_bidi_stream.stream_ref._v == jref._v) {
            out->responses++;
        }
        moq_action_cleanup(&a);
    }
}

/* The rejection a buffered join gets once its subscription is gone. */
static const uint64_t k_join_rejection_code = MOQ_REQUEST_ERROR_INVALID_JOINING_REQUEST_ID;

static void test_stop_with_buffered_join(moq_version_t v, bool reset)
{
    const char *what = reset ? "RESET of a pending SUBSCRIBE with a buffered join"
                             : "STOP of a pending SUBSCRIBE with a buffered join";
    rig_t r;
    if (!rig_open(&r, v, 0, 0)) { failures++; rig_close(&r, what); return; }
    uint8_t req[512];
    /* A joining FETCH joins a LARGEST_OBJECT subscription (§10.12.2). */
    size_t n = request_bytes_filtered(&r, "a", MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT,
                                      req, sizeof(req));
    moq_stream_ref_t sref = new_ref(&r), jref = new_ref(&r);
    r.watch_ref = sref._v;
    MOQ_TEST_CHECK(n > 0 && feed(&r, sref, req, n, false) == MOQ_OK);
    observe(&r);
    expect(r.requests == 1, "SUBSCRIBE surfaced, left pending", &r);
    push_raw_join(&r, jref, 2, 0);
    expect(pending_joins(r.sv) == 1, "join buffered", &r);

    /* Older references: all but one slot in use. */
    size_t older = r.sv->drain_ref_cap - 1;
    for (size_t i = 0; i < older; i++)
        MOQ_TEST_CHECK(drain_ref_add(r.sv, moq_stream_ref_from_u64(0x70000 + 4 * i)));

    if (!reset) {
        size_t ev_used = r.sv->event_tail - r.sv->event_head;
        size_t act_avail = action_queue_avail(r.sv);
        size_t drains = r.sv->drain_ref_count;
        moq_result_t rc = moq_session_on_bidi_stream_stop(r.sv, sref, 0x1, 0);
        expect(rc == MOQ_ERR_WOULD_BLOCK, "refused: two references, one slot", &r);
        expect(request_registry_find_by_streamref(r.sv, sref).kind == MOQ_REQ_SUBSCRIPTION,
               "SUBSCRIBE owner unchanged", &r);
        expect(request_registry_find_by_streamref(r.sv, jref).kind == MOQ_REQ_FETCH &&
               pending_joins(r.sv) == 1, "join owner unchanged", &r);
        expect(r.sv->event_tail - r.sv->event_head == ev_used &&
               action_queue_avail(r.sv) == act_avail, "queues unchanged", &r);
        expect(r.sv->drain_ref_count == drains, "references unchanged", &r);
        (void)drain_ref_remove(r.sv, moq_stream_ref_from_u64(0x70000));
    }

    moq_result_t rc = reset
        ? moq_session_on_bidi_stream_reset(r.sv, sref, 0x1, 0)
        : moq_session_on_bidi_stream_stop(r.sv, sref, 0x1, 0);
    expect(rc == MOQ_OK, "takes effect", &r);
    join_response_t jr = { 0 };
    {
        moq_event_t ev;
        while (moq_session_poll_events(r.sv, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_UNSUBSCRIBED) r.cancels++;
            if (ev.kind == MOQ_EVENT_SESSION_CLOSED) r.closes++;
            moq_event_cleanup(&ev);
        }
        collect_join_response(&r, jref, &jr);
    }
    expect(r.cancels == 1, "one subscription cancellation", &r);
    expect(jr.responses == 1, "exactly one response on the join's stream", &r);
    expect(jr.is_request_error, "the response is a REQUEST_ERROR", &r);
    expect(jr.error_code == k_join_rejection_code, "INVALID_JOINING_REQUEST_ID", &r);
    expect(jr.fin, "the rejection carries the FIN", &r);
    expect(pending_joins(r.sv) == 0, "join owner retired", &r);
    expect(request_registry_find_by_streamref(r.sv, sref).kind == MOQ_REQ_NONE,
           "SUBSCRIBE owner retired", &r);
    expect(drain_ref_contains(r.sv, jref), "join identity kept", &r);
    expect(reset ? !drain_ref_contains(r.sv, sref) : drain_ref_contains(r.sv, sref),
           reset ? "no reference for a reset SUBSCRIBE" : "SUBSCRIBE identity kept", &r);

    if (!reset) {
        MOQ_TEST_CHECK(moq_session_on_bidi_stream_stop(r.sv, sref, 0x1, 0) == MOQ_OK);
        expect(drain_ref_contains(r.sv, sref), "repeated STOP keeps the identity", &r);
        (void)feed(&r, sref, k_late, sizeof(k_late), true);       /* late bytes + FIN */
        expect(!drain_ref_contains(r.sv, sref), "SUBSCRIBE identity retired by FIN", &r);
    }
    MOQ_TEST_CHECK(moq_session_on_bidi_stream_reset(r.sv, jref, 0x1, 0) == MOQ_OK);
    expect(!drain_ref_contains(r.sv, jref), "join identity retired by RESET", &r);
    {
        join_response_t again = { 0 };
        collect_join_response(&r, jref, &again);
        expect(again.responses == 0,
               "no second response after repeated STOP and terminals", &r);
    }
    observe(&r);
    expect(r.closes == 0 && moq_session_state(r.sv) == MOQ_SESS_ESTABLISHED,
           "session survives", &r);
    expect(r.cancels == 1, "cancelled exactly once", &r);
    for (size_t i = reset ? 0 : 1; i < older; i++)
        (void)drain_ref_remove(r.sv, moq_stream_ref_from_u64(0x70000 + 4 * i));
    expect(r.sv->drain_ref_count == 0, "every identity retired exactly once", &r);
    rig_close(&r, what);
}

static void test_controls(moq_version_t v, bool goaway)
{
    const char *what = goaway ? "GOAWAY as first message" : "malformed uncancelled request";
    rig_t r;
    if (!rig_open(&r, v, 0, 0)) { failures++; rig_close(&r, what); return; }
    moq_stream_ref_t ref = new_ref(&r);
    if (goaway) {
        static const uint8_t g[] = { 0x10, 0x00, 0x02, 0x00, 0x00 };
        (void)feed(&r, ref, g, sizeof(g), false);
    } else {
        (void)feed(&r, ref, k_late, sizeof(k_late), false);
    }
    observe(&r);
    expect(moq_session_state(r.sv) == MOQ_SESS_CLOSED && r.close_code == 0x3, what, &r);
    rig_close(&r, what);
}

int main(void)
{
    const moq_version_t vs[] = { MOQ_VERSION_DRAFT_18 };
    for (size_t i = 0; i < sizeof(vs) / sizeof(vs[0]); i++) {
        moq_version_t v = vs[i];
        int before = failures;
        for (int after = 0; after < 3; after++) {
            test_complete_then_stop(v, true, after);
            test_complete_then_stop(v, false, after);
        }
        test_partial_then_stop(v, false);
        test_partial_then_stop(v, true);
        test_fin_then_stop_and_duplicate(v);
        for (int frag = 0; frag < 2; frag++)
            for (int rst = 0; rst < 2; rst++)
                test_repeated_stop(v, frag != 0, rst != 0, 0);
        test_repeated_stop(v, false, false, 1);
        test_repeated_stop(v, true, true, 2);
        test_pressure(v, false);
        test_pressure(v, true);
        test_slot_reuse(v);
        test_stop_with_buffered_join(v, false);
        test_stop_with_buffered_join(v, true);
        test_controls(v, false);
        test_controls(v, true);
        if (failures != before) fprintf(stderr, "  in d%u\n", (unsigned)v);
    }
    if (failures) {
        fprintf(stderr, "%d failure(s)\n", failures);
        return 1;
    }
    printf("PASS: request_stop_lifetime\n");
    return 0;
}
