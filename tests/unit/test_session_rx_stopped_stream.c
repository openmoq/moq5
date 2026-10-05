/*
 * Receive-stream lifetime after a local STOP (both drafts).
 *
 * A fetcher that has cancelled a request STOPs the late response stream at
 * its FETCH_HEADER. STOP_SENDING cannot recall what the publisher already
 * sent, so bytes keep arriving on that stream until the peer's FIN or RESET:
 * the session must keep the stream's identity in a stopped/discard state,
 * consume those bytes without parsing or emitting, release the stream only
 * at FIN/RESET (never counting it as a completed stream), queue the STOP
 * exactly once even when the action queue first refuses it, keep ordinary
 * streams flowing meanwhile, reuse the receive slot afterwards, and still
 * refuse a genuinely malformed new stream and data after a real FIN.
 *
 * Harness: the publisher's real response bytes are captured once through
 * SimPair (every transport input the fetcher received, in order), then
 * replayed byte-for-byte into fresh raw fetcher sessions whose request ids
 * match (same session code, same request order) under each schedule.
 */
#include <moq/session.h>
#include <moq/sim.h>
#include "test_session_support.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>

static int failures = 0;

/* -- capture ------------------------------------------------------------- */

enum { CK_CONTROL = 1, CK_DATA = 2, CK_BIDI = 3 };

typedef struct chunk {
    int      kind;
    uint64_t stream;
    bool     fin;
    size_t   len;
    uint8_t  bytes[256];
} chunk_t;

#define CAP_MAX 128
typedef struct cap {
    bool    armed;
    int     n;
    chunk_t c[CAP_MAX];
    bool    overflow;
} cap_t;

static void
cap_fn(void *ctx, const moq_sim_trace_record_t *r)
{
    cap_t *t = (cap_t *)ctx;
    if (!t->armed || r->kind != MOQ_SIM_TRACE_INPUT || r->to != MOQ_PERSPECTIVE_CLIENT) return;
    int kind;
    if (r->input_kind == MOQ_SIM_INPUT_CONTROL_BYTES) kind = CK_CONTROL;
    else if (r->input_kind == MOQ_SIM_INPUT_DATA_BYTES) kind = CK_DATA;
    else if (r->input_kind == MOQ_SIM_INPUT_BIDI_BYTES) kind = CK_BIDI;
    else return;
    if (r->bytes.len > sizeof(t->c[0].bytes) || t->n >= CAP_MAX) { t->overflow = true; return; }
    chunk_t *c = &t->c[t->n++];
    c->kind = kind;
    c->stream = (r->struct_size >= offsetof(moq_sim_trace_record_t, stream_ref) + sizeof(r->stream_ref))
                    ? r->stream_ref._v : 0;
    c->fin = (r->struct_size >= offsetof(moq_sim_trace_record_t, fin) + sizeof(r->fin)) ? r->fin : false;
    c->len = r->bytes.len;
    if (c->len) memcpy(c->bytes, r->bytes.data, c->len);
}

/* One captured response per request k (0..3): the inputs the fetcher saw
 * between issuing FETCH k and polling its FETCH_COMPLETE. */
#define N_REQ 4
typedef struct responses {
    cap_t   per[N_REQ];
    uint64_t data_stream[N_REQ];   /* the sim's data stream ref for k */
    uint64_t bidi_stream[N_REQ];   /* the sim's request bidi ref for k (d18) */
} responses_t;

typedef struct ev_obs {
    int request, ok, object, complete, error, reset, cancelled, closed;
    moq_fetch_t req;
    char close_reason[64];
} ev_obs_t;

static void
drain(moq_session_t *s, ev_obs_t *o)
{
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        switch (ev.kind) {
        case MOQ_EVENT_FETCH_REQUEST:   o->request++; o->req = ev.u.fetch_request.fetch; break;
        case MOQ_EVENT_FETCH_OK:        o->ok++; break;
        case MOQ_EVENT_FETCH_OBJECT:
            MOQ_TEST_CHECK(moq_rcbuf_len(ev.u.fetch_object.payload) == 16);
            for (size_t j = 0; j < moq_rcbuf_len(ev.u.fetch_object.payload); j++)
                MOQ_TEST_CHECK(moq_rcbuf_data(ev.u.fetch_object.payload)[j] ==
                               (uint8_t)(0xd0 + ev.u.fetch_object.object_id));
            o->object++;
            break;
        case MOQ_EVENT_FETCH_COMPLETE:  o->complete++; break;
        case MOQ_EVENT_FETCH_ERROR:     o->error++; break;
        case MOQ_EVENT_FETCH_RESET:     o->reset++; break;
        case MOQ_EVENT_FETCH_CANCELLED: o->cancelled++; break;
        case MOQ_EVENT_SESSION_CLOSED: {
            o->closed++;
            size_t n = ev.u.closed.reason.len < sizeof(o->close_reason) - 1
                           ? ev.u.closed.reason.len : sizeof(o->close_reason) - 1;
            if (n) memcpy(o->close_reason, ev.u.closed.reason.data, n);
            o->close_reason[n] = '\0';
            break;
        }
        default: break;
        }
        moq_event_cleanup(&ev);
    }
}

static moq_result_t
issue_fetch(moq_session_t *s, uint64_t now, moq_fetch_t *h)
{
    moq_bytes_t nsp[1] = { MOQ_BYTES_LITERAL("live") };
    moq_fetch_cfg_t fc;
    moq_fetch_cfg_init(&fc);
    fc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 1 };
    fc.track_name = MOQ_BYTES_LITERAL("t");
    fc.end_group = 0;
    fc.end_object = 3;
    return moq_session_fetch(s, &fc, now, h);
}

static moq_result_t
write_obj(moq_session_t *pub, moq_fetch_t fh, uint64_t oid, uint64_t now)
{
    uint8_t body[16];
    memset(body, (int)(0xd0 + oid), sizeof(body));
    moq_rcbuf_t *pl = NULL;
    if (moq_rcbuf_create(moq_alloc_default(), body, sizeof(body), &pl) != MOQ_OK) return MOQ_ERR_NOMEM;
    moq_fetch_object_cfg_t oc;
    moq_fetch_object_cfg_init(&oc);
    oc.group_id = 0;
    oc.subgroup_id = 0;
    oc.object_id = oid;
    oc.publisher_priority = 100;
    oc.payload = pl;
    moq_result_t rc = moq_session_write_fetch_object(pub, fh, &oc, now);
    moq_rcbuf_decref(pl);
    return rc;
}

static bool
capture(moq_version_t version, responses_t *out)
{
    memset(out, 0, sizeof(*out));
    static cap_t all;
    memset(&all, 0, sizeof(all));
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = moq_alloc_default();
    cfg.seed = 0x5709u;
    cfg.version = version;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 64;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 64;
    cfg.trace_fn = cap_fn;
    cfg.trace_ctx = &all;
    moq_simpair_t *sp = NULL;
    if (moq_simpair_create(&cfg, &sp) != MOQ_OK || moq_simpair_start(sp) != MOQ_OK) return false;
    moq_session_t *fetcher = moq_simpair_client(sp);
    moq_session_t *pub = moq_simpair_server(sp);
    uint64_t now = 1;
#define STEP() do { now += 1000; (void)moq_simpair_advance_to(sp, now); size_t st_ = 0; \
                    (void)moq_simpair_run_until_quiescent(sp, 64, &st_); } while (0)
    for (int i = 0; i < 8; i++) STEP();
    ev_obs_t fo, po;
    memset(&fo, 0, sizeof(fo));
    memset(&po, 0, sizeof(po));
    drain(fetcher, &fo);
    drain(pub, &po);
    bool ok = moq_session_state(fetcher) == MOQ_SESS_ESTABLISHED;
    for (int k = 0; k < N_REQ && ok; k++) {
        moq_fetch_t fh;
        ok = issue_fetch(fetcher, now, &fh) == MOQ_OK;
        po.request = 0;
        for (int i = 0; i < 6 && ok && po.request == 0; i++) { STEP(); drain(pub, &po); }
        ok = ok && po.request == 1;
        all.n = 0;
        all.armed = true;
        moq_accept_fetch_cfg_t ac;
        moq_accept_fetch_cfg_init(&ac);
        ac.end_group = 0;
        ac.end_object = 3;
        ok = ok && moq_session_accept_fetch(pub, po.req, &ac, now) == MOQ_OK;
        ok = ok && write_obj(pub, po.req, 0, now) == MOQ_OK;
        ok = ok && write_obj(pub, po.req, 1, now) == MOQ_OK;
        ok = ok && moq_session_end_fetch(pub, po.req, now) == MOQ_OK;
        fo.complete = 0;
        for (int i = 0; i < 8 && ok && fo.complete == 0; i++) { STEP(); drain(fetcher, &fo); }
        all.armed = false;
        ok = ok && fo.complete == 1 && !all.overflow && all.n > 0;
        out->per[k] = all;
        for (int i = 0; i < all.n; i++) {
            if (all.c[i].kind == CK_DATA && out->data_stream[k] == 0) out->data_stream[k] = all.c[i].stream;
            if (all.c[i].kind == CK_BIDI && out->bidi_stream[k] == 0) out->bidi_stream[k] = all.c[i].stream;
        }
        ok = ok && out->data_stream[k] != 0;
    }
#undef STEP
    moq_simpair_destroy(sp);
    return ok;
}

/* -- replay into raw fetcher sessions ------------------------------------ */

typedef struct raw {
    moq_session_t *c, *sv;
    moq_alloc_t    alloc;
    test_alloc_state_t alloc_state;
    uint64_t       now;
    uint64_t       bidi_ref[N_REQ];   /* the raw fetcher's request bidi per k (d18) */
    int            stops;             /* STOP_DATA actions polled, total      */
    uint64_t       last_stop_ref;
} raw_t;

/* Drain the raw fetcher's actions (its FETCH / cancel / STOP outputs are not
 * delivered anywhere); count STOP_DATA and remember OPEN_BIDI refs. */
static void
raw_drain_actions(raw_t *r, int k_for_bidi)
{
    moq_action_t acts[16];
    size_t n;
    while ((n = moq_session_poll_actions(r->c, acts, 16)) > 0) {
        for (size_t i = 0; i < n; i++) {
            if (acts[i].kind == MOQ_ACTION_STOP_DATA) {
                r->stops++;
                r->last_stop_ref = acts[i].u.stop_data.stream_ref._v;
            } else if (acts[i].kind == MOQ_ACTION_OPEN_BIDI_STREAM && k_for_bidi >= 0 &&
                       r->bidi_ref[k_for_bidi] == 0) {
                r->bidi_ref[k_for_bidi] = acts[i].u.open_bidi_stream.stream_ref._v;
            }
            moq_action_cleanup(&acts[i]);
        }
    }
}

/* Deliver one session's control output to the other (both control
 * topologies: the shared bidi channel and the uni control pair). */
static void
raw_pump_control(moq_session_t *from, moq_session_t *to, uint64_t now)
{
    moq_action_t acts[16];
    size_t n;
    while ((n = moq_session_poll_actions(from, acts, 16)) > 0) {
        for (size_t i = 0; i < n; i++) {
            const uint8_t *d = NULL;
            size_t len = 0;
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL) {
                d = acts[i].u.send_control.data; len = acts[i].u.send_control.len;
            } else if (acts[i].kind == MOQ_ACTION_OPEN_UNI_CONTROL) {
                d = acts[i].u.open_uni_control.data; len = acts[i].u.open_uni_control.len;
            } else if (acts[i].kind == MOQ_ACTION_SEND_UNI_CONTROL) {
                d = acts[i].u.send_uni_control.data; len = acts[i].u.send_uni_control.len;
            }
            if (d && len) (void)moq_session_on_control_bytes(to, d, len, now);
            moq_action_cleanup(&acts[i]);
        }
    }
}

/* A raw client/server pair of the given draft, established through their
 * own setup exchange; the client is the fetcher under test. */
static bool
raw_open(raw_t *r, moq_version_t version, uint32_t max_actions, uint32_t max_data_streams)
{
    memset(r, 0, sizeof(*r));
    r->alloc = test_allocator(&r->alloc_state);
    r->now = 1000;
    moq_session_cfg_t ccfg, scfg;
    moq_session_cfg_init_sized(&ccfg, sizeof(ccfg), &r->alloc, MOQ_PERSPECTIVE_CLIENT);
    ccfg.version = version;
    ccfg.send_request_capacity = true;
    ccfg.initial_request_capacity = 10;
    ccfg.max_actions = max_actions;
    ccfg.max_data_streams = max_data_streams;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), &r->alloc, MOQ_PERSPECTIVE_SERVER);
    scfg.version = version;
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 10;
    if (moq_session_create(&ccfg, r->now, &r->c) < 0) return false;
    if (moq_session_create(&scfg, r->now, &r->sv) < 0) return false;
    /* The client opens the exchange; a draft-18 server also opens its own
     * uni control stream at start, a draft-16 server answers on the client's
     * bidi channel. */
    if (moq_session_start(r->c, r->now) < 0) return false;
    if (version == MOQ_VERSION_DRAFT_18 && moq_session_start(r->sv, r->now) < 0) return false;
    for (int i = 0; i < 6; i++) {
        r->now += 10;
        raw_pump_control(r->c, r->sv, r->now);
        raw_pump_control(r->sv, r->c, r->now);
    }
    ev_obs_t o;
    memset(&o, 0, sizeof(o));
    drain(r->c, &o);
    drain(r->sv, &o);
    if (moq_session_state(r->c) != MOQ_SESS_ESTABLISHED ||
        moq_session_state(r->sv) != MOQ_SESS_ESTABLISHED) {
        printf("raw_open: states client=%d server=%d (version %d)\n", (int)moq_session_state(r->c),
               (int)moq_session_state(r->sv), (int)version);
        return false;
    }
    return true;
}

static void
raw_close(raw_t *r)
{
    if (r->c) moq_session_destroy(r->c);
    if (r->sv) moq_session_destroy(r->sv);
    MOQ_TEST_CHECK_EQ_INT(r->alloc_state.balance, 0);
}

/* Feed captured chunk i of response k; data streams are remapped to `ref`,
 * the request bidi to the raw fetcher's own. */
static moq_result_t
raw_feed(raw_t *r, const responses_t *resp, int k, int i, uint64_t ref)
{
    const chunk_t *c = &resp->per[k].c[i];
    r->now += 10;
    if (c->kind == CK_DATA) {
        return moq_session_on_data_bytes(r->c, moq_stream_ref_from_u64(ref), c->bytes, c->len, c->fin, r->now);
    }
    if (c->kind == CK_BIDI) {
        return moq_session_on_bidi_stream_bytes(r->c, moq_stream_ref_from_u64(r->bidi_ref[k]), c->bytes,
                                                c->len, c->fin, r->now);
    }
    return moq_session_on_control_bytes(r->c, c->bytes, c->len, r->now);
}

static int
rx_active_count(const moq_session_t *s)
{
    int n = 0;
    for (size_t i = 0; i < s->rx_cap; i++) if (s->rx_streams[i].active) n++;
    return n;
}

static int
rx_slot_of(const moq_session_t *s, uint64_t ref)
{
    for (size_t i = 0; i < s->rx_cap; i++)
        if (s->rx_streams[i].active && s->rx_streams[i].stream_ref._v == ref) return (int)i;
    return -1;
}

/* Feed every DATA chunk of response k on `ref`, checking the session stays
 * established after each; returns the number of chunks fed. */
static int
raw_feed_data(raw_t *r, const responses_t *resp, int k, uint64_t ref, bool include_fin_chunk)
{
    int fed = 0;
    for (int i = 0; i < resp->per[k].n; i++) {
        const chunk_t *c = &resp->per[k].c[i];
        if (c->kind != CK_DATA) continue;
        if (c->fin && !include_fin_chunk) {
            /* feed its bytes without the FIN flag */
            r->now += 10;
            moq_result_t rc = moq_session_on_data_bytes(r->c, moq_stream_ref_from_u64(ref), c->bytes, c->len,
                                                        false, r->now);
            MOQ_TEST_CHECK(rc == MOQ_OK);
        } else {
            MOQ_TEST_CHECK(raw_feed(r, resp, k, i, ref) == MOQ_OK);
        }
        fed++;
        MOQ_TEST_CHECK(moq_session_state(r->c) == MOQ_SESS_ESTABLISHED);
    }
    return fed;
}

static bool replay_only = false;   /* Run only the lossless replay schedule. */

static int
run(moq_version_t version)
{
    const char *lbl = version == MOQ_VERSION_DRAFT_16 ? "v16" : "v18";
    int before = failures;
    static responses_t resp;
    MOQ_TEST_CHECK(capture(version, &resp));
    int data_chunks0 = 0;
    for (int i = 0; i < resp.per[0].n; i++) if (resp.per[0].c[i].kind == CK_DATA) data_chunks0++;
    printf("captured %s: req0 inputs=%d data_chunks=%d data_stream=%llu bidi=%llu\n", lbl, resp.per[0].n,
           data_chunks0, (unsigned long long)resp.data_stream[0], (unsigned long long)resp.bidi_stream[0]);
    MOQ_TEST_CHECK(data_chunks0 >= 3);   /* header, objects, FIN carrier */
    uint64_t REF_A = 0x5E00, REF_B = 0x5E01, REF_C = 0x5E02, REF_D = 0x5E03;
    ev_obs_t o;
    if (replay_only) goto replay;

    /* S1: unbound late stream, every chunk including the FIN: exactly one STOP,
     * no event, no close, entry retained until the FIN frees it. */
    {
        raw_t r;
        MOQ_TEST_CHECK(raw_open(&r, version, 0, 0));
        moq_fetch_t fh;
        MOQ_TEST_CHECK(issue_fetch(r.c, r.now, &fh) == MOQ_OK);
        raw_drain_actions(&r, 0);
        MOQ_TEST_CHECK(moq_session_fetch_cancel(r.c, fh, r.now) == MOQ_OK);
        raw_drain_actions(&r, -1);
        r.stops = 0;
        int fed = 0;
        for (int i = 0; i < resp.per[0].n; i++) {
            if (resp.per[0].c[i].kind != CK_DATA) continue;
            MOQ_TEST_CHECK(raw_feed(&r, &resp, 0, i, REF_A) == MOQ_OK);
            fed++;
            MOQ_TEST_CHECK(moq_session_state(r.c) == MOQ_SESS_ESTABLISHED);
            raw_drain_actions(&r, -1);
            if (fed == 1) {
                MOQ_TEST_CHECK(r.stops == 1 && r.last_stop_ref == REF_A);
                MOQ_TEST_CHECK(rx_slot_of(r.c, REF_A) >= 0);   /* retained, stopped */
            }
        }
        MOQ_TEST_CHECK(r.stops == 1);
        memset(&o, 0, sizeof(o));
        drain(r.c, &o);
        MOQ_TEST_CHECK(o.ok == 0 && o.object == 0 && o.complete == 0 && o.reset == 0 && o.closed == 0);
        MOQ_TEST_CHECK(rx_active_count(r.c) == 0);   /* the FIN released it */
        MOQ_TEST_CHECK(moq_session_state(r.c) == MOQ_SESS_ESTABLISHED);
        printf("S1 %s: chunks=%d stops=%d active_after_fin=%d state=%d\n", lbl, fed, r.stops,
               rx_active_count(r.c), (int)moq_session_state(r.c));
        raw_close(&r);
    }
    /* S2: RESET retirement instead of FIN. */
    {
        raw_t r;
        MOQ_TEST_CHECK(raw_open(&r, version, 0, 0));
        moq_fetch_t fh;
        MOQ_TEST_CHECK(issue_fetch(r.c, r.now, &fh) == MOQ_OK);
        raw_drain_actions(&r, 0);
        MOQ_TEST_CHECK(moq_session_fetch_cancel(r.c, fh, r.now) == MOQ_OK);
        raw_drain_actions(&r, -1);
        r.stops = 0;
        raw_feed_data(&r, &resp, 0, REF_A, false);
        raw_drain_actions(&r, -1);
        MOQ_TEST_CHECK(r.stops == 1 && rx_slot_of(r.c, REF_A) >= 0);
        r.now += 10;
        MOQ_TEST_CHECK(moq_session_on_data_reset(r.c, moq_stream_ref_from_u64(REF_A), 0x1, r.now) == MOQ_OK);
        MOQ_TEST_CHECK(rx_active_count(r.c) == 0);
        memset(&o, 0, sizeof(o));
        drain(r.c, &o);
        MOQ_TEST_CHECK(o.reset == 0 && o.closed == 0 && moq_session_state(r.c) == MOQ_SESS_ESTABLISHED);
        raw_close(&r);
    }
    /* S3: the STOP is refused by a full action queue first (3 deep): the
     * entry waits in NEED_STOP, the same bytes are re-fed (bridge retry),
     * then exactly one STOP is queued; later chunks are consumed; FIN frees. */
    {
        raw_t r;
        MOQ_TEST_CHECK(raw_open(&r, version, 3, 0));
        moq_fetch_t fh;
        MOQ_TEST_CHECK(issue_fetch(r.c, r.now, &fh) == MOQ_OK);
        raw_drain_actions(&r, 0);
        MOQ_TEST_CHECK(moq_session_fetch_cancel(r.c, fh, r.now) == MOQ_OK);
        raw_drain_actions(&r, -1);
        /* Fill the queue with distinct TRACK_STATUS requests. */
        int filled = 0;
        for (int i = 0; i < 8; i++) {
            char nm[8];
            (void)snprintf(nm, sizeof(nm), "s%d", i);
            moq_bytes_t nsp[1] = { MOQ_BYTES_LITERAL("live") };
            moq_track_status_cfg_t tsc;
            moq_track_status_cfg_init(&tsc);
            tsc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 1 };
            tsc.track_name = (moq_bytes_t){ (const uint8_t *)nm, strlen(nm) };
            moq_track_status_handle_t tsh;
            if (moq_session_track_status(r.c, &tsc, r.now, &tsh) != MOQ_OK) break;
            filled++;
        }
        MOQ_TEST_CHECK(filled == 3);
        int first = -1;
        for (int i = 0; i < resp.per[0].n; i++) if (resp.per[0].c[i].kind == CK_DATA) { first = i; break; }
        MOQ_TEST_CHECK(first >= 0);
        r.stops = 0;
        MOQ_TEST_CHECK(raw_feed(&r, &resp, 0, first, REF_A) == MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK(raw_feed(&r, &resp, 0, first, REF_A) == MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK(rx_slot_of(r.c, REF_A) >= 0 &&
                       r.c->rx_streams[rx_slot_of(r.c, REF_A)].parse_state == MOQ_RX_NEED_STOP);
        /* Draining the queue returns action capacity: the owed STOP is issued
         * right then, with no further peer input, and polled by this same
         * drain. */
        raw_drain_actions(&r, -1);
        MOQ_TEST_CHECK(r.stops == 1 && r.last_stop_ref == REF_A);
        MOQ_TEST_CHECK(rx_slot_of(r.c, REF_A) >= 0 &&
                       r.c->rx_streams[rx_slot_of(r.c, REF_A)].parse_state == MOQ_RX_STOPPED);
        /* The bridge's retry of the same bytes is consumed by the stopped entry. */
        MOQ_TEST_CHECK(raw_feed(&r, &resp, 0, first, REF_A) == MOQ_OK);
        raw_drain_actions(&r, -1);
        MOQ_TEST_CHECK(r.stops == 1 && r.last_stop_ref == REF_A);
        for (int i = first + 1; i < resp.per[0].n; i++) {
            if (resp.per[0].c[i].kind != CK_DATA) continue;
            MOQ_TEST_CHECK(raw_feed(&r, &resp, 0, i, REF_A) == MOQ_OK);
            MOQ_TEST_CHECK(moq_session_state(r.c) == MOQ_SESS_ESTABLISHED);
        }
        raw_drain_actions(&r, -1);
        MOQ_TEST_CHECK(r.stops == 1 && rx_active_count(r.c) == 0);
        memset(&o, 0, sizeof(o));
        drain(r.c, &o);
        MOQ_TEST_CHECK(o.closed == 0);
        raw_close(&r);
    }
    /* S4: two owned stopped streams exhaust admission. RESET cancels a
     * third caller-held response, then an unrelated live fetch completes on
     * released capacity. Bytes after its genuine FIN remain invalid. */
    {
        raw_t r;
        MOQ_TEST_CHECK(raw_open(&r, version, 0, 2));
        moq_fetch_t fh[N_REQ];
        for (int k = 0; k < N_REQ; k++) {
            MOQ_TEST_CHECK(issue_fetch(r.c, r.now, &fh[k]) == MOQ_OK);
            raw_drain_actions(&r, k);
        }
        for (int k = 0; k < 3; k++) {
            MOQ_TEST_CHECK(moq_session_fetch_cancel(r.c, fh[k], r.now) == MOQ_OK);
        }
        raw_drain_actions(&r, -1);
        r.stops = 0;
        raw_feed_data(&r, &resp, 0, REF_A, false);
        raw_feed_data(&r, &resp, 1, REF_B, false);
        raw_drain_actions(&r, -1);
        MOQ_TEST_CHECK(r.stops == 2 && rx_active_count(r.c) == 2);
        /* A refused first chunk stays caller-owned; RESET cancels its replay. */
        int first = -1;
        for (int i = 0; i < resp.per[2].n; i++)
            if (resp.per[2].c[i].kind == CK_DATA && resp.per[2].c[i].len > 0) { first = i; break; }
        MOQ_TEST_CHECK(first >= 0);
        MOQ_TEST_CHECK(raw_feed(&r, &resp, 2, first, REF_C) == MOQ_ERR_WOULD_BLOCK);
        raw_drain_actions(&r, -1);
        MOQ_TEST_CHECK(r.stops == 2 && rx_active_count(r.c) == 2);
        MOQ_TEST_CHECK(!moq_session_has_transport_stream(r.c, moq_stream_ref_from_u64(REF_C)));
        r.now += 10;
        MOQ_TEST_CHECK(moq_session_on_data_reset(r.c, moq_stream_ref_from_u64(REF_C), 0x1, r.now) == MOQ_OK);
        MOQ_TEST_CHECK(!moq_session_has_transport_stream(r.c, moq_stream_ref_from_u64(REF_C)));
        MOQ_TEST_CHECK(moq_session_on_data_reset(r.c, moq_stream_ref_from_u64(REF_A), 0x1, r.now) == MOQ_OK);
        MOQ_TEST_CHECK(rx_active_count(r.c) == 1);
        /* Request 3 is live: its whole captured response, remapped. */
        memset(&o, 0, sizeof(o));
        drain(r.c, &o);
        for (int i = 0; i < resp.per[3].n; i++) {
            moq_result_t rc = raw_feed(&r, &resp, 3, i, REF_D);
            MOQ_TEST_CHECK(rc == MOQ_OK);
            drain(r.c, &o);
        }
        MOQ_TEST_CHECK(o.ok == 1 && o.object == 2 && o.complete == 1 && o.closed == 0);
        MOQ_TEST_CHECK(rx_active_count(r.c) == 1);   /* only the stopped REF_B remains */
        MOQ_TEST_CHECK(moq_session_state(r.c) == MOQ_SESS_ESTABLISHED);
        /* Data after a real FIN stays a violation. */
        uint8_t late = 0x00;
        r.now += 10;
        (void)moq_session_on_data_bytes(r.c, moq_stream_ref_from_u64(REF_D), &late, 1, false, r.now);
        drain(r.c, &o);
        MOQ_TEST_CHECK(moq_session_state(r.c) == MOQ_SESS_CLOSED && o.closed == 1);
        printf("S4 %s: close reason after real FIN: \"%s\"\n", lbl, o.close_reason);
        raw_close(&r);
    }
    /* S5: a bound stream (OK + first object delivered) then cancel, then the
     * publisher's remaining chunks: dropped, no close, no event. */
    {
        raw_t r;
        MOQ_TEST_CHECK(raw_open(&r, version, 0, 0));
        moq_fetch_t fh;
        MOQ_TEST_CHECK(issue_fetch(r.c, r.now, &fh) == MOQ_OK);
        raw_drain_actions(&r, 0);
        memset(&o, 0, sizeof(o));
        int i = 0;
        for (; i < resp.per[0].n && o.object == 0; i++) {
            MOQ_TEST_CHECK(raw_feed(&r, &resp, 0, i, REF_A) == MOQ_OK);
            drain(r.c, &o);
        }
        MOQ_TEST_CHECK(o.ok == 1 && o.object == 1);
        MOQ_TEST_CHECK(moq_session_fetch_cancel(r.c, fh, r.now) == MOQ_OK);
        raw_drain_actions(&r, -1);
        for (; i < resp.per[0].n; i++) {
            if (resp.per[0].c[i].kind != CK_DATA) continue;
            MOQ_TEST_CHECK(raw_feed(&r, &resp, 0, i, REF_A) == MOQ_OK);
            MOQ_TEST_CHECK(moq_session_state(r.c) == MOQ_SESS_ESTABLISHED);
        }
        drain(r.c, &o);
        MOQ_TEST_CHECK(o.object == 1 && o.complete == 0 && o.closed == 0 && rx_active_count(r.c) == 0);
        raw_close(&r);
    }
    /* S6: a genuinely malformed new stream is still a protocol violation. */
    {
        raw_t r;
        MOQ_TEST_CHECK(raw_open(&r, version, 0, 0));
        uint8_t junk[3] = { 0x3f, 0x00, 0x00 };   /* complete, unknown stream type on both drafts */
        r.now += 10;
        (void)moq_session_on_data_bytes(r.c, moq_stream_ref_from_u64(0x77), junk, sizeof(junk), false, r.now);
        memset(&o, 0, sizeof(o));
        drain(r.c, &o);
        MOQ_TEST_CHECK(moq_session_state(r.c) == MOQ_SESS_CLOSED && o.closed == 1);
        printf("S6 %s: malformed new stream close reason: \"%s\"\n", lbl, o.close_reason);
        raw_close(&r);
    }
    /* S7: retain the first chunk while two stopped entries hold capacity.
     * Replay from the header when one entry frees, then stop the cancelled
     * response exactly once and consume its tail through FIN. */
    {
        raw_t r;
        MOQ_TEST_CHECK(raw_open(&r, version, 0, 2));
        moq_fetch_t fh[N_REQ];
        for (int k = 0; k < N_REQ; k++) {
            MOQ_TEST_CHECK(issue_fetch(r.c, r.now, &fh[k]) == MOQ_OK);
            raw_drain_actions(&r, k);
        }
        for (int k = 0; k < 3; k++)
            MOQ_TEST_CHECK(moq_session_fetch_cancel(r.c, fh[k], r.now) == MOQ_OK);
        raw_drain_actions(&r, -1);
        r.stops = 0;
        raw_feed_data(&r, &resp, 0, REF_A, false);
        raw_feed_data(&r, &resp, 1, REF_B, false);
        raw_drain_actions(&r, -1);
        MOQ_TEST_CHECK(r.stops == 2 && rx_active_count(r.c) == 2);
        int first = -1, second = -1;
        for (int i = 0; i < resp.per[2].n; i++) {
            if (resp.per[2].c[i].kind != CK_DATA || resp.per[2].c[i].len == 0) continue;
            if (first < 0) first = i;
            else { second = i; break; }
        }
        MOQ_TEST_CHECK(first >= 0 && second >= 0);
        MOQ_TEST_CHECK(raw_feed(&r, &resp, 2, first, REF_C) == MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK(raw_feed(&r, &resp, 2, first, REF_C) == MOQ_ERR_WOULD_BLOCK);
        raw_drain_actions(&r, -1);
        MOQ_TEST_CHECK(r.stops == 2 && rx_active_count(r.c) == 2);
        MOQ_TEST_CHECK(!moq_session_has_transport_stream(r.c, moq_stream_ref_from_u64(REF_C)));
        r.now += 10;
        MOQ_TEST_CHECK(moq_session_on_data_reset(r.c, moq_stream_ref_from_u64(REF_A), 0x1, r.now) == MOQ_OK);
        MOQ_TEST_CHECK(rx_active_count(r.c) == 1);
        bool fin_seen = false;
        for (int i = first; i < resp.per[2].n; i++) {
            if (resp.per[2].c[i].kind != CK_DATA) continue;
            MOQ_TEST_CHECK(raw_feed(&r, &resp, 2, i, REF_C) == MOQ_OK);
            MOQ_TEST_CHECK(moq_session_state(r.c) == MOQ_SESS_ESTABLISHED);
            MOQ_TEST_CHECK(rx_active_count(r.c) == (resp.per[2].c[i].fin ? 1 : 2));
            if (resp.per[2].c[i].fin) fin_seen = true;
        }
        MOQ_TEST_CHECK(fin_seen);
        raw_drain_actions(&r, -1);
        MOQ_TEST_CHECK(r.stops == 3);
        MOQ_TEST_CHECK(!moq_session_has_transport_stream(r.c, moq_stream_ref_from_u64(REF_C)));
        memset(&o, 0, sizeof(o));
        drain(r.c, &o);
        MOQ_TEST_CHECK(o.closed == 0 && o.ok == 0 && o.object == 0 && o.complete == 0);
        printf("S7 %s: state=%d closed=%d reason=\"%s\" stops=%d active=%d\n", lbl, (int)moq_session_state(r.c),
               o.closed, o.close_reason, r.stops, rx_active_count(r.c));
        MOQ_TEST_CHECK(moq_session_state(r.c) == MOQ_SESS_ESTABLISHED);
        /* The freed slot still serves a live request. */
        for (int i = 0; i < resp.per[3].n; i++) {
            MOQ_TEST_CHECK(raw_feed(&r, &resp, 3, i, REF_D) == MOQ_OK);
            drain(r.c, &o);
        }
        MOQ_TEST_CHECK(o.ok == 1 && o.object == 2 && o.complete == 1 && o.closed == 0);
        MOQ_TEST_CHECK(rx_active_count(r.c) == 1);   /* only the stopped REF_B remains */
        raw_close(&r);
    }
    /* S8: more refused identities than the former stopped-record capacity
     * remain entirely caller-owned. RESET cancels one; freeing an actual
     * receive entry permits exact replay of a live response. */
    {
        raw_t r;
        MOQ_TEST_CHECK(raw_open(&r, version, 0, 2));
        moq_fetch_t fh[3];
        for (int k = 0; k < 3; k++) {
            MOQ_TEST_CHECK(issue_fetch(r.c, r.now, &fh[k]) == MOQ_OK);
            raw_drain_actions(&r, k);
            if (k < 2) MOQ_TEST_CHECK(moq_session_fetch_cancel(r.c, fh[k], r.now) == MOQ_OK);
        }
        raw_drain_actions(&r, -1);
        r.stops = 0;
        raw_feed_data(&r, &resp, 0, REF_A, false);
        raw_feed_data(&r, &resp, 1, REF_B, false);
        raw_drain_actions(&r, -1);
        MOQ_TEST_CHECK(r.stops == 2 && rx_active_count(r.c) == 2);
        int first = -1;
        for (int i = 0; i < resp.per[2].n; i++)
            if (resp.per[2].c[i].kind == CK_DATA && resp.per[2].c[i].len > 0) { first = i; break; }
        MOQ_TEST_CHECK(first >= 0);
        const uint64_t BASE = 0x6E00, OVER = BASE + 16;
        for (uint64_t ref = BASE; ref <= OVER; ref++) {
            MOQ_TEST_CHECK(raw_feed(&r, &resp, 2, first, ref) == MOQ_ERR_WOULD_BLOCK);
            MOQ_TEST_CHECK(!moq_session_has_transport_stream(r.c, moq_stream_ref_from_u64(ref)));
        }
        raw_drain_actions(&r, -1);
        MOQ_TEST_CHECK(r.stops == 2 && r.c->rx_stopped_count == 0);
        MOQ_TEST_CHECK(moq_session_on_data_reset(r.c, moq_stream_ref_from_u64(BASE), 1, ++r.now) == MOQ_OK);
        MOQ_TEST_CHECK(raw_feed(&r, &resp, 2, first, OVER) == MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK(moq_session_on_data_reset(r.c, moq_stream_ref_from_u64(REF_A), 1, ++r.now) == MOQ_OK);
        memset(&o, 0, sizeof(o));
        for (int i = 0; i < resp.per[2].n; i++) {
            MOQ_TEST_CHECK(raw_feed(&r, &resp, 2, i, OVER) == MOQ_OK);
            drain(r.c, &o);
        }
        MOQ_TEST_CHECK(o.ok == 1 && o.object == 2 && o.complete == 1 && o.closed == 0);
        MOQ_TEST_CHECK(moq_session_fetch_cancel(r.c, fh[2], ++r.now) == MOQ_ERR_STALE_HANDLE);
        MOQ_TEST_CHECK(rx_active_count(r.c) == 1);
        raw_drain_actions(&r, -1);
        MOQ_TEST_CHECK(r.stops == 2);
        raw_close(&r);
    }
    /* S9: a FIN delivered while the STOP is still refused is not lost. The
     * queue is 3 deep and full; (a) the first chunk is refused, then the same
     * bytes re-driven with the FIN are refused again; (b) the first chunk is
     * refused, then a FIN-only delivery is refused. In both, draining the
     * actions issues exactly one STOP and the retained FIN releases the entry
     * at once (the transport FIN is noted; completion counting for a bound
     * stream is pinned in session_subscribe) and an empty re-drive
     * afterwards is a no-op. */
    for (int variant = 0; variant < 2; variant++) {
        raw_t r;
        MOQ_TEST_CHECK(raw_open(&r, version, 3, 0));
        moq_fetch_t fh;
        MOQ_TEST_CHECK(issue_fetch(r.c, r.now, &fh) == MOQ_OK);
        raw_drain_actions(&r, 0);
        MOQ_TEST_CHECK(moq_session_fetch_cancel(r.c, fh, r.now) == MOQ_OK);
        raw_drain_actions(&r, -1);
        int filled = 0;
        for (int i = 0; i < 8; i++) {
            char nm[8];
            (void)snprintf(nm, sizeof(nm), "f%d", i);
            moq_bytes_t nsp[1] = { MOQ_BYTES_LITERAL("live") };
            moq_track_status_cfg_t tsc;
            moq_track_status_cfg_init(&tsc);
            tsc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 1 };
            tsc.track_name = (moq_bytes_t){ (const uint8_t *)nm, strlen(nm) };
            moq_track_status_handle_t tsh;
            if (moq_session_track_status(r.c, &tsc, r.now, &tsh) != MOQ_OK) break;
            filled++;
        }
        MOQ_TEST_CHECK(filled == 3);
        int first = -1;
        for (int i = 0; i < resp.per[0].n; i++) if (resp.per[0].c[i].kind == CK_DATA) { first = i; break; }
        MOQ_TEST_CHECK(first >= 0);
        const chunk_t *c0 = &resp.per[0].c[first];
        size_t fin_count_before = r.c->rx_fin_count;
        r.stops = 0;
        MOQ_TEST_CHECK(raw_feed(&r, &resp, 0, first, REF_A) == MOQ_ERR_WOULD_BLOCK);
        r.now += 10;
        if (variant == 0) {
            MOQ_TEST_CHECK(moq_session_on_data_bytes(r.c, moq_stream_ref_from_u64(REF_A), c0->bytes, c0->len,
                                                     true, r.now) == MOQ_ERR_WOULD_BLOCK);
        } else {
            MOQ_TEST_CHECK(moq_session_on_data_bytes(r.c, moq_stream_ref_from_u64(REF_A), NULL, 0, true,
                                                     r.now) == MOQ_ERR_WOULD_BLOCK);
        }
        int slot = rx_slot_of(r.c, REF_A);
        MOQ_TEST_CHECK(slot >= 0 && r.c->rx_streams[slot].parse_state == MOQ_RX_NEED_STOP);
        MOQ_TEST_CHECK(slot >= 0 && r.c->rx_streams[slot].pending_fin);
        raw_drain_actions(&r, -1);
        MOQ_TEST_CHECK(r.stops == 1 && r.last_stop_ref == REF_A);
        MOQ_TEST_CHECK(rx_active_count(r.c) == 0);
        MOQ_TEST_CHECK(r.c->rx_fin_count == fin_count_before + 1);   /* the transport FIN is noted */
        r.now += 10;
        MOQ_TEST_CHECK(moq_session_on_data_bytes(r.c, moq_stream_ref_from_u64(REF_A), NULL, 0, false, r.now) == MOQ_OK);
        raw_drain_actions(&r, -1);
        MOQ_TEST_CHECK(r.stops == 1 && rx_active_count(r.c) == 0);
        memset(&o, 0, sizeof(o));
        drain(r.c, &o);
        MOQ_TEST_CHECK(o.closed == 0 && o.ok == 0 && o.object == 0 && o.complete == 0);
        MOQ_TEST_CHECK(moq_session_state(r.c) == MOQ_SESS_ESTABLISHED);
        printf("S9%c %s: stops=%d active=%d fin_recorded=%d\n", variant ? 'b' : 'a', lbl, r.stops,
               rx_active_count(r.c), (int)(r.c->rx_fin_count - fin_count_before));
        raw_close(&r);
    }
    /* S10: owned STOPPED and deferred STOP lifetimes, plus first-header and
     * header+FIN refusal/replay. Only an accepted FIN marks the stream finished;
     * later nonempty input on that identity is always rejected. */
    for (int variant = 0; variant < 4; variant++) {
        raw_t r;
        uint32_t max_actions = variant == 2 ? 3 : 0;
        uint32_t pool = (variant == 1 || variant == 3) ? 2 : 0;
        MOQ_TEST_CHECK(raw_open(&r, version, max_actions, pool));
        moq_fetch_t fh[3];
        int n_req = pool ? 3 : 1;
        for (int k = 0; k < n_req; k++) {
            MOQ_TEST_CHECK(issue_fetch(r.c, r.now, &fh[k]) == MOQ_OK);
            raw_drain_actions(&r, k);
            MOQ_TEST_CHECK(moq_session_fetch_cancel(r.c, fh[k], r.now) == MOQ_OK);
        }
        raw_drain_actions(&r, -1);
        r.stops = 0;
        uint64_t ref = REF_A;
        int first = -1;
        int response_index = pool ? 2 : 0;
        for (int i = 0; i < resp.per[response_index].n; i++)
            if (resp.per[response_index].c[i].kind == CK_DATA && resp.per[response_index].c[i].len > 0) { first = i; break; }
        MOQ_TEST_CHECK(first >= 0);
        const chunk_t *c0 = &resp.per[response_index].c[first];
        size_t fin_before = r.c->rx_fin_count;
        if (pool) {
            raw_feed_data(&r, &resp, 0, REF_A, false);
            raw_feed_data(&r, &resp, 1, REF_B, false);
            raw_drain_actions(&r, -1);
            MOQ_TEST_CHECK(r.stops == 2 && rx_active_count(r.c) == 2);
            ref = REF_C;
        }
        r.now += 10;
        switch (variant) {
        case 0:   /* STOPPED entry, then FIN-only */
            MOQ_TEST_CHECK(raw_feed(&r, &resp, 0, first, ref) == MOQ_OK);
            raw_drain_actions(&r, -1);
            MOQ_TEST_CHECK(r.stops == 1 && rx_slot_of(r.c, ref) >= 0);
            MOQ_TEST_CHECK(moq_session_on_data_bytes(r.c, moq_stream_ref_from_u64(ref), NULL, 0, true, r.now) == MOQ_OK);
            break;
        case 1:   /* refused header, then exact replay and FIN-only */
            MOQ_TEST_CHECK(raw_feed(&r, &resp, 2, first, ref) == MOQ_ERR_WOULD_BLOCK);
            MOQ_TEST_CHECK(moq_session_on_data_reset(r.c, moq_stream_ref_from_u64(REF_A), 1, ++r.now) == MOQ_OK);
            MOQ_TEST_CHECK(raw_feed(&r, &resp, 2, first, ref) == MOQ_OK);
            raw_drain_actions(&r, -1);
            MOQ_TEST_CHECK(r.stops == 3 && moq_session_has_transport_stream(r.c, moq_stream_ref_from_u64(ref)));
            MOQ_TEST_CHECK(moq_session_on_data_bytes(r.c, moq_stream_ref_from_u64(ref), NULL, 0, true, r.now) == MOQ_OK);
            break;
        case 2: { /* STOP refused, FIN latched, STOP committed by the drain */
            int filled = 0;
            for (int i = 0; i < 8; i++) {
                char nm[8];
                (void)snprintf(nm, sizeof(nm), "g%d", i);
                moq_bytes_t nsp[1] = { MOQ_BYTES_LITERAL("live") };
                moq_track_status_cfg_t tsc;
                moq_track_status_cfg_init(&tsc);
                tsc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 1 };
                tsc.track_name = (moq_bytes_t){ (const uint8_t *)nm, strlen(nm) };
                moq_track_status_handle_t tsh;
                if (moq_session_track_status(r.c, &tsc, r.now, &tsh) != MOQ_OK) break;
                filled++;
            }
            MOQ_TEST_CHECK(filled == 3);
            MOQ_TEST_CHECK(raw_feed(&r, &resp, 0, first, ref) == MOQ_ERR_WOULD_BLOCK);
            MOQ_TEST_CHECK(moq_session_on_data_bytes(r.c, moq_stream_ref_from_u64(ref), NULL, 0, true, r.now) ==
                           MOQ_ERR_WOULD_BLOCK);
            raw_drain_actions(&r, -1);
            MOQ_TEST_CHECK(r.stops == 1);
            break;
        }
        default:  /* FIN with refused delivery must be replayed too */
            MOQ_TEST_CHECK(moq_session_on_data_bytes(r.c, moq_stream_ref_from_u64(ref), c0->bytes, c0->len, true,
                                                     r.now) == MOQ_ERR_WOULD_BLOCK);
            MOQ_TEST_CHECK(r.c->rx_fin_count == fin_before);
            MOQ_TEST_CHECK(moq_session_on_data_reset(r.c, moq_stream_ref_from_u64(REF_A), 1, ++r.now) == MOQ_OK);
            MOQ_TEST_CHECK(moq_session_on_data_bytes(r.c, moq_stream_ref_from_u64(ref), c0->bytes, c0->len, true,
                                                     r.now) == MOQ_OK);
            raw_drain_actions(&r, -1);
            MOQ_TEST_CHECK(r.stops == 3);
            break;
        }
        MOQ_TEST_CHECK(!moq_session_has_transport_stream(r.c, moq_stream_ref_from_u64(ref)));
        MOQ_TEST_CHECK(rx_active_count(r.c) == (pool ? 1 : 0));
        MOQ_TEST_CHECK(r.c->rx_fin_count == fin_before + 1);   /* the transport FIN is remembered */
        MOQ_TEST_CHECK(moq_session_state(r.c) == MOQ_SESS_ESTABLISHED);
        /* Non-empty bytes after that FIN: the existing policy. */
        int stops_before = r.stops;
        r.now += 10;
        (void)moq_session_on_data_bytes(r.c, moq_stream_ref_from_u64(ref), c0->bytes, c0->len, false, r.now);
        raw_drain_actions(&r, -1);
        memset(&o, 0, sizeof(o));
        drain(r.c, &o);
        printf("S10%c %s: close reason after stopped-stream FIN: \"%s\" extra_stops=%d\n", 'a' + variant, lbl,
               o.close_reason, r.stops - stops_before);
        MOQ_TEST_CHECK(moq_session_state(r.c) == MOQ_SESS_CLOSED && o.closed == 1);
        MOQ_TEST_CHECK(strcmp(o.close_reason, "data after FIN") == 0);
        MOQ_TEST_CHECK(r.stops == stops_before);
        MOQ_TEST_CHECK(o.ok == 0 && o.object == 0 && o.complete == 0);
        raw_close(&r);
    }
    if (failures == before) printf("PASS: session_rx_stopped_stream %s\n", lbl);
    return failures - before;

replay:
    /* S11: a live response first arrives without capacity. Retain its exact
     * first chunk, replay all data after capacity returns, then deliver the
     * late FETCH_OK. Two exact objects and one COMPLETE, never local failure. */
    {
        raw_t r;
        MOQ_TEST_CHECK(raw_open(&r, version, 0, 2));
        moq_fetch_t fh[3];
        for (int k = 0; k < 3; k++) {
            MOQ_TEST_CHECK(issue_fetch(r.c, r.now, &fh[k]) == MOQ_OK);
            raw_drain_actions(&r, k);
        }
        for (int k = 0; k < 2; k++) MOQ_TEST_CHECK(moq_session_fetch_cancel(r.c, fh[k], r.now) == MOQ_OK);
        raw_drain_actions(&r, -1);
        r.stops = 0;
        raw_feed_data(&r, &resp, 0, REF_A, false);
        raw_feed_data(&r, &resp, 1, REF_B, false);
        raw_drain_actions(&r, -1);
        MOQ_TEST_CHECK(r.stops == 2 && rx_active_count(r.c) == 2);
        /* Request 2 is live: its data stream first (header + first object). */
        int first = -1, second = -1;
        for (int i = 0; i < resp.per[2].n; i++) {
            if (resp.per[2].c[i].kind != CK_DATA || resp.per[2].c[i].len == 0) continue;
            if (first < 0) first = i;
            else { second = i; break; }
        }
        MOQ_TEST_CHECK(first >= 0 && second >= 0);
        MOQ_TEST_CHECK(raw_feed(&r, &resp, 2, first, REF_C) == MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK(raw_feed(&r, &resp, 2, first, REF_C) == MOQ_ERR_WOULD_BLOCK);
        raw_drain_actions(&r, -1);
        memset(&o, 0, sizeof(o));
        drain(r.c, &o);
        printf("S11 %s: after no-entry live response: stops=%d owned=%d reset=%d complete=%d error=%d\n", lbl, r.stops,
               (int)moq_session_has_transport_stream(r.c, moq_stream_ref_from_u64(REF_C)), o.reset, o.complete,
               o.error);
        MOQ_TEST_CHECK(r.stops == 2);
        MOQ_TEST_CHECK(!moq_session_has_transport_stream(r.c, moq_stream_ref_from_u64(REF_C)));
        MOQ_TEST_CHECK(o.reset == 0 && o.complete == 0 && o.error == 0 && o.closed == 0);
        /* Release capacity and replay all data before the control response. */
        MOQ_TEST_CHECK(moq_session_on_data_reset(r.c, moq_stream_ref_from_u64(REF_A), 1, ++r.now) == MOQ_OK);
        raw_feed_data(&r, &resp, 2, REF_C, true);
        drain(r.c, &o);
        MOQ_TEST_CHECK(o.object == 2 && o.ok == 0 && o.complete == 0);
        /* Late FETCH_OK settles the already received data exactly once. */
        for (int i = 0; i < resp.per[2].n; i++) {
            if (resp.per[2].c[i].kind == CK_DATA) continue;
            MOQ_TEST_CHECK(raw_feed(&r, &resp, 2, i, REF_C) >= 0);
        }
        drain(r.c, &o);
        MOQ_TEST_CHECK(moq_session_state(r.c) == MOQ_SESS_ESTABLISHED && o.closed == 0);
        MOQ_TEST_CHECK(o.ok == 1 && o.reset == 0 && o.complete == 1);
        MOQ_TEST_CHECK(moq_session_fetch_cancel(r.c, fh[2], r.now) == MOQ_ERR_STALE_HANDLE);
        /* Duplicate RESET of the released owner cannot disturb settlement. */
        r.now += 10;
        MOQ_TEST_CHECK(moq_session_on_data_reset(r.c, moq_stream_ref_from_u64(REF_A), 0x1, r.now) == MOQ_OK);
        MOQ_TEST_CHECK(rx_active_count(r.c) == 1);
        raw_drain_actions(&r, -1);
        MOQ_TEST_CHECK(r.stops == 2);
        MOQ_TEST_CHECK(!moq_session_has_transport_stream(r.c, moq_stream_ref_from_u64(REF_C)));   /* FIN released it */
        drain(r.c, &o);
        MOQ_TEST_CHECK(o.reset == 0 && o.complete == 1 && o.closed == 0);
        raw_close(&r);
    }
    if (failures == before) printf("PASS: session_rx_stopped_stream_replay %s\n", lbl);
    return failures - before;
}

int main(int argc, char **argv)
{
    replay_only = argc > 1 && (strcmp(argv[1], "--replay") == 0 || strcmp(argv[1], "--attribution-red") == 0);
    (void)run(MOQ_VERSION_DRAFT_16);
    (void)run(MOQ_VERSION_DRAFT_18);
    if (failures == 0)
        MOQ_TEST_PASS(replay_only ? "session_rx_stopped_stream_replay" : "session_rx_stopped_stream");
    return failures ? 1 : 0;
}
