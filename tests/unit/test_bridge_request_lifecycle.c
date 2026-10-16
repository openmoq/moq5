/*
 * Request-stream lifecycle of FETCH and TRACK_STATUS through two real
 * transport bridges over fake endpoints.
 *
 * Draft-18 carries each request on its own bidirectional stream (§3.3). A
 * finished FETCH (§5.2, §10.12.3) and a TRACK_STATUS (§10.14) must leave that
 * stream closed in both directions, with each side's half closed by FIN, and
 * neither side may treat the other's ordinary FIN as a new request or a
 * protocol violation. Draft-16 carries the same requests on the control
 * stream, so the draft-16 rows are controls: identical application outcomes
 * with no request streams at all.
 *
 * The wire between the two bridges (support/bridge_wire_pair.h) is an ordered
 * per-direction queue. A row can hold one stream's operations (QUIC gives no
 * cross-stream ordering), drop them, or inject a peer RESET/STOP/FIN, so the
 * rows cover data-FIN versus request-FIN reordering, late peer terminals,
 * action / event / transport backpressure and repeated reuse of small pools,
 * not just the in-order replay.
 *
 * Every row ends with the same quiescence oracle: both sessions established,
 * no bridge terminal, each bridge back to its post-setup stream count, no
 * drain reference left, and the FETCH / TRACK_STATUS pools empty on both
 * sides.
 */

#include <stdbool.h>

static int failures = 0;

#include "../support/bridge_wire_pair.h"

/* -- FETCH helpers ----------------------------------------------------------- */

static moq_bytes_t g_ns_part[1] = { { (const uint8_t *)"live", 4 } };

static moq_result_t issue_fetch(rl_pair_t *p, moq_fetch_t *out)
{
    moq_fetch_cfg_t fc;
    moq_fetch_cfg_init(&fc);
    fc.track_namespace = (moq_namespace_t){ .parts = g_ns_part, .count = 1 };
    fc.track_name = MOQ_BYTES_LITERAL("t");
    fc.end_group = 0;
    fc.end_object = 3;
    return moq_session_fetch(p->c, &fc, p->now, out);
}

static moq_result_t write_object(rl_pair_t *p, moq_fetch_t fh, uint64_t oid)
{
    static const uint8_t body[] = { 'o', 'b', 'j' };
    moq_rcbuf_t *pl = NULL;
    if (moq_rcbuf_create(&p->salloc, body, sizeof(body), &pl) != MOQ_OK)
        return MOQ_ERR_NOMEM;
    moq_fetch_object_cfg_t oc;
    moq_fetch_object_cfg_init(&oc);
    oc.group_id = 0;
    oc.object_id = oid;
    oc.publisher_priority = 100;
    oc.payload = pl;
    moq_result_t rc = moq_session_write_fetch_object(p->s, fh, &oc, p->now);
    moq_rcbuf_decref(pl);
    return rc;
}

/* Issue one FETCH and deliver it; returns the publisher's handle. */
static moq_fetch_t request_fetch(rl_pair_t *p, uint64_t *req_sid)
{
    moq_fetch_t fh;
    MOQ_TEST_CHECK_EQ_INT(issue_fetch(p, &fh), MOQ_OK);
    if (req_sid) *req_sid = p->cep.next_bidi_id;   /* the bidi the bridge opens */
    shuttle(p);
    obs_t so;
    memset(&so, 0, sizeof(so));
    drain(p->s, &so);
    MOQ_TEST_CHECK_EQ_INT(so.fetch_request, 1);
    return so.fetch;
}

static moq_result_t accept_fetch(rl_pair_t *p, moq_fetch_t sfh, bool empty)
{
    moq_accept_fetch_cfg_t ac;
    moq_accept_fetch_cfg_init(&ac);
    ac.end_group = 0;
    ac.end_object = 3;
    ac.empty = empty;
    return moq_session_accept_fetch(p->s, sfh, &ac, p->now);
}

/* Publisher side of a successful FETCH: accept, two objects, end. */
static void serve_fetch(rl_pair_t *p, moq_fetch_t sfh, bool empty)
{
    MOQ_TEST_CHECK_EQ_INT(accept_fetch(p, sfh, empty), MOQ_OK);
    if (empty) return;
    MOQ_TEST_CHECK_EQ_INT(write_object(p, sfh, 0), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(write_object(p, sfh, 1), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(moq_session_end_fetch(p->s, sfh, p->now), MOQ_OK);
}

static void check_fetch_done(rl_pair_t *p, const obs_t *co, bool empty,
                             const char *row)
{
    int before = failures;
    MOQ_TEST_CHECK_EQ_INT(co->fetch_ok, 1);
    MOQ_TEST_CHECK_EQ_INT(co->fetch_object, empty ? 0 : 2);
    MOQ_TEST_CHECK_EQ_INT(co->fetch_complete, 1);
    MOQ_TEST_CHECK_EQ_INT(co->fetch_error + co->fetch_reset, 0);
    MOQ_TEST_CHECK_EQ_INT(co->closed, 0);
    if (failures != before)
        fprintf(stderr, "  in row %s (draft-%d)\n", row, (int)p->ver);
}

/* -- FETCH rows -------------------------------------------------------------- */

/* A served FETCH completes on both sides. On draft-18 the publisher FINs its
 * half of the request stream when the response ends (end_fetch, or the empty
 * acceptance that ends it at once), the requester FINs its half once it holds
 * both FETCH_OK and the data FIN, and each side's FIN is ordinary input to the
 * other: no session is lost and no stream is left open. */
static int row_fetch_complete(moq_version_t ver, bool empty)
{
    failures = 0;
    rl_pair_t p;
    MOQ_TEST_CHECK(pair_open(&p, ver, NULL));
    uint64_t sid = 0;
    moq_fetch_t sfh = request_fetch(&p, &sid);
    serve_fetch(&p, sfh, empty);
    shuttle(&p);
    obs_t co;
    memset(&co, 0, sizeof(co));
    drain(p.c, &co);
    check_fetch_done(&p, &co, empty, "fetch_complete");
    if (d18(&p)) {
        MOQ_TEST_CHECK(fin_seen(&p, sid, false));   /* publisher's half */
        MOQ_TEST_CHECK(fin_seen(&p, sid, true));    /* requester's half */
    }
    check_quiet(&p, empty ? "fetch_complete_empty" : "fetch_complete");
    pair_close(&p);
    MOQ_TEST_PASS(empty ? "fetch completes and closes its request stream (empty)"
                        : "fetch completes and closes its request stream");
    return failures;
}

/* The requester FINs its request half only after the publisher has finished
 * (end_fetch, or an empty acceptance) and freed its FETCH state, as other
 * implementations do after every FETCH: that FIN is the ordinary end of the
 * requester's half, absorbed without closing the publisher's session. */
static int row_fetch_requester_fin_after_completion(bool empty)
{
    failures = 0;
    rl_pair_t p;
    MOQ_TEST_CHECK(pair_open(&p, MOQ_VERSION_DRAFT_18, NULL));
    uint64_t sid = 0;
    moq_fetch_t sfh = request_fetch(&p, &sid);
    p.c2s.drop = sid;          /* the publisher sees only the injected FIN */
    serve_fetch(&p, sfh, empty);
    shuttle(&p);
    MOQ_TEST_CHECK_EQ_SIZE(live_fetches(p.s), 0);
    note_rc(&p, moq_transport_bridge_on_peer_bidi_bytes(p.sb, sid, NULL, 0, true, p.now));
    shuttle(&p);
    obs_t so, co;
    memset(&so, 0, sizeof(so));
    memset(&co, 0, sizeof(co));
    drain(p.s, &so);
    drain(p.c, &co);
    MOQ_TEST_CHECK_EQ_INT(so.closed, 0);
    check_fetch_done(&p, &co, empty, "fetch_requester_fin_after_completion");
    check_quiet(&p, "fetch_requester_fin_after_completion");
    pair_close(&p);
    MOQ_TEST_PASS(empty ? "requester FIN after an empty fetch keeps the session"
                        : "requester FIN after end_fetch keeps the session");
    return failures;
}

/* A requester that FINs its request half right after FETCH (before any
 * response) does not cancel the fetch, and the publisher's later completion
 * needs no drain reference for a half already closed. */
static int row_fetch_requester_fin_first(bool empty)
{
    failures = 0;
    rl_pair_t p;
    MOQ_TEST_CHECK(pair_open(&p, MOQ_VERSION_DRAFT_18, NULL));
    uint64_t sid = 0;
    moq_fetch_t sfh = request_fetch(&p, &sid);
    /* The requester's own FIN reaches the publisher now; the one its session
     * sends at completion is the same transport fact and is not repeated. */
    p.c2s.drop = sid;
    note_rc(&p, moq_transport_bridge_on_peer_bidi_bytes(p.sb, sid, NULL, 0, true, p.now));
    serve_fetch(&p, sfh, empty);
    MOQ_TEST_CHECK_EQ_SIZE(p.s->drain_ref_count, 0);
    shuttle(&p);
    obs_t co;
    memset(&co, 0, sizeof(co));
    drain(p.c, &co);
    check_fetch_done(&p, &co, empty, "fetch_requester_fin_first");
    check_quiet(&p, "fetch_requester_fin_first");
    pair_close(&p);
    MOQ_TEST_PASS("early requester FIN neither cancels nor leaks a fetch");
    return failures;
}

/* The publisher's request-stream FIN (and FETCH_OK) arrive before the data
 * stream's tail: closing the request half must not discard the still-arriving
 * objects, and completion waits for the data FIN. */
static int row_fetch_request_fin_before_data(bool empty)
{
    failures = 0;
    rl_pair_t p;
    MOQ_TEST_CHECK(pair_open(&p, MOQ_VERSION_DRAFT_18, NULL));
    uint64_t sid = 0;
    moq_fetch_t sfh = request_fetch(&p, &sid);
    p.s2c.hold_data = true;
    serve_fetch(&p, sfh, empty);
    shuttle(&p);
    obs_t co;
    memset(&co, 0, sizeof(co));
    drain(p.c, &co);
    MOQ_TEST_CHECK(fin_seen(&p, sid, false));
    MOQ_TEST_CHECK_EQ_INT(co.fetch_ok, 1);
    MOQ_TEST_CHECK_EQ_INT(co.fetch_complete, 0);
    MOQ_TEST_CHECK_EQ_SIZE(live_fetches(p.c), 1);   /* still owed the data */
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(p.cb));
    wire_release(&p.s2c);
    shuttle(&p);
    drain(p.c, &co);
    check_fetch_done(&p, &co, empty, "fetch_request_fin_before_data");
    check_quiet(&p, "fetch_request_fin_before_data");
    pair_close(&p);
    MOQ_TEST_PASS("request FIN before the data tail keeps the fetch whole");
    return failures;
}

/* The data FIN arrives before FETCH_OK and the publisher's request FIN: the
 * requester completes only on FETCH_OK and closes its half then. */
static int row_fetch_data_fin_before_response(bool empty)
{
    failures = 0;
    rl_pair_t p;
    MOQ_TEST_CHECK(pair_open(&p, MOQ_VERSION_DRAFT_18, NULL));
    uint64_t sid = 0;
    moq_fetch_t sfh = request_fetch(&p, &sid);
    p.s2c.hold[p.s2c.nhold++] = sid;
    serve_fetch(&p, sfh, empty);
    shuttle(&p);
    obs_t co;
    memset(&co, 0, sizeof(co));
    drain(p.c, &co);
    MOQ_TEST_CHECK_EQ_INT(co.fetch_ok, 0);
    MOQ_TEST_CHECK_EQ_INT(co.fetch_complete, 0);
    MOQ_TEST_CHECK(!fin_seen(&p, sid, true));   /* nothing to close on yet */
    MOQ_TEST_CHECK_EQ_SIZE(live_fetches(p.c), 1);
    wire_release(&p.s2c);
    shuttle(&p);
    drain(p.c, &co);
    check_fetch_done(&p, &co, empty, "fetch_data_fin_before_response");
    MOQ_TEST_CHECK(fin_seen(&p, sid, true));
    check_quiet(&p, "fetch_data_fin_before_response");
    pair_close(&p);
    MOQ_TEST_PASS("data FIN before FETCH_OK completes on the response");
    return failures;
}

/* After the publisher has finished, the requester's half ends with a RESET
 * (or a STOP_SENDING and then its FIN) instead of a plain FIN: each is an
 * ordinary late terminal that retires the publisher's drain reference, never
 * a new request. */
static int row_fetch_late_terminal(bool stop_then_fin)
{
    failures = 0;
    rl_pair_t p;
    MOQ_TEST_CHECK(pair_open(&p, MOQ_VERSION_DRAFT_18, NULL));
    uint64_t sid = 0;
    moq_fetch_t sfh = request_fetch(&p, &sid);
    p.c2s.hold[p.c2s.nhold++] = sid;      /* the requester's FIN stays queued */
    serve_fetch(&p, sfh, false);
    shuttle(&p);
    obs_t co;
    memset(&co, 0, sizeof(co));
    drain(p.c, &co);
    check_fetch_done(&p, &co, false, "fetch_late_terminal");
    MOQ_TEST_CHECK_EQ_SIZE(live_fetches(p.s), 0);
    MOQ_TEST_CHECK_EQ_SIZE(p.s->drain_ref_count, 1);
    if (stop_then_fin) {
        note_rc(&p, moq_transport_bridge_on_peer_stop_sending(p.sb, sid, 0x1, p.now));
        shuttle(&p);
        MOQ_TEST_CHECK(moq_session_state(p.s) == MOQ_SESS_ESTABLISHED);
        wire_release(&p.c2s);           /* now the FIN */
    } else {
        p.c2s.drop = sid;
        wire_release(&p.c2s);
        note_rc(&p, moq_transport_bridge_on_peer_stream_reset(p.sb, sid, 0x1, p.now));
    }
    shuttle(&p);
    obs_t so;
    memset(&so, 0, sizeof(so));
    drain(p.s, &so);
    MOQ_TEST_CHECK_EQ_INT(so.closed, 0);
    MOQ_TEST_CHECK_EQ_INT(so.fetch_cancelled, 0);
    check_quiet(&p, stop_then_fin ? "fetch_late_stop" : "fetch_late_reset");
    pair_close(&p);
    MOQ_TEST_PASS(stop_then_fin ? "late STOP then FIN after a finished fetch"
                                : "late RESET after a finished fetch");
    return failures;
}

/* end_fetch reserves everything it queues: with the action queue full it
 * returns WOULD_BLOCK and leaves the fetch exactly as it was; the retry
 * finishes it. */
static int row_fetch_action_pressure(moq_version_t ver)
{
    failures = 0;
    rl_pair_t p;
    rl_opts_t o = { .server_max_actions = 4 };
    MOQ_TEST_CHECK(pair_open(&p, ver, &o));
    uint64_t sid = 0;
    moq_fetch_t sfh = request_fetch(&p, &sid);
    MOQ_TEST_CHECK_EQ_INT(accept_fetch(&p, sfh, false), MOQ_OK);
    uint64_t oid = 0;
    moq_result_t rc;
    while ((rc = write_object(&p, sfh, oid)) == MOQ_OK) oid++;
    MOQ_TEST_CHECK_EQ_INT(rc, MOQ_ERR_WOULD_BLOCK);
    size_t drains = p.s->drain_ref_count;
    MOQ_TEST_CHECK_EQ_INT(moq_session_end_fetch(p.s, sfh, p.now), MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK_EQ_SIZE(live_fetches(p.s), 1);          /* nothing retired */
    MOQ_TEST_CHECK_EQ_SIZE(p.s->drain_ref_count, drains);  /* nothing reserved */
    shuttle(&p);
    MOQ_TEST_CHECK_EQ_INT(moq_session_end_fetch(p.s, sfh, p.now), MOQ_OK);
    shuttle(&p);
    obs_t co;
    memset(&co, 0, sizeof(co));
    drain(p.c, &co);
    MOQ_TEST_CHECK_EQ_INT(co.fetch_ok, 1);
    MOQ_TEST_CHECK_EQ_INT(co.fetch_object, (int)oid);
    MOQ_TEST_CHECK_EQ_INT(co.fetch_complete, 1);
    check_quiet(&p, "fetch_action_pressure");
    pair_close(&p);
    MOQ_TEST_PASS("end_fetch is atomic under action-queue pressure");
    return failures;
}

/* The publisher's transport refuses writes while the response finishes; the
 * bridge retains the data FIN and the request FIN and completes both later. */
static int row_fetch_transport_pressure(moq_version_t ver, bool empty)
{
    failures = 0;
    rl_pair_t p;
    MOQ_TEST_CHECK(pair_open(&p, ver, NULL));
    moq_fetch_t sfh = request_fetch(&p, NULL);
    p.sep.block_write = true;
    serve_fetch(&p, sfh, empty);
    shuttle(&p);
    shuttle(&p);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(p.sb));
    obs_t co;
    memset(&co, 0, sizeof(co));
    drain(p.c, &co);
    MOQ_TEST_CHECK_EQ_INT(co.fetch_complete, 0);
    p.sep.block_write = false;
    shuttle(&p);
    drain(p.c, &co);
    check_fetch_done(&p, &co, empty, "fetch_transport_pressure");
    check_quiet(&p, "fetch_transport_pressure");
    pair_close(&p);
    MOQ_TEST_PASS("fetch completion survives transport WOULD_BLOCK");
    return failures;
}

/* The requester's event queue is full when the data FIN lands: completion
 * (event, request-half FIN and drain reservation together) is retried, not
 * lost or half-applied. */
static int row_fetch_event_pressure(moq_version_t ver)
{
    failures = 0;
    rl_pair_t p;
    rl_opts_t o = { .client_max_events = 1 };
    MOQ_TEST_CHECK(pair_open(&p, ver, &o));
    moq_fetch_t sfh = request_fetch(&p, NULL);
    serve_fetch(&p, sfh, false);
    obs_t co;
    memset(&co, 0, sizeof(co));
    for (int i = 0; i < 32 && co.fetch_complete == 0; i++) {
        shuttle(&p);
        MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(p.cb));
        drain_one(p.c, &co, 1);
    }
    shuttle(&p);
    drain(p.c, &co);
    check_fetch_done(&p, &co, false, "fetch_event_pressure");
    check_quiet(&p, "fetch_event_pressure");
    pair_close(&p);
    MOQ_TEST_PASS("fetch completion is retried under event pressure");
    return failures;
}

/* Many sequential fetches through two-entry pools: every slot, drain
 * reference and stream mapping is returned each time. */
static int row_fetch_repeat(moq_version_t ver)
{
    failures = 0;
    rl_pair_t p;
    rl_opts_t o = { .max_fetches = 2 };
    MOQ_TEST_CHECK(pair_open(&p, ver, &o));
    for (int round = 0; round < 8; round++) {
        bool empty = (round % 3) == 2;
        moq_fetch_t sfh = request_fetch(&p, NULL);
        serve_fetch(&p, sfh, empty);
        shuttle(&p);
        obs_t co;
        memset(&co, 0, sizeof(co));
        drain(p.c, &co);
        check_fetch_done(&p, &co, empty, "fetch_repeat");
        check_quiet(&p, "fetch_repeat");
    }
    pair_close(&p);
    MOQ_TEST_PASS("repeated fetches reuse every slot and stream");
    return failures;
}

/* An unaccepted FETCH rejected with REQUEST_ERROR still ends cleanly on both
 * sides (the existing reject path, kept as a control). */
static int row_fetch_reject(moq_version_t ver)
{
    failures = 0;
    rl_pair_t p;
    MOQ_TEST_CHECK(pair_open(&p, ver, NULL));
    moq_fetch_t sfh = request_fetch(&p, NULL);
    moq_reject_fetch_cfg_t rj;
    moq_reject_fetch_cfg_init(&rj);
    rj.error_code = MOQ_REQUEST_ERROR_NOT_SUPPORTED;
    MOQ_TEST_CHECK_EQ_INT(moq_session_reject_fetch(p.s, sfh, &rj, p.now), MOQ_OK);
    shuttle(&p);
    obs_t co;
    memset(&co, 0, sizeof(co));
    drain(p.c, &co);
    MOQ_TEST_CHECK_EQ_INT(co.fetch_error, 1);
    check_quiet(&p, "fetch_reject");
    pair_close(&p);
    MOQ_TEST_PASS("rejected fetch ends cleanly");
    return failures;
}

/* -- TRACK_STATUS rows -------------------------------------------------------- */

static moq_track_status_handle_t request_track_status(rl_pair_t *p, uint64_t *sid)
{
    moq_track_status_cfg_t cfg;
    moq_track_status_cfg_init(&cfg);
    cfg.track_namespace = (moq_namespace_t){ .parts = g_ns_part, .count = 1 };
    cfg.track_name = MOQ_BYTES_LITERAL("t");
    moq_track_status_handle_t ch;
    if (sid) *sid = p->cep.next_bidi_id;
    MOQ_TEST_CHECK_EQ_INT(moq_session_track_status(p->c, &cfg, p->now, &ch), MOQ_OK);
    shuttle(p);
    obs_t so;
    memset(&so, 0, sizeof(so));
    drain(p->s, &so);
    MOQ_TEST_CHECK_EQ_INT(so.ts_request, 1);
    return so.ts;
}

static moq_result_t answer_track_status(rl_pair_t *p, moq_track_status_handle_t h,
                                        bool ok)
{
    if (ok) {
        moq_accept_track_status_cfg_t ac;
        moq_accept_track_status_cfg_init(&ac);
        ac.has_largest = true;
        ac.largest_group = 4;
        ac.largest_object = 2;
        return moq_session_accept_track_status(p->s, h, &ac, p->now);
    }
    moq_reject_track_status_cfg_t rj;
    moq_reject_track_status_cfg_init(&rj);
    rj.error_code = MOQ_REQUEST_ERROR_NOT_SUPPORTED;
    return moq_session_reject_track_status(p->s, h, &rj, p->now);
}

/* TRACK_STATUS opens its request stream with FIN (its first and only
 * message); the response still arrives on the peer's half of that stream and
 * must reach the requester. */
static int row_ts_answer(moq_version_t ver, bool ok)
{
    failures = 0;
    rl_pair_t p;
    MOQ_TEST_CHECK(pair_open(&p, ver, NULL));
    uint64_t sid = 0;
    moq_track_status_handle_t h = request_track_status(&p, &sid);
    MOQ_TEST_CHECK_EQ_INT(answer_track_status(&p, h, ok), MOQ_OK);
    shuttle(&p);
    obs_t co;
    memset(&co, 0, sizeof(co));
    drain(p.c, &co);
    MOQ_TEST_CHECK_EQ_INT(co.ts_ok, ok ? 1 : 0);
    MOQ_TEST_CHECK_EQ_INT(co.ts_error, ok ? 0 : 1);
    MOQ_TEST_CHECK_EQ_INT(co.closed, 0);
    if (d18(&p)) {
        MOQ_TEST_CHECK(fin_seen(&p, sid, true));
        MOQ_TEST_CHECK(fin_seen(&p, sid, false));
    }
    check_quiet(&p, ok ? "ts_ok" : "ts_error");
    pair_close(&p);
    MOQ_TEST_PASS(ok ? "track status OK reaches the requester"
                     : "track status REQUEST_ERROR reaches the requester");
    return failures;
}

/* The response arrives in two pieces and its FIN separately (or together
 * with the second piece): the requester reassembles it and retires the stream
 * on the FIN. */
static int row_ts_split(bool fin_with_tail)
{
    failures = 0;
    rl_pair_t p;
    MOQ_TEST_CHECK(pair_open(&p, MOQ_VERSION_DRAFT_18, NULL));
    uint64_t sid = 0;
    moq_track_status_handle_t h = request_track_status(&p, &sid);
    p.s2c.hold[p.s2c.nhold++] = sid;
    MOQ_TEST_CHECK_EQ_INT(answer_track_status(&p, h, true), MOQ_OK);
    shuttle(&p);
    /* Exactly one queued write carries the response and its FIN. */
    MOQ_TEST_CHECK_EQ_SIZE(p.s2c.n, 1);
    fake_op_t resp = p.s2c.q[0];
    p.s2c.n = 0;
    wire_release(&p.s2c);
    MOQ_TEST_CHECK(resp.fin && resp.data_len >= 2);
    size_t half = resp.data_len / 2;
    note_rc(&p, moq_transport_bridge_on_peer_bidi_bytes(p.cb, sid, resp.data, half,
                                                        false, p.now));
    obs_t co;
    memset(&co, 0, sizeof(co));
    drain(p.c, &co);
    MOQ_TEST_CHECK_EQ_INT(co.ts_ok, 0);
    note_rc(&p, moq_transport_bridge_on_peer_bidi_bytes(p.cb, sid, resp.data + half,
                                                        resp.data_len - half,
                                                        fin_with_tail, p.now));
    drain(p.c, &co);
    MOQ_TEST_CHECK_EQ_INT(co.ts_ok, 1);
    if (!fin_with_tail) {
        MOQ_TEST_CHECK_EQ_SIZE(live_track_statuses(p.c), 1);  /* awaits the FIN */
        note_rc(&p, moq_transport_bridge_on_peer_bidi_bytes(p.cb, sid, NULL, 0,
                                                            true, p.now));
    }
    shuttle(&p);
    check_quiet(&p, fin_with_tail ? "ts_split_fin_with_tail" : "ts_split_fin_alone");
    pair_close(&p);
    MOQ_TEST_PASS(fin_with_tail ? "split track status response, FIN with tail"
                                : "split track status response, FIN alone");
    return failures;
}

/* The responder resets its half before answering: the requester's query ends
 * without a session or bridge failure and the stream is retired. */
static int row_ts_premature_reset(void)
{
    failures = 0;
    rl_pair_t p;
    MOQ_TEST_CHECK(pair_open(&p, MOQ_VERSION_DRAFT_18, NULL));
    uint64_t sid = 0;
    moq_track_status_handle_t h = request_track_status(&p, &sid);
    note_rc(&p, moq_transport_bridge_on_peer_stream_reset(p.cb, sid, 0x1, p.now));
    MOQ_TEST_CHECK_EQ_SIZE(live_track_statuses(p.c), 0);
    /* The responder's own view: it answers and its half ends, but nothing it
     * sends on the reset stream reaches the requester any more. */
    p.s2c.drop = sid;
    MOQ_TEST_CHECK_EQ_INT(answer_track_status(&p, h, true), MOQ_OK);
    shuttle(&p);
    obs_t co;
    memset(&co, 0, sizeof(co));
    drain(p.c, &co);
    MOQ_TEST_CHECK_EQ_INT(co.ts_ok, 0);
    MOQ_TEST_CHECK_EQ_INT(co.closed, 0);
    check_quiet(&p, "ts_premature_reset");
    pair_close(&p);
    MOQ_TEST_PASS("track status survives a premature responder reset");
    return failures;
}

/* The requester's transport refuses the opening write, and later its event
 * queue is full when the response lands: both are retried and the response
 * is delivered once. */
static int row_ts_pressure(void)
{
    failures = 0;
    rl_pair_t p;
    rl_opts_t o = { .client_max_events = 1 };
    MOQ_TEST_CHECK(pair_open(&p, MOQ_VERSION_DRAFT_18, &o));
    moq_track_status_cfg_t cfg;
    moq_track_status_cfg_init(&cfg);
    cfg.track_namespace = (moq_namespace_t){ .parts = g_ns_part, .count = 1 };
    cfg.track_name = MOQ_BYTES_LITERAL("t");
    moq_track_status_handle_t ch[2];
    p.cep.block_write = true;
    MOQ_TEST_CHECK_EQ_INT(moq_session_track_status(p.c, &cfg, p.now, &ch[0]), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(moq_session_track_status(p.c, &cfg, p.now, &ch[1]), MOQ_OK);
    shuttle(&p);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(p.cb));
    p.cep.block_write = false;
    shuttle(&p);
    obs_t so;
    memset(&so, 0, sizeof(so));
    moq_track_status_handle_t sh[2];
    moq_event_t ev;
    int got = 0;
    while (moq_session_poll_events(p.s, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_TRACK_STATUS_REQUEST && got < 2)
            sh[got++] = ev.u.track_status_request.handle;
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK_EQ_INT(got, 2);
    if (got == 2) {
        MOQ_TEST_CHECK_EQ_INT(answer_track_status(&p, sh[0], true), MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(answer_track_status(&p, sh[1], false), MOQ_OK);
    }
    obs_t co;
    memset(&co, 0, sizeof(co));
    for (int i = 0; i < 16 && co.ts_ok + co.ts_error < 2; i++) {
        shuttle(&p);
        MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(p.cb));
        drain_one(p.c, &co, 1);
    }
    shuttle(&p);
    drain(p.c, &co);
    MOQ_TEST_CHECK_EQ_INT(co.ts_ok, 1);
    MOQ_TEST_CHECK_EQ_INT(co.ts_error, 1);
    check_quiet(&p, "ts_pressure");
    pair_close(&p);
    MOQ_TEST_PASS("track status survives write and event pressure");
    return failures;
}

/* Many sequential queries through two-entry pools. */
static int row_ts_repeat(moq_version_t ver)
{
    failures = 0;
    rl_pair_t p;
    rl_opts_t o = { .max_track_statuses = 2 };
    MOQ_TEST_CHECK(pair_open(&p, ver, &o));
    for (int round = 0; round < 8; round++) {
        bool ok = (round % 2) == 0;
        moq_track_status_handle_t h = request_track_status(&p, NULL);
        MOQ_TEST_CHECK_EQ_INT(answer_track_status(&p, h, ok), MOQ_OK);
        shuttle(&p);
        obs_t co;
        memset(&co, 0, sizeof(co));
        drain(p.c, &co);
        MOQ_TEST_CHECK_EQ_INT(co.ts_ok, ok ? 1 : 0);
        MOQ_TEST_CHECK_EQ_INT(co.ts_error, ok ? 0 : 1);
        check_quiet(&p, "ts_repeat");
    }
    pair_close(&p);
    MOQ_TEST_PASS("repeated track status queries reuse every slot and stream");
    return failures;
}

int main(void)
{
    int total = 0;
    const moq_version_t vers[2] = { MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18 };
    for (int v = 0; v < 2; v++) {
        total += row_fetch_complete(vers[v], false);
        total += row_fetch_complete(vers[v], true);
        total += row_fetch_action_pressure(vers[v]);
        total += row_fetch_transport_pressure(vers[v], false);
        total += row_fetch_transport_pressure(vers[v], true);
        total += row_fetch_event_pressure(vers[v]);
        total += row_fetch_repeat(vers[v]);
        total += row_fetch_reject(vers[v]);
        total += row_ts_answer(vers[v], true);
        total += row_ts_answer(vers[v], false);
        total += row_ts_repeat(vers[v]);
    }
    for (int e = 0; e < 2; e++) {
        total += row_fetch_requester_fin_after_completion(e != 0);
        total += row_fetch_requester_fin_first(e != 0);
        total += row_fetch_request_fin_before_data(e != 0);
        total += row_fetch_data_fin_before_response(e != 0);
    }
    total += row_fetch_late_terminal(false);
    total += row_fetch_late_terminal(true);
    total += row_ts_split(false);
    total += row_ts_split(true);
    total += row_ts_premature_reset();
    total += row_ts_pressure();
    if (total) fprintf(stderr, "test_bridge_request_lifecycle: %d failure(s)\n", total);
    return total ? 1 : 0;
}
