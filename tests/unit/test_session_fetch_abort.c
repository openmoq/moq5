/*
 * moq_session_abort_fetch: request-local abort of an accepted, unfinished
 * FETCH on the publisher side, driven over SimPair on both drafts.
 *
 * Oracles (each row runs on draft-16 and draft-18):
 *   terminal   -- the publisher's abort queues exactly one RESET_DATA for the
 *                 fetch's own data stream with the given code; the fetcher
 *                 never sees FETCH_COMPLETE for that fetch; the publisher's
 *                 handle is retired (repeat abort / write -> STALE_HANDLE).
 *   refusals   -- NULL session, non-varint code, pending (not accepted) fetch,
 *                 fetcher-role handle, full action queue (WOULD_BLOCK with the
 *                 fetch intact, then OK on retry).
 *   liveness   -- a second accepted FETCH and an unrelated live subscription on
 *                 the same session keep delivering after the abort; the session
 *                 stays open; a new FETCH reuses the pool with a fresh handle.
 *   lifetimes  -- objects already queued at the fetcher before the reset are
 *                 polled and released; owned payloads and request-stream
 *                 mappings retire without leaks (counting allocator == 0).
 *   late peer  -- after the abort, the fetcher cancels its (still pending)
 *                 request: the publisher must not misbehave on the late
 *                 cancellation; the measured outcome per draft is recorded, with
 *                 an end_fetch control proving parity with the existing
 *                 completion path.
 */
#include "test_fetch_abort_rig.h"
#include "test_session_support.h"
#include "../../core/src/session/session_internal.h"
#include <moq/control.h>

/* ---- main row: accepted unfinished FETCH A aborted while FETCH B and a live
 *      subscription keep flowing; `late` selects the post-abort peer action. */
typedef enum { LATE_NONE = 0, LATE_PEER_CANCEL = 1 } late_t;
typedef enum { PATH_ABORT = 0, PATH_END_FETCH = 1 } path_t;

static int abort_row(moq_version_t v, late_t late, path_t path)
{
    const char *lbl = v == MOQ_VERSION_DRAFT_16 ? "v16" : "v18";
    const char *plbl = path == PATH_ABORT ? "abort" : "end_fetch-control";
    int failures = 0;
    rig_t r;
    if (!rig_up(&r, v, 0)) { printf("FAIL: rig up %s\n", lbl); return 1; }

    /* Unrelated live subscription: fetcher subscribes a track the publisher
     * serves with one object before and one after the abort. */
    moq_bytes_t nsp[1] = { MOQ_BYTES_LITERAL("abort") };
    moq_subscribe_cfg_t sc;
    memset(&sc, 0, sizeof(sc));
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 1 };
    sc.track_name = MOQ_BYTES_LITERAL("live");
    sc.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    moq_subscription_t sh;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscribe(r.fet, &sc, r.now, &sh), (int)MOQ_OK);
    for (int i = 0; i < 8 && !r.pe.up_seen; i++) { cycle(&r); drain(r.pub, &r.pe, true); }
    MOQ_TEST_CHECK(r.pe.up_seen);
    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = 0; sgc.subgroup_id = 0; sgc.publisher_priority = 100;
    moq_subgroup_handle_t sgh;
    for (int i = 0; i < 4; i++) { cycle(&r); drain(r.fet, &r.fe, false); }
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_subgroup(r.pub, r.pe.up_sub, &sgc, r.now, &sgh), (int)MOQ_OK);
    {
        uint8_t b[8]; memset(b, 0x11, sizeof(b));
        moq_rcbuf_t *pl = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_rcbuf_create(&r.alloc.vt, b, sizeof(b), &pl), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_write_object(r.pub, sgh, 0, pl, r.now), (int)MOQ_OK);
        moq_rcbuf_decref(pl);
    }
    for (int i = 0; i < 4; i++) { cycle(&r); drain(r.fet, &r.fe, false); }
    MOQ_TEST_CHECK_EQ_INT(r.fe.sub_objects, 1);

    /* FETCH A (to be aborted) and FETCH B (sibling), both accepted. */
    moq_fetch_t fa, fb;
    MOQ_TEST_CHECK_EQ_INT((int)fetch_issue(&r, "A", &fa), (int)MOQ_OK);
    MOQ_TEST_CHECK(pub_await_requests(&r, 1));
    moq_fetch_t pa = r.pe.last_request;
    MOQ_TEST_CHECK_EQ_INT((int)fetch_issue(&r, "B", &fb), (int)MOQ_OK);
    MOQ_TEST_CHECK(pub_await_requests(&r, 2));
    moq_fetch_t pb = r.pe.last_request;
    MOQ_TEST_CHECK_EQ_INT((int)pub_accept(&r, pa, 4), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)pub_write(&r, pa, 0), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)pub_accept(&r, pb, 2), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)pub_write(&r, pb, 0), (int)MOQ_OK);
    for (int i = 0; i < 6 && !(r.fe.ok == 2 && r.fe.objects == 2); i++) { cycle(&r); drain(r.fet, &r.fe, false); }
    MOQ_TEST_CHECK_EQ_INT(r.fe.ok, 2);
    MOQ_TEST_CHECK_EQ_INT(r.fe.objects, 2);
    MOQ_TEST_CHECK_EQ_INT(r.fe.complete, 0);
    /* The data stream ref A's FETCH_HEADER went out on: the first SEND_DATA
     * stream opened by the server after A's accept. Two distinct data streams
     * exist (A, B) plus the live subgroup. */
    MOQ_TEST_CHECK_EQ_INT(r.tr.data_streams, 3);
    uint64_t a_ref = r.tr.data_refs[1];   /* [0] = live subgroup, [1] = A, [2] = B */

    /* Objects queued at the fetcher BEFORE the reset: A (0,1),(0,2) transported,
     * not polled. */
    MOQ_TEST_CHECK_EQ_INT((int)pub_write(&r, pa, 1), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)pub_write(&r, pa, 2), (int)MOQ_OK);
    cycle(&r); cycle(&r);

    /* -- the operation under test (or the completion control) -- */
    int resets_before = r.tr.resets;
    moq_result_t op;
    if (path == PATH_ABORT) {
        op = moq_session_abort_fetch(r.pub, pa, 0x0 /* INTERNAL_ERROR */, r.now);
    } else {
        op = moq_session_end_fetch(r.pub, pa, r.now);
    }
    MOQ_TEST_CHECK_EQ_INT((int)op, (int)MOQ_OK);
    /* Handle retired: repeat abort / write / end on it are STALE_HANDLE. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_abort_fetch(r.pub, pa, 0x0, r.now), (int)MOQ_ERR_STALE_HANDLE);
    MOQ_TEST_CHECK_EQ_INT((int)pub_write(&r, pa, 3), (int)MOQ_ERR_STALE_HANDLE);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_end_fetch(r.pub, pa, r.now), (int)MOQ_ERR_STALE_HANDLE);
    /* The genuine crossing: the fetcher cancels NOW, before the abort's reset
     * has been delivered to it (its entry is still pending), so the cancel and
     * the reset are in flight at once. */
    moq_result_t crossing_crc = MOQ_ERR_INTERNAL;
    if (late == LATE_PEER_CANCEL && path == PATH_ABORT) {
        crossing_crc = moq_session_fetch_cancel(r.fet, fa, r.now);
        MOQ_TEST_CHECK_EQ_INT((int)crossing_crc, (int)MOQ_OK);
    }
    for (int i = 0; i < 6; i++) { cycle(&r); drain(r.fet, &r.fe, false); drain(r.pub, &r.pe, true); }

    if (path == PATH_ABORT) {
        /* terminal oracle: exactly one RESET_DATA, on A's data stream, code 0x0 */
        MOQ_TEST_CHECK_EQ_INT(r.tr.resets - resets_before, 1);
        MOQ_TEST_CHECK_EQ_U64(r.tr.reset_ref, a_ref);
        MOQ_TEST_CHECK_EQ_U64(r.tr.reset_code, 0);
        MOQ_TEST_CHECK_EQ_INT(r.fe.complete, 0);            /* A never completes */
    }
    MOQ_TEST_CHECK_EQ_INT(r.tr.close_session, 0);           /* no session close */
    MOQ_TEST_CHECK_EQ_INT(r.fe.closed + r.pe.closed, 0);
    /* queued old-handle objects were polled (released by event cleanup) */
    int a_objs = 0;
    for (int i = 0; i < r.fe.obj_n && i < 32; i++) if (r.fe.obj_handles[i] == fa._opaque) a_objs++;
    printf("MEASURED %s %s: fetcher A objects polled=%d (1 before + queued after), complete=%d, error=%d, "
           "server resets=%d\n", lbl, plbl, a_objs, r.fe.complete, r.fe.error, r.tr.resets - resets_before);

    /* liveness oracle: B keeps streaming to completion; the live subscription
     * delivers another object; a NEW fetch reuses the pool with a fresh handle. */
    MOQ_TEST_CHECK_EQ_INT((int)pub_write(&r, pb, 1), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_end_fetch(r.pub, pb, r.now), (int)MOQ_OK);
    {
        uint8_t b[8]; memset(b, 0x22, sizeof(b));
        moq_rcbuf_t *pl = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_rcbuf_create(&r.alloc.vt, b, sizeof(b), &pl), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_write_object(r.pub, sgh, 1, pl, r.now), (int)MOQ_OK);
        moq_rcbuf_decref(pl);
    }
    int cmp_before = r.fe.complete;
    for (int i = 0; i < 8 && !(r.fe.complete == cmp_before + 1 && r.fe.sub_objects == 2); i++) { cycle(&r); drain(r.fet, &r.fe, false); drain(r.pub, &r.pe, true); }
    int b_objs = 0;
    for (int i = 0; i < r.fe.obj_n && i < 32; i++) if (r.fe.obj_handles[i] == fb._opaque) b_objs++;
    MOQ_TEST_CHECK_EQ_INT(b_objs, 2);                        /* B's two objects */
    MOQ_TEST_CHECK_EQ_INT(r.fe.complete, cmp_before + 1);   /* exactly B completes */
    MOQ_TEST_CHECK_EQ_INT(r.fe.sub_objects, 2);              /* live sub advanced */
    moq_fetch_t fc_new;
    MOQ_TEST_CHECK_EQ_INT((int)fetch_issue(&r, "C", &fc_new), (int)MOQ_OK);
    MOQ_TEST_CHECK(pub_await_requests(&r, 3));
    moq_fetch_t pc = r.pe.last_request;
    MOQ_TEST_CHECK(!moq_fetch_eq(pc, pa));                   /* fresh generation */
    MOQ_TEST_CHECK_EQ_INT((int)pub_accept(&r, pc, 1), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)pub_write(&r, pc, 0), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_end_fetch(r.pub, pc, r.now), (int)MOQ_OK);
    cmp_before = r.fe.complete;
    for (int i = 0; i < 8 && r.fe.complete == cmp_before; i++) { cycle(&r); drain(r.fet, &r.fe, false); }
    MOQ_TEST_CHECK_EQ_INT(r.fe.complete, cmp_before + 1);

    /* late peer action on the aborted request: the fetcher's entry for A is
     * still pending (no terminal reached it), so it may cancel. Record what the
     * publisher does with that late cancellation. */
    /* Fetcher-side terminal: an abort whose reset reached a still-pending
     * entry yields exactly one FETCH_RESET (the stream code, data stream); a
     * crossing cancel freed the entry first, so the reset finds a stale handle
     * and nothing is fabricated; the completion control never emits one. */
    {
        reset_view_t rv = ev_resets_for(&r.fe, fa);
        int want = (path == PATH_ABORT && late == LATE_NONE) ? 1 : 0;
        MOQ_TEST_CHECK_EQ_INT(rv.n, want);
        if (want == 1) {
            MOQ_TEST_CHECK_EQ_U64(rv.code, 0x0);
            MOQ_TEST_CHECK(rv.data_stream);
            MOQ_TEST_CHECK(!rv.stop_sending);
        }
        MOQ_TEST_CHECK_EQ_INT(ev_terminals_for(&r.fe, fa), 0);   /* never ERROR/CANCELLED */
    }
    if (late == LATE_PEER_CANCEL) {
        moq_result_t crc = moq_session_fetch_cancel(r.fet, fa, r.now);
        for (int i = 0; i < 6; i++) { cycle(&r); drain(r.fet, &r.fe, false); drain(r.pub, &r.pe, true); }
        if (path == PATH_ABORT)
            printf("MEASURED %s %s late-cancel: crossing rc=%d later rc=%d publisher FETCH_CANCELLED=%d "
                   "session_closed(pub=%d,fet=%d) close_actions=%d\n", lbl, plbl, (int)crossing_crc, (int)crc,
                   r.pe.cancelled, r.pe.closed, r.fe.closed, r.tr.close_session);
        else
            printf("MEASURED %s %s late-cancel: later rc=%d publisher FETCH_CANCELLED=%d "
                   "session_closed(pub=%d,fet=%d) close_actions=%d\n", lbl, plbl, (int)crc,
                   r.pe.cancelled, r.pe.closed, r.fe.closed, r.tr.close_session);
        if (path == PATH_ABORT) {
            /* The crossing cancel was accepted (entry pending at the time); the
             * publisher absorbed it without closing; the entry is now gone. */
            MOQ_TEST_CHECK_EQ_INT((int)crossing_crc, (int)MOQ_OK);
            MOQ_TEST_CHECK_EQ_INT(r.tr.close_session, 0);
            MOQ_TEST_CHECK_EQ_INT(r.pe.closed + r.fe.closed, 0);
        }
        /* Either way the request is retired at the fetcher by now. */
        MOQ_TEST_CHECK_EQ_INT((int)crc, (int)MOQ_ERR_STALE_HANDLE);
    } else if (path == PATH_ABORT) {
        /* Terminal delivered: a later local cancel is STALE_HANDLE. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch_cancel(r.fet, fa, r.now), (int)MOQ_ERR_STALE_HANDLE);
    }
    rig_down(&r);
    MOQ_TEST_CHECK_EQ_SIZE(r.alloc.live, 0);
    int f = failures;
    if (f == 0) printf("PASS: abort_row %s %s late=%d\n", lbl, plbl, (int)late);
    return f;
}

/* ---- refusals: NULL, invalid code, pending fetch, fetcher role, queue full --- */
static int refusal_row(moq_version_t v)
{
    const char *lbl = v == MOQ_VERSION_DRAFT_16 ? "v16" : "v18";
    int failures = 0;
    rig_t r;
    if (!rig_up(&r, v, 3 /* three-deep action queues */)) { printf("FAIL: rig up %s\n", lbl); return 1; }
    moq_fetch_t fa;
    MOQ_TEST_CHECK_EQ_INT((int)fetch_issue(&r, "A", &fa), (int)MOQ_OK);
    MOQ_TEST_CHECK(pub_await_requests(&r, 1));
    moq_fetch_t pa = r.pe.last_request;
    /* NULL session */
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_abort_fetch(NULL, pa, 0, r.now), (int)MOQ_ERR_INVAL);
    /* code outside the QUIC varint domain */
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_abort_fetch(r.pub, pa, MOQ_QUIC_VARINT_MAX + 1, r.now), (int)MOQ_ERR_INVAL);
    /* pending (not accepted): refused, nothing emitted, still rejectable */
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_abort_fetch(r.pub, pa, 0, r.now), (int)MOQ_ERR_WRONG_STATE);
    MOQ_TEST_CHECK_EQ_INT(r.tr.resets, 0);
    /* fetcher-role handle */
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_abort_fetch(r.fet, fa, 0, r.now), (int)MOQ_ERR_WRONG_STATE);
    /* accept + one object, then fill the publisher's 3-deep action queue with
     * TRACK_STATUS requests until refused; abort must be WOULD_BLOCK with the
     * fetch intact, then succeed after one cycle drains the queue. */
    MOQ_TEST_CHECK_EQ_INT((int)pub_accept(&r, pa, 2), (int)MOQ_OK);
    moq_result_t w = pub_write(&r, pa, 0);
    for (int i = 0; i < 6 && w == MOQ_ERR_WOULD_BLOCK; i++) { cycle(&r); w = pub_write(&r, pa, 0); }
    MOQ_TEST_CHECK_EQ_INT((int)w, (int)MOQ_OK);
    cycle(&r); drain(r.fet, &r.fe, false);
    int filled = 0;
    moq_result_t fill_rc = MOQ_OK;
    for (int i = 0; i < 64; i++) {
        char nm[8];
        (void)snprintf(nm, sizeof(nm), "t%03d", i);
        moq_bytes_t nsp[1] = { MOQ_BYTES_LITERAL("abort") };
        moq_track_status_cfg_t tsc;
        memset(&tsc, 0, sizeof(tsc));
        moq_track_status_cfg_init(&tsc);
        tsc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 1 };
        tsc.track_name = (moq_bytes_t){ (const uint8_t *)nm, strlen(nm) };
        moq_track_status_handle_t tsh;
        fill_rc = moq_session_track_status(r.pub, &tsc, r.now, &tsh);
        if (fill_rc != MOQ_OK) break;
        filled++;
    }
    MOQ_TEST_CHECK_EQ_INT((int)fill_rc, (int)MOQ_ERR_WOULD_BLOCK);
    moq_result_t a1 = moq_session_abort_fetch(r.pub, pa, 0, r.now);
    MOQ_TEST_CHECK_EQ_INT((int)a1, (int)MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK_EQ_INT(r.tr.resets, 0);                 /* nothing emitted */
    MOQ_TEST_CHECK_EQ_INT((int)pub_write(&r, pa, 1), (int)MOQ_ERR_WOULD_BLOCK); /* fetch intact, queue still full */
    cycle(&r);                                               /* drain the queue */
    moq_result_t a2 = moq_session_abort_fetch(r.pub, pa, 0, r.now);
    MOQ_TEST_CHECK_EQ_INT((int)a2, (int)MOQ_OK);
    for (int i = 0; i < 6; i++) { cycle(&r); drain(r.fet, &r.fe, false); drain(r.pub, &r.pe, true); }
    MOQ_TEST_CHECK_EQ_INT(r.tr.resets, 1);
    MOQ_TEST_CHECK_EQ_U64(r.tr.reset_code, 0);
    MOQ_TEST_CHECK_EQ_INT(r.fe.complete, 0);
    MOQ_TEST_CHECK_EQ_INT(r.tr.close_session, 0);
    printf("MEASURED %s refusals: filled=%d then abort WOULD_BLOCK, retry OK, resets=%d\n", lbl, filled, r.tr.resets);
    rig_down(&r);
    MOQ_TEST_CHECK_EQ_SIZE(r.alloc.live, 0);
    int f = failures;
    if (f == 0) printf("PASS: refusal_row %s\n", lbl);
    return f;
}


/* ---- late FETCH_CANCEL after an abort (draft-16 control-message profile) ----
 *
 * The abort frees the publisher's slot, so a FETCH_CANCEL that crosses the
 * reset names an id the request registry no longer knows. The session keeps a
 * bounded cache of ids it ABORTED (fetch_abort_tombs, capacity = fetch pool):
 * a cancel naming a cached id is consumed once; every other unknown or
 * wrong-parity id still closes the session. Draft-18 cancels by resetting the
 * request bidi, which the drain ring already absorbs, so no id is cached there.
 */

static uint64_t pub_request_id(moq_session_t *pub, moq_fetch_t h)
{
    int slot = fetch_resolve_handle(pub, h);
    return slot < 0 ? UINT64_MAX : pub->fetches[slot].request_id;
}

static moq_result_t feed_raw_cancel(moq_session_t *pub, uint64_t request_id, uint64_t now)
{
    uint8_t buf[32];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    if (moq_d16_encode_varint_msg(&w, MOQ_D16_FETCH_CANCEL, request_id) < 0) return MOQ_ERR_INVAL;
    return moq_session_on_control_bytes(pub, buf, w.pos, now);
}

/* Refused aborts leave no cache entry; a committed abort records exactly the
 * aborted id (draft-16) or nothing (draft-18); the crossing cancel is absorbed
 * once, and the id is not honoured a second time. */
static int tomb_row(moq_version_t v)
{
    const char *lbl = v == MOQ_VERSION_DRAFT_16 ? "v16" : "v18";
    const bool caches = (v == MOQ_VERSION_DRAFT_16);
    int failures = 0;
    rig_t r;
    if (!rig_up(&r, v, 3 /* three-deep action queues */)) { printf("FAIL: rig up %s\n", lbl); return 1; }
    moq_fetch_t fa, fb;
    MOQ_TEST_CHECK_EQ_INT((int)fetch_issue(&r, "A", &fa), (int)MOQ_OK);
    MOQ_TEST_CHECK(pub_await_requests(&r, 1));
    moq_fetch_t pa = r.pe.last_request;
    MOQ_TEST_CHECK_EQ_INT((int)fetch_issue(&r, "B", &fb), (int)MOQ_OK);
    MOQ_TEST_CHECK(pub_await_requests(&r, 2));
    moq_fetch_t pb = r.pe.last_request;
    uint64_t id_a = pub_request_id(r.pub, pa);
    MOQ_TEST_CHECK(id_a != UINT64_MAX);
    MOQ_TEST_CHECK_EQ_SIZE(r.pub->fetch_abort_tomb_count, 0);

    MOQ_TEST_CHECK_EQ_INT((int)pub_accept(&r, pa, 2), (int)MOQ_OK);
    moq_result_t w = pub_write(&r, pa, 0);
    for (int i = 0; i < 6 && w == MOQ_ERR_WOULD_BLOCK; i++) { cycle(&r); w = pub_write(&r, pa, 0); }
    MOQ_TEST_CHECK_EQ_INT((int)w, (int)MOQ_OK);
    cycle(&r); drain(r.fet, &r.fe, false);

    /* refused aborts: invalid code, pending sibling, fetcher-role handle, full
     * action queue -- none installs a cache entry */
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_abort_fetch(r.pub, pa, MOQ_QUIC_VARINT_MAX + 1, r.now), (int)MOQ_ERR_INVAL);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_abort_fetch(r.pub, pb, 0, r.now), (int)MOQ_ERR_WRONG_STATE);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_abort_fetch(r.fet, fa, 0, r.now), (int)MOQ_ERR_WRONG_STATE);
    moq_result_t fill_rc = MOQ_OK;
    for (int i = 0; i < 64 && fill_rc == MOQ_OK; i++) {
        char nm[8];
        (void)snprintf(nm, sizeof(nm), "t%03d", i);
        moq_bytes_t nsp[1] = { MOQ_BYTES_LITERAL("abort") };
        moq_track_status_cfg_t tsc;
        memset(&tsc, 0, sizeof(tsc));
        moq_track_status_cfg_init(&tsc);
        tsc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 1 };
        tsc.track_name = (moq_bytes_t){ (const uint8_t *)nm, strlen(nm) };
        moq_track_status_handle_t tsh;
        fill_rc = moq_session_track_status(r.pub, &tsc, r.now, &tsh);
    }
    MOQ_TEST_CHECK_EQ_INT((int)fill_rc, (int)MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_abort_fetch(r.pub, pa, 0, r.now), (int)MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK_EQ_SIZE(r.pub->fetch_abort_tomb_count, 0);
    cycle(&r);

    /* the pending sibling is cancelled by the peer the ordinary way: the
     * publisher sees FETCH_CANCELLED and nothing closes */
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch_cancel(r.fet, fb, r.now), (int)MOQ_OK);
    for (int i = 0; i < 6; i++) { cycle(&r); drain(r.pub, &r.pe, true); drain(r.fet, &r.fe, false); }
    MOQ_TEST_CHECK_EQ_INT(r.pe.cancelled, 1);
    MOQ_TEST_CHECK_EQ_INT(r.tr.close_session, 0);
    MOQ_TEST_CHECK_EQ_SIZE(r.pub->fetch_abort_tomb_count, 0);

    /* committed abort: exactly the aborted id is cached (draft-16 only) */
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_abort_fetch(r.pub, pa, 0, r.now), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(r.pub->fetch_abort_tomb_count, caches ? 1 : 0);
    if (caches) MOQ_TEST_CHECK_EQ_U64(r.pub->fetch_abort_tombs[0], id_a);

    /* the crossing cancel: issued while the reset is still in flight (the
     * fetcher's entry is pending), absorbed once by the publisher: no close,
     * no FETCH_CANCELLED (the reset already answered it), cache entry consumed;
     * the fetcher, having cancelled first, gets no terminal for A */
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch_cancel(r.fet, fa, r.now), (int)MOQ_OK);
    for (int i = 0; i < 6; i++) { cycle(&r); drain(r.pub, &r.pe, true); drain(r.fet, &r.fe, false); }
    MOQ_TEST_CHECK_EQ_INT(r.tr.resets, 1);
    MOQ_TEST_CHECK_EQ_INT(ev_resets_for(&r.fe, fa).n, 0);
    MOQ_TEST_CHECK_EQ_INT(r.pe.cancelled, 1);
    MOQ_TEST_CHECK_EQ_INT(r.tr.close_session, 0);
    MOQ_TEST_CHECK_EQ_INT(r.pe.closed + r.fe.closed, 0);
    MOQ_TEST_CHECK_EQ_SIZE(r.pub->fetch_abort_tomb_count, 0);

    /* a fresh fetch reuses the pool with a fresh handle and completes */
    moq_fetch_t fc;
    MOQ_TEST_CHECK_EQ_INT((int)fetch_issue(&r, "C", &fc), (int)MOQ_OK);
    MOQ_TEST_CHECK(pub_await_requests(&r, 3));
    moq_fetch_t pc = r.pe.last_request;
    MOQ_TEST_CHECK(!moq_fetch_eq(pc, pa));
    MOQ_TEST_CHECK_EQ_INT((int)pub_accept(&r, pc, 1), (int)MOQ_OK);
    w = pub_write(&r, pc, 0);
    for (int i = 0; i < 6 && w == MOQ_ERR_WOULD_BLOCK; i++) { cycle(&r); w = pub_write(&r, pc, 0); }
    MOQ_TEST_CHECK_EQ_INT((int)w, (int)MOQ_OK);
    moq_result_t e = moq_session_end_fetch(r.pub, pc, r.now);
    for (int i = 0; i < 6 && e == MOQ_ERR_WOULD_BLOCK; i++) { cycle(&r); e = moq_session_end_fetch(r.pub, pc, r.now); }
    MOQ_TEST_CHECK_EQ_INT((int)e, (int)MOQ_OK);
    for (int i = 0; i < 8 && r.fe.complete == 0; i++) { cycle(&r); drain(r.fet, &r.fe, false); drain(r.pub, &r.pe, true); }
    MOQ_TEST_CHECK_EQ_INT(r.fe.complete, 1);

    if (caches) {
        /* the consumed id is not honoured twice: a duplicate FETCH_CANCEL for it
         * is an unknown request and closes the session (protocol violation 0x4) */
        (void)feed_raw_cancel(r.pub, id_a, r.now);
        for (int i = 0; i < 6; i++) { cycle(&r); drain(r.pub, &r.pe, true); drain(r.fet, &r.fe, false); }
        MOQ_TEST_CHECK_EQ_INT(r.tr.close_session, 1);
        MOQ_TEST_CHECK_EQ_INT(r.pe.closed, 1);
    }
    printf("MEASURED %s tomb: refused aborts cached=0, committed cached=%zu, late cancel absorbed, "
           "duplicate cancel closes=%d\n", lbl, (size_t)(caches ? 1 : 0), r.tr.close_session);
    rig_down(&r);
    MOQ_TEST_CHECK_EQ_SIZE(r.alloc.live, 0);
    int f = failures;
    if (f == 0) printf("PASS: tomb_row %s\n", lbl);
    return f;
}

/* Unrelated FETCH_CANCELs keep failing closed while an aborted id is cached:
 * an unknown id of the right parity, and an id of the wrong parity. */
typedef enum { NEG_UNKNOWN_ID = 0, NEG_WRONG_PARITY = 1 } neg_t;

static int negative_control_row(neg_t kind)
{
    const char *klbl = kind == NEG_UNKNOWN_ID ? "unknown-id" : "wrong-parity";
    int failures = 0;
    rig_t r;
    if (!rig_up(&r, MOQ_VERSION_DRAFT_16, 0)) { printf("FAIL: rig up v16\n"); return 1; }
    moq_fetch_t fa;
    MOQ_TEST_CHECK_EQ_INT((int)fetch_issue(&r, "A", &fa), (int)MOQ_OK);
    MOQ_TEST_CHECK(pub_await_requests(&r, 1));
    moq_fetch_t pa = r.pe.last_request;
    uint64_t id_a = pub_request_id(r.pub, pa);
    MOQ_TEST_CHECK_EQ_INT((int)pub_accept(&r, pa, 2), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)pub_write(&r, pa, 0), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_abort_fetch(r.pub, pa, 0, r.now), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(r.pub->fetch_abort_tomb_count, 1);
    for (int i = 0; i < 4; i++) { cycle(&r); drain(r.pub, &r.pe, true); drain(r.fet, &r.fe, false); }
    /* the peer is the client, so its request ids are even: an unrelated even id
     * is unknown; an odd id is the wrong parity */
    uint64_t bad = kind == NEG_UNKNOWN_ID ? ((id_a + 1000) & ~(uint64_t)1) : (id_a | 1);
    (void)feed_raw_cancel(r.pub, bad, r.now);
    for (int i = 0; i < 6; i++) { cycle(&r); drain(r.pub, &r.pe, true); drain(r.fet, &r.fe, false); }
    MOQ_TEST_CHECK_EQ_INT(r.tr.close_session, 1);
    MOQ_TEST_CHECK_EQ_INT(r.pe.closed, 1);
    MOQ_TEST_CHECK_EQ_SIZE(r.pub->fetch_abort_tomb_count, 1);   /* the cached id was not spent */
    printf("MEASURED v16 negative %s: cancel id=%llu closes=%d\n", klbl, (unsigned long long)bad, r.tr.close_session);
    rig_down(&r);
    MOQ_TEST_CHECK_EQ_SIZE(r.alloc.live, 0);
    int f = failures;
    if (f == 0) printf("PASS: negative_control_row v16 %s\n", klbl);
    return f;
}

/* Bounded grace: the cache holds at most fetch-pool-many ids and drops the
 * oldest. A raw pair whose publisher is fed FETCH messages directly (its
 * responses are discarded, so the fetcher's own pool never limits the count)
 * aborts pool+1 fetches; the newest id's late cancel is absorbed, the evicted
 * oldest id's late cancel closes the session. */
static int exhaustion_row(void)
{
    int failures = 0;
    test_alloc_state_t ta = { 0 };
    moq_alloc_t alloc = test_allocator(&ta);
    moq_session_cfg_t extra = MOQ_SESSION_CFG_INIT;
    extra.version = MOQ_VERSION_DRAFT_16;
    moq_session_t *c = NULL, *s = NULL;
    establish_pair(&alloc, 1024, 1024, &c, &s, &extra, &extra);
    const size_t cap = s->fetch_abort_tomb_cap;
    MOQ_TEST_CHECK(cap >= 2);
    uint64_t now = 1;
    moq_bytes_t nsp[1] = { MOQ_BYTES_LITERAL("abort") };
    uint64_t last_id = 0;
    for (size_t i = 0; i <= cap; i++) {
        uint64_t id = 2 * (uint64_t)i;   /* client-originated: even */
        last_id = id;
        uint8_t buf[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_d16_fetch_t f;
        memset(&f, 0, sizeof(f));
        f.request_id = id;
        f.fetch_type = 1;
        f.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 1 };
        f.track_name = MOQ_BYTES_LITERAL("X");
        f.end_object = 1;
        MOQ_TEST_CHECK_EQ_INT((int)moq_d16_encode_fetch(&w, &f, NULL, 0), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_control_bytes(s, buf, w.pos, now), (int)MOQ_OK);
        moq_event_t ev;
        moq_fetch_t ph; bool got = false;
        while (moq_session_poll_events(s, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_FETCH_REQUEST) { ph = ev.u.fetch_request.fetch; got = true; }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got);
        if (!got) break;
        moq_accept_fetch_cfg_t ac;
        moq_accept_fetch_cfg_init(&ac);
        ac.end_object = 1;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_fetch(s, ph, &ac, now), (int)MOQ_OK);
        uint8_t body[8]; memset(body, 0x5a, sizeof(body));
        moq_rcbuf_t *pl = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_rcbuf_create(&alloc, body, sizeof(body), &pl), (int)MOQ_OK);
        moq_fetch_object_cfg_t oc;
        moq_fetch_object_cfg_init(&oc);
        oc.publisher_priority = 100; oc.payload = pl;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_write_fetch_object(s, ph, &oc, now), (int)MOQ_OK);
        moq_rcbuf_decref(pl);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_abort_fetch(s, ph, 0, now), (int)MOQ_OK);
        /* the publisher's output is not delivered anywhere in this row */
        moq_action_t acts[16];
        size_t n;
        while ((n = moq_session_poll_actions(s, acts, 16)) > 0)
            for (size_t k = 0; k < n; k++) moq_action_cleanup(&acts[k]);
        MOQ_TEST_CHECK_EQ_SIZE(s->fetch_abort_tomb_count, i + 1 < cap ? i + 1 : cap);
    }
    MOQ_TEST_CHECK_EQ_SIZE(s->fetch_abort_tomb_count, cap);
    MOQ_TEST_CHECK_EQ_U64(s->fetch_abort_tombs[0], 2);           /* id 0 evicted */
    MOQ_TEST_CHECK_EQ_U64(s->fetch_abort_tombs[cap - 1], last_id);
    /* newest: absorbed */
    (void)feed_raw_cancel(s, last_id, now);
    MOQ_TEST_CHECK(moq_session_state(s) != MOQ_SESS_CLOSED);
    MOQ_TEST_CHECK_EQ_SIZE(s->fetch_abort_tomb_count, cap - 1);
    /* evicted oldest: unknown request, fails closed */
    (void)feed_raw_cancel(s, 0, now);
    MOQ_TEST_CHECK(moq_session_state(s) == MOQ_SESS_CLOSED);
    printf("MEASURED v16 exhaustion: cap=%zu aborted=%zu newest absorbed, evicted oldest closes\n", cap, cap + 1);
    moq_session_destroy(s);
    moq_session_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)ta.balance, 0);
    int f = failures;
    if (f == 0) printf("PASS: exhaustion_row v16\n");
    return f;
}

int main(void)
{
    int failures = 0;
    failures += abort_row(MOQ_VERSION_DRAFT_16, LATE_NONE, PATH_ABORT);
    failures += abort_row(MOQ_VERSION_DRAFT_18, LATE_NONE, PATH_ABORT);
    failures += abort_row(MOQ_VERSION_DRAFT_16, LATE_PEER_CANCEL, PATH_ABORT);
    failures += abort_row(MOQ_VERSION_DRAFT_18, LATE_PEER_CANCEL, PATH_ABORT);
    /* parity controls: the existing completion path under the same late cancel */
    failures += abort_row(MOQ_VERSION_DRAFT_16, LATE_PEER_CANCEL, PATH_END_FETCH);
    failures += abort_row(MOQ_VERSION_DRAFT_18, LATE_PEER_CANCEL, PATH_END_FETCH);
    failures += refusal_row(MOQ_VERSION_DRAFT_16);
    failures += refusal_row(MOQ_VERSION_DRAFT_18);
    /* late FETCH_CANCEL recognition: cache discipline, negative controls, bound */
    failures += tomb_row(MOQ_VERSION_DRAFT_16);
    failures += tomb_row(MOQ_VERSION_DRAFT_18);
    failures += negative_control_row(NEG_UNKNOWN_ID);
    failures += negative_control_row(NEG_WRONG_PARITY);
    failures += exhaustion_row();
    printf("%s: session_fetch_abort (%d failures)\n", failures == 0 ? "PASS" : "FAIL", failures);
    return failures == 0 ? 0 : 1;
}
