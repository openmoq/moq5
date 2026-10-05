/*
 * Fetcher-side terminal for a reset FETCH stream (MOQ_EVENT_FETCH_RESET).
 *
 * Contract pinned here, on draft-16 and draft-18: when the peer abruptly
 * terminates a stream of an identified, uncompleted FETCH -- its response data
 * stream (RESET_STREAM) or, on draft-18, its request stream (RESET_STREAM or
 * STOP_SENDING) -- the fetcher receives exactly one FETCH_RESET carrying its
 * handle, the peer's stream error code exactly as received and which stream
 * carried it; the request is retired (a later cancel is STALE_HANDLE, the pool
 * slot is reusable); no FETCH_COMPLETE / FETCH_ERROR / FETCH_CANCELLED is
 * attributed to it; sibling FETCH traffic is untouched; objects queued before
 * the reset are still polled and released. A reset DATA stream whose
 * FETCH_HEADER never arrived carries no request identity and reports nothing
 * (the request-stream terminal, by contrast, needs no data header); a request
 * that already surfaced FETCH_ERROR, REQUEST_REDIRECT or FETCH_COMPLETE
 * reports nothing more, whichever stream is torn down afterwards.
 *
 * SimPair rows cover the natural delivery order; raw-pair rows (manual pump
 * with hold-back) cover event-queue refusal and both documented retries, a
 * delayed FETCH_OK after the terminal, draft-18 request-stream termination
 * before any data header (RESET and STOP), the reverse draft-18 arrival order
 * with the duplicate second signal, the already-ERROR / already-REDIRECT
 * controls for both a later data reset and a later request-stream RESET/STOP
 * (also under event-queue pressure), and the fetcher in streaming-object mode
 * (fetch objects are always whole, so no chunk terminal exists for a FETCH
 * stream and exactly the same single FETCH_RESET results).
 */
#include "test_fetch_abort_rig.h"
#include "../../core/src/session/session_internal.h"
#include <moq/control.h>
#include <moq/control_d18.h>
#include <moq/subscriber.h>
#include <stddef.h>

static const char *vlbl(moq_version_t v) { return v == MOQ_VERSION_DRAFT_16 ? "v16" : "v18"; }

/* ---- layout: appended detail, unchanged enclosing records ---------------- */
static int layout_row(void)
{
    int failures = 0;
    MOQ_TEST_CHECK_EQ_SIZE(sizeof(moq_fetch_reset_event_t), 24);
    MOQ_TEST_CHECK_EQ_SIZE(offsetof(moq_fetch_reset_event_t, fetch), 0);
    MOQ_TEST_CHECK_EQ_SIZE(offsetof(moq_fetch_reset_event_t, error_code), 8);
    MOQ_TEST_CHECK_EQ_SIZE(offsetof(moq_fetch_reset_event_t, data_stream), 16);
    MOQ_TEST_CHECK_EQ_SIZE(offsetof(moq_fetch_reset_event_t, stop_sending), 17);
    MOQ_TEST_CHECK_EQ_SIZE(offsetof(moq_fetch_reset_event_t, _pad), 18);
    MOQ_TEST_CHECK(sizeof(moq_fetch_reset_event_t) <= MOQ_EVENT_DETAIL_MAX);
    /* the reserve dominates the detail union, so the event record is unchanged */
    MOQ_TEST_CHECK_EQ_SIZE(sizeof(moq_event_t), offsetof(moq_event_t, u) + MOQ_EVENT_DETAIL_MAX);
    MOQ_TEST_CHECK_EQ_SIZE(offsetof(moq_event_t, u.fetch_reset), offsetof(moq_event_t, u.fetch_complete));
    MOQ_TEST_CHECK_EQ_U64((uint64_t)MOQ_EVENT_FETCH_RESET, 50);
    /* facade item: the reset member fits inside the existing union extent */
    moq_sub_fetch_item_t it;
    MOQ_TEST_CHECK(sizeof(it.u.reset) <= sizeof(it.u.object));
    MOQ_TEST_CHECK_EQ_SIZE(sizeof(it.u.reset), 16);
    MOQ_TEST_CHECK_EQ_INT((int)MOQ_SUB_FETCH_RESET, 6);
    printf("MEASURED layout: sizeof(moq_fetch_reset_event_t)=%zu sizeof(moq_event_t)=%zu "
           "sizeof(moq_sub_fetch_item_t)=%zu\n", sizeof(moq_fetch_reset_event_t), sizeof(moq_event_t), sizeof(it));
    if (failures == 0) printf("PASS: layout_row\n");
    return failures;
}

/* ---- SimPair: natural order ---------------------------------------------- */
static int terminal_row(moq_version_t v)
{
    const char *lbl = vlbl(v);
    int failures = 0;
    rig_t r;
    if (!rig_up(&r, v, 0)) { printf("FAIL: rig up %s\n", lbl); return 1; }

    moq_fetch_t fa, fb;
    MOQ_TEST_CHECK_EQ_INT((int)fetch_issue(&r, "A", &fa), (int)MOQ_OK);
    MOQ_TEST_CHECK(pub_await_requests(&r, 1));
    moq_fetch_t pa = r.pe.last_request;
    MOQ_TEST_CHECK_EQ_INT((int)fetch_issue(&r, "B", &fb), (int)MOQ_OK);
    MOQ_TEST_CHECK(pub_await_requests(&r, 2));
    moq_fetch_t pb = r.pe.last_request;
    MOQ_TEST_CHECK_EQ_INT((int)pub_accept(&r, pa, 4), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)pub_accept(&r, pb, 1), (int)MOQ_OK);

    /* A: header + one object polled (identified); two more left queued. */
    MOQ_TEST_CHECK_EQ_INT((int)pub_write(&r, pa, 0), (int)MOQ_OK);
    for (int i = 0; i < 8 && ev_objects_for(&r.fe, fa) < 1; i++) { cycle(&r); drain(r.fet, &r.fe, false); }
    MOQ_TEST_CHECK_EQ_INT(ev_objects_for(&r.fe, fa), 1);
    int a_slot = fetch_resolve_handle(r.fet, fa);
    MOQ_TEST_CHECK(a_slot >= 0);
    moq_stream_ref_t a_data_ref = a_slot >= 0 ? r.fet->fetches[a_slot].data_stream_ref : moq_stream_ref_from_u64(0);
    MOQ_TEST_CHECK(a_data_ref._v != 0);
    MOQ_TEST_CHECK_EQ_INT((int)pub_write(&r, pa, 1), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)pub_write(&r, pa, 2), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)pub_write(&r, pb, 0), (int)MOQ_OK);
    for (int i = 0; i < 4; i++) cycle(&r);

    /* publisher aborts A with a distinct nonzero code */
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_abort_fetch(r.pub, pa, 0x7, r.now), (int)MOQ_OK);
    for (int i = 0; i < 8; i++) { cycle(&r); drain(r.fet, &r.fe, false); drain(r.pub, &r.pe, true); }
    MOQ_TEST_CHECK_EQ_INT(r.tr.resets, 1);

    reset_view_t rv = ev_resets_for(&r.fe, fa);
    moq_result_t late_cancel = moq_session_fetch_cancel(r.fet, fa, r.now);
    printf("MEASURED %s terminal: A objects polled=%d, FETCH_RESET n=%d code=0x%llx data=%d stop=%d, "
           "other terminals=%d, complete=%d, late cancel rc=%d\n", lbl, ev_objects_for(&r.fe, fa), rv.n,
           (unsigned long long)rv.code, (int)rv.data_stream, (int)rv.stop_sending, ev_terminals_for(&r.fe, fa),
           r.fe.complete, (int)late_cancel);

    MOQ_TEST_CHECK_EQ_INT(ev_objects_for(&r.fe, fa), 3);        /* queued objects preserved */
    MOQ_TEST_CHECK_EQ_INT(rv.n, 1);                             /* exactly one FETCH_RESET */
    MOQ_TEST_CHECK_EQ_U64(rv.code, 0x7);                        /* the peer's code, verbatim */
    MOQ_TEST_CHECK(rv.data_stream);
    MOQ_TEST_CHECK(!rv.stop_sending);
    MOQ_TEST_CHECK_EQ_INT(ev_terminals_for(&r.fe, fa), 0);     /* no ERROR / CANCELLED */
    MOQ_TEST_CHECK_EQ_INT(ev_all_terminals_for(&r.fe, fa), 1); /* one terminal in total */
    MOQ_TEST_CHECK_EQ_INT((int)late_cancel, (int)MOQ_ERR_STALE_HANDLE);
    MOQ_TEST_CHECK_EQ_INT(r.fe.complete, 0);
    MOQ_TEST_CHECK_EQ_INT(r.tr.close_session, 0);
    MOQ_TEST_CHECK_EQ_INT(r.pe.closed + r.fe.closed, 0);
    /* draft-18: the abort also tore the request stream down (the second and
     * third signals, reset + stop of that bidi, were absorbed: still one) */
    if (v == MOQ_VERSION_DRAFT_18)
        MOQ_TEST_CHECK_EQ_SIZE(r.fet->drain_ref_count, 0);

    /* siblings: B completes; a new fetch C reuses A's slot with a fresh
     * generation and completes; A's ledger is unchanged */
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_end_fetch(r.pub, pb, r.now), (int)MOQ_OK);
    for (int i = 0; i < 8 && r.fe.complete == 0; i++) { cycle(&r); drain(r.fet, &r.fe, false); drain(r.pub, &r.pe, true); }
    MOQ_TEST_CHECK_EQ_INT(ev_objects_for(&r.fe, fb), 1);
    MOQ_TEST_CHECK_EQ_INT(r.fe.complete, 1);
    moq_fetch_t fc;
    MOQ_TEST_CHECK_EQ_INT((int)fetch_issue(&r, "C", &fc), (int)MOQ_OK);
    MOQ_TEST_CHECK(!moq_fetch_eq(fc, fa));
    MOQ_TEST_CHECK_EQ_INT(fetch_resolve_handle(r.fet, fc), a_slot);          /* slot reclaimed */
    MOQ_TEST_CHECK(moq_handle_generation(fc._opaque) != moq_handle_generation(fa._opaque));
    MOQ_TEST_CHECK(pub_await_requests(&r, 3));
    moq_fetch_t pc = r.pe.last_request;
    MOQ_TEST_CHECK_EQ_INT((int)pub_accept(&r, pc, 1), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)pub_write(&r, pc, 0), (int)MOQ_OK);
    for (int i = 0; i < 8 && ev_objects_for(&r.fe, fc) < 1; i++) { cycle(&r); drain(r.fet, &r.fe, false); }
    int c_slot = fetch_resolve_handle(r.fet, fc);
    moq_stream_ref_t c_data_ref = c_slot >= 0 ? r.fet->fetches[c_slot].data_stream_ref : moq_stream_ref_from_u64(0);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_end_fetch(r.pub, pc, r.now), (int)MOQ_OK);
    for (int i = 0; i < 8 && r.fe.complete == 1; i++) { cycle(&r); drain(r.fet, &r.fe, false); drain(r.pub, &r.pe, true); }
    MOQ_TEST_CHECK_EQ_INT(r.fe.complete, 2);

    /* controls: a late reset on A's old data stream (stream gone) and on C's
     * completed stream (already COMPLETE) report nothing */
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_data_reset(r.fet, a_data_ref, 0x9, r.now), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_data_reset(r.fet, c_data_ref, 0x9, r.now), (int)MOQ_OK);
    drain(r.fet, &r.fe, false);
    MOQ_TEST_CHECK_EQ_INT(r.fe.reset_n, 1);
    MOQ_TEST_CHECK_EQ_INT(ev_resets_for(&r.fe, fc).n, 0);
    MOQ_TEST_CHECK_EQ_INT(ev_terminals_for(&r.fe, fb) + ev_terminals_for(&r.fe, fc), 0);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(r.fet), (int)MOQ_SESS_ESTABLISHED);

    rig_down(&r);
    MOQ_TEST_CHECK_EQ_SIZE(r.alloc.live, 0);
    int f = failures;
    if (f == 0) printf("PASS: terminal_row %s\n", lbl);
    return f;
}

/* Every slot of the fetcher's pool is reset without a local cancel: the
 * terminals reclaim them for the next fetch. */
static int pool_reclaim_row(moq_version_t v)
{
    const char *lbl = vlbl(v);
    int failures = 0;
    rig_t r;
    if (!rig_up(&r, v, 0)) { printf("FAIL: rig up %s\n", lbl); return 1; }
    const int pool = 16;   /* session default fetch pool */
    int terminals = 0;
    moq_fetch_t fh[16];
    for (int i = 0; i < pool; i++) {
        char nm[8];
        (void)snprintf(nm, sizeof(nm), "f%02d", i);
        MOQ_TEST_CHECK_EQ_INT((int)fetch_issue(&r, nm, &fh[i]), (int)MOQ_OK);
        MOQ_TEST_CHECK(pub_await_requests(&r, i + 1));
        moq_fetch_t ph = r.pe.last_request;
        MOQ_TEST_CHECK_EQ_INT((int)pub_accept(&r, ph, 2), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)pub_write(&r, ph, 0), (int)MOQ_OK);
        for (int k = 0; k < 4; k++) { cycle(&r); drain(r.fet, &r.fe, false); }
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_abort_fetch(r.pub, ph, 0x10 + (uint64_t)i, r.now), (int)MOQ_OK);
        for (int k = 0; k < 4; k++) { cycle(&r); drain(r.fet, &r.fe, false); drain(r.pub, &r.pe, true); }
        reset_view_t rv = ev_resets_for(&r.fe, fh[i]);
        if (rv.n == 1 && rv.code == 0x10 + (uint64_t)i && rv.data_stream) terminals++;
    }
    MOQ_TEST_CHECK_EQ_INT(r.tr.resets, pool);
    moq_fetch_t next;
    moq_result_t rc = fetch_issue(&r, "next", &next);
    printf("MEASURED %s pool-reclaim: %d resets, exact terminals=%d, next fetch rc=%d, closed=%d\n",
           lbl, pool, terminals, (int)rc, r.fe.closed + r.pe.closed);
    MOQ_TEST_CHECK_EQ_INT(terminals, pool);
    MOQ_TEST_CHECK_EQ_INT(r.fe.reset_n, pool);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(r.tr.close_session, 0);
    MOQ_TEST_CHECK_EQ_INT(r.fe.closed + r.pe.closed, 0);
    rig_down(&r);
    MOQ_TEST_CHECK_EQ_SIZE(r.alloc.live, 0);
    int f = failures;
    if (f == 0) printf("PASS: pool_reclaim_row %s\n", lbl);
    return f;
}

/* ---- raw-pair helpers ---------------------------------------------------- */
static moq_result_t raw_fetch(raw_t *r, const char *name, moq_fetch_t *out)
{
    moq_bytes_t nsp[1] = { MOQ_BYTES_LITERAL("abort") };
    moq_fetch_cfg_t fc;
    memset(&fc, 0, sizeof(fc));
    moq_fetch_cfg_init(&fc);
    fc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 1 };
    fc.track_name = (moq_bytes_t){ (const uint8_t *)name, strlen(name) };
    fc.start_group = 0; fc.start_object = 0; fc.end_group = 0; fc.end_object = 4;
    return moq_session_fetch(r->fet, &fc, r->now, out);
}

static moq_result_t raw_pub_write(raw_t *r, moq_fetch_t fh, uint64_t oid)
{
    uint8_t body[24];
    memset(body, (int)(0x60 + oid), sizeof(body));
    moq_rcbuf_t *pl = NULL;
    if (moq_rcbuf_create(&r->alloc, body, sizeof(body), &pl) != MOQ_OK) return MOQ_ERR_NOMEM;
    moq_fetch_object_cfg_t oc;
    moq_fetch_object_cfg_init(&oc);
    oc.group_id = 0; oc.subgroup_id = 0; oc.object_id = oid; oc.publisher_priority = 100;
    oc.payload = pl;
    moq_result_t rc = moq_session_write_fetch_object(r->pub, fh, &oc, r->now);
    moq_rcbuf_decref(pl);
    return rc;
}

static moq_result_t raw_pub_accept(raw_t *r, moq_fetch_t fh, uint64_t end_object)
{
    moq_accept_fetch_cfg_t ac;
    moq_accept_fetch_cfg_init(&ac);
    ac.end_of_track = false; ac.end_group = 0; ac.end_object = end_object;
    return moq_session_accept_fetch(r->pub, fh, &ac, r->now);
}

/* issue A on the fetcher, carry it to the publisher, return the publisher handle */
static bool raw_issue(raw_t *r, ev_cap_t *pe, const char *name, moq_fetch_t *fa, moq_fetch_t *pa)
{
    if (raw_fetch(r, name, fa) != MOQ_OK) return false;
    int before = pe->requests;
    raw_pump(r, r->fet, r->pub, NULL, NULL);
    drain(r->pub, pe, false);
    if (pe->requests != before + 1) return false;
    *pa = pe->last_request;
    return true;
}

static uint64_t fet_request_id(moq_session_t *fet, moq_fetch_t h)
{
    int slot = fetch_resolve_handle(fet, h);
    return slot < 0 ? UINT64_MAX : fet->fetches[slot].request_id;
}

/* hold filters */
static bool hold_reset_data(const moq_action_t *a, void *ctx) { (void)ctx; return a->kind == MOQ_ACTION_RESET_DATA; }
static bool hold_send_data(const moq_action_t *a, void *ctx) { (void)ctx; return a->kind == MOQ_ACTION_SEND_DATA; }
static bool hold_fetch_ok(const moq_action_t *a, void *ctx)
{
    moq_version_t v = *(moq_version_t *)ctx;
    /* draft-18: the response travels on the request bidi; the publisher's
     * later teardown of that same bidi must stay behind it (a stream cannot
     * deliver bytes after its own reset), so both are held and released in
     * order */
    if (v == MOQ_VERSION_DRAFT_18)
        return a->kind == MOQ_ACTION_SEND_BIDI_STREAM || a->kind == MOQ_ACTION_ABORT_BIDI_STREAM;
    if (a->kind != MOQ_ACTION_SEND_CONTROL) return false;
    moq_control_envelope_t env;
    moq_buf_reader_t rd;
    moq_buf_reader_init(&rd, a->u.send_control.data, a->u.send_control.len);
    return moq_control_decode_envelope(&rd, &env) >= 0 && env.msg_type == MOQ_D16_FETCH_OK;
}

/* ---- event-queue refusal: the terminal survives and both documented retries
 *      complete it with the exact code, without fresh peer bytes ------------ */
typedef enum { RETRY_RESET_AGAIN = 0, RETRY_EMPTY_REFEED = 1 } retry_t;

static int capacity_row(moq_version_t v, retry_t retry, bool streaming)
{
    const char *lbl = vlbl(v);
    const char *rlbl = retry == RETRY_RESET_AGAIN ? "reset-again" : "empty-refeed";
    const char *slbl = streaming ? "streaming" : "whole";
    int failures = 0;
    raw_t r;
    ev_cap_t pe, fe;
    memset(&pe, 0, sizeof(pe)); memset(&fe, 0, sizeof(fe));
    if (!raw_up_ex(&r, v, 2 /* two-deep fetcher event queue */, streaming)) { printf("FAIL: raw up %s\n", lbl); return 1; }
    drain(r.fet, &fe, false); drain(r.pub, &pe, false);
    memset(&pe, 0, sizeof(pe)); memset(&fe, 0, sizeof(fe));

    moq_fetch_t fa, pa;
    MOQ_TEST_CHECK(raw_issue(&r, &pe, "A", &fa, &pa));
    MOQ_TEST_CHECK_EQ_INT((int)raw_pub_accept(&r, pa, 4), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)raw_pub_write(&r, pa, 0), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)raw_pub_write(&r, pa, 1), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)raw_pub_write(&r, pa, 2), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_abort_fetch(r.pub, pa, 0x7, r.now), (int)MOQ_OK);
    /* deliver everything to the fetcher WITHOUT draining its events: the
     * two-deep queue refuses part of the stream and the reset itself */
    raw_pump(&r, r.pub, r.fet, NULL, NULL);
    MOQ_TEST_CHECK_EQ_INT((int)r.last_deliver_rc, (int)MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK_EQ_INT(fe.reset_n, 0);                        /* nothing polled yet */
    int a_slot = fetch_resolve_handle(r.fet, fa);
    MOQ_TEST_CHECK(a_slot >= 0);                                 /* obligation retained, entry intact */
    moq_stream_ref_t data_ref = raw_map_ref(r.last_reset_ref);
    MOQ_TEST_CHECK(r.last_reset_ref._v != 0);

    /* drain one event at a time and retry the documented way until it lands */
    int rounds = 0;
    moq_result_t rrc = MOQ_ERR_WOULD_BLOCK;
    for (; rounds < 12 && rrc == MOQ_ERR_WOULD_BLOCK; rounds++) {
        drain(r.fet, &fe, false);
        if (retry == RETRY_RESET_AGAIN)
            rrc = moq_session_on_data_reset(r.fet, data_ref, 0x7, r.now);
        else
            rrc = moq_session_on_data_bytes(r.fet, data_ref, NULL, 0, false, r.now);
    }
    drain(r.fet, &fe, false);
    reset_view_t rv = ev_resets_for(&fe, fa);
    printf("MEASURED %s capacity/%s/%s: retry rounds=%d final rc=%d, A objects polled=%d, chunks=%d, "
           "FETCH_RESET n=%d code=0x%llx data=%d\n", lbl, rlbl, slbl, rounds, (int)rrc, ev_objects_for(&fe, fa),
           fe.chunks, rv.n, (unsigned long long)rv.code, (int)rv.data_stream);
    MOQ_TEST_CHECK_EQ_INT((int)rrc, (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(rv.n, 1);
    MOQ_TEST_CHECK_EQ_INT(ev_all_terminals_for(&fe, fa), 1);
    MOQ_TEST_CHECK_EQ_INT(fe.chunks, 0);                         /* fetch objects are whole, never chunked */
    MOQ_TEST_CHECK_EQ_U64(rv.code, 0x7);                         /* code survived the refusal */
    MOQ_TEST_CHECK(rv.data_stream);
    MOQ_TEST_CHECK(ev_objects_for(&fe, fa) >= 1);               /* objects already queued were kept */
    MOQ_TEST_CHECK_EQ_INT(ev_terminals_for(&fe, fa), 0);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch_cancel(r.fet, fa, r.now), (int)MOQ_ERR_STALE_HANDLE);
    /* nothing further for A after the terminal; a second retry is a no-op */
    int objs = ev_objects_for(&fe, fa);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_data_reset(r.fet, data_ref, 0x7, r.now), (int)MOQ_OK);
    raw_cycle(&r); drain(r.fet, &fe, false); drain(r.pub, &pe, false);
    MOQ_TEST_CHECK_EQ_INT(ev_objects_for(&fe, fa), objs);
    MOQ_TEST_CHECK_EQ_INT(fe.reset_n, 1);
    MOQ_TEST_CHECK_EQ_INT(r.close_actions, 0);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(r.fet), (int)MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(r.pub), (int)MOQ_SESS_ESTABLISHED);
    raw_down(&r);
    MOQ_TEST_CHECK_EQ_INT((int)r.ta.balance, 0);
    int f = failures;
    if (f == 0) printf("PASS: capacity_row %s %s %s\n", lbl, rlbl, slbl);
    return f;
}

/* ---- delayed FETCH_OK: the response arrives after the terminal ----------- */
static int delayed_ok_row(moq_version_t v)
{
    const char *lbl = vlbl(v);
    int failures = 0;
    raw_t r;
    ev_cap_t pe, fe;
    if (!raw_up(&r, v, 0)) { printf("FAIL: raw up %s\n", lbl); return 1; }
    memset(&pe, 0, sizeof(pe)); memset(&fe, 0, sizeof(fe));
    drain(r.fet, &fe, false); drain(r.pub, &pe, false);
    memset(&pe, 0, sizeof(pe)); memset(&fe, 0, sizeof(fe));

    moq_fetch_t fa, pa;
    MOQ_TEST_CHECK(raw_issue(&r, &pe, "A", &fa, &pa));
    uint64_t id_a = fet_request_id(r.fet, fa);
    MOQ_TEST_CHECK_EQ_INT((int)raw_pub_accept(&r, pa, 2), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)raw_pub_write(&r, pa, 0), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_abort_fetch(r.pub, pa, 0x7, r.now), (int)MOQ_OK);
    moq_version_t vv = v;
    raw_pump(&r, r.pub, r.fet, hold_fetch_ok, &vv);
    MOQ_TEST_CHECK_EQ_INT(r.held_n, v == MOQ_VERSION_DRAFT_18 ? 2 : 1);   /* the FETCH_OK is held back */
    drain(r.fet, &fe, false);
    reset_view_t rv = ev_resets_for(&fe, fa);
    MOQ_TEST_CHECK_EQ_INT(rv.n, 1);
    MOQ_TEST_CHECK_EQ_U64(rv.code, 0x7);
    MOQ_TEST_CHECK(rv.data_stream);
    MOQ_TEST_CHECK_EQ_INT(fe.ok, 0);
    MOQ_TEST_CHECK_EQ_INT(ev_objects_for(&fe, fa), 1);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch_cancel(r.fet, fa, r.now), (int)MOQ_ERR_STALE_HANDLE);
    MOQ_TEST_CHECK(fetch_cancel_tomb_contains(r.fet, id_a));      /* late response is absorbable */
    /* the fetcher's own teardown of the request (draft-18 ABORT) reaches the
     * publisher, whose entry is already gone: absorbed */
    raw_pump(&r, r.fet, r.pub, NULL, NULL);
    drain(r.pub, &pe, false);
    MOQ_TEST_CHECK_EQ_INT(pe.closed, 0);
    /* now the delayed FETCH_OK (and, on draft-18, the publisher's teardown of
     * that bidi behind it): no event, no close, no new entry */
    MOQ_TEST_CHECK_EQ_INT(raw_release(&r, r.fet), v == MOQ_VERSION_DRAFT_18 ? 2 : 1);
    drain(r.fet, &fe, false);
    printf("MEASURED %s delayed-ok: FETCH_RESET n=%d, FETCH_OK after terminal=%d, fetcher state=%d\n",
           lbl, ev_resets_for(&fe, fa).n, fe.ok, (int)moq_session_state(r.fet));
    MOQ_TEST_CHECK_EQ_INT(fe.ok, 0);
    MOQ_TEST_CHECK_EQ_INT(fe.reset_n, 1);
    MOQ_TEST_CHECK_EQ_INT(fe.closed, 0);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(r.fet), (int)MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK(fetch_resolve_handle(r.fet, fa) < 0);
    raw_cycle(&r); drain(r.fet, &fe, false); drain(r.pub, &pe, false);
    MOQ_TEST_CHECK_EQ_INT(r.close_actions, 0);
    raw_down(&r);
    MOQ_TEST_CHECK_EQ_INT((int)r.ta.balance, 0);
    int f = failures;
    if (f == 0) printf("PASS: delayed_ok_row %s\n", lbl);
    return f;
}

/* ---- draft-18: request-stream termination before any data header --------- */
static int request_terminal_row(bool stop)
{
    const char *klbl = stop ? "stop" : "reset";
    int failures = 0;
    raw_t r;
    ev_cap_t pe, fe;
    if (!raw_up(&r, MOQ_VERSION_DRAFT_18, 0)) { printf("FAIL: raw up v18\n"); return 1; }
    memset(&pe, 0, sizeof(pe)); memset(&fe, 0, sizeof(fe));
    drain(r.fet, &fe, false); drain(r.pub, &pe, false);
    memset(&pe, 0, sizeof(pe)); memset(&fe, 0, sizeof(fe));

    moq_fetch_t fa, pa;
    MOQ_TEST_CHECK(raw_issue(&r, &pe, "A", &fa, &pa));
    uint64_t id_a = fet_request_id(r.fet, fa);
    MOQ_TEST_CHECK_EQ_INT((int)raw_pub_accept(&r, pa, 2), (int)MOQ_OK);
    raw_pump(&r, r.pub, r.fet, hold_send_data, NULL);             /* FETCH_OK arrives; the data header is held */
    MOQ_TEST_CHECK_EQ_INT(r.held_n, 1);
    drain(r.fet, &fe, false);
    MOQ_TEST_CHECK_EQ_INT(fe.ok, 1);
    int a_slot = fetch_resolve_handle(r.fet, fa);
    MOQ_TEST_CHECK(a_slot >= 0);
    MOQ_TEST_CHECK(!r.fet->fetches[a_slot].data_stream_started);
    moq_stream_ref_t req_ref = r.fet->fetches[a_slot].request_stream_ref;
    MOQ_TEST_CHECK(req_ref._v != 0);

    /* the peer terminates the request stream (no identifying data header
     * ever arrived): the request-side terminal closes the fetch */
    uint64_t code = stop ? 0xB : 0x9;
    moq_result_t trc = stop ? moq_session_on_bidi_stream_stop(r.fet, req_ref, code, r.now)
                            : moq_session_on_bidi_stream_reset(r.fet, req_ref, code, r.now);
    MOQ_TEST_CHECK_EQ_INT((int)trc, (int)MOQ_OK);
    drain(r.fet, &fe, false);
    reset_view_t rv = ev_resets_for(&fe, fa);
    printf("MEASURED v18 request-%s: FETCH_RESET n=%d code=0x%llx data=%d stop=%d\n", klbl, rv.n,
           (unsigned long long)rv.code, (int)rv.data_stream, (int)rv.stop_sending);
    MOQ_TEST_CHECK_EQ_INT(rv.n, 1);
    MOQ_TEST_CHECK_EQ_U64(rv.code, code);
    MOQ_TEST_CHECK(!rv.data_stream);
    MOQ_TEST_CHECK_EQ_INT((int)rv.stop_sending, (int)stop);
    MOQ_TEST_CHECK_EQ_INT(ev_terminals_for(&fe, fa), 0);
    MOQ_TEST_CHECK_EQ_INT(ev_all_terminals_for(&fe, fa), 1);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch_cancel(r.fet, fa, r.now), (int)MOQ_ERR_STALE_HANDLE);
    MOQ_TEST_CHECK(fetch_cancel_tomb_contains(r.fet, id_a));
    /* the other signal of the same teardown is absorbed (exactly once) */
    moq_result_t src = stop ? moq_session_on_bidi_stream_reset(r.fet, req_ref, code, r.now)
                            : moq_session_on_bidi_stream_stop(r.fet, req_ref, code, r.now);
    MOQ_TEST_CHECK_EQ_INT((int)src, (int)MOQ_OK);
    drain(r.fet, &fe, false);
    MOQ_TEST_CHECK_EQ_INT(fe.reset_n, 1);
    /* the data stream for the retired request arrives late (its header was
     * in flight): header for a tombstoned id -> stopped, no event; the STOP
     * reaches the publisher, which retires its side without closing */
    MOQ_TEST_CHECK_EQ_INT(raw_release(&r, r.fet), 1);
    drain(r.fet, &fe, false);
    raw_pump(&r, r.fet, r.pub, NULL, NULL);
    drain(r.pub, &pe, false);
    MOQ_TEST_CHECK_EQ_INT(pe.closed, 0);
    MOQ_TEST_CHECK_EQ_INT(ev_objects_for(&fe, fa), 0);
    MOQ_TEST_CHECK_EQ_INT(fe.reset_n, 1);
    MOQ_TEST_CHECK_EQ_INT(ev_all_terminals_for(&fe, fa), 1);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(r.fet), (int)MOQ_SESS_ESTABLISHED);
    raw_cycle(&r); drain(r.fet, &fe, false); drain(r.pub, &pe, false);
    MOQ_TEST_CHECK_EQ_INT(r.close_actions, 0);
    raw_down(&r);
    MOQ_TEST_CHECK_EQ_INT((int)r.ta.balance, 0);
    int f = failures;
    if (f == 0) printf("PASS: request_terminal_row v18 %s\n", klbl);
    return f;
}

/* ---- draft-18: the abort's request-stream teardown arrives BEFORE the data
 *      reset (held back); still exactly one terminal, from the request side -- */
static int order_row(void)
{
    int failures = 0;
    raw_t r;
    ev_cap_t pe, fe;
    if (!raw_up(&r, MOQ_VERSION_DRAFT_18, 0)) { printf("FAIL: raw up v18\n"); return 1; }
    memset(&pe, 0, sizeof(pe)); memset(&fe, 0, sizeof(fe));
    drain(r.fet, &fe, false); drain(r.pub, &pe, false);
    memset(&pe, 0, sizeof(pe)); memset(&fe, 0, sizeof(fe));

    moq_fetch_t fa, pa;
    MOQ_TEST_CHECK(raw_issue(&r, &pe, "A", &fa, &pa));
    MOQ_TEST_CHECK_EQ_INT((int)raw_pub_accept(&r, pa, 2), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)raw_pub_write(&r, pa, 0), (int)MOQ_OK);
    raw_pump(&r, r.pub, r.fet, NULL, NULL);
    drain(r.fet, &fe, false);
    MOQ_TEST_CHECK_EQ_INT(ev_objects_for(&fe, fa), 1);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_abort_fetch(r.pub, pa, 0x7, r.now), (int)MOQ_OK);
    raw_pump(&r, r.pub, r.fet, hold_reset_data, NULL);           /* bidi abort (reset+stop) first */
    MOQ_TEST_CHECK_EQ_INT(r.held_n, 1);
    drain(r.fet, &fe, false);
    reset_view_t rv = ev_resets_for(&fe, fa);
    MOQ_TEST_CHECK_EQ_INT(rv.n, 1);
    MOQ_TEST_CHECK_EQ_U64(rv.code, 0x7);
    MOQ_TEST_CHECK(!rv.data_stream);                              /* the request stream carried it */
    MOQ_TEST_CHECK(!rv.stop_sending);                             /* its RESET half arrived first */
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch_cancel(r.fet, fa, r.now), (int)MOQ_ERR_STALE_HANDLE);
    /* now the data reset: the stream is bound to a retired handle -> silent */
    MOQ_TEST_CHECK_EQ_INT(raw_release(&r, r.fet), 1);
    drain(r.fet, &fe, false);
    printf("MEASURED v18 order(bidi-first): FETCH_RESET n=%d data=%d stop=%d, after data reset n=%d\n",
           rv.n, (int)rv.data_stream, (int)rv.stop_sending, fe.reset_n);
    MOQ_TEST_CHECK_EQ_INT(fe.reset_n, 1);
    MOQ_TEST_CHECK_EQ_INT(ev_terminals_for(&fe, fa), 0);
    MOQ_TEST_CHECK_EQ_INT(ev_all_terminals_for(&fe, fa), 1);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(r.fet), (int)MOQ_SESS_ESTABLISHED);
    raw_cycle(&r); drain(r.fet, &fe, false); drain(r.pub, &pe, false);
    MOQ_TEST_CHECK_EQ_INT(r.close_actions, 0);
    MOQ_TEST_CHECK_EQ_INT(pe.closed, 0);
    raw_down(&r);
    MOQ_TEST_CHECK_EQ_INT((int)r.ta.balance, 0);
    int f = failures;
    if (f == 0) printf("PASS: order_row v18 bidi-first\n");
    return f;
}

/* ---- already-ERROR control: a request that surfaced FETCH_ERROR reports no
 *      FETCH_RESET when its (already bound) data stream is reset afterwards -- */
static int already_error_row(moq_version_t v)
{
    const char *lbl = vlbl(v);
    int failures = 0;
    raw_t r;
    ev_cap_t pe, fe;
    if (!raw_up(&r, v, 0)) { printf("FAIL: raw up %s\n", lbl); return 1; }
    memset(&pe, 0, sizeof(pe)); memset(&fe, 0, sizeof(fe));
    drain(r.fet, &fe, false); drain(r.pub, &pe, false);
    memset(&pe, 0, sizeof(pe)); memset(&fe, 0, sizeof(fe));

    moq_fetch_t fa, pa;
    MOQ_TEST_CHECK(raw_issue(&r, &pe, "A", &fa, &pa));
    uint64_t id_a = fet_request_id(r.fet, fa);
    int a_slot = fetch_resolve_handle(r.fet, fa);
    MOQ_TEST_CHECK(a_slot >= 0);
    moq_stream_ref_t req_ref = a_slot >= 0 ? r.fet->fetches[a_slot].request_stream_ref : moq_stream_ref_from_u64(0);

    /* scripted peer: the data stream binds first (FETCH_HEADER for A, objects
     * may precede the response), then a terminal REQUEST_ERROR */
    moq_stream_ref_t data_ref = moq_stream_ref_from_u64(0xD001);
    {
        uint8_t hdr[16];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, hdr, sizeof(hdr));
        moq_buf_write_varint(&w, 0x05);                   /* FETCH_HEADER stream type, both drafts */
        moq_buf_write_varint(&w, id_a);
        MOQ_TEST_CHECK(moq_session_on_data_bytes(r.fet, data_ref, hdr, w.pos, false, r.now) >= 0);
    }
    MOQ_TEST_CHECK(r.fet->fetches[a_slot].data_stream_started);
    {
        uint8_t msg[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        if (v == MOQ_VERSION_DRAFT_18) {
            MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_request_error(&w, 0x5, 0, (moq_bytes_t){0}), (int)MOQ_OK);
            MOQ_TEST_CHECK(moq_session_on_bidi_stream_bytes(r.fet, req_ref, msg, w.pos, false, r.now) >= 0);
        } else {
            MOQ_TEST_CHECK_EQ_INT((int)moq_d16_encode_request_error(&w, id_a, 0x5, 0, NULL, 0), (int)MOQ_OK);
            MOQ_TEST_CHECK(moq_session_on_control_bytes(r.fet, msg, w.pos, r.now) >= 0);
        }
    }
    drain(r.fet, &fe, false);
    MOQ_TEST_CHECK_EQ_INT(fe.error, 1);
    MOQ_TEST_CHECK_EQ_INT(ev_terminals_for(&fe, fa), 1);
    int state_after_error = fetch_resolve_handle(r.fet, fa) >= 0 ? (int)r.fet->fetches[a_slot].state : -1;
    /* the data stream is reset afterwards: no second terminal */
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_data_reset(r.fet, data_ref, 0x7, r.now), (int)MOQ_OK);
    drain(r.fet, &fe, false);
    printf("MEASURED %s already-error: entry state after ERROR=%d (5=DRAINING_RESPONSE, -1=freed), "
           "FETCH_RESET n=%d\n", lbl, state_after_error, fe.reset_n);
    MOQ_TEST_CHECK_EQ_INT(fe.reset_n, 0);
    MOQ_TEST_CHECK_EQ_INT(ev_terminals_for(&fe, fa), 1);
    MOQ_TEST_CHECK_EQ_INT(ev_all_terminals_for(&fe, fa), 1);
    if (v == MOQ_VERSION_DRAFT_18) {
        MOQ_TEST_CHECK_EQ_INT(state_after_error, (int)MOQ_FETCH_DRAINING_RESPONSE);
        /* the response stream's FIN frees the draining entry */
        MOQ_TEST_CHECK(moq_session_on_bidi_stream_bytes(r.fet, req_ref, NULL, 0, true, r.now) >= 0);
    } else {
        MOQ_TEST_CHECK_EQ_INT(state_after_error, -1);
    }
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch_cancel(r.fet, fa, r.now), (int)MOQ_ERR_STALE_HANDLE);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(r.fet), (int)MOQ_SESS_ESTABLISHED);
    drain(r.fet, &fe, false);
    MOQ_TEST_CHECK_EQ_INT(fe.reset_n, 0);
    raw_down(&r);
    MOQ_TEST_CHECK_EQ_INT((int)r.ta.balance, 0);
    int f = failures;
    if (f == 0) printf("PASS: already_error_row %s\n", lbl);
    return f;
}

/* ---- already-ERROR / already-REDIRECT, then the request stream itself is
 *      torn down (draft-18): the draining entry is retired with its stream
 *      cleanup but no second application terminal ------------------------- */
typedef struct ae_opts { bool stop; bool tiny_events; bool redirect; bool bind_data; } ae_opts_t;

static int already_terminal_request_row(ae_opts_t o)
{
    int failures = 0;
    raw_t r;
    ev_cap_t pe, fe;
    if (!raw_up(&r, MOQ_VERSION_DRAFT_18, o.tiny_events ? 1 : 0)) { printf("FAIL: raw up v18\n"); return 1; }
    memset(&pe, 0, sizeof(pe)); memset(&fe, 0, sizeof(fe));
    drain(r.fet, &fe, false); drain(r.pub, &pe, false);
    memset(&pe, 0, sizeof(pe)); memset(&fe, 0, sizeof(fe));

    moq_fetch_t fa, pa;
    MOQ_TEST_CHECK(raw_issue(&r, &pe, "A", &fa, &pa));
    uint64_t id_a = fet_request_id(r.fet, fa);
    int a_slot = fetch_resolve_handle(r.fet, fa);
    MOQ_TEST_CHECK(a_slot >= 0);
    moq_stream_ref_t req_ref = a_slot >= 0 ? r.fet->fetches[a_slot].request_stream_ref : moq_stream_ref_from_u64(0);
    moq_stream_ref_t data_ref = moq_stream_ref_from_u64(0xD002);
    if (o.bind_data) {
        uint8_t hdr[16];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, hdr, sizeof(hdr));
        moq_buf_write_varint(&w, 0x05);
        moq_buf_write_varint(&w, id_a);
        MOQ_TEST_CHECK(moq_session_on_data_bytes(r.fet, data_ref, hdr, w.pos, false, r.now) >= 0);
        MOQ_TEST_CHECK(r.fet->fetches[a_slot].data_stream_started);
    }
    /* the terminal response, without FIN: the entry drains */
    {
        uint8_t msg[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        if (o.redirect) {
            moq_d18_redirect_t rd;
            memset(&rd, 0, sizeof(rd));
            MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_request_error_redirect(&w, 0x34, 0, (moq_bytes_t){0}, &rd), (int)MOQ_OK);
        } else {
            MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_request_error(&w, 0x5, 0, (moq_bytes_t){0}), (int)MOQ_OK);
        }
        MOQ_TEST_CHECK(moq_session_on_bidi_stream_bytes(r.fet, req_ref, msg, w.pos, false, r.now) >= 0);
    }
    if (!o.tiny_events) drain(r.fet, &fe, false);   /* under pressure the terminal stays queued */
    MOQ_TEST_CHECK_EQ_INT((int)r.fet->fetches[a_slot].state, (int)MOQ_FETCH_DRAINING_RESPONSE);

    /* the peer tears the request stream down; a draining request needs no
     * event slot: it is retired even with the queue full */
    uint64_t code = o.stop ? 0xB : 0x9;
    moq_result_t trc = o.stop ? moq_session_on_bidi_stream_stop(r.fet, req_ref, code, r.now)
                              : moq_session_on_bidi_stream_reset(r.fet, req_ref, code, r.now);
    MOQ_TEST_CHECK_EQ_INT((int)trc, (int)MOQ_OK);
    MOQ_TEST_CHECK(fetch_resolve_handle(r.fet, fa) < 0);                /* retired */
    int stops = 0, other = 0;
    { moq_action_t a; while (moq_session_poll_actions(r.fet, &a, 1) > 0) { if (a.kind == MOQ_ACTION_STOP_DATA) stops++; else other++; moq_action_cleanup(&a); } }
    drain(r.fet, &fe, false);
    int all = ev_all_terminals_for(&fe, fa);
    printf("MEASURED v18 already-%s then request-%s%s%s: terminals total=%d (error=%d redirect=%d reset=%d), "
           "STOP_DATA=%d other actions=%d\n", o.redirect ? "redirect" : "error", o.stop ? "stop" : "reset",
           o.tiny_events ? " events=1" : "", o.bind_data ? " data-bound" : "", all, fe.error, fe.redirect,
           fe.reset_n, stops, other);
    MOQ_TEST_CHECK_EQ_INT(all, 1);                                      /* exactly one terminal in total */
    MOQ_TEST_CHECK_EQ_INT(fe.reset_n, 0);
    MOQ_TEST_CHECK_EQ_INT(o.redirect ? fe.redirect : fe.error, 1);
    MOQ_TEST_CHECK_EQ_INT(stops, o.bind_data ? 1 : 0);                 /* data uni still stopped */
    MOQ_TEST_CHECK(fetch_cancel_tomb_contains(r.fet, id_a));            /* late data absorbed */
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch_cancel(r.fet, fa, r.now), (int)MOQ_ERR_STALE_HANDLE);
    /* the other signal of the same teardown is absorbed */
    moq_result_t src = o.stop ? moq_session_on_bidi_stream_reset(r.fet, req_ref, code, r.now)
                              : moq_session_on_bidi_stream_stop(r.fet, req_ref, code, r.now);
    MOQ_TEST_CHECK_EQ_INT((int)src, (int)MOQ_OK);
    drain(r.fet, &fe, false);
    MOQ_TEST_CHECK_EQ_INT(ev_all_terminals_for(&fe, fa), 1);
    if (o.bind_data) {
        /* a late reset of the bound data stream reports nothing either */
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_data_reset(r.fet, data_ref, 0x7, r.now), (int)MOQ_OK);
        drain(r.fet, &fe, false);
        MOQ_TEST_CHECK_EQ_INT(ev_all_terminals_for(&fe, fa), 1);
    }
    /* slot reuse: the next fetch takes A's slot with a fresh generation */
    moq_fetch_t fb;
    MOQ_TEST_CHECK_EQ_INT((int)raw_fetch(&r, "B", &fb), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(fetch_resolve_handle(r.fet, fb), a_slot);
    MOQ_TEST_CHECK(!moq_fetch_eq(fb, fa));
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(r.fet), (int)MOQ_SESS_ESTABLISHED);
    raw_down(&r);
    MOQ_TEST_CHECK_EQ_INT((int)r.ta.balance, 0);
    int f = failures;
    if (f == 0) printf("PASS: already_terminal_request_row v18 %s/%s%s%s\n", o.redirect ? "redirect" : "error",
                       o.stop ? "stop" : "reset", o.tiny_events ? " events=1" : "", o.bind_data ? " data-bound" : "");
    return f;
}

int main(void)
{
    int failures = 0;
    failures += layout_row();
    failures += terminal_row(MOQ_VERSION_DRAFT_16);
    failures += terminal_row(MOQ_VERSION_DRAFT_18);
    failures += pool_reclaim_row(MOQ_VERSION_DRAFT_16);
    failures += pool_reclaim_row(MOQ_VERSION_DRAFT_18);
    failures += capacity_row(MOQ_VERSION_DRAFT_16, RETRY_RESET_AGAIN, false);
    failures += capacity_row(MOQ_VERSION_DRAFT_18, RETRY_RESET_AGAIN, false);
    failures += capacity_row(MOQ_VERSION_DRAFT_16, RETRY_EMPTY_REFEED, false);
    failures += capacity_row(MOQ_VERSION_DRAFT_18, RETRY_EMPTY_REFEED, false);
    /* fetcher in streaming-object mode: same single terminal, no chunk path */
    failures += capacity_row(MOQ_VERSION_DRAFT_16, RETRY_RESET_AGAIN, true);
    failures += capacity_row(MOQ_VERSION_DRAFT_18, RETRY_RESET_AGAIN, true);
    failures += capacity_row(MOQ_VERSION_DRAFT_16, RETRY_EMPTY_REFEED, true);
    failures += capacity_row(MOQ_VERSION_DRAFT_18, RETRY_EMPTY_REFEED, true);
    failures += delayed_ok_row(MOQ_VERSION_DRAFT_16);
    failures += delayed_ok_row(MOQ_VERSION_DRAFT_18);
    failures += request_terminal_row(false);
    failures += request_terminal_row(true);
    failures += order_row();
    failures += already_error_row(MOQ_VERSION_DRAFT_16);
    failures += already_error_row(MOQ_VERSION_DRAFT_18);
    /* draining (already ERROR / REDIRECT) then the request stream is torn down */
    failures += already_terminal_request_row((ae_opts_t){ .stop = false, .tiny_events = false, .redirect = false, .bind_data = true });
    failures += already_terminal_request_row((ae_opts_t){ .stop = true,  .tiny_events = false, .redirect = false, .bind_data = true });
    failures += already_terminal_request_row((ae_opts_t){ .stop = false, .tiny_events = true,  .redirect = false, .bind_data = false });
    failures += already_terminal_request_row((ae_opts_t){ .stop = true,  .tiny_events = true,  .redirect = false, .bind_data = true });
    failures += already_terminal_request_row((ae_opts_t){ .stop = false, .tiny_events = false, .redirect = true,  .bind_data = false });
    failures += already_terminal_request_row((ae_opts_t){ .stop = true,  .tiny_events = true,  .redirect = true,  .bind_data = true });
    printf("%s: session_fetch_reset_terminal (%d failures)\n", failures == 0 ? "PASS" : "FAIL", failures);
    return failures == 0 ? 0 : 1;
}
