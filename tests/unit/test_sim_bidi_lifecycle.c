/*
 * SimPair request-bidi lifecycle (draft-18): the harness retires a bidi slot only
 * once BOTH halves are closed (or on RESET/STOP), so a responder's terminal FIN
 * does not end the stream while the opener's half is still open. Each clean
 * terminal exchange closes both halves (the responder FINs its response; the
 * requester closes its send half), so the slot retires and can be reused.
 *
 * This churns far more reject / cancel cycles than MOQ_SIM_MAX_BIDI_STREAMS on a
 * single pair: if slots leaked (e.g. retired on the first FIN, dropping the
 * requester's later close, or never reclaimed), the pair would exhaust its slot
 * pool and run_until_quiescent would fail. It also asserts the drain rings return
 * to empty after each clean exchange.
 */
#include <moq/moq.h>
#include <moq/control_d18.h>
#include <moq/sim.h>
#include "test_support.h"
#include "../../core/src/session/session_internal.h"

/* Comfortably exceeds MOQ_SIM_MAX_BIDI_STREAMS so retirement (not headroom) is
 * what keeps the pool from exhausting. */
#define CYCLES 100

static moq_simpair_t *make_pair(void)
{
    moq_simpair_cfg_t scfg = MOQ_SIMPAIR_CFG_INIT;
    scfg.alloc = moq_alloc_default();
    scfg.version = MOQ_VERSION_DRAFT_18;
    moq_simpair_t *sp = NULL;
    if (moq_simpair_create(&scfg, &sp) < 0) return NULL;
    if (moq_simpair_start(sp) < 0) { moq_simpair_destroy(sp); return NULL; }
    moq_simpair_run_until_quiescent(sp, 32, NULL);
    return sp;
}

static void drain_events(moq_session_t *s)
{
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) moq_event_cleanup(&ev);
}

int main(void)
{
    int failures = 0;

    /* == SUBSCRIBE_NAMESPACE reject churn (requester auto-closes) ====== */
    {
        moq_simpair_t *sp = make_pair();
        MOQ_TEST_CHECK(sp != NULL);
        moq_session_t *c = moq_simpair_client(sp);
        moq_session_t *sv = moq_simpair_server(sp);
        bool ok = true;
        for (int i = 0; i < CYCLES && ok; i++) {
            char name[24];
            snprintf(name, sizeof(name), "rej%d", i);
            moq_subscribe_namespace_cfg_t cfg;
            moq_subscribe_namespace_cfg_init(&cfg);
            cfg.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
            moq_bytes_t parts[] = { { (const uint8_t *)name, strlen(name) } };
            cfg.track_namespace_prefix = (moq_namespace_t){ parts, 1 };
            moq_ns_sub_handle_t h;
            if (moq_session_subscribe_namespace(c, &cfg,
                    moq_simpair_now_us(sp), &h) != MOQ_OK) { ok = false; break; }
            if (moq_simpair_run_until_quiescent(sp, 32, NULL) < 0) { ok = false; break; }
            moq_ns_sub_handle_t sh = { 0 };
            moq_event_t ev;
            while (moq_session_poll_events(sv, &ev, 1) > 0) {
                if (ev.kind == MOQ_EVENT_NS_SUB_REQUEST)
                    sh = ev.u.ns_sub_request.handle;
                moq_event_cleanup(&ev);
            }
            if (sh._opaque == 0) { ok = false; break; }
            moq_reject_ns_sub_cfg_t rej;
            moq_reject_ns_sub_cfg_init(&rej);
            rej.error_code = MOQ_REQUEST_ERROR_UNAUTHORIZED;
            if (moq_session_reject_ns_sub(sv, sh, &rej,
                    moq_simpair_now_us(sp)) != MOQ_OK) { ok = false; break; }
            if (moq_simpair_run_until_quiescent(sp, 32, NULL) < 0) { ok = false; break; }
            drain_events(c);
        }
        MOQ_TEST_CHECK(ok);   /* no slot exhaustion across CYCLES > pool */
        MOQ_TEST_CHECK_EQ_SIZE(c->drain_ref_count, 0);
        MOQ_TEST_CHECK_EQ_SIZE(sv->drain_ref_count, 0);
        MOQ_TEST_CHECK_EQ_INT((int)c->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT((int)sv->state, (int)MOQ_SESS_ESTABLISHED);
        moq_simpair_destroy(sp);
    }

    /* == SUBSCRIBE_NAMESPACE accept+cancel churn (publisher reciprocal) = */
    {
        moq_simpair_t *sp = make_pair();
        MOQ_TEST_CHECK(sp != NULL);
        moq_session_t *c = moq_simpair_client(sp);
        moq_session_t *sv = moq_simpair_server(sp);
        bool ok = true;
        for (int i = 0; i < CYCLES && ok; i++) {
            char name[24];
            snprintf(name, sizeof(name), "can%d", i);
            moq_subscribe_namespace_cfg_t cfg;
            moq_subscribe_namespace_cfg_init(&cfg);
            cfg.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
            moq_bytes_t parts[] = { { (const uint8_t *)name, strlen(name) } };
            cfg.track_namespace_prefix = (moq_namespace_t){ parts, 1 };
            moq_ns_sub_handle_t ch;
            if (moq_session_subscribe_namespace(c, &cfg,
                    moq_simpair_now_us(sp), &ch) != MOQ_OK) { ok = false; break; }
            if (moq_simpair_run_until_quiescent(sp, 32, NULL) < 0) { ok = false; break; }
            moq_ns_sub_handle_t sh = { 0 };
            moq_event_t ev;
            while (moq_session_poll_events(sv, &ev, 1) > 0) {
                if (ev.kind == MOQ_EVENT_NS_SUB_REQUEST)
                    sh = ev.u.ns_sub_request.handle;
                moq_event_cleanup(&ev);
            }
            if (sh._opaque == 0) { ok = false; break; }
            moq_accept_ns_sub_cfg_t ac;
            moq_accept_ns_sub_cfg_init(&ac);
            if (moq_session_accept_ns_sub(sv, sh, &ac,
                    moq_simpair_now_us(sp)) != MOQ_OK) { ok = false; break; }
            if (moq_simpair_run_until_quiescent(sp, 32, NULL) < 0) { ok = false; break; }
            drain_events(c);
            /* The subscriber cancels the established subscription (graceful FIN);
             * the publisher reciprocates so both halves close and the slot frees. */
            if (moq_session_cancel_namespace_sub(c, ch,
                    moq_simpair_now_us(sp)) != MOQ_OK) { ok = false; break; }
            if (moq_simpair_run_until_quiescent(sp, 32, NULL) < 0) { ok = false; break; }
            drain_events(sv);
        }
        MOQ_TEST_CHECK(ok);
        MOQ_TEST_CHECK_EQ_SIZE(c->drain_ref_count, 0);
        MOQ_TEST_CHECK_EQ_SIZE(sv->drain_ref_count, 0);
        MOQ_TEST_CHECK_EQ_INT((int)c->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT((int)sv->state, (int)MOQ_SESS_ESTABLISHED);
        moq_simpair_destroy(sp);
    }

    /* == SUBSCRIBE_TRACKS reject churn (requester auto-closes) ========= */
    {
        moq_simpair_t *sp = make_pair();
        MOQ_TEST_CHECK(sp != NULL);
        moq_session_t *c = moq_simpair_client(sp);
        moq_session_t *sv = moq_simpair_server(sp);
        bool ok = true;
        for (int i = 0; i < CYCLES && ok; i++) {
            char name[24];
            snprintf(name, sizeof(name), "trk%d", i);
            moq_subscribe_tracks_cfg_t cfg;
            moq_subscribe_tracks_cfg_init(&cfg);
            moq_bytes_t parts[] = { { (const uint8_t *)name, strlen(name) } };
            cfg.track_namespace_prefix = (moq_namespace_t){ parts, 1 };
            moq_track_sub_handle_t ch;
            if (moq_session_subscribe_tracks(c, &cfg,
                    moq_simpair_now_us(sp), &ch) != MOQ_OK) { ok = false; break; }
            if (moq_simpair_run_until_quiescent(sp, 32, NULL) < 0) { ok = false; break; }
            moq_track_sub_handle_t sh = { 0 };
            moq_event_t ev;
            while (moq_session_poll_events(sv, &ev, 1) > 0) {
                if (ev.kind == MOQ_EVENT_SUBSCRIBE_TRACKS_REQUEST)
                    sh = ev.u.subscribe_tracks_request.handle;
                moq_event_cleanup(&ev);
            }
            if (sh._opaque == 0) { ok = false; break; }
            moq_reject_subscribe_tracks_cfg_t rej;
            moq_reject_subscribe_tracks_cfg_init(&rej);
            rej.error_code = MOQ_REQUEST_ERROR_UNAUTHORIZED;
            if (moq_session_reject_subscribe_tracks(sv, sh, &rej,
                    moq_simpair_now_us(sp)) != MOQ_OK) { ok = false; break; }
            if (moq_simpair_run_until_quiescent(sp, 32, NULL) < 0) { ok = false; break; }
            drain_events(c);
        }
        MOQ_TEST_CHECK(ok);
        MOQ_TEST_CHECK_EQ_SIZE(c->drain_ref_count, 0);
        MOQ_TEST_CHECK_EQ_SIZE(sv->drain_ref_count, 0);
        MOQ_TEST_CHECK_EQ_INT((int)c->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT((int)sv->state, (int)MOQ_SESS_ESTABLISHED);
        moq_simpair_destroy(sp);
    }

    /* == TRACK_STATUS churn (opener opens with FIN; symmetric close) === */
    {
        moq_simpair_t *sp = make_pair();
        MOQ_TEST_CHECK(sp != NULL);
        moq_session_t *c = moq_simpair_client(sp);
        moq_session_t *sv = moq_simpair_server(sp);
        bool ok = true;
        for (int i = 0; i < CYCLES && ok; i++) {
            moq_track_status_cfg_t cfg;
            moq_track_status_cfg_init(&cfg);
            moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
            cfg.track_namespace = (moq_namespace_t){ parts, 1 };
            cfg.track_name = MOQ_BYTES_LITERAL("v");
            moq_track_status_handle_t ch;
            if (moq_session_track_status(c, &cfg,
                    moq_simpair_now_us(sp), &ch) != MOQ_OK) { ok = false; break; }
            if (moq_simpair_run_until_quiescent(sp, 32, NULL) < 0) { ok = false; break; }
            moq_track_status_handle_t sh = { 0 };
            moq_event_t ev;
            while (moq_session_poll_events(sv, &ev, 1) > 0) {
                if (ev.kind == MOQ_EVENT_TRACK_STATUS_REQUEST)
                    sh = ev.u.track_status_request.handle;
                moq_event_cleanup(&ev);
            }
            if (sh._opaque == 0) { ok = false; break; }
            moq_accept_track_status_cfg_t ac;
            moq_accept_track_status_cfg_init(&ac);
            if (moq_session_accept_track_status(sv, sh, &ac,
                    moq_simpair_now_us(sp)) != MOQ_OK) { ok = false; break; }
            if (moq_simpair_run_until_quiescent(sp, 32, NULL) < 0) { ok = false; break; }
            drain_events(c);
        }
        MOQ_TEST_CHECK(ok);
        MOQ_TEST_CHECK_EQ_SIZE(c->drain_ref_count, 0);
        MOQ_TEST_CHECK_EQ_SIZE(sv->drain_ref_count, 0);
        MOQ_TEST_CHECK_EQ_INT((int)c->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT((int)sv->state, (int)MOQ_SESS_ESTABLISHED);
        moq_simpair_destroy(sp);
    }

    MOQ_TEST_PASS("sim_bidi_lifecycle");
    return failures != 0;
}
