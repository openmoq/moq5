/*
 * Opaque transport-key allocation — EXTERNALLY VISIBLE behavior, MsQuic-free.
 *
 * This drives the adapter's real endpoint entry point ep_open_uni() and the
 * peer-open handler ev_stream_opened(), not just the internal allocator, so
 * it catches the distinctions that matter to the bridge:
 *   - table pressure (a full 16-slot table) -> MOQ_TRANSPORT_WOULD_BLOCK
 *     (retryable), and the key is NEVER UINT64_MAX;
 *   - key exhaustion -> MOQ_TRANSPORT_ERROR (fatal, terminates the
 *     connection) — NOT an endless WOULD_BLOCK open-and-abort retry loop;
 *   - freeing a slot yields a NEW key (never reused), so a late event on
 *     the old key cannot collide;
 *   - the peer-open table-pressure path is NONFATAL (aborts the stream).
 *
 * It is MsQuic-free: the wtq_* transport calls the adapter makes are STUBBED
 * here, so the test links neither libwtquic nor libmsquic (only moq::core +
 * the wtquic headers for declarations). The adapter translation unit is
 * included directly to exercise its static endpoint functions.
 */
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include <wtquic/wtquic.h>

#include <moq/session.h>
#include <moq/transport_bridge.h>
#include "../../../core/src/session/session_internal.h"

/* -- wtq_* stubs: a minimal fake transport ---------------------------------- */

struct wtq_stream {
    uint64_t native_id;
    bool bidi;
    int aborts;
    int pauses;         /* wtq_stream_pause_receive calls on this stream */
    int resumes;        /* wtq_stream_resume_receive calls on this stream */
};
/* Forced return codes for the next pause/resume calls (WTQ_OK unless a case
 * overrides one to exercise the STATE/CLOSED/unexpected-failure branches). */
static wtq_result_t g_resume_rc = WTQ_OK;
static wtq_result_t g_pause_rc = WTQ_OK;
static struct wtq_stream g_streams[64];
static int g_stream_n;
static bool g_open_fail; /* force the transport to refuse an open */

static struct wtq_stream *new_stream(bool bidi)
{
    struct wtq_stream *st = &g_streams[g_stream_n];
    st->native_id = WTQ_STREAM_ID_UNKNOWN; /* async transport: never known */
    st->bidi = bidi;
    st->aborts = 0;
    g_stream_n++;
    return st;
}

wtq_result_t wtq_session_open_uni(wtq_session_t *s, wtq_stream_t **out)
{
    (void)s;
    if (g_open_fail) return WTQ_ERR_WOULD_BLOCK;
    *out = new_stream(false);
    return WTQ_OK;
}
wtq_result_t wtq_session_open_bidi(wtq_session_t *s, wtq_stream_t **out)
{
    (void)s;
    if (g_open_fail) return WTQ_ERR_WOULD_BLOCK;
    *out = new_stream(true);
    return WTQ_OK;
}
uint64_t wtq_stream_id(const wtq_stream_t *st)
{
    return st ? st->native_id : WTQ_STREAM_ID_UNKNOWN;
}
/* Reentrancy harness: when armed, wtq_stream_abort synchronously raises
 * on_stream_closed (as MsQuic does when the abort completes the stream),
 * exactly the path that used to re-enter the bridge from an endpoint op.
 * The synchronous-terminal work lives in a hook defined AFTER the adapter
 * TU (it needs the adapter's types), forward-declared here. */
static bool g_raise_terminal;
static void test_abort_terminal_hook(wtq_stream_t *st);

wtq_result_t wtq_stream_abort(wtq_stream_t *st, uint32_t code)
{
    (void)code;
    if (st) st->aborts++;
    if (g_raise_terminal)
        test_abort_terminal_hook(st);
    return WTQ_OK;
}
bool wtq_stream_is_bidi(const wtq_stream_t *st) { return st && st->bidi; }
wtq_result_t wtq_stream_reset(wtq_stream_t *st, uint32_t c) { (void)st; (void)c; return WTQ_OK; }
wtq_result_t wtq_stream_stop_sending(wtq_stream_t *st, uint32_t c) { (void)st; (void)c; return WTQ_OK; }
wtq_result_t wtq_stream_send(wtq_stream_t *st, const wtq_span_t *sp, size_t n,
                             uint32_t f, void *ctx)
{ (void)st; (void)sp; (void)n; (void)f; (void)ctx; return WTQ_OK; }
wtq_result_t wtq_stream_pause_receive(wtq_stream_t *st)
{ if (st) st->pauses++; return g_pause_rc; }
wtq_result_t wtq_stream_resume_receive(wtq_stream_t *st)
{ if (st) st->resumes++; return g_resume_rc; }
void wtq_session_events_init(wtq_session_events_t *e) { if (e) memset(e, 0, sizeof(*e)); }
static int g_session_closes;
wtq_result_t wtq_session_close(wtq_session_t *s, uint32_t c, const uint8_t *r,
                               size_t rl)
{ (void)s; (void)c; (void)r; (void)rl; g_session_closes++; return WTQ_OK; }

/* Reach the adapter's static endpoint functions. */
#include "../wtquic_adapter.c"

static moq_wtquic_conn_t *g_conn;
static uint64_t g_abort_key;
static int g_intact_during_op; /* -1 unset; 1 mapping intact; 0 retired */
static int g_terminal_raised;  /* synchronous on_stream_closed firings */
static size_t g_queued_after;  /* queue depth right after the firing   */

static void test_abort_terminal_hook(wtq_stream_t *st)
{
    if (g_conn == NULL)
        return;
    /* synchronous terminal from INSIDE the endpoint op */
    ev_stream_closed((wtq_session_t *)(void *)0x1, st, g_conn);
    g_terminal_raised++;
    g_queued_after = g_conn->term_count;
    /* the bridge mapping MUST still be present here: the terminal was
     * deferred, not fed to the bridge during the endpoint op */
    g_intact_during_op =
        (moq_transport_bridge_find_ref(g_conn->bridge, g_abort_key)._v != 0)
            ? 1 : 0;
}

#define CHECK(expr)                                                       \
    do {                                                                  \
        if (!(expr)) {                                                    \
            printf("FAIL %s:%d: %s\n", __FILE__, __LINE__, #expr);        \
            failures++;                                                   \
        }                                                                 \
    } while (0)

int main(void)
{
    int failures = 0;
    moq_wtquic_conn_t c;

    /* A conn with just enough state for ep_open_uni: a non-NULL session so
     * the "not bound yet" guard passes; the uni path never derefs bridge. */
    memset(&c, 0, sizeof(c));
    c.next_stream_key = 1;
    c.ws = (wtq_session_t *)(void *)0x1; /* sentinel: only checked != NULL */

    /* Fill the 16-slot table via the REAL endpoint op. Every id distinct,
     * monotonic, and never the reserved UINT64_MAX sentinel. */
    uint64_t ids[MOQ_WTQ_MAX_STREAMS];
    for (int i = 0; i < MOQ_WTQ_MAX_STREAMS; i++) {
        uint64_t id = 0xDEAD;
        CHECK(ep_open_uni(&c, &id) == MOQ_TRANSPORT_OK);
        CHECK(id != UINT64_MAX);
        ids[i] = id;
        for (int j = 0; j < i; j++)
            CHECK(ids[i] != ids[j]);
    }

    /* Table pressure: the 17th open is retryable WOULD_BLOCK, NOT an error,
     * and issues no key. */
    {
        uint64_t id = 0xBEEF;
        CHECK(ep_open_uni(&c, &id) == MOQ_TRANSPORT_WOULD_BLOCK);
        CHECK(id == 0xBEEF); /* out_id untouched */
    }

    /* Free a slot: the next open reuses the SLOT but gets a NEW key. */
    {
        c.streams[5].in_use = false;
        uint64_t id = 0;
        CHECK(ep_open_uni(&c, &id) == MOQ_TRANSPORT_OK);
        CHECK(id != UINT64_MAX);
        for (int i = 0; i < MOQ_WTQ_MAX_STREAMS; i++)
            CHECK(id != ids[i]); /* never a reused key */
    }

    /* Key exhaustion is FATAL, not retryable: MOQ_TRANSPORT_ERROR (the
     * bridge turns this into a connection teardown), never WOULD_BLOCK,
     * and never issues UINT64_MAX. One free slot rules out table pressure. */
    {
        c.streams[7].in_use = false;
        c.next_stream_key = UINT64_MAX;
        uint64_t id = 0xF00D;
        CHECK(ep_open_uni(&c, &id) == MOQ_TRANSPORT_ERROR);
        CHECK(id == 0xF00D); /* out_id untouched: no key issued */
    }

    /* Peer-open TABLE PRESSURE is NONFATAL: the new stream is aborted and
     * ev_stream_opened returns without touching the bridge. */
    {
        moq_wtquic_conn_t p;
        memset(&p, 0, sizeof(p));
        p.next_stream_key = 1;
        p.ws = NULL;
        g_stream_n = 0;
        for (int i = 0; i < MOQ_WTQ_MAX_STREAMS; i++)
            p.streams[i].in_use = true; /* table already full */
        struct wtq_stream *peer = new_stream(false);
        peer->native_id = 3;
        ev_stream_opened((wtq_session_t *)(void *)0x1, peer, false, &p);
        CHECK(peer->aborts == 1);        /* stream aborted */
    }

    /* Peer-open KEY EXHAUSTION is FATAL. Drive the REAL branch with a real
     * moq session + bridge: the stream is aborted, the bridge becomes
     * FATAL (not cleanly closed), exactly one session close is issued, a
     * repeated callback cannot initiate another, and no key is handed out. */
    {
        moq_session_cfg_t scfg;
        moq_session_t *sess = NULL;
        moq_session_cfg_init_sized(&scfg, sizeof(scfg), moq_alloc_default(),
                                   MOQ_PERSPECTIVE_SERVER);
        CHECK(moq_session_create(&scfg, 0, &sess) == MOQ_OK);

        /* A fully-wired conn (real bridge with the adapter's own ops). */
        moq_wtquic_conn_cfg_t ccfg;
        moq_wtquic_conn_cfg_init_sized(&ccfg, sizeof(ccfg));
        ccfg.alloc = moq_alloc_default();
        ccfg.session = sess;
        moq_wtquic_conn_t *pp = NULL;
        CHECK(moq_wtquic_conn_create(&ccfg, &pp) == MOQ_OK);
        pp->next_stream_key = UINT64_MAX; /* exhausted */

        g_stream_n = 0;
        g_session_closes = 0;
        struct wtq_stream *peer = new_stream(false);
        peer->native_id = 5;
        ev_stream_opened((wtq_session_t *)(void *)0x1, peer, false, pp);

        CHECK(peer->aborts == 1);                    /* stream aborted   */
        CHECK(moq_transport_bridge_is_fatal(pp->bridge));   /* FATAL     */
        CHECK(!moq_transport_bridge_is_closed(pp->bridge)); /* not clean */
        CHECK(g_session_closes == 1);                /* exactly one close */
        /* NO opaque key was issued: no adapter slot references the
         * stream, none became active, and the counter never moved. */
        for (int i = 0; i < MOQ_WTQ_MAX_STREAMS; i++) {
            CHECK(pp->streams[i].st != (wtq_stream_t *)peer);
            CHECK(!pp->streams[i].in_use);
        }
        CHECK(pp->next_stream_key == UINT64_MAX);

        /* a repeated callback cannot initiate a second close */
        struct wtq_stream *peer2 = new_stream(false);
        peer2->native_id = 9;
        ev_stream_opened((wtq_session_t *)(void *)0x1, peer2, false, pp);
        CHECK(peer2->aborts == 1);
        CHECK(g_session_closes == 1);                /* still exactly one */
        for (int i = 0; i < MOQ_WTQ_MAX_STREAMS; i++) {
            CHECK(pp->streams[i].st != (wtq_stream_t *)peer2);
            CHECK(!pp->streams[i].in_use);
        }
        CHECK(pp->next_stream_key == UINT64_MAX);

        moq_wtquic_conn_destroy(pp);
        moq_session_destroy(sess);
    }

    /* Deferred stream-terminal: a synchronous on_stream_closed fired from
     * inside the abort endpoint op must NOT re-enter the bridge. The
     * aborting mapping stays intact during the op, is retired afterward by
     * the drain, and the terminal is delivered exactly once. */
    {
        moq_session_cfg_t scfg;
        moq_session_t *sess = NULL;
        moq_session_cfg_init_sized(&scfg, sizeof(scfg), moq_alloc_default(),
                                   MOQ_PERSPECTIVE_CLIENT);
        CHECK(moq_session_create(&scfg, 0, &sess) == MOQ_OK);
        moq_wtquic_conn_cfg_t ccfg;
        moq_wtquic_conn_cfg_init_sized(&ccfg, sizeof(ccfg));
        ccfg.alloc = moq_alloc_default();
        ccfg.session = sess;
        moq_wtquic_conn_t *cc = NULL;
        CHECK(moq_wtquic_conn_create(&ccfg, &cc) == MOQ_OK);
        cc->ws = (wtq_session_t *)(void *)0x1; /* "bound": teardown guard */
        g_conn = cc;
        g_stream_n = 0;

        /* 1. Open a request bidi through the real bridge -> ep_open_bidi. */
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0x9100);
        static const uint8_t rb[] = { 0xAB };
        moq_action_t a;
        memset(&a, 0, sizeof(a));
        a.kind = MOQ_ACTION_OPEN_BIDI_STREAM;
        a.detail_size = (uint32_t)sizeof(moq_open_bidi_stream_action_t);
        a.borrow_epoch = sess->borrow_epoch;
        a.u.open_bidi_stream.stream_ref = ref;
        a.u.open_bidi_stream.data = rb;
        a.u.open_bidi_stream.len = sizeof(rb);
        CHECK(push_action(sess, &a) == MOQ_OK);
        conn_service(cc);
        /* the adapter registered exactly one slot; that's the abort target */
        int active = 0;
        for (int i = 0; i < MOQ_WTQ_MAX_STREAMS; i++)
            if (cc->streams[i].in_use) { g_abort_key = cc->streams[i].id; active++; }
        CHECK(active == 1);

        /* 2. Abort it: arm the synchronous terminal, then service. */
        g_raise_terminal = true;
        g_intact_during_op = -1;
        memset(&a, 0, sizeof(a));
        a.kind = MOQ_ACTION_ABORT_BIDI_STREAM;
        a.detail_size = (uint32_t)sizeof(moq_abort_bidi_stream_action_t);
        a.borrow_epoch = sess->borrow_epoch;
        a.u.abort_bidi_stream.stream_ref = ref;
        a.u.abort_bidi_stream.error_code = 0x1;
        CHECK(push_action(sess, &a) == MOQ_OK);
        conn_service(cc);

        /* the synchronous terminal ran EXACTLY ONCE, queued exactly one
         * entry, and the mapping was INTACT during the endpoint op */
        CHECK(g_terminal_raised == 1);
        CHECK(g_queued_after == 1);
        CHECK(g_intact_during_op == 1);
        /* afterward: the drain delivered it exactly once (queue emptied,
         * mapping retired) and the adapter slot was cleared */
        CHECK(cc->term_count == 0);
        CHECK(!cc->term_overflow);
        CHECK(moq_transport_bridge_find_ref(cc->bridge, g_abort_key)._v == 0);
        int still = 0;
        for (int i = 0; i < MOQ_WTQ_MAX_STREAMS; i++)
            if (cc->streams[i].in_use) still++;
        CHECK(still == 0);

        /* 3. MULTIPLE synchronous terminals before one drain: two streams
         * aborted in the same service pass — both terminals queue, none
         * is lost, both mappings retire, no duplicate delivery. */
        moq_stream_ref_t r1 = moq_stream_ref_from_u64(0x9200);
        moq_stream_ref_t r2 = moq_stream_ref_from_u64(0x9300);
        static const uint8_t rb2[] = { 0xCD };
        g_raise_terminal = false; /* open cleanly first */
        memset(&a, 0, sizeof(a));
        a.kind = MOQ_ACTION_OPEN_BIDI_STREAM;
        a.detail_size = (uint32_t)sizeof(moq_open_bidi_stream_action_t);
        a.borrow_epoch = sess->borrow_epoch;
        a.u.open_bidi_stream.stream_ref = r1;
        a.u.open_bidi_stream.data = rb2;
        a.u.open_bidi_stream.len = sizeof(rb2);
        CHECK(push_action(sess, &a) == MOQ_OK);
        a.u.open_bidi_stream.stream_ref = r2;
        CHECK(push_action(sess, &a) == MOQ_OK);
        conn_service(cc);
        uint64_t k1 = 0, k2 = 0;
        for (int i = 0; i < MOQ_WTQ_MAX_STREAMS; i++)
            if (cc->streams[i].in_use) {
                if (k1 == 0) k1 = cc->streams[i].id;
                else k2 = cc->streams[i].id;
            }
        CHECK(k1 != 0 && k2 != 0 && k1 != k2);

        g_raise_terminal = true;
        g_terminal_raised = 0;
        g_abort_key = k1; /* the hook watches the FIRST aborted mapping */
        memset(&a, 0, sizeof(a));
        a.kind = MOQ_ACTION_ABORT_BIDI_STREAM;
        a.detail_size = (uint32_t)sizeof(moq_abort_bidi_stream_action_t);
        a.borrow_epoch = sess->borrow_epoch;
        a.u.abort_bidi_stream.stream_ref = r1;
        a.u.abort_bidi_stream.error_code = 0x1;
        CHECK(push_action(sess, &a) == MOQ_OK);
        a.u.abort_bidi_stream.stream_ref = r2;
        CHECK(push_action(sess, &a) == MOQ_OK);
        conn_service(cc);

        CHECK(g_terminal_raised == 2);   /* both fired synchronously */
        CHECK(g_intact_during_op == 1);  /* first mapping intact through
                                          * the SECOND op too (no drain
                                          * inside any endpoint frame) */
        CHECK(cc->term_count == 0);      /* both delivered, none lost */
        CHECK(!cc->term_overflow);
        CHECK(moq_transport_bridge_find_ref(cc->bridge, k1)._v == 0);
        CHECK(moq_transport_bridge_find_ref(cc->bridge, k2)._v == 0);
        still = 0;
        for (int i = 0; i < MOQ_WTQ_MAX_STREAMS; i++)
            if (cc->streams[i].in_use) still++;
        CHECK(still == 0);

        g_raise_terminal = false;
        g_conn = NULL;
        moq_wtquic_conn_destroy(cc);
        moq_session_destroy(sess);
    }

    /* Terminal-queue overflow is FATAL, never a silent drop: with the
     * queue artificially full, one more synchronous close latches the
     * overflow flag and the next service pass fails the connection. */
    {
        moq_session_cfg_t scfg;
        moq_session_t *sess = NULL;
        moq_session_cfg_init_sized(&scfg, sizeof(scfg), moq_alloc_default(),
                                   MOQ_PERSPECTIVE_CLIENT);
        CHECK(moq_session_create(&scfg, 0, &sess) == MOQ_OK);
        moq_wtquic_conn_cfg_t ccfg;
        moq_wtquic_conn_cfg_init_sized(&ccfg, sizeof(ccfg));
        ccfg.alloc = moq_alloc_default();
        ccfg.session = sess;
        moq_wtquic_conn_t *cc = NULL;
        CHECK(moq_wtquic_conn_create(&ccfg, &cc) == MOQ_OK);
        cc->ws = (wtq_session_t *)(void *)0x1;

        g_stream_n = 0;
        struct wtq_stream *st = new_stream(true);
        cc->streams[0].in_use = true;
        cc->streams[0].st = (wtq_stream_t *)st;
        cc->streams[0].id = 1;
        cc->term_count = MOQ_WTQ_MAX_STREAMS; /* queue already full */

        g_session_closes = 0;
        cc->in_service = true; /* hold the drain to observe the latch */
        ev_stream_closed((wtq_session_t *)(void *)0x1,
                         (wtq_stream_t *)st, cc);
        CHECK(cc->term_overflow);                 /* latched, not dropped */
        cc->in_service = false;
        conn_service(cc);
        CHECK(!cc->term_overflow);                /* consumed by the drain */
        CHECK(moq_transport_bridge_is_fatal(cc->bridge)); /* drain failed
                                                   * the connection */
        CHECK(g_session_closes == 1);             /* one teardown */

        moq_wtquic_conn_destroy(cc);
        moq_session_destroy(sess);
    }

    /* Paused-stream reconciliation on the service path: a stream paused on its
     * receive callback is resumed by a LATER service that clears its bridge
     * input — the only place that can, since the paused stream delivers no
     * further receive callback of its own. */
    {
        moq_session_cfg_t scfg;
        moq_session_t *sess = NULL;
        moq_session_cfg_init_sized(&scfg, sizeof(scfg), moq_alloc_default(),
                                   MOQ_PERSPECTIVE_CLIENT);
        CHECK(moq_session_create(&scfg, 0, &sess) == MOQ_OK);
        moq_wtquic_conn_cfg_t ccfg;
        moq_wtquic_conn_cfg_init_sized(&ccfg, sizeof(ccfg));
        ccfg.alloc = moq_alloc_default();
        ccfg.session = sess;
        moq_wtquic_conn_t *cc = NULL;
        CHECK(moq_wtquic_conn_create(&ccfg, &cc) == MOQ_OK);
        cc->ws = (wtq_session_t *)(void *)0x1;
        g_stream_n = 0;

        /* PAUSE return-code matrix (pause_stream_for_backpressure): `paused` is
         * published only on a CONFIRMED suppression, so a failed pause never
         * leaves the adapter believing delivery is suppressed. */
        struct wtq_stream *pz = new_stream(false);
        cc->streams[3].in_use = true;
        cc->streams[3].st = (wtq_stream_t *)pz;
        cc->streams[3].id = 909;

        /* (a) WTQ_OK: suppression confirmed -> paused published. */
        g_pause_rc = WTQ_OK;
        pause_stream_for_backpressure(cc, &cc->streams[3],
                                      (wtq_stream_t *)pz);
        CHECK(pz->pauses == 1);
        CHECK(cc->streams[3].paused);
        /* already paused: a second call is a no-op (no duplicate pause) */
        pause_stream_for_backpressure(cc, &cc->streams[3],
                                      (wtq_stream_t *)pz);
        CHECK(pz->pauses == 1);
        cc->streams[3].paused = false;

        /* (b) WTQ_ERR_STATE / (c) WTQ_ERR_CLOSED: the receive direction is
         * already finished — nothing to suppress, so paused stays FALSE and the
         * bridge is not failed. */
        g_pause_rc = WTQ_ERR_STATE;
        pause_stream_for_backpressure(cc, &cc->streams[3],
                                      (wtq_stream_t *)pz);
        CHECK(pz->pauses == 2);
        CHECK(!cc->streams[3].paused);
        CHECK(!moq_transport_bridge_is_fatal(cc->bridge));

        g_pause_rc = WTQ_ERR_CLOSED;
        pause_stream_for_backpressure(cc, &cc->streams[3],
                                      (wtq_stream_t *)pz);
        CHECK(pz->pauses == 3);
        CHECK(!cc->streams[3].paused);
        CHECK(!moq_transport_bridge_is_fatal(cc->bridge));

        /* An injected paused stream whose id is NOT a bridge stream reports no
         * pending, modelling a pause whose input cleared on THIS service. */
        struct wtq_stream *st = new_stream(false);
        cc->streams[1].in_use = true;
        cc->streams[1].st = (wtq_stream_t *)st;
        cc->streams[1].id = 4242;
        cc->streams[1].paused = true;

        /* (a) a service call resumes it EXACTLY once, clearing local pause. */
        g_resume_rc = WTQ_OK;
        conn_service(cc);
        CHECK(st->resumes == 1);
        CHECK(!cc->streams[1].paused);
        /* (b) a subsequent service does NOT resume again. */
        conn_service(cc);
        CHECK(st->resumes == 1);
        CHECK(!moq_transport_bridge_is_fatal(cc->bridge));

        /* (c) WTQ_ERR_STATE (incoming already finished): clear pause, no retry. */
        cc->streams[1].paused = true;
        g_resume_rc = WTQ_ERR_STATE;
        conn_service(cc);
        CHECK(st->resumes == 2);
        CHECK(!cc->streams[1].paused);
        conn_service(cc);
        CHECK(st->resumes == 2);
        CHECK(!moq_transport_bridge_is_fatal(cc->bridge));

        /* (d) WTQ_ERR_CLOSED (stream/session ended): clear pause, no retry. */
        cc->streams[1].paused = true;
        g_resume_rc = WTQ_ERR_CLOSED;
        conn_service(cc);
        CHECK(st->resumes == 3);
        CHECK(!cc->streams[1].paused);
        conn_service(cc);
        CHECK(st->resumes == 3);
        CHECK(!moq_transport_bridge_is_fatal(cc->bridge));

        /* (d) an UNEXPECTED pause failure (a backend that cannot honor the
         * inbound-backpressure contract) fails the bridge rather than pretending
         * delivery is suppressed: paused stays FALSE and the conn is failed.
         * Checked on its own conn so the resume cases above stay unaffected. */
        {
            moq_session_cfg_t pcfg;
            moq_session_t *psess = NULL;
            moq_session_cfg_init_sized(&pcfg, sizeof(pcfg), moq_alloc_default(),
                                       MOQ_PERSPECTIVE_CLIENT);
            CHECK(moq_session_create(&pcfg, 0, &psess) == MOQ_OK);
            moq_wtquic_conn_cfg_t pccfg;
            moq_wtquic_conn_cfg_init_sized(&pccfg, sizeof(pccfg));
            pccfg.alloc = moq_alloc_default();
            pccfg.session = psess;
            moq_wtquic_conn_t *pc = NULL;
            CHECK(moq_wtquic_conn_create(&pccfg, &pc) == MOQ_OK);
            pc->ws = (wtq_session_t *)(void *)0x1;
            struct wtq_stream *fs = new_stream(false);
            pc->streams[0].in_use = true;
            pc->streams[0].st = (wtq_stream_t *)fs;
            pc->streams[0].id = 5150;

            g_pause_rc = WTQ_ERR_WOULD_BLOCK;   /* unexpected on pause */
            pause_stream_for_backpressure(pc, &pc->streams[0],
                                          (wtq_stream_t *)fs);
            CHECK(fs->pauses == 1);
            CHECK(!pc->streams[0].paused);      /* never claimed suppression */
            CHECK(moq_transport_bridge_is_fatal(pc->bridge));
            g_pause_rc = WTQ_OK;

            moq_wtquic_conn_destroy(pc);
            moq_session_destroy(psess);
        }

        /* (e) an UNEXPECTED resume failure makes the bridge fatal rather than
         * silently stranding the stream: pause stays set and the conn fails. */
        cc->streams[1].paused = true;
        g_resume_rc = WTQ_ERR_WOULD_BLOCK;
        conn_service(cc);
        CHECK(st->resumes == 4);
        CHECK(cc->streams[1].paused);
        CHECK(moq_transport_bridge_is_fatal(cc->bridge));

        g_resume_rc = WTQ_OK;
        moq_wtquic_conn_destroy(cc);
        moq_session_destroy(sess);
    }

    if (failures == 0)
        printf("PASS test_wtquic_keys\n");
    return failures;
}
