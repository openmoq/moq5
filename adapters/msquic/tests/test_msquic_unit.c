/*
 * White-box units for the msquic attach adapter, over the fake
 * QUIC_API_TABLE: stream-id self-computation, the in-flight send
 * budget (depth throttle, idle-admit, wrap pin), send-record ownership
 * (rcbuf borrow until SEND_COMPLETE, cold-write copies, pool reuse),
 * credit gating, idempotent reset/stop, and the connected-opens-
 * control lifecycle through a real session and bridge. No real
 * transport, no timing.
 */

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "msquic_internal.h"

#include "support/fake_msq_table.h"
#include "support/fake_endpoint.h" /* raw peer for the receive-pending pair */

#include <moq/rcbuf.h>
#include <moq/session.h>
#include <moq/transport_bridge.h>

static int failures = 0;

#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

/* --- rig ---------------------------------------------------------------------- */

struct rig {
    fake_msq_t fake;
    moq_session_t *session;
    moq_msquic_conn_t *conn;
    const moq_transport_endpoint_ops_t *ops;
};

static int rig_up_hooked(struct rig *r, moq_perspective_t persp,
                         moq_msquic_hook_fn hook, void *hook_user)
{
    moq_session_cfg_t scfg;

    memset(r, 0, sizeof(*r));
    fake_msq_init(&r->fake, persp == MOQ_PERSPECTIVE_CLIENT);

    moq_session_cfg_init_sized(&scfg, sizeof(scfg), moq_alloc_default(),
                               persp);
    if (moq_session_create(&scfg, 0, &r->session) < 0)
        return -1;

    moq_msquic_conn_cfg_t cfg;
    moq_msquic_conn_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.session = r->session;
    cfg.api = fake_msq_table(&r->fake);
    cfg.hook = hook;
    cfg.hook_user = hook_user;
    if (moq_msquic_conn_create(&cfg, &r->conn) != MOQ_OK)
        return -1;
    if (moq_msquic_conn_bind(r->conn, fake_msq_conn_handle(&r->fake)) !=
        MOQ_OK)
        return -1;
    r->ops = moq_msquic_test_ops(r->conn);
    return 0;
}

static int rig_up(struct rig *r, moq_perspective_t persp)
{
    return rig_up_hooked(r, persp, NULL, NULL);
}

/* Like rig_up but with a bounded session event queue, so inbound objects
 * back the bridge up (drive receive backpressure). */
static int rig_up_events(struct rig *r, moq_perspective_t persp,
                         uint32_t max_events)
{
    moq_session_cfg_t scfg;

    memset(r, 0, sizeof(*r));
    fake_msq_init(&r->fake, persp == MOQ_PERSPECTIVE_CLIENT);
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), moq_alloc_default(),
                               persp);
    scfg.max_events = max_events;
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 16;
    if (moq_session_create(&scfg, 0, &r->session) < 0)
        return -1;

    moq_msquic_conn_cfg_t cfg;
    moq_msquic_conn_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.session = r->session;
    cfg.api = fake_msq_table(&r->fake);
    if (moq_msquic_conn_create(&cfg, &r->conn) != MOQ_OK)
        return -1;
    if (moq_msquic_conn_bind(r->conn, fake_msq_conn_handle(&r->fake)) !=
        MOQ_OK)
        return -1;
    r->ops = moq_msquic_test_ops(r->conn);
    return 0;
}

static void rig_down(struct rig *r)
{
    if (r->conn != NULL)
        moq_msquic_conn_destroy(r->conn);
    if (r->session != NULL)
        moq_session_destroy(r->session);
}

static void deliver_connected(struct rig *r)
{
    QUIC_CONNECTION_EVENT ev;

    memset(&ev, 0, sizeof(ev));
    ev.Type = QUIC_CONNECTION_EVENT_CONNECTED;
    moq_msquic_conn_callback()(fake_msq_conn_handle(&r->fake), r->conn,
                               &ev);
}

static void deliver_streams_available(struct rig *r, uint16_t bidi,
                                      uint16_t uni)
{
    QUIC_CONNECTION_EVENT ev;

    memset(&ev, 0, sizeof(ev));
    ev.Type = QUIC_CONNECTION_EVENT_STREAMS_AVAILABLE;
    ev.STREAMS_AVAILABLE.BidirectionalCount = bidi;
    ev.STREAMS_AVAILABLE.UnidirectionalCount = uni;
    moq_msquic_conn_callback()(fake_msq_conn_handle(&r->fake), r->conn,
                               &ev);
}

static moq_rcbuf_t *make_rcbuf(size_t len)
{
    uint8_t *bytes = malloc(len);
    moq_rcbuf_t *buf = NULL;

    memset(bytes, 0x5a, len);
    if (moq_rcbuf_create(moq_alloc_default(), bytes, len, &buf) < 0)
        buf = NULL;
    free(bytes);
    return buf;
}

/* Register a peer-initiated stream with the adapter (as MsQuic would via
 * PEER_STREAM_STARTED) and return the fake stream to deliver receives on.
 * After this the fake stream's callback routes to the adapter's handler
 * for that stream. */
static fake_msq_stream_t *deliver_peer_stream(struct rig *r, uint64_t id,
                                              bool uni)
{
    fake_msq_stream_t *st = fake_msq_peer_stream(&r->fake, id, uni);
    QUIC_CONNECTION_EVENT ev;

    if (st == NULL)
        return NULL;
    memset(&ev, 0, sizeof(ev));
    ev.Type = QUIC_CONNECTION_EVENT_PEER_STREAM_STARTED;
    ev.PEER_STREAM_STARTED.Stream = (HQUIC)st;
    ev.PEER_STREAM_STARTED.Flags =
        uni ? QUIC_STREAM_OPEN_FLAG_UNIDIRECTIONAL
            : QUIC_STREAM_OPEN_FLAG_NONE;
    moq_msquic_conn_callback()(fake_msq_conn_handle(&r->fake), r->conn,
                               &ev);
    return st;
}

/* Deliver a parameterless stream event (e.g. PEER_SEND_ABORTED) on a
 * fake stream to the adapter's handler. */
static void deliver_stream_event(fake_msq_stream_t *st,
                                 QUIC_STREAM_EVENT_TYPE type,
                                 uint64_t error_code)
{
    QUIC_STREAM_EVENT ev;

    memset(&ev, 0, sizeof(ev));
    ev.Type = type;
    if (type == QUIC_STREAM_EVENT_PEER_SEND_ABORTED)
        ev.PEER_SEND_ABORTED.ErrorCode = error_code;
    st->cb((HQUIC)st, st->ctx, &ev);
}

/* --- config validation ---------------------------------------------------------- */

static void t_cfg_validation(void)
{
    int before = failures;
    struct rig r;
    moq_msquic_conn_t *conn = NULL;
    moq_msquic_conn_cfg_t cfg;

    CHECK(rig_up(&r, MOQ_PERSPECTIVE_CLIENT) == 0);

    CHECK(moq_msquic_conn_create(NULL, &conn) == MOQ_ERR_INVAL);
    CHECK(moq_msquic_conn_create(&cfg, NULL) == MOQ_ERR_INVAL);

    moq_msquic_conn_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.session = r.session;
    cfg.api = fake_msq_table(&r.fake);
    cfg.struct_size = sizeof(cfg) - 1; /* too small */
    CHECK(moq_msquic_conn_create(&cfg, &conn) == MOQ_ERR_INVAL);
    cfg.struct_size = sizeof(cfg);

    cfg.session = NULL;
    CHECK(moq_msquic_conn_create(&cfg, &conn) == MOQ_ERR_INVAL);
    cfg.session = r.session;
    cfg.api = NULL;
    CHECK(moq_msquic_conn_create(&cfg, &conn) == MOQ_ERR_INVAL);
    cfg.api = fake_msq_table(&r.fake);
    cfg.alloc = NULL;
    CHECK(moq_msquic_conn_create(&cfg, &conn) == MOQ_ERR_INVAL);

    /* double bind refused */
    CHECK(moq_msquic_conn_bind(r.conn, fake_msq_conn_handle(&r.fake)) ==
          MOQ_ERR_WRONG_STATE);

    rig_down(&r);
    if (failures == before)
        printf("PASS: cfg_validation\n");
}

/* --- lifecycle through the real bridge ------------------------------------------ */

/* CONNECTED starts the client session, whose SETUP flows through the
 * bridge into a real StreamOpen/StreamStart/StreamSend sequence on the
 * fake — with the control stream on the first client bidi (id 0). */
static void t_connected_opens_control(void)
{
    int before = failures;
    struct rig r;

    CHECK(rig_up(&r, MOQ_PERSPECTIVE_CLIENT) == 0);
    deliver_streams_available(&r, 8, 8);
    deliver_connected(&r);

    fake_msq_stream_t *ctrl = fake_msq_stream_at(&r.fake, 0);
    CHECK(ctrl != NULL);
    if (ctrl != NULL) {
        CHECK(!ctrl->uni);
        CHECK(ctrl->started);
        CHECK(ctrl->id == 0);
        /* the id-verification backstop is armed */
        CHECK((ctrl->start_flags & QUIC_STREAM_START_FLAG_FAIL_BLOCKED)
              != 0);
    }
    CHECK(fake_msq_pending_sends(&r.fake) >= 1);
    CHECK(r.fake.sends[0].total > 0); /* SETUP bytes */
    CHECK(!moq_msquic_conn_is_fatal(r.conn));

    /* the transport confirms the computed id */
    if (ctrl != NULL) {
        fake_msq_deliver_start_complete(ctrl, QUIC_STATUS_SUCCESS);
        CHECK(!moq_msquic_conn_is_fatal(r.conn));
    }
    rig_down(&r);
    if (failures == before)
        printf("PASS: connected_opens_control\n");
}

/* --- stream ids ------------------------------------------------------------------ */

static void t_stream_id_allocation(void)
{
    int before = failures;
    struct rig r;
    uint64_t id = 0;

    CHECK(rig_up(&r, MOQ_PERSPECTIVE_CLIENT) == 0);
    deliver_streams_available(&r, 8, 8);

    static const uint64_t uni_ids[3] = { 2, 6, 10 };
    for (int i = 0; i < 3; i++) {
        CHECK(r.ops->open_uni(r.conn, &id) == MOQ_TRANSPORT_OK);
        CHECK(id == uni_ids[i]);
        fake_msq_stream_t *st = fake_msq_stream_at(&r.fake, i);
        CHECK(st != NULL && st->id == id); /* fake agrees */
    }
    CHECK(r.ops->open_bidi(r.conn, &id) == MOQ_TRANSPORT_OK);
    CHECK(id == 0);
    CHECK(r.ops->open_bidi(r.conn, &id) == MOQ_TRANSPORT_OK);
    CHECK(id == 4);

    /* server numbering */
    struct rig s;
    CHECK(rig_up(&s, MOQ_PERSPECTIVE_SERVER) == 0);
    deliver_streams_available(&s, 8, 8);
    CHECK(s.ops->open_uni(s.conn, &id) == MOQ_TRANSPORT_OK);
    CHECK(id == 3);
    CHECK(s.ops->open_bidi(s.conn, &id) == MOQ_TRANSPORT_OK);
    CHECK(id == 1);

    rig_down(&s);
    rig_down(&r);
    if (failures == before)
        printf("PASS: stream_id_allocation\n");
}

/* A transport-assigned id diverging from the adapter's is fatal: every
 * byte routed so far went to the wrong stream. */
static void t_id_mismatch_fatal(void)
{
    int before = failures;
    struct rig r;
    uint64_t id = 0;

    CHECK(rig_up(&r, MOQ_PERSPECTIVE_CLIENT) == 0);
    deliver_streams_available(&r, 8, 8);

    r.fake.force_next_id = 42;
    CHECK(r.ops->open_uni(r.conn, &id) == MOQ_TRANSPORT_OK);
    CHECK(id == 2); /* the adapter computed the correct one */
    fake_msq_stream_t *st = fake_msq_stream_at(&r.fake, 0);
    CHECK(st != NULL && st->id == 42);
    CHECK(!moq_msquic_conn_is_fatal(r.conn));
    fake_msq_deliver_start_complete(st, QUIC_STATUS_SUCCESS);
    CHECK(moq_msquic_conn_is_fatal(r.conn));

    rig_down(&r);
    if (failures == before)
        printf("PASS: id_mismatch_fatal\n");
}

/* --- send budget ------------------------------------------------------------------ */

static void t_budget_depth_not_size(void)
{
    int before = failures;
    struct rig r;
    uint64_t id = 0;

    CHECK(rig_up(&r, MOQ_PERSPECTIVE_CLIENT) == 0);
    deliver_streams_available(&r, 8, 8);
    CHECK(r.ops->open_uni(r.conn, &id) == MOQ_TRANSPORT_OK);

    /* two 600 KiB sends: the second exceeds the 1 MiB floor behind
     * the first (staged bytes count as in flight) and must park */
    moq_rcbuf_t *a = make_rcbuf(600 * 1024);
    moq_rcbuf_t *b = make_rcbuf(600 * 1024);
    CHECK(a != NULL && b != NULL);
    CHECK(r.ops->write_payload(r.conn, id, a, false) ==
          MOQ_TRANSPORT_OK);
    CHECK(r.ops->write_payload(r.conn, id, b, false) ==
          MOQ_TRANSPORT_WOULD_BLOCK);
    /* zero-byte FIN passes regardless of budget */
    CHECK(r.ops->write(r.conn, id, NULL, 0, false) == MOQ_TRANSPORT_OK);
    /* the completion of the flushed batch frees the budget */
    moq_msquic_test_flush(r.conn);
    CHECK(fake_msq_deliver_send_complete(&r.fake, false));
    CHECK(r.ops->write_payload(r.conn, id, b, false) ==
          MOQ_TRANSPORT_OK);
    moq_msquic_test_flush(r.conn);
    CHECK(fake_msq_deliver_send_complete(&r.fake, false));
    moq_rcbuf_decref(a);
    moq_rcbuf_decref(b);

    /* idle stream admits one send of ANY size (a refused send has no
     * completion to wake a retry) */
    uint64_t big_id = 0;
    CHECK(r.ops->open_uni(r.conn, &big_id) == MOQ_TRANSPORT_OK);
    moq_rcbuf_t *big = make_rcbuf(2 * 1024 * 1024);
    moq_rcbuf_t *small = make_rcbuf(16);
    CHECK(big != NULL && small != NULL);
    CHECK(r.ops->write_payload(r.conn, big_id, big, false) ==
          MOQ_TRANSPORT_OK);
    /* while it is staged/in flight, the depth throttle holds */
    CHECK(r.ops->write_payload(r.conn, big_id, small, false) ==
          MOQ_TRANSPORT_WOULD_BLOCK);
    moq_msquic_test_flush(r.conn);
    CHECK(fake_msq_deliver_send_complete(&r.fake, false));
    CHECK(r.ops->write_payload(r.conn, big_id, small, false) ==
          MOQ_TRANSPORT_OK);
    moq_msquic_test_flush(r.conn);
    CHECK(fake_msq_deliver_send_complete(&r.fake, false));

    /* the budget math must not wrap: a huge in-flight count plus a
     * small span still blocks */
    struct moq_msq_stream *ms =
        moq_msquic_test_stream_find(r.conn, big_id);
    CHECK(ms != NULL);
    if (ms != NULL) {
        uint64_t saved = ms->inflight_bytes;

        moq_msquic_test_flush(r.conn);
        ms->inflight_bytes = UINT64_MAX - 1;
        CHECK(r.ops->write_payload(r.conn, big_id, small, false) ==
              MOQ_TRANSPORT_WOULD_BLOCK);
        ms->inflight_bytes = saved;
    }
    moq_rcbuf_decref(big);
    moq_rcbuf_decref(small);

    rig_down(&r);
    if (failures == before)
        printf("PASS: budget_depth_not_size\n");
}


/* --- inbound receive backpressure (pause / arrest / resume) --------------------- */

/*
 * While the bridge is backed up on a stream, an arriving RECEIVE must be
 * arrested SYNCHRONOUSLY by accepting zero bytes (RECEIVE.TotalBufferLength
 * -> 0), NOT fed onward — StreamReceiveSetEnabled(FALSE) is asynchronous
 * and cannot stop an already-queued RECEIVE. Proof: with the stream marked
 * paused, a delivered RECEIVE leaves its bytes HELD by the transport (the
 * fake models MsQuic holding unconsumed bytes) rather than consumed. On the
 * pre-fix adapter the bytes were fed and fully consumed (held_len == 0).
 */
static void t_receive_arrest_holds_while_paused(void)
{
    int before = failures;
    struct rig r;
    const uint8_t bytes[64] = { 0x11, 0x22, 0x33 };

    CHECK(rig_up(&r, MOQ_PERSPECTIVE_CLIENT) == 0);
    deliver_streams_available(&r, 8, 8);
    deliver_connected(&r);

    fake_msq_stream_t *st = deliver_peer_stream(&r, 3, true);
    CHECK(st != NULL);
    struct moq_msq_stream *ms = moq_msquic_test_stream_find(r.conn, 3);
    CHECK(ms != NULL);
    if (ms != NULL)
        ms->paused = true; /* the bridge is backed up on this stream */

    fake_msq_deliver_receive(st, bytes, sizeof(bytes), false);

    /* the whole event was held by the transport (accept-0), never fed
     * onward — so no extra bytes entered the bridge's pending state, and
     * the connection is not disturbed. The rejecting path touches no
     * session content, so no subscription is needed here; the end-to-end
     * redelivery-exactly-once is proven over real MsQuic in the loopback
     * proof (test_msquic_recv_loopback). */
    CHECK(st->held_len == sizeof(bytes));
    CHECK(!moq_msquic_conn_is_fatal(r.conn));

    rig_down(&r);
    if (failures == before)
        printf("PASS: receive_arrest_holds_while_paused\n");
}

/* A failed resume (StreamReceiveSetEnabled(TRUE)) is unrecoverable: the
 * stream can never deliver again, so the adapter must go fatal rather than
 * silently stall. The pre-fix adapter cleared paused before the call and
 * ignored the failure. */
static void t_receive_resume_failure_fatal(void)
{
    int before = failures;
    struct rig r;

    CHECK(rig_up(&r, MOQ_PERSPECTIVE_CLIENT) == 0);
    deliver_streams_available(&r, 8, 8);
    deliver_connected(&r);

    fake_msq_stream_t *st = deliver_peer_stream(&r, 3, true);
    CHECK(st != NULL);
    struct moq_msq_stream *ms = moq_msquic_test_stream_find(r.conn, 3);
    CHECK(ms != NULL);
    if (ms != NULL) {
        ms->paused = true;         /* a receive was arrested earlier... */
        ms->recv_disabled = true;  /* ...leaving delivery disabled */
    }

    /* the bridge has no pending state for this stream, so the next
     * service pass tries to resume it — make that resume fail */
    r.fake.recv_set_enabled_fails = 1;
    CHECK(!moq_msquic_conn_is_fatal(r.conn));
    moq_msquic_conn_service(r.conn);
    CHECK(moq_msquic_conn_is_fatal(r.conn));

    rig_down(&r);
    if (failures == before)
        printf("PASS: receive_resume_failure_fatal\n");
}

/* A pure zero-byte FIN carries no data to hold and is the stream's
 * terminal marker — it is delivered even while paused (nothing is held for
 * redelivery). This white-box case exercises the RECEIVE handler's paused
 * branch in isolation; feeding a FIN onto a stream with no subscription
 * drives the session terminal (an artifact of the standalone rig, not of
 * FIN handling — the loopback proof exercises the accepted subgroup-close
 * path end to end). What is under test is the handler's decision: the FIN
 * is FED, not held. */
static void t_receive_zero_fin_while_paused(void)
{
    int before = failures;
    struct rig r;

    CHECK(rig_up(&r, MOQ_PERSPECTIVE_CLIENT) == 0);
    deliver_streams_available(&r, 8, 8);
    deliver_connected(&r);

    fake_msq_stream_t *st = deliver_peer_stream(&r, 3, true);
    CHECK(st != NULL);
    struct moq_msq_stream *ms = moq_msquic_test_stream_find(r.conn, 3);
    CHECK(ms != NULL);
    if (ms != NULL)
        ms->paused = true;

    fake_msq_deliver_receive(st, NULL, 0, true); /* zero-byte FIN */

    CHECK(st->held_len == 0);          /* delivered, not held */
    CHECK(ms == NULL || ms->fin_fed);  /* the terminal marker went through */

    rig_down(&r);
    if (failures == before)
        printf("PASS: receive_zero_fin_while_paused\n");
}

/* A multi-buffer RECEIVE (data spanning several contiguous buffers) is
 * consumed in one pass when the stream is not paused: the handler iterates
 * every buffer and accepts the whole event (nothing held). As above, the
 * standalone rig has no subscription so the fed bytes drive the session
 * terminal — irrelevant to what is under test, which is that all buffers
 * were iterated and accepted (TotalBufferLength consumed in full). */
static void t_receive_multibuffer_full_accept(void)
{
    int before = failures;
    struct rig r;
    const uint8_t b0[8] = { 1, 2, 3, 4, 5, 6, 7, 8 };
    const uint8_t b1[4] = { 9, 10, 11, 12 };
    const uint8_t b2[16] = { 0 };
    const uint8_t *bufs[3] = { b0, b1, b2 };
    const size_t lens[3] = { sizeof(b0), sizeof(b1), sizeof(b2) };

    CHECK(rig_up(&r, MOQ_PERSPECTIVE_CLIENT) == 0);
    deliver_streams_available(&r, 8, 8);
    deliver_connected(&r);

    fake_msq_stream_t *st = deliver_peer_stream(&r, 3, true);
    CHECK(st != NULL);

    fake_msq_deliver_receive_multi(st, bufs, lens, 3, false);

    CHECK(st->held_len == 0); /* every buffer iterated and accepted */

    rig_down(&r);
    if (failures == before)
        printf("PASS: receive_multibuffer_full_accept\n");
}

/* A peer reset / terminal on a paused stream is handled cleanly: the
 * bridge is told, no resume is owed once the stream is gone, and
 * SHUTDOWN_COMPLETE frees it — no crash, no fatal. */
static void t_receive_terminal_while_paused(void)
{
    int before = failures;
    struct rig r;

    CHECK(rig_up(&r, MOQ_PERSPECTIVE_CLIENT) == 0);
    deliver_streams_available(&r, 8, 8);
    deliver_connected(&r);

    fake_msq_stream_t *st = deliver_peer_stream(&r, 3, true);
    CHECK(st != NULL);
    struct moq_msq_stream *ms = moq_msquic_test_stream_find(r.conn, 3);
    CHECK(ms != NULL);
    if (ms != NULL) {
        ms->paused = true;
        ms->recv_disabled = true;
    }

    /* Arm a resume failure: if the reset path wrongly tried to re-enable
     * the aborted receive direction, the failing StreamReceiveSetEnabled
     * would turn a valid peer reset into a transport fatal. The fix clears
     * the pause/hold state BEFORE servicing, so no resume is attempted. */
    int rse_before = st->receive_set_enabled;
    r.fake.recv_set_enabled_fails = 1;
    deliver_stream_event(st, QUIC_STREAM_EVENT_PEER_SEND_ABORTED, 0x7);
    CHECK(st->receive_set_enabled == rse_before); /* zero resume attempts */
    CHECK(!moq_msquic_conn_is_fatal(r.conn));     /* reset is not fatal */

    /* the transport tears the stream down: it is freed, nothing lingers */
    deliver_stream_event(st, QUIC_STREAM_EVENT_SHUTDOWN_COMPLETE, 0);
    CHECK(moq_msquic_test_stream_find(r.conn, 3) == NULL);
    CHECK(!moq_msquic_conn_is_fatal(r.conn));

    rig_down(&r);
    if (failures == before)
        printf("PASS: receive_terminal_while_paused\n");
}

/* moq_msquic_settings_init stamps the settings the adapter's contract
 * requires, with their IsSet bits, so a caller loading them into its
 * Configuration cannot silently drop them. SendBufferingEnabled=FALSE is
 * mandatory (send ownership / budget), and non-multi receive mode is
 * mandatory (inbound backpressure) — the latter is a preview-gated field,
 * asserted only where the ABI exposes it. */
static void t_settings_required(void)
{
    int before = failures;
    QUIC_SETTINGS s;

    moq_msquic_settings_init(&s);
    CHECK(s.SendBufferingEnabled == FALSE);
    CHECK(s.IsSet.SendBufferingEnabled == TRUE);
    CHECK(s.PeerBidiStreamCount > 0);
    CHECK(s.IsSet.PeerBidiStreamCount == TRUE);
    CHECK(s.PeerUnidiStreamCount > 0);
    CHECK(s.IsSet.PeerUnidiStreamCount == TRUE);
#ifdef QUIC_API_ENABLE_PREVIEW_FEATURES
    CHECK(s.StreamMultiReceiveEnabled == FALSE);
    CHECK(s.IsSet.StreamMultiReceiveEnabled == TRUE);
#endif

    if (failures == before)
        printf("PASS: settings_required\n");
}

/* --- multi-buffer partial acceptance over a real subscription ------------------- */

/*
 * A minimal pair to drive REAL inbound object pending: the adapter-under-
 * test is a CLIENT subscriber; a raw SERVER session + bridge + fake
 * endpoint is the publisher. Control bytes are relayed both ways so the
 * subscriber holds a real subscription; the publisher's subgroup object
 * bytes are captured (not delivered) so the test can hand them to the
 * subscriber's adapter as ONE crafted multi-buffer RECEIVE.
 */
struct rx_pair {
    struct rig aut;                 /* client subscriber (msquic adapter) */
    moq_session_t *peer;            /* server publisher (raw) */
    moq_transport_bridge_t *peer_bridge;
    fake_endpoint_t peer_ep;
    fake_msq_stream_t *ctrl;        /* subscriber control bidi fake stream */
    size_t aut_send_cur;
    size_t peer_op_cur;
    uint64_t uni_id;                /* the one subgroup uni stream */
    fake_msq_stream_t *uni_st;
    bool uni_open;
    uint8_t uni_bytes[2048];        /* captured subgroup wire (undelivered) */
    size_t uni_len;
};

static void rx_relay(struct rx_pair *p)
{
    moq_msquic_conn_service(p->aut.conn);
    fake_msq_t *f = &p->aut.fake;
    for (; p->aut_send_cur < (size_t)f->send_count; p->aut_send_cur++) {
        fake_msq_send_t *s = &f->sends[p->aut_send_cur];
        bool fin = (s->flags & QUIC_SEND_FLAG_FIN) != 0;

        (void)moq_transport_bridge_on_peer_control_bytes(
            p->peer_bridge, s->stream->id, s->bytes, s->bytes_len, fin, 0);
    }
    while (fake_msq_deliver_send_complete(f, false))
        ;
    (void)moq_transport_bridge_service(p->peer_bridge, 0);
    fake_endpoint_t *ep = &p->peer_ep;
    for (; p->peer_op_cur < ep->count; p->peer_op_cur++) {
        fake_op_t *o = &ep->ops[p->peer_op_cur];

        if (o->kind == FAKE_OP_OPEN_UNI) {
            p->uni_id = o->stream_id;
            p->uni_st = deliver_peer_stream(&p->aut, o->stream_id, true);
            p->uni_open = true;
        } else if (o->kind == FAKE_OP_WRITE) {
            if (p->ctrl != NULL && o->stream_id == p->ctrl->id)
                fake_msq_deliver_receive(p->ctrl, o->data, o->data_len,
                                         o->fin);
            else if (p->uni_open && o->stream_id == p->uni_id &&
                     p->uni_len + o->data_len <= sizeof(p->uni_bytes)) {
                memcpy(p->uni_bytes + p->uni_len, o->data, o->data_len);
                p->uni_len += o->data_len;
            }
        }
    }
    /* consumed: reset the op log so it cannot overflow across many pumps */
    fake_endpoint_clear_ops(ep);
    p->peer_op_cur = 0;
}

static void rx_pump(struct rx_pair *p, int rounds)
{
    for (int i = 0; i < rounds; i++)
        rx_relay(p);
}

static void rx_pair_down(struct rx_pair *p)
{
    if (p->peer_bridge != NULL)
        moq_transport_bridge_destroy(p->peer_bridge);
    if (p->peer != NULL)
        moq_session_destroy(p->peer);
    rig_down(&p->aut);
}

/* Bring the pair up and establish a subscription; leaves the subscriber's
 * event queue drained. Returns the accepted server-side subscription. */
static int rx_pair_up(struct rx_pair *p, uint32_t aut_max_events,
                      moq_subscription_t *out_server_sub)
{
    memset(p, 0, sizeof(*p));
    if (rig_up_events(&p->aut, MOQ_PERSPECTIVE_CLIENT, aut_max_events) < 0)
        return -1;
    deliver_streams_available(&p->aut, 8, 8);

    moq_session_cfg_t scfg;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_SERVER);
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 16;
    if (moq_session_create(&scfg, 0, &p->peer) < 0)
        return -1;
    fake_endpoint_init(&p->peer_ep, 3, 1);
    moq_transport_bridge_cfg_t bcfg;
    moq_transport_bridge_cfg_init(&bcfg, moq_alloc_default());
    if (moq_transport_bridge_create(&bcfg, p->peer, &p->peer_ep.vtable,
                                    &p->peer_ep, &p->peer_bridge) != MOQ_OK)
        return -1;

    deliver_connected(&p->aut); /* subscriber opens control + sends SETUP */
    p->ctrl = fake_msq_stream_at(&p->aut.fake, 0);
    fake_msq_deliver_start_complete(p->ctrl, QUIC_STATUS_SUCCESS);
    rx_pump(p, 12); /* complete the handshake */

    static const moq_bytes_t ns[] = { { (const uint8_t *)"live", 4 } };
    moq_subscribe_cfg_t sc;
    moq_subscription_t sub;
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace.parts = (moq_bytes_t *)ns;
    sc.track_namespace.count = 1;
    sc.track_name = (moq_bytes_t){ (const uint8_t *)"video", 5 };
    sc.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    if (moq_session_subscribe(p->aut.session, &sc, 0, &sub) < 0)
        return -1;
    rx_pump(p, 10);

    moq_subscription_t server_sub = MOQ_SUBSCRIPTION_INVALID;
    moq_event_t ev;
    while (moq_session_poll_events(p->peer, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST)
            server_sub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
    }
    if (!moq_subscription_is_valid(server_sub))
        return -1;
    moq_accept_subscribe_cfg_t acc;
    moq_accept_subscribe_cfg_init(&acc);
    if (moq_session_accept_subscribe(p->peer, server_sub, &acc, 0) < 0)
        return -1;
    rx_pump(p, 10);
    /* drain the subscriber's SETUP_COMPLETE + SUBSCRIBE_OK so its (small)
     * event queue starts empty before objects arrive */
    while (moq_session_poll_events(p->aut.session, &ev, 1) > 0)
        moq_event_cleanup(&ev);

    *out_server_sub = server_sub;
    return 0;
}

/* Poll the subscriber; for each object, record its id-count and a byte
 * check against the deterministic fill. */
/* Ordered arrival log, so a row can assert the exact sequence rather than
 * only a set. Sized for the largest workload any caller delivers. */
struct rx_tally {
    int count;
    uint8_t seen[8];
    int errors;
    uint64_t order[16];
    int order_len;
};

static void rx_collect_tally(struct rx_pair *p, struct rx_tally *t)
{
    moq_event_t ev;

    while (moq_session_poll_events(p->aut.session, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) {
            uint64_t oid = ev.u.object_received.object_id;
            const moq_rcbuf_t *pl = ev.u.object_received.payload;

            t->count++;
            if (oid < 8)
                t->seen[oid]++;
            if (t->order_len <
                (int)(sizeof(t->order) / sizeof(t->order[0])))
                t->order[t->order_len++] = oid;
            /* every payload byte, not just the first: the fill is the
             * object's own letter repeated, so a corrupted middle byte is
             * a real difference the old first-byte check missed */
            if (pl == NULL || moq_rcbuf_len(pl) != 3) {
                t->errors++;
            } else {
                const uint8_t *d = moq_rcbuf_data(pl);
                uint8_t want = (uint8_t)('A' + oid);

                if (d[0] != want || d[1] != want || d[2] != want)
                    t->errors++;
            }
        }
        moq_event_cleanup(&ev);
    }
}

static void rx_collect(struct rx_pair *p, int *count, uint8_t *seen,
                       int *errors)
{
    struct rx_tally t;

    memset(&t, 0, sizeof(t));
    t.count = *count;
    memcpy(t.seen, seen, sizeof(t.seen));
    t.errors = *errors;
    rx_collect_tally(p, &t);
    *count = t.count;
    memcpy(seen, t.seen, sizeof(t.seen));
    *errors = t.errors;
}

/*
 * A multi-buffer RECEIVE whose middle buffer leaves the bridge pending must
 * NOT feed the buffers after it — those are held by MsQuic
 * (TotalBufferLength lowered) and redelivered exactly once after resume.
 * Four objects are laid into four buffers; with a 2-deep event queue
 * objects 0 and 1 fill it and object 2 backs the bridge up, so the fourth
 * buffer must be held, then delivered once. Pre-fix, every buffer was fed.
 */
static void t_receive_multibuffer_partial_hold(void)
{
    int before = failures;
    struct rx_pair p;
    moq_subscription_t server_sub;

    /* event queue of 2: the handshake's two events (SETUP_COMPLETE,
     * SUBSCRIBE_OK) fit and drain cleanly, then two objects fill it so a
     * later object in the multi-buffer RECEIVE backs the bridge up. */
    CHECK(rx_pair_up(&p, 2, &server_sub) == 0);
    CHECK(moq_subscription_is_valid(server_sub));
    if (!moq_subscription_is_valid(server_sub)) {
        rx_pair_down(&p);
        return;
    }

    moq_subgroup_cfg_t sgc;
    moq_subgroup_handle_t sg;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = 0;
    sgc.subgroup_id = 0;
    sgc.publisher_priority = 200;
    CHECK(moq_session_open_subgroup(p.peer, server_sub, &sgc, 0, &sg) ==
          MOQ_OK);

    /* four objects captured as four wire chunks (header+obj0, obj1, obj2,
     * obj3) by servicing between writes */
    enum { NOBJ = 4 };
    size_t off[NOBJ + 1];
    off[0] = 0;
    const char *payload[NOBJ] = { "AAA", "BBB", "CCC", "DDD" };
    for (int i = 0; i < NOBJ; i++) {
        moq_rcbuf_t *b = NULL;
        moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)payload[i],
                         3, &b);
        CHECK(moq_session_write_object(p.peer, sg, (uint64_t)i, b, 0) ==
              MOQ_OK);
        moq_rcbuf_decref(b);
        rx_pump(&p, 3);
        off[i + 1] = p.uni_len;
    }
    CHECK(p.uni_open);
    for (int i = 0; i < NOBJ; i++)
        CHECK(off[i + 1] > off[i]);

    /* deliver the four chunks as ONE multi-buffer RECEIVE, FIN on the last.
     * With a 2-deep queue, chunk0/chunk1 emit obj0/obj1 (queue full),
     * chunk2 backs the bridge up (pending) -> the handler MUST STOP there
     * and hold chunk3 for MsQuic rather than feed it onward. */
    const uint8_t *bufs[NOBJ];
    size_t lens[NOBJ];
    for (int i = 0; i < NOBJ; i++) {
        bufs[i] = p.uni_bytes + off[i];
        lens[i] = off[i + 1] - off[i];
    }
    fake_msq_deliver_receive_multi(p.uni_st, bufs, lens, NOBJ, true);

    /* exactly the last chunk was held (not fed); delivery was disabled */
    CHECK(p.uni_st->held_len == lens[NOBJ - 1]);
    CHECK(p.uni_st->recv_disabled);

    int count = 0, errors = 0;
    uint8_t seen[8] = { 0 };
    rx_collect(&p, &count, seen, &errors); /* obj0, obj1 */
    CHECK(count == 2);

    /* draining frees the queue: obj2 (buffered on the WOULD_BLOCK) retries
     * and the stream resumes, re-enabling delivery for the held chunk */
    moq_msquic_conn_service(p.aut.conn);
    rx_collect(&p, &count, seen, &errors); /* obj2 */
    CHECK(!p.uni_st->recv_disabled);       /* resume happened */

    /* the held chunk redelivers exactly once -> obj3 */
    CHECK(fake_msq_redeliver_held(p.uni_st));
    moq_msquic_conn_service(p.aut.conn);
    rx_collect(&p, &count, seen, &errors); /* obj3 */

    CHECK(count == NOBJ);
    CHECK(seen[0] == 1 && seen[1] == 1 && seen[2] == 1 && seen[3] == 1);
    CHECK(errors == 0);                    /* byte-exact */
    CHECK(!moq_msquic_conn_is_fatal(p.aut.conn));

    rx_pair_down(&p);
    if (failures == before)
        printf("PASS: receive_multibuffer_partial_hold\n");
}

/*
 * Capacity invariance: the SAME workload must arrive complete, in order,
 * byte-exact and exactly once at every receive-queue ceiling. A small
 * ceiling only changes how often the adapter must arrest and resume -- it
 * may never change what the application ends up with.
 *
 * Objects are laid into one multi-buffer RECEIVE so the arrest lands
 * mid-event at the tight ceilings, and the drain loop is a bounded
 * service/redeliver cycle whose bound is a hang guard: the verdict is the
 * exact final tally, never elapsed rounds.
 */
static void t_receive_capacity_invariance(uint32_t cap)
{
    int before = failures;
    struct rx_pair p;
    moq_subscription_t server_sub;
    enum { NOBJ = 6, MAX_ROUNDS = 8 * NOBJ };

    if (rx_pair_up(&p, cap, &server_sub) != 0) {
        CHECK(0 && "rx pair up");
        rx_pair_down(&p);
        return;
    }
    CHECK(moq_subscription_is_valid(server_sub));
    if (!moq_subscription_is_valid(server_sub)) {
        rx_pair_down(&p);
        return;
    }

    moq_subgroup_cfg_t sgc;
    moq_subgroup_handle_t sg;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = 0;
    sgc.subgroup_id = 0;
    sgc.publisher_priority = 200;
    CHECK(moq_session_open_subgroup(p.peer, server_sub, &sgc, 0, &sg) ==
          MOQ_OK);

    /* one wire chunk per object, captured by servicing between writes */
    size_t off[NOBJ + 1];
    off[0] = 0;
    for (int i = 0; i < NOBJ; i++) {
        char payload[4] = { (char)('A' + i), (char)('A' + i),
                            (char)('A' + i), 0 };
        moq_rcbuf_t *b = NULL;

        CHECK(moq_rcbuf_create(moq_alloc_default(),
                               (const uint8_t *)payload, 3, &b) == MOQ_OK);
        if (b == NULL)
            break;
        CHECK(moq_session_write_object(p.peer, sg, (uint64_t)i, b, 0) ==
              MOQ_OK);
        moq_rcbuf_decref(b);
        rx_pump(&p, 3);
        off[i + 1] = p.uni_len;
    }
    CHECK(p.uni_open);
    for (int i = 0; i < NOBJ; i++)
        CHECK(off[i + 1] > off[i]);

    const uint8_t *bufs[NOBJ];
    size_t lens[NOBJ];
    for (int i = 0; i < NOBJ; i++) {
        bufs[i] = p.uni_bytes + off[i];
        lens[i] = off[i + 1] - off[i];
    }
    fake_msq_deliver_receive_multi(p.uni_st, bufs, lens, NOBJ, true);

    /* Immediately after the single delivery, the ceiling decides whether
     * the adapter had to arrest: a tight queue cannot take all six objects
     * in one go, so delivery must be disabled with bytes held; a ceiling
     * above the workload must take the lot without arresting at all. */
    if (cap < NOBJ) {
        CHECK(p.uni_st->recv_disabled);
        CHECK(p.uni_st->held_len > 0);
    } else {
        CHECK(!p.uni_st->recv_disabled);
        CHECK(p.uni_st->held_len == 0);
    }

    /* drain: collect what fits, service to retry the pending feed, and
     * redeliver whatever the transport is holding, until the tally is
     * complete. The round bound is a hang guard only. */
    struct rx_tally t;
    int rounds = 0;

    memset(&t, 0, sizeof(t));
    rx_collect_tally(&p, &t);
    while (t.count < NOBJ && rounds < MAX_ROUNDS) {
        moq_msquic_conn_service(p.aut.conn);
        rx_collect_tally(&p, &t);
        if (t.count < NOBJ && fake_msq_redeliver_held(p.uni_st)) {
            moq_msquic_conn_service(p.aut.conn);
            rx_collect_tally(&p, &t);
        }
        rounds++;
    }

    /* the invariant, identical at every ceiling */
    CHECK(t.count == NOBJ);
    for (int i = 0; i < NOBJ; i++)
        CHECK(t.seen[i] == 1); /* each id exactly once: no loss, no dup */
    CHECK(t.errors == 0);      /* byte-exact, every byte */
    /* and in the order they were published, not merely all present */
    CHECK(t.order_len == NOBJ);
    for (int i = 0; i < NOBJ && i < t.order_len; i++)
        CHECK(t.order[i] == (uint64_t)i);
    CHECK(!moq_msquic_conn_is_fatal(p.aut.conn));
    /* nothing is left arrested once the application has caught up */
    CHECK(!p.uni_st->recv_disabled);
    CHECK(p.uni_st->held_len == 0);

    rx_pair_down(&p);
    if (failures == before)
        printf("PASS: receive_capacity_invariance[cap=%u]\n", cap);
}

/*
 * The event-progress token is what lets the managed doorbell re-drive a
 * receiver whose event queue re-fills mid-drain. Two semantics under a bounded
 * queue, with several objects delivered as ONE buffer (so the session buffers
 * the tail internally and drains it a batch at a time):
 *   (1) a service retry that emits events but STILL leaves inbound pending
 *       (returns WOULD_BLOCK) MUST advance the token; and
 *   (2) a service retry against a still-full event queue advances NOTHING.
 */
static void t_event_progress_token_semantics(void)
{
    int before = failures;
    struct rx_pair p;
    moq_subscription_t server_sub;

    /* event queue of 2: the handshake events drain, then a burst of objects
     * fills it (2 emitted) and the rest are buffered inside the session. */
    CHECK(rx_pair_up(&p, 2, &server_sub) == 0);
    if (!moq_subscription_is_valid(server_sub)) {
        rx_pair_down(&p);
        return;
    }

    moq_subgroup_cfg_t sgc;
    moq_subgroup_handle_t sg;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = 0;
    sgc.subgroup_id = 0;
    sgc.publisher_priority = 200;
    CHECK(moq_session_open_subgroup(p.peer, server_sub, &sgc, 0, &sg) == MOQ_OK);

    enum { NOBJ = 6 };
    size_t off[NOBJ + 1];
    off[0] = 0;
    for (int i = 0; i < NOBJ; i++) {
        char pl[3] = { (char)('A' + i), (char)('A' + i), (char)('A' + i) };
        moq_rcbuf_t *b = NULL;
        moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)pl, 3, &b);
        CHECK(moq_session_write_object(p.peer, sg, (uint64_t)i, b, 0) == MOQ_OK);
        moq_rcbuf_decref(b);
        rx_pump(&p, 3);
        off[i + 1] = p.uni_len;
    }
    CHECK(p.uni_open);

    /* Deliver all six objects as ONE contiguous RECEIVE: queue(2) fills with
     * obj0/obj1 and the session buffers obj2..obj5 internally (WOULD_BLOCK). */
    fake_msq_deliver_receive(p.uni_st, p.uni_bytes, off[NOBJ], false);

    uint64_t tok_full = moq_msq_conn_event_token(p.aut.conn);

    /* (2) queue is full (nothing drained): a service retry emits nothing, so
     * the token does not move. */
    moq_msquic_conn_service(p.aut.conn);
    CHECK(moq_msq_conn_event_token(p.aut.conn) == tok_full);

    /* drain the two queued events -> frees event capacity */
    moq_event_t ev;
    int drained = 0;
    while (moq_session_poll_events(p.aut.session, &ev, 1) > 0) {
        drained++;
        moq_event_cleanup(&ev);
    }
    CHECK(drained == 2);
    /* dequeue advances the token too: exactly the two transfers moved it. */
    CHECK(moq_msq_conn_event_token(p.aut.conn) == tok_full + 2);
    uint64_t tok_drained = moq_msq_conn_event_token(p.aut.conn);

    /* (1) a service retry now re-feeds the buffered tail: it emits more events
     * (queue re-fills) yet inbound is STILL pending (obj4/obj5 remain), i.e. it
     * "emitted-and-WOULD_BLOCK'd" -- the token MUST advance. */
    moq_msquic_conn_service(p.aut.conn);
    CHECK(moq_msq_conn_event_token(p.aut.conn) != tok_drained);
    CHECK(!moq_msquic_conn_is_fatal(p.aut.conn));

    rx_pair_down(&p);
    if (failures == before)
        printf("PASS: event_progress_token_semantics\n");
}

/* --- send-record ownership ---------------------------------------------------------- */

static void t_rcbuf_lifetime(void)
{
    int before = failures;
    struct rig r;
    uint64_t id = 0;

    CHECK(rig_up(&r, MOQ_PERSPECTIVE_CLIENT) == 0);
    deliver_streams_available(&r, 8, 8);
    CHECK(r.ops->open_uni(r.conn, &id) == MOQ_TRANSPORT_OK);

    /* the adapter's incref keeps the bytes alive past the caller's
     * release; the batch's SEND_COMPLETE drops the last ref (ASan
     * owns the proof of exactly-once) */
    moq_rcbuf_t *buf = make_rcbuf(1024);
    CHECK(buf != NULL);
    CHECK(r.ops->write_payload(r.conn, id, buf, false) ==
          MOQ_TRANSPORT_OK);
    moq_rcbuf_decref(buf); /* caller's ref gone; adapter's remains */
    moq_msquic_test_flush(r.conn);
    CHECK(r.fake.sends[0].data != NULL);
    CHECK(r.fake.sends[0].data[0] == 0x5a); /* still readable */
    CHECK(fake_msq_deliver_send_complete(&r.fake, false));

    /* a flush the transport refuses releases every staged entry's
     * ownership exactly once (no completion will fire), and the next
     * service pass reports the dead transport as fatal */
    moq_rcbuf_t *buf2 = make_rcbuf(64);
    CHECK(buf2 != NULL);
    r.fake.stream_send_fails = 1;
    CHECK(r.ops->write_payload(r.conn, id, buf2, false) ==
          MOQ_TRANSPORT_OK); /* staged: acceptance is at flush */
    moq_msquic_test_flush(r.conn);
    moq_rcbuf_decref(buf2); /* frees: the adapter released its ref */
    CHECK(!moq_msquic_conn_is_fatal(r.conn));
    moq_msquic_conn_service(r.conn); /* the deferred fatal feed */
    CHECK(moq_msquic_conn_is_fatal(r.conn));

    rig_down(&r);
    if (failures == before)
        printf("PASS: rcbuf_lifetime\n");
}

static void t_cold_write_copies(void)
{
    int before = failures;
    struct rig r;
    uint64_t id = 0;
    uint8_t bytes[32];

    CHECK(rig_up(&r, MOQ_PERSPECTIVE_CLIENT) == 0);
    deliver_streams_available(&r, 8, 8);
    CHECK(r.ops->open_uni(r.conn, &id) == MOQ_TRANSPORT_OK);

    memset(bytes, 0xab, sizeof(bytes));
    CHECK(r.ops->write(r.conn, id, bytes, sizeof(bytes), false) ==
          MOQ_TRANSPORT_OK);
    /* the caller's buffer is released at return: scribbling it must
     * not corrupt the staged send */
    memset(bytes, 0x00, sizeof(bytes));
    moq_msquic_test_flush(r.conn);
    CHECK(r.fake.sends[0].len == sizeof(bytes));
    CHECK(r.fake.sends[0].data[0] == 0xab);
    CHECK(r.fake.sends[0].data[31] == 0xab);
    CHECK(fake_msq_deliver_send_complete(&r.fake, false));

    rig_down(&r);
    if (failures == before)
        printf("PASS: cold_write_copies\n");
}

static void t_record_pool_reuse(void)
{
    int before = failures;
    struct rig r;
    uint64_t id = 0;

    CHECK(rig_up(&r, MOQ_PERSPECTIVE_CLIENT) == 0);
    deliver_streams_available(&r, 8, 8);
    CHECK(r.ops->open_uni(r.conn, &id) == MOQ_TRANSPORT_OK);

    moq_rcbuf_t *buf = make_rcbuf(256);
    CHECK(buf != NULL);
    CHECK(r.ops->write_payload(r.conn, id, buf, false) ==
          MOQ_TRANSPORT_OK);
    moq_msquic_test_flush(r.conn);
    void *rec0 = r.fake.sends[0].client_ctx;
    CHECK(fake_msq_deliver_send_complete(&r.fake, false));
    CHECK(r.ops->write_payload(r.conn, id, buf, false) ==
          MOQ_TRANSPORT_OK);
    moq_msquic_test_flush(r.conn);
    /* the completed record went back to the pool and came out again */
    CHECK(r.fake.sends[1].client_ctx == rec0);
    CHECK(fake_msq_deliver_send_complete(&r.fake, false));
    moq_rcbuf_decref(buf);

    rig_down(&r);
    if (failures == before)
        printf("PASS: record_pool_reuse\n");
}

/* --- credit gating -------------------------------------------------------------------- */

static void t_credit_gating(void)
{
    int before = failures;
    struct rig r;
    uint64_t id = 0;

    CHECK(rig_up(&r, MOQ_PERSPECTIVE_CLIENT) == 0);

    deliver_streams_available(&r, 1, 0);
    CHECK(r.ops->open_uni(r.conn, &id) == MOQ_TRANSPORT_WOULD_BLOCK);
    CHECK(r.ops->open_bidi(r.conn, &id) == MOQ_TRANSPORT_OK);
    CHECK(r.ops->open_bidi(r.conn, &id) == MOQ_TRANSPORT_WOULD_BLOCK);

    /* fresh credit unblocks */
    deliver_streams_available(&r, 2, 3);
    CHECK(r.ops->open_uni(r.conn, &id) == MOQ_TRANSPORT_OK);
    CHECK(r.ops->open_bidi(r.conn, &id) == MOQ_TRANSPORT_OK);

    rig_down(&r);
    if (failures == before)
        printf("PASS: credit_gating\n");
}

/* --- idempotent teardown ops + pure FIN + close --------------------------------------- */

static void t_teardown_ops(void)
{
    int before = failures;
    struct rig r;
    uint64_t id = 0;

    CHECK(rig_up(&r, MOQ_PERSPECTIVE_CLIENT) == 0);
    deliver_streams_available(&r, 8, 8);

    /* unknown stream: idempotent OK, not an error */
    CHECK(r.ops->reset_stream(r.conn, 999, 7) == MOQ_TRANSPORT_OK);
    CHECK(r.ops->stop_sending(r.conn, 999, 7) == MOQ_TRANSPORT_OK);

    CHECK(r.ops->open_bidi(r.conn, &id) == MOQ_TRANSPORT_OK);
    fake_msq_stream_t *st = fake_msq_stream_at(&r.fake, 0);
    CHECK(st != NULL);

    CHECK(r.ops->reset_stream(r.conn, id, 0x77) == MOQ_TRANSPORT_OK);
    CHECK(st->shutdown_calls == 1);
    CHECK(st->last_shutdown_flags == QUIC_STREAM_SHUTDOWN_FLAG_ABORT_SEND);
    CHECK(st->last_shutdown_code == 0x77);
    CHECK(r.ops->stop_sending(r.conn, id, 0x33) == MOQ_TRANSPORT_OK);
    CHECK(st->last_shutdown_flags ==
          QUIC_STREAM_SHUTDOWN_FLAG_ABORT_RECEIVE);

    /* pure FIN is a graceful shutdown, not a send record */
    int sends_before = r.fake.send_count;
    CHECK(r.ops->write(r.conn, id, NULL, 0, true) == MOQ_TRANSPORT_OK);
    CHECK(r.fake.send_count == sends_before);
    CHECK(st->last_shutdown_flags == QUIC_STREAM_SHUTDOWN_FLAG_GRACEFUL);

    CHECK(r.ops->close_transport(r.conn, 0x10, NULL, 0) ==
          MOQ_TRANSPORT_OK);
    CHECK(r.fake.conn_shutdowns == 1);
    CHECK(r.fake.last_conn_shutdown_code == 0x10);

    rig_down(&r);
    if (failures == before)
        printf("PASS: teardown_ops\n");
}

/* --- batching ---------------------------------------------------------------- */

/* Writes staged in one pass coalesce into ONE multi-buffer StreamSend
 * with one completion releasing every entry exactly once; a FIN rides
 * its batch; staged data flushes ahead of a reset. */
static void t_batch_coalescing(void)
{
    int before = failures;
    struct rig r;
    uint64_t id = 0;

    CHECK(rig_up(&r, MOQ_PERSPECTIVE_CLIENT) == 0);
    deliver_streams_available(&r, 8, 8);
    CHECK(r.ops->open_uni(r.conn, &id) == MOQ_TRANSPORT_OK);

    /* five writes — cold and payload interleaved, like the bridge's
     * header+payload pattern — stage without touching the transport */
    moq_rcbuf_t *p1 = make_rcbuf(100);
    moq_rcbuf_t *p2 = make_rcbuf(200);
    uint8_t hdr[4] = { 1, 2, 3, 4 };

    CHECK(p1 != NULL && p2 != NULL);
    CHECK(r.ops->write(r.conn, id, hdr, sizeof(hdr), false) ==
          MOQ_TRANSPORT_OK);
    CHECK(r.ops->write_payload(r.conn, id, p1, false) ==
          MOQ_TRANSPORT_OK);
    CHECK(r.ops->write(r.conn, id, hdr, sizeof(hdr), false) ==
          MOQ_TRANSPORT_OK);
    CHECK(r.ops->write_payload(r.conn, id, p2, false) ==
          MOQ_TRANSPORT_OK);
    CHECK(r.ops->write(r.conn, id, hdr, sizeof(hdr), false) ==
          MOQ_TRANSPORT_OK);
    CHECK(r.fake.send_count == 0); /* nothing flushed yet */

    moq_msquic_test_flush(r.conn);
    CHECK(r.fake.send_count == 1); /* ONE send... */
    CHECK(r.fake.sends[0].buf_count == 5); /* ...five buffers */
    CHECK(r.fake.sends[0].total == 4 + 100 + 4 + 200 + 4);

    /* one completion releases everything exactly once (the caller
     * refs below become the last ones; ASan owns the proof) */
    CHECK(fake_msq_deliver_send_complete(&r.fake, false));
    moq_rcbuf_decref(p1);
    moq_rcbuf_decref(p2);

    /* a FIN write flushes its batch immediately with the FIN flag */
    moq_rcbuf_t *p3 = make_rcbuf(64);
    CHECK(p3 != NULL);
    CHECK(r.ops->write_payload(r.conn, id, p3, true) ==
          MOQ_TRANSPORT_OK);
    CHECK(r.fake.send_count == 2);
    CHECK((r.fake.sends[1].flags & QUIC_SEND_FLAG_FIN) != 0);
    CHECK(fake_msq_deliver_send_complete(&r.fake, false));
    moq_rcbuf_decref(p3);

    /* staged data must not reorder past a reset: the flush happens
     * inside the reset op, before the shutdown */
    uint64_t id2 = 0;
    CHECK(r.ops->open_uni(r.conn, &id2) == MOQ_TRANSPORT_OK);
    moq_rcbuf_t *p4 = make_rcbuf(32);
    CHECK(p4 != NULL);
    CHECK(r.ops->write_payload(r.conn, id2, p4, false) ==
          MOQ_TRANSPORT_OK);
    CHECK(r.fake.send_count == 2); /* staged */
    CHECK(r.ops->reset_stream(r.conn, id2, 0x7) == MOQ_TRANSPORT_OK);
    CHECK(r.fake.send_count == 3); /* flushed by the reset */
    CHECK(fake_msq_deliver_send_complete(&r.fake, false));
    moq_rcbuf_decref(p4);

    rig_down(&r);
    if (failures == before)
        printf("PASS: batch_coalescing\n");
}

/* A batch longer than the entry cap splits: cap+1 writes make two
 * sends, with ownership released once per entry across both. */
static void t_batch_cap_split(void)
{
    int before = failures;
    struct rig r;
    uint64_t id = 0;
    uint8_t byte = 0x33;

    CHECK(rig_up(&r, MOQ_PERSPECTIVE_CLIENT) == 0);
    deliver_streams_available(&r, 8, 8);
    CHECK(r.ops->open_uni(r.conn, &id) == MOQ_TRANSPORT_OK);

    for (int i = 0; i < 65; i++)
        CHECK(r.ops->write(r.conn, id, &byte, 1, false) ==
              MOQ_TRANSPORT_OK);
    /* the 64th write filled and flushed one batch */
    CHECK(r.fake.send_count == 1);
    CHECK(r.fake.sends[0].buf_count == 64);
    moq_msquic_test_flush(r.conn);
    CHECK(r.fake.send_count == 2);
    CHECK(r.fake.sends[1].buf_count == 1);
    CHECK(fake_msq_deliver_send_complete(&r.fake, false));
    CHECK(fake_msq_deliver_send_complete(&r.fake, false));

    rig_down(&r);
    if (failures == before)
        printf("PASS: batch_cap_split\n");
}

/* A flush that fails DURING a service pass must poison the bridge
 * before any app code runs: the hook is skipped for that pass (the
 * bytes the bridge counted as written are gone), and the app observes
 * the fatal on the next pass instead. */
static int ffh_calls;
static bool ffh_saw_fatal_first;

static void flush_fail_hook(moq_msquic_conn_t *conn, void *user)
{
    (void)user;
    if (ffh_calls++ == 0)
        ffh_saw_fatal_first = moq_msquic_conn_is_fatal(conn);
}

static void t_flush_failure_ordering(void)
{
    int before = failures;
    fake_msq_t fake;
    moq_session_t *session = NULL;
    moq_msquic_conn_t *conn = NULL;
    moq_session_cfg_t scfg;
    uint64_t id = 0;

    ffh_calls = 0;
    ffh_saw_fatal_first = false;
    fake_msq_init(&fake, true);
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_CLIENT);
    CHECK(moq_session_create(&scfg, 0, &session) == 0);

    moq_msquic_conn_cfg_t cfg;
    moq_msquic_conn_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.session = session;
    cfg.api = fake_msq_table(&fake);
    cfg.hook = flush_fail_hook;
    CHECK(moq_msquic_conn_create(&cfg, &conn) == MOQ_OK);
    CHECK(moq_msquic_conn_bind(conn, fake_msq_conn_handle(&fake)) ==
          MOQ_OK);

    const moq_transport_endpoint_ops_t *ops = moq_msquic_test_ops(conn);
    QUIC_CONNECTION_EVENT ev;
    memset(&ev, 0, sizeof(ev));
    ev.Type = QUIC_CONNECTION_EVENT_STREAMS_AVAILABLE;
    ev.STREAMS_AVAILABLE.UnidirectionalCount = 4;
    moq_msquic_conn_callback()(fake_msq_conn_handle(&fake), conn, &ev);
    /* the STREAMS_AVAILABLE event already ran one healthy hook pass */
    int hooks_before = ffh_calls;

    CHECK(ops->open_uni(conn, &id) == MOQ_TRANSPORT_OK);
    moq_rcbuf_t *buf = make_rcbuf(64);
    CHECK(buf != NULL);
    CHECK(ops->write_payload(conn, id, buf, false) == MOQ_TRANSPORT_OK);
    moq_rcbuf_decref(buf);

    /* the service-triggered flush fails: the bridge must be fatal
     * BEFORE the hook could run in that pass */
    fake.stream_send_fails = 1;
    ffh_calls = 0;
    ffh_saw_fatal_first = false;
    moq_msquic_conn_service(conn);
    CHECK(moq_msquic_conn_is_fatal(conn));
    if (ffh_calls > 0)
        CHECK(ffh_saw_fatal_first); /* app never saw a healthy lie */
    (void)hooks_before;

    moq_msquic_conn_destroy(conn);
    moq_session_destroy(session);
    if (failures == before)
        printf("PASS: flush_failure_ordering\n");
}

/* A flush that fails INSIDE an endpoint op (stream switch, batch
 * full) must stop the op immediately: the write is refused with an
 * error — never staged, never reported OK — no ownership is dropped
 * twice, and the bridge goes fatal before any further action. */
static void t_flush_failure_in_op(void)
{
    int before = failures;
    struct rig r;
    uint64_t id_a = 0, id_b = 0;

    CHECK(rig_up(&r, MOQ_PERSPECTIVE_CLIENT) == 0);
    deliver_streams_available(&r, 8, 8);
    CHECK(r.ops->open_uni(r.conn, &id_a) == MOQ_TRANSPORT_OK);
    CHECK(r.ops->open_uni(r.conn, &id_b) == MOQ_TRANSPORT_OK);

    /* stage on stream A, then force the transport down: the write to
     * stream B triggers the stream-switch flush of A, which fails —
     * B's write must be refused, not silently staged */
    moq_rcbuf_t *pa = make_rcbuf(64);
    moq_rcbuf_t *pb = make_rcbuf(64);
    CHECK(pa != NULL && pb != NULL);
    CHECK(r.ops->write_payload(r.conn, id_a, pa, false) ==
          MOQ_TRANSPORT_OK);
    r.fake.stream_send_fails = 1;
    int sends_before = r.fake.send_count;
    CHECK(r.ops->write_payload(r.conn, id_b, pb, false) ==
          MOQ_TRANSPORT_ERROR);
    CHECK(r.fake.send_count == sends_before); /* nothing accepted */
    moq_rcbuf_decref(pa); /* frees: the failed flush released A's ref */
    moq_rcbuf_decref(pb); /* frees: B was never staged */
    /* the very next service pass latches the fatal */
    moq_msquic_conn_service(r.conn);
    CHECK(moq_msquic_conn_is_fatal(r.conn));
    rig_down(&r);

    /* batch-full variant: the 64th entry's flush fails — the op
     * reports the error and every staged entry is released once */
    struct rig r2;
    uint64_t id2 = 0;
    uint8_t byte = 0x44;

    CHECK(rig_up(&r2, MOQ_PERSPECTIVE_CLIENT) == 0);
    deliver_streams_available(&r2, 8, 8);
    CHECK(r2.ops->open_uni(r2.conn, &id2) == MOQ_TRANSPORT_OK);
    for (int i = 0; i < 63; i++)
        CHECK(r2.ops->write(r2.conn, id2, &byte, 1, false) ==
              MOQ_TRANSPORT_OK);
    r2.fake.stream_send_fails = 1;
    CHECK(r2.ops->write(r2.conn, id2, &byte, 1, false) ==
          MOQ_TRANSPORT_ERROR);
    CHECK(r2.fake.send_count == 0);
    moq_msquic_conn_service(r2.conn);
    CHECK(moq_msquic_conn_is_fatal(r2.conn));
    rig_down(&r2);

    if (failures == before)
        printf("PASS: flush_failure_in_op\n");
}

/* --- oversized cold write ---------------------------------------------------------- */

/* A cold write longer than one QUIC_BUFFER can carry must be refused
 * before anything allocates, copies, or narrows the length for the
 * transport — the borrowed buffer must never be misrepresented. */
static int count_allocs;

static void *counting_alloc(size_t size, void *ctx)
{
    (void)ctx;
    count_allocs++;
    return malloc(size);
}

static void *counting_realloc(void *ptr, size_t old_size, size_t new_size,
                              void *ctx)
{
    (void)old_size;
    (void)ctx;
    count_allocs++;
    return realloc(ptr, new_size);
}

static void counting_free(void *ptr, size_t size, void *ctx)
{
    (void)size;
    (void)ctx;
    free(ptr);
}

static void t_cold_write_too_large(void)
{
    int before = failures;
    fake_msq_t fake;
    moq_session_t *session = NULL;
    moq_msquic_conn_t *conn = NULL;
    moq_session_cfg_t scfg;
    moq_alloc_t alloc = {
        .ctx = NULL,
        .alloc = counting_alloc,
        .realloc = counting_realloc,
        .free = counting_free,
    };
    uint64_t id = 0;
    uint8_t byte = 0;

    fake_msq_init(&fake, true);
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_CLIENT);
    CHECK(moq_session_create(&scfg, 0, &session) == 0);

    moq_msquic_conn_cfg_t cfg;
    moq_msquic_conn_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = &alloc;
    cfg.session = session;
    cfg.api = fake_msq_table(&fake);
    CHECK(moq_msquic_conn_create(&cfg, &conn) == MOQ_OK);
    CHECK(moq_msquic_conn_bind(conn, fake_msq_conn_handle(&fake)) ==
          MOQ_OK);

    const moq_transport_endpoint_ops_t *ops = moq_msquic_test_ops(conn);
    QUIC_CONNECTION_EVENT ev;
    memset(&ev, 0, sizeof(ev));
    ev.Type = QUIC_CONNECTION_EVENT_STREAMS_AVAILABLE;
    ev.STREAMS_AVAILABLE.UnidirectionalCount = 4;
    moq_msquic_conn_callback()(fake_msq_conn_handle(&fake), conn, &ev);
    CHECK(ops->open_uni(conn, &id) == MOQ_TRANSPORT_OK);

#if SIZE_MAX > UINT32_MAX
    int allocs_before = count_allocs;
    int sends_before = fake.send_count;

    /* the data pointer is a single byte: the op must reject on length
     * alone, touching neither the bytes nor the allocator */
    CHECK(ops->write(conn, id, &byte, (size_t)UINT32_MAX + 1, false) ==
          MOQ_TRANSPORT_ERROR);
    CHECK(count_allocs == allocs_before);
    CHECK(fake.send_count == sends_before);
#else
    (void)byte;
#endif

    moq_msquic_conn_destroy(conn);
    moq_session_destroy(session);
    if (failures == before)
        printf("PASS: cold_write_too_large\n");
}

static void deliver_dgram_state(struct rig *r, bool enabled,
                                uint16_t max)
{
    QUIC_CONNECTION_EVENT ev;

    memset(&ev, 0, sizeof(ev));
    ev.Type = QUIC_CONNECTION_EVENT_DATAGRAM_STATE_CHANGED;
    ev.DATAGRAM_STATE_CHANGED.SendEnabled = enabled ? TRUE : FALSE;
    ev.DATAGRAM_STATE_CHANGED.MaxSendLength = max;
    moq_msquic_conn_callback()(fake_msq_conn_handle(&r->fake), r->conn,
                               &ev);
}

static bool deliver_dgram_final(struct rig *r,
                                QUIC_DATAGRAM_SEND_STATE state)
{
    void *ctx = fake_msq_next_dgram_ctx(&r->fake);
    QUIC_CONNECTION_EVENT ev;

    if (ctx == NULL)
        return false;
    memset(&ev, 0, sizeof(ev));
    ev.Type = QUIC_CONNECTION_EVENT_DATAGRAM_SEND_STATE_CHANGED;
    ev.DATAGRAM_SEND_STATE_CHANGED.ClientContext = ctx;
    ev.DATAGRAM_SEND_STATE_CHANGED.State = state;
    moq_msquic_conn_callback()(fake_msq_conn_handle(&r->fake), r->conn,
                               &ev);
    return true;
}

/* --- doorbell wake service path --------------------------------------------------- */

struct wake_rig_ctx {
    struct rig *r;
    int calls;
    bool stage_write;          /* hook stages one small write */
    uint64_t stream_id;
    bool saw_nonfatal_dead;    /* hook ran while a latched flush
                                  failure had not yet been fed */
    moq_rcbuf_t *retry_rcbuf;  /* hook resubmits this payload once */
    moq_transport_result_t retry_rc;
};

static void wake_rig_hook(moq_msquic_conn_t *conn, void *user)
{
    struct wake_rig_ctx *wc = user;

    wc->calls++;
    if (conn->flush_failed && !moq_msquic_conn_is_fatal(conn))
        wc->saw_nonfatal_dead = true;
    if (wc->stage_write) {
        static const uint8_t bytes[32] = { 0x42 };

        wc->stage_write = false;
        (void)moq_msquic_test_ops(conn)->write(conn, wc->stream_id,
                                               bytes, sizeof(bytes),
                                               false);
    }
    if (wc->retry_rcbuf != NULL) {
        moq_rcbuf_t *buf = wc->retry_rcbuf;

        wc->retry_rcbuf = NULL;
        wc->retry_rc = moq_msquic_test_ops(conn)->write_payload(
            conn, wc->stream_id, buf, false);
    }
}

/* The doorbell's cheap wake path: a bare wake still runs the hook
 * exactly once; a wake whose hook queues data flushes that data to the
 * transport in the same cycle; and a wake finding a latched flush
 * failure diverts to the full ordered service pass, so the hook never
 * observes a healthy-looking transport that is already dead. */
static void t_wake_service_path(void)
{
    int before = failures;
    struct rig r;
    struct wake_rig_ctx wc;
    uint64_t id = 0;

    memset(&wc, 0, sizeof(wc));
    CHECK(rig_up_hooked(&r, MOQ_PERSPECTIVE_CLIENT, wake_rig_hook,
                        &wc) == 0);
    wc.r = &r;
    deliver_streams_available(&r, 8, 8);
    CHECK(r.ops->open_uni(r.conn, &id) == MOQ_TRANSPORT_OK);
    wc.stream_id = id;

    /* bare wake: hook runs exactly once, nothing is sent */
    int base_calls = wc.calls;
    int base_sends = r.fake.send_count;

    moq_msq_conn_service_wake(r.conn);
    CHECK(wc.calls == base_calls + 1);
    CHECK(r.fake.send_count == base_sends);

    /* a wake whose hook queues a write must flush it this cycle — no
     * transport event will come to do it later */
    wc.stage_write = true;
    base_calls = wc.calls;
    moq_msq_conn_service_wake(r.conn);
    CHECK(wc.calls == base_calls + 1);
    CHECK(r.fake.send_count == base_sends + 1);
    CHECK(fake_msq_deliver_send_complete(&r.fake, false));

    /* latch a flush failure, then wake: the terminal-ish state diverts
     * to the full service pass, which feeds the fatal before any hook
     * could see the dead transport as healthy */
    static const uint8_t tail[16] = { 0x24 };

    r.fake.stream_send_fails = 1;
    CHECK(r.ops->write(r.conn, id, tail, sizeof(tail), false) ==
          MOQ_TRANSPORT_OK);
    moq_msquic_test_flush(r.conn);
    CHECK(!moq_msquic_conn_is_fatal(r.conn));
    moq_msq_conn_service_wake(r.conn);
    CHECK(moq_msquic_conn_is_fatal(r.conn));
    CHECK(!wc.saw_nonfatal_dead);

    rig_down(&r);
    if (failures == before)
        printf("PASS: wake_service_path\n");
}


/* A budget-parked publish must progress when the transport raises the
 * ideal send size: with the peer fully caught up there is no reverse
 * traffic, so the batch's SEND_COMPLETE (== full ACK) sits behind the
 * peer's delayed-ACK timer and cannot be the retry trigger. The
 * IDEAL_SEND_BUFFER_SIZE growth must run a service pass that retries
 * what the budget parked; a non-growing update must not. */
static void t_ideal_growth_retries_parked(void)
{
    int before = failures;
    struct rig r;
    struct wake_rig_ctx wc;
    uint64_t id = 0;

    memset(&wc, 0, sizeof(wc));
    CHECK(rig_up_hooked(&r, MOQ_PERSPECTIVE_CLIENT, wake_rig_hook,
                        &wc) == 0);
    wc.r = &r;
    deliver_streams_available(&r, 8, 8);
    CHECK(r.ops->open_uni(r.conn, &id) == MOQ_TRANSPORT_OK);
    wc.stream_id = id;

    /* fill the 1 MiB budget floor and verify a follow-up parks */
    moq_rcbuf_t *big = make_rcbuf(900 * 1024);
    moq_rcbuf_t *tail = make_rcbuf(256 * 1024);

    CHECK(big != NULL && tail != NULL);
    CHECK(r.ops->write_payload(r.conn, id, big, false) ==
          MOQ_TRANSPORT_OK);
    moq_msquic_test_flush(r.conn);
    CHECK(r.ops->write_payload(r.conn, id, tail, false) ==
          MOQ_TRANSPORT_WOULD_BLOCK);

    struct moq_msq_stream *ms = moq_msquic_test_stream_find(r.conn, id);
    QUIC_STREAM_EVENT sev;

    CHECK(ms != NULL);

    /* establish a nonzero baseline ideal (any first report grows from
     * "none"; the ceiling stays at the 1 MiB floor, so the tail stays
     * parked) */
    memset(&sev, 0, sizeof(sev));
    sev.Type = QUIC_STREAM_EVENT_IDEAL_SEND_BUFFER_SIZE;
    sev.IDEAL_SEND_BUFFER_SIZE.ByteCount = 1024 * 1024;
    moq_msquic_test_stream_callback()(fake_msq_conn_handle(&r.fake), ms,
                                      &sev);
    CHECK(r.ops->write_payload(r.conn, id, tail, false) ==
          MOQ_TRANSPORT_WOULD_BLOCK);

    /* a ceiling that did not grow is not a retry signal */
    int base_calls = wc.calls;
    int base_sends = r.fake.send_count;

    memset(&sev, 0, sizeof(sev));
    sev.Type = QUIC_STREAM_EVENT_IDEAL_SEND_BUFFER_SIZE;
    sev.IDEAL_SEND_BUFFER_SIZE.ByteCount = 64 * 1024;
    moq_msquic_test_stream_callback()(fake_msq_conn_handle(&r.fake), ms,
                                      &sev);
    CHECK(wc.calls == base_calls);
    CHECK(r.fake.send_count == base_sends);

    /* growth past the parked write's need runs the service pass: the
     * hook (standing in for the bridge's retry) resubmits, and the
     * write both passes the raised budget and flushes this pass —
     * no SEND_COMPLETE, no wake */
    wc.retry_rcbuf = tail;
    memset(&sev, 0, sizeof(sev));
    sev.Type = QUIC_STREAM_EVENT_IDEAL_SEND_BUFFER_SIZE;
    sev.IDEAL_SEND_BUFFER_SIZE.ByteCount = 2 * 1024 * 1024;
    moq_msquic_test_stream_callback()(fake_msq_conn_handle(&r.fake), ms,
                                      &sev);
    CHECK(wc.calls == base_calls + 1);
    CHECK(wc.retry_rc == MOQ_TRANSPORT_OK);
    CHECK(r.fake.send_count == base_sends + 1);

    CHECK(fake_msq_deliver_send_complete(&r.fake, false));
    CHECK(fake_msq_deliver_send_complete(&r.fake, false));
    moq_rcbuf_decref(big);
    moq_rcbuf_decref(tail);

    rig_down(&r);
    if (failures == before)
        printf("PASS: ideal_growth_retries_parked\n");
}

/* --- datagram send ------------------------------------------------------------ */

static void t_datagram_send(void)
{
    int before = failures;
    struct rig r;
    uint8_t bytes[32];

    CHECK(rig_up(&r, MOQ_PERSPECTIVE_CLIENT) == 0);

    /* transport datagrams not (yet) available: honest non-fatal drop,
     * nothing reaches the transport */
    CHECK(r.ops->send_datagram(r.conn, bytes, sizeof(bytes)) ==
          MOQ_TRANSPORT_DROPPED);
    CHECK(r.ops->max_datagram_size(r.conn) == 0);
    CHECK(r.fake.dgram_count == 0);

    deliver_dgram_state(&r, true, 1200);
    CHECK(r.ops->max_datagram_size(r.conn) == 1200);

    /* over the peer max: refused before the transport is touched */
    uint8_t big[1300];
    CHECK(r.ops->send_datagram(r.conn, big, sizeof(big)) ==
          MOQ_TRANSPORT_TOO_LARGE);
    CHECK(r.fake.dgram_count == 0);

    /* accepted send copies: the caller's buffer is released at return,
     * scribbling it must not corrupt the in-flight datagram */
    memset(bytes, 0xcd, sizeof(bytes));
    CHECK(r.ops->send_datagram(r.conn, bytes, sizeof(bytes)) ==
          MOQ_TRANSPORT_OK);
    memset(bytes, 0x00, sizeof(bytes));
    CHECK(r.fake.dgram_count == 1);
    CHECK(r.fake.dgrams[0].len == sizeof(bytes));
    CHECK(r.fake.dgrams[0].data[0] == 0xcd);
    CHECK(r.fake.dgrams[0].data[31] == 0xcd);

    /* the FINAL state releases the record exactly once (ASan owns the
     * exactly-once proof) and frees the in-flight slot */
    CHECK(deliver_dgram_final(&r, QUIC_DATAGRAM_SEND_ACKNOWLEDGED));
    CHECK(fake_msq_pending_dgrams(&r.fake) == 0);

    /* a synchronously refused send frees its record and stays
     * non-fatal */
    memset(bytes, 0x11, sizeof(bytes));
    r.fake.dgram_send_fails = 1;
    CHECK(r.ops->send_datagram(r.conn, bytes, sizeof(bytes)) ==
          MOQ_TRANSPORT_DROPPED);
    CHECK(!moq_msquic_conn_is_fatal(r.conn));

    rig_down(&r);
    if (failures == before)
        printf("PASS: datagram_send\n");
}

static void t_datagram_cap(void)
{
    int before = failures;
    struct rig r;
    uint8_t bytes[16];

    CHECK(rig_up(&r, MOQ_PERSPECTIVE_CLIENT) == 0);
    deliver_dgram_state(&r, true, 1200);
    memset(bytes, 0x42, sizeof(bytes));

    /* fill the in-flight cap; the next send parks with no transport
     * call and nothing accepted */
    for (int i = 0; i < 64; i++)
        CHECK(r.ops->send_datagram(r.conn, bytes, sizeof(bytes)) ==
              MOQ_TRANSPORT_OK);
    CHECK(r.fake.dgram_count == 64);
    CHECK(r.ops->send_datagram(r.conn, bytes, sizeof(bytes)) ==
          MOQ_TRANSPORT_WOULD_BLOCK);
    CHECK(r.fake.dgram_count == 64);

    /* one final frees one slot */
    CHECK(deliver_dgram_final(&r, QUIC_DATAGRAM_SEND_ACKNOWLEDGED));
    CHECK(r.ops->send_datagram(r.conn, bytes, sizeof(bytes)) ==
          MOQ_TRANSPORT_OK);

    /* shutdown-style CANCELED finals drain every outstanding record —
     * MsQuic guarantees them all before SHUTDOWN_COMPLETE, so this is
     * the complete teardown path (ASan pins no leak, no double free) */
    while (deliver_dgram_final(&r, QUIC_DATAGRAM_SEND_CANCELED))
        ;
    CHECK(fake_msq_pending_dgrams(&r.fake) == 0);

    rig_down(&r);
    if (failures == before)
        printf("PASS: datagram_cap\n");
}

static void t_datagram_too_large(void)
{
    int before = failures;
    fake_msq_t fake;
    moq_session_t *session = NULL;
    moq_msquic_conn_t *conn = NULL;
    moq_session_cfg_t scfg;
    moq_alloc_t alloc = {
        .ctx = NULL,
        .alloc = counting_alloc,
        .realloc = counting_realloc,
        .free = counting_free,
    };
    uint8_t byte = 0;

    fake_msq_init(&fake, true);
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_CLIENT);
    CHECK(moq_session_create(&scfg, 0, &session) == 0);

    moq_msquic_conn_cfg_t cfg;
    moq_msquic_conn_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = &alloc;
    cfg.session = session;
    cfg.api = fake_msq_table(&fake);
    CHECK(moq_msquic_conn_create(&cfg, &conn) == MOQ_OK);
    CHECK(moq_msquic_conn_bind(conn, fake_msq_conn_handle(&fake)) ==
          MOQ_OK);

    const moq_transport_endpoint_ops_t *ops = moq_msquic_test_ops(conn);

#if SIZE_MAX > UINT32_MAX
    int allocs_before = count_allocs;

    /* the data pointer is a single byte: the op must reject on length
     * alone, touching neither the bytes, the allocator, nor the
     * transport — regardless of datagram availability */
    CHECK(ops->send_datagram(conn, &byte, (size_t)UINT32_MAX + 1) ==
          MOQ_TRANSPORT_TOO_LARGE);
    CHECK(count_allocs == allocs_before);
    CHECK(fake.dgram_count == 0);
#else
    (void)byte;
    (void)ops;
#endif

    moq_msquic_conn_destroy(conn);
    moq_session_destroy(session);
    if (failures == before)
        printf("PASS: datagram_too_large\n");
}

/* --- accepted-flush accounting ------------------------------------------------- */

/* flush_sends/flush_bytes count ACCEPTED StreamSend batches and their
 * exact byte totals — staging attempts and synchronous failures never
 * count (a failed flush latches flush_failed and returns ownership, so
 * counting it would invent wire traffic that never happened). */
static void t_flush_accounting(void)
{
    int before = failures;
    struct rig r;
    uint64_t id = 0;
    uint8_t bytes[250];

    memset(bytes, 0x6b, sizeof(bytes));
    CHECK(rig_up(&r, MOQ_PERSPECTIVE_CLIENT) == 0);
    deliver_streams_available(&r, 8, 8);
    CHECK(r.conn->flush_sends == 0 && r.conn->flush_bytes == 0);
    CHECK(r.ops->open_uni(r.conn, &id) == MOQ_TRANSPORT_OK);

    /* two accepted batches: exact send count and exact byte sum */
    CHECK(r.ops->write(r.conn, id, bytes, 100, false) == MOQ_TRANSPORT_OK);
    moq_msquic_test_flush(r.conn);
    CHECK(r.conn->flush_sends == 1);
    CHECK(r.conn->flush_bytes == 100);
    CHECK(fake_msq_deliver_send_complete(&r.fake, false));
    CHECK(r.ops->write(r.conn, id, bytes, 250, false) == MOQ_TRANSPORT_OK);
    moq_msquic_test_flush(r.conn);
    CHECK(r.conn->flush_sends == 2);
    CHECK(r.conn->flush_bytes == 350);
    CHECK(fake_msq_deliver_send_complete(&r.fake, false));

    /* a synchronous StreamSend failure is NOT an accepted flush */
    CHECK(r.ops->write(r.conn, id, bytes, 50, false) == MOQ_TRANSPORT_OK);
    r.fake.stream_send_fails = 1;
    moq_msquic_test_flush(r.conn);
    CHECK(r.conn->flush_sends == 2);   /* unchanged */
    CHECK(r.conn->flush_bytes == 350); /* unchanged */
    rig_down(&r);

    if (failures == before)
        printf("PASS: flush accounting (accepted-only, exact bytes)\n");
}

int main(void)
{
    t_cfg_validation();
    t_connected_opens_control();
    t_stream_id_allocation();
    t_id_mismatch_fatal();
    t_budget_depth_not_size();
    t_receive_arrest_holds_while_paused();
    t_receive_resume_failure_fatal();
    t_receive_zero_fin_while_paused();
    t_receive_multibuffer_full_accept();
    t_receive_terminal_while_paused();
    t_receive_multibuffer_partial_hold();
    t_receive_capacity_invariance(1);
    t_receive_capacity_invariance(2);
    t_receive_capacity_invariance(4);
    t_receive_capacity_invariance(64);
    t_event_progress_token_semantics();
    t_settings_required();
    t_rcbuf_lifetime();
    t_cold_write_copies();
    t_cold_write_too_large();
    t_wake_service_path();
    t_ideal_growth_retries_parked();
    t_record_pool_reuse();
    t_batch_coalescing();
    t_batch_cap_split();
    t_flush_failure_ordering();
    t_flush_failure_in_op();
    t_credit_gating();
    t_teardown_ops();
    t_datagram_send();
    t_datagram_cap();
    t_datagram_too_large();
    t_flush_accounting();

    if (failures == 0)
        printf("PASS: msquic_unit\n");
    else
        fprintf(stderr, "FAIL: msquic_unit (%d)\n", failures);
    return failures;
}
