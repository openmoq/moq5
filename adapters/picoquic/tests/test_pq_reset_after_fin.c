/*
 * picoquic local-stream reset after FIN (patches/picoquic/0002), and the
 * provider reset rules the adapters build on.
 *
 * RFC 9000 Section 3.1 lets a sender abandon a stream with RESET_STREAM in
 * the "Data Sent" state, after the FIN has been sent but before every byte
 * and the FIN are acknowledged. Once all of it is acknowledged ("Data
 * Recvd", a terminal state) there is nothing left to abandon: a reset
 * requested then is refused. The draft-18 subgroup DELIVERY_TIMEOUT relies
 * on this: a subgroup whose FIN is in flight must still be cancellable.
 *
 * picoquic queues RESET_STREAM / RESET_STREAM_AT as soon as it accepts the
 * reset, with the bytes sent so far as the Final Size, and frees whatever is
 * still queued. A Reliable Size beyond the bytes sent is refused with
 * PICOQUIC_ERROR_OFFSET_TOO_BIG; a refused reset leaves the stream, and its
 * application context, unchanged. A reliable reset's stream retires once its
 * prefix, bytes 0 .. reliable_size - 1, and the reset are acknowledged; until
 * the prefix is acknowledged the stream can still be activated to send it.
 *
 * Constructed rows set stream and SACK state directly to pin one boundary
 * each. Packet rows drive two real picoquic endpoints over the tls_api
 * simulated links, with virtual time and a loss mask, so the frames really
 * go on the wire.
 */
#include <picoquictest_internal.h>
#include <picoquic_utils.h>
#include <inttypes.h>
#include <stdio.h>
#include <string.h>

/* Provider-internal entry points, used only by the constructed rows. */
int picoquic_process_ack_of_stream_frame(picoquic_cnx_t *, uint8_t *, size_t, size_t *);
int picoquic_process_ack_of_reset_stream_at_frame(picoquic_cnx_t *, const uint8_t *, size_t, size_t *);

static int failures;
static const char *row;
#define CHECK(x) do { if (!(x)) { \
    fprintf(stderr, "FAIL[%s]:%d: %s\n", row, __LINE__, #x); failures++; \
} } while (0)

typedef struct {
    picoquic_test_tls_api_ctx_t *tls;
    uint64_t now, loss, blocked, sid, reset_code;
    unsigned resets, fins;
    size_t received;
    int saw_reset_sent;
    uint8_t rx[64];          /* received bytes, in delivery order */
    size_t rx_len;
    size_t rx_at_reset;      /* rx_len when the reset was signalled */
    int data_action;         /* server data callback: see rx_action_t */
    int data_action_done;
    void *reset_stream_ctx;  /* stream context the reset was signalled with */
} pair_t;

/* What the server's data callback does on its first data event. */
typedef enum {
    RX_PLAIN = 0,
    RX_FAIL,          /* returns an error */
    RX_STOP,          /* sends STOP_SENDING on the stream */
    RX_DISCARD,       /* discards the stream */
    RX_REPLACE_CTX,   /* replaces the stream's application context */
    RX_CLOSE,         /* closes the connection */
} rx_action_t;

static int replacement_ctx;

static int callback(picoquic_cnx_t *cnx, uint64_t sid, uint8_t *bytes,
                    size_t length, picoquic_call_back_event_t event,
                    void *context, void *stream_context)
{
    pair_t *p = context;
    if (event == picoquic_callback_stream_data || event == picoquic_callback_stream_fin) {
        p->received += length;
        for (size_t i = 0; i < length && bytes != NULL && p->rx_len < sizeof(p->rx); i++)
            p->rx[p->rx_len++] = bytes[i];
        if (event == picoquic_callback_stream_fin) p->fins++;
        if (p->data_action != RX_PLAIN && !p->data_action_done &&
            cnx == p->tls->cnx_server) {
            p->data_action_done = 1;
            switch (p->data_action) {
            case RX_FAIL: return -1;
            case RX_STOP: (void)picoquic_stop_sending(cnx, sid, 9); break;
            case RX_DISCARD: (void)picoquic_discard_stream(cnx, sid, 9); break;
            case RX_REPLACE_CTX: (void)picoquic_set_app_stream_ctx(cnx, sid, &replacement_ctx); break;
            case RX_CLOSE: (void)picoquic_close(cnx, 0); break;
            default: break;
            }
        }
    } else if (event == picoquic_callback_stream_reset) {
        p->resets++;
        p->rx_at_reset = p->rx_len;
        p->reset_stream_ctx = stream_context;
        CHECK(picoquic_get_remote_stream_error(cnx, sid) == p->reset_code);
    }
    return 0;
}

static int setup(pair_t *p)
{
    memset(p, 0, sizeof(*p));
    picoquic_solution_dir = PICOQUIC_SOURCE_DIR;
    if (tls_api_init_ctx(&p->tls, PICOQUIC_INTERNAL_TEST_VERSION_1,
            PICOQUIC_TEST_SNI, PICOQUIC_TEST_ALPN, &p->now, NULL, NULL, 0, 1, 0) != 0)
        return -1;
    picoquic_set_callback(p->tls->cnx_client, callback, p);
    picoquic_set_default_callback(p->tls->qserver, callback, p);
    picoquic_tp_t tp = *picoquic_get_transport_parameters(p->tls->cnx_client, 1);
    tp.is_reset_stream_at_enabled = 1;
    picoquic_set_transport_parameters(p->tls->cnx_client, &tp);
    picoquic_set_default_tp_value(p->tls->qserver, picoquic_tp_reset_stream_at, 1);
    if (picoquic_start_client_cnx(p->tls->cnx_client) != 0 ||
        tls_api_connection_loop(p->tls, &p->loss, 0, &p->now) != 0) return -1;
    return 0;
}

/* Runs the simulation; records whether the client's stream p->sid was ever
 * seen with its reset on the wire. */
static void pump(pair_t *p, uint64_t duration)
{
    uint64_t limit = p->now + duration;
    for (unsigned i = 0; i < 10000 && p->now < limit; i++) {
        int active = 0;
        CHECK(tls_api_one_sim_round(p->tls, &p->now, limit, &active) == 0);
        picoquic_stream_head_t *st = picoquic_find_stream(p->tls->cnx_client, p->sid);
        if (st != NULL && st->reset_sent) p->saw_reset_sent = 1;
    }
}

static void open_stream(pair_t *p, int unidir)
{
    p->sid = picoquic_get_next_local_stream_id(p->tls->cnx_client, unidir);
    CHECK(picoquic_set_app_stream_ctx(p->tls->cnx_client, p->sid, p) == 0);
}

/* A local stream whose five bytes and FIN were sent. fin_offset stays 0:
 * picoquic sets it only from received frames, so it says nothing about
 * what this endpoint sent. A UNI stream is closed once its FIN is sent; a
 * BIDI stream stays open while the peer's direction is. */
static picoquic_stream_head_t *fin_sent_stream(pair_t *p, int unidir)
{
    open_stream(p, unidir);
    picoquic_stream_head_t *st = picoquic_find_stream(p->tls->cnx_client, p->sid);
    CHECK(st != NULL);
    if (st == NULL) return NULL;
    st->sent_offset = 5;
    st->fin_requested = st->fin_sent = 1;
    st->is_closed = unidir ? 1 : 0;
    CHECK(st->fin_offset == 0);
    /* The scheduler drops a stream from the output list once its FIN is sent. */
    picoquic_remove_output_stream(p->tls->cnx_client, st);
    CHECK(!st->is_output_stream);
    return st;
}

/* acked: 0 = nothing, 1 = every byte but not the FIN, 2 = bytes and FIN. */
static void constructed_reset(unsigned acked)
{
    static const char *names[] = {
        "constructed-reset-after-fin-unacked",
        "constructed-reset-after-fin-bytes-acked",
        "constructed-reset-after-fin-acked-refused",
    };
    pair_t p;
    row = names[acked];
    CHECK(setup(&p) == 0);
    picoquic_cnx_t *cnx = p.tls->cnx_client;
    picoquic_stream_head_t *st = fin_sent_stream(&p, 1);
    if (st == NULL) { tls_api_delete_ctx(p.tls); return; }
    /* The FIN occupies the position after the last byte in the SACK list. */
    if (acked) CHECK(picoquic_update_sack_list(&st->sack_list, 0, acked == 1 ? 4 : 5, 0) == 0);
    int rc = picoquic_reset_stream(cnx, p.sid, 42);
    if (acked == 2) {
        /* Refused without side effects: the application keeps its context. */
        CHECK(rc == PICOQUIC_ERROR_STREAM_ALREADY_CLOSED);
        CHECK(!st->reset_requested && st->app_stream_ctx == &p);
    } else {
        CHECK(rc == 0 && st->reset_requested && st->local_error == 42);
        CHECK(st->app_stream_ctx == NULL);
        /* The reset is queued at once, with the bytes sent as Final Size. */
        CHECK(st->reset_sent);
        /* A repeated reset is accepted and keeps the first error code. */
        CHECK(picoquic_reset_stream(cnx, p.sid, 99) == 0);
        CHECK(st->local_error == 42);
    }
    tls_api_delete_ctx(p.tls);
}

/* Stream state with a gap at byte 2; the FIN position (5) is acknowledged. */
static picoquic_stream_head_t *gapped_stream(pair_t *p, int unidir)
{
    picoquic_stream_head_t *st = fin_sent_stream(p, unidir);
    if (st == NULL) return NULL;
    CHECK(picoquic_update_sack_list(&st->sack_list, 0, 1, 0) == 0);
    CHECK(picoquic_update_sack_list(&st->sack_list, 3, 5, 0) == 0);
    (void)picoquic_delete_stream_if_closed(p->tls->cnx_client, st);
    CHECK(picoquic_find_stream(p->tls->cnx_client, p->sid) == st);
    return st;
}

/* STREAM|OFF|LEN at offset 2, one byte: fills the gap. */
static void ack_gap(pair_t *p)
{
    uint8_t gap_ack[] = { 0x0e, (uint8_t)p->sid, 2, 1, 'c' };
    size_t consumed = 0;
    CHECK(picoquic_process_ack_of_stream_frame(p->tls->cnx_client, gap_ack,
        sizeof(gap_ack), &consumed) == 0);
}

static void constructed_empty_ack(void)
{
    pair_t p;
    row = "constructed-empty-ack-range";
    CHECK(setup(&p) == 0);
    picoquic_cnx_t *cnx = p.tls->cnx_client;
    picoquic_stream_head_t *st = gapped_stream(&p, 1);
    if (st == NULL) { tls_api_delete_ctx(p.tls); return; }
    /* STREAM|LEN, offset 0, length 0, no FIN: acknowledges nothing. It must
     * not become the range [0, UINT64_MAX] and retire the stream. */
    uint8_t zero_ack[] = { 0x0a, (uint8_t)p.sid, 0 };
    size_t consumed = 0;
    CHECK(picoquic_process_ack_of_stream_frame(cnx, zero_ack, sizeof(zero_ack), &consumed) == 0);
    CHECK(consumed == sizeof(zero_ack));
    CHECK(picoquic_find_stream(cnx, p.sid) == st);
    if (picoquic_find_stream(cnx, p.sid) == st)
        CHECK(!picoquic_check_sack_list(&st->sack_list, 0, 5));
    tls_api_delete_ctx(p.tls);
}

/* Control: the ACK that completes bytes and FIN retires a stream that has
 * no reset. */
static void constructed_fin_ack_retires(void)
{
    pair_t p;
    row = "constructed-fin-ack-retires";
    CHECK(setup(&p) == 0);
    picoquic_stream_head_t *st = gapped_stream(&p, 1);
    if (st == NULL) { tls_api_delete_ctx(p.tls); return; }
    ack_gap(&p);
    CHECK(picoquic_find_stream(p.tls->cnx_client, p.sid) == NULL);
    tls_api_delete_ctx(p.tls);
}

/* An ACK that leaves a gap is not the end of the stream: a reset accepted
 * after the FIN stays in force through it, and the stream waits for the
 * reset's own acknowledgement. */
static void constructed_partial_ack_keeps_reset(void)
{
    pair_t p;
    row = "constructed-partial-ack-keeps-reset";
    CHECK(setup(&p) == 0);
    picoquic_cnx_t *cnx = p.tls->cnx_client;
    picoquic_stream_head_t *st = fin_sent_stream(&p, 1);
    if (st == NULL) { tls_api_delete_ctx(p.tls); return; }
    CHECK(picoquic_reset_stream(cnx, p.sid, 12) == 0);
    /* Bytes 3..4 and the FIN: byte 0..2 remain unacknowledged. */
    uint8_t tail_ack[] = { 0x0f, (uint8_t)p.sid, 3, 2, 'd', 'e' };
    size_t consumed = 0;
    CHECK(picoquic_process_ack_of_stream_frame(cnx, tail_ack, sizeof(tail_ack), &consumed) == 0);
    st = picoquic_find_stream(cnx, p.sid);
    CHECK(st != NULL);
    if (st != NULL) {
        CHECK(st->reset_requested && st->reset_sent && st->local_error == 12);
    }
    tls_api_delete_ctx(p.tls);
}

/* A reliable reset after FIN whose prefix is not acknowledged keeps the
 * stream, even when the FIN position and the rest are acknowledged. */
static void constructed_prefix_unacked_keeps_reset(void)
{
    pair_t p;
    row = "constructed-prefix-unacked-keeps-reset";
    CHECK(setup(&p) == 0);
    picoquic_cnx_t *cnx = p.tls->cnx_client;
    picoquic_stream_head_t *st = fin_sent_stream(&p, 1);
    if (st == NULL) { tls_api_delete_ctx(p.tls); return; }
    CHECK(picoquic_update_sack_list(&st->sack_list, 3, 5, 0) == 0);
    CHECK(picoquic_reset_stream_at(cnx, p.sid, 12, 3) == 0);
    /* Byte 0 only: bytes 1..2 of the three-byte prefix stay unacknowledged. */
    uint8_t first_ack[] = { 0x0a, (uint8_t)p.sid, 1, 'a' };
    size_t consumed = 0;
    CHECK(picoquic_process_ack_of_stream_frame(cnx, first_ack, sizeof(first_ack), &consumed) == 0);
    st = picoquic_find_stream(cnx, p.sid);
    CHECK(st != NULL);
    if (st != NULL) CHECK(st->reset_requested && st->reliable_size == 3);
    tls_api_delete_ctx(p.tls);
}

/* Once the reset is queued, the stream is in "Reset Sent": an ACK that
 * completes the FIN does not retire it before the reset is acknowledged,
 * and a repeated reset is accepted and keeps the first error code. */
static void constructed_repeat_reset_after_fin_ack(void)
{
    pair_t p;
    row = "constructed-repeat-reset-after-fin-ack";
    CHECK(setup(&p) == 0);
    picoquic_cnx_t *cnx = p.tls->cnx_client;
    picoquic_stream_head_t *st = gapped_stream(&p, 1);
    if (st == NULL) { tls_api_delete_ctx(p.tls); return; }
    CHECK(picoquic_reset_stream(cnx, p.sid, 12) == 0);
    CHECK(st->reset_sent);
    ack_gap(&p);
    st = picoquic_find_stream(cnx, p.sid);
    CHECK(st != NULL);
    if (st != NULL) {
        CHECK(picoquic_check_sack_list(&st->sack_list, 0, 5));
        CHECK(picoquic_reset_stream(cnx, p.sid, 99) == 0);
        CHECK(st->reset_requested && st->local_error == 12);
    }
    tls_api_delete_ctx(p.tls);
}

/* The Reliable Size may not exceed the bytes sent, which become the Final
 * Size. The refusal changes nothing: the stream is not reset and keeps its
 * application context. */
static void constructed_reliable_size_bound(int fin)
{
    pair_t p;
    row = fin ? "constructed-reliable-size-above-final-after-fin"
              : "constructed-reliable-size-above-final";
    CHECK(setup(&p) == 0);
    picoquic_cnx_t *cnx = p.tls->cnx_client;
    picoquic_stream_head_t *st;
    if (fin) {
        st = fin_sent_stream(&p, 1);
    } else {
        open_stream(&p, 1);
        st = picoquic_find_stream(cnx, p.sid);
        if (st != NULL) st->sent_offset = 5;
    }
    CHECK(st != NULL);
    if (st == NULL) { tls_api_delete_ctx(p.tls); return; }
    CHECK(picoquic_reset_stream_at(cnx, p.sid, 12, 6) == PICOQUIC_ERROR_OFFSET_TOO_BIG);
    CHECK(!st->reset_requested && !st->reset_sent && st->app_stream_ctx == &p);
    CHECK(picoquic_reset_stream_at(cnx, p.sid, 12, 5) == 0);
    CHECK(st->reset_requested && st->reliable_size == 5 && st->app_stream_ctx == NULL);
    tls_api_delete_ctx(p.tls);
}

/* Bytes queued but not yet sent do not count toward the Final Size: a
 * reliable reset covering a queued WebTransport-style preamble is refused
 * and leaves the queue in place, and a plain reset frees it. */
static void constructed_queued_bytes_not_final(void)
{
    pair_t p;
    row = "constructed-queued-bytes-not-final";
    CHECK(setup(&p) == 0);
    picoquic_cnx_t *cnx = p.tls->cnx_client;
    open_stream(&p, 1);
    CHECK(picoquic_add_to_stream_with_ctx(cnx, p.sid, (const uint8_t *)"\x40\x54\x00", 3, 0, &p) == 0);
    picoquic_stream_head_t *st = picoquic_find_stream(cnx, p.sid);
    CHECK(st != NULL && st->sent_offset == 0);
    if (st == NULL) { tls_api_delete_ctx(p.tls); return; }
    CHECK(picoquic_reset_stream_at(cnx, p.sid, 12, 3) == PICOQUIC_ERROR_OFFSET_TOO_BIG);
    CHECK(!st->reset_requested && st->send_queue != NULL && st->app_stream_ctx == &p);
    CHECK(picoquic_reset_stream(cnx, p.sid, 12) == 0);
    CHECK(st->reset_requested && st->reset_sent && st->send_queue == NULL);
    tls_api_delete_ctx(p.tls);
}

/* A received RESET_STREAM_AT whose Reliable Size exceeds its Final Size is
 * a FRAME_ENCODING_ERROR (draft-ietf-quic-reliable-stream-reset Section 4). */
static void constructed_reset_at_receive_bound(void)
{
    pair_t p;
    row = "constructed-reset-at-reliable-above-final";
    CHECK(setup(&p) == 0);
    picoquic_cnx_t *srv = p.tls->cnx_server;
    CHECK(srv != NULL);
    if (srv == NULL) { tls_api_delete_ctx(p.tls); return; }
    /* Client-initiated UNI stream 2: final size 2, reliable size 2 is valid. */
    static const uint8_t ok[] = { 0x24, 0x02, 0x07, 0x02, 0x02 };
    picoquic_state_enum before = srv->cnx_state;
    CHECK(before < picoquic_state_disconnecting);
    CHECK(picoquic_decode_reset_stream_at_frame(srv, ok, ok + sizeof(ok)) == ok + sizeof(ok));
    CHECK(srv->cnx_state == before);
    /* Stream 6: final size 2, reliable size 3. */
    static const uint8_t bad[] = { 0x24, 0x06, 0x07, 0x02, 0x03 };
    CHECK(picoquic_decode_reset_stream_at_frame(srv, bad, bad + sizeof(bad)) == NULL);
    CHECK(srv->cnx_state == picoquic_state_disconnecting);
    CHECK(srv->local_error == PICOQUIC_TRANSPORT_FRAME_FORMAT_ERROR);
    tls_api_delete_ctx(p.tls);
}

/* One packet carrying RESET_STREAM_AT for stream A, then STREAM+FIN for
 * stream B. Its acknowledgement must retire A's reset and still read B's
 * frame, so the RESET_STREAM_AT handler has to consume all four fields.
 * prefix_acked = 1: A's reliable prefix (bytes 0..1) was acknowledged before
 * the reset, so the reset's acknowledgement retires A. prefix_acked = 0: the
 * prefix is still outstanding, so A stays until it is acknowledged. */
static void constructed_reset_at_ack(int prefix_acked)
{
    pair_t p;
    row = prefix_acked ? "constructed-reset-stream-at-ack"
                       : "constructed-reset-stream-at-ack-prefix-outstanding";
    CHECK(setup(&p) == 0);
    picoquic_cnx_t *cnx = p.tls->cnx_client;
    const uint64_t a = 2, b = 6; /* client-initiated UNI */
    CHECK(picoquic_set_app_stream_ctx(cnx, a, &p) == 0);
    CHECK(picoquic_set_app_stream_ctx(cnx, b, &p) == 0);
    picoquic_stream_head_t *sa = picoquic_find_stream(cnx, a);
    picoquic_stream_head_t *sb = picoquic_find_stream(cnx, b);
    CHECK(sa != NULL && sb != NULL);
    if (sa == NULL || sb == NULL) { tls_api_delete_ctx(p.tls); return; }
    sa->sent_offset = 5;
    sa->local_error = 7;
    sa->reliable_size = 2;
    sa->reset_requested = sa->reset_sent = 1;
    if (prefix_acked)
        CHECK(picoquic_update_sack_list(&sa->sack_list, 0, 1, 0) == 0);
    else
        CHECK(picoquic_update_sack_list(&sa->sack_list, 0, 0, 0) == 0);
    sb->sent_offset = 3;
    sb->fin_requested = sb->fin_sent = 1;
    picoquic_packet_t *pkt = picoquic_create_packet(cnx->quic);
    CHECK(pkt != NULL);
    if (pkt == NULL) { tls_api_delete_ctx(p.tls); return; }
    static const uint8_t frames[] = {
        0x24, 0x02, 0x07, 0x05, 0x02,                 /* RESET_STREAM_AT a */
        0x0f, 0x06, 0x00, 0x03, 'x', 'y', 'z',        /* STREAM b, FIN */
    };
    memcpy(pkt->bytes, frames, sizeof(frames));
    pkt->offset = 0;
    pkt->length = sizeof(frames);
    pkt->ptype = picoquic_packet_1rtt_protected;
    pkt->send_path = NULL;
    picoquic_process_ack_of_frames(cnx, pkt, 0);
    picoquic_recycle_packet(cnx->quic, pkt);
    CHECK(picoquic_find_stream(cnx, b) == NULL);
    if (prefix_acked) {
        CHECK(picoquic_find_stream(cnx, a) == NULL);
    } else {
        /* Not retired early: the reset is acknowledged, its prefix is not. */
        sa = picoquic_find_stream(cnx, a);
        CHECK(sa != NULL && sa->reset_acked);
        /* The last prefix byte's acknowledgement retires it. */
        uint8_t prefix_ack[] = { 0x0e, 0x02, 0x01, 0x01, 'b' };
        size_t consumed = 0;
        CHECK(picoquic_process_ack_of_stream_frame(cnx, prefix_ack, sizeof(prefix_ack), &consumed) == 0);
        CHECK(picoquic_find_stream(cnx, a) == NULL);
    }
    tls_api_delete_ctx(p.tls);
}

/* Receiver side of a reliable reset: the bytes below the Reliable Size
 * reach the application exactly once and in order, then the reset, signalled
 * once. Frames are fed to the server for the client's UNI stream 2 (final
 * size 10, error 7).
 *   order = 0: "def" at 3 arrives before the reset and "abc" at 0 after it,
 *              Reliable Size 6: the chunk that completes the prefix is the
 *              queued, out-of-order one; "abcdef" is delivered, then the
 *              reset.
 *   order = 1: the reset (Reliable Size 4) arrives first, then "abcdef" as
 *              one chunk that crosses the Reliable Size: "abcd", then the
 *              reset.
 *   order = 2: "abcdef" is consumed before the reset (Reliable Size 4)
 *              arrives; the reset is signalled at once.
 * Later bytes past the Reliable Size ("ghij" at 6) and a repeated reset
 * produce no further event: once the reset is signalled the stream is done
 * for the application (a choice of this provider; the draft requires only
 * delivery through the Reliable Size). */
static void constructed_reliable_prefix_delivery(int order)
{
    static const char *names[] = {
        "constructed-reliable-prefix-out-of-order",
        "constructed-reliable-prefix-crossing-chunk",
        "constructed-reliable-prefix-already-consumed",
    };
    pair_t p;
    row = names[order];
    CHECK(setup(&p) == 0);
    picoquic_cnx_t *srv = p.tls->cnx_server;
    CHECK(srv != NULL);
    if (srv == NULL) { tls_api_delete_ctx(p.tls); return; }
    p.reset_code = 7;
    static const uint8_t def[] = { 0x0e, 0x02, 0x03, 0x03, 'd', 'e', 'f' };
    static const uint8_t abc[] = { 0x0a, 0x02, 0x03, 'a', 'b', 'c' };
    static const uint8_t abcdef[] = { 0x0a, 0x02, 0x06, 'a', 'b', 'c', 'd', 'e', 'f' };
    static const uint8_t ghij[] = { 0x0e, 0x02, 0x06, 0x04, 'g', 'h', 'i', 'j' };
    static const uint8_t reset6[] = { 0x24, 0x02, 0x07, 0x0a, 0x06 };
    static const uint8_t reset4[] = { 0x24, 0x02, 0x07, 0x0a, 0x04 };
    const char *want = order == 0 ? "abcdef" : order == 1 ? "abcd" : "abcdef";
    const uint8_t *again = order == 0 ? reset6 : reset4;
    if (order == 0) {
        CHECK(picoquic_decode_stream_frame(srv, def, def + sizeof(def), NULL, p.now) != NULL);
        CHECK(picoquic_decode_reset_stream_at_frame(srv, reset6, reset6 + sizeof(reset6)) != NULL);
        CHECK(p.resets == 0 && p.rx_len == 0);
        CHECK(picoquic_decode_stream_frame(srv, abc, abc + sizeof(abc), NULL, p.now) != NULL);
    } else if (order == 1) {
        CHECK(picoquic_decode_reset_stream_at_frame(srv, reset4, reset4 + sizeof(reset4)) != NULL);
        CHECK(p.resets == 0);
        CHECK(picoquic_decode_stream_frame(srv, abcdef, abcdef + sizeof(abcdef), NULL, p.now) != NULL);
    } else {
        CHECK(picoquic_decode_stream_frame(srv, abcdef, abcdef + sizeof(abcdef), NULL, p.now) != NULL);
        CHECK(p.resets == 0 && p.rx_len == 6);
        CHECK(picoquic_decode_reset_stream_at_frame(srv, reset4, reset4 + sizeof(reset4)) != NULL);
    }
    CHECK(p.rx_len == strlen(want) && memcmp(p.rx, want, strlen(want)) == 0);
    CHECK(p.resets == 1 && p.fins == 0);
    CHECK(p.rx_at_reset == strlen(want));
    (void)picoquic_decode_stream_frame(srv, ghij, ghij + sizeof(ghij), NULL, p.now);
    (void)picoquic_decode_reset_stream_at_frame(srv, again, again + 5);
    CHECK(p.rx_len == strlen(want) && p.resets == 1);
    CHECK(srv->cnx_state < picoquic_state_disconnecting);
    tls_api_delete_ctx(p.tls);
}

/* The data callback for the chunk that completes a reliable prefix acts on
 * the stream or the connection. The reset follows the delivered prefix at
 * most once, never after a failed callback or a discard, and with the
 * application context in force at that time. Reset first (Reliable Size 4),
 * then "abcdef" as one chunk. */
static void constructed_reliable_prefix_callback_action(rx_action_t action)
{
    static const char *names[] = {
        "", "constructed-reliable-prefix-callback-fails",
        "constructed-reliable-prefix-callback-stops",
        "constructed-reliable-prefix-callback-discards",
        "constructed-reliable-prefix-callback-replaces-ctx",
        "constructed-reliable-prefix-callback-closes",
    };
    pair_t p;
    row = names[action];
    CHECK(setup(&p) == 0);
    picoquic_cnx_t *srv = p.tls->cnx_server;
    CHECK(srv != NULL);
    if (srv == NULL) { tls_api_delete_ctx(p.tls); return; }
    p.reset_code = 7;
    p.data_action = action;
    static const uint8_t abcdef[] = { 0x0a, 0x02, 0x06, 'a', 'b', 'c', 'd', 'e', 'f' };
    static const uint8_t ghij[] = { 0x0e, 0x02, 0x06, 0x04, 'g', 'h', 'i', 'j' };
    static const uint8_t reset4[] = { 0x24, 0x02, 0x07, 0x0a, 0x04 };
    CHECK(picoquic_decode_reset_stream_at_frame(srv, reset4, reset4 + sizeof(reset4)) != NULL);
    (void)picoquic_decode_stream_frame(srv, abcdef, abcdef + sizeof(abcdef), NULL, p.now);
    /* The prefix was delivered once, whatever the callback then did. */
    CHECK(p.data_action_done);
    CHECK(p.rx_len == 4 && memcmp(p.rx, "abcd", 4) == 0);
    switch (action) {
    case RX_FAIL:
        CHECK(p.resets == 0);
        CHECK(srv->cnx_state >= picoquic_state_disconnecting);
        CHECK(srv->local_error == PICOQUIC_TRANSPORT_INTERNAL_ERROR);
        break;
    case RX_DISCARD:
        CHECK(p.resets == 0);
        break;
    case RX_REPLACE_CTX:
        CHECK(p.resets == 1 && p.reset_stream_ctx == &replacement_ctx);
        break;
    default:
        CHECK(p.resets == 1 && p.rx_at_reset == 4);
        break;
    }
    unsigned resets = p.resets;
    if (srv->cnx_state < picoquic_state_disconnecting) {
        (void)picoquic_decode_stream_frame(srv, ghij, ghij + sizeof(ghij), NULL, p.now);
        (void)picoquic_decode_reset_stream_at_frame(srv, reset4, reset4 + sizeof(reset4));
    }
    CHECK(p.rx_len == 4 && p.resets == resets);
    tls_api_delete_ctx(p.tls);
}

/* A reliable reset after FIN whose reset is acknowledged before its prefix.
 * The FIN already closed the stream, so only the prefix rule keeps it: the
 * stream, with what it needs to repeat the prefix, stays until the last
 * prefix byte (reliable_size - 1) is acknowledged, in either ACK order.
 *   reset_first = 1: reset ACK, then bytes 0..1, then byte 2 (the boundary).
 *   reset_first = 0: bytes 0..2, then the reset ACK. */
static void constructed_reset_ack_before_prefix(int reset_first)
{
    pair_t p;
    row = reset_first ? "constructed-reset-ack-before-prefix"
                      : "constructed-prefix-before-reset-ack";
    CHECK(setup(&p) == 0);
    picoquic_cnx_t *cnx = p.tls->cnx_client;
    picoquic_stream_head_t *st = fin_sent_stream(&p, 1);
    if (st == NULL) { tls_api_delete_ctx(p.tls); return; }
    /* Bytes 3..4 and the FIN are acknowledged; the prefix 0..2 is not. */
    CHECK(picoquic_update_sack_list(&st->sack_list, 3, 5, 0) == 0);
    (void)picoquic_delete_stream_if_closed(cnx, st);
    CHECK(picoquic_find_stream(cnx, p.sid) == st && st->is_closed);
    CHECK(picoquic_reset_stream_at(cnx, p.sid, 12, 3) == 0);
    CHECK(st->reset_sent);
    uint8_t reset_frame[] = { 0x24, (uint8_t)p.sid, 12, 5, 3 };
    uint8_t first_two[] = { 0x0e, (uint8_t)p.sid, 0, 2, 'a', 'b' };
    uint8_t boundary[] = { 0x0e, (uint8_t)p.sid, 2, 1, 'c' };
    uint8_t all_three[] = { 0x0a, (uint8_t)p.sid, 3, 'a', 'b', 'c' };
    size_t consumed = 0;
    if (reset_first) {
        CHECK(picoquic_process_ack_of_reset_stream_at_frame(cnx, reset_frame, sizeof(reset_frame), &consumed) == 0);
        CHECK(picoquic_find_stream(cnx, p.sid) == st && st->reset_acked);
        CHECK(picoquic_process_ack_of_stream_frame(cnx, first_two, sizeof(first_two), &consumed) == 0);
        CHECK(picoquic_find_stream(cnx, p.sid) == st);
        CHECK(picoquic_process_ack_of_stream_frame(cnx, boundary, sizeof(boundary), &consumed) == 0);
        CHECK(picoquic_find_stream(cnx, p.sid) == NULL);
    } else {
        CHECK(picoquic_process_ack_of_stream_frame(cnx, all_three, sizeof(all_three), &consumed) == 0);
        CHECK(picoquic_find_stream(cnx, p.sid) == st);
        CHECK(picoquic_process_ack_of_reset_stream_at_frame(cnx, reset_frame, sizeof(reset_frame), &consumed) == 0);
        CHECK(picoquic_find_stream(cnx, p.sid) == NULL);
    }
    tls_api_delete_ctx(p.tls);
}

/* Control: data and FIN delivered and acknowledged retire the stream. */
static void packet_fin_retires(void)
{
    pair_t p;
    row = "packet-fin-retires";
    CHECK(setup(&p) == 0);
    open_stream(&p, 1);
    CHECK(picoquic_add_to_stream_with_ctx(p.tls->cnx_client, p.sid,
        (const uint8_t *)"abc", 3, 1, &p) == 0);
    pump(&p, 200000);
    CHECK(p.received == 3 && p.fins == 1 && p.resets == 0);
    CHECK(picoquic_find_stream(p.tls->cnx_client, p.sid) == NULL);
    tls_api_delete_ctx(p.tls);
}

/* Bytes and FIN sent but lost, then reset. The reset is accepted and goes
 * on the wire; the stream retires once the reset is acknowledged. */
static void packet_reset_after_fin(void)
{
    pair_t p;
    row = "packet-reset-after-fin";
    CHECK(setup(&p) == 0);
    open_stream(&p, 1);
    p.blocked = UINT64_MAX;
    p.tls->c_to_s_link->loss_mask = &p.blocked;
    CHECK(picoquic_add_to_stream_with_ctx(p.tls->cnx_client, p.sid,
        (const uint8_t *)"abcde", 5, 1, &p) == 0);
    pump(&p, 50000);
    picoquic_stream_head_t *st = picoquic_find_stream(p.tls->cnx_client, p.sid);
    CHECK(st && st->fin_sent && st->sent_offset == 5);
    CHECK(st && !picoquic_check_sack_list(&st->sack_list, 0, 5));
    CHECK(p.received == 0);
    p.reset_code = UINT64_C(0x3000000000000001);
    CHECK(picoquic_reset_stream(p.tls->cnx_client, p.sid, p.reset_code) == 0);
    p.blocked = 0;
    pump(&p, 1000000);
    CHECK(p.saw_reset_sent);
    CHECK(p.resets == 1 && p.fins == 0);
    CHECK(picoquic_find_stream(p.tls->cnx_client, p.sid) == NULL);
    tls_api_delete_ctx(p.tls);
}

/* A reliable reset after FIN while everything after the prefix is lost.
 * The reset goes on the wire at once; the prefix still reaches the peer,
 * which then sees the reset, and the stream retires once the prefix and the
 * reset are acknowledged. prefix_delivered = 1 has the peer receive the
 * prefix before the reset (its acknowledgement lost); prefix_delivered = 0
 * loses the whole stream first, so the prefix arrives only as a repeat,
 * after the reset, and must still be delivered before it. */
static void packet_reliable_reset_after_fin(int prefix_delivered)
{
    pair_t p;
    row = prefix_delivered ? "packet-reliable-reset-after-fin-prefix-delivered"
                           : "packet-reliable-reset-after-fin-all-lost";
    CHECK(setup(&p) == 0);
    open_stream(&p, 1);
    static uint8_t data[8000];
    for (size_t i = 0; i < sizeof(data); i++) data[i] = (uint8_t)i;
    uint64_t c_to_s = 0, s_to_c = UINT64_MAX;
    p.tls->c_to_s_link->loss_mask = &c_to_s;
    p.tls->s_to_c_link->loss_mask = &s_to_c;
    if (prefix_delivered) {
        /* The 1000-byte prefix reaches the peer; its acknowledgement is lost. */
        CHECK(picoquic_add_to_stream_with_ctx(p.tls->cnx_client, p.sid, data, 1000, 0, &p) == 0);
        pump(&p, 20000);
        CHECK(p.received == 1000);
    }
    /* The rest (or all of it) and the FIN are lost. */
    c_to_s = UINT64_MAX;
    size_t from = prefix_delivered ? 1000 : 0;
    CHECK(picoquic_add_to_stream_with_ctx(p.tls->cnx_client, p.sid,
        data + from, sizeof(data) - from, 1, &p) == 0);
    pump(&p, 50000);
    picoquic_stream_head_t *st = picoquic_find_stream(p.tls->cnx_client, p.sid);
    CHECK(st && st->fin_sent && st->sent_offset == sizeof(data));
    CHECK(st && !picoquic_check_sack_list(&st->sack_list, 0, 999));
    p.reset_code = UINT64_C(0x3000000000000002);
    CHECK(picoquic_reset_stream_at(p.tls->cnx_client, p.sid, p.reset_code, 1000) == 0);
    CHECK(p.saw_reset_sent || (st && st->reset_sent));
    s_to_c = 0;
    c_to_s = 0;
    pump(&p, 2000000);
    /* Exactly the prefix reaches the peer, then the reset. */
    CHECK(p.received == 1000);
    CHECK(p.resets == 1 && p.fins == 0);
    CHECK(picoquic_find_stream(p.tls->cnx_client, p.sid) == NULL);
    tls_api_delete_ctx(p.tls);
}

/* A reliable reset whose prefix is the whole stream, with no FIN: the
 * WebTransport preamble-only reset. queued = 1 asks while the preamble is
 * still queued, which is refused without side effects, and asks again once
 * the preamble is sent; queued = 0 asks after it is acknowledged. The peer
 * gets the preamble and the reset, and the stream retires on their ACKs. */
static void packet_reliable_reset_whole_prefix(int queued)
{
    pair_t p;
    row = queued ? "packet-reliable-reset-queued-preamble-no-fin"
                 : "packet-reliable-reset-acked-preamble-no-fin";
    CHECK(setup(&p) == 0);
    open_stream(&p, 1);
    CHECK(picoquic_add_to_stream_with_ctx(p.tls->cnx_client, p.sid,
        (const uint8_t *)"\x40\x54\x00", 3, 0, &p) == 0);
    if (queued) {
        CHECK(picoquic_reset_stream_at(p.tls->cnx_client, p.sid, 0x17, 3) ==
              PICOQUIC_ERROR_OFFSET_TOO_BIG);
        picoquic_stream_head_t *st = picoquic_find_stream(p.tls->cnx_client, p.sid);
        CHECK(st && !st->reset_requested && st->app_stream_ctx == &p);
        /* One round: the preamble goes out, its acknowledgement not yet back. */
        for (unsigned i = 0; i < 100 && st && st->sent_offset < 3; i++) {
            int active = 0;
            CHECK(tls_api_one_sim_round(p.tls, &p.now, p.now + 100000, &active) == 0);
        }
        CHECK(st && st->sent_offset == 3);
    } else {
        pump(&p, 100000);
        picoquic_stream_head_t *st = picoquic_find_stream(p.tls->cnx_client, p.sid);
        CHECK(st && st->sent_offset == 3 && picoquic_check_sack_list(&st->sack_list, 0, 2));
        CHECK(p.received == 3);
    }
    p.reset_code = 0x17;
    CHECK(picoquic_reset_stream_at(p.tls->cnx_client, p.sid, p.reset_code, 3) == 0);
    pump(&p, 1000000);
    CHECK(p.saw_reset_sent);
    CHECK(p.received == 3 && p.resets == 1 && p.fins == 0);
    CHECK(picoquic_find_stream(p.tls->cnx_client, p.sid) == NULL);
    tls_api_delete_ctx(p.tls);
}

/* Reliable-reset prefix boundaries, on constructed stream state with no
 * FIN. prefix = 0: a plain reset cannot be activated again. prefix = 5:
 * with bytes 0..3 acknowledged the stream may still be activated to send the
 * prefix; once byte 4 (the last prefix byte, not byte 5) is acknowledged,
 * activation is refused. Either reset is queued as soon as it is accepted. */
static void constructed_prefix_boundary(uint64_t prefix)
{
    pair_t p;
    row = prefix == 0 ? "constructed-prefix-empty" : "constructed-prefix-last-byte";
    CHECK(setup(&p) == 0);
    picoquic_cnx_t *cnx = p.tls->cnx_client;
    uint64_t sid = picoquic_get_next_local_stream_id(cnx, 1);
    p.sid = sid;
    CHECK(picoquic_mark_active_stream(cnx, sid, 0, NULL) == 0);
    picoquic_stream_head_t *st = picoquic_find_stream(cnx, sid);
    CHECK(st != NULL);
    if (st == NULL) { tls_api_delete_ctx(p.tls); return; }
    st->sent_offset = prefix;
    CHECK(picoquic_reset_stream_at(cnx, sid, 1, prefix) == 0);
    CHECK(st->reset_sent);
    if (prefix > 0) {
        CHECK(picoquic_update_sack_list(&st->sack_list, 0, prefix - 2, 0) == 0);
        CHECK(picoquic_mark_active_stream(cnx, sid, 1, NULL) == 0);
        CHECK(picoquic_mark_active_stream(cnx, sid, 0, NULL) == 0);
        CHECK(picoquic_update_sack_list(&st->sack_list, prefix - 1, prefix - 1, 0) == 0);
        CHECK(picoquic_check_sack_list(&st->sack_list, 0, prefix - 1) != 0);
        CHECK(picoquic_check_sack_list(&st->sack_list, 0, prefix) == 0);
    }
    CHECK(picoquic_mark_active_stream(cnx, sid, 1, NULL) == PICOQUIC_ERROR_CANNOT_SET_ACTIVE_STREAM);
    tls_api_delete_ctx(p.tls);
}

/* A bidirectional stream outlives its acknowledged FIN while the peer's
 * direction is open. Its send side is then in "Data Recvd": the reset is
 * refused, the application context stays, and nothing reaches the peer. */
static void packet_reset_after_fin_acked(void)
{
    pair_t p;
    row = "packet-reset-after-fin-acked-refused";
    CHECK(setup(&p) == 0);
    open_stream(&p, 0);
    CHECK(picoquic_add_to_stream_with_ctx(p.tls->cnx_client, p.sid,
        (const uint8_t *)"abc", 3, 1, &p) == 0);
    pump(&p, 200000);
    CHECK(p.received == 3 && p.fins == 1);
    picoquic_stream_head_t *st = picoquic_find_stream(p.tls->cnx_client, p.sid);
    CHECK(st && st->fin_sent && picoquic_check_sack_list(&st->sack_list, 0, 3));
    p.reset_code = 5;
    CHECK(picoquic_reset_stream(p.tls->cnx_client, p.sid, 5) == PICOQUIC_ERROR_STREAM_ALREADY_CLOSED);
    st = picoquic_find_stream(p.tls->cnx_client, p.sid);
    CHECK(st && !st->reset_requested && st->app_stream_ctx == &p);
    pump(&p, 200000);
    CHECK(p.resets == 0 && !p.saw_reset_sent);
    tls_api_delete_ctx(p.tls);
}

int main(void)
{
    for (unsigned acked = 0; acked < 3; acked++) constructed_reset(acked);
    constructed_empty_ack();
    constructed_fin_ack_retires();
    constructed_partial_ack_keeps_reset();
    constructed_prefix_unacked_keeps_reset();
    constructed_repeat_reset_after_fin_ack();
    constructed_reliable_size_bound(0);
    constructed_reliable_size_bound(1);
    constructed_queued_bytes_not_final();
    constructed_reset_at_receive_bound();
    constructed_reset_at_ack(1);
    constructed_reset_at_ack(0);
    constructed_reliable_prefix_delivery(0);
    constructed_reliable_prefix_delivery(1);
    constructed_reliable_prefix_delivery(2);
    constructed_reliable_prefix_callback_action(RX_FAIL);
    constructed_reliable_prefix_callback_action(RX_STOP);
    constructed_reliable_prefix_callback_action(RX_DISCARD);
    constructed_reliable_prefix_callback_action(RX_REPLACE_CTX);
    constructed_reliable_prefix_callback_action(RX_CLOSE);
    constructed_reset_ack_before_prefix(1);
    constructed_reset_ack_before_prefix(0);
    packet_fin_retires();
    packet_reset_after_fin();
    packet_reliable_reset_after_fin(1);
    packet_reliable_reset_after_fin(0);
    packet_reset_after_fin_acked();
    constructed_prefix_boundary(0);
    constructed_prefix_boundary(5);
    packet_reliable_reset_whole_prefix(1);
    packet_reliable_reset_whole_prefix(0);
    printf("reset-after-fin failures=%d\n", failures);
    return failures != 0;
}
