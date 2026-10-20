/*
 * pico_wt endpoint: a reliable reset requested after the stream's FIN was
 * pushed but before it was handed to picoquic.
 *
 * The endpoint keeps only the part of the MoQ reliable prefix picoquic has not
 * yet been handed, and sends RESET_STREAM_AT once that part is out. Bytes
 * handed over before the FIN was pushed count as provided, so an object still
 * queued behind an already-provided stream header is dropped, not waited for.
 * A subgroup's delivery timeout starts once its publication is known to be
 * complete -- the upstream FIN, or the application's notification -- so the
 * FIN may already be queued when it expires; if the stream has not reached
 * the "all data committed" state by then, "the implementation MUST reset the
 * stream" (draft-ietf-moq-transport-22 §5.2). That reset must not
 * wait behind payload it is meant to abandon (RFC 9000 §3.1: a sender may
 * reset in "Send" or "Data Sent"; the reliable-reset extension delivers only
 * the bytes below Reliable Size).
 *
 * "Handed over" here is the local provide-data callback: no row claims the
 * peer received or acknowledged anything.
 *
 * picoquic and h3zero are stubbed: provide-data runs only when a row calls
 * pico_wt_endpoint_on_provide_data, so "no further credit" is simply not
 * calling it. The reliable size the endpoint passes to picoquic is the
 * WebTransport stream preamble (stream type + session id) plus the MoQ
 * reliable size; the send queue counts MoQ bytes only. Each row that issues
 * RESET_STREAM_AT pins both.
 */

#include "../pico_wt_endpoint.h"
#include <moq/rcbuf.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int failures = 0;
#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

/* -- picoquic / h3zero / picowt stubs ----------------------------------- */

static uint64_t g_next_uni = 2;   /* client-initiated uni: 2, 6, 10 .. */

#if defined(PICOQUIC_STREAM_TERMINAL_CONTRACT_VERSION) && PICOQUIC_STREAM_TERMINAL_CONTRACT_VERSION >= 1
int picoquic_enable_stream_send_commit(picoquic_cnx_t *c, uint64_t sid)
{ (void)c; (void)sid; return 0; }
void h3zero_delete_stream(picoquic_cnx_t *c, h3zero_callback_ctx_t *h3, h3zero_stream_ctx_t *sc)
{ (void)c; (void)h3; sc->path_callback = NULL; sc->path_callback_ctx = NULL; }
#endif
int picoquic_is_client(picoquic_cnx_t *c) { (void)c; return 1; }
int picoquic_set_stream_priority(picoquic_cnx_t *c, uint64_t sid, uint8_t p)
{ (void)c; (void)sid; (void)p; return 0; }
size_t picoquic_frames_varint_encode_length(uint64_t n)
{ return n < 64 ? 1 : n < 16384 ? 2 : n < 1073741824ull ? 4 : 8; }
picoquic_tp_t const *picoquic_get_transport_parameters(picoquic_cnx_t *c, int get_local)
{
    (void)c; (void)get_local;
    static picoquic_tp_t tp;
    memset(&tp, 0, sizeof(tp));
    tp.is_reset_stream_at_enabled = 1;   /* negotiated by both sides */
    return &tp;
}

#define SC_MAX 16
static h3zero_stream_ctx_t g_sc[SC_MAX];
static int g_sc_n = 0;
h3zero_stream_ctx_t *picowt_create_local_stream(picoquic_cnx_t *cnx, int is_bidir,
    h3zero_callback_ctx_t *h3_ctx, uint64_t control_stream_id)
{
    (void)cnx; (void)is_bidir; (void)h3_ctx; (void)control_stream_id;
    if (g_sc_n >= SC_MAX) return NULL;
    h3zero_stream_ctx_t *sc = &g_sc[g_sc_n++];
    memset(sc, 0, sizeof(*sc));
    sc->stream_id = g_next_uni;
    g_next_uni += 4;
    return sc;
}
h3zero_stream_ctx_t *h3zero_find_stream(h3zero_callback_ctx_t *ctx, uint64_t stream_id)
{
    (void)ctx;
    for (int i = 0; i < g_sc_n; i++)
        if (g_sc[i].stream_id == stream_id) return &g_sc[i];
    return NULL;
}
static h3zero_stream_prefix_t g_prefix;
h3zero_stream_prefix_t *h3zero_find_stream_prefix(h3zero_callback_ctx_t *ctx, uint64_t prefix)
{ (void)ctx; (void)prefix; return &g_prefix; }
static uint8_t g_dg_buf[2048];
uint8_t *h3zero_provide_datagram_buffer(void *context, size_t length, int ready_to_send)
{ (void)context; (void)ready_to_send; return length <= sizeof(g_dg_buf) ? g_dg_buf : NULL; }
int picoquic_mark_datagram_ready(picoquic_cnx_t *cnx, int is_ready)
{ (void)cnx; (void)is_ready; return 0; }
int picoquic_mark_active_stream(picoquic_cnx_t *c, uint64_t sid, int active, void *v)
{ (void)c; (void)sid; (void)active; (void)v; return 0; }

static uint8_t g_provide_buf[4096];
static size_t g_provide_nb;
static int g_provide_fin, g_provide_still;
uint8_t *picoquic_provide_stream_data_buffer(void *ctx, size_t nb, int is_fin,
                                             int is_still_active)
{
    (void)ctx;
    g_provide_nb = nb; g_provide_fin = is_fin; g_provide_still = is_still_active;
    return nb <= sizeof(g_provide_buf) ? g_provide_buf : NULL;
}

/* g_rsa_fail / g_reset_fail make picoquic refuse RESET_STREAM_AT / a plain
 * RESET_STREAM on stream g_fail_sid; a refused call is not counted. */
static bool g_rsa_fail = false, g_reset_fail = false;
static uint64_t g_fail_sid = 0;
static int g_plain_resets = 0;
int picoquic_reset_stream(picoquic_cnx_t *c, uint64_t sid, uint64_t ec)
{
    (void)c; (void)ec;
    if (g_reset_fail && sid == g_fail_sid) return -1;
    g_plain_resets++;
    return 0;
}
static int g_rsa_count = 0;
static uint64_t g_rsa_sid = 0, g_rsa_size = 0;
int picoquic_reset_stream_at(picoquic_cnx_t *c, uint64_t sid, uint64_t ec,
                             uint64_t reliable_size)
{
    (void)c; (void)ec;
    if (g_rsa_fail && sid == g_fail_sid) return -1;
    g_rsa_count++; g_rsa_sid = sid; g_rsa_size = reliable_size;
    return 0;
}
int picowt_reset_stream(picoquic_cnx_t *cnx, h3zero_stream_ctx_t *stream_ctx,
                        uint64_t local_stream_error)
{ (void)cnx; (void)stream_ctx; (void)local_stream_error; g_plain_resets++; return 0; }
/* The endpoint puts h3zero's stream record back as the QUIC stream context
 * after a STOP or reset; nothing reads it here. */
int picoquic_set_app_stream_ctx(picoquic_cnx_t *c, uint64_t sid, void *ctx)
{ (void)c; (void)sid; (void)ctx; return 0; }
int picoquic_stop_sending(picoquic_cnx_t *c, uint64_t sid, uint64_t ec)
{ (void)c; (void)sid; (void)ec; return 0; }
int picowt_send_close_session_message(picoquic_cnx_t *cnx,
    h3zero_stream_ctx_t *control_stream_ctx, uint32_t picowt_err, const char *err_msg)
{ (void)cnx; (void)control_stream_ctx; (void)picowt_err; (void)err_msg; return 0; }

/* -- fixture ---------------------------------------------------------------- */

static void *ta(size_t n, void *c) { (void)c; return malloc(n); }
static void *tr(void *p, size_t o, size_t n, void *c) { (void)o; (void)c; return realloc(p, n); }
static void tf(void *p, size_t n, void *c) { (void)n; (void)c; free(p); }

static picoquic_cnx_t *const CNX = (picoquic_cnx_t *)(uintptr_t)0xC0FFEE;
static h3zero_callback_ctx_t g_h3;
static h3zero_stream_ctx_t g_ctrl;   /* the WT CONNECT stream, session id 0 */

/* MoQ subgroup header and one object. */
static const uint8_t HDR[5] = { 0x10, 0x01, 0x02, 0x03, 0x04 };
static const uint8_t OBJ[7] = { 0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26 };

typedef struct {
    moq_transport_endpoint_ops_t ops;
    pico_wt_endpoint_ctx_t ep;
    uint64_t preamble;
} wt_rig_t;

/* Open the endpoint on a WT session whose CONNECT stream (the session id) is
 * `session`. The preamble each data stream carries is the stream type (0x54,
 * a two-byte varint) and the session id as a varint: three bytes for session
 * 0, four for session 64. The expected values are written out here rather
 * than computed with the endpoint's own helper. */
static void rig_open_session(wt_rig_t *r, uint64_t session, uint64_t preamble)
{
    moq_alloc_t al = { .alloc = ta, .realloc = tr, .free = tf, .ctx = NULL };
    memset(r, 0, sizeof(*r));
    memset(&g_ctrl, 0, sizeof(g_ctrl));
    g_ctrl.stream_id = session;
    CHECK(pico_wt_endpoint_init(&r->ops, &r->ep, CNX, &g_h3, &g_ctrl, &al) == 0);
    CHECK(r->ops.reset_stream_at != NULL);
    r->preamble = preamble;
    g_rsa_count = 0;
    g_plain_resets = 0;
}

static void rig_open(wt_rig_t *r) { rig_open_session(r, 0, 3); }

static uint64_t rig_stream(wt_rig_t *r)
{
    uint64_t sid = 0;
    CHECK(r->ops.open_uni(&r->ep, &sid) == MOQ_TRANSPORT_OK);
    return sid;
}

static void provide(wt_rig_t *r, uint64_t sid, size_t space)
{
    g_provide_nb = 0; g_provide_fin = g_provide_still = 0;
    (void)pico_wt_endpoint_on_provide_data(&r->ep, sid, NULL, space);
}

/* -- rows ------------------------------------------------------------------- */

/* The whole header was handed over; an object and then a separate zero-byte
 * FIN were pushed behind it, and no further credit arrives. The reset goes
 * out at once with exactly preamble + header as its reliable size, and the
 * object and FIN are dropped. */
static void row_prefix_provided_bare_fin(void)
{
    wt_rig_t r; rig_open(&r);
    uint64_t sid = rig_stream(&r);
    CHECK(r.ops.write(&r.ep, sid, HDR, sizeof(HDR), false) == MOQ_TRANSPORT_OK);
    provide(&r, sid, 64);
    CHECK(g_provide_nb == sizeof(HDR));
    CHECK(r.ops.write(&r.ep, sid, OBJ, sizeof(OBJ), false) == MOQ_TRANSPORT_OK);
    CHECK(r.ops.write(&r.ep, sid, NULL, 0, true) == MOQ_TRANSPORT_OK);
    CHECK(r.ops.reset_stream_at(&r.ep, sid, 0x2, sizeof(HDR)) == MOQ_TRANSPORT_OK);
    CHECK(g_rsa_count == 1 && g_rsa_sid == sid);
    CHECK(g_rsa_size == r.preamble + sizeof(HDR));
    CHECK(r.ep.last_reset_reliable_size == r.preamble + sizeof(HDR));
    CHECK(r.ep.deferred_at_count == 0);
    CHECK(!moq_pq_send_queue_has_data(r.ep.queue, sid));
    CHECK(moq_pq_send_queue_queued_bytes(r.ep.queue) == 0);
    pico_wt_endpoint_flush_resets(&r.ep);
    CHECK(g_rsa_count == 1);           /* issued once, not again on flush */
    CHECK(g_plain_resets == 0);
    pico_wt_endpoint_cleanup(&r.ep);
}

/* As above, with the FIN carried by the object's own write. */
static void row_prefix_provided_fin_with_bytes(void)
{
    wt_rig_t r; rig_open(&r);
    uint64_t sid = rig_stream(&r);
    CHECK(r.ops.write(&r.ep, sid, HDR, sizeof(HDR), false) == MOQ_TRANSPORT_OK);
    provide(&r, sid, 64);
    CHECK(g_provide_nb == sizeof(HDR));
    CHECK(r.ops.write(&r.ep, sid, OBJ, sizeof(OBJ), true) == MOQ_TRANSPORT_OK);
    CHECK(r.ops.reset_stream_at(&r.ep, sid, 0x2, sizeof(HDR)) == MOQ_TRANSPORT_OK);
    CHECK(g_rsa_count == 1 && g_rsa_size == r.preamble + sizeof(HDR));
    CHECK(r.ep.deferred_at_count == 0);
    CHECK(!moq_pq_send_queue_has_data(r.ep.queue, sid));
    pico_wt_endpoint_cleanup(&r.ep);
}

/* Only three header bytes were handed over before the FIN was pushed. The
 * remaining two header bytes are still owed: they alone stay queued (no
 * object byte, no FIN), the reset waits for them, and once they are handed
 * over it goes out with preamble + header as its reliable size. Waiting here
 * is correct: those bytes are inside the reliable prefix. */
static void row_prefix_partial(void)
{
    wt_rig_t r; rig_open(&r);
    uint64_t sid = rig_stream(&r);
    CHECK(r.ops.write(&r.ep, sid, HDR, sizeof(HDR), false) == MOQ_TRANSPORT_OK);
    provide(&r, sid, 3);
    CHECK(g_provide_nb == 3);
    CHECK(r.ops.write(&r.ep, sid, OBJ, sizeof(OBJ), true) == MOQ_TRANSPORT_OK);
    CHECK(r.ops.reset_stream_at(&r.ep, sid, 0x2, sizeof(HDR)) == MOQ_TRANSPORT_OK);
    CHECK(g_rsa_count == 0);
    CHECK(r.ep.deferred_at_count == 1);
    CHECK(moq_pq_send_queue_queued_bytes(r.ep.queue) == 2);
    pico_wt_endpoint_flush_resets(&r.ep);
    CHECK(g_rsa_count == 0);           /* the owed bytes are not out yet */
    provide(&r, sid, 64);
    CHECK(g_provide_nb == 2 && g_provide_fin == 0 && g_provide_still == 0);
    CHECK(memcmp(g_provide_buf, HDR + 3, 2) == 0);
    pico_wt_endpoint_flush_resets(&r.ep);
    CHECK(g_rsa_count == 1 && g_rsa_sid == sid);
    CHECK(g_rsa_size == r.preamble + sizeof(HDR));
    CHECK(r.ep.deferred_at_count == 0);
    pico_wt_endpoint_cleanup(&r.ep);
}

/* A sibling stream with its FIN still pending is not disturbed by another
 * stream's reset, and its own count survives a moment of empty queue. */
static void row_sibling_and_empty_queue(void)
{
    wt_rig_t r; rig_open(&r);
    uint64_t a = rig_stream(&r), b = rig_stream(&r);
    CHECK(r.ops.write(&r.ep, a, HDR, sizeof(HDR), false) == MOQ_TRANSPORT_OK);
    CHECK(r.ops.write(&r.ep, b, HDR, sizeof(HDR), false) == MOQ_TRANSPORT_OK);
    provide(&r, a, 64);
    provide(&r, b, 64);
    /* b's queue is empty now; more bytes and a FIN follow. */
    CHECK(r.ops.write(&r.ep, b, OBJ, sizeof(OBJ), true) == MOQ_TRANSPORT_OK);
    CHECK(r.ops.write(&r.ep, a, OBJ, sizeof(OBJ), true) == MOQ_TRANSPORT_OK);
    CHECK(r.ops.reset_stream_at(&r.ep, a, 0x2, sizeof(HDR)) == MOQ_TRANSPORT_OK);
    CHECK(g_rsa_count == 1 && g_rsa_sid == a);
    CHECK(moq_pq_send_queue_has_data(r.ep.queue, b));
    CHECK(r.ops.reset_stream_at(&r.ep, b, 0x2, sizeof(HDR)) == MOQ_TRANSPORT_OK);
    CHECK(g_rsa_count == 2 && g_rsa_sid == b);
    CHECK(g_rsa_size == r.preamble + sizeof(HDR));
    CHECK(moq_pq_send_queue_queued_bytes(r.ep.queue) == 0);
    pico_wt_endpoint_cleanup(&r.ep);
}

/* Session 64 needs a two-byte session-id varint: the preamble is four bytes,
 * and the reliable size handed to picoquic is preamble + header with the
 * owed bytes still counted in MoQ bytes only. */
static void row_two_byte_session_id(void)
{
    wt_rig_t r; rig_open_session(&r, 64, 4);
    uint64_t sid = rig_stream(&r);
    CHECK(r.ops.write(&r.ep, sid, HDR, sizeof(HDR), false) == MOQ_TRANSPORT_OK);
    provide(&r, sid, 3);
    CHECK(g_provide_nb == 3);
    CHECK(r.ops.write(&r.ep, sid, OBJ, sizeof(OBJ), true) == MOQ_TRANSPORT_OK);
    CHECK(r.ops.reset_stream_at(&r.ep, sid, 0x2, sizeof(HDR)) == MOQ_TRANSPORT_OK);
    CHECK(g_rsa_count == 0);
    CHECK(moq_pq_send_queue_queued_bytes(r.ep.queue) == 2);
    provide(&r, sid, 64);
    CHECK(g_provide_nb == 2 && memcmp(g_provide_buf, HDR + 3, 2) == 0);
    pico_wt_endpoint_flush_resets(&r.ep);
    CHECK(g_rsa_count == 1 && g_rsa_sid == sid);
    CHECK(g_rsa_size == 4 + sizeof(HDR));
    CHECK(r.ep.last_reset_reliable_size == 4 + sizeof(HDR));
    pico_wt_endpoint_cleanup(&r.ep);
}

/* Deferred reliable resets on two streams, both issued by one flush once
 * their headers are handed over; picoquic refuses the frame on the first.
 * If the plain RESET_STREAM fallback is refused too, that stream cannot be
 * ended at all: the flush reports the failure (with the existing plain-reset
 * failures) so the caller fails the transport, as it does for a reset refused
 * on the spot. The failure does not stop the flush: the second stream's
 * reset still goes out. Every attempted entry is consumed, never retried.
 * Control: a fallback that succeeds ends the first stream plainly and the
 * flush succeeds. */
static void row_deferred_refused(bool fallback_ok)
{
    wt_rig_t r; rig_open(&r);
    uint64_t a = rig_stream(&r), b = rig_stream(&r);
    CHECK(r.ops.write(&r.ep, a, HDR, sizeof(HDR), false) == MOQ_TRANSPORT_OK);
    CHECK(r.ops.write(&r.ep, a, OBJ, sizeof(OBJ), true) == MOQ_TRANSPORT_OK);
    CHECK(r.ops.write(&r.ep, b, HDR, sizeof(HDR), false) == MOQ_TRANSPORT_OK);
    CHECK(r.ops.reset_stream_at(&r.ep, a, 0x2, sizeof(HDR)) == MOQ_TRANSPORT_OK);
    CHECK(r.ops.reset_stream_at(&r.ep, b, 0x2, sizeof(HDR)) == MOQ_TRANSPORT_OK);
    CHECK(g_rsa_count == 0 && g_plain_resets == 0);
    CHECK(r.ep.deferred_at_count == 2);
    CHECK(pico_wt_endpoint_flush_resets(&r.ep) == 0);   /* headers still owed */
    CHECK(r.ep.deferred_at_count == 2);
    provide(&r, a, 64);
    CHECK(g_provide_nb == sizeof(HDR) && g_provide_fin == 0);
    provide(&r, b, 64);
    CHECK(g_provide_nb == sizeof(HDR) && g_provide_fin == 0);

    g_fail_sid = a;
    g_rsa_fail = true;
    g_reset_fail = !fallback_ok;
    int rc = pico_wt_endpoint_flush_resets(&r.ep);
    CHECK(g_rsa_count == 1 && g_rsa_sid == b);          /* b still went out */
    CHECK(g_rsa_size == r.preamble + sizeof(HDR));
    CHECK(r.ep.deferred_at_count == 0);
    if (fallback_ok) {
        CHECK(rc == 0);
        CHECK(g_plain_resets == 1);
    } else {
        CHECK(rc != 0);
        CHECK(g_plain_resets == 0);
    }
    g_rsa_fail = g_reset_fail = false;
    CHECK(pico_wt_endpoint_flush_resets(&r.ep) == 0);   /* nothing retried */
    CHECK(g_rsa_count == 1 && g_plain_resets == (fallback_ok ? 1 : 0));
    pico_wt_endpoint_cleanup(&r.ep);
}

int main(void)
{
    row_prefix_provided_bare_fin();
    row_prefix_provided_fin_with_bytes();
    row_prefix_partial();
    row_sibling_and_empty_queue();
    row_two_byte_session_id();
    row_deferred_refused(true);
    row_deferred_refused(false);
    if (failures) {
        fprintf(stderr, "test_pico_wt_reset_after_fin: %d failure(s)\n", failures);
        return 1;
    }
    printf("test_pico_wt_reset_after_fin: PASS (7 rows)\n");
    return 0;
}
