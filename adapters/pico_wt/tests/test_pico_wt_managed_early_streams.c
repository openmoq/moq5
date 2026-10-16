/*
 * The managed client's buffer for peer WebTransport stream events delivered
 * before the CONNECT response (draft-ietf-webtrans-http3 §4.6), driven through
 * the facade's own client control-stream callback and replay.
 *
 * Pinned here:
 *   - each delivery is kept -- bytes, a FIN, or the peer's STOP_SENDING -- in
 *     arrival order, and replayed in that order onto the stream rebound to
 *     the adapter;
 *   - a stream beyond MOQ_PWM_EARLY_MAX_STREAMS, or one whose entry would pass
 *     MOQ_PWM_EARLY_MAX_ENTRIES or MOQ_PWM_EARLY_MAX_BYTES (payload plus node
 *     storage), is closed once with WT_BUFFERED_STREAM_REJECTED and detached
 *     from the facade; other streams keep their entries;
 *   - a peer reset drops the stream's entries; h3zero freeing a stream's
 *     context (its own deletion route) forgets it, so replay has nothing to
 *     rebind or deliver for it;
 *   - a refused CONNECT closes every buffered stream once with
 *     WT_SESSION_GONE and closes the buffer: a stream arriving afterwards is
 *     closed once and allocates nothing, and a repeated refusal closes
 *     nothing more;
 *   - a stream whose FIN arrived early is retired by the transport (picoquic
 *     deletes a remote stream once all its data and FIN are in), and its
 *     replay into a live adapter after acceptance is not a transport failure.
 *   - a stream whose FIN is already buffered is complete from its first
 *     replayed entry, so its window is never taken; a stream whose FIN has
 *     not arrived gains no completion and its window is taken; the transient
 *     completion marker is clear after a successful, a fatal and an
 *     already-terminal delivery.
 *
 * The facade source is compiled into this test with MOQ_PICO_WT_TESTING: the
 * stream-close seam records closes, and the replay seam records deliveries
 * where a test has no live adapter. The early-FIN and completion-marker rows
 * use the real adapter over the real picoquic transport in the deterministic
 * simulation instead.
 */
#include "../pico_wt_managed.c"
#include "pico_wt_harness.h"
#include <moq/buf.h>
#include <moq/control_d18.h>

#include <stdio.h>

static int failures;
#define CHECK(e) do { if (!(e)) { \
    fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #e); \
    failures++; } } while (0)

/* -- counting allocator ---------------------------------------------- */

static long g_live;
static void *cnt_alloc(size_t n, void *ctx)
{ (void)ctx; g_live++; return malloc(n); }
static void *cnt_realloc(void *p, size_t o, size_t n, void *ctx)
{ (void)o; (void)ctx; if (!p) g_live++; return realloc(p, n); }
static void cnt_free(void *p, size_t n, void *ctx)
{ (void)n; (void)ctx; if (p) g_live--; free(p); }

/* -- recorded stream closes and replayed deliveries ------------------ */

typedef struct { uint64_t sid, code; } abort_rec_t;
static abort_rec_t g_aborts[32];
static size_t g_naborts;

static void rec_abort(picoquic_cnx_t *cnx, uint64_t sid, uint64_t code)
{
    (void)cnx;
    if (g_naborts < 32)
        g_aborts[g_naborts].sid = sid, g_aborts[g_naborts].code = code;
    g_naborts++;
}

static size_t aborts_of(uint64_t sid, uint64_t code)
{
    size_t n = 0;
    for (size_t i = 0; i < g_naborts && i < 32; i++)
        if (g_aborts[i].sid == sid && g_aborts[i].code == code) n++;
    return n;
}

typedef struct { uint64_t sid; int event; size_t len; uint8_t first; } deliv_t;
static deliv_t g_deliv[32];
static size_t g_ndeliv;

static int rec_replay(picoquic_cnx_t *cnx, uint8_t *bytes, size_t len, int event,
                      struct st_h3zero_stream_ctx_t *sc, void *conn)
{
    (void)cnx; (void)conn;
    if (g_ndeliv < 32) {
        g_deliv[g_ndeliv].sid = sc->stream_id;
        g_deliv[g_ndeliv].event = event;
        g_deliv[g_ndeliv].len = len;
        g_deliv[g_ndeliv].first = (len && bytes) ? bytes[0] : 0;
    }
    g_ndeliv++;
    return 0;
}

/* -- a client facade shell: what the control-stream callback uses ---- */

static h3zero_stream_ctx_t g_ctrl;
static h3zero_stream_ctx_t g_streams[16];
static picoquic_cnx_t *const g_cnx = (picoquic_cnx_t *)(uintptr_t)0x1000;

static moq_pico_wt_managed_t *shell(void)
{
    moq_pico_wt_managed_t *m = calloc(1, sizeof(*m));
    m->alloc = (moq_alloc_t){ .ctx = NULL, .alloc = cnt_alloc,
                              .realloc = cnt_realloc, .free = cnt_free };
    m->perspective = MOQ_PERSPECTIVE_CLIENT;
    m->ctrl_ctx = &g_ctrl;
    pthread_mutex_init(&m->mutex, NULL);
    pthread_cond_init(&m->cond, NULL);
    g_naborts = 0;
    g_ndeliv = 0;
    return m;
}

static void shell_free(moq_pico_wt_managed_t *m)
{
    pwm_early_clear(m);
    pthread_cond_destroy(&m->cond);
    pthread_mutex_destroy(&m->mutex);
    free(m);
}

/* A peer stream bound to the facade, as h3zero binds an arriving WT stream
 * to the session prefix's function. n < 8: server unidirectional (3, 7, ...);
 * n >= 8: server bidirectional (1, 5, ...). */
static h3zero_stream_ctx_t *stream(moq_pico_wt_managed_t *m, size_t n)
{
    h3zero_stream_ctx_t *sc = &g_streams[n];
    memset(sc, 0, sizeof(*sc));
    sc->stream_id = n < 8 ? 4u * n + 3u : 4u * (n - 8) + 1u;
    sc->path_callback = managed_client_wt_cb;
    sc->path_callback_ctx = m;
    return sc;
}

static bool bound_to_facade(const h3zero_stream_ctx_t *sc, const void *m)
{
    return sc->path_callback == managed_client_wt_cb && sc->path_callback_ctx == m;
}

static int deliver(moq_pico_wt_managed_t *m, h3zero_stream_ctx_t *sc,
                   const void *bytes, size_t len, picohttp_call_back_event_t ev)
{
    return managed_client_wt_cb(g_cnx, (uint8_t *)(uintptr_t)bytes, len, ev,
                                sc, m);
}
#define DATA picohttp_callback_post_data
#define FIN  picohttp_callback_post_fin
#define STOP picohttp_callback_stop_sending

static size_t entries(const moq_pico_wt_managed_t *m)
{
    size_t n = 0;
    for (const pwm_early_chunk_t *c = m->early_head; c; c = c->next) n++;
    return n;
}

/* Arrival order with FIN and STOP, replayed in that order onto the rebound
 * streams; an early datagram is not kept. */
static void test_order_and_replay(void)
{
    moq_pico_wt_managed_t *m = shell();
    h3zero_stream_ctx_t *u = stream(m, 0), *b = stream(m, 8);
    CHECK(deliver(m, b, "a", 1, DATA) == 0);
    CHECK(deliver(m, u, "xy", 2, DATA) == 0);
    CHECK(deliver(m, b, NULL, 0, STOP) == 0);       /* peer stops our half */
    CHECK(deliver(m, b, "b", 1, DATA) == 0);        /* its half still sends */
    CHECK(deliver(m, b, NULL, 0, FIN) == 0);
    CHECK(managed_client_wt_cb(g_cnx, (uint8_t *)"dg", 2,
                               picohttp_callback_post_datagram, &g_ctrl, m) == 0);
    CHECK(entries(m) == 5 && m->early_entries == 5);
    CHECK(m->early_bytes == 5 * sizeof(pwm_early_chunk_t) + 4);
    CHECK(g_naborts == 0);

    moq_pico_wt_managed_test_replay = rec_replay;
    void *conn = (void *)(uintptr_t)0x2000;
    pwm_early_replay(m, g_cnx, (moq_pico_wt_conn_t *)conn);
    moq_pico_wt_managed_test_replay = NULL;

    CHECK(u->path_callback == moq_pico_wt_callback && u->path_callback_ctx == conn);
    CHECK(b->path_callback == moq_pico_wt_callback && b->path_callback_ctx == conn);
    static const struct { size_t n; int ev; size_t len; uint8_t first; } want[] = {
        { 8, picohttp_callback_post_data, 1, 'a' },
        { 0, picohttp_callback_post_data, 2, 'x' },
        { 8, picohttp_callback_stop_sending, 0, 0 },
        { 8, picohttp_callback_post_data, 1, 'b' },
        { 8, picohttp_callback_post_fin, 0, 0 },
    };
    CHECK(g_ndeliv == 5);
    for (size_t i = 0; i < 5 && i < g_ndeliv; i++) {
        CHECK(g_deliv[i].sid == g_streams[want[i].n].stream_id);
        CHECK(g_deliv[i].event == want[i].ev);
        CHECK(g_deliv[i].len == want[i].len);
        CHECK(g_deliv[i].first == want[i].first);
    }
    CHECK(m->early_head == NULL && m->early_entries == 0 &&
          m->early_bytes == 0 && m->early_ended);
    CHECK(g_live == 0);
    shell_free(m);
}

/* The stream limit: the next stream is closed once with
 * WT_BUFFERED_STREAM_REJECTED and detached, so h3zero delivers it nothing
 * more; the kept streams are untouched. */
static void test_stream_limit(void)
{
    moq_pico_wt_managed_t *m = shell();
    for (size_t i = 0; i < MOQ_PWM_EARLY_MAX_STREAMS; i++)
        CHECK(deliver(m, stream(m, i), "s", 1, DATA) == 0);
    CHECK(g_naborts == 0);
    h3zero_stream_ctx_t *extra = stream(m, 8);
    CHECK(deliver(m, extra, "over", 4, DATA) == 0);
    CHECK(aborts_of(extra->stream_id,
                    H3ZERO_WEBTRANSPORT_BUFFERED_STREAM_REJECTED) == 1);
    CHECK(g_naborts == 1);
    CHECK(extra->path_callback == NULL && extra->path_callback_ctx == NULL);
    CHECK(m->early_nstreams == MOQ_PWM_EARLY_MAX_STREAMS);
    CHECK(entries(m) == MOQ_PWM_EARLY_MAX_STREAMS);
    for (size_t i = 0; i < MOQ_PWM_EARLY_MAX_STREAMS; i++)
        CHECK(bound_to_facade(&g_streams[i], m));
    shell_free(m);
    CHECK(g_live == 0);
}

/* The entry and storage limits under fragmentation: every entry is charged its
 * payload plus its node. The stream whose entry would pass a limit is closed
 * once and detached, its entries dropped; the other streams keep theirs, in
 * order. */
static void test_entry_and_storage_limits(void)
{
    moq_pico_wt_managed_t *m = shell();
    h3zero_stream_ctx_t *keep = stream(m, 0), *frag = stream(m, 1);
    CHECK(deliver(m, keep, "k", 1, DATA) == 0);
    for (size_t i = 1; i < MOQ_PWM_EARLY_MAX_ENTRIES; i++)
        CHECK(deliver(m, frag, "f", 1, DATA) == 0);
    CHECK(m->early_entries == MOQ_PWM_EARLY_MAX_ENTRIES);
    CHECK(m->early_bytes ==
          MOQ_PWM_EARLY_MAX_ENTRIES * (sizeof(pwm_early_chunk_t) + 1));
    CHECK(g_naborts == 0);
    CHECK(deliver(m, frag, "f", 1, DATA) == 0);            /* one entry over */
    CHECK(aborts_of(frag->stream_id,
                    H3ZERO_WEBTRANSPORT_BUFFERED_STREAM_REJECTED) == 1);
    CHECK(frag->path_callback == NULL);
    CHECK(entries(m) == 1 && m->early_head->sid == keep->stream_id);
    CHECK(m->early_bytes == sizeof(pwm_early_chunk_t) + 1);
    CHECK(g_live == 1);

    /* storage: the largest entry that fits is kept; one byte more is not */
    static uint8_t big[MOQ_PWM_EARLY_MAX_BYTES];
    size_t room = MOQ_PWM_EARLY_MAX_BYTES - m->early_bytes -
                  sizeof(pwm_early_chunk_t);
    h3zero_stream_ctx_t *s2 = stream(m, 2), *s3 = stream(m, 3);
    CHECK(deliver(m, s2, big, room + 1, DATA) == 0);
    CHECK(aborts_of(s2->stream_id,
                    H3ZERO_WEBTRANSPORT_BUFFERED_STREAM_REJECTED) == 1);
    CHECK(deliver(m, s3, big, room, DATA) == 0);
    CHECK(m->early_bytes == MOQ_PWM_EARLY_MAX_BYTES);
    CHECK(bound_to_facade(s3, m) && bound_to_facade(keep, m));
    CHECK(g_naborts == 2);
    shell_free(m);
    CHECK(g_live == 0);
}

/* A peer reset drops the stream's entries and detaches it; h3zero freeing a
 * stream's context (its node-deletion route) forgets it, so replay delivers
 * nothing for it and rebinds nothing. */
static void test_reset_and_freed_context(void)
{
    moq_pico_wt_managed_t *m = shell();
    h3zero_stream_ctx_t *r = stream(m, 0), *keep = stream(m, 1);
    CHECK(deliver(m, r, "gone", 4, DATA) == 0);
    CHECK(deliver(m, keep, "stay", 4, DATA) == 0);
    CHECK(managed_client_wt_cb(g_cnx, NULL, 0, picohttp_callback_reset, r, m) == 0);
    CHECK(entries(m) == 1 && m->early_head->sid == keep->stream_id);
    CHECK(r->path_callback == NULL);
    CHECK(g_naborts == 0);

    /* A real h3zero stream context, deleted through h3zero's own route: the
     * deletion delivers picohttp_callback_free to the bound callback. */
    h3zero_callback_ctx_t *h3 = h3zero_callback_create_context(NULL);
    h3zero_stream_ctx_t *sc = h3zero_find_or_create_stream(NULL, 9, h3, 1, 0);
    CHECK(h3 != NULL && sc != NULL);
    sc->path_callback = managed_client_wt_cb;
    sc->path_callback_ctx = m;
    CHECK(deliver(m, sc, "lost", 4, DATA) == 0);
    CHECK(m->early_nstreams == 2);
    h3zero_delete_stream(NULL, h3, sc);
    CHECK(m->early_nstreams == 1);
    CHECK(entries(m) == 1 && m->early_head->sid == keep->stream_id);

    moq_pico_wt_managed_test_replay = rec_replay;
    pwm_early_replay(m, g_cnx, (moq_pico_wt_conn_t *)(uintptr_t)0x2000);
    moq_pico_wt_managed_test_replay = NULL;
    CHECK(g_ndeliv == 1 && g_deliv[0].sid == keep->stream_id);
    CHECK(g_live == 0);
    h3zero_callback_delete_context(NULL, h3);
    shell_free(m);
}

/* A refused CONNECT: every buffered stream is closed once with WT_SESSION_GONE
 * and detached, every allocation returned, the facade fatal. The buffer stays
 * closed: a stream arriving afterwards (still bound to the session prefix) is
 * closed once and allocates nothing; a repeated refusal closes nothing more. */
static void test_connect_refused(void)
{
    moq_pico_wt_managed_t *m = shell();
    h3zero_stream_ctx_t *a = stream(m, 0), *b = stream(m, 8);
    CHECK(deliver(m, a, "one", 3, DATA) == 0);
    CHECK(deliver(m, b, "two", 3, FIN) == 0);
    CHECK(g_live == 2);

    CHECK(managed_client_wt_cb(g_cnx, NULL, 0, picohttp_callback_connect_refused,
                               &g_ctrl, m) == 0);
    CHECK(aborts_of(a->stream_id, H3ZERO_WEBTRANSPORT_SESSION_GONE) == 1);
    CHECK(aborts_of(b->stream_id, H3ZERO_WEBTRANSPORT_SESSION_GONE) == 1);
    CHECK(g_naborts == 2);
    CHECK(a->path_callback == NULL && b->path_callback == NULL);
    CHECK(m->early_head == NULL && m->early_nstreams == 0 && m->early_ended);
    CHECK(g_live == 0);
    CHECK(m->fatal);

    h3zero_stream_ctx_t *late = stream(m, 1);
    CHECK(deliver(m, late, "late", 4, DATA) == 0);
    CHECK(aborts_of(late->stream_id, H3ZERO_WEBTRANSPORT_SESSION_GONE) == 1);
    CHECK(late->path_callback == NULL);
    CHECK(g_live == 0 && m->early_head == NULL);

    CHECK(managed_client_wt_cb(g_cnx, NULL, 0, picohttp_callback_connect_refused,
                               &g_ctrl, m) == 0);
    CHECK(g_naborts == 3);
    CHECK(g_live == 0);
    shell_free(m);
}

/* -- early FIN through the real transport ---------------------------- */

/* The adapter's allocator in the early-FIN rows: libc, except that the next
 * allocation can be made to fail once. */
static int g_fail_next;
static void *once_alloc(size_t n, void *ctx)
{ (void)ctx; if (g_fail_next) { g_fail_next = 0; return NULL; } return malloc(n); }
static void *once_realloc(void *p, size_t o, size_t n, void *ctx)
{ (void)o; (void)ctx; if (g_fail_next) { g_fail_next = 0; return NULL; } return realloc(p, n); }
static void once_free(void *p, size_t n, void *ctx)
{ (void)n; (void)ctx; free(p); }
static const moq_alloc_t g_once_alloc = { .ctx = NULL, .alloc = once_alloc,
                                          .realloc = once_realloc,
                                          .free = once_free };

static size_t active_rx(const moq_pico_wt_conn_t *c)
{
    size_t n = 0;
    for (size_t i = 0; i < c->rx_count; i++) if (c->rx[i].active) n++;
    return n;
}

/* The server opens a WebTransport uni stream and writes a whole stream to it
 * -- bytes and FIN -- while the client's session prefix still names the
 * facade. picoquic retires the client's stream once all of it is in. Then the
 * session is accepted exactly as the facade accepts it (adapter created,
 * session started, buffer replayed): the replayed stream must not turn into a
 * transport failure for the retired stream. */
static void test_early_fin_real_transport(bool fail_mark)
{
    pico_wt_harness_t h;
    pico_wt_harness_cfg_t cfg = { .cid_byte = 0x9e, .request_capacity = 10,
                                  .version = MOQ_VERSION_DRAFT_18 };
    if (pico_wt_harness_setup(&h, &cfg) != 0) {
        CHECK(0 && "harness setup");
        pico_wt_harness_cleanup(&h);
        return;
    }
    /* Detach the harness's client adapter: the client is now in the state a
     * managed client is in before the CONNECT response -- its session prefix
     * names the facade. */
    moq_pico_wt_conn_destroy(h.client_conn);
    h.client_conn = NULL;
    moq_pico_wt_managed_t *m = shell();
    m->h3_ctx = h.client_h3_ctx;
    m->ctrl_ctx = h.client_ctrl_ctx;
    CHECK(h3zero_declare_stream_prefix(h.client_h3_ctx,
              h.client_ctrl_ctx->stream_id, managed_client_wt_cb, m) == 0);

    /* The server writes a whole uni stream raw: one byte and FIN. */
    h3zero_stream_ctx_t *ssc = picowt_create_local_stream(
        h.test_ctx->cnx_server, 0, h.server_wt.h3_ctx,
        h.server_wt.ctrl_ctx->stream_id);
    CHECK(ssc != NULL);
    uint64_t sid = ssc ? ssc->stream_id : 0;
    static const uint8_t payload[] = { 0xaf, 0x00, 0x00, 0x00 };  /* SETUP */
    CHECK(ssc && picoquic_add_to_stream(h.test_ctx->cnx_server, sid, payload,
                                        sizeof(payload), 1) == 0);
    CHECK(pico_wt_harness_pump(&h, 300) == 0);

    CHECK(m->early_nstreams == 1);
    CHECK(m->early_tail && m->early_tail->kind == PWM_EARLY_FIN);
    /* picoquic retired the client's stream once the data and FIN were in */
    CHECK(picoquic_find_stream(h.test_ctx->cnx_client, sid) == NULL);

    /* Accept, as managed_client_wt_cb does on picohttp_callback_connect_accepted. */
    moq_session_t *s = NULL;
    moq_session_cfg_t scfg;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_CLIENT);
    scfg.version = MOQ_VERSION_DRAFT_18;
    CHECK(moq_session_create(&scfg, h.now, &s) == MOQ_OK);
    moq_pico_wt_conn_cfg_t wc;
    moq_pico_wt_conn_cfg_init_sized(&wc, sizeof(wc));
    wc.session = s;
    wc.cnx = h.test_ctx->cnx_client;
    wc.h3_ctx = h.client_h3_ctx;
    wc.ctrl_ctx = h.client_ctrl_ctx;
    wc.alloc = &g_once_alloc;
    moq_pico_wt_conn_t *conn = NULL;
    CHECK(moq_pico_wt_conn_create(&wc, &conn) == 0);
    if (conn) {
        h.client_conn = conn;
        moq_session_destroy(h.client_session);
        h.client_session = s;
        CHECK(moq_session_start(s, h.now) == MOQ_OK);
        g_fail_next = fail_mark;   /* the complete stream's receive state */
        pwm_early_replay(m, h.test_ctx->cnx_client, conn);
        g_fail_next = 0;
        CHECK(m->early_head == NULL && m->early_ended);
        CHECK(!conn->rx_replay_complete);
    }
    if (conn && fail_mark) {
        /* The complete stream's receive state could not be created: the
         * adapter's inbound failure, and the retired stream never reaches the
         * session -- no receive state, so no window was ever asked of the
         * transport. */
        CHECK(moq_pico_wt_conn_is_fatal(conn));
        CHECK(moq_transport_bridge_fatal_code(conn->bridge) ==
              PICO_WT_INBOUND_FATAL_CODE);
        CHECK(active_rx(conn) == 0);
    } else if (conn) {
        /* The replayed SETUP and FIN reach the session, whose verdict on a
         * peer control stream that ended is a PROTOCOL_VIOLATION (0x3); it is
         * not the adapter failing to own the retired stream's window (the
         * inbound fatal code). */
        uint64_t code = moq_transport_bridge_fatal_code(conn->bridge);
        if (code == PICO_WT_INBOUND_FATAL_CODE)
            fprintf(stderr, "  replay of the retired stream was a transport failure\n");
        CHECK(code != PICO_WT_INBOUND_FATAL_CODE);
        CHECK(moq_pico_wt_conn_is_fatal(conn) && code == 0x3);
    } else {
        moq_session_destroy(s);
    }
    CHECK(g_live == 0);
    pico_wt_harness_cleanup(&h);
    shell_free(m);
}

static const pw_rx_stream_t *rx_of(const moq_pico_wt_conn_t *c, uint64_t sid)
{
    for (size_t i = 0; i < c->rx_count; i++)
        if (c->rx[i].active && c->rx[i].stream_id == sid) return &c->rx[i];
    return NULL;
}

/* Whether picoquic leaves `sid`'s receive window to the application: 1 or 0,
 * or -1 when picoquic no longer has the stream. */
static int pq_app_fc(picoquic_cnx_t *cnx, uint64_t sid)
{
    picoquic_stream_head_t *st = picoquic_find_stream(cnx, sid);
    return st ? (int)st->use_app_flow_control : -1;
}

/* Two streams buffered interleaved before the CONNECT response: a request
 * bidi whose standalone FETCH arrives in two DATA entries and whose FIN (the
 * fetcher closing its half) follows as a third, and the server's control uni
 * whose SETUP arrives in two DATA entries with no FIN. Replayed into the real
 * adapter:
 *   - each stream's entries arrive in order: the split SETUP establishes the
 *     session and the split FETCH surfaces as one request;
 *   - the bidi's FIN is already in the buffer when its first DATA is
 *     replayed, so the adapter records the stream complete at first sight and
 *     never takes its window: picoquic still grows that stream's window
 *     itself. Its send half is still open, so picoquic has not retired it and
 *     the flag is observable after replay;
 *   - the control uni's FIN has not arrived, so it gains no completion fact:
 *     the adapter takes its window (picoquic now waits for the adapter's
 *     grants) and keeps an incomplete receive state;
 *   - the transient completion marker is clear once replay returns. */
static void test_early_fin_interleaved_real_adapter(void)
{
    pico_wt_harness_t h;
    pico_wt_harness_cfg_t cfg = { .cid_byte = 0x9d, .request_capacity = 10,
                                  .version = MOQ_VERSION_DRAFT_18 };
    if (pico_wt_harness_setup(&h, &cfg) != 0) {
        CHECK(0 && "harness setup");
        pico_wt_harness_cleanup(&h);
        return;
    }
    moq_pico_wt_conn_destroy(h.client_conn);
    h.client_conn = NULL;
    moq_pico_wt_managed_t *m = shell();
    m->h3_ctx = h.client_h3_ctx;
    m->ctrl_ctx = h.client_ctrl_ctx;
    CHECK(h3zero_declare_stream_prefix(h.client_h3_ctx,
              h.client_ctrl_ctx->stream_id, managed_client_wt_cb, m) == 0);

    uint8_t msg[128];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, msg, sizeof(msg));
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_d18_fetch_t f;
    memset(&f, 0, sizeof(f));
    f.request_id = 1;
    f.fetch_type = 1;                               /* standalone */
    f.track_namespace = (moq_namespace_t){ parts, 1 };
    f.track_name = MOQ_BYTES_LITERAL("video");
    f.start = (moq_d18_location_t){ 0, 0 };
    f.end = (moq_d18_location_t){ 10, 0 };
    CHECK(moq_d18_encode_fetch(&w, &f) == MOQ_OK);
    size_t n = moq_buf_writer_offset(&w);
    size_t half = n / 2;
    static const uint8_t setup[] = { 0xaf, 0x00, 0x00, 0x00 };

    h3zero_stream_ctx_t *req = picowt_create_local_stream(
        h.test_ctx->cnx_server, 1, h.server_wt.h3_ctx,
        h.server_wt.ctrl_ctx->stream_id);
    h3zero_stream_ctx_t *ctl = picowt_create_local_stream(
        h.test_ctx->cnx_server, 0, h.server_wt.h3_ctx,
        h.server_wt.ctrl_ctx->stream_id);
    CHECK(req != NULL && ctl != NULL);
    if (req == NULL || ctl == NULL) {
        pico_wt_harness_cleanup(&h);
        shell_free(m);
        return;
    }
    uint64_t rsid = req->stream_id, csid = ctl->stream_id;
    picoquic_cnx_t *srv = h.test_ctx->cnx_server;
    CHECK(picoquic_add_to_stream(srv, csid, setup, 2, 0) == 0);
    CHECK(pico_wt_harness_pump(&h, 200) == 0);
    CHECK(picoquic_add_to_stream(srv, rsid, msg, half, 0) == 0);
    CHECK(pico_wt_harness_pump(&h, 200) == 0);
    CHECK(picoquic_add_to_stream(srv, csid, setup + 2, 2, 0) == 0);
    CHECK(pico_wt_harness_pump(&h, 200) == 0);
    CHECK(picoquic_add_to_stream(srv, rsid, msg + half, n - half, 0) == 0);
    CHECK(pico_wt_harness_pump(&h, 200) == 0);
    CHECK(picoquic_add_to_stream(srv, rsid, NULL, 0, 1) == 0);
    CHECK(pico_wt_harness_pump(&h, 200) == 0);

    /* buffered in arrival order: C data, R data, C data, R data, R FIN */
    struct { uint64_t sid; int kind; size_t len; } want[] = {
        { csid, PWM_EARLY_DATA, 2 }, { rsid, PWM_EARLY_DATA, half },
        { csid, PWM_EARLY_DATA, 2 }, { rsid, PWM_EARLY_DATA, n - half },
        { rsid, PWM_EARLY_FIN, 0 } };
    size_t got = 0;
    for (const pwm_early_chunk_t *c = m->early_head; c; c = c->next, got++) {
        if (got < 5) {
            CHECK(c->sid == want[got].sid && (int)c->kind == want[got].kind &&
                  c->len == want[got].len);
        }
    }
    CHECK(got == 5);
    /* before replay, picoquic grows both windows itself */
    CHECK(pq_app_fc(h.test_ctx->cnx_client, rsid) == 0);
    CHECK(pq_app_fc(h.test_ctx->cnx_client, csid) == 0);

    moq_session_t *s = NULL;
    moq_session_cfg_t scfg;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_CLIENT);
    scfg.version = MOQ_VERSION_DRAFT_18;
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 10;
    CHECK(moq_session_create(&scfg, h.now, &s) == MOQ_OK);
    moq_pico_wt_conn_cfg_t wc;
    moq_pico_wt_conn_cfg_init_sized(&wc, sizeof(wc));
    wc.session = s;
    wc.cnx = h.test_ctx->cnx_client;
    wc.h3_ctx = h.client_h3_ctx;
    wc.ctrl_ctx = h.client_ctrl_ctx;
    wc.alloc = moq_alloc_default();
    moq_pico_wt_conn_t *conn = NULL;
    CHECK(moq_pico_wt_conn_create(&wc, &conn) == 0);
    if (conn) {
        h.client_conn = conn;
        moq_session_destroy(h.client_session);
        h.client_session = s;
        CHECK(moq_session_start(s, h.now) == MOQ_OK);

        pwm_early_replay(m, h.test_ctx->cnx_client, conn);
        CHECK(m->early_head == NULL && m->early_ended);
        CHECK(!conn->rx_replay_complete);

        /* the request bidi: its window was never taken (the adapter only
         * ever sets picoquic's flag, at first sight), and its receive state
         * was released once its FIN was fed */
        CHECK(pq_app_fc(h.test_ctx->cnx_client, rsid) == 0);
        CHECK(rx_of(conn, rsid) == NULL);
        /* the control uni: no FIN yet, so no completion; window taken */
        const pw_rx_stream_t *u = rx_of(conn, csid);
        CHECK(u != NULL && !u->rx_complete && u->app_fc);
        CHECK(pq_app_fc(h.test_ctx->cnx_client, csid) == 1);

        /* both streams' entries reached the session in order */
        int requests = 0;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_REQUEST) requests++;
            moq_event_cleanup(&ev);
        }
        CHECK(requests == 1);
        CHECK(moq_session_state(s) == MOQ_SESS_ESTABLISHED);
        CHECK(!moq_pico_wt_conn_is_fatal(conn));
    } else {
        moq_session_destroy(s);
    }
    CHECK(g_live == 0);
    pico_wt_harness_cleanup(&h);
    shell_free(m);
}

/* The completion marker around a single replayed delivery of a complete
 * stream, through the facade's own delivery step into the real adapter:
 *   - the delivery whose receive state cannot be created fails the adapter
 *     with its inbound fatal code, leaves no receive state, and leaves the
 *     marker clear;
 *   - a further delivery, now into a terminal adapter, creates nothing and
 *     also leaves the marker clear. */
static void test_replay_marker_cleared_on_failure(void)
{
    pico_wt_harness_t h;
    pico_wt_harness_cfg_t cfg = { .cid_byte = 0x9c, .request_capacity = 10,
                                  .version = MOQ_VERSION_DRAFT_18 };
    if (pico_wt_harness_setup(&h, &cfg) != 0) {
        CHECK(0 && "harness setup");
        pico_wt_harness_cleanup(&h);
        return;
    }
    moq_pico_wt_conn_destroy(h.client_conn);
    h.client_conn = NULL;
    moq_pico_wt_managed_t *m = shell();
    m->h3_ctx = h.client_h3_ctx;
    m->ctrl_ctx = h.client_ctrl_ctx;
    CHECK(h3zero_declare_stream_prefix(h.client_h3_ctx,
              h.client_ctrl_ctx->stream_id, managed_client_wt_cb, m) == 0);

    moq_session_t *s = NULL;
    moq_session_cfg_t scfg;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_CLIENT);
    scfg.version = MOQ_VERSION_DRAFT_18;
    CHECK(moq_session_create(&scfg, h.now, &s) == MOQ_OK);
    moq_pico_wt_conn_cfg_t wc;
    moq_pico_wt_conn_cfg_init_sized(&wc, sizeof(wc));
    wc.session = s;
    wc.cnx = h.test_ctx->cnx_client;
    wc.h3_ctx = h.client_h3_ctx;
    wc.ctrl_ctx = h.client_ctrl_ctx;
    wc.alloc = &g_once_alloc;
    moq_pico_wt_conn_t *conn = NULL;
    CHECK(moq_pico_wt_conn_create(&wc, &conn) == 0);
    if (conn) {
        h.client_conn = conn;
        moq_session_destroy(h.client_session);
        h.client_session = s;
        CHECK(moq_session_start(s, h.now) == MOQ_OK);

        /* a retired peer uni: only its stream id survives */
        h3zero_stream_ctx_t retired;
        memset(&retired, 0, sizeof(retired));
        retired.stream_id = 4u * 40u + 3u;
        static const uint8_t setup[] = { 0xaf, 0x00, 0x00, 0x00 };

        g_fail_next = 1;   /* the complete stream's receive state */
        pwm_early_deliver(h.test_ctx->cnx_client, (uint8_t *)(uintptr_t)setup,
                          sizeof(setup), picohttp_callback_post_data,
                          &retired, conn, true);
        g_fail_next = 0;
        CHECK(moq_pico_wt_conn_is_fatal(conn));
        CHECK(moq_transport_bridge_fatal_code(conn->bridge) ==
              PICO_WT_INBOUND_FATAL_CODE);
        CHECK(active_rx(conn) == 0);
        CHECK(!conn->rx_replay_complete);

        pwm_early_deliver(h.test_ctx->cnx_client, NULL, 0,
                          picohttp_callback_post_fin, &retired, conn, true);
        CHECK(moq_transport_bridge_is_terminal(conn->bridge));
        CHECK(active_rx(conn) == 0);
        CHECK(!conn->rx_replay_complete);
    } else {
        moq_session_destroy(s);
    }
    pico_wt_harness_cleanup(&h);
    shell_free(m);
}

/* A server-initiated request bidi delivered before the CONNECT response: a
 * whole SUBSCRIBE in two chunks, then the server's STOP_SENDING for the
 * client's half; no bytes follow the STOP. After acceptance the replay
 * reaches the real adapter in that order: the request surfaces from both
 * chunks, and the STOP reaches the adapter and cancels the client's sending
 * direction on that stream -- its send half is reset and output already
 * queued on it is dropped (a peer-opened request bidi's STOP is its requester
 * cancelling it) -- with the session still up. What happens to bytes or a FIN
 * the peer sends after its STOP is the session's request-stream lifetime,
 * pinned by request_stop_lifetime. */
static void test_early_stop_real_adapter(void)
{
    pico_wt_harness_t h;
    pico_wt_harness_cfg_t cfg = { .cid_byte = 0x9f, .request_capacity = 10,
                                  .version = MOQ_VERSION_DRAFT_18 };
    if (pico_wt_harness_setup(&h, &cfg) != 0) {
        CHECK(0 && "harness setup");
        pico_wt_harness_cleanup(&h);
        return;
    }
    moq_pico_wt_conn_destroy(h.client_conn);
    h.client_conn = NULL;
    moq_pico_wt_managed_t *m = shell();
    m->h3_ctx = h.client_h3_ctx;
    m->ctrl_ctx = h.client_ctrl_ctx;
    CHECK(h3zero_declare_stream_prefix(h.client_h3_ctx,
              h.client_ctrl_ctx->stream_id, managed_client_wt_cb, m) == 0);

    uint8_t msg[128];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, msg, sizeof(msg));
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_namespace_t ns = { parts, 1 };
    moq_d18_msg_params_t params;
    memset(&params, 0, sizeof(params));
    CHECK(moq_d18_encode_subscribe(&w, 1, &ns, MOQ_BYTES_LITERAL("video"),
                                   &params) == MOQ_OK);
    size_t n = moq_buf_writer_offset(&w);
    size_t half = n / 2;

    /* The server's SETUP on a control uni, also early, so the client session
     * can establish and surface the request. */
    h3zero_stream_ctx_t *ctl = picowt_create_local_stream(
        h.test_ctx->cnx_server, 0, h.server_wt.h3_ctx,
        h.server_wt.ctrl_ctx->stream_id);
    static const uint8_t setup[] = { 0xaf, 0x00, 0x00, 0x00 };
    CHECK(ctl && picoquic_add_to_stream(h.test_ctx->cnx_server, ctl->stream_id,
                                        setup, sizeof(setup), 0) == 0);

    h3zero_stream_ctx_t *ssc = picowt_create_local_stream(
        h.test_ctx->cnx_server, 1, h.server_wt.h3_ctx,
        h.server_wt.ctrl_ctx->stream_id);
    CHECK(ssc != NULL);
    uint64_t sid = ssc ? ssc->stream_id : 0;
    CHECK(ssc && picoquic_add_to_stream(h.test_ctx->cnx_server, sid, msg,
                                        half, 0) == 0);
    CHECK(pico_wt_harness_pump(&h, 200) == 0);
    CHECK(picoquic_add_to_stream(h.test_ctx->cnx_server, sid, msg + half,
                                 n - half, 0) == 0);
    CHECK(pico_wt_harness_pump(&h, 200) == 0);
    CHECK(picoquic_stop_sending(h.test_ctx->cnx_server, sid, 0x170d7b68) == 0);
    CHECK(pico_wt_harness_pump(&h, 200) == 0);

    /* buffered for the bidi: data, data, STOP -- in that order */
    int kinds[8], nk = 0;
    for (const pwm_early_chunk_t *c = m->early_head; c && nk < 8; c = c->next)
        if (c->sid == sid) kinds[nk++] = c->kind;
    CHECK(nk == 3 && kinds[0] == PWM_EARLY_DATA && kinds[1] == PWM_EARLY_DATA &&
          kinds[2] == PWM_EARLY_STOP);

    moq_session_t *s = NULL;
    moq_session_cfg_t scfg;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_CLIENT);
    scfg.version = MOQ_VERSION_DRAFT_18;
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 10;
    CHECK(moq_session_create(&scfg, h.now, &s) == MOQ_OK);
    moq_pico_wt_conn_cfg_t wc;
    moq_pico_wt_conn_cfg_init_sized(&wc, sizeof(wc));
    wc.session = s;
    wc.cnx = h.test_ctx->cnx_client;
    wc.h3_ctx = h.client_h3_ctx;
    wc.ctrl_ctx = h.client_ctrl_ctx;
    wc.alloc = moq_alloc_default();
    moq_pico_wt_conn_t *conn = NULL;
    CHECK(moq_pico_wt_conn_create(&wc, &conn) == 0);
    if (conn) {
        h.client_conn = conn;
        moq_session_destroy(h.client_session);
        h.client_session = s;
        CHECK(moq_session_start(s, h.now) == MOQ_OK);

        /* Output queued on the client's half before the STOP is replayed,
         * and the baseline: picoquic has the STOP but answers it with no
         * reset of its own, so whatever reset follows is the adapter's. */
        static const uint8_t queued[] = { 0x55, 0x55 };
        CHECK(conn->endpoint_ops.write(&conn->endpoint_ctx, sid, queued,
                                       sizeof(queued), false) ==
              MOQ_TRANSPORT_OK);
        picoquic_stream_head_t *qs =
            picoquic_find_stream(h.test_ctx->cnx_client, sid);
        CHECK(qs != NULL && qs->stop_sending_received && !qs->reset_requested);
        CHECK(moq_pq_send_queue_has_data(conn->endpoint_ctx.queue, sid));

        pwm_early_replay(m, h.test_ctx->cnx_client, conn);
        moq_pico_wt_service(conn, h.now);

        /* the STOP reached the adapter and cancelled our direction: the
         * send half is reset and the output queued on it dropped */
        CHECK(conn->stop_sending_count == 1);
        CHECK(conn->last_stop_sending_stream_id == sid);
        qs = picoquic_find_stream(h.test_ctx->cnx_client, sid);
        CHECK(qs == NULL || qs->reset_requested);
        CHECK(!moq_pq_send_queue_has_data(conn->endpoint_ctx.queue, sid));
        /* both chunks, which arrived before the STOP, were processed: the
         * whole request surfaced */
        int requests = 0;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) requests++;
            moq_event_cleanup(&ev);
        }
        CHECK(requests == 1);
        CHECK(moq_session_state(s) == MOQ_SESS_ESTABLISHED);
        CHECK(!moq_pico_wt_conn_is_fatal(conn) && !moq_pico_wt_conn_is_closed(conn));
    } else {
        moq_session_destroy(s);
    }
    CHECK(g_live == 0);
    pico_wt_harness_cleanup(&h);
    shell_free(m);
}

int main(void)
{
    moq_pico_wt_managed_test_abort_stream = rec_abort;
    test_order_and_replay();
    test_stream_limit();
    test_entry_and_storage_limits();
    test_reset_and_freed_context();
    test_connect_refused();
    test_early_fin_real_transport(false);
    test_early_fin_real_transport(true);
    test_early_stop_real_adapter();
    test_early_fin_interleaved_real_adapter();
    test_replay_marker_cleared_on_failure();
    if (failures == 0)
        printf("test_pico_wt_managed_early_streams: PASS\n");
    return failures ? 1 : 0;
}
