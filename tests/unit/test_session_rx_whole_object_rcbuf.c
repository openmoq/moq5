/* Whole-object receive (streaming_objects false) over rcbuf ingest, on every
 * supported draft. When an object's header arrives without any of its payload
 * and the next moq_session_on_data_rcbuf call delivers, with nothing buffered
 * ahead of it, an input that is exactly that payload, is a created rcbuf
 * (neither a slice nor wrapped) and was allocated with the session's own
 * allocator, the OBJECT_RECEIVED payload is that input itself: it keeps alive
 * exactly what the copy it replaces would, so every byte charge stays exact.
 * Any other payload is a copy, and the session keeps no reference to the
 * input. The caller keeps its own reference and may feed
 * one input more than once; every alias is released exactly once. A payload
 * whose header arrived without any of its bytes is admitted and charged with
 * no storage yet; teardown in that state releases the charge.
 *
 * Every case drives a real client/server pair: the server writes objects,
 * the test captures each SEND_DATA's header and payload and feeds them to
 * the client in the shape the case names. Input roots come from the client's
 * own allocator, which watches them, so the release of a root is observed
 * directly; the server allocates from another. */
#include "test_session_support.h"
#include "test_raw_pair_support.h"

#include <stdint.h>
#include <string.h>

#define WS_MAX_UNITS 16
#define WS_WIRE_CAP  4096
#define WS_NOW       100u

static int failures = 0;
static moq_version_t g_version;

/* -- Watching allocator: counts, and observes the frees of chosen blocks -- */

typedef struct {
    int64_t balance;
    void   *watch[4];
    int     freed[4];
    size_t  refuse_size;    /* refuse the next allocation of exactly this size */
    int     refused;
} ws_alloc_t;

static void *ws_alloc_fn(size_t n, void *ctx)
{
    ws_alloc_t *a = ctx;
    if (a->refuse_size && n == a->refuse_size) {
        a->refuse_size = 0;
        a->refused++;
        return NULL;
    }
    void *p = malloc(n);
    if (p) a->balance++;
    return p;
}
static void *ws_realloc_fn(void *p, size_t o, size_t n, void *ctx)
{
    (void)o; (void)ctx;
    return realloc(p, n);
}
static void ws_free_fn(void *p, size_t n, void *ctx)
{
    ws_alloc_t *a = ctx;
    (void)n;
    if (!p) return;
    for (int i = 0; i < 4; i++)
        if (a->watch[i] == p) { a->freed[i]++; a->watch[i] = NULL; }
    a->balance--;
    free(p);
}
static moq_alloc_t ws_allocator(ws_alloc_t *a)
{
    return (moq_alloc_t){ a, ws_alloc_fn, ws_realloc_fn, ws_free_fn };
}

/* A created input root over `n` bytes, watched in slot `w`. */
static moq_rcbuf_t *ws_root(ws_alloc_t *a, const moq_alloc_t *al, const uint8_t *src,
                            size_t n, int w)
{
    moq_rcbuf_t *b = NULL;
    if (moq_rcbuf_create(al, src, n, &b) != MOQ_OK) abort();
    a->watch[w] = b;
    a->freed[w] = 0;
    return b;
}

/* -- The pair --------------------------------------------------------------- */

typedef struct {
    size_t   hdr_off[WS_MAX_UNITS];
    size_t   hdr_len[WS_MAX_UNITS];
    size_t   pl_off[WS_MAX_UNITS];
    size_t   pl_len[WS_MAX_UNITS];
    size_t   units;
    uint8_t  wire[WS_WIRE_CAP];
    size_t   len;
} ws_stream_t;

typedef struct {
    ws_alloc_t         sa;     /* the server's allocator */
    moq_alloc_t        alloc;
    ws_alloc_t         ia;     /* the client's allocator, and the input roots' */
    moq_alloc_t        in_alloc;
    moq_session_t     *c, *sv;
    moq_subscription_t ssub;
    uint64_t           next_group;
} ws_pair_t;

static moq_session_t *ws_session(ws_pair_t *p, moq_perspective_t persp, size_t max_events,
                                 uint64_t budget)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg),
                               persp == MOQ_PERSPECTIVE_CLIENT ? &p->in_alloc : &p->alloc,
                               persp);
    cfg.version = g_version;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 64;
    if (persp == MOQ_PERSPECTIVE_CLIENT) {
        cfg.streaming_objects = false;
        if (max_events) cfg.max_events = max_events;
        if (budget) cfg.max_receive_buffer_bytes = budget;
    }
    moq_session_t *s = NULL;
    if (moq_session_create(&cfg, 0, &s) < 0) return NULL;
    if (test_bind_if_required(s) < 0) { moq_session_destroy(s); return NULL; }
    if (moq_session_start(s, 0) < 0 && persp == MOQ_PERSPECTIVE_CLIENT) {
        moq_session_destroy(s);
        return NULL;
    }
    return s;
}

static bool ws_take(moq_session_t *s, moq_event_kind_t kind, moq_event_t *out)
{
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) == 1) {
        if (ev.kind == kind) { *out = ev; return true; }
        moq_event_cleanup(&ev);
    }
    return false;
}

static void ws_drain(moq_session_t *s)
{
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) == 1) moq_event_cleanup(&ev);
}

static bool ws_open(ws_pair_t *p, size_t max_events, uint64_t budget)
{
    memset(p, 0, sizeof(*p));
    p->alloc = ws_allocator(&p->sa);
    p->in_alloc = ws_allocator(&p->ia);
    p->c = ws_session(p, MOQ_PERSPECTIVE_CLIENT, max_events, budget);
    p->sv = ws_session(p, MOQ_PERSPECTIVE_SERVER, 0, 0);
    if (!p->c || !p->sv) return false;
    run(p->c, p->sv, 1);
    ws_drain(p->c);
    ws_drain(p->sv);
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ parts, 1 };
    sc.track_name = MOQ_BYTES_LITERAL("video");
    sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    sc.start_group = 1;
    sc.start_object = 0;
    moq_subscription_t sub;
    if (moq_session_subscribe(p->c, &sc, WS_NOW, &sub) != MOQ_OK) return false;
    run(p->c, p->sv, WS_NOW);
    moq_event_t ev;
    if (!ws_take(p->sv, MOQ_EVENT_SUBSCRIBE_REQUEST, &ev)) return false;
    p->ssub = ev.u.subscribe_request.sub;
    moq_event_cleanup(&ev);
    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    if (g_version == MOQ_VERSION_DRAFT_22) {
        ac.has_largest = true;
        ac.largest_group = 0;
        ac.largest_object = 0;
    }
    if (moq_session_accept_subscribe(p->sv, p->ssub, &ac, WS_NOW) != MOQ_OK) return false;
    run(p->c, p->sv, WS_NOW);
    ws_drain(p->c);
    p->next_group = 1;
    return true;
}

static void ws_close(ws_pair_t *p)
{
    if (p->c) moq_session_destroy(p->c);
    if (p->sv) moq_session_destroy(p->sv);
    MOQ_TEST_CHECK(p->sa.balance == 0);
    MOQ_TEST_CHECK(p->ia.balance == 0);
}

/* Payload byte k of object i (fill 0 means the pattern; otherwise constant). */
static uint8_t ws_byte(size_t i, size_t k, int fill)
{
    return fill ? (uint8_t)fill : (uint8_t)((i * 31 + k) & 0xff);
}

/* The server writes one subgroup (the next group) of objects with the given
 * payload sizes and closes it; its SEND_DATA actions are captured in order. */
static void ws_capture(ws_pair_t *p, const size_t *sizes, size_t n, int fill, ws_stream_t *st)
{
    memset(st, 0, sizeof(*st));
    moq_subgroup_cfg_t gc;
    moq_subgroup_cfg_init(&gc);
    gc.group_id = p->next_group++;
    moq_subgroup_handle_t sg;
    MOQ_TEST_CHECK(moq_session_open_subgroup(p->sv, p->ssub, &gc, WS_NOW, &sg) == MOQ_OK);
    for (size_t i = 0; i < n; i++) {
        uint8_t body[1024];
        for (size_t k = 0; k < sizes[i]; k++) body[k] = ws_byte(i, k, fill);
        moq_rcbuf_t *pl = NULL;
        MOQ_TEST_CHECK(moq_rcbuf_create(&p->alloc, body, sizes[i], &pl) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_write_object(p->sv, sg, i, pl, WS_NOW) == MOQ_OK);
        moq_rcbuf_decref(pl);
    }
    MOQ_TEST_CHECK(moq_session_close_subgroup(p->sv, sg, WS_NOW) == MOQ_OK);
    moq_action_t a[16];
    size_t na;
    while ((na = moq_session_poll_actions(p->sv, a, 16)) > 0) {
        for (size_t i = 0; i < na; i++) {
            if (a[i].kind == MOQ_ACTION_SEND_DATA && st->units < WS_MAX_UNITS) {
                const moq_send_data_action_t *d = &a[i].u.send_data;
                size_t pl = d->payload ? moq_rcbuf_len(d->payload) : 0;
                if (st->len + d->header_len + pl <= WS_WIRE_CAP) {
                    size_t u = st->units++;
                    st->hdr_off[u] = st->len;
                    st->hdr_len[u] = d->header_len;
                    memcpy(st->wire + st->len, d->header, d->header_len);
                    st->len += d->header_len;
                    st->pl_off[u] = st->len;
                    st->pl_len[u] = pl;
                    if (pl) memcpy(st->wire + st->len, moq_rcbuf_data(d->payload), pl);
                    st->len += pl;
                }
            } else if (a[i].kind != MOQ_ACTION_SEND_DATA) {
                deliver(p->c, &a[i], true, WS_NOW);
            }
            moq_action_cleanup(&a[i]);
        }
    }
}

static bool ws_payload_ok(const moq_rcbuf_t *b, size_t obj, size_t size, int fill)
{
    if (!b || moq_rcbuf_len(b) != size) return false;
    const uint8_t *d = moq_rcbuf_data(b);
    for (size_t k = 0; k < size; k++)
        if (d[k] != ws_byte(obj, k, fill)) return false;
    return true;
}

static bool ws_within(const moq_rcbuf_t *b, const uint8_t *lo, size_t n)
{
    uintptr_t q = (uintptr_t)moq_rcbuf_data(b), l = (uintptr_t)lo;
    return q >= l && q < l + n;
}

/* Bytes [off, off + n) of the stream as plain bytes from a scratch copy that
 * is scrubbed after the call. */
static moq_result_t ws_bytes(ws_pair_t *p, moq_stream_ref_t ref, const ws_stream_t *st,
                             size_t off, size_t n, bool fin)
{
    uint8_t tmp[WS_WIRE_CAP];
    memcpy(tmp, st->wire + off, n);
    moq_result_t rc = moq_session_on_data_bytes(p->c, ref, n ? tmp : NULL, n, fin, WS_NOW);
    memset(tmp, 0xA5, sizeof(tmp));
    return rc;
}

static moq_rx_stream_t *ws_rx(ws_pair_t *p, moq_stream_ref_t ref)
{
    int slot = moq_index_find(p->c->idx_rx_by_ref, p->c->idx_rx_mask, ref._v);
    return slot >= 0 ? &p->c->rx_streams[slot] : NULL;
}

static moq_stream_ref_t ws_ref(unsigned n) { return moq_stream_ref_from_u64(0x200000u + n); }

/* -- Cases ------------------------------------------------------------------ */

/* Header via bytes, payload as its own created rcbuf: the payload is that
 * very rcbuf, no staging grows, and the root is freed once, at the last of
 * the caller's and the event's releases. */
static void test_split_created_input_is_shared(void)
{
    ws_pair_t p;
    MOQ_TEST_CHECK(ws_open(&p, 0, 0));
    const size_t sizes[] = { 600 };
    ws_stream_t st;
    ws_capture(&p, sizes, 1, 0, &st);
    MOQ_TEST_CHECK(st.units == 3);
    moq_stream_ref_t ref = ws_ref(1);
    MOQ_TEST_CHECK(ws_bytes(&p, ref, &st, 0, st.pl_off[1], false) == MOQ_OK);
    moq_rx_stream_t *rx = ws_rx(&p, ref);
    MOQ_TEST_CHECK(rx && rx->payload_deferred && !rx->payload_rcbuf);
    MOQ_TEST_CHECK(p.c->recv_payload_bytes == 600);

    moq_rcbuf_t *in = ws_root(&p.ia, &p.in_alloc, st.wire + st.pl_off[1], 600, 0);
    MOQ_TEST_CHECK(moq_session_on_data_rcbuf(p.c, ref, in, false, WS_NOW) == MOQ_OK);
    moq_rcbuf_decref(in);
    MOQ_TEST_CHECK(p.ia.freed[0] == 0);
    rx = ws_rx(&p, ref);
    MOQ_TEST_CHECK(rx && rx->input_len == 0 && rx->input_cap < 600);

    moq_event_t ev;
    MOQ_TEST_CHECK(ws_take(p.c, MOQ_EVENT_OBJECT_RECEIVED, &ev));
    MOQ_TEST_CHECK(ws_payload_ok(ev.u.object_received.payload, 0, 600, 0));
    MOQ_TEST_CHECK(ev.u.object_received.payload == in);
    MOQ_TEST_CHECK(p.c->recv_payload_bytes == 0);
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK(p.ia.freed[0] == 1);

    MOQ_TEST_CHECK(ws_bytes(&p, ref, &st, st.hdr_off[2], 0, true) == MOQ_OK);
    MOQ_TEST_CHECK(ws_take(p.c, MOQ_EVENT_SUBGROUP_FINISHED, &ev));
    moq_event_cleanup(&ev);
    ws_close(&p);
}

/* The same exact created input, but from an allocator other than the
 * session's (here the server's): its release may be valid only where that
 * allocator says, so the payload is a copy from the session's allocator, and
 * the input is released at the caller's decref. */
static void test_other_allocator_input_is_copied(void)
{
    ws_pair_t p;
    MOQ_TEST_CHECK(ws_open(&p, 0, 0));
    const size_t sizes[] = { 600 };
    ws_stream_t st;
    ws_capture(&p, sizes, 1, 0, &st);
    moq_stream_ref_t ref = ws_ref(2);
    MOQ_TEST_CHECK(ws_bytes(&p, ref, &st, 0, st.pl_off[1], false) == MOQ_OK);
    moq_rcbuf_t *in = ws_root(&p.sa, &p.alloc, st.wire + st.pl_off[1], 600, 0);
    MOQ_TEST_CHECK(moq_session_on_data_rcbuf(p.c, ref, in, false, WS_NOW) == MOQ_OK);
    MOQ_TEST_CHECK(moq_rcbuf_refcount(in) == 1);         /* the session kept nothing */
    moq_rcbuf_decref(in);
    MOQ_TEST_CHECK(p.sa.freed[0] == 1);
    moq_event_t ev;
    MOQ_TEST_CHECK(ws_take(p.c, MOQ_EVENT_OBJECT_RECEIVED, &ev));
    MOQ_TEST_CHECK(ws_payload_ok(ev.u.object_received.payload, 0, 600, 0));
    MOQ_TEST_CHECK(ev.u.object_received.payload != in);
    MOQ_TEST_CHECK(p.c->recv_payload_bytes == 0);
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK(ws_bytes(&p, ref, &st, st.hdr_off[2], 0, true) == MOQ_OK);
    MOQ_TEST_CHECK(ws_take(p.c, MOQ_EVENT_SUBGROUP_FINISHED, &ev));
    moq_event_cleanup(&ev);
    ws_close(&p);
}

/* A wrapped input whose visible span is exactly the payload but whose release
 * context owns a much larger backing block: its retained size is unknown, so
 * the payload is copied and the backing is released at the caller's decref. */
static int g_backing_releases;
static void ws_backing_release(void *ctx, const uint8_t *data, size_t len)
{
    (void)data; (void)len;
    free(ctx);
    g_backing_releases++;
}
static void test_wrapped_small_span_over_large_backing_is_copied(void)
{
    ws_pair_t p;
    MOQ_TEST_CHECK(ws_open(&p, 0, 0));
    const size_t sizes[] = { 600 };
    ws_stream_t st;
    ws_capture(&p, sizes, 1, 0, &st);
    moq_stream_ref_t ref = ws_ref(2);
    MOQ_TEST_CHECK(ws_bytes(&p, ref, &st, 0, st.pl_off[1], false) == MOQ_OK);
    uint8_t *backing = malloc(1u << 20);
    memcpy(backing + 4096, st.wire + st.pl_off[1], 600);
    g_backing_releases = 0;
    moq_rcbuf_t *in = NULL;
    MOQ_TEST_CHECK(moq_rcbuf_wrap(&p.in_alloc, backing + 4096, 600, ws_backing_release,
                                  backing, &in) == MOQ_OK);
    MOQ_TEST_CHECK(moq_session_on_data_rcbuf(p.c, ref, in, false, WS_NOW) == MOQ_OK);
    moq_rcbuf_decref(in);
    MOQ_TEST_CHECK(g_backing_releases == 1);   /* nothing kept */
    moq_event_t ev;
    MOQ_TEST_CHECK(ws_take(p.c, MOQ_EVENT_OBJECT_RECEIVED, &ev));
    MOQ_TEST_CHECK(ws_payload_ok(ev.u.object_received.payload, 0, 600, 0));
    moq_event_cleanup(&ev);
    ws_close(&p);
}

/* One created rcbuf carrying the subgroup header and two objects: neither
 * payload is the whole input, so both are copied and the session keeps no
 * reference to the input. */
static void test_coalesced_input_is_copied(void)
{
    ws_pair_t p;
    MOQ_TEST_CHECK(ws_open(&p, 0, 0));
    const size_t sizes[] = { 500, 3 };
    ws_stream_t st;
    ws_capture(&p, sizes, 2, 0, &st);
    MOQ_TEST_CHECK(st.units == 4);
    size_t n = st.pl_off[2] + st.pl_len[2];
    moq_stream_ref_t ref = ws_ref(3);
    moq_rcbuf_t *in = ws_root(&p.ia, &p.in_alloc, st.wire, n, 0);
    const uint8_t *base = moq_rcbuf_data(in);
    MOQ_TEST_CHECK(moq_session_on_data_rcbuf(p.c, ref, in, false, WS_NOW) == MOQ_OK);
    MOQ_TEST_CHECK(moq_rcbuf_refcount(in) == 1);
    moq_rcbuf_decref(in);
    MOQ_TEST_CHECK(p.ia.freed[0] == 1);
    moq_event_t a, b;
    MOQ_TEST_CHECK(ws_take(p.c, MOQ_EVENT_OBJECT_RECEIVED, &a));
    MOQ_TEST_CHECK(ws_take(p.c, MOQ_EVENT_OBJECT_RECEIVED, &b));
    MOQ_TEST_CHECK(ws_payload_ok(a.u.object_received.payload, 0, 500, 0));
    MOQ_TEST_CHECK(ws_payload_ok(b.u.object_received.payload, 1, 3, 0));
    MOQ_TEST_CHECK(!ws_within(a.u.object_received.payload, base, n));
    moq_event_cleanup(&a);
    moq_event_cleanup(&b);
    ws_close(&p);
}

/* Header alone, then one created rcbuf with that object's whole payload and
 * the next object, with the FIN: the input is more than the payload, so both
 * payloads are copied, the input is not kept, and the FIN finishes the
 * stream after both. */
static void test_deferred_payload_then_more_in_one_input(void)
{
    ws_pair_t p;
    MOQ_TEST_CHECK(ws_open(&p, 0, 0));
    const size_t sizes[] = { 600, 300 };
    ws_stream_t st;
    ws_capture(&p, sizes, 2, 0, &st);
    moq_stream_ref_t ref = ws_ref(4);
    MOQ_TEST_CHECK(ws_bytes(&p, ref, &st, 0, st.pl_off[1], false) == MOQ_OK);
    size_t n = st.len - st.pl_off[1];
    moq_rcbuf_t *in = ws_root(&p.ia, &p.in_alloc, st.wire + st.pl_off[1], n, 0);
    const uint8_t *base = moq_rcbuf_data(in);
    MOQ_TEST_CHECK(moq_session_on_data_rcbuf(p.c, ref, in, true, WS_NOW) == MOQ_OK);
    MOQ_TEST_CHECK(moq_rcbuf_refcount(in) == 1);
    moq_rcbuf_decref(in);
    MOQ_TEST_CHECK(p.ia.freed[0] == 1);
    moq_event_t a, b, f;
    MOQ_TEST_CHECK(ws_take(p.c, MOQ_EVENT_OBJECT_RECEIVED, &a));
    MOQ_TEST_CHECK(ws_take(p.c, MOQ_EVENT_OBJECT_RECEIVED, &b));
    MOQ_TEST_CHECK(ws_take(p.c, MOQ_EVENT_SUBGROUP_FINISHED, &f));
    MOQ_TEST_CHECK(ws_payload_ok(a.u.object_received.payload, 0, 600, 0));
    MOQ_TEST_CHECK(ws_payload_ok(b.u.object_received.payload, 1, 300, 0));
    MOQ_TEST_CHECK(!ws_within(a.u.object_received.payload, base, n));
    MOQ_TEST_CHECK(!moq_session_has_transport_stream(p.c, ref));
    moq_event_cleanup(&a);
    moq_event_cleanup(&b);
    moq_event_cleanup(&f);
    ws_close(&p);
}

/* One input fed as the payload of two streams of one session, and of a stream
 * of a second session with its own allocator: both payloads of the first
 * session are that same rcbuf, the second session copies it (the input is
 * not from its allocator), and the input is freed once, after the caller and
 * the first session's two events release it, in any order. */
static void test_one_input_fed_to_streams_and_sessions(void)
{
    ws_pair_t p, q;
    MOQ_TEST_CHECK(ws_open(&p, 0, 0));
    MOQ_TEST_CHECK(ws_open(&q, 0, 0));
    const size_t sizes[] = { 400 };
    ws_stream_t s1, s2, s3;
    ws_capture(&p, sizes, 1, 0x5A, &s1);
    ws_capture(&p, sizes, 1, 0x5A, &s2);
    ws_capture(&q, sizes, 1, 0x5A, &s3);
    MOQ_TEST_CHECK(ws_bytes(&p, ws_ref(5), &s1, 0, s1.pl_off[1], false) == MOQ_OK);
    MOQ_TEST_CHECK(ws_bytes(&p, ws_ref(6), &s2, 0, s2.pl_off[1], false) == MOQ_OK);
    MOQ_TEST_CHECK(ws_bytes(&q, ws_ref(7), &s3, 0, s3.pl_off[1], false) == MOQ_OK);
    moq_rcbuf_t *in = ws_root(&p.ia, &p.in_alloc, s1.wire + s1.pl_off[1], 400, 0);
    MOQ_TEST_CHECK(moq_session_on_data_rcbuf(p.c, ws_ref(5), in, false, WS_NOW) == MOQ_OK);
    MOQ_TEST_CHECK(moq_session_on_data_rcbuf(p.c, ws_ref(6), in, false, WS_NOW) == MOQ_OK);
    MOQ_TEST_CHECK(moq_session_on_data_rcbuf(q.c, ws_ref(7), in, false, WS_NOW) == MOQ_OK);
    MOQ_TEST_CHECK(moq_rcbuf_refcount(in) == 3);
    /* Each payload is charged in full: over-counting what one root
     * retains, never under-counting. */
    MOQ_TEST_CHECK(p.c->recv_payload_bytes == 800);
    MOQ_TEST_CHECK(q.c->recv_payload_bytes == 400);
    moq_event_t e1, e2, e3;
    MOQ_TEST_CHECK(ws_take(p.c, MOQ_EVENT_OBJECT_RECEIVED, &e1));
    MOQ_TEST_CHECK(ws_take(p.c, MOQ_EVENT_OBJECT_RECEIVED, &e2));
    MOQ_TEST_CHECK(ws_take(q.c, MOQ_EVENT_OBJECT_RECEIVED, &e3));
    MOQ_TEST_CHECK(e1.u.object_received.payload == in);
    MOQ_TEST_CHECK(e2.u.object_received.payload == in);
    MOQ_TEST_CHECK(e3.u.object_received.payload != in);
    MOQ_TEST_CHECK(ws_payload_ok(e3.u.object_received.payload, 0, 400, 0x5A));
    moq_event_cleanup(&e2);
    moq_rcbuf_decref(in);
    moq_event_cleanup(&e3);
    MOQ_TEST_CHECK(p.ia.freed[0] == 0);
    moq_event_cleanup(&e1);
    MOQ_TEST_CHECK(p.ia.freed[0] == 1);
    ws_close(&q);
    ws_close(&p);
}

/* Two overlapping slices of one created root fed as the payloads of two
 * streams: a slice is not a created root (it keeps its root's other bytes
 * and a second header alive), so both payloads are copied and the root is
 * freed at the caller's last release, before the events are. */
static void test_slices_are_copied(void)
{
    ws_pair_t p;
    MOQ_TEST_CHECK(ws_open(&p, 0, 0));
    const size_t sizes[] = { 600 };
    ws_stream_t s1, s2;
    ws_capture(&p, sizes, 1, 0x3C, &s1);
    ws_capture(&p, sizes, 1, 0x3C, &s2);
    MOQ_TEST_CHECK(ws_bytes(&p, ws_ref(8), &s1, 0, s1.pl_off[1], false) == MOQ_OK);
    MOQ_TEST_CHECK(ws_bytes(&p, ws_ref(9), &s2, 0, s2.pl_off[1], false) == MOQ_OK);
    uint8_t body[1000];
    memset(body, 0x3C, sizeof(body));
    moq_rcbuf_t *root = ws_root(&p.ia, &p.in_alloc, body, 1000, 0);
    const uint8_t *base = moq_rcbuf_data(root);
    moq_rcbuf_t *x = NULL, *y = NULL;
    MOQ_TEST_CHECK(moq_rcbuf_slice(&p.in_alloc, root, 0, 600, &x) == MOQ_OK);
    MOQ_TEST_CHECK(moq_rcbuf_slice(&p.in_alloc, root, 400, 600, &y) == MOQ_OK);
    MOQ_TEST_CHECK(moq_session_on_data_rcbuf(p.c, ws_ref(8), x, false, WS_NOW) == MOQ_OK);
    MOQ_TEST_CHECK(moq_session_on_data_rcbuf(p.c, ws_ref(9), y, false, WS_NOW) == MOQ_OK);
    MOQ_TEST_CHECK(moq_rcbuf_refcount(x) == 1 && moq_rcbuf_refcount(y) == 1);
    moq_rcbuf_decref(root);
    moq_rcbuf_decref(x);
    moq_rcbuf_decref(y);
    MOQ_TEST_CHECK(p.ia.freed[0] == 1);
    moq_event_t a, b;
    MOQ_TEST_CHECK(ws_take(p.c, MOQ_EVENT_OBJECT_RECEIVED, &a));
    MOQ_TEST_CHECK(ws_take(p.c, MOQ_EVENT_OBJECT_RECEIVED, &b));
    MOQ_TEST_CHECK(ws_payload_ok(a.u.object_received.payload, 0, 600, 0x3C));
    MOQ_TEST_CHECK(ws_payload_ok(b.u.object_received.payload, 0, 600, 0x3C));
    MOQ_TEST_CHECK(!ws_within(a.u.object_received.payload, base, 1000));
    MOQ_TEST_CHECK(!ws_within(b.u.object_received.payload, base, 1000));
    moq_event_cleanup(&a);
    moq_event_cleanup(&b);
    ws_close(&p);
}

/* The caller keeps its input past the call and past the event: the root is
 * freed at whichever release comes last, once. */
static void test_caller_retained_input(void)
{
    for (int caller_last = 0; caller_last < 2; caller_last++) {
        ws_pair_t p;
        MOQ_TEST_CHECK(ws_open(&p, 0, 0));
        const size_t sizes[] = { 300 };
        ws_stream_t st;
        ws_capture(&p, sizes, 1, 0, &st);
        moq_stream_ref_t ref = ws_ref(10);
        MOQ_TEST_CHECK(ws_bytes(&p, ref, &st, 0, st.pl_off[1], false) == MOQ_OK);
        moq_rcbuf_t *in = ws_root(&p.ia, &p.in_alloc, st.wire + st.pl_off[1], 300, 0);
        MOQ_TEST_CHECK(moq_session_on_data_rcbuf(p.c, ref, in, false, WS_NOW) == MOQ_OK);
        moq_event_t ev;
        MOQ_TEST_CHECK(ws_take(p.c, MOQ_EVENT_OBJECT_RECEIVED, &ev));
        MOQ_TEST_CHECK(ev.u.object_received.payload == in);
        MOQ_TEST_CHECK(moq_rcbuf_refcount(in) == 2);
        if (caller_last) moq_event_cleanup(&ev); else moq_rcbuf_decref(in);
        MOQ_TEST_CHECK(p.ia.freed[0] == 0);
        if (caller_last) moq_rcbuf_decref(in); else moq_event_cleanup(&ev);
        MOQ_TEST_CHECK(p.ia.freed[0] == 1);
        ws_close(&p);
    }
}

/* A payload that arrives as a slice, of a larger root or covering its whole
 * root, is copied: the receive charge is exactly the payload, and the session
 * keeps nothing of the root, which the caller's release frees at once. */
static void test_slice_inputs_are_copied_with_exact_charge(void)
{
    for (int whole = 0; whole < 2; whole++) {
        ws_pair_t p;
        MOQ_TEST_CHECK(ws_open(&p, 0, 0));
        const size_t sizes[] = { 100 };
        ws_stream_t st;
        ws_capture(&p, sizes, 1, 0, &st);
        moq_stream_ref_t ref = ws_ref(11);
        MOQ_TEST_CHECK(ws_bytes(&p, ref, &st, 0, st.pl_off[1], false) == MOQ_OK);
        size_t root_n = whole ? 100 : 150;
        uint8_t body[150];
        memset(body, 0, sizeof(body));
        memcpy(body, st.wire + st.pl_off[1], 100);
        moq_rcbuf_t *root = ws_root(&p.ia, &p.in_alloc, body, root_n, 0);
        moq_rcbuf_t *in = NULL;
        MOQ_TEST_CHECK(moq_rcbuf_slice(&p.in_alloc, root, 0, 100, &in) == MOQ_OK);
        const uint8_t *base = moq_rcbuf_data(root);
        moq_rcbuf_decref(root);
        MOQ_TEST_CHECK(moq_session_on_data_rcbuf(p.c, ref, in, false, WS_NOW) == MOQ_OK);
        MOQ_TEST_CHECK(moq_rcbuf_refcount(in) == 1);
        moq_rcbuf_decref(in);
        MOQ_TEST_CHECK(p.ia.freed[0] == 1);
        MOQ_TEST_CHECK(p.c->recv_payload_bytes == 100);
        moq_event_t ev;
        MOQ_TEST_CHECK(ws_take(p.c, MOQ_EVENT_OBJECT_RECEIVED, &ev));
        MOQ_TEST_CHECK(ws_payload_ok(ev.u.object_received.payload, 0, 100, 0));
        MOQ_TEST_CHECK(!ws_within(ev.u.object_received.payload, base, root_n));
        MOQ_TEST_CHECK(p.c->recv_payload_bytes == 0);
        moq_event_cleanup(&ev);
        ws_close(&p);
    }
}

/* Bytes ingest never exposes caller memory, split or coalesced. */
static void test_bytes_ingest_copies(void)
{
    ws_pair_t p;
    MOQ_TEST_CHECK(ws_open(&p, 0, 0));
    const size_t sizes[] = { 400, 400 };
    ws_stream_t st;
    ws_capture(&p, sizes, 2, 0, &st);
    moq_stream_ref_t ref = ws_ref(12);
    MOQ_TEST_CHECK(ws_bytes(&p, ref, &st, 0, st.pl_off[1], false) == MOQ_OK);
    MOQ_TEST_CHECK(ws_rx(&p, ref) && ws_rx(&p, ref)->payload_deferred);
    MOQ_TEST_CHECK(ws_bytes(&p, ref, &st, st.pl_off[1], 400, false) == MOQ_OK);
    MOQ_TEST_CHECK(ws_bytes(&p, ref, &st, st.hdr_off[2], st.hdr_len[2] + 400, true) == MOQ_OK);
    moq_event_t a, b, f;
    MOQ_TEST_CHECK(ws_take(p.c, MOQ_EVENT_OBJECT_RECEIVED, &a));
    MOQ_TEST_CHECK(ws_take(p.c, MOQ_EVENT_OBJECT_RECEIVED, &b));
    MOQ_TEST_CHECK(ws_take(p.c, MOQ_EVENT_SUBGROUP_FINISHED, &f));
    MOQ_TEST_CHECK(ws_payload_ok(a.u.object_received.payload, 0, 400, 0));
    MOQ_TEST_CHECK(ws_payload_ok(b.u.object_received.payload, 1, 400, 0));
    moq_event_cleanup(&a);
    moq_event_cleanup(&b);
    moq_event_cleanup(&f);
    ws_close(&p);
}

/* A payload split across two created inputs is copied; both are freed as
 * soon as the caller releases them. */
static void test_partial_payload_inputs_copy(void)
{
    ws_pair_t p;
    MOQ_TEST_CHECK(ws_open(&p, 0, 0));
    const size_t sizes[] = { 800 };
    ws_stream_t st;
    ws_capture(&p, sizes, 1, 0, &st);
    moq_stream_ref_t ref = ws_ref(13);
    MOQ_TEST_CHECK(ws_bytes(&p, ref, &st, 0, st.pl_off[1], false) == MOQ_OK);
    for (int half = 0; half < 2; half++) {
        moq_rcbuf_t *in = ws_root(&p.ia, &p.in_alloc,
                                  st.wire + st.pl_off[1] + (size_t)half * 400, 400, half);
        MOQ_TEST_CHECK(moq_session_on_data_rcbuf(p.c, ref, in, half == 1, WS_NOW) == MOQ_OK);
        moq_rcbuf_decref(in);
        MOQ_TEST_CHECK(p.ia.freed[half] == 1);
    }
    moq_event_t a, f;
    MOQ_TEST_CHECK(ws_take(p.c, MOQ_EVENT_OBJECT_RECEIVED, &a));
    MOQ_TEST_CHECK(ws_take(p.c, MOQ_EVENT_SUBGROUP_FINISHED, &f));
    MOQ_TEST_CHECK(ws_payload_ok(a.u.object_received.payload, 0, 800, 0));
    moq_event_cleanup(&a);
    moq_event_cleanup(&f);
    ws_close(&p);
}

/* A shared payload whose event cannot be queued stays parked (the stream
 * owns it): the input outlives the caller's reference, and the empty retry
 * delivers that same rcbuf. Then a reset while another shared payload is
 * parked releases it exactly once and returns the charge. */
static void test_parked_shared_payload(void)
{
    for (int reset = 0; reset < 2; reset++) {
        ws_pair_t p;
        MOQ_TEST_CHECK(ws_open(&p, 1, 0));
        const size_t sizes[] = { 300, 300 };
        ws_stream_t st;
        ws_capture(&p, sizes, 2, 0, &st);
        moq_stream_ref_t ref = ws_ref(14);
        MOQ_TEST_CHECK(ws_bytes(&p, ref, &st, 0, st.pl_off[1] + 300, false) == MOQ_OK);
        MOQ_TEST_CHECK(ws_bytes(&p, ref, &st, st.hdr_off[2], st.hdr_len[2], false) == MOQ_OK);
        moq_rcbuf_t *in = ws_root(&p.ia, &p.in_alloc, st.wire + st.pl_off[2], 300, 0);
        MOQ_TEST_CHECK(moq_session_on_data_rcbuf(p.c, ref, in, false, WS_NOW) == MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK(moq_session_has_transport_stream(p.c, ref));
        moq_rcbuf_decref(in);
        MOQ_TEST_CHECK(p.ia.freed[0] == 0);
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(p.c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ws_payload_ok(ev.u.object_received.payload, 0, 300, 0));
        moq_event_cleanup(&ev);
        if (reset) {
            (void)moq_session_on_data_reset(p.c, ref, 0, WS_NOW);
            ws_drain(p.c);
            MOQ_TEST_CHECK(p.ia.freed[0] == 1);
            MOQ_TEST_CHECK(p.c->recv_payload_bytes == 0);
        } else {
            MOQ_TEST_CHECK(moq_session_on_data_bytes(p.c, ref, NULL, 0, false, WS_NOW) == MOQ_OK);
            MOQ_TEST_CHECK(ws_take(p.c, MOQ_EVENT_OBJECT_RECEIVED, &ev));
            MOQ_TEST_CHECK(ev.u.object_received.payload == in);
            moq_event_cleanup(&ev);
            MOQ_TEST_CHECK(p.ia.freed[0] == 1);
        }
        ws_close(&p);
    }
}

/* A payload that cannot be shared (a slice) whose deferred allocation is
 * refused: the object stays admitted and charged, the session keeps no
 * reference to the input, the bytes are retained like any unparsed input, and
 * the retry delivers the object once memory returns. */
static void test_deferred_allocation_refused(void)
{
    ws_pair_t p;
    MOQ_TEST_CHECK(ws_open(&p, 0, 0));
    const size_t sizes[] = { 500 };
    ws_stream_t st;
    ws_capture(&p, sizes, 1, 0, &st);
    moq_stream_ref_t ref = ws_ref(15);
    MOQ_TEST_CHECK(ws_bytes(&p, ref, &st, 0, st.pl_off[1], false) == MOQ_OK);
    MOQ_TEST_CHECK(p.c->recv_payload_bytes == 500);
    moq_rcbuf_t *root = ws_root(&p.ia, &p.in_alloc, st.wire + st.pl_off[1], 500, 0);
    moq_rcbuf_t *in = NULL;
    MOQ_TEST_CHECK(moq_rcbuf_slice(&p.in_alloc, root, 0, 500, &in) == MOQ_OK);
    moq_rcbuf_decref(root);
    size_t pay = 0;
    MOQ_TEST_CHECK(moq_rcbuf_allocation_size(500, &pay) == MOQ_OK);
    p.ia.refuse_size = pay;
    MOQ_TEST_CHECK(moq_session_on_data_rcbuf(p.c, ref, in, false, WS_NOW) == MOQ_ERR_NOMEM);
    MOQ_TEST_CHECK(p.ia.refused == 1);
    MOQ_TEST_CHECK(moq_rcbuf_refcount(in) == 1);
    MOQ_TEST_CHECK(p.c->recv_payload_bytes == 500);
    moq_rcbuf_decref(in);
    MOQ_TEST_CHECK(p.ia.freed[0] == 1);
    moq_event_t ev;
    if (moq_session_poll_events(p.c, &ev, 1) != 0) {
        MOQ_TEST_CHECK(!"no event before the retry");
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK(moq_session_on_data_bytes(p.c, ref, NULL, 0, false, WS_NOW) == MOQ_OK);
    MOQ_TEST_CHECK(ws_take(p.c, MOQ_EVENT_OBJECT_RECEIVED, &ev));
    MOQ_TEST_CHECK(ws_payload_ok(ev.u.object_received.payload, 0, 500, 0));
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK(p.c->recv_payload_bytes == 0);
    ws_close(&p);
}

/* A payload the receive budget refuses: the stream is stopped at admission
 * and no reference to the input is kept. */
static void test_budget_refusal_keeps_nothing(void)
{
    ws_pair_t p;
    MOQ_TEST_CHECK(ws_open(&p, 0, 512));
    const size_t sizes[] = { 600 };
    ws_stream_t st;
    ws_capture(&p, sizes, 1, 0, &st);
    size_t n = st.pl_off[1] + st.pl_len[1];
    moq_stream_ref_t ref = ws_ref(16);
    moq_rcbuf_t *in = ws_root(&p.ia, &p.in_alloc, st.wire, n, 0);
    (void)moq_session_on_data_rcbuf(p.c, ref, in, false, WS_NOW);
    MOQ_TEST_CHECK(moq_rcbuf_refcount(in) == 1);
    MOQ_TEST_CHECK(p.c->recv_payload_bytes == 0);
    moq_rcbuf_decref(in);
    MOQ_TEST_CHECK(p.ia.freed[0] == 1);
    ws_drain(p.c);
    ws_close(&p);
}

/* An admitted object with its storage still deferred holds its charge until
 * the stream goes away: a peer reset, and separately session destruction,
 * release it with nothing leaked. */
static void test_deferred_teardown_releases_charge(void)
{
    for (int how = 0; how < 2; how++) {
        ws_pair_t p;
        MOQ_TEST_CHECK(ws_open(&p, 0, 0));
        const size_t sizes[] = { 700 };
        ws_stream_t st;
        ws_capture(&p, sizes, 1, 0, &st);
        moq_stream_ref_t ref = ws_ref(17);
        MOQ_TEST_CHECK(ws_bytes(&p, ref, &st, 0, st.pl_off[1], false) == MOQ_OK);
        MOQ_TEST_CHECK(ws_rx(&p, ref) && ws_rx(&p, ref)->payload_deferred);
        MOQ_TEST_CHECK(p.c->recv_payload_bytes == 700);
        if (how == 0) {
            (void)moq_session_on_data_reset(p.c, ref, 0, WS_NOW);
            ws_drain(p.c);
            MOQ_TEST_CHECK(p.c->recv_payload_bytes == 0);
            MOQ_TEST_CHECK(!moq_session_has_transport_stream(p.c, ref));
        }
        ws_close(&p);
    }
}

int main(void)
{
    static const moq_version_t versions[] = {
        MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_22,
    };
    for (size_t v = 0; v < sizeof(versions) / sizeof(versions[0]); v++) {
        g_version = versions[v];
        int before = failures;
        test_split_created_input_is_shared();
        test_other_allocator_input_is_copied();
        test_wrapped_small_span_over_large_backing_is_copied();
        test_coalesced_input_is_copied();
        test_deferred_payload_then_more_in_one_input();
        test_one_input_fed_to_streams_and_sessions();
        test_slices_are_copied();
        test_caller_retained_input();
        test_slice_inputs_are_copied_with_exact_charge();
        test_bytes_ingest_copies();
        test_partial_payload_inputs_copy();
        test_parked_shared_payload();
        test_deferred_allocation_refused();
        test_budget_refusal_keeps_nothing();
        test_deferred_teardown_releases_charge();
        printf("draft version 0x%llx: %d failure(s)\n",
               (unsigned long long)g_version, failures - before);
    }
    if (failures) {
        fprintf(stderr, "test_session_rx_whole_object_rcbuf: %d failure(s)\n", failures);
        return 1;
    }
    printf("test_session_rx_whole_object_rcbuf: all passed\n");
    return 0;
}
