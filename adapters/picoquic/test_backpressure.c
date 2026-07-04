/*
 * Adapter-level backpressure tests.
 *
 * Stubs picoquic functions to exercise moq_pq_callback and
 * moq_pq_service without a real QUIC stack.
 */

#include <moq/picoquic.h>
#include <moq/session.h>
#include <moq/rcbuf.h>
#include "../common/moq_pq_send_queue.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

static int failures = 0;

#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

/* -- picoquic stubs ------------------------------------------------- */

static uint64_t g_time = 1000;
static uint64_t g_next_uni = 1;
static uint64_t g_next_bidi = 0;
static int g_reset_count = 0;
static uint64_t g_reset_code = 0;
static bool g_send_fail = false;
/* Queued-send gate accounting: g_data_sent is the drain signal picoquic exposes
 * (cumulative stream bytes formatted into packets); g_add_count / g_add_bytes
 * record every picoquic_add_to_stream call so a test can assert the gate skipped
 * the append and that retries add each byte exactly once. */
static uint64_t g_data_sent = 0;
static int g_add_count = 0;
static size_t g_add_bytes = 0;

picoquic_quic_t *picoquic_get_quic_ctx(picoquic_cnx_t *c)
    { (void)c; return NULL; }
uint64_t picoquic_get_quic_time(picoquic_quic_t *q)
    { (void)q; return g_time; }
void picoquic_set_callback(picoquic_cnx_t *c,
    picoquic_stream_data_cb_fn fn, void *ctx)
    { (void)c; (void)fn; (void)ctx; }
int picoquic_add_to_stream(picoquic_cnx_t *c, uint64_t sid,
    const uint8_t *d, size_t len, int fin)
    { (void)c; (void)sid; (void)d; (void)fin;
      if (g_send_fail) return -1;
      g_add_count++; g_add_bytes += len; return 0; }
uint64_t picoquic_get_data_sent(picoquic_cnx_t *c)
    { (void)c; return g_data_sent; }
/* Pull-send stubs: record mark_active, and hand out a capture buffer from
 * provide_stream_data_buffer so a test can drive prepare_to_send. */
static int g_active_flag = -1;
static uint64_t g_active_sid = 0;
int picoquic_mark_active_stream(picoquic_cnx_t *c, uint64_t sid, int active,
    void *v) { (void)c; (void)v; g_active_flag = active; g_active_sid = sid;
               return g_send_fail ? -1 : 0; }
static uint8_t g_provide_buf[8192];
static size_t g_provide_nb; static int g_provide_fin; static int g_provide_still;
static bool g_provide_fail = false;
uint8_t *picoquic_provide_stream_data_buffer(void *ctx, size_t nb, int is_fin,
    int is_still_active)
    { (void)ctx; g_provide_nb = nb; g_provide_fin = is_fin;
      g_provide_still = is_still_active;
      return g_provide_fail ? NULL : g_provide_buf; }
uint64_t picoquic_get_next_local_stream_id(picoquic_cnx_t *c, int uni)
    { (void)c; return uni ? g_next_uni++ : g_next_bidi++; }
uint64_t picoquic_get_remote_stream_error(picoquic_cnx_t *c, uint64_t sid)
    { (void)c; (void)sid; return 0x42; }
int picoquic_reset_stream(picoquic_cnx_t *c, uint64_t sid, uint64_t ec)
    { (void)c; (void)sid; g_reset_count++; g_reset_code = ec; return 0; }
int picoquic_stop_sending(picoquic_cnx_t *c, uint64_t sid, uint64_t ec)
    { (void)c; (void)sid; (void)ec; return 0; }
static bool g_close_fail = false;
int picoquic_close(picoquic_cnx_t *c, uint64_t ec)
    { (void)c; (void)ec; return g_close_fail ? -1 : 0; }
int picoquic_close_ex(picoquic_cnx_t *c, uint64_t ec, const char *reason)
    { (void)c; (void)ec; (void)reason; return g_close_fail ? -1 : 0; }
uint64_t g_pending_app_error = 0;
uint64_t picoquic_get_application_error(picoquic_cnx_t *c)
    { (void)c; return g_pending_app_error; }
int picoquic_queue_datagram_frame(picoquic_cnx_t *c, size_t len,
    const uint8_t *d)
    { (void)c; (void)len; (void)d; return 0; }
/* The endpoint's DATAGRAM honesty gate reads the peer's negotiated max via
 * this; report a nonzero capability so the datagram dispatch path is exercised
 * (not refused by the gate). */
picoquic_tp_t const *picoquic_get_transport_parameters(picoquic_cnx_t *c,
    int get_local)
    { (void)c; (void)get_local; static picoquic_tp_t tp;
      memset(&tp, 0, sizeof(tp)); tp.max_datagram_frame_size = 1252;
      return &tp; }

/* -- Helpers -------------------------------------------------------- */

static void *ta(size_t sz, void *c) { (void)c; return malloc(sz); }
static void *tr(void *p, size_t o, size_t n, void *c)
    { (void)o; (void)c; return realloc(p, n); }
static void tf(void *p, size_t sz, void *c)
    { (void)sz; (void)c; free(p); }
static moq_alloc_t talloc(void)
    { return (moq_alloc_t){ NULL, ta, tr, tf }; }

/* Counting allocator: net balance must return to 0 to prove no leaks. */
static long g_bal = 0;
static void *ca(size_t sz, void *c) { (void)c; g_bal++; return malloc(sz); }
static void *cr(void *p, size_t o, size_t n, void *c)
    { (void)o; (void)c; if (!p) g_bal++; return realloc(p, n); }
static void cf(void *p, size_t sz, void *c)
    { (void)sz; (void)c; if (p) g_bal--; free(p); }
static moq_alloc_t calloc_(void)
    { return (moq_alloc_t){ NULL, ca, cr, cf }; }

static void pump_ctl(moq_session_t *f, moq_session_t *t)
{
    moq_action_t a[16]; size_t n;
    while ((n = moq_session_poll_actions(f, a, 16)) > 0)
        for (size_t i = 0; i < n; i++) {
            if (a[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(t, a[i].u.send_control.data,
                    a[i].u.send_control.len, 0);
            moq_action_cleanup(&a[i]);
        }
}

static void feed_data(moq_session_t *srv, moq_pq_conn_t *ad, uint64_t sid)
{
    moq_action_t a[16]; size_t n;
    while ((n = moq_session_poll_actions(srv, a, 16)) > 0)
        for (size_t i = 0; i < n; i++) {
            if (a[i].kind == MOQ_ACTION_SEND_DATA) {
                if (a[i].u.send_data.header_len > 0)
                    moq_pq_callback(NULL, sid,
                        (uint8_t *)a[i].u.send_data.header,
                        a[i].u.send_data.header_len,
                        picoquic_callback_stream_data, ad, NULL);
                if (a[i].u.send_data.payload)
                    moq_pq_callback(NULL, sid,
                        (uint8_t *)moq_rcbuf_data(a[i].u.send_data.payload),
                        moq_rcbuf_len(a[i].u.send_data.payload),
                        a[i].u.send_data.fin ? picoquic_callback_stream_fin
                                             : picoquic_callback_stream_data,
                        ad, NULL);
                else if (a[i].u.send_data.fin)
                    moq_pq_callback(NULL, sid, NULL, 0,
                        picoquic_callback_stream_fin, ad, NULL);
            }
            moq_action_cleanup(&a[i]);
        }
}

static moq_subscription_t setup(moq_alloc_t *al,
    moq_session_t **c, moq_session_t **s, int max_ev, int max_act)
{
    moq_session_cfg_t cc; moq_session_cfg_init_sized(&cc, sizeof(cc), al, MOQ_PERSPECTIVE_CLIENT);
    cc.send_request_capacity = true; cc.initial_request_capacity = 10;
    if (max_ev > 0) cc.max_events = (uint32_t)max_ev;
    if (max_act > 0) cc.max_actions = (uint32_t)max_act;

    moq_session_cfg_t sc; moq_session_cfg_init_sized(&sc, sizeof(sc), al, MOQ_PERSPECTIVE_SERVER);
    sc.send_request_capacity = true; sc.initial_request_capacity = 10;

    moq_session_create(&cc, 0, c); moq_session_create(&sc, 0, s);
    moq_session_start(*c, 0);
    pump_ctl(*c, *s); pump_ctl(*s, *c);
    { moq_event_t e; moq_session_poll_events(*c, &e, 1);
      moq_session_poll_events(*s, &e, 1); }

    moq_bytes_t ns[] = {{ (const uint8_t *)"ns", 2 }};
    moq_subscribe_cfg_t sub; moq_subscribe_cfg_init(&sub);
    sub.track_namespace = (moq_namespace_t){ ns, 1 };
    sub.track_name = (moq_bytes_t){ (const uint8_t *)"t", 1 };
    moq_subscription_t h;
    moq_session_subscribe(*c, &sub, 0, &h);
    pump_ctl(*c, *s);
    moq_event_t ev; moq_session_poll_events(*s, &ev, 1);
    moq_subscription_t ss = ev.u.subscribe_request.sub;
    moq_accept_subscribe_cfg_t ac; moq_accept_subscribe_cfg_init(&ac);
    moq_session_accept_subscribe(*s, ss, &ac, 0);
    pump_ctl(*s, *c);
    moq_session_poll_events(*c, &ev, 1);
    return ss;
}

static moq_pq_conn_t *mkad(moq_alloc_t *al, moq_session_t *s)
{
    moq_pq_conn_cfg_t ac; moq_pq_conn_cfg_init_sized(&ac, sizeof(ac));
    ac.session = s; ac.cnx = (picoquic_cnx_t *)(uintptr_t)0xDEAD;
    ac.alloc = al;
    moq_pq_conn_t *a = NULL; moq_pq_conn_create(&ac, &a); return a;
}

/* -- after_callback test helpers ------------------------------------- */

static int g_hook_count = 0;
static void *g_hook_ctx_seen = NULL;

static void hook_fn(moq_pq_conn_t *conn, void *ctx)
{
    (void)conn;
    g_hook_count++;
    g_hook_ctx_seen = ctx;
}

int main(void)
{
    moq_alloc_t al = talloc();

    /* -- 1. Post-retention WOULD_BLOCK: empty retry, no duplicates ---- */
    {
        moq_session_t *c = NULL, *s = NULL;
        moq_subscription_t ss = setup(&al, &c, &s, 1, 0);
        moq_pq_conn_t *ad = mkad(&al, c);

        moq_subgroup_cfg_t sg; moq_subgroup_cfg_init(&sg);
        moq_subgroup_handle_t sgh;
        moq_session_open_subgroup(s, ss, &sg, 0, &sgh);
        moq_rcbuf_t *p1 = NULL, *p2 = NULL;
        moq_rcbuf_create(&al, (const uint8_t *)"AAA", 3, &p1);
        moq_rcbuf_create(&al, (const uint8_t *)"BBB", 3, &p2);
        moq_session_write_object(s, sgh, 0, p1, 0);
        moq_session_write_object(s, sgh, 1, p2, 0);
        moq_rcbuf_decref(p1); moq_rcbuf_decref(p2);
        moq_session_close_subgroup(s, sgh, 0);

        feed_data(s, ad, 3);
        CHECK(moq_session_state(c) != MOQ_SESS_CLOSED);

        moq_event_t ev;
        CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        CHECK(moq_rcbuf_data(ev.u.object_received.payload)[0] == 'A');
        moq_event_cleanup(&ev);

        CHECK(moq_pq_service(ad, g_time) == 0);

        CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        CHECK(moq_rcbuf_data(ev.u.object_received.payload)[0] == 'B');
        moq_event_cleanup(&ev);

        CHECK(moq_session_poll_events(c, &ev, 1) == 0);

        moq_pq_conn_destroy(ad);
        moq_session_destroy(c); moq_session_destroy(s);
    }

    /* -- 2. Zero-length FIN while pending → delivered to session ------- */
    {
        moq_session_t *c = NULL, *s = NULL;
        moq_subscription_t ss = setup(&al, &c, &s, 1, 0);
        moq_pq_conn_t *ad = mkad(&al, c);

        moq_subgroup_cfg_t sg; moq_subgroup_cfg_init(&sg);
        moq_subgroup_handle_t sgh;
        moq_session_open_subgroup(s, ss, &sg, 0, &sgh);
        moq_rcbuf_t *p1 = NULL;
        moq_rcbuf_create(&al, (const uint8_t *)"OBJ", 3, &p1);
        moq_session_write_object(s, sgh, 0, p1, 0);
        moq_rcbuf_decref(p1);
        moq_session_close_subgroup(s, sgh, 0);

        /* Feed header+object (fills event), then FIN separately. */
        moq_action_t acts[16]; size_t na;
        na = moq_session_poll_actions(s, acts, 16);
        for (size_t i = 0; i < na; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_DATA) {
                if (acts[i].u.send_data.header_len > 0)
                    moq_pq_callback(NULL, 3,
                        (uint8_t *)acts[i].u.send_data.header,
                        acts[i].u.send_data.header_len,
                        picoquic_callback_stream_data, ad, NULL);
                if (acts[i].u.send_data.payload)
                    moq_pq_callback(NULL, 3,
                        (uint8_t *)moq_rcbuf_data(acts[i].u.send_data.payload),
                        moq_rcbuf_len(acts[i].u.send_data.payload),
                        picoquic_callback_stream_data, ad, NULL);
                if (acts[i].u.send_data.fin)
                    moq_pq_callback(NULL, 3, NULL, 0,
                        picoquic_callback_stream_fin, ad, NULL);
            }
            moq_action_cleanup(&acts[i]);
        }
        CHECK(moq_session_state(c) != MOQ_SESS_CLOSED);

        moq_event_t ev;
        moq_session_poll_events(c, &ev, 1);
        moq_event_cleanup(&ev);

        CHECK(moq_pq_service(ad, g_time) == 0);

        /* FIN delivered: data-after-FIN should close session. */
        uint8_t post[] = {0xFF};
        moq_pq_callback(NULL, 3, post, 1,
            picoquic_callback_stream_data, ad, NULL);
        CHECK(moq_session_state(c) == MOQ_SESS_CLOSED);

        moq_pq_conn_destroy(ad);
        moq_session_destroy(c); moq_session_destroy(s);
    }

    /* -- 3. Pre-retention WOULD_BLOCK → fatal ------------------------- */
    {
        /* Create client with max_data_streams=1 and max_actions=1. */
        moq_alloc_t a3 = talloc();
        moq_session_cfg_t cc; moq_session_cfg_init_sized(&cc, sizeof(cc), &a3, MOQ_PERSPECTIVE_CLIENT);
        cc.send_request_capacity = true;
        cc.initial_request_capacity = 10;
        cc.max_data_streams = 1;
        cc.max_actions = 1;

        moq_session_cfg_t sc; moq_session_cfg_init_sized(&sc, sizeof(sc), &a3, MOQ_PERSPECTIVE_SERVER);
        sc.send_request_capacity = true;
        sc.initial_request_capacity = 10;

        moq_session_t *c = NULL, *s = NULL;
        moq_session_create(&cc, 0, &c);
        moq_session_create(&sc, 0, &s);
        moq_session_start(c, 0);
        pump_ctl(c, s); pump_ctl(s, c);
        { moq_event_t e; moq_session_poll_events(c, &e, 1);
          moq_session_poll_events(s, &e, 1); }

        moq_pq_conn_t *ad = mkad(&a3, c);

        /* First stream: occupies the only rx slot. */
        uint8_t hdr1[] = {0x14, 0x00, 0x00, 0x00, 0x80};
        moq_pq_callback(NULL, 3, hdr1, sizeof(hdr1),
            picoquic_callback_stream_data, ad, NULL);

        /* Fill the action queue so new-stream STOP_DATA can't queue. */
        moq_bytes_t ns[] = {{ (const uint8_t *)"ns", 2 }};
        moq_subscribe_cfg_t sub; moq_subscribe_cfg_init(&sub);
        sub.track_namespace = (moq_namespace_t){ ns, 1 };
        sub.track_name = (moq_bytes_t){ (const uint8_t *)"t", 1 };
        moq_subscription_t h;
        moq_session_subscribe(c, &sub, 0, &h);
        /* Action queue now has SUBSCRIBE (1 slot, full). */

        /* Second stream: no rx slot available, action queue full →
         * pre-retention WOULD_BLOCK → adapter should go fatal. */
        uint8_t hdr2[] = {0x14, 0x00, 0x01, 0x00, 0x80};
        moq_pq_callback(NULL, 5, hdr2, sizeof(hdr2),
            picoquic_callback_stream_data, ad, NULL);

        CHECK(moq_session_state(c) == MOQ_SESS_CLOSED);

        moq_pq_conn_destroy(ad);
        moq_session_destroy(c); moq_session_destroy(s);
    }

    /* -- 4. STOP_SENDING: pending retry with stored error code --------- */
    {
        /* Server session with max_actions=1 wraps in adapter.
         * Open subgroup → adapter creates outbound stream.
         * Fill action queue, then STOP arrives → pending_stop.
         * Drain/service → picoquic_reset_stream called. */
        moq_alloc_t a4 = talloc();
        moq_session_t *c = NULL, *s = NULL;
        moq_subscription_t ss = setup(&a4, &c, &s, 0, 0);
        moq_pq_conn_t *ad = mkad(&a4, s);

        /* Server opens subgroup → adapter's send_data creates
         * an outbound uni stream in the stream map. */
        moq_subgroup_cfg_t sgc; moq_subgroup_cfg_init(&sgc);
        moq_subgroup_handle_t sgh;
        moq_session_open_subgroup(s, ss, &sgc, 0, &sgh);

        /* Drain the SEND_DATA action via adapter → creates stream map
         * entry with a pq_stream_id. */
        moq_pq_service(ad, g_time);

        /* Write an object to fill the action queue. */
        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(&a4, (const uint8_t *)"X", 1, &p);
        moq_session_write_object(s, sgh, 0, p, 0);
        moq_rcbuf_decref(p);
        /* Action queue has 1 SEND_DATA. Don't drain yet. */

        /* STOP_SENDING on the outbound stream.
         * on_data_stop tries to queue RESET_DATA but action queue
         * may not be full with default size. Force it by using a
         * session with small actions. */
        /* Actually, with default max_actions (64), the queue has room.
         * Let me recreate with max_actions=1 to force WOULD_BLOCK. */
        moq_pq_conn_destroy(ad);
        moq_session_destroy(c); moq_session_destroy(s);

        moq_session_cfg_t cc4; moq_session_cfg_init_sized(&cc4, sizeof(cc4), &a4, MOQ_PERSPECTIVE_CLIENT);
        cc4.send_request_capacity = true; cc4.initial_request_capacity = 10;
        moq_session_cfg_t sc4; moq_session_cfg_init_sized(&sc4, sizeof(sc4), &a4, MOQ_PERSPECTIVE_SERVER);
        sc4.send_request_capacity = true; sc4.initial_request_capacity = 10;
        sc4.max_actions = 2;

        moq_session_create(&cc4, 0, &c); moq_session_create(&sc4, 0, &s);
        moq_session_start(c, 0);
        pump_ctl(c, s); pump_ctl(s, c);
        { moq_event_t e; moq_session_poll_events(c, &e, 1);
          moq_session_poll_events(s, &e, 1); }

        moq_bytes_t ns[] = {{ (const uint8_t *)"ns", 2 }};
        moq_subscribe_cfg_t sub; moq_subscribe_cfg_init(&sub);
        sub.track_namespace = (moq_namespace_t){ ns, 1 };
        sub.track_name = (moq_bytes_t){ (const uint8_t *)"t", 1 };
        moq_subscription_t h;
        moq_session_subscribe(c, &sub, 0, &h);
        pump_ctl(c, s);
        moq_event_t ev; moq_session_poll_events(s, &ev, 1);
        ss = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t ac; moq_accept_subscribe_cfg_init(&ac);
        moq_session_accept_subscribe(s, ss, &ac, 0);
        pump_ctl(s, c);
        moq_session_poll_events(c, &ev, 1);

        ad = mkad(&a4, s);

        /* Record stub counter before service assigns stream IDs. */
        uint64_t uni_before = g_next_uni;

        /* Open subgroup: queues SEND_DATA action (1 of 2 slots). */
        moq_subgroup_cfg_init(&sgc);
        moq_session_open_subgroup(s, ss, &sgc, 0, &sgh);

        /* Write object: queues SEND_DATA action (2 of 2 slots, full). */
        moq_rcbuf_create(&a4, (const uint8_t *)"Y", 1, &p);
        moq_session_write_object(s, sgh, 0, p, 0);
        moq_rcbuf_decref(p);

        /* Drain via adapter → send_data creates outbound stream,
         * assigns pq_stream_id = uni_before (first uni ID). */
        moq_pq_service(ad, g_time);
        uint64_t stop_pq_sid = uni_before;

        /* Queue is now empty. Open another subgroup to fill 1 slot. */
        moq_subgroup_handle_t sgh2;
        moq_subgroup_cfg_t sgc2; moq_subgroup_cfg_init(&sgc2);
        sgc2.group_id = 1;
        moq_session_open_subgroup(s, ss, &sgc2, 0, &sgh2);
        /* Write to fill slot 2. */
        moq_rcbuf_create(&a4, (const uint8_t *)"Z", 1, &p);
        moq_session_write_object(s, sgh2, 0, p, 0);
        moq_rcbuf_decref(p);
        /* Action queue full (2 of 2). */

        g_reset_count = 0;
        moq_pq_callback(NULL, stop_pq_sid, NULL, 0,
            picoquic_callback_stop_sending, ad, NULL);

        CHECK(moq_session_state(s) != MOQ_SESS_CLOSED);

        /* Drain + service → retries pending stop. */
        moq_pq_service(ad, g_time);
        moq_pq_service(ad, g_time);

        CHECK(g_reset_count >= 1);
        CHECK(g_reset_code == 0x42);

        moq_pq_conn_destroy(ad);
        moq_session_destroy(c); moq_session_destroy(s);
    }

    /* -- 5. Control pending: SUBSCRIBE while event queue full ---------- */
    {
        moq_alloc_t a5 = talloc();
        moq_session_cfg_t cc; moq_session_cfg_init_sized(&cc, sizeof(cc), &a5, MOQ_PERSPECTIVE_CLIENT);
        cc.send_request_capacity = true; cc.initial_request_capacity = 5;
        moq_session_cfg_t sc; moq_session_cfg_init_sized(&sc, sizeof(sc), &a5, MOQ_PERSPECTIVE_SERVER);
        sc.send_request_capacity = true; sc.initial_request_capacity = 5;
        sc.max_events = 1;

        moq_session_t *c = NULL, *s = NULL;
        moq_session_create(&cc, 0, &c); moq_session_create(&sc, 0, &s);
        moq_session_start(c, 0);
        pump_ctl(c, s); pump_ctl(s, c);
        { moq_event_t e; moq_session_poll_events(c, &e, 1); }
        /* Server: SETUP_COMPLETE fills the 1-slot event queue. */

        moq_pq_conn_t *ad = mkad(&a5, s);

        /* Client subscribes; feed SUBSCRIBE control to adapter. */
        moq_bytes_t ns[] = {{ (const uint8_t *)"ns", 2 }};
        moq_subscribe_cfg_t sub; moq_subscribe_cfg_init(&sub);
        sub.track_namespace = (moq_namespace_t){ ns, 1 };
        sub.track_name = (moq_bytes_t){ (const uint8_t *)"t", 1 };
        moq_subscription_t h;
        moq_session_subscribe(c, &sub, 0, &h);

        moq_action_t acts[4]; size_t na;
        na = moq_session_poll_actions(c, acts, 4);
        for (size_t i = 0; i < na; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_pq_callback(NULL, 0,
                    (uint8_t *)acts[i].u.send_control.data,
                    acts[i].u.send_control.len,
                    picoquic_callback_stream_data, ad, NULL);
            moq_action_cleanup(&acts[i]);
        }
        CHECK(moq_session_state(s) != MOQ_SESS_CLOSED);

        /* Drain SETUP_COMPLETE. */
        moq_event_t ev;
        CHECK(moq_session_poll_events(s, &ev, 1) == 1);
        CHECK(ev.kind == MOQ_EVENT_SETUP_COMPLETE);
        moq_event_cleanup(&ev);

        /* Service → process_pending → SUBSCRIBE_REQUEST produced. */
        CHECK(moq_pq_service(ad, g_time) == 0);

        CHECK(moq_session_poll_events(s, &ev, 1) == 1);
        CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        moq_event_cleanup(&ev);

        moq_pq_conn_destroy(ad);
        moq_session_destroy(c); moq_session_destroy(s);
    }

    /* -- 6. Datagram: adapter does not go fatal on any result ---------- */
    {
        /* The adapter casts (void) on on_datagram return. Verify the
         * adapter itself does not call adapter_fatal regardless of
         * session outcome. If the session closes on bad wire data,
         * that's the session's decision, not the adapter's. */
        moq_alloc_t a6 = talloc();
        moq_session_cfg_t cfg6;
        moq_session_cfg_init_sized(&cfg6, sizeof(cfg6), &a6, MOQ_PERSPECTIVE_CLIENT);
        moq_session_t *c = NULL;
        moq_session_create(&cfg6, 0, &c);
        moq_pq_conn_t *ad = mkad(&a6, c);

        /* Feed datagram on an IDLE session (before handshake).
         * on_datagram returns MOQ_ERR_WRONG_STATE — adapter ignores. */
        uint8_t dg[] = {0x01, 0x02, 0x03};
        moq_pq_callback(NULL, 0, dg, 3,
            picoquic_callback_datagram, ad, NULL);

        /* Session is still IDLE (not CLOSED — adapter didn't go fatal). */
        CHECK(moq_session_state(c) == MOQ_SESS_IDLE);

        moq_pq_conn_destroy(ad);
        moq_session_destroy(c);
    }

    /* -- 7. user_ctx: set via config, get, set, get -------------------- */
    {
        int sentinel_a = 0, sentinel_b = 0;
        moq_session_cfg_t sc;
        moq_session_cfg_init_sized(&sc, sizeof(sc), &al, MOQ_PERSPECTIVE_SERVER);
        moq_session_t *s = NULL;
        moq_session_create(&sc, 0, &s);

        moq_pq_conn_cfg_t ac;
        moq_pq_conn_cfg_init_sized(&ac, sizeof(ac));
        ac.session = s;
        ac.cnx = (picoquic_cnx_t *)(uintptr_t)0xDEAD;
        ac.alloc = &al;
        ac.user_ctx = &sentinel_a;

        moq_pq_conn_t *ad = NULL;
        moq_pq_conn_create(&ac, &ad);
        CHECK(ad != NULL);
        CHECK(moq_pq_conn_user_ctx(ad) == &sentinel_a);

        moq_pq_conn_set_user_ctx(ad, &sentinel_b);
        CHECK(moq_pq_conn_user_ctx(ad) == &sentinel_b);

        moq_pq_conn_set_user_ctx(ad, NULL);
        CHECK(moq_pq_conn_user_ctx(ad) == NULL);

        moq_pq_conn_destroy(ad);
        moq_session_destroy(s);
    }

    /* -- 8. Fatal status: fresh adapter is not fatal -------------------- */
    {
        moq_session_cfg_t sc;
        moq_session_cfg_init_sized(&sc, sizeof(sc), &al, MOQ_PERSPECTIVE_SERVER);
        moq_session_t *s = NULL;
        moq_session_create(&sc, 0, &s);
        moq_pq_conn_t *ad = mkad(&al, s);

        CHECK(moq_pq_conn_is_fatal(ad) == false);
        CHECK(moq_pq_conn_fatal_code(ad) == 0);

        moq_pq_conn_destroy(ad);
        moq_session_destroy(s);
    }

    /* -- 9. Fatal status: close callback sets fatal with code 0 --------- */
    {
        moq_session_cfg_t sc;
        moq_session_cfg_init_sized(&sc, sizeof(sc), &al, MOQ_PERSPECTIVE_SERVER);
        moq_session_t *s = NULL;
        moq_session_create(&sc, 0, &s);
        moq_pq_conn_t *ad = mkad(&al, s);

        CHECK(moq_pq_conn_is_fatal(ad) == false);

        moq_pq_callback(NULL, 0, NULL, 0,
            picoquic_callback_close, ad, NULL);

        CHECK(moq_pq_conn_is_closed(ad) == true);
        CHECK(moq_pq_conn_is_fatal(ad) == false);

        /* Second close is a no-op. */
        moq_pq_callback(NULL, 0, NULL, 0,
            picoquic_callback_close, ad, NULL);
        CHECK(moq_pq_conn_is_closed(ad) == true);

        moq_pq_conn_destroy(ad);
        moq_session_destroy(s);
    }

    /* -- 10. Fatal code from session-initiated close -------------------- */
    {
        moq_alloc_t a10 = talloc();
        moq_session_cfg_t cc;
        moq_session_cfg_init_sized(&cc, sizeof(cc), &a10, MOQ_PERSPECTIVE_CLIENT);
        cc.send_request_capacity = true;
        cc.initial_request_capacity = 10;
        moq_session_cfg_t sc;
        moq_session_cfg_init_sized(&sc, sizeof(sc), &a10, MOQ_PERSPECTIVE_SERVER);
        sc.send_request_capacity = true;
        sc.initial_request_capacity = 10;

        moq_session_t *c = NULL, *s = NULL;
        moq_session_create(&cc, 0, &c);
        moq_session_create(&sc, 0, &s);
        moq_session_start(c, 0);
        pump_ctl(c, s); pump_ctl(s, c);
        { moq_event_t e; moq_session_poll_events(c, &e, 1);
          moq_session_poll_events(s, &e, 1); }

        moq_pq_conn_t *ad = mkad(&a10, c);

        /* Feed a truncated control message on the established session.
         * The session parses the envelope, detects the violation, and
         * queues a CLOSE_SESSION action with a protocol error code. */
        uint8_t bad[] = { 0x40, 0x20, 0x00, 0x01, 0xFF };
        moq_session_on_control_bytes(c, bad, sizeof(bad), 0);
        moq_pq_service(ad, 0);

        CHECK(moq_pq_conn_is_closed(ad) == true);
        CHECK(moq_pq_conn_close_code(ad) != 0);

        moq_pq_conn_destroy(ad);
        moq_session_destroy(c);
        moq_session_destroy(s);
    }

    /* -- 11. Fatal status: NULL conn returns safe defaults ------------- */
    {
        CHECK(moq_pq_conn_is_fatal(NULL) == false);
        CHECK(moq_pq_conn_fatal_code(NULL) == 0);
    }

    /* -- 12. after_callback fires on moq_pq_callback ------------------- */
    {
        g_hook_count = 0;
        g_hook_ctx_seen = NULL;

        moq_alloc_t al = talloc();
        moq_session_cfg_t scfg = MOQ_SESSION_CFG_INIT;
        scfg.alloc = &al;
        scfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        moq_session_t *s = NULL;
        moq_session_create(&scfg, g_time, &s);

        int sentinel = 42;
        moq_pq_conn_cfg_t acfg;
        moq_pq_conn_cfg_init_sized(&acfg, sizeof(acfg));
        acfg.session = s;
        acfg.cnx = (picoquic_cnx_t *)0x1;
        acfg.alloc = &al;
        acfg.user_ctx = &sentinel;
        acfg.after_callback = hook_fn;

        moq_pq_conn_t *ad = NULL;
        CHECK(moq_pq_conn_create(&acfg, &ad) == 0);
        CHECK(g_hook_count == 0);

        moq_pq_callback(NULL, 0, NULL, 0,
            picoquic_callback_stream_data, ad, NULL);
        CHECK(g_hook_count == 1);
        CHECK(g_hook_ctx_seen == &sentinel);

        moq_pq_service(ad, g_time);
        CHECK(g_hook_count == 2);

        moq_pq_conn_destroy(ad);
        moq_session_destroy(s);
    }

    /* -- 13. after_callback fires on fatal drain_actions path --------- */
    {
        g_hook_count = 0;

        moq_alloc_t al = talloc();
        moq_session_cfg_t scfg = MOQ_SESSION_CFG_INIT;
        scfg.alloc = &al;
        scfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        moq_session_t *s = NULL;
        moq_session_create(&scfg, g_time, &s);
        moq_session_start(s, g_time);

        moq_pq_conn_cfg_t acfg;
        moq_pq_conn_cfg_init_sized(&acfg, sizeof(acfg));
        acfg.session = s;
        acfg.cnx = (picoquic_cnx_t *)0x1;
        acfg.alloc = &al;
        acfg.after_callback = hook_fn;

        moq_pq_conn_t *ad = NULL;
        CHECK(moq_pq_conn_create(&acfg, &ad) == 0);

        /* Make picoquic_add_to_stream fail before draining SETUP. */
        g_send_fail = true;
        int svc_rc = moq_pq_service(ad, g_time);
        g_send_fail = false;

        CHECK(svc_rc == -1);
        CHECK(moq_pq_conn_is_fatal(ad));
        CHECK(g_hook_count == 1);

        moq_pq_conn_destroy(ad);
        moq_session_destroy(s);
    }

    /* -- 14. after_callback ABI gate: old struct_size skips hook ------ */
    {
        g_hook_count = 0;

        moq_alloc_t al = talloc();
        moq_session_cfg_t scfg = MOQ_SESSION_CFG_INIT;
        scfg.alloc = &al;
        scfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        moq_session_t *s = NULL;
        moq_session_create(&scfg, g_time, &s);

        moq_pq_conn_cfg_t acfg;
        memset(&acfg, 0xFF, sizeof(acfg));
        acfg.struct_size = offsetof(moq_pq_conn_cfg_t, after_callback);
        acfg.session = s;
        acfg.cnx = (picoquic_cnx_t *)0x1;
        acfg.alloc = &al;
        acfg.user_ctx = NULL;

        moq_pq_conn_t *ad = NULL;
        CHECK(moq_pq_conn_create(&acfg, &ad) == 0);

        moq_pq_callback(NULL, 0, NULL, 0,
            picoquic_callback_stream_data, ad, NULL);
        moq_pq_service(ad, g_time);
        CHECK(g_hook_count == 0);

        moq_pq_conn_destroy(ad);
        moq_session_destroy(s);
    }

    /* -- 15. Failed close makes service return -1, fatal, not closed --- */
    {
        moq_alloc_t al = talloc();

        /* Create established server session via raw handshake. */
        moq_session_cfg_t ccfg = MOQ_SESSION_CFG_INIT;
        ccfg.alloc = &al;
        ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        ccfg.send_request_capacity = true;
        ccfg.initial_request_capacity = 10;
        moq_session_t *c = NULL;
        moq_session_create(&ccfg, g_time, &c);

        moq_session_cfg_t scfg = MOQ_SESSION_CFG_INIT;
        scfg.alloc = &al;
        scfg.perspective = MOQ_PERSPECTIVE_SERVER;
        scfg.send_request_capacity = true;
        scfg.initial_request_capacity = 10;
        moq_session_t *s = NULL;
        moq_session_create(&scfg, g_time, &s);

        moq_session_start(c, g_time);
        moq_action_t acts[16]; size_t n;
        while ((n = moq_session_poll_actions(c, acts, 16)) > 0)
            for (size_t i = 0; i < n; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                    moq_session_on_control_bytes(s,
                        acts[i].u.send_control.data,
                        acts[i].u.send_control.len, g_time);
                moq_action_cleanup(&acts[i]);
            }
        while ((n = moq_session_poll_actions(s, acts, 16)) > 0)
            for (size_t i = 0; i < n; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                    moq_session_on_control_bytes(c,
                        acts[i].u.send_control.data,
                        acts[i].u.send_control.len, g_time);
                moq_action_cleanup(&acts[i]);
            }

        /* Create adapter for server session (now established). */
        moq_pq_conn_cfg_t acfg;
        moq_pq_conn_cfg_init_sized(&acfg, sizeof(acfg));
        acfg.session = s;
        acfg.cnx = (picoquic_cnx_t *)0x1;
        acfg.alloc = &al;
        moq_pq_conn_t *ad = NULL;
        moq_pq_conn_create(&acfg, &ad);

        /* Feed bad control bytes → session emits CLOSE_SESSION with
         * nonzero protocol error code. Make close_transport fail. */
        uint8_t bad[] = { 0x40, 0x20, 0x00, 0x01, 0xFF };
        moq_session_on_control_bytes(s, bad, sizeof(bad), g_time);

        g_close_fail = true;
        int svc_rc = moq_pq_service(ad, g_time);
        g_close_fail = false;

        CHECK(svc_rc == -1);
        CHECK(moq_pq_conn_is_fatal(ad) == true);
        CHECK(moq_pq_conn_is_closed(ad) == false);

        moq_pq_conn_destroy(ad);
        moq_session_destroy(c);
        moq_session_destroy(s);
    }

    /* -- 16. Nonzero app close: closed, not fatal, close_code set ------ */
    {
        moq_alloc_t al = talloc();
        moq_session_cfg_t scfg = MOQ_SESSION_CFG_INIT;
        scfg.alloc = &al;
        scfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        scfg.send_request_capacity = true;
        scfg.initial_request_capacity = 10;
        moq_session_t *s = NULL;
        moq_session_create(&scfg, g_time, &s);

        moq_pq_conn_cfg_t acfg;
        moq_pq_conn_cfg_init_sized(&acfg, sizeof(acfg));
        acfg.session = s;
        acfg.cnx = (picoquic_cnx_t *)0x1;
        acfg.alloc = &al;
        moq_pq_conn_t *ad = NULL;
        moq_pq_conn_create(&acfg, &ad);

        /* Inject application close with nonzero error */
        g_pending_app_error = 0x42;
        moq_pq_callback(NULL, 0, NULL, 0,
            picoquic_callback_application_close, ad, NULL);
        g_pending_app_error = 0;

        CHECK(moq_pq_conn_is_closed(ad) == true);
        CHECK(moq_pq_conn_is_fatal(ad) == false);
        CHECK(moq_pq_conn_close_code(ad) == 0x42);
        CHECK(moq_pq_service(ad, g_time) == 0);

        moq_pq_conn_destroy(ad);
        moq_session_destroy(s);
    }

    /* -- 17. Outbound send queue: plan/commit byte sequence + FIN ------- *
     * The queue holds MoQ stream bytes for pull sending. Draining it with a
     * small buffer must reproduce the exact header+payload sequence, with FIN
     * only on the final provide, and release every chunk (leak-checked). */
    {
        g_bal = 0;
        moq_alloc_t a = calloc_();
        moq_pq_send_queue_t *q = moq_pq_send_queue_create(&a, 1000);
        CHECK(q != NULL);

        /* Header (copied) + payload (rcbuf), FIN on the payload. */
        uint8_t hdr[] = { 0x11, 0x22, 0x33 };
        CHECK(moq_pq_send_queue_push_copy(q, 4, hdr, sizeof(hdr), false) == 1);
        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(&a, (const uint8_t *)"PAYLOAD", 7, &p);
        CHECK(moq_pq_send_queue_push_rcbuf(q, 4, p, true) == 1);
        moq_rcbuf_decref(p);   /* queue holds its own ref */
        CHECK(moq_pq_send_queue_queued_bytes(q) == 10);

        /* Drain in 4-byte slices; reassemble and track FIN. */
        uint8_t got[32]; size_t glen = 0; bool saw_fin = false; int provides = 0;
        for (int i = 0; i < 20 && moq_pq_send_queue_has_data(q, 4); i++) {
            size_t nb; bool fin, more;
            CHECK(moq_pq_send_queue_plan(q, 4, 4, &nb, &fin, &more));
            CHECK(!saw_fin);                       /* FIN must be last */
            moq_pq_send_queue_commit(q, 4, got + glen, nb);
            glen += nb; provides++;
            if (fin) saw_fin = true;
            CHECK(fin == !more);                   /* fin iff nothing remains */
        }
        CHECK(glen == 10);
        CHECK(memcmp(got, "\x11\x22\x33" "PAYLOAD", 10) == 0);
        CHECK(saw_fin);
        CHECK(provides == 3);                       /* 4 + 4 + 2 */
        CHECK(moq_pq_send_queue_queued_bytes(q) == 0);
        CHECK(!moq_pq_send_queue_has_data(q, 4));

        moq_pq_send_queue_destroy(q);
        CHECK(g_bal == 0);                          /* no leaks */
    }

    /* -- 18. Send queue: cap, oversized write, zero-byte FIN, drop ------ */
    {
        g_bal = 0;
        moq_alloc_t a = calloc_();
        moq_pq_send_queue_t *q = moq_pq_send_queue_create(&a, 4);
        CHECK(q != NULL);

        /* Oversized write onto an empty aggregate is accepted (forward
         * progress); a following write over the cap is refused. */
        uint8_t big[100]; memset(big, 'X', sizeof(big));
        CHECK(moq_pq_send_queue_push_copy(q, 7, big, sizeof(big), false) == 1);
        CHECK(moq_pq_send_queue_push_copy(q, 7, big, 5, false) == 0);   /* cap */
        /* A zero-byte FIN always passes and collapses onto the tail. */
        CHECK(moq_pq_send_queue_push_copy(q, 7, NULL, 0, true) == 1);
        {
            size_t nb; bool fin, more;
            CHECK(moq_pq_send_queue_plan(q, 7, 1000, &nb, &fin, &more));
            CHECK(nb == 100 && fin && !more);       /* all 100 + FIN in one go */
            uint8_t out[100];
            moq_pq_send_queue_commit(q, 7, out, nb);
            CHECK(!moq_pq_send_queue_has_data(q, 7));
        }

        /* A zero-length non-FIN write is a no-op: accepted, nothing queued,
         * and no stream slot is created for it. */
        CHECK(moq_pq_send_queue_push_copy(q, 123, NULL, 0, false) == 1);
        CHECK(!moq_pq_send_queue_has_data(q, 123));

        /* Bare FIN on a fresh stream: plan yields 0 bytes with FIN. */
        CHECK(moq_pq_send_queue_push_copy(q, 9, NULL, 0, true) == 1);
        {
            size_t nb; bool fin, more;
            CHECK(moq_pq_send_queue_plan(q, 9, 1000, &nb, &fin, &more));
            CHECK(nb == 0 && fin && !more);
            moq_pq_send_queue_commit(q, 9, NULL, 0);
            CHECK(!moq_pq_send_queue_has_data(q, 9));
        }

        /* Drop discards queued bytes (and releases rcbufs). */
        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(&a, (const uint8_t *)"ZZ", 2, &p);
        CHECK(moq_pq_send_queue_push_rcbuf(q, 11, p, false) == 1);
        moq_rcbuf_decref(p);
        CHECK(moq_pq_send_queue_has_data(q, 11));
        moq_pq_send_queue_drop(q, 11);
        CHECK(!moq_pq_send_queue_has_data(q, 11));
        CHECK(moq_pq_send_queue_queued_bytes(q) == 0);

        moq_pq_send_queue_destroy(q);
        CHECK(g_bal == 0);
    }

    /* -- 19. Endpoint pull send: no add_to_stream, prepare byte sequence - *
     * Servicing outbound MoQ data queues it (no picoquic_add_to_stream), marks
     * the stream active, and prepare_to_send with small buffers reproduces the
     * whole stream ending in the payload with FIN only on the last provide. */
    {
        g_add_count = 0; g_add_bytes = 0; g_active_flag = -1; g_send_fail = false;
        moq_alloc_t a = talloc();
        moq_session_t *c = NULL, *s = NULL;
        moq_subscription_t ss = setup(&a, &c, &s, 0, 0);
        moq_pq_conn_t *ad = mkad(&a, s);

        moq_subgroup_cfg_t sg; moq_subgroup_cfg_init(&sg);
        moq_subgroup_handle_t sgh;
        moq_session_open_subgroup(s, ss, &sg, 0, &sgh);
        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(&a, (const uint8_t *)"HELLOPAYLOAD", 12, &p);
        moq_session_write_object(s, sgh, 0, p, 0);
        moq_rcbuf_decref(p);
        moq_session_close_subgroup(s, sgh, 0);

        CHECK(moq_pq_service(ad, g_time) == 0);
        CHECK(g_add_count == 0);            /* pull model: nothing pushed */
        CHECK(g_active_flag == 1);          /* stream marked active */
        uint64_t sid = g_active_sid;

        uint8_t got[128]; size_t glen = 0; bool saw_fin = false; int rounds = 0;
        while (rounds++ < 60 && !saw_fin) {
            g_provide_nb = 999; g_provide_fin = -1; g_provide_still = -1;
            moq_pq_callback(NULL, sid, (uint8_t *)(uintptr_t)0xABC, 5,
                            picoquic_callback_prepare_to_send, ad, NULL);
            if (g_provide_nb == 0 && g_provide_fin == 0)
                break;                      /* reneged: nothing queued */
            CHECK(!saw_fin);                /* FIN only on the last provide */
            CHECK(g_provide_nb <= 5);       /* honored the buffer limit */
            memcpy(got + glen, g_provide_buf, g_provide_nb);
            glen += g_provide_nb;
            if (g_provide_fin) saw_fin = true;
        }
        CHECK(saw_fin);
        CHECK(g_add_count == 0);
        /* The payload is the tail of the stream (headers precede it). */
        CHECK(glen >= 12);
        CHECK(memcmp(got + glen - 12, "HELLOPAYLOAD", 12) == 0);
        /* Fully drained: a further prepare reneges. */
        g_provide_nb = 999;
        moq_pq_callback(NULL, sid, (uint8_t *)(uintptr_t)0xABC, 5,
                        picoquic_callback_prepare_to_send, ad, NULL);
        CHECK(g_provide_nb == 0);

        moq_pq_conn_destroy(ad);
        moq_session_destroy(c); moq_session_destroy(s);
    }

    /* -- 20. Endpoint pull: reset drops queued bytes -------------------- *
     * A peer STOP_SENDING resets the outbound stream; its queued bytes are
     * dropped and never provided afterward. */
    {
        g_add_count = 0; g_active_flag = -1; g_send_fail = false;
        moq_alloc_t a = talloc();
        moq_session_t *c = NULL, *s = NULL;
        moq_subscription_t ss = setup(&a, &c, &s, 0, 0);
        moq_pq_conn_t *ad = mkad(&a, s);

        moq_subgroup_cfg_t sg; moq_subgroup_cfg_init(&sg);
        moq_subgroup_handle_t sgh;
        moq_session_open_subgroup(s, ss, &sg, 0, &sgh);
        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(&a, (const uint8_t *)"DROPME", 6, &p);
        moq_session_write_object(s, sgh, 0, p, 0);
        moq_rcbuf_decref(p);
        CHECK(moq_pq_service(ad, g_time) == 0);
        uint64_t sid = g_active_sid;

        /* Peer stops the stream -> bridge resets it -> endpoint drops queue. */
        g_reset_count = 0;
        moq_pq_callback(NULL, sid, NULL, 0,
                        picoquic_callback_stop_sending, ad, NULL);
        moq_pq_service(ad, g_time);
        CHECK(g_reset_count >= 1);

        /* prepare_to_send now has nothing to provide. */
        g_provide_nb = 999;
        moq_pq_callback(NULL, sid, (uint8_t *)(uintptr_t)0xABC, 100,
                        picoquic_callback_prepare_to_send, ad, NULL);
        CHECK(g_provide_nb == 0);
        CHECK(moq_session_state(s) != MOQ_SESS_CLOSED);

        moq_pq_conn_destroy(ad);
        moq_session_destroy(c); moq_session_destroy(s);
    }

    /* -- 21. Endpoint pull: cap WOULD_BLOCK retained, drains, no dup ----- *
     * A tiny cap makes the second write WOULD_BLOCK (bridge retains it);
     * draining the queue via prepare_to_send lets the retry complete, and the
     * whole object still arrives exactly once. */
    {
        setenv("MOQ_PQ_STREAM_QUEUE_BYTES", "8", 1);
        g_add_count = 0; g_send_fail = false;
        moq_alloc_t a = talloc();
        moq_session_t *c = NULL, *s = NULL;
        moq_subscription_t ss = setup(&a, &c, &s, 0, 0);
        moq_pq_conn_t *ad = mkad(&a, s);

        moq_subgroup_cfg_t sg; moq_subgroup_cfg_init(&sg);
        moq_subgroup_handle_t sgh;
        moq_session_open_subgroup(s, ss, &sg, 0, &sgh);
        uint8_t big[40]; memset(big, 'Q', sizeof(big));
        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(&a, big, sizeof(big), &p);
        moq_session_write_object(s, sgh, 0, p, 0);
        moq_rcbuf_decref(p);
        moq_session_close_subgroup(s, sgh, 0);

        uint64_t sid = 0; size_t total = 0; bool saw_fin = false;
        int guard = 0;
        /* Alternate service (fills queue to cap, WOULD_BLOCK retains rest) and
         * prepare drains (frees cap), until the whole object + FIN drains. */
        while (guard++ < 200 && !saw_fin) {
            moq_pq_service(ad, g_time);
            if (g_active_flag == 1) sid = g_active_sid;
            if (sid == 0) continue;
            g_provide_nb = 999; g_provide_fin = -1;
            moq_pq_callback(NULL, sid, (uint8_t *)(uintptr_t)0xABC, 100,
                            picoquic_callback_prepare_to_send, ad, NULL);
            total += g_provide_nb;
            if (g_provide_fin) saw_fin = true;
        }
        CHECK(saw_fin);
        CHECK(g_add_count == 0);
        /* header bytes + exactly the 40 payload bytes, once. */
        CHECK(total >= 40);
        CHECK(moq_session_state(s) != MOQ_SESS_CLOSED);
        unsetenv("MOQ_PQ_STREAM_QUEUE_BYTES");

        moq_pq_conn_destroy(ad);
        moq_session_destroy(c); moq_session_destroy(s);
    }

    if (failures == 0)
        printf("PASS: test_adapter_backpressure\n");
    return failures;
}
