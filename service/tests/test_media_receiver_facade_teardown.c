/*
 * The receiver's subscriber facade holds payloads it took from the session
 * before receiver intake: its object queue and its pending slot. A session
 * shares a whole created input instead of copying it only when the input
 * comes from the session's own allocator -- in a service session, the
 * endpoint's, which the endpoint contract requires to be valid on any thread.
 * An input from any other allocator (here one whose blocks may be released
 * only on the network thread) is copied, so the facade never holds it.
 *
 * Destroying the receiver on the application thread must then leave the
 * facade's buffers to the network thread while that thread runs -- also when
 * every allocation destroy could make is refused -- and, once stop() has
 * joined the network thread, the facade is destroyed by stop()'s terminal
 * drain on stop's caller, or (when the endpoint was already stopped) by the
 * destroying thread: in both cases only buffers from the endpoint's
 * allocator are released there.
 *
 * White-box: the real receiver (media_receiver.c is included), a bare
 * endpoint with a live post queue and a stand-in managed context -- a real
 * thread, joined by the endpoint's real stop() -- a scripted draft-16 peer,
 * and the real moq_media_receiver_destroy() on the application (main) thread.
 * On the network thread, 65 objects arrive on one subgroup stream as the
 * transport's own created rcbufs; the subscriber takes them in (64 queued,
 * the 65th in its pending slot) and intake never runs. The transport keeps
 * referencing the odd-numbered inputs and releases them later on its thread.
 *
 * Rows (the input allocator; how the facade's teardown runs):
 *   live_trusted, refuse_receiver, refuse_endpoint: the session's allocator;
 *       the posted task runs on the live network thread (in the last two,
 *       every request to that allocator is refused while destroy runs);
 *   drain_trusted: the session's allocator; the posted task runs in stop()'s
 *       terminal drain, on stop's caller, after the join;
 *   refused_trusted: the session's allocator; the endpoint was stopped first,
 *       so the post is refused and the destroying thread tears down;
 *   drain_affine, refused_affine: an allocator confined to the network thread
 *       (a release anywhere else is a violation); the session copied every
 *       input, so the facade holds only the endpoint allocator's copies.
 * And construction: when either reservation (the teardown context, the task
 * node) is refused, attach fails with no hook attached and nothing kept.
 */
#define MOQ_MEDIA_RECEIVER_TESTING 1
#include "../src/media_receiver.c"
#include <moq/sim.h>
#include <moq/control.h>
#include <moq/codec.h>
#include "test_support.h"

#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static int failures;

moq_endpoint_t *moq_endpoint_test_make_bare_alloc(const moq_alloc_t *alloc);
void moq_endpoint_test_free_bare(moq_endpoint_t *ep);
void moq_endpoint_test_drain_posted(moq_endpoint_t *ep, moq_session_t *session,
                                    uint64_t now_us);
size_t moq_endpoint_test_task_count(moq_endpoint_t *ep);
int moq_endpoint_test_attachments(moq_endpoint_t *ep);
void moq_endpoint_test_set_join_facade(moq_endpoint_t *ep,
                                       moq_result_t (*stop_fn)(void *), void *ctx);

#define CHECKED(call) do { int ck_e_ = (call); if (ck_e_ != 0) { \
    fprintf(stderr, "FATAL: %s:%d: %s = %d\n", __FILE__, __LINE__, #call, ck_e_); \
    abort(); } } while (0)

/* -- A thread-safe counting allocator: refuses every request, or the n-th
 *    request after arming ---------------------------------------------- */

typedef struct {
    pthread_mutex_t mu;
    int64_t         balance;
    uint64_t        requests;
    bool            refuse_all;
    uint64_t        refuse_nth;   /* 0: off; else refuse when it counts down to 1 */
    uint64_t        refused;
} cnt_alloc_t;

static void *cnt_alloc_fn(size_t n, void *ctx)
{
    cnt_alloc_t *a = ctx;
    CHECKED(pthread_mutex_lock(&a->mu));
    a->requests++;
    bool refuse = a->refuse_all;
    if (a->refuse_nth) {
        if (a->refuse_nth == 1) refuse = true;
        a->refuse_nth--;
    }
    if (refuse) {
        a->refused++;
        CHECKED(pthread_mutex_unlock(&a->mu));
        return NULL;
    }
    void *p = malloc(n);
    if (p) a->balance++;
    CHECKED(pthread_mutex_unlock(&a->mu));
    return p;
}
static void *cnt_realloc_fn(void *p, size_t o, size_t n, void *ctx)
{
    (void)o; (void)ctx;
    return realloc(p, n);
}
static void cnt_free_fn(void *p, size_t n, void *ctx)
{
    cnt_alloc_t *a = ctx;
    (void)n;
    if (!p) return;
    CHECKED(pthread_mutex_lock(&a->mu));
    a->balance--;
    CHECKED(pthread_mutex_unlock(&a->mu));
    free(p);
}
static void cnt_init(cnt_alloc_t *a)
{
    memset(a, 0, sizeof(*a));
    CHECKED(pthread_mutex_init(&a->mu, NULL));
}
static moq_alloc_t cnt_allocator(cnt_alloc_t *a)
{
    return (moq_alloc_t){ a, cnt_alloc_fn, cnt_realloc_fn, cnt_free_fn };
}
static void cnt_arm(cnt_alloc_t *a, bool refuse_all, uint64_t nth)
{
    CHECKED(pthread_mutex_lock(&a->mu));
    a->refuse_all = refuse_all;
    a->refuse_nth = nth;
    CHECKED(pthread_mutex_unlock(&a->mu));
}
static uint64_t cnt_requests(cnt_alloc_t *a)
{
    CHECKED(pthread_mutex_lock(&a->mu));
    uint64_t n = a->requests;
    CHECKED(pthread_mutex_unlock(&a->mu));
    return n;
}
static int64_t cnt_balance(cnt_alloc_t *a)
{
    CHECKED(pthread_mutex_lock(&a->mu));
    int64_t b = a->balance;
    CHECKED(pthread_mutex_unlock(&a->mu));
    return b;
}

/* -- The transport's input allocator: records which thread frees each
 *    watched block; when confined, a release off its owner thread is a
 *    violation (counted, so the run reports rather than crashes) -------- */

#define INPUTS (RECEIVER_SUB_MAX_OBJECTS + 1u)
typedef struct {
    pthread_mutex_t mu;
    bool            confined;
    pthread_t       owner;
    int             violations;
    int64_t         balance;
    void           *watch[INPUTS];
    int             freed[INPUTS];
    pthread_t       freed_by[INPUTS];
} in_alloc_t;

static void *in_alloc_fn(size_t n, void *ctx)
{
    in_alloc_t *a = ctx;
    void *p = malloc(n);
    CHECKED(pthread_mutex_lock(&a->mu));
    if (a->confined && !pthread_equal(a->owner, pthread_self())) a->violations++;
    if (p) a->balance++;
    CHECKED(pthread_mutex_unlock(&a->mu));
    return p;
}
static void *in_realloc_fn(void *p, size_t o, size_t n, void *ctx)
{
    (void)o; (void)ctx;
    return realloc(p, n);
}
static void in_free_fn(void *p, size_t n, void *ctx)
{
    in_alloc_t *a = ctx;
    (void)n;
    if (!p) return;
    CHECKED(pthread_mutex_lock(&a->mu));
    if (a->confined && !pthread_equal(a->owner, pthread_self())) a->violations++;
    for (size_t i = 0; i < INPUTS; i++)
        if (a->watch[i] == p) {
            a->freed[i]++;
            a->freed_by[i] = pthread_self();
            a->watch[i] = NULL;
        }
    a->balance--;
    CHECKED(pthread_mutex_unlock(&a->mu));
    free(p);
}

/* -- Scripted draft-16 peer --------------------------------------------- */

static size_t put_varint(uint8_t *buf, uint64_t v)
{
    if (v < 0x40) { buf[0] = (uint8_t)v; return 1; }
    if (v < 0x4000) { buf[0] = (uint8_t)(0x40 | (v >> 8)); buf[1] = (uint8_t)v; return 2; }
    buf[0] = (uint8_t)(0x80 | (v >> 24)); buf[1] = (uint8_t)(v >> 16);
    buf[2] = (uint8_t)(v >> 8); buf[3] = (uint8_t)v;
    return 4;
}

static size_t build_subscribe_ok(uint8_t *out, size_t cap, uint64_t rid, uint64_t alias)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, out, cap);
    if (moq_d16_encode_subscribe_ok(&w, rid, alias, NULL, 0, NULL, 0) < 0) return 0;
    return moq_buf_writer_offset(&w);
}

typedef struct {
    bool have_cat, have_vid;
    uint64_t cat_rid, vid_rid;
    int unsubscribes;
} learned_t;

static void drain_and_learn(moq_session_t *client, learned_t *l)
{
    moq_action_t acts[16];
    size_t n;
    while ((n = moq_session_poll_actions(client, acts, 16)) > 0) {
        for (size_t i = 0; i < n; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL) {
                moq_control_envelope_t env;
                moq_buf_reader_t rd;
                moq_buf_reader_init(&rd, acts[i].u.send_control.data,
                                    acts[i].u.send_control.len);
                if (moq_control_decode_envelope(&rd, &env) >= 0) {
                    if (env.msg_type == MOQ_D16_UNSUBSCRIBE) l->unsubscribes++;
                    if (env.msg_type == MOQ_D16_SUBSCRIBE) {
                        moq_bytes_t parts[8];
                        moq_kvp_entry_t params[16];
                        moq_d16_subscribe_t s;
                        memset(&s, 0, sizeof(s));
                        s.params = params;
                        s.params_cap = 16;
                        if (moq_d16_decode_subscribe(env.payload, env.payload_len, parts, 8, &s) >= 0) {
                            if (!l->have_cat) { l->have_cat = true; l->cat_rid = s.request_id; }
                            else if (!l->have_vid) { l->have_vid = true; l->vid_rid = s.request_id; }
                        }
                    }
                }
            }
            moq_action_cleanup(&acts[i]);
        }
    }
}

static const char CATALOG[] =
    "{\"version\":1,\"tracks\":["
    "{\"name\":\"video\",\"packaging\":\"loc\",\"isLive\":true,"
    "\"role\":\"video\",\"codec\":\"avc1.42e01e\"}]}";

static const moq_bytes_t g_ns_parts[] = {
    { (const uint8_t *)"media", 5 }, { (const uint8_t *)"cat", 3 },
};

static void receiver_cfg(moq_media_receiver_cfg_t *rcfg)
{
    moq_media_receiver_cfg_init_live_sized(rcfg, sizeof(*rcfg));
    rcfg->namespace_.parts = g_ns_parts;
    rcfg->namespace_.count = 2;
    rcfg->auto_subscribe = true;
    rcfg->time_mode = MOQ_MEDIA_TIME_RAW;
    rcfg->overflow.policy = MOQ_MEDIA_OVERFLOW_DROP_GROUP;
}

/* -- The fixture --------------------------------------------------------- */

enum {
    ROW_LIVE_TRUSTED, ROW_REFUSE_RECEIVER, ROW_REFUSE_ENDPOINT,
    ROW_DRAIN_TRUSTED, ROW_REFUSED_TRUSTED, ROW_DRAIN_AFFINE, ROW_REFUSED_AFFINE,
    ROW_COUNT
};
static const char *const row_name[] = {
    "live_trusted", "refuse_receiver", "refuse_endpoint",
    "drain_trusted", "refused_trusted", "drain_affine", "refused_affine",
};
static bool row_affine(int row) { return row == ROW_DRAIN_AFFINE || row == ROW_REFUSED_AFFINE; }
static bool row_live(int row) { return row <= ROW_REFUSE_ENDPOINT; }
static bool row_refused_post(int row) { return row == ROW_REFUSED_TRUSTED || row == ROW_REFUSED_AFFINE; }

enum { CMD_NONE, CMD_RELEASE, CMD_DRAIN, CMD_EXIT };

typedef struct fx {
    cnt_alloc_t           ea, ra;        /* endpoint (= session) and receiver */
    moq_alloc_t           ealloc, ralloc;
    in_alloc_t            ia;            /* the transport's input allocator */
    moq_alloc_t           ialloc;
    bool                  trusted;       /* inputs from the session's allocator */
    moq_simpair_t        *sp;
    moq_session_t        *client;
    moq_endpoint_t       *ep;
    moq_media_receiver_t *r;
    moq_media_track_t    *track;
    moq_rcbuf_t          *in[INPUTS];
    moq_rcbuf_t          *held[INPUTS];  /* the subscriber's payload, when it is input k */
    uint64_t              now;
    learned_t             l;
    /* The network thread and its mailbox. */
    pthread_t             net;
    bool                  net_running;
    pthread_mutex_t       mu;
    pthread_cond_t        cv;
    int                   cmd;
    bool                  fed, done_cmd;
    bool                  feed_ok;
} fx_t;

static void fx_pump(fx_t *f, int cycles)
{
    for (int i = 0; i < cycles; i++) {
        f->now += 1000;
        receiver_hook(NULL, f->client, f->now, f->r);
        drain_and_learn(f->client, &f->l);
        moq_media_track_event_t te;
        while (moq_media_receiver_poll_track(f->r, &te, sizeof(te)) == MOQ_OK)
            if (te.kind == MOQ_MEDIA_TRACK_ADDED && !f->track) f->track = te.track;
        (void)moq_session_process_pending(f->client, f->now);
    }
}

static bool fx_open(fx_t *f, bool trusted)
{
    memset(f, 0, sizeof(*f));
    cnt_init(&f->ea); cnt_init(&f->ra);
    f->ealloc = cnt_allocator(&f->ea);
    f->ralloc = cnt_allocator(&f->ra);
    CHECKED(pthread_mutex_init(&f->ia.mu, NULL));
    f->ialloc = (moq_alloc_t){ &f->ia, in_alloc_fn, in_realloc_fn, in_free_fn };
    f->trusted = trusted;
    CHECKED(pthread_mutex_init(&f->mu, NULL));
    CHECKED(pthread_cond_init(&f->cv, NULL));

    /* The sessions use the endpoint's allocator, as a service backend's do. */
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = &f->ealloc;
    cfg.seed = 7;
    cfg.initial_now_us = 1000;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 16;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 16;
    if (moq_simpair_create(&cfg, &f->sp) != MOQ_OK) return false;
    moq_simpair_start(f->sp);
    moq_simpair_run_until_quiescent(f->sp, 16, NULL);
    f->client = moq_simpair_client(f->sp);
    moq_event_t ev;
    while (moq_session_poll_events(f->client, &ev, 1) == 1) moq_event_cleanup(&ev);
    moq_session_t *server = moq_simpair_server(f->sp);
    while (moq_session_poll_events(server, &ev, 1) == 1) moq_event_cleanup(&ev);

    moq_media_receiver_cfg_t rcfg;
    receiver_cfg(&rcfg);
    moq_media_receiver_test_set_alloc(&f->ralloc);
    f->r = moq_media_receiver_test_new_cfg(&rcfg);
    moq_media_receiver_test_set_alloc(NULL);
    if (!f->r) return false;
    /* On a live endpoint, reserved as receiver_new reserves. */
    f->ep = moq_endpoint_test_make_bare_alloc(&f->ealloc);
    if (!f->ep) return false;
    f->r->ep = f->ep;
    if (!receiver_reserve_teardown(f->r)) return false;

    uint8_t ctrl[256];
    bool cat_ok = false, cat_data = false, vid_ok = false;
    f->now = 1000;
    for (int cycle = 0; cycle < 64 && !vid_ok; cycle++) {
        fx_pump(f, 1);
        if (f->l.have_cat && !cat_ok) {
            size_t n = build_subscribe_ok(ctrl, sizeof(ctrl), f->l.cat_rid, 0);
            if (!n || moq_session_on_control_bytes(f->client, ctrl, n, f->now) < 0) return false;
            cat_ok = true;
        }
        if (cat_ok && !cat_data) {
            uint8_t buf[512];
            size_t o = 0;
            buf[o++] = 0x30;
            o += put_varint(buf + o, 0);
            o += put_varint(buf + o, 0);
            o += put_varint(buf + o, 0);
            size_t jlen = strlen(CATALOG);
            o += put_varint(buf + o, jlen);
            memcpy(buf + o, CATALOG, jlen);
            o += jlen;
            if (moq_session_on_data_bytes(f->client, moq_stream_ref_from_u64(101), buf, o,
                                          true, f->now) < 0) return false;
            cat_data = true;
        }
        if (f->track && f->l.have_vid && !vid_ok) {
            size_t n = build_subscribe_ok(ctrl, sizeof(ctrl), f->l.vid_rid, 2);
            if (!n || moq_session_on_control_bytes(f->client, ctrl, n, f->now) < 0) return false;
            vid_ok = true;
        }
    }
    fx_pump(f, 4);
    return vid_ok && f->track && f->r->sub;
}

/* On the network thread: INPUTS objects of group 2 on one subgroup stream,
 * each header as bytes and each payload as the transport's own created
 * rcbuf; the subscriber takes each in (a pass after each object: the
 * session's event queue is bounded), the first RECEIVER_SUB_MAX_OBJECTS into
 * its queue and the last into its pending slot. */
static bool net_feed(fx_t *f)
{
    const moq_alloc_t *al = f->trusted ? &f->ealloc : &f->ialloc;
    moq_stream_ref_t ref = moq_stream_ref_from_u64(1002);
    for (size_t k = 0; k < INPUTS; k++) {
        uint8_t body[16];
        memset(body, (int)(k + 1), sizeof(body));
        moq_rcbuf_t *in = NULL;
        if (moq_rcbuf_create(al, body, sizeof(body), &in) != MOQ_OK) return false;
        f->in[k] = in;
        if (!f->trusted) {
            CHECKED(pthread_mutex_lock(&f->ia.mu));
            f->ia.watch[k] = in;
            CHECKED(pthread_mutex_unlock(&f->ia.mu));
        }
        uint8_t hdr[16];
        size_t h = 0;
        if (k == 0) {
            hdr[h++] = 0x30;
            h += put_varint(hdr + h, 2);      /* track alias */
            h += put_varint(hdr + h, 2);      /* group */
        }
        h += put_varint(hdr + h, 0);          /* object id delta */
        h += put_varint(hdr + h, sizeof(body));
        moq_result_t rc = moq_session_on_data_bytes(f->client, ref, hdr, h, false, f->now);
        if (rc == MOQ_OK) rc = moq_session_on_data_rcbuf(f->client, ref, in, false, f->now);
        if (rc != MOQ_OK) {
            fprintf(stderr, "  feed: object %zu refused: %d\n", k, (int)rc);
            return false;
        }
        f->now += 1;
        (void)moq_sub_tick(f->r->sub, f->now);   /* the subscriber's pass, no intake */
    }
    return true;
}

static void *net_main(void *arg)
{
    fx_t *f = arg;
    CHECKED(pthread_mutex_lock(&f->ia.mu));
    f->ia.owner = pthread_self();
    f->ia.confined = !f->trusted;
    CHECKED(pthread_mutex_unlock(&f->ia.mu));
    bool ok = net_feed(f);
    /* What the subscriber holds for each odd input: the input itself when
     * the session shared it (its count is then 2: the transport's and the
     * subscriber's), else the session's copy (the input's count stays 1). */
    for (size_t k = 1; k < INPUTS; k += 2)
        f->held[k] = (f->in[k] && moq_rcbuf_refcount(f->in[k]) == 2) ? f->in[k] : NULL;
    /* The transport lets go of the even inputs at once and keeps the odd. */
    for (size_t k = 0; k < INPUTS; k += 2)
        if (f->in[k]) { moq_rcbuf_decref(f->in[k]); f->in[k] = NULL; }
    CHECKED(pthread_mutex_lock(&f->mu));
    f->feed_ok = ok;
    f->fed = true;
    CHECKED(pthread_cond_broadcast(&f->cv));
    for (;;) {
        while (f->cmd == CMD_NONE) CHECKED(pthread_cond_wait(&f->cv, &f->mu));
        int cmd = f->cmd;
        f->cmd = CMD_NONE;
        CHECKED(pthread_mutex_unlock(&f->mu));
        if (cmd == CMD_RELEASE) {
            for (size_t k = 1; k < INPUTS; k += 2)
                if (f->in[k]) { moq_rcbuf_decref(f->in[k]); f->in[k] = NULL; }
        } else if (cmd == CMD_DRAIN) {
            f->now += 1000;
            moq_endpoint_test_drain_posted(f->ep, f->client, f->now);
            drain_and_learn(f->client, &f->l);
        }
        CHECKED(pthread_mutex_lock(&f->mu));
        f->done_cmd = true;
        CHECKED(pthread_cond_broadcast(&f->cv));
        if (cmd == CMD_EXIT) break;
    }
    CHECKED(pthread_mutex_unlock(&f->mu));
    return NULL;
}

/* Post a command to the network thread. */
static void net_post(fx_t *f, int cmd)
{
    CHECKED(pthread_mutex_lock(&f->mu));
    f->done_cmd = false;
    f->cmd = cmd;
    CHECKED(pthread_cond_broadcast(&f->cv));
    CHECKED(pthread_mutex_unlock(&f->mu));
}

/* The network thread did not acknowledge the feed or a command in time, so
 * it may still be running it: no read of what it publishes, no further
 * command and no teardown may follow. The test stops here. */
static void net_unacknowledged(const char *what)
{
    fprintf(stderr, "FATAL: the network thread did not acknowledge %s in time\n", what);
    abort();
}

/* Wait (bounded) for the posted command to complete; stop the test if it
 * does not. */
static void net_wait(fx_t *f)
{
    CHECKED(pthread_mutex_lock(&f->mu));
    struct timespec dl;
    clock_gettime(CLOCK_REALTIME, &dl);
    dl.tv_sec += 10;
    while (!f->done_cmd) {
        int e = pthread_cond_timedwait(&f->cv, &f->mu, &dl);
        if (e == ETIMEDOUT) net_unacknowledged("a command");
        CHECKED(e);
    }
    CHECKED(pthread_mutex_unlock(&f->mu));
}

static void net_cmd(fx_t *f, int cmd)
{
    net_post(f, cmd);
    net_wait(f);
}

/* The managed context's stop: the network thread finishes and is joined. */
static moq_result_t net_stop(void *ctx)
{
    fx_t *f = ctx;
    if (!f->net_running) return MOQ_OK;
    net_cmd(f, CMD_EXIT);
    CHECKED(pthread_join(f->net, NULL));
    f->net_running = false;
    return MOQ_OK;
}

/* Start the network thread and wait (bounded) for its feed; stop the test if
 * the feed is not acknowledged. Returns whether the feed succeeded. */
static bool net_start(fx_t *f)
{
    moq_endpoint_test_set_join_facade(f->ep, net_stop, f);
    CHECKED(pthread_create(&f->net, NULL, net_main, f));
    f->net_running = true;
    CHECKED(pthread_mutex_lock(&f->mu));
    struct timespec dl;
    clock_gettime(CLOCK_REALTIME, &dl);
    dl.tv_sec += 10;
    while (!f->fed) {
        int e = pthread_cond_timedwait(&f->cv, &f->mu, &dl);
        if (e == ETIMEDOUT) net_unacknowledged("the feed");
        CHECKED(e);
    }
    bool ok = f->feed_ok;
    CHECKED(pthread_mutex_unlock(&f->mu));
    return ok;
}

/* The confined allocator's watched inputs freed so far: once each, and how
 * many on the network thread and on this (main) thread. */
static void input_frees(fx_t *f, int *once, int *net, int *main_thread)
{
    *once = *net = *main_thread = 0;
    CHECKED(pthread_mutex_lock(&f->ia.mu));
    for (size_t k = 0; k < INPUTS; k++) {
        if (f->ia.freed[k] != 1) continue;
        (*once)++;
        if (pthread_equal(f->ia.freed_by[k], f->ia.owner)) (*net)++;
        else if (pthread_equal(f->ia.freed_by[k], pthread_self())) (*main_thread)++;
    }
    CHECKED(pthread_mutex_unlock(&f->ia.mu));
}

static void run_row(int row)
{
    int before = failures;
    fx_t f;
    bool opened = fx_open(&f, !row_affine(row));
    MOQ_TEST_CHECK(opened);
    if (!opened) return;
    moq_sub_stats_t s0, ss;
    memset(&s0, 0, sizeof(s0));
    memset(&ss, 0, sizeof(ss));
    MOQ_TEST_CHECK(moq_sub_get_stats(f.r->sub, &s0, sizeof(s0)) == MOQ_OK);
    uint64_t taken = f.r->stats.objects_received;
    MOQ_TEST_CHECK(net_start(&f));

    /* The subscriber took in every input; intake took none. Inputs from the
     * session's allocator are the subscriber's payloads themselves; others
     * were copied. */
    MOQ_TEST_CHECK(moq_sub_get_stats(f.r->sub, &ss, sizeof(ss)) == MOQ_OK);
    MOQ_TEST_CHECK(ss.objects_received - s0.objects_received == INPUTS);
    MOQ_TEST_CHECK(ss.objects_polled == s0.objects_polled);
    MOQ_TEST_CHECK(f.r->stats.objects_received == taken);
    int shared = 0;
    for (size_t k = 1; k < INPUTS; k += 2) if (f.held[k]) shared++;
    MOQ_TEST_CHECK(shared == (row_affine(row) ? 0 : (int)INPUTS / 2));
    int pre_once = 0, pre_net = 0, pre_main = 0;
    if (row_affine(row)) {
        /* The even inputs were the transport's alone: released on its thread. */
        input_frees(&f, &pre_once, &pre_net, &pre_main);
        MOQ_TEST_CHECK(pre_once == (int)(INPUTS + 1) / 2 && pre_net == pre_once);
    }

    /* Rows without a live endpoint: the transport releases its own
     * references on its thread before stop(). */
    if (!row_live(row)) net_cmd(&f, CMD_RELEASE);
    if (row_refused_post(row)) MOQ_TEST_CHECK(moq_endpoint_stop(f.ep) == MOQ_OK);

    uint64_t rreq = cnt_requests(&f.ra), ereq = cnt_requests(&f.ea);
    if (row == ROW_REFUSE_RECEIVER) cnt_arm(&f.ra, true, 0);
    if (row == ROW_REFUSE_ENDPOINT) cnt_arm(&f.ea, true, 0);
    if (row_live(row)) {
        /* Destroy here while the live network thread releases its
         * references. */
        net_post(&f, CMD_RELEASE);
        moq_media_receiver_destroy(f.r);
        net_wait(&f);
    } else {
        moq_media_receiver_destroy(f.r);
    }
    f.r = NULL;
    cnt_arm(&f.ra, false, 0);
    cnt_arm(&f.ea, false, 0);
    uint64_t rreq_destroy = cnt_requests(&f.ra) - rreq;
    uint64_t ereq_destroy = cnt_requests(&f.ea) - ereq;
    MOQ_TEST_CHECK(rreq_destroy == 0);
    MOQ_TEST_CHECK(ereq_destroy == 0);

    if (row_live(row)) {
        /* The teardown is the live network thread's: posted, not run here. */
        MOQ_TEST_CHECK(moq_endpoint_test_task_count(f.ep) == 1);
        net_cmd(&f, CMD_DRAIN);
        MOQ_TEST_CHECK(f.l.unsubscribes == 2);       /* catalog and video */
        MOQ_TEST_CHECK(moq_endpoint_test_task_count(f.ep) == 0);
        MOQ_TEST_CHECK(moq_endpoint_stop(f.ep) == MOQ_OK);
    } else if (row_refused_post(row)) {
        /* Refused: the destroying thread tore the subscriber down. */
        MOQ_TEST_CHECK(moq_endpoint_test_task_count(f.ep) == 0);
    } else {
        /* Accepted; stop() then joined the network thread and its terminal
         * drain ran the teardown here, on stop's caller. */
        MOQ_TEST_CHECK(moq_endpoint_test_task_count(f.ep) == 1);
        MOQ_TEST_CHECK(moq_endpoint_stop(f.ep) == MOQ_OK);
        MOQ_TEST_CHECK(moq_endpoint_test_task_count(f.ep) == 0);
    }
    MOQ_TEST_CHECK(!f.net_running);

    /* Every transport input released exactly once; an input from the
     * confined allocator only ever on the network thread. */
    int once = 0, on_net = 0, on_main = 0, violations;
    CHECKED(pthread_mutex_lock(&f.ia.mu));
    violations = f.ia.violations;
    CHECKED(pthread_mutex_unlock(&f.ia.mu));
    if (row_affine(row)) {
        input_frees(&f, &once, &on_net, &on_main);
        MOQ_TEST_CHECK(once == (int)INPUTS && on_net == (int)INPUTS && on_main == 0);
        MOQ_TEST_CHECK(violations == 0);
    }
    for (size_t k = 0; k < INPUTS; k++) MOQ_TEST_CHECK(f.in[k] == NULL);
    printf("%s: destroy requests receiver=%llu endpoint=%llu; shared inputs %d; "
           "confined-allocator inputs freed once %d (network %d, main %d), violations %d\n",
           row_name[row], (unsigned long long)rreq_destroy, (unsigned long long)ereq_destroy,
           shared, once, on_net, on_main, violations);

    moq_endpoint_test_free_bare(f.ep);
    moq_simpair_destroy(f.sp);
    MOQ_TEST_CHECK(cnt_balance(&f.ea) == 0);
    MOQ_TEST_CHECK(cnt_balance(&f.ra) == 0);
    CHECKED(pthread_mutex_lock(&f.ia.mu));
    int64_t ib = f.ia.balance;
    CHECKED(pthread_mutex_unlock(&f.ia.mu));
    MOQ_TEST_CHECK(ib == 0);
    CHECKED(pthread_cond_destroy(&f.cv));
    CHECKED(pthread_mutex_destroy(&f.mu));
    CHECKED(pthread_mutex_destroy(&f.ia.mu));
    CHECKED(pthread_mutex_destroy(&f.ea.mu));
    CHECKED(pthread_mutex_destroy(&f.ra.mu));
    printf("ROW %s: %s\n", row_name[row], failures == before ? "ok" : "FAILED");
}

/* Construction reserves the teardown context, then the task node, from the
 * endpoint's allocator. Either refused: attach fails with NOMEM, attaches no
 * hook and keeps nothing; with both granted it attaches, and destroy
 * returns everything. */
static void test_construction_reservation(void)
{
    int before = failures;
    cnt_alloc_t ea, ra;
    cnt_init(&ea);
    cnt_init(&ra);
    moq_alloc_t ealloc = cnt_allocator(&ea), ralloc = cnt_allocator(&ra);
    moq_endpoint_t *ep = moq_endpoint_test_make_bare_alloc(&ealloc);
    MOQ_TEST_CHECK(ep != NULL);
    if (!ep) return;
    int64_t ebase = cnt_balance(&ea);
    moq_media_receiver_cfg_t cfg;
    receiver_cfg(&cfg);
    for (uint64_t nth = 1; nth <= 3; nth++) {
        uint64_t req0 = cnt_requests(&ea);
        cnt_arm(&ea, false, nth);
        moq_media_receiver_test_set_alloc(&ralloc);
        moq_media_receiver_t *r = NULL;
        moq_result_t rc = moq_media_receiver_attach(ep, &cfg, &r);
        moq_media_receiver_test_set_alloc(NULL);
        cnt_arm(&ea, false, 0);
        uint64_t reqs = cnt_requests(&ea) - req0;
        if (nth <= 2) {
            MOQ_TEST_CHECK(rc == MOQ_ERR_NOMEM && r == NULL);
            MOQ_TEST_CHECK(reqs == nth);
            MOQ_TEST_CHECK(moq_endpoint_test_attachments(ep) == 0);
        } else {
            MOQ_TEST_CHECK(rc == MOQ_OK && r != NULL);
            MOQ_TEST_CHECK(reqs == 2);                 /* the context and the node */
            MOQ_TEST_CHECK(moq_endpoint_test_attachments(ep) == 1);
            if (r) moq_media_receiver_destroy(r);
            MOQ_TEST_CHECK(moq_endpoint_test_attachments(ep) == 0);
        }
        MOQ_TEST_CHECK(cnt_balance(&ea) == ebase);
        MOQ_TEST_CHECK(cnt_balance(&ra) == 0);
        printf("construction, endpoint request %llu refused: rc %d, endpoint requests %llu\n",
               (unsigned long long)nth, (int)rc, (unsigned long long)reqs);
    }
    moq_endpoint_test_free_bare(ep);
    MOQ_TEST_CHECK(cnt_balance(&ea) == 0);
    CHECKED(pthread_mutex_destroy(&ea.mu));
    CHECKED(pthread_mutex_destroy(&ra.mu));
    printf("ROW construction_reservation: %s\n", failures == before ? "ok" : "FAILED");
}

int main(void)
{
    for (int row = 0; row < ROW_COUNT; row++) run_row(row);
    test_construction_reservation();
    if (failures) {
        fprintf(stderr, "test_media_receiver_facade_teardown: %d failure(s)\n", failures);
        return 1;
    }
    printf("test_media_receiver_facade_teardown: all passed\n");
    return 0;
}
