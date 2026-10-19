/*
 * What does and does not hold the threaded adapter's drain, over real
 * loopback (test-internals build for the stage hook and drain facts).
 *
 *   receive_backlog   a subscriber whose application stops polling while
 *                     the publisher sends objects it then sees acknowledged:
 *                     the subscriber's own send halves are flushed and
 *                     acknowledged, so its drain reaches DONE with the
 *                     received objects still unpolled;
 *   open_idle_bidi    draft-18: the subscription's request stream stays open
 *                     in both directions, idle (neither side has received a
 *                     FIN or RESET on it); both sides reach DONE;
 *   timer_alone       after a GOAWAY the session holds a future deadline (the
 *                     GOAWAY drain timer); the drain reaches DONE while that
 *                     deadline is still pending;
 *   unacked_fin       the pass that writes a subgroup's object and FIN parks
 *                     at stage 2: session output is already handed over, yet
 *                     the drain is PENDING because the stream's send half is
 *                     not acknowledged (or not yet flushed) -- a transport
 *                     reason, not the observer; released, it reaches DONE.
 *
 *   half_classes      no network: on a real picoquic connection, a RESET-ended
 *                     direction is not an open one -- the open_idle_bidi
 *                     classifier tells open, FIN-ended and RESET-ended apart.
 *
 * Preparation failures fail the row and unwind; parking is bounded.
 */
#include <moq/picoquic_threaded.h>
#include <moq/rcbuf.h>

#include <picoquic.h>
#include <picoquic_internal.h>   /* stream records, for the classifier row */

#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>

typedef struct {
    size_t connections;
    bool   ready, session_pending, bridge_pending, backlog_empty, acked, gen_unchanged;
} moq_pq_threaded_test_drain_facts_t;
extern void moq_pq_threaded_test_set_stage_hook(
    moq_pq_threaded_t *t, void (*fn)(moq_pq_threaded_t *, int, void *), void *ctx);
typedef struct {
    size_t open, fin, reset;
} moq_pq_threaded_test_bidi_halves_t;
extern void moq_pq_threaded_test_bidi_halves(moq_pq_threaded_t *t, bool peer_initiated,
                                             moq_pq_threaded_test_bidi_halves_t *out);
extern void moq_pq_threaded_test_cnx_bidi_halves(picoquic_cnx_t *cnx, bool client_initiated,
                                                 moq_pq_threaded_test_bidi_halves_t *out);
extern void moq_pq_threaded_test_drain_facts(moq_pq_threaded_t *t,
                                             moq_pq_threaded_test_drain_facts_t *out);

static int failures = 0;
#define CHECK(expr)                                                          \
    do {                                                                     \
        if (!(expr)) {                                                       \
            fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr);  \
            failures++;                                                      \
        }                                                                    \
    } while (0)

/* The classifier the open_idle_bidi row relies on, on a client connection
 * no thread drives: server-initiated bidis 1 (open), 5 (FIN), 9 (RESET
 * without FIN), 13 (FIN then RESET), and a client-initiated 0 (open, not in
 * the server-initiated set). */
static void run_half_classes(void)
{
    int before = failures;
    uint64_t now = 0;
    picoquic_quic_t *quic = picoquic_create(1, NULL, NULL, NULL, "moqt-18", NULL, NULL,
                                            NULL, NULL, NULL, now, &now, NULL, NULL, 0);
    picoquic_cnx_t *cnx = NULL;
    CHECK(quic != NULL);
    if (quic) {
        struct sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_port = htons(4433);
        cnx = picoquic_create_cnx(quic, picoquic_null_connection_id,
                                  picoquic_null_connection_id, (struct sockaddr *)&addr,
                                  now, 0, "localhost", "moqt-18", 1);
    }
    CHECK(cnx != NULL);
    if (cnx) {
        picoquic_stream_head_t *s0 = picoquic_create_stream(cnx, 0);
        picoquic_stream_head_t *s1 = picoquic_create_stream(cnx, 1);
        picoquic_stream_head_t *s5 = picoquic_create_stream(cnx, 5);
        picoquic_stream_head_t *s9 = picoquic_create_stream(cnx, 9);
        picoquic_stream_head_t *s13 = picoquic_create_stream(cnx, 13);
        CHECK(s0 && s1 && s5 && s9 && s13);
        if (s0 && s1 && s5 && s9 && s13) {
            s5->fin_received = 1;
            s9->reset_received = 1;
            s13->fin_received = 1;
            s13->reset_received = 1;
            moq_pq_threaded_test_bidi_halves_t h;
            memset(&h, 0, sizeof(h));
            moq_pq_threaded_test_cnx_bidi_halves(cnx, false, &h);
            CHECK(h.open == 1);                  /* stream 1 only */
            CHECK(h.fin == 1);                   /* stream 5 */
            CHECK(h.reset == 2);                 /* 9 and 13: RESET-ended */
            memset(&h, 0, sizeof(h));
            moq_pq_threaded_test_cnx_bidi_halves(cnx, true, &h);
            CHECK(h.open == 1 && h.fin == 0 && h.reset == 0);   /* stream 0 */
        }
    }
    if (quic) picoquic_free(quic);           /* frees its connections */
    printf("%s: threaded drain control half_classes\n",
           failures == before ? "PASS" : "FAIL");
}

#ifdef MOQ_TEST_CERT_PATH

enum { BOUND_MS = 5000, BACKLOG_OBJECTS = 40 };

typedef enum { ROW_RECEIVE_BACKLOG, ROW_OPEN_IDLE_BIDI, ROW_TIMER_ALONE, ROW_UNACKED_FIN } row_kind_t;

typedef struct {
    pthread_mutex_t mu;
    pthread_cond_t  cv;
    row_kind_t      kind;
    moq_pq_threaded_t *srv, *cli;      /* server = publisher, client = subscriber */
    /* subscription */
    bool               subscribed, established, sub_ok;
    moq_subscription_t pub_sub;
    /* receive_backlog */
    bool               stop_polling;   /* the subscriber's app stops polling */
    int                sent;           /* objects the publisher wrote */
    bool               count_now;
    int                unpolled_objects;
    bool               counted;
    /* timer_alone */
    bool               goaway_sent;
    uint64_t           last_deadline, last_now;
    /* unacked_fin */
    bool               hook_installed, arm_fin, fin_this_pass;
    uint64_t           fin_group;       /* the next group the FIN row writes */
    int                overtaken;
    bool               parked, release, observed;
    bool               park_timed_out;
    /* open_idle_bidi: request streams without a FIN from the peer, sampled
     * by each side's own pump on request */
    unsigned           srv_req, srv_done, cli_req, cli_done;
    moq_pq_threaded_test_bidi_halves_t srv_halves, cli_halves;
    moq_pq_threaded_test_drain_facts_t facts;
} ctx_t;

static ctx_t g;
static void lock(void) { pthread_mutex_lock(&g.mu); }
static void unlock(void) { pthread_mutex_unlock(&g.mu); }

static bool wait_flag(bool *flag)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += BOUND_MS / 1000;
    int rc = 0;
    while (!*flag && rc == 0) rc = pthread_cond_timedwait(&g.cv, &g.mu, &ts);
    return *flag;
}

/* Under g.mu: wait until *done == want within BOUND_MS. */
static bool wait_count(unsigned *done, unsigned want)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += BOUND_MS / 1000;
    int rc = 0;
    while (*done != want && rc == 0) rc = pthread_cond_timedwait(&g.cv, &g.mu, &ts);
    return *done == want;
}

static void stage_hook(moq_pq_threaded_t *t, int stage, void *ctx)
{
    (void)ctx;
    if (stage != 2) return;
    lock();
    bool mine = g.fin_this_pass && !g.observed;
    unlock();
    if (!mine) return;
    moq_pq_threaded_test_drain_facts_t f;
    moq_pq_threaded_test_drain_facts(t, &f);
    /* Observe only a pass no wake overtook; the pending wake brings another
     * pass, which writes a fresh group. */
    if (!f.gen_unchanged) {
        lock(); g.overtaken++; g.fin_this_pass = false; unlock();
        return;
    }
    lock();
    g.facts = f;
    g.observed = g.parked = true;
    pthread_cond_broadcast(&g.cv);
    if (!wait_flag(&g.release))
        g.park_timed_out = true;
    g.parked = false;
    pthread_cond_broadcast(&g.cv);
    unlock();
}

static moq_rcbuf_t *one_byte(void)
{
    static const uint8_t byte = 9;
    moq_rcbuf_t *b = NULL;
    return moq_rcbuf_create(moq_alloc_default(), &byte, 1, &b) < 0 ? NULL : b;
}

/* One reliable object on its own subgroup, then the subgroup's FIN. */
static bool write_group(moq_session_t *s, moq_subscription_t sub, uint64_t group, uint64_t now)
{
    if (moq_session_action_capacity(s) < 4) return false;
    moq_subgroup_cfg_t sc;
    moq_subgroup_cfg_init(&sc);
    sc.group_id = group;
    moq_subgroup_handle_t sg;
    if (moq_session_open_subgroup(s, sub, &sc, now, &sg) != MOQ_OK) return false;
    moq_rcbuf_t *b = one_byte();
    bool ok = b && moq_session_write_object(s, sg, 0, b, now) == MOQ_OK;
    moq_rcbuf_decref(b);
    return moq_session_close_subgroup(s, sg, now) == MOQ_OK && ok;
}

/* On a pump: answer an outstanding open-stream sample. */
static void sample_open(moq_pq_threaded_t *t, unsigned *req, unsigned *done,
                        moq_pq_threaded_test_bidi_halves_t *out, bool peer_initiated)
{
    lock();
    bool want = *req != *done;
    unsigned r = *req;
    unlock();
    if (!want) return;
    moq_pq_threaded_test_bidi_halves_t h;
    moq_pq_threaded_test_bidi_halves(t, peer_initiated, &h);
    lock(); *out = h; *done = r; pthread_cond_broadcast(&g.cv); unlock();
}

static int server_pump(moq_pq_threaded_t *t, moq_pq_threaded_lane_t *lane, uint64_t now, void *ctx)
{
    (void)ctx;
    sample_open(t, &g.srv_req, &g.srv_done, &g.srv_halves, true);
    if (g.kind == ROW_UNACKED_FIN && !g.hook_installed) {
        moq_pq_threaded_test_set_stage_hook(t, stage_hook, NULL);   /* owner executor */
        lock(); g.hook_installed = true; pthread_cond_broadcast(&g.cv); unlock();
    }
    for (moq_pq_threaded_conn_t *c = moq_pq_threaded_lane_next_conn(lane, NULL); c;
         c = moq_pq_threaded_lane_next_conn(lane, c)) {
        moq_session_t *s = moq_pq_threaded_conn_session(c);
        if (!s) continue;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                moq_subscription_t sub = ev.u.subscribe_request.sub;
                moq_accept_subscribe_cfg_t ac;
                moq_accept_subscribe_cfg_init(&ac);
                if (moq_session_accept_subscribe(s, sub, &ac, now) == MOQ_OK) {
                    lock(); g.pub_sub = sub; g.established = true;
                    pthread_cond_broadcast(&g.cv); unlock();
                }
            }
            moq_event_cleanup(&ev);
        }
        lock();
        bool est = g.established, stop = g.stop_polling, arm = g.arm_fin, fin_done = g.observed;
        int sent = g.sent;
        moq_subscription_t sub = g.pub_sub;
        unlock();
        if (g.kind == ROW_RECEIVE_BACKLOG && est && stop) {
            while (sent < BACKLOG_OBJECTS && write_group(s, sub, 1 + (uint64_t)sent, now)) sent++;
            lock(); g.sent = sent; pthread_cond_broadcast(&g.cv); unlock();
        }
        if (g.kind == ROW_TIMER_ALONE && est && !g.goaway_sent) {
            if (moq_session_goaway(s, NULL, 0, now) == MOQ_OK) {
                lock(); g.goaway_sent = true; pthread_cond_broadcast(&g.cv); unlock();
            }
        }
        if (g.kind == ROW_TIMER_ALONE) {
            uint64_t dl = moq_session_next_deadline_us(s);
            lock(); g.last_deadline = dl; g.last_now = now; unlock();
        }
        if (g.kind == ROW_UNACKED_FIN && est && arm && !fin_done) {
            lock(); bool again = !g.fin_this_pass; uint64_t grp = ++g.fin_group; unlock();
            if (again) {
                bool ok = write_group(s, sub, grp, now);
                lock(); g.fin_this_pass = ok; unlock();
            }
        }
    }
    return 0;
}

static int client_pump(moq_pq_threaded_t *t, moq_pq_threaded_lane_t *lane, uint64_t now, void *ctx)
{
    (void)lane; (void)ctx;
    sample_open(t, &g.cli_req, &g.cli_done, &g.cli_halves, false);
    moq_session_t *s = moq_pq_threaded_session(t);
    if (!s) return 0;
    lock();
    bool stop = g.stop_polling, count = g.count_now && !g.counted;
    unlock();
    if (!g.subscribed && moq_session_state(s) == MOQ_SESS_ESTABLISHED) {
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("ctl") };
        moq_subscribe_cfg_t sc;
        moq_subscribe_cfg_init(&sc);
        sc.track_namespace = (moq_namespace_t){ parts, 1 };
        sc.track_name = MOQ_BYTES_LITERAL("t");
        sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
        moq_subscription_t sub;
        if (moq_session_subscribe(s, &sc, now, &sub) == MOQ_OK) g.subscribed = true;
    }
    if (stop && !count) return 0;               /* the app is not polling */
    int objects = 0;
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) == 1) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_OK) {
            lock(); g.sub_ok = true; pthread_cond_broadcast(&g.cv); unlock();
        }
        if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) objects++;
        moq_event_cleanup(&ev);
    }
    if (count) {
        lock(); g.unpolled_objects = objects; g.counted = true;
        pthread_cond_broadcast(&g.cv); unlock();
    }
    return 0;
}

static const char *const k_d18[] = { "moqt-18" };

static moq_pq_threaded_t *make(bool server, int port, bool d18)
{
    moq_pq_threaded_cfg_t cfg;
    moq_pq_threaded_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = server ? MOQ_PERSPECTIVE_SERVER : MOQ_PERSPECTIVE_CLIENT;
    cfg.port = port;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 16;
    if (d18) { cfg.alpn_list = k_d18; cfg.alpn_count = 1; }
    if (server) {
        cfg.cert_path = MOQ_TEST_CERT_PATH;
        cfg.key_path = MOQ_TEST_KEY_PATH;
        cfg.on_lane_pump = server_pump;
        cfg.goaway_timeout_us = 30000000;       /* a timer well past the row */
    } else {
        cfg.host = "localhost";
        cfg.insecure_skip_verify = true;
        cfg.on_lane_pump = client_pump;
    }
    moq_pq_threaded_t *t = NULL;
    return moq_pq_threaded_create(&cfg, &t) == MOQ_OK ? t : NULL;
}

static uint64_t mono_us(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000u + (uint64_t)ts.tv_nsec / 1000u;
}

/* Wake once, read as passes complete, re-wake after a monotonic quiet
 * second; a BOUND_MS monotonic deadline; a closed or failed facade ends it. */
static bool probe_reaches(moq_pq_threaded_t *t, int want)
{
    uint64_t start = mono_us(), deadline = start + (uint64_t)BOUND_MS * 1000u;
    uint64_t next_wake = start + 1000000u;
    moq_pq_threaded_wake(t);
    for (;;) {
        if (moq_pq_threaded_drain_state(t) == want) return true;
        uint64_t now = mono_us();
        if (now >= deadline) return false;
        if (now >= next_wake) {
            moq_pq_threaded_wake(t);
            next_wake = now + 1000000u;
        }
        uint64_t slice = deadline - now < 10000u ? deadline - now : 10000u;
        moq_result_t rc = moq_pq_threaded_wait(t, slice);
        if (rc == MOQ_ERR_CLOSED || rc == MOQ_ERR_INTERNAL) return false;
    }
}

static void run_row(row_kind_t kind, const char *name, int port)
{
    int before = failures;
    memset(&g, 0, sizeof(g));
    pthread_mutex_init(&g.mu, NULL);
    pthread_cond_init(&g.cv, NULL);
    g.kind = kind;
    bool d18 = kind == ROW_OPEN_IDLE_BIDI;
    g.srv = make(true, port, d18);
    g.cli = g.srv ? make(false, port, d18) : NULL;
    CHECK(g.srv && g.cli);
    if (!g.srv || !g.cli) goto out;
    lock();
    bool est = wait_flag(&g.established) && wait_flag(&g.sub_ok);
    unlock();
    CHECK(est);
    if (!est) goto out;

    switch (kind) {
    case ROW_RECEIVE_BACKLOG: {
        lock(); g.stop_polling = true; unlock();
        moq_pq_threaded_wake(g.srv);
        bool all_sent = false;
        for (int i = 0; i < 500 && !all_sent; i++) {
            lock(); all_sent = g.sent >= BACKLOG_OBJECTS; unlock();
            if (!all_sent) { moq_pq_threaded_wake(g.srv); moq_pq_threaded_wait(g.srv, 10000); }
        }
        CHECK(all_sent);
        if (!all_sent) goto out;
        /* Written is not delivered: the publisher's drain reaches DONE only
         * once the subscriber acknowledged every object and FIN, so the
         * subscriber's transport holds them all before its drain is read. */
        bool delivered = probe_reaches(g.srv, 1);
        CHECK(delivered);
        if (!delivered) goto out;
        /* The subscriber has unpolled objects and still drains. */
        CHECK(probe_reaches(g.cli, 1));
        lock(); g.count_now = true; unlock();
        moq_pq_threaded_wake(g.cli);
        lock(); bool counted = wait_flag(&g.counted); int n = g.unpolled_objects; unlock();
        CHECK(counted);
        CHECK(n > 0);                            /* the backlog existed */
        printf("receive_backlog: %d objects were unpolled at DONE\n", n);
        break;
    }
    case ROW_OPEN_IDLE_BIDI: {
        /* Draft-18: both drains reach DONE while the subscription's request
         * stream is open both ways -- after DONE, the server has no FIN from
         * the client on it, and the client none from the server. */
        CHECK(probe_reaches(g.cli, 1));
        CHECK(probe_reaches(g.srv, 1));
        lock(); unsigned sr = ++g.srv_req, cr = ++g.cli_req; unlock();
        moq_pq_threaded_wake(g.srv);
        moq_pq_threaded_wake(g.cli);
        lock();
        bool sampled = wait_count(&g.srv_done, sr) && wait_count(&g.cli_done, cr);
        moq_pq_threaded_test_bidi_halves_t sh = g.srv_halves, ch = g.cli_halves;
        unlock();
        CHECK(sampled);
        /* The client's half, seen by the server; the server's, by the client:
         * open, neither FIN- nor RESET-ended. */
        CHECK(sh.open == 1 && sh.fin == 0 && sh.reset == 0);
        CHECK(ch.open == 1 && ch.fin == 0 && ch.reset == 0);
        printf("open_idle_bidi: request stream open both ways at DONE "
               "(server sees open=%zu fin=%zu reset=%zu; client sees open=%zu "
               "fin=%zu reset=%zu)\n", sh.open, sh.fin, sh.reset, ch.open, ch.fin, ch.reset);
        break;
    }
    case ROW_TIMER_ALONE: {
        lock(); bool sent = wait_flag(&g.goaway_sent); unlock();
        CHECK(sent);
        if (!sent) goto out;
        bool done = probe_reaches(g.srv, 1);
        lock(); uint64_t dl = g.last_deadline, now = g.last_now; unlock();
        CHECK(done);
        CHECK(dl != UINT64_MAX && dl > now);     /* the timer is still pending */
        break;
    }
    case ROW_UNACKED_FIN: {
        lock(); bool hooked = wait_flag(&g.hook_installed); unlock();
        bool baseline = hooked && probe_reaches(g.srv, 1);
        CHECK(baseline);
        if (!baseline) goto out;
        /* The arm and its wake are one step under g.mu, held from there
         * through the probe read (see the gates test for the lock order):
         * a pass that began before the wake fails gen_unchanged, and the
         * parked hook cannot resume until g.mu is released. */
        lock();
        g.arm_fin = true;
        moq_result_t wrc = moq_pq_threaded_wake(g.srv);
        bool parked = wrc == MOQ_OK && wait_flag(&g.parked);
        moq_pq_threaded_test_drain_facts_t f = g.facts;
        int probe = parked ? moq_pq_threaded_drain_state(g.srv) : -99;
        unlock();
        CHECK(parked);
        if (parked) {
            CHECK(f.gen_unchanged);
            CHECK(!f.session_pending);           /* not the observer */
            CHECK(!f.bridge_pending);
            CHECK(!f.acked || !f.backlog_empty); /* the FIN's send half */
            CHECK(probe == 0);
        }
        lock(); g.release = true; pthread_cond_broadcast(&g.cv); unlock();
        CHECK(probe_reaches(g.srv, 1));          /* acknowledged: DONE */
        lock(); bool timed_out = g.park_timed_out; unlock();
        CHECK(!timed_out);
        break;
    }
    }

out:
    lock(); g.release = true; pthread_cond_broadcast(&g.cv); unlock();
    if (g.cli) { moq_pq_threaded_stop(g.cli); moq_pq_threaded_destroy(g.cli); }
    if (g.srv) { moq_pq_threaded_stop(g.srv); moq_pq_threaded_destroy(g.srv); }
    pthread_cond_destroy(&g.cv);
    pthread_mutex_destroy(&g.mu);
    printf("%s: threaded drain control %s\n", failures == before ? "PASS" : "FAIL", name);
}

int main(void)
{
    run_half_classes();
    srand((unsigned)getpid());
    int port = 16700 + (rand() % 400);
    run_row(ROW_RECEIVE_BACKLOG, "receive_backlog", port);
    run_row(ROW_OPEN_IDLE_BIDI, "open_idle_bidi", port + 5);
    run_row(ROW_TIMER_ALONE, "timer_alone", port + 10);
    run_row(ROW_UNACKED_FIN, "unacked_fin", port + 15);
    return failures ? 1 : 0;
}

#else

int main(void)
{
    run_half_classes();
    printf("SKIP: threaded drain controls over loopback (no MOQ_TEST_CERT_PATH)\n");
    return failures ? 1 : 0;
}

#endif
