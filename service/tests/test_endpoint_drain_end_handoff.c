/*
 * moq_endpoint_drain over a real media sender and a real managed backend --
 * the threaded picoquic facade (raw QUIC), or with END_BACKEND_WT the managed
 * WebTransport facade -- against a peer of the same transport: a finite
 * publication's END handoff. One source, built once per backend.
 *
 * A push-mode sender publishes "v", writes its objects and drains (a baseline
 * success). Then, with the endpoint's network thread held after a pass has
 * published DONE, the application ends the track:
 *   - stale idle: the DONE that pass latched before END was queued is gone as
 *     soon as end_track returns, and a drain started while no pass can run
 *     keeps probing PENDING and has no quiet service state;
 *   - END handoff: released, the next real sender pump hands END to the
 *     session and the endpoint publishes the drain request as quiet; that pass
 *     parks after its pump, before its post-pump bridge/provider service,
 *     holding neither the endpoint nor the facade mutex. The drain observes
 *     the quiet request and keeps probing PENDING; it does not succeed there.
 *     Released, service hands END to the transport, the peer acknowledges it,
 *     and only then does the drain succeed -- the peer then reports the
 *     publication finished (TRACK_ENDED, every stream).
 * The same park also keeps the drain's other outcomes: the interrupt latch
 * wins while the pass is parked, and a peer close during the handoff ends
 * the drain as CLOSED, never as success.
 *
 * The old pass: a pass held after its pump while END is queued and woken,
 * then parked after it published, must leave the probe PENDING although its
 * own drain inputs are quiet -- it began before the work. And a refused drain
 * thread unwinds without a join.
 *
 * "PENDING" is shown by the drain's own facade wakes advancing while it has
 * not returned, never by a wait that timed out. Holds and parks are bounded
 * and recorded: a hold or park that timed out fails the row. Built on the
 * MOQ_MEDIA_SENDER_TESTING endpoint seams and the backend's stage hook
 * (MOQ_PQ_THREADED_TESTING or MOQ_PICO_WT_TESTING; test-internals builds
 * only).
 */
#include <moq/media_sender.h>
#include <moq/endpoint.h>
#include <moq/session.h>
#ifdef END_BACKEND_WT
#include <moq/pico_wt_managed.h>
#include "pico_wt_test_seam.h"
#else
#include <moq/picoquic_threaded.h>
#endif
#include <moq/msf.h>
#include <moq/rcbuf.h>
#include "test_support.h"

#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

static int failures = 0;

/* Endpoint seams (endpoint.c, MOQ_MEDIA_SENDER_TESTING). */
void *moq_endpoint_test_facade(moq_endpoint_t *ep);
uint64_t moq_endpoint_test_drain_req(moq_endpoint_t *ep);
uint64_t moq_endpoint_test_drain_quiet(moq_endpoint_t *ep);

#ifdef END_BACKEND_WT
/* Managed WebTransport seams (pico_wt_managed.c, MOQ_PICO_WT_TESTING). */
typedef moq_pico_wt_managed_t facade_t;
typedef moq_pwt_test_drain_facts_t facts_t;
#define facade_drain_state     moq_pico_wt_managed_drain_state
#define facade_set_stage_hook  moq_pico_wt_managed_test_set_stage_hook
#define facade_drain_facts     moq_pico_wt_managed_test_drain_facts
#define facade_drain_gen       moq_pico_wt_managed_test_drain_gen
#define BACKEND_NAME           "managed WebTransport"
#else
/* Threaded backend seams (moq_picoquic_threaded.c, MOQ_PQ_THREADED_TESTING). */
typedef struct {
    size_t connections;
    bool   ready, session_pending, bridge_pending, backlog_empty, acked, gen_unchanged;
} moq_pq_threaded_test_drain_facts_t;
void moq_pq_threaded_test_set_stage_hook(
    moq_pq_threaded_t *t, void (*fn)(moq_pq_threaded_t *, int, void *), void *ctx);
void moq_pq_threaded_test_drain_facts(moq_pq_threaded_t *t,
                                      moq_pq_threaded_test_drain_facts_t *out);
uint64_t moq_pq_threaded_test_drain_gen(moq_pq_threaded_t *t);
typedef moq_pq_threaded_t facade_t;
typedef moq_pq_threaded_test_drain_facts_t facts_t;
#define facade_drain_state     moq_pq_threaded_drain_state
#define facade_set_stage_hook  moq_pq_threaded_test_set_stage_hook
#define facade_drain_facts     moq_pq_threaded_test_drain_facts
#define facade_drain_gen       moq_pq_threaded_test_drain_gen
#define BACKEND_NAME           "threaded"
#endif

enum { BOUND_MS = 5000, OBJECTS = 12 };

typedef enum {
    MODE_SUCCESS, MODE_INTERRUPT, MODE_PEER_CLOSE, MODE_OLD_PASS, MODE_SPAWN_FAIL
} row_mode_t;

/* -- push-only peer -------------------------------------------------- */

typedef struct {
    pthread_mutex_t   mu;
    bool              pub_v_ok;
    moq_publication_t pub_v;
    int               objs, finished;
    uint64_t          fin_status, fin_streams;
    bool              close_now, closed;
} peer_t;

static peer_t g_peer;

/* The peer application's events, on its pump. */
static void peer_events(moq_session_t *s, uint64_t now_us)
{
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) == 1) {
        if (ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED) {
            moq_accept_namespace_cfg_t acc;
            moq_accept_namespace_cfg_init(&acc);
            moq_session_accept_namespace(s, ev.u.namespace_published.ann, &acc, now_us);
        } else if (ev.kind == MOQ_EVENT_PUBLISH_REQUEST) {
            const moq_publish_request_event_t *pr = &ev.u.publish_request;
            bool is_v = pr->track_name.len == 1 && pr->track_name.data[0] == 'v';
            moq_publication_t cand = pr->pub;
            moq_accept_publish_cfg_t acc;
            moq_accept_publish_cfg_init(&acc);
            if (moq_session_accept_publish(s, cand, &acc, now_us) == MOQ_OK && is_v) {
                pthread_mutex_lock(&g_peer.mu);
                g_peer.pub_v = cand;
                g_peer.pub_v_ok = true;
                pthread_mutex_unlock(&g_peer.mu);
            }
        } else if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) {
            const moq_object_received_event_t *o = &ev.u.object_received;
            pthread_mutex_lock(&g_peer.mu);
            if (g_peer.pub_v_ok && moq_publication_eq(o->pub, g_peer.pub_v) &&
                o->status == MOQ_OBJECT_NORMAL)
                g_peer.objs++;
            pthread_mutex_unlock(&g_peer.mu);
        } else if (ev.kind == MOQ_EVENT_PUBLISH_FINISHED) {
            pthread_mutex_lock(&g_peer.mu);
            if (g_peer.pub_v_ok &&
                moq_publication_eq(ev.u.publish_finished.pub, g_peer.pub_v)) {
                g_peer.finished++;
                g_peer.fin_status = ev.u.publish_finished.status_code;
                g_peer.fin_streams = ev.u.publish_finished.stream_count;
            }
            pthread_mutex_unlock(&g_peer.mu);
        }
        moq_event_cleanup(&ev);
    }
}

/* The peer closes once asked: true when the close was accepted (not proof
 * that it reached the wire). */
static bool peer_close_requested(void)
{
    pthread_mutex_lock(&g_peer.mu);
    bool close = g_peer.close_now && !g_peer.closed;
    pthread_mutex_unlock(&g_peer.mu);
    return close;
}

static void peer_close_accepted(void)
{
    pthread_mutex_lock(&g_peer.mu);
    g_peer.closed = true;
    pthread_mutex_unlock(&g_peer.mu);
}

#ifdef END_BACKEND_WT
typedef moq_pico_wt_managed_t peer_facade_t;
static int peer_pump(moq_pico_wt_managed_t *m, uint64_t now_us, void *ctx)
{
    (void)ctx;
    moq_session_t *s = moq_pico_wt_managed_session(m);
    if (!s || moq_session_state(s) != MOQ_SESS_ESTABLISHED) return 0;
    peer_events(s, now_us);
    if (peer_close_requested() && moq_session_close(s, 0, "peer close", now_us) == MOQ_OK)
        peer_close_accepted();
    return 0;
}
#else
typedef moq_pq_threaded_t peer_facade_t;
static int peer_pump(moq_pq_threaded_t *t, moq_pq_threaded_lane_t *lane,
                     uint64_t now_us, void *ctx)
{
    (void)t; (void)ctx;
    for (moq_pq_threaded_conn_t *c = moq_pq_threaded_lane_next_conn(lane, NULL); c;
         c = moq_pq_threaded_lane_next_conn(lane, c)) {
        moq_session_t *s = moq_pq_threaded_conn_session(c);
        if (!s || moq_session_state(s) != MOQ_SESS_ESTABLISHED) continue;
        peer_events(s, now_us);
        if (peer_close_requested() && moq_pq_threaded_conn_close(c, 0) == MOQ_OK)
            peer_close_accepted();
    }
    return 0;
}
#endif

static peer_facade_t *peer_start(const char *cert, const char *key, int *out_port)
{
    /* Port range disjoint from the other service tests. */
    static int calls = 0;
    int base = 20900 + (int)(getpid() % 997) + calls++ * 37;
    for (int attempt = 0; attempt < 8; attempt++) {
        int port = base + attempt * 11;
#ifdef END_BACKEND_WT
        moq_pico_wt_managed_cfg_t cfg;
        moq_pico_wt_managed_cfg_init_sized(&cfg, sizeof(cfg));
        cfg.alloc = moq_alloc_default();
        cfg.perspective = MOQ_PERSPECTIVE_SERVER;
        cfg.cert_path = cert;
        cfg.key_path = key;
        cfg.port = port;
        cfg.path = "/moq";
        cfg.wt_protocols = "moqt-18, moqt-16";
        cfg.send_request_capacity = true;
        cfg.initial_request_capacity = 16;
        cfg.on_pump = peer_pump;
        moq_pico_wt_managed_t *t = NULL;
        if (moq_pico_wt_managed_create(&cfg, &t) == MOQ_OK) {
#else
        moq_pq_threaded_cfg_t cfg;
        moq_pq_threaded_cfg_init_sized(&cfg, sizeof(cfg));
        cfg.alloc = moq_alloc_default();
        cfg.perspective = MOQ_PERSPECTIVE_SERVER;
        cfg.cert_path = cert;
        cfg.key_path = key;
        cfg.port = port;
        cfg.send_request_capacity = true;
        cfg.initial_request_capacity = 16;
        cfg.on_lane_pump = peer_pump;
        moq_pq_threaded_t *t = NULL;
        if (moq_pq_threaded_create(&cfg, &t) == MOQ_OK) {
#endif
            *out_port = port;
            return t;
        }
    }
    return NULL;
}

#ifdef END_BACKEND_WT
#define peer_wake     moq_pico_wt_managed_wake
#define peer_wait     moq_pico_wt_managed_wait
#define peer_stop     moq_pico_wt_managed_stop
#define peer_destroy  moq_pico_wt_managed_destroy
#define URL_FORMAT    "https://127.0.0.1:%d/moq"
#else
#define peer_wake     moq_pq_threaded_wake
#define peer_wait     moq_pq_threaded_wait
#define peer_stop     moq_pq_threaded_stop
#define peer_destroy  moq_pq_threaded_destroy
#define URL_FORMAT    "moqt://127.0.0.1:%d"
#endif

/* -- the endpoint's network thread: hold and park -------------------- */

typedef struct {
    pthread_mutex_t mu;
    pthread_cond_t  cv;
    moq_endpoint_t *ep;
    bool            hook_installed;
    /* Stage 2 hold: the next pass after it published its drain state. */
    bool            hold_next, held, hold_release, hold_ended, hold_timed_out;
    int             hold_skipped;      /* passes passed over: not DONE */
    /* The old-pass row: hold a pass at stage 1 (its pump over), then park
     * that SAME pass at stage 2, after it published its drain state. */
    bool            hold1_next, held1, hold1_release, hold1_timed_out;
    bool            old_pass_at_stage2, parked2, park2_release, park2_timed_out;
    facts_t park2_facts;
    /* The drain thread: created through spawn (a control replaces it). */
    int             joins;
    /* Stage 1 park: the first pass whose pump published drain request
     * park_want as quiet, after its pump and before its post-pump service. */
    bool            park_armed, parked, park_release, park_ended, park_timed_out;
    uint64_t        park_want;
    facts_t park_facts;
    /* The drain under test, on its own thread. */
    bool            drain_returned;
    moq_result_t    drain_rc;
    bool            returned_while_parked, returned_while_held;
} fix_t;

static fix_t g;
static void lock(void) { pthread_mutex_lock(&g.mu); }
static void unlock(void) { pthread_mutex_unlock(&g.mu); }

static uint64_t mono_us(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000u + (uint64_t)ts.tv_nsec / 1000u;
}

/* Under g.mu: wait for *flag within BOUND_MS. */
static bool wait_flag(bool *flag)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += BOUND_MS / 1000;
    int rc = 0;
    while (!*flag && rc == 0) rc = pthread_cond_timedwait(&g.cv, &g.mu, &ts);
    return *flag;
}

static void stage_hook(facade_t *t, int stage, void *ctx)
{
    (void)ctx;
    if (stage == 1) {
        lock();
        if (g.hold1_next) {
            g.hold1_next = false;
            g.held1 = true;
            pthread_cond_broadcast(&g.cv);
            if (!wait_flag(&g.hold1_release)) g.hold1_timed_out = true;
            g.held1 = false;
            g.old_pass_at_stage2 = !g.hold1_timed_out;
            pthread_cond_broadcast(&g.cv);
        }
        unlock();
    }
    if (stage == 2) {
        lock();
        bool old_pass = g.old_pass_at_stage2;
        g.old_pass_at_stage2 = false;
        unlock();
        if (old_pass) {
            /* Exactly this pass, whatever it published: no skip. */
            facts_t of;
            facade_drain_facts(t, &of);
            lock();
            g.park2_facts = of;
            g.parked2 = true;
            pthread_cond_broadcast(&g.cv);
            if (!wait_flag(&g.park2_release)) g.park2_timed_out = true;
            g.parked2 = false;
            pthread_cond_broadcast(&g.cv);
            unlock();
            return;
        }
    }
    if (stage == 2) {
        /* Hold only a pass that published DONE: its own drain inputs all
         * quiet and no wake since it began. A pass the test's wake overtook
         * published nothing; the pending wake brings another. */
        lock();
        bool want_hold = g.hold_next;
        unlock();
        if (!want_hold) return;
        facts_t hf;
        facade_drain_facts(t, &hf);
        bool published_done = hf.ready && !hf.session_pending && !hf.bridge_pending &&
                              hf.backlog_empty && hf.acked && hf.gen_unchanged;
        if (!published_done) {
            lock(); g.hold_skipped++; unlock();
            return;
        }
        lock();
        if (g.hold_next) {
            g.hold_next = false;
            g.held = true;
            pthread_cond_broadcast(&g.cv);
            if (!wait_flag(&g.hold_release)) g.hold_timed_out = true;
            g.held = false;
            g.hold_ended = true;
            pthread_cond_broadcast(&g.cv);
        }
        unlock();
        return;
    }
    if (stage != 1) return;
    lock();
    bool armed = g.park_armed && !g.park_ended && !g.parked;
    uint64_t want = g.park_want;
    unlock();
    if (!armed || moq_endpoint_test_drain_quiet(g.ep) < want) return;
    facts_t f;
    facade_drain_facts(t, &f);
    lock();
    g.park_facts = f;
    g.parked = true;
    pthread_cond_broadcast(&g.cv);
    if (!wait_flag(&g.park_release)) g.park_timed_out = true;
    g.parked = false;
    g.park_ended = true;
    pthread_cond_broadcast(&g.cv);
    unlock();
}

/* Installed by a posted task: on the network thread, the facade's owner. */
static moq_result_t install_hook(moq_endpoint_t *ep, moq_session_t *session,
                                 uint64_t now_us, void *ctx)
{
    (void)now_us; (void)ctx;
    if (!session) return MOQ_OK;                 /* closed marker */
    facade_set_stage_hook(
        (facade_t *)moq_endpoint_test_facade(ep), stage_hook, NULL);
    lock(); g.hook_installed = true; pthread_cond_broadcast(&g.cv); unlock();
    return MOQ_OK;
}

static void *drain_thread(void *arg)
{
    (void)arg;
    moq_result_t rc = moq_endpoint_drain(g.ep, 20000000);
    lock();
    g.drain_rc = rc;
    g.drain_returned = true;
    g.returned_while_parked = g.parked;
    g.returned_while_held = g.held;
    pthread_cond_broadcast(&g.cv);
    unlock();
    return NULL;
}

/* The drain thread is created through this, so a control can refuse it. */
static int (*spawn)(pthread_t *, const pthread_attr_t *, void *(*)(void *), void *) =
    pthread_create;
static int refuse_spawn(pthread_t *th, const pthread_attr_t *attr,
                        void *(*fn)(void *), void *arg)
{
    (void)th; (void)attr; (void)fn; (void)arg;
    return EAGAIN;
}

/* The drain is still running after it has kicked the facade at least `n`
 * more times: each kick follows a full quiet + probe evaluation that did not
 * succeed. Bounded by a monotonic deadline. */
static bool drain_kicks_while_pending(facade_t *f, unsigned n)
{
    uint64_t g0 = facade_drain_gen(f);
    uint64_t end = mono_us() + (uint64_t)BOUND_MS * 1000u;
    while (mono_us() < end) {
        lock(); bool returned = g.drain_returned; unlock();
        if (returned) return false;
        if (facade_drain_gen(f) >= g0 + n) {
            lock(); returned = g.drain_returned; unlock();
            return !returned;
        }
        struct timespec ts = { 0, 1000000 };
        nanosleep(&ts, NULL);                    /* polling an observed count */
    }
    return false;
}

/* -- the row ---------------------------------------------------------- */

static moq_rcbuf_t *mkbuf(size_t n, uint8_t fill)
{
    uint8_t tmp[256];
    memset(tmp, fill, n < sizeof(tmp) ? n : sizeof(tmp));
    moq_rcbuf_t *b = NULL;
    moq_rcbuf_create(moq_alloc_default(), tmp, n < sizeof(tmp) ? n : sizeof(tmp), &b);
    return b;
}

static bool write_objects(moq_media_sender_t *s, moq_media_track_t *v)
{
    for (int i = 0; i < OBJECTS; i++) {
        moq_rcbuf_t *b = mkbuf(64, (uint8_t)i);
        if (!b) return false;
        moq_media_send_object_t o;
        memset(&o, 0, sizeof(o));
        o.struct_size = sizeof(o);
        o.payload = b;
        o.is_sync = o.starts_group = i % 4 == 0;
        o.ends_group = i % 4 == 3;
        moq_result_t rc = moq_media_sender_write(s, v, &o);
        if (rc != MOQ_OK) { moq_rcbuf_decref(b); return false; }
    }
    return true;
}

static bool wait_peer(int *field, int want)
{
    for (uint64_t end = mono_us() + (uint64_t)BOUND_MS * 1000u; mono_us() < end;) {
        pthread_mutex_lock(&g_peer.mu);
        int v = *field;
        pthread_mutex_unlock(&g_peer.mu);
        if (v >= want) return true;
        struct timespec ts = { 0, 10000000 };
        nanosleep(&ts, NULL);                    /* polling an observed count */
    }
    return false;
}

static const char *mode_name(row_mode_t m)
{
    return m == MODE_SUCCESS ? "end_handoff" :
           m == MODE_INTERRUPT ? "end_handoff_interrupt" :
           m == MODE_PEER_CLOSE ? "end_handoff_peer_close" :
           m == MODE_OLD_PASS ? "old_pass_publication" : "drain_thread_refused";
}

/* The old pass: a pass whose pump is over when END is queued and its wake
 * lands. Held at stage 1, then parked at stage 2 of that same pass: its
 * post-service drain inputs are all quiet, yet a wake overtook it, so the
 * public probe must read PENDING -- the facade's publication rejects a pass
 * that began before the work. Released, a later pump hands END over and the
 * drain succeeds. */
static void run_old_pass(moq_endpoint_t *ep, moq_media_sender_t *s,
                         moq_media_track_t *v, facade_t *f)
{
    lock();
    g.hold1_next = true;
    moq_endpoint_wake(ep);
    bool held1 = wait_flag(&g.held1);
    unlock();
    MOQ_TEST_CHECK(held1);
    if (!held1) return;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_end_track(s, v), (int)MOQ_OK);
    lock();
    bool active = g.held1;                       /* still held: same pass */
    if (active) {
        g.hold1_release = true;
        pthread_cond_broadcast(&g.cv);
    }
    bool parked = active && wait_flag(&g.parked2);
    facts_t of = g.park2_facts;
    int probe = parked ? facade_drain_state(f) : -99;
    unlock();
    MOQ_TEST_CHECK(active);
    MOQ_TEST_CHECK(parked);
    if (parked) {
        /* Quiet after its service: END was never handed to it. */
        MOQ_TEST_CHECK(of.ready);
        MOQ_TEST_CHECK(!of.session_pending);
        MOQ_TEST_CHECK(!of.bridge_pending);
        MOQ_TEST_CHECK(of.backlog_empty);
        MOQ_TEST_CHECK(of.acked);
        MOQ_TEST_CHECK(!of.gen_unchanged);       /* overtaken by END's wake */
        MOQ_TEST_CHECK_EQ_INT(probe, 0);         /* PENDING, not a stale DONE */
    }
    pthread_mutex_lock(&g_peer.mu);
    int finished = g_peer.finished;
    pthread_mutex_unlock(&g_peer.mu);
    MOQ_TEST_CHECK_EQ_INT(finished, 0);
    lock();
    g.park2_release = true;
    pthread_cond_broadcast(&g.cv);
    unlock();
    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_drain(ep, 10000000), (int)MOQ_OK);
    bool fin = wait_peer(&g_peer.finished, 1);
    MOQ_TEST_CHECK(fin);
    lock();
    MOQ_TEST_CHECK(!g.hold1_timed_out);
    MOQ_TEST_CHECK(!g.park2_timed_out);
    unlock();
}

static void run_row(const char *cert, const char *key, row_mode_t mode)
{
    int before = failures;
    memset(&g, 0, sizeof(g));
    pthread_mutex_init(&g.mu, NULL);
    pthread_cond_init(&g.cv, NULL);
    memset(&g_peer, 0, sizeof(g_peer));
    pthread_mutex_init(&g_peer.mu, NULL);
    moq_endpoint_t *ep = NULL;
    moq_media_sender_t *s = NULL;
    facade_t *f = NULL;
    pthread_t th;
    bool th_started = false;

    int port = 0;
    peer_facade_t *srv = peer_start(cert, key, &port);
    MOQ_TEST_CHECK(srv != NULL);
    if (!srv) goto out;

    char url[64];
    snprintf(url, sizeof(url), URL_FORMAT, port);
    moq_endpoint_cfg_t ec;
    moq_endpoint_cfg_init(&ec);
    ec.url = (moq_bytes_t){ (const uint8_t *)url, strlen(url) };
    ec.insecure_skip_verify = true;
    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_connect(&ec, &ep), (int)MOQ_OK);
    if (!ep) goto out;
    g.ep = ep;
    f = (facade_t *)moq_endpoint_test_facade(ep);
    MOQ_TEST_CHECK(f != NULL);
    if (!f) goto out;

    moq_bytes_t parts[2] = { MOQ_BYTES_LITERAL("svc"), MOQ_BYTES_LITERAL("demo") };
    moq_media_sender_cfg_t cfg;
    moq_media_sender_cfg_init_live_sized(&cfg, sizeof(cfg));
    cfg.namespace_ = (moq_namespace_t){ parts, 2 };
    cfg.publish_tracks = true;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_attach(ep, &cfg, &s), (int)MOQ_OK);
    if (!s) goto out;
    moq_media_track_cfg_t tc;
    moq_media_track_cfg_init(&tc);
    tc.name = MOQ_BYTES_LITERAL("v");
    tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
    tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
    tc.codec = MOQ_BYTES_LITERAL("av01");
    tc.bitrate = 1500000;
    tc.width = 1280;
    tc.height = 720;
    tc.timescale = 1000000;
    moq_media_track_t *v = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(s, &tc, &v), (int)MOQ_OK);
    if (!v) goto out;
    bool ready = false;
    for (uint64_t end = mono_us() + (uint64_t)BOUND_MS * 1000u; !ready && mono_us() < end;) {
        ready = moq_media_sender_is_ready(s);
        if (!ready) moq_endpoint_wait(ep, 20000);
    }
    MOQ_TEST_CHECK(ready);
    if (!ready) goto out;

    /* Baseline: the objects are written, drained and delivered. */
    bool wrote = write_objects(s, v);
    MOQ_TEST_CHECK(wrote);
    if (!wrote) goto out;
    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_drain(ep, 10000000), (int)MOQ_OK);
    bool delivered = wait_peer(&g_peer.objs, OBJECTS);
    MOQ_TEST_CHECK(delivered);
    if (!delivered) goto out;

    /* The stage hook, installed on the facade's own thread. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_post(ep, install_hook, NULL), (int)MOQ_OK);
    lock();
    bool hooked = wait_flag(&g.hook_installed);
    unlock();
    MOQ_TEST_CHECK(hooked);
    if (!hooked) goto out;

    if (mode == MODE_OLD_PASS) {
        run_old_pass(ep, s, v, f);
        goto out;
    }

    /* Stale idle. Hold the next pass after it published DONE. The hold
     * request and its wake are one step under g.mu: a pass that sampled its
     * facts before the wake cannot be held (its generation changed), so the
     * held pass's DONE was published after the wake. Lock order: g.mu, then
     * the facade's mutex inside the wake; the network thread never holds
     * that mutex (or ep->mu) while it takes g.mu. */
    lock();
    g.hold_next = true;
    moq_endpoint_wake(ep);
    bool held = wait_flag(&g.held);
    unlock();
    MOQ_TEST_CHECK(held);
    if (!held) goto out;
    int latched = facade_drain_state(f);
    MOQ_TEST_CHECK_EQ_INT(latched, 1);            /* idle DONE, latched */
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_end_track(s, v), (int)MOQ_OK);
    int after_end = facade_drain_state(f);
    MOQ_TEST_CHECK_EQ_INT(after_end, 0);          /* not reused after END queued */
    if (mode == MODE_SPAWN_FAIL) spawn = refuse_spawn;
    int sc = spawn(&th, NULL, drain_thread, NULL);
    spawn = pthread_create;
    if (mode == MODE_SPAWN_FAIL) {
        /* The control: a refused creation unwinds with nothing to join. */
        MOQ_TEST_CHECK(sc != 0);
        goto out;
    }
    MOQ_TEST_CHECK(sc == 0);
    if (sc != 0) goto out;
    th_started = true;
    /* This drain's request is the second (the baseline took the first). */
    uint64_t want = 0;
    for (uint64_t end = mono_us() + (uint64_t)BOUND_MS * 1000u; mono_us() < end;) {
        want = moq_endpoint_test_drain_req(ep);
        if (want >= 2) break;
        struct timespec ts = { 0, 1000000 };
        nanosleep(&ts, NULL);                    /* polling an observed count */
    }
    MOQ_TEST_CHECK_EQ_U64(want, 2);
    if (want != 2) goto out;
    bool pending_held = drain_kicks_while_pending(f, 3);
    MOQ_TEST_CHECK(pending_held);                 /* probing, not succeeding */
    MOQ_TEST_CHECK(moq_endpoint_test_drain_quiet(ep) < want);   /* no service quiet */
    lock();
    MOQ_TEST_CHECK(g.held);                       /* still held: no pass ran */
    int finished_before = 0;
    unlock();
    pthread_mutex_lock(&g_peer.mu);
    finished_before = g_peer.finished;
    pthread_mutex_unlock(&g_peer.mu);
    MOQ_TEST_CHECK_EQ_INT(finished_before, 0);

    /* END handoff: release the hold with the stage-1 park armed for the pass
     * that publishes this request as quiet. */
    lock();
    g.park_want = want;
    g.park_armed = true;
    g.hold_release = true;
    pthread_cond_broadcast(&g.cv);
    bool parked = wait_flag(&g.parked);
    facts_t pf = g.park_facts;
    unlock();
    MOQ_TEST_CHECK(parked);
    if (!parked) goto out;
    MOQ_TEST_CHECK(moq_endpoint_test_drain_quiet(ep) >= want);  /* quiet published */
    MOQ_TEST_CHECK(pf.session_pending);           /* END in the session, unserviced */
    bool pending_parked = drain_kicks_while_pending(f, 3);
    lock();
    bool still_parked = g.parked;
    unlock();
    pthread_mutex_lock(&g_peer.mu);
    int finished_parked = g_peer.finished;
    pthread_mutex_unlock(&g_peer.mu);
    if (mode == MODE_SUCCESS) {
        MOQ_TEST_CHECK(pending_parked);           /* quiet, yet not succeeding */
        MOQ_TEST_CHECK(still_parked);
        MOQ_TEST_CHECK_EQ_INT(facade_drain_state(f), 0);
        MOQ_TEST_CHECK_EQ_INT(finished_parked, 0);
        lock(); g.park_release = true; pthread_cond_broadcast(&g.cv);
        bool returned = wait_flag(&g.drain_returned);
        moq_result_t rc = g.drain_rc;
        bool early = g.returned_while_parked || g.returned_while_held;
        unlock();
        MOQ_TEST_CHECK(returned);
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
        MOQ_TEST_CHECK(!early);
        bool fin = wait_peer(&g_peer.finished, 1);
        MOQ_TEST_CHECK(fin);
        pthread_mutex_lock(&g_peer.mu);
        MOQ_TEST_CHECK_EQ_INT(g_peer.finished, 1);
        MOQ_TEST_CHECK_EQ_U64(g_peer.fin_status, 0x2);   /* TRACK_ENDED */
        MOQ_TEST_CHECK_EQ_U64(g_peer.fin_streams, 3);
        MOQ_TEST_CHECK_EQ_INT(g_peer.objs, OBJECTS);
        pthread_mutex_unlock(&g_peer.mu);
    } else if (mode == MODE_INTERRUPT) {
        MOQ_TEST_CHECK(pending_parked);
        moq_endpoint_set_interrupted(ep, true);
        lock();
        bool returned = wait_flag(&g.drain_returned);
        moq_result_t rc = g.drain_rc;
        bool while_parked = g.returned_while_parked;
        g.park_release = true;
        pthread_cond_broadcast(&g.cv);
        unlock();
        MOQ_TEST_CHECK(returned);
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_ERR_INTERRUPTED);
        MOQ_TEST_CHECK(while_parked);             /* the latch wins at once */
    } else {
        MOQ_TEST_CHECK(pending_parked);
        /* The peer closes the connection; then the parked pass resumes. */
        pthread_mutex_lock(&g_peer.mu);
        g_peer.close_now = true;
        pthread_mutex_unlock(&g_peer.mu);
        peer_wake(srv);
        bool closed = false;
        for (uint64_t end = mono_us() + (uint64_t)BOUND_MS * 1000u; !closed && mono_us() < end;) {
            pthread_mutex_lock(&g_peer.mu);
            closed = g_peer.closed;
            pthread_mutex_unlock(&g_peer.mu);
            if (!closed) peer_wait(srv, 10000);
        }
        MOQ_TEST_CHECK(closed);
        lock(); g.park_release = true; pthread_cond_broadcast(&g.cv);
        bool returned = wait_flag(&g.drain_returned);
        moq_result_t rc = g.drain_rc;
        unlock();
        MOQ_TEST_CHECK(returned);
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_ERR_CLOSED);   /* never success */
    }
    lock();
    MOQ_TEST_CHECK(!g.hold_timed_out);
    MOQ_TEST_CHECK(!g.park_timed_out);
    unlock();

out:
    lock();
    g.hold_release = g.park_release = true;
    pthread_cond_broadcast(&g.cv);
    unlock();
    lock();
    g.hold1_release = g.park2_release = true;
    pthread_cond_broadcast(&g.cv);
    unlock();
    if (th_started) {
        if (ep) moq_endpoint_set_interrupted(ep, true);   /* bound the join */
        pthread_join(th, NULL);
        g.joins++;
    }
    if (s) moq_media_sender_destroy(s);
    moq_result_t stop_rc = MOQ_OK;
    if (ep) { stop_rc = moq_endpoint_stop(ep); moq_endpoint_destroy(ep); }
    if (mode == MODE_SPAWN_FAIL) {
        /* Unwound: no join, the held pass released (not timed out), and the
         * endpoint stopped cleanly with its network thread joined. */
        MOQ_TEST_CHECK_EQ_INT(g.joins, 0);
        MOQ_TEST_CHECK(g.hold_ended && !g.hold_timed_out);
        MOQ_TEST_CHECK_EQ_INT((int)stop_rc, (int)MOQ_OK);
    }
    if (srv) { peer_stop(srv); peer_destroy(srv); }
    pthread_mutex_destroy(&g_peer.mu);
    pthread_cond_destroy(&g.cv);
    pthread_mutex_destroy(&g.mu);
    printf("%s: endpoint drain (%s) %s\n", failures == before ? "PASS" : "FAIL",
           BACKEND_NAME, mode_name(mode));
}

int main(int argc, char **argv)
{
    const char *cert = NULL, *key = NULL;
    for (int i = 1; i + 1 < argc; i++) {
        if (strcmp(argv[i], "--cert") == 0) cert = argv[++i];
        else if (strcmp(argv[i], "--key") == 0) key = argv[++i];
    }
    if (!cert || !key) { fprintf(stderr, "need --cert/--key\n"); return 1; }
    run_row(cert, key, MODE_SUCCESS);
    run_row(cert, key, MODE_INTERRUPT);
    run_row(cert, key, MODE_PEER_CLOSE);
    run_row(cert, key, MODE_OLD_PASS);
    run_row(cert, key, MODE_SPAWN_FAIL);
    return failures ? 1 : 0;
}
