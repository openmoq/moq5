/*
 * The serve coordinator wakes for its own work, over the production serve.
 *
 * The real serve composition (cmd_serve_lanes over PicoQUIC facades) runs on
 * its own thread with no client, so no packet ever arrives. Each case drives
 * the coordinator only through what wakes it in production -- the dump and
 * stop signal handlers, called directly, and lane row publications. Unless a
 * case says otherwise the health fallback is DISABLED (no deadline), so a lost
 * notification cannot be rescued by the 500 ms health turn: the work it
 * should have caused never happens, and the case fails at its bound.
 *
 * Cases cross admin absent / present with idle / busy lanes (busy: a thread
 * wakes every lane continuously, so lanes pump without packets):
 *
 *   SIGUSR1 -- admin absent: the coordinator converts the latch and renders
 *              exactly one generation; admin present: it forwards the demand
 *              to the endpoint's owner, which consumes the latch and retires
 *              the generation, and the coordinator neither converts nor
 *              renders.
 *   SIGUSR2 -- the coordinator wakes the lanes for the trace epoch.
 *   SIGTERM -- the coordinator ends the serve; some cases keep notifying from
 *              another thread across the shutdown, one through a drain that
 *              never empties (a sustained producer).
 *
 * Further cases:
 *   deferred release -- a generation deferred while both banks were taken
 *              opens when the rendered one is released; the coordinator wakes
 *              the lanes for it (wake_after_release) before it sleeps, and it
 *              renders, with no health fallback. Setup requires an
 *              acknowledged lane-pump boundary before the release. The
 *              release-specific wake is observed through its own call and
 *              ordered against the renders and the coordinator's wait, so a
 *              missing wake fails whatever else pumps the lane.
 *   render retry -- the render of a complete generation fails once; with no
 *              further notification, the health turn retries and renders it.
 *              The coordinator records its counters at the failure and at the
 *              render, so the classification does not depend on when the
 *              driver looks.
 *   late byte -- the render retry with its last row notification held back
 *              and released after the coordinator's drain: the armed failure
 *              waits while that byte is readable, is injected after the
 *              ordinary wake consumed it, and the retry is health-only.
 *   terminal health -- the lane-0 facade is stopped from outside; nothing
 *              writes the self-pipe, and a health turn ends the serve.
 *
 * Lifetime: every case either proves its serve thread finished and joined,
 * or ENDS THE PROCESS -- no later case may reuse state a live serve can still
 * reach. Broker state is only read through the serve's published-context
 * lease. A failed startup goes straight to that settlement, skipping the
 * workload. Counters (moqr_test_coord_counts) are the oracle; the waits
 * bound how long a case may take, in monotonic time, and a condition first
 * seen after its deadline does not count. Only the deferred-release render
 * wait is part of an oracle (see workload_deferred_release).
 *
 * argv: cert key [stuck | control-stuck]
 *   stuck          a first case that never raises stop (its serve stays
 *                  live), then a normal case: the process must end at the
 *                  first case's settlement, before the second starts.
 *   control-stuck  run `stuck` as a child process and check exactly that.
 */

#if defined(__linux__) && !defined(_POSIX_C_SOURCE)
#define _POSIX_C_SOURCE 200809L
#endif

#include "../cli/broker.h"
#include "../cli/config.h"
#include "logevent.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

extern int moqr_test_cmd_serve_lanes(const moqr_cli_config_t *cfg);
extern void moqr_test_serve_log_set_io(const moqr_cli_log_io_t *io);
extern void moqr_test_raise_stop(void);
extern void moqr_test_clear_stop(void);
extern void moqr_test_coord_set_health_us(uint64_t us);
extern void moqr_test_coord_counts(uint64_t out[10]);
extern void moqr_test_coord_wake_notify(void);
extern int moqr_test_coord_wake_wr(void);
extern void moqr_test_dump_signal(int sig);
extern int moqr_test_signal_metrics_pending(void);
extern bool moqr_test_serve_wake_lanes(void);
extern bool moqr_test_serve_stop_lane0(void);
extern bool moqr_test_serve_broker_busy(bool *live);
extern bool moqr_test_serve_with_broker(void (*fn)(moqr_broker_t *, void *),
                                        void *arg);
extern void moqr_test_set_coord_pinned(void (*fn)(void *), void *ctx);
extern void moqr_test_coord_set_read(ssize_t (*fn)(int, void *, size_t));
extern void moqr_test_coord_fail_renders(unsigned n);
extern unsigned moqr_test_coord_render_failures_left(void);
extern bool moqr_test_serve_lane0_pump_boundary(int max_ms);
extern void moqr_test_coord_fail_at(uint64_t out[3]);
extern void moqr_test_coord_render_at(uint64_t out[3]);
extern void moqr_test_coord_release_wake(uint64_t out[5]);
extern bool moqr_test_coord_render_mark(unsigned i, uint64_t out[3]);
extern void moqr_test_coord_retry_boundary(uint64_t out[12]);
extern void moqr_test_set_publish_gate(void (*fn)(void *), void *ctx);
extern void moqr_test_set_coord_midflight(void (*fn)(void *), void *ctx);
extern bool moqr_test_coord_wake_readable(void);
extern uint64_t moqr_test_coord_now_us(void);

enum { C_NOTIFIED, C_COALESCED, C_FAILED, C_TURNS, C_PIPE, C_HEALTH,
       C_RENDERS, C_CONVERSIONS, C_FORWARDS, C_TRACE };

static int g_failures;
#define CHECK(expr) do { if (!(expr)) { \
    fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
    g_failures++; \
} } while (0)

static int g_bound_ms = 20000;

/* A serve that cannot be proven finished and joined may still reach every
 * piece of process state a later case would reuse: end the process here. */
static void
die_unsettled(const char *why)
{
    fprintf(stderr, "FAIL: %s; ending the process before any further case\n",
            why);
    printf("UNSETTLED: %s\n", why);
    fflush(stdout);
    fflush(stderr);
    _exit(3);
}

static uint64_t
count(int k)
{
    uint64_t c[10];
    moqr_test_coord_counts(c);
    return c[k];
}

/* Poll `pred` until it holds or `ms` of monotonic time elapse. A predicate
 * counts only if it was seen true by the deadline: the clock is read after
 * each evaluation, so a predicate that became true only after the deadline
 * (a late fallback, a descheduled poller) is refused, never accepted. */
static bool
await(bool (*pred)(void *), void *arg, int ms)
{
    const uint64_t deadline =
        moqr_test_coord_now_us() + (uint64_t)(ms > 0 ? ms : 0) * 1000u;
    for (;;) {
        const bool held = pred(arg);
        const uint64_t seen = moqr_test_coord_now_us();
        if (held) {
            return seen <= deadline;
        }
        if (seen > deadline) {
            return false;
        }
        struct timespec ts = { 0, 2 * 1000 * 1000 };
        nanosleep(&ts, NULL);
    }
}

/* -- the serve, on its own thread ------------------------------------------ */

static atomic_int g_ready;

static size_t
io_write(void *ctx, FILE *f, const char *p, size_t n)
{
    (void)ctx;
    return fwrite(p, 1, n, f);
}

static int
io_flush(void *ctx, FILE *f)
{
    (void)ctx;
    int rc = fflush(f);
    atomic_store(&g_ready, 1);
    return rc;
}

static const moqr_cli_log_io_t kServeIo = {
    .clock_us = NULL,
    .write = io_write,
    .flush = io_flush,
    .ctx = NULL,
};

typedef struct serve_run {
    moqr_cli_config_t cfg;
    int               rc;
    atomic_int        done;
} serve_run_t;

static void *
serve_main(void *arg)
{
    serve_run_t *r = arg;
    r->rc = moqr_test_cmd_serve_lanes(&r->cfg);
    atomic_store(&r->done, 1);
    return NULL;
}

static uint16_t
free_port(int type)
{
    int fd = socket(AF_INET, type, 0);
    if (fd < 0) {
        return 0;
    }
    struct sockaddr_in a;
    memset(&a, 0, sizeof(a));
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    socklen_t len = sizeof(a);
    uint16_t port = 0;
    if (bind(fd, (struct sockaddr *)&a, sizeof(a)) == 0 &&
        getsockname(fd, (struct sockaddr *)&a, &len) == 0) {
        port = ntohs(a.sin_port);
    }
    close(fd);
    return port;
}

/* -- background drivers ------------------------------------------------------ */

static atomic_int g_busy_run;
static atomic_int g_spam_run;

static void *
busy_main(void *arg)
{
    (void)arg;
    while (atomic_load(&g_busy_run)) {
        (void)moqr_test_serve_wake_lanes();
        sched_yield();
    }
    return NULL;
}

static void *
spam_main(void *arg)
{
    (void)arg;
    while (atomic_load(&g_spam_run)) {
        moqr_test_coord_wake_notify();
        sched_yield();
    }
    return NULL;
}

/* A drain read that always finds data: a producer as fast as the reader. */
static ssize_t
read_always_data(int fd, void *buf, size_t n)
{
    (void)fd;
    memset(buf, 1, n);
    return (ssize_t)n;
}

/* -- predicates -------------------------------------------------------------- */

static bool
p_ready(void *a)
{
    (void)a;
    return atomic_load(&g_ready) && moqr_test_serve_wake_lanes();
}

static bool
p_done(void *a)
{
    return atomic_load(&((serve_run_t *)a)->done) != 0;
}

typedef struct { int k; uint64_t above; } above_t;

static bool
p_above(void *a)
{
    const above_t *x = a;
    return count(x->k) > x->above;
}

/* Under the lease: the broker is idle while the serve is published. */
static bool
p_broker_idle(void *a)
{
    (void)a;
    bool live = false;
    bool busy = moqr_test_serve_broker_busy(&live);
    return live && !busy;
}

static bool
p_admin_retired(void *a)
{
    return moqr_test_signal_metrics_pending() == 0 && p_broker_idle(a);
}

static bool
p_failure_consumed(void *a)
{
    (void)a;
    return moqr_test_coord_render_failures_left() == 0u;
}

/* -- the deferred-release fixture (all of it under the lease) --------------- */

typedef struct {
    uint64_t      hold;
    uint32_t      bank;
    moqr_result_t rc;
    bool          taken;
} hold_t;

static void
hold_second_bank(moqr_broker_t *b, void *arg)
{
    hold_t *h = arg;
    bool w = false;
    uint32_t d = 0;
    h->rc = moqr_broker_request(b, MOQR_BROKER_DEMAND_HTTP, &h->hold, &w);
    if (h->rc == MOQR_OK) {
        moqr_broker_on_complete(b, h->hold);
        h->taken = moqr_broker_take_serial(b, h->hold, &d, &h->bank);
    }
}

static void
release_second_bank(moqr_broker_t *b, void *arg)
{
    hold_t *h = arg;
    bool w = false;
    h->rc = moqr_broker_release(b, h->hold, h->bank, &w);
}

static atomic_int g_defer_calls;
static atomic_int g_defer_rc;
static atomic_int g_defer_boundary;   /* the pump boundary was acknowledged */

/* Runs on the coordinator, inside the retirement of the generation it
 * rendered: that bank is pinned and the other is held, so this SIGNAL demand
 * is refused and remembered -- opened by the release that follows.
 *
 * The turn that rendered began by waking the lanes for the outstanding
 * generation. A pump from that wake still running when the release opens
 * the deferred generation would publish for it, standing in for the release
 * wake under test. So before the release the hook requires an acknowledged
 * lane-pump boundary: a lane pump begun after this point has finished, and
 * with it every earlier one; each found nothing new to publish (the
 * deferred generation is not open yet). Unacknowledged, the case fails at
 * setup. The boundary does not cover a pump a wake still pending here causes
 * later, nor the lane's own timer, either of which could publish the deferred
 * row; the release wake is therefore asserted directly, through the
 * coordinator's observation of that call (workload_deferred_release). */
static void
defer_one_signal(void *ctx)
{
    if (atomic_fetch_add(&g_defer_calls, 1) != 0) {
        return;
    }
    moqr_broker_t *b = ctx;
    uint64_t s = 0;
    bool w = false;
    atomic_store(&g_defer_rc, (int)moqr_broker_request(
                                  b, MOQR_BROKER_DEMAND_SIGNAL, &s, &w));
    atomic_store(&g_defer_boundary,
                 moqr_test_serve_lane0_pump_boundary(2000) ? 1 : 0);
}

static void
pass_broker(moqr_broker_t *b, void *arg)
{
    *(moqr_broker_t **)arg = b;
}

/* -- one serve case ------------------------------------------------------------ */

typedef enum {
    W_SIGNALS,          /* SIGUSR1, SIGUSR2, then SIGTERM */
    W_DEFERRED_RELEASE,
    W_RENDER_RETRY,
    W_LATE_BYTE,        /* render retry, the last notification after a drain */
    W_TERMINAL_HEALTH,
    W_STUCK,            /* never raises stop */
} work_t;

typedef struct {
    work_t work;
    bool admin;
    bool busy;
    bool spam_at_stop;     /* notify from another thread across shutdown */
    bool sustained_drain;  /* the drain always finds data at shutdown */
} case_t;

static int g_case_no;

/* Prove the serve finished and join it, or end the process. */
static void
settle(pthread_t th, serve_run_t *sr)
{
    if (!await(p_done, sr, g_bound_ms)) {
        die_unsettled("serve did not return");
    }
    if (pthread_join(th, NULL) != 0) {
        die_unsettled("serve thread join failed");
    }
}

static void
join_or_die(pthread_t th, const char *what)
{
    if (pthread_join(th, NULL) != 0) {
        die_unsettled(what);
    }
}

static void
workload_signals(const case_t *c)
{
    const uint64_t renders = count(C_RENDERS);
    const uint64_t conv = count(C_CONVERSIONS);
    const uint64_t fwd = count(C_FORWARDS);
    moqr_test_dump_signal(SIGUSR1);
    if (c->admin) {
        above_t a = { C_FORWARDS, fwd };
        CHECK(await(p_above, &a, g_bound_ms));
        CHECK(await(p_admin_retired, NULL, g_bound_ms));
        CHECK(count(C_CONVERSIONS) == conv);   /* the owner's latch */
        CHECK(count(C_RENDERS) == renders);    /* the owner rendered */
    } else {
        above_t a = { C_RENDERS, renders };
        CHECK(await(p_above, &a, g_bound_ms));
        CHECK(count(C_RENDERS) == renders + 1u);   /* exactly one */
        CHECK(count(C_CONVERSIONS) == conv + 1u);
        CHECK(count(C_FORWARDS) == fwd);
        CHECK(moqr_test_signal_metrics_pending() == 0);
        CHECK(await(p_broker_idle, NULL, g_bound_ms));
    }
    above_t t = { C_TRACE, count(C_TRACE) };
    moqr_test_dump_signal(SIGUSR2);
    CHECK(await(p_above, &t, g_bound_ms));
}

static void
workload_deferred_release(void)
{
    hold_t h = { 0, 0, MOQR_OK, false };
    CHECK(moqr_test_serve_with_broker(hold_second_bank, &h));
    CHECK(h.rc == MOQR_OK && h.taken);
    moqr_broker_t *b = NULL;
    CHECK(moqr_test_serve_with_broker(pass_broker, &b));
    atomic_store(&g_defer_calls, 0);
    atomic_store(&g_defer_rc, (int)MOQR_OK);
    atomic_store(&g_defer_boundary, 0);
    moqr_test_set_coord_pinned(defer_one_signal, b);
    const uint64_t renders = count(C_RENDERS);
    moqr_test_dump_signal(SIGUSR1);
    /* The rendered generation and the deferred one its release opened, the
     * second within 4 s of monotonic time. An idle PicoQUIC lane also pumps
     * on its own timer, which would publish the deferred row as well; the
     * packet loop caps that wait at 10 s (a maximum, not a promise: the
     * provider may shorten it), and without the release wake the row was
     * measured to arrive at about 10 s. So this bound separates the release
     * wake from that observed timer; it does not exclude every other pump. */
    above_t a = { C_RENDERS, renders + 1u };
    const bool rendered = await(p_above, &a, 4000);
    CHECK(atomic_load(&g_defer_calls) >= 1);
    if (atomic_load(&g_defer_boundary) == 0) {
        fprintf(stderr, "FAIL: setup: lane-pump boundary not acknowledged "
                        "before the release\n");
        g_failures++;
    }
    CHECK(rendered);
    CHECK(atomic_load(&g_defer_rc) == (int)MOQR_ERR_WOULD_BLOCK);
    CHECK(count(C_RENDERS) == renders + 2u);
    CHECK(count(C_HEALTH) == 0u);   /* no fallback turn rescued it */
    /* The release-specific wake itself, observed through its own call's
     * result: without that call nothing is recorded, however the lanes come
     * to pump. It was accepted, ran in the turn whose render retired the
     * first generation -- after that release, before that render was
     * counted (seq), with no wait entered in between (waits) -- and the
     * deferred generation rendered in a later turn, after a wait. */
    uint64_t rw[5] = { 0 }, r0[3] = { 0 }, r1[3] = { 0 };
    moqr_test_coord_release_wake(rw);
    CHECK(moqr_test_coord_render_mark((unsigned)renders, r0));
    CHECK(moqr_test_coord_render_mark((unsigned)renders + 1u, r1));
    CHECK(rw[0] >= 1u);          /* at least one accepted release wake */
    CHECK(rw[1] == 0u);          /* none refused */
    CHECK(rw[2] != 0u && r0[0] != 0u && r1[0] != 0u);
    CHECK(rw[2] < r0[0]);        /* before the first render completed */
    CHECK(rw[3] == r0[1]);       /* in that render's turn */
    CHECK(rw[4] == r0[2]);       /* before that turn's wait */
    CHECK(r1[1] > rw[3] && r1[2] > rw[4]);   /* deferred: a later turn */
    printf("release wake: accepted %llu refused %llu seq %llu turn %llu "
           "waits %llu; render0 seq %llu turn %llu waits %llu; render1 seq "
           "%llu turn %llu waits %llu\n",
           (unsigned long long)rw[0], (unsigned long long)rw[1],
           (unsigned long long)rw[2], (unsigned long long)rw[3],
           (unsigned long long)rw[4], (unsigned long long)r0[0],
           (unsigned long long)r0[1], (unsigned long long)r0[2],
           (unsigned long long)r1[0], (unsigned long long)r1[1],
           (unsigned long long)r1[2]);
    moqr_test_set_coord_pinned(NULL, NULL);
    CHECK(moqr_test_serve_with_broker(release_second_bank, &h));
    CHECK(h.rc == MOQR_OK);
    CHECK(await(p_broker_idle, NULL, g_bound_ms));
}

/* -- the late-byte control ------------------------------------------------- */

/* The lane's notification for its row is held between publication and
 * notification (the publish gate). The coordinator, in a turn begun after
 * that publication -- so after that turn's drain -- releases it from its
 * midflight hook and waits until the notification has been posted. The
 * byte is therefore written after the drain and is still in the pipe when
 * the armed failure's boundary is checked in that same turn. */
static atomic_int g_gate_armed;      /* the next publication is held */
static atomic_int g_gate_held;       /* a lane is waiting at the gate */
static atomic_int g_gate_open;       /* the coordinator released it */
static atomic_int g_gate_timeouts;   /* the lane gave up waiting */
static atomic_int g_late_hook_done;  /* the hook released and saw the post */
static atomic_int g_late_posted;     /* the post completed within its bound */
static atomic_int g_late_readable;   /* the byte was waiting after the post */
static _Atomic uint64_t g_late_hook_turn;

static void
late_publish_gate(void *ctx)
{
    (void)ctx;
    if (!atomic_exchange(&g_gate_armed, 0)) {
        return;
    }
    atomic_store(&g_gate_held, 1);
    const uint64_t deadline = moqr_test_coord_now_us() + UINT64_C(3000000);
    while (!atomic_load(&g_gate_open)) {
        if (moqr_test_coord_now_us() > deadline) {
            atomic_fetch_add(&g_gate_timeouts, 1);
            break;
        }
        struct timespec ts = { 0, 200 * 1000 };
        nanosleep(&ts, NULL);
    }
    atomic_store(&g_gate_held, 0);
}

static bool
late_row_posted(void)
{
    uint64_t rb[12];
    moqr_test_coord_retry_boundary(rb);
    return rb[2] >= 1u && rb[3] == rb[2];
}

/* Coordinator thread, inside a turn, after its drain. */
static void
late_release_hook(void *ctx)
{
    (void)ctx;
    if (!atomic_load(&g_gate_held) || atomic_load(&g_gate_open)) {
        return;
    }
    atomic_store(&g_late_hook_turn, count(C_TURNS));
    atomic_store(&g_gate_open, 1);
    const uint64_t deadline = moqr_test_coord_now_us() + UINT64_C(3000000);
    bool posted = false;
    for (;;) {
        posted = late_row_posted();
        if (posted || moqr_test_coord_now_us() > deadline) {
            break;
        }
        struct timespec ts = { 0, 200 * 1000 };
        nanosleep(&ts, NULL);
    }
    atomic_store(&g_late_posted, posted ? 1 : 0);
    atomic_store(&g_late_readable, moqr_test_coord_wake_readable() ? 1 : 0);
    atomic_store(&g_late_hook_done, 1);
}

/* The render retry (W_RENDER_RETRY), and with `late_byte` its late-byte
 * control (W_LATE_BYTE). */
static void
workload_render_retry(bool late_byte)
{
    const uint64_t renders = count(C_RENDERS);
    const uint64_t conv = count(C_CONVERSIONS);
    if (late_byte) {
        atomic_store(&g_gate_held, 0);
        atomic_store(&g_gate_open, 0);
        atomic_store(&g_gate_timeouts, 0);
        atomic_store(&g_late_hook_done, 0);
        atomic_store(&g_late_posted, 0);
        atomic_store(&g_late_readable, 0);
        atomic_store(&g_late_hook_turn, 0);
        atomic_store(&g_gate_armed, 1);
        moqr_test_set_publish_gate(late_publish_gate, NULL);
        moqr_test_set_coord_midflight(late_release_hook, NULL);
    }
    moqr_test_coord_fail_renders(1);
    moqr_test_dump_signal(SIGUSR1);
    CHECK(await(p_failure_consumed, NULL, g_bound_ms));
    above_t a = { C_RENDERS, renders };
    CHECK(await(p_above, &a, g_bound_ms));
    if (late_byte) {
        moqr_test_set_coord_midflight(NULL, NULL);
        moqr_test_set_publish_gate(NULL, NULL);
        atomic_store(&g_gate_armed, 0);
    }
    CHECK(count(C_RENDERS) == renders + 1u);
    /* The premise: one generation, one row publication from the one lane,
     * and no other signal producer -- so once that row's byte is consumed,
     * nothing but the health turn can end the coordinator's wait. */
    uint64_t rb[12];
    moqr_test_coord_retry_boundary(rb);
    CHECK(count(C_CONVERSIONS) == conv + 1u);
    CHECK(rb[2] == 1u && rb[3] == 1u);
    CHECK(rb[1] == 0u);   /* no poll result other than empty or readable */
    /* The coordinator recorded (notified, pipe, health) itself, at the
     * failure -- every row in, every notification posted, the pipe empty --
     * and at the render that followed. Between the two: no new notification
     * and no pipe wake, and at least one health wake. So the turn that
     * retried the failed render was a health turn, whatever the driver's own
     * timing. */
    uint64_t at_fail[3], at_render[3], r0[3] = { 0 };
    moqr_test_coord_fail_at(at_fail);
    moqr_test_coord_render_at(at_render);
    CHECK(at_render[0] == at_fail[0]);
    CHECK(at_render[1] == at_fail[1]);
    CHECK(at_render[2] > at_fail[2]);
    CHECK(moqr_test_coord_render_mark((unsigned)renders, r0));
    CHECK(r0[0] > rb[11] && r0[1] > rb[9]);   /* render: a later turn */
    printf("render retry: at failure notified %llu pipe %llu health %llu; "
           "at render notified %llu pipe %llu health %llu; deferred %llu\n",
           (unsigned long long)at_fail[0], (unsigned long long)at_fail[1],
           (unsigned long long)at_fail[2], (unsigned long long)at_render[0],
           (unsigned long long)at_render[1], (unsigned long long)at_render[2],
           (unsigned long long)rb[0]);
    if (late_byte) {
        /* In order: (1) the notification was posted after the drain of the
         * hook's turn and its byte was waiting; (2) in that same turn the
         * armed failure was deferred, still armed, because the byte was
         * readable; (3) the failure was injected in a later turn, after an
         * ordinary pipe wake consumed that byte and with no new
         * notification; (4) the retry above was a later, health-only one. */
        CHECK(atomic_load(&g_gate_timeouts) == 0);
        CHECK(atomic_load(&g_late_hook_done) == 1);
        CHECK(atomic_load(&g_late_posted) == 1);
        CHECK(atomic_load(&g_late_readable) == 1);
        const uint64_t hook_turn = atomic_load(&g_late_hook_turn);
        CHECK(hook_turn != 0u);
        CHECK(rb[0] >= 1u);
        CHECK(rb[8] == hook_turn);                /* deferred in that turn */
        CHECK(rb[7] == 1u);                       /* still armed then */
        CHECK(rb[11] > rb[10] && rb[9] > rb[8]);  /* injected later */
        CHECK(at_fail[1] > rb[5]);                /* after a pipe wake */
        CHECK(at_fail[0] == rb[4]);               /* no new notification */
        printf("late byte: hook turn %llu deferred turn %llu (armed %llu, "
               "pipe %llu) injected turn %llu (pipe %llu) render turn %llu\n",
               (unsigned long long)hook_turn, (unsigned long long)rb[8],
               (unsigned long long)rb[7], (unsigned long long)rb[5],
               (unsigned long long)rb[9], (unsigned long long)at_fail[1],
               (unsigned long long)r0[1]);
    }
    CHECK(await(p_broker_idle, NULL, g_bound_ms));
}

static int
run_case(const char *cert, const char *key, const case_t *c)
{
    const int before = g_failures;
    const int no = ++g_case_no;
    printf("CASE START %d\n", no);
    fflush(stdout);
    uint16_t port = free_port(SOCK_DGRAM);
    uint16_t admin_port = c->admin ? free_port(SOCK_STREAM) : 0;
    if (port == 0 || (c->admin && admin_port == 0)) {
        fprintf(stderr, "FAIL: no free port\n");
        return 1;
    }
    static serve_run_t sr;
    memset(&sr, 0, sizeof(sr));
    char json[2048];
    if (c->admin) {
        snprintf(json, sizeof(json),
                 "{\"listener\":{\"transport\":\"picoquic\",\"host\":"
                 "\"127.0.0.1\",\"port\":%u,\"cert\":\"%s\",\"key\":\"%s\","
                 "\"lanes\":1,\"versions\":[18]},"
                 "\"admin\":{\"tcp\":{\"host\":\"127.0.0.1\",\"port\":%u}}}",
                 (unsigned)port, cert, key, (unsigned)admin_port);
    } else {
        snprintf(json, sizeof(json),
                 "{\"listener\":{\"transport\":\"picoquic\",\"host\":"
                 "\"127.0.0.1\",\"port\":%u,\"cert\":\"%s\",\"key\":\"%s\","
                 "\"lanes\":1,\"versions\":[18]}}",
                 (unsigned)port, cert, key);
    }
    char err[256] = { 0 };
    if (moqr_cli_config_parse(json, strlen(json), &sr.cfg, err, sizeof(err)) !=
        MOQR_OK) {
        fprintf(stderr, "FAIL: config refused: %s\n", err);
        return 1;
    }

    moqr_test_clear_stop();
    moqr_test_coord_set_read(NULL);
    moqr_test_coord_fail_renders(0);
    uint64_t health_us = UINT64_MAX;   /* no fallback */
    if (c->work == W_RENDER_RETRY || c->work == W_LATE_BYTE) {
        health_us = UINT64_C(200000);
    } else if (c->work == W_TERMINAL_HEALTH) {
        health_us = UINT64_C(500000);
    }
    moqr_test_coord_set_health_us(health_us);
    atomic_store(&g_ready, 0);
    moqr_test_serve_log_set_io(&kServeIo);
    pthread_t th;
    if (pthread_create(&th, NULL, serve_main, &sr) != 0) {
        fprintf(stderr, "FAIL: serve thread\n");
        moqr_test_serve_log_set_io(NULL);
        return 1;
    }
    /* A serve that never came up skips the workload and settles at once. */
    const bool up = await(p_ready, NULL, g_bound_ms);
    CHECK(up);

    pthread_t busy, spam;
    bool busy_on = false, spam_on = false;
    if (up && c->busy) {
        atomic_store(&g_busy_run, 1);
        busy_on = pthread_create(&busy, NULL, busy_main, NULL) == 0;
        CHECK(busy_on);
    }
    if (up) {
        switch (c->work) {
        case W_SIGNALS:          workload_signals(c); break;
        case W_DEFERRED_RELEASE: workload_deferred_release(); break;
        case W_RENDER_RETRY:     workload_render_retry(false); break;
        case W_LATE_BYTE:        workload_render_retry(true); break;
        case W_TERMINAL_HEALTH:
            /* Lane 0 ends with no self-pipe byte: the health turn ends it. */
            CHECK(moqr_test_serve_stop_lane0());
            break;
        case W_STUCK:            break;
        }
    }
    if (up && c->spam_at_stop) {
        atomic_store(&g_spam_run, 1);
        spam_on = pthread_create(&spam, NULL, spam_main, NULL) == 0;
        CHECK(spam_on);
    }
    if (up && c->sustained_drain) {
        /* Installed while the coordinator runs: the seam publishes the read
         * with a release store and the drain loads it with an acquire load,
         * so this switch is not a data race with a drain in progress. */
        moqr_test_coord_set_read(read_always_data);
        moqr_test_coord_wake_notify();   /* one real byte: the wait returns */
    }
    if (!up || (c->work != W_TERMINAL_HEALTH && c->work != W_STUCK)) {
        moqr_test_raise_stop();
    }
    settle(th, &sr);
    if (spam_on) {
        atomic_store(&g_spam_run, 0);
        join_or_die(spam, "notifier thread join failed");
    }
    if (busy_on) {
        atomic_store(&g_busy_run, 0);
        join_or_die(busy, "lane waker thread join failed");
    }
    moqr_test_coord_set_read(NULL);
    moqr_test_serve_log_set_io(NULL);
    /* Retired: the write end is unpublished; nothing failed to notify. */
    CHECK(moqr_test_coord_wake_wr() == -1);
    CHECK(count(C_FAILED) == 0u);
    if (up) {
        if (c->work == W_TERMINAL_HEALTH) {
            /* Nothing wrote the pipe; a health turn ended the serve. */
            CHECK(count(C_PIPE) == 0u);
            CHECK(count(C_HEALTH) >= 1u);
        } else {
            CHECK(sr.rc == 0);
            if (c->work != W_RENDER_RETRY && c->work != W_LATE_BYTE) {
                /* No health turn ever ran: every wake was a self-pipe byte. */
                CHECK(count(C_HEALTH) == 0u);
            }
        }
    }
    printf("%s: case %d coord_wake work=%d admin=%d busy=%d spam=%d "
           "sustained=%d (turns %llu pipe %llu health %llu notified %llu "
           "coalesced %llu renders %llu)\n",
           g_failures == before ? "PASS" : "FAIL", no, (int)c->work,
           (int)c->admin, (int)c->busy, (int)c->spam_at_stop,
           (int)c->sustained_drain, (unsigned long long)count(C_TURNS),
           (unsigned long long)count(C_PIPE),
           (unsigned long long)count(C_HEALTH),
           (unsigned long long)count(C_NOTIFIED),
           (unsigned long long)count(C_COALESCED),
           (unsigned long long)count(C_RENDERS));
    fflush(stdout);
    return g_failures != before;
}

/* Run this binary in `stuck` mode as a child and check that its process
 * ended at the stuck case's settlement: case 1 started, case 2 never did,
 * and the exit is the unsettled exit. */
static int
control_stuck(const char *self, const char *cert, const char *key)
{
    int p[2];
    if (pipe(p) != 0) {
        return 1;
    }
    pid_t pid = fork();
    if (pid < 0) {
        return 1;
    }
    if (pid == 0) {
        dup2(p[1], STDOUT_FILENO);
        close(p[0]);
        close(p[1]);
        execl(self, self, cert, key, "stuck", (char *)NULL);
        _exit(127);
    }
    close(p[1]);
    static char out[65536];
    size_t got = 0;
    ssize_t r;
    while (got + 1 < sizeof(out) &&
           (r = read(p[0], out + got, sizeof(out) - 1 - got)) > 0) {
        got += (size_t)r;
    }
    out[got] = '\0';
    close(p[0]);
    int st = 0;
    CHECK(waitpid(pid, &st, 0) == pid);
    CHECK(WIFEXITED(st) && WEXITSTATUS(st) == 3);
    CHECK(strstr(out, "CASE START 1\n") != NULL);
    CHECK(strstr(out, "UNSETTLED: serve did not return") != NULL);
    CHECK(strstr(out, "CASE START 2\n") == NULL);
    fputs(out, stdout);
    printf("%s: stuck control (exit %d)\n", g_failures ? "FAIL" : "PASS",
           WIFEXITED(st) ? WEXITSTATUS(st) : -1);
    return g_failures != 0;
}

int
main(int argc, char **argv)
{
    if (argc != 3 && argc != 4) {
        fprintf(stderr, "usage: %s cert key [stuck|control-stuck]\n", argv[0]);
        return 2;
    }
    if (argc == 4 && strcmp(argv[3], "control-stuck") == 0) {
        return control_stuck(argv[0], argv[1], argv[2]);
    }
    if (argc == 4 && strcmp(argv[3], "stuck") == 0) {
        g_bound_ms = 1500;
        static const case_t stuck[] = {
            { .work = W_STUCK },
            { .work = W_SIGNALS },
        };
        for (size_t i = 0; i < sizeof(stuck) / sizeof(stuck[0]); i++) {
            (void)run_case(argv[1], argv[2], &stuck[i]);
        }
        return 0;   /* reaching here means the stuck serve was not caught */
    }
    static const case_t cases[] = {
        { .work = W_SIGNALS, .admin = false, .busy = false },
        { .work = W_SIGNALS, .admin = false, .busy = true },
        { .work = W_SIGNALS, .admin = true,  .busy = false },
        { .work = W_SIGNALS, .admin = true,  .busy = true },
        { .work = W_SIGNALS, .admin = false, .busy = true, .spam_at_stop = true },
        { .work = W_SIGNALS, .admin = true,  .busy = false, .spam_at_stop = true },
        { .work = W_SIGNALS, .admin = false, .busy = true, .spam_at_stop = true,
          .sustained_drain = true },
        { .work = W_DEFERRED_RELEASE },
        { .work = W_DEFERRED_RELEASE, .busy = true },
        { .work = W_RENDER_RETRY },
        { .work = W_LATE_BYTE },
        { .work = W_TERMINAL_HEALTH },
    };
    int bad = 0;
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        bad |= run_case(argv[1], argv[2], &cases[i]);
    }
    if (bad || g_failures) {
        fprintf(stderr, "relay_coord_wake: %d failure(s)\n", g_failures);
        return 1;
    }
    printf("relay_coord_wake: all passed\n");
    return 0;
}
