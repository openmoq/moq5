/*
 * The threaded facade's wake scheduling (pq_wake_sched.h), driven against a
 * step-by-step model of picoquic's packet loop (sockloop.c,
 * picoquic_packet_loop_v3) in its callback order:
 *
 *   time check  (skipped while draining immediately)
 *   select      (zero timeout when draining; a readable wake pipe wins and
 *                the sockets are not read; else one datagram is received)
 *   system_call_duration (after a zero-timeout select, when monitored)
 *   wake_up | after_receive
 *   -- after a receive, up to PICOQUIC_PACKET_LOOP_RECV_MAX iterations
 *      drain immediately: back to select, no time check, no send --
 *   send, after_send
 *
 * and the facade's wake: one pipe byte outstanding at most (wake_pending,
 * cleared by the pass that serves it).
 *
 * A waker may wake at any point of an iteration, several times, or stop.
 * Pinned whatever it does: a select that is not pipe-woken comes between any
 * two pipe-woken selects (the sockets get their turn); every wake is followed
 * by a pump that begins after it, and none is left behind a select that
 * blocks; at most one pipe byte is outstanding.
 */

#include "../pq_wake_sched.h"

#include <stdio.h>
#include <string.h>

static int failures = 0;

#define CHECK(expr)                                                     \
    do {                                                                \
        if (!(expr)) {                                                  \
            fprintf(stderr, "FAIL: %s:%d: %s\n",                        \
                    __FILE__, __LINE__, #expr);                         \
            failures++;                                                 \
        }                                                               \
    } while (0)

#define RECV_MAX 10    /* PICOQUIC_PACKET_LOOP_RECV_MAX */

/* Points of one iteration where the waker may act. */
enum {
    AT_TIME_CHECK,     /* just after the time check */
    AT_SELECT,         /* while select waits (before it returns) */
    AT_RETURNED,       /* select returned, before its callback */
    AT_WAKE_UP_PUMP,   /* during the wake_up callback's pump */
    AT_RECV_PUMP,      /* during after_receive's pump */
    AT_SEND,           /* while sending */
    AT_SEND_PUMP,      /* during after_send's pump */
    AT_COUNT
};

typedef struct {
    pq_wake_sched_t s;
    /* the facade */
    bool wake_pending;
    int  pipe_bytes;
    /* the network */
    int  datagrams;              /* waiting on the UDP sockets; <0: endless */
    bool monitor_syscalls;       /* system_call_duration after delta 0 */
    /* observations */
    int  iterations, selects, pipe_selects, socket_selects, reads;
    int  pumps, pipe_writes, max_pipe_bytes;
    int  pipe_run, worst_pipe_run; /* pipe-woken selects in a row */
    unsigned long wake_seq, served_seq;  /* last wake / last wake served */
    int  pumps_since_unserved, worst_wake_latency;
    int  stranded;               /* wakes left behind a select that blocks */
    bool stopped, loop_ended;
    int  stop_iteration, end_iteration;
    /* the waker */
    unsigned waker_mask;         /* bit per AT_* point: wake there */
    int  wakes_per_point;
    unsigned long rng;
    bool random_waker;
} loop_t;

static void wake(loop_t *l)
{
    l->wake_seq++;
    if (l->wake_pending)
        return;
    if (pq_wake_sched_on_wake(&l->s) == PQ_WAKE_RERUN)
        return;
    l->wake_pending = true;
    l->pipe_bytes++;
    l->pipe_writes++;
    if (l->pipe_bytes > l->max_pipe_bytes) l->max_pipe_bytes = l->pipe_bytes;
}

static bool rnd(loop_t *l)
{
    l->rng = l->rng * 6364136223846793005UL + 1442695040888963407UL;
    return (l->rng >> 33) & 1u;
}

static void waker(loop_t *l, int at)
{
    bool go = l->random_waker ? rnd(l) : ((l->waker_mask >> at) & 1u) != 0;
    if (!go) return;
    for (int i = 0; i < l->wakes_per_point; i++) wake(l);
}

/* The facade's pump: a pass that begins now serves every wake before it. */
static void pump(loop_t *l, int cb_wake_up)
{
    l->pumps++;
    if (cb_wake_up || l->s.rerun_pass)
        l->wake_pending = false;
    if (l->served_seq < l->wake_seq) {
        int lat = l->pumps_since_unserved;
        if (lat > l->worst_wake_latency) l->worst_wake_latency = lat;
    }
    l->served_seq = l->wake_seq;
    l->pumps_since_unserved = 0;
    if (l->stopped && !l->loop_ended) {
        l->loop_ended = true;
        l->end_iteration = l->iterations;
    }
}

static void callback(loop_t *l, bool pumping, bool pipe_wake, int at)
{
    pq_wake_sched_on_pass(&l->s, pumping, pipe_wake);
    if (!pumping) return;
    waker(l, at);       /* a wake while the pass runs: before the pump reads */
    pump(l, pipe_wake);
}

/* One select and what follows it; returns true when the iteration ends in
 * the immediate drain (no send). */
static bool select_and_dispatch(loop_t *l, int64_t delta, int *immediate_n)
{
    bool has_dgram = l->datagrams != 0;
    if (!l->pipe_bytes && !has_dgram && delta != 0) {
        /* It blocks: a wake that comes now must reach it through the pipe,
         * or it waits out picoquic's own timer. */
        unsigned long before = l->wake_seq;
        waker(l, AT_SELECT);
        if (l->wake_seq != before && !l->pipe_bytes)
            l->stranded++;
    }
    l->selects++;
    bool pipe = l->pipe_bytes > 0;
    bool recv = false;
    if (pipe) {
        l->pipe_bytes = 0;                /* the loop reads up to 8 bytes */
        l->pipe_selects++;
        l->pipe_run++;
        if (l->pipe_run > l->worst_pipe_run) l->worst_pipe_run = l->pipe_run;
    } else {
        l->socket_selects++;
        l->pipe_run = 0;
        if (has_dgram) {
            recv = true;
            l->reads++;
            if (l->datagrams > 0) l->datagrams--;
        }
    }
    waker(l, AT_RETURNED);
    if (l->monitor_syscalls && delta == 0)
        pq_wake_sched_on_pass(&l->s, false, false);
    if (pipe) {
        callback(l, true, true, AT_WAKE_UP_PUMP);
    } else if (recv) {
        callback(l, true, false, AT_RECV_PUMP);
        if (*immediate_n < RECV_MAX) {
            (*immediate_n)++;
            return true;
        }
    }
    return false;
}

static void iterate(loop_t *l)
{
    l->iterations++;
    l->pumps_since_unserved++;
    int immediate_n = 1;
    int64_t delta = 1000000;              /* picoquic's own next wake */
    pq_wake_sched_on_time_check(&l->s, &delta);
    waker(l, AT_TIME_CHECK);
    while (select_and_dispatch(l, delta, &immediate_n))
        delta = 0;                        /* the immediate drain */
    waker(l, AT_SEND);
    callback(l, true, false, AT_SEND_PUMP);
}

static void run(loop_t *l, int n)
{
    for (int i = 0; i < n; i++) iterate(l);
}

static void check_invariants(const loop_t *l, const char *what)
{
    if (l->worst_pipe_run > 1 || l->max_pipe_bytes > 1 ||
        l->worst_wake_latency > 2 || l->stranded) {
        printf("  %s: pipe-woken selects in a row %d, pipe bytes %d, "
               "wake latency %d, stranded wakes %d\n", what,
               l->worst_pipe_run, l->max_pipe_bytes, l->worst_wake_latency,
               l->stranded);
    }
    CHECK(l->worst_pipe_run <= 1);
    CHECK(l->max_pipe_bytes <= 1);
    CHECK(l->worst_wake_latency <= 2);
    CHECK(l->stranded == 0);
}

int main(void)
{
    /* 1. Every single wake point and every pair of points, with and without
     *    traffic, one or three wakes per point, syscall monitoring on/off:
     *    the sockets get a select between any two pipe-woken ones, at most
     *    one byte is outstanding, and a pump follows every wake. */
    int combos = 0;
    for (unsigned mask = 1; mask < (1u << AT_COUNT); mask++) {
        if (__builtin_popcount(mask) > 2) continue;
        for (int traffic = 0; traffic < 2; traffic++)
        for (int wakes = 1; wakes <= 3; wakes += 2)
        for (int mon = 0; mon < 2; mon++) {
            loop_t l;
            memset(&l, 0, sizeof(l));
            l.waker_mask = mask;
            l.wakes_per_point = wakes;
            l.datagrams = traffic ? -1 : 0;
            l.monitor_syscalls = mon != 0;
            run(&l, 200);
            check_invariants(&l, "points");
            combos++;
        }
    }

    /* 2. The worst waker: at every point, under endless traffic. */
    {
        loop_t l;
        memset(&l, 0, sizeof(l));
        l.waker_mask = (1u << AT_COUNT) - 1;
        l.wakes_per_point = 1;
        l.datagrams = -1;
        run(&l, 1000);
        check_invariants(&l, "every point");
        printf("every point: %d iterations, %d selects (%d pipe, %d socket), "
               "%d datagrams read, %d pipe writes\n", l.iterations, l.selects,
               l.pipe_selects, l.socket_selects, l.reads, l.pipe_writes);
        CHECK(l.socket_selects >= l.pipe_selects);
        CHECK(l.reads >= 1000);
    }

    /* 3. The waker that defeats a forced pass consumed by a later pump: it
     *    wakes only at the time check, as the loop is about to block. */
    {
        loop_t l;
        memset(&l, 0, sizeof(l));
        l.waker_mask = 1u << AT_TIME_CHECK;
        l.wakes_per_point = 1;
        l.datagrams = -1;
        run(&l, 1000);
        check_invariants(&l, "time check");
        printf("time-check waker: %d pipe-woken and %d socket selects, "
               "%d datagrams read\n", l.pipe_selects, l.socket_selects, l.reads);
        CHECK(l.reads >= 1000);
    }

    /* 4. Seeded random wakers. */
    for (unsigned long seed = 1; seed <= 64; seed++) {
        loop_t l;
        memset(&l, 0, sizeof(l));
        l.random_waker = true;
        l.rng = seed;
        l.wakes_per_point = (int)(seed % 3) + 1;
        l.datagrams = (seed & 1) ? -1 : 37;
        l.monitor_syscalls = (seed & 2) != 0;
        run(&l, 500);
        check_invariants(&l, "random");
        combos++;
    }

    /* 5. Stop: a stop's wake, at any point, reaches a pump that sees it
     *    within the next iteration. */
    for (int at = 0; at < AT_COUNT; at++) {
        loop_t l;
        memset(&l, 0, sizeof(l));
        l.waker_mask = (1u << AT_TIME_CHECK) | (1u << AT_SEND_PUMP);
        l.wakes_per_point = 1;
        l.datagrams = -1;
        run(&l, 7);
        l.waker_mask = 1u << at;          /* the stop wakes here */
        l.stopped = true;
        l.stop_iteration = l.iterations;
        run(&l, 3);
        CHECK(l.loop_ended);
        CHECK(l.end_iteration <= l.stop_iteration + 2);
    }

    /* 6. Unit rows of the state itself. */
    {
        /* A wake while not about to block is a rerun; the next time check
         * does not block; the next pump serves it. */
        pq_wake_sched_t s = { 0 };
        pq_wake_sched_on_pass(&s, true, false);
        CHECK(pq_wake_sched_on_wake(&s) == PQ_WAKE_RERUN);
        int64_t delta = 1000000;
        pq_wake_sched_on_time_check(&s, &delta);
        CHECK(delta == 0 && !s.in_select);
        pq_wake_sched_on_pass(&s, true, false);
        CHECK(s.rerun_pass && !s.rerun_requested);
        delta = 1000000;
        pq_wake_sched_on_time_check(&s, &delta);
        CHECK(delta == 1000000 && s.in_select);
        CHECK(pq_wake_sched_on_wake(&s) == PQ_WAKE_PIPE);
    }
    {
        /* The owed socket poll outlives the after_send pump of the wake-up
         * iteration, and only the next select clears it. */
        pq_wake_sched_t s = { 0 };
        int64_t delta = 1000000;
        pq_wake_sched_on_time_check(&s, &delta);
        CHECK(pq_wake_sched_on_wake(&s) == PQ_WAKE_PIPE);
        pq_wake_sched_on_pass(&s, true, true);     /* wake_up */
        pq_wake_sched_on_pass(&s, true, false);    /* after_send */
        CHECK(s.socket_owed);
        delta = 1000000;
        pq_wake_sched_on_time_check(&s, &delta);
        CHECK(delta == 0 && !s.in_select);
        CHECK(pq_wake_sched_on_wake(&s) == PQ_WAKE_RERUN);
        pq_wake_sched_on_pass(&s, false, false);   /* system_call_duration */
        CHECK(!s.socket_owed);
        pq_wake_sched_on_pass(&s, true, false);    /* after_send */
        CHECK(s.rerun_pass);
    }

    printf("%d waker schedules\n", combos);
    if (failures) {
        printf("FAILED: %d check(s) [pq_wake_sched]\n", failures);
        return 1;
    }
    printf("PASS: pq_wake_sched\n");
    return 0;
}
