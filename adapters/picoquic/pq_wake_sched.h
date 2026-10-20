#ifndef MOQ_PQ_WAKE_SCHED_H
#define MOQ_PQ_WAKE_SCHED_H

/*
 * How the threaded facade turns wakes into picoquic packet-loop passes.
 * PRIVATE to adapters/picoquic: pure state, no I/O; the facade holds its mutex
 * around every call.
 *
 * One picoquic loop iteration is: time check (skipped while the loop is
 * draining received packets "immediately"), select, then the callbacks for
 * what select returned -- system_call_duration, wake_up or after_receive --
 * and, unless it drains immediately, a send and after_send. A readable wake
 * pipe wins the select: the UDP sockets are not read that iteration. So a
 * pipe write per wake would keep the sockets from ever being read. Two rules
 * keep them read whatever the wakers' timing:
 *
 *   - The pipe is written only while the network thread is about to block in
 *     select (the time check, its last call before select, records it). A
 *     wake at any other moment is a rerun: the next time check gives select a
 *     zero timeout, and the next pass that pumps serves it.
 *   - After a pass the pipe woke, a socket poll is owed: the pipe is not
 *     written again, and the next time check gives select a zero timeout,
 *     until a callback shows that select has run. With the pipe unwritten
 *     that select reads the sockets, or finds them empty.
 *
 * The owed poll is kept apart from the rerun: the pumps that follow a wake-up
 * in the same iteration (after_send) serve reruns, but only a select clears
 * the owed poll.
 */

#include <stdbool.h>
#include <stdint.h>

typedef struct pq_wake_sched {
    bool in_select;        /* the loop is about to block (or is) in select */
    bool rerun_requested;  /* a wake not yet served by a pump */
    bool rerun_pass;       /* the pass now running serves a rerun */
    bool socket_owed;      /* the pipe woke a pass; sockets not read since */
    bool socket_armed;     /* the time check ran with socket_owed: the
                              select that follows reads the sockets */
} pq_wake_sched_t;

typedef enum {
    PQ_WAKE_PIPE,          /* write picoquic's wake pipe */
    PQ_WAKE_RERUN          /* no pipe write: the next time check reruns */
} pq_wake_action_t;

/* A wake from any thread. */
static inline pq_wake_action_t pq_wake_sched_on_wake(pq_wake_sched_t *s)
{
    if (s->in_select)
        return PQ_WAKE_PIPE;
    s->rerun_requested = true;
    return PQ_WAKE_RERUN;
}

/* The loop's time check, before select: lowers *delta_t to 0 for a rerun or
 * an owed socket poll, else records that the loop is about to block. */
static inline void pq_wake_sched_on_time_check(pq_wake_sched_t *s,
                                               int64_t *delta_t)
{
    if (s->socket_owed)
        s->socket_armed = true;
    if (s->rerun_requested || s->socket_owed) {
        if (delta_t) *delta_t = 0;
    } else {
        s->in_select = true;
    }
}

/* Any loop callback after select returned. `pumping` for a pass that pumps;
 * `pipe_wake` for the wake_up callback, the pass the wake pipe woke. */
static inline void pq_wake_sched_on_pass(pq_wake_sched_t *s, bool pumping,
                                         bool pipe_wake)
{
    s->in_select = false;
    if (s->socket_armed) {
        /* The select after the armed time check has run, with the pipe
         * unwritten since the last wake-up: it read the sockets. */
        s->socket_owed = false;
        s->socket_armed = false;
    }
    if (pipe_wake)
        s->socket_owed = true;
    if (!pumping)
        return;
    s->rerun_pass = s->rerun_requested;
    s->rerun_requested = false;
}

#endif /* MOQ_PQ_WAKE_SCHED_H */
