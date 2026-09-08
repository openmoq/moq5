/*
 * The admin endpoint boundary: one listening socket, one owner thread, and the
 * bounded multi-client state machine from the frozen sans-I/O admin tier.
 *
 * ONE BROKER. The broker is held by reference. Signal and HTTP demand share one
 * identity space, so a generation opened for a scrape is visible to the lanes
 * and can coalesce with a latched signal into one token.
 *
 * ONE OWNER. The admin thread performs every moqr_admin_t transition and every
 * broker delivery transaction. Nothing else touches them -- not the coordinator,
 * not the signal handler, not stop(). Cancellation is a request plus a wake;
 * the owner carries it out and acknowledges before it exits.
 *
 * NO LIVE STATE. The owner reaches the rest of the process only through the
 * wake_all / collect / emit_signal callbacks. `collect` reads copied snapshot
 * rows under their own row mutexes, which is not lane traversal.
 */
#include "admin_listen.h"
#include "admin_listen_layout.h"

#include <moq/relay/moqr_obs.h>

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#ifdef MOQR_ADMIN_LISTEN_TESTING
/*
 * Deterministic failure seams.
 *
 * A clock that stops, a poll that fails and a join that cannot prove its
 * thread dead are all reachable in production and none of them can be provoked
 * from outside. They are injected here rather than reasoned about, because a
 * fail-closed path nothing ever executes is a fail-closed path nobody has
 * checked. Compiled only into the boundary test's own translation unit; the
 * shipped object has no such symbols.
 */
static atomic_int g_test_fail_clock;
/* Refuses only the sample ARMING takes. The owner's own clock is untouched, so
 * a case about arming cannot accidentally terminate the endpoint. */
static atomic_int g_test_fail_arm_clock;
static atomic_int g_test_fail_poll;
/* Park the owner: it ignores the stop request and keeps running its loop,
 * which keeps reading the broker and the callback context. That is what makes
 * "the owner may still be running" a testable state rather than a story. */
static atomic_int g_test_park_owner;
/* Make the condition-variable half of the view initialization fail, so the
 * partial-acquisition arm is exercised rather than reasoned about. */
static atomic_int g_test_fail_view_cv;

void moqr_admin_listen_test_fail_view_cv(int on) {
    atomic_store(&g_test_fail_view_cv, on);
}

/* Acquisition accounting for the view lock. A partial initialization that is
 * never recorded in the ledger cannot be released by unwind, and nothing else
 * on this platform makes an undestroyed mutex observable -- so it is counted. */
static atomic_int g_view_mu_init;
static atomic_int g_view_mu_destroy;

int moqr_admin_listen_test_view_mu_balance(void) {
    return atomic_load(&g_view_mu_init) - atomic_load(&g_view_mu_destroy);
}
/* Loop iterations. A parked owner is no longer SERVING, so its liveness has to
 * be observable without driving a turn. */
static atomic_int g_test_turns;

int moqr_admin_listen_test_turns(void) { return atomic_load(&g_test_turns); }

void moqr_admin_listen_test_park_owner(int on) {
    atomic_store(&g_test_park_owner, on);
}
void moqr_admin_listen_test_fail_arm_clock(int on) {
    atomic_store(&g_test_fail_arm_clock, on);
}
void moqr_admin_listen_test_fail_clock(int on) {
    atomic_store(&g_test_fail_clock, on);
}
void moqr_admin_listen_test_fail_poll(int on) {
    atomic_store(&g_test_fail_poll, on);
}

/* Every settlement counted, under the same lock the view is published with, so
 * "exactly once" is arithmetic rather than an inference from a final empty
 * state. */
static void
owner_count(struct moqr_admin_listen *l, unsigned *field)
{
    (void)pthread_mutex_lock(&l->view_mu);
    (*field)++;
    (void)pthread_mutex_unlock(&l->view_mu);
}

/*
 * THIS listener's interior view, published by its owner.
 *
 * Cancellation obligations are PER STATE and per resource, so a test that
 * cannot see the states cannot prove they existed when terminality arrived,
 * and a test that only sees the final empty state cannot tell one settlement
 * from three.
 *
 * Two properties matter and neither is optional. The view belongs to the
 * LISTENER, so a second endpoint cannot overwrite the evidence or satisfy a
 * waiter that is watching the first. And the generation counter is monotonic
 * with a condition variable behind it, so a reader waits on PUBLICATION -- any
 * timeout is a fail-closed bound, never the mechanism.
 */
static void
owner_publish_view(struct moqr_admin_listen *l)
{
    (void)pthread_mutex_lock(&l->view_mu);
    for (uint32_t i = 0; i < MOQR_ADMIN_MAX_CLIENTS; i++) {
        l->view_state[i] = (int)l->admin.client[i].state;
        l->view_bytes[i] = (unsigned long long)l->admin.client[i].bytes_out;
        l->view_serial[i] = (unsigned long long)l->admin.client[i].serial;
        l->view_fds[i] = l->client_fd[i];
    }
    for (uint32_t b = 0; b < MOQR_ADMIN_BANKS; b++) {
        l->view_pins[b] = (int)l->admin.bank[b].pins;
    }
    l->view_probe.hits = l->clock_probe_hits;
    l->view_probe.unlocked = l->clock_unlocked_samples;
    l->view_probe.foreign = l->clock_foreign_holder_samples;
    l->view_probe.errors = l->clock_probe_errors;
    l->view_probe.disagreements = l->clock_marker_disagreements;
    l->view_probe.poll_used_ms = l->poll_used_ms;
    l->view_gen++;
    (void)pthread_cond_broadcast(&l->view_cv);
    (void)pthread_mutex_unlock(&l->view_mu);
}

/* Record the retirement carrier while it still exists, so the test can observe
 * the abandoned generation and its retained {serial, demand, bank} rather than
 * inferring it from the emptiness that follows. */
static void
owner_publish_carrier(struct moqr_admin_listen *l,
                      const moqr_admin_retire_t *ret, bool live)
{
    (void)pthread_mutex_lock(&l->view_mu);
    l->view_retiring = live ? 1 : 0;
    if (live && ret != NULL) {
        l->view_retire_serial = (unsigned long long)ret->serial;
        l->view_retire_demand = ret->demand;
        l->view_retire_bank = ret->bank;
        l->view_retire_taken = ret->taken ? 1 : 0;
    }
    l->view_gen++;
    (void)pthread_cond_broadcast(&l->view_cv);
    (void)pthread_mutex_unlock(&l->view_mu);
}

/* The owner-side pause at the cancellation/retirement boundary. It holds the
 * owner where the carrier is observable, and it is released by the test. */
/* The pause immediately after terminal cancellation and BEFORE any cleanup,
 * so the live descriptor set can be recorded before either sweep touches it. */
static void
owner_pause_at_cancel(struct moqr_admin_listen *l)
{
    (void)pthread_mutex_lock(&l->view_mu);
    if (l->pause_at_cancel) {
        l->paused_at_cancel = 1;
        for (uint32_t i = 0; i < MOQR_ADMIN_MAX_CLIENTS; i++) {
            l->view_state[i] = (int)l->admin.client[i].state;
            l->view_bytes[i] =
                (unsigned long long)l->admin.client[i].bytes_out;
            l->view_serial[i] =
                (unsigned long long)l->admin.client[i].serial;
            l->view_fds[i] = l->client_fd[i];
        }
        l->view_gen++;
        (void)pthread_cond_broadcast(&l->view_cv);
        while (l->pause_at_cancel) {
            (void)pthread_cond_wait(&l->view_cv, &l->view_mu);
        }
        l->paused_at_cancel = 0;
        l->view_gen++;
        (void)pthread_cond_broadcast(&l->view_cv);
    }
    (void)pthread_mutex_unlock(&l->view_mu);
}

/*
 * THE ONE CHECKED VIEW-LOCK BOUNDARY for the clock step.
 *
 * The ownership marker is not a fact the clock code asserts about itself: it
 * is maintained here, by the same operations that take and release the lock,
 * and both pthread results are checked. A step that took the lock some other
 * way would not have the marker, and a step that released it some other way
 * would leave the marker behind -- which is why `check_listener_contracts`
 * forbids raw lock and unlock of `view_mu` inside the clock reader.
 */
static bool
view_lock_for_clock(struct moqr_admin_listen *l)
{
    if (pthread_mutex_lock(&l->view_mu) != 0) {
        return false;
    }
    l->view_held = 1;
    l->view_holder = pthread_self();
    return true;
}

static bool
view_unlock_for_clock(struct moqr_admin_listen *l)
{
    /* The marker stops being authoritative BEFORE the lock is actually
     * released, so no window exists in which it claims ownership this thread
     * no longer has. */
    l->view_held = 0;
    l->view_holder = (pthread_t)0;
    return pthread_mutex_unlock(&l->view_mu) == 0;
}

/*
 * THE REAL MUTEX ANSWERS.
 *
 * A marker written by the same helpers that claim to lock is self-certified:
 * helpers that set the marker and never call pthreads satisfy it perfectly.
 * So the question is put to `view_mu` itself, at the real sample point, on an
 * error-checking mutex where the answer is unambiguous:
 *
 *   locked  -- something really holds it, which is the fact no marker can
 *              fake; the marker then says whether it is this thread;
 *   free    -- nobody held it, so this sample is not protected at all, and a
 *              marker claiming otherwise is lying.
 *
 * `pthread_mutex_trylock` reports a lock held by the calling thread as busy,
 * exactly as it reports one held by anybody else, so it answers "is it
 * locked" and nothing more. That is the half a marker cannot forge; identity
 * is the half a marker is for. Where identity itself is under attack -- at the
 * poll drain -- the marker is not trusted and `poll_drain_probe` asks the
 * mutex directly.
 */
/*
 * A ONE-SHOT FAILPOINT ON THE PROBE'S OWN RELEASE.
 *
 * The refusal arm below is otherwise unreachable, and a source check can only
 * say where an increment sits, never that control flow reaches it. So the real
 * release happens first -- the mutex is genuinely free afterwards -- and the
 * injected non-zero result is returned once, exercising the real arm and the
 * real counter. Consumption is recorded so a test can prove the injection was
 * used rather than merely armed.
 */
/* Accessors that could not take the view lock, and so released nothing. */
static atomic_int g_test_accessor_lock_failed;

int
moqr_admin_listen_test_accessor_lock_failures(void)
{
    return atomic_load(&g_test_accessor_lock_failed);
}

static atomic_int g_test_fail_probe_unlock;
static atomic_int g_test_probe_unlock_consumed;

void
moqr_admin_listen_test_fail_probe_unlock(int on)
{
    atomic_store(&g_test_fail_probe_unlock, on);
}

int
moqr_admin_listen_test_probe_unlock_consumed(void)
{
    return atomic_load(&g_test_probe_unlock_consumed);
}

static int
probe_release(struct moqr_admin_listen *l)
{
    int rc = pthread_mutex_unlock(&l->view_mu);

    if (rc == 0 && atomic_load(&g_test_fail_probe_unlock) != 0) {
        atomic_store(&g_test_fail_probe_unlock, 0);
        (void)atomic_fetch_add(&g_test_probe_unlock_consumed, 1);
        return EPERM;
    }
    return rc;
}

/*
 * WHO REALLY OWNS THE LOCK AT THE DRAIN.
 *
 * `pthread_mutex_trylock` cannot answer this on its own: on this platform it
 * returns EBUSY whether the caller owns the lock or somebody else does. It is
 * the BLOCKING `pthread_mutex_lock` that reports EDEADLK for an error-checking
 * self-lock. So the two operations are used together, and each of the answers
 * that matters is immediate:
 *
 *   trylock succeeds        -- nobody held it. Release what this took and say
 *                              FREE. No blocking.
 *   trylock EBUSY, lock
 *     returns EDEADLK       -- this thread owns it. Immediate, no blocking.
 *   trylock EBUSY, lock
 *     returns 0             -- somebody else owned it and has now released;
 *                              this thread acquired it. Release and say
 *                              FOREIGN.
 *
 * Only the FOREIGN answer waits, and only for as long as that other party
 * holds the lock. In this proof the only other party is a request publication,
 * which holds it for the length of two assignments.
 *
 *   1  -- this thread's critical section owns it;
 *   0  -- nobody held it: the drain is outside the hold;
 *  -1  -- somebody else owned it, or an answer that is none of these.
 */
static int
poll_drain_probe(struct moqr_admin_listen *l)
{
    int rc = pthread_mutex_trylock(&l->view_mu);

    if (rc == 0) {
        (void)pthread_mutex_unlock(&l->view_mu);
        return 0;
    }
    if (rc != EBUSY) {
        return -1;
    }
    rc = pthread_mutex_lock(&l->view_mu);
    if (rc == EDEADLK) {
        return 1;
    }
    if (rc == 0) {
        (void)pthread_mutex_unlock(&l->view_mu);
        return -1;
    }
    return -1;
}

static void
clock_lock_probe(struct moqr_admin_listen *l)
{
    int rc = pthread_mutex_trylock(&l->view_mu);

    l->clock_probe_hits++;
    if (rc == 0) {
        /*
         * THE MUTEX WAS FREE. Nothing protected this sample -- whatever the
         * boundary helpers claim to have done, they did not lock. Put it back
         * immediately; this thread never wanted it.
         */
        l->clock_unlocked_samples++;
        if (l->view_held) {
            /* And the marker said otherwise, so the marker is lying. */
            l->clock_marker_disagreements++;
        }
        /* This probe really took the lock, so this release is a real release:
         * a refusal leaves the mutex in a state nobody may rely on, and
         * reporting it as restored would be the same attempt-for-effect
         * confusion the boundary itself was corrected for. */
        if (probe_release(l) != 0) {
            l->clock_probe_errors++;
        }
    } else if (rc != EBUSY) {
        l->clock_probe_errors++;
    } else if (!l->view_held ||
               !pthread_equal(l->view_holder, pthread_self())) {
        /* Really locked, but not by this thread's critical section. */
        l->clock_foreign_holder_samples++;
    }
}

/*
 * Run the probe once from a caller that does NOT hold the view lock, so its
 * "nobody held it" arm is reached the way it really would be: by a sample that
 * genuinely was not protected. Everything it counts is real.
 */
void
moqr_admin_listen_test_probe_once(struct moqr_admin_listen *l)
{
    clock_lock_probe(l);
}

/* Arm the logical clock at `now_us`; the owner reads it from here on. Private:
 * every caller goes through the expectation-taking entry point below. */
static bool
clock_arm_raw(struct moqr_admin_listen *l, uint64_t now_us)
{
    struct timespec ts;
    uint64_t real_us;
    bool ok = false;

    /*
     * THE LOGICAL CLOCK MAY NOT RUN BACKWARDS.
     *
     * The owner is entitled to a monotonic clock: it has already read real
     * time, and interior deadlines it computed from those readings are live.
     * Handing it a smaller value makes every one of those deadlines look
     * expired at once, which drops live clients for a reason that cannot
     * happen in production. The requested origin is therefore a floor -- and
     * the sample it is floored against is taken under the same lock the owner
     * reads its clock under, so no reading can slip between them.
     *
     * A real clock that will not answer means there is no floor to establish,
     * and arming anyway would install exactly the stale origin this exists to
     * prevent. That refuses.
     */
    /* THE SAME checked boundary the owner reads its clock under. Taking a
     * different lock -- or none -- would let this land between the owner's
     * choice of clock and its sample, which is the whole failure this floor
     * exists to prevent. */
    if (!view_lock_for_clock(l)) {
        return false;
    }
    if (atomic_load(&g_test_fail_arm_clock) != 0 ||
        clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        ok = false;
        goto arm_done;
    }
    clock_lock_probe(l);
    real_us = ((uint64_t)ts.tv_sec * 1000000ull) +
              ((uint64_t)ts.tv_nsec / 1000ull);
    if (l->last_real_us > real_us) {
        real_us = l->last_real_us;
    }
    /* A second arming is still an arming: whatever the clock has already
     * reached, including everything advanced onto it, is part of the floor.
     * Otherwise a stale origin walks the owner backwards past deadlines that
     * were computed from the advanced value. */
    if (l->logical_clock && l->logical_now_us > real_us) {
        real_us = l->logical_now_us;
    }
    l->logical_clock = 1;
    l->logical_now_us = now_us > real_us ? now_us : real_us;
    ok = true;
arm_done:
    return view_unlock_for_clock(l) && ok;
}

/* What the owner's clock actually is, for the monotonicity proof. */
void
moqr_admin_listen_test_clock_state(struct moqr_admin_listen *l,
                                   int *armed,
                                   uint64_t *logical_now_us,
                                   uint64_t *last_real_us)
{
    (void)pthread_mutex_lock(&l->view_mu);
    /* The SELECTOR, not only the value. A zero origin is not evidence that no
     * clock was installed: the selector is what decides which clock the owner
     * reads, and it can be set while the value is still zero. */
    *armed = l->logical_clock;
    *logical_now_us = l->logical_now_us;
    *last_real_us = l->last_real_us;
    (void)pthread_mutex_unlock(&l->view_mu);
}

/*
 * Held at the capture, WITHOUT giving up view_mu.
 *
 * The rendezvous has its own lock, so a request arriving while the owner is
 * held here is genuinely blocked on the view lock rather than merely late --
 * which is what makes the drain-window defect arrangeable instead of raced.
 */
static void
poll_seam_wait(struct moqr_admin_listen *l)
{
    (void)pthread_mutex_lock(&l->seam_mu);
    if (l->poll_seam_armed) {
        l->poll_seam_held = 1;
        (void)pthread_cond_broadcast(&l->seam_cv);
        while (l->poll_seam_armed) {
            (void)pthread_cond_wait(&l->seam_cv, &l->seam_mu);
        }
        l->poll_seam_held = 0;
        (void)pthread_cond_broadcast(&l->seam_cv);
    }
    (void)pthread_mutex_unlock(&l->seam_mu);
}

/*
 * ASK THE REAL MUTEX WHETHER IT IS BUSY.
 *
 * A flag saying a thread started, or is about to lock, proves nothing: either
 * can be published and the thread then descheduled before it touches
 * anything. This touches `view_mu` itself and reports what it found. If it
 * unexpectedly acquired the lock it puts it straight back, so nothing is left
 * held and no pthread semantics are bent.
 */
int
moqr_admin_listen_test_view_mu_is_busy(struct moqr_admin_listen *l)
{
    int rc = pthread_mutex_trylock(&l->view_mu);

    if (rc == 0) {
        (void)pthread_mutex_unlock(&l->view_mu);
        return 0;
    }
    return rc == EBUSY ? 1 : -1;
}

/* Publish that the real lock was found busy, and wait for that fact. */
void
moqr_admin_listen_test_poll_seam_contended(struct moqr_admin_listen *l)
{
    (void)pthread_mutex_lock(&l->seam_mu);
    l->poll_seam_contended = 1;
    (void)pthread_cond_broadcast(&l->seam_cv);
    (void)pthread_mutex_unlock(&l->seam_mu);
}

bool
moqr_admin_listen_test_poll_seam_await_contended(struct moqr_admin_listen *l,
                                                 int budget_ms)
{
    struct timespec deadline;
    bool seen;

    if (clock_gettime(CLOCK_REALTIME, &deadline) != 0) {
        return false;
    }
    deadline.tv_sec += budget_ms / 1000;
    deadline.tv_nsec += (long)(budget_ms % 1000) * 1000000L;
    if (deadline.tv_nsec >= 1000000000L) {
        deadline.tv_sec++;
        deadline.tv_nsec -= 1000000000L;
    }
    (void)pthread_mutex_lock(&l->seam_mu);
    while (!l->poll_seam_contended) {
        if (pthread_cond_timedwait(&l->seam_cv, &l->seam_mu, &deadline) != 0) {
            break;
        }
    }
    seen = l->poll_seam_contended != 0;
    (void)pthread_mutex_unlock(&l->seam_mu);
    return seen;
}

void
moqr_admin_listen_test_poll_seam(struct moqr_admin_listen *l, int on)
{
    (void)pthread_mutex_lock(&l->seam_mu);
    l->poll_seam_armed = on;
    (void)pthread_cond_broadcast(&l->seam_cv);
    (void)pthread_mutex_unlock(&l->seam_mu);
}

/* Block until the owner is standing in the seam, or the deadline passes. */
bool
moqr_admin_listen_test_poll_seam_await(struct moqr_admin_listen *l,
                                       int budget_ms)
{
    struct timespec deadline;
    bool held;

    if (clock_gettime(CLOCK_REALTIME, &deadline) != 0) {
        return false;
    }
    deadline.tv_sec += budget_ms / 1000;
    deadline.tv_nsec += (long)(budget_ms % 1000) * 1000000L;
    if (deadline.tv_nsec >= 1000000000L) {
        deadline.tv_sec++;
        deadline.tv_nsec -= 1000000000L;
    }
    (void)pthread_mutex_lock(&l->seam_mu);
    while (!l->poll_seam_held) {
        if (pthread_cond_timedwait(&l->seam_cv, &l->seam_mu, &deadline) != 0) {
            break;
        }
    }
    held = l->poll_seam_held != 0;
    (void)pthread_mutex_unlock(&l->seam_mu);
    return held;
}

/*
 * Ask the owner to adopt a poll timeout. The request is per listener, and the
 * caller learns it was adopted from the acknowledgement, never from waiting to
 * see whether something fails to happen.
 */
unsigned
moqr_admin_listen_test_request_poll_ms(struct moqr_admin_listen *l, int ms)
{
    unsigned gen;

    (void)pthread_mutex_lock(&l->view_mu);
    l->poll_req_ms = ms;
    l->poll_req_gen++;
    gen = l->poll_req_gen;
    (void)pthread_mutex_unlock(&l->view_mu);
    moqr_admin_listen_notify(l);
    return gen;
}

/*
 * The answer to ONE request, if it has been given.
 *
 * Returns 1 with the answer, 0 while it is still outstanding, and -1 once a
 * later generation has been acknowledged without this one ever appearing --
 * which is a skipped request, not a slow one, and must not be waited out.
 */
int
moqr_admin_listen_test_poll_ack_for(struct moqr_admin_listen *l, unsigned gen,
                                    int *ms, int *adopted, int *drain_held)
{
    int found = 0;
    unsigned highest = 0;

    (void)pthread_mutex_lock(&l->view_mu);
    for (unsigned i = 0; i < MOQR_ADMIN_POLL_ACK_HIST; i++) {
        if (l->poll_ack_hist[i].gen == 0u) {
            continue;
        }
        if (l->poll_ack_hist[i].gen > highest) {
            highest = l->poll_ack_hist[i].gen;
        }
        if (l->poll_ack_hist[i].gen == gen) {
            *ms = l->poll_ack_hist[i].ms;
            *adopted = l->poll_ack_hist[i].adopted;
            *drain_held = l->poll_ack_hist[i].drain_held;
            found = 1;
        }
    }
    if (found == 0 && highest > gen) {
        found = -1;
    }
    (void)pthread_mutex_unlock(&l->view_mu);
    return found;
}

/* What the owner has acknowledged, and what it actually adopted. */
void
moqr_admin_listen_test_poll_ack(struct moqr_admin_listen *l, unsigned *gen,
                                int *ms, int *adopted)
{
    (void)pthread_mutex_lock(&l->view_mu);
    *gen = l->poll_ack_gen;
    *ms = l->poll_ack_ms;
    *adopted = l->poll_adopted_ms;
    (void)pthread_mutex_unlock(&l->view_mu);
}

/* Wakes actually posted to the owner. Synchronous: the count a caller reads
 * immediately after an operation already includes every wake that operation
 * sent, so "no wake" is a number rather than the absence of an event within
 * some window. */
unsigned
moqr_admin_listen_test_wake_posts(struct moqr_admin_listen *l)
{
    unsigned n;

    (void)pthread_mutex_lock(&l->view_mu);
    n = l->wake_posts;
    (void)pthread_mutex_unlock(&l->view_mu);
    return n;
}

/* What the clock-choice probe observed: how often it ran, and the two ways the
 * step can be wrong. */
void
moqr_admin_listen_test_clock_probe(struct moqr_admin_listen *l,
                                   moqr_admin_listen_probe_t *out)
{
    /*
     * A READER MAY ONLY RELEASE WHAT IT ACQUIRED.
     *
     * Ignoring the acquisition and unlocking anyway is not a read that failed
     * -- it is a read that hands somebody else's lock back. On the
     * error-checking view lock a self-lock returns EDEADLK, so an accessor
     * called while this thread already holds the mutex would otherwise
     * release a hold it never took, healing exactly the state a test may be
     * about to measure.
     */
    if (pthread_mutex_lock(&l->view_mu) != 0) {
        (void)atomic_fetch_add(&g_test_accessor_lock_failed, 1);
        return;   /* `out` untouched, and nothing released */
    }
    out->hits = l->clock_probe_hits;
    out->unlocked = l->clock_unlocked_samples;
    out->foreign = l->clock_foreign_holder_samples;
    out->errors = l->clock_probe_errors;
    out->disagreements = l->clock_marker_disagreements;
    out->poll_used_ms = l->poll_used_ms;
    (void)pthread_mutex_unlock(&l->view_mu);
}

/* Advance it, and wake the owner so the step is taken now rather than at the
 * next poll expiry. */
static bool
clock_advance_raw(struct moqr_admin_listen *l, uint64_t delta_us)
{
    bool ok;
    /* Wrapping would move the clock backwards by an enormous amount -- the
     * exact failure the floor exists to prevent, arriving through the other
     * door. A step that cannot be taken is refused, not approximated. */
    if (!view_lock_for_clock(l)) {
        return false;
    }
    /* Advancing a clock the owner is not reading changes nothing and would
     * wake it for a step that did not happen. There is no such step. */
    if (!l->logical_clock) {
        ok = false;
    } else if (delta_us > UINT64_MAX - l->logical_now_us) {
        ok = false;
    } else {
        l->logical_now_us += delta_us;
        ok = true;
    }
    if (!view_unlock_for_clock(l)) {
        return false;
    }
    if (ok) {
        moqr_admin_listen_notify(l);
    }
    return ok;
}

/*
 * THE PUBLIC TEST-CLOCK BOUNDARY.
 *
 * Consuming a result is not handling it: `if (arm(l, 1)) { }` reads the value
 * and does nothing when the operation refuses. These are the only clock
 * operations a test can reach, and their signatures make the expected outcome
 * part of the call, so a mismatch is a named failure rather than a silent
 * one.
 */
/*
 * THE COMPARISON PUBLISHES ITS OWN FACTS.
 *
 * A ledger the caller supplies is a destination the caller chooses, and a
 * caller can choose one nobody ever looks at -- a throwaway local, or none at
 * all. Then the mismatch is printed and the run stays green. So every mismatch
 * and every invalid destination is also counted here, at the comparison, where
 * no caller can route it away; the selection's balance compares those totals
 * against the mismatches a case deliberately registered.
 *
 * The caller's ledger remains, for immediate control flow. It is no longer the
 * authority.
 */
static atomic_int g_clock_expect_no_ledger;
static atomic_int g_clock_expect_mismatches;

int
moqr_admin_listen_test_clock_expect_misdirected(void)
{
    return atomic_load(&g_clock_expect_no_ledger);
}

int
moqr_admin_listen_test_clock_expect_mismatches(void)
{
    return atomic_load(&g_clock_expect_mismatches);
}

static void
clock_expect_record(bool ok, bool expect_ok, const char *what,
                    const char *who, int *failures)
{
    /* An invalid destination is invalid whether or not this particular
     * expectation happened to hold: checking it only on mismatch would let a
     * matching call establish the habit unnoticed. */
    if (failures == NULL) {
        printf("  %s: a clock expectation was stated with no failure ledger "
               "to answer to\n", who);
        (void)atomic_fetch_add(&g_clock_expect_no_ledger, 1);
    }
    if (ok == expect_ok) {
        return;
    }
    printf("  %s: %s the clock %s, expected it to %s\n", who, what,
           ok ? "succeeded" : "refused", expect_ok ? "succeed" : "refuse");
    (void)atomic_fetch_add(&g_clock_expect_mismatches, 1);
    if (failures != NULL) {
        (*failures)++;
    }
}

void
moqr_admin_listen_test_clock_arm_expect(struct moqr_admin_listen *l,
                                        uint64_t now_us, bool expect_ok,
                                        const char *who, int *failures)
{
    clock_expect_record(clock_arm_raw(l, now_us), expect_ok, "arming", who,
                        failures);
}

void
moqr_admin_listen_test_clock_advance_expect(struct moqr_admin_listen *l,
                                            uint64_t delta_us, bool expect_ok,
                                            const char *who, int *failures)
{
    clock_expect_record(clock_advance_raw(l, delta_us), expect_ok,
                        "advancing", who, failures);
}

void
moqr_admin_listen_test_pause_at_cancel(struct moqr_admin_listen *l, int on)
{
    (void)pthread_mutex_lock(&l->view_mu);
    l->pause_at_cancel = on;
    (void)pthread_cond_broadcast(&l->view_cv);
    (void)pthread_mutex_unlock(&l->view_mu);
}

static void
owner_pause_point(struct moqr_admin_listen *l)
{
    (void)pthread_mutex_lock(&l->view_mu);
    if (l->pause_at_retire) {
        l->paused = 1;
        l->view_gen++;
        (void)pthread_cond_broadcast(&l->view_cv);
        while (l->pause_at_retire) {
            (void)pthread_cond_wait(&l->view_cv, &l->view_mu);
        }
        l->paused = 0;
        l->view_gen++;
        (void)pthread_cond_broadcast(&l->view_cv);
    }
    (void)pthread_mutex_unlock(&l->view_mu);
}

/*
 * Wait until THIS listener publishes a view newer than `after_gen`.
 *
 * `budget_ms` is a fail-closed safety bound and nothing else: on expiry the
 * caller is told the publication never came, rather than being handed a stale
 * sample that happens to satisfy it.
 */
uint64_t
moqr_admin_listen_test_wait_view(struct moqr_admin_listen *l,
                                 uint64_t after_gen, int budget_ms,
                                 moqr_admin_listen_view_t *out)
{
    struct timespec deadline;
    uint64_t gen;

    (void)clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += budget_ms / 1000;
    deadline.tv_nsec += (long)(budget_ms % 1000) * 1000000L;
    if (deadline.tv_nsec >= 1000000000L) {
        deadline.tv_sec++;
        deadline.tv_nsec -= 1000000000L;
    }
    (void)pthread_mutex_lock(&l->view_mu);
    while (l->view_gen <= after_gen) {
        if (pthread_cond_timedwait(&l->view_cv, &l->view_mu, &deadline) != 0) {
            break;
        }
    }
    gen = l->view_gen;
    if (gen <= after_gen) {
        /*
         * NOTHING NEWER ARRIVED. The caller gets no payload at all -- not the
         * current view, which is by definition the stale one it already had.
         * Copying it out and returning 0 makes the contract depend on every
         * caller remembering to check the result; zeroing it makes a caller
         * that forgets fail loudly instead of reading state that never
         * happened.
         */
        if (out != NULL) {
            memset(out, 0, sizeof(*out));
        }
        (void)pthread_mutex_unlock(&l->view_mu);
        return 0u;
    }
    if (out != NULL) {
        for (uint32_t i = 0; i < MOQR_ADMIN_MAX_CLIENTS; i++) {
            out->state[i] = l->view_state[i];
            out->serial[i] = l->view_serial[i];
            out->bytes[i] = l->view_bytes[i];
            out->fds[i] = l->view_fds[i];
        }
        for (uint32_t b = 0; b < MOQR_ADMIN_BANKS; b++) {
            out->pins[b] = l->view_pins[b];
        }
        out->probe = l->view_probe;
        out->gen = gen;
        out->retiring = l->view_retiring;
        out->retire_serial = l->view_retire_serial;
        out->retire_demand = l->view_retire_demand;
        out->retire_bank = l->view_retire_bank;
        out->retire_taken = l->view_retire_taken;
        out->paused = l->paused;
        out->paused_at_cancel = l->paused_at_cancel;
        for (uint32_t i = 0; i < MOQR_ADMIN_MAX_CLIENTS; i++) {
            out->drop_site[i] = l->drop_site[i];
            out->drop_count[i] = l->drop_count[i];
            out->drop_attempt[i] = l->drop_attempt[i];
        }
        for (int r = 0; r < 3; r++) {
            out->rel_attempt[r] = l->rel_attempt[r];
            out->rel_ok[r] = l->rel_ok[r];
            out->rel_serial[r] = l->rel_serial[r];
            out->rel_bank[r] = l->rel_bank[r];
        }
        out->n_cancel = l->n_cancel;
        out->n_drop = l->n_drop;
        out->n_settle = l->n_settle;
        out->n_bank_release = l->n_bank_release;
        out->n_ack_release = l->n_ack_release;
    }
    (void)pthread_mutex_unlock(&l->view_mu);
    return gen;
}

/* The current view, without waiting. */
uint64_t
moqr_admin_listen_test_view(struct moqr_admin_listen *l,
                            moqr_admin_listen_view_t *out)
{
    return moqr_admin_listen_test_wait_view(l, 0u, 0, out);
}

/* Arm or release the owner-side pause. */
void
moqr_admin_listen_test_pause_at_retire(struct moqr_admin_listen *l, int on)
{
    (void)pthread_mutex_lock(&l->view_mu);
    l->pause_at_retire = on;
    (void)pthread_cond_broadcast(&l->view_cv);
    (void)pthread_mutex_unlock(&l->view_mu);
}

/*
 * A state-machine transition that the listener treats as an invariant failure.
 * These branches are unreachable while the broker and the state machine agree,
 * which is exactly why they are injected: a fail-closed arm nobody executes is
 * a fail-closed arm nobody has checked.
 */
static atomic_int g_test_fail_transition;
static atomic_int g_test_transition_hit;
typedef void (*moqr_admin_test_hook_fn)(void *);
static _Atomic(moqr_admin_test_hook_fn) g_test_on_bytes_failure_hook;
static _Atomic(void *) g_test_on_bytes_failure_ctx;

void moqr_admin_listen_test_fail_transition(int which) {
    if (which != 0) {
        atomic_store(&g_test_transition_hit, 0);
    }
    atomic_store(&g_test_fail_transition, which);
}

const char *
moqr_admin_listen_test_site_name(int site)
{
    static const char *names[] = {
#define MOQR_ADMIN_TEST_SITE_NAME(name) #name,
        MOQR_ADMIN_TEST_SITES(MOQR_ADMIN_TEST_SITE_NAME)
#undef MOQR_ADMIN_TEST_SITE_NAME
    };
    if (site < 0 || site >= (int)MOQR_ADMIN_TEST_FAIL__COUNT) {
        return "?";
    }
    return names[site];
}

int moqr_admin_listen_test_transition_hit(void) {
    return atomic_load(&g_test_transition_hit);
}

void
moqr_admin_listen_test_on_bytes_failure_hook(moqr_admin_test_hook_fn fn,
                                              void *ctx)
{
    atomic_store_explicit(&g_test_on_bytes_failure_ctx, ctx,
                          memory_order_relaxed);
    atomic_store_explicit(&g_test_on_bytes_failure_hook, fn,
                          memory_order_release);
}

/* The poll timeout is a SAFETY BOUND on a lost notification. Removing it makes
 * the notification the only thing that can advance the owner, which is how a
 * dropped notification is proven decisive without making elapsed time the
 * verdict. */
static atomic_int g_test_poll_ms = MOQR_ADMIN_POLL_MS;

void moqr_admin_listen_test_set_poll_ms(int ms) {
    atomic_store(&g_test_poll_ms, ms);
}

#define MOQR_TEST_FAIL(which) (atomic_load(&g_test_fail_##which) != 0)
#define MOQR_TEST_PARKED()    (atomic_load(&g_test_park_owner) != 0)
#define MOQR_TEST_TRANSITION(which) \
    (atomic_load(&g_test_fail_transition) == (which))
#define MOQR_TEST_TRANSITION_HIT(which) \
    (atomic_store(&g_test_transition_hit, (which)))
#define MOQR_POLL_TIMEOUT_MS  (atomic_load(&g_test_poll_ms))
#else
#define MOQR_TEST_FAIL(which) (0)
#define MOQR_TEST_PARKED()    (0)
#define MOQR_TEST_TRANSITION(which) (0)
#define MOQR_TEST_TRANSITION_HIT(which) ((void)0)
#define MOQR_POLL_TIMEOUT_MS  (MOQR_ADMIN_POLL_MS)
#endif

/* The owner cannot honestly serve past this point. Recorded as ONE named
 * state, so the serve loop can fail the relay for a reason rather than for a
 * flag. The first reason wins: later collapse is a consequence, not a cause. */
static void
owner_fail(struct moqr_admin_listen *l, moqr_admin_listen_health_t why)
{
    unsigned expected = MOQR_ADMIN_HEALTH_OK;
    (void)atomic_compare_exchange_strong(&l->health, &expected,
                                         (unsigned)why);
    /*
     * A failed owner is terminal immediately, not merely after owner_turn
     * returns. Several invariant checks sit before the generation handshake;
     * setting only stop would let that same turn enter the non-terminal arm,
     * open another generation and wake lanes after the endpoint had failed.
     */
    atomic_store(&l->terminal, 1);
    atomic_store(&l->stop, 1);
}

static ssize_t admin_write(int fd, const void *buf, size_t len);

#ifdef MOQR_ADMIN_LISTEN_TESTING
/*
 * Refusal-slot syscall results.
 *
 * A fixed 503 on a loopback socket fits in one write, so the kernel will never
 * produce a short write, an EAGAIN, an EINTR or a failed shutdown here. Those
 * results are the property under test, not the kernel, so they are supplied
 * directly. The seam is local to the boundary test's translation unit; the
 * shipped object calls the real syscalls and has no such symbols.
 *
 * A script is a list of results consumed one call at a time, so a sequence like
 * "short write, then EAGAIN, then finish" is expressed exactly rather than
 * provoked by timing.
 */
#define MOQR_ADMIN_IO_SCRIPT_MAX 16

static atomic_int g_io_write_script[MOQR_ADMIN_IO_SCRIPT_MAX];
static atomic_int g_io_write_len;
static atomic_int g_io_write_at;
static atomic_int g_io_shutdown_fail;
static atomic_int g_io_read_calls;
/* Deterministic client-response write control. -1 uses the real syscall, 0
 * reports EAGAIN, and a positive value reports that many bytes once before
 * returning to the held state. Test translation unit only. */
static atomic_int g_client_write_budget = -1;
/* One slot whose writes never make progress, so a single response can be held
 * partial while every other client is served normally. A global budget cannot
 * express that, and the alternative -- a peer that declines to read a body
 * larger than the socket buffer -- puts the fixture back on physical timing. */
static atomic_int g_frozen_slot = -1;

void moqr_admin_listen_test_freeze_client(int slot) {
    atomic_store(&g_frozen_slot, slot);
}

void
moqr_admin_listen_test_script_write(const int *results, int n)
{
    if (n > MOQR_ADMIN_IO_SCRIPT_MAX) {
        n = MOQR_ADMIN_IO_SCRIPT_MAX;
    }
    for (int i = 0; i < n; i++) {
        atomic_store(&g_io_write_script[i], results[i]);
    }
    atomic_store(&g_io_write_len, n);
    atomic_store(&g_io_write_at, 0);
}

void moqr_admin_listen_test_fail_shutdown(int on) {
    atomic_store(&g_io_shutdown_fail, on);
}
void moqr_admin_listen_test_client_write_budget(int bytes) {
    atomic_store(&g_client_write_budget, bytes);
}
int  moqr_admin_listen_test_read_calls(void) {
    return atomic_load(&g_io_read_calls);
}
void moqr_admin_listen_test_reset_read_calls(void) {
    atomic_store(&g_io_read_calls, 0);
}

/*
 * A scripted result: >0 accepts at most that many bytes, 0 = EAGAIN,
 * -1 = EINTR, -2 = a hard error. Anything past the end of the script falls
 * through to the real syscall.
 *
 * A positive result still performs a REAL write of the accepted prefix, so a
 * short write is genuinely short on the wire and the peer sees exactly what
 * the resumed offsets produced. Faking the bytes as well would prove only that
 * the counter arithmetic agrees with itself.
 */
static bool
io_next_write(ssize_t *out, int fd, const void *buf, size_t cap)
{
    int at = atomic_load(&g_io_write_at);
    int len = atomic_load(&g_io_write_len);
    int r;
    if (at >= len) {
        return false;
    }
    atomic_store(&g_io_write_at, at + 1);
    r = atomic_load(&g_io_write_script[at]);
    if (r > 0) {
        size_t want = (size_t)r > cap ? cap : (size_t)r;
        *out = admin_write(fd, buf, want);
        return true;
    }
    if (r == 0) {
        errno = EAGAIN;
    } else if (r == -1) {
        errno = EINTR;
    } else {
        errno = ECONNRESET;
    }
    *out = -1;
    return true;
}

static bool
io_next_client_write(ssize_t *out, size_t cap, int slot)
{
    int budget = atomic_load(&g_client_write_budget);

    if (atomic_load(&g_frozen_slot) == slot) {
        errno = EAGAIN;
        *out = -1;
        return true;
    }

    if (budget < 0) {
        return false;
    }
    if (budget == 0) {
        errno = EAGAIN;
        *out = -1;
        return true;
    }
    if (!atomic_compare_exchange_strong(&g_client_write_budget, &budget, 0)) {
        errno = EAGAIN;
        *out = -1;
        return true;
    }
    *out = (ssize_t)((size_t)budget < cap ? (size_t)budget : cap);
    return true;
}
#define MOQR_IO_SHUTDOWN_FAILS() (atomic_load(&g_io_shutdown_fail) != 0)
#define MOQR_IO_COUNT_READ()     (atomic_fetch_add(&g_io_read_calls, 1))
#else
#define MOQR_IO_SHUTDOWN_FAILS() (0)
#define MOQR_IO_COUNT_READ()     ((void)0)
#endif

/* The refusal slot's half-close, routed through one call so its failure is
 * both injectable and handled in exactly one place. */
static int
admin_shutdown_wr(int fd)
{
    if (MOQR_IO_SHUTDOWN_FAILS()) {
        errno = ENOTCONN;
        return -1;
    }
    return shutdown(fd, SHUT_WR);
}

/* Which transition the seam above makes fail. Named here so the listener and
 * its boundary test agree on one vocabulary. */
/* The other two destructive releases. Each site has its own id because each is
 * reached by a different path and owes a different transaction: the ordinary
 * bank release is acknowledged to the state machine, the abandoned settlement
 * runs inside a non-yielding transaction, and the no-carrier retirement has no
 * admin record at all. One shared seam could not tell them apart. */
/* Settlement reports success without the release callback having run -- the
 * one shape in which the admin side is settled for a token nobody gave back. */
/* The generation-wide failure refuses. */

/*
 * The state-machine calls whose failure is an invariant contradiction. Each is
 * routed through one wrapper so an injected failure is indistinguishable from
 * the real one, and so each classification has exactly one place to live.
 */
static moqr_result_t
admin_close(struct moqr_admin_listen *l, uint32_t client)
{
    if (MOQR_TEST_TRANSITION(MOQR_ADMIN_TEST_FAIL_CLOSE)) {
        MOQR_TEST_TRANSITION_HIT(MOQR_ADMIN_TEST_FAIL_CLOSE);
        return MOQR_ERR_WRONG_STATE;
    }
    return moqr_admin_close(&l->admin, client);
}

static moqr_result_t
admin_on_bytes(struct moqr_admin_listen *l, uint32_t client, const char *buf,
               size_t n, uint64_t now_us)
{
    if (MOQR_TEST_TRANSITION(MOQR_ADMIN_TEST_FAIL_ON_BYTES)) {
        MOQR_TEST_TRANSITION_HIT(MOQR_ADMIN_TEST_FAIL_ON_BYTES);
#ifdef MOQR_ADMIN_LISTEN_TESTING
        moqr_admin_test_hook_fn fn = atomic_load_explicit(
            &g_test_on_bytes_failure_hook, memory_order_acquire);
        if (fn != NULL) {
            fn(atomic_load_explicit(&g_test_on_bytes_failure_ctx,
                                    memory_order_relaxed));
        }
#endif
        return MOQR_ERR_WRONG_STATE;
    }
    return moqr_admin_on_bytes(&l->admin, client, buf, n, now_us);
}

static moqr_result_t
admin_fail_serial(struct moqr_admin_listen *l, uint64_t serial,
                  moqr_http_status_t status, uint64_t now_us)
{
    if (MOQR_TEST_TRANSITION(MOQR_ADMIN_TEST_FAIL_FAIL_SERIAL)) {
        MOQR_TEST_TRANSITION_HIT(MOQR_ADMIN_TEST_FAIL_FAIL_SERIAL);
        return MOQR_ERR_INVAL;
    }
    return moqr_admin_fail_serial(&l->admin, serial, status, now_us);
}

static moqr_result_t
admin_ack_release(struct moqr_admin_listen *l, uint64_t serial, uint32_t bank)
{
    if (MOQR_TEST_TRANSITION(MOQR_ADMIN_TEST_FAIL_ACK_RELEASE)) {
        MOQR_TEST_TRANSITION_HIT(MOQR_ADMIN_TEST_FAIL_ACK_RELEASE);
        return MOQR_ERR_INVAL;
    }
    return moqr_admin_ack_release(&l->admin, serial, bank);
}

static moqr_result_t
admin_reserve(struct moqr_admin_listen *l, uint32_t bank, uint64_t serial)
{
    if (MOQR_TEST_TRANSITION(MOQR_ADMIN_TEST_FAIL_RESERVE)) {
        MOQR_TEST_TRANSITION_HIT(MOQR_ADMIN_TEST_FAIL_RESERVE);
        return MOQR_ERR_INVAL;
    }
    return moqr_admin_reserve(&l->admin, bank, serial);
}

static moqr_result_t
admin_settle_abandon(struct moqr_admin_listen *l, uint64_t serial,
                     moqr_admin_release_fn release, void *ctx)
{
    if (MOQR_TEST_TRANSITION(MOQR_ADMIN_TEST_FAIL_SETTLE)) {
        MOQR_TEST_TRANSITION_HIT(MOQR_ADMIN_TEST_FAIL_SETTLE);
        return MOQR_ERR_INVAL;
    }
    if (MOQR_TEST_TRANSITION(MOQR_ADMIN_TEST_FAIL_SETTLE_SILENT)) {
        MOQR_TEST_TRANSITION_HIT(MOQR_ADMIN_TEST_FAIL_SETTLE_SILENT);
        return MOQR_OK;   /* settled, but nothing was released */
    }
    return moqr_admin_settle_abandon(&l->admin, serial, release, ctx);
}

/* The exact destructive take after this owner froze a complete generation. */
static bool
broker_take_ready(struct moqr_admin_listen *l, uint64_t serial,
                  uint32_t *demand, uint32_t *bank)
{
    if (MOQR_TEST_TRANSITION(MOQR_ADMIN_TEST_FAIL_TAKE_READY)) {
        MOQR_TEST_TRANSITION_HIT(MOQR_ADMIN_TEST_FAIL_TAKE_READY);
        return false;
    }
    return moqr_broker_take_serial(l->broker, serial, demand, bank);
}

/*
 * THE THREE DESTRUCTIVE BROKER RELEASES, one wrapper each.
 *
 * They are not interchangeable. The ordinary bank release is followed by an
 * acknowledgement to the state machine; the abandoned settlement runs inside a
 * transaction that must not yield before the admin side is settled; the
 * no-carrier retirement has no admin record behind it at all. Sharing one seam
 * would make a failure at any of them indistinguishable from a failure at the
 * others, which is exactly what an exact-call seam is for.
 */
/* One ledger entry per destructive release: which site, whether it succeeded,
 * and the identity it released. A count alone cannot distinguish "one release
 * through the expected site" from "none at all". */
static void
rel_record(struct moqr_admin_listen *l, int site, uint64_t serial,
           uint32_t bank, bool ok)
{
#ifdef MOQR_ADMIN_LISTEN_TESTING
    (void)pthread_mutex_lock(&l->view_mu);
    l->rel_attempt[site]++;
    if (ok) {
        l->rel_ok[site]++;
        l->rel_serial[site] = (unsigned long long)serial;
        l->rel_bank[site] = bank;
    }
    (void)pthread_mutex_unlock(&l->view_mu);
#else
    (void)l; (void)site; (void)serial; (void)bank; (void)ok;
#endif
}

static moqr_result_t
admin_bank_release(struct moqr_admin_listen *l, uint64_t serial, uint32_t bank,
                   bool *wake)
{
    moqr_result_t rc;
    if (MOQR_TEST_TRANSITION(MOQR_ADMIN_TEST_FAIL_BANK_RELEASE)) {
        MOQR_TEST_TRANSITION_HIT(MOQR_ADMIN_TEST_FAIL_BANK_RELEASE);
        rel_record(l, MOQR_ADMIN_REL_BANK, serial, bank, false);
        return MOQR_ERR_INVAL;
    }
    rc = moqr_broker_release(l->broker, serial, bank, wake);
    rel_record(l, MOQR_ADMIN_REL_BANK, serial, bank, rc == MOQR_OK);
    return rc;
}

/* Inside moqr_admin_settle_abandon's callback: the admin side is NOT settled
 * yet when this runs. */
static moqr_result_t
admin_settle_release(struct moqr_admin_listen *l, uint64_t serial,
                     uint32_t bank, bool *wake)
{
    moqr_result_t rc;
    if (MOQR_TEST_TRANSITION(MOQR_ADMIN_TEST_FAIL_SETTLE_RELEASE)) {
        MOQR_TEST_TRANSITION_HIT(MOQR_ADMIN_TEST_FAIL_SETTLE_RELEASE);
        rel_record(l, MOQR_ADMIN_REL_SETTLE, serial, bank, false);
        return MOQR_ERR_INVAL;
    }
    rc = moqr_broker_release(l->broker, serial, bank, wake);
    rel_record(l, MOQR_ADMIN_REL_SETTLE, serial, bank, rc == MOQR_OK);
    return rc;
}

/* A generation the state machine never knew: signal-only demand poisoned
 * before any client or record existed. There is no admin token to lose here,
 * which is precisely why it is retired directly. */
static moqr_result_t
admin_nocarrier_release(struct moqr_admin_listen *l, uint64_t serial,
                        uint32_t bank, bool *wake)
{
    moqr_result_t rc;
    if (MOQR_TEST_TRANSITION(MOQR_ADMIN_TEST_FAIL_NOCARRIER_RELEASE)) {
        MOQR_TEST_TRANSITION_HIT(MOQR_ADMIN_TEST_FAIL_NOCARRIER_RELEASE);
        rel_record(l, MOQR_ADMIN_REL_NOCARRIER, serial, bank, false);
        return MOQR_ERR_INVAL;
    }
    rc = moqr_broker_release(l->broker, serial, bank, wake);
    rel_record(l, MOQR_ADMIN_REL_NOCARRIER, serial, bank, rc == MOQR_OK);
    return rc;
}

static moqr_result_t
admin_accept(struct moqr_admin_listen *l, uint64_t now_us,
             uint32_t *out_client)
{
    if (MOQR_TEST_TRANSITION(MOQR_ADMIN_TEST_FAIL_ACCEPT)) {
        MOQR_TEST_TRANSITION_HIT(MOQR_ADMIN_TEST_FAIL_ACCEPT);
        return MOQR_ERR_INVAL;
    }
    return moqr_admin_accept(&l->admin, now_us, out_client);
}

static moqr_result_t
admin_bind_serial(struct moqr_admin_listen *l, uint32_t client,
                  uint64_t serial, uint64_t now_us)
{
    if (MOQR_TEST_TRANSITION(MOQR_ADMIN_TEST_FAIL_BIND_SERIAL)) {
        MOQR_TEST_TRANSITION_HIT(MOQR_ADMIN_TEST_FAIL_BIND_SERIAL);
        return MOQR_ERR_INVAL;
    }
    return moqr_admin_bind_serial(&l->admin, client, serial, now_us);
}

static moqr_result_t
admin_refuse(struct moqr_admin_listen *l, uint32_t client,
             moqr_http_status_t status, uint64_t now_us)
{
    if (MOQR_TEST_TRANSITION(MOQR_ADMIN_TEST_FAIL_REFUSE)) {
        MOQR_TEST_TRANSITION_HIT(MOQR_ADMIN_TEST_FAIL_REFUSE);
        return MOQR_ERR_WRONG_STATE;
    }
    return moqr_admin_refuse(&l->admin, client, status, now_us);
}

static moqr_result_t
admin_on_written(struct moqr_admin_listen *l, uint32_t client, size_t n,
                 uint64_t now_us)
{
    if (MOQR_TEST_TRANSITION(MOQR_ADMIN_TEST_FAIL_ON_WRITTEN)) {
        MOQR_TEST_TRANSITION_HIT(MOQR_ADMIN_TEST_FAIL_ON_WRITTEN);
        return MOQR_ERR_WRONG_STATE;
    }
    return moqr_admin_on_written(&l->admin, client, n, now_us);
}

static moqr_result_t
admin_tick(struct moqr_admin_listen *l, uint64_t now_us)
{
    if (MOQR_TEST_TRANSITION(MOQR_ADMIN_TEST_FAIL_TICK)) {
        MOQR_TEST_TRANSITION_HIT(MOQR_ADMIN_TEST_FAIL_TICK);
        return MOQR_ERR_INVAL;
    }
    return moqr_admin_tick(&l->admin, now_us);
}

static moqr_result_t
admin_abort(struct moqr_admin_listen *l, uint32_t bank, uint64_t serial)
{
    if (MOQR_TEST_TRANSITION(MOQR_ADMIN_TEST_FAIL_ABORT)) {
        MOQR_TEST_TRANSITION_HIT(MOQR_ADMIN_TEST_FAIL_ABORT);
        return MOQR_ERR_INVAL;
    }
    return moqr_admin_abort(&l->admin, bank, serial);
}

static moqr_result_t
admin_commit(struct moqr_admin_listen *l, uint32_t bank, uint64_t serial,
             const size_t len[MOQR_ADMIN_BODY__COUNT], uint64_t now_us)
{
    if (MOQR_TEST_TRANSITION(MOQR_ADMIN_TEST_FAIL_COMMIT)) {
        MOQR_TEST_TRANSITION_HIT(MOQR_ADMIN_TEST_FAIL_COMMIT);
        return MOQR_ERR_INVAL;
    }
    return moqr_admin_commit(&l->admin, bank, serial, len, now_us);
}

static moqr_result_t
admin_record_take(struct moqr_admin_listen *l, uint64_t serial,
                  uint32_t demand, uint32_t bank)
{
    if (MOQR_TEST_TRANSITION(MOQR_ADMIN_TEST_FAIL_RECORD_TAKE)) {
        MOQR_TEST_TRANSITION_HIT(MOQR_ADMIN_TEST_FAIL_RECORD_TAKE);
        return MOQR_ERR_INVAL;
    }
    return moqr_admin_record_take(&l->admin, serial, demand, bank);
}

static moqr_result_t
broker_request(struct moqr_admin_listen *l, uint32_t demand,
               uint64_t *serial, bool *wake)
{
    if (MOQR_TEST_TRANSITION(MOQR_ADMIN_TEST_FAIL_BROKER_REQUEST)) {
        MOQR_TEST_TRANSITION_HIT(MOQR_ADMIN_TEST_FAIL_BROKER_REQUEST);
        return MOQR_ERR_INVAL;
    }
    return moqr_broker_request(l->broker, demand, serial, wake);
}

/*
 * Cancellation happens ONCE, whichever site reaches it first.
 *
 * There are two: the terminal turn, and the owner's exit sequence. On a
 * shutdown that is also terminal both are reached, and cancelling twice would
 * answer clients that were already answered -- and would make "exactly once"
 * unprovable, because the count would depend on which site won.
 */
static void
owner_cancel_once(struct moqr_admin_listen *l, uint64_t now_us)
{
    if (l->terminal_cancelled) {
        return;
    }
    l->terminal_cancelled = true;
    moqr_admin_cancel(&l->admin, now_us);
#ifdef MOQR_ADMIN_LISTEN_TESTING
    owner_count(l, &l->n_cancel);
    owner_pause_at_cancel(l);
#endif

}

/*
 * Write to an admin socket without ever raising SIGPIPE in this process.
 *
 * A scraper that closes early must not be able to terminate the relay. The
 * suppression is PER SOCKET or PER CALL -- never the process-global signal
 * disposition, which belongs to the embedding application and not to a metrics
 * endpoint.
 */
static ssize_t
admin_write(int fd, const void *buf, size_t len)
{
#if defined(MSG_NOSIGNAL)
    return send(fd, buf, len, MSG_NOSIGNAL);
#else
    return write(fd, buf, len);
#endif
}

/* The other half of the same guarantee, for platforms that suppress at the
 * socket rather than at the call. Contract-establishing: a socket that cannot
 * be made SIGPIPE-safe is not accepted. */
static bool
set_nosigpipe(int fd)
{
#if defined(SO_NOSIGPIPE)
    int on = 1;
    return setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &on, sizeof(on)) == 0;
#elif defined(MSG_NOSIGNAL)
    (void)fd;
    return true;   /* suppressed per call, above */
#else
#error "no way to suppress SIGPIPE on an admin socket"
#endif
}

#ifdef MOQR_ADMIN_LISTEN_TESTING
/*
 * A LOCK-OWNERSHIP PROBE, evaluated where the real clock is sampled.
 *
 * Choosing which clock to read and reading it must be one step under the view
 * lock; if they were two, arming could land between them and move the owner
 * backwards. The marker this reads is maintained by the checked boundary
 * above, so what it reports is ownership by THIS thread -- not that some
 * thread somewhere holds the lock, which is a different and much weaker fact.
 * `hits` is the positive half: a counter that only ever stays at zero says the
 * same thing whether the step is sound or the probe is gone.
 *
 * It is a state question, so it needs no scheduling, no second thread, and no
 * elapsed time to answer.
 */
#endif

/* A monotonic clock. On failure the owner fails closed: a zero timestamp would
 * make every deadline unreachable, which is the opposite of what a broken clock
 * should cause. */
static bool
clock_now_us(struct moqr_admin_listen *l, uint64_t *out)
{
    struct timespec ts;
#ifdef MOQR_ADMIN_LISTEN_TESTING
    bool ok;
#endif
    (void)l;   /* the logical clock below is a testing-build seam */
    if (MOQR_TEST_FAIL(clock)) {
        return false;
    }
#ifdef MOQR_ADMIN_LISTEN_TESTING
    /*
     * A TEST-OWNED LOGICAL CLOCK.
     *
     * The interior deadlines -- the epoch deadline above all -- are the state
     * transitions these ownership proofs are about. Waiting two real seconds
     * for one is not a proof that the transition happens, it is a proof that
     * time passes; and it makes the case's cost the production budget rather
     * than the work. When a logical clock is armed the owner reads it instead,
     * so the test advances the state deliberately and the socket boundary
     * stays the only real thing in the case.
     */
    /*
     * CHOOSING A CLOCK AND READING IT ARE ONE STEP.
     *
     * If the choice were made under the lock and the real sample taken after
     * releasing it, arming could land between the two: the owner would decide
     * to use real time, arming would install an origin sampled earlier, and
     * the owner's next turn would read a time before the one it had just
     * used. Both happen here under the same lock, and the real reading is
     * recorded so arming has something to floor against.
     */
    /*
     * ONE TAKE, ONE RELEASE, ONE EXIT.
     *
     * Everything between them runs under the lock, and there is no second
     * release for a broken shape to hide behind: an implementation that
     * released early would have to release twice, and one that returned early
     * would leave the lock held. `check_listener_contracts` pins exactly that
     * -- one of each call, no return between them, and the sample inside.
     */
    if (!view_lock_for_clock(l)) {
        return false;
    }
    if (l->logical_clock) {
        *out = l->logical_now_us;
        ok = true;
    } else {
        clock_lock_probe(l);
        if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
            ok = false;
        } else {
            *out = ((uint64_t)ts.tv_sec * 1000000ull) +
                   ((uint64_t)ts.tv_nsec / 1000ull);
            l->last_real_us = *out;
            ok = true;
        }
    }
    /* A release that refuses is a lock still held: the caller may not be told
     * the reading is good. */
    return view_unlock_for_clock(l) && ok;
#else
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return false;
    }
    *out = ((uint64_t)ts.tv_sec * 1000000ull) + ((uint64_t)ts.tv_nsec / 1000ull);
    return true;
#endif
}

static bool
set_nonblocking(int fd)
{
    int fl = fcntl(fd, F_GETFL, 0);
    if (fl < 0) {
        return false;
    }
    return fcntl(fd, F_SETFL, fl | O_NONBLOCK) == 0;
}

static void
close_fd(int *fd)
{
    if (*fd >= 0) {
        (void)close(*fd);   /* a close error leaves nothing to recover */
        *fd = -1;
    }
}

/* -- the wake ------------------------------------------------------------- */

/* A self-pipe, not a flag: the owner blocks in poll(), and a flag it is not
 * waiting on cannot wake it. A byte in the pipe is level-triggered, so a
 * publication that lands between the owner's check and its poll is still
 * pending when the poll runs. */
static bool
wake_arm(struct moqr_admin_listen *l)
{
    int fds[2];
    if (pipe(fds) != 0) {
        return false;
    }
    if (!set_nonblocking(fds[0]) || !set_nonblocking(fds[1])) {
        (void)close(fds[0]);
        (void)close(fds[1]);
        return false;
    }
    l->wake_r = fds[0];
    l->wake_w = fds[1];
    return true;
}

static void
wake_post(struct moqr_admin_listen *l)
{
    const char b = 'w';
#ifdef MOQR_ADMIN_LISTEN_TESTING
    (void)pthread_mutex_lock(&l->view_mu);
    l->wake_posts++;
    (void)pthread_mutex_unlock(&l->view_mu);
#endif
    if (l->wake_w >= 0) {
        ssize_t n = write(l->wake_w, &b, 1);
        (void)n;   /* a full pipe already carries a pending wake */
    }
}

static void
wake_drain(struct moqr_admin_listen *l)
{
    char buf[64];
    for (;;) {
        ssize_t n = read(l->wake_r, buf, sizeof(buf));
        if (n <= 0) {
            break;
        }
    }
}

#ifdef MOQR_ADMIN_LISTEN_TESTING
/*
 * The drain, with the lock measured on BOTH sides of it.
 *
 * Measuring once before the drain would miss a release taken between the
 * measurement and the drain itself; measuring once after would miss one taken
 * before. The drain happens here, between two readings of the real mutex, so
 * the published fact is about the drain rather than about a nearby line.
 */
static int
poll_drain_measured(struct moqr_admin_listen *l)
{
    int before = poll_drain_probe(l);
    int after;

    wake_drain(l);
    after = poll_drain_probe(l);
    return (before == 1 && after == 1) ? 1 : 0;
}
#endif


/*
 * Consume a wake obligation exactly once. A discarded wake can be the only
 * notification a generation will ever get.
 *
 * A wake that could not reach every lane is a FAILED wake: the generation can
 * never complete, so the endpoint fails closed rather than waiting forever on
 * rows that will not arrive.
 */
static void
consume_wake(struct moqr_admin_listen *l, bool wake)
{
    moqr_admin_wake_all_fn fn = l->wake_all;
    if (!wake) {
        return;
    }
    if (atomic_load(&l->terminal)) {
        return;   /* no new lane wakes after terminality */
    }
    atomic_fetch_add(&l->wakes, 1u);
    if (fn != NULL && fn(l->ctx) != MOQR_OK) {
        owner_fail(l, MOQR_ADMIN_HEALTH_WAKE_FAILED);
    }
}

/* -- the refusal slot ----------------------------------------------------- */

/* One bounded slot outside the client table. It writes a fixed 503 and closes.
 * It never parses, never takes a generation, never pins a bank, never
 * allocates. */
static bool
refuse_take(struct moqr_admin_listen *l, int fd, uint64_t t)
{
    size_t n;
    if (l->refuse_fd >= 0) {
        return false;   /* occupied: never aliased or overwritten */
    }
    n = moqr_http_write_head(MOQR_HTTP_503, MOQR_HTTP_REP_METRICS,
                             MOQR_OBS_FMT_OPENMETRICS_100, 0,
                             l->refuse_head, sizeof(l->refuse_head));
    if (n == 0) {
        return false;   /* refuse to half-answer */
    }
    l->refuse_fd = fd;
    l->refuse_len = n;
    l->refuse_sent = 0;
    l->refuse_start_us = t;
    l->refuse_draining = false;
    atomic_fetch_add(&l->refusals, 1u);
    return true;
}

static void
refuse_drop(struct moqr_admin_listen *l)
{
    close_fd(&l->refuse_fd);
    l->refuse_len = 0;
    l->refuse_sent = 0;
    l->refuse_draining = false;
}

/*
 * Advance the refusal slot by at most one bounded step.
 *
 * Called on EVERY owner turn, not only when the descriptor reports an event. A
 * deadline that is only examined when the peer does something is not a
 * deadline: a peer that connects, sends nothing and never closes would hold
 * the single slot against every later overflow connection forever.
 */
static void
refuse_step(struct moqr_admin_listen *l, uint64_t t, short revents)
{
    ssize_t n;

    if (l->refuse_fd < 0) {
        return;
    }
    if ((uint64_t)(t - l->refuse_start_us) > MOQR_ADMIN_REFUSE_DEADLINE_US) {
        refuse_drop(l);   /* will not finish its own 503; free the slot */
        return;
    }
    if ((revents & (POLLERR | POLLNVAL)) != 0) {
        refuse_drop(l);
        return;
    }
    if (l->refuse_draining) {
        /* ONE bounded read per turn, the same rule every other socket in this
         * loop follows. Consume what has already arrived, then close: the
         * half-close has told the peer the response is complete, so waiting
         * for the peer's own EOF would hold the slot for as long as it chose
         * to keep its socket open. */
        char sink[MOQR_ADMIN_READ_CHUNK];
        if ((revents & (POLLIN | POLLHUP)) == 0) {
            return;       /* nothing to drain yet; the deadline still runs */
        }
        MOQR_IO_COUNT_READ();
        n = read(l->refuse_fd, sink, sizeof(sink));
        if (n > 0 && (size_t)n == sizeof(sink)) {
            return;       /* more may be buffered; resume next turn */
        }
        refuse_drop(l);
        return;
    }
    if ((revents & POLLOUT) == 0) {
        return;           /* not writable yet; the deadline still runs */
    }
#ifdef MOQR_ADMIN_LISTEN_TESTING
    if (!io_next_write(&n, l->refuse_fd, l->refuse_head + l->refuse_sent,
                       l->refuse_len - l->refuse_sent))
#endif
    {
        n = admin_write(l->refuse_fd, l->refuse_head + l->refuse_sent,
                        l->refuse_len - l->refuse_sent);
    }
    if (n > 0) {
        l->refuse_sent += (size_t)n;
        if (l->refuse_sent >= l->refuse_len) {
            /*
             * The half-close is what tells the peer its 503 is complete. If it
             * fails, the peer has been told nothing and there is no point
             * holding the single slot open to drain a conversation that cannot
             * be ended cleanly: the slot is released at once so the next
             * overflow connection can have it.
             */
            if (admin_shutdown_wr(l->refuse_fd) != 0) {
                refuse_drop(l);
                return;
            }
            l->refuse_draining = true;
        }
        return;
    }
    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
        return;   /* resume next turn, under the deadline */
    }
    refuse_drop(l);
}

/* -- the owner turn ------------------------------------------------------- */

static void drop_client_inner(struct moqr_admin_listen *l, uint32_t i);

/*
 * Every client drop names the site that performed it.
 *
 * Two cleanup sites exist -- the DONE sweep in an ordinary turn and the
 * owner's exit sequence -- and an aggregate count cannot tell them apart, nor
 * a final empty table tell one drop from three. Recording the site and the
 * count PER SLOT is what makes "this slot, dropped exactly once, by that site"
 * an observation.
 */
static void
drop_client_from(struct moqr_admin_listen *l, uint32_t i, unsigned char site)
{
#ifdef MOQR_ADMIN_LISTEN_TESTING
    /* The ATTEMPT is recorded whether or not it finds a descriptor: this
     * function is idempotent, so a second call is invisible in the effect and
     * only visible in the attempt. "Dropped exactly once" is a statement about
     * how many times the site asked, not about how many times it succeeded. */
    (void)pthread_mutex_lock(&l->view_mu);
    if (l->drop_attempt[i] < 255u) {
        l->drop_attempt[i]++;
    }
    if (l->client_fd[i] >= 0) {
        l->drop_site[i] = site;
        if (l->drop_count[i] < 255u) {
            l->drop_count[i]++;
        }
    }
    (void)pthread_mutex_unlock(&l->view_mu);
#else
    (void)site;
#endif
    drop_client_inner(l, i);
}

static void
drop_client_inner(struct moqr_admin_listen *l, uint32_t i)
{
    if (l->client_fd[i] >= 0) {
        close_fd(&l->client_fd[i]);
        /*
         * close() releases every pin this client held, exactly once.
         *
         * WRONG_STATE means the slot was already FREE -- and in THIS owner
         * nothing else can free it: cancellation moves clients to WRITING or
         * DONE, never FREE, and only this call frees, exactly once per live
         * descriptor. So a refusal means the descriptor table and the state
         * machine disagree about which slots are occupied, and every later
         * index is suspect.
         */
        if (admin_close(l, i) != MOQR_OK) {
            owner_fail(l, MOQR_ADMIN_HEALTH_INVARIANT);
        }
#ifdef MOQR_ADMIN_LISTEN_TESTING
        owner_count(l, &l->n_drop);
#endif
    }
}

static bool
slot_free(const struct moqr_admin_listen *l)
{
    for (uint32_t i = 0; i < MOQR_ADMIN_MAX_CLIENTS; i++) {
        if (l->client_fd[i] < 0 &&
            l->admin.client[i].state == MOQR_ADMIN_CS_FREE) {
            return true;
        }
    }
    return false;
}

/* -- poisoned generations -------------------------------------------------
 *
 * A poisoned generation owes its SIGNAL sink an explicit suppression
 * diagnostic, and whether it carries SIGNAL demand is only knowable from the
 * authoritative token -- which is taken later, in the settle pass. The serial
 * is therefore noted here and consumed there. The set is bounded by the
 * generation count and a serial of 0 is an empty entry. */
static void
poison_note(struct moqr_admin_listen *l, uint64_t serial)
{
    for (uint32_t i = 0; i < MOQR_ADMIN_MAX_CLIENTS; i++) {
        if (l->poisoned[i] == serial) {
            return;   /* already noted; never twice */
        }
    }
    for (uint32_t i = 0; i < MOQR_ADMIN_MAX_CLIENTS; i++) {
        if (l->poisoned[i] == 0u) {
            l->poisoned[i] = serial;
            return;
        }
    }
    /* More poisoned generations in flight than there are generation slots is
     * a contradiction, not a capacity problem. */
    owner_fail(l, MOQR_ADMIN_HEALTH_INVARIANT);
}

static bool
poison_take(struct moqr_admin_listen *l, uint64_t serial)
{
    for (uint32_t i = 0; i < MOQR_ADMIN_MAX_CLIENTS; i++) {
        if (l->poisoned[i] == serial && serial != 0u) {
            l->poisoned[i] = 0u;
            return true;
        }
    }
    return false;
}

/*
 * Serve the exact collecting generation, if it is complete.
 *
 * The order is the whole point. Completeness is established FIRST, on copied
 * rows, because a take is destructive and a reservation that must be undone
 * owes a release for a token that was never issued. Only once the epoch is
 * known whole is it frozen, taken, its exact bank reserved, and rendered --
 * into that bank's storage, which nothing else can reach while it is RESERVED.
 *
 * EVERY destructive take in this function is followed, without a return in
 * between, by a durable carrier for the token it consumed: a reserved bank
 * (which carries its own release once aborted) or a recorded retirement. The
 * one case where neither is possible is a contradiction between the broker and
 * the state machine, and that is a named terminal -- never an ignored return.
 */
static void
owner_produce(struct moqr_admin_listen *l, uint64_t t)
{
    moqr_admin_collect_fn collect = l->collect;
    moqr_admin_render_fn render = l->render;
    moqr_admin_emit_signal_fn emit = l->emit_signal;
    uint64_t serial = 0;
    uint32_t demand = 0;
    uint32_t bank = 0;
    size_t len[MOQR_ADMIN_BODY__COUNT];
    char *dst[MOQR_ADMIN_BODY__COUNT];
    size_t dcap[MOQR_ADMIN_BODY__COUNT];
    moqr_result_t rc;

    if (atomic_load(&l->terminal)) {
        return;
    }
    if (!moqr_broker_current(l->broker, &serial, &demand)) {
        return;
    }
    /* A generation with nowhere to be rendered must not be frozen: freezing it
     * would strand it READY with no bank until a pin happened to free. */
    if (!moqr_admin_bank_available(&l->admin)) {
        return;
    }
    rc = collect(l->ctx, serial);
    if (rc == MOQR_ERR_WOULD_BLOCK) {
        return;   /* still COLLECTING; the publish predicate will wake us */
    }
    if (rc != MOQR_OK) {
        /*
         * POISONED. Nothing is taken here.
         *
         * A take is destructive, and at this point there is no carrier able to
         * hold the token: the generation is JOINED, and the state machine
         * accepts a recorded take only once a retirement has been CLAIMED. So
         * the failure is declared first -- which answers every unstarted
         * waiter 500 and makes the generation retirable -- and the settle pass
         * performs the take against the retirement record that then exists.
         *
         * A generation with no admin carrier at all (signal-only demand has no
         * client and no generation record) is refused by fail_serial; that one
         * is retired directly against the broker, where losing an admin token
         * is not possible because there is none.
         */
        poison_note(l, serial);
        {
        moqr_result_t frc = admin_fail_serial(l, serial, MOQR_HTTP_500, t);
        if (frc != MOQR_OK) {
            bool wake = false;
            /*
             * ONE shape is expected here, and it is not "anything that is not
             * OK": a generation carrying no HTTP demand has no client and no
             * generation record, so the state machine refuses an identity it
             * has never heard of. Anything else -- a refusal while an HTTP
             * waiter exists, or any result other than that documented
             * unknown-identity one -- means a waiter is owed a response that
             * this path is about to pretend was delivered.
             */
            if ((demand & MOQR_BROKER_DEMAND_HTTP) != 0u ||
                frc != MOQR_ERR_INVAL) {
                owner_fail(l, MOQR_ADMIN_HEALTH_INVARIANT);
                return;
            }
            moqr_broker_on_complete(l->broker, serial);
            if (!moqr_broker_take_serial(l->broker, serial, &demand, &bank)) {
                /* Frozen a moment ago by this same thread, and only this
                 * thread takes. */
                owner_fail(l, MOQR_ADMIN_HEALTH_INVARIANT);
                return;
            }
            (void)poison_take(l, serial);
            if (admin_nocarrier_release(l, serial, bank, &wake) !=
                MOQR_OK) {
                owner_fail(l, MOQR_ADMIN_HEALTH_INVARIANT);
                return;
            }
            if ((demand & MOQR_BROKER_DEMAND_SIGNAL) != 0u &&
                l->emit_suppressed != NULL) {
                l->emit_suppressed(l->ctx, serial);
            }
            consume_wake(l, wake);
        }
        }
        return;
    }

    moqr_broker_on_complete(l->broker, serial);
    if (!broker_take_ready(l, serial, &demand, &bank)) {
        /* This sole owner just read this COLLECTING serial and froze it. An
         * exact take cannot legitimately fail; returning would strand the
         * generation READY forever. */
        owner_fail(l, MOQR_ADMIN_HEALTH_INVARIANT);
        return;
    }
    /* From here the token IS consumed. A bank must carry it. */
    if (bank >= MOQR_ADMIN_BANKS ||
        admin_reserve(l, bank, serial) != MOQR_OK) {
        /* The broker pinned a bank the state machine cannot reserve, or named
         * one that does not exist. There is no carrier for the token that was
         * just consumed, and inventing one would be worse than stopping. */
        owner_fail(l, MOQR_ADMIN_HEALTH_INVARIANT);
        return;
    }
    for (uint32_t k = 0; k < MOQR_ADMIN_BODY__COUNT; k++) {
        dcap[k] = 0;
        dst[k] = moqr_admin_bank_storage(&l->admin, bank, serial,
                                         (moqr_obs_format_t)k, &dcap[k]);
        len[k] = 0;
        if (dst[k] == NULL) {
            /* The bank is reserved for this exact serial, so storage must
             * exist. Settle what can be settled, but do not keep serving from
             * a bank whose checked allocation and state disagree. */
            (void)admin_abort(l, bank, serial);
            (void)admin_fail_serial(l, serial, MOQR_HTTP_500, t);
            owner_fail(l, MOQR_ADMIN_HEALTH_INVARIANT);
            return;
        }
    }
    if (render(l->ctx, serial, dst, dcap, len) != MOQR_OK) {
        /*
         * No partial body is ever exposed: the bank is given up before any
         * waiter can be attached to it, and it carries the release.
         *
         * The poison ledger is deliberately NOT used here. This path already
         * holds the authoritative demand from the token it took, so there is
         * nothing to rediscover later -- and a note left behind because the
         * demand happened to lack SIGNAL would occupy a ledger slot forever.
         * That is a bounded resource: enough of them and an otherwise
         * recoverable failure becomes a terminal.
         */
        if (admin_abort(l, bank, serial) != MOQR_OK) {
            owner_fail(l, MOQR_ADMIN_HEALTH_INVARIANT);
            return;
        }
        if ((demand & MOQR_BROKER_DEMAND_SIGNAL) != 0u &&
            l->emit_suppressed != NULL) {
            l->emit_suppressed(l->ctx, serial);
        }
        /* The serial is known to the state machine -- it was just reserved and
         * aborted -- so a refusal here is a contradiction, not a stale id. */
        if (admin_fail_serial(l, serial, MOQR_HTTP_500, t) !=
            MOQR_OK) {
            owner_fail(l, MOQR_ADMIN_HEALTH_INVARIANT);
        }
        return;
    }
    /* The accepted sink policy, unchanged. SIGNAL is emitted once from RESERVED
     * storage before any commit, and its outcome is deliberately not branched
     * on: a short or failed sink write never withdraws an otherwise valid HTTP
     * body, and a partial write is never retried. */
    if ((demand & MOQR_BROKER_DEMAND_SIGNAL) != 0u && emit != NULL) {
        /* The signal sink's document is the Prometheus 0.0.4 exposition, which
         * is what the stderr dump has always carried. */
        emit(l->ctx, serial, dst[MOQR_OBS_FMT_PROMETHEUS_004],
             len[MOQR_OBS_FMT_PROMETHEUS_004]);
    }
    if ((demand & MOQR_BROKER_DEMAND_HTTP) != 0u) {
        if (admin_commit(l, bank, serial, len, t) != MOQR_OK) {
            /* This owner supplied the exact RESERVED bank/serial and lengths
             * already bounded by the renderer contract. A refusal is an
             * invariant contradiction. Cleanup is best-effort for balanced
             * teardown, never evidence that the endpoint can keep serving. */
            (void)admin_abort(l, bank, serial);
            (void)admin_fail_serial(l, serial, MOQR_HTTP_500, t);
            owner_fail(l, MOQR_ADMIN_HEALTH_INVARIANT);
        }
        return;
    }
    /* Signal-only: the projection is out, and the bank has no reader. Abort is
     * the closed owner transition for that path -- the bank moves to RELEASING
     * carrying its own release, which the settle pass hands back to the
     * broker. */
    if (admin_abort(l, bank, serial) != MOQR_OK) {
        owner_fail(l, MOQR_ADMIN_HEALTH_INVARIANT);
    }
}

/* -- settlement -----------------------------------------------------------
 *
 * The broker release and the admin acknowledgement are ONE transaction with no
 * yield point between them. A wake reaches external transport code, so it is
 * RETAINED here and performed only after the admin side has been settled --
 * a callback running between the two would find the transaction half-done.
 */
typedef struct settle_txn {
    struct moqr_admin_listen *l;
    bool                      wake;
    bool                      released;
} settle_txn_t;

static moqr_result_t
owner_release_cb(void *ctx, uint64_t serial, uint32_t demand, uint32_t bank)
{
    settle_txn_t *txn = (settle_txn_t *)ctx;
    bool wake = false;
    moqr_result_t rc;
    (void)demand;

    rc = admin_settle_release(txn->l, serial, bank, &wake);
    if (rc == MOQR_OK) {
        /* Retained, NOT performed: the admin settlement has not happened yet,
         * and this callback must not yield before it does. */
        txn->wake = txn->wake || wake;
        txn->released = true;
    }
    return rc;
}

static void
owner_settle(struct moqr_admin_listen *l)
{
    uint32_t guard = 0;

    /* Banks that owe a release. Acknowledge FIRST, then wake: the broker
     * release and its acknowledgement are one transaction, and the wake is an
     * external call that must sit outside it. */
    for (;;) {
        moqr_admin_release_t tok;
        bool wake = false;
        if (++guard > MOQR_ADMIN_BANKS + 1u) {
            /* More release tokens than there are banks means the pass is not
             * converging: something is regenerating work it has just settled. */
            owner_fail(l, MOQR_ADMIN_HEALTH_INVARIANT);
            break;
        }
        if (!moqr_admin_peek_release(&l->admin, &tok)) {
            break;
        }
        if (admin_bank_release(l, tok.serial, tok.bank, &wake) != MOQR_OK) {
            /*
             * The frozen contract says a refused release is RETRYABLE: neither
             * the broker nor the bank was mutated, so the token stays valid.
             * That is written for a caller whose peer might repair the
             * mismatch. This owner has no such peer -- it is the only thread
             * that takes or releases, so nothing can change between one
             * attempt and the next, and a retry loop here would spin on a
             * permanent disagreement while the bank never comes back.
             *
             * So this is a DELIBERATE narrowing of a retryable result to a
             * terminal one, justified by sole ownership, not a restatement of
             * the API.
             */
            owner_fail(l, MOQR_ADMIN_HEALTH_INVARIANT);
            break;
        }
#ifdef MOQR_ADMIN_LISTEN_TESTING
        owner_count(l, &l->n_bank_release);
#endif
        if (admin_ack_release(l, tok.serial, tok.bank) != MOQR_OK) {
            /* The broker freed a bank the state machine will not give up. The
             * two sides now disagree about who owns storage. */
            owner_fail(l, MOQR_ADMIN_HEALTH_INVARIANT);
            break;
        }
#ifdef MOQR_ADMIN_LISTEN_TESTING
        owner_count(l, &l->n_ack_release);
#endif
        consume_wake(l, wake);   /* outside the transaction, after the ack */
    }

    /* Generations abandoned before anything was rendered. */
    guard = 0;
    for (;;) {
        moqr_admin_retire_t ret;
        settle_txn_t txn;
        bool poisoned;
        if (++guard > MOQR_ADMIN_MAX_CLIENTS + 1u) {
            owner_fail(l, MOQR_ADMIN_HEALTH_INVARIANT);
            break;
        }
        if (!moqr_admin_claim_abandon(&l->admin, &ret)) {
            break;
        }
        if (!ret.taken) {
            uint32_t d = 0;
            uint32_t b = 0;
            moqr_broker_on_complete(l->broker, ret.serial);
            if (!moqr_broker_take_serial(l->broker, ret.serial, &d, &b)) {
                /* Claimed for retirement but the broker has no token to give:
                 * the two sides disagree about this identity. */
                owner_fail(l, MOQR_ADMIN_HEALTH_INVARIANT);
                break;
            }
            /* The retirement is CLAIMED, so the record can carry the token --
             * this is the durable carrier the take above requires. */
            if (admin_record_take(l, ret.serial, d, b) != MOQR_OK) {
                owner_fail(l, MOQR_ADMIN_HEALTH_INVARIANT);
                break;
            }
            /* The record now holds the token, so the carrier describes that
             * rather than the pre-take claim it arrived as. */
            ret.demand = d;
            ret.bank = b;
            ret.taken = true;
        }
#ifdef MOQR_ADMIN_LISTEN_TESTING
        /*
         * The carrier EXISTS at this point: the retirement is claimed and its
         * {serial, demand, bank} is recorded. Publishing it here, and pausing
         * here if the test asked, is what lets the abandoned generation be
         * observed while it is real rather than inferred from the emptiness
         * that follows.
         */
        owner_publish_carrier(l, &ret, true);
        owner_pause_point(l);
#endif
        txn.l = l;
        txn.wake = false;
        txn.released = false;
        if (admin_settle_abandon(l, ret.serial, owner_release_cb, &txn) !=
            MOQR_OK) {
            /* A failed settle is an invariant failure, not a retry: the token
             * stays readable for diagnosis and the endpoint stops serving. */
            owner_fail(l, MOQR_ADMIN_HEALTH_INVARIANT);
            break;
        }
        if (!txn.released) {
            /* Settlement reported success without the release callback having
             * run. The admin record is now settled for a broker token nobody
             * gave back, and the two sides disagree about that bank. */
            owner_fail(l, MOQR_ADMIN_HEALTH_INVARIANT);
            break;
        }
#ifdef MOQR_ADMIN_LISTEN_TESTING
        owner_count(l, &l->n_settle);
        owner_publish_carrier(l, NULL, false);
#endif
        /* Both sides are settled. Only now do the external calls run. */
        poisoned = poison_take(l, ret.serial);
        if (poisoned && (ret.demand & MOQR_BROKER_DEMAND_SIGNAL) != 0u &&
            l->emit_suppressed != NULL) {
            l->emit_suppressed(l->ctx, ret.serial);
        }
        consume_wake(l, txn.wake);
    }
}

static void
owner_turn(struct moqr_admin_listen *l, struct pollfd *p, nfds_t n,
           const int *slot_of, uint64_t t)
{
    /*
     * Accepts, bounded, and only while ownership can be taken.
     *
     * The capacity test is re-evaluated before EVERY accept, not once for the
     * turn: the first accept of a turn can be the one that fills the refusal
     * slot, and accepting the next peer after that would take ownership of a
     * connection nothing can serve, only to close it silently. A peer left in
     * the kernel backlog is a peer that still gets an answer, in a later slot
     * lifetime.
     */
    if (n > 0 && p[0].fd == l->listen_fd && (p[0].revents & POLLIN) != 0) {
        for (uint32_t k = 0; k < MOQR_ADMIN_ACCEPTS_PER_TURN; k++) {
            uint32_t slot = 0;
            bool have_slot = slot_free(l);
            int fd;
            if (!have_slot && l->refuse_fd >= 0) {
                break;   /* neither ownership class is free: leave it queued */
            }
            fd = accept(l->listen_fd, NULL, NULL);
            if (fd < 0) {
                break;
            }
            if (!set_nonblocking(fd) || !set_nosigpipe(fd)) {
                /* A socket this loop cannot own safely is closed at once,
                 * before it is admitted to anything. */
                (void)close(fd);
                continue;
            }
            if (have_slot) {
                /*
                 * `slot_free` already proved a normal slot exists, so the
                 * state machine cannot legitimately refuse, cannot name an
                 * index outside its own table, and cannot hand back a slot
                 * whose descriptor is still live. Folding any of those into
                 * ordinary overload would admit a client with no descriptor,
                 * or overwrite one that has one -- and the refusal slot would
                 * quietly paper over the contradiction.
                 */
                moqr_result_t arc = admin_accept(l, t, &slot);
                if (arc == MOQR_ERR_CAPACITY && l->terminal_cancelled) {
                    /* The ONE legitimate refusal that is not about slots: a
                     * cancelled state machine admits nobody, whatever the
                     * table looks like. That connection is overload from the
                     * endpoint's point of view and gets the fixed 503. */
                    if (!refuse_take(l, fd, t)) {
                        (void)close(fd);
                    }
                    continue;
                }
                if (arc != MOQR_OK || slot >= MOQR_ADMIN_MAX_CLIENTS ||
                    l->client_fd[slot] >= 0) {
                    owner_fail(l, MOQR_ADMIN_HEALTH_INVARIANT);
                    (void)close(fd);
                    break;
                }
                l->client_fd[slot] = fd;
                atomic_fetch_add(&l->accepts, 1u);
                continue;
            }
            /* No normal slot: the bounded refusal slot answers a fixed 503. */
            if (!refuse_take(l, fd, t)) {
                (void)close(fd);
            }
        }
    }

    /* Reads and writes, driven by revents, one bounded chunk each. */
    short refusal_revents = 0;
    const char *span;
    size_t len;
    for (nfds_t i = 1; i < n; i++) {
        int s = slot_of[i];
        short re = p[i].revents;
        if (re == 0) {
            continue;
        }
        if (s == -1) {           /* the wake pipe */
            continue;
        }
        if (s == -2) {           /* the refusal slot, stepped below */
            refusal_revents = re;
            continue;
        }
        /*
         * POLLHUP is NOT a drop on its own. A peer that half-closes its write
         * side after sending a complete request is reported as POLLIN|POLLHUP,
         * and dropping on the hangup would throw away a request that arrived
         * in full. Buffered readable and writable events are processed first;
         * only a hangup with nothing left to do ends the connection.
         * POLLERR/POLLNVAL stay fail-closed -- there is nothing to salvage.
         */
        if ((re & (POLLERR | POLLNVAL)) != 0) {
            drop_client_from(l, (uint32_t)s, MOQR_ADMIN_DROP_TURN);
            continue;
        }
        if ((re & POLLIN) != 0) {
            char buf[MOQR_ADMIN_READ_CHUNK];
            ssize_t r = read(l->client_fd[s], buf, sizeof(buf));
            if (r > 0) {
                /* WRONG_STATE means there was nothing left to act on -- but
                 * this owner arms POLLIN only while a client is READING, and
                 * no other thread touches the state machine, so bytes cannot
                 * arrive for a client that has nothing pending. Any refusal
                 * here means the descriptor and the slot are not the same
                 * client. */
                if (admin_on_bytes(l, (uint32_t)s, buf, (size_t)r, t) !=
                    MOQR_OK) {
                    owner_fail(l, MOQR_ADMIN_HEALTH_INVARIANT);
                }
            } else if (r == 0) {
                /* A real end of stream. The request head, if it was complete,
                 * has already been fed above. */
                if (!moqr_admin_pending(&l->admin, (uint32_t)s, &span, &len)) {
                    drop_client_from(l, (uint32_t)s, MOQR_ADMIN_DROP_TURN);
                    continue;
                }
            } else if (errno != EAGAIN && errno != EWOULDBLOCK &&
                       errno != EINTR) {
                drop_client_from(l, (uint32_t)s, MOQR_ADMIN_DROP_TURN);
                continue;
            }
        }
        /* POLLHUP is treated as writable too. A reset peer is reported as a
         * hangup rather than as writable, and a client mid-response would
         * otherwise never be written to again -- holding its bank, and with it
         * a broker slot, for the life of the process. Attempting the write is
         * what turns a dead peer into an error the drop path can see. */
        if ((re & (POLLOUT | POLLHUP)) != 0) {
            if (moqr_admin_pending(&l->admin, (uint32_t)s, &span, &len)) {
                ssize_t w;
                if (len > MOQR_ADMIN_WRITE_CHUNK) {
                    len = MOQR_ADMIN_WRITE_CHUNK;
                }
#ifdef MOQR_ADMIN_LISTEN_TESTING
                if (!io_next_client_write(&w, len, s))
#endif
                {
                    w = admin_write(l->client_fd[s], span, len);
                }
                if (w > 0) {
                    /* The span came from moqr_admin_pending for this exact
                     * client a moment ago, so a refusal here means the report
                     * does not describe the write that was made. */
                    if (admin_on_written(l, (uint32_t)s, (size_t)w, t) !=
                        MOQR_OK) {
                        owner_fail(l, MOQR_ADMIN_HEALTH_INVARIANT);
                    }
                } else if (w < 0 && errno != EAGAIN && errno != EWOULDBLOCK &&
                           errno != EINTR) {
                    drop_client_from(l, (uint32_t)s, MOQR_ADMIN_DROP_TURN);
                    continue;
                }
            }
        }
        if ((re & POLLHUP) != 0 &&
            l->client_fd[s] >= 0 &&
            !moqr_admin_pending(&l->admin, (uint32_t)s, &span, &len) &&
            (l->admin.client[s].state == MOQR_ADMIN_CS_READING ||
             l->admin.client[s].state == MOQR_ADMIN_CS_DONE)) {
            /*
             * Hung up with nothing left to send AND nothing owed. A client
             * that has already delivered a complete request is still owed a
             * response even though it has closed its write side, so the states
             * between PARSED and WRITING deliberately survive a hangup.
             */
            drop_client_from(l, (uint32_t)s, MOQR_ADMIN_DROP_TURN);
        }
    }
    /* The refusal slot advances EVERY turn, event or not: its deadline is a
     * deadline, not a reaction to the peer doing something. */
    refuse_step(l, t, refusal_revents);

    /* The generation handshake. */
    if (atomic_load(&l->terminal)) {
        uint32_t c;
        uint32_t guard = 0;
        if (!l->terminal_cancelled) {
            /* Once. Repeating it every turn would also destroy connections
             * that arrived afterwards before they could be answered. */
            owner_cancel_once(l, t);
        }
        /* A request arriving after terminality is answered 503 immediately: no
         * generation is opened and no lane is woken, because the lanes will
         * never pump again. */
        while (moqr_admin_next_demand(&l->admin, &c)) {
            if (++guard > MOQR_ADMIN_MAX_CLIENTS + 1u) {
                owner_fail(l, MOQR_ADMIN_HEALTH_INVARIANT);
                break;
            }
            if (admin_refuse(l, c, MOQR_HTTP_503, t) != MOQR_OK) {
                owner_fail(l, MOQR_ADMIN_HEALTH_INVARIANT);
                break;
            }
        }
    } else {
        uint32_t c;
        uint32_t guard = 0;
        /* The latched signal demand becomes a request HERE, on the owner, so
         * one broker has exactly one thread opening generations against it. */
        if (l->signal_pending != NULL && l->signal_pending(l->ctx)) {
            uint64_t issued = 0;
            bool wake = false;
            moqr_result_t rrc = broker_request(
                l, MOQR_BROKER_DEMAND_SIGNAL, &issued, &wake);
            switch (rrc) {
            case MOQR_OK:
                consume_wake(l, wake);
                break;
            case MOQR_ERR_WOULD_BLOCK:
                /* Documented: every bank is busy. SIGNAL demand is REMEMBERED
                 * across this refusal, so the generation opens when one frees
                 * and the operator's request is not lost. Nothing to do. */
                break;
            case MOQR_ERR_CAPACITY:
                /* Documented and TERMINAL: the serial space is spent, so no
                 * further generation can ever be opened. An endpoint that
                 * cannot open a generation cannot serve, and pretending
                 * otherwise would answer every later scrape 503 forever
                 * without saying why. */
                owner_fail(l, MOQR_ADMIN_HEALTH_SERIAL_EXHAUSTED);
                break;
            default:
                /* The demand mask is a compile-time constant and the out
                 * pointers are ours, so INVAL is impossible. */
                owner_fail(l, MOQR_ADMIN_HEALTH_INVARIANT);
                break;
            }
        }
        while (moqr_admin_next_demand(&l->admin, &c)) {
            uint64_t serial = 0;
            bool wake = false;
            if (++guard > MOQR_ADMIN_MAX_CLIENTS + 1u) {
                /* More demanding clients than the table holds: the demand
                 * scan is not converging. */
                owner_fail(l, MOQR_ADMIN_HEALTH_INVARIANT);
                break;
            }
            moqr_result_t rrc = broker_request(
                l, MOQR_BROKER_DEMAND_HTTP, &serial, &wake);
            if (rrc != MOQR_OK && rrc != MOQR_ERR_WOULD_BLOCK &&
                rrc != MOQR_ERR_CAPACITY) {
                /* INVAL cannot happen: the demand mask is a constant and the
                 * out pointers are ours. */
                owner_fail(l, MOQR_ADMIN_HEALTH_INVARIANT);
                break;
            }
            if (rrc == MOQR_OK) {
                moqr_result_t brc;
                consume_wake(l, wake);
                brc = admin_bind_serial(l, c, serial, t);
                if (brc == MOQR_ERR_CAPACITY) {
                    /* Documented: no generation slot is free, and the caller
                     * answers 503. The broker generation keeps its other
                     * joiners; this one simply never became a waiter. */
                    if (admin_refuse(l, c, MOQR_HTTP_503, t) != MOQR_OK) {
                        owner_fail(l, MOQR_ADMIN_HEALTH_INVARIANT);
                        break;
                    }
                } else if (brc != MOQR_OK) {
                    /* Anything else means HTTP demand is now inside a broker
                     * generation with no waiter that owns it: the request
                     * would wait for a document nobody will deliver. */
                    owner_fail(l, MOQR_ADMIN_HEALTH_INVARIANT);
                    break;
                }
            } else if (rrc == MOQR_ERR_WOULD_BLOCK) {
                /* Every bank is busy. Rejected HTTP demand is deliberately
                 * not remembered, so nothing is coming for this request and
                 * 503 is the honest answer. */
                if (admin_refuse(l, c, MOQR_HTTP_503, t) != MOQR_OK) {
                    owner_fail(l, MOQR_ADMIN_HEALTH_INVARIANT);
                    break;
                }
            } else {
                /* CAPACITY is not load: the serial identity space is spent
                 * permanently. No later HTTP or SIGNAL generation can open. */
                owner_fail(l, MOQR_ADMIN_HEALTH_SERIAL_EXHAUSTED);
                break;
            }
        }
        owner_produce(l, t);
    }
    owner_settle(l);
    /* A pure function of state and time: it has no failure this owner can
     * cause, so a non-OK result means the state it was handed is not the state
     * it requires. */
    if (admin_tick(l, t) != MOQR_OK) {
        owner_fail(l, MOQR_ADMIN_HEALTH_INVARIANT);
    }

    /* One request per connection. DONE is the state machine's own statement
     * that the response is fully written or the request was dropped, so it is
     * the exact condition to close on. */
    for (uint32_t i = 0; i < MOQR_ADMIN_MAX_CLIENTS; i++) {
        if (l->client_fd[i] >= 0 &&
            l->admin.client[i].state == MOQR_ADMIN_CS_DONE) {
            drop_client_from(l, i, MOQR_ADMIN_DROP_TURN);
        }
    }
}

/* -- the activation gate ---------------------------------------------------
 *
 * A condition variable, not a sleep and not a spin: the owner blocks here until
 * the caller has installed everything its callbacks reach, and the caller
 * blocks until the owner reports what happened on its first turn. Both
 * directions are exact, so neither side has to guess how long the other takes.
 */
static void
gate_wait_for_open(struct moqr_admin_listen *l)
{
    (void)pthread_mutex_lock(&l->gate_mu);
    while (!l->gate_open) {
        (void)pthread_cond_wait(&l->gate_cv, &l->gate_mu);
    }
    (void)pthread_mutex_unlock(&l->gate_mu);
}

static void
gate_report(struct moqr_admin_listen *l, int report)
{
    (void)pthread_mutex_lock(&l->gate_mu);
    if (l->gate_report == 0) {
        l->gate_report = report;
        (void)pthread_cond_broadcast(&l->gate_cv);
    }
    (void)pthread_mutex_unlock(&l->gate_mu);
}

static int
gate_open_and_await_report(struct moqr_admin_listen *l)
{
    int report;
    (void)pthread_mutex_lock(&l->gate_mu);
    l->gate_open = true;
    (void)pthread_cond_broadcast(&l->gate_cv);
    while (l->gate_report == 0) {
        (void)pthread_cond_wait(&l->gate_cv, &l->gate_mu);
    }
    report = l->gate_report;
    (void)pthread_mutex_unlock(&l->gate_mu);
    return report;
}

static void *
owner_thread(void *arg)
{
    struct moqr_admin_listen *l = (struct moqr_admin_listen *)arg;
    uint64_t t = 0;
    bool reported = false;

    /* Parked until the caller has installed every pointer these callbacks
     * reach. Nothing is accepted, opened or woken before this returns. */
    gate_wait_for_open(l);

    for (;;) {
        struct pollfd p[MOQR_ADMIN_MAX_CLIENTS + 3];
        int slot_of[MOQR_ADMIN_MAX_CLIENTS + 3];
        nfds_t n = 0;
        int poll_ms;

        if (!clock_now_us(l, &t)) {
            /* Without a monotonic clock no deadline can be honoured, so
             * nothing may be promised. Named, not silent. */
            owner_fail(l, MOQR_ADMIN_HEALTH_NO_CLOCK);
            break;
        }
        /* The listen fd is offered ONLY while ownership can be taken -- a
         * normal slot, or the refusal slot. Otherwise pending connections wait
         * in the kernel backlog rather than spinning this loop. */
        p[n].fd = (slot_free(l) || l->refuse_fd < 0) ? l->listen_fd : -1;
        p[n].events = POLLIN;
        p[n].revents = 0;
        slot_of[n] = -3;
        n++;
        p[n].fd = l->wake_r;
        p[n].events = POLLIN;
        p[n].revents = 0;
        slot_of[n] = -1;
        n++;
        for (uint32_t i = 0; i < MOQR_ADMIN_MAX_CLIENTS; i++) {
            short ev = 0;
            const char *span;
            size_t len;
            if (l->client_fd[i] < 0) {
                continue;
            }
            /* POLLIN only while still reading, POLLOUT only when there is
             * something to write. An unconditional POLLOUT on a normally
             * writable stream socket is a busy-spin. */
            if (l->admin.client[i].state == MOQR_ADMIN_CS_READING) {
                ev |= POLLIN;
            }
            if (moqr_admin_pending(&l->admin, i, &span, &len)) {
                ev |= POLLOUT;
            }
            if (ev == 0) {
                continue;
            }
            p[n].fd = l->client_fd[i];
            p[n].events = ev;
            p[n].revents = 0;
            slot_of[n] = (int)i;
            n++;
        }
        if (l->refuse_fd >= 0) {
            p[n].fd = l->refuse_fd;
            p[n].events = l->refuse_draining ? POLLIN : POLLOUT;
            p[n].revents = 0;
            slot_of[n] = -2;
            n++;
        }

        /* The timeout is a SAFETY BOUND on a lost notification and on the
         * refusal deadline, not the publication mechanism -- lanes notify at
         * the point they publish. */
        if (MOQR_TEST_FAIL(poll)) {
            owner_fail(l, MOQR_ADMIN_HEALTH_POLL_FAILED);
            break;
        }
        /*
         * ADOPT A REQUESTED POLL MODE, THEN SAY SO.
         *
         * The acknowledgement is published after the timeout for the poll
         * below has been decided and after the wake pipe has been drained, so
         * a notify issued before the request cannot be mistaken for the first
         * turn that ran under the new mode. It carries the request generation
         * and the adopted value, so a test can check it is answering ITS
         * request with the value it asked for.
         */
        poll_ms = MOQR_POLL_TIMEOUT_MS;
#ifdef MOQR_ADMIN_LISTEN_TESTING
        if (!view_lock_for_clock(l)) {
            owner_fail(l, MOQR_ADMIN_HEALTH_NO_CLOCK);
            break;
        }
        if (l->poll_req_gen != l->poll_ack_gen) {
            /*
             * THE GENERATION AND THE TIMEOUT ARE ONE CAPTURE.
             *
             * Rereading the request generation after the lock has been
             * released would let a request that arrived meanwhile be
             * acknowledged with the previous request's timeout -- and, because
             * the generations would then compare equal, its own timeout would
             * never be adopted. What is acknowledged is exactly what was
             * taken; anything newer stays pending for the next turn.
             */
            unsigned taken_gen = l->poll_req_gen;

            /*
             * CAPTURE, DRAIN AND ACKNOWLEDGE UNDER ONE HOLD.
             *
             * Releasing the lock between the capture and the drain leaves a
             * window in which a newer request can arrive: its wake is then
             * drained while the request itself stays pending, and with the
             * adopted timeout of -1 nothing ever wakes the owner again. The
             * drain is non-blocking, so holding the lock across it costs
             * nothing -- and a later requester simply cannot publish until
             * this acknowledgement is out, so its wake necessarily arrives
             * after the drain.
             */
            poll_ms = l->poll_req_ms;
            l->poll_adopted_ms = poll_ms;
            poll_seam_wait(l);
            /*
             * WHO OWNS THE LOCK AT THE DRAIN.
             *
             * The drain and the acknowledgement must happen under the same
             * hold as the capture: a shape that released the lock across the
             * drain would let a request arriving meanwhile have its wake eaten
             * while the request stayed pending. This is the fact itself, taken
             * where it matters, and it is published with the acknowledgement.
             */
            l->poll_drain_held = poll_drain_measured(l);
            l->poll_ack_ms = poll_ms;
            l->poll_ack_gen = taken_gen;
            {
                unsigned slot = l->poll_ack_hist_n % MOQR_ADMIN_POLL_ACK_HIST;

                /* The record is of what was PUBLISHED, so an observer and
                 * the acknowledgement can never disagree. */
                l->poll_ack_hist[slot].gen = l->poll_ack_gen;
                l->poll_ack_hist[slot].ms = l->poll_ack_ms;
                l->poll_ack_hist[slot].adopted = l->poll_adopted_ms;
                l->poll_ack_hist[slot].drain_held = l->poll_drain_held;
                l->poll_ack_hist_n++;
            }
            l->view_gen++;
            (void)pthread_cond_broadcast(&l->view_cv);
        } else if (l->poll_adopted_ms != MOQR_ADMIN_POLL_NO_MODE) {
            poll_ms = l->poll_adopted_ms;
        }
        l->poll_used_ms = poll_ms;
        if (!view_unlock_for_clock(l)) {
            owner_fail(l, MOQR_ADMIN_HEALTH_NO_CLOCK);
            break;
        }
#endif
        if (poll(p, n, poll_ms) < 0) {
            if (errno == EINTR) {
                continue;
            }
            owner_fail(l, MOQR_ADMIN_HEALTH_POLL_FAILED);
            break;
        }
        wake_drain(l);
        /* The first completed wait IS the serving report: the clock answered
         * and the poll returned, which is everything "entered the serving
         * loop" can honestly mean. */
        if (!reported) {
            reported = true;
            gate_report(l, 1);
        }
#ifdef MOQR_ADMIN_LISTEN_TESTING
        atomic_fetch_add(&g_test_turns, 1);
#endif
        if (atomic_load(&l->stop)) {
            if (!MOQR_TEST_PARKED()) {
                break;
            }
            /* Parked: alive, and no longer serving -- which is the state a
             * caller has to reason about when a stop cannot prove the owner
             * dead. */
            continue;
        }
        owner_turn(l, p, n, slot_of, t);
#ifdef MOQR_ADMIN_LISTEN_TESTING
        owner_publish_view(l);
#endif
        if (atomic_load(&l->stop) && !MOQR_TEST_PARKED()) {
            break;   /* the turn declared a terminal */
        }
    }

    /*
     * ONE exit sequence, whatever the reason. Every arm -- asked to stop, a
     * dead clock, a failed poll, an invariant contradiction -- leaves through
     * here, so a failing endpoint releases exactly what an ordinary shutdown
     * does. A half-torn-down owner is how a dead endpoint keeps a socket open
     * and keeps answering.
     */
    /* If the owner never reached its first successful wait, the caller is
     * still blocked on the gate and must be told the endpoint failed rather
     * than left to time out. */
    gate_report(l, -1);
#ifdef MOQR_ADMIN_LISTEN_TESTING
    /* A failed owner that is ALSO stuck: it has reported its failure, so the
     * caller is unblocked, but it has not left -- which is the one shape in
     * which an activation failure and an unprovable join coincide. */
    while (MOQR_TEST_PARKED()) {
        struct timespec pts = { 0, 2 * 1000 * 1000 };
        (void)nanosleep(&pts, NULL);
    }
#endif
    owner_cancel_once(l, t);
    owner_settle(l);
    for (uint32_t i = 0; i < MOQR_ADMIN_MAX_CLIENTS; i++) {
        /* Only the slots this sweep still owns. Asking for one an earlier
         * sweep already released is not a drop, and counting it as an attempt
         * would make "asked exactly once" unprovable for every slot. */
        if (l->client_fd[i] >= 0) {
            drop_client_from(l, i, MOQR_ADMIN_DROP_EXIT);
        }
    }
    refuse_drop(l);
    /* The listening socket closes with the owner. An endpoint whose owner has
     * died must not keep accepting connections nobody will ever answer. */
    close_fd(&l->listen_fd);
    {
        unsigned expected = MOQR_ADMIN_HEALTH_OK;
        (void)atomic_compare_exchange_strong(&l->health, &expected,
                                             MOQR_ADMIN_HEALTH_STOPPED);
    }
#ifdef MOQR_ADMIN_LISTEN_TESTING
    /* The final view: everything released, nothing outstanding. */
    owner_publish_view(l);
#endif
    atomic_store(&l->owner_done, 1);
    return NULL;
}

/* -- lifecycle ------------------------------------------------------------ */

/*
 * Join the owner thread, if one was started.
 *
 * Returns false when the join did not prove the thread dead. That is not a
 * recoverable condition: the thread may still be reading this object, so its
 * storage can never be reused. The caller leaks it deliberately -- an
 * unjoinable thread's memory is not ours to give back.
 */
static bool
join_owner(struct moqr_admin_listen *l)
{
    if ((l->ledger & MOQR_ADMIN_LEDGER_THREAD) == 0u) {
        return true;   /* nothing was started */
    }
    if (l->owner_joined) {
        return true;
    }
    atomic_store(&l->stop, 1);
    wake_post(l);
    /*
     * A bounded proof BEFORE the join, so "the owner is dead" is established
     * rather than assumed. `pthread_join` blocks forever on a thread that will
     * not exit, and a join that never returns is indistinguishable from one
     * that cannot be trusted -- so the owner is required to signal its own
     * exit first, and only then is it reaped.
     */
    for (uint32_t i = 0; i < MOQR_ADMIN_JOIN_PROOF_TICKS; i++) {
        struct timespec ts = { 0, 2 * 1000 * 1000 };
        if (atomic_load(&l->owner_done) != 0) {
            break;
        }
        (void)nanosleep(&ts, NULL);
    }
    if (atomic_load(&l->owner_done) == 0) {
        /* Still running, or stuck. Its death cannot be proved, so nothing it
         * can reach may be released -- by this module or by the caller. */
        return false;
    }
    if (pthread_join(l->thread, NULL) != 0) {
        return false;
    }
    l->owner_joined = true;
    return true;
}

/*
 * Release only what was acquired, in strict reverse ledger order.
 *
 * Never called while the owner might be running: every path here either joined
 * it or never started it.
 */
static void
unwind(struct moqr_admin_listen *l)
{
    if ((l->ledger & MOQR_ADMIN_LEDGER_THREAD) != 0u && !l->owner_joined) {
        /* Freeing storage a live thread can still reach is not recoverable.
         * Leaking it is the only honest outcome. */
        return;
    }
#ifdef MOQR_ADMIN_LISTEN_TESTING
    if ((l->ledger & MOQR_ADMIN_LEDGER_SEAM_CV) != 0u) {
        (void)pthread_cond_destroy(&l->seam_cv);
    }
    if ((l->ledger & MOQR_ADMIN_LEDGER_SEAM_MU) != 0u) {
        (void)pthread_mutex_destroy(&l->seam_mu);
    }
    if ((l->ledger & MOQR_ADMIN_LEDGER_VIEW_CV) != 0u) {
        (void)pthread_cond_destroy(&l->view_cv);
    }
    if ((l->ledger & MOQR_ADMIN_LEDGER_VIEW_MU) != 0u) {
        /* The RESULT decides. Counting the attempt would make the balance
         * prove that destruction was tried, which is not the same claim as
         * the resource having been released. */
        int mrc = pthread_mutex_destroy(&l->view_mu);
#ifdef MOQR_ADMIN_LISTEN_TESTING
        if (mrc == 0) {
            atomic_fetch_add(&g_view_mu_destroy, 1);
        }
#endif
        (void)mrc;
    }
#endif
    if ((l->ledger & MOQR_ADMIN_LEDGER_GATE) != 0u) {
        (void)pthread_cond_destroy(&l->gate_cv);
        (void)pthread_mutex_destroy(&l->gate_mu);
    }
    if ((l->ledger & MOQR_ADMIN_LEDGER_WAKE) != 0u) {
        close_fd(&l->wake_r);
        close_fd(&l->wake_w);
    }
    if ((l->ledger & MOQR_ADMIN_LEDGER_SOCKET) != 0u) {
        for (uint32_t i = 0; i < MOQR_ADMIN_MAX_CLIENTS; i++) {
            close_fd(&l->client_fd[i]);
        }
        close_fd(&l->refuse_fd);
        close_fd(&l->listen_fd);
    }
    if ((l->ledger & MOQR_ADMIN_LEDGER_ADMIN) != 0u) {
        moqr_admin_destroy(&l->admin);   /* exactly once */
    }
    if ((l->ledger & MOQR_ADMIN_LEDGER_BODIES) != 0u) {
        for (uint32_t b = 0; b < MOQR_ADMIN_BANKS; b++) {
            for (uint32_t k = 0; k < MOQR_ADMIN_BODY__COUNT; k++) {
                free(l->bodies[b][k]);
                l->bodies[b][k] = NULL;
            }
        }
    }
    l->ledger = 0;
    free(l);
}

/* A constructor failure: join whatever was started, then release. */
static void
unwind_failed_start(struct moqr_admin_listen *l)
{
    (void)join_owner(l);
    unwind(l);
}

/*
 * The listener is the FINAL socket boundary, so it decides for itself.
 *
 * The configuration parser already refuses a non-loopback host, but a parser is
 * not a boundary: a direct constructor call, a future caller, or a struct built
 * somewhere else all reach this function with whatever they please. A wildcard
 * bind is the difference between a local diagnostic surface and one exposed to
 * the network, and that is not a decision to inherit on trust.
 */
static bool
admin_addr_is_loopback(const char *host, bool *out_v6,
                       struct in_addr *out_v4, struct in6_addr *out_v6addr)
{
    if (host == NULL) {
        return false;
    }
    if (strchr(host, ':') != NULL) {
        struct in6_addr a6;
        if (inet_pton(AF_INET6, host, &a6) != 1) {
            return false;
        }
        /* ::1 only. A v4-mapped address is refused rather than unwrapped: it
         * would bind a v4 address through a v6 socket, which is exactly the
         * kind of reach a literal check exists to make visible. */
        if (!IN6_IS_ADDR_LOOPBACK(&a6)) {
            return false;
        }
        *out_v6 = true;
        *out_v6addr = a6;
        return true;
    }
    {
        struct in_addr a4;
        if (inet_pton(AF_INET, host, &a4) != 1) {
            return false;
        }
        /* 127.0.0.0/8, and nothing else -- 0.0.0.0 is a wildcard, not a
         * loopback address. */
        if ((ntohl(a4.s_addr) & 0xff000000u) != 0x7f000000u) {
            return false;
        }
        *out_v6 = false;
        *out_v4 = a4;
        return true;
    }
}

static moqr_result_t
bind_tcp(struct moqr_admin_listen *l, const moqr_cli_admin_t *a)
{
    int fd = -1;
    int on = 1;
    bool v6 = false;
    struct in_addr a4;
    struct in6_addr a6;

    memset(&a4, 0, sizeof(a4));
    memset(&a6, 0, sizeof(a6));
    if (!admin_addr_is_loopback(a->host, &v6, &a4, &a6)) {
        return MOQR_ERR_INVAL;
    }
    if (v6) {
        struct sockaddr_in6 sa;
        memset(&sa, 0, sizeof(sa));
        sa.sin6_family = AF_INET6;
        sa.sin6_port = htons((uint16_t)a->port);
        sa.sin6_addr = a6;
        fd = socket(AF_INET6, SOCK_STREAM, 0);
        if (fd < 0) {
            return MOQR_ERR_INTERNAL;
        }
        if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)) != 0) {
            (void)close(fd);
            return MOQR_ERR_INTERNAL;
        }
#ifdef IPV6_V6ONLY
        /* Contract-establishing: a dual-stack socket would also accept v4
         * traffic the config never named, so a failure here is fatal. */
        if (setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &on, sizeof(on)) != 0) {
            (void)close(fd);
            return MOQR_ERR_INTERNAL;
        }
#endif
        if (bind(fd, (struct sockaddr *)&sa, sizeof(sa)) != 0) {
            (void)close(fd);
            return MOQR_ERR_INTERNAL;
        }
    } else {
        struct sockaddr_in sa;
        memset(&sa, 0, sizeof(sa));
        sa.sin_family = AF_INET;
        sa.sin_port = htons((uint16_t)a->port);
        sa.sin_addr = a4;
        fd = socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) {
            return MOQR_ERR_INTERNAL;
        }
        if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)) != 0) {
            (void)close(fd);
            return MOQR_ERR_INTERNAL;
        }
        if (bind(fd, (struct sockaddr *)&sa, sizeof(sa)) != 0) {
            (void)close(fd);
            return MOQR_ERR_INTERNAL;
        }
    }
    {
        struct sockaddr_storage ss;
        socklen_t sl = sizeof(ss);
        if (getsockname(fd, (struct sockaddr *)&ss, &sl) != 0) {
            (void)close(fd);
            return MOQR_ERR_INTERNAL;   /* the bound port is a contract */
        }
        l->port = (ss.ss_family == AF_INET6)
                      ? ntohs(((struct sockaddr_in6 *)&ss)->sin6_port)
                      : ntohs(((struct sockaddr_in *)&ss)->sin_port);
    }
    l->listen_fd = fd;
    return MOQR_OK;
}

moqr_result_t
moqr_admin_listen_start(const moqr_admin_listen_cfg_t *cfg,
                        moqr_admin_listen_t **out)
{
    struct moqr_admin_listen *l;
    pthread_attr_t attr;
    size_t got_stack = 0;

    if (out == NULL) {
        return MOQR_ERR_INVAL;
    }
    *out = NULL;   /* before any acquisition */
    if (cfg == NULL || cfg->admin == NULL || cfg->broker == NULL ||
        cfg->collect == NULL || cfg->render == NULL ||
        cfg->wake_all == NULL) {
        return MOQR_ERR_INVAL;
    }
    {
        unsigned signal_seams = (cfg->signal_pending != NULL ? 1u : 0u) +
                                (cfg->emit_signal != NULL ? 1u : 0u) +
                                (cfg->emit_suppressed != NULL ? 1u : 0u);
        /* A partial trio can consume a signal request without emitting its
         * document or its poison diagnostic. Signal support is one contract:
         * either every seam exists, or none does. */
        if (signal_seams != 0u && signal_seams != 3u) {
            return MOQR_ERR_INVAL;
        }
    }
    if (!cfg->admin->enabled || cfg->admin->mode != MOQR_CLI_ADMIN_TCP) {
        return MOQR_ERR_INVAL;
    }
    /*
     * The port range is checked HERE as well as in the parser, for the same
     * reason the address is: this is the boundary, and a negative or oversized
     * value silently truncated by a cast to uint16_t would bind a port nobody
     * asked for. Zero -- an ephemeral port -- is reachable only through the
     * explicit test seam.
     */
    if (cfg->admin->port < 0 || cfg->admin->port > 65535) {
        return MOQR_ERR_INVAL;
    }
    if (cfg->admin->port == 0 && !cfg->allow_ephemeral_port) {
        return MOQR_ERR_INVAL;
    }
    /* The parser always terminates this fixed array, but this constructor is
     * also a boundary for direct callers. Do not pass an unterminated array to
     * strchr/inet_pton or copy from it as if it were a C string. */
    if (memchr(cfg->admin->host, '\0', sizeof(cfg->admin->host)) == NULL) {
        return MOQR_ERR_INVAL;
    }
    /* The document contract is explicit at this boundary too: the endpoint
     * advertises /api/v1/info, so it must have been handed the bytes. Checked
     * after the address, so an address refusal is still reported as one. */
    if (cfg->info == NULL || cfg->info_len == 0u ||
        cfg->info_len > MOQR_ADMIN_MAX_STATIC_DOC) {
        return MOQR_ERR_INVAL;
    }
    l = calloc(1, sizeof(*l));
#ifdef MOQR_ADMIN_LISTEN_TESTING
    if (l != NULL) {
        /* calloc would otherwise read as "a mode of 0 was adopted". */
        l->poll_adopted_ms = MOQR_ADMIN_POLL_NO_MODE;
        l->poll_ack_ms = MOQR_ADMIN_POLL_NO_MODE;
        l->poll_used_ms = MOQR_ADMIN_POLL_NO_MODE;
    }
#endif
    if (l == NULL) {
        return MOQR_ERR_NOMEM;
    }
    l->listen_fd = -1;
    l->wake_r = -1;
    l->wake_w = -1;
    l->refuse_fd = -1;
    for (uint32_t i = 0; i < MOQR_ADMIN_MAX_CLIENTS; i++) {
        l->client_fd[i] = -1;
    }
    l->broker = cfg->broker;
    l->wake_all = cfg->wake_all;
    l->collect = cfg->collect;
    l->render = cfg->render;
    l->signal_pending = cfg->signal_pending;
    l->emit_signal = cfg->emit_signal;
    l->emit_suppressed = cfg->emit_suppressed;
    l->ctx = cfg->ctx;
    atomic_store(&l->stop, 0);
    atomic_store(&l->terminal, 0);
    atomic_store(&l->owner_done, 0);

    /* Bodies from the SAME checked descriptor the capacity model reports.
     * Two formulas would drift; there is exactly one. */
    {
        moqr_admin_listen_footprint_t fp;
        if (moqr_admin_listen_footprint(cfg->lanes, &fp) != MOQR_OK) {
            unwind_failed_start(l);
            return MOQR_ERR_CAPACITY;
        }
        for (uint32_t k = 0; k < MOQR_ADMIN_BODY__COUNT; k++) {
            if (fp.body_cap[k] == 0u || fp.body_cap[k] > (uint64_t)SIZE_MAX) {
                unwind_failed_start(l);
                return MOQR_ERR_CAPACITY;
            }
            l->caps[k] = (size_t)fp.body_cap[k];
        }
    }
    for (uint32_t b = 0; b < MOQR_ADMIN_BANKS; b++) {
        for (uint32_t k = 0; k < MOQR_ADMIN_BODY__COUNT; k++) {
            l->bodies[b][k] = calloc(1, l->caps[k]);
            if (l->bodies[b][k] == NULL) {
                l->ledger |= MOQR_ADMIN_LEDGER_BODIES;
                unwind_failed_start(l);
                return MOQR_ERR_NOMEM;
            }
        }
    }
    l->ledger |= MOQR_ADMIN_LEDGER_BODIES;

    if (moqr_admin_init(&l->admin, l->bodies, l->caps, cfg->info,
                        cfg->info_len) != MOQR_OK) {
        unwind_failed_start(l);
        return MOQR_ERR_INTERNAL;
    }
    l->ledger |= MOQR_ADMIN_LEDGER_ADMIN;

    if (bind_tcp(l, cfg->admin) != MOQR_OK) {
        unwind_failed_start(l);
        return MOQR_ERR_INTERNAL;
    }
    l->ledger |= MOQR_ADMIN_LEDGER_SOCKET;
    /* Bound, but deliberately NOT listening yet: until activation the kernel
     * refuses connections outright rather than queueing them for an owner that
     * is not allowed to serve. */
    if (!set_nonblocking(l->listen_fd)) {
        unwind_failed_start(l);
        return MOQR_ERR_INTERNAL;
    }
    if (!wake_arm(l)) {
        unwind_failed_start(l);
        return MOQR_ERR_INTERNAL;
    }
    l->ledger |= MOQR_ADMIN_LEDGER_WAKE;

    if (pthread_mutex_init(&l->gate_mu, NULL) != 0) {
        unwind_failed_start(l);
        return MOQR_ERR_INTERNAL;
    }
    if (pthread_cond_init(&l->gate_cv, NULL) != 0) {
        (void)pthread_mutex_destroy(&l->gate_mu);
        unwind_failed_start(l);
        return MOQR_ERR_INTERNAL;
    }
    l->ledger |= MOQR_ADMIN_LEDGER_GATE;
#ifdef MOQR_ADMIN_LISTEN_TESTING
    /* Two acquisitions, two ledger entries. Joining them with || leaves the
     * mutex initialized and unrecorded when the condition variable fails, and
     * unwind then cannot release what it was never told about. */
    /*
     * AN ERROR-CHECKING MUTEX, in the testing build only.
     *
     * It makes the blocking `pthread_mutex_lock` report EDEADLK for a
     * self-lock instead of deadlocking, which is what lets a probe tell the
     * current owner from a foreign one. `pthread_mutex_trylock` reports EBUSY
     * either way, so it answers only whether the lock is held -- the half no
     * marker can forge.
     */
    {
        pthread_mutexattr_t view_attr;
        int arc;

        if (pthread_mutexattr_init(&view_attr) != 0) {
            unwind_failed_start(l);
            return MOQR_ERR_INTERNAL;
        }
        if (pthread_mutexattr_settype(&view_attr,
                                      PTHREAD_MUTEX_ERRORCHECK) != 0) {
            (void)pthread_mutexattr_destroy(&view_attr);
            unwind_failed_start(l);
            return MOQR_ERR_INTERNAL;
        }
        arc = pthread_mutex_init(&l->view_mu, &view_attr);
        (void)pthread_mutexattr_destroy(&view_attr);
        if (arc != 0) {
            unwind_failed_start(l);
            return MOQR_ERR_INTERNAL;
        }
    }
    if (0) {
        unwind_failed_start(l);
        return MOQR_ERR_INTERNAL;
    }
    atomic_fetch_add(&g_view_mu_init, 1);
    l->ledger |= MOQR_ADMIN_LEDGER_VIEW_MU;
    if (atomic_load(&g_test_fail_view_cv) ||
        pthread_cond_init(&l->view_cv, NULL) != 0) {
        unwind_failed_start(l);
        return MOQR_ERR_INTERNAL;
    }
    l->ledger |= MOQR_ADMIN_LEDGER_VIEW_CV;
    if (pthread_mutex_init(&l->seam_mu, NULL) != 0) {
        unwind_failed_start(l);
        return MOQR_ERR_INTERNAL;
    }
    l->ledger |= MOQR_ADMIN_LEDGER_SEAM_MU;
    if (pthread_cond_init(&l->seam_cv, NULL) != 0) {
        unwind_failed_start(l);
        return MOQR_ERR_INTERNAL;
    }
    l->ledger |= MOQR_ADMIN_LEDGER_SEAM_CV;
#endif

    if (pthread_attr_init(&attr) != 0) {
        unwind_failed_start(l);
        return MOQR_ERR_INTERNAL;
    }
    /* A FIXED, CHECKED stack, so the reservation the capacity line reports is
     * the reservation the thread actually takes. */
    if (pthread_attr_setstacksize(&attr, MOQR_CLI_ADMIN_STACK_BYTES) != 0 ||
        pthread_attr_getstacksize(&attr, &got_stack) != 0 ||
        got_stack != (size_t)MOQR_CLI_ADMIN_STACK_BYTES) {
        (void)pthread_attr_destroy(&attr);
        unwind_failed_start(l);
        return MOQR_ERR_INTERNAL;
    }
    if (pthread_create(&l->thread, &attr, owner_thread, l) != 0) {
        (void)pthread_attr_destroy(&attr);
        unwind_failed_start(l);
        return MOQR_ERR_INTERNAL;
    }
    (void)pthread_attr_destroy(&attr);
    l->ledger |= MOQR_ADMIN_LEDGER_THREAD;
    *out = l;
    return MOQR_OK;
}

/*
 * A failed activation still has to stop the owner it started -- and that stop
 * can fail to prove the owner dead, exactly like any other.
 *
 * Collapsing that into one "activation failed" answer would tell the caller to
 * tear down its snapshot, broker and callback context while a live owner still
 * holds them: the same use-after-free the ordinary stop path exists to
 * prevent, reached through the startup path instead. So the unprovable outcome
 * is propagated verbatim.
 */
static moqr_result_t
activation_failed(struct moqr_admin_listen *l)
{
    moqr_result_t src = moqr_admin_listen_stop(l);
    return src == MOQR_ERR_WOULD_BLOCK ? MOQR_ERR_WOULD_BLOCK
                                       : MOQR_ERR_INTERNAL;
}

moqr_result_t
moqr_admin_listen_activate(moqr_admin_listen_t *l)
{
    if (l == NULL) {
        return MOQR_ERR_INVAL;
    }
    if ((l->ledger & MOQR_ADMIN_LEDGER_THREAD) == 0u) {
        return MOQR_ERR_WRONG_STATE;   /* nothing to release */
    }
    /* Only now may a connection be accepted: every pointer the owner's
     * callbacks reach has been installed by the caller. */
    if (listen(l->listen_fd, (int)MOQR_ADMIN_MAX_CLIENTS) != 0) {
        owner_fail(l, MOQR_ADMIN_HEALTH_INVARIANT);
        return activation_failed(l);
    }
    if (gate_open_and_await_report(l) != 1) {
        /* The owner failed its first turn. It is already unwinding; join it so
         * the caller is not left with a half-started endpoint. */
        return activation_failed(l);
    }
    return MOQR_OK;
}

void
moqr_admin_listen_note_terminal(moqr_admin_listen_t *l)
{
    if (l == NULL) {
        return;
    }
    atomic_store(&l->terminal, 1);
    wake_post(l);
}

void
moqr_admin_listen_notify(moqr_admin_listen_t *l)
{
    if (l != NULL) {
        wake_post(l);
    }
}

moqr_result_t
moqr_admin_listen_stop(moqr_admin_listen_t *l)
{
    if (l == NULL) {
        return MOQR_ERR_INVAL;
    }
    /* A request plus a wake, then a join. The OWNER cancels and settles;
     * nothing here touches the state machine. The object stays alive so a lane
     * still being joined can safely notify it.
     *
     * The gate is opened unconditionally: an endpoint stopped before it was
     * ever activated must not leave its owner parked forever. */
    (void)pthread_mutex_lock(&l->gate_mu);
    l->gate_open = true;
    (void)pthread_cond_broadcast(&l->gate_cv);
    (void)pthread_mutex_unlock(&l->gate_mu);

    if (!join_owner(l)) {
        /* The owner's death cannot be proved. Its reach is the caller's
         * snapshot, broker, facades and callback context -- not just this
         * object -- so none of that may be touched or freed. */
        atomic_store(&l->owner_unjoinable, 1);
        return MOQR_ERR_WOULD_BLOCK;
    }
    return atomic_load(&l->health) == MOQR_ADMIN_HEALTH_STOPPED ||
                   atomic_load(&l->health) == MOQR_ADMIN_HEALTH_OK
               ? MOQR_OK
               : MOQR_ERR_INTERNAL;
}

void
moqr_admin_listen_destroy(moqr_admin_listen_t *l)
{
    if (l == NULL) {
        return;
    }
    /* unwind refuses to free storage a live thread can still reach. */
    unwind(l);
}

moqr_admin_listen_health_t
moqr_admin_listen_health(const moqr_admin_listen_t *l)
{
    if (l == NULL) {
        return MOQR_ADMIN_HEALTH_OK;
    }
    return (moqr_admin_listen_health_t)atomic_load(
        &((struct moqr_admin_listen *)l)->health);
}

bool
moqr_admin_listen_owner_unjoinable(const moqr_admin_listen_t *l)
{
    return l != NULL &&
           atomic_load(&((struct moqr_admin_listen *)l)->owner_unjoinable) != 0;
}

bool
moqr_admin_listen_owner_exited(const moqr_admin_listen_t *l)
{
    return l != NULL &&
           atomic_load(&((struct moqr_admin_listen *)l)->owner_done) != 0;
}

uint64_t
moqr_admin_listen_accepts(const moqr_admin_listen_t *l)
{
    return l == NULL ? 0u
                     : atomic_load(&((struct moqr_admin_listen *)l)->accepts);
}

uint64_t
moqr_admin_listen_refusals(const moqr_admin_listen_t *l)
{
    return l == NULL ? 0u
                     : atomic_load(&((struct moqr_admin_listen *)l)->refusals);
}

uint64_t
moqr_admin_listen_wakes(const moqr_admin_listen_t *l)
{
    return l == NULL ? 0u : atomic_load(&((struct moqr_admin_listen *)l)->wakes);
}

int
moqr_admin_listen_port(const moqr_admin_listen_t *l)
{
    return l == NULL ? 0 : l->port;
}
