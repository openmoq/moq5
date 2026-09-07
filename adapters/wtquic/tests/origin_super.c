/* syscall() and SYS_pidfd_open are extensions: a strict -std=c11 translation
 * unit does not see them without asking. */
#if defined(__linux__) && !defined(_GNU_SOURCE)
#define _GNU_SOURCE
#endif

#include "origin_super.h"

#include <errno.h>
#include <poll.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__) || \
    defined(__NetBSD__)
#define ORIGIN_EXIT_KQUEUE 1
#include <sys/event.h>
#elif defined(__linux__)
#define ORIGIN_EXIT_PIDFD 1
#include <sys/syscall.h>
#ifndef SYS_pidfd_open
/* the same number on every Linux architecture since 5.3 */
#define SYS_pidfd_open 434
#endif
#else
#error "no owned process-exit notification for this platform"
#endif

static long long
now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long long)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static void
detail(origin_child_result_t *r, const char *fmt, const char *a)
{
    if (a != NULL) {
        snprintf(r->detail, sizeof(r->detail), fmt, a);
    } else {
        snprintf(r->detail, sizeof(r->detail), "%s", fmt);
    }
}

/* Write all of `len`, retrying a short write and EINTR. */
static bool
write_all(int fd, const void *buf, size_t len)
{
    const unsigned char *p = buf;
    while (len > 0) {
        ssize_t k = write(fd, p, len);
        if (k < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        p += (size_t)k;
        len -= (size_t)k;
    }
    return true;
}

/*
 * Wait for THIS pid to exit, up to an ABSOLUTE monotonic deadline, on an owned
 * termination notification rather than a polling sleep. Returns true only when
 * the kernel actually reported the exit inside the window.
 *
 * The deadline is absolute so that an interrupted wait resumes against the
 * time that is left; a relative timeout restarted after every EINTR would
 * silently extend the grace once per signal.
 *
 * A registration failure is NOT evidence that the child is gone -- only ESRCH
 * is. Anything else returns false and the caller proceeds to KILL and the
 * exact blocking reap, which is the outcome that cannot be wrong.
 */
static bool
await_exit(pid_t pid, long long deadline_ms)
{
    bool got = false;
    int nfd;
#if defined(ORIGIN_EXIT_KQUEUE)
    struct kevent ev;

    nfd = kqueue();
    if (nfd < 0) {
        return false;
    }
    EV_SET(&ev, (uintptr_t)pid, EVFILT_PROC, EV_ADD | EV_ONESHOT, NOTE_EXIT,
           0, NULL);
    if (kevent(nfd, &ev, 1, NULL, 0, NULL) < 0) {
        int e = errno;
        close(nfd);
        return e == ESRCH;
    }
#elif defined(ORIGIN_EXIT_PIDFD)
    /*
     * A pidfd refers to the exact process, so it cannot be confused by the pid
     * reuse a later plain waitpid on a recycled number could see. POLLIN on it
     * means that process has exited.
     */
    nfd = (int)syscall(SYS_pidfd_open, pid, 0u);
    if (nfd < 0) {
        return errno == ESRCH;
    }
#endif
    for (;;) {
        long long left = deadline_ms - now_ms();
        struct pollfd pfd;
        int pr;

        if (left <= 0) {
            break;                       /* the deadline, not a sleep */
        }
        if (left > 2147483647LL) {
            left = 2147483647LL;
        }
        pfd.fd = nfd;
        pfd.events = POLLIN;
        pr = poll(&pfd, 1, (int)left);
        if (pr > 0) {
            got = true;
            break;
        }
        if (pr == 0) {
            break;
        }
        if (errno != EINTR) {
            break;
        }
        /* interrupted: the loop recomputes what is LEFT of the deadline */
    }
    close(nfd);
    return got;
}

/*
 * Stop one OWNED child by its exact pid: TERM, an event-driven bounded wait,
 * then KILL and a blocking reap only once that window has expired. Never a name
 * pattern, never a process group we do not own, and never a sleep used as a
 * correctness predicate.
 */
static void
stop_child(pid_t pid, int *status, bool *reaped)
{
    *reaped = false;
    kill(pid, SIGTERM);
    (void)await_exit(pid, now_ms() + (long long)ORIGIN_TERM_GRACE_MS);
    {
        pid_t w = waitpid(pid, status, WNOHANG);
        if (w == pid) {
            *reaped = true;
            return;
        }
    }
    kill(pid, SIGKILL);
    for (;;) {
        pid_t w = waitpid(pid, status, 0);
        if (w == pid) {
            *reaped = true;
            return;
        }
        if (w < 0 && errno != EINTR) {
            return;
        }
    }
}

/* The write end this child reports on; set only inside a forked child. */
static int g_child_fd = -1;

void
origin_super_child_enter_teardown(void)
{
    origin_phase_t ph;

    if (g_child_fd < 0) {
        return;                 /* not running as a supervised child */
    }
    origin_phase_init(&ph, ORIGIN_PHASE_ENTER_TEARDOWN, (uint64_t)now_ms());
    (void)write_all(g_child_fd, &ph, sizeof(ph));
}

void
origin_super_child_emit_phase(const origin_phase_t *p)
{
    if (g_child_fd < 0 || p == NULL) {
        return;
    }
    (void)write_all(g_child_fd, p, sizeof(*p));
}

void
origin_super_run_row(const origin_super_cfg_t *cfg, size_t row_index,
                     origin_child_result_t *out)
{
    origin_super_run_row_until(cfg, row_index, 0, out);
}

void
origin_super_run_row_until(const origin_super_cfg_t *cfg, size_t row_index,
                           long long table_deadline_ms,
                           origin_child_result_t *out)
{
    int fds[2];
    pid_t pid;
    int budget = (cfg != NULL && cfg->row_budget_ms > 0)
                     ? cfg->row_budget_ms : ORIGIN_ROW_BUDGET_MS;
    int grace = (cfg != NULL && cfg->teardown_grace_ms > 0)
                    ? cfg->teardown_grace_ms : ORIGIN_TEARDOWN_MS;
    unsigned char buf[sizeof(origin_phase_t) + sizeof(origin_report_t) + 64];
    size_t got = 0;
    long long started;
    long long phase_deadline;   /* the ACTIVE phase's absolute deadline */
    long long work_deadline;    /* the work phase's, kept past the swap */
    bool in_teardown = false;
    bool bad_phase = false;
    bool ceiling_hit = false;
    int status = 0;
    bool reaped = false, timed_out = false;
    size_t n = 0;
    const origin_row_t *rows = origin_rows(&n);

    memset(out, 0, sizeof(*out));
    out->status = ORIGIN_CHILD_SPAWN_FAILED;
    if (cfg == NULL || cfg->child == NULL || row_index >= n) {
        detail(out, "no child callback or row out of range", NULL);
        return;
    }
    /*
     * The work clock starts HERE, before the first setup or launch operation,
     * so that pipe creation and fork are spent from the row's own budget. A
     * timestamp taken after the fork would donate all of that to the row.
     */
    started = now_ms();
    work_deadline = started + budget;
    phase_deadline = work_deadline;
    if (cfg->on_before_launch != NULL) {
        cfg->on_before_launch(cfg->ctx);
    }
    if (pipe(fds) != 0) {
        detail(out, "the report pipe could not be created", NULL);
        return;
    }
    pid = fork();
    if (pid < 0) {
        close(fds[0]);
        close(fds[1]);
        detail(out, "the row child could not be started", NULL);
        return;
    }
    if (pid == 0) {
        /* -- child -- */
        origin_report_t rep;
        /* room for more than one record, so a test hook CAN write extra
         * bytes -- the parent must refuse them, not be unable to receive
         * them */
        unsigned char cbuf[2 * sizeof(origin_report_t) + 64];
        size_t len;
        int rc;

        close(fds[0]);
        g_child_fd = fds[1];
        memset(&rep, 0, sizeof(rep));
        rc = cfg->child(cfg->ctx, row_index, &rep);
        len = origin_report_encode(&rep, cbuf, sizeof(cbuf));
        if (cfg->mangle != NULL) {
            len = cfg->mangle(cfg->ctx, cbuf, len, sizeof(cbuf));
        }
        if (len > 0) {
            (void)write_all(fds[1], cbuf, len);
        }
        close(fds[1]);
        _exit(rc == 0 ? 0 : 1);
    }
    /* -- parent -- */
    close(fds[1]);
    out->pid = pid;
    if (cfg->on_after_launch != NULL) {
        cfg->on_after_launch(cfg->ctx);
    }

    /*
     * Event-driven, with THREE absolute deadlines: the work phase, the teardown
     * grace that starts on the child's own transition, and the whole-table
     * ceiling that preempts either. Each wait uses the minimum remaining.
     *
     * The ordering matters more than the deadlines do. Whichever deadline
     * governs a wait stays authoritative until the read that wait authorized
     * has been judged against it -- EOF included -- and only then may a
     * transition install a new one. Checking after the swap would examine the
     * teardown deadline that the late bytes had just bought, which is no check
     * at all: a descriptor that became readable at the edge, with the parent
     * descheduled past the deadline, would be accepted every time.
     */
    for (;;) {
        struct pollfd pfd;
        long long now = now_ms();
        long long limit = phase_deadline;   /* the deadline governing this read */
        bool limit_is_ceiling = false;
        ssize_t k;
        int left;
        int pr;

        if (table_deadline_ms > 0 && table_deadline_ms < limit) {
            limit = table_deadline_ms;
            limit_is_ceiling = true;
        }
        if (now >= limit) {
            timed_out = true;
            ceiling_hit = limit_is_ceiling;
            break;
        }
        left = (int)(limit - now);
        if (cfg->on_before_wait != NULL) {
            cfg->on_before_wait(cfg->ctx, got);
        }
        pfd.fd = fds[0];
        pfd.events = POLLIN;
        pr = poll(&pfd, 1, left);
        if (pr < 0) {
            if (errno == EINTR) {
                continue;           /* the top of the loop recomputes `limit` */
            }
            break;
        }
        if (pr == 0) {
            timed_out = true;
            ceiling_hit = limit_is_ceiling;
            break;
        }
        if (got >= sizeof(buf)) {
            /* more than one record could ever be: stop reading and refuse */
            break;
        }
        {
            /*
             * A short read is always legal, so a test may cap the capacity to
             * put a real boundary where it needs one. Nothing else may depend
             * on this: zero means "as much as will fit", which is every
             * production path.
             */
            size_t want = sizeof(buf) - got;
            if (cfg->read_cap > 0 && cfg->read_cap < want) {
                want = cfg->read_cap;
            }
            k = read(fds[0], buf + got, want);
        }
        if (k < 0) {
            if (errno == EINTR) {
                continue;
            }
            break;
        }
        /*
         * Judge the read against the deadline that authorized it, BEFORE the
         * bytes mean anything and before EOF can end the loop. EOF is the one
         * result that used to leave without being checked.
         */
        if (cfg->on_after_read != NULL) {
            cfg->on_after_read(cfg->ctx, (long long)k);
        }
        {
            long long after = now_ms();
            if (after >= limit) {
                timed_out = true;
                ceiling_hit = limit_is_ceiling;
                break;
            }
        }
        if (k == 0) {
            break;              /* clean EOF, and it arrived in time */
        }
        got += (size_t)k;
        /*
         * As soon as a whole first frame has arrived it must BE the transition:
         * the teardown grace starts on that, not on a guess. The rest of the
         * sequence is validated after the stream ends, so a duplicate or a
         * reordered frame is caught however the reads happened to split.
         */
        if (!in_teardown && got >= sizeof(origin_phase_t)) {
            origin_phase_t ph;
            long long at;
            long long seen = now_ms();

            if (!origin_phase_decode(buf, sizeof(ph), &ph) ||
                ph.kind != ORIGIN_PHASE_ENTER_TEARDOWN) {
                bad_phase = true;
                break;
            }
            /*
             * The instant must lie inside the window this parent can vouch
             * for: not before its own work start, and not after it read the
             * frame. A child claiming either would be buying grace it never
             * had.
             */
            at = (long long)ph.at_ms;
            if (at < started || at > seen) {
                bad_phase = true;
                break;
            }
            /*
             * And it must have happened before the work deadline it ends. A
             * transition at or after that instant is a row that overran its
             * WORK budget, and it is reported as one -- it does not get to
             * convert lateness into a fresh teardown grace and then be
             * reported as a teardown overrun.
             */
            if (at >= work_deadline) {
                timed_out = true;
                break;
            }
            in_teardown = true;
            /* the grace runs from the CHILD'S transition, not from receipt */
            phase_deadline = at + grace;
        }
    }
    close(fds[0]);

    if (bad_phase) {
        stop_child(pid, &status, &reaped);
        out->status = ORIGIN_CHILD_BAD_PHASE;
        out->reaped = reaped;
        detail(out, "the child's phase sequence was missing, duplicated or "
                    "corrupt", NULL);
        return;
    }

    if (timed_out) {
        stop_child(pid, &status, &reaped);
        out->status = ceiling_hit ? ORIGIN_CHILD_TABLE_CEILING
                                  : ORIGIN_CHILD_TIMEOUT;
        out->reaped = reaped;
        detail(out, ceiling_hit
                        ? "the table ceiling preempted this row"
                        : (in_teardown
                               ? "the child exceeded its teardown grace"
                               : "the child exceeded its work deadline"),
               NULL);
        return;
    }
    for (;;) {
        pid_t w = waitpid(pid, &status, 0);
        if (w == pid) {
            reaped = true;
            break;
        }
        if (w < 0 && errno != EINTR) {
            break;
        }
    }
    out->reaped = reaped;
    if (!reaped) {
        out->status = ORIGIN_CHILD_NO_REPORT;
        detail(out, "the row child could not be reaped", NULL);
        return;
    }
    if (WIFSIGNALED(status)) {
        out->signal = WTERMSIG(status);
        out->status = ORIGIN_CHILD_SIGNAL;
        detail(out, "the row child died on a signal", NULL);
        return;
    }
    out->exit_code = WIFEXITED(status) ? WEXITSTATUS(status) : -1;

    if (got == 0) {
        out->status = ORIGIN_CHILD_NO_REPORT;
        detail(out, "the row child exited without a report", NULL);
        return;
    }
    if (!in_teardown) {
        out->status = ORIGIN_CHILD_BAD_PHASE;
        detail(out, "the child reported without announcing teardown", NULL);
        return;
    }
    /*
     * The exact sequence is one transition followed by one report and nothing
     * else. A second transition -- which is what a duplicate announcement looks
     * like on the wire -- is refused here rather than mistaken for a report.
     */
    {
        origin_phase_t dup;
        if (got >= sizeof(origin_phase_t) + sizeof(origin_phase_t) &&
            origin_phase_decode(buf + sizeof(origin_phase_t), sizeof(dup),
                                &dup)) {
            out->status = ORIGIN_CHILD_BAD_PHASE;
            detail(out, "the child announced teardown more than once", NULL);
            return;
        }
    }
    memmove(buf, buf + sizeof(origin_phase_t), got - sizeof(origin_phase_t));
    got -= sizeof(origin_phase_t);
    if (got == 0) {
        out->status = ORIGIN_CHILD_NO_REPORT;
        detail(out, "the child announced teardown but never reported", NULL);
        return;
    }
    if (got > sizeof(origin_report_t)) {
        /* a second report, or trailing bytes after the first */
        out->status = ORIGIN_CHILD_EXTRA_DATA;
        detail(out, "the row child wrote more than one record", NULL);
        return;
    }
    /*
     * The last gate before anything can be published. Leaving the timed loop
     * is not permission to accept a row: the reap, and the judgement that
     * follows it, both happen in time that the active deadline still governs.
     * Without this a row could be scored OK after its own grace had expired,
     * simply because the expiry happened on this side of the loop.
     */
    if (cfg->on_before_score != NULL) {
        cfg->on_before_score(cfg->ctx);
    }
    {
        long long now = now_ms();
        bool ceiling_first = table_deadline_ms > 0 &&
                             table_deadline_ms < phase_deadline;
        long long limit = ceiling_first ? table_deadline_ms : phase_deadline;

        if (now >= limit) {
            out->status = ceiling_first ? ORIGIN_CHILD_TABLE_CEILING
                                        : ORIGIN_CHILD_TIMEOUT;
            detail(out, ceiling_first
                            ? "the table ceiling preempted this row"
                            : (in_teardown
                                   ? "the child exceeded its teardown grace"
                                   : "the child exceeded its work deadline"),
                   NULL);
            return;
        }
    }

    {
        origin_report_t rep;
        char why[ORIGIN_REASON_CAP];
        origin_snapshot_t snap;
        bool recomputed;

        if (!origin_report_decode(buf, got, &rep, why, sizeof(why))) {
            out->status = ORIGIN_CHILD_BAD_REPORT;
            detail(out, "%s", why);
            return;
        }
        if (rep.row_index != (uint32_t)row_index) {
            out->status = ORIGIN_CHILD_BAD_REPORT;
            detail(out, "the report is for a different row", NULL);
            return;
        }
        /* A claim outside {0,1} is not a boolean this parent will interpret:
         * comparing it after a silent truncation would let 2 read as true. */
        if (rep.passed > 1u) {
            out->status = ORIGIN_CHILD_BAD_REPORT;
            detail(out, "the report's verdict field is not 0 or 1", NULL);
            return;
        }
        /* The parent decides. The child's own claim is compared with a verdict
         * recomputed here from its retained snapshot, so a child that lies -- or
         * that was built against a different rule -- cannot publish a pass. */
        snap = rep.snap;
        recomputed = origin_row_passed(&rows[row_index], &snap, why,
                                       sizeof(why));
        out->passed = recomputed;
        if (recomputed != (rep.passed != 0u)) {
            out->status = ORIGIN_CHILD_CLAIM_MISMATCH;
            detail(out, "the child's claim disagrees with its own snapshot",
                   NULL);
            return;
        }
        /* exit status must agree with the report it wrote */
        if ((out->exit_code == 0) != recomputed) {
            out->status = ORIGIN_CHILD_EXIT_MISMATCH;
            detail(out, "the child's exit status disagrees with its report",
                   NULL);
            return;
        }
        if (!recomputed) {
            detail(out, "%s", why);
        }
        out->status = ORIGIN_CHILD_OK;
    }
}

bool
origin_super_run_table(const origin_super_cfg_t *cfg, size_t *executed,
                       size_t *failed_index, origin_child_result_t *last)
{
    size_t n = 0;
    size_t i;
    long long started = now_ms();
    int ceiling = (cfg != NULL && cfg->table_ceiling_ms > 0)
                      ? cfg->table_ceiling_ms : ORIGIN_PROCESS_CEILING_MS;

    (void)origin_rows(&n);
    if (executed != NULL) {
        *executed = 0;
    }
    if (failed_index != NULL) {
        *failed_index = n;
    }
    for (i = 0; i < n; i++) {
        origin_child_result_t r;

        /*
         * The ceiling is an ABSOLUTE instant handed down to the row, so it
         * preempts a row already running. Checking it only before starting one
         * left the process free to run past it inside that row.
         */
        if (now_ms() >= started + ceiling) {
            if (failed_index != NULL) {
                *failed_index = i;
            }
            if (last != NULL) {
                memset(last, 0, sizeof(*last));
                last->status = ORIGIN_CHILD_TABLE_CEILING;
                snprintf(last->detail, sizeof(last->detail),
                         "the table exceeded its process ceiling");
            }
            return false;
        }
        origin_super_run_row_until(cfg, i, started + ceiling, &r);
        if (executed != NULL) {
            (*executed)++;
        }
        if (last != NULL) {
            *last = r;
        }
        if (r.status != ORIGIN_CHILD_OK || !r.passed) {
            if (failed_index != NULL) {
                *failed_index = i;
            }
            return false;      /* first failure stops the table */
        }
    }
    return true;
}
