/*
 * The emergency halt terminates even when it cannot report.
 *
 * When the admin owner cannot be proved stopped, the relay must halt without
 * teardown. That halt is the last resort, so nothing it does on the way out may
 * be able to block it: not a full standard-error pipe, not a full standard-
 * output pipe, not a FILE lock the unjoinable owner still holds. Error
 * reporting is best effort; termination is not.
 *
 * Both fatal branches of the production CLI are driven here -- the ordinary
 * stop and the failed activation -- by compiling the real main.c into this
 * translation unit with only the listener stop/activate calls substituted, so
 * the code under test is the shipped branch, not a copy of it.
 *
 * Every case runs in an OWNED CHILD PROCESS. An event pipe proves the child
 * reached the fatal branch; the child's exit is observed as the hang-up of a
 * second pipe it holds until it dies. A watchdog bounds the wait and is only
 * ever a failure: elapsed time is never evidence that the halt is safe.
 */
#define main moqr_cli_disabled_main
#define moqr_admin_listen_stop halt_test_stop
#define moqr_admin_listen_note_terminal halt_test_note_terminal
#define moqr_admin_listen_activate halt_test_activate
#define moqr_admin_listen_owner_unjoinable halt_test_owner_unjoinable
/* The reporter's stdout flush is its last step before the completion byte
 * the halting thread polls for, so a case can park the REAL reporter there --
 * after the report is written, before completion is notified -- and prove the
 * halting thread does not depend on it. The real declaration is taken before
 * the rename so the stub can forward to it. */
#include <stdio.h>
int halt_test_fflush(FILE *f);
#define fflush halt_test_fflush
#include "../cli/main.c"
#undef main
#undef moqr_admin_listen_stop
#undef moqr_admin_listen_note_terminal
#undef moqr_admin_listen_activate
#undef moqr_admin_listen_owner_unjoinable
#undef fflush

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

/*
 * ThreadSanitizer's runtime flushes stdio inside its `_exit` interceptor, so a
 * TSan-instrumented process with bytes buffered for a full stdout pipe cannot
 * terminate through `_exit` at all -- with or without any thread of ours. That
 * is a property of the sanitizer runtime, which production binaries do not
 * carry, so the stdout-full cases are not run under TSan and the summary says
 * so; they are never reported as passed there.
 */
#if defined(__SANITIZE_THREAD__)
#define HALT_UNDER_TSAN 1
#elif defined(__has_feature)
#if __has_feature(thread_sanitizer)
#define HALT_UNDER_TSAN 1
#endif
#endif
#ifndef HALT_UNDER_TSAN
#define HALT_UNDER_TSAN 0
#endif

/* The child announces that it has reached the production fatal branch. */
static int g_reached_fd = -1;
static bool g_activate_unjoinable = false;

static void
announce_reached(void)
{
    if (write(g_reached_fd, "R", 1) != 1) {
        _exit(91);
    }
}

moqr_result_t
halt_test_stop(moqr_admin_listen_t *l)
{
    (void)l;
    announce_reached();
    return MOQR_ERR_WOULD_BLOCK;
}

void
halt_test_note_terminal(moqr_admin_listen_t *l)
{
    (void)l;
}

moqr_result_t
halt_test_activate(moqr_admin_listen_t *l)
{
    (void)l;
    announce_reached();
    return g_activate_unjoinable ? MOQR_OK : MOQR_ERR_WOULD_BLOCK;
}

bool
halt_test_owner_unjoinable(const moqr_admin_listen_t *l)
{
    (void)l;
    return g_activate_unjoinable;
}

typedef enum { BRANCH_STOP, BRANCH_ACTIVATE, BRANCH_UNJOINABLE } branch_t;
/* REPORTER_STALLED: a writable sink, but the reporter is parked after the
 * report is written and before it notifies completion, and never resumes. */
typedef enum { SINK_WRITABLE, SINK_STDERR_FULL, SINK_STDOUT_FULL,
               SINK_REPORTER_STALLED } sink_t;

/* Parking the reporter: it announces that it is parked, then waits on a
 * gate nobody ever opens. Armed only in the child of a stall case. */
static int g_stall_event_fd = -1;
static int g_stall_gate_fd = -1;

static void
halt_test_park(void)
{
    char gate;

    if (write(g_stall_event_fd, "S", 1) != 1) {
        _exit(94);
    }
    (void)read(g_stall_gate_fd, &gate, 1);   /* never returns in practice */
    _exit(95);
}

/* A stalled case parks the reporter here, so it never delivers the byte the
 * halting thread polls for. Every other case forwards to the real flush. */
int
halt_test_fflush(FILE *f)
{
    if (g_stall_event_fd >= 0) {
        halt_test_park();
    }
    return fflush(f);
}

#define HALT_STATUS 70
#define REACH_BUDGET_MS 5000
#define HALT_BUDGET_MS 5000

/* Fill a pipe's write side to EAGAIN, then return it to blocking mode: the
 * next blocking write on it cannot complete until someone reads. */
static bool
fill_pipe(int wfd)
{
    static char bytes[4096];
    int flags = fcntl(wfd, F_GETFL);

    memset(bytes, 'x', sizeof(bytes));
    if (flags < 0 || fcntl(wfd, F_SETFL, flags | O_NONBLOCK) < 0) {
        return false;
    }
    while (write(wfd, bytes, sizeof(bytes)) > 0) {
    }
    if (errno != EAGAIN) {
        return false;
    }
    return fcntl(wfd, F_SETFL, flags) == 0;
}

static const char *
branch_name(branch_t b)
{
    return b == BRANCH_STOP ? "stop"
         : b == BRANCH_ACTIVATE ? "activation" : "activation-unjoinable";
}

static const char *
sink_name(sink_t s)
{
    return s == SINK_WRITABLE ? "writable"
         : s == SINK_STDERR_FULL ? "stderr-full"
         : s == SINK_STDOUT_FULL ? "stdout-full" : "reporter-stalled";
}

static int
halt_case(branch_t branch, sink_t sink)
{
    int failures = 0;
    int sinkp[2], reached[2], ended[2], stall[2], gate[2];
    pid_t child;
    int status = 0;
    const char *what = branch_name(branch);
    const char *how = sink_name(sink);

    if (pipe(sinkp) != 0 || pipe(reached) != 0 || pipe(ended) != 0 ||
        pipe(stall) != 0 || pipe(gate) != 0) {
        printf("  halt[%s/%s]: no pipe\n", what, how);
        return 1;
    }
    if ((sink == SINK_STDERR_FULL || sink == SINK_STDOUT_FULL) &&
        !fill_pipe(sinkp[1])) {
        printf("  halt[%s/%s]: the fixture could not fill its pipe\n", what,
               how);
        return 1;
    }
    fflush(stdout);
    child = fork();
    if (child < 0) {
        printf("  halt[%s/%s]: no fork\n", what, how);
        return 1;
    }
    if (child == 0) {
        (void)close(reached[0]);
        (void)close(ended[0]);
        (void)close(sinkp[0]);
        (void)close(stall[0]);
        (void)close(gate[1]);
        g_reached_fd = reached[1];
        if (sink == SINK_REPORTER_STALLED) {
            g_stall_event_fd = stall[1];
            g_stall_gate_fd = gate[0];
        }
        g_activate_unjoinable = (branch == BRANCH_UNJOINABLE);
        if (sink == SINK_STDOUT_FULL) {
            /* Bytes are left buffered in stdout on purpose: a flush of them is
             * what a full pipe turns into a hang. */
            if (dup2(sinkp[1], STDOUT_FILENO) < 0) {
                _exit(92);
            }
            fputs("buffered", stdout);
        } else if (dup2(sinkp[1], STDERR_FILENO) < 0) {
            _exit(92);
        }
        if (branch == BRANCH_STOP) {
            (void)admin_endpoint_stop((moqr_admin_listen_t *)(uintptr_t)1);
        } else {
            (void)admin_endpoint_activate((moqr_admin_listen_t *)(uintptr_t)1);
        }
        _exit(93);   /* the fatal branch returned: it did not halt */
    }
    (void)close(reached[1]);
    (void)close(ended[1]);
    (void)close(stall[1]);
    (void)close(gate[0]);
    {
        struct pollfd ready = { .fd = reached[0], .events = POLLIN,
                                .revents = 0 };
        char r = 0;
        if (poll(&ready, 1, REACH_BUDGET_MS) != 1 ||
            read(reached[0], &r, 1) != 1 || r != 'R') {
            printf("  halt[%s/%s]: the child never reached the fatal branch; "
                   "the fixture is unusable\n", what, how);
            (void)kill(child, SIGKILL);
            (void)waitpid(child, &status, 0);
            failures++;
            goto out;
        }
    }
    if (sink == SINK_REPORTER_STALLED) {
        /* The reporter must actually be parked; otherwise the case proves
         * nothing about a stalled reporter. */
        struct pollfd parked = { .fd = stall[0], .events = POLLIN,
                                 .revents = 0 };
        char p = 0;
        if (poll(&parked, 1, REACH_BUDGET_MS) != 1 ||
            read(stall[0], &p, 1) != 1 || p != 'S') {
            printf("  halt[%s/%s]: the reporter never reached its handoff; "
                   "the fixture is unusable\n", what, how);
            (void)kill(child, SIGKILL);
            (void)waitpid(child, &status, 0);
            failures++;
            goto out;
        }
    }
    {
        /* The child holds the write side of `ended` until it dies; hang-up on
         * the read side is the exit event. */
        struct pollfd exit_event = { .fd = ended[0], .events = POLLIN,
                                     .revents = 0 };
        int ev = poll(&exit_event, 1, HALT_BUDGET_MS);
        if (ev != 1) {
            printf("  halt[%s/%s]: reached the fatal branch but did not "
                   "halt — the last resort %s\n", what, how,
                   sink == SINK_REPORTER_STALLED
                       ? "waited on its stalled reporter"
                       : "blocked on its own report");
            (void)kill(child, SIGKILL);
            (void)waitpid(child, &status, 0);
            failures++;
            goto out;
        }
    }
    if (waitpid(child, &status, 0) != child) {
        printf("  halt[%s/%s]: could not reap the child\n", what, how);
        failures++;
        goto out;
    }
    if (!WIFEXITED(status) || WEXITSTATUS(status) != HALT_STATUS) {
        printf("  halt[%s/%s]: the child ended with status %d, not the halt "
               "status %d\n", what, how,
               WIFEXITED(status) ? WEXITSTATUS(status) : -1, HALT_STATUS);
        failures++;
    }
    if (sink == SINK_WRITABLE || sink == SINK_REPORTER_STALLED) {
        /* When the sink CAN take the report, the report arrives. */
        static char got[8192];
        ssize_t n;
        int flags = fcntl(sinkp[0], F_GETFL);
        (void)fcntl(sinkp[0], F_SETFL, flags | O_NONBLOCK);
        n = read(sinkp[0], got, sizeof(got) - 1u);
        if (n <= 0) {
            printf("  halt[%s/%s]: a writable sink received no report\n",
                   what, how);
            failures++;
        } else {
            got[n] = '\0';
            if (strstr(got, "halting without teardown") == NULL) {
                printf("  halt[%s/%s]: the report does not say it is halting "
                       "without teardown: [%.80s]\n", what, how, got);
                failures++;
            }
        }
    }
out:
    (void)close(sinkp[0]);
    (void)close(sinkp[1]);
    (void)close(reached[0]);
    (void)close(ended[0]);
    (void)close(stall[0]);
    (void)close(gate[1]);
    return failures;
}

int
main(void)
{
    int failures = 0;
    static const branch_t branches[] = { BRANCH_STOP, BRANCH_ACTIVATE,
                                         BRANCH_UNJOINABLE };
    static const sink_t sinks[] = { SINK_WRITABLE, SINK_STDERR_FULL,
                                    SINK_STDOUT_FULL, SINK_REPORTER_STALLED };
    int cases = 0;
    int not_run = 0;

    (void)signal(SIGPIPE, SIG_IGN);
    for (size_t b = 0; b < sizeof(branches) / sizeof(branches[0]); b++) {
        for (size_t s = 0; s < sizeof(sinks) / sizeof(sinks[0]); s++) {
            if (HALT_UNDER_TSAN && sinks[s] == SINK_STDOUT_FULL) {
                not_run++;
                continue;
            }
            failures += halt_case(branches[b], sinks[s]);
            cases++;
        }
    }
    if (not_run != 0) {
        printf("  halt: %d stdout-full case(s) not run under ThreadSanitizer, "
               "whose runtime flushes stdout inside _exit\n", not_run);
    }
    if (failures != 0) {
        printf("FAIL: %d emergency-halt violation(s)\n", failures);
        return 1;
    }
    printf("PASS: relay_admin_halt (%d cases)\n", cases);
    return 0;
}
