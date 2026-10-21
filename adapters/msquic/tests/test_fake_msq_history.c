/*
 * Fake-MsQuic shutdown history.
 *
 * A stream that is shut down more than once must retain the ORDER and the
 * distinct (flags, code) pairs it saw, and the moment history can no longer
 * be recorded must be observable rather than silent.
 *
 * A fake that keeps only the LATEST shutdown cannot distinguish "abort-send
 * then abort-receive" from "abort-receive alone", which is exactly the
 * distinction a teardown-ordering test needs. The capability is therefore
 * required, not optional: when the substrate does not offer it this file
 * reports a missing observation and fails, rather than passing on what it
 * cannot see.
 */
#if defined(MOQ_FAKE_MSQ_POSIX_GUARDS) && defined(__linux__) && \
    !defined(_POSIX_C_SOURCE)
#define _POSIX_C_SOURCE 200809L
#endif

#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#ifdef MOQ_FAKE_MSQ_POSIX_GUARDS
#include <errno.h>
#include <signal.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

#include "support/fake_msq_table.h"

static int failures = 0;

#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

/*
 * How many ordered entries the substrate can report. A substrate without the
 * capability answers 0, and HISTORY_AVAILABLE is what the assertions below
 * fail on -- by design, so a missing observation is reported as a failure
 * rather than skipped.
 */
#ifdef FAKE_MSQ_SHUTDOWN_LOG
enum { HISTORY_CAP = FAKE_MSQ_SHUTDOWN_LOG };
#define HISTORY_AVAILABLE 1
static uint64_t hist_code(const fake_msq_stream_t *st, int i)
{ return st->shutdown_codes_log[i]; }
static uint32_t hist_flags(const fake_msq_stream_t *st, int i)
{ return st->shutdown_flags_log[i]; }
static bool hist_overflow(const fake_msq_stream_t *st)
{ return st->shutdown_log_overflow; }
static int hist_len(const fake_msq_stream_t *st)
{ return st->shutdown_log_len; }
#else
enum { HISTORY_CAP = 0 };
#define HISTORY_AVAILABLE 0
/* Without the capability every history question answers "nothing recorded",
 * which is what the assertions below reject -- so a missing observation is a
 * failure rather than a skip. */
static uint64_t hist_code(const fake_msq_stream_t *st, int i)
{ (void)st; (void)i; return 0; }
static uint32_t hist_flags(const fake_msq_stream_t *st, int i)
{ (void)st; (void)i; return 0; }
static bool hist_overflow(const fake_msq_stream_t *st)
{ (void)st; return false; }
static int hist_len(const fake_msq_stream_t *st)
{ (void)st; return -1; }
#endif

static fake_msq_t g_fake;

static fake_msq_stream_t *open_stream(void)
{
    fake_msq_init(&g_fake, true);
    return fake_msq_peer_stream(&g_fake, 3, false);
}

static void shutdown_once(fake_msq_stream_t *st,
                          QUIC_STREAM_SHUTDOWN_FLAGS flags, uint64_t code)
{
    const QUIC_API_TABLE *api = fake_msq_table(&g_fake);

    api->StreamShutdown((HQUIC)st, flags, code);
}

static void t_ordered_history_is_retained(void)
{
    int before = failures;
    fake_msq_stream_t *st = open_stream();

    CHECK(st != NULL);
    if (st == NULL)
        goto done;

    shutdown_once(st, QUIC_STREAM_SHUTDOWN_FLAG_ABORT_SEND, 0x11);
    shutdown_once(st, QUIC_STREAM_SHUTDOWN_FLAG_ABORT_RECEIVE, 0x22);
    shutdown_once(st, QUIC_STREAM_SHUTDOWN_FLAG_GRACEFUL, 0x33);

    /* the total count is exact regardless of history capacity */
    CHECK(st->shutdown_calls == 3);
    /* and the LATEST observation stays correct */
    CHECK(st->last_shutdown_code == 0x33);
    CHECK(st->last_shutdown_flags == QUIC_STREAM_SHUTDOWN_FLAG_GRACEFUL);

    CHECK(HISTORY_AVAILABLE); /* the missing observation, when absent */
    CHECK(HISTORY_CAP >= 3);
    if (HISTORY_CAP >= 3) {
        /* order preserved, and the FIRST entries were not overwritten */
        CHECK(hist_code(st, 0) == 0x11);
        CHECK(hist_code(st, 1) == 0x22);
        CHECK(hist_code(st, 2) == 0x33);
        CHECK(hist_flags(st, 0) ==
              (uint32_t)QUIC_STREAM_SHUTDOWN_FLAG_ABORT_SEND);
        CHECK(hist_flags(st, 1) ==
              (uint32_t)QUIC_STREAM_SHUTDOWN_FLAG_ABORT_RECEIVE);
        CHECK(hist_flags(st, 2) ==
              (uint32_t)QUIC_STREAM_SHUTDOWN_FLAG_GRACEFUL);
    }
    CHECK(hist_len(st) == 3);  /* three recorded, none dropped */
    CHECK(!hist_overflow(st)); /* three fits */

done:
    if (failures == before)
        printf("PASS: ordered_history_is_retained\n");
}

static void t_overflow_is_observable(void)
{
    int before = failures;
    fake_msq_stream_t *st = open_stream();

    CHECK(st != NULL);
    if (st == NULL)
        goto done;

    CHECK(HISTORY_AVAILABLE);
    int i;

    /* exactly fill the log: still no overflow */
    for (i = 0; i < HISTORY_CAP; i++)
        shutdown_once(st, QUIC_STREAM_SHUTDOWN_FLAG_GRACEFUL,
                      (uint64_t)(0x100 + i));
    CHECK(st->shutdown_calls == HISTORY_CAP);
    CHECK(hist_len(st) == HISTORY_CAP); /* exactly at the bound */
    CHECK(!hist_overflow(st));
    if (HISTORY_CAP > 0)
        CHECK(hist_code(st, 0) == 0x100);

    /* the FIRST unrecordable call makes the loss observable ... */
    shutdown_once(st, QUIC_STREAM_SHUTDOWN_FLAG_GRACEFUL, 0xBEEF);
    CHECK(hist_overflow(st));
    /* ... while the total count and the latest observation stay correct */
    CHECK(st->shutdown_calls == HISTORY_CAP + 1);
    CHECK(st->last_shutdown_code == 0xBEEF);
    CHECK(hist_len(st) == HISTORY_CAP); /* the bound still holds */
    /* ... and nothing already recorded was overwritten */
    if (HISTORY_CAP > 0) {
        CHECK(hist_code(st, 0) == 0x100);
        CHECK(hist_code(st, HISTORY_CAP - 1) ==
              (uint64_t)(0x100 + HISTORY_CAP - 1));
    }

    /* a SECOND overflowing call changes nothing it must not: the bound, the
     * sticky flag and the retained entries all hold, while the total count
     * and the latest-call facts still advance */
    shutdown_once(st, QUIC_STREAM_SHUTDOWN_FLAG_ABORT_SEND, 0xF00D);
    CHECK(hist_len(st) == HISTORY_CAP);
    CHECK(hist_overflow(st));
    CHECK(st->shutdown_calls == HISTORY_CAP + 2);
    CHECK(st->last_shutdown_code == 0xF00D);
    CHECK(st->last_shutdown_flags == QUIC_STREAM_SHUTDOWN_FLAG_ABORT_SEND);
    if (HISTORY_CAP > 0) {
        CHECK(hist_code(st, 0) == 0x100);
        CHECK(hist_code(st, HISTORY_CAP - 1) ==
              (uint64_t)(0x100 + HISTORY_CAP - 1));
    }

done:
    if (failures == before)
        printf("PASS: overflow_is_observable\n");
}

static void t_counting_allocator(void)
{
    int before = failures;
    fake_msq_count_alloc_t counted;
    fake_msq_count_alloc_init(&counted);
    const moq_alloc_t *a = &counted.api;
    CHECK(atomic_load(&counted.live_bytes) == 0);
    void *empty = a->realloc(NULL, 0, 0, a->ctx);
    CHECK(empty == NULL);
    a->free(empty, 0, a->ctx);
    CHECK(atomic_load(&counted.live_bytes) == 0);

    size_t size = 16;
    unsigned char *p = a->alloc(size, a->ctx);
    CHECK(p != NULL);
    if (p == NULL) goto done;
    memset(p, 0x51, size);
    CHECK(atomic_load(&counted.live_bytes) == 16);

    unsigned char *q = a->realloc(p, size, 64, a->ctx);
    CHECK(q != NULL);
    if (q == NULL) goto done;
    p = q;
    size = 64;
    CHECK(atomic_load(&counted.live_bytes) == 64);
    for (size_t i = 0; i < 16; ++i) CHECK(p[i] == 0x51);

    q = a->realloc(p, size, 8, a->ctx);
    CHECK(q != NULL);
    if (q == NULL) goto done;
    p = q;
    size = 8;
    CHECK(atomic_load(&counted.live_bytes) == 8);
    for (size_t i = 0; i < size; ++i) CHECK(p[i] == 0x51);

    p = a->realloc(p, size, 0, a->ctx);
    size = 0;
    CHECK(p == NULL);
    CHECK(atomic_load(&counted.live_bytes) == 0);
    a->free(p, size, a->ctx);
    p = NULL;

    size = 24;
    p = a->realloc(NULL, 0, size, a->ctx);
    CHECK(p != NULL);
    if (p == NULL) goto done;
    CHECK(atomic_load(&counted.live_bytes) == 24);

done:
    a->free(p, size, a->ctx);
    CHECK(atomic_load(&counted.live_bytes) == 0);
    if (failures == before)
        printf("PASS: counting_allocator (alloc, grow, shrink, zero-release)\n");
}

#ifdef MOQ_FAKE_MSQ_POSIX_GUARDS
enum guard_case {
    FINAL_PENDING_SEND, CLOSE_PENDING_DGRAM, FINAL_BAD_COUNTS,
    STREAM_CLOSE_PENDING, SEND_AFTER_FINAL, SEND_AFTER_CLOSE,
    SEND_AFTER_STREAM_CLOSE, DGRAM_AFTER_FINAL, DGRAM_AFTER_CLOSE,
    CANCEL_CLOSED_STREAM, CLOSE_AFTER_FINAL_PENDING
};

static QUIC_STATUS QUIC_API forbidden_callback(HQUIC stream, void *ctx,
                                               QUIC_STREAM_EVENT *ev)
{
    (void)stream; (void)ctx; (void)ev;
    _exit(92); /* A removed guard must fail before touching a stale context. */
}

static void exercise_guard(enum guard_case which)
{
    fake_msq_stream_t *st = open_stream();
    uint8_t byte = 0x51;
    QUIC_BUFFER buf = { .Length = 1, .Buffer = &byte };
    const QUIC_API_TABLE *api = fake_msq_table(&g_fake);
    bool dgram = which == CLOSE_PENDING_DGRAM || which == DGRAM_AFTER_FINAL ||
                 which == DGRAM_AFTER_CLOSE;
    if (st == NULL)
        _exit(91);
    st->cb = forbidden_callback;
    QUIC_STATUS status = dgram
        ? api->DatagramSend(fake_msq_conn_handle(&g_fake), &buf, 1,
                            QUIC_SEND_FLAG_NONE, st)
        : api->StreamSend((HQUIC)st, &buf, 1, QUIC_SEND_FLAG_NONE, st);
    if (status != QUIC_STATUS_SUCCESS)
        _exit(91);

    /* Deliberately corrupt only the lifecycle fact each guard must reject. */
    switch (which) {
    case FINAL_PENDING_SEND:
        fake_msq_mark_shutdown_complete(&g_fake);
        break;
    case CLOSE_PENDING_DGRAM:
        fake_msq_mark_connection_closed(&g_fake);
        break;
    case FINAL_BAD_COUNTS:
        g_fake.sends[0].completed = true;
        fake_msq_mark_shutdown_complete(&g_fake);
        break;
    case STREAM_CLOSE_PENDING:
        api->StreamClose((HQUIC)st);
        break;
    case SEND_AFTER_FINAL:
    case SEND_AFTER_CLOSE:
        g_fake.shutdown_complete = which == SEND_AFTER_FINAL;
        g_fake.connection_closed = which == SEND_AFTER_CLOSE;
        (void)fake_msq_deliver_send_complete(&g_fake, true);
        break;
    case SEND_AFTER_STREAM_CLOSE:
        st->closed = true;
        (void)fake_msq_deliver_send_complete(&g_fake, true);
        break;
    case DGRAM_AFTER_FINAL:
    case DGRAM_AFTER_CLOSE:
        g_fake.shutdown_complete = which == DGRAM_AFTER_FINAL;
        g_fake.connection_closed = which == DGRAM_AFTER_CLOSE;
        (void)fake_msq_next_dgram_ctx(&g_fake);
        break;
    case CANCEL_CLOSED_STREAM:
        st->closed = true;
        fake_msq_cancel_stream(st);
        break;
    case CLOSE_AFTER_FINAL_PENDING:
        g_fake.shutdown_complete = true;
        api->ConnectionClose(fake_msq_conn_handle(&g_fake));
        break;
    }
    _exit(90); /* Returning, a signal other than ABRT, or a callback is failure. */
}

static void t_terminal_guards(void)
{
    static const struct {
        enum guard_case which;
        const char *name;
        const char *condition;
    } rows[] = {
        { FINAL_PENDING_SEND, "final-pending-send",
          "fake_msq_pending_sends(f) == 0 && fake_msq_pending_dgrams(f) == 0" },
        { CLOSE_PENDING_DGRAM, "close-pending-dgram",
          "fake_msq_pending_sends(f) == 0 && fake_msq_pending_dgrams(f) == 0" },
        { FINAL_BAD_COUNTS, "final-bad-counts",
          "f->send_completions == (unsigned)f->send_count && f->dgram_completions == (unsigned)f->dgram_count" },
        { STREAM_CLOSE_PENDING, "stream-close-pending",
          "st->owner->sends[i].stream != st || st->owner->sends[i].completed" },
        { SEND_AFTER_FINAL, "send-after-final",
          "!f->shutdown_complete && !f->connection_closed" },
        { SEND_AFTER_CLOSE, "send-after-close",
          "!f->shutdown_complete && !f->connection_closed" },
        { SEND_AFTER_STREAM_CLOSE, "send-after-stream-close",
          "!s->stream->closed && s->stream->cb != NULL" },
        { DGRAM_AFTER_FINAL, "dgram-after-final",
          "!f->shutdown_complete && !f->connection_closed" },
        { DGRAM_AFTER_CLOSE, "dgram-after-close",
          "!f->shutdown_complete && !f->connection_closed" },
        { CANCEL_CLOSED_STREAM, "cancel-closed-stream",
          "!st->closed && st->cb != NULL" },
        { CLOSE_AFTER_FINAL_PENDING, "close-after-final-pending",
          "fake_msq_pending_sends(f) == 0 && fake_msq_pending_dgrams(f) == 0" },
    };
    for (size_t i = 0; i < sizeof(rows) / sizeof(rows[0]); ++i) {
        int before = failures;
        FILE *log = tmpfile();
        CHECK(log != NULL);
        if (log == NULL)
            return;
        fflush(stdout);
        pid_t pid = fork();
        CHECK(pid >= 0);
        if (pid == 0) {
            struct rlimit no_core = { .rlim_cur = 0, .rlim_max = 0 };
            if (dup2(fileno(log), STDERR_FILENO) < 0 ||
                setrlimit(RLIMIT_CORE, &no_core) != 0)
                _exit(91);
            alarm(2); /* Hang guard only; elapsed time never means success. */
            exercise_guard(rows[i].which);
        }
        if (pid < 0) {
            fclose(log);
            return;
        }
        int status = 0;
        pid_t reaped;
        do {
            reaped = waitpid(pid, &status, 0);
        } while (reaped < 0 && errno == EINTR);
        CHECK(reaped == pid);
        CHECK(WIFSIGNALED(status) && WTERMSIG(status) == SIGABRT);
        rewind(log);
        char output[1024];
        size_t n = fread(output, 1, sizeof(output) - 1, log);
        output[n] = '\0';
        CHECK(fgetc(log) == EOF && !ferror(log));
        const char *detail = strstr(output, ": requirement failed: ");
        CHECK(strncmp(output, "fake_msq_table: ", 16) == 0);
        CHECK(strstr(output, "fake_msq_table.c:") != NULL);
        CHECK(detail != NULL);
        if (detail != NULL) {
            detail += strlen(": requirement failed: ");
            size_t len = strlen(rows[i].condition);
            CHECK(strncmp(detail, rows[i].condition, len) == 0 &&
                  strcmp(detail + len, "\n") == 0);
        }
        fclose(log);
        if (failures == before)
            printf("PASS: terminal_guard[%s]\n", rows[i].name);
        else
            fprintf(stderr, "guard[%s] unexpected output: %s\n",
                    rows[i].name, output);
    }
}

#endif

int main(int argc, char **argv)
{
    setvbuf(stdout, NULL, _IOLBF, 0);
    const char *name = "fake_msq_history";
    if (argc == 1) {
        t_ordered_history_is_retained();
        t_overflow_is_observable();
        t_counting_allocator();
    } else if (argc == 2 && strcmp(argv[1], "--terminal-guards") == 0) {
#ifdef MOQ_FAKE_MSQ_POSIX_GUARDS
        name = "fake_msq_history_terminal_guards";
        t_terminal_guards();
#else
        fprintf(stderr, "FAIL: terminal guards unavailable on this platform\n");
        return 1;
#endif
    } else {
        fprintf(stderr, "FAIL: fake_msq_history: unsupported arguments\n");
        return 1;
    }
    if (failures == 0)
        printf("PASS: %s\n", name);
    else
        fprintf(stderr, "FAIL: %s (%d)\n", name, failures);
    return failures != 0;
}
