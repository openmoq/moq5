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
#include <stdbool.h>
#include <stdio.h>
#include <string.h>

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

int main(void)
{
    setvbuf(stdout, NULL, _IOLBF, 0);
    t_ordered_history_is_retained();
    t_overflow_is_observable();
    if (failures == 0)
        printf("PASS: fake_msq_history\n");
    else
        fprintf(stderr, "FAIL: fake_msq_history (%d)\n", failures);
    return failures != 0;
}
